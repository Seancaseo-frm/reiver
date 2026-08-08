use axum::{
    extract::{Path, State},
    response::Json,
    routing::post,
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::app_state::WatchState;
use crate::error::{AppError, Result};

pub fn create_widget_query_router() -> Router<Arc<WatchState>> {
    Router::new()
        .route("/{project_id}/widget-query", post(execute_widget_query))
        .route(
            "/{project_id}/discovered-services",
            axum::routing::get(list_discovered_services),
        )
        .route("/{project_id}/variable-values", post(get_variable_values))
}

/// SQL-based widget query configuration
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct SqlQueryConfig {
    pub table: String,
    pub select: Vec<SelectField>,
    #[serde(default, alias = "where")]
    pub where_clause: Option<String>,
    #[serde(default, alias = "groupBy")]
    pub group_by: Option<Vec<String>>,
    #[serde(default, alias = "orderBy")]
    pub order_by: Option<String>,
    #[serde(default)]
    pub limit: Option<u32>,
    #[serde(default)]
    pub interval: Option<String>,
    #[serde(default)]
    pub field_overrides: Option<serde_json::Value>,
}

/// PromQL-based widget query configuration (for Grafana-imported dashboards).
///
/// Single-query widgets have a top-level `promql` field.  Multi-query widgets
/// only carry a `queries` array (no top-level `promql`).  Both shapes must
/// deserialize successfully.
#[derive(Debug, Deserialize)]
pub struct PromQLQueryConfig {
    #[serde(default)]
    pub promql: Option<String>,
    #[serde(default, alias = "legendFormat")]
    pub legend_format: Option<String>,
    #[serde(default)]
    pub queries: Option<Vec<PromQLSubQuery>>,
    #[serde(default)]
    pub instant: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct PromQLSubQuery {
    pub promql: String,
    #[serde(default, alias = "legendFormat", alias = "legend_format")]
    pub legend_format: Option<String>,
}

/// Widget query configuration - either SQL or PromQL mode.
///
/// Sql is tried first because `SqlQueryConfig` has a required `table` field
/// that makes it unambiguous.  `PromQLQueryConfig` has all-optional fields
/// and would otherwise match any payload.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum WidgetQueryConfig {
    Sql(SqlQueryConfig),
    PromQL(PromQLQueryConfig),
}

#[derive(Debug, Deserialize)]
pub struct SelectField {
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub expr: Option<String>,
    #[serde(default, alias = "fn")]
    pub fn_name: Option<String>, // 'count', 'sum', 'avg', 'quantile', 'countIf', etc.
    #[serde(default)]
    pub args: Option<Vec<serde_json::Value>>,
    #[serde(default)]
    pub alias: Option<String>, // Made optional - will use field name if not provided
}

#[derive(Debug, Deserialize)]
pub struct ExecuteQueryRequest {
    pub query: WidgetQueryConfig,
    pub time_range: TimeRange,
    #[serde(default)]
    pub variables: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct TimeRange {
    pub from: String, // ISO timestamp or relative like 'now-1h'
    pub to: String,
}

#[derive(Debug, Serialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub data: Vec<serde_json::Value>,
    pub meta: QueryMeta,
}

#[derive(Debug, Serialize)]
pub struct QueryMeta {
    pub total_rows: u64,
    pub executed_query: String,
    pub elapsed_ms: u64,
}

async fn execute_widget_query(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Json(payload): Json<ExecuteQueryRequest>,
) -> Result<Json<QueryResult>> {
    let start = std::time::Instant::now();

    match &payload.query {
        WidgetQueryConfig::PromQL(promql_config) => {
            execute_promql_widget(promql_config, &payload, &project_id, &state, start).await
        }
        WidgetQueryConfig::Sql(sql_config) => {
            execute_sql_widget(sql_config, &payload, &project_id, &state, start).await
        }
    }
}

/// Resolve `{{ label }}` patterns in a legend_format template using label
/// values from a result row. Handles any whitespace around the label name
/// inside the braces (e.g. `{{pod}}`, `{{ pod }}`, `{{ pod}}`).
///
/// When a label is not found, the placeholder is removed (matching Grafana
/// behaviour). The lookup also tries the OTel storage name for the label
/// (e.g. `instance` → `service_instance_id` from `service.instance.id`).
fn resolve_legend_template(
    template: &str,
    obj: &serde_json::Map<String, serde_json::Value>,
) -> String {
    use reiver_core::promql::metric_names::resolve_label_name;

    let mut result = template.to_string();
    let mut search_start = 0;
    while let Some(open_offset) = result[search_start..].find("{{") {
        let abs_open = search_start + open_offset;
        if let Some(close_offset) = result[abs_open + 2..].find("}}") {
            let abs_close = abs_open + 2 + close_offset;
            let inner = result[abs_open + 2..abs_close].trim();
            let col_key = format!("lbl_{}", inner);

            let val = obj
                .get(&col_key)
                .and_then(|v| v.as_str())
                .or_else(|| {
                    // Try OTel storage name (e.g. instance → service.instance.id)
                    resolve_label_name(inner).and_then(|otel_name| {
                        obj.get(&format!("lbl_{}", otel_name))
                            .and_then(|v| v.as_str())
                    })
                });

            let replacement = val.unwrap_or("");
            result.replace_range(abs_open..abs_close + 2, replacement);
            search_start = abs_open + replacement.len();
        } else {
            break;
        }
    }
    result
}

/// Map PromQL evaluation errors to appropriate HTTP status codes.
/// Parse/validation errors → 400 (client mistake), infrastructure errors → 500.
fn promql_eval_error_to_app_error(e: reiver_core::promql::eval::error::EvalError) -> AppError {
    use reiver_core::promql::eval::error::EvalError;
    match &e {
        EvalError::Parse(_) | EvalError::Invalid(_) | EvalError::Unsupported(_) | EvalError::InvalidRange(_) => {
            AppError::Validation(format!("PromQL error: {}", e))
        }
        _ => AppError::Internal(anyhow::anyhow!("PromQL evaluation failed: {}", e)),
    }
}

async fn execute_promql_widget(
    config: &PromQLQueryConfig,
    payload: &ExecuteQueryRequest,
    project_id: &Uuid,
    state: &Arc<crate::app_state::WatchState>,
    start: std::time::Instant,
) -> Result<Json<QueryResult>> {
    use tracing::Instrument;
    let from_time = parse_time(&payload.time_range.from)?;
    let to_time = parse_time(&payload.time_range.to)?;

    let start_ms = from_time.timestamp_millis();
    let end_ms = to_time.timestamp_millis();
    let range_secs = (end_ms - start_ms) / 1000;
    let step_ms = compute_step_ms(range_secs) * 1000;

    let num_queries = config
        .queries
        .as_ref()
        .map(|q| q.len())
        .unwrap_or(1);
    let span = tracing::info_span!(
        "widget_query",
        otel.name = "PromQL widget query",
        project_id = %project_id,
        range_secs = range_secs,
        step_ms = step_ms,
        num_queries = num_queries,
    );

    let clickhouse_url =
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());

    let evaluator =
        crate::promql_provider::PromQLEvaluator::new(state.http_client.clone(), clickhouse_url);

    async {
        // Multi-query path: use the `queries` array when it exists and either
        // has >1 entries or there is no top-level `promql` to fall back to.
        let use_instant = config.instant.unwrap_or(false);
        let use_multi = config
            .queries
            .as_ref()
            .is_some_and(|q| !q.is_empty() && (q.len() > 1 || config.promql.is_none()));
        if use_multi {
            let queries = config.queries.as_ref().unwrap();
            let mut all_columns: Vec<String> = Vec::new();
            let mut all_data: Vec<serde_json::Value> = Vec::new();
            let mut executed_parts: Vec<String> = Vec::new();

            for sub in queries {
                let promql_str = substitute_variables(
                    &sub.promql,
                    &payload.variables,
                    range_secs,
                    step_ms / 1000,
                );
                executed_parts.push(promql_str.clone());

                let (batches, otel_map) = evaluator
                    .execute(
                        &promql_str,
                        project_id,
                        start_ms,
                        end_ms,
                        step_ms,
                        use_instant,
                    )
                    .await
                    .map_err(|e| {
                        tracing::error!(promql = %promql_str, error = %e, "PromQL sub-query failed");
                        promql_eval_error_to_app_error(e)
                    })?;

                let (mut cols, mut data, _) =
                    crate::promql_provider::batches_to_json(&batches).map_err(|e| {
                        AppError::Internal(anyhow::anyhow!("Failed to format results: {}", e))
                    })?;
                crate::promql_provider::restore_otel_column_names(&mut cols, &mut data, &otel_map);

                if all_columns.is_empty() {
                    all_columns = cols;
                    if !all_columns.contains(&"lbl__series".to_string()) {
                        all_columns.push("lbl__series".to_string());
                    }
                }

                let legend_tmpl = sub
                    .legend_format
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .unwrap_or(&sub.promql);
                for mut row in data {
                    if let Some(obj) = row.as_object_mut() {
                        let resolved = resolve_legend_template(legend_tmpl, obj);
                        obj.insert(
                            "lbl__series".to_string(),
                            serde_json::Value::String(resolved),
                        );
                    }
                    all_data.push(row);
                }
            }

            let total_rows = all_data.len() as u64;
            let elapsed = start.elapsed().as_millis() as u64;
            return Ok(Json(QueryResult {
                columns: all_columns,
                data: all_data,
                meta: QueryMeta {
                    total_rows,
                    executed_query: executed_parts.join(" ; "),
                    elapsed_ms: elapsed,
                },
            }));
        }

        // Single-query path — requires a top-level `promql` field.
        let promql_raw = config.promql.as_deref().ok_or_else(|| {
            AppError::Validation(
                "PromQL widget query must have either 'promql' or 'queries'".into(),
            )
        })?;
        let instant = config.instant.unwrap_or(false);
        let promql_str =
            substitute_variables(promql_raw, &payload.variables, range_secs, step_ms / 1000);

        if promql_str.contains("[[") {
            tracing::warn!(
                original = %promql_raw,
                substituted = %promql_str,
                variables = ?payload.variables,
                "PromQL still contains [[ after variable substitution"
            );
        }

        let (batches, otel_map) = evaluator
            .execute(&promql_str, project_id, start_ms, end_ms, step_ms, instant)
            .await
            .map_err(|e| {
                tracing::error!(
                    promql = %promql_str,
                    error = %e,
                    instant = instant,
                    "PromQL evaluation failed"
                );
                promql_eval_error_to_app_error(e)
            })?;

        let (mut columns, mut data, total_rows) =
            crate::promql_provider::batches_to_json(&batches)
                .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to format results: {}", e)))?;
        crate::promql_provider::restore_otel_column_names(&mut columns, &mut data, &otel_map);

        if let Some(legend_tmpl) = &config.legend_format {
            if !columns.contains(&"lbl__series".to_string()) {
                columns.push("lbl__series".to_string());
            }
            for row in &mut data {
                if let Some(obj) = row.as_object_mut() {
                    let resolved = resolve_legend_template(legend_tmpl, obj);
                    obj.insert(
                        "lbl__series".to_string(),
                        serde_json::Value::String(resolved),
                    );
                }
            }
        }

        let elapsed = start.elapsed().as_millis() as u64;

        Ok(Json(QueryResult {
            columns,
            data,
            meta: QueryMeta {
                total_rows,
                executed_query: promql_str,
                elapsed_ms: elapsed,
            },
        }))
    }
    .instrument(span)
    .await
}

async fn execute_sql_widget(
    sql_config: &SqlQueryConfig,
    payload: &ExecuteQueryRequest,
    project_id: &Uuid,
    state: &Arc<WatchState>,
    start: std::time::Instant,
) -> Result<Json<QueryResult>> {
    let sql = build_clickhouse_query(
        sql_config,
        &payload.time_range,
        project_id,
        state.config.clickhouse_max_rows,
        state.config.clickhouse_default_limit,
        &payload.variables,
    )?;

    let clickhouse_url =
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());

    let response = state
        .http_client
        .post(&clickhouse_url)
        .query(&[
            ("default_format", "JSONEachRow"),
            ("output_format_json_quote_64bit_integers", "0"),
        ])
        .body(sql.clone())
        .send()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse request failed: {}", e)))?;

    if !response.status().is_success() {
        let error_text = response.text().await.unwrap_or_default();
        return Err(AppError::Internal(anyhow::anyhow!(
            "ClickHouse query failed: {}",
            error_text
        )));
    }

    let rows = crate::ch_stream::stream_json_lines(response).await.map_err(|e| {
        AppError::Internal(anyhow::anyhow!("Failed to stream ClickHouse response: {}", e))
    })?;

    let columns: Vec<String> = rows
        .first()
        .and_then(|r| r.as_object())
        .map(|map| map.keys().cloned().collect())
        .unwrap_or_default();

    let elapsed = start.elapsed().as_millis() as u64;
    let total_rows = rows.len() as u64;

    Ok(Json(QueryResult {
        columns,
        data: rows,
        meta: QueryMeta {
            total_rows,
            executed_query: sql,
            elapsed_ms: elapsed,
        },
    }))
}

/// Discovered service from ClickHouse
#[derive(Debug, Serialize)]
struct DiscoveredService {
    service_name: String,
    first_seen: String,
    last_seen: String,
    has_http_spans: bool,
    has_db_spans: bool,
    has_rpc_spans: bool,
    has_messaging_spans: bool,
    span_count: u64,
    error_count: u64,
}

/// List discovered services for a project
/// Used for populating service dropdown in dashboards
async fn list_discovered_services(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
) -> Result<Json<Vec<DiscoveredService>>> {
    // Query ClickHouse for discovered services
    let clickhouse_url =
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());

    let sql = format!(
        r#"SELECT 
            service_name,
            min(first_seen) as first_seen,
            max(last_seen) as last_seen,
            max(has_http_spans) as has_http_spans,
            max(has_db_spans) as has_db_spans,
            max(has_rpc_spans) as has_rpc_spans,
            max(has_messaging_spans) as has_messaging_spans,
            sum(span_count) as span_count,
            sum(error_count) as error_count
        FROM reiver.discovered_services_agg
        WHERE project_id = '{}'
        GROUP BY service_name
        ORDER BY span_count DESC
        LIMIT {}"#,
        project_id, state.config.clickhouse_max_rows
    );

    let client = reqwest::Client::new();
    let response = client
        .post(&clickhouse_url)
        .query(&[("default_format", "JSONEachRow")])
        .body(sql)
        .send()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse request failed: {}", e)))?;

    if !response.status().is_success() {
        let error_text = response.text().await.unwrap_or_default();
        return Err(AppError::Internal(anyhow::anyhow!(
            "ClickHouse query failed: {}",
            error_text
        )));
    }

    let rows = crate::ch_stream::stream_json_lines(response).await.map_err(|e| {
        AppError::Internal(anyhow::anyhow!("Failed to stream ClickHouse response: {}", e))
    })?;

    let services: Vec<DiscoveredService> = rows
        .into_iter()
        .map(|row| DiscoveredService {
            service_name: row["service_name"].as_str().unwrap_or("").to_string(),
            first_seen: row["first_seen"].as_str().unwrap_or("").to_string(),
            last_seen: row["last_seen"].as_str().unwrap_or("").to_string(),
            has_http_spans: row["has_http_spans"].as_u64().unwrap_or(0) > 0,
            has_db_spans: row["has_db_spans"].as_u64().unwrap_or(0) > 0,
            has_rpc_spans: row["has_rpc_spans"].as_u64().unwrap_or(0) > 0,
            has_messaging_spans: row["has_messaging_spans"].as_u64().unwrap_or(0) > 0,
            span_count: row["span_count"].as_u64().unwrap_or(0),
            error_count: row["error_count"].as_u64().unwrap_or(0),
        })
        .collect();

    Ok(Json(services))
}

/// Compute a reasonable step size in seconds based on the time range
fn compute_step_ms(range_secs: i64) -> i64 {
    if range_secs <= 3600 {
        15 // 15s step for <= 1h
    } else if range_secs <= 21600 {
        60 // 1m step for <= 6h
    } else if range_secs <= 86400 {
        300 // 5m step for <= 1d
    } else if range_secs <= 604800 {
        900 // 15m step for <= 7d
    } else {
        3600 // 1h step for > 7d
    }
}

/// Substitute Grafana-style variables in a PromQL string.
///
/// Handles: `$var`, `${var}`, `${var:regex}`, and built-in
/// `$__interval`, `$__rate_interval`, `$__range`.
fn substitute_variables(
    promql: &str,
    variables: &Option<serde_json::Value>,
    range_secs: i64,
    step_secs: i64,
) -> String {
    let mut result = promql.to_string();

    let interval_str = format!("{}s", step_secs);
    let rate_interval_secs = std::cmp::max(step_secs * 4, 60);
    let rate_interval_str = format!("{}s", rate_interval_secs);
    let range_str = format!("{}s", range_secs);

    result = result.replace("$__rate_interval", &rate_interval_str);
    result = result.replace("$__interval", &interval_str);
    result = result.replace("$__range", &range_str);

    if let Some(vars) = variables {
        if let Some(obj) = vars.as_object() {
            let mut names: Vec<&String> = obj.keys().collect();
            names.sort_by(|a, b| b.len().cmp(&a.len()));

            for name in names {
                let value = match obj.get(name).and_then(|v| v.as_str()) {
                    Some(s) if !s.is_empty() && s != "null" && s != "undefined" => s.to_string(),
                    _ => {
                        if let Some(arr) = obj.get(name).and_then(|v| v.as_array()) {
                            let joined = arr
                                .iter()
                                .filter_map(|v| v.as_str())
                                .collect::<Vec<_>>()
                                .join("|");
                            if joined.is_empty() {
                                continue;
                            } else {
                                joined
                            }
                        } else {
                            continue;
                        }
                    }
                };

                let pattern_double_bracket = format!("[[{}]]", name);
                let pattern_braces_regex = format!("${{{}:regex}}", name);
                let pattern_braces = format!("${{{}}}", name);
                let pattern_dollar = format!("${}", name);

                result = result.replace(&pattern_double_bracket, &value);
                result = result.replace(&pattern_braces_regex, &value);
                result = result.replace(&pattern_braces, &value);
                result = result.replace(&pattern_dollar, &value);
            }
        }
    }

    // Safety net: replace any unresolved `$variable` inside range selectors `[...]`
    // with the computed step-based interval. This prevents PromQL parse failures
    // from leftover Grafana variables like `[$interval]` or `[$__auto_interval_interval]`.
    // Must run BEFORE the general cleanup which would replace `$var` with `1`.
    {
        let mut out = String::with_capacity(result.len());
        let mut pos = 0;
        while pos < result.len() {
            if let Some(offset) = result[pos..].find('[') {
                let bracket = pos + offset;
                out.push_str(&result[pos..bracket]);
                if let Some(close_offset) = result[bracket + 1..].find(']') {
                    let inner = &result[bracket + 1..bracket + 1 + close_offset];
                    if inner.contains('$') {
                        out.push('[');
                        out.push_str(&interval_str);
                        out.push(']');
                    } else {
                        out.push_str(&result[bracket..bracket + 1 + close_offset + 1]);
                    }
                    pos = bracket + 1 + close_offset + 1;
                } else {
                    out.push_str(&result[bracket..]);
                    break;
                }
            } else {
                out.push_str(&result[pos..]);
                break;
            }
        }
        result = out;
    }

    // After all substitutions, clean up any remaining [[var]] patterns.
    loop {
        let before = result.clone();

        if let Some(start) = result.find("[[") {
            if let Some(end) = result[start..].find("]]") {
                let end = start + end + 2;

                let before_text = &result[..start];
                let quote_count = before_text.chars().filter(|&c| c == '"').count();
                let in_quotes = quote_count % 2 == 1;

                if in_quotes {
                    result = format!("{}{}{}", &result[..start], ".*", &result[end..]);
                } else {
                    result = format!("{}{}", &result[..start], &result[end..]);
                    result = result.replace(", )", ")");
                    result = result.replace("(, ", "(");
                    result = result.replace(",)", ")");
                    result = result.replace("(,", "(");
                    result = result.replace(", ,", ",");
                }
            } else {
                break;
            }
        } else {
            break;
        }
        if result == before {
            break;
        }
    }

    // Clean up any remaining $var or ${var} patterns that weren't substituted.
    loop {
        let before = result.clone();

        // Handle ${var} and ${var:regex} patterns
        if let Some(start) = result.find("${") {
            if let Some(end) = result[start..].find('}') {
                let end = start + end + 1;
                let before_text = &result[..start];
                let quote_count = before_text.chars().filter(|&c| c == '"').count();
                let in_quotes = quote_count % 2 == 1;

                if in_quotes {
                    result = format!("{}{}{}", &result[..start], ".*", &result[end..]);
                } else {
                    result = format!("{}{}", &result[..start], &result[end..]);
                }
                continue;
            }
        }

        // Handle bare $var patterns (word boundary: $var followed by non-alphanum).
        // Scan all `$` positions — a bare `$` that is not followed by an
        // alphanumeric/underscore (e.g. regex anchor `$"`) must be skipped
        // so we can find real variables later in the string.
        {
            let mut found = false;
            let mut search_from = 0;
            while let Some(rel) = result[search_from..].find('$') {
                let dollar = search_from + rel;
                if dollar + 1 >= result.len() {
                    break;
                }
                let rest = &result[dollar + 1..];
                if rest.starts_with(|c: char| c.is_alphabetic() || c == '_') {
                    let var_end = rest
                        .find(|c: char| !c.is_alphanumeric() && c != '_')
                        .unwrap_or(rest.len());
                    let end = dollar + 1 + var_end;

                    let before_text = &result[..dollar];
                    let quote_count = before_text.chars().filter(|&c| c == '"').count();
                    let in_quotes = quote_count % 2 == 1;

                    if in_quotes {
                        result = format!("{}{}{}", &result[..dollar], ".*", &result[end..]);
                    } else {
                        result = format!("{}1{}", &result[..dollar], &result[end..]);
                    }
                    found = true;
                    break;
                }
                search_from = dollar + 1;
            }
            if found {
                continue;
            }
        }

        if result == before {
            break;
        }
    }

    // Clean up dangling commas/spaces from removed variables
    result = result.replace(", )", ")");
    result = result.replace("(, ", "(");
    result = result.replace(",)", ")");
    result = result.replace("(,", "(");
    result = result.replace(", ,", ",");

    // Convert =\".*\" to =~\".*\" so regex wildcards aren't treated as literal strings.
    result = result.replace("=\".*\"", "=~\".*\"");

    result
}

// ============================================================================
// Variable Values Endpoint
// ============================================================================

#[derive(Debug, Deserialize)]
struct VariableValuesRequest {
    query: String,
    #[serde(default)]
    time_range: Option<TimeRange>,
}

#[derive(Debug, Serialize)]
struct VariableValuesResponse {
    values: Vec<String>,
}

async fn get_variable_values(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Json(payload): Json<VariableValuesRequest>,
) -> Result<Json<VariableValuesResponse>> {
    let query = payload.query.trim();

    if let Some(inner) = parse_query_result(query) {
        return get_variable_values_query_result(
            state,
            project_id,
            &inner,
            payload.time_range.as_ref(),
        )
        .await;
    }

    let (metric_name, label_name) = parse_label_values_query(query)
        .ok_or_else(|| AppError::Validation(
            format!("Unsupported variable query format: {}. Expected label_values(metric, label), label_values(label), or query_result(expr)", query)
        ))?;

    use reiver_core::promql::metric_names::{normalize_histogram_suffix, resolve_label_name, resolve_storage_name};

    let storage_metric = if metric_name.is_empty() {
        String::new()
    } else {
        let normalized = normalize_histogram_suffix(&metric_name);
        let effective = normalized.as_deref().unwrap_or(&metric_name);
        resolve_storage_name(effective)
            .unwrap_or(effective)
            .to_string()
    };
    let storage_label = resolve_label_name(&label_name)
        .unwrap_or(&label_name)
        .to_string();

    let time_clause = if let Some(ref tr) = payload.time_range {
        let from = parse_time(&tr.from)?;
        let to = parse_time(&tr.to)?;
        format!(
            "AND unix_milli >= {} AND unix_milli < {}",
            from.timestamp_millis(),
            to.timestamp_millis()
        )
    } else {
        String::new()
    };

    let sql = if storage_metric.is_empty() {
        format!(
            "SELECT DISTINCT JSONExtractString(labels, '{}') AS val \
             FROM reiver.time_series_v1 \
             WHERE project_id = '{}' {} \
             AND val != '' \
             ORDER BY val \
             LIMIT 1000",
            escape_clickhouse_value(&storage_label),
            project_id,
            time_clause,
        )
    } else {
        format!(
            "SELECT DISTINCT JSONExtractString(labels, '{}') AS val \
             FROM reiver.time_series_v1 \
             WHERE project_id = '{}' \
             AND metric_name = '{}' {} \
             AND val != '' \
             ORDER BY val \
             LIMIT 1000",
            escape_clickhouse_value(&storage_label),
            project_id,
            escape_clickhouse_value(&storage_metric),
            time_clause,
        )
    };

    let clickhouse_url =
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());

    let client = reqwest::Client::new();
    let response = client
        .post(&clickhouse_url)
        .query(&[("default_format", "JSONEachRow")])
        .body(sql)
        .send()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse request failed: {}", e)))?;

    if !response.status().is_success() {
        let error_text = response.text().await.unwrap_or_default();
        return Err(AppError::Internal(anyhow::anyhow!(
            "ClickHouse query failed: {}",
            error_text
        )));
    }

    let rows = crate::ch_stream::stream_json_lines(response).await.map_err(|e| {
        AppError::Internal(anyhow::anyhow!("Failed to stream ClickHouse response: {}", e))
    })?;

    let values: Vec<String> = rows
        .into_iter()
        .filter_map(|row| row.get("val").and_then(|v| v.as_str()).map(String::from))
        .collect();

    Ok(Json(VariableValuesResponse { values }))
}

/// Parse `label_values(metric, label)` or `label_values(label)` Grafana variable queries
fn parse_label_values_query(query: &str) -> Option<(String, String)> {
    let trimmed = query.trim();
    if !trimmed.starts_with("label_values(") || !trimmed.ends_with(')') {
        return None;
    }

    let inner = &trimmed["label_values(".len()..trimmed.len() - 1];
    let parts: Vec<&str> = inner.splitn(2, ',').map(|s| s.trim()).collect();

    match parts.len() {
        1 => Some((String::new(), parts[0].to_string())),
        2 => Some((parts[0].to_string(), parts[1].to_string())),
        _ => None,
    }
}

/// Parse `query_result(promql_expr)` — returns the inner PromQL expression.
fn parse_query_result(query: &str) -> Option<String> {
    let trimmed = query.trim();
    if !trimmed.starts_with("query_result(") || !trimmed.ends_with(')') {
        return None;
    }
    let inner = &trimmed["query_result(".len()..trimmed.len() - 1];
    let inner = inner.trim();
    if inner.is_empty() {
        return None;
    }
    Some(inner.to_string())
}

/// Handle `query_result(promql_expr)` variable queries.
///
/// Extracts the metric name (and optional label matchers) from the PromQL expression,
/// queries ClickHouse for all distinct label JSON blobs on that metric, and returns
/// a formatted series representation for each unique label set.
async fn get_variable_values_query_result(
    state: Arc<WatchState>,
    project_id: Uuid,
    promql_inner: &str,
    time_range: Option<&TimeRange>,
) -> Result<Json<VariableValuesResponse>> {
    let (metric_name, label_filters) = parse_promql_selector(promql_inner);
    if metric_name.is_empty() {
        return Ok(Json(VariableValuesResponse { values: vec![] }));
    }

    let normalized = reiver_core::promql::metric_names::normalize_histogram_suffix(&metric_name);
    let effective_name = normalized.as_deref().unwrap_or(metric_name.as_str());
    let storage_name = reiver_core::promql::metric_names::resolve_storage_name(effective_name)
        .unwrap_or(effective_name);

    let time_clause = if let Some(tr) = time_range {
        let from = parse_time(&tr.from)?;
        let to = parse_time(&tr.to)?;
        format!(
            "AND unix_milli >= {} AND unix_milli < {}",
            from.timestamp_millis(),
            to.timestamp_millis()
        )
    } else {
        String::new()
    };

    let mut label_clause = String::new();
    for (key, val) in &label_filters {
        let storage_key =
            reiver_core::promql::metric_names::resolve_label_name(key).unwrap_or(key.as_str());
        label_clause.push_str(&format!(
            " AND JSONExtractString(labels, '{}') = '{}'",
            escape_clickhouse_value(storage_key),
            escape_clickhouse_value(val),
        ));
    }

    let sql = format!(
        "SELECT DISTINCT labels \
         FROM reiver.time_series_v1 \
         WHERE project_id = '{}' \
         AND metric_name = '{}' {} {} \
         LIMIT 1000",
        project_id,
        escape_clickhouse_value(storage_name),
        time_clause,
        label_clause,
    );

    let clickhouse_url =
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());

    let client = reqwest::Client::new();
    let response = client
        .post(&clickhouse_url)
        .query(&[("default_format", "JSONEachRow")])
        .body(sql)
        .send()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse request failed: {}", e)))?;

    if !response.status().is_success() {
        let error_text = response.text().await.unwrap_or_default();
        return Err(AppError::Internal(anyhow::anyhow!(
            "ClickHouse query failed: {}",
            error_text
        )));
    }

    let rows = crate::ch_stream::stream_json_lines(response).await.map_err(|e| {
        AppError::Internal(anyhow::anyhow!("Failed to stream ClickHouse response: {}", e))
    })?;

    let mut values = Vec::new();
    for row in rows {
        if let Some(labels_str) = row.get("labels").and_then(|v| v.as_str()) {
            if let Ok(labels_map) =
                serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(labels_str)
            {
                let formatted = format_series_repr(&metric_name, &labels_map);
                if !values.contains(&formatted) {
                    values.push(formatted);
                }
            }
        }
    }

    values.sort();
    Ok(Json(VariableValuesResponse { values }))
}

/// Format a series representation: `metric_name{key="value", key2="value2"}`
fn format_series_repr(metric: &str, labels: &serde_json::Map<String, serde_json::Value>) -> String {
    if labels.is_empty() {
        return metric.to_string();
    }
    let pairs: Vec<String> = labels
        .iter()
        .filter_map(|(k, v)| v.as_str().map(|s| format!("{}=\"{}\"", k, s)))
        .collect();
    format!("{}{{{}}}", metric, pairs.join(", "))
}

/// Extract metric name and label equality filters from a simple PromQL selector.
/// Handles: `metric_name`, `metric_name{key="val", key2="val2"}`.
/// Does not handle complex PromQL (aggregations, functions, etc.) — returns
/// just the metric name in those cases.
fn parse_promql_selector(expr: &str) -> (String, Vec<(String, String)>) {
    let expr = expr.trim();
    let brace_start = expr.find('{');
    let metric_name = match brace_start {
        Some(pos) => expr[..pos].trim().to_string(),
        None => expr.to_string(),
    };

    let mut filters = Vec::new();
    if let (Some(start), Some(end)) = (brace_start, expr.rfind('}')) {
        let inner = &expr[start + 1..end];
        for part in inner.split(',') {
            let part = part.trim();
            if let Some(eq_pos) = part.find('=') {
                let key = part[..eq_pos].trim().trim_start_matches('~');
                let val = part[eq_pos + 1..].trim().trim_matches('"');
                if !key.is_empty() && !val.is_empty() && !val.contains('$') && !val.contains(".*") {
                    filters.push((key.to_string(), val.to_string()));
                }
            }
        }
    }

    (metric_name, filters)
}

fn escape_clickhouse_value(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

/// Build a ClickHouse query from widget configuration
fn build_clickhouse_query(
    config: &SqlQueryConfig,
    time_range: &TimeRange,
    project_id: &Uuid,
    max_rows: u32,
    default_limit: u32,
    variables: &Option<serde_json::Value>,
) -> Result<String> {
    // Map table name to actual ClickHouse table
    let table = match config.table.as_str() {
        "spans" => "reiver.spans",
        "logs" => "reiver.logs",
        "metrics" => "reiver.samples_v1",
        "metric_exemplars" => "reiver.metric_exemplars",
        _ => {
            return Err(AppError::Validation(format!(
                "Unknown table: {}",
                config.table
            )))
        }
    };

    // Build SELECT clause
    let mut select_parts: Vec<String> = config
        .select
        .iter()
        .map(|field| build_select_field(field))
        .collect();

    // If this is a time series query (has interval), include time_bucket in SELECT
    // so that the frontend receives the x-axis time values
    if config.interval.is_some() {
        let time_column = match config.table.as_str() {
            "spans" => "timestamp",
            "logs" => "timestamp",
            "metrics" => "unix_milli",
            "metric_exemplars" => "exemplar_time_unix_nano",
            _ => "timestamp",
        };
        let bucket_expr = time_bucket_expression(
            time_column,
            config.interval.as_deref().unwrap(),
            &config.table,
        );
        select_parts.insert(0, bucket_expr);
    }

    // Build WHERE clause
    let mut where_parts = vec![format!("project_id = '{}'", project_id)];

    // Add time range filter - use correct column for each table type (snake_case)
    let time_column = match config.table.as_str() {
        "spans" => "timestamp",
        "logs" => "timestamp",
        "metrics" => "unix_milli",
        "metric_exemplars" => "exemplar_time_unix_nano",
        _ => "timestamp",
    };

    // Parse time range - support relative times like 'now-1h'
    let from_time = parse_time(&time_range.from)?;
    let to_time = parse_time(&time_range.to)?;

    if config.table.as_str() == "metrics" {
        // Metrics use unix_milli
        where_parts.push(format!(
            "{} >= {} AND {} < {}",
            time_column,
            from_time.timestamp_millis(),
            time_column,
            to_time.timestamp_millis()
        ));
    } else if config.table.as_str() == "metric_exemplars" {
        // Exemplars use nanosecond timestamps
        where_parts.push(format!(
            "{} >= {} AND {} < {}",
            time_column,
            from_time.timestamp_nanos_opt().unwrap_or(0),
            time_column,
            to_time.timestamp_nanos_opt().unwrap_or(0)
        ));
    } else {
        where_parts.push(format!(
            "{} >= '{}' AND {} < '{}'",
            time_column,
            from_time.format("%Y-%m-%d %H:%M:%S"),
            time_column,
            to_time.format("%Y-%m-%d %H:%M:%S")
        ));
    }

    if let Some(ref where_clause) = config.where_clause {
        // Sanitize the where clause to prevent SQL injection
        let sanitized = sanitize_where_clause(where_clause);
        if !sanitized.is_empty() {
            where_parts.push(sanitized);
        }
    }

    // Inject service filter from dashboard variables
    if let Some(service) = variables
        .as_ref()
        .and_then(|v| v.get("service"))
        .and_then(|s| s.as_str())
    {
        if !service.is_empty() && is_safe_identifier(service) {
            match config.table.as_str() {
                "spans" | "logs" => {
                    where_parts.push(format!("service_name = '{}'", service));
                }
                "metrics" | "metric_exemplars" => {
                    where_parts.push(format!(
                        "resource_attributes['service.name'] = '{}'",
                        service
                    ));
                }
                _ => {}
            }
        }
    }

    let where_clause = where_parts.join(" AND ");

    // Build GROUP BY clause
    let group_by_clause = if let Some(ref group_by) = config.group_by {
        if !group_by.is_empty() {
            // Add time bucket if interval is specified
            let mut groups = group_by.clone();
            if let Some(ref interval) = config.interval {
                let bucket = time_bucket_expression(time_column, interval, &config.table);
                groups.insert(0, bucket);
            }

            format!(" GROUP BY {}", groups.join(", "))
        } else {
            String::new()
        }
    } else if let Some(ref interval) = config.interval {
        // Time series without explicit group by
        let bucket = time_bucket_expression(time_column, interval, &config.table);
        format!(" GROUP BY {}", bucket)
    } else {
        String::new()
    };

    // Build SELECT clause
    let select_clause = if select_parts.is_empty() {
        "*".to_string()
    } else {
        select_parts.join(", ")
    };

    // Build ORDER BY clause
    let order_by_clause = if let Some(ref order_by) = config.order_by {
        format!(" ORDER BY {}", order_by)
    } else if config.interval.is_some() {
        // Default: order by time bucket for time series
        let time_col = match config.table.as_str() {
            "metrics" => "time_bucket",
            _ => "time_bucket",
        };
        format!(" ORDER BY {} ASC", time_col)
    } else {
        String::new()
    };

    // Build LIMIT clause - use configured limits
    let limit_clause = if let Some(limit) = config.limit {
        format!(" LIMIT {}", limit.min(max_rows)) // Cap at configured max
    } else {
        format!(" LIMIT {}", default_limit) // Use configured default
    };

    let sql = format!(
        "SELECT {} FROM {} WHERE {}{}{}{}",
        select_clause, table, where_clause, group_by_clause, order_by_clause, limit_clause
    );

    Ok(sql)
}

fn build_select_field(field: &SelectField) -> String {
    let alias = field.alias.as_deref().unwrap_or("value");

    if let Some(ref fn_name) = field.fn_name {
        // Aggregate function
        match fn_name.as_str() {
            "count" => format!("count() AS {}", alias),
            "sum" => {
                let f = field.field.as_deref().unwrap_or("1");
                format!("sum({}) AS {}", f, alias)
            }
            "avg" => {
                let f = field.field.as_deref().unwrap_or("1");
                format!("avg({}) AS {}", f, alias)
            }
            "min" => {
                let f = field.field.as_deref().unwrap_or("1");
                format!("min({}) AS {}", f, alias)
            }
            "max" => {
                let f = field.field.as_deref().unwrap_or("1");
                format!("max({}) AS {}", f, alias)
            }
            "quantile" => {
                let f = field.field.as_deref().unwrap_or("Duration");
                let q = field
                    .args
                    .as_ref()
                    .and_then(|a| a.first())
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.95);
                format!("quantile({})(toFloat64({})) AS {}", q, f, alias)
            }
            "countIf" => {
                let condition = field
                    .args
                    .as_ref()
                    .and_then(|a| a.first())
                    .and_then(|v| v.as_str())
                    .unwrap_or("1=1");
                format!("countIf({}) AS {}", condition, alias)
            }
            "histogram" => {
                let f = field.field.as_deref().unwrap_or("Duration");
                let buckets = field
                    .args
                    .as_ref()
                    .and_then(|a| a.first())
                    .and_then(|v| v.as_u64())
                    .unwrap_or(20);
                format!("histogram({})(toFloat64({})) AS {}", buckets, f, alias)
            }
            "uniqExact" => {
                let f = field.field.as_deref().unwrap_or("1");
                format!("uniqExact({}) AS {}", f, alias)
            }
            _ => {
                // Unknown function, try direct
                let f = field.field.as_deref().unwrap_or("1");
                format!("{}({}) AS {}", fn_name, f, alias)
            }
        }
    } else if let Some(ref expr) = field.expr {
        // Raw expression - ClickHouse supports referencing aliases in the same SELECT
        format!("{} AS {}", expr, alias)
    } else if let Some(ref f) = field.field {
        // Simple field reference
        format!("{} AS {}", f, alias)
    } else {
        format!("1 AS {}", alias)
    }
}

fn time_bucket_expression(time_column: &str, interval: &str, table: &str) -> String {
    let seconds = parse_interval_seconds(interval);

    if table == "metrics" {
        // Metrics use unix_milli
        format!(
            "intDiv({}, {}) * {} AS time_bucket",
            time_column,
            seconds * 1000,
            seconds * 1000
        )
    } else if table == "metric_exemplars" {
        // Exemplars use nanosecond timestamps
        let nanos = seconds * 1_000_000_000;
        format!(
            "intDiv({}, {}) * {} AS time_bucket",
            time_column, nanos, nanos
        )
    } else {
        // Other tables use DateTime64
        format!(
            "toStartOfInterval({}, INTERVAL {} SECOND) AS time_bucket",
            time_column, seconds
        )
    }
}

fn parse_interval_seconds(interval: &str) -> i64 {
    let len = interval.len();
    if len < 2 {
        return 60; // Default 1 minute
    }

    let (num_str, unit) = interval.split_at(len - 1);
    let num: i64 = num_str.parse().unwrap_or(1);

    match unit {
        "s" => num,
        "m" => num * 60,
        "h" => num * 3600,
        "d" => num * 86400,
        _ => 60,
    }
}

fn parse_time(time_str: &str) -> Result<chrono::DateTime<chrono::Utc>> {
    use chrono::Utc;

    if time_str == "now" {
        return Ok(Utc::now());
    }

    // Handle relative times like 'now-1h', 'now-30m'
    if time_str.starts_with("now-") {
        let duration_str = &time_str[4..];
        let duration = parse_duration(duration_str)?;
        return Ok(Utc::now() - duration);
    }

    if time_str.starts_with("now+") {
        let duration_str = &time_str[4..];
        let duration = parse_duration(duration_str)?;
        return Ok(Utc::now() + duration);
    }

    // Try parsing as ISO timestamp
    chrono::DateTime::parse_from_rfc3339(time_str)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|_| AppError::Validation(format!("Invalid time format: {}", time_str)))
}

fn parse_duration(duration_str: &str) -> Result<chrono::Duration> {
    use chrono::Duration;

    let len = duration_str.len();
    if len < 2 {
        return Err(AppError::Validation(format!(
            "Invalid duration: {}",
            duration_str
        )));
    }

    let (num_str, unit) = duration_str.split_at(len - 1);
    let num: i64 = num_str
        .parse()
        .map_err(|_| AppError::Validation(format!("Invalid duration number: {}", num_str)))?;

    match unit {
        "s" => Ok(Duration::seconds(num)),
        "m" => Ok(Duration::minutes(num)),
        "h" => Ok(Duration::hours(num)),
        "d" => Ok(Duration::days(num)),
        "w" => Ok(Duration::weeks(num)),
        _ => Err(AppError::Validation(format!(
            "Invalid duration unit: {}",
            unit
        ))),
    }
}

/// Validate that a string is safe to use as an identifier value in SQL.
/// Allows alphanumeric characters, hyphens, underscores, dots, and slashes.
fn is_safe_identifier(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.' || c == '/')
}

/// Sanitize a WHERE clause using allowlist-based validation.
///
/// Only allows:
/// - Simple comparisons: column = 'value', column != 'value'
/// - Numeric comparisons: column > 1, column < 100, column >= 1, column <= 100
/// - IN clauses: column IN ('a', 'b', 'c')
/// - LIKE clauses: column LIKE 'pattern%'
/// - AND/OR combinators
///
/// Returns empty string if the clause contains anything suspicious.
fn sanitize_where_clause(clause: &str) -> String {
    use crate::utils::escape_clickhouse_string;

    // Strip quoted string literals first so that values like 'execute_tool'
    // are not mistakenly matched by the dangerous-pattern or column checks.
    let clause_no_strings = {
        let mut s = String::with_capacity(clause.len());
        let mut in_quote = false;
        for ch in clause.chars() {
            if ch == '\'' {
                in_quote = !in_quote;
            } else if !in_quote {
                s.push(ch);
            }
        }
        s
    };

    // Reject if the non-literal portion contains dangerous patterns
    let dangerous_patterns = [
        ";",
        "--",
        "/*",
        "*/",
        "@@",
        "CHAR(",
        "CHR(",
        "CONCAT(",
        "EXEC",
        "EXECUTE",
        "XP_",
        "SP_",
        "0x",
        "\\x",
        "DROP",
        "DELETE",
        "TRUNCATE",
        "INSERT",
        "UPDATE",
        "ALTER",
        "CREATE",
        "GRANT",
        "REVOKE",
        "UNION",
        "INTO OUTFILE",
        "LOAD_FILE",
    ];

    let upper = clause_no_strings.to_uppercase();
    for pattern in dangerous_patterns {
        if upper.contains(pattern) {
            tracing::warn!(
                "WHERE clause rejected due to dangerous pattern: {}",
                pattern
            );
            return String::new();
        }
    }

    // Allowlist of valid column names for ClickHouse tables
    let allowed_columns = [
        // Spans table columns
        "service_name",
        "span_name",
        "span_kind",
        "status_code",
        "status_message",
        "trace_id",
        "span_id",
        "parent_span_id",
        "duration",
        "timestamp",
        "span_attributes",
        "resource_attributes",
        "events",
        "links",
        // Logs table columns
        "level",
        "message",
        "source",
        "template",
        "body",
        "severity_text",
        "severity_number",
        "log_attributes",
        // Metrics table columns
        "metric_name",
        "metric_type",
        "value",
        "fingerprint",
        "temporality",
        "metric_attributes",
        "unix_milli",
        // Metric exemplars table columns
        "exemplar_time_unix_nano",
        "filtered_attributes",
        "inserted_at",
        // Common columns
        "project_id",
        "environment",
        "version",
        "host",
        "region",
    ];
    let words: Vec<&str> = clause_no_strings
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .filter(|w| !w.is_empty())
        .collect();

    for word in &words {
        let lower = word.to_lowercase();
        // Skip SQL keywords and values
        if [
            "and", "or", "in", "like", "not", "is", "null", "true", "false",
        ]
        .contains(&lower.as_str())
        {
            continue;
        }
        // Skip numeric values
        if word.chars().all(|c| c.is_ascii_digit() || c == '.') {
            continue;
        }
        // Check if it looks like a column name (starts with letter, contains only alphanumeric/_)
        if word
            .chars()
            .next()
            .map(|c| c.is_alphabetic())
            .unwrap_or(false)
        {
            if !allowed_columns.contains(&lower.as_str()) {
                tracing::warn!("WHERE clause rejected due to unknown column: {}", word);
                return String::new();
            }
        }
    }

    // Escape string literals in the clause
    // Find patterns like 'value' and escape the content
    let mut result = String::with_capacity(clause.len());
    let mut chars = clause.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '\'' {
            // Start of string literal - collect until closing quote
            let mut literal = String::new();
            while let Some(&next) = chars.peek() {
                chars.next();
                if next == '\'' {
                    // Check for escaped quote ('')
                    if chars.peek() == Some(&'\'') {
                        literal.push('\'');
                        chars.next();
                    } else {
                        break;
                    }
                } else {
                    literal.push(next);
                }
            }
            // Escape the literal content and reconstruct
            result.push('\'');
            result.push_str(&escape_clickhouse_string(&literal));
            result.push('\'');
        } else {
            result.push(c);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    /// Helper: build a minimal SqlQueryConfig for tests.
    fn make_config(
        table: &str,
        select: Vec<SelectField>,
        group_by: Option<Vec<String>>,
        interval: Option<&str>,
        order_by: Option<&str>,
        limit: Option<u32>,
        where_clause: Option<&str>,
    ) -> SqlQueryConfig {
        SqlQueryConfig {
            table: table.to_string(),
            select,
            where_clause: where_clause.map(|s| s.to_string()),
            group_by,
            order_by: order_by.map(|s| s.to_string()),
            limit,
            interval: interval.map(|s| s.to_string()),
            field_overrides: None,
        }
    }

    fn make_time_range() -> TimeRange {
        TimeRange {
            from: "now-1h".to_string(),
            to: "now".to_string(),
        }
    }

    fn field_fn(fn_name: &str, field: Option<&str>, alias: &str) -> SelectField {
        SelectField {
            field: field.map(|s| s.to_string()),
            expr: None,
            fn_name: Some(fn_name.to_string()),
            args: None,
            alias: Some(alias.to_string()),
        }
    }

    fn field_raw(field: &str, alias: &str) -> SelectField {
        SelectField {
            field: Some(field.to_string()),
            expr: None,
            fn_name: None,
            args: None,
            alias: Some(alias.to_string()),
        }
    }

    fn field_expr(expr: &str, alias: &str) -> SelectField {
        SelectField {
            field: None,
            expr: Some(expr.to_string()),
            fn_name: None,
            args: None,
            alias: Some(alias.to_string()),
        }
    }

    fn field_quantile(quantile: f64, field: &str, alias: &str) -> SelectField {
        SelectField {
            field: Some(field.to_string()),
            expr: None,
            fn_name: Some("quantile".to_string()),
            args: Some(vec![serde_json::json!(quantile)]),
            alias: Some(alias.to_string()),
        }
    }

    fn field_count_if(condition: &str, alias: &str) -> SelectField {
        SelectField {
            field: None,
            expr: None,
            fn_name: Some("countIf".to_string()),
            args: Some(vec![serde_json::json!(condition)]),
            alias: Some(alias.to_string()),
        }
    }

    // ── build_select_field tests ──────────────────────────────────────

    #[test]
    fn test_build_select_field_count() {
        let f = field_fn("count", None, "total");
        assert_eq!(build_select_field(&f), "count() AS total");
    }

    #[test]
    fn test_build_select_field_sum() {
        let f = field_fn("sum", Some("duration"), "total_time");
        assert_eq!(build_select_field(&f), "sum(duration) AS total_time");
    }

    #[test]
    fn test_build_select_field_avg() {
        let f = field_fn("avg", Some("value"), "avg_val");
        assert_eq!(build_select_field(&f), "avg(value) AS avg_val");
    }

    #[test]
    fn test_build_select_field_quantile() {
        let f = field_quantile(0.95, "duration", "p95");
        assert_eq!(
            build_select_field(&f),
            "quantile(0.95)(toFloat64(duration)) AS p95"
        );
    }

    #[test]
    fn test_build_select_field_countif() {
        let f = field_count_if("status_code = 'STATUS_CODE_ERROR'", "errors");
        assert_eq!(
            build_select_field(&f),
            "countIf(status_code = 'STATUS_CODE_ERROR') AS errors"
        );
    }

    #[test]
    fn test_build_select_field_raw_field() {
        let f = field_raw("span_attributes['http.route']", "endpoint");
        assert_eq!(
            build_select_field(&f),
            "span_attributes['http.route'] AS endpoint"
        );
    }

    #[test]
    fn test_build_select_field_expression() {
        let f = field_expr("errors / total * 100", "error_rate");
        assert_eq!(build_select_field(&f), "errors / total * 100 AS error_rate");
    }

    // ── parse_interval_seconds tests ──────────────────────────────────

    #[test]
    fn test_parse_interval_seconds() {
        assert_eq!(parse_interval_seconds("1m"), 60);
        assert_eq!(parse_interval_seconds("5m"), 300);
        assert_eq!(parse_interval_seconds("1h"), 3600);
        assert_eq!(parse_interval_seconds("1d"), 86400);
        assert_eq!(parse_interval_seconds("30s"), 30);
    }

    // ── time_bucket_expression tests ──────────────────────────────────

    #[test]
    fn test_time_bucket_spans() {
        let expr = time_bucket_expression("timestamp", "1m", "spans");
        assert_eq!(
            expr,
            "toStartOfInterval(timestamp, INTERVAL 60 SECOND) AS time_bucket"
        );
    }

    #[test]
    fn test_time_bucket_metrics() {
        let expr = time_bucket_expression("unix_milli", "1m", "metrics");
        assert_eq!(expr, "intDiv(unix_milli, 60000) * 60000 AS time_bucket");
    }

    // ── build_clickhouse_query tests ──────────────────────────────────

    #[test]
    fn test_basic_count_query() {
        let config = make_config(
            "spans",
            vec![field_fn("count", None, "total")],
            None, // no group by
            None, // no interval
            None, // no order by
            Some(10),
            None, // no where
        );
        let pid = Uuid::nil();
        let sql =
            build_clickhouse_query(&config, &make_time_range(), &pid, 1000, 100, &None).unwrap();

        assert!(sql.starts_with("SELECT count() AS total FROM reiver.spans WHERE"));
        assert!(sql.contains(&format!("project_id = '{}'", pid)));
        assert!(sql.ends_with("LIMIT 10"));
        // No GROUP BY in output
        assert!(!sql.contains("GROUP BY"));
    }

    #[test]
    fn test_time_series_query_injects_time_bucket() {
        let config = make_config(
            "spans",
            vec![field_fn("count", None, "requests")],
            None, // no explicit group by
            Some("1m"),
            None,
            Some(100),
            None,
        );
        let pid = Uuid::nil();
        let sql =
            build_clickhouse_query(&config, &make_time_range(), &pid, 1000, 100, &None).unwrap();

        // time_bucket should be in both SELECT and GROUP BY
        assert!(sql.contains("toStartOfInterval(timestamp, INTERVAL 60 SECOND) AS time_bucket"));
        assert!(sql
            .contains("GROUP BY toStartOfInterval(timestamp, INTERVAL 60 SECOND) AS time_bucket"));
        assert!(sql.contains("ORDER BY time_bucket ASC"));
    }

    #[test]
    fn test_group_by_does_not_auto_inject_into_select() {
        // This is the key behavioral test: GROUP BY columns should NOT be
        // auto-injected into SELECT. The widget config must explicitly
        // declare all columns it needs in `select`.
        let config = make_config(
            "spans",
            vec![
                field_fn("sum", Some("duration"), "total_time"),
                field_fn("count", None, "requests"),
            ],
            Some(vec!["span_attributes['http.route']".to_string()]),
            None, // no interval
            Some("total_time DESC"),
            Some(20),
            Some("span_kind = 'SPAN_KIND_SERVER'"),
        );
        let pid = Uuid::nil();
        let sql =
            build_clickhouse_query(&config, &make_time_range(), &pid, 1000, 100, &None).unwrap();

        // The SELECT should only contain what we declared
        assert!(sql.contains("sum(duration) AS total_time"));
        assert!(sql.contains("count() AS requests"));
        // GROUP BY column should NOT be auto-injected into SELECT with a sanitized alias
        assert!(!sql.contains("AS httproute"));
        // But GROUP BY itself should be present
        assert!(sql.contains("GROUP BY span_attributes['http.route']"));
    }

    #[test]
    fn test_bar_chart_with_explicit_endpoint_in_select() {
        // When the widget config explicitly includes the endpoint field
        // in its SELECT (the correct pattern after our fix), it should
        // appear in the generated SQL.
        let config = make_config(
            "spans",
            vec![
                field_fn("sum", Some("duration"), "total_time"),
                field_fn("count", None, "requests"),
                field_quantile(0.95, "duration", "p95"),
                field_raw("span_attributes['http.route']", "endpoint"),
            ],
            Some(vec!["span_attributes['http.route']".to_string()]),
            None,
            Some("total_time DESC"),
            Some(20),
            Some("span_kind = 'SPAN_KIND_SERVER' AND span_attributes['http.route'] != ''"),
        );
        let pid = Uuid::nil();
        let sql =
            build_clickhouse_query(&config, &make_time_range(), &pid, 1000, 100, &None).unwrap();

        // Endpoint should be in SELECT via the explicit field declaration
        assert!(sql.contains("span_attributes['http.route'] AS endpoint"));
        assert!(sql.contains("sum(duration) AS total_time"));
        assert!(sql.contains("quantile(0.95)(toFloat64(duration)) AS p95"));
        assert!(sql.contains("GROUP BY span_attributes['http.route']"));
        assert!(sql.contains("ORDER BY total_time DESC"));
        assert!(sql.contains("LIMIT 20"));
    }

    #[test]
    fn test_table_widget_query_with_correct_column_names() {
        // Endpoints table widget: all columns are explicitly declared with
        // the correct aliases (p95_ns, median_ns, total_ns, errors).
        let config = make_config(
            "spans",
            vec![
                field_raw("span_attributes['http.route']", "endpoint"),
                field_fn("count", None, "requests"),
                field_expr("requests / 60", "req_per_min"),
                field_quantile(0.95, "duration", "p95_ns"),
                field_quantile(0.5, "duration", "median_ns"),
                field_fn("sum", Some("duration"), "total_ns"),
                field_count_if("status_code = 'STATUS_CODE_ERROR'", "errors"),
            ],
            Some(vec!["span_attributes['http.route']".to_string()]),
            None,
            Some("total_ns DESC"),
            Some(50),
            Some("span_kind = 'SPAN_KIND_SERVER' AND span_attributes['http.route'] != ''"),
        );
        let pid = Uuid::nil();
        let sql =
            build_clickhouse_query(&config, &make_time_range(), &pid, 1000, 100, &None).unwrap();

        // All columns should appear in SELECT with correct aliases
        assert!(sql.contains("span_attributes['http.route'] AS endpoint"));
        assert!(sql.contains("count() AS requests"));
        assert!(sql.contains("requests / 60 AS req_per_min"));
        assert!(sql.contains("quantile(0.95)(toFloat64(duration)) AS p95_ns"));
        assert!(sql.contains("quantile(0.5)(toFloat64(duration)) AS median_ns"));
        assert!(sql.contains("sum(duration) AS total_ns"));
        assert!(sql.contains("countIf(status_code = 'STATUS_CODE_ERROR') AS errors"));
        assert!(sql.contains("ORDER BY total_ns DESC"));
        assert!(sql.contains("LIMIT 50"));
    }

    #[test]
    fn test_time_series_with_group_by_injects_time_bucket_and_group() {
        // Time series with both interval and group_by
        let config = make_config(
            "spans",
            vec![field_fn("count", None, "errors")],
            Some(vec!["service_name".to_string()]),
            Some("1m"),
            None,
            None,
            Some("status_code = 'STATUS_CODE_ERROR'"),
        );
        let pid = Uuid::nil();
        let sql =
            build_clickhouse_query(&config, &make_time_range(), &pid, 1000, 100, &None).unwrap();

        // time_bucket is injected first in SELECT
        assert!(sql.contains("toStartOfInterval(timestamp, INTERVAL 60 SECOND) AS time_bucket"));
        // GROUP BY has time_bucket first, then the declared group_by column
        assert!(sql.contains("GROUP BY toStartOfInterval(timestamp, INTERVAL 60 SECOND) AS time_bucket, service_name"));
        // service_name is NOT auto-injected into SELECT (no alias generation)
        let select_part = sql.split(" FROM ").next().unwrap();
        assert!(!select_part.contains("service_name AS"));
    }

    #[test]
    fn test_limit_capped_at_max_rows() {
        let config = make_config(
            "spans",
            vec![field_fn("count", None, "total")],
            None,
            None,
            None,
            Some(5000), // exceeds max
            None,
        );
        let pid = Uuid::nil();
        let sql =
            build_clickhouse_query(&config, &make_time_range(), &pid, 1000, 100, &None).unwrap();

        // Limit should be capped at max_rows (1000)
        assert!(sql.ends_with("LIMIT 1000"));
    }

    #[test]
    fn test_default_limit_applied() {
        let config = make_config(
            "spans",
            vec![field_fn("count", None, "total")],
            None,
            None,
            None,
            None, // no explicit limit
            None,
        );
        let pid = Uuid::nil();
        let sql =
            build_clickhouse_query(&config, &make_time_range(), &pid, 1000, 100, &None).unwrap();

        // Default limit should be applied
        assert!(sql.ends_with("LIMIT 100"));
    }

    #[test]
    fn test_metrics_table_uses_unix_milli() {
        let config = make_config(
            "metrics",
            vec![field_fn("avg", Some("value"), "cpu")],
            None,
            Some("1m"),
            None,
            None,
            None,
        );
        let pid = Uuid::nil();
        let sql =
            build_clickhouse_query(&config, &make_time_range(), &pid, 1000, 100, &None).unwrap();

        assert!(sql.contains("reiver.samples_v1"));
        // Metrics use unix_milli-based time bucket
        assert!(sql.contains("intDiv(unix_milli, 60000) * 60000 AS time_bucket"));
    }

    #[test]
    fn test_invalid_table_returns_error() {
        let config = make_config(
            "unknown_table",
            vec![field_fn("count", None, "total")],
            None,
            None,
            None,
            None,
            None,
        );
        let pid = Uuid::nil();
        let result = build_clickhouse_query(&config, &make_time_range(), &pid, 1000, 100, &None);
        assert!(result.is_err());
    }

    // ── parse_time tests ──────────────────────────────────────────────

    #[test]
    fn test_parse_time_now() {
        let result = parse_time("now");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_time_relative() {
        let result = parse_time("now-1h");
        assert!(result.is_ok());
        let dt = result.unwrap();
        let now = chrono::Utc::now();
        // Should be roughly 1 hour ago (within a few seconds)
        let diff = now - dt;
        assert!(diff.num_seconds() >= 3599 && diff.num_seconds() <= 3601);
    }

    #[test]
    fn test_parse_time_iso() {
        let result = parse_time("2026-01-15T12:00:00Z");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_time_invalid() {
        let result = parse_time("not-a-time");
        assert!(result.is_err());
    }

    // ── sanitize_where_clause tests ───────────────────────────────────

    #[test]
    fn test_sanitize_valid_clause() {
        let result = sanitize_where_clause("span_kind = 'SPAN_KIND_SERVER'");
        assert_eq!(result, "span_kind = 'SPAN_KIND_SERVER'");
    }

    #[test]
    fn test_sanitize_rejects_sql_injection() {
        let result = sanitize_where_clause("1=1; DROP TABLE spans");
        assert_eq!(result, "");
    }

    #[test]
    fn test_sanitize_rejects_unknown_column() {
        let result = sanitize_where_clause("evil_column = 'value'");
        assert_eq!(result, "");
    }

    #[test]
    fn test_sanitize_allows_span_attributes() {
        let result = sanitize_where_clause("span_attributes['http.route'] != ''");
        assert!(!result.is_empty());
    }

    #[test]
    fn test_sanitize_allows_execute_tool_in_string_literal() {
        let result = sanitize_where_clause(
            "service_name = 'reiver-mcp' AND span_attributes['gen_ai.operation.name'] = 'execute_tool'"
        );
        assert!(
            !result.is_empty(),
            "execute_tool inside a string literal must not trigger the EXECUTE pattern"
        );
    }

    #[test]
    fn test_sanitize_rejects_execute_outside_literal() {
        let result = sanitize_where_clause("EXECUTE dangerous_func()");
        assert_eq!(result, "");
    }

    // ── service variable injection tests ─────────────────────────────

    #[test]
    fn test_service_variable_injected_for_spans() {
        let config = make_config(
            "spans",
            vec![field_fn("count", None, "total")],
            None,
            None,
            None,
            Some(10),
            None,
        );
        let pid = Uuid::nil();
        let vars = Some(serde_json::json!({"service": "my-service"}));
        let sql =
            build_clickhouse_query(&config, &make_time_range(), &pid, 1000, 100, &vars).unwrap();

        assert!(sql.contains("service_name = 'my-service'"));
    }

    #[test]
    fn test_service_variable_injected_for_metrics() {
        let config = make_config(
            "metrics",
            vec![field_fn("avg", Some("value"), "cpu")],
            None,
            Some("1m"),
            None,
            None,
            None,
        );
        let pid = Uuid::nil();
        let vars = Some(serde_json::json!({"service": "clickhouse"}));
        let sql =
            build_clickhouse_query(&config, &make_time_range(), &pid, 1000, 100, &vars).unwrap();

        assert!(sql.contains("resource_attributes['service.name'] = 'clickhouse'"));
    }

    #[test]
    fn test_service_variable_empty_string_ignored() {
        let config = make_config(
            "spans",
            vec![field_fn("count", None, "total")],
            None,
            None,
            None,
            Some(10),
            None,
        );
        let pid = Uuid::nil();
        let vars = Some(serde_json::json!({"service": ""}));
        let sql =
            build_clickhouse_query(&config, &make_time_range(), &pid, 1000, 100, &vars).unwrap();

        assert!(!sql.contains("service_name ="));
    }

    #[test]
    fn test_service_variable_unsafe_value_rejected() {
        let config = make_config(
            "spans",
            vec![field_fn("count", None, "total")],
            None,
            None,
            None,
            Some(10),
            None,
        );
        let pid = Uuid::nil();
        let vars = Some(serde_json::json!({"service": "'; DROP TABLE spans; --"}));
        let sql =
            build_clickhouse_query(&config, &make_time_range(), &pid, 1000, 100, &vars).unwrap();

        assert!(!sql.contains("service_name ="));
        assert!(!sql.contains("DROP"));
    }

    #[test]
    fn test_is_safe_identifier() {
        assert!(is_safe_identifier("my-service"));
        assert!(is_safe_identifier("watch_worker.v2"));
        assert!(is_safe_identifier("clickhouse"));
        assert!(!is_safe_identifier(""));
        assert!(!is_safe_identifier("bad'; DROP TABLE"));
        assert!(!is_safe_identifier("has spaces"));
    }

    // ── substitute_variables tests ────────────────────────────────────

    #[test]
    fn test_subst_builtin_interval_vars() {
        let result = substitute_variables(
            "rate(m[$__rate_interval]) + rate(m[$__interval]) + m[$__range]",
            &None,
            3600,
            15,
        );
        assert_eq!(result, "rate(m[60s]) + rate(m[15s]) + m[3600s]");
    }

    #[test]
    fn test_subst_dollar_var() {
        let vars = Some(serde_json::json!({"node": "server-1"}));
        let result = substitute_variables(r#"metric{instance=~"$node"}"#, &vars, 3600, 15);
        assert_eq!(result, r#"metric{instance=~"server-1"}"#);
    }

    #[test]
    fn test_subst_braces_var() {
        let vars = Some(serde_json::json!({"node": "server-1"}));
        let result = substitute_variables(r#"metric{instance=~"${node}"}"#, &vars, 3600, 15);
        assert_eq!(result, r#"metric{instance=~"server-1"}"#);
    }

    #[test]
    fn test_subst_braces_regex_var() {
        let vars = Some(serde_json::json!({"node": "server-1"}));
        let result = substitute_variables(r#"metric{instance=~"${node:regex}"}"#, &vars, 3600, 15);
        assert_eq!(result, r#"metric{instance=~"server-1"}"#);
    }

    #[test]
    fn test_subst_double_bracket_var() {
        let vars = Some(serde_json::json!({"node": "server-1"}));
        let result = substitute_variables(r#"metric{instance=~"[[node]]"}"#, &vars, 3600, 15);
        assert_eq!(result, r#"metric{instance=~"server-1"}"#);
    }

    #[test]
    fn test_subst_array_var_joins_with_pipe() {
        let vars = Some(serde_json::json!({"node": ["srv-1", "srv-2", "srv-3"]}));
        let result = substitute_variables(r#"metric{instance=~"$node"}"#, &vars, 3600, 15);
        assert_eq!(result, r#"metric{instance=~"srv-1|srv-2|srv-3"}"#);
    }

    #[test]
    fn test_subst_empty_vars_quoted_fallback() {
        let vars = Some(serde_json::json!({}));
        let result = substitute_variables(r#"metric{instance=~"[[node]]"}"#, &vars, 3600, 15);
        assert_eq!(result, r#"metric{instance=~".*"}"#);
    }

    #[test]
    fn test_subst_empty_vars_bare_by_removed() {
        let vars = Some(serde_json::json!({}));
        let result =
            substitute_variables(r#"sum(metric{}) by ([[aggr_criteria]])"#, &vars, 3600, 15);
        assert_eq!(result, r#"sum(metric{}) by ()"#);
    }

    #[test]
    fn test_subst_empty_vars_by_with_other_label() {
        let vars = Some(serde_json::json!({}));
        let result = substitute_variables(
            r#"sum(metric{}) by (le, [[aggr_criteria]])"#,
            &vars,
            3600,
            15,
        );
        assert_eq!(result, r#"sum(metric{}) by (le)"#);
    }

    #[test]
    fn test_subst_mixed_quoted_and_bare_empty_vars() {
        let vars = Some(serde_json::json!({}));
        let result = substitute_variables(
            r#"sum(metric{instance=~"[[node]]",shard=~"[[shard]]"}) by ([[aggr_criteria]])"#,
            &vars,
            3600,
            15,
        );
        assert_eq!(result, r#"sum(metric{instance=~".*",shard=~".*"}) by ()"#);
    }

    #[test]
    fn test_subst_multiple_quoted_vars_then_bare() {
        let vars = Some(serde_json::json!({}));
        let result = substitute_variables(
            r#"histogram_quantile(0.99, sum(rate(m{instance=~"[[node]]",exported_instance=~"[[exported_node]]",shard=~"[[shard]]",data_cluster=~"[[dc]]"}[5m])) by (le, [[aggr]]))"#,
            &vars,
            3600,
            15,
        );
        assert_eq!(
            result,
            r#"histogram_quantile(0.99, sum(rate(m{instance=~".*",exported_instance=~".*",shard=~".*",data_cluster=~".*"}[5m])) by (le))"#
        );
    }

    #[test]
    fn test_subst_no_vars_object() {
        let result = substitute_variables(
            r#"metric{instance=~"[[node]]"} + rate(m[$__rate_interval])"#,
            &None,
            3600,
            15,
        );
        assert_eq!(result, r#"metric{instance=~".*"} + rate(m[60s])"#);
    }

    #[test]
    fn test_subst_longer_var_name_first() {
        let vars = Some(serde_json::json!({"node": "a", "node_shard": "b"}));
        let result = substitute_variables(r#"m{i=~"$node",s=~"$node_shard"}"#, &vars, 3600, 15);
        assert_eq!(result, r#"m{i=~"a",s=~"b"}"#);
    }

    #[test]
    fn test_subst_var_after_regex_anchor_dollar() {
        let result = substitute_variables(
            r#"m{node=~"^.*$",namespace=~"$NameSpace"}"#,
            &None,
            3600,
            15,
        );
        assert_eq!(result, r#"m{node=~"^.*$",namespace=~".*"}"#);
    }

    // ── ArgoCD dashboard variable patterns ──────────────────────────────
    // Covers the exact query patterns from a real Grafana-imported ArgoCD dashboard
    // to verify that variable substitution works end-to-end with dashboard filters.

    #[test]
    fn test_subst_argocd_namespace_all() {
        let vars = Some(serde_json::json!({
            "namespace": ".*",
            "cluster": ".*",
            "health_status": ".*",
            "sync_status": ".*",
            "interval": "5m",
            "grouping": "namespace"
        }));
        let result = substitute_variables(
            r#"sum(argocd_app_info{namespace=~"$namespace", dest_server=~"$cluster", health_status=~"$health_status", sync_status=~"$sync_status"}) by (namespace)"#,
            &vars,
            3600,
            15,
        );
        assert_eq!(
            result,
            r#"sum(argocd_app_info{namespace=~".*", dest_server=~".*", health_status=~".*", sync_status=~".*"}) by (namespace)"#
        );
    }

    #[test]
    fn test_subst_argocd_namespace_specific() {
        let vars = Some(serde_json::json!({
            "namespace": "argocd",
            "cluster": ".*",
            "health_status": ".*",
            "sync_status": ".*",
            "interval": "5m",
            "grouping": "namespace"
        }));
        let result = substitute_variables(
            r#"sum(argocd_app_info{namespace=~"$namespace", dest_server=~"$cluster", health_status=~"$health_status", sync_status=~"$sync_status"}) by (namespace)"#,
            &vars,
            3600,
            15,
        );
        assert_eq!(
            result,
            r#"sum(argocd_app_info{namespace=~"argocd", dest_server=~".*", health_status=~".*", sync_status=~".*"}) by (namespace)"#
        );
    }

    #[test]
    fn test_subst_argocd_sync_activity_with_interval_and_grouping() {
        let vars = Some(serde_json::json!({
            "namespace": "argocd",
            "cluster": ".*",
            "interval": "10m",
            "grouping": "name"
        }));
        let result = substitute_variables(
            r#"sum(round(increase(argocd_app_sync_total{namespace=~"$namespace", dest_server=~"$cluster"}[$interval]))) by ($grouping)"#,
            &vars,
            3600,
            15,
        );
        assert_eq!(
            result,
            r#"sum(round(increase(argocd_app_sync_total{namespace=~"argocd", dest_server=~".*"}[10m]))) by (name)"#
        );
    }

    #[test]
    fn test_subst_argocd_redis_requests() {
        let vars = Some(serde_json::json!({
            "namespace": "argocd",
            "interval": "5m"
        }));
        let result = substitute_variables(
            r#"sum(increase(argocd_redis_request_total{namespace=~"$namespace"}[$interval])) by (failed)"#,
            &vars,
            3600,
            15,
        );
        assert_eq!(
            result,
            r#"sum(increase(argocd_redis_request_total{namespace=~"argocd"}[5m])) by (failed)"#
        );
    }

    #[test]
    fn test_subst_argocd_health_status_specific() {
        let vars = Some(serde_json::json!({
            "namespace": "argocd",
            "cluster": ".*",
            "health_status": "Healthy",
            "sync_status": "Synced"
        }));
        let result = substitute_variables(
            r#"sum(argocd_app_info{namespace=~"$namespace", dest_server=~"$cluster", health_status=~"$health_status", sync_status=~"$sync_status", health_status!=""}) by (health_status)"#,
            &vars,
            3600,
            15,
        );
        assert_eq!(
            result,
            r#"sum(argocd_app_info{namespace=~"argocd", dest_server=~".*", health_status=~"Healthy", sync_status=~"Synced", health_status!=""}) by (health_status)"#
        );
    }

    #[test]
    fn test_subst_unresolved_interval_in_range_selector() {
        let vars = Some(serde_json::json!({"namespace": "argocd"}));
        let result = substitute_variables(
            r#"sum(increase(m{namespace=~"$namespace"}[$interval])) by (ns)"#,
            &vars,
            3600,
            15,
        );
        assert_eq!(
            result,
            r#"sum(increase(m{namespace=~"argocd"}[15s])) by (ns)"#
        );
    }

    #[test]
    fn test_subst_grouping_as_by_clause_label() {
        let vars = Some(serde_json::json!({"grouping": "project"}));
        let result = substitute_variables(
            r#"sum(m{}) by ($grouping)"#,
            &vars,
            3600,
            15,
        );
        assert_eq!(result, r#"sum(m{}) by (project)"#);
    }
}
