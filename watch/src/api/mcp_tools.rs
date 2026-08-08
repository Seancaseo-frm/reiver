//! MCP Tool usage analytics API.
//!
//! Discovers tools per project from ClickHouse `llm_requests.tool_names`,
//! enriches with blocking status from Postgres guardrails and prompt
//! allowed-tool whitelists, and provides usage statistics.

use axum::{
    extract::{Query, State},
    http::HeaderMap,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::app_state::WatchState;
use crate::error::Result;
use crate::utils::escape_clickhouse_string;

pub fn create_mcp_tools_router() -> Router<Arc<WatchState>> {
    Router::new()
        .route("/tools", get(list_tools))
        .route("/stats", get(get_stats))
        .route("/stats/by-tool", get(stats_by_tool))
        .route("/stats/by-token", get(stats_by_token))
        .route("/stats/timeline", get(stats_timeline))
        .route("/calls", get(recent_calls))
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct TimeRangeQuery {
    #[serde(default = "default_time_range")]
    time_range: String,
}

fn default_time_range() -> String {
    "24h".into()
}

fn time_range_to_interval(tr: &str) -> &str {
    match tr {
        "1h" => "1 HOUR",
        "7d" => "7 DAY",
        "30d" => "30 DAY",
        _ => "1 DAY",
    }
}

fn time_range_to_bucket(tr: &str) -> &str {
    match tr {
        "1h" => "toStartOfFiveMinutes",
        "30d" => "toStartOfDay",
        _ => "toStartOfHour",
    }
}

// ---------------------------------------------------------------------------
// 1. Per-project tool catalog (discovered from llm_requests)
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct PromptRef {
    prompt_id: String,
    prompt_name: String,
}

#[derive(Debug, Serialize)]
struct ToolDef {
    name: String,
    total_calls: u64,
    request_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_used: Option<String>,
    blocked_project_wide: bool,
    blocked_by_prompts: Vec<PromptRef>,
}

async fn list_tools(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<ToolDef>>> {
    let project_id = super::extract_project_id(&headers)?;
    let pid_str = escape_clickhouse_string(&project_id.to_string());

    // 1) Discover tools from ClickHouse llm_requests.tool_names
    let ch_query = format!(
        r#"
        SELECT
            tool_name,
            sum(call_count) AS total_calls,
            count() AS request_count,
            max(last_ts) AS last_used
        FROM (
            SELECT
                arrayJoin(tool_names) AS tool_name,
                tool_call_count AS call_count,
                timestamp AS last_ts
            FROM reiver.llm_requests
            WHERE project_id = '{pid}'
              AND length(tool_names) > 0
              AND timestamp >= now() - INTERVAL 30 DAY
        )
        GROUP BY tool_name
        ORDER BY total_calls DESC
        "#,
        pid = pid_str,
    );

    #[derive(Debug, clickhouse::Row, serde::Deserialize)]
    struct ChToolRow {
        tool_name: String,
        total_calls: u64,
        request_count: u64,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        last_used: chrono::DateTime<chrono::Utc>,
    }

    let ch_rows = state
        .clickhouse
        .query(&ch_query)
        .fetch_all::<ChToolRow>()
        .await
        .map_err(|e| {
            crate::error::AppError::Internal(anyhow::anyhow!("ClickHouse error: {}", e))
        })?;

    if ch_rows.is_empty() {
        return Ok(Json(vec![]));
    }

    // 2) Load project-wide blocked tools from guardrails
    let blocked_tools: Vec<String> = sqlx::query_scalar::<_, String>(
        "SELECT value FROM project_settings WHERE project_id = $1 AND key = 'gateway_guardrails'",
    )
    .bind(project_id)
    .fetch_optional(state.db.as_ref())
    .await
    .ok()
    .flatten()
    .and_then(|json_str| {
        serde_json::from_str::<serde_json::Value>(&json_str)
            .ok()
            .and_then(|v| {
                v.get("blocked_tools")
                    .and_then(|bt| serde_json::from_value::<Vec<String>>(bt.clone()).ok())
            })
    })
    .unwrap_or_default();

    let blocked_lower: Vec<String> = blocked_tools.iter().map(|s| s.to_lowercase()).collect();

    // 3) Load per-prompt allowed_tools whitelists
    #[derive(Debug, sqlx::FromRow)]
    struct PromptAllowedRow {
        prompt_id: Uuid,
        prompt_name: String,
        allowed_tools: serde_json::Value,
    }

    let prompt_rows = sqlx::query_as::<_, PromptAllowedRow>(
        r#"
        SELECT pc.id AS prompt_id, pc.name AS prompt_name, pv.allowed_tools
        FROM llm_prompt_configs pc
        JOIN llm_prompt_versions pv ON pv.id = pc.active_version_id
        WHERE pc.project_id = $1
          AND pv.allowed_tools IS NOT NULL
        "#,
    )
    .bind(project_id)
    .fetch_all(state.db.as_ref())
    .await
    .unwrap_or_default();

    let prompt_whitelists: Vec<(Uuid, String, Vec<String>)> = prompt_rows
        .into_iter()
        .filter_map(|r| {
            serde_json::from_value::<Vec<String>>(r.allowed_tools)
                .ok()
                .map(|tools| (r.prompt_id, r.prompt_name, tools))
        })
        .collect();

    // 4) Build response
    let defs: Vec<ToolDef> = ch_rows
        .into_iter()
        .map(|r| {
            let is_blocked = blocked_lower.contains(&r.tool_name.to_lowercase());

            let blocked_by: Vec<PromptRef> = prompt_whitelists
                .iter()
                .filter(|(_, _, allowed)| {
                    !allowed.iter().any(|a| a.eq_ignore_ascii_case(&r.tool_name))
                })
                .map(|(id, name, _)| PromptRef {
                    prompt_id: id.to_string(),
                    prompt_name: name.clone(),
                })
                .collect();

            ToolDef {
                name: r.tool_name,
                total_calls: r.total_calls,
                request_count: r.request_count,
                last_used: Some(r.last_used.to_rfc3339()),
                blocked_project_wide: is_blocked,
                blocked_by_prompts: blocked_by,
            }
        })
        .collect();

    Ok(Json(defs))
}

// ---------------------------------------------------------------------------
// 2. Aggregate stats
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Default)]
struct AggregateStats {
    total_calls: u64,
    unique_tools: u64,
    avg_duration_ms: f64,
    error_count: u64,
    error_rate: f64,
    auth_failures: u64,
}

async fn get_stats(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Query(q): Query<TimeRangeQuery>,
) -> Result<Json<AggregateStats>> {
    let project_id = super::extract_project_id(&headers)?;
    let interval = time_range_to_interval(&q.time_range);

    let query = format!(
        r#"
        SELECT
            count() as total_calls,
            uniq(span_attributes['tool_name']) as unique_tools,
            avg(duration / 1000000) as avg_duration_ms,
            countIf(status_code = 'STATUS_CODE_ERROR') as error_count
        FROM reiver.spans
        WHERE project_id = '{pid}'
          AND service_name = 'reiver-mcp'
          AND span_name = 'mcp.tool.call'
          AND timestamp >= now() - INTERVAL {interval}
        "#,
        pid = escape_clickhouse_string(&project_id.to_string()),
        interval = interval,
    );

    #[derive(Debug, clickhouse::Row, serde::Deserialize)]
    struct Row {
        total_calls: u64,
        unique_tools: u64,
        avg_duration_ms: f64,
        error_count: u64,
    }

    let row = state
        .clickhouse
        .query(&query)
        .fetch_optional::<Row>()
        .await
        .map_err(|e| {
            crate::error::AppError::Internal(anyhow::anyhow!("ClickHouse error: {}", e))
        })?;

    let r = row.unwrap_or(Row {
        total_calls: 0,
        unique_tools: 0,
        avg_duration_ms: 0.0,
        error_count: 0,
    });

    let stats = AggregateStats {
        total_calls: r.total_calls,
        unique_tools: r.unique_tools,
        avg_duration_ms: r.avg_duration_ms,
        error_count: r.error_count,
        error_rate: if r.total_calls > 0 {
            r.error_count as f64 / r.total_calls as f64
        } else {
            0.0
        },
        auth_failures: 0,
    };

    Ok(Json(stats))
}

// ---------------------------------------------------------------------------
// 3. Per-tool breakdown
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct ToolStats {
    tool_name: String,
    call_count: u64,
    avg_duration_ms: f64,
    error_count: u64,
    p95_duration_ms: f64,
}

async fn stats_by_tool(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Query(q): Query<TimeRangeQuery>,
) -> Result<Json<Vec<ToolStats>>> {
    let project_id = super::extract_project_id(&headers)?;
    let interval = time_range_to_interval(&q.time_range);

    let query = format!(
        r#"
        SELECT
            span_attributes['tool_name'] as tool_name,
            count() as call_count,
            avg(duration / 1000000) as avg_duration_ms,
            countIf(status_code = 'STATUS_CODE_ERROR') as error_count,
            quantile(0.95)(duration / 1000000) as p95_duration_ms
        FROM reiver.spans
        WHERE project_id = '{pid}'
          AND service_name = 'reiver-mcp'
          AND span_name = 'mcp.tool.call'
          AND timestamp >= now() - INTERVAL {interval}
        GROUP BY tool_name
        ORDER BY call_count DESC
        "#,
        pid = escape_clickhouse_string(&project_id.to_string()),
        interval = interval,
    );

    #[derive(Debug, clickhouse::Row, serde::Deserialize)]
    struct Row {
        tool_name: String,
        call_count: u64,
        avg_duration_ms: f64,
        error_count: u64,
        p95_duration_ms: f64,
    }

    let rows = state
        .clickhouse
        .query(&query)
        .fetch_all::<Row>()
        .await
        .map_err(|e| {
            crate::error::AppError::Internal(anyhow::anyhow!("ClickHouse error: {}", e))
        })?;

    let stats: Vec<ToolStats> = rows
        .into_iter()
        .map(|r| ToolStats {
            tool_name: r.tool_name,
            call_count: r.call_count,
            avg_duration_ms: r.avg_duration_ms,
            error_count: r.error_count,
            p95_duration_ms: r.p95_duration_ms,
        })
        .collect();

    Ok(Json(stats))
}

// ---------------------------------------------------------------------------
// 4. Per-token breakdown
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct TokenStats {
    key_prefix: String,
    key_label: String,
    call_count: u64,
    last_used: String,
    tools_used: Vec<String>,
}

async fn stats_by_token(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Query(q): Query<TimeRangeQuery>,
) -> Result<Json<Vec<TokenStats>>> {
    let project_id = super::extract_project_id(&headers)?;
    let interval = time_range_to_interval(&q.time_range);

    let query = format!(
        r#"
        SELECT
            span_attributes['key_prefix'] as key_prefix,
            span_attributes['key_label'] as key_label,
            count() as call_count,
            max(timestamp) as last_used,
            groupUniqArray(span_attributes['tool_name']) as tools_used
        FROM reiver.spans
        WHERE project_id = '{pid}'
          AND service_name = 'reiver-mcp'
          AND span_name = 'mcp.tool.call'
          AND timestamp >= now() - INTERVAL {interval}
        GROUP BY key_prefix, key_label
        ORDER BY call_count DESC
        "#,
        pid = escape_clickhouse_string(&project_id.to_string()),
        interval = interval,
    );

    #[derive(Debug, clickhouse::Row, serde::Deserialize)]
    struct Row {
        key_prefix: String,
        key_label: String,
        call_count: u64,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        last_used: chrono::DateTime<chrono::Utc>,
        tools_used: Vec<String>,
    }

    let rows = state
        .clickhouse
        .query(&query)
        .fetch_all::<Row>()
        .await
        .map_err(|e| {
            crate::error::AppError::Internal(anyhow::anyhow!("ClickHouse error: {}", e))
        })?;

    let stats: Vec<TokenStats> = rows
        .into_iter()
        .map(|r| TokenStats {
            key_prefix: r.key_prefix,
            key_label: r.key_label,
            call_count: r.call_count,
            last_used: r.last_used.to_rfc3339(),
            tools_used: r.tools_used,
        })
        .collect();

    Ok(Json(stats))
}

// ---------------------------------------------------------------------------
// 5. Usage timeline
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct TimelineQuery {
    #[serde(default = "default_time_range")]
    time_range: String,
    #[serde(default)]
    tool_name: Option<String>,
}

#[derive(Debug, Serialize)]
struct TimelinePoint {
    timestamp: String,
    call_count: u64,
    error_count: u64,
    avg_duration_ms: f64,
}

async fn stats_timeline(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Query(q): Query<TimelineQuery>,
) -> Result<Json<Vec<TimelinePoint>>> {
    let project_id = super::extract_project_id(&headers)?;
    let interval = time_range_to_interval(&q.time_range);
    let bucket_fn = time_range_to_bucket(&q.time_range);

    let tool_filter = if let Some(ref t) = q.tool_name {
        format!(
            "AND span_attributes['tool_name'] = '{}'",
            escape_clickhouse_string(t)
        )
    } else {
        String::new()
    };

    let query = format!(
        r#"
        SELECT
            toDateTime64({bucket_fn}(timestamp), 9) as bucket,
            count() as call_count,
            countIf(status_code = 'STATUS_CODE_ERROR') as error_count,
            avg(duration / 1000000) as avg_duration_ms
        FROM reiver.spans
        WHERE project_id = '{pid}'
          AND service_name = 'reiver-mcp'
          AND span_name = 'mcp.tool.call'
          AND timestamp >= now() - INTERVAL {interval}
          {tool_filter}
        GROUP BY bucket
        ORDER BY bucket ASC
        "#,
        bucket_fn = bucket_fn,
        pid = escape_clickhouse_string(&project_id.to_string()),
        interval = interval,
        tool_filter = tool_filter,
    );

    #[derive(Debug, clickhouse::Row, serde::Deserialize)]
    struct Row {
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        bucket: chrono::DateTime<chrono::Utc>,
        call_count: u64,
        error_count: u64,
        avg_duration_ms: f64,
    }

    let rows = state
        .clickhouse
        .query(&query)
        .fetch_all::<Row>()
        .await
        .map_err(|e| {
            crate::error::AppError::Internal(anyhow::anyhow!("ClickHouse error: {}", e))
        })?;

    let points: Vec<TimelinePoint> = rows
        .into_iter()
        .map(|r| TimelinePoint {
            timestamp: r.bucket.to_rfc3339(),
            call_count: r.call_count,
            error_count: r.error_count,
            avg_duration_ms: r.avg_duration_ms,
        })
        .collect();

    Ok(Json(points))
}

// ---------------------------------------------------------------------------
// 6. Recent tool calls
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct RecentCallsQuery {
    #[serde(default = "default_limit")]
    limit: u32,
    #[serde(default)]
    offset: u32,
    #[serde(default)]
    tool_name: Option<String>,
    #[serde(default)]
    key_prefix: Option<String>,
}

fn default_limit() -> u32 {
    50
}

#[derive(Debug, Serialize)]
struct RecentCall {
    trace_id: String,
    tool_name: String,
    key_prefix: String,
    key_label: String,
    timestamp: String,
    duration_ms: f64,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_message: Option<String>,
}

async fn recent_calls(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Query(q): Query<RecentCallsQuery>,
) -> Result<Json<Vec<RecentCall>>> {
    let project_id = super::extract_project_id(&headers)?;
    let limit = q.limit.min(200);

    let mut filters = Vec::new();
    if let Some(ref t) = q.tool_name {
        filters.push(format!(
            "AND span_attributes['tool_name'] = '{}'",
            escape_clickhouse_string(t)
        ));
    }
    if let Some(ref kp) = q.key_prefix {
        filters.push(format!(
            "AND span_attributes['key_prefix'] = '{}'",
            escape_clickhouse_string(kp)
        ));
    }
    let extra_filters = filters.join(" ");

    let query = format!(
        r#"
        SELECT
            trace_id,
            span_attributes['tool_name'] as tool_name,
            span_attributes['key_prefix'] as key_prefix,
            span_attributes['key_label'] as key_label,
            timestamp,
            duration / 1000000 as duration_ms,
            status_code,
            status_message
        FROM reiver.spans
        WHERE project_id = '{pid}'
          AND service_name = 'reiver-mcp'
          AND span_name = 'mcp.tool.call'
          AND timestamp >= now() - INTERVAL 7 DAY
          {extra_filters}
        ORDER BY timestamp DESC
        LIMIT {limit}
        OFFSET {offset}
        "#,
        pid = escape_clickhouse_string(&project_id.to_string()),
        extra_filters = extra_filters,
        limit = limit,
        offset = q.offset,
    );

    #[derive(Debug, clickhouse::Row, serde::Deserialize)]
    struct Row {
        trace_id: String,
        tool_name: String,
        key_prefix: String,
        key_label: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: chrono::DateTime<chrono::Utc>,
        duration_ms: f64,
        status_code: String,
        status_message: String,
    }

    let rows = state
        .clickhouse
        .query(&query)
        .fetch_all::<Row>()
        .await
        .map_err(|e| {
            crate::error::AppError::Internal(anyhow::anyhow!("ClickHouse error: {}", e))
        })?;

    let calls: Vec<RecentCall> = rows
        .into_iter()
        .map(|r| {
            let is_error = r.status_code == "STATUS_CODE_ERROR";
            RecentCall {
                trace_id: r.trace_id,
                tool_name: r.tool_name,
                key_prefix: r.key_prefix,
                key_label: r.key_label,
                timestamp: r.timestamp.to_rfc3339(),
                duration_ms: r.duration_ms,
                status: if is_error {
                    "error".into()
                } else {
                    "ok".into()
                },
                error_message: if is_error && !r.status_message.is_empty() {
                    Some(r.status_message)
                } else {
                    None
                },
            }
        })
        .collect();

    Ok(Json(calls))
}
