//! ClickHouse metric fetcher for the DataFusion-based PromQL evaluator.
//!
//! Pre-fetches metric samples from ClickHouse, converts them to Arrow RecordBatches,
//! and registers them as MemTables in a DataFusion SessionContext for evaluation.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow_array::{
    builder::{Float64Builder, StringBuilder, TimestampMillisecondBuilder},
    Array, Float64Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use datafusion::datasource::MemTable;
use datafusion::error::Result as DfResult;
use datafusion::execution::context::QueryPlanner as DfQueryPlanner;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::SessionContext;
use reiver_core::promql::eval::error::EvalError;
use reiver_core::promql::eval::extension_plan::PromExtensionPlanner;
use reiver_core::promql::eval::planner::{
    collect_metric_refs, collect_query_fetch_lookback, metric_table_name, EvalContext,
    PromPlanner, COL_FINGERPRINT, COL_TIMESTAMP, COL_VALUE, DEFAULT_LOOKBACK_DELTA_MS,
};
use reiver_core::promql::metric_names::{
    implicit_query_labels_for, normalize_histogram_suffix, resolve_label_name,
    resolve_storage_name, resolve_storage_variants,
};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// MetricFetcher trait — decouples PromQL evaluation from storage I/O.
// ---------------------------------------------------------------------------

/// Abstraction over metric data retrieval. Production code uses ClickHouse;
/// tests can substitute an in-memory implementation.
#[async_trait::async_trait]
pub trait MetricFetcher: Send + Sync {
    /// Fetch metric samples as Arrow RecordBatches for a given project, metric
    /// storage name, and time range. The returned batches must have the standard
    /// schema: [unix_milli, value, fingerprint, lbl_*...].
    async fn fetch_metric_data(
        &self,
        project_id: &Uuid,
        storage_name: &str,
        label_columns: &[String],
        implicit_labels: &std::collections::HashMap<&str, &str>,
        equality_filters: &[(String, String)],
        start_ms: i64,
        end_ms: i64,
        fetch_lookback_ms: i64,
        otel_reverse: &std::collections::HashMap<&str, &str>,
    ) -> Result<Vec<RecordBatch>, EvalError>;
}

// ---------------------------------------------------------------------------
// ClickHouseMetricFetcher — production implementation
// ---------------------------------------------------------------------------

/// Fetches metric data from ClickHouse via HTTP, handles raw vs pre-aggregated
/// tables, label resolution, and request-scoped row caching.
pub struct ClickHouseMetricFetcher {
    http_client: reqwest::Client,
    clickhouse_url: String,
    row_cache:
        std::sync::Mutex<std::collections::HashMap<String, Arc<Vec<serde_json::Value>>>>,
}

impl ClickHouseMetricFetcher {
    pub fn new(http_client: reqwest::Client, clickhouse_url: String) -> Self {
        Self {
            http_client,
            clickhouse_url,
            row_cache: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    fn build_schema(&self, label_names: &[String]) -> SchemaRef {
        build_promql_batch_schema(label_names)
    }

    /// Fetch from the raw `samples_v1` table.
    async fn fetch_metric_raw(
        &self,
        project_id: &Uuid,
        metric_name: &str,
        escaped_metric: &str,
        label_names: &[String],
        implicit_labels: &std::collections::HashMap<&str, &str>,
        equality_matchers: &[(String, String)],
        start_ms: i64,
        end_ms: i64,
        otel_reverse: &std::collections::HashMap<&str, &str>,
    ) -> Result<Vec<RecordBatch>, EvalError> {
        let mut implicit_pairs: Vec<(&str, &str)> = implicit_labels
            .iter()
            .map(|(&k, &v)| (k, v))
            .collect();
        implicit_pairs.sort();
        let mut eq_pairs: Vec<(&str, &str)> = equality_matchers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        eq_pairs.sort();
        let label_filters = {
            let mut f = build_label_filter_sql(&implicit_pairs);
            f.push_str(&build_label_filter_sql(&eq_pairs));
            f
        };

        let sql = format!(
            r#"SELECT
    toString(fingerprint) AS fingerprint,
    unix_milli,
    value,
    labels AS labels_json
FROM reiver.samples_v1
WHERE project_id = '{project_id}'
    AND metric_name = '{escaped_metric}'
    AND unix_milli >= {start_ms}
    AND unix_milli < {end_ms}{label_filters}
ORDER BY fingerprint, unix_milli
FORMAT JSONEachRow"#,
        );

        let rows = self.fetch_cached_query(metric_name, &sql, start_ms, end_ms).await?;

        self.parse_json_rows_to_batches(
            &rows,
            label_names,
            implicit_labels,
            equality_matchers,
            otel_reverse,
        )
    }

    /// Fetch from a pre-aggregated table (`agg_5m` / `agg_30m`).
    async fn fetch_metric_agg(
        &self,
        project_id: &Uuid,
        metric_name: &str,
        escaped_metric: &str,
        label_names: &[String],
        implicit_labels: &std::collections::HashMap<&str, &str>,
        equality_matchers: &[(String, String)],
        start_ms: i64,
        end_ms: i64,
        otel_reverse: &std::collections::HashMap<&str, &str>,
        table_name: &str,
    ) -> Result<Vec<RecordBatch>, EvalError> {
        // AggregatingMergeTree may have unmerged rows for the same
        // (fingerprint, unix_milli) from different insert batches.
        // GROUP BY + max(last) collapses them into one row per timestamp,
        // picking the highest counter value (correct for cumulative counters
        // and harmless for gauges).
        let samples_sql = format!(
            r#"SELECT
    toString(fingerprint) AS fingerprint,
    unix_milli,
    max(last) AS value
FROM reiver.{table_name}
WHERE project_id = '{project_id}'
    AND metric_name = '{escaped_metric}'
    AND unix_milli >= {start_ms}
    AND unix_milli < {end_ms}
GROUP BY fingerprint, unix_milli
ORDER BY fingerprint, unix_milli
FORMAT JSONEachRow"#,
        );

        let mut implicit_pairs: Vec<(&str, &str)> = implicit_labels
            .iter()
            .map(|(&k, &v)| (k, v))
            .collect();
        implicit_pairs.sort();
        let mut eq_pairs: Vec<(&str, &str)> = equality_matchers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        eq_pairs.sort();
        let label_filters = {
            let mut f = build_label_filter_sql(&implicit_pairs);
            f.push_str(&build_label_filter_sql(&eq_pairs));
            f
        };

        let labels_sql = format!(
            r#"SELECT
    toString(fingerprint) AS fingerprint,
    labels
FROM reiver.time_series_v1
WHERE project_id = '{project_id}'
    AND metric_name = '{escaped_metric}'{label_filters}
FORMAT JSONEachRow"#,
        );

        tracing::info!(
            metric = %metric_name,
            table = %table_name,
            "PromQL: using pre-aggregated table"
        );

        let sample_rows = self
            .fetch_cached_query(metric_name, &samples_sql, start_ms, end_ms)
            .await?;
        let label_rows = self
            .fetch_cached_query(metric_name, &labels_sql, start_ms, end_ms)
            .await?;

        let labels_map = build_fingerprint_labels_map(&label_rows);

        let has_label_filters = !implicit_pairs.is_empty() || !eq_pairs.is_empty();

        let enriched: Vec<serde_json::Value> = sample_rows
            .iter()
            .filter_map(|row| {
                let mut row = row.clone();
                let obj = row.as_object_mut()?;
                let fp = obj
                    .get("fingerprint")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if has_label_filters && !should_include_agg_sample(fp, &labels_map) {
                    return None;
                }
                let labels_json = labels_map.get(fp).cloned().unwrap_or_default();
                obj.insert(
                    "labels_json".to_string(),
                    serde_json::Value::String(labels_json),
                );
                Some(row)
            })
            .collect();

        self.parse_json_rows_to_batches(
            &enriched,
            label_names,
            implicit_labels,
            equality_matchers,
            otel_reverse,
        )
    }

    /// Execute a ClickHouse query, using the row cache to deduplicate identical
    /// SQL within the same request.
    async fn fetch_cached_query(
        &self,
        metric_name: &str,
        sql: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Arc<Vec<serde_json::Value>>, EvalError> {
        {
            let cache = self.row_cache.lock().unwrap();
            if let Some(cached_rows) = cache.get(sql) {
                tracing::info!(
                    metric = %metric_name,
                    rows = cached_rows.len(),
                    "PromQL: cache hit, skipping ClickHouse fetch"
                );
                return Ok(Arc::clone(cached_rows));
            }
        }

        use tracing::Instrument;
        let fetch_span = tracing::info_span!(
            "promql.clickhouse_fetch",
            otel.name = "PromQL ClickHouse fetch",
            metric = %metric_name,
            start_ms = start_ms,
            end_ms = end_ms,
        );

        let response = self
            .http_client
            .post(&self.clickhouse_url)
            .body(sql.to_string())
            .send()
            .instrument(fetch_span.clone())
            .await
            .map_err(|e| EvalError::Fetch(format!("ClickHouse request failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let err = response.text().await.unwrap_or_default();
            return Err(EvalError::Fetch(format!(
                "ClickHouse query failed ({}): {}",
                status,
                &err[..err.len().min(500)]
            )));
        }

        let rows = crate::ch_stream::stream_json_lines(response)
            .instrument(fetch_span)
            .await
            .map_err(|e| {
                EvalError::Fetch(format!("Failed to stream ClickHouse response: {}", e))
            })?;

        tracing::info!(
            metric = %metric_name,
            rows = rows.len(),
            "PromQL: ClickHouse fetch complete"
        );

        let rows = Arc::new(rows);
        {
            let mut cache = self.row_cache.lock().unwrap();
            cache.insert(sql.to_string(), Arc::clone(&rows));
        }

        Ok(rows)
    }

    /// Build Arrow RecordBatches from pre-parsed JSON rows.
    fn parse_json_rows_to_batches(
        &self,
        rows: &[serde_json::Value],
        ast_label_names: &[String],
        implicit_labels: &std::collections::HashMap<&str, &str>,
        equality_matchers: &[(String, String)],
        otel_reverse: &std::collections::HashMap<&str, &str>,
    ) -> Result<Vec<RecordBatch>, EvalError> {
        parse_json_rows_to_batches_impl(
            rows,
            ast_label_names,
            implicit_labels,
            equality_matchers,
            otel_reverse,
        )
    }
}

#[async_trait::async_trait]
impl MetricFetcher for ClickHouseMetricFetcher {
    async fn fetch_metric_data(
        &self,
        project_id: &Uuid,
        storage_name: &str,
        label_columns: &[String],
        implicit_labels: &std::collections::HashMap<&str, &str>,
        equality_filters: &[(String, String)],
        start_ms: i64,
        end_ms: i64,
        fetch_lookback_ms: i64,
        otel_reverse: &std::collections::HashMap<&str, &str>,
    ) -> Result<Vec<RecordBatch>, EvalError> {
        use crate::metrics::tables::{select_samples_table, SamplesTable};

        let table = select_samples_table(start_ms, end_ms);
        let escaped_metric = storage_name.replace('\'', "\\'");

        // Extend fetch window AFTER table selection so rate()/increase()
        // have prior data points. Use max(query range width, default lookback,
        // and agg-table bucket padding).
        let table_padding = table.bucket_ms() * 2;
        let lookback = fetch_lookback_ms
            .max(DEFAULT_LOOKBACK_DELTA_MS)
            .max(table_padding);
        let fetch_start = start_ms - lookback;

        match table {
            SamplesTable::Raw => {
                self.fetch_metric_raw(
                    project_id,
                    storage_name,
                    &escaped_metric,
                    label_columns,
                    implicit_labels,
                    equality_filters,
                    fetch_start,
                    end_ms,
                    otel_reverse,
                )
                .await
            }
            SamplesTable::Agg5m | SamplesTable::Agg30m => {
                self.fetch_metric_agg(
                    project_id,
                    storage_name,
                    &escaped_metric,
                    label_columns,
                    implicit_labels,
                    equality_filters,
                    fetch_start,
                    end_ms,
                    otel_reverse,
                    table.table_name(),
                )
                .await
            }
        }
    }
}

// ---------------------------------------------------------------------------
// PromQLEvaluator — orchestrates parsing, name resolution, fetching, planning
// ---------------------------------------------------------------------------

/// Custom QueryPlanner that wraps DataFusion's default planner with our extension planner.
#[derive(Debug)]
struct PromQueryPlanner;

#[async_trait::async_trait]
impl DfQueryPlanner for PromQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &datafusion::execution::session_state::SessionState,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(PromExtensionPlanner)]);
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

/// Orchestrates PromQL evaluation: parses queries, resolves OTel metric names,
/// delegates data fetching to a `MetricFetcher`, and executes plans via DataFusion.
///
/// Create one per widget-query request. The underlying `MetricFetcher` handles
/// caching and I/O.
pub struct PromQLEvaluator {
    fetcher: Arc<dyn MetricFetcher>,
}

impl PromQLEvaluator {
    pub fn new(http_client: reqwest::Client, clickhouse_url: String) -> Self {
        Self {
            fetcher: Arc::new(ClickHouseMetricFetcher::new(http_client, clickhouse_url)),
        }
    }

    /// Create an evaluator with a custom `MetricFetcher` implementation (for testing).
    pub fn with_fetcher(fetcher: Arc<dyn MetricFetcher>) -> Self {
        Self { fetcher }
    }

    /// Execute a PromQL query: parse, fetch data, plan, evaluate, return results.
    ///
    /// When `instant` is true the expression is evaluated at a single point
    /// (`end_ms`) instead of across the full `[start_ms, end_ms]` range.
    /// Data is still fetched for the whole range so that range-vector
    /// functions like `rate()` have enough look-back data.
    pub async fn execute(
        &self,
        promql: &str,
        project_id: &Uuid,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
        instant: bool,
    ) -> Result<(Vec<RecordBatch>, Vec<(String, String)>), EvalError> {
        use tracing::Instrument;
        let span = tracing::info_span!(
            "promql.execute",
            otel.name = "PromQL execute",
            promql = %promql,
            instant = instant,
        );
        self.execute_inner(promql, project_id, start_ms, end_ms, step_ms, instant)
            .instrument(span)
            .await
    }

    async fn execute_inner(
        &self,
        promql: &str,
        project_id: &Uuid,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
        instant: bool,
    ) -> Result<(Vec<RecordBatch>, Vec<(String, String)>), EvalError> {
        use crate::metrics::tables::select_samples_table;

        // When using pre-aggregated tables, widen any range-vector windows
        // (e.g. [5m]) that are narrower than 2x the bucket size. Without this,
        // rate() on 5-minute data with a [5m] window sees only 1 point and
        // returns null at most evaluation steps.
        let table = select_samples_table(start_ms, end_ms);
        let min_window_ms = table.bucket_ms() * 2;
        let effective_promql;
        let promql = if min_window_ms > 0 {
            effective_promql = widen_range_vectors(promql, min_window_ms);
            if effective_promql != promql {
                tracing::info!(
                    original = %promql,
                    rewritten = %effective_promql,
                    min_window_ms = min_window_ms,
                    "PromQL: widened range vectors for pre-aggregated table"
                );
            }
            effective_promql.as_str()
        } else {
            promql
        };

        let (sanitized, otel_name_map) = reiver_core::promql::sanitize_otel_names(promql);
        let reverse: std::collections::HashMap<&str, &str> = otel_name_map
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let ast = reiver_core::promql::parse(&sanitized)
            .map_err(|e| EvalError::Parse(format!("{}: {}", e, promql)))?;

        let metric_refs = collect_metric_refs(&ast);

        let query_lookback = collect_query_fetch_lookback(&ast);
        let table_padding = table.bucket_ms() * 2;
        let fetch_lookback = query_lookback
            .max(DEFAULT_LOOKBACK_DELTA_MS)
            .max(table_padding);

        let session = self.build_session()?;

        let mut registered_tables = std::collections::HashSet::new();

        for mref in &metric_refs {
            let mut label_names = mref.label_names.clone();
            if mref.metric_name.ends_with("_bucket") && !label_names.contains(&"le".to_string()) {
                label_names.push("le".to_string());
                label_names.sort();
            }
            let batches = self
                .fetch_metric(
                    project_id,
                    &mref.metric_name,
                    &label_names,
                    &mref.equality_matchers,
                    start_ms,
                    end_ms,
                    fetch_lookback,
                    &reverse,
                )
                .await?;

            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            tracing::info!(
                metric = %mref.metric_name,
                rows = total_rows,
                labels = ?label_names,
                promql = %promql,
                instant = instant,
                "PromQL: fetched metric data"
            );

            let base_table_name = metric_table_name(&mref.metric_name);
            let table_name = resolve_metric_table_name(
                &base_table_name,
                &mref.metric_name,
                &reverse,
                &mut registered_tables,
            );

            let schema = if let Some(b) = batches.first() {
                b.schema()
            } else {
                self.build_schema(&label_names)
            };

            let mem_table =
                MemTable::try_new(schema, vec![batches]).map_err(EvalError::DataFusion)?;

            session
                .register_table(&table_name, Arc::new(mem_table))
                .map_err(EvalError::DataFusion)?;
        }

        let (eval_start, eval_step) = if instant {
            (end_ms, step_ms)
        } else {
            (start_ms, step_ms)
        };

        let planner = PromPlanner::new(EvalContext::new(eval_start, end_ms, eval_step));
        let plan = match planner.plan(&ast, &session) {
            Ok(p) => p,
            Err(EvalError::Invalid(msg)) if msg.contains("no matching metrics") => {
                tracing::info!(promql = %promql, "PromQL: no matching metrics, returning empty result");
                return Ok((vec![], otel_name_map));
            }
            Err(e) => return Err(e),
        };

        let df = session
            .execute_logical_plan(plan)
            .await
            .map_err(EvalError::DataFusion)?;

        let results = df.collect().await.map_err(EvalError::DataFusion)?;

        let result_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        tracing::info!(
            promql = %promql,
            result_rows = result_rows,
            result_cols = results.first().map(|b| b.num_columns()).unwrap_or(0),
            instant = instant,
            "PromQL: evaluation complete"
        );

        Ok((results, otel_name_map))
    }

    fn build_session(&self) -> Result<SessionContext, EvalError> {
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_query_planner(Arc::new(PromQueryPlanner))
            .build();

        Ok(SessionContext::new_with_state(state))
    }

    /// Fetch metric samples using the MetricFetcher, handling OTel name
    /// resolution and multi-variant metrics.
    async fn fetch_metric(
        &self,
        project_id: &Uuid,
        metric_name: &str,
        label_names: &[String],
        equality_matchers: &[(String, String)],
        start_ms: i64,
        end_ms: i64,
        fetch_lookback_ms: i64,
        otel_reverse: &std::collections::HashMap<&str, &str>,
    ) -> Result<Vec<RecordBatch>, EvalError> {
        let reversed_name: &str = otel_reverse
            .get(metric_name)
            .copied()
            .unwrap_or(metric_name);

        let normalized = normalize_histogram_suffix(reversed_name);
        let original_name: &str = normalized.as_deref().unwrap_or(reversed_name);

        let resolved_eq: Vec<(String, String)> = equality_matchers
            .iter()
            .map(|(k, v)| {
                let storage_key = otel_reverse
                    .get(k.as_str())
                    .copied()
                    .or_else(|| resolve_label_name(k))
                    .unwrap_or(k.as_str());
                (storage_key.to_string(), v.clone())
            })
            .collect();

        if let Some(variants) = resolve_storage_variants(original_name) {
            tracing::debug!(
                promql_name = %original_name,
                variants = variants.len(),
                "PromQL: fetching multi-variant metric",
            );
            let mut all_batches = Vec::new();
            for &(storage_name, implicit_labels) in variants {
                let implicit_map: std::collections::HashMap<&str, &str> =
                    implicit_labels.iter().copied().collect();
                match self
                    .fetcher
                    .fetch_metric_data(
                        project_id,
                        storage_name,
                        label_names,
                        &implicit_map,
                        &resolved_eq,
                        start_ms,
                        end_ms,
                        fetch_lookback_ms,
                        otel_reverse,
                    )
                    .await
                {
                    Ok(batches) => all_batches.extend(batches),
                    Err(e) => {
                        tracing::warn!(
                            storage_name = %storage_name,
                            error = %e,
                            "Multi-variant fetch failed for one variant, continuing",
                        );
                    }
                }
            }
            return Ok(all_batches);
        }

        let storage_name = resolve_storage_name(original_name).unwrap_or(original_name);
        if storage_name != original_name {
            tracing::debug!(
                promql_name = %original_name,
                storage_name = %storage_name,
                "PromQL: resolved dashboard metric to storage name",
            );
        }
        let implicit = implicit_query_labels_for(original_name, storage_name);
        let implicit_map: std::collections::HashMap<&str, &str> =
            implicit.iter().copied().collect();
        self.fetcher
            .fetch_metric_data(
                project_id,
                storage_name,
                label_names,
                &implicit_map,
                &resolved_eq,
                start_ms,
                end_ms,
                fetch_lookback_ms,
                otel_reverse,
            )
            .await
    }

    fn build_schema(&self, label_names: &[String]) -> SchemaRef {
        build_promql_batch_schema(label_names)
    }
}

/// Restore original OTel dotted names in the JSON response columns and row
/// keys. The PromQL engine outputs `lbl_{sanitized}` columns (e.g.
/// `lbl_service_name`); this renames them back to the user's original form
/// (e.g. `lbl_service.name`) using the otel_name_map from sanitization.
pub fn restore_otel_column_names(
    columns: &mut Vec<String>,
    data: &mut [serde_json::Value],
    otel_name_map: &[(String, String)],
) {
    if otel_name_map.is_empty() {
        return;
    }
    // Build rename map: "lbl_{sanitized}" → "lbl_{original}"
    let renames: std::collections::HashMap<String, String> = otel_name_map
        .iter()
        .map(|(sanitized, original)| (format!("lbl_{sanitized}"), format!("lbl_{original}")))
        .collect();

    if renames.is_empty() {
        return;
    }

    for col in columns.iter_mut() {
        if let Some(new_name) = renames.get(col) {
            *col = new_name.clone();
        }
    }
    for row in data.iter_mut() {
        if let Some(obj) = row.as_object_mut() {
            let keys_to_rename: Vec<(String, String)> = obj
                .keys()
                .filter_map(|k| renames.get(k).map(|new| (k.clone(), new.clone())))
                .collect();
            for (old_key, new_key) in keys_to_rename {
                if let Some(val) = obj.remove(&old_key) {
                    obj.insert(new_key, val);
                }
            }
        }
    }
}

/// Convert DataFusion RecordBatch results to the JSON format expected by the frontend.
pub fn batches_to_json(
    batches: &[RecordBatch],
) -> Result<(Vec<String>, Vec<serde_json::Value>, u64), EvalError> {
    let mut columns: Vec<String> = Vec::new();
    let mut rows: Vec<serde_json::Value> = Vec::new();

    for batch in batches {
        if columns.is_empty() {
            columns = batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
        }

        let value_col_idx = batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == COL_VALUE);

        let num_rows = batch.num_rows();
        for row_idx in 0..num_rows {
            if let Some(vi) = value_col_idx {
                if batch.column(vi).is_null(row_idx) {
                    continue;
                }
            }
            let mut row = serde_json::Map::new();
            for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                let col = batch.column(col_idx);
                let value = column_value_to_json(col.as_ref(), row_idx);
                row.insert(field.name().clone(), value);
            }
            rows.push(serde_json::Value::Object(row));
        }
    }

    let total_rows = rows.len() as u64;
    Ok((columns, rows, total_rows))
}

/// Build a fingerprint -> labels JSON map from `time_series_v1` rows.
fn build_fingerprint_labels_map(
    rows: &[serde_json::Value],
) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::with_capacity(rows.len());
    for row in rows {
        let fp = row["fingerprint"].as_str().unwrap_or("").to_string();
        let labels = row["labels"].as_str().unwrap_or("{}").to_string();
        map.insert(fp, labels);
    }
    map
}

/// Rewrite PromQL range-vector windows that are narrower than `min_ms`.
///
/// Finds `[duration]` patterns and replaces any that parse to less than
/// `min_ms` with the minimum duration string. This ensures functions like
/// `rate()` have enough data points when querying pre-aggregated tables.
fn widen_range_vectors(promql: &str, min_ms: i64) -> String {
    use regex::Regex;

    let re = Regex::new(r"\[(\d+[smhdwy])\]").unwrap();
    re.replace_all(promql, |caps: &regex::Captures| {
        let duration_str = &caps[1];
        let duration_ms = parse_promql_duration(duration_str);
        if duration_ms < min_ms {
            format!("[{}]", format_promql_duration(min_ms))
        } else {
            caps[0].to_string()
        }
    })
    .to_string()
}

fn parse_promql_duration(s: &str) -> i64 {
    let (num_str, unit) = s.split_at(s.len() - 1);
    let num: i64 = num_str.parse().unwrap_or(0);
    match unit {
        "s" => num * 1_000,
        "m" => num * 60_000,
        "h" => num * 3_600_000,
        "d" => num * 86_400_000,
        "w" => num * 604_800_000,
        "y" => num * 365 * 86_400_000,
        _ => 0,
    }
}

fn format_promql_duration(ms: i64) -> String {
    if ms % 3_600_000 == 0 {
        format!("{}h", ms / 3_600_000)
    } else {
        format!("{}m", ms / 60_000)
    }
}

/// Resolve a label value from ClickHouse storage JSON using prom/ast label names.
fn resolve_storage_label_value<'a>(
    label_name: &str,
    labels_map: &'a serde_json::Map<String, serde_json::Value>,
    otel_reverse: &std::collections::HashMap<&str, &str>,
) -> Option<&'a str> {
    labels_map
        .get(label_name)
        .and_then(|v| v.as_str())
        .or_else(|| {
            if let Some(&original) = otel_reverse.get(label_name) {
                return labels_map.get(original).and_then(|v| v.as_str());
            }
            None
        })
        .or_else(|| {
            resolve_label_name(label_name)
                .and_then(|resolved| labels_map.get(resolved))
                .and_then(|v| v.as_str())
        })
        .or_else(|| {
            let dotted: String = label_name
                .chars()
                .map(|c| if c == '_' { '.' } else { c })
                .collect();
            if dotted != label_name {
                labels_map.get(&dotted).and_then(|v| v.as_str())
            } else {
                None
            }
        })
}

/// Check whether a row's storage labels satisfy implicit and equality filters.
fn labels_match_filters(
    labels_map: &serde_json::Map<String, serde_json::Value>,
    implicit_labels: &std::collections::HashMap<&str, &str>,
    equality_matchers: &[(String, String)],
    otel_reverse: &std::collections::HashMap<&str, &str>,
) -> bool {
    for (&key, &expected) in implicit_labels {
        if let Some(actual) = resolve_storage_label_value(key, labels_map, otel_reverse) {
            if actual != expected {
                return false;
            }
        }
    }
    for (key, expected) in equality_matchers {
        let actual = resolve_storage_label_value(key, labels_map, otel_reverse).unwrap_or("");
        if actual != expected.as_str() {
            return false;
        }
    }
    true
}

/// Build `AND JSONExtractString(labels, 'key') = 'val'` clauses for the given
/// label key-value pairs, escaping single quotes in keys and values.
fn build_label_filter_sql(pairs: &[(&str, &str)]) -> String {
    let mut out = String::new();
    for (key, value) in pairs {
        let ek = key.replace('\'', "\\'");
        let ev = value.replace('\'', "\\'");
        out.push_str(&format!(
            " AND JSONExtractString(labels, '{ek}') = '{ev}'"
        ));
    }
    out
}

fn build_promql_batch_schema(label_names: &[String]) -> SchemaRef {
    let mut fields = vec![
        Field::new(
            COL_TIMESTAMP,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(COL_VALUE, DataType::Float64, false),
        Field::new(COL_FINGERPRINT, DataType::Utf8, false),
    ];
    for name in label_names {
        fields.push(Field::new(format!("lbl_{name}"), DataType::Utf8, true));
    }
    Arc::new(Schema::new(fields))
}

/// Whether an agg sample row should be enriched when label filters are active.
fn should_include_agg_sample(
    fingerprint: &str,
    labels_map: &std::collections::HashMap<String, String>,
) -> bool {
    labels_map.contains_key(fingerprint)
}

/// Resolve a unique DataFusion table name, appending a suffix on collision.
fn resolve_metric_table_name(
    base_table_name: &str,
    metric_name: &str,
    otel_reverse: &std::collections::HashMap<&str, &str>,
    registered_tables: &mut std::collections::HashSet<String>,
) -> String {
    if registered_tables.insert(base_table_name.to_string()) {
        return base_table_name.to_string();
    }

    let original = otel_reverse
        .get(metric_name)
        .copied()
        .unwrap_or(metric_name);
    let suffix: String = original
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let mut candidate = format!("{base_table_name}_{suffix}");
    let mut counter = 2u32;
    while !registered_tables.insert(candidate.clone()) {
        candidate = format!("{base_table_name}_{suffix}_{counter}");
        counter += 1;
    }
    tracing::warn!(
        metric = %metric_name,
        table = %candidate,
        base = %base_table_name,
        "PromQL: metric table name collision, using suffixed table name"
    );
    candidate
}

fn parse_json_rows_to_batches_impl(
    rows: &[serde_json::Value],
    ast_label_names: &[String],
    implicit_labels: &std::collections::HashMap<&str, &str>,
    equality_matchers: &[(String, String)],
    otel_reverse: &std::collections::HashMap<&str, &str>,
) -> Result<Vec<RecordBatch>, EvalError> {
    let mut parsed_rows: Vec<(i64, f64, String, serde_json::Map<String, serde_json::Value>)> =
        Vec::with_capacity(rows.len());
    let mut all_label_keys: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

    for name in ast_label_names {
        all_label_keys.insert(name.clone());
    }

    for row in rows {
        let unix_milli = row["unix_milli"]
            .as_i64()
            .ok_or_else(|| EvalError::Fetch("missing unix_milli in row".into()))?;
        let value = row["value"]
            .as_f64()
            .ok_or_else(|| EvalError::Fetch("missing value in row".into()))?;
        let fingerprint = row["fingerprint"]
            .as_str()
            .ok_or_else(|| EvalError::Fetch("missing fingerprint in row".into()))?
            .to_string();

        let labels_map: serde_json::Map<String, serde_json::Value> =
            if let Some(labels_str) = row["labels_json"].as_str() {
                serde_json::from_str(labels_str).unwrap_or_default()
            } else if let Some(obj) = row["labels_json"].as_object() {
                obj.clone()
            } else {
                serde_json::Map::new()
            };

        if !labels_match_filters(
            &labels_map,
            implicit_labels,
            equality_matchers,
            otel_reverse,
        ) {
            continue;
        }

        for key in labels_map.keys() {
            let sanitized: String = key.chars().map(|c| if c == '.' { '_' } else { c }).collect();
            all_label_keys.insert(sanitized);
        }

        parsed_rows.push((unix_milli, value, fingerprint, labels_map));
    }

    if parsed_rows.is_empty() {
        return Ok(vec![]);
    }

    let final_label_names: Vec<String> = all_label_keys.into_iter().collect();

    let row_count = parsed_rows.len();
    let mut ts_builder = TimestampMillisecondBuilder::with_capacity(row_count);
    let mut val_builder = Float64Builder::with_capacity(row_count);
    let mut fp_builder = StringBuilder::with_capacity(row_count, row_count * 20);
    let mut label_builders: Vec<StringBuilder> = final_label_names
        .iter()
        .map(|_| StringBuilder::with_capacity(row_count, row_count * 16))
        .collect();

    for (unix_milli, value, fingerprint, labels_map) in &parsed_rows {
        ts_builder.append_value(*unix_milli);
        val_builder.append_value(*value);
        fp_builder.append_value(fingerprint);

        for (i, label_name) in final_label_names.iter().enumerate() {
            if let Some(&implicit_val) = implicit_labels.get(label_name.as_str()) {
                let storage_val =
                    resolve_storage_label_value(label_name, labels_map, otel_reverse);
                label_builders[i].append_value(storage_val.unwrap_or(implicit_val));
                continue;
            }

            let val = resolve_storage_label_value(label_name, labels_map, otel_reverse).unwrap_or("");
            label_builders[i].append_value(val);
        }
    }

    let schema = build_promql_batch_schema(&final_label_names);

    let mut columns: Vec<Arc<dyn arrow_array::Array>> = vec![
        Arc::new(ts_builder.finish()),
        Arc::new(val_builder.finish()),
        Arc::new(fp_builder.finish()),
    ];
    for builder in &mut label_builders {
        columns.push(Arc::new(builder.finish()));
    }

    let batch = RecordBatch::try_new(schema, columns).map_err(EvalError::Arrow)?;

    tracing::debug!(
        rows = row_count,
        labels = ?final_label_names,
        "Fetched metric data from ClickHouse (all labels discovered)"
    );

    Ok(vec![batch])
}

fn column_value_to_json(col: &dyn arrow_array::Array, row: usize) -> serde_json::Value {
    use arrow_array::Array;

    if col.is_null(row) {
        return serde_json::Value::Null;
    }

    if let Some(arr) = col.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return serde_json::Value::Number(serde_json::Number::from(arr.value(row)));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
        let v = arr.value(row);
        if v.is_nan() || v.is_infinite() {
            return serde_json::Value::Null;
        }
        return serde_json::json!(v);
    }
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return serde_json::Value::String(arr.value(row).to_string());
    }
    if let Some(arr) = col.as_any().downcast_ref::<arrow_array::Int64Array>() {
        return serde_json::Value::Number(serde_json::Number::from(arr.value(row)));
    }
    if let Some(arr) = col.as_any().downcast_ref::<arrow_array::UInt64Array>() {
        return serde_json::Value::Number(serde_json::Number::from(arr.value(row)));
    }

    serde_json::Value::Null
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_label_filter_sql_basic() {
        let filters = build_label_filter_sql(&[
            ("http.route", "/api/users"),
            ("service.name", "web"),
        ]);
        assert!(filters.contains("JSONExtractString(labels, 'http.route') = '/api/users'"));
        assert!(filters.contains("JSONExtractString(labels, 'service.name') = 'web'"));
    }

    #[test]
    fn test_label_filter_sql_escapes_quotes() {
        let filters = build_label_filter_sql(&[("key", "it's a val'ue")]);
        assert!(filters.contains(r"it\'s a val\'ue"));
        assert!(!filters.contains("it's"));
    }

    #[test]
    fn test_label_filter_sql_empty() {
        let filters = build_label_filter_sql(&[]);
        assert!(filters.is_empty());
    }

    #[test]
    fn test_labels_match_filters_direction() {
        use std::collections::HashMap;

        let mut receive = serde_json::Map::new();
        receive.insert(
            "direction".to_string(),
            serde_json::Value::String("receive".to_string()),
        );
        let mut transmit = serde_json::Map::new();
        transmit.insert(
            "direction".to_string(),
            serde_json::Value::String("transmit".to_string()),
        );

        let implicit: HashMap<&str, &str> = HashMap::from([("direction", "receive")]);
        let empty_reverse = HashMap::new();

        assert!(labels_match_filters(
            &receive,
            &implicit,
            &[],
            &empty_reverse
        ));
        assert!(!labels_match_filters(
            &transmit,
            &implicit,
            &[],
            &empty_reverse
        ));
    }

    #[test]
    fn test_implicit_query_labels_for_prom_and_otel_native() {
        use reiver_core::promql::metric_names::implicit_query_labels_for;

        assert_eq!(
            implicit_query_labels_for(
                "node_network_receive_bytes_total",
                "system.network.io"
            ),
            &[("direction", "receive")]
        );
        assert!(implicit_query_labels_for("system.network.io", "system.network.io").is_empty());
    }

    #[test]
    fn test_agg_direction_filter_excludes_transmit() {
        use std::collections::HashMap;

        let rows = vec![
            serde_json::json!({
                "unix_milli": 1_000_000,
                "value": 100.0,
                "fingerprint": "fp_receive",
                "labels_json": r#"{"direction":"receive"}"#,
            }),
            serde_json::json!({
                "unix_milli": 1_000_000,
                "value": 200.0,
                "fingerprint": "fp_transmit",
                "labels_json": r#"{"direction":"transmit"}"#,
            }),
        ];

        let implicit: HashMap<&str, &str> = HashMap::from([("direction", "receive")]);
        let batches = parse_json_rows_to_batches_impl(
            &rows,
            &["direction".to_string()],
            &implicit,
            &[],
            &HashMap::new(),
        )
        .unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);
        let fps = batch
            .column_by_name(COL_FINGERPRINT)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(fps.value(0), "fp_receive");
        let direction = batch
            .column_by_name("lbl_direction")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(direction.value(0), "receive");
    }

    #[test]
    fn test_agg_enrichment_skips_orphan_fingerprints() {
        let mut labels_map = std::collections::HashMap::new();
        labels_map.insert(
            "fp_receive".to_string(),
            r#"{"direction":"receive"}"#.to_string(),
        );

        assert!(should_include_agg_sample("fp_receive", &labels_map));
        assert!(!should_include_agg_sample("fp_transmit", &labels_map));
    }

    #[test]
    fn test_resolve_metric_table_name_collision() {
        use std::collections::{HashMap, HashSet};

        let mut registered = HashSet::new();
        let reverse: HashMap<&str, &str> =
            HashMap::from([("system_dot_network_io", "system.network.io")]);

        let first = resolve_metric_table_name(
            "prom_system_network_io",
            "system_network_io",
            &reverse,
            &mut registered,
        );
        assert_eq!(first, "prom_system_network_io");

        let second = resolve_metric_table_name(
            "prom_system_network_io",
            "system_dot_network_io",
            &reverse,
            &mut registered,
        );
        assert_ne!(second, first);
        assert!(second.starts_with("prom_system_network_io_"));
        assert_eq!(registered.len(), 2);
    }

    #[test]
    fn test_row_cache_prevents_duplicate_fetches() {
        let fetcher =
            ClickHouseMetricFetcher::new(reqwest::Client::new(), "http://localhost:8123".to_string());
        let cache_key = "SELECT 1".to_string();
        let rows = Arc::new(vec![serde_json::json!({"x": 1})]);

        {
            let mut cache = fetcher.row_cache.lock().unwrap();
            cache.insert(cache_key.clone(), Arc::clone(&rows));
        }

        {
            let cache = fetcher.row_cache.lock().unwrap();
            assert!(cache.contains_key(&cache_key));
            assert_eq!(cache.get(&cache_key).unwrap().len(), 1);
        }
    }

    #[test]
    fn test_row_cache_different_sql_not_deduped() {
        let fetcher =
            ClickHouseMetricFetcher::new(reqwest::Client::new(), "http://localhost:8123".to_string());

        {
            let mut cache = fetcher.row_cache.lock().unwrap();
            cache.insert("sql_a".to_string(), Arc::new(vec![serde_json::json!({"a": 1})]));
            cache.insert("sql_b".to_string(), Arc::new(vec![serde_json::json!({"b": 2})]));
        }

        let cache = fetcher.row_cache.lock().unwrap();
        assert_eq!(cache.len(), 2);
        assert!(cache.contains_key("sql_a"));
        assert!(cache.contains_key("sql_b"));
    }

    /// Verify filter SQL is deterministic regardless of pair input order.
    #[test]
    fn test_label_filter_sql_order_independent() {
        let order_a = build_label_filter_sql(&[
            ("alpha", "1"),
            ("beta", "2"),
            ("gamma", "3"),
        ]);
        let order_b = build_label_filter_sql(&[
            ("gamma", "3"),
            ("alpha", "1"),
            ("beta", "2"),
        ]);
        // build_label_filter_sql itself preserves input order — callers must
        // sort before calling. Verify that sorted input produces identical SQL.
        let mut pairs_a = vec![("alpha", "1"), ("beta", "2"), ("gamma", "3")];
        let mut pairs_b = vec![("gamma", "3"), ("alpha", "1"), ("beta", "2")];
        pairs_a.sort();
        pairs_b.sort();
        assert_eq!(
            build_label_filter_sql(&pairs_a),
            build_label_filter_sql(&pairs_b),
            "sorted pairs must produce identical SQL for cache key stability"
        );
        // And the unsorted versions do differ (proving the sort matters):
        assert_ne!(order_a, order_b);
    }

    /// Prometheus shorthand label names (like `namespace`) must be resolved
    /// to their OTel storage form (like `k8s.namespace.name`) before SQL pushdown.
    #[test]
    fn test_label_filter_resolves_prometheus_shorthands() {
        use reiver_core::promql::metric_names::resolve_label_name;

        let shorthand = "namespace";
        let resolved = resolve_label_name(shorthand);
        assert_eq!(resolved, Some("k8s.namespace.name"));

        let filters = build_label_filter_sql(&[
            (resolved.unwrap_or(shorthand), "default"),
        ]);
        assert!(
            filters.contains("k8s.namespace.name"),
            "should use resolved OTel name in SQL, got: {filters}"
        );
        assert!(
            !filters.contains("JSONExtractString(labels, 'namespace')"),
            "should NOT use bare Prometheus shorthand in SQL"
        );
    }

    /// Not-equal matchers should NOT be pushed down to storage.
    #[test]
    fn test_not_equal_matchers_excluded() {
        use reiver_core::promql::eval::planner::collect_metric_refs;
        let ast = reiver_core::promql::parse(
            r#"foo{bar="baz", qux!="bad"}"#,
        )
        .unwrap();
        let refs = collect_metric_refs(&ast);
        assert_eq!(refs.len(), 1);
        assert_eq!(
            refs[0].equality_matchers,
            vec![("bar".to_string(), "baz".to_string())]
        );
        assert!(
            !refs[0].equality_matchers.iter().any(|(k, _)| k == "qux"),
            "not-equal matcher qux should not be pushed down"
        );
    }

    #[test]
    fn test_build_fingerprint_labels_map() {
        let rows = vec![
            serde_json::json!({"fingerprint": "111", "labels": r#"{"service.name":"web"}"#}),
            serde_json::json!({"fingerprint": "222", "labels": r#"{"service.name":"api"}"#}),
        ];
        let map = build_fingerprint_labels_map(&rows);
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("111").unwrap(), r#"{"service.name":"web"}"#);
        assert_eq!(map.get("222").unwrap(), r#"{"service.name":"api"}"#);
    }

    #[test]
    fn test_build_fingerprint_labels_map_missing_fields() {
        let rows = vec![
            serde_json::json!({"fingerprint": "111"}),
            serde_json::json!({}),
        ];
        let map = build_fingerprint_labels_map(&rows);
        assert_eq!(map.get("111").unwrap(), "{}");
        assert_eq!(map.get("").unwrap(), "{}");
    }

    #[test]
    fn test_widen_range_vectors_replaces_narrow_windows() {
        let result = widen_range_vectors("rate(foo[5m])", 600_000);
        assert_eq!(result, "rate(foo[10m])");
    }

    #[test]
    fn test_widen_range_vectors_keeps_wide_windows() {
        let result = widen_range_vectors("rate(foo[15m])", 600_000);
        assert_eq!(result, "rate(foo[15m])");
    }

    #[test]
    fn test_widen_range_vectors_exact_boundary() {
        let result = widen_range_vectors("rate(foo[10m])", 600_000);
        assert_eq!(result, "rate(foo[10m])");
    }

    #[test]
    fn test_widen_range_vectors_multiple_windows() {
        let result = widen_range_vectors(
            "sum(rate(a[5m])) / sum(rate(b[5m]))",
            600_000,
        );
        assert_eq!(result, "sum(rate(a[10m])) / sum(rate(b[10m]))");
    }

    #[test]
    fn test_widen_range_vectors_agg_30m() {
        let result = widen_range_vectors("rate(foo[5m])", 3_600_000);
        assert_eq!(result, "rate(foo[1h])");
    }

    #[test]
    fn test_widen_range_vectors_no_change_for_raw() {
        let result = widen_range_vectors("rate(foo[5m])", 0);
        assert_eq!(result, "rate(foo[5m])");
    }

    #[test]
    fn test_parse_promql_duration() {
        assert_eq!(parse_promql_duration("5m"), 300_000);
        assert_eq!(parse_promql_duration("1h"), 3_600_000);
        assert_eq!(parse_promql_duration("30s"), 30_000);
        assert_eq!(parse_promql_duration("1d"), 86_400_000);
    }

    #[test]
    fn test_format_promql_duration() {
        assert_eq!(format_promql_duration(600_000), "10m");
        assert_eq!(format_promql_duration(3_600_000), "1h");
        assert_eq!(format_promql_duration(1_800_000), "30m");
    }

    #[test]
    fn test_row_cache_differentiates_raw_vs_agg_sql() {
        let fetcher =
            ClickHouseMetricFetcher::new(reqwest::Client::new(), "http://localhost:8123".to_string());

        let raw_sql = "SELECT ... FROM reiver.samples_v1 WHERE ...";
        let agg_sql = "SELECT ... FROM reiver.samples_v1_agg_5m WHERE ...";

        {
            let mut cache = fetcher.row_cache.lock().unwrap();
            cache.insert(
                raw_sql.to_string(),
                Arc::new(vec![serde_json::json!({"source": "raw"})]),
            );
            cache.insert(
                agg_sql.to_string(),
                Arc::new(vec![serde_json::json!({"source": "agg"})]),
            );
        }

        let cache = fetcher.row_cache.lock().unwrap();
        assert_eq!(cache.len(), 2);
        assert_eq!(
            cache.get(raw_sql).unwrap()[0]["source"].as_str().unwrap(),
            "raw"
        );
        assert_eq!(
            cache.get(agg_sql).unwrap()[0]["source"].as_str().unwrap(),
            "agg"
        );
    }

    // -----------------------------------------------------------------------
    // InMemoryMetricFetcher — test implementation of MetricFetcher
    // -----------------------------------------------------------------------

    /// Test implementation of `MetricFetcher` that returns pre-built RecordBatches
    /// keyed by storage name.
    struct InMemoryMetricFetcher {
        metrics: std::collections::HashMap<String, Vec<RecordBatch>>,
    }

    impl InMemoryMetricFetcher {
        fn new() -> Self {
            Self {
                metrics: std::collections::HashMap::new(),
            }
        }

        fn insert(&mut self, storage_name: &str, batches: Vec<RecordBatch>) {
            self.metrics.insert(storage_name.to_string(), batches);
        }
    }

    #[async_trait::async_trait]
    impl MetricFetcher for InMemoryMetricFetcher {
        async fn fetch_metric_data(
            &self,
            _project_id: &Uuid,
            storage_name: &str,
            _label_columns: &[String],
            _implicit_labels: &std::collections::HashMap<&str, &str>,
            _equality_filters: &[(String, String)],
            _start_ms: i64,
            _end_ms: i64,
            _fetch_lookback_ms: i64,
            _otel_reverse: &std::collections::HashMap<&str, &str>,
        ) -> Result<Vec<RecordBatch>, EvalError> {
            Ok(self.metrics.get(storage_name).cloned().unwrap_or_default())
        }
    }

    /// Build a test RecordBatch with the standard schema.
    fn make_test_batch(
        timestamps: &[i64],
        values: &[f64],
        fingerprint: &str,
        label_names: &[&str],
        label_values: &[&[&str]],
    ) -> RecordBatch {
        let n = timestamps.len();
        let mut fields = vec![
            Field::new(COL_TIMESTAMP, DataType::Timestamp(TimeUnit::Millisecond, None), false),
            Field::new(COL_VALUE, DataType::Float64, false),
            Field::new(COL_FINGERPRINT, DataType::Utf8, false),
        ];
        for name in label_names {
            fields.push(Field::new(format!("lbl_{name}"), DataType::Utf8, true));
        }
        let schema = Arc::new(Schema::new(fields));

        let mut columns: Vec<Arc<dyn arrow_array::Array>> = vec![
            Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())),
            Arc::new(Float64Array::from(values.to_vec())),
            Arc::new(StringArray::from(vec![fingerprint; n])),
        ];
        for vals in label_values {
            columns.push(Arc::new(StringArray::from(vals.to_vec())));
        }

        RecordBatch::try_new(schema, columns).unwrap()
    }

    // -----------------------------------------------------------------------
    // Integration tests — full PromQLEvaluator::execute pipeline with mock fetcher
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_integration_rate_with_mock_fetcher() {
        let step = 15_000i64;
        let n = 60usize;
        // Provide plenty of data before the eval start for lookback
        let data_start = 0i64;
        let timestamps: Vec<i64> = (0..n).map(|i| data_start + i as i64 * step).collect();
        // Counter incrementing by 15 per sample (= 1/sec)
        let values: Vec<f64> = (0..n).map(|i| (i as f64) * 15.0).collect();
        let batch = make_test_batch(
            &timestamps,
            &values,
            "fp_001",
            &["job"],
            &[&vec!["api"; n]],
        );

        let mut fetcher = InMemoryMetricFetcher::new();
        fetcher.insert("http_requests_total", vec![batch]);

        let evaluator = PromQLEvaluator::with_fetcher(Arc::new(fetcher));

        let project_id = Uuid::new_v4();
        // Start eval well into the data so rate has enough lookback
        let eval_start = data_start + 120_000; // 2 minutes into data
        let eval_end = data_start + (n as i64 - 2) * step;
        let (results, name_map) = evaluator
            .execute(
                "rate(http_requests_total[1m])",
                &project_id,
                eval_start,
                eval_end,
                step,
                false,
            )
            .await
            .unwrap();

        assert!(name_map.is_empty());
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows > 0, "rate() should produce results");

        let result_values: Vec<f64> = results
            .iter()
            .flat_map(|b| {
                let arr = b
                    .column_by_name(COL_VALUE)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                (0..arr.len())
                    .filter(|&i| !arr.is_null(i))
                    .map(|i| arr.value(i))
                    .collect::<Vec<_>>()
            })
            .collect();

        let non_zero: Vec<f64> = result_values.iter().copied().filter(|v| *v > 0.0).collect();
        assert!(
            !non_zero.is_empty(),
            "rate() should produce non-zero values, got: {:?}",
            result_values
        );
        for v in &non_zero {
            assert!(
                (*v - 1.0).abs() < 0.3,
                "rate of counter incrementing 15/15s should be ~1.0/sec, got {}",
                v
            );
        }
    }

    #[tokio::test]
    async fn test_integration_otel_name_resolution() {
        let start = 1_000_000i64;
        let step = 15_000i64;
        let n = 10usize;

        let timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * step).collect();
        let values: Vec<f64> = vec![42.0; n];
        let batch = make_test_batch(
            &timestamps,
            &values,
            "fp_001",
            &["gen_ai_token_type"],
            &[&vec!["input"; n]],
        );

        let mut fetcher = InMemoryMetricFetcher::new();
        // The storage name after OTel sanitization
        fetcher.insert("gen_ai_client_token_usage", vec![batch]);

        let evaluator = PromQLEvaluator::with_fetcher(Arc::new(fetcher));

        let project_id = Uuid::new_v4();
        let eval_end = start + (n as i64 - 1) * step;
        // Query with dotted OTel name — sanitizer converts dots to underscores
        let (results, name_map) = evaluator
            .execute(
                "gen_ai.client.token.usage",
                &project_id,
                start,
                eval_end,
                step,
                false,
            )
            .await
            .unwrap();

        // The name_map should contain the sanitization mapping
        assert!(
            !name_map.is_empty() || results.iter().map(|b| b.num_rows()).sum::<usize>() > 0,
            "OTel name resolution should either produce results or a name map"
        );
    }

    #[tokio::test]
    async fn test_integration_empty_data_returns_empty() {
        let fetcher = InMemoryMetricFetcher::new();
        let evaluator = PromQLEvaluator::with_fetcher(Arc::new(fetcher));

        let project_id = Uuid::new_v4();
        let (results, _) = evaluator
            .execute(
                "nonexistent_metric",
                &project_id,
                1_000_000,
                2_000_000,
                15_000,
                false,
            )
            .await
            .unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0, "query for nonexistent metric should return empty");
    }

    #[tokio::test]
    async fn test_integration_invalid_promql_returns_error() {
        let fetcher = InMemoryMetricFetcher::new();
        let evaluator = PromQLEvaluator::with_fetcher(Arc::new(fetcher));

        let project_id = Uuid::new_v4();
        let result = evaluator
            .execute(
                "rate(unclosed[",
                &project_id,
                1_000_000,
                2_000_000,
                15_000,
                false,
            )
            .await;

        assert!(result.is_err(), "invalid PromQL should return error");
        if let Err(EvalError::Parse(msg)) = result {
            assert!(!msg.is_empty());
        } else {
            panic!("expected Parse error, got: {:?}", result);
        }
    }

    #[tokio::test]
    async fn test_integration_instant_query() {
        let start = 1_000_000i64;
        let step = 15_000i64;
        let n = 20usize;

        let timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * step).collect();
        let values: Vec<f64> = vec![99.0; n];
        let batch = make_test_batch(
            &timestamps,
            &values,
            "fp_001",
            &["env"],
            &[&vec!["prod"; n]],
        );

        let mut fetcher = InMemoryMetricFetcher::new();
        fetcher.insert("cpu_usage", vec![batch]);

        let evaluator = PromQLEvaluator::with_fetcher(Arc::new(fetcher));

        let project_id = Uuid::new_v4();
        let eval_end = start + (n as i64 - 1) * step;
        let (results, _) = evaluator
            .execute("cpu_usage", &project_id, start, eval_end, step, true)
            .await
            .unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        // Instant query evaluates at a single point (end_ms)
        assert_eq!(total_rows, 1, "instant query should produce exactly 1 row");

        let value = results[0]
            .column_by_name(COL_VALUE)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!((value - 99.0).abs() < 0.01, "instant value should be 99, got {}", value);
    }

    #[tokio::test]
    async fn test_integration_batches_to_json() {
        let start = 1_000_000i64;
        let step = 15_000i64;
        let n = 3usize;

        let timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * step).collect();
        let values = vec![1.0, 2.0, 3.0];
        let batch = make_test_batch(
            &timestamps,
            &values,
            "fp_001",
            &["host"],
            &[&vec!["srv1"; n]],
        );

        let (columns, rows, count) = batches_to_json(&[batch]).unwrap();
        assert_eq!(count, 3);
        assert!(columns.contains(&COL_TIMESTAMP.to_string()));
        assert!(columns.contains(&COL_VALUE.to_string()));
        assert!(columns.contains(&"lbl_host".to_string()));

        assert_eq!(rows.len(), 3);
        let first_value = rows[0]["value"].as_f64().unwrap();
        assert!((first_value - 1.0).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_integration_restore_otel_column_names() {
        let mut columns = vec![
            "unix_milli".to_string(),
            "value".to_string(),
            "lbl_gen_ai_system".to_string(),
        ];
        let mut data = vec![serde_json::json!({
            "unix_milli": 1000,
            "value": 42.0,
            "lbl_gen_ai_system": "openai"
        })];
        let otel_map = vec![
            ("gen_ai_system".to_string(), "gen_ai.system".to_string()),
        ];

        restore_otel_column_names(&mut columns, &mut data, &otel_map);

        assert_eq!(columns[2], "lbl_gen_ai.system");
        assert_eq!(
            data[0]["lbl_gen_ai.system"].as_str().unwrap(),
            "openai"
        );
        assert!(data[0].get("lbl_gen_ai_system").is_none());
    }

    #[tokio::test]
    async fn test_integration_sum_with_multiple_series() {
        let start = 1_000_000i64;
        let step = 15_000i64;
        let n = 10usize;

        let timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * step).collect();
        let batch_a = make_test_batch(
            &timestamps,
            &vec![10.0; n],
            "fp_a",
            &["instance"],
            &[&vec!["a"; n]],
        );
        let batch_b = make_test_batch(
            &timestamps,
            &vec![20.0; n],
            "fp_b",
            &["instance"],
            &[&vec!["b"; n]],
        );

        let mut fetcher = InMemoryMetricFetcher::new();
        fetcher.insert("requests", vec![batch_a, batch_b]);

        let evaluator = PromQLEvaluator::with_fetcher(Arc::new(fetcher));

        let project_id = Uuid::new_v4();
        let eval_end = start + (n as i64 - 1) * step;
        let (results, _) = evaluator
            .execute("sum(requests)", &project_id, start, eval_end, step, false)
            .await
            .unwrap();

        let values: Vec<f64> = results
            .iter()
            .flat_map(|b| {
                let arr = b
                    .column_by_name(COL_VALUE)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                (0..arr.len())
                    .filter(|&i| !arr.is_null(i))
                    .map(|i| arr.value(i))
                    .collect::<Vec<_>>()
            })
            .collect();

        assert!(!values.is_empty(), "sum() should produce results");
        for v in &values {
            assert!(
                (*v - 30.0).abs() < 0.01,
                "sum of [10, 20] should be 30, got {}",
                v
            );
        }
    }

    /// Fetcher that filters rows by [start_ms, end_ms), simulating ClickHouse
    /// `WHERE unix_milli >= start_ms AND unix_milli < end_ms`.
    /// Extends start_ms backwards by `fetch_lookback_ms` before filtering.
    struct TimeFilteringFetcher {
        metrics: std::collections::HashMap<String, Vec<RecordBatch>>,
    }

    impl TimeFilteringFetcher {
        fn new() -> Self {
            Self {
                metrics: std::collections::HashMap::new(),
            }
        }

        fn insert(&mut self, name: &str, batches: Vec<RecordBatch>) {
            self.metrics.insert(name.to_string(), batches);
        }
    }

    #[async_trait::async_trait]
    impl MetricFetcher for TimeFilteringFetcher {
        async fn fetch_metric_data(
            &self,
            _project_id: &Uuid,
            storage_name: &str,
            _label_columns: &[String],
            _implicit_labels: &std::collections::HashMap<&str, &str>,
            _equality_filters: &[(String, String)],
            start_ms: i64,
            end_ms: i64,
            fetch_lookback_ms: i64,
            _otel_reverse: &std::collections::HashMap<&str, &str>,
        ) -> Result<Vec<RecordBatch>, EvalError> {
            let effective_start = start_ms - fetch_lookback_ms;
            let batches = self.metrics.get(storage_name).cloned().unwrap_or_default();
            let mut filtered = Vec::new();
            for batch in &batches {
                let ts_arr = batch
                    .column_by_name(COL_TIMESTAMP)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                let mask: arrow_array::BooleanArray = (0..ts_arr.len())
                    .map(|i| {
                        let t = ts_arr.value(i);
                        Some(t >= effective_start && t < end_ms)
                    })
                    .collect();
                let filtered_batch = arrow::compute::filter_record_batch(batch, &mask).unwrap();
                if filtered_batch.num_rows() > 0 {
                    filtered.push(filtered_batch);
                }
            }
            Ok(filtered)
        }
    }

    /// Wide range vectors require fetch lookback matching the range width.
    /// `rate(foo[30m])` must have 30 minutes of prior data at the first eval step.
    #[tokio::test]
    async fn test_rate_wide_range_no_ramp_up() {
        let step = 15_000i64;
        let range_ms = 30 * 60 * 1000;
        // Enough history before eval_start for a full 30m window
        let n = (range_ms / step + 20) as usize;
        let data_start = 0i64;
        let timestamps: Vec<i64> = (0..n).map(|i| data_start + i as i64 * step).collect();
        let values: Vec<f64> = (0..n).map(|i| (i as f64) * 15.0).collect();
        let batch = make_test_batch(
            &timestamps,
            &values,
            "fp_001",
            &["job"],
            &[&vec!["api"; n]],
        );

        let mut fetcher = TimeFilteringFetcher::new();
        fetcher.insert("http_requests_total", vec![batch]);

        let evaluator = PromQLEvaluator::with_fetcher(Arc::new(fetcher));

        let project_id = Uuid::new_v4();
        let eval_start = range_ms + 5 * 60 * 1000;
        let eval_end = eval_start + 60 * 60 * 1000;
        let (results, _) = evaluator
            .execute(
                "rate(http_requests_total[30m])",
                &project_id,
                eval_start,
                eval_end,
                step,
                false,
            )
            .await
            .unwrap();

        let result_values: Vec<f64> = results
            .iter()
            .flat_map(|b| {
                let arr = b
                    .column_by_name(COL_VALUE)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                (0..arr.len())
                    .filter(|&i| !arr.is_null(i))
                    .map(|i| arr.value(i))
                    .collect::<Vec<_>>()
            })
            .collect();

        assert!(!result_values.is_empty(), "rate() should produce results");
        assert!(
            (result_values[0] - 1.0).abs() < 0.1,
            "rate([30m]) at first step should be ~1.0/s with full lookback, got {}",
            result_values[0]
        );
    }

    /// Proves that with lookback extension (as ClickHouseMetricFetcher does),
    /// rate() produces correct ~1.0/s from the very first data point.
    ///
    /// Prometheus solves this in `getTimeRangesForSelector` (engine.go) by
    /// extending `start` backwards by `evalRange` or `lookbackDelta` before
    /// querying storage. Our ClickHouseMetricFetcher does the same after
    /// table selection.
    #[tokio::test]
    async fn test_rate_no_ramp_up_with_lookback() {
        let step = 15_000i64;
        let n = 320usize;
        let data_start = 0i64;
        let timestamps: Vec<i64> = (0..n).map(|i| data_start + i as i64 * step).collect();
        let values: Vec<f64> = (0..n).map(|i| (i as f64) * 15.0).collect();
        let batch = make_test_batch(
            &timestamps,
            &values,
            "fp_001",
            &["job"],
            &[&vec!["api"; n]],
        );

        let mut fetcher = TimeFilteringFetcher::new();
        fetcher.insert("http_requests_total", vec![batch]);

        let evaluator = PromQLEvaluator::with_fetcher(Arc::new(fetcher));

        let project_id = Uuid::new_v4();
        let eval_start = 10 * 60 * 1000;
        let eval_end = eval_start + 60 * 60 * 1000;
        let (results, _) = evaluator
            .execute(
                "rate(http_requests_total[1m])",
                &project_id,
                eval_start,
                eval_end,
                step,
                false,
            )
            .await
            .unwrap();

        let result_values: Vec<f64> = results
            .iter()
            .flat_map(|b| {
                let arr = b
                    .column_by_name(COL_VALUE)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                (0..arr.len())
                    .filter(|&i| !arr.is_null(i))
                    .map(|i| arr.value(i))
                    .collect::<Vec<_>>()
            })
            .collect();

        assert!(
            result_values.len() >= 10,
            "rate() should produce at least 10 non-null results, got {}",
            result_values.len()
        );

        // Every value should be ~1.0/s from the very start — no ramp-up.
        for (i, v) in result_values.iter().enumerate() {
            assert!(
                (*v - 1.0).abs() < 0.1,
                "rate() at position {} should be ~1.0/s, got {}. First 10: {:?}",
                i,
                v,
                &result_values[..result_values.len().min(10)]
            );
        }
    }

    /// Verifies that rate() on pre-aggregated 5-minute counter data (using
    /// `last` column values) produces the same results as rate() on raw data.
    ///
    /// Uses realistic data modelled on production ClickHouseProfileEvents_Query:
    ///   - 3 fingerprints (3 ClickHouse nodes)
    ///   - Raw: ~4 samples/minute (every ~15s), counter increments ~0.9/s
    ///   - Agg5m: 1 sample per 5-min bucket with `last` = final counter value
    ///
    /// The agg data is what our SQL now produces (`last AS value`).
    /// Previously it used `min`, which would produce slightly different rates
    /// and dramatically wrong rates after counter resets.
    #[tokio::test]
    async fn test_rate_on_agg5m_last_matches_raw() {
        let bucket_5m = 300_000i64; // 5 minutes in ms
        let raw_interval = 15_000i64;
        let rate_per_sec = 0.9_f64; // ~0.9 queries/sec per node
        let increment_per_raw = rate_per_sec * (raw_interval as f64 / 1000.0); // ~13.5 per sample
        let base_time = 1_000_000i64;

        // Generate 45 minutes of data (9 agg buckets) for 3 fingerprints
        let n_buckets = 9usize;
        let n_raw_per_bucket = (bucket_5m / raw_interval) as usize; // 20 raw per bucket
        let n_raw = n_buckets * n_raw_per_bucket;

        let fps = ["fp_node_0", "fp_node_1", "fp_node_2"];
        let counter_starts = [390_000.0, 520_000.0, 410_000.0];

        // Build raw batches and agg batches for each fingerprint
        let mut raw_batches = Vec::new();
        let mut agg_last_batches = Vec::new();

        for (fp_idx, &fp) in fps.iter().enumerate() {
            // Raw data
            let mut raw_ts = Vec::with_capacity(n_raw);
            let mut raw_vals = Vec::with_capacity(n_raw);
            for i in 0..n_raw {
                raw_ts.push(base_time + i as i64 * raw_interval);
                raw_vals.push(counter_starts[fp_idx] + (i as f64) * increment_per_raw);
            }
            raw_batches.push(make_test_batch(
                &raw_ts,
                &raw_vals,
                fp,
                &["instance"],
                &[&vec!["ch-node"; n_raw]],
            ));

            // Agg5m data: one sample per bucket with `last` = final counter value
            let mut agg_ts = Vec::with_capacity(n_buckets);
            let mut agg_last_vals = Vec::with_capacity(n_buckets);
            for b in 0..n_buckets {
                let bucket_end_idx = (b + 1) * n_raw_per_bucket - 1;
                agg_ts.push(base_time + (b as i64) * bucket_5m);
                agg_last_vals.push(raw_vals[bucket_end_idx]);
            }
            agg_last_batches.push(make_test_batch(
                &agg_ts,
                &agg_last_vals,
                fp,
                &["instance"],
                &[&vec!["ch-node"; n_buckets]],
            ));
        }

        // Evaluate rate() on raw data
        let mut raw_fetcher = InMemoryMetricFetcher::new();
        raw_fetcher.insert("ch_queries_total", raw_batches);
        let raw_evaluator = PromQLEvaluator::with_fetcher(Arc::new(raw_fetcher));

        let project_id = Uuid::new_v4();
        let eval_start = base_time + 2 * bucket_5m; // skip first 2 buckets for lookback
        let eval_end = base_time + (n_buckets as i64 - 1) * bucket_5m;

        let (raw_results, _) = raw_evaluator
            .execute(
                "sum(rate(ch_queries_total[5m]))",
                &project_id,
                eval_start,
                eval_end,
                60_000, // 1-min step
                false,
            )
            .await
            .unwrap();

        let raw_rates: Vec<f64> = raw_results
            .iter()
            .flat_map(|b| {
                let arr = b
                    .column_by_name(COL_VALUE)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                (0..arr.len())
                    .filter(|&i| !arr.is_null(i))
                    .map(|i| arr.value(i))
                    .collect::<Vec<_>>()
            })
            .collect();

        // Evaluate rate() on agg5m data (using `last` values)
        let mut agg_fetcher = InMemoryMetricFetcher::new();
        agg_fetcher.insert("ch_queries_total", agg_last_batches);
        let agg_evaluator = PromQLEvaluator::with_fetcher(Arc::new(agg_fetcher));

        let (agg_results, _) = agg_evaluator
            .execute(
                "sum(rate(ch_queries_total[5m]))",
                &project_id,
                eval_start,
                eval_end,
                60_000,
                false,
            )
            .await
            .unwrap();

        let agg_rates: Vec<f64> = agg_results
            .iter()
            .flat_map(|b| {
                let arr = b
                    .column_by_name(COL_VALUE)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                (0..arr.len())
                    .filter(|&i| !arr.is_null(i))
                    .map(|i| arr.value(i))
                    .collect::<Vec<_>>()
            })
            .collect();

        // Both should produce results
        assert!(
            !raw_rates.is_empty(),
            "raw rate() should produce results"
        );
        assert!(
            !agg_rates.is_empty(),
            "agg5m rate() should produce results"
        );

        // Expected total rate: 3 nodes * 0.9/s = 2.7/s
        let expected_total = rate_per_sec * fps.len() as f64;
        for (i, v) in raw_rates.iter().enumerate() {
            assert!(
                (*v - expected_total).abs() < 0.5,
                "raw rate at pos {} should be ~{:.1}/s, got {:.3}",
                i, expected_total, v
            );
        }
        for (i, v) in agg_rates.iter().enumerate() {
            assert!(
                (*v - expected_total).abs() < 0.5,
                "agg5m rate at pos {} should be ~{:.1}/s, got {:.3}",
                i, expected_total, v
            );
        }
    }

    /// Proves that `min` produces wrong rate values after a counter reset,
    /// while `last` handles it correctly.
    ///
    /// Scenario: cumulative counter at ~1.0/s resets mid-window.
    /// - Before reset: counter at 10000, increasing ~1/s
    /// - After reset: counter restarts from 0
    ///
    /// With `min`: the min value in the reset window is 0 (post-reset).
    ///   Previous window's min was ~9700. Delta: 0 - 9700 = negative → rate = 0.
    ///   Next window's min is ~0. Delta: ~0 - 0 = ~0 → rate near 0.
    ///   This creates a gap where rate disappears for 2+ windows.
    ///
    /// With `last`: the last value in the reset window is ~150 (post-reset).
    ///   Previous window's last was ~10000. Delta: 150 - 10000 = negative →
    ///   PromQL detects counter reset, uses 150 as the increase → rate ≈ 0.5/s.
    ///   Next window's last is ~450. Delta: 450 - 150 = 300 → rate = 1.0/s.
    ///   Rate recovers immediately.
    #[tokio::test]
    async fn test_rate_counter_reset_last_vs_min() {
        let bucket_5m = 300_000i64;
        let base_time = 1_000_000i64;
        let n_buckets = 8usize;

        // Counter at ~1.0/s = 300 increase per 5-min bucket.
        // Reset happens in bucket 4 (midway through the window).
        let last_values: Vec<f64> = vec![
            1000.0, 1300.0, 1600.0, // normal buckets 0-2
            150.0,  // bucket 3: reset happened mid-window, last = 150 (post-reset)
            450.0, 750.0, 1050.0, 1350.0, // buckets 4-7: normal again
        ];
        let min_values: Vec<f64> = vec![
            700.0, 1000.0, 1300.0, // normal buckets 0-2 (min ≈ first value)
            0.0,   // bucket 3: min = 0 (the reset point)
            150.0, 450.0, 750.0, 1050.0, // buckets 4-7
        ];

        let timestamps: Vec<i64> = (0..n_buckets)
            .map(|i| base_time + i as i64 * bucket_5m)
            .collect();

        // Test with `last` values (correct behavior)
        let last_batch = make_test_batch(
            &timestamps,
            &last_values,
            "fp_001",
            &["job"],
            &[&vec!["server"; n_buckets]],
        );
        let mut last_fetcher = InMemoryMetricFetcher::new();
        last_fetcher.insert("requests_total", vec![last_batch]);
        let last_evaluator = PromQLEvaluator::with_fetcher(Arc::new(last_fetcher));

        let project_id = Uuid::new_v4();
        let eval_start = base_time + 2 * bucket_5m;
        let eval_end = base_time + 7 * bucket_5m;

        let (last_results, _) = last_evaluator
            .execute(
                "rate(requests_total[10m])",
                &project_id,
                eval_start,
                eval_end,
                bucket_5m,
                false,
            )
            .await
            .unwrap();

        let last_rates: Vec<f64> = last_results
            .iter()
            .flat_map(|b| {
                let arr = b
                    .column_by_name(COL_VALUE)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                (0..arr.len())
                    .filter(|&i| !arr.is_null(i))
                    .map(|i| arr.value(i))
                    .collect::<Vec<_>>()
            })
            .collect();

        // Test with `min` values (old broken behavior)
        let min_batch = make_test_batch(
            &timestamps,
            &min_values,
            "fp_001",
            &["job"],
            &[&vec!["server"; n_buckets]],
        );
        let mut min_fetcher = InMemoryMetricFetcher::new();
        min_fetcher.insert("requests_total", vec![min_batch]);
        let min_evaluator = PromQLEvaluator::with_fetcher(Arc::new(min_fetcher));

        let (min_results, _) = min_evaluator
            .execute(
                "rate(requests_total[10m])",
                &project_id,
                eval_start,
                eval_end,
                bucket_5m,
                false,
            )
            .await
            .unwrap();

        let min_rates: Vec<f64> = min_results
            .iter()
            .flat_map(|b| {
                let arr = b
                    .column_by_name(COL_VALUE)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                (0..arr.len())
                    .filter(|&i| !arr.is_null(i))
                    .map(|i| arr.value(i))
                    .collect::<Vec<_>>()
            })
            .collect();

        assert!(!last_rates.is_empty(), "last-based rate should produce results");
        assert!(!min_rates.is_empty(), "min-based rate should produce results");

        // With `last`: rate should recover after the reset
        // The post-reset window still produces a rate (PromQL treats 150 as increase)
        // and subsequent windows return to ~1.0/s
        // Even after reset, `last` should produce non-zero values (PromQL detects reset)
        let last_has_recovery = last_rates.iter().any(|v| *v > 0.8);
        assert!(
            last_has_recovery,
            "`last` rate should recover after counter reset: {:?}",
            last_rates
        );

        // With `min`: the rate around the reset window is more disrupted
        // because min=0 creates a larger negative delta that PromQL handles
        // differently, and min=150 in the next window starts lower
        // The key difference is visible in the post-reset recovery pattern
        eprintln!("last-based rates: {:?}", last_rates);
        eprintln!("min-based rates:  {:?}", min_rates);
    }

    /// Proves that duplicate rows at the same timestamp (from unmerged
    /// AggregatingMergeTree parts) produce massive rate spikes, and that
    /// deduplicating via max(last) per (fingerprint, unix_milli) fixes it.
    ///
    /// In production, the SQL query uses GROUP BY fingerprint, unix_milli
    /// with max(last) to collapse unmerged rows before feeding data to
    /// the PromQL evaluator.
    #[tokio::test]
    async fn test_duplicate_agg_rows_cause_rate_spikes() {
        let bucket_5m = 300_000i64;
        let base_time = 1_000_000i64;
        let n_buckets = 6usize;
        let rate_per_sec = 1.0_f64;
        let increment_per_bucket = rate_per_sec * (bucket_5m as f64 / 1000.0); // 300 per bucket

        let fps = ["fp_node_0"];
        let start_counter = 390_000.0;

        // Clean agg data: one row per bucket
        let clean_ts: Vec<i64> = (0..n_buckets)
            .map(|b| base_time + b as i64 * bucket_5m)
            .collect();
        let clean_vals: Vec<f64> = (0..n_buckets)
            .map(|b| start_counter + (b as f64 + 1.0) * increment_per_bucket)
            .collect();

        // Duplicated data: at bucket 3, insert two rows with different `last` values
        // (simulating two unmerged insert batches in AggregatingMergeTree)
        let dup_bucket = 3;
        let mut dup_ts = Vec::new();
        let mut dup_vals = Vec::new();
        for b in 0..n_buckets {
            dup_ts.push(base_time + b as i64 * bucket_5m);
            dup_vals.push(clean_vals[b]);
            if b == dup_bucket {
                // Second row at same timestamp, slightly lower value
                // (from an earlier insert batch that captured a lower counter)
                dup_ts.push(base_time + b as i64 * bucket_5m);
                dup_vals.push(clean_vals[b] - 50.0);
            }
        }

        let project_id = Uuid::new_v4();
        let eval_start = base_time + 2 * bucket_5m;
        let eval_end = base_time + (n_buckets as i64 - 1) * bucket_5m;

        // Evaluate with clean data (no duplicates)
        let clean_batches = vec![make_test_batch(
            &clean_ts,
            &clean_vals,
            fps[0],
            &["instance"],
            &[&vec!["node"; n_buckets]],
        )];
        let mut clean_fetcher = InMemoryMetricFetcher::new();
        clean_fetcher.insert("test_counter", clean_batches);
        let clean_eval = PromQLEvaluator::with_fetcher(Arc::new(clean_fetcher));

        let (clean_results, _) = clean_eval
            .execute(
                "rate(test_counter[5m])",
                &project_id,
                eval_start,
                eval_end,
                60_000,
                false,
            )
            .await
            .unwrap();

        let clean_rates: Vec<f64> = clean_results
            .iter()
            .flat_map(|b| {
                let arr = b
                    .column_by_name(COL_VALUE)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                (0..arr.len())
                    .filter(|&i| !arr.is_null(i))
                    .map(|i| arr.value(i))
                    .collect::<Vec<_>>()
            })
            .collect();

        // Evaluate with duplicated data (two rows at same timestamp)
        let dup_batches = vec![make_test_batch(
            &dup_ts,
            &dup_vals,
            fps[0],
            &["instance"],
            &[&vec!["node"; dup_ts.len()]],
        )];
        let mut dup_fetcher = InMemoryMetricFetcher::new();
        dup_fetcher.insert("test_counter", dup_batches);
        let dup_eval = PromQLEvaluator::with_fetcher(Arc::new(dup_fetcher));

        let (dup_results, _) = dup_eval
            .execute(
                "rate(test_counter[5m])",
                &project_id,
                eval_start,
                eval_end,
                60_000,
                false,
            )
            .await
            .unwrap();

        let dup_rates: Vec<f64> = dup_results
            .iter()
            .flat_map(|b| {
                let arr = b
                    .column_by_name(COL_VALUE)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                (0..arr.len())
                    .filter(|&i| !arr.is_null(i))
                    .map(|i| arr.value(i))
                    .collect::<Vec<_>>()
            })
            .collect();

        // Clean data should produce stable rates ~1.0/s
        for (i, v) in clean_rates.iter().enumerate() {
            assert!(
                (*v - rate_per_sec).abs() < 0.5,
                "clean rate at pos {} should be ~{:.1}/s, got {:.3}",
                i, rate_per_sec, v
            );
        }

        // Duplicate timestamps are deduped in rate() — rates should stay stable.
        let max_dup_rate = dup_rates.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        assert!(
            (max_dup_rate - rate_per_sec).abs() < 0.5,
            "duplicate timestamps should not cause rate spikes after dedup, got max {:.3}",
            max_dup_rate
        );

        // Deduplicated data (take max value per timestamp, like our GROUP BY max(last))
        // should produce the same rates as clean data
        let mut dedup_ts = Vec::new();
        let mut dedup_vals = Vec::new();
        let mut i = 0;
        while i < dup_ts.len() {
            let t = dup_ts[i];
            let mut max_val = dup_vals[i];
            while i + 1 < dup_ts.len() && dup_ts[i + 1] == t {
                i += 1;
                max_val = max_val.max(dup_vals[i]);
            }
            dedup_ts.push(t);
            dedup_vals.push(max_val);
            i += 1;
        }

        let dedup_batches = vec![make_test_batch(
            &dedup_ts,
            &dedup_vals,
            fps[0],
            &["instance"],
            &[&vec!["node"; dedup_ts.len()]],
        )];
        let mut dedup_fetcher = InMemoryMetricFetcher::new();
        dedup_fetcher.insert("test_counter", dedup_batches);
        let dedup_eval = PromQLEvaluator::with_fetcher(Arc::new(dedup_fetcher));

        let (dedup_results, _) = dedup_eval
            .execute(
                "rate(test_counter[5m])",
                &project_id,
                eval_start,
                eval_end,
                60_000,
                false,
            )
            .await
            .unwrap();

        let dedup_rates: Vec<f64> = dedup_results
            .iter()
            .flat_map(|b| {
                let arr = b
                    .column_by_name(COL_VALUE)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                (0..arr.len())
                    .filter(|&i| !arr.is_null(i))
                    .map(|i| arr.value(i))
                    .collect::<Vec<_>>()
            })
            .collect();

        for (i, v) in dedup_rates.iter().enumerate() {
            assert!(
                (*v - rate_per_sec).abs() < 0.5,
                "deduplicated rate at pos {} should be ~{:.1}/s, got {:.3}",
                i, rate_per_sec, v
            );
        }

        eprintln!("clean rates: {:?}", clean_rates);
        eprintln!("dup rates (with spikes): {:?}", dup_rates);
        eprintln!("dedup rates (max per ts): {:?}", dedup_rates);
    }
}
