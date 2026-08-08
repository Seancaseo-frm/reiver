//! Metrics ingestion and query API for time series data.
//!
//! This module provides endpoints for:
//! - Ingesting metric samples with fingerprinting
//! - Querying metrics with automatic table selection
//! - Listing available metrics and their labels

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::Json,
    routing::{get, post},
    Router,
};
use chrono::{DateTime, Utc};
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::app_state::WatchState;
use crate::error::{AppError, Result};
use crate::metrics::{
    build_metric_query, compute_fingerprint, MetricDataPoint, MetricQuery, MetricType,
    SpaceAggregation, Temporality, TimeAggregation,
};

pub fn create_metrics_router() -> Router<Arc<WatchState>> {
    // Routes will be nested under /metrics in api.rs
    Router::new()
        .route("/", post(receive_metrics))
        .route("/query", post(query_metrics))
        .route("/names", get(list_metric_names))
        .route("/{metric_name}/labels", get(get_metric_labels))
}

/// Request payload for metrics ingestion
#[derive(Debug, Deserialize)]
struct MetricsPayload {
    metrics: Vec<MetricPointPayload>,
}

/// A single metric point to ingest
#[derive(Debug, Deserialize)]
pub struct MetricPointPayload {
    /// Metric name (e.g., "http.requests", "cpu.usage")
    pub name: String,

    /// Metric value
    pub value: MetricValuePayload,

    /// Metric type (gauge, sum, histogram, summary)
    #[serde(rename = "type", default)]
    pub metric_type: MetricType,

    /// Temporality (delta, cumulative, unspecified)
    #[serde(default)]
    pub temporality: Temporality,

    /// Timestamp (optional, defaults to now)
    #[serde(default)]
    pub timestamp: Option<DateTime<Utc>>,

    /// Labels as key-value pairs
    #[serde(default)]
    pub labels: BTreeMap<String, String>,

    /// Legacy tags field (array of strings) - will be converted to labels
    #[serde(default)]
    pub tags: Vec<String>,

    /// OTel Resource Attributes (service.name, k8s.pod.name, etc.)
    #[serde(default)]
    pub resource_attributes: std::collections::HashMap<String, String>,

    /// OTel Metric Attributes (specific to this metric)
    #[serde(default)]
    pub metric_attributes: std::collections::HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum MetricValuePayload {
    Float(f64),
    Int(i64),
    UInt(u64),
}

impl MetricValuePayload {
    pub fn as_f64(&self) -> f64 {
        match self {
            MetricValuePayload::Float(v) => *v,
            MetricValuePayload::Int(v) => *v as f64,
            MetricValuePayload::UInt(v) => *v as f64,
        }
    }
}

/// Handle metrics ingestion endpoint
/// POST /api/v1/metrics
async fn receive_metrics(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<MetricsPayload>,
) -> Result<StatusCode> {
    let project_id = crate::api::extract_project_id(&headers)?;
    let num_metrics = payload.metrics.len() as u64;

    info!(
        "Received {} metrics from project_id: {}",
        num_metrics, project_id
    );

    store_metrics_v1(&state.clickhouse, project_id, &payload.metrics).await?;

    if num_metrics > 0 {
        write_usage(
            &state.clickhouse,
            &project_id.to_string(),
            "metric",
            num_metrics,
        )
        .await;
    }

    Ok(StatusCode::OK)
}

#[derive(Row, Serialize)]
struct UsageInsert {
    project_id: String,
    event_type: String,
    #[serde(with = "clickhouse::serde::chrono::date")]
    date: chrono::NaiveDate,
    value: u64,
}

async fn write_usage(
    clickhouse: &crate::clickhouse_db::ClickHousePool,
    project_id: &str,
    event_type: &str,
    value: u64,
) {
    let row = UsageInsert {
        project_id: project_id.to_string(),
        event_type: event_type.to_string(),
        date: Utc::now().date_naive(),
        value,
    };
    let mut insert = match clickhouse.as_ref().insert::<UsageInsert>("usage").await {
        Ok(insert) => insert,
        Err(e) => {
            tracing::error!("Failed to create usage insert: {}", e);
            return;
        }
    };
    if let Err(e) = insert.write(&row).await {
        tracing::error!("Failed to write usage: {}", e);
        return;
    }
    if let Err(e) = insert.end().await {
        tracing::error!("Failed to end usage insert: {}", e);
    }
}

/// Row structure for samples_v1 table (snake_case)
#[derive(Row, Serialize)]
struct SampleInsert {
    #[serde(with = "clickhouse::serde::uuid")]
    project_id: Uuid,
    metric_name: String,
    fingerprint: u64,
    unix_milli: i64,
    value: f64,
    temporality: String,
    metric_type: String,
    flags: u8,
    resource_attributes: Vec<(String, String)>,
    metric_attributes: Vec<(String, String)>,
    labels: String,
}

/// Row structure for time_series_v1 table (snake_case)
#[derive(Row, Serialize)]
struct TimeSeriesInsert {
    #[serde(with = "clickhouse::serde::uuid")]
    project_id: Uuid,
    metric_name: String,
    fingerprint: u64,
    labels: String,
    temporality: String,
    metric_type: String,
    unix_milli: i64,
    resource_attributes: Vec<(String, String)>,
    metric_attributes: Vec<(String, String)>,
}

/// Store metrics using the new v1 schema with fingerprinting
pub async fn store_metrics_v1(
    clickhouse: &crate::clickhouse_db::ClickHousePool,
    project_id: Uuid,
    metrics: &[MetricPointPayload],
) -> Result<()> {
    if metrics.is_empty() {
        return Ok(());
    }

    let now = Utc::now();

    // Create inserters for both tables
    let mut samples_inserter = clickhouse
        .as_ref()
        .inserter::<SampleInsert>("samples_v1")
        .with_period(Some(Duration::from_secs(30)))
        .with_max_rows(500_000);

    let mut time_series_inserter = clickhouse
        .as_ref()
        .inserter::<TimeSeriesInsert>("time_series_v1")
        .with_period(Some(Duration::from_secs(30)))
        .with_max_rows(100_000);

    for metric in metrics {
        // Convert legacy tags to labels if labels is empty
        let labels = if metric.labels.is_empty() && !metric.tags.is_empty() {
            convert_tags_to_labels(&metric.tags)
        } else {
            metric.labels.clone()
        };

        // Compute fingerprint from metric name and labels
        let fingerprint = compute_fingerprint(&metric.name, &labels);

        // Get timestamp in milliseconds
        let timestamp = metric.timestamp.unwrap_or(now);
        let unix_milli = timestamp.timestamp_millis();

        // Serialize labels to JSON
        let labels_json = serde_json::to_string(&labels).unwrap_or_else(|_| "{}".to_string());

        // Convert HashMap to Vec<(String, String)> for ClickHouse Map type
        let resource_attrs: Vec<(String, String)> = metric
            .resource_attributes
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let metric_attrs: Vec<(String, String)> = metric
            .metric_attributes
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // Insert sample
        let sample = SampleInsert {
            project_id: project_id,
            metric_name: metric.name.clone(),
            fingerprint,
            unix_milli,
            value: metric.value.as_f64(),
            temporality: metric.temporality.as_str().to_string(),
            metric_type: metric.metric_type.as_str().to_string(),
            flags: 0,
            resource_attributes: resource_attrs.clone(),
            metric_attributes: metric_attrs.clone(),
            labels: labels_json.clone(),
        };

        samples_inserter.write(&sample).await.map_err(|e| {
            error!("Failed to write sample to inserter: {}", e);
            AppError::Internal(anyhow::anyhow!("ClickHouse samples insert failed: {}", e))
        })?;

        // Insert/update time series metadata
        let time_series = TimeSeriesInsert {
            project_id: project_id,
            metric_name: metric.name.clone(),
            fingerprint,
            labels: labels_json,
            temporality: metric.temporality.as_str().to_string(),
            metric_type: metric.metric_type.as_str().to_string(),
            unix_milli,
            resource_attributes: resource_attrs,
            metric_attributes: metric_attrs,
        };

        time_series_inserter
            .write(&time_series)
            .await
            .map_err(|e| {
                error!("Failed to write time series to inserter: {}", e);
                AppError::Internal(anyhow::anyhow!(
                    "ClickHouse time_series insert failed: {}",
                    e
                ))
            })?;
    }

    // Commit both inserters
    samples_inserter.commit().await.map_err(|e| {
        error!("Failed to commit samples: {}", e);
        AppError::Internal(anyhow::anyhow!("ClickHouse samples commit failed: {}", e))
    })?;

    time_series_inserter.commit().await.map_err(|e| {
        error!("Failed to commit time series: {}", e);
        AppError::Internal(anyhow::anyhow!(
            "ClickHouse time_series commit failed: {}",
            e
        ))
    })?;

    // End inserters
    samples_inserter.end().await.map_err(|e| {
        error!("Failed to end samples inserter: {}", e);
        AppError::Internal(anyhow::anyhow!("ClickHouse samples end failed: {}", e))
    })?;

    time_series_inserter.end().await.map_err(|e| {
        error!("Failed to end time series inserter: {}", e);
        AppError::Internal(anyhow::anyhow!("ClickHouse time_series end failed: {}", e))
    })?;

    info!(
        "Successfully inserted {} metrics into ClickHouse (samples_v1 + time_series_v1)",
        metrics.len()
    );
    Ok(())
}

/// Convert legacy tags array to labels map
fn convert_tags_to_labels(tags: &[String]) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    for tag in tags {
        // Parse tags in format "key:value" or just add as "tag" -> "value"
        if let Some((key, value)) = tag.split_once(':') {
            labels.insert(key.trim().to_string(), value.trim().to_string());
        } else if let Some((key, value)) = tag.split_once('=') {
            labels.insert(key.trim().to_string(), value.trim().to_string());
        } else {
            // If no delimiter, use the tag as both key and value
            labels.insert(tag.clone(), "true".to_string());
        }
    }
    labels
}

// ============================================================================
// QUERY ENDPOINTS
// ============================================================================

/// Request payload for querying metrics
#[derive(Debug, Deserialize)]
pub struct MetricQueryRequest {
    /// Project ID to query
    pub project_id: Uuid,
    /// Metric name to query
    pub metric_name: String,
    /// Start time (ISO 8601 or Unix milliseconds)
    pub start: TimeSpec,
    /// End time (ISO 8601 or Unix milliseconds)
    pub end: TimeSpec,
    /// Step interval in seconds (default: 60)
    #[serde(default = "default_step")]
    pub step: u64,
    /// Time aggregation function (default: avg)
    #[serde(default)]
    pub time_aggregation: TimeAggregation,
    /// Space aggregation function (default: sum)
    #[serde(default)]
    pub space_aggregation: SpaceAggregation,
    /// Label filters (key -> value)
    #[serde(default)]
    pub filters: BTreeMap<String, String>,
    /// Group by these label keys
    #[serde(default)]
    pub group_by: Vec<String>,
}

fn default_step() -> u64 {
    60
}

/// Time specification - either ISO 8601 string or Unix milliseconds
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum TimeSpec {
    Millis(i64),
    Iso(String),
}

impl TimeSpec {
    fn to_millis(&self) -> Result<i64> {
        match self {
            TimeSpec::Millis(ms) => Ok(*ms),
            TimeSpec::Iso(s) => {
                // Try parsing as RFC 3339
                DateTime::parse_from_rfc3339(s)
                    .map(|dt| dt.timestamp_millis())
                    .or_else(|_| {
                        // Try parsing as simple date-time
                        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                            .map(|dt| dt.and_utc().timestamp_millis())
                    })
                    .map_err(|e| AppError::Validation(format!("Invalid time format: {}", e)))
            }
        }
    }
}

/// Response for metric queries
#[derive(Debug, Serialize)]
pub struct MetricQueryResponse {
    pub metric_name: String,
    pub data: Vec<MetricDataPoint>,
    pub table_used: String,
    pub query_time_ms: u64,
}

/// Query metrics endpoint
/// POST /api/v1/metrics/query
async fn query_metrics(
    State(state): State<Arc<WatchState>>,
    Json(request): Json<MetricQueryRequest>,
) -> Result<Json<MetricQueryResponse>> {
    let start_time = std::time::Instant::now();

    let start_ms = request.start.to_millis()?;
    let end_ms = request.end.to_millis()?;

    if start_ms >= end_ms {
        return Err(AppError::Validation(
            "Start time must be before end time".to_string(),
        ));
    }

    // Try to get cached data first
    use std::collections::HashMap;
    let filters_hashmap: HashMap<String, String> = request.filters.clone().into_iter().collect();

    let cached_data = crate::metrics::get_cached_data_points(
        &state.redis,
        &request.project_id.to_string(),
        &request.metric_name,
        &filters_hashmap,
        start_ms,
        end_ms,
    )
    .await?;

    let (data, table_used, query_time_ms) = if let Some(cached_points) = cached_data {
        info!(
            "Metrics query served from cache: {} data points",
            cached_points.len()
        );
        (
            cached_points,
            "cache".to_string(),
            start_time.elapsed().as_millis() as u64,
        )
    } else {
        // Cache miss - execute the query
        // Build the query
        let query = MetricQuery::new(
            request.project_id,
            request.metric_name.clone(),
            start_ms,
            end_ms,
        )
        .with_step(request.step as i64 * 1000) // Convert seconds to milliseconds
        .with_time_aggregation(request.time_aggregation)
        .with_space_aggregation(request.space_aggregation)
        .with_label_filters(request.filters)
        .with_group_by(request.group_by);

        let (sql, table) =
            build_metric_query(&query).map_err(|e| AppError::Validation(e.to_string()))?;

        info!(
            "Executing metrics query on table {}: {}",
            table.table_name(),
            &sql[..sql.len().min(200)]
        );

        // Execute the query
        let data = execute_metric_query(&state.clickhouse, &sql, &query.group_by).await?;

        let query_time_ms = start_time.elapsed().as_millis() as u64;

        info!(
            "Metrics query completed: {} data points in {}ms using {}",
            data.len(),
            query_time_ms,
            table.table_name()
        );

        (data, table.table_name().to_string(), query_time_ms)
    };

    Ok(Json(MetricQueryResponse {
        metric_name: request.metric_name,
        data,
        table_used,
        query_time_ms,
    }))
}

/// Execute a metric query and return data points
async fn execute_metric_query(
    clickhouse: &crate::clickhouse_db::ClickHousePool,
    sql: &str,
    group_by: &[String],
) -> Result<Vec<MetricDataPoint>> {
    // For queries with grouping, we need to handle the extra columns
    if group_by.is_empty() {
        // Simple query - just bucket and value
        #[derive(Row, Deserialize)]
        struct SimpleRow {
            bucket: i64,
            value: f64,
        }

        let rows: Vec<SimpleRow> = clickhouse.query(sql).fetch_all().await.map_err(|e| {
            error!("ClickHouse query failed: {}", e);
            AppError::Internal(anyhow::anyhow!("Metrics query failed: {}", e))
        })?;

        Ok(rows
            .into_iter()
            .map(|r| MetricDataPoint {
                timestamp_ms: r.bucket,
                value: r.value,
                labels: BTreeMap::new(),
            })
            .collect())
    } else {
        // Query with grouping - fetch as JSON to handle dynamic columns
        // For simplicity, we'll use a workaround with string parsing
        warn!("Grouped queries with dynamic labels not fully implemented yet");

        #[derive(Row, Deserialize)]
        struct SimpleRow {
            bucket: i64,
            value: f64,
        }

        let rows: Vec<SimpleRow> = clickhouse.query(sql).fetch_all().await.map_err(|e| {
            error!("ClickHouse query failed: {}", e);
            AppError::Internal(anyhow::anyhow!("Metrics query failed: {}", e))
        })?;

        Ok(rows
            .into_iter()
            .map(|r| MetricDataPoint {
                timestamp_ms: r.bucket,
                value: r.value,
                labels: BTreeMap::new(), // TODO: Extract labels from grouped query
            })
            .collect())
    }
}

/// Response for listing metric names
#[derive(Debug, Serialize)]
pub struct MetricNamesResponse {
    pub metrics: Vec<MetricNameInfo>,
}

#[derive(Debug, Serialize)]
pub struct MetricNameInfo {
    pub name: String,
    pub metric_type: String,
    pub temporality: String,
    pub series_count: u64,
}

/// Query parameters for listing metrics
#[derive(Debug, Deserialize)]
pub struct ListMetricsQuery {
    pub project_id: Uuid,
    /// Optional prefix filter
    pub prefix: Option<String>,
    /// Limit results (default: 100)
    #[serde(default = "default_limit")]
    pub limit: u32,
}

fn default_limit() -> u32 {
    100
}

/// List available metric names for a project
/// GET /api/v1/metrics/names?project_id=...
async fn list_metric_names(
    State(state): State<Arc<WatchState>>,
    Query(params): Query<ListMetricsQuery>,
) -> Result<Json<MetricNamesResponse>> {
    let cutoff_ms = (chrono::Utc::now() - chrono::Duration::days(30)).timestamp_millis();
    let mut sql = format!(
        r#"SELECT 
            metric_name,
            anyLast(metric_type) as metric_type,
            anyLast(temporality) as temporality,
            count(DISTINCT fingerprint) as series_count
        FROM reiver.time_series_v1
        WHERE project_id = '{}'
          AND unix_milli >= {}"#,
        params.project_id, cutoff_ms
    );

    if let Some(prefix) = &params.prefix {
        sql.push_str(&format!(
            " AND metric_name LIKE '{}%'",
            crate::utils::escape_clickhouse_string(prefix)
        ));
    }

    sql.push_str(&format!(
        " GROUP BY metric_name ORDER BY metric_name LIMIT {}",
        params.limit
    ));

    #[derive(Row, Deserialize)]
    struct MetricRow {
        metric_name: String,
        metric_type: String,
        temporality: String,
        series_count: u64,
    }

    let rows: Vec<MetricRow> = state
        .clickhouse
        .query(&sql)
        .fetch_all()
        .await
        .map_err(|e| {
            error!("Failed to list metric names: {}", e);
            AppError::Internal(anyhow::anyhow!("Failed to list metrics: {}", e))
        })?;

    let metrics = rows
        .into_iter()
        .map(|r| MetricNameInfo {
            name: r.metric_name,
            metric_type: r.metric_type,
            temporality: r.temporality,
            series_count: r.series_count,
        })
        .collect();

    Ok(Json(MetricNamesResponse { metrics }))
}

/// Response for metric labels
#[derive(Debug, Serialize)]
pub struct MetricLabelsResponse {
    pub metric_name: String,
    pub label_keys: Vec<String>,
    pub label_values: BTreeMap<String, Vec<String>>,
}

/// Query parameters for getting metric labels
#[derive(Debug, Deserialize)]
pub struct GetLabelsQuery {
    pub project_id: Uuid,
    /// Limit values per label (default: 100)
    #[serde(default = "default_limit")]
    pub limit: u32,
}

/// Get label keys and values for a specific metric
/// GET /api/v1/metrics/{metric_name}/labels?project_id=...
async fn get_metric_labels(
    State(state): State<Arc<WatchState>>,
    Path(metric_name): Path<String>,
    Query(params): Query<GetLabelsQuery>,
) -> Result<Json<MetricLabelsResponse>> {
    // First, get a sample of labels JSON to extract keys
    let sample_sql = format!(
        r#"SELECT DISTINCT labels
        FROM reiver.time_series_v1
        WHERE project_id = '{}' AND metric_name = '{}'
        LIMIT 1000"#,
        params.project_id,
        crate::utils::escape_clickhouse_string(&metric_name)
    );

    #[derive(Row, Deserialize)]
    struct LabelRow {
        labels: String,
    }

    let rows: Vec<LabelRow> = state
        .clickhouse
        .query(&sample_sql)
        .fetch_all()
        .await
        .map_err(|e| {
            error!("Failed to get metric labels: {}", e);
            AppError::Internal(anyhow::anyhow!("Failed to get labels: {}", e))
        })?;

    // Extract all unique label keys and their values
    let mut label_keys = std::collections::HashSet::new();
    let mut label_values: BTreeMap<String, std::collections::HashSet<String>> = BTreeMap::new();

    for row in rows {
        if let Ok(labels) = serde_json::from_str::<BTreeMap<String, String>>(&row.labels) {
            for (key, value) in labels {
                label_keys.insert(key.clone());
                label_values.entry(key).or_default().insert(value);
            }
        }
    }

    // Convert to sorted vecs and limit values
    let mut label_keys: Vec<String> = label_keys.into_iter().collect();
    label_keys.sort();

    let label_values: BTreeMap<String, Vec<String>> = label_values
        .into_iter()
        .map(|(k, v)| {
            let mut values: Vec<String> = v.into_iter().collect();
            values.sort();
            values.truncate(params.limit as usize);
            (k, values)
        })
        .collect();

    Ok(Json(MetricLabelsResponse {
        metric_name,
        label_keys,
        label_values,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_tags_to_labels() {
        let tags = vec![
            "env:production".to_string(),
            "host=web-1".to_string(),
            "debug".to_string(),
        ];

        let labels = convert_tags_to_labels(&tags);

        assert_eq!(labels.get("env"), Some(&"production".to_string()));
        assert_eq!(labels.get("host"), Some(&"web-1".to_string()));
        assert_eq!(labels.get("debug"), Some(&"true".to_string()));
    }
}
