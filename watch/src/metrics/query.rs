//! Metrics query builder for efficient time series queries.
//!
//! This module provides query building capabilities that automatically select
//! the appropriate table (raw or pre-aggregated) based on the query time range.

#![allow(dead_code)] // Query builder - some methods for future advanced query features

use super::tables::{select_samples_table_for_agg, SamplesTable, UnsupportedAggregationError};
use super::types::{SpaceAggregation, TimeAggregation};
use std::collections::BTreeMap;

/// Error type for metric query building
#[derive(Debug)]
pub enum MetricQueryError {
    UnsupportedAggregation(UnsupportedAggregationError),
}

impl std::fmt::Display for MetricQueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetricQueryError::UnsupportedAggregation(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for MetricQueryError {}

impl From<UnsupportedAggregationError> for MetricQueryError {
    fn from(e: UnsupportedAggregationError) -> Self {
        MetricQueryError::UnsupportedAggregation(e)
    }
}

/// A metric query request.
#[derive(Debug, Clone)]
pub struct MetricQuery {
    /// Project ID to filter by
    pub project_id: uuid::Uuid,
    /// Metric name to query
    pub metric_name: String,
    /// Start time in milliseconds since epoch
    pub start_ms: i64,
    /// End time in milliseconds since epoch
    pub end_ms: i64,
    /// Step/interval in milliseconds for time bucketing
    pub step_ms: i64,
    /// Time aggregation function
    pub time_aggregation: TimeAggregation,
    /// Space aggregation function (across fingerprints)
    pub space_aggregation: SpaceAggregation,
    /// Label filters (key -> value)
    pub label_filters: BTreeMap<String, String>,
    /// Group by these label keys
    pub group_by: Vec<String>,
}

impl MetricQuery {
    /// Create a new metric query with defaults.
    pub fn new(project_id: uuid::Uuid, metric_name: String, start_ms: i64, end_ms: i64) -> Self {
        // Default step to 1 minute
        let step_ms = 60_000;

        Self {
            project_id,
            metric_name,
            start_ms,
            end_ms,
            step_ms,
            time_aggregation: TimeAggregation::Avg,
            space_aggregation: SpaceAggregation::Sum,
            label_filters: BTreeMap::new(),
            group_by: Vec::new(),
        }
    }

    /// Set the step interval.
    pub fn with_step(mut self, step_ms: i64) -> Self {
        self.step_ms = step_ms;
        self
    }

    /// Set the time aggregation function.
    pub fn with_time_aggregation(mut self, agg: TimeAggregation) -> Self {
        self.time_aggregation = agg;
        self
    }

    /// Set the space aggregation function.
    pub fn with_space_aggregation(mut self, agg: SpaceAggregation) -> Self {
        self.space_aggregation = agg;
        self
    }

    /// Add a label filter.
    pub fn with_label_filter(mut self, key: String, value: String) -> Self {
        self.label_filters.insert(key, value);
        self
    }

    /// Set label filters.
    pub fn with_label_filters(mut self, filters: BTreeMap<String, String>) -> Self {
        self.label_filters = filters;
        self
    }

    /// Set group by keys.
    pub fn with_group_by(mut self, keys: Vec<String>) -> Self {
        self.group_by = keys;
        self
    }
}

/// Result of a metric query - a time series data point.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MetricDataPoint {
    /// Timestamp in milliseconds
    pub timestamp_ms: i64,
    /// Aggregated value
    pub value: f64,
    /// Labels for this series (when grouping)
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

/// A complete metric query result.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MetricQueryResult {
    /// The metric name
    pub metric_name: String,
    /// Data points
    pub data: Vec<MetricDataPoint>,
    /// Which table was used for the query
    pub table_used: String,
}

/// Build a ClickHouse SQL query for metrics.
///
/// # Errors
/// Returns an error if the requested aggregation is not supported on the
/// automatically selected table (e.g., percentiles on aggregated tables).
pub fn build_metric_query(query: &MetricQuery) -> Result<(String, SamplesTable), MetricQueryError> {
    let table = select_samples_table_for_agg(query.start_ms, query.end_ms, query.time_aggregation);
    let table_name = format!("reiver.{}", table.table_name());

    // Build the aggregation expression based on table type
    let agg_expr = table.aggregation_expr(query.time_aggregation)?;

    // Build time bucket expression
    let time_bucket = format!(
        "toInt64(intDiv(unix_milli, {}) * {}) AS bucket",
        query.step_ms, query.step_ms
    );

    // Build WHERE clause
    let where_clauses = vec![
        format!("project_id = '{}'", query.project_id),
        format!("metric_name = '{}'", escape_string(&query.metric_name)),
        format!("unix_milli >= {}", query.start_ms),
        format!("unix_milli < {}", query.end_ms),
    ];

    // Add label filters using JSON extraction
    // Note: For production, you'd want to use the time_series table for label filtering
    // This is a simplified version that filters directly on the samples table

    // Build GROUP BY clause
    let group_by = vec!["bucket".to_string()];

    // Build SELECT clause
    let select_cols = vec![time_bucket, format!("{} AS value", agg_expr)];

    // If grouping by labels, we need to join with time_series table
    let sql = if query.group_by.is_empty() && query.label_filters.is_empty() {
        // Simple query without label filtering or grouping
        // Add ClickHouse optimization hints
        format!(
            "SELECT {} FROM {} WHERE {} GROUP BY {} ORDER BY bucket SETTINGS max_threads = 4, max_block_size = 8192",
            select_cols.join(", "),
            table_name,
            where_clauses.join(" AND "),
            group_by.join(", ")
        )
    } else {
        // Query with label filtering - use CTE pattern
        let cte_query = build_cte_query(query, &table, &agg_expr);
        // Add optimization settings to CTE query
        format!(
            "{} SETTINGS max_threads = 4, max_block_size = 8192",
            cte_query
        )
    };

    Ok((sql, table))
}

/// Build a CTE-based query for label filtering and grouping.
fn build_cte_query(query: &MetricQuery, table: &SamplesTable, agg_expr: &str) -> String {
    let samples_table = format!("reiver.{}", table.table_name());

    // CTE 1: Get fingerprints matching the label filters from time_series
    // Optimize by using a narrower time window for metadata lookup
    let metadata_lookback = std::cmp::min(86400000, (query.end_ms - query.start_ms) * 2); // Max 1 day or 2x query range
    let mut ts_where = vec![
        format!("project_id = '{}'", query.project_id),
        format!("metric_name = '{}'", escape_string(&query.metric_name)),
        format!("unix_milli >= {}", query.start_ms - metadata_lookback),
    ];

    // Add label filters
    for (key, value) in &query.label_filters {
        ts_where.push(format!(
            "JSONExtractString(labels, '{}') = '{}'",
            escape_string(key),
            escape_string(value)
        ));
    }

    // Build label extraction for grouping
    let label_extracts: Vec<String> = query
        .group_by
        .iter()
        .map(|k| {
            format!(
                "JSONExtractString(labels, '{}') AS {}",
                escape_string(k),
                escape_string(k)
            )
        })
        .collect();

    let ts_select = if label_extracts.is_empty() {
        "fingerprint".to_string()
    } else {
        format!("fingerprint, {}", label_extracts.join(", "))
    };

    // CTE 2: Get samples for matching fingerprints
    let time_bucket = format!(
        "toInt64(intDiv(s.unix_milli, {}) * {}) AS bucket",
        query.step_ms, query.step_ms
    );

    // Build final GROUP BY
    let mut final_group_by = vec!["bucket".to_string()];
    final_group_by.extend(query.group_by.iter().cloned());

    // Build final SELECT
    let mut final_select = vec![
        "bucket".to_string(),
        format!(
            "{} AS value",
            agg_expr
                .replace("value", "s.value")
                .replace("sum", "s.sum")
                .replace("count", "s.count")
                .replace("min", "s.min")
                .replace("max", "s.max")
                .replace("last", "s.last")
        ),
    ];
    final_select.extend(query.group_by.iter().map(|k| format!("ts.{}", k)));

    format!(
        r#"WITH filtered_ts AS (
    SELECT DISTINCT {}
    FROM reiver.time_series_v1
    WHERE {}
)
SELECT {}, {}
FROM {} s
INNER JOIN filtered_ts ts ON s.fingerprint = ts.fingerprint
WHERE s.project_id = '{}'
  AND s.metric_name = '{}'
  AND s.unix_milli >= {}
  AND s.unix_milli < {}
GROUP BY {}
ORDER BY bucket"#,
        ts_select,
        ts_where.join(" AND "),
        time_bucket,
        final_select[1..].join(", "),
        samples_table,
        query.project_id,
        escape_string(&query.metric_name),
        query.start_ms,
        query.end_ms,
        final_group_by.join(", ")
    )
}

/// Escape a string for use in ClickHouse SQL.
fn escape_string(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn test_simple_query_building() {
        let project_id = Uuid::new_v4();
        let query = MetricQuery::new(project_id, "http.requests".to_string(), 1000000, 2000000);

        let (sql, table) = build_metric_query(&query).expect("Query should build successfully");

        assert!(sql.contains("http.requests"));
        assert!(sql.contains(&project_id.to_string()));
        assert_eq!(table, SamplesTable::Raw);
    }

    #[test]
    fn test_query_with_label_filter() {
        let project_id = Uuid::new_v4();
        let query = MetricQuery::new(project_id, "http.requests".to_string(), 1000000, 2000000)
            .with_label_filter("env".to_string(), "production".to_string());

        let (sql, _) = build_metric_query(&query).expect("Query should build successfully");

        assert!(sql.contains("filtered_ts"));
        assert!(sql.contains("JSONExtractString"));
        assert!(sql.contains("production"));
    }

    #[test]
    fn test_table_selection_for_long_range() {
        let project_id = Uuid::new_v4();
        let now = chrono::Utc::now().timestamp_millis();
        let seven_days_ago = now - (7 * 24 * 60 * 60 * 1000);

        let query = MetricQuery::new(project_id, "http.requests".to_string(), seven_days_ago, now);

        let (_, table) = build_metric_query(&query).expect("Query should build successfully");

        // For 7-day range (>= 1 week), should use 30m aggregation
        assert_eq!(table, SamplesTable::Agg30m);
    }

    #[test]
    fn test_escape_string() {
        assert_eq!(escape_string("hello"), "hello");
        assert_eq!(escape_string("it's"), "it\\'s");
        assert_eq!(escape_string("back\\slash"), "back\\\\slash");
    }
}
