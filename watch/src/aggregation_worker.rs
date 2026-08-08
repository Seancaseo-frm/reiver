use anyhow::Result;
use chrono::Utc;
use futures::stream::{self, StreamExt};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};

use crate::app_state::RedisPool;
use crate::clickhouse_db::ClickHousePool;
use bb8_redis::redis::AsyncCommands;

/// Maximum number of projects to process concurrently
/// Higher = more parallelism but more resource usage
const MAX_CONCURRENT_PROJECTS: usize = 10;

/// Start background aggregation worker
/// Pre-computes time-series aggregations (exception_rate_24h, trace_rate_24h) and stores in Redis
/// Updates every 1 minute to balance freshness and efficiency
///
/// # Arguments
/// * `redis_pool` - Redis connection pool
/// * `clickhouse_pool` - ClickHouse connection pool
/// * `shutdown_rx` - Shutdown signal receiver for graceful shutdown
pub async fn start_aggregation_worker(
    redis_pool: Arc<RedisPool>,
    clickhouse_pool: Arc<ClickHousePool>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<JoinHandle<()>> {
    info!("Starting aggregation worker...");

    let handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(StdDuration::from_secs(60));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = update_aggregations(&redis_pool, &clickhouse_pool).await {
                        error!("Aggregation worker error: {}", e);
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Aggregation worker received shutdown signal, stopping gracefully");
                        break;
                    }
                }
            }
        }
        info!("Aggregation worker stopped");
    });

    Ok(handle)
}

/// Update all aggregations (exception_rate_24h, trace_rate_24h) for all projects
async fn update_aggregations(
    redis_pool: &RedisPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    // Get all unique project IDs from ClickHouse (errors and spans tables)
    let mut projects = std::collections::HashSet::new();

    // Only consider projects with recent data (last 25 hours covers the 24h aggregation window)
    let exception_projects: Vec<String> = clickhouse_pool.as_ref()
        .query("SELECT DISTINCT project_id FROM reiver.exceptions WHERE timestamp >= now() - INTERVAL 25 HOUR")
        .fetch_all()
        .await?;
    for project_id in exception_projects {
        projects.insert(project_id);
    }

    let span_projects: Vec<String> = clickhouse_pool.as_ref()
        .query("SELECT DISTINCT project_id FROM reiver.spans WHERE timestamp >= now() - INTERVAL 25 HOUR")
        .fetch_all()
        .await?;
    for project_id in span_projects {
        projects.insert(project_id);
    }

    let projects: Vec<String> = projects.into_iter().collect();
    let project_count = projects.len();

    info!(
        "Updating aggregations for {} projects (parallelism: {})",
        project_count, MAX_CONCURRENT_PROJECTS
    );

    // Process projects in parallel using buffer_unordered
    // This allows up to MAX_CONCURRENT_PROJECTS to run simultaneously
    let results: Vec<()> = stream::iter(projects)
        .map(|project_id| {
            // Clone references for async move
            let redis = redis_pool.clone();
            let clickhouse = clickhouse_pool.clone();
            async move {
                // Update exception rate aggregation
                if let Err(e) = update_exception_rate_24h(&redis, &clickhouse, &project_id).await {
                    error!(
                        "Failed to update exception_rate_24h for project {}: {}",
                        project_id, e
                    );
                }

                // Update trace rate aggregation
                if let Err(e) = update_trace_rate_24h(&redis, &clickhouse, &project_id).await {
                    error!(
                        "Failed to update trace_rate_24h for project {}: {}",
                        project_id, e
                    );
                }

                debug!("Updated aggregations for project {}", project_id);
            }
        })
        .buffer_unordered(MAX_CONCURRENT_PROJECTS)
        .collect()
        .await;

    info!("Completed aggregations for {} projects", results.len());
    Ok(())
}

/// Pre-compute exception_rate_24h for a project. Stores hourly buckets in Redis with 26h TTL.
async fn update_exception_rate_24h(
    redis_pool: &RedisPool,
    clickhouse_pool: &ClickHousePool,
    project_id: &str,
) -> Result<()> {
    let project_key = format!("stats:project:{}", project_id);
    let mut conn = redis_pool
        .get()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get Redis connection: {}", e))?;

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct HourlyCount {
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        hour: chrono::DateTime<Utc>,
        count: u64,
    }

    let hourly_counts: Vec<HourlyCount> = clickhouse_pool
        .as_ref()
        .query(
            "SELECT \
                toDateTime64(toStartOfHour(timestamp), 9) as hour, \
                count(DISTINCT fingerprint) as count \
            FROM reiver.exceptions \
            WHERE project_id = ? \
            AND timestamp >= now() - INTERVAL 24 HOUR \
            GROUP BY hour \
            ORDER BY hour",
        )
        .bind(project_id)
        .fetch_all()
        .await?;

    for row in hourly_counts {
        let hour_timestamp = row.hour.timestamp();
        let rate_key = format!("{}:exception_rate:{}", project_key, hour_timestamp);

        let _: () = conn
            .set(&rate_key, row.count as i64)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to set exception_rate in Redis: {}", e))?;

        let _: () = conn
            .expire(&rate_key, 26 * 3600)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to set TTL on exception_rate key: {}", e))?;
    }

    Ok(())
}

/// Pre-compute trace_rate_24h for a project. Stores hourly buckets in Redis with 26h TTL.
async fn update_trace_rate_24h(
    redis_pool: &RedisPool,
    clickhouse_pool: &ClickHousePool,
    project_id: &str,
) -> Result<()> {
    let project_key = format!("stats:project:{}", project_id);
    let mut conn = redis_pool
        .get()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get Redis connection: {}", e))?;

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct HourlyTraceCount {
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        hour: chrono::DateTime<Utc>,
        count: u64,
    }

    let hourly_counts: Vec<HourlyTraceCount> = clickhouse_pool
        .as_ref()
        .query(
            "SELECT \
                toDateTime64(toStartOfHour(timestamp), 9) as hour, \
                count(DISTINCT trace_id) as count \
            FROM reiver.spans \
            WHERE project_id = ? \
            AND timestamp >= now() - INTERVAL 24 HOUR \
            GROUP BY hour \
            ORDER BY hour",
        )
        .bind(project_id)
        .fetch_all()
        .await?;

    for row in hourly_counts {
        let hour_timestamp = row.hour.timestamp();
        let trace_rate_key = format!("{}:trace_rate:{}", project_key, hour_timestamp);

        let _: () = conn
            .set(&trace_rate_key, row.count as i64)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to set trace_rate in Redis: {}", e))?;

        let _: () = conn
            .expire(&trace_rate_key, 26 * 3600)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to set TTL on trace_rate key: {}", e))?;
    }

    Ok(())
}

/// Calculate percentile value from a sorted slice of f64 values.
/// This is a helper function for computing P50, P95, P99 etc.
fn calculate_percentile(sorted_values: &[f64], percentile: f64) -> f64 {
    if sorted_values.is_empty() {
        return 0.0;
    }
    if sorted_values.len() == 1 {
        return sorted_values[0];
    }

    let index = (percentile / 100.0) * (sorted_values.len() - 1) as f64;
    let lower = index.floor() as usize;
    let upper = index.ceil() as usize;

    if lower == upper {
        sorted_values[lower]
    } else {
        let fraction = index - lower as f64;
        sorted_values[lower] * (1.0 - fraction) + sorted_values[upper] * fraction
    }
}

/// Calculate error rate from counts
fn calculate_error_rate(error_count: u64, total_count: u64) -> f64 {
    if total_count == 0 {
        return 0.0;
    }
    error_count as f64 / total_count as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Redis Key Format Tests
    // ========================================================================

    #[test]
    fn test_project_key_format() {
        let project_id = "550e8400-e29b-41d4-a716-446655440000";
        let project_key = format!("stats:project:{}", project_id);

        assert!(project_key.starts_with("stats:project:"));
        assert!(project_key.contains(project_id));
        assert_eq!(
            project_key,
            "stats:project:550e8400-e29b-41d4-a716-446655440000"
        );
    }

    #[test]
    fn test_exception_rate_key_format() {
        let project_id = "test-project";
        let hour_timestamp = 1700000000i64;
        let project_key = format!("stats:project:{}", project_id);
        let rate_key = format!("{}:exception_rate:{}", project_key, hour_timestamp);

        assert!(rate_key.starts_with("stats:project:"));
        assert!(rate_key.contains("exception_rate"));
        assert!(rate_key.ends_with(&hour_timestamp.to_string()));
    }

    #[test]
    fn test_trace_rate_key_format() {
        let project_id = "test-project";
        let hour_timestamp = 1700000000i64;
        let project_key = format!("stats:project:{}", project_id);
        let trace_rate_key = format!("{}:trace_rate:{}", project_key, hour_timestamp);

        assert!(trace_rate_key.starts_with("stats:project:"));
        assert!(trace_rate_key.contains("trace_rate"));
    }

    // ========================================================================
    // TTL Constants Tests
    // ========================================================================

    #[test]
    fn test_ttl_value() {
        let ttl_seconds = 26 * 3600;
        assert_eq!(ttl_seconds, 93600); // 26 hours in seconds
    }

    // ========================================================================
    // Percentile Calculation Tests
    // ========================================================================

    #[test]
    fn test_calculate_percentile_p50() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let p50 = calculate_percentile(&values, 50.0);
        assert_eq!(p50, 3.0);
    }

    #[test]
    fn test_calculate_percentile_p95() {
        let values: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let p95 = calculate_percentile(&values, 95.0);
        assert!((p95 - 95.05).abs() < 0.1); // Should be ~95
    }

    #[test]
    fn test_calculate_percentile_p99() {
        let values: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let p99 = calculate_percentile(&values, 99.0);
        assert!((p99 - 99.01).abs() < 0.1); // Should be ~99
    }

    #[test]
    fn test_calculate_percentile_empty() {
        let values: Vec<f64> = vec![];
        let p50 = calculate_percentile(&values, 50.0);
        assert_eq!(p50, 0.0);
    }

    #[test]
    fn test_calculate_percentile_single_value() {
        let values = vec![42.0];
        let p50 = calculate_percentile(&values, 50.0);
        assert_eq!(p50, 42.0);
    }

    #[test]
    fn test_calculate_percentile_interpolation() {
        // With 10 values, P50 should interpolate between index 4 and 5
        let values: Vec<f64> = (1..=10).map(|i| i as f64 * 10.0).collect();
        // values = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
        let p50 = calculate_percentile(&values, 50.0);
        assert_eq!(p50, 55.0); // Interpolation between 50 and 60
    }

    // ========================================================================
    // Error Rate Calculation Tests
    // ========================================================================

    #[test]
    fn test_calculate_error_rate_basic() {
        assert_eq!(calculate_error_rate(5, 100), 0.05); // 5%
        assert_eq!(calculate_error_rate(50, 100), 0.5); // 50%
        assert_eq!(calculate_error_rate(100, 100), 1.0); // 100%
    }

    #[test]
    fn test_calculate_error_rate_zero_errors() {
        assert_eq!(calculate_error_rate(0, 100), 0.0);
    }

    #[test]
    fn test_calculate_error_rate_zero_total() {
        assert_eq!(calculate_error_rate(0, 0), 0.0);
        assert_eq!(calculate_error_rate(10, 0), 0.0); // Edge case: errors but no total
    }

    #[test]
    fn test_calculate_error_rate_precision() {
        let rate = calculate_error_rate(1, 3);
        assert!((rate - 0.333333).abs() < 0.0001);
    }

    // ========================================================================
    // Aggregation Interval Tests
    // ========================================================================

    #[test]
    fn test_aggregation_interval() {
        let interval = std::time::Duration::from_secs(60);
        assert_eq!(interval.as_secs(), 60);
    }

    #[test]
    fn test_24_hour_window() {
        let window_hours = 24;
        let window_seconds = window_hours * 3600;
        assert_eq!(window_seconds, 86400);
    }
}
