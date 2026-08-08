//! Metrics query result caching with time-bucket optimization
//!
//! This implements a bucket-based caching strategy for metrics queries:
//! - Results are cached in time buckets (e.g., 5-minute buckets)
//! - Cache hits are assembled from multiple buckets when possible
//! - Reduces database load for repeated queries

#![allow(dead_code)] // Caching module implemented but not yet integrated

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::app_state::RedisPool;
use crate::metrics::query::MetricDataPoint;

/// Cache bucket size in seconds (5 minutes = 300 seconds)
const BUCKET_SIZE_SECONDS: i64 = 300;

/// Generate cache key for metrics query bucket
fn metrics_bucket_key(
    project_id: &str,
    metric_name: &str,
    filters: &HashMap<String, String>,
    time_bucket: i64,
) -> String {
    // Sort filters for consistent key generation
    let mut sorted_filters: Vec<_> = filters.iter().collect();
    sorted_filters.sort_by(|a, b| a.0.cmp(b.0));

    let filters_str = sorted_filters
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join("&");

    format!(
        "metrics_cache:{}:{}:{}:{}",
        project_id, metric_name, filters_str, time_bucket
    )
}

/// Get time bucket for a given timestamp
fn get_time_bucket(timestamp: i64) -> i64 {
    timestamp / BUCKET_SIZE_SECONDS
}

/// Cached metrics data for a single time bucket
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedMetricsBucket {
    /// Project ID
    pub project_id: String,
    /// Metric name
    pub metric_name: String,
    /// Filters used in the query
    pub filters: HashMap<String, String>,
    /// Time bucket (timestamp / BUCKET_SIZE_SECONDS)
    pub time_bucket: i64,
    /// Data points in this bucket
    pub data_points: Vec<MetricDataPoint>,
    /// When this bucket was cached
    pub cached_at: DateTime<Utc>,
    /// TTL in seconds
    pub ttl: i64,
}

impl CachedMetricsBucket {
    /// Check if bucket is still valid
    pub fn is_valid(&self) -> bool {
        let now = Utc::now();
        let elapsed = now.signed_duration_since(self.cached_at);
        elapsed.num_seconds() < self.ttl
    }

    /// Get the time range covered by this bucket
    pub fn time_range(&self) -> (i64, i64) {
        let start = self.time_bucket * BUCKET_SIZE_SECONDS;
        let end = start + BUCKET_SIZE_SECONDS;
        (start, end)
    }
}

/// Store metrics data in time buckets
pub async fn cache_metrics_bucket(
    redis_pool: &RedisPool,
    bucket: &CachedMetricsBucket,
) -> Result<()> {
    let key = metrics_bucket_key(
        &bucket.project_id,
        &bucket.metric_name,
        &bucket.filters,
        bucket.time_bucket,
    );

    let serialized = serde_json::to_string(bucket)?;
    let mut conn = redis_pool.get().await?;

    let _: () = bb8_redis::redis::AsyncCommands::set(&mut *conn, &key, &serialized).await?;
    let _: () = bb8_redis::redis::AsyncCommands::expire(&mut *conn, &key, bucket.ttl).await?;

    Ok(())
}

/// Retrieve cached metrics bucket
pub async fn get_cached_metrics_bucket(
    redis_pool: &RedisPool,
    project_id: &str,
    metric_name: &str,
    filters: &HashMap<String, String>,
    time_bucket: i64,
) -> Result<Option<CachedMetricsBucket>> {
    let key = metrics_bucket_key(project_id, metric_name, filters, time_bucket);
    let mut conn = redis_pool.get().await?;

    let cached: Option<String> = bb8_redis::redis::AsyncCommands::get(&mut *conn, &key).await?;

    if let Some(cached_str) = cached {
        let bucket: CachedMetricsBucket = serde_json::from_str(&cached_str)?;
        if bucket.is_valid() {
            Ok(Some(bucket))
        } else {
            // Bucket expired, remove it
            let _: () = bb8_redis::redis::AsyncCommands::del(&mut *conn, &key).await?;
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

/// Get multiple buckets for a time range
pub async fn get_cached_metrics_buckets(
    redis_pool: &RedisPool,
    project_id: &str,
    metric_name: &str,
    filters: &HashMap<String, String>,
    start_time: i64,
    end_time: i64,
) -> Result<Vec<CachedMetricsBucket>> {
    let start_bucket = get_time_bucket(start_time);
    let end_bucket = get_time_bucket(end_time);

    let mut buckets = Vec::new();

    for bucket in start_bucket..=end_bucket {
        if let Some(bucket_data) =
            get_cached_metrics_bucket(redis_pool, project_id, metric_name, filters, bucket).await?
        {
            buckets.push(bucket_data);
        }
    }

    Ok(buckets)
}

/// Assemble cached data points for a time range from available buckets
pub async fn get_cached_data_points(
    redis_pool: &RedisPool,
    project_id: &str,
    metric_name: &str,
    filters: &HashMap<String, String>,
    start_time: i64,
    end_time: i64,
) -> Result<Option<Vec<MetricDataPoint>>> {
    let buckets = get_cached_metrics_buckets(
        redis_pool,
        project_id,
        metric_name,
        filters,
        start_time,
        end_time,
    )
    .await?;

    if buckets.is_empty() {
        return Ok(None); // No cached data available
    }

    let mut all_points = Vec::new();

    for bucket in buckets {
        // Filter points to only include those within our time range
        for point in bucket.data_points {
            if point.timestamp_ms >= start_time && point.timestamp_ms < end_time {
                all_points.push(point);
            }
        }
    }

    // Sort by timestamp to ensure proper ordering
    all_points.sort_by_key(|p| p.timestamp_ms);

    Ok(Some(all_points))
}

/// Invalidate metrics cache for a specific metric and filters
pub async fn invalidate_metrics_cache(
    redis_pool: &RedisPool,
    project_id: &str,
    metric_name: &str,
    _filters: &HashMap<String, String>,
) -> Result<()> {
    // For now, we'll use a simple pattern-based invalidation
    // In production, you might want to maintain a set of cache keys per metric
    let pattern = format!("metrics_cache:{}:{}:*", project_id, metric_name);
    let mut conn = redis_pool.get().await?;

    let keys: Vec<String> = bb8_redis::redis::AsyncCommands::keys(&mut *conn, &pattern).await?;

    if !keys.is_empty() {
        let _: () = bb8_redis::redis::AsyncCommands::del(&mut *conn, &keys).await?;
        tracing::debug!(
            "Invalidated {} metrics cache keys for {}/{}",
            keys.len(),
            project_id,
            metric_name
        );
    }

    Ok(())
}

/// Create cache buckets from query results
pub fn create_cache_buckets(
    project_id: &str,
    metric_name: &str,
    filters: &HashMap<String, String>,
    data_points: &[MetricDataPoint],
    ttl_seconds: i64,
) -> Vec<CachedMetricsBucket> {
    let mut bucket_map: HashMap<i64, Vec<MetricDataPoint>> = HashMap::new();

    // Group data points by time bucket
    for point in data_points {
        let bucket = get_time_bucket(point.timestamp_ms);
        bucket_map.entry(bucket).or_default().push(point.clone());
    }

    let cached_at = Utc::now();

    // Create bucket objects
    bucket_map
        .into_iter()
        .map(|(time_bucket, points)| CachedMetricsBucket {
            project_id: project_id.to_string(),
            metric_name: metric_name.to_string(),
            filters: filters.clone(),
            time_bucket,
            data_points: points,
            cached_at,
            ttl: ttl_seconds,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_bucket_calculation() {
        // Test bucket calculation
        assert_eq!(get_time_bucket(0), 0);
        assert_eq!(get_time_bucket(299), 0); // 299 seconds = bucket 0
        assert_eq!(get_time_bucket(300), 1); // 300 seconds = bucket 1
        assert_eq!(get_time_bucket(599), 1); // 599 seconds = bucket 1
        assert_eq!(get_time_bucket(600), 2); // 600 seconds = bucket 2
    }

    #[test]
    fn test_bucket_key_generation() {
        let mut filters = HashMap::new();
        filters.insert("service".to_string(), "api".to_string());
        filters.insert("env".to_string(), "prod".to_string());

        let key = metrics_bucket_key("project1", "http_requests", &filters, 42);

        // Should contain all components
        assert!(key.contains("project1"));
        assert!(key.contains("http_requests"));
        assert!(key.contains("42"));

        // Should be deterministic (same inputs produce same key)
        let key2 = metrics_bucket_key("project1", "http_requests", &filters, 42);
        assert_eq!(key, key2);

        // Different filters should produce different keys
        let mut filters2 = HashMap::new();
        filters2.insert("service".to_string(), "web".to_string());
        let key3 = metrics_bucket_key("project1", "http_requests", &filters2, 42);
        assert_ne!(key, key3);
    }
}
