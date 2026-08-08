//! Statistics Collector
//!
//! Collects statistics from various data sources for cost-based query planning.
//!
//! # Collection Methods
//!
//! - **Sync**: Collect during data sync using HyperLogLog cardinality estimation
//! - **Catalog**: Query database system catalogs (pg_stats for PostgreSQL)
//! - **Metadata**: Extract from Parquet file metadata
//! - **Sample**: Sample rows from external sources

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::time::Instant;

use lru::LruCache;
use tokio::sync::RwLock;
use tracing::{debug, info, trace};
use uuid::Uuid;

use crate::warehouse::indexes::cardinality::{
    ColumnCardinalityEstimator, TableCardinalityEstimator,
};
use crate::warehouse::statistics::StatisticsError;
use crate::warehouse::types::{ColumnType, SourceType};

use super::persistence::{
    CollectionMethod, ColumnStatistics, StatisticsRepository, StatisticsResult, TableStatistics,
};

// ============================================================================
// Cache Constants
// ============================================================================

/// Default TTL for cached statistics (5 minutes)
const DEFAULT_CACHE_TTL_SECS: u64 = 300;

/// Default maximum cache size
const DEFAULT_MAX_CACHE_SIZE: usize = 1000;

// ============================================================================
// Cache Entry
// ============================================================================

/// A cached statistics entry with timestamp for TTL expiration.
#[derive(Debug, Clone)]
struct CacheEntry {
    /// The cached statistics
    stats: TableStatistics,
    /// When this entry was cached
    cached_at: Instant,
}

impl CacheEntry {
    /// Create a new cache entry.
    fn new(stats: TableStatistics) -> Self {
        Self {
            stats,
            cached_at: Instant::now(),
        }
    }

    /// Check if this entry has expired based on TTL.
    fn is_ttl_expired(&self, ttl_secs: u64) -> bool {
        self.cached_at.elapsed().as_secs() > ttl_secs
    }
}

// ============================================================================
// Cache Key
// ============================================================================

/// Key for cache lookups.
type CacheKey = (Uuid, String, String);

// ============================================================================
// Statistics Collector
// ============================================================================

/// Collects and manages statistics for data sources.
///
/// Provides methods to collect statistics from different source types
/// and maintains an LRU cache with TTL expiration for frequently accessed statistics.
pub struct StatisticsCollector {
    /// Repository for persisting statistics
    repository: StatisticsRepository,
    /// LRU cache for hot statistics with TTL
    cache: RwLock<LruCache<CacheKey, CacheEntry>>,
    /// TTL for cached entries in seconds
    cache_ttl_secs: u64,
}

impl StatisticsCollector {
    /// Create a new statistics collector with default settings.
    pub fn new(repository: StatisticsRepository) -> Self {
        Self::with_config(repository, DEFAULT_MAX_CACHE_SIZE, DEFAULT_CACHE_TTL_SECS)
    }

    /// Create a new statistics collector with custom cache configuration.
    pub fn with_config(
        repository: StatisticsRepository,
        max_cache_size: usize,
        cache_ttl_secs: u64,
    ) -> Self {
        let cache_size = NonZeroUsize::new(max_cache_size)
            .unwrap_or(NonZeroUsize::new(DEFAULT_MAX_CACHE_SIZE).unwrap());

        Self {
            repository,
            cache: RwLock::new(LruCache::new(cache_size)),
            cache_ttl_secs,
        }
    }

    /// Get statistics for a table, using LRU cache if available.
    ///
    /// The cache uses both LRU eviction and TTL expiration.
    pub async fn get(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
    ) -> StatisticsResult<Option<TableStatistics>> {
        let cache_key = (project_id, source_name.to_string(), table_name.to_string());

        // Check cache first (uses LRU get which promotes the entry)
        {
            let mut cache = self.cache.write().await;
            if let Some(entry) = cache.get(&cache_key) {
                // Check TTL expiration
                if !entry.is_ttl_expired(self.cache_ttl_secs) && !entry.stats.is_expired() {
                    trace!(
                        project_id = %project_id,
                        source = source_name,
                        table = table_name,
                        "Cache hit for statistics"
                    );
                    return Ok(Some(entry.stats.clone()));
                }
                // Entry expired, remove it
                cache.pop(&cache_key);
            }
        }

        // Load from database
        let stats = self
            .repository
            .get(project_id, source_name, table_name)
            .await?;

        // Update cache if found and not expired
        if let Some(ref stats) = stats {
            if !stats.is_expired() {
                let mut cache = self.cache.write().await;
                cache.put(cache_key, CacheEntry::new(stats.clone()));
                trace!(
                    project_id = %project_id,
                    source = source_name,
                    table = table_name,
                    "Added statistics to cache"
                );
            }
        }

        Ok(stats)
    }

    /// Save statistics and update cache.
    pub async fn save(&self, stats: &TableStatistics) -> StatisticsResult<()> {
        self.repository.save(stats).await?;

        // Update cache (LRU will automatically evict oldest if full)
        let cache_key = (
            stats.project_id,
            stats.source_name.clone(),
            stats.table_name.clone(),
        );
        let mut cache = self.cache.write().await;
        cache.put(cache_key, CacheEntry::new(stats.clone()));

        Ok(())
    }

    /// Invalidate cached statistics for a table.
    pub async fn invalidate(&self, project_id: Uuid, source_name: &str, table_name: &str) {
        let cache_key = (project_id, source_name.to_string(), table_name.to_string());
        let mut cache = self.cache.write().await;
        cache.pop(&cache_key);
    }

    /// Clear the entire cache.
    pub async fn clear_cache(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
    }

    /// Get current cache size.
    pub async fn cache_size(&self) -> usize {
        let cache = self.cache.read().await;
        cache.len()
    }

    /// Evict all TTL-expired entries from the cache.
    ///
    /// This is useful for periodic cleanup to free memory from stale entries.
    pub async fn evict_expired(&self) -> usize {
        let mut cache = self.cache.write().await;
        let before = cache.len();

        // Collect expired keys first
        let expired_keys: Vec<CacheKey> = cache
            .iter()
            .filter(|(_, entry)| {
                entry.is_ttl_expired(self.cache_ttl_secs) || entry.stats.is_expired()
            })
            .map(|(key, _)| key.clone())
            .collect();

        // Remove expired entries
        for key in expired_keys {
            cache.pop(&key);
        }

        let evicted = before - cache.len();
        if evicted > 0 {
            debug!(evicted_count = evicted, "Evicted expired cache entries");
        }

        evicted
    }

    // ========================================================================
    // Collection from Sync
    // ========================================================================

    /// Create statistics from a TableCardinalityEstimator (used during sync).
    pub fn from_sync_estimator(
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
        estimator: &TableCardinalityEstimator,
        total_rows: i64,
        total_bytes: i64,
    ) -> TableStatistics {
        let mut stats =
            TableStatistics::new(project_id, source_name, table_name, CollectionMethod::Sync)
                .with_row_count(total_rows)
                .with_size_bytes(total_bytes);

        stats.confidence = Some(0.95); // High confidence from full sync

        // Convert column estimators to column statistics
        for col_name in estimator.column_names() {
            if let Some(col_est) = estimator.get(col_name) {
                let col_stats = Self::column_stats_from_estimator(col_est);
                stats.add_column_stats(col_name, col_stats);
            }
        }

        stats
    }

    /// Convert a ColumnCardinalityEstimator to ColumnStatistics.
    fn column_stats_from_estimator(estimator: &ColumnCardinalityEstimator) -> ColumnStatistics {
        let mut stats = ColumnStatistics::new().with_distinct_count(estimator.estimate() as i64);

        // Add min/max for numeric columns
        let col_stats = estimator.stats();
        if let (Some(min), Some(max)) = (col_stats.min_value, col_stats.max_value) {
            stats = stats.with_range(min.to_string(), max.to_string());
        }

        stats
    }

    // ========================================================================
    // Collection from Parquet Metadata
    // ========================================================================

    /// Collect statistics from pre-extracted Parquet file stats.
    ///
    /// This is called with stats that have already been extracted during indexing.
    pub fn collect_from_parquet_stats(
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
        file_stats: &[crate::warehouse::parquet_metadata::FileStats],
    ) -> TableStatistics {
        info!(
            project_id = %project_id,
            source = source_name,
            table = table_name,
            file_count = file_stats.len(),
            "Collecting statistics from Parquet metadata"
        );

        let mut total_rows: i64 = 0;
        let mut column_stats: HashMap<String, ColumnStatistics> = HashMap::new();

        for fs in file_stats {
            total_rows += fs.row_count as i64;

            // Merge column stats
            for (col_name, col_stat) in &fs.columns {
                let entry = column_stats
                    .entry(col_name.clone())
                    .or_insert_with(ColumnStatistics::new);

                if let Some(distinct) = col_stat.distinct_count {
                    entry.distinct_count =
                        Some(entry.distinct_count.unwrap_or(0).max(distinct as i64));
                }

                // Accumulate null counts
                if let Some(null_count) = col_stat.null_count {
                    entry.null_count = Some(entry.null_count.unwrap_or(0) + null_count as i64);
                }

                if let Some(ref min) = col_stat.min {
                    let min_str = column_value_to_string(min);
                    entry.min_value = match &entry.min_value {
                        Some(existing) if compare_stat_values(existing, &min_str).is_lt() => {
                            Some(existing.clone())
                        }
                        _ => Some(min_str),
                    };
                }
                if let Some(ref max) = col_stat.max {
                    let max_str = column_value_to_string(max);
                    entry.max_value = match &entry.max_value {
                        Some(existing) if compare_stat_values(existing, &max_str).is_gt() => {
                            Some(existing.clone())
                        }
                        _ => Some(max_str),
                    };
                }
            }
        }

        let mut stats = TableStatistics::new(
            project_id,
            source_name,
            table_name,
            CollectionMethod::Metadata,
        )
        .with_row_count(total_rows)
        .with_file_count(file_stats.len() as i32);

        stats.confidence = Some(0.99); // Very high confidence from metadata
        stats.column_stats = column_stats;

        stats
    }

    // ========================================================================
    // Collection from PostgreSQL Catalog
    // ========================================================================

    /// Collect statistics from PostgreSQL pg_stats.
    pub async fn collect_from_postgres(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
        pg_pool: &sqlx::PgPool,
        schema: Option<&str>,
    ) -> StatisticsResult<TableStatistics> {
        use sqlx::Row;

        let schema_name = schema.unwrap_or("public");

        info!(
            project_id = %project_id,
            source = source_name,
            table = table_name,
            schema = schema_name,
            "Collecting statistics from PostgreSQL catalog"
        );

        // Get table size and row count estimate
        let table_info = sqlx::query(
            r#"
            SELECT 
                pg_total_relation_size($1::regclass) as size_bytes,
                reltuples::bigint as row_count
            FROM pg_class
            WHERE oid = $1::regclass
            "#,
        )
        .bind(format!("{}.{}", schema_name, table_name))
        .fetch_optional(pg_pool)
        .await
        .map_err(|e| StatisticsError::Database(e))?;

        let (size_bytes, row_count) = match table_info {
            Some(row) => (
                row.get::<i64, _>("size_bytes"),
                row.get::<i64, _>("row_count"),
            ),
            None => (0, 0),
        };

        // Get column statistics from pg_stats
        let pg_stats = sqlx::query(
            r#"
            SELECT 
                attname as column_name,
                n_distinct,
                null_frac,
                avg_width,
                most_common_vals::text as mcv,
                most_common_freqs::text as mcf
            FROM pg_stats
            WHERE schemaname = $1 AND tablename = $2
            "#,
        )
        .bind(schema_name)
        .bind(table_name)
        .fetch_all(pg_pool)
        .await
        .map_err(|e| StatisticsError::Database(e))?;

        let mut stats = TableStatistics::new(
            project_id,
            source_name,
            table_name,
            CollectionMethod::Catalog,
        )
        .with_row_count(row_count)
        .with_size_bytes(size_bytes);

        stats.confidence = Some(0.9); // Good confidence from pg_stats

        // Parse column statistics
        for row in pg_stats {
            let column_name: String = row.get("column_name");
            let n_distinct: Option<f32> = row.get("n_distinct");
            let null_frac: Option<f32> = row.get("null_frac");
            let avg_width: Option<i32> = row.get("avg_width");

            let mut col_stats = ColumnStatistics::new();

            // n_distinct: negative = fraction, positive = absolute
            if let Some(nd) = n_distinct {
                col_stats.distinct_count = Some(if nd < 0.0 {
                    (row_count as f32 * nd.abs()) as i64
                } else {
                    nd as i64
                });
            }

            if let Some(nf) = null_frac {
                col_stats.null_fraction = Some(nf);
                col_stats.null_count = Some((row_count as f32 * nf) as i64);
            }

            if let Some(width) = avg_width {
                col_stats.avg_length = Some(width);
            }

            stats.add_column_stats(column_name, col_stats);
        }

        Ok(stats)
    }

    // ========================================================================
    // Sampling Collection
    // ========================================================================

    /// Collect statistics by sampling rows from a source.
    ///
    /// This is used for sources that don't have native statistics support.
    pub async fn collect_from_sample(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
        sample_rows: Vec<HashMap<String, serde_json::Value>>,
        total_row_estimate: Option<i64>,
        sample_rate: f32,
    ) -> StatisticsResult<TableStatistics> {
        info!(
            project_id = %project_id,
            source = source_name,
            table = table_name,
            sample_size = sample_rows.len(),
            sample_rate = sample_rate,
            "Collecting statistics from sample"
        );

        if sample_rows.is_empty() {
            return Ok(TableStatistics::new(
                project_id,
                source_name,
                table_name,
                CollectionMethod::Sample,
            ));
        }

        // Create estimators for each column
        let mut estimator = TableCardinalityEstimator::new();
        let mut total_bytes: usize = 0;

        for row in &sample_rows {
            for (col_name, value) in row {
                // Estimate bytes
                total_bytes += col_name.len() + estimate_json_size(value);

                // Add to cardinality estimator
                match value {
                    serde_json::Value::String(s) => {
                        estimator.add_string(col_name, ColumnType::String, s);
                    }
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            estimator.add_i64(col_name, ColumnType::Int64, i);
                        } else if let Some(f) = n.as_f64() {
                            estimator.add_f64(col_name, ColumnType::Float64, f);
                        }
                    }
                    serde_json::Value::Bool(b) => {
                        estimator.add_string(
                            col_name,
                            ColumnType::Boolean,
                            if *b { "true" } else { "false" },
                        );
                    }
                    serde_json::Value::Null => {
                        // Track nulls separately
                    }
                    _ => {
                        // For objects/arrays, use JSON string
                        estimator.add_string(col_name, ColumnType::String, &value.to_string());
                    }
                }
            }
        }

        // Scale up estimates based on sample rate
        let sample_rows_count = sample_rows.len() as i64;
        let estimated_total_rows = total_row_estimate.unwrap_or_else(|| {
            if sample_rate > 0.0 {
                (sample_rows_count as f32 / sample_rate) as i64
            } else {
                sample_rows_count
            }
        });

        let estimated_total_bytes = if sample_rate > 0.0 {
            (total_bytes as f32 / sample_rate) as i64
        } else {
            total_bytes as i64
        };

        let mut stats = TableStatistics::new(
            project_id,
            source_name,
            table_name,
            CollectionMethod::Sample,
        )
        .with_row_count(estimated_total_rows)
        .with_size_bytes(estimated_total_bytes)
        .with_sample_rate(sample_rate);

        // Lower confidence for sampled data
        stats.confidence = Some(0.7 * sample_rate.sqrt());

        // Convert estimators to column stats (with scaling)
        for col_name in estimator.column_names() {
            if let Some(col_est) = estimator.get(col_name) {
                let mut col_stats = Self::column_stats_from_estimator(col_est);

                // Scale distinct count based on sample rate
                // Use a logarithmic scaling to account for sampling bias
                if let Some(distinct) = col_stats.distinct_count {
                    if sample_rate < 1.0 && sample_rate > 0.0 {
                        let scale_factor = 1.0 / sample_rate.sqrt();
                        col_stats.distinct_count = Some((distinct as f32 * scale_factor) as i64);
                    }
                }

                stats.add_column_stats(col_name, col_stats);
            }
        }

        Ok(stats)
    }

    // ========================================================================
    // Utility Methods
    // ========================================================================

    /// Get statistics or estimate if not available.
    pub async fn get_or_estimate(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
        source_type: SourceType,
    ) -> TableStatistics {
        // Try to get existing statistics
        if let Ok(Some(stats)) = self.get(project_id, source_name, table_name).await {
            if !stats.is_expired() {
                return stats;
            }
        }

        // Fall back to estimates based on source type
        self.estimate_for_source_type(project_id, source_name, table_name, source_type)
    }

    /// Create rough estimates based on source type.
    fn estimate_for_source_type(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
        source_type: SourceType,
    ) -> TableStatistics {
        let (estimated_rows, estimated_bytes) = match source_type {
            // SaaS APIs typically have smaller tables
            SourceType::Stripe | SourceType::HubSpot | SourceType::Salesforce => {
                (100_000i64, 50 * 1024 * 1024i64) // 100K rows, 50MB
            }
            // Databases can be large
            SourceType::PostgreSQL | SourceType::MySQL => {
                (1_000_000, 500 * 1024 * 1024) // 1M rows, 500MB
            }
            // Parquet/Data warehouses can be very large
            SourceType::ExternalParquet | SourceType::Snowflake | SourceType::BigQuery => {
                (10_000_000, 5 * 1024 * 1024 * 1024) // 10M rows, 5GB
            }
            // Derived tables vary wildly — use moderate default (M6)
            SourceType::Derived => {
                (1_000_000, 500 * 1024 * 1024) // 1M rows, 500MB
            }
            // Default moderate estimate
            _ => (500_000, 100 * 1024 * 1024), // 500K rows, 100MB
        };

        let mut stats = TableStatistics::new(
            project_id,
            source_name,
            table_name,
            CollectionMethod::Estimate,
        )
        .with_row_count(estimated_rows)
        .with_size_bytes(estimated_bytes);

        stats.confidence = Some(0.2); // Low confidence for estimates

        stats
    }

    /// Refresh expired statistics for a project.
    pub async fn refresh_expired(&self, project_id: Uuid) -> StatisticsResult<usize> {
        let expired = self.repository.get_expired(project_id).await?;

        debug!(
            project_id = %project_id,
            expired_count = expired.len(),
            "Found expired statistics to refresh"
        );

        // For now, just mark them for refresh. Actual refresh would need
        // access to the data sources.
        Ok(expired.len())
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Convert a ColumnValue to a string for storage.
/// Compare two statistic value strings, using numeric ordering when both
/// values are parseable as `f64`, and falling back to lexicographic ordering
/// otherwise.
fn compare_stat_values(a: &str, b: &str) -> std::cmp::Ordering {
    if let (Ok(na), Ok(nb)) = (a.parse::<f64>(), b.parse::<f64>()) {
        na.total_cmp(&nb)
    } else {
        a.cmp(b)
    }
}

fn column_value_to_string(value: &crate::warehouse::parquet_metadata::ColumnValue) -> String {
    use crate::warehouse::parquet_metadata::ColumnValue;
    match value {
        ColumnValue::Int32(v) => v.to_string(),
        ColumnValue::Int64(v) => v.to_string(),
        ColumnValue::Float32(v) => v.to_string(),
        ColumnValue::Float64(v) => v.to_string(),
        ColumnValue::String(s) => s.clone(),
        ColumnValue::Boolean(b) => b.to_string(),
        ColumnValue::Bytes(b) => format!("{:?}", b),
    }
}

/// Estimate the size of a JSON value in bytes.
fn estimate_json_size(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Null => 4,
        serde_json::Value::Bool(_) => 5,
        serde_json::Value::Number(n) => n.to_string().len(),
        serde_json::Value::String(s) => s.len() + 2,
        serde_json::Value::Array(arr) => {
            arr.iter().map(estimate_json_size).sum::<usize>() + arr.len() + 2
        }
        serde_json::Value::Object(obj) => {
            obj.iter()
                .map(|(k, v)| k.len() + 3 + estimate_json_size(v))
                .sum::<usize>()
                + 2
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_estimate_json_size() {
        assert_eq!(estimate_json_size(&serde_json::Value::Null), 4);
        assert_eq!(estimate_json_size(&serde_json::json!(true)), 5);
        assert_eq!(estimate_json_size(&serde_json::json!(123)), 3);
        assert_eq!(estimate_json_size(&serde_json::json!("hello")), 7);
    }

    #[test]
    fn test_column_stats_from_estimator() {
        let mut estimator = ColumnCardinalityEstimator::new("test", ColumnType::String);
        estimator.add_string("a");
        estimator.add_string("b");
        estimator.add_string("c");

        let stats = StatisticsCollector::column_stats_from_estimator(&estimator);
        assert!(stats.distinct_count.is_some());
        assert!(stats.distinct_count.unwrap() >= 2 && stats.distinct_count.unwrap() <= 4);
    }

    #[test]
    fn test_estimate_for_source_type() {
        // This test would need a mock repository, so we just test the helper function
        let project_id = Uuid::new_v4();

        // Test that different source types get different estimates
        let stripe_estimate = (100_000i64, 50 * 1024 * 1024i64);
        let postgres_estimate = (1_000_000i64, 500 * 1024 * 1024i64);

        assert!(stripe_estimate.0 < postgres_estimate.0);
        assert!(stripe_estimate.1 < postgres_estimate.1);
    }

    #[test]
    fn test_compare_stat_values_numeric_ordering() {
        use std::cmp::Ordering;
        assert_eq!(
            compare_stat_values("5", "100"),
            Ordering::Less,
            "Numeric comparison: 5 < 100"
        );
        assert_eq!(
            compare_stat_values("100", "5"),
            Ordering::Greater,
            "Numeric comparison: 100 > 5"
        );
        assert_eq!(
            compare_stat_values("-3", "2"),
            Ordering::Less,
            "Numeric comparison: -3 < 2"
        );
        assert_eq!(
            compare_stat_values("3.14", "100.0"),
            Ordering::Less,
            "Numeric comparison: 3.14 < 100.0"
        );
    }

    #[test]
    fn test_compare_stat_values_string_fallback() {
        use std::cmp::Ordering;
        assert_eq!(
            compare_stat_values("abc", "def"),
            Ordering::Less,
            "String comparison fallback: abc < def"
        );
        assert_eq!(
            compare_stat_values("def", "abc"),
            Ordering::Greater,
            "String comparison fallback: def > abc"
        );
    }

    #[test]
    fn test_distinct_count_uses_max_not_sum() {
        use crate::warehouse::parquet_metadata::{
            ColumnDataType, ColumnStats, ColumnValue, FileStats,
        };
        use std::collections::HashMap;

        let mut cols_a = HashMap::new();
        cols_a.insert(
            "user_id".to_string(),
            ColumnStats {
                name: "user_id".to_string(),
                data_type: ColumnDataType::Int64,
                distinct_count: Some(100),
                null_count: Some(0),
                min: Some(ColumnValue::Int64(1)),
                max: Some(ColumnValue::Int64(100)),
                row_count: 1000,
            },
        );
        let mut cols_b = HashMap::new();
        cols_b.insert(
            "user_id".to_string(),
            ColumnStats {
                name: "user_id".to_string(),
                data_type: ColumnDataType::Int64,
                distinct_count: Some(80),
                null_count: Some(0),
                min: Some(ColumnValue::Int64(50)),
                max: Some(ColumnValue::Int64(150)),
                row_count: 800,
            },
        );

        let file_stats = vec![
            FileStats {
                file_path: "a.parquet".to_string(),
                row_count: 1000,
                columns: cols_a,
            },
            FileStats {
                file_path: "b.parquet".to_string(),
                row_count: 800,
                columns: cols_b,
            },
        ];

        let table_stats = StatisticsCollector::collect_from_parquet_stats(
            Uuid::new_v4(),
            "src",
            "tbl",
            &file_stats,
        );

        let user_id_stats = table_stats
            .column_stats
            .get("user_id")
            .expect("user_id column must exist in merged stats");

        assert_eq!(
            user_id_stats.distinct_count,
            Some(100),
            "Distinct count must use max (100), not sum (180)"
        );
    }

    #[test]
    fn test_merge_numeric_min_max_correct_ordering() {
        // Regression: string comparison of "5" vs "100" gives wrong result
        // because "5" > "1" lexicographically.
        use crate::warehouse::parquet_metadata::ColumnValue;

        let min_a = column_value_to_string(&ColumnValue::Int64(5));
        let min_b = column_value_to_string(&ColumnValue::Int64(100));

        assert!(
            compare_stat_values(&min_a, &min_b).is_lt(),
            "Min of '5' and '100' must recognize 5 < 100, got: 5={}, 100={}",
            min_a,
            min_b
        );

        let max_a = column_value_to_string(&ColumnValue::Int64(5));
        let max_b = column_value_to_string(&ColumnValue::Int64(100));
        assert!(
            compare_stat_values(&max_b, &max_a).is_gt(),
            "Max of '100' and '5' must recognize 100 > 5"
        );
    }

    #[test]
    fn test_parquet_stats_size_bytes_is_none() {
        use crate::warehouse::parquet_metadata::{
            ColumnDataType, ColumnStats, ColumnValue, FileStats,
        };
        use std::collections::HashMap;

        let mut cols = HashMap::new();
        cols.insert(
            "id".to_string(),
            ColumnStats {
                name: "id".to_string(),
                data_type: ColumnDataType::Int64,
                distinct_count: Some(10),
                null_count: Some(0),
                min: Some(ColumnValue::Int64(1)),
                max: Some(ColumnValue::Int64(10)),
                row_count: 100,
            },
        );

        let file_stats = vec![FileStats {
            file_path: "a.parquet".to_string(),
            row_count: 100,
            columns: cols,
        }];

        let table_stats = StatisticsCollector::collect_from_parquet_stats(
            Uuid::new_v4(),
            "src",
            "tbl",
            &file_stats,
        );

        // size_bytes must be None (unknown), not Some(0), since Parquet
        // metadata does not provide file sizes. Some(0) would cause
        // bytes_per_row() to return Some(0), misleading the cost model.
        assert!(
            table_stats.size_bytes.is_none(),
            "size_bytes should be None when not computable from Parquet metadata, got {:?}",
            table_stats.size_bytes,
        );
    }

    #[test]
    fn test_compare_stat_values_nan_does_not_overwrite_valid() {
        use std::cmp::Ordering;

        // NaN should be treated as greater than any number, so a valid min is
        // always preferred over NaN during merging.
        assert_eq!(
            compare_stat_values("50", "NaN"),
            Ordering::Less,
            "A valid number should compare as less than NaN"
        );
        assert_eq!(
            compare_stat_values("NaN", "50"),
            Ordering::Greater,
            "NaN should compare as greater than a valid number"
        );
        assert_eq!(
            compare_stat_values("NaN", "NaN"),
            Ordering::Equal,
            "Two NaNs should compare as equal"
        );
    }
}
