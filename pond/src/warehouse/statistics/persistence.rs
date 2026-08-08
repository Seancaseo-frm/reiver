//! Statistics Persistence Layer
//!
//! Provides database access for storing and retrieving table and column statistics.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use thiserror::Error;
use uuid::Uuid;

// ============================================================================
// Errors
// ============================================================================

/// Errors that can occur during statistics operations.
#[derive(Debug, Error)]
pub enum StatisticsError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Statistics not found for {source_name}.{table_name}")]
    NotFound {
        source_name: String,
        table_name: String,
    },

    #[error("Invalid statistics data: {0}")]
    InvalidData(String),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Result type for statistics operations.
pub type StatisticsResult<T> = Result<T, StatisticsError>;

// ============================================================================
// Collection Method
// ============================================================================

/// How statistics were collected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CollectionMethod {
    /// Collected during data sync
    Sync,
    /// Collected by sampling rows
    Sample,
    /// Extracted from file metadata (Parquet)
    Metadata,
    /// Queried from database catalog (pg_stats)
    Catalog,
    /// Rough estimate based on file size
    Estimate,
}

impl CollectionMethod {
    /// Convert to database enum string.
    pub fn as_str(&self) -> &'static str {
        match self {
            CollectionMethod::Sync => "sync",
            CollectionMethod::Sample => "sample",
            CollectionMethod::Metadata => "metadata",
            CollectionMethod::Catalog => "catalog",
            CollectionMethod::Estimate => "estimate",
        }
    }

    /// Parse from database enum string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "sync" => Some(CollectionMethod::Sync),
            "sample" => Some(CollectionMethod::Sample),
            "metadata" => Some(CollectionMethod::Metadata),
            "catalog" => Some(CollectionMethod::Catalog),
            "estimate" => Some(CollectionMethod::Estimate),
            _ => None,
        }
    }
}

impl Default for CollectionMethod {
    fn default() -> Self {
        CollectionMethod::Estimate
    }
}

// ============================================================================
// Table Statistics
// ============================================================================

/// Statistics for a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStatistics {
    /// Unique identifier
    pub id: Uuid,
    /// Project ID
    pub project_id: Uuid,
    /// Source name (e.g., "stripe", "postgres")
    pub source_name: String,
    /// Table name
    pub table_name: String,

    /// Estimated row count
    pub row_count: Option<i64>,
    /// Estimated size in bytes
    pub size_bytes: Option<i64>,
    /// Average row size in bytes
    pub avg_row_size_bytes: Option<i32>,
    /// Number of files (for Parquet sources)
    pub file_count: Option<i32>,

    /// How statistics were collected
    pub collection_method: CollectionMethod,
    /// Sample rate (0.0-1.0) for sampled stats
    pub sample_rate: Option<f32>,
    /// Confidence level (0.0-1.0)
    pub confidence: Option<f32>,

    /// When statistics were collected
    pub collected_at: DateTime<Utc>,
    /// When statistics expire
    pub expires_at: Option<DateTime<Utc>>,

    /// Per-column statistics
    #[serde(default)]
    pub column_stats: HashMap<String, ColumnStatistics>,
}

impl TableStatistics {
    /// Create new table statistics.
    pub fn new(
        project_id: Uuid,
        source_name: impl Into<String>,
        table_name: impl Into<String>,
        method: CollectionMethod,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            project_id,
            source_name: source_name.into(),
            table_name: table_name.into(),
            row_count: None,
            size_bytes: None,
            avg_row_size_bytes: None,
            file_count: None,
            collection_method: method,
            sample_rate: None,
            confidence: None,
            collected_at: Utc::now(),
            expires_at: None,
            column_stats: HashMap::new(),
        }
    }

    /// Set row count.
    pub fn with_row_count(mut self, count: i64) -> Self {
        self.row_count = Some(count);
        self
    }

    /// Set size in bytes.
    pub fn with_size_bytes(mut self, size: i64) -> Self {
        self.size_bytes = Some(size);
        self
    }

    /// Set file count.
    pub fn with_file_count(mut self, count: i32) -> Self {
        self.file_count = Some(count);
        self
    }

    /// Set sample rate.
    pub fn with_sample_rate(mut self, rate: f32) -> Self {
        self.sample_rate = Some(rate);
        self
    }

    /// Add column statistics.
    pub fn add_column_stats(&mut self, column_name: impl Into<String>, stats: ColumnStatistics) {
        self.column_stats.insert(column_name.into(), stats);
    }

    /// Check if statistics are expired.
    pub fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(expires) => Utc::now() > expires,
            None => false,
        }
    }

    /// Get estimated size per row.
    pub fn bytes_per_row(&self) -> Option<i64> {
        match (self.size_bytes, self.row_count) {
            (Some(size), Some(rows)) if rows > 0 => Some(size / rows),
            _ => self.avg_row_size_bytes.map(|s| s as i64),
        }
    }
}

// ============================================================================
// Column Statistics
// ============================================================================

/// Statistics for a column.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ColumnStatistics {
    /// Estimated number of distinct values
    pub distinct_count: Option<i64>,
    /// Number of NULL values
    pub null_count: Option<i64>,
    /// Fraction of NULLs (0.0-1.0)
    pub null_fraction: Option<f32>,

    /// Minimum value (as string for any type)
    pub min_value: Option<String>,
    /// Maximum value (as string for any type)
    pub max_value: Option<String>,
    /// Average length for string columns
    pub avg_length: Option<i32>,

    /// Histogram bounds (for range queries)
    #[serde(default)]
    pub histogram_bounds: Vec<String>,

    /// Most common values with frequencies
    #[serde(default)]
    pub most_common_values: Vec<CommonValue>,
}

impl ColumnStatistics {
    /// Create new column statistics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set distinct count.
    pub fn with_distinct_count(mut self, count: i64) -> Self {
        self.distinct_count = Some(count);
        self
    }

    /// Set null statistics.
    pub fn with_nulls(mut self, count: i64, fraction: f32) -> Self {
        self.null_count = Some(count);
        self.null_fraction = Some(fraction);
        self
    }

    /// Set min/max values.
    pub fn with_range(mut self, min: impl Into<String>, max: impl Into<String>) -> Self {
        self.min_value = Some(min.into());
        self.max_value = Some(max.into());
        self
    }

    /// Calculate selectivity for an equality predicate.
    ///
    /// Returns estimated fraction of rows that match `column = value`.
    pub fn estimate_equality_selectivity(&self, value: &str) -> f64 {
        // Check most common values first
        for cv in &self.most_common_values {
            if cv.value == value {
                return cv.frequency as f64;
            }
        }

        // Fall back to 1/distinct_count
        match self.distinct_count {
            Some(distinct) if distinct > 0 => 1.0 / distinct as f64,
            _ => 0.1, // Default 10% selectivity
        }
    }

    /// Compare two statistic value strings, using numeric ordering when both
    /// values are parseable as `f64`, and falling back to lexicographic ordering
    /// otherwise.
    fn compare_values(a: &str, b: &str) -> std::cmp::Ordering {
        if let (Ok(na), Ok(nb)) = (a.parse::<f64>(), b.parse::<f64>()) {
            na.total_cmp(&nb)
        } else {
            a.cmp(b)
        }
    }

    /// Calculate selectivity for a range predicate.
    ///
    /// Returns estimated fraction of rows where `column >= low AND column <= high`.
    pub fn estimate_range_selectivity(&self, low: &str, high: &str) -> f64 {
        // If we have histogram bounds, use them for a more accurate estimate
        if self.histogram_bounds.len() >= 2 {
            return self.estimate_range_from_histogram(low, high);
        }

        // Fall back to min/max bounds if available
        if let (Some(min_val), Some(max_val)) = (&self.min_value, &self.max_value) {
            return self.estimate_range_from_minmax(low, high, min_val, max_val);
        }

        // Default heuristic: 33% for range queries
        0.33
    }

    /// Estimate range selectivity using histogram bounds.
    ///
    /// Histogram bounds represent bucket boundaries. For N bounds, there are N-1 buckets,
    /// each containing approximately 1/(N-1) of the data.
    fn estimate_range_from_histogram(&self, low: &str, high: &str) -> f64 {
        let bounds = &self.histogram_bounds;
        let num_buckets = bounds.len() - 1;

        if num_buckets == 0 {
            return 0.33;
        }

        let bucket_selectivity = 1.0 / num_buckets as f64;

        // Find first bucket that overlaps with [low, high]
        let mut overlapping_buckets = 0.0;

        for i in 0..num_buckets {
            let bucket_low = &bounds[i];
            let bucket_high = &bounds[i + 1];

            // Check if range [low, high] overlaps with bucket [bucket_low, bucket_high]
            // Overlap occurs if: low <= bucket_high AND high >= bucket_low
            if !Self::compare_values(low, bucket_high).is_gt()
                && !Self::compare_values(high, bucket_low).is_lt()
            {
                // Calculate partial overlap within the bucket
                let overlap = self.calculate_bucket_overlap(low, high, bucket_low, bucket_high);
                overlapping_buckets += overlap * bucket_selectivity;
            }
        }

        // Clamp to valid range
        overlapping_buckets.clamp(0.01, 1.0)
    }

    /// Calculate the fraction of a bucket that overlaps with the query range.
    fn calculate_bucket_overlap(
        &self,
        query_low: &str,
        query_high: &str,
        bucket_low: &str,
        bucket_high: &str,
    ) -> f64 {
        // Full overlap if query range contains entire bucket
        if !Self::compare_values(query_low, bucket_low).is_gt()
            && !Self::compare_values(query_high, bucket_high).is_lt()
        {
            return 1.0;
        }

        // Partial overlap - use linear interpolation
        // This is a simplification; works best for numeric data
        let effective_low = if Self::compare_values(query_low, bucket_low).is_gt() {
            query_low
        } else {
            bucket_low
        };
        let effective_high = if Self::compare_values(query_high, bucket_high).is_lt() {
            query_high
        } else {
            bucket_high
        };

        // For string comparison, we can't easily interpolate
        // Use a conservative estimate of 0.5 for partial overlap
        if effective_low == bucket_low && effective_high == bucket_high {
            1.0
        } else if effective_low == bucket_low || effective_high == bucket_high {
            0.75 // One edge matches
        } else {
            0.5 // Partial overlap in the middle
        }
    }

    /// Estimate range selectivity from min/max values.
    fn estimate_range_from_minmax(
        &self,
        low: &str,
        high: &str,
        min_val: &str,
        max_val: &str,
    ) -> f64 {
        // If query range is completely outside data range, return 0
        if Self::compare_values(low, max_val).is_gt() || Self::compare_values(high, min_val).is_lt()
        {
            return 0.0;
        }

        // If query range contains entire data range, return 1
        if !Self::compare_values(low, min_val).is_gt()
            && !Self::compare_values(high, max_val).is_lt()
        {
            return 1.0;
        }

        // Linear interpolation for partial overlap
        // This is approximate and works best for uniformly distributed data
        let effective_low = if Self::compare_values(low, min_val).is_gt() {
            low
        } else {
            min_val
        };
        let effective_high = if Self::compare_values(high, max_val).is_lt() {
            high
        } else {
            max_val
        };

        // Rough heuristic based on whether we're taking a subset
        if effective_low == min_val && effective_high == max_val {
            1.0
        } else if effective_low == min_val || effective_high == max_val {
            0.5 // Half the range
        } else {
            0.33 // Interior range
        }
    }

    /// Estimate selectivity for a less-than predicate.
    pub fn estimate_lt_selectivity(&self, value: &str) -> f64 {
        if let Some(min_val) = &self.min_value {
            if !Self::compare_values(value, min_val.as_str()).is_gt() {
                return 0.0;
            }
        }
        if let Some(max_val) = &self.max_value {
            if !Self::compare_values(value, max_val.as_str()).is_lt() {
                return 1.0;
            }
        }

        // Use histogram if available
        if !self.histogram_bounds.is_empty() {
            if let Some(first) = self.histogram_bounds.first() {
                return self.estimate_range_selectivity(first, value);
            }
        }

        0.33 // Default
    }

    /// Estimate selectivity for a greater-than predicate.
    pub fn estimate_gt_selectivity(&self, value: &str) -> f64 {
        if let Some(max_val) = &self.max_value {
            if !Self::compare_values(value, max_val.as_str()).is_lt() {
                return 0.0;
            }
        }
        if let Some(min_val) = &self.min_value {
            if !Self::compare_values(value, min_val.as_str()).is_gt() {
                return 1.0;
            }
        }

        // Use histogram if available
        if !self.histogram_bounds.is_empty() {
            if let Some(last) = self.histogram_bounds.last() {
                return self.estimate_range_selectivity(value, last);
            }
        }

        0.33 // Default
    }
}

/// A common value with its frequency.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommonValue {
    /// The value (as string)
    pub value: String,
    /// Frequency (0.0-1.0)
    pub frequency: f32,
}

// ============================================================================
// Statistics Repository
// ============================================================================

/// Repository for persisting and retrieving statistics.
pub struct StatisticsRepository {
    db: Arc<PgPool>,
}

impl StatisticsRepository {
    /// Create a new statistics repository.
    pub fn new(db: Arc<PgPool>) -> Self {
        Self { db }
    }

    /// Get statistics for a table.
    pub async fn get(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
    ) -> StatisticsResult<Option<TableStatistics>> {
        let row = sqlx::query(
            r#"
            SELECT 
                id, project_id, source_name, table_name,
                row_count, size_bytes, avg_row_size_bytes, file_count,
                collection_method::text, sample_rate, confidence,
                collected_at, expires_at
            FROM warehouse_table_statistics
            WHERE project_id = $1 AND source_name = $2 AND table_name = $3
            "#,
        )
        .bind(project_id)
        .bind(source_name)
        .bind(table_name)
        .fetch_optional(self.db.as_ref())
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let mut stats = self.row_to_table_stats(&row)?;

        // Load column statistics
        stats.column_stats = self.get_column_stats(stats.id).await?;

        Ok(Some(stats))
    }

    /// Get all statistics for a project.
    ///
    /// Uses batch loading for column statistics to avoid N+1 queries.
    pub async fn get_all_for_project(
        &self,
        project_id: Uuid,
    ) -> StatisticsResult<Vec<TableStatistics>> {
        let rows = sqlx::query(
            r#"
            SELECT 
                id, project_id, source_name, table_name,
                row_count, size_bytes, avg_row_size_bytes, file_count,
                collection_method::text, sample_rate, confidence,
                collected_at, expires_at
            FROM warehouse_table_statistics
            WHERE project_id = $1
            ORDER BY source_name, table_name
            "#,
        )
        .bind(project_id)
        .fetch_all(self.db.as_ref())
        .await?;

        // Parse table stats first
        let mut results = Vec::with_capacity(rows.len());
        let mut table_ids = Vec::with_capacity(rows.len());
        for row in rows {
            let stats = self.row_to_table_stats(&row)?;
            table_ids.push(stats.id);
            results.push(stats);
        }

        // Batch load all column statistics in one query
        if !table_ids.is_empty() {
            let column_stats_map = self.get_column_stats_batch(&table_ids).await?;

            // Assign column stats to each table
            for stats in &mut results {
                if let Some(col_stats) = column_stats_map.get(&stats.id) {
                    stats.column_stats = col_stats.clone();
                }
            }
        }

        Ok(results)
    }

    /// Get expired statistics that need refreshing.
    pub async fn get_expired(&self, project_id: Uuid) -> StatisticsResult<Vec<TableStatistics>> {
        let rows = sqlx::query(
            r#"
            SELECT 
                id, project_id, source_name, table_name,
                row_count, size_bytes, avg_row_size_bytes, file_count,
                collection_method::text, sample_rate, confidence,
                collected_at, expires_at
            FROM warehouse_table_statistics
            WHERE project_id = $1 AND expires_at < now()
            ORDER BY expires_at
            "#,
        )
        .bind(project_id)
        .fetch_all(self.db.as_ref())
        .await?;

        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            let stats = self.row_to_table_stats(&row)?;
            results.push(stats);
        }

        Ok(results)
    }

    /// Save or update statistics for a table.
    pub async fn save(&self, stats: &TableStatistics) -> StatisticsResult<()> {
        // Upsert table statistics
        sqlx::query(
            r#"
            INSERT INTO warehouse_table_statistics (
                id, project_id, source_name, table_name,
                row_count, size_bytes, avg_row_size_bytes, file_count,
                collection_method, sample_rate, confidence,
                collected_at, expires_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::statistics_collection_method, $10, $11, $12, $13)
            ON CONFLICT (project_id, source_name, table_name)
            DO UPDATE SET
                row_count = EXCLUDED.row_count,
                size_bytes = EXCLUDED.size_bytes,
                avg_row_size_bytes = EXCLUDED.avg_row_size_bytes,
                file_count = EXCLUDED.file_count,
                collection_method = EXCLUDED.collection_method,
                sample_rate = EXCLUDED.sample_rate,
                confidence = EXCLUDED.confidence,
                collected_at = EXCLUDED.collected_at,
                expires_at = NULL  -- Let trigger set it
            "#,
        )
        .bind(stats.id)
        .bind(stats.project_id)
        .bind(&stats.source_name)
        .bind(&stats.table_name)
        .bind(stats.row_count)
        .bind(stats.size_bytes)
        .bind(stats.avg_row_size_bytes)
        .bind(stats.file_count)
        .bind(stats.collection_method.as_str())
        .bind(stats.sample_rate)
        .bind(stats.confidence)
        .bind(stats.collected_at)
        .bind(stats.expires_at)
        .execute(self.db.as_ref())
        .await?;

        // Get the actual ID (might be different if this was an update)
        let actual_id: Uuid = sqlx::query_scalar(
            r#"
            SELECT id FROM warehouse_table_statistics
            WHERE project_id = $1 AND source_name = $2 AND table_name = $3
            "#,
        )
        .bind(stats.project_id)
        .bind(&stats.source_name)
        .bind(&stats.table_name)
        .fetch_one(self.db.as_ref())
        .await?;

        // Save column statistics
        for (column_name, col_stats) in &stats.column_stats {
            self.save_column_stats(actual_id, column_name, col_stats)
                .await?;
        }

        Ok(())
    }

    /// Delete statistics for a table.
    pub async fn delete(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
    ) -> StatisticsResult<bool> {
        let result = sqlx::query(
            r#"
            DELETE FROM warehouse_table_statistics
            WHERE project_id = $1 AND source_name = $2 AND table_name = $3
            "#,
        )
        .bind(project_id)
        .bind(source_name)
        .bind(table_name)
        .execute(self.db.as_ref())
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Delete all statistics for a source.
    pub async fn delete_source(
        &self,
        project_id: Uuid,
        source_name: &str,
    ) -> StatisticsResult<u64> {
        let result = sqlx::query(
            r#"
            DELETE FROM warehouse_table_statistics
            WHERE project_id = $1 AND source_name = $2
            "#,
        )
        .bind(project_id)
        .bind(source_name)
        .execute(self.db.as_ref())
        .await?;

        Ok(result.rows_affected())
    }

    // ========================================================================
    // Private helpers
    // ========================================================================

    fn row_to_table_stats(&self, row: &sqlx::postgres::PgRow) -> StatisticsResult<TableStatistics> {
        let method_str: String = row.get("collection_method");
        let method = CollectionMethod::from_str(&method_str).ok_or_else(|| {
            StatisticsError::InvalidData(format!("Invalid method: {}", method_str))
        })?;

        Ok(TableStatistics {
            id: row.get("id"),
            project_id: row.get("project_id"),
            source_name: row.get("source_name"),
            table_name: row.get("table_name"),
            row_count: row.get("row_count"),
            size_bytes: row.get("size_bytes"),
            avg_row_size_bytes: row.get("avg_row_size_bytes"),
            file_count: row.get("file_count"),
            collection_method: method,
            sample_rate: row.get("sample_rate"),
            confidence: row.get("confidence"),
            collected_at: row.get("collected_at"),
            expires_at: row.get("expires_at"),
            column_stats: HashMap::new(),
        })
    }

    async fn get_column_stats(
        &self,
        table_stats_id: Uuid,
    ) -> StatisticsResult<HashMap<String, ColumnStatistics>> {
        let rows = sqlx::query(
            r#"
            SELECT 
                column_name, distinct_count, null_count, null_fraction,
                min_value, max_value, avg_length,
                histogram_bounds, most_common_values
            FROM warehouse_column_statistics
            WHERE table_stats_id = $1
            "#,
        )
        .bind(table_stats_id)
        .fetch_all(self.db.as_ref())
        .await?;

        let mut result = HashMap::new();
        for row in rows {
            let column_name: String = row.get("column_name");
            let stats = Self::row_to_column_stats(&row);
            result.insert(column_name, stats);
        }

        Ok(result)
    }

    /// Batch load column statistics for multiple tables.
    ///
    /// This avoids N+1 queries when loading statistics for many tables.
    async fn get_column_stats_batch(
        &self,
        table_stats_ids: &[Uuid],
    ) -> StatisticsResult<HashMap<Uuid, HashMap<String, ColumnStatistics>>> {
        if table_stats_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let rows = sqlx::query(
            r#"
            SELECT 
                table_stats_id, column_name, distinct_count, null_count, null_fraction,
                min_value, max_value, avg_length,
                histogram_bounds, most_common_values
            FROM warehouse_column_statistics
            WHERE table_stats_id = ANY($1)
            "#,
        )
        .bind(table_stats_ids)
        .fetch_all(self.db.as_ref())
        .await?;

        let mut result: HashMap<Uuid, HashMap<String, ColumnStatistics>> = HashMap::new();
        for row in rows {
            let table_id: Uuid = row.get("table_stats_id");
            let column_name: String = row.get("column_name");
            let stats = Self::row_to_column_stats(&row);

            result
                .entry(table_id)
                .or_insert_with(HashMap::new)
                .insert(column_name, stats);
        }

        Ok(result)
    }

    /// Parse a row into ColumnStatistics.
    fn row_to_column_stats(row: &sqlx::postgres::PgRow) -> ColumnStatistics {
        let histogram_bounds: Vec<String> = row
            .get::<Option<serde_json::Value>, _>("histogram_bounds")
            .and_then(|v| serde_json::from_value(v).ok())
            .unwrap_or_default();

        let most_common_values: Vec<CommonValue> = row
            .get::<Option<serde_json::Value>, _>("most_common_values")
            .and_then(|v| serde_json::from_value(v).ok())
            .unwrap_or_default();

        ColumnStatistics {
            distinct_count: row.get("distinct_count"),
            null_count: row.get("null_count"),
            null_fraction: row.get("null_fraction"),
            min_value: row.get("min_value"),
            max_value: row.get("max_value"),
            avg_length: row.get("avg_length"),
            histogram_bounds,
            most_common_values,
        }
    }

    async fn save_column_stats(
        &self,
        table_stats_id: Uuid,
        column_name: &str,
        stats: &ColumnStatistics,
    ) -> StatisticsResult<()> {
        let histogram_json = serde_json::to_value(&stats.histogram_bounds)?;
        let mcv_json = serde_json::to_value(&stats.most_common_values)?;

        sqlx::query(
            r#"
            INSERT INTO warehouse_column_statistics (
                table_stats_id, column_name,
                distinct_count, null_count, null_fraction,
                min_value, max_value, avg_length,
                histogram_bounds, most_common_values
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (table_stats_id, column_name)
            DO UPDATE SET
                distinct_count = EXCLUDED.distinct_count,
                null_count = EXCLUDED.null_count,
                null_fraction = EXCLUDED.null_fraction,
                min_value = EXCLUDED.min_value,
                max_value = EXCLUDED.max_value,
                avg_length = EXCLUDED.avg_length,
                histogram_bounds = EXCLUDED.histogram_bounds,
                most_common_values = EXCLUDED.most_common_values
            "#,
        )
        .bind(table_stats_id)
        .bind(column_name)
        .bind(stats.distinct_count)
        .bind(stats.null_count)
        .bind(stats.null_fraction)
        .bind(&stats.min_value)
        .bind(&stats.max_value)
        .bind(stats.avg_length)
        .bind(&histogram_json)
        .bind(&mcv_json)
        .execute(self.db.as_ref())
        .await?;

        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collection_method_roundtrip() {
        for method in [
            CollectionMethod::Sync,
            CollectionMethod::Sample,
            CollectionMethod::Metadata,
            CollectionMethod::Catalog,
            CollectionMethod::Estimate,
        ] {
            let s = method.as_str();
            let parsed = CollectionMethod::from_str(s).unwrap();
            assert_eq!(method, parsed);
        }
    }

    #[test]
    fn test_table_statistics_builder() {
        let project_id = Uuid::new_v4();
        let stats = TableStatistics::new(project_id, "stripe", "customers", CollectionMethod::Sync)
            .with_row_count(10000)
            .with_size_bytes(1024 * 1024);

        assert_eq!(stats.project_id, project_id);
        assert_eq!(stats.source_name, "stripe");
        assert_eq!(stats.table_name, "customers");
        assert_eq!(stats.row_count, Some(10000));
        assert_eq!(stats.size_bytes, Some(1024 * 1024));
        assert_eq!(stats.bytes_per_row(), Some(104)); // 1MB / 10000
    }

    #[test]
    fn test_column_statistics_selectivity() {
        let mut stats = ColumnStatistics::new().with_distinct_count(100);

        // Without MCV, use 1/distinct
        assert!((stats.estimate_equality_selectivity("foo") - 0.01).abs() < 0.001);

        // With MCV, use actual frequency
        stats.most_common_values.push(CommonValue {
            value: "bar".to_string(),
            frequency: 0.25,
        });
        assert!((stats.estimate_equality_selectivity("bar") - 0.25).abs() < 0.001);
    }

    #[test]
    fn test_table_statistics_expired() {
        let project_id = Uuid::new_v4();
        let mut stats = TableStatistics::new(project_id, "test", "table", CollectionMethod::Sync);

        // No expiry set
        assert!(!stats.is_expired());

        // Future expiry
        stats.expires_at = Some(Utc::now() + chrono::Duration::hours(1));
        assert!(!stats.is_expired());

        // Past expiry
        stats.expires_at = Some(Utc::now() - chrono::Duration::hours(1));
        assert!(stats.is_expired());
    }

    #[test]
    fn test_histogram_selectivity_weights_by_bucket() {
        let stats = ColumnStatistics {
            histogram_bounds: vec![
                "00".into(),
                "10".into(),
                "20".into(),
                "30".into(),
                "40".into(),
                "50".into(),
                "60".into(),
                "70".into(),
                "80".into(),
                "90".into(),
            ],
            ..Default::default()
        };

        // 9 buckets. Range "10"-"40" fully covers 3 buckets and partially
        // covers 2 edge buckets (0.75 each), giving (3*1.0 + 2*0.75)/9 = 0.5.
        // Before the fix the raw overlap (4.5) was returned and clamped to 1.0.
        let selectivity = stats.estimate_range_selectivity("10", "40");
        assert!(
            selectivity < 0.6,
            "Selectivity should be weighted by bucket fraction, got {:.4}",
            selectivity,
        );
        let expected = 4.5 / 9.0;
        assert!(
            (selectivity - expected).abs() < 0.01,
            "Expected selectivity ~{:.4}, got {:.4}",
            expected,
            selectivity,
        );

        // Full range should return ~1.0
        let full = stats.estimate_range_selectivity("00", "90");
        assert!(
            (full - 1.0).abs() < 0.01,
            "Full-range selectivity should be ~1.0, got {:.4}",
            full,
        );
    }

    #[test]
    fn test_numeric_selectivity_not_lexicographic() {
        // min=100, max=1000 — value "5" is numerically < 100, so gt should be ~1.0.
        // With lexicographic comparison, "5" > "1000" which wrongly returns 0.0.
        let stats = ColumnStatistics::new().with_range("100", "1000");

        let gt_sel = stats.estimate_gt_selectivity("5");
        assert!(
            (gt_sel - 1.0).abs() < 1e-6,
            "GT selectivity for 5 with range [100,1000] should be ~1.0 (all values > 5), got {gt_sel}"
        );

        let lt_sel = stats.estimate_lt_selectivity("5");
        assert!(
            lt_sel < 1e-6,
            "LT selectivity for 5 with range [100,1000] should be 0.0 (no values < 5), got {lt_sel}"
        );

        // "5000" is numerically > 1000, so gt should be 0.0
        let gt_above = stats.estimate_gt_selectivity("5000");
        assert!(
            gt_above < 1e-6,
            "GT selectivity for 5000 with range [100,1000] should be 0.0, got {gt_above}"
        );

        let lt_above = stats.estimate_lt_selectivity("5000");
        assert!(
            (lt_above - 1.0).abs() < 1e-6,
            "LT selectivity for 5000 with range [100,1000] should be ~1.0, got {lt_above}"
        );
    }
}
