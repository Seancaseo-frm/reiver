//! Cardinality Estimation
//!
//! HyperLogLog-based cardinality estimation for automatic index strategy selection.
//!
//! # Purpose
//!
//! During data sync, we need to decide which index type to use for each column:
//! - Low cardinality → FST (exact queries)
//! - High cardinality → Xor Filter (probabilistic)
//! - Very high cardinality → Skip indexing
//!
//! HyperLogLog provides O(1) space cardinality estimation with ~1-2% error,
//! allowing us to make this decision without storing all distinct values.
//!
//! # Memory Usage
//!
//! A single CardinalityEstimator uses approximately 1.6KB of memory regardless
//! of how many values are added. This makes it practical to track cardinality
//! for every column during sync.
//!
//! # Usage Example
//!
//! ```ignore
//! let mut estimator = ColumnCardinalityEstimator::new("user_id");
//!
//! // Add values during sync
//! for row in rows {
//!     estimator.add(&row.user_id);
//! }
//!
//! // Get estimated cardinality
//! let cardinality = estimator.estimate();
//!
//! // Decide index strategy
//! let strategy = IndexStrategy::from_stats(ColumnType::String, cardinality, row_count);
//! ```

use cardinality_estimator::CardinalityEstimator;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use wyhash::WyHash;

use crate::warehouse::types::ColumnType;

use super::strategy::{ColumnStats, IndexStrategy};

/// Type alias for our HyperLogLog estimator configuration.
/// - `u64`: We hash all values to u64 before insertion
/// - `WyHash`: Fast non-cryptographic hasher
/// - `12`: Precision parameter (2^12 = 4096 registers, ~1.6KB memory)
/// - `6`: Width parameter (6 bits per register)
type HllEstimator = CardinalityEstimator<u64, WyHash, 12, 6>;

/// Cardinality estimator for a single column.
///
/// Uses HyperLogLog to estimate the number of distinct values with ~1-2% error.
/// Memory usage is approximately 1.6KB regardless of data size.
pub struct ColumnCardinalityEstimator {
    /// Column name
    column_name: String,
    /// Column data type
    data_type: ColumnType,
    /// HyperLogLog estimator
    estimator: HllEstimator,
    /// Row count
    row_count: usize,
    /// For numeric columns: min value observed
    min_value: Option<f64>,
    /// For numeric columns: max value observed
    max_value: Option<f64>,
}

impl ColumnCardinalityEstimator {
    /// Create a new cardinality estimator for a column.
    pub fn new(column_name: &str, data_type: ColumnType) -> Self {
        Self {
            column_name: column_name.to_string(),
            data_type,
            estimator: CardinalityEstimator::new(),
            row_count: 0,
            min_value: None,
            max_value: None,
        }
    }

    /// Add a string value to the estimator.
    pub fn add_string(&mut self, value: &str) {
        self.row_count += 1;
        let hash = Self::hash_value(value);
        self.estimator.insert(&hash);
    }

    /// Add an integer value to the estimator.
    pub fn add_i64(&mut self, value: i64) {
        self.row_count += 1;
        // Convert to u64 for HLL (preserves uniqueness)
        self.estimator.insert(&(value as u64));
        self.update_numeric_bounds(value as f64);
    }

    /// Add a float value to the estimator.
    pub fn add_f64(&mut self, value: f64) {
        self.row_count += 1;
        // Hash the float bits for HLL
        let bits = value.to_bits();
        self.estimator.insert(&bits);
        self.update_numeric_bounds(value);
    }

    /// Update min/max bounds for numeric columns.
    fn update_numeric_bounds(&mut self, value: f64) {
        self.min_value = Some(match self.min_value {
            Some(min) => min.min(value),
            None => value,
        });
        self.max_value = Some(match self.max_value {
            Some(max) => max.max(value),
            None => value,
        });
    }

    /// Hash a string value for HLL insertion.
    fn hash_value(value: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    /// Get the estimated cardinality.
    ///
    /// Returns the estimated number of distinct values with ~1-2% error.
    pub fn estimate(&self) -> usize {
        self.estimator.estimate() as usize
    }

    /// Get the row count.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Get the column name.
    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    /// Get the data type.
    pub fn data_type(&self) -> ColumnType {
        self.data_type
    }

    /// Calculate selectivity (cardinality / row_count).
    ///
    /// Returns a value between 0.0 and 1.0:
    /// - 0.0 = all values are the same (low cardinality)
    /// - 1.0 = all values are unique (high cardinality)
    pub fn selectivity(&self) -> f64 {
        if self.row_count == 0 {
            return 1.0;
        }
        let cardinality = self.estimate();
        (cardinality as f64 / self.row_count as f64).min(1.0)
    }

    /// Get the recommended index strategy based on observed data.
    pub fn recommended_strategy(&self) -> IndexStrategy {
        IndexStrategy::from_stats(self.data_type, self.estimate(), self.row_count)
    }

    /// Get full statistics for this column.
    pub fn stats(&self) -> ColumnStats {
        ColumnStats {
            estimated_cardinality: self.estimate(),
            row_count: self.row_count,
            data_type: self.data_type,
            min_value: self.min_value,
            max_value: self.max_value,
        }
    }

    /// Merge another estimator into this one.
    ///
    /// Useful for combining estimates from multiple files into a table-level estimate.
    pub fn merge(&mut self, other: &ColumnCardinalityEstimator) {
        self.row_count += other.row_count;
        self.estimator.merge(&other.estimator);

        // Merge min/max bounds
        if let Some(other_min) = other.min_value {
            self.min_value = Some(match self.min_value {
                Some(min) => min.min(other_min),
                None => other_min,
            });
        }
        if let Some(other_max) = other.max_value {
            self.max_value = Some(match self.max_value {
                Some(max) => max.max(other_max),
                None => other_max,
            });
        }
    }
}

impl std::fmt::Debug for ColumnCardinalityEstimator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColumnCardinalityEstimator")
            .field("column_name", &self.column_name)
            .field("data_type", &self.data_type)
            .field("row_count", &self.row_count)
            .field("estimated_cardinality", &self.estimate())
            .field("selectivity", &format!("{:.2}%", self.selectivity() * 100.0))
            .field("recommended_strategy", &self.recommended_strategy())
            .finish()
    }
}

/// Cardinality estimator for all columns in a table.
///
/// Tracks cardinality for each column during sync, enabling automatic
/// index strategy selection.
pub struct TableCardinalityEstimator {
    /// Per-column estimators
    columns: HashMap<String, ColumnCardinalityEstimator>,
}

impl TableCardinalityEstimator {
    /// Create a new table cardinality estimator.
    pub fn new() -> Self {
        Self {
            columns: HashMap::new(),
        }
    }

    /// Get or create an estimator for a column.
    pub fn get_or_create(&mut self, column_name: &str, data_type: ColumnType) -> &mut ColumnCardinalityEstimator {
        self.columns
            .entry(column_name.to_string())
            .or_insert_with(|| ColumnCardinalityEstimator::new(column_name, data_type))
    }

    /// Add a string value for a column.
    pub fn add_string(&mut self, column_name: &str, data_type: ColumnType, value: &str) {
        self.get_or_create(column_name, data_type).add_string(value);
    }

    /// Add an integer value for a column.
    pub fn add_i64(&mut self, column_name: &str, data_type: ColumnType, value: i64) {
        self.get_or_create(column_name, data_type).add_i64(value);
    }

    /// Add a float value for a column.
    pub fn add_f64(&mut self, column_name: &str, data_type: ColumnType, value: f64) {
        self.get_or_create(column_name, data_type).add_f64(value);
    }

    /// Get the estimator for a column.
    pub fn get(&self, column_name: &str) -> Option<&ColumnCardinalityEstimator> {
        self.columns.get(column_name)
    }

    /// Get all column names.
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.keys().map(|s| s.as_str()).collect()
    }

    /// Get recommended strategies for all columns.
    pub fn recommended_strategies(&self) -> HashMap<String, IndexStrategy> {
        self.columns
            .iter()
            .map(|(name, est)| (name.clone(), est.recommended_strategy()))
            .collect()
    }

    /// Get statistics for all columns.
    pub fn all_stats(&self) -> HashMap<String, ColumnStats> {
        self.columns
            .iter()
            .map(|(name, est)| (name.clone(), est.stats()))
            .collect()
    }

    /// Merge another table estimator into this one.
    pub fn merge(&mut self, other: &TableCardinalityEstimator) {
        for (name, other_est) in &other.columns {
            if let Some(est) = self.columns.get_mut(name) {
                est.merge(other_est);
            } else {
                // Clone the estimator - this is a bit awkward but necessary
                let mut new_est = ColumnCardinalityEstimator::new(name, other_est.data_type);
                new_est.merge(other_est);
                self.columns.insert(name.clone(), new_est);
            }
        }
    }

    /// Log a summary of all columns and their recommended strategies.
    pub fn log_summary(&self, table_name: &str) {
        tracing::info!(
            table = table_name,
            column_count = self.columns.len(),
            "Cardinality estimation complete"
        );

        for (name, est) in &self.columns {
            let strategy = est.recommended_strategy();
            tracing::debug!(
                table = table_name,
                column = name,
                cardinality = est.estimate(),
                row_count = est.row_count(),
                selectivity = format!("{:.2}%", est.selectivity() * 100.0),
                strategy = ?strategy,
                "Column cardinality estimate"
            );
        }
    }
}

impl Default for TableCardinalityEstimator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_cardinality_low() {
        let mut est = ColumnCardinalityEstimator::new("status", ColumnType::String);

        // Add values with low cardinality (only 3 distinct values)
        for _ in 0..10000 {
            est.add_string("active");
            est.add_string("pending");
            est.add_string("inactive");
        }

        // Should estimate ~3 distinct values (with some error)
        let cardinality = est.estimate();
        assert!(cardinality >= 2 && cardinality <= 5, "Expected ~3, got {}", cardinality);

        // Selectivity should be very low
        let selectivity = est.selectivity();
        assert!(selectivity < 0.001, "Expected low selectivity, got {}", selectivity);

        // Should recommend FST
        assert_eq!(est.recommended_strategy(), IndexStrategy::Fst);
    }

    #[test]
    fn test_column_cardinality_high() {
        let mut est = ColumnCardinalityEstimator::new("user_id", ColumnType::String);

        // Add 200K unique values, but each value appears 5 times
        // Total rows: 1M, cardinality: 200K, selectivity: 20%
        for _ in 0..5 {
            for i in 0..200_000 {
                est.add_string(&format!("user_{}", i));
            }
        }

        // Should estimate ~200K distinct values (with HLL error margin)
        let cardinality = est.estimate();
        let error = (cardinality as f64 - 200_000.0).abs() / 200_000.0;
        assert!(error < 0.10, "Expected ~200K, got {} (error: {:.2}%)", cardinality, error * 100.0);

        // Row count should be 1M
        assert_eq!(est.row_count(), 1_000_000);

        // Selectivity = 200K / 1M = 20%, which is < 50% threshold
        // High cardinality (>100K) + values repeat → Xor Filter
        assert_eq!(est.recommended_strategy(), IndexStrategy::XorFilter);
    }

    #[test]
    fn test_column_cardinality_numeric() {
        let mut est = ColumnCardinalityEstimator::new("amount", ColumnType::Int64);

        for i in 0..1000 {
            est.add_i64(i * 100);
        }

        // Should track min/max
        assert_eq!(est.min_value, Some(0.0));
        assert_eq!(est.max_value, Some(99900.0));

        // Numeric columns always use NumericStats
        assert_eq!(est.recommended_strategy(), IndexStrategy::NumericStats);
    }

    #[test]
    fn test_table_cardinality_estimator() {
        let mut table_est = TableCardinalityEstimator::new();

        // Simulate adding rows
        for i in 0..1000 {
            table_est.add_string("status", ColumnType::String, if i % 2 == 0 { "active" } else { "inactive" });
            table_est.add_string("user_id", ColumnType::String, &format!("user_{}", i));
            table_est.add_i64("amount", ColumnType::Int64, i * 10);
        }

        let strategies = table_est.recommended_strategies();

        // Status should use FST (low cardinality)
        assert_eq!(strategies.get("status"), Some(&IndexStrategy::Fst));

        // Amount should use NumericStats
        assert_eq!(strategies.get("amount"), Some(&IndexStrategy::NumericStats));
    }

    #[test]
    fn test_merge_estimators() {
        let mut est1 = ColumnCardinalityEstimator::new("id", ColumnType::String);
        for i in 0..1000 {
            est1.add_string(&format!("id_{}", i));
        }

        let mut est2 = ColumnCardinalityEstimator::new("id", ColumnType::String);
        for i in 1000..2000 {
            est2.add_string(&format!("id_{}", i));
        }

        // Merge
        est1.merge(&est2);

        // Should have ~2000 distinct values
        let cardinality = est1.estimate();
        assert!(cardinality >= 1800 && cardinality <= 2200, "Expected ~2000, got {}", cardinality);
        assert_eq!(est1.row_count(), 2000);
    }
}
