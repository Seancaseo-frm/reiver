//! Index Strategy Selection
//!
//! This module provides automatic selection of optimal index types based on
//! column data characteristics. Different data structures have different
//! trade-offs, and this module encapsulates the decision logic.
//!
//! # Index Data Structure Comparison
//!
//! | Structure      | Incremental Add | Incremental Delete | Prefix Queries | Range Queries | Probabilistic | Memory Efficiency |
//! |----------------|-----------------|--------------------| ---------------|---------------|---------------|-------------------|
//! | FST            | No (rebuild)    | No (rebuild)       | Yes            | Yes           | No            | Excellent         |
//! | Roaring Bitmap | Yes             | Yes                | No             | Yes (integers)| No            | Good (sparse)     |
//! | Xor Filter     | No (rebuild)    | No (rebuild)       | No             | No            | Yes (~1% FP)  | Excellent         |
//! | HyperLogLog    | Yes             | No                 | No             | No            | Yes           | Excellent (~1.6KB)|
//! | qp-trie        | Yes             | Yes                | Yes            | No            | No            | Good              |
//! | Bloom Filter   | Yes             | No                 | No             | No            | Yes           | Good              |
//!
//! # Why Cardinality Matters for Summary Indexes (Not Per-File)
//!
//! FST handles high cardinality well at the per-file level. The issue arises
//! specifically when **merging multiple FSTs into partition/global summaries**
//! via union operations.
//!
//! The union operation requires:
//! 1. Streaming through all keys from all source FSTs
//! 2. Building a new FST with combined keys  
//! 3. Holding intermediate data in memory
//!
//! For high-cardinality columns (UUIDs, user IDs), partition summaries can
//! explode to millions of entries. This is why `MAX_SUMMARY_CARDINALITY`
//! exists - to prevent OOM during summary builds.
//!
//! **Solution**: Keep FST for per-file indexes (any cardinality). Use Xor Filters
//! for partition/global summaries of high-cardinality columns where FST summaries
//! would be too large.
//!
//! # Recommended Strategy by Scope
//!
//! | Scope       | Data Type              | Index Type   | Mutable? | Notes                          |
//! |-------------|------------------------|--------------|----------|--------------------------------|
//! | Per-file    | Any string             | FST Set      | No       | Immutable is fine (write-once) |
//! | Per-file    | Numeric                | Min/Max      | No       | NumericColumnStats             |
//! | Summary     | Low cardinality (<100K)| FST (union)  | No       | Current approach works well    |
//! | Summary     | High cardinality       | Xor Filter   | No       | Fixed size, probabilistic      |
//! | Runtime     | File set operations    | RoaringBitmap| Yes      | Fast intersection/union        |
//! | Runtime     | Cardinality estimation | HyperLogLog  | Yes      | Decide strategy during sync    |

use crate::warehouse::types::{CardinalityHint, ColumnType, IndexColumnConfig};

/// Maximum cardinality for FST-based summary indexes.
/// Columns exceeding this are indexed with Xor Filters instead.
pub const MAX_FST_SUMMARY_CARDINALITY: usize = 100_000;

/// Maximum selectivity (distinct_values / row_count) for Xor Filter to be worthwhile.
/// Above this threshold (nearly unique values), we skip indexing as filtering provides little benefit.
/// Below this threshold, values repeat enough that filtering is effective.
pub const MAX_XOR_FILTER_SELECTIVITY: f64 = 0.5;

/// Index strategy for a column based on its characteristics.
///
/// This enum represents the decision made about how to index a column,
/// taking into account data type, cardinality, and query patterns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexStrategy {
    /// FST (Finite State Transducer) for ordered string data.
    ///
    /// **Best for:** Low-to-medium cardinality strings with prefix/range queries.
    /// **Trade-offs:** Immutable, requires sorted input, excellent compression.
    Fst,

    /// Xor Filter for probabilistic membership testing.
    ///
    /// **Best for:** High-cardinality strings where exact FST would be too large.
    /// **Trade-offs:** ~1% false positive rate, no prefix queries, smaller than Bloom.
    XorFilter,

    /// Min/Max statistics for numeric range filtering.
    ///
    /// **Best for:** Numeric columns (integers, floats, timestamps).
    /// **Trade-offs:** Only supports range and equality pruning, not prefix/substring.
    NumericStats,

    /// Roaring Bitmap for tracking row/file membership.
    ///
    /// **Best for:** Integer sets, file ID tracking, combining predicates.
    /// **Trade-offs:** Mutable, fast set operations, good for sparse sets.
    RoaringBitmap,

    /// Skip indexing entirely.
    ///
    /// **When used:** Cardinality too high even for Xor Filter, or column
    /// is unlikely to be queried.
    Skip,
}

impl IndexStrategy {
    /// Select the optimal index strategy based on column statistics.
    ///
    /// # Arguments
    /// * `data_type` - The column's data type
    /// * `cardinality` - Number of distinct values observed
    /// * `row_count` - Total number of rows
    ///
    /// # Returns
    /// The recommended `IndexStrategy` for this column.
    ///
    /// # Algorithm
    ///
    /// 1. Numeric types → NumericStats (min/max range filtering)
    /// 2. String types with low cardinality → FST (exact + prefix queries)
    /// 3. String types with high cardinality but reasonable selectivity → XorFilter
    /// 4. Everything else → Skip
    pub fn from_stats(data_type: ColumnType, cardinality: usize, row_count: usize) -> Self {
        // Numeric columns always use min/max stats
        match data_type {
            ColumnType::Int32
            | ColumnType::Int64
            | ColumnType::Float32
            | ColumnType::Float64
            | ColumnType::Decimal
            | ColumnType::Timestamp
            | ColumnType::Date => return Self::NumericStats,
            _ => {}
        }

        // Boolean has only 2 values - FST is fine
        if data_type == ColumnType::Boolean {
            return Self::Fst;
        }

        // For string-like types, decide based on cardinality
        let selectivity = if row_count > 0 {
            cardinality as f64 / row_count as f64
        } else {
            1.0
        };

        if cardinality <= MAX_FST_SUMMARY_CARDINALITY {
            // Low cardinality: FST works well for summaries
            Self::Fst
        } else if selectivity <= MAX_XOR_FILTER_SELECTIVITY {
            // High cardinality but values repeat enough: use Xor Filter
            // Selectivity <= 0.5 means at least 2x repetition on average
            Self::XorFilter
        } else {
            // Very high cardinality (nearly unique values): skip indexing
            // When selectivity > 0.5, most values are unique and filtering
            // provides little benefit with ~1% false positive rate
            Self::Skip
        }
    }

    /// Returns true if this strategy produces an immutable index.
    ///
    /// Immutable indexes must be rebuilt entirely when data changes,
    /// but this is fine for Parquet files which are write-once.
    pub fn is_immutable(&self) -> bool {
        matches!(self, Self::Fst | Self::XorFilter | Self::NumericStats)
    }

    /// Returns true if this strategy supports prefix queries.
    pub fn supports_prefix_queries(&self) -> bool {
        matches!(self, Self::Fst)
    }

    /// Returns true if this strategy supports range queries.
    pub fn supports_range_queries(&self) -> bool {
        matches!(self, Self::Fst | Self::NumericStats)
    }

    /// Returns true if this is a probabilistic data structure (may have false positives).
    pub fn is_probabilistic(&self) -> bool {
        matches!(self, Self::XorFilter)
    }

    /// Get a human-readable description of why this strategy was chosen.
    pub fn description(&self) -> &'static str {
        match self {
            Self::Fst => "FST: Low cardinality string column, supports prefix/range queries",
            Self::XorFilter => "Xor Filter: High cardinality column, probabilistic membership test",
            Self::NumericStats => "Numeric Stats: Min/max range filtering for numeric column",
            Self::RoaringBitmap => "Roaring Bitmap: Integer sets with fast set operations",
            Self::Skip => "Skip: Cardinality too high for efficient indexing",
        }
    }

    /// Select index strategy from user-provided configuration.
    ///
    /// This method respects user overrides and hints:
    /// 1. If `force_strategy` is set, use that directly
    /// 2. If `cardinality` hint is set, use that to guide selection
    /// 3. Otherwise, fall back to automatic selection
    ///
    /// # Arguments
    /// * `config` - User-provided column configuration
    /// * `data_type` - The column's data type
    /// * `observed_cardinality` - Cardinality observed from sampling (if available)
    /// * `observed_row_count` - Row count observed from sampling (if available)
    pub fn from_config(
        config: &IndexColumnConfig,
        data_type: ColumnType,
        observed_cardinality: Option<usize>,
        observed_row_count: Option<usize>,
    ) -> Self {
        // Priority 1: User forced a specific strategy
        if let Some(hint) = &config.force_strategy {
            if let Some(strategy) = hint.to_strategy() {
                return strategy;
            }
        }

        // Priority 2: User provided a cardinality hint
        if let Some(hint) = &config.cardinality {
            return hint.recommended_strategy(data_type);
        }

        // Priority 3: Use observed statistics if available
        if let (Some(cardinality), Some(row_count)) = (observed_cardinality, observed_row_count) {
            return Self::from_stats(data_type, cardinality, row_count);
        }

        // Priority 4: Fall back to type-based default
        Self::default_for_type(data_type)
    }

    /// Get the default strategy for a data type when no cardinality info is available.
    pub fn default_for_type(data_type: ColumnType) -> Self {
        match data_type {
            ColumnType::Int32
            | ColumnType::Int64
            | ColumnType::Float32
            | ColumnType::Float64
            | ColumnType::Decimal
            | ColumnType::Timestamp
            | ColumnType::Date => Self::NumericStats,
            ColumnType::Boolean => Self::Fst,
            ColumnType::String | ColumnType::Json | ColumnType::Uuid => Self::Fst,
        }
    }

    /// Select strategy with a cardinality hint override.
    ///
    /// This is a simpler alternative to `from_config` when you just want
    /// to apply a hint.
    pub fn from_hint(hint: CardinalityHint, data_type: ColumnType) -> Self {
        hint.recommended_strategy(data_type)
    }
}

/// Statistics collected during data ingestion for index strategy selection.
#[derive(Debug, Clone)]
pub struct ColumnStats {
    /// Estimated cardinality (from HyperLogLog)
    pub estimated_cardinality: usize,
    /// Total rows observed
    pub row_count: usize,
    /// Column data type
    pub data_type: ColumnType,
    /// For numeric columns: observed minimum value
    pub min_value: Option<f64>,
    /// For numeric columns: observed maximum value
    pub max_value: Option<f64>,
}

impl ColumnStats {
    /// Create new column stats.
    pub fn new(data_type: ColumnType) -> Self {
        Self {
            estimated_cardinality: 0,
            row_count: 0,
            data_type,
            min_value: None,
            max_value: None,
        }
    }

    /// Get the recommended index strategy for this column.
    pub fn recommended_strategy(&self) -> IndexStrategy {
        IndexStrategy::from_stats(self.data_type, self.estimated_cardinality, self.row_count)
    }

    /// Calculate selectivity (cardinality / row_count).
    pub fn selectivity(&self) -> f64 {
        if self.row_count > 0 {
            self.estimated_cardinality as f64 / self.row_count as f64
        } else {
            1.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::warehouse::types::IndexStrategyHint;

    #[test]
    fn test_numeric_columns_use_stats() {
        assert_eq!(
            IndexStrategy::from_stats(ColumnType::Int64, 1000, 10000),
            IndexStrategy::NumericStats
        );
        assert_eq!(
            IndexStrategy::from_stats(ColumnType::Float64, 1000, 10000),
            IndexStrategy::NumericStats
        );
        assert_eq!(
            IndexStrategy::from_stats(ColumnType::Timestamp, 1000, 10000),
            IndexStrategy::NumericStats
        );
    }

    #[test]
    fn test_low_cardinality_strings_use_fst() {
        // 1000 distinct values out of 10000 rows = 10% selectivity, low cardinality
        assert_eq!(
            IndexStrategy::from_stats(ColumnType::String, 1000, 10000),
            IndexStrategy::Fst
        );

        // Even at 100K, still use FST
        assert_eq!(
            IndexStrategy::from_stats(ColumnType::String, 100_000, 1_000_000),
            IndexStrategy::Fst
        );
    }

    #[test]
    fn test_high_cardinality_strings_use_xor_filter() {
        // 200K distinct values out of 1M rows = 20% selectivity
        // High cardinality (>100K) but values repeat enough (selectivity < 50%)
        assert_eq!(
            IndexStrategy::from_stats(ColumnType::String, 200_000, 1_000_000),
            IndexStrategy::XorFilter
        );
    }

    #[test]
    fn test_very_high_cardinality_skips_indexing() {
        // 600K distinct values out of 1M rows = 60% selectivity (nearly unique)
        // Selectivity > 50% threshold, so skip
        assert_eq!(
            IndexStrategy::from_stats(ColumnType::String, 600_000, 1_000_000),
            IndexStrategy::Skip
        );
    }

    #[test]
    fn test_uuid_columns_high_cardinality() {
        // UUIDs are typically unique per row (100% selectivity)
        assert_eq!(
            IndexStrategy::from_stats(ColumnType::Uuid, 1_000_000, 1_000_000),
            IndexStrategy::Skip
        );
    }

    #[test]
    fn test_boolean_uses_fst() {
        // Boolean has at most 2 distinct values
        assert_eq!(
            IndexStrategy::from_stats(ColumnType::Boolean, 2, 10000),
            IndexStrategy::Fst
        );
    }

    #[test]
    fn test_strategy_properties() {
        assert!(IndexStrategy::Fst.is_immutable());
        assert!(IndexStrategy::XorFilter.is_immutable());
        assert!(!IndexStrategy::RoaringBitmap.is_immutable());

        assert!(IndexStrategy::Fst.supports_prefix_queries());
        assert!(!IndexStrategy::XorFilter.supports_prefix_queries());

        assert!(!IndexStrategy::Fst.is_probabilistic());
        assert!(IndexStrategy::XorFilter.is_probabilistic());
    }

    #[test]
    fn test_from_config_forced_strategy() {
        let config = IndexColumnConfig {
            name: "user_id".to_string(),
            cardinality: None,
            force_strategy: Some(IndexStrategyHint::XorFilter),
            fulltext_indexed: false,
        };

        let strategy = IndexStrategy::from_config(&config, ColumnType::String, None, None);
        assert_eq!(strategy, IndexStrategy::XorFilter);
    }

    #[test]
    fn test_from_config_cardinality_hint() {
        let config = IndexColumnConfig {
            name: "country".to_string(),
            cardinality: Some(CardinalityHint::VeryLow),
            force_strategy: None,
            fulltext_indexed: false,
        };

        let strategy = IndexStrategy::from_config(&config, ColumnType::String, None, None);
        assert_eq!(strategy, IndexStrategy::Fst);
    }

    #[test]
    fn test_from_config_high_cardinality_hint() {
        let config = IndexColumnConfig {
            name: "session_id".to_string(),
            cardinality: Some(CardinalityHint::High),
            force_strategy: None,
            fulltext_indexed: false,
        };

        let strategy = IndexStrategy::from_config(&config, ColumnType::String, None, None);
        assert_eq!(strategy, IndexStrategy::XorFilter);
    }

    #[test]
    fn test_from_config_very_high_cardinality_skips() {
        let config = IndexColumnConfig {
            name: "uuid".to_string(),
            cardinality: Some(CardinalityHint::VeryHigh),
            force_strategy: None,
            fulltext_indexed: false,
        };

        let strategy = IndexStrategy::from_config(&config, ColumnType::Uuid, None, None);
        assert_eq!(strategy, IndexStrategy::Skip);
    }

    #[test]
    fn test_from_config_with_observed_stats() {
        // No hints, but we have observed stats
        let config = IndexColumnConfig {
            name: "product_id".to_string(),
            cardinality: None,
            force_strategy: None,
            fulltext_indexed: false,
        };

        // High cardinality observed (200K out of 1M)
        let strategy =
            IndexStrategy::from_config(&config, ColumnType::String, Some(200_000), Some(1_000_000));
        assert_eq!(strategy, IndexStrategy::XorFilter);
    }

    #[test]
    fn test_from_config_force_overrides_hint() {
        // Both hint and force set - force should win
        let config = IndexColumnConfig {
            name: "test".to_string(),
            cardinality: Some(CardinalityHint::VeryHigh), // Would Skip
            force_strategy: Some(IndexStrategyHint::Fst), // But forced to FST
            fulltext_indexed: false,
        };

        let strategy = IndexStrategy::from_config(&config, ColumnType::String, None, None);
        assert_eq!(strategy, IndexStrategy::Fst);
    }

    #[test]
    fn test_default_for_type() {
        assert_eq!(
            IndexStrategy::default_for_type(ColumnType::Int64),
            IndexStrategy::NumericStats
        );
        assert_eq!(
            IndexStrategy::default_for_type(ColumnType::Timestamp),
            IndexStrategy::NumericStats
        );
        assert_eq!(
            IndexStrategy::default_for_type(ColumnType::Boolean),
            IndexStrategy::Fst
        );
        assert_eq!(
            IndexStrategy::default_for_type(ColumnType::String),
            IndexStrategy::Fst
        );
    }

    #[test]
    fn test_from_hint() {
        assert_eq!(
            IndexStrategy::from_hint(CardinalityHint::Low, ColumnType::String),
            IndexStrategy::Fst
        );
        assert_eq!(
            IndexStrategy::from_hint(CardinalityHint::High, ColumnType::String),
            IndexStrategy::XorFilter
        );
        // Numeric types still use stats regardless of hint
        assert_eq!(
            IndexStrategy::from_hint(CardinalityHint::Low, ColumnType::Int64),
            IndexStrategy::NumericStats
        );
    }

    #[test]
    fn test_float32_uses_numeric_stats() {
        assert_eq!(
            IndexStrategy::from_stats(ColumnType::Float32, 100, 1000),
            IndexStrategy::NumericStats,
            "Float32 should use NumericStats, consistent with default_for_type"
        );
        assert_eq!(
            IndexStrategy::default_for_type(ColumnType::Float32),
            IndexStrategy::NumericStats,
        );
    }
}
