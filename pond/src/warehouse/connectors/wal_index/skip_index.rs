//! Block-Level Skip Indexes
//!
//! Provides coarse-grained filtering to eliminate entire blocks that
//! definitely don't contain matching values.

use std::collections::HashMap;

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use serde::{Deserialize, Serialize};
use xorf::{BinaryFuse8, Filter};

use super::types::ColumnValue;
use crate::warehouse::connectors::{ConnectorError, ConnectorResult};

/// Type of skip index for a column in a block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SkipIndexType {
    /// Min/Max statistics for numeric columns
    MinMax,
    /// Xor filter for high-cardinality string columns
    XorFilter,
    /// Bloom filter (alternative to Xor)
    Bloom,
}

impl SkipIndexType {
    /// Get the enum value for ClickHouse storage.
    pub fn to_clickhouse_enum(&self) -> u8 {
        match self {
            Self::XorFilter => 1,
            Self::MinMax => 2,
            Self::Bloom => 3,
        }
    }

    /// Parse from ClickHouse enum value.
    pub fn from_clickhouse_enum(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::XorFilter),
            2 => Some(Self::MinMax),
            3 => Some(Self::Bloom),
            _ => None,
        }
    }
}

/// Min/Max statistics for a numeric column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MinMaxStats {
    pub min: f64,
    pub max: f64,
    pub null_count: u64,
    pub value_count: u64,
}

impl MinMaxStats {
    /// Create new stats from initial value.
    pub fn new(value: f64) -> Self {
        if value.is_nan() {
            let mut s = Self::empty();
            s.value_count = 1;
            return s;
        }
        Self {
            min: value,
            max: value,
            null_count: 0,
            value_count: 1,
        }
    }

    /// Create empty stats (for null-only columns).
    pub fn empty() -> Self {
        Self {
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            null_count: 0,
            value_count: 0,
        }
    }

    /// Update stats with a new value.
    pub fn update(&mut self, value: Option<f64>) {
        match value {
            Some(v) if !v.is_nan() => {
                if v < self.min {
                    self.min = v;
                }
                if v > self.max {
                    self.max = v;
                }
                self.value_count += 1;
            }
            Some(_) => {
                self.value_count += 1;
            }
            None => {
                self.null_count += 1;
            }
        }
    }

    /// Check if a value might be in this range.
    pub fn might_contain(&self, value: f64) -> bool {
        if value.is_nan() || self.min.is_nan() || self.max.is_nan() {
            return true;
        }
        value >= self.min && value <= self.max
    }

    /// Check if values > threshold might exist.
    pub fn might_contain_gt(&self, threshold: f64) -> bool {
        self.max > threshold
    }

    /// Check if values >= threshold might exist.
    pub fn might_contain_gte(&self, threshold: f64) -> bool {
        self.max >= threshold
    }

    /// Check if values < threshold might exist.
    pub fn might_contain_lt(&self, threshold: f64) -> bool {
        self.min < threshold
    }

    /// Check if values <= threshold might exist.
    pub fn might_contain_lte(&self, threshold: f64) -> bool {
        self.min <= threshold
    }

    /// Check if a range overlaps with this block's range.
    pub fn might_contain_range(&self, min: Option<f64>, max: Option<f64>) -> bool {
        let query_min = min.unwrap_or(f64::NEG_INFINITY);
        let query_max = max.unwrap_or(f64::INFINITY);
        query_min <= self.max && query_max >= self.min
    }

    /// Serialize to bytes for storage.
    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap_or_default()
    }

    /// Deserialize from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        bincode::deserialize(bytes).ok()
    }

    /// Serialize to base64 string.
    pub fn to_base64(&self) -> String {
        BASE64.encode(self.to_bytes())
    }

    /// Deserialize from base64 string.
    pub fn from_base64(s: &str) -> Option<Self> {
        BASE64.decode(s).ok().and_then(|b| Self::from_bytes(&b))
    }
}

/// Xor filter wrapper for string values.
#[derive(Debug)]
pub struct XorFilterIndex {
    filter: BinaryFuse8,
    value_count: usize,
}

impl XorFilterIndex {
    /// Build a filter from value hashes.
    pub fn build(hashes: &[u64]) -> ConnectorResult<Self> {
        if hashes.is_empty() {
            return Err(ConnectorError::Validation(
                "Cannot build Xor filter from empty data".to_string(),
            ));
        }

        let mut deduped = hashes.to_vec();
        deduped.sort_unstable();
        deduped.dedup();

        let filter = BinaryFuse8::try_from(&deduped[..]).map_err(|_| {
            ConnectorError::Internal("Failed to build Xor filter".to_string())
        })?;

        Ok(Self {
            filter,
            value_count: deduped.len(),
        })
    }

    /// Check if a value hash might be in this filter.
    pub fn might_contain(&self, hash: u64) -> bool {
        self.filter.contains(&hash)
    }

    /// Get the number of values in the filter.
    pub fn value_count(&self) -> usize {
        self.value_count
    }

    /// Serialize to bytes for storage.
    pub fn to_bytes(&self) -> Vec<u8> {
        // Serialize the filter's internal state
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(self.value_count as u64).to_le_bytes());
        
        // BinaryFuse8 serialization
        let filter_bytes = bincode::serialize(&self.filter).unwrap_or_default();
        bytes.extend_from_slice(&(filter_bytes.len() as u64).to_le_bytes());
        bytes.extend(filter_bytes);
        
        bytes
    }

    /// Deserialize from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 16 {
            return None;
        }

        let value_count = u64::from_le_bytes(bytes[0..8].try_into().ok()?) as usize;
        let filter_len = u64::from_le_bytes(bytes[8..16].try_into().ok()?) as usize;
        
        if bytes.len() < 16 + filter_len {
            return None;
        }

        let filter: BinaryFuse8 = bincode::deserialize(&bytes[16..16 + filter_len]).ok()?;

        Some(Self {
            filter,
            value_count,
        })
    }

    /// Serialize to base64 string.
    pub fn to_base64(&self) -> String {
        BASE64.encode(self.to_bytes())
    }

    /// Deserialize from base64 string.
    pub fn from_base64(s: &str) -> Option<Self> {
        BASE64.decode(s).ok().and_then(|b| Self::from_bytes(&b))
    }
}

/// A skip index for a single column in a single block.
#[derive(Debug)]
pub enum BlockSkipIndex {
    /// Min/Max for numeric columns
    MinMax(MinMaxStats),
    /// Xor filter for string columns
    XorFilter(XorFilterIndex),
}

impl BlockSkipIndex {
    /// Get the index type.
    pub fn index_type(&self) -> SkipIndexType {
        match self {
            Self::MinMax(_) => SkipIndexType::MinMax,
            Self::XorFilter(_) => SkipIndexType::XorFilter,
        }
    }

    /// Check if the block might contain an equal value.
    pub fn might_contain_eq(&self, value: &ColumnValue) -> bool {
        match (self, value) {
            (Self::MinMax(stats), v) => {
                if let Some(f) = v.as_f64() {
                    stats.might_contain(f)
                } else {
                    true // Can't determine, assume might contain
                }
            }
            (Self::XorFilter(filter), _) => {
                filter.might_contain(value.stable_hash())
            }
        }
    }

    /// Check if the block might contain values > threshold.
    pub fn might_contain_gt(&self, value: &ColumnValue) -> bool {
        match (self, value) {
            (Self::MinMax(stats), v) => {
                if let Some(f) = v.as_f64() {
                    stats.might_contain_gt(f)
                } else {
                    true
                }
            }
            _ => true, // Xor filter can't answer range queries
        }
    }

    /// Check if the block might contain values >= threshold.
    pub fn might_contain_gte(&self, value: &ColumnValue) -> bool {
        match (self, value) {
            (Self::MinMax(stats), v) => {
                if let Some(f) = v.as_f64() {
                    stats.might_contain_gte(f)
                } else {
                    true
                }
            }
            _ => true,
        }
    }

    /// Check if the block might contain values < threshold.
    pub fn might_contain_lt(&self, value: &ColumnValue) -> bool {
        match (self, value) {
            (Self::MinMax(stats), v) => {
                if let Some(f) = v.as_f64() {
                    stats.might_contain_lt(f)
                } else {
                    true
                }
            }
            _ => true,
        }
    }

    /// Check if the block might contain values <= threshold.
    pub fn might_contain_lte(&self, value: &ColumnValue) -> bool {
        match (self, value) {
            (Self::MinMax(stats), v) => {
                if let Some(f) = v.as_f64() {
                    stats.might_contain_lte(f)
                } else {
                    true
                }
            }
            _ => true,
        }
    }

    /// Serialize to base64 string.
    pub fn to_base64(&self) -> String {
        match self {
            Self::MinMax(stats) => stats.to_base64(),
            Self::XorFilter(filter) => filter.to_base64(),
        }
    }

    /// Deserialize from base64 string.
    pub fn from_base64(s: &str, index_type: SkipIndexType) -> Option<Self> {
        match index_type {
            SkipIndexType::MinMax => MinMaxStats::from_base64(s).map(Self::MinMax),
            SkipIndexType::XorFilter | SkipIndexType::Bloom => {
                XorFilterIndex::from_base64(s).map(Self::XorFilter)
            }
        }
    }

    /// Get estimated cardinality.
    pub fn cardinality_estimate(&self) -> u64 {
        match self {
            Self::MinMax(stats) => stats.value_count,
            Self::XorFilter(filter) => filter.value_count() as u64,
        }
    }
}

/// Builder for creating skip indexes from streaming values.
pub struct SkipIndexBuilder {
    /// For numeric columns: accumulate min/max
    min_max: Option<MinMaxStats>,
    /// For string columns: accumulate hashes
    value_hashes: Vec<u64>,
    /// Whether this is a numeric column
    is_numeric: bool,
}

impl SkipIndexBuilder {
    /// Create a builder for a numeric column.
    pub fn numeric() -> Self {
        Self {
            min_max: None,
            value_hashes: Vec::new(),
            is_numeric: true,
        }
    }

    /// Create a builder for a string column.
    pub fn string() -> Self {
        Self {
            min_max: None,
            value_hashes: Vec::new(),
            is_numeric: false,
        }
    }

    /// Add a value to the builder.
    pub fn add_value(&mut self, value: &ColumnValue) {
        if self.is_numeric {
            let f = value.as_f64();
            match &mut self.min_max {
                Some(stats) => stats.update(f),
                None => {
                    if let Some(v) = f {
                        self.min_max = Some(MinMaxStats::new(v));
                    } else {
                        let mut stats = MinMaxStats::empty();
                        stats.null_count = 1;
                        self.min_max = Some(stats);
                    }
                }
            }
        } else {
            self.value_hashes.push(value.stable_hash());
        }
    }

    /// Build the skip index.
    pub fn build(self) -> ConnectorResult<BlockSkipIndex> {
        if self.is_numeric {
            Ok(BlockSkipIndex::MinMax(
                self.min_max.unwrap_or_else(MinMaxStats::empty),
            ))
        } else {
            if self.value_hashes.is_empty() {
                return Err(ConnectorError::Validation(
                    "No values to build Xor filter".to_string(),
                ));
            }
            let filter = XorFilterIndex::build(&self.value_hashes)?;
            Ok(BlockSkipIndex::XorFilter(filter))
        }
    }
}

/// Collection of skip indexes for all columns in a block.
#[derive(Debug, Default)]
pub struct BlockSkipIndexes {
    /// Column name -> skip index
    indexes: HashMap<String, BlockSkipIndex>,
}

impl BlockSkipIndexes {
    /// Create empty indexes.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an index for a column.
    pub fn insert(&mut self, column: impl Into<String>, index: BlockSkipIndex) {
        self.indexes.insert(column.into(), index);
    }

    /// Get an index for a column.
    pub fn get(&self, column: &str) -> Option<&BlockSkipIndex> {
        self.indexes.get(column)
    }

    /// Iterate over all indexes.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &BlockSkipIndex)> {
        self.indexes.iter()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }

    /// Get number of indexed columns.
    pub fn len(&self) -> usize {
        self.indexes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_min_max_stats() {
        let mut stats = MinMaxStats::new(10.0);
        stats.update(Some(5.0));
        stats.update(Some(15.0));
        stats.update(None);

        assert_eq!(stats.min, 5.0);
        assert_eq!(stats.max, 15.0);
        assert_eq!(stats.value_count, 3);
        assert_eq!(stats.null_count, 1);

        assert!(stats.might_contain(10.0));
        assert!(!stats.might_contain(4.0));
        assert!(!stats.might_contain(16.0));

        assert!(stats.might_contain_gt(5.0));
        assert!(!stats.might_contain_gt(15.0));
    }

    #[test]
    fn test_min_max_serialization() {
        let stats = MinMaxStats::new(42.0);
        let base64 = stats.to_base64();
        let restored = MinMaxStats::from_base64(&base64).unwrap();

        assert_eq!(stats.min, restored.min);
        assert_eq!(stats.max, restored.max);
    }

    #[test]
    fn test_xor_filter() {
        let hashes: Vec<u64> = vec![1, 2, 3, 4, 5];
        let filter = XorFilterIndex::build(&hashes).unwrap();

        assert!(filter.might_contain(1));
        assert!(filter.might_contain(3));
        assert!(filter.might_contain(5));
        // False positives are possible but rare
    }

    #[test]
    fn test_skip_index_builder_numeric() {
        let mut builder = SkipIndexBuilder::numeric();
        builder.add_value(&ColumnValue::Int64(10));
        builder.add_value(&ColumnValue::Int64(20));
        builder.add_value(&ColumnValue::Float64(15.0));

        let index = builder.build().unwrap();
        assert!(matches!(index, BlockSkipIndex::MinMax(_)));
        assert!(index.might_contain_eq(&ColumnValue::Int64(15)));
    }

    #[test]
    fn test_skip_index_builder_string() {
        let mut builder = SkipIndexBuilder::string();
        builder.add_value(&ColumnValue::String("pending".to_string()));
        builder.add_value(&ColumnValue::String("complete".to_string()));

        let index = builder.build().unwrap();
        assert!(matches!(index, BlockSkipIndex::XorFilter(_)));
        assert!(index.might_contain_eq(&ColumnValue::String("pending".to_string())));
    }

    #[test]
    fn test_xor_filter_build_with_duplicates() {
        let hashes: Vec<u64> = vec![1, 2, 3, 2, 1, 3, 3];
        let filter = XorFilterIndex::build(&hashes).unwrap();
        assert_eq!(filter.value_count, 3, "value_count must reflect unique hashes");
        assert!(filter.might_contain(1));
        assert!(filter.might_contain(2));
        assert!(filter.might_contain(3));
    }

    #[test]
    fn test_min_max_nan_first_value_not_corrupted() {
        let mut stats = MinMaxStats::new(f64::NAN);
        stats.update(Some(5.0));
        stats.update(Some(15.0));

        assert_eq!(stats.min, 5.0, "NaN as first value must not corrupt min");
        assert_eq!(stats.max, 15.0, "NaN as first value must not corrupt max");
        assert_eq!(stats.value_count, 3);
        assert!(stats.might_contain(10.0));
    }

    #[test]
    fn test_min_max_nan_update_not_corrupted() {
        let mut stats = MinMaxStats::new(10.0);
        stats.update(Some(f64::NAN));
        stats.update(Some(5.0));

        assert_eq!(stats.min, 5.0, "NaN in update must not corrupt min");
        assert_eq!(stats.max, 10.0, "NaN in update must not corrupt max");
        assert_eq!(stats.value_count, 3);
    }

    #[test]
    fn test_min_max_might_contain_nan_conservative() {
        let stats = MinMaxStats::new(10.0);
        assert!(
            stats.might_contain(f64::NAN),
            "might_contain(NaN) must return true conservatively"
        );
    }
}
