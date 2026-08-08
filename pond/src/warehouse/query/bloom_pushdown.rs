//! Bloom Filter Pushdown for Semi-Join Optimization
//!
//! Provides Bloom filter-based filtering for medium-sized key sets (10K-1M keys)
//! where IN clauses would be too large but temp table materialization is overkill.
//!
//! # How It Works
//!
//! 1. Extract join keys from probe side
//! 2. Build a Bloom filter from the keys
//! 3. Apply filter client-side (or leverage table-level Bloom indexes when available)
//! 4. Filter false positives with exact matching
//!
//! # Source Support
//!
//! Currently all sources use client-side filtering. For databases with Bloom filter
//! indexes (ClickHouse, PostgreSQL with extensions), the filter can be leveraged
//! automatically if configured at the table level.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use thiserror::Error;

use crate::warehouse::types::SourceType;

/// Minimum size for Bloom filter bit array (in bits).
const MIN_BLOOM_BITS: usize = 64;

/// Maximum number of hash functions for Bloom filter.
const MAX_HASH_FUNCTIONS: u32 = 16;

/// Maximum number of bits in a deserialized Bloom filter (128 MB).
const MAX_BLOOM_BITS: usize = 128 * 1024 * 1024 * 8;

/// Errors during Bloom filter operations.
#[derive(Debug, Error)]
pub enum BloomError {
    #[error("Failed to build Bloom filter: {0}")]
    BuildError(String),

    #[error("Filter too large: {0} bytes exceeds maximum {1}")]
    TooLarge(usize, usize),

    #[error("Source does not support Bloom filter pushdown")]
    NotSupported,
}

/// Result type for Bloom filter operations.
pub type BloomResult<T> = Result<T, BloomError>;

/// A simple Bloom filter implementation.
///
/// Uses multiple hash functions (via different seeds) to minimize false positives.
#[derive(Debug, Clone)]
pub struct BloomFilter {
    /// Bit array stored as bytes (wrapped in Arc for cheap cloning).
    bits: Arc<Vec<u8>>,
    /// Number of hash functions (k).
    num_hashes: u32,
    /// Number of bits (m).
    num_bits: usize,
    /// Number of items inserted.
    num_items: usize,
}

impl BloomFilter {
    /// Create a new Bloom filter sized for the expected number of items
    /// and desired false positive rate.
    ///
    /// # Arguments
    /// * `expected_items` - Expected number of items to insert
    /// * `false_positive_rate` - Desired false positive rate (e.g., 0.01 for 1%)
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        let expected_items = expected_items.max(1);
        let false_positive_rate = false_positive_rate.clamp(1e-15, 1.0 - f64::EPSILON);

        let ln2 = std::f64::consts::LN_2;
        let ln2_sq = ln2 * ln2;

        let num_bits = (-(expected_items as f64) * false_positive_rate.ln() / ln2_sq).ceil() as usize;
        let num_bits = num_bits.clamp(MIN_BLOOM_BITS, MAX_BLOOM_BITS);

        let num_hashes = ((num_bits as f64 / expected_items as f64) * ln2).ceil() as u32;
        let num_hashes = num_hashes.clamp(1, MAX_HASH_FUNCTIONS);

        let num_bytes = (num_bits + 7) / 8;

        Self {
            bits: Arc::new(vec![0u8; num_bytes]),
            num_hashes,
            num_bits,
            num_items: 0,
        }
    }

    /// Insert a value into the filter.
    ///
    /// Callers **must not** insert duplicate values.  The `num_items` counter
    /// is incremented unconditionally, so duplicates would inflate it and skew
    /// the estimated false-positive probability.  Pre-deduplicate keys (e.g.
    /// via `HashSet`) before calling this method.
    pub fn insert<T: Hash>(&mut self, value: &T) {
        // Double-hashing optimization: compute two hash values once, then
        // derive all k hash indices using linear combinations.
        let mut h1_hasher = DefaultHasher::new();
        value.hash(&mut h1_hasher);
        let h1 = h1_hasher.finish();

        let mut h2_hasher = DefaultHasher::new();
        (value, 0x517cc1b727220a95u64).hash(&mut h2_hasher); // different seed
        let h2 = h2_hasher.finish();

        for i in 0..self.num_hashes as u64 {
            let bit_index = (h1.wrapping_add(i.wrapping_mul(h2))) as usize % self.num_bits;
            self.set_bit(bit_index);
        }
        self.num_items += 1;
    }

    /// Check if a value might be in the filter.
    ///
    /// Returns `true` if the value might be in the set (may be false positive).
    /// Returns `false` if the value is definitely not in the set.
    pub fn might_contain<T: Hash>(&self, value: &T) -> bool {
        // Double-hashing optimization: compute two hash values once, then
        // derive all k hash indices using linear combinations.
        let mut h1_hasher = DefaultHasher::new();
        value.hash(&mut h1_hasher);
        let h1 = h1_hasher.finish();

        let mut h2_hasher = DefaultHasher::new();
        (value, 0x517cc1b727220a95u64).hash(&mut h2_hasher); // different seed
        let h2 = h2_hasher.finish();

        for i in 0..self.num_hashes as u64 {
            let bit_index = (h1.wrapping_add(i.wrapping_mul(h2))) as usize % self.num_bits;
            if !self.get_bit(bit_index) {
                return false;
            }
        }
        true
    }

    /// Get the number of items inserted.
    pub fn len(&self) -> usize {
        self.num_items
    }

    /// Check if the filter is empty.
    pub fn is_empty(&self) -> bool {
        self.num_items == 0
    }

    /// Get the size of the filter in bytes.
    pub fn size_bytes(&self) -> usize {
        self.bits.len()
    }

    /// Get the actual false positive rate based on current fill ratio.
    pub fn estimated_fpp(&self) -> f64 {
        if self.num_items == 0 {
            return 0.0;
        }
        // p ≈ (1 - e^(-k*n/m))^k
        let k = self.num_hashes as f64;
        let n = self.num_items as f64;
        let m = self.num_bits as f64;
        (1.0 - (-k * n / m).exp()).powf(k)
    }

    /// Serialize the filter to bytes for transmission.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(24 + self.bits.len());
        result.extend_from_slice(&(self.num_bits as u64).to_le_bytes());
        result.extend_from_slice(&self.num_hashes.to_le_bytes());
        // 4 bytes padding for alignment after the u32 num_hashes
        result.extend_from_slice(&0u32.to_le_bytes());
        result.extend_from_slice(&(self.num_items as u64).to_le_bytes());
        result.extend_from_slice(self.bits.as_slice());
        result
    }

    /// Deserialize a filter from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 24 {
            return None;
        }

        let num_bits = u64::from_le_bytes(bytes[0..8].try_into().ok()?) as usize;
        let num_hashes = u32::from_le_bytes(bytes[8..12].try_into().ok()?);
        // bytes[12..16] is padding
        let num_items = u64::from_le_bytes(bytes[16..24].try_into().ok()?) as usize;

        if num_bits == 0 || num_bits > MAX_BLOOM_BITS || num_hashes == 0 || num_hashes > MAX_HASH_FUNCTIONS {
            return None;
        }

        let expected_len = 24 + (num_bits + 7) / 8;
        if bytes.len() < expected_len {
            return None;
        }

        Some(Self {
            bits: Arc::new(bytes[24..expected_len].to_vec()),
            num_hashes,
            num_bits,
            num_items,
        })
    }

    // Private helper methods

    fn set_bit(&mut self, index: usize) {
        let byte_index = index / 8;
        let bit_offset = index % 8;
        let bits = Arc::make_mut(&mut self.bits);
        if byte_index < bits.len() {
            bits[byte_index] |= 1 << bit_offset;
        }
    }

    fn get_bit(&self, index: usize) -> bool {
        let byte_index = index / 8;
        let bit_offset = index % 8;
        if byte_index < self.bits.len() {
            (self.bits[byte_index] & (1 << bit_offset)) != 0
        } else {
            false
        }
    }
}

/// Strategy for applying Bloom filter to a source.
#[derive(Debug, Clone)]
pub enum FilterStrategy {
    /// Push the filter as a native SQL expression.
    NativePushdown {
        /// SQL expression that uses the Bloom filter.
        sql_expression: String,
    },
    /// Apply filter client-side after fetching data.
    ClientSide {
        /// The Bloom filter to use for filtering.
        filter: BloomFilter,
        /// Column to filter on.
        column_name: String,
    },
    /// Materialize to temp table (fallback for very large filters).
    TempTable {
        /// Temp table name.
        table_name: String,
    },
}

/// Handles Bloom filter pushdown for semi-join optimization.
pub struct BloomFilterPushdown {
    /// The Bloom filter.
    filter: BloomFilter,
    /// Maximum filter size in bytes (default 1MB).
    max_size_bytes: usize,
}

impl BloomFilterPushdown {
    /// Create a new Bloom filter pushdown handler.
    pub fn new(false_positive_rate: f64) -> Self {
        Self {
            filter: BloomFilter::new(1000, false_positive_rate),
            max_size_bytes: 1024 * 1024, // 1MB default
        }
    }

    /// Set the maximum filter size.
    pub fn with_max_size(mut self, max_bytes: usize) -> Self {
        self.max_size_bytes = max_bytes;
        self
    }

    /// Build a Bloom filter from string keys.
    ///
    /// Deduplicates keys before insertion so `num_items` accurately reflects
    /// the unique key count (avoiding inflated FPP estimates).
    pub fn from_keys(keys: &[String], false_positive_rate: f64) -> BloomResult<Self> {
        let mut unique_keys: std::collections::HashSet<&String> = std::collections::HashSet::with_capacity(keys.len());
        unique_keys.extend(keys.iter());
        let filter = BloomFilter::new(unique_keys.len(), false_positive_rate);
        let max_size_bytes = 1024 * 1024; // 1MB default

        if filter.size_bytes() > max_size_bytes {
            return Err(BloomError::TooLarge(filter.size_bytes(), max_size_bytes));
        }

        let mut pushdown = Self {
            filter,
            max_size_bytes,
        };

        for key in &unique_keys {
            pushdown.filter.insert(key);
        }

        Ok(pushdown)
    }

    /// Get the underlying Bloom filter.
    pub fn filter(&self) -> &BloomFilter {
        &self.filter
    }

    /// Get the estimated false positive rate.
    pub fn estimated_fpp(&self) -> f64 {
        self.filter.estimated_fpp()
    }

    /// Determine the best filter strategy for a source type.
    ///
    /// Currently, all sources use client-side Bloom filtering because:
    /// - ClickHouse Bloom filter indexes are table-level, not query-level
    /// - PostgreSQL requires extensions for Bloom filter support
    /// - Other sources have no native support
    ///
    /// Future enhancement: For ClickHouse tables with Bloom filter indexes,
    /// queries can leverage the index automatically without explicit SQL.
    pub fn to_filter_strategy(
        &self,
        column: &str,
        _source_type: SourceType,
    ) -> FilterStrategy {
        // All sources currently use client-side filtering
        // Native pushdown requires table-level Bloom filter indexes which
        // must be configured separately and are used automatically by the engine.
        FilterStrategy::ClientSide {
            filter: self.filter.clone(),
            column_name: column.to_string(),
        }
    }

    /// Generate SQL for the filter strategy.
    ///
    /// Currently always returns `None` because all sources use client-side
    /// filtering. Will return `Some` once native Bloom filter pushdown is
    /// implemented for specific database backends.
    pub fn to_sql(&self, column: &str, source_type: SourceType) -> Option<String> {
        match self.to_filter_strategy(column, source_type) {
            FilterStrategy::NativePushdown { sql_expression } => Some(sql_expression),
            _ => None,
        }
    }

    /// Check if a value passes the filter.
    pub fn might_contain(&self, value: &str) -> bool {
        self.filter.might_contain(&value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_basic() {
        let mut filter = BloomFilter::new(1000, 0.01);

        filter.insert(&"hello");
        filter.insert(&"world");
        filter.insert(&"test");

        assert!(filter.might_contain(&"hello"));
        assert!(filter.might_contain(&"world"));
        assert!(filter.might_contain(&"test"));

        // These should likely not be in the filter (may have false positives)
        // We can't assert they're definitely not there due to FP nature
        assert_eq!(filter.len(), 3);
    }

    #[test]
    fn test_bloom_filter_false_positive_rate() {
        let mut filter = BloomFilter::new(1000, 0.01);

        // Insert 1000 items
        for i in 0..1000 {
            filter.insert(&format!("key_{}", i));
        }

        // Check false positives for items not in the set
        let mut false_positives = 0;
        for i in 1000..2000 {
            if filter.might_contain(&format!("key_{}", i)) {
                false_positives += 1;
            }
        }

        // Should be roughly 1% false positive rate (allow some variance)
        let fpp = false_positives as f64 / 1000.0;
        assert!(fpp < 0.05, "False positive rate {} is too high", fpp);
    }

    #[test]
    fn test_bloom_filter_serialization() {
        let mut original = BloomFilter::new(100, 0.01);
        original.insert(&"foo");
        original.insert(&"bar");
        original.insert(&"baz");

        let bytes = original.to_bytes();
        let restored = BloomFilter::from_bytes(&bytes).unwrap();

        assert!(restored.might_contain(&"foo"));
        assert!(restored.might_contain(&"bar"));
        assert!(restored.might_contain(&"baz"));
        assert_eq!(restored.len(), original.len());
    }

    #[test]
    fn test_bloom_pushdown_from_keys() {
        let keys: Vec<String> = (0..1000).map(|i| format!("id_{}", i)).collect();

        let pushdown = BloomFilterPushdown::from_keys(&keys, 0.01).unwrap();

        // All keys should be found
        for key in &keys {
            assert!(pushdown.might_contain(key));
        }

        assert!(pushdown.filter.size_bytes() < 1024 * 1024); // Under 1MB
    }

    #[test]
    fn test_filter_strategy_snowflake() {
        // All sources now use client-side filtering until proper database-level
        // Bloom filter index support is implemented
        let keys: Vec<String> = vec!["a".to_string(), "b".to_string()];
        let pushdown = BloomFilterPushdown::from_keys(&keys, 0.01).unwrap();

        let strategy = pushdown.to_filter_strategy("user_id", SourceType::Snowflake);

        match strategy {
            FilterStrategy::ClientSide { column_name, .. } => {
                assert_eq!(column_name, "user_id");
            }
            _ => panic!("Expected ClientSide for Snowflake"),
        }
    }

    #[test]
    fn test_filter_strategy_postgres() {
        let keys: Vec<String> = vec!["x".to_string()];
        let pushdown = BloomFilterPushdown::from_keys(&keys, 0.01).unwrap();

        let strategy = pushdown.to_filter_strategy("col", SourceType::PostgreSQL);

        match strategy {
            FilterStrategy::ClientSide { column_name, .. } => {
                assert_eq!(column_name, "col");
            }
            _ => panic!("Expected ClientSide for PostgreSQL"),
        }
    }

    #[test]
    fn test_estimated_fpp() {
        let mut filter = BloomFilter::new(100, 0.01);

        // Empty filter should have 0 FPP
        assert_eq!(filter.estimated_fpp(), 0.0);

        // Add some items
        for i in 0..50 {
            filter.insert(&i);
        }

        // FPP should be low since we're under capacity
        assert!(filter.estimated_fpp() < 0.1);
    }

    #[test]
    fn test_bloom_filter_zero_expected_items() {
        let filter = BloomFilter::new(0, 0.01);
        assert!(filter.num_bits >= MIN_BLOOM_BITS,
            "Zero expected items should not produce broken filter");
        assert!(filter.num_hashes >= 1 && filter.num_hashes <= MAX_HASH_FUNCTIONS);
    }

    #[test]
    fn test_bloom_filter_serialization_roundtrip_large() {
        let mut original = BloomFilter::new(100_000, 0.01);
        for i in 0..1000 {
            original.insert(&format!("item_{}", i));
        }

        let bytes = original.to_bytes();
        let restored = BloomFilter::from_bytes(&bytes).unwrap();

        assert_eq!(restored.num_bits, original.num_bits);
        assert_eq!(restored.num_hashes, original.num_hashes);
        assert_eq!(restored.num_items, original.num_items);
        assert_eq!(restored.bits, original.bits);

        for i in 0..1000 {
            assert!(restored.might_contain(&format!("item_{}", i)));
        }
    }

    #[test]
    fn test_from_keys_rejects_oversized_before_insertion() {
        let keys: Vec<String> = (0..10_000_000)
            .map(|i| format!("key_{}", i))
            .collect();

        let result = BloomFilterPushdown::from_keys(&keys, 0.001);
        assert!(result.is_err(), "Oversized filter should be rejected");
        match result {
            Err(BloomError::TooLarge(actual, max)) => {
                assert!(actual > max, "Actual size {} should exceed max {}", actual, max);
            }
            Err(other) => panic!("Expected TooLarge error, got {:?}", other),
            Ok(_) => panic!("Expected error but got Ok"),
        }
    }

    /// Regression test for Bug 3: `&str` and `String` must produce identical
    /// Bloom filter results since they hash identically in Rust.
    #[test]
    fn test_bloom_filter_str_and_string_equivalent() {
        let mut filter = BloomFilter::new(100, 0.01);
        let owned = String::from("test_value");
        let borrowed: &str = "test_value";

        filter.insert(&owned);

        assert!(
            filter.might_contain(&borrowed),
            "&str lookup must match String insertion"
        );
        assert!(
            filter.might_contain(&owned),
            "String lookup must also still work"
        );

        let mut filter2 = BloomFilter::new(100, 0.01);
        filter2.insert(&borrowed);

        assert!(
            filter2.might_contain(&owned),
            "String lookup must match &str insertion"
        );
    }

    #[test]
    fn test_bloom_filter_num_items_tracks_inserts() {
        let mut filter = BloomFilter::new(1000, 0.01);

        filter.insert(&"value_a");
        assert_eq!(filter.len(), 1);

        filter.insert(&"value_b");
        assert_eq!(filter.len(), 2);

        filter.insert(&"value_c");
        assert_eq!(filter.len(), 3);
    }

    #[test]
    fn test_bloom_filter_estimated_fpp_accurate_with_unique_keys() {
        let mut filter = BloomFilter::new(100, 0.01);

        for i in 0..5 {
            filter.insert(&format!("key_{}", i));
        }

        assert_eq!(filter.len(), 5);
        let fpp = filter.estimated_fpp();
        assert!(
            fpp < 0.01,
            "FPP with 5 items in a 100-capacity filter should be very low, got {}",
            fpp
        );
    }

    #[test]
    fn test_from_bytes_rejects_zero_num_hashes() {
        let mut bytes = vec![0u8; 32];
        // num_bits = 64 (valid)
        bytes[0..8].copy_from_slice(&64u64.to_le_bytes());
        // num_hashes = 0 (invalid)
        bytes[8..12].copy_from_slice(&0u32.to_le_bytes());
        // padding
        bytes[12..16].copy_from_slice(&0u32.to_le_bytes());
        // num_items = 0
        bytes[16..24].copy_from_slice(&0u64.to_le_bytes());

        assert!(
            BloomFilter::from_bytes(&bytes).is_none(),
            "num_hashes=0 would make might_contain() always return true"
        );
    }

    #[test]
    fn test_from_bytes_rejects_excessive_num_hashes() {
        let mut bytes = vec![0u8; 32];
        bytes[0..8].copy_from_slice(&64u64.to_le_bytes());
        bytes[8..12].copy_from_slice(&(MAX_HASH_FUNCTIONS + 1).to_le_bytes());
        bytes[12..16].copy_from_slice(&0u32.to_le_bytes());
        bytes[16..24].copy_from_slice(&0u64.to_le_bytes());

        assert!(
            BloomFilter::from_bytes(&bytes).is_none(),
            "num_hashes > MAX_HASH_FUNCTIONS should be rejected"
        );
    }

    #[test]
    fn test_from_bytes_rejects_zero_num_bits() {
        let mut bytes = vec![0u8; 24];
        // num_bits = 0 (invalid)
        bytes[0..8].copy_from_slice(&0u64.to_le_bytes());
        bytes[8..12].copy_from_slice(&3u32.to_le_bytes());
        bytes[12..16].copy_from_slice(&0u32.to_le_bytes());
        bytes[16..24].copy_from_slice(&0u64.to_le_bytes());

        assert!(
            BloomFilter::from_bytes(&bytes).is_none(),
            "num_bits=0 should be rejected"
        );
    }

    #[test]
    fn test_from_bytes_valid_roundtrip() {
        let mut filter = BloomFilter::new(50, 0.01);
        filter.insert(&"apple");
        filter.insert(&"banana");

        let bytes = filter.to_bytes();
        let restored = BloomFilter::from_bytes(&bytes).unwrap();

        assert!(restored.might_contain(&"apple"));
        assert!(restored.might_contain(&"banana"));
        assert_eq!(restored.len(), filter.len());
        assert_eq!(restored.num_hashes, filter.num_hashes);
        assert_eq!(restored.num_bits, filter.num_bits);
    }

    #[test]
    fn test_bloom_filter_zero_false_positive_rate_does_not_panic() {
        let filter = BloomFilter::new(10, 0.0);
        assert!(filter.num_bits >= MIN_BLOOM_BITS);
        assert!(filter.num_hashes >= 1);
    }

    #[test]
    fn test_bloom_filter_negative_false_positive_rate_does_not_panic() {
        let filter = BloomFilter::new(10, -0.5);
        assert!(filter.num_bits >= MIN_BLOOM_BITS);
        assert!(filter.num_hashes >= 1);
    }

    #[test]
    fn test_bloom_filter_fpp_one_does_not_panic() {
        let filter = BloomFilter::new(10, 1.0);
        assert!(filter.num_bits >= MIN_BLOOM_BITS);
        assert!(filter.num_hashes >= 1);
    }

    #[test]
    fn test_bloom_filter_num_bits_capped_at_max() {
        let filter = BloomFilter::new(usize::MAX / 2, 1e-15);
        assert!(
            filter.num_bits <= MAX_BLOOM_BITS,
            "num_bits {} should be capped at MAX_BLOOM_BITS {}",
            filter.num_bits,
            MAX_BLOOM_BITS
        );

        let bytes = filter.to_bytes();
        let restored = BloomFilter::from_bytes(&bytes);
        assert!(
            restored.is_some(),
            "Capped filter must survive round-trip serialization"
        );
    }

    #[test]
    fn test_from_keys_deduplicates() {
        let keys = vec![
            "a".to_string(),
            "b".to_string(),
            "a".to_string(),
            "c".to_string(),
            "b".to_string(),
        ];
        let pushdown = BloomFilterPushdown::from_keys(&keys, 0.01).unwrap();
        assert_eq!(
            pushdown.filter().num_items, 3,
            "Duplicate keys should be deduplicated: expected 3 unique items, got {}",
            pushdown.filter().num_items
        );
        assert!(pushdown.filter().might_contain(&"a".to_string()));
        assert!(pushdown.filter().might_contain(&"b".to_string()));
        assert!(pushdown.filter().might_contain(&"c".to_string()));
    }
}
