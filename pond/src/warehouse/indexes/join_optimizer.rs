//! Join Key Pre-filtering
//!
//! FST-based index to skip empty JOINs and estimate join cardinality.
//!
//! PERFORMANCE:
//! - FST intersection operations for O(n+m) complexity instead of O(n*m)
//! - Lock-free reads via `quick_cache` for overlap results
//! - Concurrent table key updates via `DashMap`
//! - `CompactString` for inline table names (no heap allocation for names <= 24 bytes)

use compact_str::CompactString;
use dashmap::DashMap;
use fst::set::OpBuilder;
use fst::{Set, SetBuilder, Streamer};
use quick_cache::sync::Cache;
use thiserror::Error;

/// Errors that can occur during join optimization operations.
#[derive(Debug, Error)]
pub enum JoinOptimizerError {
    #[error("FST error: {0}")]
    Fst(#[from] fst::Error),

    #[error("Table not found: {0}")]
    TableNotFound(String),
}

/// Result type for join optimizer operations.
pub type JoinOptimizerResult<T> = Result<T, JoinOptimizerError>;

/// Cached result of a join overlap check.
#[derive(Debug, Clone)]
struct OverlapCacheEntry {
    has_overlap: bool,
    intersection_size: usize,
}

/// FST-based join key index.
///
/// Fully concurrent: multiple readers and writers can operate simultaneously.
/// - `DashMap` for table keys (sharded concurrent map)
/// - `quick_cache` for overlap results (lock-free reads, S3-FIFO eviction)
/// - `CompactString` for table names (inlined up to 24 bytes)
pub struct JoinOptimizer {
    table_keys: DashMap<CompactString, Set<Vec<u8>>>,
    overlap_cache: Cache<(CompactString, CompactString), OverlapCacheEntry>,
}

impl JoinOptimizer {
    pub fn new() -> Self {
        Self {
            table_keys: DashMap::new(),
            overlap_cache: Cache::new(1024),
        }
    }

    /// Add keys for a table. Invalidates cached overlap results involving this table.
    pub fn add_table_keys(&self, table: &str, keys: Vec<String>) -> JoinOptimizerResult<()> {
        let mut sorted_keys = keys;
        sorted_keys.sort_unstable();
        sorted_keys.dedup();

        let mut builder = SetBuilder::memory();
        for key in &sorted_keys {
            builder.insert(key)?;
        }

        let table_name = CompactString::from(table);
        self.table_keys
            .insert(table_name.clone(), builder.into_set());
        self.invalidate_cache_for_table(&table_name);

        Ok(())
    }

    fn invalidate_cache_for_table(&self, table: &CompactString) {
        let keys_to_remove: Vec<(CompactString, CompactString)> = self
            .table_keys
            .iter()
            .filter_map(|entry| {
                let other = entry.key();
                if other == table {
                    return None;
                }
                let key = if table < other {
                    (table.clone(), other.clone())
                } else {
                    (other.clone(), table.clone())
                };
                if self.overlap_cache.get(&key).is_some() {
                    Some(key)
                } else {
                    None
                }
            })
            .collect();

        for key in keys_to_remove {
            self.overlap_cache.remove(&key);
        }
    }

    fn get_or_compute_overlap(
        &self,
        left_table: &str,
        right_table: &str,
    ) -> Option<OverlapCacheEntry> {
        let cache_key = if left_table < right_table {
            (
                CompactString::from(left_table),
                CompactString::from(right_table),
            )
        } else {
            (
                CompactString::from(right_table),
                CompactString::from(left_table),
            )
        };

        if let Some(entry) = self.overlap_cache.get(&cache_key) {
            return Some(entry);
        }

        let left_ref = self.table_keys.get(left_table)?;
        let right_ref = self.table_keys.get(right_table)?;

        let intersection_size = compute_intersection_size(&left_ref, &right_ref);

        let entry = OverlapCacheEntry {
            has_overlap: intersection_size > 0,
            intersection_size,
        };

        self.overlap_cache.insert(cache_key, entry.clone());
        Some(entry)
    }

    /// Check if JOIN would return any rows.
    ///
    /// Returns in O(1) for cached pairs, O(n+m) on first call.
    pub fn has_key_overlap(&self, left_table: &str, right_table: &str) -> bool {
        match self.get_or_compute_overlap(left_table, right_table) {
            Some(entry) => entry.has_overlap,
            None => true,
        }
    }

    /// Estimate join cardinality via FST intersection size.
    pub fn estimate_join_size(&self, left_table: &str, right_table: &str) -> Option<usize> {
        self.get_or_compute_overlap(left_table, right_table)
            .map(|entry| entry.intersection_size)
    }

    /// Get common keys between two tables (for small result sets).
    pub fn get_common_keys(&self, left: &str, right: &str, limit: usize) -> Vec<String> {
        let (l, r) = match (self.table_keys.get(left), self.table_keys.get(right)) {
            (Some(l), Some(r)) => (l, r),
            _ => return vec![],
        };

        let mut results = Vec::with_capacity(limit);
        let mut op = OpBuilder::new();
        op.push(l.stream());
        op.push(r.stream());

        let mut intersection = op.intersection();
        while let Some(key) = intersection.next() {
            if results.len() >= limit {
                break;
            }
            if let Ok(s) = std::str::from_utf8(key) {
                results.push(s.to_string());
            }
        }

        results
    }

    /// Get keys unique to left table (for LEFT JOIN optimization).
    pub fn get_left_only_keys(&self, left: &str, right: &str, limit: usize) -> Vec<String> {
        let (l, r) = match (self.table_keys.get(left), self.table_keys.get(right)) {
            (Some(l), Some(r)) => (l, r),
            _ => return vec![],
        };

        let mut results = Vec::with_capacity(limit);
        let mut stream = l.stream();

        while let Some(key) = stream.next() {
            if results.len() >= limit {
                break;
            }
            if !r.contains(key) {
                if let Ok(s) = std::str::from_utf8(key) {
                    results.push(s.to_string());
                }
            }
        }

        results
    }

    /// Get keys unique to right table.
    pub fn get_right_only_keys(&self, left: &str, right: &str, limit: usize) -> Vec<String> {
        self.get_left_only_keys(right, left, limit)
    }

    pub fn table_count(&self) -> usize {
        self.table_keys.len()
    }

    pub fn key_count(&self, table: &str) -> Option<usize> {
        self.table_keys.get(table).map(|s| s.len())
    }

    /// Get cache statistics: (current_entries, max_possible_pairs).
    pub fn cache_stats(&self) -> (usize, usize) {
        let total_tables = self.table_keys.len();
        let max_cache_size = if total_tables >= 2 {
            total_tables * (total_tables - 1) / 2
        } else {
            0
        };
        (self.overlap_cache.len(), max_cache_size)
    }
}

/// Compute intersection size using FST streaming intersection — O(n+m).
fn compute_intersection_size(left: &Set<Vec<u8>>, right: &Set<Vec<u8>>) -> usize {
    let (small, large) = if left.len() <= right.len() {
        (left, right)
    } else {
        (right, left)
    };

    let mut count = 0;
    let mut stream = small.stream();
    while let Some(key) = stream.next() {
        if large.contains(key) {
            count += 1;
        }
    }
    count
}

impl Default for JoinOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_overlap() {
        let optimizer = JoinOptimizer::new();

        optimizer
            .add_table_keys(
                "orders",
                vec![
                    "cust_1".to_string(),
                    "cust_2".to_string(),
                    "cust_3".to_string(),
                ],
            )
            .unwrap();

        optimizer
            .add_table_keys(
                "customers",
                vec![
                    "cust_1".to_string(),
                    "cust_2".to_string(),
                    "cust_4".to_string(),
                ],
            )
            .unwrap();

        assert!(optimizer.has_key_overlap("orders", "customers"));
    }

    #[test]
    fn test_no_key_overlap() {
        let optimizer = JoinOptimizer::new();

        optimizer
            .add_table_keys("orders", vec!["cust_1".to_string(), "cust_2".to_string()])
            .unwrap();

        optimizer
            .add_table_keys(
                "customers",
                vec!["cust_3".to_string(), "cust_4".to_string()],
            )
            .unwrap();

        assert!(!optimizer.has_key_overlap("orders", "customers"));
    }

    #[test]
    fn test_estimate_join_size() {
        let optimizer = JoinOptimizer::new();

        optimizer
            .add_table_keys(
                "orders",
                vec![
                    "cust_1".to_string(),
                    "cust_2".to_string(),
                    "cust_3".to_string(),
                ],
            )
            .unwrap();

        optimizer
            .add_table_keys(
                "customers",
                vec![
                    "cust_1".to_string(),
                    "cust_2".to_string(),
                    "cust_4".to_string(),
                ],
            )
            .unwrap();

        assert_eq!(optimizer.estimate_join_size("orders", "customers"), Some(2));
    }

    #[test]
    fn test_get_common_keys() {
        let optimizer = JoinOptimizer::new();

        optimizer
            .add_table_keys(
                "orders",
                vec!["a".to_string(), "b".to_string(), "c".to_string()],
            )
            .unwrap();

        optimizer
            .add_table_keys(
                "customers",
                vec!["b".to_string(), "c".to_string(), "d".to_string()],
            )
            .unwrap();

        let common = optimizer.get_common_keys("orders", "customers", 10);
        assert_eq!(common.len(), 2);
        assert!(common.contains(&"b".to_string()));
        assert!(common.contains(&"c".to_string()));
    }

    #[test]
    fn test_missing_table() {
        let optimizer = JoinOptimizer::new();

        assert!(optimizer.has_key_overlap("orders", "customers"));
        assert!(optimizer
            .estimate_join_size("orders", "customers")
            .is_none());
    }

    #[test]
    fn test_overlap_caching() {
        let optimizer = JoinOptimizer::new();

        optimizer
            .add_table_keys(
                "orders",
                vec!["a".to_string(), "b".to_string(), "c".to_string()],
            )
            .unwrap();

        optimizer
            .add_table_keys(
                "customers",
                vec!["b".to_string(), "c".to_string(), "d".to_string()],
            )
            .unwrap();

        let (cache_size_before, _) = optimizer.cache_stats();
        assert_eq!(cache_size_before, 0);

        assert!(optimizer.has_key_overlap("orders", "customers"));

        let (cache_size_after, _) = optimizer.cache_stats();
        assert_eq!(cache_size_after, 1);

        assert!(optimizer.has_key_overlap("orders", "customers"));
        assert!(optimizer.has_key_overlap("customers", "orders"));

        let (cache_size_still, _) = optimizer.cache_stats();
        assert_eq!(cache_size_still, 1);
    }

    #[test]
    fn test_get_left_only_keys() {
        let optimizer = JoinOptimizer::new();

        optimizer
            .add_table_keys(
                "orders",
                vec!["a".to_string(), "b".to_string(), "c".to_string()],
            )
            .unwrap();

        optimizer
            .add_table_keys(
                "customers",
                vec!["b".to_string(), "c".to_string(), "d".to_string()],
            )
            .unwrap();

        let left_only = optimizer.get_left_only_keys("orders", "customers", 10);
        assert_eq!(left_only.len(), 1);
        assert!(left_only.contains(&"a".to_string()));

        let right_only = optimizer.get_right_only_keys("orders", "customers", 10);
        assert_eq!(right_only.len(), 1);
        assert!(right_only.contains(&"d".to_string()));
    }

    #[test]
    fn test_left_only_and_right_only_are_asymmetric() {
        let optimizer = JoinOptimizer::new();
        optimizer
            .add_table_keys("t1", vec!["x".to_string(), "y".to_string()])
            .unwrap();
        optimizer
            .add_table_keys("t2", vec!["y".to_string(), "z".to_string()])
            .unwrap();

        let left = optimizer.get_left_only_keys("t1", "t2", 10);
        let right = optimizer.get_right_only_keys("t1", "t2", 10);

        assert_eq!(left, vec!["x".to_string()]);
        assert_eq!(right, vec!["z".to_string()]);
        assert_ne!(left, right, "left-only and right-only must differ");
    }

    #[test]
    fn test_left_only_empty_when_subset() {
        let optimizer = JoinOptimizer::new();
        optimizer
            .add_table_keys("sub", vec!["a".to_string(), "b".to_string()])
            .unwrap();
        optimizer
            .add_table_keys(
                "sup",
                vec!["a".to_string(), "b".to_string(), "c".to_string()],
            )
            .unwrap();

        let left = optimizer.get_left_only_keys("sub", "sup", 10);
        assert!(left.is_empty(), "subset should have no unique keys");

        let right = optimizer.get_right_only_keys("sub", "sup", 10);
        assert_eq!(right, vec!["c".to_string()]);
    }

    #[test]
    fn test_cache_invalidation() {
        let optimizer = JoinOptimizer::new();

        optimizer
            .add_table_keys("orders", vec!["a".to_string(), "b".to_string()])
            .unwrap();

        optimizer
            .add_table_keys("customers", vec!["c".to_string(), "d".to_string()])
            .unwrap();

        assert!(!optimizer.has_key_overlap("orders", "customers"));

        let (cache_size, _) = optimizer.cache_stats();
        assert_eq!(cache_size, 1);

        optimizer
            .add_table_keys("customers", vec!["a".to_string(), "c".to_string()])
            .unwrap();

        let (cache_size_after, _) = optimizer.cache_stats();
        assert_eq!(cache_size_after, 0);

        assert!(optimizer.has_key_overlap("orders", "customers"));
    }
}
