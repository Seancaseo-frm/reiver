//! Query Cache Lookup
//!
//! FST-based index for O(1) query result cache hit detection.

use fst::{Map, MapBuilder};
use thiserror::Error;

use crate::warehouse::utils::{hash_query, normalize_query};

/// Errors that can occur during query cache operations.
#[derive(Debug, Error)]
pub enum QueryCacheError {
    #[error("FST error: {0}")]
    Fst(#[from] fst::Error),
}

/// Result type for query cache operations.
pub type QueryCacheResult<T> = Result<T, QueryCacheError>;

/// FST-based query cache index.
pub struct QueryCacheIndex {
    /// Map: normalized query hash -> cache entry id
    index: Map<Vec<u8>>,
}

impl QueryCacheIndex {
    /// Build a cache index from cached query hashes.
    pub fn build(entries: Vec<(String, u64)>) -> QueryCacheResult<Self> {
        let mut sorted_entries: Vec<_> = entries
            .into_iter()
            .map(|(query, id)| (hash_query(&normalize_query(&query)), id))
            .collect();

        sorted_entries.sort_by_key(|(hash, _)| hash.clone());
        sorted_entries.dedup_by(|a, b| a.0 == b.0);

        let mut builder = MapBuilder::memory();
        for (hash, id) in sorted_entries {
            builder.insert(&hash, id)?;
        }

        Ok(Self {
            index: builder.into_map(),
        })
    }

    /// Create an empty cache index.
    pub fn empty() -> QueryCacheResult<Self> {
        let builder = MapBuilder::memory();
        Ok(Self {
            index: builder.into_map(),
        })
    }

    /// O(1) check if query result is cached.
    pub fn get_cached(&self, query: &str) -> Option<u64> {
        let normalized = normalize_query(query);
        let hash = hash_query(&normalized);
        self.index.get(&hash)
    }

    /// Check if query is cached (returns bool).
    pub fn is_cached(&self, query: &str) -> bool {
        self.get_cached(query).is_some()
    }

    /// Get the number of cached queries.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_cache() {
        let cache = QueryCacheIndex::empty().unwrap();
        assert!(cache.is_empty());
        assert!(cache.get_cached("SELECT 1").is_none());
    }

    #[test]
    fn test_cache_hit() {
        let entries = vec![
            ("SELECT * FROM customers".to_string(), 1),
            ("SELECT * FROM orders".to_string(), 2),
        ];

        let cache = QueryCacheIndex::build(entries).unwrap();

        // Exact match
        assert_eq!(cache.get_cached("SELECT * FROM customers"), Some(1));

        // Normalized match (extra whitespace)
        assert_eq!(cache.get_cached("SELECT  *  FROM  customers"), Some(1));

        // Case insensitive
        assert_eq!(cache.get_cached("select * from customers"), Some(1));
    }

    #[test]
    fn test_cache_miss() {
        let entries = vec![("SELECT * FROM customers".to_string(), 1)];

        let cache = QueryCacheIndex::build(entries).unwrap();

        assert!(cache.get_cached("SELECT * FROM orders").is_none());
    }

    #[test]
    fn test_is_cached() {
        let entries = vec![("SELECT * FROM customers".to_string(), 1)];

        let cache = QueryCacheIndex::build(entries).unwrap();

        assert!(cache.is_cached("SELECT * FROM customers"));
        assert!(!cache.is_cached("SELECT * FROM orders"));
    }

    #[test]
    fn test_duplicate_queries_do_not_crash_fst() {
        let entries = vec![
            ("SELECT * FROM customers".to_string(), 1),
            ("SELECT * FROM customers".to_string(), 2),
            ("SELECT * FROM customers".to_string(), 3),
        ];
        let cache = QueryCacheIndex::build(entries).unwrap();
        assert!(cache.is_cached("SELECT * FROM customers"));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_normalized_duplicates_do_not_crash_fst() {
        let entries = vec![
            ("SELECT * FROM customers".to_string(), 1),
            ("select  *  from  customers".to_string(), 2),
        ];
        let cache = QueryCacheIndex::build(entries).unwrap();
        assert!(cache.is_cached("SELECT * FROM customers"));
    }
}
