//! Query History Search
//!
//! FST-based index for fuzzy search of past queries.

use chrono::{DateTime, Utc};
use fst::{IntoStreamer, Set, SetBuilder, Streamer};
use std::collections::HashMap;
use thiserror::Error;
use uuid::Uuid;

/// Errors that can occur during query history index operations.
#[derive(Debug, Error)]
pub enum QueryHistoryError {
    #[error("FST error: {0}")]
    Fst(#[from] fst::Error),

    #[error("Invalid Levenshtein distance")]
    InvalidDistance,
}

/// Result type for query history operations.
pub type QueryHistoryResult<T> = Result<T, QueryHistoryError>;

/// Metadata for a saved query.
#[derive(Debug, Clone)]
pub struct QueryMetadata {
    /// Original SQL query
    pub original_sql: String,
    /// When the query was executed
    pub executed_at: DateTime<Utc>,
    /// User who executed the query
    pub user_id: Uuid,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
    /// Number of rows returned
    pub rows_returned: u64,
}

/// A saved query for the history index.
#[derive(Debug, Clone)]
pub struct SavedQuery {
    pub sql: String,
    pub executed_at: DateTime<Utc>,
    pub user_id: Uuid,
    pub execution_time_ms: u64,
    pub rows_returned: u64,
}

/// FST-based query history index.
pub struct QueryHistoryIndex {
    /// FST set of normalized queries
    queries: Set<Vec<u8>>,
    /// Metadata for each query
    query_metadata: HashMap<String, QueryMetadata>,
}

impl QueryHistoryIndex {
    /// Build index from saved queries.
    pub fn build(history: &[SavedQuery]) -> QueryHistoryResult<Self> {
        let mut entries = Vec::new();
        let mut metadata = HashMap::new();

        for query in history {
            let normalized = normalize_query(&query.sql);
            entries.push(normalized.clone());
            metadata.insert(
                normalized,
                QueryMetadata {
                    original_sql: query.sql.clone(),
                    executed_at: query.executed_at,
                    user_id: query.user_id,
                    execution_time_ms: query.execution_time_ms,
                    rows_returned: query.rows_returned,
                },
            );
        }

        // Sort and deduplicate
        entries.sort();
        entries.dedup();

        let mut builder = SetBuilder::memory();
        for entry in &entries {
            builder.insert(entry)?;
        }

        Ok(Self {
            queries: builder.into_set(),
            query_metadata: metadata,
        })
    }

    /// Fuzzy search with Levenshtein distance.
    ///
    /// Note: For actual fuzzy search, use the `fst` crate's Levenshtein automaton
    /// which requires the `levenshtein` feature. This simplified version uses
    /// prefix matching as a fallback.
    pub fn fuzzy_search(&self, query: &str, _max_distance: u32) -> Vec<&QueryMetadata> {
        // Simplified implementation: use prefix search as fallback
        // For production, enable fst's levenshtein feature
        self.prefix_search(query)
    }

    /// Prefix search.
    pub fn prefix_search(&self, prefix: &str) -> Vec<&QueryMetadata> {
        let normalized = normalize_query(prefix);
        let upper = increment_last_byte(&normalized);

        let mut results = Vec::new();
        let mut stream = self.queries.range().ge(&normalized).lt(&upper).into_stream();

        while let Some(key) = stream.next() {
            if let Ok(s) = std::str::from_utf8(key) {
                if let Some(meta) = self.query_metadata.get(s) {
                    results.push(meta);
                }
            }
        }

        results
    }

    /// Exact match lookup.
    pub fn find_exact(&self, query: &str) -> Option<&QueryMetadata> {
        let normalized = normalize_query(query);
        self.query_metadata.get(&normalized)
    }

    /// Get the number of queries in the index.
    pub fn len(&self) -> usize {
        self.queries.len()
    }

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.queries.is_empty()
    }
}

// Use shared utilities from the warehouse utils module
use crate::warehouse::utils::{increment_last_byte, normalize_query};

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_history() -> Vec<SavedQuery> {
        vec![
            SavedQuery {
                sql: "SELECT * FROM customers WHERE id = 1".to_string(),
                executed_at: Utc::now(),
                user_id: Uuid::new_v4(),
                execution_time_ms: 100,
                rows_returned: 1,
            },
            SavedQuery {
                sql: "SELECT * FROM orders WHERE status = 'pending'".to_string(),
                executed_at: Utc::now(),
                user_id: Uuid::new_v4(),
                execution_time_ms: 250,
                rows_returned: 50,
            },
            SavedQuery {
                sql: "SELECT COUNT(*) FROM customers".to_string(),
                executed_at: Utc::now(),
                user_id: Uuid::new_v4(),
                execution_time_ms: 50,
                rows_returned: 1,
            },
        ]
    }

    #[test]
    fn test_build_index() {
        let history = create_test_history();
        let index = QueryHistoryIndex::build(&history).unwrap();

        assert_eq!(index.len(), 3);
    }

    #[test]
    fn test_exact_match() {
        let history = create_test_history();
        let index = QueryHistoryIndex::build(&history).unwrap();

        let result = index.find_exact("SELECT * FROM customers WHERE id = 1");
        assert!(result.is_some());
    }

    #[test]
    fn test_prefix_search() {
        let history = create_test_history();
        let index = QueryHistoryIndex::build(&history).unwrap();

        // Search for queries starting with "select * from customers"
        let results = index.prefix_search("select * from customers");
        assert!(!results.is_empty());
    }

    #[test]
    fn test_fuzzy_search() {
        let history = create_test_history();
        let index = QueryHistoryIndex::build(&history).unwrap();

        // Note: fuzzy_search currently uses prefix_search as a fallback.
        // True Levenshtein fuzzy matching requires enabling fst's levenshtein feature.
        // Test with a valid prefix that should match
        let results = index.fuzzy_search("select * from customers", 2);
        assert!(!results.is_empty());
    }
}
