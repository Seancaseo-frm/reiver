//! LRU cache for NL-to-SQL query results.
//!
//! Keyed by `(project_id, normalized_question_hash)` → cached SQL string.
//! Entries are invalidated when the project's catalog changes.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use lru::LruCache;
use parking_lot::RwLock;
use uuid::Uuid;

const CACHE_CAPACITY: usize = 10_000;

/// Cached NL query result.
struct CachedEntry {
    sql: String,
}

/// Thread-safe LRU cache for NL query SQL results.
pub struct NlQueryCache {
    inner: RwLock<LruCache<(Uuid, u64), CachedEntry>>,
}

impl NlQueryCache {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(LruCache::new(
                std::num::NonZeroUsize::new(CACHE_CAPACITY).unwrap(),
            )),
        }
    }

    /// Look up a cached SQL query for the given project and question.
    pub fn get(&self, project_id: Uuid, question: &str) -> Option<String> {
        let key = (project_id, normalize_and_hash(question));
        self.inner.write().get(&key).map(|e| e.sql.clone())
    }

    /// Insert a successful NL query result into the cache.
    pub fn insert(&self, project_id: Uuid, question: &str, sql: &str) {
        let key = (project_id, normalize_and_hash(question));
        self.inner.write().put(key, CachedEntry {
            sql: sql.to_string(),
        });
    }

    /// Remove a specific entry (e.g., when cached SQL is stale).
    pub fn remove(&self, project_id: Uuid, question: &str) {
        let key = (project_id, normalize_and_hash(question));
        self.inner.write().pop(&key);
    }

    /// Invalidate all cached entries for a project (after catalog changes).
    pub fn invalidate_project(&self, project_id: Uuid) {
        let mut cache = self.inner.write();
        let keys_to_remove: Vec<(Uuid, u64)> = cache
            .iter()
            .filter(|((pid, _), _)| *pid == project_id)
            .map(|(k, _)| *k)
            .collect();
        for key in keys_to_remove {
            cache.pop(&key);
        }
    }
}

/// Normalize a question and produce a hash for cache keying.
///
/// Normalization: lowercase, collapse whitespace, strip trailing punctuation.
fn normalize_and_hash(question: &str) -> u64 {
    let normalized: String = question
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim_end_matches(|c: char| c.is_ascii_punctuation())
        .to_string();

    let mut hasher = DefaultHasher::new();
    normalized.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pid() -> Uuid {
        Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap()
    }

    fn pid2() -> Uuid {
        Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap()
    }

    #[test]
    fn test_cache_miss() {
        let cache = NlQueryCache::new();
        assert!(cache.get(pid(), "how many orders?").is_none());
    }

    #[test]
    fn test_cache_hit() {
        let cache = NlQueryCache::new();
        cache.insert(pid(), "how many orders?", "SELECT count(*) FROM orders");
        let result = cache.get(pid(), "how many orders?");
        assert_eq!(result.as_deref(), Some("SELECT count(*) FROM orders"));
    }

    #[test]
    fn test_normalization_whitespace() {
        let cache = NlQueryCache::new();
        cache.insert(pid(), "how many  orders?", "SELECT count(*) FROM orders");
        assert!(cache.get(pid(), "how many orders?").is_some());
    }

    #[test]
    fn test_normalization_case() {
        let cache = NlQueryCache::new();
        cache.insert(pid(), "How Many Orders?", "SELECT count(*) FROM orders");
        assert!(cache.get(pid(), "how many orders?").is_some());
    }

    #[test]
    fn test_normalization_trailing_punctuation() {
        let cache = NlQueryCache::new();
        cache.insert(pid(), "how many orders", "SELECT count(*) FROM orders");
        assert!(cache.get(pid(), "how many orders?").is_some());
    }

    #[test]
    fn test_project_isolation() {
        let cache = NlQueryCache::new();
        cache.insert(pid(), "count", "SELECT count(*) FROM a");
        assert!(cache.get(pid2(), "count").is_none());
    }

    #[test]
    fn test_remove() {
        let cache = NlQueryCache::new();
        cache.insert(pid(), "count", "SELECT count(*) FROM a");
        cache.remove(pid(), "count");
        assert!(cache.get(pid(), "count").is_none());
    }

    #[test]
    fn test_invalidate_project() {
        let cache = NlQueryCache::new();
        cache.insert(pid(), "q1", "sql1");
        cache.insert(pid(), "q2", "sql2");
        cache.insert(pid2(), "q1", "sql3");

        cache.invalidate_project(pid());

        assert!(cache.get(pid(), "q1").is_none());
        assert!(cache.get(pid(), "q2").is_none());
        assert!(cache.get(pid2(), "q1").is_some());
    }
}
