//! Tag/Label Autocomplete Index
//!
//! FST-based index for fast tag key and value autocomplete.

use fst::{IntoStreamer, Set, SetBuilder, Streamer};
use thiserror::Error;

use crate::warehouse::utils::increment_last_byte;

/// Errors that can occur during tag index operations.
#[derive(Debug, Error)]
pub enum TagIndexError {
    #[error("FST error: {0}")]
    Fst(#[from] fst::Error),
}

/// Result type for tag index operations.
pub type TagIndexResult<T> = Result<T, TagIndexError>;

/// FST-based tag index for autocomplete.
pub struct TagIndex {
    /// All tag keys: "host", "service", "env"
    tag_keys: Set<Vec<u8>>,
    /// Tag values prefixed by key: "service:api", "service:web", "env:prod"
    tag_values: Set<Vec<u8>>,
}

impl TagIndex {
    /// Build index from an iterator of (key, value) pairs.
    pub fn build(tags: impl IntoIterator<Item = (String, String)>) -> TagIndexResult<Self> {
        let mut keys = Vec::new();
        let mut values = Vec::new();

        for (key, value) in tags {
            keys.push(key.clone());
            values.push(format!("{}:{}", key, value));
        }

        // Sort and deduplicate
        keys.sort();
        keys.dedup();
        values.sort();
        values.dedup();

        let mut key_builder = SetBuilder::memory();
        for k in &keys {
            key_builder.insert(k)?;
        }

        let mut value_builder = SetBuilder::memory();
        for v in &values {
            value_builder.insert(v)?;
        }

        Ok(Self {
            tag_keys: key_builder.into_set(),
            tag_values: value_builder.into_set(),
        })
    }

    /// Autocomplete tag keys.
    pub fn autocomplete_keys(&self, prefix: &str) -> Vec<String> {
        let mut results = Vec::new();

        if prefix.is_empty() {
            let mut stream = self.tag_keys.stream();
            while let Some(key) = stream.next() {
                if let Ok(s) = std::str::from_utf8(key) {
                    results.push(s.to_string());
                }
            }
        } else {
            let upper = increment_last_byte(prefix);
            let mut stream = self.tag_keys.range().ge(prefix).lt(&upper).into_stream();
            while let Some(key) = stream.next() {
                if let Ok(s) = std::str::from_utf8(key) {
                    results.push(s.to_string());
                }
            }
        }

        results
    }

    /// Autocomplete tag values for a specific key.
    pub fn autocomplete_values(&self, key: &str, prefix: &str) -> Vec<String> {
        let search_prefix = format!("{}:{}", key, prefix);
        let upper = increment_last_byte(&search_prefix);

        let mut results = Vec::new();
        let mut stream = self
            .tag_values
            .range()
            .ge(&search_prefix)
            .lt(&upper)
            .into_stream();

        while let Some(value) = stream.next() {
            if let Ok(s) = std::str::from_utf8(value) {
                if let Some(value_part) = s.splitn(2, ':').nth(1) {
                    results.push(value_part.to_string());
                }
            }
        }

        results
    }

    /// Get all values for a specific key.
    pub fn get_values_for_key(&self, key: &str) -> Vec<String> {
        let prefix = format!("{}:", key);
        let upper = format!("{};\x00", key); // ';' is after ':' in ASCII

        let mut results = Vec::new();
        let mut stream = self.tag_values.range().ge(&prefix).lt(&upper).into_stream();

        while let Some(value) = stream.next() {
            if let Ok(s) = std::str::from_utf8(value) {
                if let Some(value_part) = s.splitn(2, ':').nth(1) {
                    results.push(value_part.to_string());
                }
            }
        }

        results
    }

    /// Get all tag keys.
    pub fn all_keys(&self) -> Vec<String> {
        let mut results = Vec::new();
        let mut stream = self.tag_keys.stream();

        while let Some(key) = stream.next() {
            if let Ok(s) = std::str::from_utf8(key) {
                results.push(s.to_string());
            }
        }

        results
    }

    /// Get the number of unique keys.
    pub fn key_count(&self) -> usize {
        self.tag_keys.len()
    }

    /// Get the number of unique key:value pairs.
    pub fn value_count(&self) -> usize {
        self.tag_values.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_index() -> TagIndex {
        let tags = vec![
            ("service".to_string(), "api".to_string()),
            ("service".to_string(), "web".to_string()),
            ("service".to_string(), "worker".to_string()),
            ("env".to_string(), "prod".to_string()),
            ("env".to_string(), "staging".to_string()),
            ("host".to_string(), "server-1".to_string()),
            ("host".to_string(), "server-2".to_string()),
        ];

        TagIndex::build(tags).unwrap()
    }

    #[test]
    fn test_autocomplete_keys() {
        let index = create_test_index();

        // Autocomplete "s" should return "service"
        let results = index.autocomplete_keys("s");
        assert!(results.contains(&"service".to_string()));

        // Autocomplete "e" should return "env"
        let results = index.autocomplete_keys("e");
        assert!(results.contains(&"env".to_string()));

        // All keys
        let all = index.all_keys();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_autocomplete_values() {
        let index = create_test_index();

        // Autocomplete values for "service" starting with "w"
        let results = index.autocomplete_values("service", "w");
        assert_eq!(results.len(), 2); // web, worker
        assert!(results.contains(&"web".to_string()));
        assert!(results.contains(&"worker".to_string()));

        // Autocomplete values for "env"
        let results = index.autocomplete_values("env", "p");
        assert!(results.contains(&"prod".to_string()));
    }

    #[test]
    fn test_get_all_values_for_key() {
        let index = create_test_index();

        let values = index.get_values_for_key("service");
        assert_eq!(values.len(), 3);
        assert!(values.contains(&"api".to_string()));
        assert!(values.contains(&"web".to_string()));
        assert!(values.contains(&"worker".to_string()));
    }

    #[test]
    fn test_counts() {
        let index = create_test_index();

        assert_eq!(index.key_count(), 3); // service, env, host
        assert_eq!(index.value_count(), 7); // all unique key:value pairs
    }

    #[test]
    fn test_autocomplete_keys_empty_prefix_returns_all() {
        let index = create_test_index();

        let all_keys = index.autocomplete_keys("");
        assert_eq!(all_keys.len(), 3,
            "Empty prefix should return all keys, got: {:?}", all_keys);
        assert!(all_keys.contains(&"env".to_string()));
        assert!(all_keys.contains(&"host".to_string()));
        assert!(all_keys.contains(&"service".to_string()));
    }

    #[test]
    fn test_autocomplete_keys_with_prefix() {
        let index = create_test_index();

        let results = index.autocomplete_keys("s");
        assert_eq!(results.len(), 1);
        assert!(results.contains(&"service".to_string()));

        let results_e = index.autocomplete_keys("e");
        assert_eq!(results_e.len(), 1);
        assert!(results_e.contains(&"env".to_string()));
    }
}
