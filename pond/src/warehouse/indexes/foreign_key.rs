//! Foreign Key Validation
//!
//! FST-based index for validating foreign key references before sync.

use fst::{Set, SetBuilder};
use std::collections::HashMap;
use thiserror::Error;

/// Errors that can occur during foreign key validation.
#[derive(Debug, Error)]
pub enum ForeignKeyError {
    #[error("FST error: {0}")]
    Fst(#[from] fst::Error),

    #[error("Reference table not indexed: {0}")]
    TableNotIndexed(String),
}

/// Result type for foreign key operations.
pub type ForeignKeyResult<T> = Result<T, ForeignKeyError>;

/// Result of foreign key validation.
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// Number of valid references
    pub valid_count: usize,
    /// Keys that don't exist in the reference table
    pub invalid_keys: Vec<String>,
}

impl ValidationResult {
    /// Check if all references are valid.
    pub fn is_valid(&self) -> bool {
        self.invalid_keys.is_empty()
    }

    /// Get the number of invalid references.
    pub fn invalid_count(&self) -> usize {
        self.invalid_keys.len()
    }
}

/// FST-based foreign key index.
pub struct ForeignKeyIndex {
    /// Primary keys of reference tables
    reference_keys: HashMap<String, Set<Vec<u8>>>,
}

impl ForeignKeyIndex {
    /// Create a new foreign key index.
    pub fn new() -> Self {
        Self {
            reference_keys: HashMap::new(),
        }
    }

    /// Add primary keys for a reference table.
    pub fn add_reference_table(&mut self, table: &str, keys: Vec<String>) -> ForeignKeyResult<()> {
        let mut sorted_keys = keys;
        sorted_keys.sort();
        sorted_keys.dedup();

        let mut builder = SetBuilder::memory();
        for key in &sorted_keys {
            builder.insert(key)?;
        }

        self.reference_keys
            .insert(table.to_string(), builder.into_set());
        Ok(())
    }

    /// Validate a batch of foreign keys before INSERT.
    pub fn validate_batch(&self, ref_table: &str, keys: &[String]) -> ValidationResult {
        match self.reference_keys.get(ref_table) {
            Some(ref_set) => {
                let invalid: Vec<String> = keys
                    .iter()
                    .filter(|k| !ref_set.contains(k.as_str()))
                    .cloned()
                    .collect();

                ValidationResult {
                    valid_count: keys.len() - invalid.len(),
                    invalid_keys: invalid,
                }
            }
            None => {
                // No index = skip validation
                ValidationResult {
                    valid_count: keys.len(),
                    invalid_keys: vec![],
                }
            }
        }
    }

    /// Check if a single key exists in the reference table.
    pub fn key_exists(&self, ref_table: &str, key: &str) -> Option<bool> {
        self.reference_keys.get(ref_table).map(|s| s.contains(key))
    }

    /// Get the number of keys in a reference table.
    pub fn key_count(&self, table: &str) -> Option<usize> {
        self.reference_keys.get(table).map(|s| s.len())
    }

    /// Get all indexed tables.
    pub fn indexed_tables(&self) -> Vec<&str> {
        self.reference_keys.keys().map(|s| s.as_str()).collect()
    }

    /// Check if a table is indexed.
    pub fn is_indexed(&self, table: &str) -> bool {
        self.reference_keys.contains_key(table)
    }
}

impl Default for ForeignKeyIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_valid_keys() {
        let mut index = ForeignKeyIndex::new();
        index
            .add_reference_table(
                "customers",
                vec![
                    "cust_1".to_string(),
                    "cust_2".to_string(),
                    "cust_3".to_string(),
                ],
            )
            .unwrap();

        let keys = vec!["cust_1".to_string(), "cust_2".to_string()];
        let result = index.validate_batch("customers", &keys);

        assert!(result.is_valid());
        assert_eq!(result.valid_count, 2);
        assert!(result.invalid_keys.is_empty());
    }

    #[test]
    fn test_validate_invalid_keys() {
        let mut index = ForeignKeyIndex::new();
        index
            .add_reference_table(
                "customers",
                vec!["cust_1".to_string(), "cust_2".to_string()],
            )
            .unwrap();

        let keys = vec![
            "cust_1".to_string(),
            "cust_2".to_string(),
            "cust_99".to_string(), // Invalid
        ];
        let result = index.validate_batch("customers", &keys);

        assert!(!result.is_valid());
        assert_eq!(result.valid_count, 2);
        assert_eq!(result.invalid_count(), 1);
        assert!(result.invalid_keys.contains(&"cust_99".to_string()));
    }

    #[test]
    fn test_validate_unindexed_table() {
        let index = ForeignKeyIndex::new();

        // Unindexed table should pass validation
        let keys = vec!["any_key".to_string()];
        let result = index.validate_batch("unindexed_table", &keys);

        assert!(result.is_valid());
        assert_eq!(result.valid_count, 1);
    }

    #[test]
    fn test_key_exists() {
        let mut index = ForeignKeyIndex::new();
        index
            .add_reference_table("customers", vec!["cust_1".to_string()])
            .unwrap();

        assert_eq!(index.key_exists("customers", "cust_1"), Some(true));
        assert_eq!(index.key_exists("customers", "cust_99"), Some(false));
        assert_eq!(index.key_exists("unknown", "key"), None);
    }
}
