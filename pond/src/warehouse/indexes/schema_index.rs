//! Schema Autocomplete Index
//!
//! FST index for instant table and column name autocomplete.

use fst::{IntoStreamer, Set, SetBuilder, Streamer};
use std::io;
use thiserror::Error;

use crate::warehouse::types::WarehouseTable;

/// Errors that can occur during schema index operations.
#[derive(Debug, Error)]
pub enum SchemaIndexError {
    #[error("FST error: {0}")]
    Fst(#[from] fst::Error),

    #[error("IO error: {0}")]
    Io(#[from] io::Error),
}

/// Result type for schema index operations.
pub type SchemaIndexResult<T> = Result<T, SchemaIndexError>;

/// FST-based schema index for autocomplete.
pub struct SchemaIndex {
    fst: Set<Vec<u8>>,
}

impl SchemaIndex {
    /// Build index from all warehouse tables and columns.
    pub fn build(tables: &[WarehouseTable]) -> SchemaIndexResult<Self> {
        let mut entries = Vec::new();

        for table in tables {
            // Index table name
            entries.push(table.name.clone());

            // Index fully qualified column names
            for column in &table.schema.columns {
                entries.push(format!("{}.{}", table.name, column.name));
            }
        }

        // Sort entries (required for FST)
        entries.sort();
        entries.dedup();

        let mut builder = SetBuilder::memory();
        for entry in &entries {
            builder.insert(entry)?;
        }

        Ok(Self {
            fst: builder.into_set(),
        })
    }

    /// Build index from a list of names.
    pub fn from_names(names: &[String]) -> SchemaIndexResult<Self> {
        let mut sorted_names = names.to_vec();
        sorted_names.sort();
        sorted_names.dedup();

        let mut builder = SetBuilder::memory();
        for name in &sorted_names {
            builder.insert(name)?;
        }

        Ok(Self {
            fst: builder.into_set(),
        })
    }

    /// Prefix search for autocomplete.
    pub fn autocomplete(&self, prefix: &str) -> Vec<String> {
        if prefix.is_empty() {
            return vec![];
        }

        let upper_bound = increment_last_byte(prefix);

        let mut results = Vec::new();
        let mut stream = if upper_bound == prefix {
            self.fst.range().ge(prefix).into_stream()
        } else {
            self.fst.range().ge(prefix).lt(&upper_bound).into_stream()
        };

        while let Some(key) = stream.next() {
            if let Ok(s) = std::str::from_utf8(key) {
                results.push(s.to_string());
            }
        }

        results
    }

    /// Exact match lookup.
    pub fn contains(&self, name: &str) -> bool {
        self.fst.contains(name)
    }

    /// Get the number of entries in the index.
    pub fn len(&self) -> usize {
        self.fst.len()
    }

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.fst.is_empty()
    }

    /// Get the size of the index in bytes.
    pub fn size_bytes(&self) -> usize {
        self.fst.as_fst().as_bytes().len()
    }
}

// Use the shared increment_last_byte from the parent module
use super::increment_last_byte;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::warehouse::types::{ColumnSchema, ColumnType, TableSchema};

    fn create_test_tables() -> Vec<WarehouseTable> {
        vec![
            WarehouseTable {
                id: uuid::Uuid::new_v4(),
                source_id: uuid::Uuid::new_v4(),
                name: "customers".to_string(),
                schema: TableSchema {
                    columns: vec![
                        ColumnSchema::new("id", ColumnType::String, false),
                        ColumnSchema::new("email", ColumnType::String, true),
                    ],
                },
                storage_type: crate::warehouse::types::StorageType::default(),
                r2_prefix: "stripe/customers".to_string(),
                clickhouse_table: None,
                sync_enabled: true,
                incremental_key: Some("created".to_string()),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            },
            WarehouseTable {
                id: uuid::Uuid::new_v4(),
                source_id: uuid::Uuid::new_v4(),
                name: "charges".to_string(),
                schema: TableSchema {
                    columns: vec![
                        ColumnSchema::new("id", ColumnType::String, false),
                        ColumnSchema::new("amount", ColumnType::Int64, false),
                        ColumnSchema::new("customer_id", ColumnType::String, true),
                    ],
                },
                storage_type: crate::warehouse::types::StorageType::default(),
                r2_prefix: "stripe/charges".to_string(),
                clickhouse_table: None,
                sync_enabled: true,
                incremental_key: Some("created".to_string()),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            },
        ]
    }

    #[test]
    fn test_build_index() {
        let tables = create_test_tables();
        let index = SchemaIndex::build(&tables).unwrap();

        // Should contain table names
        assert!(index.contains("customers"));
        assert!(index.contains("charges"));

        // Should contain fully qualified column names
        assert!(index.contains("customers.id"));
        assert!(index.contains("customers.email"));
        assert!(index.contains("charges.amount"));
    }

    #[test]
    fn test_autocomplete() {
        let tables = create_test_tables();
        let index = SchemaIndex::build(&tables).unwrap();

        // Autocomplete "cu" should return "customers" and related
        let results = index.autocomplete("cu");
        assert!(results.iter().any(|r| r == "customers"));
        assert!(results.iter().any(|r| r == "customers.id"));
        assert!(results.iter().any(|r| r == "customers.email"));

        // Autocomplete "cha" should return "charges" and related
        let results = index.autocomplete("cha");
        assert!(results.iter().any(|r| r == "charges"));

        // Autocomplete "customers." should return column names
        let results = index.autocomplete("customers.");
        assert!(results.iter().any(|r| r == "customers.id"));
        assert!(results.iter().any(|r| r == "customers.email"));
    }

    #[test]
    fn test_empty_prefix() {
        let tables = create_test_tables();
        let index = SchemaIndex::build(&tables).unwrap();

        // Empty prefix should return nothing
        let results = index.autocomplete("");
        assert!(results.is_empty());
    }

    #[test]
    fn test_autocomplete_char_max_prefix() {
        let max_str = char::MAX.to_string();
        let key = format!("{}test", max_str);
        let names = vec![key.clone()];
        let index = SchemaIndex::from_names(&names).unwrap();

        let results = index.autocomplete(&max_str);
        assert_eq!(
            results.len(), 1,
            "Autocomplete with char::MAX prefix must find matching entries"
        );
        assert_eq!(results[0], key);
    }
}
