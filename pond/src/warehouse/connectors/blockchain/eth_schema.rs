//! Arrow schemas for Ethereum tables.
//!
//! Defines the column layouts for blocks, transactions, and logs
//! tables that the Ethereum connector exposes.  Delegates to the
//! EVM schema definitions in `super` (mod.rs) so the canonical
//! column list lives in one place.

use super::{evm_blocks_schema, evm_logs_schema, evm_transactions_schema};
use crate::warehouse::types::TableSchema;

/// Table name constants.
pub const TABLE_BLOCKS: &str = "blocks";
pub const TABLE_TRANSACTIONS: &str = "transactions";
pub const TABLE_LOGS: &str = "logs";

/// All Ethereum table names.
pub const ALL_TABLES: &[&str] = &[TABLE_BLOCKS, TABLE_TRANSACTIONS, TABLE_LOGS];

/// Schema for the `blocks` table.
pub fn blocks_schema() -> TableSchema {
    evm_blocks_schema()
}

/// Schema for the `transactions` table.
pub fn transactions_schema() -> TableSchema {
    evm_transactions_schema()
}

/// Schema for the `logs` table.
pub fn logs_schema() -> TableSchema {
    evm_logs_schema()
}

/// Return the schema for a given Ethereum table name.
pub fn schema_for_table(table: &str) -> Option<TableSchema> {
    match table {
        TABLE_BLOCKS => Some(blocks_schema()),
        TABLE_TRANSACTIONS => Some(transactions_schema()),
        TABLE_LOGS => Some(logs_schema()),
        _ => None,
    }
}

/// Return the primary key columns for a given Ethereum table.
pub fn primary_key_for_table(table: &str) -> Vec<String> {
    match table {
        TABLE_BLOCKS => vec!["block_number".to_string()],
        TABLE_TRANSACTIONS => vec!["tx_hash".to_string()],
        TABLE_LOGS => vec!["block_number".to_string(), "log_index".to_string()],
        _ => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blocks_schema_columns() {
        let schema = blocks_schema();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"block_number"));
        assert!(names.contains(&"block_hash"));
        assert!(names.contains(&"timestamp"));
        assert!(names.contains(&"gas_used"));
    }

    #[test]
    fn test_transactions_schema_columns() {
        let schema = transactions_schema();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"tx_hash"));
        assert!(names.contains(&"block_number"));
        assert!(names.contains(&"from_address"));
        assert!(names.contains(&"value"));
    }

    #[test]
    fn test_logs_schema_columns() {
        let schema = logs_schema();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"log_index"));
        assert!(names.contains(&"address"));
        assert!(names.contains(&"topic0"));
        assert!(names.contains(&"data"));
    }

    #[test]
    fn test_schema_for_table() {
        assert!(schema_for_table(TABLE_BLOCKS).is_some());
        assert!(schema_for_table(TABLE_TRANSACTIONS).is_some());
        assert!(schema_for_table(TABLE_LOGS).is_some());
        assert!(schema_for_table("unknown").is_none());
    }

    #[test]
    fn test_primary_key_for_table() {
        assert_eq!(primary_key_for_table(TABLE_BLOCKS), vec!["block_number"]);
        assert_eq!(primary_key_for_table(TABLE_TRANSACTIONS), vec!["tx_hash"]);
        assert_eq!(
            primary_key_for_table(TABLE_LOGS),
            vec!["block_number", "log_index"]
        );
    }

    #[test]
    fn test_all_tables_constant() {
        assert_eq!(ALL_TABLES.len(), 3);
        assert!(ALL_TABLES.contains(&TABLE_BLOCKS));
        assert!(ALL_TABLES.contains(&TABLE_TRANSACTIONS));
        assert!(ALL_TABLES.contains(&TABLE_LOGS));
    }
}
