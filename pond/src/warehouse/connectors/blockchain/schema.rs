//! Arrow schemas for Bitcoin tables.
//!
//! Defines the column layouts for blocks, transactions, inputs, and outputs
//! tables that the Bitcoin connector exposes.

use crate::warehouse::types::{ColumnSchema, ColumnType, TableSchema};

/// Table name constants.
pub const TABLE_BLOCKS: &str = "blocks";
pub const TABLE_TRANSACTIONS: &str = "transactions";
pub const TABLE_INPUTS: &str = "inputs";
pub const TABLE_OUTPUTS: &str = "outputs";

/// All Bitcoin table names.
pub const ALL_TABLES: &[&str] = &[TABLE_BLOCKS, TABLE_TRANSACTIONS, TABLE_INPUTS, TABLE_OUTPUTS];

// ── Blocks ───────────────────────────────────────────────────────────────

/// Schema for the `blocks` table.
///
/// Primary key: `block_height`
pub fn blocks_schema() -> TableSchema {
    TableSchema {
        columns: vec![
            ColumnSchema::new("block_height", ColumnType::Int64, false)
                .with_description("Block height in the chain"),
            ColumnSchema::new("block_hash", ColumnType::String, false)
                .with_description("Block header hash"),
            ColumnSchema::new("previous_block_hash", ColumnType::String, true)
                .with_description("Hash of the previous block"),
            ColumnSchema::new("timestamp", ColumnType::Timestamp, false)
                .with_description("Block timestamp (median time)")
                .with_timezone("UTC"),
            ColumnSchema::new("size", ColumnType::Int64, false)
                .with_description("Block size in bytes"),
            ColumnSchema::new("weight", ColumnType::Int64, false)
                .with_description("Block weight in weight units"),
            ColumnSchema::new("version", ColumnType::Int32, false)
                .with_description("Block version"),
            ColumnSchema::new("nonce", ColumnType::Int64, false)
                .with_description("Nonce used for proof-of-work"),
            ColumnSchema::new("difficulty", ColumnType::Float64, false)
                .with_description("Mining difficulty"),
            ColumnSchema::new("merkle_root", ColumnType::String, false)
                .with_description("Merkle root of the block transactions"),
            ColumnSchema::new("num_transactions", ColumnType::Int32, false)
                .with_description("Number of transactions in the block"),
            ColumnSchema::new("stripped_size", ColumnType::Int64, false)
                .with_description("Block size excluding witness data"),
        ],
    }
}

// ── Transactions ─────────────────────────────────────────────────────────

/// Schema for the `transactions` table.
///
/// Primary key: `txid`
pub fn transactions_schema() -> TableSchema {
    TableSchema {
        columns: vec![
            ColumnSchema::new("txid", ColumnType::String, false)
                .with_description("Transaction ID (hash)"),
            ColumnSchema::new("block_height", ColumnType::Int64, false)
                .with_description("Block height containing this transaction"),
            ColumnSchema::new("block_hash", ColumnType::String, false)
                .with_description("Block hash containing this transaction"),
            ColumnSchema::new("size", ColumnType::Int64, false)
                .with_description("Transaction size in bytes"),
            ColumnSchema::new("vsize", ColumnType::Int64, false)
                .with_description("Virtual size (weight / 4)"),
            ColumnSchema::new("weight", ColumnType::Int64, false)
                .with_description("Transaction weight"),
            ColumnSchema::new("version", ColumnType::Int32, false)
                .with_description("Transaction version"),
            ColumnSchema::new("locktime", ColumnType::Int64, false)
                .with_description("Transaction locktime"),
            ColumnSchema::new("fee", ColumnType::Int64, true)
                .with_description("Transaction fee in satoshis (null for coinbase)"),
            ColumnSchema::new("is_coinbase", ColumnType::Boolean, false)
                .with_description("Whether this is a coinbase transaction"),
            ColumnSchema::new("input_count", ColumnType::Int32, false)
                .with_description("Number of inputs"),
            ColumnSchema::new("output_count", ColumnType::Int32, false)
                .with_description("Number of outputs"),
            ColumnSchema::new("input_value", ColumnType::Int64, true)
                .with_description("Total input value in satoshis (null for coinbase)"),
            ColumnSchema::new("output_value", ColumnType::Int64, false)
                .with_description("Total output value in satoshis"),
        ],
    }
}

// ── Inputs ───────────────────────────────────────────────────────────────

/// Schema for the `inputs` table.
///
/// Primary key: (`txid`, `input_index`)
pub fn inputs_schema() -> TableSchema {
    TableSchema {
        columns: vec![
            ColumnSchema::new("txid", ColumnType::String, false)
                .with_description("Transaction ID containing this input"),
            ColumnSchema::new("input_index", ColumnType::Int32, false)
                .with_description("Index of this input within the transaction"),
            ColumnSchema::new("block_height", ColumnType::Int64, false)
                .with_description("Block height of the containing transaction"),
            ColumnSchema::new("prev_txid", ColumnType::String, true)
                .with_description("Previous output transaction ID (null for coinbase)"),
            ColumnSchema::new("prev_output_index", ColumnType::Int32, true)
                .with_description("Previous output index (null for coinbase)"),
            ColumnSchema::new("script_sig", ColumnType::String, true)
                .with_description("Input script (hex)"),
            ColumnSchema::new("sequence", ColumnType::Int64, false)
                .with_description("Sequence number"),
            ColumnSchema::new("witness", ColumnType::String, true)
                .with_description("Segregated witness data (JSON array of hex strings)"),
            ColumnSchema::new("value", ColumnType::Int64, true)
                .with_description("Value of the spent output in satoshis (if available)"),
            ColumnSchema::new("is_coinbase", ColumnType::Boolean, false)
                .with_description("Whether this input is a coinbase input"),
        ],
    }
}

// ── Outputs ──────────────────────────────────────────────────────────────

/// Schema for the `outputs` table.
///
/// Primary key: (`txid`, `output_index`)
pub fn outputs_schema() -> TableSchema {
    TableSchema {
        columns: vec![
            ColumnSchema::new("txid", ColumnType::String, false)
                .with_description("Transaction ID containing this output"),
            ColumnSchema::new("output_index", ColumnType::Int32, false)
                .with_description("Index of this output within the transaction"),
            ColumnSchema::new("block_height", ColumnType::Int64, false)
                .with_description("Block height of the containing transaction"),
            ColumnSchema::new("value_satoshis", ColumnType::Int64, false)
                .with_description("Output value in satoshis"),
            ColumnSchema::new("script_pubkey", ColumnType::String, false)
                .with_description("Output script (hex)"),
            ColumnSchema::new("script_type", ColumnType::String, true)
                .with_description("Script type (e.g. pubkeyhash, scripthash, witness_v0_keyhash)"),
            ColumnSchema::new("address", ColumnType::String, true)
                .with_description("Recipient address (null for non-standard outputs)"),
            ColumnSchema::new("required_signatures", ColumnType::Int32, true)
                .with_description("Number of required signatures"),
        ],
    }
}

/// Return the schema for a given Bitcoin table name.
pub fn schema_for_table(table: &str) -> Option<TableSchema> {
    match table {
        TABLE_BLOCKS => Some(blocks_schema()),
        TABLE_TRANSACTIONS => Some(transactions_schema()),
        TABLE_INPUTS => Some(inputs_schema()),
        TABLE_OUTPUTS => Some(outputs_schema()),
        _ => None,
    }
}

/// Return the primary key columns for a given Bitcoin table.
pub fn primary_key_for_table(table: &str) -> Vec<String> {
    match table {
        TABLE_BLOCKS => vec!["block_height".to_string()],
        TABLE_TRANSACTIONS => vec!["txid".to_string()],
        TABLE_INPUTS => vec!["txid".to_string(), "input_index".to_string()],
        TABLE_OUTPUTS => vec!["txid".to_string(), "output_index".to_string()],
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
        assert!(names.contains(&"block_height"));
        assert!(names.contains(&"block_hash"));
        assert!(names.contains(&"timestamp"));
        assert!(names.contains(&"difficulty"));
        assert!(names.contains(&"num_transactions"));
    }

    #[test]
    fn test_transactions_schema_columns() {
        let schema = transactions_schema();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"txid"));
        assert!(names.contains(&"block_height"));
        assert!(names.contains(&"fee"));
        assert!(names.contains(&"is_coinbase"));
    }

    #[test]
    fn test_inputs_schema_columns() {
        let schema = inputs_schema();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"txid"));
        assert!(names.contains(&"input_index"));
        assert!(names.contains(&"prev_txid"));
        assert!(names.contains(&"sequence"));
    }

    #[test]
    fn test_outputs_schema_columns() {
        let schema = outputs_schema();
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"txid"));
        assert!(names.contains(&"output_index"));
        assert!(names.contains(&"value_satoshis"));
        assert!(names.contains(&"address"));
    }

    #[test]
    fn test_schema_for_table() {
        assert!(schema_for_table(TABLE_BLOCKS).is_some());
        assert!(schema_for_table(TABLE_TRANSACTIONS).is_some());
        assert!(schema_for_table(TABLE_INPUTS).is_some());
        assert!(schema_for_table(TABLE_OUTPUTS).is_some());
        assert!(schema_for_table("unknown").is_none());
    }

    #[test]
    fn test_primary_key_for_table() {
        assert_eq!(primary_key_for_table(TABLE_BLOCKS), vec!["block_height"]);
        assert_eq!(primary_key_for_table(TABLE_TRANSACTIONS), vec!["txid"]);
        assert_eq!(
            primary_key_for_table(TABLE_INPUTS),
            vec!["txid", "input_index"]
        );
        assert_eq!(
            primary_key_for_table(TABLE_OUTPUTS),
            vec!["txid", "output_index"]
        );
    }

    #[test]
    fn test_all_tables_constant() {
        assert_eq!(ALL_TABLES.len(), 4);
        assert!(ALL_TABLES.contains(&TABLE_BLOCKS));
        assert!(ALL_TABLES.contains(&TABLE_TRANSACTIONS));
        assert!(ALL_TABLES.contains(&TABLE_INPUTS));
        assert!(ALL_TABLES.contains(&TABLE_OUTPUTS));
    }
}
