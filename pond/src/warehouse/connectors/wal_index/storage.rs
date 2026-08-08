//! ClickHouse Storage for WAL Indexes
//!
//! Provides persistence layer for blocks, skip indexes, and inverted indexes.

use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine as _;
use roaring::RoaringBitmap;

use super::block::{Block, BlockId};
use super::skip_index::{BlockSkipIndex, SkipIndexType};
use crate::warehouse::connectors::{ConnectorError, ConnectorResult};

/// Schema for wal_blocks table.
pub const WAL_BLOCKS_TABLE: &str = "wal_blocks";

/// Schema for wal_block_indexes table.
pub const WAL_BLOCK_INDEXES_TABLE: &str = "wal_block_indexes";

/// Schema for wal_inverted_index table.
pub const WAL_INVERTED_INDEX_TABLE: &str = "wal_inverted_index";

/// DDL for creating wal_blocks table.
pub const WAL_BLOCKS_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS wal_blocks (
    source_id String,
    table_name String,
    block_id UInt32,
    pk_start String,
    pk_end String,
    row_count UInt64,
    is_closed UInt8,
    created_at DateTime64(3)
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (source_id, table_name, block_id)
"#;

/// DDL for creating wal_block_indexes table.
pub const WAL_BLOCK_INDEXES_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS wal_block_indexes (
    source_id String,
    table_name String,
    block_id UInt32,
    column_name String,
    index_type UInt8,
    index_data String,
    cardinality_estimate UInt64,
    updated_at DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (source_id, table_name, block_id, column_name)
"#;

/// DDL for creating wal_inverted_index table.
pub const WAL_INVERTED_INDEX_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS wal_inverted_index (
    source_id String,
    table_name String,
    column_name String,
    value_hash UInt64,
    pk_bitmap String,
    updated_at DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (source_id, table_name, column_name, value_hash)
"#;

/// Block index data from storage.
#[derive(Debug, Clone)]
pub struct StoredBlockIndex {
    pub block_id: BlockId,
    pub column_name: String,
    pub index_type: SkipIndexType,
    pub index_data: String,
    pub cardinality_estimate: u64,
}

/// Inverted index entry from storage.
#[derive(Debug, Clone)]
pub struct StoredInvertedEntry {
    pub column_name: String,
    pub value_hash: u64,
    pub pk_bitmap: String,
}

/// Trait for WAL index storage operations.
#[async_trait]
pub trait WalIndexStorage: Send + Sync {
    /// Initialize storage (create tables if needed).
    async fn initialize(&self) -> ConnectorResult<()>;

    // Block operations
    
    /// Save a block definition.
    async fn save_block(&self, source_id: &str, table_name: &str, block: &Block) -> ConnectorResult<()>;

    /// Load all blocks for a table.
    async fn load_blocks(&self, source_id: &str, table_name: &str) -> ConnectorResult<Vec<Block>>;

    /// Delete blocks for a table.
    async fn delete_blocks(&self, source_id: &str, table_name: &str) -> ConnectorResult<()>;

    // Skip index operations

    /// Save a block skip index.
    async fn save_skip_index(
        &self,
        source_id: &str,
        table_name: &str,
        block_id: BlockId,
        column_name: &str,
        index: &BlockSkipIndex,
    ) -> ConnectorResult<()>;

    /// Load skip indexes for a table.
    async fn load_skip_indexes(
        &self,
        source_id: &str,
        table_name: &str,
    ) -> ConnectorResult<Vec<StoredBlockIndex>>;

    /// Load skip indexes for specific blocks.
    async fn load_skip_indexes_for_blocks(
        &self,
        source_id: &str,
        table_name: &str,
        block_ids: &[BlockId],
    ) -> ConnectorResult<Vec<StoredBlockIndex>>;

    /// Delete skip indexes for a table.
    async fn delete_skip_indexes(&self, source_id: &str, table_name: &str) -> ConnectorResult<()>;

    // Inverted index operations

    /// Save an inverted index entry.
    async fn save_inverted_entry(
        &self,
        source_id: &str,
        table_name: &str,
        column_name: &str,
        value_hash: u64,
        pk_bitmap: &RoaringBitmap,
    ) -> ConnectorResult<()>;

    /// Save multiple inverted index entries (batch).
    async fn save_inverted_entries_batch(
        &self,
        source_id: &str,
        table_name: &str,
        entries: Vec<(String, u64, RoaringBitmap)>,
    ) -> ConnectorResult<()>;

    /// Load inverted index entries for a column.
    async fn load_inverted_entries(
        &self,
        source_id: &str,
        table_name: &str,
        column_name: &str,
    ) -> ConnectorResult<Vec<StoredInvertedEntry>>;

    /// Load inverted index entry for a specific value hash.
    async fn load_inverted_entry(
        &self,
        source_id: &str,
        table_name: &str,
        column_name: &str,
        value_hash: u64,
    ) -> ConnectorResult<Option<StoredInvertedEntry>>;

    /// Delete inverted index entries for a table.
    async fn delete_inverted_entries(&self, source_id: &str, table_name: &str) -> ConnectorResult<()>;

    // Checkpoint operations (for CDC/Oplog resume)

    /// Save checkpoint (LSN or resume token).
    async fn save_checkpoint(
        &self,
        source_id: &str,
        table_name: &str,
        checkpoint: &[u8],
    ) -> ConnectorResult<()>;

    /// Load checkpoint.
    async fn load_checkpoint(
        &self,
        source_id: &str,
        table_name: &str,
    ) -> ConnectorResult<Option<Vec<u8>>>;
}

/// ClickHouse implementation of WAL index storage.
pub struct ClickHouseWalStorage {
    /// ClickHouse client
    client: Arc<dyn ClickHouseClient>,
}

/// Trait for ClickHouse operations (allows mocking in tests).
#[async_trait]
pub trait ClickHouseClient: Send + Sync {
    /// Execute a DDL statement.
    async fn execute_ddl(&self, ddl: &str) -> ConnectorResult<()>;

    /// Execute an INSERT statement.
    async fn execute_insert(&self, query: &str) -> ConnectorResult<()>;

    /// Execute a SELECT query and return rows.
    async fn execute_query(&self, query: &str) -> ConnectorResult<Vec<Vec<String>>>;

    /// Execute a DELETE statement.
    async fn execute_delete(&self, query: &str) -> ConnectorResult<()>;
}

impl ClickHouseWalStorage {
    /// Create a new ClickHouse storage.
    pub fn new(client: Arc<dyn ClickHouseClient>) -> Self {
        Self { client }
    }

    /// Escape a string for ClickHouse SQL.
    fn escape_string(s: &str) -> String {
        s.replace('\\', "\\\\").replace('\'', "\\'")
    }
}

#[async_trait]
impl WalIndexStorage for ClickHouseWalStorage {
    async fn initialize(&self) -> ConnectorResult<()> {
        self.client.execute_ddl(WAL_BLOCKS_DDL).await?;
        self.client.execute_ddl(WAL_BLOCK_INDEXES_DDL).await?;
        self.client.execute_ddl(WAL_INVERTED_INDEX_DDL).await?;
        Ok(())
    }

    async fn save_block(&self, source_id: &str, table_name: &str, block: &Block) -> ConnectorResult<()> {
        let query = format!(
            "INSERT INTO {} (source_id, table_name, block_id, pk_start, pk_end, row_count, is_closed, created_at) VALUES ('{}', '{}', {}, '{}', '{}', {}, {}, now64(3))",
            WAL_BLOCKS_TABLE,
            Self::escape_string(source_id),
            Self::escape_string(table_name),
            block.id,
            Self::escape_string(&block.pk_start),
            Self::escape_string(&block.pk_end),
            block.row_count,
            if block.is_closed { 1 } else { 0 }
        );
        self.client.execute_insert(&query).await
    }

    async fn load_blocks(&self, source_id: &str, table_name: &str) -> ConnectorResult<Vec<Block>> {
        let query = format!(
            "SELECT block_id, pk_start, pk_end, row_count, is_closed, created_at FROM {} WHERE source_id = '{}' AND table_name = '{}' ORDER BY block_id",
            WAL_BLOCKS_TABLE,
            Self::escape_string(source_id),
            Self::escape_string(table_name)
        );

        let rows = self.client.execute_query(&query).await?;
        let mut blocks = Vec::new();

        for row in rows {
            if row.len() >= 5 {
                let id: BlockId = row[0].parse().unwrap_or(0);
                let mut block = Block::new(id, &row[1]);
                block.pk_end = row[2].clone();
                block.row_count = row[3].parse().unwrap_or(0);
                block.is_closed = row[4] == "1";
                blocks.push(block);
            }
        }

        Ok(blocks)
    }

    async fn delete_blocks(&self, source_id: &str, table_name: &str) -> ConnectorResult<()> {
        let query = format!(
            "ALTER TABLE {} DELETE WHERE source_id = '{}' AND table_name = '{}'",
            WAL_BLOCKS_TABLE,
            Self::escape_string(source_id),
            Self::escape_string(table_name)
        );
        self.client.execute_delete(&query).await
    }

    async fn save_skip_index(
        &self,
        source_id: &str,
        table_name: &str,
        block_id: BlockId,
        column_name: &str,
        index: &BlockSkipIndex,
    ) -> ConnectorResult<()> {
        let query = format!(
            "INSERT INTO {} (source_id, table_name, block_id, column_name, index_type, index_data, cardinality_estimate, updated_at) VALUES ('{}', '{}', {}, '{}', {}, '{}', {}, now64(3))",
            WAL_BLOCK_INDEXES_TABLE,
            Self::escape_string(source_id),
            Self::escape_string(table_name),
            block_id,
            Self::escape_string(column_name),
            index.index_type().to_clickhouse_enum(),
            Self::escape_string(&index.to_base64()),
            index.cardinality_estimate()
        );
        self.client.execute_insert(&query).await
    }

    async fn load_skip_indexes(
        &self,
        source_id: &str,
        table_name: &str,
    ) -> ConnectorResult<Vec<StoredBlockIndex>> {
        let query = format!(
            "SELECT block_id, column_name, index_type, index_data, cardinality_estimate FROM {} WHERE source_id = '{}' AND table_name = '{}' ORDER BY block_id, column_name",
            WAL_BLOCK_INDEXES_TABLE,
            Self::escape_string(source_id),
            Self::escape_string(table_name)
        );

        let rows = self.client.execute_query(&query).await?;
        let mut indexes = Vec::new();

        for row in rows {
            if row.len() >= 5 {
                let index_type_val: u8 = row[2].parse().unwrap_or(1);
                if let Some(index_type) = SkipIndexType::from_clickhouse_enum(index_type_val) {
                    indexes.push(StoredBlockIndex {
                        block_id: row[0].parse().unwrap_or(0),
                        column_name: row[1].clone(),
                        index_type,
                        index_data: row[3].clone(),
                        cardinality_estimate: row[4].parse().unwrap_or(0),
                    });
                }
            }
        }

        Ok(indexes)
    }

    async fn load_skip_indexes_for_blocks(
        &self,
        source_id: &str,
        table_name: &str,
        block_ids: &[BlockId],
    ) -> ConnectorResult<Vec<StoredBlockIndex>> {
        if block_ids.is_empty() {
            return Ok(Vec::new());
        }

        let block_ids_str = block_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        let query = format!(
            "SELECT block_id, column_name, index_type, index_data, cardinality_estimate FROM {} WHERE source_id = '{}' AND table_name = '{}' AND block_id IN ({}) ORDER BY block_id, column_name",
            WAL_BLOCK_INDEXES_TABLE,
            Self::escape_string(source_id),
            Self::escape_string(table_name),
            block_ids_str
        );

        let rows = self.client.execute_query(&query).await?;
        let mut indexes = Vec::new();

        for row in rows {
            if row.len() >= 5 {
                let index_type_val: u8 = row[2].parse().unwrap_or(1);
                if let Some(index_type) = SkipIndexType::from_clickhouse_enum(index_type_val) {
                    indexes.push(StoredBlockIndex {
                        block_id: row[0].parse().unwrap_or(0),
                        column_name: row[1].clone(),
                        index_type,
                        index_data: row[3].clone(),
                        cardinality_estimate: row[4].parse().unwrap_or(0),
                    });
                }
            }
        }

        Ok(indexes)
    }

    async fn delete_skip_indexes(&self, source_id: &str, table_name: &str) -> ConnectorResult<()> {
        let query = format!(
            "ALTER TABLE {} DELETE WHERE source_id = '{}' AND table_name = '{}'",
            WAL_BLOCK_INDEXES_TABLE,
            Self::escape_string(source_id),
            Self::escape_string(table_name)
        );
        self.client.execute_delete(&query).await
    }

    async fn save_inverted_entry(
        &self,
        source_id: &str,
        table_name: &str,
        column_name: &str,
        value_hash: u64,
        pk_bitmap: &RoaringBitmap,
    ) -> ConnectorResult<()> {
        let mut bitmap_bytes = Vec::new();
        pk_bitmap.serialize_into(&mut bitmap_bytes).map_err(|e| {
            ConnectorError::Internal(format!("Failed to serialize bitmap: {}", e))
        })?;
        let bitmap_base64 = base64::engine::general_purpose::STANDARD.encode(&bitmap_bytes);

        let query = format!(
            "INSERT INTO {} (source_id, table_name, column_name, value_hash, pk_bitmap, updated_at) VALUES ('{}', '{}', '{}', {}, '{}', now64(3))",
            WAL_INVERTED_INDEX_TABLE,
            Self::escape_string(source_id),
            Self::escape_string(table_name),
            Self::escape_string(column_name),
            value_hash,
            Self::escape_string(&bitmap_base64)
        );
        self.client.execute_insert(&query).await
    }

    async fn save_inverted_entries_batch(
        &self,
        source_id: &str,
        table_name: &str,
        entries: Vec<(String, u64, RoaringBitmap)>,
    ) -> ConnectorResult<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut values = Vec::new();
        for (column_name, value_hash, pk_bitmap) in entries {
            let mut bitmap_bytes = Vec::new();
            pk_bitmap.serialize_into(&mut bitmap_bytes).map_err(|e| {
                ConnectorError::Internal(format!("Failed to serialize bitmap: {}", e))
            })?;
            let bitmap_base64 = base64::engine::general_purpose::STANDARD.encode(&bitmap_bytes);

            values.push(format!(
                "('{}', '{}', '{}', {}, '{}', now64(3))",
                Self::escape_string(source_id),
                Self::escape_string(table_name),
                Self::escape_string(&column_name),
                value_hash,
                Self::escape_string(&bitmap_base64)
            ));
        }

        let query = format!(
            "INSERT INTO {} (source_id, table_name, column_name, value_hash, pk_bitmap, updated_at) VALUES {}",
            WAL_INVERTED_INDEX_TABLE,
            values.join(", ")
        );
        self.client.execute_insert(&query).await
    }

    async fn load_inverted_entries(
        &self,
        source_id: &str,
        table_name: &str,
        column_name: &str,
    ) -> ConnectorResult<Vec<StoredInvertedEntry>> {
        let query = format!(
            "SELECT column_name, value_hash, pk_bitmap FROM {} WHERE source_id = '{}' AND table_name = '{}' AND column_name = '{}'",
            WAL_INVERTED_INDEX_TABLE,
            Self::escape_string(source_id),
            Self::escape_string(table_name),
            Self::escape_string(column_name)
        );

        let rows = self.client.execute_query(&query).await?;
        let mut entries = Vec::new();

        for row in rows {
            if row.len() >= 3 {
                entries.push(StoredInvertedEntry {
                    column_name: row[0].clone(),
                    value_hash: row[1].parse().unwrap_or(0),
                    pk_bitmap: row[2].clone(),
                });
            }
        }

        Ok(entries)
    }

    async fn load_inverted_entry(
        &self,
        source_id: &str,
        table_name: &str,
        column_name: &str,
        value_hash: u64,
    ) -> ConnectorResult<Option<StoredInvertedEntry>> {
        let query = format!(
            "SELECT column_name, value_hash, pk_bitmap FROM {} WHERE source_id = '{}' AND table_name = '{}' AND column_name = '{}' AND value_hash = {} LIMIT 1",
            WAL_INVERTED_INDEX_TABLE,
            Self::escape_string(source_id),
            Self::escape_string(table_name),
            Self::escape_string(column_name),
            value_hash
        );

        let rows = self.client.execute_query(&query).await?;

        if let Some(row) = rows.first() {
            if row.len() >= 3 {
                return Ok(Some(StoredInvertedEntry {
                    column_name: row[0].clone(),
                    value_hash: row[1].parse().unwrap_or(0),
                    pk_bitmap: row[2].clone(),
                }));
            }
        }

        Ok(None)
    }

    async fn delete_inverted_entries(&self, source_id: &str, table_name: &str) -> ConnectorResult<()> {
        let query = format!(
            "ALTER TABLE {} DELETE WHERE source_id = '{}' AND table_name = '{}'",
            WAL_INVERTED_INDEX_TABLE,
            Self::escape_string(source_id),
            Self::escape_string(table_name)
        );
        self.client.execute_delete(&query).await
    }

    async fn save_checkpoint(
        &self,
        _source_id: &str,
        _table_name: &str,
        _checkpoint: &[u8],
    ) -> ConnectorResult<()> {
        // This uses the existing checkpoint tables in the connectors
        // Just a passthrough for now
        Ok(())
    }

    async fn load_checkpoint(
        &self,
        _source_id: &str,
        _table_name: &str,
    ) -> ConnectorResult<Option<Vec<u8>>> {
        // This uses the existing checkpoint tables in the connectors
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct MockClickHouseClient {
        queries: Arc<Mutex<Vec<String>>>,
    }

    impl MockClickHouseClient {
        fn new() -> Self {
            Self {
                queries: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl ClickHouseClient for MockClickHouseClient {
        async fn execute_ddl(&self, ddl: &str) -> ConnectorResult<()> {
            self.queries.lock().unwrap().push(ddl.to_string());
            Ok(())
        }

        async fn execute_insert(&self, query: &str) -> ConnectorResult<()> {
            self.queries.lock().unwrap().push(query.to_string());
            Ok(())
        }

        async fn execute_query(&self, _query: &str) -> ConnectorResult<Vec<Vec<String>>> {
            Ok(Vec::new())
        }

        async fn execute_delete(&self, query: &str) -> ConnectorResult<()> {
            self.queries.lock().unwrap().push(query.to_string());
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_initialize() {
        let client = Arc::new(MockClickHouseClient::new());
        let storage = ClickHouseWalStorage::new(client.clone());

        storage.initialize().await.unwrap();

        let queries = client.queries.lock().unwrap();
        assert_eq!(queries.len(), 3);
        assert!(queries[0].contains("wal_blocks"));
        assert!(queries[1].contains("wal_block_indexes"));
        assert!(queries[2].contains("wal_inverted_index"));
    }

    #[tokio::test]
    async fn test_save_block() {
        let client = Arc::new(MockClickHouseClient::new());
        let storage = ClickHouseWalStorage::new(client.clone());

        let block = Block::new(1, "pk_001");
        storage.save_block("source", "table", &block).await.unwrap();

        let queries = client.queries.lock().unwrap();
        assert_eq!(queries.len(), 1);
        assert!(queries[0].contains("INSERT INTO wal_blocks"));
        assert!(queries[0].contains("pk_001"));
    }

    #[tokio::test]
    async fn test_escape_string() {
        assert_eq!(ClickHouseWalStorage::escape_string("test"), "test");
        assert_eq!(ClickHouseWalStorage::escape_string("te'st"), "te\\'st");
        assert_eq!(ClickHouseWalStorage::escape_string("te\\st"), "te\\\\st");
    }
}
