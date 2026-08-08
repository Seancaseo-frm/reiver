//! WAL-based Index Manager for SQL Server Tables
//!
//! Manages two-phase indexing using block skip indexes and inverted indexes
//! stored in ClickHouse. Unlike traditional replication, this stores only
//! index structures that reference primary keys in the source database.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};

use super::schema::{sqlserver_type_to_arrow, ColumnInfo};
use super::utils::escape_clickhouse_string;
use crate::warehouse::connectors::wal_index::{
    Block, BlockId, BlockManager, BlockManagerConfig, BlockSkipIndex, ColumnValue,
    InvertedIndexManager, PrimaryKey, Predicate, SkipIndexBuilder, TwoPhaseQueryExecutor,
    WalEvent, WalEventType,
};
use crate::warehouse::connectors::{ConnectorError, ConnectorResult};

/// Prefix for SQL Server index tables in ClickHouse.
const INDEX_TABLE_PREFIX: &str = "sqlserver_idx_";

/// Legacy ClickHouse index manager for SQL Server tables.
/// 
/// NOTE: This is maintained for backwards compatibility.
/// New code should use `SqlServerWalIndexManager` for two-phase indexing.
pub struct SqlServerIndexManager {
    /// Executor for running ClickHouse queries
    executor: Arc<dyn IndexQueryExecutor>,
    /// ClickHouse database name for index tables
    database: String,
}

/// Trait for executing ClickHouse queries.
///
/// This abstraction allows for testing without a real ClickHouse connection.
#[async_trait::async_trait]
pub trait IndexQueryExecutor: Send + Sync {
    /// Execute a DDL statement (CREATE TABLE, etc.).
    async fn execute_ddl(&self, sql: &str) -> ConnectorResult<()>;

    /// Execute an INSERT statement.
    async fn execute_insert(&self, sql: &str) -> ConnectorResult<u64>;

    /// Query for IDs matching a filter.
    async fn query_ids(&self, sql: &str) -> ConnectorResult<Vec<String>>;

    /// Query for a single scalar value.
    async fn query_scalar(&self, sql: &str) -> ConnectorResult<Option<String>>;

    /// Check if a table exists.
    async fn table_exists(&self, db: &str, table: &str) -> ConnectorResult<bool>;

    /// Execute a SELECT query and return rows.
    async fn execute_query(&self, sql: &str) -> ConnectorResult<Vec<Vec<String>>>;

    /// Execute a DELETE statement.
    async fn execute_delete(&self, sql: &str) -> ConnectorResult<()>;
}

/// WAL-based index manager for SQL Server tables.
///
/// Uses the two-phase indexing approach:
/// 1. Block skip indexes (MinMax, Xor filters) for coarse elimination
/// 2. Inverted indexes for low-cardinality columns
pub struct SqlServerWalIndexManager {
    /// Source identifier (database name)
    source_id: String,
    /// ClickHouse executor
    executor: Arc<dyn IndexQueryExecutor>,
    /// ClickHouse database name
    ch_database: String,
    /// Block managers per table
    block_managers: parking_lot::RwLock<HashMap<String, Arc<BlockManager>>>,
    /// Inverted index managers per table
    inverted_managers: parking_lot::RwLock<HashMap<String, Arc<InvertedIndexManager>>>,
    /// Column metadata per table
    column_metadata: parking_lot::RwLock<HashMap<String, Vec<ColumnInfo>>>,
    /// Primary key column per table
    primary_keys: parking_lot::RwLock<HashMap<String, String>>,
}

impl SqlServerWalIndexManager {
    /// Create a new WAL index manager.
    pub fn new(
        source_id: impl Into<String>,
        executor: Arc<dyn IndexQueryExecutor>,
        ch_database: impl Into<String>,
    ) -> Self {
        Self {
            source_id: source_id.into(),
            executor,
            ch_database: ch_database.into(),
            block_managers: parking_lot::RwLock::new(HashMap::new()),
            inverted_managers: parking_lot::RwLock::new(HashMap::new()),
            column_metadata: parking_lot::RwLock::new(HashMap::new()),
            primary_keys: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    /// Get the source identifier.
    pub fn source_id(&self) -> &str {
        &self.source_id
    }

    /// Initialize index storage for a table.
    pub async fn initialize_table(
        &self,
        table: &str,
        columns: Vec<ColumnInfo>,
        primary_key: &str,
    ) -> ConnectorResult<()> {
        // Ensure WAL index tables exist
        self.ensure_wal_tables().await?;

        // Create block manager
        let block_manager = Arc::new(BlockManager::new(
            &self.source_id,
            table,
            BlockManagerConfig::default(),
        ));

        // Create inverted index manager
        let inverted_manager = Arc::new(InvertedIndexManager::new(&self.source_id, table));

        // Load existing blocks from storage
        let blocks = self.load_blocks(table).await?;
        block_manager.load_blocks(blocks);

        // Store managers and metadata
        self.block_managers.write().insert(table.to_string(), block_manager);
        self.inverted_managers.write().insert(table.to_string(), inverted_manager);
        self.column_metadata.write().insert(table.to_string(), columns);
        self.primary_keys.write().insert(table.to_string(), primary_key.to_string());

        Ok(())
    }

    /// Ensure WAL index tables exist in ClickHouse.
    async fn ensure_wal_tables(&self) -> ConnectorResult<()> {
        // Create wal_blocks table
        let blocks_ddl = format!(
            r#"
CREATE TABLE IF NOT EXISTS `{}`.`wal_blocks` (
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
"#,
            self.ch_database
        );
        self.executor.execute_ddl(&blocks_ddl).await?;

        // Create wal_block_indexes table
        let indexes_ddl = format!(
            r#"
CREATE TABLE IF NOT EXISTS `{}`.`wal_block_indexes` (
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
"#,
            self.ch_database
        );
        self.executor.execute_ddl(&indexes_ddl).await?;

        // Create wal_inverted_index table
        let inverted_ddl = format!(
            r#"
CREATE TABLE IF NOT EXISTS `{}`.`wal_inverted_index` (
    source_id String,
    table_name String,
    column_name String,
    value_hash UInt64,
    pk_bitmap String,
    updated_at DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (source_id, table_name, column_name, value_hash)
"#,
            self.ch_database
        );
        self.executor.execute_ddl(&inverted_ddl).await?;

        Ok(())
    }

    /// Load blocks for a table from storage.
    async fn load_blocks(&self, table: &str) -> ConnectorResult<Vec<Block>> {
        let query = format!(
            "SELECT block_id, pk_start, pk_end, row_count, is_closed, created_at FROM `{}`.`wal_blocks` WHERE source_id = '{}' AND table_name = '{}' ORDER BY block_id",
            self.ch_database,
            escape_clickhouse_string(&self.source_id),
            escape_clickhouse_string(table)
        );

        let rows = self.executor.execute_query(&query).await?;
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

    /// Process a WAL event and update indexes.
    pub async fn process_wal_event(
        &self,
        table: &str,
        event: &WalEvent,
    ) -> ConnectorResult<()> {
        let block_manager = self.block_managers.read().get(table).cloned();
        let inverted_manager = self.inverted_managers.read().get(table).cloned();
        let columns = self.column_metadata.read().get(table).cloned();

        let block_manager = block_manager.ok_or_else(|| {
            ConnectorError::Config(format!("Table '{}' not initialized", table))
        })?;
        let inverted_manager = inverted_manager.ok_or_else(|| {
            ConnectorError::Config(format!("Table '{}' not initialized", table))
        })?;
        let columns = columns.ok_or_else(|| {
            ConnectorError::Config(format!("Table '{}' not initialized", table))
        })?;

        match event.event_type {
            WalEventType::Insert | WalEventType::Update => {
                // Assign to a block
                let (block_id, is_new_block) = block_manager.assign_block(&event.primary_key)?;

                // Update inverted indexes for each column
                for (col_name, value) in &event.columns {
                    if let Some(col_info) = columns.iter().find(|c| &c.column_name == col_name) {
                        // Only index low-cardinality columns in inverted index
                        if should_use_inverted_index(&col_info.data_type) {
                            inverted_manager.add(col_name, value, &event.primary_key);
                        }
                    }
                }

                // If this is a new block, save it
                if is_new_block {
                    if let Some(block) = block_manager.get_block(block_id) {
                        self.save_block(table, &block).await?;
                    }
                }
            }
            WalEventType::Delete => {
                // Handle delete - remove from inverted indexes
                if let Some(_block_id) = block_manager.handle_delete(&event.primary_key) {
                    // For deletes, we need the old values to remove from inverted index
                    // In practice, CDC provides the before image for deletes
                    for (col_name, value) in &event.columns {
                        inverted_manager.remove(col_name, value, &event.primary_key);
                    }
                }
            }
        }

        Ok(())
    }

    /// Build skip indexes for a block.
    ///
    /// This should be called when a block is closed or periodically for open blocks.
    pub async fn build_skip_indexes(
        &self,
        table: &str,
        block_id: BlockId,
        column_values: &HashMap<String, Vec<ColumnValue>>,
    ) -> ConnectorResult<()> {
        let columns = self.column_metadata.read().get(table).cloned();
        let columns = columns.ok_or_else(|| {
            ConnectorError::Config(format!("Table '{}' not initialized", table))
        })?;

        for (col_name, values) in column_values {
            if values.is_empty() {
                continue;
            }

            // Determine if numeric or string column
            let is_numeric = columns
                .iter()
                .find(|c| &c.column_name == col_name)
                .map(|c| is_numeric_type(&c.data_type))
                .unwrap_or(false);

            // Build the appropriate skip index
            let mut builder = if is_numeric {
                SkipIndexBuilder::numeric()
            } else {
                SkipIndexBuilder::string()
            };

            for value in values {
                builder.add_value(value);
            }

            let index = builder.build()?;

            // Save to ClickHouse
            self.save_skip_index(table, block_id, col_name, &index).await?;
        }

        Ok(())
    }

    /// Save a block to ClickHouse.
    async fn save_block(&self, table: &str, block: &Block) -> ConnectorResult<()> {
        let query = format!(
            "INSERT INTO `{}`.`wal_blocks` (source_id, table_name, block_id, pk_start, pk_end, row_count, is_closed, created_at) VALUES ('{}', '{}', {}, '{}', '{}', {}, {}, now64(3))",
            self.ch_database,
            escape_clickhouse_string(&self.source_id),
            escape_clickhouse_string(table),
            block.id,
            escape_clickhouse_string(&block.pk_start),
            escape_clickhouse_string(&block.pk_end),
            block.row_count,
            if block.is_closed { 1 } else { 0 }
        );
        self.executor.execute_insert(&query).await?;
        Ok(())
    }

    /// Save a skip index to ClickHouse.
    async fn save_skip_index(
        &self,
        table: &str,
        block_id: BlockId,
        column_name: &str,
        index: &BlockSkipIndex,
    ) -> ConnectorResult<()> {
        let query = format!(
            "INSERT INTO `{}`.`wal_block_indexes` (source_id, table_name, block_id, column_name, index_type, index_data, cardinality_estimate, updated_at) VALUES ('{}', '{}', {}, '{}', {}, '{}', {}, now64(3))",
            self.ch_database,
            escape_clickhouse_string(&self.source_id),
            escape_clickhouse_string(table),
            block_id,
            escape_clickhouse_string(column_name),
            index.index_type().to_clickhouse_enum(),
            escape_clickhouse_string(&index.to_base64()),
            index.cardinality_estimate()
        );
        self.executor.execute_insert(&query).await?;
        Ok(())
    }

    /// Save inverted index entries to ClickHouse.
    pub async fn persist_inverted_indexes(&self, table: &str) -> ConnectorResult<()> {
        let inverted_manager = self.inverted_managers.read().get(table).cloned();
        let inverted_manager = inverted_manager.ok_or_else(|| {
            ConnectorError::Config(format!("Table '{}' not initialized", table))
        })?;

        let all_indexes = inverted_manager.all_indexes();

        for (col_name, index) in all_indexes {
            for (value_hash, entry) in index.entries() {
                let bitmap_bytes = entry.bitmap_to_bytes();
                let bitmap_base64 = base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    &bitmap_bytes,
                );

                let query = format!(
                    "INSERT INTO `{}`.`wal_inverted_index` (source_id, table_name, column_name, value_hash, pk_bitmap, updated_at) VALUES ('{}', '{}', '{}', {}, '{}', now64(3))",
                    self.ch_database,
                    escape_clickhouse_string(&self.source_id),
                    escape_clickhouse_string(table),
                    escape_clickhouse_string(&col_name),
                    value_hash,
                    escape_clickhouse_string(&bitmap_base64)
                );
                self.executor.execute_insert(&query).await?;
            }
        }

        Ok(())
    }

    /// Query the index to get matching primary keys.
    pub async fn query_index(
        &self,
        table: &str,
        predicates: &[Predicate],
    ) -> ConnectorResult<Vec<PrimaryKey>> {
        let block_manager = self.block_managers.read().get(table).cloned();
        let inverted_manager = self.inverted_managers.read().get(table).cloned();

        let block_manager = block_manager.ok_or_else(|| {
            ConnectorError::Config(format!("Table '{}' not initialized", table))
        })?;
        let inverted_manager = inverted_manager.ok_or_else(|| {
            ConnectorError::Config(format!("Table '{}' not initialized", table))
        })?;

        // Create storage adapter
        let storage = Arc::new(ClickHouseWalStorageAdapter::new(
            self.executor.clone(),
            self.ch_database.clone(),
        ));

        // Create query executor
        let executor = TwoPhaseQueryExecutor::new(
            &self.source_id,
            table,
            block_manager,
            inverted_manager,
            storage,
        );

        // Execute the query
        executor.execute(predicates).await
    }

    /// Get block manager for a table.
    pub fn get_block_manager(&self, table: &str) -> Option<Arc<BlockManager>> {
        self.block_managers.read().get(table).cloned()
    }

    /// Get inverted index manager for a table.
    pub fn get_inverted_manager(&self, table: &str) -> Option<Arc<InvertedIndexManager>> {
        self.inverted_managers.read().get(table).cloned()
    }
}

/// ClickHouse storage adapter for WalIndexStorage trait.
struct ClickHouseWalStorageAdapter {
    executor: Arc<dyn IndexQueryExecutor>,
    database: String,
}

impl ClickHouseWalStorageAdapter {
    fn new(executor: Arc<dyn IndexQueryExecutor>, database: impl Into<String>) -> Self {
        Self {
            executor,
            database: database.into(),
        }
    }
}

#[async_trait::async_trait]
impl crate::warehouse::connectors::wal_index::storage::WalIndexStorage for ClickHouseWalStorageAdapter {
    async fn initialize(&self) -> ConnectorResult<()> {
        // Tables are created by SqlServerWalIndexManager
        Ok(())
    }

    async fn save_block(&self, source_id: &str, table_name: &str, block: &Block) -> ConnectorResult<()> {
        let query = format!(
            "INSERT INTO `{}`.`wal_blocks` (source_id, table_name, block_id, pk_start, pk_end, row_count, is_closed, created_at) VALUES ('{}', '{}', {}, '{}', '{}', {}, {}, now64(3))",
            self.database,
            escape_clickhouse_string(source_id),
            escape_clickhouse_string(table_name),
            block.id,
            escape_clickhouse_string(&block.pk_start),
            escape_clickhouse_string(&block.pk_end),
            block.row_count,
            if block.is_closed { 1 } else { 0 }
        );
        self.executor.execute_insert(&query).await?;
        Ok(())
    }

    async fn load_blocks(&self, source_id: &str, table_name: &str) -> ConnectorResult<Vec<Block>> {
        let query = format!(
            "SELECT block_id, pk_start, pk_end, row_count, is_closed FROM `{}`.`wal_blocks` WHERE source_id = '{}' AND table_name = '{}' ORDER BY block_id",
            self.database,
            escape_clickhouse_string(source_id),
            escape_clickhouse_string(table_name)
        );

        let rows = self.executor.execute_query(&query).await?;
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
            "ALTER TABLE `{}`.`wal_blocks` DELETE WHERE source_id = '{}' AND table_name = '{}'",
            self.database,
            escape_clickhouse_string(source_id),
            escape_clickhouse_string(table_name)
        );
        self.executor.execute_delete(&query).await
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
            "INSERT INTO `{}`.`wal_block_indexes` (source_id, table_name, block_id, column_name, index_type, index_data, cardinality_estimate, updated_at) VALUES ('{}', '{}', {}, '{}', {}, '{}', {}, now64(3))",
            self.database,
            escape_clickhouse_string(source_id),
            escape_clickhouse_string(table_name),
            block_id,
            escape_clickhouse_string(column_name),
            index.index_type().to_clickhouse_enum(),
            escape_clickhouse_string(&index.to_base64()),
            index.cardinality_estimate()
        );
        self.executor.execute_insert(&query).await?;
        Ok(())
    }

    async fn load_skip_indexes(
        &self,
        source_id: &str,
        table_name: &str,
    ) -> ConnectorResult<Vec<crate::warehouse::connectors::wal_index::storage::StoredBlockIndex>> {
        let query = format!(
            "SELECT block_id, column_name, index_type, index_data, cardinality_estimate FROM `{}`.`wal_block_indexes` WHERE source_id = '{}' AND table_name = '{}' ORDER BY block_id, column_name",
            self.database,
            escape_clickhouse_string(source_id),
            escape_clickhouse_string(table_name)
        );

        let rows = self.executor.execute_query(&query).await?;
        let mut indexes = Vec::new();

        for row in rows {
            if row.len() >= 5 {
                let index_type_val: u8 = row[2].parse().unwrap_or(1);
                if let Some(index_type) = crate::warehouse::connectors::wal_index::skip_index::SkipIndexType::from_clickhouse_enum(index_type_val) {
                    indexes.push(crate::warehouse::connectors::wal_index::storage::StoredBlockIndex {
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
    ) -> ConnectorResult<Vec<crate::warehouse::connectors::wal_index::storage::StoredBlockIndex>> {
        if block_ids.is_empty() {
            return Ok(Vec::new());
        }

        let block_ids_str = block_ids.iter().map(|id| id.to_string()).collect::<Vec<_>>().join(", ");

        let query = format!(
            "SELECT block_id, column_name, index_type, index_data, cardinality_estimate FROM `{}`.`wal_block_indexes` WHERE source_id = '{}' AND table_name = '{}' AND block_id IN ({}) ORDER BY block_id, column_name",
            self.database,
            escape_clickhouse_string(source_id),
            escape_clickhouse_string(table_name),
            block_ids_str
        );

        let rows = self.executor.execute_query(&query).await?;
        let mut indexes = Vec::new();

        for row in rows {
            if row.len() >= 5 {
                let index_type_val: u8 = row[2].parse().unwrap_or(1);
                if let Some(index_type) = crate::warehouse::connectors::wal_index::skip_index::SkipIndexType::from_clickhouse_enum(index_type_val) {
                    indexes.push(crate::warehouse::connectors::wal_index::storage::StoredBlockIndex {
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
            "ALTER TABLE `{}`.`wal_block_indexes` DELETE WHERE source_id = '{}' AND table_name = '{}'",
            self.database,
            escape_clickhouse_string(source_id),
            escape_clickhouse_string(table_name)
        );
        self.executor.execute_delete(&query).await
    }

    async fn save_inverted_entry(
        &self,
        source_id: &str,
        table_name: &str,
        column_name: &str,
        value_hash: u64,
        pk_bitmap: &roaring::RoaringBitmap,
    ) -> ConnectorResult<()> {
        let mut bitmap_bytes = Vec::new();
        pk_bitmap.serialize_into(&mut bitmap_bytes).map_err(|e| {
            ConnectorError::Internal(format!("Failed to serialize bitmap: {}", e))
        })?;
        let bitmap_base64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bitmap_bytes);

        let query = format!(
            "INSERT INTO `{}`.`wal_inverted_index` (source_id, table_name, column_name, value_hash, pk_bitmap, updated_at) VALUES ('{}', '{}', '{}', {}, '{}', now64(3))",
            self.database,
            escape_clickhouse_string(source_id),
            escape_clickhouse_string(table_name),
            escape_clickhouse_string(column_name),
            value_hash,
            escape_clickhouse_string(&bitmap_base64)
        );
        self.executor.execute_insert(&query).await?;
        Ok(())
    }

    async fn save_inverted_entries_batch(
        &self,
        source_id: &str,
        table_name: &str,
        entries: Vec<(String, u64, roaring::RoaringBitmap)>,
    ) -> ConnectorResult<()> {
        for (column_name, value_hash, pk_bitmap) in entries {
            self.save_inverted_entry(source_id, table_name, &column_name, value_hash, &pk_bitmap).await?;
        }
        Ok(())
    }

    async fn load_inverted_entries(
        &self,
        source_id: &str,
        table_name: &str,
        column_name: &str,
    ) -> ConnectorResult<Vec<crate::warehouse::connectors::wal_index::storage::StoredInvertedEntry>> {
        let query = format!(
            "SELECT column_name, value_hash, pk_bitmap FROM `{}`.`wal_inverted_index` WHERE source_id = '{}' AND table_name = '{}' AND column_name = '{}'",
            self.database,
            escape_clickhouse_string(source_id),
            escape_clickhouse_string(table_name),
            escape_clickhouse_string(column_name)
        );

        let rows = self.executor.execute_query(&query).await?;
        let mut entries = Vec::new();

        for row in rows {
            if row.len() >= 3 {
                entries.push(crate::warehouse::connectors::wal_index::storage::StoredInvertedEntry {
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
    ) -> ConnectorResult<Option<crate::warehouse::connectors::wal_index::storage::StoredInvertedEntry>> {
        let query = format!(
            "SELECT column_name, value_hash, pk_bitmap FROM `{}`.`wal_inverted_index` WHERE source_id = '{}' AND table_name = '{}' AND column_name = '{}' AND value_hash = {} LIMIT 1",
            self.database,
            escape_clickhouse_string(source_id),
            escape_clickhouse_string(table_name),
            escape_clickhouse_string(column_name),
            value_hash
        );

        let rows = self.executor.execute_query(&query).await?;

        if let Some(row) = rows.first() {
            if row.len() >= 3 {
                return Ok(Some(crate::warehouse::connectors::wal_index::storage::StoredInvertedEntry {
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
            "ALTER TABLE `{}`.`wal_inverted_index` DELETE WHERE source_id = '{}' AND table_name = '{}'",
            self.database,
            escape_clickhouse_string(source_id),
            escape_clickhouse_string(table_name)
        );
        self.executor.execute_delete(&query).await
    }

    async fn save_checkpoint(&self, _source_id: &str, _table_name: &str, _checkpoint: &[u8]) -> ConnectorResult<()> {
        // Checkpointing is handled by the CDC tailer
        Ok(())
    }

    async fn load_checkpoint(&self, _source_id: &str, _table_name: &str) -> ConnectorResult<Option<Vec<u8>>> {
        // Checkpointing is handled by the CDC tailer
        Ok(None)
    }
}

/// Check if a column type should use inverted index.
fn should_use_inverted_index(data_type: &str) -> bool {
    let lower = data_type.to_lowercase();
    // String types with typically low cardinality
    matches!(
        lower.as_str(),
        "char" | "varchar" | "nchar" | "nvarchar" | "bit"
    )
}

/// Check if a column type is numeric (for MinMax indexes).
fn is_numeric_type(data_type: &str) -> bool {
    let lower = data_type.to_lowercase();
    matches!(
        lower.as_str(),
        "tinyint" | "smallint" | "int" | "integer" | "bigint" 
        | "real" | "float" | "decimal" | "numeric" | "money" | "smallmoney"
    )
}

// ============================================================================
// Legacy SqlServerIndexManager implementation (for backwards compatibility)
// ============================================================================

impl SqlServerIndexManager {
    /// Create a new index manager.
    pub fn new(executor: Arc<dyn IndexQueryExecutor>, database: impl Into<String>) -> Self {
        Self {
            executor,
            database: database.into(),
        }
    }

    /// Get the index table name for a SQL Server table.
    pub fn index_table_name(&self, source_database: &str, table: &str) -> String {
        format!(
            "{}{}__{}",
            INDEX_TABLE_PREFIX,
            sanitize_identifier(source_database),
            sanitize_identifier(table)
        )
    }

    /// Create an index table in ClickHouse.
    pub async fn create_index_table(
        &self,
        source_database: &str,
        table: &str,
        columns: &[ColumnInfo],
        primary_key: &str,
    ) -> ConnectorResult<()> {
        let table_name = self.index_table_name(source_database, table);

        let column_defs: Vec<String> = columns
            .iter()
            .filter(|c| is_indexable_type(&c.data_type))
            .map(|c| {
                let ch_type = arrow_to_clickhouse_type(&sqlserver_type_to_arrow(&c.data_type));
                let nullable = if c.is_nullable {
                    format!("Nullable({})", ch_type)
                } else {
                    ch_type
                };
                format!("`{}` {}", c.column_name, nullable)
            })
            .collect();

        if column_defs.is_empty() {
            return Err(ConnectorError::Config(
                "No indexable columns found".to_string(),
            ));
        }

        let mut all_columns = column_defs;
        all_columns.push("`_dh_synced_at` DateTime64(3) DEFAULT now64(3)".to_string());

        let ddl = format!(
            r#"
CREATE TABLE IF NOT EXISTS `{}`.`{}` (
    {}
)
ENGINE = ReplacingMergeTree(_dh_synced_at)
PRIMARY KEY (`{}`)
ORDER BY (`{}`)
"#,
            self.database,
            table_name,
            all_columns.join(",\n    "),
            primary_key,
            primary_key
        );

        self.executor.execute_ddl(&ddl).await
    }

    /// Query the index for IDs matching a SQL filter.
    pub async fn query_index(
        &self,
        source_database: &str,
        table: &str,
        id_column: &str,
        sql_filter: Option<&str>,
        limit: Option<usize>,
    ) -> ConnectorResult<Vec<String>> {
        let table_name = self.index_table_name(source_database, table);

        super::filter::validate_column_name(id_column)?;

        if let Some(filter) = sql_filter {
            super::filter::validate_sql_filter(filter)?;
        }

        let mut query = format!(
            "SELECT DISTINCT `{}` FROM `{}`.`{}`",
            id_column, self.database, table_name
        );

        if let Some(filter) = sql_filter {
            query.push_str(&format!(" WHERE {}", filter));
        }

        if let Some(lim) = limit {
            query.push_str(&format!(" LIMIT {}", lim));
        }

        self.executor.query_ids(&query).await
    }

    /// Insert rows into the index table.
    pub async fn index_rows(
        &self,
        source_database: &str,
        table: &str,
        columns: &[String],
        rows: &[Vec<IndexValue>],
    ) -> ConnectorResult<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        let table_name = self.index_table_name(source_database, table);
        const BATCH_SIZE: usize = 1000;
        let mut total_inserted = 0u64;

        for chunk in rows.chunks(BATCH_SIZE) {
            let column_list = columns
                .iter()
                .map(|c| format!("`{}`", c))
                .collect::<Vec<_>>()
                .join(", ");

            let value_rows: Vec<String> = chunk
                .iter()
                .map(|row| {
                    let values: Vec<String> = row.iter().map(|v| v.to_sql()).collect();
                    format!("({})", values.join(", "))
                })
                .collect();

            let sql = format!(
                "INSERT INTO `{}`.`{}` ({}) VALUES {}",
                self.database,
                table_name,
                column_list,
                value_rows.join(", ")
            );

            total_inserted += self.executor.execute_insert(&sql).await?;
        }

        Ok(total_inserted)
    }

    /// Delete rows from the index by IDs.
    pub async fn delete_from_index(
        &self,
        source_database: &str,
        table: &str,
        id_column: &str,
        ids: &[String],
    ) -> ConnectorResult<()> {
        if ids.is_empty() {
            return Ok(());
        }

        super::filter::validate_column_name(id_column)?;

        let table_name = self.index_table_name(source_database, table);
        const BATCH_SIZE: usize = 1000;

        for chunk in ids.chunks(BATCH_SIZE) {
            let id_list: Vec<String> = chunk
                .iter()
                .map(|id| format!("'{}'", escape_clickhouse_string(id)))
                .collect();

            let sql = format!(
                "ALTER TABLE `{}`.`{}` DELETE WHERE `{}` IN ({})",
                self.database,
                table_name,
                id_column,
                id_list.join(", ")
            );

            self.executor.execute_ddl(&sql).await?;
        }

        Ok(())
    }

    /// Check if an index table exists.
    pub async fn index_exists(
        &self,
        source_database: &str,
        table: &str,
    ) -> ConnectorResult<bool> {
        let table_name = self.index_table_name(source_database, table);
        self.executor.table_exists(&self.database, &table_name).await
    }

    /// Drop an index table.
    pub async fn drop_index(
        &self,
        source_database: &str,
        table: &str,
    ) -> ConnectorResult<()> {
        let table_name = self.index_table_name(source_database, table);
        let sql = format!("DROP TABLE IF EXISTS `{}`.`{}`", self.database, table_name);
        self.executor.execute_ddl(&sql).await
    }
}

/// Value types for index operations (legacy).
#[derive(Debug, Clone)]
pub enum IndexValue {
    Null,
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
    DateTime(i64),
}

impl IndexValue {
    /// Convert to SQL literal for ClickHouse.
    pub fn to_sql(&self) -> String {
        match self {
            IndexValue::Null => "NULL".to_string(),
            IndexValue::Bool(b) => if *b { "1" } else { "0" }.to_string(),
            IndexValue::Int8(v) => v.to_string(),
            IndexValue::Int16(v) => v.to_string(),
            IndexValue::Int32(v) => v.to_string(),
            IndexValue::Int64(v) => v.to_string(),
            IndexValue::Float32(v) => v.to_string(),
            IndexValue::Float64(v) => v.to_string(),
            IndexValue::String(s) => format!("'{}'", escape_clickhouse_string(s)),
            IndexValue::DateTime(ms) => {
                format!("toDateTime64({} / 1000, 3)", ms)
            }
        }
    }

    /// Convert to ColumnValue for WAL indexing.
    pub fn to_column_value(&self) -> ColumnValue {
        match self {
            IndexValue::Null => ColumnValue::Null,
            IndexValue::Bool(b) => ColumnValue::Bool(*b),
            IndexValue::Int8(v) => ColumnValue::Int64(*v as i64),
            IndexValue::Int16(v) => ColumnValue::Int64(*v as i64),
            IndexValue::Int32(v) => ColumnValue::Int64(*v as i64),
            IndexValue::Int64(v) => ColumnValue::Int64(*v),
            IndexValue::Float32(v) => ColumnValue::Float64(*v as f64),
            IndexValue::Float64(v) => ColumnValue::Float64(*v),
            IndexValue::String(s) => ColumnValue::String(s.clone()),
            IndexValue::DateTime(ms) => ColumnValue::Timestamp(*ms),
        }
    }
}

/// Check if a SQL Server type should be indexed.
fn is_indexable_type(data_type: &str) -> bool {
    let lower = data_type.to_lowercase();
    matches!(
        lower.as_str(),
        "tinyint" | "smallint" | "int" | "integer" | "bigint"
            | "real" | "float" | "decimal" | "numeric" | "money" | "smallmoney"
            | "bit" | "char" | "varchar" | "nchar" | "nvarchar"
            | "date" | "datetime" | "datetime2" | "smalldatetime"
            | "uniqueidentifier"
    )
}

/// Convert Arrow DataType to ClickHouse type string.
fn arrow_to_clickhouse_type(arrow_type: &DataType) -> String {
    match arrow_type {
        DataType::Boolean => "UInt8".to_string(),
        DataType::Int8 => "Int8".to_string(),
        DataType::Int16 => "Int16".to_string(),
        DataType::Int32 => "Int32".to_string(),
        DataType::Int64 => "Int64".to_string(),
        DataType::UInt8 => "UInt8".to_string(),
        DataType::UInt16 => "UInt16".to_string(),
        DataType::UInt32 => "UInt32".to_string(),
        DataType::UInt64 => "UInt64".to_string(),
        DataType::Float32 => "Float32".to_string(),
        DataType::Float64 => "Float64".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 => "String".to_string(),
        DataType::Timestamp(TimeUnit::Millisecond, _) => "DateTime64(3)".to_string(),
        DataType::Timestamp(TimeUnit::Microsecond, _) => "DateTime64(6)".to_string(),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => "DateTime64(9)".to_string(),
        DataType::Timestamp(TimeUnit::Second, _) => "DateTime".to_string(),
        DataType::Date32 | DataType::Date64 => "Date".to_string(),
        _ => "String".to_string(),
    }
}

/// Sanitize an identifier for use in table names.
fn sanitize_identifier(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockExecutor;

    #[async_trait::async_trait]
    impl IndexQueryExecutor for MockExecutor {
        async fn execute_ddl(&self, _sql: &str) -> ConnectorResult<()> {
            Ok(())
        }

        async fn execute_insert(&self, _sql: &str) -> ConnectorResult<u64> {
            Ok(0)
        }

        async fn query_ids(&self, _sql: &str) -> ConnectorResult<Vec<String>> {
            Ok(vec![])
        }

        async fn query_scalar(&self, _sql: &str) -> ConnectorResult<Option<String>> {
            Ok(None)
        }

        async fn table_exists(&self, _db: &str, _table: &str) -> ConnectorResult<bool> {
            Ok(false)
        }

        async fn execute_query(&self, _sql: &str) -> ConnectorResult<Vec<Vec<String>>> {
            Ok(vec![])
        }

        async fn execute_delete(&self, _sql: &str) -> ConnectorResult<()> {
            Ok(())
        }
    }

    #[test]
    fn test_index_table_name() {
        let manager = SqlServerIndexManager::new(
            Arc::new(MockExecutor),
            "reiver_indexes",
        );

        assert_eq!(
            manager.index_table_name("mydb", "users"),
            "sqlserver_idx_mydb__users"
        );

        assert_eq!(
            manager.index_table_name("my-db", "user-table"),
            "sqlserver_idx_my_db__user_table"
        );
    }

    #[test]
    fn test_is_indexable_type() {
        assert!(is_indexable_type("int"));
        assert!(is_indexable_type("bigint"));
        assert!(is_indexable_type("varchar"));
        assert!(is_indexable_type("datetime"));
        assert!(is_indexable_type("uniqueidentifier"));

        assert!(!is_indexable_type("text"));
        assert!(!is_indexable_type("image"));
        assert!(!is_indexable_type("xml"));
    }

    #[test]
    fn test_is_numeric_type() {
        assert!(is_numeric_type("int"));
        assert!(is_numeric_type("bigint"));
        assert!(is_numeric_type("decimal"));
        assert!(!is_numeric_type("varchar"));
        assert!(!is_numeric_type("datetime"));
    }

    #[test]
    fn test_should_use_inverted_index() {
        assert!(should_use_inverted_index("varchar"));
        assert!(should_use_inverted_index("nvarchar"));
        assert!(should_use_inverted_index("bit"));
        assert!(!should_use_inverted_index("int"));
        assert!(!should_use_inverted_index("datetime"));
    }

    #[test]
    fn test_index_value_to_column_value() {
        let int_val = IndexValue::Int64(42);
        assert!(matches!(int_val.to_column_value(), ColumnValue::Int64(42)));

        let str_val = IndexValue::String("test".to_string());
        assert!(matches!(str_val.to_column_value(), ColumnValue::String(s) if s == "test"));
    }

    #[tokio::test]
    async fn test_wal_index_manager_creation() {
        let executor = Arc::new(MockExecutor);
        let manager = SqlServerWalIndexManager::new("source_db", executor, "ch_db");

        assert_eq!(manager.source_id(), "source_db");
    }
}
