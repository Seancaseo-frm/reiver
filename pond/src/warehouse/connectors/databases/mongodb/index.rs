//! WAL-based Index Manager for MongoDB Collections
//!
//! Manages two-phase indexing using block skip indexes and inverted indexes
//! stored in ClickHouse. Unlike traditional replication, this stores only
//! index structures that reference document _id's in the source MongoDB.

use std::collections::HashMap;
use std::sync::Arc;

use bson::Document;

use super::schema::{extract_indexable_fields, IndexableValue, InferredSchema};
use super::utils::escape_clickhouse_string;
use crate::warehouse::connectors::wal_index::{
    Block, BlockId, BlockManager, BlockManagerConfig, BlockSkipIndex, ColumnValue,
    InvertedIndexManager, PrimaryKey, Predicate, SkipIndexBuilder, TwoPhaseQueryExecutor,
};
use crate::warehouse::connectors::{ConnectorError, ConnectorResult};

/// Trait for executing index queries against ClickHouse.
///
/// This abstraction allows the index manager to work with different
/// ClickHouse client implementations.
#[async_trait::async_trait]
pub trait IndexQueryExecutor: Send + Sync {
    /// Execute a DDL statement (CREATE TABLE, DROP TABLE, etc.)
    async fn execute_ddl(&self, sql: &str) -> ConnectorResult<()>;

    /// Execute an INSERT statement
    async fn execute_insert(&self, sql: &str) -> ConnectorResult<u64>;

    /// Execute a SELECT query and return matching IDs
    async fn query_ids(&self, sql: &str) -> ConnectorResult<Vec<String>>;

    /// Execute a SELECT query and return a single value
    async fn query_scalar(&self, sql: &str) -> ConnectorResult<Option<String>>;

    /// Check if a table exists
    async fn table_exists(&self, database: &str, table: &str) -> ConnectorResult<bool>;

    /// Execute a SELECT query and return rows
    async fn execute_query(&self, sql: &str) -> ConnectorResult<Vec<Vec<String>>>;

    /// Execute a DELETE statement
    async fn execute_delete(&self, sql: &str) -> ConnectorResult<()>;
}

/// WAL-based index manager for MongoDB collections.
///
/// Uses the two-phase indexing approach:
/// 1. Block skip indexes (MinMax, Xor filters) for coarse elimination
/// 2. Inverted indexes for low-cardinality columns
pub struct MongoDBWalIndexManager {
    /// Source identifier
    source_id: String,
    /// ClickHouse executor
    executor: Arc<dyn IndexQueryExecutor>,
    /// ClickHouse database name
    ch_database: String,
    /// Max nested depth for flattening documents
    max_nested_depth: usize,
    /// Block managers per collection
    block_managers: parking_lot::RwLock<HashMap<String, Arc<BlockManager>>>,
    /// Inverted index managers per collection
    inverted_managers: parking_lot::RwLock<HashMap<String, Arc<InvertedIndexManager>>>,
    /// Schema cache per collection
    schema_cache: parking_lot::RwLock<HashMap<String, InferredSchema>>,
}

impl MongoDBWalIndexManager {
    /// Create a new WAL index manager.
    pub fn new(
        source_id: impl Into<String>,
        executor: Arc<dyn IndexQueryExecutor>,
        ch_database: impl Into<String>,
        max_nested_depth: usize,
    ) -> Self {
        Self {
            source_id: source_id.into(),
            executor,
            ch_database: ch_database.into(),
            max_nested_depth,
            block_managers: parking_lot::RwLock::new(HashMap::new()),
            inverted_managers: parking_lot::RwLock::new(HashMap::new()),
            schema_cache: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    /// Get the source identifier.
    pub fn source_id(&self) -> &str {
        &self.source_id
    }

    /// Initialize index storage for a collection.
    pub async fn initialize_collection(
        &self,
        collection: &str,
        schema: &InferredSchema,
    ) -> ConnectorResult<()> {
        // Ensure WAL index tables exist
        self.ensure_wal_tables().await?;

        // Create block manager
        let block_manager = Arc::new(BlockManager::new(
            &self.source_id,
            collection,
            BlockManagerConfig::default(),
        ));

        // Create inverted index manager
        let inverted_manager = Arc::new(InvertedIndexManager::new(&self.source_id, collection));

        // Load existing blocks from storage
        let blocks = self.load_blocks(collection).await?;
        block_manager.load_blocks(blocks);

        // Store managers and schema
        self.block_managers.write().insert(collection.to_string(), block_manager);
        self.inverted_managers.write().insert(collection.to_string(), inverted_manager);
        self.schema_cache.write().insert(collection.to_string(), schema.clone());

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

    /// Load blocks for a collection from storage.
    async fn load_blocks(&self, collection: &str) -> ConnectorResult<Vec<Block>> {
        let query = format!(
            "SELECT block_id, pk_start, pk_end, row_count, is_closed FROM `{}`.`wal_blocks` WHERE source_id = '{}' AND table_name = '{}' ORDER BY block_id",
            self.ch_database,
            escape_clickhouse_string(&self.source_id),
            escape_clickhouse_string(collection)
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

    /// Process a document insert/update and update indexes.
    pub async fn index_document(
        &self,
        collection: &str,
        doc: &Document,
    ) -> ConnectorResult<()> {
        let block_manager = self.block_managers.read().get(collection).cloned();
        let inverted_manager = self.inverted_managers.read().get(collection).cloned();
        let _schema = self.schema_cache.read().get(collection).cloned();

        let block_manager = block_manager.ok_or_else(|| {
            ConnectorError::Config(format!("Collection '{}' not initialized", collection))
        })?;
        let inverted_manager = inverted_manager.ok_or_else(|| {
            ConnectorError::Config(format!("Collection '{}' not initialized", collection))
        })?;

        // Extract document ID
        let doc_id = doc.get_object_id("_id")
            .map(|oid| oid.to_hex())
            .unwrap_or_else(|_| String::new());

        if doc_id.is_empty() {
            return Err(ConnectorError::Validation("Document missing _id".to_string()));
        }

        let pk = PrimaryKey::from_string(&doc_id);

        // Assign to a block
        let (block_id, is_new_block) = block_manager.assign_block(&pk)?;

        // Extract indexable fields
        let fields = extract_indexable_fields(doc, self.max_nested_depth);

        // Update inverted indexes for each field
        for (field_name, value) in &fields {
            let col_value = indexable_to_column_value(value);
            
            // Only index string fields in inverted index
            if matches!(&col_value, ColumnValue::String(_)) {
                inverted_manager.add(field_name, &col_value, &pk);
            }
        }

        // If this is a new block, save it
        if is_new_block {
            if let Some(block) = block_manager.get_block(block_id) {
                self.save_block(collection, &block).await?;
            }
        }

        Ok(())
    }

    /// Process a document deletion.
    pub async fn delete_document(
        &self,
        collection: &str,
        doc_id: &str,
    ) -> ConnectorResult<()> {
        let block_manager = self.block_managers.read().get(collection).cloned();
        let _inverted_manager = self.inverted_managers.read().get(collection).cloned();

        let block_manager = block_manager.ok_or_else(|| {
            ConnectorError::Config(format!("Collection '{}' not initialized", collection))
        })?;
        
        let pk = PrimaryKey::from_string(doc_id);

        // Handle delete in block manager
        block_manager.handle_delete(&pk);

        // Note: We don't have the old document values, so we can't remove from inverted index
        // In practice, the inverted index will have stale entries that point to deleted documents
        // These are handled at query time by verifying against the source

        Ok(())
    }

    /// Build skip indexes for a block.
    pub async fn build_skip_indexes(
        &self,
        collection: &str,
        block_id: BlockId,
        column_values: &HashMap<String, Vec<ColumnValue>>,
    ) -> ConnectorResult<()> {
        let _schema = self.schema_cache.read().get(collection).cloned();

        for (col_name, values) in column_values {
            if values.is_empty() {
                continue;
            }

            // Determine if numeric or string column based on first value
            let is_numeric = values.iter().any(|v| matches!(v, ColumnValue::Int64(_) | ColumnValue::Float64(_)));

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
            self.save_skip_index(collection, block_id, col_name, &index).await?;
        }

        Ok(())
    }

    /// Save a block to ClickHouse.
    async fn save_block(&self, collection: &str, block: &Block) -> ConnectorResult<()> {
        let query = format!(
            "INSERT INTO `{}`.`wal_blocks` (source_id, table_name, block_id, pk_start, pk_end, row_count, is_closed, created_at) VALUES ('{}', '{}', {}, '{}', '{}', {}, {}, now64(3))",
            self.ch_database,
            escape_clickhouse_string(&self.source_id),
            escape_clickhouse_string(collection),
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
        collection: &str,
        block_id: BlockId,
        column_name: &str,
        index: &BlockSkipIndex,
    ) -> ConnectorResult<()> {
        let query = format!(
            "INSERT INTO `{}`.`wal_block_indexes` (source_id, table_name, block_id, column_name, index_type, index_data, cardinality_estimate, updated_at) VALUES ('{}', '{}', {}, '{}', {}, '{}', {}, now64(3))",
            self.ch_database,
            escape_clickhouse_string(&self.source_id),
            escape_clickhouse_string(collection),
            block_id,
            escape_clickhouse_string(column_name),
            index.index_type().to_clickhouse_enum(),
            escape_clickhouse_string(&index.to_base64()),
            index.cardinality_estimate()
        );
        self.executor.execute_insert(&query).await?;
        Ok(())
    }

    /// Persist inverted indexes to ClickHouse.
    pub async fn persist_inverted_indexes(&self, collection: &str) -> ConnectorResult<()> {
        let inverted_manager = self.inverted_managers.read().get(collection).cloned();
        let inverted_manager = inverted_manager.ok_or_else(|| {
            ConnectorError::Config(format!("Collection '{}' not initialized", collection))
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
                    escape_clickhouse_string(collection),
                    escape_clickhouse_string(&col_name),
                    value_hash,
                    escape_clickhouse_string(&bitmap_base64)
                );
                self.executor.execute_insert(&query).await?;
            }
        }

        Ok(())
    }

    /// Query the index to get matching document IDs.
    pub async fn query_index(
        &self,
        collection: &str,
        predicates: &[Predicate],
    ) -> ConnectorResult<Vec<PrimaryKey>> {
        let block_manager = self.block_managers.read().get(collection).cloned();
        let inverted_manager = self.inverted_managers.read().get(collection).cloned();

        let block_manager = block_manager.ok_or_else(|| {
            ConnectorError::Config(format!("Collection '{}' not initialized", collection))
        })?;
        let inverted_manager = inverted_manager.ok_or_else(|| {
            ConnectorError::Config(format!("Collection '{}' not initialized", collection))
        })?;

        // Create storage adapter
        let storage = Arc::new(ClickHouseWalStorageAdapter::new(
            self.executor.clone(),
            self.ch_database.clone(),
        ));

        // Create query executor
        let executor = TwoPhaseQueryExecutor::new(
            &self.source_id,
            collection,
            block_manager,
            inverted_manager,
            storage,
        );

        // Execute the query
        executor.execute(predicates).await
    }

    /// Get block manager for a collection.
    pub fn get_block_manager(&self, collection: &str) -> Option<Arc<BlockManager>> {
        self.block_managers.read().get(collection).cloned()
    }

    /// Get inverted index manager for a collection.
    pub fn get_inverted_manager(&self, collection: &str) -> Option<Arc<InvertedIndexManager>> {
        self.inverted_managers.read().get(collection).cloned()
    }
}

/// Convert IndexableValue to ColumnValue.
fn indexable_to_column_value(value: &IndexableValue) -> ColumnValue {
    match value {
        IndexableValue::String(s) => ColumnValue::String(s.clone()),
        IndexableValue::Int64(i) => ColumnValue::Int64(*i),
        IndexableValue::Float64(f) => ColumnValue::Float64(*f),
        IndexableValue::Boolean(b) => ColumnValue::Bool(*b),
        IndexableValue::DateTime(millis) => ColumnValue::Timestamp(*millis),
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
        Ok(())
    }

    async fn load_checkpoint(&self, _source_id: &str, _table_name: &str) -> ConnectorResult<Option<Vec<u8>>> {
        Ok(None)
    }
}

