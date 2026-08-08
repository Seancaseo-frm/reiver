//! MongoDB Oplog Tailer for WAL-based Index Updates
//!
//! Uses MongoDB Change Streams to tail the oplog and build
//! two-phase indexes in ClickHouse instead of storing data values.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bson::{doc, Document};
use futures::StreamExt;
use mongodb::change_stream::event::{ChangeStreamEvent, OperationType, ResumeToken};
use mongodb::options::{ChangeStreamOptions, FullDocumentType};
use mongodb::Client;
use tokio::sync::{broadcast, RwLock};
use tokio::task::JoinHandle;

use super::index::{IndexQueryExecutor, MongoDBWalIndexManager};
use super::schema::{extract_indexable_fields, infer_schema, IndexableValue};
use super::utils::escape_clickhouse_string;
use crate::warehouse::connectors::wal_index::{BlockId, ColumnValue, PrimaryKey};
use crate::warehouse::connectors::{ConnectorError, ConnectorResult};

/// Batch size for building skip indexes.
const SKIP_INDEX_BUILD_BATCH: usize = 10_000;

/// State of the oplog tailer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OplogTailerState {
    /// Tailer is not running
    Stopped,
    /// Initial sync in progress
    InitialSync,
    /// Tailing the oplog for changes
    Tailing,
    /// Paused (e.g., due to error)
    Paused,
}

/// Configuration for creating an OplogTailer.
pub struct OplogTailerConfig {
    pub database: String,
    pub collection: String,
    pub max_nested_depth: usize,
    pub batch_size: usize,
}

/// Oplog tailer for a MongoDB collection with WAL-based indexing.
pub struct OplogTailer {
    /// MongoDB client
    mongo_client: Client,
    /// Database name
    database: String,
    /// Collection to tail
    collection: String,
    /// WAL-based index manager
    wal_index_manager: Arc<MongoDBWalIndexManager>,
    /// Resume token storage
    token_storage: Arc<dyn ResumeTokenStorage>,
    /// Current state
    state: Arc<RwLock<OplogTailerState>>,
    /// Shutdown signal sender
    shutdown_tx: broadcast::Sender<()>,
    /// Max nested depth for schema inference
    max_nested_depth: usize,
    /// Batch size for initial sync
    batch_size: usize,
}

/// Trait for persisting resume tokens.
#[async_trait::async_trait]
pub trait ResumeTokenStorage: Send + Sync {
    /// Save a resume token (serialized as a hex string).
    async fn save_token(&self, source_id: &str, collection: &str, token: &ResumeToken)
        -> ConnectorResult<()>;

    /// Load the last saved resume token.
    async fn load_token(
        &self,
        source_id: &str,
        collection: &str,
    ) -> ConnectorResult<Option<ResumeToken>>;

    /// Delete a resume token.
    async fn delete_token(&self, source_id: &str, collection: &str) -> ConnectorResult<()>;
}

/// ClickHouse-based resume token storage.
pub struct ClickHouseTokenStorage {
    executor: Arc<dyn IndexQueryExecutor>,
    database: String,
}

impl ClickHouseTokenStorage {
    /// Table name for resume token storage.
    const TABLE_NAME: &'static str = "mongodb_oplog_state";

    /// Create a new ClickHouse token storage.
    pub fn new(executor: Arc<dyn IndexQueryExecutor>, database: impl Into<String>) -> Self {
        Self {
            executor,
            database: database.into(),
        }
    }

    /// Create the resume token table if it doesn't exist.
    pub async fn ensure_table(&self) -> ConnectorResult<()> {
        let ddl = format!(
            r#"CREATE TABLE IF NOT EXISTS {}.{} (
    source_id String,
    collection String,
    resume_token String,
    updated_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (source_id, collection)"#,
            self.database,
            Self::TABLE_NAME
        );

        self.executor.execute_ddl(&ddl).await
    }
}

#[async_trait::async_trait]
impl ResumeTokenStorage for ClickHouseTokenStorage {
    async fn save_token(
        &self,
        source_id: &str,
        collection: &str,
        token: &ResumeToken,
    ) -> ConnectorResult<()> {
        let token_json = serde_json::to_string(token)
            .map_err(|e| ConnectorError::Internal(format!("Failed to serialize token: {}", e)))?;

        let sql = format!(
            "INSERT INTO {}.{} (source_id, collection, resume_token) VALUES ('{}', '{}', '{}')",
            self.database,
            Self::TABLE_NAME,
            escape_clickhouse_string(source_id),
            escape_clickhouse_string(collection),
            escape_clickhouse_string(&token_json)
        );

        self.executor.execute_insert(&sql).await?;
        Ok(())
    }

    async fn load_token(
        &self,
        source_id: &str,
        collection: &str,
    ) -> ConnectorResult<Option<ResumeToken>> {
        let sql = format!(
            "SELECT resume_token FROM {}.{} FINAL WHERE source_id = '{}' AND collection = '{}'",
            self.database,
            Self::TABLE_NAME,
            escape_clickhouse_string(source_id),
            escape_clickhouse_string(collection)
        );

        match self.executor.query_scalar(&sql).await? {
            Some(token_json) => {
                let token: ResumeToken = serde_json::from_str(&token_json).map_err(|e| {
                    ConnectorError::Internal(format!("Failed to parse resume token: {}", e))
                })?;
                Ok(Some(token))
            }
            None => Ok(None),
        }
    }

    async fn delete_token(&self, source_id: &str, collection: &str) -> ConnectorResult<()> {
        let sql = format!(
            "ALTER TABLE {}.{} DELETE WHERE source_id = '{}' AND collection = '{}'",
            self.database,
            Self::TABLE_NAME,
            escape_clickhouse_string(source_id),
            escape_clickhouse_string(collection)
        );

        self.executor.execute_ddl(&sql).await
    }
}

impl OplogTailer {
    /// Create a new oplog tailer with WAL-based indexing.
    pub fn new(
        mongo_client: Client,
        config: OplogTailerConfig,
        wal_index_manager: Arc<MongoDBWalIndexManager>,
        token_storage: Arc<dyn ResumeTokenStorage>,
    ) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        Self {
            mongo_client,
            database: config.database,
            collection: config.collection,
            wal_index_manager,
            token_storage,
            state: Arc::new(RwLock::new(OplogTailerState::Stopped)),
            shutdown_tx,
            max_nested_depth: config.max_nested_depth,
            batch_size: config.batch_size,
        }
    }

    /// Get the current state of the tailer.
    pub async fn state(&self) -> OplogTailerState {
        *self.state.read().await
    }

    /// Start the oplog tailer in a background task.
    pub async fn start(&self) -> ConnectorResult<JoinHandle<()>> {
        {
            let state = self.state.read().await;
            if *state != OplogTailerState::Stopped {
                return Err(ConnectorError::Internal(
                    "Oplog tailer is already running".to_string(),
                ));
            }
        }

        // Perform initial sync first
        self.initial_sync().await?;

        // Start tailing
        let mongo_client = self.mongo_client.clone();
        let database = self.database.clone();
        let collection = self.collection.clone();
        let wal_index_manager = self.wal_index_manager.clone();
        let token_storage = self.token_storage.clone();
        let state = self.state.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        let handle = tokio::spawn(async move {
            let result = tail_oplog_wal(
                mongo_client,
                database,
                collection,
                wal_index_manager,
                token_storage,
                state.clone(),
                &mut shutdown_rx,
            )
            .await;

            if let Err(e) = result {
                tracing::error!(error = %e, "Oplog tailer error");
                *state.write().await = OplogTailerState::Paused;
            } else {
                *state.write().await = OplogTailerState::Stopped;
            }
        });

        Ok(handle)
    }

    /// Stop the oplog tailer.
    pub async fn stop(&self) {
        let _ = self.shutdown_tx.send(());
    }

    /// Perform initial sync to build indexes from the full collection.
    pub async fn initial_sync(&self) -> ConnectorResult<()> {
        *self.state.write().await = OplogTailerState::InitialSync;

        tracing::info!(
            database = %self.database,
            collection = %self.collection,
            "Starting initial sync for MongoDB collection"
        );

        self.initial_sync_wal().await
    }

    /// Initial sync using WAL-based indexing.
    async fn initial_sync_wal(&self) -> ConnectorResult<()> {
        let collection = self
            .mongo_client
            .database(&self.database)
            .collection::<Document>(&self.collection);

        // Sample documents for schema inference
        let mut sample_cursor = collection
            .find(doc! {})
            .limit(100)
            .await
            .map_err(|e| ConnectorError::Internal(format!("Failed to sample collection: {}", e)))?;

        let mut sample_docs = Vec::new();
        while let Some(result) = sample_cursor.next().await {
            if let Ok(doc) = result {
                sample_docs.push(doc);
            }
        }

        if sample_docs.is_empty() {
            tracing::warn!(
                collection = %self.collection,
                "Collection is empty, skipping initial sync"
            );
            return Ok(());
        }

        // Infer schema and initialize collection
        let schema = infer_schema(&sample_docs, self.max_nested_depth);
        self.wal_index_manager
            .initialize_collection(&self.collection, &schema)
            .await?;

        // Sync all documents in batches
        let mut cursor = collection.find(doc! {}).batch_size(self.batch_size as u32).await.map_err(|e| {
            ConnectorError::Internal(format!("Failed to query collection for sync: {}", e))
        })?;

        let mut total_docs = 0u64;
        let mut block_column_values: HashMap<BlockId, HashMap<String, Vec<ColumnValue>>> = HashMap::new();

        while let Some(result) = cursor.next().await {
            match result {
                Ok(doc) => {
                    // Index the document
                    self.wal_index_manager
                        .index_document(&self.collection, &doc)
                        .await?;

                    // Accumulate values for skip index building
                    let doc_id = doc.get_object_id("_id")
                        .map(|oid| oid.to_hex())
                        .unwrap_or_default();

                    if !doc_id.is_empty() {
                        let pk = PrimaryKey::from_string(&doc_id);
                        if let Some(block_manager) = self.wal_index_manager.get_block_manager(&self.collection) {
                            if let Some(block_id) = block_manager.find_block_for_pk(&pk) {
                                let fields = extract_indexable_fields(&doc, self.max_nested_depth);
                                let block_values = block_column_values.entry(block_id).or_default();
                                for (name, value) in fields {
                                    let col_value = indexable_to_column_value(&value);
                                    block_values.entry(name).or_default().push(col_value);
                                }
                            }
                        }
                    }

                    total_docs += 1;

                    // Build skip indexes for blocks with enough data
                    if total_docs % SKIP_INDEX_BUILD_BATCH as u64 == 0 {
                        for (block_id, col_values) in block_column_values.iter() {
                            self.wal_index_manager
                                .build_skip_indexes(&self.collection, *block_id, col_values)
                                .await?;
                        }
                        block_column_values.clear();
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        collection = %self.collection,
                        error = %e,
                        "Error reading document during initial sync"
                    );
                }
            }
        }

        // Build remaining skip indexes
        for (block_id, col_values) in block_column_values.iter() {
            if !col_values.is_empty() {
                self.wal_index_manager
                    .build_skip_indexes(&self.collection, *block_id, col_values)
                    .await?;
            }
        }

        // Persist inverted indexes
        self.wal_index_manager
            .persist_inverted_indexes(&self.collection)
            .await?;

        tracing::info!(
            database = %self.database,
            collection = %self.collection,
            documents = total_docs,
            "Initial sync completed (WAL indexing)"
        );

        Ok(())
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

/// Tail the oplog using WAL-based indexing.
async fn tail_oplog_wal(
    mongo_client: Client,
    database: String,
    collection: String,
    wal_index_manager: Arc<MongoDBWalIndexManager>,
    token_storage: Arc<dyn ResumeTokenStorage>,
    state: Arc<RwLock<OplogTailerState>>,
    shutdown_rx: &mut broadcast::Receiver<()>,
) -> ConnectorResult<()> {
    *state.write().await = OplogTailerState::Tailing;

    let coll = mongo_client
        .database(&database)
        .collection::<Document>(&collection);

    let source_id = wal_index_manager.source_id();
    let resume_token = token_storage.load_token(source_id, &collection).await?;

    let options = ChangeStreamOptions::builder()
        .full_document(Some(FullDocumentType::UpdateLookup))
        .build();

    let mut change_stream = if let Some(token) = resume_token {
        tracing::info!(
            collection = %collection,
            "Resuming change stream from saved token"
        );
        coll.watch().resume_after(token).with_options(options).await
    } else {
        coll.watch().with_options(options).await
    }
    .map_err(|e| ConnectorError::Internal(format!("Failed to open change stream: {}", e)))?;

    tracing::info!(
        database = %database,
        collection = %collection,
        "Started tailing MongoDB oplog (WAL indexing)"
    );

    let mut batch_count = 0u64;
    let token_save_interval = 100;

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                tracing::info!(collection = %collection, "Oplog tailer shutdown requested");
                break;
            }
            change = change_stream.next() => {
                match change {
                    Some(Ok(event)) => {
                        if let Err(e) = handle_change_event_wal(
                            &event,
                            &collection,
                            &wal_index_manager,
                        ).await {
                            tracing::warn!(
                                collection = %collection,
                                error = %e,
                                "Error processing change event"
                            );
                        }

                        batch_count += 1;

                        if batch_count % token_save_interval == 0 {
                            if let Err(e) = token_storage
                                .save_token(source_id, &collection, &event.id)
                                .await
                            {
                                tracing::warn!(
                                    collection = %collection,
                                    error = %e,
                                    "Failed to save resume token"
                                );
                            }
                        }

                        // Periodically persist inverted indexes
                        if batch_count % 100 == 0 {
                            if let Err(e) = wal_index_manager.persist_inverted_indexes(&collection).await {
                                tracing::warn!(
                                    collection = %collection,
                                    error = %e,
                                    "Failed to persist inverted indexes"
                                );
                            }
                        }
                    }
                    Some(Err(e)) => {
                        tracing::error!(
                            collection = %collection,
                            error = %e,
                            "Change stream error"
                        );
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                    None => {
                        tracing::warn!(
                            collection = %collection,
                            "Change stream ended unexpectedly"
                        );
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

/// Handle a change stream event using WAL-based indexing.
async fn handle_change_event_wal(
    event: &ChangeStreamEvent<Document>,
    collection: &str,
    wal_index_manager: &MongoDBWalIndexManager,
) -> ConnectorResult<()> {
    match event.operation_type {
        OperationType::Insert | OperationType::Update | OperationType::Replace => {
            if let Some(full_doc) = &event.full_document {
                wal_index_manager
                    .index_document(collection, full_doc)
                    .await?;
            }
        }
        OperationType::Delete => {
            if let Some(doc_key) = &event.document_key {
                if let Some(id) = doc_key.get_object_id("_id").ok() {
                    wal_index_manager
                        .delete_document(collection, &id.to_hex())
                        .await?;
                }
            }
        }
        OperationType::Drop => {
            tracing::warn!(
                collection = %collection,
                "Collection dropped, consider removing index"
            );
        }
        OperationType::Invalidate => {
            tracing::warn!(
                collection = %collection,
                "Change stream invalidated, restart required"
            );
            return Err(ConnectorError::Internal(
                "Change stream invalidated".to_string(),
            ));
        }
        _ => {}
    }

    Ok(())
}

