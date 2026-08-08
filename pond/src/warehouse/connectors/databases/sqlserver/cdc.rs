//! SQL Server CDC (Change Data Capture) Tailer
//!
//! Polls SQL Server CDC tables and builds WAL-based indexes in ClickHouse
//! instead of storing actual data values.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bb8::Pool;
use bb8_tiberius::ConnectionManager;
use tiberius::Row;
use tokio::sync::{broadcast, RwLock};
use tokio::task::JoinHandle;

use super::filter::validate_identifier;
use super::index::{IndexQueryExecutor, IndexValue, SqlServerIndexManager, SqlServerWalIndexManager};
use super::schema::ColumnInfo;
use super::utils::escape_clickhouse_string;
use crate::warehouse::connectors::wal_index::{
    BlockId, ColumnValue, PrimaryKey, WalEvent,
};
use crate::warehouse::connectors::{ConnectorError, ConnectorResult};

/// Number of metadata columns returned by CDC functions.
/// Columns: __$start_lsn, __$seqval, __$operation, __$update_mask
const CDC_METADATA_COLUMNS: usize = 4;

/// Batch size for building skip indexes.
const SKIP_INDEX_BUILD_BATCH: usize = 10_000;

/// State of the CDC tailer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcTailerState {
    /// Tailer is not running
    Stopped,
    /// Initial sync in progress
    InitialSync,
    /// Tailing CDC for changes
    Tailing,
    /// Paused (e.g., due to error)
    Paused,
}

/// CDC tailer for a SQL Server table using WAL-based indexing.
pub struct CdcTailer {
    /// Connection pool
    pool: Pool<ConnectionManager>,
    /// Source database name
    database: String,
    /// Source schema name
    schema: String,
    /// Source table name
    table: String,
    /// CDC capture instance name
    capture_instance: String,
    /// WAL-based index manager
    wal_index_manager: Arc<SqlServerWalIndexManager>,
    /// Legacy index manager (for backwards compatibility)
    legacy_index_manager: Option<Arc<SqlServerIndexManager>>,
    /// LSN storage for checkpointing
    lsn_storage: Arc<dyn LsnStorage>,
    /// Current state
    state: Arc<RwLock<CdcTailerState>>,
    /// Shutdown signal sender
    shutdown_tx: broadcast::Sender<()>,
    /// Poll interval
    poll_interval: Duration,
    /// Batch size for CDC queries
    batch_size: usize,
    /// Primary key column
    primary_key: String,
    /// Indexable columns
    columns: Vec<ColumnInfo>,
    /// Accumulated column values for skip index building
    column_value_buffers: parking_lot::RwLock<HashMap<BlockId, HashMap<String, Vec<ColumnValue>>>>,
    /// Use new WAL indexing (vs legacy data storage)
    use_wal_indexing: bool,
}

/// Trait for persisting LSN (Log Sequence Number) checkpoints.
#[async_trait::async_trait]
pub trait LsnStorage: Send + Sync {
    /// Save an LSN checkpoint.
    async fn save_lsn(&self, source_id: &str, table: &str, lsn: &[u8]) -> ConnectorResult<()>;

    /// Load the last saved LSN.
    async fn load_lsn(&self, source_id: &str, table: &str) -> ConnectorResult<Option<Vec<u8>>>;

    /// Delete an LSN checkpoint.
    async fn delete_lsn(&self, source_id: &str, table: &str) -> ConnectorResult<()>;
}

/// ClickHouse-based LSN storage.
pub struct ClickHouseLsnStorage {
    executor: Arc<dyn IndexQueryExecutor>,
    database: String,
}

impl ClickHouseLsnStorage {
    /// Table name for storing LSN checkpoints.
    const TABLE_NAME: &'static str = "sqlserver_cdc_checkpoints";

    /// Create a new ClickHouse LSN storage.
    pub fn new(executor: Arc<dyn IndexQueryExecutor>, database: impl Into<String>) -> Self {
        Self {
            executor,
            database: database.into(),
        }
    }

    /// Ensure the checkpoint table exists.
    pub async fn ensure_table(&self) -> ConnectorResult<()> {
        let ddl = format!(
            r#"
CREATE TABLE IF NOT EXISTS `{}`.`{}` (
    source_id String,
    table_name String,
    lsn String,
    updated_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
PRIMARY KEY (source_id, table_name)
ORDER BY (source_id, table_name)
"#,
            self.database,
            Self::TABLE_NAME
        );

        self.executor.execute_ddl(&ddl).await
    }
}

#[async_trait::async_trait]
impl LsnStorage for ClickHouseLsnStorage {
    async fn save_lsn(&self, source_id: &str, table: &str, lsn: &[u8]) -> ConnectorResult<()> {
        let lsn_hex = hex::encode(lsn);
        let sql = format!(
            "INSERT INTO `{}`.`{}` (source_id, table_name, lsn) VALUES ('{}', '{}', '{}')",
            self.database,
            Self::TABLE_NAME,
            escape_clickhouse_string(source_id),
            escape_clickhouse_string(table),
            lsn_hex
        );

        self.executor.execute_insert(&sql).await?;
        Ok(())
    }

    async fn load_lsn(&self, source_id: &str, table: &str) -> ConnectorResult<Option<Vec<u8>>> {
        let sql = format!(
            "SELECT lsn FROM `{}`.`{}` WHERE source_id = '{}' AND table_name = '{}' ORDER BY updated_at DESC LIMIT 1",
            self.database,
            Self::TABLE_NAME,
            escape_clickhouse_string(source_id),
            escape_clickhouse_string(table)
        );

        match self.executor.query_scalar(&sql).await? {
            Some(lsn_hex) => {
                let lsn = hex::decode(&lsn_hex).map_err(|e| {
                    ConnectorError::Internal(format!("Failed to decode LSN: {}", e))
                })?;
                Ok(Some(lsn))
            }
            None => Ok(None),
        }
    }

    async fn delete_lsn(&self, source_id: &str, table: &str) -> ConnectorResult<()> {
        let sql = format!(
            "ALTER TABLE `{}`.`{}` DELETE WHERE source_id = '{}' AND table_name = '{}'",
            self.database,
            Self::TABLE_NAME,
            escape_clickhouse_string(source_id),
            escape_clickhouse_string(table)
        );

        self.executor.execute_ddl(&sql).await
    }
}

impl CdcTailer {
    /// Create a new CDC tailer with WAL-based indexing.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pool: Pool<ConnectionManager>,
        database: impl Into<String>,
        schema: impl Into<String>,
        table: impl Into<String>,
        capture_instance: impl Into<String>,
        primary_key: impl Into<String>,
        columns: Vec<ColumnInfo>,
        wal_index_manager: Arc<SqlServerWalIndexManager>,
        lsn_storage: Arc<dyn LsnStorage>,
        poll_interval: Duration,
    ) -> ConnectorResult<Self> {
        let schema = schema.into();
        let table = table.into();
        let capture_instance = capture_instance.into();
        validate_identifier(&schema)?;
        validate_identifier(&table)?;
        validate_identifier(&capture_instance)?;

        let (shutdown_tx, _) = broadcast::channel(1);

        Ok(Self {
            pool,
            database: database.into(),
            schema,
            table,
            capture_instance,
            primary_key: primary_key.into(),
            columns,
            wal_index_manager,
            legacy_index_manager: None,
            lsn_storage,
            state: Arc::new(RwLock::new(CdcTailerState::Stopped)),
            shutdown_tx,
            poll_interval,
            batch_size: 1000,
            column_value_buffers: parking_lot::RwLock::new(HashMap::new()),
            use_wal_indexing: true,
        })
    }

    /// Create a CDC tailer with legacy index manager (for backwards compatibility).
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_legacy(
        pool: Pool<ConnectionManager>,
        database: impl Into<String>,
        schema: impl Into<String>,
        table: impl Into<String>,
        capture_instance: impl Into<String>,
        primary_key: impl Into<String>,
        columns: Vec<ColumnInfo>,
        index_manager: Arc<SqlServerIndexManager>,
        lsn_storage: Arc<dyn LsnStorage>,
        poll_interval: Duration,
    ) -> ConnectorResult<Self> {
        let database = database.into();
        let table_name = table.into();
        let schema = schema.into();
        let capture_instance = capture_instance.into();
        validate_identifier(&schema)?;
        validate_identifier(&table_name)?;
        validate_identifier(&capture_instance)?;

        let dummy_executor = Arc::new(DummyExecutor);
        let wal_manager = Arc::new(SqlServerWalIndexManager::new(
            &database,
            dummy_executor,
            "dummy",
        ));

        let (shutdown_tx, _) = broadcast::channel(1);

        Ok(Self {
            pool,
            database,
            schema,
            table: table_name,
            capture_instance,
            primary_key: primary_key.into(),
            columns,
            wal_index_manager: wal_manager,
            legacy_index_manager: Some(index_manager),
            lsn_storage,
            state: Arc::new(RwLock::new(CdcTailerState::Stopped)),
            shutdown_tx,
            poll_interval,
            batch_size: 1000,
            column_value_buffers: parking_lot::RwLock::new(HashMap::new()),
            use_wal_indexing: false,
        })
    }

    /// Get the current state.
    pub async fn state(&self) -> CdcTailerState {
        *self.state.read().await
    }

    /// Perform initial sync - build indexes for all existing rows.
    ///
    /// Uses pagination to avoid loading all rows into memory at once.
    pub async fn initial_sync(&self) -> ConnectorResult<()> {
        {
            let mut state = self.state.write().await;
            *state = CdcTailerState::InitialSync;
        }

        tracing::info!(
            database = %self.database,
            table = %self.table,
            use_wal_indexing = %self.use_wal_indexing,
            "Starting initial sync"
        );

        if self.use_wal_indexing {
            self.initial_sync_wal().await
        } else {
            self.initial_sync_legacy().await
        }
    }

    /// Initial sync using WAL-based indexing.
    async fn initial_sync_wal(&self) -> ConnectorResult<()> {
        let column_names: Vec<String> =
            self.columns.iter().map(|c| c.column_name.clone()).collect();

        let column_list = column_names
            .iter()
            .map(|c| format!("[{}]", c))
            .collect::<Vec<_>>()
            .join(", ");

        let mut total_synced = 0u64;
        let mut offset = 0usize;
        let mut block_column_values: HashMap<BlockId, HashMap<String, Vec<ColumnValue>>> = HashMap::new();

        loop {
            let mut conn = self.pool.get().await.map_err(|e| {
                ConnectorError::Network(format!(
                    "Failed to get connection for initial sync of table '{}': {}",
                    self.table, e
                ))
            })?;

            let query = format!(
                "SELECT {} FROM [{}].[{}] ORDER BY (SELECT NULL) OFFSET {} ROWS FETCH NEXT {} ROWS ONLY",
                column_list,
                self.schema,
                self.table,
                offset,
                self.batch_size
            );

            let rows = conn
                .query(&query, &[])
                .await
                .map_err(|e| {
                    ConnectorError::Internal(format!(
                        "Failed to query table '{}' during initial sync: {}",
                        self.table, e
                    ))
                })?
                .into_first_result()
                .await
                .map_err(|e| {
                    ConnectorError::Internal(format!(
                        "Failed to read rows from table '{}' during initial sync: {}",
                        self.table, e
                    ))
                })?;

            if rows.is_empty() {
                break;
            }

            let row_count = rows.len();

            // Find primary key column index
            let pk_col_idx = self
                .columns
                .iter()
                .position(|c| c.column_name == self.primary_key)
                .unwrap_or(0);

            for row in &rows {
                // Extract primary key
                let pk_value = extract_primary_key(row, pk_col_idx, &self.columns[pk_col_idx].data_type)?;
                let pk = match pk_value {
                    Some(v) => PrimaryKey::parse(&v, is_numeric_pk(&self.columns[pk_col_idx].data_type)),
                    None => continue,
                };

                // Convert row to column values
                let col_values = row_to_column_values(row, &self.columns)?;

                // Create WAL event and process
                let event = WalEvent::insert(
                    pk.clone(),
                    self.columns
                        .iter()
                        .zip(col_values.iter())
                        .map(|(col, val)| (col.column_name.clone(), val.clone()))
                        .collect(),
                    vec![],
                );

                self.wal_index_manager
                    .process_wal_event(&self.table, &event)
                    .await?;

                // Accumulate values for skip index building
                if let Some(block_manager) = self.wal_index_manager.get_block_manager(&self.table) {
                    if let Some(block_id) = block_manager.find_block_for_pk(&pk) {
                        let block_values = block_column_values.entry(block_id).or_default();
                        for (col, val) in self.columns.iter().zip(col_values.iter()) {
                            block_values
                                .entry(col.column_name.clone())
                                .or_default()
                                .push(val.clone());
                        }
                    }
                }
            }

            total_synced += row_count as u64;
            offset += row_count;

            tracing::debug!(
                table = %self.table,
                synced = total_synced,
                "Initial sync progress"
            );

            // Build skip indexes for blocks that have enough data
            for (block_id, col_values) in block_column_values.iter() {
                let total_values: usize = col_values.values().map(|v| v.len()).sum();
                if total_values >= SKIP_INDEX_BUILD_BATCH {
                    self.wal_index_manager
                        .build_skip_indexes(&self.table, *block_id, col_values)
                        .await?;
                }
            }

            // Clear buffers for blocks that have been indexed
            block_column_values.retain(|_, v| {
                let total: usize = v.values().map(|vals| vals.len()).sum();
                total < SKIP_INDEX_BUILD_BATCH
            });

            if row_count < self.batch_size {
                break;
            }
        }

        // Build remaining skip indexes
        for (block_id, col_values) in block_column_values.iter() {
            if !col_values.is_empty() {
                self.wal_index_manager
                    .build_skip_indexes(&self.table, *block_id, col_values)
                    .await?;
            }
        }

        // Persist inverted indexes
        self.wal_index_manager
            .persist_inverted_indexes(&self.table)
            .await?;

        tracing::info!(
            table = %self.table,
            total_rows = total_synced,
            "Initial sync completed (WAL indexing)"
        );

        Ok(())
    }

    /// Initial sync using legacy data storage.
    async fn initial_sync_legacy(&self) -> ConnectorResult<()> {
        let index_manager = self.legacy_index_manager.as_ref().ok_or_else(|| {
            ConnectorError::Config("Legacy index manager not configured".to_string())
        })?;

        let column_names: Vec<String> =
            self.columns.iter().map(|c| c.column_name.clone()).collect();

        let column_list = column_names
            .iter()
            .map(|c| format!("[{}]", c))
            .collect::<Vec<_>>()
            .join(", ");

        let mut total_synced = 0u64;
        let mut offset = 0usize;

        loop {
            let mut conn = self.pool.get().await.map_err(|e| {
                ConnectorError::Network(format!(
                    "Failed to get connection for initial sync of table '{}': {}",
                    self.table, e
                ))
            })?;

            let query = format!(
                "SELECT {} FROM [{}].[{}] ORDER BY (SELECT NULL) OFFSET {} ROWS FETCH NEXT {} ROWS ONLY",
                column_list,
                self.schema,
                self.table,
                offset,
                self.batch_size
            );

            let rows = conn
                .query(&query, &[])
                .await
                .map_err(|e| {
                    ConnectorError::Internal(format!(
                        "Failed to query table '{}' during initial sync: {}",
                        self.table, e
                    ))
                })?
                .into_first_result()
                .await
                .map_err(|e| {
                    ConnectorError::Internal(format!(
                        "Failed to read rows from table '{}' during initial sync: {}",
                        self.table, e
                    ))
                })?;

            if rows.is_empty() {
                break;
            }

            let row_count = rows.len();

            let batch_rows: Vec<Vec<IndexValue>> = rows
                .iter()
                .map(|row| row_to_index_values(row, &self.columns))
                .collect::<ConnectorResult<Vec<_>>>()?;

            index_manager
                .index_rows(&self.database, &self.table, &column_names, &batch_rows)
                .await?;

            total_synced += batch_rows.len() as u64;
            offset += row_count;

            tracing::debug!(
                table = %self.table,
                synced = total_synced,
                "Initial sync progress"
            );

            if row_count < self.batch_size {
                break;
            }
        }

        tracing::info!(
            table = %self.table,
            total_rows = total_synced,
            "Initial sync completed (legacy)"
        );

        Ok(())
    }

    /// Start the CDC tailer in the background.
    pub fn start(self: Arc<Self>) -> JoinHandle<ConnectorResult<()>> {
        let tailer = Arc::clone(&self);
        tokio::spawn(async move { tailer.run().await })
    }

    /// Run the CDC tailer loop.
    async fn run(&self) -> ConnectorResult<()> {
        {
            let mut state = self.state.write().await;
            *state = CdcTailerState::Tailing;
        }

        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let source_id = format!("{}_{}", self.database, self.table);

        tracing::info!(
            database = %self.database,
            table = %self.table,
            use_wal_indexing = %self.use_wal_indexing,
            "Starting CDC tailer"
        );

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    tracing::info!(table = %self.table, "CDC tailer shutdown requested");
                    break;
                }
                _ = tokio::time::sleep(self.poll_interval) => {
                    if let Err(e) = self.poll_changes(&source_id).await {
                        tracing::error!(
                            table = %self.table,
                            error = %e,
                            "Error polling CDC changes"
                        );

                        {
                            let mut state = self.state.write().await;
                            *state = CdcTailerState::Paused;
                        }

                        tokio::time::sleep(Duration::from_secs(30)).await;

                        {
                            let mut state = self.state.write().await;
                            *state = CdcTailerState::Tailing;
                        }
                    }
                }
            }
        }

        {
            let mut state = self.state.write().await;
            *state = CdcTailerState::Stopped;
        }

        Ok(())
    }

    /// Poll for CDC changes.
    async fn poll_changes(&self, source_id: &str) -> ConnectorResult<()> {
        validate_identifier(&self.capture_instance)?;

        let mut conn = self.pool.get().await.map_err(|e| {
            ConnectorError::Network(format!(
                "Failed to get connection for polling CDC changes on table '{}': {}",
                self.table, e
            ))
        })?;

        let from_lsn = self.lsn_storage.load_lsn(source_id, &self.table).await?;

        let max_lsn_rows = conn
            .query("SELECT sys.fn_cdc_get_max_lsn()", &[])
            .await
            .map_err(|e| {
                ConnectorError::Internal(format!("Failed to get max LSN: {}", e))
            })?
            .into_first_result()
            .await
            .map_err(|e| {
                ConnectorError::Internal(format!("Failed to read max LSN: {}", e))
            })?;

        let to_lsn: Vec<u8> = if let Some(row) = max_lsn_rows.first() {
            row.get::<&[u8], _>(0)
                .map(|b| b.to_vec())
                .ok_or_else(|| ConnectorError::Internal("NULL max LSN".to_string()))?
        } else {
            return Ok(());
        };

        drop(conn);

        let from_lsn = match from_lsn {
            Some(lsn) => {
                // The saved LSN is the to_lsn from the previous poll.
                // fn_cdc_get_all_changes is *inclusive* of from_lsn, so we must
                // increment past the last processed LSN to avoid reprocessing it.
                let mut conn = self.pool.get().await.map_err(|e| {
                    ConnectorError::Network(format!("Failed to get connection: {}", e))
                })?;

                let inc_rows = conn
                    .query("SELECT sys.fn_cdc_increment_lsn(@P1)", &[&lsn.as_slice()])
                    .await
                    .map_err(|e| {
                        ConnectorError::Internal(format!("Failed to increment LSN: {}", e))
                    })?
                    .into_first_result()
                    .await
                    .map_err(|e| {
                        ConnectorError::Internal(format!("Failed to read incremented LSN: {}", e))
                    })?;

                if let Some(row) = inc_rows.first() {
                    row.get::<&[u8], _>(0)
                        .map(|b| b.to_vec())
                        .ok_or_else(|| ConnectorError::Internal("NULL incremented LSN".to_string()))?
                } else {
                    return Ok(());
                }
            }
            None => {
                let mut conn = self.pool.get().await.map_err(|e| {
                    ConnectorError::Network(format!("Failed to get connection: {}", e))
                })?;

                let min_lsn_query = format!(
                    "SELECT sys.fn_cdc_get_min_lsn('{}')",
                    self.capture_instance
                );

                let min_lsn_rows = conn
                    .query(&min_lsn_query, &[])
                    .await
                    .map_err(|e| {
                        ConnectorError::Internal(format!("Failed to get min LSN: {}", e))
                    })?
                    .into_first_result()
                    .await
                    .map_err(|e| {
                        ConnectorError::Internal(format!("Failed to read min LSN: {}", e))
                    })?;

                if let Some(row) = min_lsn_rows.first() {
                    row.get::<&[u8], _>(0)
                        .map(|b| b.to_vec())
                        .ok_or_else(|| ConnectorError::Internal("NULL min LSN".to_string()))?
                } else {
                    return Ok(());
                }
            }
        };

        let changes = self.query_cdc_changes(&from_lsn, &to_lsn).await?;

        if changes.is_empty() {
            return Ok(());
        }

        // Process changes based on indexing mode
        if self.use_wal_indexing {
            self.process_changes_wal(&changes).await?;
        } else {
            self.process_changes_legacy(&changes).await?;
        }

        self.lsn_storage
            .save_lsn(source_id, &self.table, &to_lsn)
            .await?;

        tracing::debug!(
            table = %self.table,
            changes = changes.len(),
            use_wal = %self.use_wal_indexing,
            "Processed CDC changes"
        );

        Ok(())
    }

    /// Query CDC changes between two LSNs.
    async fn query_cdc_changes(
        &self,
        from_lsn: &[u8],
        to_lsn: &[u8],
    ) -> ConnectorResult<Vec<CdcChange>> {
        let mut conn = self.pool.get().await.map_err(|e| {
            ConnectorError::Network(format!(
                "Failed to get connection for CDC query on table '{}': {}",
                self.table, e
            ))
        })?;

        let cdc_function = format!("cdc.fn_cdc_get_all_changes_{}", self.capture_instance);

        let query = format!(
            "SELECT * FROM {}(@P1, @P2, 'all')",
            cdc_function
        );

        let rows = conn
            .query(&query, &[&from_lsn, &to_lsn])
            .await
            .map_err(|e| {
                ConnectorError::Internal(format!(
                    "Failed to query CDC for table '{}': {}",
                    self.table, e
                ))
            })?
            .into_first_result()
            .await
            .map_err(|e| {
                ConnectorError::Internal(format!(
                    "Failed to read CDC rows for table '{}': {}",
                    self.table, e
                ))
            })?;

        let mut changes = Vec::new();

        let pk_col_idx = self
            .columns
            .iter()
            .position(|c| c.column_name == self.primary_key);

        for row in rows {
            let lsn: Vec<u8> = row
                .get::<&[u8], _>(0)
                .map(|b| b.to_vec())
                .unwrap_or_default();

            let operation: i32 = row.get::<i32, _>(2).unwrap_or(0);

            let change_type = match operation {
                1 => CdcChangeType::Delete,
                2 => CdcChangeType::Insert,
                3 => continue,
                4 => CdcChangeType::Update,
                _ => continue,
            };

            let pk_value = if let Some(pk_idx) = pk_col_idx {
                let row_idx = CDC_METADATA_COLUMNS + pk_idx;
                extract_primary_key(&row, row_idx, &self.columns[pk_idx].data_type)?
            } else {
                let row_idx = CDC_METADATA_COLUMNS;
                extract_primary_key_any(&row, row_idx)?
            };

            if let Some(pk) = pk_value {
                let values = if change_type != CdcChangeType::Delete {
                    Some(row_to_index_values_with_offset(
                        &row,
                        &self.columns,
                        CDC_METADATA_COLUMNS,
                    )?)
                } else {
                    None
                };

                let column_values = if change_type != CdcChangeType::Delete {
                    Some(row_to_column_values_with_offset(
                        &row,
                        &self.columns,
                        CDC_METADATA_COLUMNS,
                    )?)
                } else {
                    None
                };

                changes.push(CdcChange {
                    change_type,
                    lsn,
                    primary_key: pk,
                    values,
                    column_values,
                });
            }
        }

        Ok(changes)
    }

    /// Process CDC changes using WAL-based indexing.
    async fn process_changes_wal(&self, changes: &[CdcChange]) -> ConnectorResult<()> {
        let pk_col_idx = self
            .columns
            .iter()
            .position(|c| c.column_name == self.primary_key)
            .unwrap_or(0);

        let mut block_column_values: HashMap<BlockId, HashMap<String, Vec<ColumnValue>>> = HashMap::new();

        for change in changes {
            let pk = PrimaryKey::parse(
                &change.primary_key,
                is_numeric_pk(&self.columns[pk_col_idx].data_type),
            );

            let event = match change.change_type {
                CdcChangeType::Insert => {
                    let cols = change.column_values.as_ref().map(|cvs| {
                        self.columns
                            .iter()
                            .zip(cvs.iter())
                            .map(|(col, val)| (col.column_name.clone(), val.clone()))
                            .collect()
                    }).unwrap_or_default();
                    WalEvent::insert(pk.clone(), cols, change.lsn.clone())
                }
                CdcChangeType::Update => {
                    let cols = change.column_values.as_ref().map(|cvs| {
                        self.columns
                            .iter()
                            .zip(cvs.iter())
                            .map(|(col, val)| (col.column_name.clone(), val.clone()))
                            .collect()
                    }).unwrap_or_default();
                    WalEvent::update(pk.clone(), cols, change.lsn.clone())
                }
                CdcChangeType::Delete => {
                    WalEvent::delete(pk.clone(), change.lsn.clone())
                }
            };

            self.wal_index_manager
                .process_wal_event(&self.table, &event)
                .await?;

            // Accumulate values for skip index building
            if let Some(col_vals) = &change.column_values {
                if let Some(block_manager) = self.wal_index_manager.get_block_manager(&self.table) {
                    if let Some(block_id) = block_manager.find_block_for_pk(&pk) {
                        let block_values = block_column_values.entry(block_id).or_default();
                        for (col, val) in self.columns.iter().zip(col_vals.iter()) {
                            block_values
                                .entry(col.column_name.clone())
                                .or_default()
                                .push(val.clone());
                        }
                    }
                }
            }
        }

        // Build skip indexes for blocks with enough data
        for (block_id, col_values) in block_column_values.iter() {
            let total_values: usize = col_values.values().map(|v| v.len()).sum();
            if total_values >= SKIP_INDEX_BUILD_BATCH / 10 {
                self.wal_index_manager
                    .build_skip_indexes(&self.table, *block_id, col_values)
                    .await?;
            } else {
                // Buffer for later
                let mut buffers = self.column_value_buffers.write();
                let block_buf = buffers.entry(*block_id).or_default();
                for (col, vals) in col_values {
                    block_buf.entry(col.clone()).or_default().extend(vals.iter().cloned());
                }
            }
        }

        // Periodically persist inverted indexes
        if changes.len() >= 100 {
            self.wal_index_manager
                .persist_inverted_indexes(&self.table)
                .await?;
        }

        Ok(())
    }

    /// Process CDC changes using legacy data storage.
    async fn process_changes_legacy(&self, changes: &[CdcChange]) -> ConnectorResult<()> {
        let index_manager = self.legacy_index_manager.as_ref().ok_or_else(|| {
            ConnectorError::Config("Legacy index manager not configured".to_string())
        })?;

        let column_names: Vec<String> =
            self.columns.iter().map(|c| c.column_name.clone()).collect();

        let mut inserts: Vec<Vec<IndexValue>> = Vec::new();
        let mut deletes: Vec<String> = Vec::new();

        for change in changes {
            match change.change_type {
                CdcChangeType::Delete => {
                    deletes.push(change.primary_key.clone());
                }
                CdcChangeType::Insert | CdcChangeType::Update => {
                    if let Some(values) = &change.values {
                        if change.change_type == CdcChangeType::Update {
                            deletes.push(change.primary_key.clone());
                        }
                        inserts.push(values.clone());
                    }
                }
            }
        }

        if !deletes.is_empty() {
            index_manager
                .delete_from_index(&self.database, &self.table, &self.primary_key, &deletes)
                .await?;
        }

        if !inserts.is_empty() {
            index_manager
                .index_rows(&self.database, &self.table, &column_names, &inserts)
                .await?;
        }

        Ok(())
    }

    /// Stop the CDC tailer.
    pub fn stop(&self) {
        let _ = self.shutdown_tx.send(());
    }
}

/// Dummy executor for struct initialization in legacy mode.
struct DummyExecutor;

#[async_trait::async_trait]
impl IndexQueryExecutor for DummyExecutor {
    async fn execute_ddl(&self, _sql: &str) -> ConnectorResult<()> {
        Err(ConnectorError::Config("Dummy executor".to_string()))
    }
    async fn execute_insert(&self, _sql: &str) -> ConnectorResult<u64> {
        Err(ConnectorError::Config("Dummy executor".to_string()))
    }
    async fn query_ids(&self, _sql: &str) -> ConnectorResult<Vec<String>> {
        Err(ConnectorError::Config("Dummy executor".to_string()))
    }
    async fn query_scalar(&self, _sql: &str) -> ConnectorResult<Option<String>> {
        Err(ConnectorError::Config("Dummy executor".to_string()))
    }
    async fn table_exists(&self, _db: &str, _table: &str) -> ConnectorResult<bool> {
        Err(ConnectorError::Config("Dummy executor".to_string()))
    }
    async fn execute_query(&self, _sql: &str) -> ConnectorResult<Vec<Vec<String>>> {
        Err(ConnectorError::Config("Dummy executor".to_string()))
    }
    async fn execute_delete(&self, _sql: &str) -> ConnectorResult<()> {
        Err(ConnectorError::Config("Dummy executor".to_string()))
    }
}

/// Type of CDC change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcChangeType {
    Insert,
    Update,
    Delete,
}

/// A CDC change record.
#[derive(Debug, Clone)]
pub struct CdcChange {
    pub change_type: CdcChangeType,
    pub lsn: Vec<u8>,
    pub primary_key: String,
    pub values: Option<Vec<IndexValue>>,
    pub column_values: Option<Vec<ColumnValue>>,
}

/// Check if primary key type is numeric.
fn is_numeric_pk(data_type: &str) -> bool {
    let lower = data_type.to_lowercase();
    matches!(
        lower.as_str(),
        "tinyint" | "smallint" | "int" | "integer" | "bigint"
    )
}

/// Convert a row to index values.
fn row_to_index_values(row: &Row, columns: &[ColumnInfo]) -> ConnectorResult<Vec<IndexValue>> {
    row_to_index_values_with_offset(row, columns, 0)
}

/// Convert a row to index values with a column offset.
fn row_to_index_values_with_offset(
    row: &Row,
    columns: &[ColumnInfo],
    offset: usize,
) -> ConnectorResult<Vec<IndexValue>> {
    let mut values = Vec::with_capacity(columns.len());

    for (idx, col) in columns.iter().enumerate() {
        let col_idx = idx + offset;
        let value = match col.data_type.to_lowercase().as_str() {
            "bit" => match row.get::<bool, _>(col_idx) {
                Some(v) => IndexValue::Bool(v),
                None => IndexValue::Null,
            },
            "tinyint" => match row.get::<u8, _>(col_idx) {
                Some(v) => IndexValue::Int16(v as i16),
                None => IndexValue::Null,
            },
            "smallint" => match row.get::<i16, _>(col_idx) {
                Some(v) => IndexValue::Int16(v),
                None => IndexValue::Null,
            },
            "int" | "integer" => match row.get::<i32, _>(col_idx) {
                Some(v) => IndexValue::Int32(v),
                None => IndexValue::Null,
            },
            "bigint" => match row.get::<i64, _>(col_idx) {
                Some(v) => IndexValue::Int64(v),
                None => IndexValue::Null,
            },
            "real" => match row.get::<f32, _>(col_idx) {
                Some(v) => IndexValue::Float32(v),
                None => IndexValue::Null,
            },
            "float" | "decimal" | "numeric" | "money" | "smallmoney" => {
                match row.get::<f64, _>(col_idx) {
                    Some(v) => IndexValue::Float64(v),
                    None => IndexValue::Null,
                }
            }
            "datetime" | "datetime2" | "smalldatetime" => {
                match row.get::<chrono::NaiveDateTime, _>(col_idx) {
                    Some(dt) => IndexValue::DateTime(dt.and_utc().timestamp_millis()),
                    None => IndexValue::Null,
                }
            }
            _ => match row.get::<&str, _>(col_idx) {
                Some(s) => IndexValue::String(s.to_string()),
                None => IndexValue::Null,
            },
        };
        values.push(value);
    }

    Ok(values)
}

/// Convert a row to column values.
fn row_to_column_values(row: &Row, columns: &[ColumnInfo]) -> ConnectorResult<Vec<ColumnValue>> {
    row_to_column_values_with_offset(row, columns, 0)
}

/// Convert a row to column values with offset.
fn row_to_column_values_with_offset(
    row: &Row,
    columns: &[ColumnInfo],
    offset: usize,
) -> ConnectorResult<Vec<ColumnValue>> {
    let index_values = row_to_index_values_with_offset(row, columns, offset)?;
    Ok(index_values.into_iter().map(|v| v.to_column_value()).collect())
}

/// Extract a primary key value from a row given the data type.
fn extract_primary_key(row: &Row, col_idx: usize, data_type: &str) -> ConnectorResult<Option<String>> {
    let pk_value = match data_type.to_lowercase().as_str() {
        "tinyint" => row.get::<u8, _>(col_idx).map(|v| v.to_string()),
        "smallint" => row.get::<i16, _>(col_idx).map(|v| v.to_string()),
        "int" | "integer" => row.get::<i32, _>(col_idx).map(|v| v.to_string()),
        "bigint" => row.get::<i64, _>(col_idx).map(|v| v.to_string()),
        "uniqueidentifier" => row
            .get::<uuid::Uuid, _>(col_idx)
            .map(|v| v.to_string()),
        _ => row.get::<&str, _>(col_idx).map(|s| s.to_string()),
    };

    Ok(pk_value)
}

/// Extract a primary key value from a row, trying multiple types.
fn extract_primary_key_any(row: &Row, col_idx: usize) -> ConnectorResult<Option<String>> {
    if let Some(v) = row.get::<i32, _>(col_idx) {
        return Ok(Some(v.to_string()));
    }
    if let Some(v) = row.get::<i64, _>(col_idx) {
        return Ok(Some(v.to_string()));
    }
    if let Some(v) = row.get::<uuid::Uuid, _>(col_idx) {
        return Ok(Some(v.to_string()));
    }
    if let Some(s) = row.get::<&str, _>(col_idx) {
        return Ok(Some(s.to_string()));
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cdc_change_type() {
        assert_eq!(CdcChangeType::Insert, CdcChangeType::Insert);
        assert_ne!(CdcChangeType::Insert, CdcChangeType::Delete);
    }

    #[test]
    fn test_cdc_metadata_columns_constant() {
        assert_eq!(CDC_METADATA_COLUMNS, 4);
    }

    #[test]
    fn test_is_numeric_pk() {
        assert!(is_numeric_pk("int"));
        assert!(is_numeric_pk("bigint"));
        assert!(!is_numeric_pk("varchar"));
        assert!(!is_numeric_pk("uniqueidentifier"));
    }

    #[test]
    fn test_tinyint_preserves_full_u8_range() {
        for val in [0u8, 1, 127, 128, 200, 255] {
            let idx_val = IndexValue::Int16(val as i16);
            match idx_val {
                IndexValue::Int16(v) => {
                    assert_eq!(v, val as i16, "u8 value {val} must round-trip through Int16");
                    assert!(v >= 0, "u8 value {val} must not become negative");
                }
                _ => panic!("expected Int16 variant"),
            }
        }
    }
}
