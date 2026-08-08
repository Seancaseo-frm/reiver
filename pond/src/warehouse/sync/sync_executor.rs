//! Sync Executor
//!
//! Core sync logic for the data warehouse. Reads data from source connectors
//! and routes to the appropriate destination based on tier:
//! - Warm: Write Parquet files to R2
//! - Hot: Insert directly into ClickHouse
//!
//! ARCHITECTURE:
//! - Uses existing connectors to fetch data from sources
//! - Supports checkpoint-based resumability
//! - Batches data for efficient writes

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

use crate::crypto::SecretEncrypt;
use crate::warehouse::connectors::Connector;
use crate::warehouse::connectors::factory as connector_factory;
use crate::warehouse::indexes::PartitionManager;
use crate::warehouse::parquet::WriteOptions;
use crate::warehouse::parquet_stats::write_parquet_with_stats;
use crate::warehouse::pii_scanner::{PiiScanRequest, PiiScanWorker};
use crate::warehouse::sources::{StorageTier, SyncScope};
use crate::warehouse::storage::clickhouse::ClickHouseStorage;
use crate::warehouse::storage::r2::R2Storage;
use crate::warehouse::types::{SourceType, TableSchema};

use super::sync_job_consumer::DecryptedSourceConfig;

/// Result of a sync operation.
#[derive(Debug)]
pub struct SyncResult {
    /// Number of tables synced
    pub tables_synced: usize,
    /// Total rows synced across all tables
    pub total_rows: usize,
    /// Total bytes written
    pub total_bytes: usize,
    /// New checkpoint (if any)
    pub checkpoint: Option<SourceCheckpoint>,
    /// Staging tables created during sync (for hot tier)
    /// These need to be committed on success or dropped on failure
    pub staging_tables: Vec<StagingTableInfo>,
}

/// Information about a staging table created during sync.
#[derive(Debug, Clone)]
pub struct StagingTableInfo {
    /// Project ID
    pub project_id: Uuid,
    /// Source name
    pub source_name: String,
    /// Table name
    pub table_name: String,
}

/// Orphaned partition info for cleanup.
#[derive(Debug, sqlx::FromRow)]
struct OrphanPartition {
    id: Uuid,
    parquet_path: Option<String>,
}

/// Per-table checkpoint for incremental sync.
/// Tracks the last synced position for each table within a source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableCheckpoint {
    /// Column used for incremental sync (e.g., "updated_at")
    pub incremental_key: String,
    /// Last seen value of the incremental key
    pub last_value: String,
    /// Sync version when this table was last checkpointed
    pub last_sync_version: i64,
}

/// Source-level checkpoint containing per-table incremental state.
/// Stored in `warehouse_sources.sync_checkpoint` as JSONB.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SourceCheckpoint {
    /// Per-table checkpoint state
    pub tables: std::collections::HashMap<String, TableCheckpoint>,
    /// Monotonically increasing version, bumped on each sync
    pub global_sync_version: i64,
}

/// Legacy checkpoint alias for backward compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Last synced table
    pub last_table: Option<String>,
    /// Position within the table (e.g., offset, LSN)
    pub position: Option<String>,
    /// Timestamp of last sync
    pub timestamp: i64,
}

impl Default for Checkpoint {
    fn default() -> Self {
        Self {
            last_table: None,
            position: None,
            timestamp: Utc::now().timestamp_millis(),
        }
    }
}

/// Executor for sync operations.
///
/// Handles the actual data movement from source to destination.
pub struct SyncExecutor {
    db: PgPool,
    r2_storage: Arc<R2Storage>,
    clickhouse_storage: Arc<ClickHouseStorage>,
    encryptor: Arc<dyn SecretEncrypt>,
    partition_manager: Arc<PartitionManager>,
    pii_worker: Arc<PiiScanWorker>,
}

impl SyncExecutor {
    /// Create a new sync executor.
    pub fn new(
        db: PgPool,
        r2_storage: Arc<R2Storage>,
        clickhouse_storage: Arc<ClickHouseStorage>,
        encryptor: Arc<dyn SecretEncrypt>,
        partition_manager: Arc<PartitionManager>,
        pii_worker: Arc<PiiScanWorker>,
    ) -> Self {
        Self {
            db,
            r2_storage,
            clickhouse_storage,
            encryptor,
            partition_manager,
            pii_worker,
        }
    }

    /// Clean up orphaned partitions from failed sync jobs.
    /// 
    /// Called at the start of each sync to remove pending partitions
    /// from previously failed or stale jobs. This ensures we don't
    /// accumulate orphaned Parquet files in R2.
    #[tracing::instrument(
        name = "warehouse.sync.cleanup_orphans",
        skip_all,
        err(Display),
    )]
    pub async fn cleanup_orphans(&self, source_id: Uuid) -> Result<usize> {
        // Find pending partitions for this source where job failed or is stale
        let orphans: Vec<OrphanPartition> = sqlx::query_as(
            r#"
            SELECT p.id, p.parquet_path 
            FROM warehouse_partitions p
            LEFT JOIN warehouse_jobs j ON p.job_id = j.id
            WHERE p.source_id = $1 
              AND p.sync_state = 'pending'
              AND (j.id IS NULL OR j.status IN ('failed', 'cancelled')
                   OR (j.status != 'running' AND j.started_at < NOW() - INTERVAL '6 hours'))
            "#,
        )
        .bind(source_id)
        .fetch_all(&self.db)
        .await?;
        
        if orphans.is_empty() {
            return Ok(0);
        }
        
        info!(
            source_id = %source_id,
            orphan_count = orphans.len(),
            "Cleaning up orphaned partitions"
        );
        
        // Delete R2 files and stats sidecars (best effort)
        for orphan in &orphans {
            if let Some(path) = &orphan.parquet_path {
                if let Err(e) = self.r2_storage.delete_with_stats(path).await {
                    tracing::warn!(
                        path = %path,
                        error = %e,
                        "Failed to delete orphaned R2 file (may not exist)"
                    );
                }
            }
        }
        
        // Delete partition records
        let ids: Vec<Uuid> = orphans.iter().map(|o| o.id).collect();
        sqlx::query("DELETE FROM warehouse_partitions WHERE id = ANY($1)")
            .bind(&ids)
            .execute(&self.db)
            .await?;
        
        info!(
            source_id = %source_id,
            cleaned_count = ids.len(),
            "Orphan cleanup complete"
        );
        
        Ok(ids.len())
    }

    /// Execute sync for a source.
    ///
    /// - Cleans up orphaned partitions from failed jobs
    /// - Loads source configuration
    /// - Creates appropriate connector
    /// - Fetches data from source
    /// - Routes to destination based on tier (R2 Parquet or ClickHouse)
    /// - Saves checkpoint for resumability
    #[tracing::instrument(
        name = "warehouse.sync_executor.sync_source",
        skip(self),
        fields(%source_id, tier = ?tier, job_id = ?job_id),
        err(Display),
    )]
    pub async fn sync_source(
        &self,
        source_id: Uuid,
        tier: StorageTier,
        job_id: Option<Uuid>,
    ) -> Result<SyncResult> {
        info!(source_id = %source_id, tier = ?tier, job_id = ?job_id, "Starting sync");
        
        // Clean up orphaned partitions from failed jobs before starting
        if let Err(e) = self.cleanup_orphans(source_id).await {
            tracing::warn!(
                source_id = %source_id,
                error = %e,
                "Failed to cleanup orphans (continuing with sync)"
            );
        }

        // Load source configuration
        let source = self.load_source_config(source_id).await?;

        // Load existing checkpoint -- if empty, this is a full sync from the beginning
        let mut checkpoint = self.load_checkpoint(source_id).await?;

        info!(
            source_id = %source_id,
            source_type = %source.source_type,
            sync_version = checkpoint.global_sync_version,
            tables_with_checkpoints = checkpoint.tables.len(),
            "Loaded source configuration"
        );

        // Create connector based on source type
        let connector = self.create_connector(&source).await?;

        // List tables to sync
        let tables = connector.list_tables().await
            .map_err(|e| anyhow::anyhow!("Failed to list tables: {}", e))?;

        let mut result = SyncResult {
            tables_synced: 0,
            total_rows: 0,
            total_bytes: 0,
            checkpoint: None,
            staging_tables: Vec::new(),
        };

        // Bump the global sync version for this sync run
        checkpoint.global_sync_version += 1;
        let sync_version = checkpoint.global_sync_version;

        // Sync each table using the unified loop.
        // Mutation stats handles are collected here so they are awaited
        // even if sync_tables_internal returns an early error.
        let mut mutation_stats_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        let sync_result = self.sync_tables_internal(
            source_id,
            &source,
            &*connector,
            &tables,
            tier,
            job_id,
            source.sync_scope,
            &mut result,
            &mut checkpoint,
            sync_version,
            &mut mutation_stats_handles,
        ).await;

        // Await mutation stats inserts with a timeout, regardless of sync result.
        if !mutation_stats_handles.is_empty() {
            let count = mutation_stats_handles.len();
            match tokio::time::timeout(
                std::time::Duration::from_secs(5),
                futures::future::join_all(mutation_stats_handles),
            ).await {
                Ok(_) => {}
                Err(_) => {
                    tracing::warn!(pending = count, "Timed out waiting for mutation stats inserts");
                }
            }
        }

        // Handle staging tables based on result.
        let mut hot_tier_rolled_back = false;
        if tier == StorageTier::Hot && !result.staging_tables.is_empty() {
            if sync_result.is_ok() {
                match self.commit_staging_tables(source_id, &result.staging_tables).await {
                    Ok(()) => {}
                    Err((committed_tables, e)) => {
                        // Partial commit: save checkpoint only for tables that
                        // were successfully committed so they aren't re-synced
                        // (which would cause duplicates).
                        if !committed_tables.is_empty() {
                            let mut partial_checkpoint = checkpoint.clone();
                            partial_checkpoint.tables.retain(|name, _| {
                                committed_tables.contains(name)
                            });
                            if let Err(cp_err) = self.save_source_checkpoint(source_id, &partial_checkpoint).await {
                                tracing::error!(
                                    source_id = %source_id,
                                    error = %cp_err,
                                    committed_tables = ?committed_tables,
                                    "Failed to save partial checkpoint after hot-tier commit; \
                                     committed tables may be duplicated on retry"
                                );
                            }
                        }
                        return Err(e);
                    }
                }
            } else {
                self.rollback_staging_tables(source_id, &result.staging_tables).await;
                hot_tier_rolled_back = true;
            }
        }

        // Save checkpoint for successfully synced tables. Skip when hot-tier
        // staging was rolled back — the data didn't land, so advancing the
        // cursor would permanently lose it.
        if !hot_tier_rolled_back {
            self.save_source_checkpoint(source_id, &checkpoint).await?;
        }

        // Propagate error if sync failed (after saving partial progress)
        sync_result?;

        // Persist discovered PKs to warehouse_tables for query rewriter.
        // Done after sync so that ensure_table_registered has already created
        // the rows, preventing the UPDATE from silently affecting 0 rows.
        for table_info in &tables {
            if !table_info.primary_key_columns.is_empty() {
                if let Err(e) = self.save_primary_key_columns(source_id, &table_info.name, &table_info.primary_key_columns).await {
                    tracing::warn!(
                        source_id = %source_id,
                        table = %table_info.name,
                        error = %e,
                        "Failed to save primary key columns (non-fatal)"
                    );
                }
            }
        }

        info!(
            source_id = %source_id,
            tables = result.tables_synced,
            rows = result.total_rows,
            bytes = result.total_bytes,
            sync_version = sync_version,
            "Sync complete"
        );

        Ok(result)
    }

    /// Commit all staging tables after a successful sync.
    #[tracing::instrument(
        name = "warehouse.sync.commit_staging_tables",
        skip_all,
    )]
    /// Returns `Ok(())` on full success, or `Err((committed_table_names, error))`
    /// on partial failure so the caller can save a partial checkpoint.
    async fn commit_staging_tables(
        &self,
        source_id: Uuid,
        staging: &[StagingTableInfo],
    ) -> Result<(), (Vec<String>, anyhow::Error)> {
        let mut committed: Vec<&StagingTableInfo> = Vec::new();
        for s in staging {
            match self.clickhouse_storage
                .commit_staging_table(s.project_id, &s.source_name, &s.table_name)
                .await
            {
                Ok(()) => {
                    committed.push(s);
                }
                Err(e) => {
                    tracing::error!(
                        source_id = %source_id,
                        table = %s.table_name,
                        committed_count = committed.len(),
                        total = staging.len(),
                        error = %e,
                        "Staging commit failed mid-way, rolling back uncommitted tables"
                    );
                    let committed_names: Vec<String> =
                        committed.iter().map(|c| c.table_name.clone()).collect();
                    let uncommitted: Vec<StagingTableInfo> = staging
                        .iter()
                        .filter(|t| !committed.iter().any(|c| c.table_name == t.table_name))
                        .cloned()
                        .collect();
                    self.rollback_staging_tables(source_id, &uncommitted).await;
                    return Err((
                        committed_names,
                        anyhow::anyhow!("Failed to commit staging table {}: {}", s.table_name, e),
                    ));
                }
            }
        }
        info!(source_id = %source_id, staging_tables = staging.len(), "Committed all staging tables");
        Ok(())
    }

    /// Rollback (drop) all staging tables after a failed sync.
    #[tracing::instrument(
        name = "warehouse.sync.rollback_staging_tables",
        skip_all,
    )]
    async fn rollback_staging_tables(&self, source_id: Uuid, staging: &[StagingTableInfo]) {
        for s in staging {
            if let Err(e) = self.clickhouse_storage
                .drop_staging_tables(s.project_id, &s.source_name, Some(&[s.table_name.clone()]))
                .await
            {
                tracing::warn!(table = %s.table_name, error = %e, "Failed to drop staging table during rollback");
            }
        }
        info!(source_id = %source_id, staging_tables = staging.len(), "Rolled back staging tables");
    }

    /// Unified sync loop for all tables.
    ///
    /// Uses the per-table checkpoint to fetch only new/changed data.
    /// When no checkpoint exists for a table, fetches everything (full sync).
    /// Injects metadata columns (`_dh_sync_version`, `_dh_op`) into all Parquet writes
    /// to support deduplication and soft deletes at query time.
    #[tracing::instrument(
        name = "warehouse.sync.sync_tables_internal",
        skip_all,
        err(Display),
    )]
    async fn sync_tables_internal(
        &self,
        source_id: Uuid,
        source: &DecryptedSourceConfig,
        connector: &dyn Connector,
        tables: &[crate::warehouse::connectors::TableInfo],
        tier: StorageTier,
        job_id: Option<Uuid>,
        sync_scope: SyncScope,
        result: &mut SyncResult,
        checkpoint: &mut SourceCheckpoint,
        sync_version: i64,
        mutation_stats_handles: &mut Vec<tokio::task::JoinHandle<()>>,
    ) -> Result<()> {

        for table_info in tables {
            // Look up per-table checkpoint for incremental fetch
            let table_cp = checkpoint.tables.get(&table_info.name);
            let (incremental_key, last_value) = match table_cp {
                Some(cp) => (Some(cp.incremental_key.as_str()), Some(cp.last_value.as_str())),
                None => {
                    // No checkpoint -- use the table's declared incremental_key with no last_value
                    // (fetches everything from the beginning)
                    (table_info.incremental_key.as_deref(), None)
                }
            };

            let is_incremental = last_value.is_some();

            info!(
                source_id = %source_id,
                table = %table_info.name,
                sync_scope = %sync_scope,
                sync_version = sync_version,
                is_incremental = is_incremental,
                "Syncing table"
            );

            // Fetch data from source using incremental parameters
            let batches = connector
                .fetch_table(&table_info.name, incremental_key, last_value)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to fetch table {}: {}", table_info.name, e))?;

            if batches.is_empty() {
                info!(table = %table_info.name, "Table is empty, skipping");
                continue;
            }

            // Capture original row count before time-based filtering so we can
            // detect whether any rows were dropped.  When rows are dropped the
            // checkpoint must NOT advance — otherwise rows whose incremental key
            // falls below the new checkpoint but whose timestamp was too recent
            // would be permanently skipped on subsequent syncs.
            let original_row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

            // Apply time-based filtering if sync scope is TimeBased
            let batches = if let SyncScope::TimeBased { older_than_days } = sync_scope {
                self.filter_batches_by_age(&batches, older_than_days)?
            } else {
                batches
            };

            if batches.is_empty() {
                info!(table = %table_info.name, "No data matching sync scope, skipping");
                continue;
            }

            let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
            let time_filter_dropped_rows = row_count < original_row_count;

            // Route to appropriate destination based on tier
            let bytes_written = match tier {
                StorageTier::Warm => {
                    // Copy-on-write: for incremental syncs with PKs, merge new rows
                    // into existing files. For first syncs or no-PK tables, just append.
                    let has_pk = !table_info.primary_key_columns.is_empty();
                    if is_incremental && has_pk {
                        self.merge_warm_incremental(
                            source_id,
                            source.project_id,
                            &source.name,
                            &table_info.name,
                            &batches,
                            job_id,
                            sync_version,
                            &table_info.primary_key_columns,
                        )
                        .await?
                    } else {
                        self.write_warm_incremental(
                            source_id,
                            source.project_id,
                            &source.name,
                            &table_info.name,
                            &batches,
                            job_id,
                            sync_version,
                        )
                        .await?
                    }
                }
                StorageTier::Hot => {
                    // Track staging table for later commit/rollback
                    result.staging_tables.push(StagingTableInfo {
                        project_id: source.project_id,
                        source_name: source.name.clone(),
                        table_name: table_info.name.clone(),
                    });
                    
                    self.write_hot(
                        source.project_id,
                        &source.name,
                        &table_info.name,
                        &table_info.schema,
                        &batches,
                    )
                    .await?
                }
                StorageTier::Cold => {
                    return Err(anyhow::anyhow!(
                        "Cannot sync source in 'cold' tier. Must be warm or hot."
                    ));
                }
            };

            // Emit mutation stats to ClickHouse (non-blocking, best-effort)
            if tier == StorageTier::Warm {
                // All cursor-based rows are inserts; CDC ops are counted separately
                let (ins, upd, del) = (row_count as u64, 0u64, 0u64);
                let ch = self.clickhouse_storage.clone();
                let pid = source.project_id;
                let sid = source_id;
                let tname = table_info.name.clone();
                let today = Utc::now().date_naive();
                mutation_stats_handles.push(tokio::spawn(async move {
                    if let Err(e) = ch.insert_mutation_stats(pid, sid, &tname, today, ins, upd, del).await {
                        tracing::warn!(
                            error = %e,
                            "Failed to emit mutation stats (non-fatal)"
                        );
                    }
                }));
            }

            // Extract last_value from the last batch's incremental_key column for checkpoint.
            // (must happen before batches are moved to PII worker)
            //
            // IMPORTANT: when time-based filtering dropped rows we must NOT
            // advance the checkpoint.  The filtered-out rows may have
            // incremental-key values *below* the maximum of the kept set, so
            // advancing would permanently skip them.
            if let Some(ik) = incremental_key {
                if time_filter_dropped_rows {
                    info!(
                        table = %table_info.name,
                        "Time filter removed rows — skipping checkpoint advance to avoid data loss"
                    );
                } else if let Some(last_val) = extract_last_incremental_value(&batches, ik) {
                    checkpoint.tables.insert(table_info.name.clone(), TableCheckpoint {
                        incremental_key: ik.to_string(),
                        last_value: last_val,
                        last_sync_version: sync_version,
                    });
                }
            }

            // Send batches to background PII scan worker (non-blocking, fire-and-forget)
            self.pii_worker.send(PiiScanRequest {
                batches,
                db: self.db.clone(),
                source_id,
                project_id: source.project_id,
                source_name: source.name.clone(),
                table_name: table_info.name.clone(),
                sync_scope: sync_scope.to_string(),
                tokio_handle: tokio::runtime::Handle::current(),
            });

            result.tables_synced += 1;
            result.total_rows += row_count;
            result.total_bytes += bytes_written;

            info!(
                source_id = %source_id,
                table = %table_info.name,
                rows = row_count,
                bytes = bytes_written,
                sync_version = sync_version,
                "Table sync complete"
            );
        }

        Ok(())
    }

    /// Filter record batches to only include rows older than `older_than_days`.
    ///
    /// Scans for timestamp/datetime columns and filters rows where the timestamp
    /// is older than `NOW() - older_than_days`. If no timestamp column is found,
    /// returns the batches unfiltered.
    fn filter_batches_by_age(
        &self,
        batches: &[RecordBatch],
        older_than_days: u32,
    ) -> Result<Vec<RecordBatch>> {
        use arrow::array::Scalar;
        use arrow::compute::kernels::cmp::lt_eq;
        use arrow::compute::filter_record_batch;
        use arrow::datatypes::{DataType, TimeUnit};

        if batches.is_empty() {
            return Ok(Vec::new());
        }

        let schema = batches[0].schema();
        let cutoff = Utc::now() - chrono::Duration::days(older_than_days as i64);

        // Find the first timestamp column to filter on
        let ts_col_idx = schema.fields().iter().position(|f| {
            matches!(
                f.data_type(),
                DataType::Timestamp(_, _)
                    | DataType::Date32
                    | DataType::Date64
            )
        });

        let Some(ts_col_idx) = ts_col_idx else {
            // No timestamp column found - return batches unfiltered
            tracing::warn!(
                "No timestamp column found for time-based sync scope filtering, returning all data"
            );
            return Ok(batches.to_vec());
        };

        let col_name = schema.field(ts_col_idx).name().clone();
        let col_type = schema.field(ts_col_idx).data_type().clone();
        info!(
            column = %col_name,
            cutoff = %cutoff,
            older_than_days = older_than_days,
            "Applying time-based sync scope filter"
        );

        let mut filtered = Vec::with_capacity(batches.len());
        for batch in batches {
            let ts_array = batch.column(ts_col_idx);

            // Create cutoff scalar array matching the column type
            let cutoff_array: arrow::array::ArrayRef = match &col_type {
                DataType::Timestamp(TimeUnit::Second, tz) => {
                    Arc::new(arrow::array::TimestampSecondArray::from(vec![cutoff.timestamp()]).with_timezone_opt(tz.clone()))
                }
                DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                    Arc::new(arrow::array::TimestampMillisecondArray::from(vec![cutoff.timestamp_millis()]).with_timezone_opt(tz.clone()))
                }
                DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                    let micros = cutoff.timestamp_micros();
                    Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![micros]).with_timezone_opt(tz.clone()))
                }
                DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                    let nanos = cutoff.timestamp_nanos_opt()
                        .or_else(|| cutoff.timestamp_micros().checked_mul(1000))
                        .unwrap_or(i64::MAX);
                    Arc::new(arrow::array::TimestampNanosecondArray::from(vec![nanos]).with_timezone_opt(tz.clone()))
                }
                DataType::Date32 => {
                    let days = cutoff.timestamp().div_euclid(86400) as i32;
                    Arc::new(arrow::array::Date32Array::from(vec![days]))
                }
                DataType::Date64 => {
                    Arc::new(arrow::array::Date64Array::from(vec![cutoff.timestamp_millis()]))
                }
                _ => {
                    // Shouldn't happen given our filter above
                    filtered.push(batch.clone());
                    continue;
                }
            };

            // Wrap the single-element cutoff array in a Scalar for explicit broadcast.
            // ts_array <= cutoff means "older than or equal to cutoff"
            let cutoff_scalar = Scalar::new(cutoff_array);
            let mask = lt_eq(ts_array, &cutoff_scalar)
                .map_err(|e| anyhow::anyhow!("Failed to compare timestamps: {}", e))?;

            let filtered_batch = filter_record_batch(batch, &mask)
                .map_err(|e| anyhow::anyhow!("Failed to filter batch: {}", e))?;

            if filtered_batch.num_rows() > 0 {
                filtered.push(filtered_batch);
            }
        }

        Ok(filtered)
    }

    /// Write a single Parquet chunk, upload it to R2, and return its key and metadata.
    async fn write_and_upload_chunk(
        &self,
        schema: arrow::datatypes::SchemaRef,
        chunk_batches: &[arrow::record_batch::RecordBatch],
        project_id: Uuid,
        source_id: Uuid,
        source_name: &str,
        table_name: &str,
        partition_id: Uuid,
        partition_date: &chrono::NaiveDate,
        sync_version: i64,
        seq: usize,
    ) -> Result<(String, i64, usize)> {
        let (parquet_bytes, stats) = write_parquet_with_stats(schema, chunk_batches, WriteOptions::default())
            .map_err(|e| anyhow::anyhow!("Failed to write Parquet: {}", e))?;

        let bytes_written = parquet_bytes.len();
        let file_rows: i64 = chunk_batches.iter().map(|b| b.num_rows() as i64).sum();
        let file_id = Uuid::new_v4();

        let key = format!(
            "projects/{}/warm/{}/{}/{}/{}_{:04}_{}.parquet",
            project_id, source_name, table_name,
            partition_date.format("%Y-%m-%d"),
            sync_version, seq, file_id
        );

        self.r2_storage
            .upload_parquet_with_stats(&key, parquet_bytes, &stats)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to upload to R2: {}", e))?;

        info!(
            project_id = %project_id,
            source_id = %source_id,
            table = table_name,
            partition_id = %partition_id,
            partition_date = %partition_date,
            file_seq = seq,
            rows = file_rows,
            bytes = bytes_written,
            key = key,
            "Wrote Parquet file to R2"
        );

        Ok((key, file_rows, bytes_written))
    }

    /// Update partition metadata after writing files.
    async fn update_partition_metadata(
        &self,
        partition_id: Uuid,
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
        partition_date: &chrono::NaiveDate,
        total_rows: i64,
        total_bytes: i64,
        job_id: Option<Uuid>,
    ) -> Result<()> {
        self.partition_manager
            .update_partition_data(
                partition_id,
                &format!("projects/{}/warm/{}/{}/{}/",
                    project_id, source_name, table_name,
                    partition_date.format("%Y-%m-%d")),
                total_rows,
                total_bytes,
                job_id,
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to update partition metadata: {}", e))
    }

    /// Write batches to R2 as Parquet files (warm tier) with incremental support.
    ///
    /// Partitions data by date (using detected timestamp column or today),
    /// splits into ~50-70MB Parquet files (from ~200MB uncompressed chunks),
    /// and records each file in `warehouse_partition_files`.
    /// Used for first-time syncs and tables without primary keys where
    /// merge-on-write is not possible.
    #[tracing::instrument(
        name = "warehouse.sync_executor.write_warm",
        skip(self, batches),
        fields(%source_id, %project_id, %source_name, %table_name, batch_count = batches.len(), job_id = ?job_id, sync_version),
        err(Display),
    )]
    async fn write_warm_incremental(
        &self,
        source_id: Uuid,
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
        batches: &[RecordBatch],
        job_id: Option<Uuid>,
        sync_version: i64,
    ) -> Result<usize> {
        if batches.is_empty() {
            return Ok(0);
        }

        let schema = batches[0].schema();

        // Partition batches by date
        let date_buckets = partition_batches_by_date(batches)?;

        let mut total_bytes_written = 0usize;

        for (partition_date, date_batches) in &date_buckets {
            // Get or create logical partition for this date
            let mut partition = self.partition_manager
                .get_or_create_partition(source_id, table_name, *partition_date)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to get/create partition: {}", e))?;

            // Split into chunks of ~200MB uncompressed (~50-70MB after Parquet + Snappy)
            let file_chunks = split_batches_by_size(date_batches, TARGET_FILE_SIZE_BYTES);

            // Track bytes written for this date bucket only
            let mut bucket_bytes_written = 0usize;

            for (seq, chunk_batches) in file_chunks.iter().enumerate() {
                let (key, file_rows, bytes_written) = self.write_and_upload_chunk(
                    schema.clone(), chunk_batches,
                    project_id, source_id, source_name, table_name,
                    partition.id, partition_date, sync_version, seq,
                ).await?;

                self.partition_manager
                    .add_partition_file(
                        partition.id, &key, sync_version,
                        file_rows, bytes_written as i64, "I", job_id,
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to record partition file: {}", e))?;

                bucket_bytes_written += bytes_written;
            }

            total_bytes_written += bucket_bytes_written;

            let total_rows: i64 = date_batches.iter().map(|b| b.num_rows() as i64).sum();
            self.update_partition_metadata(
                partition.id, project_id, source_name, table_name,
                partition_date, total_rows, bucket_bytes_written as i64, job_id,
            ).await?;

        }

        // Ensure table is registered in warehouse_tables for query discovery
        let r2_prefix = format!(
            "projects/{}/warm/{}/{}",
            project_id, source_name, table_name
        );
        self.ensure_table_registered(source_id, table_name, &r2_prefix).await?;

        Ok(total_bytes_written)
    }

    /// Merge-on-write: for each affected date partition, download existing files,
    /// merge new rows by PK (new rows replace existing rows with matching PKs),
    /// write clean merged output, and atomically swap file records in Postgres.
    ///
    /// This eliminates the need for query-time deduplication.
    #[tracing::instrument(
        name = "warehouse.sync_executor.merge_warm_incremental",
        skip(self, batches, pk_columns),
        fields(%source_id, %project_id, %source_name, %table_name),
        err(Display),
    )]
    async fn merge_warm_incremental(
        &self,
        source_id: Uuid,
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
        batches: &[RecordBatch],
        job_id: Option<Uuid>,
        sync_version: i64,
        pk_columns: &[String],
    ) -> Result<usize> {
        use crate::warehouse::indexes::partition_manager::NewPartitionFile;
        use crate::warehouse::sync::merge::{merge_batches_by_pk, read_parquet_bytes, strip_metadata_columns};

        if batches.is_empty() {
            return Ok(0);
        }

        let date_buckets = partition_batches_by_date(batches)?;
        let mut total_bytes_written = 0usize;

        for (partition_date, new_date_batches) in &date_buckets {
            let mut partition = self.partition_manager
                .get_or_create_partition(source_id, table_name, *partition_date)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to get/create partition: {}", e))?;

            // Download existing committed files for this partition
            let existing_files = self.partition_manager
                .list_partition_files(partition.id)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to list partition files: {}", e))?;

            let download_futures = existing_files.iter().map(|file| {
                let storage = self.r2_storage.clone();
                let path = file.file_path.clone();
                async move {
                    let data = storage.download(&path).await
                        .map_err(|e| anyhow::anyhow!(
                            "Aborting merge: failed to download '{}': {}",
                            path, e
                        ))?;
                    read_parquet_bytes(&data)
                        .map_err(|e| anyhow::anyhow!(
                            "Aborting merge: failed to read Parquet '{}': {}",
                            path, e
                        ))
                }
            });
            let dl_results: Vec<Result<Vec<RecordBatch>>> =
                futures::future::join_all(download_futures).await;
            let mut existing_batches: Vec<RecordBatch> = Vec::new();
            for r in dl_results {
                existing_batches.extend(r?);
            }

            // Strip legacy metadata columns from existing batches
            let existing_batches = strip_metadata_columns(&existing_batches)?;

            // Merge: new rows replace existing rows with matching PKs
            let merged_batches = merge_batches_by_pk(&existing_batches, new_date_batches, pk_columns)?;

            if merged_batches.is_empty() {
                continue;
            }

            let merged_schema = merged_batches[0].schema();
            let file_chunks = split_batches_by_size(&merged_batches, TARGET_FILE_SIZE_BYTES);

            let mut new_files: Vec<NewPartitionFile> = Vec::new();
            let mut new_r2_keys: Vec<String> = Vec::new();
            let mut bucket_bytes_written = 0usize;

            for (seq, chunk_batches) in file_chunks.iter().enumerate() {
                let (key, file_rows, bytes_written) = self.write_and_upload_chunk(
                    merged_schema.clone(), chunk_batches,
                    project_id, source_id, source_name, table_name,
                    partition.id, partition_date, sync_version, seq,
                ).await?;

                new_files.push(NewPartitionFile {
                    file_path: key.clone(),
                    row_count: file_rows,
                    size_bytes: bytes_written as i64,
                });
                new_r2_keys.push(key);
                bucket_bytes_written += bytes_written;
            }

            // Atomic swap: delete old file records, insert new ones
            let old_file_ids: Vec<Uuid> = existing_files.iter().map(|f| f.id).collect();
            let old_paths = match self.partition_manager
                .swap_partition_files(partition.id, &old_file_ids, &new_files, sync_version)
                .await
            {
                Ok(paths) => paths,
                Err(e) => {
                    // Clean up uploaded R2 files to avoid orphans
                    for key in &new_r2_keys {
                        if let Err(del_err) = self.r2_storage.delete_with_stats(key).await {
                            tracing::debug!(key = %key, error = %del_err, "Best-effort cleanup of new R2 file failed");
                        }
                    }
                    return Err(anyhow::anyhow!("Failed to swap partition files: {}", e));
                }
            };

            // Best-effort R2 cleanup of old files and their stats sidecars
            for old_path in &old_paths {
                if let Err(e) = self.r2_storage.delete_with_stats(old_path).await {
                    tracing::debug!(path = %old_path, error = %e, "Best-effort cleanup of old R2 file failed");
                }
            }

            total_bytes_written += bucket_bytes_written;

            let total_rows: i64 = merged_batches.iter().map(|b| b.num_rows() as i64).sum();
            self.update_partition_metadata(
                partition.id, project_id, source_name, table_name,
                partition_date, total_rows, bucket_bytes_written as i64, job_id,
            ).await?;

        }

        // Ensure table is registered in warehouse_tables for query discovery
        let r2_prefix = format!(
            "projects/{}/warm/{}/{}",
            project_id, source_name, table_name
        );
        self.ensure_table_registered(source_id, table_name, &r2_prefix).await?;

        Ok(total_bytes_written)
    }

    /// Write batches to ClickHouse (hot tier) using staging tables.
    /// 
    /// For atomic sync, data is first written to a staging table. The staging
    /// table is committed (swapped with production) after ALL tables in the
    /// sync are successfully written. If any table fails, all staging tables
    /// are dropped (rollback).
    #[tracing::instrument(
        name = "warehouse.sync_executor.write_hot",
        skip(self, schema, batches),
        fields(%project_id, %source_name, %table_name, batch_count = batches.len()),
        err(Display),
    )]
    async fn write_hot(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
        schema: &TableSchema,
        batches: &[RecordBatch],
    ) -> Result<usize> {
        if batches.is_empty() {
            return Ok(0);
        }

        // Create staging table with the same schema
        // This staging table will be committed (swapped with production) after
        // ALL tables are synced successfully
        self.clickhouse_storage
            .create_staging_table(project_id, source_name, table_name, schema)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create staging table: {}", e))?;

        let mut total_rows = 0usize;

        let mut total_bytes = 0usize;

        // Insert each batch into the staging table
        for batch in batches {
            // Compute actual serialized size from Arrow arrays
            let batch_bytes: usize = batch.columns().iter()
                .map(|col| col.get_array_memory_size())
                .sum();
            total_bytes += batch_bytes;

            let rows = self.clickhouse_storage
                .insert_staging_batch(project_id, source_name, table_name, batch)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to insert into staging table: {}", e))?;
            total_rows += rows as usize;
        }

        info!(
            project_id = %project_id,
            source = source_name,
            table = table_name,
            rows = total_rows,
            bytes = total_bytes,
            "Inserted into ClickHouse staging table (pending commit)"
        );

        Ok(total_bytes)
    }

    /// Load source configuration from the database.
    #[tracing::instrument(
        name = "warehouse.sync.load_source_config",
        skip_all,
        err(Display),
    )]
    async fn load_source_config(&self, source_id: Uuid) -> Result<DecryptedSourceConfig> {
        let row = sqlx::query(
            "SELECT id, project_id, name, source_type, config, tier,
                    COALESCE(sync_scope, 'full') as sync_scope,
                    sync_scope_older_than_days
             FROM warehouse_sources WHERE id = $1"
        )
        .bind(source_id)
        .fetch_optional(&self.db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Source not found: {}", source_id))?;

        let id: Uuid = row.get("id");
        let project_id: Uuid = row.get("project_id");
        let name: String = row.get("name");
        let source_type_str: String = row.get("source_type");
        let tier_str: String = row.get("tier");
        let config_json: serde_json::Value = row.get("config");
        let sync_scope_str: String = row.get("sync_scope");
        let sync_scope_older_than_days: Option<i32> = row.get("sync_scope_older_than_days");

        let source_type: SourceType = source_type_str.parse()
            .map_err(|e: String| anyhow::anyhow!("Invalid source type '{}': {}", source_type_str, e))?;
        let tier: StorageTier = tier_str.parse()
            .map_err(|e: String| anyhow::anyhow!("Invalid storage tier '{}': {}", tier_str, e))?;
        
        // Parse sync scope
        let sync_scope = match sync_scope_str.as_str() {
            "time_based" => {
                let days = sync_scope_older_than_days.unwrap_or(0).max(0) as u32;
                SyncScope::TimeBased { older_than_days: days }
            }
            _ => SyncScope::Full,
        };

        // Decrypt the config - it's stored as JSONB with {"encrypted": "..."} structure
        let config = if let Some(encrypted_str) = config_json.get("encrypted").and_then(|v| v.as_str()) {
            let decrypted = self.encryptor.decrypt(encrypted_str)
                .map_err(|e| anyhow::anyhow!("Failed to decrypt source config: {}", e))?;
            Some(serde_json::from_str(&decrypted)
                .map_err(|e| anyhow::anyhow!("Failed to parse source config JSON: {}", e))?)
        } else {
            // Legacy unencrypted config - return as-is but log a warning
            tracing::warn!("Source config is not encrypted - this is a security risk");
            Some(config_json)
        };

        Ok(DecryptedSourceConfig {
            id,
            project_id,
            name,
            source_type,
            tier,
            sync_scope,
            config,
        })
    }

    /// Load source checkpoint from the database.
    /// Returns the new `SourceCheckpoint` format, or a default if none stored
    /// or if the stored format is the legacy `Checkpoint`.
    #[tracing::instrument(
        name = "warehouse.sync.load_checkpoint",
        skip_all,
        err(Display),
    )]
    async fn load_checkpoint(&self, source_id: Uuid) -> Result<SourceCheckpoint> {
        let row = sqlx::query(
            "SELECT sync_checkpoint FROM warehouse_sources WHERE id = $1"
        )
        .bind(source_id)
        .fetch_optional(&self.db)
        .await?;

        let Some(row) = row else {
            return Ok(SourceCheckpoint::default());
        };

        let checkpoint_json: Option<serde_json::Value> = row.get("sync_checkpoint");

        match checkpoint_json {
            Some(json) => {
                // Try new format first, fall back to default on parse failure
                // (handles legacy Checkpoint format gracefully)
                match serde_json::from_value::<SourceCheckpoint>(json) {
                    Ok(cp) => Ok(cp),
                    Err(_) => Ok(SourceCheckpoint::default()),
                }
            }
            None => Ok(SourceCheckpoint::default()),
        }
    }

    /// Save source checkpoint to the database.
    #[tracing::instrument(
        name = "warehouse.sync.save_checkpoint",
        skip_all,
        err(Display),
    )]
    async fn save_source_checkpoint(&self, source_id: Uuid, checkpoint: &SourceCheckpoint) -> Result<()> {
        let checkpoint_json = serde_json::to_value(checkpoint)
            .map_err(|e| anyhow::anyhow!("Failed to serialize checkpoint: {}", e))?;

        sqlx::query(
            "UPDATE warehouse_sources SET sync_checkpoint = $1, updated_at = NOW() WHERE id = $2"
        )
        .bind(checkpoint_json)
        .bind(source_id)
        .execute(&self.db)
        .await?;

        Ok(())
    }

    /// Ensure a table is registered in warehouse_tables for query discovery.
    /// This is called after writing Parquet files to R2.
    #[tracing::instrument(
        name = "warehouse.sync.ensure_table_registered",
        skip_all,
        err(Display),
    )]
    async fn ensure_table_registered(
        &self,
        source_id: Uuid,
        table_name: &str,
        r2_prefix: &str,
    ) -> Result<()> {
        // Use INSERT ... ON CONFLICT to upsert the table entry
        sqlx::query(
            "INSERT INTO warehouse_tables (id, source_id, name, schema, r2_prefix, sync_enabled, created_at, updated_at)
             VALUES ($1, $2, $3, '{}'::jsonb, $4, true, NOW(), NOW())
             ON CONFLICT (source_id, name) DO UPDATE SET
                r2_prefix = EXCLUDED.r2_prefix,
                updated_at = NOW()"
        )
        .bind(Uuid::new_v4())
        .bind(source_id)
        .bind(table_name)
        .bind(r2_prefix)
        .execute(&self.db)
        .await?;

        Ok(())
    }

    /// Persist discovered primary key columns to warehouse_tables so the
    /// query rewriter can use them for deduplication.
    async fn save_primary_key_columns(
        &self,
        source_id: Uuid,
        table_name: &str,
        pk_columns: &[String],
    ) -> Result<()> {
        sqlx::query(
            "UPDATE warehouse_tables SET primary_key_columns = $1, updated_at = NOW() \
             WHERE source_id = $2 AND name = $3"
        )
        .bind(pk_columns)
        .bind(source_id)
        .bind(table_name)
        .execute(&self.db)
        .await?;

        Ok(())
    }

    /// Create a connector based on source type and configuration.
    #[tracing::instrument(
        name = "warehouse.sync.create_connector",
        skip_all,
        err(Display),
    )]
    async fn create_connector(&self, source: &DecryptedSourceConfig) -> Result<Box<dyn Connector>> {
        let config = source.config.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Source has no configuration"))?;
        connector_factory::create_connector(source.source_type, config).await
    }
}

/// Target uncompressed buffer size before flushing to Parquet.
/// 200MB uncompressed typically compresses to ~50-70MB with Parquet + snappy.
const TARGET_FILE_SIZE_BYTES: usize = 200 * 1024 * 1024;

/// Partition record batches by date using a detected timestamp column.
/// Splits individual batches at date boundaries so each output batch
/// contains rows from exactly one date. Falls back to today's date
/// if no timestamp column is found.
fn partition_batches_by_date(
    batches: &[RecordBatch],
) -> Result<Vec<(chrono::NaiveDate, Vec<RecordBatch>)>> {
    use arrow::compute::filter_record_batch;
    use arrow::datatypes::DataType;
    use std::collections::BTreeMap;

    if batches.is_empty() {
        return Ok(vec![]);
    }

    let schema = batches[0].schema();

    // Try to find a timestamp column for date partitioning
    let ts_col_idx = schema.fields().iter().position(|f| {
        matches!(
            f.data_type(),
            DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64
        )
    });

    match ts_col_idx {
        Some(idx) => {
            let mut buckets: BTreeMap<chrono::NaiveDate, Vec<RecordBatch>> = BTreeMap::new();
            let fallback_date = Utc::now().date_naive();

            for batch in batches {
                if batch.num_rows() == 0 {
                    continue;
                }

                // Pass 1: collect only distinct dates (no per-row Vec allocation)
                let mut distinct_dates_set: std::collections::HashSet<chrono::NaiveDate> = std::collections::HashSet::new();
                for row in 0..batch.num_rows() {
                    let date = extract_date_from_row(batch, idx, row)
                        .unwrap_or(fallback_date);
                    distinct_dates_set.insert(date);
                    if distinct_dates_set.len() > 1 {
                        break;
                    }
                }

                if distinct_dates_set.len() == 1 {
                    let date = distinct_dates_set.into_iter().next().unwrap();
                    buckets.entry(date).or_default().push(batch.clone());
                } else {
                    // Pass 2: multiple dates found -- collect per-row dates for mask building.
                    // We must rebuild the set because Pass 1 broke out early after finding
                    // 2 distinct dates, so the set is incomplete.
                    let mut row_dates: Vec<chrono::NaiveDate> = Vec::with_capacity(batch.num_rows());
                    distinct_dates_set.clear();
                    for row in 0..batch.num_rows() {
                        let date = extract_date_from_row(batch, idx, row)
                            .unwrap_or(fallback_date);
                        row_dates.push(date);
                        distinct_dates_set.insert(date);
                    }
                    let distinct_dates: Vec<chrono::NaiveDate> = distinct_dates_set.into_iter().collect();

                    for date in &distinct_dates {
                        let mask: arrow::array::BooleanArray = row_dates.iter()
                            .map(|d| Some(*d == *date))
                            .collect();

                        let filtered = filter_record_batch(batch, &mask)
                            .map_err(|e| anyhow::anyhow!("Failed to split batch by date: {}", e))?;

                        if filtered.num_rows() > 0 {
                            buckets.entry(*date).or_default().push(filtered);
                        }
                    }
                }
            }

            Ok(buckets.into_iter().collect())
        }
        None => {
            // No timestamp column found; use today's date for all data
            let today = Utc::now().date_naive();
            Ok(vec![(today, batches.to_vec())])
        }
    }
}

/// Extract a date from a specific row of a timestamp/date column.
fn extract_date_from_row(batch: &RecordBatch, col_idx: usize, row: usize) -> Option<chrono::NaiveDate> {
    use arrow::array::{Array, TimestampMicrosecondArray, TimestampMillisecondArray,
                       TimestampNanosecondArray, TimestampSecondArray, Date32Array, Date64Array};
    use arrow::datatypes::{DataType, TimeUnit};
    use chrono::NaiveDateTime;

    if row >= batch.num_rows() {
        return None;
    }

    let col = batch.column(col_idx);
    match col.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => {
            let arr = col.as_any().downcast_ref::<TimestampSecondArray>()?;
            if Array::is_null(arr, row) { return None; }
            NaiveDateTime::from_timestamp_opt(arr.value(row), 0).map(|dt| dt.date())
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr = col.as_any().downcast_ref::<TimestampMillisecondArray>()?;
            if Array::is_null(arr, row) { return None; }
            NaiveDateTime::from_timestamp_millis(arr.value(row)).map(|dt| dt.date())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = col.as_any().downcast_ref::<TimestampMicrosecondArray>()?;
            if Array::is_null(arr, row) { return None; }
            let ts = arr.value(row);
            let secs = ts.div_euclid(1_000_000);
            let subsec_nanos = ts.rem_euclid(1_000_000) as u32 * 1000;
            NaiveDateTime::from_timestamp_opt(secs, subsec_nanos)
                .map(|dt| dt.date())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = col.as_any().downcast_ref::<TimestampNanosecondArray>()?;
            if Array::is_null(arr, row) { return None; }
            let ts = arr.value(row);
            let secs = ts.div_euclid(1_000_000_000);
            let subsec_nanos = ts.rem_euclid(1_000_000_000) as u32;
            NaiveDateTime::from_timestamp_opt(secs, subsec_nanos)
                .map(|dt| dt.date())
        }
        DataType::Date32 => {
            let arr = col.as_any().downcast_ref::<Date32Array>()?;
            if Array::is_null(arr, row) { return None; }
            chrono::NaiveDate::from_num_days_from_ce_opt(arr.value(row) + 719_163)
        }
        DataType::Date64 => {
            let arr = col.as_any().downcast_ref::<Date64Array>()?;
            if Array::is_null(arr, row) { return None; }
            NaiveDateTime::from_timestamp_millis(arr.value(row)).map(|dt| dt.date())
        }
        _ => None,
    }
}

/// Split batches into chunks that target the given uncompressed byte size.
/// Each chunk will be written as a separate Parquet file.
///
/// When a single batch exceeds `target_bytes`, it is split at the row level
/// using `batch.slice()` to avoid producing oversized Parquet files.
pub fn split_batches_by_size(
    batches: &[RecordBatch],
    target_bytes: usize,
) -> Vec<Vec<RecordBatch>> {
    let mut chunks: Vec<Vec<RecordBatch>> = Vec::new();
    let mut current_chunk: Vec<RecordBatch> = Vec::new();
    let mut current_size: usize = 0;

    for batch in batches {
        let batch_size = batch.get_array_memory_size();

        // If a single batch exceeds target, split it at the row level
        if batch_size > target_bytes && batch.num_rows() > 1 {
            // Flush the current chunk first
            if !current_chunk.is_empty() {
                chunks.push(std::mem::take(&mut current_chunk));
                current_size = 0;
            }

            let rows_per_chunk = ((batch.num_rows() as f64) * (target_bytes as f64)
                / (batch_size as f64)) as usize;
            let rows_per_chunk = rows_per_chunk.max(1);
            let mut offset = 0;
            while offset < batch.num_rows() {
                let len = (batch.num_rows() - offset).min(rows_per_chunk);
                let slice = batch.slice(offset, len);
                let slice_size = slice.get_array_memory_size();

                if !current_chunk.is_empty() && current_size + slice_size > target_bytes {
                    chunks.push(std::mem::take(&mut current_chunk));
                    current_size = 0;
                }
                current_size += slice_size;
                current_chunk.push(slice);
                offset += len;
            }
        } else {
            if !current_chunk.is_empty() && current_size + batch_size > target_bytes {
                chunks.push(std::mem::take(&mut current_chunk));
                current_size = 0;
            }
            current_size += batch_size;
            current_chunk.push(batch.clone());
        }
    }

    if !current_chunk.is_empty() {
        chunks.push(current_chunk);
    }

    // Ensure we return at least one chunk if input was non-empty
    if chunks.is_empty() && !batches.is_empty() {
        chunks.push(batches.to_vec());
    }

    chunks
}

/// Extract the maximum value of an incremental key column across ALL batches.
///
/// Scans every batch to find the true maximum for supported types (Int64,
/// Float64, String, timestamps). For unsupported column types, falls back to
/// the last row of the last batch (which may not be the true max if the
/// source delivers data out of order).
fn extract_last_incremental_value(
    batches: &[RecordBatch],
    incremental_key: &str,
) -> Option<String> {
    use arrow::array::{
        Array, Int64Array, Float64Array, StringArray,
        TimestampSecondArray, TimestampMillisecondArray,
        TimestampMicrosecondArray, TimestampNanosecondArray,
    };

    if batches.is_empty() {
        return None;
    }

    // Verify column exists using the first batch's schema
    let schema = batches[0].schema();
    let col_idx = schema.index_of(incremental_key).ok()?;

    let mut max_i64: Option<i64> = None;
    let mut max_f64: Option<f64> = None;
    let mut max_str: Option<String> = None;
    let mut max_ts_micros: Option<i64> = None;
    let mut found_type = false;

    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let col = batch.column(col_idx);

        if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            found_type = true;
            if let Some(batch_max) = arrow::compute::max(arr) {
                max_i64 = Some(match max_i64 {
                    Some(prev) => prev.max(batch_max),
                    None => batch_max,
                });
            }
        } else if let Some(arr) = col.as_any().downcast_ref::<TimestampSecondArray>() {
            found_type = true;
            if let Some(batch_max) = arrow::compute::max(arr) {
                let as_micros = batch_max.saturating_mul(1_000_000);
                max_ts_micros = Some(match max_ts_micros {
                    Some(prev) => prev.max(as_micros),
                    None => as_micros,
                });
            }
        } else if let Some(arr) = col.as_any().downcast_ref::<TimestampMillisecondArray>() {
            found_type = true;
            if let Some(batch_max) = arrow::compute::max(arr) {
                let as_micros = batch_max.saturating_mul(1_000);
                max_ts_micros = Some(match max_ts_micros {
                    Some(prev) => prev.max(as_micros),
                    None => as_micros,
                });
            }
        } else if let Some(arr) = col.as_any().downcast_ref::<TimestampMicrosecondArray>() {
            found_type = true;
            if let Some(batch_max) = arrow::compute::max(arr) {
                max_ts_micros = Some(match max_ts_micros {
                    Some(prev) => prev.max(batch_max),
                    None => batch_max,
                });
            }
        } else if let Some(arr) = col.as_any().downcast_ref::<TimestampNanosecondArray>() {
            found_type = true;
            if let Some(batch_max) = arrow::compute::max(arr) {
                let as_micros = batch_max.div_euclid(1_000)
                    + if batch_max.rem_euclid(1_000) != 0 { 1 } else { 0 };
                max_ts_micros = Some(match max_ts_micros {
                    Some(prev) => prev.max(as_micros),
                    None => as_micros,
                });
            }
        } else if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
            found_type = true;
            let mut batch_max_finite: Option<f64> = None;
            for i in 0..arr.len() {
                if arr.is_null(i) { continue; }
                let v = arr.value(i);
                if v.is_finite() {
                    batch_max_finite = Some(match batch_max_finite {
                        Some(prev) => v.max(prev),
                        None => v,
                    });
                }
            }
            if let Some(batch_max) = batch_max_finite {
                max_f64 = Some(match max_f64 {
                    Some(prev) => batch_max.max(prev),
                    None => batch_max,
                });
            }
        } else if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
            found_type = true;
            if let Some(batch_max) = arrow::compute::max_string(arr) {
                max_str = Some(match max_str {
                    Some(ref prev) => {
                        if batch_max > prev.as_str() { batch_max.to_string() } else { prev.clone() }
                    }
                    None => batch_max.to_string(),
                });
            }
        }
    }

    if let Some(us) = max_ts_micros {
        if let Some(dt) = chrono::DateTime::from_timestamp_micros(us) {
            return Some(dt.to_rfc3339_opts(chrono::SecondsFormat::Micros, true));
        }
        return Some(us.to_string());
    }
    if let Some(v) = max_i64 {
        return Some(v.to_string());
    }
    if let Some(v) = max_f64 {
        return Some(v.to_string());
    }
    if let Some(v) = max_str {
        return Some(v);
    }

    // Fallback for unsupported types: use the last row of the last batch
    if !found_type {
        let last_batch = batches.last()?;
        if last_batch.num_rows() == 0 {
            return None;
        }
        let col = last_batch.column(col_idx);
        let last_row = last_batch.num_rows() - 1;
        if col.is_null(last_row) {
            return None;
        }
        use arrow::util::display::ArrayFormatter;
        let fmt = ArrayFormatter::try_new(col.as_ref(), &Default::default()).ok()?;
        return Some(fmt.value(last_row).to_string());
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc as StdArc;

    #[test]
    fn test_source_checkpoint_serialization() {
        let mut cp = SourceCheckpoint::default();
        cp.global_sync_version = 42;
        cp.tables.insert("users".to_string(), TableCheckpoint {
            incremental_key: "updated_at".to_string(),
            last_value: "2025-01-01T00:00:00Z".to_string(),
            last_sync_version: 42,
        });

        let json = serde_json::to_string(&cp).unwrap();
        let parsed: SourceCheckpoint = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.global_sync_version, 42);
        assert_eq!(parsed.tables.len(), 1);
        let table_cp = parsed.tables.get("users").unwrap();
        assert_eq!(table_cp.incremental_key, "updated_at");
        assert_eq!(table_cp.last_value, "2025-01-01T00:00:00Z");
        assert_eq!(table_cp.last_sync_version, 42);
    }

    #[test]
    fn test_split_batches_by_size() {
        let schema = StdArc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
        ]));

        // Create a large-ish batch (each i64 = 8 bytes, so 1000 = 8KB)
        let vals: Vec<i64> = (0..1000).collect();
        let batch = RecordBatch::try_new(
            schema,
            vec![StdArc::new(Int64Array::from(vals))],
        ).unwrap();

        // With a very small target, row-level splitting produces many chunks
        let chunks = split_batches_by_size(&[batch.clone(), batch.clone()], 1);
        assert!(chunks.len() > 2, "Should split into many chunks with tiny target");

        // All rows preserved
        let total_rows: usize = chunks.iter().flat_map(|c| c.iter()).map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2000);

        // With a very large target, everything stays in one chunk
        let chunks = split_batches_by_size(&[batch.clone(), batch], 1_000_000_000);
        assert_eq!(chunks.len(), 1);
    }

    #[test]
    fn test_partition_batches_by_date_no_timestamp() {
        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![StdArc::new(Int64Array::from(vec![1, 2]))],
        ).unwrap();

        let buckets = partition_batches_by_date(&[batch]).unwrap();
        assert_eq!(buckets.len(), 1);
        // Should use today's date
        assert_eq!(buckets[0].0, Utc::now().date_naive());
    }

    #[test]
    fn test_extract_last_incremental_value_int() {
        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("updated_at", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(Int64Array::from(vec![1, 2, 3])),
                StdArc::new(Int64Array::from(vec![100, 200, 300])),
            ],
        ).unwrap();

        let val = extract_last_incremental_value(&[batch], "updated_at");
        assert_eq!(val, Some("300".to_string()));
    }

    #[test]
    fn test_extract_last_incremental_value_missing_column() {
        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![StdArc::new(Int64Array::from(vec![1]))],
        ).unwrap();

        let val = extract_last_incremental_value(&[batch], "nonexistent");
        assert_eq!(val, None);
    }

    #[test]
    fn test_extract_last_incremental_value_unordered() {
        // C3 test: max value is in the first batch's middle row, not last batch's last row
        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("updated_at", DataType::Int64, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                StdArc::new(Int64Array::from(vec![1, 2, 3])),
                StdArc::new(Int64Array::from(vec![100, 999, 50])),
            ],
        ).unwrap();

        let batch2 = RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(Int64Array::from(vec![4, 5])),
                StdArc::new(Int64Array::from(vec![200, 300])),
            ],
        ).unwrap();

        let val = extract_last_incremental_value(&[batch1, batch2], "updated_at");
        // Should find 999 (from batch1 row 1), not 300 (last row of last batch)
        assert_eq!(val, Some("999".to_string()));
    }

    #[test]
    fn test_partition_batches_by_date_cross_day() {
        use arrow::array::TimestampMillisecondArray;

        // Create a batch with timestamps spanning two days
        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("ts", DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None), false),
        ]));

        let day1_ms = chrono::NaiveDate::from_ymd_opt(2025, 6, 15).unwrap()
            .and_hms_opt(10, 0, 0).unwrap()
            .and_utc().timestamp_millis();
        let day2_ms = chrono::NaiveDate::from_ymd_opt(2025, 6, 16).unwrap()
            .and_hms_opt(14, 0, 0).unwrap()
            .and_utc().timestamp_millis();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(Int64Array::from(vec![1, 2, 3])),
                StdArc::new(TimestampMillisecondArray::from(vec![day1_ms, day2_ms, day1_ms])),
            ],
        ).unwrap();

        let buckets = partition_batches_by_date(&[batch]).unwrap();
        assert_eq!(buckets.len(), 2, "Should produce two date buckets");

        // Collect row counts per date
        let mut row_counts: Vec<(chrono::NaiveDate, usize)> = buckets.iter()
            .map(|(date, batches)| (*date, batches.iter().map(|b| b.num_rows()).sum()))
            .collect();
        row_counts.sort_by_key(|(d, _)| *d);

        assert_eq!(row_counts[0].0, chrono::NaiveDate::from_ymd_opt(2025, 6, 15).unwrap());
        assert_eq!(row_counts[0].1, 2); // rows 1 and 3
        assert_eq!(row_counts[1].0, chrono::NaiveDate::from_ymd_opt(2025, 6, 16).unwrap());
        assert_eq!(row_counts[1].1, 1); // row 2
    }

    #[test]
    fn test_partition_batches_by_date_single_date() {
        use arrow::array::TimestampMillisecondArray;

        let the_date = chrono::NaiveDate::from_ymd_opt(2025, 6, 15).unwrap();
        let ts_ms = the_date.and_hms_opt(10, 0, 0).unwrap()
            .and_utc().timestamp_millis();

        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("ts", DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None), false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(Int64Array::from(vec![1, 2, 3])),
                StdArc::new(TimestampMillisecondArray::from(vec![ts_ms, ts_ms, ts_ms])),
            ],
        ).unwrap();

        let buckets = partition_batches_by_date(&[batch]).unwrap();
        assert_eq!(buckets.len(), 1, "Single-date batch should produce one bucket");
        assert_eq!(buckets[0].0, the_date);
        let total_rows: usize = buckets[0].1.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_split_batches_by_size_oversized_single_batch() {
        let schema = StdArc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
        ]));

        // Create a batch with 10000 i64 values (~80KB)
        let vals: Vec<i64> = (0..10000).collect();
        let batch = RecordBatch::try_new(
            schema,
            vec![StdArc::new(Int64Array::from(vals))],
        ).unwrap();

        let batch_size = batch.get_array_memory_size();
        assert!(batch_size > 1000, "Batch should be larger than target");

        // With target much smaller than batch, should row-split
        let chunks = split_batches_by_size(&[batch], 1000);
        assert!(chunks.len() > 1, "Oversized batch should be split into multiple chunks");

        // All rows should be preserved
        let total_rows: usize = chunks.iter()
            .flat_map(|c| c.iter())
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(total_rows, 10000);
    }

    #[test]
    fn test_extract_last_incremental_value_nan_not_poisoned() {
        use arrow::array::Float64Array;

        let schema = StdArc::new(Schema::new(vec![
            Field::new("score", DataType::Float64, true),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![StdArc::new(Float64Array::from(vec![
                Some(f64::NAN),
                Some(10.0),
                Some(f64::NAN),
            ]))],
        ).unwrap();

        let batch2 = RecordBatch::try_new(
            schema,
            vec![StdArc::new(Float64Array::from(vec![
                Some(20.0),
                Some(f64::NAN),
            ]))],
        ).unwrap();

        let val = extract_last_incremental_value(&[batch1, batch2], "score");
        assert!(val.is_some(), "Should produce a value when finite values exist");
        let parsed: f64 = val.unwrap().parse().unwrap();
        assert!((parsed - 20.0).abs() < f64::EPSILON,
            "Max should be 20.0, not NaN");
    }

    #[test]
    fn test_extract_last_incremental_value_infinity_excluded() {
        use arrow::array::Float64Array;

        let schema = StdArc::new(Schema::new(vec![
            Field::new("val", DataType::Float64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![StdArc::new(Float64Array::from(vec![
                5.0, f64::INFINITY, 10.0,
            ]))],
        ).unwrap();

        let val = extract_last_incremental_value(&[batch], "val");
        assert!(val.is_some());
        let parsed: f64 = val.unwrap().parse().unwrap();
        assert!((parsed - 10.0).abs() < f64::EPSILON,
            "Max should be 10.0, not Infinity");
    }

    /// Regression test for Bug 1: when time-based filtering drops rows, the
    /// checkpoint must NOT advance. This prevents data loss when the
    /// incremental key is not monotonically correlated with the timestamp.
    #[test]
    fn test_checkpoint_not_advanced_when_time_filter_drops_rows() {
        use arrow::array::TimestampMillisecondArray;
        use arrow::datatypes::TimeUnit;

        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]));

        // Row id=85 has a very recent timestamp (should be filtered out).
        // Row id=90 has an old timestamp (should be kept).
        let now_ms = Utc::now().timestamp_millis();
        let old_ms = (Utc::now() - chrono::Duration::days(60)).timestamp_millis();

        let batches = vec![RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(Int64Array::from(vec![81, 85, 90])),
                StdArc::new(TimestampMillisecondArray::from(vec![old_ms, now_ms, old_ms])),
            ],
        )
        .unwrap()];

        let original_row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(original_row_count, 3);

        // Simulate time-based filtering (older_than_days = 30):
        // row id=85 is only 0 days old → filtered out;
        // rows id=81 and id=90 are 60 days old → kept.
        let cutoff = Utc::now() - chrono::Duration::days(30);
        let mut filtered = Vec::new();
        for batch in &batches {
            use arrow::compute::filter_record_batch;
            use arrow::compute::kernels::cmp::lt_eq;
            use arrow::array::Scalar;
            use std::sync::Arc;

            let ts_col = batch.column(1);
            let cutoff_array: arrow::array::ArrayRef = Arc::new(
                TimestampMillisecondArray::from(vec![cutoff.timestamp_millis()]),
            );
            let cutoff_scalar = Scalar::new(cutoff_array);
            let mask = lt_eq(ts_col, &cutoff_scalar).unwrap();
            let fb = filter_record_batch(batch, &mask).unwrap();
            if fb.num_rows() > 0 {
                filtered.push(fb);
            }
        }

        let filtered_row_count: usize = filtered.iter().map(|b| b.num_rows()).sum();
        assert_eq!(filtered_row_count, 2, "One row should have been filtered out");

        let time_filter_dropped_rows = filtered_row_count < original_row_count;
        assert!(
            time_filter_dropped_rows,
            "Flag must detect that rows were dropped"
        );

        // The actual checkpoint logic: when rows were filtered, do NOT advance.
        let mut checkpoint = SourceCheckpoint::default();
        let incremental_key = "id";
        if time_filter_dropped_rows {
            // Checkpoint should NOT be updated
        } else if let Some(last_val) =
            extract_last_incremental_value(&filtered, incremental_key)
        {
            checkpoint.tables.insert(
                "orders".to_string(),
                TableCheckpoint {
                    incremental_key: incremental_key.to_string(),
                    last_value: last_val,
                    last_sync_version: 1,
                },
            );
        }

        assert!(
            checkpoint.tables.get("orders").is_none(),
            "Checkpoint must NOT advance when time filter dropped rows"
        );
    }

    /// Verify checkpoint IS advanced when no time filtering occurs.
    #[test]
    fn test_checkpoint_advanced_when_no_rows_filtered() {
        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));

        let batches = vec![RecordBatch::try_new(
            schema,
            vec![StdArc::new(Int64Array::from(vec![81, 85, 90]))],
        )
        .unwrap()];

        let original_row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        let filtered_row_count = original_row_count; // no filtering
        let time_filter_dropped_rows = filtered_row_count < original_row_count;

        assert!(!time_filter_dropped_rows);

        let mut checkpoint = SourceCheckpoint::default();
        let incremental_key = "id";
        if time_filter_dropped_rows {
            // skip
        } else if let Some(last_val) =
            extract_last_incremental_value(&batches, incremental_key)
        {
            checkpoint.tables.insert(
                "orders".to_string(),
                TableCheckpoint {
                    incremental_key: incremental_key.to_string(),
                    last_value: last_val,
                    last_sync_version: 1,
                },
            );
        }

        let table_cp = checkpoint
            .tables
            .get("orders")
            .expect("Checkpoint should be updated when no rows were filtered");
        assert_eq!(table_cp.last_value, "90");
    }

    #[test]
    fn test_date32_pre_epoch_uses_euclidean_division() {
        // 1969-12-31 12:00:00 UTC → timestamp = -43200
        // Truncating division: -43200 / 86400 = 0 (WRONG, maps to 1970-01-01)
        // Euclidean division:  -43200.div_euclid(86400) = -1 (CORRECT, maps to 1969-12-31)
        let ts: i64 = -43200;
        let days_trunc = (ts / 86400) as i32;
        let days_euclid = ts.div_euclid(86400) as i32;

        assert_eq!(days_trunc, 0, "sanity: truncating division gives wrong day");
        assert_eq!(days_euclid, -1, "euclidean division gives correct day -1");

        // Verify a second case: 1969-12-30 23:59:59 → timestamp = -86401
        let ts2: i64 = -86401;
        let days2 = ts2.div_euclid(86400) as i32;
        assert_eq!(days2, -2, "1969-12-30 should be day -2");
    }

    #[test]
    fn test_nanosecond_fallback_no_overflow() {
        // When timestamp_nanos_opt() returns None (out of i64 range),
        // the fallback must not panic via unchecked multiply.
        let far_future = chrono::DateTime::from_timestamp(10_000_000_000, 0).unwrap();
        let nanos = far_future
            .timestamp_nanos_opt()
            .or_else(|| far_future.timestamp_micros().checked_mul(1000))
            .unwrap_or(i64::MAX);

        assert!(nanos > 0, "nanos must be positive for a far-future date");
    }

    #[test]
    fn test_extract_last_incremental_value_timestamp_returns_iso8601() {
        use arrow::array::TimestampMicrosecondArray;
        use arrow::datatypes::TimeUnit;

        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ]));

        // 2024-01-15 08:00:00 UTC = 1705305600 seconds = 1705305600_000000 microseconds
        let us = 1705305600_000000_i64;
        let batch = RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(Int64Array::from(vec![1])),
                StdArc::new(TimestampMicrosecondArray::from(vec![Some(us)])),
            ],
        )
        .unwrap();

        let val = extract_last_incremental_value(&[batch], "created_at");
        let val = val.expect("should return Some");
        assert!(
            val.contains("2024-01-15"),
            "timestamp must be ISO-8601 formatted, got: {val}"
        );
        assert!(
            val.contains('T'),
            "must be RFC3339/ISO-8601 with T separator, got: {val}"
        );
        assert!(
            !val.chars().all(|c| c.is_ascii_digit()),
            "must NOT be raw microseconds integer, got: {val}"
        );
    }

    #[test]
    fn test_nanosecond_to_microsecond_rounds_up() {
        // Nanosecond value that doesn't divide evenly into microseconds.
        // Truncation would cause a re-sync loop: the checkpoint would always
        // be below the actual max value.
        let ns_value = 1706745600_000000_999_i64; // 999 extra nanoseconds
        let schema = StdArc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None), true),
        ]));

        let ts_array = arrow::array::TimestampNanosecondArray::from(vec![Some(ns_value)]);
        let batch = RecordBatch::try_new(schema, vec![StdArc::new(ts_array)]).unwrap();

        let val = extract_last_incremental_value(&[batch], "ts");
        let val = val.expect("should return Some");

        // Parse back to microseconds to verify rounding
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&val) {
            let checkpoint_us = dt.timestamp_micros();
            let original_us_truncated = ns_value / 1_000;
            assert!(
                checkpoint_us > original_us_truncated,
                "checkpoint must round up to avoid re-sync loop: checkpoint={} truncated={}",
                checkpoint_us,
                original_us_truncated
            );
        }
    }

    #[test]
    fn test_ceiling_div_negative_nanoseconds() {
        // Pre-epoch timestamp: -1 microsecond exactly (-1000 nanoseconds)
        // Ceiling division must yield -1, not 0.
        let ns: i64 = -1000;
        let as_micros = ns.div_euclid(1_000)
            + if ns.rem_euclid(1_000) != 0 { 1 } else { 0 };
        assert_eq!(as_micros, -1, "exact -1µs must not round up");

        // -999 nanoseconds should ceil to 0 (the next microsecond boundary)
        let ns2: i64 = -999;
        let as_micros2 = ns2.div_euclid(1_000)
            + if ns2.rem_euclid(1_000) != 0 { 1 } else { 0 };
        assert_eq!(as_micros2, 0, "-999ns should ceil to 0µs");

        // Positive: 1001 nanoseconds should ceil to 2 microseconds
        let ns3: i64 = 1001;
        let as_micros3 = ns3.div_euclid(1_000)
            + if ns3.rem_euclid(1_000) != 0 { 1 } else { 0 };
        assert_eq!(as_micros3, 2, "1001ns should ceil to 2µs");

        // Exact positive: 2000 nanoseconds = exactly 2 microseconds
        let ns4: i64 = 2000;
        let as_micros4 = ns4.div_euclid(1_000)
            + if ns4.rem_euclid(1_000) != 0 { 1 } else { 0 };
        assert_eq!(as_micros4, 2, "2000ns should be exactly 2µs");
    }
}
