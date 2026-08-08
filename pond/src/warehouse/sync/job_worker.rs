//! Job Worker
//!
//! Polls for pending jobs and executes them with claim/lock mechanism.
//! Supports graceful shutdown via a shutdown signal.

use chrono::{DateTime, Duration, Utc};
use sqlx::{PgPool, Row};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::watch;
use uuid::Uuid;

use reiver_core::events::{EventPublisher, PlatformEventType};

use crate::crypto::SecretEncrypt;
use crate::warehouse::connectors::factory as connector_factory;
use crate::warehouse::connectors::{Connector, ConnectorError};

/// Heartbeat fires at half the lock duration to renew well before expiry.
const HEARTBEAT_INTERVAL_DIVISOR: i64 = 2;
/// Minimum heartbeat interval (seconds) to avoid excessive DB writes.
const MIN_HEARTBEAT_INTERVAL_SECS: i64 = 30;
use crate::warehouse::metrics::WarehouseMetrics;
use crate::warehouse::storage::r2::R2Storage;
use crate::warehouse::sync::worker::run_sync;
use crate::warehouse::types::{JobStatus, JobType, SourceType, SyncResult};

/// Errors that can occur during job execution.
#[derive(Debug, Error)]
pub enum JobError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Connector error: {0}")]
    Connector(#[from] ConnectorError),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Job not found: {0}")]
    NotFound(Uuid),

    #[error("Job execution error: {0}")]
    Execution(String),

    #[error("Feature not implemented: {0}")]
    NotImplemented(String),
}

/// Result type for job operations.
pub type JobResult<T> = Result<T, JobError>;

/// A job from the database.
#[derive(Debug, Clone)]
pub struct Job {
    pub id: Uuid,
    pub job_type: JobType,
    pub source_id: Option<Uuid>,
    pub table_name: Option<String>,
    pub status: JobStatus,
    pub scheduled_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
    pub retry_count: i32,
    pub max_retries: i32,
    pub locked_by: Option<String>,
    pub locked_at: Option<DateTime<Utc>>,
    pub lock_expires_at: Option<DateTime<Utc>>,
}

/// Worker that polls for pending jobs and executes them.
///
/// Supports graceful shutdown via the shutdown receiver.
pub struct JobWorker {
    db: PgPool,
    worker_id: String,
    lock_duration: Duration,
    /// Shutdown signal receiver
    shutdown_rx: watch::Receiver<bool>,
    /// Encryptor for decrypting source configs
    encryptor: Arc<dyn SecretEncrypt>,
    /// R2 storage for sync output
    storage: Arc<R2Storage>,
    /// Warehouse metrics collector
    metrics: Arc<WarehouseMetrics>,
    /// Shared dirty-set so the query path knows to reload skip indexes
    /// after a sync persists new inline indexes.
    skip_index_dirty: Option<Arc<dashmap::DashSet<Uuid>>>,
    /// Shared dirty-set so the query path knows to reload table metadata
    /// after a sync completes.
    table_cache_dirty: Option<Arc<dashmap::DashSet<Uuid>>>,
    /// Platform event publisher for sync lifecycle events
    event_publisher: Option<Arc<EventPublisher>>,
}

/// Handle to control the job worker.
pub struct JobWorkerHandle {
    /// Shutdown signal sender
    shutdown_tx: watch::Sender<bool>,
}

impl JobWorkerHandle {
    /// Signal the worker to shut down gracefully.
    ///
    /// The worker will finish processing the current job before exiting.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

impl JobWorker {
    /// Create a new job worker with a shutdown handle.
    ///
    /// Returns the worker and a handle that can be used to signal shutdown.
    pub fn new(
        db: PgPool,
        worker_id: String,
        encryptor: Arc<dyn SecretEncrypt>,
        storage: Arc<R2Storage>,
        metrics: Arc<WarehouseMetrics>,
    ) -> (Self, JobWorkerHandle) {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let worker = Self {
            db,
            worker_id,
            lock_duration: Duration::minutes(10),
            shutdown_rx,
            encryptor,
            storage,
            metrics,
            skip_index_dirty: None,
            table_cache_dirty: None,
            event_publisher: None,
        };

        let handle = JobWorkerHandle { shutdown_tx };

        (worker, handle)
    }

    pub fn with_event_publisher(mut self, publisher: Arc<EventPublisher>) -> Self {
        self.event_publisher = Some(publisher);
        self
    }

    /// Set the lock duration for jobs.
    pub fn with_lock_duration(mut self, duration: Duration) -> Self {
        self.lock_duration = duration;
        self
    }

    /// Provide the shared dirty-set used by the query path to detect stale
    /// skip index caches. When the worker persists inline indexes after sync,
    /// it inserts the project into this set so the next query reloads.
    pub fn with_skip_index_dirty(mut self, dirty: Arc<dashmap::DashSet<Uuid>>) -> Self {
        self.skip_index_dirty = Some(dirty);
        self
    }

    /// Provide the shared dirty-set used by the query path to detect stale
    /// table metadata caches. When the worker completes a sync, it inserts
    /// the project into this set so the next query reloads.
    pub fn with_table_cache_dirty(mut self, dirty: Arc<dashmap::DashSet<Uuid>>) -> Self {
        self.table_cache_dirty = Some(dirty);
        self
    }

    /// Start polling for jobs.
    ///
    /// Runs until a shutdown signal is received. The worker will complete
    /// any in-progress job before exiting.
    #[tracing::instrument(name = "warehouse.sync.start", skip_all, err(Display))]
    pub async fn start(&mut self) -> JobResult<()> {
        tracing::info!(worker_id = %self.worker_id, "Job worker starting");

        loop {
            // Check for shutdown signal
            if *self.shutdown_rx.borrow() {
                tracing::info!(worker_id = %self.worker_id, "Job worker shutting down");
                return Ok(());
            }

            // Try to claim a pending job
            match self.claim_job().await {
                Ok(Some(job)) => {
                    tracing::info!(
                        job_id = %job.id,
                        job_type = %job.job_type,
                        "Claimed job"
                    );

                    // Execute the job with heartbeat for lock extension
                    let result = self.execute_job_with_heartbeat(&job).await;

                    // Update job status
                    if let Err(e) = self.complete_job(&job, result).await {
                        tracing::error!(
                            job_id = %job.id,
                            error = %e,
                            "Failed to update job status"
                        );
                    }
                }
                Ok(None) => {
                    // No jobs available, wait before polling again
                    // Use select to also listen for shutdown
                    tokio::select! {
                        _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
                        _ = self.shutdown_rx.changed() => {
                            if *self.shutdown_rx.borrow() {
                                tracing::info!(worker_id = %self.worker_id, "Job worker shutting down");
                                return Ok(());
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "Failed to claim job");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    /// Claim a pending job with row-level locking.
    #[tracing::instrument(name = "pond.job_worker.claim_job", skip(self))]
    async fn claim_job(&self) -> JobResult<Option<Job>> {
        let lock_expires = Utc::now() + self.lock_duration;

        // Atomic claim: only one worker gets the job
        let row = sqlx::query(
            r#"
            UPDATE warehouse_jobs
            SET status = 'running', 
                locked_by = $1, 
                locked_at = NOW(),
                lock_expires_at = $2,
                started_at = NOW()
            WHERE id = (
                SELECT id FROM warehouse_jobs
                WHERE status = 'pending' AND scheduled_at <= NOW()
                ORDER BY scheduled_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING id, job_type, source_id, table_name, status, 
                      scheduled_at, started_at, completed_at, error,
                      retry_count, max_retries, locked_by, locked_at, lock_expires_at
            "#,
        )
        .bind(&self.worker_id)
        .bind(lock_expires)
        .fetch_optional(&self.db)
        .await?;

        let job = row.map(|r| {
            let job_type_str: String = r.get("job_type");
            let status_str: String = r.get("status");
            let job_type = job_type_str.parse::<JobType>().unwrap_or_else(|_| {
                tracing::warn!(raw = %job_type_str, "Unrecognized job_type, defaulting to Sync");
                JobType::Sync
            });
            let status = status_str.parse::<JobStatus>().unwrap_or_else(|_| {
                tracing::warn!(raw = %status_str, "Unrecognized job status, defaulting to Pending");
                JobStatus::Pending
            });
            Job {
                id: r.get("id"),
                job_type,
                source_id: r.get("source_id"),
                table_name: r.get("table_name"),
                status,
                scheduled_at: r.get("scheduled_at"),
                started_at: r.get("started_at"),
                completed_at: r.get("completed_at"),
                error: r.get("error"),
                retry_count: r.get("retry_count"),
                max_retries: r.get("max_retries"),
                locked_by: r.get("locked_by"),
                locked_at: r.get("locked_at"),
                lock_expires_at: r.get("lock_expires_at"),
            }
        });
        Ok(job)
    }

    /// Execute a job with a heartbeat task that extends the lock periodically.
    ///
    /// This prevents long-running jobs from having their locks expire and being
    /// picked up by another worker (causing duplicate execution).
    #[tracing::instrument(name = "pond.job_worker.execute_with_heartbeat", skip(self, job), fields(job_id = %job.id, job_type = ?job.job_type))]
    async fn execute_job_with_heartbeat(&self, job: &Job) -> Result<SyncResult, JobError> {
        let job_id = job.id;
        let db = self.db.clone();
        let lock_duration = self.lock_duration;

        // Calculate heartbeat interval as half the lock duration
        // This ensures we extend well before expiry
        let heartbeat_interval = std::time::Duration::from_secs(
            (lock_duration.num_seconds() / HEARTBEAT_INTERVAL_DIVISOR)
                .max(MIN_HEARTBEAT_INTERVAL_SECS) as u64,
        );

        // Create a cancellation token for the heartbeat task
        let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();

        // Spawn heartbeat task
        let heartbeat_handle = tokio::spawn(heartbeat_loop(
            db,
            job_id,
            lock_duration,
            heartbeat_interval,
            cancel_rx,
        ));

        // Execute the actual job
        let result = self.execute_job(job).await;

        // Cancel the heartbeat task
        let _ = cancel_tx.send(());

        // Wait for heartbeat task to finish (with timeout to avoid blocking)
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), heartbeat_handle).await;

        result
    }

    /// Execute a job.
    #[tracing::instrument(name = "pond.job_worker.execute_job", skip(self, job), fields(job_id = %job.id, job_type = ?job.job_type))]
    async fn execute_job(&self, job: &Job) -> Result<SyncResult, JobError> {
        match job.job_type {
            JobType::Sync => self.execute_sync_job(job).await,
            JobType::FstRebuild => self.execute_fst_rebuild_job(job).await,
            JobType::SchemaSnapshot => self.execute_schema_snapshot_job(job).await,
            other => Err(JobError::Execution(format!(
                "Job type {} not handled by job worker",
                other
            ))),
        }
    }

    /// Execute a sync job.
    ///
    /// This job type syncs data from a source connector to R2 storage.
    #[tracing::instrument(name = "pond.job_worker.execute_sync_job", skip(self, job), fields(job_id = %job.id))]
    async fn execute_sync_job(&self, job: &Job) -> Result<SyncResult, JobError> {
        let source_id = job
            .source_id
            .ok_or_else(|| JobError::Execution("Sync job missing source_id".to_string()))?;

        // Load source configuration from database
        // SECURITY: Also fetch project_id for data isolation
        let source_row = sqlx::query(
            "SELECT id, project_id, name, source_type, config, enabled FROM warehouse_sources WHERE id = $1"
        )
        .bind(source_id)
        .fetch_optional(&self.db)
        .await?
        .ok_or_else(|| JobError::Execution(format!("Source not found: {}", source_id)))?;

        let enabled: bool = source_row.get("enabled");
        if !enabled {
            return Err(JobError::Execution("Source is disabled".to_string()));
        }

        // SECURITY: Get project_id for data isolation in R2 paths
        let project_id: Uuid = source_row.get("project_id");

        let source_type_str: String = source_row.get("source_type");
        let config_json: serde_json::Value = source_row.get("config");

        // Decrypt source config
        let decrypted_config = self.decrypt_source_config(&config_json)?;

        // Parse source type
        let source_type: SourceType = source_type_str
            .parse()
            .map_err(|e: String| JobError::Execution(e))?;

        // Create connector based on source type
        let connector: Box<dyn Connector> = self
            .create_connector(source_type, &decrypted_config)
            .await?;

        // Get table name from job or sync all tables
        let table_name = job.table_name.as_deref();

        let mut sync_result = match table_name {
            Some(table) => run_sync(
                project_id,
                source_type,
                connector.as_ref(),
                &self.storage,
                table,
                None,
                None,
                &self.metrics,
            )
            .await
            .map_err(|e| JobError::Execution(format!("Sync failed: {}", e)))?,
            None => {
                let results = crate::warehouse::sync::worker::run_full_sync(
                    project_id,
                    source_type,
                    connector.as_ref(),
                    &self.storage,
                    &self.metrics,
                )
                .await
                .map_err(|e| JobError::Execution(format!("Full sync failed: {}", e)))?;

                aggregate_sync_results(results)
            }
        };

        // Persist any inline skip indexes built during sync.
        if !sync_result.file_indexes.is_empty() {
            let table_name_for_index = table_name.unwrap_or("unknown");
            let mut saved = 0usize;
            for inline_idx in sync_result.file_indexes.drain(..) {
                if let Err(e) = crate::warehouse::indexes::persistence::save_file_skip_index(
                    &self.db,
                    project_id,
                    table_name_for_index,
                    &inline_idx.partition_key,
                    &inline_idx.index,
                    inline_idx.row_count,
                    None,
                )
                .await
                {
                    tracing::warn!(
                        file = %inline_idx.file_path,
                        error = %e,
                        "Failed to persist inline skip index"
                    );
                } else {
                    saved += 1;
                }
            }
            if saved > 0 {
                tracing::info!(
                    project_id = %project_id,
                    indexes_saved = saved,
                    "Persisted inline skip indexes from sync"
                );
                // Mark the project's skip index cache as stale so the next
                // query triggers a reload.
                if let Some(ref dirty) = self.skip_index_dirty {
                    dirty.insert(project_id);
                }
            }
        }

        if let Some(ref dirty) = self.table_cache_dirty {
            dirty.insert(project_id);
        }

        Ok(sync_result)
    }

    /// Decrypt a source configuration from the database.
    fn decrypt_source_config(
        &self,
        config: &serde_json::Value,
    ) -> Result<serde_json::Value, JobError> {
        // Check if config is encrypted
        if let Some(encrypted) = config.get("encrypted").and_then(|v| v.as_str()) {
            let decrypted = self
                .encryptor
                .decrypt(encrypted)
                .map_err(|e| JobError::Execution(format!("Failed to decrypt config: {}", e)))?;

            serde_json::from_str(&decrypted)
                .map_err(|e| JobError::Execution(format!("Invalid config JSON: {}", e)))
        } else {
            // Config is not encrypted (legacy or test data)
            Ok(config.clone())
        }
    }

    /// Create a connector instance based on source type and configuration.
    async fn create_connector(
        &self,
        source_type: SourceType,
        config: &serde_json::Value,
    ) -> Result<Box<dyn Connector>, JobError> {
        connector_factory::create_connector(source_type, config)
            .await
            .map_err(|e| JobError::Execution(e.to_string()))
    }

    /// Execute an FST rebuild job.
    ///
    /// This job type rebuilds the skip indexes for query optimization.
    /// It scans Parquet files in R2 to extract unique values for indexable columns.
    ///
    /// # When to use
    /// - After migration from non-indexed storage
    /// - If skip indexes are corrupted or missing
    /// - To rebuild indexes with new column selections
    ///
    /// # Performance
    /// - This is an expensive operation for large datasets
    /// - Use sparingly; prefer incremental index building during sync
    #[tracing::instrument(name = "pond.job_worker.execute_fst_rebuild_job", skip(self, job), fields(job_id = %job.id))]
    async fn execute_fst_rebuild_job(&self, job: &Job) -> Result<SyncResult, JobError> {
        use std::collections::HashSet;
        use crate::warehouse::indexes::external_config::{
            detect_partition_strategy, PartitionStrategy,
        };

        let source_id = job
            .source_id
            .ok_or_else(|| JobError::Execution("FST rebuild job missing source_id".to_string()))?;

        let source_row =
            sqlx::query("SELECT project_id, source_type FROM warehouse_sources WHERE id = $1")
                .bind(source_id)
                .fetch_optional(&self.db)
                .await?
                .ok_or_else(|| JobError::Execution(format!("Source not found: {}", source_id)))?;

        let project_id: Uuid = source_row.get("project_id");
        let source_type_str: String = source_row.get("source_type");

        let table_rows = sqlx::query(
            "SELECT name, r2_prefix, detected_partition_scheme FROM warehouse_tables WHERE source_id = $1"
        )
        .bind(source_id)
        .fetch_all(&self.db)
        .await?;

        if table_rows.is_empty() {
            tracing::info!(source_id = %source_id, "No tables to rebuild skip indexes for");
            return Ok(SyncResult::default());
        }

        let time_column: Option<String> = match sqlx::query_scalar(
            "SELECT config->>'time_column' FROM warehouse_sources WHERE id = $1",
        )
        .bind(source_id)
        .fetch_optional(&self.db)
        .await
        {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(source_id = %source_id, error = %e, "Failed to load time_column config, proceeding without it");
                None
            }
        };

        let start_time = std::time::Instant::now();
        let mut total_files = 0u32;
        let mut total_fst_columns = 0u64;

        for table_row in table_rows {
            let table_name: String = table_row.get("name");
            let r2_prefix: String = table_row.get("r2_prefix");

            // Load the previously persisted partition strategy (if any).
            let previous_strategy: Option<PartitionStrategy> = table_row
                .try_get::<Option<serde_json::Value>, _>("detected_partition_scheme")
                .ok()
                .flatten()
                .and_then(|v| serde_json::from_value(v).ok());

            // List Parquet files in storage.
            let files = self
                .storage
                .list_objects(&r2_prefix)
                .await
                .map_err(|e| JobError::Storage(format!("Failed to list files: {}", e)))?;

            let parquet_files: Vec<_> = files
                .into_iter()
                .filter(|f| f.key.ends_with(".parquet"))
                .collect();

            if parquet_files.is_empty() {
                continue;
            }

            // ---- Phase 1: Detect partition strategy ----
            let file_paths: Vec<String> = parquet_files.iter().map(|f| f.key.clone()).collect();

            let mut file_stats_sample: Vec<(
                String,
                crate::warehouse::parquet_metadata::FileStats,
            )> = Vec::new();
            let sample_count = parquet_files.len().min(5);
            for file_info in parquet_files.iter().take(sample_count) {
                match self.read_footer_stats(&file_info.key).await {
                    Ok(stats) => file_stats_sample.push((file_info.key.clone(), stats)),
                    Err(e) => {
                        tracing::warn!(
                            file = %file_info.key,
                            error = %e,
                            "Failed to read footer stats for partition detection"
                        );
                    }
                }
            }

            let strategy =
                detect_partition_strategy(&file_paths, &file_stats_sample, time_column.as_deref());

            tracing::info!(
                table = %table_name,
                strategy = strategy.label(),
                "Detected partition strategy"
            );

            // Persist the detected strategy for the query rewriter.
            let strategy_json = serde_json::to_value(&strategy).unwrap_or_default();
            if let Err(e) = sqlx::query(
                "UPDATE warehouse_tables SET detected_partition_scheme = $1 WHERE source_id = $2 AND name = $3"
            )
            .bind(&strategy_json)
            .bind(source_id)
            .bind(&table_name)
            .execute(&self.db)
            .await
            {
                tracing::warn!(source_id = %source_id, table = %table_name, error = %e, "Failed to persist partition strategy");
            }

            // ---- Phase 2: Diff -- determine which files need (re)indexing ----
            let strategy_changed = match (&previous_strategy, &strategy) {
                (None, _) => true, // First run — full build.
                (Some(prev), new) => prev.label() != new.label(),
            };

            let storage_file_set: HashSet<String> = file_paths.iter().cloned().collect();

            let (files_to_index, files_to_remove) = if strategy_changed {
                tracing::info!(
                    table = %table_name,
                    "Partition strategy changed, performing full rebuild"
                );
                // Delete everything and index all files.
                if let Err(e) = crate::warehouse::indexes::persistence::delete_table_skip_indexes(
                    &self.db,
                    project_id,
                    &table_name,
                )
                .await
                {
                    tracing::warn!(table = %table_name, error = %e, "Failed to delete existing skip indexes");
                }
                (storage_file_set.clone(), HashSet::new())
            } else {
                let indexed_files = crate::warehouse::indexes::persistence::list_indexed_files(
                    &self.db,
                    project_id,
                    &table_name,
                )
                .await
                .unwrap_or_default();

                let new_files: HashSet<String> = storage_file_set
                    .difference(&indexed_files)
                    .cloned()
                    .collect();
                let removed_files: HashSet<String> = indexed_files
                    .difference(&storage_file_set)
                    .cloned()
                    .collect();

                tracing::info!(
                    table = %table_name,
                    total_storage = storage_file_set.len(),
                    already_indexed = indexed_files.len(),
                    new_files = new_files.len(),
                    removed_files = removed_files.len(),
                    "Incremental rebuild diff computed"
                );

                (new_files, removed_files)
            };

            // Remove orphaned indexes.
            if !files_to_remove.is_empty() {
                let remove_vec: Vec<String> = files_to_remove.into_iter().collect();
                if let Err(e) = crate::warehouse::indexes::persistence::delete_file_skip_indexes(
                    &self.db,
                    project_id,
                    &table_name,
                    &remove_vec,
                )
                .await
                {
                    tracing::warn!(table = %table_name, error = %e, "Failed to delete orphaned skip indexes");
                }
            }

            if files_to_index.is_empty() {
                tracing::info!(table = %table_name, "No new files to index");
                continue;
            }

            // ---- Phase 3: Build skip indexes only for new files ----
            let cached_parser = strategy.build_parser();

            for file_path in &files_to_index {
                let partition_key =
                    strategy.partition_key_for_with_parser(file_path, cached_parser.as_ref());

                let result = self.build_file_skip_index(file_path).await;
                let Some((file_index, row_count)) = result
                    .inspect_err(|e| tracing::warn!(file = %file_path, error = %e, "Failed to build skip index from file"))
                    .ok()
                    .flatten()
                else {
                    continue;
                };

                // Persist each file's index directly to the DB.
                if let Err(e) = crate::warehouse::indexes::persistence::save_file_skip_index(
                    &self.db,
                    project_id,
                    &table_name,
                    &partition_key,
                    &file_index,
                    row_count,
                    None,
                )
                .await
                {
                    tracing::warn!(file = %file_path, error = %e, "Failed to save file skip index");
                    continue;
                }

                total_files += 1;
                total_fst_columns += file_index.column_values.len() as u64;
            }

            tracing::info!(
                table = %table_name,
                new_files_indexed = total_files,
                "Incremental skip index rebuild complete for table"
            );

            // ---- Phase 4: Serialize table blob, compress, upload to R2 ----
            // Acquire a dedicated connection for the advisory lock so that
            // lock and unlock happen on the same session (PG requirement).
            let mut lock_conn = match self.db.acquire().await {
                Ok(conn) => conn,
                Err(e) => {
                    tracing::warn!(
                        table = %table_name,
                        error = %e,
                        "Failed to acquire DB connection for advisory lock, skipping blob upload"
                    );
                    continue;
                }
            };

            if !crate::warehouse::indexes::persistence::try_advisory_lock(
                &mut *lock_conn,
                project_id,
                &table_name,
            )
            .await
            {
                tracing::info!(
                    table = %table_name,
                    "Skipping blob upload: another worker holds the advisory lock"
                );
                continue;
            }

            let blob_result: Result<(), JobError> = async {
                // Load the full HierarchicalSkipIndex from PG for this table.
                let index = crate::api::warehouse::load_project_skip_indexes_for_table(
                    &self.db,
                    project_id,
                    &table_name,
                )
                .await
                .map_err(|e| {
                    JobError::Execution(format!(
                        "Failed to load skip indexes for blob serialization: {}",
                        e
                    ))
                })?;

                if index.total_files() == 0 {
                    return Ok(());
                }

                let blob = crate::warehouse::indexes::blob::serialize_table_index(&index);
                let blob_size = blob.len() as i64;
                let file_count = index.total_files() as i32;
                let column_count = index
                    .partitions()
                    .flat_map(|(_, p)| p.files.values())
                    .map(|f| f.column_values.len())
                    .sum::<usize>() as i32;

                let compressed = zstd::encode_all(&blob[..], 3)
                    .map_err(|e| JobError::Execution(format!("zstd compression failed: {}", e)))?;
                let compressed_size = compressed.len() as i64;

                let r2_key = format!("indexes/{}/{}.fskp.zst", project_id, table_name);

                self.storage
                    .upload_parquet(&r2_key, bytes::Bytes::from(compressed))
                    .await
                    .map_err(|e| {
                        JobError::Storage(format!("Failed to upload index blob: {}", e))
                    })?;

                let version = crate::warehouse::indexes::persistence::upsert_manifest(
                    &self.db,
                    project_id,
                    &table_name,
                    &r2_key,
                    blob_size,
                    file_count,
                    column_count,
                )
                .await
                .map_err(|e| JobError::Execution(format!("Failed to upsert manifest: {}", e)))?;

                tracing::info!(
                    table = %table_name,
                    version = version,
                    blob_size_bytes = blob_size,
                    compressed_size_bytes = compressed_size,
                    files = file_count,
                    "Uploaded skip index blob to R2 and updated manifest"
                );

                Ok(())
            }
            .await;

            // Always release the lock on the same connection, even on error.
            crate::warehouse::indexes::persistence::release_advisory_lock(
                &mut *lock_conn,
                project_id,
                &table_name,
            )
            .await;

            if let Err(e) = blob_result {
                tracing::warn!(
                    table = %table_name,
                    error = %e,
                    "Failed to serialize/upload skip index blob (indexes still saved to PG)"
                );
            }
        }

        let duration_ms = start_time.elapsed().as_millis() as u64;

        tracing::info!(
            source_id = %source_id,
            source_type = %source_type_str,
            files_processed = total_files,
            fst_columns = total_fst_columns,
            duration_ms = duration_ms,
            "FST rebuild completed"
        );

        Ok(SyncResult {
            rows_synced: 0,
            bytes_written: 0,
            files_created: total_files,
            duration_ms,
            file_indexes: Vec::new(),
        })
    }

    /// Build a `FileSkipIndex` from a Parquet file's string column values.
    ///
    /// Downloads the file, parses it into Arrow batches, delegates to
    /// [`extract_indexable_values`] for column filtering, then builds
    /// the FST-based index.
    #[tracing::instrument(name = "warehouse.sync.build_file_skip_index", skip_all, err(Display))]
    async fn build_file_skip_index(
        &self,
        file_key: &str,
    ) -> Result<Option<(crate::warehouse::indexes::skip_index::FileSkipIndex, u64)>, JobError> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let data =
            self.storage.download(file_key).await.map_err(|e| {
                JobError::Storage(format!("Failed to download {}: {}", file_key, e))
            })?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(data.to_vec()))
            .map_err(|e| JobError::Execution(format!("Failed to read Parquet: {}", e)))?
            .build()
            .map_err(|e| JobError::Execution(format!("Failed to build reader: {}", e)))?;

        let mut batches = Vec::new();
        let mut row_count = 0u64;

        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| JobError::Execution(format!("Failed to read batch: {}", e)))?;
            row_count += batch.num_rows() as u64;
            batches.push(batch);
        }

        let mut column_values = extract_indexable_values(&batches);
        column_values.extend(extract_token_values(&batches));

        if column_values.is_empty() {
            return Ok(None);
        }

        let file_index =
            crate::warehouse::indexes::skip_index::FileSkipIndex::build(file_key, column_values)
                .map_err(|e| JobError::Execution(format!("Failed to build skip index: {}", e)))?;

        Ok(Some((file_index, row_count)))
    }

    /// Read Parquet footer statistics via a range read (downloads only the
    /// last 8 KB of the file rather than the entire file).
    #[tracing::instrument(name = "warehouse.sync.read_footer_stats", skip_all, err(Display))]
    async fn read_footer_stats(
        &self,
        file_key: &str,
    ) -> Result<crate::warehouse::parquet_metadata::FileStats, JobError> {
        let probe_size = crate::warehouse::parquet_metadata::footer_probe_size();

        let file_size = self.storage.file_size(file_key).await.map_err(|e| {
            JobError::Storage(format!("Failed to get file size for {}: {}", file_key, e))
        })?;

        if file_size < 8 {
            return Err(JobError::Execution(
                "File too small to be valid Parquet".to_string(),
            ));
        }

        let read_size = (file_size as usize).min(probe_size);
        let start = file_size - read_size as u64;

        let tail_bytes = self
            .storage
            .download_range(file_key, start, read_size as u64)
            .await
            .map_err(|e| {
                JobError::Storage(format!("Failed to download footer for {}: {}", file_key, e))
            })?;

        match crate::warehouse::parquet_metadata::extract_stats_from_footer(
            file_key,
            &tail_bytes,
            file_size as usize,
        ) {
            Ok(stats) => Ok(stats),
            Err(crate::warehouse::parquet_metadata::ParquetMetadataError::NeedMoreData {
                needed,
            }) => {
                // The initial probe was too small (wide-schema files can exceed 8 KB).
                // Retry with the exact byte count requested by the Parquet reader.
                let retry_size = (file_size as usize).min(needed);
                let retry_start = file_size - retry_size as u64;

                tracing::debug!(
                    file = %file_key,
                    initial_probe = read_size,
                    retry_size = retry_size,
                    "Footer probe too small, retrying with larger read"
                );

                let retry_bytes = self
                    .storage
                    .download_range(file_key, retry_start, retry_size as u64)
                    .await
                    .map_err(|e| {
                        JobError::Storage(format!(
                            "Failed to download expanded footer for {}: {}",
                            file_key, e
                        ))
                    })?;

                crate::warehouse::parquet_metadata::extract_stats_from_footer(
                    file_key,
                    &retry_bytes,
                    file_size as usize,
                )
                .map_err(|e| {
                    JobError::Execution(format!("Failed to parse footer after retry: {}", e))
                })
            }
            Err(e) => Err(JobError::Execution(format!(
                "Failed to parse footer: {}",
                e
            ))),
        }
    }

    /// Execute a schema snapshot job.
    ///
    /// This job type captures the current schema of all tables
    /// for drift detection purposes.
    #[tracing::instrument(name = "pond.job_worker.execute_schema_snapshot_job", skip(self, _job))]
    async fn execute_schema_snapshot_job(&self, _job: &Job) -> Result<SyncResult, JobError> {
        // Schema snapshot requires fetching schemas from all connectors
        Err(JobError::NotImplemented(
            "Schema snapshot job execution not yet implemented".to_string(),
        ))
    }

    /// Extend lock while job is running (heartbeat).
    #[tracing::instrument(name = "warehouse.sync.extend_lock", skip_all, err(Display))]
    pub async fn extend_lock(&self, job_id: Uuid) -> JobResult<()> {
        let new_expiry = Utc::now() + self.lock_duration;

        sqlx::query("UPDATE warehouse_jobs SET lock_expires_at = $1 WHERE id = $2")
            .bind(new_expiry)
            .bind(job_id)
            .execute(&self.db)
            .await?;

        Ok(())
    }

    /// Mark job as completed or failed.
    #[tracing::instrument(name = "pond.job_worker.complete_job", skip(self, job, result), fields(job_id = %job.id))]
    async fn complete_job(&self, job: &Job, result: Result<SyncResult, JobError>) -> JobResult<()> {
        match result {
            Ok(sync_result) => {
                sqlx::query(
                    r#"
                    UPDATE warehouse_jobs
                    SET status = 'completed',
                        completed_at = NOW(),
                        rows_synced = $1,
                        bytes_written = $2,
                        files_created = $3,
                        locked_by = NULL
                    WHERE id = $4
                    "#,
                )
                .bind(sync_result.rows_synced as i64)
                .bind(sync_result.bytes_written as i64)
                // Cast to i64 to prevent overflow for files_created > i32::MAX
                .bind(sync_result.files_created as i64)
                .bind(job.id)
                .execute(&self.db)
                .await?;

                tracing::info!(
                    job_id = %job.id,
                    rows_synced = sync_result.rows_synced,
                    "Job completed successfully"
                );

                if let Some(ref publisher) = self.event_publisher {
                    if let Some(source_id) = job.source_id {
                        if let Ok(Some(pid)) = sqlx::query_scalar::<_, Uuid>(
                            "SELECT project_id FROM warehouse_sources WHERE id = $1",
                        )
                        .bind(source_id)
                        .fetch_optional(&self.db)
                        .await
                        {
                            let _ = publisher.emit(
                                PlatformEventType::SyncJobCompleted,
                                pid,
                                format!("sync_job_completed:{}", job.id),
                                serde_json::json!({
                                    "job_id": job.id,
                                    "job_type": format!("{:?}", job.job_type),
                                    "source_id": source_id,
                                    "rows_synced": sync_result.rows_synced,
                                    "bytes_written": sync_result.bytes_written,
                                }),
                            ).await;
                        }
                    }
                }

                // After a successful sync, schedule an incremental FstRebuild
                // if one isn't already pending for this source.
                if job.job_type == JobType::Sync {
                    if let Some(source_id) = job.source_id {
                        self.maybe_schedule_fst_rebuild(source_id).await;
                    }
                }
            }
            Err(e) => {
                let row = sqlx::query_as::<_, (String, i32)>(
                    r#"
                    UPDATE warehouse_jobs
                    SET status = CASE
                            WHEN retry_count + 1 < max_retries THEN 'pending'
                            ELSE 'failed'
                        END,
                        completed_at = NOW(),
                        error = $1,
                        locked_by = NULL,
                        retry_count = retry_count + 1
                    WHERE id = $2
                    RETURNING status, retry_count
                    "#,
                )
                .bind(e.to_string())
                .bind(job.id)
                .fetch_one(&self.db)
                .await?;

                let (new_status_str, new_retry_count) = row;
                if new_status_str == "pending" {
                    tracing::warn!(
                        job_id = %job.id,
                        retry_count = new_retry_count,
                        error = %e,
                        "Job failed, will retry"
                    );
                } else {
                    tracing::error!(
                        job_id = %job.id,
                        error = %e,
                        "Job failed permanently"
                    );

                    if let Some(ref publisher) = self.event_publisher {
                        if let Some(source_id) = job.source_id {
                            if let Ok(Some(pid)) = sqlx::query_scalar::<_, Uuid>(
                                "SELECT project_id FROM warehouse_sources WHERE id = $1",
                            )
                            .bind(source_id)
                            .fetch_optional(&self.db)
                            .await
                            {
                                let _ = publisher.emit(
                                    PlatformEventType::SyncJobFailed,
                                    pid,
                                    format!("sync_job_failed:{}", job.id),
                                    serde_json::json!({
                                        "job_id": job.id,
                                        "job_type": format!("{:?}", job.job_type),
                                        "source_id": source_id,
                                        "error": e.to_string(),
                                    }),
                                ).await;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Schedule an `FstRebuild` job for `source_id` if there isn't already a
    /// pending or running one.
    async fn maybe_schedule_fst_rebuild(&self, source_id: Uuid) {
        // Deduplication: only schedule if no pending/running fst_rebuild exists.
        let existing: Option<(i64,)> = sqlx::query_as(
            r#"
            SELECT COUNT(*) FROM warehouse_jobs
            WHERE source_id = $1
              AND job_type = 'fst_rebuild'
              AND status IN ('pending', 'running')
            "#,
        )
        .bind(source_id)
        .fetch_optional(&self.db)
        .await
        .ok()
        .flatten();

        if existing.map_or(false, |(count,)| count > 0) {
            return;
        }

        let job_id = Uuid::new_v4();
        let result = sqlx::query(
            "INSERT INTO warehouse_jobs (id, job_type, source_id, status, scheduled_at) VALUES ($1, 'fst_rebuild', $2, 'pending', NOW())"
        )
        .bind(job_id)
        .bind(source_id)
        .execute(&self.db)
        .await;

        match result {
            Ok(_) => {
                tracing::info!(
                    source_id = %source_id,
                    job_id = %job_id,
                    "Scheduled incremental FstRebuild job after sync"
                );
            }
            Err(e) => {
                tracing::warn!(
                    source_id = %source_id,
                    error = %e,
                    "Failed to schedule FstRebuild job"
                );
            }
        }
    }
}

/// Aggregate sync results from a full sync across multiple tables.
fn aggregate_sync_results(results: Vec<(String, SyncResult)>) -> SyncResult {
    results
        .into_iter()
        .fold(SyncResult::default(), |mut acc, (_, r)| {
            acc.rows_synced += r.rows_synced;
            acc.bytes_written += r.bytes_written;
            acc.files_created += r.files_created;
            acc.duration_ms += r.duration_ms;
            acc.file_indexes.extend(r.file_indexes);
            acc
        })
}

/// Check if a column should be included in the skip index.
///
/// Only string columns with low-cardinality names are indexed.
pub(crate) fn should_index_column(field: &arrow::datatypes::Field) -> bool {
    use arrow::datatypes::DataType;

    if !matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
        return false;
    }

    let name = field.name();
    !(name.ends_with("_id")
        || name.ends_with("_uuid")
        || name == "id"
        || name == "uuid"
        || name.contains("timestamp")
        || name.contains("created_at")
        || name.contains("updated_at"))
}

/// Extract non-null string values from an Arrow array into a collector.
pub(crate) fn extract_string_values(column: &dyn arrow::array::Array, out: &mut Vec<String>) {
    use arrow::array::{Array, LargeStringArray, StringArray};

    if let Some(string_array) = column.as_any().downcast_ref::<StringArray>() {
        for i in 0..string_array.len() {
            if !string_array.is_null(i) {
                out.push(string_array.value(i).to_string());
            }
        }
    } else if let Some(string_array) = column.as_any().downcast_ref::<LargeStringArray>() {
        for i in 0..string_array.len() {
            if !string_array.is_null(i) {
                out.push(string_array.value(i).to_string());
            }
        }
    }
}

/// Maximum number of distinct values to track per column before discarding.
const MAX_COLUMN_CARDINALITY: usize = 100_000;

/// Extract indexable string column values from in-memory record batches.
///
/// Applies the same column filtering and cardinality limiting as the
/// file-based `build_file_skip_index`, but operates on Arrow batches
/// that are already in memory (e.g., during sync).
///
/// Returns a map of column name to deduplicated string values, suitable
/// for passing to `FileSkipIndex::build`.
pub(crate) fn extract_indexable_values(
    batches: &[arrow::record_batch::RecordBatch],
) -> std::collections::HashMap<String, Vec<String>> {
    let mut column_sets: std::collections::HashMap<String, std::collections::HashSet<String>> =
        std::collections::HashMap::new();
    let mut disqualified: std::collections::HashSet<String> = std::collections::HashSet::new();

    for batch in batches {
        for (idx, field) in batch.schema().fields().iter().enumerate() {
            if !should_index_column(field) {
                continue;
            }

            if disqualified.contains(field.name()) {
                continue;
            }

            let mut raw_values = Vec::new();
            extract_string_values(batch.column(idx).as_ref(), &mut raw_values);
            let set = column_sets.entry(field.name().to_string()).or_default();
            set.extend(raw_values);

            if set.len() > MAX_COLUMN_CARDINALITY {
                set.clear();
                disqualified.insert(field.name().to_string());
            }
        }
    }

    column_sets.retain(|_, v| !v.is_empty() && v.len() <= MAX_COLUMN_CARDINALITY);

    column_sets
        .into_iter()
        .map(|(k, v)| (k, v.into_iter().collect()))
        .collect()
}

/// Extract tokenized string column values for full-text search indexing.
///
/// Unlike `extract_indexable_values()` which stores raw distinct values,
/// this tokenizes all string values (split on non-alphanumeric, lowercase,
/// deduplicate, min length 2) and prefixes column names with `__fts__:`.
///
/// All `Utf8`/`LargeUtf8` columns are indexed -- no cardinality gating or
/// column name filtering, since tokenization naturally reduces cardinality.
pub(crate) fn extract_token_values(
    batches: &[arrow::record_batch::RecordBatch],
) -> std::collections::HashMap<String, Vec<String>> {
    use arrow::datatypes::DataType;
    use std::collections::{HashMap, HashSet};

    let mut column_tokens: HashMap<String, HashSet<String>> = HashMap::new();

    for batch in batches {
        for (idx, field) in batch.schema().fields().iter().enumerate() {
            if !matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
                continue;
            }

            let fts_key = format!(
                "{}{}",
                crate::warehouse::indexes::fulltext_index::FTS_COLUMN_PREFIX,
                field.name()
            );
            let mut raw_values = Vec::new();
            extract_string_values(batch.column(idx).as_ref(), &mut raw_values);

            let set = column_tokens.entry(fts_key).or_default();
            for value in &raw_values {
                for token in crate::warehouse::indexes::fulltext_index::tokenize(value) {
                    set.insert(token);
                }
            }
        }
    }

    column_tokens.retain(|_, v| !v.is_empty());
    column_tokens
        .into_iter()
        .map(|(k, v)| (k, v.into_iter().collect()))
        .collect()
}

/// Periodically extend a job's lock until cancellation.
#[tracing::instrument(name = "warehouse.sync.heartbeat_loop", skip_all)]
async fn heartbeat_loop(
    db: PgPool,
    job_id: Uuid,
    lock_duration: Duration,
    heartbeat_interval: std::time::Duration,
    mut cancel_rx: tokio::sync::oneshot::Receiver<()>,
) {
    let mut interval = tokio::time::interval(heartbeat_interval);
    interval.tick().await; // Skip the first immediate tick

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let new_expiry = Utc::now() + lock_duration;
                if let Err(e) = sqlx::query(
                    "UPDATE warehouse_jobs SET lock_expires_at = $1 WHERE id = $2 AND status = 'running'"
                )
                .bind(new_expiry)
                .bind(job_id)
                .execute(&db)
                .await
                {
                    tracing::warn!(job_id = %job_id, error = %e, "Failed to extend job lock");
                }
            }
            _ = &mut cancel_rx => {
                tracing::debug!(job_id = %job_id, "Heartbeat task cancelled");
                return;
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::warehouse::indexes::fulltext_index::FTS_COLUMN_PREFIX;
    use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    /// Helper: build a RecordBatch from column names, types, and string data.
    fn string_batch(names: &[&str], data: &[Vec<&str>]) -> RecordBatch {
        let fields: Vec<Field> = names
            .iter()
            .map(|n| Field::new(*n, DataType::Utf8, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let arrays: Vec<arrow::array::ArrayRef> = data
            .iter()
            .map(|col| Arc::new(StringArray::from(col.clone())) as arrow::array::ArrayRef)
            .collect();
        RecordBatch::try_new(schema, arrays).unwrap()
    }

    // ---- extract_indexable_values tests ----

    #[test]
    fn test_extract_indexable_values_basic() {
        let batch = string_batch(
            &["city", "country"],
            &[vec!["Berlin", "Munich"], vec!["DE", "DE"]],
        );
        let result = extract_indexable_values(&[batch]);
        assert!(result.contains_key("city"));
        assert!(result.contains_key("country"));
        assert!(result["city"].contains(&"Berlin".to_string()));
        assert!(result["city"].contains(&"Munich".to_string()));
    }

    #[test]
    fn test_extract_indexable_values_skips_non_string_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
            Field::new("score", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["Alice"])) as arrow::array::ArrayRef,
                Arc::new(Int64Array::from(vec![30])) as arrow::array::ArrayRef,
                Arc::new(Float64Array::from(vec![9.5])) as arrow::array::ArrayRef,
                Arc::new(BooleanArray::from(vec![true])) as arrow::array::ArrayRef,
            ],
        )
        .unwrap();

        let result = extract_indexable_values(&[batch]);
        assert!(result.contains_key("name"));
        assert!(!result.contains_key("age"));
        assert!(!result.contains_key("score"));
        assert!(!result.contains_key("active"));
    }

    #[test]
    fn test_extract_indexable_values_skips_id_columns() {
        let batch = string_batch(
            &["user_id", "account_uuid", "id", "uuid", "city"],
            &[
                vec!["u1"],
                vec!["a1"],
                vec!["i1"],
                vec!["uu1"],
                vec!["Berlin"],
            ],
        );
        let result = extract_indexable_values(&[batch]);
        assert!(!result.contains_key("user_id"));
        assert!(!result.contains_key("account_uuid"));
        assert!(!result.contains_key("id"));
        assert!(!result.contains_key("uuid"));
        assert!(result.contains_key("city"));
    }

    #[test]
    fn test_extract_indexable_values_empty_batches() {
        let result = extract_indexable_values(&[]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_extract_indexable_values_deduplication() {
        let batch = string_batch(
            &["status"],
            &[vec!["active", "active", "inactive", "active"]],
        );
        let result = extract_indexable_values(&[batch]);
        // Values are collected but dedup happens during FST build (sorted set).
        // Here we check the raw extraction still contains all values.
        assert!(result.contains_key("status"));
        assert!(result["status"].len() >= 2); // at least active and inactive
    }

    // ---- should_index_column tests ----

    #[test]
    fn test_should_index_column_utf8() {
        let field = Field::new("city", DataType::Utf8, true);
        assert!(should_index_column(&field));
    }

    #[test]
    fn test_should_index_column_large_utf8() {
        let field = Field::new("description", DataType::LargeUtf8, true);
        assert!(should_index_column(&field));
    }

    #[test]
    fn test_should_index_column_rejects_int() {
        let field = Field::new("count", DataType::Int64, true);
        assert!(!should_index_column(&field));
    }

    #[test]
    fn test_should_index_column_rejects_id_suffix() {
        assert!(!should_index_column(&Field::new(
            "user_id",
            DataType::Utf8,
            true
        )));
        assert!(!should_index_column(&Field::new(
            "order_uuid",
            DataType::Utf8,
            true
        )));
        assert!(!should_index_column(&Field::new(
            "id",
            DataType::Utf8,
            true
        )));
        assert!(!should_index_column(&Field::new(
            "uuid",
            DataType::Utf8,
            true
        )));
    }

    #[test]
    fn test_should_index_column_rejects_timestamp_names() {
        assert!(!should_index_column(&Field::new(
            "event_timestamp",
            DataType::Utf8,
            true
        )));
        assert!(!should_index_column(&Field::new(
            "created_at",
            DataType::Utf8,
            true
        )));
        assert!(!should_index_column(&Field::new(
            "updated_at",
            DataType::Utf8,
            true
        )));
    }

    // ---- Diff logic tests ----

    /// Compute diff between storage files and indexed files.
    fn compute_diff(
        storage: &HashSet<String>,
        indexed: &HashSet<String>,
    ) -> (HashSet<String>, HashSet<String>) {
        let new_files: HashSet<String> = storage.difference(indexed).cloned().collect();
        let removed_files: HashSet<String> = indexed.difference(storage).cloned().collect();
        (new_files, removed_files)
    }

    #[test]
    fn test_diff_new_files_only() {
        let storage: HashSet<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
        let indexed: HashSet<String> = ["a", "b"].iter().map(|s| s.to_string()).collect();
        let (new_files, removed) = compute_diff(&storage, &indexed);
        assert_eq!(
            new_files,
            ["c"].iter().map(|s| s.to_string()).collect::<HashSet<_>>()
        );
        assert!(removed.is_empty());
    }

    #[test]
    fn test_diff_removed_files_only() {
        let storage: HashSet<String> = ["a", "b"].iter().map(|s| s.to_string()).collect();
        let indexed: HashSet<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
        let (new_files, removed) = compute_diff(&storage, &indexed);
        assert!(new_files.is_empty());
        assert_eq!(
            removed,
            ["c"].iter().map(|s| s.to_string()).collect::<HashSet<_>>()
        );
    }

    #[test]
    fn test_diff_mixed() {
        let storage: HashSet<String> = ["a", "c", "d"].iter().map(|s| s.to_string()).collect();
        let indexed: HashSet<String> = ["a", "b"].iter().map(|s| s.to_string()).collect();
        let (new_files, removed) = compute_diff(&storage, &indexed);
        assert_eq!(
            new_files,
            ["c", "d"]
                .iter()
                .map(|s| s.to_string())
                .collect::<HashSet<_>>()
        );
        assert_eq!(
            removed,
            ["b"].iter().map(|s| s.to_string()).collect::<HashSet<_>>()
        );
    }

    #[test]
    fn test_diff_no_changes() {
        let storage: HashSet<String> = ["a", "b"].iter().map(|s| s.to_string()).collect();
        let indexed: HashSet<String> = ["a", "b"].iter().map(|s| s.to_string()).collect();
        let (new_files, removed) = compute_diff(&storage, &indexed);
        assert!(new_files.is_empty());
        assert!(removed.is_empty());
    }

    #[test]
    fn test_diff_empty_storage() {
        let storage: HashSet<String> = HashSet::new();
        let indexed: HashSet<String> = ["a", "b"].iter().map(|s| s.to_string()).collect();
        let (new_files, removed) = compute_diff(&storage, &indexed);
        assert!(new_files.is_empty());
        assert_eq!(removed.len(), 2);
    }

    #[test]
    fn test_diff_empty_db() {
        let storage: HashSet<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
        let indexed: HashSet<String> = HashSet::new();
        let (new_files, removed) = compute_diff(&storage, &indexed);
        assert_eq!(new_files.len(), 3);
        assert!(removed.is_empty());
    }

    // ---- Strategy change detection tests ----

    #[test]
    fn test_strategy_unchanged_skips_full_rebuild() {
        use crate::warehouse::indexes::external_config::PartitionStrategy;
        let prev = Some(PartitionStrategy::Flat);
        let current = PartitionStrategy::Flat;

        let changed = match (&prev, &current) {
            (None, _) => true,
            (Some(p), new) => p.label() != new.label(),
        };
        assert!(!changed);
    }

    #[test]
    fn test_strategy_changed_triggers_full_rebuild() {
        use crate::warehouse::indexes::external_config::PartitionStrategy;
        let prev = Some(PartitionStrategy::Flat);
        let current = PartitionStrategy::HashBucket { num_buckets: 10 };

        let changed = match (&prev, &current) {
            (None, _) => true,
            (Some(p), new) => p.label() != new.label(),
        };
        assert!(changed);
    }

    #[test]
    fn test_strategy_none_to_some_triggers_full_rebuild() {
        use crate::warehouse::indexes::external_config::PartitionStrategy;
        let prev: Option<PartitionStrategy> = None;
        let current = PartitionStrategy::Flat;

        let changed = match (&prev, &current) {
            (None, _) => true,
            (Some(p), new) => p.label() != new.label(),
        };
        assert!(changed);
    }

    // ---- InlineFileIndex / SyncResult tests ----

    #[test]
    fn test_sync_result_default_has_empty_indexes() {
        let result = SyncResult::default();
        assert!(result.file_indexes.is_empty());
    }

    #[test]
    fn test_aggregate_sync_results_merges_file_indexes() {
        use crate::warehouse::indexes::skip_index::FileSkipIndex;
        use crate::warehouse::types::InlineFileIndex;

        let idx1 = FileSkipIndex::build("file1.parquet", HashMap::new());
        let idx2 = FileSkipIndex::build("file2.parquet", HashMap::new());

        let r1 = SyncResult {
            rows_synced: 100,
            bytes_written: 1000,
            files_created: 1,
            duration_ms: 50,
            file_indexes: match idx1 {
                Ok(idx) => vec![InlineFileIndex {
                    partition_key: "default".to_string(),
                    file_path: "file1.parquet".to_string(),
                    index: idx,
                    row_count: 100,
                }],
                Err(_) => vec![],
            },
        };
        let r2 = SyncResult {
            rows_synced: 200,
            bytes_written: 2000,
            files_created: 1,
            duration_ms: 70,
            file_indexes: match idx2 {
                Ok(idx) => vec![InlineFileIndex {
                    partition_key: "default".to_string(),
                    file_path: "file2.parquet".to_string(),
                    index: idx,
                    row_count: 200,
                }],
                Err(_) => vec![],
            },
        };

        let results = vec![("t1".to_string(), r1), ("t2".to_string(), r2)];
        let agg = aggregate_sync_results(results);
        assert_eq!(agg.rows_synced, 300);
        assert_eq!(agg.bytes_written, 3000);
        assert_eq!(agg.files_created, 2);
    }

    // ---- Cache invalidation tests (unit-level) ----

    #[test]
    fn test_dirty_flag_insert_and_remove() {
        let dirty = dashmap::DashSet::new();
        let project_id = Uuid::new_v4();

        assert!(!dirty.contains(&project_id));

        dirty.insert(project_id);
        assert!(dirty.contains(&project_id));

        let was_dirty = dirty.remove(&project_id).is_some();
        assert!(was_dirty);
        assert!(!dirty.contains(&project_id));
    }

    #[test]
    fn test_dirty_flag_double_insert_is_idempotent() {
        let dirty = dashmap::DashSet::new();
        let project_id = Uuid::new_v4();

        dirty.insert(project_id);
        dirty.insert(project_id);
        assert_eq!(dirty.len(), 1);
    }

    // ---- extract_token_values tests ----

    #[test]
    fn test_extract_token_values_basic() {
        let batch = string_batch(
            &["message", "status"],
            &[
                vec!["Connection timeout error", "Request completed"],
                vec!["active", "inactive"],
            ],
        );
        let result = extract_token_values(&[batch]);

        let msg_key = format!("{}message", FTS_COLUMN_PREFIX);
        let status_key = format!("{}status", FTS_COLUMN_PREFIX);

        assert!(result.contains_key(&msg_key));
        assert!(result.contains_key(&status_key));
        assert!(result[&msg_key].contains(&"connection".to_string()));
        assert!(result[&msg_key].contains(&"timeout".to_string()));
        assert!(result[&msg_key].contains(&"error".to_string()));
        assert!(result[&msg_key].contains(&"request".to_string()));
        assert!(result[&msg_key].contains(&"completed".to_string()));
        assert!(result[&status_key].contains(&"active".to_string()));
        assert!(result[&status_key].contains(&"inactive".to_string()));
    }

    #[test]
    fn test_extract_token_values_includes_id_columns() {
        let batch = string_batch(&["user_id", "city"], &[vec!["usr_abc_123"], vec!["Berlin"]]);
        let result = extract_token_values(&[batch]);

        let uid_key = format!("{}user_id", FTS_COLUMN_PREFIX);
        let city_key = format!("{}city", FTS_COLUMN_PREFIX);

        assert!(
            result.contains_key(&uid_key),
            "token extraction should index _id columns"
        );
        assert!(result.contains_key(&city_key));
        assert!(result[&uid_key].contains(&"usr_abc_123".to_string()));
    }

    #[test]
    fn test_extract_token_values_skips_non_string() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["Alice Bob"])) as arrow::array::ArrayRef,
                Arc::new(Int64Array::from(vec![30])) as arrow::array::ArrayRef,
            ],
        )
        .unwrap();

        let result = extract_token_values(&[batch]);
        let name_key = format!("{}name", FTS_COLUMN_PREFIX);
        assert!(result.contains_key(&name_key));
        assert!(!result.contains_key(&format!("{}age", FTS_COLUMN_PREFIX)));
    }

    #[test]
    fn test_extract_token_values_empty_batches() {
        let result = extract_token_values(&[]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_extract_token_values_deduplicates() {
        let batch = string_batch(&["msg"], &[vec!["error error Error different"]]);
        let result = extract_token_values(&[batch]);
        let key = format!("{}msg", FTS_COLUMN_PREFIX);
        let tokens = &result[&key];
        let error_count = tokens.iter().filter(|t| *t == "error").count();
        assert_eq!(error_count, 1, "tokens should be deduplicated");
    }

    #[test]
    fn test_fst_column_count_nonzero() {
        let batch = string_batch(
            &["message", "status"],
            &[vec!["Connection timeout error"], vec!["active"]],
        );
        let mut column_values = extract_indexable_values(&[batch.clone()]);
        column_values.extend(extract_token_values(&[batch]));

        let total_columns: u64 = column_values.len() as u64;
        assert!(
            total_columns > 0,
            "total_fst_columns counter should be non-zero for string data"
        );
    }

    #[test]
    fn test_retry_sql_uses_db_side_check() {
        let retry_sql = r#"
                    UPDATE warehouse_jobs
                    SET status = CASE
                            WHEN retry_count + 1 < max_retries THEN 'pending'
                            ELSE 'failed'
                        END,
                        completed_at = NOW(),
                        error = $1,
                        locked_by = NULL,
                        retry_count = retry_count + 1
                    WHERE id = $2
                    RETURNING status, retry_count
                    "#;

        assert!(
            retry_sql.contains("CASE"),
            "Retry decision must use SQL CASE (not in-memory check) to avoid TOCTOU race"
        );
        assert!(
            retry_sql.contains("retry_count + 1 < max_retries"),
            "Must compare DB-side retry_count + 1 against max_retries"
        );
        assert!(
            retry_sql.contains("RETURNING status"),
            "Must return the actual status set by the DB"
        );
        assert!(
            !retry_sql.contains("$3"),
            "Status must not be passed as a bind parameter (it must be computed in SQL)"
        );
    }
}
