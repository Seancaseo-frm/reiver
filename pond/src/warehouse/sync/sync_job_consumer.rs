//! Sync Job Consumer
//!
//! Kafka consumer for warehouse sync jobs.
//! Processes upgrade_to_warm, upgrade_to_hot, downgrade_to_warm, downgrade_to_cold, index_build, and sync jobs.
//!
//! ARCHITECTURE: Jobs are published to Kafka by the API layer and consumed here.
//! This decouples job creation from execution, enabling horizontal scaling of workers.

use anyhow::Result;
use chrono::Utc;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use sqlx::{PgPool, Row};
use std::sync::Arc;
use tokio::sync::watch;
use tokio_stream::StreamExt;
use tracing::{error, info, warn};
use uuid::Uuid;

use reiver_core::events::{EventPublisher, PlatformEventType};

use crate::crypto::SecretEncrypt;
use crate::kafka::SyncJobKafkaMessage;
use crate::warehouse::indexes::{PartitionManager, delete_table_skip_indexes};
use crate::warehouse::sources::{StorageTier, SyncScope};
use crate::warehouse::storage::clickhouse::ClickHouseStorage;
use crate::warehouse::storage::r2::R2Storage;
use crate::warehouse::types::{JobStatus, JobType, SourceType};

use super::sync_executor::SyncExecutor;
use crate::warehouse::pii_scanner::PiiScanWorker;

/// Simple Kafka context for consumer
pub struct SyncJobConsumerContext;

impl rdkafka::ClientContext for SyncJobConsumerContext {
    fn stats(&self, _stats: rdkafka::Statistics) {
        // Can add metrics/stats reporting here later
    }
}

impl rdkafka::consumer::ConsumerContext for SyncJobConsumerContext {}

/// Kafka configuration for the sync job consumer.
pub struct SyncJobConsumerConfig {
    pub kafka_hosts: String,
    pub sync_jobs_topic: String,
    pub client_id: Option<String>,
}

/// Consumer that processes warehouse sync jobs from Kafka.
///
/// Supports the following job types:
/// - `upgrade_to_warm`: ETL to R2 Parquet + build FST indexes
/// - `upgrade_to_hot`: ETL to ClickHouse native tables
/// - `downgrade_to_warm`: Drop ClickHouse tables, keep Parquet
/// - `downgrade_to_cold`: Delete all cached data, revert to cold
/// - `index_build`: Build FST indexes for external Parquet
/// - `sync`: Incremental sync for warm/hot sources
pub struct SyncJobConsumer {
    consumer: StreamConsumer<SyncJobConsumerContext>,
    db: PgPool,
    encryptor: Arc<dyn SecretEncrypt>,
    r2_storage: Arc<R2Storage>,
    clickhouse_storage: Arc<ClickHouseStorage>,
    partition_manager: Arc<PartitionManager>,
    pii_worker: Arc<PiiScanWorker>,
    derived_table_manager: Option<Arc<crate::warehouse::derived::DerivedTableManager>>,
    event_publisher: Option<Arc<EventPublisher>>,
    shutdown_rx: watch::Receiver<bool>,
}

/// Handle to control the sync job consumer.
pub struct SyncJobConsumerHandle {
    shutdown_tx: watch::Sender<bool>,
}

impl SyncJobConsumerHandle {
    /// Signal the consumer to shut down gracefully.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

impl SyncJobConsumer {
    /// Create a new sync job consumer.
    pub fn new(
        kafka_config: SyncJobConsumerConfig,
        db: PgPool,
        encryptor: Arc<dyn SecretEncrypt>,
        r2_storage: Arc<R2Storage>,
        clickhouse_storage: Arc<ClickHouseStorage>,
        partition_manager: Arc<PartitionManager>,
        derived_table_manager: Option<Arc<crate::warehouse::derived::DerivedTableManager>>,
    ) -> Result<(Self, SyncJobConsumerHandle)> {
        info!("Creating Kafka consumer for sync jobs topic: {}", kafka_config.sync_jobs_topic);

        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &kafka_config.kafka_hosts)
            .set("group.id", "reiver-sync-job-processor")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "30000")
            .set("enable.partition.eof", "false");

        if let Some(ref client_id) = kafka_config.client_id {
            client_config.set("client.id", client_id);
        }

        let consumer: StreamConsumer<SyncJobConsumerContext> = client_config
            .create_with_context(SyncJobConsumerContext)?;

        consumer.subscribe(&[&kafka_config.sync_jobs_topic])?;
        info!("Subscribed to Kafka topic: {}", kafka_config.sync_jobs_topic);

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let pii_worker = Arc::new(PiiScanWorker::new());

        let consumer_instance = Self {
            consumer,
            db,
            encryptor,
            r2_storage,
            clickhouse_storage,
            partition_manager,
            pii_worker,
            derived_table_manager,
            event_publisher: None,
            shutdown_rx,
        };

        let handle = SyncJobConsumerHandle { shutdown_tx };

        Ok((consumer_instance, handle))
    }

    pub fn with_event_publisher(mut self, publisher: Arc<EventPublisher>) -> Self {
        self.event_publisher = Some(publisher);
        self
    }

    /// Start consuming messages from Kafka.
    ///
    /// Runs until a shutdown signal is received.
    #[tracing::instrument(name = "pond.consumer.run", skip(self))]
    pub async fn run(&mut self) -> Result<()> {
        info!("Sync job consumer started");

        let mut message_stream = self.consumer.stream();

        loop {
            tokio::select! {
                message_opt = message_stream.next() => {
                    let Some(message) = message_opt else { break; };
                    match message {
                        Ok(m) => {
                            match self.process_message(&m).await {
                                Ok(()) => {
                                    if let Err(e) = self.consumer.commit_message(&m, rdkafka::consumer::CommitMode::Async) {
                                        error!("Failed to commit Kafka offset: {}", e);
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to process sync job message: {}", e);
                                    // Do NOT commit the offset on failure so the
                                    // message can be re-delivered and retried.
                                }
                            }
                        }
                        Err(e) => {
                            error!("Kafka consumer error: {}", e);
                        }
                    }
                }
                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        info!("Sync job consumer shutting down");
                        return Ok(());
                    }
                }
            }
        }

        Ok(())
    }

    /// Process a single Kafka message.
    #[tracing::instrument(
        name = "warehouse.sync.process_message",
        skip(self, message),
        fields(job_id = tracing::field::Empty, source_id = tracing::field::Empty, job_type = tracing::field::Empty),
        err(Display),
    )]
    async fn process_message<M: Message>(&self, message: &M) -> Result<()> {
        let payload = message.payload().ok_or_else(|| anyhow::anyhow!("Empty payload"))?;
        
        let msg: SyncJobKafkaMessage = serde_json::from_slice(payload)?;

        // Populate span fields now that we have the deserialized message
        let span = tracing::Span::current();
        span.record("job_id", tracing::field::display(&msg.job_id));
        span.record("source_id", tracing::field::display(&msg.source_id));
        span.record("job_type", msg.job_type.as_str());
        
        info!(
            job_id = %msg.job_id,
            job_type = %msg.job_type,
            source_id = %msg.source_id,
            "Processing sync job"
        );

        // Parse job type before setting status to avoid infinite retry on invalid types
        let job_type: JobType = match msg.job_type.parse() {
            Ok(jt) => jt,
            Err(e) => {
                error!(
                    job_id = %msg.job_id,
                    job_type = %msg.job_type,
                    error = %e,
                    "Unknown job type, marking as failed"
                );
                self.update_job_status(msg.job_id, JobStatus::Failed, Some(&e)).await?;
                return Ok(());
            }
        };

        self.update_job_status(msg.job_id, JobStatus::Running, None).await?;

        // Execute the job based on type
        let result = match job_type {
            JobType::UpgradeToWarm => self.execute_upgrade_to_warm(&msg).await,
            JobType::UpgradeToHot => self.execute_upgrade_to_hot(&msg).await,
            JobType::DowngradeToWarm => self.execute_downgrade_to_warm(&msg).await,
            JobType::DowngradeToCold => self.execute_downgrade_to_cold(&msg).await,
            JobType::IndexBuild => {
                tracing::warn!("IndexBuild jobs are deprecated and will be ignored");
                Ok(())
            }
            JobType::Sync => self.execute_sync(&msg).await,
            JobType::DerivedRefresh => self.execute_derived_refresh(&msg).await,
            JobType::FstRebuild | JobType::SchemaSnapshot => {
                Err(anyhow::anyhow!("Job type {} not handled by sync consumer", job_type))
            }
        };

        // Finalize job status
        match result {
            Ok(_) => {
                self.update_job_status(msg.job_id, JobStatus::Completed, None).await?;
                info!(job_id = %msg.job_id, "Job completed successfully");

                if let Some(ref publisher) = self.event_publisher {
                    let _ = publisher.emit(
                        PlatformEventType::SyncJobCompleted,
                        msg.project_id,
                        format!("sync_job_completed:{}", msg.job_id),
                        serde_json::json!({
                            "job_id": msg.job_id,
                            "job_type": msg.job_type,
                            "source_id": msg.source_id,
                        }),
                    ).await;
                }
            }
            Err(e) => {
                let error_msg = e.to_string();
                error!(job_id = %msg.job_id, error = %error_msg, "Job failed");
                self.update_job_status(msg.job_id, JobStatus::Failed, Some(&error_msg)).await?;

                if let Some(ref publisher) = self.event_publisher {
                    let _ = publisher.emit(
                        PlatformEventType::SyncJobFailed,
                        msg.project_id,
                        format!("sync_job_failed:{}", msg.job_id),
                        serde_json::json!({
                            "job_id": msg.job_id,
                            "job_type": msg.job_type,
                            "source_id": msg.source_id,
                            "error": error_msg,
                        }),
                    ).await;
                }
            }
        }

        Ok(())
    }

    /// Update job status in the database.
    #[tracing::instrument(name = "pond.consumer.update_job_status", skip(self, error), fields(%job_id, %status))]
    async fn update_job_status(
        &self,
        job_id: Uuid,
        status: JobStatus,
        error: Option<&str>,
    ) -> Result<()> {
        let now = Utc::now();

        if status == JobStatus::Running {
            sqlx::query(
                "UPDATE warehouse_jobs SET status = $1, started_at = $2 WHERE id = $3"
            )
            .bind(status.to_string())
            .bind(now)
            .bind(job_id)
            .execute(&self.db)
            .await?;
        } else {
            sqlx::query(
                "UPDATE warehouse_jobs SET status = $1, completed_at = $2, error = $3 WHERE id = $4"
            )
            .bind(status.to_string())
            .bind(now)
            .bind(error)
            .bind(job_id)
            .execute(&self.db)
            .await?;
        }

        Ok(())
    }

    /// Update source tier after successful job completion.
    #[tracing::instrument(name = "pond.consumer.update_source_tier", skip(self), fields(%source_id, tier = %new_tier))]
    async fn update_source_tier(
        &self,
        source_id: Uuid,
        new_tier: StorageTier,
        storage_bytes: Option<i64>,
    ) -> Result<()> {
        let now = Utc::now();
        
        match new_tier {
            StorageTier::Warm => {
                sqlx::query(
                    "UPDATE warehouse_sources SET tier = $1, warm_at = $2, 
                     last_sync_at = $2, storage_bytes = COALESCE($3, storage_bytes), 
                     updated_at = $2 WHERE id = $4"
                )
                .bind(new_tier.to_string())
                .bind(now)
                .bind(storage_bytes)
                .bind(source_id)
                .execute(&self.db)
                .await?;
            }
            StorageTier::Hot => {
                sqlx::query(
                    "UPDATE warehouse_sources SET tier = $1, hot_at = $2,
                     last_sync_at = $2, updated_at = $2 WHERE id = $3"
                )
                .bind(new_tier.to_string())
                .bind(now)
                .bind(source_id)
                .execute(&self.db)
                .await?;
            }
            StorageTier::Cold => {
                sqlx::query(
                    "UPDATE warehouse_sources SET tier = $1, warm_at = NULL,
                     hot_at = NULL, storage_bytes = 0, updated_at = $2 WHERE id = $3"
                )
                .bind(new_tier.to_string())
                .bind(now)
                .bind(source_id)
                .execute(&self.db)
                .await?;
            }
        }
        
        Ok(())
    }

    /// Execute an upgrade to warm job: ETL to R2 Parquet + build indexes.
    #[tracing::instrument(
        name = "warehouse.sync.upgrade_to_warm",
        skip(self, msg),
        fields(source_id = %msg.source_id, job_id = %msg.job_id),
        err(Display),
    )]
    async fn execute_upgrade_to_warm(&self, msg: &SyncJobKafkaMessage) -> Result<()> {
        info!(
            source_id = %msg.source_id,
            "Executing upgrade to warm job"
        );

        // Create sync executor
        let executor = SyncExecutor::new(
            self.db.clone(),
            self.r2_storage.clone(),
            self.clickhouse_storage.clone(),
            self.encryptor.clone(),
            self.partition_manager.clone(),
            self.pii_worker.clone(),
        );

        // Execute sync in warm tier (with job_id for transactional tracking)
        let result = executor.sync_source(msg.source_id, StorageTier::Warm, Some(msg.job_id)).await?;

        info!(
            source_id = %msg.source_id,
            tables = result.tables_synced,
            rows = result.total_rows,
            bytes = result.total_bytes,
            "Upgrade to warm job complete"
        );
        
        // Commit sync - mark partitions as committed
        self.commit_sync(msg.job_id, msg.source_id).await?;

        // Update source tier to 'warm'
        self.update_source_tier(msg.source_id, StorageTier::Warm, Some(result.total_bytes as i64)).await?;

        Ok(())
    }

    /// Execute an upgrade to hot job: ETL to ClickHouse.
    /// 
    /// If upgrading from warm tier, imports existing R2 Parquet files
    /// directly into ClickHouse (no need to re-sync from source).
    /// 
    /// CRITICAL: The sync_checkpoint is NOT modified during upgrade.
    /// Parquet data represents a snapshot at checkpoint X; after import,
    /// ClickHouse has the same data at checkpoint X; future syncs continue from checkpoint X.
    #[tracing::instrument(
        name = "warehouse.sync.upgrade_to_hot",
        skip(self, msg),
        fields(source_id = %msg.source_id, job_id = %msg.job_id),
        err(Display),
    )]
    async fn execute_upgrade_to_hot(&self, msg: &SyncJobKafkaMessage) -> Result<()> {
        info!(source_id = %msg.source_id, "Executing upgrade to hot job");

        let source = self.load_source(msg.source_id).await?;
        let current_tier = crate::warehouse::sources::types::parse_storage_tier(&source.get::<String, _>("tier"));
        let source_name: String = source.get("name");
        let project_id: Uuid = source.get("project_id");

        if current_tier == StorageTier::Warm {
            self.upgrade_warm_to_hot(msg, project_id, &source_name).await?;
        } else {
            self.upgrade_cold_to_hot(msg, current_tier).await?;
        }

        Ok(())
    }

    /// Upgrade path: import existing R2 Parquet files into ClickHouse.
    #[tracing::instrument(name = "pond.consumer.upgrade_warm_to_hot", skip(self, msg), fields(source_id = %msg.source_id, %project_id))]
    async fn upgrade_warm_to_hot(
        &self,
        msg: &SyncJobKafkaMessage,
        project_id: Uuid,
        source_name: &str,
    ) -> Result<()> {
        use bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        info!(
            source_id = %msg.source_id,
            "Upgrading from warm - importing R2 Parquet files to ClickHouse"
        );

        let partitions = self.partition_manager
            .list_committed_partitions(msg.source_id)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list committed partitions: {}", e))?;

        if partitions.is_empty() {
            warn!(source_id = %msg.source_id, "No partitions found for warm source");
        }

        let mut tables_imported = 0usize;
        let mut total_rows = 0u64;
        let mut r2_keys_to_delete: Vec<String> = Vec::new();
        let mut created_tables: std::collections::HashSet<String> = std::collections::HashSet::new();

        // Download Parquet files concurrently (up to 8 at a time) for better throughput
        let download_futures: Vec<_> = partitions.iter()
            .filter_map(|p| {
                p.parquet_path.as_ref().map(|path| {
                    let r2 = &self.r2_storage;
                    let path = path.clone();
                    async move { (path.clone(), r2.download(&path).await) }
                })
            })
            .collect();

        let downloaded: Vec<_> = futures::stream::StreamExt::collect::<Vec<_>>(
            futures::stream::StreamExt::buffer_unordered(
                futures::stream::iter(download_futures),
                8,
            ),
        )
        .await;

        // Build a map of path -> data for fast lookup
        let mut downloaded_data: std::collections::HashMap<String, Bytes> = std::collections::HashMap::new();
        for (path, result) in downloaded {
            match result {
                Ok(data) => { downloaded_data.insert(path, Bytes::from(data)); }
                Err(e) => {
                    warn!(source_id = %msg.source_id, parquet_path = %path, error = %e, "Failed to download Parquet file, skipping");
                }
            }
        }

        for partition in &partitions {
            let Some(parquet_path) = &partition.parquet_path else { continue };
            let Some(parquet_data) = downloaded_data.remove(parquet_path) else { continue };

            // Parse Parquet into Arrow RecordBatches
            let reader = match ParquetRecordBatchReaderBuilder::try_new(parquet_data)
                .and_then(|b| b.build())
            {
                Ok(reader) => reader,
                Err(e) => {
                    warn!(parquet_path = %parquet_path, error = %e, "Failed to build Parquet reader, skipping");
                    continue;
                }
            };

            let batches: Vec<arrow::record_batch::RecordBatch> = reader.collect::<Result<Vec<_>, _>>()
                .map_err(|e| anyhow::anyhow!(
                    "Failed to read Parquet batches from {}: {}", parquet_path, e
                ))?;
            if batches.is_empty() { continue; }

            if !created_tables.contains(&partition.table_name) {
                let arrow_schema = batches[0].schema();
                let table_schema = self.arrow_schema_to_table_schema(&arrow_schema);

                self.clickhouse_storage
                    .create_source_tables(project_id, source_name, &[(partition.table_name.clone(), table_schema)])
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to create ClickHouse table: {}", e))?;

                created_tables.insert(partition.table_name.clone());
                tables_imported += 1;
            }

            let rows = self.clickhouse_storage
                .import_from_arrow(project_id, source_name, &partition.table_name, &batches)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to import into ClickHouse: {}", e))?;

            total_rows += rows;
            r2_keys_to_delete.push(parquet_path.clone());
        }

        info!(source_id = %msg.source_id, tables = tables_imported, total_rows = total_rows, "Imported R2 Parquet files to ClickHouse");

        if tables_imported == 0 && !partitions.is_empty() {
            return Err(anyhow::anyhow!(
                "Failed to import any tables during upgrade. {} partitions found but none imported.",
                partitions.len()
            ));
        }

        // Update source tier BEFORE cleanup
        self.update_source_tier(msg.source_id, StorageTier::Hot, None).await?;

        // Collect partition file paths from warehouse_partition_files
        // (multi-file partitions are tracked there, not in partition.parquet_path)
        let partition_ids: Vec<Uuid> = partitions.iter().map(|p| p.id).collect();
        let mut file_list_failures = 0u32;
        for &pid in &partition_ids {
            match self.partition_manager.get_partition_file_paths(pid).await {
                Ok(paths) => r2_keys_to_delete.extend(paths),
                Err(_) => { file_list_failures += 1; }
            }
        }
        if file_list_failures > 0 {
            warn!(source_id = %msg.source_id, failed = file_list_failures, total = partition_ids.len(), "Failed to list some partition files for R2 cleanup");
        }

        // Clean up R2 files
        if !r2_keys_to_delete.is_empty() {
            let count = r2_keys_to_delete.len();
            if let Err(e) = self.r2_storage.delete_objects(&r2_keys_to_delete).await {
                warn!(source_id = %msg.source_id, error = %e, "Failed to delete R2 objects after import");
            } else {
                info!(source_id = %msg.source_id, count = count, "Deleted R2 Parquet files after import");
            }
        }

        // Delete partition records (no longer needed in hot tier)
        TierTransitionCleanup::cleanup_partitions(&self.db, &partition_ids).await;

        // Delete skip indexes for imported tables (warm-tier indexes reference R2 paths that no longer exist)
        let table_names_vec: Vec<String> = created_tables.into_iter().collect();
        let cleanup_results = TierTransitionCleanup::cleanup_skip_indexes_for_tables(&self.db, project_id, &table_names_vec).await;
        let index_cleanup_failures: Vec<_> = cleanup_results.iter()
            .filter(|(_, result)| result.is_err())
            .map(|(name, _)| name.as_str())
            .collect();
        if !index_cleanup_failures.is_empty() {
            warn!(source_id = %msg.source_id, failed_tables = index_cleanup_failures.len(), "Failed to delete skip indexes for some tables during warm-to-hot upgrade");
        }

        info!(source_id = %msg.source_id, "Upgrade to hot complete (imported from R2)");
        Ok(())
    }

    /// Fresh upgrade to hot: sync from source database into ClickHouse.
    #[tracing::instrument(name = "pond.consumer.upgrade_cold_to_hot", skip(self, msg), fields(source_id = %msg.source_id, %current_tier))]
    async fn upgrade_cold_to_hot(&self, msg: &SyncJobKafkaMessage, current_tier: StorageTier) -> Result<()> {
        info!(source_id = %msg.source_id, current_tier = %current_tier, "Syncing from source to ClickHouse (fresh upgrade to hot)");

        let executor = SyncExecutor::new(
            self.db.clone(),
            self.r2_storage.clone(),
            self.clickhouse_storage.clone(),
            self.encryptor.clone(),
            self.partition_manager.clone(),
            self.pii_worker.clone(),
        );

        let result = executor.sync_source(msg.source_id, StorageTier::Hot, Some(msg.job_id)).await?;
        info!(source_id = %msg.source_id, tables = result.tables_synced, rows = result.total_rows, "Upgrade to hot job sync complete");

        self.update_source_tier(msg.source_id, StorageTier::Hot, None).await?;
        Ok(())
    }

    /// Convert Arrow schema to TableSchema for ClickHouse table creation.
    fn arrow_schema_to_table_schema(&self, arrow_schema: &arrow::datatypes::Schema) -> crate::warehouse::types::TableSchema {
        use crate::warehouse::types::{ColumnSchema, ColumnType, TableSchema};
        use arrow::datatypes::DataType;
        
        let columns: Vec<ColumnSchema> = arrow_schema.fields().iter().map(|field| {
            let col_type = match field.data_type() {
                DataType::Utf8 | DataType::LargeUtf8 => ColumnType::String,
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => ColumnType::Int64,
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => ColumnType::Int64,
                DataType::Float32 | DataType::Float64 => ColumnType::Float64,
                DataType::Boolean => ColumnType::Boolean,
                DataType::Timestamp(_, _) => ColumnType::Timestamp,
                DataType::Date32 | DataType::Date64 => ColumnType::Date,
                _ => ColumnType::String, // Default to string for unknown types
            };
            ColumnSchema::new(field.name(), col_type, field.is_nullable())
        }).collect();
        
        TableSchema { columns }
    }

    /// Execute a transactional downgrade to warm job: Export CH to R2, then drop CH.
    /// 
    /// This is transactional: queries continue using ClickHouse until Parquet
    /// data is fully committed. Steps:
    /// 1. Export each ClickHouse table to Parquet in R2 (with sync_state=pending)
    /// 2. Create partition records for each exported table
    /// 3. Commit: mark partitions as committed
    /// 4. Update tier to warm (now queries will use R2)
    /// 5. Drop ClickHouse tables (safe now - R2 data is committed)
    /// 
    /// CRITICAL: The sync_checkpoint is NOT modified during downgrade to warm.
    /// ClickHouse data represents a snapshot at checkpoint X; after export,
    /// R2 has the same data at checkpoint X; future syncs continue from checkpoint X.
    #[tracing::instrument(
        name = "warehouse.sync.downgrade_to_warm",
        skip(self, msg),
        fields(source_id = %msg.source_id, job_id = %msg.job_id),
        err(Display),
    )]
    async fn execute_downgrade_to_warm(&self, msg: &SyncJobKafkaMessage) -> Result<()> {
        info!(source_id = %msg.source_id, job_id = %msg.job_id, "Executing transactional downgrade to warm job");

        let source = self.load_source(msg.source_id).await?;
        let source_name: String = source.get("name");
        let project_id: Uuid = source.get("project_id");
        let current_tier = crate::warehouse::sources::types::parse_storage_tier(&source.get::<String, _>("tier"));

        if current_tier != StorageTier::Hot {
            info!(source_id = %msg.source_id, current_tier = %current_tier, "Source not hot, nothing to downgrade to warm");
            return Ok(());
        }

        // If a warm backing source already has data on R2, skip the CH-to-R2
        // export entirely. Just update the tier and drop the CH tables.
        let has_backing_with_data: bool = match sqlx::query_scalar(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM warehouse_sources ws
                JOIN warehouse_tables wt ON wt.source_id = ws.id
                WHERE ws.backs_source_id = $1 AND ws.tier = 'warm' AND wt.sync_enabled = true
            )
            "#
        )
        .bind(msg.source_id)
        .fetch_one(&self.db)
        .await
        {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(source_id = %msg.source_id, error = %e, "Failed to check for warm backing source, assuming none");
                false
            }
        };

        if has_backing_with_data {
            info!(source_id = %msg.source_id, "Warm backing source exists with data, skipping CH export");
            let now = Utc::now();
            sqlx::query("UPDATE warehouse_sources SET tier = 'warm', hot_at = NULL, updated_at = $1 WHERE id = $2")
                .bind(now).bind(msg.source_id).execute(&self.db).await?;
            self.cleanup_clickhouse_after_downgrade(msg, project_id, &source_name).await;
            return Ok(());
        }

        // Phase 1: list ClickHouse tables
        let ch_table_names = self.clickhouse_storage
            .list_source_tables(project_id, &source_name)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list ClickHouse tables: {}", e))?;

        if ch_table_names.is_empty() {
            let now = Utc::now();
            sqlx::query("UPDATE warehouse_sources SET tier = 'warm', hot_at = NULL, updated_at = $1 WHERE id = $2")
                .bind(now).bind(msg.source_id).execute(&self.db).await?;
            return Ok(());
        }

        // Phase 2: export each table to R2
        let tables_exported = self.export_tables_to_r2(msg, project_id, &source_name, &ch_table_names).await?;

        if tables_exported == 0 && !ch_table_names.is_empty() {
            return Err(anyhow::anyhow!(
                "Failed to export any tables during downgrade to warm. {} tables found but none exported.",
                ch_table_names.len()
            ));
        }

        // Phase 3: commit partitions
        self.commit_sync(msg.job_id, msg.source_id).await?;

        // Phase 4: update tier
        let now = Utc::now();
        sqlx::query("UPDATE warehouse_sources SET tier = 'warm', hot_at = NULL, updated_at = $1 WHERE id = $2")
            .bind(now).bind(msg.source_id).execute(&self.db).await?;
        info!(source_id = %msg.source_id, "Source tier updated to warm");

        // Phase 5: drop ClickHouse tables (safe — R2 data committed)
        self.cleanup_clickhouse_after_downgrade(msg, project_id, &source_name).await;

        // Phase 6: delete stale skip indexes (hot-tier indexes are stale; new warm-tier indexes will be built during next sync)
        let cleanup_results = TierTransitionCleanup::cleanup_skip_indexes_for_tables(&self.db, project_id, &ch_table_names).await;
        let index_cleanup_failures: usize = cleanup_results.iter().filter(|(_, r)| r.is_err()).count();
        if index_cleanup_failures > 0 {
            warn!(source_id = %msg.source_id, failed = index_cleanup_failures, total = ch_table_names.len(), "Failed to delete skip indexes for some tables during hot-to-warm downgrade");
        }

        info!(source_id = %msg.source_id, "Transactional downgrade to warm complete");
        Ok(())
    }

    /// Export all ClickHouse tables for a source directly to R2 as Parquet files.
    ///
    /// Uses ClickHouse's `INSERT INTO FUNCTION s3()` to stream data directly
    /// to R2 without routing through Pond's memory. ClickHouse handles the
    /// Parquet encoding natively.
    ///
    /// Returns the number of tables that were successfully exported.
    #[tracing::instrument(
        name = "warehouse.sync.export_to_r2",
        skip(self, msg, ch_table_names),
        fields(source_id = %msg.source_id, job_id = %msg.job_id, %project_id, %source_name, table_count = ch_table_names.len()),
        err(Display),
    )]
    async fn export_tables_to_r2(
        &self,
        msg: &SyncJobKafkaMessage,
        project_id: Uuid,
        source_name: &str,
        ch_table_names: &[String],
    ) -> Result<usize> {
        let partition_date = Utc::now().date_naive();
        let mut tables_exported = 0usize;

        // Get the named collection for R2 credentials (same one used for reads)
        let bucket = std::env::var("R2_BUCKET").unwrap_or_else(|_| "warehouse".to_string());
        let s3_collection_name = format!("r2_{}", bucket.replace('-', "_"));

        let ch_prefix = format!(
            "warehouse_{}_{}_",
            project_id.to_string().replace('-', "_"),
            crate::warehouse::storage::clickhouse::ClickHouseStorage::sanitize_identifier(source_name)
        );

        for ch_table_name in ch_table_names {
            let table_name = ch_table_name.strip_prefix(&ch_prefix).unwrap_or(ch_table_name.as_str());

            let partition = self.partition_manager
                .get_or_create_partition(msg.source_id, table_name, partition_date)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to get/create partition: {}", e))?;

            let r2_key = format!(
                "projects/{}/warm/{}/{}/{}/{}.parquet",
                project_id, source_name, table_name,
                partition_date.format("%Y-%m-%d"),
                partition.id
            );

            // Let ClickHouse export directly to R2 — no data passes through Pond.
            let row_count = match self.clickhouse_storage
                .export_table_to_s3(
                    project_id,
                    source_name,
                    table_name,
                    &s3_collection_name,
                    &r2_key,
                )
                .await
            {
                Ok(0) => continue, // empty table
                Ok(n) => n,
                Err(e) => {
                    warn!(source_id = %msg.source_id, table = %table_name, error = %e, "Failed to export table to R2, skipping");
                    continue;
                }
            };

            // Get the file size from R2 for bookkeeping (the file was written by ClickHouse)
            let bytes_written = match self.r2_storage.get_object_size(&r2_key).await {
                Ok(size) => size as i64,
                Err(e) => {
                    tracing::warn!(
                        r2_key = %r2_key,
                        error = %e,
                        "Failed to get R2 object size, defaulting to 0 for bookkeeping"
                    );
                    0i64
                }
            };

            self.partition_manager
                .update_partition_data(partition.id, &r2_key, row_count as i64, bytes_written, Some(msg.job_id))
                .await
                .map_err(|e| anyhow::anyhow!("Failed to update partition: {}", e))?;

            let r2_prefix = format!("projects/{}/warm/{}/{}", project_id, source_name, table_name);
            sqlx::query(
                "INSERT INTO warehouse_tables (id, source_id, name, schema, r2_prefix, sync_enabled, sync_state, job_id, created_at, updated_at)
                 VALUES ($1, $2, $3, '{}'::jsonb, $4, true, 'pending', $5, NOW(), NOW())
                 ON CONFLICT (source_id, name) DO UPDATE SET
                    r2_prefix = EXCLUDED.r2_prefix, sync_state = 'pending', job_id = EXCLUDED.job_id, updated_at = NOW()"
            )
            .bind(Uuid::new_v4()).bind(msg.source_id).bind(table_name).bind(&r2_prefix).bind(msg.job_id)
            .execute(&self.db).await?;

            tables_exported += 1;
            info!(source_id = %msg.source_id, table = %table_name, rows = row_count, bytes = bytes_written, "Exported table directly to R2 via ClickHouse s3()");
        }

        Ok(tables_exported)
    }

    /// Best-effort cleanup of ClickHouse tables after downgrade.
    #[tracing::instrument(name = "pond.consumer.cleanup_clickhouse_after_downgrade", skip(self, msg), fields(source_id = %msg.source_id, %project_id))]
    async fn cleanup_clickhouse_after_downgrade(
        &self,
        msg: &SyncJobKafkaMessage,
        project_id: Uuid,
        source_name: &str,
    ) {
        match self.clickhouse_storage.drop_source_tables(project_id, source_name, None).await {
            Ok(dropped) => info!(source_id = %msg.source_id, dropped_count = dropped.len(), "Dropped ClickHouse tables"),
            Err(e) => warn!(source_id = %msg.source_id, error = %e, "Failed to drop ClickHouse tables"),
        }
        if let Err(e) = self.clickhouse_storage.drop_staging_tables(project_id, source_name, None).await {
            warn!(source_id = %msg.source_id, error = %e, "Failed to drop staging tables");
        }
    }

    /// Execute a downgrade to cold job: Delete all cached data.
    #[tracing::instrument(
        name = "warehouse.sync.downgrade_to_cold",
        skip(self, msg),
        fields(source_id = %msg.source_id, job_id = %msg.job_id),
        err(Display),
    )]
    async fn execute_downgrade_to_cold(&self, msg: &SyncJobKafkaMessage) -> Result<()> {
        info!(
            source_id = %msg.source_id,
            "Executing downgrade to cold job"
        );

        // Load source to get info
        let source = self.load_source(msg.source_id).await?;
        let source_name: String = source.get("name");
        let project_id: Uuid = source.get("project_id");
        let tier = crate::warehouse::sources::types::parse_storage_tier(&source.get::<String, _>("tier"));

        // 1. Drop ClickHouse tables (if hot)
        if tier == StorageTier::Hot {
            match self.clickhouse_storage.drop_source_tables(project_id, &source_name, None).await {
                Ok(dropped) => {
                    info!(source_id = %msg.source_id, count = dropped.len(), "Dropped ClickHouse tables");
                }
                Err(e) => {
                    warn!(source_id = %msg.source_id, error = %e, "Failed to drop ClickHouse tables");
                }
            }
        }

        // 2. Delete Parquet files from R2
        let r2_prefix = crate::warehouse::types::warm_source_path(project_id, &source_name);
        match self.r2_storage.list_objects(&r2_prefix).await {
            Ok(objects) => {
                if !objects.is_empty() {
                    let keys: Vec<String> = objects.into_iter().map(|o| o.key).collect();
                    let count = keys.len();
                    if let Err(e) = self.r2_storage.delete_objects(&keys).await {
                        warn!(source_id = %msg.source_id, error = %e, "Failed to delete R2 objects");
                    } else {
                        info!(source_id = %msg.source_id, count = count, "Deleted R2 objects");
                    }
                }
            }
            Err(e) => {
                warn!(source_id = %msg.source_id, error = %e, "Failed to list R2 objects for deletion");
            }
        }

        // 3. Delete local FST indexes
        let index_path = crate::warehouse::types::local_index_source_path(project_id, &source_name);
        if let Err(e) = tokio::fs::remove_dir_all(&index_path).await {
            // Not an error if directory doesn't exist
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!(source_id = %msg.source_id, path = %index_path, error = %e, "Failed to delete local indexes");
            }
        }

        // 4. Delete skip indexes (all data is being removed; indexes must go too)
        let cleanup_results = TierTransitionCleanup::cleanup_skip_indexes_for_source(&self.db, msg.source_id, project_id).await;
        for (table_name, result) in &cleanup_results {
            if let Err(e) = result {
                warn!(source_id = %msg.source_id, table = %table_name, error = %e, "Failed to delete skip indexes during cold downgrade");
            }
        }

        // 5. Update source tier to 'cold'
        self.update_source_tier(msg.source_id, StorageTier::Cold, None).await?;

        Ok(())
    }

    /// Execute a sync job: Incremental sync for warm/hot sources.
    #[tracing::instrument(
        name = "warehouse.sync.incremental",
        skip(self, msg),
        fields(source_id = %msg.source_id, job_id = %msg.job_id, table_name = ?msg.table_name),
        err(Display),
    )]
    async fn execute_sync(&self, msg: &SyncJobKafkaMessage) -> Result<()> {
        info!(
            source_id = %msg.source_id,
            table_name = ?msg.table_name,
            "Executing sync job"
        );

        // Load source to check current tier
        let source = self.load_source(msg.source_id).await?;
        let tier_str: String = source.get("tier");

        // Determine the StorageTier
        let storage_tier: StorageTier = tier_str.parse()
            .map_err(|e: String| anyhow::anyhow!("{}", e))?;
        
        if storage_tier.is_cold() {
            return Err(anyhow::anyhow!(
                "Cannot sync source in tier '{}', must be warm or hot",
                storage_tier
            ));
        }

        // Create sync executor
        let executor = SyncExecutor::new(
            self.db.clone(),
            self.r2_storage.clone(),
            self.clickhouse_storage.clone(),
            self.encryptor.clone(),
            self.partition_manager.clone(),
            self.pii_worker.clone(),
        );

        // Execute sync (will use checkpoint if available, with job_id for transactional tracking)
        let result = executor.sync_source(msg.source_id, storage_tier, Some(msg.job_id)).await?;

        info!(
            source_id = %msg.source_id,
            tier = %storage_tier,
            tables = result.tables_synced,
            rows = result.total_rows,
            bytes = result.total_bytes,
            "Sync job complete"
        );
        
        // Commit sync - mark partitions as committed (for warm tier)
        if storage_tier == StorageTier::Warm {
            self.commit_sync(msg.job_id, msg.source_id).await?;
        }

        // Update last_sync_at timestamp
        let now = Utc::now();
        sqlx::query(
            "UPDATE warehouse_sources SET last_sync_at = $1, updated_at = $1 WHERE id = $2"
        )
        .bind(now)
        .bind(msg.source_id)
        .execute(&self.db)
        .await?;

        Ok(())
    }

    /// Commit sync by marking all pending partitions as committed.
    /// 
    /// This is called after a successful sync to atomically mark all partitions
    /// created by this job as committed. Also deletes old committed partitions
    /// that have been replaced by new ones.
    /// 
    /// All operations are wrapped in a database transaction for atomicity.
    #[tracing::instrument(name = "pond.consumer.commit_sync", skip(self), fields(%job_id, %source_id))]
    async fn commit_sync(&self, job_id: Uuid, source_id: Uuid) -> Result<()> {
        info!(
            job_id = %job_id,
            source_id = %source_id,
            "Committing sync - marking partitions and files as committed"
        );
        
        // Begin transaction for atomic commit
        let mut tx = self.db.begin().await?;
        
        // 1. Mark all pending partitions as committed
        let updated = sqlx::query(
            "UPDATE warehouse_partitions SET sync_state = 'committed' 
             WHERE job_id = $1 AND sync_state = 'pending'"
        )
        .bind(job_id)
        .execute(&mut *tx)
        .await?;
        
        info!(
            job_id = %job_id,
            partitions_committed = updated.rows_affected(),
            "Partitions marked as committed"
        );

        // 2. Commit all pending partition files for this job
        let files_committed = sqlx::query(
            "UPDATE warehouse_partition_files SET sync_state = 'committed' \
             WHERE job_id = $1 AND sync_state = 'pending'"
        )
        .bind(job_id)
        .execute(&mut *tx)
        .await?;

        info!(
            job_id = %job_id,
            files_committed = files_committed.rows_affected(),
            "Partition files marked as committed"
        );

        // 3. Update aggregate stats on logical partitions from their committed files.
        // This replaces the old logic that deleted/replaced partitions.
        sqlx::query(
            r#"
            UPDATE warehouse_partitions p SET
                row_count = COALESCE(agg.total_rows, 0),
                size_bytes = COALESCE(agg.total_bytes, 0),
                last_updated_at = NOW()
            FROM (
                SELECT partition_id,
                       SUM(row_count) AS total_rows,
                       SUM(size_bytes) AS total_bytes
                FROM warehouse_partition_files
                WHERE sync_state = 'committed'
                GROUP BY partition_id
            ) agg
            WHERE p.id = agg.partition_id
              AND p.source_id = $1
            "#,
        )
        .bind(source_id)
        .execute(&mut *tx)
        .await?;
        
        // 4. Mark warehouse_tables as committed for this job
        sqlx::query(
            "UPDATE warehouse_tables SET sync_state = 'committed' 
             WHERE job_id = $1 AND sync_state = 'pending'"
        )
        .bind(job_id)
        .execute(&mut *tx)
        .await?;
        
        // Commit the transaction
        tx.commit().await?;
        
        info!(
            job_id = %job_id,
            source_id = %source_id,
            "Sync committed successfully"
        );
        
        Ok(())
    }

    /// Load source configuration from the database.
    #[tracing::instrument(name = "pond.consumer.load_source", skip(self), fields(%source_id))]
    async fn load_source(&self, source_id: Uuid) -> Result<sqlx::postgres::PgRow> {
        let row = sqlx::query(
            "SELECT id, project_id, name, source_type, config, tier 
             FROM warehouse_sources WHERE id = $1"
        )
        .bind(source_id)
        .fetch_optional(&self.db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Source not found: {}", source_id))?;

        Ok(row)
    }

    /// Execute a derived table refresh job.
    ///
    /// Loads the derived table definition from the database, rewrites its SQL,
    /// and re-materializes the results to R2.
    #[tracing::instrument(
        name = "warehouse.sync.derived_refresh",
        skip(self, msg),
        fields(source_id = %msg.source_id, job_id = %msg.job_id),
        err(Display),
    )]
    async fn execute_derived_refresh(&self, msg: &SyncJobKafkaMessage) -> Result<()> {
        use crate::warehouse::derived::substitute_last_refresh;
        use crate::api::warehouse::{load_project_tables_with_tier, build_table_rewriter_from_env};

        info!(source_id = %msg.source_id, "Executing derived table refresh");

        let manager = self.derived_table_manager.as_ref()
            .ok_or_else(|| anyhow::anyhow!(
                "DerivedTableManager not available; R2/ClickHouse may not be configured"
            ))?;

        let dt = manager
            .get_by_source_id(msg.source_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("No derived table found for source_id {}", msg.source_id))?;

        let effective_sql = if dt.refresh_mode == crate::warehouse::derived::RefreshMode::Incremental {
            substitute_last_refresh(&dt.sql, dt.last_refreshed_at)
        } else {
            dt.sql.clone()
        };

        let (tables, _hot_tables, _hot_backing) = load_project_tables_with_tier(&self.db, dt.project_id)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to load project tables for rewrite: {}", e))?;

        let rewriter = build_table_rewriter_from_env();
        let rewritten_sql = rewriter
            .rewrite(&effective_sql, &tables)
            .map_err(|e| anyhow::anyhow!("Failed to rewrite derived table SQL: {}", e))?;

        let result = manager
            .refresh(&dt, &rewritten_sql)
            .await?;

        info!(
            source_id = %msg.source_id,
            derived_id = %dt.id,
            row_count = result.row_count,
            bytes_written = result.bytes_written,
            files_created = result.files_created,
            duration_ms = result.duration_ms,
            "Derived table refresh complete"
        );

        Ok(())
    }
}

/// Helper for tier transition cleanup operations.
///
/// Encapsulates database cleanup steps (skip indexes, partitions) as independently
/// testable functions. These are pure SQL operations against Postgres, decoupled
/// from ClickHouse/R2/Kafka dependencies.
pub struct TierTransitionCleanup;

impl TierTransitionCleanup {
    /// Delete skip indexes for a list of tables in a project.
    ///
    /// Used after tier transitions when existing skip indexes reference data
    /// that no longer exists (e.g., warm-tier R2 paths after upgrade to hot).
    /// Failures are logged as warnings but do not cause the transition to fail.
    pub async fn cleanup_skip_indexes_for_tables(
        db: &PgPool,
        project_id: Uuid,
        table_names: &[String],
    ) -> Vec<(String, Result<u64, String>)> {
        let mut results = Vec::new();
        for table_name in table_names {
            let result = delete_table_skip_indexes(db, project_id, table_name)
                .await
                .map_err(|e| e.to_string());
            results.push((table_name.clone(), result));
        }
        results
    }

    /// Delete skip indexes for all tables belonging to a source.
    ///
    /// Queries `warehouse_tables` for the source's table names, then deletes
    /// skip indexes for each. Used during cold downgrade when all data is removed.
    pub async fn cleanup_skip_indexes_for_source(
        db: &PgPool,
        source_id: Uuid,
        project_id: Uuid,
    ) -> Vec<(String, Result<u64, String>)> {
        let table_names: Vec<String> = sqlx::query_scalar(
            "SELECT name FROM warehouse_tables WHERE source_id = $1"
        )
        .bind(source_id)
        .fetch_all(db)
        .await
        .unwrap_or_default();

        Self::cleanup_skip_indexes_for_tables(db, project_id, &table_names).await
    }

    /// Delete partition records by their IDs.
    ///
    /// Used during warm-to-hot upgrade when partition records are no longer
    /// needed after data has been imported into ClickHouse.
    pub async fn cleanup_partitions(
        db: &PgPool,
        partition_ids: &[Uuid],
    ) -> Vec<(Uuid, bool)> {
        if partition_ids.is_empty() {
            return Vec::new();
        }

        match sqlx::query("DELETE FROM warehouse_partitions WHERE id = ANY($1)")
            .bind(partition_ids)
            .execute(db)
            .await
        {
            Ok(_) => partition_ids.iter().map(|&id| (id, true)).collect(),
            Err(e) => {
                tracing::error!(error = %e, "Batch partition delete failed, falling back to individual deletes");
                let mut results = Vec::new();
                for &partition_id in partition_ids {
                    let success = sqlx::query("DELETE FROM warehouse_partitions WHERE id = $1")
                        .bind(partition_id)
                        .execute(db)
                        .await
                        .is_ok();
                    results.push((partition_id, success));
                }
                results
            }
        }
    }

    /// Count skip indexes for a specific project and table.
    ///
    /// Useful for verifying cleanup in tests.
    pub async fn count_skip_indexes(
        db: &PgPool,
        project_id: Uuid,
        table_name: &str,
    ) -> Result<i64> {
        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM warehouse_skip_indexes WHERE project_id = $1 AND table_name = $2"
        )
        .bind(project_id)
        .bind(table_name)
        .fetch_one(db)
        .await?;
        Ok(count.0)
    }

    /// Get the current tier of a source.
    ///
    /// Useful for verifying state in tests.
    pub async fn get_source_tier(
        db: &PgPool,
        source_id: Uuid,
    ) -> Result<String> {
        let tier: (String,) = sqlx::query_as(
            "SELECT tier FROM warehouse_sources WHERE id = $1"
        )
        .bind(source_id)
        .fetch_one(db)
        .await?;
        Ok(tier.0)
    }
}

/// Decrypted source configuration for use in job handlers.
#[derive(Debug)]
pub struct DecryptedSourceConfig {
    pub id: Uuid,
    pub project_id: Uuid,
    pub name: String,
    pub source_type: SourceType,
    pub tier: StorageTier,
    pub sync_scope: SyncScope,
    pub config: Option<serde_json::Value>,
}
