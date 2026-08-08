//! Sync Worker
//!
//! Executes data syncs from connectors to storage (ClickHouse or R2).
//!
//! Supports two storage backends:
//! - **Native ClickHouse**: Best performance, syncs directly to MergeTree tables
//! - **Object Storage (R2/S3)**: Uses chunked Parquet writing for large datasets
//!
//! PERFORMANCE: Uses chunked Parquet writing for large datasets to prevent OOM
//! and enable parallel uploads to R2.

use std::time::Instant;

/// Sync status values used in metrics and logging.
const SYNC_STATUS_SUCCESS: &str = "success";
const SYNC_STATUS_PARQUET_ERROR: &str = "parquet_error";
const SYNC_STATUS_STORAGE_ERROR: &str = "storage_error";

use crate::warehouse::connectors::Connector;
use crate::warehouse::indexes::skip_index::FileSkipIndex;
use crate::warehouse::metrics::WarehouseMetrics;
use crate::warehouse::parquet::{write_parquet_chunked, ChunkedWriteOptions, ChunkedWriteResult};
use crate::warehouse::parquet_stats::extract_stats_from_parquet_bytes;
use crate::warehouse::storage::{ClickHouseStorage, R2Storage};
use crate::warehouse::types::{
    InlineFileIndex, R2TablePath, SourceType, StorageType, SyncResult, TableSchema,
};

use chrono::Utc;
use futures::stream::{self, StreamExt};
use thiserror::Error;

/// Maximum number of tables to sync in parallel.
/// This limits concurrent API calls and memory usage.
const MAX_CONCURRENT_TABLE_SYNCS: usize = 4;

/// Maximum concurrent chunk uploads for large syncs.
const MAX_CONCURRENT_CHUNK_UPLOADS: usize = 4;

/// Errors that can occur during sync execution.
#[derive(Debug, Error)]
pub enum SyncWorkerError {
    #[error("Connector error: {0}")]
    Connector(#[from] crate::warehouse::connectors::ConnectorError),

    #[error("Storage error: {0}")]
    Storage(#[from] crate::warehouse::storage::r2::R2Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] crate::warehouse::parquet::ParquetError),

    #[error("ClickHouse storage error: {0}")]
    ClickHouse(#[from] crate::warehouse::storage::clickhouse::ClickHouseStorageError),
}

/// Result type for sync operations.
pub type SyncWorkerResult<T> = Result<T, SyncWorkerError>;

/// Storage backend for sync operations.
pub enum SyncStorage<'a> {
    /// Native ClickHouse storage (best performance).
    ClickHouse(&'a ClickHouseStorage),
    /// Object storage (R2/S3) with Parquet files.
    ObjectStorage(&'a R2Storage),
}

/// Run a sync operation to the appropriate storage backend.
///
/// Dispatches to either native ClickHouse or R2 based on storage_type.
pub async fn run_sync_to_storage(
    project_id: uuid::Uuid,
    source_type: SourceType,
    storage_type: StorageType,
    connector: &dyn Connector,
    storage: SyncStorage<'_>,
    table: &str,
    table_schema: Option<&TableSchema>,
    incremental_key: Option<&str>,
    last_value: Option<&str>,
    metrics: &WarehouseMetrics,
) -> SyncWorkerResult<SyncResult> {
    match (storage_type, storage) {
        (StorageType::NativeClickHouse, SyncStorage::ClickHouse(ch_storage)) => {
            run_sync_to_clickhouse(
                project_id,
                source_type,
                connector,
                ch_storage,
                table,
                table_schema,
                incremental_key,
                last_value,
                metrics,
            )
            .await
        }
        (StorageType::ObjectStorage, SyncStorage::ObjectStorage(r2_storage)) => {
            run_sync(
                project_id,
                source_type,
                connector,
                r2_storage,
                table,
                incremental_key,
                last_value,
                metrics,
            )
            .await
        }
        _ => {
            // Mismatch between storage type and provided storage backend
            tracing::error!(
                storage_type = %storage_type,
                "Storage type mismatch with provided storage backend"
            );
            Err(SyncWorkerError::ClickHouse(
                crate::warehouse::storage::clickhouse::ClickHouseStorageError::Connection(
                    "Storage type mismatch".to_string(),
                ),
            ))
        }
    }
}

/// Run a sync operation to native ClickHouse.
///
/// PERFORMANCE: Directly inserts data into MergeTree tables for best query performance.
/// ClickHouse handles indexing, sorting, and merges automatically.
pub async fn run_sync_to_clickhouse(
    project_id: uuid::Uuid,
    source_type: SourceType,
    connector: &dyn Connector,
    storage: &ClickHouseStorage,
    table: &str,
    table_schema: Option<&TableSchema>,
    incremental_key: Option<&str>,
    last_value: Option<&str>,
    metrics: &WarehouseMetrics,
) -> SyncWorkerResult<SyncResult> {
    let start_time = Instant::now();
    let source_type_str = source_type.to_string();

    // 1. Fetch data from source
    let batches = match connector
        .fetch_table(table, incremental_key, last_value)
        .await
    {
        Ok(batches) => batches,
        Err(e) => {
            let duration_ms = start_time.elapsed().as_millis() as u64;
            emit_sync_error_metrics(metrics, &source_type_str, &e, duration_ms);
            return Err(e.into());
        }
    };

    // Handle empty tables
    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        tracing::debug!(table = table, "No data to sync - table is empty");
        let duration_ms = start_time.elapsed().as_millis() as u64;
        emit_sync_success_metrics(metrics, &source_type_str, 0, 0, duration_ms);
        return Ok(SyncResult {
            rows_synced: 0,
            bytes_written: 0,
            files_created: 0,
            duration_ms,
            file_indexes: Vec::new(),
        });
    }

    // 2. Ensure table exists in ClickHouse
    if let Some(schema) = table_schema {
        if !storage.table_exists(project_id, table).await? {
            storage
                .create_table(project_id, table, schema, None)
                .await?;
        }
    }

    // 3. Insert batches into ClickHouse
    let mut total_rows: u64 = 0;
    let mut bytes_written: u64 = 0;

    for batch in &batches {
        let rows = storage.insert_batch(project_id, table, batch).await?;
        total_rows += rows;
        // Estimate bytes (Arrow doesn't expose exact size easily)
        bytes_written += batch.get_array_memory_size() as u64;
    }

    let duration_ms = start_time.elapsed().as_millis() as u64;

    emit_sync_success_metrics(
        metrics,
        &source_type_str,
        total_rows,
        bytes_written,
        duration_ms,
    );

    tracing::info!(
        project_id = %project_id,
        source_type = source_type_str,
        table = table,
        rows = total_rows,
        duration_ms = duration_ms,
        "Synced data to native ClickHouse"
    );

    Ok(SyncResult {
        rows_synced: total_rows,
        bytes_written,
        files_created: 0,
        duration_ms,
        file_indexes: Vec::new(),
    })
}

/// Run a sync operation.
///
/// 1. Fetches data from the connector
/// 2. Converts to Parquet format (chunked for large datasets)
/// 3. Uploads to R2 storage (parallel uploads for multiple chunks)
/// 4. Returns sync statistics
///
/// SECURITY: The project_id is required to ensure data isolation. All files
/// are written under the project's prefix in R2.
///
/// PERFORMANCE: Uses chunked Parquet writing to prevent OOM for large datasets.
/// Files are split at ~256MB and uploaded in parallel.
///
/// Emits metrics via `emit_sync_metrics` / `emit_sync_success_metrics` which
/// record counters through `WarehouseMetrics` and structured tracing events
/// (target `warehouse_metrics`) with fields `warehouse_sync_total`,
/// `warehouse_sync_duration_ms`, `warehouse_sync_rows_synced`, and
/// `warehouse_sync_bytes_written` (tags: source_type, status).
pub async fn run_sync(
    project_id: uuid::Uuid,
    source_type: SourceType,
    connector: &dyn Connector,
    storage: &R2Storage,
    table: &str,
    incremental_key: Option<&str>,
    last_value: Option<&str>,
    metrics: &WarehouseMetrics,
) -> SyncWorkerResult<SyncResult> {
    let start_time = Instant::now();
    let source_type_str = source_type.to_string();

    // 1. Fetch data from source
    let batches = match connector
        .fetch_table(table, incremental_key, last_value)
        .await
    {
        Ok(batches) => batches,
        Err(e) => {
            let duration_ms = start_time.elapsed().as_millis() as u64;
            emit_sync_error_metrics(metrics, &source_type_str, &e, duration_ms);
            return Err(e.into());
        }
    };

    // Handle empty tables - this is a valid state, not an error
    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        let duration_ms = start_time.elapsed().as_millis() as u64;
        tracing::debug!(table = table, "No data to sync - table is empty");
        emit_sync_success_metrics(metrics, &source_type_str, 0, 0, duration_ms);
        return Ok(SyncResult {
            rows_synced: 0,
            bytes_written: 0,
            files_created: 0,
            duration_ms,
            file_indexes: Vec::new(),
        });
    }

    // 2. Calculate total rows
    let total_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();

    // 3. Convert to Parquet using chunked writing for large datasets
    let schema = batches[0].schema();

    // Use chunked options optimized for large datasets
    let chunked_options = if total_rows > 100_000 {
        ChunkedWriteOptions::for_large_datasets()
    } else {
        ChunkedWriteOptions::default()
    };

    let write_result = match write_parquet_chunked(schema, &batches, chunked_options) {
        Ok(result) => result,
        Err(e) => {
            let duration_ms = start_time.elapsed().as_millis() as u64;
            emit_sync_metrics(
                metrics,
                &source_type_str,
                SYNC_STATUS_PARQUET_ERROR,
                0,
                0,
                duration_ms,
            );
            return Err(e.into());
        }
    };

    let bytes_written = write_result.total_bytes as u64;
    let files_created = write_result.chunks.len() as u32;

    // 4. Generate base path with timestamp
    // SECURITY: Use with_project() to ensure data isolation between projects
    let timestamp = Utc::now().format("%Y-%m-%d_%H%M%S");
    let r2_path = R2TablePath::with_project(project_id, source_type, table);

    // 5. Upload chunks to R2 (parallel for multiple chunks)
    if write_result.chunks.len() == 1 {
        // Single file - simple upload with stats sidecar
        let chunk = &write_result.chunks[0];
        let object_key = format!("{}/{}.parquet", r2_path.prefix, timestamp);

        let stats = extract_stats_from_parquet_bytes(&chunk.data).ok();
        let upload_result = if let Some(ref stats) = stats {
            storage
                .upload_parquet_with_stats(&object_key, chunk.data.clone(), stats)
                .await
        } else {
            storage
                .upload_parquet(&object_key, chunk.data.clone())
                .await
        };
        if let Err(e) = upload_result {
            let duration_ms = start_time.elapsed().as_millis() as u64;
            emit_sync_metrics(
                metrics,
                &source_type_str,
                SYNC_STATUS_STORAGE_ERROR,
                total_rows,
                bytes_written,
                duration_ms,
            );
            return Err(e.into());
        }
    } else {
        // Multiple chunks - parallel upload with stats sidecars
        tracing::info!(
            table = table,
            chunks = write_result.chunks.len(),
            total_bytes = bytes_written,
            "Uploading multiple Parquet chunks"
        );

        let upload_futures = write_result.chunks.iter().map(|chunk| {
            let object_key = format!(
                "{}/{}_part{:04}.parquet",
                r2_path.prefix, timestamp, chunk.chunk_index
            );
            let data = chunk.data.clone();
            let chunk_stats = extract_stats_from_parquet_bytes(&data).ok();

            async move {
                let result = if let Some(ref stats) = chunk_stats {
                    storage
                        .upload_parquet_with_stats(&object_key, data, stats)
                        .await
                } else {
                    storage.upload_parquet(&object_key, data).await
                };
                result.map(|_| object_key)
            }
        });

        let results: Vec<_> = stream::iter(upload_futures)
            .buffer_unordered(MAX_CONCURRENT_CHUNK_UPLOADS)
            .collect()
            .await;

        let (uploaded_keys, upload_errors) = collect_upload_results(results);

        if !upload_errors.is_empty() {
            let total_chunks = uploaded_keys.len() + upload_errors.len();
            tracing::warn!(
                table = table,
                failed = upload_errors.len(),
                total = total_chunks,
                first_error = %upload_errors[0],
                "Chunk uploads failed"
            );
            // Clean up partially uploaded files
            for key in &uploaded_keys {
                if let Err(del_err) = storage.delete(key).await {
                    tracing::debug!(key = %key, error = %del_err, "Best-effort cleanup of partial upload failed");
                }
            }
            let duration_ms = start_time.elapsed().as_millis() as u64;
            emit_sync_metrics(
                metrics,
                &source_type_str,
                SYNC_STATUS_STORAGE_ERROR,
                total_rows,
                bytes_written,
                duration_ms,
            );
            let msg = format!(
                "{} of {} chunk uploads failed: {}",
                upload_errors.len(),
                total_chunks,
                upload_errors[0]
            );
            return Err(crate::warehouse::storage::r2::R2Error::Operation(msg).into());
        }
    }

    // ---- Build inline skip indexes from the batches still in memory ----
    let file_indexes =
        build_inline_indexes(&batches, &r2_path, &write_result, &timestamp.to_string());

    let final_duration_ms = start_time.elapsed().as_millis() as u64;
    emit_sync_success_metrics(
        metrics,
        &source_type_str,
        total_rows,
        bytes_written,
        final_duration_ms,
    );

    Ok(SyncResult {
        rows_synced: total_rows,
        bytes_written,
        files_created,
        duration_ms: final_duration_ms,
        file_indexes,
    })
}

/// Build inline skip indexes from record batches for freshly written Parquet files.
///
/// This is best-effort: index building failures are logged but do not
/// propagate as errors. The caller can persist the returned indexes later.
fn build_inline_indexes(
    batches: &[arrow::record_batch::RecordBatch],
    r2_path: &R2TablePath,
    write_result: &ChunkedWriteResult,
    timestamp: &str,
) -> Vec<InlineFileIndex> {
    use crate::warehouse::sync::job_worker::{extract_indexable_values, extract_token_values};

    let mut indexes = Vec::new();
    let partition_key = "default".to_string();

    if write_result.chunks.len() == 1 {
        let mut column_values = extract_indexable_values(batches);
        column_values.extend(extract_token_values(batches));
        if column_values.is_empty() {
            return Vec::new();
        }
        let file_path = format!("{}/{}.parquet", r2_path.prefix, timestamp);
        let row_count = write_result.chunks[0].num_rows as u64;

        match FileSkipIndex::build(&file_path, column_values) {
            Ok(index) => {
                indexes.push(InlineFileIndex {
                    partition_key,
                    file_path,
                    index,
                    row_count,
                });
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to build inline skip index for file, skipping");
            }
        }
    } else {
        for chunk in &write_result.chunks {
            let file_path = format!(
                "{}/{}_part{:04}.parquet",
                r2_path.prefix, timestamp, chunk.chunk_index
            );
            let row_count = chunk.num_rows as u64;

            let chunk_batches = match crate::warehouse::sync::merge::read_parquet_bytes(&chunk.data)
            {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to re-read chunk for index building, skipping");
                    continue;
                }
            };

            let mut column_values = extract_indexable_values(&chunk_batches);
            column_values.extend(extract_token_values(&chunk_batches));
            if column_values.is_empty() {
                continue;
            }

            match FileSkipIndex::build(&file_path, column_values) {
                Ok(index) => {
                    indexes.push(InlineFileIndex {
                        partition_key: partition_key.clone(),
                        file_path,
                        index,
                        row_count,
                    });
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to build inline skip index for file, skipping");
                }
            }
        }
    }

    indexes
}

/// Partition upload results into successful keys and error messages.
///
/// Collects all errors rather than keeping only the last one, so callers
/// can report accurate failure counts and include the first error message.
fn collect_upload_results(
    results: Vec<Result<String, crate::warehouse::storage::r2::R2Error>>,
) -> (Vec<String>, Vec<String>) {
    let mut keys = Vec::new();
    let mut errors = Vec::new();
    for result in results {
        match result {
            Ok(key) => keys.push(key),
            Err(e) => errors.push(e.to_string()),
        }
    }
    (keys, errors)
}

/// Emit metrics for a successful sync operation.
///
/// Uses low-cardinality tags only (source_type, status) per workspace rules.
/// Note: Table names are NOT included as tags to avoid high-cardinality metrics.
fn emit_sync_success_metrics(
    metrics: &WarehouseMetrics,
    source_type: &str,
    rows_synced: u64,
    bytes_written: u64,
    duration_ms: u64,
) {
    emit_sync_metrics(
        metrics,
        source_type,
        SYNC_STATUS_SUCCESS,
        rows_synced,
        bytes_written,
        duration_ms,
    );
}

/// Emit metrics for sync operations.
///
/// These structured log events can be picked up by observability tools
/// (Datadog, OpenTelemetry, etc.) and converted to metrics.
fn emit_sync_metrics(
    metrics: &WarehouseMetrics,
    source_type: &str,
    status: &str,
    rows_synced: u64,
    bytes_written: u64,
    duration_ms: u64,
) {
    let success = status == SYNC_STATUS_SUCCESS;
    if success {
        metrics.record_sync(rows_synced, bytes_written, true);
    } else {
        metrics.record_sync(0, 0, false);
    }

    // Use tracing events with structured data for metrics
    // These can be aggregated by observability backends
    tracing::info!(
        target: "warehouse_metrics",
        warehouse_sync_total = 1_u64,
        warehouse_sync_duration_ms = duration_ms,
        warehouse_sync_rows_synced = rows_synced,
        warehouse_sync_bytes_written = bytes_written,
        source_type = source_type,
        status = status,
        "Warehouse sync completed"
    );
}

/// Emit metrics for sync errors.
fn emit_sync_error_metrics(
    metrics: &WarehouseMetrics,
    source_type: &str,
    error: &crate::warehouse::connectors::ConnectorError,
    duration_ms: u64,
) {
    metrics.record_sync(0, 0, false);

    // Classify error type for low-cardinality tagging
    let error_type = match error {
        crate::warehouse::connectors::ConnectorError::Authentication(_) => "authentication",
        crate::warehouse::connectors::ConnectorError::RateLimited { .. } => "rate_limited",
        crate::warehouse::connectors::ConnectorError::TableNotFound(_) => "table_not_found",
        crate::warehouse::connectors::ConnectorError::Network(_) => "network",
        crate::warehouse::connectors::ConnectorError::Validation(_) => "validation",
        crate::warehouse::connectors::ConnectorError::Config(_) => "config",
        crate::warehouse::connectors::ConnectorError::Internal(_) => "internal",
        crate::warehouse::connectors::ConnectorError::OAuthExpired(_) => "oauth_expired",
        crate::warehouse::connectors::ConnectorError::StreamEnded(_) => "stream_ended",
        crate::warehouse::connectors::ConnectorError::SchemaInference(_) => "schema_inference",
        crate::warehouse::connectors::ConnectorError::UnsupportedFormat(_) => "unsupported_format",
        crate::warehouse::connectors::ConnectorError::BlockchainRpc(_) => "blockchain_rpc",
    };

    tracing::info!(
        target: "warehouse_metrics",
        warehouse_sync_errors_total = 1_u64,
        warehouse_sync_duration_ms = duration_ms,
        source_type = source_type,
        status = "error",
        error_type = error_type,
        "Warehouse sync failed"
    );
}

/// Run a full sync for all tables in a source.
///
/// Tables are synced in parallel with controlled concurrency to improve
/// throughput while limiting resource usage.
///
/// SECURITY: The project_id is required to ensure data isolation.
pub async fn run_full_sync(
    project_id: uuid::Uuid,
    source_type: SourceType,
    connector: &dyn Connector,
    storage: &R2Storage,
    metrics: &WarehouseMetrics,
) -> SyncWorkerResult<Vec<(String, SyncResult)>> {
    run_full_sync_with_concurrency(
        project_id,
        source_type,
        connector,
        storage,
        MAX_CONCURRENT_TABLE_SYNCS,
        metrics,
    )
    .await
}

/// Run a full sync with configurable concurrency.
///
/// # Arguments
/// * `project_id` - The project ID for data isolation
/// * `source_type` - The type of data source
/// * `connector` - The connector to fetch data from
/// * `storage` - R2 storage for uploading parquet files
/// * `max_concurrent` - Maximum number of tables to sync in parallel
pub async fn run_full_sync_with_concurrency(
    project_id: uuid::Uuid,
    source_type: SourceType,
    connector: &dyn Connector,
    storage: &R2Storage,
    max_concurrent: usize,
    metrics: &WarehouseMetrics,
) -> SyncWorkerResult<Vec<(String, SyncResult)>> {
    let tables = connector.list_tables().await?;

    if tables.is_empty() {
        return Ok(Vec::new());
    }

    tracing::info!(
        table_count = tables.len(),
        max_concurrent = max_concurrent,
        "Starting parallel table sync"
    );

    // Create futures for each table sync
    let sync_futures = tables.into_iter().map(|table_info| {
        let table_name = table_info.name.clone();
        let incremental_key = table_info.incremental_key.clone();

        async move {
            let result = run_sync(
                project_id,
                source_type,
                connector,
                storage,
                &table_name,
                incremental_key.as_deref(),
                None,
                metrics,
            )
            .await;

            match result {
                Ok(sync_result) => {
                    if sync_result.rows_synced == 0 {
                        tracing::debug!(table = %table_name, "Table is empty, no data to sync");
                    }
                    Some((table_name, sync_result))
                }
                Err(e) => {
                    tracing::error!(table = %table_name, error = %e, "Failed to sync table");
                    // Return None for failed tables - we continue with others
                    None
                }
            }
        }
    });

    // Execute with controlled concurrency using buffer_unordered
    // This runs up to max_concurrent syncs in parallel
    let results: Vec<(String, SyncResult)> = stream::iter(sync_futures)
        .buffer_unordered(max_concurrent)
        .filter_map(|result| async { result })
        .collect()
        .await;

    tracing::info!(
        successful_tables = results.len(),
        "Parallel table sync complete"
    );

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collect_upload_results_all_success() {
        let results: Vec<Result<String, crate::warehouse::storage::r2::R2Error>> =
            vec![Ok("key1".into()), Ok("key2".into()), Ok("key3".into())];
        let (keys, errors) = collect_upload_results(results);
        assert_eq!(keys, vec!["key1", "key2", "key3"]);
        assert!(errors.is_empty());
    }

    #[test]
    fn test_collect_upload_results_all_errors() {
        let results: Vec<Result<String, crate::warehouse::storage::r2::R2Error>> = vec![
            Err(crate::warehouse::storage::r2::R2Error::Operation(
                "err1".into(),
            )),
            Err(crate::warehouse::storage::r2::R2Error::Operation(
                "err2".into(),
            )),
        ];
        let (keys, errors) = collect_upload_results(results);
        assert!(keys.is_empty());
        assert_eq!(errors.len(), 2);
        assert!(errors[0].contains("err1"));
        assert!(errors[1].contains("err2"));
    }

    #[test]
    fn test_collect_upload_results_mixed() {
        let results: Vec<Result<String, crate::warehouse::storage::r2::R2Error>> = vec![
            Ok("key1".into()),
            Err(crate::warehouse::storage::r2::R2Error::Operation(
                "err1".into(),
            )),
            Ok("key2".into()),
            Err(crate::warehouse::storage::r2::R2Error::Operation(
                "err2".into(),
            )),
            Err(crate::warehouse::storage::r2::R2Error::Operation(
                "err3".into(),
            )),
        ];
        let (keys, errors) = collect_upload_results(results);
        assert_eq!(keys, vec!["key1", "key2"]);
        assert_eq!(
            errors.len(),
            3,
            "all errors must be captured, not just the last one"
        );
        assert!(errors[0].contains("err1"));
    }
}
