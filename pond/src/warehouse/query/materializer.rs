//! Streaming Materializer
//!
//! Executes a SQL query against ClickHouse via the native TCP protocol
//! (klickhouse), converts result blocks into Arrow `RecordBatch`es, and
//! writes them to R2 as Parquet files with column-level statistics sidecars.

use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use uuid::Uuid;

use crate::warehouse::ch_client::{block_to_record_batch, NativePool};
use crate::warehouse::parquet::WriteOptions;
use crate::warehouse::parquet_stats::write_parquet_with_stats;
use crate::warehouse::storage::r2::R2Storage;

/// Options controlling how a query is materialized to Parquet in R2.
#[derive(Debug, Clone)]
pub struct MaterializeOptions {
    /// Project ID for R2 path isolation.
    pub project_id: Uuid,
    /// Destination table name (used in R2 key prefix).
    pub table_name: String,
    /// Target Parquet file size in bytes before flushing (~64 MB default).
    pub target_file_size: usize,
    /// Refresh version tag for atomic swap on full refresh.
    /// Typically a timestamp like `20260215T120000`.
    pub refresh_version: String,
    /// Maximum ArrowStream response size in bytes before aborting.
    /// Prevents unbounded memory growth from very large materializations.
    /// Defaults to 2 GB.
    pub max_response_bytes: usize,
    /// Maximum in-memory pending batch bytes before forcing a flush.
    /// Prevents OOM when rows are wide and `target_file_size` takes long to reach.
    /// Defaults to 256 MB.
    pub max_pending_memory_bytes: usize,
}

impl MaterializeOptions {
    /// Default target file size: 64 MB.
    pub const DEFAULT_TARGET_FILE_SIZE: usize = 64 * 1024 * 1024;

    /// Default max response size: 2 GB.
    pub const DEFAULT_MAX_RESPONSE_BYTES: usize = 2 * 1024 * 1024 * 1024;

    /// Default max pending memory: 256 MB.
    pub const DEFAULT_MAX_PENDING_MEMORY_BYTES: usize = 256 * 1024 * 1024;

    /// Build the R2 key prefix for this materialization.
    ///
    /// The table_name is validated by `validate_table_name` (alphanumeric + underscore only)
    /// before reaching this point, preventing path traversal attacks.
    ///
    /// Returns an error (instead of panicking) if the table name contains
    /// unexpected characters — defence-in-depth that won't crash the server.
    pub fn r2_prefix(&self) -> Result<String> {
        if !self.table_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            anyhow::bail!(
                "BUG: table_name '{}' contains invalid characters; it must be validated before creating MaterializeOptions",
                self.table_name
            );
        }
        Ok(format!(
            "projects/{}/derived/{}",
            self.project_id, self.table_name,
        ))
    }
}

/// Result of a materialization run.
#[derive(Debug, Clone)]
pub struct MaterializeResult {
    /// Total rows written across all Parquet files.
    pub row_count: u64,
    /// Total compressed bytes written to R2.
    pub bytes_written: u64,
    /// Number of Parquet files created.
    pub files_created: u32,
    /// R2 prefix where the files are stored.
    pub r2_prefix: String,
    /// Arrow schema of the output data.
    pub arrow_schema: Arc<Schema>,
    /// List of R2 keys for all created files.
    pub file_keys: Vec<String>,
    /// Wall-clock duration of the materialization.
    pub duration_ms: u64,
}

/// Execute a rewritten SQL query against ClickHouse via the native TCP
/// protocol, convert result blocks to Arrow `RecordBatch`es, and write
/// them to R2 as Parquet files.
///
/// On partial failure, any files already uploaded to R2 are cleaned up before
/// returning the error.
#[tracing::instrument(
    name = "warehouse.materializer.materialize_query",
    skip_all,
    fields(
        project_id = %options.project_id,
        table_name = %options.table_name,
        query_length = rewritten_sql.len(),
    ),
    err(Display),
)]
pub async fn materialize_query(
    native_pool: &NativePool,
    rewritten_sql: &str,
    r2: &R2Storage,
    options: &MaterializeOptions,
) -> Result<MaterializeResult> {
    let start = Instant::now();
    let prefix = options.r2_prefix()?;

    let sql = rewritten_sql.trim().trim_end_matches(';').to_string();

    let conn = native_pool
        .get()
        .await
        .context("Failed to checkout ClickHouse connection from pool")?;
    let mut block_stream = conn
        .query_raw(&sql)
        .await
        .context("ClickHouse native query failed")?;

    let mut arrow_schema: Option<Arc<Schema>> = None;
    let mut total_rows: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut files_created: u32 = 0;
    let mut file_keys: Vec<String> = Vec::new();

    let mut pending_batches: Vec<RecordBatch> = Vec::new();
    let mut pending_bytes: usize = 0;
    let mut estimated_response_bytes: usize = 0;

    let flush_result: Result<()> = async {
        while let Some(block_result) = block_stream.next().await {
            let block = block_result
                .context("Failed to read block from ClickHouse")?;

            if block.rows == 0 {
                continue;
            }

            let batch = block_to_record_batch(&block)
                .map_err(|e| anyhow::anyhow!("Block-to-Arrow conversion failed: {}", e))?;

            if arrow_schema.is_none() {
                arrow_schema = Some(batch.schema());
            }

            let batch_mem = batch.get_array_memory_size();
            estimated_response_bytes += batch_mem;
            if estimated_response_bytes > options.max_response_bytes {
                anyhow::bail!(
                    "Response exceeds maximum size of {} bytes; \
                     consider adding filters or reducing the result set",
                    options.max_response_bytes,
                );
            }

            total_rows += batch.num_rows() as u64;
            pending_bytes += batch_mem;
            pending_batches.push(batch);

            if pending_bytes >= options.target_file_size
                || pending_bytes >= options.max_pending_memory_bytes
            {
                let schema = arrow_schema.as_ref().unwrap();
                let (key, parquet_size) = flush_batches(
                    &pending_batches,
                    schema,
                    r2,
                    &prefix,
                    &options.refresh_version,
                    files_created,
                )
                .await?;

                total_bytes += parquet_size;
                files_created += 1;
                file_keys.push(key);
                pending_batches.clear();
                pending_bytes = 0;
            }
        }

        if !pending_batches.is_empty() {
            let schema = arrow_schema.as_ref()
                .context("No schema available for flush")?;
            let (key, parquet_size) = flush_batches(
                &pending_batches,
                schema,
                r2,
                &prefix,
                &options.refresh_version,
                files_created,
            )
            .await?;

            total_bytes += parquet_size;
            files_created += 1;
            file_keys.push(key);
        }

        Ok(())
    }
    .await;

    if let Err(e) = flush_result {
        if !file_keys.is_empty() {
            tracing::warn!(
                files_to_cleanup = file_keys.len(),
                "Cleaning up partially uploaded files after materialization failure"
            );
            delete_materialized_files(r2, &file_keys).await;
        }
        return Err(e);
    }

    let final_schema = arrow_schema.unwrap_or_else(|| Arc::new(Schema::empty()));
    let duration_ms = start.elapsed().as_millis() as u64;

    tracing::info!(
        row_count = total_rows,
        bytes_written = total_bytes,
        files_created = files_created,
        duration_ms = duration_ms,
        "Materialization complete"
    );

    Ok(MaterializeResult {
        row_count: total_rows,
        bytes_written: total_bytes,
        files_created,
        r2_prefix: prefix,
        arrow_schema: final_schema,
        file_keys,
        duration_ms,
    })
}

/// Write accumulated batches to a single Parquet file in R2 with a stats sidecar.
///
/// Returns the R2 key and the actual compressed Parquet size in bytes.
async fn flush_batches(
    batches: &[RecordBatch],
    schema: &Arc<Schema>,
    r2: &R2Storage,
    prefix: &str,
    refresh_version: &str,
    seq: u32,
) -> Result<(String, u64)> {
    let (parquet_bytes, stats) =
        write_parquet_with_stats(Arc::clone(schema), batches, WriteOptions::default())
            .context("Failed to write Parquet")?;

    let parquet_size = parquet_bytes.len() as u64;

    let key = format!(
        "{}/{}_{}.parquet",
        prefix, refresh_version, seq,
    );

    r2.upload_parquet_with_stats(&key, parquet_bytes, &stats)
        .await
        .context("R2 upload failed")?;

    Ok((key, parquet_size))
}

/// Delete materialized files from R2 (with stats sidecars).
///
/// Best-effort: individual deletion failures are logged as warnings but
/// do not propagate. Callers should treat this as fire-and-forget cleanup.
pub async fn delete_materialized_files(
    r2: &R2Storage,
    keys: &[String],
) {
    for key in keys {
        if let Err(e) = r2.delete_with_stats(key).await {
            tracing::warn!(
                key = %key,
                error = %e,
                "Failed to delete old materialized file (non-fatal)"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_options(table_name: &str) -> MaterializeOptions {
        MaterializeOptions {
            project_id: Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
            table_name: table_name.to_string(),
            target_file_size: MaterializeOptions::DEFAULT_TARGET_FILE_SIZE,
            refresh_version: "20260215T120000".to_string(),
            max_response_bytes: MaterializeOptions::DEFAULT_MAX_RESPONSE_BYTES,
            max_pending_memory_bytes: MaterializeOptions::DEFAULT_MAX_PENDING_MEMORY_BYTES,
        }
    }

    #[test]
    fn test_r2_prefix() {
        let opts = make_test_options("token_transfers");
        assert_eq!(
            opts.r2_prefix().unwrap(),
            "projects/550e8400-e29b-41d4-a716-446655440000/derived/token_transfers"
        );
    }

    #[test]
    fn test_r2_prefix_different_tables() {
        let opts1 = make_test_options("transfers");
        let opts2 = make_test_options("balances");
        assert_ne!(opts1.r2_prefix().unwrap(), opts2.r2_prefix().unwrap());
        assert!(opts1.r2_prefix().unwrap().ends_with("/transfers"));
        assert!(opts2.r2_prefix().unwrap().ends_with("/balances"));
    }

    #[test]
    fn test_r2_prefix_includes_project_id() {
        let opts = make_test_options("my_table");
        assert!(opts.r2_prefix().unwrap().contains("550e8400-e29b-41d4-a716-446655440000"));
    }

    #[test]
    fn test_default_target_file_size() {
        assert_eq!(MaterializeOptions::DEFAULT_TARGET_FILE_SIZE, 64 * 1024 * 1024);
    }

    #[test]
    fn test_materialize_result_fields() {
        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, true),
        ]));
        let result = MaterializeResult {
            row_count: 1000,
            bytes_written: 50_000,
            files_created: 2,
            r2_prefix: "projects/abc/derived/test".to_string(),
            arrow_schema: schema.clone(),
            file_keys: vec!["key1.parquet".to_string(), "key2.parquet".to_string()],
            duration_ms: 1234,
        };
        assert_eq!(result.row_count, 1000);
        assert_eq!(result.bytes_written, 50_000);
        assert_eq!(result.files_created, 2);
        assert_eq!(result.file_keys.len(), 2);
        assert_eq!(result.arrow_schema.fields().len(), 2);
        assert_eq!(result.duration_ms, 1234);
    }

    #[test]
    fn test_options_custom_file_size() {
        let opts = MaterializeOptions {
            project_id: Uuid::new_v4(),
            table_name: "small_table".to_string(),
            target_file_size: 1024 * 1024, // 1 MB
            refresh_version: "v1".to_string(),
            max_response_bytes: MaterializeOptions::DEFAULT_MAX_RESPONSE_BYTES,
            max_pending_memory_bytes: MaterializeOptions::DEFAULT_MAX_PENDING_MEMORY_BYTES,
        };
        assert_eq!(opts.target_file_size, 1024 * 1024);
    }

    #[test]
    fn test_format_arrow_stream_query_building() {
        let sql = "SELECT * FROM my_table WHERE id > 10";
        let formatted = format!(
            "{} FORMAT ArrowStream",
            sql.trim().trim_end_matches(';')
        );
        assert_eq!(formatted, "SELECT * FROM my_table WHERE id > 10 FORMAT ArrowStream");
    }

    #[test]
    fn test_format_arrow_stream_strips_trailing_semicolon() {
        let sql = "SELECT * FROM my_table;";
        let formatted = format!(
            "{} FORMAT ArrowStream",
            sql.trim().trim_end_matches(';')
        );
        assert_eq!(formatted, "SELECT * FROM my_table FORMAT ArrowStream");
    }

    #[test]
    fn test_format_arrow_stream_strips_whitespace() {
        let sql = "  SELECT * FROM my_table  ;  ";
        let formatted = format!(
            "{} FORMAT ArrowStream",
            sql.trim().trim_end_matches(';')
        );
        assert_eq!(formatted, "SELECT * FROM my_table   FORMAT ArrowStream");
    }

    #[test]
    fn test_flush_key_format() {
        let prefix = "projects/abc123/derived/my_table";
        let refresh_version = "20260215T120000";
        let seq: u32 = 3;
        let key = format!("{}/{}_{}.parquet", prefix, refresh_version, seq);
        assert_eq!(key, "projects/abc123/derived/my_table/20260215T120000_3.parquet");
    }

    #[test]
    fn test_r2_prefix_rejects_path_traversal() {
        let opts = MaterializeOptions {
            project_id: Uuid::new_v4(),
            table_name: "../../evil".to_string(),
            target_file_size: MaterializeOptions::DEFAULT_TARGET_FILE_SIZE,
            refresh_version: "v1".to_string(),
            max_response_bytes: MaterializeOptions::DEFAULT_MAX_RESPONSE_BYTES,
            max_pending_memory_bytes: MaterializeOptions::DEFAULT_MAX_PENDING_MEMORY_BYTES,
        };
        let err = opts.r2_prefix().unwrap_err();
        assert!(err.to_string().contains("invalid characters"));
    }

    #[test]
    fn test_r2_prefix_rejects_hyphens() {
        let opts = MaterializeOptions {
            project_id: Uuid::new_v4(),
            table_name: "my-table".to_string(),
            target_file_size: MaterializeOptions::DEFAULT_TARGET_FILE_SIZE,
            refresh_version: "v1".to_string(),
            max_response_bytes: MaterializeOptions::DEFAULT_MAX_RESPONSE_BYTES,
            max_pending_memory_bytes: MaterializeOptions::DEFAULT_MAX_PENDING_MEMORY_BYTES,
        };
        assert!(opts.r2_prefix().is_err());
    }
}
