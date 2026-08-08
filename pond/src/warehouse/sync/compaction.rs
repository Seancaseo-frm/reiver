//! Background Compaction Job
//!
//! Compaction consolidates many small Parquet files into fewer larger files
//! for better query performance. With copy-on-write sync, files are always
//! deduplicated at write time, so compaction only needs to merge files
//! without any PK-based deduplication or tombstone filtering.
//!
//! **Critical invariant**: correctness depends on rows being deduplicated
//! *before* they reach compaction. If a partial sync failure writes a file
//! without updating the PK index, compaction will cement duplicate rows.
//!
//! ## Trigger conditions
//! - **File count threshold**: partition has > N files (configurable, default 10)
//!
//! ## Process
//! 1. Acquire advisory lock to prevent concurrent compaction on the same table
//! 2. Read all committed Parquet files for a partition from R2
//! 3. Concatenate all batches
//! 4. Split output into ~200MB uncompressed chunks (compress to ~64MB Parquet)
//! 5. Swap file records in a single DB transaction (insert new, delete old); R2 cleanup is best-effort afterward

use anyhow::Result;
use arrow::compute::{concat_batches, lexsort_to_indices, take, SortColumn, SortOptions};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

use crate::warehouse::indexes::PartitionManager;
use crate::warehouse::indexes::partition_manager::NewPartitionFile;
use crate::warehouse::indexes::persistence::{try_advisory_lock, release_advisory_lock};
use crate::warehouse::parquet::WriteOptions;
use crate::warehouse::parquet_stats::write_parquet_with_stats;
use crate::warehouse::storage::r2::R2Storage;
use crate::warehouse::sync::merge::{read_parquet_bytes, strip_metadata_columns, unify_batch_schemas, project_batch_to_schema};
use crate::warehouse::sync::sync_executor::split_batches_by_size;

/// Default file count threshold for triggering compaction.
const DEFAULT_FILE_COUNT_THRESHOLD: usize = 10;

/// Target uncompressed size for compacted output (~200MB, compresses to ~64MB Parquet).
const COMPACTION_TARGET_FILE_SIZE: usize = 200 * 1024 * 1024;

/// Infer sort columns from an Arrow schema, mirroring ClickHouse ORDER BY inference.
///
/// Priority: `id` > `created_at` > `timestamp` > empty (no sort).
/// Sorting compacted Parquet files by these columns produces tighter row group
/// min/max statistics, enabling more effective predicate pushdown.
pub fn infer_sort_columns(schema: &Schema) -> Vec<String> {
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    if names.contains(&"id") {
        vec!["id".to_string()]
    } else if names.contains(&"created_at") {
        vec!["created_at".to_string()]
    } else if names.contains(&"timestamp") {
        vec!["timestamp".to_string()]
    } else {
        vec![]
    }
}

/// Sort a slice of `RecordBatch`es by the given columns.
///
/// Concatenates all batches, sorts by `sort_columns` with nulls last,
/// and returns the result split back into batches of the original total row count.
fn sort_batches(
    schema: Arc<Schema>,
    batches: &[RecordBatch],
    sort_columns: &[String],
) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() || sort_columns.is_empty() {
        return Ok(batches.to_vec());
    }

    let combined = concat_batches(&schema, batches)
        .map_err(|e| anyhow::anyhow!("Failed to concatenate batches for sorting: {}", e))?;

    let sort_cols: Vec<SortColumn> = sort_columns
        .iter()
        .filter_map(|name| {
            combined.schema().index_of(name).ok().map(|idx| SortColumn {
                values: combined.column(idx).clone(),
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
            })
        })
        .collect();

    if sort_cols.is_empty() {
        return Ok(batches.to_vec());
    }

    let indices = lexsort_to_indices(&sort_cols, None)
        .map_err(|e| anyhow::anyhow!("Failed to compute sort indices: {}", e))?;

    let sorted_columns: Vec<_> = combined
        .columns()
        .iter()
        .map(|col| take(col.as_ref(), &indices, None).map_err(|e| anyhow::anyhow!("Sort take failed: {}", e)))
        .collect::<Result<Vec<_>>>()?;

    let sorted_batch = RecordBatch::try_new(schema, sorted_columns)
        .map_err(|e| anyhow::anyhow!("Failed to build sorted RecordBatch: {}", e))?;

    Ok(vec![sorted_batch])
}

/// Background compaction worker.
pub struct CompactionWorker {
    db: PgPool,
    r2_storage: Arc<R2Storage>,
    partition_manager: Arc<PartitionManager>,
    file_count_threshold: usize,
}

/// Result of a compaction run.
#[derive(Debug, Default)]
pub struct CompactionResult {
    pub partitions_compacted: usize,
    pub files_removed: usize,
    pub files_created: usize,
    pub rows_after_compaction: u64,
    pub bytes_after_compaction: u64,
}

impl CompactionWorker {
    pub fn new(
        db: PgPool,
        r2_storage: Arc<R2Storage>,
        partition_manager: Arc<PartitionManager>,
    ) -> Self {
        Self {
            db,
            r2_storage,
            partition_manager,
            file_count_threshold: DEFAULT_FILE_COUNT_THRESHOLD,
        }
    }

    /// Set the file count threshold for triggering compaction.
    pub fn with_file_count_threshold(mut self, threshold: usize) -> Self {
        self.file_count_threshold = threshold;
        self
    }

    /// Run a compaction pass over all partitions that need it.
    #[tracing::instrument(
        name = "warehouse.compaction.run",
        skip_all,
        err(Display),
    )]
    pub async fn run_compaction_pass(&self) -> Result<CompactionResult> {
        let mut result = CompactionResult::default();

        let candidates = self.find_compaction_candidates().await?;

        info!(
            candidates = candidates.len(),
            "Found compaction candidates"
        );

        for (partition_id, reason) in candidates {
            match self.compact_partition(partition_id).await {
                Ok(partition_result) => {
                    result.partitions_compacted += 1;
                    result.files_removed += partition_result.files_removed;
                    result.files_created += partition_result.files_created;
                    result.rows_after_compaction += partition_result.rows_after_compaction;
                    result.bytes_after_compaction += partition_result.bytes_after_compaction;

                    info!(
                        partition_id = %partition_id,
                        reason = reason,
                        files_removed = partition_result.files_removed,
                        files_created = partition_result.files_created,
                        "Compacted partition"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        partition_id = %partition_id,
                        error = %e,
                        "Failed to compact partition"
                    );
                }
            }
        }

        Ok(result)
    }

    /// Find partitions that are candidates for compaction.
    /// Returns (partition_id, reason) tuples.
    async fn find_compaction_candidates(&self) -> Result<Vec<(Uuid, &'static str)>> {
        let mut candidates = Vec::new();

        // File count threshold -- too many small files
        let threshold = self.file_count_threshold as i64;
        let file_heavy: Vec<(Uuid,)> = sqlx::query_as(
            r#"
            SELECT pf.partition_id
            FROM warehouse_partition_files pf
            JOIN warehouse_partitions p ON p.id = pf.partition_id
            WHERE pf.sync_state = 'committed'
              AND p.sync_state = 'committed'
            GROUP BY pf.partition_id
            HAVING COUNT(*) > $1
            "#,
        )
        .bind(threshold)
        .fetch_all(&self.db)
        .await?;

        for (id,) in file_heavy {
            candidates.push((id, "file_count_threshold"));
        }

        Ok(candidates)
    }

    /// Compact a single partition.
    ///
    /// 1. Acquire advisory lock
    /// 2. Read all committed files
    /// 3. Concatenate batches
    /// 4. Split into ~200MB uncompressed chunks (compress to ~64MB)
    /// 5. Atomically swap records in a DB transaction
    #[tracing::instrument(
        name = "warehouse.compaction.compact_partition",
        skip(self),
        fields(%partition_id),
        err(Display),
    )]
    async fn compact_partition(&self, partition_id: Uuid) -> Result<CompactionResult> {
        let (partition, project_id) = self.partition_manager
            .get_partition_with_project(partition_id)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get partition: {}", e))?
            .ok_or_else(|| anyhow::anyhow!("Partition {} not found", partition_id))?;

        let mut lock_conn = self.db.acquire().await
            .map_err(|e| anyhow::anyhow!("Failed to acquire DB connection for advisory lock: {}", e))?;

        if !try_advisory_lock(&mut *lock_conn, project_id, &partition.table_name).await {
            info!(
                partition_id = %partition_id,
                table = %partition.table_name,
                "Skipping compaction: another worker holds the advisory lock"
            );
            return Ok(CompactionResult::default());
        }

        // Use catch_unwind so the advisory lock is released even if the
        // inner logic panics (session-level advisory locks persist on pooled
        // connections and would block all future compaction for this table).
        let result = std::panic::AssertUnwindSafe(
            self.compact_partition_inner(partition_id, &partition)
        );
        let result = match futures::FutureExt::catch_unwind(result).await {
            Ok(r) => r,
            Err(panic_payload) => {
                release_advisory_lock(&mut *lock_conn, project_id, &partition.table_name).await;
                std::panic::resume_unwind(panic_payload);
            }
        };

        release_advisory_lock(&mut *lock_conn, project_id, &partition.table_name).await;

        result
    }

    /// Inner compaction logic, called while holding the advisory lock.
    async fn compact_partition_inner(
        &self,
        partition_id: Uuid,
        partition: &crate::warehouse::indexes::partition_manager::Partition,
    ) -> Result<CompactionResult> {
        // List all committed files
        let files = self.partition_manager
            .list_partition_files(partition_id)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list files: {}", e))?;

        if files.len() <= 1 {
            return Ok(CompactionResult::default());
        }

        // Read all files from R2 into RecordBatches.
        // Download files in parallel. Abort compaction if ANY file fails
        // to download or parse, to prevent data loss when old files are
        // deleted in the swap step.
        let download_futures = files.iter().map(|file| {
            let storage = self.r2_storage.clone();
            let path = file.file_path.clone();
            async move {
                let data = storage.download(&path).await
                    .map_err(|e| anyhow::anyhow!(
                        "Aborting compaction: failed to download Parquet file {}: {}",
                        path, e
                    ))?;
                read_parquet_bytes(&data)
                    .map_err(|e| anyhow::anyhow!(
                        "Aborting compaction: failed to read Parquet file {}: {}",
                        path, e
                    ))
            }
        });

        let results: Vec<Result<Vec<RecordBatch>>> =
            futures::future::join_all(download_futures).await;

        let mut all_batches: Vec<RecordBatch> = Vec::new();
        for result in results {
            all_batches.extend(result?);
        }

        if all_batches.is_empty() {
            return Ok(CompactionResult::default());
        }

        // Strip legacy metadata columns if present in old files
        let clean_batches = strip_metadata_columns(&all_batches)?;

        if clean_batches.is_empty() {
            return Ok(CompactionResult::default());
        }

        // Unify schemas in case files were written at different schema versions
        let schema = unify_batch_schemas(&clean_batches)?;
        let clean_batches: Vec<RecordBatch> = clean_batches
            .iter()
            .map(|b| {
                if b.schema() == schema {
                    Ok(b.clone())
                } else {
                    project_batch_to_schema(b, &schema)
                }
            })
            .collect::<Result<Vec<_>>>()?;

        // Sort by inferred clustering key so row group min/max statistics are tight,
        // enabling effective predicate pushdown by ClickHouse.
        let sort_cols = infer_sort_columns(&schema);
        let clean_batches = if sort_cols.is_empty() {
            clean_batches
        } else {
            info!(
                partition_id = %partition_id,
                sort_columns = ?sort_cols,
                "Sorting compacted data for tighter row group statistics"
            );
            sort_batches(schema.clone(), &clean_batches, &sort_cols)?
        };

        let file_chunks = split_batches_by_size(&clean_batches, COMPACTION_TARGET_FILE_SIZE);

        // Upload new files to R2
        let mut new_files: Vec<NewPartitionFile> = Vec::new();
        let new_sync_version = Utc::now().timestamp_millis();

        for (seq, chunk) in file_chunks.iter().enumerate() {
            let (parquet_bytes, raw_stats) = write_parquet_with_stats(schema.clone(), chunk, WriteOptions::default())
                .map_err(|e| anyhow::anyhow!("Failed to write compacted Parquet: {}", e))?;
            let stats = raw_stats.with_sort_columns(sort_cols.clone());

            let file_rows: i64 = chunk.iter().map(|b| b.num_rows() as i64).sum();
            let file_bytes = parquet_bytes.len() as i64;
            let file_id = Uuid::new_v4();

            let base_path = partition.parquet_path.as_deref()
                .ok_or_else(|| anyhow::anyhow!(
                    "Partition {} has no parquet_path configured; refusing to compact to avoid data loss",
                    partition_id
                ))?
                .trim_end_matches('/');
            let key = format!(
                "{}/compacted_{}_{:04}_{}.parquet",
                base_path, new_sync_version, seq, file_id
            );

            if let Err(e) = self.r2_storage
                .upload_parquet_with_stats(&key, parquet_bytes, &stats)
                .await
            {
                for uploaded in &new_files {
                    if let Err(cleanup_err) = self.r2_storage.delete_with_stats(&uploaded.file_path).await {
                        tracing::warn!(
                            file = %uploaded.file_path,
                            error = %cleanup_err,
                            "Failed to clean up orphaned compacted file after upload failure"
                        );
                    }
                }
                return Err(anyhow::anyhow!("Failed to upload compacted file {}: {}", key, e));
            }

            new_files.push(NewPartitionFile {
                file_path: key,
                row_count: file_rows,
                size_bytes: file_bytes,
            });
        }

        // DB-level swap: delete old file records, insert new ones (R2 cleanup follows)
        let old_file_ids: Vec<Uuid> = files.iter().map(|f| f.id).collect();
        let old_paths = match self.partition_manager
            .swap_partition_files(partition_id, &old_file_ids, &new_files, new_sync_version)
            .await
        {
            Ok(paths) => paths,
            Err(e) => {
                for uploaded in &new_files {
                    if let Err(cleanup_err) = self.r2_storage.delete_with_stats(&uploaded.file_path).await {
                        tracing::warn!(
                            file = %uploaded.file_path,
                            error = %cleanup_err,
                            "Failed to clean up compacted file after swap failure"
                        );
                    }
                }
                return Err(anyhow::anyhow!("Failed to swap partition files: {}", e));
            }
        };

        for old_path in &old_paths {
            if let Err(e) = self.r2_storage.delete_with_stats(old_path).await {
                tracing::warn!(
                    file = %old_path,
                    error = %e,
                    "Failed to delete old R2 file during compaction cleanup"
                );
            }
        }

        let total_rows: u64 = new_files.iter().map(|f| f.row_count as u64).sum();
        let total_bytes: u64 = new_files.iter().map(|f| f.size_bytes as u64).sum();

        Ok(CompactionResult {
            partitions_compacted: 1,
            files_removed: files.len(),
            files_created: new_files.len(),
            rows_after_compaction: total_rows,
            bytes_after_compaction: total_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc as StdArc;
    use crate::warehouse::sync::merge::strip_metadata_columns;

    fn make_schema() -> StdArc<Schema> {
        StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn make_schema_with_meta() -> StdArc<Schema> {
        StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("_dh_sync_version", DataType::Int64, false),
            Field::new("_dh_op", DataType::Utf8, false),
        ]))
    }

    #[test]
    fn test_strip_metadata_from_legacy_files() {
        let schema = make_schema_with_meta();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(Int64Array::from(vec![1, 2])),
                StdArc::new(StringArray::from(vec!["Alice", "Bob"])),
                StdArc::new(Int64Array::from(vec![5, 10])),
                StdArc::new(StringArray::from(vec!["I", "U"])),
            ],
        ).unwrap();

        let result = strip_metadata_columns(&[batch]).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_columns(), 2);
        assert_eq!(result[0].schema().field(0).name(), "id");
        assert_eq!(result[0].schema().field(1).name(), "name");
    }

    #[test]
    fn test_clean_files_pass_through() {
        let schema = make_schema();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(Int64Array::from(vec![1, 2])),
                StdArc::new(StringArray::from(vec!["Alice", "Bob"])),
            ],
        ).unwrap();

        let result = strip_metadata_columns(&[batch]).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_columns(), 2);
    }

    #[test]
    fn test_infer_sort_columns_id() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        assert_eq!(infer_sort_columns(&schema), vec!["id"]);
    }

    #[test]
    fn test_infer_sort_columns_created_at() {
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("created_at", DataType::Int64, false),
        ]);
        assert_eq!(infer_sort_columns(&schema), vec!["created_at"]);
    }

    #[test]
    fn test_infer_sort_columns_timestamp() {
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("timestamp", DataType::Int64, false),
        ]);
        assert_eq!(infer_sort_columns(&schema), vec!["timestamp"]);
    }

    #[test]
    fn test_infer_sort_columns_none() {
        let schema = Schema::new(vec![
            Field::new("foo", DataType::Utf8, false),
            Field::new("bar", DataType::Int64, false),
        ]);
        assert!(infer_sort_columns(&schema).is_empty());
    }

    #[test]
    fn test_infer_sort_columns_id_takes_priority() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("created_at", DataType::Int64, false),
            Field::new("timestamp", DataType::Int64, false),
        ]);
        assert_eq!(infer_sort_columns(&schema), vec!["id"]);
    }

    #[test]
    fn test_sort_batches_sorts_by_id() {
        let schema = make_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                StdArc::new(Int64Array::from(vec![3, 1, 2])),
                StdArc::new(StringArray::from(vec!["c", "a", "b"])),
            ],
        ).unwrap();

        let sorted = sort_batches(schema.clone(), &[batch], &["id".to_string()]).unwrap();
        assert_eq!(sorted.len(), 1);

        let ids = sorted[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ids.values(), &[1, 2, 3]);

        let names = sorted[0].column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(names.value(0), "a");
        assert_eq!(names.value(1), "b");
        assert_eq!(names.value(2), "c");
    }

    #[test]
    fn test_sort_batches_multiple_batches() {
        let schema = make_schema();
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                StdArc::new(Int64Array::from(vec![5, 3])),
                StdArc::new(StringArray::from(vec!["e", "c"])),
            ],
        ).unwrap();
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                StdArc::new(Int64Array::from(vec![1, 4, 2])),
                StdArc::new(StringArray::from(vec!["a", "d", "b"])),
            ],
        ).unwrap();

        let sorted = sort_batches(schema.clone(), &[batch1, batch2], &["id".to_string()]).unwrap();
        assert_eq!(sorted.len(), 1);
        assert_eq!(sorted[0].num_rows(), 5);

        let ids = sorted[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ids.values(), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_sort_batches_empty_sort_columns() {
        let schema = make_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                StdArc::new(Int64Array::from(vec![3, 1, 2])),
                StdArc::new(StringArray::from(vec!["c", "a", "b"])),
            ],
        ).unwrap();

        let result = sort_batches(schema, &[batch.clone()], &[]).unwrap();
        assert_eq!(result.len(), 1);
        let ids = result[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ids.values(), &[3, 1, 2]);
    }
}
