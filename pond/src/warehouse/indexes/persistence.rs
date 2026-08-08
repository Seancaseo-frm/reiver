//! FST Index Persistence
//!
//! Provides serialization and deserialization of FST indexes to/from
//! bytes for storage in R2 or local disk.
//!
//! This enables cache survival across restarts and sharing between instances.

use std::sync::Arc;
use thiserror::Error;

use crate::warehouse::metrics::WarehouseMetrics;

use super::skip_index::FileSkipIndex;

/// Errors that can occur during index persistence.
#[derive(Debug, Error)]
pub enum PersistenceError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("FST error: {0}")]
    Fst(#[from] fst::Error),
    
    #[error("Storage error: {0}")]
    Storage(String),
    
    #[error("Invalid format: {0}")]
    InvalidFormat(String),
}

/// Result type for persistence operations.
pub type PersistenceResult<T> = Result<T, PersistenceError>;

// ============================================================================
// Database-backed Skip Index Persistence
// ============================================================================

/// Save a FileSkipIndex to the database.
///
/// This saves each column's FST bytes as a separate row for efficient partial loading.
///
/// # Arguments
/// * `db` - Database connection pool
/// * `project_id` - Owner UUID. For project-scoped sources this is the project
///   UUID; for global blockchain sources this is the `GlobalSource.id` (the two
///   UUID spaces never overlap because they come from different DB tables).
/// * `table_name` - The table this index is for
/// * `partition_key` - The partition (e.g., "2025/01")
/// * `file_index` - The file skip index to save
/// * `row_count` - Estimated row count for the file
///
/// # Performance
///
/// Uses upsert (INSERT ... ON CONFLICT) to handle re-syncs efficiently.
pub async fn save_file_skip_index(
    db: &sqlx::PgPool,
    project_id: uuid::Uuid,
    table_name: &str,
    partition_key: &str,
    file_index: &FileSkipIndex,
    row_count: u64,
    metrics: Option<&Arc<WarehouseMetrics>>,
) -> PersistenceResult<usize> {
    use chrono::Utc;
    
    let now = Utc::now();
    let mut saved_count = 0;
    let mut total_fst_bytes: u64 = 0;

    // Use a transaction to batch all column INSERTs into a single round trip,
    // reducing N individual statements to 1 transaction with N statements.
    let mut tx = db.begin().await
        .map_err(|e| PersistenceError::Storage(format!("Failed to begin transaction: {}", e)))?;

    for (column_name, fst_set) in &file_index.column_values {
        let fst_bytes = fst_set.as_fst().as_bytes().to_vec();

        if fst_bytes.len() > 50 * 1024 * 1024 {
            tracing::warn!(
                project_id = %project_id,
                table = %table_name,
                partition = %partition_key,
                column = %column_name,
                file = %file_index.file_path,
                size_mb = fst_bytes.len() / (1024 * 1024),
                "Skipping FST save: exceeds 50MB size limit"
            );
            continue;
        }

        let fst_len = fst_bytes.len() as u64;

        sqlx::query(
            r#"
            INSERT INTO warehouse_skip_indexes
                (project_id, table_name, partition_key, file_path, column_name, values_fst, row_count, created_at, updated_at)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7, $8, $8)
            ON CONFLICT (project_id, table_name, partition_key, file_path, column_name)
            DO UPDATE SET
                values_fst = EXCLUDED.values_fst,
                row_count = EXCLUDED.row_count,
                updated_at = EXCLUDED.updated_at
            "#
        )
        .bind(project_id)
        .bind(table_name)
        .bind(partition_key)
        .bind(&file_index.file_path)
        .bind(column_name)
        .bind(&fst_bytes)
        .bind(row_count as i64)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(|e| PersistenceError::Storage(format!("Failed to save skip index: {}", e)))?;

        saved_count += 1;
        total_fst_bytes += fst_len;
    }

    tx.commit().await
        .map_err(|e| PersistenceError::Storage(format!("Failed to commit skip index transaction: {}", e)))?;
    
    // Record indexing metrics for billing
    if let Some(m) = metrics {
        if total_fst_bytes > 0 {
            m.record_indexed_bytes(0, total_fst_bytes);
        }
    }
    
    Ok(saved_count)
}

/// Save a HierarchicalSkipIndex to the database.
///
/// This iterates through all partitions and files, saving each column's FST.
/// Designed to be called after sync operations complete.
///
/// # Arguments
/// * `db` - Database connection pool
/// * `project_id` - The project this index belongs to
/// * `table_name` - The table this index is for
/// * `index` - The hierarchical skip index to save
///
/// # Returns
/// The total number of FST entries saved.
pub async fn save_hierarchical_skip_index(
    db: &sqlx::PgPool,
    project_id: uuid::Uuid,
    table_name: &str,
    index: &super::skip_index::HierarchicalSkipIndex,
    metrics: Option<&Arc<WarehouseMetrics>>,
) -> PersistenceResult<usize> {
    let mut total_saved = 0;
    
    for (partition_key, partition) in index.partitions() {
        for (file_path, file_index) in partition.files.iter() {
            // Estimate row count from partition stats divided by file count
            let rows_per_file = if partition.files.len() > 0 {
                partition.estimated_rows / partition.files.len() as u64
            } else {
                0
            };
            
            let saved = save_file_skip_index(
                db,
                project_id,
                table_name,
                partition_key,
                file_index,
                rows_per_file,
                metrics,
            ).await?;
            
            total_saved += saved;
        }
    }
    
    tracing::info!(
        project_id = %project_id,
        table = %table_name,
        partitions = index.partition_count(),
        files = index.total_files(),
        fst_entries = total_saved,
        "Saved hierarchical skip index to database"
    );
    
    Ok(total_saved)
}

/// List all file paths that have skip indexes for a given table.
///
/// Returns a `HashSet` of distinct file paths. Used by the incremental
/// rebuild job to diff storage state against the DB and process only
/// new/removed files.
pub async fn list_indexed_files(
    db: &sqlx::PgPool,
    project_id: uuid::Uuid,
    table_name: &str,
) -> PersistenceResult<std::collections::HashSet<String>> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT file_path FROM warehouse_skip_indexes WHERE project_id = $1 AND table_name = $2"
    )
    .bind(project_id)
    .bind(table_name)
    .fetch_all(db)
    .await
    .map_err(|e| PersistenceError::Storage(format!("Failed to list indexed files: {}", e)))?;

    Ok(rows.into_iter().map(|(path,)| path).collect())
}

/// Delete all skip indexes for a table.
///
/// Used before re-syncing to ensure stale indexes are removed.
pub async fn delete_table_skip_indexes(
    db: &sqlx::PgPool,
    project_id: uuid::Uuid,
    table_name: &str,
) -> PersistenceResult<u64> {
    let result = sqlx::query(
        "DELETE FROM warehouse_skip_indexes WHERE project_id = $1 AND table_name = $2"
    )
    .bind(project_id)
    .bind(table_name)
    .execute(db)
    .await
    .map_err(|e| PersistenceError::Storage(format!("Failed to delete skip indexes: {}", e)))?;
    
    Ok(result.rows_affected())
}

/// Delete skip indexes for specific files.
///
/// Used for incremental sync to remove indexes for files being replaced.
pub async fn delete_file_skip_indexes(
    db: &sqlx::PgPool,
    project_id: uuid::Uuid,
    table_name: &str,
    file_paths: &[String],
) -> PersistenceResult<u64> {
    if file_paths.is_empty() {
        return Ok(0);
    }
    
    let result = sqlx::query(
        r#"
        DELETE FROM warehouse_skip_indexes 
        WHERE project_id = $1 
          AND table_name = $2 
          AND file_path = ANY($3)
        "#
    )
    .bind(project_id)
    .bind(table_name)
    .bind(file_paths)
    .execute(db)
    .await
    .map_err(|e| PersistenceError::Storage(format!("Failed to delete file skip indexes: {}", e)))?;
    
    Ok(result.rows_affected())
}

/// Get statistics about skip indexes for a project.
pub async fn get_skip_index_stats(
    db: &sqlx::PgPool,
    project_id: uuid::Uuid,
) -> PersistenceResult<SkipIndexStats> {
    use sqlx::Row;
    
    let row = sqlx::query(
        r#"
        SELECT 
            COUNT(DISTINCT table_name) as table_count,
            COUNT(DISTINCT partition_key) as partition_count,
            COUNT(DISTINCT file_path) as file_count,
            COUNT(*) as fst_count,
            COALESCE(SUM(LENGTH(values_fst)), 0) as total_bytes
        FROM warehouse_skip_indexes
        WHERE project_id = $1
        "#
    )
    .bind(project_id)
    .fetch_one(db)
    .await
    .map_err(|e| PersistenceError::Storage(format!("Failed to get skip index stats: {}", e)))?;
    
    Ok(SkipIndexStats {
        table_count: row.get::<i64, _>("table_count") as usize,
        partition_count: row.get::<i64, _>("partition_count") as usize,
        file_count: row.get::<i64, _>("file_count") as usize,
        fst_count: row.get::<i64, _>("fst_count") as usize,
        total_bytes: row.get::<i64, _>("total_bytes") as usize,
    })
}

/// Statistics about skip indexes for a project.
#[derive(Debug, Clone)]
pub struct SkipIndexStats {
    pub table_count: usize,
    pub partition_count: usize,
    pub file_count: usize,
    pub fst_count: usize,
    pub total_bytes: usize,
}

// ============================================================================
// Skip Index Manifest (Hybrid R2 Storage)
// ============================================================================

/// A row from the `warehouse_skip_index_manifests` table.
#[derive(Debug, Clone)]
pub struct ManifestRow {
    pub project_id: uuid::Uuid,
    pub table_name: String,
    pub r2_key: String,
    pub version: i64,
    pub blob_size: i64,
}

/// Upsert a manifest row for a table's skip index blob.
///
/// On first insert sets `version = 1`. On subsequent calls bumps
/// `version = version + 1` and returns the new version.
pub async fn upsert_manifest(
    db: &sqlx::PgPool,
    project_id: uuid::Uuid,
    table_name: &str,
    r2_key: &str,
    blob_size: i64,
    file_count: i32,
    column_count: i32,
) -> PersistenceResult<i64> {
    use sqlx::Row;

    let row = sqlx::query(
        r#"
        INSERT INTO warehouse_skip_index_manifests
            (project_id, table_name, r2_key, version, blob_size, file_count, column_count, updated_at)
        VALUES ($1, $2, $3, 1, $4, $5, $6, NOW())
        ON CONFLICT (project_id, table_name)
        DO UPDATE SET
            r2_key = EXCLUDED.r2_key,
            version = warehouse_skip_index_manifests.version + 1,
            blob_size = EXCLUDED.blob_size,
            file_count = EXCLUDED.file_count,
            column_count = EXCLUDED.column_count,
            updated_at = NOW()
        RETURNING version
        "#,
    )
    .bind(project_id)
    .bind(table_name)
    .bind(r2_key)
    .bind(blob_size)
    .bind(file_count)
    .bind(column_count)
    .fetch_one(db)
    .await
    .map_err(|e| PersistenceError::Storage(format!("Failed to upsert manifest: {}", e)))?;

    Ok(row.get("version"))
}

/// Get all manifest rows for a project.
pub async fn get_manifests_for_project(
    db: &sqlx::PgPool,
    project_id: uuid::Uuid,
) -> PersistenceResult<Vec<ManifestRow>> {
    use sqlx::Row;

    let rows = sqlx::query(
        "SELECT project_id, table_name, r2_key, version, blob_size FROM warehouse_skip_index_manifests WHERE project_id = $1"
    )
    .bind(project_id)
    .fetch_all(db)
    .await
    .map_err(|e| PersistenceError::Storage(format!("Failed to get manifests: {}", e)))?;

    Ok(rows
        .into_iter()
        .map(|row| ManifestRow {
            project_id: row.get("project_id"),
            table_name: row.get("table_name"),
            r2_key: row.get("r2_key"),
            version: row.get("version"),
            blob_size: row.get("blob_size"),
        })
        .collect())
}

/// Get all manifest rows across all projects (for startup preload).
pub async fn get_all_manifests(
    db: &sqlx::PgPool,
) -> PersistenceResult<Vec<ManifestRow>> {
    use sqlx::Row;

    let rows = sqlx::query(
        "SELECT project_id, table_name, r2_key, version, blob_size FROM warehouse_skip_index_manifests"
    )
    .fetch_all(db)
    .await
    .map_err(|e| PersistenceError::Storage(format!("Failed to get all manifests: {}", e)))?;

    Ok(rows
        .into_iter()
        .map(|row| ManifestRow {
            project_id: row.get("project_id"),
            table_name: row.get("table_name"),
            r2_key: row.get("r2_key"),
            version: row.get("version"),
            blob_size: row.get("blob_size"),
        })
        .collect())
}

/// Build a skip index blob from a HierarchicalSkipIndex, compress, upload to R2,
/// update the manifest, and optionally persist to the disk cache.
///
/// This is the unified "direct-blob" path that bypasses the per-file PG rows.
pub async fn persist_skip_index_blob(
    db: &sqlx::PgPool,
    r2_storage: &crate::warehouse::storage::r2::R2Storage,
    disk_cache: Option<&super::disk_cache::DiskIndexCache>,
    project_id: uuid::Uuid,
    table_name: &str,
    index: &super::skip_index::HierarchicalSkipIndex,
) -> PersistenceResult<()> {
    if index.total_files() == 0 {
        return Ok(());
    }

    let blob = super::blob::serialize_table_index(index);
    let blob_size = blob.len() as i64;
    let file_count = index.total_files() as i32;
    let column_count = index
        .partitions()
        .flat_map(|(_, p)| p.files.values())
        .map(|f| f.column_values.len())
        .sum::<usize>() as i32;

    let compressed = zstd::encode_all(&blob[..], 3)
        .map_err(|e| PersistenceError::Storage(format!("zstd compression failed: {}", e)))?;

    let r2_key = format!("indexes/{}/{}.fskp.zst", project_id, table_name);

    r2_storage
        .upload_parquet(&r2_key, bytes::Bytes::from(compressed))
        .await
        .map_err(|e| PersistenceError::Storage(format!("Failed to upload index blob: {}", e)))?;

    let version = upsert_manifest(
        db,
        project_id,
        table_name,
        &r2_key,
        blob_size,
        file_count,
        column_count,
    )
    .await?;

    if let Some(cache) = disk_cache {
        if let Err(e) = cache.store(project_id, table_name, &blob, version) {
            tracing::warn!(
                error = %e,
                "Failed to persist skip index blob to disk cache (non-fatal)"
            );
        }
    }

    tracing::info!(
        project_id = %project_id,
        table = %table_name,
        version = version,
        blob_size_bytes = blob_size,
        files = file_count,
        "Persisted skip index blob to R2"
    );

    Ok(())
}

/// Try to acquire a PG advisory lock for building a table's blob.
///
/// Returns `true` if the lock was acquired, `false` if another worker
/// already holds it. Uses session-level advisory locks so the lock is
/// released when `release_advisory_lock` is called (or the connection drops).
///
/// **Important**: The caller must pass the same connection to
/// `release_advisory_lock`, because session-level advisory locks are
/// bound to the database connection that acquired them.
///
/// The two `int` arguments to `pg_try_advisory_lock` are derived from
/// a hash of `(project_id, table_name)` to avoid collisions.
pub async fn try_advisory_lock(
    conn: &mut sqlx::PgConnection,
    project_id: uuid::Uuid,
    table_name: &str,
) -> bool {
    use sqlx::Row;

    let (key1, key2) = advisory_lock_keys(project_id, table_name);

    let result = sqlx::query("SELECT pg_try_advisory_lock($1, $2) AS acquired")
        .bind(key1)
        .bind(key2)
        .fetch_one(&mut *conn)
        .await;

    match result {
        Ok(row) => row.get::<bool, _>("acquired"),
        Err(e) => {
            tracing::warn!(
                error = %e,
                "Failed to acquire advisory lock, proceeding without lock"
            );
            false
        }
    }
}

/// Release a PG advisory lock previously acquired with `try_advisory_lock`.
///
/// **Important**: Must be called on the same connection that was passed
/// to `try_advisory_lock`, because session-level advisory locks are
/// bound to the connection that acquired them.
pub async fn release_advisory_lock(
    conn: &mut sqlx::PgConnection,
    project_id: uuid::Uuid,
    table_name: &str,
) {
    let (key1, key2) = advisory_lock_keys(project_id, table_name);

    if let Err(e) = sqlx::query("SELECT pg_advisory_unlock($1, $2)")
        .bind(key1)
        .bind(key2)
        .execute(&mut *conn)
        .await
    {
        tracing::warn!(error = %e, "Failed to release advisory lock");
    }
}

/// Compute the two `i32` keys for `pg_advisory_lock(int, int)`.
///
/// Hashes the full (project_id, table_name) pair together and splits the 64-bit
/// result into two 32-bit keys to minimize collision probability.
fn advisory_lock_keys(project_id: uuid::Uuid, table_name: &str) -> (i32, i32) {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    project_id.hash(&mut hasher);
    table_name.hash(&mut hasher);
    let h = hasher.finish();
    ((h >> 32) as i32, h as i32)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Integration tests for list_indexed_files (require DB) ----
    // These tests are marked #[ignore] because they require a running
    // PostgreSQL instance with the warehouse_skip_indexes table.

    #[tokio::test]
    #[ignore = "requires PostgreSQL"]
    async fn test_list_indexed_files_returns_distinct_paths() {
        let db = test_db_pool().await;
        let project_id = uuid::Uuid::new_v4();
        let table = "test_table";

        // Insert FSTs for 2 columns in the same file.
        let fst1 = build_test_fst(&["hello"]);
        let fst2 = build_test_fst(&["world"]);
        insert_test_index(&db, project_id, table, "p1", "file_a.parquet", "col1", &fst1).await;
        insert_test_index(&db, project_id, table, "p1", "file_a.parquet", "col2", &fst2).await;
        insert_test_index(&db, project_id, table, "p1", "file_b.parquet", "col1", &fst1).await;

        let files = list_indexed_files(&db, project_id, table).await.unwrap();
        assert_eq!(files.len(), 2);
        assert!(files.contains("file_a.parquet"));
        assert!(files.contains("file_b.parquet"));

        // Cleanup.
        cleanup_test_indexes(&db, project_id, table).await;
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL"]
    async fn test_list_indexed_files_scoped_to_table() {
        let db = test_db_pool().await;
        let project_id = uuid::Uuid::new_v4();
        let fst = build_test_fst(&["val"]);
        insert_test_index(&db, project_id, "table_a", "p1", "fa.parquet", "col", &fst).await;
        insert_test_index(&db, project_id, "table_b", "p1", "fb.parquet", "col", &fst).await;

        let files_a = list_indexed_files(&db, project_id, "table_a").await.unwrap();
        assert_eq!(files_a.len(), 1);
        assert!(files_a.contains("fa.parquet"));

        let files_b = list_indexed_files(&db, project_id, "table_b").await.unwrap();
        assert_eq!(files_b.len(), 1);
        assert!(files_b.contains("fb.parquet"));

        cleanup_test_indexes(&db, project_id, "table_a").await;
        cleanup_test_indexes(&db, project_id, "table_b").await;
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL"]
    async fn test_list_indexed_files_empty_table() {
        let db = test_db_pool().await;
        let project_id = uuid::Uuid::new_v4();

        let files = list_indexed_files(&db, project_id, "nonexistent").await.unwrap();
        assert!(files.is_empty());
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL"]
    async fn test_incremental_save_preserves_existing() {
        let db = test_db_pool().await;
        let project_id = uuid::Uuid::new_v4();
        let table = "incr_test";
        let fst = build_test_fst(&["alpha", "beta"]);

        insert_test_index(&db, project_id, table, "p1", "a.parquet", "col", &fst).await;
        insert_test_index(&db, project_id, table, "p1", "b.parquet", "col", &fst).await;
        // Now add a third file.
        insert_test_index(&db, project_id, table, "p1", "c.parquet", "col", &fst).await;

        let files = list_indexed_files(&db, project_id, table).await.unwrap();
        assert_eq!(files.len(), 3);

        cleanup_test_indexes(&db, project_id, table).await;
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL"]
    async fn test_delete_orphaned_indexes() {
        let db = test_db_pool().await;
        let project_id = uuid::Uuid::new_v4();
        let table = "orphan_test";
        let fst = build_test_fst(&["val"]);

        insert_test_index(&db, project_id, table, "p1", "a.parquet", "col", &fst).await;
        insert_test_index(&db, project_id, table, "p1", "b.parquet", "col", &fst).await;
        insert_test_index(&db, project_id, table, "p1", "c.parquet", "col", &fst).await;

        // Delete b.
        delete_file_skip_indexes(&db, project_id, table, &["b.parquet".to_string()]).await.unwrap();

        let files = list_indexed_files(&db, project_id, table).await.unwrap();
        assert_eq!(files.len(), 2);
        assert!(files.contains("a.parquet"));
        assert!(files.contains("c.parquet"));

        cleanup_test_indexes(&db, project_id, table).await;
    }

    // ---- Test helpers ----

    #[cfg(test)]
    fn build_test_fst(values: &[&str]) -> Vec<u8> {
        use fst::SetBuilder;
        let mut builder = SetBuilder::memory();
        let mut sorted: Vec<&str> = values.to_vec();
        sorted.sort();
        for v in sorted {
            builder.insert(v).unwrap();
        }
        builder.into_inner().unwrap()
    }

    #[cfg(test)]
    async fn test_db_pool() -> sqlx::PgPool {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/reiver_test".to_string());
        sqlx::PgPool::connect(&url).await.expect("Failed to connect to test DB")
    }

    #[cfg(test)]
    async fn insert_test_index(
        db: &sqlx::PgPool,
        project_id: uuid::Uuid,
        table: &str,
        partition: &str,
        file: &str,
        column: &str,
        fst_bytes: &[u8],
    ) {
        sqlx::query(
            r#"
            INSERT INTO warehouse_skip_indexes
                (project_id, table_name, partition_key, file_path, column_name, values_fst, row_count, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, 100, NOW(), NOW())
            ON CONFLICT (project_id, table_name, partition_key, file_path, column_name)
            DO UPDATE SET values_fst = EXCLUDED.values_fst, updated_at = NOW()
            "#,
        )
        .bind(project_id)
        .bind(table)
        .bind(partition)
        .bind(file)
        .bind(column)
        .bind(fst_bytes)
        .execute(db)
        .await
        .expect("Failed to insert test index");
    }

    #[cfg(test)]
    async fn cleanup_test_indexes(db: &sqlx::PgPool, project_id: uuid::Uuid, table: &str) {
        let _ = delete_table_skip_indexes(db, project_id, table).await;
    }

    #[cfg(test)]
    async fn cleanup_test_manifests(db: &sqlx::PgPool, project_id: uuid::Uuid) {
        let _ = sqlx::query("DELETE FROM warehouse_skip_index_manifests WHERE project_id = $1")
            .bind(project_id)
            .execute(db)
            .await;
    }

    // ---- Advisory lock key tests (no DB needed) ----

    #[test]
    fn test_advisory_lock_keys_deterministic() {
        let id = uuid::Uuid::new_v4();
        let (k1a, k2a) = advisory_lock_keys(id, "orders");
        let (k1b, k2b) = advisory_lock_keys(id, "orders");
        assert_eq!(k1a, k1b);
        assert_eq!(k2a, k2b);
    }

    #[test]
    fn test_advisory_lock_keys_differ_by_table() {
        let id = uuid::Uuid::new_v4();
        let (_, k2a) = advisory_lock_keys(id, "orders");
        let (_, k2b) = advisory_lock_keys(id, "users");
        assert_ne!(k2a, k2b);
    }

    // ---- Manifest tests (require DB) ----

    #[tokio::test]
    #[ignore = "requires PostgreSQL"]
    async fn test_manifest_upsert_and_version_bump() {
        let db = test_db_pool().await;
        let project_id = uuid::Uuid::new_v4();

        // First insert: version 1
        let v1 = upsert_manifest(&db, project_id, "t1", "indexes/t1.fskp.zst", 1024, 10, 3)
            .await
            .unwrap();
        assert_eq!(v1, 1);

        // Upsert again: version 2
        let v2 = upsert_manifest(&db, project_id, "t1", "indexes/t1.fskp.zst", 2048, 20, 3)
            .await
            .unwrap();
        assert_eq!(v2, 2);

        cleanup_test_manifests(&db, project_id).await;
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL"]
    async fn test_get_manifests_for_project() {
        let db = test_db_pool().await;
        let project_id = uuid::Uuid::new_v4();

        upsert_manifest(&db, project_id, "t1", "r2/t1", 100, 1, 1).await.unwrap();
        upsert_manifest(&db, project_id, "t2", "r2/t2", 200, 2, 2).await.unwrap();

        let manifests = get_manifests_for_project(&db, project_id).await.unwrap();
        assert_eq!(manifests.len(), 2);

        let names: Vec<&str> = manifests.iter().map(|m| m.table_name.as_str()).collect();
        assert!(names.contains(&"t1"));
        assert!(names.contains(&"t2"));

        cleanup_test_manifests(&db, project_id).await;
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL"]
    async fn test_get_all_manifests_includes_multiple_projects() {
        let db = test_db_pool().await;
        let p1 = uuid::Uuid::new_v4();
        let p2 = uuid::Uuid::new_v4();

        upsert_manifest(&db, p1, "t1", "r2/p1/t1", 100, 1, 1).await.unwrap();
        upsert_manifest(&db, p2, "t1", "r2/p2/t1", 200, 1, 1).await.unwrap();

        let all = get_all_manifests(&db).await.unwrap();
        let our_manifests: Vec<_> = all
            .iter()
            .filter(|m| m.project_id == p1 || m.project_id == p2)
            .collect();
        assert_eq!(our_manifests.len(), 2);

        cleanup_test_manifests(&db, p1).await;
        cleanup_test_manifests(&db, p2).await;
    }
}
