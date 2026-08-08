//! Partition Manager for Warm Tier
//!
//! Manages time-based partitions for warm tier data sources (Parquet on R2/S3).
//! Each partition represents one day of data for a table.
//!
//! This module is only used for warm tier. Hot tier uses ClickHouse
//! native indexing.

use chrono::{DateTime, NaiveDate, Utc};
use sqlx::{PgPool, Row};
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

pub use crate::warehouse::types::PartitionState;

/// Sync state values stored in the database.
const SYNC_STATE_PENDING: &str = "pending";
const SYNC_STATE_COMMITTED: &str = "committed";

/// A file within a logical partition (one Parquet file in R2).
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PartitionFile {
    pub id: Uuid,
    pub partition_id: Uuid,
    pub file_path: String,
    pub sync_version: i64,
    pub row_count: i64,
    pub size_bytes: i64,
    pub op_types: String,
    pub job_id: Option<Uuid>,
    pub sync_state: String,
    pub created_at: DateTime<Utc>,
}

/// Metadata for a new file to insert during an atomic swap.
#[derive(Debug, Clone)]
pub struct NewPartitionFile {
    pub file_path: String,
    pub row_count: i64,
    pub size_bytes: i64,
}

/// A partition representing one day of data for a table.
#[derive(Debug, Clone)]
pub struct Partition {
    /// Unique partition ID.
    pub id: Uuid,
    /// Source this partition belongs to.
    pub source_id: Uuid,
    /// Table name within the source.
    pub table_name: String,
    /// Date for this partition.
    pub partition_date: NaiveDate,
    /// Current state (mutable or frozen).
    pub state: PartitionState,
    /// Path to the Parquet file in R2/S3.
    pub parquet_path: Option<String>,
    /// Number of rows in this partition.
    pub row_count: i64,
    /// Size of the Parquet file in bytes.
    pub size_bytes: i64,
    /// When this partition was last updated.
    pub last_updated_at: DateTime<Utc>,
    /// When this partition was frozen (None if still mutable).
    pub frozen_at: Option<DateTime<Utc>>,
    /// When this partition was created.
    pub created_at: DateTime<Utc>,
}

// ============================================================================
// Errors
// ============================================================================

/// Errors that can occur during partition operations.
#[derive(Debug, Error)]
pub enum PartitionError {
    #[error("Partition not found: {0}")]
    NotFound(String),
    
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}

/// Result type for partition operations.
pub type PartitionResult<T> = Result<T, PartitionError>;

// ============================================================================
// Partition Manager
// ============================================================================

/// Manager for time-based partitions in warm tier.
///
/// Responsibilities:
/// - Create and track partitions in the database
/// - Manage partition file records
/// - Look up partitions for a source/table
pub struct PartitionManager {
    db: Arc<PgPool>,
}

impl PartitionManager {
    /// Create a new partition manager.
    pub fn new(db: Arc<PgPool>) -> Self {
        Self { db }
    }
    
    // ========================================================================
    // Partition CRUD
    // ========================================================================
    
    /// Get or create a partition for a specific date.
    ///
    /// If the partition already exists, it is returned. Otherwise, a new mutable
    /// partition is created.
    #[tracing::instrument(name = "warehouse.partition.get_or_create_partition", skip_all, err(Display))]
    pub async fn get_or_create_partition(
        &self,
        source_id: Uuid,
        table_name: &str,
        partition_date: NaiveDate,
    ) -> PartitionResult<Partition> {
        // Try to get existing partition
        if let Some(partition) = self.get_partition(source_id, table_name, partition_date).await? {
            return Ok(partition);
        }
        
        // Create new partition
        let id = Uuid::new_v4();
        let now = Utc::now();
        
        sqlx::query(
            r#"
            INSERT INTO warehouse_partitions (
                id, source_id, table_name, partition_date, state,
                row_count, size_bytes, last_updated_at, created_at
            )
            VALUES ($1, $2, $3, $4, 'mutable', 0, 0, $5, $5)
            ON CONFLICT (source_id, table_name, partition_date) DO NOTHING
            "#,
        )
        .bind(id)
        .bind(source_id)
        .bind(table_name)
        .bind(partition_date)
        .bind(now)
        .execute(self.db.as_ref())
        .await?;
        
        // Fetch the partition (might have been created by concurrent request)
        self.get_partition(source_id, table_name, partition_date)
            .await?
            .ok_or_else(|| PartitionError::NotFound(format!(
                "Failed to create partition for {}/{}/{}",
                source_id, table_name, partition_date
            )))
    }
    
    /// Get a partition by source, table, and date.
    #[tracing::instrument(name = "warehouse.partition.get_partition", skip_all, err(Display))]
    pub async fn get_partition(
        &self,
        source_id: Uuid,
        table_name: &str,
        partition_date: NaiveDate,
    ) -> PartitionResult<Option<Partition>> {
        let row = sqlx::query(
            r#"
            SELECT 
                id, source_id, table_name, partition_date, state,
                parquet_path, row_count, size_bytes, last_updated_at,
                frozen_at, created_at
            FROM warehouse_partitions
            WHERE source_id = $1 AND table_name = $2 AND partition_date = $3
            "#,
        )
        .bind(source_id)
        .bind(table_name)
        .bind(partition_date)
        .fetch_optional(self.db.as_ref())
        .await?;
        
        Ok(row.map(|r| self.row_to_partition(r)))
    }
    
    /// Get a partition by ID.
    #[tracing::instrument(name = "warehouse.partition.get_partition_by_id", skip_all, err(Display))]
    pub async fn get_partition_by_id(&self, partition_id: Uuid) -> PartitionResult<Option<Partition>> {
        let row = sqlx::query(
            r#"
            SELECT 
                id, source_id, table_name, partition_date, state,
                parquet_path, row_count, size_bytes, last_updated_at,
                frozen_at, created_at
            FROM warehouse_partitions
            WHERE id = $1
            "#,
        )
        .bind(partition_id)
        .fetch_optional(self.db.as_ref())
        .await?;
        
        Ok(row.map(|r| self.row_to_partition(r)))
    }
    
    /// Get partition details including project_id (for job creation).
    /// 
    /// Returns (partition, project_id) if found.
    #[tracing::instrument(name = "warehouse.partition.get_partition_with_project", skip_all, err(Display))]
    pub async fn get_partition_with_project(
        &self, 
        partition_id: Uuid
    ) -> PartitionResult<Option<(Partition, Uuid)>> {
        use sqlx::Row;
        
        let row = sqlx::query(
            r#"
            SELECT 
                p.id, p.source_id, p.table_name, p.partition_date, p.state,
                p.parquet_path, p.row_count, p.size_bytes, p.last_updated_at,
                p.frozen_at, p.created_at,
                s.project_id
            FROM warehouse_partitions p
            JOIN warehouse_sources s ON s.id = p.source_id
            WHERE p.id = $1
            "#,
        )
        .bind(partition_id)
        .fetch_optional(self.db.as_ref())
        .await?;
        
        Ok(row.map(|r| {
            let project_id: Uuid = r.get("project_id");
            (self.row_to_partition(r), project_id)
        }))
    }
    
    /// List all partitions for a source.
    #[tracing::instrument(name = "warehouse.partition.list_partitions", skip_all, err(Display))]
    pub async fn list_partitions(&self, source_id: Uuid) -> PartitionResult<Vec<Partition>> {
        let rows = sqlx::query(
            r#"
            SELECT 
                id, source_id, table_name, partition_date, state,
                parquet_path, row_count, size_bytes, last_updated_at,
                frozen_at, created_at
            FROM warehouse_partitions
            WHERE source_id = $1
            ORDER BY table_name, partition_date DESC
            "#,
        )
        .bind(source_id)
        .fetch_all(self.db.as_ref())
        .await?;
        
        Ok(rows.into_iter().map(|r| self.row_to_partition(r)).collect())
    }
    
    /// List only committed partitions for a source.
    /// 
    /// This filters out pending partitions from failed/incomplete sync jobs,
    /// ensuring only successfully synced data is returned.
    #[tracing::instrument(name = "warehouse.partition.list_committed_partitions", skip_all, err(Display))]
    pub async fn list_committed_partitions(&self, source_id: Uuid) -> PartitionResult<Vec<Partition>> {
        let rows = sqlx::query(
            r#"
            SELECT 
                id, source_id, table_name, partition_date, state,
                parquet_path, row_count, size_bytes, last_updated_at,
                frozen_at, created_at
            FROM warehouse_partitions
            WHERE source_id = $1 AND sync_state = 'committed'
            ORDER BY table_name, partition_date DESC
            "#,
        )
        .bind(source_id)
        .fetch_all(self.db.as_ref())
        .await?;
        
        Ok(rows.into_iter().map(|r| self.row_to_partition(r)).collect())
    }
    
    /// List partitions for a specific table.
    #[tracing::instrument(name = "warehouse.partition.list_table_partitions", skip_all, err(Display))]
    pub async fn list_table_partitions(
        &self,
        source_id: Uuid,
        table_name: &str,
    ) -> PartitionResult<Vec<Partition>> {
        let rows = sqlx::query(
            r#"
            SELECT 
                id, source_id, table_name, partition_date, state,
                parquet_path, row_count, size_bytes, last_updated_at,
                frozen_at, created_at
            FROM warehouse_partitions
            WHERE source_id = $1 AND table_name = $2
            ORDER BY partition_date DESC
            "#,
        )
        .bind(source_id)
        .bind(table_name)
        .fetch_all(self.db.as_ref())
        .await?;
        
        Ok(rows.into_iter().map(|r| self.row_to_partition(r)).collect())
    }
    
    // ========================================================================
    // Partition Updates
    // ========================================================================
    
    /// Update partition metadata after writing data.
    /// 
    /// Sets sync_state to 'pending' when job_id is provided. The partition
    /// will be committed to 'committed' state when the job completes successfully.
    #[tracing::instrument(name = "warehouse.partition.update_partition_data", skip_all, err(Display))]
    pub async fn update_partition_data(
        &self,
        partition_id: Uuid,
        parquet_path: &str,
        row_count: i64,
        size_bytes: i64,
        job_id: Option<Uuid>,
    ) -> PartitionResult<()> {
        let now = Utc::now();
        
        // If job_id is provided, set sync_state to 'pending' (transactional sync)
        // Otherwise, set to 'committed' for backwards compatibility
        let sync_state = if job_id.is_some() { SYNC_STATE_PENDING } else { SYNC_STATE_COMMITTED };
        
        sqlx::query(
            r#"
            UPDATE warehouse_partitions
            SET parquet_path = $1, row_count = $2, size_bytes = $3, last_updated_at = $4,
                sync_state = $5, job_id = $6
            WHERE id = $7
            "#,
        )
        .bind(parquet_path)
        .bind(row_count)
        .bind(size_bytes)
        .bind(now)
        .bind(sync_state)
        .bind(job_id)
        .bind(partition_id)
        .execute(self.db.as_ref())
        .await?;
        
        Ok(())
    }
    
    // ---------------------------------------------------------------
    // Partition file-level CRUD (warehouse_partition_files)
    // ---------------------------------------------------------------

    /// Record a new Parquet file belonging to a logical partition.
    /// The file starts in `pending` state and is committed by `commit_partition_files`.
    pub async fn add_partition_file(
        &self,
        partition_id: Uuid,
        file_path: &str,
        sync_version: i64,
        row_count: i64,
        size_bytes: i64,
        op_types: &str,
        job_id: Option<Uuid>,
    ) -> PartitionResult<Uuid> {
        let id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO warehouse_partition_files
                (id, partition_id, file_path, sync_version, row_count, size_bytes, op_types, job_id, sync_state)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending')
            "#,
        )
        .bind(id)
        .bind(partition_id)
        .bind(file_path)
        .bind(sync_version)
        .bind(row_count)
        .bind(size_bytes)
        .bind(op_types)
        .bind(job_id)
        .execute(self.db.as_ref())
        .await?;

        Ok(id)
    }

    /// List all committed file paths for a partition.
    pub async fn list_partition_files(
        &self,
        partition_id: Uuid,
    ) -> PartitionResult<Vec<PartitionFile>> {
        let rows = sqlx::query_as::<_, PartitionFile>(
            r#"
            SELECT id, partition_id, file_path, sync_version, row_count, size_bytes, op_types, job_id, sync_state, created_at
            FROM warehouse_partition_files
            WHERE partition_id = $1 AND sync_state = 'committed'
            ORDER BY sync_version, created_at
            "#,
        )
        .bind(partition_id)
        .fetch_all(self.db.as_ref())
        .await?;

        Ok(rows)
    }

    /// Commit all pending files for a given job ID (set sync_state = 'committed').
    pub async fn commit_partition_files(&self, job_id: Uuid) -> PartitionResult<u64> {
        let result = sqlx::query(
            r#"
            UPDATE warehouse_partition_files
            SET sync_state = 'committed'
            WHERE job_id = $1 AND sync_state = 'pending'
            "#,
        )
        .bind(job_id)
        .execute(self.db.as_ref())
        .await?;

        Ok(result.rows_affected())
    }

    /// Delete partition file records and return the file paths for R2 cleanup.
    pub async fn delete_partition_files(
        &self,
        file_ids: &[Uuid],
    ) -> PartitionResult<Vec<String>> {
        if file_ids.is_empty() {
            return Ok(Vec::new());
        }

        let paths: Vec<String> = sqlx::query_scalar(
            r#"
            DELETE FROM warehouse_partition_files
            WHERE id = ANY($1)
            RETURNING file_path
            "#,
        )
        .bind(file_ids)
        .fetch_all(self.db.as_ref())
        .await?;

        Ok(paths)
    }

    /// Atomically swap partition files: delete old file records and insert new ones
    /// in a single database transaction. New files are inserted as 'committed' immediately.
    ///
    /// Returns the file paths of the deleted (old) records for R2 cleanup.
    pub async fn swap_partition_files(
        &self,
        partition_id: Uuid,
        old_file_ids: &[Uuid],
        new_files: &[NewPartitionFile],
        sync_version: i64,
    ) -> PartitionResult<Vec<String>> {
        let mut tx = self.db.begin().await?;

        // Insert new file records as committed
        for nf in new_files {
            let id = Uuid::new_v4();
            sqlx::query(
                r#"
                INSERT INTO warehouse_partition_files
                    (id, partition_id, file_path, sync_version, row_count, size_bytes, op_types, job_id, sync_state)
                VALUES ($1, $2, $3, $4, $5, $6, 'I', NULL, 'committed')
                "#,
            )
            .bind(id)
            .bind(partition_id)
            .bind(&nf.file_path)
            .bind(sync_version)
            .bind(nf.row_count)
            .bind(nf.size_bytes)
            .execute(&mut *tx)
            .await?;
        }

        // Delete old file records and collect their paths
        let old_paths: Vec<String> = if old_file_ids.is_empty() {
            Vec::new()
        } else {
            sqlx::query_scalar(
                "DELETE FROM warehouse_partition_files WHERE id = ANY($1) RETURNING file_path",
            )
            .bind(old_file_ids)
            .fetch_all(&mut *tx)
            .await?
        };

        tx.commit().await?;

        Ok(old_paths)
    }

    /// Get all committed file paths for a partition (for the query rewriter to build s3() patterns).
    pub async fn get_partition_file_paths(
        &self,
        partition_id: Uuid,
    ) -> PartitionResult<Vec<String>> {
        let paths: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT file_path FROM warehouse_partition_files
            WHERE partition_id = $1 AND sync_state = 'committed'
            ORDER BY sync_version, created_at
            "#,
        )
        .bind(partition_id)
        .fetch_all(self.db.as_ref())
        .await?;

        Ok(paths)
    }

    /// Get aggregate op_types for all committed files in a partition.
    /// Returns a string like 'I', 'IU', 'IUD', etc.
    pub async fn get_partition_op_types(
        &self,
        partition_id: Uuid,
    ) -> PartitionResult<String> {
        let op_types: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT DISTINCT op_types FROM warehouse_partition_files
            WHERE partition_id = $1 AND sync_state = 'committed'
            "#,
        )
        .bind(partition_id)
        .fetch_all(self.db.as_ref())
        .await?;

        // Merge all distinct op_types into a single string
        let mut has_i = false;
        let mut has_u = false;
        let mut has_d = false;
        for ot in &op_types {
            if ot.contains('I') { has_i = true; }
            if ot.contains('U') { has_u = true; }
            if ot.contains('D') { has_d = true; }
        }

        let mut result = String::new();
        if has_i { result.push('I'); }
        if has_u { result.push('U'); }
        if has_d { result.push('D'); }
        if result.is_empty() { result.push('I'); }

        Ok(result)
    }

    /// Mark a partition as updated (touch last_updated_at).
    #[tracing::instrument(name = "warehouse.partition.touch_partition", skip_all, err(Display))]
    pub async fn touch_partition(&self, partition_id: Uuid) -> PartitionResult<()> {
        let now = Utc::now();
        
        sqlx::query(
            r#"
            UPDATE warehouse_partitions
            SET last_updated_at = $1
            WHERE id = $2
            "#,
        )
        .bind(now)
        .bind(partition_id)
        .execute(self.db.as_ref())
        .await?;
        
        Ok(())
    }
    
    // ========================================================================
    // Cleanup
    // ========================================================================
    
    /// Delete a partition and its indexes.
    #[tracing::instrument(name = "warehouse.partition.delete_partition", skip_all, err(Display))]
    pub async fn delete_partition(&self, partition_id: Uuid) -> PartitionResult<()> {
        // Indexes are deleted by CASCADE
        sqlx::query("DELETE FROM warehouse_partitions WHERE id = $1")
            .bind(partition_id)
            .execute(self.db.as_ref())
            .await?;
        
        Ok(())
    }
    
    /// Delete all partitions for a source.
    #[tracing::instrument(name = "warehouse.partition.delete_source_partitions", skip_all, err(Display))]
    pub async fn delete_source_partitions(&self, source_id: Uuid) -> PartitionResult<()> {
        // Indexes are deleted by CASCADE
        sqlx::query("DELETE FROM warehouse_partitions WHERE source_id = $1")
            .bind(source_id)
            .execute(self.db.as_ref())
            .await?;
        
        Ok(())
    }
    
    // ========================================================================
    // Helpers
    // ========================================================================
    
    fn row_to_partition(&self, row: sqlx::postgres::PgRow) -> Partition {
        let state_str: String = row.get("state");
        
        Partition {
            id: row.get("id"),
            source_id: row.get("source_id"),
            table_name: row.get("table_name"),
            partition_date: row.get("partition_date"),
            state: state_str.parse::<PartitionState>().unwrap_or_default(),
            parquet_path: row.get("parquet_path"),
            row_count: row.get("row_count"),
            size_bytes: row.get("size_bytes"),
            last_updated_at: row.get("last_updated_at"),
            frozen_at: row.get("frozen_at"),
            created_at: row.get("created_at"),
        }
    }
    
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_partition_state_conversion() {
        assert_eq!(PartitionState::Mutable.as_str(), "mutable");
        
        assert_eq!("mutable".parse::<PartitionState>().unwrap(), PartitionState::Mutable);
        assert_eq!("unknown".parse::<PartitionState>().unwrap(), PartitionState::Mutable);
    }
}
