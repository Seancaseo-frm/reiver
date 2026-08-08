//! Derived Table Manager
//!
//! CRUD operations and refresh logic for derived tables (CTAS / materialized views).
//!
//! A derived table is created from a SQL query. Its results are stored as
//! Parquet files in R2 (warm tier) and registered in `warehouse_tables` so the
//! query rewriter can resolve them like any other table.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use std::sync::Arc;
use uuid::Uuid;

use crate::warehouse::ch_client::NativePool;
use crate::warehouse::metrics::WarehouseMetrics;
use crate::warehouse::query::materializer::{
    self, MaterializeOptions, MaterializeResult,
};
use crate::warehouse::storage::r2::R2Storage;
use crate::warehouse::types::SyncInterval;

/// File count threshold for auto-triggering compaction on incremental refresh.
const DERIVED_FILE_COUNT_THRESHOLD: usize = 10;

/// Target uncompressed size for compacted Parquet files (~64 MB after compression).
const COMPACTION_TARGET_FILE_SIZE: usize = 200 * 1024 * 1024;

/// Maximum total download size for compaction input files (2 GB).
/// Prevents OOM when a derived table has accumulated many incremental appends.
const COMPACTION_MAX_INPUT_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Result of a derived table compaction.
#[derive(Debug, Default)]
pub struct CompactResult {
    pub files_before: usize,
    pub files_after: usize,
    pub rows: u64,
    pub bytes: u64,
}

/// Error types produced by `DerivedTableManager`.
///
/// Using a typed enum allows the API layer to map errors to proper HTTP status
/// codes via `downcast_ref` instead of fragile string matching.
#[derive(Debug, thiserror::Error)]
pub enum DerivedError {
    #[error("{0}")]
    NotFound(String),

    #[error("{0}")]
    Validation(String),

    #[error("{0}")]
    Conflict(String),
}

/// Refresh mode for a derived table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefreshMode {
    /// Drop all existing data and re-materialize the entire query.
    Full,
    /// Append new results alongside existing data.
    Incremental,
}

impl std::fmt::Display for RefreshMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RefreshMode::Full => write!(f, "full"),
            RefreshMode::Incremental => write!(f, "incremental"),
        }
    }
}

impl std::str::FromStr for RefreshMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "full" => Ok(RefreshMode::Full),
            "incremental" => Ok(RefreshMode::Incremental),
            _ => Err(format!("Unknown refresh mode: '{}'. Must be 'full' or 'incremental'.", s)),
        }
    }
}

/// A derived table row from the database.
#[derive(Debug, Clone, Serialize)]
pub struct DerivedTable {
    pub id: Uuid,
    pub project_id: Uuid,
    pub source_id: Uuid,
    pub name: String,
    pub sql: String,
    pub description: Option<String>,
    pub refresh_mode: RefreshMode,
    pub schedule: Option<String>,
    pub last_refreshed_at: Option<DateTime<Utc>>,
    pub last_refresh_duration_ms: Option<i64>,
    pub last_refresh_rows: Option<i64>,
    pub row_count: i64,
    pub size_bytes: i64,
    pub last_error: Option<String>,
    pub file_keys: Vec<String>,
    pub created_by: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Paginated list result for derived tables.
#[derive(Debug, Serialize)]
pub struct DerivedTableList {
    pub items: Vec<DerivedTable>,
    pub total_count: i64,
}

/// Request to create a new derived table.
#[derive(Debug, Deserialize)]
pub struct CreateDerivedTableRequest {
    pub name: String,
    pub sql: String,
    pub description: Option<String>,
    pub refresh_mode: Option<String>,
    pub schedule: Option<String>,
}

/// Manager for derived table CRUD and refresh operations.
pub struct DerivedTableManager {
    db: PgPool,
    r2_storage: Arc<R2Storage>,
    native_pool: NativePool,
    metrics: Arc<WarehouseMetrics>,
}

impl DerivedTableManager {
    pub fn new(
        db: PgPool,
        r2_storage: Arc<R2Storage>,
        native_pool: NativePool,
        metrics: Arc<WarehouseMetrics>,
    ) -> Self {
        Self { db, r2_storage, native_pool, metrics }
    }

    /// Create a derived table: register metadata, execute the query, materialize results.
    ///
    /// The caller is responsible for rewriting the SQL (via `validate_and_rewrite_query`)
    /// before calling this method. This matches the existing pattern in the API layer.
    ///
    /// If the DB transaction fails after materialization, orphaned R2 files are
    /// cleaned up before returning the error.
    #[tracing::instrument(
        name = "warehouse.derived.create",
        skip(self, rewritten_sql),
        fields(project_id = %project_id, name = %req.name),
        err(Display),
    )]
    pub async fn create(
        &self,
        project_id: Uuid,
        created_by: Option<Uuid>,
        req: &CreateDerivedTableRequest,
        rewritten_sql: &str,
    ) -> anyhow::Result<DerivedTable> {
        let refresh_mode: RefreshMode = req.refresh_mode.as_deref().unwrap_or("full")
            .parse()
            .map_err(|e: String| DerivedError::Validation(e))?;

        validate_table_name(&req.name)?;

        // Validate schedule if provided
        if let Some(sched) = req.schedule.as_deref() {
            sched.parse::<SyncInterval>().map_err(|_| {
                DerivedError::Validation(format!(
                    "Invalid schedule '{}'. Valid values: 5m, 15m, 1h, 6h, 24h, weekly, manual",
                    sched
                ))
            })?;
        }

        // Check for name uniqueness
        let existing: Option<(Uuid,)> = sqlx::query_as(
            "SELECT id FROM warehouse_derived_tables WHERE project_id = $1 AND name = $2"
        )
        .bind(project_id)
        .bind(&req.name)
        .fetch_optional(&self.db)
        .await?;

        if existing.is_some() {
            return Err(DerivedError::Conflict(format!(
                "A derived table named '{}' already exists in this project.", req.name
            )).into());
        }

        // Materialize the query to R2
        let refresh_version = Utc::now().format("%Y%m%dT%H%M%S").to_string();
        let mat_opts = MaterializeOptions {
            project_id,
            table_name: req.name.clone(),
            target_file_size: MaterializeOptions::DEFAULT_TARGET_FILE_SIZE,
            refresh_version,
            max_response_bytes: MaterializeOptions::DEFAULT_MAX_RESPONSE_BYTES,
            max_pending_memory_bytes: MaterializeOptions::DEFAULT_MAX_PENDING_MEMORY_BYTES,
        };

        let mat_result = materializer::materialize_query(
            &self.native_pool,
            rewritten_sql,
            &self.r2_storage,
            &mat_opts,
        )
        .await?;

        // Register in the database (source + table + derived_tables) atomically.
        // If the transaction fails, clean up the R2 files we just uploaded.
        let db_result = self
            .register_derived_table_in_db(project_id, created_by, req, refresh_mode, &mat_result)
            .await;

        match db_result {
            Ok(dt) => {
                self.metrics.record_derived_create();
                Ok(dt)
            }
            Err(e) => {
                self.metrics.record_derived_failure();
                tracing::warn!(
                    files = mat_result.file_keys.len(),
                    "DB transaction failed during create; cleaning up R2 files"
                );
                materializer::delete_materialized_files(&self.r2_storage, &mat_result.file_keys)
                    .await;
                Err(e)
            }
        }
    }

    /// Internal: register all DB rows for a new derived table in a single transaction.
    async fn register_derived_table_in_db(
        &self,
        project_id: Uuid,
        created_by: Option<Uuid>,
        req: &CreateDerivedTableRequest,
        refresh_mode: RefreshMode,
        mat_result: &MaterializeResult,
    ) -> anyhow::Result<DerivedTable> {
        let mut tx = self.db.begin().await?;

        let source_id = Uuid::new_v4();
        let table_id = Uuid::new_v4();
        let derived_id = Uuid::new_v4();
        let now = Utc::now();

        // 1. Create a warehouse_sources row (source_type = 'derived').
        // Store the r2_prefix in config so the federated query executor can
        // resolve the path.
        let source_config = serde_json::json!({
            "r2_prefix": &mat_result.r2_prefix,
        });
        sqlx::query(
            "INSERT INTO warehouse_sources (id, project_id, name, source_type, storage_type, config, tier, connection_config_hash, enabled, created_at, updated_at)
             VALUES ($1, $2, $3, 'derived', 'object_storage', $4, 'warm', $5, true, $6, $6)"
        )
        .bind(source_id)
        .bind(project_id)
        .bind(&req.name)
        .bind(&source_config)
        .bind(format!("derived:{}", req.name))
        .bind(now)
        .execute(&mut *tx)
        .await?;

        // 2. Build schema JSON from the Arrow schema
        let schema_json = arrow_schema_to_json(mat_result);

        // 3. Create a warehouse_tables row so the rewriter can resolve it
        sqlx::query(
            "INSERT INTO warehouse_tables (id, source_id, name, schema, r2_prefix, sync_enabled, sync_state, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, true, 'committed', $6, $6)"
        )
        .bind(table_id)
        .bind(source_id)
        .bind(&req.name)
        .bind(&schema_json)
        .bind(&mat_result.r2_prefix)
        .bind(now)
        .execute(&mut *tx)
        .await?;

        // 4. Create the warehouse_derived_tables row
        sqlx::query(
            "INSERT INTO warehouse_derived_tables (id, project_id, source_id, name, sql, description, refresh_mode, schedule, last_refreshed_at, last_refresh_duration_ms, last_refresh_rows, row_count, size_bytes, file_keys, created_by, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $16)"
        )
        .bind(derived_id)
        .bind(project_id)
        .bind(source_id)
        .bind(&req.name)
        .bind(&req.sql)
        .bind(req.description.as_deref())
        .bind(refresh_mode.to_string())
        .bind(req.schedule.as_deref())
        .bind(now)
        .bind(mat_result.duration_ms as i64)
        .bind(mat_result.row_count as i64)
        .bind(mat_result.row_count as i64)
        .bind(mat_result.bytes_written as i64)
        .bind(&mat_result.file_keys)
        .bind(created_by)
        .bind(now)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(DerivedTable {
            id: derived_id,
            project_id,
            source_id,
            name: req.name.clone(),
            sql: req.sql.clone(),
            description: req.description.clone(),
            refresh_mode,
            schedule: req.schedule.clone(),
            last_refreshed_at: Some(now),
            last_refresh_duration_ms: Some(mat_result.duration_ms as i64),
            last_refresh_rows: Some(mat_result.row_count as i64),
            row_count: mat_result.row_count as i64,
            size_bytes: mat_result.bytes_written as i64,
            last_error: None,
            file_keys: mat_result.file_keys.clone(),
            created_by,
            created_at: now,
            updated_at: now,
        })
    }

    /// List derived tables for a project with pagination.
    pub async fn list(
        &self,
        project_id: Uuid,
        limit: i64,
        offset: i64,
    ) -> anyhow::Result<DerivedTableList> {
        let total_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM warehouse_derived_tables WHERE project_id = $1"
        )
        .bind(project_id)
        .fetch_one(&self.db)
        .await?;

        let rows = sqlx::query(
            "SELECT id, project_id, source_id, name, sql, description, refresh_mode, schedule,
                    last_refreshed_at, last_refresh_duration_ms, last_refresh_rows,
                    row_count, size_bytes, last_error, file_keys, created_by, created_at, updated_at
             FROM warehouse_derived_tables
             WHERE project_id = $1
             ORDER BY name
             LIMIT $2 OFFSET $3"
        )
        .bind(project_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.db)
        .await?;

        Ok(DerivedTableList {
            items: rows.iter().map(row_to_derived_table).collect(),
            total_count,
        })
    }

    /// Get a single derived table by ID.
    pub async fn get(&self, project_id: Uuid, id: Uuid) -> anyhow::Result<Option<DerivedTable>> {
        let row = sqlx::query(
            "SELECT id, project_id, source_id, name, sql, description, refresh_mode, schedule,
                    last_refreshed_at, last_refresh_duration_ms, last_refresh_rows,
                    row_count, size_bytes, last_error, file_keys, created_by, created_at, updated_at
             FROM warehouse_derived_tables
             WHERE project_id = $1 AND id = $2"
        )
        .bind(project_id)
        .bind(id)
        .fetch_optional(&self.db)
        .await?;

        Ok(row.as_ref().map(row_to_derived_table))
    }

    /// Get a single derived table by its `warehouse_sources` source ID.
    pub async fn get_by_source_id(&self, source_id: Uuid) -> anyhow::Result<Option<DerivedTable>> {
        let row = sqlx::query(
            "SELECT id, project_id, source_id, name, sql, description, refresh_mode, schedule,
                    last_refreshed_at, last_refresh_duration_ms, last_refresh_rows,
                    row_count, size_bytes, last_error, file_keys, created_by, created_at, updated_at
             FROM warehouse_derived_tables
             WHERE source_id = $1"
        )
        .bind(source_id)
        .fetch_optional(&self.db)
        .await?;

        Ok(row.as_ref().map(row_to_derived_table))
    }

    /// Refresh a derived table (full or incremental).
    ///
    /// The `rewritten_sql` should already be the result of running the derived
    /// table's SQL through `validate_and_rewrite_query`. For incremental mode
    /// the caller must also substitute `{{last_refresh}}` before rewriting.
    ///
    /// Uses a Postgres transaction-level advisory lock to prevent concurrent
    /// refreshes. The lock is automatically released when the transaction
    /// ends — even on panic — so there is no risk of leaked locks.
    ///
    /// **Pool connection cost:** The `lock_tx` transaction is held open for the
    /// full materialization duration (potentially minutes). Each concurrent
    /// refresh of a *different* table therefore consumes one pool connection
    /// solely for locking. With a typical pool size of 10–20, this limits
    /// parallelism under heavy refresh load. If this becomes a bottleneck,
    /// consider using a dedicated single-connection pool for advisory locks or
    /// switching to application-level distributed locking (e.g. Redis).
    ///
    /// Accepts a pre-loaded `DerivedTable` so callers (API handler, consumer)
    /// that already loaded the table for SQL rewriting don't trigger a
    /// redundant DB query.
    #[tracing::instrument(
        name = "warehouse.derived.refresh",
        skip(self, dt, rewritten_sql),
        fields(project_id = %dt.project_id, derived_id = %dt.id),
        err(Display),
    )]
    pub async fn refresh(
        &self,
        dt: &DerivedTable,
        rewritten_sql: &str,
    ) -> anyhow::Result<MaterializeResult> {
        let lock_key = derive_lock_key(dt.id);

        let mut lock_tx = self.db.begin().await?;
        let acquired: bool = sqlx::query_scalar(
            "SELECT pg_try_advisory_xact_lock($1)"
        )
        .bind(lock_key)
        .fetch_one(&mut *lock_tx)
        .await?;

        if !acquired {
            return Err(DerivedError::Conflict(
                "A refresh is already in progress for this derived table".into(),
            ).into());
        }

        let result = self
            .refresh_inner(dt, rewritten_sql)
            .await;

        if let Err(e) = lock_tx.commit().await {
            tracing::debug!(
                error = %e,
                derived_id = %dt.id,
                "Failed to commit advisory lock transaction (lock still released on drop)"
            );
        }

        match &result {
            Ok(mat) => {
                self.metrics.record_derived_refresh(
                    mat.row_count, mat.bytes_written, mat.duration_ms,
                );
                self.clear_last_error(dt.id).await;
            }
            Err(e) => {
                self.metrics.record_derived_failure();
                self.write_last_error(dt.id, &e.to_string()).await;
            }
        }

        result
    }

    /// Inner refresh logic, called with advisory lock held.
    ///
    /// Accepts a pre-loaded `DerivedTable` to avoid a redundant DB round-trip
    /// (the caller already loaded it to resolve SQL / refresh mode).
    async fn refresh_inner(
        &self,
        dt: &DerivedTable,
        rewritten_sql: &str,
    ) -> anyhow::Result<MaterializeResult> {
        let mode = dt.refresh_mode;

        // Use DB-stored file keys for cleanup instead of R2 listing.
        // Fall back to R2 listing if the DB column is empty (backward compat).
        let old_keys = if mode == RefreshMode::Full {
            if !dt.file_keys.is_empty() {
                dt.file_keys.clone()
            } else {
                match self.list_existing_file_keys(dt).await {
                    Ok(keys) => keys,
                    Err(e) => {
                        tracing::warn!(
                            derived_id = %dt.id,
                            error = %e,
                            "Failed to list existing R2 file keys before refresh; old files may be orphaned"
                        );
                        Vec::new()
                    }
                }
            }
        } else {
            Vec::new()
        };

        // Step 1: Materialize new files to R2
        let refresh_version = Utc::now().format("%Y%m%dT%H%M%S").to_string();
        let mat_opts = MaterializeOptions {
            project_id: dt.project_id,
            table_name: dt.name.clone(),
            target_file_size: MaterializeOptions::DEFAULT_TARGET_FILE_SIZE,
            refresh_version,
            max_response_bytes: MaterializeOptions::DEFAULT_MAX_RESPONSE_BYTES,
            max_pending_memory_bytes: MaterializeOptions::DEFAULT_MAX_PENDING_MEMORY_BYTES,
        };

        let mat_result = materializer::materialize_query(
            &self.native_pool,
            rewritten_sql,
            &self.r2_storage,
            &mat_opts,
        )
        .await?;

        if mode == RefreshMode::Full && mat_result.row_count == 0 {
            tracing::warn!(
                derived_id = %dt.id,
                name = %dt.name,
                "Full refresh produced 0 rows — table data will be empty. \
                 This may indicate a transient upstream issue."
            );
        }

        // Step 2: Update DB metadata atomically
        let now = Utc::now();
        let new_row_count = if mode == RefreshMode::Full {
            mat_result.row_count as i64
        } else {
            dt.row_count + mat_result.row_count as i64
        };
        let new_size_bytes = if mode == RefreshMode::Full {
            mat_result.bytes_written as i64
        } else {
            dt.size_bytes + mat_result.bytes_written as i64
        };

        let schema_json = arrow_schema_to_json(&mat_result);

        let mut tx = self.db.begin().await?;

        // For full refresh, replace file_keys entirely.
        // For incremental, append new keys to existing.
        let new_file_keys = if mode == RefreshMode::Full {
            mat_result.file_keys.clone()
        } else {
            let mut keys = dt.file_keys.clone();
            keys.extend(mat_result.file_keys.clone());
            keys
        };

        sqlx::query(
            "UPDATE warehouse_derived_tables
             SET last_refreshed_at = $1,
                 last_refresh_duration_ms = $2,
                 last_refresh_rows = $3,
                 row_count = $4,
                 size_bytes = $5,
                 file_keys = $6,
                 updated_at = $1
             WHERE id = $7"
        )
        .bind(now)
        .bind(mat_result.duration_ms as i64)
        .bind(mat_result.row_count as i64)
        .bind(new_row_count)
        .bind(new_size_bytes)
        .bind(&new_file_keys)
        .bind(dt.id)
        .execute(&mut *tx)
        .await?;

        // For full refresh, update the r2_prefix and schema
        if mode == RefreshMode::Full {
            sqlx::query(
                "UPDATE warehouse_tables SET r2_prefix = $1, schema = $2, updated_at = $3 WHERE source_id = $4"
            )
            .bind(&mat_result.r2_prefix)
            .bind(&schema_json)
            .bind(now)
            .bind(dt.source_id)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        // Step 3: Delete old files AFTER DB update succeeds.
        // On failure here the old files are orphaned but the table remains queryable.
        // This is best-effort — failures are logged, not propagated.
        if mode == RefreshMode::Full && !old_keys.is_empty() {
            materializer::delete_materialized_files(&self.r2_storage, &old_keys).await;
        }

        // Step 4: Auto-compact if incremental mode has accumulated too many files.
        if mode == RefreshMode::Incremental {
            self.maybe_compact(dt).await;
        }

        Ok(mat_result)
    }

    /// Append query results to an existing derived table (INSERT INTO ... SELECT).
    ///
    /// The `rewritten_sql` should already be passed through `validate_and_rewrite_query`.
    /// Validates that the appended data's schema is compatible with the existing table.
    ///
    /// Accepts a pre-loaded `DerivedTable` so callers that already loaded the
    /// table don't trigger a redundant DB query.
    ///
    /// Uses the same advisory lock as `refresh()` to prevent concurrent
    /// append/refresh corruption.
    #[tracing::instrument(
        name = "warehouse.derived.append",
        skip(self, dt, rewritten_sql),
        fields(project_id = %dt.project_id, derived_id = %dt.id),
        err(Display),
    )]
    pub async fn append(
        &self,
        dt: &DerivedTable,
        rewritten_sql: &str,
    ) -> anyhow::Result<MaterializeResult> {
        let lock_key = derive_lock_key(dt.id);

        let mut lock_tx = self.db.begin().await?;
        let acquired: bool = sqlx::query_scalar(
            "SELECT pg_try_advisory_xact_lock($1)"
        )
        .bind(lock_key)
        .fetch_one(&mut *lock_tx)
        .await?;

        if !acquired {
            return Err(DerivedError::Conflict(
                "A refresh or append is already in progress for this derived table".into(),
            ).into());
        }

        let result = self
            .append_inner(dt, rewritten_sql)
            .await;

        if let Err(e) = lock_tx.commit().await {
            tracing::debug!(
                error = %e,
                derived_id = %dt.id,
                "Failed to commit advisory lock transaction (lock still released on drop)"
            );
        }

        match &result {
            Ok(mat) => {
                self.metrics.record_derived_append(
                    mat.row_count, mat.bytes_written, mat.duration_ms,
                );
                self.clear_last_error(dt.id).await;
            }
            Err(e) => {
                self.metrics.record_derived_failure();
                self.write_last_error(dt.id, &e.to_string()).await;
            }
        }

        result
    }

    /// Inner append logic, called with advisory lock held.
    async fn append_inner(
        &self,
        dt: &DerivedTable,
        rewritten_sql: &str,
    ) -> anyhow::Result<MaterializeResult> {
        let existing_schema: Option<serde_json::Value> = sqlx::query_scalar(
            "SELECT schema FROM warehouse_tables WHERE source_id = $1"
        )
        .bind(dt.source_id)
        .fetch_optional(&self.db)
        .await?;

        let refresh_version = Utc::now().format("%Y%m%dT%H%M%S").to_string();
        let mat_opts = MaterializeOptions {
            project_id: dt.project_id,
            table_name: dt.name.clone(),
            target_file_size: MaterializeOptions::DEFAULT_TARGET_FILE_SIZE,
            refresh_version,
            max_response_bytes: MaterializeOptions::DEFAULT_MAX_RESPONSE_BYTES,
            max_pending_memory_bytes: MaterializeOptions::DEFAULT_MAX_PENDING_MEMORY_BYTES,
        };

        let mat_result = materializer::materialize_query(
            &self.native_pool,
            rewritten_sql,
            &self.r2_storage,
            &mat_opts,
        )
        .await?;

        // Validate schema compatibility.
        // If existing schema is NULL, set it from the appended data within the
        // transaction below so both updates are atomic.
        let new_schema_json = arrow_schema_to_json(&mat_result);
        if let Some(ref existing) = existing_schema {
            if let Err(mismatch) = check_schema_compatibility(existing, &new_schema_json) {
                materializer::delete_materialized_files(&self.r2_storage, &mat_result.file_keys)
                    .await;
                return Err(DerivedError::Validation(
                    format!("Schema mismatch on append: {}", mismatch),
                ).into());
            }
        }

        // Wrap schema SET + metadata UPDATE in a single transaction.
        // On failure, clean up the R2 files we just uploaded.
        let now = Utc::now();
        let db_result = async {
            let mut tx = self.db.begin().await?;

            if existing_schema.is_none() {
                sqlx::query(
                    "UPDATE warehouse_tables SET schema = $1, updated_at = $2 WHERE source_id = $3"
                )
                .bind(&new_schema_json)
                .bind(now)
                .bind(dt.source_id)
                .execute(&mut *tx)
                .await?;
            }

            let mut updated_keys = dt.file_keys.clone();
            updated_keys.extend(mat_result.file_keys.clone());

            sqlx::query(
                "UPDATE warehouse_derived_tables
                 SET last_refreshed_at = $1,
                     last_refresh_duration_ms = $2,
                     last_refresh_rows = $3,
                     row_count = row_count + $3,
                     size_bytes = size_bytes + $4,
                     file_keys = $5,
                     updated_at = $1
                 WHERE id = $6"
            )
            .bind(now)
            .bind(mat_result.duration_ms as i64)
            .bind(mat_result.row_count as i64)
            .bind(mat_result.bytes_written as i64)
            .bind(&updated_keys)
            .bind(dt.id)
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok::<(), anyhow::Error>(())
        }
        .await;

        if let Err(e) = db_result {
            tracing::warn!(
                files = mat_result.file_keys.len(),
                "DB transaction failed during append; cleaning up R2 files"
            );
            materializer::delete_materialized_files(&self.r2_storage, &mat_result.file_keys).await;
            return Err(e);
        }

        self.maybe_compact(dt).await;

        Ok(mat_result)
    }

    /// Update the refresh schedule for a derived table.
    ///
    /// Validates that the schedule string is a valid `SyncInterval` before
    /// storing it, so the scheduler doesn't silently skip it later.
    pub async fn set_schedule(
        &self,
        project_id: Uuid,
        derived_id: Uuid,
        schedule: Option<&str>,
    ) -> anyhow::Result<()> {
        // Validate the schedule string parses as a SyncInterval.
        if let Some(sched) = schedule {
            sched.parse::<SyncInterval>().map_err(|_| {
                DerivedError::Validation(format!(
                    "Invalid schedule '{}'. Valid values: 5m, 15m, 1h, 6h, 24h, weekly, manual",
                    sched
                ))
            })?;
        }

        let result = sqlx::query(
            "UPDATE warehouse_derived_tables SET schedule = $1, updated_at = NOW()
             WHERE project_id = $2 AND id = $3"
        )
        .bind(schedule)
        .bind(project_id)
        .bind(derived_id)
        .execute(&self.db)
        .await?;

        if result.rows_affected() == 0 {
            return Err(DerivedError::NotFound("Derived table not found".into()).into());
        }

        Ok(())
    }

    /// Delete a derived table: remove DB rows first, then R2 files best-effort.
    ///
    /// DB deletion is prioritized so the table becomes logically invisible even
    /// if R2 cleanup fails. Orphaned R2 files are logged but don't block
    /// the delete from succeeding.
    #[tracing::instrument(
        name = "warehouse.derived.delete",
        skip(self),
        fields(project_id = %project_id, derived_id = %derived_id),
        err(Display),
    )]
    pub async fn delete(
        &self,
        project_id: Uuid,
        derived_id: Uuid,
    ) -> anyhow::Result<()> {
        let dt = self.get(project_id, derived_id).await?
            .ok_or_else(|| DerivedError::NotFound("Derived table not found".into()))?;

        // Collect file keys before deleting DB rows (we need the metadata to find them).
        // Log a warning if listing fails so orphaned files are visible.
        // Prefer DB-stored file keys; fall back to R2 listing for backward compat.
        let keys = if !dt.file_keys.is_empty() {
            dt.file_keys.clone()
        } else {
            match self.list_existing_file_keys(&dt).await {
                Ok(keys) => keys,
                Err(e) => {
                    tracing::warn!(
                        derived_id = %derived_id,
                        error = %e,
                        "Failed to list R2 file keys before delete; old files may be orphaned"
                    );
                    Vec::new()
                }
            }
        };

        // Delete DB rows first (cascade from source deletes tables and derived_tables entries).
        // This makes the table logically invisible immediately.
        sqlx::query("DELETE FROM warehouse_sources WHERE id = $1")
            .bind(dt.source_id)
            .execute(&self.db)
            .await?;

        if !keys.is_empty() {
            materializer::delete_materialized_files(&self.r2_storage, &keys).await;
        }

        self.metrics.record_derived_delete();

        Ok(())
    }

    /// Compact Parquet files for a derived table.
    ///
    /// Reads all existing Parquet files, merges them into fewer larger files,
    /// atomically updates the R2 prefix, and deletes old files.
    ///
    /// Uses an advisory lock to prevent concurrent compaction/refresh.
    #[tracing::instrument(
        name = "warehouse.derived.compact",
        skip(self, dt),
        fields(project_id = %dt.project_id, derived_id = %dt.id),
        err(Display),
    )]
    pub async fn compact(&self, dt: &DerivedTable) -> anyhow::Result<CompactResult> {
        let lock_key = derive_lock_key(dt.id);

        let mut lock_tx = self.db.begin().await?;
        let acquired: bool = sqlx::query_scalar(
            "SELECT pg_try_advisory_xact_lock($1)"
        )
        .bind(lock_key)
        .fetch_one(&mut *lock_tx)
        .await?;

        if !acquired {
            return Err(DerivedError::Conflict(
                "A refresh or compaction is already in progress".into(),
            ).into());
        }

        let result = self.compact_inner(dt).await;

        if let Err(e) = lock_tx.commit().await {
            tracing::debug!(
                error = %e,
                derived_id = %dt.id,
                "Failed to commit advisory lock transaction (lock still released on drop)"
            );
        }

        result
    }

    /// Inner compaction logic.
    async fn compact_inner(&self, dt: &DerivedTable) -> anyhow::Result<CompactResult> {
        use crate::warehouse::sync::merge::read_parquet_bytes;
        use crate::warehouse::sync::sync_executor::split_batches_by_size;
        use crate::warehouse::parquet::WriteOptions;
        use crate::warehouse::parquet_stats::write_parquet_with_stats;

        let r2_prefix = format!(
            "projects/{}/derived/{}/",
            dt.project_id, dt.name,
        );
        let objects = self.r2_storage.list_objects(&r2_prefix).await
            .map_err(|e| anyhow::anyhow!("Failed to list R2 objects: {}", e))?;
        let parquet_objects: Vec<_> = objects
            .into_iter()
            .filter(|o| o.key.ends_with(".parquet"))
            .collect();

        if parquet_objects.len() <= 1 {
            return Ok(CompactResult::default());
        }

        let total_input_bytes: u64 = parquet_objects.iter().map(|o| o.size).sum();
        if total_input_bytes > COMPACTION_MAX_INPUT_BYTES {
            anyhow::bail!(
                "Compaction aborted: total input size ({:.1} GB) exceeds {:.1} GB limit. \
                 Consider a full refresh instead.",
                total_input_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
                COMPACTION_MAX_INPUT_BYTES as f64 / (1024.0 * 1024.0 * 1024.0),
            );
        }

        let old_keys: Vec<String> = parquet_objects.iter().map(|o| o.key.clone()).collect();

        let mut all_batches = Vec::new();
        let mut downloaded_bytes: u64 = 0;
        for key in &old_keys {
            let data = self.r2_storage.download(key).await
                .map_err(|e| anyhow::anyhow!(
                    "Aborting compaction: failed to download Parquet file {}: {}",
                    key, e
                ))?;

            downloaded_bytes += data.len() as u64;
            if downloaded_bytes > COMPACTION_MAX_INPUT_BYTES {
                anyhow::bail!(
                    "Compaction aborted: downloaded {:.1} GB exceeds memory budget",
                    downloaded_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
                );
            }

            let batches = read_parquet_bytes(&data)
                .map_err(|e| anyhow::anyhow!(
                    "Aborting compaction: failed to read Parquet file {}: {}",
                    key, e
                ))?;

            all_batches.extend(batches);
        }

        if all_batches.is_empty() {
            return Ok(CompactResult::default());
        }

        let schema = all_batches[0].schema();
        let chunks = split_batches_by_size(&all_batches, COMPACTION_TARGET_FILE_SIZE);

        let compact_version = Utc::now().format("%Y%m%dT%H%M%S").to_string();
        let prefix = format!("projects/{}/derived/{}", dt.project_id, dt.name);
        let mut new_keys = Vec::new();
        let mut total_bytes: u64 = 0;
        let mut total_rows: u64 = 0;

        for (seq, chunk) in chunks.iter().enumerate() {
            let (parquet_bytes, stats) = write_parquet_with_stats(
                schema.clone(), chunk, WriteOptions::default(),
            ).map_err(|e| anyhow::anyhow!("Failed to write compacted Parquet: {}", e))?;

            let rows: u64 = chunk.iter().map(|b| b.num_rows() as u64).sum();
            let key = format!("{}/compacted_{}_{:04}.parquet", prefix, compact_version, seq);

            self.r2_storage
                .upload_parquet_with_stats(&key, parquet_bytes, &stats)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to upload compacted file: {}", e))?;

            total_bytes += stats.size_bytes;
            total_rows += rows;
            new_keys.push(key);
        }

        // Atomically update both warehouse_derived_tables and warehouse_tables
        // so the query rewriter always sees consistent metadata.
        let now = Utc::now();
        let mut tx = self.db.begin().await?;

        sqlx::query(
            "UPDATE warehouse_derived_tables
             SET row_count = $1, size_bytes = $2, file_keys = $3, updated_at = $4
             WHERE id = $5"
        )
        .bind(total_rows as i64)
        .bind(total_bytes as i64)
        .bind(&new_keys)
        .bind(now)
        .bind(dt.id)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "UPDATE warehouse_tables SET r2_prefix = $1, updated_at = $2 WHERE source_id = $3"
        )
        .bind(&prefix)
        .bind(now)
        .bind(dt.source_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        // Delete old files (best-effort, batched)
        materializer::delete_materialized_files(&self.r2_storage, &old_keys).await;

        Ok(CompactResult {
            files_before: old_keys.len(),
            files_after: new_keys.len(),
            rows: total_rows,
            bytes: total_bytes,
        })
    }

    /// Persist an error message to `last_error` so the API exposes it.
    /// Best-effort: a DB failure here is logged but does not propagate.
    async fn write_last_error(&self, derived_id: Uuid, message: &str) {
        let truncated = if message.len() > 2048 {
            let mut end = 2048;
            while !message.is_char_boundary(end) {
                end -= 1;
            }
            &message[..end]
        } else {
            message
        };
        if let Err(e) = sqlx::query(
            "UPDATE warehouse_derived_tables SET last_error = $1, updated_at = NOW() WHERE id = $2"
        )
        .bind(truncated)
        .bind(derived_id)
        .execute(&self.db)
        .await
        {
            tracing::warn!(
                derived_id = %derived_id,
                error = %e,
                "Failed to persist last_error to database"
            );
        }
    }

    /// Clear `last_error` after a successful refresh or append.
    async fn clear_last_error(&self, derived_id: Uuid) {
        if let Err(e) = sqlx::query(
            "UPDATE warehouse_derived_tables SET last_error = NULL, updated_at = NOW() WHERE id = $1"
        )
        .bind(derived_id)
        .execute(&self.db)
        .await
        {
            tracing::warn!(
                derived_id = %derived_id,
                error = %e,
                "Failed to clear last_error in database"
            );
        }
    }

    /// Auto-trigger compaction if the number of files exceeds the threshold.
    async fn maybe_compact(&self, dt: &DerivedTable) {
        match self.list_existing_file_keys(dt).await {
            Ok(keys) if keys.len() > DERIVED_FILE_COUNT_THRESHOLD => {
                tracing::info!(
                    derived_id = %dt.id,
                    file_count = keys.len(),
                    "Auto-compacting derived table (file count threshold exceeded)"
                );
                if let Err(e) = self.compact_inner(dt).await {
                    tracing::warn!(
                        derived_id = %dt.id,
                        error = %e,
                        "Auto-compaction failed (non-fatal)"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    derived_id = %dt.id,
                    error = %e,
                    "Failed to list files for auto-compaction check"
                );
            }
            _ => {}
        }
    }

    /// List R2 object keys for all Parquet files belonging to a derived table.
    async fn list_existing_file_keys(&self, dt: &DerivedTable) -> anyhow::Result<Vec<String>> {
        let prefix = format!(
            "projects/{}/derived/{}/",
            dt.project_id, dt.name,
        );
        let objects = self.r2_storage.list_objects(&prefix).await
            .map_err(|e| anyhow::anyhow!("Failed to list R2 objects: {}", e))?;

        Ok(objects
            .into_iter()
            .filter(|o| o.key.ends_with(".parquet"))
            .map(|o| o.key)
            .collect())
    }
}

// ============================================================================
// Helpers
// ============================================================================

fn row_to_derived_table(row: &sqlx::postgres::PgRow) -> DerivedTable {
    DerivedTable {
        id: row.get("id"),
        project_id: row.get("project_id"),
        source_id: row.get("source_id"),
        name: row.get("name"),
        sql: row.get("sql"),
        description: row.get("description"),
        refresh_mode: row.get::<String, _>("refresh_mode")
            .parse::<RefreshMode>()
            .unwrap_or(RefreshMode::Full),
        schedule: row.get("schedule"),
        last_refreshed_at: row.get("last_refreshed_at"),
        last_refresh_duration_ms: row.get("last_refresh_duration_ms"),
        last_refresh_rows: row.get("last_refresh_rows"),
        row_count: row.get("row_count"),
        size_bytes: row.get("size_bytes"),
        last_error: row.get("last_error"),
        file_keys: row.get::<Vec<String>, _>("file_keys"),
        created_by: row.get("created_by"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

/// Convert the Arrow schema from a materialization result to a JSON array
/// suitable for the `warehouse_tables.schema` JSONB column.
///
/// Uses the same stable `field_to_json` serialization as the rest of the
/// warehouse type system, so stored schemas survive Arrow library upgrades.
fn arrow_schema_to_json(result: &MaterializeResult) -> serde_json::Value {
    use crate::warehouse::types::field_to_json;

    let columns: Vec<serde_json::Value> = result
        .arrow_schema
        .fields()
        .iter()
        .map(|field| field_to_json(field))
        .collect();

    serde_json::Value::Array(columns)
}

/// Check that a new schema is compatible with an existing schema for append operations.
///
/// Both schemas are JSON arrays produced by `field_to_json`, each element
/// containing `name`, `nullable`, and `dataType` fields.
/// Checks column names, data types, and nullability. Column order must be identical.
fn check_schema_compatibility(
    existing: &serde_json::Value,
    new_schema: &serde_json::Value,
) -> Result<(), String> {
    let existing_cols = existing.as_array().ok_or("Existing schema is not an array")?;
    let new_cols = new_schema.as_array().ok_or("New schema is not an array")?;

    if existing_cols.len() != new_cols.len() {
        return Err(format!(
            "Column count mismatch: existing table has {} columns, new data has {}",
            existing_cols.len(),
            new_cols.len()
        ));
    }

    for (i, (existing_col, new_col)) in existing_cols.iter().zip(new_cols.iter()).enumerate() {
        let existing_name = existing_col["name"].as_str().unwrap_or("");
        let new_name = new_col["name"].as_str().unwrap_or("");
        if existing_name != new_name {
            return Err(format!(
                "Column {} name mismatch: expected '{}', got '{}'",
                i, existing_name, new_name
            ));
        }

        // Compare the structured `dataType` object (from `field_to_json`).
        let existing_type = &existing_col["dataType"];
        let new_type = &new_col["dataType"];
        if existing_type != new_type {
            return Err(format!(
                "Column '{}' type mismatch: expected {}, got {}",
                existing_name, existing_type, new_type
            ));
        }

        // ClickHouse's ArrowStream format often marks all output columns as
        // nullable regardless of the underlying schema.  A strict rejection
        // here would cause spurious failures when the same query is re-run and
        // the engine returns `nullable: true` for a column that was originally
        // stored as `nullable: false`.  We therefore log a warning instead of
        // rejecting the append.
        let existing_nullable = existing_col["nullable"].as_bool().unwrap_or(true);
        let new_nullable = new_col["nullable"].as_bool().unwrap_or(true);
        if !existing_nullable && new_nullable {
            tracing::warn!(
                column = existing_name,
                "Column nullability mismatch on append: existing column is NOT NULL but \
                 new data is nullable (common with ClickHouse ArrowStream output; allowing)"
            );
        }
    }

    Ok(())
}

/// Derive an advisory lock key from a derived table UUID.
///
/// Uses the first 8 bytes of the UUID, which provides ~60 bits of entropy
/// for UUIDv4 (birthday paradox collision at ~2^30 tables — practically safe).
fn derive_lock_key(derived_id: Uuid) -> i64 {
    derived_id.as_bytes()[..8]
        .try_into()
        .map(i64::from_le_bytes)
        .unwrap_or(0)
}

/// Validate that a derived table name is safe for use in R2 paths and SQL.
pub fn validate_table_name(name: &str) -> anyhow::Result<()> {
    if name.is_empty() || name.len() > 128 {
        return Err(DerivedError::Validation(
            "Table name must be 1-128 characters long".into(),
        ).into());
    }
    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(DerivedError::Validation(
            "Table name may only contain alphanumeric characters and underscores".into(),
        ).into());
    }
    if name.starts_with('_') {
        return Err(DerivedError::Validation(
            "Table name may not start with an underscore".into(),
        ).into());
    }
    Ok(())
}

/// Substitute `{{last_refresh}}` in incremental SQL with the actual timestamp.
///
/// # SQL Injection Safety
///
/// The substituted value is always produced by `DateTime<Utc>::format()` with
/// a fixed `%Y-%m-%d %H:%M:%S` pattern, yielding only digits, dashes, colons,
/// and a space. No user-controllable input reaches the replacement string, so
/// SQL injection through this path is not possible.
pub fn substitute_last_refresh(sql: &str, last_refreshed_at: Option<DateTime<Utc>>) -> String {
    let ts = last_refreshed_at
        .map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| "1970-01-01 00:00:00".to_string());

    sql.replace("{{last_refresh}}", &ts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    // =====================================================
    // Table name validation
    // =====================================================

    #[test]
    fn test_validate_table_name_valid() {
        assert!(validate_table_name("token_transfers").is_ok());
        assert!(validate_table_name("MyTable123").is_ok());
        assert!(validate_table_name("a").is_ok());
        assert!(validate_table_name("table_1_v2").is_ok());
        assert!(validate_table_name("X").is_ok());
    }

    #[test]
    fn test_validate_table_name_empty() {
        let err = validate_table_name("").unwrap_err();
        assert!(err.to_string().contains("1-128 characters"));
    }

    #[test]
    fn test_validate_table_name_too_long() {
        let long_name = "a".repeat(129);
        let err = validate_table_name(&long_name).unwrap_err();
        assert!(err.to_string().contains("1-128 characters"));
    }

    #[test]
    fn test_validate_table_name_max_length() {
        let max_name = "a".repeat(128);
        assert!(validate_table_name(&max_name).is_ok());
    }

    #[test]
    fn test_validate_table_name_starts_with_underscore() {
        let err = validate_table_name("_private").unwrap_err();
        assert!(err.to_string().contains("underscore"));
    }

    #[test]
    fn test_validate_table_name_special_chars() {
        assert!(validate_table_name("has spaces").is_err());
        assert!(validate_table_name("has-dashes").is_err());
        assert!(validate_table_name("path/traversal").is_err());
        assert!(validate_table_name("dots.not.ok").is_err());
        assert!(validate_table_name("semi;colon").is_err());
        assert!(validate_table_name("tab\there").is_err());
        assert!(validate_table_name("new\nline").is_err());
        assert!(validate_table_name("SELECT").is_ok()); // SQL keyword but valid chars
    }

    // =====================================================
    // Last refresh substitution
    // =====================================================

    #[test]
    fn test_substitute_last_refresh_with_timestamp() {
        let sql = "SELECT * FROM events WHERE created_at > '{{last_refresh}}'";
        let ts = DateTime::parse_from_rfc3339("2026-01-15T10:30:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let result = substitute_last_refresh(sql, Some(ts));
        assert_eq!(
            result,
            "SELECT * FROM events WHERE created_at > '2026-01-15 10:30:00'"
        );
    }

    #[test]
    fn test_substitute_last_refresh_none_defaults_to_epoch() {
        let sql = "SELECT * FROM events WHERE created_at > '{{last_refresh}}'";
        let result = substitute_last_refresh(sql, None);
        assert_eq!(
            result,
            "SELECT * FROM events WHERE created_at > '1970-01-01 00:00:00'"
        );
    }

    #[test]
    fn test_substitute_last_refresh_multiple_placeholders() {
        let sql = "SELECT * FROM events WHERE created_at > '{{last_refresh}}' AND updated_at > '{{last_refresh}}'";
        let ts = DateTime::parse_from_rfc3339("2026-06-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let result = substitute_last_refresh(sql, Some(ts));
        assert!(result.contains("2026-06-01 00:00:00"));
        assert!(!result.contains("{{last_refresh}}"));
        assert_eq!(result.matches("2026-06-01 00:00:00").count(), 2);
    }

    #[test]
    fn test_substitute_last_refresh_no_placeholder() {
        let sql = "SELECT * FROM events";
        let result = substitute_last_refresh(sql, None);
        assert_eq!(result, "SELECT * FROM events");
    }

    // =====================================================
    // RefreshMode
    // =====================================================

    #[test]
    fn test_refresh_mode_parsing() {
        assert_eq!("full".parse::<RefreshMode>().unwrap(), RefreshMode::Full);
        assert_eq!("incremental".parse::<RefreshMode>().unwrap(), RefreshMode::Incremental);
        assert!("invalid".parse::<RefreshMode>().is_err());
    }

    #[test]
    fn test_refresh_mode_case_insensitive() {
        assert_eq!("Full".parse::<RefreshMode>().unwrap(), RefreshMode::Full);
        assert_eq!("FULL".parse::<RefreshMode>().unwrap(), RefreshMode::Full);
        assert_eq!("Incremental".parse::<RefreshMode>().unwrap(), RefreshMode::Incremental);
        assert_eq!("INCREMENTAL".parse::<RefreshMode>().unwrap(), RefreshMode::Incremental);
    }

    #[test]
    fn test_refresh_mode_display() {
        assert_eq!(RefreshMode::Full.to_string(), "full");
        assert_eq!(RefreshMode::Incremental.to_string(), "incremental");
    }

    #[test]
    fn test_refresh_mode_roundtrip() {
        let modes = [RefreshMode::Full, RefreshMode::Incremental];
        for mode in modes {
            let s = mode.to_string();
            let parsed: RefreshMode = s.parse().unwrap();
            assert_eq!(parsed, mode);
        }
    }

    #[test]
    fn test_refresh_mode_invalid_error_message() {
        let err = "oops".parse::<RefreshMode>().unwrap_err();
        assert!(err.contains("oops"));
        assert!(err.contains("full"));
        assert!(err.contains("incremental"));
    }

    // =====================================================
    // Arrow schema to JSON
    // =====================================================

    #[test]
    fn test_arrow_schema_to_json_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Float64, true),
        ]));

        let result = MaterializeResult {
            row_count: 0,
            bytes_written: 0,
            files_created: 0,
            r2_prefix: String::new(),
            arrow_schema: schema,
            file_keys: vec![],
            duration_ms: 0,
        };

        let json = arrow_schema_to_json(&result);
        let columns = json.as_array().unwrap();
        assert_eq!(columns.len(), 3);

        assert_eq!(columns[0]["name"], "id");
        assert_eq!(columns[0]["nullable"], false);
        assert_eq!(columns[0]["dataType"]["type"], "int64");

        assert_eq!(columns[1]["name"], "name");
        assert_eq!(columns[1]["nullable"], true);
        assert_eq!(columns[1]["dataType"]["type"], "utf8");

        assert_eq!(columns[2]["name"], "amount");
        assert_eq!(columns[2]["nullable"], true);
        assert_eq!(columns[2]["dataType"]["type"], "float64");
    }

    #[test]
    fn test_arrow_schema_to_json_empty_schema() {
        let schema = Arc::new(Schema::empty());
        let result = MaterializeResult {
            row_count: 0,
            bytes_written: 0,
            files_created: 0,
            r2_prefix: String::new(),
            arrow_schema: schema,
            file_keys: vec![],
            duration_ms: 0,
        };

        let json = arrow_schema_to_json(&result);
        let columns = json.as_array().unwrap();
        assert!(columns.is_empty());
    }

    #[test]
    fn test_arrow_schema_to_json_data_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None), false),
            Field::new("flag", DataType::Boolean, false),
            Field::new("data", DataType::Binary, true),
        ]));

        let result = MaterializeResult {
            row_count: 0,
            bytes_written: 0,
            files_created: 0,
            r2_prefix: String::new(),
            arrow_schema: schema,
            file_keys: vec![],
            duration_ms: 0,
        };

        let json = arrow_schema_to_json(&result);
        let columns = json.as_array().unwrap();
        assert_eq!(columns.len(), 3);
        assert_eq!(columns[0]["name"], "ts");
        assert_eq!(columns[0]["dataType"]["type"], "timestamp");
        assert_eq!(columns[1]["name"], "flag");
        assert_eq!(columns[1]["dataType"]["type"], "bool");
        assert_eq!(columns[2]["name"], "data");
        assert_eq!(columns[2]["dataType"]["type"], "binary");

        // dataType field should be present and be an object
        for col in columns {
            assert!(col["dataType"].is_object());
        }
    }

    // =====================================================
    // DerivedTable struct
    // =====================================================

    #[test]
    fn test_derived_table_serialization() {
        let dt = DerivedTable {
            id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            source_id: Uuid::new_v4(),
            name: "test_table".to_string(),
            sql: "SELECT 1 AS x".to_string(),
            description: Some("A test table".to_string()),
            refresh_mode: RefreshMode::Full,
            schedule: Some("0 * * * *".to_string()),
            last_refreshed_at: Some(Utc::now()),
            last_refresh_duration_ms: Some(500),
            last_refresh_rows: Some(100),
            row_count: 100,
            size_bytes: 4096,
            last_error: None,
            file_keys: vec!["projects/p1/derived/test_table/part_0000.parquet".to_string()],
            created_by: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let json = serde_json::to_value(&dt).unwrap();
        assert_eq!(json["name"], "test_table");
        assert_eq!(json["sql"], "SELECT 1 AS x");
        assert_eq!(json["refresh_mode"], "full");
        assert_eq!(json["row_count"], 100);
        assert_eq!(json["size_bytes"], 4096);
    }

    #[test]
    fn test_create_derived_table_request_deserialization() {
        let json = serde_json::json!({
            "name": "my_derived",
            "sql": "SELECT * FROM source_table",
            "description": "Some description",
            "refresh_mode": "incremental",
            "schedule": "0 */6 * * *"
        });

        let req: CreateDerivedTableRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.name, "my_derived");
        assert_eq!(req.sql, "SELECT * FROM source_table");
        assert_eq!(req.description.as_deref(), Some("Some description"));
        assert_eq!(req.refresh_mode.as_deref(), Some("incremental"));
        assert_eq!(req.schedule.as_deref(), Some("0 */6 * * *"));
    }

    // =====================================================
    // Schema compatibility
    // =====================================================

    #[test]
    fn test_schema_compatibility_matching() {
        let schema_a = serde_json::json!([
            {"name": "id", "dataType": {"type": "int64"}, "nullable": false},
            {"name": "name", "dataType": {"type": "utf8"}, "nullable": true},
        ]);
        let schema_b = serde_json::json!([
            {"name": "id", "dataType": {"type": "int64"}, "nullable": false},
            {"name": "name", "dataType": {"type": "utf8"}, "nullable": true},
        ]);
        assert!(check_schema_compatibility(&schema_a, &schema_b).is_ok());
    }

    #[test]
    fn test_schema_compatibility_column_count_mismatch() {
        let existing = serde_json::json!([
            {"name": "id", "dataType": {"type": "int64"}, "nullable": false},
        ]);
        let new_schema = serde_json::json!([
            {"name": "id", "dataType": {"type": "int64"}, "nullable": false},
            {"name": "extra", "dataType": {"type": "utf8"}, "nullable": true},
        ]);
        let err = check_schema_compatibility(&existing, &new_schema).unwrap_err();
        assert!(err.contains("Column count mismatch"));
    }

    #[test]
    fn test_schema_compatibility_name_mismatch() {
        let existing = serde_json::json!([
            {"name": "id", "dataType": {"type": "int64"}, "nullable": false},
        ]);
        let new_schema = serde_json::json!([
            {"name": "user_id", "dataType": {"type": "int64"}, "nullable": false},
        ]);
        let err = check_schema_compatibility(&existing, &new_schema).unwrap_err();
        assert!(err.contains("name mismatch"));
    }

    #[test]
    fn test_schema_compatibility_type_mismatch() {
        let existing = serde_json::json!([
            {"name": "id", "dataType": {"type": "int64"}, "nullable": false},
        ]);
        let new_schema = serde_json::json!([
            {"name": "id", "dataType": {"type": "utf8"}, "nullable": false},
        ]);
        let err = check_schema_compatibility(&existing, &new_schema).unwrap_err();
        assert!(err.contains("type mismatch"));
    }

    // =====================================================
    // DerivedTable struct
    // =====================================================

    #[test]
    fn test_create_derived_table_request_minimal() {
        let json = serde_json::json!({
            "name": "minimal",
            "sql": "SELECT 1"
        });

        let req: CreateDerivedTableRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.name, "minimal");
        assert_eq!(req.sql, "SELECT 1");
        assert!(req.description.is_none());
        assert!(req.refresh_mode.is_none());
        assert!(req.schedule.is_none());
    }

    // =====================================================
    // DerivedError downcast roundtrip through anyhow::Error
    // =====================================================

    #[test]
    fn test_derived_error_downcast_not_found() {
        let err: anyhow::Error = DerivedError::NotFound("gone".into()).into();
        let de = err.downcast_ref::<DerivedError>().expect("should downcast");
        match de {
            DerivedError::NotFound(msg) => assert_eq!(msg, "gone"),
            other => panic!("Expected NotFound, got {:?}", other),
        }
    }

    #[test]
    fn test_derived_error_downcast_validation() {
        let err: anyhow::Error = DerivedError::Validation("bad input".into()).into();
        let de = err.downcast_ref::<DerivedError>().expect("should downcast");
        match de {
            DerivedError::Validation(msg) => assert_eq!(msg, "bad input"),
            other => panic!("Expected Validation, got {:?}", other),
        }
    }

    #[test]
    fn test_derived_error_downcast_conflict() {
        let err: anyhow::Error = DerivedError::Conflict("already running".into()).into();
        let de = err.downcast_ref::<DerivedError>().expect("should downcast");
        match de {
            DerivedError::Conflict(msg) => assert_eq!(msg, "already running"),
            other => panic!("Expected Conflict, got {:?}", other),
        }
    }

    #[test]
    fn test_derived_error_not_downcastable_from_other_anyhow() {
        let err = anyhow::anyhow!("some other error");
        assert!(err.downcast_ref::<DerivedError>().is_none());
    }

    // =====================================================
    // derive_lock_key
    // =====================================================

    #[test]
    fn test_derive_lock_key_deterministic() {
        let id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let key1 = derive_lock_key(id);
        let key2 = derive_lock_key(id);
        assert_eq!(key1, key2, "Same UUID should produce the same lock key");
    }

    #[test]
    fn test_derive_lock_key_different_for_different_uuids() {
        let id_a = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let id_b = Uuid::parse_str("660e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_ne!(
            derive_lock_key(id_a),
            derive_lock_key(id_b),
            "Different UUIDs should (almost certainly) produce different lock keys"
        );
    }

    #[test]
    fn test_derive_lock_key_nonzero_for_random_uuid() {
        let id = Uuid::new_v4();
        let key = derive_lock_key(id);
        // UUIDv4 has 122 random bits — the first 8 bytes being all-zero is
        // vanishingly unlikely.  This just guards against an obviously broken
        // implementation that always returns 0.
        assert_ne!(key, 0, "Random UUIDv4 lock key should not be 0");
    }

    // =====================================================
    // Nullable schema compatibility (relaxed check)
    // =====================================================

    #[test]
    fn test_schema_compatibility_nullable_to_nonnullable_ok() {
        // non-nullable existing, nullable new  → allowed (ClickHouse often does this)
        let existing = serde_json::json!([
            {"name": "id", "dataType": {"type": "int64"}, "nullable": false},
        ]);
        let new_schema = serde_json::json!([
            {"name": "id", "dataType": {"type": "int64"}, "nullable": true},
        ]);
        assert!(
            check_schema_compatibility(&existing, &new_schema).is_ok(),
            "Should allow nullable mismatch (ClickHouse ArrowStream compatibility)"
        );
    }

    #[test]
    fn test_schema_compatibility_nonnullable_to_nonnullable_ok() {
        let existing = serde_json::json!([
            {"name": "id", "dataType": {"type": "int64"}, "nullable": false},
        ]);
        let new_schema = serde_json::json!([
            {"name": "id", "dataType": {"type": "int64"}, "nullable": false},
        ]);
        assert!(check_schema_compatibility(&existing, &new_schema).is_ok());
    }

    #[test]
    fn test_schema_compatibility_nullable_both_true() {
        let existing = serde_json::json!([
            {"name": "val", "dataType": {"type": "utf8"}, "nullable": true},
        ]);
        let new_schema = serde_json::json!([
            {"name": "val", "dataType": {"type": "utf8"}, "nullable": true},
        ]);
        assert!(check_schema_compatibility(&existing, &new_schema).is_ok());
    }
}
