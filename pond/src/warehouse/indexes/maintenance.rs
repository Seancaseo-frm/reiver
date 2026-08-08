//! FST Maintenance & Rebuild
//!
//! Scheduled rebuilds and incremental merge strategy for FST indexes.
//!
//! PERFORMANCE: Uses double-buffering for index rebuilds to avoid blocking
//! reads during rebuild. The new index is built entirely outside of any lock,
//! then atomically swapped with the old index.

use arc_swap::ArcSwap;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio_cron_scheduler::{Job, JobScheduler};

use super::schema_index::SchemaIndex;
use crate::warehouse::types::{ColumnSchema, ColumnType, StorageType, TableSchema, WarehouseTable};
use chrono::Utc;

/// Errors that can occur during FST maintenance.
#[derive(Debug, Error)]
pub enum MaintenanceError {
    #[error("Scheduler error: {0}")]
    Scheduler(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("FST error: {0}")]
    Fst(String),
    
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}

/// Result type for maintenance operations.
pub type MaintenanceResult<T> = Result<T, MaintenanceError>;

/// Double-buffered schema index for lock-free reads during rebuilds.
/// 
/// PERFORMANCE: Uses ArcSwap for O(1) atomic swaps. Readers never block,
/// even during index rebuilds. The old index remains valid until all
/// readers are done with it (via Arc reference counting).
static SCHEMA_INDEX: once_cell::sync::OnceCell<ArcSwap<Option<SchemaIndex>>> = 
    once_cell::sync::OnceCell::new();

/// Get the schema index (lock-free).
/// 
/// PERFORMANCE: This function never blocks. It returns a reference to the
/// current index which remains valid even if a rebuild starts.
pub fn get_schema_index() -> Arc<Option<SchemaIndex>> {
    SCHEMA_INDEX.get_or_init(|| ArcSwap::from_pointee(None)).load_full()
}

/// Swap in a new schema index atomically.
/// 
/// PERFORMANCE: O(1) swap. Old readers continue using the old index until done.
fn swap_schema_index(new_index: SchemaIndex) {
    let holder = SCHEMA_INDEX.get_or_init(|| ArcSwap::from_pointee(None));
    holder.store(Arc::new(Some(new_index)));
}

/// FST maintenance scheduler.
pub struct FstMaintenanceScheduler {
    scheduler: std::sync::Arc<tokio::sync::RwLock<JobScheduler>>,
    db: PgPool,
}

impl FstMaintenanceScheduler {
    /// Create a new maintenance scheduler.
    #[tracing::instrument(
        name = "warehouse.index_maintenance.new",
        skip_all,
        err(Display)
    )]
    pub async fn new(db: PgPool) -> MaintenanceResult<Self> {
        let scheduler = JobScheduler::new()
            .await
            .map_err(|e| MaintenanceError::Scheduler(e.to_string()))?;

        Ok(Self { 
            scheduler: std::sync::Arc::new(tokio::sync::RwLock::new(scheduler)),
            db,
        })
    }

    /// Start the scheduler with default jobs.
    #[tracing::instrument(
        name = "warehouse.index_maintenance.start",
        skip_all,
        err(Display)
    )]
    pub async fn start(&self) -> MaintenanceResult<()> {
        // Perform initial index build
        rebuild_all_indexes(&self.db).await?;
        
        // Rebuild all FST indexes daily at 3 AM
        let db = self.db.clone();
        let job = Job::new_async("0 0 3 * * *", move |_uuid, _lock| {
            let db = db.clone();
            Box::pin(async move {
                if let Err(e) = rebuild_all_indexes(&db).await {
                    tracing::error!(error = %e, "Failed to rebuild FST indexes");
                }
            })
        })
        .map_err(|e| MaintenanceError::Scheduler(e.to_string()))?;

        let mut scheduler = self.scheduler.write().await;
        scheduler
            .add(job)
            .await
            .map_err(|e| MaintenanceError::Scheduler(e.to_string()))?;

        scheduler
            .start()
            .await
            .map_err(|e| MaintenanceError::Scheduler(e.to_string()))?;

        Ok(())
    }

    /// Stop the scheduler.
    #[tracing::instrument(
        name = "warehouse.index_maintenance.shutdown",
        skip_all,
        err(Display)
    )]
    pub async fn shutdown(&self) -> MaintenanceResult<()> {
        let mut scheduler = self.scheduler.write().await;
        scheduler
            .shutdown()
            .await
            .map_err(|e| MaintenanceError::Scheduler(e.to_string()))?;
        Ok(())
    }

    /// Add a custom maintenance job.
    #[tracing::instrument(
        name = "warehouse.index_maintenance.add_job",
        skip_all,
        err(Display)
    )]
    pub async fn add_job(&self, cron: &str, job_fn: impl Fn() + Send + Sync + 'static) -> MaintenanceResult<()> {
        let job = Job::new_async(cron, move |_uuid, _lock| {
            job_fn();
            Box::pin(async {})
        })
        .map_err(|e| MaintenanceError::Scheduler(e.to_string()))?;

        let mut scheduler = self.scheduler.write().await;
        scheduler
            .add(job)
            .await
            .map_err(|e| MaintenanceError::Scheduler(e.to_string()))?;

        Ok(())
    }
    
    /// Trigger an immediate index rebuild.
    #[tracing::instrument(
        name = "warehouse.index_maintenance.rebuild_now",
        skip_all,
        err(Display)
    )]
    pub async fn rebuild_now(&self) -> MaintenanceResult<()> {
        rebuild_all_indexes(&self.db).await
    }
}

/// Rebuild all FST indexes.
#[tracing::instrument(name = "pond.maintenance.rebuild_all_indexes", skip(db))]
async fn rebuild_all_indexes(db: &PgPool) -> MaintenanceResult<()> {
    let start = std::time::Instant::now();
    tracing::info!("Starting FST index rebuild");

    // 1. Rebuild schema autocomplete index
    rebuild_schema_index(db).await?;

    // Note: Other index types (string column indexes, query history index, skip indexes)
    // are typically ClickHouse-side MergeTree features and don't need FST rebuilding here.

    let duration = start.elapsed();
    tracing::info!(
        duration_ms = duration.as_millis(),
        "FST index rebuild complete"
    );
    Ok(())
}

/// Rebuild the schema autocomplete index from database tables.
#[tracing::instrument(name = "pond.maintenance.rebuild_schema_index", skip(db))]
async fn rebuild_schema_index(db: &PgPool) -> MaintenanceResult<()> {
    use sqlx::Row;
    
    // Fetch all tables and their schemas from the database
    let rows = sqlx::query(
        r#"
        SELECT t.id, t.source_id, t.name, t.schema, t.r2_prefix, s.source_type
        FROM warehouse_tables t
        JOIN warehouse_sources s ON t.source_id = s.id
        WHERE t.sync_enabled = TRUE AND s.enabled = TRUE
        "#
    )
    .fetch_all(db)
    .await?;
    
    let mut tables = Vec::new();
    
    for row in rows {
        let table_id: uuid::Uuid = row.get("id");
        let table_name: String = row.get("name");
        let source_id: uuid::Uuid = row.get("source_id");
        let schema_json: serde_json::Value = row.get("schema");
        let source_type: String = row.get("source_type");
        let r2_prefix: String = row.try_get("r2_prefix").unwrap_or_else(|_| format!("{}/{}", source_type, table_name));
        
        // Parse schema from JSON
        let table_schema = parse_table_schema(&schema_json);
        let now = Utc::now();
        
        tables.push(WarehouseTable {
            id: table_id,
            source_id,
            name: table_name,
            schema: table_schema,
            storage_type: StorageType::default(),
            r2_prefix,
            clickhouse_table: None,
            sync_enabled: true,
            incremental_key: None,
            created_at: now,
            updated_at: now,
        });
    }
    
    // Build the FST index entirely outside of any lock
    // PERFORMANCE: This can take significant time for large schemas,
    // but readers are not blocked during this process.
    let new_index = SchemaIndex::build(&tables)
        .map_err(|e| MaintenanceError::Fst(e.to_string()))?;
    
    // Atomically swap the new index in (O(1), lock-free)
    // PERFORMANCE: Old readers continue using the old index via Arc refcount.
    // The old index is deallocated when the last reader is done.
    swap_schema_index(new_index);
    
    tracing::info!(
        table_count = tables.len(),
        "Schema autocomplete index rebuilt (lock-free swap)"
    );
    
    Ok(())
}

/// Parse a TableSchema from JSON.
fn parse_table_schema(json: &serde_json::Value) -> TableSchema {
    let columns = if let Some(cols) = json.get("columns").and_then(|v| v.as_array()) {
        cols.iter()
            .filter_map(|col| {
                let name = col.get("name")?.as_str()?.to_string();
                let data_type_str = col.get("data_type").and_then(|v| v.as_str()).unwrap_or("string");
                let nullable = col.get("nullable").and_then(|v| v.as_bool()).unwrap_or(true);
                let description = col.get("description").and_then(|v| v.as_str()).map(|s| s.to_string());
                let data_type = parse_column_type(data_type_str);
                let timezone = if matches!(data_type, ColumnType::Timestamp) {
                    Some("UTC".to_string())
                } else {
                    None
                };
                
                Some(ColumnSchema {
                    name,
                    data_type,
                    nullable,
                    description,
                    timezone,
                })
            })
            .collect()
    } else {
        Vec::new()
    };
    
    TableSchema { columns }
}

/// Parse a ColumnType from a string.
fn parse_column_type(s: &str) -> ColumnType {
    match s.to_lowercase().as_str() {
        "int32" | "int" | "integer" => ColumnType::Int32,
        "int64" | "bigint" | "long" => ColumnType::Int64,
        "float64" | "double" | "float" => ColumnType::Float64,
        "bool" | "boolean" => ColumnType::Boolean,
        "timestamp" | "datetime" => ColumnType::Timestamp,
        "date" => ColumnType::Date,
        "json" | "jsonb" => ColumnType::Json,
        "uuid" => ColumnType::Uuid,
        "decimal" | "numeric" => ColumnType::Decimal,
        _ => ColumnType::String,
    }
}

/// Incremental index that supports adding new keys without full rebuild.
pub struct IncrementalIndex {
    /// Main FST (rebuilt periodically)
    main_keys: Vec<String>,
    /// Incremental keys (added since last rebuild)
    incremental_keys: Vec<String>,
    /// Last rebuild timestamp
    last_rebuild: std::time::Instant,
}

impl IncrementalIndex {
    /// Create a new incremental index.
    pub fn new(initial_keys: Vec<String>) -> Self {
        let mut sorted_keys = initial_keys;
        sorted_keys.sort();
        sorted_keys.dedup();

        Self {
            main_keys: sorted_keys,
            incremental_keys: Vec::new(),
            last_rebuild: std::time::Instant::now(),
        }
    }

    /// Add new keys (will be included in next search).
    pub fn add_keys(&mut self, keys: Vec<String>) {
        self.incremental_keys.extend(keys);
    }

    /// Search across main + incremental keys.
    pub fn search_prefix(&self, prefix: &str) -> Vec<&str> {
        let mut results: Vec<&str> = self
            .main_keys
            .iter()
            .filter(|k| k.starts_with(prefix))
            .map(|s| s.as_str())
            .collect();

        results.extend(
            self.incremental_keys
                .iter()
                .filter(|k| k.starts_with(prefix))
                .map(|s| s.as_str()),
        );

        results.sort();
        results.dedup();
        results
    }

    /// Merge incremental keys into main (call periodically).
    pub fn compact(&mut self) {
        self.main_keys.append(&mut self.incremental_keys);
        self.main_keys.sort();
        self.main_keys.dedup();
        self.last_rebuild = std::time::Instant::now();
    }

    /// Check if compaction is needed.
    pub fn needs_compaction(&self, threshold: usize, max_age: Duration) -> bool {
        self.incremental_keys.len() > threshold
            || self.last_rebuild.elapsed() > max_age
    }

    /// Get the number of keys in main index.
    pub fn main_count(&self) -> usize {
        self.main_keys.len()
    }

    /// Get the number of incremental keys.
    pub fn incremental_count(&self) -> usize {
        self.incremental_keys.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_incremental_index() {
        let mut index = IncrementalIndex::new(vec![
            "alice".to_string(),
            "bob".to_string(),
        ]);

        // Search main keys
        let results = index.search_prefix("a");
        assert!(results.contains(&"alice"));

        // Add incremental key
        index.add_keys(vec!["andy".to_string()]);

        // Search should include incremental
        let results = index.search_prefix("a");
        assert!(results.contains(&"alice"));
        assert!(results.contains(&"andy"));

        // Compact
        index.compact();
        assert_eq!(index.main_count(), 3);
        assert_eq!(index.incremental_count(), 0);
    }

    #[test]
    fn test_needs_compaction() {
        let index = IncrementalIndex::new(vec!["a".to_string()]);

        // Should not need compaction initially
        assert!(!index.needs_compaction(100, Duration::from_secs(3600)));
    }
}
