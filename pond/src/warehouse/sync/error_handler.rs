//! Sync Error Handler
//!
//! Handles sync errors with schema drift detection and retry logic.
//! Schema cache is persisted to the database for reliability across restarts.

use chrono::Duration;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::warehouse::connectors::{Connector, ConnectorError};
use crate::warehouse::types::TableSchema;

/// Types of sync errors.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SyncErrorType {
    AuthenticationFailed,
    RateLimited { retry_after_secs: u64 },
    SchemaChanged { changes: Vec<SchemaChange> },
    Timeout,
    NetworkError,
    DataValidation { invalid_rows: Vec<InvalidRow> },
    Unknown { message: String },
}

/// A detected schema change.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaChange {
    pub change_type: SchemaChangeType,
    pub column_name: String,
    pub old_type: Option<String>,
    pub new_type: Option<String>,
}

/// Types of schema changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaChangeType {
    ColumnAdded,
    ColumnRemoved,
    TypeChanged,
    NullabilityChanged,
}

/// An invalid row detected during validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvalidRow {
    pub row_index: usize,
    pub column: String,
    pub error: String,
}

/// A sync error with context.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncError {
    pub error_type: SyncErrorType,
    pub message: String,
    pub details: Option<serde_json::Value>,
    pub suggested_action: String,
    pub retryable: bool,
}

impl SyncError {
    /// Create a new sync error.
    pub fn new(error_type: SyncErrorType, message: impl Into<String>) -> Self {
        let (suggested_action, retryable) = match &error_type {
            SyncErrorType::AuthenticationFailed => (
                "Check credentials and reconnect the source".to_string(),
                false,
            ),
            SyncErrorType::RateLimited { retry_after_secs } => {
                (format!("Wait {} seconds and retry", retry_after_secs), true)
            }
            SyncErrorType::SchemaChanged { .. } => (
                "Review schema changes and update table configuration".to_string(),
                false,
            ),
            SyncErrorType::Timeout => ("Retry with a smaller batch size".to_string(), true),
            SyncErrorType::NetworkError => {
                ("Check network connectivity and retry".to_string(), true)
            }
            SyncErrorType::DataValidation { .. } => {
                ("Review invalid data and fix at source".to_string(), false)
            }
            SyncErrorType::Unknown { .. } => {
                ("Contact support if issue persists".to_string(), true)
            }
        };

        Self {
            error_type,
            message: message.into(),
            details: None,
            suggested_action,
            retryable,
        }
    }

    /// Add additional details to the error.
    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }
}

/// Action to take after a sync error.
#[derive(Debug, Clone)]
pub enum SyncAction {
    /// Retry after specified duration
    RetryAfter(Duration),
    /// Retry with exponential backoff
    RetryWithBackoff,
    /// Requires user intervention
    RequiresUserAction,
    /// Failed permanently
    Failed,
}

/// Sync error handler with persistent schema cache.
///
/// Schema cache is stored in the database (`warehouse_schema_snapshots` table)
/// for reliability across service restarts.
pub struct SyncErrorHandler {
    /// Database connection pool
    db: PgPool,
    /// In-memory cache for fast lookups (populated from DB on startup)
    schema_cache: Arc<RwLock<HashMap<String, TableSchema>>>,
}

impl SyncErrorHandler {
    /// Create a new error handler with database persistence.
    #[tracing::instrument(name = "warehouse.error.new", skip_all, err(Display))]
    pub async fn new(db: PgPool) -> Result<Self, sqlx::Error> {
        let handler = Self {
            db,
            schema_cache: Arc::new(RwLock::new(HashMap::new())),
        };

        // Load existing schemas from database
        handler.load_schemas_from_db().await?;

        Ok(handler)
    }

    /// Create a new error handler without database (for testing).
    pub fn new_without_db() -> Self {
        // Create a dummy pool that will fail on use
        // This is only for tests that don't need persistence
        Self {
            db: PgPool::connect_lazy("postgres://localhost/test").unwrap(),
            schema_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Load schemas from the database into the in-memory cache.
    #[tracing::instrument(name = "warehouse.error.load_schemas_from_db", skip_all, err(Display))]
    async fn load_schemas_from_db(&self) -> Result<(), sqlx::Error> {
        let rows = sqlx::query(
            "SELECT DISTINCT ON (table_id) table_id, schema_json 
             FROM warehouse_schema_snapshots 
             ORDER BY table_id, captured_at DESC",
        )
        .fetch_all(&self.db)
        .await?;

        let mut cache = self.schema_cache.write().await;
        for row in rows {
            let table_id: String = sqlx::Row::get(&row, "table_id");
            let schema_json: serde_json::Value = sqlx::Row::get(&row, "schema_json");

            if let Ok(schema) = serde_json::from_value::<TableSchema>(schema_json) {
                cache.insert(table_id, schema);
            }
        }

        tracing::info!(count = cache.len(), "Loaded schema cache from database");
        Ok(())
    }

    /// Detect schema drift before sync.
    pub async fn detect_schema_drift(
        &self,
        connector: &dyn Connector,
        table: &str,
    ) -> Result<Vec<SchemaChange>, ConnectorError> {
        let current_schema = connector.get_schema(table).await?;

        let cache = self.schema_cache.read().await;
        let stored_schema = match cache.get(table) {
            Some(schema) => schema.clone(),
            None => return Ok(Vec::new()), // No stored schema, no drift
        };
        drop(cache);

        Ok(compare_schemas(&stored_schema, &current_schema))
    }

    /// Store schema for future drift detection.
    ///
    /// This persists the schema to both the in-memory cache and the database.
    pub async fn store_schema(
        &self,
        source_id: Uuid,
        table: &str,
        schema: TableSchema,
    ) -> Result<(), sqlx::Error> {
        // Update in-memory cache
        {
            let mut cache = self.schema_cache.write().await;
            cache.insert(table.to_string(), schema.clone());
        }

        // Persist to database
        let schema_json = serde_json::to_value(&schema).unwrap_or_else(|_| serde_json::json!({}));

        sqlx::query(
            "INSERT INTO warehouse_schema_snapshots (id, source_id, table_id, schema_json, captured_at)
             VALUES ($1, $2, $3, $4, NOW())"
        )
        .bind(Uuid::new_v4())
        .bind(source_id)
        .bind(table)
        .bind(&schema_json)
        .execute(&self.db)
        .await?;

        tracing::debug!(table = table, "Stored schema snapshot");
        Ok(())
    }

    /// Get the cached schema for a table.
    #[tracing::instrument(name = "warehouse.error.get_cached_schema", skip_all)]
    pub async fn get_cached_schema(&self, table: &str) -> Option<TableSchema> {
        let cache = self.schema_cache.read().await;
        cache.get(table).cloned()
    }

    /// Clear the schema cache (for testing or manual reset).
    #[tracing::instrument(name = "warehouse.error.clear_cache", skip_all)]
    pub async fn clear_cache(&self) {
        let mut cache = self.schema_cache.write().await;
        cache.clear();
    }

    /// Handle a sync error with retry logic.
    pub fn handle_error(&self, error: SyncError) -> SyncAction {
        match &error.error_type {
            SyncErrorType::RateLimited { retry_after_secs } => {
                SyncAction::RetryAfter(Duration::seconds(*retry_after_secs as i64))
            }
            SyncErrorType::SchemaChanged { .. } => SyncAction::RequiresUserAction,
            SyncErrorType::Timeout if error.retryable => SyncAction::RetryWithBackoff,
            SyncErrorType::NetworkError if error.retryable => SyncAction::RetryWithBackoff,
            _ if error.retryable => SyncAction::RetryWithBackoff,
            _ => SyncAction::Failed,
        }
    }

    /// Convert a connector error to a sync error.
    pub fn from_connector_error(error: ConnectorError) -> SyncError {
        match error {
            ConnectorError::Authentication(msg) => {
                SyncError::new(SyncErrorType::AuthenticationFailed, msg)
            }
            ConnectorError::RateLimited { retry_after_secs } => SyncError::new(
                SyncErrorType::RateLimited { retry_after_secs },
                format!("Rate limited, retry after {} seconds", retry_after_secs),
            ),
            ConnectorError::Network(msg) => SyncError::new(SyncErrorType::NetworkError, msg),
            ConnectorError::Validation(msg) => SyncError::new(
                SyncErrorType::DataValidation {
                    invalid_rows: vec![],
                },
                msg,
            ),
            _ => SyncError::new(
                SyncErrorType::Unknown {
                    message: error.to_string(),
                },
                error.to_string(),
            ),
        }
    }
}

/// Compare two schemas and return the differences.
pub fn compare_schemas(old: &TableSchema, new: &TableSchema) -> Vec<SchemaChange> {
    let mut changes = Vec::new();

    let old_columns: HashMap<&str, _> = old.columns.iter().map(|c| (c.name.as_str(), c)).collect();

    let new_columns: HashMap<&str, _> = new.columns.iter().map(|c| (c.name.as_str(), c)).collect();

    // Check for removed columns
    for (name, _col) in &old_columns {
        if !new_columns.contains_key(name) {
            changes.push(SchemaChange {
                change_type: SchemaChangeType::ColumnRemoved,
                column_name: name.to_string(),
                old_type: None,
                new_type: None,
            });
        }
    }

    // Check for added columns and type changes
    for (name, new_col) in &new_columns {
        match old_columns.get(name) {
            None => {
                changes.push(SchemaChange {
                    change_type: SchemaChangeType::ColumnAdded,
                    column_name: name.to_string(),
                    old_type: None,
                    new_type: Some(format!("{:?}", new_col.data_type)),
                });
            }
            Some(old_col) => {
                if old_col.data_type != new_col.data_type {
                    changes.push(SchemaChange {
                        change_type: SchemaChangeType::TypeChanged,
                        column_name: name.to_string(),
                        old_type: Some(format!("{:?}", old_col.data_type)),
                        new_type: Some(format!("{:?}", new_col.data_type)),
                    });
                }
                if old_col.nullable != new_col.nullable {
                    changes.push(SchemaChange {
                        change_type: SchemaChangeType::NullabilityChanged,
                        column_name: name.to_string(),
                        old_type: Some(format!("nullable={}", old_col.nullable)),
                        new_type: Some(format!("nullable={}", new_col.nullable)),
                    });
                }
            }
        }
    }

    changes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::warehouse::types::{ColumnSchema, ColumnType};

    #[test]
    fn test_schema_comparison_no_changes() {
        let schema = TableSchema {
            columns: vec![ColumnSchema::new("id", ColumnType::Int64, false)],
        };

        let changes = compare_schemas(&schema, &schema);
        assert!(changes.is_empty());
    }

    #[test]
    fn test_schema_comparison_column_added() {
        let old = TableSchema {
            columns: vec![ColumnSchema::new("id", ColumnType::Int64, false)],
        };

        let new = TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::Int64, false),
                ColumnSchema::new("name", ColumnType::String, true),
            ],
        };

        let changes = compare_schemas(&old, &new);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].change_type, SchemaChangeType::ColumnAdded);
        assert_eq!(changes[0].column_name, "name");
    }

    #[test]
    fn test_schema_comparison_column_removed() {
        let old = TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::Int64, false),
                ColumnSchema::new("name", ColumnType::String, true),
            ],
        };

        let new = TableSchema {
            columns: vec![ColumnSchema::new("id", ColumnType::Int64, false)],
        };

        let changes = compare_schemas(&old, &new);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].change_type, SchemaChangeType::ColumnRemoved);
        assert_eq!(changes[0].column_name, "name");
    }

    // ==================== handle_error Tests ====================

    #[tokio::test]
    async fn test_handle_error_rate_limited() {
        let handler = SyncErrorHandler::new_without_db();
        let error = SyncError::new(
            SyncErrorType::RateLimited {
                retry_after_secs: 30,
            },
            "Rate limited",
        );
        let action = handler.handle_error(error);
        match action {
            SyncAction::RetryAfter(d) => assert_eq!(d.num_seconds(), 30),
            other => panic!("Expected RetryAfter, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_handle_error_timeout() {
        let handler = SyncErrorHandler::new_without_db();
        let error = SyncError::new(SyncErrorType::Timeout, "Connection timed out");
        assert!(error.retryable);
        let action = handler.handle_error(error);
        assert!(matches!(action, SyncAction::RetryWithBackoff));
    }

    #[tokio::test]
    async fn test_handle_error_network() {
        let handler = SyncErrorHandler::new_without_db();
        let error = SyncError::new(SyncErrorType::NetworkError, "DNS failure");
        assert!(error.retryable);
        let action = handler.handle_error(error);
        assert!(matches!(action, SyncAction::RetryWithBackoff));
    }

    #[tokio::test]
    async fn test_handle_error_auth_failed() {
        let handler = SyncErrorHandler::new_without_db();
        let error = SyncError::new(SyncErrorType::AuthenticationFailed, "Invalid token");
        assert!(!error.retryable);
        let action = handler.handle_error(error);
        assert!(matches!(action, SyncAction::Failed));
    }

    #[tokio::test]
    async fn test_handle_error_schema_changed() {
        let handler = SyncErrorHandler::new_without_db();
        let error = SyncError::new(
            SyncErrorType::SchemaChanged { changes: vec![] },
            "Schema drift detected",
        );
        let action = handler.handle_error(error);
        assert!(matches!(action, SyncAction::RequiresUserAction));
    }

    #[tokio::test]
    async fn test_handle_error_data_validation() {
        let handler = SyncErrorHandler::new_without_db();
        let error = SyncError::new(
            SyncErrorType::DataValidation {
                invalid_rows: vec![],
            },
            "Invalid data",
        );
        assert!(!error.retryable);
        let action = handler.handle_error(error);
        assert!(matches!(action, SyncAction::Failed));
    }

    #[tokio::test]
    async fn test_handle_error_unknown_retryable() {
        let handler = SyncErrorHandler::new_without_db();
        let error = SyncError::new(
            SyncErrorType::Unknown {
                message: "Transient failure".into(),
            },
            "Something went wrong",
        );
        assert!(error.retryable);
        let action = handler.handle_error(error);
        assert!(matches!(action, SyncAction::RetryWithBackoff));
    }

    // ==================== from_connector_error Tests ====================

    #[test]
    fn test_from_connector_error_authentication() {
        let error = ConnectorError::Authentication("bad creds".into());
        let sync_err = SyncErrorHandler::from_connector_error(error);
        assert!(matches!(
            sync_err.error_type,
            SyncErrorType::AuthenticationFailed
        ));
        assert!(!sync_err.retryable);
    }

    #[test]
    fn test_from_connector_error_rate_limited() {
        let error = ConnectorError::RateLimited {
            retry_after_secs: 60,
        };
        let sync_err = SyncErrorHandler::from_connector_error(error);
        match &sync_err.error_type {
            SyncErrorType::RateLimited { retry_after_secs } => assert_eq!(*retry_after_secs, 60),
            other => panic!("Expected RateLimited, got {:?}", other),
        }
        assert!(sync_err.retryable);
    }

    #[test]
    fn test_from_connector_error_network() {
        let error = ConnectorError::Network("connection refused".into());
        let sync_err = SyncErrorHandler::from_connector_error(error);
        assert!(matches!(sync_err.error_type, SyncErrorType::NetworkError));
        assert!(sync_err.retryable);
    }

    #[test]
    fn test_from_connector_error_validation() {
        let error = ConnectorError::Validation("invalid value in column X".into());
        let sync_err = SyncErrorHandler::from_connector_error(error);
        assert!(matches!(
            sync_err.error_type,
            SyncErrorType::DataValidation { .. }
        ));
        assert!(!sync_err.retryable);
    }

    #[test]
    fn test_from_connector_error_generic() {
        let error = ConnectorError::Internal("unexpected EOF".into());
        let sync_err = SyncErrorHandler::from_connector_error(error);
        assert!(matches!(sync_err.error_type, SyncErrorType::Unknown { .. }));
        assert!(sync_err.retryable);
    }

    // ==================== compare_schemas Edge Cases ====================

    #[test]
    fn test_schema_comparison_type_changed() {
        let old = TableSchema {
            columns: vec![ColumnSchema::new("amount", ColumnType::Int64, false)],
        };
        let new = TableSchema {
            columns: vec![ColumnSchema::new("amount", ColumnType::Float64, false)],
        };
        let changes = compare_schemas(&old, &new);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].change_type, SchemaChangeType::TypeChanged);
        assert_eq!(changes[0].column_name, "amount");
    }

    #[test]
    fn test_schema_comparison_nullability_changed() {
        let old = TableSchema {
            columns: vec![ColumnSchema::new("name", ColumnType::String, false)],
        };
        let new = TableSchema {
            columns: vec![ColumnSchema::new("name", ColumnType::String, true)],
        };
        let changes = compare_schemas(&old, &new);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].change_type, SchemaChangeType::NullabilityChanged);
    }

    #[test]
    fn test_schema_comparison_both_empty() {
        let empty = TableSchema { columns: vec![] };
        let changes = compare_schemas(&empty, &empty);
        assert!(changes.is_empty());
    }

    #[test]
    fn test_schema_comparison_different_column_order() {
        let old = TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::Int64, false),
                ColumnSchema::new("name", ColumnType::String, false),
            ],
        };
        let new = TableSchema {
            columns: vec![
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("id", ColumnType::Int64, false),
            ],
        };
        let changes = compare_schemas(&old, &new);
        assert!(
            changes.is_empty(),
            "Column reordering should not count as a change"
        );
    }
}
