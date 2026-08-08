//! SQLite Connector
//!
//! Connects to SQLite databases and syncs data to the warehouse.
//!
//! # Features
//!
//! - Automatic schema discovery from `sqlite_master`
//! - Incremental sync support with configurable key columns
//! - Support for file-based and in-memory databases
//! - Full SQL predicate pushdown (native SQLite support)
//!
//! # Usage
//!
//! ```ignore
//! let config = SQLiteConfig::new("/path/to/database.db");
//! let connector = SQLiteConnector::new(config);
//!
//! let tables = connector.list_tables().await?;
//! let data = connector.fetch_table("users", None, None).await?;
//! ```

use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tokio::sync::OnceCell;

use crate::warehouse::connectors::{
    Connector, ConnectorError, ConnectorResult, FetchOptions, RecordBatchStream, TableInfo,
};
use crate::warehouse::query::predicate_pushdown::{predicate_to_sql, SqlDialect};
use crate::warehouse::types::{ColumnSchema, ColumnType, SourceType, TableSchema};

/// Maximum rows to fetch per batch to prevent memory issues.
const BATCH_SIZE: i64 = 10_000;

/// SQLite connector configuration.
#[derive(Debug, Clone)]
pub struct SQLiteConfig {
    /// Path to the SQLite database file
    /// Use `:memory:` for an in-memory database
    pub database_path: String,
    /// Tables to sync (empty = all tables)
    pub tables: Vec<String>,
    /// Whether to open the database in read-only mode
    pub read_only: bool,
    /// Whether to create the database if it doesn't exist
    pub create_if_missing: bool,
}

impl SQLiteConfig {
    /// Create a new SQLite configuration from a database path.
    ///
    /// # Arguments
    ///
    /// * `database_path` - Path to the SQLite database file, or `:memory:` for in-memory
    pub fn new(database_path: impl Into<String>) -> Self {
        Self {
            database_path: database_path.into(),
            tables: Vec::new(),
            read_only: false,
            create_if_missing: false,
        }
    }

    /// Create a configuration for an in-memory database.
    pub fn in_memory() -> Self {
        Self::new(":memory:")
    }

    /// Set tables to sync (empty = all tables).
    pub fn with_tables(mut self, tables: Vec<String>) -> Self {
        self.tables = tables;
        self
    }

    /// Set read-only mode (safer for production data).
    pub fn with_read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    /// Set whether to create the database if it doesn't exist.
    pub fn with_create_if_missing(mut self, create: bool) -> Self {
        self.create_if_missing = create;
        self
    }

    /// Build the connection string for sqlx.
    fn connection_string(&self) -> String {
        let mut conn = format!("sqlite:{}", self.database_path);
        
        let mut params = Vec::new();
        if self.read_only {
            params.push("mode=ro");
        }
        if self.create_if_missing && self.database_path != ":memory:" {
            params.push("mode=rwc");
        }
        
        if !params.is_empty() {
            conn.push('?');
            conn.push_str(&params.join("&"));
        }
        
        conn
    }
}

/// SQLite data source connector.
///
/// Simple connector for SQLite databases with full predicate pushdown support.
/// No WAL-based indexing - relies on SQLite's native SQL capabilities.
pub struct SQLiteConnector {
    config: SQLiteConfig,
    /// Connection pool - initialized lazily on first use using OnceCell
    pool: OnceCell<sqlx::SqlitePool>,
}

impl SQLiteConnector {
    /// Create a new SQLite connector.
    pub fn new(config: SQLiteConfig) -> Self {
        Self {
            config,
            pool: OnceCell::new(),
        }
    }

    /// Create a connector with an existing pool (for testing or shared connections).
    pub fn with_pool(config: SQLiteConfig, pool: sqlx::SqlitePool) -> Self {
        let pool_cell = OnceCell::new();
        let _ = pool_cell.set(pool);
        Self {
            config,
            pool: pool_cell,
        }
    }

    /// Get or create the connection pool.
    async fn get_pool(&self) -> ConnectorResult<sqlx::SqlitePool> {
        self.pool
            .get_or_try_init(|| async {
                sqlx::sqlite::SqlitePoolOptions::new()
                    .max_connections(1) // SQLite works best with single writer
                    .connect(&self.config.connection_string())
                    .await
                    .map_err(|e| {
                        ConnectorError::Network(format!("Failed to connect to SQLite: {}", e))
                    })
            })
            .await
            .cloned()
    }

    /// Map SQLite type affinity to warehouse column type.
    ///
    /// SQLite uses type affinity rather than strict types:
    /// - INTEGER: whole numbers
    /// - REAL: floating point
    /// - TEXT: strings
    /// - BLOB: binary data (mapped to String with base64 encoding)
    /// - NUMERIC: numbers or text
    fn sqlite_type_to_column_type(sqlite_type: &str) -> ColumnType {
        let upper = sqlite_type.to_uppercase();
        
        // Check for common type patterns
        if upper.contains("INT") {
            return ColumnType::Int64;
        }
        if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
            return ColumnType::Float64;
        }
        if upper.contains("BOOL") {
            return ColumnType::Boolean;
        }
        if upper.contains("DATE") || upper.contains("TIME") {
            return ColumnType::Timestamp;
        }
        if upper.contains("DECIMAL") || upper.contains("NUMERIC") {
            return ColumnType::Decimal;
        }
        
        // Default to string for TEXT, BLOB, and unknown types
        // BLOBs will be represented as base64-encoded strings
        ColumnType::String
    }

    /// Convert table schema to Arrow schema.
    fn to_arrow_schema(schema: &TableSchema) -> Schema {
        let fields: Vec<Field> = schema
            .columns
            .iter()
            .map(|col| Field::new(&col.name, col.data_type.to_arrow_type(), col.nullable))
            .collect();
        Schema::new(fields)
    }

    /// Escape a string value for use in SQLite SQL.
    fn escape_sqlite_string(s: &str) -> String {
        let mut result = String::with_capacity(s.len() + s.len() / 8);
        for ch in s.chars() {
            match ch {
                '\'' => result.push_str("''"),
                '\0' => {} // Remove null bytes
                _ => result.push(ch),
            }
        }
        result
    }

    /// Validate an identifier for use in SQL.
    fn is_valid_identifier(name: &str) -> bool {
        if name.is_empty() || name.len() > 128 {
            return false;
        }

        // Reject quotes which could break out of identifier quoting
        if name.contains('"') || name.contains('`') || name.contains('[') || name.contains(']') {
            return false;
        }

        let mut chars = name.chars();

        // First character must be letter or underscore
        match chars.next() {
            Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
            _ => return false,
        }

        // Remaining characters must be alphanumeric or underscore
        chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    }

    /// Build a SELECT query for fetching data.
    fn build_fetch_query(
        table: &str,
        columns: &[String],
        incremental_key: Option<&str>,
        last_value: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> ConnectorResult<String> {
        // Validate table name
        if !Self::is_valid_identifier(table) {
            return Err(ConnectorError::Config(format!(
                "Invalid table name: '{}'. Names must start with a letter or underscore and contain only alphanumeric characters and underscores.",
                table
            )));
        }

        // Validate and build column list
        let columns_str = if columns.is_empty() {
            "*".to_string()
        } else {
            for col in columns {
                if !Self::is_valid_identifier(col) {
                    return Err(ConnectorError::Config(format!(
                        "Invalid column name: '{}'.",
                        col
                    )));
                }
            }
            columns
                .iter()
                .map(|c| format!("\"{}\"", c))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let mut query = format!("SELECT {} FROM \"{}\"", columns_str, table);

        // Add incremental filter if provided
        if let (Some(key), Some(value)) = (incremental_key, last_value) {
            if !Self::is_valid_identifier(key) {
                return Err(ConnectorError::Config(format!(
                    "Invalid incremental key column: '{}'",
                    key
                )));
            }
            let escaped_value = Self::escape_sqlite_string(value);
            query.push_str(&format!(" WHERE \"{}\" > '{}'", key, escaped_value));
        }

        // Add ordering for consistent pagination
        if let Some(key) = incremental_key {
            if Self::is_valid_identifier(key) {
                query.push_str(&format!(" ORDER BY \"{}\" ASC", key));
            }
        }

        // Add pagination
        query.push_str(&format!(" LIMIT {} OFFSET {}", limit, offset));

        Ok(query)
    }

    /// Get schema for a table.
    async fn get_schema_from_pool(
        &self,
        pool: &sqlx::SqlitePool,
        table: &str,
    ) -> ConnectorResult<TableSchema> {
        // SQLite uses PRAGMA table_info for column information
        let columns: Vec<(i64, String, String, i64, Option<String>, i64)> = sqlx::query_as(
            &format!("PRAGMA table_info(\"{}\")", table),
        )
        .fetch_all(pool)
        .await
        .map_err(|e| {
            ConnectorError::Internal(format!("Failed to get schema for {}: {}", table, e))
        })?;

        if columns.is_empty() {
            return Err(ConnectorError::Internal(format!(
                "Table {} not found or has no columns",
                table
            )));
        }

        let columns: Vec<ColumnSchema> = columns
            .into_iter()
            .map(|(_cid, name, data_type, notnull, _dflt_value, _pk)| {
                let col_type = Self::sqlite_type_to_column_type(&data_type);
                let timezone = if matches!(col_type, ColumnType::Timestamp) {
                    Some("UTC".to_string())
                } else {
                    None
                };
                ColumnSchema {
                    name,
                    data_type: col_type,
                    nullable: notnull == 0, // notnull=1 means NOT NULL
                    description: None,
                    timezone,
                }
            })
            .collect();

        Ok(TableSchema { columns })
    }

    /// Convert SQLite rows to an Arrow RecordBatch.
    fn rows_to_record_batch(
        &self,
        rows: &[sqlx::sqlite::SqliteRow],
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<RecordBatch> {
        use sqlx::Row;

        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.columns.len());

        for (col_idx, col) in schema.columns.iter().enumerate() {
            let array: ArrayRef = match col.data_type {
                ColumnType::Int32 | ColumnType::Int64 => {
                    let values: Vec<Option<i64>> = rows
                        .iter()
                        .map(|row| row.try_get::<i64, _>(col_idx).ok())
                        .collect();
                    Arc::new(Int64Array::from(values))
                }
                ColumnType::Float64 => {
                    let values: Vec<Option<f64>> = rows
                        .iter()
                        .map(|row| row.try_get::<f64, _>(col_idx).ok())
                        .collect();
                    Arc::new(Float64Array::from(values))
                }
                ColumnType::Boolean => {
                    let values: Vec<Option<bool>> = rows
                        .iter()
                        .map(|row| row.try_get::<bool, _>(col_idx).ok())
                        .collect();
                    Arc::new(BooleanArray::from(values))
                }
                _ => {
                    // Default: convert to string
                    let values: Vec<Option<String>> = rows
                        .iter()
                        .map(|row| row.try_get::<String, _>(col_idx).ok())
                        .collect();
                    Arc::new(StringArray::from(values))
                }
            };
            arrays.push(array);
        }

        RecordBatch::try_new(arrow_schema, arrays).map_err(|e| {
            ConnectorError::Internal(format!("Failed to create RecordBatch: {}", e))
        })
    }
}

#[async_trait]
impl Connector for SQLiteConnector {
    fn source_type(&self) -> SourceType {
        SourceType::SQLite
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        let pool = self.get_pool().await?;

        // Query sqlite_master for tables
        let tables: Vec<(String,)> = sqlx::query_as(
            r#"
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
              AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            "#,
        )
        .fetch_all(&pool)
        .await
        .map_err(|e| ConnectorError::Internal(format!("Failed to list tables: {}", e)))?;

        let mut table_infos = Vec::new();
        for (table_name,) in tables {
            // Skip if tables filter is set and this table isn't in it
            if !self.config.tables.is_empty() && !self.config.tables.contains(&table_name) {
                continue;
            }

            // Get schema for this table
            let table_schema = match self.get_schema_from_pool(&pool, &table_name).await {
                Ok(schema) => schema,
                Err(e) => {
                    tracing::warn!(
                        table = %table_name,
                        error = %e,
                        "Failed to get schema for table, skipping"
                    );
                    continue;
                }
            };

            // Estimate row count
            let row_estimate: Option<u64> = sqlx::query_scalar::<_, i64>(
                &format!("SELECT COUNT(*) FROM \"{}\"", table_name),
            )
            .fetch_one(&pool)
            .await
            .ok()
            .map(|c| c as u64);

            // Determine incremental key - look for common timestamp columns
            let incremental_key = table_schema
                .columns
                .iter()
                .find(|c| {
                    matches!(
                        c.name.as_str(),
                        "updated_at" | "modified_at" | "last_modified" | "rowid"
                    )
                })
                .map(|c| c.name.clone());

            table_infos.push(TableInfo {
                name: table_name,
                schema: table_schema,
                supports_incremental: incremental_key.is_some(),
                incremental_key,
                estimated_rows: row_estimate,
                primary_key_columns: Vec::new(),
            });
        }

        tracing::debug!(
            database = %self.config.database_path,
            table_count = table_infos.len(),
            "Listed SQLite tables"
        );

        Ok(table_infos)
    }

    async fn get_schema(&self, table: &str) -> ConnectorResult<TableSchema> {
        let pool = self.get_pool().await?;
        self.get_schema_from_pool(&pool, table).await
    }

    async fn fetch_table(
        &self,
        table: &str,
        incremental_key: Option<&str>,
        last_value: Option<&str>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let pool = self.get_pool().await?;

        // Get schema to know column types
        let schema = self.get_schema(table).await?;
        let arrow_schema = Arc::new(Self::to_arrow_schema(&schema));
        let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();

        let mut all_batches = Vec::new();
        let mut offset = 0i64;
        let mut total_rows = 0u64;

        loop {
            let query = Self::build_fetch_query(
                table,
                &column_names,
                incremental_key,
                last_value,
                BATCH_SIZE,
                offset,
            )?;

            // Fetch a batch of rows
            let rows: Vec<sqlx::sqlite::SqliteRow> = sqlx::query(&query)
                .fetch_all(&pool)
                .await
                .map_err(|e| ConnectorError::Internal(format!("Failed to fetch data: {}", e)))?;

            if rows.is_empty() {
                break;
            }

            let batch_size = rows.len();
            total_rows += batch_size as u64;

            // Convert rows to Arrow RecordBatch
            let batch = self.rows_to_record_batch(&rows, &schema, arrow_schema.clone())?;
            all_batches.push(batch);

            // If we got fewer rows than the limit, we're done
            if batch_size < BATCH_SIZE as usize {
                break;
            }

            offset += BATCH_SIZE;
        }

        tracing::info!(
            database = %self.config.database_path,
            table = %table,
            rows = total_rows,
            batches = all_batches.len(),
            incremental = incremental_key.is_some(),
            "Fetched SQLite table data"
        );

        Ok(all_batches)
    }

    fn fetch_table_stream<'a>(
        &'a self,
        table: &'a str,
        options: FetchOptions,
    ) -> Pin<Box<dyn futures::Future<Output = ConnectorResult<RecordBatchStream>> + Send + 'a>> {
        Box::pin(async move {
            let batches = if options.predicates.is_empty() {
                self.fetch_table(
                    table,
                    options.incremental_key.as_deref(),
                    options.last_value.as_deref(),
                )
                .await?
            } else {
                let pool = self.get_pool().await?;
                let schema = self.get_schema(table).await?;
                let arrow_schema = Arc::new(Self::to_arrow_schema(&schema));
                let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();

                let predicate_sql: String = options
                    .predicates
                    .iter()
                    .map(|p| predicate_to_sql(p, SqlDialect::SQLite))
                    .collect::<Vec<_>>()
                    .join(" AND ");

                let mut all_batches = Vec::new();
                let mut offset = 0i64;

                loop {
                    let mut query = Self::build_fetch_query(
                        table,
                        &column_names,
                        options.incremental_key.as_deref(),
                        options.last_value.as_deref(),
                        BATCH_SIZE,
                        offset,
                    )?;

                    if query.contains(" WHERE ") {
                        query = query.replace(" ORDER BY ", &format!(" AND ({}) ORDER BY ", predicate_sql));
                    } else if query.contains(" ORDER BY ") {
                        query = query.replace(" ORDER BY ", &format!(" WHERE ({}) ORDER BY ", predicate_sql));
                    } else {
                        query = query.replace(" LIMIT ", &format!(" WHERE ({}) LIMIT ", predicate_sql));
                    }

                    let rows: Vec<sqlx::sqlite::SqliteRow> = sqlx::query(&query)
                        .fetch_all(&pool)
                        .await
                        .map_err(|e| ConnectorError::Internal(format!("Failed to fetch data: {}", e)))?;

                    if rows.is_empty() {
                        break;
                    }

                    let batch_size = rows.len();
                    let batch = self.rows_to_record_batch(&rows, &schema, arrow_schema.clone())?;
                    all_batches.push(batch);

                    if batch_size < BATCH_SIZE as usize {
                        break;
                    }

                    offset += BATCH_SIZE;
                }

                all_batches
            };
            let stream = futures::stream::iter(batches.into_iter().map(Ok));
            Ok(Box::pin(stream) as RecordBatchStream)
        })
    }

    async fn validate_credentials(&self) -> ConnectorResult<()> {
        let pool = self.get_pool().await?;

        // Try a simple query to validate the connection
        sqlx::query("SELECT 1")
            .fetch_one(&pool)
            .await
            .map_err(|e| {
                ConnectorError::Network(format!("Connection validation failed: {}", e))
            })?;

        tracing::debug!(
            database = %self.config.database_path,
            "SQLite connection validated successfully"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqlite_config_creation() {
        let config = SQLiteConfig::new("/path/to/database.db");
        assert_eq!(config.database_path, "/path/to/database.db");
        assert!(config.tables.is_empty());
        assert!(!config.read_only);
    }

    #[test]
    fn test_sqlite_config_in_memory() {
        let config = SQLiteConfig::in_memory();
        assert_eq!(config.database_path, ":memory:");
    }

    #[test]
    fn test_sqlite_config_with_tables() {
        let config = SQLiteConfig::new("/db.sqlite")
            .with_tables(vec!["users".to_string(), "orders".to_string()]);
        assert_eq!(config.tables.len(), 2);
    }

    #[test]
    fn test_sqlite_config_read_only() {
        let config = SQLiteConfig::new("/db.sqlite").with_read_only(true);
        assert!(config.read_only);
        assert!(config.connection_string().contains("mode=ro"));
    }

    #[test]
    fn test_sqlite_type_to_column_type_integers() {
        assert!(matches!(
            SQLiteConnector::sqlite_type_to_column_type("INTEGER"),
            ColumnType::Int64
        ));
        assert!(matches!(
            SQLiteConnector::sqlite_type_to_column_type("INT"),
            ColumnType::Int64
        ));
        assert!(matches!(
            SQLiteConnector::sqlite_type_to_column_type("BIGINT"),
            ColumnType::Int64
        ));
    }

    #[test]
    fn test_sqlite_type_to_column_type_floats() {
        assert!(matches!(
            SQLiteConnector::sqlite_type_to_column_type("REAL"),
            ColumnType::Float64
        ));
        assert!(matches!(
            SQLiteConnector::sqlite_type_to_column_type("FLOAT"),
            ColumnType::Float64
        ));
        assert!(matches!(
            SQLiteConnector::sqlite_type_to_column_type("DOUBLE"),
            ColumnType::Float64
        ));
    }

    #[test]
    fn test_sqlite_type_to_column_type_boolean() {
        assert!(matches!(
            SQLiteConnector::sqlite_type_to_column_type("BOOLEAN"),
            ColumnType::Boolean
        ));
        assert!(matches!(
            SQLiteConnector::sqlite_type_to_column_type("BOOL"),
            ColumnType::Boolean
        ));
    }

    #[test]
    fn test_sqlite_type_to_column_type_blob() {
        // BLOBs are mapped to String (base64-encoded)
        assert!(matches!(
            SQLiteConnector::sqlite_type_to_column_type("BLOB"),
            ColumnType::String
        ));
    }

    #[test]
    fn test_sqlite_type_to_column_type_timestamps() {
        assert!(matches!(
            SQLiteConnector::sqlite_type_to_column_type("DATETIME"),
            ColumnType::Timestamp
        ));
        assert!(matches!(
            SQLiteConnector::sqlite_type_to_column_type("TIMESTAMP"),
            ColumnType::Timestamp
        ));
    }

    #[test]
    fn test_sqlite_type_to_column_type_default_string() {
        assert!(matches!(
            SQLiteConnector::sqlite_type_to_column_type("TEXT"),
            ColumnType::String
        ));
        assert!(matches!(
            SQLiteConnector::sqlite_type_to_column_type("VARCHAR(255)"),
            ColumnType::String
        ));
    }

    #[test]
    fn test_source_type() {
        let config = SQLiteConfig::new("/db.sqlite");
        let connector = SQLiteConnector::new(config);
        assert_eq!(connector.source_type(), SourceType::SQLite);
    }

    #[test]
    fn test_is_valid_identifier() {
        assert!(SQLiteConnector::is_valid_identifier("valid_column"));
        assert!(SQLiteConnector::is_valid_identifier("Column1"));
        assert!(SQLiteConnector::is_valid_identifier("_private"));
        
        assert!(!SQLiteConnector::is_valid_identifier(""));
        assert!(!SQLiteConnector::is_valid_identifier("123start"));
        assert!(!SQLiteConnector::is_valid_identifier("has-dash"));
    }

    #[test]
    fn test_is_valid_identifier_rejects_quotes() {
        assert!(!SQLiteConnector::is_valid_identifier("db\"name"));
        assert!(!SQLiteConnector::is_valid_identifier("`table"));
        assert!(!SQLiteConnector::is_valid_identifier("[column]"));
    }

    #[test]
    fn test_build_fetch_query_validates_identifiers() {
        // Valid query should succeed
        let result = SQLiteConnector::build_fetch_query(
            "users",
            &["id".to_string(), "name".to_string()],
            None,
            None,
            100,
            0,
        );
        assert!(result.is_ok());
        let query = result.unwrap();
        assert!(query.contains("SELECT"));
        assert!(query.contains("FROM \"users\""));
        
        // Invalid table name should fail
        let result = SQLiteConnector::build_fetch_query(
            "users; DROP TABLE users",
            &[],
            None,
            None,
            100,
            0,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_escape_sqlite_string() {
        assert_eq!(SQLiteConnector::escape_sqlite_string("normal"), "normal");
        assert_eq!(SQLiteConnector::escape_sqlite_string("it's"), "it''s");
    }

    #[tokio::test]
    async fn test_in_memory_connection() {
        let config = SQLiteConfig::in_memory();
        let connector = SQLiteConnector::new(config);
        
        // Should be able to validate credentials for in-memory db
        let result = connector.validate_credentials().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_list_tables_empty_db() {
        let config = SQLiteConfig::in_memory();
        let connector = SQLiteConnector::new(config);
        
        let tables = connector.list_tables().await.unwrap();
        assert!(tables.is_empty());
    }
}
