//! MySQL Connector
//!
//! Connects to MySQL/MariaDB databases and syncs data to the warehouse.
//!
//! # Features
//!
//! - Automatic schema discovery from `information_schema`
//! - Incremental sync support with configurable key columns
//! - Connection pooling for efficient resource usage
//! - Batched data fetching to prevent memory issues
//!
//! # Usage
//!
//! ```ignore
//! let config = MySqlConfig::new("mysql://user:pass@localhost/mydb");
//! let connector = MySqlConnector::new(config);
//!
//! let tables = connector.list_tables().await?;
//! let data = connector.fetch_table("users", None, None).await?;
//! ```

use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tokio::sync::OnceCell;

use crate::warehouse::connectors::{
    Connector, ConnectorError, ConnectorResult, FetchOptions, RecordBatchStream, TableInfo,
};
use crate::warehouse::types::{ColumnSchema, ColumnType, SourceType, TableSchema};

/// Maximum rows to fetch per batch to prevent memory issues.
const BATCH_SIZE: i64 = 10_000;

/// MySQL connector configuration.
#[derive(Debug, Clone)]
pub struct MySqlConfig {
    /// MySQL connection string
    pub connection_string: String,
    /// Database name to sync
    pub database: String,
    /// Tables to sync (empty = all tables)
    pub tables: Vec<String>,
    /// Maximum connections in the pool
    pub max_connections: u32,
    /// Connection timeout in seconds
    pub connect_timeout_secs: u64,
}

impl MySqlConfig {
    /// Create a new MySQL configuration from a connection string.
    ///
    /// Connection string format: `mysql://user:password@host:port/database`
    pub fn new(connection_string: impl Into<String>) -> Self {
        let conn_str = connection_string.into();
        
        // Extract database name from connection string
        let database = conn_str
            .rsplit('/')
            .next()
            .unwrap_or("mysql")
            .split('?')
            .next()
            .unwrap_or("mysql")
            .to_string();

        Self {
            connection_string: conn_str,
            database,
            tables: Vec::new(),
            max_connections: 5,
            connect_timeout_secs: 30,
        }
    }

    /// Set the database name explicitly.
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = database.into();
        self
    }

    /// Set tables to sync (empty = all tables).
    pub fn with_tables(mut self, tables: Vec<String>) -> Self {
        self.tables = tables;
        self
    }

    /// Set maximum connections in the pool.
    pub fn with_max_connections(mut self, max_connections: u32) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// Set connection timeout.
    pub fn with_connect_timeout(mut self, timeout_secs: u64) -> Self {
        self.connect_timeout_secs = timeout_secs;
        self
    }
}

/// MySQL data source connector.
///
/// Production-ready connector with connection pooling and incremental sync support.
/// The connection pool is lazily initialized on first use and cached for subsequent calls.
pub struct MySqlConnector {
    config: MySqlConfig,
    /// Connection pool - initialized lazily on first use using OnceCell
    pool: OnceCell<sqlx::MySqlPool>,
}

impl MySqlConnector {
    /// Create a new MySQL connector.
    pub fn new(config: MySqlConfig) -> Self {
        Self {
            config,
            pool: OnceCell::new(),
        }
    }

    /// Create a connector with an existing pool (for testing or shared connections).
    pub fn with_pool(config: MySqlConfig, pool: sqlx::MySqlPool) -> Self {
        let pool_cell = OnceCell::new();
        // Set the pool in the OnceCell - this will not fail since we just created it
        let _ = pool_cell.set(pool);
        Self {
            config,
            pool: pool_cell,
        }
    }

    /// Get or create the connection pool.
    ///
    /// The pool is created lazily on first call and cached for subsequent calls.
    /// This is thread-safe and the pool will only be created once even if called
    /// concurrently from multiple tasks.
    async fn get_pool(&self) -> ConnectorResult<sqlx::MySqlPool> {
        self.pool
            .get_or_try_init(|| async {
                sqlx::mysql::MySqlPoolOptions::new()
                    .max_connections(self.config.max_connections)
                    .acquire_timeout(std::time::Duration::from_secs(
                        self.config.connect_timeout_secs,
                    ))
                    .connect(&self.config.connection_string)
                    .await
                    .map_err(|e| {
                        ConnectorError::Network(format!("Failed to connect to MySQL: {}", e))
                    })
            })
            .await
            .cloned()
    }

    /// Map MySQL type to warehouse column type.
    fn mysql_type_to_column_type(mysql_type: &str) -> ColumnType {
        let lower = mysql_type.to_lowercase();
        
        if lower.contains("tinyint(1)") || lower.contains("boolean") {
            return ColumnType::Boolean;
        }

        match lower.split('(').next().unwrap_or(&lower) {
            "int" | "integer" | "mediumint" | "smallint" | "tinyint" => ColumnType::Int32,
            "bigint" => ColumnType::Int64,
            "float" | "real" => ColumnType::Float64,
            "double" | "double precision" => ColumnType::Float64,
            "decimal" | "numeric" | "dec" | "fixed" => ColumnType::Decimal,
            "bit" | "bool" | "boolean" => ColumnType::Boolean,
            "datetime" | "timestamp" => ColumnType::Timestamp,
            "date" => ColumnType::Date,
            "json" => ColumnType::Json,
            _ => ColumnType::String, // Default to string for unknown types
        }
    }

    /// Convert table schema to Arrow schema.
    fn to_arrow_schema(schema: &TableSchema) -> Schema {
        let fields: Vec<Field> = schema
            .columns
            .iter()
            // Always mark columns as nullable in Arrow to handle edge cases
            .map(|col| Field::new(&col.name, col.data_type.to_arrow_type(), true))
            .collect();
        Schema::new(fields)
    }

    /// Escape a string value for use in MySQL SQL.
    ///
    /// # Security
    ///
    /// This escapes special characters to prevent SQL injection.
    fn escape_mysql_string(s: &str) -> String {
        let mut result = String::with_capacity(s.len() + s.len() / 8);
        for ch in s.chars() {
            match ch {
                '\'' => result.push_str("''"),
                '\\' => result.push_str("\\\\"),
                '\0' => {} // Remove null bytes
                _ => result.push(ch),
            }
        }
        result
    }

    /// Validate an identifier (database, table, or column name) for use in SQL.
    ///
    /// # Security
    ///
    /// This prevents SQL injection by ensuring identifiers contain only safe characters.
    /// Valid identifiers:
    /// - Start with a letter or underscore
    /// - Contain only ASCII alphanumeric characters and underscores
    /// - Are between 1 and 128 characters
    /// - Do not contain backticks (which could break out of backtick quoting)
    fn is_valid_identifier(name: &str) -> bool {
        if name.is_empty() || name.len() > 128 {
            return false;
        }

        // Reject backticks which could break out of identifier quoting
        if name.contains('`') {
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
    ///
    /// # Security
    ///
    /// All identifiers (database, table, columns) are validated before use.
    /// This prevents SQL injection attacks via malicious identifier names.
    ///
    /// # Errors
    ///
    /// Returns an error if any identifier fails validation.
    fn build_fetch_query(
        database: &str,
        table: &str,
        columns: &[String],
        incremental_key: Option<&str>,
        last_value: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> ConnectorResult<String> {
        // Validate database name
        if !Self::is_valid_identifier(database) {
            return Err(ConnectorError::Config(format!(
                "Invalid database name: '{}'. Names must start with a letter or underscore and contain only alphanumeric characters and underscores.",
                database
            )));
        }

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
            // Validate all column names
            for col in columns {
                if !Self::is_valid_identifier(col) {
                    return Err(ConnectorError::Config(format!(
                        "Invalid column name: '{}'. Names must start with a letter or underscore and contain only alphanumeric characters and underscores.",
                        col
                    )));
                }
            }
            columns
                .iter()
                .map(|c| format!("`{}`", c))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let mut query = format!(
            "SELECT {} FROM `{}`.`{}`",
            columns_str, database, table
        );

        // Add incremental filter if provided
        if let (Some(key), Some(value)) = (incremental_key, last_value) {
            if !Self::is_valid_identifier(key) {
                return Err(ConnectorError::Config(format!(
                    "Invalid incremental key column: '{}'",
                    key
                )));
            }
            let escaped_value = Self::escape_mysql_string(value);
            query.push_str(&format!(" WHERE `{}` > '{}'", key, escaped_value));
        }

        // Add ordering for consistent pagination
        if let Some(key) = incremental_key {
            if Self::is_valid_identifier(key) {
                query.push_str(&format!(" ORDER BY `{}` ASC", key));
            }
        }

        // Add pagination
        query.push_str(&format!(" LIMIT {} OFFSET {}", limit, offset));

        Ok(query)
    }

    /// Get schema for a table using an existing pool connection.
    async fn get_schema_from_pool(
        &self,
        pool: &sqlx::MySqlPool,
        table: &str,
    ) -> ConnectorResult<TableSchema> {
        // Cast columns to CHAR to handle VARBINARY charset issues
        let columns: Vec<(String, String, String)> = sqlx::query_as(
            r#"
            SELECT 
                CAST(COLUMN_NAME AS CHAR(255)) AS COLUMN_NAME,
                CAST(DATA_TYPE AS CHAR(255)) AS DATA_TYPE,
                CAST(IS_NULLABLE AS CHAR(10)) AS IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
            "#,
        )
        .bind(&self.config.database)
        .bind(table)
        .fetch_all(pool)
        .await
        .map_err(|e| {
            ConnectorError::Internal(format!("Failed to get schema for {}: {}", table, e))
        })?;

        if columns.is_empty() {
            return Err(ConnectorError::Internal(format!(
                "Table {}.{} not found or has no columns",
                self.config.database, table
            )));
        }

        let columns: Vec<ColumnSchema> = columns
            .into_iter()
            .map(|(name, data_type, is_nullable)| {
                let col_type = Self::mysql_type_to_column_type(&data_type);
                let timezone = if matches!(col_type, ColumnType::Timestamp) {
                    Some("UTC".to_string())
                } else {
                    None
                };
                ColumnSchema {
                    name,
                    data_type: col_type,
                    nullable: is_nullable.to_lowercase() == "yes",
                    description: None,
                    timezone,
                }
            })
            .collect();

        Ok(TableSchema { columns })
    }

    /// Convert MySQL rows to an Arrow RecordBatch.
    fn rows_to_record_batch(
        &self,
        rows: &[sqlx::mysql::MySqlRow],
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<RecordBatch> {
        use rust_decimal::prelude::ToPrimitive;
        use sqlx::Row;

        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.columns.len());

        for (col_idx, col) in schema.columns.iter().enumerate() {
            let array: ArrayRef = match col.data_type {
                ColumnType::Int32 => {
                    let mut values = Vec::with_capacity(rows.len());
                    values.extend(rows.iter().map(|row| row.try_get::<i32, _>(col_idx).ok()));
                    Arc::new(Int32Array::from(values))
                }
                ColumnType::Int64 => {
                    let mut values = Vec::with_capacity(rows.len());
                    values.extend(rows.iter().map(|row| row.try_get::<i64, _>(col_idx).ok()));
                    Arc::new(Int64Array::from(values))
                }
                ColumnType::Float64 => {
                    let mut values = Vec::with_capacity(rows.len());
                    values.extend(rows.iter().map(|row| row.try_get::<f64, _>(col_idx).ok()));
                    Arc::new(Float64Array::from(values))
                }
                ColumnType::Decimal => {
                    // Fetch as Decimal and convert to f64
                    let mut values = Vec::with_capacity(rows.len());
                    values.extend(rows.iter().map(|row| {
                        row.try_get::<rust_decimal::Decimal, _>(col_idx)
                            .ok()
                            .and_then(|d| d.to_f64())
                    }));
                    Arc::new(Float64Array::from(values))
                }
                ColumnType::Boolean => {
                    let mut values = Vec::with_capacity(rows.len());
                    values.extend(rows.iter().map(|row| row.try_get::<bool, _>(col_idx).ok()));
                    Arc::new(BooleanArray::from(values))
                }
                ColumnType::Timestamp => {
                    // MySQL DATETIME/TIMESTAMP -> chrono::NaiveDateTime
                    let mut values = Vec::with_capacity(rows.len());
                    values.extend(rows.iter().map(|row| {
                        row.try_get::<chrono::NaiveDateTime, _>(col_idx)
                            .ok()
                            .map(|dt| dt.and_utc().timestamp_micros())
                    }));
                    Arc::new(TimestampMicrosecondArray::from(values))
                }
                ColumnType::Date => {
                    // MySQL DATE -> chrono::NaiveDate
                    let mut values = Vec::with_capacity(rows.len());
                    values.extend(rows.iter().map(|row| {
                        row.try_get::<chrono::NaiveDate, _>(col_idx)
                            .ok()
                            .map(|d| {
                                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                                (d - epoch).num_days() as i32
                            })
                    }));
                    Arc::new(Date32Array::from(values))
                }
                _ => {
                    // Default: convert to string
                    let mut values = Vec::with_capacity(rows.len());
                    values.extend(rows.iter().map(|row| row.try_get::<String, _>(col_idx).ok()));
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
impl Connector for MySqlConnector {
    fn source_type(&self) -> SourceType {
        SourceType::MySQL
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        let pool = self.get_pool().await?;

        // Query information_schema for tables
        // Cast TABLE_NAME to CHAR to handle VARBINARY charset issues
        // Cast TABLE_ROWS to SIGNED to handle BIGINT UNSIGNED
        let tables: Vec<(String, Option<i64>)> = sqlx::query_as(
            r#"
            SELECT 
                CAST(TABLE_NAME AS CHAR(255)) AS TABLE_NAME,
                CAST(TABLE_ROWS AS SIGNED) AS TABLE_ROWS
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = ?
              AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
            "#,
        )
        .bind(&self.config.database)
        .fetch_all(&pool)
        .await
        .map_err(|e| ConnectorError::Internal(format!("Failed to list tables: {}", e)))?;

        // Discover primary keys for all tables in one query
        let pk_rows: Vec<(String, String)> = sqlx::query_as(
            r#"
            SELECT CAST(TABLE_NAME AS CHAR(255)), CAST(COLUMN_NAME AS CHAR(255))
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE CONSTRAINT_NAME = 'PRIMARY'
              AND TABLE_SCHEMA = ?
            ORDER BY TABLE_NAME, ORDINAL_POSITION
            "#,
        )
        .bind(&self.config.database)
        .fetch_all(&pool)
        .await
        .unwrap_or_default();

        let mut pk_map: std::collections::HashMap<String, Vec<String>> = std::collections::HashMap::new();
        for (table_name, col_name) in pk_rows {
            pk_map.entry(table_name).or_default().push(col_name);
        }

        // For each table, get its schema
        let mut table_infos = Vec::new();
        for (table_name, row_estimate) in tables {
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

            // Determine incremental key - look for common timestamp columns
            let incremental_key = table_schema
                .columns
                .iter()
                .find(|c| {
                    matches!(
                        c.name.as_str(),
                        "updated_at" | "modified_at" | "last_modified"
                    )
                })
                .map(|c| c.name.clone());

            let primary_key_columns = pk_map.remove(&table_name).unwrap_or_default();

            table_infos.push(TableInfo {
                name: table_name,
                schema: table_schema,
                supports_incremental: incremental_key.is_some(),
                incremental_key,
                estimated_rows: row_estimate.and_then(|c| if c >= 0 { Some(c as u64) } else { None }),
                primary_key_columns,
            });
        }

        tracing::debug!(
            database = %self.config.database,
            table_count = table_infos.len(),
            "Listed MySQL tables"
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
                &self.config.database,
                table,
                &column_names,
                incremental_key,
                last_value,
                BATCH_SIZE,
                offset,
            )?;

            // Fetch a batch of rows
            let rows: Vec<sqlx::mysql::MySqlRow> = sqlx::query(&query)
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
            database = %self.config.database,
            table = %table,
            rows = total_rows,
            batches = all_batches.len(),
            incremental = incremental_key.is_some(),
            "Fetched MySQL table data"
        );

        Ok(all_batches)
    }

    fn fetch_table_stream<'a>(
        &'a self,
        table: &'a str,
        options: FetchOptions,
    ) -> Pin<Box<dyn futures::Future<Output = ConnectorResult<RecordBatchStream>> + Send + 'a>> {
        Box::pin(async move {
            let pool = self.get_pool().await?;
            let schema = self.get_schema(table).await?;
            let arrow_schema = Arc::new(Self::to_arrow_schema(&schema));
            let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();

            let mut all_batches = Vec::new();
            let mut offset = 0i64;

            loop {
                let mut query = Self::build_fetch_query(
                    &self.config.database,
                    table,
                    &column_names,
                    options.incremental_key.as_deref(),
                    options.last_value.as_deref(),
                    BATCH_SIZE,
                    offset,
                )?;

                if !options.predicates.is_empty() {
                    use crate::warehouse::query::predicate_pushdown::{predicate_to_sql, SqlDialect};
                    let pred_sql = options.predicates.iter()
                        .map(|p| predicate_to_sql(p, SqlDialect::MySQL))
                        .collect::<Vec<_>>()
                        .join(" AND ");
                    let upper = query.to_uppercase();
                    if let Some(pos) = upper.find(" WHERE ") {
                        let insert_at = pos + " WHERE ".len();
                        query.insert_str(insert_at, &format!("({}) AND ", pred_sql));
                    } else if let Some(pos) = upper.find(" ORDER BY ") {
                        query.insert_str(pos, &format!(" WHERE ({})", pred_sql));
                    } else if let Some(pos) = upper.find(" LIMIT ") {
                        query.insert_str(pos, &format!(" WHERE ({})", pred_sql));
                    } else {
                        query.push_str(&format!(" WHERE ({})", pred_sql));
                    }
                }

                let rows: Vec<sqlx::mysql::MySqlRow> = sqlx::query(&query)
                    .fetch_all(&pool)
                    .await
                    .map_err(|e| ConnectorError::Internal(format!("Failed to fetch data: {}", e)))?;

                if rows.is_empty() { break; }

                let count = rows.len();
                let batch = self.rows_to_record_batch(&rows, &schema, arrow_schema.clone())?;
                all_batches.push(batch);

                if count < BATCH_SIZE as usize { break; }
                offset += BATCH_SIZE;
            }

            let stream = futures::stream::iter(all_batches.into_iter().map(Ok));
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

        tracing::debug!("MySQL credentials validated successfully");
        Ok(())
    }
    
    fn supports_sql_pushdown(&self) -> bool {
        true // MySQL supports arbitrary SQL execution
    }
    
    fn supports_cdc(&self) -> bool {
        true // MySQL supports binlog-based change tracking
    }
    
    async fn execute_sql(&self, sql: &str) -> ConnectorResult<Vec<RecordBatch>> {
        crate::warehouse::connectors::enforce_read_only_sql(sql)?;

        let pool = self.get_pool().await?;
        
        // Execute the query
        let rows: Vec<sqlx::mysql::MySqlRow> = sqlx::query(sql)
            .fetch_all(&pool)
            .await
            .map_err(|e| ConnectorError::Internal(format!("Failed to execute SQL: {}", e)))?;
        
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        
        // Infer schema from the result columns
        let schema = self.infer_schema_from_rows(&rows)?;
        let arrow_schema = Arc::new(Self::to_arrow_schema(&schema));
        
        // Convert rows to RecordBatch
        let batch = self.rows_to_record_batch(&rows, &schema, arrow_schema)?;
        
        tracing::info!(
            sql_length = sql.len(),
            rows = rows.len(),
            "Executed MySQL SQL query"
        );
        
        Ok(vec![batch])
    }
}

impl MySqlConnector {
    /// Infer schema from query result rows.
    fn infer_schema_from_rows(&self, rows: &[sqlx::mysql::MySqlRow]) -> ConnectorResult<TableSchema> {
        use sqlx::{Column, Row, TypeInfo};
        
        if rows.is_empty() {
            return Ok(TableSchema { columns: vec![] });
        }
        
        let first_row = &rows[0];
        let columns: Vec<ColumnSchema> = first_row
            .columns()
            .iter()
            .map(|col| {
                let mysql_type = col.type_info().name().to_string();
                let col_type = Self::mysql_type_to_column_type(&mysql_type);
                ColumnSchema {
                    name: col.name().to_string(),
                    data_type: col_type,
                    nullable: true, // Assume nullable for dynamic queries
                    description: None,
                    timezone: None,
                }
            })
            .collect();
        
        Ok(TableSchema { columns })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mysql_config_creation() {
        let config = MySqlConfig::new("mysql://user:pass@localhost/testdb");
        assert_eq!(config.connection_string, "mysql://user:pass@localhost/testdb");
        assert_eq!(config.database, "testdb");
        assert!(config.tables.is_empty());
    }

    #[test]
    fn test_mysql_config_with_database() {
        let config = MySqlConfig::new("mysql://localhost/db").with_database("custom_db");
        assert_eq!(config.database, "custom_db");
    }

    #[test]
    fn test_mysql_config_with_tables() {
        let config = MySqlConfig::new("mysql://localhost/db")
            .with_tables(vec!["users".to_string(), "orders".to_string()]);
        assert_eq!(config.tables.len(), 2);
    }

    #[test]
    fn test_mysql_type_to_column_type_integers() {
        assert!(matches!(
            MySqlConnector::mysql_type_to_column_type("int"),
            ColumnType::Int32
        ));
        assert!(matches!(
            MySqlConnector::mysql_type_to_column_type("bigint"),
            ColumnType::Int64
        ));
        assert!(matches!(
            MySqlConnector::mysql_type_to_column_type("smallint"),
            ColumnType::Int32
        ));
    }

    #[test]
    fn test_mysql_type_to_column_type_floats() {
        assert!(matches!(
            MySqlConnector::mysql_type_to_column_type("float"),
            ColumnType::Float64
        ));
        assert!(matches!(
            MySqlConnector::mysql_type_to_column_type("double"),
            ColumnType::Float64
        ));
    }

    #[test]
    fn test_mysql_type_to_column_type_boolean() {
        assert!(matches!(
            MySqlConnector::mysql_type_to_column_type("tinyint(1)"),
            ColumnType::Boolean
        ));
        assert!(matches!(
            MySqlConnector::mysql_type_to_column_type("boolean"),
            ColumnType::Boolean
        ));
    }

    #[test]
    fn test_mysql_type_to_column_type_timestamps() {
        assert!(matches!(
            MySqlConnector::mysql_type_to_column_type("datetime"),
            ColumnType::Timestamp
        ));
        assert!(matches!(
            MySqlConnector::mysql_type_to_column_type("timestamp"),
            ColumnType::Timestamp
        ));
    }

    #[test]
    fn test_mysql_type_to_column_type_date() {
        assert!(matches!(
            MySqlConnector::mysql_type_to_column_type("date"),
            ColumnType::Date
        ));
    }

    #[test]
    fn test_mysql_type_to_column_type_json() {
        assert!(matches!(
            MySqlConnector::mysql_type_to_column_type("json"),
            ColumnType::Json
        ));
    }

    #[test]
    fn test_mysql_type_to_column_type_default_string() {
        assert!(matches!(
            MySqlConnector::mysql_type_to_column_type("varchar"),
            ColumnType::String
        ));
        assert!(matches!(
            MySqlConnector::mysql_type_to_column_type("text"),
            ColumnType::String
        ));
    }

    #[test]
    fn test_source_type() {
        let config = MySqlConfig::new("mysql://localhost/db");
        let connector = MySqlConnector::new(config);
        assert_eq!(connector.source_type(), SourceType::MySQL);
    }

    #[test]
    fn test_is_valid_column_name() {
        assert!(MySqlConnector::is_valid_identifier("valid_column"));
        assert!(MySqlConnector::is_valid_identifier("Column1"));
        assert!(MySqlConnector::is_valid_identifier("_private"));

        assert!(!MySqlConnector::is_valid_identifier(""));
        assert!(!MySqlConnector::is_valid_identifier("123start"));
        assert!(!MySqlConnector::is_valid_identifier("has-dash"));
    }

    #[test]
    fn test_is_valid_identifier_rejects_backticks() {
        // Backticks could be used to break out of identifier quoting
        assert!(!MySqlConnector::is_valid_identifier("db`name"));
        assert!(!MySqlConnector::is_valid_identifier("`table"));
        assert!(!MySqlConnector::is_valid_identifier("table`"));
        assert!(!MySqlConnector::is_valid_identifier("ta`ble"));
    }

    #[test]
    fn test_is_valid_identifier_rejects_sql_injection_attempts() {
        // Common SQL injection patterns
        assert!(!MySqlConnector::is_valid_identifier("table; DROP TABLE users"));
        assert!(!MySqlConnector::is_valid_identifier("table--"));
        assert!(!MySqlConnector::is_valid_identifier("table/**/"));
        assert!(!MySqlConnector::is_valid_identifier("1=1"));
        assert!(!MySqlConnector::is_valid_identifier("' OR '1'='1"));
    }

    #[test]
    fn test_build_fetch_query_validates_identifiers() {
        // Valid query should succeed
        let result = MySqlConnector::build_fetch_query(
            "mydb",
            "users",
            &["id".to_string(), "name".to_string()],
            None,
            None,
            100,
            0,
        );
        assert!(result.is_ok());
        
        // Invalid database name should fail
        let result = MySqlConnector::build_fetch_query(
            "my`db",
            "users",
            &[],
            None,
            None,
            100,
            0,
        );
        assert!(result.is_err());
        
        // Invalid table name should fail
        let result = MySqlConnector::build_fetch_query(
            "mydb",
            "users; DROP TABLE users",
            &[],
            None,
            None,
            100,
            0,
        );
        assert!(result.is_err());
        
        // Invalid column name should fail
        let result = MySqlConnector::build_fetch_query(
            "mydb",
            "users",
            &["valid".to_string(), "in`valid".to_string()],
            None,
            None,
            100,
            0,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_escape_mysql_string() {
        assert_eq!(MySqlConnector::escape_mysql_string("normal"), "normal");
        assert_eq!(MySqlConnector::escape_mysql_string("it's"), "it''s");
        assert_eq!(MySqlConnector::escape_mysql_string("back\\slash"), "back\\\\slash");
    }
}
