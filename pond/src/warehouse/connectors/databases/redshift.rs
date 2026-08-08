//! Amazon Redshift connector for the data warehouse.
//!
//! Syncs Redshift tables to the warehouse with automatic schema discovery.
//!
//! # Features
//!
//! - Automatic schema discovery from Redshift system views (svv_tables, svv_columns)
//! - Incremental sync support with configurable key columns
//! - Connection pooling using PostgreSQL-compatible driver
//! - Batched data fetching to prevent memory issues
//! - SSL support (required by default for Redshift)
//!
//! # Connection
//!
//! Redshift uses PostgreSQL wire protocol, so we use the same sqlx PostgreSQL driver.
//! Default port is 5439 (not 5432 like standard PostgreSQL).

use std::pin::Pin;

use super::super::{Connector, ConnectorError, ConnectorResult, FetchOptions, RecordBatchStream, TableInfo};
use crate::warehouse::query::predicate_pushdown::{predicate_to_sql, SqlDialect};
use crate::warehouse::types::{ColumnSchema, ColumnType, SourceType, TableSchema};
use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use sqlx::Row;
use std::sync::Arc;

/// Maximum rows to fetch per batch to prevent memory issues.
const BATCH_SIZE: i64 = 10_000;

/// Default Redshift port.
const DEFAULT_PORT: u16 = 5439;

/// SSL mode for Redshift connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SslMode {
    /// Require SSL (default for Redshift)
    Require,
    /// Prefer SSL but allow non-SSL
    Prefer,
    /// Disable SSL (not recommended)
    Disable,
}

impl Default for SslMode {
    fn default() -> Self {
        Self::Require
    }
}

impl std::fmt::Display for SslMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SslMode::Require => write!(f, "require"),
            SslMode::Prefer => write!(f, "prefer"),
            SslMode::Disable => write!(f, "disable"),
        }
    }
}

impl std::str::FromStr for SslMode {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "prefer" => Ok(SslMode::Prefer),
            "disable" => Ok(SslMode::Disable),
            _ => Ok(SslMode::Require), // Default to require
        }
    }
}

/// Redshift connector configuration.
#[derive(Debug, Clone)]
pub struct RedshiftConfig {
    /// Redshift cluster endpoint (cluster-name.xxxx.region.redshift.amazonaws.com)
    pub host: String,
    /// Redshift port (default: 5439)
    pub port: u16,
    /// Database name
    pub database: String,
    /// Username
    pub username: String,
    /// Password
    pub password: String,
    /// Schema to sync (default: "public")
    pub schema: String,
    /// Tables to sync (empty = all tables)
    pub tables: Vec<String>,
    /// SSL mode (default: Require)
    pub ssl_mode: SslMode,
    /// Maximum connections in the pool
    pub max_connections: u32,
    /// Connection timeout in seconds
    pub connect_timeout_secs: u64,
}

impl RedshiftConfig {
    /// Create a new Redshift configuration.
    pub fn new(
        host: impl Into<String>,
        database: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port: DEFAULT_PORT,
            database: database.into(),
            username: username.into(),
            password: password.into(),
            schema: "public".to_string(),
            tables: Vec::new(),
            ssl_mode: SslMode::default(),
            max_connections: 5,
            connect_timeout_secs: 30,
        }
    }

    /// Set the port.
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set the schema.
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = schema.into();
        self
    }

    /// Set specific tables to sync.
    pub fn with_tables(mut self, tables: Vec<String>) -> Self {
        self.tables = tables;
        self
    }

    /// Set the SSL mode.
    pub fn with_ssl_mode(mut self, ssl_mode: SslMode) -> Self {
        self.ssl_mode = ssl_mode;
        self
    }

    /// Set the maximum connections.
    pub fn with_max_connections(mut self, max_connections: u32) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// Set the connection timeout.
    pub fn with_connect_timeout(mut self, timeout_secs: u64) -> Self {
        self.connect_timeout_secs = timeout_secs;
        self
    }

    /// Build the connection string for sqlx.
    fn connection_string(&self) -> String {
        format!(
            "postgresql://{}:{}@{}:{}/{}?sslmode={}",
            urlencoding::encode(&self.username),
            urlencoding::encode(&self.password),
            &self.host,
            self.port,
            urlencoding::encode(&self.database),
            self.ssl_mode
        )
    }
}

/// Amazon Redshift data source connector.
///
/// Production-ready connector with connection pooling and incremental sync support.
/// Uses PostgreSQL-compatible driver since Redshift uses PG wire protocol.
pub struct RedshiftConnector {
    config: RedshiftConfig,
    /// Connection pool - initialized lazily on first use
    pool: Option<sqlx::PgPool>,
}

impl RedshiftConnector {
    /// Create a new Redshift connector.
    pub fn new(config: RedshiftConfig) -> Self {
        Self { config, pool: None }
    }

    /// Create a connector with an existing pool (for testing or shared connections).
    pub fn with_pool(config: RedshiftConfig, pool: sqlx::PgPool) -> Self {
        Self {
            config,
            pool: Some(pool),
        }
    }

    /// Get or create the connection pool.
    async fn get_pool(&self) -> ConnectorResult<sqlx::PgPool> {
        if let Some(pool) = &self.pool {
            return Ok(pool.clone());
        }

        // Create a new connection pool
        let connection_string = self.config.connection_string();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(self.config.max_connections)
            .acquire_timeout(std::time::Duration::from_secs(
                self.config.connect_timeout_secs,
            ))
            .connect(&connection_string)
            .await
            .map_err(|e| {
                ConnectorError::Network(format!("Failed to connect to Redshift: {}", e))
            })?;

        Ok(pool)
    }

    /// Map Redshift type to warehouse column type.
    fn redshift_type_to_column_type(rs_type: &str) -> ColumnType {
        match rs_type.to_lowercase().as_str() {
            // Integer types
            "integer" | "int" | "int4" => ColumnType::Int32,
            "bigint" | "int8" => ColumnType::Int64,
            "smallint" | "int2" => ColumnType::Int32,

            // Floating point types
            "real" | "float4" => ColumnType::Float64,
            "double precision" | "float8" | "float" => ColumnType::Float64,
            "numeric" | "decimal" => ColumnType::Decimal,

            // Boolean
            "boolean" | "bool" => ColumnType::Boolean,

            // Date/Time types
            "timestamp" | "timestamp without time zone" => ColumnType::Timestamp,
            "timestamp with time zone" | "timestamptz" => ColumnType::Timestamp,
            "date" => ColumnType::Date,
            "time" | "time without time zone" | "time with time zone" | "timetz" => {
                ColumnType::String
            }

            // String types
            "character" | "char" | "bpchar" => ColumnType::String,
            "character varying" | "varchar" | "text" => ColumnType::String,
            "name" => ColumnType::String,

            // Redshift-specific types
            "super" => ColumnType::Json, // Redshift SUPER type for semi-structured data
            "geometry" | "geography" => ColumnType::String, // Spatial types as WKT strings

            // Default to string for unknown types
            _ => ColumnType::String,
        }
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

    /// Escape a string value for use in Redshift SQL.
    fn escape_string(s: &str) -> String {
        // Remove null bytes to prevent truncation attacks
        let s = s.replace('\0', "");

        let mut result = String::with_capacity(s.len() + s.len() / 8);
        for ch in s.chars() {
            match ch {
                '\'' => result.push_str("''"),
                '\\' => result.push_str("\\\\"),
                _ => result.push(ch),
            }
        }
        result
    }

    /// Validate a column name for use in SQL.
    fn is_valid_column_name(name: &str) -> bool {
        if name.is_empty() || name.len() > 128 {
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
        schema: &str,
        table: &str,
        columns: &[String],
        incremental_key: Option<&str>,
        last_value: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> String {
        let columns_str = if columns.is_empty() {
            "*".to_string()
        } else {
            columns
                .iter()
                .map(|c| format!("\"{}\"", c))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let mut query = format!("SELECT {} FROM \"{}\".\"{}\"", columns_str, schema, table);

        // Add incremental filter if provided
        if let (Some(key), Some(value)) = (incremental_key, last_value) {
            if Self::is_valid_column_name(key) {
                let escaped_value = Self::escape_string(value);
                query.push_str(&format!(" WHERE \"{}\" > '{}'", key, escaped_value));
            }
        }

        // Add ordering for consistent pagination
        if let Some(key) = incremental_key {
            if Self::is_valid_column_name(key) {
                query.push_str(&format!(" ORDER BY \"{}\" ASC", key));
            }
        }

        // Add pagination
        query.push_str(&format!(" LIMIT {} OFFSET {}", limit, offset));

        query
    }

    /// Get schema for a specific table from the pool.
    async fn get_schema_from_pool(
        &self,
        pool: &sqlx::PgPool,
        table: &str,
    ) -> ConnectorResult<TableSchema> {
        // Use svv_columns for Redshift schema discovery
        let columns: Vec<(String, String, String)> = sqlx::query_as(
            r#"
            SELECT 
                column_name,
                data_type,
                is_nullable
            FROM svv_columns
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position
            "#,
        )
        .bind(&self.config.schema)
        .bind(table)
        .fetch_all(pool)
        .await
        .map_err(|e| {
            ConnectorError::Internal(format!("Failed to get schema for {}: {}", table, e))
        })?;

        if columns.is_empty() {
            return Err(ConnectorError::Internal(format!(
                "Table {}.{} not found or has no columns",
                self.config.schema, table
            )));
        }

        let columns: Vec<ColumnSchema> = columns
            .into_iter()
            .map(|(name, data_type, is_nullable)| {
                let col_type = Self::redshift_type_to_column_type(&data_type);
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

    /// Convert database rows to Arrow RecordBatch.
    fn rows_to_record_batch(
        &self,
        rows: &[sqlx::postgres::PgRow],
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<RecordBatch> {
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.columns.len());

        for col_schema in &schema.columns {
            let array: ArrayRef = match col_schema.data_type {
                ColumnType::Int32 | ColumnType::Int64 => {
                    let mut values = Vec::with_capacity(rows.len());
                    values.extend(rows.iter().map(|row| row.try_get::<i64, _>(col_schema.name.as_str()).ok()));
                    Arc::new(Int64Array::from(values))
                }
                ColumnType::Float64 | ColumnType::Decimal => {
                    let mut values = Vec::with_capacity(rows.len());
                    values.extend(rows.iter().map(|row| row.try_get::<f64, _>(col_schema.name.as_str()).ok()));
                    Arc::new(Float64Array::from(values))
                }
                ColumnType::Boolean => {
                    let mut values = Vec::with_capacity(rows.len());
                    values.extend(rows.iter().map(|row| row.try_get::<bool, _>(col_schema.name.as_str()).ok()));
                    Arc::new(BooleanArray::from(values))
                }
                _ => {
                    // Default to string for all other types
                    let mut values = Vec::with_capacity(rows.len());
                    values.extend(rows.iter().map(|row| {
                        // Try various ways to get the value as a string
                        row.try_get::<String, _>(col_schema.name.as_str())
                            .ok()
                            .or_else(|| {
                                row.try_get::<i64, _>(col_schema.name.as_str())
                                    .ok()
                                    .map(|v| v.to_string())
                            })
                            .or_else(|| {
                                row.try_get::<f64, _>(col_schema.name.as_str())
                                    .ok()
                                    .map(|v| v.to_string())
                            })
                    }));
                    Arc::new(StringArray::from(values))
                }
            };
            columns.push(array);
        }

        RecordBatch::try_new(arrow_schema, columns)
            .map_err(|e| ConnectorError::Internal(format!("Failed to create RecordBatch: {}", e)))
    }
}

#[async_trait]
impl Connector for RedshiftConnector {
    fn source_type(&self) -> SourceType {
        SourceType::Redshift
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        let pool = self.get_pool().await?;

        // Query svv_tables for tables in the configured schema
        // svv_tables is Redshift's system view for table metadata
        let tables: Vec<(String,)> = sqlx::query_as(
            r#"
            SELECT table_name
            FROM svv_tables
            WHERE table_schema = $1
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
            "#,
        )
        .bind(&self.config.schema)
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
                        "Failed to get schema for Redshift table, skipping"
                    );
                    continue;
                }
            };

            // Try to get row estimate from SVV_TABLE_INFO
            let row_estimate: Option<i64> = sqlx::query_scalar(
                r#"
                SELECT tbl_rows::bigint
                FROM svv_table_info
                WHERE schema = $1 AND "table" = $2
                "#,
            )
            .bind(&self.config.schema)
            .bind(&table_name)
            .fetch_optional(&pool)
            .await
            .ok()
            .flatten();

            // Determine incremental key
            let incremental_key = table_schema
                .columns
                .iter()
                .find(|c| {
                    matches!(
                        c.name.as_str(),
                        "updated_at" | "modified_at" | "last_modified" | "_sdc_received_at"
                    )
                })
                .map(|c| c.name.clone());

            table_infos.push(TableInfo {
                name: table_name,
                schema: table_schema,
                supports_incremental: incremental_key.is_some(),
                incremental_key,
                estimated_rows: row_estimate.map(|c| c as u64),
                primary_key_columns: Vec::new(),
            });
        }

        tracing::debug!(
            schema = %self.config.schema,
            table_count = table_infos.len(),
            "Listed Redshift tables"
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
                &self.config.schema,
                table,
                &column_names,
                incremental_key,
                last_value,
                BATCH_SIZE,
                offset,
            );

            // Fetch a batch of rows
            let rows: Vec<sqlx::postgres::PgRow> = sqlx::query(&query)
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
            schema = %self.config.schema,
            table = %table,
            rows = total_rows,
            batches = all_batches.len(),
            incremental = incremental_key.is_some(),
            "Fetched Redshift table data"
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
                    .map(|p| predicate_to_sql(p, SqlDialect::Redshift))
                    .collect::<Vec<_>>()
                    .join(" AND ");

                let mut all_batches = Vec::new();
                let mut offset = 0i64;

                loop {
                    let mut query = Self::build_fetch_query(
                        &self.config.schema,
                        table,
                        &column_names,
                        options.incremental_key.as_deref(),
                        options.last_value.as_deref(),
                        BATCH_SIZE,
                        offset,
                    );

                    if query.contains(" WHERE ") {
                        query = query.replace(" ORDER BY ", &format!(" AND ({}) ORDER BY ", predicate_sql));
                    } else if query.contains(" ORDER BY ") {
                        query = query.replace(" ORDER BY ", &format!(" WHERE ({}) ORDER BY ", predicate_sql));
                    } else {
                        query = query.replace(" LIMIT ", &format!(" WHERE ({}) LIMIT ", predicate_sql));
                    }

                    let rows: Vec<sqlx::postgres::PgRow> = sqlx::query(&query)
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

        // Simple query to validate connection
        sqlx::query("SELECT 1")
            .fetch_one(&pool)
            .await
            .map_err(|e| ConnectorError::Authentication(format!("Failed to validate Redshift credentials: {}", e)))?;

        tracing::debug!(
            host = %self.config.host,
            database = %self.config.database,
            "Validated Redshift credentials"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = RedshiftConfig::new(
            "my-cluster.xxxx.us-east-1.redshift.amazonaws.com",
            "mydb",
            "admin",
            "password123",
        )
        .with_port(5439)
        .with_schema("analytics")
        .with_ssl_mode(SslMode::Require);

        assert_eq!(config.host, "my-cluster.xxxx.us-east-1.redshift.amazonaws.com");
        assert_eq!(config.port, 5439);
        assert_eq!(config.database, "mydb");
        assert_eq!(config.schema, "analytics");
        assert_eq!(config.ssl_mode, SslMode::Require);
    }

    #[test]
    fn test_connection_string() {
        let config = RedshiftConfig::new("host.redshift.amazonaws.com", "db", "user", "pass");
        let conn_str = config.connection_string();
        
        assert!(conn_str.starts_with("postgresql://"));
        assert!(conn_str.contains("host.redshift.amazonaws.com"));
        assert!(conn_str.contains(":5439/"));
        assert!(conn_str.contains("sslmode=require"));
    }

    #[test]
    fn test_type_mapping() {
        assert!(matches!(
            RedshiftConnector::redshift_type_to_column_type("bigint"),
            ColumnType::Int64
        ));
        assert!(matches!(
            RedshiftConnector::redshift_type_to_column_type("double precision"),
            ColumnType::Float64
        ));
        assert!(matches!(
            RedshiftConnector::redshift_type_to_column_type("varchar"),
            ColumnType::String
        ));
        assert!(matches!(
            RedshiftConnector::redshift_type_to_column_type("super"),
            ColumnType::Json
        ));
        assert!(matches!(
            RedshiftConnector::redshift_type_to_column_type("timestamp"),
            ColumnType::Timestamp
        ));
    }

    #[test]
    fn test_valid_column_names() {
        assert!(RedshiftConnector::is_valid_column_name("my_column"));
        assert!(RedshiftConnector::is_valid_column_name("Column1"));
        assert!(RedshiftConnector::is_valid_column_name("_private"));
        assert!(!RedshiftConnector::is_valid_column_name("123column"));
        assert!(!RedshiftConnector::is_valid_column_name(""));
        assert!(!RedshiftConnector::is_valid_column_name("column-name"));
    }

    #[test]
    fn test_escape_string() {
        assert_eq!(RedshiftConnector::escape_string("hello"), "hello");
        assert_eq!(RedshiftConnector::escape_string("it's"), "it''s");
        assert_eq!(RedshiftConnector::escape_string("back\\slash"), "back\\\\slash");
    }
}
