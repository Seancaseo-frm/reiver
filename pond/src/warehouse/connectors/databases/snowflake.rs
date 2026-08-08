//! Snowflake connector for the data warehouse.
//!
//! Syncs Snowflake tables to the warehouse with automatic schema discovery.
//!
//! # Features
//!
//! - Automatic schema discovery from Snowflake INFORMATION_SCHEMA
//! - Incremental sync support with configurable key columns
//! - Session-based connection management
//! - Batched data fetching to prevent memory issues
//!
//! # Connection
//!
//! Uses the snowflake-connector-rs crate which connects via Snowflake's SQL API.
//! Authentication supports password and key-pair methods.

use std::pin::Pin;

use super::super::{Connector, ConnectorError, ConnectorResult, FetchOptions, RecordBatchStream, TableInfo};
use crate::warehouse::query::predicate_pushdown::{predicate_to_sql, SqlDialect};
use crate::warehouse::types::{ColumnSchema, ColumnType, SourceType, TableSchema};
use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use snowflake_connector_rs::{SnowflakeAuthMethod, SnowflakeClient, SnowflakeClientConfig, SnowflakeRow};
use std::sync::Arc;
use std::time::Duration;

/// Maximum rows to fetch per batch to prevent memory issues.
const BATCH_SIZE: i64 = 10_000;

/// Default connection timeout in seconds.
const DEFAULT_TIMEOUT_SECS: u64 = 60;

/// Snowflake connector configuration.
#[derive(Debug, Clone)]
pub struct SnowflakeConfig {
    /// Snowflake account identifier (e.g., xy12345.us-east-1.aws)
    pub account: String,
    /// Compute warehouse name
    pub warehouse: String,
    /// Database name
    pub database: String,
    /// Schema name (default: "PUBLIC")
    pub schema: String,
    /// Username for authentication
    pub username: String,
    /// Password for authentication
    pub password: String,
    /// Optional role to use
    pub role: Option<String>,
    /// Tables to sync (empty = all tables)
    pub tables: Vec<String>,
    /// Connection timeout in seconds
    pub timeout_secs: u64,
}

impl SnowflakeConfig {
    /// Create a new Snowflake configuration.
    pub fn new(
        account: impl Into<String>,
        warehouse: impl Into<String>,
        database: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            account: account.into(),
            warehouse: warehouse.into(),
            database: database.into(),
            schema: "PUBLIC".to_string(),
            username: username.into(),
            password: password.into(),
            role: None,
            tables: Vec::new(),
            timeout_secs: DEFAULT_TIMEOUT_SECS,
        }
    }

    /// Set the schema.
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = schema.into();
        self
    }

    /// Set the role.
    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.role = Some(role.into());
        self
    }

    /// Set specific tables to sync.
    pub fn with_tables(mut self, tables: Vec<String>) -> Self {
        self.tables = tables;
        self
    }

    /// Set the connection timeout.
    pub fn with_timeout(mut self, timeout_secs: u64) -> Self {
        self.timeout_secs = timeout_secs;
        self
    }
}

/// Snowflake data source connector.
///
/// Production-ready connector with session management and incremental sync support.
pub struct SnowflakeConnector {
    config: SnowflakeConfig,
}

/// Parameters for building a Snowflake data fetch query.
struct SfFetchQueryParams<'a> {
    database: &'a str,
    schema: &'a str,
    table: &'a str,
    columns: &'a [String],
    incremental_key: Option<&'a str>,
    last_value: Option<&'a str>,
    limit: i64,
    offset: i64,
}

impl SnowflakeConnector {
    /// Create a new Snowflake connector.
    pub fn new(config: SnowflakeConfig) -> Self {
        Self { config }
    }

    /// Create a Snowflake session.
    async fn create_session(&self) -> ConnectorResult<snowflake_connector_rs::SnowflakeSession> {
        let client_config = SnowflakeClientConfig {
            account: self.config.account.clone(),
            role: self.config.role.clone(),
            warehouse: Some(self.config.warehouse.clone()),
            database: Some(self.config.database.clone()),
            schema: Some(self.config.schema.clone()),
            timeout: Some(Duration::from_secs(self.config.timeout_secs)),
        };

        let client = SnowflakeClient::new(
            &self.config.username,
            SnowflakeAuthMethod::Password(self.config.password.clone()),
            client_config,
        )
        .map_err(|e| ConnectorError::Config(format!("Failed to create Snowflake client: {}", e)))?;

        let session = client
            .create_session()
            .await
            .map_err(|e| ConnectorError::Authentication(format!("Failed to create Snowflake session: {}", e)))?;

        Ok(session)
    }

    /// Map Snowflake type to warehouse column type.
    fn snowflake_type_to_column_type(sf_type: &str) -> ColumnType {
        let sf_type_upper = sf_type.to_uppercase();
        
        // Handle types with precision like NUMBER(38,0), VARCHAR(16777216)
        let base_type = sf_type_upper
            .split('(')
            .next()
            .unwrap_or(&sf_type_upper)
            .trim();

        match base_type {
            // Numeric types
            "NUMBER" | "DECIMAL" | "NUMERIC" => ColumnType::Decimal,
            "INT" | "INTEGER" | "BIGINT" | "SMALLINT" | "TINYINT" | "BYTEINT" => ColumnType::Int64,
            "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" | "REAL" => {
                ColumnType::Float64
            }

            // Boolean
            "BOOLEAN" => ColumnType::Boolean,

            // String types
            "VARCHAR" | "STRING" | "TEXT" | "CHAR" | "CHARACTER" => ColumnType::String,

            // Date/Time types
            "DATE" => ColumnType::Date,
            "TIMESTAMP" | "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" | "TIMESTAMP_TZ" | "DATETIME" => {
                ColumnType::Timestamp
            }
            "TIME" => ColumnType::String,

            // Semi-structured types (store as JSON strings)
            "VARIANT" | "OBJECT" | "ARRAY" => ColumnType::Json,

            // Binary types
            "BINARY" | "VARBINARY" => ColumnType::String,

            // Geospatial types
            "GEOGRAPHY" | "GEOMETRY" => ColumnType::String,

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

    /// Escape a string value for use in Snowflake SQL.
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
        if name.is_empty() || name.len() > 255 {
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
    fn build_fetch_query(params: &SfFetchQueryParams) -> String {
        let columns_str = if params.columns.is_empty() {
            "*".to_string()
        } else {
            params.columns
                .iter()
                .map(|c| format!("\"{}\"", c))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let mut query = format!(
            "SELECT {} FROM \"{}\".\"{}\".\"{}\""  ,
            columns_str, params.database, params.schema, params.table
        );

        // Add incremental filter if provided
        if let (Some(key), Some(value)) = (params.incremental_key, params.last_value) {
            if Self::is_valid_column_name(key) {
                let escaped_value = Self::escape_string(value);
                query.push_str(&format!(" WHERE \"{}\" > '{}'", key, escaped_value));
            }
        }

        // Add ordering for consistent pagination
        if let Some(key) = params.incremental_key {
            if Self::is_valid_column_name(key) {
                query.push_str(&format!(" ORDER BY \"{}\" ASC", key));
            }
        }

        // Add pagination
        query.push_str(&format!(" LIMIT {} OFFSET {}", params.limit, params.offset));

        query
    }

    /// Convert Snowflake rows to Arrow RecordBatch.
    fn rows_to_record_batch(
        &self,
        rows: &[SnowflakeRow],
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<RecordBatch> {
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.columns.len());

        for col_schema in &schema.columns {
            let array: ArrayRef = match col_schema.data_type {
                ColumnType::Int32 | ColumnType::Int64 => {
                    let values: Vec<Option<i64>> = rows
                        .iter()
                        .map(|row| row.get::<i64>(&col_schema.name).ok())
                        .collect();
                    Arc::new(Int64Array::from(values))
                }
                ColumnType::Float64 | ColumnType::Decimal => {
                    let values: Vec<Option<f64>> = rows
                        .iter()
                        .map(|row| row.get::<f64>(&col_schema.name).ok())
                        .collect();
                    Arc::new(Float64Array::from(values))
                }
                ColumnType::Boolean => {
                    let values: Vec<Option<bool>> = rows
                        .iter()
                        .map(|row| row.get::<bool>(&col_schema.name).ok())
                        .collect();
                    Arc::new(BooleanArray::from(values))
                }
                _ => {
                    // Default to string for all other types
                    let values: Vec<Option<String>> = rows
                        .iter()
                        .map(|row| {
                            row.get::<String>(&col_schema.name)
                                .ok()
                                .or_else(|| {
                                    row.get::<i64>(&col_schema.name)
                                        .ok()
                                        .map(|v| v.to_string())
                                })
                                .or_else(|| {
                                    row.get::<f64>(&col_schema.name)
                                        .ok()
                                        .map(|v| v.to_string())
                                })
                        })
                        .collect();
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
impl Connector for SnowflakeConnector {
    fn source_type(&self) -> SourceType {
        SourceType::Snowflake
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        let session = self.create_session().await?;

        // Query INFORMATION_SCHEMA for tables
        let query = format!(
            r#"
            SELECT TABLE_NAME
            FROM "{}"."INFORMATION_SCHEMA"."TABLES"
            WHERE TABLE_SCHEMA = '{}'
              AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
            "#,
            self.config.database, self.config.schema
        );

        let rows = session
            .query(query.as_str())
            .await
            .map_err(|e| ConnectorError::Internal(format!("Failed to list tables: {}", e)))?;

        let mut table_infos = Vec::new();
        for row in &rows {
            let table_name: String = row
                .get("TABLE_NAME")
                .map_err(|e| ConnectorError::Internal(format!("Failed to get table name: {}", e)))?;

            // Skip if tables filter is set and this table isn't in it
            if !self.config.tables.is_empty() && !self.config.tables.contains(&table_name) {
                continue;
            }

            // Get schema for this table
            let table_schema = match self.get_schema(&table_name).await {
                Ok(schema) => schema,
                Err(e) => {
                    tracing::warn!(
                        table = %table_name,
                        error = %e,
                        "Failed to get schema for Snowflake table, skipping"
                    );
                    continue;
                }
            };

            // Determine incremental key
            let incremental_key = table_schema
                .columns
                .iter()
                .find(|c| {
                    let name_lower = c.name.to_lowercase();
                    matches!(
                        name_lower.as_str(),
                        "updated_at" | "modified_at" | "last_modified" | "_sdc_received_at"
                    )
                })
                .map(|c| c.name.clone());

            table_infos.push(TableInfo {
                name: table_name,
                schema: table_schema,
                supports_incremental: incremental_key.is_some(),
                incremental_key,
                estimated_rows: None, // Snowflake doesn't provide easy row estimates
                primary_key_columns: Vec::new(),
            });
        }

        tracing::debug!(
            database = %self.config.database,
            schema = %self.config.schema,
            table_count = table_infos.len(),
            "Listed Snowflake tables"
        );

        Ok(table_infos)
    }

    async fn get_schema(&self, table: &str) -> ConnectorResult<TableSchema> {
        let session = self.create_session().await?;

        // Query INFORMATION_SCHEMA for column definitions
        let query = format!(
            r#"
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE
            FROM "{}"."INFORMATION_SCHEMA"."COLUMNS"
            WHERE TABLE_SCHEMA = '{}'
              AND TABLE_NAME = '{}'
            ORDER BY ORDINAL_POSITION
            "#,
            self.config.database, self.config.schema, table
        );

        let rows = session
            .query(query.as_str())
            .await
            .map_err(|e| ConnectorError::Internal(format!("Failed to get schema for {}: {}", table, e)))?;

        if rows.is_empty() {
            return Err(ConnectorError::Internal(format!(
                "Table {}.{}.{} not found or has no columns",
                self.config.database, self.config.schema, table
            )));
        }

        let columns: Vec<ColumnSchema> = rows
            .iter()
            .filter_map(|row| {
                let name: String = row.get("COLUMN_NAME").ok()?;
                let data_type: String = row.get("DATA_TYPE").ok()?;
                let is_nullable: String = row.get("IS_NULLABLE").ok().unwrap_or_else(|| "YES".to_string());

                let col_type = Self::snowflake_type_to_column_type(&data_type);
                let timezone = if matches!(col_type, ColumnType::Timestamp) {
                    Some("UTC".to_string())
                } else {
                    None
                };

                Some(ColumnSchema {
                    name,
                    data_type: col_type,
                    nullable: is_nullable.to_uppercase() == "YES",
                    description: None,
                    timezone,
                })
            })
            .collect();

        tracing::debug!(
            database = %self.config.database,
            schema = %self.config.schema,
            table = %table,
            column_count = columns.len(),
            "Retrieved Snowflake table schema"
        );

        Ok(TableSchema { columns })
    }

    async fn fetch_table(
        &self,
        table: &str,
        incremental_key: Option<&str>,
        last_value: Option<&str>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let session = self.create_session().await?;

        // Get schema to know column types
        let schema = self.get_schema(table).await?;
        let arrow_schema = Arc::new(Self::to_arrow_schema(&schema));
        let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();

        let mut all_batches = Vec::new();
        let mut offset = 0i64;
        let mut total_rows = 0u64;

        loop {
            let query = Self::build_fetch_query(&SfFetchQueryParams {
                database: &self.config.database,
                schema: &self.config.schema,
                table,
                columns: &column_names,
                incremental_key,
                last_value,
                limit: BATCH_SIZE,
                offset,
            });

            // Fetch a batch of rows
            let rows = session
                .query(query.as_str())
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
            schema = %self.config.schema,
            table = %table,
            rows = total_rows,
            batches = all_batches.len(),
            incremental = incremental_key.is_some(),
            "Fetched Snowflake table data"
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
                let session = self.create_session().await?;
                let schema = self.get_schema(table).await?;
                let arrow_schema = Arc::new(Self::to_arrow_schema(&schema));
                let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();

                let predicate_sql: String = options
                    .predicates
                    .iter()
                    .map(|p| predicate_to_sql(p, SqlDialect::Snowflake))
                    .collect::<Vec<_>>()
                    .join(" AND ");

                let mut all_batches = Vec::new();
                let mut offset = 0i64;

                loop {
                    let mut query = Self::build_fetch_query(&SfFetchQueryParams {
                        database: &self.config.database,
                        schema: &self.config.schema,
                        table,
                        columns: &column_names,
                        incremental_key: options.incremental_key.as_deref(),
                        last_value: options.last_value.as_deref(),
                        limit: BATCH_SIZE,
                        offset,
                    });

                    if query.contains(" WHERE ") {
                        query = query.replace(" ORDER BY ", &format!(" AND ({}) ORDER BY ", predicate_sql));
                    } else if query.contains(" ORDER BY ") {
                        query = query.replace(" ORDER BY ", &format!(" WHERE ({}) ORDER BY ", predicate_sql));
                    } else {
                        query = query.replace(" LIMIT ", &format!(" WHERE ({}) LIMIT ", predicate_sql));
                    }

                    let rows = session
                        .query(query.as_str())
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
        let session = self.create_session().await?;

        // Simple query to validate connection
        session
            .query("SELECT 1")
            .await
            .map_err(|e| ConnectorError::Authentication(format!("Failed to validate Snowflake credentials: {}", e)))?;

        tracing::debug!(
            account = %self.config.account,
            database = %self.config.database,
            warehouse = %self.config.warehouse,
            "Validated Snowflake credentials"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = SnowflakeConfig::new(
            "xy12345.us-east-1.aws",
            "COMPUTE_WH",
            "ANALYTICS_DB",
            "admin",
            "password123",
        )
        .with_schema("SALES")
        .with_role("ANALYST");

        assert_eq!(config.account, "xy12345.us-east-1.aws");
        assert_eq!(config.warehouse, "COMPUTE_WH");
        assert_eq!(config.database, "ANALYTICS_DB");
        assert_eq!(config.schema, "SALES");
        assert_eq!(config.role, Some("ANALYST".to_string()));
    }

    #[test]
    fn test_type_mapping() {
        assert!(matches!(
            SnowflakeConnector::snowflake_type_to_column_type("NUMBER(38,0)"),
            ColumnType::Decimal
        ));
        assert!(matches!(
            SnowflakeConnector::snowflake_type_to_column_type("INTEGER"),
            ColumnType::Int64
        ));
        assert!(matches!(
            SnowflakeConnector::snowflake_type_to_column_type("FLOAT"),
            ColumnType::Float64
        ));
        assert!(matches!(
            SnowflakeConnector::snowflake_type_to_column_type("VARCHAR(16777216)"),
            ColumnType::String
        ));
        assert!(matches!(
            SnowflakeConnector::snowflake_type_to_column_type("VARIANT"),
            ColumnType::Json
        ));
        assert!(matches!(
            SnowflakeConnector::snowflake_type_to_column_type("TIMESTAMP_NTZ"),
            ColumnType::Timestamp
        ));
    }

    #[test]
    fn test_valid_column_names() {
        assert!(SnowflakeConnector::is_valid_column_name("my_column"));
        assert!(SnowflakeConnector::is_valid_column_name("Column1"));
        assert!(SnowflakeConnector::is_valid_column_name("_private"));
        assert!(!SnowflakeConnector::is_valid_column_name("123column"));
        assert!(!SnowflakeConnector::is_valid_column_name(""));
        assert!(!SnowflakeConnector::is_valid_column_name("column-name"));
    }

    #[test]
    fn test_escape_string() {
        assert_eq!(SnowflakeConnector::escape_string("hello"), "hello");
        assert_eq!(SnowflakeConnector::escape_string("it's"), "it''s");
        assert_eq!(SnowflakeConnector::escape_string("back\\slash"), "back\\\\slash");
    }
}
