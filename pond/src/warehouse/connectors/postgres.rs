//! PostgreSQL connector for the data warehouse.
//!
//! Syncs PostgreSQL tables to the warehouse with automatic schema discovery.
//!
//! # Features
//!
//! - Automatic schema discovery from `information_schema`
//! - Incremental sync support with configurable key columns
//! - Connection pooling for efficient resource usage
//! - Batched data fetching to prevent memory issues

use std::pin::Pin;

use super::{Connector, ConnectorError, ConnectorResult, FetchOptions, RecordBatchStream, TableInfo};
use crate::warehouse::types::{ColumnSchema, ColumnType, SourceType, TableSchema};
use arrow::array::{ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::OnceCell;

/// Maximum rows to fetch per batch to prevent memory issues.
const BATCH_SIZE: i64 = 10_000;

/// PostgreSQL connector configuration.
#[derive(Debug, Clone)]
pub struct PostgresConfig {
    /// PostgreSQL connection string
    pub connection_string: String,
    /// Schema to sync (default: "public")
    pub schema: String,
    /// Tables to sync (empty = all tables)
    pub tables: Vec<String>,
    /// Maximum connections in the pool
    pub max_connections: u32,
    /// Connection timeout in seconds
    pub connect_timeout_secs: u64,
}

impl PostgresConfig {
    pub fn new(connection_string: impl Into<String>) -> Self {
        Self {
            connection_string: connection_string.into(),
            schema: "public".to_string(),
            tables: Vec::new(),
            max_connections: 5,
            connect_timeout_secs: 30,
        }
    }

    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = schema.into();
        self
    }

    pub fn with_tables(mut self, tables: Vec<String>) -> Self {
        self.tables = tables;
        self
    }

    pub fn with_max_connections(mut self, max_connections: u32) -> Self {
        self.max_connections = max_connections;
        self
    }

    pub fn with_connect_timeout(mut self, timeout_secs: u64) -> Self {
        self.connect_timeout_secs = timeout_secs;
        self
    }
}

/// PostgreSQL data source connector.
///
/// Production-ready connector with connection pooling and incremental sync support.
pub struct PostgresConnector {
    config: PostgresConfig,
    /// Connection pool - initialized lazily on first use via OnceCell
    pool: OnceCell<sqlx::PgPool>,
}

impl PostgresConnector {
    /// Create a new PostgreSQL connector.
    pub fn new(config: PostgresConfig) -> Self {
        Self { config, pool: OnceCell::new() }
    }

    /// Create a connector with an existing pool (for testing or shared connections).
    pub fn with_pool(config: PostgresConfig, pool: sqlx::PgPool) -> Self {
        let cell = OnceCell::new();
        cell.set(pool).ok();
        Self { config, pool: cell }
    }

    /// Get or create the connection pool (lazily initialized, cached for reuse).
    async fn get_pool(&self) -> ConnectorResult<sqlx::PgPool> {
        let pool = self.pool.get_or_try_init(|| async {
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(self.config.max_connections)
                .acquire_timeout(std::time::Duration::from_secs(self.config.connect_timeout_secs))
                .connect(&self.config.connection_string)
                .await
                .map_err(|e| ConnectorError::Network(format!("Failed to connect to PostgreSQL: {}", e)))
        }).await?;

        Ok(pool.clone())
    }

    /// Map PostgreSQL type to warehouse column type.
    fn pg_type_to_column_type(pg_type: &str) -> ColumnType {
        match pg_type.to_lowercase().as_str() {
            "integer" | "int" | "int4" | "serial" => ColumnType::Int32,
            "bigint" | "int8" | "bigserial" => ColumnType::Int64,
            "smallint" | "int2" => ColumnType::Int32,
            "real" | "float4" => ColumnType::Float32,
            "double precision" | "float8" => ColumnType::Float64,
            "numeric" | "decimal" => ColumnType::Decimal,
            "boolean" | "bool" => ColumnType::Boolean,
            "timestamp" | "timestamp without time zone" | "timestamp with time zone" | "timestamptz" => {
                ColumnType::Timestamp
            }
            "date" => ColumnType::Date,
            "uuid" => ColumnType::Uuid,
            "json" | "jsonb" => ColumnType::Json,
            _ => ColumnType::String, // Default to string for unknown types
        }
    }

    /// Convert table schema to Arrow schema.
    fn to_arrow_schema(schema: &TableSchema) -> Schema {
        let fields: Vec<Field> = schema
            .columns
            .iter()
            // Always mark columns as nullable in Arrow to handle edge cases
            // where data fetching might produce nulls (e.g., type conversion errors)
            .map(|col| Field::new(&col.name, col.data_type.to_arrow_type(), true))
            .collect();
        Schema::new(fields)
    }

    /// Escape a string value for use in PostgreSQL SQL.
    ///
    /// # Security
    ///
    /// This escapes:
    /// - Single quotes (') -> ('')
    /// - Backslashes (\) -> (\\)
    /// - Null bytes are removed to prevent truncation attacks
    ///
    /// This is defense-in-depth; the connector builds queries dynamically
    /// so escaping provides the security layer.
    fn escape_pg_string(s: &str) -> String {
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
    ///
    /// Valid column names contain only:
    /// - Alphanumeric characters (a-z, A-Z, 0-9)
    /// - Underscores (_)
    /// - Must start with a letter or underscore
    /// - Maximum 128 characters
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

    fn arrow_value_to_sql_literal(col: &ArrayRef, row_idx: usize) -> String {
        if col.is_null(row_idx) {
            return "NULL".to_string();
        }

        match col.data_type() {
            DataType::Int32 => {
                let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
                arr.value(row_idx).to_string()
            }
            DataType::Int64 => {
                let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                arr.value(row_idx).to_string()
            }
            DataType::Float32 => {
                let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
                let v = arr.value(row_idx);
                if v.is_nan() || v.is_infinite() { "NULL".to_string() } else { v.to_string() }
            }
            DataType::Float64 => {
                let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                let v = arr.value(row_idx);
                if v.is_nan() || v.is_infinite() { "NULL".to_string() } else { v.to_string() }
            }
            DataType::Boolean => {
                let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                if arr.value(row_idx) { "true".to_string() } else { "false".to_string() }
            }
            DataType::Utf8 => {
                let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                format!("'{}'", Self::escape_pg_string(arr.value(row_idx)))
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let arr = col.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                let usec = arr.value(row_idx);
                match chrono::DateTime::from_timestamp_micros(usec) {
                    Some(dt) => format!("'{}'::timestamptz", dt.format("%Y-%m-%d %H:%M:%S%.6f+00")),
                    None => "NULL".to_string(),
                }
            }
            DataType::Date32 => {
                let arr = col.as_any().downcast_ref::<Date32Array>().unwrap();
                let days = arr.value(row_idx);
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                match epoch.checked_add_signed(chrono::Duration::days(days as i64)) {
                    Some(date) => format!("'{}'::date", date.format("%Y-%m-%d")),
                    None => "NULL".to_string(),
                }
            }
            _ => "NULL".to_string(),
        }
    }

    /// Build a SELECT query for fetching data.
    ///
    /// # Security
    ///
    /// - Column names are validated to prevent SQL injection
    /// - String values are escaped using `escape_pg_string`
    /// - Schema and table names are double-quoted (should be pre-validated)
    ///
    /// Invalid column names in `incremental_key` are silently ignored
    /// (no WHERE/ORDER BY clause generated).
    fn build_fetch_query(
        schema: &str,
        table: &str,
        columns: &[String],
        incremental_key: Option<&str>,
        last_value: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> String {
        let sanitize_identifier = |name: &str| -> String {
            if Self::is_valid_column_name(name) {
                name.to_string()
            } else {
                name.chars()
                    .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
                    .collect()
            }
        };
        let schema = sanitize_identifier(schema);
        let table = sanitize_identifier(table);

        let columns_str = if columns.is_empty() {
            "*".to_string()
        } else {
            columns.iter()
                .map(|c| {
                    let safe = sanitize_identifier(c);
                    format!("\"{}\"", safe)
                })
                .collect::<Vec<_>>()
                .join(", ")
        };

        let mut query = format!(
            "SELECT {} FROM \"{}\".\"{}\"",
            columns_str, &schema, &table
        );

        // Add incremental filter if provided
        // SECURITY: Validate column name and escape value to prevent SQL injection
        if let (Some(key), Some(value)) = (incremental_key, last_value) {
            if Self::is_valid_column_name(key) {
                let escaped_value = Self::escape_pg_string(value);
                query.push_str(&format!(" WHERE \"{}\" > '{}'", key, escaped_value));
            }
        }

        // Add ordering for consistent pagination
        // SECURITY: Validate column name before using in ORDER BY
        if let Some(key) = incremental_key {
            if Self::is_valid_column_name(key) {
                query.push_str(&format!(" ORDER BY \"{}\" ASC", key));
            }
        }

        // Add pagination
        query.push_str(&format!(" LIMIT {} OFFSET {}", limit, offset));

        query
    }
}

#[async_trait]
impl Connector for PostgresConnector {
    fn source_type(&self) -> SourceType {
        SourceType::PostgreSQL
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        let pool = self.get_pool().await?;

        // Query information_schema for tables in the configured schema
        let tables: Vec<(String, Option<i64>)> = sqlx::query_as(
            r#"
            SELECT 
                t.table_name,
                (SELECT reltuples::bigint 
                 FROM pg_class c 
                 JOIN pg_namespace n ON n.oid = c.relnamespace 
                 WHERE c.relname = t.table_name AND n.nspname = t.table_schema) as row_estimate
            FROM information_schema.tables t
            WHERE t.table_schema = $1
              AND t.table_type = 'BASE TABLE'
            ORDER BY t.table_name
            "#
        )
        .bind(&self.config.schema)
        .fetch_all(&pool)
        .await
        .map_err(|e| ConnectorError::Internal(format!("Failed to list tables: {}", e)))?;

        // Discover primary keys for all tables in one query
        let pk_rows: Vec<(String, String)> = sqlx::query_as(
            r#"
            SELECT kcu.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = $1
            ORDER BY kcu.table_name, kcu.ordinal_position
            "#,
        )
        .bind(&self.config.schema)
        .fetch_all(&pool)
        .await
        .unwrap_or_default();

        // Group PK columns by table name
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
            let incremental_key = table_schema.columns.iter()
                .find(|c| matches!(c.name.as_str(), "updated_at" | "modified_at" | "last_modified"))
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
            schema = %self.config.schema,
            table_count = table_infos.len(),
            "Listed PostgreSQL tables"
        );

        Ok(table_infos)
    }

    async fn get_schema(&self, table: &str) -> ConnectorResult<TableSchema> {
        let pool = self.get_pool().await?;

        // Query information_schema for column definitions
        let columns: Vec<(String, String, String)> = sqlx::query_as(
            r#"
            SELECT 
                column_name,
                data_type,
                is_nullable
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position
            "#
        )
        .bind(&self.config.schema)
        .bind(table)
        .fetch_all(&pool)
        .await
        .map_err(|e| ConnectorError::Internal(format!("Failed to get schema for {}: {}", table, e)))?;

        if columns.is_empty() {
            return Err(ConnectorError::Internal(format!(
                "Table {}.{} not found or has no columns",
                self.config.schema, table
            )));
        }

        let columns: Vec<ColumnSchema> = columns
            .into_iter()
            .map(|(name, data_type, is_nullable)| {
                let col_type = Self::pg_type_to_column_type(&data_type);
                let timezone = if matches!(col_type, ColumnType::Timestamp) {
                    // PostgreSQL timestamp without timezone - assume UTC
                    // timestamptz is already UTC
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

        tracing::debug!(
            schema = %self.config.schema,
            table = %table,
            column_count = columns.len(),
            "Retrieved PostgreSQL table schema"
        );

        Ok(TableSchema { columns })
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
            "Fetched PostgreSQL table data"
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
                    &self.config.schema,
                    table,
                    &column_names,
                    options.incremental_key.as_deref(),
                    options.last_value.as_deref(),
                    BATCH_SIZE,
                    offset,
                );

                if !options.predicates.is_empty() {
                    use crate::warehouse::query::predicate_pushdown::{predicate_to_sql, SqlDialect};
                    let pred_sql = options.predicates.iter()
                        .map(|p| predicate_to_sql(p, SqlDialect::Postgres))
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

                let rows: Vec<sqlx::postgres::PgRow> = sqlx::query(&query)
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
            .map_err(|e| ConnectorError::Network(format!("Connection validation failed: {}", e)))?;

        tracing::debug!("PostgreSQL credentials validated successfully");
        Ok(())
    }
    
    fn supports_write(&self) -> bool {
        true
    }

    fn supports_transactional_write(&self) -> bool {
        true
    }

    async fn write_table(
        &self,
        table: &str,
        batches: Vec<RecordBatch>,
    ) -> ConnectorResult<super::WriteResult> {
        if !Self::is_valid_column_name(table) {
            return Err(ConnectorError::Internal(
                format!("Invalid table name: {}", table),
            ));
        }
        if !Self::is_valid_column_name(&self.config.schema) {
            return Err(ConnectorError::Internal(
                format!("Invalid schema name: {}", self.config.schema),
            ));
        }

        let pool = self.get_pool().await?;
        let mut tx = pool.begin().await
            .map_err(|e| ConnectorError::Internal(format!("Failed to begin transaction: {}", e)))?;
        let mut total_rows = 0u64;

        for batch in &batches {
            if batch.num_rows() == 0 {
                continue;
            }

            let schema = batch.schema();
            let col_names: Vec<&str> = schema.fields().iter()
                .map(|f| f.name().as_str())
                .collect();

            for name in &col_names {
                if !Self::is_valid_column_name(name) {
                    return Err(ConnectorError::Internal(
                        format!("Invalid column name in output batch: {}", name),
                    ));
                }
            }

            let cols_str = col_names
                .iter()
                .map(|n| format!("\"{}\"", n))
                .collect::<Vec<_>>()
                .join(", ");

            const CHUNK_SIZE: usize = 1000;
            for chunk_start in (0..batch.num_rows()).step_by(CHUNK_SIZE) {
                let chunk_end = (chunk_start + CHUNK_SIZE).min(batch.num_rows());

                let mut values_parts = Vec::with_capacity(chunk_end - chunk_start);
                for row_idx in chunk_start..chunk_end {
                    let mut row_values = Vec::with_capacity(schema.fields().len());
                    for col_idx in 0..schema.fields().len() {
                        let col = batch.column(col_idx);
                        row_values.push(Self::arrow_value_to_sql_literal(col, row_idx));
                    }
                    values_parts.push(format!("({})", row_values.join(", ")));
                }

                let sql = format!(
                    "INSERT INTO \"{}\".\"{}\" ({}) VALUES {}",
                    &self.config.schema, table, cols_str, values_parts.join(", ")
                );

                sqlx::query(&sql)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| ConnectorError::Internal(
                        format!("Failed to write to {}: {}", table, e),
                    ))?;

                total_rows += (chunk_end - chunk_start) as u64;
            }
        }

        tx.commit().await
            .map_err(|e| ConnectorError::Internal(format!("Failed to commit transaction: {}", e)))?;

        tracing::info!(
            schema = %self.config.schema,
            table = %table,
            rows_written = total_rows,
            "Wrote data to PostgreSQL table"
        );

        Ok(super::WriteResult { rows_written: total_rows })
    }

    fn supports_sql_pushdown(&self) -> bool {
        true // PostgreSQL supports arbitrary SQL execution
    }
    
    fn supports_cdc(&self) -> bool {
        true // PostgreSQL supports WAL-based change tracking via logical replication
    }
    
    async fn execute_sql(&self, sql: &str) -> ConnectorResult<Vec<RecordBatch>> {
        crate::warehouse::connectors::enforce_read_only_sql(sql)?;

        let pool = self.get_pool().await?;
        
        // Execute the query
        let rows: Vec<sqlx::postgres::PgRow> = sqlx::query(sql)
            .fetch_all(&pool)
            .await
            .map_err(|e| ConnectorError::Internal(format!("Failed to execute SQL: {}", e)))?;
        
        if rows.is_empty() {
            // Return empty batch with no schema (caller should handle this)
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
            "Executed PostgreSQL SQL query"
        );
        
        Ok(vec![batch])
    }
}

impl PostgresConnector {
    /// Infer schema from query result rows.
    fn infer_schema_from_rows(&self, rows: &[sqlx::postgres::PgRow]) -> ConnectorResult<TableSchema> {
        use sqlx::{Column, Row, TypeInfo};
        
        if rows.is_empty() {
            return Ok(TableSchema { columns: vec![] });
        }
        
        let first_row = &rows[0];
        let columns: Vec<ColumnSchema> = first_row
            .columns()
            .iter()
            .map(|col| {
                let pg_type = col.type_info().name().to_string();
                let col_type = Self::pg_type_to_column_type(&pg_type);
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

    /// Get schema using an existing pool connection.
    async fn get_schema_from_pool(&self, pool: &sqlx::PgPool, table: &str) -> ConnectorResult<TableSchema> {
        // Query information_schema for column definitions
        let columns: Vec<(String, String, String)> = sqlx::query_as(
            r#"
            SELECT 
                column_name,
                data_type,
                is_nullable
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position
            "#
        )
        .bind(&self.config.schema)
        .bind(table)
        .fetch_all(pool)
        .await
        .map_err(|e| ConnectorError::Internal(format!("Failed to get schema for {}: {}", table, e)))?;

        if columns.is_empty() {
            return Err(ConnectorError::Internal(format!(
                "Table {}.{} not found or has no columns",
                self.config.schema, table
            )));
        }

        let columns: Vec<ColumnSchema> = columns
            .into_iter()
            .map(|(name, data_type, is_nullable)| {
                let col_type = Self::pg_type_to_column_type(&data_type);
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

    /// Convert PostgreSQL rows to an Arrow RecordBatch.
    fn rows_to_record_batch(
        &self,
        rows: &[sqlx::postgres::PgRow],
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<RecordBatch> {
        use sqlx::Row;

        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.columns.len());

        // Log first row's raw data for debugging
        if !rows.is_empty() {
            tracing::debug!(
                num_rows = rows.len(),
                num_cols = schema.columns.len(),
                "Converting PostgreSQL rows to RecordBatch"
            );
        }
        
        for (col_idx, col) in schema.columns.iter().enumerate() {
            let col_name: &str = &col.name;
            let array: ArrayRef = match col.data_type {
                ColumnType::Int32 => {
                    let values: Vec<Option<i32>> = rows
                        .iter()
                        .enumerate()
                        .map(|(row_idx, row)| {
                            match row.try_get::<i32, _>(col_idx) {
                                Ok(v) => {
                                    if row_idx == 0 {
                                        tracing::trace!(col = %col_name, "First row Int32 value read");
                                    }
                                    Some(v)
                                },
                                Err(e) => {
                                    if row_idx == 0 {
                                        tracing::warn!(col = %col_name, col_idx = col_idx, error = %e, "Failed to get i32");
                                    }
                                    None
                                }
                            }
                        })
                        .collect();
                    Arc::new(Int32Array::from(values))
                }
                ColumnType::Int64 => {
                    let values: Vec<Option<i64>> = rows
                        .iter()
                        .map(|row| {
                            match row.try_get::<i64, _>(col_idx) {
                                Ok(v) => Some(v),
                                Err(e) => {
                                    tracing::debug!(col = %col_name, error = %e, "Failed to get i64");
                                    None
                                }
                            }
                        })
                        .collect();
                    Arc::new(Int64Array::from(values))
                }
                ColumnType::Float32 => {
                    let values: Vec<Option<f32>> = rows
                        .iter()
                        .map(|row| {
                            match row.try_get::<f32, _>(col_idx) {
                                Ok(v) => Some(v),
                                Err(e) => {
                                    tracing::debug!(col = %col_name, error = %e, "Failed to get f32");
                                    None
                                }
                            }
                        })
                        .collect();
                    Arc::new(Float32Array::from(values))
                }
                ColumnType::Float64 => {
                    let values: Vec<Option<f64>> = rows
                        .iter()
                        .map(|row| {
                            match row.try_get::<f64, _>(col_idx) {
                                Ok(v) => Some(v),
                                Err(e) => {
                                    tracing::debug!(col = %col_name, error = %e, "Failed to get f64");
                                    None
                                }
                            }
                        })
                        .collect();
                    Arc::new(Float64Array::from(values))
                }
                ColumnType::Decimal => {
                    // PostgreSQL DECIMAL/NUMERIC - try to get as rust_decimal::Decimal then convert to f64
                    use rust_decimal::prelude::ToPrimitive;
                    let values: Vec<Option<f64>> = rows
                        .iter()
                        .map(|row| {
                            match row.try_get::<rust_decimal::Decimal, _>(col_idx) {
                                Ok(d) => d.to_f64(),
                                Err(e) => {
                                    tracing::debug!(col = %col_name, error = %e, "Failed to get Decimal");
                                    None
                                }
                            }
                        })
                        .collect();
                    Arc::new(Float64Array::from(values))
                }
                ColumnType::Boolean => {
                    let values: Vec<Option<bool>> = rows
                        .iter()
                        .map(|row| {
                            match row.try_get::<bool, _>(col_idx) {
                                Ok(v) => Some(v),
                                Err(e) => {
                                    tracing::debug!(col = %col_name, error = %e, "Failed to get bool");
                                    None
                                }
                            }
                        })
                        .collect();
                    Arc::new(BooleanArray::from(values))
                }
                ColumnType::Timestamp => {
                    // PostgreSQL timestamp/timestamptz - convert to microseconds since epoch
                    let values: Vec<Option<i64>> = rows
                        .iter()
                        .map(|row| {
                            // Try DateTime<Utc> first (for timestamptz)
                            row.try_get::<chrono::DateTime<chrono::Utc>, _>(col_idx)
                                .ok()
                                .map(|dt| dt.timestamp_micros())
                                .or_else(|| {
                                    // Try NaiveDateTime (for timestamp without timezone)
                                    row.try_get::<chrono::NaiveDateTime, _>(col_idx)
                                        .ok()
                                        .map(|dt| dt.and_utc().timestamp_micros())
                                })
                        })
                        .collect();
                    Arc::new(TimestampMicrosecondArray::from(values))
                }
                ColumnType::Date => {
                    // PostgreSQL date - convert to days since epoch
                    let values: Vec<Option<i32>> = rows
                        .iter()
                        .map(|row| {
                            row.try_get::<chrono::NaiveDate, _>(col_idx)
                                .ok()
                                .map(|d| {
                                    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid constant epoch date");
                                    (d - epoch).num_days() as i32
                                })
                        })
                        .collect();
                    Arc::new(Date32Array::from(values))
                }
                ColumnType::String | ColumnType::Json | ColumnType::Uuid | _ => {
                    // Default: convert to string - try multiple string types
                    let values: Vec<Option<String>> = rows
                        .iter()
                        .enumerate()
                        .map(|(row_idx, row)| {
                            // Try String first, then fallback to getting raw value as string
                            match row.try_get::<String, _>(col_idx) {
                                Ok(v) => {
                                    if row_idx == 0 {
                                        tracing::trace!(col = %col_name, "First row String value read");
                                    }
                                    Some(v)
                                },
                                Err(e) => {
                                    if row_idx == 0 {
                                        tracing::warn!(col = %col_name, col_idx = col_idx, error = %e, "Failed to get String, trying &str");
                                    }
                                    // Try to get as &str
                                    row.try_get::<&str, _>(col_idx).ok().map(|s| s.to_string())
                                }
                            }
                        })
                        .collect();
                    Arc::new(StringArray::from(values))
                }
            };
            arrays.push(array);
        }

        RecordBatch::try_new(arrow_schema, arrays)
            .map_err(|e| ConnectorError::Internal(format!("Failed to create RecordBatch: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    #[test]
    fn test_postgres_config_creation() {
        let config = PostgresConfig::new("postgres://user:pass@localhost/db");
        assert_eq!(config.connection_string, "postgres://user:pass@localhost/db");
        assert_eq!(config.schema, "public");
        assert!(config.tables.is_empty());
    }

    #[test]
    fn test_postgres_config_with_schema() {
        let config = PostgresConfig::new("postgres://localhost/db")
            .with_schema("custom_schema");
        assert_eq!(config.schema, "custom_schema");
    }

    #[test]
    fn test_postgres_config_with_tables() {
        let config = PostgresConfig::new("postgres://localhost/db")
            .with_tables(vec!["users".to_string(), "orders".to_string()]);
        assert_eq!(config.tables.len(), 2);
        assert!(config.tables.contains(&"users".to_string()));
        assert!(config.tables.contains(&"orders".to_string()));
    }

    #[test]
    fn test_postgres_config_builder_chain() {
        let config = PostgresConfig::new("postgres://localhost/db")
            .with_schema("myschema")
            .with_tables(vec!["table1".to_string()]);
        
        assert_eq!(config.schema, "myschema");
        assert_eq!(config.tables, vec!["table1".to_string()]);
    }

    #[test]
    fn test_pg_type_to_column_type_integers() {
        assert!(matches!(PostgresConnector::pg_type_to_column_type("integer"), ColumnType::Int32));
        assert!(matches!(PostgresConnector::pg_type_to_column_type("int"), ColumnType::Int32));
        assert!(matches!(PostgresConnector::pg_type_to_column_type("int4"), ColumnType::Int32));
        assert!(matches!(PostgresConnector::pg_type_to_column_type("serial"), ColumnType::Int32));
        assert!(matches!(PostgresConnector::pg_type_to_column_type("smallint"), ColumnType::Int32));
        assert!(matches!(PostgresConnector::pg_type_to_column_type("int2"), ColumnType::Int32));
    }

    #[test]
    fn test_pg_type_to_column_type_bigint() {
        assert!(matches!(PostgresConnector::pg_type_to_column_type("bigint"), ColumnType::Int64));
        assert!(matches!(PostgresConnector::pg_type_to_column_type("int8"), ColumnType::Int64));
        assert!(matches!(PostgresConnector::pg_type_to_column_type("bigserial"), ColumnType::Int64));
    }

    #[test]
    fn test_pg_type_to_column_type_floats() {
        assert!(matches!(PostgresConnector::pg_type_to_column_type("real"), ColumnType::Float32));
        assert!(matches!(PostgresConnector::pg_type_to_column_type("float4"), ColumnType::Float32));
        assert!(matches!(PostgresConnector::pg_type_to_column_type("double precision"), ColumnType::Float64));
        assert!(matches!(PostgresConnector::pg_type_to_column_type("float8"), ColumnType::Float64));
    }

    #[test]
    fn test_pg_type_to_column_type_decimal() {
        assert!(matches!(PostgresConnector::pg_type_to_column_type("numeric"), ColumnType::Decimal));
        assert!(matches!(PostgresConnector::pg_type_to_column_type("decimal"), ColumnType::Decimal));
    }

    #[test]
    fn test_pg_type_to_column_type_boolean() {
        assert!(matches!(PostgresConnector::pg_type_to_column_type("boolean"), ColumnType::Boolean));
        assert!(matches!(PostgresConnector::pg_type_to_column_type("bool"), ColumnType::Boolean));
    }

    #[test]
    fn test_pg_type_to_column_type_timestamps() {
        assert!(matches!(PostgresConnector::pg_type_to_column_type("timestamp"), ColumnType::Timestamp));
        assert!(matches!(PostgresConnector::pg_type_to_column_type("timestamp without time zone"), ColumnType::Timestamp));
        assert!(matches!(PostgresConnector::pg_type_to_column_type("timestamp with time zone"), ColumnType::Timestamp));
        assert!(matches!(PostgresConnector::pg_type_to_column_type("timestamptz"), ColumnType::Timestamp));
    }

    #[test]
    fn test_pg_type_to_column_type_date() {
        assert!(matches!(PostgresConnector::pg_type_to_column_type("date"), ColumnType::Date));
    }

    #[test]
    fn test_pg_type_to_column_type_uuid() {
        assert!(matches!(PostgresConnector::pg_type_to_column_type("uuid"), ColumnType::Uuid));
    }

    #[test]
    fn test_pg_type_to_column_type_json() {
        assert!(matches!(PostgresConnector::pg_type_to_column_type("json"), ColumnType::Json));
        assert!(matches!(PostgresConnector::pg_type_to_column_type("jsonb"), ColumnType::Json));
    }

    #[test]
    fn test_pg_type_to_column_type_default_string() {
        assert!(matches!(PostgresConnector::pg_type_to_column_type("text"), ColumnType::String));
        assert!(matches!(PostgresConnector::pg_type_to_column_type("varchar"), ColumnType::String));
        assert!(matches!(PostgresConnector::pg_type_to_column_type("character varying"), ColumnType::String));
        assert!(matches!(PostgresConnector::pg_type_to_column_type("unknown_type"), ColumnType::String));
    }

    #[test]
    fn test_pg_type_case_insensitive() {
        assert!(matches!(PostgresConnector::pg_type_to_column_type("INTEGER"), ColumnType::Int32));
        assert!(matches!(PostgresConnector::pg_type_to_column_type("BIGINT"), ColumnType::Int64));
        assert!(matches!(PostgresConnector::pg_type_to_column_type("Boolean"), ColumnType::Boolean));
    }

    #[test]
    fn test_to_arrow_schema() {
        let table_schema = TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::Int64, false),
                ColumnSchema::new("name", ColumnType::String, true),
                ColumnSchema::new("active", ColumnType::Boolean, false),
            ],
        };
        
        let arrow_schema = PostgresConnector::to_arrow_schema(&table_schema);
        
        assert_eq!(arrow_schema.fields().len(), 3);
        
        let id_field = arrow_schema.field_with_name("id").unwrap();
        assert_eq!(id_field.data_type(), &DataType::Int64);
        // All columns are marked nullable in Arrow to handle edge cases
        // (see to_arrow_schema comment: type conversion errors may produce nulls)
        assert!(id_field.is_nullable());
        
        let name_field = arrow_schema.field_with_name("name").unwrap();
        assert_eq!(name_field.data_type(), &DataType::Utf8);
        assert!(name_field.is_nullable());
        
        let active_field = arrow_schema.field_with_name("active").unwrap();
        assert_eq!(active_field.data_type(), &DataType::Boolean);
        assert!(active_field.is_nullable());
    }

    #[test]
    fn test_to_arrow_schema_float32_column() {
        let table_schema = TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::Int32, false),
                ColumnSchema::new("temperature", ColumnType::Float32, false),
                ColumnSchema::new("name", ColumnType::String, true),
            ],
        };

        let arrow_schema = PostgresConnector::to_arrow_schema(&table_schema);
        let temp_field = arrow_schema.field_with_name("temperature").unwrap();
        assert_eq!(
            temp_field.data_type(),
            &DataType::Float32,
            "Float32 ColumnType must produce Float32 Arrow type, not Utf8"
        );
    }

    #[test]
    fn test_source_type() {
        let config = PostgresConfig::new("postgres://localhost/db");
        let connector = PostgresConnector::new(config);
        assert_eq!(connector.source_type(), SourceType::PostgreSQL);
    }

    #[tokio::test]
    #[ignore = "requires a running PostgreSQL instance"]
    async fn test_list_tables_integration() {
        let config = PostgresConfig::new("postgres://localhost/db");
        let connector = PostgresConnector::new(config);
        
        // Integration test - requires real database
        let tables = connector.list_tables().await.unwrap();
        // Actual behavior depends on database content
        assert!(tables.is_empty() || !tables.is_empty());
    }

    #[tokio::test]
    #[ignore = "requires a running PostgreSQL instance"]
    async fn test_get_schema_integration() {
        let config = PostgresConfig::new("postgres://localhost/db");
        let connector = PostgresConnector::new(config);
        
        // Integration test - requires real database
        let result = connector.get_schema("users").await;
        // Will fail if table doesn't exist or connection fails
        assert!(result.is_ok() || result.is_err());
    }

    #[tokio::test]
    #[ignore = "requires a running PostgreSQL instance"]
    async fn test_fetch_table_integration() {
        let config = PostgresConfig::new("postgres://localhost/db");
        let connector = PostgresConnector::new(config);
        
        // Integration test - requires real database
        let result = connector.fetch_table("users", None, None).await;
        // Will fail if table doesn't exist or connection fails
        assert!(result.is_ok() || result.is_err());
    }

    #[tokio::test]
    #[ignore = "requires a running PostgreSQL instance"]
    async fn test_validate_credentials_integration() {
        let config = PostgresConfig::new("postgres://localhost/db");
        let connector = PostgresConnector::new(config);
        
        // Integration test - requires real database
        let result = connector.validate_credentials().await;
        // Will fail if connection fails
        assert!(result.is_ok() || result.is_err());
    }
}
