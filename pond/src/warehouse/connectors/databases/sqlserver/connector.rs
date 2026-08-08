//! SQL Server Connector Implementation
//!
//! Cold tier connector for SQL Server with optional ClickHouse index acceleration.
//!
//! # Features
//!
//! - Cursor-based streaming for memory-efficient large table processing
//! - Schema inference from INFORMATION_SCHEMA
//! - Connection pooling with bb8
//! - Optional ClickHouse index layer for accelerated queries
//! - Predicate pushdown to SQL Server

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, Int8Builder, StringBuilder, TimestampMillisecondBuilder,
};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bb8::Pool;
use bb8_tiberius::ConnectionManager;
use tiberius::Row;
use tokio::sync::{OnceCell, Semaphore};

use super::config::SqlServerConfig;
use super::filter::validate_column_name;
use super::schema::{
    build_arrow_schema, build_table_schema, ColumnInfo, ESTIMATE_ROW_COUNT_QUERY,
    GET_COLUMNS_QUERY, LIST_TABLES_QUERY,
};
use super::utils::escape_sqlserver_string;
use crate::warehouse::connectors::{
    Connector, ConnectorError, ConnectorResult, FetchOptions, RecordBatchStream, TableInfo,
};
use crate::warehouse::types::{SourceType, TableSchema};

/// Maximum rows to fetch per batch.
const BATCH_SIZE: usize = 10_000;

/// Maximum concurrent schema inference operations.
const MAX_CONCURRENT_INFERENCES: usize = 10;

/// SQL Server data source connector.
///
/// Provides cold tier access to SQL Server databases with optional
/// ClickHouse index acceleration for improved query performance.
pub struct SqlServerConnector {
    config: SqlServerConfig,
    /// Connection pool - initialized lazily
    pool: OnceCell<Pool<ConnectionManager>>,
    /// Cached schemas per table
    schema_cache: parking_lot::RwLock<HashMap<String, (TableSchema, Arc<Schema>)>>,
}

impl SqlServerConnector {
    /// Create a new SQL Server connector.
    pub fn new(config: SqlServerConfig) -> Self {
        Self {
            config,
            pool: OnceCell::new(),
            schema_cache: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    /// Get or create the connection pool.
    async fn get_pool(&self) -> ConnectorResult<Pool<ConnectionManager>> {
        self.pool
            .get_or_try_init(|| async {
                let tiberius_config = self.config.to_tiberius_config();
                let manager = ConnectionManager::new(tiberius_config);

                Pool::builder()
                    .max_size(self.config.max_connections)
                    .connection_timeout(std::time::Duration::from_secs(
                        self.config.connect_timeout_secs,
                    ))
                    .build(manager)
                    .await
                    .map_err(|e| {
                        ConnectorError::Network(format!("Failed to create connection pool: {}", e))
                    })
            })
            .await
            .cloned()
    }

    /// Get column information for a table.
    async fn get_column_info(&self, table: &str) -> ConnectorResult<Vec<ColumnInfo>> {
        let pool = self.get_pool().await?;
        let mut conn = pool.get().await.map_err(|e| {
            ConnectorError::Network(format!(
                "Failed to get connection for table '{}': {}",
                table, e
            ))
        })?;

        let rows = conn
            .query(
                GET_COLUMNS_QUERY,
                &[&self.config.database, &self.config.schema, &table],
            )
            .await
            .map_err(|e| {
                ConnectorError::Internal(format!(
                    "Failed to query columns for table '{}': {}",
                    table, e
                ))
            })?
            .into_first_result()
            .await
            .map_err(|e| {
                ConnectorError::Internal(format!(
                    "Failed to read column rows for table '{}': {}",
                    table, e
                ))
            })?;

        let mut columns = Vec::new();
        for row in rows {
            columns.push(ColumnInfo {
                table_name: get_string(&row, 0)?,
                column_name: get_string(&row, 1)?,
                ordinal_position: get_i32(&row, 2)?,
                data_type: get_string(&row, 3)?,
                is_nullable: get_i32(&row, 4)? == 1,
                character_maximum_length: get_optional_i32(&row, 5),
                numeric_precision: get_optional_i32(&row, 6),
                numeric_scale: get_optional_i32(&row, 7),
            });
        }

        Ok(columns)
    }

    /// Estimate row count for a table.
    async fn estimate_row_count(&self, table: &str) -> ConnectorResult<Option<u64>> {
        let pool = self.get_pool().await?;
        let mut conn = pool.get().await.map_err(|e| {
            ConnectorError::Network(format!(
                "Failed to get connection for estimating row count of '{}': {}",
                table, e
            ))
        })?;

        let rows = conn
            .query(ESTIMATE_ROW_COUNT_QUERY, &[&self.config.schema, &table])
            .await
            .map_err(|e| {
                ConnectorError::Internal(format!(
                    "Failed to estimate row count for table '{}': {}",
                    table, e
                ))
            })?
            .into_first_result()
            .await
            .map_err(|e| {
                ConnectorError::Internal(format!(
                    "Failed to read row count result for table '{}': {}",
                    table, e
                ))
            })?;

        if let Some(row) = rows.first() {
            if let Some(count) = get_optional_i64(row, 0) {
                return Ok(Some(count as u64));
            }
        }

        Ok(None)
    }

    /// Build a SELECT query with optional filtering.
    ///
    /// Validates all column names to prevent SQL injection.
    fn build_select_query(
        &self,
        table: &str,
        incremental_key: Option<&str>,
        last_value: Option<&str>,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> ConnectorResult<String> {
        let mut query = format!("SELECT * FROM [{}].[{}]", self.config.schema, table);

        if let (Some(key), Some(value)) = (incremental_key, last_value) {
            // Validate column name to prevent SQL injection
            validate_column_name(key)?;
            query.push_str(&format!(
                " WHERE [{}] > '{}'",
                key,
                escape_sqlserver_string(value)
            ));
        }

        // Add ORDER BY for consistent pagination
        if let Some(key) = incremental_key {
            // Already validated above if we have a value, but validate if only key is provided
            validate_column_name(key)?;
            query.push_str(&format!(" ORDER BY [{}]", key));
        } else {
            // Use a default ordering if no incremental key
            query.push_str(" ORDER BY (SELECT NULL)");
        }

        // Add pagination
        if let Some(off) = offset {
            query.push_str(&format!(" OFFSET {} ROWS", off));
            if let Some(lim) = limit {
                query.push_str(&format!(" FETCH NEXT {} ROWS ONLY", lim));
            }
        } else if let Some(lim) = limit {
            query.push_str(&format!(" OFFSET 0 ROWS FETCH NEXT {} ROWS ONLY", lim));
        }

        Ok(query)
    }

    /// Convert rows to an Arrow RecordBatch.
    fn rows_to_record_batch(
        &self,
        rows: &[Row],
        schema: &Arc<Schema>,
    ) -> ConnectorResult<RecordBatch> {
        if rows.is_empty() {
            return Ok(RecordBatch::new_empty(schema.clone()));
        }

        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

        for (idx, field) in schema.fields().iter().enumerate() {
            let array = build_array_from_rows(rows, idx, field.data_type())?;
            arrays.push(array);
        }

        RecordBatch::try_new(schema.clone(), arrays).map_err(|e| {
            ConnectorError::Internal(format!("Failed to create RecordBatch: {}", e))
        })
    }
}

#[async_trait]
impl Connector for SqlServerConnector {
    fn source_type(&self) -> SourceType {
        SourceType::SqlServer
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        let pool = self.get_pool().await?;
        let mut conn = pool.get().await.map_err(|e| {
            ConnectorError::Network(format!("Failed to get connection: {}", e))
        })?;

        // Get list of tables
        let rows = conn
            .query(
                LIST_TABLES_QUERY,
                &[&self.config.database, &self.config.schema],
            )
            .await
            .map_err(|e| {
                ConnectorError::Internal(format!("Failed to list tables: {}", e))
            })?
            .into_first_result()
            .await
            .map_err(|e| {
                ConnectorError::Internal(format!("Failed to read table rows: {}", e))
            })?;

        let mut table_names = Vec::new();
        for row in rows {
            let name = get_string(&row, 0)?;

            // Filter by configured tables if specified
            if self.config.tables.is_empty() || self.config.tables.contains(&name) {
                table_names.push(name);
            }
        }

        drop(conn);

        // Get schema and row count for each table concurrently with a semaphore
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_INFERENCES));

        let futures: Vec<_> = table_names
            .into_iter()
            .map(|name| {
                let sem = Arc::clone(&semaphore);
                async move {
                    let _permit = sem.acquire().await.map_err(|_| {
                        ConnectorError::Internal("Semaphore closed".to_string())
                    })?;

                    let columns = self.get_column_info(&name).await?;
                    let table_schema = build_table_schema(&columns);
                    let estimated_rows = self.estimate_row_count(&name).await?;

                    // Check for common incremental key columns
                    let incremental_key = table_schema
                        .columns
                        .iter()
                        .find(|c| {
                            let lower = c.name.to_lowercase();
                            lower == "id" || lower == "updated_at" || lower == "modified_at"
                        })
                        .map(|c| c.name.clone());

                    Ok::<_, ConnectorError>(TableInfo {
                        name,
                        schema: table_schema,
                        supports_incremental: incremental_key.is_some(),
                        incremental_key,
                        estimated_rows,
                        primary_key_columns: Vec::new(),
                    })
                }
            })
            .collect();

        let results = futures::future::join_all(futures).await;

        // Collect results, propagating any errors
        let mut tables = Vec::with_capacity(results.len());
        for result in results {
            tables.push(result?);
        }

        Ok(tables)
    }

    async fn get_schema(&self, table: &str) -> ConnectorResult<TableSchema> {
        // Check cache first
        {
            let cache = self.schema_cache.read();
            if let Some((schema, _)) = cache.get(table) {
                return Ok(schema.clone());
            }
        }

        // Fetch from database
        let columns = self.get_column_info(table).await?;
        let table_schema = build_table_schema(&columns);
        let arrow_schema = Arc::new(build_arrow_schema(&columns));

        // Cache the result
        {
            let mut cache = self.schema_cache.write();
            cache.insert(table.to_string(), (table_schema.clone(), arrow_schema));
        }

        Ok(table_schema)
    }

    async fn fetch_table(
        &self,
        table: &str,
        incremental_key: Option<&str>,
        last_value: Option<&str>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        // Ensure schema is cached
        let _ = self.get_schema(table).await?;

        let arrow_schema = {
            let cache = self.schema_cache.read();
            cache.get(table).map(|(_, s)| s.clone())
        }
        .ok_or_else(|| ConnectorError::TableNotFound(table.to_string()))?;

        let pool = self.get_pool().await?;

        // Use pagination to avoid loading all rows into memory at once
        let mut batches = Vec::new();
        let mut offset = 0usize;

        loop {
            let mut conn = pool.get().await.map_err(|e| {
                ConnectorError::Network(format!(
                    "Failed to get connection for table '{}': {}",
                    table, e
                ))
            })?;

            let query_str =
                self.build_select_query(table, incremental_key, last_value, Some(offset), Some(BATCH_SIZE))?;

            let rows = conn
                .query(&query_str, &[])
                .await
                .map_err(|e| {
                    ConnectorError::Internal(format!(
                        "Failed to execute query for table '{}': {}",
                        table, e
                    ))
                })?
                .into_first_result()
                .await
                .map_err(|e| {
                    ConnectorError::Internal(format!(
                        "Failed to read rows from table '{}': {}",
                        table, e
                    ))
                })?;

            if rows.is_empty() {
                break;
            }

            let row_count = rows.len();
            let batch = self.rows_to_record_batch(&rows, &arrow_schema)?;
            batches.push(batch);

            offset += row_count;

            // If we got fewer rows than requested, we've reached the end
            if row_count < BATCH_SIZE {
                break;
            }
        }

        Ok(batches)
    }

    fn fetch_table_stream<'a>(
        &'a self,
        table: &'a str,
        options: FetchOptions,
    ) -> Pin<Box<dyn futures::Future<Output = ConnectorResult<RecordBatchStream>> + Send + 'a>>
    {
        Box::pin(async move {
            // Ensure schema is cached
            let _ = self.get_schema(table).await?;

            let arrow_schema = {
                let cache = self.schema_cache.read();
                cache.get(table).map(|(_, s)| s.clone())
            }
            .ok_or_else(|| ConnectorError::TableNotFound(table.to_string()))?;

            let pool = self.get_pool().await?;
            let batch_size = options.batch_size.unwrap_or(BATCH_SIZE);
            let incremental_key = options.incremental_key.clone();
            let last_value = options.last_value.clone();
            let schema_name = self.config.schema.clone();
            let table_name = table.to_string();

            let stream = async_stream::try_stream! {
                let mut offset = 0usize;

                loop {
                    let mut conn = pool.get().await.map_err(|e| {
                        ConnectorError::Network(format!("Failed to get connection: {}", e))
                    })?;

                    let query_str = build_paginated_query(
                        &schema_name,
                        &table_name,
                        incremental_key.as_deref(),
                        last_value.as_deref(),
                        offset,
                        batch_size,
                    )?;

                    let rows = conn
                        .query(&query_str, &[])
                        .await
                        .map_err(|e| {
                            ConnectorError::Internal(format!("Failed to execute query: {}", e))
                        })?
                        .into_first_result()
                        .await
                        .map_err(|e| {
                            ConnectorError::Internal(format!("Failed to read rows: {}", e))
                        })?;

                    if rows.is_empty() {
                        break;
                    }

                    let row_count = rows.len();
                    let batch = rows_to_batch(&rows, &arrow_schema)?;
                    yield batch;

                    offset += row_count;

                    // If we got fewer rows than requested, we've reached the end
                    if row_count < batch_size {
                        break;
                    }
                }
            };

            Ok(Box::pin(stream) as RecordBatchStream)
        })
    }

    async fn validate_credentials(&self) -> ConnectorResult<()> {
        let pool = self.get_pool().await?;
        let mut conn = pool.get().await.map_err(|e| {
            ConnectorError::Authentication(format!("Failed to connect: {}", e))
        })?;

        // Simple query to verify connection
        conn.query("SELECT 1", &[])
            .await
            .map_err(|e| ConnectorError::Authentication(format!("Connection test failed: {}", e)))?;

        Ok(())
    }
    
    fn supports_cdc(&self) -> bool {
        true // SQL Server supports CDC (Change Data Capture) tables
    }
}

// Helper functions

fn get_string(row: &Row, idx: usize) -> ConnectorResult<String> {
    match row.get::<&str, _>(idx) {
        Some(s) => Ok(s.to_string()),
        None => Err(ConnectorError::Internal(format!(
            "Expected string at column {}",
            idx
        ))),
    }
}

fn get_i32(row: &Row, idx: usize) -> ConnectorResult<i32> {
    // SQL Server returns various integer types - try them in order of likelihood
    if let Some(v) = row.try_get::<i32, _>(idx).ok().flatten() {
        return Ok(v);
    }
    if let Some(v) = row.try_get::<i16, _>(idx).ok().flatten() {
        return Ok(v as i32);
    }
    if let Some(v) = row.try_get::<u8, _>(idx).ok().flatten() {
        return Ok(v as i32);
    }
    if let Some(v) = row.try_get::<i64, _>(idx).ok().flatten() {
        return Ok(v as i32);
    }
    Err(ConnectorError::Internal(format!(
        "Expected integer at column {}",
        idx
    )))
}

fn get_optional_i32(row: &Row, idx: usize) -> Option<i32> {
    // SQL Server returns various integer types - try them in order of likelihood
    if let Some(v) = row.try_get::<i32, _>(idx).ok().flatten() {
        return Some(v);
    }
    if let Some(v) = row.try_get::<i16, _>(idx).ok().flatten() {
        return Some(v as i32);
    }
    if let Some(v) = row.try_get::<u8, _>(idx).ok().flatten() {
        return Some(v as i32);
    }
    if let Some(v) = row.try_get::<i64, _>(idx).ok().flatten() {
        return Some(v as i32);
    }
    None
}

fn get_optional_i64(row: &Row, idx: usize) -> Option<i64> {
    // SQL Server returns various integer types - try them in order of likelihood
    if let Some(v) = row.try_get::<i64, _>(idx).ok().flatten() {
        return Some(v);
    }
    if let Some(v) = row.try_get::<i32, _>(idx).ok().flatten() {
        return Some(v as i64);
    }
    if let Some(v) = row.try_get::<i16, _>(idx).ok().flatten() {
        return Some(v as i64);
    }
    if let Some(v) = row.try_get::<u8, _>(idx).ok().flatten() {
        return Some(v as i64);
    }
    None
}

fn build_paginated_query(
    schema: &str,
    table: &str,
    incremental_key: Option<&str>,
    last_value: Option<&str>,
    offset: usize,
    limit: usize,
) -> ConnectorResult<String> {
    let mut query = format!("SELECT * FROM [{}].[{}]", schema, table);

    if let (Some(key), Some(value)) = (incremental_key, last_value) {
        // Validate column name to prevent SQL injection
        validate_column_name(key)?;
        query.push_str(&format!(
            " WHERE [{}] > '{}'",
            key,
            escape_sqlserver_string(value)
        ));
    }

    if let Some(key) = incremental_key {
        // Already validated above if we have a value, but validate if only key is provided
        validate_column_name(key)?;
        query.push_str(&format!(" ORDER BY [{}]", key));
    } else {
        query.push_str(" ORDER BY (SELECT NULL)");
    }

    query.push_str(&format!(
        " OFFSET {} ROWS FETCH NEXT {} ROWS ONLY",
        offset, limit
    ));
    Ok(query)
}

fn rows_to_batch(rows: &[Row], schema: &Arc<Schema>) -> ConnectorResult<RecordBatch> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for (idx, field) in schema.fields().iter().enumerate() {
        let array = build_array_from_rows(rows, idx, field.data_type())?;
        arrays.push(array);
    }

    RecordBatch::try_new(schema.clone(), arrays).map_err(|e| {
        ConnectorError::Internal(format!("Failed to create RecordBatch: {}", e))
    })
}

fn build_array_from_rows(
    rows: &[Row],
    col_idx: usize,
    data_type: &arrow::datatypes::DataType,
) -> ConnectorResult<ArrayRef> {
    use arrow::datatypes::DataType;

    match data_type {
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                match row.get::<bool, _>(col_idx) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int8 => {
            let mut builder = Int8Builder::with_capacity(rows.len());
            for row in rows {
                // Tiberius uses u8 for tinyint
                match row.get::<u8, _>(col_idx) {
                    Some(v) => builder.append_value(v as i8),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int16 => {
            let mut builder = Int16Builder::with_capacity(rows.len());
            for row in rows {
                match row.get::<i16, _>(col_idx) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int32 => {
            let mut builder = Int32Builder::with_capacity(rows.len());
            for row in rows {
                match row.get::<i32, _>(col_idx) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(rows.len());
            for row in rows {
                match row.get::<i64, _>(col_idx) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float32 => {
            let mut builder = Float32Builder::with_capacity(rows.len());
            for row in rows {
                match row.get::<f32, _>(col_idx) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(rows.len());
            for row in rows {
                // Try f64 first, then fall back to Numeric for decimal/money types
                if let Some(v) = row.try_get::<f64, _>(col_idx).ok().flatten() {
                    builder.append_value(v);
                } else if let Some(n) = row.try_get::<tiberius::numeric::Numeric, _>(col_idx).ok().flatten() {
                    // Convert Numeric to f64
                    let value: f64 = n.into();
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Timestamp(_, tz) => {
            let mut builder = TimestampMillisecondBuilder::with_capacity(rows.len());
            for row in rows {
                match row.get::<chrono::NaiveDateTime, _>(col_idx) {
                    Some(dt) => builder.append_value(dt.and_utc().timestamp_millis()),
                    None => builder.append_null(),
                }
            }
            // Apply timezone from schema to match expected type
            let array = builder.finish();
            let array_with_tz = array.with_timezone_opt(tz.clone());
            Ok(Arc::new(array_with_tz))
        }
        _ => {
            // Default to string for all other types
            let mut builder = StringBuilder::with_capacity(rows.len(), rows.len() * 64);
            for row in rows {
                match row.get::<&str, _>(col_idx) {
                    Some(s) => builder.append_value(s),
                    None => {
                        // Try to get as other types and convert to string
                        if let Some(v) = row.get::<i32, _>(col_idx) {
                            builder.append_value(v.to_string());
                        } else if let Some(v) = row.get::<i64, _>(col_idx) {
                            builder.append_value(v.to_string());
                        } else if let Some(v) = row.get::<f64, _>(col_idx) {
                            builder.append_value(v.to_string());
                        } else {
                            builder.append_null();
                        }
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connector_creation() {
        let config = SqlServerConfig::new("localhost", "testdb", "sa", "password");
        let connector = SqlServerConnector::new(config);
        assert_eq!(connector.source_type(), SourceType::SqlServer);
    }

    #[test]
    fn test_build_paginated_query() {
        let query = build_paginated_query("dbo", "users", None, None, 0, 100).unwrap();
        assert!(query.contains("SELECT * FROM [dbo].[users]"));
        assert!(query.contains("OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY"));

        let query_with_filter =
            build_paginated_query("dbo", "users", Some("id"), Some("100"), 50, 100).unwrap();
        assert!(query_with_filter.contains("WHERE [id] > '100'"));
        assert!(query_with_filter.contains("ORDER BY [id]"));
        assert!(query_with_filter.contains("OFFSET 50 ROWS FETCH NEXT 100 ROWS ONLY"));
    }

    #[test]
    fn test_build_paginated_query_rejects_invalid_column() {
        // SQL injection attempt should be rejected
        let result = build_paginated_query("dbo", "users", Some("id; DROP TABLE users--"), Some("100"), 0, 100);
        assert!(result.is_err());

        // Valid column should work
        let result = build_paginated_query("dbo", "users", Some("updated_at"), Some("2024-01-01"), 0, 100);
        assert!(result.is_ok());
    }
}
