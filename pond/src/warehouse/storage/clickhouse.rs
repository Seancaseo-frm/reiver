//! Native ClickHouse Storage
//!
//! Manages MergeTree tables for synced warehouse data.
//!
//! PERFORMANCE: Native ClickHouse tables provide the best query performance
//! with automatic indexing, sorted data, and efficient merges.
//!
//! SECURITY: Tables are isolated per project using naming convention:
//! `warehouse_{project_id}_{table_name}`

use arrow::array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray,
};
use arrow::record_batch::RecordBatch;
use serde_json::{json, Value as JsonValue};
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

use crate::warehouse::ch_client::{ChClient, NativeChConfig, klickhouse_value_to_json};
use crate::warehouse::types::{ColumnType, TableSchema};

/// Errors that can occur during ClickHouse storage operations.
#[derive(Debug, Error)]
pub enum ClickHouseStorageError {
    #[error("ClickHouse connection error: {0}")]
    Connection(String),

    #[error("Query execution error: {0}")]
    Query(String),

    #[error("Table creation error: {0}")]
    TableCreation(String),

    #[error("Insert error: {0}")]
    Insert(String),

    #[error("Schema conversion error: {0}")]
    SchemaConversion(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Invalid table name: {0}")]
    InvalidTableName(String),

    #[error("Staging table not found: {0}")]
    StagingTableNotFound(String),
}

/// Result type for ClickHouse storage operations.
pub type ClickHouseStorageResult<T> = Result<T, ClickHouseStorageError>;

/// Configuration for ClickHouse native storage.
#[derive(Debug, Clone)]
pub struct ClickHouseStorageConfig {
    /// ClickHouse host
    pub host: String,
    /// Native TCP port (typically 9000)
    pub native_port: u16,
    /// Database name for warehouse tables
    pub database: String,
    /// Username for authentication
    pub username: Option<String>,
    /// Password for authentication
    pub password: Option<String>,
    /// Default table engine settings
    pub table_settings: TableSettings,
}

impl Default for ClickHouseStorageConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            native_port: 9000,
            database: "default".to_string(),
            username: Some("default".to_string()),
            password: None,
            table_settings: TableSettings::default(),
        }
    }
}

impl ClickHouseStorageConfig {
    fn native_config(&self) -> NativeChConfig {
        NativeChConfig {
            host: self.host.clone(),
            port: self.native_port,
            database: self.database.clone(),
            username: self.username.clone().unwrap_or_else(|| "default".to_string()),
            password: self.password.clone().unwrap_or_default(),
        }
    }
}

/// Settings for MergeTree table creation.
#[derive(Debug, Clone)]
pub struct TableSettings {
    /// Partition granularity (e.g., "toYYYYMM(created_at)")
    pub partition_by: Option<String>,
    /// Primary key / ORDER BY columns
    pub order_by: Vec<String>,
    /// Index granularity (default 8192)
    pub index_granularity: u32,
    /// TTL expression (e.g., "created_at + INTERVAL 1 YEAR")
    pub ttl: Option<String>,
}

impl Default for TableSettings {
    fn default() -> Self {
        Self {
            partition_by: None,
            order_by: vec!["tuple()".to_string()], // No ordering by default
            index_granularity: 8192,
            ttl: None,
        }
    }
}

/// Native ClickHouse storage for warehouse tables.
///
/// Provides MergeTree table management with project isolation.
pub struct ClickHouseStorage {
    config: ClickHouseStorageConfig,
    client: ChClient,
}

impl ClickHouseStorage {
    /// Create a new ClickHouse storage instance.
    ///
    /// Connects to ClickHouse via native TCP protocol.
    #[tracing::instrument(name = "warehouse.storage.ch.try_new", skip_all, err(Display))]
    pub async fn try_new(config: ClickHouseStorageConfig) -> Result<Self, ClickHouseStorageError> {
        let client = ChClient::connect(&config.native_config())
            .await
            .map_err(|e| ClickHouseStorageError::Connection(format!(
                "Failed to connect to ClickHouse: {}",
                e
            )))?;

        Ok(Self { config, client })
    }

    /// Create from environment variables.
    #[tracing::instrument(name = "warehouse.storage.ch.from_env", skip_all)]
    pub async fn from_env() -> Self {
        let config = ClickHouseStorageConfig {
            host: std::env::var("CLICKHOUSE_HOST")
                .unwrap_or_else(|_| "localhost".to_string()),
            native_port: std::env::var("CLICKHOUSE_NATIVE_PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(9000),
            database: std::env::var("CLICKHOUSE_DATABASE")
                .unwrap_or_else(|_| "default".to_string()),
            username: std::env::var("CLICKHOUSE_USER").ok(),
            password: std::env::var("CLICKHOUSE_PASSWORD").ok(),
            table_settings: TableSettings::default(),
        };
        Self::try_new(config).await.expect("Failed to connect to ClickHouse for storage")
    }

    /// Generate the full table name with project isolation.
    ///
    /// SECURITY: Uses project_id in table name to ensure data isolation.
    pub fn table_name(project_id: Uuid, table: &str) -> String {
        crate::warehouse::types::WarehouseTable::generate_clickhouse_table_name(project_id, table)
    }

    /// Create a MergeTree table based on schema.
    ///
    /// PERFORMANCE: Automatically selects optimal ORDER BY based on schema:
    /// - If `id` column exists: ORDER BY id
    /// - If `created_at` exists: ORDER BY created_at
    /// - Otherwise: ORDER BY tuple() (no ordering)
    #[tracing::instrument(
        name = "warehouse.storage.ch.create_table",
        skip_all,
        err(Display),
        fields(project_id = %project_id, table_name = table, database = %self.config.database)
    )]
    pub async fn create_table(
        &self,
        project_id: Uuid,
        table: &str,
        schema: &TableSchema,
        settings: Option<TableSettings>,
    ) -> ClickHouseStorageResult<()> {
        let table_name = Self::table_name(project_id, table);
        let settings = settings.unwrap_or_else(|| Self::infer_settings(schema));

        // Build column definitions
        let columns: Vec<String> = schema
            .columns
            .iter()
            .map(|col| {
                let ch_type = Self::column_type_to_clickhouse(&col.data_type, col.nullable);
                format!("    `{}` {}", col.name, ch_type)
            })
            .collect();

        let fts_indexes = Self::build_fts_index_clauses(schema);

        let mut all_definitions = columns;
        all_definitions.extend(fts_indexes);

        // Build ORDER BY clause
        let order_by = if settings.order_by.is_empty() {
            "tuple()".to_string()
        } else {
            settings.order_by.join(", ")
        };

        // Build CREATE TABLE statement
        let mut sql = format!(
            "CREATE TABLE IF NOT EXISTS `{}`.`{}` (\n{}\n)\nENGINE = MergeTree()\n",
            self.config.database,
            table_name,
            all_definitions.join(",\n")
        );

        // Add PARTITION BY if specified
        if let Some(partition) = &settings.partition_by {
            sql.push_str(&format!("PARTITION BY {}\n", partition));
        }

        sql.push_str(&format!("ORDER BY ({})\n", order_by));
        sql.push_str(&format!(
            "SETTINGS index_granularity = {}",
            settings.index_granularity
        ));

        // Add TTL if specified
        if let Some(ttl) = &settings.ttl {
            sql.push_str(&format!("\nTTL {}", ttl));
        }

        self.execute_query(&sql).await?;

        tracing::info!(
            project_id = %project_id,
            table = table,
            clickhouse_table = table_name,
            columns = schema.columns.len(),
            "Created ClickHouse MergeTree table"
        );

        Ok(())
    }

    /// Drop a table.
    #[tracing::instrument(
        name = "warehouse.storage.ch.drop_table",
        skip_all,
        err(Display),
        fields(project_id = %project_id, table_name = table, database = %self.config.database)
    )]
    pub async fn drop_table(&self, project_id: Uuid, table: &str) -> ClickHouseStorageResult<()> {
        let table_name = Self::table_name(project_id, table);

        let sql = format!(
            "DROP TABLE IF EXISTS `{}`.`{}`",
            self.config.database, table_name
        );

        self.execute_query(&sql).await?;

        tracing::info!(
            project_id = %project_id,
            table = table,
            clickhouse_table = table_name,
            "Dropped ClickHouse table"
        );

        Ok(())
    }

    /// Insert a batch of records into a table.
    ///
    /// SECURITY: Uses JSONEachRow format which properly handles escaping,
    /// preventing SQL injection attacks. This is the recommended approach
    /// for ClickHouse HTTP API inserts.
    ///
    /// PERFORMANCE: JSONEachRow is efficient for batch inserts and handles
    /// all data types safely including strings with special characters.
    #[tracing::instrument(
        name = "warehouse.storage.ch.insert_batch",
        skip_all,
        err(Display),
        fields(project_id = %project_id, table_name = table, database = %self.config.database)
    )]
    pub async fn insert_batch(
        &self,
        project_id: Uuid,
        table: &str,
        batch: &RecordBatch,
    ) -> ClickHouseStorageResult<u64> {
        if batch.num_rows() == 0 {
            return Ok(0);
        }

        let table_name = Self::table_name(project_id, table);
        let schema = batch.schema();

        // Build column names for the INSERT statement
        let columns: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        // Pre-resolve column types once to avoid per-cell dynamic downcasts.
        let col_arrays: Vec<&Arc<dyn Array>> = (0..batch.num_columns())
            .map(|i| batch.column(i))
            .collect();

        let mut json_rows = Vec::with_capacity(batch.num_rows());
        for row_idx in 0..batch.num_rows() {
            let mut row_obj = serde_json::Map::new();
            for (col_idx, col_name) in columns.iter().enumerate() {
                let json_value = self.array_value_to_json(col_arrays[col_idx], row_idx);
                row_obj.insert((*col_name).to_string(), json_value);
            }
            json_rows.push(serde_json::to_string(&JsonValue::Object(row_obj))
                .map_err(|e| ClickHouseStorageError::Insert(format!("JSON serialization error: {}", e)))?);
        }

        // Join with newlines for JSONEachRow format
        let json_body = json_rows.join("\n");

        // Table and database names are validated identifiers, not user input.
        // Do NOT append FORMAT here — execute_insert_with_data adds it.
        let query = format!(
            "INSERT INTO `{}`.`{}`",
            self.config.database,
            table_name,
        );

        self.execute_insert_with_data(&query, &json_body).await?;

        let rows_inserted = batch.num_rows() as u64;

        tracing::debug!(
            project_id = %project_id,
            table = table,
            rows = rows_inserted,
            "Inserted batch into ClickHouse table using JSONEachRow"
        );

        Ok(rows_inserted)
    }

    /// Convert an Arrow array value at a given index to a JSON value.
    ///
    /// This method safely converts Arrow data to JSON for use with
    /// ClickHouse's JSONEachRow format, avoiding SQL injection risks.
    fn array_value_to_json(&self, array: &Arc<dyn Array>, row_idx: usize) -> JsonValue {
        if array.is_null(row_idx) {
            return JsonValue::Null;
        }

        if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
            JsonValue::String(arr.value(row_idx).to_string())
        } else if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
            json!(arr.value(row_idx))
        } else if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
            json!(arr.value(row_idx))
        } else if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
            let val = arr.value(row_idx);
            // Handle NaN and Infinity
            if val.is_nan() || val.is_infinite() {
                JsonValue::Null
            } else {
                json!(val)
            }
        } else if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
            JsonValue::Bool(arr.value(row_idx))
        } else if let Some(arr) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
            let us = arr.value(row_idx);
            let secs = us.div_euclid(1_000_000);
            let nsecs = us.rem_euclid(1_000_000) as u32 * 1_000;
            if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) {
                JsonValue::String(dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
            } else {
                JsonValue::Null
            }
        } else if let Some(arr) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
            let ms = arr.value(row_idx);
            let secs = ms.div_euclid(1000);
            let nsecs = ms.rem_euclid(1000) as u32 * 1_000_000;
            if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) {
                JsonValue::String(dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string())
            } else {
                JsonValue::Null
            }
        } else {
            // Fallback for unsupported types
            JsonValue::Null
        }
    }

    /// Execute an INSERT query with data body (for JSONEachRow format).
    #[tracing::instrument(
        name = "warehouse.clickhouse.execute_insert_with_data",
        skip_all,
        err(Display)
    )]
    async fn execute_insert_with_data(&self, query: &str, data: &str) -> ClickHouseStorageResult<()> {
        let full_sql = format!("{} FORMAT JSONEachRow\n{}", query, data);
        self.client.execute(&full_sql)
            .await
            .map_err(|e| ClickHouseStorageError::Insert(format!(
                "ClickHouse insert failed: {}", e
            )))?;
        Ok(())
    }

    /// Truncate a table (delete all data but keep structure).
    #[tracing::instrument(
        name = "warehouse.storage.ch.truncate_table",
        skip_all,
        err(Display),
        fields(project_id = %project_id, table_name = table, database = %self.config.database)
    )]
    pub async fn truncate_table(
        &self,
        project_id: Uuid,
        table: &str,
    ) -> ClickHouseStorageResult<()> {
        let table_name = Self::table_name(project_id, table);

        let sql = format!(
            "TRUNCATE TABLE `{}`.`{}`",
            self.config.database, table_name
        );

        self.execute_query(&sql).await?;

        tracing::info!(
            project_id = %project_id,
            table = table,
            "Truncated ClickHouse table"
        );

        Ok(())
    }

    /// Check if a table exists.
    #[tracing::instrument(
        name = "warehouse.storage.ch.table_exists",
        skip_all,
        err(Display),
        fields(project_id = %project_id, table_name = table, database = %self.config.database)
    )]
    pub async fn table_exists(
        &self,
        project_id: Uuid,
        table: &str,
    ) -> ClickHouseStorageResult<bool> {
        let table_name = Self::table_name(project_id, table);

        let sql = format!(
            "SELECT 1 FROM system.tables WHERE database = '{}' AND name = '{}' LIMIT 1",
            Self::escape_clickhouse_string(&self.config.database),
            Self::escape_clickhouse_string(&table_name)
        );

        let response = self.execute_query_with_response(&sql).await?;
        Ok(!response.trim().is_empty())
    }

    /// Get row count for a table.
    #[tracing::instrument(
        name = "warehouse.storage.ch.row_count",
        skip_all,
        err(Display),
        fields(project_id = %project_id, table_name = table, database = %self.config.database)
    )]
    pub async fn row_count(&self, project_id: Uuid, table: &str) -> ClickHouseStorageResult<u64> {
        let table_name = Self::table_name(project_id, table);

        let sql = format!(
            "SELECT count() FROM `{}`.`{}`",
            self.config.database, table_name
        );

        let response = self.execute_query_with_response(&sql).await?;
        response
            .trim()
            .parse::<u64>()
            .map_err(|e| ClickHouseStorageError::Query(format!("Failed to parse count: {}", e)))
    }

    /// Generate the full table name with source context.
    ///
    /// Format: warehouse_{project_id}_{source_name}_{table_name}
    /// This allows multiple sources per project each with their own tables.
    pub fn source_table_name(project_id: Uuid, source_name: &str, table_name: &str) -> String {
        // Sanitize source_name and table_name to be valid ClickHouse identifiers
        // Valid identifiers: letters, digits, underscores (must start with letter/underscore)
        let sanitized_source = Self::sanitize_identifier(source_name);
        let sanitized_table = Self::sanitize_identifier(table_name);
        format!(
            "warehouse_{}_{}_{}",
            project_id.to_string().replace('-', "_"),
            sanitized_source,
            sanitized_table
        )
    }

    /// Escape a string value for safe interpolation into ClickHouse SQL single-quoted literals.
    /// Prevents SQL injection by escaping backslashes and single quotes.
    fn escape_clickhouse_string(s: &str) -> String {
        s.replace('\\', "\\\\").replace('\'', "\\'")
    }

    /// Sanitize a string to be a valid ClickHouse identifier.
    ///
    /// Valid identifiers contain only letters (a-z, A-Z), digits (0-9), and underscores.
    /// Must start with a letter or underscore. All other characters are replaced with underscores.
    /// Consecutive underscores are collapsed to single underscores.
    pub fn sanitize_identifier(name: &str) -> String {
        if name.is_empty() {
            return "_empty".to_string();
        }

        // Replace all non-alphanumeric characters with underscores
        let mut result: String = name
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();

        // Collapse consecutive underscores (single pass)
        let mut collapsed = String::with_capacity(result.len());
        let mut prev_underscore = false;
        for c in result.chars() {
            if c == '_' {
                if !prev_underscore {
                    collapsed.push(c);
                }
                prev_underscore = true;
            } else {
                collapsed.push(c);
                prev_underscore = false;
            }
        }
        result = collapsed;

        // Ensure it starts with a letter or underscore
        if let Some(first) = result.chars().next() {
            if first.is_ascii_digit() {
                result = format!("_{}", result);
            }
        }

        // Trim trailing underscores
        let trimmed = result.trim_end_matches('_');
        
        // If result is empty after all processing, return a placeholder
        if trimmed.is_empty() {
            "_empty".to_string()
        } else {
            trimmed.to_string()
        }
    }

    /// Create all ClickHouse tables for a source.
    ///
    /// This method creates MergeTree tables for each table in the provided schemas.
    /// Tables are named using the source-aware convention:
    /// `warehouse_{project_id}_{source_name}_{table_name}`
    #[tracing::instrument(
        name = "warehouse.storage.ch.create_source_tables",
        skip_all,
        err(Display),
        fields(project_id = %project_id, database = %self.config.database)
    )]
    pub async fn create_source_tables(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_schemas: &[(String, TableSchema)],
    ) -> ClickHouseStorageResult<Vec<String>> {
        let mut created_tables = Vec::with_capacity(table_schemas.len());

        for (table_name, schema) in table_schemas {
            let ch_table_name = Self::source_table_name(project_id, source_name, table_name);
            let settings = Self::infer_settings(schema);

            // Build column definitions
            let columns: Vec<String> = schema
                .columns
                .iter()
                .map(|col| {
                    let ch_type = Self::column_type_to_clickhouse(&col.data_type, col.nullable);
                    format!("    `{}` {}", col.name, ch_type)
                })
                .collect();

            // Build ORDER BY clause
            let order_by = if settings.order_by.is_empty() {
                "tuple()".to_string()
            } else {
                settings.order_by.join(", ")
            };

            // Build CREATE TABLE statement
            let mut sql = format!(
                "CREATE TABLE IF NOT EXISTS `{}`.`{}` (\n{}\n)\nENGINE = MergeTree()\n",
                self.config.database,
                ch_table_name,
                columns.join(",\n")
            );

            // Add PARTITION BY if specified
            if let Some(partition) = &settings.partition_by {
                sql.push_str(&format!("PARTITION BY {}\n", partition));
            }

            sql.push_str(&format!("ORDER BY ({})\n", order_by));
            
            // Build SETTINGS clause
            // Note: allow_nullable_key=1 is required for schema-flexible sources like MongoDB
            // where all columns are nullable
            let setting_parts = vec![
                format!("index_granularity = {}", settings.index_granularity),
                "allow_nullable_key = 1".to_string(),
            ];
            sql.push_str(&format!("SETTINGS {}", setting_parts.join(", ")));

            // Add TTL if specified
            if let Some(ttl) = &settings.ttl {
                sql.push_str(&format!("\nTTL {}", ttl));
            }

            self.execute_query(&sql).await?;
            created_tables.push(ch_table_name.clone());

            tracing::info!(
                project_id = %project_id,
                source = source_name,
                table = table_name,
                clickhouse_table = ch_table_name,
                columns = schema.columns.len(),
                "Created ClickHouse table for source"
            );
        }

        Ok(created_tables)
    }

    /// Drop all ClickHouse tables for a source.
    ///
    /// This method drops all tables matching the source naming convention.
    /// If `table_names` is Some, only those specific tables are dropped.
    /// If `table_names` is None, all tables for the source are dropped (requires lookup).
    #[tracing::instrument(
        name = "warehouse.storage.ch.drop_source_tables",
        skip_all,
        err(Display),
        fields(project_id = %project_id, database = %self.config.database)
    )]
    pub async fn drop_source_tables(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_names: Option<&[String]>,
    ) -> ClickHouseStorageResult<Vec<String>> {
        let mut dropped_tables = Vec::new();

        if let Some(names) = table_names {
            // Drop specific tables
            for table_name in names {
                let ch_table_name = Self::source_table_name(project_id, source_name, table_name);
                let sql = format!(
                    "DROP TABLE IF EXISTS `{}`.`{}`",
                    self.config.database, ch_table_name
                );

                self.execute_query(&sql).await?;
                dropped_tables.push(ch_table_name.clone());

                tracing::info!(
                    project_id = %project_id,
                    source = source_name,
                    table = table_name,
                    clickhouse_table = ch_table_name,
                    "Dropped ClickHouse table for source"
                );
            }
        } else {
            let prefix = format!(
                "warehouse_{}_{}_",
                project_id.to_string().replace('-', "_"),
                Self::sanitize_identifier(source_name)
            );

            let escaped_prefix = prefix.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_");
            let sql = format!(
                "SELECT name FROM system.tables WHERE database = '{}' AND name LIKE '{}%' ESCAPE '\\'",
                Self::escape_clickhouse_string(&self.config.database), escaped_prefix
            );

            let response = self.execute_query_with_response(&sql).await?;
            
            for line in response.lines() {
                let ch_table_name = line.trim();
                if ch_table_name.is_empty() {
                    continue;
                }

                let drop_sql = format!(
                    "DROP TABLE IF EXISTS `{}`.`{}`",
                    self.config.database, ch_table_name
                );

                self.execute_query(&drop_sql).await?;
                dropped_tables.push(ch_table_name.to_string());

                tracing::info!(
                    project_id = %project_id,
                    source = source_name,
                    clickhouse_table = ch_table_name,
                    "Dropped ClickHouse table for source"
                );
            }
        }

        Ok(dropped_tables)
    }

    /// Truncate all tables for a source.
    ///
    /// This keeps the table structure but removes all data.
    #[tracing::instrument(
        name = "warehouse.storage.ch.truncate_source_tables",
        skip_all,
        err(Display),
        fields(project_id = %project_id, database = %self.config.database)
    )]
    pub async fn truncate_source_tables(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_names: &[String],
    ) -> ClickHouseStorageResult<Vec<String>> {
        let mut truncated_tables = Vec::new();

        for table_name in table_names {
            let ch_table_name = Self::source_table_name(project_id, source_name, table_name);
            let sql = format!(
                "TRUNCATE TABLE IF EXISTS `{}`.`{}`",
                self.config.database, ch_table_name
            );

            self.execute_query(&sql).await?;
            truncated_tables.push(ch_table_name.clone());

            tracing::info!(
                project_id = %project_id,
                source = source_name,
                table = table_name,
                clickhouse_table = ch_table_name,
                "Truncated ClickHouse table for source"
            );
        }

        Ok(truncated_tables)
    }

    /// Insert a batch of records into a source table.
    #[tracing::instrument(
        name = "warehouse.storage.ch.insert_source_batch",
        skip_all,
        err(Display),
        fields(project_id = %project_id, table_name = table_name, database = %self.config.database)
    )]
    pub async fn insert_source_batch(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
        batch: &RecordBatch,
    ) -> ClickHouseStorageResult<u64> {
        if batch.num_rows() == 0 {
            return Ok(0);
        }

        let ch_table_name = Self::source_table_name(project_id, source_name, table_name);
        let schema = batch.schema();

        // Build column names for the INSERT statement
        let columns: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        // Build JSONEachRow data - one JSON object per line
        let mut json_rows = Vec::with_capacity(batch.num_rows());
        for row_idx in 0..batch.num_rows() {
            let mut row_obj = serde_json::Map::new();
            for (col_idx, col_name) in columns.iter().enumerate() {
                let json_value = self.array_value_to_json(batch.column(col_idx), row_idx);
                row_obj.insert((*col_name).to_string(), json_value);
            }
            json_rows.push(serde_json::to_string(&JsonValue::Object(row_obj))
                .map_err(|e| ClickHouseStorageError::Insert(format!("JSON serialization error: {}", e)))?);
        }

        // Join with newlines for JSONEachRow format
        let json_body = json_rows.join("\n");

        // Do NOT append FORMAT here — execute_insert_with_data adds it.
        let query = format!(
            "INSERT INTO `{}`.`{}`",
            self.config.database,
            ch_table_name,
        );

        self.execute_insert_with_data(&query, &json_body).await?;

        let rows_inserted = batch.num_rows() as u64;

        tracing::debug!(
            project_id = %project_id,
            source = source_name,
            table = table_name,
            rows = rows_inserted,
            "Inserted batch into ClickHouse source table"
        );

        Ok(rows_inserted)
    }

    /// Check if a source table exists.
    #[tracing::instrument(
        name = "warehouse.storage.ch.source_table_exists",
        skip_all,
        err(Display),
        fields(project_id = %project_id, table_name = table_name, database = %self.config.database)
    )]
    pub async fn source_table_exists(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
    ) -> ClickHouseStorageResult<bool> {
        let ch_table_name = Self::source_table_name(project_id, source_name, table_name);

        let sql = format!(
            "SELECT 1 FROM system.tables WHERE database = '{}' AND name = '{}' LIMIT 1",
            Self::escape_clickhouse_string(&self.config.database),
            Self::escape_clickhouse_string(&ch_table_name)
        );

        let response = self.execute_query_with_response(&sql).await?;
        Ok(!response.trim().is_empty())
    }

    /// Get row count for a source table.
    #[tracing::instrument(
        name = "warehouse.storage.ch.source_row_count",
        skip_all,
        err(Display),
        fields(project_id = %project_id, table_name = table_name, database = %self.config.database)
    )]
    pub async fn source_row_count(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
    ) -> ClickHouseStorageResult<u64> {
        let ch_table_name = Self::source_table_name(project_id, source_name, table_name);

        let sql = format!(
            "SELECT count() FROM `{}`.`{}`",
            self.config.database, ch_table_name
        );

        let response = self.execute_query_with_response(&sql).await?;
        response
            .trim()
            .parse::<u64>()
            .map_err(|e| ClickHouseStorageError::Query(format!("Failed to parse count: {}", e)))
    }

    /// Infer optimal table settings from schema.
    fn infer_settings(schema: &TableSchema) -> TableSettings {
        let mut settings = TableSettings::default();

        // Look for good ORDER BY candidates
        let column_names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();

        // Prefer id column
        if column_names.contains(&"id") {
            settings.order_by = vec!["id".to_string()];
        }
        // Or use created_at for time-series data
        else if column_names.contains(&"created_at") {
            settings.order_by = vec!["created_at".to_string()];

            // Also partition by month if we have created_at
            settings.partition_by = Some("toYYYYMM(created_at)".to_string());
        }
        // Or use timestamp
        else if column_names.contains(&"timestamp") {
            settings.order_by = vec!["timestamp".to_string()];
            settings.partition_by = Some("toYYYYMM(timestamp)".to_string());
        }

        settings
    }

    /// Convert ColumnType to ClickHouse type.
    fn column_type_to_clickhouse(col_type: &ColumnType, nullable: bool) -> String {
        let base_type = match col_type {
            ColumnType::String => "String",
            ColumnType::Int32 => "Int32",
            ColumnType::Int64 => "Int64",
            ColumnType::Float32 => "Float32",
            ColumnType::Float64 => "Float64",
            ColumnType::Boolean => "Bool",
            ColumnType::Timestamp => "DateTime64(6)",
            ColumnType::Date => "Date",
            ColumnType::Json => "String", // Store JSON as String
            ColumnType::Uuid => "UUID",
            ColumnType::Decimal => "Decimal(18, 4)",
        };

        if nullable {
            format!("Nullable({})", base_type)
        } else {
            base_type.to_string()
        }
    }

    /// Build tokenbf_v1 INDEX clauses for all string columns in a schema.
    ///
    /// These bloom-token indexes accelerate `hasToken()` queries by letting
    /// ClickHouse skip granules that definitely don't contain a search token.
    fn build_fts_index_clauses(schema: &TableSchema) -> Vec<String> {
        schema
            .columns
            .iter()
            .filter(|col| col.data_type == ColumnType::String || col.data_type == ColumnType::Json)
            .map(|col| {
                format!(
                    "    INDEX idx_fts_{} `{}` TYPE tokenbf_v1(10240, 3, 0) GRANULARITY 4",
                    col.name, col.name
                )
            })
            .collect()
    }

    /// Add tokenbf_v1 indexes to an existing table via ALTER TABLE.
    pub async fn add_fulltext_indexes(
        &self,
        project_id: Uuid,
        table: &str,
        columns: &[String],
    ) -> ClickHouseStorageResult<()> {
        let table_name = Self::table_name(project_id, table);
        for col in columns {
            let sql = format!(
                "ALTER TABLE `{}`.`{}` ADD INDEX IF NOT EXISTS idx_fts_{} `{}` TYPE tokenbf_v1(10240, 3, 0) GRANULARITY 4",
                self.config.database, table_name, col, col
            );
            self.execute_query(&sql).await?;
        }
        Ok(())
    }

    /// Create a MergeTree buffer table (idempotent) with explicit ORDER BY.
    ///
    /// Used by the blockchain sync daemon to stage rows before they are
    /// flushed to R2 as Parquet.
    pub async fn ensure_buffer_table(
        &self,
        table_name: &str,
        schema: &TableSchema,
        order_by: &[&str],
    ) -> ClickHouseStorageResult<()> {
        let columns: Vec<String> = schema
            .columns
            .iter()
            .map(|col| {
                let ch_type = Self::column_type_to_clickhouse(&col.data_type, col.nullable);
                format!("    `{}` {}", col.name, ch_type)
            })
            .collect();

        let order_clause = if order_by.is_empty() {
            "tuple()".to_string()
        } else {
            order_by.join(", ")
        };

        let sql = format!(
            "CREATE TABLE IF NOT EXISTS `{}`.`{}` (\n{}\n)\nENGINE = MergeTree()\nORDER BY ({})\nSETTINGS index_granularity = 8192, allow_nullable_key = 1",
            self.config.database,
            table_name,
            columns.join(",\n"),
            order_clause,
        );

        self.execute_query(&sql).await?;
        tracing::info!(table = table_name, "Ensured buffer table exists");
        Ok(())
    }

    /// Insert a RecordBatch into a table by its raw name (no project/source prefix).
    pub async fn insert_batch_raw(
        &self,
        table_name: &str,
        batch: &RecordBatch,
    ) -> ClickHouseStorageResult<u64> {
        if batch.num_rows() == 0 {
            return Ok(0);
        }

        let schema = batch.schema();
        let columns: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        let mut json_rows = Vec::with_capacity(batch.num_rows());
        for row_idx in 0..batch.num_rows() {
            let mut row_obj = serde_json::Map::new();
            for (col_idx, col_name) in columns.iter().enumerate() {
                let json_value = self.array_value_to_json(batch.column(col_idx), row_idx);
                row_obj.insert((*col_name).to_string(), json_value);
            }
            json_rows.push(
                serde_json::to_string(&serde_json::Value::Object(row_obj))
                    .map_err(|e| ClickHouseStorageError::Insert(format!("JSON serialization error: {}", e)))?,
            );
        }

        let json_body = json_rows.join("\n");
        let query = format!(
            "INSERT INTO `{}`.`{}`",
            self.config.database, table_name,
        );
        self.execute_insert_with_data(&query, &json_body).await?;

        Ok(batch.num_rows() as u64)
    }

    /// Delete rows from a raw table where `column >= value`.
    pub async fn delete_from_raw(
        &self,
        table_name: &str,
        column: &str,
        min_value: i64,
    ) -> ClickHouseStorageResult<()> {
        let sql = format!(
            "ALTER TABLE `{}`.`{}` DELETE WHERE `{}` >= {}",
            self.config.database, table_name, column, min_value,
        );
        self.execute_query(&sql).await
    }

    /// Export rows from a raw table name to R2 via `INSERT INTO FUNCTION s3()`.
    /// `where_clause` is appended if non-empty (e.g. "WHERE block_height < 100").
    /// Returns the number of rows exported.
    pub async fn export_raw_to_s3(
        &self,
        table_name: &str,
        s3_collection_name: &str,
        r2_key: &str,
        where_clause: &str,
    ) -> ClickHouseStorageResult<u64> {
        Self::validate_s3_export_params(s3_collection_name, r2_key)?;

        let count_sql = if where_clause.is_empty() {
            format!(
                "SELECT count() FROM `{}`.`{}`",
                self.config.database, table_name
            )
        } else {
            format!(
                "SELECT count() FROM `{}`.`{}` {}",
                self.config.database, table_name, where_clause
            )
        };
        let resp = self.execute_query_with_response(&count_sql).await?;
        let row_count: u64 = resp.trim().parse().unwrap_or(0);

        if row_count == 0 {
            return Ok(0);
        }

        let select = if where_clause.is_empty() {
            format!(
                "SELECT * FROM `{}`.`{}`",
                self.config.database, table_name
            )
        } else {
            format!(
                "SELECT * FROM `{}`.`{}` {}",
                self.config.database, table_name, where_clause
            )
        };

        let sql = format!(
            "INSERT INTO FUNCTION s3({}, filename='{}', format='Parquet') {}",
            s3_collection_name, r2_key, select,
        );
        self.execute_query(&sql).await?;

        Ok(row_count)
    }

    /// Delete rows from a raw table matching a WHERE clause.
    /// `where_clause` must include the `WHERE` keyword.
    pub async fn delete_raw_where(
        &self,
        table_name: &str,
        where_clause: &str,
    ) -> ClickHouseStorageResult<()> {
        let sql = format!(
            "ALTER TABLE `{}`.`{}` DELETE {}",
            self.config.database, table_name, where_clause,
        );
        self.execute_query(&sql).await
    }

    /// Run a query and return the text response (public).
    pub async fn query_text(
        &self,
        sql: &str,
    ) -> ClickHouseStorageResult<String> {
        self.execute_query_with_response(sql).await
    }

    /// The database name configured for this ClickHouse storage.
    pub fn database(&self) -> &str {
        &self.config.database
    }

    /// Execute a query that doesn't return results.
    #[tracing::instrument(
        name = "warehouse.clickhouse.execute_query",
        skip_all,
        err(Display)
    )]
    async fn execute_query(&self, sql: &str) -> ClickHouseStorageResult<()> {
        let _ = self.execute_query_with_response(sql).await?;
        Ok(())
    }

    /// Execute a query and return the response body as text.
    #[tracing::instrument(
        name = "warehouse.clickhouse.execute_query_with_response",
        skip_all,
        err(Display)
    )]
    async fn execute_query_with_response(&self, sql: &str) -> ClickHouseStorageResult<String> {
        use futures::StreamExt;

        let mut stream = self.client.inner().query_raw(sql)
            .await
            .map_err(|e| ClickHouseStorageError::Query(format!("Query failed: {}", e)))?;

        let mut output = String::new();
        while let Some(block_result) = stream.next().await {
            let block = block_result
                .map_err(|e| ClickHouseStorageError::Query(format!("Block error: {}", e)))?;

            let col_names: Vec<&String> = block.column_data.keys().collect();
            let num_rows = block.rows as usize;

            for row_idx in 0..num_rows {
                for (col_idx, col_name) in col_names.iter().enumerate() {
                    if col_idx > 0 { output.push('\t'); }
                    let val = &block.column_data[*col_name][row_idx];
                    let json = klickhouse_value_to_json(val.clone());
                    match json {
                        serde_json::Value::String(s) => output.push_str(&s),
                        serde_json::Value::Null => {},
                        other => output.push_str(&other.to_string()),
                    }
                }
                output.push('\n');
            }
        }

        Ok(output.trim_end().to_string())
    }

    // ==================== Staging Table Methods (Transactional Sync) ====================

    /// Generate staging table name with _staging_ prefix.
    pub fn staging_table_name(project_id: Uuid, source_name: &str, table_name: &str) -> String {
        let base_name = Self::source_table_name(project_id, source_name, table_name);
        format!("_staging_{}", base_name)
    }

    /// Create a staging table with the same schema as the production table.
    /// 
    /// Staging tables are used for atomic sync operations - data is written
    /// to the staging table first, then swapped with the production table
    /// on success using EXCHANGE TABLES.
    #[tracing::instrument(
        name = "warehouse.storage.ch.create_staging_table",
        skip_all,
        err(Display),
        fields(project_id = %project_id, table_name = table_name, database = %self.config.database)
    )]
    pub async fn create_staging_table(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
        schema: &TableSchema,
    ) -> ClickHouseStorageResult<String> {
        let staging_name = Self::staging_table_name(project_id, source_name, table_name);
        let settings = Self::infer_settings(schema);

        // Build column definitions
        let columns: Vec<String> = schema
            .columns
            .iter()
            .map(|col| {
                let ch_type = Self::column_type_to_clickhouse(&col.data_type, col.nullable);
                format!("    `{}` {}", col.name, ch_type)
            })
            .collect();

        let fts_indexes = Self::build_fts_index_clauses(schema);

        let mut all_definitions = columns;
        all_definitions.extend(fts_indexes);

        let columns_sql = all_definitions.join(",\n");

        // Build ORDER BY clause.
        // Don't quote function calls like "tuple()" — only quote plain column names.
        let order_by_sql = if settings.order_by.is_empty() {
            "tuple()".to_string()
        } else {
            settings.order_by.iter()
                .map(|c| {
                    if c.contains('(') {
                        c.clone()
                    } else {
                        format!("`{}`", c)
                    }
                })
                .collect::<Vec<_>>()
                .join(", ")
        };

        // Build optional PARTITION BY clause
        let partition_sql = settings
            .partition_by
            .as_ref()
            .map(|p| format!("PARTITION BY {}", p))
            .unwrap_or_default();

        // Build optional TTL clause
        let ttl_sql = settings
            .ttl
            .as_ref()
            .map(|t| format!("TTL {}", t))
            .unwrap_or_default();

        let sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS `{}`.`{}`
            (
            {}
            )
            ENGINE = MergeTree()
            {}
            ORDER BY ({})
            {}
            SETTINGS index_granularity = {}, allow_nullable_key = 1
            "#,
            self.config.database,
            staging_name,
            columns_sql,
            partition_sql,
            order_by_sql,
            ttl_sql,
            settings.index_granularity,
        );

        self.execute_query(&sql).await?;

        tracing::info!(
            project_id = %project_id,
            source = source_name,
            table = table_name,
            staging_table = %staging_name,
            "Created ClickHouse staging table"
        );

        Ok(staging_name)
    }

    /// Insert a batch of records into a staging table.
    #[tracing::instrument(
        name = "warehouse.storage.ch.insert_staging_batch",
        skip_all,
        err(Display),
        fields(project_id = %project_id, table_name = table_name, database = %self.config.database)
    )]
    pub async fn insert_staging_batch(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
        batch: &RecordBatch,
    ) -> ClickHouseStorageResult<u64> {
        if batch.num_rows() == 0 {
            return Ok(0);
        }

        let staging_name = Self::staging_table_name(project_id, source_name, table_name);
        let schema = batch.schema();

        // Build column names for the INSERT statement
        let columns: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        // Build JSONEachRow data - one JSON object per line
        let mut json_rows = Vec::with_capacity(batch.num_rows());
        for row_idx in 0..batch.num_rows() {
            let mut row_obj = serde_json::Map::new();
            for (col_idx, col_name) in columns.iter().enumerate() {
                let json_value = self.array_value_to_json(batch.column(col_idx), row_idx);
                row_obj.insert((*col_name).to_string(), json_value);
            }
            json_rows.push(serde_json::to_string(&JsonValue::Object(row_obj))
                .map_err(|e| ClickHouseStorageError::Insert(format!("JSON serialization error: {}", e)))?);
        }

        // Join with newlines for JSONEachRow format
        let json_body = json_rows.join("\n");

        // Do NOT append FORMAT here — execute_insert_with_data adds it.
        let query = format!(
            "INSERT INTO `{}`.`{}`",
            self.config.database,
            staging_name,
        );

        self.execute_insert_with_data(&query, &json_body).await?;

        tracing::debug!(
            project_id = %project_id,
            source = source_name,
            table = table_name,
            staging_table = %staging_name,
            rows = batch.num_rows(),
            "Inserted batch into ClickHouse staging table"
        );

        Ok(batch.num_rows() as u64)
    }

    /// Commit a staging table by atomically swapping it with the production table.
    /// 
    /// Uses EXCHANGE TABLES for an atomic swap, then drops the old table
    /// (which now has the staging name).
    #[tracing::instrument(
        name = "warehouse.storage.ch.commit_staging_table",
        skip_all,
        err(Display),
        fields(project_id = %project_id, table_name = table_name, database = %self.config.database)
    )]
    pub async fn commit_staging_table(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
    ) -> ClickHouseStorageResult<()> {
        let prod_name = Self::source_table_name(project_id, source_name, table_name);
        let staging_name = Self::staging_table_name(project_id, source_name, table_name);

        let staging_exists = self.table_exists_internal(&staging_name).await?;
        if !staging_exists {
            return Err(ClickHouseStorageError::StagingTableNotFound(staging_name));
        }

        // Check if production table exists - if not, just rename staging to prod
        let prod_exists = self.table_exists_internal(&prod_name).await?;

        if prod_exists {
            // EXCHANGE TABLES is atomic in ClickHouse
            let exchange_sql = format!(
                "EXCHANGE TABLES `{}`.`{}` AND `{}`.`{}`",
                self.config.database, staging_name,
                self.config.database, prod_name
            );

            self.execute_query(&exchange_sql).await?;

            // Drop old table (now has staging name after exchange)
            let drop_sql = format!(
                "DROP TABLE IF EXISTS `{}`.`{}`",
                self.config.database, staging_name
            );
            self.execute_query(&drop_sql).await?;

            tracing::info!(
                project_id = %project_id,
                source = source_name,
                table = table_name,
                "Committed staging table via EXCHANGE TABLES"
            );
        } else {
            // No production table exists, just rename staging to prod
            let rename_sql = format!(
                "RENAME TABLE `{}`.`{}` TO `{}`.`{}`",
                self.config.database, staging_name,
                self.config.database, prod_name
            );

            self.execute_query(&rename_sql).await?;

            tracing::info!(
                project_id = %project_id,
                source = source_name,
                table = table_name,
                "Committed staging table via RENAME (no existing production table)"
            );
        }

        Ok(())
    }

    /// Drop staging tables on failure or cleanup.
    /// 
    /// If `table_names` is None, drops all staging tables for the source.
    #[tracing::instrument(
        name = "warehouse.storage.ch.drop_staging_tables",
        skip_all,
        err(Display),
        fields(project_id = %project_id, database = %self.config.database)
    )]
    pub async fn drop_staging_tables(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_names: Option<&[String]>,
    ) -> ClickHouseStorageResult<Vec<String>> {
        let mut dropped_tables = Vec::new();

        if let Some(names) = table_names {
            // Drop specific staging tables
            for table_name in names {
                let staging_name = Self::staging_table_name(project_id, source_name, table_name);
                let sql = format!(
                    "DROP TABLE IF EXISTS `{}`.`{}`",
                    self.config.database, staging_name
                );

                self.execute_query(&sql).await?;
                dropped_tables.push(staging_name.clone());

                tracing::info!(
                    project_id = %project_id,
                    source = source_name,
                    table = table_name,
                    staging_table = %staging_name,
                    "Dropped ClickHouse staging table"
                );
            }
        } else {
            // Drop all staging tables for this source by pattern matching
            let prefix = format!(
                "_staging_warehouse_{}_{}",
                project_id.to_string().replace('-', "_"),
                Self::sanitize_identifier(source_name)
            );

            let escaped_prefix = prefix.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_");
            let list_sql = format!(
                "SELECT name FROM system.tables WHERE database = '{}' AND name LIKE '{}%' ESCAPE '\\'",
                Self::escape_clickhouse_string(&self.config.database), escaped_prefix
            );

            let response = self.execute_query_with_response(&list_sql).await?;
            let table_names: Vec<&str> = response.lines().filter(|l| !l.is_empty()).collect();

            for staging_name in table_names {
                let sql = format!(
                    "DROP TABLE IF EXISTS `{}`.`{}`",
                    self.config.database, staging_name
                );

                self.execute_query(&sql).await?;
                dropped_tables.push(staging_name.to_string());

                tracing::info!(
                    project_id = %project_id,
                    source = source_name,
                    staging_table = %staging_name,
                    "Dropped ClickHouse staging table (pattern match)"
                );
            }
        }

        Ok(dropped_tables)
    }

    /// Check if a table exists (internal helper).
    #[tracing::instrument(
        name = "warehouse.clickhouse.table_exists_internal",
        skip_all,
        err(Display)
    )]
    async fn table_exists_internal(&self, table_name: &str) -> ClickHouseStorageResult<bool> {
        let sql = format!(
            "SELECT 1 FROM system.tables WHERE database = '{}' AND name = '{}' LIMIT 1",
            Self::escape_clickhouse_string(&self.config.database),
            Self::escape_clickhouse_string(table_name)
        );

        let response = self.execute_query_with_response(&sql).await?;
        Ok(!response.trim().is_empty())
    }

    /// Export a ClickHouse table directly to R2/S3 as Parquet.
    ///
    /// Uses ClickHouse's `INSERT INTO FUNCTION s3(...)` to stream data directly
    /// to object storage without routing through Pond's memory. This is the
    /// preferred method for downgrade (hot → warm) as it:
    /// - Uses zero Pond memory regardless of table size
    /// - Avoids the JSON → Arrow → Parquet conversion overhead
    /// - Lets ClickHouse handle Parquet encoding natively
    ///
    /// Returns the number of rows exported (from ClickHouse's row count of the table).
    /// Validate that s3 export parameters don't contain characters that could
    /// break the SQL single-quoted string (defense-in-depth).
    fn validate_s3_export_params(
        s3_collection_name: &str,
        r2_key: &str,
    ) -> ClickHouseStorageResult<()> {
        if s3_collection_name.contains('\'') || s3_collection_name.contains('\\') {
            return Err(ClickHouseStorageError::Query(
                "s3_collection_name contains invalid characters".to_string(),
            ));
        }
        if r2_key.contains('\'') || r2_key.contains('\\') {
            return Err(ClickHouseStorageError::Query(
                "r2_key contains invalid characters".to_string(),
            ));
        }
        Ok(())
    }

    #[tracing::instrument(
        name = "warehouse.storage.ch.export_table_to_s3",
        skip_all,
        err(Display),
        fields(project_id = %project_id, table_name = table_name, database = %self.config.database)
    )]
    pub async fn export_table_to_s3(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
        s3_collection_name: &str,
        r2_key: &str,
    ) -> ClickHouseStorageResult<u64> {
        Self::validate_s3_export_params(s3_collection_name, r2_key)?;

        let ch_table_name = Self::source_table_name(project_id, source_name, table_name);

        // Get the row count first (to return to caller for bookkeeping).
        // Note: if the table is being written to concurrently, this count
        // may differ slightly from the actual exported rows.
        let row_count = self.get_table_row_count(&ch_table_name).await?;

        if row_count == 0 {
            tracing::info!(
                project_id = %project_id,
                source = source_name,
                table = table_name,
                "Table is empty, skipping s3 export"
            );
            return Ok(0);
        }

        // Use INSERT INTO FUNCTION s3() to have ClickHouse write directly to R2.
        // The named collection contains the R2 credentials securely.
        let sql = format!(
            "INSERT INTO FUNCTION s3({}, filename='{}', format='Parquet') \
             SELECT * FROM `{}`.`{}`",
            s3_collection_name,
            r2_key,
            self.config.database,
            ch_table_name,
        );

        self.execute_query(&sql).await?;

        tracing::info!(
            project_id = %project_id,
            source = source_name,
            table = table_name,
            rows = row_count,
            r2_key = r2_key,
            "Exported ClickHouse table directly to R2 Parquet via s3()"
        );

        Ok(row_count)
    }

    /// Get the row count for a ClickHouse table.
    #[tracing::instrument(
        name = "warehouse.clickhouse.get_table_row_count",
        skip_all,
        err(Display)
    )]
    async fn get_table_row_count(&self, ch_table_name: &str) -> ClickHouseStorageResult<u64> {
        let sql = format!(
            "SELECT count() FROM `{}`.`{}`",
            self.config.database, ch_table_name
        );
        let response = self.execute_query_with_response(&sql).await?;
        response
            .trim()
            .parse::<u64>()
            .map_err(|e| ClickHouseStorageError::Query(format!("Failed to parse row count: {}", e)))
    }

    /// List all source tables for a given project and source.
    #[tracing::instrument(
        name = "warehouse.storage.ch.list_source_tables",
        skip_all,
        err(Display),
        fields(project_id = %project_id, database = %self.config.database)
    )]
    pub async fn list_source_tables(
        &self,
        project_id: Uuid,
        source_name: &str,
    ) -> ClickHouseStorageResult<Vec<String>> {
        let prefix = format!(
            "warehouse_{}_{}",
            project_id.to_string().replace('-', "_"),
            Self::sanitize_identifier(source_name)
        );

        let escaped_prefix = prefix.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_");
        let sql = format!(
            "SELECT name FROM system.tables WHERE database = '{}' AND name LIKE '{}\\_%' ESCAPE '\\' AND name NOT LIKE '\\_staging\\_%' ESCAPE '\\'",
            Self::escape_clickhouse_string(&self.config.database), escaped_prefix
        );

        let response = self.execute_query_with_response(&sql).await?;
        let tables: Vec<String> = response
            .lines()
            .filter(|l| !l.is_empty())
            .map(|s| s.to_string())
            .collect();

        Ok(tables)
    }

    /// Import data from S3/R2 Parquet files into a ClickHouse table.
    /// 
    /// Used for upgrading from warm to hot tier.
    /// ClickHouse can directly read Parquet files from S3-compatible storage.
    /// 
    /// CRITICAL: The sync_checkpoint is NOT modified during upgrade.
    /// Parquet data represents a snapshot at checkpoint X; after import,
    /// ClickHouse has the same data at checkpoint X; future syncs continue from checkpoint X.
    #[tracing::instrument(
        name = "warehouse.storage.ch.import_from_s3",
        skip_all,
        err(Display),
        fields(project_id = %project_id, table_name = table_name, database = %self.config.database)
    )]
    pub async fn import_from_s3(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
        parquet_path: &str,
        collection_name: &str,
    ) -> ClickHouseStorageResult<u64> {
        let ch_table_name = Self::source_table_name(project_id, source_name, table_name);

        // Check if target table exists
        if !self.table_exists_internal(&ch_table_name).await? {
            return Err(ClickHouseStorageError::TableNotFound(format!(
                "Target table {} does not exist. Create it first.", ch_table_name
            )));
        }

        Self::validate_s3_export_params(collection_name, parquet_path)?;

        let sql = format!(
            "INSERT INTO `{}`.`{}` SELECT * FROM s3({}, '{}', 'Parquet')",
            self.config.database,
            ch_table_name,
            collection_name,
            parquet_path
        );

        // Capture row count before import to compute the delta.
        let count_before = self.source_row_count(project_id, source_name, table_name).await
            .unwrap_or(0);

        self.execute_query(&sql).await?;

        let count_after = self.source_row_count(project_id, source_name, table_name).await?;
        let rows_imported = count_after.saturating_sub(count_before);

        tracing::info!(
            project_id = %project_id,
            source = source_name,
            table = table_name,
            parquet_path = parquet_path,
            rows_imported = rows_imported,
            total_rows = count_after,
            "Imported Parquet from S3 into ClickHouse"
        );

        Ok(rows_imported)
    }

    /// Import Parquet data using Arrow RecordBatches.
    /// 
    /// This is an alternative to import_from_s3() that downloads the Parquet file,
    /// parses it as Arrow, and inserts via JSONEachRow. Useful when named collections
    /// are not configured or for testing.
    #[tracing::instrument(
        name = "warehouse.storage.ch.import_from_arrow",
        skip_all,
        err(Display),
        fields(project_id = %project_id, table_name = table_name, database = %self.config.database)
    )]
    pub async fn import_from_arrow(
        &self,
        project_id: Uuid,
        source_name: &str,
        table_name: &str,
        batches: &[RecordBatch],
    ) -> ClickHouseStorageResult<u64> {
        if batches.is_empty() {
            return Ok(0);
        }

        let ch_table_name = Self::source_table_name(project_id, source_name, table_name);

        // Check if target table exists
        if !self.table_exists_internal(&ch_table_name).await? {
            return Err(ClickHouseStorageError::TableNotFound(format!(
                "Target table {} does not exist. Create it first.", ch_table_name
            )));
        }

        let mut total_rows = 0u64;

        // Insert each batch
        for batch in batches {
            let rows = self.insert_source_batch(project_id, source_name, table_name, batch).await?;
            total_rows += rows;
        }

        tracing::info!(
            project_id = %project_id,
            source = source_name,
            table = table_name,
            rows = total_rows,
            "Imported Arrow batches into ClickHouse"
        );

        Ok(total_rows)
    }

    // ==================== Mutation Stats Methods ====================

    /// Create the `warehouse_mutation_stats` table using SummingMergeTree engine.
    /// This table tracks per-source, per-table daily mutation counts.
    /// SummingMergeTree automatically sums numeric columns for rows with the
    /// same sorting key during background merges.
    pub async fn create_mutation_stats_table(&self) -> ClickHouseStorageResult<()> {
        let sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS `{}`.warehouse_mutation_stats (
                project_id UUID,
                source_id UUID,
                table_name String,
                stat_date Date,
                insert_count UInt64,
                update_count UInt64,
                delete_count UInt64
            )
            ENGINE = SummingMergeTree()
            PARTITION BY toYYYYMM(stat_date)
            ORDER BY (project_id, source_id, table_name, stat_date)
            TTL stat_date + INTERVAL 90 DAY
            "#,
            self.config.database,
        );

        self.execute_query(&sql).await
    }

    /// Insert daily mutation counts for a source table.
    /// Each sync appends its counts; SummingMergeTree sums them automatically.
    pub async fn insert_mutation_stats(
        &self,
        project_id: Uuid,
        source_id: Uuid,
        table_name: &str,
        stat_date: chrono::NaiveDate,
        inserts: u64,
        updates: u64,
        deletes: u64,
    ) -> ClickHouseStorageResult<()> {
        if inserts == 0 && updates == 0 && deletes == 0 {
            return Ok(());
        }

        let sql = format!(
            "INSERT INTO `{}`.warehouse_mutation_stats \
             (project_id, source_id, table_name, stat_date, insert_count, update_count, delete_count) \
             VALUES ('{}', '{}', '{}', '{}', {}, {}, {})",
            self.config.database,
            project_id, source_id,
            Self::escape_clickhouse_string(table_name),
            stat_date.format("%Y-%m-%d"),
            inserts, updates, deletes,
        );

        self.execute_query(&sql).await
    }

    /// Query for sources exceeding the mutation threshold in the last N days.
    /// Returns a list of (source_id, table_name, total_updates, total_deletes).
    pub async fn query_high_churn_sources(
        &self,
        project_id: Uuid,
        days: u32,
        threshold: u64,
    ) -> ClickHouseStorageResult<Vec<HighChurnSource>> {
        let sql = format!(
            r#"
            SELECT
                source_id,
                table_name,
                sum(update_count) AS total_updates,
                sum(delete_count) AS total_deletes
            FROM `{}`.warehouse_mutation_stats
            WHERE project_id = '{}'
              AND stat_date >= today() - {}
            GROUP BY source_id, table_name
            HAVING total_updates + total_deletes > {}
            "#,
            self.config.database, project_id, days, threshold,
        );

        let response = self.execute_query_with_response(&sql).await?;

        let mut results = Vec::new();
        for line in response.lines() {
            let line = line.trim();
            if line.is_empty() { continue; }
            let cols: Vec<&str> = line.split('\t').collect();
            if cols.len() < 4 { continue; }
            let total_updates = cols[2].parse::<u64>().unwrap_or(0);
            let total_deletes = cols[3].parse::<u64>().unwrap_or(0);
            results.push(HighChurnSource {
                source_id: cols[0].to_string(),
                table_name: cols[1].to_string(),
                total_updates,
                total_deletes,
            });
        }

        Ok(results)
    }
}

/// A source that exceeds the mutation churn threshold.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct HighChurnSource {
    pub source_id: String,
    pub table_name: String,
    pub total_updates: u64,
    pub total_deletes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::warehouse::types::ColumnSchema;

    fn test_schema() -> TableSchema {
        TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("name", ColumnType::String, true),
                ColumnSchema::new("amount", ColumnType::Int64, false),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false)
                    .with_timezone("UTC"),
            ],
        }
    }

    #[test]
    fn test_table_name_generation() {
        let project_id = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
        let table_name = ClickHouseStorage::table_name(project_id, "customers");

        // Should contain project ID (with underscores instead of hyphens)
        assert!(table_name.contains("12345678_1234_1234_1234_123456789abc"));
        assert!(table_name.contains("customers"));
        assert!(table_name.starts_with("warehouse_"));
    }

    #[test]
    fn test_column_type_conversion() {
        assert_eq!(
            ClickHouseStorage::column_type_to_clickhouse(&ColumnType::String, false),
            "String"
        );
        assert_eq!(
            ClickHouseStorage::column_type_to_clickhouse(&ColumnType::String, true),
            "Nullable(String)"
        );
        assert_eq!(
            ClickHouseStorage::column_type_to_clickhouse(&ColumnType::Int64, false),
            "Int64"
        );
        assert_eq!(
            ClickHouseStorage::column_type_to_clickhouse(&ColumnType::Timestamp, false),
            "DateTime64(6)"
        );
    }

    #[test]
    fn test_infer_settings_with_id() {
        let schema = test_schema();

        let settings = ClickHouseStorage::infer_settings(&schema);

        // Should use id for ORDER BY
        assert_eq!(settings.order_by, vec!["id".to_string()]);
    }

    #[test]
    fn test_infer_settings_without_id() {
        let schema = TableSchema {
            columns: vec![
                ColumnSchema::new("event_type", ColumnType::String, false),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false)
                    .with_timezone("UTC"),
            ],
        };

        let settings = ClickHouseStorage::infer_settings(&schema);

        // Should use created_at for ORDER BY and PARTITION BY
        assert_eq!(settings.order_by, vec!["created_at".to_string()]);
        assert_eq!(
            settings.partition_by,
            Some("toYYYYMM(created_at)".to_string())
        );
    }

    // --- export_table_to_s3 validation tests ---

    #[test]
    fn test_export_rejects_r2_key_with_single_quote() {
        let result = ClickHouseStorage::validate_s3_export_params(
            "my_collection",
            "path/to'bad",
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("r2_key"));
    }

    #[test]
    fn test_export_rejects_r2_key_with_backslash() {
        let result = ClickHouseStorage::validate_s3_export_params(
            "my_collection",
            "path\\bad",
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("r2_key"));
    }

    #[test]
    fn test_export_rejects_s3_collection_with_single_quote() {
        let result = ClickHouseStorage::validate_s3_export_params(
            "my'collection",
            "valid/path/key.parquet",
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("s3_collection_name"));
    }

    #[test]
    fn test_export_rejects_s3_collection_with_backslash() {
        let result = ClickHouseStorage::validate_s3_export_params(
            "my\\collection",
            "valid/path/key.parquet",
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("s3_collection_name"));
    }

    #[test]
    fn test_export_accepts_valid_params() {
        let result = ClickHouseStorage::validate_s3_export_params(
            "r2_reiver_bucket",
            "projects/abc-123/warm/source/table/2024-01-01/part-uuid.parquet",
        );
        assert!(result.is_ok());
    }

    // ==================== sanitize_identifier Tests ====================

    #[test]
    fn test_sanitize_identifier_empty() {
        assert_eq!(ClickHouseStorage::sanitize_identifier(""), "_empty");
    }

    #[test]
    fn test_sanitize_identifier_special_chars() {
        assert_eq!(
            ClickHouseStorage::sanitize_identifier("my-table.name"),
            "my_table_name"
        );
    }

    #[test]
    fn test_sanitize_identifier_consecutive_underscores_collapsed() {
        assert_eq!(
            ClickHouseStorage::sanitize_identifier("a___b"),
            "a_b"
        );
    }

    #[test]
    fn test_sanitize_identifier_leading_digit() {
        let result = ClickHouseStorage::sanitize_identifier("123abc");
        assert!(result.starts_with('_'), "Leading digit should be prefixed with underscore");
        assert!(result.contains("123abc"));
    }

    #[test]
    fn test_sanitize_identifier_all_special_chars() {
        let result = ClickHouseStorage::sanitize_identifier("---");
        // All chars replaced with underscores, collapsed, then trailing trimmed
        assert_eq!(result, "_empty");
    }

    #[test]
    fn test_sanitize_identifier_already_clean() {
        assert_eq!(
            ClickHouseStorage::sanitize_identifier("valid_name_123"),
            "valid_name_123"
        );
    }

    #[test]
    fn test_sanitize_identifier_unicode() {
        // Non-ASCII chars should be replaced with underscores
        let result = ClickHouseStorage::sanitize_identifier("café");
        assert!(result.starts_with("caf"), "ASCII prefix should be kept");
        // é is non-ASCII so replaced
        assert!(!result.contains('é'));
    }

    #[test]
    fn test_sanitize_identifier_trailing_underscores_trimmed() {
        assert_eq!(
            ClickHouseStorage::sanitize_identifier("name___"),
            "name"
        );
    }

    // ==================== source_table_name / staging_table_name Tests ====================

    #[test]
    fn test_source_table_name_uuid_hyphens() {
        let project_id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let name = ClickHouseStorage::source_table_name(project_id, "my-source", "orders");
        assert!(name.starts_with("warehouse_"));
        assert!(name.contains("550e8400_e29b_41d4_a716_446655440000"));
        assert!(name.contains("my_source"));
        assert!(name.contains("orders"));
        // No hyphens should remain
        assert!(!name.contains('-'));
    }

    #[test]
    fn test_source_table_name_special_chars_in_names() {
        let project_id = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
        let name = ClickHouseStorage::source_table_name(project_id, "stripe.prod", "user-events");
        assert!(name.contains("stripe_prod"));
        assert!(name.contains("user_events"));
    }

    #[test]
    fn test_staging_table_name_wraps_source() {
        let project_id = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
        let source_name = ClickHouseStorage::source_table_name(project_id, "src", "tbl");
        let staging_name = ClickHouseStorage::staging_table_name(project_id, "src", "tbl");
        assert!(staging_name.starts_with("_staging_"));
        assert_eq!(staging_name, format!("_staging_{}", source_name));
    }

    // ==================== infer_settings Edge Cases ====================

    #[test]
    fn test_infer_settings_timestamp_only_no_id() {
        let schema = TableSchema {
            columns: vec![
                ColumnSchema::new("value", ColumnType::Float64, false),
                ColumnSchema::new("timestamp", ColumnType::Timestamp, false),
            ],
        };
        let settings = ClickHouseStorage::infer_settings(&schema);
        assert_eq!(settings.order_by, vec!["timestamp".to_string()]);
        assert_eq!(
            settings.partition_by,
            Some("toYYYYMM(timestamp)".to_string())
        );
    }

    #[test]
    fn test_infer_settings_no_temporal_columns() {
        let schema = TableSchema {
            columns: vec![
                ColumnSchema::new("name", ColumnType::String, false),
                ColumnSchema::new("value", ColumnType::Int64, false),
            ],
        };
        let settings = ClickHouseStorage::infer_settings(&schema);
        // No id, no created_at, no timestamp -> falls back to default tuple() ordering
        assert_eq!(settings.order_by, vec!["tuple()".to_string()]);
        assert!(settings.partition_by.is_none());
    }

    #[test]
    fn test_infer_settings_updated_at_not_used() {
        let schema = TableSchema {
            columns: vec![
                ColumnSchema::new("updated_at", ColumnType::Timestamp, false),
                ColumnSchema::new("data", ColumnType::String, false),
            ],
        };
        let settings = ClickHouseStorage::infer_settings(&schema);
        // Falls back to default tuple() ordering since updated_at is not recognized
        assert_eq!(settings.order_by, vec!["tuple()".to_string()]);
        assert!(settings.partition_by.is_none());
    }

    #[test]
    fn test_infer_settings_id_takes_precedence_over_timestamp() {
        let schema = TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::String, false),
                ColumnSchema::new("timestamp", ColumnType::Timestamp, false),
                ColumnSchema::new("created_at", ColumnType::Timestamp, false),
            ],
        };
        let settings = ClickHouseStorage::infer_settings(&schema);
        // id takes top priority for ORDER BY
        assert_eq!(settings.order_by, vec!["id".to_string()]);
        // When id is present, no PARTITION BY is set
        assert!(settings.partition_by.is_none());
    }

    #[test]
    fn test_build_fts_index_clauses_string_columns() {
        let schema = test_schema();
        let clauses = ClickHouseStorage::build_fts_index_clauses(&schema);
        assert_eq!(clauses.len(), 2);
        assert!(clauses[0].contains("idx_fts_id"));
        assert!(clauses[0].contains("tokenbf_v1"));
        assert!(clauses[1].contains("idx_fts_name"));
        assert!(clauses[1].contains("tokenbf_v1"));
    }

    #[test]
    fn test_build_fts_index_clauses_no_string_columns() {
        let schema = TableSchema {
            columns: vec![
                ColumnSchema::new("count", ColumnType::Int64, false),
                ColumnSchema::new("price", ColumnType::Float64, false),
            ],
        };
        let clauses = ClickHouseStorage::build_fts_index_clauses(&schema);
        assert!(clauses.is_empty());
    }

    #[test]
    fn test_build_fts_index_clauses_json_included() {
        let schema = TableSchema {
            columns: vec![
                ColumnSchema::new("id", ColumnType::Int64, false),
                ColumnSchema::new("metadata", ColumnType::Json, true),
            ],
        };
        let clauses = ClickHouseStorage::build_fts_index_clauses(&schema);
        assert_eq!(clauses.len(), 1);
        assert!(clauses[0].contains("idx_fts_metadata"));
    }

    #[test]
    fn test_fts_index_clause_format() {
        let schema = TableSchema {
            columns: vec![
                ColumnSchema::new("message", ColumnType::String, false),
            ],
        };
        let clauses = ClickHouseStorage::build_fts_index_clauses(&schema);
        assert_eq!(clauses.len(), 1);
        assert_eq!(
            clauses[0],
            "    INDEX idx_fts_message `message` TYPE tokenbf_v1(10240, 3, 0) GRANULARITY 4"
        );
    }

    #[test]
    fn test_timestamp_microsecond_to_json_not_null() {
        let us = 1705305600_123456_i64;
        let arr = TimestampMicrosecondArray::from(vec![Some(us)]);
        let array: Arc<dyn Array> = Arc::new(arr);

        assert!(!array.is_null(0));
        let downcasted = array.as_any().downcast_ref::<TimestampMicrosecondArray>();
        assert!(downcasted.is_some(), "TimestampMicrosecondArray must be handled, not fall through to null");

        let us_val = downcasted.unwrap().value(0);
        let secs = us_val.div_euclid(1_000_000);
        let nsecs = us_val.rem_euclid(1_000_000) as u32 * 1_000;
        let dt = chrono::DateTime::from_timestamp(secs, nsecs).expect("valid timestamp");
        let s = dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
        assert!(s.starts_with("2024-01-15"), "got: {s}");
        assert!(s.contains("123456"), "microsecond precision must be preserved, got: {s}");
    }

    #[test]
    fn test_timestamp_and_decimal_clickhouse_type_consistency() {
        assert_eq!(
            ClickHouseStorage::column_type_to_clickhouse(&ColumnType::Timestamp, false),
            ColumnType::Timestamp.to_clickhouse_type(),
            "column_type_to_clickhouse and to_clickhouse_type must agree for Timestamp"
        );
        assert_eq!(
            ClickHouseStorage::column_type_to_clickhouse(&ColumnType::Decimal, false),
            ColumnType::Decimal.to_clickhouse_type(),
            "column_type_to_clickhouse and to_clickhouse_type must agree for Decimal"
        );
    }

    #[test]
    fn test_staging_table_like_pattern_escaping() {
        let project_id = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
        let source_name = "my_source";

        // Build prefix the same way as drop_staging_tables
        let prefix = format!(
            "_staging_warehouse_{}_{}",
            project_id.to_string().replace('-', "_"),
            ClickHouseStorage::sanitize_identifier(source_name)
        );
        let escaped_prefix = prefix.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_");
        let like_sql = format!("name LIKE '{}%' ESCAPE '\\\\'", escaped_prefix);

        // Every underscore in the prefix must be preceded by a backslash
        let has_unescaped_underscore = escaped_prefix
            .char_indices()
            .any(|(i, c)| c == '_' && (i == 0 || escaped_prefix.as_bytes()[i - 1] != b'\\'));
        assert!(
            !has_unescaped_underscore,
            "all underscores in prefix must be escaped with \\, got: {}",
            escaped_prefix
        );

        // The trailing wildcard % in the SQL must be unescaped (not preceded by \)
        assert!(
            like_sql.contains("source%' ESCAPE"),
            "trailing % must be unescaped wildcard, got: {}",
            like_sql
        );

        // Verify drop_source_tables and drop_staging_tables use consistent escaping
        let source_prefix = format!(
            "warehouse_{}_{}",
            project_id.to_string().replace('-', "_"),
            ClickHouseStorage::sanitize_identifier(source_name)
        );
        let source_escaped = source_prefix.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_");
        let has_unescaped_percent = source_escaped
            .char_indices()
            .any(|(i, c)| c == '%' && (i == 0 || source_escaped.as_bytes()[i - 1] != b'\\'));
        assert!(
            !has_unescaped_percent,
            "source prefix escaping must also handle %, got: {}",
            source_escaped
        );
    }
}
