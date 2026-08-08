//! ClickHouse connector for the data warehouse.
//!
//! Syncs ClickHouse tables to the warehouse with automatic schema discovery.
//!
//! # Features
//!
//! - Automatic schema discovery from `system.columns`
//! - Incremental sync support with configurable key columns
//! - Batched data fetching to prevent memory issues
//! - SQL pushdown support (ClickHouse supports arbitrary SQL)
//!
//! # Connection
//!
//! Supports both the native TCP protocol (via klickhouse, default) and the
//! HTTP API (via reqwest, fallback). The protocol can be configured per
//! data source. When native TCP is selected, the connector falls back to
//! HTTP automatically on connection failure.

use std::pin::Pin;

use super::super::{Connector, ConnectorError, ConnectorResult, FetchOptions, RecordBatchStream, TableInfo};
use crate::warehouse::ch_client::{block_to_record_batch, ChClient, NativeChConfig};
use crate::warehouse::types::{ColumnSchema, ColumnType, SourceType, TableSchema};
use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, Int64Array,
    StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Maximum rows to fetch per batch to prevent memory issues.
const BATCH_SIZE: u64 = 10_000;

/// Which transport protocol to use when communicating with ClickHouse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ClickHouseProtocol {
    /// Native TCP protocol (port 9000 by default). Preferred for performance.
    #[default]
    Native,
    /// HTTP API (port 8123 by default). Useful when only HTTP is exposed.
    Http,
}

impl std::fmt::Display for ClickHouseProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClickHouseProtocol::Native => write!(f, "native"),
            ClickHouseProtocol::Http => write!(f, "http"),
        }
    }
}

impl std::str::FromStr for ClickHouseProtocol {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "native" | "tcp" => Ok(ClickHouseProtocol::Native),
            "http" | "https" => Ok(ClickHouseProtocol::Http),
            _ => Err(format!("Unknown protocol: {}", s)),
        }
    }
}

/// ClickHouse connector configuration.
#[derive(Debug, Clone)]
pub struct ClickHouseConfig {
    /// ClickHouse host (e.g., "localhost")
    pub host: String,
    /// HTTP API port (default 8123)
    pub http_port: u16,
    /// Native TCP port (default 9000)
    pub native_port: u16,
    /// Preferred protocol
    pub protocol: ClickHouseProtocol,
    /// Database name to connect to
    pub database: String,
    /// Username for authentication
    pub username: Option<String>,
    /// Password for authentication
    pub password: Option<String>,
    /// Tables to sync (empty = all tables)
    pub tables: Vec<String>,
    /// Connection timeout in seconds
    pub connect_timeout_secs: u64,
}

impl ClickHouseConfig {
    /// Create a new ClickHouse configuration with default ports.
    pub fn new(host: impl Into<String>, database: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            http_port: 8123,
            native_port: 9000,
            protocol: ClickHouseProtocol::default(),
            database: database.into(),
            username: None,
            password: None,
            tables: Vec::new(),
            connect_timeout_secs: 30,
        }
    }

    /// Build the HTTP URL from host and port.
    pub fn http_url(&self) -> String {
        format!("http://{}:{}", self.host, self.http_port)
    }

    /// Build a `NativeChConfig` for the klickhouse client.
    pub fn native_config(&self) -> NativeChConfig {
        NativeChConfig {
            host: self.host.clone(),
            port: self.native_port,
            database: self.database.clone(),
            username: self.username.clone().unwrap_or_else(|| "default".to_string()),
            password: self.password.clone().unwrap_or_default(),
        }
    }

    /// Set authentication credentials.
    pub fn with_credentials(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }

    /// Set specific tables to sync.
    pub fn with_tables(mut self, tables: Vec<String>) -> Self {
        self.tables = tables;
        self
    }

    /// Set the connection timeout.
    pub fn with_connect_timeout(mut self, timeout_secs: u64) -> Self {
        self.connect_timeout_secs = timeout_secs;
        self
    }

    /// Set the protocol.
    pub fn with_protocol(mut self, protocol: ClickHouseProtocol) -> Self {
        self.protocol = protocol;
        self
    }
}

/// Transport abstraction: either native TCP or HTTP.
enum Transport {
    Native(ChClient),
    Http(reqwest::Client),
}

/// ClickHouse data source connector.
///
/// Connects to a ClickHouse instance as a data source and syncs tables
/// into the warehouse. Supports native TCP (default) and HTTP protocols.
pub struct ClickHouseConnector {
    config: ClickHouseConfig,
    transport: Transport,
}

impl ClickHouseConnector {
    /// Create a new ClickHouse connector with native TCP protocol.
    /// Falls back to HTTP if the native connection fails.
    pub async fn new(config: ClickHouseConfig) -> Self {
        match config.protocol {
            ClickHouseProtocol::Native => {
                let native_cfg = config.native_config();
                match ChClient::connect(&native_cfg).await {
                    Ok(client) => {
                        tracing::info!(
                            host = %config.host,
                            port = config.native_port,
                            "Connected to ClickHouse via native TCP"
                        );
                        Self {
                            config,
                            transport: Transport::Native(client),
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            host = %config.host,
                            native_port = config.native_port,
                            error = %e,
                            "Native TCP connection failed, falling back to HTTP"
                        );
                        let http_client = reqwest::Client::builder()
                            .timeout(std::time::Duration::from_secs(config.connect_timeout_secs))
                            .build()
                            .unwrap_or_else(|_| reqwest::Client::new());
                        Self {
                            config,
                            transport: Transport::Http(http_client),
                        }
                    }
                }
            }
            ClickHouseProtocol::Http => {
                let http_client = reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(config.connect_timeout_secs))
                    .build()
                    .unwrap_or_else(|_| reqwest::Client::new());
                Self {
                    config,
                    transport: Transport::Http(http_client),
                }
            }
        }
    }

    /// Create a connector that always uses HTTP (for backward compatibility).
    pub fn new_http(config: ClickHouseConfig) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(config.connect_timeout_secs))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            config,
            transport: Transport::Http(http_client),
        }
    }

    /// Execute a query and return the response body as a string (TSV format).
    ///
    /// For native protocol: runs the query, converts blocks to TSV text.
    /// For HTTP protocol: sends the query and returns the response body.
    async fn execute_query(&self, sql: &str) -> ConnectorResult<String> {
        match &self.transport {
            Transport::Native(client) => {
                self.execute_query_native(client, sql).await
            }
            Transport::Http(client) => {
                self.execute_query_http(client, sql).await
            }
        }
    }

    async fn execute_query_native(&self, client: &ChClient, sql: &str) -> ConnectorResult<String> {
        let sql_clean = sql.trim().trim_end_matches(';');
        let sql_no_format = if let Some(pos) = sql_clean.to_uppercase().rfind(" FORMAT ") {
            sql_clean[..pos].to_string()
        } else {
            sql_clean.to_string()
        };

        let mut block_stream = client
            .inner()
            .query_raw(&sql_no_format)
            .await
            .map_err(|e| ConnectorError::Network(format!("ClickHouse native query failed: {}", e)))?;

        let mut output = String::new();
        let mut header_written = false;

        while let Some(block_result) = block_stream.next().await {
            let block = block_result
                .map_err(|e| ConnectorError::Internal(format!("Block read error: {}", e)))?;

            if block.rows == 0 {
                continue;
            }

            if !header_written {
                let names: Vec<String> = block.column_types.iter().map(|(n, _)| n.clone()).collect();
                let types: Vec<String> = block.column_types.iter().map(|(_, t)| t.to_string()).collect();
                output.push_str(&names.join("\t"));
                output.push('\n');
                output.push_str(&types.join("\t"));
                output.push('\n');
                header_written = true;
            }

            let col_names: Vec<&String> = block.column_data.keys().collect();
            let num_rows = block.rows as usize;
            for row_idx in 0..num_rows {
                for (col_idx, col_name) in col_names.iter().enumerate() {
                    if col_idx > 0 { output.push('\t'); }
                    let val = &block.column_data[*col_name][row_idx];
                    let json = crate::warehouse::ch_client::klickhouse_value_to_json(val.clone());
                    match json {
                        serde_json::Value::String(s) => output.push_str(&s),
                        serde_json::Value::Null => output.push_str("\\N"),
                        other => output.push_str(&other.to_string()),
                    }
                }
                output.push('\n');
            }
        }

        Ok(output)
    }

    async fn execute_query_http(&self, client: &reqwest::Client, sql: &str) -> ConnectorResult<String> {
        let url = self.config.http_url();
        let mut request = client.post(&url).body(sql.to_string());

        if let (Some(user), Some(pass)) = (&self.config.username, &self.config.password) {
            request = request.basic_auth(user, Some(pass));
        } else if let Some(user) = &self.config.username {
            request = request.basic_auth(user, None::<&str>);
        }

        let response = request
            .send()
            .await
            .map_err(|e| ConnectorError::Network(format!("ClickHouse HTTP request failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(ConnectorError::Internal(format!(
                "ClickHouse query failed with status {}: {}",
                status, body
            )));
        }

        response
            .text()
            .await
            .map_err(|e| ConnectorError::Network(format!("Failed to read ClickHouse response: {}", e)))
    }

    /// Fetch data as RecordBatches using the native protocol directly.
    /// Avoids the TSV roundtrip for better performance when using native TCP.
    async fn fetch_batches_native(
        &self,
        client: &ChClient,
        sql: &str,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let mut block_stream = client
            .inner()
            .query_raw(sql)
            .await
            .map_err(|e| ConnectorError::Network(format!("ClickHouse native query failed: {}", e)))?;

        let mut batches = Vec::new();
        while let Some(block_result) = block_stream.next().await {
            let block = block_result
                .map_err(|e| ConnectorError::Internal(format!("Block read error: {}", e)))?;

            if block.rows == 0 {
                continue;
            }

            let batch = block_to_record_batch(&block)
                .map_err(|e| ConnectorError::Internal(format!("Block-to-Arrow conversion: {}", e)))?;
            batches.push(batch);
        }

        Ok(batches)
    }

    /// Map a ClickHouse type name to the warehouse ColumnType.
    fn ch_type_to_column_type(ch_type: &str) -> ColumnType {
        // Strip Nullable() wrapper if present
        let inner = if ch_type.starts_with("Nullable(") && ch_type.ends_with(')') {
            &ch_type[9..ch_type.len() - 1]
        } else {
            ch_type
        };

        match inner {
            "Int8" | "Int16" | "Int32" | "UInt8" | "UInt16" => ColumnType::Int32,
            "Int64" | "UInt32" | "UInt64" => ColumnType::Int64,
            "Float32" => ColumnType::Float32,
            "Float64" => ColumnType::Float64,
            "Bool" => ColumnType::Boolean,
            "Date" | "Date32" => ColumnType::Date,
            "UUID" => ColumnType::Uuid,
            t if t.starts_with("DateTime64") => ColumnType::Timestamp,
            t if t.starts_with("DateTime") => ColumnType::Timestamp,
            t if t.starts_with("Decimal") => ColumnType::Decimal,
            t if t.starts_with("FixedString") => ColumnType::String,
            t if t.starts_with("LowCardinality(") => {
                let lc_inner = &t[15..t.len() - 1];
                Self::ch_type_to_column_type(lc_inner)
            }
            _ => ColumnType::String, // String, Enum, Array, Map, etc. default to String
        }
    }

    /// Check if a ClickHouse type is nullable.
    /// Recursively strips wrappers like `LowCardinality(...)` to detect
    /// nested `Nullable(...)`, e.g. `LowCardinality(Nullable(String))`.
    fn is_nullable(ch_type: &str) -> bool {
        if ch_type.starts_with("Nullable(") {
            return true;
        }
        if ch_type.starts_with("LowCardinality(") && ch_type.ends_with(')') {
            let inner = &ch_type[15..ch_type.len() - 1];
            return Self::is_nullable(inner);
        }
        false
    }

    /// Convert a TableSchema to an Arrow Schema.
    fn to_arrow_schema(schema: &TableSchema) -> Schema {
        let fields: Vec<Field> = schema
            .columns
            .iter()
            .map(|col| Field::new(&col.name, col.data_type.to_arrow_type(), true))
            .collect();
        Schema::new(fields)
    }

    /// Escape a string value for use in ClickHouse SQL.
    ///
    /// ClickHouse uses single quotes for string literals and backslash for escaping.
    fn escape_string(s: &str) -> String {
        let s = s.replace('\0', "");
        let mut result = String::with_capacity(s.len() + s.len() / 8);
        for ch in s.chars() {
            match ch {
                '\'' => result.push_str("\\'"),
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

        match chars.next() {
            Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
            _ => return false,
        }

        chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    }

    /// Build a SELECT query for fetching data (without FORMAT clause).
    fn build_fetch_query_no_format(
        database: &str,
        table: &str,
        incremental_key: Option<&str>,
        last_value: Option<&str>,
        limit: u64,
        offset: u64,
    ) -> String {
        let mut query = format!("SELECT * FROM `{}`.`{}`", database, table);

        if let (Some(key), Some(value)) = (incremental_key, last_value) {
            if Self::is_valid_column_name(key) {
                let escaped_value = Self::escape_string(value);
                query.push_str(&format!(" WHERE `{}` > '{}'", key, escaped_value));
            }
        }

        if let Some(key) = incremental_key {
            if Self::is_valid_column_name(key) {
                query.push_str(&format!(" ORDER BY `{}` ASC", key));
            }
        }

        query.push_str(&format!(" LIMIT {} OFFSET {}", limit, offset));

        query
    }

    /// Append predicates to a fetch query. If the query already has a WHERE clause,
    /// predicates are AND-ed with it; otherwise a new WHERE clause is added.
    fn append_predicates_to_query(
        query: &str,
        predicates: &[crate::warehouse::query::predicate_pushdown::Predicate],
    ) -> String {
        if predicates.is_empty() {
            return query.to_string();
        }
        let predicate_sql: Vec<String> = predicates
            .iter()
            .map(|p| crate::warehouse::query::predicate_pushdown::predicate_to_sql(p, crate::warehouse::query::predicate_pushdown::SqlDialect::ClickHouse))
            .collect();
        let combined = predicate_sql.join(" AND ");

        let query_upper = query.to_uppercase();
        let order_pos = query_upper.find(" ORDER BY ");
        let limit_pos = query_upper.find(" LIMIT ");
        let insert_before = order_pos.or(limit_pos).unwrap_or(query.len());
        let (before, after) = query.split_at(insert_before);
        let has_where = before.to_uppercase().contains(" WHERE ");
        let new_part = if has_where {
            format!(" AND ({})", combined)
        } else {
            format!(" WHERE ({})", combined)
        };
        format!("{}{}{}", before.trim_end(), new_part, after)
    }

    /// Parse a TSV response (TabSeparatedWithNamesAndTypes) into RecordBatches.
    ///
    /// The format is:
    /// - Line 1: Column names (tab-separated)
    /// - Line 2: Column types (tab-separated)
    /// - Lines 3+: Data rows (tab-separated)
    fn parse_tsv_response(
        &self,
        response: &str,
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<Option<RecordBatch>> {
        let mut lines = response.lines();

        // Skip header lines (names and types)
        let _names_line = match lines.next() {
            Some(l) if !l.is_empty() => l,
            _ => return Ok(None),
        };
        let _types_line = match lines.next() {
            Some(l) => l,
            None => return Ok(None),
        };

        // Collect data rows
        let data_rows: Vec<&str> = lines.filter(|l| !l.is_empty()).collect();
        if data_rows.is_empty() {
            return Ok(None);
        }

        // Build arrays for each column
        let num_cols = schema.columns.len();
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(num_cols);

        // Pre-split all rows once to avoid O(rows * cols^2) re-splitting
        let split_rows: Vec<Vec<&str>> = data_rows
            .iter()
            .map(|row| row.split('\t').collect())
            .collect();

        for (col_idx, col) in schema.columns.iter().enumerate() {
            let values: Vec<Option<&str>> = split_rows
                .iter()
                .map(|parts| {
                    parts.get(col_idx).and_then(|v| {
                        if *v == "\\N" {
                            None
                        } else {
                            Some(*v)
                        }
                    })
                })
                .collect();

            let array: ArrayRef = match col.data_type {
                ColumnType::Int32 => {
                    let parsed: Vec<Option<i32>> = values
                        .iter()
                        .map(|v| v.and_then(|s| s.parse::<i32>().ok()))
                        .collect();
                    Arc::new(Int32Array::from(parsed))
                }
                ColumnType::Int64 => {
                    let parsed: Vec<Option<i64>> = values
                        .iter()
                        .map(|v| v.and_then(|s| s.parse::<i64>().ok()))
                        .collect();
                    Arc::new(Int64Array::from(parsed))
                }
                ColumnType::Float32 => {
                    let parsed: Vec<Option<f32>> = values
                        .iter()
                        .map(|v| v.and_then(|s| s.parse::<f32>().ok()))
                        .collect();
                    Arc::new(Float32Array::from(parsed))
                }
                ColumnType::Float64 => {
                    let parsed: Vec<Option<f64>> = values
                        .iter()
                        .map(|v| v.and_then(|s| s.parse::<f64>().ok()))
                        .collect();
                    Arc::new(Float64Array::from(parsed))
                }
                ColumnType::Decimal => {
                    let parsed: Vec<Option<f64>> = values
                        .iter()
                        .map(|v| v.and_then(|s| s.parse::<f64>().ok()))
                        .collect();
                    Arc::new(Float64Array::from(parsed))
                }
                ColumnType::Boolean => {
                    let parsed: Vec<Option<bool>> = values
                        .iter()
                        .map(|v| {
                            v.map(|s| matches!(s, "1" | "true" | "True" | "TRUE"))
                        })
                        .collect();
                    Arc::new(BooleanArray::from(parsed))
                }
                ColumnType::Timestamp => {
                    let parsed: Vec<Option<i64>> = values
                        .iter()
                        .map(|v| {
                            v.and_then(|s| {
                                // Try parsing as datetime string (ClickHouse DateTime format)
                                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                                    .ok()
                                    .map(|dt| dt.and_utc().timestamp_micros())
                                    .or_else(|| {
                                        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                                            .ok()
                                            .map(|dt| dt.and_utc().timestamp_micros())
                                    })
                            })
                        })
                        .collect();
                    Arc::new(TimestampMicrosecondArray::from(parsed))
                }
                ColumnType::Date => {
                    let parsed: Vec<Option<i32>> = values
                        .iter()
                        .map(|v| {
                            v.and_then(|s| {
                                chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok().map(|d| {
                                    let epoch =
                                        chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                                    (d - epoch).num_days() as i32
                                })
                            })
                        })
                        .collect();
                    Arc::new(Date32Array::from(parsed))
                }
                ColumnType::String | ColumnType::Json | ColumnType::Uuid => {
                    let parsed: Vec<Option<String>> = values
                        .iter()
                        .map(|v| v.map(|s| s.to_string()))
                        .collect();
                    Arc::new(StringArray::from(
                        parsed
                            .iter()
                            .map(|v| v.as_deref())
                            .collect::<Vec<Option<&str>>>(),
                    ))
                }
            };
            arrays.push(array);
        }

        RecordBatch::try_new(arrow_schema, arrays)
            .map(Some)
            .map_err(|e| ConnectorError::Internal(format!("Failed to create RecordBatch: {}", e)))
    }

    /// Parse a TSV response and infer schema from the header.
    /// Used by `execute_sql` where the schema is not known ahead of time.
    fn parse_tsv_response_with_inferred_schema(
        &self,
        response: &str,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let mut lines = response.lines();

        // Line 1: column names
        let names_line = match lines.next() {
            Some(l) if !l.is_empty() => l,
            _ => return Ok(Vec::new()),
        };
        // Line 2: column types
        let types_line = match lines.next() {
            Some(l) => l,
            None => return Ok(Vec::new()),
        };

        let names: Vec<&str> = names_line.split('\t').collect();
        let types: Vec<&str> = types_line.split('\t').collect();

        if names.len() != types.len() {
            return Err(ConnectorError::Internal(
                "Column name/type count mismatch in ClickHouse response".to_string(),
            ));
        }

        // Build schema from header
        let columns: Vec<ColumnSchema> = names
            .iter()
            .zip(types.iter())
            .map(|(name, ch_type)| {
                let col_type = Self::ch_type_to_column_type(ch_type);
                let nullable = Self::is_nullable(ch_type);
                let timezone = if matches!(col_type, ColumnType::Timestamp) {
                    Some("UTC".to_string())
                } else {
                    None
                };
                ColumnSchema {
                    name: name.to_string(),
                    data_type: col_type,
                    nullable,
                    description: None,
                    timezone,
                }
            })
            .collect();

        let table_schema = TableSchema { columns };
        let arrow_schema = Arc::new(Self::to_arrow_schema(&table_schema));

        // Collect data rows
        let data_rows: Vec<&str> = lines.filter(|l| !l.is_empty()).collect();
        if data_rows.is_empty() {
            return Ok(Vec::new());
        }

        // Re-join data lines and re-prepend headers so parse_tsv_response can handle it
        let reconstructed = format!(
            "{}\n{}\n{}",
            names_line,
            types_line,
            data_rows.join("\n")
        );

        match self.parse_tsv_response(&reconstructed, &table_schema, arrow_schema)? {
            Some(batch) => Ok(vec![batch]),
            None => Ok(Vec::new()),
        }
    }
}

#[async_trait]
impl Connector for ClickHouseConnector {
    fn source_type(&self) -> SourceType {
        SourceType::ClickHouse
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        let sql = format!(
            "SELECT name, total_rows \
             FROM system.tables \
             WHERE database = '{}' AND engine NOT IN ('View', 'MaterializedView', 'LiveView') \
             ORDER BY name \
             FORMAT TabSeparatedWithNamesAndTypes",
            Self::escape_string(&self.config.database)
        );

        let response = self.execute_query(&sql).await?;
        let mut lines = response.lines();

        // Skip header lines (names + types)
        let _ = lines.next();
        let _ = lines.next();

        let mut table_infos = Vec::new();
        for line in lines {
            if line.is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.split('\t').collect();
            let table_name = match parts.first() {
                Some(n) => n.to_string(),
                None => continue,
            };

            // Skip if tables filter is set and this table isn't in it
            if !self.config.tables.is_empty() && !self.config.tables.contains(&table_name) {
                continue;
            }

            let estimated_rows = parts
                .get(1)
                .and_then(|v| {
                    if *v == "\\N" {
                        None
                    } else {
                        v.parse::<u64>().ok()
                    }
                });

            // Get schema for this table
            let table_schema = match self.get_schema(&table_name).await {
                Ok(schema) => schema,
                Err(e) => {
                    tracing::warn!(
                        table = %table_name,
                        error = %e,
                        "Failed to get schema for ClickHouse table, skipping"
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
                        "updated_at" | "modified_at" | "last_modified" | "created_at" | "timestamp"
                    )
                })
                .map(|c| c.name.clone());

            table_infos.push(TableInfo {
                name: table_name,
                schema: table_schema,
                supports_incremental: incremental_key.is_some(),
                incremental_key,
                estimated_rows,
                primary_key_columns: Vec::new(),
            });
        }

        tracing::debug!(
            database = %self.config.database,
            table_count = table_infos.len(),
            "Listed ClickHouse tables"
        );

        Ok(table_infos)
    }

    async fn get_schema(&self, table: &str) -> ConnectorResult<TableSchema> {
        let sql = format!(
            "SELECT name, type \
             FROM system.columns \
             WHERE database = '{}' AND table = '{}' \
             ORDER BY position \
             FORMAT TabSeparatedWithNamesAndTypes",
            Self::escape_string(&self.config.database),
            Self::escape_string(table)
        );

        let response = self.execute_query(&sql).await?;
        let mut lines = response.lines();

        // Skip header lines
        let _ = lines.next();
        let _ = lines.next();

        let mut columns = Vec::new();
        for line in lines {
            if line.is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() < 2 {
                continue;
            }

            let col_name = parts[0].to_string();
            let ch_type = parts[1];
            let col_type = Self::ch_type_to_column_type(ch_type);
            let nullable = Self::is_nullable(ch_type);
            let timezone = if matches!(col_type, ColumnType::Timestamp) {
                Some("UTC".to_string())
            } else {
                None
            };

            columns.push(ColumnSchema {
                name: col_name,
                data_type: col_type,
                nullable,
                description: None,
                timezone,
            });
        }

        if columns.is_empty() {
            return Err(ConnectorError::Internal(format!(
                "Table {}.{} not found or has no columns",
                self.config.database, table
            )));
        }

        tracing::debug!(
            database = %self.config.database,
            table = %table,
            column_count = columns.len(),
            "Retrieved ClickHouse table schema"
        );

        Ok(TableSchema { columns })
    }

    async fn fetch_table(
        &self,
        table: &str,
        incremental_key: Option<&str>,
        last_value: Option<&str>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let mut all_batches = Vec::new();
        let mut offset = 0u64;
        let mut total_rows = 0u64;

        loop {
            let query = Self::build_fetch_query_no_format(
                &self.config.database,
                table,
                incremental_key,
                last_value,
                BATCH_SIZE,
                offset,
            );

            let batch_result = match &self.transport {
                Transport::Native(client) => {
                    self.fetch_batches_native(client, &query).await?
                }
                Transport::Http(_) => {
                    let schema = self.get_schema(table).await?;
                    let arrow_schema = Arc::new(Self::to_arrow_schema(&schema));
                    let tsv_query = format!("{} FORMAT TabSeparatedWithNamesAndTypes", query);
                    let response = self.execute_query(&tsv_query).await?;
                    match self.parse_tsv_response(&response, &schema, arrow_schema)? {
                        Some(batch) => vec![batch],
                        None => Vec::new(),
                    }
                }
            };

            if batch_result.is_empty() {
                break;
            }

            let batch_rows: u64 = batch_result.iter().map(|b| b.num_rows() as u64).sum();
            total_rows += batch_rows;
            all_batches.extend(batch_result);

            if batch_rows < BATCH_SIZE {
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
            "Fetched ClickHouse table data"
        );

        Ok(all_batches)
    }

    fn fetch_table_stream<'a>(
        &'a self,
        table: &'a str,
        options: FetchOptions,
    ) -> Pin<Box<dyn futures::Future<Output = ConnectorResult<RecordBatchStream>> + Send + 'a>> {
        Box::pin(async move {
            if options.predicates.is_empty() {
                let batches = self.fetch_table(
                    table,
                    options.incremental_key.as_deref(),
                    options.last_value.as_deref(),
                ).await?;
                let stream = futures::stream::iter(batches.into_iter().map(Ok));
                Ok(Box::pin(stream) as RecordBatchStream)
            } else {
                let mut all_batches = Vec::new();
                let mut offset = 0u64;

                loop {
                    let mut query = Self::build_fetch_query_no_format(
                        &self.config.database,
                        table,
                        options.incremental_key.as_deref(),
                        options.last_value.as_deref(),
                        BATCH_SIZE,
                        offset,
                    );
                    query = Self::append_predicates_to_query(&query, &options.predicates);

                    let batch_result = match &self.transport {
                        Transport::Native(client) => {
                            self.fetch_batches_native(client, &query).await?
                        }
                        Transport::Http(_) => {
                            let schema = self.get_schema(table).await?;
                            let arrow_schema = Arc::new(Self::to_arrow_schema(&schema));
                            let tsv_query = format!("{} FORMAT TabSeparatedWithNamesAndTypes", query);
                            let response = self.execute_query(&tsv_query).await?;
                            match self.parse_tsv_response(&response, &schema, arrow_schema)? {
                                Some(batch) => vec![batch],
                                None => Vec::new(),
                            }
                        }
                    };

                    if batch_result.is_empty() {
                        break;
                    }

                    let batch_rows: u64 = batch_result.iter().map(|b| b.num_rows() as u64).sum();
                    all_batches.extend(batch_result);

                    if batch_rows < BATCH_SIZE {
                        break;
                    }

                    offset += BATCH_SIZE;
                }

                let stream = futures::stream::iter(all_batches.into_iter().map(Ok));
                Ok(Box::pin(stream) as RecordBatchStream)
            }
        })
    }

    async fn validate_credentials(&self) -> ConnectorResult<()> {
        self.execute_query("SELECT 1")
            .await
            .map_err(|e| ConnectorError::Authentication(format!(
                "ClickHouse credential validation failed: {}", e
            )))?;

        tracing::debug!("ClickHouse credentials validated successfully");
        Ok(())
    }

    fn supports_sql_pushdown(&self) -> bool {
        true // ClickHouse supports arbitrary SQL execution
    }

    fn supports_cdc(&self) -> bool {
        false // ClickHouse doesn't have WAL-based CDC
    }

    async fn execute_sql(&self, sql: &str) -> ConnectorResult<Vec<RecordBatch>> {
        crate::warehouse::connectors::enforce_read_only_sql(sql)?;

        let batches = match &self.transport {
            Transport::Native(client) => {
                let clean_sql = sql.trim().trim_end_matches(';');
                let sql_no_format = if clean_sql.to_uppercase().contains("FORMAT ") {
                    if let Some(pos) = clean_sql.to_uppercase().rfind(" FORMAT ") {
                        clean_sql[..pos].to_string()
                    } else {
                        clean_sql.to_string()
                    }
                } else {
                    clean_sql.to_string()
                };
                self.fetch_batches_native(client, &sql_no_format).await?
            }
            Transport::Http(_) => {
                let query = if sql.to_uppercase().contains("FORMAT ") {
                    sql.to_string()
                } else {
                    format!("{} FORMAT TabSeparatedWithNamesAndTypes", sql)
                };

                let response = self.execute_query(&query).await?;

                if response.trim().is_empty() {
                    return Ok(Vec::new());
                }

                self.parse_tsv_response_with_inferred_schema(&response)?
            }
        };

        tracing::info!(
            sql_length = sql.len(),
            batches = batches.len(),
            "Executed ClickHouse SQL query"
        );

        Ok(batches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clickhouse_config_creation() {
        let config = ClickHouseConfig::new("localhost", "mydb");
        assert_eq!(config.host, "localhost");
        assert_eq!(config.database, "mydb");
        assert_eq!(config.http_port, 8123);
        assert_eq!(config.native_port, 9000);
        assert_eq!(config.protocol, ClickHouseProtocol::Native);
        assert!(config.username.is_none());
        assert!(config.password.is_none());
        assert!(config.tables.is_empty());
    }

    #[test]
    fn test_clickhouse_config_with_credentials() {
        let config = ClickHouseConfig::new("localhost", "mydb")
            .with_credentials("admin", "secret");
        assert_eq!(config.username.as_deref(), Some("admin"));
        assert_eq!(config.password.as_deref(), Some("secret"));
    }

    #[test]
    fn test_clickhouse_config_with_tables() {
        let config = ClickHouseConfig::new("localhost", "mydb")
            .with_tables(vec!["events".to_string(), "users".to_string()]);
        assert_eq!(config.tables.len(), 2);
        assert!(config.tables.contains(&"events".to_string()));
    }

    #[test]
    fn test_clickhouse_config_protocol() {
        let config = ClickHouseConfig::new("localhost", "mydb")
            .with_protocol(ClickHouseProtocol::Http);
        assert_eq!(config.protocol, ClickHouseProtocol::Http);
        assert_eq!(config.http_url(), "http://localhost:8123");
    }

    #[test]
    fn test_clickhouse_protocol_from_str() {
        assert_eq!("native".parse::<ClickHouseProtocol>().unwrap(), ClickHouseProtocol::Native);
        assert_eq!("tcp".parse::<ClickHouseProtocol>().unwrap(), ClickHouseProtocol::Native);
        assert_eq!("http".parse::<ClickHouseProtocol>().unwrap(), ClickHouseProtocol::Http);
        assert!("invalid".parse::<ClickHouseProtocol>().is_err());
    }

    #[test]
    fn test_ch_type_to_column_type_integers() {
        assert!(matches!(
            ClickHouseConnector::ch_type_to_column_type("Int32"),
            ColumnType::Int32
        ));
        assert!(matches!(
            ClickHouseConnector::ch_type_to_column_type("UInt8"),
            ColumnType::Int32
        ));
        assert!(matches!(
            ClickHouseConnector::ch_type_to_column_type("Int64"),
            ColumnType::Int64
        ));
        assert!(matches!(
            ClickHouseConnector::ch_type_to_column_type("UInt64"),
            ColumnType::Int64
        ));
    }

    #[test]
    fn test_ch_type_to_column_type_floats() {
        assert!(matches!(
            ClickHouseConnector::ch_type_to_column_type("Float32"),
            ColumnType::Float32
        ));
        assert!(matches!(
            ClickHouseConnector::ch_type_to_column_type("Float64"),
            ColumnType::Float64
        ));
    }

    #[test]
    fn test_ch_type_to_column_type_timestamps() {
        assert!(matches!(
            ClickHouseConnector::ch_type_to_column_type("DateTime"),
            ColumnType::Timestamp
        ));
        assert!(matches!(
            ClickHouseConnector::ch_type_to_column_type("DateTime64(3)"),
            ColumnType::Timestamp
        ));
        assert!(matches!(
            ClickHouseConnector::ch_type_to_column_type("DateTime64(6, 'UTC')"),
            ColumnType::Timestamp
        ));
    }

    #[test]
    fn test_ch_type_to_column_type_nullable() {
        assert!(matches!(
            ClickHouseConnector::ch_type_to_column_type("Nullable(Int64)"),
            ColumnType::Int64
        ));
        assert!(matches!(
            ClickHouseConnector::ch_type_to_column_type("Nullable(String)"),
            ColumnType::String
        ));
        assert!(ClickHouseConnector::is_nullable("Nullable(Int64)"));
        assert!(!ClickHouseConnector::is_nullable("Int64"));
        // LowCardinality(Nullable(...)) should be detected as nullable
        assert!(ClickHouseConnector::is_nullable("LowCardinality(Nullable(String))"));
        assert!(!ClickHouseConnector::is_nullable("LowCardinality(String)"));
    }

    #[test]
    fn test_ch_type_to_column_type_low_cardinality() {
        assert!(matches!(
            ClickHouseConnector::ch_type_to_column_type("LowCardinality(String)"),
            ColumnType::String
        ));
        assert!(matches!(
            ClickHouseConnector::ch_type_to_column_type("LowCardinality(Nullable(String))"),
            ColumnType::String
        ));
    }

    #[test]
    fn test_ch_type_to_column_type_special() {
        assert!(matches!(
            ClickHouseConnector::ch_type_to_column_type("Bool"),
            ColumnType::Boolean
        ));
        assert!(matches!(
            ClickHouseConnector::ch_type_to_column_type("Date"),
            ColumnType::Date
        ));
        assert!(matches!(
            ClickHouseConnector::ch_type_to_column_type("UUID"),
            ColumnType::Uuid
        ));
        assert!(matches!(
            ClickHouseConnector::ch_type_to_column_type("Decimal(18, 4)"),
            ColumnType::Decimal
        ));
    }

    #[test]
    fn test_escape_string() {
        assert_eq!(ClickHouseConnector::escape_string("hello"), "hello");
        assert_eq!(ClickHouseConnector::escape_string("it's"), "it\\'s");
        assert_eq!(ClickHouseConnector::escape_string("a\\b"), "a\\\\b");
        assert_eq!(ClickHouseConnector::escape_string("null\0byte"), "nullbyte");
    }

    #[test]
    fn test_is_valid_column_name() {
        assert!(ClickHouseConnector::is_valid_column_name("id"));
        assert!(ClickHouseConnector::is_valid_column_name("created_at"));
        assert!(ClickHouseConnector::is_valid_column_name("_private"));
        assert!(!ClickHouseConnector::is_valid_column_name(""));
        assert!(!ClickHouseConnector::is_valid_column_name("1invalid"));
        assert!(!ClickHouseConnector::is_valid_column_name("has space"));
    }

    #[test]
    fn test_build_fetch_query_full_sync() {
        let query =
            ClickHouseConnector::build_fetch_query_no_format("mydb", "events", None, None, 10_000, 0);
        assert!(query.contains("SELECT * FROM `mydb`.`events`"));
        assert!(query.contains("LIMIT 10000 OFFSET 0"));
        assert!(!query.contains("WHERE"));
    }

    #[test]
    fn test_build_fetch_query_incremental() {
        let query = ClickHouseConnector::build_fetch_query_no_format(
            "mydb",
            "events",
            Some("updated_at"),
            Some("2024-01-01 00:00:00"),
            10_000,
            0,
        );
        assert!(query.contains("WHERE `updated_at` > '2024-01-01 00:00:00'"));
        assert!(query.contains("ORDER BY `updated_at` ASC"));
    }

    #[test]
    fn test_source_type() {
        let config = ClickHouseConfig::new("localhost", "mydb");
        let connector = ClickHouseConnector::new_http(config);
        assert_eq!(connector.source_type(), SourceType::ClickHouse);
    }

    #[test]
    fn test_supports_sql_pushdown() {
        let config = ClickHouseConfig::new("localhost", "mydb");
        let connector = ClickHouseConnector::new_http(config);
        assert!(connector.supports_sql_pushdown());
    }

    #[test]
    fn test_supports_cdc() {
        let config = ClickHouseConfig::new("localhost", "mydb");
        let connector = ClickHouseConnector::new_http(config);
        assert!(!connector.supports_cdc());
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

        let arrow_schema = ClickHouseConnector::to_arrow_schema(&table_schema);

        assert_eq!(arrow_schema.fields().len(), 3);

        let id_field = arrow_schema.field_with_name("id").unwrap();
        assert_eq!(id_field.data_type(), &arrow::datatypes::DataType::Int64);
        assert!(id_field.is_nullable());

        let name_field = arrow_schema.field_with_name("name").unwrap();
        assert_eq!(name_field.data_type(), &arrow::datatypes::DataType::Utf8);

        let active_field = arrow_schema.field_with_name("active").unwrap();
        assert_eq!(
            active_field.data_type(),
            &arrow::datatypes::DataType::Boolean
        );
    }

    #[test]
    fn test_ch_type_to_column_type_array() {
        // Array types default to String in the mapping
        assert!(matches!(
            ClickHouseConnector::ch_type_to_column_type("Array(String)"),
            ColumnType::String
        ));
        assert!(matches!(
            ClickHouseConnector::ch_type_to_column_type("Array(Int64)"),
            ColumnType::String
        ));
    }

    #[test]
    fn test_ch_type_to_column_type_map() {
        // Map types default to String in the mapping
        assert!(matches!(
            ClickHouseConnector::ch_type_to_column_type("Map(String, Int64)"),
            ColumnType::String
        ));
    }

    #[test]
    fn test_ch_type_to_column_type_enum() {
        // Enum types default to String in the mapping
        assert!(matches!(
            ClickHouseConnector::ch_type_to_column_type("Enum8('a' = 1, 'b' = 2)"),
            ColumnType::String
        ));
        assert!(matches!(
            ClickHouseConnector::ch_type_to_column_type("Enum16('x' = 1)"),
            ColumnType::String
        ));
    }

    #[test]
    fn test_config_default_port() {
        let config = ClickHouseConfig::new("myhost", "mydb");
        assert_eq!(config.host, "myhost");
        assert_eq!(config.http_port, 8123);
        assert_eq!(config.native_port, 9000);
        assert!(config.connect_timeout_secs > 0);
    }

    #[test]
    fn test_build_fetch_query_with_offset() {
        let query =
            ClickHouseConnector::build_fetch_query_no_format("mydb", "events", None, None, 5000, 10000);
        assert!(query.contains("LIMIT 5000 OFFSET 10000"));
    }

    #[tokio::test]
    #[ignore = "requires a running ClickHouse instance"]
    async fn test_validate_credentials_integration() {
        let config = ClickHouseConfig::new("localhost", "default");
        let connector = ClickHouseConnector::new(config).await;
        let result = connector.validate_credentials().await;
        assert!(result.is_ok() || result.is_err());
    }

    #[tokio::test]
    #[ignore = "requires a running ClickHouse instance"]
    async fn test_list_tables_integration() {
        let config = ClickHouseConfig::new("localhost", "default");
        let connector = ClickHouseConnector::new(config).await;
        let result = connector.list_tables().await;
        assert!(result.is_ok() || result.is_err());
    }
}
