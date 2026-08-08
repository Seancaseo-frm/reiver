//! JSON/NDJSON Connector
//!
//! Connects to JSON and NDJSON (Newline-Delimited JSON) files and syncs data to the warehouse.
//!
//! # Features
//!
//! - Automatic schema inference from file content
//! - Support for local files, S3, GCS, and HTTP sources
//! - NDJSON (newline-delimited) and standard JSON array formats
//! - Nested JSON extraction via `records_path` (e.g., "data.items")
//! - ETag-based change detection for efficient re-sync
//!
//! # Usage
//!
//! ```ignore
//! // NDJSON file
//! let config = JsonConnectorConfig::new("/data/events.ndjson")
//!     .with_ndjson(true);
//!
//! // Standard JSON with nested records
//! let config = JsonConnectorConfig::new("/data/api_response.json")
//!     .with_ndjson(false)
//!     .with_records_path("data.items");
//!
//! let connector = JsonConnector::new(config);
//! let data = connector.fetch_table("events", None, None).await?;
//! ```

use std::io::Cursor;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::json::ReaderBuilder as JsonReaderBuilder;
use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::OnceCell;

use crate::warehouse::connectors::file::{FileStorage, JsonOptions};
use crate::warehouse::connectors::schema_utils::arrow_schema_to_table_schema;
use crate::warehouse::connectors::{
    Connector, ConnectorError, ConnectorResult, FetchOptions, RecordBatchStream, TableInfo,
};
use crate::warehouse::types::{SourceType, TableSchema};


/// Default batch size for reading records.
const DEFAULT_BATCH_SIZE: usize = 8192;

/// JSON/NDJSON connector configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonConnectorConfig {
    /// Path or URL to the JSON file
    pub path: String,
    /// Storage type (local, S3, GCS, HTTP)
    #[serde(skip)]
    pub storage: Option<FileStorage>,
    /// JSON parsing options
    #[serde(default)]
    pub options: JsonOptions,
    /// Table name to use (derived from filename if not set)
    pub table_name: Option<String>,
    /// Batch size for reading
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl JsonConnectorConfig {
    /// Create a new JSON connector configuration for a local file.
    pub fn new(path: impl Into<String>) -> Self {
        let path = path.into();
        Self {
            path,
            storage: None,
            options: JsonOptions::default(),
            table_name: None,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Create a configuration for an HTTP URL.
    pub fn from_http(url: impl Into<String>) -> Self {
        let url = url.into();
        Self {
            path: url.clone(),
            storage: Some(FileStorage::Http {
                base_url: url,
                headers: Vec::new(),
            }),
            options: JsonOptions::default(),
            table_name: None,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Set whether the file is NDJSON (newline-delimited).
    pub fn with_ndjson(mut self, is_ndjson: bool) -> Self {
        self.options.is_ndjson = is_ndjson;
        self
    }

    /// Set the path to records array in the JSON (e.g., "data.items").
    pub fn with_records_path(mut self, path: impl Into<String>) -> Self {
        self.options.records_path = Some(path.into());
        self
    }

    /// Set the schema sample size for inference.
    pub fn with_schema_sample_size(mut self, size: usize) -> Self {
        self.options.schema_sample_size = size;
        self
    }

    /// Set the table name.
    pub fn with_table_name(mut self, name: impl Into<String>) -> Self {
        self.table_name = Some(name.into());
        self
    }

    /// Set the batch size.
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Get the table name (derived from file path if not set).
    pub fn get_table_name(&self) -> String {
        if let Some(name) = &self.table_name {
            return name.clone();
        }

        // Derive from file path
        self.path
            .rsplit('/')
            .next()
            .unwrap_or("json_data")
            .trim_end_matches(".json")
            .trim_end_matches(".JSON")
            .trim_end_matches(".ndjson")
            .trim_end_matches(".NDJSON")
            .trim_end_matches(".jsonl")
            .trim_end_matches(".JSONL")
            .to_string()
    }
}

/// JSON/NDJSON file data source connector.
///
/// Uses caching for both data and schema to avoid repeated I/O and inference operations.
pub struct JsonConnector {
    config: JsonConnectorConfig,
    /// HTTP client for remote files
    client: reqwest::Client,
    /// Cached file data - uses Arc for cheap cloning
    cached_data: OnceCell<Arc<[u8]>>,
    /// Cached inferred schema
    cached_schema: OnceLock<TableSchema>,
    /// Stored ETag for change detection
    stored_etag: RwLock<Option<String>>,
}

impl JsonConnector {
    /// Create a new JSON connector.
    pub fn new(config: JsonConnectorConfig) -> Self {
        Self {
            config,
            client: reqwest::Client::new(),
            cached_data: OnceCell::new(),
            cached_schema: OnceLock::new(),
            stored_etag: RwLock::new(None),
        }
    }

    /// Create a connector with pre-loaded data (for testing or embedding).
    pub fn with_data(config: JsonConnectorConfig, data: Vec<u8>) -> Self {
        let cached_data = OnceCell::new();
        let _ = cached_data.set(Arc::from(data.into_boxed_slice()));
        Self {
            config,
            client: reqwest::Client::new(),
            cached_data,
            cached_schema: OnceLock::new(),
            stored_etag: RwLock::new(None),
        }
    }

    /// Check if remote file has changed since last sync.
    pub async fn check_if_changed(&self) -> ConnectorResult<bool> {
        if let Some(storage) = &self.config.storage {
            let current_etag = storage.check_etag("").await?;
            let stored = self.stored_etag.read();
            Ok(current_etag != *stored)
        } else {
            // For local files without storage config, always assume changed
            Ok(true)
        }
    }

    /// Update stored ETag after successful fetch.
    fn update_etag(&self, etag: Option<String>) {
        *self.stored_etag.write() = etag;
    }

    /// Get the current stored ETag.
    pub fn get_etag(&self) -> Option<String> {
        self.stored_etag.read().clone()
    }

    /// Fetch the JSON file data (cached after first fetch).
    async fn fetch_data(&self) -> ConnectorResult<Arc<[u8]>> {
        self.cached_data
            .get_or_try_init(|| async { self.fetch_data_internal().await })
            .await
            .cloned()
    }

    /// Internal method to actually fetch the data.
    async fn fetch_data_internal(&self) -> ConnectorResult<Arc<[u8]>> {
        if let Some(storage) = &self.config.storage {
            // Use shared storage implementation for HTTP, S3, GCS
            let (data, etag) = storage.fetch_file("").await?;
            self.update_etag(etag);
            Ok(Arc::from(data.into_boxed_slice()))
        } else {
            // Local file without explicit storage config
            tokio::fs::read(&self.config.path)
                .await
                .map(|data| Arc::from(data.into_boxed_slice()))
                .map_err(|e| {
                    ConnectorError::Internal(format!(
                        "Failed to read JSON file '{}': {}",
                        self.config.path, e
                    ))
                })
        }
    }

    /// Infer Arrow schema from JSON data.
    fn infer_arrow_schema(&self, data: &[u8]) -> ConnectorResult<Schema> {
        if self.config.options.is_ndjson {
            self.infer_ndjson_schema(data)
        } else {
            self.infer_standard_json_schema(data)
        }
    }

    /// Infer schema from NDJSON data using Arrow's built-in inference.
    fn infer_ndjson_schema(&self, data: &[u8]) -> ConnectorResult<Schema> {
        let cursor = Cursor::new(data);

        let (schema, _) = arrow::json::reader::infer_json_schema_from_seekable(
            cursor,
            Some(self.config.options.schema_sample_size),
        )
        .map_err(|e| {
            ConnectorError::SchemaInference(format!("Failed to infer NDJSON schema: {}", e))
        })?;

        Ok(schema)
    }

    /// Infer schema from standard JSON data.
    fn infer_standard_json_schema(&self, data: &[u8]) -> ConnectorResult<Schema> {
        // Parse JSON
        let json_value: Value = serde_json::from_slice(data).map_err(|e| {
            ConnectorError::SchemaInference(format!("Failed to parse JSON: {}", e))
        })?;

        // Extract records at path
        let records = self.extract_records(&json_value)?;

        if records.is_empty() {
            return Err(ConnectorError::SchemaInference(
                "No records found in JSON".to_string(),
            ));
        }

        // Convert records to NDJSON for Arrow's schema inference
        let sample_size = records.len().min(self.config.options.schema_sample_size);
        let ndjson_data: Vec<u8> = records[..sample_size]
            .iter()
            .filter_map(|r| serde_json::to_string(r).ok())
            .collect::<Vec<_>>()
            .join("\n")
            .into_bytes();

        self.infer_ndjson_schema(&ndjson_data)
    }

    /// Extract records from JSON using records_path.
    fn extract_records<'a>(&self, value: &'a Value) -> ConnectorResult<Vec<&'a Value>> {
        let target = match &self.config.options.records_path {
            Some(path) => {
                let mut current = value;
                for segment in path.split('.') {
                    current = current.get(segment).ok_or_else(|| {
                        ConnectorError::SchemaInference(format!(
                            "Path segment '{}' not found in JSON",
                            segment
                        ))
                    })?;
                }
                current
            }
            None => value,
        };

        match target {
            Value::Array(arr) => Ok(arr.iter().collect()),
            Value::Object(_) => Ok(vec![target]),
            _ => Err(ConnectorError::SchemaInference(
                "JSON target must be an array or object".to_string(),
            )),
        }
    }

    /// Get the cached TableSchema, inferring it if necessary.
    fn get_cached_table_schema(&self, data: &[u8]) -> ConnectorResult<TableSchema> {
        if let Some(schema) = self.cached_schema.get() {
            return Ok(schema.clone());
        }

        let arrow_schema = self.infer_arrow_schema(data)?;
        let table_schema = arrow_schema_to_table_schema(&arrow_schema);

        Ok(self
            .cached_schema
            .get_or_init(|| table_schema.clone())
            .clone())
    }

    /// Read NDJSON data into RecordBatches.
    fn read_ndjson(&self, data: &[u8], schema: SchemaRef) -> ConnectorResult<Vec<RecordBatch>> {
        let cursor = Cursor::new(data);

        let reader = JsonReaderBuilder::new(schema)
            .build(cursor)
            .map_err(|e| ConnectorError::Internal(format!("Failed to build JSON reader: {}", e)))?;

        let mut batches = Vec::new();
        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| ConnectorError::Internal(format!("Failed to read JSON batch: {}", e)))?;
            batches.push(batch);
        }

        Ok(batches)
    }

    /// Read standard JSON data into RecordBatches.
    fn read_standard_json(
        &self,
        data: &[u8],
        schema: SchemaRef,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        // Parse JSON
        let json_value: Value = serde_json::from_slice(data)
            .map_err(|e| ConnectorError::Internal(format!("Failed to parse JSON: {}", e)))?;

        // Extract records
        let records = self.extract_records(&json_value)?;

        if records.is_empty() {
            return Ok(vec![]);
        }

        // Convert records to NDJSON for Arrow reader
        let ndjson_data: Vec<u8> = records
            .iter()
            .filter_map(|r| serde_json::to_string(r).ok())
            .collect::<Vec<_>>()
            .join("\n")
            .into_bytes();

        self.read_ndjson(&ndjson_data, schema)
    }

    /// Read JSON data into RecordBatches.
    fn read_json_batches(&self, data: &[u8], schema: SchemaRef) -> ConnectorResult<Vec<RecordBatch>> {
        if self.config.options.is_ndjson {
            self.read_ndjson(data, schema)
        } else {
            self.read_standard_json(data, schema)
        }
    }
}

#[async_trait]
impl Connector for JsonConnector {
    fn source_type(&self) -> SourceType {
        SourceType::Json
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        // Fetch data (cached after first call)
        let data = self.fetch_data().await?;

        // Get cached schema (cached after first inference)
        let table_schema = self.get_cached_table_schema(&data)?;

        // JSON files are treated as a single table
        let table_name = self.config.get_table_name();

        // Estimate row count
        let estimated_rows = self.estimate_row_count(&data);

        let table_info = TableInfo {
            name: table_name,
            schema: table_schema,
            supports_incremental: false,
            incremental_key: None,
            estimated_rows: Some(estimated_rows),
            primary_key_columns: Vec::new(),
        };

        Ok(vec![table_info])
    }

    async fn get_schema(&self, _table: &str) -> ConnectorResult<TableSchema> {
        let data = self.fetch_data().await?;
        self.get_cached_table_schema(&data)
    }

    async fn fetch_table(
        &self,
        _table: &str,
        _incremental_key: Option<&str>,
        _last_value: Option<&str>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        // Fetch file data (cached after first call)
        let data = self.fetch_data().await?;

        // Infer Arrow schema
        let arrow_schema = Arc::new(self.infer_arrow_schema(&data)?);

        // Read all data
        let batches = self.read_json_batches(&data, arrow_schema)?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        tracing::info!(
            path = %self.config.path,
            rows = total_rows,
            batches = batches.len(),
            is_ndjson = self.config.options.is_ndjson,
            "Fetched JSON data"
        );

        Ok(batches)
    }

    fn fetch_table_stream<'a>(
        &'a self,
        table: &'a str,
        _options: FetchOptions,
    ) -> Pin<Box<dyn futures::Future<Output = ConnectorResult<RecordBatchStream>> + Send + 'a>>
    {
        Box::pin(async move {
            // For JSON files, we read all data and stream it
            let batches = self.fetch_table(table, None, None).await?;

            let stream: BoxStream<'static, ConnectorResult<RecordBatch>> =
                stream::iter(batches.into_iter().map(Ok)).boxed();

            Ok(stream)
        })
    }

    async fn validate_credentials(&self) -> ConnectorResult<()> {
        match &self.config.storage {
            Some(FileStorage::Http { base_url, .. }) => {
                // For HTTP, do a HEAD request
                let response = self
                    .client
                    .head(base_url)
                    .send()
                    .await
                    .map_err(|e| ConnectorError::Network(format!("HEAD request failed: {}", e)))?;

                if !response.status().is_success() {
                    return Err(ConnectorError::Authentication(format!(
                        "Cannot access URL: {}",
                        response.status()
                    )));
                }
            }
            Some(FileStorage::Local(_)) | None => {
                // Check if local file exists
                if !std::path::Path::new(&self.config.path).exists() {
                    return Err(ConnectorError::Config(format!(
                        "JSON file not found: {}",
                        self.config.path
                    )));
                }
            }
            _ => {
                return Err(ConnectorError::UnsupportedFormat(
                    "Storage type not supported".to_string(),
                ));
            }
        }

        Ok(())
    }
}

impl JsonConnector {
    /// Estimate row count from file data.
    fn estimate_row_count(&self, data: &[u8]) -> u64 {
        if self.config.options.is_ndjson {
            // Count newlines for NDJSON
            let sample_size = data.len().min(65536);
            let sample = &data[..sample_size];
            let newlines = sample.iter().filter(|&&b| b == b'\n').count();

            if newlines == 0 {
                return 1;
            }

            ((newlines as f64 / sample_size as f64) * data.len() as f64) as u64
        } else {
            // For standard JSON, parse to count records
            if let Ok(value) = serde_json::from_slice::<Value>(data) {
                if let Ok(records) = self.extract_records(&value) {
                    return records.len() as u64;
                }
            }
            1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_connector_config_creation() {
        let config = JsonConnectorConfig::new("/data/test.json");
        assert_eq!(config.path, "/data/test.json");
        assert!(config.options.is_ndjson);
    }

    #[test]
    fn test_json_connector_config_get_table_name() {
        let config = JsonConnectorConfig::new("/data/events.json");
        assert_eq!(config.get_table_name(), "events");

        let config = JsonConnectorConfig::new("/data/events.ndjson");
        assert_eq!(config.get_table_name(), "events");

        let config = JsonConnectorConfig::new("/data/events.jsonl");
        assert_eq!(config.get_table_name(), "events");

        let config = JsonConnectorConfig::new("http://example.com/data.json");
        assert_eq!(config.get_table_name(), "data");
    }

    #[test]
    fn test_json_connector_config_with_table_name() {
        let config = JsonConnectorConfig::new("/data/test.json").with_table_name("custom_table");
        assert_eq!(config.get_table_name(), "custom_table");
    }

    #[test]
    fn test_json_connector_config_with_records_path() {
        let config = JsonConnectorConfig::new("/data/api.json")
            .with_ndjson(false)
            .with_records_path("data.items");
        assert!(!config.options.is_ndjson);
        assert_eq!(config.options.records_path, Some("data.items".to_string()));
    }

    #[test]
    fn test_json_connector_with_data() {
        let json_data = br#"{"name":"Alice","age":30}
{"name":"Bob","age":25}"#
            .to_vec();
        let config = JsonConnectorConfig::new("inline");
        let connector = JsonConnector::with_data(config, json_data);
        assert!(connector.cached_data.get().is_some());
    }

    #[test]
    fn test_infer_ndjson_schema() {
        let ndjson_data = br#"{"id":1,"name":"Alice","age":30}
{"id":2,"name":"Bob","age":25}"#
            .to_vec();
        let config = JsonConnectorConfig::new("inline").with_ndjson(true);
        let connector = JsonConnector::with_data(config, ndjson_data.clone());

        let table_schema = connector.get_cached_table_schema(&ndjson_data).unwrap();
        assert_eq!(table_schema.columns.len(), 3);

        let column_names: Vec<_> = table_schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(column_names.contains(&"id"));
        assert!(column_names.contains(&"name"));
        assert!(column_names.contains(&"age"));
    }

    #[test]
    fn test_infer_standard_json_schema() {
        let json_data = br#"{"data":{"items":[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]}}"#
            .to_vec();
        let config = JsonConnectorConfig::new("inline")
            .with_ndjson(false)
            .with_records_path("data.items");
        let connector = JsonConnector::with_data(config, json_data.clone());

        let table_schema = connector.get_cached_table_schema(&json_data).unwrap();
        assert_eq!(table_schema.columns.len(), 2);

        let column_names: Vec<_> = table_schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(column_names.contains(&"id"));
        assert!(column_names.contains(&"name"));
    }

    #[test]
    fn test_extract_records_simple_array() {
        let json_data = br#"[{"id":1},{"id":2}]"#.to_vec();
        let config = JsonConnectorConfig::new("inline").with_ndjson(false);
        let connector = JsonConnector::with_data(config, json_data);

        let value: Value = serde_json::from_slice(b"[{\"id\":1},{\"id\":2}]").unwrap();
        let records = connector.extract_records(&value).unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn test_extract_records_nested_path() {
        let json_data = br#"{"response":{"data":[{"id":1},{"id":2}]}}"#.to_vec();
        let config = JsonConnectorConfig::new("inline")
            .with_ndjson(false)
            .with_records_path("response.data");
        let connector = JsonConnector::with_data(config, json_data);

        let value: Value =
            serde_json::from_slice(b"{\"response\":{\"data\":[{\"id\":1},{\"id\":2}]}}").unwrap();
        let records = connector.extract_records(&value).unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn test_read_ndjson_batches() {
        let ndjson_data = br#"{"id":1,"name":"Alice","score":95.5}
{"id":2,"name":"Bob","score":87.3}"#
            .to_vec();
        let config = JsonConnectorConfig::new("inline").with_ndjson(true);
        let connector = JsonConnector::with_data(config, ndjson_data.clone());

        let arrow_schema = Arc::new(connector.infer_arrow_schema(&ndjson_data).unwrap());
        let batches = connector.read_ndjson(&ndjson_data, arrow_schema).unwrap();

        assert!(!batches.is_empty());
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_source_type() {
        let config = JsonConnectorConfig::new("/data/test.json");
        let connector = JsonConnector::new(config);
        assert_eq!(connector.source_type(), SourceType::Json);
    }

    #[test]
    fn test_estimate_row_count_ndjson() {
        let ndjson_data = br#"{"id":1}
{"id":2}
{"id":3}
{"id":4}"#
            .to_vec();
        let config = JsonConnectorConfig::new("inline").with_ndjson(true);
        let connector = JsonConnector::with_data(config, ndjson_data.clone());

        let count = connector.estimate_row_count(&ndjson_data);
        assert!(count >= 3);
    }

    #[test]
    fn test_estimate_row_count_standard_json() {
        let json_data = br#"[{"id":1},{"id":2},{"id":3}]"#.to_vec();
        let config = JsonConnectorConfig::new("inline").with_ndjson(false);
        let connector = JsonConnector::with_data(config, json_data.clone());

        let count = connector.estimate_row_count(&json_data);
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_list_tables() {
        let ndjson_data = br#"{"name":"Alice","score":95}
{"name":"Bob","score":87}"#
            .to_vec();
        let config = JsonConnectorConfig::new("test.json").with_ndjson(true);
        let connector = JsonConnector::with_data(config, ndjson_data);

        let tables = connector.list_tables().await.unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "test");
        assert_eq!(tables[0].schema.columns.len(), 2);
        assert!(!tables[0].supports_incremental);
    }

    #[tokio::test]
    async fn test_fetch_table_ndjson() {
        let ndjson_data = br#"{"id":1,"value":100}
{"id":2,"value":200}
{"id":3,"value":300}"#
            .to_vec();
        let config = JsonConnectorConfig::new("data.json").with_ndjson(true);
        let connector = JsonConnector::with_data(config, ndjson_data);

        let batches = connector.fetch_table("data", None, None).await.unwrap();
        assert!(!batches.is_empty());

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_fetch_table_standard_json() {
        let json_data = br#"{"records":[{"id":1,"value":100},{"id":2,"value":200}]}"#.to_vec();
        let config = JsonConnectorConfig::new("data.json")
            .with_ndjson(false)
            .with_records_path("records");
        let connector = JsonConnector::with_data(config, json_data);

        let batches = connector.fetch_table("data", None, None).await.unwrap();
        assert!(!batches.is_empty());

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }
}
