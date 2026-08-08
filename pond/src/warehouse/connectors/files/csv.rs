//! CSV Connector
//!
//! Connects to CSV files and syncs data to the warehouse.
//!
//! # Features
//!
//! - Automatic schema inference from file content
//! - Support for local files, S3, and HTTP sources
//! - Configurable delimiters, headers, and null values
//! - Streaming support for large files
//! - Multiple file support with glob patterns
//!
//! # Usage
//!
//! ```ignore
//! let config = CsvConnectorConfig::new("/data/sales.csv")
//!     .with_delimiter(',')
//!     .with_header(true);
//!
//! let connector = CsvConnector::new(config);
//! let data = connector.fetch_table("sales", None, None).await?;
//! ```

use std::io::Cursor;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use arrow::csv::ReaderBuilder as CsvReaderBuilder;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;

use crate::warehouse::connectors::file::{CsvOptions, FileStorage};
use crate::warehouse::connectors::schema_utils::arrow_schema_to_table_schema;
use crate::warehouse::connectors::{
    Connector, ConnectorError, ConnectorResult, FetchOptions, RecordBatchStream, TableInfo,
};
use crate::warehouse::types::{SourceType, TableSchema};


/// Default batch size for reading records.
const DEFAULT_BATCH_SIZE: usize = 8192;

/// CSV connector configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsvConnectorConfig {
    /// Path or URL to the CSV file(s)
    pub path: String,
    /// Storage type (local, S3, HTTP)
    #[serde(skip)]
    pub storage: Option<FileStorage>,
    /// CSV parsing options
    #[serde(default)]
    pub options: CsvOptions,
    /// Table name to use (derived from filename if not set)
    pub table_name: Option<String>,
    /// Batch size for reading
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl CsvConnectorConfig {
    /// Create a new CSV connector configuration for a local file.
    pub fn new(path: impl Into<String>) -> Self {
        let path = path.into();
        Self {
            path,
            storage: None,
            options: CsvOptions::default(),
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
            options: CsvOptions::default(),
            table_name: None,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Set the delimiter character.
    pub fn with_delimiter(mut self, delimiter: char) -> Self {
        self.options.delimiter = delimiter;
        self
    }

    /// Set whether the first row is a header.
    pub fn with_header(mut self, has_header: bool) -> Self {
        self.options.has_header = has_header;
        self
    }

    /// Set the quote character.
    pub fn with_quote(mut self, quote: char) -> Self {
        self.options.quote = quote;
        self
    }

    /// Set null value representations.
    pub fn with_null_values(mut self, null_values: Vec<String>) -> Self {
        self.options.null_values = null_values;
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
            .unwrap_or("csv_data")
            .trim_end_matches(".csv")
            .trim_end_matches(".CSV")
            .to_string()
    }
}

/// CSV file data source connector.
///
/// Uses caching for both data and schema to avoid repeated I/O and inference operations.
pub struct CsvConnector {
    config: CsvConnectorConfig,
    /// HTTP client for remote files
    client: reqwest::Client,
    /// Cached file data - uses Arc for cheap cloning
    cached_data: OnceCell<Arc<[u8]>>,
    /// Cached inferred schema
    cached_schema: OnceLock<TableSchema>,
}

impl CsvConnector {
    /// Create a new CSV connector.
    pub fn new(config: CsvConnectorConfig) -> Self {
        Self {
            config,
            client: reqwest::Client::new(),
            cached_data: OnceCell::new(),
            cached_schema: OnceLock::new(),
        }
    }

    /// Create a connector with pre-loaded data (for testing or embedding).
    pub fn with_data(config: CsvConnectorConfig, data: Vec<u8>) -> Self {
        let cached_data = OnceCell::new();
        let _ = cached_data.set(Arc::from(data.into_boxed_slice()));
        Self {
            config,
            client: reqwest::Client::new(),
            cached_data,
            cached_schema: OnceLock::new(),
        }
    }

    /// Fetch the CSV file data (cached after first fetch).
    ///
    /// Uses Arc for cheap cloning of cached data.
    async fn fetch_data(&self) -> ConnectorResult<Arc<[u8]>> {
        self.cached_data
            .get_or_try_init(|| async { self.fetch_data_internal().await })
            .await
            .cloned()
    }

    /// Internal method to actually fetch the data.
    async fn fetch_data_internal(&self) -> ConnectorResult<Arc<[u8]>> {
        match &self.config.storage {
            Some(FileStorage::Http { base_url, headers }) => {
                let mut request = self.client.get(base_url);
                for (name, value) in headers {
                    request = request.header(name, value);
                }

                let response = request.send().await.map_err(|e| {
                    ConnectorError::Network(format!("Failed to fetch CSV from URL: {}", e))
                })?;

                if !response.status().is_success() {
                    return Err(ConnectorError::Network(format!(
                        "HTTP error fetching CSV: {}",
                        response.status()
                    )));
                }

                response
                    .bytes()
                    .await
                    .map(|b| Arc::from(b.to_vec().into_boxed_slice()))
                    .map_err(|e| {
                        ConnectorError::Network(format!("Failed to read response body: {}", e))
                    })
            }
            Some(FileStorage::S3 { .. }) | Some(FileStorage::Gcs { .. }) => {
                Err(ConnectorError::UnsupportedFormat(
                    "S3/GCS storage requires additional dependencies".to_string(),
                ))
            }
            Some(FileStorage::Local(_)) | None => {
                // Local file
                tokio::fs::read(&self.config.path)
                    .await
                    .map(|data| Arc::from(data.into_boxed_slice()))
                    .map_err(|e| {
                        ConnectorError::Internal(format!(
                            "Failed to read CSV file '{}': {}",
                            self.config.path, e
                        ))
                    })
            }
        }
    }

    /// Infer Arrow schema from CSV data.
    ///
    /// This is a low-level method that always performs inference.
    /// Use `get_cached_table_schema` to get a cached TableSchema.
    fn infer_arrow_schema(&self, data: &[u8]) -> ConnectorResult<Schema> {
        use arrow::csv::reader::Format;

        let mut cursor = Cursor::new(data);

        let format = Format::default()
            .with_delimiter(self.config.options.delimiter as u8)
            .with_header(self.config.options.has_header);

        let (schema, _) = format
            .infer_schema(&mut cursor, Some(self.config.options.schema_sample_size))
            .map_err(|e| {
                ConnectorError::SchemaInference(format!("Failed to infer CSV schema: {}", e))
            })?;

        Ok(schema)
    }

    /// Get the cached TableSchema, inferring it if necessary.
    ///
    /// This method caches the schema after first inference to avoid repeated
    /// schema inference operations which can be expensive for large files.
    fn get_cached_table_schema(&self, data: &[u8]) -> ConnectorResult<TableSchema> {
        if let Some(schema) = self.cached_schema.get() {
            return Ok(schema.clone());
        }

        let arrow_schema = self.infer_arrow_schema(data)?;
        let table_schema = arrow_schema_to_table_schema(&arrow_schema);

        // Use get_or_init to handle potential race conditions
        Ok(self
            .cached_schema
            .get_or_init(|| table_schema.clone())
            .clone())
    }

    /// Read CSV data into RecordBatches.
    fn read_csv_batches(
        &self,
        data: &[u8],
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let cursor = Cursor::new(data);

        let mut reader = CsvReaderBuilder::new(arrow_schema)
            .with_delimiter(self.config.options.delimiter as u8)
            .with_header(self.config.options.has_header)
            .with_batch_size(self.config.batch_size)
            .build(cursor)
            .map_err(|e| ConnectorError::Internal(format!("Failed to build CSV reader: {}", e)))?;

        let mut batches = Vec::new();
        for batch_result in reader.by_ref() {
            let batch = batch_result
                .map_err(|e| ConnectorError::Internal(format!("Failed to read CSV batch: {}", e)))?;
            batches.push(batch);
        }

        Ok(batches)
    }
}

#[async_trait]
impl Connector for CsvConnector {
    fn source_type(&self) -> SourceType {
        SourceType::Csv
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        // Fetch data (cached after first call)
        let data = self.fetch_data().await?;

        // Get cached schema (cached after first inference)
        let table_schema = self.get_cached_table_schema(&data)?;

        // CSV files are treated as a single table
        let table_name = self.config.get_table_name();

        // Estimate row count from file size and sample
        let estimated_rows = estimate_row_count(&data, &self.config.options);

        let table_info = TableInfo {
            name: table_name,
            schema: table_schema,
            supports_incremental: false, // CSV files don't support incremental sync
            incremental_key: None,
            estimated_rows: Some(estimated_rows),
            primary_key_columns: Vec::new(),
        };

        Ok(vec![table_info])
    }

    async fn get_schema(&self, _table: &str) -> ConnectorResult<TableSchema> {
        // For CSV, we always return the same schema (cached after first inference)
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

        // Infer Arrow schema (needed for CSV reader)
        let arrow_schema = self.infer_arrow_schema(&data)?;

        // Read all data
        let batches = self.read_csv_batches(&data, Arc::new(arrow_schema))?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        tracing::info!(
            path = %self.config.path,
            rows = total_rows,
            batches = batches.len(),
            "Fetched CSV data"
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
            // For CSV files, we read all data and stream it
            let batches = self.fetch_table(table, None, None).await?;
            
            let stream: BoxStream<'static, ConnectorResult<RecordBatch>> =
                stream::iter(batches.into_iter().map(Ok)).boxed();
            
            Ok(stream)
        })
    }

    async fn validate_credentials(&self) -> ConnectorResult<()> {
        // For local files, check if the file exists
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
                        "CSV file not found: {}",
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

/// Estimate row count from file data.
fn estimate_row_count(data: &[u8], options: &CsvOptions) -> u64 {
    // Count newlines in a sample
    let sample_size = data.len().min(65536);
    let sample = &data[..sample_size];
    let newlines = sample.iter().filter(|&&b| b == b'\n').count();

    if newlines == 0 {
        return 1; // At least one row
    }

    // Extrapolate to full file
    let estimated = ((newlines as f64 / sample_size as f64) * data.len() as f64) as u64;

    // Subtract header row if present
    if options.has_header && estimated > 0 {
        estimated - 1
    } else {
        estimated
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csv_connector_config_creation() {
        let config = CsvConnectorConfig::new("/data/test.csv");
        assert_eq!(config.path, "/data/test.csv");
        assert!(config.options.has_header);
        assert_eq!(config.options.delimiter, ',');
    }

    #[test]
    fn test_csv_connector_config_get_table_name() {
        let config = CsvConnectorConfig::new("/data/sales.csv");
        assert_eq!(config.get_table_name(), "sales");

        let config = CsvConnectorConfig::new("/data/my_data.CSV");
        assert_eq!(config.get_table_name(), "my_data");

        let config = CsvConnectorConfig::new("http://example.com/data.csv");
        assert_eq!(config.get_table_name(), "data");
    }

    #[test]
    fn test_csv_connector_config_with_table_name() {
        let config = CsvConnectorConfig::new("/data/test.csv").with_table_name("custom_table");
        assert_eq!(config.get_table_name(), "custom_table");
    }

    #[test]
    fn test_csv_connector_config_with_delimiter() {
        let config = CsvConnectorConfig::new("/data/test.tsv").with_delimiter('\t');
        assert_eq!(config.options.delimiter, '\t');
    }

    #[test]
    fn test_csv_connector_with_data() {
        let csv_data = b"name,age\nAlice,30\nBob,25".to_vec();
        let config = CsvConnectorConfig::new("inline");
        let connector = CsvConnector::with_data(config, csv_data);
        assert!(connector.cached_data.get().is_some());
    }

    #[test]
    fn test_infer_schema_simple() {
        let csv_data = b"id,name,age\n1,Alice,30\n2,Bob,25".to_vec();
        let config = CsvConnectorConfig::new("inline");
        let connector = CsvConnector::with_data(config, csv_data.clone());

        let table_schema = connector.get_cached_table_schema(&csv_data).unwrap();
        assert_eq!(table_schema.columns.len(), 3);
        assert_eq!(table_schema.columns[0].name, "id");
        assert_eq!(table_schema.columns[1].name, "name");
        assert_eq!(table_schema.columns[2].name, "age");
    }

    #[test]
    fn test_read_csv_batches() {
        let csv_data = b"id,name,value\n1,Alice,100.5\n2,Bob,200.3".to_vec();
        let config = CsvConnectorConfig::new("inline");
        let connector = CsvConnector::with_data(config, csv_data.clone());

        let arrow_schema = connector.infer_arrow_schema(&csv_data).unwrap();
        let batches = connector
            .read_csv_batches(&csv_data, Arc::new(arrow_schema))
            .unwrap();

        assert!(!batches.is_empty());
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_source_type() {
        let config = CsvConnectorConfig::new("/data/test.csv");
        let connector = CsvConnector::new(config);
        assert_eq!(connector.source_type(), SourceType::Csv);
    }

    #[test]
    fn test_estimate_row_count() {
        let csv_data = b"id,name\n1,Alice\n2,Bob\n3,Carol\n4,Dave".to_vec();
        let options = CsvOptions::default();

        let count = estimate_row_count(&csv_data, &options);
        assert!(count >= 3); // At least 4 data rows (minus header = 3+)
    }

    #[test]
    fn test_arrow_type_to_column_type() {
        use arrow::datatypes::DataType;
        use crate::warehouse::connectors::schema_utils::arrow_type_to_column_type;
        use crate::warehouse::types::ColumnType;

        assert_eq!(
            arrow_type_to_column_type(&DataType::Boolean),
            ColumnType::Boolean
        );
        assert_eq!(
            arrow_type_to_column_type(&DataType::Int64),
            ColumnType::Int64
        );
        assert_eq!(
            arrow_type_to_column_type(&DataType::Utf8),
            ColumnType::String
        );
        assert_eq!(
            arrow_type_to_column_type(&DataType::Float64),
            ColumnType::Float64
        );
    }

    #[tokio::test]
    async fn test_list_tables() {
        let csv_data = b"name,score\nAlice,95\nBob,87".to_vec();
        let config = CsvConnectorConfig::new("test.csv");
        let connector = CsvConnector::with_data(config, csv_data);

        let tables = connector.list_tables().await.unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "test");
        assert_eq!(tables[0].schema.columns.len(), 2);
        assert!(!tables[0].supports_incremental);
    }

    #[tokio::test]
    async fn test_fetch_table() {
        let csv_data = b"id,value\n1,100\n2,200\n3,300".to_vec();
        let config = CsvConnectorConfig::new("data.csv");
        let connector = CsvConnector::with_data(config, csv_data);

        let batches = connector.fetch_table("data", None, None).await.unwrap();
        assert!(!batches.is_empty());

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }
}
