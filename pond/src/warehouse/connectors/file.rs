//! File Connector Base
//!
//! Provides base implementations for file-based data sources.
//!
//! # Supported Formats
//!
//! - CSV (with schema inference)
//! - JSON/NDJSON (newline-delimited JSON)
//! - Excel (.xlsx, .xls)
//! - XML
//!
//! # Features
//!
//! - Schema inference from sample data
//! - Streaming reads for large files
//! - Support for local files, S3, GCS, and HTTP sources
//! - Format-specific parsing options

use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::csv::ReaderBuilder as CsvReaderBuilder;
use arrow::json::ReaderBuilder as JsonReaderBuilder;
use serde::{Deserialize, Serialize};

use crate::crypto::SecretString;
use super::schema_utils::arrow_schema_to_table_schema;
use super::{ConnectorError, ConnectorResult, TableSchema};
use crate::warehouse::types::ColumnType;

/// Maximum rows to sample for schema inference.
const SCHEMA_INFERENCE_SAMPLE_SIZE: usize = 1000;

/// File storage location.
#[derive(Debug, Clone)]
pub enum FileStorage {
    /// Local file system path
    Local(PathBuf),
    /// S3-compatible storage
    S3 {
        bucket: String,
        prefix: String,
        region: Option<String>,
        endpoint: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<SecretString>,
    },
    /// Google Cloud Storage
    Gcs {
        bucket: String,
        prefix: String,
        credentials_json: Option<SecretString>,
    },
    /// HTTP/HTTPS URL
    Http {
        base_url: String,
        headers: Vec<(String, String)>,
    },
}

impl FileStorage {
    /// Create a local file storage.
    pub fn local(path: impl Into<PathBuf>) -> Self {
        Self::Local(path.into())
    }

    /// Create an S3 storage.
    pub fn s3(bucket: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self::S3 {
            bucket: bucket.into(),
            prefix: prefix.into(),
            region: None,
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
        }
    }

    /// Create an S3 storage with full configuration.
    pub fn s3_with_config(
        bucket: impl Into<String>,
        prefix: impl Into<String>,
        region: Option<String>,
        endpoint: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<SecretString>,
    ) -> Self {
        Self::S3 {
            bucket: bucket.into(),
            prefix: prefix.into(),
            region,
            endpoint,
            access_key_id,
            secret_access_key,
        }
    }

    /// Create a GCS storage.
    pub fn gcs(bucket: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self::Gcs {
            bucket: bucket.into(),
            prefix: prefix.into(),
            credentials_json: None,
        }
    }

    /// Create a GCS storage with credentials.
    pub fn gcs_with_credentials(
        bucket: impl Into<String>,
        prefix: impl Into<String>,
        credentials_json: SecretString,
    ) -> Self {
        Self::Gcs {
            bucket: bucket.into(),
            prefix: prefix.into(),
            credentials_json: Some(credentials_json),
        }
    }

    /// Create an HTTP storage.
    pub fn http(base_url: impl Into<String>) -> Self {
        Self::Http {
            base_url: base_url.into(),
            headers: Vec::new(),
        }
    }

    /// Fetch file data from this storage location.
    ///
    /// Returns the file contents as bytes along with an optional ETag.
    pub async fn fetch_file(&self, path: &str) -> super::ConnectorResult<(Vec<u8>, Option<String>)> {
        use object_store::{ObjectStore, path::Path as ObjectPath};
        use object_store::aws::AmazonS3Builder;
        use object_store::gcp::GoogleCloudStorageBuilder;

        match self {
            FileStorage::Local(base_path) => {
                let full_path = base_path.join(path);
                let data = tokio::fs::read(&full_path)
                    .await
                    .map_err(|e| super::ConnectorError::Internal(format!("Failed to read file: {}", e)))?;
                Ok((data, None))
            }
            FileStorage::S3 {
                bucket,
                prefix,
                region,
                endpoint,
                access_key_id,
                secret_access_key,
            } => {
                let mut builder = AmazonS3Builder::from_env()
                    .with_bucket_name(bucket);

                if let Some(region) = region {
                    builder = builder.with_region(region);
                }
                if let Some(endpoint) = endpoint {
                    builder = builder.with_endpoint(endpoint);
                }
                if let Some(access_key) = access_key_id {
                    builder = builder.with_access_key_id(access_key);
                }
                if let Some(secret_key) = secret_access_key {
                    builder = builder.with_secret_access_key(secret_key.expose());
                }

                let store = builder.build()
                    .map_err(|e| super::ConnectorError::Config(format!("Failed to create S3 client: {}", e)))?;

                let object_path = if prefix.is_empty() {
                    ObjectPath::from(path)
                } else {
                    ObjectPath::from(format!("{}/{}", prefix.trim_end_matches('/'), path))
                };

                let result = store.get(&object_path)
                    .await
                    .map_err(|e| super::ConnectorError::Network(format!("Failed to fetch from S3: {}", e)))?;

                // Extract ETag from object metadata
                let etag = result.meta.e_tag.clone();

                let bytes = result.bytes()
                    .await
                    .map_err(|e| super::ConnectorError::Network(format!("Failed to read S3 response: {}", e)))?;

                Ok((bytes.to_vec(), etag))
            }
            FileStorage::Gcs {
                bucket,
                prefix,
                credentials_json,
            } => {
                let mut builder = GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket);

                if let Some(creds) = credentials_json {
                    builder = builder.with_service_account_key(creds.expose());
                }

                let store = builder.build()
                    .map_err(|e| super::ConnectorError::Config(format!("Failed to create GCS client: {}", e)))?;

                let object_path = if prefix.is_empty() {
                    ObjectPath::from(path)
                } else {
                    ObjectPath::from(format!("{}/{}", prefix.trim_end_matches('/'), path))
                };

                let result = store.get(&object_path)
                    .await
                    .map_err(|e| super::ConnectorError::Network(format!("Failed to fetch from GCS: {}", e)))?;

                // Extract ETag from object metadata
                let etag = result.meta.e_tag.clone();

                let bytes = result.bytes()
                    .await
                    .map_err(|e| super::ConnectorError::Network(format!("Failed to read GCS response: {}", e)))?;

                Ok((bytes.to_vec(), etag))
            }
            FileStorage::Http { base_url, headers } => {
                let client = reqwest::Client::new();
                let url = if path.is_empty() {
                    base_url.clone()
                } else {
                    format!("{}/{}", base_url.trim_end_matches('/'), path)
                };

                let mut request = client.get(&url);
                for (name, value) in headers {
                    request = request.header(name, value);
                }

                let response = request.send()
                    .await
                    .map_err(|e| super::ConnectorError::Network(format!("HTTP request failed: {}", e)))?;

                if !response.status().is_success() {
                    return Err(super::ConnectorError::Network(format!(
                        "HTTP error: {}",
                        response.status()
                    )));
                }

                // Extract ETag from response headers
                let etag = response.headers()
                    .get("etag")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());

                let bytes = response.bytes()
                    .await
                    .map_err(|e| super::ConnectorError::Network(format!("Failed to read response: {}", e)))?;

                Ok((bytes.to_vec(), etag))
            }
        }
    }

    /// Check if a file has changed using ETag comparison.
    ///
    /// Returns the current ETag if available.
    pub async fn check_etag(&self, path: &str) -> super::ConnectorResult<Option<String>> {
        use object_store::{ObjectStore, path::Path as ObjectPath};
        use object_store::aws::AmazonS3Builder;
        use object_store::gcp::GoogleCloudStorageBuilder;

        match self {
            FileStorage::Local(_) => Ok(None), // Local files don't have ETags
            FileStorage::S3 {
                bucket,
                prefix,
                region,
                endpoint,
                access_key_id,
                secret_access_key,
            } => {
                let mut builder = AmazonS3Builder::from_env()
                    .with_bucket_name(bucket);

                if let Some(region) = region {
                    builder = builder.with_region(region);
                }
                if let Some(endpoint) = endpoint {
                    builder = builder.with_endpoint(endpoint);
                }
                if let Some(access_key) = access_key_id {
                    builder = builder.with_access_key_id(access_key);
                }
                if let Some(secret_key) = secret_access_key {
                    builder = builder.with_secret_access_key(secret_key.expose());
                }

                let store = builder.build()
                    .map_err(|e| super::ConnectorError::Config(format!("Failed to create S3 client: {}", e)))?;

                let object_path = if prefix.is_empty() {
                    ObjectPath::from(path)
                } else {
                    ObjectPath::from(format!("{}/{}", prefix.trim_end_matches('/'), path))
                };

                let meta = store.head(&object_path)
                    .await
                    .map_err(|e| super::ConnectorError::Network(format!("Failed to get S3 metadata: {}", e)))?;

                Ok(meta.e_tag)
            }
            FileStorage::Gcs {
                bucket,
                prefix,
                credentials_json,
            } => {
                let mut builder = GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket);

                if let Some(creds) = credentials_json {
                    builder = builder.with_service_account_key(creds.expose());
                }

                let store = builder.build()
                    .map_err(|e| super::ConnectorError::Config(format!("Failed to create GCS client: {}", e)))?;

                let object_path = if prefix.is_empty() {
                    ObjectPath::from(path)
                } else {
                    ObjectPath::from(format!("{}/{}", prefix.trim_end_matches('/'), path))
                };

                let meta = store.head(&object_path)
                    .await
                    .map_err(|e| super::ConnectorError::Network(format!("Failed to get GCS metadata: {}", e)))?;

                Ok(meta.e_tag)
            }
            FileStorage::Http { base_url, headers } => {
                let client = reqwest::Client::new();
                let url = if path.is_empty() {
                    base_url.clone()
                } else {
                    format!("{}/{}", base_url.trim_end_matches('/'), path)
                };

                let mut request = client.head(&url);
                for (name, value) in headers {
                    request = request.header(name, value);
                }

                let response = request.send()
                    .await
                    .map_err(|e| super::ConnectorError::Network(format!("HEAD request failed: {}", e)))?;

                if !response.status().is_success() {
                    return Err(super::ConnectorError::Network(format!(
                        "HTTP HEAD error: {}",
                        response.status()
                    )));
                }

                let etag = response.headers()
                    .get("etag")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());

                Ok(etag)
            }
        }
    }
}

/// File format with parsing options.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FileFormat {
    /// CSV format
    Csv(CsvOptions),
    /// JSON/NDJSON format
    Json(JsonOptions),
    /// Excel format
    Excel(ExcelOptions),
    /// XML format
    Xml(XmlOptions),
}

impl Default for FileFormat {
    fn default() -> Self {
        Self::Csv(CsvOptions::default())
    }
}

/// CSV parsing options.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsvOptions {
    /// Field delimiter character
    #[serde(default = "default_csv_delimiter")]
    pub delimiter: char,
    /// Whether the first row is a header
    #[serde(default = "default_true")]
    pub has_header: bool,
    /// Quote character
    #[serde(default = "default_csv_quote")]
    pub quote: char,
    /// Escape character
    pub escape: Option<char>,
    /// Comment prefix (lines starting with this are ignored)
    pub comment: Option<char>,
    /// Null value representation
    #[serde(default)]
    pub null_values: Vec<String>,
    /// Date format string
    pub date_format: Option<String>,
    /// Timestamp format string
    pub timestamp_format: Option<String>,
    /// Maximum records to infer schema from
    #[serde(default = "default_schema_sample_size")]
    pub schema_sample_size: usize,
}

fn default_csv_delimiter() -> char { ',' }
fn default_csv_quote() -> char { '"' }
fn default_true() -> bool { true }
fn default_schema_sample_size() -> usize { SCHEMA_INFERENCE_SAMPLE_SIZE }

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            delimiter: ',',
            has_header: true,
            quote: '"',
            escape: None,
            comment: None,
            // NOTE: Empty string is NOT included - it's a valid value, not NULL.
            // Users can set treat_empty_as_null: true in NullSemantics for legacy behavior.
            null_values: vec!["NULL".to_string(), "null".to_string()],
            date_format: None,
            timestamp_format: None,
            schema_sample_size: SCHEMA_INFERENCE_SAMPLE_SIZE,
        }
    }
}

impl CsvOptions {
    /// Create CSV options with legacy NULL semantics.
    ///
    /// In legacy mode, empty strings are treated as NULL values.
    pub fn legacy() -> Self {
        Self {
            delimiter: ',',
            has_header: true,
            quote: '"',
            escape: None,
            comment: None,
            null_values: vec![
                String::new(), // Empty string = NULL in legacy mode
                "NULL".to_string(),
                "null".to_string(),
            ],
            date_format: None,
            timestamp_format: None,
            schema_sample_size: SCHEMA_INFERENCE_SAMPLE_SIZE,
        }
    }

    /// Apply NullSemantics to get the effective null values for the reader.
    pub fn null_values_with_semantics(&self, semantics: &crate::warehouse::types::NullSemantics) -> Vec<String> {
        let mut values = self.null_values.clone();
        
        // Add empty string if treat_empty_as_null is enabled
        if semantics.treat_empty_as_null && !values.contains(&String::new()) {
            values.insert(0, String::new());
        }
        
        // Add any additional null values from semantics
        for v in &semantics.null_values {
            if !values.contains(v) {
                values.push(v.clone());
            }
        }
        
        values
    }

    /// Auto-detect date format from sample column values.
    ///
    /// If a format is already set, returns that. Otherwise, attempts
    /// to detect the format from the provided samples.
    pub fn detect_date_format(&self, samples: &[&str]) -> Option<super::date_parsing::DateFormat> {
        // If user provided a format, use it
        if let Some(ref fmt) = self.date_format {
            return Some(super::date_parsing::DateFormat::new(fmt, "User-provided", false, false));
        }

        // Try to auto-detect
        super::date_parsing::detect_date_format(samples).ok()
    }

    /// Auto-detect timestamp format from sample column values.
    ///
    /// If a format is already set, returns that. Otherwise, attempts
    /// to detect the format from the provided samples.
    pub fn detect_timestamp_format(&self, samples: &[&str]) -> Option<super::date_parsing::DateFormat> {
        // If user provided a format, use it
        if let Some(ref fmt) = self.timestamp_format {
            return Some(super::date_parsing::DateFormat::new(fmt, "User-provided", true, false));
        }

        // Try to auto-detect (will prefer datetime formats)
        super::date_parsing::detect_date_format(samples).ok()
    }

    /// Create options with a specific date format.
    pub fn with_date_format(mut self, format: impl Into<String>) -> Self {
        self.date_format = Some(format.into());
        self
    }

    /// Create options with a specific timestamp format.
    pub fn with_timestamp_format(mut self, format: impl Into<String>) -> Self {
        self.timestamp_format = Some(format.into());
        self
    }
}

/// JSON parsing options.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonOptions {
    /// Whether the file is newline-delimited JSON (NDJSON)
    #[serde(default = "default_true")]
    pub is_ndjson: bool,
    /// JSON path to the array of records (e.g., "data.items")
    pub records_path: Option<String>,
    /// Schema sample size for inference
    #[serde(default = "default_schema_sample_size")]
    pub schema_sample_size: usize,
}

impl Default for JsonOptions {
    fn default() -> Self {
        Self {
            is_ndjson: true,
            records_path: None,
            schema_sample_size: SCHEMA_INFERENCE_SAMPLE_SIZE,
        }
    }
}

/// Excel parsing options.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExcelOptions {
    /// Sheet name or index (0-based)
    pub sheet: SheetSelector,
    /// Whether the first row is a header
    #[serde(default = "default_true")]
    pub has_header: bool,
    /// Range to read (e.g., "A1:D100")
    pub range: Option<String>,
    /// Skip N rows from the top
    #[serde(default)]
    pub skip_rows: usize,
}

impl Default for ExcelOptions {
    fn default() -> Self {
        Self {
            sheet: SheetSelector::Index(0),
            has_header: true,
            range: None,
            skip_rows: 0,
        }
    }
}

/// Excel sheet selector.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SheetSelector {
    /// Select by 0-based index
    Index(usize),
    /// Select by name
    Name(String),
}

impl Default for SheetSelector {
    fn default() -> Self {
        Self::Index(0)
    }
}

/// XML parsing options.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XmlOptions {
    /// XPath to the record elements
    pub record_xpath: String,
    /// Field mappings (XPath relative to record)
    #[serde(default)]
    pub field_mappings: Vec<XmlFieldMapping>,
    /// Whether to infer schema from sample
    #[serde(default = "default_true")]
    pub infer_schema: bool,
}

impl Default for XmlOptions {
    fn default() -> Self {
        Self {
            record_xpath: "/root/record".to_string(),
            field_mappings: Vec::new(),
            infer_schema: true,
        }
    }
}

/// XML field mapping.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XmlFieldMapping {
    /// Field name in output
    pub name: String,
    /// XPath relative to record element
    pub xpath: String,
    /// Target data type
    pub data_type: Option<ColumnType>,
}

/// Schema inference configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaInferenceConfig {
    /// Whether to infer schema automatically
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Maximum rows to sample
    #[serde(default = "default_schema_sample_size")]
    pub sample_size: usize,
    /// Column type overrides
    #[serde(default)]
    pub type_overrides: Vec<ColumnTypeOverride>,
}

impl Default for SchemaInferenceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            sample_size: SCHEMA_INFERENCE_SAMPLE_SIZE,
            type_overrides: Vec::new(),
        }
    }
}

/// Column type override for schema inference.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnTypeOverride {
    /// Column name
    pub name: String,
    /// Forced data type
    pub data_type: ColumnType,
}

/// File connector for reading data from files.
pub struct FileConnector {
    /// Storage location
    storage: FileStorage,
    /// File format
    format: FileFormat,
    /// Schema inference configuration
    schema_config: SchemaInferenceConfig,
    /// HTTP client for remote files
    client: reqwest::Client,
}

impl std::fmt::Debug for FileConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileConnector")
            .field("storage", &self.storage)
            .field("format", &self.format)
            .finish()
    }
}

impl FileConnector {
    /// Create a new file connector.
    pub fn new(storage: FileStorage, format: FileFormat) -> Self {
        Self {
            storage,
            format,
            schema_config: SchemaInferenceConfig::default(),
            client: reqwest::Client::new(),
        }
    }

    /// Set schema inference configuration.
    pub fn with_schema_config(mut self, config: SchemaInferenceConfig) -> Self {
        self.schema_config = config;
        self
    }

    /// Infer schema from file contents.
    pub async fn infer_schema(&self, data: &[u8]) -> ConnectorResult<TableSchema> {
        match &self.format {
            FileFormat::Csv(options) => self.infer_csv_schema(data, options),
            FileFormat::Json(options) => self.infer_json_schema(data, options),
            FileFormat::Excel(_) => Err(ConnectorError::UnsupportedFormat(
                "Excel schema inference requires calamine dependency".to_string(),
            )),
            FileFormat::Xml(_) => Err(ConnectorError::UnsupportedFormat(
                "XML schema inference requires quick-xml dependency".to_string(),
            )),
        }
    }

    /// Infer schema from CSV data.
    fn infer_csv_schema(&self, data: &[u8], options: &CsvOptions) -> ConnectorResult<TableSchema> {
        use arrow::csv::reader::Format;
        
        let mut cursor = Cursor::new(data);
        
        let format = Format::default()
            .with_delimiter(options.delimiter as u8)
            .with_header(options.has_header);
        
        let (schema, _) = format
            .infer_schema(&mut cursor, Some(options.schema_sample_size))
            .map_err(|e| ConnectorError::SchemaInference(format!("CSV schema inference failed: {}", e)))?;

        Ok(arrow_schema_to_table_schema(&schema))
    }

    /// Infer schema from JSON data.
    fn infer_json_schema(&self, data: &[u8], options: &JsonOptions) -> ConnectorResult<TableSchema> {
        let cursor = Cursor::new(data);
        
        let (schema, _) = arrow::json::reader::infer_json_schema_from_seekable(
            cursor,
            Some(options.schema_sample_size),
        )
        .map_err(|e| ConnectorError::SchemaInference(format!("JSON schema inference failed: {}", e)))?;

        Ok(arrow_schema_to_table_schema(&schema))
    }

    /// Read CSV data into RecordBatches.
    pub fn read_csv(&self, data: &[u8], options: &CsvOptions) -> ConnectorResult<Vec<RecordBatch>> {
        use arrow::csv::reader::Format;
        
        let cursor = Cursor::new(data);
        
        // First infer schema
        let format = Format::default()
            .with_delimiter(options.delimiter as u8)
            .with_header(options.has_header);
        
        let (schema, _) = format
            .infer_schema(&mut Cursor::new(data), Some(options.schema_sample_size))
            .map_err(|e| ConnectorError::SchemaInference(format!("CSV schema inference failed: {}", e)))?;

        // Build reader
        let mut reader = CsvReaderBuilder::new(Arc::new(schema))
            .with_delimiter(options.delimiter as u8)
            .with_header(options.has_header)
            .build(cursor)
            .map_err(|e| ConnectorError::Internal(format!("Failed to build CSV reader: {}", e)))?;

        // Read all batches
        let mut batches = Vec::new();
        for batch_result in reader.by_ref() {
            let batch = batch_result
                .map_err(|e| ConnectorError::Internal(format!("Failed to read CSV batch: {}", e)))?;
            batches.push(batch);
        }

        Ok(batches)
    }

    /// Read JSON/NDJSON data into RecordBatches.
    pub fn read_json(&self, data: &[u8], options: &JsonOptions) -> ConnectorResult<Vec<RecordBatch>> {
        let cursor = Cursor::new(data);
        
        // First infer schema
        let (schema, _) = arrow::json::reader::infer_json_schema_from_seekable(
            Cursor::new(data),
            Some(options.schema_sample_size),
        )
        .map_err(|e| ConnectorError::SchemaInference(format!("JSON schema inference failed: {}", e)))?;

        // Build reader
        let mut reader = JsonReaderBuilder::new(Arc::new(schema))
            .build(cursor)
            .map_err(|e| ConnectorError::Internal(format!("Failed to build JSON reader: {}", e)))?;

        // Read all batches
        let mut batches = Vec::new();
        for batch_result in reader.by_ref() {
            let batch = batch_result
                .map_err(|e| ConnectorError::Internal(format!("Failed to read JSON batch: {}", e)))?;
            batches.push(batch);
        }

        Ok(batches)
    }

    /// Read file data based on format.
    pub fn read_data(&self, data: &[u8]) -> ConnectorResult<Vec<RecordBatch>> {
        match &self.format {
            FileFormat::Csv(options) => self.read_csv(data, options),
            FileFormat::Json(options) => self.read_json(data, options),
            FileFormat::Excel(_) => Err(ConnectorError::UnsupportedFormat(
                "Excel reading requires calamine dependency".to_string(),
            )),
            FileFormat::Xml(_) => Err(ConnectorError::UnsupportedFormat(
                "XML reading requires quick-xml dependency".to_string(),
            )),
        }
    }

    /// Fetch file data from storage.
    pub async fn fetch_file(&self, path: &str) -> ConnectorResult<Vec<u8>> {
        match &self.storage {
            FileStorage::Local(base_path) => {
                let full_path = base_path.join(path);
                tokio::fs::read(&full_path)
                    .await
                    .map_err(|e| ConnectorError::Internal(format!("Failed to read file: {}", e)))
            }
            FileStorage::Http { base_url, headers } => {
                let url = format!("{}/{}", base_url.trim_end_matches('/'), path);
                let mut request = self.client.get(&url);
                
                for (name, value) in headers {
                    request = request.header(name, value);
                }
                
                let response = request
                    .send()
                    .await
                    .map_err(|e| ConnectorError::Network(format!("HTTP request failed: {}", e)))?;
                
                if !response.status().is_success() {
                    return Err(ConnectorError::Network(format!(
                        "HTTP error: {}",
                        response.status()
                    )));
                }
                
                response
                    .bytes()
                    .await
                    .map(|b| b.to_vec())
                    .map_err(|e| ConnectorError::Network(format!("Failed to read response: {}", e)))
            }
            FileStorage::S3 { .. } | FileStorage::Gcs { .. } => {
                Err(ConnectorError::UnsupportedFormat(
                    "S3/GCS storage requires aws-sdk or google-cloud dependencies".to_string(),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    #[test]
    fn test_csv_options_default() {
        let options = CsvOptions::default();
        assert_eq!(options.delimiter, ',');
        assert!(options.has_header);
        assert_eq!(options.quote, '"');
    }

    #[test]
    fn test_json_options_default() {
        let options = JsonOptions::default();
        assert!(options.is_ndjson);
        assert!(options.records_path.is_none());
    }

    #[test]
    fn test_file_storage_local() {
        let storage = FileStorage::local("/tmp/data");
        assert!(matches!(storage, FileStorage::Local(_)));
    }

    #[test]
    fn test_file_storage_s3() {
        let storage = FileStorage::s3("my-bucket", "data/");
        assert!(matches!(storage, FileStorage::S3 { .. }));
    }

    #[test]
    fn test_file_connector_creation() {
        let connector = FileConnector::new(
            FileStorage::local("/tmp"),
            FileFormat::Csv(CsvOptions::default()),
        );
        assert!(matches!(connector.format, FileFormat::Csv(_)));
    }

    #[test]
    fn test_arrow_type_to_column_type() {
        use super::super::schema_utils::arrow_type_to_column_type;
        assert_eq!(arrow_type_to_column_type(&DataType::Boolean), ColumnType::Boolean);
        assert_eq!(arrow_type_to_column_type(&DataType::Int64), ColumnType::Int64);
        assert_eq!(arrow_type_to_column_type(&DataType::Utf8), ColumnType::String);
        assert_eq!(arrow_type_to_column_type(&DataType::Float64), ColumnType::Float64);
    }

    #[test]
    fn test_read_csv_simple() {
        let csv_data = b"name,age\nAlice,30\nBob,25";
        let connector = FileConnector::new(
            FileStorage::local("/tmp"),
            FileFormat::Csv(CsvOptions::default()),
        );
        
        let batches = connector.read_csv(csv_data, &CsvOptions::default()).unwrap();
        assert!(!batches.is_empty());
        
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_infer_csv_schema() {
        let csv_data = b"id,name,score\n1,Alice,95.5\n2,Bob,87.3";
        let connector = FileConnector::new(
            FileStorage::local("/tmp"),
            FileFormat::Csv(CsvOptions::default()),
        );
        
        let schema = connector.infer_csv_schema(csv_data, &CsvOptions::default()).unwrap();
        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.columns[0].name, "id");
        assert_eq!(schema.columns[1].name, "name");
        assert_eq!(schema.columns[2].name, "score");
    }
}
