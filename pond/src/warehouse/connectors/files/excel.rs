//! Excel Connector
//!
//! Connects to Excel files (.xlsx, .xls) and syncs data to the warehouse.
//!
//! # Features
//!
//! - Support for .xlsx (Office Open XML) and .xls (BIFF) formats
//! - Sheet selection by name or index
//! - Range selection (e.g., "A1:D100")
//! - Automatic schema inference from cell values
//! - Header row detection
//! - Skip rows option
//! - ETag-based change detection for remote files
//!
//! # Usage
//!
//! ```ignore
//! // Read first sheet with default options
//! let config = ExcelConnectorConfig::new("/data/report.xlsx");
//!
//! // Read specific sheet by name with range
//! let config = ExcelConnectorConfig::new("/data/report.xlsx")
//!     .with_sheet_name("Sales Data")
//!     .with_range("A1:E100")
//!     .with_skip_rows(1);
//!
//! let connector = ExcelConnector::new(config);
//! let data = connector.fetch_table("sales", None, None).await?;
//! ```

use std::io::Cursor;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use calamine::{open_workbook_from_rs, Data, Range, Reader, Xlsx, Xls};
use futures::stream::{self, BoxStream, StreamExt};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;

use crate::warehouse::connectors::file::{ExcelOptions, FileStorage, SheetSelector};
use crate::warehouse::connectors::schema_utils::arrow_schema_to_table_schema;
use crate::warehouse::connectors::{
    Connector, ConnectorError, ConnectorResult, FetchOptions, RecordBatchStream, TableInfo,
};
use crate::warehouse::types::{SourceType, TableSchema};

/// Default batch size for reading records.
const DEFAULT_BATCH_SIZE: usize = 8192;

/// Maximum rows to sample for schema inference.
const DEFAULT_SCHEMA_SAMPLE_SIZE: usize = 1000;

/// Excel connector configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExcelConnectorConfig {
    /// Path or URL to the Excel file
    pub path: String,
    /// Storage type (local, S3, GCS, HTTP)
    #[serde(skip)]
    pub storage: Option<FileStorage>,
    /// Excel parsing options
    #[serde(default)]
    pub options: ExcelOptions,
    /// Table name to use (derived from filename if not set)
    pub table_name: Option<String>,
    /// Batch size for reading
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl ExcelConnectorConfig {
    /// Create a new Excel connector configuration for a local file.
    pub fn new(path: impl Into<String>) -> Self {
        let path = path.into();
        Self {
            path,
            storage: None,
            options: ExcelOptions::default(),
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
            options: ExcelOptions::default(),
            table_name: None,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Set sheet to read by index (0-based).
    pub fn with_sheet_index(mut self, index: usize) -> Self {
        self.options.sheet = SheetSelector::Index(index);
        self
    }

    /// Set sheet to read by name.
    pub fn with_sheet_name(mut self, name: impl Into<String>) -> Self {
        self.options.sheet = SheetSelector::Name(name.into());
        self
    }

    /// Set whether the first row is a header.
    pub fn with_header(mut self, has_header: bool) -> Self {
        self.options.has_header = has_header;
        self
    }

    /// Set the cell range to read (e.g., "A1:D100").
    pub fn with_range(mut self, range: impl Into<String>) -> Self {
        self.options.range = Some(range.into());
        self
    }

    /// Set the number of rows to skip from the top.
    pub fn with_skip_rows(mut self, skip: usize) -> Self {
        self.options.skip_rows = skip;
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
            .unwrap_or("excel_data")
            .trim_end_matches(".xlsx")
            .trim_end_matches(".XLSX")
            .trim_end_matches(".xls")
            .trim_end_matches(".XLS")
            .to_string()
    }

    /// Check if the file is an xlsx format.
    pub fn is_xlsx(&self) -> bool {
        self.path.to_lowercase().ends_with(".xlsx")
    }
}

/// Excel file data source connector.
///
/// Uses caching for both data and schema to avoid repeated I/O and inference operations.
pub struct ExcelConnector {
    config: ExcelConnectorConfig,
    /// HTTP client for remote files
    client: reqwest::Client,
    /// Cached file data - uses Arc for cheap cloning
    cached_data: OnceCell<Arc<[u8]>>,
    /// Cached inferred schema
    cached_schema: OnceLock<TableSchema>,
    /// Stored ETag for change detection
    stored_etag: RwLock<Option<String>>,
}

impl ExcelConnector {
    /// Create a new Excel connector.
    pub fn new(config: ExcelConnectorConfig) -> Self {
        Self {
            config,
            client: reqwest::Client::new(),
            cached_data: OnceCell::new(),
            cached_schema: OnceLock::new(),
            stored_etag: RwLock::new(None),
        }
    }

    /// Create a connector with pre-loaded data (for testing or embedding).
    pub fn with_data(config: ExcelConnectorConfig, data: Vec<u8>) -> Self {
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

    /// Fetch the Excel file data (cached after first fetch).
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
                        "Failed to read Excel file '{}': {}",
                        self.config.path, e
                    ))
                })
        }
    }

    /// Open and read the sheet from Excel data.
    fn read_sheet(&self, data: &[u8]) -> ConnectorResult<Range<Data>> {
        let cursor = Cursor::new(data);

        if self.config.is_xlsx() {
            let mut workbook: Xlsx<_> = open_workbook_from_rs(cursor).map_err(|e| {
                ConnectorError::Internal(format!("Failed to open xlsx workbook: {}", e))
            })?;

            self.get_sheet_range(&mut workbook)
        } else {
            let mut workbook: Xls<_> = open_workbook_from_rs(cursor).map_err(|e| {
                ConnectorError::Internal(format!("Failed to open xls workbook: {}", e))
            })?;

            self.get_sheet_range(&mut workbook)
        }
    }

    /// Get the sheet range based on configuration.
    fn get_sheet_range<RS: std::io::Read + std::io::Seek, R: Reader<RS>>(&self, workbook: &mut R) -> ConnectorResult<Range<Data>> {
        let sheet_names = workbook.sheet_names().to_vec();

        let sheet_name = match &self.config.options.sheet {
            SheetSelector::Index(idx) => {
                sheet_names.get(*idx).cloned().ok_or_else(|| {
                    ConnectorError::Config(format!(
                        "Sheet index {} out of range (found {} sheets)",
                        idx,
                        sheet_names.len()
                    ))
                })?
            }
            SheetSelector::Name(name) => {
                if sheet_names.contains(name) {
                    name.clone()
                } else {
                    return Err(ConnectorError::Config(format!(
                        "Sheet '{}' not found. Available: {:?}",
                        name, sheet_names
                    )));
                }
            }
        };

        // Get full sheet range (range filtering is applied when reading data)
        workbook
            .worksheet_range(&sheet_name)
            .map_err(|e| ConnectorError::Internal(format!("Failed to read sheet: {:?}", e)))
    }

    /// Infer schema from the sheet data.
    fn infer_schema(&self, range: &Range<Data>) -> ConnectorResult<Schema> {
        let height = range.height();
        if height == 0 {
            return Err(ConnectorError::SchemaInference(
                "Empty sheet - no data to infer schema from".to_string(),
            ));
        }

        let skip = self.config.options.skip_rows;
        let header_row = skip;
        let data_start = if self.config.options.has_header { skip + 1 } else { skip };

        let width = range.width();

        // Get column names
        let column_names: Vec<String> = if self.config.options.has_header && header_row < height {
            (0..width)
                .map(|col| {
                    range.get((header_row, col))
                        .map(|cell| cell_to_string(cell))
                        .unwrap_or_else(|| format!("column_{}", col))
                })
                .collect()
        } else {
            (0..width).map(|col| format!("column_{}", col)).collect()
        };

        // Infer types from data rows
        let sample_end = (data_start + DEFAULT_SCHEMA_SAMPLE_SIZE).min(height);
        let column_types: Vec<DataType> = (0..width)
            .map(|col| {
                let mut has_int = false;
                let mut has_float = false;
                let mut has_bool = false;
                let mut has_string = false;

                for row in data_start..sample_end {
                    if let Some(cell) = range.get((row, col)) {
                        match cell {
                            Data::Int(_) => has_int = true,
                            Data::Float(_) => has_float = true,
                            Data::Bool(_) => has_bool = true,
                            Data::String(_) => has_string = true,
                            Data::DateTime(_) | Data::DateTimeIso(_) | Data::DurationIso(_) => {
                                has_string = true
                            }
                            Data::Error(_) | Data::Empty => {}
                        }
                    }
                }

                // Determine best type
                if has_string {
                    DataType::Utf8
                } else if has_float {
                    DataType::Float64
                } else if has_int {
                    DataType::Int64
                } else if has_bool {
                    DataType::Boolean
                } else {
                    DataType::Utf8 // Default to string
                }
            })
            .collect();

        let fields: Vec<Field> = column_names
            .into_iter()
            .zip(column_types)
            .map(|(name, dtype)| Field::new(name, dtype, true))
            .collect();

        Ok(Schema::new(fields))
    }

    /// Get the cached TableSchema, inferring it if necessary.
    fn get_cached_table_schema(&self, data: &[u8]) -> ConnectorResult<TableSchema> {
        if let Some(schema) = self.cached_schema.get() {
            return Ok(schema.clone());
        }

        let range = self.read_sheet(data)?;
        let arrow_schema = self.infer_schema(&range)?;
        let table_schema = arrow_schema_to_table_schema(&arrow_schema);

        Ok(self
            .cached_schema
            .get_or_init(|| table_schema.clone())
            .clone())
    }

    /// Convert sheet range to Arrow RecordBatches.
    fn sheet_to_batches(
        &self,
        range: &Range<Data>,
        schema: SchemaRef,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let height = range.height();
        if height == 0 {
            return Ok(vec![]);
        }

        let skip = self.config.options.skip_rows;
        let data_start = if self.config.options.has_header { skip + 1 } else { skip };

        let num_rows = height.saturating_sub(data_start);
        if num_rows == 0 {
            return Ok(vec![]);
        }

        // Build arrays for each column
        let arrays: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(col_idx, field)| {
                self.column_to_array(range, col_idx, data_start, height, field.data_type())
            })
            .collect::<ConnectorResult<Vec<_>>>()?;

        let batch = RecordBatch::try_new(schema, arrays)
            .map_err(|e| ConnectorError::Internal(format!("Failed to create RecordBatch: {}", e)))?;

        Ok(vec![batch])
    }

    /// Convert a column to an Arrow array.
    fn column_to_array(
        &self,
        range: &Range<Data>,
        col_idx: usize,
        data_start: usize,
        height: usize,
        data_type: &DataType,
    ) -> ConnectorResult<ArrayRef> {
        match data_type {
            DataType::Int64 => {
                let values: Vec<Option<i64>> = (data_start..height)
                    .map(|row| {
                        range
                            .get((row, col_idx))
                            .and_then(cell_to_i64)
                    })
                    .collect();
                Ok(Arc::new(Int64Array::from(values)))
            }
            DataType::Float64 => {
                let values: Vec<Option<f64>> = (data_start..height)
                    .map(|row| {
                        range
                            .get((row, col_idx))
                            .and_then(cell_to_f64)
                    })
                    .collect();
                Ok(Arc::new(Float64Array::from(values)))
            }
            DataType::Boolean => {
                let values: Vec<Option<bool>> = (data_start..height)
                    .map(|row| {
                        range
                            .get((row, col_idx))
                            .and_then(cell_to_bool)
                    })
                    .collect();
                Ok(Arc::new(BooleanArray::from(values)))
            }
            DataType::Utf8 | _ => {
                let values: Vec<Option<String>> = (data_start..height)
                    .map(|row| {
                        range
                            .get((row, col_idx))
                            .map(cell_to_string)
                            .filter(|s| !s.is_empty())
                    })
                    .collect();
                Ok(Arc::new(StringArray::from(values)))
            }
        }
    }
}

/// Convert a calamine cell to a string.
fn cell_to_string(cell: &Data) -> String {
    match cell {
        Data::Int(i) => i.to_string(),
        Data::Float(f) => f.to_string(),
        Data::String(s) => s.clone(),
        Data::Bool(b) => b.to_string(),
        Data::DateTime(dt) => format!("{}", dt),
        Data::DateTimeIso(dt) => dt.clone(),
        Data::DurationIso(d) => d.clone(),
        Data::Error(e) => format!("#ERROR: {:?}", e),
        Data::Empty => String::new(),
    }
}

/// Convert a calamine cell to i64.
fn cell_to_i64(cell: &Data) -> Option<i64> {
    match cell {
        Data::Int(i) => Some(*i),
        Data::Float(f) => Some(*f as i64),
        Data::String(s) => s.parse().ok(),
        Data::Empty => None,
        _ => None,
    }
}

/// Convert a calamine cell to f64.
fn cell_to_f64(cell: &Data) -> Option<f64> {
    match cell {
        Data::Int(i) => Some(*i as f64),
        Data::Float(f) => Some(*f),
        Data::String(s) => s.parse().ok(),
        Data::Empty => None,
        _ => None,
    }
}

/// Convert a calamine cell to bool.
fn cell_to_bool(cell: &Data) -> Option<bool> {
    match cell {
        Data::Bool(b) => Some(*b),
        Data::Int(i) => Some(*i != 0),
        Data::Float(f) => Some(*f != 0.0),
        Data::String(s) => {
            let s = s.to_lowercase();
            match s.as_str() {
                "true" | "yes" | "1" => Some(true),
                "false" | "no" | "0" => Some(false),
                _ => None,
            }
        }
        Data::Empty => None,
        _ => None,
    }
}

#[async_trait]
impl Connector for ExcelConnector {
    fn source_type(&self) -> SourceType {
        SourceType::Excel
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        // Fetch data (cached after first call)
        let data = self.fetch_data().await?;

        // Get cached schema (cached after first inference)
        let table_schema = self.get_cached_table_schema(&data)?;

        // Excel files are treated as a single table
        let table_name = self.config.get_table_name();

        // Read sheet to get row count
        let range = self.read_sheet(&data)?;
        let skip = self.config.options.skip_rows;
        let data_start = if self.config.options.has_header { skip + 1 } else { skip };
        let estimated_rows = range.height().saturating_sub(data_start) as u64;

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

        // Read sheet
        let range = self.read_sheet(&data)?;

        // Infer Arrow schema
        let arrow_schema = Arc::new(self.infer_schema(&range)?);

        // Convert to RecordBatches
        let batches = self.sheet_to_batches(&range, arrow_schema)?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        tracing::info!(
            path = %self.config.path,
            rows = total_rows,
            batches = batches.len(),
            "Fetched Excel data"
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
            // For Excel files, we read all data and stream it
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
                        "Excel file not found: {}",
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_excel_connector_config_creation() {
        let config = ExcelConnectorConfig::new("/data/test.xlsx");
        assert_eq!(config.path, "/data/test.xlsx");
        assert!(config.options.has_header);
        assert!(config.is_xlsx());
    }

    #[test]
    fn test_excel_connector_config_xls() {
        let config = ExcelConnectorConfig::new("/data/test.xls");
        assert!(!config.is_xlsx());
    }

    #[test]
    fn test_excel_connector_config_get_table_name() {
        let config = ExcelConnectorConfig::new("/data/report.xlsx");
        assert_eq!(config.get_table_name(), "report");

        let config = ExcelConnectorConfig::new("/data/report.xls");
        assert_eq!(config.get_table_name(), "report");

        let config = ExcelConnectorConfig::new("http://example.com/data.xlsx");
        assert_eq!(config.get_table_name(), "data");
    }

    #[test]
    fn test_excel_connector_config_with_table_name() {
        let config = ExcelConnectorConfig::new("/data/test.xlsx").with_table_name("custom_table");
        assert_eq!(config.get_table_name(), "custom_table");
    }

    #[test]
    fn test_excel_connector_config_with_sheet_name() {
        let config = ExcelConnectorConfig::new("/data/test.xlsx")
            .with_sheet_name("Sales Data");
        assert!(matches!(config.options.sheet, SheetSelector::Name(_)));
    }

    #[test]
    fn test_excel_connector_config_with_sheet_index() {
        let config = ExcelConnectorConfig::new("/data/test.xlsx")
            .with_sheet_index(2);
        assert!(matches!(config.options.sheet, SheetSelector::Index(2)));
    }

    #[test]
    fn test_excel_connector_config_with_range() {
        let config = ExcelConnectorConfig::new("/data/test.xlsx")
            .with_range("A1:D100");
        assert_eq!(config.options.range, Some("A1:D100".to_string()));
    }

    #[test]
    fn test_excel_connector_config_with_skip_rows() {
        let config = ExcelConnectorConfig::new("/data/test.xlsx")
            .with_skip_rows(2);
        assert_eq!(config.options.skip_rows, 2);
    }

    #[test]
    fn test_cell_to_string() {
        assert_eq!(cell_to_string(&Data::Int(42)), "42");
        assert_eq!(cell_to_string(&Data::Float(3.14)), "3.14");
        assert_eq!(
            cell_to_string(&Data::String("hello".to_string())),
            "hello"
        );
        assert_eq!(cell_to_string(&Data::Bool(true)), "true");
        assert_eq!(cell_to_string(&Data::Empty), "");
    }

    #[test]
    fn test_cell_to_i64() {
        assert_eq!(cell_to_i64(&Data::Int(42)), Some(42));
        assert_eq!(cell_to_i64(&Data::Float(3.14)), Some(3));
        assert_eq!(
            cell_to_i64(&Data::String("100".to_string())),
            Some(100)
        );
        assert_eq!(cell_to_i64(&Data::Empty), None);
    }

    #[test]
    fn test_cell_to_f64() {
        assert_eq!(cell_to_f64(&Data::Int(42)), Some(42.0));
        assert_eq!(cell_to_f64(&Data::Float(3.14)), Some(3.14));
        assert_eq!(
            cell_to_f64(&Data::String("2.5".to_string())),
            Some(2.5)
        );
        assert_eq!(cell_to_f64(&Data::Empty), None);
    }

    #[test]
    fn test_cell_to_bool() {
        assert_eq!(cell_to_bool(&Data::Bool(true)), Some(true));
        assert_eq!(cell_to_bool(&Data::Bool(false)), Some(false));
        assert_eq!(cell_to_bool(&Data::Int(1)), Some(true));
        assert_eq!(cell_to_bool(&Data::Int(0)), Some(false));
        assert_eq!(
            cell_to_bool(&Data::String("yes".to_string())),
            Some(true)
        );
        assert_eq!(
            cell_to_bool(&Data::String("no".to_string())),
            Some(false)
        );
        assert_eq!(cell_to_bool(&Data::Empty), None);
    }

    #[test]
    fn test_source_type() {
        let config = ExcelConnectorConfig::new("/data/test.xlsx");
        let connector = ExcelConnector::new(config);
        assert_eq!(connector.source_type(), SourceType::Excel);
    }
}
