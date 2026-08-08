//! Google Sheets Connector
//!
//! Connects to Google Sheets and allows querying spreadsheet data in place.
//!
//! # Features
//!
//! - Query spreadsheet data in real-time via Google Sheets API v4
//! - TTL-based caching to minimize API calls
//! - Automatic schema inference from header row and data sampling
//! - OAuth 2.0 authentication via refresh tokens
//! - Each sheet tab becomes a queryable table
//!
//! # Architecture
//!
//! Unlike sync-based connectors, Google Sheets uses cold tier:
//! - Data is fetched from the API on each query (with TTL caching)
//! - No data is stored in ClickHouse
//! - All filtering happens after fetching (no server-side pushdown)
//!
//! # Usage
//!
//! ```ignore
//! let config = GoogleSheetsConfig::new("spreadsheet_id_here")
//!     .with_cache_ttl(Duration::from_secs(60));
//!
//! let connector = GoogleSheetsConnector::new(config, oauth_config);
//! let tables = connector.list_tables().await?;
//! let data = connector.fetch_table("Sheet1", None, None).await?;
//! ```

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info};

use super::oauth::OAuthConfig;
use super::{Connector, ConnectorError, ConnectorResult, FetchOptions, RecordBatchStream, TableInfo};
use crate::warehouse::types::{ColumnSchema, ColumnType, SourceType, TableSchema};

// =============================================================================
// Constants
// =============================================================================

/// Google Sheets API base URL
const SHEETS_API_BASE: &str = "https://sheets.googleapis.com/v4/spreadsheets";

/// Default cache TTL in seconds
const DEFAULT_CACHE_TTL_SECS: u64 = 60;

/// Maximum rows to sample for type inference
const TYPE_INFERENCE_SAMPLE_SIZE: usize = 100;

/// Google Sheets API scopes required
pub const SHEETS_READONLY_SCOPE: &str = "https://www.googleapis.com/auth/spreadsheets.readonly";

// =============================================================================
// Configuration
// =============================================================================

/// Google Sheets connector configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoogleSheetsConfig {
    /// Google Spreadsheet ID (from the URL)
    pub spreadsheet_id: String,
    /// Cache TTL in seconds (default: 60)
    #[serde(default = "default_cache_ttl")]
    pub cache_ttl_secs: u64,
    /// Whether the first row contains headers (default: true)
    #[serde(default = "default_true")]
    pub first_row_is_header: bool,
    /// Specific sheets to expose (empty = all sheets)
    #[serde(default)]
    pub sheets: Vec<String>,
}

fn default_cache_ttl() -> u64 {
    DEFAULT_CACHE_TTL_SECS
}

fn default_true() -> bool {
    true
}

impl GoogleSheetsConfig {
    /// Create a new Google Sheets configuration.
    ///
    /// # Arguments
    /// * `spreadsheet_id` - The spreadsheet ID from the Google Sheets URL
    ///   (e.g., from `https://docs.google.com/spreadsheets/d/SPREADSHEET_ID/edit`)
    pub fn new(spreadsheet_id: impl Into<String>) -> Self {
        Self {
            spreadsheet_id: spreadsheet_id.into(),
            cache_ttl_secs: DEFAULT_CACHE_TTL_SECS,
            first_row_is_header: true,
            sheets: Vec::new(),
        }
    }

    /// Set the cache TTL.
    pub fn with_cache_ttl(mut self, ttl: Duration) -> Self {
        self.cache_ttl_secs = ttl.as_secs();
        self
    }

    /// Set whether the first row is a header.
    pub fn with_header(mut self, has_header: bool) -> Self {
        self.first_row_is_header = has_header;
        self
    }

    /// Set specific sheets to expose.
    pub fn with_sheets(mut self, sheets: Vec<String>) -> Self {
        self.sheets = sheets;
        self
    }

    /// Get the cache TTL as a Duration.
    pub fn cache_ttl(&self) -> Duration {
        Duration::from_secs(self.cache_ttl_secs)
    }
}

// =============================================================================
// Cache
// =============================================================================

/// Cached sheet data with TTL.
/// 
/// Uses `Arc<Vec<RecordBatch>>` to avoid cloning the vector on cache hits.
/// RecordBatch itself uses Arc internally, so this is very cheap.
struct CacheEntry {
    /// Cached Arrow RecordBatches (wrapped in Arc for cheap cloning)
    data: Arc<Vec<RecordBatch>>,
    /// When the data was fetched
    fetched_at: Instant,
    /// TTL for this entry
    ttl: Duration,
}

impl CacheEntry {
    /// Check if this cache entry is still valid.
    fn is_valid(&self) -> bool {
        self.fetched_at.elapsed() < self.ttl
    }
}

/// TTL cache for sheet data.
struct SheetsCache {
    entries: HashMap<String, CacheEntry>,
}

impl SheetsCache {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Get cached data if valid.
    /// 
    /// Returns an Arc clone which is O(1) - no data copying.
    fn get(&self, sheet_name: &str) -> Option<Arc<Vec<RecordBatch>>> {
        self.entries.get(sheet_name).and_then(|entry| {
            if entry.is_valid() {
                Some(Arc::clone(&entry.data))
            } else {
                None
            }
        })
    }

    /// Insert data into cache.
    fn insert(&mut self, sheet_name: String, data: Arc<Vec<RecordBatch>>, ttl: Duration) {
        self.entries.insert(
            sheet_name,
            CacheEntry {
                data,
                fetched_at: Instant::now(),
                ttl,
            },
        );
    }

}

/// Cached metadata with TTL.
struct MetadataCacheEntry {
    /// Cached metadata
    data: SpreadsheetMetadata,
    /// When the metadata was fetched
    fetched_at: Instant,
    /// TTL for this entry (typically longer than data cache)
    ttl: Duration,
}

impl MetadataCacheEntry {
    /// Check if this cache entry is still valid.
    fn is_valid(&self) -> bool {
        self.fetched_at.elapsed() < self.ttl
    }
}

/// Default metadata cache TTL - 5 minutes (longer than data cache since sheet structure changes rarely)
const METADATA_CACHE_TTL_SECS: u64 = 300;

// =============================================================================
// API Response Types
// =============================================================================

/// Response from GET /v4/spreadsheets/{id}
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SpreadsheetMetadata {
    #[allow(dead_code)]
    spreadsheet_id: String,
    #[allow(dead_code)]
    properties: SpreadsheetProperties,
    sheets: Vec<SheetMetadata>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SpreadsheetProperties {
    #[allow(dead_code)]
    title: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SheetMetadata {
    properties: SheetProperties,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SheetProperties {
    #[allow(dead_code)]
    sheet_id: i64,
    title: String,
    #[serde(default)]
    grid_properties: Option<GridProperties>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GridProperties {
    row_count: Option<i32>,
    #[allow(dead_code)]
    column_count: Option<i32>,
}

/// Response from GET /v4/spreadsheets/{id}/values/{range}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ValueRange {
    #[allow(dead_code)]
    range: String,
    #[allow(dead_code)]
    major_dimension: Option<String>,
    values: Option<Vec<Vec<serde_json::Value>>>,
}

/// Response from batchGet API for fetching multiple sheets at once.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BatchGetResponse {
    /// Each ValueRange corresponds to one requested range
    value_ranges: Option<Vec<ValueRange>>,
}

// =============================================================================
// Connector Implementation
// =============================================================================

/// Google Sheets data source connector.
///
/// Fetches spreadsheet data in real-time with TTL caching.
/// 
/// # Thread Safety
/// 
/// Uses `Arc<RwLock<_>>` for caches to ensure `Send + Sync` bounds
/// required by the `Connector` trait for async execution across threads.
pub struct GoogleSheetsConnector {
    config: GoogleSheetsConfig,
    oauth: Arc<OAuthConfig>,
    client: reqwest::Client,
    cache: Arc<RwLock<SheetsCache>>,
    /// Cached spreadsheet metadata with TTL
    metadata_cache: Arc<RwLock<Option<MetadataCacheEntry>>>,
}

impl std::fmt::Debug for GoogleSheetsConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GoogleSheetsConnector")
            .field("spreadsheet_id", &self.config.spreadsheet_id)
            .field("cache_ttl_secs", &self.config.cache_ttl_secs)
            .finish()
    }
}

impl GoogleSheetsConnector {
    /// Create a new Google Sheets connector.
    pub fn new(config: GoogleSheetsConfig, oauth: OAuthConfig) -> Self {
        Self {
            config,
            oauth: Arc::new(oauth),
            client: reqwest::Client::new(),
            cache: Arc::new(RwLock::new(SheetsCache::new())),
            metadata_cache: Arc::new(RwLock::new(None)),
        }
    }

    /// Create a connector with a shared OAuth config.
    pub fn with_shared_oauth(config: GoogleSheetsConfig, oauth: Arc<OAuthConfig>) -> Self {
        Self {
            config,
            oauth,
            client: reqwest::Client::new(),
            cache: Arc::new(RwLock::new(SheetsCache::new())),
            metadata_cache: Arc::new(RwLock::new(None)),
        }
    }

    /// Get the spreadsheet ID.
    pub fn spreadsheet_id(&self) -> &str {
        &self.config.spreadsheet_id
    }

    /// Get authorization header from OAuth config.
    async fn get_auth_header(&self) -> ConnectorResult<String> {
        self.oauth
            .authorization_header()
            .await
            .map_err(|e| ConnectorError::Authentication(e.to_string()))
    }

    /// Fetch spreadsheet metadata (sheet names, properties).
    /// 
    /// Metadata is cached with a 5-minute TTL since sheet structure changes rarely.
    async fn fetch_metadata(&self) -> ConnectorResult<SpreadsheetMetadata> {
        // Check cache first (with TTL validation)
        {
            let cache = self.metadata_cache.read().await;
            if let Some(entry) = cache.as_ref() {
                if entry.is_valid() {
                    debug!(spreadsheet_id = %self.config.spreadsheet_id, "Returning cached metadata");
                    return Ok(entry.data.clone());
                }
            }
        }

        let url = format!("{}/{}", SHEETS_API_BASE, self.config.spreadsheet_id);
        let auth_header = self.get_auth_header().await?;

        debug!(spreadsheet_id = %self.config.spreadsheet_id, "Fetching spreadsheet metadata from API");

        let response = self
            .client
            .get(&url)
            .header("Authorization", auth_header)
            .send()
            .await
            .map_err(|e| ConnectorError::Network(format!("Failed to connect to Google Sheets API: {}", e)))?;

        let response = self.check_api_response(response).await?;

        let metadata: SpreadsheetMetadata = response
            .json()
            .await
            .map_err(|e| ConnectorError::Internal(format!("Failed to parse spreadsheet metadata: {}", e)))?;

        // Cache the metadata with TTL
        {
            let mut cache = self.metadata_cache.write().await;
            *cache = Some(MetadataCacheEntry {
                data: metadata.clone(),
                fetched_at: Instant::now(),
                ttl: Duration::from_secs(METADATA_CACHE_TTL_SECS),
            });
        }

        Ok(metadata)
    }
    

    /// Fetch all values from a sheet.
    async fn fetch_sheet_values(&self, sheet_name: &str) -> ConnectorResult<Vec<Vec<serde_json::Value>>> {
        let range = format!("'{}'", sheet_name); // Fetch entire sheet
        let url = format!(
            "{}/{}/values/{}",
            SHEETS_API_BASE,
            self.config.spreadsheet_id,
            urlencoding::encode(&range)
        );
        let auth_header = self.get_auth_header().await?;

        debug!(sheet = %sheet_name, "Fetching sheet values");

        let response = self
            .client
            .get(&url)
            .header("Authorization", auth_header)
            .query(&[("valueRenderOption", "UNFORMATTED_VALUE")])
            .send()
            .await
            .map_err(|e| ConnectorError::Network(format!("Failed to fetch sheet values: {}", e)))?;

        let response = self.check_api_response(response).await?;

        let value_range: ValueRange = response
            .json()
            .await
            .map_err(|e| ConnectorError::Internal(format!("Failed to parse sheet values: {}", e)))?;

        Ok(value_range.values.unwrap_or_default())
    }

    /// Fetch values from multiple sheets in a single API call.
    /// 
    /// Uses the batchGet endpoint to reduce API calls from N to 1.
    /// Returns a map of sheet_name -> values.
    async fn fetch_sheet_values_batch(
        &self,
        sheet_names: &[String],
    ) -> ConnectorResult<HashMap<String, Vec<Vec<serde_json::Value>>>> {
        if sheet_names.is_empty() {
            return Ok(HashMap::new());
        }

        // Build ranges - one per sheet
        let ranges: Vec<String> = sheet_names
            .iter()
            .map(|name| format!("'{}'", name))
            .collect();

        let url = format!(
            "{}/{}/values:batchGet",
            SHEETS_API_BASE,
            self.config.spreadsheet_id,
        );
        let auth_header = self.get_auth_header().await?;

        debug!(
            spreadsheet_id = %self.config.spreadsheet_id,
            num_sheets = sheet_names.len(),
            "Fetching multiple sheet values via batchGet"
        );

        // Build query params - multiple "ranges" parameters
        let range_params: Vec<(&str, String)> = ranges
            .iter()
            .map(|r| ("ranges", urlencoding::encode(r).into_owned()))
            .collect();

        let mut request = self
            .client
            .get(&url)
            .header("Authorization", auth_header)
            .query(&[("valueRenderOption", "UNFORMATTED_VALUE")]);

        for (key, value) in &range_params {
            request = request.query(&[(key, value)]);
        }

        let response = request
            .send()
            .await
            .map_err(|e| ConnectorError::Network(format!("Failed to fetch batch sheet values: {}", e)))?;

        let response = self.check_api_response(response).await?;

        let batch_response: BatchGetResponse = response
            .json()
            .await
            .map_err(|e| ConnectorError::Internal(format!("Failed to parse batch response: {}", e)))?;

        // Map responses back to sheet names
        let mut result = HashMap::new();
        if let Some(value_ranges) = batch_response.value_ranges {
            for (idx, vr) in value_ranges.into_iter().enumerate() {
                if idx < sheet_names.len() {
                    result.insert(
                        sheet_names[idx].clone(),
                        vr.values.unwrap_or_default(),
                    );
                }
            }
        }

        Ok(result)
    }

    /// Check API response status and return error if not successful.
    /// 
    /// Takes ownership of the response to allow reading error body for better messages.
    /// Returns the response on success for further processing.
    async fn check_api_response(&self, response: reqwest::Response) -> ConnectorResult<reqwest::Response> {
        if response.status().is_success() {
            return Ok(response);
        }

        let status = response.status();
        let retry_after = response
            .headers()
            .get("Retry-After")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok());

        // Read error body for better error messages
        let error_body = response.text().await.unwrap_or_default();

        if status == reqwest::StatusCode::UNAUTHORIZED {
            return Err(ConnectorError::Authentication(format!(
                "Google Sheets API authentication failed. Token may be expired. Details: {}",
                if error_body.is_empty() { "No details" } else { &error_body }
            )));
        }

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(ConnectorError::RateLimited {
                retry_after_secs: retry_after.unwrap_or(60),
            });
        }

        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(ConnectorError::TableNotFound(format!(
                "Spreadsheet not found: {}. Details: {}",
                self.config.spreadsheet_id,
                if error_body.is_empty() { "No details" } else { &error_body }
            )));
        }

        if status == reqwest::StatusCode::FORBIDDEN {
            return Err(ConnectorError::Authentication(format!(
                "Access denied to spreadsheet. Check sharing permissions. Details: {}",
                if error_body.is_empty() { "No details" } else { &error_body }
            )));
        }

        Err(ConnectorError::Internal(format!(
            "Google Sheets API error ({}): {}",
            status,
            if error_body.is_empty() { "No details".to_string() } else { error_body }
        )))
    }

    /// Infer column types from sample data.
    fn infer_column_types(&self, rows: &[Vec<serde_json::Value>], num_columns: usize) -> Vec<DataType> {
        let mut types = vec![DataType::Utf8; num_columns];

        // Sample rows for type inference
        let sample_size = std::cmp::min(rows.len(), TYPE_INFERENCE_SAMPLE_SIZE);
        let sample = &rows[..sample_size];

        for col_idx in 0..num_columns {
            let mut has_int = false;
            let mut has_float = false;
            let mut has_bool = false;
            let mut has_string = false;
            let mut non_null_count = 0;

            for row in sample {
                if col_idx >= row.len() {
                    continue;
                }

                let value = &row[col_idx];
                if value.is_null() || (value.is_string() && value.as_str().unwrap_or("").is_empty()) {
                    continue;
                }

                non_null_count += 1;

                match value {
                    serde_json::Value::Bool(_) => has_bool = true,
                    serde_json::Value::Number(n) => {
                        if n.is_f64() && n.as_f64().map(|f| f.fract() != 0.0).unwrap_or(false) {
                            has_float = true;
                        } else {
                            has_int = true;
                        }
                    }
                    serde_json::Value::String(_) => has_string = true,
                    _ => has_string = true,
                }
            }

            // Determine type based on observed values
            // Priority: String > Float > Int > Boolean (to handle mixed types safely)
            if non_null_count == 0 {
                types[col_idx] = DataType::Utf8; // Default to string for empty columns
            } else if has_string {
                types[col_idx] = DataType::Utf8;
            } else if has_bool && !has_int && !has_float {
                types[col_idx] = DataType::Boolean;
            } else if has_float {
                types[col_idx] = DataType::Float64;
            } else if has_int {
                types[col_idx] = DataType::Int64;
            }
        }

        types
    }

    /// Extract headers and data rows from sheet values.
    /// 
    /// If `first_row_is_header` is true, uses the first row as column names.
    /// Otherwise, generates column names as "column_0", "column_1", etc.
    /// 
    /// Returns (headers, data_rows) where data_rows is a slice of the input.
    fn extract_headers_and_data<'a>(
        &self,
        values: &'a [Vec<serde_json::Value>],
    ) -> (Vec<String>, &'a [Vec<serde_json::Value>]) {
        if values.is_empty() {
            return (Vec::new(), &[]);
        }

        if self.config.first_row_is_header {
            let header_row = &values[0];
            let headers: Vec<String> = header_row
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    v.as_str()
                        .filter(|s| !s.is_empty())
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| format!("column_{}", i))
                })
                .collect();
            (headers, &values[1..])
        } else {
            let num_cols = values.iter().map(|r| r.len()).max().unwrap_or(0);
            let headers: Vec<String> = (0..num_cols).map(|i| format!("column_{}", i)).collect();
            (headers, values)
        }
    }

    /// Convert sheet values to Arrow RecordBatch.
    fn values_to_record_batch(
        &self,
        values: Vec<Vec<serde_json::Value>>,
        headers: &[String],
        types: &[DataType],
    ) -> ConnectorResult<RecordBatch> {
        let _num_rows = values.len();
        let num_columns = headers.len();

        // Build Arrow arrays for each column
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(num_columns);

        for (col_idx, dtype) in types.iter().enumerate() {
            let array: ArrayRef = match dtype {
                DataType::Int64 => {
                    let values: Vec<Option<i64>> = values
                        .iter()
                        .map(|row| {
                            row.get(col_idx)
                                .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)))
                        })
                        .collect();
                    Arc::new(Int64Array::from(values))
                }
                DataType::Float64 => {
                    let values: Vec<Option<f64>> = values
                        .iter()
                        .map(|row| row.get(col_idx).and_then(|v| v.as_f64()))
                        .collect();
                    Arc::new(Float64Array::from(values))
                }
                DataType::Boolean => {
                    let values: Vec<Option<bool>> = values
                        .iter()
                        .map(|row| row.get(col_idx).and_then(|v| v.as_bool()))
                        .collect();
                    Arc::new(BooleanArray::from(values))
                }
                _ => {
                    // Default to string
                    let values: Vec<Option<String>> = values
                        .iter()
                        .map(|row| {
                            row.get(col_idx).and_then(|v| {
                                if v.is_null() {
                                    None
                                } else if let Some(s) = v.as_str() {
                                    if s.is_empty() {
                                        None
                                    } else {
                                        Some(s.to_string())
                                    }
                                } else {
                                    Some(v.to_string())
                                }
                            })
                        })
                        .collect();
                    Arc::new(StringArray::from(values))
                }
            };
            arrays.push(array);
        }

        // Build schema
        let fields: Vec<Field> = headers
            .iter()
            .zip(types.iter())
            .map(|(name, dtype)| Field::new(name, dtype.clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        RecordBatch::try_new(schema, arrays)
            .map_err(|e| ConnectorError::Internal(format!("Failed to create RecordBatch: {}", e)))
    }

    /// Get cached data or fetch fresh.
    /// 
    /// Returns Vec<RecordBatch> for API compatibility. Internally uses Arc
    /// for efficient cache storage and retrieval.
    async fn fetch_cached(&self, sheet_name: &str) -> ConnectorResult<Vec<RecordBatch>> {
        // Check cache first - Arc clone is O(1)
        {
            let cache = self.cache.read().await;
            if let Some(data) = cache.get(sheet_name) {
                debug!(sheet = %sheet_name, "Returning cached sheet data");
                // Convert Arc<Vec<RecordBatch>> to Vec<RecordBatch>
                // This clones the Vec but RecordBatch clone is cheap (Arc internally)
                return Ok((*data).clone());
            }
        }

        // Fetch fresh data
        info!(sheet = %sheet_name, "Cache miss, fetching sheet data from API");
        let values = self.fetch_sheet_values(sheet_name).await?;

        if values.is_empty() {
            // Return empty batch with inferred schema
            let schema = Arc::new(Schema::empty());
            let batch = RecordBatch::new_empty(schema);
            return Ok(vec![batch]);
        }

        // Extract headers and data rows
        let (headers, data_rows) = self.extract_headers_and_data(&values);

        if data_rows.is_empty() {
            // Return empty batch with header schema
            let fields: Vec<Field> = headers
                .iter()
                .map(|name| Field::new(name, DataType::Utf8, true))
                .collect();
            let schema = Arc::new(Schema::new(fields));
            let batch = RecordBatch::new_empty(schema);
            return Ok(vec![batch]);
        }

        // Infer types from data
        let types = self.infer_column_types(data_rows, headers.len());

        // Convert to RecordBatch
        let batch = self.values_to_record_batch(data_rows.to_vec(), &headers, &types)?;
        let batches = Arc::new(vec![batch]);

        // Cache the result
        {
            let mut cache = self.cache.write().await;
            cache.insert(sheet_name.to_string(), Arc::clone(&batches), self.config.cache_ttl());
        }

        Ok((*batches).clone())
    }

    /// Build table schema from sheet data.
    fn build_table_schema(&self, headers: &[String], types: &[DataType]) -> TableSchema {
        let columns: Vec<ColumnSchema> = headers
            .iter()
            .zip(types.iter())
            .map(|(name, dtype)| {
                let col_type = match dtype {
                    DataType::Int64 => ColumnType::Int64,
                    DataType::Float64 => ColumnType::Float64,
                    DataType::Boolean => ColumnType::Boolean,
                    _ => ColumnType::String,
                };
                ColumnSchema::new(name, col_type, true)
            })
            .collect();
        TableSchema { columns }
    }
}

#[async_trait]
impl Connector for GoogleSheetsConnector {
    fn source_type(&self) -> SourceType {
        SourceType::GoogleSheets
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        let metadata = self.fetch_metadata().await?;

        // Collect sheet names to fetch (respecting config filter)
        let sheet_names: Vec<String> = metadata
            .sheets
            .iter()
            .map(|s| s.properties.title.clone())
            .filter(|name| {
                self.config.sheets.is_empty() || self.config.sheets.contains(name)
            })
            .collect();

        if sheet_names.is_empty() {
            return Ok(Vec::new());
        }

        // Fetch all sheets in one batch API call instead of N separate calls
        let values_map = self.fetch_sheet_values_batch(&sheet_names).await?;

        let mut tables = Vec::new();

        for sheet in &metadata.sheets {
            let sheet_name = &sheet.properties.title;

            // Skip sheets not in our list
            if !sheet_names.contains(sheet_name) {
                continue;
            }

            // Get values from batch response
            let values = values_map.get(sheet_name).cloned().unwrap_or_default();
            let (headers, data_rows) = self.extract_headers_and_data(&values);

            let types = if data_rows.is_empty() {
                vec![DataType::Utf8; headers.len()]
            } else {
                self.infer_column_types(data_rows, headers.len())
            };

            let schema = self.build_table_schema(&headers, &types);
            let estimated_rows = sheet
                .properties
                .grid_properties
                .as_ref()
                .and_then(|g| g.row_count)
                .map(|r| (r as u64).saturating_sub(1)); // Subtract header row

            tables.push(TableInfo {
                name: sheet_name.clone(),
                schema,
                supports_incremental: false, // Google Sheets doesn't support incremental sync
                incremental_key: None,
                estimated_rows,
                primary_key_columns: Vec::new(),
            });
        }

        Ok(tables)
    }

    async fn get_schema(&self, table: &str) -> ConnectorResult<TableSchema> {
        let values = self.fetch_sheet_values(table).await?;

        if values.is_empty() {
            return Ok(TableSchema { columns: vec![] });
        }

        let (headers, data_rows) = self.extract_headers_and_data(&values);

        let types = if data_rows.is_empty() {
            vec![DataType::Utf8; headers.len()]
        } else {
            self.infer_column_types(data_rows, headers.len())
        };

        Ok(self.build_table_schema(&headers, &types))
    }

    async fn fetch_table(
        &self,
        table: &str,
        _incremental_key: Option<&str>,
        _last_value: Option<&str>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        // Note: Google Sheets doesn't support incremental sync,
        // so we ignore incremental_key and last_value
        self.fetch_cached(table).await
    }

    fn fetch_table_stream<'a>(
        &'a self,
        table: &'a str,
        options: FetchOptions,
    ) -> Pin<Box<dyn futures::Future<Output = ConnectorResult<RecordBatchStream>> + Send + 'a>> {
        Box::pin(async move {
            let batches = self.fetch_table(
                table,
                options.incremental_key.as_deref(),
                options.last_value.as_deref(),
            ).await?;
            let stream = futures::stream::iter(batches.into_iter().map(Ok));
            Ok(Box::pin(stream) as RecordBatchStream)
        })
    }

    async fn validate_credentials(&self) -> ConnectorResult<()> {
        // Try to fetch metadata to validate credentials
        self.fetch_metadata().await?;
        Ok(())
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = GoogleSheetsConfig::new("test_spreadsheet_id")
            .with_cache_ttl(Duration::from_secs(120))
            .with_header(true)
            .with_sheets(vec!["Sheet1".to_string()]);

        assert_eq!(config.spreadsheet_id, "test_spreadsheet_id");
        assert_eq!(config.cache_ttl_secs, 120);
        assert!(config.first_row_is_header);
        assert_eq!(config.sheets, vec!["Sheet1"]);
    }

    #[test]
    fn test_cache_ttl() {
        let config = GoogleSheetsConfig::new("test");
        assert_eq!(config.cache_ttl(), Duration::from_secs(60));

        let config = config.with_cache_ttl(Duration::from_secs(300));
        assert_eq!(config.cache_ttl(), Duration::from_secs(300));
    }

    #[test]
    fn test_cache_entry_validity() {
        let entry = CacheEntry {
            data: Arc::new(vec![]),
            fetched_at: Instant::now(),
            ttl: Duration::from_secs(60),
        };
        assert!(entry.is_valid());

        let old_entry = CacheEntry {
            data: Arc::new(vec![]),
            fetched_at: Instant::now() - Duration::from_secs(120),
            ttl: Duration::from_secs(60),
        };
        assert!(!old_entry.is_valid());
    }

    #[test]
    fn test_infer_column_types() {
        let oauth = OAuthConfig::new("client_id", "client_secret", "https://oauth2.googleapis.com/token");
        let config = GoogleSheetsConfig::new("test");
        let connector = GoogleSheetsConnector::new(config, oauth);

        // Test with mixed data
        let rows = vec![
            vec![
                serde_json::json!(1),
                serde_json::json!(1.5),
                serde_json::json!(true),
                serde_json::json!("text"),
            ],
            vec![
                serde_json::json!(2),
                serde_json::json!(2.5),
                serde_json::json!(false),
                serde_json::json!("more"),
            ],
        ];

        let types = connector.infer_column_types(&rows, 4);
        assert_eq!(types[0], DataType::Int64);
        assert_eq!(types[1], DataType::Float64);
        assert_eq!(types[2], DataType::Boolean);
        assert_eq!(types[3], DataType::Utf8);
    }

    #[test]
    fn test_infer_mixed_numbers_as_float() {
        let oauth = OAuthConfig::new("client_id", "client_secret", "https://oauth2.googleapis.com/token");
        let config = GoogleSheetsConfig::new("test");
        let connector = GoogleSheetsConnector::new(config, oauth);

        // Mixed int and float should become float
        let rows = vec![
            vec![serde_json::json!(1)],
            vec![serde_json::json!(2.5)],
            vec![serde_json::json!(3)],
        ];

        let types = connector.infer_column_types(&rows, 1);
        assert_eq!(types[0], DataType::Float64);
    }

    #[test]
    fn test_build_table_schema() {
        let oauth = OAuthConfig::new("client_id", "client_secret", "https://oauth2.googleapis.com/token");
        let config = GoogleSheetsConfig::new("test");
        let connector = GoogleSheetsConnector::new(config, oauth);

        let headers = vec!["id".to_string(), "name".to_string(), "active".to_string()];
        let types = vec![DataType::Int64, DataType::Utf8, DataType::Boolean];

        let schema = connector.build_table_schema(&headers, &types);
        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.columns[0].name, "id");
        assert_eq!(schema.columns[0].data_type, ColumnType::Int64);
        assert_eq!(schema.columns[1].name, "name");
        assert_eq!(schema.columns[1].data_type, ColumnType::String);
        assert_eq!(schema.columns[2].name, "active");
        assert_eq!(schema.columns[2].data_type, ColumnType::Boolean);
    }

    #[test]
    fn test_values_to_record_batch() {
        let oauth = OAuthConfig::new("client_id", "client_secret", "https://oauth2.googleapis.com/token");
        let config = GoogleSheetsConfig::new("test");
        let connector = GoogleSheetsConnector::new(config, oauth);

        let values = vec![
            vec![
                serde_json::json!(1),
                serde_json::json!("Alice"),
                serde_json::json!(true),
            ],
            vec![
                serde_json::json!(2),
                serde_json::json!("Bob"),
                serde_json::json!(false),
            ],
        ];
        let headers = vec!["id".to_string(), "name".to_string(), "active".to_string()];
        let types = vec![DataType::Int64, DataType::Utf8, DataType::Boolean];

        let batch = connector.values_to_record_batch(values, &headers, &types).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.schema().field(0).name(), "id");
        assert_eq!(batch.schema().field(1).name(), "name");
        assert_eq!(batch.schema().field(2).name(), "active");
    }

    #[test]
    fn test_infer_column_types_with_nulls() {
        let oauth = OAuthConfig::new("client_id", "client_secret", "https://oauth2.googleapis.com/token");
        let config = GoogleSheetsConfig::new("test");
        let connector = GoogleSheetsConnector::new(config, oauth);

        // Test with null values - should still infer correct types
        let rows = vec![
            vec![serde_json::json!(null), serde_json::json!(1.5)],
            vec![serde_json::json!(42), serde_json::json!(null)],
            vec![serde_json::json!(null), serde_json::json!(2.5)],
        ];

        let types = connector.infer_column_types(&rows, 2);
        assert_eq!(types[0], DataType::Int64); // Only one non-null int
        assert_eq!(types[1], DataType::Float64); // Floats detected
    }

    #[test]
    fn test_infer_column_types_empty() {
        let oauth = OAuthConfig::new("client_id", "client_secret", "https://oauth2.googleapis.com/token");
        let config = GoogleSheetsConfig::new("test");
        let connector = GoogleSheetsConnector::new(config, oauth);

        // Empty column should default to string
        let rows = vec![
            vec![serde_json::json!(null)],
            vec![serde_json::json!("")],
        ];

        let types = connector.infer_column_types(&rows, 1);
        assert_eq!(types[0], DataType::Utf8); // Default to string for empty
    }

    #[test]
    fn test_infer_column_types_string_dominates() {
        let oauth = OAuthConfig::new("client_id", "client_secret", "https://oauth2.googleapis.com/token");
        let config = GoogleSheetsConfig::new("test");
        let connector = GoogleSheetsConnector::new(config, oauth);

        // If any row has a string, the column should be string
        let rows = vec![
            vec![serde_json::json!(1)],
            vec![serde_json::json!("not a number")],
            vec![serde_json::json!(3)],
        ];

        let types = connector.infer_column_types(&rows, 1);
        assert_eq!(types[0], DataType::Utf8);
    }

    #[test]
    fn test_sheets_cache_insert_and_get() {
        let mut cache = SheetsCache::new();
        
        // Create empty record batch for testing
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::new_empty(schema);
        let batches = Arc::new(vec![batch]);

        cache.insert("Sheet1".to_string(), batches, Duration::from_secs(60));

        // Should find cached data
        assert!(cache.get("Sheet1").is_some());
        
        // Should not find non-existent sheet
        assert!(cache.get("Sheet2").is_none());
    }

    #[test]
    fn test_values_to_record_batch_with_missing_values() {
        let oauth = OAuthConfig::new("client_id", "client_secret", "https://oauth2.googleapis.com/token");
        let config = GoogleSheetsConfig::new("test");
        let connector = GoogleSheetsConnector::new(config, oauth);

        // Test with jagged rows (missing values)
        let values = vec![
            vec![serde_json::json!(1), serde_json::json!("Alice")],
            vec![serde_json::json!(2)], // Missing second column
        ];
        let headers = vec!["id".to_string(), "name".to_string()];
        let types = vec![DataType::Int64, DataType::Utf8];

        let batch = connector.values_to_record_batch(values, &headers, &types).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_config_default_values() {
        let config = GoogleSheetsConfig::new("spreadsheet_123");
        
        assert_eq!(config.spreadsheet_id, "spreadsheet_123");
        assert_eq!(config.cache_ttl_secs, 60); // Default 60 seconds
        assert!(config.first_row_is_header); // Default true
        assert!(config.sheets.is_empty()); // Default empty (all sheets)
    }

    #[test]
    fn test_sheets_cache_clear_expired() {
        let mut cache = SheetsCache::new();
        
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::new_empty(schema);
        let batches = Arc::new(vec![batch]);

        // Insert with very short TTL
        let entry = CacheEntry {
            data: batches,
            fetched_at: Instant::now() - Duration::from_secs(120), // Already expired
            ttl: Duration::from_secs(60),
        };
        cache.entries.insert("expired_sheet".to_string(), entry);

        // Should not return expired data
        assert!(cache.get("expired_sheet").is_none());
    }

    #[test]
    fn test_connector_debug() {
        let oauth = OAuthConfig::new("client_id", "client_secret", "https://oauth2.googleapis.com/token");
        let config = GoogleSheetsConfig::new("test_spreadsheet_id")
            .with_cache_ttl(Duration::from_secs(120));
        let connector = GoogleSheetsConnector::new(config, oauth);

        let debug_str = format!("{:?}", connector);
        assert!(debug_str.contains("test_spreadsheet_id"));
        assert!(debug_str.contains("120"));
    }

    // =========================================================================
    // Mock API Response Tests
    // =========================================================================

    /// Helper to create a mock spreadsheet metadata response
    fn mock_spreadsheet_metadata() -> SpreadsheetMetadata {
        SpreadsheetMetadata {
            spreadsheet_id: "test_id".to_string(),
            properties: SpreadsheetProperties {
                title: "Test Spreadsheet".to_string(),
            },
            sheets: vec![
                SheetMetadata {
                    properties: SheetProperties {
                        sheet_id: 0,
                        title: "Sheet1".to_string(),
                        grid_properties: Some(GridProperties {
                            row_count: Some(100),
                            column_count: Some(5),
                        }),
                    },
                },
                SheetMetadata {
                    properties: SheetProperties {
                        sheet_id: 1,
                        title: "Sheet2".to_string(),
                        grid_properties: Some(GridProperties {
                            row_count: Some(50),
                            column_count: Some(3),
                        }),
                    },
                },
            ],
        }
    }

    #[test]
    fn test_mock_spreadsheet_metadata_parsing() {
        let metadata = mock_spreadsheet_metadata();
        
        assert_eq!(metadata.spreadsheet_id, "test_id");
        assert_eq!(metadata.sheets.len(), 2);
        assert_eq!(metadata.sheets[0].properties.title, "Sheet1");
        assert_eq!(metadata.sheets[1].properties.title, "Sheet2");
        assert_eq!(metadata.sheets[0].properties.grid_properties.as_ref().unwrap().row_count, Some(100));
    }

    #[test]
    fn test_value_range_deserialization() {
        // Test deserializing a mock value range response
        let json = r#"{
            "range": "'Sheet1'!A1:C10",
            "majorDimension": "ROWS",
            "values": [
                ["Name", "Age", "Active"],
                ["Alice", 30, true],
                ["Bob", 25, false]
            ]
        }"#;

        let value_range: ValueRange = serde_json::from_str(json).unwrap();
        assert_eq!(value_range.range, "'Sheet1'!A1:C10");
        assert!(value_range.values.is_some());
        let values = value_range.values.unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0][0], serde_json::json!("Name"));
    }

    #[test]
    fn test_empty_value_range() {
        // Test deserializing an empty sheet response
        let json = r#"{
            "range": "'EmptySheet'!A1:Z1000"
        }"#;

        let value_range: ValueRange = serde_json::from_str(json).unwrap();
        assert_eq!(value_range.range, "'EmptySheet'!A1:Z1000");
        assert!(value_range.values.is_none());
    }
}
