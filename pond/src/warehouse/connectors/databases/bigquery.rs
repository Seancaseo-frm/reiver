//! BigQuery Connector
//!
//! Connects to Google BigQuery and provides cold tier data access.
//!
//! # Features
//!
//! - Automatic schema discovery from `INFORMATION_SCHEMA`
//! - Incremental sync support with configurable key columns
//! - Full SQL predicate pushdown (native BigQuery support)
//! - Cost awareness with bytes scanned logging
//!
//! # Usage
//!
//! ```ignore
//! let config = BigQueryConfig::new("my-project", "my_dataset")
//!     .with_credentials_path("/path/to/service-account.json");
//! let connector = BigQueryConnector::new(config);
//!
//! let tables = connector.list_tables().await?;
//! let data = connector.fetch_table("users", None, None).await?;
//! ```

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use google_cloud_auth::credentials::CredentialsFile;
use google_cloud_auth::project::{create_token_source_from_credentials, create_token_source_from_project, project, Config};
use tokio::sync::OnceCell;

use crate::warehouse::connectors::{Connector, ConnectorError, ConnectorResult, FetchOptions, RecordBatchStream, TableInfo};
use crate::warehouse::types::{ColumnSchema, ColumnType, SourceType, TableSchema};

/// Maximum rows to fetch per batch to prevent memory issues.
const BATCH_SIZE: i64 = 10_000;

/// BigQuery REST API base URL.
const BIGQUERY_API_BASE: &str = "https://bigquery.googleapis.com/bigquery/v2";

/// BigQuery connector configuration.
#[derive(Debug, Clone)]
pub struct BigQueryConfig {
    /// GCP project ID
    pub project_id: String,
    /// BigQuery dataset name
    pub dataset: String,
    /// Tables to sync (empty = all tables)
    pub tables: Vec<String>,
    /// Path to service account JSON credentials file
    pub credentials_path: Option<String>,
    /// Service account JSON credentials as a string
    pub credentials_json: Option<String>,
    /// BigQuery location (e.g., "US", "EU")
    pub location: String,
    /// Maximum bytes billed per query (cost control)
    pub maximum_bytes_billed: Option<i64>,
}

impl BigQueryConfig {
    /// Create a new BigQuery configuration.
    ///
    /// # Arguments
    ///
    /// * `project_id` - GCP project ID
    /// * `dataset` - BigQuery dataset name
    pub fn new(project_id: impl Into<String>, dataset: impl Into<String>) -> Self {
        Self {
            project_id: project_id.into(),
            dataset: dataset.into(),
            tables: Vec::new(),
            credentials_path: None,
            credentials_json: None,
            location: "US".to_string(),
            maximum_bytes_billed: None,
        }
    }

    /// Set the path to the service account JSON credentials file.
    pub fn with_credentials_path(mut self, path: impl Into<String>) -> Self {
        self.credentials_path = Some(path.into());
        self
    }

    /// Set the service account JSON credentials directly.
    pub fn with_credentials_json(mut self, json: impl Into<String>) -> Self {
        self.credentials_json = Some(json.into());
        self
    }

    /// Set the BigQuery location.
    pub fn with_location(mut self, location: impl Into<String>) -> Self {
        self.location = location.into();
        self
    }

    /// Set tables to sync (empty = all tables).
    pub fn with_tables(mut self, tables: Vec<String>) -> Self {
        self.tables = tables;
        self
    }

    /// Set maximum bytes billed per query for cost control.
    pub fn with_maximum_bytes_billed(mut self, bytes: i64) -> Self {
        self.maximum_bytes_billed = Some(bytes);
        self
    }
}

/// A token source for BigQuery authentication.
type TokenSource = Box<dyn google_cloud_auth::token_source::TokenSource>;

/// BigQuery data source connector.
///
/// Provides cold tier access to BigQuery with full predicate pushdown.
/// No local indexing - relies on BigQuery's native query performance.
pub struct BigQueryConnector {
    config: BigQueryConfig,
    /// HTTP client
    http_client: reqwest::Client,
    /// Token source - initialized lazily
    token_source: OnceCell<TokenSource>,
}

/// Maximum retry attempts for transient errors.
const MAX_RETRY_ATTEMPTS: u32 = 3;

/// Initial retry delay in milliseconds.
const INITIAL_RETRY_DELAY_MS: u64 = 200;

/// Parameters for building a BigQuery data fetch query.
struct BqFetchQueryParams<'a> {
    project: &'a str,
    dataset: &'a str,
    table: &'a str,
    columns: &'a [String],
    incremental_key: Option<&'a str>,
    last_value: Option<&'a str>,
    pagination_key: Option<&'a str>,
    last_seen_key: Option<&'a str>,
    limit: i64,
}

impl BigQueryConnector {
    /// Create a new BigQuery connector.
    pub fn new(config: BigQueryConfig) -> Self {
        // Configure HTTP client with timeouts
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(300)) // 5 min for long queries
            .connect_timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(5)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        Self {
            config,
            http_client,
            token_source: OnceCell::new(),
        }
    }

    /// Check if an error is retryable (transient network issues, rate limiting).
    fn is_retryable_error(error: &ConnectorError) -> bool {
        match error {
            ConnectorError::Network(_) => true,
            ConnectorError::Internal(msg) => {
                // Retry on rate limiting (429) or server errors (5xx)
                msg.contains("status 429")
                    || msg.contains("status 500")
                    || msg.contains("status 502")
                    || msg.contains("status 503")
                    || msg.contains("status 504")
            }
            _ => false,
        }
    }

    /// Execute an operation with retry logic and exponential backoff.
    async fn with_retry<F, T, Fut>(&self, operation: F) -> ConnectorResult<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = ConnectorResult<T>>,
    {
        let mut attempts = 0;
        loop {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(e) if attempts < MAX_RETRY_ATTEMPTS && Self::is_retryable_error(&e) => {
                    attempts += 1;
                    let delay = INITIAL_RETRY_DELAY_MS * 2u64.pow(attempts - 1);
                    tracing::warn!(
                        attempt = attempts,
                        max_attempts = MAX_RETRY_ATTEMPTS,
                        delay_ms = delay,
                        error = %e,
                        "Retrying BigQuery request after transient error"
                    );
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Get or create the token source.
    async fn get_token_source(&self) -> ConnectorResult<&TokenSource> {
        self.token_source
            .get_or_try_init(|| async {
                let config = Config::default()
                    .with_scopes(&["https://www.googleapis.com/auth/bigquery"]);

                // Priority: credentials_json > credentials_path > ADC
                if let Some(ref json) = self.config.credentials_json {
                    // Parse credentials from JSON string
                    let credentials = CredentialsFile::new_from_str(json)
                        .await
                        .map_err(|e| {
                            ConnectorError::Config(format!(
                                "Failed to parse credentials JSON: {}",
                                e
                            ))
                        })?;
                    
                    create_token_source_from_credentials(&credentials, &config)
                        .await
                        .map_err(|e| {
                            ConnectorError::Config(format!(
                                "Failed to create token source from credentials JSON: {}",
                                e
                            ))
                        })
                } else if let Some(ref path) = self.config.credentials_path {
                    // Load credentials from file path
                    let credentials = CredentialsFile::new_from_file(path.clone())
                        .await
                        .map_err(|e| {
                            ConnectorError::Config(format!(
                                "Failed to load credentials from '{}': {}",
                                path, e
                            ))
                        })?;
                    
                    create_token_source_from_credentials(&credentials, &config)
                        .await
                        .map_err(|e| {
                            ConnectorError::Config(format!(
                                "Failed to create token source from credentials file: {}",
                                e
                            ))
                        })
                } else {
                    // Fall back to Application Default Credentials (ADC)
                    let proj = project()
                        .await
                        .map_err(|e| {
                            ConnectorError::Config(format!(
                                "Failed to discover project credentials: {}. \
                                 Provide credentials_path or credentials_json, or set up ADC.",
                                e
                            ))
                        })?;
                    create_token_source_from_project(&proj, config)
                        .await
                        .map_err(|e| {
                            ConnectorError::Config(format!(
                                "Failed to create token source from ADC: {}. \
                                 Provide credentials_path or credentials_json, or set up ADC.",
                                e
                            ))
                        })
                }
            })
            .await
    }

    /// Get an access token for authentication.
    async fn get_access_token(&self) -> ConnectorResult<String> {
        let token_source = self.get_token_source().await?;
        let token = token_source
            .token()
            .await
            .map_err(|e| ConnectorError::Config(format!("Failed to get access token: {}", e)))?;
        Ok(token.access_token)
    }

    /// Map BigQuery type to warehouse column type.
    fn bigquery_type_to_column_type(bq_type: &str) -> ColumnType {
        let upper = bq_type.to_uppercase();

        match upper.as_str() {
            "INT64" | "INTEGER" | "INT" | "SMALLINT" | "TINYINT" | "BYTEINT" => ColumnType::Int64,
            "FLOAT64" | "FLOAT" => ColumnType::Float64,
            "NUMERIC" | "BIGNUMERIC" | "DECIMAL" => ColumnType::Decimal,
            "BOOL" | "BOOLEAN" => ColumnType::Boolean,
            "DATE" => ColumnType::Date,
            "DATETIME" | "TIMESTAMP" => ColumnType::Timestamp,
            "JSON" => ColumnType::Json,
            // Default to string for STRING, BYTES, TIME, GEOGRAPHY, ARRAY, STRUCT, etc.
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

    /// Escape a string value for use in BigQuery SQL.
    fn escape_bigquery_string(s: &str) -> String {
        let mut result = String::with_capacity(s.len() + s.len() / 8);
        for ch in s.chars() {
            match ch {
                '\'' => result.push_str("\\'"),
                '\\' => result.push_str("\\\\"),
                '\n' => result.push_str("\\n"),
                '\r' => result.push_str("\\r"),
                '\t' => result.push_str("\\t"),
                '\0' => {} // Remove null bytes
                _ => result.push(ch),
            }
        }
        result
    }

    /// Validate an identifier (dataset, table, column) for use in SQL.
    fn is_valid_identifier(name: &str) -> bool {
        if name.is_empty() || name.len() > 1024 {
            return false;
        }

        // Reject backticks which could break out of identifier quoting
        if name.contains('`') {
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

    /// Validate a GCP project ID.
    /// Project IDs: 6-30 chars, lowercase letters, digits, hyphens; must start with letter.
    fn is_valid_project_id(project_id: &str) -> bool {
        if project_id.len() < 6 || project_id.len() > 30 {
            return false;
        }

        // Reject backticks which could break out of identifier quoting
        if project_id.contains('`') {
            return false;
        }

        let mut chars = project_id.chars();

        // First character must be lowercase letter
        match chars.next() {
            Some(c) if c.is_ascii_lowercase() => {}
            _ => return false,
        }

        // Remaining characters: lowercase letters, digits, or hyphens
        // Cannot end with hyphen (checked separately)
        if project_id.ends_with('-') {
            return false;
        }

        chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    }

    /// Build a SELECT query for fetching data.
    /// Uses keyset pagination when a pagination key is available, otherwise uses OFFSET.
    fn build_fetch_query(params: &BqFetchQueryParams) -> ConnectorResult<String> {
        // Validate all identifiers
        if !Self::is_valid_project_id(params.project) {
            return Err(ConnectorError::Config(format!(
                "Invalid project ID: '{}'",
                params.project
            )));
        }
        if !Self::is_valid_identifier(params.dataset) {
            return Err(ConnectorError::Config(format!(
                "Invalid dataset name: '{}'",
                params.dataset
            )));
        }
        if !Self::is_valid_identifier(params.table) {
            return Err(ConnectorError::Config(format!(
                "Invalid table name: '{}'",
                params.table
            )));
        }

        // Build column list
        let columns_str = if params.columns.is_empty() {
            "*".to_string()
        } else {
            for col in params.columns {
                if !Self::is_valid_identifier(col) {
                    return Err(ConnectorError::Config(format!(
                        "Invalid column name: '{}'",
                        col
                    )));
                }
            }
            params.columns
                .iter()
                .map(|c| format!("`{}`", c))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let mut query = format!(
            "SELECT {} FROM `{}.{}.{}`",
            columns_str, params.project, params.dataset, params.table
        );

        // Build WHERE clause conditions
        let mut conditions = Vec::new();

        // Add incremental filter if provided (for incremental sync)
        if let (Some(key), Some(value)) = (params.incremental_key, params.last_value) {
            if !Self::is_valid_identifier(key) {
                return Err(ConnectorError::Config(format!(
                    "Invalid incremental key column: '{}'",
                    key
                )));
            }
            let escaped_value = Self::escape_bigquery_string(value);
            conditions.push(format!("`{}` > '{}'", key, escaped_value));
        }

        // Add keyset pagination condition if provided
        if let (Some(key), Some(last_key)) = (params.pagination_key, params.last_seen_key) {
            if !Self::is_valid_identifier(key) {
                return Err(ConnectorError::Config(format!(
                    "Invalid pagination key column: '{}'",
                    key
                )));
            }
            let escaped_key = Self::escape_bigquery_string(last_key);
            conditions.push(format!("`{}` > '{}'", key, escaped_key));
        }

        if !conditions.is_empty() {
            query.push_str(" WHERE ");
            query.push_str(&conditions.join(" AND "));
        }

        // Add ordering for consistent keyset pagination
        // Prefer pagination_key, fall back to incremental_key
        let order_key = params.pagination_key.or(params.incremental_key);
        if let Some(key) = order_key {
            if Self::is_valid_identifier(key) {
                query.push_str(&format!(" ORDER BY `{}` ASC", key));
            }
        }

        // Add limit (no OFFSET with keyset pagination)
        query.push_str(&format!(" LIMIT {}", params.limit));

        Ok(query)
    }

    /// Internal query execution without retry (called by execute_query with retry wrapper).
    async fn execute_query_internal(&self, query: &str) -> ConnectorResult<QueryResponse> {
        let token = self.get_access_token().await?;
        let request_id = uuid::Uuid::new_v4().to_string();
        
        let url = format!(
            "{}/projects/{}/queries",
            BIGQUERY_API_BASE, self.config.project_id
        );

        let mut request_body = serde_json::json!({
            "query": query,
            "useLegacySql": false,
            "location": self.config.location
        });

        if let Some(max_bytes) = self.config.maximum_bytes_billed {
            request_body["maximumBytesBilled"] = serde_json::Value::String(max_bytes.to_string());
        }

        // Log query with preview (truncated for safety)
        let query_preview: String = query.chars().take(200).collect();
        tracing::debug!(
            request_id = %request_id,
            project = %self.config.project_id,
            dataset = %self.config.dataset,
            query_preview = %query_preview,
            "Executing BigQuery query"
        );

        let response = self
            .http_client
            .post(&url)
            .bearer_auth(&token)
            .header("X-Request-ID", &request_id)
            .json(&request_body)
            .send()
            .await
            .map_err(|e| ConnectorError::Network(format!("BigQuery request failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            tracing::warn!(
                request_id = %request_id,
                status = %status,
                "BigQuery query failed"
            );
            return Err(ConnectorError::Internal(format!(
                "BigQuery query failed with status {}: {}",
                status, error_text
            )));
        }

        response
            .json()
            .await
            .map_err(|e| ConnectorError::Internal(format!("Failed to parse BigQuery response: {}", e)))
    }

    /// Execute a query using the BigQuery REST API.
    /// Polls for completion if the query is long-running.
    async fn execute_query(&self, query: &str) -> ConnectorResult<QueryResponse> {
        // Execute initial query with retry
        let query_owned = query.to_string();
        let mut query_response = self
            .with_retry(|| self.execute_query_internal(&query_owned))
            .await?;

        // Poll for completion if the query is still running
        const MAX_POLL_ATTEMPTS: u32 = 120; // 2 minutes at 1s intervals
        let mut poll_count = 0;

        while query_response.job_complete == Some(false) && poll_count < MAX_POLL_ATTEMPTS {
            if let Some(job_ref) = query_response.job_reference.clone() {
                // Exponential backoff: 500ms, 1s, 1s, 1s... (cap at 1s)
                let delay = if poll_count == 0 { 500 } else { 1000 };
                tokio::time::sleep(Duration::from_millis(delay)).await;
                
                // Poll with retry
                query_response = self
                    .with_retry(|| self.get_query_results_internal(&job_ref.project_id, &job_ref.job_id))
                    .await?;
                poll_count += 1;
            } else {
                // No job reference, can't poll - return what we have
                break;
            }
        }

        if query_response.job_complete == Some(false) {
            return Err(ConnectorError::Internal(
                "BigQuery query timed out waiting for completion".to_string()
            ));
        }

        Ok(query_response)
    }

    /// Internal method to get query results (called with retry wrapper).
    async fn get_query_results_internal(&self, project_id: &str, job_id: &str) -> ConnectorResult<QueryResponse> {
        let token = self.get_access_token().await?;
        
        let url = format!(
            "{}/projects/{}/queries/{}",
            BIGQUERY_API_BASE, project_id, job_id
        );

        tracing::debug!(
            job_id = %job_id,
            project_id = %project_id,
            "Polling BigQuery job results"
        );

        let response = self
            .http_client
            .get(&url)
            .bearer_auth(&token)
            .header("X-Request-ID", uuid::Uuid::new_v4().to_string())
            .send()
            .await
            .map_err(|e| ConnectorError::Network(format!("BigQuery poll request failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            tracing::warn!(
                job_id = %job_id,
                status = %status,
                "BigQuery poll failed"
            );
            return Err(ConnectorError::Internal(format!(
                "BigQuery poll failed with status {}: {}",
                status, error_text
            )));
        }

        response
            .json()
            .await
            .map_err(|e| ConnectorError::Internal(format!("Failed to parse BigQuery poll response: {}", e)))
    }

    /// Get schema for a table.
    async fn get_schema_internal(&self, table: &str) -> ConnectorResult<TableSchema> {
        // Validate identifiers to prevent SQL injection
        if !Self::is_valid_project_id(&self.config.project_id) {
            return Err(ConnectorError::Config(format!(
                "Invalid project ID: '{}'",
                self.config.project_id
            )));
        }
        if !Self::is_valid_identifier(&self.config.dataset) {
            return Err(ConnectorError::Config(format!(
                "Invalid dataset name: '{}'",
                self.config.dataset
            )));
        }

        // Escape table name for use in string comparison (not backtick-quoted)
        let escaped_table = Self::escape_bigquery_string(table);

        let query = format!(
            r#"
            SELECT 
                column_name,
                data_type,
                is_nullable
            FROM `{}.{}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{}'
            ORDER BY ordinal_position
            "#,
            self.config.project_id, self.config.dataset, escaped_table
        );

        let response = self.execute_query(&query).await?;
        let rows = response.rows.unwrap_or_default();
        
        if rows.is_empty() {
            return Err(ConnectorError::Internal(format!(
                "Table {}.{} not found or has no columns",
                self.config.dataset, table
            )));
        }

        let columns: Vec<ColumnSchema> = rows
            .into_iter()
            .filter_map(|row| {
                let cells = row.f?;
                if cells.len() < 3 {
                    return None;
                }

                let name = cells.first()?.v.as_ref()?.as_str()?.to_string();
                let data_type = cells.get(1)?.v.as_ref()?.as_str()?;
                let is_nullable = cells.get(2)?.v.as_ref()?.as_str()?;

                let col_type = Self::bigquery_type_to_column_type(data_type);
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

        if columns.is_empty() {
            return Err(ConnectorError::Internal(format!(
                "Failed to parse schema for table {}",
                table
            )));
        }

        Ok(TableSchema { columns })
    }

    /// Convert BigQuery rows to an Arrow RecordBatch.
    fn rows_to_record_batch(
        &self,
        rows: &[QueryRow],
        schema: &TableSchema,
        arrow_schema: Arc<Schema>,
    ) -> ConnectorResult<RecordBatch> {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.columns.len());

        for (col_idx, col) in schema.columns.iter().enumerate() {
            let array: ArrayRef = match col.data_type {
                ColumnType::Int32 | ColumnType::Int64 => {
                    let mut values = Vec::with_capacity(rows.len());
                    values.extend(rows.iter().map(|row| {
                        row.f
                            .as_ref()
                            .and_then(|f| f.get(col_idx))
                            .and_then(|cell| cell.v.as_ref())
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<i64>().ok())
                    }));
                    Arc::new(Int64Array::from(values))
                }
                ColumnType::Float64 => {
                    let mut values = Vec::with_capacity(rows.len());
                    values.extend(rows.iter().map(|row| {
                        row.f
                            .as_ref()
                            .and_then(|f| f.get(col_idx))
                            .and_then(|cell| cell.v.as_ref())
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<f64>().ok())
                    }));
                    Arc::new(Float64Array::from(values))
                }
                ColumnType::Boolean => {
                    let mut values = Vec::with_capacity(rows.len());
                    values.extend(rows.iter().map(|row| {
                        row.f
                            .as_ref()
                            .and_then(|f| f.get(col_idx))
                            .and_then(|cell| cell.v.as_ref())
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_lowercase() == "true")
                    }));
                    Arc::new(BooleanArray::from(values))
                }
                _ => {
                    // Default: convert to string
                    let mut values = Vec::with_capacity(rows.len());
                    values.extend(rows.iter().map(|row| {
                        row.f
                            .as_ref()
                            .and_then(|f| f.get(col_idx))
                            .and_then(|cell| cell.v.as_ref())
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                    }));
                    Arc::new(StringArray::from(values))
                }
            };
            arrays.push(array);
        }

        RecordBatch::try_new(arrow_schema, arrays).map_err(|e| {
            ConnectorError::Internal(format!("Failed to create RecordBatch: {}", e))
        })
    }
}

/// BigQuery query response structure.
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryResponse {
    /// Schema of the result (unused, kept for JSON deserialization)
    #[allow(dead_code)]
    schema: Option<serde_json::Value>,
    /// Result rows
    rows: Option<Vec<QueryRow>>,
    /// Total rows in result
    #[allow(dead_code)]
    total_rows: Option<String>,
    /// Total bytes processed
    total_bytes_processed: Option<String>,
    /// Whether the query is complete
    job_complete: Option<bool>,
    /// Job reference for polling incomplete jobs
    job_reference: Option<JobReference>,
    /// Page token for paginated results
    #[allow(dead_code)]
    page_token: Option<String>,
}

/// BigQuery job reference for tracking long-running queries.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct JobReference {
    /// Project ID
    project_id: String,
    /// Job ID
    job_id: String,
    /// Location
    #[allow(dead_code)]
    location: Option<String>,
}

/// BigQuery row structure.
#[derive(Debug, serde::Deserialize)]
struct QueryRow {
    f: Option<Vec<QueryCell>>,
}

/// BigQuery cell structure.
#[derive(Debug, serde::Deserialize)]
struct QueryCell {
    v: Option<serde_json::Value>,
}

#[async_trait]
impl Connector for BigQueryConnector {
    fn source_type(&self) -> SourceType {
        SourceType::BigQuery
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        // Validate identifiers to prevent SQL injection
        if !Self::is_valid_project_id(&self.config.project_id) {
            return Err(ConnectorError::Config(format!(
                "Invalid project ID: '{}'",
                self.config.project_id
            )));
        }
        if !Self::is_valid_identifier(&self.config.dataset) {
            return Err(ConnectorError::Config(format!(
                "Invalid dataset name: '{}'",
                self.config.dataset
            )));
        }

        // Single batched query to get all tables with their columns (avoids N+1)
        let query = format!(
            r#"
            SELECT 
                t.table_name,
                CAST(t.row_count AS INT64) as row_count,
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.ordinal_position
            FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES` t
            LEFT JOIN `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS` c
                ON t.table_name = c.table_name
            WHERE t.table_type = 'BASE TABLE'
            ORDER BY t.table_name, c.ordinal_position
            "#,
            project = self.config.project_id,
            dataset = self.config.dataset
        );

        let response = self.execute_query(&query).await?;
        let rows = response.rows.unwrap_or_default();

        // Group rows by table name to build TableInfo objects
        use std::collections::HashMap;

        #[derive(Default)]
        struct TableData {
            row_count: Option<u64>,
            columns: Vec<ColumnSchema>,
        }

        let mut tables_map: HashMap<String, TableData> = HashMap::new();

        for row in rows {
            let cells = match row.f {
                Some(c) if c.len() >= 6 => c,
                _ => continue,
            };

            let table_name = match cells[0].v.as_ref().and_then(|v| v.as_str()) {
                Some(name) => name.to_string(),
                None => continue,
            };

            // Skip if tables filter is set and this table isn't in it
            if !self.config.tables.is_empty() && !self.config.tables.contains(&table_name) {
                continue;
            }

            let table_data = tables_map.entry(table_name).or_default();

            // Set row count (same for all rows of this table)
            if table_data.row_count.is_none() {
                table_data.row_count = cells[1]
                    .v
                    .as_ref()
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<u64>().ok());
            }

            // Parse column info
            let column_name = match cells[2].v.as_ref().and_then(|v| v.as_str()) {
                Some(name) => name.to_string(),
                None => continue, // Table exists but no columns yet (shouldn't happen)
            };

            let data_type = cells[3]
                .v
                .as_ref()
                .and_then(|v| v.as_str())
                .unwrap_or("STRING");

            let is_nullable = cells[4]
                .v
                .as_ref()
                .and_then(|v| v.as_str())
                .map(|s| s.to_uppercase() == "YES")
                .unwrap_or(true);

            let col_type = Self::bigquery_type_to_column_type(data_type);
            let timezone = if matches!(col_type, ColumnType::Timestamp) {
                Some("UTC".to_string())
            } else {
                None
            };

            table_data.columns.push(ColumnSchema {
                name: column_name,
                data_type: col_type,
                nullable: is_nullable,
                description: None,
                timezone,
            });
        }

        // Convert to TableInfo list
        let mut table_infos: Vec<TableInfo> = tables_map
            .into_iter()
            .filter(|(_, data)| !data.columns.is_empty())
            .map(|(name, data)| {
                let table_schema = TableSchema {
                    columns: data.columns,
                };

                // Determine incremental key - look for common timestamp columns
                let incremental_key = table_schema
                    .columns
                    .iter()
                    .find(|c| {
                        matches!(
                            c.name.as_str(),
                            "updated_at" | "modified_at" | "last_modified" | "_PARTITIONTIME"
                        )
                    })
                    .map(|c| c.name.clone());

                TableInfo {
                    name,
                    schema: table_schema,
                    supports_incremental: incremental_key.is_some(),
                    incremental_key,
                    estimated_rows: data.row_count,
                    primary_key_columns: Vec::new(),
                }
            })
            .collect();

        // Sort by table name for consistent ordering
        table_infos.sort_by(|a, b| a.name.cmp(&b.name));

        tracing::debug!(
            project = %self.config.project_id,
            dataset = %self.config.dataset,
            table_count = table_infos.len(),
            "Listed BigQuery tables"
        );

        Ok(table_infos)
    }

    async fn get_schema(&self, table: &str) -> ConnectorResult<TableSchema> {
        self.get_schema_internal(table).await
    }

    async fn fetch_table(
        &self,
        table: &str,
        incremental_key: Option<&str>,
        last_value: Option<&str>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        // Get schema to know column types
        let schema = self.get_schema(table).await?;
        let arrow_schema = Arc::new(Self::to_arrow_schema(&schema));
        let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();

        // Find pagination key: prefer 'id', '_row_id', or fall back to incremental_key
        let pagination_key = schema
            .columns
            .iter()
            .find(|c| matches!(c.name.as_str(), "id" | "_row_id" | "row_id"))
            .map(|c| c.name.as_str())
            .or(incremental_key);

        // Find pagination key column index for extracting last seen value
        let pagination_key_idx = pagination_key.and_then(|key| {
            column_names.iter().position(|c| c == key)
        });

        let mut all_batches = Vec::new();
        let mut last_seen_key: Option<String> = None;
        let mut total_rows = 0u64;
        let mut total_bytes_scanned = 0u64;

        loop {
            let query = Self::build_fetch_query(&BqFetchQueryParams {
                project: &self.config.project_id,
                dataset: &self.config.dataset,
                table,
                columns: &column_names,
                incremental_key,
                last_value,
                pagination_key,
                last_seen_key: last_seen_key.as_deref(),
                limit: BATCH_SIZE,
            })?;

            let response = self.execute_query(&query).await?;

            // Track bytes scanned for cost awareness
            if let Some(bytes_str) = &response.total_bytes_processed {
                if let Ok(bytes) = bytes_str.parse::<u64>() {
                    total_bytes_scanned = total_bytes_scanned.saturating_add(bytes);
                }
            }

            let rows = response.rows.unwrap_or_default();
            if rows.is_empty() {
                break;
            }

            let batch_size = rows.len();
            total_rows += batch_size as u64;

            // Extract last seen key from the last row for keyset pagination
            if let Some(key_idx) = pagination_key_idx {
                if let Some(last_row) = rows.last() {
                    last_seen_key = last_row
                        .f
                        .as_ref()
                        .and_then(|cells| cells.get(key_idx))
                        .and_then(|cell| cell.v.as_ref())
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                }
            }

            // Convert rows to Arrow RecordBatch
            let batch = self.rows_to_record_batch(&rows, &schema, arrow_schema.clone())?;
            all_batches.push(batch);

            // If we got fewer rows than the limit, we're done
            if batch_size < BATCH_SIZE as usize {
                break;
            }

            // Safety check: if we can't extract pagination key, stop to avoid infinite loop
            if pagination_key_idx.is_some() && last_seen_key.is_none() {
                tracing::warn!(
                    table = %table,
                    "Could not extract pagination key value, stopping fetch"
                );
                break;
            }
        }

        tracing::info!(
            project = %self.config.project_id,
            dataset = %self.config.dataset,
            table = %table,
            rows = total_rows,
            batches = all_batches.len(),
            bytes_scanned = total_bytes_scanned,
            incremental = incremental_key.is_some(),
            "Fetched BigQuery table data"
        );

        Ok(all_batches)
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
        // Validate identifiers to prevent SQL injection
        if !Self::is_valid_project_id(&self.config.project_id) {
            return Err(ConnectorError::Config(format!(
                "Invalid project ID: '{}'",
                self.config.project_id
            )));
        }
        if !Self::is_valid_identifier(&self.config.dataset) {
            return Err(ConnectorError::Config(format!(
                "Invalid dataset name: '{}'",
                self.config.dataset
            )));
        }

        // Try to list tables to validate credentials
        let query = format!(
            "SELECT 1 FROM `{}.{}.INFORMATION_SCHEMA.TABLES` LIMIT 1",
            self.config.project_id, self.config.dataset
        );

        self.execute_query(&query).await?;

        tracing::debug!(
            project = %self.config.project_id,
            dataset = %self.config.dataset,
            "BigQuery credentials validated successfully"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bigquery_config_creation() {
        let config = BigQueryConfig::new("my-project", "my_dataset");
        assert_eq!(config.project_id, "my-project");
        assert_eq!(config.dataset, "my_dataset");
        assert!(config.tables.is_empty());
        assert_eq!(config.location, "US");
    }

    #[test]
    fn test_bigquery_config_with_credentials_path() {
        let config = BigQueryConfig::new("project", "dataset")
            .with_credentials_path("/path/to/creds.json");
        assert_eq!(
            config.credentials_path,
            Some("/path/to/creds.json".to_string())
        );
    }

    #[test]
    fn test_bigquery_config_with_tables() {
        let config = BigQueryConfig::new("project", "dataset")
            .with_tables(vec!["users".to_string(), "orders".to_string()]);
        assert_eq!(config.tables.len(), 2);
    }

    #[test]
    fn test_bigquery_config_with_location() {
        let config = BigQueryConfig::new("project", "dataset").with_location("EU");
        assert_eq!(config.location, "EU");
    }

    #[test]
    fn test_bigquery_config_with_max_bytes() {
        let config =
            BigQueryConfig::new("project", "dataset").with_maximum_bytes_billed(1_000_000_000);
        assert_eq!(config.maximum_bytes_billed, Some(1_000_000_000));
    }

    #[test]
    fn test_bigquery_type_to_column_type_integers() {
        assert!(matches!(
            BigQueryConnector::bigquery_type_to_column_type("INT64"),
            ColumnType::Int64
        ));
        assert!(matches!(
            BigQueryConnector::bigquery_type_to_column_type("INTEGER"),
            ColumnType::Int64
        ));
    }

    #[test]
    fn test_bigquery_type_to_column_type_floats() {
        assert!(matches!(
            BigQueryConnector::bigquery_type_to_column_type("FLOAT64"),
            ColumnType::Float64
        ));
        assert!(matches!(
            BigQueryConnector::bigquery_type_to_column_type("FLOAT"),
            ColumnType::Float64
        ));
    }

    #[test]
    fn test_bigquery_type_to_column_type_boolean() {
        assert!(matches!(
            BigQueryConnector::bigquery_type_to_column_type("BOOL"),
            ColumnType::Boolean
        ));
        assert!(matches!(
            BigQueryConnector::bigquery_type_to_column_type("BOOLEAN"),
            ColumnType::Boolean
        ));
    }

    #[test]
    fn test_bigquery_type_to_column_type_decimal() {
        assert!(matches!(
            BigQueryConnector::bigquery_type_to_column_type("NUMERIC"),
            ColumnType::Decimal
        ));
        assert!(matches!(
            BigQueryConnector::bigquery_type_to_column_type("BIGNUMERIC"),
            ColumnType::Decimal
        ));
    }

    #[test]
    fn test_bigquery_type_to_column_type_timestamps() {
        assert!(matches!(
            BigQueryConnector::bigquery_type_to_column_type("TIMESTAMP"),
            ColumnType::Timestamp
        ));
        assert!(matches!(
            BigQueryConnector::bigquery_type_to_column_type("DATETIME"),
            ColumnType::Timestamp
        ));
    }

    #[test]
    fn test_bigquery_type_to_column_type_date() {
        assert!(matches!(
            BigQueryConnector::bigquery_type_to_column_type("DATE"),
            ColumnType::Date
        ));
    }

    #[test]
    fn test_bigquery_type_to_column_type_json() {
        assert!(matches!(
            BigQueryConnector::bigquery_type_to_column_type("JSON"),
            ColumnType::Json
        ));
    }

    #[test]
    fn test_bigquery_type_to_column_type_default_string() {
        assert!(matches!(
            BigQueryConnector::bigquery_type_to_column_type("STRING"),
            ColumnType::String
        ));
        assert!(matches!(
            BigQueryConnector::bigquery_type_to_column_type("BYTES"),
            ColumnType::String
        ));
        assert!(matches!(
            BigQueryConnector::bigquery_type_to_column_type("GEOGRAPHY"),
            ColumnType::String
        ));
    }

    #[test]
    fn test_source_type() {
        let config = BigQueryConfig::new("project", "dataset");
        let connector = BigQueryConnector::new(config);
        assert_eq!(connector.source_type(), SourceType::BigQuery);
    }

    #[test]
    fn test_is_valid_identifier() {
        assert!(BigQueryConnector::is_valid_identifier("valid_column"));
        assert!(BigQueryConnector::is_valid_identifier("Column1"));
        assert!(BigQueryConnector::is_valid_identifier("_private"));

        assert!(!BigQueryConnector::is_valid_identifier(""));
        assert!(!BigQueryConnector::is_valid_identifier("123start"));
        assert!(!BigQueryConnector::is_valid_identifier("has-dash"));
    }

    #[test]
    fn test_is_valid_identifier_rejects_backticks() {
        assert!(!BigQueryConnector::is_valid_identifier("db`name"));
        assert!(!BigQueryConnector::is_valid_identifier("`table"));
    }

    #[test]
    fn test_build_fetch_query_validates_identifiers() {
        // Valid query should succeed
        let result = BigQueryConnector::build_fetch_query(&BqFetchQueryParams {
            project: "my-project-123",
            dataset: "dataset",
            table: "users",
            columns: &["id".to_string(), "name".to_string()],
            incremental_key: None,
            last_value: None,
            pagination_key: None,
            last_seen_key: None,
            limit: 100,
        });
        assert!(result.is_ok());
        let query = result.unwrap();
        assert!(query.contains("SELECT"));
        assert!(query.contains("`my-project-123.dataset.users`"));

        // Invalid table name should fail
        let result = BigQueryConnector::build_fetch_query(&BqFetchQueryParams {
            project: "my-project-123",
            dataset: "dataset",
            table: "users; DROP TABLE users",
            columns: &[],
            incremental_key: None,
            last_value: None,
            pagination_key: None,
            last_seen_key: None,
            limit: 100,
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_escape_bigquery_string() {
        assert_eq!(
            BigQueryConnector::escape_bigquery_string("normal"),
            "normal"
        );
        assert_eq!(
            BigQueryConnector::escape_bigquery_string("it's"),
            "it\\'s"
        );
        assert_eq!(
            BigQueryConnector::escape_bigquery_string("back\\slash"),
            "back\\\\slash"
        );
        assert_eq!(
            BigQueryConnector::escape_bigquery_string("new\nline"),
            "new\\nline"
        );
    }

    #[test]
    fn test_is_valid_project_id() {
        // Valid project IDs
        assert!(BigQueryConnector::is_valid_project_id("my-project-123"));
        assert!(BigQueryConnector::is_valid_project_id("myproject"));
        assert!(BigQueryConnector::is_valid_project_id("project123"));
        assert!(BigQueryConnector::is_valid_project_id("a-b-c-123"));

        // Invalid project IDs
        assert!(!BigQueryConnector::is_valid_project_id("")); // Empty
        assert!(!BigQueryConnector::is_valid_project_id("abc")); // Too short (<6)
        assert!(!BigQueryConnector::is_valid_project_id("12345")); // Too short
        assert!(!BigQueryConnector::is_valid_project_id("123project")); // Starts with digit
        assert!(!BigQueryConnector::is_valid_project_id("Project")); // Uppercase
        assert!(!BigQueryConnector::is_valid_project_id("my_project")); // Underscore not allowed
        assert!(!BigQueryConnector::is_valid_project_id("project-")); // Ends with hyphen
        assert!(!BigQueryConnector::is_valid_project_id("project`inject")); // Backtick
        assert!(!BigQueryConnector::is_valid_project_id(
            "this-project-id-is-way-too-long-exceeds-30-chars"
        )); // Too long (>30)
    }

    #[test]
    fn test_is_valid_project_id_rejects_backticks() {
        assert!(!BigQueryConnector::is_valid_project_id("my-pro`ject"));
        assert!(!BigQueryConnector::is_valid_project_id("`project"));
    }

    #[test]
    fn test_build_fetch_query_with_keyset_pagination() {
        // Query with keyset pagination
        let result = BigQueryConnector::build_fetch_query(&BqFetchQueryParams {
            project: "my-project-123",
            dataset: "dataset",
            table: "users",
            columns: &["id".to_string(), "name".to_string()],
            incremental_key: None,
            last_value: None,
            pagination_key: Some("id"),
            last_seen_key: Some("12345"),
            limit: 100,
        });
        assert!(result.is_ok());
        let query = result.unwrap();
        assert!(query.contains("WHERE `id` > '12345'"));
        assert!(query.contains("ORDER BY `id` ASC"));
        assert!(query.contains("LIMIT 100"));
        assert!(!query.contains("OFFSET")); // No OFFSET with keyset pagination
    }

    #[test]
    fn test_build_fetch_query_with_incremental_and_keyset() {
        // Query with both incremental filter and keyset pagination
        let result = BigQueryConnector::build_fetch_query(&BqFetchQueryParams {
            project: "my-project-123",
            dataset: "dataset",
            table: "events",
            columns: &[],
            incremental_key: Some("updated_at"),
            last_value: Some("2024-01-01"),
            pagination_key: Some("id"),
            last_seen_key: Some("999"),
            limit: 50,
        });
        assert!(result.is_ok());
        let query = result.unwrap();
        // Both conditions should be in WHERE clause
        assert!(query.contains("`updated_at` > '2024-01-01'"));
        assert!(query.contains("`id` > '999'"));
        assert!(query.contains(" AND "));
    }

    #[test]
    fn test_is_retryable_error() {
        // Network errors are retryable
        assert!(BigQueryConnector::is_retryable_error(&ConnectorError::Network(
            "connection reset".to_string()
        )));

        // Rate limiting (429) is retryable
        assert!(BigQueryConnector::is_retryable_error(&ConnectorError::Internal(
            "BigQuery query failed with status 429: Rate limit exceeded".to_string()
        )));

        // Server errors (5xx) are retryable
        assert!(BigQueryConnector::is_retryable_error(&ConnectorError::Internal(
            "BigQuery query failed with status 503: Service unavailable".to_string()
        )));

        // Config errors are not retryable
        assert!(!BigQueryConnector::is_retryable_error(&ConnectorError::Config(
            "Invalid project ID".to_string()
        )));

        // Client errors (4xx other than 429) are not retryable
        assert!(!BigQueryConnector::is_retryable_error(&ConnectorError::Internal(
            "BigQuery query failed with status 403: Forbidden".to_string()
        )));
    }

    #[test]
    fn test_config_with_credentials_json() {
        let config = BigQueryConfig::new("my-project-123", "dataset")
            .with_credentials_json(r#"{"type": "service_account"}"#);
        assert_eq!(
            config.credentials_json,
            Some(r#"{"type": "service_account"}"#.to_string())
        );
        // credentials_json takes precedence, so credentials_path should be None
        assert!(config.credentials_path.is_none());
    }

    #[test]
    fn test_escape_bigquery_string_edge_cases() {
        // Null bytes are removed
        assert_eq!(
            BigQueryConnector::escape_bigquery_string("hello\0world"),
            "helloworld"
        );

        // Tab characters are escaped
        assert_eq!(
            BigQueryConnector::escape_bigquery_string("col1\tcol2"),
            "col1\\tcol2"
        );

        // Carriage return is escaped
        assert_eq!(
            BigQueryConnector::escape_bigquery_string("line1\rline2"),
            "line1\\rline2"
        );

        // Complex escaping
        assert_eq!(
            BigQueryConnector::escape_bigquery_string("it's a \"test\"\nwith\\slashes"),
            "it\\'s a \"test\"\\nwith\\\\slashes"
        );
    }
}
