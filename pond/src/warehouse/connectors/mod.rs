//! Data source connectors for the data warehouse.
//!
//! Each connector implements the `Connector` trait to provide a unified
//! interface for fetching data from external sources.
//!
//! # Streaming Support
//!
//! The `Connector` trait supports two modes of fetching data:
//! - `fetch_table`: Returns all data as a Vec (simple, but limited by memory)
//! - `fetch_table_stream`: Returns a Stream for memory-efficient large table syncs
//!
//! Implement `fetch_table_stream` for connectors that need to handle TB-scale tables.
//!
//! # Connector Categories
//!
//! - **Database connectors**: PostgreSQL, MySQL, MongoDB, etc.
//! - **SaaS/API connectors**: Stripe, Salesforce, HubSpot, etc. (use `http_api` base)
//! - **File connectors**: CSV, Excel, JSON, XML (use `file` base)
//! - **Blockchain connectors**: Ethereum, Solana, etc. (use `blockchain` base)

// Existing connectors
pub mod postgres;
pub mod stripe;
pub mod hubspot;
pub mod salesforce;
pub mod shopify;
pub mod notion;
pub mod linear;
pub mod airtable;
pub mod jira;
pub mod zendesk;
pub mod google_ads;
pub mod facebook_ads;
pub mod intercom;
pub mod github;
pub mod asana;
pub mod google_sheets;
pub mod quickbooks;
pub mod xero;
pub mod mixpanel;
pub mod woocommerce;
pub mod posthog;
pub mod monday;

// Base implementations for connector categories
pub mod oauth;
pub mod http_api;
pub mod file;
pub mod blockchain;

// Organized connector subdirectories
pub mod databases;
pub mod files;

// WAL-based indexing for CDC/Oplog databases
pub mod wal_index;

// Shared utilities
pub mod schema_utils;
pub mod date_parsing;
pub mod builders;

// Connector factory
pub mod factory;

// Connector catalog (UI metadata + config field schemas)
pub mod catalog;

// Re-export commonly used types
pub use oauth::OAuthConfig;
pub use http_api::{HttpApiClient, AuthConfig, PaginationStyle};
pub use file::{FileConnector, FileFormat, FileStorage};
pub use blockchain::{BlockchainConnector, BlockchainConfig, BlockchainType, BitcoinConnector, BitcoinConfig, BitcoinNetwork, EthereumConnector, EthereumConfig};
pub use databases::{ClickHouseConfig, ClickHouseConnector, MongoDBConfig, MongoDBConnector, MongoDBWalIndexManager, MySqlConfig, MySqlConnector, ReadPreference, RedshiftConfig, RedshiftConnector, RedshiftSslMode, SnowflakeConfig, SnowflakeConnector, SqlServerConfig, SqlServerConnector, SqlServerIndexManager, SqlServerWalIndexManager};
pub use postgres::{PostgresConfig, PostgresConnector};
pub use files::{CsvConnector, CsvConnectorConfig};
pub use date_parsing::{DateFormat, DateParseError, detect_date_format};
pub use google_sheets::{GoogleSheetsConnector, GoogleSheetsConfig, SHEETS_READONLY_SCOPE};
pub use hubspot::{HubSpotConnector, HubSpotConfig};
pub use salesforce::{SalesforceConnector, SalesforceConfig};
pub use shopify::{ShopifyConnector, ShopifyConfig};
pub use notion::{NotionConnector, NotionConfig};
pub use linear::{LinearConnector, LinearConfig};
pub use airtable::{AirtableConnector, AirtableConfig};
pub use jira::{JiraConnector, JiraConfig};
pub use zendesk::{ZendeskConnector, ZendeskConfig};
pub use google_ads::{GoogleAdsConnector, GoogleAdsConfig};
pub use facebook_ads::{FacebookAdsConnector, FacebookAdsConfig};
pub use intercom::{IntercomConnector, IntercomConfig};
pub use github::{GitHubConnector, GitHubConfig};
pub use quickbooks::{QuickBooksConnector, QuickBooksConfig};
pub use xero::{XeroConnector, XeroConfig};
pub use mixpanel::{MixpanelConnector, MixpanelConfig};
pub use woocommerce::{WooCommerceConnector, WooCommerceConfig};
pub use posthog::{PostHogConnector, PostHogConfig};
pub use asana::{AsanaConnector, AsanaConfig};
pub use monday::{MondayConnector, MondayConfig};

use crate::warehouse::types::{SourceType, TableSchema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream::BoxStream;
use std::pin::Pin;
use thiserror::Error;

/// Errors that can occur during connector operations.
#[derive(Debug, Error)]
pub enum ConnectorError {
    #[error("Authentication failed: {0}")]
    Authentication(String),

    #[error("Rate limited, retry after {retry_after_secs} seconds")]
    RateLimited { retry_after_secs: u64 },

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Network error: {0}")]
    Network(String),

    #[error("Data validation error: {0}")]
    Validation(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("OAuth token expired and refresh failed: {0}")]
    OAuthExpired(String),

    #[error("Stream ended unexpectedly: {0}")]
    StreamEnded(String),

    #[error("Schema inference failed: {0}")]
    SchemaInference(String),

    #[error("Unsupported file format: {0}")]
    UnsupportedFormat(String),

    #[error("Blockchain RPC error: {0}")]
    BlockchainRpc(String),
}

/// Result type for connector operations.
pub type ConnectorResult<T> = Result<T, ConnectorError>;

/// Enforce that a SQL string is read-only (SELECT or EXPLAIN only).
///
/// Uses sqlparser to parse the statement and check the AST, which is safe
/// against leading comments, whitespace, and other bypass tricks that
/// string-prefix checking misses.
pub fn enforce_read_only_sql(sql: &str) -> ConnectorResult<()> {
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql.trim())
        .map_err(|e| ConnectorError::Validation(format!("Invalid SQL: {}", e)))?;

    if statements.is_empty() {
        return Err(ConnectorError::Validation(
            "Empty SQL statement".to_string(),
        ));
    }

    for stmt in &statements {
        match stmt {
            Statement::Query(_) | Statement::Explain { .. } => {}
            _ => {
                return Err(ConnectorError::Validation(
                    "Only SELECT queries are permitted on source databases".to_string(),
                ));
            }
        }
    }

    Ok(())
}

/// Table information returned by list_tables.
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// Table name
    pub name: String,
    /// Schema definition
    pub schema: TableSchema,
    /// Whether the table supports incremental sync
    pub supports_incremental: bool,
    /// Column to use for incremental sync (e.g., "updated_at")
    pub incremental_key: Option<String>,
    /// Estimated row count (if available)
    pub estimated_rows: Option<u64>,
    /// Primary key column(s) for deduplication during incremental sync.
    /// Empty if PK is unknown or not discoverable.
    pub primary_key_columns: Vec<String>,
}

/// A stream of RecordBatches for memory-efficient data fetching.
pub type RecordBatchStream = BoxStream<'static, ConnectorResult<RecordBatch>>;

/// Options for fetching table data.
#[derive(Debug, Clone, Default)]
pub struct FetchOptions {
    /// Column to use for incremental sync (e.g., "updated_at")
    pub incremental_key: Option<String>,
    /// Last value of the incremental key (for incremental sync)
    pub last_value: Option<String>,
    /// Maximum number of rows to fetch per batch (for pagination)
    pub batch_size: Option<usize>,
    /// Maximum total rows to fetch (for sampling/testing)
    pub max_rows: Option<usize>,
    /// Predicates to push down to the source for filtering.
    /// Connectors that support filtering should translate these into
    /// source-specific filters (SQL WHERE clauses, API parameters, etc.).
    /// Unsupported predicates are silently ignored -- the caller applies
    /// post-filtering for anything not pushed down.
    pub predicates: Vec<crate::warehouse::query::predicate_pushdown::Predicate>,
    /// Column projection -- only fetch these columns if supported.
    pub projection: Option<Vec<String>>,
}

impl FetchOptions {
    /// Create options for full sync.
    pub fn full_sync() -> Self {
        Self::default()
    }
    
    /// Create options for incremental sync.
    pub fn incremental(key: impl Into<String>, last_value: impl Into<String>) -> Self {
        Self {
            incremental_key: Some(key.into()),
            last_value: Some(last_value.into()),
            ..Default::default()
        }
    }
    
    /// Set the batch size for pagination.
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = Some(size);
        self
    }
}

/// Trait for data source connectors.
///
/// Connectors provide a unified interface for:
/// - Discovering available tables and their schemas
/// - Fetching table data (full or incremental)
/// - Validating credentials
///
/// # Memory-Efficient Fetching
///
/// For TB-scale tables, implement `fetch_table_stream` to return a Stream that
/// yields RecordBatches one at a time. This prevents loading the entire table
/// into memory.
///
/// The default implementation of `fetch_table_stream` wraps the `fetch_table`
/// method, but connectors should override it for large datasets.
#[async_trait]
pub trait Connector: Send + Sync {
    /// Get the source type for this connector.
    fn source_type(&self) -> SourceType;

    /// List all available tables and their schemas.
    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>>;

    /// Get the schema for a specific table.
    async fn get_schema(&self, table: &str) -> ConnectorResult<TableSchema>;

    /// Fetch data from a table as a stream (primary method).
    ///
    /// Every connector must implement this. The stream yields `RecordBatch`es
    /// one at a time, enabling memory-efficient processing of large tables.
    /// Connectors with pagination should yield one batch per page instead of
    /// buffering the entire result set.
    fn fetch_table_stream<'a>(
        &'a self,
        table: &'a str,
        options: FetchOptions,
    ) -> Pin<Box<dyn futures::Future<Output = ConnectorResult<RecordBatchStream>> + Send + 'a>>;

    /// Fetch data from a table (buffered convenience method).
    ///
    /// Default implementation collects `fetch_table_stream` into a `Vec`.
    /// Connectors may override for optimized buffered paths.
    async fn fetch_table(
        &self,
        table: &str,
        incremental_key: Option<&str>,
        last_value: Option<&str>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        use futures::TryStreamExt;
        let stream = self.fetch_table_stream(table, FetchOptions {
            incremental_key: incremental_key.map(String::from),
            last_value: last_value.map(String::from),
            ..Default::default()
        }).await?;
        stream.try_collect().await
    }

    /// Validate that the connector credentials are valid.
    async fn validate_credentials(&self) -> ConnectorResult<()>;
    
    /// Check if this connector supports direct SQL execution.
    ///
    /// Connectors that can execute arbitrary SQL queries (like PostgreSQL, MySQL)
    /// should override this to return true. This enables query pushdown optimization
    /// where the entire query is executed on the source database.
    fn supports_sql_pushdown(&self) -> bool {
        false // Default: does not support SQL pushdown
    }
    
    /// Execute an arbitrary SQL query on the data source.
    ///
    /// This method is optional and only available for connectors that support
    /// direct SQL execution (like PostgreSQL, MySQL, BigQuery). Use 
    /// `supports_sql_pushdown()` to check if this method is available.
    ///
    /// # Arguments
    /// * `sql` - The SQL query to execute
    ///
    /// # Returns
    /// Vector of Arrow RecordBatches containing the query results.
    ///
    /// # Default Implementation
    /// Returns an error indicating SQL execution is not supported.
    async fn execute_sql(&self, _sql: &str) -> ConnectorResult<Vec<RecordBatch>> {
        Err(ConnectorError::Internal(
            "SQL execution not supported by this connector".to_string()
        ))
    }
    
    /// Check if this connector supports CDC (Change Data Capture) / WAL tracking.
    ///
    /// Connectors with CDC support can track incremental changes and support
    /// warm or hot tiers. Sources without CDC (like REST APIs,
    /// Stripe, Salesforce) can only operate in cold tier.
    ///
    /// CDC-capable sources include:
    /// - PostgreSQL (via logical replication / WAL)
    /// - MySQL (via binlog)
    /// - MongoDB (via change streams / oplog)
    /// - SQL Server (via CDC tables)
    ///
    /// Non-CDC sources include:
    /// - REST APIs (Stripe, Salesforce, etc.)
    /// - File-based sources (CSV, Excel, JSON)
    /// - Google Sheets
    ///
    /// # Default Implementation
    /// Returns `false` (no CDC support). Database connectors should override
    /// this to return `true`.
    fn supports_cdc(&self) -> bool {
        false
    }

    /// Fetch changes from a CDC-capable source since the given checkpoint.
    ///
    /// Returns a vector of WAL events (inserts, updates, deletes) that occurred
    /// since the last checkpoint. Only available for connectors that return
    /// `true` from `supports_cdc()`.
    ///
    /// # Arguments
    /// * `table` - Table to read changes from
    /// * `checkpoint` - Opaque checkpoint bytes from the last sync
    ///
    /// # Default Implementation
    /// Returns an error indicating CDC is not supported.
    async fn fetch_changes(
        &self,
        _table: &str,
        _checkpoint: &[u8],
    ) -> ConnectorResult<Vec<crate::warehouse::connectors::wal_index::types::WalEvent>> {
        Err(ConnectorError::Internal("CDC not supported by this connector".to_string()))
    }

    /// Fetch all primary keys from a table (for reconciliation mode).
    ///
    /// Used by cursor-based (non-CDC) sources to detect deletes by comparing
    /// the set of PKs in the source against what's stored in Parquet files.
    ///
    /// # Default Implementation
    /// Returns an error indicating PK fetch is not supported.
    async fn fetch_primary_keys(
        &self,
        _table: &str,
        _pk_columns: &[String],
    ) -> ConnectorResult<Vec<Vec<String>>> {
        Err(ConnectorError::Internal("Primary key fetch not supported by this connector".to_string()))
    }

    /// Check if this connector supports writing data.
    fn supports_write(&self) -> bool {
        false
    }

    /// Check if this connector wraps writes in a transaction so that
    /// either all rows land or none do. Database connectors with SQL
    /// transaction support should return `true`.
    fn supports_transactional_write(&self) -> bool {
        false
    }

    /// Write record batches to a table on the data source.
    ///
    /// Database connectors implement this via INSERT statements.
    /// API connectors implement via POST/PUT requests.
    async fn write_table(
        &self,
        _table: &str,
        _batches: Vec<RecordBatch>,
    ) -> ConnectorResult<WriteResult> {
        Err(ConnectorError::Internal("Write not supported by this connector".to_string()))
    }
}

/// Result of a write operation.
#[derive(Debug, Clone)]
pub struct WriteResult {
    pub rows_written: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enforce_read_only_allows_select() {
        assert!(enforce_read_only_sql("SELECT 1").is_ok());
        assert!(enforce_read_only_sql("SELECT * FROM t WHERE x = 1").is_ok());
        assert!(enforce_read_only_sql("WITH cte AS (SELECT 1) SELECT * FROM cte").is_ok());
    }

    #[test]
    fn test_enforce_read_only_allows_explain() {
        assert!(enforce_read_only_sql("EXPLAIN SELECT 1").is_ok());
    }

    #[test]
    fn test_enforce_read_only_rejects_mutations() {
        assert!(enforce_read_only_sql("INSERT INTO t VALUES (1)").is_err());
        assert!(enforce_read_only_sql("UPDATE t SET x = 1").is_err());
        assert!(enforce_read_only_sql("DELETE FROM t").is_err());
        assert!(enforce_read_only_sql("DROP TABLE t").is_err());
        assert!(enforce_read_only_sql("CREATE TABLE t (id INT)").is_err());
        assert!(enforce_read_only_sql("ALTER TABLE t ADD COLUMN x INT").is_err());
        assert!(enforce_read_only_sql("TRUNCATE t").is_err());
    }

    #[test]
    fn test_enforce_read_only_rejects_comment_bypass() {
        assert!(enforce_read_only_sql("/* SELECT */ DROP TABLE t").is_err());
        assert!(enforce_read_only_sql("-- SELECT\nDROP TABLE t").is_err());
    }

    #[test]
    fn test_enforce_read_only_rejects_empty() {
        assert!(enforce_read_only_sql("").is_err());
    }

    #[test]
    fn test_enforce_read_only_rejects_multi_statement_with_mutation() {
        assert!(enforce_read_only_sql("SELECT 1; DROP TABLE t").is_err());
    }
}
