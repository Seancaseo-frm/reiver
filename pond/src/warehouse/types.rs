//! Common types for the data warehouse module.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::Duration as StdDuration;
use thiserror::Error;
use uuid::Uuid;

use crate::warehouse::indexes::strategy::IndexStrategy;

/// Errors that can occur during warehouse type validation.
#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("Invalid table name: {0}")]
    InvalidTableName(String),
    #[error("Invalid column name: {0}")]
    InvalidColumnName(String),
}

/// Validate that a table name is safe for use in R2 paths.
///
/// SECURITY: This prevents path traversal attacks where a malicious table name
/// like "../other_project/secrets" could access data outside the project's prefix.
///
/// Valid table names:
/// - Contain only alphanumeric characters, underscores, hyphens, and dots
/// - Do not contain path separators (/, \)
/// - Do not contain path traversal sequences (..)
/// - Are not empty
/// - Do not start with a dot
pub fn validate_table_name(name: &str) -> Result<(), ValidationError> {
    if name.is_empty() {
        return Err(ValidationError::InvalidTableName(
            "Table name cannot be empty".to_string(),
        ));
    }

    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
    {
        return Err(ValidationError::InvalidTableName(
            "Table name must contain only alphanumeric characters, underscores, hyphens, and dots"
                .to_string(),
        ));
    }

    if name.starts_with('.') {
        return Err(ValidationError::InvalidTableName(
            "Table name cannot start with a dot".to_string(),
        ));
    }

    if name.contains("..") {
        return Err(ValidationError::InvalidTableName(
            "Table name cannot contain path traversal sequence '..'".to_string(),
        ));
    }

    Ok(())
}

/// Validate that a column name is safe for use in SQL and file paths.
///
/// SECURITY: This prevents injection attacks where a malicious column name
/// could contain SQL injection or path traversal characters.
///
/// Valid column names:
/// - Contain only alphanumeric characters and underscores
/// - Are not empty
/// - Do not exceed 128 characters
/// - Start with a letter or underscore
pub fn validate_column_name(name: &str) -> Result<(), ValidationError> {
    if name.is_empty() {
        return Err(ValidationError::InvalidColumnName(
            "Column name cannot be empty".to_string(),
        ));
    }

    if name.len() > 128 {
        return Err(ValidationError::InvalidColumnName(
            "Column name cannot exceed 128 characters".to_string(),
        ));
    }

    let mut chars = name.chars();

    // First character must be letter or underscore
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => {
            return Err(ValidationError::InvalidColumnName(
                "Column name must start with a letter or underscore".to_string(),
            ));
        }
    }

    // Remaining characters must be alphanumeric or underscore
    for c in chars {
        if !c.is_ascii_alphanumeric() && c != '_' {
            return Err(ValidationError::InvalidColumnName(format!(
                "Column name contains invalid character: '{}'",
                c
            )));
        }
    }

    Ok(())
}

/// Where synced data is stored.
///
/// This determines the query path and performance characteristics:
/// - `NativeClickHouse`: Data stored in ClickHouse MergeTree tables (best performance)
/// - `ObjectStorage`: Data stored as Parquet files in R2/S3 (uses FST skip index)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum StorageType {
    /// Native ClickHouse MergeTree tables.
    ///
    /// PERFORMANCE: Best query performance with native indexes, sorting, and merges.
    /// Used for synced data where we control the storage.
    #[default]
    NativeClickHouse,

    /// Parquet files in R2/S3 object storage.
    ///
    /// PERFORMANCE: Uses FST skip index for file filtering, queries via s3() function.
    /// Used for client-stored data or cost-sensitive deployments.
    ObjectStorage,

    /// External data fetched on-demand (cold tier).
    ///
    /// PERFORMANCE: Data is fetched from external APIs at query time.
    /// Uses TTL caching to minimize repeated API calls.
    /// No data is stored in ClickHouse - materialized as temporary Arrow RecordBatches.
    External,
}

impl std::fmt::Display for StorageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageType::NativeClickHouse => write!(f, "native_clickhouse"),
            StorageType::ObjectStorage => write!(f, "object_storage"),
            StorageType::External => write!(f, "external"),
        }
    }
}

impl std::str::FromStr for StorageType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "native_clickhouse" | "native" => Ok(StorageType::NativeClickHouse),
            "object_storage" | "s3" | "r2" => Ok(StorageType::ObjectStorage),
            "external" => Ok(StorageType::External),
            _ => Err(format!("Unknown storage type: {}", s)),
        }
    }
}

// ============================================================================
// Job Types & Statuses
// ============================================================================

/// Type of job to execute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobType {
    /// Full data sync from source to storage.
    Sync,
    /// Upgrade a source to warm tier (copy to R2/S3 as Parquet).
    UpgradeToWarm,
    /// Upgrade a source to hot tier (copy to ClickHouse native tables).
    UpgradeToHot,
    /// Downgrade a source to warm tier.
    DowngradeToWarm,
    /// Downgrade a source to cold tier (remove synced data).
    DowngradeToCold,
    /// Legacy variant kept for DB/Kafka backwards compatibility. No-op at runtime.
    IndexBuild,
    /// Rebuild FST indexes.
    FstRebuild,
    /// Take a schema snapshot.
    SchemaSnapshot,
    /// Refresh a derived table (re-execute its defining query).
    DerivedRefresh,
}

impl std::fmt::Display for JobType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobType::Sync => write!(f, "sync"),
            JobType::UpgradeToWarm => write!(f, "upgrade_to_warm"),
            JobType::UpgradeToHot => write!(f, "upgrade_to_hot"),
            JobType::DowngradeToWarm => write!(f, "downgrade_to_warm"),
            JobType::DowngradeToCold => write!(f, "downgrade_to_cold"),
            JobType::IndexBuild => write!(f, "index_build"),
            JobType::FstRebuild => write!(f, "fst_rebuild"),
            JobType::SchemaSnapshot => write!(f, "schema_snapshot"),
            JobType::DerivedRefresh => write!(f, "derived_refresh"),
        }
    }
}

impl std::str::FromStr for JobType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "sync" => Ok(JobType::Sync),
            "upgrade_to_warm" => Ok(JobType::UpgradeToWarm),
            "upgrade_to_hot" => Ok(JobType::UpgradeToHot),
            "downgrade_to_warm" => Ok(JobType::DowngradeToWarm),
            "downgrade_to_cold" => Ok(JobType::DowngradeToCold),
            "index_build" => Ok(JobType::IndexBuild),
            "fst_rebuild" => Ok(JobType::FstRebuild),
            "schema_snapshot" => Ok(JobType::SchemaSnapshot),
            "derived_refresh" => Ok(JobType::DerivedRefresh),
            _ => Err(format!("Unknown job type: {}", s)),
        }
    }
}

/// Status of a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobStatus::Pending => write!(f, "pending"),
            JobStatus::Running => write!(f, "running"),
            JobStatus::Completed => write!(f, "completed"),
            JobStatus::Failed => write!(f, "failed"),
            JobStatus::Cancelled => write!(f, "cancelled"),
        }
    }
}

impl std::str::FromStr for JobStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "pending" => Ok(JobStatus::Pending),
            "running" => Ok(JobStatus::Running),
            "completed" => Ok(JobStatus::Completed),
            "failed" => Ok(JobStatus::Failed),
            "cancelled" => Ok(JobStatus::Cancelled),
            _ => Err(format!("Unknown job status: {}", s)),
        }
    }
}

// ============================================================================
// Partition State
// ============================================================================

/// Partition state.
///
/// The `Frozen` variant is a legacy value that may exist in the database but
/// no longer triggers special behavior. All partitions are treated uniformly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionState {
    /// Active partition.
    Mutable,
    /// Legacy state kept for DB backwards compatibility. No special behavior.
    Frozen,
}

impl Default for PartitionState {
    fn default() -> Self {
        PartitionState::Mutable
    }
}

impl PartitionState {
    /// Convert to database string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            PartitionState::Mutable => "mutable",
            PartitionState::Frozen => "frozen",
        }
    }
}

impl std::fmt::Display for PartitionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for PartitionState {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "frozen" => Ok(PartitionState::Frozen),
            _ => Ok(PartitionState::Mutable),
        }
    }
}

/// Represents a connected data source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WarehouseSource {
    pub id: Uuid,
    pub project_id: Uuid,
    pub name: String,
    pub source_type: SourceType,
    /// Where synced data is stored (native ClickHouse or object storage).
    #[serde(default)]
    pub storage_type: StorageType,
    pub config: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Types of data sources that can be connected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceType {
    // ===== Currently Supported =====
    Stripe,
    #[serde(rename = "postgresql")]
    PostgreSQL,
    #[serde(rename = "mysql")]
    MySQL,
    HubSpot,
    GoogleSheets,
    Salesforce,
    Snowflake,
    BigQuery,
    /// External Parquet files stored in customer's own S3/GCS/etc.
    /// reiver queries these in-place without syncing.
    ExternalParquet,

    // ===== File Formats =====
    /// CSV files (local, S3, or HTTP)
    Csv,
    /// JSON/NDJSON files
    Json,
    /// Excel files (.xlsx, .xls)
    Excel,
    /// XML files
    Xml,

    // ===== Databases =====
    /// MongoDB (NoSQL document database)
    #[serde(rename = "mongodb")]
    MongoDB,
    /// Microsoft SQL Server
    #[serde(rename = "sqlserver")]
    SqlServer,
    /// SQLite (embedded database)
    #[serde(rename = "sqlite")]
    SQLite,
    /// Amazon Redshift
    Redshift,
    /// ClickHouse analytical database
    #[serde(rename = "clickhouse")]
    ClickHouse,

    // ===== SaaS - E-commerce =====
    /// Shopify e-commerce platform
    Shopify,
    /// WooCommerce (WordPress e-commerce)
    WooCommerce,

    // ===== SaaS - Marketing =====
    /// Google Analytics
    GoogleAnalytics,
    /// Facebook/Meta Ads
    FacebookAds,
    /// Google Ads
    GoogleAds,

    // ===== SaaS - Support =====
    /// Zendesk customer support
    Zendesk,
    /// Intercom customer messaging
    Intercom,

    // ===== SaaS - Accounting =====
    /// QuickBooks accounting
    QuickBooks,
    /// Xero accounting
    Xero,

    // ===== SaaS - Product Analytics =====
    /// Mixpanel product analytics
    Mixpanel,
    /// Amplitude product analytics
    Amplitude,
    /// PostHog open-source analytics
    PostHog,

    // ===== SaaS - Dev Tools =====
    /// GitHub repositories and issues
    GitHub,
    /// Jira project management
    Jira,
    /// Linear issue tracking
    Linear,

    // ===== SaaS - Productivity =====
    /// Notion workspace
    Notion,
    /// Confluence wiki
    Confluence,
    /// Airtable spreadsheet/database
    Airtable,
    /// Asana project management
    Asana,
    /// Monday.com work OS
    Monday,

    // ===== Cloud Storage =====
    /// Google Cloud Storage
    GoogleCloudStorage,
    /// Azure Blob Storage
    AzureBlob,

    // ===== Streaming =====
    /// Apache Kafka
    Kafka,
    /// AWS Kinesis
    AwsKinesis,

    // ===== Blockchain =====
    /// Ethereum blockchain
    Ethereum,
    /// Solana blockchain
    Solana,
    /// Bitcoin blockchain
    Bitcoin,
    /// Polygon blockchain
    Polygon,

    // ===== Derived =====
    /// Derived table created from a query (CTAS / materialized view).
    /// Data is stored in R2 as Parquet, refreshed on demand or on schedule.
    Derived,
}

impl std::fmt::Display for SourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // Currently supported
            SourceType::Stripe => write!(f, "stripe"),
            SourceType::PostgreSQL => write!(f, "postgresql"),
            SourceType::MySQL => write!(f, "mysql"),
            SourceType::HubSpot => write!(f, "hubspot"),
            SourceType::GoogleSheets => write!(f, "google_sheets"),
            SourceType::Salesforce => write!(f, "salesforce"),
            SourceType::Snowflake => write!(f, "snowflake"),
            SourceType::BigQuery => write!(f, "bigquery"),
            SourceType::ExternalParquet => write!(f, "external_parquet"),

            // File formats
            SourceType::Csv => write!(f, "csv"),
            SourceType::Json => write!(f, "json"),
            SourceType::Excel => write!(f, "excel"),
            SourceType::Xml => write!(f, "xml"),

            // Databases
            SourceType::MongoDB => write!(f, "mongodb"),
            SourceType::SqlServer => write!(f, "sqlserver"),
            SourceType::SQLite => write!(f, "sqlite"),
            SourceType::Redshift => write!(f, "redshift"),
            SourceType::ClickHouse => write!(f, "clickhouse"),

            // SaaS - E-commerce
            SourceType::Shopify => write!(f, "shopify"),
            SourceType::WooCommerce => write!(f, "woocommerce"),

            // SaaS - Marketing
            SourceType::GoogleAnalytics => write!(f, "google_analytics"),
            SourceType::FacebookAds => write!(f, "facebook_ads"),
            SourceType::GoogleAds => write!(f, "google_ads"),

            // SaaS - Support
            SourceType::Zendesk => write!(f, "zendesk"),
            SourceType::Intercom => write!(f, "intercom"),

            // SaaS - Accounting
            SourceType::QuickBooks => write!(f, "quickbooks"),
            SourceType::Xero => write!(f, "xero"),

            // SaaS - Product Analytics
            SourceType::Mixpanel => write!(f, "mixpanel"),
            SourceType::Amplitude => write!(f, "amplitude"),
            SourceType::PostHog => write!(f, "posthog"),

            // SaaS - Dev Tools
            SourceType::GitHub => write!(f, "github"),
            SourceType::Jira => write!(f, "jira"),
            SourceType::Linear => write!(f, "linear"),

            // SaaS - Productivity
            SourceType::Notion => write!(f, "notion"),
            SourceType::Confluence => write!(f, "confluence"),
            SourceType::Airtable => write!(f, "airtable"),
            SourceType::Asana => write!(f, "asana"),
            SourceType::Monday => write!(f, "monday"),

            // Cloud Storage
            SourceType::GoogleCloudStorage => write!(f, "gcs"),
            SourceType::AzureBlob => write!(f, "azure_blob"),

            // Streaming
            SourceType::Kafka => write!(f, "kafka"),
            SourceType::AwsKinesis => write!(f, "kinesis"),

            // Blockchain
            SourceType::Ethereum => write!(f, "ethereum"),
            SourceType::Solana => write!(f, "solana"),
            SourceType::Bitcoin => write!(f, "bitcoin"),
            SourceType::Polygon => write!(f, "polygon"),

            // Derived
            SourceType::Derived => write!(f, "derived"),
        }
    }
}

impl SourceType {
    /// Returns `true` for blockchain data source types.
    ///
    /// Blockchain sources use managed storage (always warm tier) and their
    /// data is synced globally — individual projects get lightweight references.
    pub fn is_blockchain(&self) -> bool {
        matches!(
            self,
            Self::Bitcoin | Self::Ethereum | Self::Solana | Self::Polygon
        )
    }
}

impl std::str::FromStr for SourceType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "stripe" => Ok(SourceType::Stripe),
            "postgresql" | "postgres" => Ok(SourceType::PostgreSQL),
            "mysql" => Ok(SourceType::MySQL),
            "hubspot" => Ok(SourceType::HubSpot),
            "google_sheets" => Ok(SourceType::GoogleSheets),
            "salesforce" => Ok(SourceType::Salesforce),
            "snowflake" => Ok(SourceType::Snowflake),
            "bigquery" => Ok(SourceType::BigQuery),
            "external_parquet" | "parquet" => Ok(SourceType::ExternalParquet),
            "csv" => Ok(SourceType::Csv),
            "json" | "ndjson" => Ok(SourceType::Json),
            "excel" | "xlsx" | "xls" => Ok(SourceType::Excel),
            "xml" => Ok(SourceType::Xml),
            "mongodb" | "mongo" => Ok(SourceType::MongoDB),
            "sqlserver" | "mssql" | "sql_server" => Ok(SourceType::SqlServer),
            "sqlite" => Ok(SourceType::SQLite),
            "redshift" => Ok(SourceType::Redshift),
            "clickhouse" => Ok(SourceType::ClickHouse),
            "shopify" => Ok(SourceType::Shopify),
            "woocommerce" => Ok(SourceType::WooCommerce),
            "google_analytics" | "ga" => Ok(SourceType::GoogleAnalytics),
            "facebook_ads" | "fb_ads" => Ok(SourceType::FacebookAds),
            "google_ads" => Ok(SourceType::GoogleAds),
            "zendesk" => Ok(SourceType::Zendesk),
            "intercom" => Ok(SourceType::Intercom),
            "quickbooks" | "qb" => Ok(SourceType::QuickBooks),
            "xero" => Ok(SourceType::Xero),
            "mixpanel" => Ok(SourceType::Mixpanel),
            "amplitude" => Ok(SourceType::Amplitude),
            "posthog" => Ok(SourceType::PostHog),
            "github" => Ok(SourceType::GitHub),
            "jira" => Ok(SourceType::Jira),
            "linear" => Ok(SourceType::Linear),
            "notion" => Ok(SourceType::Notion),
            "confluence" => Ok(SourceType::Confluence),
            "airtable" => Ok(SourceType::Airtable),
            "asana" => Ok(SourceType::Asana),
            "monday" => Ok(SourceType::Monday),
            "gcs" | "google_cloud_storage" => Ok(SourceType::GoogleCloudStorage),
            "azure_blob" => Ok(SourceType::AzureBlob),
            "kafka" => Ok(SourceType::Kafka),
            "kinesis" | "aws_kinesis" => Ok(SourceType::AwsKinesis),
            "ethereum" => Ok(SourceType::Ethereum),
            "solana" => Ok(SourceType::Solana),
            "bitcoin" => Ok(SourceType::Bitcoin),
            "polygon" => Ok(SourceType::Polygon),
            "derived" => Ok(SourceType::Derived),
            _ => Err(format!("Unknown source type: {}", s)),
        }
    }
}

// ============================================================================
// External Parquet Source Configuration
// ============================================================================

/// Configuration for external Parquet data sources.
///
/// This is used when querying customer-owned Parquet files in-place, without
/// syncing them into reiver's storage. The configuration tells reiver:
/// - Which columns to index and how (FST vs Xor Filter)
/// - How to detect partitions and their mutability
/// - When to refresh indexes
/// - Whether the customer uses Iceberg/Delta Lake table formats
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalSourceConfig {
    /// Table format used by the customer (if any).
    ///
    /// When set to Iceberg or Delta, we use their manifests instead of custom indexes.
    /// This avoids duplicate work when the customer already has table format statistics.
    #[serde(default)]
    pub table_format: TableFormat,

    /// Columns to index with optional hints.
    ///
    /// Ignored when `table_format` is Iceberg or Delta (we use their statistics).
    #[serde(default)]
    pub index_columns: Vec<IndexColumnConfig>,

    /// Column containing timestamps for partitioning (e.g., "created_at", "event_time").
    ///
    /// Used to determine partition boundaries for mutability detection.
    pub time_column: Option<String>,

    /// Path pattern for discovering partitions.
    ///
    /// Examples:
    /// - `"year={year}/month={month}/day={day}"` - Hive-style partitioning
    /// - `"dt={date}"` - Date partition
    ///
    /// Ignored when `table_format` is Iceberg or Delta (we read from manifests).
    pub partition_pattern: Option<String>,

    /// How to determine partition mutability.
    ///
    /// This affects index refresh behavior - immutable partitions never need
    /// index rebuilds, while mutable partitions may.
    #[serde(default)]
    pub mutability: MutabilityStrategy,

    /// Index refresh settings.
    #[serde(default)]
    pub refresh: RefreshConfig,
}

impl Default for ExternalSourceConfig {
    fn default() -> Self {
        Self {
            table_format: TableFormat::default(),
            index_columns: Vec::new(),
            time_column: None,
            partition_pattern: None,
            mutability: MutabilityStrategy::default(),
            refresh: RefreshConfig::default(),
        }
    }
}

/// Table format used by the customer's data.
///
/// When the customer already uses Iceberg or Delta Lake, we leverage their
/// manifest files instead of building custom FST/Xor indexes. This provides:
/// - File list without bucket listing
/// - Column statistics without sampling
/// - Partition info for pruning
/// - Time travel capabilities
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TableFormat {
    /// Auto-detect format by checking for metadata/ or _delta_log/ directories.
    Auto,

    /// Raw Parquet files without a table format.
    ///
    /// reiver builds its own FST/Xor indexes for file pruning.
    #[default]
    RawParquet,

    /// Apache Iceberg table format.
    ///
    /// Uses `metadata/` directory with manifest-list and manifest files.
    /// Provides: file list, partition info, column statistics, snapshots.
    Iceberg,

    /// Delta Lake table format.
    ///
    /// Uses `_delta_log/` directory with JSON transaction logs.
    /// Provides: file list, partition info, column statistics, time travel.
    DeltaLake,
}

impl std::fmt::Display for TableFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableFormat::Auto => write!(f, "auto"),
            TableFormat::RawParquet => write!(f, "raw_parquet"),
            TableFormat::Iceberg => write!(f, "iceberg"),
            TableFormat::DeltaLake => write!(f, "delta_lake"),
        }
    }
}

/// Configuration for a single indexed column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexColumnConfig {
    /// Column name.
    pub name: String,

    /// Optional cardinality hint to guide index strategy selection.
    ///
    /// If not provided, reiver samples the data to estimate cardinality.
    pub cardinality: Option<CardinalityHint>,

    /// Override automatic strategy selection.
    ///
    /// Use this when you know better than the automatic selector
    /// (e.g., force FST for a column you know will have low cardinality).
    pub force_strategy: Option<IndexStrategyHint>,

    /// When true, an FST index is always built for this column on freeze,
    /// regardless of cardinality. This enables `CONTAINS` / `LIKE '%term%'`
    /// queries to prune files via FST substring search.
    #[serde(default)]
    pub fulltext_indexed: bool,
}

impl IndexColumnConfig {
    /// Create a new column config with just the name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            cardinality: None,
            force_strategy: None,
            fulltext_indexed: false,
        }
    }

    /// Create a column config with a cardinality hint.
    pub fn with_cardinality(name: impl Into<String>, cardinality: CardinalityHint) -> Self {
        Self {
            name: name.into(),
            cardinality: Some(cardinality),
            force_strategy: None,
            fulltext_indexed: false,
        }
    }

    /// Create a column config with full-text indexing enabled.
    pub fn with_fulltext(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            cardinality: None,
            force_strategy: None,
            fulltext_indexed: true,
        }
    }
}

/// Cardinality hint for a column.
///
/// These hints help reiver choose the right index strategy without
/// needing to sample the entire dataset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CardinalityHint {
    /// Very low cardinality: < 100 distinct values.
    /// Examples: country codes, status enums, boolean-like strings.
    VeryLow,

    /// Low cardinality: < 10,000 distinct values.
    /// Examples: product categories, user roles, regions.
    Low,

    /// Medium cardinality: < 100,000 distinct values.
    /// Examples: product IDs, city names, tags.
    Medium,

    /// High cardinality: < 1,000,000 distinct values.
    /// Examples: user IDs in a mid-size app, order IDs.
    High,

    /// Very high cardinality: >= 1,000,000 distinct values.
    /// Examples: UUIDs, email addresses, session IDs.
    VeryHigh,
}

impl CardinalityHint {
    /// Convert to an approximate cardinality value for strategy selection.
    pub fn to_approximate_cardinality(&self) -> usize {
        match self {
            CardinalityHint::VeryLow => 50,
            CardinalityHint::Low => 5_000,
            CardinalityHint::Medium => 50_000,
            CardinalityHint::High => 500_000,
            CardinalityHint::VeryHigh => 5_000_000,
        }
    }

    /// Get the recommended IndexStrategy based on this hint.
    pub fn recommended_strategy(&self, data_type: ColumnType) -> IndexStrategy {
        // Numeric types always use stats
        match data_type {
            ColumnType::Int32
            | ColumnType::Int64
            | ColumnType::Float32
            | ColumnType::Float64
            | ColumnType::Decimal
            | ColumnType::Timestamp
            | ColumnType::Date => return IndexStrategy::NumericStats,
            _ => {}
        }

        match self {
            CardinalityHint::VeryLow | CardinalityHint::Low | CardinalityHint::Medium => {
                IndexStrategy::Fst
            }
            CardinalityHint::High => IndexStrategy::XorFilter,
            CardinalityHint::VeryHigh => IndexStrategy::Skip,
        }
    }
}

/// Index strategy hint for user overrides.
///
/// This is separate from `IndexStrategy` to provide a user-friendly API
/// that doesn't expose internal implementation details.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexStrategyHint {
    /// Force FST index (exact + prefix queries, ordered).
    Fst,
    /// Force Xor Filter (probabilistic membership, space-efficient).
    XorFilter,
    /// Force skipping this column (no index).
    Skip,
    /// Let reiver decide automatically.
    Auto,
}

impl IndexStrategyHint {
    /// Convert to IndexStrategy if this is a forced strategy.
    pub fn to_strategy(&self) -> Option<IndexStrategy> {
        match self {
            IndexStrategyHint::Fst => Some(IndexStrategy::Fst),
            IndexStrategyHint::XorFilter => Some(IndexStrategy::XorFilter),
            IndexStrategyHint::Skip => Some(IndexStrategy::Skip),
            IndexStrategyHint::Auto => None,
        }
    }
}

/// How partitions become immutable over time.
///
/// This affects index refresh strategy:
/// - Immutable partitions: build index once, never refresh
/// - Mutable partitions: rebuild index on query or at intervals
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MutabilityStrategy {
    /// All partitions are immutable (historical data, never changes).
    ///
    /// Best for: archived data, completed transactions, historical logs.
    AllImmutable,

    /// All partitions are mutable (any file can be updated).
    ///
    /// Best for: append-only tables that may receive late data.
    AllMutable,

    /// Only the last N time units are mutable (rolling window).
    ///
    /// Best for: event streams where today's data is still arriving
    /// but yesterday's data is complete.
    RollingWindow {
        /// Number of time units in the mutable window.
        window: u32,
        /// Time unit (day, hour, etc.).
        unit: TimeUnit,
    },

    /// Partitions older than N hours based on file modification time.
    ///
    /// Best for: when you don't have a time column but files stabilize
    /// after a certain period.
    FileAge {
        /// Hours after which files are considered immutable.
        hours: u32,
    },
}

impl Default for MutabilityStrategy {
    fn default() -> Self {
        // Default to rolling window of 1 day - most common pattern
        Self::RollingWindow {
            window: 1,
            unit: TimeUnit::Day,
        }
    }
}

impl MutabilityStrategy {
    /// Check if a given timestamp represents a mutable partition.
    pub fn is_mutable(&self, partition_time: DateTime<Utc>, now: DateTime<Utc>) -> bool {
        match self {
            MutabilityStrategy::AllImmutable => false,
            MutabilityStrategy::AllMutable => true,
            MutabilityStrategy::RollingWindow { window, unit } => {
                let duration = unit.to_duration(*window);
                let cutoff = now - duration;
                partition_time > cutoff
            }
            MutabilityStrategy::FileAge { hours } => {
                let cutoff = now - Duration::hours(*hours as i64);
                partition_time > cutoff
            }
        }
    }

    /// Get the duration of the mutable window (if applicable).
    pub fn mutable_window(&self) -> Option<Duration> {
        match self {
            MutabilityStrategy::RollingWindow { window, unit } => Some(unit.to_duration(*window)),
            MutabilityStrategy::FileAge { hours } => Some(Duration::hours(*hours as i64)),
            _ => None,
        }
    }
}

/// Time unit for partitioning and mutability windows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeUnit {
    Hour,
    Day,
    Week,
    Month,
}

impl TimeUnit {
    /// Convert to chrono Duration for the given count.
    pub fn to_duration(&self, count: u32) -> Duration {
        match self {
            TimeUnit::Hour => Duration::hours(count as i64),
            TimeUnit::Day => Duration::days(count as i64),
            TimeUnit::Week => Duration::weeks(count as i64),
            TimeUnit::Month => Duration::days(count as i64 * 30), // Approximate
        }
    }

    /// Convert to std::time::Duration for the given count.
    pub fn to_std_duration(&self, count: u32) -> StdDuration {
        match self {
            TimeUnit::Hour => StdDuration::from_secs(count as u64 * 3600),
            TimeUnit::Day => StdDuration::from_secs(count as u64 * 86400),
            TimeUnit::Week => StdDuration::from_secs(count as u64 * 604800),
            TimeUnit::Month => StdDuration::from_secs(count as u64 * 2592000), // ~30 days
        }
    }
}

/// Index refresh configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshConfig {
    /// Refresh interval for mutable partitions.
    ///
    /// Immutable partitions are never refreshed.
    #[serde(default)]
    pub mutable_refresh: RefreshInterval,

    /// Whether to auto-detect new files.
    ///
    /// If true, reiver will periodically list the bucket to find new files.
    #[serde(default = "default_true")]
    pub auto_discover: bool,

    /// Discovery interval when auto_discover is enabled.
    #[serde(default)]
    pub discovery_interval: RefreshInterval,
}

fn default_true() -> bool {
    true
}

impl Default for RefreshConfig {
    fn default() -> Self {
        Self {
            mutable_refresh: RefreshInterval::OnQuery,
            auto_discover: true,
            discovery_interval: RefreshInterval::Hourly,
        }
    }
}

/// When to refresh indexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RefreshInterval {
    /// Refresh on every query (checks file modification times).
    #[default]
    OnQuery,
    /// Refresh every 5 minutes.
    Every5Min,
    /// Refresh every 15 minutes.
    Every15Min,
    /// Refresh every hour.
    Hourly,
    /// Refresh every 6 hours.
    Every6Hours,
    /// Refresh daily.
    Daily,
    /// Never refresh automatically.
    Never,
}

impl RefreshInterval {
    /// Convert to duration (None for OnQuery which doesn't have a fixed interval).
    pub fn to_duration(&self) -> Option<StdDuration> {
        match self {
            RefreshInterval::OnQuery => None,
            RefreshInterval::Every5Min => Some(StdDuration::from_secs(300)),
            RefreshInterval::Every15Min => Some(StdDuration::from_secs(900)),
            RefreshInterval::Hourly => Some(StdDuration::from_secs(3600)),
            RefreshInterval::Every6Hours => Some(StdDuration::from_secs(21600)),
            RefreshInterval::Daily => Some(StdDuration::from_secs(86400)),
            RefreshInterval::Never => None,
        }
    }

    /// Check if this interval should trigger a refresh now.
    pub fn should_refresh(&self, last_refresh: DateTime<Utc>, now: DateTime<Utc>) -> bool {
        match self.to_duration() {
            Some(interval) => {
                let elapsed = now.signed_duration_since(last_refresh);
                elapsed >= Duration::from_std(interval).unwrap_or_else(|_| Duration::hours(1))
            }
            None => {
                // OnQuery or Never - don't trigger on time basis
                matches!(self, RefreshInterval::OnQuery)
            }
        }
    }
}

/// Represents a table synced from a source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WarehouseTable {
    pub id: Uuid,
    pub source_id: Uuid,
    pub name: String,
    pub schema: TableSchema,
    /// Where this table's data is stored.
    #[serde(default)]
    pub storage_type: StorageType,
    /// R2/S3 prefix for object storage (used when storage_type = ObjectStorage).
    pub r2_prefix: String,
    /// ClickHouse table name for native storage (used when storage_type = NativeClickHouse).
    /// Format: "warehouse_{project_id}_{table_name}"
    #[serde(default)]
    pub clickhouse_table: Option<String>,
    pub sync_enabled: bool,
    pub incremental_key: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl WarehouseTable {
    /// Generate the ClickHouse table name for native storage.
    ///
    /// SECURITY: Uses project_id to ensure table isolation between projects.
    pub fn generate_clickhouse_table_name(project_id: Uuid, table_name: &str) -> String {
        // Replace hyphens in UUID and sanitize table name
        let project_id_clean = project_id.to_string().replace('-', "_");
        let table_name_clean = table_name
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect::<String>();
        format!("warehouse_{}_{}", project_id_clean, table_name_clean)
    }
}

/// Schema definition for a warehouse table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub columns: Vec<ColumnSchema>,
}

/// Schema definition for a column.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: ColumnType,
    pub nullable: bool,
    pub description: Option<String>,
    /// For timestamp columns: the assumed timezone.
    ///
    /// - `None` for non-timestamp columns
    /// - `Some("UTC")` for sources that are known to be UTC (e.g., Stripe)
    /// - User-editable during source registration for ambiguous sources
    ///
    /// When this is `None` for a timestamp column, UTC is assumed.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub timezone: Option<String>,
}

impl ColumnSchema {
    /// Create a new column schema.
    pub fn new(name: impl Into<String>, data_type: ColumnType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
            description: None,
            timezone: None,
        }
    }

    /// Set the description.
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    /// Set the timezone (for timestamp columns).
    pub fn with_timezone(mut self, tz: impl Into<String>) -> Self {
        self.timezone = Some(tz.into());
        self
    }
}

/// Column data types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnType {
    #[default]
    String,
    Int32,
    Int64,
    Float32,
    Float64,
    Boolean,
    Timestamp,
    Date,
    Json,
    Uuid,
    Decimal,
}

impl ColumnType {
    /// Convert to Arrow data type.
    pub fn to_arrow_type(&self) -> arrow::datatypes::DataType {
        use arrow::datatypes::DataType;
        match self {
            ColumnType::String => DataType::Utf8,
            ColumnType::Int32 => DataType::Int32,
            ColumnType::Int64 => DataType::Int64,
            ColumnType::Float32 => DataType::Float32,
            ColumnType::Float64 => DataType::Float64,
            ColumnType::Boolean => DataType::Boolean,
            ColumnType::Timestamp => {
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
            }
            ColumnType::Date => DataType::Date32,
            ColumnType::Json => DataType::Utf8, // Store JSON as string
            ColumnType::Uuid => DataType::Utf8, // Store UUID as string
            ColumnType::Decimal => DataType::Decimal128(18, 4),
        }
    }

    /// Convert to ClickHouse type name.
    pub fn to_clickhouse_type(&self) -> &'static str {
        match self {
            ColumnType::String => "String",
            ColumnType::Int32 => "Int32",
            ColumnType::Int64 => "Int64",
            ColumnType::Float32 => "Float32",
            ColumnType::Float64 => "Float64",
            ColumnType::Boolean => "Bool",
            ColumnType::Timestamp => "DateTime64(6)",
            ColumnType::Date => "Date",
            ColumnType::Json => "String",
            ColumnType::Uuid => "UUID",
            ColumnType::Decimal => "Decimal(18, 4)",
        }
    }
}

// =============================================================================
// Rich Type System (Arrow-based with Semantic Metadata)
// =============================================================================

/// Convert an Arrow Schema to a JSON-serializable representation.
/// Uses Arrow's field metadata format for proper round-tripping.
fn schema_to_json(schema: &arrow::datatypes::Schema) -> serde_json::Value {
    use serde_json::json;

    let fields: Vec<serde_json::Value> = schema.fields().iter().map(|f| field_to_json(f)).collect();

    json!({ "fields": fields })
}

/// Convert an Arrow Field to a stable JSON representation.
///
/// Used by `TypedColumn::serialize_arrow_type` and the derived table module
/// for schema storage that survives Arrow library upgrades.
pub fn field_to_json(field: &arrow::datatypes::Field) -> serde_json::Value {
    use arrow::datatypes::DataType::*;
    use serde_json::json;

    let type_json = match field.data_type() {
        Boolean => json!({"type": "bool"}),
        Int8 => json!({"type": "int8"}),
        Int16 => json!({"type": "int16"}),
        Int32 => json!({"type": "int32"}),
        Int64 => json!({"type": "int64"}),
        UInt8 => json!({"type": "uint8"}),
        UInt16 => json!({"type": "uint16"}),
        UInt32 => json!({"type": "uint32"}),
        UInt64 => json!({"type": "uint64"}),
        Float16 => json!({"type": "float16"}),
        Float32 => json!({"type": "float32"}),
        Float64 => json!({"type": "float64"}),
        Utf8 => json!({"type": "utf8"}),
        LargeUtf8 => json!({"type": "largeutf8"}),
        Binary => json!({"type": "binary"}),
        LargeBinary => json!({"type": "largebinary"}),
        Date32 => json!({"type": "date32"}),
        Date64 => json!({"type": "date64"}),
        Time32(u) => json!({"type": "time32", "unit": format!("{:?}", u)}),
        Time64(u) => json!({"type": "time64", "unit": format!("{:?}", u)}),
        Timestamp(u, tz) => json!({
            "type": "timestamp",
            "unit": format!("{:?}", u),
            "tz": tz
        }),
        Duration(u) => json!({"type": "duration", "unit": format!("{:?}", u)}),
        Decimal128(p, s) => json!({"type": "decimal128", "precision": p, "scale": s}),
        Decimal256(p, s) => json!({"type": "decimal256", "precision": p, "scale": s}),
        FixedSizeBinary(n) => json!({"type": "fixedsizebinary", "size": n}),
        List(f) => json!({"type": "list", "item": field_to_json(f)}),
        LargeList(f) => json!({"type": "largelist", "item": field_to_json(f)}),
        FixedSizeList(f, n) => {
            json!({"type": "fixedsizelist", "item": field_to_json(f), "size": n})
        }
        Map(f, sorted) => json!({"type": "map", "entries": field_to_json(f), "sorted": sorted}),
        Struct(fields) => {
            let field_jsons: Vec<_> = fields.iter().map(|f| field_to_json(f)).collect();
            json!({"type": "struct", "fields": field_jsons})
        }
        _ => json!({"type": "utf8"}), // Fallback for unsupported types
    };

    json!({
        "name": field.name(),
        "nullable": field.is_nullable(),
        "dataType": type_json
    })
}

/// Parse a JSON representation back to an Arrow Schema.
fn json_to_schema(json: &serde_json::Value) -> Option<arrow::datatypes::Schema> {
    let fields = json.get("fields")?.as_array()?;
    let arrow_fields: Vec<arrow::datatypes::Field> =
        fields.iter().filter_map(|f| json_to_field(f)).collect();
    Some(arrow::datatypes::Schema::new(arrow_fields))
}

/// Parse a JSON field back to an Arrow Field.
fn json_to_field(json: &serde_json::Value) -> Option<arrow::datatypes::Field> {
    use arrow::datatypes::{DataType, Field, TimeUnit};
    use std::sync::Arc;

    let name = json.get("name")?.as_str()?;
    let nullable = json.get("nullable")?.as_bool().unwrap_or(true);
    let dt_json = json.get("dataType")?;
    let type_name = dt_json.get("type")?.as_str()?;

    let parse_time_unit = |s: &str| -> TimeUnit {
        match s {
            "Second" => TimeUnit::Second,
            "Millisecond" => TimeUnit::Millisecond,
            "Microsecond" => TimeUnit::Microsecond,
            "Nanosecond" => TimeUnit::Nanosecond,
            _ => TimeUnit::Microsecond,
        }
    };

    let data_type = match type_name {
        "bool" => DataType::Boolean,
        "int8" => DataType::Int8,
        "int16" => DataType::Int16,
        "int32" => DataType::Int32,
        "int64" => DataType::Int64,
        "uint8" => DataType::UInt8,
        "uint16" => DataType::UInt16,
        "uint32" => DataType::UInt32,
        "uint64" => DataType::UInt64,
        "float16" => DataType::Float16,
        "float32" => DataType::Float32,
        "float64" => DataType::Float64,
        "utf8" => DataType::Utf8,
        "largeutf8" => DataType::LargeUtf8,
        "binary" => DataType::Binary,
        "largebinary" => DataType::LargeBinary,
        "date32" => DataType::Date32,
        "date64" => DataType::Date64,
        "time32" => {
            let unit = dt_json
                .get("unit")
                .and_then(|u| u.as_str())
                .unwrap_or("Millisecond");
            DataType::Time32(parse_time_unit(unit))
        }
        "time64" => {
            let unit = dt_json
                .get("unit")
                .and_then(|u| u.as_str())
                .unwrap_or("Microsecond");
            DataType::Time64(parse_time_unit(unit))
        }
        "timestamp" => {
            let unit = dt_json
                .get("unit")
                .and_then(|u| u.as_str())
                .unwrap_or("Microsecond");
            let tz = dt_json.get("tz").and_then(|t| t.as_str()).map(|s| s.into());
            DataType::Timestamp(parse_time_unit(unit), tz)
        }
        "duration" => {
            let unit = dt_json
                .get("unit")
                .and_then(|u| u.as_str())
                .unwrap_or("Microsecond");
            DataType::Duration(parse_time_unit(unit))
        }
        "decimal128" => {
            let precision = dt_json
                .get("precision")
                .and_then(|p| p.as_u64())
                .unwrap_or(38) as u8;
            let scale = dt_json.get("scale").and_then(|s| s.as_i64()).unwrap_or(0) as i8;
            DataType::Decimal128(precision, scale)
        }
        "decimal256" => {
            let precision = dt_json
                .get("precision")
                .and_then(|p| p.as_u64())
                .unwrap_or(76) as u8;
            let scale = dt_json.get("scale").and_then(|s| s.as_i64()).unwrap_or(0) as i8;
            DataType::Decimal256(precision, scale)
        }
        "fixedsizebinary" => {
            let size = dt_json.get("size").and_then(|s| s.as_i64()).unwrap_or(16) as i32;
            DataType::FixedSizeBinary(size)
        }
        "list" => {
            let item_json = dt_json.get("item")?;
            let item_field = json_to_field(item_json)?;
            DataType::List(Arc::new(item_field))
        }
        "largelist" => {
            let item_json = dt_json.get("item")?;
            let item_field = json_to_field(item_json)?;
            DataType::LargeList(Arc::new(item_field))
        }
        "fixedsizelist" => {
            let item_json = dt_json.get("item")?;
            let item_field = json_to_field(item_json)?;
            let size = dt_json.get("size").and_then(|s| s.as_i64()).unwrap_or(1) as i32;
            DataType::FixedSizeList(Arc::new(item_field), size)
        }
        "struct" => {
            let fields_json = dt_json.get("fields")?.as_array()?;
            let fields: Vec<_> = fields_json
                .iter()
                .filter_map(|f| json_to_field(f))
                .collect();
            DataType::Struct(fields.into())
        }
        "map" => {
            let entries_json = dt_json.get("entries")?;
            let entries_field = json_to_field(entries_json)?;
            let sorted = dt_json
                .get("sorted")
                .and_then(|s| s.as_bool())
                .unwrap_or(false);
            DataType::Map(Arc::new(entries_field), sorted)
        }
        _ => DataType::Utf8, // Default fallback
    };

    Some(Field::new(name, data_type, nullable))
}

/// Semantic type hints that provide domain-specific meaning.
/// These are layered on top of Arrow's structural types to capture
/// semantic information that cannot be expressed in the type system alone.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticType {
    /// Monetary value with currency and representation info.
    Money {
        /// ISO 4217 currency code (e.g., "USD", "EUR").
        /// None if currency is unknown or mixed.
        currency: Option<String>,
        /// If true, the underlying integer represents cents/smallest unit.
        /// If false, the value is in whole currency units (e.g., dollars).
        in_cents: bool,
    },

    /// Identifier field - should not be aggregated, used for JOINs.
    /// Examples: user_id, order_id, stripe customer id.
    Identifier,

    /// Percentage value with scale information.
    Percentage {
        /// The scale of the percentage.
        scale: PercentageScale,
    },

    /// Duration/interval with unit information.
    Duration {
        /// The unit of the duration value.
        unit: DurationUnit,
    },

    /// Low-cardinality categorical value (good for GROUP BY, indexing).
    /// Examples: status, country_code, payment_method.
    Categorical,

    /// Email address (for validation, PII handling).
    Email,

    /// URL (for validation, link handling).
    Url,

    /// Phone number (for validation, PII handling).
    PhoneNumber,

    /// Timestamp with precision and timezone metadata.
    ///
    /// Used to track source-specific timestamp characteristics for proper
    /// normalization during cross-source queries.
    Timestamp {
        /// Original precision from source (e.g., Stripe uses seconds).
        precision: TimestampPrecision,
        /// Source timezone for naive timestamps.
        ///
        /// - Auto-detected sources (Stripe): always "UTC"
        /// - User-configurable sources (CSV, PostgreSQL `timestamp`):
        ///   defaults to "UTC", editable during source registration
        source_timezone: String,
    },
}

/// Scale for percentage values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PercentageScale {
    /// 0.0 to 1.0 representation (0.5 = 50%).
    ZeroToOne,
    /// 0 to 100 representation (50 = 50%).
    ZeroToHundred,
}

/// Unit for duration values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurationUnit {
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
    Minutes,
    Hours,
    Days,
}

/// Timestamp precision for cross-source compatibility.
///
/// Different sources store timestamps with varying precisions:
/// - Stripe API: Unix seconds
/// - PostgreSQL: Microseconds
/// - Parquet: Can be any of these
/// - ClickHouse DateTime64(6): Microseconds
///
/// We standardize internally on microseconds (matching ClickHouse).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampPrecision {
    /// Unix epoch seconds (e.g., Stripe API).
    Seconds,
    /// Milliseconds since epoch (e.g., JavaScript Date).
    Milliseconds,
    /// Microseconds since epoch (internal standard, ClickHouse DateTime64(6)).
    Microseconds,
    /// Nanoseconds since epoch (e.g., some Parquet files).
    Nanoseconds,
}

impl TimestampPrecision {
    /// Convert a raw i64 value from this precision to microseconds.
    ///
    /// # Example
    /// ```ignore
    /// let stripe_ts = 1705312800i64; // Unix seconds
    /// let micros = TimestampPrecision::Seconds.to_microseconds(stripe_ts);
    /// assert_eq!(micros, 1705312800_000_000);
    /// ```
    pub fn to_microseconds(&self, value: i64) -> i64 {
        match self {
            TimestampPrecision::Seconds => value.saturating_mul(1_000_000),
            TimestampPrecision::Milliseconds => value.saturating_mul(1_000),
            TimestampPrecision::Microseconds => value,
            TimestampPrecision::Nanoseconds => value / 1_000,
        }
    }

    /// Convert a microseconds value to this precision.
    ///
    /// Note: Converting to lower precision loses data.
    pub fn from_microseconds(&self, micros: i64) -> i64 {
        match self {
            TimestampPrecision::Seconds => micros / 1_000_000,
            TimestampPrecision::Milliseconds => micros / 1_000,
            TimestampPrecision::Microseconds => micros,
            TimestampPrecision::Nanoseconds => micros.saturating_mul(1_000),
        }
    }

    /// Get the Arrow TimeUnit corresponding to this precision.
    pub fn to_arrow_time_unit(&self) -> arrow::datatypes::TimeUnit {
        match self {
            TimestampPrecision::Seconds => arrow::datatypes::TimeUnit::Second,
            TimestampPrecision::Milliseconds => arrow::datatypes::TimeUnit::Millisecond,
            TimestampPrecision::Microseconds => arrow::datatypes::TimeUnit::Microsecond,
            TimestampPrecision::Nanoseconds => arrow::datatypes::TimeUnit::Nanosecond,
        }
    }

    /// Create from Arrow TimeUnit.
    pub fn from_arrow_time_unit(unit: arrow::datatypes::TimeUnit) -> Self {
        match unit {
            arrow::datatypes::TimeUnit::Second => TimestampPrecision::Seconds,
            arrow::datatypes::TimeUnit::Millisecond => TimestampPrecision::Milliseconds,
            arrow::datatypes::TimeUnit::Microsecond => TimestampPrecision::Microseconds,
            arrow::datatypes::TimeUnit::Nanosecond => TimestampPrecision::Nanoseconds,
        }
    }
}

impl Default for TimestampPrecision {
    /// Default to microseconds (our internal standard).
    fn default() -> Self {
        TimestampPrecision::Microseconds
    }
}

// =============================================================================
// NULL Semantics
// =============================================================================

/// How a source represents NULL values.
///
/// Different data sources have different conventions for representing missing or null values.
/// This struct allows configuring how Reiver interprets these values uniformly.
///
/// # Core Semantics
///
/// Reiver uses consistent NULL semantics across all sources:
/// - **Empty string `""`** = Valid string with empty value (NOT NULL)
/// - **Missing field/object** = NULL
///
/// Users who need legacy behavior (treating empty strings as NULL) can opt-in
/// via the `treat_empty_as_null` flag.
///
/// # Example
///
/// ```ignore
/// // Default: empty string is a valid value
/// let default_semantics = NullSemantics::default();
/// assert!(!default_semantics.treat_empty_as_null);
///
/// // Legacy mode: treat empty strings as NULL
/// let legacy_semantics = NullSemantics::legacy();
/// assert!(legacy_semantics.treat_empty_as_null);
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct NullSemantics {
    /// String literals that should be treated as NULL.
    ///
    /// Default: `["NULL", "null"]` - does NOT include empty string.
    /// Common additions: `"N/A"`, `"NA"`, `"n/a"`, `"(null)"`, `"-"`.
    #[serde(default = "default_null_values")]
    pub null_values: Vec<String>,

    /// Whether empty strings are treated as NULL.
    ///
    /// Default: `false` - empty string is a valid value, not NULL.
    /// Set to `true` only for legacy data where empty means NULL.
    #[serde(default)]
    pub treat_empty_as_null: bool,

    /// Pre-computed set for O(1) lookups, rebuilt from `null_values`.
    #[serde(skip)]
    null_set: HashSet<String>,
}

impl<'de> Deserialize<'de> for NullSemantics {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(default = "default_null_values")]
            null_values: Vec<String>,
            #[serde(default)]
            treat_empty_as_null: bool,
        }
        let raw = Raw::deserialize(deserializer)?;
        let null_set = raw.null_values.iter().cloned().collect();
        Ok(NullSemantics {
            null_values: raw.null_values,
            treat_empty_as_null: raw.treat_empty_as_null,
            null_set,
        })
    }
}

impl PartialEq for NullSemantics {
    fn eq(&self, other: &Self) -> bool {
        self.null_values == other.null_values
            && self.treat_empty_as_null == other.treat_empty_as_null
    }
}

impl Eq for NullSemantics {}

/// Default NULL value literals (does NOT include empty string).
fn default_null_values() -> Vec<String> {
    vec!["NULL".to_string(), "null".to_string()]
}

impl Default for NullSemantics {
    fn default() -> Self {
        let null_values = default_null_values();
        let null_set = null_values.iter().cloned().collect();
        Self {
            null_values,
            treat_empty_as_null: false,
            null_set,
        }
    }
}

impl NullSemantics {
    /// Create NULL semantics with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create legacy NULL semantics where empty strings are treated as NULL.
    ///
    /// This is for backwards compatibility with data sources that use
    /// empty strings to represent NULL values.
    pub fn legacy() -> Self {
        let null_values = vec![
            String::new(), // Empty string
            "NULL".to_string(),
            "null".to_string(),
        ];
        let null_set = null_values.iter().cloned().collect();
        Self {
            null_values,
            treat_empty_as_null: true,
            null_set,
        }
    }

    /// Create NULL semantics with custom null value literals.
    pub fn with_null_values(null_values: Vec<String>) -> Self {
        let null_set = null_values.iter().cloned().collect();
        Self {
            null_values,
            treat_empty_as_null: false,
            null_set,
        }
    }

    /// Set whether empty strings should be treated as NULL.
    pub fn with_treat_empty_as_null(mut self, treat_empty: bool) -> Self {
        self.treat_empty_as_null = treat_empty;
        if treat_empty && !self.null_values.contains(&String::new()) {
            self.null_values.insert(0, String::new());
            self.null_set.insert(String::new());
        } else if !treat_empty {
            self.null_values.retain(|v| !v.is_empty());
            self.null_set.remove(&String::new());
        }
        self
    }

    /// Add additional null value literals.
    pub fn with_additional_nulls(mut self, values: &[&str]) -> Self {
        for value in values {
            let s = value.to_string();
            if self.null_set.insert(s.clone()) {
                self.null_values.push(s);
            }
        }
        self
    }

    /// Check if a string value should be treated as NULL.
    pub fn is_null(&self, value: &str) -> bool {
        if self.treat_empty_as_null && value.is_empty() {
            return true;
        }
        self.null_set.contains(value)
    }

    /// Get the null values list for use with Arrow's CSV reader.
    ///
    /// If `treat_empty_as_null` is true, ensures empty string is in the list.
    pub fn null_values_for_reader(&self) -> Vec<String> {
        let mut values = self.null_values.clone();
        if self.treat_empty_as_null && !values.contains(&String::new()) {
            values.insert(0, String::new());
        }
        values
    }
}

/// A column with full type information.
///
/// Uses Arrow DataType as the canonical structural type,
/// with optional semantic metadata for domain-specific meaning.
/// This provides richer type information than the simple `ColumnType` enum,
/// preserving source type details for accurate cross-source operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypedColumn {
    /// Column name.
    pub name: String,

    /// Arrow data type stored as JSON schema for proper serialization.
    /// Use `arrow_data_type()` to get the actual Arrow DataType.
    data_type_json: String,

    /// Whether the column can contain NULL values.
    pub nullable: bool,

    /// Original type name from the source (for error messages).
    /// Examples: "DECIMAL(18,4)", "bigint", "varchar(255)", "Int64 (cents)".
    pub source_type_name: String,

    /// Source this column came from.
    /// Examples: "postgres", "mysql", "stripe", "csv".
    pub source_name: String,

    /// Optional semantic meaning layered on top of the structural type.
    pub semantic: Option<SemanticType>,

    /// Optional human-readable description.
    pub description: Option<String>,
}

impl TypedColumn {
    /// Create a new TypedColumn from an Arrow DataType.
    pub fn new(
        name: impl Into<String>,
        data_type: &arrow::datatypes::DataType,
        nullable: bool,
        source_type_name: impl Into<String>,
        source_name: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            data_type_json: Self::serialize_arrow_type(data_type),
            nullable,
            source_type_name: source_type_name.into(),
            source_name: source_name.into(),
            semantic: None,
            description: None,
        }
    }

    /// Serialize an Arrow DataType to JSON using Arrow's schema format.
    fn serialize_arrow_type(data_type: &arrow::datatypes::DataType) -> String {
        use arrow::datatypes::{Field, Schema};
        // Create a schema with a single field to use Arrow's JSON serialization
        let field = Field::new("_type", data_type.clone(), true);
        let schema = Schema::new(vec![field]);
        // Arrow's schema can be serialized to JSON via its IPC format metadata
        serde_json::to_string(&schema_to_json(&schema)).unwrap_or_else(|_| "Utf8".to_string())
    }

    /// Deserialize an Arrow DataType from JSON.
    fn deserialize_arrow_type(json: &str) -> Option<arrow::datatypes::DataType> {
        let schema_json: serde_json::Value = serde_json::from_str(json).ok()?;
        let schema = json_to_schema(&schema_json)?;
        schema.fields().first().map(|f| f.data_type().clone())
    }

    /// Get the Arrow DataType for this column.
    ///
    /// Returns None if the stored type cannot be parsed (should not happen
    /// for properly constructed TypedColumns).
    pub fn arrow_data_type(&self) -> Option<arrow::datatypes::DataType> {
        Self::deserialize_arrow_type(&self.data_type_json)
    }

    /// Get the Arrow DataType, falling back to Utf8 if parsing fails.
    pub fn arrow_data_type_or_string(&self) -> arrow::datatypes::DataType {
        self.arrow_data_type()
            .unwrap_or(arrow::datatypes::DataType::Utf8)
    }

    /// Create a TypedColumn with semantic type.
    pub fn with_semantic(mut self, semantic: SemanticType) -> Self {
        self.semantic = Some(semantic);
        self
    }

    /// Create a TypedColumn with description.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Check if this column has a specific semantic type.
    pub fn is_money(&self) -> bool {
        matches!(self.semantic, Some(SemanticType::Money { .. }))
    }

    /// Check if this column is an identifier.
    pub fn is_identifier(&self) -> bool {
        matches!(self.semantic, Some(SemanticType::Identifier))
    }

    /// Check if this column is categorical.
    pub fn is_categorical(&self) -> bool {
        matches!(self.semantic, Some(SemanticType::Categorical))
    }

    /// Get money details if this is a money column.
    pub fn money_details(&self) -> Option<(Option<&str>, bool)> {
        match &self.semantic {
            Some(SemanticType::Money { currency, in_cents }) => {
                Some((currency.as_deref(), *in_cents))
            }
            _ => None,
        }
    }

    /// Check if this column has timestamp semantic type.
    pub fn is_timestamp_semantic(&self) -> bool {
        matches!(self.semantic, Some(SemanticType::Timestamp { .. }))
    }

    /// Get timestamp details if this has timestamp semantic type.
    pub fn timestamp_details(&self) -> Option<(TimestampPrecision, &str)> {
        match &self.semantic {
            Some(SemanticType::Timestamp {
                precision,
                source_timezone,
            }) => Some((*precision, source_timezone.as_str())),
            _ => None,
        }
    }

    /// Check if the Arrow type is a timestamp type (regardless of semantic type).
    pub fn is_timestamp_arrow_type(&self) -> bool {
        matches!(
            self.arrow_data_type_or_string(),
            arrow::datatypes::DataType::Timestamp(_, _)
        )
    }

    /// Check if this column represents a UUID type.
    ///
    /// Detects UUID columns via Arrow type (`FixedSizeBinary(16)`) or by
    /// matching known source type names from PostgreSQL, ClickHouse, and
    /// SQL Server.
    pub fn is_uuid(&self) -> bool {
        use arrow::datatypes::DataType;
        let arrow = self.arrow_data_type_or_string();
        if matches!(arrow, DataType::FixedSizeBinary(16)) {
            return true;
        }
        if matches!(arrow, DataType::Utf8) {
            match self.source_type_name.to_lowercase().as_str() {
                "uuid" | "uniqueidentifier" => return true,
                _ => {}
            }
        }
        false
    }

    /// Check if the Arrow type is a date type.
    pub fn is_date_arrow_type(&self) -> bool {
        matches!(
            self.arrow_data_type_or_string(),
            arrow::datatypes::DataType::Date32 | arrow::datatypes::DataType::Date64
        )
    }
}

/// Schema with rich type information.
/// Extends TableSchema with TypedColumn for full type fidelity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypedSchema {
    /// Table name.
    pub table_name: String,

    /// Columns with full type information.
    pub columns: Vec<TypedColumn>,

    /// Source identifier.
    pub source_name: String,

    /// When this schema was last updated.
    pub updated_at: Option<DateTime<Utc>>,
}

impl TypedSchema {
    /// Create a new TypedSchema.
    pub fn new(table_name: impl Into<String>, source_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            columns: Vec::new(),
            source_name: source_name.into(),
            updated_at: Some(Utc::now()),
        }
    }

    /// Add a column to the schema.
    pub fn with_column(mut self, column: TypedColumn) -> Self {
        self.columns.push(column);
        self
    }

    /// Get a column by name.
    pub fn get_column(&self, name: &str) -> Option<&TypedColumn> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Get all money columns.
    pub fn money_columns(&self) -> Vec<&TypedColumn> {
        self.columns.iter().filter(|c| c.is_money()).collect()
    }

    /// Get all identifier columns.
    pub fn identifier_columns(&self) -> Vec<&TypedColumn> {
        self.columns.iter().filter(|c| c.is_identifier()).collect()
    }
}

// =============================================================================
// Type Coercion Engine
// =============================================================================

/// Result of attempting to coerce two types together.
#[derive(Debug, Clone, PartialEq)]
pub enum CoercionResult {
    /// Types are identical, no coercion needed.
    Same,

    /// Can auto-coerce to target type (safe, transparent to user).
    AutoCoerce {
        /// The target Arrow type to coerce to.
        target: arrow::datatypes::DataType,
        /// Optional warning message for potential precision loss.
        warning: Option<String>,
    },

    /// Requires explicit CAST or conversion function.
    RequiresExplicit {
        /// Reason why explicit conversion is required.
        reason: String,
        /// Suggested SQL to fix the issue.
        suggestion: String,
    },

    /// Types are incompatible, cannot be coerced.
    Incompatible {
        /// Reason for incompatibility.
        reason: String,
    },
}

impl CoercionResult {
    /// Check if this result allows automatic coercion.
    pub fn is_auto(&self) -> bool {
        matches!(
            self,
            CoercionResult::Same | CoercionResult::AutoCoerce { .. }
        )
    }

    /// Check if this result requires explicit action.
    pub fn requires_explicit(&self) -> bool {
        matches!(self, CoercionResult::RequiresExplicit { .. })
    }

    /// Check if this result is an error.
    pub fn is_error(&self) -> bool {
        matches!(self, CoercionResult::Incompatible { .. })
    }
}

/// Check if two Arrow types can be coerced together.
///
/// This is the main entry point for type coercion checks.
/// It first checks for semantic conflicts, then structural type compatibility.
///
/// # Arguments
/// * `left` - The left Arrow DataType
/// * `left_semantic` - Optional semantic type for the left operand
/// * `right` - The right Arrow DataType
/// * `right_semantic` - Optional semantic type for the right operand
///
/// # Returns
/// A `CoercionResult` indicating how (or if) the types can be combined.
pub fn coerce_types(
    left: &arrow::datatypes::DataType,
    left_semantic: Option<&SemanticType>,
    right: &arrow::datatypes::DataType,
    right_semantic: Option<&SemanticType>,
) -> CoercionResult {
    // Check semantic conflicts first (Money in cents vs dollars, etc.)
    if let (Some(left_sem), Some(right_sem)) = (left_semantic, right_semantic) {
        if let Some(conflict) = check_semantic_conflict(left_sem, right_sem) {
            return conflict;
        }
    }

    // Then check structural type compatibility
    coerce_arrow_types(left, right)
}

/// Check for semantic type conflicts.
fn check_semantic_conflict(left: &SemanticType, right: &SemanticType) -> Option<CoercionResult> {
    match (left, right) {
        // Money with different cent/unit representation
        (
            SemanticType::Money { in_cents: true, .. },
            SemanticType::Money {
                in_cents: false, ..
            },
        )
        | (
            SemanticType::Money {
                in_cents: false, ..
            },
            SemanticType::Money { in_cents: true, .. },
        ) => Some(CoercionResult::RequiresExplicit {
            reason: "Money types have different representations (cents vs units)".into(),
            suggestion: "Use cents_to_dollars() or dollars_to_cents() to convert".into(),
        }),

        // Different percentage scales
        (SemanticType::Percentage { scale: s1 }, SemanticType::Percentage { scale: s2 })
            if s1 != s2 =>
        {
            Some(CoercionResult::RequiresExplicit {
                reason: "Percentage scales differ (0-1 vs 0-100)".into(),
                suggestion: "Multiply or divide by 100 to align scales".into(),
            })
        }

        // Different duration units
        (SemanticType::Duration { unit: u1 }, SemanticType::Duration { unit: u2 }) if u1 != u2 => {
            Some(CoercionResult::RequiresExplicit {
                reason: format!("Duration units differ ({:?} vs {:?})", u1, u2),
                suggestion: "Convert to the same duration unit before comparing".into(),
            })
        }

        // Different timestamp timezones (both have explicit but different timezones)
        (
            SemanticType::Timestamp {
                source_timezone: tz1,
                ..
            },
            SemanticType::Timestamp {
                source_timezone: tz2,
                ..
            },
        ) if tz1 != tz2 && tz1 != "UTC" && tz2 != "UTC" => Some(CoercionResult::RequiresExplicit {
            reason: format!("Timestamps have different timezones ({} vs {})", tz1, tz2),
            suggestion: "Convert both to UTC before comparing".into(),
        }),

        // Timestamp precision differences are auto-coerced (not a conflict)
        // The precision normalization happens at read time
        _ => None, // No conflict
    }
}

/// Coerce Arrow types based on structural compatibility.
fn coerce_arrow_types(
    left: &arrow::datatypes::DataType,
    right: &arrow::datatypes::DataType,
) -> CoercionResult {
    use arrow::datatypes::DataType::*;
    // Same type = OK
    if left == right {
        return CoercionResult::Same;
    }

    match (left, right) {
        // Integer widening (always safe)
        (Int8, Int16 | Int32 | Int64) | (Int16, Int32 | Int64) | (Int32, Int64) => {
            CoercionResult::AutoCoerce {
                target: right.clone(),
                warning: None,
            }
        }

        // Reverse direction - narrowing requires explicit cast
        (Int16 | Int32 | Int64, Int8) | (Int32 | Int64, Int16) | (Int64, Int32) => {
            CoercionResult::RequiresExplicit {
                reason: "Integer narrowing may lose data".into(),
                suggestion: format!("Use CAST(column AS {:?}) if you're sure values fit", right),
            }
        }

        // Unsigned to signed widening (safe when target is large enough)
        (UInt8, Int16 | Int32 | Int64) | (UInt16, Int32 | Int64) | (UInt32, Int64) => {
            CoercionResult::AutoCoerce {
                target: right.clone(),
                warning: None,
            }
        }

        // Unsigned widening
        (UInt8, UInt16 | UInt32 | UInt64) | (UInt16, UInt32 | UInt64) | (UInt32, UInt64) => {
            CoercionResult::AutoCoerce {
                target: right.clone(),
                warning: None,
            }
        }

        // Float widening
        (Float32, Float64) => CoercionResult::AutoCoerce {
            target: Float64,
            warning: None,
        },

        // Float narrowing requires explicit cast
        (Float64, Float32) => CoercionResult::RequiresExplicit {
            reason: "Float narrowing may lose precision".into(),
            suggestion: "Use CAST(column AS FLOAT) if precision loss is acceptable".into(),
        },

        // Integer to float (may lose precision for large values)
        (Int32 | Int64, Float64) => CoercionResult::AutoCoerce {
            target: Float64,
            warning: Some("Large integers may lose precision when converted to Float64".into()),
        },

        // Float to integer (loses fractional part)
        (Float64, Int32 | Int64) => CoercionResult::RequiresExplicit {
            reason: "Float to integer conversion loses fractional part".into(),
            suggestion:
                "Use CAST(column AS Int32/Int64) or FLOOR()/CEIL() if truncation is intended".into(),
        },

        (Float32, Int32 | Int64) => CoercionResult::RequiresExplicit {
            reason: "Float to integer conversion loses fractional part".into(),
            suggestion:
                "Use CAST(column AS Int32/Int64) or FLOOR()/CEIL() if truncation is intended".into(),
        },

        // Timestamp precision alignment
        // Timestamp without timezone + with timezone (must come before the
        // general Timestamp arm to avoid being shadowed by irrefutable bindings)
        (Timestamp(u1, None), Timestamp(u2, Some(tz)))
        | (Timestamp(u2, Some(tz)), Timestamp(u1, None)) => {
            let target_unit = std::cmp::max(*u1, *u2);
            CoercionResult::AutoCoerce {
                target: Timestamp(target_unit, Some(tz.clone())),
                warning: Some("Timestamp without timezone interpreted as UTC".into()),
            }
        }

        (Timestamp(u1, tz1), Timestamp(u2, tz2)) => {
            let target_unit = std::cmp::max(*u1, *u2);
            let target_tz = tz1.clone().or(tz2.clone());

            let warning = if u1 != u2 {
                Some(format!(
                    "Timestamp precision mismatch ({:?} vs {:?}), aligning to {:?}",
                    u1, u2, target_unit
                ))
            } else {
                None
            };

            CoercionResult::AutoCoerce {
                target: Timestamp(target_unit, target_tz),
                warning,
            }
        }

        // Date to Timestamp (safe, date becomes midnight)
        (Date32 | Date64, Timestamp(unit, tz)) => CoercionResult::AutoCoerce {
            target: Timestamp(*unit, tz.clone()),
            warning: Some("Date interpreted as midnight UTC".into()),
        },

        // Timestamp to Date (loses time component)
        (Timestamp(..), Date32 | Date64) => CoercionResult::RequiresExplicit {
            reason: "Converting timestamp to date loses time component".into(),
            suggestion: "Use CAST(column AS DATE) or date_trunc()".into(),
        },

        // Decimal precision alignment
        (Decimal128(p1, s1), Decimal128(p2, s2)) => {
            let target_precision = std::cmp::max(*p1, *p2);
            let target_scale = std::cmp::max(*s1, *s2);

            let warning = if p1 != p2 || s1 != s2 {
                Some(format!(
                    "Decimal precision mismatch ({},{}) vs ({},{})",
                    p1, s1, p2, s2
                ))
            } else {
                None
            };

            CoercionResult::AutoCoerce {
                target: Decimal128(target_precision, target_scale),
                warning,
            }
        }

        // Decimal to Float (may lose precision)
        (Decimal128(..), Float64) => CoercionResult::AutoCoerce {
            target: Float64,
            warning: Some("Decimal to Float64 may lose precision".into()),
        },

        // Float to Decimal (may lose precision)
        (Float64, Decimal128(p, s)) => CoercionResult::AutoCoerce {
            target: Decimal128(*p, *s),
            warning: Some("Float64 to Decimal may lose precision".into()),
        },

        // String conversions require explicit CAST
        (Utf8 | LargeUtf8, _) | (_, Utf8 | LargeUtf8) => {
            // Exception: Utf8 to LargeUtf8 is safe
            if matches!((left, right), (Utf8, LargeUtf8) | (LargeUtf8, Utf8)) {
                return CoercionResult::AutoCoerce {
                    target: LargeUtf8,
                    warning: None,
                };
            }
            CoercionResult::RequiresExplicit {
                reason: "String conversion may lose data or fail".into(),
                suggestion: "Use explicit CAST(column AS type)".into(),
            }
        }

        // Binary types
        (Binary, LargeBinary) | (LargeBinary, Binary) => CoercionResult::AutoCoerce {
            target: LargeBinary,
            warning: None,
        },

        // Boolean to integer (0/1)
        (Boolean, Int8 | Int16 | Int32 | Int64) => CoercionResult::AutoCoerce {
            target: right.clone(),
            warning: Some("Boolean converted to 0/1".into()),
        },

        // Integer to boolean
        (Int8 | Int16 | Int32 | Int64, Boolean) => CoercionResult::RequiresExplicit {
            reason: "Integer to boolean is ambiguous (what is truthy?)".into(),
            suggestion: "Use column != 0 or CAST(column AS BOOLEAN)".into(),
        },

        // Time types - widen to Time64 with the finer precision
        (Time32(u1), Time64(u2)) | (Time64(u2), Time32(u1)) => {
            let target_unit = std::cmp::max(*u1, *u2);
            CoercionResult::AutoCoerce {
                target: Time64(target_unit),
                warning: None,
            }
        }

        // Duration types
        (Duration(u1), Duration(u2)) => {
            let target_unit = std::cmp::max(*u1, *u2);
            CoercionResult::AutoCoerce {
                target: Duration(target_unit),
                warning: None,
            }
        }

        // List types - check element compatibility
        (List(f1), List(f2)) | (LargeList(f1), LargeList(f2)) => {
            match coerce_arrow_types(f1.data_type(), f2.data_type()) {
                CoercionResult::Same => CoercionResult::Same,
                CoercionResult::AutoCoerce { target, warning } => {
                    let target_field = arrow::datatypes::Field::new(
                        f1.name(),
                        target,
                        f1.is_nullable() || f2.is_nullable(),
                    );
                    CoercionResult::AutoCoerce {
                        target: List(std::sync::Arc::new(target_field)),
                        warning,
                    }
                }
                other => other,
            }
        }

        // Default: incompatible
        _ => CoercionResult::Incompatible {
            reason: format!("Cannot coerce {:?} to {:?}", left, right),
        },
    }
}

/// Get the common supertype for two Arrow types.
/// Returns the type that both can be safely coerced to.
pub fn common_supertype(
    left: &arrow::datatypes::DataType,
    right: &arrow::datatypes::DataType,
) -> Option<arrow::datatypes::DataType> {
    match coerce_arrow_types(left, right) {
        CoercionResult::Same => Some(left.clone()),
        CoercionResult::AutoCoerce { target, .. } => Some(target),
        _ => None,
    }
}

// =============================================================================
// Type Conversion Functions
// =============================================================================

/// Conversion function metadata for SQL registration.
#[derive(Debug, Clone)]
pub struct ConversionFunction {
    /// Function name as used in SQL (e.g., "cents_to_dollars")
    pub name: &'static str,
    /// Description for documentation
    pub description: &'static str,
    /// SQL expression template (use $1 for the input column)
    pub sql_template: &'static str,
    /// Source semantic type
    pub from_semantic: Option<SemanticType>,
    /// Target semantic type (None = removes semantic annotation)
    pub to_semantic: Option<SemanticType>,
}

/// All registered conversion functions.
///
/// Returns a cached reference to avoid allocating on every call.
pub fn registered_conversions() -> &'static [ConversionFunction] {
    use std::sync::OnceLock;

    static CONVERSIONS: OnceLock<Vec<ConversionFunction>> = OnceLock::new();

    CONVERSIONS.get_or_init(|| {
        vec![
            ConversionFunction {
                name: "cents_to_dollars",
                description: "Convert amount in cents to dollars (divide by 100)",
                sql_template: "CAST($1 AS DECIMAL(38,2)) / 100.0",
                from_semantic: Some(SemanticType::Money {
                    currency: None,
                    in_cents: true,
                }),
                to_semantic: Some(SemanticType::Money {
                    currency: None,
                    in_cents: false,
                }),
            },
            ConversionFunction {
                name: "dollars_to_cents",
                description: "Convert amount in dollars to cents (multiply by 100)",
                sql_template: "CAST(ROUND($1 * 100) AS BIGINT)",
                from_semantic: Some(SemanticType::Money {
                    currency: None,
                    in_cents: false,
                }),
                to_semantic: Some(SemanticType::Money {
                    currency: None,
                    in_cents: true,
                }),
            },
            ConversionFunction {
                name: "unix_seconds_to_timestamp",
                description: "Convert Unix timestamp (seconds) to TIMESTAMP",
                sql_template: "to_timestamp($1)",
                from_semantic: None,
                to_semantic: None,
            },
            ConversionFunction {
                name: "unix_millis_to_timestamp",
                description: "Convert Unix timestamp (milliseconds) to TIMESTAMP",
                sql_template: "to_timestamp($1 / 1000.0)",
                from_semantic: None,
                to_semantic: None,
            },
            ConversionFunction {
                name: "timestamp_to_unix_seconds",
                description: "Convert TIMESTAMP to Unix timestamp (seconds)",
                sql_template: "EXTRACT(EPOCH FROM $1)",
                from_semantic: None,
                to_semantic: None,
            },
            ConversionFunction {
                name: "percent_to_fraction",
                description: "Convert percentage (0-100) to fraction (0-1)",
                sql_template: "$1 / 100.0",
                from_semantic: Some(SemanticType::Percentage {
                    scale: PercentageScale::ZeroToHundred,
                }),
                to_semantic: Some(SemanticType::Percentage {
                    scale: PercentageScale::ZeroToOne,
                }),
            },
            ConversionFunction {
                name: "fraction_to_percent",
                description: "Convert fraction (0-1) to percentage (0-100)",
                sql_template: "$1 * 100.0",
                from_semantic: Some(SemanticType::Percentage {
                    scale: PercentageScale::ZeroToOne,
                }),
                to_semantic: Some(SemanticType::Percentage {
                    scale: PercentageScale::ZeroToHundred,
                }),
            },
            ConversionFunction {
                name: "seconds_to_milliseconds",
                description: "Convert duration in seconds to milliseconds",
                sql_template: "$1 * 1000",
                from_semantic: Some(SemanticType::Duration {
                    unit: DurationUnit::Seconds,
                }),
                to_semantic: Some(SemanticType::Duration {
                    unit: DurationUnit::Milliseconds,
                }),
            },
            ConversionFunction {
                name: "milliseconds_to_seconds",
                description: "Convert duration in milliseconds to seconds",
                sql_template: "$1 / 1000.0",
                from_semantic: Some(SemanticType::Duration {
                    unit: DurationUnit::Milliseconds,
                }),
                to_semantic: Some(SemanticType::Duration {
                    unit: DurationUnit::Seconds,
                }),
            },
        ]
    })
}

/// Get a conversion function by name.
pub fn get_conversion_function(name: &str) -> Option<&'static ConversionFunction> {
    registered_conversions().iter().find(|f| f.name == name)
}

/// Find a conversion function that matches the semantic type transformation.
pub fn find_conversion_for_semantics(
    from: &SemanticType,
    to: &SemanticType,
) -> Option<&'static ConversionFunction> {
    registered_conversions().iter().find(|f| {
        if let (Some(from_sem), Some(to_sem)) = (&f.from_semantic, &f.to_semantic) {
            semantic_types_compatible(from_sem, from) && semantic_types_compatible(to_sem, to)
        } else {
            false
        }
    })
}

/// Check if two semantic types are compatible (ignoring currency for money).
fn semantic_types_compatible(pattern: &SemanticType, actual: &SemanticType) -> bool {
    match (pattern, actual) {
        (
            SemanticType::Money {
                in_cents: c1,
                currency: _,
            },
            SemanticType::Money {
                in_cents: c2,
                currency: _,
            },
        ) => c1 == c2,
        (SemanticType::Percentage { scale: s1 }, SemanticType::Percentage { scale: s2 }) => {
            s1 == s2
        }
        (SemanticType::Duration { unit: u1 }, SemanticType::Duration { unit: u2 }) => u1 == u2,
        // Timestamps are compatible if precision matches (timezone is normalized at read time)
        (
            SemanticType::Timestamp { precision: p1, .. },
            SemanticType::Timestamp { precision: p2, .. },
        ) => p1 == p2,
        _ => pattern == actual,
    }
}

// =============================================================================
// Direct Conversion Helper Functions (for use in Rust code)
// =============================================================================

/// Convert cents to dollars.
#[inline]
pub fn cents_to_dollars(cents: i64) -> f64 {
    cents as f64 / 100.0
}

/// Convert dollars to cents.
#[inline]
pub fn dollars_to_cents(dollars: f64) -> i64 {
    (dollars * 100.0).round() as i64
}

/// Convert Unix seconds to microseconds.
#[inline]
pub fn unix_seconds_to_micros(seconds: i64) -> i64 {
    seconds.saturating_mul(1_000_000)
}

/// Convert Unix milliseconds to microseconds.
#[inline]
pub fn unix_millis_to_micros(millis: i64) -> i64 {
    millis.saturating_mul(1_000)
}

/// Convert fraction (0-1) to percentage (0-100).
#[inline]
pub fn fraction_to_percent(fraction: f64) -> f64 {
    fraction * 100.0
}

/// Convert percentage (0-100) to fraction (0-1).
#[inline]
pub fn percent_to_fraction(percent: f64) -> f64 {
    percent / 100.0
}

/// Status of a sync operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl std::fmt::Display for SyncStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncStatus::Pending => write!(f, "pending"),
            SyncStatus::Running => write!(f, "running"),
            SyncStatus::Completed => write!(f, "completed"),
            SyncStatus::Failed => write!(f, "failed"),
            SyncStatus::Cancelled => write!(f, "cancelled"),
        }
    }
}

impl std::str::FromStr for SyncStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "pending" => Ok(SyncStatus::Pending),
            "running" => Ok(SyncStatus::Running),
            "completed" => Ok(SyncStatus::Completed),
            "failed" => Ok(SyncStatus::Failed),
            "cancelled" => Ok(SyncStatus::Cancelled),
            _ => Err(format!("Unknown sync status: {}", s)),
        }
    }
}

/// A file-level skip index built during sync, ready to be persisted.
///
/// Carries the partition key, file path, FST index, and row count so the
/// caller can save it to the `warehouse_skip_indexes` table.
#[derive(Debug)]
pub struct InlineFileIndex {
    pub partition_key: String,
    pub file_path: String,
    pub index: crate::warehouse::indexes::skip_index::FileSkipIndex,
    pub row_count: u64,
}

/// Result of a sync operation.
#[derive(Debug, Serialize, Deserialize)]
pub struct SyncResult {
    pub rows_synced: u64,
    pub bytes_written: u64,
    pub files_created: u32,
    pub duration_ms: u64,
    /// File-level skip indexes built inline during sync.
    /// Skipped during serialization because FSTs are not JSON-friendly.
    #[serde(skip)]
    pub file_indexes: Vec<InlineFileIndex>,
}

impl Default for SyncResult {
    fn default() -> Self {
        Self {
            rows_synced: 0,
            bytes_written: 0,
            files_created: 0,
            duration_ms: 0,
            file_indexes: Vec::new(),
        }
    }
}

/// Sync interval options.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncInterval {
    /// Every 5 minutes
    Every5Min,
    /// Every 15 minutes
    Every15Min,
    /// Every hour
    Hourly,
    /// Every 6 hours
    Every6Hours,
    /// Once a day
    Daily,
    /// Once a week
    Weekly,
    /// Manual sync only
    Manual,
}

impl std::fmt::Display for SyncInterval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncInterval::Every5Min => write!(f, "5m"),
            SyncInterval::Every15Min => write!(f, "15m"),
            SyncInterval::Hourly => write!(f, "1h"),
            SyncInterval::Every6Hours => write!(f, "6h"),
            SyncInterval::Daily => write!(f, "24h"),
            SyncInterval::Weekly => write!(f, "weekly"),
            SyncInterval::Manual => write!(f, "manual"),
        }
    }
}

impl std::str::FromStr for SyncInterval {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "5m" | "every_5_min" | "every5min" => Ok(SyncInterval::Every5Min),
            "15m" | "every_15_min" | "every15min" => Ok(SyncInterval::Every15Min),
            "1h" | "hourly" => Ok(SyncInterval::Hourly),
            "6h" | "every_6_hours" | "every6hours" => Ok(SyncInterval::Every6Hours),
            "24h" | "daily" => Ok(SyncInterval::Daily),
            "weekly" => Ok(SyncInterval::Weekly),
            "manual" => Ok(SyncInterval::Manual),
            _ => Err(format!("Unknown sync interval: {}", s)),
        }
    }
}

impl From<SyncInterval> for StdDuration {
    fn from(interval: SyncInterval) -> Self {
        match interval {
            SyncInterval::Every5Min => StdDuration::from_secs(5 * 60),
            SyncInterval::Every15Min => StdDuration::from_secs(15 * 60),
            SyncInterval::Hourly => StdDuration::from_secs(60 * 60),
            SyncInterval::Every6Hours => StdDuration::from_secs(6 * 60 * 60),
            SyncInterval::Daily => StdDuration::from_secs(24 * 60 * 60),
            SyncInterval::Weekly => StdDuration::from_secs(7 * 24 * 60 * 60),
            SyncInterval::Manual => StdDuration::MAX,
        }
    }
}

impl From<SyncInterval> for Duration {
    fn from(interval: SyncInterval) -> Self {
        match interval {
            SyncInterval::Every5Min => Duration::minutes(5),
            SyncInterval::Every15Min => Duration::minutes(15),
            SyncInterval::Hourly => Duration::hours(1),
            SyncInterval::Every6Hours => Duration::hours(6),
            SyncInterval::Daily => Duration::hours(24),
            SyncInterval::Weekly => Duration::days(7),
            SyncInterval::Manual => Duration::max_value(),
        }
    }
}

impl SyncInterval {
    /// Convert to cron expression. Returns `None` for `Manual` since manual
    /// syncs are not scheduled.
    pub fn to_cron(&self) -> Option<&'static str> {
        match self {
            SyncInterval::Every5Min => Some("0 */5 * * * *"),
            SyncInterval::Every15Min => Some("0 */15 * * * *"),
            SyncInterval::Hourly => Some("0 0 * * * *"),
            SyncInterval::Every6Hours => Some("0 0 */6 * * *"),
            SyncInterval::Daily => Some("0 0 0 * * *"),
            SyncInterval::Weekly => Some("0 0 0 * * 0"),
            SyncInterval::Manual => None,
        }
    }
}

/// R2 table path information.
///
/// SECURITY: All paths are prefixed with project_id to ensure data isolation
/// between projects. This prevents cross-project data access.
///
/// For production code, always use constructors that require `project_id`:
/// - `with_project()` - standard production constructor
/// - `with_date_partition()` - for date-partitioned tables
#[derive(Debug, Clone)]
pub struct R2TablePath {
    /// Object key prefix, e.g., "project-uuid/stripe/customers"
    pub prefix: String,
    /// Project ID for validation
    pub project_id: Option<Uuid>,
    /// Whether this table uses date-based partitioning
    pub date_partitioned: bool,
    /// Column name used for date partitioning (e.g., "created_at")
    pub partition_column: Option<String>,
    /// Auto-detected partition strategy for external Parquet files.
    /// Populated from `warehouse_tables.detected_partition_scheme` during query planning.
    pub detected_partition_scheme:
        Option<crate::warehouse::indexes::external_config::PartitionStrategy>,
    /// ClickHouse buffer table name for hot data that hasn't been flushed to R2 yet.
    /// When set, queries UNION ALL the buffer table with the R2 files.
    pub buffer_ch_table: Option<String>,
}

impl R2TablePath {
    /// Create a new R2 table path for testing purposes only.
    ///
    /// **WARNING**: This method creates paths without project isolation and should
    /// ONLY be used in tests. Production code must use `with_project()` to ensure
    /// proper data isolation between projects.
    ///
    /// # Security
    ///
    /// Paths created with this method do not have project isolation. Using this
    /// in production code is a security vulnerability.
    #[doc(hidden)]
    pub fn for_testing(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
            project_id: None,
            date_partitioned: false,
            partition_column: None,
            detected_partition_scheme: None,
            buffer_ch_table: None,
        }
    }

    /// Create a new R2 table path with project isolation (validated).
    ///
    /// SECURITY: This is the recommended constructor for production use.
    /// The prefix will include the project_id to ensure data isolation.
    /// The table name is validated to prevent path traversal attacks.
    ///
    /// # Errors
    ///
    /// Returns `ValidationError::InvalidTableName` if the table name contains
    /// path traversal sequences, path separators, or other invalid characters.
    pub fn try_with_project(
        project_id: Uuid,
        source_type: SourceType,
        table_name: &str,
    ) -> Result<Self, ValidationError> {
        validate_table_name(table_name)?;
        Ok(Self {
            prefix: format!("{}/{}/{}", project_id, source_type, table_name),
            project_id: Some(project_id),
            date_partitioned: false,
            partition_column: None,
            detected_partition_scheme: None,
            buffer_ch_table: None,
        })
    }

    /// Create a new R2 table path with project isolation.
    ///
    /// SECURITY: This is the recommended constructor for production use.
    /// The prefix will include the project_id to ensure data isolation.
    ///
    /// # Panics
    ///
    /// Panics if the table name is invalid. Use `try_with_project` if you need
    /// to handle validation errors gracefully.
    pub fn with_project(project_id: Uuid, source_type: SourceType, table_name: &str) -> Self {
        Self::try_with_project(project_id, source_type, table_name)
            .unwrap_or_else(|e| panic!("Invalid table name '{}': {}", table_name, e))
    }

    /// Create a date-partitioned table path (validated).
    ///
    /// SECURITY: Requires project_id for data isolation.
    /// The table name and partition column are validated to prevent injection attacks.
    ///
    /// Date-partitioned tables store data in `prefix/year/month/day/` structure
    /// for efficient partition pruning.
    ///
    /// # Errors
    ///
    /// Returns `ValidationError::InvalidTableName` if the table name contains
    /// path traversal sequences, path separators, or other invalid characters.
    /// Returns `ValidationError::InvalidColumnName` if the partition column contains
    /// invalid characters.
    pub fn try_with_date_partition(
        project_id: Uuid,
        source_type: SourceType,
        table_name: &str,
        partition_column: &str,
    ) -> Result<Self, ValidationError> {
        validate_table_name(table_name)?;
        validate_column_name(partition_column)?;
        Ok(Self {
            prefix: format!("{}/{}/{}", project_id, source_type, table_name),
            project_id: Some(project_id),
            date_partitioned: true,
            partition_column: Some(partition_column.to_string()),
            detected_partition_scheme: None,
            buffer_ch_table: None,
        })
    }

    /// Create a date-partitioned table path.
    ///
    /// SECURITY: Requires project_id for data isolation.
    ///
    /// Date-partitioned tables store data in `prefix/year/month/day/` structure
    /// for efficient partition pruning.
    ///
    /// # Panics
    ///
    /// Panics if the table name or partition column is invalid. Use `try_with_date_partition`
    /// if you need to handle validation errors gracefully.
    pub fn with_date_partition(
        project_id: Uuid,
        source_type: SourceType,
        table_name: &str,
        partition_column: &str,
    ) -> Self {
        Self::try_with_date_partition(project_id, source_type, table_name, partition_column)
            .unwrap_or_else(|e| {
                panic!(
                    "Validation error for table '{}' column '{}': {}",
                    table_name, partition_column, e
                )
            })
    }

    /// Create from project, source type and table name.
    pub fn from_project_source_table(
        project_id: Uuid,
        source_type: SourceType,
        table_name: &str,
    ) -> Self {
        Self::with_project(project_id, source_type, table_name)
    }

    /// Check if this path belongs to the given project.
    ///
    /// # Security
    ///
    /// Returns `true` if:
    /// - The path was created with an explicit project_id that matches, OR
    /// - The path's prefix starts with the project_id string (fallback for legacy paths)
    ///
    /// The fallback is less secure because it relies on prefix format conventions.
    /// Always prefer creating paths with `with_project()` for reliable isolation.
    pub fn belongs_to_project(&self, project_id: Uuid) -> bool {
        match self.project_id {
            Some(pid) => pid == project_id,
            None => {
                // Fallback: check prefix - less secure but needed for legacy data
                tracing::warn!(
                    prefix = %self.prefix,
                    project_id = %project_id,
                    "R2TablePath missing project_id, falling back to prefix matching"
                );
                self.prefix.starts_with(&project_id.to_string())
            }
        }
    }

    /// Check if this path has explicit project isolation.
    pub fn has_project_id(&self) -> bool {
        self.project_id.is_some()
    }

    /// Get the project_id if explicitly set.
    pub fn get_project_id(&self) -> Option<Uuid> {
        self.project_id
    }

    /// Get the file pattern for this table.
    ///
    /// For date-partitioned tables with a date range, returns a more specific pattern.
    /// Non-partitioned tables use a flat glob (`*.parquet`) since their files sit
    /// directly under the prefix without subdirectories.
    pub fn file_pattern(&self, date_range: Option<&DateRange>) -> String {
        if self.date_partitioned {
            if let Some(range) = date_range {
                return range.to_glob_pattern(&self.prefix);
            }
            format!("{}/**/*.parquet", self.prefix)
        } else {
            format!("{}/*.parquet", self.prefix)
        }
    }
}

/// A date range for partition pruning.
#[derive(Debug, Clone)]
pub struct DateRange {
    pub start: Option<chrono::NaiveDate>,
    pub end: Option<chrono::NaiveDate>,
}

impl DateRange {
    /// Create a new date range.
    pub fn new(start: Option<chrono::NaiveDate>, end: Option<chrono::NaiveDate>) -> Self {
        Self { start, end }
    }

    /// Create a date range from a single date (start = end).
    pub fn single_day(date: chrono::NaiveDate) -> Self {
        Self {
            start: Some(date),
            end: Some(date),
        }
    }

    /// Returns `true` when both bounds are set and start > end,
    /// meaning no date can satisfy the range.
    pub fn is_impossible(&self) -> bool {
        matches!((self.start, self.end), (Some(s), Some(e)) if s > e)
    }

    /// Maximum number of months before falling back to year-level patterns.
    /// PERFORMANCE: Brace expansion with too many patterns can slow down ClickHouse parsing.
    const MAX_MONTH_PATTERNS: usize = 24; // 2 years worth of months

    /// Convert to a glob pattern for partition pruning.
    ///
    /// For a date range spanning multiple months or years, this generates a union pattern.
    /// For example, 2024-12-15 to 2025-02-10 would generate:
    /// `{prefix/2024/12/**/*.parquet,prefix/2025/01/**/*.parquet,prefix/2025/02/**/*.parquet}`
    ///
    /// # Pattern Optimization
    ///
    /// This method uses smart pattern collapsing to avoid exponential brace expansion:
    /// - Full years are collapsed to `YYYY/**/*.parquet`
    /// - Partial years use month-level patterns
    /// - Very long ranges (>24 months) are split into year-level patterns
    ///
    /// For very large date ranges (>5 years), consider using `to_pattern_list()`
    /// with multiple s3() calls joined by UNION ALL.
    pub fn to_glob_pattern(&self, prefix: &str) -> String {
        if self.is_impossible() {
            return format!("{}/__dh_no_match__/*.parquet", prefix);
        }

        match (self.start, self.end) {
            (Some(start), Some(end)) if start == end => {
                // Single day - use exact day path
                format!(
                    "{}/{:04}/{:02}/{:02}/*.parquet",
                    prefix,
                    start.year(),
                    start.month(),
                    start.day()
                )
            }
            (Some(start), Some(end))
                if start.year() == end.year() && start.month() == end.month() =>
            {
                // Same month - use day wildcard
                format!(
                    "{}/{:04}/{:02}/*/*.parquet",
                    prefix,
                    start.year(),
                    start.month()
                )
            }
            (Some(start), Some(end)) => {
                // Different months/years - use optimized pattern generation
                let patterns = Self::generate_optimized_patterns(prefix, start, end);
                if patterns.len() == 1 {
                    patterns.into_iter().next().unwrap()
                } else {
                    format!("{{{}}}", patterns.join(","))
                }
            }
            (Some(start), None) => {
                // Open-ended range (start to infinity) - scan from start year to current year
                // This ensures we don't miss data from years after the start date
                let current_year = chrono::Utc::now().year();
                let start_year = start.year();

                if start_year >= current_year {
                    // Start year is current or future - just scan that year
                    format!("{}/{:04}/**/*.parquet", prefix, start_year)
                } else if current_year - start_year <= 5 {
                    // Reasonable range - use brace expansion for all years
                    let years: Vec<String> = (start_year..=current_year)
                        .map(|y| format!("{}/{:04}/**/*.parquet", prefix, y))
                        .collect();
                    if years.len() == 1 {
                        years.into_iter().next().unwrap()
                    } else {
                        format!("{{{}}}", years.join(","))
                    }
                } else {
                    // Very long range (>5 years) - fall back to full scan
                    // This prevents extremely long patterns
                    format!("{}/**/*.parquet", prefix)
                }
            }
            (None, Some(_end)) => {
                // Open-ended range (beginning to end) - must scan all
                format!("{}/**/*.parquet", prefix)
            }
            _ => {
                // No range - use full scan
                format!("{}/**/*.parquet", prefix)
            }
        }
    }

    /// Convert to a list of individual patterns for UNION ALL optimization.
    ///
    /// PERFORMANCE: For very large date ranges (>5 years), use this method with
    /// multiple s3() function calls joined by UNION ALL. This avoids ClickHouse
    /// parsing issues with very long brace expansion patterns.
    ///
    /// # Example
    /// ```ignore
    /// let patterns = date_range.to_pattern_list(prefix);
    /// let s3_calls: Vec<String> = patterns.iter()
    ///     .map(|p| format!("SELECT * FROM s3('{}')", p))
    ///     .collect();
    /// let sql = s3_calls.join(" UNION ALL ");
    /// ```
    pub fn to_pattern_list(&self, prefix: &str) -> Vec<String> {
        if self.is_impossible() {
            return Vec::new();
        }

        match (self.start, self.end) {
            (Some(start), Some(end)) if start == end => {
                vec![format!(
                    "{}/{:04}/{:02}/{:02}/*.parquet",
                    prefix,
                    start.year(),
                    start.month(),
                    start.day()
                )]
            }
            (Some(start), Some(end))
                if start.year() == end.year() && start.month() == end.month() =>
            {
                vec![format!(
                    "{}/{:04}/{:02}/*/*.parquet",
                    prefix,
                    start.year(),
                    start.month()
                )]
            }
            (Some(start), Some(end)) => Self::generate_optimized_patterns(prefix, start, end),
            (Some(start), None) => {
                // Open-ended range - scan from start year to current year
                let current_year = chrono::Utc::now().year();
                let start_year = start.year();

                if start_year >= current_year {
                    vec![format!("{}/{:04}/**/*.parquet", prefix, start_year)]
                } else {
                    // Generate pattern for each year from start to current
                    (start_year..=current_year)
                        .map(|y| format!("{}/{:04}/**/*.parquet", prefix, y))
                        .collect()
                }
            }
            (None, Some(_)) | (None, None) => {
                vec![format!("{}/**/*.parquet", prefix)]
            }
        }
    }

    /// Check if this date range should use UNION ALL instead of brace expansion.
    ///
    /// Returns true for ranges spanning more than 5 years, which would generate
    /// patterns too long for efficient ClickHouse parsing.
    pub fn should_use_union_all(&self) -> bool {
        match (self.start, self.end) {
            (Some(start), Some(end)) => {
                let year_diff = end.year() - start.year();
                year_diff > 5
            }
            _ => false,
        }
    }

    /// Generate optimized glob patterns with smart collapsing.
    ///
    /// This method optimizes patterns by:
    /// 1. Collapsing full years to `YYYY/**/*.parquet`
    /// 2. Using month-level patterns only for partial years at start/end
    /// 3. Limiting total patterns to MAX_MONTH_PATTERNS
    fn generate_optimized_patterns(
        prefix: &str,
        start: chrono::NaiveDate,
        end: chrono::NaiveDate,
    ) -> Vec<String> {
        use chrono::Datelike;

        let mut patterns = Vec::new();

        // Check if we can use year-level optimization
        let spans_multiple_years = end.year() > start.year();

        if spans_multiple_years && end.year() - start.year() > 1 {
            // Multi-year range: use hybrid pattern (partial year + full years + partial year)

            // First year (partial): from start month to December
            if start.month() == 1 && start.day() == 1 {
                // Full first year
                patterns.push(format!("{}/{:04}/**/*.parquet", prefix, start.year()));
            } else {
                // Partial first year
                for month in start.month()..=12 {
                    patterns.push(format!(
                        "{}/{:04}/{:02}/**/*.parquet",
                        prefix,
                        start.year(),
                        month
                    ));
                }
            }

            // Full middle years
            for year in (start.year() + 1)..end.year() {
                patterns.push(format!("{}/{:04}/**/*.parquet", prefix, year));
            }

            // Last year (partial): from January to end month
            if end.month() == 12 && end.day() == 31 {
                // Full last year
                patterns.push(format!("{}/{:04}/**/*.parquet", prefix, end.year()));
            } else {
                // Partial last year
                for month in 1..=end.month() {
                    patterns.push(format!(
                        "{}/{:04}/{:02}/**/*.parquet",
                        prefix,
                        end.year(),
                        month
                    ));
                }
            }
        } else {
            // Single year or adjacent years: use month-level patterns
            patterns = Self::generate_month_patterns(prefix, start, end);

            // If too many patterns, fall back to year-level
            if patterns.len() > Self::MAX_MONTH_PATTERNS {
                let year_patterns: Vec<String> = (start.year()..=end.year())
                    .map(|y| format!("{}/{:04}/**/*.parquet", prefix, y))
                    .collect();
                return year_patterns;
            }
        }

        patterns
    }

    /// Generate glob patterns for each month in the date range.
    fn generate_month_patterns(
        prefix: &str,
        start: chrono::NaiveDate,
        end: chrono::NaiveDate,
    ) -> Vec<String> {
        use chrono::Datelike;

        let mut patterns = Vec::new();
        let mut current = start;

        while current <= end {
            patterns.push(format!(
                "{}/{:04}/{:02}/**/*.parquet",
                prefix,
                current.year(),
                current.month()
            ));

            // Move to the first day of the next month
            let (next_year, next_month) = if current.month() == 12 {
                (current.year() + 1, 1)
            } else {
                (current.year(), current.month() + 1)
            };

            // Create the first day of the next month
            match chrono::NaiveDate::from_ymd_opt(next_year, next_month, 1) {
                Some(next) => current = next,
                None => break, // Invalid date, stop iteration
            }
        }

        patterns
    }
}

use chrono::Datelike;

// =============================================================================
// Warm Storage Path Helpers
// =============================================================================

/// Validate that a path component is safe (no traversal or separators).
fn validate_path_component(component: &str, label: &str) -> Result<(), ValidationError> {
    if component.is_empty() {
        return Err(ValidationError::InvalidTableName(format!(
            "{} cannot be empty",
            label
        )));
    }
    if component.contains('/') || component.contains('\\') || component.contains('\0') {
        return Err(ValidationError::InvalidTableName(format!(
            "{} contains path separators or null bytes",
            label
        )));
    }
    if component.contains("..") {
        return Err(ValidationError::InvalidTableName(format!(
            "{} contains path traversal sequence",
            label
        )));
    }
    Ok(())
}

/// Get the base R2 path for a warm source.
///
/// Path format: `projects/{project_id}/warm/{source_name}/`
///
/// This path contains all Parquet files for a warm source.
///
/// # Panics
/// Panics if `source_name` contains path traversal characters. Callers
/// must validate user input before calling.
pub fn warm_source_path(project_id: Uuid, source_name: &str) -> String {
    validate_path_component(source_name, "source_name")
        .expect("warm_source_path called with invalid source_name");
    format!("projects/{}/warm/{}/", project_id, source_name)
}

/// Get the R2 path for a specific table within a warm source.
///
/// Path format: `projects/{project_id}/warm/{source_name}/{table_name}/`
///
/// This path contains all Parquet files for a specific table.
///
/// # Panics
/// Panics if `source_name` or `table_name` contain path traversal characters.
pub fn warm_table_path(project_id: Uuid, source_name: &str, table_name: &str) -> String {
    validate_path_component(source_name, "source_name")
        .expect("warm_table_path called with invalid source_name");
    validate_path_component(table_name, "table_name")
        .expect("warm_table_path called with invalid table_name");
    format!(
        "projects/{}/warm/{}/{}/",
        project_id, source_name, table_name
    )
}

/// Get the R2 path for a Parquet file within a warm table.
///
/// Path format: `projects/{project_id}/warm/{source_name}/{table_name}/{partition}.parquet`
///
/// The partition can be a timestamp, batch number, or other identifier.
///
/// # Panics
/// Panics if any path component contains path traversal characters.
pub fn warm_file_path(
    project_id: Uuid,
    source_name: &str,
    table_name: &str,
    partition: &str,
) -> String {
    validate_path_component(source_name, "source_name")
        .expect("warm_file_path called with invalid source_name");
    validate_path_component(table_name, "table_name")
        .expect("warm_file_path called with invalid table_name");
    validate_path_component(partition, "partition")
        .expect("warm_file_path called with invalid partition");
    format!(
        "projects/{}/warm/{}/{}/{}.parquet",
        project_id, source_name, table_name, partition
    )
}

/// Get the local path for FST indexes for a warm source.
///
/// Path format: `data/indexes/{project_id}/{source_name}/`
///
/// This path contains all FST indexes for the source.
///
/// # Panics
/// Panics if `source_name` contains path traversal characters.
pub fn local_index_source_path(project_id: Uuid, source_name: &str) -> String {
    validate_path_component(source_name, "source_name")
        .expect("local_index_source_path called with invalid source_name");
    format!("data/indexes/{}/{}/", project_id, source_name)
}

/// Get the local path for a specific column's FST index.
///
/// Path format: `data/indexes/{project_id}/{source_name}/{table_name}/{column_name}.fst`
///
/// # Panics
/// Panics if any path component contains path traversal characters.
pub fn local_index_column_path(
    project_id: Uuid,
    source_name: &str,
    table_name: &str,
    column_name: &str,
) -> String {
    validate_path_component(source_name, "source_name")
        .expect("local_index_column_path called with invalid source_name");
    validate_path_component(table_name, "table_name")
        .expect("local_index_column_path called with invalid table_name");
    validate_path_component(column_name, "column_name")
        .expect("local_index_column_path called with invalid column_name");
    format!(
        "data/indexes/{}/{}/{}/{}.fst",
        project_id, source_name, table_name, column_name
    )
}

// NOTE: parse_sync_interval and parse_sync_interval_chrono have been replaced by
// `From<SyncInterval> for std::time::Duration` and `From<SyncInterval> for chrono::Duration`.
// Use: `let interval: SyncInterval = "5m".parse()?;`
// Then: `let duration: std::time::Duration = interval.into();`
// Or:   `let duration: chrono::Duration = interval.into();`

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    // ===== DateRange::to_glob_pattern Tests =====

    #[test]
    fn test_single_day_pattern() {
        let date = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
        let range = DateRange::single_day(date);
        let pattern = range.to_glob_pattern("data/orders");

        assert_eq!(pattern, "data/orders/2025/01/15/*.parquet");
    }

    #[test]
    fn test_same_month_pattern() {
        let start = NaiveDate::from_ymd_opt(2025, 3, 5).unwrap();
        let end = NaiveDate::from_ymd_opt(2025, 3, 25).unwrap();
        let range = DateRange::new(Some(start), Some(end));
        let pattern = range.to_glob_pattern("data/orders");

        // Same month should use day wildcard
        assert_eq!(pattern, "data/orders/2025/03/*/*.parquet");
    }

    #[test]
    fn test_two_month_pattern() {
        let start = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
        let end = NaiveDate::from_ymd_opt(2025, 2, 10).unwrap();
        let range = DateRange::new(Some(start), Some(end));
        let pattern = range.to_glob_pattern("data/orders");

        // Two months should use brace expansion
        assert!(pattern.contains("{"));
        assert!(pattern.contains("2025/01"));
        assert!(pattern.contains("2025/02"));
    }

    #[test]
    fn test_year_boundary_pattern() {
        let start = NaiveDate::from_ymd_opt(2024, 12, 15).unwrap();
        let end = NaiveDate::from_ymd_opt(2025, 2, 10).unwrap();
        let range = DateRange::new(Some(start), Some(end));
        let pattern = range.to_glob_pattern("data/orders");

        // Should span year boundary
        assert!(pattern.contains("2024/12"));
        assert!(pattern.contains("2025/01"));
        assert!(pattern.contains("2025/02"));
    }

    #[test]
    fn test_full_year_pattern() {
        let start = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2025, 12, 31).unwrap();
        let range = DateRange::new(Some(start), Some(end));
        let pattern = range.to_glob_pattern("data/orders");

        // 12 months is under the 50 limit, should use brace expansion
        assert!(pattern.contains("{"));
        // Should have 12 patterns
        let count = pattern.matches("2025/").count();
        assert_eq!(count, 12);
    }

    #[test]
    fn test_multi_year_pattern_under_50_months() {
        // 3 full years: uses year-level patterns for efficiency
        let start = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2025, 12, 31).unwrap();
        let range = DateRange::new(Some(start), Some(end));
        let pattern = range.to_glob_pattern("data/orders");

        // Should have brace expansion
        assert!(pattern.contains("{"));
        // Full years use year-level wildcards (2023/**/*, not 2023/01)
        assert!(pattern.contains("2023/**"));
        assert!(pattern.contains("2024/**"));
        assert!(pattern.contains("2025/**"));
    }

    #[test]
    fn test_large_range_over_50_months_falls_back_to_years() {
        // 5 years = 60 months, over 50 limit
        let start = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
        let range = DateRange::new(Some(start), Some(end));
        let pattern = range.to_glob_pattern("data/orders");

        // Should fall back to year-level wildcards
        // Pattern should be {data/orders/2020/**/*.parquet,...,data/orders/2024/**/*.parquet}
        assert!(pattern.contains("{"));
        assert!(pattern.contains("2020/**/*.parquet"));
        assert!(pattern.contains("2024/**/*.parquet"));
        // Should NOT contain month-level patterns
        assert!(!pattern.contains("/01/**/*.parquet"));
    }

    #[test]
    fn test_open_ended_start() {
        let current_year = chrono::Utc::now().year();
        let start = NaiveDate::from_ymd_opt(current_year, 6, 1).unwrap();
        let range = DateRange::new(Some(start), None);
        let pattern = range.to_glob_pattern("data/orders");

        // When start year is current year, should just scan that year
        assert_eq!(
            pattern,
            format!("data/orders/{}/**/*.parquet", current_year)
        );
    }

    #[test]
    fn test_open_ended_start_past_year() {
        let current_year = chrono::Utc::now().year();
        // Start from 2 years ago
        let start = NaiveDate::from_ymd_opt(current_year - 2, 1, 1).unwrap();
        let range = DateRange::new(Some(start), None);
        let pattern = range.to_glob_pattern("data/orders");

        // Open-ended should scan from start year to current year
        // Should be a brace expansion with 3 years
        assert!(pattern.contains("{"));
        assert!(pattern.contains(&format!("{}", current_year - 2)));
        assert!(pattern.contains(&format!("{}", current_year - 1)));
        assert!(pattern.contains(&format!("{}", current_year)));
    }

    #[test]
    fn test_open_ended_end() {
        let end = NaiveDate::from_ymd_opt(2025, 6, 1).unwrap();
        let range = DateRange::new(None, Some(end));
        let pattern = range.to_glob_pattern("data/orders");

        // Open-ended at beginning must scan all
        assert_eq!(pattern, "data/orders/**/*.parquet");
    }

    #[test]
    fn test_no_range() {
        let range = DateRange::new(None, None);
        let pattern = range.to_glob_pattern("data/orders");

        // No range = full scan
        assert_eq!(pattern, "data/orders/**/*.parquet");
    }

    #[test]
    fn test_first_day_of_month() {
        let start = NaiveDate::from_ymd_opt(2025, 3, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2025, 3, 1).unwrap();
        let range = DateRange::new(Some(start), Some(end));
        let pattern = range.to_glob_pattern("data/orders");

        // Single day
        assert_eq!(pattern, "data/orders/2025/03/01/*.parquet");
    }

    #[test]
    fn test_last_day_of_month() {
        let start = NaiveDate::from_ymd_opt(2025, 1, 31).unwrap();
        let end = NaiveDate::from_ymd_opt(2025, 2, 28).unwrap();
        let range = DateRange::new(Some(start), Some(end));
        let pattern = range.to_glob_pattern("data/orders");

        // Should span two months
        assert!(pattern.contains("2025/01"));
        assert!(pattern.contains("2025/02"));
    }

    #[test]
    fn test_leap_year_february() {
        let start = NaiveDate::from_ymd_opt(2024, 2, 28).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 2, 29).unwrap();
        let range = DateRange::new(Some(start), Some(end));
        let pattern = range.to_glob_pattern("data/orders");

        // Same month
        assert_eq!(pattern, "data/orders/2024/02/*/*.parquet");
    }

    #[test]
    fn test_december_to_january() {
        let start = NaiveDate::from_ymd_opt(2024, 12, 25).unwrap();
        let end = NaiveDate::from_ymd_opt(2025, 1, 5).unwrap();
        let range = DateRange::new(Some(start), Some(end));
        let pattern = range.to_glob_pattern("data/orders");

        // Year boundary
        assert!(pattern.contains("2024/12"));
        assert!(pattern.contains("2025/01"));
    }

    // ===== Coercion Tests =====

    #[test]
    fn test_coercion_same_type() {
        use arrow::datatypes::DataType;
        let result = super::coerce_types(&DataType::Int64, None, &DataType::Int64, None);
        assert!(matches!(result, super::CoercionResult::Same));
    }

    #[test]
    fn test_coercion_int_widening() {
        use arrow::datatypes::DataType;
        let result = super::coerce_types(&DataType::Int32, None, &DataType::Int64, None);
        assert!(matches!(
            result,
            super::CoercionResult::AutoCoerce {
                target: DataType::Int64,
                ..
            }
        ));
    }

    #[test]
    fn test_coercion_money_semantic_conflict() {
        use arrow::datatypes::DataType;
        let cents = super::SemanticType::Money {
            currency: Some("USD".to_string()),
            in_cents: true,
        };
        let dollars = super::SemanticType::Money {
            currency: Some("USD".to_string()),
            in_cents: false,
        };
        let result = super::coerce_types(
            &DataType::Int64,
            Some(&cents),
            &DataType::Int64,
            Some(&dollars),
        );
        assert!(matches!(
            result,
            super::CoercionResult::RequiresExplicit { .. }
        ));
    }

    #[test]
    fn test_conversion_cents_to_dollars() {
        let result = super::cents_to_dollars(1999);
        assert!((result - 19.99).abs() < 0.001);
    }

    #[test]
    fn test_conversion_dollars_to_cents() {
        let result = super::dollars_to_cents(19.99);
        assert_eq!(result, 1999);
    }

    // ===== validate_table_name Tests =====

    #[test]
    fn test_validate_table_name_valid() {
        assert!(validate_table_name("customers").is_ok());
        assert!(validate_table_name("order_items").is_ok());
        assert!(validate_table_name("users-v2").is_ok());
        assert!(validate_table_name("Table123").is_ok());
    }

    #[test]
    fn test_validate_table_name_empty() {
        assert!(validate_table_name("").is_err());
    }

    #[test]
    fn test_validate_table_name_path_traversal() {
        assert!(validate_table_name("..").is_err());
        assert!(validate_table_name("../other").is_err());
        assert!(validate_table_name("foo/../bar").is_err());
    }

    #[test]
    fn test_validate_table_name_path_separators() {
        assert!(validate_table_name("foo/bar").is_err());
        assert!(validate_table_name("foo\\bar").is_err());
    }

    #[test]
    fn test_validate_table_name_dot_prefix() {
        assert!(validate_table_name(".hidden").is_err());
        assert!(validate_table_name(".gitignore").is_err());
    }

    #[test]
    fn test_validate_table_name_null_byte() {
        assert!(validate_table_name("foo\0bar").is_err());
    }

    // ===== NullSemantics Tests =====

    #[test]
    fn test_null_semantics_default() {
        let semantics = NullSemantics::default();

        // Empty string is NOT treated as NULL by default
        assert!(!semantics.treat_empty_as_null);

        // Default null values are "NULL" and "null"
        assert!(semantics.null_values.contains(&"NULL".to_string()));
        assert!(semantics.null_values.contains(&"null".to_string()));
        assert!(!semantics.null_values.contains(&String::new()));
    }

    #[test]
    fn test_null_semantics_is_null() {
        let semantics = NullSemantics::default();

        // "NULL" and "null" are NULL
        assert!(semantics.is_null("NULL"));
        assert!(semantics.is_null("null"));

        // Empty string is NOT NULL by default
        assert!(!semantics.is_null(""));

        // Random strings are not NULL
        assert!(!semantics.is_null("hello"));
        assert!(!semantics.is_null("  "));
    }

    #[test]
    fn test_null_semantics_legacy() {
        let semantics = NullSemantics::legacy();

        // Legacy mode treats empty as NULL
        assert!(semantics.treat_empty_as_null);

        // Empty string IS NULL in legacy mode
        assert!(semantics.is_null(""));
        assert!(semantics.is_null("NULL"));
        assert!(semantics.is_null("null"));
    }

    #[test]
    fn test_null_semantics_with_treat_empty() {
        let semantics = NullSemantics::default().with_treat_empty_as_null(true);

        assert!(semantics.treat_empty_as_null);
        assert!(semantics.is_null(""));

        // Should have added empty string to null_values
        assert!(semantics.null_values.contains(&String::new()));
    }

    #[test]
    fn test_null_semantics_with_additional_nulls() {
        let semantics = NullSemantics::default().with_additional_nulls(&["N/A", "n/a", "-"]);

        assert!(semantics.is_null("N/A"));
        assert!(semantics.is_null("n/a"));
        assert!(semantics.is_null("-"));
        assert!(semantics.is_null("NULL"));

        // Still doesn't treat empty as NULL
        assert!(!semantics.is_null(""));
    }

    #[test]
    fn test_null_semantics_null_values_for_reader() {
        let semantics = NullSemantics::default();
        let values = semantics.null_values_for_reader();

        // Should not include empty string
        assert!(!values.contains(&String::new()));

        let legacy = NullSemantics::legacy();
        let legacy_values = legacy.null_values_for_reader();

        // Legacy should include empty string
        assert!(legacy_values.contains(&String::new()));
    }

    // ===== Timestamp Precision Tests =====

    #[test]
    fn test_timestamp_precision_to_microseconds_seconds() {
        let precision = TimestampPrecision::Seconds;
        assert_eq!(precision.to_microseconds(1), 1_000_000);
        assert_eq!(precision.to_microseconds(1705312800), 1705312800_000_000);
    }

    #[test]
    fn test_timestamp_precision_to_microseconds_milliseconds() {
        let precision = TimestampPrecision::Milliseconds;
        assert_eq!(precision.to_microseconds(1), 1_000);
        assert_eq!(precision.to_microseconds(1705312800000), 1705312800_000_000);
    }

    #[test]
    fn test_timestamp_precision_to_microseconds_microseconds() {
        let precision = TimestampPrecision::Microseconds;
        assert_eq!(
            precision.to_microseconds(1705312800_000_000),
            1705312800_000_000
        );
    }

    #[test]
    fn test_timestamp_precision_to_microseconds_nanoseconds() {
        let precision = TimestampPrecision::Nanoseconds;
        // 1 billion nanoseconds = 1 million microseconds
        assert_eq!(precision.to_microseconds(1_000_000_000), 1_000_000);
    }

    #[test]
    fn test_timestamp_precision_from_microseconds() {
        let precision = TimestampPrecision::Seconds;
        assert_eq!(precision.from_microseconds(1_000_000), 1);

        let precision = TimestampPrecision::Nanoseconds;
        assert_eq!(precision.from_microseconds(1), 1_000);
    }

    #[test]
    fn test_timestamp_precision_arrow_conversion() {
        use arrow::datatypes::TimeUnit;

        assert_eq!(
            TimestampPrecision::Seconds.to_arrow_time_unit(),
            TimeUnit::Second
        );
        assert_eq!(
            TimestampPrecision::Microseconds.to_arrow_time_unit(),
            TimeUnit::Microsecond
        );

        assert_eq!(
            TimestampPrecision::from_arrow_time_unit(TimeUnit::Millisecond),
            TimestampPrecision::Milliseconds
        );
    }

    #[test]
    fn test_timestamp_precision_default() {
        // Default should be microseconds (our internal standard)
        assert_eq!(
            TimestampPrecision::default(),
            TimestampPrecision::Microseconds
        );
    }

    #[test]
    fn test_semantic_type_timestamp() {
        let ts_semantic = SemanticType::Timestamp {
            precision: TimestampPrecision::Seconds,
            source_timezone: "UTC".to_string(),
        };

        // Verify it serializes/deserializes correctly
        let json = serde_json::to_string(&ts_semantic).unwrap();
        let deserialized: SemanticType = serde_json::from_str(&json).unwrap();

        match deserialized {
            SemanticType::Timestamp {
                precision,
                source_timezone,
            } => {
                assert_eq!(precision, TimestampPrecision::Seconds);
                assert_eq!(source_timezone, "UTC");
            }
            _ => panic!("Expected Timestamp variant"),
        }
    }

    #[test]
    fn test_column_schema_with_timezone() {
        let col = ColumnSchema::new("created_at", ColumnType::Timestamp, false)
            .with_description("Creation timestamp")
            .with_timezone("America/New_York");

        assert_eq!(col.name, "created_at");
        assert_eq!(col.data_type, ColumnType::Timestamp);
        assert!(!col.nullable);
        assert_eq!(col.description, Some("Creation timestamp".to_string()));
        assert_eq!(col.timezone, Some("America/New_York".to_string()));
    }

    #[test]
    fn test_column_schema_default_no_timezone() {
        let col = ColumnSchema::new("id", ColumnType::Int64, false);
        assert_eq!(col.timezone, None);
    }

    #[test]
    fn test_coercion_timestamp_timezone_conflict() {
        use arrow::datatypes::DataType;

        let utc_ts = SemanticType::Timestamp {
            precision: TimestampPrecision::Microseconds,
            source_timezone: "UTC".to_string(),
        };
        let ny_ts = SemanticType::Timestamp {
            precision: TimestampPrecision::Microseconds,
            source_timezone: "America/New_York".to_string(),
        };

        // Both non-UTC timezones should trigger explicit conversion
        let result = coerce_types(
            &DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                Some("America/New_York".into()),
            ),
            Some(&ny_ts),
            &DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                Some("Europe/London".into()),
            ),
            Some(&SemanticType::Timestamp {
                precision: TimestampPrecision::Microseconds,
                source_timezone: "Europe/London".to_string(),
            }),
        );

        assert!(matches!(result, CoercionResult::RequiresExplicit { .. }));
    }

    // ========== Regression Tests ==========

    #[test]
    fn test_coerce_float64_to_int_requires_explicit() {
        use arrow::datatypes::DataType;

        let result = coerce_arrow_types(&DataType::Float64, &DataType::Int32);
        assert!(
            matches!(result, CoercionResult::RequiresExplicit { .. }),
            "Float64 -> Int32 should require explicit cast, got {:?}",
            result
        );

        let result = coerce_arrow_types(&DataType::Float64, &DataType::Int64);
        assert!(
            matches!(result, CoercionResult::RequiresExplicit { .. }),
            "Float64 -> Int64 should require explicit cast, got {:?}",
            result
        );

        let result = coerce_arrow_types(&DataType::Float32, &DataType::Int32);
        assert!(
            matches!(result, CoercionResult::RequiresExplicit { .. }),
            "Float32 -> Int32 should require explicit cast, got {:?}",
            result
        );
    }

    #[test]
    fn test_coerce_int_to_float_auto() {
        use arrow::datatypes::DataType;

        let result = coerce_arrow_types(&DataType::Int32, &DataType::Float64);
        assert!(
            matches!(result, CoercionResult::AutoCoerce { .. }),
            "Int32 -> Float64 should auto-coerce, got {:?}",
            result
        );
    }

    #[test]
    fn test_coerce_time32_to_time64_picks_finer_precision() {
        use arrow::datatypes::{DataType, TimeUnit};

        let result = coerce_arrow_types(
            &DataType::Time32(TimeUnit::Second),
            &DataType::Time64(TimeUnit::Nanosecond),
        );
        match result {
            CoercionResult::AutoCoerce { target, .. } => {
                assert_eq!(target, DataType::Time64(TimeUnit::Nanosecond));
            }
            other => panic!("Expected AutoCoerce, got {:?}", other),
        }

        let result2 = coerce_arrow_types(
            &DataType::Time64(TimeUnit::Microsecond),
            &DataType::Time32(TimeUnit::Millisecond),
        );
        match result2 {
            CoercionResult::AutoCoerce { target, .. } => {
                assert_eq!(target, DataType::Time64(TimeUnit::Microsecond));
            }
            other => panic!("Expected AutoCoerce, got {:?}", other),
        }
    }

    #[test]
    fn test_validate_table_name_allows_dots() {
        assert!(validate_table_name("my.table").is_ok());
        assert!(validate_table_name("schema.name").is_ok());
        // But path traversal via dots is still blocked
        assert!(validate_table_name("..sneaky").is_err());
        assert!(validate_table_name("a..b").is_err());
    }

    #[test]
    fn test_null_semantics_hashset_perf() {
        let ns = NullSemantics::default();
        assert!(ns.is_null("NULL"));
        assert!(ns.is_null("null"));
        assert!(!ns.is_null(""));
        assert!(!ns.is_null("something"));
    }

    #[test]
    fn test_null_semantics_roundtrip_serde() {
        let ns = NullSemantics::legacy();
        let json = serde_json::to_string(&ns).unwrap();
        let deserialized: NullSemantics = serde_json::from_str(&json).unwrap();
        assert!(deserialized.is_null("NULL"));
        assert!(deserialized.is_null("null"));
        assert!(deserialized.is_null(""));
        assert!(deserialized.treat_empty_as_null);
    }

    #[test]
    fn test_null_semantics_additional_nulls_hashset_sync() {
        let ns = NullSemantics::default().with_additional_nulls(&["N/A", "n/a", "-"]);
        assert!(ns.is_null("N/A"));
        assert!(ns.is_null("n/a"));
        assert!(ns.is_null("-"));
        assert!(ns.is_null("NULL"));
        assert!(!ns.is_null("valid"));
    }

    #[test]
    fn test_null_semantics_disable_treat_empty_cleans_up() {
        let ns = NullSemantics::default()
            .with_treat_empty_as_null(true)
            .with_treat_empty_as_null(false);

        assert!(!ns.treat_empty_as_null);
        assert!(
            !ns.is_null(""),
            "Empty string must not be null after disabling treat_empty_as_null"
        );
        assert!(!ns.null_values.contains(&String::new()));
        assert!(!ns.null_set.contains(&String::new()));

        assert!(ns.is_null("NULL"), "Standard null literals must still work");
        assert!(ns.is_null("null"));
    }

    // ========== Regression tests for impossible DateRange ==========

    #[test]
    fn test_to_glob_pattern_impossible_range() {
        let range = DateRange::new(
            Some(NaiveDate::from_ymd_opt(2025, 6, 1).unwrap()),
            Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
        );
        let pattern = range.to_glob_pattern("data/orders");
        assert!(
            pattern.contains("__dh_no_match__"),
            "Impossible range must return a no-match sentinel pattern, got: {}",
            pattern,
        );
    }

    #[test]
    fn test_to_pattern_list_impossible_range() {
        let range = DateRange::new(
            Some(NaiveDate::from_ymd_opt(2025, 6, 1).unwrap()),
            Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
        );
        let patterns = range.to_pattern_list("data/orders");
        assert!(
            patterns.is_empty(),
            "Impossible range must produce no patterns, got {:?}",
            patterns,
        );
    }

    #[test]
    fn test_is_impossible_start_after_end() {
        let range = DateRange::new(
            Some(NaiveDate::from_ymd_opt(2025, 1, 1).unwrap()),
            Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
        );
        assert!(range.is_impossible());
    }

    #[test]
    fn test_is_impossible_equal_bounds() {
        let range = DateRange::new(
            Some(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap()),
            Some(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap()),
        );
        assert!(
            !range.is_impossible(),
            "Equal bounds are a valid single-day range"
        );
    }

    #[test]
    fn test_is_impossible_open_bounds() {
        assert!(!DateRange::new(None, None).is_impossible());
        assert!(
            !DateRange::new(Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()), None)
                .is_impossible()
        );
        assert!(
            !DateRange::new(None, Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()))
                .is_impossible()
        );
    }

    #[test]
    fn test_impossible_range_returns_no_match_pattern() {
        let range = DateRange::new(
            Some(NaiveDate::from_ymd_opt(2025, 12, 1).unwrap()),
            Some(NaiveDate::from_ymd_opt(2025, 1, 1).unwrap()),
        );
        assert!(range.is_impossible());
        let pattern = range.to_glob_pattern("data/orders");
        assert!(
            pattern.contains("__dh_no_match__"),
            "Impossible date range must return a no-match sentinel pattern, got: {}",
            pattern,
        );
    }

    #[test]
    fn test_job_type_from_str_known_values() {
        assert!(matches!("sync".parse::<JobType>(), Ok(JobType::Sync)));
        assert!(matches!(
            "upgrade_to_warm".parse::<JobType>(),
            Ok(JobType::UpgradeToWarm)
        ));
        assert!(matches!(
            "upgrade_to_hot".parse::<JobType>(),
            Ok(JobType::UpgradeToHot)
        ));
        assert!(matches!(
            "downgrade_to_warm".parse::<JobType>(),
            Ok(JobType::DowngradeToWarm)
        ));
        assert!(matches!(
            "downgrade_to_cold".parse::<JobType>(),
            Ok(JobType::DowngradeToCold)
        ));
        assert!(matches!(
            "fst_rebuild".parse::<JobType>(),
            Ok(JobType::FstRebuild)
        ));
        assert!(matches!(
            "schema_snapshot".parse::<JobType>(),
            Ok(JobType::SchemaSnapshot)
        ));
        assert!(matches!(
            "derived_refresh".parse::<JobType>(),
            Ok(JobType::DerivedRefresh)
        ));
    }

    #[test]
    fn test_job_type_from_str_unknown_returns_err() {
        assert!("unknown_type".parse::<JobType>().is_err());
        assert!("".parse::<JobType>().is_err());
        assert!("SYNC_V2".parse::<JobType>().is_err());
    }

    #[test]
    fn test_job_status_from_str_known_values() {
        assert!(matches!(
            "pending".parse::<JobStatus>(),
            Ok(JobStatus::Pending)
        ));
        assert!(matches!(
            "running".parse::<JobStatus>(),
            Ok(JobStatus::Running)
        ));
        assert!(matches!(
            "completed".parse::<JobStatus>(),
            Ok(JobStatus::Completed)
        ));
        assert!(matches!(
            "failed".parse::<JobStatus>(),
            Ok(JobStatus::Failed)
        ));
        assert!(matches!(
            "cancelled".parse::<JobStatus>(),
            Ok(JobStatus::Cancelled)
        ));
    }

    #[test]
    fn test_job_status_from_str_unknown_returns_err() {
        assert!("unknown_status".parse::<JobStatus>().is_err());
        assert!("".parse::<JobStatus>().is_err());
    }
}
