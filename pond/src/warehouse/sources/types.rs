//! Data Source Types
//!
//! Defines the unified source abstraction for the multi-source warehouse.
//! Each source has a name, backend type, and configuration specific to its type.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

use crate::warehouse::types::{ExternalSourceConfig, NullSemantics, SourceType, StorageType};

// ============================================================================
// Consistency Level
// ============================================================================

/// Consistency level for synced data sources.
///
/// Controls how quickly writes become visible to readers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ConsistencyLevel {
    /// Eventual consistency: Batch writes, seconds-minutes delay.
    /// Best for dashboards and analytics where slight staleness is acceptable.
    #[default]
    Eventual,
    
    /// Read-after-write consistency: Flush on write, immediate for writer.
    /// Users see their own writes immediately but others may see stale data.
    ReadAfterWrite,
    
    /// Strong consistency: Synchronous write + barrier.
    /// All readers see writes immediately. Highest latency.
    Strong,
}

impl ConsistencyLevel {
    /// Check if this is eventual consistency.
    pub fn is_eventual(&self) -> bool {
        matches!(self, Self::Eventual)
    }
    
    /// Check if this requires immediate flush on write.
    pub fn requires_flush(&self) -> bool {
        matches!(self, Self::ReadAfterWrite | Self::Strong)
    }
    
    /// Check if this requires read barriers.
    pub fn requires_barrier(&self) -> bool {
        matches!(self, Self::Strong)
    }
}

impl std::fmt::Display for ConsistencyLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsistencyLevel::Eventual => write!(f, "eventual"),
            ConsistencyLevel::ReadAfterWrite => write!(f, "read_after_write"),
            ConsistencyLevel::Strong => write!(f, "strong"),
        }
    }
}

impl std::str::FromStr for ConsistencyLevel {
    type Err = String;
    
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "eventual" => Ok(Self::Eventual),
            "read_after_write" | "readafterwrite" | "read-after-write" => Ok(Self::ReadAfterWrite),
            "strong" => Ok(Self::Strong),
            _ => Err(format!("Invalid consistency level: {}", s)),
        }
    }
}

// ============================================================================
// Storage Tier
// ============================================================================

/// The storage tier for a data source.
///
/// This determines how data is accessed and where it's stored:
/// - `Cold`: Data is queried directly at the source (federated query)
/// - `Warm`: Data is synced to Parquet on R2/S3 with local indexes
/// - `Hot`: Data is synced to ClickHouse for maximum query speed
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum StorageTier {
    /// Data is queried directly at the source (federated query).
    /// No data replication, queries execute directly on the source.
    /// Uses DataFusion with table providers for external databases.
    #[default]
    Cold,
    
    /// Data is synced to Parquet files on R2/S3 with local indexes.
    /// Medium query speed, low storage cost.
    /// Uses DataFusion with local index pruning for efficient queries.
    Warm,
    
    /// Data is synced into ClickHouse MergeTree tables.
    /// Fastest query performance, higher storage cost.
    /// Uses native ClickHouse queries with full indexing.
    Hot,
}

impl StorageTier {
    /// Check if this is cold tier.
    pub fn is_cold(&self) -> bool {
        matches!(self, Self::Cold)
    }
    
    /// Check if this is warm tier.
    pub fn is_warm(&self) -> bool {
        matches!(self, Self::Warm)
    }
    
    /// Check if this is hot tier.
    pub fn is_hot(&self) -> bool {
        matches!(self, Self::Hot)
    }
    
    /// Check if data is synced (warm or hot).
    pub fn has_synced_data(&self) -> bool {
        matches!(self, Self::Warm | Self::Hot)
    }
}

impl std::fmt::Display for StorageTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageTier::Cold => write!(f, "cold"),
            StorageTier::Warm => write!(f, "warm"),
            StorageTier::Hot => write!(f, "hot"),
        }
    }
}

impl std::str::FromStr for StorageTier {
    type Err = String;
    
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "cold" => Ok(Self::Cold),
            "warm" => Ok(Self::Warm),
            "hot" => Ok(Self::Hot),
            _ => Err(format!("Invalid storage tier: {}", s)),
        }
    }
}

/// Parse a storage tier string with a warning on unrecognized values.
///
/// Returns the parsed tier, or `StorageTier::default()` (Cold) if the value
/// is unrecognized. Logs a warning with the raw string so silent data
/// corruption is visible in production.
pub fn parse_storage_tier(raw: &str) -> StorageTier {
    raw.parse().unwrap_or_else(|_| {
        tracing::warn!(raw = %raw, "Unrecognized storage tier, defaulting to Cold");
        StorageTier::default()
    })
}

// ============================================================================
// Sync Scope
// ============================================================================

/// The scope of data to sync from a source.
///
/// - `Full`: Sync all data from the source.
/// - `TimeBased`: Only sync data older than a specified number of days.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SyncScope {
    /// Sync all data from the source.
    #[default]
    Full,
    /// Only sync data older than the specified number of days.
    /// This is useful for sources where recent data should stay in the original store.
    TimeBased {
        older_than_days: u32,
    },
}

impl SyncScope {
    /// Check if this is a full sync scope.
    pub fn is_full(&self) -> bool {
        matches!(self, Self::Full)
    }

    /// Check if this is a time-based sync scope.
    pub fn is_time_based(&self) -> bool {
        matches!(self, Self::TimeBased { .. })
    }

    /// Get the older_than_days value if this is a time-based scope.
    pub fn older_than_days(&self) -> Option<u32> {
        match self {
            Self::TimeBased { older_than_days } => Some(*older_than_days),
            _ => None,
        }
    }
}

impl std::fmt::Display for SyncScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncScope::Full => write!(f, "full"),
            SyncScope::TimeBased { older_than_days } => {
                write!(f, "time_based({}d)", older_than_days)
            }
        }
    }
}

// ============================================================================
// Storage Tier Policy
// ============================================================================

/// Policy for how a source's data moves between storage tiers.
///
/// - `Fixed`: Data stays at whatever tier the source is set to. Manual upgrade/downgrade only.
/// - `Lifecycle`: Data automatically transitions between tiers based on age.
/// - `AccessBased`: Data automatically transitions between tiers based on query frequency.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StorageTierPolicy {
    /// Data stays at the current tier. Manual upgrade/downgrade only.
    #[default]
    Fixed,
    /// Data automatically transitions between tiers based on partition age.
    Lifecycle {
        transitions: Vec<TierTransition>,
    },
    /// Data automatically transitions between tiers based on access frequency.
    /// Frequently queried sources are promoted to hotter tiers; infrequently
    /// queried sources are demoted to colder tiers. One step at a time.
    AccessBased {
        sensitivity: AccessSensitivity,
    },
}

/// A tier transition rule for lifecycle policies.
///
/// Specifies that data older than `after_days` should be moved to `tier`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TierTransition {
    /// Number of days after which data should move to this tier.
    pub after_days: u32,
    /// Target storage tier.
    pub tier: StorageTier,
}

// ============================================================================
// Access-Based Tier Policy
// ============================================================================

/// Sensitivity level for access-based tier policies.
///
/// Controls how aggressively the system promotes/demotes sources based on
/// query frequency. Each level maps to internal thresholds for the evaluation
/// window, demotion threshold, and promotion threshold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AccessSensitivity {
    /// Quick to react: 7-day window, demote below 5 queries, promote above 20.
    Aggressive,
    /// Balanced: 14-day window, demote below 5 queries, promote above 50.
    #[default]
    Moderate,
    /// Slow to change: 30-day window, demote below 3 queries, promote above 100.
    Conservative,
}

/// Internal thresholds for an access sensitivity level.
#[derive(Debug, Clone, Copy)]
pub struct AccessThresholds {
    /// Number of days to look back when counting queries.
    pub window_days: u32,
    /// Demote the source one tier colder if query count is below this value.
    pub demote_below: u64,
    /// Promote the source one tier hotter if query count is above this value.
    pub promote_above: u64,
}

impl AccessSensitivity {
    /// Return the internal thresholds for this sensitivity level.
    pub fn thresholds(&self) -> AccessThresholds {
        match self {
            Self::Aggressive => AccessThresholds {
                window_days: 7,
                demote_below: 5,
                promote_above: 20,
            },
            Self::Moderate => AccessThresholds {
                window_days: 14,
                demote_below: 5,
                promote_above: 50,
            },
            Self::Conservative => AccessThresholds {
                window_days: 30,
                demote_below: 3,
                promote_above: 100,
            },
        }
    }

    /// Maximum evaluation window across all sensitivity levels (used for cleanup).
    pub fn max_window_days() -> u32 {
        30
    }
}

impl std::fmt::Display for AccessSensitivity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AccessSensitivity::Aggressive => write!(f, "aggressive"),
            AccessSensitivity::Moderate => write!(f, "moderate"),
            AccessSensitivity::Conservative => write!(f, "conservative"),
        }
    }
}

impl std::str::FromStr for AccessSensitivity {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "aggressive" => Ok(Self::Aggressive),
            "moderate" => Ok(Self::Moderate),
            "conservative" => Ok(Self::Conservative),
            _ => Err(format!("Invalid access sensitivity: {}", s)),
        }
    }
}

// ============================================================================
// Registered Source
// ============================================================================

/// A registered data source for a project.
///
/// Each source has a user-defined name that can be used in SQL queries
/// to reference tables from that source (e.g., `stripe.customers`).
#[derive(Debug, Clone)]
pub struct RegisteredSource {
    /// Unique identifier for the source.
    pub id: Uuid,
    /// Project this source belongs to.
    pub project_id: Uuid,
    /// User-defined name for referencing in queries (e.g., "stripe", "events", "metrics").
    pub name: String,
    /// Type of source (Stripe, PostgreSQL, ExternalParquet, etc.).
    pub source_type: SourceType,
    /// Current storage tier.
    pub tier: StorageTier,
    /// The execution backend for this source.
    pub backend: SourceBackend,
    /// Source-specific configuration.
    pub config: SourceConfig,
    /// Whether the source is enabled.
    pub enabled: bool,
    /// Whether this source supports CDC/WAL-based change tracking.
    /// Sources without CDC (like Stripe API, REST endpoints) can only use cold tier.
    pub supports_cdc: bool,
    /// Consistency level for synced data (warm/hot tiers).
    pub consistency_level: ConsistencyLevel,
    /// Scope of data to sync from this source.
    pub sync_scope: SyncScope,
    /// Policy for automatic tier transitions.
    pub storage_tier_policy: StorageTierPolicy,
    /// When the source was created.
    pub created_at: DateTime<Utc>,
    /// When the source was last updated.
    pub updated_at: DateTime<Utc>,
    /// If set, this source is a warm backing for the referenced hot source.
    pub backs_source_id: Option<Uuid>,
}

impl RegisteredSource {
    /// Check if this source uses object storage (S3/R2).
    pub fn is_object_storage(&self) -> bool {
        matches!(self.backend, SourceBackend::ObjectStorage { .. })
    }

    /// Check if this source uses native ClickHouse tables.
    pub fn is_native_clickhouse(&self) -> bool {
        matches!(self.backend, SourceBackend::ClickHouseNative { .. })
    }

    /// Check if this source is an external database.
    pub fn is_external_database(&self) -> bool {
        matches!(self.backend, SourceBackend::ExternalDatabase { .. })
    }

    /// Get the storage type for query routing.
    pub fn storage_type(&self) -> StorageType {
        match &self.backend {
            SourceBackend::ClickHouseNative { .. } => StorageType::NativeClickHouse,
            SourceBackend::ObjectStorage { .. } => StorageType::ObjectStorage,
            SourceBackend::ExternalDatabase { .. } => StorageType::ObjectStorage, // Route through ClickHouse table functions
            SourceBackend::ExternalApi { .. } => StorageType::External, // Query-in-place sources
        }
    }
    
    /// Check if this source can be upgraded to a higher tier.
    /// 
    /// Sources without CDC support (like REST APIs) can only operate in cold tier.
    pub fn can_upgrade(&self) -> bool {
        self.supports_cdc
    }
    
    /// Check if the current tier is valid for this source.
    /// 
    /// Non-CDC sources can only be cold tier.
    pub fn is_tier_valid(&self) -> bool {
        if self.supports_cdc {
            true // CDC sources can use any tier
        } else {
            self.tier.is_cold() // Non-CDC sources must be cold
        }
    }
}

// ============================================================================
// Source Backend
// ============================================================================

/// The execution backend for a source.
///
/// This determines where the data physically lives and how it's accessed.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceBackend {
    /// Native ClickHouse MergeTree tables.
    ///
    /// Data is synced into ClickHouse for fast querying with native indexes.
    ClickHouseNative {
        /// ClickHouse database name.
        database: String,
        /// Table name prefix for this source's tables.
        table_prefix: String,
    },

    /// S3/R2/GCS Parquet files.
    ///
    /// Data is stored in object storage and queried via ClickHouse's s3() function.
    ObjectStorage {
        /// Bucket URL (e.g., "s3://bucket-name" or "https://account.r2.cloudflarestorage.com/bucket").
        bucket_url: String,
        /// Prefix path within the bucket.
        prefix: String,
        /// Access key ID (optional for public buckets).
        access_key_id: Option<String>,
        /// Secret access key (optional for public buckets).
        /// Note: This is skipped in serialization for security.
        #[serde(skip)]
        secret_access_key: Option<String>,
    },

    /// External database connection.
    ///
    /// Data is queried via ClickHouse table functions (postgresql(), mysql(), etc.).
    ExternalDatabase {
        /// Database type.
        db_type: ExternalDbType,
        /// Connection host.
        host: String,
        /// Connection port.
        port: u16,
        /// Database name.
        database: String,
        /// Username.
        username: String,
        /// Password (skipped in serialization for security).
        #[serde(skip)]
        password: Option<String>,
        /// Optional schema (for PostgreSQL).
        schema: Option<String>,
    },

    /// External API data source (cold tier).
    ///
    /// Data is fetched on-demand from external APIs and materialized as
    /// Arrow RecordBatches for query execution. No data is stored in ClickHouse.
    /// Uses TTL caching to minimize repeated API calls.
    ///
    /// Examples: Google Sheets, Airtable, Notion, REST APIs
    ExternalApi {
        /// The source type (e.g., GoogleSheets, Airtable).
        source_type: crate::warehouse::types::SourceType,
        /// Source-specific configuration as JSON.
        /// Contains connection details, credentials, etc.
        config_json: String,
        /// Cache TTL in seconds (0 = no caching).
        cache_ttl_secs: u64,
    },
}

impl SourceBackend {
    /// Create a new ClickHouse native backend.
    pub fn clickhouse(database: impl Into<String>, table_prefix: impl Into<String>) -> Self {
        Self::ClickHouseNative {
            database: database.into(),
            table_prefix: table_prefix.into(),
        }
    }

    /// Create a new object storage backend.
    pub fn object_storage(bucket_url: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self::ObjectStorage {
            bucket_url: bucket_url.into(),
            prefix: prefix.into(),
            access_key_id: None,
            secret_access_key: None,
        }
    }

    /// Create a new external API backend.
    pub fn external_api(
        source_type: crate::warehouse::types::SourceType,
        config_json: impl Into<String>,
        cache_ttl_secs: u64,
    ) -> Self {
        Self::ExternalApi {
            source_type,
            config_json: config_json.into(),
            cache_ttl_secs,
        }
    }

    /// Check if this is an external API backend.
    pub fn is_external_api(&self) -> bool {
        matches!(self, Self::ExternalApi { .. })
    }

    /// Check if this is a cold tier source (no data stored).
    pub fn is_cold_tier(&self) -> bool {
        matches!(self, Self::ExternalApi { .. })
    }
}

/// External database types supported via ClickHouse table functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExternalDbType {
    /// PostgreSQL database
    PostgreSQL,
    /// MySQL/MariaDB database
    MySQL,
    /// SQLite embedded database
    SQLite,
    /// MongoDB document database
    MongoDB,
    /// Microsoft SQL Server
    SqlServer,
    /// Google BigQuery
    BigQuery,
    /// Amazon Redshift
    Redshift,
    /// Snowflake data warehouse
    Snowflake,
}

impl std::fmt::Display for ExternalDbType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExternalDbType::PostgreSQL => write!(f, "postgresql"),
            ExternalDbType::MySQL => write!(f, "mysql"),
            ExternalDbType::SQLite => write!(f, "sqlite"),
            ExternalDbType::MongoDB => write!(f, "mongodb"),
            ExternalDbType::SqlServer => write!(f, "sqlserver"),
            ExternalDbType::BigQuery => write!(f, "bigquery"),
            ExternalDbType::Redshift => write!(f, "redshift"),
            ExternalDbType::Snowflake => write!(f, "snowflake"),
        }
    }
}

// ============================================================================
// Source Configuration
// ============================================================================

/// Configuration specific to the source type.
///
/// Different source types require different configuration options.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "config_type", rename_all = "snake_case")]
pub enum SourceConfig {
    /// Configuration for external Parquet sources.
    Parquet {
        /// The external source configuration with indexing settings.
        #[serde(flatten)]
        config: ExternalSourceConfig,
    },

    /// Configuration for synced sources (Stripe, HubSpot, etc.).
    Synced {
        /// Sync interval in seconds.
        sync_interval_secs: u64,
        /// Tables to sync (empty = all tables).
        #[serde(default)]
        tables: Vec<String>,
    },

    /// Configuration for direct database connections.
    Database {
        /// Schema to use (for databases that support schemas).
        schema: Option<String>,
        /// Tables to expose (empty = all tables).
        #[serde(default)]
        tables: Vec<String>,
    },

    /// Configuration for OAuth-based SaaS sources (e.g., Salesforce, HubSpot).
    OAuth {
        /// OAuth client ID.
        client_id: String,
        /// OAuth token endpoint URL.
        token_endpoint: String,
        /// Required OAuth scopes.
        #[serde(default)]
        scopes: Vec<String>,
        /// Sync interval in seconds.
        #[serde(default = "default_sync_interval")]
        sync_interval_secs: u64,
        /// Tables to sync (empty = all tables).
        #[serde(default)]
        tables: Vec<String>,
    },

    /// Configuration for file-based sources (CSV, JSON, Excel, XML).
    File {
        /// Path or URL to the file(s).
        path: String,
        /// File format type (csv, json, excel, xml).
        format: String,
        /// Delimiter for CSV files (default: ,).
        #[serde(default)]
        delimiter: Option<char>,
        /// Whether the file has a header row.
        #[serde(default = "default_true")]
        has_header: bool,
        /// Table name to use for the file.
        table_name: Option<String>,
        /// NULL value handling semantics.
        ///
        /// By default, empty strings are valid values (NOT NULL) and only
        /// explicit NULL literals like "NULL" are treated as NULL.
        #[serde(default)]
        null_semantics: NullSemantics,
    },

    /// Configuration for streaming sources (Kafka, Kinesis).
    Streaming {
        /// Broker/endpoint addresses.
        brokers: Vec<String>,
        /// Topic(s) or stream name(s) to consume.
        topics: Vec<String>,
        /// Consumer group ID.
        consumer_group: String,
        /// Starting offset (earliest, latest, or specific timestamp).
        #[serde(default = "default_start_offset")]
        start_offset: String,
        /// Maximum messages to poll per batch.
        #[serde(default = "default_max_poll_messages")]
        max_poll_messages: usize,
    },

    /// ClickHouse database connection.
    /// Reads tables from a ClickHouse database as a data source.
    ClickHouseDatabase {
        /// ClickHouse host address.
        host: String,
        /// ClickHouse HTTP port (typically 8123).
        port: u16,
        /// Database name to read from.
        database: String,
        /// Username for authentication.
        username: String,
        /// Password for authentication (not serialized).
        #[serde(skip)]
        password: Option<String>,
        /// User-selected tables to sync.
        tables: Vec<String>,
    },

    /// Configuration for blockchain sources (Ethereum, Solana, etc.).
    Blockchain {
        /// RPC endpoint URL.
        rpc_url: String,
        /// Blockchain network type.
        chain_type: String,
        /// Block range to sync (start block).
        start_block: Option<u64>,
        /// Block range to sync (end block, None = latest).
        end_block: Option<u64>,
        /// Contract addresses to filter events/transactions.
        #[serde(default)]
        contract_addresses: Vec<String>,
        /// Event signatures to filter (e.g., Transfer(address,address,uint256)).
        #[serde(default)]
        event_signatures: Vec<String>,
    },
}

fn default_sync_interval() -> u64 {
    3600 // 1 hour
}

fn default_true() -> bool {
    true
}

fn default_start_offset() -> String {
    "latest".to_string()
}

fn default_max_poll_messages() -> usize {
    1000
}

impl SourceConfig {
    /// Create a new Parquet source config.
    pub fn parquet(config: ExternalSourceConfig) -> Self {
        Self::Parquet { config }
    }

    /// Create a new synced source config.
    pub fn synced(interval: Duration) -> Self {
        Self::Synced {
            sync_interval_secs: interval.as_secs(),
            tables: Vec::new(),
        }
    }

    /// Create a new database source config.
    pub fn database(schema: Option<String>) -> Self {
        Self::Database {
            schema,
            tables: Vec::new(),
        }
    }

    /// Get the sync interval for synced sources.
    pub fn sync_interval(&self) -> Option<Duration> {
        match self {
            Self::Synced { sync_interval_secs, .. } => Some(Duration::from_secs(*sync_interval_secs)),
            Self::OAuth { sync_interval_secs, .. } => Some(Duration::from_secs(*sync_interval_secs)),
            _ => None,
        }
    }

    /// Get the Parquet config if this is a Parquet source.
    pub fn as_parquet(&self) -> Option<&ExternalSourceConfig> {
        match self {
            Self::Parquet { config } => Some(config),
            _ => None,
        }
    }

    /// Create a new OAuth source config.
    pub fn oauth(client_id: String, token_endpoint: String, interval: Duration) -> Self {
        Self::OAuth {
            client_id,
            token_endpoint,
            scopes: Vec::new(),
            sync_interval_secs: interval.as_secs(),
            tables: Vec::new(),
        }
    }

    /// Create a new file source config.
    pub fn file(path: String, format: String) -> Self {
        Self::File {
            path,
            format,
            delimiter: None,
            has_header: true,
            table_name: None,
            null_semantics: NullSemantics::default(),
        }
    }

    /// Create a new file source config with legacy NULL semantics.
    ///
    /// In legacy mode, empty strings are treated as NULL values.
    pub fn file_legacy(path: String, format: String) -> Self {
        Self::File {
            path,
            format,
            delimiter: None,
            has_header: true,
            table_name: None,
            null_semantics: NullSemantics::legacy(),
        }
    }

    /// Create a new streaming source config.
    pub fn streaming(brokers: Vec<String>, topics: Vec<String>, consumer_group: String) -> Self {
        Self::Streaming {
            brokers,
            topics,
            consumer_group,
            start_offset: "latest".to_string(),
            max_poll_messages: 1000,
        }
    }

    /// Create a new blockchain source config.
    pub fn blockchain(rpc_url: String, chain_type: String) -> Self {
        Self::Blockchain {
            rpc_url,
            chain_type,
            start_block: None,
            end_block: None,
            contract_addresses: Vec::new(),
            event_signatures: Vec::new(),
        }
    }

    /// Check if this is an OAuth-based source.
    pub fn is_oauth(&self) -> bool {
        matches!(self, Self::OAuth { .. })
    }

    /// Check if this is a streaming source.
    pub fn is_streaming(&self) -> bool {
        matches!(self, Self::Streaming { .. })
    }

    /// Check if this is a file-based source.
    pub fn is_file(&self) -> bool {
        matches!(self, Self::File { .. })
    }

    /// Check if this is a blockchain source.
    pub fn is_blockchain(&self) -> bool {
        matches!(self, Self::Blockchain { .. })
    }
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self::Synced {
            sync_interval_secs: 3600, // 1 hour default
            tables: Vec::new(),
        }
    }
}

// ============================================================================
// Table Info
// ============================================================================

/// Information about a table within a source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceTableInfo {
    /// Table name.
    pub name: String,
    /// Fully qualified name for querying (source.table).
    pub qualified_name: String,
    /// Column information.
    pub columns: Vec<SourceColumnInfo>,
    /// Estimated row count (if available).
    pub estimated_rows: Option<u64>,
    /// Last sync time (for synced sources).
    pub last_synced_at: Option<DateTime<Utc>>,
}

/// Information about a column within a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceColumnInfo {
    /// Column name.
    pub name: String,
    /// Data type.
    pub data_type: String,
    /// Whether the column is nullable.
    pub nullable: bool,
    /// Optional description.
    pub description: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_backend_helpers() {
        let ch = SourceBackend::clickhouse("reiver", "stripe_");
        assert!(matches!(ch, SourceBackend::ClickHouseNative { .. }));

        let s3 = SourceBackend::object_storage("s3://bucket", "data/");
        assert!(matches!(s3, SourceBackend::ObjectStorage { .. }));
    }

    #[test]
    fn test_registered_source_storage_type() {
        let source = RegisteredSource {
            id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            name: "test".to_string(),
            source_type: SourceType::ExternalParquet,
            tier: StorageTier::Cold,
            backend: SourceBackend::object_storage("s3://bucket", "data/"),
            config: SourceConfig::default(),
            enabled: true,
            supports_cdc: true,
            consistency_level: ConsistencyLevel::Eventual,
            sync_scope: SyncScope::default(),
            storage_tier_policy: StorageTierPolicy::default(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            backs_source_id: None,
        };

        assert!(source.is_object_storage());
        assert!(!source.is_native_clickhouse());
        assert_eq!(source.storage_type(), StorageType::ObjectStorage);
    }
    
    #[test]
    fn test_consistency_level() {
        // Test eventual consistency
        assert!(ConsistencyLevel::Eventual.is_eventual());
        assert!(!ConsistencyLevel::Eventual.requires_flush());
        assert!(!ConsistencyLevel::Eventual.requires_barrier());
        
        // Test read-after-write consistency
        assert!(!ConsistencyLevel::ReadAfterWrite.is_eventual());
        assert!(ConsistencyLevel::ReadAfterWrite.requires_flush());
        assert!(!ConsistencyLevel::ReadAfterWrite.requires_barrier());
        
        // Test strong consistency
        assert!(!ConsistencyLevel::Strong.is_eventual());
        assert!(ConsistencyLevel::Strong.requires_flush());
        assert!(ConsistencyLevel::Strong.requires_barrier());
        
        // Test parsing
        assert_eq!("eventual".parse::<ConsistencyLevel>().unwrap(), ConsistencyLevel::Eventual);
        assert_eq!("read_after_write".parse::<ConsistencyLevel>().unwrap(), ConsistencyLevel::ReadAfterWrite);
        assert_eq!("strong".parse::<ConsistencyLevel>().unwrap(), ConsistencyLevel::Strong);
        
        // Test display
        assert_eq!(ConsistencyLevel::Eventual.to_string(), "eventual");
        assert_eq!(ConsistencyLevel::ReadAfterWrite.to_string(), "read_after_write");
        assert_eq!(ConsistencyLevel::Strong.to_string(), "strong");
    }
    
    #[test]
    fn test_source_cdc_support() {
        // CDC source can use any tier
        let cdc_source = RegisteredSource {
            id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            name: "postgres".to_string(),
            source_type: SourceType::PostgreSQL,
            tier: StorageTier::Warm,
            backend: SourceBackend::object_storage("s3://bucket", "data/"),
            config: SourceConfig::default(),
            enabled: true,
            supports_cdc: true,
            consistency_level: ConsistencyLevel::Eventual,
            sync_scope: SyncScope::default(),
            storage_tier_policy: StorageTierPolicy::default(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            backs_source_id: None,
        };
        assert!(cdc_source.can_upgrade());
        assert!(cdc_source.is_tier_valid());
        
        // Non-CDC source must be cold
        let non_cdc_cold = RegisteredSource {
            id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            name: "stripe".to_string(),
            source_type: SourceType::Stripe,
            tier: StorageTier::Cold,
            backend: SourceBackend::external_api(SourceType::Stripe, "{}", 3600),
            config: SourceConfig::default(),
            enabled: true,
            supports_cdc: false,
            consistency_level: ConsistencyLevel::Eventual,
            sync_scope: SyncScope::default(),
            storage_tier_policy: StorageTierPolicy::default(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            backs_source_id: None,
        };
        assert!(!non_cdc_cold.can_upgrade());
        assert!(non_cdc_cold.is_tier_valid()); // cold is valid for non-CDC
        
        // Non-CDC source with warm tier is invalid
        let non_cdc_warm = RegisteredSource {
            id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            name: "stripe".to_string(),
            source_type: SourceType::Stripe,
            tier: StorageTier::Warm,
            backend: SourceBackend::external_api(SourceType::Stripe, "{}", 3600),
            config: SourceConfig::default(),
            enabled: true,
            supports_cdc: false,
            consistency_level: ConsistencyLevel::Eventual,
            sync_scope: SyncScope::default(),
            storage_tier_policy: StorageTierPolicy::default(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            backs_source_id: None,
        };
        assert!(!non_cdc_warm.can_upgrade());
        assert!(!non_cdc_warm.is_tier_valid()); // warm is invalid for non-CDC
    }
    
    #[test]
    fn test_storage_tier() {
        // Test cold tier
        assert!(StorageTier::Cold.is_cold());
        assert!(!StorageTier::Cold.is_warm());
        assert!(!StorageTier::Cold.is_hot());
        assert!(!StorageTier::Cold.has_synced_data());
        
        // Test warm tier
        assert!(!StorageTier::Warm.is_cold());
        assert!(StorageTier::Warm.is_warm());
        assert!(!StorageTier::Warm.is_hot());
        assert!(StorageTier::Warm.has_synced_data());
        
        // Test hot tier
        assert!(!StorageTier::Hot.is_cold());
        assert!(!StorageTier::Hot.is_warm());
        assert!(StorageTier::Hot.is_hot());
        assert!(StorageTier::Hot.has_synced_data());
        
        // Test parsing
        assert_eq!("cold".parse::<StorageTier>().unwrap(), StorageTier::Cold);
        assert_eq!("warm".parse::<StorageTier>().unwrap(), StorageTier::Warm);
        assert_eq!("hot".parse::<StorageTier>().unwrap(), StorageTier::Hot);
        
        // Test display
        assert_eq!(StorageTier::Cold.to_string(), "cold");
        assert_eq!(StorageTier::Warm.to_string(), "warm");
        assert_eq!(StorageTier::Hot.to_string(), "hot");
    }

    #[test]
    fn test_source_config_sync_interval() {
        let synced = SourceConfig::synced(Duration::from_secs(1800));
        assert_eq!(synced.sync_interval(), Some(Duration::from_secs(1800)));

        let parquet = SourceConfig::parquet(ExternalSourceConfig::default());
        assert_eq!(parquet.sync_interval(), None);
    }

    // --- SyncScope tests ---

    #[test]
    fn test_sync_scope_full_defaults() {
        let scope = SyncScope::default();
        assert!(matches!(scope, SyncScope::Full));
    }

    #[test]
    fn test_sync_scope_is_full() {
        assert!(SyncScope::Full.is_full());
        assert!(!SyncScope::TimeBased { older_than_days: 30 }.is_full());
    }

    #[test]
    fn test_sync_scope_is_time_based() {
        assert!(SyncScope::TimeBased { older_than_days: 30 }.is_time_based());
        assert!(!SyncScope::Full.is_time_based());
    }

    #[test]
    fn test_sync_scope_older_than_days() {
        assert_eq!(
            SyncScope::TimeBased { older_than_days: 30 }.older_than_days(),
            Some(30)
        );
        assert_eq!(SyncScope::Full.older_than_days(), None);
        assert_eq!(
            SyncScope::TimeBased { older_than_days: 0 }.older_than_days(),
            Some(0)
        );
    }

    #[test]
    fn test_sync_scope_display() {
        assert_eq!(SyncScope::Full.to_string(), "full");
        assert_eq!(
            SyncScope::TimeBased { older_than_days: 30 }.to_string(),
            "time_based(30d)"
        );
        assert_eq!(
            SyncScope::TimeBased { older_than_days: 0 }.to_string(),
            "time_based(0d)"
        );
    }

    #[test]
    fn test_sync_scope_serde_roundtrip() {
        // Full variant
        let full = SyncScope::Full;
        let json = serde_json::to_string(&full).unwrap();
        let deserialized: SyncScope = serde_json::from_str(&json).unwrap();
        assert_eq!(full, deserialized);

        // TimeBased variant
        let time_based = SyncScope::TimeBased { older_than_days: 30 };
        let json = serde_json::to_string(&time_based).unwrap();
        let deserialized: SyncScope = serde_json::from_str(&json).unwrap();
        assert_eq!(time_based, deserialized);

        // Verify JSON shape uses snake_case
        assert!(json.contains("time_based"));
        assert!(json.contains("older_than_days"));
    }

    // --- AccessSensitivity tests ---

    #[test]
    fn test_access_sensitivity_thresholds() {
        let aggressive = AccessSensitivity::Aggressive.thresholds();
        assert_eq!(aggressive.window_days, 7);
        assert_eq!(aggressive.demote_below, 5);
        assert_eq!(aggressive.promote_above, 20);

        let moderate = AccessSensitivity::Moderate.thresholds();
        assert_eq!(moderate.window_days, 14);
        assert_eq!(moderate.demote_below, 5);
        assert_eq!(moderate.promote_above, 50);

        let conservative = AccessSensitivity::Conservative.thresholds();
        assert_eq!(conservative.window_days, 30);
        assert_eq!(conservative.demote_below, 3);
        assert_eq!(conservative.promote_above, 100);
    }

    #[test]
    fn test_access_sensitivity_display_and_parse() {
        assert_eq!(AccessSensitivity::Aggressive.to_string(), "aggressive");
        assert_eq!(AccessSensitivity::Moderate.to_string(), "moderate");
        assert_eq!(AccessSensitivity::Conservative.to_string(), "conservative");

        assert_eq!("aggressive".parse::<AccessSensitivity>().unwrap(), AccessSensitivity::Aggressive);
        assert_eq!("moderate".parse::<AccessSensitivity>().unwrap(), AccessSensitivity::Moderate);
        assert_eq!("conservative".parse::<AccessSensitivity>().unwrap(), AccessSensitivity::Conservative);
        assert!("invalid".parse::<AccessSensitivity>().is_err());
    }

    #[test]
    fn test_access_sensitivity_default() {
        assert_eq!(AccessSensitivity::default(), AccessSensitivity::Moderate);
    }

    #[test]
    fn test_storage_tier_policy_access_based_serde() {
        let policy = StorageTierPolicy::AccessBased {
            sensitivity: AccessSensitivity::Moderate,
        };
        let json = serde_json::to_string(&policy).unwrap();
        let deserialized: StorageTierPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(policy, deserialized);

        // Verify JSON shape
        assert!(json.contains("access_based"));
        assert!(json.contains("moderate"));

        // Verify deserialization from explicit JSON
        let from_json: StorageTierPolicy = serde_json::from_str(
            r#"{"type": "access_based", "sensitivity": "aggressive"}"#
        ).unwrap();
        assert_eq!(
            from_json,
            StorageTierPolicy::AccessBased { sensitivity: AccessSensitivity::Aggressive }
        );
    }

    #[test]
    fn test_max_window_days() {
        assert_eq!(AccessSensitivity::max_window_days(), 30);
    }

    #[test]
    fn test_parse_storage_tier_valid() {
        assert_eq!(parse_storage_tier("cold"), StorageTier::Cold);
        assert_eq!(parse_storage_tier("warm"), StorageTier::Warm);
        assert_eq!(parse_storage_tier("hot"), StorageTier::Hot);
        assert_eq!(parse_storage_tier("Cold"), StorageTier::Cold);
        assert_eq!(parse_storage_tier("HOT"), StorageTier::Hot);
    }

    #[test]
    fn test_parse_storage_tier_invalid_returns_default() {
        assert_eq!(parse_storage_tier("unknown"), StorageTier::Cold);
        assert_eq!(parse_storage_tier(""), StorageTier::Cold);
        assert_eq!(parse_storage_tier("lukewarm"), StorageTier::Cold);
    }
}
