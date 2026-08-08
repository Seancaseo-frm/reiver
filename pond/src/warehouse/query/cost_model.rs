//! Cost Model for Federated Query Planning
//!
//! Provides cost estimation for cross-source JOINs based on source access
//! characteristics like latency, bandwidth, parallelism, and rate limits.
//!
//! # Source Access Profiles
//!
//! Each source type has different access characteristics:
//! - **ClickHouse native**: 1-10ms latency, 10+ GB/s bandwidth, high parallelism
//! - **Parquet in S3**: 50-200ms latency, 100+ MB/s bandwidth, high parallelism
//! - **PostgreSQL**: 10-50ms latency, 50-200 MB/s bandwidth, medium parallelism
//! - **Stripe API**: 100-500ms latency, 1-10 MB/s bandwidth, low parallelism (rate limited)
//!
//! # Cost Units
//!
//! Costs are measured in milliseconds for easy interpretation:
//! - `network_io_cost`: Time to transfer data over network
//! - `compute_cost`: Time for CPU/processing
//! - `memory_cost`: Memory pressure penalty (derived from memory fraction, scaled to ms equivalent)

use ahash::{AHashMap, AHashSet};
use std::ops::RangeInclusive;

use serde::{Deserialize, Serialize};

use crate::warehouse::types::SourceType;

// ============================================================================
// Cost Model Constants
// ============================================================================

/// Rows that can be filtered/projected per millisecond.
/// Based on typical CPU performance for simple predicates.
const COMPUTE_ROWS_PER_MS: f64 = 100_000.0;

/// Rows that can be hashed per millisecond during hash join build phase.
/// Hashing involves computing hash + inserting into hash table, which is
/// more expensive per-row than a simple predicate filter.
const HASH_BUILD_ROWS_PER_MS: f64 = 80_000.0;

/// Rows that can be probed per millisecond during hash join probe phase.
/// Probing is faster than building: one hash computation + one hash table lookup.
const HASH_PROBE_ROWS_PER_MS: f64 = 150_000.0;

/// Memory overhead factor for hash tables.
/// Hash tables typically use 1.5-2x the raw data size for pointers and buckets.
const HASH_TABLE_OVERHEAD_FACTOR: f64 = 1.5;

/// Write speed to local temp tables (ClickHouse) in bytes per second.
/// ClickHouse can write at ~100MB/s for typical workloads.
const TEMP_TABLE_WRITE_SPEED_BPS: f64 = 100.0 * 1024.0 * 1024.0;

/// Scaling factor to convert memory fraction to millisecond-equivalent cost.
/// Memory pressure has significant impact on performance.
const MEMORY_TO_MS_SCALE: f64 = 1000.0;

// ============================================================================
// Parallelism Level
// ============================================================================

/// Level of parallelism supported by a data source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParallelismLevel {
    /// Single-threaded access only (e.g., rate-limited APIs)
    Low,
    /// Limited parallelism (e.g., connection pool limits)
    Medium,
    /// High parallelism (e.g., distributed storage, multiple shards)
    High,
}

impl ParallelismLevel {
    /// Get the effective parallelism factor for cost calculations.
    ///
    /// Higher values mean more parallel operations can run simultaneously.
    pub fn factor(&self) -> f64 {
        match self {
            ParallelismLevel::Low => 1.0,
            ParallelismLevel::Medium => 4.0,
            ParallelismLevel::High => 16.0,
        }
    }
}

impl Default for ParallelismLevel {
    fn default() -> Self {
        ParallelismLevel::Medium
    }
}

// ============================================================================
// Rate Limit Info
// ============================================================================

/// Rate limiting configuration for API sources.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitInfo {
    /// Maximum requests per window
    pub max_requests: u32,
    /// Window duration in seconds
    pub window_secs: u64,
    /// Typical rows per request (for cost estimation)
    pub rows_per_request: u32,
}

impl RateLimitInfo {
    /// Create rate limit info for a source.
    pub fn new(max_requests: u32, window_secs: u64, rows_per_request: u32) -> Self {
        Self {
            max_requests,
            window_secs,
            rows_per_request,
        }
    }

    /// Estimate time to fetch N rows given rate limits.
    ///
    /// Returns estimated time in milliseconds.  Returns `f64::MAX` when the
    /// fetch is impossible (zero window or zero rows-per-request).
    pub fn estimate_fetch_time_ms(&self, row_count: u64) -> f64 {
        if row_count == 0 {
            return 0.0;
        }

        if self.rows_per_request == 0 || self.window_secs == 0 {
            return f64::MAX;
        }

        let requests_needed = (row_count as f64 / self.rows_per_request as f64).ceil();
        let requests_per_ms = self.max_requests as f64 / (self.window_secs as f64 * 1000.0);

        if requests_per_ms <= 0.0 {
            return f64::MAX;
        }

        requests_needed / requests_per_ms
    }
}

// ============================================================================
// Source Access Profile
// ============================================================================

/// Access characteristics for a data source.
///
/// Used by the cost model to estimate query execution time and
/// make decisions about materialization strategy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceAccessProfile {
    /// Expected latency range for a single request (milliseconds)
    pub latency_ms: (u32, u32),
    /// Expected bandwidth range (MB/s)
    pub bandwidth_mb_s: (u32, u32),
    /// Level of parallelism supported
    pub parallelism: ParallelismLevel,
    /// Whether the source supports predicate pushdown
    pub supports_predicate_pushdown: bool,
    /// Rate limiting configuration (for API sources)
    pub rate_limit: Option<RateLimitInfo>,
    /// Whether data is local (no network transfer cost)
    pub is_local: bool,
    /// Typical bytes per row for this source type
    pub avg_bytes_per_row: u32,
}

impl SourceAccessProfile {
    /// Create a new source access profile.
    pub fn new(
        latency_ms: RangeInclusive<u32>,
        bandwidth_mb_s: RangeInclusive<u32>,
        parallelism: ParallelismLevel,
    ) -> Self {
        Self {
            latency_ms: (*latency_ms.start(), *latency_ms.end()),
            bandwidth_mb_s: (*bandwidth_mb_s.start(), *bandwidth_mb_s.end()),
            parallelism,
            supports_predicate_pushdown: true,
            rate_limit: None,
            is_local: false,
            avg_bytes_per_row: 100,
        }
    }

    /// Set whether predicate pushdown is supported.
    pub fn with_predicate_pushdown(mut self, supported: bool) -> Self {
        self.supports_predicate_pushdown = supported;
        self
    }

    /// Set rate limiting configuration.
    pub fn with_rate_limit(mut self, rate_limit: RateLimitInfo) -> Self {
        self.rate_limit = Some(rate_limit);
        self
    }

    /// Set whether the source is local.
    pub fn with_local(mut self, is_local: bool) -> Self {
        self.is_local = is_local;
        self
    }

    /// Set average bytes per row.
    pub fn with_avg_bytes_per_row(mut self, bytes: u32) -> Self {
        self.avg_bytes_per_row = bytes;
        self
    }

    /// Get average latency (midpoint of range).
    pub fn avg_latency_ms(&self) -> f64 {
        (self.latency_ms.0 as f64 + self.latency_ms.1 as f64) / 2.0
    }

    /// Get average bandwidth in MB/s (midpoint of range).
    pub fn avg_bandwidth_mb_s(&self) -> f64 {
        (self.bandwidth_mb_s.0 as f64 + self.bandwidth_mb_s.1 as f64) / 2.0
    }

    /// Estimate time to transfer data given size.
    ///
    /// Returns time in milliseconds.
    pub fn estimate_transfer_time_ms(&self, size_bytes: u64) -> f64 {
        if self.is_local {
            // Local data has minimal transfer time
            return 1.0;
        }

        let size_mb = size_bytes as f64 / (1024.0 * 1024.0);
        let bandwidth = self.avg_bandwidth_mb_s();

        if bandwidth <= 0.0 {
            return f64::MAX;
        }

        // Transfer time = size / bandwidth (converted to ms)
        (size_mb / bandwidth) * 1000.0 + self.avg_latency_ms()
    }

    /// Estimate time to fetch rows considering rate limits and parallelism.
    pub fn estimate_fetch_time_ms(&self, row_count: u64, size_bytes: u64) -> f64 {
        // Check rate limit first
        if let Some(rate_limit) = &self.rate_limit {
            let rate_limited_time = rate_limit.estimate_fetch_time_ms(row_count);
            // Rate limit is typically the bottleneck for API sources
            return rate_limited_time + self.avg_latency_ms();
        }

        // Calculate transfer time
        let transfer_time = self.estimate_transfer_time_ms(size_bytes);

        // Adjust for parallelism
        transfer_time / self.parallelism.factor()
    }

    /// Get default profile for a source type.
    pub fn default_for_source_type(source_type: SourceType) -> Self {
        match source_type {
            // ===== Native/High Performance =====
            SourceType::ExternalParquet => Self::new(50..=200, 100..=500, ParallelismLevel::High)
                .with_predicate_pushdown(true)
                .with_avg_bytes_per_row(100),

            // ===== Databases =====
            SourceType::PostgreSQL => Self::new(10..=50, 50..=200, ParallelismLevel::Medium)
                .with_predicate_pushdown(true)
                .with_avg_bytes_per_row(150),

            SourceType::MySQL => Self::new(10..=50, 50..=200, ParallelismLevel::Medium)
                .with_predicate_pushdown(true)
                .with_avg_bytes_per_row(150),

            SourceType::MongoDB => Self::new(20..=100, 30..=100, ParallelismLevel::Medium)
                .with_predicate_pushdown(true)
                .with_avg_bytes_per_row(200),

            SourceType::SqlServer => Self::new(10..=50, 50..=200, ParallelismLevel::Medium)
                .with_predicate_pushdown(true)
                .with_avg_bytes_per_row(150),

            SourceType::SQLite => Self::new(1..=5, 100..=500, ParallelismLevel::Low)
                .with_predicate_pushdown(true)
                .with_local(true)
                .with_avg_bytes_per_row(100),

            // ===== Cloud Data Warehouses =====
            SourceType::Snowflake => Self::new(100..=500, 100..=1000, ParallelismLevel::High)
                .with_predicate_pushdown(true)
                .with_avg_bytes_per_row(120),

            SourceType::BigQuery => Self::new(500..=2000, 100..=500, ParallelismLevel::High)
                .with_predicate_pushdown(true)
                .with_avg_bytes_per_row(120),

            SourceType::Redshift => Self::new(100..=500, 100..=500, ParallelismLevel::High)
                .with_predicate_pushdown(true)
                .with_avg_bytes_per_row(120),

            SourceType::ClickHouse => Self::new(5..=50, 100..=1000, ParallelismLevel::High)
                .with_predicate_pushdown(true)
                .with_avg_bytes_per_row(120),

            // ===== Payment/Finance APIs =====
            SourceType::Stripe => Self::new(100..=500, 1..=10, ParallelismLevel::Low)
                .with_predicate_pushdown(false)
                .with_rate_limit(RateLimitInfo::new(100, 1, 100))
                .with_avg_bytes_per_row(500),

            SourceType::QuickBooks => Self::new(200..=1000, 1..=5, ParallelismLevel::Low)
                .with_predicate_pushdown(false)
                .with_rate_limit(RateLimitInfo::new(500, 60, 100))
                .with_avg_bytes_per_row(400),

            SourceType::Xero => Self::new(200..=1000, 1..=5, ParallelismLevel::Low)
                .with_predicate_pushdown(false)
                .with_rate_limit(RateLimitInfo::new(60, 60, 100))
                .with_avg_bytes_per_row(400),

            // ===== CRM/Sales APIs =====
            SourceType::HubSpot => Self::new(100..=500, 1..=10, ParallelismLevel::Low)
                .with_predicate_pushdown(false)
                .with_rate_limit(RateLimitInfo::new(100, 10, 100))
                .with_avg_bytes_per_row(600),

            SourceType::Salesforce => Self::new(200..=1000, 1..=10, ParallelismLevel::Low)
                .with_predicate_pushdown(true) // SOQL supports filtering
                .with_rate_limit(RateLimitInfo::new(100, 1, 2000))
                .with_avg_bytes_per_row(800),

            SourceType::Zendesk => Self::new(100..=500, 1..=10, ParallelismLevel::Low)
                .with_predicate_pushdown(false)
                .with_rate_limit(RateLimitInfo::new(700, 60, 100))
                .with_avg_bytes_per_row(500),

            SourceType::Intercom => Self::new(100..=500, 1..=10, ParallelismLevel::Low)
                .with_predicate_pushdown(false)
                .with_rate_limit(RateLimitInfo::new(1000, 60, 50))
                .with_avg_bytes_per_row(400),

            // ===== E-commerce APIs =====
            SourceType::Shopify => Self::new(100..=500, 1..=10, ParallelismLevel::Low)
                .with_predicate_pushdown(true) // GraphQL supports filtering
                .with_rate_limit(RateLimitInfo::new(40, 1, 250))
                .with_avg_bytes_per_row(600),

            SourceType::WooCommerce => Self::new(100..=500, 1..=10, ParallelismLevel::Low)
                .with_predicate_pushdown(true)
                .with_rate_limit(RateLimitInfo::new(25, 1, 100))
                .with_avg_bytes_per_row(500),

            // ===== Analytics APIs =====
            SourceType::GoogleAnalytics => Self::new(500..=2000, 1..=10, ParallelismLevel::Low)
                .with_predicate_pushdown(true) // GA supports date/dimension filters
                .with_rate_limit(RateLimitInfo::new(10, 1, 10000))
                .with_avg_bytes_per_row(200),

            SourceType::Mixpanel => Self::new(200..=1000, 1..=10, ParallelismLevel::Low)
                .with_predicate_pushdown(true)
                .with_rate_limit(RateLimitInfo::new(60, 60, 1000))
                .with_avg_bytes_per_row(300),

            SourceType::Amplitude => Self::new(200..=1000, 1..=10, ParallelismLevel::Low)
                .with_predicate_pushdown(true)
                .with_rate_limit(RateLimitInfo::new(360, 3600, 1000))
                .with_avg_bytes_per_row(300),

            SourceType::PostHog => Self::new(100..=500, 5..=50, ParallelismLevel::Medium)
                .with_predicate_pushdown(true)
                .with_avg_bytes_per_row(250),

            // ===== Ads APIs =====
            SourceType::FacebookAds => Self::new(200..=1000, 1..=10, ParallelismLevel::Low)
                .with_predicate_pushdown(true)
                .with_rate_limit(RateLimitInfo::new(200, 3600, 500))
                .with_avg_bytes_per_row(400),

            SourceType::GoogleAds => Self::new(200..=1000, 1..=10, ParallelismLevel::Low)
                .with_predicate_pushdown(true)
                .with_rate_limit(RateLimitInfo::new(1000, 60, 1000))
                .with_avg_bytes_per_row(350),

            // ===== Dev Tools APIs =====
            SourceType::GitHub => Self::new(100..=500, 1..=10, ParallelismLevel::Low)
                .with_predicate_pushdown(true) // GraphQL supports filtering
                .with_rate_limit(RateLimitInfo::new(5000, 3600, 100))
                .with_avg_bytes_per_row(500),

            SourceType::Jira => Self::new(100..=500, 1..=10, ParallelismLevel::Low)
                .with_predicate_pushdown(true) // JQL supports filtering
                .with_rate_limit(RateLimitInfo::new(100, 1, 100))
                .with_avg_bytes_per_row(800),

            SourceType::Linear => Self::new(50..=200, 5..=20, ParallelismLevel::Low)
                .with_predicate_pushdown(true) // GraphQL supports filtering
                .with_rate_limit(RateLimitInfo::new(5000, 3600, 100))
                .with_avg_bytes_per_row(400),

            // ===== Productivity APIs =====
            SourceType::GoogleSheets => Self::new(200..=1000, 1..=10, ParallelismLevel::Low)
                .with_predicate_pushdown(false)
                .with_rate_limit(RateLimitInfo::new(100, 100, 1000))
                .with_avg_bytes_per_row(200),

            SourceType::Notion => Self::new(100..=500, 1..=10, ParallelismLevel::Low)
                .with_predicate_pushdown(true)
                .with_rate_limit(RateLimitInfo::new(3, 1, 100))
                .with_avg_bytes_per_row(600),

            SourceType::Confluence => Self::new(100..=500, 1..=10, ParallelismLevel::Low)
                .with_predicate_pushdown(false)
                .with_rate_limit(RateLimitInfo::new(100, 1, 25))
                .with_avg_bytes_per_row(1000),

            // ===== File Formats =====
            SourceType::Csv => Self::new(10..=100, 50..=200, ParallelismLevel::Low)
                .with_predicate_pushdown(false)
                .with_avg_bytes_per_row(100),

            SourceType::Json => Self::new(10..=100, 50..=200, ParallelismLevel::Low)
                .with_predicate_pushdown(false)
                .with_avg_bytes_per_row(200),

            SourceType::Excel => Self::new(50..=500, 10..=50, ParallelismLevel::Low)
                .with_predicate_pushdown(false)
                .with_avg_bytes_per_row(150),

            SourceType::Xml => Self::new(10..=100, 20..=100, ParallelismLevel::Low)
                .with_predicate_pushdown(false)
                .with_avg_bytes_per_row(300),

            // ===== Blockchain =====
            SourceType::Ethereum
            | SourceType::Solana
            | SourceType::Bitcoin
            | SourceType::Polygon => Self::new(100..=1000, 1..=50, ParallelismLevel::Medium)
                .with_predicate_pushdown(true)
                .with_rate_limit(RateLimitInfo::new(100, 1, 100))
                .with_avg_bytes_per_row(500),

            // ===== Streaming =====
            SourceType::Kafka => Self::new(5..=50, 100..=500, ParallelismLevel::High)
                .with_predicate_pushdown(false)
                .with_avg_bytes_per_row(200),

            SourceType::AwsKinesis => Self::new(10..=100, 50..=200, ParallelismLevel::High)
                .with_predicate_pushdown(false)
                .with_avg_bytes_per_row(200),

            // ===== Cloud Storage =====
            SourceType::GoogleCloudStorage | SourceType::AzureBlob => {
                Self::new(50..=200, 100..=500, ParallelismLevel::High)
                    .with_predicate_pushdown(true)
                    .with_avg_bytes_per_row(100)
            }

            // ===== Additional Productivity =====
            SourceType::Airtable => Self::new(100..=500, 1..=10, ParallelismLevel::Low)
                .with_predicate_pushdown(true)
                .with_rate_limit(RateLimitInfo::new(5, 1, 100))
                .with_avg_bytes_per_row(400),

            SourceType::Asana => Self::new(100..=500, 1..=10, ParallelismLevel::Low)
                .with_predicate_pushdown(false)
                .with_rate_limit(RateLimitInfo::new(150, 60, 100))
                .with_avg_bytes_per_row(500),

            SourceType::Monday => Self::new(100..=500, 1..=10, ParallelismLevel::Low)
                .with_predicate_pushdown(true)
                .with_rate_limit(RateLimitInfo::new(5000, 60, 100))
                .with_avg_bytes_per_row(600),

            // Derived tables are local Parquet on R2 — same profile as ExternalParquet
            SourceType::Derived => Self::new(50..=200, 100..=500, ParallelismLevel::High)
                .with_predicate_pushdown(true)
                .with_avg_bytes_per_row(100),
        }
    }
}

impl Default for SourceAccessProfile {
    fn default() -> Self {
        Self::new(100..=500, 10..=100, ParallelismLevel::Medium)
    }
}

// ============================================================================
// Filter Operations and Source Capabilities
// ============================================================================

/// Specific filter operations a source can support.
///
/// This enum represents the different types of filter predicates that can
/// potentially be pushed down to a data source. Each source type has different
/// capabilities for handling these operations natively.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterOperation {
    /// Equality comparison: column = value
    Equals,
    /// Inequality comparison: column != value or column <> value
    NotEquals,
    /// Less than: column < value
    LessThan,
    /// Less than or equal: column <= value
    LessThanOrEquals,
    /// Greater than: column > value
    GreaterThan,
    /// Greater than or equal: column >= value
    GreaterThanOrEquals,
    /// Between range: column BETWEEN low AND high
    Between,
    /// IN list: column IN (value1, value2, ...)
    /// max_values limits the number of values for API sources
    In {
        max_values: Option<usize>,
    },
    /// LIKE pattern matching
    /// supports_leading_wildcard indicates if %pattern is supported (many APIs don't)
    Like {
        supports_leading_wildcard: bool,
    },
    /// IS NULL check
    IsNull,
    /// IS NOT NULL check
    IsNotNull,
    /// Date after filter (API-specific, e.g., Stripe's created[gte])
    DateAfter,
    /// Date before filter (API-specific, e.g., Stripe's created[lte])
    DateBefore,
    /// Text contains/search (for full-text search APIs)
    Contains,
    /// Regular expression matching (for databases that support it)
    Regex,
    /// Array contains element (for array columns)
    ArrayContains,
    /// JSON path filter (for JSON columns)
    JsonPath,
    /// Custom source-specific filter
    /// Used for API-specific filters that don't map to standard SQL
    Custom(String),
}

impl FilterOperation {
    /// Check if this operation is a comparison operation.
    pub fn is_comparison(&self) -> bool {
        matches!(
            self,
            FilterOperation::Equals
                | FilterOperation::NotEquals
                | FilterOperation::LessThan
                | FilterOperation::LessThanOrEquals
                | FilterOperation::GreaterThan
                | FilterOperation::GreaterThanOrEquals
        )
    }

    /// Check if this operation is a range operation.
    pub fn is_range(&self) -> bool {
        matches!(
            self,
            FilterOperation::Between
                | FilterOperation::LessThan
                | FilterOperation::LessThanOrEquals
                | FilterOperation::GreaterThan
                | FilterOperation::GreaterThanOrEquals
                | FilterOperation::DateAfter
                | FilterOperation::DateBefore
        )
    }

    /// Get the SQL operator for this operation.
    pub fn sql_operator(&self) -> Option<&'static str> {
        match self {
            FilterOperation::Equals => Some("="),
            FilterOperation::NotEquals => Some("<>"),
            FilterOperation::LessThan => Some("<"),
            FilterOperation::LessThanOrEquals => Some("<="),
            FilterOperation::GreaterThan => Some(">"),
            FilterOperation::GreaterThanOrEquals => Some(">="),
            FilterOperation::IsNull => Some("IS NULL"),
            FilterOperation::IsNotNull => Some("IS NOT NULL"),
            _ => None,
        }
    }

    /// Check if this operation (from a query) can be handled by a capability.
    ///
    /// This is different from equality - a query operation may match a capability
    /// even if the parameters differ, as long as the capability can handle it:
    /// - `In { max_values: Some(5) }` matches `In { max_values: Some(100) }` (5 <= 100)
    /// - `In { max_values: Some(5) }` matches `In { max_values: None }` (unlimited)
    /// - `Like { supports_leading_wildcard: false }` matches `Like { supports_leading_wildcard: true }`
    /// - `Like { supports_leading_wildcard: true }` does NOT match `Like { supports_leading_wildcard: false }`
    pub fn matches_capability(&self, capability: &FilterOperation) -> bool {
        match (self, capability) {
            // In: query values must fit within capability's max
            (
                FilterOperation::In { max_values: query_max },
                FilterOperation::In { max_values: cap_max },
            ) => {
                match (query_max, cap_max) {
                    // Capability is unlimited - any query fits
                    (_, None) => true,
                    // Query is unlimited but capability has limit - doesn't fit
                    (None, Some(_)) => false,
                    // Both have limits - query must be <= capability
                    (Some(q), Some(c)) => q <= c,
                }
            }

            // Like: query with leading wildcard only matches capability that supports it
            (
                FilterOperation::Like { supports_leading_wildcard: query_leading },
                FilterOperation::Like { supports_leading_wildcard: cap_leading },
            ) => {
                // If query uses leading wildcard, capability must support it
                // If query doesn't use leading wildcard, any Like capability works
                !query_leading || *cap_leading
            }

            // Custom: must match the exact custom operation
            (FilterOperation::Custom(q), FilterOperation::Custom(c)) => q == c,

            // For all other variants, they match if they're the same variant
            // Use discriminant comparison for simple variants
            _ => std::mem::discriminant(self) == std::mem::discriminant(capability),
        }
    }

    /// Get a canonical version of this operation for AHashSet lookups.
    ///
    /// This normalizes parametric variants to a canonical form so they can
    /// be found in capability sets regardless of parameter values.
    pub fn canonical(&self) -> FilterOperation {
        match self {
            FilterOperation::In { .. } => FilterOperation::In { max_values: None },
            FilterOperation::Like { .. } => FilterOperation::Like { supports_leading_wildcard: true },
            other => other.clone(),
        }
    }
}

/// Value transformation for API parameter translation.
///
/// Some APIs require values in specific formats. This enum defines
/// the transformations needed to convert SQL-style values to API format.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueTransform {
    /// Convert timestamp/datetime to Unix epoch seconds
    TimestampToEpoch,
    /// Convert timestamp to Unix epoch milliseconds
    TimestampToEpochMs,
    /// Convert date to ISO 8601 format (YYYY-MM-DD)
    DateToIso8601,
    /// Convert datetime to ISO 8601 with timezone
    DateTimeToIso8601,
    /// Convert boolean to string ("true"/"false")
    BooleanToString,
    /// Convert boolean to integer (1/0)
    BooleanToInt,
    /// Convert cents to dollars (divide by 100)
    CentsToDollars,
    /// Convert dollars to cents (multiply by 100)
    DollarsToCents,
    /// URL encode the value
    UrlEncode,
    /// Base64 encode the value
    Base64Encode,
    /// Convert to lowercase
    ToLowercase,
    /// Convert to uppercase
    ToUppercase,
    /// Custom transformation (expression-based)
    Custom(String),
}

/// Column-specific filter capability.
///
/// Defines what filter operations are supported for a specific column,
/// and how to translate the column name and values for the target API.
#[derive(Debug, Clone, Default)]
pub struct ColumnFilterCapability {
    /// Filter operations supported for this column
    pub supported_ops: AHashSet<FilterOperation>,
    /// API parameter name if different from column name
    /// For example, Stripe uses "created[gte]" instead of "created >="
    pub api_param_name: Option<String>,
    /// Value transformation needed when pushing down
    pub value_transform: Option<ValueTransform>,
    /// Whether this column is indexed (affects selectivity estimation)
    pub is_indexed: bool,
    /// For IN operations, maximum number of values allowed
    pub max_in_values: Option<usize>,
    /// Column data type (for validation)
    pub data_type: Option<String>,
}

impl ColumnFilterCapability {
    /// Create a new column filter capability with specified operations.
    pub fn new(ops: impl IntoIterator<Item = FilterOperation>) -> Self {
        Self {
            supported_ops: ops.into_iter().collect(),
            api_param_name: None,
            value_transform: None,
            is_indexed: false,
            max_in_values: None,
            data_type: None,
        }
    }

    /// Set the API parameter name.
    pub fn with_api_param(mut self, name: impl Into<String>) -> Self {
        self.api_param_name = Some(name.into());
        self
    }

    /// Set the value transformation.
    pub fn with_transform(mut self, transform: ValueTransform) -> Self {
        self.value_transform = Some(transform);
        self
    }

    /// Mark column as indexed.
    pub fn with_indexed(mut self, indexed: bool) -> Self {
        self.is_indexed = indexed;
        self
    }

    /// Set maximum IN values.
    pub fn with_max_in_values(mut self, max: usize) -> Self {
        self.max_in_values = Some(max);
        self
    }

    /// Check if a specific operation is supported.
    ///
    /// This properly handles parametric variants like `In` and `Like`:
    /// - `In { max_values: Some(5) }` matches a capability with `In { max_values: Some(100) }`
    /// - `Like { supports_leading_wildcard: true }` requires the capability to support leading wildcards
    pub fn supports(&self, op: &FilterOperation) -> bool {
        self.supported_ops.iter().any(|cap| op.matches_capability(cap))
    }
}

/// Source capabilities for predicate pushdown.
///
/// Defines what filter operations a data source supports, both globally
/// and on a per-column basis. This is used by the query planner to
/// determine which predicates can be pushed down to the source.
#[derive(Debug, Clone)]
pub struct SourceCapabilities {
    /// Global filter operations supported by this source
    /// These apply to all columns unless overridden
    pub supported_operations: AHashSet<FilterOperation>,

    /// Column-specific filter capabilities (overrides global)
    /// Key is table.column or just column for single-table sources
    pub column_filters: AHashMap<String, ColumnFilterCapability>,

    /// Whether the source supports arbitrary SQL WHERE clauses
    /// If true, any valid SQL predicate can be pushed down
    pub supports_arbitrary_sql: bool,

    /// Whether predicates can be combined with AND
    pub supports_and: bool,

    /// Whether predicates can be combined with OR
    pub supports_or: bool,

    /// Whether NOT is supported for predicate negation
    pub supports_not: bool,

    /// Whether nested/parenthesized predicates are supported
    pub supports_nested: bool,

    /// Maximum number of filter conditions (None = unlimited)
    pub max_filters: Option<usize>,

    /// Cost multiplier when filters cannot be pushed down
    /// Higher values indicate more expensive full scans
    pub full_scan_cost_multiplier: f64,

    /// Whether the source supports column pruning (selecting specific columns)
    pub supports_column_pruning: bool,

    /// Whether the source supports LIMIT/TOP
    pub supports_limit: bool,

    /// Whether the source supports ORDER BY pushdown
    pub supports_order_by: bool,

    /// Whether the source supports aggregate pushdown (GROUP BY, SUM, etc.)
    pub supports_aggregates: bool,

    /// Human-readable description of limitations
    pub limitations_description: Option<String>,
}

impl SourceCapabilities {
    /// Create source capabilities with full SQL support.
    ///
    /// Used for SQL databases like PostgreSQL, MySQL, ClickHouse.
    pub fn full_sql_support() -> Self {
        Self {
            supported_operations: [
                FilterOperation::Equals,
                FilterOperation::NotEquals,
                FilterOperation::LessThan,
                FilterOperation::LessThanOrEquals,
                FilterOperation::GreaterThan,
                FilterOperation::GreaterThanOrEquals,
                FilterOperation::Between,
                FilterOperation::In { max_values: None },
                FilterOperation::Like { supports_leading_wildcard: true },
                FilterOperation::IsNull,
                FilterOperation::IsNotNull,
                FilterOperation::Regex,
            ]
            .into_iter()
            .collect(),
            column_filters: AHashMap::new(),
            supports_arbitrary_sql: true,
            supports_and: true,
            supports_or: true,
            supports_not: true,
            supports_nested: true,
            max_filters: None,
            full_scan_cost_multiplier: 1.0,
            supports_column_pruning: true,
            supports_limit: true,
            supports_order_by: true,
            supports_aggregates: true,
            limitations_description: None,
        }
    }

    /// Create source capabilities with no pushdown support.
    ///
    /// Used for file formats like CSV, Excel that must be fully scanned.
    pub fn no_pushdown() -> Self {
        Self {
            supported_operations: AHashSet::new(),
            column_filters: AHashMap::new(),
            supports_arbitrary_sql: false,
            supports_and: false,
            supports_or: false,
            supports_not: false,
            supports_nested: false,
            max_filters: Some(0),
            full_scan_cost_multiplier: 10.0, // High cost for full scan
            supports_column_pruning: false,
            supports_limit: false,
            supports_order_by: false,
            supports_aggregates: false,
            limitations_description: Some(
                "This source does not support any filter pushdown. All data must be fetched.".to_string()
            ),
        }
    }

    /// Create source capabilities for Parquet files.
    ///
    /// Parquet supports column pruning and row group filtering via statistics.
    pub fn parquet_capabilities() -> Self {
        Self {
            supported_operations: [
                FilterOperation::Equals,
                FilterOperation::NotEquals,
                FilterOperation::LessThan,
                FilterOperation::LessThanOrEquals,
                FilterOperation::GreaterThan,
                FilterOperation::GreaterThanOrEquals,
                FilterOperation::Between,
                FilterOperation::In { max_values: Some(100) },
                FilterOperation::IsNull,
                FilterOperation::IsNotNull,
            ]
            .into_iter()
            .collect(),
            column_filters: AHashMap::new(),
            supports_arbitrary_sql: false,
            supports_and: true,
            supports_or: false, // OR requires reading all row groups
            supports_not: false,
            supports_nested: false,
            max_filters: None,
            full_scan_cost_multiplier: 3.0,
            supports_column_pruning: true,
            supports_limit: false, // Must scan to apply limit
            supports_order_by: false,
            supports_aggregates: false,
            limitations_description: Some(
                "Parquet supports column pruning and row group skipping via min/max statistics. \
                 Complex predicates and OR conditions cannot be pushed down.".to_string()
            ),
        }
    }

    /// Check if a predicate operation can be pushed down globally.
    pub fn supports_operation(&self, op: &FilterOperation) -> bool {
        if self.supports_arbitrary_sql {
            return true;
        }
        
        // Check if any capability in the set can handle this operation
        // This properly handles parametric variants like In and Like
        self.supported_operations.iter().any(|cap| op.matches_capability(cap))
    }

    /// Check if a predicate on a specific column can be pushed down.
    pub fn supports_column_filter(&self, column: &str, op: &FilterOperation) -> bool {
        // Check column-specific override first
        if let Some(col_cap) = self.column_filters.get(column) {
            return col_cap.supports(op);
        }

        // Fall back to global capabilities
        self.supports_operation(op)
    }

    /// Get column-specific capability, if defined.
    pub fn get_column_capability(&self, column: &str) -> Option<&ColumnFilterCapability> {
        self.column_filters.get(column)
    }

    /// Check if any pushdown is supported.
    pub fn has_any_pushdown(&self) -> bool {
        self.supports_arbitrary_sql
            || !self.supported_operations.is_empty()
            || !self.column_filters.is_empty()
    }

    /// Add a column-specific filter capability.
    pub fn with_column_filter(
        mut self,
        column: impl Into<String>,
        capability: ColumnFilterCapability,
    ) -> Self {
        self.column_filters.insert(column.into(), capability);
        self
    }

    /// Set limitations description.
    pub fn with_limitations(mut self, description: impl Into<String>) -> Self {
        self.limitations_description = Some(description.into());
        self
    }
}

impl Default for SourceCapabilities {
    fn default() -> Self {
        Self::no_pushdown()
    }
}

// ============================================================================
// Query Cost
// ============================================================================

/// Estimated cost of a query or operation.
///
/// All cost components are measured in milliseconds for easy interpretation.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryCost {
    /// Network transfer time in milliseconds
    pub network_io_cost: f64,
    /// CPU/processing time in milliseconds
    pub compute_cost: f64,
    /// Memory pressure (0-1 scale, represents relative memory usage)
    pub memory_cost: f64,
    /// Total weighted cost
    pub total_cost: f64,
}

impl QueryCost {
    /// Create a new query cost using default cost model weights.
    ///
    /// Note: For custom weights, use `with_model()` instead.
    pub fn new(network_io_cost: f64, compute_cost: f64, memory_cost: f64) -> Self {
        let model = CostModel::default();
        Self::with_model(network_io_cost, compute_cost, memory_cost, &model)
    }

    /// Create a new query cost using a specific cost model.
    ///
    /// This ensures the total cost is calculated using the provided weights.
    pub fn with_model(
        network_io_cost: f64,
        compute_cost: f64,
        memory_cost: f64,
        model: &CostModel,
    ) -> Self {
        let total = model.calculate_total(network_io_cost, compute_cost, memory_cost);
        Self {
            network_io_cost,
            compute_cost,
            memory_cost,
            total_cost: total,
        }
    }

    /// Create a zero cost.
    pub fn zero() -> Self {
        Self::default()
    }

    /// Add another cost to this one.
    ///
    /// Memory cost uses `max()` semantics (peak memory across sequential operations).
    /// For concurrent allocations where memory is additive, use `add_concurrent()`.
    pub fn add(&self, other: &QueryCost) -> Self {
        Self::new(
            self.network_io_cost + other.network_io_cost,
            self.compute_cost + other.compute_cost,
            self.memory_cost.max(other.memory_cost), // Memory is max, not sum
        )
    }

    /// Add another cost with additive memory semantics.
    ///
    /// Use this when both allocations coexist simultaneously (e.g., parallel operations).
    /// For sequential operations, use `add()` which takes the max memory.
    pub fn add_concurrent(&self, other: &QueryCost) -> Self {
        Self::new(
            self.network_io_cost + other.network_io_cost,
            self.compute_cost + other.compute_cost,
            self.memory_cost + other.memory_cost, // Memory is additive for concurrent
        )
    }

    /// Add with custom model for total calculation.
    pub fn add_with_model(&self, other: &QueryCost, model: &CostModel) -> Self {
        Self::with_model(
            self.network_io_cost + other.network_io_cost,
            self.compute_cost + other.compute_cost,
            self.memory_cost.max(other.memory_cost),
            model,
        )
    }

    /// Multiply cost by a factor.
    pub fn scale(&self, factor: f64) -> Self {
        Self::new(
            self.network_io_cost * factor,
            self.compute_cost * factor,
            self.memory_cost, // Memory doesn't scale linearly
        )
    }

    /// Multiply cost by a factor using custom model.
    pub fn scale_with_model(&self, factor: f64, model: &CostModel) -> Self {
        Self::with_model(
            self.network_io_cost * factor,
            self.compute_cost * factor,
            self.memory_cost,
            model,
        )
    }
}

impl std::fmt::Display for QueryCost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "QueryCost(network={:.1}ms, compute={:.1}ms, memory={:.2}, total={:.1}ms)",
            self.network_io_cost, self.compute_cost, self.memory_cost, self.total_cost
        )
    }
}

// ============================================================================
// Cost Model
// ============================================================================

/// Weights for cost model components.
///
/// Used to calculate total cost from individual components.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostModel {
    /// Weight for network I/O cost (default 1.0)
    pub network_weight: f64,
    /// Weight for compute cost (default 0.5 - compute is typically faster)
    pub compute_weight: f64,
    /// Weight for memory cost (default 2.0 - memory pressure is expensive)
    pub memory_weight: f64,
    /// Base memory available in MB (for memory cost scaling)
    pub memory_budget_mb: u32,
}

impl CostModel {
    /// Create a new cost model with custom weights.
    pub fn new(network_weight: f64, compute_weight: f64, memory_weight: f64) -> Self {
        Self {
            network_weight,
            compute_weight,
            memory_weight,
            memory_budget_mb: 1024,
        }
    }

    /// Set memory budget for cost calculations.
    pub fn with_memory_budget(mut self, budget_mb: u32) -> Self {
        self.memory_budget_mb = budget_mb;
        self
    }

    /// Calculate total cost from components.
    pub fn calculate_total(&self, network_io: f64, compute: f64, memory: f64) -> f64 {
        (network_io * self.network_weight)
            + (compute * self.compute_weight)
            + (memory * self.memory_weight * MEMORY_TO_MS_SCALE)
    }

    /// Estimate scan cost for a table.
    pub fn estimate_scan_cost(
        &self,
        row_count: u64,
        size_bytes: u64,
        profile: &SourceAccessProfile,
        selectivity: f64, // 0-1, fraction of rows that pass predicates
    ) -> QueryCost {
        let effective_rows = (row_count as f64 * selectivity) as u64;
        let effective_bytes = (size_bytes as f64 * selectivity) as u64;

        let network_cost = if profile.supports_predicate_pushdown {
            profile.estimate_fetch_time_ms(effective_rows, effective_bytes)
        } else {
            profile.estimate_fetch_time_ms(row_count, size_bytes)
        };

        let compute_cost = effective_rows as f64 / COMPUTE_ROWS_PER_MS;

        let memory_mb = effective_bytes as f64 / (1024.0 * 1024.0);
        let memory_cost = (memory_mb / self.memory_budget_mb as f64).min(1.0);

        QueryCost::with_model(network_cost, compute_cost, memory_cost, self)
    }

    /// Estimate materialization cost (fetching and storing in temp table).
    pub fn estimate_materialization_cost(
        &self,
        row_count: u64,
        size_bytes: u64,
        source_profile: &SourceAccessProfile,
    ) -> QueryCost {
        // Fetch from source
        let fetch_cost = source_profile.estimate_fetch_time_ms(row_count, size_bytes);

        // Write to ClickHouse temp table (local, fast)
        let write_cost = size_bytes as f64 / TEMP_TABLE_WRITE_SPEED_BPS * 1000.0;

        // Memory for buffering
        let memory_mb = size_bytes as f64 / (1024.0 * 1024.0);
        let memory_cost = (memory_mb / self.memory_budget_mb as f64).min(1.0);

        QueryCost::with_model(fetch_cost, write_cost, memory_cost, self)
    }

    /// Estimate hash join cost.
    ///
    /// Assumes build side is held in memory, probe side is streamed.
    pub fn estimate_hash_join_cost(
        &self,
        build_rows: u64,
        build_bytes: u64,
        probe_rows: u64,
        _probe_bytes: u64,
    ) -> QueryCost {
        // Build phase: hash all build rows
        let build_compute = build_rows as f64 / HASH_BUILD_ROWS_PER_MS;

        // Probe phase: look up each probe row
        let probe_compute = probe_rows as f64 / HASH_PROBE_ROWS_PER_MS;

        // Memory for hash table (build side + overhead)
        let hash_table_mb = (build_bytes as f64 * HASH_TABLE_OVERHEAD_FACTOR) / (1024.0 * 1024.0);
        let memory_cost = (hash_table_mb / self.memory_budget_mb as f64).min(1.0);

        // No network cost for join itself (data already fetched)
        QueryCost::with_model(0.0, build_compute + probe_compute, memory_cost, self)
    }

    /// Compare two plans and return the cheaper one's index.
    pub fn cheaper_plan(&self, cost_a: &QueryCost, cost_b: &QueryCost) -> usize {
        if cost_a.total_cost <= cost_b.total_cost {
            0
        } else {
            1
        }
    }
}

impl Default for CostModel {
    fn default() -> Self {
        Self {
            network_weight: 1.0,
            compute_weight: 0.5,
            memory_weight: 2.0,
            memory_budget_mb: 1024,
        }
    }
}

// ============================================================================
// Build Side Selection
// ============================================================================

/// Which side of a join to use as the build side for hash join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildSide {
    Left,
    Right,
}

impl BuildSide {
    /// Select the optimal build side based on statistics and profiles.
    ///
    /// The build side should be:
    /// 1. The smaller side (fewer rows/bytes)
    /// 2. The side that's cheaper to materialize
    /// 3. The side that fits in memory budget
    pub fn select(
        left_rows: u64,
        left_bytes: u64,
        left_profile: &SourceAccessProfile,
        right_rows: u64,
        right_bytes: u64,
        right_profile: &SourceAccessProfile,
        cost_model: &CostModel,
    ) -> Self {
        // First, check memory constraints
        let left_mb = left_bytes as f64 / (1024.0 * 1024.0);
        let right_mb = right_bytes as f64 / (1024.0 * 1024.0);
        let budget_mb = cost_model.memory_budget_mb as f64;

        // If one side doesn't fit in memory, use the other
        if left_mb > budget_mb && right_mb <= budget_mb {
            return BuildSide::Right;
        }
        if right_mb > budget_mb && left_mb <= budget_mb {
            return BuildSide::Left;
        }

        // Estimate materialization cost for each side
        let left_mat_cost =
            cost_model.estimate_materialization_cost(left_rows, left_bytes, left_profile);
        let right_mat_cost =
            cost_model.estimate_materialization_cost(right_rows, right_bytes, right_profile);

        // Compare total cost (materialization + memory pressure)
        let left_total = left_mat_cost.total_cost;
        let right_total = right_mat_cost.total_cost;

        if left_total <= right_total {
            BuildSide::Left
        } else {
            BuildSide::Right
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parallelism_factor() {
        assert_eq!(ParallelismLevel::Low.factor(), 1.0);
        assert_eq!(ParallelismLevel::Medium.factor(), 4.0);
        assert_eq!(ParallelismLevel::High.factor(), 16.0);
    }

    #[test]
    fn test_rate_limit_fetch_time() {
        let rate_limit = RateLimitInfo::new(100, 1, 100); // 100 req/s, 100 rows/req
        
        // 10000 rows = 100 requests = 1 second = 1000ms
        let time = rate_limit.estimate_fetch_time_ms(10000);
        assert!((time - 1000.0).abs() < 1.0);
    }

    #[test]
    fn test_source_profile_stripe() {
        let profile = SourceAccessProfile::default_for_source_type(SourceType::Stripe);
        
        assert_eq!(profile.parallelism, ParallelismLevel::Low);
        assert!(!profile.supports_predicate_pushdown);
        assert!(profile.rate_limit.is_some());
    }

    #[test]
    fn test_source_profile_postgres() {
        let profile = SourceAccessProfile::default_for_source_type(SourceType::PostgreSQL);
        
        assert_eq!(profile.parallelism, ParallelismLevel::Medium);
        assert!(profile.supports_predicate_pushdown);
        assert!(profile.rate_limit.is_none());
    }

    #[test]
    fn test_source_profile_parquet() {
        let profile = SourceAccessProfile::default_for_source_type(SourceType::ExternalParquet);
        
        assert_eq!(profile.parallelism, ParallelismLevel::High);
        assert!(profile.supports_predicate_pushdown);
        assert!(profile.rate_limit.is_none());
    }

    #[test]
    fn test_cost_model_scan() {
        let model = CostModel::default();
        let profile = SourceAccessProfile::default_for_source_type(SourceType::PostgreSQL);
        
        // 1M rows, 100MB, 50% selectivity
        let cost = model.estimate_scan_cost(1_000_000, 100 * 1024 * 1024, &profile, 0.5);
        
        assert!(cost.network_io_cost > 0.0);
        assert!(cost.compute_cost > 0.0);
        assert!(cost.memory_cost > 0.0);
        assert!(cost.total_cost > 0.0);
    }

    #[test]
    fn test_cost_model_hash_join() {
        let model = CostModel::default();
        
        // Build: 100K rows, 10MB; Probe: 1M rows, 100MB
        let cost = model.estimate_hash_join_cost(
            100_000, 10 * 1024 * 1024,
            1_000_000, 100 * 1024 * 1024,
        );
        
        assert_eq!(cost.network_io_cost, 0.0); // Join has no network cost
        assert!(cost.compute_cost > 0.0);
        assert!(cost.memory_cost > 0.0);
    }

    #[test]
    fn test_build_side_selection_smaller() {
        let model = CostModel::default();
        let profile = SourceAccessProfile::default_for_source_type(SourceType::PostgreSQL);
        
        // Left is smaller
        let side = BuildSide::select(
            10_000, 1 * 1024 * 1024,  // 10K rows, 1MB
            &profile,
            1_000_000, 100 * 1024 * 1024,  // 1M rows, 100MB
            &profile,
            &model,
        );
        
        assert_eq!(side, BuildSide::Left);
    }

    #[test]
    fn test_build_side_selection_memory_constraint() {
        let model = CostModel::new(1.0, 0.5, 2.0).with_memory_budget(50); // Only 50MB
        let profile = SourceAccessProfile::default_for_source_type(SourceType::PostgreSQL);
        
        // Left doesn't fit, right does
        let side = BuildSide::select(
            1_000_000, 100 * 1024 * 1024,  // 100MB - too big
            &profile,
            10_000, 10 * 1024 * 1024,  // 10MB - fits
            &profile,
            &model,
        );
        
        assert_eq!(side, BuildSide::Right);
    }

    #[test]
    fn test_query_cost_add() {
        let cost1 = QueryCost::new(100.0, 50.0, 0.3);
        let cost2 = QueryCost::new(200.0, 100.0, 0.5);
        
        let combined = cost1.add(&cost2);
        
        assert_eq!(combined.network_io_cost, 300.0);
        assert_eq!(combined.compute_cost, 150.0);
        assert_eq!(combined.memory_cost, 0.5); // Max, not sum
    }

    #[test]
    fn test_query_cost_with_model() {
        // Custom model with different weights
        let model = CostModel::new(2.0, 1.0, 3.0);
        
        let cost = QueryCost::with_model(100.0, 50.0, 0.5, &model);
        
        // Total should be: 100*2.0 + 50*1.0 + 0.5*3.0*1000 = 200 + 50 + 1500 = 1750
        assert_eq!(cost.network_io_cost, 100.0);
        assert_eq!(cost.compute_cost, 50.0);
        assert_eq!(cost.memory_cost, 0.5);
        assert!((cost.total_cost - 1750.0).abs() < 0.01);
    }

    #[test]
    fn test_query_cost_add_concurrent() {
        let cost1 = QueryCost::new(100.0, 50.0, 0.3);
        let cost2 = QueryCost::new(200.0, 100.0, 0.5);
        
        let combined = cost1.add_concurrent(&cost2);
        
        assert_eq!(combined.network_io_cost, 300.0);
        assert_eq!(combined.compute_cost, 150.0);
        assert_eq!(combined.memory_cost, 0.8); // Sum, not max
    }

    #[test]
    fn test_query_cost_add_with_model() {
        let model = CostModel::new(2.0, 1.0, 3.0);
        
        let cost1 = QueryCost::with_model(100.0, 50.0, 0.3, &model);
        let cost2 = QueryCost::with_model(200.0, 100.0, 0.5, &model);
        
        let combined = cost1.add_with_model(&cost2, &model);
        
        // Memory is max (0.5), network is sum (300), compute is sum (150)
        // Total: 300*2.0 + 150*1.0 + 0.5*3.0*1000 = 600 + 150 + 1500 = 2250
        assert_eq!(combined.network_io_cost, 300.0);
        assert_eq!(combined.compute_cost, 150.0);
        assert_eq!(combined.memory_cost, 0.5);
        assert!((combined.total_cost - 2250.0).abs() < 0.01);
    }

    // ========== Filter Operation Tests ==========

    #[test]
    fn test_filter_operation_is_comparison() {
        assert!(FilterOperation::Equals.is_comparison());
        assert!(FilterOperation::NotEquals.is_comparison());
        assert!(FilterOperation::LessThan.is_comparison());
        assert!(FilterOperation::GreaterThanOrEquals.is_comparison());
        
        assert!(!FilterOperation::Between.is_comparison());
        assert!(!FilterOperation::In { max_values: None }.is_comparison());
        assert!(!FilterOperation::IsNull.is_comparison());
    }

    #[test]
    fn test_filter_operation_is_range() {
        assert!(FilterOperation::Between.is_range());
        assert!(FilterOperation::LessThan.is_range());
        assert!(FilterOperation::GreaterThanOrEquals.is_range());
        assert!(FilterOperation::DateAfter.is_range());
        assert!(FilterOperation::DateBefore.is_range());
        
        assert!(!FilterOperation::Equals.is_range());
        assert!(!FilterOperation::In { max_values: None }.is_range());
    }

    #[test]
    fn test_filter_operation_sql_operator() {
        assert_eq!(FilterOperation::Equals.sql_operator(), Some("="));
        assert_eq!(FilterOperation::NotEquals.sql_operator(), Some("<>"));
        assert_eq!(FilterOperation::LessThan.sql_operator(), Some("<"));
        assert_eq!(FilterOperation::IsNull.sql_operator(), Some("IS NULL"));
        
        // Operations without direct SQL operators
        assert_eq!(FilterOperation::Between.sql_operator(), None);
        assert_eq!(FilterOperation::In { max_values: None }.sql_operator(), None);
    }

    // ========== Column Filter Capability Tests ==========

    #[test]
    fn test_column_filter_capability_new() {
        let cap = ColumnFilterCapability::new([
            FilterOperation::Equals,
            FilterOperation::GreaterThanOrEquals,
        ]);
        
        assert!(cap.supports(&FilterOperation::Equals));
        assert!(cap.supports(&FilterOperation::GreaterThanOrEquals));
        assert!(!cap.supports(&FilterOperation::LessThan));
    }

    #[test]
    fn test_column_filter_capability_builder() {
        let cap = ColumnFilterCapability::new([FilterOperation::Equals])
            .with_api_param("created[eq]")
            .with_transform(ValueTransform::TimestampToEpoch)
            .with_indexed(true)
            .with_max_in_values(10);
        
        assert_eq!(cap.api_param_name, Some("created[eq]".to_string()));
        assert_eq!(cap.value_transform, Some(ValueTransform::TimestampToEpoch));
        assert!(cap.is_indexed);
        assert_eq!(cap.max_in_values, Some(10));
    }

    // ========== Source Capabilities Tests ==========

    #[test]
    fn test_source_capabilities_full_sql() {
        let caps = SourceCapabilities::full_sql_support();
        
        assert!(caps.supports_arbitrary_sql);
        assert!(caps.supports_and);
        assert!(caps.supports_or);
        assert!(caps.supports_not);
        assert!(caps.supports_nested);
        assert!(caps.supports_column_pruning);
        assert!(caps.supports_limit);
        assert!(caps.supports_aggregates);
        assert_eq!(caps.max_filters, None);
        assert!(caps.has_any_pushdown());
    }

    #[test]
    fn test_source_capabilities_no_pushdown() {
        let caps = SourceCapabilities::no_pushdown();
        
        assert!(!caps.supports_arbitrary_sql);
        assert!(!caps.supports_and);
        assert!(!caps.supports_or);
        assert!(!caps.supports_column_pruning);
        assert_eq!(caps.max_filters, Some(0));
        assert!(!caps.has_any_pushdown());
        assert!(caps.full_scan_cost_multiplier > 1.0);
    }

    #[test]
    fn test_source_capabilities_parquet() {
        let caps = SourceCapabilities::parquet_capabilities();
        
        assert!(!caps.supports_arbitrary_sql);
        assert!(caps.supports_and);
        assert!(!caps.supports_or); // Parquet doesn't support OR well
        assert!(caps.supports_column_pruning);
        assert!(caps.supports_operation(&FilterOperation::Equals));
        assert!(caps.supports_operation(&FilterOperation::Between));
        assert!(!caps.supports_operation(&FilterOperation::Regex));
        assert!(caps.has_any_pushdown());
    }

    #[test]
    fn test_source_capabilities_column_override() {
        let caps = SourceCapabilities::no_pushdown()
            .with_column_filter(
                "created",
                ColumnFilterCapability::new([
                    FilterOperation::GreaterThanOrEquals,
                    FilterOperation::LessThanOrEquals,
                ])
            );
        
        // Global: no pushdown
        assert!(!caps.supports_operation(&FilterOperation::Equals));
        
        // But specific column has capabilities
        assert!(caps.supports_column_filter("created", &FilterOperation::GreaterThanOrEquals));
        assert!(caps.supports_column_filter("created", &FilterOperation::LessThanOrEquals));
        assert!(!caps.supports_column_filter("created", &FilterOperation::Equals));
        
        // Other columns still have no pushdown
        assert!(!caps.supports_column_filter("other", &FilterOperation::Equals));
        
        // Now has pushdown since column filters exist
        assert!(caps.has_any_pushdown());
    }

    #[test]
    fn test_source_capabilities_get_column_capability() {
        let caps = SourceCapabilities::no_pushdown()
            .with_column_filter(
                "created",
                ColumnFilterCapability::new([FilterOperation::Equals])
                    .with_api_param("created[eq]")
            );
        
        let col_cap = caps.get_column_capability("created");
        assert!(col_cap.is_some());
        assert_eq!(col_cap.unwrap().api_param_name, Some("created[eq]".to_string()));
        
        assert!(caps.get_column_capability("nonexistent").is_none());
    }

    // ========== FilterOperation Matching Tests ==========

    #[test]
    fn test_filter_operation_in_matching_with_different_max_values() {
        // Test that In with fewer values matches capability with higher max
        let query_op = FilterOperation::In { max_values: Some(5) };
        let cap_op = FilterOperation::In { max_values: Some(100) };
        
        assert!(query_op.matches_capability(&cap_op), 
            "In with 5 values should match capability allowing 100");
        
        // Test that In matches unlimited capability
        let unlimited_cap = FilterOperation::In { max_values: None };
        assert!(query_op.matches_capability(&unlimited_cap),
            "In with 5 values should match unlimited capability");
        
        // Test that In with more values than allowed does NOT match
        let query_large = FilterOperation::In { max_values: Some(150) };
        assert!(!query_large.matches_capability(&cap_op),
            "In with 150 values should NOT match capability allowing 100");
        
        // Test that unlimited query does NOT match limited capability
        let query_unlimited = FilterOperation::In { max_values: None };
        assert!(!query_unlimited.matches_capability(&cap_op),
            "Unlimited In should NOT match limited capability");
        
        // Unlimited matches unlimited
        assert!(query_unlimited.matches_capability(&unlimited_cap),
            "Unlimited In should match unlimited capability");
    }

    #[test]
    fn test_filter_operation_like_matching_with_wildcard() {
        // Query without leading wildcard matches any Like capability
        let query_no_leading = FilterOperation::Like { supports_leading_wildcard: false };
        let cap_with_leading = FilterOperation::Like { supports_leading_wildcard: true };
        let cap_no_leading = FilterOperation::Like { supports_leading_wildcard: false };
        
        assert!(query_no_leading.matches_capability(&cap_with_leading),
            "Like without leading wildcard should match capability with leading wildcard support");
        assert!(query_no_leading.matches_capability(&cap_no_leading),
            "Like without leading wildcard should match capability without leading wildcard support");
        
        // Query WITH leading wildcard only matches capability that supports it
        let query_with_leading = FilterOperation::Like { supports_leading_wildcard: true };
        
        assert!(query_with_leading.matches_capability(&cap_with_leading),
            "Like with leading wildcard should match capability supporting it");
        assert!(!query_with_leading.matches_capability(&cap_no_leading),
            "Like with leading wildcard should NOT match capability without support");
    }

    #[test]
    fn test_filter_operation_simple_variant_matching() {
        // Simple variants match themselves
        assert!(FilterOperation::Equals.matches_capability(&FilterOperation::Equals));
        assert!(FilterOperation::NotEquals.matches_capability(&FilterOperation::NotEquals));
        assert!(FilterOperation::LessThan.matches_capability(&FilterOperation::LessThan));
        assert!(FilterOperation::IsNull.matches_capability(&FilterOperation::IsNull));
        
        // Different simple variants don't match
        assert!(!FilterOperation::Equals.matches_capability(&FilterOperation::NotEquals));
        assert!(!FilterOperation::LessThan.matches_capability(&FilterOperation::GreaterThan));
        assert!(!FilterOperation::IsNull.matches_capability(&FilterOperation::IsNotNull));
    }

    #[test]
    fn test_filter_operation_custom_matching() {
        let custom1 = FilterOperation::Custom("special_filter".to_string());
        let custom2 = FilterOperation::Custom("special_filter".to_string());
        let custom3 = FilterOperation::Custom("other_filter".to_string());
        
        assert!(custom1.matches_capability(&custom2), "Same custom should match");
        assert!(!custom1.matches_capability(&custom3), "Different custom should not match");
    }

    #[test]
    fn test_filter_operation_canonical() {
        // In variants all canonicalize to unlimited
        let in1 = FilterOperation::In { max_values: Some(5) };
        let in2 = FilterOperation::In { max_values: Some(100) };
        let in3 = FilterOperation::In { max_values: None };
        
        assert_eq!(in1.canonical(), FilterOperation::In { max_values: None });
        assert_eq!(in2.canonical(), FilterOperation::In { max_values: None });
        assert_eq!(in3.canonical(), FilterOperation::In { max_values: None });
        
        // Like variants canonicalize to supports_leading_wildcard: true
        let like1 = FilterOperation::Like { supports_leading_wildcard: true };
        let like2 = FilterOperation::Like { supports_leading_wildcard: false };
        
        assert_eq!(like1.canonical(), FilterOperation::Like { supports_leading_wildcard: true });
        assert_eq!(like2.canonical(), FilterOperation::Like { supports_leading_wildcard: true });
        
        // Simple variants return themselves
        assert_eq!(FilterOperation::Equals.canonical(), FilterOperation::Equals);
    }

    #[test]
    fn test_source_capabilities_in_with_different_sizes() {
        // Create capabilities that support In with max 100 values
        let caps = SourceCapabilities {
            supported_operations: [
                FilterOperation::In { max_values: Some(100) },
            ].into_iter().collect(),
            column_filters: AHashMap::new(),
            supports_arbitrary_sql: false,
            supports_and: true,
            supports_or: false,
            supports_not: false,
            supports_nested: false,
            max_filters: None,
            full_scan_cost_multiplier: 1.0,
            supports_column_pruning: false,
            supports_limit: false,
            supports_order_by: false,
            supports_aggregates: false,
            limitations_description: None,
        };
        
        // Query with fewer values should be supported
        assert!(caps.supports_operation(&FilterOperation::In { max_values: Some(5) }),
            "In with 5 values should be supported by capability allowing 100");
        assert!(caps.supports_operation(&FilterOperation::In { max_values: Some(50) }),
            "In with 50 values should be supported by capability allowing 100");
        assert!(caps.supports_operation(&FilterOperation::In { max_values: Some(100) }),
            "In with exactly 100 values should be supported");
        
        // Query with more values should NOT be supported
        assert!(!caps.supports_operation(&FilterOperation::In { max_values: Some(101) }),
            "In with 101 values should NOT be supported by capability allowing 100");
        assert!(!caps.supports_operation(&FilterOperation::In { max_values: Some(1000) }),
            "In with 1000 values should NOT be supported by capability allowing 100");
    }

    #[test]
    fn test_column_filter_in_with_different_sizes() {
        let cap = ColumnFilterCapability::new([
            FilterOperation::Equals,
            FilterOperation::In { max_values: Some(50) },
        ]);
        
        // Fewer values should be supported
        assert!(cap.supports(&FilterOperation::In { max_values: Some(10) }));
        assert!(cap.supports(&FilterOperation::In { max_values: Some(50) }));
        
        // More values should NOT be supported
        assert!(!cap.supports(&FilterOperation::In { max_values: Some(51) }));
        assert!(!cap.supports(&FilterOperation::In { max_values: Some(100) }));
    }

    // ==================== Edge Case / Zero-Value Tests ====================

    #[test]
    fn test_rate_limit_zero_rows_per_request() {
        let rate_limit = RateLimitInfo::new(100, 1, 0);
        // rows_per_request = 0 with row_count > 0 is an impossible fetch
        let time = rate_limit.estimate_fetch_time_ms(1000);
        assert_eq!(time, f64::MAX);
    }

    #[test]
    fn test_rate_limit_zero_max_requests() {
        let rate_limit = RateLimitInfo::new(0, 1, 100);
        // max_requests = 0 -> requests_per_ms = 0 -> should return f64::MAX
        let time = rate_limit.estimate_fetch_time_ms(1000);
        assert_eq!(time, f64::MAX);
    }

    #[test]
    fn test_rate_limit_zero_window_secs() {
        let rate_limit = RateLimitInfo::new(100, 0, 100);
        let time = rate_limit.estimate_fetch_time_ms(1000);
        assert_eq!(
            time,
            f64::MAX,
            "Zero window means no requests can complete — fetch time must be f64::MAX"
        );
    }

    #[test]
    fn test_rate_limit_zero_row_count() {
        let rate_limit = RateLimitInfo::new(100, 1, 100);
        let time = rate_limit.estimate_fetch_time_ms(0);
        assert_eq!(time, 0.0);
    }

    #[test]
    fn test_transfer_time_zero_bandwidth() {
        let profile = SourceAccessProfile::new(10..=50, 0..=0, ParallelismLevel::Medium);
        // Zero bandwidth -> should return f64::MAX (no transfer possible)
        let time = profile.estimate_transfer_time_ms(1_000_000);
        assert_eq!(time, f64::MAX);
    }

    #[test]
    fn test_transfer_time_local_source() {
        let profile = SourceAccessProfile::new(10..=50, 0..=0, ParallelismLevel::Medium)
            .with_local(true);
        // Local source should return minimal time regardless of bandwidth
        let time = profile.estimate_transfer_time_ms(1_000_000_000);
        assert_eq!(time, 1.0);
    }

    #[test]
    fn test_query_cost_scale_zero() {
        let cost = QueryCost::new(100.0, 200.0, 0.5);
        let scaled = cost.scale(0.0);
        assert_eq!(scaled.network_io_cost, 0.0);
        assert_eq!(scaled.compute_cost, 0.0);
        // Memory doesn't scale
        assert_eq!(scaled.memory_cost, 0.5);
    }

    #[test]
    fn test_cost_model_calculate_total_all_zero() {
        let model = CostModel::default();
        let total = model.calculate_total(0.0, 0.0, 0.0);
        assert_eq!(total, 0.0);
    }

    #[test]
    fn test_build_side_both_exceed_budget() {
        let model = CostModel::default().with_memory_budget(1); // 1 MB budget
        let left_profile = SourceAccessProfile::default_for_source_type(SourceType::PostgreSQL);
        let right_profile = SourceAccessProfile::default_for_source_type(SourceType::PostgreSQL);

        // Both sides exceed 1 MB budget
        let side = BuildSide::select(
            1000, 10_000_000,  // ~10 MB
            &left_profile,
            2000, 20_000_000,  // ~20 MB
            &right_profile,
            &model,
        );
        // When both exceed budget, falls through to cost comparison -> left is cheaper
        assert_eq!(side, BuildSide::Left);
    }

    #[test]
    fn test_build_side_equal_costs() {
        let model = CostModel::default();
        let profile = SourceAccessProfile::default_for_source_type(SourceType::PostgreSQL);

        let side = BuildSide::select(
            1000, 100_000,
            &profile,
            1000, 100_000,
            &profile,
            &model,
        );
        // Equal costs -> Left wins (left_total <= right_total)
        assert_eq!(side, BuildSide::Left);
    }

    #[test]
    fn test_estimate_scan_cost_zero_rows() {
        let model = CostModel::default();
        let profile = SourceAccessProfile::default_for_source_type(SourceType::PostgreSQL);
        let cost = model.estimate_scan_cost(0, 0, &profile, 1.0);
        assert_eq!(cost.compute_cost, 0.0);
    }

    #[test]
    fn test_scan_cost_no_pushdown_uses_full_row_count_for_network() {
        let model = CostModel::default();

        let api_profile = SourceAccessProfile::default_for_source_type(SourceType::Stripe);
        assert!(!api_profile.supports_predicate_pushdown);

        let db_profile = SourceAccessProfile::default_for_source_type(SourceType::PostgreSQL);
        assert!(db_profile.supports_predicate_pushdown);

        let rows = 100_000_u64;
        let bytes = 10_000_000_u64;
        let selectivity = 0.1;

        let api_cost = model.estimate_scan_cost(rows, bytes, &api_profile, selectivity);
        let api_cost_full = model.estimate_scan_cost(rows, bytes, &api_profile, 1.0);

        assert!(
            (api_cost.network_io_cost - api_cost_full.network_io_cost).abs() < 1e-6,
            "API source without pushdown: network cost must use full row count regardless of selectivity. \
             sel=0.1 got {}, sel=1.0 got {}",
            api_cost.network_io_cost,
            api_cost_full.network_io_cost,
        );

        let db_cost_filtered = model.estimate_scan_cost(rows, bytes, &db_profile, selectivity);
        let db_cost_full = model.estimate_scan_cost(rows, bytes, &db_profile, 1.0);
        assert!(
            db_cost_filtered.network_io_cost < db_cost_full.network_io_cost,
            "DB source with pushdown: selectivity should reduce network cost"
        );
    }

    #[test]
    fn test_custom_weights_affect_scan_cost() {
        let default_model = CostModel::default();
        let custom_model = CostModel {
            network_weight: 10.0,
            compute_weight: 0.1,
            memory_weight: 0.1,
            ..Default::default()
        };

        let profile = SourceAccessProfile::default_for_source_type(SourceType::PostgreSQL);
        let default_cost = default_model.estimate_scan_cost(10_000, 1_000_000, &profile, 1.0);
        let custom_cost = custom_model.estimate_scan_cost(10_000, 1_000_000, &profile, 1.0);

        assert_ne!(
            default_cost.total_cost, custom_cost.total_cost,
            "Custom weights must produce a different total cost than default weights. \
             default={}, custom={}",
            default_cost.total_cost, custom_cost.total_cost
        );
    }

    #[test]
    fn test_custom_weights_affect_hash_join_cost() {
        let default_model = CostModel::default();
        let custom_model = CostModel {
            network_weight: 0.1,
            compute_weight: 10.0,
            memory_weight: 0.1,
            ..Default::default()
        };

        let default_cost = default_model.estimate_hash_join_cost(10_000, 1_000_000, 50_000, 5_000_000);
        let custom_cost = custom_model.estimate_hash_join_cost(10_000, 1_000_000, 50_000, 5_000_000);

        assert_ne!(
            default_cost.total_cost, custom_cost.total_cost,
            "Custom weights must produce a different hash join cost. \
             default={}, custom={}",
            default_cost.total_cost, custom_cost.total_cost
        );
    }
}
