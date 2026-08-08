//! Source Capability Matrix
//!
//! Defines the filtering capabilities for each data source type.
//! This is used by the query planner to determine which predicates
//! can be pushed down to each source.
//!
//! # Capability Categories
//!
//! - **Full SQL**: PostgreSQL, MySQL, ClickHouse - support all SQL predicates
//! - **Limited API**: Stripe, HubSpot - support specific filters only
//! - **File-based**: CSV, Excel - no pushdown, must scan all data
//! - **Columnar**: Parquet - column pruning and row group filtering
//!
//! # Example
//!
//! ```ignore
//! use crate::warehouse::query::source_capabilities::SourceCapabilityMatrix;
//! use crate::warehouse::types::SourceType;
//!
//! let caps = SourceCapabilityMatrix::for_source_type(SourceType::Stripe);
//! if caps.supports_column_filter("created", &FilterOperation::GreaterThanOrEquals) {
//!     // Push down the date filter to Stripe API
//! }
//! ```

use ahash::{AHashMap, AHashSet};

use super::cost_model::{
    ColumnFilterCapability, FilterOperation, SourceCapabilities, ValueTransform,
};
use crate::warehouse::types::SourceType;

/// Factory for creating source capability configurations.
///
/// This struct provides a centralized way to get the filtering
/// capabilities for any source type.
pub struct SourceCapabilityMatrix;

impl SourceCapabilityMatrix {
    /// Get capabilities for a specific source type.
    pub fn for_source_type(source_type: SourceType) -> SourceCapabilities {
        match source_type {
            // ===== Full SQL Databases =====
            SourceType::PostgreSQL => Self::postgresql_capabilities(),
            SourceType::MySQL => Self::mysql_capabilities(),
            SourceType::SqlServer => Self::sqlserver_capabilities(),
            SourceType::SQLite => Self::sqlite_capabilities(),

            // ===== Document Databases =====
            SourceType::MongoDB => Self::mongodb_capabilities(),

            // ===== Cloud Data Warehouses =====
            SourceType::Snowflake => Self::snowflake_capabilities(),
            SourceType::BigQuery => Self::bigquery_capabilities(),
            SourceType::Redshift => Self::redshift_capabilities(),
            SourceType::ClickHouse => Self::clickhouse_capabilities(),

            // ===== Payment/Finance APIs =====
            SourceType::Stripe => Self::stripe_capabilities(),
            SourceType::QuickBooks => Self::quickbooks_capabilities(),
            SourceType::Xero => Self::xero_capabilities(),

            // ===== CRM/Sales APIs =====
            SourceType::HubSpot => Self::hubspot_capabilities(),
            SourceType::Salesforce => Self::salesforce_capabilities(),
            SourceType::Zendesk => Self::zendesk_capabilities(),
            SourceType::Intercom => Self::intercom_capabilities(),

            // ===== E-commerce APIs =====
            SourceType::Shopify => Self::shopify_capabilities(),
            SourceType::WooCommerce => Self::woocommerce_capabilities(),

            // ===== Analytics APIs =====
            SourceType::GoogleAnalytics => Self::google_analytics_capabilities(),
            SourceType::Mixpanel => Self::mixpanel_capabilities(),
            SourceType::Amplitude => Self::amplitude_capabilities(),
            SourceType::PostHog => Self::posthog_capabilities(),

            // ===== Ads APIs =====
            SourceType::FacebookAds => Self::facebook_ads_capabilities(),
            SourceType::GoogleAds => Self::google_ads_capabilities(),

            // ===== Dev Tools APIs =====
            SourceType::GitHub => Self::github_capabilities(),
            SourceType::Jira => Self::jira_capabilities(),
            SourceType::Linear => Self::linear_capabilities(),

            // ===== Productivity APIs =====
            SourceType::GoogleSheets => Self::google_sheets_capabilities(),
            SourceType::Notion => Self::notion_capabilities(),
            SourceType::Airtable => Self::airtable_capabilities(),
            SourceType::Asana => Self::asana_capabilities(),
            SourceType::Monday => Self::monday_capabilities(),
            SourceType::Confluence => Self::confluence_capabilities(),

            // ===== File Formats =====
            SourceType::Csv => Self::csv_capabilities(),
            SourceType::Json => Self::json_capabilities(),
            SourceType::Excel => Self::excel_capabilities(),
            SourceType::Xml => Self::xml_capabilities(),
            SourceType::ExternalParquet => Self::parquet_capabilities(),

            // ===== Cloud Storage =====
            SourceType::GoogleCloudStorage => Self::cloud_storage_capabilities(),
            SourceType::AzureBlob => Self::cloud_storage_capabilities(),

            // ===== Streaming =====
            SourceType::Kafka => Self::streaming_capabilities(),
            SourceType::AwsKinesis => Self::streaming_capabilities(),

            // ===== Blockchain =====
            SourceType::Ethereum
            | SourceType::Solana
            | SourceType::Bitcoin
            | SourceType::Polygon => Self::blockchain_capabilities(),

            // Derived tables use Parquet on R2 — same capabilities as external Parquet
            SourceType::Derived => Self::parquet_capabilities(),
        }
    }

    // ========================================================================
    // Full SQL Databases
    // ========================================================================

    fn postgresql_capabilities() -> SourceCapabilities {
        SourceCapabilities::full_sql_support()
            .with_limitations("PostgreSQL supports all SQL predicates including regex (~, ~*).")
    }

    fn mysql_capabilities() -> SourceCapabilities {
        SourceCapabilities::full_sql_support()
            .with_limitations("MySQL supports all SQL predicates. REGEXP available for regex.")
    }

    fn sqlserver_capabilities() -> SourceCapabilities {
        SourceCapabilities::full_sql_support()
            .with_limitations("SQL Server supports all SQL predicates. LIKE patterns for regex-like matching.")
    }

    fn sqlite_capabilities() -> SourceCapabilities {
        SourceCapabilities::full_sql_support()
            .with_limitations("SQLite supports all standard SQL predicates. GLOB for pattern matching.")
    }

    fn mongodb_capabilities() -> SourceCapabilities {
        SourceCapabilities {
            supported_operations: [
                FilterOperation::Equals,
                FilterOperation::NotEquals,
                FilterOperation::LessThan,
                FilterOperation::LessThanOrEquals,
                FilterOperation::GreaterThan,
                FilterOperation::GreaterThanOrEquals,
                FilterOperation::In { max_values: None },
                FilterOperation::IsNull,
                FilterOperation::IsNotNull,
                FilterOperation::Regex,
                FilterOperation::ArrayContains,
            ]
            .into_iter()
            .collect(),
            column_filters: AHashMap::new(),
            supports_arbitrary_sql: false, // MongoDB uses its own query language
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
            limitations_description: Some(
                "MongoDB supports most query operations but uses its own query language, not SQL."
                    .to_string(),
            ),
        }
    }

    // ========================================================================
    // Cloud Data Warehouses
    // ========================================================================

    fn snowflake_capabilities() -> SourceCapabilities {
        SourceCapabilities::full_sql_support()
            .with_limitations("Snowflake supports all SQL predicates including RLIKE for regex.")
    }

    fn bigquery_capabilities() -> SourceCapabilities {
        SourceCapabilities::full_sql_support()
            .with_limitations("BigQuery supports all SQL predicates including REGEXP_CONTAINS.")
    }

    fn redshift_capabilities() -> SourceCapabilities {
        SourceCapabilities::full_sql_support()
            .with_limitations("Redshift supports all SQL predicates. SIMILAR TO for regex-like matching.")
    }

    fn clickhouse_capabilities() -> SourceCapabilities {
        SourceCapabilities::full_sql_support()
            .with_limitations("ClickHouse supports all SQL predicates. MATCH for regex matching.")
    }

    // ========================================================================
    // Payment/Finance APIs
    // ========================================================================

    fn stripe_capabilities() -> SourceCapabilities {
        let mut column_filters = AHashMap::new();

        // Stripe common filters across most resources
        column_filters.insert(
            "created".to_string(),
            ColumnFilterCapability::new([
                FilterOperation::GreaterThanOrEquals,
                FilterOperation::LessThanOrEquals,
                FilterOperation::GreaterThan,
                FilterOperation::LessThan,
            ])
            .with_transform(ValueTransform::TimestampToEpoch)
            .with_indexed(true),
        );

        // Customer filter for charges, invoices, etc.
        column_filters.insert(
            "customer".to_string(),
            ColumnFilterCapability::new([FilterOperation::Equals])
                .with_indexed(true),
        );

        // Status filter for many resources
        column_filters.insert(
            "status".to_string(),
            ColumnFilterCapability::new([FilterOperation::Equals]),
        );

        // Subscription filter for invoices
        column_filters.insert(
            "subscription".to_string(),
            ColumnFilterCapability::new([FilterOperation::Equals]),
        );

        SourceCapabilities {
            supported_operations: AHashSet::new(), // No global ops, only column-specific
            column_filters,
            supports_arbitrary_sql: false,
            supports_and: true,
            supports_or: false,
            supports_not: false,
            supports_nested: false,
            max_filters: Some(10),
            full_scan_cost_multiplier: 10.0, // API calls are expensive
            supports_column_pruning: false,
            supports_limit: true, // Stripe supports limit parameter
            supports_order_by: false,
            supports_aggregates: false,
            limitations_description: Some(
                "Stripe API supports limited filters: created date ranges, customer/subscription IDs, \
                 and status. Other predicates must be applied after fetching.".to_string()
            ),
        }
    }

    fn quickbooks_capabilities() -> SourceCapabilities {
        let mut column_filters = AHashMap::new();

        // QuickBooks supports date range queries
        column_filters.insert(
            "TxnDate".to_string(),
            ColumnFilterCapability::new([
                FilterOperation::GreaterThanOrEquals,
                FilterOperation::LessThanOrEquals,
            ])
            .with_transform(ValueTransform::DateToIso8601),
        );

        column_filters.insert(
            "CreateTime".to_string(),
            ColumnFilterCapability::new([
                FilterOperation::GreaterThanOrEquals,
                FilterOperation::LessThanOrEquals,
            ])
            .with_transform(ValueTransform::DateTimeToIso8601),
        );

        SourceCapabilities {
            supported_operations: AHashSet::new(),
            column_filters,
            supports_arbitrary_sql: false,
            supports_and: true,
            supports_or: false,
            supports_not: false,
            supports_nested: false,
            max_filters: Some(5),
            full_scan_cost_multiplier: 8.0,
            supports_column_pruning: false,
            supports_limit: true,
            supports_order_by: true,
            supports_aggregates: false,
            limitations_description: Some(
                "QuickBooks supports date range filters on TxnDate and CreateTime. \
                 Most other filters must be applied locally.".to_string()
            ),
        }
    }

    fn xero_capabilities() -> SourceCapabilities {
        let mut column_filters = AHashMap::new();

        column_filters.insert(
            "UpdatedDateUTC".to_string(),
            ColumnFilterCapability::new([FilterOperation::GreaterThanOrEquals])
                .with_transform(ValueTransform::DateTimeToIso8601),
        );

        SourceCapabilities {
            supported_operations: AHashSet::new(),
            column_filters,
            supports_arbitrary_sql: false,
            supports_and: false,
            supports_or: false,
            supports_not: false,
            supports_nested: false,
            max_filters: Some(1), // Xero is very limited
            full_scan_cost_multiplier: 15.0,
            supports_column_pruning: false,
            supports_limit: false,
            supports_order_by: false,
            supports_aggregates: false,
            limitations_description: Some(
                "Xero only supports filtering by UpdatedDateUTC for incremental sync. \
                 All other filtering must be done locally.".to_string()
            ),
        }
    }

    // ========================================================================
    // CRM/Sales APIs
    // ========================================================================

    fn hubspot_capabilities() -> SourceCapabilities {
        let mut column_filters = AHashMap::new();

        column_filters.insert(
            "createdate".to_string(),
            ColumnFilterCapability::new([
                FilterOperation::GreaterThanOrEquals,
                FilterOperation::LessThanOrEquals,
            ])
            .with_transform(ValueTransform::TimestampToEpochMs),
        );

        column_filters.insert(
            "lastmodifieddate".to_string(),
            ColumnFilterCapability::new([
                FilterOperation::GreaterThanOrEquals,
                FilterOperation::LessThanOrEquals,
            ])
            .with_transform(ValueTransform::TimestampToEpochMs),
        );

        SourceCapabilities {
            supported_operations: AHashSet::new(),
            column_filters,
            supports_arbitrary_sql: false,
            supports_and: true,
            supports_or: false,
            supports_not: false,
            supports_nested: false,
            max_filters: Some(3),
            full_scan_cost_multiplier: 8.0,
            supports_column_pruning: true, // HubSpot supports property selection
            supports_limit: true,
            supports_order_by: false,
            supports_aggregates: false,
            limitations_description: Some(
                "HubSpot supports date range filters and property selection. \
                 Complex filters must be applied after fetching.".to_string()
            ),
        }
    }

    fn salesforce_capabilities() -> SourceCapabilities {
        // Salesforce uses SOQL which is SQL-like but with limitations
        SourceCapabilities {
            supported_operations: [
                FilterOperation::Equals,
                FilterOperation::NotEquals,
                FilterOperation::LessThan,
                FilterOperation::LessThanOrEquals,
                FilterOperation::GreaterThan,
                FilterOperation::GreaterThanOrEquals,
                FilterOperation::In { max_values: Some(200) },
                FilterOperation::Like { supports_leading_wildcard: true },
                FilterOperation::IsNull,
                FilterOperation::IsNotNull,
            ]
            .into_iter()
            .collect(),
            column_filters: AHashMap::new(),
            supports_arbitrary_sql: false, // SOQL, not SQL
            supports_and: true,
            supports_or: true,
            supports_not: true,
            supports_nested: true,
            max_filters: None,
            full_scan_cost_multiplier: 2.0,
            supports_column_pruning: true,
            supports_limit: true,
            supports_order_by: true,
            supports_aggregates: true, // SOQL supports basic aggregates
            limitations_description: Some(
                "Salesforce SOQL supports most SQL-like operations but with some limitations \
                 on complex expressions and functions.".to_string()
            ),
        }
    }

    fn zendesk_capabilities() -> SourceCapabilities {
        let mut column_filters = AHashMap::new();

        column_filters.insert(
            "updated_at".to_string(),
            ColumnFilterCapability::new([FilterOperation::GreaterThanOrEquals])
                .with_transform(ValueTransform::DateTimeToIso8601),
        );

        column_filters.insert(
            "status".to_string(),
            ColumnFilterCapability::new([FilterOperation::Equals]),
        );

        SourceCapabilities {
            supported_operations: AHashSet::new(),
            column_filters,
            supports_arbitrary_sql: false,
            supports_and: true,
            supports_or: false,
            supports_not: false,
            supports_nested: false,
            max_filters: Some(5),
            full_scan_cost_multiplier: 6.0,
            supports_column_pruning: false,
            supports_limit: true,
            supports_order_by: true,
            supports_aggregates: false,
            limitations_description: Some(
                "Zendesk supports filtering by updated_at and status. \
                 Use incremental sync for large datasets.".to_string()
            ),
        }
    }

    fn intercom_capabilities() -> SourceCapabilities {
        let mut column_filters = AHashMap::new();

        column_filters.insert(
            "updated_at".to_string(),
            ColumnFilterCapability::new([FilterOperation::GreaterThan])
                .with_transform(ValueTransform::TimestampToEpoch),
        );

        SourceCapabilities {
            supported_operations: AHashSet::new(),
            column_filters,
            supports_arbitrary_sql: false,
            supports_and: false,
            supports_or: false,
            supports_not: false,
            supports_nested: false,
            max_filters: Some(1),
            full_scan_cost_multiplier: 10.0,
            supports_column_pruning: false,
            supports_limit: true,
            supports_order_by: false,
            supports_aggregates: false,
            limitations_description: Some(
                "Intercom only supports filtering by updated_at for incremental sync."
                    .to_string()
            ),
        }
    }

    // ========================================================================
    // E-commerce APIs
    // ========================================================================

    fn shopify_capabilities() -> SourceCapabilities {
        let mut column_filters = AHashMap::new();

        column_filters.insert(
            "created_at".to_string(),
            ColumnFilterCapability::new([
                FilterOperation::GreaterThanOrEquals,
                FilterOperation::LessThanOrEquals,
            ])
            .with_api_param("created_at_min/created_at_max")
            .with_transform(ValueTransform::DateTimeToIso8601),
        );

        column_filters.insert(
            "updated_at".to_string(),
            ColumnFilterCapability::new([
                FilterOperation::GreaterThanOrEquals,
                FilterOperation::LessThanOrEquals,
            ])
            .with_api_param("updated_at_min/updated_at_max")
            .with_transform(ValueTransform::DateTimeToIso8601),
        );

        column_filters.insert(
            "status".to_string(),
            ColumnFilterCapability::new([FilterOperation::Equals]),
        );

        SourceCapabilities {
            supported_operations: AHashSet::new(),
            column_filters,
            supports_arbitrary_sql: false,
            supports_and: true,
            supports_or: false,
            supports_not: false,
            supports_nested: false,
            max_filters: Some(10),
            full_scan_cost_multiplier: 5.0,
            supports_column_pruning: true, // GraphQL field selection
            supports_limit: true,
            supports_order_by: false,
            supports_aggregates: false,
            limitations_description: Some(
                "Shopify supports date range filters and status filtering. \
                 GraphQL queries support field selection.".to_string()
            ),
        }
    }

    fn woocommerce_capabilities() -> SourceCapabilities {
        let mut column_filters = AHashMap::new();

        column_filters.insert(
            "date_modified".to_string(),
            ColumnFilterCapability::new([
                FilterOperation::GreaterThanOrEquals,
                FilterOperation::LessThanOrEquals,
            ])
            .with_transform(ValueTransform::DateTimeToIso8601),
        );

        column_filters.insert(
            "status".to_string(),
            ColumnFilterCapability::new([FilterOperation::Equals]),
        );

        SourceCapabilities {
            supported_operations: AHashSet::new(),
            column_filters,
            supports_arbitrary_sql: false,
            supports_and: true,
            supports_or: false,
            supports_not: false,
            supports_nested: false,
            max_filters: Some(5),
            full_scan_cost_multiplier: 6.0,
            supports_column_pruning: false,
            supports_limit: true,
            supports_order_by: true,
            supports_aggregates: false,
            limitations_description: Some(
                "WooCommerce supports date and status filters via REST API parameters."
                    .to_string()
            ),
        }
    }

    // ========================================================================
    // Analytics APIs
    // ========================================================================

    fn google_analytics_capabilities() -> SourceCapabilities {
        let mut column_filters = AHashMap::new();

        // GA4 requires date range
        column_filters.insert(
            "date".to_string(),
            ColumnFilterCapability::new([
                FilterOperation::GreaterThanOrEquals,
                FilterOperation::LessThanOrEquals,
                FilterOperation::Between,
            ])
            .with_transform(ValueTransform::DateToIso8601)
            .with_indexed(true),
        );

        SourceCapabilities {
            supported_operations: [
                FilterOperation::Equals,
                FilterOperation::In { max_values: Some(10) },
            ]
            .into_iter()
            .collect(),
            column_filters,
            supports_arbitrary_sql: false,
            supports_and: true,
            supports_or: true,
            supports_not: false,
            supports_nested: false,
            max_filters: Some(10),
            full_scan_cost_multiplier: 3.0,
            supports_column_pruning: true, // Select dimensions and metrics
            supports_limit: true,
            supports_order_by: true,
            supports_aggregates: true, // GA is inherently aggregated
            limitations_description: Some(
                "Google Analytics requires date range and supports dimension/metric filters. \
                 Data is pre-aggregated.".to_string()
            ),
        }
    }

    fn mixpanel_capabilities() -> SourceCapabilities {
        let mut column_filters = AHashMap::new();

        column_filters.insert(
            "time".to_string(),
            ColumnFilterCapability::new([
                FilterOperation::GreaterThanOrEquals,
                FilterOperation::LessThanOrEquals,
            ])
            .with_transform(ValueTransform::DateTimeToIso8601),
        );

        SourceCapabilities {
            supported_operations: [FilterOperation::Equals].into_iter().collect(),
            column_filters,
            supports_arbitrary_sql: false,
            supports_and: true,
            supports_or: false,
            supports_not: false,
            supports_nested: false,
            max_filters: Some(5),
            full_scan_cost_multiplier: 4.0,
            supports_column_pruning: true,
            supports_limit: true,
            supports_order_by: false,
            supports_aggregates: true,
            limitations_description: Some(
                "Mixpanel supports date range and property filters.".to_string()
            ),
        }
    }

    fn amplitude_capabilities() -> SourceCapabilities {
        Self::mixpanel_capabilities() // Similar capabilities
            .with_limitations("Amplitude supports date range and property filters.")
    }

    fn posthog_capabilities() -> SourceCapabilities {
        SourceCapabilities {
            supported_operations: [
                FilterOperation::Equals,
                FilterOperation::NotEquals,
                FilterOperation::Contains,
                FilterOperation::In { max_values: Some(20) },
            ]
            .into_iter()
            .collect(),
            column_filters: AHashMap::new(),
            supports_arbitrary_sql: false,
            supports_and: true,
            supports_or: true,
            supports_not: true,
            supports_nested: false,
            max_filters: Some(10),
            full_scan_cost_multiplier: 3.0,
            supports_column_pruning: true,
            supports_limit: true,
            supports_order_by: false,
            supports_aggregates: true,
            limitations_description: Some(
                "PostHog supports property filters with AND/OR logic.".to_string()
            ),
        }
    }

    // ========================================================================
    // Ads APIs
    // ========================================================================

    fn facebook_ads_capabilities() -> SourceCapabilities {
        let mut column_filters = AHashMap::new();

        column_filters.insert(
            "date_preset".to_string(),
            ColumnFilterCapability::new([FilterOperation::Equals]),
        );

        column_filters.insert(
            "time_range".to_string(),
            ColumnFilterCapability::new([FilterOperation::Between]),
        );

        SourceCapabilities {
            supported_operations: AHashSet::new(),
            column_filters,
            supports_arbitrary_sql: false,
            supports_and: true,
            supports_or: false,
            supports_not: false,
            supports_nested: false,
            max_filters: Some(5),
            full_scan_cost_multiplier: 5.0,
            supports_column_pruning: true, // Select fields
            supports_limit: true,
            supports_order_by: false,
            supports_aggregates: true,
            limitations_description: Some(
                "Facebook Ads requires date range and supports field selection."
                    .to_string()
            ),
        }
    }

    fn google_ads_capabilities() -> SourceCapabilities {
        // Google Ads uses GAQL (Google Ads Query Language)
        SourceCapabilities {
            supported_operations: [
                FilterOperation::Equals,
                FilterOperation::NotEquals,
                FilterOperation::LessThan,
                FilterOperation::LessThanOrEquals,
                FilterOperation::GreaterThan,
                FilterOperation::GreaterThanOrEquals,
                FilterOperation::In { max_values: Some(20) },
                FilterOperation::Between,
            ]
            .into_iter()
            .collect(),
            column_filters: AHashMap::new(),
            supports_arbitrary_sql: false, // GAQL, not SQL
            supports_and: true,
            supports_or: false, // GAQL doesn't support OR
            supports_not: true,
            supports_nested: false,
            max_filters: None,
            full_scan_cost_multiplier: 2.0,
            supports_column_pruning: true,
            supports_limit: true,
            supports_order_by: true,
            supports_aggregates: true,
            limitations_description: Some(
                "Google Ads uses GAQL which supports most filters but not OR conditions."
                    .to_string()
            ),
        }
    }

    // ========================================================================
    // Dev Tools APIs
    // ========================================================================

    fn github_capabilities() -> SourceCapabilities {
        let mut column_filters = AHashMap::new();

        column_filters.insert(
            "updated_at".to_string(),
            ColumnFilterCapability::new([
                FilterOperation::GreaterThanOrEquals,
                FilterOperation::LessThanOrEquals,
            ])
            .with_transform(ValueTransform::DateTimeToIso8601),
        );

        column_filters.insert(
            "state".to_string(),
            ColumnFilterCapability::new([FilterOperation::Equals]),
        );

        SourceCapabilities {
            supported_operations: [FilterOperation::Equals].into_iter().collect(),
            column_filters,
            supports_arbitrary_sql: false,
            supports_and: true,
            supports_or: false,
            supports_not: false,
            supports_nested: false,
            max_filters: Some(10),
            full_scan_cost_multiplier: 3.0,
            supports_column_pruning: true, // GraphQL field selection
            supports_limit: true,
            supports_order_by: true,
            supports_aggregates: false,
            limitations_description: Some(
                "GitHub supports date, state, and other filters via GraphQL/REST."
                    .to_string()
            ),
        }
    }

    fn jira_capabilities() -> SourceCapabilities {
        // Jira uses JQL (Jira Query Language)
        SourceCapabilities {
            supported_operations: [
                FilterOperation::Equals,
                FilterOperation::NotEquals,
                FilterOperation::LessThan,
                FilterOperation::LessThanOrEquals,
                FilterOperation::GreaterThan,
                FilterOperation::GreaterThanOrEquals,
                FilterOperation::In { max_values: None },
                FilterOperation::Like { supports_leading_wildcard: false },
                FilterOperation::IsNull,
                FilterOperation::IsNotNull,
            ]
            .into_iter()
            .collect(),
            column_filters: AHashMap::new(),
            supports_arbitrary_sql: false, // JQL, not SQL
            supports_and: true,
            supports_or: true,
            supports_not: true,
            supports_nested: true,
            max_filters: None,
            full_scan_cost_multiplier: 2.0,
            supports_column_pruning: true,
            supports_limit: true,
            supports_order_by: true,
            supports_aggregates: false,
            limitations_description: Some(
                "Jira uses JQL which supports most filter operations. Use JQL syntax for best results."
                    .to_string()
            ),
        }
    }

    fn linear_capabilities() -> SourceCapabilities {
        SourceCapabilities {
            supported_operations: [
                FilterOperation::Equals,
                FilterOperation::In { max_values: Some(50) },
            ]
            .into_iter()
            .collect(),
            column_filters: AHashMap::new(),
            supports_arbitrary_sql: false,
            supports_and: true,
            supports_or: true,
            supports_not: false,
            supports_nested: false,
            max_filters: Some(10),
            full_scan_cost_multiplier: 3.0,
            supports_column_pruning: true, // GraphQL
            supports_limit: true,
            supports_order_by: true,
            supports_aggregates: false,
            limitations_description: Some(
                "Linear supports GraphQL filtering on most fields.".to_string()
            ),
        }
    }

    // ========================================================================
    // Productivity APIs
    // ========================================================================

    fn google_sheets_capabilities() -> SourceCapabilities {
        SourceCapabilities::no_pushdown()
            .with_limitations("Google Sheets does not support filter pushdown. All data must be fetched.")
    }

    fn notion_capabilities() -> SourceCapabilities {
        let mut column_filters = AHashMap::new();

        column_filters.insert(
            "last_edited_time".to_string(),
            ColumnFilterCapability::new([
                FilterOperation::GreaterThanOrEquals,
                FilterOperation::LessThanOrEquals,
            ])
            .with_transform(ValueTransform::DateTimeToIso8601),
        );

        SourceCapabilities {
            supported_operations: [
                FilterOperation::Equals,
                FilterOperation::Contains,
            ]
            .into_iter()
            .collect(),
            column_filters,
            supports_arbitrary_sql: false,
            supports_and: true,
            supports_or: true,
            supports_not: false,
            supports_nested: false,
            max_filters: Some(10),
            full_scan_cost_multiplier: 5.0,
            supports_column_pruning: false,
            supports_limit: true,
            supports_order_by: true,
            supports_aggregates: false,
            limitations_description: Some(
                "Notion supports property filters including date ranges and text contains."
                    .to_string()
            ),
        }
    }

    fn airtable_capabilities() -> SourceCapabilities {
        SourceCapabilities {
            supported_operations: [
                FilterOperation::Equals,
                FilterOperation::NotEquals,
                FilterOperation::Contains,
            ]
            .into_iter()
            .collect(),
            column_filters: AHashMap::new(),
            supports_arbitrary_sql: false,
            supports_and: true,
            supports_or: true,
            supports_not: true,
            supports_nested: true, // Airtable formulas support nesting
            max_filters: None,
            full_scan_cost_multiplier: 4.0,
            supports_column_pruning: true,
            supports_limit: true,
            supports_order_by: true,
            supports_aggregates: false,
            limitations_description: Some(
                "Airtable supports filter formulas with most common operations."
                    .to_string()
            ),
        }
    }

    fn asana_capabilities() -> SourceCapabilities {
        let mut column_filters = AHashMap::new();

        column_filters.insert(
            "modified_at".to_string(),
            ColumnFilterCapability::new([FilterOperation::GreaterThanOrEquals])
                .with_transform(ValueTransform::DateTimeToIso8601),
        );

        SourceCapabilities {
            supported_operations: AHashSet::new(),
            column_filters,
            supports_arbitrary_sql: false,
            supports_and: false,
            supports_or: false,
            supports_not: false,
            supports_nested: false,
            max_filters: Some(1),
            full_scan_cost_multiplier: 8.0,
            supports_column_pruning: true,
            supports_limit: true,
            supports_order_by: false,
            supports_aggregates: false,
            limitations_description: Some(
                "Asana only supports modified_at for incremental sync."
                    .to_string()
            ),
        }
    }

    fn monday_capabilities() -> SourceCapabilities {
        SourceCapabilities {
            supported_operations: [
                FilterOperation::Equals,
                FilterOperation::In { max_values: Some(20) },
            ]
            .into_iter()
            .collect(),
            column_filters: AHashMap::new(),
            supports_arbitrary_sql: false,
            supports_and: true,
            supports_or: true,
            supports_not: false,
            supports_nested: false,
            max_filters: Some(10),
            full_scan_cost_multiplier: 4.0,
            supports_column_pruning: true, // GraphQL
            supports_limit: true,
            supports_order_by: false,
            supports_aggregates: false,
            limitations_description: Some(
                "Monday.com supports GraphQL filters on columns and groups."
                    .to_string()
            ),
        }
    }

    fn confluence_capabilities() -> SourceCapabilities {
        let mut column_filters = AHashMap::new();

        column_filters.insert(
            "lastModified".to_string(),
            ColumnFilterCapability::new([FilterOperation::GreaterThanOrEquals])
                .with_transform(ValueTransform::DateTimeToIso8601),
        );

        SourceCapabilities {
            supported_operations: [FilterOperation::Contains].into_iter().collect(),
            column_filters,
            supports_arbitrary_sql: false,
            supports_and: true,
            supports_or: true,
            supports_not: false,
            supports_nested: false,
            max_filters: Some(5),
            full_scan_cost_multiplier: 6.0,
            supports_column_pruning: false,
            supports_limit: true,
            supports_order_by: false,
            supports_aggregates: false,
            limitations_description: Some(
                "Confluence supports CQL (Confluence Query Language) for searching."
                    .to_string()
            ),
        }
    }

    // ========================================================================
    // File Formats
    // ========================================================================

    fn csv_capabilities() -> SourceCapabilities {
        SourceCapabilities::no_pushdown()
            .with_limitations("CSV files must be fully scanned. No filter pushdown is possible.")
    }

    fn json_capabilities() -> SourceCapabilities {
        SourceCapabilities::no_pushdown()
            .with_limitations("JSON files must be fully parsed. No filter pushdown is possible.")
    }

    fn excel_capabilities() -> SourceCapabilities {
        SourceCapabilities::no_pushdown()
            .with_limitations("Excel files must be fully read. No filter pushdown is possible.")
    }

    fn xml_capabilities() -> SourceCapabilities {
        SourceCapabilities::no_pushdown()
            .with_limitations("XML files must be fully parsed. No filter pushdown is possible.")
    }

    fn parquet_capabilities() -> SourceCapabilities {
        SourceCapabilities::parquet_capabilities()
    }

    // ========================================================================
    // Cloud Storage
    // ========================================================================

    fn cloud_storage_capabilities() -> SourceCapabilities {
        // Cloud storage capabilities depend on the file format
        // Default to Parquet-like capabilities for object storage
        SourceCapabilities::parquet_capabilities()
            .with_limitations(
                "Cloud storage filter pushdown depends on file format. \
                 Parquet files support row group filtering; CSV/JSON do not."
            )
    }

    // ========================================================================
    // Streaming
    // ========================================================================

    fn streaming_capabilities() -> SourceCapabilities {
        SourceCapabilities::no_pushdown()
            .with_limitations(
                "Streaming sources (Kafka, Kinesis) do not support filter pushdown. \
                 Filtering is applied after consuming messages."
            )
    }

    // ========================================================================
    // Blockchain
    // ========================================================================

    fn blockchain_capabilities() -> SourceCapabilities {
        let mut column_filters = AHashMap::new();

        column_filters.insert(
            "block_number".to_string(),
            ColumnFilterCapability::new([
                FilterOperation::Equals,
                FilterOperation::GreaterThanOrEquals,
                FilterOperation::LessThanOrEquals,
                FilterOperation::Between,
            ])
            .with_indexed(true),
        );

        column_filters.insert(
            "from_address".to_string(),
            ColumnFilterCapability::new([FilterOperation::Equals])
                .with_transform(ValueTransform::ToLowercase),
        );

        column_filters.insert(
            "to_address".to_string(),
            ColumnFilterCapability::new([FilterOperation::Equals])
                .with_transform(ValueTransform::ToLowercase),
        );

        SourceCapabilities {
            supported_operations: AHashSet::new(),
            column_filters,
            supports_arbitrary_sql: false,
            supports_and: true,
            supports_or: false,
            supports_not: false,
            supports_nested: false,
            max_filters: Some(5),
            full_scan_cost_multiplier: 20.0, // Blockchain queries are expensive
            supports_column_pruning: false,
            supports_limit: true,
            supports_order_by: false,
            supports_aggregates: false,
            limitations_description: Some(
                "Blockchain APIs support block range and address filters. \
                 Other filtering must be done locally.".to_string()
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgresql_full_sql() {
        let caps = SourceCapabilityMatrix::for_source_type(SourceType::PostgreSQL);
        assert!(caps.supports_arbitrary_sql);
        assert!(caps.supports_or);
        assert!(caps.supports_not);
        assert!(caps.supports_aggregates);
    }

    #[test]
    fn test_stripe_limited_filters() {
        let caps = SourceCapabilityMatrix::for_source_type(SourceType::Stripe);
        
        assert!(!caps.supports_arbitrary_sql);
        assert!(!caps.supports_or);
        
        // Check column-specific capabilities
        assert!(caps.supports_column_filter("created", &FilterOperation::GreaterThanOrEquals));
        assert!(caps.supports_column_filter("customer", &FilterOperation::Equals));
        
        // Should not support arbitrary columns
        assert!(!caps.supports_column_filter("amount", &FilterOperation::GreaterThan));
    }

    #[test]
    fn test_csv_no_pushdown() {
        let caps = SourceCapabilityMatrix::for_source_type(SourceType::Csv);
        
        assert!(!caps.has_any_pushdown());
        assert!(!caps.supports_column_filter("any_column", &FilterOperation::Equals));
        assert!(caps.full_scan_cost_multiplier > 1.0);
    }

    #[test]
    fn test_parquet_capabilities() {
        let caps = SourceCapabilityMatrix::for_source_type(SourceType::ExternalParquet);
        
        assert!(caps.supports_column_pruning);
        assert!(caps.supports_and);
        assert!(!caps.supports_or); // Parquet doesn't handle OR well
        assert!(caps.supports_operation(&FilterOperation::Between));
    }

    #[test]
    fn test_salesforce_soql() {
        let caps = SourceCapabilityMatrix::for_source_type(SourceType::Salesforce);
        
        assert!(!caps.supports_arbitrary_sql); // SOQL, not SQL
        assert!(caps.supports_and);
        assert!(caps.supports_or);
        assert!(caps.supports_aggregates);
    }

    #[test]
    fn test_jira_jql() {
        let caps = SourceCapabilityMatrix::for_source_type(SourceType::Jira);
        
        assert!(!caps.supports_arbitrary_sql); // JQL, not SQL
        assert!(caps.supports_or);
        assert!(caps.supports_nested);
        assert!(caps.supports_operation(&FilterOperation::In { max_values: None }));
    }

    #[test]
    fn test_blockchain_block_filters() {
        let caps = SourceCapabilityMatrix::for_source_type(SourceType::Ethereum);
        
        assert!(caps.supports_column_filter("block_number", &FilterOperation::Between));
        assert!(caps.supports_column_filter("from_address", &FilterOperation::Equals));
        assert!(!caps.supports_column_filter("gas_price", &FilterOperation::GreaterThan));
        
        // Check value transform for addresses
        let addr_cap = caps.get_column_capability("from_address").unwrap();
        assert_eq!(addr_cap.value_transform, Some(ValueTransform::ToLowercase));
    }

    #[test]
    fn test_all_source_types_have_capabilities() {
        // Ensure all source types return valid capabilities
        let source_types = [
            SourceType::PostgreSQL,
            SourceType::MySQL,
            SourceType::Stripe,
            SourceType::Csv,
            SourceType::ExternalParquet,
            SourceType::Snowflake,
            SourceType::HubSpot,
            SourceType::Salesforce,
            SourceType::GitHub,
            SourceType::Ethereum,
        ];
        
        for source_type in source_types {
            let caps = SourceCapabilityMatrix::for_source_type(source_type);
            // Just ensure it doesn't panic and returns something valid
            assert!(caps.full_scan_cost_multiplier >= 1.0);
        }
    }
}
