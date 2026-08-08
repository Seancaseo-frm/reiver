//! Data Warehouse Module
//!
//! This module implements a data warehouse that syncs external data sources
//! (Stripe, PostgreSQL, HubSpot, etc.) to Parquet files stored in Cloudflare R2,
//! then queries them via ClickHouse's s3() table function.
//!
//! # Observability
//!
//! The `metrics` module provides comprehensive observability for:
//! - Query cache hit/miss rates
//! - Skip index effectiveness and prune rates
//! - Query queue wait times and concurrency
//! - Sync operation performance

pub mod ai_config;
pub mod catalog;
pub mod ch_client;
pub mod ch_type_parser;
pub mod connectors;
pub mod derived;
pub mod freshness;
pub mod indexes;
pub mod materialized;
pub mod metrics;
pub mod nl_query;
pub mod parquet;
pub mod parquet_metadata;
pub mod pipeline;
pub mod parquet_stats;
pub mod pii_scanner;
pub mod query;
pub mod search;
pub mod sessions;
pub mod sources;
pub mod statistics;
pub mod storage;
pub mod sync;
pub mod table_formats;
pub mod types;
pub mod udf;
pub mod utils;

// Re-exports for convenience
pub use catalog::{CatalogService, CatalogRepository, CatalogEntry, CatalogError, CatalogResult};
pub use freshness::{FreshnessService, StalenessLevel, TableFreshness};
pub use metrics::{WarehouseMetrics, MetricsSnapshot};
pub use sources::{DataSourceRegistry, RegisteredSource, SourceBackend, SourceConfig};
pub use statistics::{
    CollectionMethod, ColumnStatistics, StatisticsCollector, StatisticsRepository, TableStatistics,
};
pub use storage::r2::{R2Config, R2Storage};
pub use types::*;
