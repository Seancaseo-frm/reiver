//! Statistics Collection and Management
//!
//! This module provides statistics collection for cost-based query planning.
//! Statistics are collected from various sources:
//!
//! - **Sync**: During data sync from Stripe, HubSpot, etc.
//! - **Catalog**: From database catalogs (pg_stats for PostgreSQL)
//! - **Metadata**: From Parquet file metadata
//! - **Sampling**: By sampling rows from external sources

pub mod collector;
pub mod persistence;

pub use collector::StatisticsCollector;
pub use persistence::{
    CollectionMethod, ColumnStatistics, CommonValue, StatisticsError, StatisticsRepository,
    StatisticsResult, TableStatistics,
};
