//! Snowflake integrations for Reiver
//!
//! This crate provides server-side integration for Snowflake data warehouse.
//! Currently supports:
//! - Warehouse usage metrics (from INFORMATION_SCHEMA and ACCOUNT_USAGE)
//! - Query execution metrics
//! - Credit consumption metrics
//! - Storage usage metrics

pub mod collector;
pub mod config;

pub use collector::{SnowflakeCollector, SnowflakeMetrics, SnowflakeAccountId, snowflake_metrics_to_reiver_format, ReiverMetric as SnowflakeReiverMetric};
pub use config::SnowflakeConfig;
