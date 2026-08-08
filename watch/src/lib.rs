//! Reiver Watch -- APM (Application Performance Monitoring)
//!
//! This crate provides the APM product:
//! - Error tracking and exception management
//! - Distributed tracing (OTLP)
//! - Metrics collection and querying
//! - Alert management and notifications
//! - GitHub integration
//! - Database monitoring

// Re-export core modules so `use crate::X` works for core types
// Note: auth, authorization, billing, rate_limit, saml, sso, mfa are handled by the website proxy
pub use reiver_core::{
    audit, clickhouse_db, config, crypto, db, error, intern, kafka, llm, models, pii, pool,
    query_cache, storage, types, utils,
};

// Shared utilities
pub mod ch_stream;
pub mod simd_json_utils;
pub mod telemetry;

// Watch state
pub mod app_state;
pub mod fingerprint;
pub mod github;
pub mod maintenance;
pub mod metrics;
pub mod promql_provider;
pub mod root_cause;

// Watch API handlers
pub mod api;

// Platform event system
pub mod event_worker;

// Watch workers (APM-specific)
pub mod aggregation_worker;
pub mod alert_worker;
//pub mod aws_worker;
//pub mod azure_worker;
//pub mod gcp_worker;
//pub mod oci_worker;
//pub mod snowflake_worker;
pub mod kafka_consumer;
pub mod kafka_log_consumer;
pub mod metrics_worker;
pub mod spans_worker;
pub mod worker;

pub mod alerts;
#[cfg(test)]
mod redis_test;
#[cfg(test)]
pub mod test_utils;
