//! Reiver Pond -- Data Warehouse
//!
//! This crate provides the data warehouse product:
//! - SQL query engine over columnar storage
//! - Connector registry for federated queries
//! - Catalog service for schema discovery
//! - Skip index optimization for TB-scale queries

#![recursion_limit = "256"]

// Re-export core modules so `use crate::X` works for core types.
// Auth-related modules (auth, authorization, rate_limit, saml, sso, mfa)
// are NOT re-exported because authentication is handled by the website gateway.
pub use reiver_core::{
    clickhouse_db, config, crypto,
    db, error, kafka, models, pii, pool, query_cache,
    storage, types, utils, intern,
    audit, billing,
};

// Re-export app_state module
pub mod app_state;

// Pond domain logic
pub mod warehouse;

// Pond API handlers
pub mod api;

// Postgres wire protocol adapter
pub mod pgwire;

// OpenTelemetry initialization (dogfooding)
pub mod telemetry;

/// Span helpers, trace correlation headers, and runbook notes for query observability.
pub mod observability;

// Continuous CPU profiling is now provided by reiver-sdk's "profiling" feature.
// See: reiver_sdk::profiling
