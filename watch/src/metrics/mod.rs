//! Metrics module for time series data storage and querying.
//!
//! This module provides:
//! - Fingerprint calculation for time series identification
//! - Metrics ingestion and storage
//! - Query building for metrics data

mod cache;
mod fingerprint;
pub mod insert_types;
mod query;
pub(crate) mod tables;
mod types;

pub use cache::*;
pub use fingerprint::compute_fingerprint;
pub use insert_types::*;
pub use query::*;
pub use types::*;
