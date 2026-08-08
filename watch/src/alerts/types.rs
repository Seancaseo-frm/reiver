//! Alert system types — re-exported from `reiver_core::alerts`.
//!
//! The canonical definitions live in `core/src/alerts/types.rs`.
//! This module re-exports them so existing `use crate::alerts::*`
//! imports throughout the watch crate continue to work.

pub use reiver_core::alerts::{
    compute_alert_fingerprint, validate_aggregation_function, AlertQueryConfig, AlertRule,
    AlertState, AlertValidationError, RuleType,
};
