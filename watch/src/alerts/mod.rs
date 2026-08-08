//! Alert system for metric-based alerting.
//!
//! This module provides:
//! - Alert rule configuration and evaluation
//! - Alert state machine (OK -> ALERT)
//! - Notification dispatch to configured channels

mod notifier;
mod types;

pub use notifier::*;
pub use types::*;
