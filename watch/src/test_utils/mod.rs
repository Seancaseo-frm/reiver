//! Test utilities module
//!
//! Provides testing infrastructure including:
//! - Fixtures for generating test data
//! - Mock implementations for external services
//! - Custom assertions for common patterns
//! - Test database and Redis helpers
//!
//! This module is only compiled in test mode.

#![cfg(test)]

pub mod assertions;
pub mod fixtures;
pub mod mocks;

pub use assertions::*;
pub use fixtures::*;
pub use mocks::*;
