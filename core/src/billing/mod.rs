//! Billing module for usage tracking, cost calculation, and payments.
//!
//! This module provides:
//! - Usage tracking via ClickHouse materialized views
//! - Cost calculation based on configurable pricing
//! - Budget management and alerting
//! - Payment method management (Stripe, etc.)
//! - Subscription management
//!
//! # Architecture
//!
//! The payment system uses a trait-based abstraction (`PaymentProvider`) that allows:
//! - Multiple payment providers (Stripe, etc.)
//! - Easy testing with `MockPaymentProvider`
//! - Consistent error handling across providers
//!
//! # Example
//!
//! ```rust,ignore
//! use billing::{PaymentProvider, StripePaymentProvider, MockPaymentProvider};
//!
//! // For production
//! let provider = StripePaymentProvider::new(api_key, db_pool, redis_pool, webhook_secret, metered_price_id);
//!
//! // For testing
//! let provider = MockPaymentProvider::new();
//! provider.set_card_declined(true); // Simulate failures
//! ```

pub mod credit_balance_sync;
pub mod credits;
pub mod meter_service;
mod payments;
mod provider;
mod service;
mod stripe;
mod types;
mod utils;

#[cfg(any(test, feature = "test-utils"))]
mod mock_provider;

#[allow(unused_imports)]
pub use payments::*;
pub use provider::*;
pub use service::*;
pub use meter_service::{MeterService, MeterName};
pub use stripe::StripePaymentProvider;
pub use types::*;
#[allow(unused_imports)]
pub use utils::{
    build_uuid_in_clause, format_uuid_for_clickhouse, retry_with_backoff, RetryConfig,
};

#[cfg(any(test, feature = "test-utils"))]
pub use mock_provider::*;
