//! Materialized Views / Pre-Aggregation
//!
//! Pre-computed aggregates for common analytics patterns to provide 100x faster queries.
//!
//! # Supported Aggregates
//!
//! - **Daily Event Counts**: Pre-aggregated counts by event type and day
//! - **Funnel Step Cache**: User progression through funnel steps
//! - **Retention Cohorts**: Weekly retention matrices
//!
//! # Architecture
//!
//! Aggregates are computed incrementally on data ingestion and stored in ClickHouse.
//! When a dashboard query matches a pre-computed aggregate, we read from the aggregate
//! table instead of scanning raw events.

pub mod aggregates;
pub mod funnel_cache;
pub mod retention_cache;

pub use aggregates::{DailyEventAggregate, AggregateManager};
pub use funnel_cache::{FunnelStepCache, FunnelStep};
pub use retention_cache::{RetentionCohortCache, CohortRetention};
