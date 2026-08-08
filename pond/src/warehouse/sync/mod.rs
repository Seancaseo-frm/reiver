//! Sync engine for the data warehouse.
//!
//! Handles scheduled and manual syncs of external data sources.
//!
//! ARCHITECTURE:
//! - `sync_executor`: Core sync logic that routes data to R2 or ClickHouse
//! - `sync_job_consumer`: Kafka consumer for processing sync jobs
//! - `job_worker`: Database polling worker (legacy, being phased out)
//! - `scheduler`: Schedules recurring sync jobs based on sync_interval
//! - `worker`: Actual ETL execution to ClickHouse or R2

pub mod blockchain_sync;
pub mod compaction;
pub mod error_handler;
pub mod merge;
pub mod job_worker;
pub mod lifecycle_worker;
pub mod parquet_rewriter;
pub mod scheduler;
pub mod sync_executor;
pub mod sync_job_consumer;
pub mod worker;

pub use error_handler::*;
pub use job_worker::*;
pub use lifecycle_worker::*;
pub use parquet_rewriter::*;
pub use scheduler::*;
pub use sync_executor::*;
pub use sync_job_consumer::*;
pub use worker::*;
