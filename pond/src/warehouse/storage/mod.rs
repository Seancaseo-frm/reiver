//! Storage backends for the data warehouse.
//!
//! Supports two storage types:
//! - `r2`: Object storage (R2/S3) with Parquet files
//! - `clickhouse`: Native ClickHouse MergeTree tables

pub mod clickhouse;
pub mod r2;

pub use clickhouse::{ClickHouseStorage, ClickHouseStorageConfig, ClickHouseStorageError, TableSettings};
pub use r2::{
    R2Config, R2Storage, R2ValidationError,
    validate_bucket_name, validate_account_id, validate_access_key, validate_secret_key,
};
