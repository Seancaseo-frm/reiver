//! Data Source Management
//!
//! This module provides a unified abstraction for managing multiple data sources
//! within a project. Each source can be:
//!
//! - **ClickHouse Native**: Data synced into ClickHouse MergeTree tables
//! - **Object Storage**: Parquet files in S3/R2/GCS queried via ClickHouse s3()
//! - **External Database**: Direct connections via ClickHouse table functions
//!
//! Sources are identified by user-defined names that can be used in SQL queries:
//!
//! ```sql
//! SELECT * FROM stripe.customers c
//! JOIN s3_events.orders o ON c.id = o.customer_id
//! ```

pub mod registry;
pub mod registry_service;
pub mod types;

pub use registry::{DataSourceRegistry, RegistryError, RegistryResult, validate_source_name};
pub use registry_service::{ConnectorRegistryService, InitializeResult, RegistryServiceError, RegistryServiceResult};
pub use types::{
    ConsistencyLevel, ExternalDbType, RegisteredSource, SourceBackend, SourceConfig,
    SourceColumnInfo, SourceTableInfo, StorageTier, SyncScope,
};
