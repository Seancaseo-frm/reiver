//! SQL Server Connector
//!
//! Cold tier connector for Microsoft SQL Server with optional ClickHouse index acceleration.
//!
//! # Features
//!
//! - Direct query access to SQL Server tables without data duplication
//! - Schema inference from INFORMATION_SCHEMA
//! - Cursor-based streaming for memory-efficient large table processing
//! - SQL predicate pushdown
//! - Optional ClickHouse index layer for accelerated queries
//! - CDC-based real-time index synchronization
//!
//! # Architecture
//!
//! The connector operates in two modes:
//!
//! ## Cold Tier Mode (Default)
//!
//! Data is fetched directly from SQL Server at query time, converted to Arrow
//! RecordBatches, and materialized for SQL execution. This mode:
//! - Has zero storage overhead
//! - Always returns fresh data
//! - May be slower for complex queries or large datasets
//!
//! ## Index-Accelerated Mode
//!
//! When enabled, a ClickHouse index table mirrors scalar fields from the
//! SQL Server table. Queries first check the index to identify matching
//! row IDs, then fetch only those rows from SQL Server. This mode:
//! - Adds minimal storage overhead (scalar fields only)
//! - Significantly faster for filtered queries
//! - Requires background CDC tailer for sync
//!
//! # Usage
//!
//! ```ignore
//! use reiver::warehouse::connectors::databases::sqlserver::{
//!     SqlServerConfig, SqlServerConnector,
//! };
//!
//! // Basic cold tier mode
//! let config = SqlServerConfig::new("localhost", "mydb", "sa", "password");
//! let connector = SqlServerConnector::new(config);
//!
//! let tables = connector.list_tables().await?;
//! let data = connector.fetch_table("users", None, None).await?;
//!
//! // With ClickHouse index acceleration
//! let config = SqlServerConfig::new("localhost", "mydb", "sa", "password")
//!     .with_index_database("reiver_indexes")
//!     .with_cdc(true);
//! let connector = SqlServerConnector::new(config);
//! ```

pub mod cdc;
pub mod config;
pub mod connector;
pub mod filter;
pub mod index;
pub mod schema;
pub mod utils;

// Re-export main types
pub use cdc::{CdcTailer, CdcTailerState, ClickHouseLsnStorage, LsnStorage};
pub use config::SqlServerConfig;
pub use connector::SqlServerConnector;
pub use filter::{PredicateValue, SqlOperator, SqlPredicate, SqlServerFilterBuilder};
pub use index::{IndexQueryExecutor, IndexValue, SqlServerIndexManager, SqlServerWalIndexManager};
pub use schema::{
    build_arrow_schema, build_table_schema, sqlserver_type_to_arrow, sqlserver_type_to_column_type,
    ColumnInfo,
};
