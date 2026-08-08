//! MongoDB Connector
//!
//! Cold tier connector for MongoDB with optional ClickHouse index acceleration.
//!
//! # Features
//!
//! - Direct query access to MongoDB collections without data duplication
//! - Schema inference from document sampling with nested field flattening
//! - Cursor-based streaming for memory-efficient large collection processing
//! - SQL to MongoDB filter translation for predicate pushdown
//! - Optional ClickHouse index layer for accelerated queries
//! - Oplog-based real-time index synchronization
//!
//! # Architecture
//!
//! The connector operates in two modes:
//!
//! ## Cold Tier Mode (Default)
//!
//! Data is fetched directly from MongoDB at query time, converted to Arrow
//! RecordBatches, and materialized for SQL execution. This mode:
//! - Has zero storage overhead
//! - Always returns fresh data
//! - May be slower for complex queries or large datasets
//!
//! ## Index-Accelerated Mode
//!
//! When enabled, a ClickHouse index table mirrors scalar fields from the
//! MongoDB collection. Queries first check the index to identify matching
//! document IDs, then fetch only those documents from MongoDB. This mode:
//! - Adds minimal storage overhead (scalar fields only)
//! - Significantly faster for filtered queries
//! - Requires background oplog tailer for sync
//!
//! # Usage
//!
//! ```ignore
//! use reiver::warehouse::connectors::databases::mongodb::{
//!     MongoDBConfig, MongoDBConnector,
//! };
//!
//! // Basic cold tier mode
//! let config = MongoDBConfig::new("mongodb://localhost:27017", "mydb");
//! let connector = MongoDBConnector::new(config);
//!
//! let tables = connector.list_tables().await?;
//! let data = connector.fetch_table("users", None, None).await?;
//!
//! // With ClickHouse index acceleration
//! let config = MongoDBConfig::new("mongodb://localhost:27017", "mydb")
//!     .with_index("reiver_indexes");
//! let connector = MongoDBConnector::new(config)
//!     .with_index_manager(index_manager);
//! ```

pub mod config;
pub mod connector;
pub mod filter;
pub mod index;
pub mod oplog;
pub mod schema;
pub mod utils;

// Re-export main types
pub use config::{MongoDBConfig, ReadPreference};
pub use connector::MongoDBConnector;
pub use filter::{MongoFilterTranslator, ParsedWhereClause, PredicateValue, SqlOperator, SqlPredicate};
pub use index::{IndexQueryExecutor, MongoDBWalIndexManager};
pub use oplog::{ClickHouseTokenStorage, OplogTailer, OplogTailerState, ResumeTokenStorage};
pub use schema::{
    extract_indexable_fields, infer_schema, BsonToArrowConverter, IndexableValue, InferredSchema,
    FIELD_SEPARATOR,
};
