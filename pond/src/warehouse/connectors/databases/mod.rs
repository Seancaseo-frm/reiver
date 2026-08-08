//! Database Connectors
//!
//! Connectors for various database systems.
//!
//! # Supported Databases
//!
//! - MySQL/MariaDB
//! - MongoDB (cold tier with optional ClickHouse index)
//! - SQL Server (cold tier with optional ClickHouse index)
//! - SQLite (cold tier with predicate pushdown)
//! - BigQuery (cold tier with predicate pushdown)
//! - Redshift (cold tier and sync to ClickHouse)
//! - Snowflake (cold tier and sync to ClickHouse)

pub mod bigquery;
pub mod clickhouse;
pub mod mongodb;
pub mod mysql;
pub mod redshift;
pub mod snowflake;
pub mod sqlite;
pub mod sqlserver;

pub use bigquery::{BigQueryConfig, BigQueryConnector};
pub use clickhouse::{ClickHouseConfig, ClickHouseConnector};
pub use mongodb::{MongoDBConfig, MongoDBConnector, MongoDBWalIndexManager, ReadPreference};
pub use mysql::{MySqlConfig, MySqlConnector};
pub use redshift::{RedshiftConfig, RedshiftConnector, SslMode as RedshiftSslMode};
pub use snowflake::{SnowflakeConfig, SnowflakeConnector};
pub use sqlite::{SQLiteConfig, SQLiteConnector};
pub use sqlserver::{SqlServerConfig, SqlServerConnector, SqlServerIndexManager, SqlServerWalIndexManager};
