//! Schema Discovery
//!
//! Implementations for discovering schemas from different data sources.

pub mod clickhouse;
pub mod inference;
pub mod parquet;
pub mod postgres;
pub mod stripe;

use async_trait::async_trait;
use thiserror::Error;

use crate::warehouse::sources::types::RegisteredSource;
use crate::warehouse::types::TypedSchema;

pub use clickhouse::ClickHouseSchemaDiscovery;
pub use inference::RelationshipInference;
pub use parquet::ParquetSchemaDiscovery;
pub use postgres::PostgresSchemaDiscovery;
pub use stripe::StripeSchemaDiscovery;

// ============================================================================
// Errors
// ============================================================================

/// Errors that can occur during schema discovery.
#[derive(Debug, Error)]
pub enum DiscoveryError {
    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Source not configured: {0}")]
    NotConfigured(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type DiscoveryResult<T> = Result<T, DiscoveryError>;

// ============================================================================
// Schema Discovery Trait
// ============================================================================

/// Trait for schema discovery from a data source.
#[async_trait]
pub trait SchemaDiscovery: Send + Sync {
    /// Discover all schemas from the source.
    async fn discover_schemas(&self, source: &RegisteredSource) -> DiscoveryResult<Vec<TypedSchema>>;

    /// Discover schema for a specific table.
    ///
    /// Returns `Ok(None)` if the table doesn't exist in the source.
    /// Returns `Err` for actual errors (connection issues, permission errors, etc.).
    async fn discover_table_schema(
        &self,
        source: &RegisteredSource,
        table_name: &str,
    ) -> DiscoveryResult<Option<TypedSchema>>;
}
