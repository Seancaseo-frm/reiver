//! Unified Catalog and Metadata
//!
//! Provides cross-source schema discovery, lineage tracking, and relationship management.
//!
//! # Features
//!
//! - **Schema Discovery**: List all tables and columns across sources with full type information
//! - **Statistics Integration**: Connect existing stats infrastructure to the catalog
//! - **Lineage Tracking**: Track data flow between columns across transformations
//! - **Relationships**: Define and discover foreign key relationships across sources
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                      CatalogService                             │
//! │  ┌─────────────┐  ┌──────────────┐  ┌───────────────────────┐  │
//! │  │ Schema      │  │ Statistics   │  │ Relationships         │  │
//! │  │ Discovery   │  │ Integration  │  │ & Lineage             │  │
//! │  └─────────────┘  └──────────────┘  └───────────────────────┘  │
//! └─────────────────────────────────────────────────────────────────┘
//!                              │
//!        ┌─────────────────────┼─────────────────────┐
//!        ▼                     ▼                     ▼
//!   ┌─────────┐          ┌─────────┐          ┌─────────┐
//!   │PostgreSQL│          │ Stripe  │          │ Parquet │
//!   │Discovery│          │Discovery│          │Discovery│
//!   └─────────┘          └─────────┘          └─────────┘
//! ```

pub mod discovery;
pub mod integration;
pub mod repository;
pub mod service;
pub mod types;

pub use repository::{CatalogRepository, CatalogError, CatalogResult};
pub use service::CatalogService;
pub use types::{
    CatalogEntry, ColumnLineage, ColumnRef, CrossSourceRelationship,
    FreshnessInfo, LineageSource, RelationshipType, SearchResult,
    SourceSummary, SyncStatus, TableRef, TableSummary, TransformationType,
    Cardinality, LineageDiscoveryMethod,
};
