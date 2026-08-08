//! Indexes for the data warehouse.
//!
//! This module provides various index types optimized for different data characteristics:
//!
//! ## Index Types
//!
//! | Structure      | Incremental Add | Prefix Queries | Probabilistic | Best For                    |
//! |----------------|-----------------|----------------|---------------|---------------------------|
//! | FST            | No (rebuild)    | Yes            | No            | Low-cardinality strings    |
//! | Xor Filter     | No (rebuild)    | No             | Yes (~0.4% FP)| High-cardinality summaries |
//! | HyperLogLog    | Yes             | No             | Yes           | Cardinality estimation     |

// Blob serialization and local disk cache for hybrid R2 storage
pub mod blob;
pub mod disk_cache;
pub mod fst_backing;

// FST-based indexes
pub mod column_index;
pub mod fulltext_index;
pub mod foreign_key;
pub mod join_optimizer;
pub mod maintenance;
pub mod persistence;
pub mod query_cache;
pub mod query_history;
pub mod schema_index;
pub mod sidecar_stats_cache;
pub mod skip_index;
pub mod skip_index_cache;
pub mod smart_builder;
pub mod tag_index;

// Supporting modules
pub mod cardinality;
pub mod external_config;
pub mod partition_manager;
pub mod strategy;
pub mod xor_index;

// Re-exports for FST-based indexes
pub use fst_backing::FstBacking;
pub use column_index::*;
pub use foreign_key::*;
pub use join_optimizer::*;
pub use maintenance::*;
pub use persistence::*;
pub use query_cache::*;
pub use query_history::*;
pub use schema_index::*;
pub use sidecar_stats_cache::*;
pub use skip_index::*;
pub use skip_index_cache::*;
pub use smart_builder::*;
pub use tag_index::*;

// Re-exports for supporting modules
pub use cardinality::{ColumnCardinalityEstimator, TableCardinalityEstimator};
pub use strategy::{ColumnStats, IndexStrategy};
pub use external_config::{
    detect_hive_partitioning, detect_partition_strategy, discover_partitions,
    group_by_partition_date, hash_bucket_key, hash_bucket_partitions, ParsedPartition,
    PartitionMutabilityResolver, PartitionPatternParser, PartitionStrategy,
};
pub use partition_manager::{
    Partition, PartitionError, PartitionManager, PartitionResult, PartitionState,
};
pub use xor_index::{DataXorIndex, FileXorIndex, XorColumnFilter};

// Re-export increment_last_byte from shared utils for backwards compatibility
pub use crate::warehouse::utils::increment_last_byte;
