//! WAL-based Two-Phase Indexing for CDC/Oplog Databases
//!
//! This module provides a shared indexing infrastructure for databases that support
//! Write-Ahead Log (WAL) based change capture (CDC for SQL Server, Oplog for MongoDB).
//!
//! # Architecture
//!
//! Unlike traditional approaches that replicate data values, this module builds
//! **true indexes** that reference primary keys in the source database:
//!
//! 1. **Block-level skip indexes**: Coarse-grained filters (Xor, MinMax) to eliminate
//!    entire blocks of rows that definitely don't match a predicate.
//!
//! 2. **Inverted indexes**: For low-cardinality columns, store value → PK bitmap mappings
//!    to identify exactly which rows contain a given value.
//!
//! # Two-Phase Query Flow
//!
//! ```text
//! Query: status='pending' AND amount > 1000
//!           │
//!           ▼
//! ┌─────────────────────────────────────┐
//! │  Phase 1: Block Elimination         │
//! │  - Check MinMax for amount > 1000   │
//! │  - Check Xor filter for 'pending'   │
//! │  - Eliminate non-matching blocks    │
//! └─────────────────────────────────────┘
//!           │
//!           ▼
//! ┌─────────────────────────────────────┐
//! │  Phase 2: PK Resolution             │
//! │  - Load inverted index for status   │
//! │  - Get PK bitmap intersection       │
//! └─────────────────────────────────────┘
//!           │
//!           ▼
//! ┌─────────────────────────────────────┐
//! │  Fetch from Source                  │
//! │  SELECT * FROM source WHERE pk IN   │
//! │  (pk1, pk2, pk3, ...)               │
//! └─────────────────────────────────────┘
//! ```
//!
//! # Storage
//!
//! Indexes are stored in ClickHouse in three tables:
//! - `wal_blocks`: Block definitions (PK ranges)
//! - `wal_block_indexes`: Block-level skip indexes (serialized filters)
//! - `wal_inverted_index`: Value → PK bitmap mappings
//!
//! # Usage
//!
//! ```ignore
//! use reiver::warehouse::connectors::wal_index::{
//!     WalIndexManager, BlockManager, IndexStrategy,
//! };
//!
//! // Create index manager
//! let manager = WalIndexManager::new(storage, "my_source", "my_table");
//!
//! // Process WAL events
//! for event in wal_events {
//!     manager.process_event(event).await?;
//! }
//!
//! // Query with two-phase execution
//! let pks = manager.query(predicates).await?;
//! ```

pub mod block;
pub mod inverted_index;
pub mod query;
pub mod skip_index;
pub mod storage;
pub mod types;

pub use block::{Block, BlockManager, BlockManagerConfig, BlockId};
pub use inverted_index::{InvertedIndex, InvertedIndexManager};
pub use query::{Predicate, PredicateOp, TwoPhaseQueryExecutor};
pub use skip_index::{BlockSkipIndex, SkipIndexBuilder, SkipIndexType};
pub use storage::{WalIndexStorage, ClickHouseWalStorage};
pub use types::{PrimaryKey, ColumnValue, WalEvent, WalEventType};
