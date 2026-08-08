//! Block Management for WAL Indexing
//!
//! Divides source tables into virtual "blocks" based on primary key ranges.
//! Each block contains ~10,000 rows and has its own skip indexes.
//!
//! # PK Ordering
//!
//! Blocks use proper PrimaryKey ordering (numeric for integers, lexicographic for strings)
//! to avoid issues where "10" < "2" lexicographically.
//!
//! # Block Lookup
//!
//! Uses a BTreeMap indexed by PK start for O(log n) block lookups.
//! Also uses an LRU cache for direct PK-to-block lookups to bound memory usage.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroUsize;
use std::sync::Arc;

use lru::LruCache;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};

use super::types::PrimaryKey;
use crate::warehouse::connectors::ConnectorResult;

/// Unique identifier for a block.
pub type BlockId = u32;

/// Default target rows per block.
pub const DEFAULT_BLOCK_SIZE: usize = 10_000;

/// Default maximum size of the PK-to-block LRU cache.
pub const DEFAULT_PK_CACHE_SIZE: usize = 100_000;

/// When a block exceeds this multiplier of target size, consider splitting.
pub const BLOCK_SPLIT_THRESHOLD: f64 = 2.0;

/// A block represents a contiguous range of primary keys.
///
/// Stores PK bounds using the PrimaryKey type to ensure proper ordering
/// (numeric ordering for Int64 PKs, lexicographic for String PKs).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Block {
    /// Unique block identifier
    pub id: BlockId,
    /// First primary key in this block (inclusive) - string representation for storage
    pub pk_start: String,
    /// Last primary key in this block (inclusive) - string representation for storage
    pub pk_end: String,
    /// Number of rows in this block
    pub row_count: u64,
    /// Whether this block is closed (no more inserts)
    pub is_closed: bool,
    /// Creation timestamp (millis since epoch)
    pub created_at: i64,
    /// Whether the PKs in this block are numeric (for proper ordering)
    #[serde(default)]
    pub is_numeric_pk: bool,
}

impl Block {
    /// Create a new block.
    pub fn new(id: BlockId, pk_start: impl Into<String>) -> Self {
        let pk_start = pk_start.into();
        // Check if the PK looks numeric
        let is_numeric_pk = pk_start.parse::<i64>().is_ok();
        Self {
            id,
            pk_start: pk_start.clone(),
            pk_end: pk_start,
            row_count: 0,
            is_closed: false,
            created_at: chrono::Utc::now().timestamp_millis(),
            is_numeric_pk,
        }
    }

    /// Create a new block with a PrimaryKey.
    pub fn new_with_pk(id: BlockId, pk: &PrimaryKey) -> Self {
        let pk_str = pk.to_string_repr();
        let is_numeric_pk = matches!(pk, PrimaryKey::Int64(_));
        Self {
            id,
            pk_start: pk_str.clone(),
            pk_end: pk_str,
            row_count: 0,
            is_closed: false,
            created_at: chrono::Utc::now().timestamp_millis(),
            is_numeric_pk,
        }
    }

    /// Compare two PKs using proper ordering based on type.
    fn compare_pks(&self, pk1: &str, pk2: &str) -> std::cmp::Ordering {
        if self.is_numeric_pk {
            // Parse as i64 for numeric comparison
            let v1 = pk1.parse::<i64>().unwrap_or(i64::MIN);
            let v2 = pk2.parse::<i64>().unwrap_or(i64::MIN);
            v1.cmp(&v2)
        } else {
            // Use lexicographic comparison for strings
            pk1.cmp(pk2)
        }
    }

    /// Check if a primary key falls within this block's range.
    pub fn contains_pk(&self, pk: &str) -> bool {
        use std::cmp::Ordering;
        let vs_start = self.compare_pks(pk, &self.pk_start);
        let vs_end = self.compare_pks(pk, &self.pk_end);
        
        matches!(vs_start, Ordering::Equal | Ordering::Greater) 
            && matches!(vs_end, Ordering::Equal | Ordering::Less)
    }

    /// Check if a PrimaryKey falls within this block's range.
    pub fn contains_primary_key(&self, pk: &PrimaryKey) -> bool {
        self.contains_pk(&pk.to_string_repr())
    }

    /// Update the block when a new row is added.
    pub fn add_row(&mut self, pk: &str) {
        self.row_count += 1;
        if self.compare_pks(pk, &self.pk_start) == std::cmp::Ordering::Less {
            self.pk_start = pk.to_string();
        }
        if self.compare_pks(pk, &self.pk_end) == std::cmp::Ordering::Greater {
            self.pk_end = pk.to_string();
        }
    }

    /// Update the block when a new row is added with PrimaryKey.
    pub fn add_row_pk(&mut self, pk: &PrimaryKey) {
        self.add_row(&pk.to_string_repr());
    }

    /// Remove a row from the block.
    pub fn remove_row(&mut self) {
        if self.row_count > 0 {
            self.row_count -= 1;
        }
    }

    /// Check if this block should be split.
    pub fn should_split(&self, target_size: usize) -> bool {
        !self.is_closed && self.row_count > (target_size as f64 * BLOCK_SPLIT_THRESHOLD) as u64
    }

    /// Close this block (no more modifications).
    pub fn close(&mut self) {
        self.is_closed = true;
    }

    /// Get the start PK as a PrimaryKey.
    pub fn pk_start_key(&self) -> PrimaryKey {
        PrimaryKey::parse(&self.pk_start, self.is_numeric_pk)
    }

    /// Get the end PK as a PrimaryKey.
    pub fn pk_end_key(&self) -> PrimaryKey {
        PrimaryKey::parse(&self.pk_end, self.is_numeric_pk)
    }
}

/// Configuration for block management.
#[derive(Debug, Clone)]
pub struct BlockManagerConfig {
    /// Target number of rows per block
    pub target_block_size: usize,
    /// Whether to auto-close blocks when they reach target size
    pub auto_close: bool,
    /// Maximum size of the PK-to-block LRU cache (bounds memory usage)
    pub pk_cache_size: usize,
}

impl Default for BlockManagerConfig {
    fn default() -> Self {
        Self {
            target_block_size: DEFAULT_BLOCK_SIZE,
            auto_close: true,
            pk_cache_size: DEFAULT_PK_CACHE_SIZE,
        }
    }
}

/// Manages blocks for a single table.
///
/// Tracks block boundaries and assigns incoming WAL events to blocks.
/// Uses:
/// - A BTreeMap indexed by PK start for O(log n) block range lookups
/// - An LRU cache for direct PK-to-block lookups to bound memory usage
pub struct BlockManager {
    /// Source identifier
    source_id: String,
    /// Table name
    table_name: String,
    /// Configuration
    config: BlockManagerConfig,
    /// All blocks, indexed by block ID
    blocks: Arc<RwLock<HashMap<BlockId, Block>>>,
    /// Next block ID to assign
    next_block_id: Arc<RwLock<BlockId>>,
    /// Current active block for new inserts
    active_block_id: Arc<RwLock<Option<BlockId>>>,
    /// PK to block mapping for fast lookup (LRU cache to bound memory)
    pk_to_block: Arc<Mutex<LruCache<String, BlockId>>>,
    /// Block range index: maps pk_start -> block_id for O(log n) range lookups
    block_range_index: Arc<RwLock<BTreeMap<PrimaryKey, BlockId>>>,
}

impl BlockManager {
    /// Create a new block manager.
    pub fn new(
        source_id: impl Into<String>,
        table_name: impl Into<String>,
        config: BlockManagerConfig,
    ) -> Self {
        let cache_size = NonZeroUsize::new(config.pk_cache_size)
            .unwrap_or(NonZeroUsize::new(DEFAULT_PK_CACHE_SIZE).unwrap());
        
        Self {
            source_id: source_id.into(),
            table_name: table_name.into(),
            config,
            blocks: Arc::new(RwLock::new(HashMap::new())),
            next_block_id: Arc::new(RwLock::new(1)),
            active_block_id: Arc::new(RwLock::new(None)),
            pk_to_block: Arc::new(Mutex::new(LruCache::new(cache_size))),
            block_range_index: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// Get the source identifier.
    pub fn source_id(&self) -> &str {
        &self.source_id
    }

    /// Get the table name.
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Load existing blocks from storage.
    pub fn load_blocks(&self, blocks: Vec<Block>) {
        let mut blocks_map = self.blocks.write();
        let mut next_id = self.next_block_id.write();
        let mut active_id = self.active_block_id.write();
        let mut range_index = self.block_range_index.write();

        for block in blocks {
            let id = block.id;
            if id >= *next_id {
                *next_id = id + 1;
            }
            if !block.is_closed && active_id.is_none() {
                *active_id = Some(id);
            }
            // Add to range index
            range_index.insert(block.pk_start_key(), id);
            blocks_map.insert(id, block);
        }
    }

    /// Get all blocks.
    pub fn get_all_blocks(&self) -> Vec<Block> {
        self.blocks.read().values().cloned().collect()
    }

    /// Get a specific block by ID.
    pub fn get_block(&self, id: BlockId) -> Option<Block> {
        self.blocks.read().get(&id).cloned()
    }

    /// Find the block containing a primary key.
    ///
    /// Uses O(log n) lookup via BTreeMap range index, with LRU cache for hot PKs.
    pub fn find_block_for_pk(&self, pk: &PrimaryKey) -> Option<BlockId> {
        let pk_str = pk.to_string_repr();
        
        // Check LRU cache first (needs mutable access for recency update)
        {
            let mut cache = self.pk_to_block.lock();
            if let Some(&block_id) = cache.get(&pk_str) {
                return Some(block_id);
            }
        }

        // Use BTreeMap range lookup for O(log n) performance
        // Find the largest pk_start <= pk, then check if pk <= pk_end
        let found_block_id = {
            let range_index = self.block_range_index.read();
            let blocks = self.blocks.read();
            
            // Get all blocks with pk_start <= pk, take the last one (largest start)
            range_index
                .range(..=pk.clone())
                .next_back()
                .and_then(|(_, &block_id)| {
                    // Verify the pk is within this block's range
                    blocks.get(&block_id).and_then(|block| {
                        if block.contains_pk(&pk_str) {
                            Some(block_id)
                        } else {
                            None
                        }
                    })
                })
        };

        // Update cache if found (outside of locks)
        if let Some(block_id) = found_block_id {
            self.pk_to_block.lock().put(pk_str, block_id);
            return Some(block_id);
        }

        None
    }

    /// Assign a primary key to a block (for inserts).
    ///
    /// Returns the block ID and whether a new block was created.
    pub fn assign_block(&self, pk: &PrimaryKey) -> ConnectorResult<(BlockId, bool)> {
        let pk_str = pk.to_string_repr();

        // Check if PK already has a block
        if let Some(block_id) = self.find_block_for_pk(pk) {
            return Ok((block_id, false));
        }

        // Get or create active block
        let mut active_id = self.active_block_id.write();
        let mut blocks = self.blocks.write();
        let mut next_id = self.next_block_id.write();
        let mut range_index = self.block_range_index.write();

        let (block_id, is_new, new_block_pk_start) = if let Some(id) = *active_id {
            if let Some(block) = blocks.get(&id) {
                if block.should_split(self.config.target_block_size) {
                    // Close current block and create new one
                    if let Some(b) = blocks.get_mut(&id) {
                        b.close();
                    }
                    let new_id = *next_id;
                    *next_id += 1;
                    let new_block = Block::new_with_pk(new_id, pk);
                    let pk_start_key = new_block.pk_start_key();
                    blocks.insert(new_id, new_block);
                    *active_id = Some(new_id);
                    (new_id, true, Some(pk_start_key))
                } else {
                    (id, false, None)
                }
            } else {
                // Active block doesn't exist, create new one
                let new_id = *next_id;
                *next_id += 1;
                let new_block = Block::new_with_pk(new_id, pk);
                let pk_start_key = new_block.pk_start_key();
                blocks.insert(new_id, new_block);
                *active_id = Some(new_id);
                (new_id, true, Some(pk_start_key))
            }
        } else {
            // No active block, create first one
            let new_id = *next_id;
            *next_id += 1;
            let new_block = Block::new_with_pk(new_id, pk);
            let pk_start_key = new_block.pk_start_key();
            blocks.insert(new_id, new_block);
            *active_id = Some(new_id);
            (new_id, true, Some(pk_start_key))
        };

        // Update the block
        if let Some(block) = blocks.get_mut(&block_id) {
            block.add_row(&pk_str);
        }

        // Add new block to range index if created
        if let Some(pk_start_key) = new_block_pk_start {
            range_index.insert(pk_start_key, block_id);
        }

        // Cache PK -> block mapping in LRU cache
        drop(blocks);
        drop(range_index);
        self.pk_to_block.lock().put(pk_str, block_id);

        Ok((block_id, is_new))
    }

    /// Handle a delete event.
    pub fn handle_delete(&self, pk: &PrimaryKey) -> Option<BlockId> {
        let pk_str = pk.to_string_repr();

        // Find and update the block
        if let Some(block_id) = self.find_block_for_pk(pk) {
            let mut blocks = self.blocks.write();
            if let Some(block) = blocks.get_mut(&block_id) {
                block.remove_row();
            }

            // Remove from LRU cache
            self.pk_to_block.lock().pop(&pk_str);

            return Some(block_id);
        }

        None
    }

    /// Get candidate blocks that might contain a value in the given PK range.
    pub fn get_blocks_in_range(&self, pk_start: Option<&str>, pk_end: Option<&str>) -> Vec<BlockId> {
        let blocks = self.blocks.read();
        
        blocks
            .iter()
            .filter(|(_, block)| {
                // Check if block overlaps with query range using proper comparison
                let block_after_end = pk_end.map_or(false, |end| {
                    block.compare_pks(&block.pk_start, end) == std::cmp::Ordering::Greater
                });
                let block_before_start = pk_start.map_or(false, |start| {
                    block.compare_pks(&block.pk_end, start) == std::cmp::Ordering::Less
                });
                !block_after_end && !block_before_start
            })
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get all block IDs.
    pub fn get_all_block_ids(&self) -> Vec<BlockId> {
        self.blocks.read().keys().copied().collect()
    }

    /// Get the total row count across all blocks.
    pub fn total_row_count(&self) -> u64 {
        self.blocks.read().values().map(|b| b.row_count).sum()
    }

    /// Close the active block and start a new one on next insert.
    pub fn close_active_block(&self) {
        let mut active_id = self.active_block_id.write();
        if let Some(id) = *active_id {
            let mut blocks = self.blocks.write();
            if let Some(block) = blocks.get_mut(&id) {
                block.close();
            }
        }
        *active_id = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_creation() {
        let block = Block::new(1, "pk_001");
        assert_eq!(block.id, 1);
        assert_eq!(block.pk_start, "pk_001");
        assert_eq!(block.pk_end, "pk_001");
        assert_eq!(block.row_count, 0);
        assert!(!block.is_closed);
    }

    #[test]
    fn test_block_add_row() {
        let mut block = Block::new(1, "pk_005");
        block.add_row("pk_001");
        block.add_row("pk_010");

        assert_eq!(block.row_count, 2);
        assert_eq!(block.pk_start, "pk_001");
        assert_eq!(block.pk_end, "pk_010");
    }

    #[test]
    fn test_block_contains_pk() {
        let mut block = Block::new(1, "pk_005");
        block.add_row("pk_001");
        block.add_row("pk_010");

        assert!(block.contains_pk("pk_001"));
        assert!(block.contains_pk("pk_005"));
        assert!(block.contains_pk("pk_010"));
        assert!(!block.contains_pk("pk_000"));
        assert!(!block.contains_pk("pk_011"));
    }

    #[test]
    fn test_block_manager_assign() {
        let config = BlockManagerConfig {
            target_block_size: 100,
            auto_close: true,
            pk_cache_size: 1000,
        };
        let manager = BlockManager::new("source", "table", config);

        // First insert creates a new block
        let (block_id1, is_new1) = manager.assign_block(&PrimaryKey::from_i64(1)).unwrap();
        assert!(is_new1);
        assert_eq!(block_id1, 1);

        // Second insert to same block
        let (block_id2, is_new2) = manager.assign_block(&PrimaryKey::from_i64(2)).unwrap();
        assert!(!is_new2);
        assert_eq!(block_id2, 1);

        // Check row count
        let block = manager.get_block(1).unwrap();
        assert_eq!(block.row_count, 2);
    }

    #[test]
    fn test_block_manager_split() {
        let config = BlockManagerConfig {
            target_block_size: 2, // Very small for testing
            auto_close: true,
            pk_cache_size: 100,
        };
        let manager = BlockManager::new("source", "table", config);

        // Fill first block beyond split threshold (threshold = 2 * 2.0 = 4)
        // Need > 4 rows before the 6th PK triggers split
        for i in 0..7 {
            manager.assign_block(&PrimaryKey::from_i64(i)).unwrap();
        }

        // Should have created a new block (split triggers on 6th PK when block has 5 rows)
        let blocks = manager.get_all_blocks();
        assert!(blocks.len() >= 2, "Expected at least 2 blocks, got {}", blocks.len());
    }

    #[test]
    fn test_numeric_pk_ordering() {
        // This test verifies that numeric PKs are ordered correctly (not lexicographically)
        // In lexicographic order: "10" < "2" < "9"
        // In numeric order: 2 < 9 < 10
        
        let mut block = Block::new_with_pk(1, &PrimaryKey::from_i64(5));
        assert!(block.is_numeric_pk);
        
        block.add_row("2");
        block.add_row("10");
        
        assert_eq!(block.row_count, 2);
        // With proper numeric ordering, pk_start should be 2, pk_end should be 10
        assert_eq!(block.pk_start, "2");
        assert_eq!(block.pk_end, "10");
        
        // Test contains_pk with numeric ordering
        assert!(block.contains_pk("2"));
        assert!(block.contains_pk("5"));
        assert!(block.contains_pk("10"));
        assert!(block.contains_pk("7")); // 7 is between 2 and 10
        assert!(!block.contains_pk("1")); // 1 < 2
        assert!(!block.contains_pk("11")); // 11 > 10
        
        // This is the key test - with lexicographic comparison "10" < "2",
        // so contains_pk("3") would be false. With numeric comparison it's true.
        assert!(block.contains_pk("3")); // 3 is between 2 and 10
    }

    #[test]
    fn test_string_pk_lexicographic_ordering() {
        // String PKs should still use lexicographic ordering
        let mut block = Block::new(1, "apple");
        assert!(!block.is_numeric_pk);
        
        block.add_row("banana");
        block.add_row("cherry");
        
        assert_eq!(block.pk_start, "apple");
        assert_eq!(block.pk_end, "cherry");
        
        assert!(block.contains_pk("banana"));
        assert!(block.contains_pk("blueberry")); // between banana and cherry
        assert!(!block.contains_pk("aardvark")); // before apple
        assert!(!block.contains_pk("date")); // after cherry
    }
}
