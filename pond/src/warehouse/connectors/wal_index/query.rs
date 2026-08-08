//! Two-Phase Query Executor
//!
//! Executes queries using the two-phase approach:
//! 1. Block elimination using skip indexes
//! 2. PK resolution using inverted indexes

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use roaring::RoaringBitmap;

use super::block::{BlockId, BlockManager};
use super::inverted_index::InvertedIndexManager;
use super::skip_index::BlockSkipIndex;
use super::storage::WalIndexStorage;
use super::types::{ColumnValue, PrimaryKey};
use crate::warehouse::connectors::{ConnectorError, ConnectorResult};

/// Predicate operator for queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateOp {
    /// Equality: column = value
    Eq,
    /// Not equal: column != value
    Ne,
    /// Greater than: column > value
    Gt,
    /// Greater than or equal: column >= value
    Gte,
    /// Less than: column < value
    Lt,
    /// Less than or equal: column <= value
    Lte,
    /// Column is null
    IsNull,
    /// Column is not null
    IsNotNull,
    /// Value in list: column IN (values)
    In,
}

impl PredicateOp {
    /// Check if this operator can use inverted index for exact match.
    pub fn can_use_inverted_index(&self) -> bool {
        matches!(self, Self::Eq | Self::In)
    }

    /// Check if this operator can use min/max for elimination.
    pub fn can_use_min_max(&self) -> bool {
        matches!(self, Self::Eq | Self::Gt | Self::Gte | Self::Lt | Self::Lte)
    }
}

/// A query predicate.
#[derive(Debug, Clone)]
pub struct Predicate {
    /// Column name
    pub column: String,
    /// Operator
    pub op: PredicateOp,
    /// Value(s) to compare against
    pub values: Vec<ColumnValue>,
}

impl Predicate {
    /// Create an equality predicate.
    pub fn eq(column: impl Into<String>, value: ColumnValue) -> Self {
        Self {
            column: column.into(),
            op: PredicateOp::Eq,
            values: vec![value],
        }
    }

    /// Create a greater-than predicate.
    pub fn gt(column: impl Into<String>, value: ColumnValue) -> Self {
        Self {
            column: column.into(),
            op: PredicateOp::Gt,
            values: vec![value],
        }
    }

    /// Create a greater-than-or-equal predicate.
    pub fn gte(column: impl Into<String>, value: ColumnValue) -> Self {
        Self {
            column: column.into(),
            op: PredicateOp::Gte,
            values: vec![value],
        }
    }

    /// Create a less-than predicate.
    pub fn lt(column: impl Into<String>, value: ColumnValue) -> Self {
        Self {
            column: column.into(),
            op: PredicateOp::Lt,
            values: vec![value],
        }
    }

    /// Create a less-than-or-equal predicate.
    pub fn lte(column: impl Into<String>, value: ColumnValue) -> Self {
        Self {
            column: column.into(),
            op: PredicateOp::Lte,
            values: vec![value],
        }
    }

    /// Create an IN predicate.
    pub fn in_values(column: impl Into<String>, values: Vec<ColumnValue>) -> Self {
        Self {
            column: column.into(),
            op: PredicateOp::In,
            values,
        }
    }

    /// Create an IS NULL predicate.
    pub fn is_null(column: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            op: PredicateOp::IsNull,
            values: vec![],
        }
    }

    /// Create an IS NOT NULL predicate.
    pub fn is_not_null(column: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            op: PredicateOp::IsNotNull,
            values: vec![],
        }
    }

    /// Get the first value (for single-value predicates).
    pub fn value(&self) -> Option<&ColumnValue> {
        self.values.first()
    }
}

/// Result of block elimination phase.
#[derive(Debug)]
pub struct BlockEliminationResult {
    /// Blocks that might contain matching rows.
    pub candidate_blocks: Vec<BlockId>,
    /// Blocks that were eliminated.
    pub eliminated_blocks: Vec<BlockId>,
    /// Columns that had useful skip indexes.
    pub indexed_columns: HashSet<String>,
}

/// Result of PK resolution phase.
#[derive(Debug)]
pub struct PkResolutionResult {
    /// Primary keys that match the predicates.
    pub matching_pks: Vec<PrimaryKey>,
    /// Whether this is a complete result (inverted index) or approximate (needs verification).
    pub is_complete: bool,
    /// Number of PKs before intersection.
    pub initial_pk_count: u64,
}

/// Two-phase query executor.
pub struct TwoPhaseQueryExecutor {
    /// Source identifier
    source_id: String,
    /// Table name
    table_name: String,
    /// Block manager
    block_manager: Arc<BlockManager>,
    /// Inverted index manager
    inverted_index_manager: Arc<InvertedIndexManager>,
    /// Storage for loading indexes
    storage: Arc<dyn WalIndexStorage>,
    /// Cached skip indexes: block_id -> column -> index
    skip_index_cache: parking_lot::RwLock<HashMap<BlockId, HashMap<String, BlockSkipIndex>>>,
}

impl TwoPhaseQueryExecutor {
    /// Create a new query executor.
    pub fn new(
        source_id: impl Into<String>,
        table_name: impl Into<String>,
        block_manager: Arc<BlockManager>,
        inverted_index_manager: Arc<InvertedIndexManager>,
        storage: Arc<dyn WalIndexStorage>,
    ) -> Self {
        Self {
            source_id: source_id.into(),
            table_name: table_name.into(),
            block_manager,
            inverted_index_manager,
            storage,
            skip_index_cache: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    /// Execute a query with the two-phase approach.
    ///
    /// Returns primary keys that match all predicates.
    pub async fn execute(&self, predicates: &[Predicate]) -> ConnectorResult<Vec<PrimaryKey>> {
        if predicates.is_empty() {
            // No predicates = return all PKs (would need to fetch from source)
            return Err(ConnectorError::Validation(
                "At least one predicate is required for indexed query".to_string(),
            ));
        }

        // Phase 1: Block elimination
        let elimination_result = self.phase1_block_elimination(predicates).await?;

        if elimination_result.candidate_blocks.is_empty() {
            return Ok(Vec::new()); // No matching blocks
        }

        // Phase 2: PK resolution
        let pk_result = self
            .phase2_pk_resolution(predicates, &elimination_result.candidate_blocks)
            .await?;

        Ok(pk_result.matching_pks)
    }

    /// Phase 1: Block elimination using skip indexes.
    pub async fn phase1_block_elimination(
        &self,
        predicates: &[Predicate],
    ) -> ConnectorResult<BlockEliminationResult> {
        let all_blocks = self.block_manager.get_all_block_ids();

        if all_blocks.is_empty() {
            return Ok(BlockEliminationResult {
                candidate_blocks: Vec::new(),
                eliminated_blocks: Vec::new(),
                indexed_columns: HashSet::new(),
            });
        }

        // Load skip indexes for all blocks
        self.ensure_skip_indexes_loaded(&all_blocks).await?;

        let mut candidate_blocks: HashSet<BlockId> = all_blocks.iter().copied().collect();
        let mut indexed_columns = HashSet::new();

        // For each predicate, try to eliminate blocks
        for predicate in predicates {
            if !predicate.op.can_use_min_max() {
                continue;
            }

            let cache = self.skip_index_cache.read();

            let mut eliminated = Vec::new();

            for &block_id in &all_blocks {
                if !candidate_blocks.contains(&block_id) {
                    continue; // Already eliminated
                }

                if let Some(block_indexes) = cache.get(&block_id) {
                    if let Some(index) = block_indexes.get(&predicate.column) {
                        indexed_columns.insert(predicate.column.clone());

                        let might_match = self.check_skip_index(index, predicate);
                        if !might_match {
                            eliminated.push(block_id);
                        }
                    }
                }
            }

            for block_id in eliminated {
                candidate_blocks.remove(&block_id);
            }
        }

        let eliminated_blocks: Vec<BlockId> = all_blocks
            .iter()
            .filter(|id| !candidate_blocks.contains(id))
            .copied()
            .collect();

        Ok(BlockEliminationResult {
            candidate_blocks: candidate_blocks.into_iter().collect(),
            eliminated_blocks,
            indexed_columns,
        })
    }

    /// Phase 2: PK resolution using inverted indexes.
    pub async fn phase2_pk_resolution(
        &self,
        predicates: &[Predicate],
        candidate_blocks: &[BlockId],
    ) -> ConnectorResult<PkResolutionResult> {
        if candidate_blocks.is_empty() {
            return Ok(PkResolutionResult {
                matching_pks: Vec::new(),
                is_complete: true,
                initial_pk_count: 0,
            });
        }

        let mut result_bitmap: Option<RoaringBitmap> = None;
        let mut is_complete = true;
        let mut initial_pk_count = 0u64;
        let mut has_string_pks = false;
        let mut all_hash_mappings: HashMap<u32, HashSet<String>> = HashMap::new();

        for predicate in predicates {
            if !predicate.op.can_use_inverted_index() {
                is_complete = false;
                continue;
            }

            if self.inverted_index_manager.is_high_cardinality(&predicate.column) {
                is_complete = false;
                continue;
            }

            let mut predicate_bitmap = RoaringBitmap::new();

            match predicate.op {
                PredicateOp::Eq => {
                    if let Some(value) = predicate.value() {
                        if let Some((pks, is_string, mappings)) = self.inverted_index_manager.get_matching_pks_with_strings(&predicate.column, value) {
                            predicate_bitmap |= &pks;
                            if is_string {
                                has_string_pks = true;
                                for (hash, originals) in mappings {
                                    all_hash_mappings.entry(hash).or_default().extend(originals);
                                }
                            }
                        }
                    }
                }
                PredicateOp::In => {
                    for value in &predicate.values {
                        if let Some((pks, is_string, mappings)) = self.inverted_index_manager.get_matching_pks_with_strings(&predicate.column, value) {
                            predicate_bitmap |= &pks;
                            if is_string {
                                has_string_pks = true;
                                for (hash, originals) in mappings {
                                    all_hash_mappings.entry(hash).or_default().extend(originals);
                                }
                            }
                        }
                    }
                }
                _ => {
                    is_complete = false;
                    continue;
                }
            }

            if result_bitmap.is_none() {
                initial_pk_count = predicate_bitmap.len();
                result_bitmap = Some(predicate_bitmap);
            } else {
                result_bitmap = result_bitmap.map(|mut bm| {
                    bm &= &predicate_bitmap;
                    bm
                });
            }
        }

        let matching_pks: Vec<PrimaryKey> = result_bitmap
            .map(|bm| {
                if has_string_pks {
                    bm.iter()
                        .flat_map(|id| {
                            if let Some(originals) = all_hash_mappings.get(&id) {
                                originals
                                    .iter()
                                    .map(|s| PrimaryKey::from_string(s.clone()))
                                    .collect::<Vec<_>>()
                            } else {
                                vec![PrimaryKey::from_i64(id as i64)]
                            }
                        })
                        .collect()
                } else {
                    bm.iter().map(|id| PrimaryKey::from_i64(id as i64)).collect()
                }
            })
            .unwrap_or_default();

        Ok(PkResolutionResult {
            matching_pks,
            is_complete,
            initial_pk_count,
        })
    }

    /// Check if a block might contain matching values based on skip index.
    fn check_skip_index(&self, index: &BlockSkipIndex, predicate: &Predicate) -> bool {
        let value = match predicate.value() {
            Some(v) => v,
            None => return true, // Can't determine without value
        };

        match predicate.op {
            PredicateOp::Eq => index.might_contain_eq(value),
            PredicateOp::Gt => index.might_contain_gt(value),
            PredicateOp::Gte => index.might_contain_gte(value),
            PredicateOp::Lt => index.might_contain_lt(value),
            PredicateOp::Lte => index.might_contain_lte(value),
            PredicateOp::In => {
                // For IN, check if any value might be in the block
                predicate.values.iter().any(|v| index.might_contain_eq(v))
            }
            _ => true, // Can't use skip index for other ops
        }
    }

    /// Ensure skip indexes are loaded into cache.
    async fn ensure_skip_indexes_loaded(&self, block_ids: &[BlockId]) -> ConnectorResult<()> {
        let missing: Vec<BlockId> = {
            let cache = self.skip_index_cache.read();
            block_ids
                .iter()
                .filter(|id| !cache.contains_key(id))
                .copied()
                .collect()
        };

        if missing.is_empty() {
            return Ok(());
        }

        // Load from storage
        let stored = self
            .storage
            .load_skip_indexes_for_blocks(&self.source_id, &self.table_name, &missing)
            .await?;

        // Parse and cache
        let mut cache = self.skip_index_cache.write();
        for entry in stored {
            if let Some(index) = BlockSkipIndex::from_base64(&entry.index_data, entry.index_type) {
                cache
                    .entry(entry.block_id)
                    .or_insert_with(HashMap::new)
                    .insert(entry.column_name, index);
            }
        }

        Ok(())
    }

    /// Add a skip index to cache (for newly built indexes).
    pub fn cache_skip_index(&self, block_id: BlockId, column: &str, index: BlockSkipIndex) {
        let mut cache = self.skip_index_cache.write();
        cache
            .entry(block_id)
            .or_insert_with(HashMap::new)
            .insert(column.to_string(), index);
    }

    /// Clear the skip index cache.
    pub fn clear_cache(&self) {
        self.skip_index_cache.write().clear();
    }

    /// Get statistics about cached indexes.
    pub fn cache_stats(&self) -> (usize, usize) {
        let cache = self.skip_index_cache.read();
        let block_count = cache.len();
        let index_count: usize = cache.values().map(|m| m.len()).sum();
        (block_count, index_count)
    }
}

/// Build a query from filter conditions.
pub struct QueryBuilder {
    predicates: Vec<Predicate>,
}

impl QueryBuilder {
    /// Create a new query builder.
    pub fn new() -> Self {
        Self {
            predicates: Vec::new(),
        }
    }

    /// Add an equality filter.
    pub fn filter_eq(mut self, column: impl Into<String>, value: ColumnValue) -> Self {
        self.predicates.push(Predicate::eq(column, value));
        self
    }

    /// Add a greater-than filter.
    pub fn filter_gt(mut self, column: impl Into<String>, value: ColumnValue) -> Self {
        self.predicates.push(Predicate::gt(column, value));
        self
    }

    /// Add a greater-than-or-equal filter.
    pub fn filter_gte(mut self, column: impl Into<String>, value: ColumnValue) -> Self {
        self.predicates.push(Predicate::gte(column, value));
        self
    }

    /// Add a less-than filter.
    pub fn filter_lt(mut self, column: impl Into<String>, value: ColumnValue) -> Self {
        self.predicates.push(Predicate::lt(column, value));
        self
    }

    /// Add a less-than-or-equal filter.
    pub fn filter_lte(mut self, column: impl Into<String>, value: ColumnValue) -> Self {
        self.predicates.push(Predicate::lte(column, value));
        self
    }

    /// Add an IN filter.
    pub fn filter_in(mut self, column: impl Into<String>, values: Vec<ColumnValue>) -> Self {
        self.predicates.push(Predicate::in_values(column, values));
        self
    }

    /// Build the query predicates.
    pub fn build(self) -> Vec<Predicate> {
        self.predicates
    }
}

impl Default for QueryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_predicate_creation() {
        let eq = Predicate::eq("status", ColumnValue::String("pending".to_string()));
        assert_eq!(eq.column, "status");
        assert_eq!(eq.op, PredicateOp::Eq);

        let gt = Predicate::gt("amount", ColumnValue::Float64(1000.0));
        assert_eq!(gt.column, "amount");
        assert_eq!(gt.op, PredicateOp::Gt);
    }

    #[test]
    fn test_predicate_op_capabilities() {
        assert!(PredicateOp::Eq.can_use_inverted_index());
        assert!(PredicateOp::In.can_use_inverted_index());
        assert!(!PredicateOp::Gt.can_use_inverted_index());

        assert!(PredicateOp::Eq.can_use_min_max());
        assert!(PredicateOp::Gt.can_use_min_max());
        assert!(!PredicateOp::IsNull.can_use_min_max());
    }

    #[test]
    fn test_query_builder() {
        let predicates = QueryBuilder::new()
            .filter_eq("status", ColumnValue::String("pending".to_string()))
            .filter_gt("amount", ColumnValue::Float64(1000.0))
            .filter_lte("created_at", ColumnValue::Timestamp(1234567890))
            .build();

        assert_eq!(predicates.len(), 3);
        assert_eq!(predicates[0].column, "status");
        assert_eq!(predicates[1].column, "amount");
        assert_eq!(predicates[2].column, "created_at");
    }
}
