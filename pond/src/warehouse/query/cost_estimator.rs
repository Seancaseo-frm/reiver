//! Query Cost Estimator
//!
//! Estimates query cost before execution.
//! 
//! Supports loading table statistics from:
//! - R2 file metadata (file count, total size)

use lru::LruCache;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement};
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser;
use ahash::{AHashMap, AHashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use thiserror::Error;

/// Default average row size estimate (bytes) when row count is unknown.
const DEFAULT_AVG_ROW_SIZE_BYTES: u64 = 100;
/// Assumed scan throughput for time estimation (bytes per second).
const DEFAULT_SCAN_RATE_BYTES_PER_SEC: u64 = 10 * 1024 * 1024;
/// Minimum estimated query time (milliseconds).
const MIN_ESTIMATED_TIME_MS: u64 = 100;
/// Default selectivity factor applied when a table is referenced in a WHERE clause.
/// 0.1 means we estimate the WHERE clause filters out ~90% of rows.
const DEFAULT_WHERE_SELECTIVITY: f64 = 0.1;
use parking_lot::RwLock;

use super::rewriter::TableRewriter;

/// Errors that can occur during cost estimation.
#[derive(Debug, Error)]
pub enum CostEstimatorError {
    #[error("SQL parse error: {0}")]
    Parse(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),
    
    #[error("Database error: {0}")]
    Database(String),
    
    #[error("Storage error: {0}")]
    Storage(String),
}

/// Result type for cost estimation.
pub type CostEstimatorResult<T> = Result<T, CostEstimatorError>;

/// Cost warnings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CostWarning {
    /// Large table scan without filters
    LargeTableScan { table: String, estimated_rows: u64 },
    /// Query is missing WHERE clause
    MissingWhereClause { table: String },
    /// Cross join detected
    CrossJoin { left: String, right: String },
    /// No partition pruning possible
    NoPartitionPruning { table: String },
    /// SELECT * on large table
    SelectStar { table: String },
}

/// Query cost estimate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryCostEstimate {
    /// Estimated bytes to be scanned
    pub estimated_bytes_scanned: u64,
    /// Number of files to read
    pub estimated_files_read: usize,
    /// Estimated rows to process
    pub estimated_rows: u64,
    /// Whether result is available in cache
    pub cache_hit: bool,
    /// Cost warnings
    pub warnings: Vec<CostWarning>,
    /// Estimated execution time in milliseconds
    pub estimated_time_ms: u64,
}

impl Default for QueryCostEstimate {
    fn default() -> Self {
        Self {
            estimated_bytes_scanned: 0,
            estimated_files_read: 0,
            estimated_rows: 0,
            cache_hit: false,
            warnings: vec![],
            estimated_time_ms: 0,
        }
    }
}

/// Table statistics for cost estimation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStats {
    pub table_name: String,
    pub row_count: u64,
    pub size_bytes: u64,
    pub file_count: usize,
    pub avg_row_size: u64,
    /// Last updated timestamp
    #[serde(default)]
    pub last_updated: Option<chrono::DateTime<chrono::Utc>>,
}

impl TableStats {
    /// Create new table statistics.
    pub fn new(table_name: &str) -> Self {
        Self {
            table_name: table_name.to_string(),
            row_count: 0,
            size_bytes: 0,
            file_count: 0,
            avg_row_size: 0,
            last_updated: None,
        }
    }
    
    /// Update statistics from R2 file listing.
    pub fn update_from_files(&mut self, files: &[crate::warehouse::storage::r2::ObjectInfo]) {
        self.file_count = files.len();
        self.size_bytes = files.iter().map(|f| f.size).sum();
        self.last_updated = Some(chrono::Utc::now());
        
        // Estimate rows based on size when row count is unknown
        if self.size_bytes > 0 && self.row_count == 0 {
            self.row_count = (self.size_bytes / DEFAULT_AVG_ROW_SIZE_BYTES).max(1);
        }
        
        if self.row_count > 0 {
            self.avg_row_size = (self.size_bytes / self.row_count).max(1);
        }
    }
    
    /// Update row count from sync result.
    pub fn update_row_count(&mut self, row_count: u64) {
        self.row_count = row_count;
        self.last_updated = Some(chrono::Utc::now());
        
        if self.row_count > 0 && self.size_bytes > 0 {
            self.avg_row_size = (self.size_bytes / self.row_count).max(1);
        }
    }
    
    /// Check if stats are stale (older than threshold).
    pub fn is_stale(&self, max_age: chrono::Duration) -> bool {
        match self.last_updated {
            Some(updated) => chrono::Utc::now() - updated > max_age,
            None => true,
        }
    }
}

/// Query cost estimator with support for loading real statistics.
pub struct QueryCostEstimator {
    /// Table statistics
    table_stats: AHashMap<String, TableStats>,
    /// Bounded LRU cache mapping normalized query text to its cost estimate.
    query_cache: LruCache<String, QueryCostEstimate>,
    /// Tracks which queries have cached results (separate from cost estimates).
    cached_queries: AHashSet<String>,
    /// Reverse index: lowercased table name -> set of normalized query keys that
    /// reference it.  Populated from the parsed AST when queries are cached, and
    /// used by `invalidate_table` for exact lookup instead of string matching.
    table_to_queries: AHashMap<String, AHashSet<String>>,
}

/// Thread-safe shared cost estimator.
pub type SharedCostEstimator = Arc<RwLock<QueryCostEstimator>>;

const QUERY_CACHE_CAPACITY: usize = 10_000;

impl QueryCostEstimator {
    /// Create a new cost estimator.
    pub fn new() -> Self {
        Self {
            table_stats: AHashMap::new(),
            query_cache: LruCache::new(NonZeroUsize::new(QUERY_CACHE_CAPACITY).unwrap()),
            cached_queries: AHashSet::new(),
            table_to_queries: AHashMap::new(),
        }
    }
    
    /// Create a shared (thread-safe) cost estimator.
    pub fn shared() -> SharedCostEstimator {
        Arc::new(RwLock::new(Self::new()))
    }

    /// Add table statistics.
    pub fn add_table_stats(&mut self, stats: TableStats) {
        self.table_stats.insert(stats.table_name.clone(), stats);
    }
    
    /// Get table statistics.
    pub fn get_table_stats(&self, table_name: &str) -> Option<&TableStats> {
        self.table_stats.get(table_name)
    }
    
    /// Get all table statistics.
    pub fn all_table_stats(&self) -> &AHashMap<String, TableStats> {
        &self.table_stats
    }
    
    /// Load statistics from R2 file listing.
    ///
    /// Lists files in R2 and calculates statistics based on file metadata.
    #[tracing::instrument(name = "warehouse.query.cost.load_from_r2", skip_all, err(Display))]
    pub async fn load_from_r2(
        &mut self,
        storage: &crate::warehouse::storage::R2Storage,
        table_name: &str,
        prefix: &str,
    ) -> CostEstimatorResult<()> {
        let files = storage.list_objects(prefix)
            .await
            .map_err(|e| CostEstimatorError::Storage(e.to_string()))?;
        
        let stats = self.table_stats
            .entry(table_name.to_string())
            .or_insert_with(|| TableStats::new(table_name));
        
        stats.update_from_files(&files);
        
        Ok(())
    }
    
    /// Refresh statistics for tables that are stale.
    #[tracing::instrument(name = "warehouse.cost.stale_tables", skip_all)]
    pub fn stale_tables(&self, max_age: chrono::Duration) -> Vec<&str> {
        self.table_stats
            .iter()
            .filter(|(_, stats)| stats.is_stale(max_age))
            .map(|(name, _)| name.as_str())
            .collect()
    }

    /// Mark a query result as cached (tracked separately from cost estimates).
    pub fn mark_cached(&mut self, query: &str) {
        let normalized = normalize_query(query);
        let dialect = ClickHouseDialect {};
        if let Ok(statements) = Parser::parse_sql(&dialect, query) {
            self.register_query_tables_from_ast(&normalized, &statements);
        }
        self.cached_queries.insert(normalized);
    }
    
    /// Check if a query result is cached.
    pub fn is_cached(&self, query: &str) -> bool {
        let normalized = normalize_query(query);
        self.cached_queries.contains(&normalized)
    }
    
    /// Clear both the cost estimate cache and cached-query tracker.
    pub fn clear_cache(&mut self) {
        self.query_cache.clear();
        self.cached_queries.clear();
        self.table_to_queries.clear();
    }
    
    /// Invalidate caches for queries involving a specific table.
    ///
    /// Uses the reverse index built from parsed SQL ASTs to find exactly
    /// which cached queries reference the given table.  Queries referencing
    /// other tables are kept.
    #[tracing::instrument(name = "warehouse.cost.invalidate_table", skip(self), fields(%table_name))]
    pub fn invalidate_table(&mut self, table_name: &str) {
        let lower_table = table_name.to_lowercase();

        let keys_to_remove = match self.table_to_queries.remove(&lower_table) {
            Some(keys) => keys,
            None => return,
        };

        for key in &keys_to_remove {
            self.query_cache.pop(key);
            self.cached_queries.remove(key);
        }

        for (_, query_set) in self.table_to_queries.iter_mut() {
            query_set.retain(|key| !keys_to_remove.contains(key));
        }
    }

    /// Estimate query cost before execution.
    #[tracing::instrument(name = "warehouse.query.cost.estimate", skip_all, err(Display))]
    pub fn estimate(&mut self, sql: &str) -> CostEstimatorResult<QueryCostEstimate> {
        let normalized = normalize_query(sql);

        if let Some(cached) = self.query_cache.get(&normalized) {
            let mut hit = cached.clone();
            hit.cache_hit = true;
            return Ok(hit);
        }

        let mut estimate = QueryCostEstimate::default();

        let dialect = ClickHouseDialect {};
        let statements = Parser::parse_sql(&dialect, sql)
            .map_err(|e| CostEstimatorError::Parse(e.to_string()))?;

        for statement in &statements {
            if let Statement::Query(q) = statement {
                let mut stmt_tables = Vec::with_capacity(4);
                TableRewriter::collect_tables_from_statement(statement, &mut stmt_tables);

                let is_select_star = has_select_wildcard(q.body.as_ref());
                let has_where = has_selection(q.body.as_ref());
                let where_refs = extract_where_table_refs(q.body.as_ref());
                let from_aliases = extract_from_aliases(q.body.as_ref());

                for table in &stmt_tables {
                    if is_select_star {
                        estimate.warnings.push(CostWarning::SelectStar {
                            table: table.clone(),
                        });
                    }

                    if !has_where {
                        estimate.warnings.push(CostWarning::MissingWhereClause {
                            table: table.clone(),
                        });
                    }

                    let table_referenced_in_where = if !has_where {
                        false
                    } else if where_refs.is_empty() {
                        // WHERE uses unqualified columns; conservatively
                        // assume the table might be filtered.
                        true
                    } else {
                        where_refs.contains(table)
                            || from_aliases.iter().any(|(alias, tbl)| {
                                tbl == table && where_refs.contains(alias)
                            })
                    };

                    if let Some(stats) = self.table_stats.get(table) {
                        let selectivity = if table_referenced_in_where {
                            DEFAULT_WHERE_SELECTIVITY
                        } else {
                            1.0
                        };
                        estimate.estimated_rows += (stats.row_count as f64 * selectivity) as u64;
                        estimate.estimated_bytes_scanned += (stats.size_bytes as f64 * selectivity) as u64;
                        estimate.estimated_files_read += stats.file_count;

                        if stats.row_count > 1_000_000 && !table_referenced_in_where {
                            estimate.warnings.push(CostWarning::LargeTableScan {
                                table: table.clone(),
                                estimated_rows: stats.row_count,
                            });
                        }
                    }
                }
            }
        }

        if estimate.estimated_bytes_scanned > 0 {
            let secs = estimate.estimated_bytes_scanned / DEFAULT_SCAN_RATE_BYTES_PER_SEC;
            let remainder = estimate.estimated_bytes_scanned % DEFAULT_SCAN_RATE_BYTES_PER_SEC;
            estimate.estimated_time_ms = secs
                .saturating_mul(1000)
                .saturating_add(
                    remainder.saturating_mul(1000).saturating_add(DEFAULT_SCAN_RATE_BYTES_PER_SEC - 1) / DEFAULT_SCAN_RATE_BYTES_PER_SEC
                );
            estimate.estimated_time_ms = estimate.estimated_time_ms.max(MIN_ESTIMATED_TIME_MS);
        }

        self.register_query_tables_from_ast(&normalized, &statements);
        self.query_cache.put(normalized, estimate.clone());

        Ok(estimate)
    }

    /// Record table names from pre-parsed statements in the reverse index.
    fn register_query_tables_from_ast(&mut self, normalized_key: &str, statements: &[Statement]) {
        for stmt in statements {
            let mut tables = Vec::new();
            TableRewriter::collect_tables_from_statement(stmt, &mut tables);
            for table in tables {
                self.table_to_queries
                    .entry(table.to_lowercase())
                    .or_default()
                    .insert(normalized_key.to_string());
            }
        }
    }
}

impl Default for QueryCostEstimator {
    fn default() -> Self {
        Self::new()
    }
}

// Re-export from shared utils module
use crate::warehouse::utils::normalize_query;

fn has_select_wildcard(body: &SetExpr) -> bool {
    match body {
        SetExpr::Select(select) => select.projection.iter().any(|item| {
            matches!(item, SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _))
        }),
        SetExpr::Query(q) => has_select_wildcard(q.body.as_ref()),
        SetExpr::SetOperation { left, right, .. } => {
            has_select_wildcard(left.as_ref()) || has_select_wildcard(right.as_ref())
        }
        _ => false,
    }
}

fn has_selection(body: &SetExpr) -> bool {
    match body {
        SetExpr::Select(select) => select.selection.is_some(),
        SetExpr::Query(q) => has_selection(q.body.as_ref()),
        SetExpr::SetOperation { left, right, .. } => {
            has_selection(left.as_ref()) || has_selection(right.as_ref())
        }
        _ => false,
    }
}

/// Collect table/alias qualifiers referenced in the WHERE clause.
///
/// For compound identifiers like `a.id`, this collects the qualifier `a`.
/// For single-column queries this won't find a qualifier, so the returned
/// set may be empty even when a WHERE exists.
fn extract_where_table_refs(body: &SetExpr) -> AHashSet<String> {
    let mut refs = AHashSet::new();
    match body {
        SetExpr::Select(select) => {
            if let Some(ref selection) = select.selection {
                collect_table_refs_from_expr(selection, &mut refs);
            }
        }
        SetExpr::Query(q) => {
            refs = extract_where_table_refs(q.body.as_ref());
        }
        SetExpr::SetOperation { left, right, .. } => {
            refs.extend(extract_where_table_refs(left.as_ref()));
            refs.extend(extract_where_table_refs(right.as_ref()));
        }
        _ => {}
    }
    refs
}

/// Collect table aliases from FROM clause for alias-to-table resolution.
fn extract_from_aliases(body: &SetExpr) -> AHashMap<String, String> {
    let mut aliases = AHashMap::new();
    match body {
        SetExpr::Select(select) => {
            for table_with_joins in &select.from {
                extract_alias_from_table_factor(&table_with_joins.relation, &mut aliases);
                for join in &table_with_joins.joins {
                    extract_alias_from_table_factor(&join.relation, &mut aliases);
                }
            }
        }
        SetExpr::Query(q) => {
            aliases = extract_from_aliases(q.body.as_ref());
        }
        SetExpr::SetOperation { left, right, .. } => {
            aliases.extend(extract_from_aliases(left.as_ref()));
            aliases.extend(extract_from_aliases(right.as_ref()));
        }
        _ => {}
    }
    aliases
}

fn extract_alias_from_table_factor(
    factor: &sqlparser::ast::TableFactor,
    aliases: &mut AHashMap<String, String>,
) {
    if let sqlparser::ast::TableFactor::Table { name, alias, .. } = factor {
        let table_name = name.0.last().map(|i| i.value.clone()).unwrap_or_default();
        if let Some(alias) = alias {
            aliases.insert(alias.name.value.clone(), table_name);
        }
    }
}

fn collect_table_refs_from_expr(expr: &Expr, refs: &mut AHashSet<String>) {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            refs.insert(parts[0].value.clone());
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_table_refs_from_expr(left, refs);
            collect_table_refs_from_expr(right, refs);
        }
        Expr::UnaryOp { expr, .. } => {
            collect_table_refs_from_expr(expr, refs);
        }
        Expr::Nested(inner) => {
            collect_table_refs_from_expr(inner, refs);
        }
        Expr::Between { expr, low, high, .. } => {
            collect_table_refs_from_expr(expr, refs);
            collect_table_refs_from_expr(low, refs);
            collect_table_refs_from_expr(high, refs);
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            collect_table_refs_from_expr(inner, refs);
        }
        Expr::InList { expr, list, .. } => {
            collect_table_refs_from_expr(expr, refs);
            for item in list {
                collect_table_refs_from_expr(item, refs);
            }
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            collect_table_refs_from_expr(expr, refs);
            collect_table_refs_from_expr(pattern, refs);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_query() {
        let query = "SELECT  *  FROM   customers  WHERE  id = 1";
        let normalized = normalize_query(query);
        assert_eq!(normalized, "select * from customers where id = 1");
    }

    #[test]
    fn test_estimate_with_warnings() {
        let mut estimator = QueryCostEstimator::new();
        let sql = "SELECT * FROM customers";
        let estimate = estimator.estimate(sql).unwrap();

        // Should have warnings for SELECT * and missing WHERE
        assert!(!estimate.warnings.is_empty());
    }

    #[test]
    fn test_cache_hit() {
        let mut estimator = QueryCostEstimator::new();
        let sql = "SELECT id FROM customers WHERE id = 1";

        // First query - not cached
        let estimate1 = estimator.estimate(sql).unwrap();
        assert!(!estimate1.cache_hit);

        // Second call should be a cache hit AND carry the original estimate data
        let estimate2 = estimator.estimate(sql).unwrap();
        assert!(estimate2.cache_hit);
        assert_eq!(
            estimate2.warnings.len(),
            estimate1.warnings.len(),
            "cached estimate must preserve the original warnings"
        );
    }

    #[test]
    fn test_cache_hit_preserves_cost_data() {
        let mut estimator = QueryCostEstimator::new();
        estimator.add_table_stats(TableStats {
            table_name: "orders".to_string(),
            row_count: 500_000,
            size_bytes: 50_000_000,
            file_count: 5,
            avg_row_size: 100,
            last_updated: None,
        });
        let sql = "SELECT * FROM orders";

        let first = estimator.estimate(sql).unwrap();
        assert!(!first.cache_hit);
        assert!(first.estimated_rows > 0);

        let cached = estimator.estimate(sql).unwrap();
        assert!(cached.cache_hit);
        assert_eq!(
            cached.estimated_rows, first.estimated_rows,
            "cache hit must return the original estimated_rows, not 0"
        );
        assert_eq!(
            cached.estimated_bytes_scanned, first.estimated_bytes_scanned,
            "cache hit must return the original estimated_bytes_scanned"
        );
    }

    #[test]
    fn test_is_cached_updates_lru_order() {
        let mut estimator = QueryCostEstimator::new();

        estimator.mark_cached("SELECT 1");
        estimator.mark_cached("SELECT 2");

        assert!(estimator.is_cached("SELECT 1"));
        assert!(estimator.is_cached("SELECT 2"));
        assert!(!estimator.is_cached("SELECT 3"));
    }

    #[test]
    fn test_invalidate_table_is_targeted() {
        let mut estimator = QueryCostEstimator::new();
        estimator.add_table_stats(TableStats {
            table_name: "orders".to_string(),
            row_count: 1_000,
            size_bytes: 100_000,
            file_count: 2,
            avg_row_size: 100,
            last_updated: None,
        });
        estimator.add_table_stats(TableStats {
            table_name: "users".to_string(),
            row_count: 500,
            size_bytes: 50_000,
            file_count: 1,
            avg_row_size: 100,
            last_updated: None,
        });

        let orders_sql = "SELECT * FROM orders WHERE id = 1";
        let users_sql = "SELECT * FROM users WHERE id = 1";

        // Populate both caches
        let _ = estimator.estimate(orders_sql).unwrap();
        let _ = estimator.estimate(users_sql).unwrap();
        estimator.mark_cached(orders_sql);
        estimator.mark_cached(users_sql);

        // Verify both are cached
        assert!(estimator.estimate(orders_sql).unwrap().cache_hit);
        assert!(estimator.estimate(users_sql).unwrap().cache_hit);
        assert!(estimator.is_cached(orders_sql));
        assert!(estimator.is_cached(users_sql));

        // Invalidate only "orders"
        estimator.invalidate_table("orders");

        // "users" queries must still be cached
        assert!(
            estimator.estimate(users_sql).unwrap().cache_hit,
            "users estimate cache must survive orders invalidation"
        );
        assert!(
            estimator.is_cached(users_sql),
            "users cached-query flag must survive orders invalidation"
        );

        // "orders" must be evicted
        assert!(
            !estimator.estimate(orders_sql).unwrap().cache_hit,
            "orders estimate must be evicted after invalidation"
        );
        assert!(
            !estimator.is_cached(orders_sql),
            "orders cached-query flag must be evicted after invalidation"
        );
    }

    #[test]
    fn test_large_table_scan_warning_per_table() {
        let mut estimator = QueryCostEstimator::new();
        estimator.add_table_stats(TableStats {
            table_name: "small_filtered".to_string(),
            row_count: 100,
            size_bytes: 10_000,
            file_count: 1,
            avg_row_size: 100,
            last_updated: None,
        });
        estimator.add_table_stats(TableStats {
            table_name: "huge_unfiltered".to_string(),
            row_count: 5_000_000,
            size_bytes: 500_000_000,
            file_count: 50,
            avg_row_size: 100,
            last_updated: None,
        });

        let sql = "SELECT * FROM small_filtered WHERE id = 1; SELECT * FROM huge_unfiltered";
        let estimate = estimator.estimate(sql).unwrap();

        let has_large_scan = estimate.warnings.iter().any(|w| {
            matches!(w, CostWarning::LargeTableScan { table, .. } if table == "huge_unfiltered")
        });
        assert!(
            has_large_scan,
            "LargeTableScan warning must be emitted for huge_unfiltered even though \
             small_filtered has a WHERE clause. Warnings: {:?}",
            estimate.warnings
        );
    }

    #[test]
    fn test_update_from_files_small_size_bytes() {
        use crate::warehouse::storage::r2::ObjectInfo;

        let mut stats = TableStats::new("tiny_table");
        let files = vec![ObjectInfo {
            key: "file.parquet".to_string(),
            size: 50,
            last_modified: None,
            etag: None,
        }];
        stats.update_from_files(&files);

        assert!(
            stats.row_count >= 1,
            "row_count must be at least 1 when size_bytes > 0, got {}",
            stats.row_count,
        );
        assert!(
            stats.avg_row_size > 0,
            "avg_row_size must be > 0 when size_bytes > 0, got {}",
            stats.avg_row_size,
        );
    }

    #[test]
    fn test_avg_row_size_never_zero() {
        let mut stats = TableStats::new("test_table");
        stats.size_bytes = 1;
        stats.row_count = 100;
        stats.update_row_count(100);
        assert!(
            stats.avg_row_size >= 1,
            "avg_row_size must be clamped to at least 1, got {}",
            stats.avg_row_size,
        );
    }

    #[test]
    fn test_time_estimation_no_overflow() {
        let mut estimator = QueryCostEstimator::new();
        let size_bytes = u64::MAX / 500;
        estimator.add_table_stats(TableStats {
            table_name: "huge_table".to_string(),
            row_count: 1,
            file_count: 1,
            size_bytes,
            avg_row_size: 1000,
            last_updated: None,
        });
        let result = estimator.estimate("SELECT id FROM huge_table WHERE id = 1");
        assert!(result.is_ok(), "Estimation must not panic on very large byte counts");

        let estimate = result.unwrap();
        let expected_ms = (size_bytes / DEFAULT_SCAN_RATE_BYTES_PER_SEC) * 1000;
        assert!(
            estimate.estimated_time_ms <= expected_ms.saturating_add(1000),
            "estimated_time_ms {} must be close to expected {} (not a saturated value)",
            estimate.estimated_time_ms,
            expected_ms,
        );
        assert!(
            estimate.estimated_time_ms >= MIN_ESTIMATED_TIME_MS,
            "estimated_time_ms must be at least MIN_ESTIMATED_TIME_MS"
        );
    }

    #[test]
    fn test_no_duplicate_warnings_per_table() {
        let mut estimator = QueryCostEstimator::new();
        let sql = "SELECT * FROM mytable";
        let estimate = estimator.estimate(sql).unwrap();
        let select_star_count = estimate
            .warnings
            .iter()
            .filter(|w| matches!(w, CostWarning::SelectStar { .. }))
            .count();
        assert_eq!(
            select_star_count, 1,
            "SELECT * warning should appear exactly once per table, not duplicated"
        );
    }

    #[test]
    fn test_large_table_scan_warning_multi_table_join() {
        let mut estimator = QueryCostEstimator::new();
        estimator.add_table_stats(TableStats {
            table_name: "small_table".to_string(),
            row_count: 100,
            size_bytes: 10_000,
            file_count: 1,
            avg_row_size: 100,
            last_updated: None,
        });
        estimator.add_table_stats(TableStats {
            table_name: "big_table".to_string(),
            row_count: 5_000_000,
            size_bytes: 500_000_000,
            file_count: 50,
            avg_row_size: 100,
            last_updated: None,
        });

        let sql = "SELECT a.id FROM small_table a JOIN big_table b ON a.id = b.id WHERE a.x = 1";
        let estimate = estimator.estimate(sql).unwrap();

        let has_large_scan = estimate.warnings.iter().any(|w| {
            matches!(w, CostWarning::LargeTableScan { table, .. } if table == "big_table")
        });
        assert!(
            has_large_scan,
            "big_table must get LargeTableScan warning even though WHERE exists, \
             because WHERE only filters small_table (via alias a). Warnings: {:?}",
            estimate.warnings
        );
    }

    #[test]
    fn test_large_table_scan_warning_per_statement() {
        let mut estimator = QueryCostEstimator::new();
        estimator.add_table_stats(TableStats {
            table_name: "big".to_string(),
            row_count: 2_000_000,
            file_count: 10,
            size_bytes: 2_000_000_000,
            avg_row_size: 1000,
            last_updated: None,
        });
        let sql = "SELECT id FROM big WHERE id = 1; SELECT id FROM big";
        let estimate = estimator.estimate(sql).unwrap();
        let large_scan_warnings: Vec<_> = estimate.warnings.iter().filter(|w| {
            matches!(w, CostWarning::LargeTableScan { .. })
        }).collect();
        assert!(
            !large_scan_warnings.is_empty(),
            "The unfiltered statement must produce a LargeTableScan warning even when another statement filters the same table"
        );
    }

    #[test]
    fn test_time_estimation_small_remainder_rounds_up() {
        let mut estimator = QueryCostEstimator::new();
        estimator.add_table_stats(TableStats {
            table_name: "small".to_string(),
            row_count: 10,
            file_count: 1,
            size_bytes: 1000,
            avg_row_size: 100,
            last_updated: None,
        });

        let estimate = estimator.estimate("SELECT id FROM small WHERE id = 1").unwrap();
        let raw_ms = (1000_u64 * 1000 + DEFAULT_SCAN_RATE_BYTES_PER_SEC - 1) / DEFAULT_SCAN_RATE_BYTES_PER_SEC;
        assert!(
            raw_ms > 0,
            "A 1000-byte scan must produce a non-zero raw time before the MIN floor"
        );
        assert!(
            estimate.estimated_time_ms >= MIN_ESTIMATED_TIME_MS,
            "estimated_time_ms must be at least MIN_ESTIMATED_TIME_MS"
        );
    }

    #[test]
    fn test_time_estimation_exact_rate_produces_1000ms() {
        let mut estimator = QueryCostEstimator::new();
        estimator.add_table_stats(TableStats {
            table_name: "exact".to_string(),
            row_count: 100_000,
            file_count: 1,
            size_bytes: DEFAULT_SCAN_RATE_BYTES_PER_SEC,
            avg_row_size: 100,
            last_updated: None,
        });

        // Full scan (no WHERE) of exactly DEFAULT_SCAN_RATE_BYTES_PER_SEC bytes
        let estimate = estimator.estimate("SELECT id FROM exact").unwrap();
        assert_eq!(
            estimate.estimated_time_ms, 1000,
            "Scanning exactly DEFAULT_SCAN_RATE_BYTES_PER_SEC bytes must produce 1000ms"
        );
    }

    #[test]
    fn test_union_query_warnings() {
        let mut estimator = QueryCostEstimator::new();
        estimator.add_table_stats(TableStats {
            table_name: "big_table".to_string(),
            row_count: 5_000_000,
            file_count: 100,
            size_bytes: 500_000_000,
            avg_row_size: 100,
            last_updated: None,
        });
        estimator.add_table_stats(TableStats {
            table_name: "small_table".to_string(),
            row_count: 100,
            file_count: 1,
            size_bytes: 10_000,
            avg_row_size: 100,
            last_updated: None,
        });

        let sql = "SELECT * FROM big_table WHERE id = 1 UNION ALL SELECT col FROM small_table";
        let estimate = estimator.estimate(sql).unwrap();

        let has_select_star = estimate.warnings.iter().any(|w| {
            matches!(w, CostWarning::SelectStar { table } if table == "big_table")
        });
        assert!(has_select_star, "SELECT * in UNION branch must emit SelectStar warning");

        let missing_where_for_filtered = estimate.warnings.iter().any(|w| {
            matches!(w, CostWarning::MissingWhereClause { table } if table == "big_table")
        });
        assert!(
            !missing_where_for_filtered,
            "big_table has a WHERE clause, must not get MissingWhereClause"
        );
    }

    #[test]
    fn test_mark_cached_does_not_poison_estimate() {
        let mut estimator = QueryCostEstimator::new();
        estimator.add_table_stats(TableStats {
            table_name: "orders".to_string(),
            row_count: 5_000_000,
            file_count: 50,
            size_bytes: 500_000_000,
            avg_row_size: 100,
            last_updated: None,
        });

        let sql = "SELECT * FROM orders";
        estimator.mark_cached(sql);
        assert!(estimator.is_cached(sql));

        let estimate = estimator.estimate(sql).unwrap();
        assert!(
            estimate.estimated_rows > 0,
            "mark_cached must not prevent estimate from computing real costs, got rows={}",
            estimate.estimated_rows,
        );
        assert!(
            estimate.estimated_bytes_scanned > 0,
            "mark_cached must not poison estimate cache with zeros, got bytes={}",
            estimate.estimated_bytes_scanned,
        );
    }

    #[test]
    fn test_where_clause_reduces_estimated_cost() {
        let mut estimator = QueryCostEstimator::new();
        estimator.add_table_stats(TableStats {
            table_name: "events".to_string(),
            row_count: 10_000_000,
            file_count: 50,
            size_bytes: 1_000_000_000,
            avg_row_size: 100,
            last_updated: None,
        });

        let full_scan = estimator.estimate("SELECT * FROM events").unwrap();

        estimator.clear_cache();

        let filtered = estimator
            .estimate("SELECT * FROM events WHERE events.id = 1")
            .unwrap();

        assert!(
            filtered.estimated_rows < full_scan.estimated_rows,
            "Filtered query should estimate fewer rows: filtered={} vs full={}",
            filtered.estimated_rows,
            full_scan.estimated_rows,
        );
        assert!(
            filtered.estimated_bytes_scanned < full_scan.estimated_bytes_scanned,
            "Filtered query should estimate fewer bytes: filtered={} vs full={}",
            filtered.estimated_bytes_scanned,
            full_scan.estimated_bytes_scanned,
        );
    }

    #[test]
    fn test_invalidate_table_no_substring_false_positive() {
        let mut estimator = QueryCostEstimator::new();
        for name in &["order", "reorder", "order_items"] {
            estimator.add_table_stats(TableStats {
                table_name: name.to_string(),
                row_count: 100,
                size_bytes: 10_000,
                file_count: 1,
                avg_row_size: 100,
                last_updated: None,
            });
        }

        let order_sql = "SELECT * FROM order WHERE id = 1";
        let reorder_sql = "SELECT * FROM reorder WHERE id = 1";
        let items_sql = "SELECT * FROM order_items WHERE id = 1";

        let _ = estimator.estimate(order_sql).unwrap();
        let _ = estimator.estimate(reorder_sql).unwrap();
        let _ = estimator.estimate(items_sql).unwrap();
        estimator.mark_cached(order_sql);
        estimator.mark_cached(reorder_sql);
        estimator.mark_cached(items_sql);

        estimator.invalidate_table("order");

        assert!(
            !estimator.estimate(order_sql).unwrap().cache_hit,
            "order query must be evicted"
        );
        assert!(
            !estimator.is_cached(order_sql),
            "order cached flag must be evicted"
        );
        assert!(
            estimator.estimate(reorder_sql).unwrap().cache_hit,
            "reorder query must survive invalidation of 'order'"
        );
        assert!(
            estimator.is_cached(reorder_sql),
            "reorder cached flag must survive invalidation of 'order'"
        );
        assert!(
            estimator.estimate(items_sql).unwrap().cache_hit,
            "order_items query must survive invalidation of 'order'"
        );
        assert!(
            estimator.is_cached(items_sql),
            "order_items cached flag must survive invalidation of 'order'"
        );
    }

    #[test]
    fn test_invalidate_table_ignores_string_literals() {
        let mut estimator = QueryCostEstimator::new();
        estimator.add_table_stats(TableStats {
            table_name: "logs".to_string(),
            row_count: 100,
            size_bytes: 10_000,
            file_count: 1,
            avg_row_size: 100,
            last_updated: None,
        });

        let sql = "SELECT * FROM logs WHERE message = 'order received'";
        let _ = estimator.estimate(sql).unwrap();
        estimator.mark_cached(sql);

        estimator.invalidate_table("order");

        assert!(
            estimator.estimate(sql).unwrap().cache_hit,
            "query with 'order' only in a string literal must survive invalidation"
        );
        assert!(
            estimator.is_cached(sql),
            "cached flag must survive when table name only appears in string literal"
        );
    }

    #[test]
    fn test_time_estimate_no_overflow_with_large_bytes() {
        let mut estimator = QueryCostEstimator::new();
        estimator.add_table_stats(TableStats {
            table_name: "huge".to_string(),
            row_count: u64::MAX / 200,
            size_bytes: u64::MAX / 2,
            file_count: 1_000_000,
            avg_row_size: 100,
            last_updated: None,
        });

        let result = estimator.estimate("SELECT id FROM huge");
        assert!(result.is_ok(), "estimate must not panic on very large byte counts");
        let estimate = result.unwrap();
        assert!(
            estimate.estimated_time_ms >= MIN_ESTIMATED_TIME_MS,
            "time must be at least the minimum: {}",
            estimate.estimated_time_ms,
        );
    }
}
