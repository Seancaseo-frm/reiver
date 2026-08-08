//! AST-Based Table Rewriter
//!
//! Rewrites warehouse table references to ClickHouse s3() function calls.
//!
//! SECURITY: This module enforces project isolation by validating that all
//! referenced tables belong to the requesting project before rewriting queries.
//!
//! PERFORMANCE: Supports date-based partition pruning by extracting date
//! predicates from WHERE clauses and generating targeted file patterns.
//!
//! ARCHITECTURE: Uses a visitor pattern (`TableTransformer`) to allow different
//! rewrite strategies (basic, partition-pruned, skip-indexed) without duplicating
//! AST traversal logic.

use ahash::{AHashMap, AHashSet};
use chrono::NaiveDate;
use quick_cache::sync::Cache;
use quick_cache::Weighter;
use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Ident, ObjectName, Query, SetExpr, Statement, TableAlias, TableFactor,
    TableWithJoins, Value, WindowType,
};
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser;
use std::cell::RefCell;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

use std::sync::LazyLock;

use crate::warehouse::indexes::skip_index::{
    HierarchicalSkipIndex, SkipPredicates, EMPTY_MATCH_PATTERN,
};
use crate::warehouse::types::{
    coerce_types, CoercionResult, DateRange, R2TablePath, SemanticType, TypedColumn, TypedSchema,
};

// ── Constant identifiers ────────────────────────────────────────────────
// These are allocated once on first access and cloned cheaply when building
// AST nodes, avoiding repeated `Ident::new("...")` heap allocations in the
// hot query-rewrite path.

static IDENT_S3: LazyLock<Ident> = LazyLock::new(|| Ident::new("s3"));
static IDENT_FILENAME: LazyLock<Ident> = LazyLock::new(|| Ident::new("filename"));
static IDENT_FORMAT: LazyLock<Ident> = LazyLock::new(|| Ident::new("format"));
static IDENT_STRUCTURE: LazyLock<Ident> = LazyLock::new(|| Ident::new("structure"));
static VALUE_PARQUET: LazyLock<Value> =
    LazyLock::new(|| Value::SingleQuotedString("Parquet".to_string()));

/// Serialize parsed SQL statements back to a SQL string.
///
/// Fast path for the common single-statement case: returns `to_string()`
/// directly, skipping the `Vec` + `join` allocation that the multi-statement
/// path requires.
pub(crate) fn serialize_statements(statements: &[Statement]) -> String {
    match statements {
        [single] => single.to_string(),
        multiple => {
            let mut out = String::new();
            for (i, s) in multiple.iter().enumerate() {
                if i > 0 {
                    out.push_str("; ");
                }
                use std::fmt::Write;
                let _ = write!(out, "{}", s);
            }
            out
        }
    }
}

/// Maximum partition keys to generate before falling back to a full scan.
const MAX_PARTITION_KEYS: usize = 24;

/// Add (or subtract) a number of months to a date, clamping the day to the
/// last valid day of the target month (e.g. Jan 31 + 1 month = Feb 28).
fn add_months_to_date(base: NaiveDate, months: i64) -> Option<NaiveDate> {
    use chrono::Datelike;
    let total_months = (base.year() as i64)
        .checked_mul(12)?
        .checked_add(base.month() as i64 - 1)?
        .checked_add(months)?;
    let month = (total_months.rem_euclid(12) + 1) as u32;
    let new_year_i64 = if total_months >= 0 {
        total_months / 12
    } else {
        (total_months - 11) / 12
    };
    let new_year = i32::try_from(new_year_i64).ok()?;
    let max_day = (28..=31u32)
        .rev()
        .find_map(|day| NaiveDate::from_ymd_opt(new_year, month, day))?
        .day();
    let day = base.day().min(max_day);
    NaiveDate::from_ymd_opt(new_year, month, day)
}

/// Convert a [`DateRange`] to partition keys in `YYYY/MM` format.
///
/// Returns an empty `Vec` for impossible or fully open ranges (`None, None`).
///
/// For open-ended start ranges (`Some(start), None`), today's date is used
/// as the implicit upper bound so that partition pruning still works for
/// common patterns like `WHERE date >= '2024-01-01'`.
fn date_range_to_partition_keys(date_range: &DateRange) -> Vec<String> {
    use chrono::Datelike;

    if date_range.is_impossible() {
        return Vec::new();
    }

    let mut keys = Vec::with_capacity(MAX_PARTITION_KEYS);

    let enumerate_months = |keys: &mut Vec<String>, start: NaiveDate, end: NaiveDate| {
        let mut current = start;
        while current <= end {
            if keys.len() >= MAX_PARTITION_KEYS {
                tracing::warn!(
                    partition_count = keys.len(),
                    max = MAX_PARTITION_KEYS,
                    "Date range spans too many partitions; falling back to full scan"
                );
                keys.clear();
                return;
            }

            keys.push(format!("{:04}/{:02}", current.year(), current.month()));

            let (next_year, next_month) = if current.month() == 12 {
                match current.year().checked_add(1) {
                    Some(y) => (y, 1),
                    None => break,
                }
            } else {
                (current.year(), current.month() + 1)
            };

            match NaiveDate::from_ymd_opt(next_year, next_month, 1) {
                Some(next) => current = next,
                None => break,
            }
        }
    };

    match (date_range.start, date_range.end) {
        (Some(start), Some(end)) => {
            enumerate_months(&mut keys, start, end);
        }
        (Some(start), None) => {
            let today = chrono::Utc::now().date_naive();
            enumerate_months(&mut keys, start, today);
        }
        _ => {}
    }

    keys
}

/// Errors that can occur during SQL rewriting.
#[derive(Debug, Error)]
pub enum RewriteError {
    #[error("SQL parse error: {0}")]
    Parse(#[from] sqlparser::parser::ParserError),

    #[error("Unsupported SQL statement")]
    UnsupportedStatement,

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Access denied: table '{table}' does not belong to project '{project_id}'")]
    AccessDenied { table: String, project_id: Uuid },

    #[error("No tables provided for rewriting")]
    NoTablesProvided,

    #[error("Type coercion error: {message}\nHint: {suggestion}")]
    TypeCoercion {
        message: String,
        suggestion: String,
        left_column: String,
        right_column: String,
    },

    #[error("Type incompatible: {message}")]
    TypeIncompatible {
        message: String,
        left_column: String,
        right_column: String,
    },
}

/// Result type for rewrite operations.
pub type RewriteResult<T> = Result<T, RewriteError>;

/// Statistics about how many files were pruned during skip index filtering.
#[derive(Debug, Clone, Default)]
pub struct PruningStats {
    pub total_files: usize,
    pub files_after_pruning: usize,
}

/// Output from `rewrite_warm_query_ast`, bundling the rewritten SQL with
/// optional pruning statistics captured during skip index evaluation.
#[derive(Debug)]
pub struct RewriteOutput {
    pub sql: String,
    pub pruning_stats: Option<PruningStats>,
}

// ===== Query Plan Cache =====

/// Default capacity for the query plan cache.
const DEFAULT_QUERY_PLAN_CACHE_CAPACITY: usize = 1000;

/// Default maximum memory limit for the query plan cache (50 MB).
const DEFAULT_QUERY_PLAN_CACHE_MAX_MEMORY_BYTES: usize = 50 * 1024 * 1024;

/// Cached entry for a rewritten query.
///
/// PERFORMANCE: Caching rewritten queries avoids repeated SQL parsing and
/// AST traversal for identical dashboard queries.
#[derive(Clone, Debug)]
struct CachedQueryPlan {
    /// The rewritten SQL string
    rewritten_sql: Arc<String>,
    /// Generation at which this entry was cached
    generation: u64,
}

/// Cache key combining query hash and tables hash as compact u64 values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct QueryPlanCacheKey {
    query_hash: u64,
    tables_hash: u64,
}

/// Weighter for QueryPlanCache entries.
///
/// Estimates memory usage based on the size of the rewritten SQL.
#[derive(Clone, Default)]
struct QueryPlanWeighter;

impl Weighter<QueryPlanCacheKey, CachedQueryPlan> for QueryPlanWeighter {
    fn weight(&self, _key: &QueryPlanCacheKey, val: &CachedQueryPlan) -> u64 {
        // Key is 16 bytes (two u64), value has one String + u64 generation.
        let size = 16 + val.rewritten_sql.len() + 32; // key + String header + generation + padding
        size as u64
    }
}

// NOTE: CachedQueryPlan memory estimation is handled by QueryPlanWeighter (quick_cache).

/// Thread-safe LRU cache for rewritten SQL queries.
///
/// PERFORMANCE: For dashboard queries that run repeatedly, this cache
/// eliminates redundant SQL parsing and AST traversal overhead.
///
/// # Cache Key
///
/// The cache key is a combination of:
/// - Original SQL query hash
/// - Tables configuration hash (to detect changed R2 paths)
///
/// # Generation-Based Invalidation
///
/// The cache uses a generation counter to invalidate stale entries without
/// clearing the entire cache. When table data changes (e.g., after sync),
/// call `increment_generation()` to invalidate all cached queries.
///
/// This is similar to how `QueryCache` handles data freshness, ensuring that
/// cached query plans using old file patterns are not served after new data
/// is synced.
///
/// # Memory Management
///
/// The cache tracks estimated memory usage and enforces a configurable maximum
/// memory limit. When the limit is exceeded, LRU entries are evicted until
/// memory usage drops below the limit.
///
/// # Thread Safety
///
/// Uses `quick_cache` for high-performance concurrent caching with S3-FIFO eviction.
pub struct QueryPlanCache {
    cache: Cache<QueryPlanCacheKey, CachedQueryPlan, QueryPlanWeighter>,
    /// Current generation - entries with older generation are considered stale
    generation: std::sync::atomic::AtomicU64,
    hits: std::sync::atomic::AtomicU64,
    misses: std::sync::atomic::AtomicU64,
    /// Tracks invalidations for monitoring
    invalidations: std::sync::atomic::AtomicU64,
    /// Tracks total successful inserts for eviction estimation
    total_inserts: std::sync::atomic::AtomicU64,
    /// Tracks inserts that overwrite an existing key (not true evictions)
    total_overwrites: std::sync::atomic::AtomicU64,
    /// Maximum allowed memory usage in bytes (for stats reporting)
    max_memory_bytes: usize,
    /// Capacity for stats reporting
    capacity: usize,
}

impl QueryPlanCache {
    /// Create a new query plan cache with the specified capacity.
    pub fn new(capacity: usize) -> Self {
        Self::with_memory_limit(capacity, DEFAULT_QUERY_PLAN_CACHE_MAX_MEMORY_BYTES)
    }

    /// Create a new query plan cache with the specified capacity and memory limit.
    ///
    /// Uses weight-based eviction where weight = estimated bytes per entry.
    /// Capacity is automatically capped so that `max_memory / capacity` stays
    /// above the minimum entry weight quick_cache needs to accept inserts.
    pub fn with_memory_limit(capacity: usize, max_memory_bytes: usize) -> Self {
        let raw_capacity = if capacity == 0 {
            DEFAULT_QUERY_PLAN_CACHE_CAPACITY
        } else {
            capacity
        };
        // quick_cache silently drops entries whose weight exceeds max_weight/capacity.
        // A typical entry weighs ~100 bytes, so cap capacity accordingly.
        let max_by_weight = (max_memory_bytes / 100).max(1);
        let effective_capacity = raw_capacity.min(max_by_weight);

        Self {
            cache: Cache::with_weighter(
                effective_capacity,
                max_memory_bytes as u64,
                QueryPlanWeighter,
            ),
            generation: std::sync::atomic::AtomicU64::new(0),
            hits: std::sync::atomic::AtomicU64::new(0),
            misses: std::sync::atomic::AtomicU64::new(0),
            invalidations: std::sync::atomic::AtomicU64::new(0),
            total_inserts: std::sync::atomic::AtomicU64::new(0),
            total_overwrites: std::sync::atomic::AtomicU64::new(0),
            max_memory_bytes,
            capacity: effective_capacity,
        }
    }

    /// Create a new query plan cache with the default capacity.
    pub fn with_default_capacity() -> Self {
        Self::new(DEFAULT_QUERY_PLAN_CACHE_CAPACITY)
    }

    /// Get a cached rewritten query if available.
    ///
    /// Returns `Some(rewritten_sql)` if the query is in the cache
    /// and the entry is from the current generation.
    pub fn get(&self, sql: &str, tables: &AHashMap<String, R2TablePath>) -> Option<Arc<String>> {
        let key = QueryPlanCacheKey {
            query_hash: Self::hash_query_u64(sql),
            tables_hash: Self::hash_tables(tables),
        };
        let current_generation = self.generation.load(std::sync::atomic::Ordering::Acquire);

        if let Some(entry) = self.cache.get(&key) {
            if entry.generation == current_generation {
                self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Some(Arc::clone(&entry.rewritten_sql));
            }
        }

        self.misses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        None
    }

    /// Store a rewritten query in the cache.
    ///
    /// # Memory Management
    ///
    /// quick_cache automatically handles eviction based on weight (estimated bytes).
    /// Very large entries (>10% of max memory) are rejected to prevent cache thrashing.
    pub fn put(&self, sql: &str, tables: &AHashMap<String, R2TablePath>, rewritten_sql: String) {
        let tables_hash = Self::hash_tables(tables);
        let query_hash = Self::hash_query_u64(sql);
        let current_generation = self.generation.load(std::sync::atomic::Ordering::Acquire);

        let entry_size = rewritten_sql.len() + 48;

        let max_single_entry = self.max_memory_bytes / 10;

        if entry_size > max_single_entry {
            tracing::debug!(
                entry_size = entry_size,
                max_single_entry = max_single_entry,
                "Rejecting large query plan cache entry"
            );
            return;
        }

        let key = QueryPlanCacheKey {
            query_hash,
            tables_hash,
        };

        let entry = CachedQueryPlan {
            rewritten_sql: Arc::new(rewritten_sql),
            generation: current_generation,
        };

        let was_overwrite = self.cache.peek(&key).is_some();
        self.cache.insert(key, entry);
        self.total_inserts
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if was_overwrite {
            self.total_overwrites
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Increment the generation counter to invalidate all cached entries.
    ///
    /// Call this after sync operations complete to ensure queries use
    /// updated file patterns that include new data.
    ///
    /// PERFORMANCE: This is more efficient than `clear()` because it allows
    /// entries to be lazily evicted. Old entries are ignored on `get()` and
    /// will eventually be replaced by the S3-FIFO eviction policy.
    ///
    /// Returns the new generation number.
    pub fn increment_generation(&self) -> u64 {
        let new_gen = self
            .generation
            .fetch_add(1, std::sync::atomic::Ordering::Release)
            + 1;
        self.invalidations
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        tracing::debug!(
            new_generation = new_gen,
            "QueryPlanCache generation incremented - cached entries invalidated"
        );

        new_gen
    }

    /// Get the current generation number.
    pub fn current_generation(&self) -> u64 {
        self.generation.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Invalidate all cached queries.
    ///
    /// Call this when the table configuration changes significantly.
    /// For normal sync operations, prefer `increment_generation()` which is more efficient.
    pub fn clear(&self) {
        self.cache.clear();
        self.invalidations
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Get cache statistics.
    pub fn stats(&self) -> QueryPlanCacheStats {
        let current_len = self.cache.len();

        let total_ins = self
            .total_inserts
            .load(std::sync::atomic::Ordering::Relaxed);
        let total_ow = self
            .total_overwrites
            .load(std::sync::atomic::Ordering::Relaxed);
        let non_overwrite_inserts = total_ins.saturating_sub(total_ow);
        let memory_evictions = if non_overwrite_inserts > current_len as u64 {
            non_overwrite_inserts - current_len as u64
        } else {
            0
        };

        QueryPlanCacheStats {
            hits: self.hits.load(std::sync::atomic::Ordering::Relaxed),
            misses: self.misses.load(std::sync::atomic::Ordering::Relaxed),
            size: current_len,
            capacity: self.capacity,
            generation: self.generation.load(std::sync::atomic::Ordering::Relaxed),
            invalidations: self
                .invalidations
                .load(std::sync::atomic::Ordering::Relaxed),
            estimated_memory_bytes: self.cache.weight() as usize,
            max_memory_bytes: self.max_memory_bytes,
            memory_evictions,
        }
    }

    /// Get current estimated memory usage in bytes.
    pub fn estimated_memory(&self) -> usize {
        self.cache.weight() as usize
    }

    /// Get the maximum memory limit in bytes.
    pub fn max_memory(&self) -> usize {
        self.max_memory_bytes
    }

    /// Create a hash of the tables configuration.
    ///
    /// Includes all `R2TablePath` fields that affect query rewriting:
    /// prefix, date_partitioned, partition_column, and detected_partition_scheme.
    fn hash_query_u64(sql: &str) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = ahash::AHasher::default();
        sql.hash(&mut hasher);
        hasher.finish()
    }

    fn hash_tables(tables: &AHashMap<String, R2TablePath>) -> u64 {
        use std::hash::{Hash, Hasher};

        let mut hasher = ahash::AHasher::default();

        let mut keys: Vec<_> = tables.keys().collect();
        keys.sort_unstable();
        for k in keys {
            let v = &tables[k];
            k.hash(&mut hasher);
            v.prefix.hash(&mut hasher);
            v.date_partitioned.hash(&mut hasher);
            v.partition_column.hash(&mut hasher);
            v.detected_partition_scheme.hash(&mut hasher);
        }
        hasher.finish()
    }
}

/// Cache statistics for monitoring.
#[derive(Debug, Clone)]
pub struct QueryPlanCacheStats {
    /// Number of cache hits
    pub hits: u64,
    /// Number of cache misses
    pub misses: u64,
    /// Current cache size (number of entries)
    pub size: usize,
    /// Maximum cache capacity
    pub capacity: usize,
    /// Current generation number
    pub generation: u64,
    /// Number of times the cache was invalidated
    pub invalidations: u64,
    /// Estimated memory usage in bytes
    pub estimated_memory_bytes: usize,
    /// Maximum memory limit in bytes
    pub max_memory_bytes: usize,
    /// Number of entries lost (eviction, invalidation, or overwrite)
    pub memory_evictions: u64,
}

impl QueryPlanCacheStats {
    /// Calculate hit rate as a percentage.
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            (self.hits as f64 / total as f64) * 100.0
        }
    }

    /// Calculate memory usage as a percentage of max.
    pub fn memory_usage_percent(&self) -> f64 {
        if self.max_memory_bytes == 0 {
            0.0
        } else {
            (self.estimated_memory_bytes as f64 / self.max_memory_bytes as f64) * 100.0
        }
    }
}

/// Shared query plan cache wrapped in Arc for use across async contexts.
pub type SharedQueryPlanCache = Arc<QueryPlanCache>;

// ===== Visitor Pattern for Table Transformation =====

/// Trait for transforming table references in SQL AST.
///
/// This visitor pattern allows different rewrite strategies without duplicating
/// AST traversal logic. Implementations can provide different ways to transform
/// table names into s3() function calls.
pub trait TableTransformer {
    /// Transform a table name into an s3() function expression.
    ///
    /// Returns None if the table should not be transformed.
    fn transform_table(&self, table_name: &str) -> Option<Expr>;

    /// Return the ClickHouse buffer table name for hot (unflushed) data, if any.
    /// When `Some`, the query engine wraps `s3()` in a `UNION ALL` with this table.
    fn buffer_table(&self, _table_name: &str) -> Option<&str> {
        None
    }
}

/// Generic AST visitor that applies a TableTransformer to all table references.
///
/// Walks the AST and transforms table references into s3() function calls
/// using the provided `TableTransformer`.
pub struct AstVisitor<'a, T: TableTransformer> {
    transformer: &'a T,
    cte_names: RefCell<AHashSet<String>>,
}

impl<'a, T: TableTransformer> AstVisitor<'a, T> {
    pub fn new(transformer: &'a T) -> Self {
        Self {
            transformer,
            cte_names: RefCell::new(AHashSet::new()),
        }
    }

    /// Build `SELECT * FROM <factor>` as a `SetExpr::Select`.
    fn make_star_select(&self, from_factor: TableFactor) -> SetExpr {
        SetExpr::Select(Box::new(sqlparser::ast::Select {
            distinct: None,
            top: None,
            top_before_distinct: false,
            projection: vec![sqlparser::ast::SelectItem::Wildcard(
                sqlparser::ast::WildcardAdditionalOptions {
                    opt_ilike: None,
                    opt_exclude: None,
                    opt_except: None,
                    opt_rename: None,
                    opt_replace: None,
                },
            )],
            into: None,
            from: vec![TableWithJoins {
                relation: from_factor,
                joins: vec![],
            }],
            lateral_views: vec![],
            prewhere: None,
            selection: None,
            group_by: sqlparser::ast::GroupByExpr::Expressions(vec![], vec![]),
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            named_window: vec![],
            qualify: None,
            window_before_qualify: false,
            value_table_mode: None,
            connect_by: None,
        }))
    }

    /// Visit and transform a statement in place.
    pub fn visit_statement(&self, stmt: &mut Statement) {
        if let Statement::Query(query) = stmt {
            self.visit_query(query);
        }
    }

    /// Visit and transform a query in place.
    ///
    /// CTE alias names are collected first so that references to them in the
    /// main body are not mistakenly rewritten into s3() calls.  Each CTE's
    /// own body is still visited (it may reference real tables).
    pub fn visit_query(&self, query: &mut Query) {
        let mut added_cte_names: Vec<String> = Vec::with_capacity(4);

        if let Some(with) = &mut query.with {
            let is_recursive = with.recursive;
            for cte in &mut with.cte_tables {
                let name = cte.alias.name.value.clone();

                if is_recursive {
                    // For WITH RECURSIVE, register the CTE name *before*
                    // visiting its body so that recursive self-references
                    // are not mistakenly rewritten into s3() calls.
                    if self.cte_names.borrow_mut().insert(name.clone()) {
                        added_cte_names.push(name);
                    }
                    self.visit_query(&mut cte.query);
                } else {
                    // For non-recursive CTEs, visit the body first — references
                    // to a table with the same name as the CTE refer to the
                    // real table (the CTE name isn't defined yet during its
                    // own body evaluation).
                    self.visit_query(&mut cte.query);
                    if self.cte_names.borrow_mut().insert(name.clone()) {
                        added_cte_names.push(name);
                    }
                }
            }
        }

        self.visit_set_expr(&mut query.body);

        if let Some(ref mut order_by) = query.order_by {
            for item in &mut order_by.exprs {
                self.visit_expr(&mut item.expr);
            }
        }

        // Remove CTE names we added so they don't leak into sibling scopes
        let mut names = self.cte_names.borrow_mut();
        for name in added_cte_names {
            names.remove(&name);
        }
    }

    /// Visit and transform a set expression in place, including subqueries
    /// in WHERE, HAVING, and SELECT clauses.
    pub fn visit_set_expr(&self, set_expr: &mut SetExpr) {
        match set_expr {
            SetExpr::Select(select) => {
                for table_with_joins in &mut select.from {
                    self.visit_table_with_joins(table_with_joins);
                }
                if let Some(ref mut selection) = select.selection {
                    self.visit_expr(selection);
                }
                if let Some(ref mut having) = select.having {
                    self.visit_expr(having);
                }
                if let sqlparser::ast::GroupByExpr::Expressions(ref mut exprs, _) = select.group_by
                {
                    for expr in exprs {
                        self.visit_expr(expr);
                    }
                }
                for item in &mut select.projection {
                    if let sqlparser::ast::SelectItem::UnnamedExpr(ref mut expr)
                    | sqlparser::ast::SelectItem::ExprWithAlias { ref mut expr, .. } = item
                    {
                        self.visit_expr(expr);
                    }
                }
            }
            SetExpr::Query(query) => self.visit_query(query),
            SetExpr::SetOperation { left, right, .. } => {
                self.visit_set_expr(left);
                self.visit_set_expr(right);
            }
            _ => {}
        }
    }

    /// Recurse into an expression to rewrite table references in subqueries.
    fn visit_expr(&self, expr: &mut Expr) {
        match expr {
            Expr::Subquery(query) => self.visit_query(query),
            Expr::InSubquery {
                subquery,
                expr: inner_expr,
                ..
            } => {
                self.visit_query(subquery);
                self.visit_expr(inner_expr);
            }
            Expr::Exists { subquery, .. } => self.visit_query(subquery),
            Expr::BinaryOp { left, right, .. } => {
                self.visit_expr(left);
                self.visit_expr(right);
            }
            Expr::UnaryOp { expr: inner, .. } => self.visit_expr(inner),
            Expr::Nested(inner) => self.visit_expr(inner),
            Expr::Between {
                expr: inner,
                low,
                high,
                ..
            } => {
                self.visit_expr(inner);
                self.visit_expr(low);
                self.visit_expr(high);
            }
            Expr::IsNull(inner) | Expr::IsNotNull(inner) => self.visit_expr(inner),
            Expr::InList {
                expr: inner, list, ..
            } => {
                self.visit_expr(inner);
                for item in list {
                    self.visit_expr(item);
                }
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
                ..
            } => {
                if let Some(op) = operand {
                    self.visit_expr(op);
                }
                for cond in conditions {
                    self.visit_expr(cond);
                }
                for res in results {
                    self.visit_expr(res);
                }
                if let Some(el) = else_result {
                    self.visit_expr(el);
                }
            }
            Expr::Cast { expr: inner, .. } => self.visit_expr(inner),
            Expr::Function(func) => {
                if let FunctionArguments::List(ref mut list) = func.args {
                    for arg in &mut list.args {
                        let expr = match arg {
                            sqlparser::ast::FunctionArg::Unnamed(
                                sqlparser::ast::FunctionArgExpr::Expr(ref mut e),
                            ) => Some(e),
                            sqlparser::ast::FunctionArg::Named {
                                arg: sqlparser::ast::FunctionArgExpr::Expr(ref mut e),
                                ..
                            } => Some(e),
                            _ => None,
                        };
                        if let Some(e) = expr {
                            self.visit_expr(e);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    /// Visit and transform a table with joins in place.
    pub fn visit_table_with_joins(&self, table: &mut TableWithJoins) {
        self.visit_table_factor(&mut table.relation);
        for join in &mut table.joins {
            self.visit_table_factor(&mut join.relation);
            match &mut join.join_operator {
                sqlparser::ast::JoinOperator::Inner(c)
                | sqlparser::ast::JoinOperator::LeftOuter(c)
                | sqlparser::ast::JoinOperator::RightOuter(c)
                | sqlparser::ast::JoinOperator::FullOuter(c)
                | sqlparser::ast::JoinOperator::LeftSemi(c)
                | sqlparser::ast::JoinOperator::RightSemi(c)
                | sqlparser::ast::JoinOperator::LeftAnti(c)
                | sqlparser::ast::JoinOperator::RightAnti(c) => {
                    if let sqlparser::ast::JoinConstraint::On(ref mut expr) = c {
                        self.visit_expr(expr);
                    }
                }
                _ => {}
            }
        }
    }

    /// Visit and transform a table factor in place.
    ///
    /// Replaces `TableFactor::Table` with a `TableFactor::Function` pointing to
    /// the ClickHouse `s3()` function produced by the transformer. The original
    /// table alias (if any) is preserved on the function call.
    pub fn visit_table_factor(&self, factor: &mut TableFactor) {
        match factor {
            TableFactor::Table { name, alias, .. } => {
                let full_name = name.to_string();
                let short_name = name.0.last().map(|i| i.value.clone()).unwrap_or_default();

                // Skip transformation for CTE references
                {
                    let cte_set = self.cte_names.borrow();
                    if cte_set.contains(&full_name) || cte_set.contains(&short_name) {
                        return;
                    }
                }

                let lookup_name = full_name.clone();
                let s3_expr = match self.transformer.transform_table(&full_name).or_else(|| {
                    if short_name != full_name {
                        self.transformer.transform_table(&short_name)
                    } else {
                        None
                    }
                }) {
                    Some(expr) => expr,
                    None => return,
                };
                if let Expr::Function(func) = s3_expr {
                    let args = match func.args {
                        FunctionArguments::List(list) => list.args,
                        _ => Vec::new(),
                    };

                    let effective_alias = alias.take().or_else(|| {
                        name.0.last().map(|ident| TableAlias {
                            name: ident.clone(),
                            columns: vec![],
                        })
                    });

                    let buffer = self
                        .transformer
                        .buffer_table(&lookup_name)
                        .or_else(|| {
                            if short_name != lookup_name {
                                self.transformer.buffer_table(&short_name)
                            } else {
                                None
                            }
                        });

                    if let Some(buf_table) = buffer {
                        // Wrap in UNION ALL: (SELECT * FROM s3(...) UNION ALL SELECT * FROM buf) AS alias
                        let s3_factor = TableFactor::Function {
                            lateral: false,
                            name: func.name,
                            args,
                            alias: None,
                        };

                        let s3_select = self.make_star_select(s3_factor);
                        let buf_select = self.make_star_select(TableFactor::Table {
                            name: ObjectName(vec![Ident::with_quote('`', buf_table)]),
                            alias: None,
                            args: None,
                            with_hints: vec![],
                            version: None,
                            partitions: vec![],
                            with_ordinality: false,
                        });

                        let union_query = Query {
                            with: None,
                            body: Box::new(SetExpr::SetOperation {
                                op: sqlparser::ast::SetOperator::Union,
                                set_quantifier: sqlparser::ast::SetQuantifier::All,
                                left: Box::new(s3_select),
                                right: Box::new(buf_select),
                            }),
                            order_by: None,
                            limit: None,
                            limit_by: vec![],
                            offset: None,
                            fetch: None,
                            locks: vec![],
                            for_clause: None,
                            settings: None,
                            format_clause: None,
                        };

                        *factor = TableFactor::Derived {
                            lateral: false,
                            subquery: Box::new(union_query),
                            alias: effective_alias,
                        };
                    } else {
                        *factor = TableFactor::Function {
                            lateral: false,
                            name: func.name,
                            args,
                            alias: effective_alias,
                        };
                    }
                }
            }
            TableFactor::Derived { subquery, .. } => {
                self.visit_query(subquery);
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                self.visit_table_with_joins(table_with_joins);
            }
            _ => {}
        }
    }
}

/// S3 configuration for building s3() function calls using ClickHouse named collections.
///
/// Borrows `collection_name` to avoid per-transformer String clones.
#[derive(Clone, Copy)]
pub struct S3Config<'a> {
    /// Named collection name configured in ClickHouse
    pub collection_name: &'a str,
}

impl<'a> S3Config<'a> {
    /// Build an s3() function call for the given file pattern using named collection.
    ///
    /// Accepts an owned `String` for `file_pattern` so callers (who typically
    /// already have an owned `String` from `format!()`) can move it in without
    /// a redundant `.to_string()` copy.
    pub fn build_s3_function(&self, file_pattern: String) -> Expr {
        // Named collection mode: s3(collection, filename='path/*.parquet', format='Parquet')
        let args = vec![
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(Ident::new(
                self.collection_name,
            )))),
            FunctionArg::Named {
                name: IDENT_FILENAME.clone(),
                arg: FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(file_pattern))),
                operator: sqlparser::ast::FunctionArgOperator::Equals,
            },
            FunctionArg::Named {
                name: IDENT_FORMAT.clone(),
                arg: FunctionArgExpr::Expr(Expr::Value(VALUE_PARQUET.clone())),
                operator: sqlparser::ast::FunctionArgOperator::Equals,
            },
        ];

        Expr::Function(Function {
            name: ObjectName(vec![IDENT_S3.clone()]),
            args: FunctionArguments::List(FunctionArgumentList {
                args,
                duplicate_treatment: None,
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
            parameters: FunctionArguments::None,
        })
    }
}

/// Basic table transformer that maps table names to s3() calls.
pub struct BasicTableTransformer<'a> {
    s3_config: S3Config<'a>,
    tables: &'a AHashMap<String, R2TablePath>,
}

impl<'a> BasicTableTransformer<'a> {
    pub fn new(collection_name: &'a str, tables: &'a AHashMap<String, R2TablePath>) -> Self {
        Self {
            s3_config: S3Config { collection_name },
            tables,
        }
    }

    pub fn with_s3_config(
        s3_config: S3Config<'a>,
        tables: &'a AHashMap<String, R2TablePath>,
    ) -> Self {
        Self { s3_config, tables }
    }

    fn build_s3_function(&self, r2_path: &R2TablePath) -> Expr {
        let filename = format!("{}/*.parquet", r2_path.prefix);
        self.s3_config.build_s3_function(filename)
    }
}

impl<'a> TableTransformer for BasicTableTransformer<'a> {
    fn transform_table(&self, table_name: &str) -> Option<Expr> {
        self.tables
            .get(table_name)
            .map(|r2_path| self.build_s3_function(r2_path))
    }

    fn buffer_table(&self, table_name: &str) -> Option<&str> {
        self.tables
            .get(table_name)
            .and_then(|r2| r2.buffer_ch_table.as_deref())
    }
}

/// Partition-pruning table transformer that uses date predicates.
pub struct PartitionPruningTransformer<'a> {
    s3_config: S3Config<'a>,
    tables: &'a AHashMap<String, R2TablePath>,
    date_predicates: &'a AHashMap<String, DateRange>,
}

impl<'a> PartitionPruningTransformer<'a> {
    pub fn new(
        collection_name: &'a str,
        tables: &'a AHashMap<String, R2TablePath>,
        date_predicates: &'a AHashMap<String, DateRange>,
    ) -> Self {
        Self {
            s3_config: S3Config { collection_name },
            tables,
            date_predicates,
        }
    }

    pub fn with_s3_config(
        s3_config: S3Config<'a>,
        tables: &'a AHashMap<String, R2TablePath>,
        date_predicates: &'a AHashMap<String, DateRange>,
    ) -> Self {
        Self {
            s3_config,
            tables,
            date_predicates,
        }
    }

    fn build_s3_function_with_pattern(&self, file_pattern: String) -> Expr {
        self.s3_config.build_s3_function(file_pattern)
    }
}

impl<'a> TableTransformer for PartitionPruningTransformer<'a> {
    fn transform_table(&self, table_name: &str) -> Option<Expr> {
        self.tables.get(table_name).map(|r2_path| {
            let file_pattern = if r2_path.date_partitioned {
                if let Some(partition_col) = &r2_path.partition_column {
                    if let Some(date_range) = self.date_predicates.get(partition_col) {
                        if date_range.is_impossible() {
                            return self.build_s3_function_with_pattern(format!(
                                "{}/__dh_no_match__",
                                r2_path.prefix
                            ));
                        }
                        r2_path.file_pattern(Some(date_range))
                    } else {
                        r2_path.file_pattern(None)
                    }
                } else {
                    r2_path.file_pattern(None)
                }
            } else {
                r2_path.file_pattern(None)
            };

            self.build_s3_function_with_pattern(file_pattern)
        })
    }

    fn buffer_table(&self, table_name: &str) -> Option<&str> {
        self.tables
            .get(table_name)
            .and_then(|r2| r2.buffer_ch_table.as_deref())
    }
}

/// Schema information for a table (used for column pruning).
#[derive(Debug, Clone, Default)]
pub struct TableSchema {
    /// Map of column_name -> ClickHouse type string
    pub columns: AHashMap<String, String>,
}

impl TableSchema {
    /// Create a new table schema.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a column to the schema.
    pub fn add_column(&mut self, name: impl Into<String>, clickhouse_type: impl Into<String>) {
        self.columns.insert(name.into(), clickhouse_type.into());
    }

    /// Get column types for a subset of columns.
    /// Returns only the columns that exist in this schema.
    pub fn get_column_types(&self, column_names: &[String]) -> AHashMap<String, String> {
        column_names
            .iter()
            .filter_map(|name| {
                self.columns
                    .get(name)
                    .map(|typ| (name.clone(), typ.clone()))
            })
            .collect()
    }
}

/// Column-pruning table transformer that generates structure hints.
///
/// PERFORMANCE: When column types are provided in the s3() function call,
/// ClickHouse can skip reading unnecessary columns from Parquet files,
/// significantly reducing I/O for queries that only need a few columns.
pub struct ColumnPruningTransformer<'a> {
    collection_name: &'a str,
    tables: &'a AHashMap<String, R2TablePath>,
    /// Schema for each table
    schemas: &'a AHashMap<String, TableSchema>,
    /// Columns referenced in the query (per table)
    referenced_columns: &'a AHashMap<String, Vec<String>>,
    /// Date predicates for partition pruning
    date_predicates: &'a AHashMap<String, DateRange>,
}

impl<'a> ColumnPruningTransformer<'a> {
    /// Create a new column pruning transformer.
    pub fn new(
        collection_name: &'a str,
        tables: &'a AHashMap<String, R2TablePath>,
        schemas: &'a AHashMap<String, TableSchema>,
        referenced_columns: &'a AHashMap<String, Vec<String>>,
        date_predicates: &'a AHashMap<String, DateRange>,
    ) -> Self {
        Self {
            collection_name,
            tables,
            schemas,
            referenced_columns,
            date_predicates,
        }
    }

    fn build_s3_function_with_structure(
        &self,
        file_pattern: String,
        column_types: Option<&AHashMap<String, String>>,
    ) -> Expr {
        let mut args = vec![
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(Ident::new(
                self.collection_name,
            )))),
            FunctionArg::Named {
                name: IDENT_FILENAME.clone(),
                arg: FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(file_pattern))),
                operator: sqlparser::ast::FunctionArgOperator::Equals,
            },
            FunctionArg::Named {
                name: IDENT_FORMAT.clone(),
                arg: FunctionArgExpr::Expr(Expr::Value(VALUE_PARQUET.clone())),
                operator: sqlparser::ast::FunctionArgOperator::Equals,
            },
        ];

        // Add structure hint if column types are provided
        if let Some(types) = column_types {
            if !types.is_empty() {
                // Sort columns for deterministic output
                let mut cols: Vec<_> = types.iter().collect();
                cols.sort_by_key(|(name, _)| *name);

                let structure: String = cols
                    .iter()
                    .map(|(name, typ)| format!("`{}` {}", name, typ))
                    .collect::<Vec<_>>()
                    .join(", ");

                args.push(FunctionArg::Named {
                    name: IDENT_STRUCTURE.clone(),
                    arg: FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(structure))),
                    operator: sqlparser::ast::FunctionArgOperator::Equals,
                });
            }
        }

        Expr::Function(Function {
            name: ObjectName(vec![IDENT_S3.clone()]),
            args: FunctionArguments::List(FunctionArgumentList {
                args,
                duplicate_treatment: None,
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
            parameters: FunctionArguments::None,
        })
    }
}

impl<'a> TableTransformer for ColumnPruningTransformer<'a> {
    fn transform_table(&self, table_name: &str) -> Option<Expr> {
        self.tables.get(table_name).map(|r2_path| {
            // Get file pattern with date partition pruning
            let file_pattern = if r2_path.date_partitioned {
                if let Some(partition_col) = &r2_path.partition_column {
                    if let Some(date_range) = self.date_predicates.get(partition_col) {
                        if date_range.is_impossible() {
                            return self.build_s3_function_with_structure(
                                format!("{}/__dh_no_match__", r2_path.prefix),
                                None,
                            );
                        }
                        r2_path.file_pattern(Some(date_range))
                    } else {
                        r2_path.file_pattern(None)
                    }
                } else {
                    r2_path.file_pattern(None)
                }
            } else {
                r2_path.file_pattern(None)
            };

            // Get column types for referenced columns
            let column_types = if let Some(schema) = self.schemas.get(table_name) {
                // Get columns referenced for this specific table
                let table_columns = self.referenced_columns.get(table_name);
                // Also get unqualified columns (might belong to any table)
                let unqualified_columns = self.referenced_columns.get("");

                let mut all_columns: Vec<String> = Vec::with_capacity(16);
                if let Some(cols) = table_columns {
                    all_columns.extend(cols.iter().cloned());
                }
                if let Some(cols) = unqualified_columns {
                    all_columns.extend(cols.iter().cloned());
                }

                // Only include columns that exist in the schema
                let types = schema.get_column_types(&all_columns);
                if types.is_empty() {
                    None
                } else {
                    Some(types)
                }
            } else {
                None
            };

            self.build_s3_function_with_structure(file_pattern, column_types.as_ref())
        })
    }

    fn buffer_table(&self, table_name: &str) -> Option<&str> {
        self.tables
            .get(table_name)
            .and_then(|r2| r2.buffer_ch_table.as_deref())
    }
}

/// Hierarchical skip index transformer for TB-scale datasets.
///
/// PERFORMANCE: Uses partition-aware filtering to generate highly selective
/// file patterns. This reduces search from O(files) to O(partitions_matching).
pub struct HierarchicalSkipIndexTransformer<'a> {
    collection_name: &'a str,
    tables: &'a AHashMap<String, R2TablePath>,
    hierarchical_indexes: &'a AHashMap<String, HierarchicalSkipIndex>,
    date_predicates: &'a AHashMap<String, DateRange>,
    skip_predicates: &'a SkipPredicates,
    sidecar_cache: Option<&'a crate::warehouse::indexes::sidecar_stats_cache::SidecarStatsCache>,
    pruning_stats: RefCell<Option<PruningStats>>,
}

impl<'a> HierarchicalSkipIndexTransformer<'a> {
    /// Create a new hierarchical skip index transformer.
    pub fn new(
        collection_name: &'a str,
        tables: &'a AHashMap<String, R2TablePath>,
        hierarchical_indexes: &'a AHashMap<String, HierarchicalSkipIndex>,
        date_predicates: &'a AHashMap<String, DateRange>,
        skip_predicates: &'a SkipPredicates,
    ) -> Self {
        Self {
            collection_name,
            tables,
            hierarchical_indexes,
            date_predicates,
            skip_predicates,
            sidecar_cache: None,
            pruning_stats: RefCell::new(None),
        }
    }

    /// Extract captured pruning statistics after the visitor has run.
    pub fn take_pruning_stats(&self) -> Option<PruningStats> {
        self.pruning_stats.borrow_mut().take()
    }

    /// Attach a sidecar stats cache for min/max-based file pruning.
    pub fn with_sidecar_cache(
        mut self,
        cache: &'a crate::warehouse::indexes::sidecar_stats_cache::SidecarStatsCache,
    ) -> Self {
        self.sidecar_cache = Some(cache);
        self
    }

    fn build_s3_function_with_pattern(&self, file_pattern: String) -> Expr {
        Expr::Function(Function {
            name: ObjectName(vec![IDENT_S3.clone()]),
            args: FunctionArguments::List(FunctionArgumentList {
                args: vec![
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(Ident::new(
                        self.collection_name,
                    )))),
                    FunctionArg::Named {
                        name: IDENT_FILENAME.clone(),
                        arg: FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(
                            file_pattern,
                        ))),
                        operator: sqlparser::ast::FunctionArgOperator::Equals,
                    },
                    FunctionArg::Named {
                        name: IDENT_FORMAT.clone(),
                        arg: FunctionArgExpr::Expr(Expr::Value(VALUE_PARQUET.clone())),
                        operator: sqlparser::ast::FunctionArgOperator::Equals,
                    },
                ],
                duplicate_treatment: None,
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
            parameters: FunctionArguments::None,
        })
    }

    /// Build an optimized file pattern using hierarchical skip index.
    fn build_hierarchical_file_pattern(
        &self,
        _table_name: &str,
        r2_path: &R2TablePath,
        hierarchical_index: Option<&HierarchicalSkipIndex>,
    ) -> String {
        if self.skip_predicates.contradicted {
            return EMPTY_MATCH_PATTERN.to_string();
        }

        // Convert date predicates to partition hints.
        // Priority: explicit date partitioning > detected partition scheme.
        let partition_hints: Option<Vec<String>> = if r2_path.date_partitioned {
            if let Some(partition_col) = &r2_path.partition_column {
                if let Some(date_range) = self.date_predicates.get(partition_col) {
                    let keys = date_range_to_partition_keys(date_range);
                    if keys.is_empty() {
                        None
                    } else {
                        Some(keys)
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            // Try hints from the detected partition scheme.
            self.partition_hints_from_detected_scheme(r2_path)
        };

        if let Some(index) = hierarchical_index {
            let hints_ref: Option<Vec<&str>> = partition_hints
                .as_ref()
                .map(|v| v.iter().map(|s| s.as_str()).collect());
            let hints_slice: Option<&[&str]> = hints_ref.as_ref().map(|v| v.as_slice());

            let total_files = index.total_files();

            if let Some(sidecar_cache) = self.sidecar_cache {
                use crate::warehouse::indexes::sidecar_stats_cache::filter_files_by_cached_stats;

                let matching_files =
                    index.filter_with_skip_predicates(self.skip_predicates, hints_slice);
                let filtered = filter_files_by_cached_stats(
                    matching_files,
                    self.skip_predicates,
                    sidecar_cache,
                );
                *self.pruning_stats.borrow_mut() = Some(PruningStats {
                    total_files,
                    files_after_pruning: filtered.len(),
                });
                return index.format_file_pattern(&r2_path.prefix, &filtered, hints_slice);
            }

            let matching_files =
                index.filter_with_skip_predicates(self.skip_predicates, hints_slice);
            let files_after_pruning = matching_files.len();
            *self.pruning_stats.borrow_mut() = Some(PruningStats {
                total_files,
                files_after_pruning,
            });
            return index.format_file_pattern(&r2_path.prefix, &matching_files, hints_slice);
        }

        // Fallback to date-based partition pruning
        if r2_path.date_partitioned {
            if let Some(partition_col) = &r2_path.partition_column {
                if let Some(date_range) = self.date_predicates.get(partition_col) {
                    if date_range.is_impossible() {
                        return EMPTY_MATCH_PATTERN.to_string();
                    }
                    return r2_path.file_pattern(Some(date_range));
                }
            }
        }

        // Default: scan all files
        r2_path.file_pattern(None)
    }

    /// Generate partition hints from a detected `PartitionStrategy`.
    ///
    /// For Hive-style strategies, checks if query equality predicates match
    /// the Hive partition columns and generates corresponding partition keys.
    /// For timestamp-bucket strategies, translates date predicates into
    /// `YYYY/MM` keys.
    fn partition_hints_from_detected_scheme(&self, r2_path: &R2TablePath) -> Option<Vec<String>> {
        use crate::warehouse::indexes::external_config::PartitionStrategy;

        let strategy = r2_path.detected_partition_scheme.as_ref()?;

        match strategy {
            PartitionStrategy::HiveStyle { columns, .. } => {
                // Build partition keys only when ALL Hive columns have equality
                // predicates. A partial key like "year=2024" won't match the
                // full AHashMap key "year=2024/month=01", causing zero results.
                let mut parts: Vec<String> = Vec::with_capacity(columns.len());
                for col in columns {
                    if let Some(val) = self.skip_predicates.equality.get(col.as_str()) {
                        parts.push(format!("{}={}", col, val));
                    } else {
                        return None;
                    }
                }
                if parts.is_empty() {
                    None
                } else {
                    Some(vec![parts.join("/")])
                }
            }
            PartitionStrategy::TimestampBucket { column, .. } => {
                // Use date predicates to derive YYYY/MM keys.
                if let Some(date_range) = self.date_predicates.get(column.as_str()) {
                    let keys = date_range_to_partition_keys(date_range);
                    if keys.is_empty() {
                        None
                    } else {
                        Some(keys)
                    }
                } else {
                    None
                }
            }
            PartitionStrategy::HashBucket { .. } | PartitionStrategy::Flat => {
                // No useful hints — the index will scan all partitions.
                None
            }
        }
    }
}

impl<'a> TableTransformer for HierarchicalSkipIndexTransformer<'a> {
    fn transform_table(&self, table_name: &str) -> Option<Expr> {
        self.tables.get(table_name).map(|r2_path| {
            let file_pattern = self.build_hierarchical_file_pattern(
                table_name,
                r2_path,
                self.hierarchical_indexes.get(table_name),
            );
            if file_pattern.is_empty() {
                return self
                    .build_s3_function_with_pattern(format!("{}/__dh_no_match__", r2_path.prefix));
            }
            self.build_s3_function_with_pattern(file_pattern)
        })
    }

    fn buffer_table(&self, table_name: &str) -> Option<&str> {
        self.tables
            .get(table_name)
            .and_then(|r2| r2.buffer_ch_table.as_deref())
    }
}

/// Pre-computed AST analysis for `rewrite_with_column_pruning_ast`, collected
/// in a single iteration over statements to avoid 4 separate traversals.
struct ColumnPruningAnalysis {
    select_columns: AHashMap<String, Vec<String>>,
    has_wildcard: bool,
    table_aliases: AHashMap<String, String>,
    date_predicates: AHashMap<String, DateRange>,
}

/// Pre-computed AST analysis for `rewrite_with_hierarchical_optimization_ast`,
/// collected in a single iteration to avoid 2 separate traversals.
struct HierarchicalAnalysis {
    date_predicates: AHashMap<String, DateRange>,
    skip_predicates: SkipPredicates,
}

/// Rewrites table names to s3() function calls using ClickHouse named collections.
///
/// SECURITY: Uses ClickHouse named collections to avoid embedding credentials
/// in SQL queries. The named collection must be configured on the ClickHouse server.
pub struct TableRewriter {
    /// Name of the ClickHouse named collection containing R2 credentials
    collection_name: String,
    /// Optional query plan cache for avoiding repeated SQL parsing
    cache: Option<SharedQueryPlanCache>,
}

impl TableRewriter {
    pub fn new(collection_name: impl Into<String>) -> Self {
        Self {
            collection_name: collection_name.into(),
            cache: None,
        }
    }

    /// Create a rewriter by deriving the collection name from the bucket name.
    pub fn from_r2_bucket(r2_bucket: &str) -> Self {
        let mut collection_name = String::with_capacity(r2_bucket.len() + 3);
        collection_name.push_str("r2_");
        for ch in r2_bucket.chars() {
            collection_name.push(if ch == '-' { '_' } else { ch });
        }
        Self {
            collection_name,
            cache: None,
        }
    }

    /// Enable query plan caching with a shared cache.
    ///
    /// PERFORMANCE: For dashboard queries that run repeatedly, enabling caching
    /// eliminates redundant SQL parsing and AST traversal overhead.
    ///
    /// # Example
    /// ```ignore
    /// let cache = Arc::new(QueryPlanCache::with_default_capacity());
    /// let rewriter = TableRewriter::new("collection")
    ///     .with_cache(cache);
    /// ```
    pub fn with_cache(mut self, cache: SharedQueryPlanCache) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Get cache statistics if caching is enabled.
    pub fn cache_stats(&self) -> Option<QueryPlanCacheStats> {
        self.cache.as_ref().map(|c| c.stats())
    }

    /// Create an S3Config borrowing this rewriter's collection name.
    fn s3_config(&self) -> S3Config<'_> {
        S3Config {
            collection_name: &self.collection_name,
        }
    }

    /// Rewrite table names to s3() function calls using AST manipulation.
    ///
    /// Uses the `BasicTableTransformer` with `AstVisitor` to avoid duplicating
    /// AST traversal logic.
    ///
    /// PERFORMANCE: If a cache is enabled via `with_cache()`, this method will
    /// check the cache first and avoid parsing/rewriting for repeated queries.
    ///
    /// # Example
    /// Input:  `SELECT * FROM stripe_customers WHERE created > '2025-01-01'`
    /// Output: `SELECT * FROM s3(r2_collection, filename='stripe/customers/*.parquet', format='Parquet') WHERE created > '2025-01-01'`
    #[tracing::instrument(name = "warehouse.query.rewrite.rewrite", skip_all, err(Display))]
    pub fn rewrite(
        &self,
        sql: &str,
        tables: &AHashMap<String, R2TablePath>,
    ) -> RewriteResult<String> {
        // Check cache first if enabled
        if let Some(cache) = &self.cache {
            if let Some(cached) = cache.get(sql, tables) {
                return Ok((*cached).clone());
            }
        }

        let dialect = ClickHouseDialect {};
        let mut statements = Parser::parse_sql(&dialect, sql)?;

        // Use the visitor pattern to avoid duplicating AST traversal logic
        let transformer = BasicTableTransformer::with_s3_config(self.s3_config(), tables);
        let visitor = AstVisitor::new(&transformer);

        for statement in &mut statements {
            visitor.visit_statement(statement);
        }

        // Regenerate SQL from modified AST
        let result = serialize_statements(&statements);

        // Store in cache if enabled
        if let Some(cache) = &self.cache {
            cache.put(sql, tables, result.clone());
        }

        Ok(result)
    }

    /// Rewrite table names with project isolation validation.
    ///
    /// SECURITY: This method validates that all referenced tables belong to the
    /// specified project before rewriting. This prevents cross-project data access.
    ///
    /// # Arguments
    /// * `sql` - The SQL query to rewrite
    /// * `tables` - Map of table names to R2 paths (must include project_id in paths)
    /// * `project_id` - The project ID to validate against
    ///
    /// # Returns
    /// The rewritten SQL query, or an error if validation fails.
    #[tracing::instrument(
        name = "warehouse.query.rewrite.rewrite_with_validation",
        skip_all,
        err(Display)
    )]
    pub fn rewrite_with_validation(
        &self,
        sql: &str,
        tables: &AHashMap<String, R2TablePath>,
        project_id: Uuid,
    ) -> RewriteResult<String> {
        let dialect = ClickHouseDialect {};
        let statements = Parser::parse_sql(&dialect, sql)?;

        for stmt in &statements {
            if !matches!(stmt, Statement::Query(_)) {
                return Err(RewriteError::UnsupportedStatement);
            }
        }

        let referenced_tables = Self::extract_tables_from_ast(&statements);

        if referenced_tables.is_empty() {
            return Err(RewriteError::NoTablesProvided);
        }

        for table_name in &referenced_tables {
            let r2_path = tables.get(table_name).or_else(|| {
                table_name
                    .rsplit('.')
                    .next()
                    .and_then(|short| tables.get(short))
            });
            match r2_path {
                Some(r2_path) => {
                    if !r2_path.belongs_to_project(project_id) {
                        return Err(RewriteError::AccessDenied {
                            table: table_name.clone(),
                            project_id,
                        });
                    }
                }
                None => {
                    return Err(RewriteError::TableNotFound(table_name.clone()));
                }
            }
        }

        if let Some(cache) = &self.cache {
            if let Some(cached) = cache.get(sql, tables) {
                return Ok((*cached).clone());
            }
        }

        let mut statements = statements;
        let transformer = BasicTableTransformer::with_s3_config(self.s3_config(), tables);
        let visitor = AstVisitor::new(&transformer);

        for statement in &mut statements {
            visitor.visit_statement(statement);
        }

        let result = serialize_statements(&statements);

        if let Some(cache) = &self.cache {
            cache.put(sql, tables, result.clone());
        }

        Ok(result)
    }

    /// Validate that a set of tables all belong to the specified project.
    ///
    /// SECURITY: Use this for pre-flight validation before query execution.
    #[tracing::instrument(
        name = "warehouse.query.rewrite.validate_table_access",
        skip_all,
        err(Display)
    )]
    pub fn validate_table_access(
        tables: &AHashMap<String, R2TablePath>,
        project_id: Uuid,
    ) -> RewriteResult<()> {
        for (table_name, r2_path) in tables {
            if !r2_path.belongs_to_project(project_id) {
                return Err(RewriteError::AccessDenied {
                    table: table_name.clone(),
                    project_id,
                });
            }
        }
        Ok(())
    }

    /// Get the set of tables referenced in a query that are missing from the provided table map.
    ///
    /// Useful for generating helpful error messages.
    #[tracing::instrument(
        name = "warehouse.query.rewrite.find_missing_tables",
        skip_all,
        err(Display)
    )]
    pub fn find_missing_tables(
        sql: &str,
        available_tables: &AHashMap<String, R2TablePath>,
    ) -> RewriteResult<AHashSet<String>> {
        let referenced = Self::extract_tables(sql)?;
        Ok(Self::compute_missing_tables(&referenced, available_tables))
    }

    /// Find missing tables from pre-parsed statements.
    ///
    /// Zero-parse variant of `find_missing_tables` for use in the optimized pipeline.
    pub fn find_missing_tables_from_ast(
        statements: &[Statement],
        available_tables: &AHashMap<String, R2TablePath>,
    ) -> AHashSet<String> {
        let referenced = Self::extract_tables_from_ast(statements);
        Self::compute_missing_tables(&referenced, available_tables)
    }

    /// Compute which referenced tables are missing from the available set.
    ///
    /// Handles schema-qualified names (e.g. `myschema.orders`) by also
    /// checking the short name (`orders`) against the available set.
    fn compute_missing_tables(
        referenced: &[String],
        available_tables: &AHashMap<String, R2TablePath>,
    ) -> AHashSet<String> {
        let available: AHashSet<&str> = available_tables.keys().map(|k| k.as_str()).collect();
        referenced
            .iter()
            .filter(|t| {
                if available.contains(t.as_str()) {
                    return false;
                }
                if let Some(short) = t.rsplit('.').next() {
                    if available.contains(short) {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect()
    }

    /// Extract date range predicates from a SQL query for partition pruning.
    ///
    /// Looks for patterns like:
    /// - `column >= '2025-01-01'`
    /// - `column BETWEEN '2025-01-01' AND '2025-01-31'`
    /// - `column > '2025-01-01' AND column < '2025-02-01'`
    ///
    /// Returns a map of column name -> DateRange for partition pruning.
    #[tracing::instrument(
        name = "warehouse.query.rewrite.extract_date_predicates",
        skip_all,
        err(Display)
    )]
    pub fn extract_date_predicates(sql: &str) -> RewriteResult<AHashMap<String, DateRange>> {
        let dialect = ClickHouseDialect {};
        let statements = Parser::parse_sql(&dialect, sql)?;
        Ok(Self::extract_date_predicates_from_ast(&statements))
    }

    /// Extract date predicates from pre-parsed statements.
    ///
    /// Zero-parse variant of `extract_date_predicates` for use in the optimized pipeline.
    pub fn extract_date_predicates_from_ast(
        statements: &[Statement],
    ) -> AHashMap<String, DateRange> {
        let mut date_ranges: AHashMap<String, DateRange> = AHashMap::new();
        // Track which table qualifier was seen for each bare column name.
        // `None` = unqualified, `Some(q)` = qualified with `q`.
        // If a column is seen with two different qualifiers (cross-table),
        // it's added to `conflicted` and removed from the ranges to prevent
        // incorrect pruning.
        let mut column_qualifiers: AHashMap<String, Option<String>> = AHashMap::new();
        let mut conflicted: AHashSet<String> = AHashSet::new();
        for statement in statements {
            if let Statement::Query(query) = statement {
                Self::extract_date_predicates_from_query(
                    query,
                    &mut date_ranges,
                    &mut column_qualifiers,
                    &mut conflicted,
                );
            }
        }
        for col in &conflicted {
            date_ranges.remove(col);
        }
        date_ranges
    }

    /// Extract date predicates from a query, including CTE bodies.
    ///
    /// CTE bodies are traversed so that date-based partition pruning works for
    /// queries like `WITH filtered AS (SELECT * FROM t WHERE date >= '...')`.
    /// Cross-table ambiguity is handled by the `column_qualifiers` /
    /// `conflicted` tracking (same mechanism used by skip predicate extraction).
    fn extract_date_predicates_from_query(
        query: &Query,
        ranges: &mut AHashMap<String, DateRange>,
        column_qualifiers: &mut AHashMap<String, Option<String>>,
        conflicted: &mut AHashSet<String>,
    ) {
        // CTE bodies are separate scopes. An unqualified `date` inside a CTE
        // may reference a completely different table than `date` in the main
        // query. Process each CTE with its own qualifier/conflicted tracking
        // and tentatively merge results. After the main query body is also
        // processed, any column that appeared in both a CTE scope and the
        // main body with incompatible ranges is marked conflicted.
        if let Some(ref with) = query.with {
            for cte in &with.cte_tables {
                let mut cte_ranges: AHashMap<String, DateRange> = AHashMap::new();
                let mut cte_qualifiers: AHashMap<String, Option<String>> = AHashMap::new();
                let mut cte_conflicted: AHashSet<String> = AHashSet::new();
                Self::extract_date_predicates_from_query(
                    &cte.query,
                    &mut cte_ranges,
                    &mut cte_qualifiers,
                    &mut cte_conflicted,
                );
                for (col, cte_range) in cte_ranges {
                    if cte_conflicted.contains(&col) {
                        continue;
                    }

                    // Detect qualifier conflicts between CTEs: if a previous
                    // CTE already recorded a different qualifier for the same
                    // bare column name, the two CTEs reference different tables
                    // and their ranges must not be merged.
                    if let Some(cq) = cte_qualifiers.get(&col) {
                        if let Some(prev) = column_qualifiers.get(&col) {
                            if prev != cq {
                                conflicted.insert(col.clone());
                                ranges.remove(&col);
                                continue;
                            }
                        }
                    }

                    let entry = ranges
                        .entry(col.clone())
                        .or_insert_with(|| DateRange::new(None, None));
                    if let Some(s) = cte_range.start {
                        entry.start = Some(entry.start.map_or(s, |es| es.max(s)));
                    }
                    if let Some(e) = cte_range.end {
                        entry.end = Some(entry.end.map_or(e, |ee| ee.min(e)));
                    }
                    if let Some(cq) = cte_qualifiers.get(&col) {
                        column_qualifiers.entry(col).or_insert_with(|| cq.clone());
                    }
                }
            }
        }

        // Snapshot which columns had ranges from CTEs before processing the
        // main body, so we can detect cross-scope conflicts.
        let cte_columns: AHashSet<String> = ranges.keys().cloned().collect();

        // Process the main query body with separate tracking.
        let mut body_ranges: AHashMap<String, DateRange> = AHashMap::new();
        let mut body_qualifiers: AHashMap<String, Option<String>> = AHashMap::new();
        let mut body_conflicted: AHashSet<String> = AHashSet::new();
        Self::extract_date_predicates_from_set_expr(
            &query.body,
            &mut body_ranges,
            &mut body_qualifiers,
            &mut body_conflicted,
        );

        for (col, body_range) in body_ranges {
            if body_conflicted.contains(&col) {
                conflicted.insert(col.clone());
                ranges.remove(&col);
                continue;
            }

            if cte_columns.contains(&col) {
                // If the CTE and body use different qualifiers for the same
                // bare column name, they reference different tables — mark
                // conflicted to avoid incorrectly merging unrelated ranges.
                let cte_qual = column_qualifiers.get(&col);
                let body_qual = body_qualifiers.get(&col);
                if cte_qual != body_qual {
                    conflicted.insert(col.clone());
                    ranges.remove(&col);
                    continue;
                }

                // Same qualifier (or both unqualified). If the merged range
                // would be impossible, the CTE and main body reference
                // different tables — mark conflicted instead.
                let cte_range = match ranges.get(&col) {
                    Some(r) => r,
                    None => continue,
                };
                let merged_start = match (cte_range.start, body_range.start) {
                    (Some(a), Some(b)) => Some(a.max(b)),
                    (s, None) | (None, s) => s,
                };
                let merged_end = match (cte_range.end, body_range.end) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (e, None) | (None, e) => e,
                };
                if let (Some(s), Some(e)) = (merged_start, merged_end) {
                    if s > e {
                        conflicted.insert(col.clone());
                        ranges.remove(&col);
                        continue;
                    }
                }
            }

            let entry = ranges
                .entry(col.clone())
                .or_insert_with(|| DateRange::new(None, None));
            if let Some(s) = body_range.start {
                entry.start = Some(entry.start.map_or(s, |es| es.max(s)));
            }
            if let Some(e) = body_range.end {
                entry.end = Some(entry.end.map_or(e, |ee| ee.min(e)));
            }
            if let Some(bq) = body_qualifiers.get(&col) {
                column_qualifiers.entry(col).or_insert_with(|| bq.clone());
            }
        }
    }

    /// Extract date predicates from a set expression, including UNION branches.
    fn extract_date_predicates_from_set_expr(
        set_expr: &SetExpr,
        ranges: &mut AHashMap<String, DateRange>,
        column_qualifiers: &mut AHashMap<String, Option<String>>,
        conflicted: &mut AHashSet<String>,
    ) {
        match set_expr {
            SetExpr::Select(select) => {
                if let Some(ref selection) = select.selection {
                    Self::extract_date_predicates_from_expr(
                        selection,
                        ranges,
                        column_qualifiers,
                        conflicted,
                    );
                }
            }
            SetExpr::Query(query) => Self::extract_date_predicates_from_query(
                query,
                ranges,
                column_qualifiers,
                conflicted,
            ),
            SetExpr::SetOperation { .. } => {
                // UNION/EXCEPT/INTERSECT branches may reference different tables
                // with different date semantics. Merging their ranges into a
                // shared map could incorrectly prune partitions for one branch
                // using the other branch's constraints. Skip extraction here.
            }
            _ => {}
        }
    }

    /// Check whether recording a predicate for `bare_col` with `qualifier` would
    /// conflict with a previously recorded qualifier. If so, mark the column as
    /// conflicted and remove it from the ranges map so it won't be used for
    /// partition pruning.
    ///
    /// Returns `true` if the column is conflicted and should be skipped.
    fn is_date_column_conflicted(
        bare_col: &str,
        qualifier: &Option<String>,
        column_qualifiers: &mut AHashMap<String, Option<String>>,
        conflicted: &mut AHashSet<String>,
        ranges: &mut AHashMap<String, DateRange>,
    ) -> bool {
        if conflicted.contains(bare_col) {
            return true;
        }
        match column_qualifiers.get(bare_col) {
            Some(prev_qualifier) if prev_qualifier != qualifier => {
                conflicted.insert(bare_col.to_string());
                ranges.remove(bare_col);
                true
            }
            None => {
                column_qualifiers.insert(bare_col.to_string(), qualifier.clone());
                false
            }
            _ => false, // same qualifier, no conflict
        }
    }

    /// Extract date predicates from an expression.
    fn extract_date_predicates_from_expr(
        expr: &Expr,
        ranges: &mut AHashMap<String, DateRange>,
        column_qualifiers: &mut AHashMap<String, Option<String>>,
        conflicted: &mut AHashSet<String>,
    ) {
        match expr {
            // Handle binary operations like column >= '2025-01-01'
            Expr::BinaryOp { left, op, right } => {
                Self::try_extract_date_comparison(
                    left,
                    op,
                    right,
                    ranges,
                    column_qualifiers,
                    conflicted,
                );
                // Only recurse into AND branches. OR branches cannot safely
                // narrow date ranges: a row might match the other disjunct.
                if matches!(op, BinaryOperator::And) {
                    Self::extract_date_predicates_from_expr(
                        left,
                        ranges,
                        column_qualifiers,
                        conflicted,
                    );
                    Self::extract_date_predicates_from_expr(
                        right,
                        ranges,
                        column_qualifiers,
                        conflicted,
                    );
                }
            }
            // Handle BETWEEN (skip NOT BETWEEN — negated ranges cannot narrow partitions)
            Expr::Between {
                expr,
                negated,
                low,
                high,
                ..
            } => {
                if !negated {
                    if let Some((qualifier, bare_col)) = Self::extract_column_name_qualified(expr) {
                        if !Self::is_date_column_conflicted(
                            &bare_col,
                            &qualifier,
                            column_qualifiers,
                            conflicted,
                            ranges,
                        ) {
                            if let (Some(start), Some(end)) = (
                                Self::try_parse_date_value(low),
                                Self::try_parse_date_value(high),
                            ) {
                                let range = ranges
                                    .entry(bare_col)
                                    .or_insert_with(|| DateRange::new(None, None));
                                range.start = Some(range.start.map_or(start, |s| s.max(start)));
                                range.end = Some(range.end.map_or(end, |e| e.min(end)));
                            }
                        }
                    }
                }
            }
            // Handle nested expressions
            Expr::Nested(inner) => {
                Self::extract_date_predicates_from_expr(
                    inner,
                    ranges,
                    column_qualifiers,
                    conflicted,
                );
            }
            _ => {}
        }
    }

    /// Extract column name from an expression (handles both simple and compound identifiers).
    ///
    /// Returns the bare column name (without table qualifier).
    /// Extract column name with optional table qualifier.
    ///
    /// Returns `(qualifier, bare_column_name)`. The qualifier is `None` for
    /// unqualified columns and `Some("alias")` for qualified ones like `o.date`.
    fn extract_column_name_qualified(expr: &Expr) -> Option<(Option<String>, String)> {
        match expr {
            Expr::Identifier(ident) => Some((None, ident.value.clone())),
            Expr::CompoundIdentifier(idents) if idents.len() >= 2 => {
                let qualifier = idents[idents.len() - 2].value.clone();
                let column = idents[idents.len() - 1].value.clone();
                Some((Some(qualifier), column))
            }
            Expr::CompoundIdentifier(idents) => {
                idents.last().map(|ident| (None, ident.value.clone()))
            }
            Expr::Cast { expr, .. } => Self::extract_column_name_qualified(expr),
            Expr::Function(func) => {
                if let FunctionArguments::List(ref list) = func.args {
                    if list.args.len() == 1 {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) = &list.args[0] {
                            return Self::extract_column_name_qualified(inner);
                        }
                    }
                }
                None
            }
            _ => None,
        }
    }

    /// Extract a bare column name without recursing through function calls.
    ///
    /// Unlike `extract_column_name_qualified`, this variant refuses to look
    /// through `Expr::Function` wrappers.  It is used for **skip-index
    /// predicates** where the indexed data stores raw column values: extracting
    /// `status` from `UPPER(status) = 'ACTIVE'` would compare `'ACTIVE'`
    /// against the raw (possibly lowercase) data and incorrectly prune files.
    fn extract_column_name_for_skip(expr: &Expr) -> Option<(Option<String>, String)> {
        match expr {
            Expr::Identifier(ident) => Some((None, ident.value.clone())),
            Expr::CompoundIdentifier(idents) if idents.len() >= 2 => {
                let qualifier = idents[idents.len() - 2].value.clone();
                let column = idents[idents.len() - 1].value.clone();
                Some((Some(qualifier), column))
            }
            Expr::CompoundIdentifier(idents) => {
                idents.last().map(|ident| (None, ident.value.clone()))
            }
            Expr::Cast { expr, .. } => Self::extract_column_name_for_skip(expr),
            _ => None,
        }
    }

    /// Try to extract a date comparison from a binary operation.
    fn try_extract_date_comparison(
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
        ranges: &mut AHashMap<String, DateRange>,
        column_qualifiers: &mut AHashMap<String, Option<String>>,
        conflicted: &mut AHashSet<String>,
    ) {
        // Try column op value (handles both simple and compound identifiers)
        if let Some((qualifier, bare_col)) = Self::extract_column_name_qualified(left) {
            if !Self::is_date_column_conflicted(
                &bare_col,
                &qualifier,
                column_qualifiers,
                conflicted,
                ranges,
            ) {
                if let Some(date) = Self::try_parse_date_value(right) {
                    Self::update_date_range(&bare_col, op, date, ranges);
                }
            }
        }
        // Try value op column (reversed)
        else if let Some((qualifier, bare_col)) = Self::extract_column_name_qualified(right) {
            if !Self::is_date_column_conflicted(
                &bare_col,
                &qualifier,
                column_qualifiers,
                conflicted,
                ranges,
            ) {
                if let Some(date) = Self::try_parse_date_value(left) {
                    let reversed_op = match op {
                        BinaryOperator::Lt => BinaryOperator::Gt,
                        BinaryOperator::LtEq => BinaryOperator::GtEq,
                        BinaryOperator::Gt => BinaryOperator::Lt,
                        BinaryOperator::GtEq => BinaryOperator::LtEq,
                        other => other.clone(),
                    };
                    Self::update_date_range(&bare_col, &reversed_op, date, ranges);
                }
            }
        }
    }

    /// Update the date range for a column based on an operator.
    ///
    /// When multiple predicates refine the same column, this keeps the
    /// tightest (most restrictive) bound: the latest start and the earliest
    /// end.  If the resulting range is contradictory (`start > end`), the
    /// range is left in that state so callers can detect it via
    /// `DateRange::is_impossible()`.
    fn update_date_range(
        col_name: &str,
        op: &BinaryOperator,
        date: NaiveDate,
        ranges: &mut AHashMap<String, DateRange>,
    ) {
        let range = ranges
            .entry(col_name.to_string())
            .or_insert_with(|| DateRange::new(None, None));

        match op {
            BinaryOperator::Eq => {
                range.start = Some(range.start.map_or(date, |s| s.max(date)));
                range.end = Some(range.end.map_or(date, |e| e.min(date)));
            }
            BinaryOperator::Gt | BinaryOperator::GtEq => {
                // Use the parsed date without adjustment. We cannot distinguish
                // Date columns (where `> '2025-01-31'` truly means `>= '2025-02-01'`)
                // from DateTime columns (where rows at '2025-01-31 00:00:01' still match).
                // The conservative approach includes the parsed date's month in the
                // partition scan, avoiding data loss on month boundaries.
                range.start = Some(range.start.map_or(date, |s| s.max(date)));
            }
            BinaryOperator::Lt | BinaryOperator::LtEq => {
                range.end = Some(range.end.map_or(date, |e| e.min(date)));
            }
            _ => {}
        }
    }

    /// Try to parse a date from a Value expression.
    ///
    /// Handles:
    /// - Literal date strings: '2025-01-01', '2025/01/01'
    /// - ClickHouse functions: today(), yesterday(), now()
    /// - toDate() wrapper: toDate('2025-01-01'), toDate(now())
    /// - toStartOf* functions: toStartOfDay(), toStartOfWeek(), etc.
    /// - Simple date arithmetic: today() - INTERVAL N DAY/WEEK/MONTH
    ///
    /// PERFORMANCE: Parsing static date functions enables partition pruning
    /// for common patterns like `WHERE date >= today() - INTERVAL 7 DAY`.
    ///
    /// # Limitations
    ///
    /// Complex date arithmetic involving variables, nested expressions, or
    /// non-constant intervals cannot be statically evaluated and will fall
    /// back to scanning all partitions.
    fn try_parse_date_value(expr: &Expr) -> Option<NaiveDate> {
        match expr {
            Expr::Value(Value::SingleQuotedString(s)) => {
                // Try common date formats
                Self::parse_date_string(s)
            }
            Expr::Function(func) => {
                let func_name = func
                    .name
                    .0
                    .last()
                    .map(|i| i.value.to_lowercase())
                    .unwrap_or_default();

                match func_name.as_str() {
                    "today" => {
                        // today() returns current date
                        Some(chrono::Utc::now().date_naive())
                    }
                    "yesterday" => {
                        // yesterday() returns current date minus 1 day
                        Some(chrono::Utc::now().date_naive() - chrono::Duration::days(1))
                    }
                    "now" => {
                        // now() returns current timestamp, we use the date part
                        Some(chrono::Utc::now().date_naive())
                    }
                    "curdate" | "current_date" => {
                        // curdate() and current_date() return current date
                        Some(chrono::Utc::now().date_naive())
                    }
                    "todate" => {
                        // toDate('...') or toDate(now()) - extract the argument and parse it
                        Self::extract_function_first_arg_date(func).or_else(|| {
                            Self::extract_function_string_arg(func)
                                .and_then(|s| Self::parse_date_string(&s))
                        })
                    }
                    "tostartofday" => {
                        // toStartOfDay(datetime) - returns date part
                        Self::extract_function_first_arg_date(func)
                    }
                    "tostartofweek" => {
                        // toStartOfWeek(date) default mode=0 returns Sunday
                        Self::extract_function_first_arg_date(func).map(|d| {
                            use chrono::Datelike;
                            let days_from_sunday = d.weekday().num_days_from_sunday();
                            d - chrono::Duration::days(days_from_sunday as i64)
                        })
                    }
                    "tostartofmonth" => {
                        // toStartOfMonth(date) - returns first day of month
                        Self::extract_function_first_arg_date(func).and_then(|d| {
                            use chrono::Datelike;
                            NaiveDate::from_ymd_opt(d.year(), d.month(), 1)
                        })
                    }
                    "tostartofyear" => {
                        // toStartOfYear(date) - returns first day of year
                        Self::extract_function_first_arg_date(func).and_then(|d| {
                            use chrono::Datelike;
                            NaiveDate::from_ymd_opt(d.year(), 1, 1)
                        })
                    }
                    "tostartofquarter" => {
                        // toStartOfQuarter(date) - returns first day of quarter
                        Self::extract_function_first_arg_date(func).and_then(|d| {
                            use chrono::Datelike;
                            let quarter_month = ((d.month() - 1) / 3) * 3 + 1;
                            NaiveDate::from_ymd_opt(d.year(), quarter_month, 1)
                        })
                    }
                    "adddays" | "subdays" => {
                        // addDays(date, N) / subDays(date, N)
                        Self::parse_date_add_sub(func, func_name.as_str() == "subdays")
                    }
                    "addweeks" | "subweeks" => {
                        Self::parse_date_add_sub_weeks(func, func_name.as_str() == "subweeks")
                    }
                    "addmonths" | "submonths" => {
                        Self::parse_date_add_sub_months(func, func_name.as_str() == "submonths")
                    }
                    _ => None,
                }
            }
            // Cast expressions like CAST('2025-01-01' AS Date)
            Expr::Cast { expr, .. } => Self::try_parse_date_value(expr),
            // Handle binary operations for date arithmetic: today() - INTERVAL 7 DAY
            Expr::BinaryOp { left, op, right } => Self::try_parse_date_arithmetic(left, op, right),
            _ => None,
        }
    }

    /// Try to parse date arithmetic expressions like `today() - INTERVAL 7 DAY`.
    fn try_parse_date_arithmetic(
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
    ) -> Option<NaiveDate> {
        // Handle addition and subtraction for date arithmetic
        // (e.g. today() - INTERVAL 7 DAY, now() + INTERVAL 1 MONTH)
        if !matches!(op, BinaryOperator::Minus | BinaryOperator::Plus) {
            return None;
        }

        let base_date = Self::try_parse_date_value(left)?;

        // Try to parse the right side as an interval
        if let Expr::Interval(interval) = right {
            return Self::apply_interval(base_date, interval, matches!(op, BinaryOperator::Minus));
        }

        None
    }

    /// Apply an interval to a date.
    fn apply_interval(
        base_date: NaiveDate,
        interval: &sqlparser::ast::Interval,
        subtract: bool,
    ) -> Option<NaiveDate> {
        use chrono::Datelike;

        // Extract the interval value
        let value_str = match interval.value.as_ref() {
            Expr::Value(Value::Number(n, _)) => n.clone(),
            Expr::Value(Value::SingleQuotedString(s)) => s.clone(),
            _ => return None,
        };

        let value: i64 = value_str.parse().ok()?;
        let value = if subtract { -value } else { value };

        // Apply based on interval unit
        let unit = interval.leading_field.as_ref()?;

        match unit {
            sqlparser::ast::DateTimeField::Day => Some(base_date + chrono::Duration::days(value)),
            sqlparser::ast::DateTimeField::Week(_) => {
                Some(base_date + chrono::Duration::weeks(value))
            }
            sqlparser::ast::DateTimeField::Month => add_months_to_date(base_date, value),
            sqlparser::ast::DateTimeField::Year => {
                let year_offset = i32::try_from(value).ok()?;
                let new_year = base_date.year().checked_add(year_offset)?;
                let day = if base_date.month() == 2 && base_date.day() == 29 {
                    if NaiveDate::from_ymd_opt(new_year, 2, 29).is_some() {
                        29
                    } else {
                        28
                    }
                } else {
                    base_date.day()
                };
                NaiveDate::from_ymd_opt(new_year, base_date.month(), day)
            }
            _ => None, // Hours, minutes, seconds don't make sense for date
        }
    }

    /// Parse addDays/subDays function calls.
    fn parse_date_add_sub(func: &Function, subtract: bool) -> Option<NaiveDate> {
        let args = match &func.args {
            FunctionArguments::List(list) => &list.args,
            _ => return None,
        };

        if args.len() != 2 {
            return None;
        }

        // First arg is the date
        let base_date = if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = &args[0] {
            Self::try_parse_date_value(expr)?
        } else {
            return None;
        };

        // Second arg is the number of days
        let days: i64 = if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = &args[1] {
            match expr {
                Expr::Value(Value::Number(n, _)) => n.parse().ok()?,
                _ => return None,
            }
        } else {
            return None;
        };

        let delta = if subtract { -days } else { days };
        Some(base_date + chrono::Duration::days(delta))
    }

    /// Parse addWeeks/subWeeks function calls.
    fn parse_date_add_sub_weeks(func: &Function, subtract: bool) -> Option<NaiveDate> {
        let args = match &func.args {
            FunctionArguments::List(list) => &list.args,
            _ => return None,
        };

        if args.len() != 2 {
            return None;
        }

        let base_date = if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = &args[0] {
            Self::try_parse_date_value(expr)?
        } else {
            return None;
        };

        let weeks: i64 = if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = &args[1] {
            match expr {
                Expr::Value(Value::Number(n, _)) => n.parse().ok()?,
                _ => return None,
            }
        } else {
            return None;
        };

        let delta = if subtract { -weeks } else { weeks };
        Some(base_date + chrono::Duration::weeks(delta))
    }

    /// Parse addMonths/subMonths function calls.
    fn parse_date_add_sub_months(func: &Function, subtract: bool) -> Option<NaiveDate> {
        let args = match &func.args {
            FunctionArguments::List(list) => &list.args,
            _ => return None,
        };

        if args.len() != 2 {
            return None;
        }

        let base_date = if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = &args[0] {
            Self::try_parse_date_value(expr)?
        } else {
            return None;
        };

        let months: i32 = if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = &args[1] {
            match expr {
                Expr::Value(Value::Number(n, _)) => n.parse().ok()?,
                _ => return None,
            }
        } else {
            return None;
        };

        let delta: i64 = if subtract {
            -(months as i64)
        } else {
            months as i64
        };
        add_months_to_date(base_date, delta)
    }

    /// Parse a date string in common formats.
    fn parse_date_string(s: &str) -> Option<NaiveDate> {
        NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .or_else(|_| NaiveDate::parse_from_str(s, "%Y/%m/%d"))
            .ok()
            .or_else(|| {
                // Handle datetime formats like '2025-01-01 00:00:00' or
                // '2025-01-01T00:00:00' by extracting the date portion.
                let date_part = s.get(..10)?;
                NaiveDate::parse_from_str(date_part, "%Y-%m-%d")
                    .or_else(|_| NaiveDate::parse_from_str(date_part, "%Y/%m/%d"))
                    .ok()
            })
    }

    /// Extract a string argument from a function call.
    fn extract_function_string_arg(func: &Function) -> Option<String> {
        use sqlparser::ast::FunctionArg;
        use sqlparser::ast::FunctionArgExpr;

        // Get the first argument
        let args = match &func.args {
            FunctionArguments::List(list) => &list.args,
            FunctionArguments::None => return None,
            FunctionArguments::Subquery(_) => return None,
        };

        if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))) = args.first() {
            if let Expr::Value(Value::SingleQuotedString(s)) = expr {
                return Some(s.clone());
            }
        }

        None
    }

    /// Extract a date from the first argument of a function (for toStartOf* functions).
    fn extract_function_first_arg_date(func: &Function) -> Option<NaiveDate> {
        use sqlparser::ast::FunctionArg;
        use sqlparser::ast::FunctionArgExpr;

        let args = match &func.args {
            FunctionArguments::List(list) => &list.args,
            FunctionArguments::None => return None,
            FunctionArguments::Subquery(_) => return None,
        };

        if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))) = args.first() {
            return Self::try_parse_date_value(expr);
        }

        None
    }

    /// Extract equality predicates from a SQL query for skip index filtering.
    ///
    /// Looks for patterns like:
    /// - `column = 'value'`
    /// - `column IN ('a', 'b', 'c')`
    /// - `column LIKE 'prefix%'`
    ///
    /// Returns SkipPredicates that can be used with HierarchicalSkipIndex.
    #[tracing::instrument(
        name = "warehouse.query.rewrite.extract_skip_predicates",
        skip_all,
        err(Display)
    )]
    pub fn extract_skip_predicates(sql: &str) -> RewriteResult<SkipPredicates> {
        let dialect = ClickHouseDialect {};
        let statements = Parser::parse_sql(&dialect, sql)?;
        Ok(Self::extract_skip_predicates_from_ast(&statements))
    }

    /// Extract skip predicates from pre-parsed statements.
    ///
    /// Zero-parse variant of `extract_skip_predicates` for use in the optimized pipeline.
    pub fn extract_skip_predicates_from_ast(statements: &[Statement]) -> SkipPredicates {
        let mut predicates = SkipPredicates::new();
        let mut column_qualifiers: AHashMap<String, Option<String>> = AHashMap::new();
        let mut conflicted: AHashSet<String> = AHashSet::new();
        for statement in statements {
            if let Statement::Query(query) = statement {
                Self::extract_predicates_from_query(
                    query,
                    &mut predicates,
                    &mut column_qualifiers,
                    &mut conflicted,
                );
            }
        }
        for col in &conflicted {
            predicates.equality.remove(col);
            predicates.prefix.remove(col);
            predicates.in_lists.remove(col);
            predicates.ranges.remove(col);
            predicates.substring.remove(col);
            predicates.token_search.remove(col);
        }
        predicates
    }

    /// Extract predicates from a query.
    ///
    /// Each CTE body is processed in its own scope so that an unqualified column
    /// appearing in multiple CTEs (or in both a CTE and the main body) with
    /// different values is detected as conflicted and removed, mirroring the
    /// scoping strategy used by `extract_date_predicates_from_query`.
    fn extract_predicates_from_query(
        query: &Query,
        predicates: &mut SkipPredicates,
        column_qualifiers: &mut AHashMap<String, Option<String>>,
        conflicted: &mut AHashSet<String>,
    ) {
        if let Some(ref with) = query.with {
            for cte in &with.cte_tables {
                let mut cte_predicates = SkipPredicates::new();
                let mut cte_qualifiers: AHashMap<String, Option<String>> = AHashMap::new();
                let mut cte_conflicted: AHashSet<String> = AHashSet::new();
                Self::extract_predicates_from_query(
                    &cte.query,
                    &mut cte_predicates,
                    &mut cte_qualifiers,
                    &mut cte_conflicted,
                );
                Self::merge_skip_predicates(
                    predicates,
                    column_qualifiers,
                    conflicted,
                    cte_predicates,
                    &cte_qualifiers,
                    &cte_conflicted,
                );
            }
        }

        let mut body_predicates = SkipPredicates::new();
        let mut body_qualifiers: AHashMap<String, Option<String>> = AHashMap::new();
        let mut body_conflicted: AHashSet<String> = AHashSet::new();
        Self::extract_predicates_from_set_expr(
            &query.body,
            &mut body_predicates,
            &mut body_qualifiers,
            &mut body_conflicted,
        );
        Self::merge_skip_predicates(
            predicates,
            column_qualifiers,
            conflicted,
            body_predicates,
            &body_qualifiers,
            &body_conflicted,
        );
    }

    /// Merge skip predicates from a child scope (CTE or main body) into the
    /// parent accumulator. If the same unqualified column already exists with a
    /// different qualifier or contradictory values, mark it conflicted.
    fn merge_skip_predicates(
        target: &mut SkipPredicates,
        target_qualifiers: &mut AHashMap<String, Option<String>>,
        target_conflicted: &mut AHashSet<String>,
        source: SkipPredicates,
        source_qualifiers: &AHashMap<String, Option<String>>,
        source_conflicted: &AHashSet<String>,
    ) {
        let all_columns: AHashSet<String> = source
            .equality
            .keys()
            .chain(source.prefix.keys())
            .chain(source.in_lists.keys())
            .chain(source.ranges.keys())
            .chain(source.substring.keys())
            .chain(source.token_search.keys())
            .cloned()
            .collect();

        for col in all_columns {
            if source_conflicted.contains(&col) || target_conflicted.contains(&col) {
                target_conflicted.insert(col.clone());
                target.equality.remove(&col);
                target.prefix.remove(&col);
                target.in_lists.remove(&col);
                target.ranges.remove(&col);
                target.substring.remove(&col);
                target.token_search.remove(&col);
                continue;
            }

            if let Some(sq) = source_qualifiers.get(&col) {
                if let Some(tq) = target_qualifiers.get(&col) {
                    if tq != sq {
                        target_conflicted.insert(col.clone());
                        target.equality.remove(&col);
                        target.prefix.remove(&col);
                        target.in_lists.remove(&col);
                        target.ranges.remove(&col);
                        target.substring.remove(&col);
                        target.token_search.remove(&col);
                        continue;
                    }
                }
            }

            if let Some(val) = source.equality.get(&col) {
                if let Some(existing) = target.equality.get(&col) {
                    if existing != val {
                        target_conflicted.insert(col.clone());
                        target.equality.remove(&col);
                        continue;
                    }
                } else {
                    target.equality.insert(col.clone(), val.clone());
                }
            }
            if let Some(val) = source.prefix.get(&col) {
                if let Some(existing) = target.prefix.get(&col) {
                    if existing != val {
                        target_conflicted.insert(col.clone());
                        target.prefix.remove(&col);
                    }
                } else {
                    target.prefix.insert(col.clone(), val.clone());
                }
            }
            if let Some(vals) = source.in_lists.get(&col) {
                if !target.in_lists.contains_key(&col) {
                    target.in_lists.insert(col.clone(), vals.clone());
                }
            }
            if let Some(range) = source.ranges.get(&col) {
                if !target.ranges.contains_key(&col) {
                    target.ranges.insert(col.clone(), range.clone());
                }
            }
            if let Some(subs) = source.substring.get(&col) {
                if !target.substring.contains_key(&col) {
                    target.substring.insert(col.clone(), subs.clone());
                }
            }
            if let Some(tokens) = source.token_search.get(&col) {
                target
                    .token_search
                    .entry(col.clone())
                    .or_default()
                    .extend(tokens.iter().cloned());
            }

            if let Some(sq) = source_qualifiers.get(&col) {
                target_qualifiers.entry(col).or_insert_with(|| sq.clone());
            }
        }
    }

    /// Extract predicates from a set expression.
    fn extract_predicates_from_set_expr(
        set_expr: &SetExpr,
        predicates: &mut SkipPredicates,
        column_qualifiers: &mut AHashMap<String, Option<String>>,
        conflicted: &mut AHashSet<String>,
    ) {
        match set_expr {
            SetExpr::Select(select) => {
                if let Some(ref selection) = select.selection {
                    Self::extract_predicates_from_expr(
                        selection,
                        predicates,
                        column_qualifiers,
                        conflicted,
                    );
                }
            }
            SetExpr::Query(query) => Self::extract_predicates_from_query(
                query,
                predicates,
                column_qualifiers,
                conflicted,
            ),
            _ => {}
        }
    }

    /// Check if a column has conflicting qualifiers for skip predicates.
    /// Returns `true` if the column should be skipped.
    fn is_skip_column_conflicted(
        bare_col: &str,
        qualifier: &Option<String>,
        column_qualifiers: &mut AHashMap<String, Option<String>>,
        conflicted: &mut AHashSet<String>,
    ) -> bool {
        if conflicted.contains(bare_col) {
            return true;
        }
        match column_qualifiers.get(bare_col) {
            Some(prev_qualifier) if prev_qualifier != qualifier => {
                conflicted.insert(bare_col.to_string());
                true
            }
            None => {
                column_qualifiers.insert(bare_col.to_string(), qualifier.clone());
                false
            }
            _ => false,
        }
    }

    /// Extract predicates from an expression.
    fn extract_predicates_from_expr(
        expr: &Expr,
        predicates: &mut SkipPredicates,
        column_qualifiers: &mut AHashMap<String, Option<String>>,
        conflicted: &mut AHashSet<String>,
    ) {
        match expr {
            // Handle binary operations like column = 'value'
            Expr::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::Eq => {
                        if let Some((qualifier, bare_col)) =
                            Self::extract_column_name_for_skip(left)
                        {
                            if !Self::is_skip_column_conflicted(
                                &bare_col,
                                &qualifier,
                                column_qualifiers,
                                conflicted,
                            ) {
                                if let Some(value) = Self::try_extract_string_value(right) {
                                    predicates.add_equals(&bare_col, &value);
                                }
                            }
                        } else if let Some((qualifier, bare_col)) =
                            Self::extract_column_name_for_skip(right)
                        {
                            if !Self::is_skip_column_conflicted(
                                &bare_col,
                                &qualifier,
                                column_qualifiers,
                                conflicted,
                            ) {
                                if let Some(value) = Self::try_extract_string_value(left) {
                                    predicates.add_equals(&bare_col, &value);
                                }
                            }
                        }
                    }
                    BinaryOperator::And => {
                        Self::extract_predicates_from_expr(
                            left,
                            predicates,
                            column_qualifiers,
                            conflicted,
                        );
                        Self::extract_predicates_from_expr(
                            right,
                            predicates,
                            column_qualifiers,
                            conflicted,
                        );
                    }
                    BinaryOperator::Or => {
                        // OR branches cannot safely contribute skip predicates:
                        // a row might match the other branch, so neither side
                        // alone can be used to prune files/partitions.
                    }
                    BinaryOperator::Gt | BinaryOperator::GtEq => {
                        if let Some((qualifier, bare_col)) =
                            Self::extract_column_name_for_skip(left)
                        {
                            if !Self::is_skip_column_conflicted(
                                &bare_col,
                                &qualifier,
                                column_qualifiers,
                                conflicted,
                            ) {
                                if let Some(value) = Self::try_extract_string_value(right) {
                                    if matches!(op, BinaryOperator::GtEq) {
                                        predicates.add_gte(&bare_col, &value);
                                    } else {
                                        predicates.add_gt(&bare_col, &value);
                                    }
                                }
                            }
                        } else if let Some((qualifier, bare_col)) =
                            Self::extract_column_name_for_skip(right)
                        {
                            if !Self::is_skip_column_conflicted(
                                &bare_col,
                                &qualifier,
                                column_qualifiers,
                                conflicted,
                            ) {
                                if let Some(value) = Self::try_extract_string_value(left) {
                                    if matches!(op, BinaryOperator::GtEq) {
                                        predicates.add_lte(&bare_col, &value);
                                    } else {
                                        predicates.add_lt(&bare_col, &value);
                                    }
                                }
                            }
                        }
                    }
                    BinaryOperator::Lt | BinaryOperator::LtEq => {
                        if let Some((qualifier, bare_col)) =
                            Self::extract_column_name_for_skip(left)
                        {
                            if !Self::is_skip_column_conflicted(
                                &bare_col,
                                &qualifier,
                                column_qualifiers,
                                conflicted,
                            ) {
                                if let Some(value) = Self::try_extract_string_value(right) {
                                    if matches!(op, BinaryOperator::LtEq) {
                                        predicates.add_lte(&bare_col, &value);
                                    } else {
                                        predicates.add_lt(&bare_col, &value);
                                    }
                                }
                            }
                        } else if let Some((qualifier, bare_col)) =
                            Self::extract_column_name_for_skip(right)
                        {
                            if !Self::is_skip_column_conflicted(
                                &bare_col,
                                &qualifier,
                                column_qualifiers,
                                conflicted,
                            ) {
                                if let Some(value) = Self::try_extract_string_value(left) {
                                    if matches!(op, BinaryOperator::LtEq) {
                                        predicates.add_gte(&bare_col, &value);
                                    } else {
                                        predicates.add_gt(&bare_col, &value);
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            // Handle LIKE expressions: column LIKE 'prefix%' or '%substring%'
            Expr::Like {
                negated,
                expr,
                pattern,
                ..
            } => {
                if !negated {
                    if let Some((qualifier, bare_col)) = Self::extract_column_name_for_skip(expr) {
                        if !Self::is_skip_column_conflicted(
                            &bare_col,
                            &qualifier,
                            column_qualifiers,
                            conflicted,
                        ) {
                            if let Some(pattern_str) = Self::try_extract_string_value(pattern) {
                                if pattern_str.starts_with('%')
                                    && pattern_str.ends_with('%')
                                    && pattern_str.len() > 2
                                {
                                    let substring = &pattern_str[1..pattern_str.len() - 1];
                                    if !substring.contains('%') && !substring.contains('_') {
                                        predicates.add_substring(&bare_col, substring);
                                    }
                                } else if pattern_str.ends_with('%')
                                    && !pattern_str.starts_with('%')
                                {
                                    let prefix = pattern_str.trim_end_matches('%');
                                    if !prefix.contains('%') && !prefix.contains('_') {
                                        predicates.add_prefix(&bare_col, prefix);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            // Handle IN expressions
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                if !negated {
                    if let Some((qualifier, bare_col)) = Self::extract_column_name_for_skip(expr) {
                        if !Self::is_skip_column_conflicted(
                            &bare_col,
                            &qualifier,
                            column_qualifiers,
                            conflicted,
                        ) {
                            let values: Vec<String> = list
                                .iter()
                                .filter_map(|e| Self::try_extract_string_value(e))
                                .collect();
                            if !values.is_empty() {
                                predicates.add_in(&bare_col, values);
                            }
                        }
                    }
                }
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
                ..
            } => {
                if !negated {
                    if let Some((qualifier, bare_col)) = Self::extract_column_name_for_skip(expr) {
                        if !Self::is_skip_column_conflicted(
                            &bare_col,
                            &qualifier,
                            column_qualifiers,
                            conflicted,
                        ) {
                            if let (Some(low_val), Some(high_val)) = (
                                Self::try_extract_string_value(low),
                                Self::try_extract_string_value(high),
                            ) {
                                predicates.add_gte(&bare_col, &low_val);
                                predicates.add_lte(&bare_col, &high_val);
                            }
                        }
                    }
                }
            }
            // Handle hasToken(column, 'token') for full-text search
            Expr::Function(func) => {
                let func_name = func.name.to_string().to_lowercase();
                if func_name == "hastoken" {
                    if let FunctionArguments::List(ref list) = func.args {
                        if list.args.len() == 2 {
                            let col_name = match &list.args[0] {
                                sqlparser::ast::FunctionArg::Unnamed(
                                    sqlparser::ast::FunctionArgExpr::Expr(e),
                                ) => Self::extract_column_name_for_skip(e),
                                _ => None,
                            };
                            let token_val = match &list.args[1] {
                                sqlparser::ast::FunctionArg::Unnamed(
                                    sqlparser::ast::FunctionArgExpr::Expr(e),
                                ) => Self::try_extract_string_value(e),
                                _ => None,
                            };
                            if let (Some((qualifier, bare_col)), Some(token)) =
                                (col_name, token_val)
                            {
                                if !Self::is_skip_column_conflicted(
                                    &bare_col,
                                    &qualifier,
                                    column_qualifiers,
                                    conflicted,
                                ) {
                                    predicates.add_token(&bare_col, &token);
                                }
                            }
                        }
                    }
                }
            }
            // Handle nested expressions
            Expr::Nested(inner) => {
                Self::extract_predicates_from_expr(
                    inner,
                    predicates,
                    column_qualifiers,
                    conflicted,
                );
            }
            _ => {}
        }
    }

    /// Try to extract a string value from an expression.
    ///
    /// Handles literals, cast expressions, and unary negation so that
    /// predicates like `col = -1` or `col = CAST('2024-01-01' AS Date)` are
    /// correctly extracted for skip index filtering.
    fn try_extract_string_value(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Value(Value::SingleQuotedString(s)) => Some(s.clone()),
            Expr::Value(Value::DoubleQuotedString(s)) => Some(s.clone()),
            Expr::Value(Value::Number(n, _)) => Some(n.clone()),
            Expr::Value(Value::Boolean(b)) => Some(b.to_string()),
            Expr::Cast { expr, .. } => Self::try_extract_string_value(expr),
            Expr::UnaryOp { op, expr } => {
                let op_str = format!("{}", op);
                if op_str == "-" {
                    Self::try_extract_string_value(expr).map(|v| format!("-{}", v))
                } else {
                    None
                }
            }
            Expr::Nested(inner) => Self::try_extract_string_value(inner),
            _ => None,
        }
    }

    /// Rewrite a query with partition pruning.
    ///
    /// This method extracts date predicates and uses them to generate
    /// more specific file patterns for date-partitioned tables.
    ///
    /// Uses the `PartitionPruningTransformer` with `AstVisitor` to avoid
    /// duplicating AST traversal logic.
    #[tracing::instrument(
        name = "warehouse.query.rewrite.rewrite_with_partition_pruning",
        skip_all,
        err(Display)
    )]
    pub fn rewrite_with_partition_pruning(
        &self,
        sql: &str,
        tables: &AHashMap<String, R2TablePath>,
    ) -> RewriteResult<String> {
        let dialect = ClickHouseDialect {};
        let mut statements = Parser::parse_sql(&dialect, sql)?;
        self.rewrite_with_partition_pruning_ast(&mut statements, tables)
    }

    pub fn rewrite_with_partition_pruning_ast(
        &self,
        statements: &mut [Statement],
        tables: &AHashMap<String, R2TablePath>,
    ) -> RewriteResult<String> {
        let date_predicates = Self::extract_date_predicates_from_ast(statements);

        let transformer =
            PartitionPruningTransformer::new(&self.collection_name, tables, &date_predicates);
        let visitor = AstVisitor::new(&transformer);

        for statement in statements.iter_mut() {
            visitor.visit_statement(statement);
        }

        Ok(serialize_statements(&statements))
    }

    /// Unified zero-parse rewrite entry point.
    ///
    /// Accepts pre-parsed statements and pre-extracted predicates. Selects the
    /// right transformer (hierarchical skip index or partition pruning) based on
    /// whether `hierarchical_indexes` / `skip_predicates` are provided.
    ///
    /// **1 parse. 1 walk. 0 re-parses.**
    #[tracing::instrument(
        name = "warehouse.query.rewrite.rewrite_warm_query_ast",
        skip_all,
        err(Display)
    )]
    pub fn rewrite_warm_query_ast(
        &self,
        statements: &mut Vec<Statement>,
        tables: &AHashMap<String, R2TablePath>,
        date_predicates: &AHashMap<String, DateRange>,
        hierarchical_indexes: Option<&AHashMap<String, HierarchicalSkipIndex>>,
        skip_predicates: Option<&SkipPredicates>,
    ) -> RewriteResult<RewriteOutput> {
        let pruning_stats = if let (Some(hi), Some(sp)) = (hierarchical_indexes, skip_predicates) {
            let transformer = HierarchicalSkipIndexTransformer::new(
                &self.collection_name,
                tables,
                hi,
                date_predicates,
                sp,
            );
            let visitor = AstVisitor::new(&transformer);
            for statement in statements.iter_mut() {
                visitor.visit_statement(statement);
            }
            transformer.take_pruning_stats()
        } else {
            let transformer =
                PartitionPruningTransformer::new(&self.collection_name, tables, date_predicates);
            let visitor = AstVisitor::new(&transformer);
            for statement in statements.iter_mut() {
                visitor.visit_statement(statement);
            }
            None
        };

        Ok(RewriteOutput {
            sql: serialize_statements(statements),
            pruning_stats,
        })
    }

    /// Build optimized file patterns for UNION ALL when date range is very large.
    ///
    /// PERFORMANCE: For date ranges >5 years, using individual s3() calls with UNION ALL
    /// is more efficient than a single s3() call with a very long brace expansion pattern.
    /// This avoids ClickHouse pattern parsing overhead.
    ///
    /// Returns a list of patterns that should be wrapped in separate s3() calls and joined
    /// with UNION ALL.
    #[tracing::instrument(
        name = "warehouse.query.rewrite.build_file_patterns_for_union",
        skip_all
    )]
    pub fn build_file_patterns_for_union(
        &self,
        r2_path: &R2TablePath,
        date_predicates: &AHashMap<String, DateRange>,
    ) -> Option<Vec<String>> {
        if !r2_path.date_partitioned {
            return None;
        }

        let partition_col = r2_path.partition_column.as_ref()?;
        let date_range = date_predicates.get(partition_col)?;

        // Only use UNION ALL for very large date ranges
        if !date_range.should_use_union_all() {
            return None;
        }

        Some(date_range.to_pattern_list(&r2_path.prefix))
    }

    /// Check if a query should use UNION ALL optimization for large date ranges.
    ///
    /// Returns true if any table in the query has a date range spanning >5 years.
    #[tracing::instrument(
        name = "warehouse.query.rewrite.should_use_union_all_optimization",
        skip_all
    )]
    pub fn should_use_union_all_optimization(
        tables: &AHashMap<String, R2TablePath>,
        date_predicates: &AHashMap<String, DateRange>,
    ) -> bool {
        for (_, r2_path) in tables {
            if let Some(partition_col) = &r2_path.partition_column {
                if let Some(date_range) = date_predicates.get(partition_col) {
                    if date_range.should_use_union_all() {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Pre-computed analysis for column-pruning rewrites, collected in a single
    /// statement iteration to avoid repeated AST walks.
    fn analyze_for_column_pruning(statements: &[Statement]) -> ColumnPruningAnalysis {
        let mut select_columns: AHashMap<String, Vec<String>> = AHashMap::new();
        let mut has_wildcard = false;
        let mut table_aliases: AHashMap<String, String> = AHashMap::new();
        let mut date_ranges: AHashMap<String, DateRange> = AHashMap::new();
        let mut date_qualifiers: AHashMap<String, Option<String>> = AHashMap::new();
        let mut date_conflicted: AHashSet<String> = AHashSet::new();

        for statement in statements.iter() {
            if let Statement::Query(query) = statement {
                Self::collect_columns_from_query(query, &mut select_columns);
                if !has_wildcard {
                    has_wildcard = Self::query_has_wildcard(query);
                }
                Self::collect_aliases_from_query(query, &mut table_aliases);
                Self::extract_date_predicates_from_query(
                    query,
                    &mut date_ranges,
                    &mut date_qualifiers,
                    &mut date_conflicted,
                );
            }
        }
        for col in &date_conflicted {
            date_ranges.remove(col);
        }

        ColumnPruningAnalysis {
            select_columns,
            has_wildcard,
            table_aliases,
            date_predicates: date_ranges,
        }
    }

    /// Pre-computed analysis for hierarchical-optimization rewrites, collected
    /// in a single statement iteration.
    fn analyze_for_hierarchical(statements: &[Statement]) -> HierarchicalAnalysis {
        let mut date_ranges: AHashMap<String, DateRange> = AHashMap::new();
        let mut date_qualifiers: AHashMap<String, Option<String>> = AHashMap::new();
        let mut date_conflicted: AHashSet<String> = AHashSet::new();
        let mut skip_predicates = SkipPredicates::new();
        let mut skip_qualifiers: AHashMap<String, Option<String>> = AHashMap::new();
        let mut skip_conflicted: AHashSet<String> = AHashSet::new();

        for statement in statements.iter() {
            if let Statement::Query(query) = statement {
                Self::extract_date_predicates_from_query(
                    query,
                    &mut date_ranges,
                    &mut date_qualifiers,
                    &mut date_conflicted,
                );
                Self::extract_predicates_from_query(
                    query,
                    &mut skip_predicates,
                    &mut skip_qualifiers,
                    &mut skip_conflicted,
                );
            }
        }
        for col in &date_conflicted {
            date_ranges.remove(col);
        }
        for col in &skip_conflicted {
            skip_predicates.equality.remove(col);
            skip_predicates.prefix.remove(col);
            skip_predicates.in_lists.remove(col);
            skip_predicates.ranges.remove(col);
            skip_predicates.substring.remove(col);
            skip_predicates.token_search.remove(col);
        }

        HierarchicalAnalysis {
            date_predicates: date_ranges,
            skip_predicates,
        }
    }

    /// Rewrite a query with column pruning optimization.
    ///
    /// PERFORMANCE: This method extracts referenced columns from the query and
    /// includes them in the s3() function's structure parameter. ClickHouse can
    /// then skip reading unused columns from Parquet files.
    ///
    /// For wide tables with 100+ columns where queries only need a few columns,
    /// this can reduce I/O by 90%+ and significantly speed up queries.
    ///
    /// # Arguments
    /// * `sql` - The SQL query to rewrite
    /// * `tables` - Map of table names to R2 paths
    /// * `schemas` - Map of table names to their schemas (column types)
    ///
    /// # Returns
    /// The rewritten SQL query with column hints in s3() calls.
    #[tracing::instrument(
        name = "warehouse.query.rewrite.rewrite_with_column_pruning",
        skip_all,
        err(Display)
    )]
    pub fn rewrite_with_column_pruning(
        &self,
        sql: &str,
        tables: &AHashMap<String, R2TablePath>,
        schemas: &AHashMap<String, TableSchema>,
    ) -> RewriteResult<String> {
        let dialect = ClickHouseDialect {};
        let mut statements = Parser::parse_sql(&dialect, sql)?;
        self.rewrite_with_column_pruning_ast(&mut statements, tables, schemas)
    }

    pub fn rewrite_with_column_pruning_ast(
        &self,
        statements: &mut [Statement],
        tables: &AHashMap<String, R2TablePath>,
        schemas: &AHashMap<String, TableSchema>,
    ) -> RewriteResult<String> {
        let analysis = Self::analyze_for_column_pruning(statements);

        if analysis.has_wildcard {
            return self.rewrite_with_partition_pruning_ast(statements, tables);
        }

        let referenced_columns =
            Self::resolve_column_aliases(&analysis.select_columns, &analysis.table_aliases);

        let transformer = ColumnPruningTransformer::new(
            &self.collection_name,
            tables,
            schemas,
            &referenced_columns,
            &analysis.date_predicates,
        );

        let visitor = AstVisitor::new(&transformer);
        for statement in statements.iter_mut() {
            visitor.visit_statement(statement);
        }

        Ok(serialize_statements(statements))
    }

    /// Check if a query has wildcard selects.
    fn query_has_wildcard(query: &Query) -> bool {
        Self::set_expr_has_wildcard(&query.body)
    }

    /// Check if a set expression has wildcard selects.
    fn set_expr_has_wildcard(set_expr: &SetExpr) -> bool {
        match set_expr {
            SetExpr::Select(select) => {
                for item in &select.projection {
                    match item {
                        sqlparser::ast::SelectItem::Wildcard(_) => return true,
                        sqlparser::ast::SelectItem::QualifiedWildcard(_, _) => return true,
                        _ => {}
                    }
                }
                false
            }
            SetExpr::Query(query) => Self::query_has_wildcard(query),
            SetExpr::SetOperation { left, right, .. } => {
                Self::set_expr_has_wildcard(left) || Self::set_expr_has_wildcard(right)
            }
            _ => false,
        }
    }

    /// Rewrite a query with hierarchical skip index optimization.
    ///
    /// PERFORMANCE: This method uses HierarchicalSkipIndex which supports
    /// partition-aware filtering for TB-scale datasets. It combines:
    /// - Date-based partition pruning
    /// - Value-based skip index filtering with partition hints
    ///
    /// For datasets with millions of files across thousands of partitions,
    /// this reduces search from O(files) to O(partitions_matching) + O(files_in_partition).
    #[tracing::instrument(
        name = "warehouse.query.rewrite.rewrite_with_hierarchical_optimization",
        skip_all,
        err(Display)
    )]
    pub fn rewrite_with_hierarchical_optimization(
        &self,
        sql: &str,
        tables: &AHashMap<String, R2TablePath>,
        hierarchical_indexes: &AHashMap<String, HierarchicalSkipIndex>,
    ) -> RewriteResult<String> {
        let dialect = ClickHouseDialect {};
        let mut statements = Parser::parse_sql(&dialect, sql)?;
        self.rewrite_with_hierarchical_optimization_ast(
            &mut statements,
            tables,
            hierarchical_indexes,
        )
    }

    pub fn rewrite_with_hierarchical_optimization_ast(
        &self,
        statements: &mut [Statement],
        tables: &AHashMap<String, R2TablePath>,
        hierarchical_indexes: &AHashMap<String, HierarchicalSkipIndex>,
    ) -> RewriteResult<String> {
        let analysis = Self::analyze_for_hierarchical(statements);

        let transformer = HierarchicalSkipIndexTransformer::new(
            &self.collection_name,
            tables,
            hierarchical_indexes,
            &analysis.date_predicates,
            &analysis.skip_predicates,
        );
        let visitor = AstVisitor::new(&transformer);

        for statement in statements.iter_mut() {
            visitor.visit_statement(statement);
        }

        Ok(serialize_statements(statements))
    }
    /// Get the list of tables referenced in a query.
    #[tracing::instrument(
        name = "warehouse.query.rewrite.extract_tables",
        skip_all,
        err(Display)
    )]
    pub fn extract_tables(sql: &str) -> RewriteResult<Vec<String>> {
        let dialect = ClickHouseDialect {};
        let statements = Parser::parse_sql(&dialect, sql)?;
        Ok(Self::extract_tables_from_ast(&statements))
    }

    /// Get the list of tables referenced in pre-parsed statements.
    ///
    /// Zero-parse variant of `extract_tables` for use in the optimized pipeline.
    pub fn extract_tables_from_ast(statements: &[Statement]) -> Vec<String> {
        let mut tables = Vec::with_capacity(8);
        for statement in statements {
            Self::collect_tables_from_statement(statement, &mut tables);
        }
        tables
    }

    /// Collect table names from a statement.
    pub fn collect_tables_from_statement(stmt: &Statement, tables: &mut Vec<String>) {
        if let Statement::Query(query) = stmt {
            Self::collect_tables_from_query(query, tables);
        }
    }

    /// Collect table names from a query.
    ///
    /// CTE alias names are excluded from subsequent sibling CTE bodies and the
    /// main query body. For non-recursive CTEs, a CTE's own body is processed
    /// before its name is added to the filter set, so self-referencing CTEs
    /// (same name as a real table) correctly preserve the real table reference.
    /// For `WITH RECURSIVE`, the CTE name is registered *before* visiting the
    /// body so that recursive self-references are not collected as real tables.
    fn collect_tables_from_query(query: &Query, tables: &mut Vec<String>) {
        let mut cte_names: AHashSet<String> = AHashSet::new();

        if let Some(with) = &query.with {
            let is_recursive = with.recursive;
            for cte in &with.cte_tables {
                let name = cte.alias.name.value.clone();

                if is_recursive {
                    cte_names.insert(name);
                    let body_start = tables.len();
                    Self::collect_tables_from_query(&cte.query, tables);
                    let snapshot = tables.split_off(body_start);
                    tables.extend(snapshot.into_iter().filter(|t| !cte_names.contains(t)));
                } else {
                    let body_start = tables.len();
                    Self::collect_tables_from_query(&cte.query, tables);
                    if !cte_names.is_empty() {
                        let snapshot = tables.split_off(body_start);
                        tables.extend(snapshot.into_iter().filter(|t| !cte_names.contains(t)));
                    }
                    cte_names.insert(name);
                }
            }
        }

        let main_start = tables.len();
        Self::collect_tables_from_set_expr(&query.body, tables);

        if !cte_names.is_empty() {
            let snapshot = tables.split_off(main_start);
            tables.extend(snapshot.into_iter().filter(|t| !cte_names.contains(t)));
        }
    }

    /// Collect table names from a set expression.
    fn collect_tables_from_set_expr(set_expr: &SetExpr, tables: &mut Vec<String>) {
        match set_expr {
            SetExpr::Select(select) => {
                for table_with_joins in &select.from {
                    Self::collect_tables_from_table_with_joins(table_with_joins, tables);
                }
                if let Some(ref selection) = select.selection {
                    Self::collect_tables_from_expr(selection, tables);
                }
                if let Some(ref having) = select.having {
                    Self::collect_tables_from_expr(having, tables);
                }
                for item in &select.projection {
                    if let sqlparser::ast::SelectItem::UnnamedExpr(ref expr)
                    | sqlparser::ast::SelectItem::ExprWithAlias { ref expr, .. } = item
                    {
                        Self::collect_tables_from_expr(expr, tables);
                    }
                }
            }
            SetExpr::Query(query) => Self::collect_tables_from_query(query, tables),
            SetExpr::SetOperation { left, right, .. } => {
                Self::collect_tables_from_set_expr(left, tables);
                Self::collect_tables_from_set_expr(right, tables);
            }
            _ => {}
        }
    }

    /// Collect table names from expressions (including subqueries).
    fn collect_tables_from_expr(expr: &Expr, tables: &mut Vec<String>) {
        match expr {
            Expr::Subquery(query) => {
                Self::collect_tables_from_query(query, tables);
            }
            Expr::InSubquery { subquery, .. } => {
                Self::collect_tables_from_query(subquery, tables);
            }
            Expr::Exists { subquery, .. } => {
                Self::collect_tables_from_query(subquery, tables);
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_tables_from_expr(left, tables);
                Self::collect_tables_from_expr(right, tables);
            }
            Expr::UnaryOp { expr, .. } => {
                Self::collect_tables_from_expr(expr, tables);
            }
            Expr::Nested(inner) => {
                Self::collect_tables_from_expr(inner, tables);
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::collect_tables_from_expr(expr, tables);
                Self::collect_tables_from_expr(low, tables);
                Self::collect_tables_from_expr(high, tables);
            }
            Expr::InList { expr, list, .. } => {
                Self::collect_tables_from_expr(expr, tables);
                for item in list {
                    Self::collect_tables_from_expr(item, tables);
                }
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
                ..
            } => {
                if let Some(op) = operand {
                    Self::collect_tables_from_expr(op, tables);
                }
                for cond in conditions {
                    Self::collect_tables_from_expr(cond, tables);
                }
                for res in results {
                    Self::collect_tables_from_expr(res, tables);
                }
                if let Some(else_res) = else_result {
                    Self::collect_tables_from_expr(else_res, tables);
                }
            }
            Expr::Cast { expr, .. } => {
                Self::collect_tables_from_expr(expr, tables);
            }
            Expr::Function(func) => {
                if let FunctionArguments::List(ref list) = func.args {
                    for arg in &list.args {
                        let inner = match arg {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(ref e)) => Some(e),
                            FunctionArg::Named {
                                arg: FunctionArgExpr::Expr(ref e),
                                ..
                            } => Some(e),
                            _ => None,
                        };
                        if let Some(e) = inner {
                            Self::collect_tables_from_expr(e, tables);
                        }
                    }
                }
                if let Some(ref filter_expr) = func.filter {
                    Self::collect_tables_from_expr(filter_expr, tables);
                }
                if let Some(WindowType::WindowSpec(ref spec)) = func.over {
                    for expr in &spec.partition_by {
                        Self::collect_tables_from_expr(expr, tables);
                    }
                    for ob in &spec.order_by {
                        Self::collect_tables_from_expr(&ob.expr, tables);
                    }
                }
            }
            _ => {}
        }
    }

    /// Collect table names from a table with joins.
    fn collect_tables_from_table_with_joins(table: &TableWithJoins, tables: &mut Vec<String>) {
        Self::collect_tables_from_table_factor(&table.relation, tables);
        for join in &table.joins {
            Self::collect_tables_from_table_factor(&join.relation, tables);
            match &join.join_operator {
                sqlparser::ast::JoinOperator::Inner(c)
                | sqlparser::ast::JoinOperator::LeftOuter(c)
                | sqlparser::ast::JoinOperator::RightOuter(c)
                | sqlparser::ast::JoinOperator::FullOuter(c)
                | sqlparser::ast::JoinOperator::LeftSemi(c)
                | sqlparser::ast::JoinOperator::RightSemi(c)
                | sqlparser::ast::JoinOperator::LeftAnti(c)
                | sqlparser::ast::JoinOperator::RightAnti(c) => {
                    if let sqlparser::ast::JoinConstraint::On(expr) = c {
                        Self::collect_tables_from_expr(expr, tables);
                    }
                }
                _ => {}
            }
        }
    }

    /// Collect table names from a table factor.
    ///
    /// Pushes only the full qualified name (e.g. `source.orders`) so that
    /// it matches the `source_name.table_name` keys used in the table maps.
    /// Unqualified references (e.g. `orders`) are pushed as-is.
    fn collect_tables_from_table_factor(factor: &TableFactor, tables: &mut Vec<String>) {
        match factor {
            TableFactor::Table { name, .. } => {
                let full_name = name.to_string();
                if !full_name.is_empty() {
                    tables.push(full_name);
                }
            }
            TableFactor::Derived { subquery, .. } => {
                Self::collect_tables_from_query(subquery, tables);
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                Self::collect_tables_from_table_with_joins(table_with_joins, tables);
            }
            _ => {}
        }
    }

    /// Extract column names referenced in a SELECT statement.
    ///
    /// PERFORMANCE: This can be used to enable column pushdown optimization
    /// by only reading the required columns from Parquet files.
    ///
    /// Returns a map of table_name -> Vec<column_name> for all referenced columns.
    /// Columns without a table qualifier are returned under the empty string key.
    #[tracing::instrument(
        name = "warehouse.query.rewrite.extract_select_columns",
        skip_all,
        err(Display)
    )]
    pub fn extract_select_columns(sql: &str) -> RewriteResult<AHashMap<String, Vec<String>>> {
        let dialect = ClickHouseDialect {};
        let statements = Parser::parse_sql(&dialect, sql)?;
        Ok(Self::extract_select_columns_from_ast(&statements))
    }

    pub fn extract_select_columns_from_ast(
        statements: &[Statement],
    ) -> AHashMap<String, Vec<String>> {
        let mut columns: AHashMap<String, Vec<String>> = AHashMap::new();
        for statement in statements {
            if let Statement::Query(query) = statement {
                Self::collect_columns_from_query(query, &mut columns);
            }
        }
        columns
    }

    /// Extract alias-to-table-name mappings from FROM clauses.
    ///
    /// For `SELECT ... FROM orders o JOIN events e ...`, produces
    /// `{"o" => "orders", "e" => "events"}`.
    fn extract_table_aliases(statements: &[Statement]) -> AHashMap<String, String> {
        let mut aliases = AHashMap::new();
        for statement in statements {
            if let Statement::Query(query) = statement {
                Self::collect_aliases_from_query(query, &mut aliases);
            }
        }
        aliases
    }

    fn collect_aliases_from_query(query: &Query, aliases: &mut AHashMap<String, String>) {
        if let Some(ref with) = query.with {
            for cte in &with.cte_tables {
                Self::collect_aliases_from_query(&cte.query, aliases);
            }
        }
        Self::collect_aliases_from_set_expr(&query.body, aliases);
    }

    fn collect_aliases_from_set_expr(set_expr: &SetExpr, aliases: &mut AHashMap<String, String>) {
        match set_expr {
            SetExpr::Select(select) => {
                for table_with_joins in &select.from {
                    Self::collect_alias_from_table_factor(&table_with_joins.relation, aliases);
                    for join in &table_with_joins.joins {
                        Self::collect_alias_from_table_factor(&join.relation, aliases);
                    }
                }
            }
            SetExpr::SetOperation { left, right, .. } => {
                Self::collect_aliases_from_set_expr(left, aliases);
                Self::collect_aliases_from_set_expr(right, aliases);
            }
            SetExpr::Query(query) => {
                Self::collect_aliases_from_query(query, aliases);
            }
            _ => {}
        }
    }

    fn collect_alias_from_table_factor(
        factor: &TableFactor,
        aliases: &mut AHashMap<String, String>,
    ) {
        if let TableFactor::Table {
            name,
            alias: Some(alias),
            ..
        } = factor
        {
            aliases.insert(alias.name.value.clone(), name.to_string());
        }
    }

    /// Resolve alias references in collected columns to real table names.
    ///
    /// If columns were recorded under alias `"o"` and alias maps to `"orders"`,
    /// merge those columns into the `"orders"` entry.
    fn resolve_column_aliases(
        columns: &AHashMap<String, Vec<String>>,
        aliases: &AHashMap<String, String>,
    ) -> AHashMap<String, Vec<String>> {
        let mut resolved: AHashMap<String, Vec<String>> = AHashMap::new();
        for (key, cols) in columns {
            let real_key = aliases.get(key).cloned().unwrap_or_else(|| key.clone());
            resolved
                .entry(real_key)
                .or_default()
                .extend(cols.iter().cloned());
        }
        resolved
    }

    /// Collect column references from a query, including CTEs.
    fn collect_columns_from_query(query: &Query, columns: &mut AHashMap<String, Vec<String>>) {
        if let Some(ref with) = query.with {
            for cte in &with.cte_tables {
                Self::collect_columns_from_query(&cte.query, columns);
            }
        }

        Self::collect_columns_from_set_expr(&query.body, columns);

        if let Some(ref order_by) = query.order_by {
            for item in &order_by.exprs {
                Self::collect_columns_from_expr(&item.expr, columns);
            }
        }
    }

    /// Collect column references from a set expression.
    fn collect_columns_from_set_expr(
        set_expr: &SetExpr,
        columns: &mut AHashMap<String, Vec<String>>,
    ) {
        match set_expr {
            SetExpr::Select(select) => {
                for item in &select.projection {
                    Self::collect_columns_from_select_item(item, columns);
                }
                if let Some(ref selection) = select.selection {
                    Self::collect_columns_from_expr(selection, columns);
                }
                if let sqlparser::ast::GroupByExpr::Expressions(exprs, _) = &select.group_by {
                    for group_by in exprs {
                        Self::collect_columns_from_expr(group_by, columns);
                    }
                }
                if let Some(ref having) = select.having {
                    Self::collect_columns_from_expr(having, columns);
                }
            }
            SetExpr::Query(query) => Self::collect_columns_from_query(query, columns),
            SetExpr::SetOperation { left, right, .. } => {
                Self::collect_columns_from_set_expr(left, columns);
                Self::collect_columns_from_set_expr(right, columns);
            }
            _ => {}
        }
    }

    /// Collect columns from a SELECT item.
    fn collect_columns_from_select_item(
        item: &sqlparser::ast::SelectItem,
        columns: &mut AHashMap<String, Vec<String>>,
    ) {
        match item {
            sqlparser::ast::SelectItem::UnnamedExpr(expr) => {
                Self::collect_columns_from_expr(expr, columns);
            }
            sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
                Self::collect_columns_from_expr(expr, columns);
            }
            sqlparser::ast::SelectItem::Wildcard(_) => {
                // SELECT * - can't push down columns
            }
            sqlparser::ast::SelectItem::QualifiedWildcard(name, _) => {
                // SELECT table.* - can't push down columns for this table
                let _ = name; // Acknowledge
            }
        }
    }

    /// Collect column references from an expression.
    fn collect_columns_from_expr(expr: &Expr, columns: &mut AHashMap<String, Vec<String>>) {
        match expr {
            Expr::Identifier(ident) => {
                // Unqualified column
                columns
                    .entry(String::new())
                    .or_default()
                    .push(ident.value.clone());
            }
            Expr::CompoundIdentifier(idents) => {
                // Qualified column: table.column
                if idents.len() >= 2 {
                    let table = idents[idents.len() - 2].value.clone();
                    let column = idents[idents.len() - 1].value.clone();
                    columns.entry(table).or_default().push(column);
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_columns_from_expr(left, columns);
                Self::collect_columns_from_expr(right, columns);
            }
            Expr::Function(func) => {
                if let FunctionArguments::List(args) = &func.args {
                    for arg in &args.args {
                        let expr = match arg {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                            FunctionArg::Named {
                                arg: FunctionArgExpr::Expr(e),
                                ..
                            } => Some(e),
                            _ => None,
                        };
                        if let Some(e) = expr {
                            Self::collect_columns_from_expr(e, columns);
                        }
                    }
                }
            }
            Expr::Nested(inner) => {
                Self::collect_columns_from_expr(inner, columns);
            }
            Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
                Self::collect_columns_from_expr(expr, columns);
            }
            Expr::UnaryOp { expr, .. } => {
                Self::collect_columns_from_expr(expr, columns);
            }
            Expr::InList { expr, list, .. } => {
                Self::collect_columns_from_expr(expr, columns);
                for item in list {
                    Self::collect_columns_from_expr(item, columns);
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::collect_columns_from_expr(expr, columns);
                Self::collect_columns_from_expr(low, columns);
                Self::collect_columns_from_expr(high, columns);
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
                ..
            } => {
                if let Some(op) = operand {
                    Self::collect_columns_from_expr(op, columns);
                }
                for cond in conditions {
                    Self::collect_columns_from_expr(cond, columns);
                }
                for res in results {
                    Self::collect_columns_from_expr(res, columns);
                }
                if let Some(el) = else_result {
                    Self::collect_columns_from_expr(el, columns);
                }
            }
            Expr::Cast { expr, .. } => {
                Self::collect_columns_from_expr(expr, columns);
            }
            Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
                Self::collect_columns_from_expr(expr, columns);
                Self::collect_columns_from_expr(pattern, columns);
            }
            Expr::Subquery(query) => {
                Self::collect_columns_from_query(query, columns);
            }
            Expr::InSubquery { subquery, expr, .. } => {
                Self::collect_columns_from_expr(expr, columns);
                Self::collect_columns_from_query(subquery, columns);
            }
            Expr::Exists { subquery, .. } => {
                Self::collect_columns_from_query(subquery, columns);
            }
            _ => {}
        }
    }
}

// =============================================================================
// Type Checking for Federated Queries
// =============================================================================

/// Type checking context for validating cross-source queries.
///
/// This struct provides methods to check type compatibility between columns
/// from different data sources and generate helpful error messages when
/// explicit conversions are needed.
#[derive(Debug, Default)]
pub struct TypeChecker {
    /// Schemas indexed by table name (qualified or unqualified).
    schemas: AHashMap<String, TypedSchema>,
}

impl TypeChecker {
    /// Create a new type checker.
    pub fn new() -> Self {
        Self {
            schemas: AHashMap::new(),
        }
    }

    /// Register a table schema for type checking.
    pub fn register_schema(&mut self, table_name: &str, schema: TypedSchema) {
        self.schemas.insert(table_name.to_string(), schema);
    }

    /// Get a column by fully-qualified name (table.column) or just column name.
    pub fn get_column(&self, table: &str, column: &str) -> Option<&TypedColumn> {
        self.schemas.get(table).and_then(|s| s.get_column(column))
    }

    /// Check if two columns can be compared (e.g., in a JOIN or WHERE clause).
    ///
    /// Returns `Ok(())` if the comparison is valid, or an error with a helpful
    /// message if explicit conversion is required.
    pub fn check_comparison(
        &self,
        left_table: &str,
        left_column: &str,
        right_table: &str,
        right_column: &str,
    ) -> RewriteResult<Option<String>> {
        let left = self.get_column(left_table, left_column);
        let right = self.get_column(right_table, right_column);

        // If we don't have type info for either column, allow the comparison
        // (the database will handle type checking)
        let (left_col, right_col) = match (left, right) {
            (Some(l), Some(r)) => (l, r),
            _ => return Ok(None),
        };

        // Get Arrow types from TypedColumn
        let left_type = left_col.arrow_data_type();
        let right_type = right_col.arrow_data_type();

        let (left_dt, right_dt) = match (left_type, right_type) {
            (Some(l), Some(r)) => (l, r),
            _ => return Ok(None), // Can't parse types, let database handle it
        };

        // Check coercion
        let result = coerce_types(
            &left_dt,
            left_col.semantic.as_ref(),
            &right_dt,
            right_col.semantic.as_ref(),
        );

        match result {
            CoercionResult::Same => Ok(None),
            CoercionResult::AutoCoerce { warning, .. } => Ok(warning),
            CoercionResult::RequiresExplicit { reason, suggestion } => {
                Err(RewriteError::TypeCoercion {
                    message: reason,
                    suggestion,
                    left_column: format!("{}.{}", left_table, left_column),
                    right_column: format!("{}.{}", right_table, right_column),
                })
            }
            CoercionResult::Incompatible { reason } => Err(RewriteError::TypeIncompatible {
                message: reason,
                left_column: format!("{}.{}", left_table, left_column),
                right_column: format!("{}.{}", right_table, right_column),
            }),
        }
    }

    /// Check all JOIN conditions in a query for type compatibility.
    ///
    /// Returns a list of warnings for auto-coercions and errors for
    /// comparisons that require explicit conversion.
    pub fn check_join_types(&self, sql: &str) -> RewriteResult<Vec<String>> {
        let dialect = ClickHouseDialect {};
        let statements = Parser::parse_sql(&dialect, sql)?;
        self.check_join_types_from_ast(&statements)
    }

    pub fn check_join_types_from_ast(
        &self,
        statements: &[Statement],
    ) -> RewriteResult<Vec<String>> {
        let mut warnings = Vec::new();
        for statement in statements {
            if let Statement::Query(query) = statement {
                self.check_query_joins(query, &mut warnings)?;
            }
        }
        Ok(warnings)
    }

    /// Check joins in a query recursively, including CTEs and set operations.
    fn check_query_joins(&self, query: &Query, warnings: &mut Vec<String>) -> RewriteResult<()> {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                self.check_query_joins(&cte.query, warnings)?;
            }
        }

        self.check_set_expr_joins(query.body.as_ref(), warnings)
    }

    fn check_set_expr_joins(
        &self,
        body: &SetExpr,
        warnings: &mut Vec<String>,
    ) -> RewriteResult<()> {
        match body {
            SetExpr::Select(select) => {
                for table_with_joins in &select.from {
                    for join in &table_with_joins.joins {
                        let constraint = match &join.join_operator {
                            sqlparser::ast::JoinOperator::Inner(c)
                            | sqlparser::ast::JoinOperator::LeftOuter(c)
                            | sqlparser::ast::JoinOperator::RightOuter(c)
                            | sqlparser::ast::JoinOperator::FullOuter(c)
                            | sqlparser::ast::JoinOperator::LeftSemi(c)
                            | sqlparser::ast::JoinOperator::RightSemi(c)
                            | sqlparser::ast::JoinOperator::LeftAnti(c)
                            | sqlparser::ast::JoinOperator::RightAnti(c) => Some(c),
                            _ => None,
                        };

                        if let Some(c) = constraint {
                            self.check_join_constraint(c, warnings)?;
                        }
                    }

                    if let sqlparser::ast::TableFactor::Derived { subquery, .. } =
                        &table_with_joins.relation
                    {
                        self.check_query_joins(subquery, warnings)?;
                    }
                    for join in &table_with_joins.joins {
                        if let sqlparser::ast::TableFactor::Derived { subquery, .. } =
                            &join.relation
                        {
                            self.check_query_joins(subquery, warnings)?;
                        }
                    }
                }
            }
            SetExpr::SetOperation { left, right, .. } => {
                self.check_set_expr_joins(left.as_ref(), warnings)?;
                self.check_set_expr_joins(right.as_ref(), warnings)?;
            }
            SetExpr::Query(q) => {
                self.check_query_joins(q, warnings)?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Check a join constraint for type compatibility.
    fn check_join_constraint(
        &self,
        constraint: &sqlparser::ast::JoinConstraint,
        warnings: &mut Vec<String>,
    ) -> RewriteResult<()> {
        if let sqlparser::ast::JoinConstraint::On(expr) = constraint {
            self.check_expr_types(expr, warnings)?;
        }
        Ok(())
    }

    /// Check types in an expression (e.g., a = b, a.x = b.y).
    fn check_expr_types(&self, expr: &Expr, warnings: &mut Vec<String>) -> RewriteResult<()> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                // Check comparison operators
                if matches!(
                    op,
                    BinaryOperator::Eq
                        | BinaryOperator::NotEq
                        | BinaryOperator::Lt
                        | BinaryOperator::LtEq
                        | BinaryOperator::Gt
                        | BinaryOperator::GtEq
                ) {
                    // Extract column references
                    if let (Some((lt, lc)), Some((rt, rc))) =
                        (extract_column_ref(left), extract_column_ref(right))
                    {
                        if let Some(warning) = self.check_comparison(&lt, &lc, &rt, &rc)? {
                            warnings.push(warning);
                        }
                    }
                }

                // Check AND/OR subexpressions
                if matches!(op, BinaryOperator::And | BinaryOperator::Or) {
                    self.check_expr_types(left, warnings)?;
                    self.check_expr_types(right, warnings)?;
                }
            }
            Expr::Nested(inner) => {
                self.check_expr_types(inner, warnings)?;
            }
            _ => {}
        }
        Ok(())
    }
}

/// Extract table and column from a column reference expression.
fn extract_column_ref(expr: &Expr) -> Option<(String, String)> {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let table = parts[parts.len() - 2].value.clone();
            let column = parts[parts.len() - 1].value.clone();
            Some((table, column))
        }
        Expr::Identifier(ident) => Some(("".to_string(), ident.value.clone())),
        _ => None,
    }
}

/// Type information for one side of a comparison, used in error messages.
pub struct TypeInfo<'a> {
    pub table: &'a str,
    pub column: &'a str,
    pub data_type: &'a str,
    pub semantic_type: Option<&'a SemanticType>,
}

/// Generate a helpful error message for a type coercion issue.
pub fn format_type_error(left: &TypeInfo, right: &TypeInfo) -> String {
    let mut msg = format!(
        "Cannot compare {}.{} ({}) with {}.{} ({})",
        left.table, left.column, left.data_type, right.table, right.column, right.data_type
    );

    // Add semantic context if available
    if let (Some(left_sem), Some(right_sem)) = (left.semantic_type, right.semantic_type) {
        match (left_sem, right_sem) {
            (
                SemanticType::Money { in_cents: true, .. },
                SemanticType::Money {
                    in_cents: false, ..
                },
            ) => {
                msg.push_str("\n\nThe left column is in cents while the right is in dollars.");
                msg.push_str("\nHint: Use cents_to_dollars() or dollars_to_cents() to convert.");
            }
            (
                SemanticType::Money {
                    in_cents: false, ..
                },
                SemanticType::Money { in_cents: true, .. },
            ) => {
                msg.push_str("\n\nThe left column is in dollars while the right is in cents.");
                msg.push_str("\nHint: Use cents_to_dollars() or dollars_to_cents() to convert.");
            }
            _ => {}
        }
    }

    msg
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::warehouse::types::SourceType;

    #[test]
    fn test_extract_tables() {
        let sql = "SELECT * FROM customers WHERE id = 1";
        let tables = TableRewriter::extract_tables(sql).unwrap();
        assert_eq!(tables, vec!["customers"]);
    }

    #[test]
    fn test_extract_tables_with_join() {
        let sql = "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id";
        let tables = TableRewriter::extract_tables(sql).unwrap();
        assert_eq!(tables, vec!["orders", "customers"]);
    }

    #[test]
    fn test_extract_tables_with_subquery() {
        let sql = "SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers)";
        let tables = TableRewriter::extract_tables(sql).unwrap();
        assert!(tables.contains(&"orders".to_string()));
        assert!(tables.contains(&"customers".to_string()));
    }

    #[test]
    fn test_extract_tables_with_cte() {
        let sql = "WITH recent AS (SELECT * FROM orders WHERE created > '2025-01-01') SELECT * FROM recent JOIN customers c ON recent.customer_id = c.id";
        let tables = TableRewriter::extract_tables(sql).unwrap();
        assert!(
            tables.contains(&"orders".to_string()),
            "Real table 'orders' inside CTE body must be collected: {:?}",
            tables,
        );
        assert!(
            tables.contains(&"customers".to_string()),
            "Real table 'customers' in main query must be collected: {:?}",
            tables,
        );
        assert!(
            !tables.contains(&"recent".to_string()),
            "CTE alias 'recent' must NOT appear in collected tables: {:?}",
            tables,
        );
    }

    #[test]
    fn test_extract_tables_self_referencing_cte() {
        let sql = "WITH users AS (SELECT * FROM users WHERE active = true) SELECT * FROM users";
        let tables = TableRewriter::extract_tables(sql).unwrap();
        assert!(
            tables.contains(&"users".to_string()),
            "Real table 'users' inside CTE body must be preserved: {:?}",
            tables,
        );
    }

    #[test]
    fn test_rewrite_simple_query() {
        let rewriter = TableRewriter::new("r2_my_bucket");

        let mut tables = AHashMap::new();
        tables.insert(
            "customers".to_string(),
            R2TablePath::for_testing("stripe/customers"),
        );

        let sql = "SELECT * FROM customers WHERE id = 1";
        let result = rewriter.rewrite(sql, &tables).unwrap();

        // Should use named collection, not embedded credentials
        assert!(result.contains("s3("));
        assert!(result.contains("r2_my_bucket"));
        assert!(result.contains("stripe/customers/*.parquet"));
        assert!(result.contains("Parquet"));
        // Should NOT contain any credential placeholders
        assert!(!result.contains("access-key"));
        assert!(!result.contains("secret-key"));
    }

    #[test]
    fn test_from_r2_bucket() {
        let rewriter = TableRewriter::from_r2_bucket("my-bucket");

        // Collection name should be derived from bucket
        let mut tables = AHashMap::new();
        tables.insert("users".to_string(), R2TablePath::for_testing("app/users"));

        let sql = "SELECT * FROM users";
        let result = rewriter.rewrite(sql, &tables).unwrap();

        assert!(result.contains("r2_my_bucket"));
    }

    // ==================== CTE Rewriting Tests ====================

    #[test]
    fn test_extract_tables_with_simple_cte() {
        let sql = "WITH active_users AS (SELECT * FROM users WHERE status = 'active') SELECT * FROM active_users";
        let tables = TableRewriter::extract_tables(sql).unwrap();
        // Should extract the underlying table from CTE, not the CTE alias
        assert!(tables.contains(&"users".to_string()));
    }

    #[test]
    fn test_extract_tables_with_multiple_ctes() {
        let sql = r#"
            WITH 
                active_users AS (SELECT * FROM users WHERE status = 'active'),
                recent_orders AS (SELECT * FROM orders WHERE created_at > '2024-01-01')
            SELECT u.name, o.total 
            FROM active_users u 
            JOIN recent_orders o ON u.id = o.user_id
        "#;
        let tables = TableRewriter::extract_tables(sql).unwrap();
        assert!(tables.contains(&"users".to_string()));
        assert!(tables.contains(&"orders".to_string()));
    }

    #[test]
    fn test_extract_tables_with_recursive_cte() {
        let sql = r#"
            WITH RECURSIVE category_tree AS (
                SELECT id, name, parent_id FROM categories WHERE parent_id IS NULL
                UNION ALL
                SELECT c.id, c.name, c.parent_id FROM categories c
                JOIN category_tree ct ON c.parent_id = ct.id
            )
            SELECT * FROM category_tree
        "#;
        let tables = TableRewriter::extract_tables(sql).unwrap();
        assert!(
            tables.contains(&"categories".to_string()),
            "Real table 'categories' must be extracted: {:?}",
            tables
        );
        assert!(
            !tables.contains(&"category_tree".to_string()),
            "Recursive CTE name 'category_tree' must NOT appear as a real table: {:?}",
            tables
        );
    }

    #[test]
    fn test_rewrite_query_with_cte() {
        let rewriter = TableRewriter::new("r2_my_bucket");

        let mut tables = AHashMap::new();
        tables.insert("users".to_string(), R2TablePath::for_testing("data/users"));
        tables.insert(
            "orders".to_string(),
            R2TablePath::for_testing("data/orders"),
        );

        let sql = r#"
            WITH vip_users AS (
                SELECT * FROM users WHERE tier = 'vip'
            )
            SELECT u.name, COUNT(o.id) as order_count
            FROM vip_users u
            JOIN orders o ON u.id = o.user_id
            GROUP BY u.name
        "#;

        let result = rewriter.rewrite(sql, &tables).unwrap();

        // Should rewrite tables inside CTE and main query
        assert!(result.contains("s3("));
        assert!(result.contains("data/users"));
        assert!(result.contains("data/orders"));
    }

    // ==================== Complex Join Pattern Tests ====================

    #[test]
    fn test_extract_tables_with_self_join() {
        let sql = "SELECT e1.name, e2.name as manager FROM employees e1 JOIN employees e2 ON e1.manager_id = e2.id";
        let tables = TableRewriter::extract_tables(sql).unwrap();
        // Self-join should still only list the table once
        assert!(tables.contains(&"employees".to_string()));
    }

    #[test]
    fn test_extract_tables_with_multi_join() {
        let sql = r#"
            SELECT o.id, c.name, p.title, s.name as shipper
            FROM orders o
            JOIN customers c ON o.customer_id = c.id
            JOIN order_items oi ON o.id = oi.order_id
            JOIN products p ON oi.product_id = p.id
            LEFT JOIN shippers s ON o.shipper_id = s.id
        "#;
        let tables = TableRewriter::extract_tables(sql).unwrap();
        assert!(tables.contains(&"orders".to_string()));
        assert!(tables.contains(&"customers".to_string()));
        assert!(tables.contains(&"order_items".to_string()));
        assert!(tables.contains(&"products".to_string()));
        assert!(tables.contains(&"shippers".to_string()));
    }

    #[test]
    fn test_rewrite_multi_table_join() {
        let rewriter = TableRewriter::new("r2_my_bucket");

        let mut tables = AHashMap::new();
        tables.insert(
            "orders".to_string(),
            R2TablePath::for_testing("data/orders"),
        );
        tables.insert(
            "customers".to_string(),
            R2TablePath::for_testing("data/customers"),
        );
        tables.insert(
            "products".to_string(),
            R2TablePath::for_testing("data/products"),
        );

        let sql = r#"
            SELECT o.id, c.name, p.title
            FROM orders o
            INNER JOIN customers c ON o.customer_id = c.id
            LEFT JOIN products p ON o.product_id = p.id
            WHERE c.country = 'USA'
        "#;

        let result = rewriter.rewrite(sql, &tables).unwrap();

        // All three tables should be rewritten to s3() calls
        assert!(result.contains("s3("));
        assert!(result.contains("data/orders"));
        assert!(result.contains("data/customers"));
        assert!(result.contains("data/products"));
    }

    #[test]
    fn test_extract_tables_with_cross_join() {
        let sql = "SELECT * FROM sizes CROSS JOIN colors";
        let tables = TableRewriter::extract_tables(sql).unwrap();
        assert!(tables.contains(&"sizes".to_string()));
        assert!(tables.contains(&"colors".to_string()));
    }

    #[test]
    fn test_extract_tables_with_subquery_in_join() {
        let sql = r#"
            SELECT o.*, sub.total_spent
            FROM orders o
            JOIN (
                SELECT customer_id, SUM(amount) as total_spent
                FROM payments
                GROUP BY customer_id
            ) sub ON o.customer_id = sub.customer_id
        "#;
        let tables = TableRewriter::extract_tables(sql).unwrap();
        assert!(tables.contains(&"orders".to_string()));
        assert!(tables.contains(&"payments".to_string()));
    }

    #[test]
    fn test_rewrite_query_with_derived_table() {
        let rewriter = TableRewriter::new("r2_my_bucket");

        let mut tables = AHashMap::new();
        tables.insert(
            "orders".to_string(),
            R2TablePath::for_testing("data/orders"),
        );
        tables.insert(
            "payments".to_string(),
            R2TablePath::for_testing("data/payments"),
        );

        let sql = r#"
            SELECT o.id, sub.total
            FROM orders o
            JOIN (SELECT order_id, SUM(amount) as total FROM payments GROUP BY order_id) sub
            ON o.id = sub.order_id
        "#;

        let result = rewriter.rewrite(sql, &tables).unwrap();

        assert!(result.contains("s3("));
        assert!(result.contains("data/orders"));
        assert!(result.contains("data/payments"));
    }

    // ==================== NULL and Edge Case Tests ====================

    #[test]
    fn test_extract_tables_with_null_comparison() {
        let sql = "SELECT * FROM orders WHERE deleted_at IS NULL";
        let tables = TableRewriter::extract_tables(sql).unwrap();
        assert!(tables.contains(&"orders".to_string()));
    }

    #[test]
    fn test_extract_tables_with_coalesce() {
        let sql = "SELECT COALESCE(name, 'Unknown') FROM customers";
        let tables = TableRewriter::extract_tables(sql).unwrap();
        assert!(tables.contains(&"customers".to_string()));
    }

    #[test]
    fn test_rewrite_empty_tables_map() {
        let rewriter = TableRewriter::new("r2_my_bucket");

        let tables = AHashMap::new();
        let sql = "SELECT * FROM unknown_table";

        let result = rewriter.rewrite(sql, &tables);
        // Should succeed but not rewrite unknown tables
        assert!(result.is_ok());
    }

    #[test]
    fn test_extract_tables_with_union() {
        let sql = r#"
            SELECT id, name FROM customers
            UNION ALL
            SELECT id, name FROM suppliers
        "#;
        let tables = TableRewriter::extract_tables(sql).unwrap();
        assert!(tables.contains(&"customers".to_string()));
        assert!(tables.contains(&"suppliers".to_string()));
    }

    #[test]
    fn test_rewrite_union_query() {
        let rewriter = TableRewriter::new("r2_my_bucket");

        let mut tables = AHashMap::new();
        tables.insert(
            "customers".to_string(),
            R2TablePath::for_testing("data/customers"),
        );
        tables.insert(
            "suppliers".to_string(),
            R2TablePath::for_testing("data/suppliers"),
        );

        let sql = r#"
            SELECT id, name FROM customers
            UNION ALL
            SELECT id, name FROM suppliers
        "#;

        let result = rewriter.rewrite(sql, &tables).unwrap();

        // Both sides of UNION should be rewritten
        assert!(result.contains("data/customers"));
        assert!(result.contains("data/suppliers"));
    }

    // ==================== extract_date_predicates Tests ====================

    #[test]
    fn test_extract_date_predicates_simple_range() {
        let sql = "SELECT * FROM t WHERE date >= '2025-01-01' AND date <= '2025-01-31'";
        let result = TableRewriter::extract_date_predicates(sql).unwrap();
        let range = result.get("date").expect("date column should be present");
        assert!(range.start.is_some());
        assert!(range.end.is_some());
        assert_eq!(range.start.unwrap().to_string(), "2025-01-01");
        assert_eq!(range.end.unwrap().to_string(), "2025-01-31");
    }

    #[test]
    fn test_extract_date_predicates_between() {
        let sql = "SELECT * FROM t WHERE date BETWEEN '2025-01-01' AND '2025-01-31'";
        let result = TableRewriter::extract_date_predicates(sql).unwrap();
        let range = result.get("date").expect("date column should be present");
        assert_eq!(range.start.unwrap().to_string(), "2025-01-01");
        assert_eq!(range.end.unwrap().to_string(), "2025-01-31");
    }

    #[test]
    fn test_extract_date_predicates_single_bound() {
        let sql = "SELECT * FROM t WHERE date > '2025-06-01'";
        let result = TableRewriter::extract_date_predicates(sql).unwrap();
        let range = result.get("date").expect("date column should be present");
        assert!(range.start.is_some());
        assert!(range.end.is_none());
    }

    #[test]
    fn test_extract_date_predicates_no_predicates() {
        let sql = "SELECT * FROM t";
        let result = TableRewriter::extract_date_predicates(sql).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_extract_date_predicates_non_date_value() {
        let sql = "SELECT * FROM t WHERE id > 42";
        let result = TableRewriter::extract_date_predicates(sql).unwrap();
        // Numeric literal should not be extracted as a date
        assert!(result.is_empty());
    }

    #[test]
    fn test_extract_date_predicates_multiple_columns() {
        let sql = "SELECT * FROM t WHERE created_at >= '2025-01-01' AND updated_at <= '2025-06-01'";
        let result = TableRewriter::extract_date_predicates(sql).unwrap();
        assert!(result.contains_key("created_at"));
        assert!(result.contains_key("updated_at"));
    }

    #[test]
    fn test_extract_date_predicates_union_not_extracted() {
        // UNION branches may reference different tables, so merging date
        // ranges across branches can incorrectly prune one branch's partitions.
        let sql = r#"
            SELECT * FROM t1 WHERE date >= '2025-01-01'
            UNION ALL
            SELECT * FROM t2 WHERE date <= '2025-06-01'
        "#;
        let result = TableRewriter::extract_date_predicates(sql).unwrap();
        assert!(
            result.is_empty(),
            "Date predicates from UNION branches should not be combined"
        );
    }

    #[test]
    fn test_extract_date_predicates_cte_traversed() {
        let sql = r#"
            WITH filtered AS (
                SELECT * FROM events WHERE event_date >= '2025-03-01' AND event_date <= '2025-03-31'
            )
            SELECT * FROM filtered
        "#;
        let result = TableRewriter::extract_date_predicates(sql).unwrap();
        assert!(
            result.contains_key("event_date"),
            "Date predicates inside CTEs must be extracted for partition pruning: {:?}",
            result,
        );
        let range = result.get("event_date").unwrap();
        assert_eq!(range.start.unwrap().to_string(), "2025-03-01");
        assert_eq!(range.end.unwrap().to_string(), "2025-03-31");
    }

    #[test]
    fn test_extract_date_predicates_between_combined_with_comparison() {
        // BETWEEN should merge with existing tighter bounds, not overwrite them.
        // GtEq before BETWEEN: the tighter start must survive.
        let sql = "SELECT * FROM t WHERE date >= '2025-03-01' AND date BETWEEN '2025-01-01' AND '2025-06-30'";
        let result = TableRewriter::extract_date_predicates(sql).unwrap();
        let range = result.get("date").expect("date column should be present");
        assert_eq!(
            range.start.unwrap().to_string(),
            "2025-03-01",
            "Tighter GtEq start should be preserved when combined with BETWEEN"
        );
        assert_eq!(
            range.end.unwrap().to_string(),
            "2025-06-30",
            "BETWEEN end should be used when no tighter end exists"
        );
    }

    #[test]
    fn test_extract_date_predicates_between_combined_with_comparison_reversed() {
        // Same semantics regardless of operand order.
        let sql = "SELECT * FROM t WHERE date BETWEEN '2025-01-01' AND '2025-06-30' AND date <= '2025-04-15'";
        let result = TableRewriter::extract_date_predicates(sql).unwrap();
        let range = result.get("date").expect("date column should be present");
        assert_eq!(range.start.unwrap().to_string(), "2025-01-01");
        assert_eq!(
            range.end.unwrap().to_string(),
            "2025-04-15",
            "Tighter LtEq end should be preserved when combined with BETWEEN"
        );
    }

    #[test]
    fn test_extract_date_predicates_between_tighter_both_bounds() {
        // Comparison predicates tighter on both sides than BETWEEN.
        let sql = "SELECT * FROM t WHERE date >= '2025-03-01' AND date <= '2025-04-30' AND date BETWEEN '2025-01-01' AND '2025-12-31'";
        let result = TableRewriter::extract_date_predicates(sql).unwrap();
        let range = result.get("date").expect("date column should be present");
        assert_eq!(range.start.unwrap().to_string(), "2025-03-01");
        assert_eq!(range.end.unwrap().to_string(), "2025-04-30");
    }

    // ==================== extract_skip_predicates Tests ====================

    #[test]
    fn test_extract_skip_predicates_equality() {
        let sql = "SELECT * FROM t WHERE status = 'active'";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        assert_eq!(result.equality.get("status").unwrap(), "active");
    }

    #[test]
    fn test_extract_skip_predicates_in_list() {
        let sql = "SELECT * FROM t WHERE region IN ('us', 'eu', 'asia')";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        let values = result.in_lists.get("region").expect("region IN list");
        assert_eq!(values.len(), 3);
        assert!(values.contains(&"us".to_string()));
        assert!(values.contains(&"eu".to_string()));
        assert!(values.contains(&"asia".to_string()));
    }

    #[test]
    fn test_extract_skip_predicates_like_prefix() {
        let sql = "SELECT * FROM t WHERE name LIKE 'foo%'";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        assert_eq!(result.prefix.get("name").unwrap(), "foo");
    }

    #[test]
    fn test_extract_skip_predicates_like_leading_wildcard_not_extracted() {
        let sql = "SELECT * FROM t WHERE name LIKE '%foo'";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        // Leading wildcard can't use skip index
        assert!(result.prefix.is_empty());
    }

    #[test]
    fn test_extract_skip_predicates_like_prefix_with_internal_wildcards_not_extracted() {
        let sql = "SELECT * FROM t WHERE name LIKE 'foo%bar%'";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        assert!(
            result.prefix.is_empty(),
            "LIKE pattern with internal wildcards must not produce a prefix predicate"
        );
    }

    #[test]
    fn test_extract_skip_predicates_like_prefix_with_internal_underscore_not_extracted() {
        let sql = "SELECT * FROM t WHERE name LIKE 'foo_bar%'";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        assert!(
            result.prefix.is_empty(),
            "LIKE pattern with internal single-char wildcard must not produce a prefix predicate"
        );
    }

    #[test]
    fn test_extract_skip_predicates_negated_like_not_extracted() {
        let sql = "SELECT * FROM t WHERE name NOT LIKE 'foo%'";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        assert!(result.prefix.is_empty());
    }

    #[test]
    fn test_extract_skip_predicates_and_compound() {
        let sql = "SELECT * FROM t WHERE status = 'active' AND region = 'us'";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        assert_eq!(result.equality.get("status").unwrap(), "active");
        assert_eq!(result.equality.get("region").unwrap(), "us");
    }

    #[test]
    fn test_extract_skip_predicates_no_predicates() {
        let sql = "SELECT 1";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        assert!(result.equality.is_empty());
        assert!(result.prefix.is_empty());
        assert!(result.in_lists.is_empty());
    }

    #[test]
    fn test_extract_skip_predicates_reversed_equality() {
        let sql = "SELECT * FROM t WHERE 'active' = status";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        assert_eq!(result.equality.get("status").unwrap(), "active");
    }

    // ==================== extract_select_columns Tests ====================

    #[test]
    fn test_extract_select_columns_explicit() {
        let sql = "SELECT a, b FROM t";
        let result = TableRewriter::extract_select_columns(sql).unwrap();
        let unqualified = result.get("").expect("unqualified columns");
        assert!(unqualified.contains(&"a".to_string()));
        assert!(unqualified.contains(&"b".to_string()));
    }

    #[test]
    fn test_extract_select_columns_qualified() {
        let sql = "SELECT t.a, t.b FROM t";
        let result = TableRewriter::extract_select_columns(sql).unwrap();
        let qualified = result.get("t").expect("qualified columns for t");
        assert!(qualified.contains(&"a".to_string()));
        assert!(qualified.contains(&"b".to_string()));
    }

    #[test]
    fn test_extract_select_columns_where_clause() {
        let sql = "SELECT a FROM t WHERE b > 5";
        let result = TableRewriter::extract_select_columns(sql).unwrap();
        let cols = result.get("").expect("unqualified columns");
        assert!(cols.contains(&"a".to_string()));
        assert!(cols.contains(&"b".to_string()));
    }

    #[test]
    fn test_extract_select_columns_group_by() {
        let sql = "SELECT a, COUNT(*) FROM t GROUP BY a";
        let result = TableRewriter::extract_select_columns(sql).unwrap();
        let cols = result.get("").expect("unqualified columns");
        assert!(cols.contains(&"a".to_string()));
    }

    #[test]
    fn test_extract_select_columns_mixed_qualified_unqualified() {
        let sql = "SELECT t.a, b FROM t";
        let result = TableRewriter::extract_select_columns(sql).unwrap();
        assert!(result
            .get("t")
            .expect("qualified")
            .contains(&"a".to_string()));
        assert!(result
            .get("")
            .expect("unqualified")
            .contains(&"b".to_string()));
    }

    #[test]
    fn test_extract_select_columns_having() {
        let sql = "SELECT region, COUNT(*) FROM t GROUP BY region HAVING COUNT(order_id) > 5";
        let result = TableRewriter::extract_select_columns(sql).unwrap();
        let cols = result.get("").expect("unqualified columns");
        assert!(cols.contains(&"region".to_string()));
        assert!(
            cols.contains(&"order_id".to_string()),
            "Columns in HAVING should be collected"
        );
    }

    #[test]
    fn test_extract_select_columns_order_by() {
        let sql = "SELECT name FROM t ORDER BY created_at";
        let result = TableRewriter::extract_select_columns(sql).unwrap();
        let cols = result.get("").expect("unqualified columns");
        assert!(cols.contains(&"name".to_string()));
        assert!(
            cols.contains(&"created_at".to_string()),
            "Columns in ORDER BY should be collected"
        );
    }

    #[test]
    fn test_extract_select_columns_order_by_qualified() {
        let sql = "SELECT t.name FROM t ORDER BY t.created_at DESC";
        let result = TableRewriter::extract_select_columns(sql).unwrap();
        let cols = result.get("t").expect("qualified columns for t");
        assert!(cols.contains(&"name".to_string()));
        assert!(
            cols.contains(&"created_at".to_string()),
            "Qualified ORDER BY columns should be collected"
        );
    }

    #[test]
    fn test_extract_select_columns_is_null() {
        let sql = "SELECT a FROM t WHERE b IS NULL";
        let result = TableRewriter::extract_select_columns(sql).unwrap();
        let cols = result.get("").expect("unqualified columns");
        assert!(cols.contains(&"a".to_string()));
        assert!(
            cols.contains(&"b".to_string()),
            "Columns in IS NULL should be collected"
        );
    }

    #[test]
    fn test_extract_select_columns_is_not_null() {
        let sql = "SELECT a FROM t WHERE b IS NOT NULL";
        let result = TableRewriter::extract_select_columns(sql).unwrap();
        let cols = result.get("").expect("unqualified columns");
        assert!(
            cols.contains(&"b".to_string()),
            "Columns in IS NOT NULL should be collected"
        );
    }

    #[test]
    fn test_extract_select_columns_in_list() {
        let sql = "SELECT a FROM t WHERE status IN ('x', 'y')";
        let result = TableRewriter::extract_select_columns(sql).unwrap();
        let cols = result.get("").expect("unqualified columns");
        assert!(
            cols.contains(&"status".to_string()),
            "Columns in IN list should be collected"
        );
    }

    #[test]
    fn test_extract_select_columns_between() {
        let sql = "SELECT a FROM t WHERE price BETWEEN 10 AND 100";
        let result = TableRewriter::extract_select_columns(sql).unwrap();
        let cols = result.get("").expect("unqualified columns");
        assert!(
            cols.contains(&"price".to_string()),
            "Columns in BETWEEN should be collected"
        );
    }

    #[test]
    fn test_extract_select_columns_case() {
        let sql = "SELECT CASE WHEN a > 1 THEN b ELSE c END FROM t";
        let result = TableRewriter::extract_select_columns(sql).unwrap();
        let cols = result.get("").expect("unqualified columns");
        assert!(
            cols.contains(&"a".to_string()),
            "CASE WHEN condition column"
        );
        assert!(cols.contains(&"b".to_string()), "CASE THEN column");
        assert!(cols.contains(&"c".to_string()), "CASE ELSE column");
    }

    #[test]
    fn test_extract_select_columns_cast() {
        let sql = "SELECT CAST(a AS INTEGER) FROM t";
        let result = TableRewriter::extract_select_columns(sql).unwrap();
        let cols = result.get("").expect("unqualified columns");
        assert!(
            cols.contains(&"a".to_string()),
            "Columns in CAST should be collected"
        );
    }

    #[test]
    fn test_extract_select_columns_like() {
        let sql = "SELECT a FROM t WHERE name LIKE 'foo%'";
        let result = TableRewriter::extract_select_columns(sql).unwrap();
        let cols = result.get("").expect("unqualified columns");
        assert!(
            cols.contains(&"name".to_string()),
            "Columns in LIKE should be collected"
        );
    }

    #[test]
    fn test_extract_select_columns_unary_not() {
        let sql = "SELECT a FROM t WHERE NOT active";
        let result = TableRewriter::extract_select_columns(sql).unwrap();
        let cols = result.get("").expect("unqualified columns");
        assert!(
            cols.contains(&"active".to_string()),
            "Columns in NOT (unary op) should be collected"
        );
    }

    // ==================== format_type_error Tests ====================

    #[test]
    fn test_format_type_error_basic_mismatch() {
        let left = TypeInfo {
            table: "orders",
            column: "amount",
            data_type: "Int64",
            semantic_type: None,
        };
        let right = TypeInfo {
            table: "products",
            column: "name",
            data_type: "String",
            semantic_type: None,
        };
        let msg = format_type_error(&left, &right);
        assert!(msg.contains("orders.amount"));
        assert!(msg.contains("products.name"));
        assert!(msg.contains("Int64"));
        assert!(msg.contains("String"));
        // No semantic hint when types have no semantic info
        assert!(!msg.contains("Hint"));
    }

    #[test]
    fn test_format_type_error_money_cents_vs_dollars() {
        let left = TypeInfo {
            table: "stripe",
            column: "amount",
            data_type: "Int64",
            semantic_type: Some(&SemanticType::Money {
                currency: Some("USD".to_string()),
                in_cents: true,
            }),
        };
        let right = TypeInfo {
            table: "invoices",
            column: "total",
            data_type: "Float64",
            semantic_type: Some(&SemanticType::Money {
                currency: Some("USD".to_string()),
                in_cents: false,
            }),
        };
        let msg = format_type_error(&left, &right);
        assert!(msg.contains("cents"));
        assert!(msg.contains("dollars"));
        assert!(msg.contains("Hint"));
    }

    #[test]
    fn test_format_type_error_no_semantic_types() {
        let left = TypeInfo {
            table: "a",
            column: "x",
            data_type: "UInt32",
            semantic_type: None,
        };
        let right = TypeInfo {
            table: "b",
            column: "y",
            data_type: "Float64",
            semantic_type: None,
        };
        let msg = format_type_error(&left, &right);
        assert!(msg.contains("Cannot compare"));
        assert!(!msg.contains("Hint"));
    }

    // ===== LIKE '%term%' Substring Extraction Tests =====

    #[test]
    fn test_extract_skip_predicates_like_substring() {
        let sql = "SELECT * FROM t WHERE name LIKE '%foo%'";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        let subs = result
            .substring
            .get("name")
            .expect("name should have substring");
        assert_eq!(subs, &vec!["foo".to_string()]);
    }

    #[test]
    fn test_extract_skip_predicates_like_substring_with_inner_wildcard_rejected() {
        let sql = "SELECT * FROM t WHERE name LIKE '%foo%bar%'";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        assert!(result.substring.is_empty(), "Inner % should be rejected");
    }

    #[test]
    fn test_extract_skip_predicates_like_substring_with_underscore_rejected() {
        let sql = "SELECT * FROM t WHERE name LIKE '%fo_o%'";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        assert!(
            result.substring.is_empty(),
            "Underscore wildcard should be rejected"
        );
    }

    #[test]
    fn test_extract_skip_predicates_like_substring_and_equality() {
        let sql = "SELECT * FROM t WHERE status = 'active' AND name LIKE '%foo%'";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        assert_eq!(result.equality.get("status").unwrap(), "active");
        let subs = result
            .substring
            .get("name")
            .expect("name should have substring");
        assert_eq!(subs, &vec!["foo".to_string()]);
    }

    #[test]
    fn test_extract_skip_predicates_multiple_substring_same_column() {
        let sql = "SELECT * FROM t WHERE name LIKE '%foo%' AND name LIKE '%bar%'";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        let subs = result
            .substring
            .get("name")
            .expect("name should have substrings");
        assert_eq!(subs.len(), 2);
        assert!(subs.contains(&"foo".to_string()));
        assert!(subs.contains(&"bar".to_string()));
    }

    // ===== Copy-on-Write: No query-time dedup needed =====

    fn make_r2_path(prefix: &str) -> R2TablePath {
        R2TablePath {
            prefix: prefix.to_string(),
            project_id: None,
            date_partitioned: false,
            partition_column: None,
            detected_partition_scheme: None,
            buffer_ch_table: None,
        }
    }

    #[test]
    fn test_rewrite_produces_plain_s3_no_dedup() {
        let mut tables = AHashMap::new();
        tables.insert("users".to_string(), make_r2_path("project/users"));

        let rewriter = TableRewriter::new("test_collection");
        let result = rewriter
            .rewrite("SELECT id, name FROM users", &tables)
            .unwrap();

        assert!(
            result.contains("s3("),
            "Should contain s3() call: {}",
            result
        );
        assert!(
            !result.contains("argMax"),
            "Should NOT contain argMax: {}",
            result
        );
        assert!(
            !result.contains("GROUP BY"),
            "Should NOT contain GROUP BY: {}",
            result
        );
    }

    #[test]
    fn test_extract_skip_predicates_or_branch_not_extracted() {
        let sql = "SELECT * FROM t WHERE status = 'active' OR name = 'john'";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        assert!(
            result.equality.is_empty(),
            "OR predicates must not be extracted for skip indexes: {:?}",
            result.equality
        );
    }

    #[test]
    fn test_extract_skip_predicates_or_nested_in_and() {
        let sql =
            "SELECT * FROM t WHERE region = 'us' AND (status = 'active' OR status = 'pending')";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        assert_eq!(
            result.equality.get("region").unwrap(),
            "us",
            "AND-level predicates should still be extracted"
        );
        assert!(
            result.equality.get("status").is_none(),
            "OR-level predicates must not be extracted"
        );
    }

    #[test]
    fn test_extract_skip_predicates_function_wrapped_column_not_extracted() {
        let sql = "SELECT * FROM t WHERE UPPER(status) = 'ACTIVE'";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        assert!(
            result.equality.is_empty(),
            "Function-wrapped columns must not produce skip predicates: {:?}",
            result.equality
        );
    }

    #[test]
    fn test_extract_skip_predicates_lower_function_not_extracted() {
        let sql = "SELECT * FROM t WHERE LOWER(name) = 'alice'";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        assert!(
            result.equality.is_empty(),
            "LOWER()-wrapped column must not produce skip predicates"
        );
    }

    #[test]
    fn test_extract_skip_predicates_nested_function_not_extracted() {
        let sql = "SELECT * FROM t WHERE TRIM(UPPER(name)) = 'FOO'";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        assert!(
            result.equality.is_empty(),
            "Nested function calls must not produce skip predicates"
        );
    }

    #[test]
    fn test_extract_skip_predicates_bare_column_still_works() {
        let sql = "SELECT * FROM t WHERE status = 'active'";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        assert_eq!(
            result.equality.get("status").unwrap(),
            "active",
            "Bare column references must still produce skip predicates"
        );
    }

    #[test]
    fn test_extract_skip_predicates_cast_still_works() {
        let sql = "SELECT * FROM t WHERE CAST(status AS String) = 'active'";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        assert_eq!(
            result.equality.get("status").unwrap(),
            "active",
            "CAST expressions must still produce skip predicates"
        );
    }

    #[test]
    fn test_extract_skip_predicates_function_like_not_extracted() {
        let sql = "SELECT * FROM t WHERE UPPER(name) LIKE 'FOO%'";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        assert!(
            result.prefix.is_empty(),
            "Function-wrapped LIKE must not produce skip predicates"
        );
    }

    #[test]
    fn test_extract_skip_predicates_function_range_not_extracted() {
        let sql = "SELECT * FROM t WHERE UPPER(price) >= '100'";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        assert!(
            result.ranges.is_empty(),
            "Function-wrapped range comparison must not produce skip predicates"
        );
    }

    #[test]
    fn test_extract_skip_predicates_double_quoted_string() {
        let sql = r#"SELECT * FROM t WHERE status = "active""#;
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        // Double-quoted strings may be treated as identifiers by ClickHouse dialect,
        // so we just verify no panic occurs
        assert!(!result.contradicted);
    }

    #[test]
    fn test_extract_skip_predicates_numeric_value() {
        let sql = "SELECT * FROM t WHERE amount = 42";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        assert_eq!(result.equality.get("amount").unwrap(), "42");
    }

    #[test]
    fn test_extract_skip_predicates_negative_number() {
        let sql = "SELECT * FROM t WHERE temperature = -10";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        assert_eq!(result.equality.get("temperature").unwrap(), "-10");
    }

    #[test]
    fn test_extract_skip_predicates_cast_value() {
        let sql = "SELECT * FROM t WHERE created_at >= CAST('2024-01-01' AS Date)";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        let range = result.ranges.get("created_at").unwrap();
        assert_eq!(range.min_value.as_deref(), Some("2024-01-01"));
    }

    #[test]
    fn test_extract_skip_predicates_between() {
        let sql = "SELECT * FROM t WHERE price BETWEEN 10 AND 100";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        let range = result.ranges.get("price").unwrap();
        assert_eq!(range.min_value.as_deref(), Some("10"));
        assert_eq!(range.max_value.as_deref(), Some("100"));
        assert!(!range.min_exclusive);
        assert!(!range.max_exclusive);
    }

    #[test]
    fn test_extract_skip_predicates_gt_lt_combined() {
        let sql = "SELECT * FROM t WHERE age > 18 AND age < 65";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        let range = result.ranges.get("age").unwrap();
        assert_eq!(range.min_value.as_deref(), Some("18"));
        assert!(range.min_exclusive);
        assert_eq!(range.max_value.as_deref(), Some("65"));
        assert!(range.max_exclusive);
    }

    #[test]
    fn test_extract_skip_predicates_hastoken() {
        let sql = "SELECT * FROM t WHERE hasToken(message, 'timeout')";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        let tokens = result
            .token_search
            .get("message")
            .expect("message token_search");
        assert!(tokens.contains(&"timeout".to_string()));
    }

    #[test]
    fn test_extract_skip_predicates_hastoken_multiple_and() {
        let sql =
            "SELECT * FROM t WHERE hasToken(message, 'timeout') AND hasToken(message, 'error')";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        let tokens = result
            .token_search
            .get("message")
            .expect("message token_search");
        assert!(tokens.contains(&"timeout".to_string()));
        assert!(tokens.contains(&"error".to_string()));
    }

    #[test]
    fn test_extract_skip_predicates_hastoken_different_columns() {
        let sql =
            "SELECT * FROM t WHERE hasToken(message, 'timeout') AND hasToken(body, 'request')";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        let msg_tokens = result
            .token_search
            .get("message")
            .expect("message token_search");
        let body_tokens = result.token_search.get("body").expect("body token_search");
        assert!(msg_tokens.contains(&"timeout".to_string()));
        assert!(body_tokens.contains(&"request".to_string()));
    }

    #[test]
    fn test_extract_skip_predicates_hastoken_with_equality() {
        let sql = "SELECT * FROM t WHERE status = 'active' AND hasToken(message, 'error')";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        assert_eq!(result.equality.get("status").unwrap(), "active");
        let tokens = result
            .token_search
            .get("message")
            .expect("message token_search");
        assert!(tokens.contains(&"error".to_string()));
    }

    #[test]
    fn test_hierarchical_analysis_removes_conflicted_token_search() {
        use sqlparser::dialect::ClickHouseDialect;
        use sqlparser::parser::Parser;

        // When the same unqualified column has conflicting qualifiers across
        // a CTE and the main body, analyze_for_hierarchical must remove ALL
        // predicate types (including token_search) for that column.
        let sql = "\
            WITH cte AS (SELECT * FROM logs WHERE logs.message = 'error' AND hasToken(logs.message, 'timeout')) \
            SELECT * FROM events WHERE events.message = 'warning' AND hasToken(events.message, 'connect')";
        let dialect = ClickHouseDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();
        let analysis = TableRewriter::analyze_for_hierarchical(&statements);
        assert!(
            analysis
                .skip_predicates
                .token_search
                .get("message")
                .is_none(),
            "token_search for conflicted column 'message' should be removed, got: {:?}",
            analysis.skip_predicates.token_search.get("message"),
        );
        assert!(
            analysis.skip_predicates.equality.get("message").is_none(),
            "equality for conflicted column 'message' should also be removed"
        );
    }

    #[test]
    fn test_date_predicate_still_sees_through_functions() {
        let sql = "SELECT * FROM t WHERE toDate(created_at) >= '2025-01-01'";
        let result = TableRewriter::extract_date_predicates(sql).unwrap();
        assert!(
            result.contains_key("created_at"),
            "Date predicates should still see through toDate() function wrappers"
        );
    }

    #[test]
    fn test_extract_date_predicates_or_branch_not_extracted() {
        let sql = "SELECT * FROM t WHERE date >= '2025-01-01' OR status = 'vip'";
        let result = TableRewriter::extract_date_predicates(sql).unwrap();
        assert!(
            result.is_empty(),
            "Date predicates inside OR must not be extracted for partition pruning"
        );
    }

    #[test]
    fn test_extract_date_predicates_or_with_and_sibling() {
        let sql = "SELECT * FROM t WHERE created_at >= '2025-01-01' AND (status = 'active' OR priority = 'high')";
        let result = TableRewriter::extract_date_predicates(sql).unwrap();
        assert!(
            result.contains_key("created_at"),
            "AND-level date predicates should be extracted"
        );
    }

    #[test]
    fn test_rewrite_join_no_dedup() {
        let mut tables = AHashMap::new();
        tables.insert("orders".to_string(), make_r2_path("project/orders"));
        tables.insert("users".to_string(), make_r2_path("project/users"));

        let rewriter = TableRewriter::new("test_collection");
        let result = rewriter
            .rewrite(
                "SELECT * FROM orders JOIN users ON orders.user_id = users.user_id",
                &tables,
            )
            .unwrap();

        assert!(
            !result.contains("argMax"),
            "JOIN should not have argMax: {}",
            result
        );
        assert!(
            !result.contains("GROUP BY"),
            "JOIN should not have GROUP BY: {}",
            result
        );
        // Both tables should be rewritten to s3() calls
        assert_eq!(
            result.matches("s3(").count(),
            2,
            "Should have 2 s3() calls: {}",
            result
        );
    }

    // ========== Regression tests for bug fixes ==========

    #[test]
    fn test_update_date_range_keeps_tightest_bounds() {
        let mut ranges = AHashMap::new();

        // First predicate: date >= 2024-03-01
        TableRewriter::update_date_range(
            "date",
            &BinaryOperator::GtEq,
            NaiveDate::from_ymd_opt(2024, 3, 1).unwrap(),
            &mut ranges,
        );
        // Second predicate: date >= 2024-06-01 (tighter start)
        TableRewriter::update_date_range(
            "date",
            &BinaryOperator::GtEq,
            NaiveDate::from_ymd_opt(2024, 6, 1).unwrap(),
            &mut ranges,
        );
        let range = &ranges["date"];
        assert_eq!(
            range.start,
            Some(NaiveDate::from_ymd_opt(2024, 6, 1).unwrap()),
            "Should keep the later (tighter) start bound"
        );
    }

    #[test]
    fn test_update_date_range_keeps_earliest_end() {
        let mut ranges = AHashMap::new();

        TableRewriter::update_date_range(
            "date",
            &BinaryOperator::LtEq,
            NaiveDate::from_ymd_opt(2025, 12, 31).unwrap(),
            &mut ranges,
        );
        TableRewriter::update_date_range(
            "date",
            &BinaryOperator::LtEq,
            NaiveDate::from_ymd_opt(2025, 6, 30).unwrap(),
            &mut ranges,
        );
        let range = &ranges["date"];
        assert_eq!(
            range.end,
            Some(NaiveDate::from_ymd_opt(2025, 6, 30).unwrap()),
            "Should keep the earlier (tighter) end bound"
        );
    }

    #[test]
    fn test_update_date_range_contradictory_is_impossible() {
        let mut ranges = AHashMap::new();

        TableRewriter::update_date_range(
            "date",
            &BinaryOperator::GtEq,
            NaiveDate::from_ymd_opt(2025, 6, 1).unwrap(),
            &mut ranges,
        );
        TableRewriter::update_date_range(
            "date",
            &BinaryOperator::LtEq,
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            &mut ranges,
        );
        let range = &ranges["date"];
        assert!(
            range.is_impossible(),
            "start > end should be detected as impossible"
        );
    }

    #[test]
    fn test_date_range_to_partition_keys_impossible_range() {
        let range = DateRange::new(
            Some(NaiveDate::from_ymd_opt(2025, 6, 1).unwrap()),
            Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
        );
        let keys = date_range_to_partition_keys(&range);
        assert!(
            keys.is_empty(),
            "Impossible range must produce no partition keys"
        );
    }

    #[test]
    fn test_date_range_to_partition_keys_single_month() {
        let range = DateRange::new(
            Some(NaiveDate::from_ymd_opt(2024, 3, 10).unwrap()),
            Some(NaiveDate::from_ymd_opt(2024, 3, 25).unwrap()),
        );
        let keys = date_range_to_partition_keys(&range);
        assert_eq!(keys, vec!["2024/03"]);
    }

    #[test]
    fn test_date_range_to_partition_keys_cross_month() {
        let range = DateRange::new(
            Some(NaiveDate::from_ymd_opt(2024, 11, 15).unwrap()),
            Some(NaiveDate::from_ymd_opt(2025, 2, 10).unwrap()),
        );
        let keys = date_range_to_partition_keys(&range);
        assert_eq!(keys, vec!["2024/11", "2024/12", "2025/01", "2025/02"]);
    }

    #[test]
    fn test_date_range_to_partition_keys_open_ended_uses_today() {
        use chrono::Datelike;

        let today = chrono::Utc::now().date_naive();
        // Use a start date within MAX_PARTITION_KEYS months of today
        let start = NaiveDate::from_ymd_opt(today.year(), today.month(), 1).unwrap()
            - chrono::Months::new(6);
        let range = DateRange::new(Some(start), None);
        let keys = date_range_to_partition_keys(&range);
        assert!(
            !keys.is_empty(),
            "Open-ended start range within cap must produce keys"
        );

        let today_key = format!("{:04}/{:02}", today.year(), today.month());
        assert_eq!(
            keys.last().unwrap(),
            &today_key,
            "Last key must be the current month"
        );
    }

    #[test]
    fn test_date_range_to_partition_keys_end_only_open_ended() {
        let range = DateRange::new(None, Some(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap()));
        let keys = date_range_to_partition_keys(&range);
        assert!(
            keys.is_empty(),
            "End-only open range should produce no keys"
        );
    }

    #[test]
    fn test_date_range_to_partition_keys_no_bounds() {
        let range = DateRange::new(None, None);
        let keys = date_range_to_partition_keys(&range);
        assert!(keys.is_empty(), "Unbounded range should produce no keys");
    }

    /// Regression: empty partition keys must become `None`, not `Some([])`,
    /// so that the hierarchical index searches all partitions instead of zero.
    #[test]
    fn test_empty_partition_keys_become_none_not_some_empty() {
        let end_only = DateRange::new(None, Some(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap()));
        let keys = date_range_to_partition_keys(&end_only);
        let hint: Option<Vec<String>> = if keys.is_empty() { None } else { Some(keys) };
        assert!(
            hint.is_none(),
            "End-only range must produce None hints, not Some([])"
        );

        let no_bounds = DateRange::new(None, None);
        let keys = date_range_to_partition_keys(&no_bounds);
        let hint: Option<Vec<String>> = if keys.is_empty() { None } else { Some(keys) };
        assert!(
            hint.is_none(),
            "Unbounded range must produce None hints, not Some([])"
        );
    }

    #[test]
    fn test_open_ended_start_range_produces_pruning_hints() {
        use chrono::Datelike;
        let today = chrono::Utc::now().date_naive();
        let start = NaiveDate::from_ymd_opt(today.year(), today.month(), 1).unwrap()
            - chrono::Months::new(3);

        let open_ended = DateRange::new(Some(start), None);
        let keys = date_range_to_partition_keys(&open_ended);
        let hint: Option<Vec<String>> = if keys.is_empty() { None } else { Some(keys) };
        assert!(
            hint.is_some(),
            "Open-ended start range within cap must produce partition keys for pruning"
        );
    }

    #[test]
    fn test_update_date_range_eq_single() {
        let mut ranges = AHashMap::new();
        TableRewriter::update_date_range(
            "date",
            &BinaryOperator::Eq,
            NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
            &mut ranges,
        );
        let range = &ranges["date"];
        assert_eq!(
            range.start,
            Some(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap())
        );
        assert_eq!(
            range.end,
            Some(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap())
        );
        assert!(
            !range.is_impossible(),
            "Single Eq should produce valid single-day range"
        );
    }

    #[test]
    fn test_update_date_range_contradictory_eq() {
        let mut ranges = AHashMap::new();
        TableRewriter::update_date_range(
            "date",
            &BinaryOperator::Eq,
            NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
            &mut ranges,
        );
        TableRewriter::update_date_range(
            "date",
            &BinaryOperator::Eq,
            NaiveDate::from_ymd_opt(2024, 7, 1).unwrap(),
            &mut ranges,
        );
        let range = &ranges["date"];
        assert!(
            range.is_impossible(),
            "Two contradictory Eq predicates must produce an impossible range"
        );
    }

    #[test]
    fn test_update_date_range_eq_after_gt() {
        let mut ranges = AHashMap::new();
        TableRewriter::update_date_range(
            "date",
            &BinaryOperator::GtEq,
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            &mut ranges,
        );
        TableRewriter::update_date_range(
            "date",
            &BinaryOperator::Eq,
            NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
            &mut ranges,
        );
        let range = &ranges["date"];
        assert_eq!(
            range.start,
            Some(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap()),
            "Eq after GtEq should tighten start to the Eq date"
        );
        assert_eq!(
            range.end,
            Some(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap()),
            "Eq should set end bound"
        );
        assert!(!range.is_impossible());
    }

    #[test]
    fn test_not_between_does_not_narrow_date_range() {
        let sql = "SELECT * FROM events WHERE date NOT BETWEEN '2025-01-01' AND '2025-06-01'";
        let ranges = TableRewriter::extract_date_predicates(sql).unwrap();
        assert!(
            ranges.is_empty(),
            "NOT BETWEEN must not produce a date range (it cannot narrow partitions)"
        );
    }

    #[test]
    fn test_between_still_narrows_date_range() {
        let sql = "SELECT * FROM events WHERE date BETWEEN '2025-01-01' AND '2025-06-01'";
        let ranges = TableRewriter::extract_date_predicates(sql).unwrap();
        assert!(!ranges.is_empty(), "BETWEEN should produce a date range");
        let range = &ranges["date"];
        assert_eq!(
            range.start,
            Some(NaiveDate::from_ymd_opt(2025, 1, 1).unwrap())
        );
        assert_eq!(
            range.end,
            Some(NaiveDate::from_ymd_opt(2025, 6, 1).unwrap())
        );
    }

    #[test]
    fn test_extract_tables_cte_does_not_remove_real_table() {
        let sql =
            "SELECT * FROM users WHERE id IN (WITH users AS (SELECT 1 AS id) SELECT id FROM users)";
        let tables = TableRewriter::extract_tables(sql).unwrap();
        assert!(
            tables.contains(&"users".to_string()),
            "The real 'users' table must not be removed by inner CTE with same name"
        );
    }

    #[test]
    fn test_extract_tables_join_on_subquery() {
        let sql = "SELECT * FROM orders o JOIN customers c ON o.status IN (SELECT status FROM valid_statuses)";
        let tables = TableRewriter::extract_tables(sql).unwrap();
        assert!(tables.contains(&"orders".to_string()));
        assert!(tables.contains(&"customers".to_string()));
        assert!(
            tables.contains(&"valid_statuses".to_string()),
            "Tables in JOIN ON subqueries must be collected"
        );
    }

    #[test]
    fn test_skip_predicates_from_cte_query() {
        let sql = "WITH cte AS (SELECT * FROM t WHERE status = 'active') SELECT * FROM cte";
        let predicates = TableRewriter::extract_skip_predicates(sql).unwrap();
        assert_eq!(
            predicates.equality.get("status").map(|s| s.as_str()),
            Some("active"),
            "Skip predicates must be extracted from CTE subqueries"
        );
    }

    #[test]
    fn test_skip_predicates_qualified_column() {
        let sql = "SELECT * FROM orders o WHERE o.status = 'shipped'";
        let predicates = TableRewriter::extract_skip_predicates(sql).unwrap();
        assert_eq!(
            predicates.equality.get("status").map(|s| s.as_str()),
            Some("shipped"),
            "Skip predicates must handle qualified column names (table.column)"
        );
    }

    #[test]
    fn test_collect_columns_union_query() {
        let sql = "SELECT name, age FROM users UNION ALL SELECT name, age FROM admins";
        let columns = TableRewriter::extract_select_columns(sql).unwrap();
        let all_cols: Vec<&String> = columns.values().flat_map(|v| v.iter()).collect();
        assert!(
            all_cols.iter().any(|c| c.as_str() == "name"),
            "Columns from UNION branches must be collected"
        );
        assert!(
            all_cols.iter().any(|c| c.as_str() == "age"),
            "Columns from UNION branches must be collected"
        );
    }

    #[test]
    fn test_date_range_partition_keys_capped() {
        let range = DateRange {
            start: Some(NaiveDate::from_ymd_opt(1990, 1, 1).unwrap()),
            end: Some(NaiveDate::from_ymd_opt(2025, 12, 31).unwrap()),
        };
        let keys = date_range_to_partition_keys(&range);
        assert!(
            keys.is_empty(),
            "Ranges exceeding MAX_PARTITION_KEYS must return empty (full scan), got {} keys",
            keys.len(),
        );
    }

    #[test]
    fn test_date_range_partition_keys_within_limit() {
        let range = DateRange {
            start: Some(NaiveDate::from_ymd_opt(2025, 1, 1).unwrap()),
            end: Some(NaiveDate::from_ymd_opt(2025, 6, 30).unwrap()),
        };
        let keys = date_range_to_partition_keys(&range);
        assert_eq!(
            keys.len(),
            6,
            "6-month range should produce 6 partition keys"
        );
        assert_eq!(keys[0], "2025/01");
        assert_eq!(keys[5], "2025/06");
    }

    #[test]
    fn test_date_range_partition_keys_exact_boundary() {
        // Exactly MAX_PARTITION_KEYS (24) months should succeed.
        let range = DateRange {
            start: Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
            end: Some(NaiveDate::from_ymd_opt(2025, 12, 31).unwrap()),
        };
        let keys = date_range_to_partition_keys(&range);
        assert_eq!(
            keys.len(),
            24,
            "24-month range should produce exactly 24 partition keys"
        );
        assert_eq!(keys[0], "2024/01");
        assert_eq!(keys[23], "2025/12");
    }

    #[test]
    fn test_date_range_partition_keys_one_over_boundary() {
        // 25 months should exceed MAX_PARTITION_KEYS and return empty.
        let range = DateRange {
            start: Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
            end: Some(NaiveDate::from_ymd_opt(2026, 1, 31).unwrap()),
        };
        let keys = date_range_to_partition_keys(&range);
        assert!(
            keys.is_empty(),
            "25-month range must exceed MAX_PARTITION_KEYS and fall back to full scan, got {} keys",
            keys.len(),
        );
    }

    #[test]
    fn test_date_range_strict_gt() {
        let mut ranges = AHashMap::new();
        TableRewriter::update_date_range(
            "date",
            &BinaryOperator::Gt,
            NaiveDate::from_ymd_opt(2025, 1, 31).unwrap(),
            &mut ranges,
        );
        let range = &ranges["date"];
        assert_eq!(
            range.start,
            Some(NaiveDate::from_ymd_opt(2025, 1, 31).unwrap()),
            "Strict > must use the parsed date directly (conservative) \
             because DateTime columns can have rows at '2025-01-31 00:00:01'"
        );
    }

    #[test]
    fn test_date_range_strict_lt() {
        let mut ranges = AHashMap::new();
        TableRewriter::update_date_range(
            "date",
            &BinaryOperator::Lt,
            NaiveDate::from_ymd_opt(2025, 3, 1).unwrap(),
            &mut ranges,
        );
        let range = &ranges["date"];
        assert_eq!(
            range.end,
            Some(NaiveDate::from_ymd_opt(2025, 3, 1).unwrap()),
            "Strict < must use the parsed date directly (conservative) \
             because DateTime columns can have rows at '2025-02-28 23:59:59'"
        );
    }

    #[test]
    fn test_to_start_of_week_returns_sunday() {
        // ClickHouse toStartOfWeek(date) default mode=0 returns Sunday.
        // 2025-03-03 is a Monday — the preceding Sunday is 2025-03-02.
        let sql = "SELECT * FROM t WHERE date >= toStartOfWeek(toDate('2025-03-03'))";
        let ranges = TableRewriter::extract_date_predicates(sql).unwrap();
        let range = &ranges["date"];
        assert_eq!(
            range.start,
            Some(NaiveDate::from_ymd_opt(2025, 3, 2).unwrap()),
            "toStartOfWeek on Monday 2025-03-03 must yield Sunday 2025-03-02"
        );
    }

    #[test]
    fn test_to_start_of_week_saturday_crosses_month() {
        // 2025-03-01 is a Saturday — the preceding Sunday is 2025-02-23.
        let sql = "SELECT * FROM t WHERE date >= toStartOfWeek(toDate('2025-03-01'))";
        let ranges = TableRewriter::extract_date_predicates(sql).unwrap();
        let range = &ranges["date"];
        assert_eq!(
            range.start,
            Some(NaiveDate::from_ymd_opt(2025, 2, 23).unwrap()),
            "toStartOfWeek on Saturday 2025-03-01 must yield Sunday 2025-02-23"
        );
    }

    #[test]
    fn test_to_start_of_week_on_sunday_is_same_day() {
        // 2025-03-02 is a Sunday — toStartOfWeek should return the same day.
        let sql = "SELECT * FROM t WHERE date >= toStartOfWeek(toDate('2025-03-02'))";
        let ranges = TableRewriter::extract_date_predicates(sql).unwrap();
        let range = &ranges["date"];
        assert_eq!(
            range.start,
            Some(NaiveDate::from_ymd_opt(2025, 3, 2).unwrap()),
            "toStartOfWeek on Sunday 2025-03-02 must yield the same day"
        );
    }

    #[test]
    fn test_check_join_types_recurses_into_cte() {
        let checker = TypeChecker::new();
        let sql = "WITH cte AS (SELECT * FROM a JOIN b ON a.id = b.id) SELECT * FROM cte";
        let warnings = checker.check_join_types(sql).unwrap();
        assert!(
            warnings.is_empty(),
            "Without registered schemas, no warnings expected but recursion must not panic"
        );
    }

    #[test]
    fn test_check_join_types_recurses_into_union() {
        let checker = TypeChecker::new();
        let sql =
            "SELECT * FROM a JOIN b ON a.id = b.id UNION ALL SELECT * FROM c JOIN d ON c.id = d.id";
        let warnings = checker.check_join_types(sql).unwrap();
        assert!(
            warnings.is_empty(),
            "Without registered schemas, no warnings expected but recursion must not panic"
        );
    }

    #[test]
    fn test_extract_column_ref_three_part_identifier() {
        use sqlparser::ast::Ident;

        let three_part = Expr::CompoundIdentifier(vec![
            Ident::new("schema"),
            Ident::new("table"),
            Ident::new("column"),
        ]);
        let result = extract_column_ref(&three_part);
        assert_eq!(
            result,
            Some(("table".to_string(), "column".to_string())),
            "3-part identifiers must extract (table, column) from the last two parts"
        );

        let two_part = Expr::CompoundIdentifier(vec![Ident::new("table"), Ident::new("column")]);
        let result = extract_column_ref(&two_part);
        assert_eq!(result, Some(("table".to_string(), "column".to_string())),);
    }

    #[test]
    fn test_visit_expr_handles_named_function_arg() {
        use sqlparser::ast::{
            Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
            ObjectName,
        };

        // Build a Function AST node with a Named arg containing a table reference
        let table_expr = Expr::Identifier(Ident::new("my_column"));
        let named_arg = FunctionArg::Named {
            name: Ident::new("arg_name"),
            arg: FunctionArgExpr::Expr(table_expr),
            operator: sqlparser::ast::FunctionArgOperator::RightArrow,
        };
        let mut func_expr = Expr::Function(Function {
            name: ObjectName(vec![Ident::new("FUNC")]),
            args: FunctionArguments::List(FunctionArgumentList {
                args: vec![named_arg],
                duplicate_treatment: None,
                clauses: vec![],
            }),
            parameters: FunctionArguments::None,
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
        });

        // Verify visit_expr doesn't panic on Named function args
        let tables = AHashMap::new();
        let s3_config = S3Config {
            collection_name: "test",
        };
        let transformer = BasicTableTransformer::with_s3_config(s3_config, &tables);
        let visitor = AstVisitor::new(&transformer);
        visitor.visit_expr(&mut func_expr);
        // No panic = Named args are handled
    }

    #[test]
    fn test_extract_tables_cte_cross_reference() {
        let sql = "WITH a AS (SELECT * FROM real_table), b AS (SELECT * FROM a) SELECT * FROM b";
        let tables = TableRewriter::extract_tables(sql).unwrap();
        assert!(
            tables.contains(&"real_table".to_string()),
            "Real table inside CTE body must be collected: {:?}",
            tables,
        );
        assert!(
            !tables.contains(&"a".to_string()),
            "CTE alias 'a' referenced by sibling CTE 'b' must NOT appear: {:?}",
            tables,
        );
        assert!(
            !tables.contains(&"b".to_string()),
            "CTE alias 'b' referenced in main body must NOT appear: {:?}",
            tables,
        );
    }

    #[test]
    fn test_rewrite_with_validation_enforces_project_id_on_cache_hit() {
        let rewriter = TableRewriter::new("r2_bucket");

        let project_a = Uuid::new_v4();
        let project_b = Uuid::new_v4();

        let mut tables = AHashMap::new();
        tables.insert(
            "orders".to_string(),
            R2TablePath::with_project(project_a, SourceType::Stripe, "orders"),
        );

        let sql = "SELECT * FROM orders WHERE id = 1";

        let first = rewriter.rewrite_with_validation(sql, &tables, project_a);
        assert!(
            first.is_ok(),
            "First call with correct project must succeed"
        );

        let second = rewriter.rewrite_with_validation(sql, &tables, project_b);
        assert!(
            matches!(second, Err(RewriteError::AccessDenied { .. })),
            "Cache hit must NOT bypass project_id validation, got: {:?}",
            second,
        );
    }

    #[test]
    fn test_rewrite_with_validation_accepts_schema_qualified_table() {
        let rewriter = TableRewriter::new("r2_bucket");
        let project_id = Uuid::new_v4();

        let mut tables = AHashMap::new();
        tables.insert(
            "orders".to_string(),
            R2TablePath::with_project(project_id, SourceType::Stripe, "orders"),
        );

        let sql = "SELECT * FROM myschema.orders WHERE id = 1";
        let result = rewriter.rewrite_with_validation(sql, &tables, project_id);
        assert!(
            result.is_ok(),
            "Schema-qualified 'myschema.orders' must resolve via short name 'orders': {:?}",
            result,
        );
    }

    #[test]
    fn test_find_missing_tables_schema_qualified() {
        let mut tables = AHashMap::new();
        tables.insert(
            "orders".to_string(),
            R2TablePath::for_testing("stripe/orders"),
        );

        let sql = "SELECT * FROM myschema.orders";
        let missing = TableRewriter::find_missing_tables(sql, &tables).unwrap();
        assert!(
            missing.is_empty(),
            "Schema-qualified 'myschema.orders' should not be reported as missing \
             when 'orders' is available: {:?}",
            missing,
        );
    }

    #[test]
    fn test_collect_tables_excludes_cte_names() {
        let sql = "WITH cte_a AS (SELECT * FROM real_table) \
                    SELECT * FROM cte_a JOIN another_table ON cte_a.id = another_table.id";
        let tables = TableRewriter::extract_tables(sql).unwrap();
        assert!(
            tables.contains(&"real_table".to_string()),
            "Real table inside CTE body must be collected: {:?}",
            tables
        );
        assert!(
            tables.contains(&"another_table".to_string()),
            "Real table in main body must be collected: {:?}",
            tables
        );
        assert!(
            !tables.contains(&"cte_a".to_string()),
            "CTE name must be filtered out: {:?}",
            tables
        );
    }

    #[test]
    fn test_collect_tables_multiple_ctes_exclude_each_other() {
        let sql = "WITH \
                    cte_a AS (SELECT * FROM t1), \
                    cte_b AS (SELECT * FROM cte_a JOIN t2 ON cte_a.id = t2.id) \
                    SELECT * FROM cte_b JOIN t3 ON cte_b.x = t3.x";
        let tables = TableRewriter::extract_tables(sql).unwrap();
        assert!(
            tables.contains(&"t1".to_string()),
            "t1 must be collected: {:?}",
            tables
        );
        assert!(
            tables.contains(&"t2".to_string()),
            "t2 must be collected: {:?}",
            tables
        );
        assert!(
            tables.contains(&"t3".to_string()),
            "t3 must be collected: {:?}",
            tables
        );
        assert!(
            !tables.contains(&"cte_a".to_string()),
            "cte_a reference inside cte_b body must be filtered out: {:?}",
            tables
        );
        assert!(
            !tables.contains(&"cte_b".to_string()),
            "cte_b reference in main body must be filtered out: {:?}",
            tables
        );
    }

    #[test]
    fn test_cache_hash_includes_partition_fields() {
        let mut tables_a = AHashMap::new();
        tables_a.insert(
            "events".to_string(),
            R2TablePath::for_testing("data/events"),
        );

        let mut path_b = R2TablePath::for_testing("data/events");
        path_b.date_partitioned = true;
        path_b.partition_column = Some("created_at".to_string());

        let mut tables_b = AHashMap::new();
        tables_b.insert("events".to_string(), path_b);

        let hash_a = QueryPlanCache::hash_tables(&tables_a);
        let hash_b = QueryPlanCache::hash_tables(&tables_b);

        assert_ne!(
            hash_a, hash_b,
            "Changing date_partitioned/partition_column must produce a different cache key"
        );
    }

    #[test]
    fn test_apply_interval_extreme_year_returns_none() {
        let sql = "SELECT * FROM t WHERE date >= today() - INTERVAL 99999999999 YEAR";
        let result = TableRewriter::extract_date_predicates(sql).unwrap();
        assert!(
            result.get("date").map_or(true, |r| r.start.is_none()),
            "Extreme year interval that overflows i32 should not produce a resolved date bound"
        );
    }

    #[test]
    fn test_apply_interval_extreme_month_returns_none() {
        let base = NaiveDate::from_ymd_opt(2025, 6, 15).unwrap();
        let result = add_months_to_date(base, i64::MAX);
        assert!(
            result.is_none(),
            "Extreme month interval that overflows i32 year must return None"
        );
        let result_neg = add_months_to_date(base, i64::MIN / 2);
        assert!(
            result_neg.is_none(),
            "Extreme negative month interval must return None"
        );
    }

    #[test]
    fn test_partition_pruning_transformer_impossible_date_range() {
        use chrono::NaiveDate;

        let mut tables = AHashMap::new();
        tables.insert(
            "events".to_string(),
            R2TablePath {
                prefix: "warehouse/events".to_string(),
                project_id: None,
                date_partitioned: true,
                partition_column: Some("date".to_string()),
                detected_partition_scheme: None,
                buffer_ch_table: None,
            },
        );

        let mut date_predicates = AHashMap::new();
        date_predicates.insert(
            "date".to_string(),
            DateRange::new(
                Some(NaiveDate::from_ymd_opt(2025, 12, 1).unwrap()),
                Some(NaiveDate::from_ymd_opt(2025, 1, 1).unwrap()),
            ),
        );
        assert!(date_predicates["date"].is_impossible());

        let transformer =
            PartitionPruningTransformer::new("test_collection", &tables, &date_predicates);

        let result = transformer.transform_table("events");
        assert!(
            result.is_some(),
            "Should produce an expression for known table"
        );

        let expr = result.unwrap();
        let sql = expr.to_string();
        assert!(
            !sql.contains("filename=''") && !sql.contains("filename = ''"),
            "Impossible date range must not produce empty filename; got: {sql}"
        );
        assert!(
            sql.contains("__dh_no_match__"),
            "Impossible date range should use __dh_no_match__ sentinel; got: {sql}"
        );
    }

    // ==================== Cross-table predicate conflict tests ====================

    #[test]
    fn test_cross_table_date_predicates_do_not_merge() {
        let sql = "SELECT * FROM orders o JOIN events e ON o.id = e.order_id \
                   WHERE o.date >= '2025-06-01' AND e.date <= '2025-01-31'";
        let result = TableRewriter::extract_date_predicates(sql).unwrap();
        assert!(
            result.get("date").is_none(),
            "Date predicates from different table aliases must not merge; got: {:?}",
            result.get("date")
        );
    }

    #[test]
    fn test_cte_body_cross_scope_different_qualifiers_conflict() {
        let sql = r#"
            WITH cte AS (SELECT * FROM t1 WHERE t1.date >= '2025-06-01')
            SELECT * FROM cte, t2 WHERE t2.date <= '2025-12-31'
        "#;
        let result = TableRewriter::extract_date_predicates(sql).unwrap();
        assert!(
            result.get("date").is_none(),
            "CTE and body predicates on the same bare column but different qualifiers \
             must not merge — they reference different tables; got: {:?}",
            result.get("date")
        );
    }

    #[test]
    fn test_same_table_date_predicates_still_merge() {
        let sql = "SELECT * FROM orders o WHERE o.date >= '2025-01-01' AND o.date <= '2025-12-31'";
        let result = TableRewriter::extract_date_predicates(sql).unwrap();
        let range = result
            .get("date")
            .expect("Same-table predicates must merge");
        assert!(range.start.is_some());
        assert!(range.end.is_some());
    }

    #[test]
    fn test_unqualified_date_predicates_still_work() {
        let sql = "SELECT * FROM orders WHERE date >= '2025-01-01' AND date <= '2025-12-31'";
        let result = TableRewriter::extract_date_predicates(sql).unwrap();
        let range = result
            .get("date")
            .expect("Unqualified predicates must still be extracted");
        assert_eq!(range.start.unwrap().to_string(), "2025-01-01");
        assert_eq!(range.end.unwrap().to_string(), "2025-12-31");
    }

    #[test]
    fn test_cross_table_skip_predicates_do_not_merge() {
        let sql = "SELECT * FROM orders o JOIN events e ON o.id = e.order_id \
                   WHERE o.status = 'active' AND e.status = 'pending'";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        assert!(
            result.equality.get("status").is_none(),
            "Skip predicates from different table aliases must not merge; got: {:?}",
            result.equality.get("status")
        );
    }

    #[test]
    fn test_same_table_skip_predicates_still_work() {
        let sql = "SELECT * FROM orders o WHERE o.status = 'active'";
        let result = TableRewriter::extract_skip_predicates(sql).unwrap();
        assert_eq!(
            result.equality.get("status").map(|s| s.as_str()),
            Some("active"),
            "Single-table qualified predicates must still be extracted"
        );
    }

    #[test]
    fn test_cross_table_different_columns_both_extracted() {
        let sql = "SELECT * FROM orders o JOIN events e ON o.id = e.order_id \
                   WHERE o.created_at >= '2025-01-01' AND e.event_date <= '2025-12-31'";
        let result = TableRewriter::extract_date_predicates(sql).unwrap();
        assert!(
            result.contains_key("created_at"),
            "created_at should be extracted (no conflict)"
        );
        assert!(
            result.contains_key("event_date"),
            "event_date should be extracted (no conflict)"
        );
    }

    #[test]
    fn test_collect_columns_includes_cte_body_columns() {
        let sql = "WITH filtered AS (SELECT id, name FROM users WHERE age > 25) \
                   SELECT name FROM filtered";
        let columns = TableRewriter::extract_select_columns(sql).unwrap();
        let all_cols: Vec<&String> = columns.values().flatten().collect();
        assert!(
            all_cols.iter().any(|c| c.as_str() == "id"),
            "Column 'id' from CTE body must be collected: {:?}",
            columns
        );
        assert!(
            all_cols.iter().any(|c| c.as_str() == "age"),
            "Column 'age' from CTE WHERE clause must be collected: {:?}",
            columns
        );
        assert!(
            all_cols.iter().any(|c| c.as_str() == "name"),
            "Column 'name' from outer query must be collected: {:?}",
            columns
        );
    }

    #[test]
    fn test_extract_table_aliases_from_union() {
        let sql = "SELECT a.id FROM orders a UNION ALL SELECT b.id FROM returns b";
        let dialect = ClickHouseDialect {};
        let stmts = Parser::parse_sql(&dialect, sql).unwrap();
        let aliases = TableRewriter::extract_table_aliases(&stmts);
        assert!(
            aliases.contains_key("a"),
            "Alias 'a' from first UNION branch must be extracted: {:?}",
            aliases
        );
        assert!(
            aliases.contains_key("b"),
            "Alias 'b' from second UNION branch must be extracted: {:?}",
            aliases
        );
    }

    #[test]
    fn test_extract_tables_schema_qualified() {
        let sql = "SELECT * FROM myschema.orders WHERE id = 1";
        let tables = TableRewriter::extract_tables(sql).unwrap();
        assert_eq!(
            tables,
            vec!["myschema.orders"],
            "Only the full qualified name should be extracted: {:?}",
            tables,
        );
    }

    #[test]
    fn test_extract_table_aliases_from_cte() {
        let sql = "WITH cte AS (SELECT t.id FROM orders t) SELECT * FROM cte";
        let dialect = ClickHouseDialect {};
        let stmts = Parser::parse_sql(&dialect, sql).unwrap();
        let aliases = TableRewriter::extract_table_aliases(&stmts);
        assert!(
            aliases.contains_key("t"),
            "Alias 't' from CTE body must be extracted: {:?}",
            aliases
        );
    }

    #[test]
    fn test_strict_gt_produces_exclusive_lower_bound() {
        let preds =
            TableRewriter::extract_skip_predicates("SELECT * FROM t WHERE price > '10'").unwrap();
        let range = preds.ranges.get("price").expect("price range should exist");
        assert_eq!(range.min_value.as_deref(), Some("10"));
        assert!(range.min_exclusive, "Gt must set min_exclusive = true");
        assert!(range.max_value.is_none());
    }

    #[test]
    fn test_gte_produces_inclusive_lower_bound() {
        let preds =
            TableRewriter::extract_skip_predicates("SELECT * FROM t WHERE price >= '10'").unwrap();
        let range = preds.ranges.get("price").expect("price range should exist");
        assert_eq!(range.min_value.as_deref(), Some("10"));
        assert!(!range.min_exclusive, "GtEq must set min_exclusive = false");
    }

    #[test]
    fn test_strict_lt_produces_exclusive_upper_bound() {
        let preds =
            TableRewriter::extract_skip_predicates("SELECT * FROM t WHERE price < '50'").unwrap();
        let range = preds.ranges.get("price").expect("price range should exist");
        assert_eq!(range.max_value.as_deref(), Some("50"));
        assert!(range.max_exclusive, "Lt must set max_exclusive = true");
        assert!(range.min_value.is_none());
    }

    #[test]
    fn test_lte_produces_inclusive_upper_bound() {
        let preds =
            TableRewriter::extract_skip_predicates("SELECT * FROM t WHERE price <= '50'").unwrap();
        let range = preds.ranges.get("price").expect("price range should exist");
        assert_eq!(range.max_value.as_deref(), Some("50"));
        assert!(!range.max_exclusive, "LtEq must set max_exclusive = false");
    }

    #[test]
    fn test_reversed_gt_produces_exclusive_upper_bound() {
        let preds =
            TableRewriter::extract_skip_predicates("SELECT * FROM t WHERE '10' > price").unwrap();
        let range = preds.ranges.get("price").expect("price range should exist");
        assert_eq!(range.max_value.as_deref(), Some("10"));
        assert!(
            range.max_exclusive,
            "reversed Gt must set max_exclusive = true"
        );
    }

    #[test]
    fn test_reversed_lt_produces_exclusive_lower_bound() {
        let preds =
            TableRewriter::extract_skip_predicates("SELECT * FROM t WHERE '50' < price").unwrap();
        let range = preds.ranges.get("price").expect("price range should exist");
        assert_eq!(range.min_value.as_deref(), Some("50"));
        assert!(
            range.min_exclusive,
            "reversed Lt must set min_exclusive = true"
        );
    }

    #[test]
    fn test_cache_key_stable_for_same_partition_strategy() {
        use crate::warehouse::indexes::external_config::PartitionStrategy;

        let mut tables1 = AHashMap::new();
        tables1.insert(
            "t".to_string(),
            R2TablePath {
                prefix: "proj/t".to_string(),
                project_id: None,
                date_partitioned: true,
                partition_column: Some("created_at".to_string()),
                detected_partition_scheme: Some(PartitionStrategy::HiveStyle {
                    pattern: "year={year}/month={month}".to_string(),
                    columns: vec!["year".to_string(), "month".to_string()],
                }),
                buffer_ch_table: None,
            },
        );

        let mut tables2 = AHashMap::new();
        tables2.insert(
            "t".to_string(),
            R2TablePath {
                prefix: "proj/t".to_string(),
                project_id: None,
                date_partitioned: true,
                partition_column: Some("created_at".to_string()),
                detected_partition_scheme: Some(PartitionStrategy::HiveStyle {
                    pattern: "year={year}/month={month}".to_string(),
                    columns: vec!["year".to_string(), "month".to_string()],
                }),
                buffer_ch_table: None,
            },
        );

        let h1 = QueryPlanCache::hash_tables(&tables1);
        let h2 = QueryPlanCache::hash_tables(&tables2);
        assert_eq!(
            h1, h2,
            "Identical PartitionStrategy must produce the same cache key"
        );

        let mut tables3 = tables1.clone();
        tables3.get_mut("t").unwrap().detected_partition_scheme = Some(PartitionStrategy::Flat);

        let h3 = QueryPlanCache::hash_tables(&tables3);
        assert_ne!(
            h1, h3,
            "Different PartitionStrategy must produce a different cache key"
        );
    }

    #[test]
    fn test_cache_stats_overwrites_not_counted_as_evictions() {
        let cache = QueryPlanCache::new(100);
        let tables = AHashMap::new();

        cache.put("SELECT 1", &tables, "rewritten_1".to_string());
        // Overwrite the same key with a new value
        cache.put("SELECT 1", &tables, "rewritten_1_v2".to_string());

        let stats = cache.stats();
        assert_eq!(stats.size, 1, "Cache should have one entry after overwrite");
        assert_eq!(
            stats.memory_evictions, 0,
            "Overwriting the same key must not count as eviction"
        );
    }

    #[test]
    fn test_add_months_to_date_negative_delta() {
        let base = NaiveDate::from_ymd_opt(2025, 6, 15).unwrap();
        let result = add_months_to_date(base, -3).unwrap();
        assert_eq!(result, NaiveDate::from_ymd_opt(2025, 3, 15).unwrap());
    }

    #[test]
    fn test_add_months_to_date_large_negative() {
        use chrono::Datelike;
        let base = NaiveDate::from_ymd_opt(2025, 1, 31).unwrap();
        let result = add_months_to_date(base, -24);
        assert!(result.is_some());
        assert_eq!(result.unwrap().year(), 2023);

        let extreme = add_months_to_date(base, -(i32::MAX as i64));
        // Extreme values may exceed NaiveDate's range; function returns None
        // which is correct behavior. The key fix is that negation itself doesn't panic.
        assert!(extreme.is_none() || extreme.unwrap().year() < 0);
    }

    #[test]
    fn test_parse_date_add_sub_months_operator_precedence() {
        use sqlparser::ast::SetExpr;
        use sqlparser::dialect::ClickHouseDialect;
        use sqlparser::parser::Parser;

        let dialect = ClickHouseDialect {};
        let sql = "SELECT subMonths(toDate('2025-06-15'), 3)";
        let stmts = Parser::new(&dialect)
            .try_with_sql(sql)
            .unwrap()
            .parse_statements()
            .unwrap();
        if let sqlparser::ast::Statement::Query(query) = &stmts[0] {
            if let SetExpr::Select(select) = query.body.as_ref() {
                if let sqlparser::ast::SelectItem::UnnamedExpr(sqlparser::ast::Expr::Function(
                    func,
                )) = &select.projection[0]
                {
                    let result = TableRewriter::parse_date_add_sub_months(func, true);
                    assert_eq!(
                        result,
                        Some(NaiveDate::from_ymd_opt(2025, 3, 15).unwrap()),
                        "subMonths(2025-06-15, 3) should give 2025-03-15"
                    );
                } else {
                    panic!("Expected Function expression");
                }
            } else {
                panic!("Expected Select");
            }
        } else {
            panic!("Expected Query");
        }
    }
}
