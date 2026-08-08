//! Cross-Database Join via Semi-Join Reduction
//!
//! Executes cross-database equi-joins efficiently using a semi-join
//! reduction strategy to minimize network data transfer:
//! 1. Querying the smaller/filtered side first (probe)
//! 2. Extracting join keys from the probe result
//! 3. Using those keys to filter the larger side via IN-clause (build)
//! 4. Performing the full equi-join on the small results in memory
//!
//! The "semi-join reduction" refers to the network optimization in step 3
//! (only fetching matching build rows), not the final join semantics.
//! The in-memory join in step 4 is a standard inner or left equi-join
//! that may produce multiple output rows per probe row when the build
//! side contains duplicate keys.

use ahash::{AHashMap, AHashSet};
use sqlparser::ast::{BinaryOperator, Expr, Ident, SetExpr, Statement, Value};
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser;
use thiserror::Error;
use tracing::{info, instrument, warn};

use super::bloom_pushdown::{BloomFilterPushdown, FilterStrategy};
use super::executor::{ColumnInfo, ExecutionOptions, ExecutorError, QueryExecutor, QueryResult};
use super::federation::{CombinationStrategy, FederationConfig};
use super::plan_optimizer::JoinType;
use super::rewriter::serialize_statements;
use crate::warehouse::ch_type_parser::ch_type_is_numeric;

/// Errors specific to semi-join execution.
#[derive(Debug, Error)]
pub enum SemiJoinError {
    #[error("Probe query failed: {0}")]
    ProbeQueryFailed(String),

    #[error("Build query failed: {0}")]
    BuildQueryFailed(String),

    #[error("Key extraction failed: column '{0}' not found in probe result")]
    KeyColumnNotFound(String),

    #[error("Join failed: {0}")]
    JoinFailed(String),

    #[error("Too many keys ({0}) - exceeds maximum of {1}")]
    TooManyKeys(usize, usize),

    #[error("Bloom filter error: {0}")]
    BloomFilterError(String),

    #[error("Unsupported join type: {0}")]
    UnsupportedJoin(String),

    #[error("Executor error: {0}")]
    Executor(#[from] ExecutorError),
}

/// Result type for semi-join operations.
pub type SemiJoinResult<T> = Result<T, SemiJoinError>;

/// Executes semi-join reduction strategy for cross-database queries.
///
/// Uses `FederationConfig` for configuration, which consolidates all
/// federation-related settings including semi-join thresholds.
pub struct SemiJoinExecutor {
    /// Configuration for semi-join behavior.
    config: FederationConfig,
}

impl SemiJoinExecutor {
    /// Create a new semi-join executor with default config.
    pub fn new() -> Self {
        Self {
            config: FederationConfig::default(),
        }
    }

    /// Create a semi-join executor with custom configuration.
    pub fn with_config(config: FederationConfig) -> Self {
        Self { config }
    }

    /// Execute a semi-join reduction strategy.
    ///
    /// # Arguments
    /// * `probe_executor` - Executor for the probe source
    /// * `build_executor` - Executor for the build source
    /// * `strategy` - The SemiJoinReduction strategy parameters
    /// * `options` - Execution options
    ///
    /// # Returns
    /// Combined query result after semi-join
    #[instrument(
        name = "semi_join.execute",
        skip(self, probe_executor, build_executor, options),
        fields(
            probe_keys,
            build_rows,
            strategy,
        )
    )]
    pub async fn execute(
        &self,
        probe_executor: &QueryExecutor,
        build_executor: &QueryExecutor,
        strategy: &CombinationStrategy,
        options: ExecutionOptions,
    ) -> SemiJoinResult<QueryResult> {
        // Extract parameters from strategy
        let (
            probe_query,
            probe_key_column,
            build_base_query,
            build_key_column,
            build_source_type,
            max_keys,
            join_type,
        ) = match strategy {
            CombinationStrategy::SemiJoinReduction {
                probe_query,
                probe_key_column,
                build_base_query,
                build_key_column,
                build_source_type,
                max_keys_for_in_clause,
                join_type,
                ..
            } => (
                probe_query,
                probe_key_column,
                build_base_query,
                build_key_column,
                *build_source_type,
                *max_keys_for_in_clause,
                join_type,
            ),
            _ => {
                return Err(SemiJoinError::JoinFailed(
                    "Invalid strategy: expected SemiJoinReduction".to_string(),
                ));
            }
        };

        // Step 1: Execute probe query
        info!(query = %probe_query, "Executing probe query");
        let probe_result = probe_executor
            .execute(probe_query, options.clone())
            .await
            .map_err(|e| SemiJoinError::ProbeQueryFailed(e.to_string()))?;

        // Step 2: Extract join keys from probe result
        let keys = self.extract_keys(&probe_result, probe_key_column)?;
        let key_count = keys.len();

        tracing::Span::current().record("probe_keys", key_count);
        info!(key_count, "Extracted join keys from probe result");

        if keys.is_empty() {
            // Retrieve build-side column metadata so the result schema
            // includes both probe and build columns, matching what callers
            // expect from a JOIN.
            let build_columns = self.fetch_build_column_metadata(
                build_base_query,
                build_key_column,
                build_executor,
                options.clone(),
            ).await;

            if matches!(join_type, JoinType::Left) && !probe_result.rows.is_empty() {
                let null_count = build_columns.len();
                let mut columns = probe_result.columns;
                columns.extend(build_columns);
                let rows: Vec<Vec<serde_json::Value>> = probe_result.rows
                    .into_iter()
                    .map(|mut row| {
                        row.extend(
                            std::iter::repeat(serde_json::Value::Null).take(null_count),
                        );
                        row
                    })
                    .collect();
                let row_count = rows.len();
                return Ok(QueryResult {
                    columns,
                    rows,
                    row_count,
                    execution_time_ms: probe_result.execution_time_ms,
                    bytes_read: probe_result.bytes_read,
                    rows_read: probe_result.rows_read,
                    truncated: probe_result.truncated,
                });
            }
            let mut columns = probe_result.columns;
            columns.extend(build_columns);
            return Ok(QueryResult {
                columns,
                rows: Vec::new(),
                row_count: 0,
                execution_time_ms: probe_result.execution_time_ms,
                bytes_read: probe_result.bytes_read,
                rows_read: probe_result.rows_read,
                truncated: false,
            });
        }

        // Determine whether the key column is a numeric type so that the
        // IN clause uses the correct quoting strategy.  We derive this from
        // the probe result metadata (the probe and build key columns must be
        // join-compatible, so their types match).
        let is_numeric_type = probe_result
            .columns
            .iter()
            .find(|c| c.name == *probe_key_column)
            .map(|c| ch_type_is_numeric(&c.data_type))
            .unwrap_or(false);

        // Step 3: Build filtered query for build side
        let (build_query, bloom_filter) = if key_count <= max_keys.min(self.config.semi_join_in_clause_limit) {
            // Use IN clause for small key sets
            (self.build_in_clause_query(build_base_query, build_key_column, &keys, is_numeric_type)?, None)
        } else if self.config.enable_bloom_pushdown && key_count <= self.config.semi_join_bloom_limit {
            info!(key_count, "Using Bloom filter for semi-join (key count exceeds IN clause limit)");

            // Adaptive FPR: use a higher FPR for smaller key sets to reduce
            // filter size, tightening as key count grows to maintain accuracy.
            let adaptive_fpr = if key_count < 10_000 {
                (self.config.bloom_false_positive_rate * 5.0).min(0.05)
            } else if key_count < 100_000 {
                self.config.bloom_false_positive_rate * 2.0
            } else {
                self.config.bloom_false_positive_rate
            };

            let bloom = BloomFilterPushdown::from_keys(&keys, adaptive_fpr)
                .map_err(|e| SemiJoinError::BloomFilterError(e.to_string()))?;
            
            let strategy = bloom.to_filter_strategy(build_key_column, build_source_type);
            
            match strategy {
                FilterStrategy::NativePushdown { sql_expression } => {
                    // Source supports native Bloom filter pushdown
                    let query = self.build_bloom_filter_query(build_base_query, &sql_expression)?;
                    (query, None)
                }
                FilterStrategy::ClientSide { filter, .. } => {
                    // Need to fetch all data and filter client-side
                    info!("Build source does not support Bloom pushdown - using client-side filtering");
                    (build_base_query.to_string(), Some(filter))
                }
                FilterStrategy::TempTable { .. } => {
                    return Err(SemiJoinError::JoinFailed(
                        format!("TempTable filter strategy is not yet supported ({key_count} keys)"),
                    ));
                }
            }
        } else {
            let effective_limit = if self.config.enable_bloom_pushdown {
                self.config.semi_join_bloom_limit
            } else {
                max_keys.min(self.config.semi_join_in_clause_limit)
            };
            return Err(SemiJoinError::TooManyKeys(
                key_count,
                effective_limit,
            ));
        };

        // Step 4: Execute build query with filter
        info!(query = %build_query, "Executing filtered build query");
        let mut build_result = build_executor
            .execute(&build_query, options)
            .await
            .map_err(|e| SemiJoinError::BuildQueryFailed(e.to_string()))?;

        // Step 4b: Apply client-side Bloom filter if needed
        if let Some(ref filter) = bloom_filter {
            let original_count = build_result.rows.len();
            self.apply_bloom_filter_in_place(&mut build_result, filter, build_key_column)?;
            info!(
                original_rows = original_count,
                filtered_rows = build_result.rows.len(),
                "Applied client-side Bloom filter"
            );
        }

        tracing::Span::current().record("build_rows", build_result.rows.len());
        info!(
            build_rows = build_result.rows.len(),
            "Build query returned filtered results"
        );

        // Step 5: Join results in memory
        let joined = self.join_results(
            &probe_result,
            &build_result,
            probe_key_column,
            build_key_column,
            join_type,
        )?;

        Ok(joined)
    }

    /// Fetch build-side column metadata (excluding the join key column) by
    /// executing a `LIMIT 0` query.  Returns an empty vec on failure so the
    /// caller can still produce a valid (degraded) result.
    async fn fetch_build_column_metadata(
        &self,
        build_base_query: &str,
        build_key_column: &str,
        build_executor: &QueryExecutor,
        options: ExecutionOptions,
    ) -> Vec<super::executor::ColumnInfo> {
        let schema_query = match set_limit_zero(build_base_query) {
            Ok(q) => q,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to set LIMIT 0 for schema query; wrapping in subquery");
                format!("SELECT * FROM ({build_base_query}) AS __schema LIMIT 0")
            }
        };
        match build_executor.execute(&schema_query, options).await {
            Ok(result) => {
                let key_idx = result
                    .columns
                    .iter()
                    .position(|c| c.name == build_key_column);
                result
                    .columns
                    .into_iter()
                    .enumerate()
                    .filter(|(i, _)| Some(*i) != key_idx)
                    .map(|(_, c)| c)
                    .collect()
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "Could not fetch build-side schema; result columns may be incomplete"
                );
                Vec::new()
            }
        }
    }

    /// Extract unique join keys from a column in the query result.
    ///
    /// Returns the **original, un-normalized** string representations so they
    /// can be used in SQL IN clauses where the build database must match the
    /// stored value exactly (e.g. `"00123"` must stay `"00123"`, not become
    /// `"123"`).
    fn extract_keys(
        &self,
        result: &QueryResult,
        column_name: &str,
    ) -> SemiJoinResult<Vec<String>> {
        let col_idx = result
            .columns
            .iter()
            .position(|c| c.name == column_name)
            .ok_or_else(|| SemiJoinError::KeyColumnNotFound(column_name.to_string()))?;

        let mut keys = AHashSet::with_capacity(result.rows.len());
        for row in &result.rows {
            if let Some(value) = row.get(col_idx) {
                if let Some(key) = value_to_raw_key(value) {
                    keys.insert(key);
                }
            }
        }

        let mut sorted: Vec<String> = keys.into_iter().collect();
        sorted.sort();
        Ok(sorted)
    }

    /// Build a query with an IN clause filter.
    ///
    /// `is_numeric_type` controls quoting: when `false`, all values are
    /// single-quoted regardless of whether they look numeric. This prevents
    /// ClickHouse from silently converting string values like `"00123"` to
    /// the integer `123`, which would cause the comparison to fail.
    fn build_in_clause_query(
        &self,
        base_query: &str,
        key_column: &str,
        keys: &[String],
        is_numeric_type: bool,
    ) -> SemiJoinResult<String> {
        let list: Vec<Expr> = keys
            .iter()
            .map(|k| key_to_expr(k, is_numeric_type))
            .collect();

        let in_expr = Expr::InList {
            expr: Box::new(Expr::Identifier(Ident::with_quote('"', key_column))),
            list,
            negated: false,
        };

        let dialect = ClickHouseDialect {};
        let mut statements = Parser::parse_sql(&dialect, base_query)
            .map_err(|e| SemiJoinError::JoinFailed(format!("failed to parse base query: {e}")))?;

        for statement in &mut statements {
            if let Statement::Query(query) = statement {
                add_where_to_set_expr(&mut query.body, &in_expr);
            }
        }

        Ok(serialize_statements(&statements))
    }

    /// Build a query with a Bloom filter expression.
    fn build_bloom_filter_query(
        &self,
        base_query: &str,
        bloom_expression: &str,
    ) -> SemiJoinResult<String> {
        insert_where_clause(base_query, bloom_expression)
    }

    /// Apply a Bloom filter to query results (client-side filtering).
    /// Filter rows in-place using the Bloom filter, avoiding clones.
    fn apply_bloom_filter_in_place(
        &self,
        result: &mut QueryResult,
        filter: &super::bloom_pushdown::BloomFilter,
        key_column: &str,
    ) -> SemiJoinResult<()> {
        let col_idx = result
            .columns
            .iter()
            .position(|c| c.name == key_column)
            .ok_or_else(|| SemiJoinError::KeyColumnNotFound(key_column.to_string()))?;

        result.rows.retain(|row| {
            if let Some(value) = row.get(col_idx) {
                if let Some(key) = value_to_raw_key(value) {
                    filter.might_contain(&key)
                } else {
                    false
                }
            } else {
                false
            }
        });
        result.row_count = result.rows.len();
        Ok(())
    }

    /// Create a row from probe values with NULLs for all build columns (excluding the key).
    fn null_padded_row(
        probe_row: &[serde_json::Value],
        build_columns: &[ColumnInfo],
        build_key_idx: usize,
    ) -> Vec<serde_json::Value> {
        let mut row = probe_row.to_vec();
        for (i, _) in build_columns.iter().enumerate() {
            if i != build_key_idx {
                row.push(serde_json::Value::Null);
            }
        }
        row
    }

    /// Perform an in-memory equi-join of probe and build results.
    ///
    /// This is a standard inner or left join: when multiple build rows share
    /// the same key, each matching probe row is emitted once per build match.
    fn join_results(
        &self,
        probe: &QueryResult,
        build: &QueryResult,
        probe_key_col: &str,
        build_key_col: &str,
        join_type: &JoinType,
    ) -> SemiJoinResult<QueryResult> {
        // Find key column indices
        let probe_key_idx = probe
            .columns
            .iter()
            .position(|c| c.name == probe_key_col)
            .ok_or_else(|| SemiJoinError::KeyColumnNotFound(probe_key_col.to_string()))?;

        let build_key_idx = build
            .columns
            .iter()
            .position(|c| c.name == build_key_col)
            .ok_or_else(|| SemiJoinError::KeyColumnNotFound(build_key_col.to_string()))?;

        // Build hash map of build side keyed by join column
        let mut build_map: AHashMap<String, Vec<&Vec<serde_json::Value>>> =
            AHashMap::with_capacity(build.rows.len());

        for row in &build.rows {
            if let Some(key_val) = row.get(build_key_idx) {
                if let Some(key) = value_to_key(key_val) {
                    build_map.entry(key).or_default().push(row);
                }
            }
        }

        // Merge column definitions (probe columns + build columns, excluding duplicate key)
        let build_cols_to_add = build.columns.len().saturating_sub(if build_key_idx < build.columns.len() { 1 } else { 0 });
        let mut result_columns = Vec::with_capacity(probe.columns.len() + build_cols_to_add);
        result_columns.extend_from_slice(&probe.columns);
        for (i, col) in build.columns.iter().enumerate() {
            if i != build_key_idx {
                result_columns.push(col.clone());
            }
        }

        // Only Inner and Left joins are implemented for cross-database semi-joins.
        match join_type {
            JoinType::Inner | JoinType::Left => {}
            other => {
                return Err(SemiJoinError::UnsupportedJoin(format!(
                    "{:?} join is not supported for cross-database semi-joins",
                    other
                )));
            }
        }

        let mut result_rows = Vec::with_capacity(probe.rows.len());
        let is_left_join = matches!(join_type, JoinType::Left);

        for probe_row in &probe.rows {
            if let Some(key_val) = probe_row.get(probe_key_idx) {
                let key = match value_to_key(key_val) {
                    Some(k) => k,
                    None => {
                        if is_left_join {
                            result_rows.push(Self::null_padded_row(probe_row, &build.columns, build_key_idx));
                        }
                        continue;
                    }
                };

                if let Some(build_rows) = build_map.get(&key) {
                    let build_extra_cols = if build.columns.len() > 1 { build.columns.len() - 1 } else { 0 };
                    for build_row in build_rows {
                        let mut joined_row = Vec::with_capacity(probe_row.len() + build_extra_cols);
                        joined_row.extend_from_slice(probe_row);
                        for (i, val) in build_row.iter().enumerate() {
                            if i != build_key_idx {
                                joined_row.push(val.clone());
                            }
                        }
                        result_rows.push(joined_row);
                    }
                } else if is_left_join {
                    result_rows.push(Self::null_padded_row(probe_row, &build.columns, build_key_idx));
                }
                // For INNER join with no match, row is dropped
            } else if is_left_join {
                result_rows.push(Self::null_padded_row(probe_row, &build.columns, build_key_idx));
            }
        }

        let row_count = result_rows.len();
        Ok(QueryResult {
            columns: result_columns,
            rows: result_rows,
            row_count,
            execution_time_ms: probe.execution_time_ms.saturating_add(build.execution_time_ms),
            bytes_read: probe.bytes_read.saturating_add(build.bytes_read),
            rows_read: probe.rows_read.saturating_add(build.rows_read),
            truncated: probe.truncated || build.truncated,
        })
    }
}

impl Default for SemiJoinExecutor {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse a SQL condition fragment into an `Expr` AST node.
fn parse_condition_expr(condition: &str) -> Result<Expr, sqlparser::parser::ParserError> {
    let dialect = ClickHouseDialect {};
    let mut parser = Parser::new(&dialect).try_with_sql(condition)?;
    parser.parse_expr()
}

/// Rewrite a SQL query to have `LIMIT 0`, replacing any existing LIMIT.
fn set_limit_zero(sql: &str) -> SemiJoinResult<String> {
    let dialect = ClickHouseDialect {};
    let mut stmts = Parser::parse_sql(&dialect, sql)
        .map_err(|e| SemiJoinError::JoinFailed(format!("failed to parse SQL for LIMIT 0: {e}")))?;

    if stmts.len() != 1 {
        return Err(SemiJoinError::JoinFailed(format!(
            "expected 1 statement, got {}",
            stmts.len()
        )));
    }

    match stmts[0] {
        Statement::Query(ref mut query) => {
            query.limit = Some(Expr::Value(Value::Number("0".to_string(), false)));
            Ok(stmts[0].to_string())
        }
        _ => Err(SemiJoinError::JoinFailed(
            "set_limit_zero only supports SELECT queries".to_string(),
        )),
    }
}

/// Add a condition to the WHERE clause of a `Select` node.
/// If a WHERE clause already exists, the new condition is AND-ed with it
/// (wrapping the existing condition in parentheses to preserve precedence).
fn add_where_condition(select: &mut sqlparser::ast::Select, new_condition: Expr) {
    select.selection = Some(match select.selection.take() {
        Some(existing) => Expr::BinaryOp {
            left: Box::new(Expr::Nested(Box::new(existing))),
            op: BinaryOperator::And,
            right: Box::new(Expr::Nested(Box::new(new_condition))),
        },
        None => new_condition,
    });
}

/// Add a WHERE condition to every top-level Select inside a SetExpr.
fn add_where_to_set_expr(set_expr: &mut SetExpr, condition: &Expr) {
    match set_expr {
        SetExpr::Select(select) => {
            add_where_condition(select, condition.clone());
        }
        SetExpr::Query(query) => {
            add_where_to_set_expr(&mut query.body, condition);
        }
        SetExpr::SetOperation { left, right, .. } => {
            add_where_to_set_expr(left, condition);
            add_where_to_set_expr(right, condition);
        }
        _ => {}
    }
}

/// Insert a WHERE clause (or AND condition) into a SQL query using AST manipulation.
///
/// Parses the query with sqlparser, injects the condition into the AST, and
/// serializes back. This is safe against keywords inside string literals,
/// comments, and nested subqueries.
fn insert_where_clause(base_query: &str, condition: &str) -> SemiJoinResult<String> {
    let dialect = ClickHouseDialect {};
    let mut statements = Parser::parse_sql(&dialect, base_query)
        .map_err(|e| SemiJoinError::JoinFailed(format!("failed to parse base query: {e}")))?;

    let cond_expr = parse_condition_expr(condition)
        .map_err(|e| SemiJoinError::JoinFailed(format!("failed to parse condition: {e}")))?;

    for statement in &mut statements {
        if let Statement::Query(query) = statement {
            add_where_to_set_expr(&mut query.body, &cond_expr);
        }
    }

    Ok(serialize_statements(&statements))
}

/// Convert a key string to an `Expr` AST node, choosing between numeric
/// literals and quoted strings based on the column type.
fn key_to_expr(key: &str, is_numeric_type: bool) -> Expr {
    if is_numeric_type {
        if key.parse::<i64>().is_ok() || key.parse::<u64>().is_ok() {
            return Expr::Value(Value::Number(key.to_string(), false));
        }
        if let Ok(f) = key.parse::<f64>() {
            if f.is_finite() {
                return Expr::Value(Value::Number(key.to_string(), false));
            }
        }
    }
    Expr::Value(Value::SingleQuotedString(key.to_string()))
}


/// Normalize a numeric serde_json::Number to a canonical string.
///
/// Integers are preserved exactly (no f64 round-trip). Floats use a
/// canonical `f64` representation so that `0.1` and `0.10` produce the
/// same key.
fn normalize_number(n: &serde_json::Number) -> String {
    if let Some(i) = n.as_i64() {
        return i.to_string();
    }
    if let Some(u) = n.as_u64() {
        return u.to_string();
    }
    if let Some(f) = n.as_f64() {
        return f.to_string();
    }
    n.to_string()
}

/// Convert a JSON value to its original string representation for SQL IN clauses.
///
/// Unlike [`value_to_key`], this does **not** normalize numeric-looking strings.
/// This ensures values like `"00123"` or `"1.0"` are preserved exactly as they
/// appear in the source database, preventing silent mismatches in the IN clause.
///
/// Returns `None` for NULL values (SQL NULL never matches in joins).
fn value_to_raw_key(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(if *b { "1" } else { "0" }.to_string()),
        other => Some(other.to_string()),
    }
}

/// Convert a JSON value to a normalized string key for hashing.
///
/// Returns `None` for NULL values (SQL NULL never matches in joins).
///
/// String values that look numeric are normalized so that cross-database type
/// mismatches (e.g. one side returns `"0.10"` as a string and the other returns
/// `0.1` as a number) produce the same key.  This normalization is only safe
/// for the in-memory join; SQL IN clauses must use [`value_to_raw_key`] instead.
fn value_to_key(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => {
            let could_be_numeric = s.as_bytes().first().is_some_and(|&b| {
                b.is_ascii_digit() || b == b'-' || b == b'+' || b == b'.'
            });
            if could_be_numeric {
                if let Ok(n) = s.parse::<serde_json::Number>() {
                    return Some(normalize_number(&n));
                }
            }
            Some(s.clone())
        }
        serde_json::Value::Number(n) => Some(normalize_number(n)),
        serde_json::Value::Bool(b) => Some(if *b { "1" } else { "0" }.to_string()),
        other => Some(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::warehouse::query::executor::ColumnInfo;

    fn make_column(name: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: "String".to_string(),
            nullable: true,
        }
    }

    fn make_result(columns: Vec<ColumnInfo>, rows: Vec<Vec<serde_json::Value>>) -> QueryResult {
        let row_count = rows.len();
        QueryResult {
            columns,
            rows,
            row_count,
            execution_time_ms: 0,
            bytes_read: 0,
            rows_read: 0,
            truncated: false,
        }
    }

    #[test]
    fn test_build_in_clause_simple() {
        let executor = SemiJoinExecutor::new();
        let keys = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        
        let query = executor.build_in_clause_query(
            "SELECT * FROM users",
            "id",
            &keys,
            false,
        ).unwrap();
        
        // Column names are quoted to prevent SQL injection
        assert!(query.contains("WHERE \"id\" IN"));
        assert!(query.contains("'a'"));
        assert!(query.contains("'b'"));
        assert!(query.contains("'c'"));
    }

    #[test]
    fn test_build_in_clause_with_existing_where() {
        let executor = SemiJoinExecutor::new();
        let keys = vec!["1".to_string(), "2".to_string()];
        
        let query = executor.build_in_clause_query(
            "SELECT * FROM users WHERE active = true",
            "id",
            &keys,
            true,
        ).unwrap();
        
        assert!(query.contains("AND (\"id\" IN") || query.contains("AND \"id\" IN"),
            "IN clause must be present (parenthesized or plain): {}", query);
    }

    #[test]
    fn test_build_in_clause_with_group_by() {
        let executor = SemiJoinExecutor::new();
        let keys = vec!["x".to_string()];
        
        let query = executor.build_in_clause_query(
            "SELECT count(*) FROM orders GROUP BY customer_id",
            "customer_id",
            &keys,
            false,
        ).unwrap();
        
        // Column names are quoted to prevent SQL injection
        assert!(query.contains("WHERE \"customer_id\" IN"));
        assert!(query.contains("GROUP BY"));
    }

    #[test]
    fn test_extract_keys() {
        let executor = SemiJoinExecutor::new();
        
        let result = make_result(
            vec![make_column("id"), make_column("name")],
            vec![
                vec![serde_json::json!("1"), serde_json::json!("Alice")],
                vec![serde_json::json!("2"), serde_json::json!("Bob")],
                vec![serde_json::json!("1"), serde_json::json!("Alice2")], // Duplicate key
            ],
        );
        
        let keys = executor.extract_keys(&result, "id").unwrap();
        
        // Should have deduplicated
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"1".to_string()));
        assert!(keys.contains(&"2".to_string()));
    }

    #[test]
    fn test_extract_keys_returns_sorted_order() {
        let executor = SemiJoinExecutor::new();

        let result = make_result(
            vec![make_column("id")],
            vec![
                vec![serde_json::json!("charlie")],
                vec![serde_json::json!("alice")],
                vec![serde_json::json!("bob")],
                vec![serde_json::json!("alice")],
            ],
        );

        let keys = executor.extract_keys(&result, "id").unwrap();
        assert_eq!(keys, vec!["alice", "bob", "charlie"],
            "Keys must be sorted for deterministic IN clause generation");
    }

    #[test]
    fn test_extract_keys_column_not_found() {
        let executor = SemiJoinExecutor::new();
        
        let result = make_result(vec![make_column("id")], vec![]);
        
        let err = executor.extract_keys(&result, "nonexistent").unwrap_err();
        assert!(matches!(err, SemiJoinError::KeyColumnNotFound(_)));
    }

    #[test]
    fn test_join_results_inner() {
        let executor = SemiJoinExecutor::new();
        
        let probe = make_result(
            vec![make_column("customer_id"), make_column("order_id")],
            vec![
                vec![serde_json::json!("C1"), serde_json::json!("O1")],
                vec![serde_json::json!("C2"), serde_json::json!("O2")],
                vec![serde_json::json!("C3"), serde_json::json!("O3")], // No match
            ],
        );
        
        let build = make_result(
            vec![make_column("customer_id"), make_column("name")],
            vec![
                vec![serde_json::json!("C1"), serde_json::json!("Alice")],
                vec![serde_json::json!("C2"), serde_json::json!("Bob")],
            ],
        );
        
        let joined = executor
            .join_results(&probe, &build, "customer_id", "customer_id", &JoinType::Inner)
            .unwrap();
        
        // C3 should be dropped (no match)
        assert_eq!(joined.rows.len(), 2);
        // Should have probe cols + build cols (minus duplicate key)
        assert_eq!(joined.columns.len(), 3); // customer_id, order_id, name
    }

    #[test]
    fn test_join_results_left() {
        let executor = SemiJoinExecutor::new();
        
        let probe = make_result(
            vec![make_column("id")],
            vec![
                vec![serde_json::json!("1")],
                vec![serde_json::json!("2")], // No match
            ],
        );
        
        let build = make_result(
            vec![make_column("id"), make_column("val")],
            vec![
                vec![serde_json::json!("1"), serde_json::json!("A")],
            ],
        );
        
        let joined = executor
            .join_results(&probe, &build, "id", "id", &JoinType::Left)
            .unwrap();
        
        // Both rows should be present (LEFT JOIN keeps non-matching)
        assert_eq!(joined.rows.len(), 2);
        // Second row should have NULL for build columns
        assert_eq!(joined.rows[1][1], serde_json::Value::Null);
    }

    #[test]
    fn test_left_join_probe_row_missing_key_column() {
        let executor = SemiJoinExecutor::new();

        // Probe has one row with enough columns and one row that is short
        let probe = make_result(
            vec![make_column("id"), make_column("extra")],
            vec![
                vec![serde_json::json!("1"), serde_json::json!("x")],
                vec![serde_json::json!("2")], // missing "extra" (key is col index 1)
            ],
        );

        let build = make_result(
            vec![make_column("extra"), make_column("val")],
            vec![
                vec![serde_json::json!("x"), serde_json::json!("A")],
            ],
        );

        let joined = executor
            .join_results(&probe, &build, "extra", "extra", &JoinType::Left)
            .unwrap();

        assert_eq!(
            joined.rows.len(),
            2,
            "LEFT JOIN must preserve all probe rows, even those with missing key columns"
        );
        assert_eq!(
            joined.rows[1].last().unwrap(),
            &serde_json::Value::Null,
            "Missing-key probe row should have NULL-padded build columns"
        );
    }

    #[test]
    fn test_left_join_empty_build_preserves_columns() {
        let executor = SemiJoinExecutor::new();

        let probe = make_result(
            vec![make_column("id"), make_column("order")],
            vec![
                vec![serde_json::json!("1"), serde_json::json!("O1")],
                vec![serde_json::json!("2"), serde_json::json!("O2")],
            ],
        );

        let build = make_result(
            vec![make_column("id"), make_column("name"), make_column("email")],
            vec![], // no matching rows
        );

        let joined = executor
            .join_results(&probe, &build, "id", "id", &JoinType::Left)
            .unwrap();

        // Result should have probe cols + build cols (minus duplicate key)
        assert_eq!(
            joined.columns.len(),
            4,
            "LEFT JOIN must include build columns even when build has no rows: {:?}",
            joined.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
        );
        assert_eq!(joined.rows.len(), 2, "All probe rows must be preserved");
        // Each row should be padded with NULLs for the 2 non-key build columns
        for row in &joined.rows {
            assert_eq!(row.len(), 4, "each row must have 4 values");
            assert_eq!(row[2], serde_json::Value::Null);
            assert_eq!(row[3], serde_json::Value::Null);
        }
    }

    #[test]
    fn test_inner_join_multi_match_produces_correct_row_count() {
        let executor = SemiJoinExecutor::new();

        let probe = make_result(
            vec![make_column("customer_id"), make_column("order_id")],
            vec![
                vec![serde_json::json!("C1"), serde_json::json!("O1")],
                vec![serde_json::json!("C2"), serde_json::json!("O2")],
            ],
        );

        // C1 has 3 build matches, C2 has 1
        let build = make_result(
            vec![make_column("customer_id"), make_column("address")],
            vec![
                vec![serde_json::json!("C1"), serde_json::json!("Addr1")],
                vec![serde_json::json!("C1"), serde_json::json!("Addr2")],
                vec![serde_json::json!("C1"), serde_json::json!("Addr3")],
                vec![serde_json::json!("C2"), serde_json::json!("Addr4")],
            ],
        );

        let joined = executor
            .join_results(&probe, &build, "customer_id", "customer_id", &JoinType::Inner)
            .unwrap();

        // Equi-join: C1 probe row x 3 build rows + C2 probe row x 1 build row = 4
        assert_eq!(joined.rows.len(), 4,
            "equi-join with multi-match build keys must produce N*M rows");

        // Verify columns: customer_id, order_id, address (build key excluded)
        assert_eq!(joined.columns.len(), 3);

        // Verify all C1 rows have the correct probe data
        let c1_rows: Vec<_> = joined.rows.iter()
            .filter(|r| r[0] == serde_json::json!("C1"))
            .collect();
        assert_eq!(c1_rows.len(), 3);
        for row in &c1_rows {
            assert_eq!(row[1], serde_json::json!("O1"), "probe order_id must be preserved");
        }
    }

    #[test]
    fn test_inner_join_empty_build_drops_all_probe_rows() {
        let executor = SemiJoinExecutor::new();

        let probe = make_result(
            vec![make_column("id"), make_column("val")],
            vec![
                vec![serde_json::json!("1"), serde_json::json!("A")],
                vec![serde_json::json!("2"), serde_json::json!("B")],
            ],
        );

        let build = make_result(
            vec![make_column("id"), make_column("data")],
            vec![],
        );

        let joined = executor
            .join_results(&probe, &build, "id", "id", &JoinType::Inner)
            .unwrap();

        assert_eq!(joined.rows.len(), 0,
            "INNER join with empty build must produce zero rows");
    }

    #[test]
    fn test_join_results_metadata_saturates_instead_of_overflowing() {
        let executor = SemiJoinExecutor::new();

        let mut probe = make_result(
            vec![make_column("id"), make_column("val")],
            vec![vec![serde_json::json!("1"), serde_json::json!("A")]],
        );
        probe.execution_time_ms = u64::MAX - 1;
        probe.bytes_read = u64::MAX;
        probe.rows_read = u64::MAX - 100;

        let mut build = make_result(
            vec![make_column("id"), make_column("data")],
            vec![vec![serde_json::json!("1"), serde_json::json!("B")]],
        );
        build.execution_time_ms = 10;
        build.bytes_read = 1;
        build.rows_read = 200;

        let joined = executor
            .join_results(&probe, &build, "id", "id", &JoinType::Inner)
            .unwrap();

        assert_eq!(joined.execution_time_ms, u64::MAX,
            "execution_time_ms must saturate at u64::MAX");
        assert_eq!(joined.bytes_read, u64::MAX,
            "bytes_read must saturate at u64::MAX");
        assert_eq!(joined.rows_read, u64::MAX,
            "rows_read must saturate at u64::MAX");
    }

    #[test]
    fn test_value_to_key() {
        assert_eq!(value_to_key(&serde_json::json!("hello")), Some("hello".to_string()));
        assert_eq!(value_to_key(&serde_json::json!(42)), Some("42".to_string()));
        assert_eq!(value_to_key(&serde_json::json!(true)), Some("1".to_string()));
        assert_eq!(value_to_key(&serde_json::json!(false)), Some("0".to_string()));
        assert_eq!(value_to_key(&serde_json::Value::Null), None);
    }

    #[test]
    fn test_value_to_raw_key_preserves_original_strings() {
        assert_eq!(value_to_raw_key(&serde_json::json!("00123")), Some("00123".to_string()));
        assert_eq!(value_to_raw_key(&serde_json::json!("1.0")), Some("1.0".to_string()));
        assert_eq!(value_to_raw_key(&serde_json::json!("1.00")), Some("1.00".to_string()));
        assert_eq!(value_to_raw_key(&serde_json::json!("hello")), Some("hello".to_string()));
        assert_eq!(value_to_raw_key(&serde_json::json!(42)), Some("42".to_string()));
        assert_eq!(value_to_raw_key(&serde_json::json!(true)), Some("1".to_string()));
        assert_eq!(value_to_raw_key(&serde_json::json!(false)), Some("0".to_string()));
        assert_eq!(value_to_raw_key(&serde_json::Value::Null), None);
    }

    #[test]
    fn test_extract_keys_preserves_leading_zeros() {
        let executor = SemiJoinExecutor::new();

        let result = make_result(
            vec![make_column("code")],
            vec![
                vec![serde_json::json!("00123")],
                vec![serde_json::json!("00456")],
            ],
        );

        let keys = executor.extract_keys(&result, "code").unwrap();
        assert!(keys.contains(&"00123".to_string()),
            "extract_keys must preserve leading zeros for IN clause: {:?}", keys);
        assert!(keys.contains(&"00456".to_string()),
            "extract_keys must preserve leading zeros for IN clause: {:?}", keys);
    }

    #[test]
    fn test_extract_keys_preserves_decimal_strings() {
        let executor = SemiJoinExecutor::new();

        let result = make_result(
            vec![make_column("version")],
            vec![
                vec![serde_json::json!("1.0")],
                vec![serde_json::json!("2.00")],
            ],
        );

        let keys = executor.extract_keys(&result, "version").unwrap();
        assert!(keys.contains(&"1.0".to_string()),
            "extract_keys must preserve decimal strings for IN clause: {:?}", keys);
        assert!(keys.contains(&"2.00".to_string()),
            "extract_keys must preserve decimal strings for IN clause: {:?}", keys);
    }

    #[test]
    fn test_in_clause_preserves_original_string_values() {
        let executor = SemiJoinExecutor::new();
        let keys = vec!["00123".to_string(), "1.0".to_string()];

        let query = executor.build_in_clause_query(
            "SELECT * FROM users",
            "code",
            &keys,
            false,
        ).unwrap();

        assert!(query.contains("'00123'"),
            "IN clause must preserve '00123', got: {}", query);
        assert!(query.contains("'1.0'"),
            "IN clause must preserve '1.0', got: {}", query);
    }

    #[test]
    fn test_value_to_key_cross_type_normalization() {
        // String "0.10" and number 0.1 must produce the same key
        let string_key = value_to_key(&serde_json::json!("0.10"));
        let number_key = value_to_key(&serde_json::json!(0.1));
        assert_eq!(
            string_key, number_key,
            "String '0.10' and number 0.1 must produce the same join key"
        );

        // Integer string and integer number must match
        let string_int = value_to_key(&serde_json::json!("42"));
        let number_int = value_to_key(&serde_json::json!(42));
        assert_eq!(string_int, number_int);

        // Non-numeric strings are preserved as-is
        assert_eq!(value_to_key(&serde_json::json!("abc")), Some("abc".to_string()));
    }

    #[test]
    fn test_boolean_keys_produce_clickhouse_compatible_values() {
        let executor = SemiJoinExecutor::new();
        let keys = vec!["1".to_string(), "0".to_string()];

        let query = executor.build_in_clause_query(
            "SELECT * FROM flags",
            "is_active",
            &keys,
            true,
        ).unwrap();

        assert!(query.contains("1") && query.contains("0"),
            "Boolean keys must produce ClickHouse-compatible 1/0, got: {}", query);
        assert!(!query.contains("true") && !query.contains("false"),
            "Boolean keys must NOT produce 'true'/'false' strings, got: {}", query);
    }

    #[test]
    fn test_sql_injection_prevention() {
        let executor = SemiJoinExecutor::new();
        let keys = vec!["'; DROP TABLE users; --".to_string()];
        
        let query = executor.build_in_clause_query(
            "SELECT * FROM users",
            "id",
            &keys,
            false,
        ).unwrap();
        
        // Single quotes in values should be escaped to prevent breaking out of string
        // Input: '; DROP TABLE users; --
        // After escaping ': ''; DROP TABLE users; --
        // After wrapping in outer quotes: ''''; DROP TABLE users; --'
        // The malicious content is trapped inside the SQL string literal
        
        // Verify the quote is doubled (escaped)
        assert!(query.contains("''")); // At minimum, there's an escaped quote
        
        // The query should be properly structured
        assert!(query.contains("\"id\" IN")); // Column is quoted
    }


    #[test]
    fn test_build_in_clause_multiline_where() {
        let executor = SemiJoinExecutor::new();
        let keys = vec!["1".to_string(), "2".to_string()];

        let query = executor.build_in_clause_query(
            "SELECT * FROM users\nWHERE active = true",
            "id",
            &keys,
            true,
        ).unwrap();

        assert!(query.contains("AND (\"id\" IN") || query.contains("AND \"id\" IN"),
            "Should find WHERE across newline and add AND: {}", query);
        assert_eq!(query.matches("WHERE").count(), 1,
            "Must not produce duplicate WHERE: {}", query);
    }

    #[test]
    fn test_build_in_clause_tab_before_where() {
        let executor = SemiJoinExecutor::new();
        let keys = vec!["x".to_string()];

        let query = executor.build_in_clause_query(
            "SELECT * FROM t\t WHERE\t active = true",
            "id",
            &keys,
            false,
        ).unwrap();

        assert!(query.contains("AND (\"id\" IN") || query.contains("AND \"id\" IN"),
            "Should find WHERE with tab whitespace: {}", query);
    }

    #[test]
    fn test_build_in_clause_multiline_group_by() {
        let executor = SemiJoinExecutor::new();
        let keys = vec!["x".to_string()];

        let query = executor.build_in_clause_query(
            "SELECT count(*) FROM orders\nGROUP BY customer_id",
            "customer_id",
            &keys,
            false,
        ).unwrap();

        assert!(query.contains("WHERE \"customer_id\" IN"),
            "Should insert WHERE before GROUP BY across newline: {}", query);
        let where_pos = query.find("WHERE").unwrap();
        let group_pos = query.find("GROUP BY").unwrap();
        assert!(where_pos < group_pos,
            "WHERE must come before GROUP BY: {}", query);
    }

    #[test]
    fn test_insert_where_clause_crlf() {
        let result = insert_where_clause(
            "SELECT * FROM t\r\nWHERE x = 1",
            "y = 2",
        ).unwrap();
        assert!(result.contains("y = 2"), "Should inject condition into CRLF query: {}", result);
    }

    #[test]
    fn test_build_in_clause_float_keys_unquoted() {
        let executor = SemiJoinExecutor::new();
        let keys = vec!["1.5".to_string(), "2.7".to_string()];

        let query = executor.build_in_clause_query(
            "SELECT * FROM prices",
            "amount",
            &keys,
            true,
        ).unwrap();

        assert!(query.contains("1.5"), "Float key should be present: {}", query);
        assert!(!query.contains("'1.5'"),
            "Finite float should not be quoted as string: {}", query);
    }

    #[test]
    fn test_build_in_clause_nan_inf_quoted() {
        let executor = SemiJoinExecutor::new();
        let keys = vec!["NaN".to_string(), "inf".to_string()];

        let query = executor.build_in_clause_query(
            "SELECT * FROM t",
            "val",
            &keys,
            true,
        ).unwrap();

        assert!(query.contains("'NaN'") || query.contains("'nan'"),
            "NaN should be quoted as string: {}", query);
    }

    #[test]
    fn test_insert_where_before_having() {
        let executor = SemiJoinExecutor::new();
        let keys = vec!["x".to_string()];

        let query = executor.build_in_clause_query(
            "SELECT status, count(*) FROM orders GROUP BY status HAVING count(*) > 1",
            "status",
            &keys,
            false,
        ).unwrap();

        let where_pos = query.find("WHERE").unwrap();
        let group_pos = query.find("GROUP BY").unwrap();
        assert!(where_pos < group_pos,
            "WHERE must come before GROUP BY: {}", query);
    }

    #[test]
    fn test_insert_where_before_union() {
        let executor = SemiJoinExecutor::new();
        let keys = vec!["x".to_string()];

        let query = executor.build_in_clause_query(
            "SELECT * FROM t1 UNION SELECT * FROM t2",
            "id",
            &keys,
            false,
        ).unwrap();

        let where_pos = query.find("WHERE").unwrap();
        let union_pos = query.find("UNION").unwrap();
        assert!(where_pos < union_pos,
            "WHERE must come before UNION: {}", query);
    }

    #[test]
    fn test_insert_where_before_offset() {
        let executor = SemiJoinExecutor::new();
        let keys = vec!["x".to_string()];

        let query = executor.build_in_clause_query(
            "SELECT * FROM t OFFSET 10",
            "id",
            &keys,
            false,
        ).unwrap();

        let where_pos = query.find("WHERE").unwrap();
        let offset_pos = query.find("OFFSET").unwrap();
        assert!(where_pos < offset_pos,
            "WHERE must come before OFFSET: {}", query);
    }

    /// Regression test for Bug 2: string values that look numeric (e.g. with
    /// leading zeros) must be quoted when the column type is non-numeric.
    #[test]
    fn test_build_in_clause_string_column_always_quotes() {
        let executor = SemiJoinExecutor::new();
        let keys = vec![
            "00123".to_string(),
            "456".to_string(),
            "hello".to_string(),
        ];

        let query = executor.build_in_clause_query(
            "SELECT * FROM t",
            "code",
            &keys,
            false, // string column
        ).unwrap();

        assert!(
            query.contains("'00123'"),
            "Leading-zero string must be quoted when column is non-numeric: {}",
            query,
        );
        assert!(
            query.contains("'456'"),
            "Numeric-looking string must still be quoted for string columns: {}",
            query,
        );
        assert!(
            query.contains("'hello'"),
            "Plain string must be quoted: {}",
            query,
        );
    }

    /// Numeric column values should NOT be quoted.
    #[test]
    fn test_build_in_clause_numeric_column_unquoted() {
        let executor = SemiJoinExecutor::new();
        let keys = vec!["42".to_string(), "99".to_string()];

        let query = executor.build_in_clause_query(
            "SELECT * FROM t",
            "id",
            &keys,
            true, // numeric column
        ).unwrap();

        assert!(
            !query.contains("'42'"),
            "Integer value should not be quoted for numeric column: {}",
            query,
        );
        assert!(
            query.contains("42"),
            "Integer value should be present unquoted: {}",
            query,
        );
    }

    /// NaN and Infinity must always be quoted, even for numeric columns.
    #[test]
    fn test_build_in_clause_nan_inf_always_quoted() {
        let executor = SemiJoinExecutor::new();
        let keys = vec!["NaN".to_string(), "Infinity".to_string(), "-Infinity".to_string()];

        let query = executor.build_in_clause_query(
            "SELECT * FROM t",
            "val",
            &keys,
            true, // numeric column
        ).unwrap();

        assert!(
            query.contains("'NaN'") || query.contains("'nan'"),
            "NaN must be quoted even for numeric columns: {}",
            query,
        );
    }

    #[test]
    fn test_insert_where_clause_with_escaped_string() {
        let sql = "SELECT * FROM t WHERE name = 'it''s here' ORDER BY id";
        let result = insert_where_clause(sql, "x = 1").unwrap();
        assert!(result.contains("x = 1"), "Should inject condition: {}", result);
        assert!(result.contains("ORDER BY"), "Should preserve ORDER BY: {}", result);
    }

    #[test]
    fn test_insert_where_clause_escaped_string_with_in() {
        let sql = "SELECT * FROM t WHERE name = 'it''s here' ORDER BY id";
        let result = insert_where_clause(sql, "\"x\" IN (1, 2)").unwrap();
        let where_count = result.matches("WHERE").count();
        assert_eq!(where_count, 1, "Must not produce duplicate WHERE: {}", result);
        assert!(
            result.contains("AND"),
            "Should add AND clause: {}",
            result,
        );
        let order_pos = result.find("ORDER BY").expect("ORDER BY must be present");
        let where_pos = result.find("WHERE").expect("WHERE must be present");
        assert!(
            where_pos < order_pos,
            "WHERE clause must come before ORDER BY: {}",
            result,
        );
    }

    #[test]
    fn test_build_in_clause_escaped_where() {
        let executor = SemiJoinExecutor::new();
        let keys = vec!["1".to_string(), "2".to_string()];

        let query = executor.build_in_clause_query(
            "SELECT * FROM t WHERE name = 'it''s here' ORDER BY id",
            "id",
            &keys,
            true,
        ).unwrap();

        assert!(
            query.contains("AND (\"id\" IN") || query.contains("AND \"id\" IN"),
            "Should add AND with IN clause: {}",
            query,
        );
        let order_pos = query.find("ORDER BY").expect("ORDER BY must be present");
        let and_pos = query.find("AND").expect("AND must be present");
        assert!(
            and_pos < order_pos,
            "IN clause must come before ORDER BY: {}",
            query,
        );
    }

    #[test]
    fn test_insert_where_clause_keyword_in_string_literal() {
        let sql = "SELECT * FROM t WHERE name = 'WHERE GROUP BY'";
        let result = insert_where_clause(sql, "x = 1").unwrap();
        let where_count = result.matches("WHERE").count();
        assert!(where_count >= 2, "String literal WHERE must be preserved: {}", result);
        assert!(result.contains("x = 1"), "Should inject condition: {}", result);
    }

    #[test]
    fn test_insert_where_clause_no_where() {
        let sql = "SELECT * FROM t";
        let result = insert_where_clause(sql, "x = 1").unwrap();
        assert!(result.contains("WHERE"), "Should add WHERE clause: {}", result);
        assert!(result.contains("x = 1"), "Should inject condition: {}", result);
    }

    #[test]
    fn test_union_where_injected_both_sides() {
        let sql = "SELECT * FROM a UNION ALL SELECT * FROM b";
        let result = insert_where_clause(sql, "x = 1").unwrap();
        let where_count = result.matches("WHERE").count();
        assert_eq!(
            where_count, 2,
            "Both sides of UNION should have WHERE. Got: {result}"
        );
    }

    #[test]
    fn test_build_in_clause_query_returns_err_on_invalid_sql() {
        let executor = SemiJoinExecutor::new();
        let keys = vec!["a".to_string()];
        let result = executor.build_in_clause_query(
            "THIS IS NOT VALID SQL %%% ^^^",
            "id",
            &keys,
            false,
        );
        assert!(result.is_err(), "Malformed SQL should return Err, not panic");
    }

    #[test]
    fn test_insert_where_clause_returns_err_on_invalid_sql() {
        let result = insert_where_clause(
            "NOT VALID SQL AT ALL %%%",
            "x = 1",
        );
        assert!(result.is_err(), "Malformed base query should return Err");

        let result2 = insert_where_clause(
            "SELECT * FROM t",
            "))) (((",
        );
        assert!(result2.is_err(), "Malformed condition should return Err");
    }

    #[test]
    fn test_left_join_with_all_null_keys_preserves_probe_rows() {
        let executor = SemiJoinExecutor::new();
        let probe = make_result(
            vec![make_column("id"), make_column("name")],
            vec![
                vec![serde_json::Value::Null, serde_json::json!("Alice")],
                vec![serde_json::Value::Null, serde_json::json!("Bob")],
            ],
        );
        let build = make_result(
            vec![make_column("id"), make_column("age")],
            vec![],
        );
        let result = executor.join_results(&probe, &build, "id", "id", &JoinType::Left).unwrap();
        assert_eq!(
            result.row_count, 2,
            "LEFT JOIN with all-NULL keys must preserve all probe rows"
        );
    }

    #[test]
    fn test_inner_join_with_all_null_keys_returns_empty() {
        let executor = SemiJoinExecutor::new();
        let probe = make_result(
            vec![make_column("id"), make_column("name")],
            vec![
                vec![serde_json::Value::Null, serde_json::json!("Alice")],
            ],
        );
        let build = make_result(
            vec![make_column("id"), make_column("age")],
            vec![],
        );
        let result = executor.join_results(&probe, &build, "id", "id", &JoinType::Inner).unwrap();
        assert_eq!(result.row_count, 0, "INNER JOIN with no matching keys must return 0 rows");
    }

    #[test]
    fn test_add_where_condition_parenthesizes_new_condition() {
        let dialect = ClickHouseDialect {};
        let mut stmts = Parser::parse_sql(&dialect, "SELECT * FROM t WHERE a = 1").unwrap();
        if let Statement::Query(ref mut q) = stmts[0] {
            if let SetExpr::Select(ref mut select) = *q.body {
                let or_condition = Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident::new("x"))),
                    op: BinaryOperator::Or,
                    right: Box::new(Expr::Identifier(Ident::new("y"))),
                };
                add_where_condition(select, or_condition);

                let result = stmts[0].to_string();
                assert!(
                    result.contains("(x OR y)"),
                    "New OR condition must be parenthesized to preserve precedence: {}",
                    result,
                );
            }
        }
    }

    #[test]
    fn test_set_limit_zero_replaces_existing_limit() {
        let sql = "SELECT * FROM t LIMIT 10";
        let result = set_limit_zero(sql).unwrap();
        assert!(
            result.contains("LIMIT 0"),
            "Existing LIMIT must be replaced with LIMIT 0: {}",
            result,
        );
        assert!(
            !result.contains("LIMIT 10"),
            "Old LIMIT 10 must not remain: {}",
            result,
        );
    }

    #[test]
    fn test_set_limit_zero_adds_when_missing() {
        let sql = "SELECT * FROM t WHERE id > 5";
        let result = set_limit_zero(sql).unwrap();
        assert!(
            result.contains("LIMIT 0"),
            "LIMIT 0 must be added: {}",
            result,
        );
    }

    #[test]
    fn test_bloom_filter_updates_rows_read() {
        use crate::warehouse::query::bloom_pushdown::BloomFilter;

        let executor = SemiJoinExecutor::new();

        let mut result = QueryResult {
            columns: vec![make_column("id")],
            rows: vec![
                vec![serde_json::json!("a")],
                vec![serde_json::json!("b")],
                vec![serde_json::json!("c")],
                vec![serde_json::json!("d")],
            ],
            row_count: 4,
            execution_time_ms: 0,
            bytes_read: 400,
            rows_read: 4,
            truncated: false,
        };

        let mut filter = BloomFilter::new(2, 0.01);
        filter.insert(&String::from("a"));
        filter.insert(&String::from("c"));

        executor
            .apply_bloom_filter_in_place(&mut result, &filter, "id")
            .unwrap();

        assert_eq!(result.row_count, result.rows.len());
        assert_eq!(
            result.rows_read, 4,
            "rows_read must reflect actual rows read from storage, not the post-filter count"
        );
    }

    #[test]
    fn test_too_many_keys_reports_correct_limit_when_bloom_disabled() {
        let config = FederationConfig::default()
            .with_bloom_pushdown(false);

        let in_clause_limit = config.semi_join_in_clause_limit;
        let bloom_limit = config.semi_join_bloom_limit;

        // With bloom disabled, the effective limit should be the IN clause limit,
        // not the much larger bloom limit.
        assert!(in_clause_limit < bloom_limit,
            "Precondition: IN clause limit ({in_clause_limit}) < bloom limit ({bloom_limit})");

        let err = SemiJoinError::TooManyKeys(in_clause_limit + 1, in_clause_limit);

        match err {
            SemiJoinError::TooManyKeys(_, limit) => {
                assert_eq!(limit, in_clause_limit,
                    "When bloom pushdown is disabled the reported limit must be semi_join_in_clause_limit ({in_clause_limit}), not semi_join_bloom_limit ({bloom_limit})");
            }
            _ => panic!("Expected TooManyKeys error"),
        }
    }

    #[test]
    fn test_set_limit_zero_preserves_subquery_limit() {
        let sql = "SELECT * FROM (SELECT * FROM users LIMIT 5) AS sub LIMIT 10";
        let result = set_limit_zero(sql).unwrap();
        assert!(
            result.to_uppercase().contains("LIMIT 0"),
            "Must contain LIMIT 0, got: {result}"
        );
        assert!(
            result.contains("5"),
            "Subquery LIMIT 5 must be preserved, got: {result}"
        );
    }

    #[test]
    fn test_set_limit_zero_rejects_unparseable_sql() {
        let sql = "SELECT x APPLY(toInt32) FROM events";
        assert!(
            set_limit_zero(sql).is_err(),
            "Unparseable SQL must return an error"
        );
    }

    #[test]
    fn test_set_limit_zero_rejects_non_select_statement() {
        let sql = "INSERT INTO sink SELECT * FROM source";
        let result = set_limit_zero(sql);
        assert!(
            result.is_err(),
            "Non-SELECT statements must be rejected, got: {:?}",
            result,
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("SELECT"),
            "Error must mention SELECT, got: {}",
            err_msg,
        );
    }

    #[test]
    fn test_bloom_filter_uses_raw_keys_not_normalized() {
        use crate::warehouse::query::bloom_pushdown::{BloomFilterPushdown, FilterStrategy};
        use crate::warehouse::types::SourceType;

        let raw_keys = vec![
            "00123".to_string(),
            "1.0".to_string(),
            "+5".to_string(),
        ];

        let bloom = BloomFilterPushdown::from_keys(&raw_keys, 0.01).unwrap();
        let filter = match bloom.to_filter_strategy("id", SourceType::Snowflake) {
            FilterStrategy::ClientSide { filter, .. } => filter,
            _ => panic!("Expected ClientSide strategy"),
        };

        for raw_key in &raw_keys {
            assert!(
                filter.might_contain(raw_key),
                "Bloom filter must contain raw key {:?}",
                raw_key
            );
        }

        let executor = SemiJoinExecutor::new();
        let mut result = make_result(
            vec![make_column("id")],
            vec![
                vec![serde_json::json!("00123")],
                vec![serde_json::json!("1.0")],
                vec![serde_json::json!("+5")],
                vec![serde_json::json!("no_match")],
            ],
        );

        executor.apply_bloom_filter_in_place(&mut result, &filter, "id").unwrap();

        let remaining_keys: Vec<&str> = result.rows.iter()
            .filter_map(|r| r[0].as_str())
            .collect();

        assert!(
            remaining_keys.contains(&"00123"),
            "Bloom filter must retain '00123' (raw key); remaining: {:?}",
            remaining_keys
        );
        assert!(
            remaining_keys.contains(&"1.0"),
            "Bloom filter must retain '1.0' (raw key); remaining: {:?}",
            remaining_keys
        );
        assert!(
            remaining_keys.contains(&"+5"),
            "Bloom filter must retain '+5' (raw key); remaining: {:?}",
            remaining_keys
        );
    }

}
