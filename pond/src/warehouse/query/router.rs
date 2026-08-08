//! Query Router
//!
//! Routes queries to the appropriate execution path based on storage type:
//! - **Native ClickHouse**: Query directly against MergeTree tables
//! - **Object Storage**: Rewrite queries to use s3() function with FST skip index
//! - **External**: Data fetched on-demand from external sources (cold tier)
//! - **Mixed**: Multiple storage types in one query; each table rewritten independently
//!
//! PERFORMANCE: Native ClickHouse queries are significantly faster due to
//! native indexes and sorted data. Object storage queries use FST filtering
//! to minimize files scanned.

use ahash::{AHashMap, AHashSet};
use std::sync::Arc;

use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments,
    Ident, JoinConstraint, JoinOperator, ObjectName, Statement, TableFactor, TableWithJoins,
    Value,
};
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser;
use uuid::Uuid;

use crate::warehouse::indexes::HierarchicalSkipIndex;
use crate::warehouse::query::executor::ClickHouseQuerySettings;
use crate::warehouse::types::{StorageType, WarehouseTable};

/// Fallback file count for query settings when a table's skip index is
/// unavailable. Used in both object-storage and mixed routing paths
/// to size ClickHouse `max_threads` / `s3_max_connections`.
const DEFAULT_FILE_COUNT_ESTIMATE: usize = 10;

/// Query routing context.
///
/// Contains all information needed to route and execute a query.
pub struct QueryContext {
    /// Project ID for isolation
    pub project_id: Uuid,
    /// The SQL query to execute
    pub query: String,
    /// Tables referenced in the query with their storage types
    pub table_info: AHashMap<String, TableInfo>,
    /// FST skip index for object storage queries (optional)
    pub skip_index: Option<Arc<HierarchicalSkipIndex>>,
}

/// Per-storage-type data for query routing. Each variant carries only the
/// fields relevant to that storage tier, so the type system prevents
/// impossible states (e.g. ObjectStorage without an `r2_prefix`).
#[derive(Debug, Clone)]
pub enum TableInfo {
    NativeClickHouse {
        clickhouse_table: String,
    },
    ObjectStorage {
        r2_prefix: String,
        /// Known file count (from skip index). Falls back to a default
        /// estimate when `None`.
        file_count: Option<usize>,
    },
    External {
        source_type: crate::warehouse::types::SourceType,
        source_identifier: String,
    },
}

impl From<&WarehouseTable> for TableInfo {
    fn from(table: &WarehouseTable) -> Self {
        match table.storage_type {
            StorageType::NativeClickHouse => TableInfo::NativeClickHouse {
                clickhouse_table: table
                    .clickhouse_table
                    .clone()
                    .unwrap_or_default(),
            },
            StorageType::ObjectStorage => TableInfo::ObjectStorage {
                r2_prefix: table.r2_prefix.clone(),
                file_count: None,
            },
            StorageType::External => TableInfo::External {
                source_type: crate::warehouse::types::SourceType::ExternalParquet,
                source_identifier: String::new(),
            },
        }
    }
}

/// Query router that dispatches queries based on storage type.
///
/// PERFORMANCE NOTES:
/// - Native ClickHouse: ~10-50ms for most queries (with indexes)
/// - Object Storage: ~100-500ms depending on FST selectivity
pub struct QueryRouter {
    /// ClickHouse database name
    database: String,
    /// ClickHouse named collection for S3/R2 access (avoids embedding credentials in SQL)
    s3_collection_name: String,
}

/// Result of query routing.
#[derive(Debug)]
pub struct RoutedQuery {
    /// The rewritten SQL query
    pub sql: String,
    /// Execution path used
    pub execution_path: ExecutionPath,
    /// Query settings optimized for the execution path
    pub settings: ClickHouseQuerySettings,
    /// Files to scan (only for object storage queries)
    pub files_to_scan: Option<Vec<String>>,
}

/// The execution path for a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionPath {
    /// Query executed against native ClickHouse tables
    NativeClickHouse,
    /// Query executed against S3/R2 parquet files
    ObjectStorage,
    /// Query executed against external data sources (cold tier)
    /// Data is fetched on demand and materialized as temporary tables
    External,
    /// Mixed query with tables in multiple storage types.
    /// Each table reference is rewritten according to its own storage type.
    Mixed,
}

impl QueryRouter {
    /// Create a new query router.
    pub fn new(
        database: String,
        s3_collection_name: String,
    ) -> Self {
        Self {
            database,
            s3_collection_name,
        }
    }

    /// Route a query to the appropriate execution path.
    ///
    /// DECISION LOGIC:
    /// 1. If all tables are native ClickHouse -> use native path
    /// 2. If all tables are object storage -> use s3() path with FST
    /// 3. If mixed -> rewrite each table per its storage type; reject if External tables present
    pub fn route(&self, ctx: &QueryContext) -> Result<RoutedQuery, QueryRouterError> {
        if ctx.table_info.is_empty() {
            return Err(QueryRouterError::NoTables);
        }

        // Determine execution path based on table storage types
        let path = self.determine_execution_path(&ctx.table_info);

        match path {
            ExecutionPath::NativeClickHouse => self.route_to_native(ctx),
            ExecutionPath::ObjectStorage => self.route_to_object_storage(ctx),
            ExecutionPath::External => self.route_to_external(ctx),
            ExecutionPath::Mixed => {
                self.route_mixed(ctx)
            }
        }
    }

    /// Determine the execution path based on table storage types.
    fn determine_execution_path(&self, table_info: &AHashMap<String, TableInfo>) -> ExecutionPath {
        let mut has_native = false;
        let mut has_object_storage = false;
        let mut has_external = false;

        for info in table_info.values() {
            match info {
                TableInfo::NativeClickHouse { .. } => has_native = true,
                TableInfo::ObjectStorage { .. } => has_object_storage = true,
                TableInfo::External { .. } => has_external = true,
            }
        }

        // If we have external sources, we need to handle them specially
        // External sources require federation with other storage types
        if has_external {
            if has_native || has_object_storage {
                // Mixed external + internal requires federation
                return ExecutionPath::Mixed;
            }
            return ExecutionPath::External;
        }

        match (has_native, has_object_storage) {
            (true, false) => ExecutionPath::NativeClickHouse,
            (false, true) => ExecutionPath::ObjectStorage,
            (true, true) => ExecutionPath::Mixed,
            (false, false) => {
                tracing::warn!(
                    "determine_execution_path: no storage type flags set — \
                     this is unexpected when table_info is non-empty; \
                     defaulting to ObjectStorage"
                );
                ExecutionPath::ObjectStorage
            }
        }
    }

    /// Route to native ClickHouse tables.
    ///
    /// PERFORMANCE: This is the fastest path - queries run directly against
    /// MergeTree tables with native indexes.
    fn route_to_native(&self, ctx: &QueryContext) -> Result<RoutedQuery, QueryRouterError> {
        let mut replacements = AHashMap::new();
        for (table_name, info) in &ctx.table_info {
            if let TableInfo::NativeClickHouse { clickhouse_table } = info {
                replacements.insert(
                    table_name.clone(),
                    TableReplacement::QualifiedName(self.database.clone(), clickhouse_table.clone()),
                );
            }
        }

        let rewritten = rewrite_table_refs(&ctx.query, &replacements)?;

        Ok(RoutedQuery {
            sql: rewritten,
            execution_path: ExecutionPath::NativeClickHouse,
            settings: ClickHouseQuerySettings::default(),
            files_to_scan: None,
        })
    }

    /// Route to object storage via s3() function.
    ///
    /// Builds wildcard file patterns per table and generates adaptive
    /// ClickHouse settings based on the estimated file count. The actual
    /// file-level filtering (via skip indexes) happens later in the rewriter.
    fn route_to_object_storage(&self, ctx: &QueryContext) -> Result<RoutedQuery, QueryRouterError> {
        let mut files_to_scan = Vec::new();
        let mut file_count = 0usize;

        for info in ctx.table_info.values() {
            if let TableInfo::ObjectStorage { r2_prefix, file_count: fc } = info {
                files_to_scan.push(format!("{}/*.parquet", r2_prefix));
                file_count += fc.unwrap_or(DEFAULT_FILE_COUNT_ESTIMATE);
            }
        }

        let settings = ClickHouseQuerySettings::for_object_storage(file_count);

        // Rewrite table references to s3() function calls with glob patterns.
        // For skip-index-aware file selection, the HierarchicalSkipIndexTransformer
        // in the rewriter provides more targeted file patterns.
        let rewritten_sql = self.build_s3_query(&ctx.query, &ctx.table_info)?;

        Ok(RoutedQuery {
            sql: rewritten_sql,
            execution_path: ExecutionPath::ObjectStorage,
            settings,
            files_to_scan: Some(files_to_scan),
        })
    }

    /// Route to external data sources (cold tier).
    ///
    /// ARCHITECTURE: External data is fetched on-demand and materialized as
    /// Arrow RecordBatches. The query is marked for external execution, and
    /// the federation layer handles:
    /// 1. Fetching data from the external source (with TTL caching)
    /// 2. Materializing as temporary ClickHouse table or in-memory Arrow
    /// 3. Executing the query against the materialized data
    fn route_to_external(&self, ctx: &QueryContext) -> Result<RoutedQuery, QueryRouterError> {
        let external_count = ctx
            .table_info
            .values()
            .filter(|info| matches!(info, TableInfo::External { .. }))
            .count();

        if external_count == 0 {
            return Err(QueryRouterError::NoTables);
        }

        tracing::info!(
            project_id = %ctx.project_id,
            table_count = external_count,
            "Routing query to external sources"
        );

        // For external sources, we pass the query through unchanged
        // The federation layer will handle data fetching and materialization
        Ok(RoutedQuery {
            sql: ctx.query.clone(),
            execution_path: ExecutionPath::External,
            settings: ClickHouseQuerySettings::default(),
            files_to_scan: None,
        })
    }

    /// Route a mixed query (some tables native ClickHouse, some object storage,
    /// possibly external).
    ///
    /// Rewrites each table reference according to its storage type. External
    /// tables cannot be inlined into ClickHouse SQL, so the query is rejected
    /// if any External tables are present -- the federation layer should handle
    /// those queries instead.
    fn route_mixed(&self, ctx: &QueryContext) -> Result<RoutedQuery, QueryRouterError> {
        if ctx.table_info.values().any(|info| matches!(info, TableInfo::External { .. })) {
            return Err(QueryRouterError::UnsupportedMixedQuery);
        }

        let mut replacements = AHashMap::new();
        let mut file_count = 0;

        for (table_name, info) in &ctx.table_info {
            match info {
                TableInfo::NativeClickHouse { clickhouse_table } => {
                    replacements.insert(
                        table_name.clone(),
                        TableReplacement::QualifiedName(self.database.clone(), clickhouse_table.clone()),
                    );
                }
                TableInfo::ObjectStorage { r2_prefix, file_count: fc } => {
                    replacements.insert(
                        table_name.clone(),
                        TableReplacement::S3Function {
                            collection: self.s3_collection_name.clone(),
                            prefix: r2_prefix.clone(),
                        },
                    );
                    file_count += fc.unwrap_or(DEFAULT_FILE_COUNT_ESTIMATE);
                }
                TableInfo::External { .. } => {
                    return Err(QueryRouterError::UnsupportedMixedQuery);
                }
            }
        }

        let sql = rewrite_table_refs(&ctx.query, &replacements)?;

        let settings = ClickHouseQuerySettings::for_mixed_storage(file_count);
        Ok(RoutedQuery {
            sql,
            execution_path: ExecutionPath::Mixed,
            settings,
            files_to_scan: None,
        })
    }

    /// Build an s3() query from the original query.
    ///
    /// Precondition: all entries in `table_info` must be `TableInfo::ObjectStorage`.
    fn build_s3_query(
        &self,
        query: &str,
        table_info: &AHashMap<String, TableInfo>,
    ) -> Result<String, QueryRouterError> {
        let mut replacements = AHashMap::new();

        for (table_name, info) in table_info {
            if let TableInfo::ObjectStorage { r2_prefix, .. } = info {
                replacements.insert(
                    table_name.clone(),
                    TableReplacement::S3Function {
                        collection: self.s3_collection_name.clone(),
                        prefix: r2_prefix.clone(),
                    },
                );
            }
        }

        rewrite_table_refs(query, &replacements)
    }
}

/// How to replace a table reference in the AST.
#[derive(Clone)]
enum TableReplacement {
    /// Replace with a qualified `database.table` name (for native ClickHouse).
    QualifiedName(String, String),
    /// Replace with an `s3(collection, filename='prefix/*.parquet', format='Parquet')` call.
    S3Function { collection: String, prefix: String },
}

/// Parse SQL, replace table references via AST manipulation, and serialize back.
fn rewrite_table_refs(
    sql: &str,
    replacements: &AHashMap<String, TableReplacement>,
) -> Result<String, QueryRouterError> {
    let dialect = ClickHouseDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| QueryRouterError::ParseError(e.to_string()))?;
    rewrite_table_refs_ast(&mut statements, replacements);
    Ok(match &statements[..] {
        [single] => single.to_string(),
        multiple => multiple.iter().map(|s| s.to_string()).collect::<Vec<_>>().join("; "),
    })
}

/// Replace table references in pre-parsed statements (zero-parse variant).
fn rewrite_table_refs_ast(
    statements: &mut [Statement],
    replacements: &AHashMap<String, TableReplacement>,
) {
    for stmt in statements.iter_mut() {
        if let Statement::Query(query) = stmt {
            visit_query_for_replacement(query, replacements);
        }
    }
}

fn visit_query_for_replacement(
    query: &mut sqlparser::ast::Query,
    replacements: &AHashMap<String, TableReplacement>,
) {
    if let Some(with) = &mut query.with {
        let cte_names: Vec<String> = with
            .cte_tables
            .iter()
            .map(|cte| cte.alias.name.value.clone())
            .collect();

        // For each CTE body, only CTEs defined *before* it are visible as
        // shadows.  The CTE's own name is NOT yet defined when its body
        // executes (non-recursive case), so references to a table with the
        // same name as the CTE itself refer to the real table.
        for (i, cte) in with.cte_tables.iter_mut().enumerate() {
            let visible: AHashSet<&str> = cte_names[..i].iter().map(|s| s.as_str()).collect();
            let cte_replacements: AHashMap<String, TableReplacement> = replacements
                .iter()
                .filter(|(k, _)| !visible.iter().any(|cte_name| cte_name.eq_ignore_ascii_case(k)))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            visit_query_for_replacement(&mut cte.query, &cte_replacements);
        }

        // In the main query body all CTE names shadow real tables.
        let all_cte_names: AHashSet<&str> = cte_names.iter().map(|s| s.as_str()).collect();
        let filtered: AHashMap<String, TableReplacement> = replacements
            .iter()
            .filter(|(k, _)| !all_cte_names.iter().any(|cte_name| cte_name.eq_ignore_ascii_case(k)))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        visit_set_expr_for_replacement(&mut query.body, &filtered);

        if let Some(ref mut order_by) = query.order_by {
            for item in &mut order_by.exprs {
                visit_expr_for_replacement(&mut item.expr, &filtered);
            }
        }
    } else {
        visit_set_expr_for_replacement(&mut query.body, replacements);
        if let Some(ref mut order_by) = query.order_by {
            for item in &mut order_by.exprs {
                visit_expr_for_replacement(&mut item.expr, replacements);
            }
        }
    }
}

fn visit_set_expr_for_replacement(
    set_expr: &mut sqlparser::ast::SetExpr,
    replacements: &AHashMap<String, TableReplacement>,
) {
    match set_expr {
        sqlparser::ast::SetExpr::Select(select) => {
            for table_with_joins in &mut select.from {
                visit_table_with_joins_for_replacement(table_with_joins, replacements);
            }
            if let Some(ref mut selection) = select.selection {
                visit_expr_for_replacement(selection, replacements);
            }
            if let Some(ref mut having) = select.having {
                visit_expr_for_replacement(having, replacements);
            }
            if let Some(ref mut prewhere) = select.prewhere {
                visit_expr_for_replacement(prewhere, replacements);
            }
            if let sqlparser::ast::GroupByExpr::Expressions(ref mut exprs, _) = select.group_by {
                for expr in exprs {
                    visit_expr_for_replacement(expr, replacements);
                }
            }
            for item in &mut select.projection {
                if let sqlparser::ast::SelectItem::UnnamedExpr(ref mut expr)
                    | sqlparser::ast::SelectItem::ExprWithAlias { ref mut expr, .. } = item
                {
                    visit_expr_for_replacement(expr, replacements);
                }
            }
        }
        sqlparser::ast::SetExpr::Query(query) => {
            visit_query_for_replacement(query, replacements);
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            visit_set_expr_for_replacement(left, replacements);
            visit_set_expr_for_replacement(right, replacements);
        }
        _ => {}
    }
}

fn visit_expr_for_replacement(
    expr: &mut Expr,
    replacements: &AHashMap<String, TableReplacement>,
) {
    match expr {
        Expr::Subquery(query) => visit_query_for_replacement(query, replacements),
        Expr::InSubquery { subquery, expr: inner, .. } => {
            visit_query_for_replacement(subquery, replacements);
            visit_expr_for_replacement(inner, replacements);
        }
        Expr::Exists { subquery, .. } => visit_query_for_replacement(subquery, replacements),
        Expr::BinaryOp { left, right, .. } => {
            visit_expr_for_replacement(left, replacements);
            visit_expr_for_replacement(right, replacements);
        }
        Expr::UnaryOp { expr: inner, .. } | Expr::Nested(inner) => {
            visit_expr_for_replacement(inner, replacements);
        }
        Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsTrue(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsFalse(inner)
        | Expr::IsNotFalse(inner)
        | Expr::IsUnknown(inner)
        | Expr::IsNotUnknown(inner) => {
            visit_expr_for_replacement(inner, replacements);
        }
        Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
            visit_expr_for_replacement(left, replacements);
            visit_expr_for_replacement(right, replacements);
        }
        Expr::Between { expr: inner, low, high, .. } => {
            visit_expr_for_replacement(inner, replacements);
            visit_expr_for_replacement(low, replacements);
            visit_expr_for_replacement(high, replacements);
        }
        Expr::InList { expr: inner, list, .. } => {
            visit_expr_for_replacement(inner, replacements);
            for item in list {
                visit_expr_for_replacement(item, replacements);
            }
        }
        Expr::Case { operand, conditions, results, else_result, .. } => {
            if let Some(op) = operand {
                visit_expr_for_replacement(op, replacements);
            }
            for cond in conditions {
                visit_expr_for_replacement(cond, replacements);
            }
            for res in results {
                visit_expr_for_replacement(res, replacements);
            }
            if let Some(el) = else_result {
                visit_expr_for_replacement(el, replacements);
            }
        }
        Expr::Cast { expr: inner, .. } => {
            visit_expr_for_replacement(inner, replacements);
        }
        Expr::Function(func) => {
            if let FunctionArguments::List(ref mut list) = func.args {
                for arg in &mut list.args {
                    let inner = match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(ref mut e)) => Some(e),
                        FunctionArg::Named { arg: FunctionArgExpr::Expr(ref mut e), .. } => Some(e),
                        _ => None,
                    };
                    if let Some(e) = inner {
                        visit_expr_for_replacement(e, replacements);
                    }
                }
            }
        }
        Expr::Like { expr: inner, pattern, .. } | Expr::ILike { expr: inner, pattern, .. } => {
            visit_expr_for_replacement(inner, replacements);
            visit_expr_for_replacement(pattern, replacements);
        }
        Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
            visit_expr_for_replacement(left, replacements);
            visit_expr_for_replacement(right, replacements);
        }
        Expr::InUnnest { expr: inner, array_expr, .. } => {
            visit_expr_for_replacement(inner, replacements);
            visit_expr_for_replacement(array_expr, replacements);
        }
        Expr::Tuple(exprs) => {
            for e in exprs {
                visit_expr_for_replacement(e, replacements);
            }
        }
        _ => {}
    }
}

fn visit_table_with_joins_for_replacement(
    table: &mut TableWithJoins,
    replacements: &AHashMap<String, TableReplacement>,
) {
    visit_table_factor_for_replacement(&mut table.relation, replacements);
    for join in &mut table.joins {
        visit_table_factor_for_replacement(&mut join.relation, replacements);
        match &mut join.join_operator {
            JoinOperator::Inner(c)
            | JoinOperator::LeftOuter(c)
            | JoinOperator::RightOuter(c)
            | JoinOperator::FullOuter(c)
            | JoinOperator::LeftSemi(c)
            | JoinOperator::RightSemi(c)
            | JoinOperator::LeftAnti(c)
            | JoinOperator::RightAnti(c) => {
                if let JoinConstraint::On(ref mut expr) = c {
                    visit_expr_for_replacement(expr, replacements);
                }
            }
            _ => {}
        }
    }
}

fn visit_table_factor_for_replacement(
    factor: &mut TableFactor,
    replacements: &AHashMap<String, TableReplacement>,
) {
    match factor {
        TableFactor::Table { ref name, .. } => {
            let table_name = name.0.last().map(|i| i.value.clone()).unwrap_or_default();

            let replacement = replacements.get(&table_name).or_else(|| {
                let lower = table_name.to_lowercase();
                replacements.iter().find(|(k, _)| k.to_lowercase() == lower).map(|(_, v)| v)
            });

            if let Some(replacement) = replacement {
                apply_table_replacement(factor, replacement);
            }
        }
        TableFactor::Derived { subquery, .. } => {
            visit_query_for_replacement(subquery, replacements);
        }
        TableFactor::NestedJoin { table_with_joins, .. } => {
            visit_table_with_joins_for_replacement(table_with_joins, replacements);
        }
        _ => {}
    }
}

fn apply_table_replacement(
    factor: &mut TableFactor,
    replacement: &TableReplacement,
) {
    match replacement {
        TableReplacement::QualifiedName(database, table) => {
            if let TableFactor::Table { ref mut name, .. } = factor {
                *name = ObjectName(vec![
                    Ident::with_quote('`', database),
                    Ident::with_quote('`', table),
                ]);
            }
        }
        TableReplacement::S3Function { collection, prefix } => {
            let alias = if let TableFactor::Table { alias, .. } = factor {
                alias.take()
            } else {
                None
            };
            let s3_args = vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(
                    Ident::new(collection),
                ))),
                FunctionArg::Named {
                    name: Ident::new("filename"),
                    arg: FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(
                        format!("{}/*.parquet", prefix),
                    ))),
                    operator: sqlparser::ast::FunctionArgOperator::Equals,
                },
                FunctionArg::Named {
                    name: Ident::new("format"),
                    arg: FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(
                        "Parquet".to_string(),
                    ))),
                    operator: sqlparser::ast::FunctionArgOperator::Equals,
                },
            ];
            *factor = TableFactor::Function {
                lateral: false,
                name: ObjectName(vec![Ident::new("s3")]),
                args: s3_args,
                alias,
            };
        }
    }
}

/// Errors that can occur during query routing.
#[derive(Debug, thiserror::Error)]
pub enum QueryRouterError {
    #[error("No tables found in query")]
    NoTables,

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Query parsing error: {0}")]
    ParseError(String),

    #[error("Unsupported query type for mixed storage")]
    UnsupportedMixedQuery,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_router() -> QueryRouter {
        QueryRouter::new(
            "default".to_string(),
            "r2_warehouse".to_string(),
        )
    }

    fn make_native_table(ch_table: &str) -> TableInfo {
        TableInfo::NativeClickHouse {
            clickhouse_table: ch_table.to_string(),
        }
    }

    fn make_object_table(prefix: &str) -> TableInfo {
        TableInfo::ObjectStorage {
            r2_prefix: prefix.to_string(),
            file_count: None,
        }
    }

    fn make_external_table() -> TableInfo {
        TableInfo::External {
            source_type: crate::warehouse::types::SourceType::GoogleSheets,
            source_identifier: "spreadsheet_id.Sheet1".to_string(),
        }
    }

    #[test]
    fn test_execution_path_native_only() {
        let router = test_router();
        let mut table_info = AHashMap::new();
        table_info.insert("customers".to_string(), make_native_table("warehouse_project_customers"));

        let path = router.determine_execution_path(&table_info);
        assert_eq!(path, ExecutionPath::NativeClickHouse);
    }

    #[test]
    fn test_execution_path_object_storage_only() {
        let router = test_router();
        let mut table_info = AHashMap::new();
        table_info.insert("events".to_string(), make_object_table("project/events"));

        let path = router.determine_execution_path(&table_info);
        assert_eq!(path, ExecutionPath::ObjectStorage);
    }

    #[test]
    fn test_execution_path_mixed() {
        let router = test_router();
        let mut table_info = AHashMap::new();
        table_info.insert("customers".to_string(), make_native_table("warehouse_project_customers"));
        table_info.insert("events".to_string(), make_object_table("project/events"));

        let path = router.determine_execution_path(&table_info);
        assert_eq!(path, ExecutionPath::Mixed);
    }

    #[test]
    fn test_execution_path_external_only() {
        let router = test_router();
        let mut table_info = AHashMap::new();
        table_info.insert("budget".to_string(), make_external_table());

        let path = router.determine_execution_path(&table_info);
        assert_eq!(path, ExecutionPath::External);
    }

    #[test]
    fn test_execution_path_external_mixed() {
        let router = test_router();
        let mut table_info = AHashMap::new();
        table_info.insert("customers".to_string(), make_native_table("warehouse_project_customers"));
        table_info.insert("budget".to_string(), make_external_table());

        let path = router.determine_execution_path(&table_info);
        assert_eq!(path, ExecutionPath::Mixed);
    }

    #[test]
    fn test_route_native_query() {
        let router = test_router();
        let mut table_info = AHashMap::new();
        table_info.insert("customers".to_string(), make_native_table("warehouse_project_customers"));

        let ctx = QueryContext {
            project_id: Uuid::new_v4(),
            query: "SELECT * FROM customers".to_string(),
            table_info,
            skip_index: None,
        };

        let routed = router.route(&ctx).unwrap();
        assert_eq!(routed.execution_path, ExecutionPath::NativeClickHouse);
        assert!(routed.sql.contains("warehouse_project_customers"));
    }

    #[test]
    fn test_route_object_storage_query() {
        let router = test_router();
        let mut table_info = AHashMap::new();
        table_info.insert("events".to_string(), make_object_table("project/events"));

        let ctx = QueryContext {
            project_id: Uuid::new_v4(),
            query: "SELECT * FROM events".to_string(),
            table_info,
            skip_index: None,
        };

        let routed = router.route(&ctx).unwrap();
        assert_eq!(routed.execution_path, ExecutionPath::ObjectStorage);
        assert!(routed.sql.contains("s3("));
        assert!(routed.files_to_scan.is_some());
    }

    #[test]
    fn test_route_external_query() {
        let router = test_router();
        let mut table_info = AHashMap::new();
        table_info.insert("budget".to_string(), make_external_table());

        let ctx = QueryContext {
            project_id: Uuid::new_v4(),
            query: "SELECT * FROM budget".to_string(),
            table_info,
            skip_index: None,
        };

        let routed = router.route(&ctx).unwrap();
        assert_eq!(routed.execution_path, ExecutionPath::External);
    }

    #[test]
    fn test_execution_path_empty_tables_defaults_to_object_storage() {
        let router = test_router();
        let table_info: AHashMap<String, TableInfo> = AHashMap::new();
        let path = router.determine_execution_path(&table_info);
        assert_eq!(path, ExecutionPath::ObjectStorage);
    }

    #[test]
    fn test_ast_rewrite_ignores_string_literals() {
        let mut replacements = AHashMap::new();
        replacements.insert(
            "users".to_string(),
            TableReplacement::QualifiedName("db".to_string(), "ch_users".to_string()),
        );
        let sql = "SELECT * FROM users WHERE note = 'active users list'";
        let result = rewrite_table_refs(sql, &replacements).unwrap();
        assert!(
            result.contains("ch_users"),
            "Table reference must be replaced: {}",
            result
        );
        assert!(
            result.contains("active users list"),
            "String literal must be preserved: {}",
            result
        );
    }

    #[test]
    fn test_ast_rewrite_does_not_touch_column_names() {
        let mut replacements = AHashMap::new();
        replacements.insert(
            "users".to_string(),
            TableReplacement::QualifiedName("db".to_string(), "ch_users".to_string()),
        );
        let sql = "SELECT users_count FROM users";
        let result = rewrite_table_refs(sql, &replacements).unwrap();
        assert!(
            result.contains("users_count"),
            "Column name must not be replaced: {}",
            result
        );
        assert!(
            result.contains("ch_users"),
            "Table reference must be replaced: {}",
            result
        );
    }

    #[test]
    fn test_route_mixed_rejects_external_tables() {
        let router = test_router();
        let mut table_info = AHashMap::new();
        table_info.insert("customers".to_string(), make_native_table("warehouse_project_customers"));
        table_info.insert("budget".to_string(), make_external_table());

        let ctx = QueryContext {
            project_id: Uuid::new_v4(),
            query: "SELECT * FROM customers JOIN budget ON customers.id = budget.id".to_string(),
            table_info,
            skip_index: None,
        };

        let result = router.route(&ctx);
        assert!(result.is_err(),
            "Mixed queries with External tables should be rejected, got: {:?}", result);
    }

    #[test]
    fn test_route_object_storage_file_count_not_multiplied_by_tables() {
        use crate::warehouse::indexes::skip_index::{FileSkipIndex, HierarchicalSkipIndex};

        let router = test_router();

        let mut idx = HierarchicalSkipIndex::new();
        for i in 0..5 {
            let file_idx = FileSkipIndex::build(
                &format!("file_{}.parquet", i),
                std::collections::HashMap::new(),
            )
            .unwrap();
            idx.add_file("2025/01", file_idx, 100).unwrap();
        }
        assert_eq!(idx.total_files(), 5);

        let mut table_info = AHashMap::new();
        table_info.insert("events".to_string(), make_object_table("project/events"));
        table_info.insert("pageviews".to_string(), make_object_table("project/pageviews"));
        table_info.insert("sessions".to_string(), make_object_table("project/sessions"));

        let ctx = QueryContext {
            project_id: Uuid::new_v4(),
            query: "SELECT * FROM events JOIN pageviews ON events.id = pageviews.event_id JOIN sessions ON events.session_id = sessions.id".to_string(),
            table_info,
            skip_index: Some(Arc::new(idx)),
        };

        let routed = router.route(&ctx).unwrap();
        assert_eq!(routed.settings.s3_max_connections, 100);
        assert_eq!(routed.files_to_scan.as_ref().unwrap().len(), 3);
    }

    #[test]
    fn test_route_mixed_uses_actual_file_count() {
        let router = test_router();
        let mut table_info = AHashMap::new();
        table_info.insert("customers".to_string(), make_native_table("wh_customers"));

        table_info.insert("events".to_string(), TableInfo::ObjectStorage {
            r2_prefix: "project/events".to_string(),
            file_count: Some(42),
        });

        let ctx = QueryContext {
            project_id: Uuid::new_v4(),
            query: "SELECT * FROM customers JOIN events ON customers.id = events.customer_id"
                .to_string(),
            table_info,
            skip_index: None,
        };

        let routed = router.route(&ctx).unwrap();
        assert_eq!(routed.execution_path, ExecutionPath::Mixed);
        let expected_connections = (42_usize * 2).clamp(100, 500) as u32;
        assert_eq!(
            routed.settings.s3_max_connections, expected_connections,
            "Mixed route must use actual file_count (42) not default 10"
        );
    }

    #[test]
    fn test_route_mixed_falls_back_to_default_file_count() {
        let router = test_router();
        let mut table_info = AHashMap::new();
        table_info.insert("customers".to_string(), make_native_table("wh_customers"));
        table_info.insert("events".to_string(), make_object_table("project/events"));

        let ctx = QueryContext {
            project_id: Uuid::new_v4(),
            query: "SELECT * FROM customers JOIN events ON customers.id = events.customer_id"
                .to_string(),
            table_info,
            skip_index: None,
        };

        let routed = router.route(&ctx).unwrap();
        assert_eq!(routed.execution_path, ExecutionPath::Mixed);
        let expected_connections = (10_usize * 2).clamp(100, 500) as u32;
        assert_eq!(
            routed.settings.s3_max_connections, expected_connections,
            "Mixed route must fall back to default 10 when file_count is None"
        );
    }

    #[test]
    fn test_object_storage_route_uses_default_file_count_when_no_skip_index() {
        let router = test_router();
        let mut table_info = AHashMap::new();
        table_info.insert("orders".to_string(), make_object_table("proj/orders"));

        let ctx = QueryContext {
            project_id: Uuid::new_v4(),
            query: "SELECT * FROM orders".to_string(),
            table_info,
            skip_index: None,
        };

        let routed = router.route(&ctx).unwrap();
        assert_eq!(routed.execution_path, ExecutionPath::ObjectStorage);

        let default_settings =
            ClickHouseQuerySettings::for_object_storage(DEFAULT_FILE_COUNT_ESTIMATE);
        assert_eq!(
            routed.settings.s3_max_connections, default_settings.s3_max_connections,
            "Object storage route must use DEFAULT_FILE_COUNT_ESTIMATE when skip_index is None"
        );
    }

    // Tests for missing r2_prefix / clickhouse_table are no longer needed:
    // the TableInfo enum makes invalid states unrepresentable.

    #[test]
    fn test_rewrite_replaces_table_in_join_on_subquery() {
        let mut replacements = AHashMap::new();
        replacements.insert(
            "orders".to_string(),
            TableReplacement::QualifiedName("db".to_string(), "ch_orders".to_string()),
        );
        replacements.insert(
            "customers".to_string(),
            TableReplacement::QualifiedName("db".to_string(), "ch_customers".to_string()),
        );
        replacements.insert(
            "latest_orders".to_string(),
            TableReplacement::QualifiedName("db".to_string(), "ch_latest_orders".to_string()),
        );

        let sql = "SELECT * FROM orders \
                    JOIN customers ON orders.customer_id IN \
                    (SELECT customer_id FROM latest_orders WHERE active = 1)";
        let result = rewrite_table_refs(sql, &replacements).unwrap();

        assert!(
            result.contains("ch_orders"),
            "orders table must be replaced: {}",
            result
        );
        assert!(
            result.contains("ch_customers"),
            "customers table must be replaced: {}",
            result
        );
        assert!(
            result.contains("ch_latest_orders"),
            "latest_orders in JOIN ON subquery must be replaced: {}",
            result
        );
    }

    #[test]
    fn test_mixed_query_uses_conservative_thread_count() {
        let router = test_router();
        let mut table_info = AHashMap::new();
        table_info.insert("customers".to_string(), make_native_table("wh_customers"));

        table_info.insert("events".to_string(), TableInfo::ObjectStorage {
            r2_prefix: "project/events".to_string(),
            file_count: Some(200),
        });

        let ctx = QueryContext {
            project_id: Uuid::new_v4(),
            query: "SELECT * FROM customers JOIN events ON customers.id = events.customer_id"
                .to_string(),
            table_info,
            skip_index: None,
        };

        let routed = router.route(&ctx).unwrap();
        assert_eq!(routed.execution_path, ExecutionPath::Mixed);

        let pure_obj = ClickHouseQuerySettings::for_object_storage(200);
        assert!(
            routed.settings.max_threads <= 8,
            "Mixed query threads ({}) must be capped at 8 (pure object storage uses {})",
            routed.settings.max_threads,
            pure_obj.max_threads
        );
        assert!(
            routed.settings.max_threads <= pure_obj.max_threads,
            "Mixed query threads must not exceed pure object storage threads"
        );
    }

    #[test]
    fn test_execution_path_empty_table_info_defaults_to_object_storage() {
        let router = test_router();
        let table_info: AHashMap<String, TableInfo> = AHashMap::new();

        let path = router.determine_execution_path(&table_info);
        assert_eq!(
            path,
            ExecutionPath::ObjectStorage,
            "Empty table_info should default to ObjectStorage as a safe fallback"
        );
    }

    #[test]
    fn test_cte_name_collision_not_rewritten_in_sibling_body() {
        let mut replacements = AHashMap::new();
        replacements.insert(
            "orders".to_string(),
            TableReplacement::QualifiedName("db".to_string(), "ch_orders".to_string()),
        );

        let sql = "WITH orders AS (SELECT * FROM raw_orders), \
                    summary AS (SELECT * FROM orders) \
                    SELECT * FROM summary";
        let result = rewrite_table_refs(sql, &replacements).unwrap();

        assert!(
            !result.contains("ch_orders"),
            "CTE reference 'orders' inside sibling CTE body must NOT be rewritten \
             to the warehouse table: {}",
            result
        );
    }

    #[test]
    fn test_cte_self_referencing_body_is_rewritten() {
        let mut replacements = AHashMap::new();
        replacements.insert(
            "orders".to_string(),
            TableReplacement::QualifiedName("db".to_string(), "ch_orders".to_string()),
        );

        let sql = "WITH orders AS (SELECT * FROM orders WHERE region = 'US') \
                    SELECT * FROM orders";
        let result = rewrite_table_refs(sql, &replacements).unwrap();

        assert!(
            result.contains("ch_orders"),
            "Inside the CTE body, 'FROM orders' refers to the real table \
             and must be rewritten: {}",
            result
        );

        let main_body_after_cte = result.split(')').last().unwrap_or("");
        assert!(
            !main_body_after_cte.contains("ch_orders"),
            "In the main query body, 'FROM orders' refers to the CTE \
             and must NOT be rewritten: {}",
            result
        );
    }

    #[test]
    fn test_cte_shadowing_case_insensitive() {
        let mut replacements = AHashMap::new();
        replacements.insert(
            "orders".to_string(),
            TableReplacement::QualifiedName("db".to_string(), "ch_orders".to_string()),
        );

        let sql = "WITH Orders AS (SELECT * FROM orders WHERE status = 'active') SELECT * FROM Orders";
        let result = rewrite_table_refs(sql, &replacements).unwrap();

        let cte_body = result.split("SELECT * FROM Orders").next().unwrap_or("");
        assert!(
            cte_body.contains("ch_orders"),
            "Inside the CTE body, 'FROM orders' refers to the real table \
             and must be rewritten: {}",
            result
        );

        let main_query = result.split(')').last().unwrap_or("");
        assert!(
            !main_query.contains("ch_orders"),
            "In the main query, 'FROM Orders' refers to the CTE (case-insensitive) \
             and must NOT be rewritten to ch_orders: {}",
            result
        );
    }

    #[test]
    fn test_prewhere_subquery_table_refs_rewritten() {
        let mut replacements = AHashMap::new();
        replacements.insert(
            "users".to_string(),
            TableReplacement::QualifiedName("db".to_string(), "ch_users".to_string()),
        );
        let sql = "SELECT * FROM events PREWHERE user_id IN (SELECT id FROM users)";
        let result = rewrite_table_refs(sql, &replacements).unwrap();
        assert!(
            result.contains("ch_users"),
            "Table reference inside PREWHERE must be rewritten: {}",
            result
        );
    }

    #[test]
    fn test_is_true_subquery_table_refs_rewritten() {
        let mut replacements = AHashMap::new();
        replacements.insert(
            "t1".to_string(),
            TableReplacement::QualifiedName("db".to_string(), "ch_t1".to_string()),
        );
        let sql = "SELECT * FROM events WHERE (SELECT flag FROM t1 LIMIT 1) IS TRUE";
        let result = rewrite_table_refs(sql, &replacements).unwrap();
        assert!(
            result.contains("ch_t1"),
            "Table reference inside IS TRUE subquery must be rewritten: {}",
            result
        );
    }

    #[test]
    fn test_is_distinct_from_subquery_table_refs_rewritten() {
        let mut replacements = AHashMap::new();
        replacements.insert(
            "t1".to_string(),
            TableReplacement::QualifiedName("db".to_string(), "ch_t1".to_string()),
        );
        let sql = "SELECT * FROM events WHERE col IS DISTINCT FROM (SELECT val FROM t1 LIMIT 1)";
        let result = rewrite_table_refs(sql, &replacements).unwrap();
        assert!(
            result.contains("ch_t1"),
            "Table reference inside IS DISTINCT FROM subquery must be rewritten: {}",
            result
        );
    }
}
