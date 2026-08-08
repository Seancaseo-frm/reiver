//! Query handlers for the pgwire server.
//!
//! Implements `SimpleQueryHandler` and `ExtendedQueryHandler` to execute
//! SQL queries through Pond's existing query engine. The handlers:
//!
//! 1. Extract the `project_id` from connection metadata (set during auth)
//! 2. Load project tables from the database
//! 3. Rewrite SQL for ClickHouse storage (table name → s3() function calls)
//! 4. Execute against ClickHouse via the shared `QueryExecutor`
//! 5. Encode results into pgwire row format

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use futures::{Sink, StreamExt};
use uuid::Uuid;

use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldInfo,
    QueryResponse, Response,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use super::auth::METADATA_PROJECT_ID;
use super::catalog::{is_catalog_query, is_catalog_query_ast, CatalogQueryEngine};
use super::session::{
    self, classify_session_command, QueryClass,
};
use super::types::{
    column_info_to_field_info, encode_klickhouse_value, native_column_to_field_info,
};
use crate::app_state::PondState;
use crate::warehouse::query::executor::{ExecutionOptions, NativeBlockStream};
use crate::warehouse::query::limiter::QueryPermit;
use crate::warehouse::query::rewriter::TableRewriter;
use crate::warehouse::types::R2TablePath;

/// Prefix used to store session parameters in client metadata.
pub const SESSION_PREFIX: &str = "session:";

/// Default query timeout for pgwire connections (seconds).
const PGWIRE_QUERY_TIMEOUT_SECS: u32 = 120;

/// Maximum memory for buffered results (200 MB).
const PGWIRE_MAX_RESULT_MEMORY_BYTES: usize = 200 * 1024 * 1024;

/// Maximum rows returned through pgwire (safety valve).
const PGWIRE_DEFAULT_ROW_LIMIT: u32 = 100_000;

/// Result of `prepare_query`: rewritten SQL and the held concurrency permit.
struct PreparedQuery {
    rewritten_sql: String,
    _query_permit: QueryPermit,
    hot_backing_tables: ahash::AHashMap<String, R2TablePath>,
}

/// Query handler that routes SQL through Pond's warehouse query engine.
///
/// Incoming SQL is classified and dispatched to one of three paths:
/// 1. **Session commands** (SET/SHOW/BEGIN/COMMIT/etc.) -- handled in-process
/// 2. **Catalog queries** (pg_catalog/information_schema) -- DataFusion engine
/// 3. **Data queries** -- ClickHouse via QueryExecutor
type ProjectTables = (
    ahash::AHashMap<String, R2TablePath>,
    ahash::AHashMap<String, String>,
    ahash::AHashMap<String, R2TablePath>,
);

pub struct PondQueryHandler {
    state: Arc<PondState>,
    catalog_engine: CatalogQueryEngine,
    query_parser: Arc<NoopQueryParser>,
    cold_source_cache: quick_cache::sync::Cache<Uuid, (bool, std::time::Instant)>,
    project_tables_cache: quick_cache::sync::Cache<Uuid, (ProjectTables, std::time::Instant)>,
}

impl PondQueryHandler {
    pub fn new(state: Arc<PondState>) -> Self {
        let catalog_engine = CatalogQueryEngine::new(state.db.clone());
        Self {
            state,
            catalog_engine,
            query_parser: Arc::new(NoopQueryParser::new()),
            cold_source_cache: quick_cache::sync::Cache::new(256),
            project_tables_cache: quick_cache::sync::Cache::new(256),
        }
    }

    fn get_cached_has_cold(&self, project_id: Uuid) -> Option<bool> {
        let (val, ts) = self.cold_source_cache.get(&project_id)?;
        if ts.elapsed() < std::time::Duration::from_secs(30) {
            Some(val)
        } else {
            self.cold_source_cache.remove(&project_id);
            None
        }
    }

    fn set_cached_has_cold(&self, project_id: Uuid, has_cold: bool) {
        self.cold_source_cache.insert(project_id, (has_cold, std::time::Instant::now()));
    }

    /// Extract the project_id from connection metadata (set during auth).
    fn get_project_id<C: ClientInfo>(&self, client: &C) -> PgWireResult<Uuid> {
        let project_id_str = client
            .metadata()
            .get(METADATA_PROJECT_ID)
            .ok_or_else(|| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "28000".to_owned(),
                    "Not authenticated: missing project_id".to_owned(),
                )))
            })?;

        Uuid::parse_str(project_id_str).map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("Invalid project_id in session: {}", e),
            )))
        })
    }

    /// Execute a SQL query with full routing: session -> catalog -> data.
    ///
    /// This is the main query router. It classifies the incoming SQL and
    /// dispatches to the appropriate handler:
    /// 1. Session commands (SET/SHOW/BEGIN/etc.) -- handled in-process via client metadata
    /// 2. Read-only guard -- reject non-SELECT statements
    /// 3. Catalog queries (pg_catalog/information_schema) -- DataFusion engine
    /// 4. Data queries -- ClickHouse via QueryExecutor
    async fn execute_query<C: ClientInfo + Send + Sync>(
        &self,
        client: &mut C,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        // ── Step 1: Session commands (no DB round-trip) ──
        if let Some(class) = classify_session_command(sql) {
            return self.handle_session_command(client, class);
        }

        // ── Step 2: Parse once and validate ──
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;

        let pg_dialect = PostgreSqlDialect {};
        let statements = match Parser::parse_sql(&pg_dialect, sql) {
            Ok(stmts) => stmts,
            Err(_) => {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    "Cannot validate unparseable SQL in read-only mode; statement rejected"
                        .to_owned(),
                ))));
            }
        };

        Self::enforce_read_only_ast(&statements, sql)?;

        let project_id = self.get_project_id(client)?;

        // ── Step 3: Catalog queries (DataFusion) ──
        if is_catalog_query_ast(&statements, sql) {
            return self.catalog_engine.execute(project_id, sql).await;
        }

        // ── Step 3b: UDF detection ──
        if let Some(registry) = &self.state.udf_registry {
            if let Some(udf_name) = self.detect_udf_in_ast(&statements, project_id, registry) {
                return self.execute_udf_query(project_id, sql, &statements, &udf_name).await;
            }
        }

        // ── Step 4: Data queries (ClickHouse) ──
        self.execute_data_query(project_id, sql, statements).await
    }

    /// Same read-only check but takes pre-parsed AST to avoid re-parsing.
    fn enforce_read_only_ast(
        statements: &[sqlparser::ast::Statement],
        raw_sql: &str,
    ) -> PgWireResult<()> {
        if statements.is_empty() && !raw_sql.trim().is_empty() {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42501".to_owned(),
                "Statement could not be validated as read-only; rejected for safety"
                    .to_owned(),
            ))));
        }

        for stmt in statements {
            match stmt {
                sqlparser::ast::Statement::Query(_) => {}
                sqlparser::ast::Statement::Explain { .. }
                | sqlparser::ast::Statement::ExplainTable { .. } => {}
                _ => {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "42501".to_owned(),
                        "Write operations are not permitted through this interface"
                            .to_owned(),
                    ))));
                }
            }
        }

        Ok(())
    }

    /// Walk the AST looking for function calls that match a registered UDF.
    /// Checks all expression positions: SELECT, WHERE, HAVING, ORDER BY, and
    /// recurses into subqueries and set operations.
    fn detect_udf_in_ast(
        &self,
        statements: &[sqlparser::ast::Statement],
        project_id: Uuid,
        registry: &crate::warehouse::udf::UdfRegistry,
    ) -> Option<String> {
        use sqlparser::ast::Statement;

        for stmt in statements {
            if let Statement::Query(query) = stmt {
                if let Some(name) = self.detect_udf_in_query(query, project_id, registry) {
                    return Some(name);
                }
            }
        }
        None
    }

    fn detect_udf_in_query(
        &self,
        query: &sqlparser::ast::Query,
        project_id: Uuid,
        registry: &crate::warehouse::udf::UdfRegistry,
    ) -> Option<String> {
        if let Some(name) = self.detect_udf_in_set_expr(&query.body, project_id, registry) {
            return Some(name);
        }
        if let Some(ref order_by) = query.order_by {
            for order_by_expr in &order_by.exprs {
                if let Some(name) = self.extract_udf_function_name(&order_by_expr.expr, project_id, registry) {
                    return Some(name);
                }
            }
        }
        None
    }

    fn detect_udf_in_set_expr(
        &self,
        set_expr: &sqlparser::ast::SetExpr,
        project_id: Uuid,
        registry: &crate::warehouse::udf::UdfRegistry,
    ) -> Option<String> {
        use sqlparser::ast::{SelectItem, SetExpr};

        match set_expr {
            SetExpr::Select(select) => {
                for item in &select.projection {
                    if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item {
                        if let Some(name) = self.extract_udf_function_name(expr, project_id, registry) {
                            return Some(name);
                        }
                    }
                }
                if let Some(ref selection) = select.selection {
                    if let Some(name) = self.extract_udf_function_name(selection, project_id, registry) {
                        return Some(name);
                    }
                }
                if let Some(ref having) = select.having {
                    if let Some(name) = self.extract_udf_function_name(having, project_id, registry) {
                        return Some(name);
                    }
                }
                None
            }
            SetExpr::Query(query) => self.detect_udf_in_query(query, project_id, registry),
            SetExpr::SetOperation { left, right, .. } => {
                self.detect_udf_in_set_expr(left, project_id, registry)
                    .or_else(|| self.detect_udf_in_set_expr(right, project_id, registry))
            }
            _ => None,
        }
    }

    /// Recursively search an expression tree for a registered UDF function call.
    fn extract_udf_function_name(
        &self,
        expr: &sqlparser::ast::Expr,
        project_id: Uuid,
        registry: &crate::warehouse::udf::UdfRegistry,
    ) -> Option<String> {
        use sqlparser::ast::Expr;

        match expr {
            Expr::Function(func) => {
                let func_name = func.name.to_string();
                if registry.get(project_id, &func_name).is_some() {
                    return Some(func_name);
                }
                None
            }
            Expr::BinaryOp { left, right, .. } => {
                self.extract_udf_function_name(left, project_id, registry)
                    .or_else(|| self.extract_udf_function_name(right, project_id, registry))
            }
            Expr::UnaryOp { expr, .. } => {
                self.extract_udf_function_name(expr, project_id, registry)
            }
            Expr::Nested(inner) => {
                self.extract_udf_function_name(inner, project_id, registry)
            }
            Expr::Cast { expr, .. } => {
                self.extract_udf_function_name(expr, project_id, registry)
            }
            Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
                self.extract_udf_function_name(inner, project_id, registry)
            }
            Expr::Between { expr, low, high, .. } => {
                self.extract_udf_function_name(expr, project_id, registry)
                    .or_else(|| self.extract_udf_function_name(low, project_id, registry))
                    .or_else(|| self.extract_udf_function_name(high, project_id, registry))
            }
            Expr::Case { operand, conditions, results, else_result, .. } => {
                if let Some(op) = operand {
                    if let Some(name) = self.extract_udf_function_name(op, project_id, registry) {
                        return Some(name);
                    }
                }
                for cond in conditions {
                    if let Some(name) = self.extract_udf_function_name(cond, project_id, registry) {
                        return Some(name);
                    }
                }
                for res in results {
                    if let Some(name) = self.extract_udf_function_name(res, project_id, registry) {
                        return Some(name);
                    }
                }
                if let Some(else_r) = else_result {
                    return self.extract_udf_function_name(else_r, project_id, registry);
                }
                None
            }
            Expr::Subquery(query) => {
                self.detect_udf_in_query(query, project_id, registry)
            }
            _ => None,
        }
    }

    /// Execute a query that contains a UDF function call.
    ///
    /// SQL UDF execution requires a DataFusion ScalarUDF integration layer
    /// that bridges the Wasm batch-oriented execution model with DataFusion's
    /// columnar evaluation. The implementation path is:
    ///
    /// 1. Create a `ScalarUDFImpl` wrapping the Wasm module and ArrowWasmBridge
    /// 2. Register it in a DataFusion SessionContext with a table provider
    ///    that reads from ClickHouse (or from the connector system)
    /// 3. Route UDF-containing queries through DataFusion instead of ClickHouse
    ///
    /// This is blocked on having a ClickHouse -> Arrow table provider for
    /// DataFusion, since warehouse data lives in ClickHouse and the existing
    /// query executor returns native blocks, not Arrow RecordBatch.
    async fn execute_udf_query(
        &self,
        project_id: Uuid,
        _sql: &str,
        _statements: &[sqlparser::ast::Statement],
        udf_name: &str,
    ) -> PgWireResult<Vec<Response>> {
        let registry = self.state.udf_registry.as_ref().ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                "UDF system not initialized".to_owned(),
            )))
        })?;

        let compiled = registry.get(project_id, udf_name).ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42883".to_owned(),
                format!("UDF '{}' not found", udf_name),
            )))
        })?;

        let mode_hint = match compiled.execution_mode {
            crate::warehouse::udf::ExecutionMode::Job => {
                format!(
                    " This UDF is configured as a data movement job. \
                     Trigger it via POST /projects/<id>/warehouse/jobs/{}/run",
                    udf_name
                )
            }
            crate::warehouse::udf::ExecutionMode::SqlFunction => {
                " Inline SQL function execution is planned but not yet available. \
                 You can use the job API to run this UDF as a data movement pipeline."
                    .to_string()
            }
        };

        Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "0A000".to_owned(),
            format!(
                "UDF '{}' is registered but SQL execution is not yet supported.{}",
                udf_name, mode_hint
            ),
        ))))
    }

    /// Handle a session command using client metadata for storage.
    fn handle_session_command<C: ClientInfo>(
        &self,
        client: &mut C,
        class: QueryClass,
    ) -> PgWireResult<Vec<Response>> {
        use pgwire::api::results::Tag;

        match class {
            QueryClass::Set { key, value } => {
                client
                    .metadata_mut()
                    .insert(format!("{}{}", SESSION_PREFIX, key), value);
                Ok(vec![Response::Execution(Tag::new("SET"))])
            }
            QueryClass::Show { key } => {
                // Look up in client metadata first, then fall back to defaults
                let meta_key = format!("{}{}", SESSION_PREFIX, key);
                let value = client
                    .metadata()
                    .get(&meta_key)
                    .cloned()
                    .unwrap_or_else(|| {
                        session::default_value_for(&key)
                            .unwrap_or_default()
                            .to_owned()
                    });

                let field = FieldInfo::new(
                    key.clone(),
                    None,
                    None,
                    Type::TEXT,
                    pgwire::api::results::FieldFormat::Text,
                );
                let fields = Arc::new(vec![field]);
                let mut encoder = DataRowEncoder::new(fields.clone());
                encoder.encode_field(&value)?;
                let row = encoder.take_row();
                let row_stream = stream::iter(vec![Ok(row)]);
                let mut response = QueryResponse::new(fields, row_stream);
                response.set_command_tag("SELECT 1");
                Ok(vec![Response::Query(response)])
            }
            QueryClass::ShowAll => {
                // Postgres SHOW ALL returns three columns: name, setting, description
                let name_field = FieldInfo::new(
                    "name".to_owned(), None, None, Type::TEXT,
                    pgwire::api::results::FieldFormat::Text,
                );
                let setting_field = FieldInfo::new(
                    "setting".to_owned(), None, None, Type::TEXT,
                    pgwire::api::results::FieldFormat::Text,
                );
                let description_field = FieldInfo::new(
                    "description".to_owned(), None, None, Type::TEXT,
                    pgwire::api::results::FieldFormat::Text,
                );
                let fields = Arc::new(vec![name_field, setting_field, description_field]);

                // Collect all session parameters from metadata
                let mut rows = Vec::new();
                let session_keys: Vec<(String, String)> = client
                    .metadata()
                    .iter()
                    .filter(|(k, _)| k.starts_with(SESSION_PREFIX))
                    .map(|(k, v)| {
                        (k[SESSION_PREFIX.len()..].to_owned(), v.clone())
                    })
                    .collect();

                for (key, value) in &session_keys {
                    let mut encoder = DataRowEncoder::new(fields.clone());
                    encoder.encode_field(key)?;
                    encoder.encode_field(value)?;
                    encoder.encode_field(&"")?; // description (empty)
                    rows.push(Ok(encoder.take_row()));
                }

                let row_count = rows.len();
                let row_stream = stream::iter(rows);
                let mut response = QueryResponse::new(fields, row_stream);
                response.set_command_tag(&format!("SELECT {}", row_count));
                Ok(vec![Response::Query(response)])
            }
            QueryClass::Begin
            | QueryClass::Commit
            | QueryClass::Rollback
            | QueryClass::Savepoint
            | QueryClass::Release => {
                let tag = match &class {
                    QueryClass::Begin => "BEGIN",
                    QueryClass::Commit => "COMMIT",
                    QueryClass::Rollback => "ROLLBACK",
                    QueryClass::Savepoint => "SAVEPOINT",
                    QueryClass::Release => "RELEASE",
                    _ => unreachable!(),
                };
                Ok(vec![Response::Execution(Tag::new(tag))])
            }
            QueryClass::DiscardAll => {
                // Remove all session: prefixed keys from metadata
                let keys_to_remove: Vec<String> = client
                    .metadata()
                    .keys()
                    .filter(|k| k.starts_with(SESSION_PREFIX))
                    .cloned()
                    .collect();
                for key in keys_to_remove {
                    client.metadata_mut().remove(&key);
                }
                Ok(vec![Response::Execution(Tag::new("DISCARD ALL"))])
            }
            QueryClass::Reset { key } => {
                let meta_key = format!("{}{}", SESSION_PREFIX, key);
                client.metadata_mut().remove(&meta_key);
                Ok(vec![Response::Execution(Tag::new("RESET"))])
            }
            // CatalogQuery and DataQuery should not reach here -- they are
            // handled by the main router in execute_query.
            QueryClass::CatalogQuery | QueryClass::DataQuery => {
                unreachable!("CatalogQuery/DataQuery should not be classified as session commands")
            }
        }
    }

    /// Execute a data query against ClickHouse (warm or federated).
    async fn execute_data_query(
        &self,
        project_id: Uuid,
        sql: &str,
        pg_statements: Vec<sqlparser::ast::Statement>,
    ) -> PgWireResult<Vec<Response>> {
        // Check for cold (federated) sources, with a short TTL cache
        let has_cold: bool = if let Some(cached) = self.get_cached_has_cold(project_id) {
            cached
        } else {
            let result: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM warehouse_sources WHERE project_id = $1 AND tier = 'cold')"
            )
            .bind(project_id)
            .fetch_one(&*self.state.db)
            .await
            .map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    format!("Database error: {}", e),
                )))
            })?;
            self.set_cached_has_cold(project_id, result);
            result
        };

        if has_cold {
            return self.execute_federated(project_id, sql).await;
        }

        // Regular path: load tables, rewrite SQL, execute against ClickHouse
        self.execute_warm_query(project_id, sql, pg_statements).await
    }

    /// Translate, validate, rewrite SQL and acquire a query permit.
    ///
    /// Shared by `execute_warm_query` and `describe_data_query_schema` to
    /// avoid duplicating the dialect translation, table loading, hot/warm
    /// classification, SQL rewriting, and concurrency-limiting logic.
    ///
    /// Returns `None` for the rewritten SQL when the project has no tables
    /// (callers should return an empty result).
    async fn prepare_query(
        &self,
        project_id: Uuid,
        sql: &str,
        pg_statements: Vec<sqlparser::ast::Statement>,
    ) -> PgWireResult<Option<PreparedQuery>> {
        let (tables, hot_tables, hot_backing_tables) = self.load_project_tables(project_id).await?;

        if tables.is_empty() && hot_tables.is_empty() {
            return Ok(None);
        }

        let mut translated = pg_statements;
        super::dialect::translate_statements_to_clickhouse(&mut translated);
        let mut parsed = crate::warehouse::query::ParsedQuery::from_statements(translated);

        let referenced_tables = TableRewriter::extract_tables_from_ast(parsed.statements());

        let all_hot = !referenced_tables.is_empty()
            && referenced_tables.iter().all(|t| hot_tables.contains_key(t));
        let any_hot = referenced_tables.iter().any(|t| hot_tables.contains_key(t));

        if any_hot && !all_hot {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42P10".to_owned(),
                "Cannot mix hot and warm tables in the same query".to_owned(),
            ))));
        }

        let rewritten_sql = if all_hot {
            rewrite_hot_statements(parsed.statements_mut(), &hot_tables)
        } else {
            let rewriter = self.create_rewriter();
            rewriter
                .rewrite_with_partition_pruning_ast(parsed.statements_mut(), &tables)
                .map_err(|e| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "42000".to_owned(),
                        format!("Query rewrite error: {}", e),
                    )))
                })?
        };

        let _query_permit = self
            .state
            .warehouse_query_limiter
            .acquire(project_id)
            .await
            .map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "53300".to_owned(),
                    format!("Too many concurrent queries: {}", e),
                )))
            })?;

        Ok(Some(PreparedQuery {
            rewritten_sql,
            _query_permit,
            hot_backing_tables,
        }))
    }

    /// Execute a query against warm (synced) tables in ClickHouse.
    async fn execute_warm_query(
        &self,
        project_id: Uuid,
        sql: &str,
        pg_statements: Vec<sqlparser::ast::Statement>,
    ) -> PgWireResult<Vec<Response>> {
        let prepared = self.prepare_query(project_id, sql, pg_statements).await?;
        let prepared = match prepared {
            Some(p) => p,
            None => {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42P01".to_owned(),
                    "No warehouse tables configured for this project".to_owned(),
                ))));
            }
        };

        let options = ExecutionOptions {
            limit: Some(PGWIRE_DEFAULT_ROW_LIMIT),
            timeout_secs: Some(PGWIRE_QUERY_TIMEOUT_SECS),
            max_memory_bytes: Some(PGWIRE_MAX_RESULT_MEMORY_BYTES),
        };

        let has_warm_backing = !prepared.hot_backing_tables.is_empty();
        let ch_is_down = self.state.ch_down_cache.get(&())
            .map_or(false, |since| since.elapsed().as_secs() < 60);

        if ch_is_down && has_warm_backing {
            tracing::info!(
                project_id = %project_id,
                "PgWire: ClickHouse circuit breaker open, falling back to DataFusion"
            );
            return self.execute_warm_backing_via_datafusion(
                project_id, sql, &prepared.hot_backing_tables,
            ).await;
        }

        let exec_result = self
            .state
            .warehouse_query_executor
            .execute_native_blocks(&prepared.rewritten_sql, options.clone())
            .await;

        let native_stream = match exec_result {
            Ok(stream) => {
                self.state.ch_down_cache.remove(&());
                stream
            }
            Err(ref e) if e.is_data_error() && has_warm_backing => {
                tracing::warn!(
                    project_id = %project_id,
                    error = %e,
                    "PgWire: ClickHouse data error, retrying with warm backing s3()"
                );
                let warm_sql = crate::api::warehouse::rewrite_for_warm_backing(
                    &self.state, project_id, sql, &prepared.hot_backing_tables,
                ).await.map_err(|e| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(), "XX000".to_owned(),
                        format!("Warm backing rewrite failed: {}", e),
                    )))
                })?;
                self.state.warehouse_query_executor
                    .execute_native_blocks(&warm_sql, options)
                    .await
                    .map_err(|e| {
                        PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(), "42000".to_owned(),
                            format!("Query execution failed (warm backing): {}", e),
                        )))
                    })?
            }
            Err(ref e) if e.is_connection_error() && has_warm_backing => {
                tracing::warn!(
                    project_id = %project_id,
                    error = %e,
                    "PgWire: ClickHouse connection error, falling back to DataFusion"
                );
                self.state.ch_down_cache.insert((), std::time::Instant::now());
                return self.execute_warm_backing_via_datafusion(
                    project_id, sql, &prepared.hot_backing_tables,
                ).await;
            }
            Err(e) => {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42000".to_owned(),
                    format!("Query execution failed: {}", e),
                ))));
            }
        };

        self.native_stream_to_response(native_stream)
    }

    /// Fallback: execute query against warm backing data through DataFusion.
    async fn execute_warm_backing_via_datafusion(
        &self,
        project_id: Uuid,
        sql: &str,
        hot_backing_tables: &ahash::AHashMap<String, R2TablePath>,
    ) -> PgWireResult<Vec<Response>> {
        use crate::warehouse::query::federated_query::{FederatedQueryExecutor, R2SourceConfig};

        let r2 = self.state.r2_storage.as_ref().ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(), "XX000".to_owned(),
                "R2 not configured for warm backing fallback".to_owned(),
            )))
        })?;

        let r2_config = R2SourceConfig {
            endpoint: r2.endpoint().to_string(),
            bucket: r2.bucket().to_string(),
            access_key_id: r2.access_key_id().to_string(),
            secret_access_key: r2.secret_access_key().to_string(),
            region: None,
        };

        let mut federated = FederatedQueryExecutor::new_for_warm_backing(r2_config, project_id);

        for (table_name, r2_path) in hot_backing_tables {
            federated.register_warm_table(table_name, &r2_path.prefix).await.map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(), "XX000".to_owned(),
                    format!("Failed to register warm backing table: {}", e),
                )))
            })?;
        }

        let batches = federated
            .execute_with_limit(sql, PGWIRE_DEFAULT_ROW_LIMIT as usize)
            .await
            .map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(), "42000".to_owned(),
                    format!("Warm backing DataFusion query failed: {}", e),
                )))
            })?;

        super::types::record_batches_to_response(batches)
    }

    /// Execute a federated query against cold sources.
    ///
    /// For v1, this returns an error since federation support via pgwire
    /// requires additional work. It will be enabled in a fast-follow.
    async fn execute_federated(
        &self,
        _project_id: Uuid,
        _sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "0A000".to_owned(),
            "Federated (cold) queries are not yet supported via the Postgres wire protocol. Use the HTTP API instead.".to_owned(),
        ))))
    }

    /// Load project tables from the database, split by tier.
    ///
    /// Delegates to `load_project_tables_with_tier` (shared with the HTTP API)
    /// and caches the result for 60 seconds to avoid per-query Postgres round-trips.
    async fn load_project_tables(
        &self,
        project_id: Uuid,
    ) -> PgWireResult<ProjectTables> {
        if let Some((tables, ts)) = self.project_tables_cache.get(&project_id) {
            if ts.elapsed() < std::time::Duration::from_secs(60) {
                return Ok(tables);
            }
            self.project_tables_cache.remove(&project_id);
        }

        let result = crate::api::warehouse::load_project_tables_with_tier(
            &self.state.db, project_id,
        )
        .await
        .map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("Failed to load project tables: {}", e),
            )))
        })?;

        self.project_tables_cache.insert(project_id, (result.clone(), std::time::Instant::now()));
        Ok(result)
    }

    /// Get the cached SQL rewriter from PondState (created once at startup).
    fn create_rewriter(&self) -> &TableRewriter {
        &self.state.table_rewriter
    }

    /// Derive the output schema for a SQL query without fetching all data.
    ///
    /// Used by `do_describe_statement` and `do_describe_portal` to tell
    /// drivers what columns a query will produce.
    async fn describe_query_schema<C: ClientInfo + Send + Sync>(
        &self,
        client: &C,
        sql: &str,
    ) -> PgWireResult<Vec<FieldInfo>> {
        // Session commands: most don't produce result sets, but SHOW does
        if let Some(class) = classify_session_command(sql) {
            return Ok(match class {
                QueryClass::Show { key } => {
                    vec![FieldInfo::new(
                        key,
                        None,
                        None,
                        Type::TEXT,
                        pgwire::api::results::FieldFormat::Text,
                    )]
                }
                QueryClass::ShowAll => {
                    vec![
                        FieldInfo::new("name".to_owned(), None, None, Type::TEXT, pgwire::api::results::FieldFormat::Text),
                        FieldInfo::new("setting".to_owned(), None, None, Type::TEXT, pgwire::api::results::FieldFormat::Text),
                        FieldInfo::new("description".to_owned(), None, None, Type::TEXT, pgwire::api::results::FieldFormat::Text),
                    ]
                }
                _ => vec![],
            });
        }

        {
            use sqlparser::dialect::PostgreSqlDialect;
            use sqlparser::parser::Parser;

            let pg_dialect = PostgreSqlDialect {};
            let statements = match Parser::parse_sql(&pg_dialect, sql) {
                Ok(stmts) => stmts,
                Err(_) => {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "42601".to_owned(),
                        "Cannot validate unparseable SQL; statement rejected".to_owned(),
                    ))));
                }
            };

            Self::enforce_read_only_ast(&statements, sql)?;

            let project_id = self.get_project_id(client)?;

            if is_catalog_query_ast(&statements, sql) {
                return self.catalog_engine.describe_schema(project_id, sql).await;
            }

            self.describe_data_query_schema(project_id, sql, statements).await
        }
    }

    /// Describe the schema of a data query by executing it with LIMIT 0.
    async fn describe_data_query_schema(
        &self,
        project_id: Uuid,
        sql: &str,
        pg_statements: Vec<sqlparser::ast::Statement>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        let prepared = match self.prepare_query(project_id, sql, pg_statements).await? {
            Some(p) => p,
            None => return Ok(vec![]),
        };

        let options = ExecutionOptions {
            limit: Some(0),
            timeout_secs: Some(PGWIRE_QUERY_TIMEOUT_SECS),
            max_memory_bytes: Some(PGWIRE_MAX_RESULT_MEMORY_BYTES),
        };

        let result = self
            .state
            .warehouse_query_executor
            .execute(&prepared.rewritten_sql, options)
            .await
            .map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42000".to_owned(),
                    format!("Schema inference failed: {}", e),
                )))
            })?;

        let fields: Vec<FieldInfo> = result
            .columns
            .iter()
            .map(column_info_to_field_info)
            .collect();

        Ok(fields)
    }

    /// Convert a `NativeBlockStream` directly into pgwire `Response` messages.
    ///
    /// Encodes klickhouse values straight into pgwire DataRows as blocks
    /// arrive, without buffering or JSON intermediates.
    fn native_stream_to_response(
        &self,
        mut native_stream: NativeBlockStream,
    ) -> PgWireResult<Vec<Response>> {
        let fields: Vec<FieldInfo> = native_stream
            .columns
            .iter()
            .map(native_column_to_field_info)
            .collect();
        let fields = Arc::new(fields);

        let col_names: Vec<String> = native_stream
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect();

        let fields_for_stream = fields.clone();
        let row_stream = async_stream::try_stream! {
            while let Some(block_result) = native_stream.blocks.next().await {
                let block = block_result.map_err(|e| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "42000".to_owned(),
                        format!("Block read error: {}", e),
                    )))
                })?;

                let num_rows = block.rows as usize;
                let col_data: Vec<&Vec<klickhouse::Value>> = col_names
                    .iter()
                    .filter_map(|name| block.column_data.get(name.as_str()))
                    .collect();

                if col_data.len() != col_names.len() {
                    let missing: Vec<_> = col_names.iter()
                        .filter(|name| !block.column_data.contains_key(name.as_str()))
                        .collect();
                    Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "XX000".to_owned(),
                        format!("Missing columns in block data: {:?}", missing),
                    ))))?;
                }

                for (idx, col_values) in col_data.iter().enumerate() {
                    if col_values.len() < num_rows {
                        Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "XX000".to_owned(),
                            format!(
                                "Column '{}' data length {} < expected rows {}",
                                col_names[idx], col_values.len(), num_rows
                            ),
                        ))))?;
                    }
                }

                let mut encoder = DataRowEncoder::new(fields_for_stream.clone());
                for row_idx in 0..num_rows {
                    for col_values in &col_data {
                        let value = &col_values[row_idx];
                        encode_klickhouse_value(&mut encoder, value)?;
                    }
                    yield encoder.take_row();
                }
            }
        };

        let response = QueryResponse::new(fields, row_stream);
        Ok(vec![Response::Query(response)])
    }
}

// =============================================================================
// AST-based hot query rewriting
// =============================================================================
//
// These functions replicate the same AST-walking logic used by the HTTP handler
// in api/warehouse.rs. They properly traverse the SQL parse tree to replace
// table references, avoiding the pitfalls of naive string replacement.

/// Rewrite SQL for hot (native ClickHouse) tables using AST manipulation.
pub(crate) fn rewrite_hot_query(
    sql: &str,
    hot_tables: &ahash::AHashMap<String, String>,
) -> Result<String, String> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| format!("SQL parse error: {}", e))?;

    if statements.is_empty() {
        return Err("Empty SQL statement".to_string());
    }

    Ok(rewrite_hot_statements(&mut statements, hot_tables))
}

/// Rewrite pre-parsed statements for hot tables (zero-parse path).
fn rewrite_hot_statements(
    statements: &mut [sqlparser::ast::Statement],
    hot_tables: &ahash::AHashMap<String, String>,
) -> String {
    for stmt in statements.iter_mut() {
        rewrite_hot_statement(stmt, hot_tables);
    }

    let rewritten: Vec<String> = statements.iter().map(|s| s.to_string()).collect();
    rewritten.join("; ")
}

/// Recursively rewrite table references in a statement for hot tier.
fn rewrite_hot_statement(
    stmt: &mut sqlparser::ast::Statement,
    hot_tables: &ahash::AHashMap<String, String>,
) {
    use sqlparser::ast::Statement;

    if let Statement::Query(query) = stmt {
        rewrite_hot_query_ast(query, hot_tables);
    }
}

fn rewrite_hot_query_ast(
    query: &mut sqlparser::ast::Query,
    hot_tables: &ahash::AHashMap<String, String>,
) {
    rewrite_hot_set_expr(&mut query.body, hot_tables);

    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            rewrite_hot_query_ast(&mut cte.query, hot_tables);
        }
    }

    if let Some(order_by) = &mut query.order_by {
        for item in &mut order_by.exprs {
            rewrite_hot_expr(&mut item.expr, hot_tables);
        }
    }
}

fn rewrite_hot_expr(
    expr: &mut sqlparser::ast::Expr,
    hot_tables: &ahash::AHashMap<String, String>,
) {
    use sqlparser::ast::Expr;

    match expr {
        Expr::Subquery(query) => {
            rewrite_hot_query_ast(query, hot_tables);
        }
        Expr::InSubquery { subquery, expr: inner, .. } => {
            rewrite_hot_query_ast(subquery, hot_tables);
            rewrite_hot_expr(inner, hot_tables);
        }
        Expr::Exists { subquery, .. } => {
            rewrite_hot_query_ast(subquery, hot_tables);
        }
        Expr::BinaryOp { left, right, .. } => {
            rewrite_hot_expr(left, hot_tables);
            rewrite_hot_expr(right, hot_tables);
        }
        Expr::UnaryOp { expr: inner, .. } => {
            rewrite_hot_expr(inner, hot_tables);
        }
        Expr::Nested(inner) => {
            rewrite_hot_expr(inner, hot_tables);
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            rewrite_hot_expr(inner, hot_tables);
        }
        Expr::Between { expr: inner, low, high, .. } => {
            rewrite_hot_expr(inner, hot_tables);
            rewrite_hot_expr(low, hot_tables);
            rewrite_hot_expr(high, hot_tables);
        }
        Expr::InList { expr: inner, list, .. } => {
            rewrite_hot_expr(inner, hot_tables);
            for item in list {
                rewrite_hot_expr(item, hot_tables);
            }
        }
        Expr::Case { operand, conditions, results, else_result, .. } => {
            if let Some(op) = operand { rewrite_hot_expr(op, hot_tables); }
            for c in conditions { rewrite_hot_expr(c, hot_tables); }
            for r in results { rewrite_hot_expr(r, hot_tables); }
            if let Some(el) = else_result { rewrite_hot_expr(el, hot_tables); }
        }
        Expr::Function(func) => {
            if let sqlparser::ast::FunctionArguments::List(ref mut arg_list) = func.args {
                for arg in &mut arg_list.args {
                    match arg {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(ref mut e),
                        ) => rewrite_hot_expr(e, hot_tables),
                        sqlparser::ast::FunctionArg::Named { arg: sqlparser::ast::FunctionArgExpr::Expr(ref mut e), .. } => {
                            rewrite_hot_expr(e, hot_tables);
                        }
                        _ => {}
                    }
                }
            }
            if let Some(ref mut filter) = func.filter {
                rewrite_hot_expr(filter, hot_tables);
            }
        }
        Expr::Cast { expr: inner, .. } => {
            rewrite_hot_expr(inner, hot_tables);
        }
        _ => {}
    }
}

fn rewrite_hot_set_expr(
    set_expr: &mut sqlparser::ast::SetExpr,
    hot_tables: &ahash::AHashMap<String, String>,
) {
    use sqlparser::ast::SetExpr;

    match set_expr {
        SetExpr::Select(select) => {
            for table_with_joins in &mut select.from {
                rewrite_hot_table_factor(&mut table_with_joins.relation, hot_tables);
                for join in &mut table_with_joins.joins {
                    rewrite_hot_table_factor(&mut join.relation, hot_tables);
                }
            }
            if let Some(ref mut selection) = select.selection {
                rewrite_hot_expr(selection, hot_tables);
            }
            if let Some(ref mut having) = select.having {
                rewrite_hot_expr(having, hot_tables);
            }
            for item in &mut select.projection {
                if let sqlparser::ast::SelectItem::UnnamedExpr(ref mut expr)
                | sqlparser::ast::SelectItem::ExprWithAlias { ref mut expr, .. } = item
                {
                    rewrite_hot_expr(expr, hot_tables);
                }
            }
        }
        SetExpr::Query(query) => {
            rewrite_hot_query_ast(query, hot_tables);
        }
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_hot_set_expr(left, hot_tables);
            rewrite_hot_set_expr(right, hot_tables);
        }
        _ => {}
    }
}

fn rewrite_hot_table_factor(
    factor: &mut sqlparser::ast::TableFactor,
    hot_tables: &ahash::AHashMap<String, String>,
) {
    use sqlparser::ast::{Ident, ObjectName, TableFactor};

    match factor {
        TableFactor::Table { name, .. } => {
            // Get the table name in source.table format (matches map keys)
            let table_name = if name.0.len() == 2 {
                format!("{}.{}", name.0[0].value, name.0[1].value)
            } else if name.0.len() == 1 {
                name.0[0].value.clone()
            } else {
                return;
            };

            // Check if this is a hot table
            if let Some(ch_table_name) = hot_tables.get(&table_name) {
                // Replace with native ClickHouse table name: default.`warehouse_...`
                *name = ObjectName(vec![
                    Ident::new("default"),
                    Ident::with_quote('`', ch_table_name),
                ]);
            }
        }
        TableFactor::Derived { subquery, .. } => {
            rewrite_hot_query_ast(subquery, hot_tables);
        }
        TableFactor::NestedJoin { table_with_joins, .. } => {
            rewrite_hot_table_factor(&mut table_with_joins.relation, hot_tables);
            for join in &mut table_with_joins.joins {
                rewrite_hot_table_factor(&mut join.relation, hot_tables);
            }
        }
        _ => {}
    }
}

// =============================================================================
// Trait implementations
// =============================================================================

#[async_trait]
impl SimpleQueryHandler for PondQueryHandler {
    async fn do_query<'a, 'b, 'c, C>(
        &'a self,
        client: &'b mut C,
        query: &'c str,
    ) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        tracing::debug!(query = %query, "PgWire simple query");
        self.execute_query(client, query).await
    }
}

#[async_trait]
impl ExtendedQueryHandler for PondQueryHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_describe_statement<'a, 'b, 'c, C>(
        &'a self,
        client: &'b mut C,
        stmt: &'c StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = &stmt.statement;
        let fields = self.describe_query_schema(client, sql).await?;
        // No parameter types -- we don't parse $N placeholders at describe time
        Ok(DescribeStatementResponse::new(vec![], fields))
    }

    async fn do_describe_portal<'a, 'b, 'c, C>(
        &'a self,
        client: &'b mut C,
        portal: &'c Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = &portal.statement.statement;
        let fields = self.describe_query_schema(client, sql).await?;
        Ok(DescribePortalResponse::new(fields))
    }

    async fn do_query<'a, 'b, 'c, C>(
        &'a self,
        client: &'b mut C,
        portal: &'c Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = &portal.statement.statement;

        // Substitute $1, $2, ... placeholders with bound parameter values
        let sql = if portal.parameters.is_empty() {
            sql.clone()
        } else {
            bind_parameters(sql, &portal.parameters)?
        };

        tracing::debug!(query = %sql, "PgWire extended query");

        let mut results = self.execute_query(client, &sql).await?;
        Ok(results.pop().unwrap_or(Response::EmptyQuery))
    }
}

/// Substitute `$1`, `$2`, ... placeholders in SQL with bound parameter values.
///
/// Uses sqlparser to parse the SQL into an AST, walks every `Expr` node, and
/// replaces `Value::Placeholder("$N")` with the corresponding literal value.
/// This avoids false positives inside string literals, comments, or identifiers
/// that the old `str::replace()` approach was vulnerable to.
pub fn bind_parameters(
    sql: &str,
    parameters: &[Option<bytes::Bytes>],
) -> PgWireResult<String> {
    use sqlparser::ast::{Expr, Statement, Value};
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    // Build a lookup: "$1" -> Value, "$2" -> Value, ...
    let mut param_map: HashMap<String, Value> = HashMap::new();
    for (i, param) in parameters.iter().enumerate() {
        let key = format!("${}", i + 1);
        let value = match param {
            None => Value::Null,
            Some(bytes) => {
                let text = std::str::from_utf8(bytes).map_err(|_| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "22021".to_owned(),
                        format!("Invalid UTF-8 in parameter ${}", i + 1),
                    )))
                })?;
                // Try to preserve numeric/boolean types so ClickHouse can use
                // indexes and avoid implicit string-to-number casts.
                if text == "true" || text == "false" {
                    Value::Boolean(text == "true")
                } else if let Ok(n) = text.parse::<i64>() {
                    let canonical = n.to_string();
                    if canonical == text {
                        Value::Number(canonical, false)
                    } else {
                        Value::SingleQuotedString(text.to_owned())
                    }
                } else if let Ok(f) = text.parse::<f64>() {
                    if f.is_finite() {
                        Value::Number(text.to_owned(), false)
                    } else {
                        Value::SingleQuotedString(text.to_owned())
                    }
                } else {
                    Value::SingleQuotedString(text.to_owned())
                }
            }
        };
        param_map.insert(key, value);
    }

    let dialect = PostgreSqlDialect {};
    let mut statements = match Parser::parse_sql(&dialect, sql) {
        Ok(stmts) => stmts,
        // If we can't parse, fall back to the original SQL unchanged
        Err(_) => return Ok(sql.to_owned()),
    };

    for stmt in &mut statements {
        bind_statement(stmt, &param_map);
    }

    Ok(statements
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join("; "))
}

/// Walk a statement and replace placeholder expressions with literal values.
fn bind_statement(
    stmt: &mut sqlparser::ast::Statement,
    params: &HashMap<String, sqlparser::ast::Value>,
) {
    use sqlparser::ast::Statement;
    match stmt {
        Statement::Query(query) => {
            bind_query(query, params);
        }
        Statement::Explain { statement, .. } => {
            bind_statement(statement, params);
        }
        _ => {}
    }
}

fn bind_query(
    query: &mut sqlparser::ast::Query,
    params: &HashMap<String, sqlparser::ast::Value>,
) {
    bind_set_expr(&mut query.body, params);
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            bind_query(&mut cte.query, params);
        }
    }
    if let Some(order_by) = &mut query.order_by {
        for item in &mut order_by.exprs {
            bind_expr(&mut item.expr, params);
        }
    }
    if let Some(limit) = &mut query.limit {
        bind_expr(limit, params);
    }
    if let Some(offset) = &mut query.offset {
        bind_expr(&mut offset.value, params);
    }
}

fn bind_set_expr(
    set_expr: &mut sqlparser::ast::SetExpr,
    params: &HashMap<String, sqlparser::ast::Value>,
) {
    use sqlparser::ast::SetExpr;
    match set_expr {
        SetExpr::Select(select) => {
            for item in &mut select.projection {
                if let sqlparser::ast::SelectItem::UnnamedExpr(expr)
                | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } = item
                {
                    bind_expr(expr, params);
                }
            }
            for twj in &mut select.from {
                bind_table_factor(&mut twj.relation, params);
                for join in &mut twj.joins {
                    bind_table_factor(&mut join.relation, params);
                    if let Some(cond) = match &mut join.join_operator {
                        sqlparser::ast::JoinOperator::Inner(c)
                        | sqlparser::ast::JoinOperator::LeftOuter(c)
                        | sqlparser::ast::JoinOperator::RightOuter(c)
                        | sqlparser::ast::JoinOperator::FullOuter(c, ..) => {
                            if let sqlparser::ast::JoinConstraint::On(expr) = c {
                                Some(expr)
                            } else {
                                None
                            }
                        }
                        _ => None,
                    } {
                        bind_expr(cond, params);
                    }
                }
            }
            if let Some(sel) = &mut select.selection {
                bind_expr(sel, params);
            }
            if let sqlparser::ast::GroupByExpr::Expressions(exprs, _) = &mut select.group_by {
                for expr in exprs {
                    bind_expr(expr, params);
                }
            }
            if let Some(having) = &mut select.having {
                bind_expr(having, params);
            }
        }
        SetExpr::Query(query) => bind_query(query, params),
        SetExpr::SetOperation { left, right, .. } => {
            bind_set_expr(left, params);
            bind_set_expr(right, params);
        }
        SetExpr::Values(values) => {
            for row in &mut values.rows {
                for expr in row {
                    bind_expr(expr, params);
                }
            }
        }
        _ => {}
    }
}

fn bind_table_factor(
    factor: &mut sqlparser::ast::TableFactor,
    params: &HashMap<String, sqlparser::ast::Value>,
) {
    use sqlparser::ast::TableFactor;
    match factor {
        TableFactor::Derived { subquery, .. } => bind_query(subquery, params),
        TableFactor::NestedJoin { table_with_joins, .. } => {
            bind_table_factor(&mut table_with_joins.relation, params);
            for join in &mut table_with_joins.joins {
                bind_table_factor(&mut join.relation, params);
            }
        }
        _ => {}
    }
}

/// Replace placeholder expressions with literal values from the parameter map.
fn bind_expr(
    expr: &mut sqlparser::ast::Expr,
    params: &HashMap<String, sqlparser::ast::Value>,
) {
    use sqlparser::ast::Expr;

    // Check if this node itself is a placeholder to replace
    if let Expr::Value(sqlparser::ast::Value::Placeholder(name)) = expr {
        if let Some(replacement) = params.get(name.as_str()) {
            *expr = Expr::Value(replacement.clone());
            return;
        }
    }

    // Recurse into child expressions
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            bind_expr(left, params);
            bind_expr(right, params);
        }
        Expr::UnaryOp { expr: inner, .. } | Expr::Nested(inner) => {
            bind_expr(inner, params);
        }
        Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsFalse(inner)
        | Expr::IsNotFalse(inner)
        | Expr::IsTrue(inner)
        | Expr::IsNotTrue(inner) => {
            bind_expr(inner, params);
        }
        Expr::InList { expr: inner, list, .. } => {
            bind_expr(inner, params);
            for item in list {
                bind_expr(item, params);
            }
        }
        Expr::InSubquery { expr: inner, subquery, .. } => {
            bind_expr(inner, params);
            bind_query(subquery, params);
        }
        Expr::Between { expr: inner, low, high, .. } => {
            bind_expr(inner, params);
            bind_expr(low, params);
            bind_expr(high, params);
        }
        Expr::Case { operand, conditions, results, else_result, .. } => {
            if let Some(op) = operand { bind_expr(op, params); }
            for cond in conditions { bind_expr(cond, params); }
            for res in results { bind_expr(res, params); }
            if let Some(el) = else_result { bind_expr(el, params); }
        }
        Expr::Cast { expr: inner, .. } => bind_expr(inner, params),
        Expr::Function(func) => {
            if let sqlparser::ast::FunctionArguments::List(list) = &mut func.args {
                for arg in &mut list.args {
                    match arg {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(e),
                        ) => bind_expr(e, params),
                        sqlparser::ast::FunctionArg::Named {
                            arg: sqlparser::ast::FunctionArgExpr::Expr(e), ..
                        } => bind_expr(e, params),
                        _ => {}
                    }
                }
            }
        }
        Expr::Subquery(q) => bind_query(q, params),
        Expr::Extract { expr: inner, .. } => bind_expr(inner, params),
        Expr::Like { expr: inner, pattern, .. }
        | Expr::ILike { expr: inner, pattern, .. }
        | Expr::SimilarTo { expr: inner, pattern, .. } => {
            bind_expr(inner, params);
            bind_expr(pattern, params);
        }
        Expr::Exists { subquery, .. } => bind_query(subquery, params),
        Expr::Tuple(exprs) => {
            for e in exprs { bind_expr(e, params); }
        }
        Expr::Array(arr) => {
            for e in &mut arr.elem { bind_expr(e, params); }
        }
        Expr::Subscript { expr: inner, subscript } => {
            bind_expr(inner, params);
            if let sqlparser::ast::Subscript::Index { index } = subscript.as_mut() {
                bind_expr(index, params);
            }
        }
        // Leaf nodes or nodes unlikely to contain placeholders
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use crate::warehouse::connectors::enforce_read_only_sql;

    #[test]
    fn test_read_only_allows_select() {
        assert!(enforce_read_only_sql("SELECT 1").is_ok());
        assert!(enforce_read_only_sql("SELECT * FROM orders WHERE id > 10").is_ok());
    }

    #[test]
    fn test_read_only_allows_explain() {
        assert!(enforce_read_only_sql("EXPLAIN SELECT 1").is_ok());
    }

    #[test]
    fn test_read_only_rejects_insert() {
        let result = enforce_read_only_sql("INSERT INTO orders (id) VALUES (1)");
        assert!(result.is_err(), "INSERT should be rejected");
    }

    #[test]
    fn test_read_only_rejects_update() {
        let result = enforce_read_only_sql("UPDATE orders SET total = 0");
        assert!(result.is_err(), "UPDATE should be rejected");
    }

    #[test]
    fn test_read_only_rejects_delete() {
        let result = enforce_read_only_sql("DELETE FROM orders WHERE id = 1");
        assert!(result.is_err(), "DELETE should be rejected");
    }

    #[test]
    fn test_read_only_rejects_drop() {
        let result = enforce_read_only_sql("DROP TABLE orders");
        assert!(result.is_err(), "DROP should be rejected");
    }

    #[test]
    fn test_read_only_rejects_create() {
        let result = enforce_read_only_sql("CREATE TABLE t (id INT)");
        assert!(result.is_err(), "CREATE should be rejected");
    }

    #[test]
    fn test_read_only_rejects_alter() {
        let result = enforce_read_only_sql("ALTER TABLE orders ADD COLUMN foo INT");
        assert!(result.is_err(), "ALTER should be rejected");
    }

    #[test]
    fn test_read_only_rejects_truncate() {
        let result = enforce_read_only_sql("TRUNCATE orders");
        assert!(result.is_err(), "TRUNCATE should be rejected");
    }

    #[test]
    fn test_read_only_rejects_unparseable() {
        // Unparseable SQL must be rejected in read-only mode (fail closed)
        // to prevent ClickHouse-specific DDL from bypassing the guard.
        assert!(enforce_read_only_sql("NOT VALID SQL @#$%").is_err());
    }

    // ── Parameter binding tests ──

    #[test]
    fn test_bind_parameters_basic() {
        let sql = "SELECT * FROM orders WHERE id = $1 AND name = $2";
        let params: Vec<Option<bytes::Bytes>> = vec![
            Some(bytes::Bytes::from("42")),
            Some(bytes::Bytes::from("hello")),
        ];
        let result = super::bind_parameters(sql, &params).unwrap();
        assert!(
            result.contains("42") && result.contains("'hello'"),
            "Expected bound values, got: {}",
            result
        );
        assert!(
            !result.contains("'42'"),
            "Numeric value should be unquoted, got: {}",
            result
        );
    }

    #[test]
    fn test_bind_parameters_null() {
        let sql = "SELECT * FROM orders WHERE id = $1";
        let params: Vec<Option<bytes::Bytes>> = vec![None];
        let result = super::bind_parameters(sql, &params).unwrap();
        assert!(result.contains("NULL"), "Expected NULL, got: {}", result);
    }

    #[test]
    fn test_bind_parameters_escapes_quotes() {
        let sql = "SELECT * FROM orders WHERE name = $1";
        let params: Vec<Option<bytes::Bytes>> = vec![
            Some(bytes::Bytes::from("O'Brien")),
        ];
        let result = super::bind_parameters(sql, &params).unwrap();
        // sqlparser handles quoting -- the value should appear safely quoted
        assert!(
            result.contains("O'Brien") || result.contains("O''Brien"),
            "Expected escaped value, got: {}",
            result
        );
        // Must not break the SQL (no unmatched quotes)
        assert!(!result.contains("O'Brien'"), "Broken quoting in: {}", result);
    }

    #[test]
    fn test_bind_parameters_multiple_digits() {
        // $10 should not be partially matched by $1
        let sql = "SELECT $1, $10";
        let mut params: Vec<Option<bytes::Bytes>> = Vec::new();
        for i in 1..=10 {
            params.push(Some(bytes::Bytes::from(format!("val{}", i))));
        }
        let result = super::bind_parameters(sql, &params).unwrap();
        assert!(result.contains("'val1'"), "Expected val1, got: {}", result);
        assert!(result.contains("'val10'"), "Expected val10, got: {}", result);
    }

    #[test]
    fn test_bind_parameters_ignores_string_literals() {
        // $1 inside a string literal must NOT be replaced
        let sql = "SELECT * FROM orders WHERE note = 'costs $1 each' AND id = $1";
        let params: Vec<Option<bytes::Bytes>> = vec![
            Some(bytes::Bytes::from("42")),
        ];
        let result = super::bind_parameters(sql, &params).unwrap();
        // The string literal should be preserved
        assert!(
            result.contains("costs $1 each"),
            "String literal was corrupted: {}",
            result
        );
        // The actual placeholder should be replaced (numeric values are unquoted)
        assert!(
            !result.contains("'42'") && result.contains("42"),
            "Numeric placeholder should be unquoted: {}",
            result
        );
    }

    // ── enforce_read_only edge cases ──

    #[test]
    fn test_read_only_rejects_mixed_statements() {
        // A SELECT followed by DROP should still be rejected
        let result = enforce_read_only_sql("SELECT 1; DROP TABLE orders");
        assert!(result.is_err(), "Mixed SELECT + DROP should be rejected");
    }

    #[test]
    fn test_read_only_rejects_copy() {
        let result = enforce_read_only_sql("COPY orders TO '/tmp/out.csv'");
        assert!(result.is_err(), "COPY should be rejected");
    }

    #[test]
    fn test_read_only_rejects_grant() {
        let result = enforce_read_only_sql("GRANT SELECT ON orders TO public");
        assert!(result.is_err(), "GRANT should be rejected");
    }

    #[test]
    fn test_read_only_rejects_revoke() {
        let result = enforce_read_only_sql("REVOKE SELECT ON orders FROM public");
        assert!(result.is_err(), "REVOKE should be rejected");
    }

    #[test]
    fn test_read_only_allows_cte_select() {
        let sql = "WITH cte AS (SELECT * FROM orders) SELECT * FROM cte";
        assert!(enforce_read_only_sql(sql).is_ok(), "CTE SELECT should be allowed");
    }

    #[test]
    fn test_read_only_allows_union() {
        let sql = "SELECT 1 UNION ALL SELECT 2 INTERSECT SELECT 3";
        assert!(enforce_read_only_sql(sql).is_ok(), "UNION/INTERSECT should be allowed");
    }

    // ── bind_parameters edge cases ──

    #[test]
    fn test_bind_parameters_empty_params() {
        let sql = "SELECT 1";
        let params: Vec<Option<bytes::Bytes>> = vec![];
        let result = super::bind_parameters(sql, &params).unwrap();
        assert!(result.contains('1'), "SQL should be unchanged, got: {}", result);
    }

    #[test]
    fn test_bind_parameters_order_by() {
        let sql = "SELECT * FROM t ORDER BY $1";
        let params: Vec<Option<bytes::Bytes>> = vec![
            Some(bytes::Bytes::from("1")),
        ];
        let result = super::bind_parameters(sql, &params).unwrap();
        assert!(
            !result.contains("$1"),
            "Placeholder in ORDER BY should be replaced, got: {}",
            result
        );
    }

    #[test]
    fn test_bind_parameters_limit_offset() {
        let sql = "SELECT * FROM t LIMIT $1 OFFSET $2";
        let params: Vec<Option<bytes::Bytes>> = vec![
            Some(bytes::Bytes::from("10")),
            Some(bytes::Bytes::from("20")),
        ];
        let result = super::bind_parameters(sql, &params).unwrap();
        assert!(
            result.contains("LIMIT 10") && result.contains("OFFSET 20"),
            "LIMIT/OFFSET placeholders should be replaced with unquoted numerics, got: {}",
            result
        );
    }

    #[test]
    fn test_bind_parameters_having() {
        let sql = "SELECT a, count(*) FROM t GROUP BY a HAVING count(*) > $1";
        let params: Vec<Option<bytes::Bytes>> = vec![
            Some(bytes::Bytes::from("5")),
        ];
        let result = super::bind_parameters(sql, &params).unwrap();
        assert!(
            !result.contains("$1") && result.contains("> 5"),
            "Placeholder in HAVING should be replaced with unquoted numeric, got: {}",
            result
        );
    }

    #[test]
    fn test_bind_parameters_between() {
        let sql = "SELECT * FROM t WHERE id BETWEEN $1 AND $2";
        let params: Vec<Option<bytes::Bytes>> = vec![
            Some(bytes::Bytes::from("10")),
            Some(bytes::Bytes::from("20")),
        ];
        let result = super::bind_parameters(sql, &params).unwrap();
        assert!(
            result.contains("BETWEEN 10 AND 20"),
            "BETWEEN placeholders should be replaced with unquoted numerics, got: {}",
            result
        );
    }

    #[test]
    fn test_bind_parameters_in_list() {
        let sql = "SELECT * FROM t WHERE id IN ($1, $2, $3)";
        let params: Vec<Option<bytes::Bytes>> = vec![
            Some(bytes::Bytes::from("1")),
            Some(bytes::Bytes::from("2")),
            Some(bytes::Bytes::from("3")),
        ];
        let result = super::bind_parameters(sql, &params).unwrap();
        assert!(
            !result.contains("$1") && !result.contains("$2") && !result.contains("$3"),
            "IN list placeholders should be replaced with unquoted numerics, got: {}",
            result
        );
    }

    #[test]
    fn test_bind_parameters_function_arg() {
        let sql = "SELECT date_trunc('day', $1)";
        let params: Vec<Option<bytes::Bytes>> = vec![
            Some(bytes::Bytes::from("2024-01-15")),
        ];
        let result = super::bind_parameters(sql, &params).unwrap();
        assert!(
            result.contains("'2024-01-15'"),
            "Placeholder in function arg should be replaced, got: {}",
            result
        );
    }

    #[test]
    fn test_bind_parameters_case_when() {
        let sql = "SELECT CASE WHEN x > $1 THEN 'a' ELSE 'b' END FROM t";
        let params: Vec<Option<bytes::Bytes>> = vec![
            Some(bytes::Bytes::from("100")),
        ];
        let result = super::bind_parameters(sql, &params).unwrap();
        assert!(
            !result.contains("$1") && result.contains("> 100"),
            "Placeholder in CASE WHEN should be replaced with unquoted numeric, got: {}",
            result
        );
    }

    #[test]
    fn test_bind_parameters_join_on() {
        let sql = "SELECT * FROM a JOIN b ON a.id = $1";
        let params: Vec<Option<bytes::Bytes>> = vec![
            Some(bytes::Bytes::from("42")),
        ];
        let result = super::bind_parameters(sql, &params).unwrap();
        assert!(
            !result.contains("$1") && result.contains("= 42"),
            "Placeholder in JOIN ON should be replaced with unquoted numeric, got: {}",
            result
        );
    }

    #[test]
    fn test_bind_parameters_unmatched_placeholder() {
        // $1 in SQL but no parameters -- parser keeps the placeholder as-is
        let sql = "SELECT * FROM t WHERE id = $1";
        let params: Vec<Option<bytes::Bytes>> = vec![];
        let result = super::bind_parameters(sql, &params).unwrap();
        assert!(
            result.contains("$1"),
            "Unmatched placeholder should remain, got: {}",
            result
        );
    }

    #[test]
    fn test_bind_parameters_preserves_leading_zeros() {
        let sql = "SELECT * FROM t WHERE zip = $1";
        let params: Vec<Option<bytes::Bytes>> = vec![
            Some(bytes::Bytes::from("007")),
        ];
        let result = super::bind_parameters(sql, &params).unwrap();
        assert!(
            result.contains("'007'"),
            "Leading zeros must be preserved as a quoted string, got: {}",
            result
        );
    }

    #[test]
    fn test_bind_parameters_preserves_leading_plus() {
        let sql = "SELECT * FROM t WHERE code = $1";
        let params: Vec<Option<bytes::Bytes>> = vec![
            Some(bytes::Bytes::from("+42")),
        ];
        let result = super::bind_parameters(sql, &params).unwrap();
        assert!(
            result.contains("'+42'"),
            "Leading plus must be preserved as a quoted string, got: {}",
            result
        );
    }

    #[test]
    fn test_bind_parameters_plain_integer_unquoted() {
        let sql = "SELECT * FROM t WHERE id = $1";
        let params: Vec<Option<bytes::Bytes>> = vec![
            Some(bytes::Bytes::from("42")),
        ];
        let result = super::bind_parameters(sql, &params).unwrap();
        assert!(
            !result.contains("'42'") && result.contains("42"),
            "Plain integer must remain unquoted numeric, got: {}",
            result
        );
    }

    // ── rewrite_hot_query tests ──

    #[test]
    fn test_rewrite_hot_single_table() {
        let mut map = ahash::AHashMap::new();
        map.insert("src.orders".to_owned(), "warehouse_abc".to_owned());
        let result = super::rewrite_hot_query("SELECT * FROM src.orders", &map).unwrap();
        assert!(
            result.contains("warehouse_abc"),
            "Table should be rewritten, got: {}",
            result
        );
    }

    #[test]
    fn test_rewrite_hot_join() {
        let mut map = ahash::AHashMap::new();
        map.insert("src.orders".to_owned(), "wh_orders".to_owned());
        map.insert("src.users".to_owned(), "wh_users".to_owned());
        let result = super::rewrite_hot_query(
            "SELECT * FROM src.orders o JOIN src.users u ON o.user_id = u.id",
            &map,
        )
        .unwrap();
        assert!(
            result.contains("wh_orders") && result.contains("wh_users"),
            "Both tables should be rewritten, got: {}",
            result
        );
    }

    #[test]
    fn test_rewrite_hot_subquery() {
        let mut map = ahash::AHashMap::new();
        map.insert("src.orders".to_owned(), "wh_orders".to_owned());
        let result = super::rewrite_hot_query(
            "SELECT * FROM (SELECT id FROM src.orders) AS sub",
            &map,
        )
        .unwrap();
        assert!(
            result.contains("wh_orders"),
            "Subquery table should be rewritten, got: {}",
            result
        );
    }

    #[test]
    fn test_rewrite_hot_unmapped_table() {
        let map = ahash::AHashMap::new(); // empty map
        let result = super::rewrite_hot_query("SELECT * FROM orders", &map).unwrap();
        assert!(
            result.contains("orders"),
            "Unmapped table should be left unchanged, got: {}",
            result
        );
    }

    #[test]
    fn test_rewrite_hot_empty_sql() {
        let map = ahash::AHashMap::new();
        let result = super::rewrite_hot_query("", &map);
        assert!(result.is_err(), "Empty SQL should return Err");
    }

    // ── bind_parameters deep recursion ──

    #[test]
    fn test_bind_parameters_inside_cast() {
        let sql = "SELECT CAST($1 AS int)";
        let params: Vec<Option<bytes::Bytes>> = vec![
            Some(bytes::Bytes::from("42")),
        ];
        let result = super::bind_parameters(sql, &params).unwrap();
        assert!(
            !result.contains("$1") && result.contains("42"),
            "Placeholder inside CAST should be replaced with unquoted numeric, got: {}",
            result
        );
    }

    #[test]
    fn test_bind_parameters_nested_parens() {
        let sql = "SELECT * FROM t WHERE (($1 > 0))";
        let params: Vec<Option<bytes::Bytes>> = vec![
            Some(bytes::Bytes::from("5")),
        ];
        let result = super::bind_parameters(sql, &params).unwrap();
        assert!(
            !result.contains("$1") && result.contains("5"),
            "Placeholder in nested parens should be replaced with unquoted numeric, got: {}",
            result
        );
    }

    #[test]
    fn test_bind_parameters_is_null() {
        let sql = "SELECT $1 IS NULL";
        let params: Vec<Option<bytes::Bytes>> = vec![
            Some(bytes::Bytes::from("test")),
        ];
        let result = super::bind_parameters(sql, &params).unwrap();
        assert!(
            result.contains("'test'"),
            "Placeholder in IS NULL should be replaced, got: {}",
            result
        );
    }

    #[test]
    fn test_bind_parameters_like_pattern() {
        let sql = "SELECT * FROM t WHERE name LIKE $1";
        let params: Vec<Option<bytes::Bytes>> = vec![
            Some(bytes::Bytes::from("%foo%")),
        ];
        let result = super::bind_parameters(sql, &params).unwrap();
        assert!(
            result.contains("'%foo%'"),
            "Placeholder in LIKE pattern should be replaced, got: {}",
            result
        );
    }

    #[test]
    fn test_bind_parameters_cte() {
        let sql = "WITH cte AS (SELECT $1 AS x) SELECT * FROM cte";
        let params: Vec<Option<bytes::Bytes>> = vec![
            Some(bytes::Bytes::from("hello")),
        ];
        let result = super::bind_parameters(sql, &params).unwrap();
        assert!(
            result.contains("'hello'"),
            "Placeholder inside CTE should be replaced, got: {}",
            result
        );
    }

    #[test]
    fn test_bind_parameters_multiple_statements() {
        let sql = "SELECT $1; SELECT $2";
        let params: Vec<Option<bytes::Bytes>> = vec![
            Some(bytes::Bytes::from("a")),
            Some(bytes::Bytes::from("b")),
        ];
        let result = super::bind_parameters(sql, &params).unwrap();
        assert!(
            result.contains("'a'") && result.contains("'b'"),
            "Both statements should have placeholders replaced, got: {}",
            result
        );
    }

    #[test]
    fn test_bind_parameters_unparseable() {
        let sql = "NOT VALID $1 SQL";
        let params: Vec<Option<bytes::Bytes>> = vec![
            Some(bytes::Bytes::from("42")),
        ];
        let result = super::bind_parameters(sql, &params).unwrap();
        assert_eq!(
            result, sql,
            "Unparseable SQL should be returned unchanged"
        );
    }

    // ── enforce_read_only more statement types ──

    #[test]
    fn test_read_only_rejects_merge() {
        let result = enforce_read_only_sql(
            "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET x = 1"
        );
        assert!(result.is_err(), "MERGE should be rejected");
    }

    #[test]
    fn test_read_only_rejects_vacuum() {
        // VACUUM produces an empty statement list from sqlparser, which must
        // be rejected (fail-closed) to prevent unvalidated SQL from executing.
        assert!(
            enforce_read_only_sql("VACUUM orders").is_err(),
            "VACUUM should be rejected by the read-only guard"
        );
    }

    #[test]
    fn test_read_only_rejects_analyze() {
        // ANALYZE produces an empty statement list from sqlparser, which must
        // be rejected (fail-closed) to prevent unvalidated SQL from executing.
        assert!(
            enforce_read_only_sql("ANALYZE orders").is_err(),
            "ANALYZE should be rejected by the read-only guard"
        );
    }

    #[test]
    fn test_read_only_rejects_comment_on() {
        let result = enforce_read_only_sql("COMMENT ON TABLE orders IS 'test'");
        assert!(result.is_err(), "COMMENT ON should be rejected");
    }

    #[test]
    fn test_read_only_rejects_call() {
        let result = enforce_read_only_sql("CALL my_procedure()");
        assert!(result.is_err(), "CALL should be rejected");
    }

    #[test]
    fn test_read_only_rejects_empty_string() {
        // Empty SQL should be rejected
        assert!(enforce_read_only_sql("").is_err());
    }

    #[test]
    fn test_read_only_allows_multiple_selects() {
        assert!(
            enforce_read_only_sql("SELECT 1; SELECT 2").is_ok(),
            "Multiple SELECTs should be allowed"
        );
    }

    // ── rewrite_hot_query deeper paths ──

    #[test]
    fn test_rewrite_hot_cte() {
        let mut map = ahash::AHashMap::new();
        map.insert("src.orders".to_owned(), "wh_orders".to_owned());
        let result = super::rewrite_hot_query(
            "WITH cte AS (SELECT * FROM src.orders) SELECT * FROM cte",
            &map,
        )
        .unwrap();
        assert!(
            result.contains("wh_orders"),
            "CTE inner table should be rewritten, got: {}",
            result
        );
    }

    #[test]
    fn test_rewrite_hot_union() {
        let mut map = ahash::AHashMap::new();
        map.insert("src.orders".to_owned(), "wh_orders".to_owned());
        map.insert("src.users".to_owned(), "wh_users".to_owned());
        let result = super::rewrite_hot_query(
            "SELECT * FROM src.orders UNION ALL SELECT * FROM src.users",
            &map,
        )
        .unwrap();
        assert!(
            result.contains("wh_orders") && result.contains("wh_users"),
            "Both sides of UNION should be rewritten, got: {}",
            result
        );
    }

    #[test]
    fn test_bind_parameters_explain_substitutes_placeholders() {
        let sql = "EXPLAIN SELECT * FROM t WHERE id = $1";
        let params: Vec<Option<bytes::Bytes>> = vec![Some(bytes::Bytes::from("42"))];
        let result = super::bind_parameters(sql, &params).unwrap();
        assert!(
            result.contains("42"),
            "EXPLAIN inner query should have $1 replaced with 42, got: {}",
            result
        );
        assert!(
            !result.contains("$1"),
            "Placeholder $1 should not remain after binding, got: {}",
            result
        );
    }

    #[test]
    fn test_bind_parameters_nan_treated_as_string() {
        let sql = "SELECT * FROM t WHERE name = $1";
        let params: Vec<Option<bytes::Bytes>> = vec![Some(bytes::Bytes::from("nan"))];
        let result = super::bind_parameters(sql, &params).unwrap();
        assert!(
            result.contains("'nan'"),
            "\"nan\" must be bound as a quoted string, not a bare literal, got: {}",
            result
        );
    }

    #[test]
    fn test_bind_parameters_infinity_treated_as_string() {
        let sql = "SELECT * FROM t WHERE name = $1";
        let params: Vec<Option<bytes::Bytes>> = vec![Some(bytes::Bytes::from("infinity"))];
        let result = super::bind_parameters(sql, &params).unwrap();
        assert!(
            result.contains("'infinity'"),
            "\"infinity\" must be bound as a quoted string, got: {}",
            result
        );
    }

    #[test]
    fn test_bind_parameters_neg_infinity_treated_as_string() {
        let sql = "SELECT * FROM t WHERE name = $1";
        let params: Vec<Option<bytes::Bytes>> = vec![Some(bytes::Bytes::from("-inf"))];
        let result = super::bind_parameters(sql, &params).unwrap();
        assert!(
            result.contains("'-inf'"),
            "\"-inf\" must be bound as a quoted string, got: {}",
            result
        );
    }

    #[test]
    fn test_bind_parameters_normal_float_still_numeric() {
        let sql = "SELECT * FROM t WHERE price = $1";
        let params: Vec<Option<bytes::Bytes>> = vec![Some(bytes::Bytes::from("3.14"))];
        let result = super::bind_parameters(sql, &params).unwrap();
        assert!(
            result.contains("3.14") && !result.contains("'3.14'"),
            "Normal float should be bound as a numeric literal, got: {}",
            result
        );
    }

    #[test]
    fn test_column_length_validation_catches_mismatch() {
        let num_rows: usize = 5;
        let col_names = vec!["a".to_string(), "b".to_string()];
        let col_a = vec![klickhouse::Value::UInt8(1); 5];
        let col_b = vec![klickhouse::Value::UInt8(2); 3]; // too short

        let col_data: Vec<&Vec<klickhouse::Value>> = vec![&col_a, &col_b];

        let mut bad_col: Option<(usize, usize)> = None;
        for (idx, col_values) in col_data.iter().enumerate() {
            if col_values.len() < num_rows {
                bad_col = Some((idx, col_values.len()));
                break;
            }
        }

        let (idx, len) = bad_col.expect("should detect short column");
        assert_eq!(idx, 1);
        assert_eq!(len, 3);
        assert_eq!(col_names[idx], "b");
    }

    #[test]
    fn test_column_length_validation_passes_for_consistent_data() {
        let num_rows: usize = 3;
        let col_a = vec![klickhouse::Value::UInt8(1); 3];
        let col_b = vec![klickhouse::Value::UInt8(2); 3];

        let col_data: Vec<&Vec<klickhouse::Value>> = vec![&col_a, &col_b];

        for col_values in &col_data {
            assert!(col_values.len() >= num_rows);
        }
    }

    #[test]
    fn test_hot_table_rewriter_handles_subqueries_in_where() {
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;

        let sql = "SELECT * FROM orders WHERE user_id IN (SELECT id FROM users)";
        let dialect = PostgreSqlDialect {};
        let mut stmts = Parser::parse_sql(&dialect, sql).unwrap();
        assert_eq!(stmts.len(), 1);

        let mut hot_tables = ahash::AHashMap::new();
        hot_tables.insert("orders".to_string(), "warehouse_proj_orders".to_string());
        hot_tables.insert("users".to_string(), "warehouse_proj_users".to_string());

        if let sqlparser::ast::Statement::Query(ref mut query) = stmts[0] {
            super::rewrite_hot_query_ast(query, &hot_tables);
        }

        let result = stmts[0].to_string();
        // Both tables should be rewritten, including the one in the subquery
        assert!(
            result.contains("warehouse_proj_users"),
            "subquery table must be rewritten, got: {}",
            result
        );
        assert!(
            result.contains("warehouse_proj_orders"),
            "FROM table must be rewritten, got: {}",
            result
        );
    }

    #[test]
    fn test_hot_table_rewriter_handles_order_by_subquery() {
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;

        let sql = "SELECT * FROM orders ORDER BY (SELECT max(id) FROM orders)";
        let dialect = PostgreSqlDialect {};
        let mut stmts = Parser::parse_sql(&dialect, sql).unwrap();
        assert_eq!(stmts.len(), 1);

        let mut hot_tables = ahash::AHashMap::new();
        hot_tables.insert("orders".to_string(), "warehouse_proj_orders".to_string());

        if let sqlparser::ast::Statement::Query(ref mut query) = stmts[0] {
            super::rewrite_hot_query_ast(query, &hot_tables);
        }

        let result = stmts[0].to_string();
        assert!(
            !result.contains(" orders"),
            "ORDER BY subquery table must be rewritten, got: {}",
            result
        );
        // The table in the ORDER BY subquery should now reference the hot table
        let count = result.matches("warehouse_proj_orders").count();
        assert_eq!(
            count, 2,
            "Both FROM and ORDER BY subquery should reference hot table, got: {}",
            result
        );
    }
}
