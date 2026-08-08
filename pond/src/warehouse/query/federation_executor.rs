//! Federation Executor
//!
//! Executes federated query plans, including external API materialization.
//!
//! This module handles the execution of `FederatedPlan` with different
//! combination strategies, particularly `ExternalApiMaterialize` for
//! cold tier sources like Google Sheets.
//!
//! # Execution Flow for ExternalApiMaterialize
//!
//! 1. Fetch data from external connectors (with TTL caching)
//! 2. Convert Arrow RecordBatches to ClickHouse-compatible format
//! 3. Create temporary tables in ClickHouse
//! 4. Insert data via Arrow format
//! 5. Execute the final SQL query
//! 6. Drop temporary tables (cleanup)

use ahash::{AHashMap, AHashSet};
use std::sync::Arc;

use arrow::array::{self as arrow_array, RecordBatch};
use arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema, TimeUnit};
use futures::StreamExt;
use klickhouse::block::{Block, BlockInfo};
use klickhouse::{Date as KlickhouseDate, DynDateTime64, IndexMap, Type as KlickhouseType, Value as KlickhouseValue};
use thiserror::Error;
use tracing::{debug, info, warn};

use super::executor::{ClickHouseConfig, ExecutionOptions, ExecutorError, QueryExecutor, QueryResult};
use super::federation::{CombinationStrategy, ExternalSourceInfo, FederatedPlan, MongoDBSourceInfo, SourceQuery};
use super::predicate_pushdown::{expr_to_predicates, Predicate, PredicateSplitter, SourcePredicateAnalysis};
use crate::warehouse::connectors::{Connector, ConnectorError, FetchOptions};
use crate::warehouse::types::SourceType;

// ============================================================================
// Errors
// ============================================================================

/// Errors that can occur during federation execution.
#[derive(Debug, Error)]
pub enum FederationExecutorError {
    #[error("Executor error: {0}")]
    Executor(#[from] ExecutorError),

    #[error("Connector error: {0}")]
    Connector(#[from] ConnectorError),

    #[error("Connector not found: {source_type:?} for {source_name}")]
    ConnectorNotFound {
        source_type: SourceType,
        source_name: String,
    },

    #[error("Failed to create temp table {table}: {message}")]
    TempTableCreation { table: String, message: String },

    #[error("Failed to insert data into {table}: {message}")]
    DataInsertion { table: String, message: String },

    #[error("Unsupported combination strategy: {0}")]
    UnsupportedStrategy(String),

    #[error("Arrow conversion error: {0}")]
    ArrowConversion(String),

    #[error("SQL rewrite error: {0}")]
    SqlRewrite(String),
}

/// Result type for federation execution.
pub type FederationExecutorResult<T> = Result<T, FederationExecutorError>;

// ============================================================================
// Federation Executor
// ============================================================================

/// Executes federated query plans.
///
/// Handles materialization of external data sources into temporary ClickHouse
/// tables, then executes the final query against the combined data.
pub struct FederationExecutor {
    /// ClickHouse query executor
    query_executor: QueryExecutor,
    /// Registry of connectors for external sources
    connector_registry: Arc<ConnectorRegistry>,
}

impl FederationExecutor {
    /// Create a new federation executor.
    pub async fn new(
        config: ClickHouseConfig,
        connector_registry: Arc<ConnectorRegistry>,
    ) -> FederationExecutorResult<Self> {
        let query_executor = QueryExecutor::with_config(config).await?;
        Ok(Self {
            query_executor,
            connector_registry,
        })
    }

    /// Execute a federated plan.
    ///
    /// Handles different combination strategies:
    /// - `None`: Single source, execute directly
    /// - `DirectMerge`: All sources accessible from ClickHouse
    /// - `ExternalApiMaterialize`: Fetch from external APIs, materialize, then query
    #[tracing::instrument(
        name = "warehouse.federation.execute",
        skip(self, plan, options),
        fields(source_count = plan.source_queries.len()),
        err(Display),
    )]
    pub async fn execute(
        &self,
        plan: &FederatedPlan,
        options: ExecutionOptions,
    ) -> FederationExecutorResult<QueryResult> {
        match &plan.combination {
            CombinationStrategy::None => {
                // Single source - execute directly
                if let Some(sq) = plan.source_queries.first() {
                    self.query_executor
                        .execute(&sq.sql, options)
                        .await
                        .map_err(FederationExecutorError::from)
                } else {
                    Err(FederationExecutorError::UnsupportedStrategy(
                        "Empty source queries".to_string(),
                    ))
                }
            }

            CombinationStrategy::DirectMerge { combined_sql } => {
                // All sources accessible from ClickHouse
                self.query_executor
                    .execute(combined_sql, options)
                    .await
                    .map_err(FederationExecutorError::from)
            }

            CombinationStrategy::ExternalApiMaterialize {
                external_sources,
                temp_tables,
                final_sql,
            } => {
                self.execute_external_api_materialize(
                    external_sources,
                    temp_tables,
                    final_sql,
                    options,
                )
                .await
            }

            CombinationStrategy::MaterializeJoin { temp_tables, join_sql } => {
                if temp_tables.is_empty() {
                    // No temp tables to create — execute join SQL directly.
                    self.query_executor
                        .execute(join_sql, options)
                        .await
                        .map_err(FederationExecutorError::from)
                } else {
                    // Materialise each source's data into ClickHouse temp
                    // tables, then execute the join SQL that references them.
                    let mut created: Vec<String> = Vec::with_capacity(temp_tables.len());
                    let result = self
                        .materialise_and_execute_join(
                            &plan.source_queries,
                            temp_tables,
                            join_sql,
                            options,
                            &mut created,
                        )
                        .await;
                    for t in &created {
                        if let Err(e) = self.execute_ddl(&format!("DROP TABLE IF EXISTS `{}`", t)).await {
                            warn!(table = %t, error = %e, "Failed to drop temp table");
                        }
                    }
                    result
                }
            }

            CombinationStrategy::MongoDBIndexed {
                source_info,
                index_filter,
                temp_table,
                final_sql,
            } => {
                self.execute_mongodb_indexed(
                    source_info,
                    index_filter.as_deref(),
                    temp_table,
                    final_sql,
                    options,
                )
                .await
            }

            strategy => Err(FederationExecutorError::UnsupportedStrategy(format!(
                "{:?}",
                strategy
            ))),
        }
    }

    /// Execute MongoDB index-accelerated materialization strategy.
    ///
    /// Uses the ClickHouse index to find matching document IDs, then fetches
    /// only those documents from MongoDB.
    #[tracing::instrument(name = "warehouse.federation.execute_mongodb_indexed", skip_all, err(Display))]
    async fn execute_mongodb_indexed(
        &self,
        source_info: &MongoDBSourceInfo,
        index_filter: Option<&str>,
        temp_table: &str,
        final_sql: &str,
        options: ExecutionOptions,
    ) -> FederationExecutorResult<QueryResult> {
        info!(
            source = %source_info.source_name,
            collection = %source_info.collection,
            index_available = source_info.index_available,
            "Starting MongoDB index-accelerated execution"
        );

        let mut created_tables: Vec<String> = Vec::with_capacity(1);

        // Execute with cleanup on any error
        let result = async {
            // Get the MongoDB connector
            let connector = self
                .connector_registry
                .get(&source_info.source_name)
                .ok_or_else(|| FederationExecutorError::ConnectorNotFound {
                    source_type: SourceType::MongoDB,
                    source_name: source_info.source_name.clone(),
                })?;

            if source_info.index_available && index_filter.is_some() {
                debug!(
                    collection = %source_info.collection,
                    filter = ?index_filter,
                    "Index-accelerated fetch not yet implemented, falling back to full fetch"
                );
            }

            let batches = {
                debug!(
                    collection = %source_info.collection,
                    "Using direct MongoDB fetch"
                );
                connector
                    .fetch_table(&source_info.collection, None, None)
                    .await
                    .map_err(FederationExecutorError::from)?
            };

            if batches.is_empty() {
                debug!(
                    source = %source_info.source_name,
                    collection = %source_info.collection,
                    "MongoDB source returned no data"
                );
            } else {
                // Create temp table and insert data
                self.create_and_populate_temp_table(temp_table, &batches)
                    .await?;
                created_tables.push(temp_table.to_string());

                info!(
                    collection = %source_info.collection,
                    batch_count = batches.len(),
                    total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                    "Materialized MongoDB data"
                );
            }

            // Execute the final query
            self.query_executor
                .execute(final_sql, options)
                .await
                .map_err(FederationExecutorError::from)
        }
        .await;

        // Always cleanup temp tables
        self.cleanup_temp_tables(&created_tables).await;

        result
    }

    /// Execute external API materialization strategy.
    ///
    /// This is the core execution path for cold tier sources like Google Sheets:
    /// 1. Fetch data from each external source (connector handles caching)
    /// 2. Create ClickHouse temp tables for each source
    /// 3. Insert Arrow data into temp tables
    /// 4. Execute the final query
    /// 5. Drop temp tables (always, even on error)
    #[tracing::instrument(name = "warehouse.federation.execute_external_api_materialize", skip_all, err(Display))]
    async fn execute_external_api_materialize(
        &self,
        external_sources: &[ExternalSourceInfo],
        _temp_tables: &[String],
        final_sql: &str,
        options: ExecutionOptions,
    ) -> FederationExecutorResult<QueryResult> {
        info!(
            external_source_count = external_sources.len(),
            "Starting external API materialization"
        );

        // Track created temp tables for cleanup
        let mut created_tables: Vec<String> = Vec::with_capacity(external_sources.len());

        // Execute with cleanup on any error
        let result = self
            .materialize_and_query(external_sources, final_sql, options, &mut created_tables)
            .await;

        // Always cleanup temp tables
        self.cleanup_temp_tables(&created_tables).await;

        result
    }

    /// Core materialization and query execution.
    #[tracing::instrument(name = "warehouse.federation.materialize_and_query", skip_all, err(Display))]
    async fn materialize_and_query(
        &self,
        external_sources: &[ExternalSourceInfo],
        final_sql: &str,
        options: ExecutionOptions,
        created_tables: &mut Vec<String>,
    ) -> FederationExecutorResult<QueryResult> {
        // Extract WHERE predicates from the SQL once, keyed by table name.
        let table_predicates = Self::extract_predicates_for_tables(final_sql, external_sources);

        // Build all fetch tasks up front, then execute concurrently.
        struct FetchTask<'a> {
            source_info: &'a ExternalSourceInfo,
            table: &'a str,
            table_idx: usize,
            pushable_predicates: Vec<Arc<Predicate>>,
            local_predicates: Vec<Predicate>,
        }

        let mut tasks: Vec<FetchTask<'_>> = Vec::with_capacity(external_sources.len());
        for source_info in external_sources {
            for (table_idx, table) in source_info.tables.iter().enumerate() {
                let raw_predicates = table_predicates
                    .get(table.as_str())
                    .cloned()
                    .unwrap_or_default();

                let mut splitter = PredicateSplitter::new();
                splitter.register_source_type(
                    &source_info.source_name,
                    source_info.source_type,
                );
                let analysis = splitter.analyze_for_source(
                    &raw_predicates,
                    &source_info.source_name,
                    table,
                );

                if analysis.has_pushable() {
                    debug!(
                        source = %source_info.source_name,
                        table = %table,
                        pushable = analysis.pushable.len(),
                        local = analysis.local_only.len(),
                        "Predicate pushdown split"
                    );
                }

                let SourcePredicateAnalysis { pushable, local_only, .. } = analysis;
                let pushable_predicates: Vec<Arc<Predicate>> = pushable
                    .into_iter()
                    .map(|tp| tp.original)
                    .collect();

                tasks.push(FetchTask {
                    source_info,
                    table,
                    table_idx,
                    pushable_predicates,
                    local_predicates: local_only,
                });
            }
        }

        // Fetch all external sources concurrently (bounded by stream buffering).
        use futures::stream::{self, StreamExt, TryStreamExt};

        const MAX_CONCURRENT_FETCHES: usize = 8;

        let fetch_results: Vec<(String, Vec<RecordBatch>)> = stream::iter(tasks)
            .map(|task| async move {
                let batches = self
                    .fetch_external_data(task.source_info, task.table, task.pushable_predicates)
                    .await?;

                let batches = if !task.local_predicates.is_empty() {
                    apply_local_predicates(&batches, &task.local_predicates)?
                } else {
                    batches
                };

                let temp_table_name = if task.source_info.tables.len() > 1 {
                    format!("{}_{}", task.source_info.temp_table, task.table_idx)
                } else {
                    task.source_info.temp_table.clone()
                };

                if batches.is_empty() {
                    debug!(
                        source = %task.source_info.source_name,
                        table = %task.table,
                        "External source returned no data"
                    );
                }

                Ok::<_, FederationExecutorError>((temp_table_name, batches))
            })
            .buffer_unordered(MAX_CONCURRENT_FETCHES)
            .try_collect()
            .await?;

        let populate_futs: Vec<_> = fetch_results
            .into_iter()
            .filter(|(_, batches)| !batches.is_empty())
            .map(|(temp_table_name, batches)| async move {
                self.create_and_populate_temp_table(&temp_table_name, &batches).await?;
                Ok::<_, FederationExecutorError>(temp_table_name)
            })
            .collect();

        let results = futures::future::join_all(populate_futs).await;
        let mut first_error = None;
        for result in results {
            match result {
                Ok(name) => created_tables.push(name),
                Err(e) if first_error.is_none() => { first_error = Some(e); }
                Err(_) => {}
            }
        }
        if let Some(e) = first_error {
            return Err(e);
        }

        // Rewrite final SQL to use temp table names
        let rewritten_sql = self.rewrite_query_for_temp_tables(final_sql, external_sources)?;

        // Execute the final query
        self.query_executor
            .execute(&rewritten_sql, options)
            .await
            .map_err(FederationExecutorError::from)
    }

    /// Fetch data from an external connector.
    ///
    /// `predicates` contains the pushable predicates already split by the caller.
    /// Connectors that support filtering will use them; others ignore them.
    #[tracing::instrument(name = "warehouse.federation.fetch_external_data", skip(self, source_info, arc_predicates), fields(%table), err(Display))]
    async fn fetch_external_data(
        &self,
        source_info: &ExternalSourceInfo,
        table: &str,
        arc_predicates: Vec<Arc<Predicate>>,
    ) -> FederationExecutorResult<Vec<RecordBatch>> {
        use futures::TryStreamExt;

        let connector = self
            .connector_registry
            .get(&source_info.source_name)
            .ok_or_else(|| FederationExecutorError::ConnectorNotFound {
                source_type: source_info.source_type,
                source_name: source_info.source_name.clone(),
            })?;

        debug!(
            source = %source_info.source_name,
            table = %table,
            source_type = ?source_info.source_type,
            predicate_count = arc_predicates.len(),
            "Fetching data from external source"
        );

        let predicates: Vec<Predicate> = arc_predicates
            .into_iter()
            .map(|arc| Arc::try_unwrap(arc).unwrap_or_else(|a| (*a).clone()))
            .collect();

        let options = FetchOptions {
            predicates,
            ..Default::default()
        };

        let stream = connector
            .fetch_table_stream(table, options)
            .await
            .map_err(FederationExecutorError::from)?;

        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(FederationExecutorError::from)?;

        info!(
            source = %source_info.source_name,
            table = %table,
            batch_count = batches.len(),
            total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            "Fetched data from external source"
        );

        Ok(batches)
    }

    /// Create a temporary table and populate it with Arrow data.
    #[tracing::instrument(name = "warehouse.federation.create_and_populate_temp_table", skip(self, batches), fields(%temp_table), err(Display))]
    async fn create_and_populate_temp_table(
        &self,
        temp_table: &str,
        batches: &[RecordBatch],
    ) -> FederationExecutorResult<()> {
        if batches.is_empty() {
            return Ok(());
        }

        let schema = batches[0].schema();

        // Generate CREATE TABLE statement from Arrow schema
        let create_sql = self.generate_create_temp_table_sql(temp_table, &schema)?;

        debug!(
            temp_table = %temp_table,
            sql = %create_sql,
            "Creating temporary table"
        );

        // Execute CREATE TABLE
        self.execute_ddl(&create_sql).await.map_err(|e| {
            FederationExecutorError::TempTableCreation {
                table: temp_table.to_string(),
                message: e.to_string(),
            }
        })?;

        // Insert data batch by batch
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        debug!(
            temp_table = %temp_table,
            batch_count = batches.len(),
            total_rows = total_rows,
            "Inserting data into temporary table"
        );

        for (idx, batch) in batches.iter().enumerate() {
            self.insert_record_batch_native(temp_table, batch).await.map_err(|e| {
                FederationExecutorError::DataInsertion {
                    table: temp_table.to_string(),
                    message: format!("Batch {}: {}", idx, e),
                }
            })?;
        }

        info!(
            temp_table = %temp_table,
            rows = total_rows,
            "Successfully populated temporary table"
        );

        Ok(())
    }

    /// Generate CREATE TEMPORARY TABLE SQL from Arrow schema.
    fn generate_create_temp_table_sql(
        &self,
        table_name: &str,
        schema: &ArrowSchema,
    ) -> FederationExecutorResult<String> {
        use sqlparser::ast::*;
        use sqlparser::dialect::ClickHouseDialect;
        use sqlparser::parser::Parser;

        let columns: Vec<ColumnDef> = schema
            .fields()
            .iter()
            .map(|field| {
                let ch_type_str = arrow_to_clickhouse_type(field.data_type(), field.is_nullable());
                let dialect = ClickHouseDialect {};
                let data_type = Parser::new(&dialect)
                    .try_with_sql(&ch_type_str)
                    .and_then(|mut p| p.parse_data_type())
                    .unwrap_or_else(|_| {
                        DataType::Custom(
                            ObjectName(vec![Ident::new(&ch_type_str)]),
                            vec![],
                        )
                    });
                ColumnDef {
                    name: Ident::with_quote('`', field.name()),
                    data_type,
                    collation: None,
                    options: vec![],
                }
            })
            .collect();

        let create = CreateTable {
            or_replace: false,
            temporary: true,
            external: false,
            global: None,
            if_not_exists: false,
            transient: false,
            volatile: false,
            name: ObjectName(vec![Ident::with_quote('`', table_name)]),
            columns,
            constraints: vec![],
            hive_distribution: HiveDistributionStyle::NONE,
            hive_formats: None,
            table_properties: vec![],
            with_options: vec![],
            file_format: None,
            location: None,
            query: None,
            without_rowid: false,
            like: None,
            clone: None,
            engine: Some(TableEngine {
                name: "Memory".to_string(),
                parameters: None,
            }),
            comment: None,
            auto_increment_offset: None,
            default_charset: None,
            collation: None,
            on_commit: None,
            on_cluster: None,
            primary_key: None,
            order_by: None,
            partition_by: None,
            cluster_by: None,
            clustered_by: None,
            options: None,
            strict: false,
            copy_grants: false,
            enable_schema_evolution: None,
            change_tracking: None,
            data_retention_time_in_days: None,
            max_data_extension_time_in_days: None,
            default_ddl_collation: None,
            with_aggregation_policy: None,
            with_row_access_policy: None,
            with_tags: None,
        };
        Ok(Statement::CreateTable(create).to_string())
    }

    /// Insert a RecordBatch into ClickHouse via the native binary protocol.
    ///
    /// Converts the Arrow RecordBatch directly to a klickhouse Block
    /// (columnar-to-columnar) and sends it over the native TCP connection,
    /// bypassing SQL text serialization entirely.
    async fn insert_record_batch_native(
        &self,
        table_name: &str,
        batch: &RecordBatch,
    ) -> FederationExecutorResult<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let block = arrow_batch_to_block(batch)?;
        let query = format!("INSERT INTO `{table_name}` FORMAT Native");

        let conn = self
            .query_executor
            .get_native()
            .await
            .map_err(FederationExecutorError::Executor)?;

        let block_stream = futures::stream::once(futures::future::ready(block));
        let mut response = conn
            .insert_native_raw(query, Box::pin(block_stream))
            .await
            .map_err(|e| FederationExecutorError::DataInsertion {
                table: table_name.to_string(),
                message: format!("Native insert failed: {e}"),
            })?;

        while let Some(result) = response.next().await {
            result.map_err(|e| FederationExecutorError::DataInsertion {
                table: table_name.to_string(),
                message: format!("Native insert response error: {e}"),
            })?;
        }

        Ok(())
    }

    /// Rewrite query to use temporary table names via AST manipulation.
    ///
    /// Returns an error if the SQL cannot be parsed -- string manipulation
    /// must never be used for SQL rewriting.
    fn rewrite_query_for_temp_tables(
        &self,
        sql: &str,
        external_sources: &[ExternalSourceInfo],
    ) -> FederationExecutorResult<String> {
        use sqlparser::ast::{Ident, ObjectName, TableFactor};
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let mut replacements: AHashMap<String, String> = AHashMap::with_capacity(external_sources.len());
        let mut ambiguous_unqualified: AHashSet<String> = AHashSet::with_capacity(4);
        for source_info in external_sources {
            for (table_idx, table) in source_info.tables.iter().enumerate() {
                let temp_table_name = if source_info.tables.len() > 1 {
                    format!("{}_{}", source_info.temp_table, table_idx)
                } else {
                    source_info.temp_table.clone()
                };
                let qualified_name = format!("{}.{}", source_info.source_name, table);
                replacements.insert(qualified_name, temp_table_name.clone());
                if ambiguous_unqualified.contains(table) {
                    // Already seen from another source — don't insert
                } else if replacements.contains_key(table) {
                    replacements.remove(table);
                    ambiguous_unqualified.insert(table.clone());
                } else {
                    replacements.insert(table.clone(), temp_table_name);
                }
            }
        }

        let dialect = GenericDialect {};
        let mut statements = Parser::parse_sql(&dialect, sql)
            .map_err(|e| FederationExecutorError::SqlRewrite(
                format!("Failed to parse SQL for temp table rewriting: {e}")
            ))?;

        fn rewrite_table_factor(factor: &mut TableFactor, map: &AHashMap<String, String>) {
            match factor {
                TableFactor::Table { name, .. } => {
                    let table_name = if name.0.len() == 2 {
                        format!("{}.{}", name.0[0].value, name.0[1].value)
                    } else if name.0.len() == 1 {
                        name.0[0].value.clone()
                    } else {
                        return;
                    };
                    if let Some(replacement) = map.get(&table_name) {
                        *name = ObjectName(vec![Ident::with_quote('`', replacement)]);
                    }
                }
                TableFactor::Derived { subquery, .. } => {
                    rewrite_query_body(subquery, map);
                }
                TableFactor::NestedJoin { table_with_joins, .. } => {
                    rewrite_table_factor(&mut table_with_joins.relation, map);
                    for join in &mut table_with_joins.joins {
                        rewrite_table_factor(&mut join.relation, map);
                    }
                }
                _ => {}
            }
        }

        fn rewrite_set_expr(expr: &mut sqlparser::ast::SetExpr, map: &AHashMap<String, String>) {
            match expr {
                sqlparser::ast::SetExpr::Select(sel) => {
                    for twj in &mut sel.from {
                        rewrite_table_factor(&mut twj.relation, map);
                        for join in &mut twj.joins {
                            rewrite_table_factor(&mut join.relation, map);
                        }
                    }
                }
                sqlparser::ast::SetExpr::Query(q) => rewrite_query_body(q, map),
                sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
                    rewrite_set_expr(left, map);
                    rewrite_set_expr(right, map);
                }
                _ => {}
            }
        }

        fn rewrite_query_body(query: &mut sqlparser::ast::Query, map: &AHashMap<String, String>) {
            rewrite_set_expr(&mut query.body, map);
            if let Some(with) = &mut query.with {
                for cte in &mut with.cte_tables {
                    rewrite_query_body(&mut cte.query, map);
                }
            }
        }

        for stmt in &mut statements {
            if let sqlparser::ast::Statement::Query(query) = stmt {
                rewrite_query_body(query, &replacements);
            }
        }

        use std::fmt::Write;
        let mut sql = String::with_capacity(256);
        for (i, s) in statements.iter().enumerate() {
            if i > 0 {
                sql.push_str("; ");
            }
            write!(sql, "{}", s).unwrap();
        }
        Ok(sql)
    }

    /// Execute a DDL statement (CREATE TABLE, DROP TABLE, INSERT).
    /// Uses `execute_raw_query` to avoid appending FORMAT clauses that are
    /// invalid for DDL statements.
    async fn execute_ddl(&self, sql: &str) -> FederationExecutorResult<()> {
        self.query_executor
            .execute_raw_query(sql)
            .await?;
        Ok(())
    }

    /// Execute a MaterializeJoin by materializing each source query
    /// into a ClickHouse temp table, then running the join SQL.
    ///
    /// Uses regular (non-temporary) tables with ENGINE = Memory because
    /// ClickHouse temporary tables are session-scoped and require a
    /// `session_id` query parameter on every HTTP request. Our HTTP client
    /// doesn't maintain ClickHouse sessions, so the temp table wouldn't be
    /// visible to subsequent requests (the join query, cleanup).
    /// Cleanup is handled by `cleanup_temp_tables`; if the process crashes
    /// mid-operation, Memory-engine tables persist until the ClickHouse
    /// server restarts.
    #[tracing::instrument(name = "warehouse.federation.materialise_and_execute_join", skip_all, err(Display))]
    async fn materialise_and_execute_join(
        &self,
        source_queries: &[SourceQuery],
        temp_tables: &[String],
        join_sql: &str,
        options: ExecutionOptions,
        created_tables: &mut Vec<String>,
    ) -> FederationExecutorResult<QueryResult> {
        use sqlparser::ast::*;
        use sqlparser::dialect::ClickHouseDialect;
        use sqlparser::parser::Parser;

        let create_stmts: Vec<(String, String)> = source_queries
            .iter()
            .enumerate()
            .map(|(i, sq)| {
                let temp_name = temp_tables
                    .get(i)
                    .cloned()
                    .unwrap_or_else(|| format!("_temp_mat_{}", i));

                let dialect = ClickHouseDialect {};
                let stmts = Parser::parse_sql(&dialect, &sq.sql).map_err(|e| {
                    FederationExecutorError::Executor(ExecutorError::Execution(format!(
                        "Failed to parse source query for materialization: {}",
                        e
                    )))
                })?;

                let inner_query = match stmts.into_iter().next() {
                    Some(Statement::Query(q)) => q,
                    _ => {
                        return Err(FederationExecutorError::Executor(
                            ExecutorError::Execution(
                                "Source query for materialization must be a SELECT".to_string(),
                            ),
                        ));
                    }
                };

                let create = CreateTable {
                    or_replace: false,
                    temporary: false,
                    external: false,
                    global: None,
                    if_not_exists: false,
                    transient: false,
                    volatile: false,
                    name: ObjectName(vec![Ident::with_quote('`', &temp_name)]),
                    columns: vec![],
                    constraints: vec![],
                    hive_distribution: HiveDistributionStyle::NONE,
                    hive_formats: None,
                    table_properties: vec![],
                    with_options: vec![],
                    file_format: None,
                    location: None,
                    query: Some(inner_query),
                    without_rowid: false,
                    like: None,
                    clone: None,
                    engine: Some(TableEngine {
                        name: "Memory".to_string(),
                        parameters: None,
                    }),
                    comment: None,
                    auto_increment_offset: None,
                    default_charset: None,
                    collation: None,
                    on_commit: None,
                    on_cluster: None,
                    primary_key: None,
                    order_by: None,
                    partition_by: None,
                    cluster_by: None,
                    clustered_by: None,
                    options: None,
                    strict: false,
                    copy_grants: false,
                    enable_schema_evolution: None,
                    change_tracking: None,
                    data_retention_time_in_days: None,
                    max_data_extension_time_in_days: None,
                    default_ddl_collation: None,
                    with_aggregation_policy: None,
                    with_row_access_policy: None,
                    with_tags: None,
                };

                Ok((temp_name, Statement::CreateTable(create).to_string()))
            })
            .collect::<FederationExecutorResult<Vec<_>>>()?;

        let futs: Vec<_> = create_stmts.iter().map(|(temp_name, ddl)| async move {
            self.execute_ddl(ddl).await?;
            Ok::<_, FederationExecutorError>(temp_name.clone())
        }).collect();

        let results = futures::future::join_all(futs).await;
        let mut first_error = None;
        for result in results {
            match result {
                Ok(name) => created_tables.push(name),
                Err(e) if first_error.is_none() => { first_error = Some(e); }
                Err(_) => {}
            }
        }
        if let Some(e) = first_error {
            return Err(e);
        }

        self.query_executor
            .execute(join_sql, options)
            .await
            .map_err(FederationExecutorError::from)
    }

    /// Cleanup temporary tables.
    #[tracing::instrument(name = "warehouse.federation.cleanup_temp_tables", skip(self), fields(table_count = tables.len()))]
    async fn cleanup_temp_tables(&self, tables: &[String]) {
        use sqlparser::ast::{Ident, ObjectName, ObjectType, Statement};

        for table in tables {
            let drop_stmt = Statement::Drop {
                object_type: ObjectType::Table,
                if_exists: true,
                names: vec![ObjectName(vec![Ident::with_quote('`', table)])],
                cascade: false,
                restrict: false,
                purge: false,
                temporary: false,
            };
            if let Err(e) = self.execute_ddl(&drop_stmt.to_string()).await {
                warn!(
                    table = %table,
                    error = %e,
                    "Failed to drop temporary table during cleanup"
                );
            }
        }
    }

    /// Parse `final_sql` and extract WHERE predicates relevant to each
    /// external source table.
    ///
    /// Returns a map from table name to its predicates. Predicates
    /// referencing columns qualified with `source.table.column` are
    /// matched by the table portion; unqualified predicates are included
    /// for every table (the splitter will handle unsupported ones).
    fn extract_predicates_for_tables(
        sql: &str,
        external_sources: &[ExternalSourceInfo],
    ) -> AHashMap<String, Vec<Predicate>> {
        use sqlparser::ast::{SetExpr, Statement};
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let mut result: AHashMap<String, Vec<Predicate>> = AHashMap::with_capacity(external_sources.len());

        let dialect = GenericDialect {};
        let statements = match Parser::parse_sql(&dialect, sql) {
            Ok(s) => s,
            Err(e) => {
                debug!(error = %e, "Failed to parse SQL for predicate extraction");
                return result;
            }
        };

        // Collect all external table names for quick lookup.
        let total_tables: usize = external_sources.iter().map(|src| src.tables.len()).sum();
        let mut all_tables: AHashSet<String> = AHashSet::with_capacity(total_tables);
        for src in external_sources {
            for t in &src.tables {
                all_tables.insert(t.clone());
            }
        }

        for stmt in &statements {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let Some(ref where_expr) = select.selection {
                        let predicates = expr_to_predicates(where_expr);
                        for pred in &predicates {
                            if let Some(col) = pred.column() {
                                // Check if any table has this column -- since we can't
                                // know which table a bare column belongs to, include it
                                // for all external tables. The source splitter will
                                // discard unsupported predicates gracefully.
                                let _ = col;
                            }
                            for t in &all_tables {
                                result.entry(t.clone()).or_default().push(pred.clone());
                            }
                        }
                    }
                }
            }
        }

        result
    }
}

/// Filter `RecordBatch`es by predicates that could not be pushed to the source.
///
/// Uses DataFusion's filter kernel: converts each `Predicate` into a
/// `datafusion::logical_expr::Expr`, builds a physical filter, and
/// evaluates it against each batch.
fn apply_local_predicates(
    batches: &[RecordBatch],
    predicates: &[Predicate],
) -> FederationExecutorResult<Vec<RecordBatch>> {
    use arrow::compute::filter_record_batch;
    use arrow::array::BooleanArray;

    if predicates.is_empty() || batches.is_empty() {
        return Ok(batches.to_vec());
    }

    let mut filtered = Vec::with_capacity(batches.len());
    let mut in_cache: AHashMap<*const Predicate, AHashSet<&str>> = AHashMap::new();

    for batch in batches {
        let schema = batch.schema();
        let num_rows = batch.num_rows();

        let mut mask = vec![true; num_rows];

        for predicate in predicates {
            apply_predicate_to_mask(batch, &schema, predicate, &mut mask, &mut in_cache)?;
        }

        let boolean_array = BooleanArray::from(mask);
        let filtered_batch = filter_record_batch(batch, &boolean_array)
            .map_err(|e| FederationExecutorError::ArrowConversion(
                format!("Failed to apply local predicate filter: {e}")
            ))?;

        if filtered_batch.num_rows() > 0 {
            filtered.push(filtered_batch);
        }
    }

    Ok(filtered)
}

/// Collect all column names referenced by a predicate (for NULL checking in NOT).
fn collect_predicate_columns<'a>(predicate: &'a Predicate, out: &mut Vec<&'a str>) {
    match predicate {
        Predicate::Equals { column, .. }
        | Predicate::In { column, .. }
        | Predicate::GreaterThan { column, .. }
        | Predicate::LessThan { column, .. }
        | Predicate::Between { column, .. }
        | Predicate::Like { column, .. }
        | Predicate::Contains { column, .. }
        | Predicate::IsNull { column, .. } => {
            out.push(column.as_str());
        }
        Predicate::And(preds) | Predicate::Or(preds) => {
            for p in preds {
                collect_predicate_columns(p, out);
            }
        }
        Predicate::Not(inner) => {
            collect_predicate_columns(inner, out);
        }
    }
}

/// Apply a single predicate to a boolean mask over a `RecordBatch`.
///
/// `in_cache` holds pre-built `AHashSet`s for `Predicate::In` variants,
/// keyed by predicate pointer so the set is built once across all batches.
fn apply_predicate_to_mask<'a>(
    batch: &RecordBatch,
    schema: &ArrowSchema,
    predicate: &'a Predicate,
    mask: &mut [bool],
    in_cache: &mut AHashMap<*const Predicate, AHashSet<&'a str>>,
) -> FederationExecutorResult<()> {
    use arrow::array::Array;

    match predicate {
        Predicate::Equals { column, value } => {
            if let Some(col_idx) = schema.index_of(column).ok() {
                let array = batch.column(col_idx);
                apply_comparison(array, value, mask, |s, v| s == v, |a, b| a == b);
            }
        }
        Predicate::In { column, values } => {
            if let Some(col_idx) = schema.index_of(column).ok() {
                let array = batch.column(col_idx);
                let set = in_cache
                    .entry(predicate as *const Predicate)
                    .or_insert_with(|| values.iter().map(|s| s.as_str()).collect());
                for (i, m) in mask.iter_mut().enumerate() {
                    if !*m { continue; }
                    if array.is_null(i) {
                        *m = false;
                    } else if let Some(s) = string_value_at(array, i) {
                        *m = set.contains(s.as_str());
                    }
                }
            }
        }
        Predicate::GreaterThan { column, value, inclusive } => {
            if let Some(col_idx) = schema.index_of(column).ok() {
                let array = batch.column(col_idx);
                let inclusive = *inclusive;
                apply_comparison(
                    array, value, mask,
                    move |s, v| if inclusive { s >= v } else { s > v },
                    move |a, b| if inclusive { a >= b } else { a > b },
                );
            }
        }
        Predicate::LessThan { column, value, inclusive } => {
            if let Some(col_idx) = schema.index_of(column).ok() {
                let array = batch.column(col_idx);
                let inclusive = *inclusive;
                apply_comparison(
                    array, value, mask,
                    move |s, v| if inclusive { s <= v } else { s < v },
                    move |a, b| if inclusive { a <= b } else { a < b },
                );
            }
        }
        Predicate::Between { column, low, high } => {
            if let Some(col_idx) = schema.index_of(column).ok() {
                let array = batch.column(col_idx);
                if array_is_numeric(array) {
                    if let (Ok(lo), Ok(hi)) = (low.parse::<f64>(), high.parse::<f64>()) {
                        for (i, m) in mask.iter_mut().enumerate() {
                            if !*m { continue; }
                            if let Some(n) = numeric_value_at(array, i) {
                                *m = n >= lo && n <= hi;
                            } else {
                                *m = false;
                            }
                        }
                    }
                } else {
                    for (i, m) in mask.iter_mut().enumerate() {
                        if !*m { continue; }
                        if array.is_null(i) {
                            *m = false;
                        } else if let Some(s) = string_value_at(array, i) {
                            *m = s.as_str() >= low.as_str() && s.as_str() <= high.as_str();
                        }
                    }
                }
            }
        }
        Predicate::Like { column, pattern } => {
            if let Some(col_idx) = schema.index_of(column).ok() {
                let array = batch.column(col_idx);
                let re = sql_like_to_regex(pattern);
                for (i, m) in mask.iter_mut().enumerate() {
                    if !*m { continue; }
                    if array.is_null(i) {
                        *m = false;
                    } else if let Some(s) = string_value_at(array, i) {
                        *m = re.is_match(&s);
                    }
                }
            }
        }
        Predicate::Contains { column, substring } => {
            if let Some(col_idx) = schema.index_of(column).ok() {
                let array = batch.column(col_idx);
                for (i, m) in mask.iter_mut().enumerate() {
                    if !*m { continue; }
                    if array.is_null(i) {
                        *m = false;
                    } else if let Some(s) = string_value_at(array, i) {
                        *m = s.contains(substring.as_str());
                    }
                }
            }
        }
        Predicate::IsNull { column, is_null } => {
            if let Some(col_idx) = schema.index_of(column).ok() {
                let array = batch.column(col_idx);
                for (i, m) in mask.iter_mut().enumerate() {
                    if !*m { continue; }
                    *m = array.is_null(i) == *is_null;
                }
            }
        }
        Predicate::And(preds) => {
            for p in preds {
                apply_predicate_to_mask(batch, schema, p, mask, in_cache)?;
            }
        }
        Predicate::Or(preds) => {
            let num_rows = mask.len();
            let mut or_mask = vec![false; num_rows];
            for p in preds {
                let mut branch = mask.to_vec();
                apply_predicate_to_mask(batch, schema, p, &mut branch, in_cache)?;
                for (i, b) in branch.iter().enumerate() {
                    if *b { or_mask[i] = true; }
                }
            }
            for (i, m) in mask.iter_mut().enumerate() {
                *m = *m && or_mask[i];
            }
        }
        Predicate::Not(inner) => {
            let mut inner_mask = vec![true; mask.len()];
            apply_predicate_to_mask(batch, schema, inner, &mut inner_mask, in_cache)?;
            // SQL three-valued logic: NOT(NULL) = NULL (falsy).
            // Inner predicates set mask=false for both non-matching and NULL rows.
            // After inversion, NULL rows would incorrectly become true.
            // Collect columns to re-check for NULLs.
            let mut columns = Vec::new();
            collect_predicate_columns(inner, &mut columns);
            for (i, m) in mask.iter_mut().enumerate() {
                if !*m { continue; }
                if !inner_mask[i] {
                    let is_null = columns.iter().any(|col_name| {
                        schema.index_of(col_name).ok().map_or(false, |idx| {
                            batch.column(idx).is_null(i)
                        })
                    });
                    *m = !is_null;
                } else {
                    *m = false;
                }
            }
        }
    }
    Ok(())
}

/// Extract string value from an Arrow array at a given index.
fn string_value_at(array: &dyn arrow::array::Array, idx: usize) -> Option<String> {
    use arrow::array as arr;
    use arrow::datatypes::DataType;

    if array.is_null(idx) {
        return None;
    }

    match array.data_type() {
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<arr::StringArray>()?;
            Some(a.value(idx).to_string())
        }
        DataType::LargeUtf8 => {
            let a = array.as_any().downcast_ref::<arr::LargeStringArray>()?;
            Some(a.value(idx).to_string())
        }
        DataType::Int8 => {
            let a = array.as_any().downcast_ref::<arr::Int8Array>()?;
            Some(a.value(idx).to_string())
        }
        DataType::Int16 => {
            let a = array.as_any().downcast_ref::<arr::Int16Array>()?;
            Some(a.value(idx).to_string())
        }
        DataType::Int32 => {
            let a = array.as_any().downcast_ref::<arr::Int32Array>()?;
            Some(a.value(idx).to_string())
        }
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<arr::Int64Array>()?;
            Some(a.value(idx).to_string())
        }
        DataType::UInt8 => {
            let a = array.as_any().downcast_ref::<arr::UInt8Array>()?;
            Some(a.value(idx).to_string())
        }
        DataType::UInt16 => {
            let a = array.as_any().downcast_ref::<arr::UInt16Array>()?;
            Some(a.value(idx).to_string())
        }
        DataType::UInt32 => {
            let a = array.as_any().downcast_ref::<arr::UInt32Array>()?;
            Some(a.value(idx).to_string())
        }
        DataType::UInt64 => {
            let a = array.as_any().downcast_ref::<arr::UInt64Array>()?;
            Some(a.value(idx).to_string())
        }
        DataType::Float32 => {
            let a = array.as_any().downcast_ref::<arr::Float32Array>()?;
            Some(a.value(idx).to_string())
        }
        DataType::Float64 => {
            let a = array.as_any().downcast_ref::<arr::Float64Array>()?;
            Some(a.value(idx).to_string())
        }
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<arr::BooleanArray>()?;
            Some(a.value(idx).to_string())
        }
        _ => None,
    }
}

/// Extract a numeric (f64) value from an Arrow array at a given index.
/// Returns `None` for non-numeric types or null values.
fn numeric_value_at(array: &dyn arrow::array::Array, idx: usize) -> Option<f64> {
    use arrow::array as arr;
    use arrow::datatypes::DataType;

    if array.is_null(idx) {
        return None;
    }

    match array.data_type() {
        DataType::Int8 => {
            let a = array.as_any().downcast_ref::<arr::Int8Array>()?;
            Some(a.value(idx) as f64)
        }
        DataType::Int16 => {
            let a = array.as_any().downcast_ref::<arr::Int16Array>()?;
            Some(a.value(idx) as f64)
        }
        DataType::Int32 => {
            let a = array.as_any().downcast_ref::<arr::Int32Array>()?;
            Some(a.value(idx) as f64)
        }
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<arr::Int64Array>()?;
            Some(a.value(idx) as f64)
        }
        DataType::UInt8 => {
            let a = array.as_any().downcast_ref::<arr::UInt8Array>()?;
            Some(a.value(idx) as f64)
        }
        DataType::UInt16 => {
            let a = array.as_any().downcast_ref::<arr::UInt16Array>()?;
            Some(a.value(idx) as f64)
        }
        DataType::UInt32 => {
            let a = array.as_any().downcast_ref::<arr::UInt32Array>()?;
            Some(a.value(idx) as f64)
        }
        DataType::UInt64 => {
            let a = array.as_any().downcast_ref::<arr::UInt64Array>()?;
            Some(a.value(idx) as f64)
        }
        DataType::Float32 => {
            let a = array.as_any().downcast_ref::<arr::Float32Array>()?;
            Some(a.value(idx) as f64)
        }
        DataType::Float64 => {
            let a = array.as_any().downcast_ref::<arr::Float64Array>()?;
            Some(a.value(idx))
        }
        _ => None,
    }
}

fn array_is_numeric(array: &dyn arrow::array::Array) -> bool {
    use arrow::datatypes::DataType;
    matches!(
        array.data_type(),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
            | DataType::Float32 | DataType::Float64
    )
}

/// Apply a comparison predicate against an Arrow array column.
///
/// Uses numeric comparison for numeric arrays and string comparison for others.
fn apply_comparison(
    array: &dyn arrow::array::Array,
    value: &str,
    mask: &mut [bool],
    str_cmp: impl Fn(&str, &str) -> bool,
    num_cmp: impl Fn(f64, f64) -> bool,
) {
    if array_is_numeric(array) {
        if let Ok(parsed_value) = value.parse::<f64>() {
            for (i, m) in mask.iter_mut().enumerate() {
                if !*m { continue; }
                if let Some(n) = numeric_value_at(array, i) {
                    *m = num_cmp(n, parsed_value);
                } else {
                    *m = false;
                }
            }
            return;
        }
    }
    for (i, m) in mask.iter_mut().enumerate() {
        if !*m { continue; }
        if array.is_null(i) {
            *m = false;
        } else if let Some(s) = string_value_at(array, i) {
            *m = str_cmp(&s, value);
        }
    }
}

thread_local! {
    static LIKE_REGEX_CACHE: std::cell::RefCell<AHashMap<String, regex::Regex>> =
        std::cell::RefCell::new(AHashMap::new());
}

/// Convert a SQL LIKE pattern to a regex, caching compiled regexes per thread.
fn sql_like_to_regex(pattern: &str) -> regex::Regex {
    LIKE_REGEX_CACHE.with(|cache| {
        let cache_ref = cache.borrow();
        if let Some(re) = cache_ref.get(pattern) {
            return re.clone();
        }
        drop(cache_ref);

        let compiled = compile_like_regex(pattern);
        cache.borrow_mut().insert(pattern.to_string(), compiled.clone());
        compiled
    })
}

fn compile_like_regex(pattern: &str) -> regex::Regex {
    let mut re = String::with_capacity(pattern.len() + 8);
    re.push_str("(?s)^");
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '%' => re.push_str(".*"),
            '_' => re.push('.'),
            '\\' => {
                if let Some(&next) = chars.peek() {
                    re.push_str(&regex::escape(&next.to_string()));
                    chars.next();
                }
            }
            other => re.push_str(&regex::escape(&other.to_string())),
        }
    }
    re.push('$');
    regex::Regex::new(&re).unwrap_or_else(|_| regex::Regex::new("^$").unwrap())
}

// ============================================================================
// Type Conversion Utilities
// ============================================================================

/// Convert Arrow DataType to ClickHouse type string.
fn arrow_to_clickhouse_type(arrow_type: &ArrowDataType, nullable: bool) -> String {
    let base_type = match arrow_type {
        ArrowDataType::Boolean => "Bool",
        ArrowDataType::Int8 => "Int8",
        ArrowDataType::Int16 => "Int16",
        ArrowDataType::Int32 => "Int32",
        ArrowDataType::Int64 => "Int64",
        ArrowDataType::UInt8 => "UInt8",
        ArrowDataType::UInt16 => "UInt16",
        ArrowDataType::UInt32 => "UInt32",
        ArrowDataType::UInt64 => "UInt64",
        ArrowDataType::Float16 => "Float32",
        ArrowDataType::Float32 => "Float32",
        ArrowDataType::Float64 => "Float64",
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => "String",
        ArrowDataType::Binary | ArrowDataType::LargeBinary => "String",
        ArrowDataType::Date32 | ArrowDataType::Date64 => "Date",
        ArrowDataType::Timestamp(_, _) => "DateTime64(3)",
        ArrowDataType::Time32(_) | ArrowDataType::Time64(_) => "String",
        ArrowDataType::Duration(_) => "Int64",
        ArrowDataType::Decimal128(_, _) | ArrowDataType::Decimal256(_, _) => "String",
        ArrowDataType::List(_) | ArrowDataType::LargeList(_) => "String", // Serialize as JSON
        ArrowDataType::Struct(_) => "String", // Serialize as JSON
        ArrowDataType::Map(_, _) => "String", // Serialize as JSON
        _ => "String", // Default fallback
    };

    if nullable {
        format!("Nullable({})", base_type)
    } else {
        base_type.to_string()
    }
}

/// Map an Arrow DataType to the corresponding klickhouse native Type.
///
/// Must stay in sync with `arrow_to_clickhouse_type` so that the Block column
/// types match the CREATE TABLE DDL.
fn arrow_type_to_klickhouse(arrow_type: &ArrowDataType, nullable: bool) -> KlickhouseType {
    let base = match arrow_type {
        ArrowDataType::Boolean => KlickhouseType::UInt8,
        ArrowDataType::Int8 => KlickhouseType::Int8,
        ArrowDataType::Int16 => KlickhouseType::Int16,
        ArrowDataType::Int32 => KlickhouseType::Int32,
        ArrowDataType::Int64 => KlickhouseType::Int64,
        ArrowDataType::UInt8 => KlickhouseType::UInt8,
        ArrowDataType::UInt16 => KlickhouseType::UInt16,
        ArrowDataType::UInt32 => KlickhouseType::UInt32,
        ArrowDataType::UInt64 => KlickhouseType::UInt64,
        ArrowDataType::Float16 | ArrowDataType::Float32 => KlickhouseType::Float32,
        ArrowDataType::Float64 => KlickhouseType::Float64,
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => KlickhouseType::String,
        ArrowDataType::Binary | ArrowDataType::LargeBinary => KlickhouseType::String,
        ArrowDataType::Date32 | ArrowDataType::Date64 => KlickhouseType::Date,
        ArrowDataType::Timestamp(_, _) => KlickhouseType::DateTime64(3, chrono_tz::UTC),
        ArrowDataType::Time32(_) | ArrowDataType::Time64(_) => KlickhouseType::String,
        ArrowDataType::Duration(_) => KlickhouseType::Int64,
        _ => KlickhouseType::String,
    };
    if nullable {
        KlickhouseType::Nullable(Box::new(base))
    } else {
        base
    }
}

/// Convert a single Arrow column to a Vec of klickhouse Values.
///
/// Handles nullability: null entries become `Value::Null`.
fn arrow_column_to_values(
    array: &dyn arrow::array::Array,
    arrow_type: &ArrowDataType,
    num_rows: usize,
) -> FederationExecutorResult<Vec<KlickhouseValue>> {
    let mut values = Vec::with_capacity(num_rows);

    macro_rules! push_typed {
        ($arr_ty:ty, $variant:ident) => {{
            let arr = array.as_any().downcast_ref::<$arr_ty>().ok_or_else(|| {
                FederationExecutorError::ArrowConversion(format!(
                    "Failed to downcast to {}",
                    stringify!($arr_ty)
                ))
            })?;
            for i in 0..num_rows {
                if array.is_null(i) {
                    values.push(KlickhouseValue::Null);
                } else {
                    values.push(KlickhouseValue::$variant(arr.value(i).into()));
                }
            }
        }};
    }

    match arrow_type {
        ArrowDataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .ok_or_else(|| {
                    FederationExecutorError::ArrowConversion("Failed to downcast to BooleanArray".into())
                })?;
            for i in 0..num_rows {
                if array.is_null(i) {
                    values.push(KlickhouseValue::Null);
                } else {
                    values.push(KlickhouseValue::UInt8(if arr.value(i) { 1 } else { 0 }));
                }
            }
        }
        ArrowDataType::Int8 => push_typed!(arrow_array::Int8Array, Int8),
        ArrowDataType::Int16 => push_typed!(arrow_array::Int16Array, Int16),
        ArrowDataType::Int32 => push_typed!(arrow_array::Int32Array, Int32),
        ArrowDataType::Int64 => push_typed!(arrow_array::Int64Array, Int64),
        ArrowDataType::UInt8 => push_typed!(arrow_array::UInt8Array, UInt8),
        ArrowDataType::UInt16 => push_typed!(arrow_array::UInt16Array, UInt16),
        ArrowDataType::UInt32 => push_typed!(arrow_array::UInt32Array, UInt32),
        ArrowDataType::UInt64 => push_typed!(arrow_array::UInt64Array, UInt64),
        ArrowDataType::Float32 => push_typed!(arrow_array::Float32Array, Float32),
        ArrowDataType::Float64 => push_typed!(arrow_array::Float64Array, Float64),
        ArrowDataType::Float16 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow_array::Float16Array>()
                .ok_or_else(|| {
                    FederationExecutorError::ArrowConversion("Failed to downcast to Float16Array".into())
                })?;
            for i in 0..num_rows {
                if array.is_null(i) {
                    values.push(KlickhouseValue::Null);
                } else {
                    values.push(KlickhouseValue::Float32(arr.value(i).to_f32()));
                }
            }
        }
        ArrowDataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| {
                    FederationExecutorError::ArrowConversion("Failed to downcast to StringArray".into())
                })?;
            for i in 0..num_rows {
                if array.is_null(i) {
                    values.push(KlickhouseValue::Null);
                } else {
                    values.push(KlickhouseValue::String(arr.value(i).as_bytes().to_vec()));
                }
            }
        }
        ArrowDataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow_array::LargeStringArray>()
                .ok_or_else(|| {
                    FederationExecutorError::ArrowConversion("Failed to downcast to LargeStringArray".into())
                })?;
            for i in 0..num_rows {
                if array.is_null(i) {
                    values.push(KlickhouseValue::Null);
                } else {
                    values.push(KlickhouseValue::String(arr.value(i).as_bytes().to_vec()));
                }
            }
        }
        ArrowDataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow_array::BinaryArray>()
                .ok_or_else(|| {
                    FederationExecutorError::ArrowConversion("Failed to downcast to BinaryArray".into())
                })?;
            for i in 0..num_rows {
                if array.is_null(i) {
                    values.push(KlickhouseValue::Null);
                } else {
                    values.push(KlickhouseValue::String(arr.value(i).to_vec()));
                }
            }
        }
        ArrowDataType::LargeBinary => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow_array::LargeBinaryArray>()
                .ok_or_else(|| {
                    FederationExecutorError::ArrowConversion("Failed to downcast to LargeBinaryArray".into())
                })?;
            for i in 0..num_rows {
                if array.is_null(i) {
                    values.push(KlickhouseValue::Null);
                } else {
                    values.push(KlickhouseValue::String(arr.value(i).to_vec()));
                }
            }
        }
        ArrowDataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow_array::Date32Array>()
                .ok_or_else(|| {
                    FederationExecutorError::ArrowConversion("Failed to downcast to Date32Array".into())
                })?;
            for i in 0..num_rows {
                if array.is_null(i) {
                    values.push(KlickhouseValue::Null);
                } else {
                    let days = arr.value(i);
                    if days < 0 || days > u16::MAX as i32 {
                        values.push(KlickhouseValue::Null);
                    } else {
                        values.push(KlickhouseValue::Date(KlickhouseDate(days as u16)));
                    }
                }
            }
        }
        ArrowDataType::Date64 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow_array::Date64Array>()
                .ok_or_else(|| {
                    FederationExecutorError::ArrowConversion("Failed to downcast to Date64Array".into())
                })?;
            for i in 0..num_rows {
                if array.is_null(i) {
                    values.push(KlickhouseValue::Null);
                } else {
                    let days = arr.value(i).div_euclid(86_400_000);
                    if days < 0 || days > u16::MAX as i64 {
                        values.push(KlickhouseValue::Null);
                    } else {
                        values.push(KlickhouseValue::Date(KlickhouseDate(days as u16)));
                    }
                }
            }
        }
        ArrowDataType::Timestamp(unit, _) => {
            let to_millis: fn(i64) -> i64 = match unit {
                TimeUnit::Second => |v| v * 1000,
                TimeUnit::Millisecond => |v| v,
                TimeUnit::Microsecond => |v| v / 1000,
                TimeUnit::Nanosecond => |v| v / 1_000_000,
            };
            macro_rules! push_timestamp {
                ($arr_ty:ty) => {{
                    let arr = array.as_any().downcast_ref::<$arr_ty>().ok_or_else(|| {
                        FederationExecutorError::ArrowConversion(
                            format!("Failed to downcast {}", stringify!($arr_ty))
                        )
                    })?;
                    for i in 0..num_rows {
                        if array.is_null(i) {
                            values.push(KlickhouseValue::Null);
                        } else {
                            let ms = to_millis(arr.value(i));
                            if ms < 0 {
                                values.push(KlickhouseValue::Null);
                            } else {
                                values.push(KlickhouseValue::DateTime64(
                                    DynDateTime64(chrono_tz::UTC, ms as u64, 3),
                                ));
                            }
                        }
                    }
                }};
            }
            match unit {
                TimeUnit::Second => push_timestamp!(arrow_array::TimestampSecondArray),
                TimeUnit::Millisecond => push_timestamp!(arrow_array::TimestampMillisecondArray),
                TimeUnit::Microsecond => push_timestamp!(arrow_array::TimestampMicrosecondArray),
                TimeUnit::Nanosecond => push_timestamp!(arrow_array::TimestampNanosecondArray),
            }
        }
        ArrowDataType::Duration(unit) => match unit {
            TimeUnit::Second => push_typed!(arrow_array::DurationSecondArray, Int64),
            TimeUnit::Millisecond => push_typed!(arrow_array::DurationMillisecondArray, Int64),
            TimeUnit::Microsecond => push_typed!(arrow_array::DurationMicrosecondArray, Int64),
            TimeUnit::Nanosecond => push_typed!(arrow_array::DurationNanosecondArray, Int64),
        },
        _ => {
            for i in 0..num_rows {
                if array.is_null(i) {
                    values.push(KlickhouseValue::Null);
                } else {
                    let s = arrow::util::display::ArrayFormatter::try_new(array, &Default::default())
                        .and_then(|f| Ok(f.value(i).to_string()))
                        .unwrap_or_default();
                    values.push(KlickhouseValue::String(s.into_bytes()));
                }
            }
        }
    }

    Ok(values)
}

/// Convert an Arrow RecordBatch to a klickhouse Block for native insertion.
///
/// Performs a columnar-to-columnar conversion without the row-by-row
/// SQL text serialization overhead.
fn arrow_batch_to_block(batch: &RecordBatch) -> FederationExecutorResult<Block> {
    let schema = batch.schema();
    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();

    let mut column_types = IndexMap::with_capacity(num_cols);
    let mut column_data = IndexMap::with_capacity(num_cols);

    for col_idx in 0..num_cols {
        let field = schema.field(col_idx);
        let array = batch.column(col_idx);

        let kh_type = arrow_type_to_klickhouse(field.data_type(), field.is_nullable());
        let kh_values = arrow_column_to_values(array.as_ref(), field.data_type(), num_rows)?;

        column_types.insert(field.name().clone(), kh_type);
        column_data.insert(field.name().clone(), kh_values);
    }

    Ok(Block {
        info: BlockInfo::default(),
        rows: num_rows as u64,
        column_types,
        column_data,
    })
}


// ============================================================================
// Connector Registry
// ============================================================================

/// Registry for managing external data source connectors.
///
/// Maps source names to connector instances for fetching external data.
/// Uses `parking_lot::RwLock` for synchronous access — all operations are
/// short HashMap lookups/inserts that never cross `.await` boundaries.
pub struct ConnectorRegistry {
    connectors: parking_lot::RwLock<AHashMap<String, Arc<dyn Connector>>>,
}

impl ConnectorRegistry {
    /// Create a new empty connector registry.
    pub fn new() -> Self {
        Self {
            connectors: parking_lot::RwLock::new(AHashMap::new()),
        }
    }

    /// Register a connector for a source.
    pub fn register(&self, source_name: String, connector: Arc<dyn Connector>) {
        self.connectors.write().insert(source_name, connector);
    }

    /// Get a connector by source name.
    pub fn get(&self, source_name: &str) -> Option<Arc<dyn Connector>> {
        self.connectors.read().get(source_name).cloned()
    }

    /// Remove a connector.
    pub fn remove(&self, source_name: &str) -> Option<Arc<dyn Connector>> {
        self.connectors.write().remove(source_name)
    }

    /// List all registered source names.
    pub fn list_sources(&self) -> Vec<String> {
        self.connectors.read().keys().cloned().collect()
    }
}

impl Default for ConnectorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;

    #[test]
    fn test_arrow_to_clickhouse_type() {
        assert_eq!(arrow_to_clickhouse_type(&ArrowDataType::Int64, false), "Int64");
        assert_eq!(arrow_to_clickhouse_type(&ArrowDataType::Int64, true), "Nullable(Int64)");
        assert_eq!(arrow_to_clickhouse_type(&ArrowDataType::Utf8, false), "String");
        assert_eq!(arrow_to_clickhouse_type(&ArrowDataType::Float64, true), "Nullable(Float64)");
        assert_eq!(arrow_to_clickhouse_type(&ArrowDataType::Boolean, false), "Bool");
    }

    #[tokio::test]
    #[ignore = "requires ClickHouse on localhost:9000"]
    async fn test_generate_create_temp_table_sql() {
        let schema = ArrowSchema::new(vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, true),
            Field::new("value", ArrowDataType::Float64, false),
        ]);

        let executor = FederationExecutor::new(
            ClickHouseConfig {
                host: "localhost".to_string(),
                native_port: 9000,
                http_port: 8123,
                database: "default".to_string(),
                username: None,
                password: None,
                pool: super::super::executor::ConnectionPoolConfig::default(),
            },
            Arc::new(ConnectorRegistry::new()),
        ).await.unwrap();

        let sql = executor.generate_create_temp_table_sql("test_table", &schema).unwrap();
        let sql_upper = sql.to_uppercase();

        assert!(sql_upper.contains("CREATE TEMPORARY TABLE"));
        assert!(sql.contains("`test_table`"));
        assert!(sql_upper.contains("`ID` INT64"));
        assert!(sql_upper.contains("NULLABLE"));
        assert!(sql_upper.contains("`VALUE` FLOAT64"));
        assert!(sql_upper.contains("ENGINE"));
    }

    #[tokio::test]
    async fn test_connector_registry() {
        let registry = ConnectorRegistry::new();

        // Initially empty
        assert!(registry.get("test").is_none());
        assert!(registry.list_sources().is_empty());
    }

    #[tokio::test]
    #[ignore = "requires ClickHouse on localhost:9000"]
    async fn test_rewrite_query_ambiguous_unqualified_table_names() {
        let executor = FederationExecutor::new(
            ClickHouseConfig {
                host: "localhost".to_string(),
                native_port: 9000,
                http_port: 8123,
                database: "default".to_string(),
                username: None,
                password: None,
                pool: super::super::executor::ConnectionPoolConfig::default(),
            },
            Arc::new(ConnectorRegistry::new()),
        ).await.unwrap();

        let sources = vec![
            ExternalSourceInfo {
                source_name: "postgres".to_string(),
                source_type: SourceType::PostgreSQL,
                tables: vec!["users".to_string()],
                temp_table: "_tmp_pg".to_string(),
            },
            ExternalSourceInfo {
                source_name: "mysql".to_string(),
                source_type: SourceType::MySQL,
                tables: vec!["users".to_string()],
                temp_table: "_tmp_my".to_string(),
            },
        ];

        // Qualified references must resolve correctly
        let sql = "SELECT * FROM postgres.users JOIN mysql.users ON postgres.users.id = mysql.users.id";
        let rewritten = executor.rewrite_query_for_temp_tables(sql, &sources).unwrap();
        assert!(
            rewritten.contains("_tmp_pg"),
            "postgres.users should map to _tmp_pg, got: {}",
            rewritten
        );
        assert!(
            rewritten.contains("_tmp_my"),
            "mysql.users should map to _tmp_my, got: {}",
            rewritten
        );

        // Unqualified reference must NOT silently resolve to the wrong source
        let sql_unqualified = "SELECT * FROM users";
        let rewritten_unq = executor.rewrite_query_for_temp_tables(sql_unqualified, &sources).unwrap();
        assert!(
            !rewritten_unq.contains("_tmp_pg") && !rewritten_unq.contains("_tmp_my"),
            "Ambiguous unqualified 'users' must not resolve to either temp table, got: {}",
            rewritten_unq
        );
    }

    #[tokio::test]
    #[ignore = "requires ClickHouse on localhost:9000"]
    async fn test_rewrite_query_unique_unqualified_table_names() {
        let executor = FederationExecutor::new(
            ClickHouseConfig {
                host: "localhost".to_string(),
                native_port: 9000,
                http_port: 8123,
                database: "default".to_string(),
                username: None,
                password: None,
                pool: super::super::executor::ConnectionPoolConfig::default(),
            },
            Arc::new(ConnectorRegistry::new()),
        ).await.unwrap();

        let sources = vec![
            ExternalSourceInfo {
                source_name: "postgres".to_string(),
                source_type: SourceType::PostgreSQL,
                tables: vec!["users".to_string()],
                temp_table: "_tmp_pg".to_string(),
            },
            ExternalSourceInfo {
                source_name: "mysql".to_string(),
                source_type: SourceType::MySQL,
                tables: vec!["orders".to_string()],
                temp_table: "_tmp_my".to_string(),
            },
        ];

        // Unique unqualified names should still resolve
        let sql = "SELECT * FROM users JOIN orders ON users.id = orders.user_id";
        let rewritten = executor.rewrite_query_for_temp_tables(sql, &sources).unwrap();
        assert!(
            rewritten.contains("_tmp_pg"),
            "unqualified 'users' should map to _tmp_pg, got: {}",
            rewritten
        );
        assert!(
            rewritten.contains("_tmp_my"),
            "unqualified 'orders' should map to _tmp_my, got: {}",
            rewritten
        );
    }

    #[tokio::test]
    #[ignore = "requires ClickHouse on localhost:9000"]
    async fn test_rewrite_query_unparseable_sql_returns_error() {
        let executor = FederationExecutor::new(
            ClickHouseConfig {
                host: "localhost".to_string(),
                native_port: 9000,
                http_port: 8123,
                database: "default".to_string(),
                username: None,
                password: None,
                pool: super::super::executor::ConnectionPoolConfig::default(),
            },
            Arc::new(ConnectorRegistry::new()),
        ).await.unwrap();

        let sources = vec![ExternalSourceInfo {
            source_name: "postgres".to_string(),
            source_type: SourceType::PostgreSQL,
            tables: vec!["users".to_string()],
            temp_table: "_tmp_pg".to_string(),
        }];

        let bad_sql = "NOT VALID SQL %%% FROM";
        let result = executor.rewrite_query_for_temp_tables(bad_sql, &sources);
        assert!(
            result.is_err(),
            "Unparseable SQL must return an error, not silently fall back to string replacement"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("SQL rewrite error"),
            "Error must be a SqlRewrite variant, got: {}",
            err_msg
        );
    }

    #[tokio::test]
    #[ignore = "requires ClickHouse on localhost:9000"]
    async fn test_rewrite_query_does_not_corrupt_similar_identifiers() {
        let executor = FederationExecutor::new(
            ClickHouseConfig {
                host: "localhost".to_string(),
                native_port: 9000,
                http_port: 8123,
                database: "default".to_string(),
                username: None,
                password: None,
                pool: super::super::executor::ConnectionPoolConfig::default(),
            },
            Arc::new(ConnectorRegistry::new()),
        ).await.unwrap();

        let sources = vec![ExternalSourceInfo {
            source_name: "pg".to_string(),
            source_type: SourceType::PostgreSQL,
            tables: vec!["users".to_string()],
            temp_table: "_tmp_users".to_string(),
        }];

        let sql = "SELECT users_count FROM users WHERE active = 1";
        let rewritten = executor.rewrite_query_for_temp_tables(sql, &sources).unwrap();
        assert!(
            rewritten.contains("users_count"),
            "Column 'users_count' must not be corrupted, got: {}",
            rewritten
        );
        assert!(
            rewritten.contains("_tmp_users"),
            "Table 'users' must be rewritten, got: {}",
            rewritten
        );
    }

    #[test]
    fn test_date32_pre_epoch_becomes_null() {
        let arr = arrow_array::Date32Array::from(vec![Some(-1), Some(0), Some(20000), None]);
        let values = arrow_column_to_values(&arr, &ArrowDataType::Date32, 4).unwrap();
        assert!(matches!(values[0], KlickhouseValue::Null), "pre-epoch Date32 must become Null");
        assert!(matches!(values[1], KlickhouseValue::Date(KlickhouseDate(0))));
        assert!(matches!(values[2], KlickhouseValue::Date(KlickhouseDate(20000))));
        assert!(matches!(values[3], KlickhouseValue::Null));
    }

    #[test]
    fn test_date32_overflow_u16_becomes_null() {
        let arr = arrow_array::Date32Array::from(vec![Some(u16::MAX as i32 + 1)]);
        let values = arrow_column_to_values(&arr, &ArrowDataType::Date32, 1).unwrap();
        assert!(matches!(values[0], KlickhouseValue::Null), "Date32 > u16::MAX must become Null");
    }

    #[test]
    fn test_date64_pre_epoch_becomes_null() {
        let arr = arrow_array::Date64Array::from(vec![Some(-86_400_000i64), Some(0)]);
        let values = arrow_column_to_values(&arr, &ArrowDataType::Date64, 2).unwrap();
        assert!(matches!(values[0], KlickhouseValue::Null), "pre-epoch Date64 must become Null");
        assert!(matches!(values[1], KlickhouseValue::Date(KlickhouseDate(0))));
    }

    #[test]
    fn test_timestamp_pre_epoch_becomes_null() {
        let arr = arrow_array::TimestampMillisecondArray::from(vec![Some(-1000i64), Some(1000)]);
        let dt = ArrowDataType::Timestamp(TimeUnit::Millisecond, None);
        let values = arrow_column_to_values(&arr, &dt, 2).unwrap();
        assert!(matches!(values[0], KlickhouseValue::Null), "pre-epoch timestamp must become Null");
        assert!(matches!(values[1], KlickhouseValue::DateTime64(..)));
    }

    #[test]
    fn test_duration_millisecond_downcast_succeeds() {
        let arr = arrow_array::DurationMillisecondArray::from(vec![Some(5000i64), None]);
        let dt = ArrowDataType::Duration(TimeUnit::Millisecond);
        let values = arrow_column_to_values(&arr, &dt, 2).unwrap();
        assert!(matches!(values[0], KlickhouseValue::Int64(5000)));
        assert!(matches!(values[1], KlickhouseValue::Null));
    }

    #[test]
    fn test_duration_nanosecond_downcast_succeeds() {
        let arr = arrow_array::DurationNanosecondArray::from(vec![Some(999i64)]);
        let dt = ArrowDataType::Duration(TimeUnit::Nanosecond);
        let values = arrow_column_to_values(&arr, &dt, 1).unwrap();
        assert!(matches!(values[0], KlickhouseValue::Int64(999)));
    }

    #[test]
    fn test_numeric_comparison_greater_than() {
        use arrow::array::Int64Array;

        let array = Int64Array::from(vec![1, 3, 10, 20, 2]);
        let mut mask = vec![true; 5];
        apply_comparison(
            &array, "2", &mut mask,
            |s, v| s > v,
            |a, b| a > b,
        );
        assert_eq!(mask, vec![false, true, true, true, false]);
    }

    #[test]
    fn test_numeric_comparison_less_than() {
        use arrow::array::Int64Array;

        let array = Int64Array::from(vec![1, 3, 10, 20, 2]);
        let mut mask = vec![true; 5];
        apply_comparison(
            &array, "10", &mut mask,
            |s, v| s < v,
            |a, b| a < b,
        );
        assert_eq!(mask, vec![true, true, false, false, true]);
    }

    #[test]
    fn test_numeric_between() {
        use arrow::array::Int64Array;

        let schema = ArrowSchema::new(vec![
            Field::new("amount", ArrowDataType::Int64, false),
        ]);
        let array = Int64Array::from(vec![1, 5, 10, 20, 100]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(array)],
        ).unwrap();

        let mut mask = vec![true; 5];
        let pred = Predicate::Between {
            column: "amount".into(),
            low: "5".into(),
            high: "20".into(),
        };
        let mut in_cache = AHashMap::new();
        apply_predicate_to_mask(&batch, &schema, &pred, &mut mask, &mut in_cache).unwrap();
        assert_eq!(mask, vec![false, true, true, true, false]);
    }

    #[test]
    fn test_string_comparison_still_works() {
        use arrow::array::StringArray;

        let array = StringArray::from(vec!["apple", "banana", "cherry"]);
        let mut mask = vec![true; 3];
        apply_comparison(
            &array, "banana", &mut mask,
            |s, v| s > v,
            |a, b| a > b,
        );
        assert_eq!(mask, vec![false, false, true]);
    }

    #[test]
    fn test_date64_pre_epoch_uses_euclidean_division() {
        // 1969-12-31 12:00 UTC = -43_200_000 ms since epoch.
        // Truncating division: -43_200_000 / 86_400_000 = 0 (WRONG: maps to 1970-01-01)
        // Euclidean division: (-43_200_000_i64).div_euclid(86_400_000) = -1 (correct: 1969-12-31)
        let pre_epoch_millis: i64 = -43_200_000;

        let days_trunc = pre_epoch_millis / 86_400_000;
        let days_euclid = pre_epoch_millis.div_euclid(86_400_000);

        assert_eq!(days_trunc, 0, "sanity: truncating division gives wrong day 0");
        assert_eq!(days_euclid, -1, "euclidean division should give day -1");

        // Verify the guard catches the negative day and maps to Null (days < 0)
        assert!(days_euclid < 0, "pre-epoch day should be negative and map to Null");
    }

    #[test]
    fn test_compile_like_regex_matches_newlines() {
        let re = compile_like_regex("%test%");
        assert!(re.is_match("hello\ntest"), "'%test%' should match across newlines");
        assert!(re.is_match("test\nworld"), "'%test%' should match before newline");

        let re2 = compile_like_regex("a_c");
        assert!(re2.is_match("a\nc"), "'_' should match a newline character");
    }

    #[test]
    fn test_not_predicate_excludes_null_rows() {
        use arrow::array::StringArray;

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("status", arrow::datatypes::DataType::Utf8, true),
        ]));
        let status_array = StringArray::from(vec![
            Some("active"),
            None,          // NULL row
            Some("inactive"),
        ]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(status_array)]).unwrap();

        // NOT(status = 'inactive') should exclude NULLs per SQL three-valued logic
        let predicate = Predicate::Not(Box::new(Predicate::Equals {
            column: "status".into(),
            value: "inactive".into(),
        }));

        let mut mask = vec![true; 3];
        let mut in_cache = AHashMap::new();
        apply_predicate_to_mask(&batch, &schema, &predicate, &mut mask, &mut in_cache).unwrap();

        assert!(mask[0], "active row should pass NOT(status='inactive')");
        assert!(!mask[1], "NULL row must be excluded by NOT predicate");
        assert!(!mask[2], "inactive row should be excluded by NOT(status='inactive')");
    }
}
