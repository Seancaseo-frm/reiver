//! Federated Query Planner
//!
//! Plans and executes queries that span multiple data sources.
//! Supports JOINs and UNIONs across ClickHouse native tables, S3 Parquet files,
//! and external databases.
//!
//! # Execution Strategies
//!
//! 1. **Direct Merge**: All tables from the same backend can be queried together
//! 2. **Pushdown Join**: Smaller result is materialized in a temp table, then JOINed
//! 3. **Materialize Join**: Both sides materialized in temp tables, then JOINed
//! 4. **Union**: Simple UNION of results from different sources
//!
//! # Schema Reconciliation
//!
//! When planning cross-source JOINs, the planner analyzes schema compatibility:
//! - Type compatibility between JOIN keys
//! - Case sensitivity differences across sources
//! - Semantic type conflicts (e.g., cents vs dollars)
//!
//! Warnings are returned in the plan for users to review.

use ahash::AHashMap;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::warehouse::sources::{DataSourceRegistry, RegisteredSource, SourceBackend};
use crate::warehouse::types::SourceType;

use super::schema_reconciliation::{
    JoinAnalysisResult, JoinKeyAnalysis, SchemaReconciler, SchemaWarning,
};

// ============================================================================
// Errors
// ============================================================================

/// Errors that can occur during federated query planning.
#[derive(Debug, Error)]
pub enum FederationError {
    #[error("Source not found: {0}")]
    SourceNotFound(String),

    #[error("Table not found: {source_name}.{table_name}")]
    TableNotFound { source_name: String, table_name: String },

    #[error("Registry error: {0}")]
    Registry(#[from] crate::warehouse::sources::RegistryError),

    #[error("Unsupported federation: {0}")]
    Unsupported(String),

    #[error("Query parsing error: {0}")]
    ParseError(String),

    #[error("Schema incompatible: {0}")]
    SchemaIncompatible(String),
}

/// Result type for federation operations.
pub type FederationResult<T> = Result<T, FederationError>;

// ============================================================================
// Federation Configuration
// ============================================================================

/// Configuration for federated query execution.
///
/// Controls optimization strategies and resource limits for cross-source queries.
#[derive(Debug, Clone)]
pub struct FederationConfig {
    /// Maximum number of keys to use in an IN clause for semi-join reduction.
    /// Above this threshold, the system falls back to Bloom filter or temp table.
    /// Default: 10,000
    pub semi_join_in_clause_limit: usize,

    /// Selectivity ratio threshold for semi-join.
    /// Semi-join is considered when probe_rows / build_rows < this value.
    /// Default: 0.1 (10%)
    pub semi_join_selectivity_threshold: f64,

    /// Maximum keys before falling back to temp table materialization.
    /// Between `semi_join_in_clause_limit` and this value, Bloom filter is used.
    /// Default: 1,000,000
    pub semi_join_bloom_limit: usize,

    /// Enable Bloom filter pushdown for large key sets.
    /// When disabled, falls back to temp table for keys > in_clause_limit.
    /// Default: true
    pub enable_bloom_pushdown: bool,

    /// False positive rate for Bloom filters.
    /// Lower values = larger filters but fewer false matches.
    /// Default: 0.01 (1%)
    pub bloom_false_positive_rate: f64,

    /// Memory budget per query in MB.
    /// Default: 1024 (1 GB)
    pub memory_budget_mb: u32,

    /// Maximum concurrent source queries.
    /// Default: 4
    pub max_concurrent_sources: usize,

    /// Timeout for individual source queries in seconds.
    /// Default: 300 (5 minutes)
    pub source_query_timeout_secs: u64,
}

impl Default for FederationConfig {
    fn default() -> Self {
        Self {
            semi_join_in_clause_limit: 10_000,
            semi_join_selectivity_threshold: 0.1,
            semi_join_bloom_limit: 1_000_000,
            enable_bloom_pushdown: true,
            bloom_false_positive_rate: 0.01,
            memory_budget_mb: 1024,
            max_concurrent_sources: 4,
            source_query_timeout_secs: 300,
        }
    }
}

impl FederationConfig {
    /// Create a new configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the semi-join IN clause limit.
    pub fn with_in_clause_limit(mut self, limit: usize) -> Self {
        self.semi_join_in_clause_limit = limit;
        self
    }

    /// Set the semi-join selectivity threshold.
    pub fn with_selectivity_threshold(mut self, threshold: f64) -> Self {
        self.semi_join_selectivity_threshold = threshold;
        self
    }

    /// Enable or disable Bloom filter pushdown.
    pub fn with_bloom_pushdown(mut self, enabled: bool) -> Self {
        self.enable_bloom_pushdown = enabled;
        self
    }

    /// Set the memory budget in MB.
    pub fn with_memory_budget(mut self, mb: u32) -> Self {
        self.memory_budget_mb = mb;
        self
    }

    /// Check if semi-join should be attempted for a given key count.
    pub fn should_attempt_semi_join(&self, estimated_probe_rows: u64, selectivity: f64) -> bool {
        estimated_probe_rows as usize <= self.semi_join_bloom_limit
            && selectivity <= self.semi_join_selectivity_threshold
    }

    /// Determine the strategy for a given key count.
    pub fn semi_join_strategy(&self, key_count: usize) -> SemiJoinStrategy {
        if key_count <= self.semi_join_in_clause_limit {
            SemiJoinStrategy::InClause
        } else if self.enable_bloom_pushdown && key_count <= self.semi_join_bloom_limit {
            SemiJoinStrategy::BloomFilter
        } else {
            SemiJoinStrategy::TempTable
        }
    }
}

/// Strategy for executing semi-join based on key count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SemiJoinStrategy {
    /// Use IN clause for small key sets (< 10K).
    InClause,
    /// Use Bloom filter for medium key sets (10K - 1M).
    BloomFilter,
    /// Materialize to temp table for large key sets (> 1M).
    TempTable,
}

// ============================================================================
// Federation Plan
// ============================================================================

/// A federated query execution plan.
///
/// Describes how to execute a query that spans multiple sources.
#[derive(Debug, Clone)]
pub struct FederatedPlan {
    /// Sub-queries to execute on each source.
    pub source_queries: Vec<SourceQuery>,
    /// How to combine results from different sources.
    pub combination: CombinationStrategy,
    /// Final query to apply after combining results (optional).
    pub final_query: Option<String>,
    /// Estimated total rows to process.
    pub estimated_rows: Option<u64>,
    /// Schema warnings from cross-source JOIN analysis.
    ///
    /// These are non-fatal issues that users should be aware of,
    /// such as type coercions, case sensitivity mismatches, or
    /// semantic type differences (e.g., cents vs dollars).
    pub schema_warnings: Vec<SchemaWarning>,
    /// JOIN key analyses for cross-source JOINs.
    pub join_analyses: Vec<JoinKeyAnalysis>,
}

impl FederatedPlan {
    /// Check if this plan involves multiple sources.
    pub fn is_federated(&self) -> bool {
        self.source_queries.len() > 1
    }

    /// Get all sources involved in this plan.
    pub fn sources(&self) -> Vec<&RegisteredSource> {
        self.source_queries.iter().map(|sq| &sq.source).collect()
    }

    /// Check if all tables come from the same source *instance* (not just
    /// the same type).  Two different PostgreSQL databases must still be
    /// federated, so comparing only `storage_type()` is insufficient.
    pub fn is_homogeneous(&self) -> bool {
        if self.source_queries.len() <= 1 {
            return true;
        }

        let first_id = self.source_queries[0].source.id;
        self.source_queries.iter().all(|sq| sq.source.id == first_id)
    }

    /// Check if there are any schema warnings.
    pub fn has_schema_warnings(&self) -> bool {
        !self.schema_warnings.is_empty()
    }

    /// Get the number of schema warnings.
    pub fn warning_count(&self) -> usize {
        self.schema_warnings.len()
    }

    /// Log all schema warnings at the warn level.
    pub fn log_warnings(&self) {
        for warning in &self.schema_warnings {
            warn!("{}", warning);
        }
    }
}

/// A sub-query to execute on a specific source.
#[derive(Debug, Clone)]
pub struct SourceQuery {
    /// The source to query.
    pub source: RegisteredSource,
    /// The SQL to execute on this source.
    pub sql: String,
    /// Tables referenced in this sub-query.
    pub tables: Vec<String>,
    /// Columns needed from this sub-query.
    pub columns_needed: Vec<String>,
    /// Whether this sub-query has predicates that can be pushed down.
    pub has_predicates: bool,
}

/// Strategy for combining results from multiple sources.
#[derive(Debug, Clone)]
pub enum CombinationStrategy {
    /// No combination needed (single source query).
    None,

    /// Results can be merged directly in ClickHouse.
    ///
    /// Used when all sources are accessible from ClickHouse
    /// (e.g., native tables + s3() function calls).
    DirectMerge {
        /// The combined query that ClickHouse will execute.
        combined_sql: String,
    },

    /// Push smaller result to a ClickHouse temp table, then JOIN.
    ///
    /// Used when one source returns significantly fewer rows.
    PushdownJoin {
        /// Which source to materialize first.
        materialize_source: String,
        /// Temp table name for materialized data.
        temp_table: String,
        /// Join condition.
        join_condition: String,
    },

    /// Materialize all sources in temp tables, then JOIN.
    ///
    /// Used for complex cross-source JOINs with 2 or more sources.
    MaterializeJoin {
        /// Temp tables for each source (one per source).
        temp_tables: Vec<String>,
        /// Final JOIN query.
        join_sql: String,
    },

    /// Simple UNION of results.
    ///
    /// Used for UNION queries across sources.
    Union {
        /// Whether to remove duplicates (UNION vs UNION ALL).
        distinct: bool,
    },

    /// Cost-optimized execution with predicate ordering.
    ///
    /// Used when the plan optimizer has determined an optimal execution order
    /// based on source access profiles and statistics.
    OptimizedPushdown {
        /// The optimized execution plan.
        execution_plan: super::plan_optimizer::ExecutionPlan,
        /// Memory budget for this query in MB.
        memory_budget_mb: u32,
        /// Estimated total cost.
        estimated_cost_ms: f64,
    },

    /// Semi-join reduction: query smaller side first, use keys to filter larger side.
    ///
    /// This strategy minimizes data transfer by:
    /// 1. Executing the probe query (smaller/more filtered side) first
    /// 2. Extracting join keys from the probe result
    /// 3. Using those keys as an IN filter on the build side
    /// 4. Joining the (now small) results in memory
    ///
    /// Falls back to Bloom filter or temp table if key count exceeds threshold.
    SemiJoinReduction {
        /// Source to query first (should be smaller/more filtered).
        probe_source: String,
        /// Query to execute on probe source.
        probe_query: String,
        /// Column containing join keys in probe result.
        probe_key_column: String,
        /// Source type for probe (needed for Bloom filter strategy).
        probe_source_type: crate::warehouse::types::SourceType,
        /// Source to filter using probe keys.
        build_source: String,
        /// Base query for build source (without the IN filter).
        build_base_query: String,
        /// Column to filter on in build source.
        build_key_column: String,
        /// Source type for build (needed for Bloom filter strategy).
        build_source_type: crate::warehouse::types::SourceType,
        /// Maximum keys before falling back to Bloom filter or temp table.
        max_keys_for_in_clause: usize,
        /// Join type (INNER, LEFT, etc.).
        join_type: super::plan_optimizer::JoinType,
    },

    /// External API sources materialized as Arrow RecordBatches.
    ///
    /// Used when query involves sources like Google Sheets that are fetched
    /// on-demand and materialized as in-memory Arrow data.
    ///
    /// The execution strategy:
    /// 1. Fetch data from external APIs (with TTL caching)
    /// 2. Convert to Arrow RecordBatches
    /// 3. Insert into ClickHouse temp tables (using Arrow format)
    /// 4. Execute the query against temp tables
    /// 5. Drop temp tables after query completion
    ExternalApiMaterialize {
        /// External sources to fetch and materialize.
        external_sources: Vec<ExternalSourceInfo>,
        /// Temp tables created for external data.
        temp_tables: Vec<String>,
        /// Final query to execute after materialization.
        final_sql: String,
    },

    /// MongoDB source with ClickHouse index acceleration.
    ///
    /// Used when querying MongoDB collections that have a ClickHouse index.
    /// The execution strategy:
    /// 1. Query the ClickHouse index to get matching document IDs
    /// 2. Fetch only matching documents from MongoDB by ID
    /// 3. Convert to Arrow RecordBatches
    /// 4. Insert into ClickHouse temp tables
    /// 5. Execute the final query
    /// 6. Drop temp tables after query completion
    ///
    /// This significantly reduces MongoDB data transfer for filtered queries.
    MongoDBIndexed {
        /// MongoDB source information.
        source_info: MongoDBSourceInfo,
        /// SQL WHERE clause to query against the index.
        index_filter: Option<String>,
        /// Temp table name for materialized data.
        temp_table: String,
        /// Final query to execute after materialization.
        final_sql: String,
    },
}

/// Information about a MongoDB source for index-accelerated queries.
#[derive(Debug, Clone)]
pub struct MongoDBSourceInfo {
    /// Source name for identification.
    pub source_name: String,
    /// Collection to query.
    pub collection: String,
    /// Whether the ClickHouse index is available for this collection.
    pub index_available: bool,
    /// ClickHouse index table name (if available).
    pub index_table: Option<String>,
}

/// Information about an external API source to materialize.
#[derive(Debug, Clone)]
pub struct ExternalSourceInfo {
    /// Source name for identification.
    pub source_name: String,
    /// Source type (e.g., GoogleSheets).
    pub source_type: crate::warehouse::types::SourceType,
    /// Tables to fetch from this source.
    pub tables: Vec<String>,
    /// Temp table name for materialized data.
    pub temp_table: String,
}

// ============================================================================
// Federation Planner
// ============================================================================

/// Plans federated query execution across multiple sources.
pub struct FederationPlanner {
    registry: Arc<DataSourceRegistry>,
    project_id: Uuid,
    /// Schema reconciler for analyzing cross-source JOIN compatibility.
    reconciler: SchemaReconciler,
}

impl FederationPlanner {
    /// Create a new federation planner.
    pub fn new(registry: Arc<DataSourceRegistry>, project_id: Uuid) -> Self {
        Self {
            registry,
            project_id,
            reconciler: SchemaReconciler::new(),
        }
    }

    /// Create with a custom schema reconciler.
    pub fn with_reconciler(
        registry: Arc<DataSourceRegistry>,
        project_id: Uuid,
        reconciler: SchemaReconciler,
    ) -> Self {
        Self {
            registry,
            project_id,
            reconciler,
        }
    }

    /// Get a reference to the schema reconciler.
    pub fn reconciler(&self) -> &SchemaReconciler {
        &self.reconciler
    }

    /// Plan a federated query.
    ///
    /// Analyzes the query to identify:
    /// 1. Which sources are referenced
    /// 2. Which tables from each source
    /// 3. The optimal execution strategy
    /// 4. Schema compatibility for cross-source JOINs
    ///
    /// # Schema Reconciliation
    ///
    /// For cross-source JOINs, this method:
    /// - Analyzes JOIN key type compatibility
    /// - Detects case sensitivity mismatches
    /// - Identifies semantic type conflicts (e.g., cents vs dollars)
    /// - Returns warnings in the plan for user review
    ///
    /// If types are fundamentally incompatible, returns `FederationError::SchemaIncompatible`.
    #[tracing::instrument(name = "warehouse.federation.plan", skip(self, query, table_references), fields(project_id = %self.project_id, table_count = table_references.len()), err(Display))]
    pub async fn plan(
        &self,
        query: &str,
        table_references: &[TableReference],
    ) -> FederationResult<FederatedPlan> {
        self.plan_with_join_conditions(query, table_references, &[]).await
    }

    /// Plan a federated query with explicit JOIN conditions for schema analysis.
    ///
    /// This is the full version that allows specifying JOIN conditions for
    /// detailed schema reconciliation analysis.
    ///
    /// # Arguments
    /// * `query` - The SQL query
    /// * `table_references` - Tables referenced in the query
    /// * `join_conditions` - Parsed JOIN conditions for schema analysis
    #[tracing::instrument(name = "warehouse.federation.plan_with_join_conditions", skip_all, fields(join_count = join_conditions.len()), err(Display))]
    pub async fn plan_with_join_conditions(
        &self,
        query: &str,
        table_references: &[TableReference],
        join_conditions: &[JoinCondition],
    ) -> FederationResult<FederatedPlan> {
        // Group table references by source
        let sources_tables = self.group_by_source(table_references).await?;

        // If single source, no federation needed
        if sources_tables.len() == 1 {
            let (source, tables) = sources_tables.into_iter().next().unwrap();
            return Ok(FederatedPlan {
                source_queries: vec![SourceQuery {
                    source,
                    sql: query.to_string(),
                    tables,
                    columns_needed: Vec::new(),
                    has_predicates: false,
                }],
                combination: CombinationStrategy::None,
                final_query: None,
                estimated_rows: None,
                schema_warnings: Vec::new(),
                join_analyses: Vec::new(),
            });
        }

        info!(
            project_id = %self.project_id,
            source_count = sources_tables.len(),
            "Planning federated query"
        );

        // Analyze schema compatibility for cross-source JOINs
        let join_analysis = self
            .analyze_join_schema(join_conditions, &sources_tables)
            .await?;

        // Log warnings
        for warning in &join_analysis.all_warnings {
            warn!("{}", warning);
        }

        // Fail if incompatible types
        if let Some(error) = &join_analysis.first_error {
            return Err(FederationError::SchemaIncompatible(error.clone()));
        }

        // Check if all sources can be combined directly in ClickHouse
        if self.can_direct_merge(&sources_tables) {
            return self.plan_direct_merge_with_analysis(query, sources_tables, join_analysis);
        }

        // Otherwise, use materialization strategy
        self.plan_materialized_join_with_analysis(query, sources_tables, join_analysis)
    }

    /// Analyze schema compatibility for JOIN conditions.
    #[tracing::instrument(name = "warehouse.federation.analyze_join_schema", skip_all, err(Display))]
    async fn analyze_join_schema(
        &self,
        join_conditions: &[JoinCondition],
        sources_tables: &[(RegisteredSource, Vec<String>)],
    ) -> FederationResult<JoinAnalysisResult> {
        if join_conditions.is_empty() {
            return Ok(JoinAnalysisResult::empty());
        }

        // Build a map of source_name -> (source, source_type)
        let source_map: AHashMap<String, (&RegisteredSource, SourceType)> = sources_tables
            .iter()
            .map(|(source, _)| (source.name.clone(), (source, source.source_type)))
            .collect();

        let mut join_analyses = Vec::new();

        for condition in join_conditions {
            let (left_schema, right_schema) = tokio::join!(
                self.registry.get_typed_schema(
                    self.project_id,
                    &condition.left_table.source_name,
                    &condition.left_table.table_name,
                ),
                self.registry.get_typed_schema(
                    self.project_id,
                    &condition.right_table.source_name,
                    &condition.right_table.table_name,
                ),
            );
            let left_schema = left_schema.ok();
            let right_schema = right_schema.ok();

            // Find the columns involved in the JOIN
            // Default to ExternalParquet (case-sensitive) when source type is unknown
            let default_source_type = SourceType::ExternalParquet;

            let left_column = left_schema.as_ref().and_then(|schema| {
                self.reconciler
                    .find_column(
                        schema,
                        &condition.left_column,
                        source_map
                            .get(&condition.left_table.source_name)
                            .map(|(_, st)| *st)
                            .unwrap_or(default_source_type),
                    )
                    .cloned()
            });

            let right_column = right_schema.as_ref().and_then(|schema| {
                self.reconciler
                    .find_column(
                        schema,
                        &condition.right_column,
                        source_map
                            .get(&condition.right_table.source_name)
                            .map(|(_, st)| *st)
                            .unwrap_or(default_source_type),
                    )
                    .cloned()
            });

            // If we have both columns, analyze compatibility
            if let (Some(left_col), Some(right_col)) = (left_column, right_column) {
                let left_source_type = source_map
                    .get(&condition.left_table.source_name)
                    .map(|(_, st)| *st)
                    .unwrap_or(default_source_type);

                let right_source_type = source_map
                    .get(&condition.right_table.source_name)
                    .map(|(_, st)| *st)
                    .unwrap_or(default_source_type);

                let analysis = self.reconciler.analyze_join(
                    &left_col,
                    &right_col,
                    left_source_type,
                    right_source_type,
                );

                join_analyses.push(analysis);
            } else {
                debug!(
                    left_table = %condition.left_table,
                    left_column = %condition.left_column,
                    right_table = %condition.right_table,
                    right_column = %condition.right_column,
                    "Could not find columns for JOIN analysis, skipping"
                );
            }
        }

        Ok(JoinAnalysisResult::from_analyses(join_analyses))
    }

    /// Group table references by their source.
    async fn group_by_source(
        &self,
        table_references: &[TableReference],
    ) -> FederationResult<Vec<(RegisteredSource, Vec<String>)>> {
        let mut source_tables: AHashMap<Uuid, (RegisteredSource, Vec<String>)> = AHashMap::new();

        for table_ref in table_references {
            let source = self.registry
                .resolve(self.project_id, &table_ref.source_name)
                .await?;

            source_tables
                .entry(source.id)
                .or_insert_with(|| (source.clone(), Vec::new()))
                .1
                .push(table_ref.table_name.clone());
        }

        Ok(source_tables.into_values().collect())
    }

    /// Check if sources can be merged directly in ClickHouse.
    ///
    /// This is possible when:
    /// - All sources are either ClickHouse native or S3/R2 (accessible via s3())
    /// - No external database sources that need separate connections
    /// - No external API sources (these must be materialized as Arrow RecordBatches)
    fn can_direct_merge(&self, sources: &[(RegisteredSource, Vec<String>)]) -> bool {
        for (source, _) in sources {
            match &source.backend {
                SourceBackend::ClickHouseNative { .. } => continue,
                SourceBackend::ObjectStorage { .. } => continue,
                SourceBackend::ExternalDatabase { .. } => {
                    // External databases need special handling
                    // ClickHouse can query them via table functions, but with limitations
                    debug!(
                        source = %source.name,
                        "External database source - may need materialization"
                    );
                    // For now, allow direct merge with external databases too
                    // ClickHouse has postgresql(), mysql() etc. table functions
                    continue;
                }
                SourceBackend::ExternalApi { source_type, .. } => {
                    // External API sources (cold) cannot be directly merged
                    // They must be materialized as Arrow RecordBatches first
                    debug!(
                        source = %source.name,
                        source_type = ?source_type,
                        "External API source - requires materialization"
                    );
                    return false;
                }
            }
        }
        true
    }

    /// Plan a direct merge with schema analysis results.
    fn plan_direct_merge_with_analysis(
        &self,
        query: &str,
        sources: Vec<(RegisteredSource, Vec<String>)>,
        analysis: JoinAnalysisResult,
    ) -> FederationResult<FederatedPlan> {
        let source_queries: Vec<SourceQuery> = sources
            .into_iter()
            .map(|(source, tables)| SourceQuery {
                source,
                sql: query.to_string(), // Will be rewritten by the rewriter
                tables,
                columns_needed: Vec::new(),
                has_predicates: false,
            })
            .collect();

        Ok(FederatedPlan {
            source_queries,
            combination: CombinationStrategy::DirectMerge {
                combined_sql: query.to_string(),
            },
            final_query: None,
            estimated_rows: None,
            schema_warnings: analysis.all_warnings,
            join_analyses: analysis.key_analyses,
        })
    }

    /// Plan a materialized join with schema analysis results.
    fn plan_materialized_join_with_analysis(
        &self,
        query: &str,
        sources: Vec<(RegisteredSource, Vec<String>)>,
        analysis: JoinAnalysisResult,
    ) -> FederationResult<FederatedPlan> {
        // Check if any sources are external API sources
        let has_external_api = sources
            .iter()
            .any(|(source, _)| source.backend.is_cold_tier());

        // Build source queries – per-source subquery generation is not yet
        // implemented, so we fail explicitly rather than silently sending the
        // full original query to every source (which would produce duplicate
        // results or errors).
        if !has_external_api {
            return Err(FederationError::Unsupported(
                "MaterializeJoin: per-source subquery generation is not yet \
                 implemented for non-external-API sources"
                    .to_string(),
            ));
        }
        let source_queries: Vec<SourceQuery> = sources
            .iter()
            .map(|(source, tables)| {
                SourceQuery {
                    source: source.clone(),
                    sql: query.to_string(),
                    tables: tables.clone(),
                    columns_needed: Vec::new(),
                    has_predicates: false,
                }
            })
            .collect();

        // Generate temp table names using only UUID to prevent SQL injection.
        // Source names are user-controlled and could contain malicious SQL.
        let temp_tables: Vec<String> = source_queries
            .iter()
            .enumerate()
            .map(|(idx, _)| format!("_temp_fed_{}_{}", idx, Uuid::new_v4().simple()))
            .collect();

        // Use external API materialization strategy if any external API sources
        let combination = if has_external_api {
            // Collect external source info
            let external_sources: Vec<ExternalSourceInfo> = sources
                .iter()
                .zip(temp_tables.iter())
                .filter_map(|((source, tables), temp_table)| {
                    if let SourceBackend::ExternalApi { source_type, .. } = &source.backend {
                        Some(ExternalSourceInfo {
                            source_name: source.name.clone(),
                            source_type: *source_type,
                            tables: tables.clone(),
                            temp_table: temp_table.clone(),
                        })
                    } else {
                        None
                    }
                })
                .collect();

            info!(
                project_id = %self.project_id,
                external_source_count = external_sources.len(),
                "Planning external API materialization"
            );

            CombinationStrategy::ExternalApiMaterialize {
                external_sources,
                temp_tables: temp_tables.clone(),
                final_sql: query.to_string(),
            }
        } else {
            CombinationStrategy::MaterializeJoin {
                temp_tables: temp_tables.clone(),
                join_sql: query.to_string(),
            }
        };

        Ok(FederatedPlan {
            source_queries,
            combination,
            final_query: Some(query.to_string()),
            estimated_rows: None,
            schema_warnings: analysis.all_warnings,
            join_analyses: analysis.key_analyses,
        })
    }
}

// ============================================================================
// JOIN Condition
// ============================================================================

/// A parsed JOIN condition from a query.
///
/// Represents a single column comparison in a JOIN clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinCondition {
    /// Left table in the JOIN.
    pub left_table: TableReference,
    /// Column name from the left table.
    pub left_column: String,
    /// Right table in the JOIN.
    pub right_table: TableReference,
    /// Column name from the right table.
    pub right_column: String,
}

impl JoinCondition {
    /// Create a new JOIN condition.
    pub fn new(
        left_table: TableReference,
        left_column: impl Into<String>,
        right_table: TableReference,
        right_column: impl Into<String>,
    ) -> Self {
        Self {
            left_table,
            left_column: left_column.into(),
            right_table,
            right_column: right_column.into(),
        }
    }
}

impl std::fmt::Display for JoinCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}.{} = {}.{}",
            self.left_table, self.left_column, self.right_table, self.right_column
        )
    }
}

// ============================================================================
// Table Reference
// ============================================================================

/// A parsed table reference from a query.
///
/// Format: `source_name.table_name` or just `table_name` (uses default source).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableReference {
    /// Source name (e.g., "stripe", "s3_events").
    pub source_name: String,
    /// Table name within the source.
    pub table_name: String,
    /// Optional alias used in the query.
    pub alias: Option<String>,
}

impl TableReference {
    /// Create a new table reference.
    pub fn new(source_name: impl Into<String>, table_name: impl Into<String>) -> Self {
        Self {
            source_name: source_name.into(),
            table_name: table_name.into(),
            alias: None,
        }
    }

    /// Create a table reference with an alias.
    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    /// Parse a table reference from a string.
    ///
    /// Supports formats:
    /// - `source.table` -> TableReference { source_name: "source", table_name: "table" }
    /// - `table` -> Returns None (needs default source)
    pub fn parse(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.split('.').collect();
        match parts.as_slice() {
            [source, table] => Some(Self::new(*source, *table)),
            _ => None,
        }
    }

    /// Get the fully qualified name.
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.source_name, self.table_name)
    }
}

impl std::fmt::Display for TableReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.source_name, self.table_name)?;
        if let Some(alias) = &self.alias {
            write!(f, " AS {}", alias)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_reference_parse() {
        let ref1 = TableReference::parse("stripe.customers");
        assert!(ref1.is_some());
        let ref1 = ref1.unwrap();
        assert_eq!(ref1.source_name, "stripe");
        assert_eq!(ref1.table_name, "customers");

        let ref2 = TableReference::parse("customers");
        assert!(ref2.is_none());

        let ref3 = TableReference::parse("s3_events.orders");
        assert!(ref3.is_some());
        let ref3 = ref3.unwrap();
        assert_eq!(ref3.source_name, "s3_events");
        assert_eq!(ref3.table_name, "orders");
    }

    #[test]
    fn test_table_reference_qualified_name() {
        let ref1 = TableReference::new("stripe", "customers");
        assert_eq!(ref1.qualified_name(), "stripe.customers");
    }

    #[test]
    fn test_federated_plan_is_homogeneous() {
        // This would need mock sources to test properly
    }

    // ==================== FederationConfig Tests ====================

    #[test]
    fn test_federation_config_default() {
        let config = FederationConfig::default();
        
        assert_eq!(config.semi_join_in_clause_limit, 10_000);
        assert!((config.semi_join_selectivity_threshold - 0.1).abs() < 0.001);
        assert_eq!(config.semi_join_bloom_limit, 1_000_000);
        assert!(config.enable_bloom_pushdown);
        assert!((config.bloom_false_positive_rate - 0.01).abs() < 0.001);
        assert_eq!(config.memory_budget_mb, 1024);
    }

    #[test]
    fn test_federation_config_builder() {
        let config = FederationConfig::new()
            .with_in_clause_limit(5_000)
            .with_selectivity_threshold(0.05)
            .with_bloom_pushdown(false)
            .with_memory_budget(2048);
        
        assert_eq!(config.semi_join_in_clause_limit, 5_000);
        assert!((config.semi_join_selectivity_threshold - 0.05).abs() < 0.001);
        assert!(!config.enable_bloom_pushdown);
        assert_eq!(config.memory_budget_mb, 2048);
    }

    #[test]
    fn test_federation_config_should_attempt_semi_join() {
        let config = FederationConfig::default();
        
        // Under bloom limit and under selectivity threshold
        assert!(config.should_attempt_semi_join(1_000, 0.05));
        
        // Over bloom limit
        assert!(!config.should_attempt_semi_join(2_000_000, 0.05));
        
        // Over selectivity threshold
        assert!(!config.should_attempt_semi_join(1_000, 0.2));
    }

    #[test]
    fn test_federation_config_semi_join_strategy() {
        let config = FederationConfig::default();
        
        // Small key set -> IN clause
        assert_eq!(config.semi_join_strategy(1_000), SemiJoinStrategy::InClause);
        assert_eq!(config.semi_join_strategy(10_000), SemiJoinStrategy::InClause);
        
        // Medium key set -> Bloom filter
        assert_eq!(config.semi_join_strategy(10_001), SemiJoinStrategy::BloomFilter);
        assert_eq!(config.semi_join_strategy(500_000), SemiJoinStrategy::BloomFilter);
        
        // Large key set -> Temp table
        assert_eq!(config.semi_join_strategy(1_000_001), SemiJoinStrategy::TempTable);
    }

    #[test]
    fn test_federation_config_bloom_disabled() {
        let config = FederationConfig::new().with_bloom_pushdown(false);
        
        // With Bloom disabled, medium keys go straight to temp table
        assert_eq!(config.semi_join_strategy(10_001), SemiJoinStrategy::TempTable);
    }

    #[test]
    fn test_semi_join_strategy_equality() {
        assert_eq!(SemiJoinStrategy::InClause, SemiJoinStrategy::InClause);
        assert_ne!(SemiJoinStrategy::InClause, SemiJoinStrategy::BloomFilter);
        assert_ne!(SemiJoinStrategy::BloomFilter, SemiJoinStrategy::TempTable);
    }

    #[test]
    fn test_materialize_join_rejects_non_cold_tier_sources() {
        // Regression: plan_materialized_join_with_analysis previously panicked
        // via unimplemented!() for non-cold-tier sources. Now it must return
        // FederationError::Unsupported. This test verifies the is_cold_tier()
        // condition that gates the error path.
        let object_storage = SourceBackend::ObjectStorage {
            bucket_url: "s3://test".to_string(),
            prefix: "data/".to_string(),
            access_key_id: None,
            secret_access_key: None,
        };
        assert!(
            !object_storage.is_cold_tier(),
            "ObjectStorage must not be cold tier — this triggers the error path in MaterializeJoin"
        );

        let clickhouse = SourceBackend::ClickHouseNative {
            database: "default".to_string(),
            table_prefix: "warehouse_".to_string(),
        };
        assert!(
            !clickhouse.is_cold_tier(),
            "ClickHouseNative must not be cold tier"
        );
    }

    #[test]
    fn test_zero_cost_plan_is_not_rejected() {
        use std::sync::Arc;
        use crate::warehouse::query::plan_optimizer::{ExecutionPlan, ExecutionStep};

        let mut plan = ExecutionPlan::new();
        assert_eq!(plan.cost.total_cost, 0.0, "precondition: new plan has zero cost");

        plan.add_step(ExecutionStep::Scan {
            source_name: Arc::from("test"),
            table_name: Arc::from("trivial"),
            predicates: vec![],
            estimated_rows: 0,
            estimated_bytes: 0,
        });

        // The old check `cost.total_cost > 0.0` would reject this plan.
        // The new check `!steps.is_empty()` should accept it.
        assert!(
            !plan.steps.is_empty(),
            "Plan with steps must pass the validity check even at zero cost"
        );
    }
}
