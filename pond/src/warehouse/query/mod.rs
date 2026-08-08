//! Query layer for the data warehouse.
//!
//! Handles SQL parsing, table rewriting, cost estimation, query execution, caching,
//! schema reconciliation, and routing based on storage type.

pub mod parsed_query;
pub mod federated_query;
pub mod bloom_pushdown;
pub mod cache;
pub mod cost_estimator;
pub mod cost_model;
pub mod executor;
pub mod explain;
pub mod federation;
pub mod federation_executor;
pub mod join_normalize;
pub mod limiter;
pub mod plan_optimizer;
pub mod predicate_pushdown;
pub mod rewriter;
pub mod router;
pub mod schema_reconciliation;
pub mod semi_join;
pub mod source_capabilities;
pub mod materializer;

pub use cost_model::{
    BuildSide, ColumnFilterCapability, CostModel, FilterOperation, ParallelismLevel, QueryCost,
    RateLimitInfo, SourceAccessProfile, SourceCapabilities, ValueTransform,
};
pub use source_capabilities::SourceCapabilityMatrix;
pub use federation::{
    CombinationStrategy, FederatedPlan, FederationConfig,
    FederationError, FederationPlanner, JoinCondition, MongoDBSourceInfo,
    SemiJoinStrategy, TableReference,
};
pub use plan_optimizer::{
    analyze_semi_join, should_use_semi_join, ExecutionPlan, ExecutionStep, JoinInfo, JoinType,
    PlanOptimizer, SemiJoinAnalysis, SemiJoinThresholds, TableInfo,
};
pub use predicate_pushdown::{
    EstimatedImpact, FilePredicate, FilePredicateType, Predicate, PredicatePushdown,
    PredicateSplitter, PredicateTranslation, PushdownStats, PushdownWarning,
    PushdownWarningReason, PushResult, SourcePredicateAnalysis, SourceQueryWithFilters,
    TranslatedPredicate,
};
pub use schema_reconciliation::{
    CaseSensitivity, IdentifierNormalizer, JoinAnalysisResult, JoinKeyAnalysis,
    JoinKeyCompatibility, NullSemanticsRegistry, SchemaReconciler, SchemaWarning,
};
pub use bloom_pushdown::{BloomFilter, BloomFilterPushdown, FilterStrategy};
pub use join_normalize::normalize_cross_joins;
pub use federation_executor::{ConnectorRegistry, FederationExecutor, FederationExecutorError};
pub use semi_join::{SemiJoinError, SemiJoinExecutor};
pub use cache::*;
pub use cost_estimator::*;
pub use executor::*;
pub use explain::*;
pub use federated_query::{
    FederatedQueryExecutor, FederatedQueryError, PostgresSourceConfig, MySqlSourceConfig,
    RegisteredSourceInfo, create_executor_from_configs, create_executor_from_configs_with_tiers,
};
pub use limiter::*;
pub use parsed_query::ParsedQuery;
pub use rewriter::*;
pub use router::*;
