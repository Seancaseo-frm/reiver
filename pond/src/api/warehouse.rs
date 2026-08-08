//! Data Warehouse REST API
//!
//! Provides endpoints for managing warehouse sources, syncs, and queries.
//!
//! Authentication is handled by the website gateway. Pond receives
//! pre-authenticated requests with the caller's identity in `X-User-Id`.

use ahash::AHashMap;
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{delete, get, patch, post, put},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::sync::Arc;
use uuid::Uuid;

use crate::app_state::PondState;
use crate::error::{AppError, Result};
use crate::kafka::SyncJobKafkaMessage;
use crate::warehouse::metrics::{EstimationAccuracy, QueryMetrics, SlowQueryAnalyzer};
use crate::warehouse::query::cost_estimator::{QueryCostEstimate, QueryCostEstimator};
use crate::warehouse::query::explain::{QueryExplain, QueryExplainer};
use crate::warehouse::sources::types::{StorageTier, StorageTierPolicy};
use crate::warehouse::table_formats::detect_table_format;
use crate::warehouse::types::{JobStatus, JobType, SourceType, SyncInterval, TableFormat};

/// Validate that a source type is `external_parquet`, returning an error otherwise.
fn ensure_external_parquet(source_type: &str) -> Result<()> {
    if source_type != SourceType::ExternalParquet.to_string() {
        return Err(AppError::Validation(
            "This endpoint is only for external Parquet sources".to_string(),
        ));
    }
    Ok(())
}

/// Extract the authenticated user ID from the trusted `X-User-Id` header.
///
/// The website gateway validates the JWT and project access before
/// forwarding the request here with this header. Pond trusts it.
fn extract_user_id(headers: &HeaderMap) -> Result<Uuid> {
    headers
        .get("X-User-Id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| AppError::Auth("Missing or invalid X-User-Id header".to_string()))
}

/// Find an active (pending or running) job for a source.
///
/// If `job_types` is `Some`, only matches jobs whose `job_type` is in the
/// given list. If `None`, matches any job type.
#[tracing::instrument(name = "warehouse.internal.find_active_job", skip_all)]
async fn find_active_job(
    db: &crate::db::DbPool,
    source_id: Uuid,
    job_types: Option<&[&str]>,
) -> Result<Option<Uuid>> {
    let job_id: Option<Uuid> = match job_types {
        Some(types) => {
            let placeholders: Vec<String> = types
                .iter()
                .enumerate()
                .map(|(i, _)| format!("${}", i + 2))
                .collect();
            let query = format!(
                "SELECT id FROM warehouse_jobs \
                 WHERE source_id = $1 \
                 AND job_type IN ({}) \
                 AND status IN ('pending', 'running') \
                 ORDER BY scheduled_at DESC \
                 LIMIT 1",
                placeholders.join(", ")
            );
            let mut q = sqlx::query_scalar(&query).bind(source_id);
            for jt in types {
                q = q.bind(*jt);
            }
            q.fetch_optional(db).await?
        }
        None => {
            sqlx::query_scalar(
                "SELECT id FROM warehouse_jobs \
                 WHERE source_id = $1 \
                 AND status IN ('pending', 'running') \
                 ORDER BY scheduled_at DESC \
                 LIMIT 1",
            )
            .bind(source_id)
            .fetch_optional(db)
            .await?
        }
    };
    Ok(job_id)
}

/// Convert a query limiter error into an API-facing error.
fn convert_limiter_error(e: crate::warehouse::query::limiter::LimiterError) -> AppError {
    match e {
        crate::warehouse::query::limiter::LimiterError::QueueFull { queued, max, .. } => {
            AppError::BadRequest(format!(
                "Too many concurrent queries for this project ({}/{}). Please wait and try again.",
                queued, max
            ))
        }
        crate::warehouse::query::limiter::LimiterError::Shutdown => {
            AppError::Internal(anyhow::anyhow!("Query service is shutting down"))
        }
    }
}

/// Convert a query executor error into an API-facing error.
fn convert_executor_error(e: crate::warehouse::query::executor::ExecutorError) -> AppError {
    match e {
        crate::warehouse::query::executor::ExecutorError::Timeout(secs) => {
            AppError::BadRequest(format!("Query timed out after {} seconds", secs))
        }
        crate::warehouse::query::executor::ExecutorError::Execution(msg) => {
            AppError::BadRequest(format!("Query execution failed: {}", msg))
        }
        other => AppError::Internal(anyhow::anyhow!("Query error: {}", other)),
    }
}

/// Convert a streaming/block error into an API-facing error.
///
/// ClickHouse timeout exceptions arrive during block streaming and need to be
/// surfaced as user-visible messages rather than hidden behind "internal error".
fn convert_stream_error(e: impl std::fmt::Display) -> AppError {
    let msg = e.to_string();
    if msg.contains("Timeout exceeded") {
        AppError::BadRequest("Query timed out. Try narrowing your query with filters or a smaller time range.".to_string())
    } else if msg.contains("Memory limit") || msg.contains("MEMORY_LIMIT_EXCEEDED") {
        AppError::BadRequest("Query exceeded the memory limit. Try selecting fewer columns or adding filters.".to_string())
    } else {
        AppError::Internal(anyhow::anyhow!("Block stream error: {}", msg))
    }
}

/// Convert a table rewrite/access error into an API-facing error.
fn convert_rewrite_error(e: crate::warehouse::query::rewriter::RewriteError) -> AppError {
    match e {
        crate::warehouse::query::rewriter::RewriteError::AccessDenied { table, .. } => {
            AppError::Forbidden(format!("Access denied to table: {}", table))
        }
        other => AppError::BadRequest(format!("Query validation error: {}", other)),
    }
}

/// Create warehouse routes.
/// All routes are nested under /projects/:project_id/warehouse
pub fn routes() -> Router<Arc<PondState>> {
    Router::new()
        // Sources
        .route(
            "/projects/{project_id}/warehouse/sources",
            get(list_sources).post(create_source),
        )
        .route(
            "/projects/{project_id}/warehouse/sources/test",
            post(test_source_connection),
        )
        .route(
            "/projects/{project_id}/warehouse/sources/{source_id}",
            get(get_source).put(update_source).delete(delete_source),
        )
        .route(
            "/projects/{project_id}/warehouse/sources/{source_id}/sync",
            post(trigger_sync),
        )
        .route(
            "/projects/{project_id}/warehouse/sources/{source_id}/tables",
            get(list_tables),
        )
        // Warm backing
        .route(
            "/projects/{project_id}/warehouse/sources/{source_id}/backing",
            post(create_warm_backing),
        )
        // Storage tier transition endpoints
        .route(
            "/projects/{project_id}/warehouse/sources/{source_id}/upgrade",
            post(upgrade_source),
        )
        .route(
            "/projects/{project_id}/warehouse/sources/{source_id}/downgrade",
            post(downgrade_source),
        )
        .route(
            "/projects/{project_id}/warehouse/sources/{source_id}/status",
            get(get_source_status),
        )
        .route(
            "/projects/{project_id}/warehouse/sources/{source_id}/sync-interval",
            put(set_sync_interval),
        )
        // External Sources (customer Parquet files)
        .route(
            "/projects/{project_id}/warehouse/sources/external",
            post(create_external_source),
        )
        .route(
            "/projects/{project_id}/warehouse/sources/{source_id}/config",
            get(get_external_config).put(update_external_config),
        )
        .route(
            "/projects/{project_id}/warehouse/sources/{source_id}/partitions",
            get(list_partitions),
        )
        .route(
            "/projects/{project_id}/warehouse/sources/{source_id}/detect-format",
            post(detect_format),
        )
        // AI Config Generation
        .route(
            "/projects/{project_id}/warehouse/sources/{source_id}/analyze",
            post(analyze_source),
        )
        .route(
            "/projects/{project_id}/warehouse/sources/{source_id}/apply-config",
            post(apply_config),
        )
        // Query
        .route(
            "/projects/{project_id}/warehouse/query",
            post(execute_query),
        )
        .route(
            "/projects/{project_id}/warehouse/query/stream",
            post(execute_query_stream),
        )
        .route(
            "/projects/{project_id}/warehouse/query/estimate",
            post(estimate_query),
        )
        .route(
            "/projects/{project_id}/warehouse/query/explain",
            post(explain_query),
        )
        .route(
            "/projects/{project_id}/warehouse/query/natural-language",
            post(execute_nl_query_handler),
        )
        .route(
            "/projects/{project_id}/warehouse/query/natural-language/suggestions",
            get(nl_query_suggestions),
        )
        // Full-text search
        .route(
            "/projects/{project_id}/warehouse/search",
            post(search_handler),
        )
        // Freshness
        .route(
            "/projects/{project_id}/warehouse/tables/freshness",
            get(get_freshness),
        )
        // Autocomplete
        .route(
            "/projects/{project_id}/warehouse/autocomplete",
            get(autocomplete),
        )
        // Views
        .route(
            "/projects/{project_id}/warehouse/views",
            get(list_views).post(create_view),
        )
        .route(
            "/projects/{project_id}/warehouse/views/{view_id}",
            get(get_view).delete(delete_view),
        )
        // Usage
        .route(
            "/projects/{project_id}/warehouse/usage/summary",
            get(usage_summary),
        )
        .route(
            "/projects/{project_id}/warehouse/usage/by-query",
            get(usage_by_query),
        )
        .route(
            "/projects/{project_id}/warehouse/budgets",
            get(get_budget).post(set_budget),
        )
        // Compliance / PII
        .route(
            "/projects/{project_id}/warehouse/compliance/findings",
            get(list_pii_findings),
        )
        .route(
            "/projects/{project_id}/warehouse/compliance/findings/{finding_id}",
            patch(update_pii_finding),
        )
        .route(
            "/projects/{project_id}/warehouse/compliance/summary",
            get(pii_compliance_summary),
        )
        // Column configuration (full-text search)
        .route(
            "/projects/{project_id}/warehouse/catalog/{source_name}/{table_name}/fulltext-columns",
            get(get_fulltext_columns).put(set_fulltext_columns),
        )
        // Mutation churn analysis
        .route(
            "/projects/{project_id}/warehouse/mutation-churn",
            get(get_mutation_churn),
        )
        // Blockchain sources
        .route(
            "/projects/{project_id}/warehouse/blockchain/{chain}",
            post(enable_blockchain),
        )
        // Derived tables (CTAS / materialized views)
        //
        // Authorization: project-level access is enforced by the website gateway
        // middleware, which validates the caller's session and sets the trusted
        // X-User-Id header. This is consistent with all other warehouse endpoints;
        // Pond does not perform in-handler permission checks.
        .route(
            "/projects/{project_id}/warehouse/derived-tables",
            get(list_derived_tables).post(create_derived_table),
        )
        .route(
            "/projects/{project_id}/warehouse/derived-tables/{derived_id}",
            get(get_derived_table).delete(delete_derived_table),
        )
        .route(
            "/projects/{project_id}/warehouse/derived-tables/{derived_id}/refresh",
            post(refresh_derived_table),
        )
        .route(
            "/projects/{project_id}/warehouse/derived-tables/{derived_id}/append",
            post(append_derived_table),
        )
        .route(
            "/projects/{project_id}/warehouse/derived-tables/{derived_id}/compact",
            post(compact_derived_table),
        )
        .route(
            "/projects/{project_id}/warehouse/derived-tables/{derived_id}/schedule",
            put(set_derived_table_schedule),
        )
        // Pipelines (transformation DAGs)
        .route(
            "/projects/{project_id}/warehouse/pipelines",
            get(list_pipelines).post(create_pipeline),
        )
        .route(
            "/projects/{project_id}/warehouse/pipelines/{pipeline_id}",
            get(get_pipeline)
                .put(update_pipeline)
                .delete(delete_pipeline),
        )
        .route(
            "/projects/{project_id}/warehouse/pipelines/{pipeline_id}/run",
            post(trigger_pipeline_run),
        )
        .route(
            "/projects/{project_id}/warehouse/pipelines/{pipeline_id}/runs",
            get(list_pipeline_runs),
        )
        .route(
            "/projects/{project_id}/warehouse/pipelines/{pipeline_id}/subscriptions",
            get(list_pipeline_subscriptions).post(create_pipeline_subscription),
        )
        .route(
            "/projects/{project_id}/warehouse/subscriptions/{subscription_id}",
            delete(delete_pipeline_subscription),
        )
        // Pipelines legacy visualization
        .route(
            "/projects/{project_id}/warehouse/pipelines/graph",
            get(get_pipelines),
        )
        // UDFs
        .route(
            "/projects/{project_id}/warehouse/udfs",
            get(list_udfs).post(create_udf),
        )
        .route(
            "/projects/{project_id}/warehouse/udfs/{udf_name}",
            get(get_udf).delete(delete_udf),
        )
        // Jobs (data movement UDFs)
        .route("/projects/{project_id}/warehouse/jobs", post(create_job))
        .route(
            "/projects/{project_id}/warehouse/jobs/{job_name}",
            get(get_job).patch(update_job),
        )
        .route(
            "/projects/{project_id}/warehouse/jobs/{job_name}/run",
            post(trigger_job_run),
        )
        .route(
            "/projects/{project_id}/warehouse/jobs/{job_name}/runs",
            get(list_job_runs),
        )
        // Connector type catalog (static metadata for the UI)
        .route(
            "/projects/{project_id}/warehouse/connector-types",
            get(list_connector_types),
        )
}

// ===== Connector type catalog =====

async fn list_connector_types() -> Json<Vec<crate::warehouse::connectors::catalog::ConnectorMeta>> {
    Json(crate::warehouse::connectors::catalog::connector_catalog())
}

// ===== Request/Response Types =====

#[derive(Debug, Deserialize)]
pub struct CreateSourceRequest {
    pub name: String,
    pub source_type: SourceType,
    pub config: serde_json::Value,
    /// Storage tier: Cold (federated query), Warm (R2/S3), or Hot (ClickHouse)
    #[serde(default)]
    pub tier: StorageTier,
    /// Sync interval - only used for warm/hot tiers
    pub sync_interval: Option<SyncInterval>,
    /// Storage tier lifecycle policy (defaults to Fixed).
    #[serde(default)]
    pub storage_tier_policy: StorageTierPolicy,
    /// Sync scope: "full" (default) or "time_based"
    #[serde(default = "default_sync_scope")]
    pub sync_scope: String,
    /// For time_based sync scope: only sync data older than this many days
    pub sync_scope_older_than_days: Option<u32>,
}

fn default_sync_scope() -> String {
    "full".to_string()
}

#[derive(Debug, Serialize)]
pub struct SourceResponse {
    pub id: Uuid,
    pub name: String,
    pub source_type: SourceType,
    pub tier: StorageTier,
    pub enabled: bool,
    pub sync_interval: Option<SyncInterval>,
    /// Storage tier lifecycle policy.
    pub storage_tier_policy: StorageTierPolicy,
    /// Sync scope: "full" or "time_based"
    pub sync_scope: String,
    /// For time_based sync scope: only sync data older than this many days
    pub sync_scope_older_than_days: Option<u32>,
    /// When data was last synced to warm tier (R2/Parquet)
    pub warm_at: Option<DateTime<Utc>>,
    /// When data was last synced to hot tier (ClickHouse)
    pub hot_at: Option<DateTime<Utc>>,
    /// Last sync timestamp
    pub last_sync_at: Option<DateTime<Utc>>,
    /// Whether there's an active sync job for this source
    pub sync_in_progress: bool,
    /// Type of current job if any
    pub current_job_type: Option<JobType>,
    /// Current job ID if any
    pub current_job_id: Option<Uuid>,
    /// Whether this source is a global/managed source (e.g. blockchain).
    pub is_global: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub sql: String,
    pub limit: Option<u32>,
}

/// Optional query parameters for format negotiation on query endpoints.
#[derive(Debug, Deserialize, Default)]
pub struct QueryFormatParams {
    pub format: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub row_count: usize,
    pub execution_time_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Serialize)]
pub struct TableFreshness {
    pub table_name: String,
    pub source_name: String,
    pub last_sync_at: Option<DateTime<Utc>>,
    pub next_sync_at: Option<DateTime<Utc>>,
    pub staleness_minutes: Option<i64>,
    pub staleness_level: String,
}

#[derive(Debug, Deserialize)]
pub struct AutocompleteRequest {
    pub prefix: String,
}

#[derive(Debug, Serialize)]
pub struct AutocompleteResponse {
    pub suggestions: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreateViewRequest {
    pub name: String,
    pub sql: String,
    pub description: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ViewResponse {
    pub id: Uuid,
    pub name: String,
    pub sql: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct UsageSummary {
    pub period: String,
    pub total_bytes_scanned: u64,
    pub total_queries: u64,
    pub cache_hit_rate: f64,
    pub avg_execution_time_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct QueryUsage {
    pub query_id: Uuid,
    pub sql_preview: String,
    pub bytes_scanned: u64,
    pub execution_count: u64,
    pub last_executed: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct SetBudgetRequest {
    pub monthly_bytes_limit: Option<i64>,
    pub alert_threshold_percent: Option<i32>,
}

#[derive(Debug, Serialize)]
pub struct BudgetResponse {
    pub monthly_bytes_limit: Option<i64>,
    pub alert_threshold_percent: i32,
    pub current_usage_bytes: i64,
    pub usage_percent: f64,
}

/// Request to test a source connection before saving.
#[derive(Debug, Deserialize)]
pub struct TestConnectionRequest {
    /// Source type (postgresql, mysql, mongodb, etc.)
    pub source_type: SourceType,
    /// Connection configuration
    pub config: serde_json::Value,
}

/// Response from testing a source connection.
#[derive(Debug, Serialize)]
pub struct TestConnectionResponse {
    /// Whether the connection was successful
    pub success: bool,
    /// Error message if connection failed
    pub error: Option<String>,
    /// Connection latency in milliseconds
    pub latency_ms: Option<u64>,
    /// Discovered tables (if connection successful)
    pub tables: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
pub struct SyncTriggerResponse {
    pub job_id: Uuid,
    pub status: JobStatus,
}

// ===== Storage Tier Types =====

/// Request for tier transition (upgrade or downgrade).
#[derive(Debug, Deserialize)]
pub struct TierTransitionRequest {
    pub target_tier: StorageTier,
}

/// Response from tier transition operations.
#[derive(Debug, Serialize)]
pub struct TierTransitionResponse {
    /// Job ID for tracking the transition progress
    pub job_id: Uuid,
    /// Current status
    pub status: JobStatus,
    /// Target tier after completion
    pub target_tier: StorageTier,
}

/// Response from source status endpoint.
#[derive(Debug, Serialize)]
pub struct SourceStatusResponse {
    /// Source ID
    pub id: Uuid,
    /// Source name
    pub name: String,
    /// Current storage tier
    pub tier: StorageTier,
    /// When data was last synced to Parquet (for warm/hot)
    pub warm_at: Option<DateTime<Utc>>,
    /// When data was last synced to ClickHouse (for hot)
    pub hot_at: Option<DateTime<Utc>>,
    /// Last successful sync time
    pub last_sync_at: Option<DateTime<Utc>>,
    /// Storage used in bytes
    pub storage_bytes: i64,
    /// Sync interval
    pub sync_interval: Option<SyncInterval>,
    /// Whether there's an active sync job
    pub sync_in_progress: bool,
    /// Current sync job ID if any
    pub current_job_id: Option<Uuid>,
}

/// Request to set sync interval.
#[derive(Debug, Deserialize)]
pub struct SetSyncIntervalRequest {
    /// Sync interval, or null for manual
    pub interval: Option<SyncInterval>,
}

// ===== External Source Types =====

/// Request to create an external Parquet source.
#[derive(Debug, Deserialize)]
pub struct CreateExternalSourceRequest {
    /// Source name.
    pub name: String,
    /// S3/GCS/Azure bucket URL (e.g., "s3://bucket/prefix").
    pub bucket_url: String,
    /// Credentials for accessing the bucket.
    pub credentials: serde_json::Value,
    /// External source configuration.
    pub config: ExternalSourceConfigRequest,
}

/// External source configuration from API request.
#[derive(Debug, Deserialize)]
pub struct ExternalSourceConfigRequest {
    /// Table format (auto, raw_parquet, iceberg, delta_lake).
    #[serde(default)]
    pub table_format: String,
    /// Columns to index with optional hints.
    #[serde(default)]
    pub index_columns: Vec<IndexColumnRequest>,
    /// Column containing timestamps for partitioning.
    pub time_column: Option<String>,
    /// Path pattern for discovering partitions.
    pub partition_pattern: Option<String>,
    /// Mutability configuration.
    #[serde(default)]
    pub mutability: MutabilityRequest,
    /// Refresh configuration.
    #[serde(default)]
    pub refresh: RefreshRequest,
}

/// Index column configuration from API request.
#[derive(Debug, Deserialize)]
pub struct IndexColumnRequest {
    pub name: String,
    /// Cardinality hint: very_low, low, medium, high, very_high.
    pub cardinality: Option<String>,
    /// Force strategy: fst, xor_filter, skip, auto.
    pub force_strategy: Option<String>,
}

/// Mutability configuration from API request.
#[derive(Debug, Deserialize, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MutabilityRequest {
    AllImmutable,
    AllMutable,
    RollingWindow {
        window: u32,
        unit: String,
    },
    FileAge {
        hours: u32,
    },
    #[default]
    #[serde(other)]
    Default,
}

/// Refresh configuration from API request.
#[derive(Debug, Deserialize, Default)]
pub struct RefreshRequest {
    #[serde(default)]
    pub mutable_refresh: String,
    #[serde(default = "default_true_api")]
    pub auto_discover: bool,
}

fn default_true_api() -> bool {
    true
}

/// Response for external source configuration.
#[derive(Debug, Serialize)]
pub struct ExternalSourceConfigResponse {
    pub table_format: String,
    pub detected_format: Option<String>,
    pub index_columns: Vec<IndexColumnResponse>,
    pub time_column: Option<String>,
    pub partition_pattern: Option<String>,
    pub mutability: MutabilityResponse,
    pub refresh: RefreshResponse,
}

/// Index column response.
#[derive(Debug, Serialize)]
pub struct IndexColumnResponse {
    pub name: String,
    pub cardinality: Option<String>,
    pub force_strategy: Option<String>,
    pub actual_strategy: String,
}

/// Mutability response.
#[derive(Debug, Serialize)]
pub struct MutabilityResponse {
    #[serde(rename = "type")]
    pub strategy_type: String,
    pub window: Option<u32>,
    pub unit: Option<String>,
    pub hours: Option<u32>,
}

/// Refresh response.
#[derive(Debug, Serialize)]
pub struct RefreshResponse {
    pub mutable_refresh: String,
    pub auto_discover: bool,
}

/// Partition information.
#[derive(Debug, Serialize)]
pub struct PartitionInfo {
    pub partition_key: String,
    pub file_count: u32,
    pub is_mutable: bool,
    pub estimated_size_bytes: Option<u64>,
    pub last_modified: Option<DateTime<Utc>>,
}

/// Detected table format response.
#[derive(Debug, Serialize)]
pub struct DetectFormatResponse {
    pub detected_format: String,
    pub confidence: String,
    pub details: Option<String>,
}

// Re-export shared memory estimation function from warehouse utils
use crate::warehouse::utils::estimate_json_value_memory;

// ===== AI Config Types =====

/// Response for AI config analysis.
#[derive(Debug, Serialize)]
pub struct ConfigRecommendationResponse {
    /// The recommended configuration.
    pub config: ExternalSourceConfigResponse,
    /// Overall confidence score (0.0 to 1.0).
    pub confidence: f64,
    /// Explanations for each configuration decision.
    pub explanations: Vec<ConfigExplanationResponse>,
    /// Warnings or suggestions for the user.
    pub warnings: Vec<String>,
}

/// Explanation for a configuration decision.
#[derive(Debug, Serialize)]
pub struct ConfigExplanationResponse {
    /// Which configuration field this explains.
    pub field: String,
    /// Human-readable reason for the decision.
    pub reason: String,
    /// Confidence in this specific decision (0.0 to 1.0).
    pub confidence: f64,
}

/// Request to apply a recommended configuration.
#[derive(Debug, Deserialize)]
pub struct ApplyConfigRequest {
    /// The configuration to apply.
    pub config: ExternalSourceConfigRequest,
}

// ===== Handlers =====

/// List all connected sources for a project.
#[tracing::instrument(name = "warehouse.api.list_sources", skip(state), fields(project_id = %project_id), err(Display))]
async fn list_sources(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
) -> Result<Json<Vec<SourceResponse>>> {
    // Fetch sources with active job info via a LEFT JOIN
    let rows = sqlx::query(
        r#"SELECT 
            s.id, s.name, s.source_type, 
            COALESCE(s.tier, 'cold') as tier, 
            s.enabled, s.sync_interval, s.last_sync_at,
            s.warm_at, s.hot_at,
            COALESCE(s.storage_tier_policy, '{"type": "fixed"}'::jsonb) as storage_tier_policy,
            COALESCE(s.sync_scope, 'full') as sync_scope,
            s.sync_scope_older_than_days,
            s.global_source_id,
            s.created_at, s.updated_at,
            j.id as job_id, j.job_type
         FROM warehouse_sources s
         LEFT JOIN LATERAL (
            SELECT id, job_type FROM warehouse_jobs
            WHERE source_id = s.id AND status IN ('pending', 'running')
            ORDER BY scheduled_at DESC
            LIMIT 1
         ) j ON true
         WHERE s.project_id = $1 
         ORDER BY s.created_at DESC"#,
    )
    .bind(project_id)
    .fetch_all(&*state.db)
    .await?;

    let sources: Vec<SourceResponse> = rows
        .into_iter()
        .map(|row| {
            let job_id: Option<Uuid> = row.get("job_id");
            let job_type_str: Option<String> = row.get("job_type");
            let source_type_str: String = row.get("source_type");
            let tier_str: String = row.get("tier");
            let sync_interval_str: Option<String> = row.get("sync_interval");
            let storage_tier_policy_json: serde_json::Value = row.get("storage_tier_policy");
            let storage_tier_policy: StorageTierPolicy =
                serde_json::from_value(storage_tier_policy_json).unwrap_or_default();
            let sync_scope: String = row.get("sync_scope");
            let sync_scope_older_than_days: Option<i32> = row.get("sync_scope_older_than_days");
            let global_source_id: Option<Uuid> = row.get("global_source_id");
            SourceResponse {
                id: row.get("id"),
                name: row.get("name"),
                source_type: source_type_str
                    .parse()
                    .unwrap_or(SourceType::ExternalParquet),
                tier: tier_str.parse().unwrap_or_default(),
                enabled: row.get("enabled"),
                sync_interval: sync_interval_str.and_then(|s| s.parse().ok()),
                storage_tier_policy,
                sync_scope,
                sync_scope_older_than_days: sync_scope_older_than_days.map(|d| d as u32),
                warm_at: row.get("warm_at"),
                hot_at: row.get("hot_at"),
                last_sync_at: row.get("last_sync_at"),
                sync_in_progress: job_id.is_some(),
                current_job_type: job_type_str.and_then(|s| s.parse().ok()),
                current_job_id: job_id,
                is_global: global_source_id.is_some(),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            }
        })
        .collect();

    Ok(Json(sources))
}

/// Create a new source connection.
#[tracing::instrument(name = "warehouse.api.create_source", skip(state, req), fields(project_id = %project_id), err(Display))]
async fn create_source(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
    Json(req): Json<CreateSourceRequest>,
) -> Result<Json<SourceResponse>> {
    // Validate input
    validate_source_request(&req)?;

    // Blockchain sources always use managed warm tier
    // For non-blockchain sources, new sources start as cold
    let tier = if req.source_type.is_blockchain() {
        StorageTier::Warm
    } else {
        match req.tier {
            StorageTier::Cold => StorageTier::Cold,
            StorageTier::Warm | StorageTier::Hot => {
                return Err(AppError::Validation(
                    "Sources must be created in 'cold' tier. Use the /upgrade endpoint to upgrade."
                        .to_string(),
                ));
            }
        }
    };

    let id = Uuid::new_v4();
    let now = Utc::now();

    // SECURITY: Encrypt the config before storing
    // The config contains sensitive credentials (API keys, passwords, etc.)
    let config_json = serde_json::to_string(&req.config)
        .map_err(|e| AppError::Validation(format!("Invalid config JSON: {}", e)))?;
    let encrypted_config = state
        .encryptor
        .encrypt(&config_json)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to encrypt config: {}", e)))?;

    // Compute connection config hash for duplicate detection
    let connection_config_hash = compute_connection_hash(&req.config, &req.source_type);

    // Use a transaction to ensure atomicity
    let mut tx = state.db.begin().await?;

    // Check for duplicate connection
    let existing: Option<(Uuid,)> = sqlx::query_as(
        "SELECT id FROM warehouse_sources 
         WHERE project_id = $1 AND connection_config_hash = $2",
    )
    .bind(project_id)
    .bind(&connection_config_hash)
    .fetch_optional(&mut *tx)
    .await?;

    if let Some((existing_id,)) = existing {
        return Err(AppError::Validation(format!(
            "A source with the same connection already exists (id: {}). \
             The same database connection cannot be added twice.",
            existing_id
        )));
    }

    // Store encrypted config as JSON string in JSONB column
    let encrypted_config_json = serde_json::json!({ "encrypted": encrypted_config });

    let storage_tier_policy_json = serde_json::to_value(&req.storage_tier_policy)
        .unwrap_or_else(|_| serde_json::json!({"type": "fixed"}));

    // Validate sync scope
    let sync_scope = req.sync_scope.as_str();
    if sync_scope != "full" && sync_scope != "time_based" {
        return Err(AppError::Validation(format!(
            "Invalid sync_scope '{}'. Must be 'full' or 'time_based'.",
            sync_scope
        )));
    }
    if sync_scope == "time_based" && req.sync_scope_older_than_days.is_none() {
        return Err(AppError::Validation(
            "sync_scope_older_than_days is required when sync_scope is 'time_based'.".to_string(),
        ));
    }
    let sync_scope_older_than_days_i32: Option<i32> =
        req.sync_scope_older_than_days.map(|d| d as i32);

    // Determine the correct storage_type based on source type
    let storage_type = match req.source_type {
        SourceType::PostgreSQL
        | SourceType::MySQL
        | SourceType::MongoDB
        | SourceType::SqlServer
        | SourceType::SQLite
        | SourceType::Redshift
        | SourceType::Snowflake
        | SourceType::ClickHouse
        | SourceType::BigQuery => "external",
        _ => "object_storage",
    };

    sqlx::query(
        "INSERT INTO warehouse_sources (id, project_id, name, source_type, storage_type, config, tier, connection_config_hash, storage_tier_policy, sync_scope, sync_scope_older_than_days, enabled, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, true, $12, $12)"
    )
    .bind(id)
    .bind(project_id)
    .bind(&req.name)
    .bind(req.source_type.to_string())
    .bind(storage_type)
    .bind(&encrypted_config_json)
    .bind(tier.to_string())
    .bind(&connection_config_hash)
    .bind(&storage_tier_policy_json)
    .bind(sync_scope)
    .bind(sync_scope_older_than_days_i32)
    .bind(now)
    .execute(&mut *tx)
    .await?;

    // Store sync interval if provided
    if let Some(interval) = &req.sync_interval {
        sqlx::query("UPDATE warehouse_sources SET sync_interval = $1 WHERE id = $2")
            .bind(interval.to_string())
            .bind(id)
            .execute(&mut *tx)
            .await?;
    }

    tx.commit().await?;

    // For cold sources, register with ConnectorRegistryService
    // This is done after commit to avoid holding the transaction
    // Note: The connector will be registered lazily on first query if this fails
    if tier.is_cold() {
        if let Some(registry_service) = &state.connector_registry_service {
            // Build a RegisteredSource for registration
            // Note: In production, we would fetch the full source from DB
            // For now, we log if registration fails but don't fail the request
            tracing::debug!(
                source_id = %id,
                source_name = %req.name,
                "Cold source created, connector will be initialized on first query"
            );
        }
    }

    Ok(Json(SourceResponse {
        id,
        name: req.name,
        source_type: req.source_type,
        tier,
        enabled: true,
        sync_interval: req.sync_interval,
        storage_tier_policy: req.storage_tier_policy,
        sync_scope: req.sync_scope,
        sync_scope_older_than_days: req.sync_scope_older_than_days,
        warm_at: None,
        hot_at: None,
        last_sync_at: None,
        sync_in_progress: false,
        current_job_type: None,
        current_job_id: None,
        is_global: false,
        created_at: now,
        updated_at: now,
    }))
}

/// Create a warm backing source for a hot source.
///
/// Clones the hot source's configuration into a new `tier = 'warm'` source
/// linked via `backs_source_id`. The backing source gets its own independent
/// sync job (picked up automatically by the scheduler) and is invisible to
/// normal queries.
#[tracing::instrument(
    name = "warehouse.api.create_warm_backing",
    skip(state),
    fields(%project_id, %source_id),
)]
async fn create_warm_backing(
    State(state): State<Arc<PondState>>,
    Path((project_id, source_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<SourceResponse>> {
    let row = sqlx::query(
        r#"
        SELECT id, name, source_type, storage_type, config, tier,
               sync_interval, sync_scope, sync_scope_older_than_days,
               storage_tier_policy, supports_cdc, consistency_level,
               connection_config_hash, enabled
        FROM warehouse_sources
        WHERE id = $1 AND project_id = $2
        "#,
    )
    .bind(source_id)
    .bind(project_id)
    .fetch_optional(&*state.db)
    .await?
    .ok_or_else(|| AppError::NotFound("Source not found".to_string()))?;

    let tier_str: String = row.get("tier");
    if tier_str != "hot" {
        return Err(AppError::Validation(
            "Only hot sources can have a warm backing".to_string(),
        ));
    }

    let existing_backing: Option<(Uuid,)> =
        sqlx::query_as("SELECT id FROM warehouse_sources WHERE backs_source_id = $1")
            .bind(source_id)
            .fetch_optional(&*state.db)
            .await?;

    if let Some((existing_id,)) = existing_backing {
        return Err(AppError::Validation(format!(
            "This source already has a warm backing (id: {})",
            existing_id,
        )));
    }

    let backing_id = Uuid::new_v4();
    let now = Utc::now();
    let hot_name: String = row.get("name");
    let backing_name = format!("{}_backing", hot_name);

    let source_type_str: String = row.get("source_type");
    let storage_type: String = row.get("storage_type");
    let config: serde_json::Value = row.get("config");
    let sync_interval: Option<String> = row.get("sync_interval");
    let sync_scope: String = row.get("sync_scope");
    let sync_scope_older_than_days: Option<i32> = row.get("sync_scope_older_than_days");
    let storage_tier_policy: serde_json::Value = row.get("storage_tier_policy");
    let supports_cdc: bool = row.get("supports_cdc");
    let consistency_level: String = row.get("consistency_level");
    let connection_config_hash: Option<String> = row.get("connection_config_hash");
    let enabled: bool = row.get("enabled");

    sqlx::query(
        r#"
        INSERT INTO warehouse_sources (
            id, project_id, name, source_type, storage_type, config,
            tier, backs_source_id, sync_interval, sync_scope,
            sync_scope_older_than_days, storage_tier_policy,
            supports_cdc, consistency_level, connection_config_hash,
            enabled, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, 'warm', $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $16)
        "#,
    )
    .bind(backing_id)
    .bind(project_id)
    .bind(&backing_name)
    .bind(&source_type_str)
    .bind(&storage_type)
    .bind(&config)
    .bind(source_id)
    .bind(&sync_interval)
    .bind(&sync_scope)
    .bind(sync_scope_older_than_days)
    .bind(&storage_tier_policy)
    .bind(supports_cdc)
    .bind(&consistency_level)
    .bind(&connection_config_hash)
    .bind(enabled)
    .bind(now)
    .execute(&*state.db)
    .await?;

    state.table_cache_dirty.insert(project_id);

    let source_type: SourceType = source_type_str.parse().unwrap_or(SourceType::PostgreSQL);
    let storage_tier_policy: StorageTierPolicy =
        serde_json::from_value(storage_tier_policy).unwrap_or_default();
    let sync_interval_parsed: Option<SyncInterval> = sync_interval.and_then(|s| s.parse().ok());

    Ok(Json(SourceResponse {
        id: backing_id,
        name: backing_name,
        source_type,
        tier: StorageTier::Warm,
        enabled,
        sync_interval: sync_interval_parsed,
        storage_tier_policy,
        sync_scope,
        sync_scope_older_than_days: sync_scope_older_than_days.map(|d| d as u32),
        warm_at: None,
        hot_at: None,
        last_sync_at: None,
        sync_in_progress: false,
        current_job_type: None,
        current_job_id: None,
        is_global: false,
        created_at: now,
        updated_at: now,
    }))
}

/// Compute a hash of the connection config for duplicate detection.
/// This excludes credentials but includes connection-identifying fields.
fn compute_connection_hash(config: &serde_json::Value, source_type: &SourceType) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();

    // Hash source type
    source_type.to_string().hash(&mut hasher);

    // Extract connection-identifying fields based on source type
    let hash_input = match source_type {
        SourceType::PostgreSQL
        | SourceType::MySQL
        | SourceType::SqlServer
        | SourceType::Redshift => {
            format!(
                "{}:{}:{}",
                config.get("host").and_then(|v| v.as_str()).unwrap_or(""),
                config.get("port").and_then(|v| v.as_u64()).unwrap_or(0),
                config
                    .get("database")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
            )
        }
        SourceType::Snowflake => {
            format!(
                "{}:{}:{}",
                config.get("account").and_then(|v| v.as_str()).unwrap_or(""),
                config
                    .get("warehouse")
                    .and_then(|v| v.as_str())
                    .unwrap_or(""),
                config
                    .get("database")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
            )
        }
        SourceType::MongoDB => config
            .get("connection_string")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| {
                format!(
                    "{}:{}",
                    config.get("host").and_then(|v| v.as_str()).unwrap_or(""),
                    config.get("port").and_then(|v| v.as_u64()).unwrap_or(0)
                )
            }),
        SourceType::BigQuery => {
            format!(
                "{}:{}",
                config
                    .get("project_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or(""),
                config.get("dataset").and_then(|v| v.as_str()).unwrap_or("")
            )
        }
        SourceType::SQLite => config
            .get("database_path")
            .or_else(|| config.get("path"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        _ => {
            // For other types, hash a subset of the config
            config.to_string()
        }
    };

    hash_input.hash(&mut hasher);
    format!("{:x}", hasher.finish())
}

/// Test a source connection before saving.
///
/// This endpoint validates that the provided credentials can successfully
/// connect to the data source and lists available tables.
#[tracing::instrument(name = "warehouse.api.test_source_connection", skip(state, req), fields(project_id = %project_id), err(Display))]
async fn test_source_connection(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
    Json(req): Json<TestConnectionRequest>,
) -> Result<Json<TestConnectionResponse>> {
    let start = std::time::Instant::now();

    // Try to create a connector and validate credentials
    let result = test_connection_internal(&req.source_type, &req.config).await;

    let latency_ms = start.elapsed().as_millis() as u64;

    match result {
        Ok(tables) => Ok(Json(TestConnectionResponse {
            success: true,
            error: None,
            latency_ms: Some(latency_ms),
            tables: Some(tables),
        })),
        Err(e) => Ok(Json(TestConnectionResponse {
            success: false,
            error: Some(e),
            latency_ms: Some(latency_ms),
            tables: None,
        })),
    }
}

/// Build a PostgreSQL connection string from a JSON config.
///
/// Prefers a `connection_string` field if present. Otherwise builds from
/// `host`, `port`, `database`, `username`, `password` — returning `None` if
/// any required field (`host`, `database`, `username`) is missing.
fn build_postgres_connection_string(config: &serde_json::Value) -> String {
    config
        .get("connection_string")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .or_else(|| {
            let host = config.get("host").and_then(|v| v.as_str())?;
            let port = config.get("port").and_then(|v| v.as_u64()).unwrap_or(5432);
            let database = config.get("database").and_then(|v| v.as_str())?;
            let username = config.get("username").and_then(|v| v.as_str())?;
            let password = config
                .get("password")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            Some(format!(
                "postgresql://{}:{}@{}:{}/{}",
                username, password, host, port, database
            ))
        })
        .unwrap_or_else(|| {
            format!(
                "postgresql://{}:{}@{}:{}/{}",
                config
                    .get("username")
                    .and_then(|v| v.as_str())
                    .unwrap_or(""),
                config
                    .get("password")
                    .and_then(|v| v.as_str())
                    .unwrap_or(""),
                config
                    .get("host")
                    .and_then(|v| v.as_str())
                    .unwrap_or("localhost"),
                config.get("port").and_then(|v| v.as_u64()).unwrap_or(5432),
                config
                    .get("database")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
            )
        })
}

/// Internal function to test a connection based on source type.
#[tracing::instrument(name = "warehouse.internal.test_connection_internal", skip_all)]
async fn test_connection_internal(
    source_type: &SourceType,
    config: &serde_json::Value,
) -> std::result::Result<Vec<String>, String> {
    use crate::warehouse::connectors::databases::{
        BigQueryConfig, BigQueryConnector, ClickHouseConfig, ClickHouseConnector, SQLiteConfig,
        SQLiteConnector,
    };
    use crate::warehouse::connectors::postgres::{PostgresConfig, PostgresConnector};
    use crate::warehouse::connectors::{
        Connector, MongoDBConfig, MongoDBConnector, MySqlConfig, MySqlConnector, RedshiftConfig,
        RedshiftConnector, SnowflakeConfig, SnowflakeConnector, SqlServerConfig,
        SqlServerConnector,
    };

    match source_type {
        SourceType::PostgreSQL => {
            let connection_string = build_postgres_connection_string(config);

            let pg_config = PostgresConfig::new(connection_string);
            let connector = PostgresConnector::new(pg_config);

            connector
                .validate_credentials()
                .await
                .map_err(|e| format!("Connection failed: {}", e))?;

            let tables = connector
                .list_tables()
                .await
                .map_err(|e| format!("Failed to list tables: {}", e))?;

            Ok(tables.into_iter().map(|t| t.name).collect())
        }

        SourceType::MySQL => {
            let connection_string = format!(
                "mysql://{}:{}@{}:{}/{}",
                config
                    .get("username")
                    .and_then(|v| v.as_str())
                    .unwrap_or(""),
                config
                    .get("password")
                    .and_then(|v| v.as_str())
                    .unwrap_or(""),
                config
                    .get("host")
                    .and_then(|v| v.as_str())
                    .unwrap_or("localhost"),
                config.get("port").and_then(|v| v.as_u64()).unwrap_or(3306),
                config
                    .get("database")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
            );

            let mysql_config = MySqlConfig::new(connection_string);
            let connector = MySqlConnector::new(mysql_config);

            connector
                .validate_credentials()
                .await
                .map_err(|e| format!("Connection failed: {}", e))?;

            let tables = connector
                .list_tables()
                .await
                .map_err(|e| format!("Failed to list tables: {}", e))?;

            Ok(tables.into_iter().map(|t| t.name).collect())
        }

        SourceType::MongoDB => {
            let connection_string = config
                .get("connection_string")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| {
                    let auth_db = config
                        .get("auth_database")
                        .and_then(|v| v.as_str())
                        .unwrap_or("admin");
                    format!(
                        "mongodb://{}:{}@{}:{}/{}?authSource={}",
                        config
                            .get("username")
                            .and_then(|v| v.as_str())
                            .unwrap_or(""),
                        config
                            .get("password")
                            .and_then(|v| v.as_str())
                            .unwrap_or(""),
                        config
                            .get("host")
                            .and_then(|v| v.as_str())
                            .unwrap_or("localhost"),
                        config.get("port").and_then(|v| v.as_u64()).unwrap_or(27017),
                        config
                            .get("database")
                            .and_then(|v| v.as_str())
                            .unwrap_or(""),
                        auth_db
                    )
                });
            let database = config
                .get("database")
                .and_then(|v| v.as_str())
                .unwrap_or("test")
                .to_string();

            let mongo_config = MongoDBConfig::new(connection_string, database);
            let connector = MongoDBConnector::new(mongo_config);

            connector
                .validate_credentials()
                .await
                .map_err(|e| format!("Connection failed: {}", e))?;

            let tables = connector
                .list_tables()
                .await
                .map_err(|e| format!("Failed to list collections: {}", e))?;

            Ok(tables.into_iter().map(|t| t.name).collect())
        }

        SourceType::SqlServer => {
            let host = config
                .get("host")
                .and_then(|v| v.as_str())
                .unwrap_or("localhost")
                .to_string();
            let database = config
                .get("database")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let username = config
                .get("username")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let password = config
                .get("password")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let port = config.get("port").and_then(|v| v.as_u64()).unwrap_or(1433) as u16;

            let ss_config = SqlServerConfig::new(host, database, username, password)
                .with_port(port)
                .with_trust_server_certificate(true);
            let connector = SqlServerConnector::new(ss_config);

            connector
                .validate_credentials()
                .await
                .map_err(|e| format!("Connection failed: {}", e))?;

            let tables = connector
                .list_tables()
                .await
                .map_err(|e| format!("Failed to list tables: {}", e))?;

            Ok(tables.into_iter().map(|t| t.name).collect())
        }

        SourceType::SQLite => {
            let path = config
                .get("database_path")
                .or_else(|| config.get("path"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let sqlite_config = SQLiteConfig::new(path).with_read_only(true);
            let connector = SQLiteConnector::new(sqlite_config);

            connector
                .validate_credentials()
                .await
                .map_err(|e| format!("Connection failed: {}", e))?;

            let tables = connector
                .list_tables()
                .await
                .map_err(|e| format!("Failed to list tables: {}", e))?;

            Ok(tables.into_iter().map(|t| t.name).collect())
        }

        SourceType::BigQuery => {
            let project_id = config
                .get("project_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let dataset = config
                .get("dataset")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let mut bq_config = BigQueryConfig::new(project_id, dataset);
            if let Some(creds_json) = config.get("credentials_json").and_then(|v| v.as_str()) {
                bq_config = bq_config.with_credentials_json(creds_json);
            }
            if let Some(creds_path) = config.get("credentials_path").and_then(|v| v.as_str()) {
                bq_config = bq_config.with_credentials_path(creds_path);
            }

            let connector = BigQueryConnector::new(bq_config);

            connector
                .validate_credentials()
                .await
                .map_err(|e| format!("Connection failed: {}", e))?;

            let tables = connector
                .list_tables()
                .await
                .map_err(|e| format!("Failed to list tables: {}", e))?;

            Ok(tables.into_iter().map(|t| t.name).collect())
        }

        SourceType::Redshift => {
            let host = config
                .get("host")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let database = config
                .get("database")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let username = config
                .get("username")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let password = config
                .get("password")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let port = config.get("port").and_then(|v| v.as_u64()).unwrap_or(5439) as u16;
            let schema = config
                .get("schema")
                .and_then(|v| v.as_str())
                .unwrap_or("public")
                .to_string();

            let redshift_config = RedshiftConfig::new(host, database, username, password)
                .with_port(port)
                .with_schema(schema);
            let connector = RedshiftConnector::new(redshift_config);

            connector
                .validate_credentials()
                .await
                .map_err(|e| format!("Connection failed: {}", e))?;

            let tables = connector
                .list_tables()
                .await
                .map_err(|e| format!("Failed to list tables: {}", e))?;

            Ok(tables.into_iter().map(|t| t.name).collect())
        }

        SourceType::Snowflake => {
            let account = config
                .get("account")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let warehouse = config
                .get("warehouse")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let database = config
                .get("database")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let username = config
                .get("username")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let password = config
                .get("password")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let schema = config
                .get("schema")
                .and_then(|v| v.as_str())
                .unwrap_or("PUBLIC")
                .to_string();
            let role = config
                .get("role")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            let mut snowflake_config =
                SnowflakeConfig::new(account, warehouse, database, username, password)
                    .with_schema(schema);

            if let Some(r) = role {
                snowflake_config = snowflake_config.with_role(r);
            }

            let connector = SnowflakeConnector::new(snowflake_config);

            connector
                .validate_credentials()
                .await
                .map_err(|e| format!("Connection failed: {}", e))?;

            let tables = connector
                .list_tables()
                .await
                .map_err(|e| format!("Failed to list tables: {}", e))?;

            Ok(tables.into_iter().map(|t| t.name).collect())
        }

        SourceType::ClickHouse => {
            let host = config
                .get("host")
                .and_then(|v| v.as_str())
                .unwrap_or("localhost")
                .to_string();
            let database = config
                .get("database")
                .and_then(|v| v.as_str())
                .unwrap_or("default")
                .to_string();

            let mut ch_config = ClickHouseConfig::new(host, database);
            if let Some(http_port) = config.get("http_port").and_then(|v| v.as_u64()) {
                ch_config.http_port = http_port as u16;
            } else if let Some(port) = config.get("port").and_then(|v| v.as_u64()) {
                ch_config.http_port = port as u16;
            }
            if let Some(native_port) = config.get("native_port").and_then(|v| v.as_u64()) {
                ch_config.native_port = native_port as u16;
            }
            if let Some(proto_str) = config.get("protocol").and_then(|v| v.as_str()) {
                if let Ok(proto) = proto_str.parse::<crate::warehouse::connectors::databases::clickhouse::ClickHouseProtocol>() {
                    ch_config = ch_config.with_protocol(proto);
                }
            }
            if let Some(username) = config.get("username").and_then(|v| v.as_str()) {
                let password = config
                    .get("password")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                ch_config = ch_config.with_credentials(username, password);
            }

            let connector = ClickHouseConnector::new(ch_config).await;

            connector
                .validate_credentials()
                .await
                .map_err(|e| format!("Connection failed: {}", e))?;

            let tables = connector
                .list_tables()
                .await
                .map_err(|e| format!("Failed to list tables: {}", e))?;

            Ok(tables.into_iter().map(|t| t.name).collect())
        }

        _ => Err(format!(
            "Source type {:?} is not supported for connection testing",
            source_type
        )),
    }
}

/// Path parameters for source-specific endpoints.
#[derive(Debug, Deserialize)]
pub struct SourcePath {
    pub project_id: Uuid,
    pub source_id: Uuid,
}

/// Path parameters for view-specific endpoints.
#[derive(Debug, Deserialize)]
pub struct ViewPath {
    pub project_id: Uuid,
    pub view_id: Uuid,
}

/// Get a source by ID.
#[tracing::instrument(name = "warehouse.api.get_source", skip(state), fields(project_id = %path.project_id, source_id = %path.source_id), err(Display))]
async fn get_source(
    State(state): State<Arc<PondState>>,
    Path(path): Path<SourcePath>,
) -> Result<Json<SourceResponse>> {
    // Fetch source with active job info
    let row = sqlx::query(
        r#"SELECT 
            s.id, s.name, s.source_type, 
            COALESCE(s.tier, 'cold') as tier, 
            s.enabled, s.sync_interval, s.last_sync_at,
            s.warm_at, s.hot_at,
            COALESCE(s.storage_tier_policy, '{"type": "fixed"}'::jsonb) as storage_tier_policy,
            COALESCE(s.sync_scope, 'full') as sync_scope,
            s.sync_scope_older_than_days,
            s.global_source_id,
            s.created_at, s.updated_at,
            j.id as job_id, j.job_type
         FROM warehouse_sources s
         LEFT JOIN LATERAL (
            SELECT id, job_type FROM warehouse_jobs
            WHERE source_id = s.id AND status IN ('pending', 'running')
            ORDER BY scheduled_at DESC
            LIMIT 1
         ) j ON true
         WHERE s.id = $1 AND s.project_id = $2"#,
    )
    .bind(path.source_id)
    .bind(path.project_id)
    .fetch_optional(&*state.db)
    .await?
    .ok_or_else(|| AppError::NotFound("Source not found".to_string()))?;

    let job_id: Option<Uuid> = row.get("job_id");
    let job_type_str: Option<String> = row.get("job_type");
    let source_type_str: String = row.get("source_type");
    let tier_str: String = row.get("tier");
    let sync_interval_str: Option<String> = row.get("sync_interval");
    let storage_tier_policy_json: serde_json::Value = row.get("storage_tier_policy");
    let storage_tier_policy: StorageTierPolicy =
        serde_json::from_value(storage_tier_policy_json).unwrap_or_default();
    let sync_scope: String = row.get("sync_scope");
    let sync_scope_older_than_days: Option<i32> = row.get("sync_scope_older_than_days");
    let global_source_id: Option<Uuid> = row.get("global_source_id");

    Ok(Json(SourceResponse {
        id: row.get("id"),
        name: row.get("name"),
        source_type: source_type_str
            .parse()
            .unwrap_or(SourceType::ExternalParquet),
        tier: tier_str.parse().unwrap_or_default(),
        enabled: row.get("enabled"),
        sync_interval: sync_interval_str.and_then(|s| s.parse().ok()),
        storage_tier_policy,
        sync_scope,
        sync_scope_older_than_days: sync_scope_older_than_days.map(|d| d as u32),
        warm_at: row.get("warm_at"),
        hot_at: row.get("hot_at"),
        last_sync_at: row.get("last_sync_at"),
        sync_in_progress: job_id.is_some(),
        current_job_type: job_type_str.and_then(|s| s.parse().ok()),
        current_job_id: job_id,
        is_global: global_source_id.is_some(),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }))
}

/// Delete a source.
///
/// Deletes the source from the database. Associated storage cleanup:
/// - R2/S3 Parquet files: Cleaned up via the downgrade_to_cold job
/// - ClickHouse tables: Cleaned up via the downgrade_to_cold job  
/// - Database records (partitions, tables, jobs): Cascade-deleted automatically
#[tracing::instrument(name = "warehouse.api.delete_source", skip(state), fields(project_id = %path.project_id, source_id = %path.source_id), err(Display))]
async fn delete_source(
    State(state): State<Arc<PondState>>,
    Path(path): Path<SourcePath>,
) -> Result<StatusCode> {
    // Load source info for logging
    let source_row =
        sqlx::query("SELECT name, tier FROM warehouse_sources WHERE id = $1 AND project_id = $2")
            .bind(path.source_id)
            .bind(path.project_id)
            .fetch_optional(&*state.db)
            .await?;

    let (source_name, tier) = match source_row {
        Some(row) => {
            let name: String = row.get("name");
            let tier: String = row.get("tier");
            (name, tier)
        }
        None => return Err(AppError::NotFound("Source not found".to_string())),
    };

    tracing::info!(
        source_id = %path.source_id,
        source_name = %source_name,
        tier = %tier,
        "Deleting source"
    );

    // Schedule storage cleanup via Kafka BEFORE deleting the source, so the
    // consumer can still `load_source` to discover R2 paths and ClickHouse tables.
    // We don't insert a warehouse_jobs record because the CASCADE DELETE below
    // would immediately remove it, causing the consumer to fail on job lookup.
    let job_id = Uuid::new_v4();
    let now = chrono::Utc::now();

    let kafka_msg = SyncJobKafkaMessage {
        job_id,
        job_type: JobType::DowngradeToCold.to_string(),
        source_id: path.source_id,
        project_id: path.project_id,
        table_name: None,
        created_at: now.to_rfc3339(),
    };

    if let Err(e) = state.kafka.send_sync_job(&kafka_msg).await {
        tracing::warn!(
            source_id = %path.source_id,
            error = %e,
            "Failed to schedule storage cleanup job (will be orphaned)"
        );
    }

    // Delete DB records (cascade will handle partitions, tables, jobs)
    let result = sqlx::query("DELETE FROM warehouse_sources WHERE id = $1 AND project_id = $2")
        .bind(path.source_id)
        .bind(path.project_id)
        .execute(&*state.db)
        .await?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound("Source not found".to_string()));
    }

    tracing::info!(
        source_id = %path.source_id,
        source_name = %source_name,
        "Source deleted (storage cleanup scheduled)"
    );

    Ok(StatusCode::NO_CONTENT)
}

/// Request to update a source.
#[derive(Debug, Deserialize)]
pub struct UpdateSourceRequest {
    /// New name for the source (optional)
    pub name: Option<String>,
    /// New configuration/credentials (optional, will be encrypted)
    pub config: Option<serde_json::Value>,
    /// Enable/disable the source (optional)
    pub enabled: Option<bool>,
    /// New sync interval cron expression (optional)
    pub sync_interval: Option<String>,
    /// New storage tier policy (optional)
    pub storage_tier_policy: Option<StorageTierPolicy>,
}

/// Update a source.
///
/// This endpoint allows updating source credentials without deleting and recreating
/// the source. Credential rotation should be done through this endpoint.
#[tracing::instrument(name = "warehouse.api.update_source", skip(state, req), fields(project_id = %path.project_id, source_id = %path.source_id), err(Display))]
async fn update_source(
    State(state): State<Arc<PondState>>,
    Path(path): Path<SourcePath>,
    Json(req): Json<UpdateSourceRequest>,
) -> Result<Json<SourceResponse>> {
    // Validate input
    if let Some(ref name) = req.name {
        if name.is_empty() {
            return Err(AppError::Validation(
                "Source name cannot be empty".to_string(),
            ));
        }
        if name.len() > MAX_SOURCE_NAME_LENGTH {
            return Err(AppError::Validation(format!(
                "Source name too long (max {} characters)",
                MAX_SOURCE_NAME_LENGTH
            )));
        }
        if !name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == ' ')
        {
            return Err(AppError::Validation(
                "Source name can only contain alphanumeric characters, underscores, hyphens, and spaces".to_string()
            ));
        }
    }

    if let Some(ref cron) = req.sync_interval {
        validate_cron_expression(cron)?;
    }

    // Verify source exists and belongs to project
    let existing = sqlx::query(
        "SELECT id, name, source_type, enabled, global_source_id, created_at FROM warehouse_sources WHERE id = $1 AND project_id = $2"
    )
    .bind(path.source_id)
    .bind(path.project_id)
    .fetch_optional(&*state.db)
    .await?
    .ok_or_else(|| AppError::NotFound("Source not found".to_string()))?;

    let is_global = existing.get::<Option<Uuid>, _>("global_source_id").is_some();
    if is_global && (req.config.is_some() || req.sync_interval.is_some() || req.storage_tier_policy.is_some() || req.enabled.is_some()) {
        return Err(AppError::Validation(
            "Global sources are managed by the system. Only the source name can be changed.".to_string(),
        ));
    }

    let now = Utc::now();

    // Start transaction
    let mut tx = state.db.begin().await?;

    // Handle name update
    if let Some(ref name) = req.name {
        sqlx::query("UPDATE warehouse_sources SET name = $1, updated_at = $2 WHERE id = $3")
            .bind(name)
            .bind(now)
            .bind(path.source_id)
            .execute(&mut *tx)
            .await?;
    }

    // Handle config update (with encryption)
    if let Some(ref config) = req.config {
        let config_json = serde_json::to_string(config)
            .map_err(|e| AppError::Validation(format!("Invalid config JSON: {}", e)))?;
        let encrypted_config = state
            .encryptor
            .encrypt(&config_json)
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to encrypt config: {}", e)))?;
        let encrypted_config_json = serde_json::json!({ "encrypted": encrypted_config });

        sqlx::query("UPDATE warehouse_sources SET config = $1, updated_at = $2 WHERE id = $3")
            .bind(&encrypted_config_json)
            .bind(now)
            .bind(path.source_id)
            .execute(&mut *tx)
            .await?;
    }

    // Handle enabled update
    if let Some(enabled) = req.enabled {
        sqlx::query("UPDATE warehouse_sources SET enabled = $1, updated_at = $2 WHERE id = $3")
            .bind(enabled)
            .bind(now)
            .bind(path.source_id)
            .execute(&mut *tx)
            .await?;
    }

    // Handle sync interval update
    if let Some(ref cron) = req.sync_interval {
        // Validate cron expression before storing
        validate_cron_expression(cron)?;

        // Update or create sync schedule
        sqlx::query(
            r#"
            INSERT INTO warehouse_sync_schedules (id, source_id, cron_expression, enabled, created_at, updated_at)
            VALUES ($1, $2, $3, true, $4, $4)
            ON CONFLICT (source_id) DO UPDATE SET
                cron_expression = $3,
                updated_at = $4
            "#
        )
        .bind(Uuid::new_v4())
        .bind(path.source_id)
        .bind(cron)
        .bind(now)
        .execute(&mut *tx)
        .await?;
    }

    // Handle storage tier policy update
    if let Some(ref policy) = req.storage_tier_policy {
        let policy_json = serde_json::to_value(policy)
            .map_err(|e| AppError::Validation(format!("Invalid storage tier policy: {}", e)))?;
        sqlx::query(
            "UPDATE warehouse_sources SET storage_tier_policy = $1, updated_at = $2 WHERE id = $3",
        )
        .bind(&policy_json)
        .bind(now)
        .bind(path.source_id)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;

    // Fetch updated source with active job info
    let row = sqlx::query(
        r#"SELECT 
            s.id, s.name, s.source_type, 
            COALESCE(s.tier, 'cold') as tier, 
            s.enabled, s.sync_interval, s.last_sync_at,
            s.warm_at, s.hot_at,
            COALESCE(s.storage_tier_policy, '{"type": "fixed"}'::jsonb) as storage_tier_policy,
            COALESCE(s.sync_scope, 'full') as sync_scope,
            s.sync_scope_older_than_days,
            s.global_source_id,
            s.created_at, s.updated_at,
            j.id as job_id, j.job_type
         FROM warehouse_sources s
         LEFT JOIN LATERAL (
            SELECT id, job_type FROM warehouse_jobs
            WHERE source_id = s.id AND status IN ('pending', 'running')
            ORDER BY scheduled_at DESC
            LIMIT 1
         ) j ON true
         WHERE s.id = $1"#,
    )
    .bind(path.source_id)
    .fetch_one(&*state.db)
    .await?;

    let job_id: Option<Uuid> = row.get("job_id");
    let job_type_str: Option<String> = row.get("job_type");
    let source_type_str: String = row.get("source_type");
    let tier_str: String = row.get("tier");
    let sync_interval_str: Option<String> = row.get("sync_interval");
    let storage_tier_policy_json: serde_json::Value = row.get("storage_tier_policy");
    let storage_tier_policy: StorageTierPolicy =
        serde_json::from_value(storage_tier_policy_json).unwrap_or_default();
    let sync_scope: String = row.get("sync_scope");
    let sync_scope_older_than_days: Option<i32> = row.get("sync_scope_older_than_days");
    let global_source_id: Option<Uuid> = row.get("global_source_id");

    Ok(Json(SourceResponse {
        id: row.get("id"),
        name: row.get("name"),
        source_type: source_type_str
            .parse()
            .unwrap_or(SourceType::ExternalParquet),
        tier: tier_str.parse().unwrap_or_default(),
        enabled: row.get("enabled"),
        sync_interval: sync_interval_str.and_then(|s| s.parse().ok()),
        storage_tier_policy,
        sync_scope,
        sync_scope_older_than_days: sync_scope_older_than_days.map(|d| d as u32),
        warm_at: row.get("warm_at"),
        hot_at: row.get("hot_at"),
        last_sync_at: row.get("last_sync_at"),
        sync_in_progress: job_id.is_some(),
        current_job_type: job_type_str.and_then(|s| s.parse().ok()),
        current_job_id: job_id,
        is_global: global_source_id.is_some(),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }))
}

/// Trigger a manual sync for a source.
#[tracing::instrument(
    name = "warehouse.api.trigger_sync",
    skip(state),
    fields(project_id = %path.project_id, source_id = %path.source_id),
    err(Display),
)]
async fn trigger_sync(
    State(state): State<Arc<PondState>>,
    Path(path): Path<SourcePath>,
) -> Result<Json<SyncTriggerResponse>> {
    // Verify source exists and belongs to the project
    let exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM warehouse_sources WHERE id = $1 AND project_id = $2)",
    )
    .bind(path.source_id)
    .bind(path.project_id)
    .fetch_one(&*state.db)
    .await?;

    if !exists {
        return Err(AppError::NotFound("Source not found".to_string()));
    }

    // DEDUPLICATION: Check if there's already a pending or running sync for this source
    if let Some(job_id) = find_active_job(&state.db, path.source_id, Some(&["sync"])).await? {
        tracing::info!(
            source_id = %path.source_id,
            existing_job_id = %job_id,
            "Sync already in progress, returning existing job"
        );
        return Ok(Json(SyncTriggerResponse {
            job_id,
            status: JobStatus::Running,
        }));
    }

    // Create a pending job
    let job_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO warehouse_jobs (id, job_type, source_id, status, scheduled_at)
         VALUES ($1, 'sync', $2, 'pending', NOW())",
    )
    .bind(job_id)
    .bind(path.source_id)
    .execute(&*state.db)
    .await?;

    Ok(Json(SyncTriggerResponse {
        job_id,
        status: JobStatus::Pending,
    }))
}

// ===== Storage Tier Transition Endpoints =====

/// Upgrade a source to a higher storage tier.
/// Transitions: cold -> warm (sync to Parquet on R2/S3), cold -> hot (sync to ClickHouse),
///              warm -> hot (sync to ClickHouse)
#[tracing::instrument(
    name = "warehouse.api.upgrade",
    skip(state, req),
    fields(project_id = %path.project_id, source_id = %path.source_id, target_tier = ?req.target_tier),
    err(Display),
)]
async fn upgrade_source(
    State(state): State<Arc<PondState>>,
    Path(path): Path<SourcePath>,
    Json(req): Json<TierTransitionRequest>,
) -> Result<Json<TierTransitionResponse>> {
    // Verify source exists and get current tier
    let source = sqlx::query(
        "SELECT id, tier, source_type FROM warehouse_sources WHERE id = $1 AND project_id = $2",
    )
    .bind(path.source_id)
    .bind(path.project_id)
    .fetch_optional(&*state.db)
    .await?
    .ok_or_else(|| AppError::NotFound("Source not found".to_string()))?;

    let current_tier: StorageTier = source.get::<String, _>("tier").parse().unwrap_or_default();
    let source_type_parsed: SourceType = source
        .get::<String, _>("source_type")
        .parse()
        .unwrap_or(SourceType::ExternalParquet);

    // Blockchain sources use managed storage — tier cannot be changed by users
    if source_type_parsed.is_blockchain() {
        return Err(AppError::Validation(
            "Blockchain sources use managed storage. Tier cannot be changed.".to_string(),
        ));
    }

    match req.target_tier {
        StorageTier::Cold => {
            return Err(AppError::Validation(
                "Cannot upgrade to cold tier. Use the /downgrade endpoint instead.".to_string(),
            ));
        }
        StorageTier::Warm => {
            // Upgrade to warm
            if current_tier == StorageTier::Warm {
                return Err(AppError::Validation("Source is already warm".to_string()));
            }
            if current_tier == StorageTier::Hot {
                return Err(AppError::Validation(
                    "Source is hot. Use /downgrade to move to warm tier.".to_string(),
                ));
            }

            // For external Parquet sources, "upgrade to warm" means "build index"
            let job_type = if source_type_parsed == SourceType::ExternalParquet {
                JobType::IndexBuild
            } else {
                JobType::UpgradeToWarm
            };

            // Check for existing upgrade job
            if let Some(job_id) = find_active_job(
                &state.db,
                path.source_id,
                Some(&["upgrade_to_warm", "index_build"]),
            )
            .await?
            {
                return Ok(Json(TierTransitionResponse {
                    job_id,
                    status: JobStatus::Running,
                    target_tier: StorageTier::Warm,
                }));
            }

            // Create upgrade job
            let job_id = Uuid::new_v4();
            sqlx::query(
                "INSERT INTO warehouse_jobs (id, job_type, source_id, status, scheduled_at)
                 VALUES ($1, $2, $3, 'pending', NOW())",
            )
            .bind(job_id)
            .bind(job_type.to_string())
            .bind(path.source_id)
            .execute(&*state.db)
            .await?;

            // Publish job to Kafka for async processing
            let kafka_msg = SyncJobKafkaMessage {
                job_id,
                job_type: job_type.to_string(),
                source_id: path.source_id,
                project_id: path.project_id,
                table_name: None,
                created_at: Utc::now().to_rfc3339(),
            };

            if let Err(e) = state.kafka.send_sync_job(&kafka_msg).await {
                tracing::error!(
                    job_id = %job_id,
                    error = %e,
                    "Failed to publish upgrade to warm job to Kafka"
                );
                let _ = sqlx::query(
                    "UPDATE warehouse_jobs SET status = 'failed', error = $1, completed_at = NOW() WHERE id = $2"
                )
                .bind(format!("Failed to queue job: {}", e))
                .bind(job_id)
                .execute(&*state.db)
                .await;

                return Err(AppError::Internal(anyhow::anyhow!(
                    "Failed to queue upgrade to warm job. Please try again."
                )));
            }

            tracing::info!(
                source_id = %path.source_id,
                job_id = %job_id,
                job_type = %job_type,
                "Upgrade to warm job queued"
            );

            Ok(Json(TierTransitionResponse {
                job_id,
                status: JobStatus::Pending,
                target_tier: StorageTier::Warm,
            }))
        }
        StorageTier::Hot => {
            // Upgrade to hot
            if current_tier == StorageTier::Hot {
                return Err(AppError::Validation("Source is already hot".to_string()));
            }

            // Check for existing upgrade job
            if let Some(job_id) =
                find_active_job(&state.db, path.source_id, Some(&["upgrade_to_hot"])).await?
            {
                return Ok(Json(TierTransitionResponse {
                    job_id,
                    status: JobStatus::Running,
                    target_tier: StorageTier::Hot,
                }));
            }

            // Create upgrade to hot job
            let job_id = Uuid::new_v4();
            sqlx::query(
                "INSERT INTO warehouse_jobs (id, job_type, source_id, status, scheduled_at)
                 VALUES ($1, 'upgrade_to_hot', $2, 'pending', NOW())",
            )
            .bind(job_id)
            .bind(path.source_id)
            .execute(&*state.db)
            .await?;

            // Publish job to Kafka for async processing
            let kafka_msg = SyncJobKafkaMessage {
                job_id,
                job_type: JobType::UpgradeToHot.to_string(),
                source_id: path.source_id,
                project_id: path.project_id,
                table_name: None,
                created_at: Utc::now().to_rfc3339(),
            };

            if let Err(e) = state.kafka.send_sync_job(&kafka_msg).await {
                tracing::error!(
                    job_id = %job_id,
                    error = %e,
                    "Failed to publish upgrade to hot job to Kafka"
                );
                let _ = sqlx::query(
                    "UPDATE warehouse_jobs SET status = 'failed', error = $1, completed_at = NOW() WHERE id = $2"
                )
                .bind(format!("Failed to queue job: {}", e))
                .bind(job_id)
                .execute(&*state.db)
                .await;

                return Err(AppError::Internal(anyhow::anyhow!(
                    "Failed to queue upgrade to hot job. Please try again."
                )));
            }

            tracing::info!(
                source_id = %path.source_id,
                job_id = %job_id,
                "Upgrade to hot job queued"
            );

            Ok(Json(TierTransitionResponse {
                job_id,
                status: JobStatus::Pending,
                target_tier: StorageTier::Hot,
            }))
        }
    }
}

/// Downgrade a source to a lower storage tier.
/// Transitions: hot -> warm (delete ClickHouse tables, keep Parquet on R2),
///              hot -> cold (delete all cached data),
///              warm -> cold (delete all cached data)
#[tracing::instrument(
    name = "warehouse.api.downgrade",
    skip(state, req),
    fields(project_id = %path.project_id, source_id = %path.source_id, target_tier = ?req.target_tier),
    err(Display),
)]
async fn downgrade_source(
    State(state): State<Arc<PondState>>,
    Path(path): Path<SourcePath>,
    Json(req): Json<TierTransitionRequest>,
) -> Result<Json<TierTransitionResponse>> {
    // Verify source exists and get current tier
    let source = sqlx::query(
        "SELECT id, tier, source_type FROM warehouse_sources WHERE id = $1 AND project_id = $2",
    )
    .bind(path.source_id)
    .bind(path.project_id)
    .fetch_optional(&*state.db)
    .await?
    .ok_or_else(|| AppError::NotFound("Source not found".to_string()))?;

    let current_tier: StorageTier = source.get::<String, _>("tier").parse().unwrap_or_default();
    let source_type_parsed: SourceType = source
        .get::<String, _>("source_type")
        .parse()
        .unwrap_or(SourceType::ExternalParquet);

    // Blockchain sources use managed storage — tier cannot be changed by users
    if source_type_parsed.is_blockchain() {
        return Err(AppError::Validation(
            "Blockchain sources use managed storage. Tier cannot be changed.".to_string(),
        ));
    }

    match req.target_tier {
        StorageTier::Hot => {
            return Err(AppError::Validation(
                "Cannot downgrade to hot tier. Use the /upgrade endpoint instead.".to_string(),
            ));
        }
        StorageTier::Warm => {
            // Downgrade to warm: hot -> warm
            if current_tier != StorageTier::Hot {
                return Err(AppError::Validation(
                    format!("Cannot downgrade to warm from '{}' tier. Only hot sources can be downgraded to warm.", current_tier)
                ));
            }

            // Create downgrade job (will delete ClickHouse tables and update tier to warm)
            let job_id = Uuid::new_v4();
            sqlx::query(
                "INSERT INTO warehouse_jobs (id, job_type, source_id, status, scheduled_at)
                 VALUES ($1, 'downgrade_to_warm', $2, 'pending', NOW())",
            )
            .bind(job_id)
            .bind(path.source_id)
            .execute(&*state.db)
            .await?;

            // Publish job to Kafka for async processing
            let kafka_msg = SyncJobKafkaMessage {
                job_id,
                job_type: JobType::DowngradeToWarm.to_string(),
                source_id: path.source_id,
                project_id: path.project_id,
                table_name: None,
                created_at: Utc::now().to_rfc3339(),
            };

            if let Err(e) = state.kafka.send_sync_job(&kafka_msg).await {
                tracing::error!(
                    job_id = %job_id,
                    error = %e,
                    "Failed to publish downgrade to warm job to Kafka"
                );
                let _ = sqlx::query(
                    "UPDATE warehouse_jobs SET status = 'failed', error = $1, completed_at = NOW() WHERE id = $2"
                )
                .bind(format!("Failed to queue job: {}", e))
                .bind(job_id)
                .execute(&*state.db)
                .await;

                return Err(AppError::Internal(anyhow::anyhow!(
                    "Failed to queue downgrade to warm job. Please try again."
                )));
            }

            tracing::info!(
                source_id = %path.source_id,
                job_id = %job_id,
                "Downgrade to warm job queued"
            );

            Ok(Json(TierTransitionResponse {
                job_id,
                status: JobStatus::Pending,
                target_tier: StorageTier::Warm,
            }))
        }
        StorageTier::Cold => {
            // Downgrade to cold: warm -> cold, hot -> cold
            if current_tier == StorageTier::Cold {
                return Err(AppError::Validation(
                    "Source is already in cold tier with no cached data".to_string(),
                ));
            }

            // Create downgrade to cold job (will delete all cached data and update tier to cold)
            let job_id = Uuid::new_v4();
            sqlx::query(
                "INSERT INTO warehouse_jobs (id, job_type, source_id, status, scheduled_at)
                 VALUES ($1, 'downgrade_to_cold', $2, 'pending', NOW())",
            )
            .bind(job_id)
            .bind(path.source_id)
            .execute(&*state.db)
            .await?;

            // Publish job to Kafka for async processing
            let kafka_msg = SyncJobKafkaMessage {
                job_id,
                job_type: JobType::DowngradeToCold.to_string(),
                source_id: path.source_id,
                project_id: path.project_id,
                table_name: None,
                created_at: Utc::now().to_rfc3339(),
            };

            if let Err(e) = state.kafka.send_sync_job(&kafka_msg).await {
                tracing::error!(
                    job_id = %job_id,
                    error = %e,
                    "Failed to publish downgrade to cold job to Kafka"
                );
                let _ = sqlx::query(
                    "UPDATE warehouse_jobs SET status = 'failed', error = $1, completed_at = NOW() WHERE id = $2"
                )
                .bind(format!("Failed to queue job: {}", e))
                .bind(job_id)
                .execute(&*state.db)
                .await;

                return Err(AppError::Internal(anyhow::anyhow!(
                    "Failed to queue downgrade to cold job. Please try again."
                )));
            }

            tracing::info!(
                source_id = %path.source_id,
                job_id = %job_id,
                from_tier = %current_tier,
                "Downgrade to cold job queued"
            );

            Ok(Json(TierTransitionResponse {
                job_id,
                status: JobStatus::Pending,
                target_tier: StorageTier::Cold,
            }))
        }
    }
}

/// Get the current status of a source including tier info.
#[tracing::instrument(name = "warehouse.api.get_source_status", skip(state), fields(project_id = %path.project_id, source_id = %path.source_id), err(Display))]
async fn get_source_status(
    State(state): State<Arc<PondState>>,
    Path(path): Path<SourcePath>,
) -> Result<Json<SourceStatusResponse>> {
    // Get source with tier metadata
    let source = sqlx::query(
        "SELECT id, name, tier, warm_at, hot_at, last_sync_at, storage_bytes, sync_interval 
         FROM warehouse_sources 
         WHERE id = $1 AND project_id = $2",
    )
    .bind(path.source_id)
    .bind(path.project_id)
    .fetch_optional(&*state.db)
    .await?
    .ok_or_else(|| AppError::NotFound("Source not found".to_string()))?;

    // Check for active sync job
    let active_job = find_active_job(&state.db, path.source_id, None).await?;

    let tier_str: String = source.get("tier");
    let tier: StorageTier = tier_str.parse().unwrap_or_default();
    let sync_interval_str: Option<String> = source.get("sync_interval");
    let sync_interval: Option<SyncInterval> = sync_interval_str.and_then(|s| s.parse().ok());

    Ok(Json(SourceStatusResponse {
        id: source.get("id"),
        name: source.get("name"),
        tier,
        warm_at: source.get("warm_at"),
        hot_at: source.get("hot_at"),
        last_sync_at: source.get("last_sync_at"),
        storage_bytes: source.get::<Option<i64>, _>("storage_bytes").unwrap_or(0),
        sync_interval,
        sync_in_progress: active_job.is_some(),
        current_job_id: active_job,
    }))
}

/// Set the sync interval for a warm or hot source.
#[tracing::instrument(name = "warehouse.api.set_sync_interval", skip(state, req), fields(project_id = %path.project_id, source_id = %path.source_id), err(Display))]
async fn set_sync_interval(
    State(state): State<Arc<PondState>>,
    Path(path): Path<SourcePath>,
    Json(req): Json<SetSyncIntervalRequest>,
) -> Result<Json<SourceStatusResponse>> {
    // Serde handles validation of the SyncInterval enum during deserialization.

    // Verify source exists and get current tier
    let source =
        sqlx::query("SELECT id, tier, global_source_id FROM warehouse_sources WHERE id = $1 AND project_id = $2")
            .bind(path.source_id)
            .bind(path.project_id)
            .fetch_optional(&*state.db)
            .await?
            .ok_or_else(|| AppError::NotFound("Source not found".to_string()))?;

    let global_source_id: Option<Uuid> = source.get("global_source_id");
    if global_source_id.is_some() {
        return Err(AppError::Validation(
            "Cannot change sync interval for a managed global source.".to_string(),
        ));
    }

    let tier_str: String = source.get("tier");
    let tier: StorageTier = tier_str.parse().unwrap_or_default();

    // Only allow sync interval for warm or hot sources
    if tier.is_cold() && req.interval.is_some() {
        return Err(AppError::Validation(
            "Sync interval can only be set for warm or hot sources".to_string(),
        ));
    }

    // Update sync interval - store as string in DB
    let interval_str: Option<String> = req.interval.map(|i| i.to_string());
    sqlx::query(
        "UPDATE warehouse_sources SET sync_interval = $1, updated_at = NOW() WHERE id = $2",
    )
    .bind(&interval_str)
    .bind(path.source_id)
    .execute(&*state.db)
    .await?;

    tracing::info!(
        source_id = %path.source_id,
        sync_interval = ?req.interval,
        "Sync interval updated"
    );

    // Return updated status
    get_source_status(State(state), Path(path)).await
}

/// List tables for a source.
#[tracing::instrument(name = "warehouse.api.list_tables", skip(state), fields(project_id = %path.project_id, source_id = %path.source_id), err(Display))]
async fn list_tables(
    State(state): State<Arc<PondState>>,
    Path(path): Path<SourcePath>,
) -> Result<Json<Vec<serde_json::Value>>> {
    // Verify source belongs to project
    let source_exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM warehouse_sources WHERE id = $1 AND project_id = $2)",
    )
    .bind(path.source_id)
    .bind(path.project_id)
    .fetch_one(&*state.db)
    .await?;

    if !source_exists {
        return Err(AppError::NotFound("Source not found".to_string()));
    }

    let rows = sqlx::query(
        "SELECT id, name, schema, r2_prefix, sync_enabled, incremental_key, created_at, updated_at
         FROM warehouse_tables WHERE source_id = $1 ORDER BY name",
    )
    .bind(path.source_id)
    .fetch_all(&*state.db)
    .await?;

    let tables: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|row| {
            serde_json::json!({
                "id": row.get::<Uuid, _>("id"),
                "name": row.get::<String, _>("name"),
                "schema": row.get::<serde_json::Value, _>("schema"),
                "r2_prefix": row.get::<String, _>("r2_prefix"),
                "sync_enabled": row.get::<bool, _>("sync_enabled"),
                "incremental_key": row.get::<Option<String>, _>("incremental_key"),
            })
        })
        .collect();

    Ok(Json(tables))
}

// ===== External Source Handlers =====

/// Create a new external Parquet source.
///
/// External sources allow querying customer-owned Parquet files in S3/GCS/Azure
/// without syncing them into reiver's storage.
#[tracing::instrument(name = "warehouse.api.create_external_source", skip(state, req), fields(project_id = %project_id), err(Display))]
async fn create_external_source(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
    Json(req): Json<CreateExternalSourceRequest>,
) -> Result<Json<SourceResponse>> {
    // Validate bucket URL
    validate_bucket_url(&req.bucket_url)?;

    let id = Uuid::new_v4();
    let now = Utc::now();

    // Convert request config to internal config
    let external_config = convert_external_config(&req.config)?;

    // Build the full config JSON (credentials + external config)
    let full_config = serde_json::json!({
        "bucket_url": req.bucket_url,
        "credentials": req.credentials,
        "external_config": external_config,
    });

    // SECURITY: Encrypt the config before storing
    let config_json = serde_json::to_string(&full_config)
        .map_err(|e| AppError::Validation(format!("Invalid config JSON: {}", e)))?;
    let encrypted_config = state
        .encryptor
        .encrypt(&config_json)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to encrypt config: {}", e)))?;

    let encrypted_config_json = serde_json::json!({ "encrypted": encrypted_config });

    sqlx::query(
        "INSERT INTO warehouse_sources (id, project_id, name, source_type, config, enabled, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, true, $6, $6)"
    )
    .bind(id)
    .bind(project_id)
    .bind(&req.name)
    .bind(SourceType::ExternalParquet.to_string())
    .bind(&encrypted_config_json)
    .bind(now)
    .execute(&*state.db)
    .await?;

    Ok(Json(SourceResponse {
        id,
        name: req.name,
        source_type: SourceType::ExternalParquet,
        tier: StorageTier::Cold,
        enabled: true,
        sync_interval: None,
        storage_tier_policy: StorageTierPolicy::default(),
        sync_scope: "full".to_string(),
        sync_scope_older_than_days: None,
        warm_at: None,
        hot_at: None,
        last_sync_at: None,
        sync_in_progress: false,
        current_job_type: None,
        current_job_id: None,
        is_global: false,
        created_at: now,
        updated_at: now,
    }))
}

/// Get external source configuration.
#[tracing::instrument(name = "warehouse.api.get_external_config", skip(state), fields(project_id = %path.project_id, source_id = %path.source_id), err(Display))]
async fn get_external_config(
    State(state): State<Arc<PondState>>,
    Path(path): Path<SourcePath>,
) -> Result<Json<ExternalSourceConfigResponse>> {
    // Get source and verify it's an external source
    let row = sqlx::query(
        "SELECT source_type, config FROM warehouse_sources WHERE id = $1 AND project_id = $2",
    )
    .bind(path.source_id)
    .bind(path.project_id)
    .fetch_optional(&*state.db)
    .await?
    .ok_or_else(|| AppError::NotFound("Source not found".to_string()))?;

    let source_type: String = row.get("source_type");
    ensure_external_parquet(&source_type)?;

    // Decrypt and parse config
    let encrypted_config: serde_json::Value = row.get("config");
    let encrypted_str = encrypted_config
        .get("encrypted")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Invalid encrypted config format")))?;

    let decrypted = state
        .encryptor
        .decrypt(encrypted_str)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to decrypt config: {}", e)))?;

    let config: serde_json::Value = serde_json::from_str(&decrypted)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to parse config: {}", e)))?;

    let external_config = config
        .get("external_config")
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Missing external_config")))?;

    // Convert to response format
    let response = convert_to_config_response(external_config)?;

    Ok(Json(response))
}

/// Update external source configuration.
#[tracing::instrument(name = "warehouse.api.update_external_config", skip(state, req), fields(project_id = %path.project_id, source_id = %path.source_id), err(Display))]
async fn update_external_config(
    State(state): State<Arc<PondState>>,
    Path(path): Path<SourcePath>,
    Json(req): Json<ExternalSourceConfigRequest>,
) -> Result<Json<ExternalSourceConfigResponse>> {
    // Get source and verify it's an external source
    let row = sqlx::query(
        "SELECT source_type, config FROM warehouse_sources WHERE id = $1 AND project_id = $2",
    )
    .bind(path.source_id)
    .bind(path.project_id)
    .fetch_optional(&*state.db)
    .await?
    .ok_or_else(|| AppError::NotFound("Source not found".to_string()))?;

    let source_type: String = row.get("source_type");
    ensure_external_parquet(&source_type)?;

    // Decrypt existing config
    let encrypted_config: serde_json::Value = row.get("config");
    let encrypted_str = encrypted_config
        .get("encrypted")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Invalid encrypted config format")))?;

    let decrypted = state
        .encryptor
        .decrypt(encrypted_str)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to decrypt config: {}", e)))?;

    let mut config: serde_json::Value = serde_json::from_str(&decrypted)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to parse config: {}", e)))?;

    // Update external_config
    let new_external_config = convert_external_config(&req)?;
    config["external_config"] = new_external_config.clone();

    // Re-encrypt and save
    let config_json = serde_json::to_string(&config)
        .map_err(|e| AppError::Validation(format!("Invalid config JSON: {}", e)))?;
    let encrypted = state
        .encryptor
        .encrypt(&config_json)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to encrypt config: {}", e)))?;

    let encrypted_config_json = serde_json::json!({ "encrypted": encrypted });

    sqlx::query("UPDATE warehouse_sources SET config = $1, updated_at = NOW() WHERE id = $2")
        .bind(&encrypted_config_json)
        .bind(path.source_id)
        .execute(&*state.db)
        .await?;

    let response = convert_to_config_response(&new_external_config)?;
    Ok(Json(response))
}

/// List partitions for an external source.
#[tracing::instrument(name = "warehouse.api.list_partitions", skip(state), fields(project_id = %path.project_id, source_id = %path.source_id), err(Display))]
async fn list_partitions(
    State(state): State<Arc<PondState>>,
    Path(path): Path<SourcePath>,
) -> Result<Json<Vec<PartitionInfo>>> {
    let row = sqlx::query(
        "SELECT source_type, config FROM warehouse_sources WHERE id = $1 AND project_id = $2",
    )
    .bind(path.source_id)
    .bind(path.project_id)
    .fetch_optional(&*state.db)
    .await?
    .ok_or_else(|| AppError::NotFound("External source not found".to_string()))?;

    let source_type: String = row.get("source_type");
    ensure_external_parquet(&source_type)?;

    let encrypted_config: serde_json::Value = row.get("config");
    let encrypted_str = encrypted_config
        .get("encrypted")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Invalid encrypted config format")))?;

    let decrypted = state
        .encryptor
        .decrypt(encrypted_str)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to decrypt config: {}", e)))?;

    let config: serde_json::Value = serde_json::from_str(&decrypted)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to parse config: {}", e)))?;

    let prefix = config.get("prefix").and_then(|v| v.as_str()).unwrap_or("");

    let r2 = state
        .r2_storage
        .as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Object storage is not configured")))?;

    const MAX_OBJECTS: usize = 10_000;
    let all_objects = r2
        .list_objects(prefix)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to list bucket objects: {}", e)))?;

    let parquet_objects: Vec<_> = all_objects
        .into_iter()
        .filter(|obj| obj.key.ends_with(".parquet") || obj.key.ends_with(".parq"))
        .take(MAX_OBJECTS)
        .collect();

    let partitions = group_objects_into_partitions(&parquet_objects);
    Ok(Json(partitions))
}

/// Group a list of object-storage files into partition buckets.
///
/// If the file paths follow a Hive-style `key=value/` layout, each unique
/// combination of partition segments becomes a partition key. Otherwise all
/// files are grouped under a single `"/"` partition.
fn group_objects_into_partitions(
    objects: &[crate::warehouse::storage::r2::ObjectInfo],
) -> Vec<PartitionInfo> {
    use crate::warehouse::indexes::detect_hive_partitioning;

    if objects.is_empty() {
        return vec![];
    }

    let keys: Vec<&str> = objects.iter().map(|o| o.key.as_str()).collect();
    let hive_layout = detect_hive_partitioning(&keys, 100);

    let mut groups: AHashMap<String, Vec<&crate::warehouse::storage::r2::ObjectInfo>> =
        AHashMap::new();

    if let Some(layout) = &hive_layout {
        for obj in objects {
            if let Some(parsed) = layout.parser.parse(&obj.key) {
                let partition_key: String = layout
                    .columns
                    .iter()
                    .filter_map(|col| parsed.values.get(col).map(|v| format!("{}={}", col, v)))
                    .collect::<Vec<_>>()
                    .join("/");

                if !partition_key.is_empty() {
                    groups.entry(partition_key).or_default().push(obj);
                    continue;
                }
            }
            groups.entry("/".to_string()).or_default().push(obj);
        }
    } else {
        groups
            .entry("/".to_string())
            .or_default()
            .extend(objects.iter());
    }

    let mut partitions: Vec<PartitionInfo> = groups
        .into_iter()
        .map(|(key, files)| {
            let total_size: u64 = files.iter().map(|f| f.size).sum();
            let newest = files.iter().filter_map(|f| f.last_modified).max();

            PartitionInfo {
                partition_key: key,
                file_count: files.len() as u32,
                is_mutable: true,
                estimated_size_bytes: Some(total_size),
                last_modified: newest,
            }
        })
        .collect();

    partitions.sort_by(|a, b| a.partition_key.cmp(&b.partition_key));
    partitions
}

/// Detect table format for an external source.
#[tracing::instrument(name = "warehouse.api.detect_format", skip(state), fields(project_id = %path.project_id, source_id = %path.source_id), err(Display))]
async fn detect_format(
    State(state): State<Arc<PondState>>,
    Path(path): Path<SourcePath>,
) -> Result<Json<DetectFormatResponse>> {
    // Get source bucket URL
    let row = sqlx::query(
        "SELECT source_type, config FROM warehouse_sources WHERE id = $1 AND project_id = $2",
    )
    .bind(path.source_id)
    .bind(path.project_id)
    .fetch_optional(&*state.db)
    .await?
    .ok_or_else(|| AppError::NotFound("Source not found".to_string()))?;

    let source_type: String = row.get("source_type");
    ensure_external_parquet(&source_type)?;

    // Decrypt config to get bucket URL
    let encrypted_config: serde_json::Value = row.get("config");
    let encrypted_str = encrypted_config
        .get("encrypted")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Invalid encrypted config format")))?;

    let decrypted = state
        .encryptor
        .decrypt(encrypted_str)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to decrypt config: {}", e)))?;

    let config: serde_json::Value = serde_json::from_str(&decrypted)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to parse config: {}", e)))?;

    let bucket_url = config
        .get("bucket_url")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Missing bucket_url")))?;

    // Detect format
    let format = detect_table_format(bucket_url).await;

    Ok(Json(DetectFormatResponse {
        detected_format: format.to_string(),
        confidence: match format {
            TableFormat::Iceberg | TableFormat::DeltaLake => "high".to_string(),
            TableFormat::RawParquet => "detected".to_string(),
            TableFormat::Auto => "unknown".to_string(),
        },
        details: match format {
            TableFormat::Iceberg => {
                Some("Found metadata/ directory with Iceberg metadata".to_string())
            }
            TableFormat::DeltaLake => {
                Some("Found _delta_log/ directory with transaction log".to_string())
            }
            _ => None,
        },
    }))
}

/// Analyze a source and generate AI-powered configuration recommendations.
///
/// This endpoint samples data from the source, profiles columns and file patterns,
/// and generates an optimized configuration recommendation with explanations.
#[tracing::instrument(name = "warehouse.api.analyze_source", skip(state), fields(project_id = %path.project_id, source_id = %path.source_id), err(Display))]
async fn analyze_source(
    State(state): State<Arc<PondState>>,
    Path(path): Path<SourcePath>,
) -> Result<Json<ConfigRecommendationResponse>> {
    use crate::warehouse::ai_config::{ConfigAnalyzer, MockAIConfigProvider};
    use crate::warehouse::sources::DataSourceRegistry;

    // Verify source exists and is external
    let row =
        sqlx::query("SELECT source_type FROM warehouse_sources WHERE id = $1 AND project_id = $2")
            .bind(path.source_id)
            .bind(path.project_id)
            .fetch_optional(&*state.db)
            .await?
            .ok_or_else(|| AppError::NotFound("Source not found".to_string()))?;

    let source_type: String = row.get("source_type");
    ensure_external_parquet(&source_type)?;

    // Create analyzer with mock AI provider
    let registry = Arc::new(DataSourceRegistry::new(
        state.db.clone(),
        state.encryptor.clone(),
    ));
    let provider = MockAIConfigProvider::new();
    let analyzer = ConfigAnalyzer::new(registry, provider);

    // Analyze the source
    let recommendation = analyzer
        .analyze_source(path.project_id, path.source_id)
        .await
        .map_err(|e| {
            AppError::Internal(anyhow::anyhow!(
                "Analysis failed for source {} in project {}: {}",
                path.source_id,
                path.project_id,
                e
            ))
        })?;

    // Convert to API response format
    let config_response = convert_to_config_response(
        &serde_json::to_value(&recommendation.config)
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Serialization error: {}", e)))?,
    )?;

    Ok(Json(ConfigRecommendationResponse {
        config: config_response,
        confidence: recommendation.confidence,
        explanations: recommendation
            .explanations
            .into_iter()
            .map(|e| ConfigExplanationResponse {
                field: e.field,
                reason: e.reason,
                confidence: e.confidence,
            })
            .collect(),
        warnings: recommendation.warnings,
    }))
}

/// Apply a recommended configuration to a source.
///
/// This endpoint updates the source's external configuration with the provided values.
#[tracing::instrument(name = "warehouse.api.apply_config", skip(state, req), fields(project_id = %path.project_id, source_id = %path.source_id), err(Display))]
async fn apply_config(
    State(state): State<Arc<PondState>>,
    Path(path): Path<SourcePath>,
    Json(req): Json<ApplyConfigRequest>,
) -> Result<Json<ExternalSourceConfigResponse>> {
    // Verify source exists and is external
    let row = sqlx::query(
        "SELECT source_type, config FROM warehouse_sources WHERE id = $1 AND project_id = $2",
    )
    .bind(path.source_id)
    .bind(path.project_id)
    .fetch_optional(&*state.db)
    .await?
    .ok_or_else(|| AppError::NotFound("Source not found".to_string()))?;

    let source_type: String = row.get("source_type");
    ensure_external_parquet(&source_type)?;

    // Validate the config
    for col in &req.config.index_columns {
        validate_column_name(&col.name)?;
    }
    if let Some(ref time_col) = req.config.time_column {
        validate_column_name(time_col)?;
    }

    // Convert request to internal format and update
    let new_external_config = convert_external_config(&req.config)?;

    // Decrypt existing config, update external_config, re-encrypt
    let encrypted_config: serde_json::Value = row.get("config");
    let encrypted_str = encrypted_config
        .get("encrypted")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Invalid encrypted config format")))?;

    let decrypted = state
        .encryptor
        .decrypt(encrypted_str)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to decrypt config: {}", e)))?;

    let mut config: serde_json::Value = serde_json::from_str(&decrypted)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to parse config: {}", e)))?;

    config["external_config"] = new_external_config.clone();

    // Re-encrypt and save
    let config_str = serde_json::to_string(&config)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Serialization error: {}", e)))?;
    let encrypted = state
        .encryptor
        .encrypt(&config_str)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Encryption error: {}", e)))?;
    let encrypted_config_json = serde_json::json!({ "encrypted": encrypted });

    sqlx::query("UPDATE warehouse_sources SET config = $1, updated_at = NOW() WHERE id = $2")
        .bind(&encrypted_config_json)
        .bind(path.source_id)
        .execute(&*state.db)
        .await?;

    let response = convert_to_config_response(&new_external_config)?;
    Ok(Json(response))
}

// ===== External Source Helper Functions =====

/// Validate a column name for external source configuration.
///
/// Valid column names:
/// - Contain only alphanumeric characters and underscores
/// - Start with a letter or underscore
/// - Maximum 128 characters
fn validate_column_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(AppError::Validation(
            "Column name cannot be empty".to_string(),
        ));
    }
    if name.len() > 128 {
        return Err(AppError::Validation(format!(
            "Column name '{}' exceeds maximum length of 128 characters",
            &name[..50]
        )));
    }

    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => {
            return Err(AppError::Validation(format!(
                "Column name '{}' must start with a letter or underscore",
                name
            )));
        }
    }

    if !chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(AppError::Validation(format!(
            "Column name '{}' contains invalid characters. Only letters, numbers, and underscores are allowed",
            name
        )));
    }

    Ok(())
}

/// Validate a bucket URL for external source configuration.
///
/// Supported URL schemes:
/// - s3:// (AWS S3)
/// - gs:// (Google Cloud Storage)
/// - az:// (Azure Blob - short form)
/// - wasbs:// (Azure Blob)
/// - abfss:// (Azure Data Lake Gen2)
fn validate_bucket_url(url: &str) -> Result<()> {
    const VALID_SCHEMES: &[&str] = &["s3://", "gs://", "az://", "wasbs://", "abfss://"];

    if url.is_empty() {
        return Err(AppError::Validation(
            "Bucket URL cannot be empty".to_string(),
        ));
    }

    if !VALID_SCHEMES.iter().any(|scheme| url.starts_with(scheme)) {
        return Err(AppError::Validation(format!(
            "Invalid bucket URL scheme. Must start with one of: {}",
            VALID_SCHEMES.join(", ")
        )));
    }

    // Basic check for path traversal attempts
    if url.contains("..") {
        return Err(AppError::Validation(
            "Bucket URL cannot contain path traversal sequences (..)".to_string(),
        ));
    }

    Ok(())
}

/// Validate mutable_refresh value.
///
/// Valid values: on_query, hourly, daily
fn validate_mutable_refresh(value: &str) -> Result<&str> {
    const VALID_VALUES: &[&str] = &["on_query", "hourly", "daily", ""];

    if !VALID_VALUES.contains(&value) {
        return Err(AppError::Validation(format!(
            "Invalid mutable_refresh value '{}'. Must be one of: on_query, hourly, daily",
            value
        )));
    }

    if value.is_empty() {
        Ok("on_query")
    } else {
        Ok(value)
    }
}

/// Convert API request config to internal ExternalSourceConfig.
fn convert_external_config(req: &ExternalSourceConfigRequest) -> Result<serde_json::Value> {
    let table_format = match req.table_format.as_str() {
        "auto" => "auto",
        "raw_parquet" | "" => "raw_parquet",
        "iceberg" => "iceberg",
        "delta_lake" => "delta_lake",
        other => {
            return Err(AppError::Validation(format!(
                "Invalid table_format: {}. Must be one of: auto, raw_parquet, iceberg, delta_lake",
                other
            )));
        }
    };

    // Validate all column names
    for col in &req.index_columns {
        validate_column_name(&col.name)?;
    }

    // Validate time_column if present
    if let Some(ref time_col) = req.time_column {
        validate_column_name(time_col)?;
    }

    // Validate mutable_refresh
    let mutable_refresh = validate_mutable_refresh(&req.refresh.mutable_refresh)?;

    let index_columns: Vec<serde_json::Value> = req
        .index_columns
        .iter()
        .map(|col| {
            let mut obj = serde_json::json!({
                "name": col.name,
            });
            if let Some(ref card) = col.cardinality {
                obj["cardinality"] = serde_json::json!(card);
            }
            if let Some(ref strategy) = col.force_strategy {
                obj["force_strategy"] = serde_json::json!(strategy);
            }
            obj
        })
        .collect();

    let mutability = match &req.mutability {
        MutabilityRequest::AllImmutable => serde_json::json!({ "type": "all_immutable" }),
        MutabilityRequest::AllMutable => serde_json::json!({ "type": "all_mutable" }),
        MutabilityRequest::RollingWindow { window, unit } => {
            serde_json::json!({
                "type": "rolling_window",
                "window": window,
                "unit": unit,
            })
        }
        MutabilityRequest::FileAge { hours } => {
            serde_json::json!({
                "type": "file_age",
                "hours": hours,
            })
        }
        MutabilityRequest::Default => {
            serde_json::json!({
                "type": "rolling_window",
                "window": 1,
                "unit": "day",
            })
        }
    };

    Ok(serde_json::json!({
        "table_format": table_format,
        "index_columns": index_columns,
        "time_column": req.time_column,
        "partition_pattern": req.partition_pattern,
        "mutability": mutability,
        "refresh": {
            "mutable_refresh": mutable_refresh,
            "auto_discover": req.refresh.auto_discover,
        },
    }))
}

/// Convert internal config to API response.
fn convert_to_config_response(config: &serde_json::Value) -> Result<ExternalSourceConfigResponse> {
    let table_format = config
        .get("table_format")
        .and_then(|v| v.as_str())
        .unwrap_or("raw_parquet")
        .to_string();

    let index_columns: Vec<IndexColumnResponse> = config
        .get("index_columns")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|col| {
                    Some(IndexColumnResponse {
                        name: col.get("name")?.as_str()?.to_string(),
                        cardinality: col
                            .get("cardinality")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        force_strategy: col
                            .get("force_strategy")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        actual_strategy: "auto".to_string(), // Would be computed from data
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    let mutability_obj = config.get("mutability").cloned().unwrap_or_default();
    let mutability = MutabilityResponse {
        strategy_type: mutability_obj
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("rolling_window")
            .to_string(),
        window: mutability_obj
            .get("window")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32),
        unit: mutability_obj
            .get("unit")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        hours: mutability_obj
            .get("hours")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32),
    };

    let refresh_obj = config.get("refresh").cloned().unwrap_or_default();
    let refresh = RefreshResponse {
        mutable_refresh: refresh_obj
            .get("mutable_refresh")
            .and_then(|v| v.as_str())
            .unwrap_or("on_query")
            .to_string(),
        auto_discover: refresh_obj
            .get("auto_discover")
            .and_then(|v| v.as_bool())
            .unwrap_or(true),
    };

    Ok(ExternalSourceConfigResponse {
        table_format,
        detected_format: None,
        index_columns,
        time_column: config
            .get("time_column")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        partition_pattern: config
            .get("partition_pattern")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        mutability,
        refresh,
    })
}

/// Maximum result size in bytes (50MB) - prevents OOM for large query results.
const MAX_RESULT_SIZE_BYTES: usize = 50 * 1024 * 1024;

/// MIME type for Arrow IPC streaming format.
const ARROW_STREAM_CONTENT_TYPE: &str = "application/vnd.apache.arrow.stream";

/// Check whether the client requests Arrow IPC format via the Accept header
/// or `?format=arrow` query parameter.
fn wants_arrow_format(headers: &HeaderMap, query_params: &Option<QueryFormatParams>) -> bool {
    if let Some(params) = query_params {
        if params.format.as_deref() == Some("arrow") {
            return true;
        }
    }
    if let Some(accept) = headers.get("accept").and_then(|v| v.to_str().ok()) {
        return accept.contains(ARROW_STREAM_CONTENT_TYPE);
    }
    false
}

/// Encode Arrow RecordBatches as an IPC stream and return a binary response.
fn arrow_ipc_response(
    batches: Vec<arrow::record_batch::RecordBatch>,
    execution_time_ms: u64,
    row_count: usize,
) -> Result<axum::response::Response> {
    use axum::response::IntoResponse;

    let ipc_bytes = encode_arrow_ipc(&batches)?;

    let et = execution_time_ms.to_string();
    let rc = row_count.to_string();
    Ok((
        StatusCode::OK,
        [
            ("content-type", ARROW_STREAM_CONTENT_TYPE),
            ("x-execution-time-ms", et.as_str()),
            ("x-row-count", rc.as_str()),
        ],
        ipc_bytes,
    )
        .into_response())
}

/// Serialize Arrow RecordBatches into IPC streaming bytes.
fn encode_arrow_ipc(batches: &[arrow::record_batch::RecordBatch]) -> Result<Vec<u8>> {
    use arrow::ipc::writer::StreamWriter;

    let schema = if batches.is_empty() {
        std::sync::Arc::new(arrow::datatypes::Schema::empty())
    } else {
        batches[0].schema()
    };

    let mut buf = Vec::with_capacity(64 * 1024);
    let mut writer = StreamWriter::try_new(&mut buf, &schema)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Arrow IPC init failed: {}", e)))?;
    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Arrow IPC write failed: {}", e)))?;
    }
    writer
        .finish()
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Arrow IPC finish failed: {}", e)))?;
    Ok(buf)
}

/// Convert a JSON `QueryResponse` to Arrow IPC format.
///
/// Rebuilds Arrow RecordBatches from the JSON row data, then encodes as IPC.
/// Used when the result was cached or came from a path that already produced JSON.
fn json_response_to_arrow_ipc(resp: &QueryResponse) -> Result<axum::response::Response> {
    use arrow::array::{
        ArrayRef, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int16Builder,
        Int32Builder, Int64Builder, Int8Builder, StringBuilder, TimestampMicrosecondBuilder,
        TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder,
        UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
    };
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;

    if resp.rows.is_empty() || resp.columns.is_empty() {
        return arrow_ipc_response(vec![], resp.execution_time_ms, resp.row_count);
    }

    let num_rows = resp.rows.len();
    let mut fields = Vec::with_capacity(resp.columns.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(resp.columns.len());

    for (col_idx, col_info) in resp.columns.iter().enumerate() {
        let (arrow_type, _nullable) =
            crate::warehouse::ch_type_parser::ch_type_to_arrow(&col_info.data_type);

        macro_rules! build_signed {
            ($builder:ty, $dt:expr) => {{
                let mut b = <$builder>::with_capacity(num_rows);
                for row in &resp.rows {
                    match row.get(col_idx) {
                        Some(serde_json::Value::Number(n)) => {
                            b.append_value(
                                n.as_i64()
                                    .unwrap_or_else(|| n.as_f64().unwrap_or(0.0) as i64)
                                    as _,
                            );
                        }
                        Some(serde_json::Value::Null) | None => b.append_null(),
                        _ => b.append_null(),
                    }
                }
                fields.push(Field::new(&col_info.name, $dt, true));
                arrays.push(std::sync::Arc::new(b.finish()));
            }};
        }

        macro_rules! build_unsigned {
            ($builder:ty, $dt:expr) => {{
                let mut b = <$builder>::with_capacity(num_rows);
                for row in &resp.rows {
                    match row.get(col_idx) {
                        Some(serde_json::Value::Number(n)) => {
                            b.append_value(
                                n.as_u64()
                                    .unwrap_or_else(|| n.as_f64().unwrap_or(0.0) as u64)
                                    as _,
                            );
                        }
                        Some(serde_json::Value::Null) | None => b.append_null(),
                        _ => b.append_null(),
                    }
                }
                fields.push(Field::new(&col_info.name, $dt, true));
                arrays.push(std::sync::Arc::new(b.finish()));
            }};
        }

        macro_rules! build_float {
            ($builder:ty, $dt:expr) => {{
                let mut b = <$builder>::with_capacity(num_rows);
                for row in &resp.rows {
                    match row.get(col_idx) {
                        Some(serde_json::Value::Number(n)) => {
                            b.append_value(n.as_f64().unwrap_or(0.0) as _);
                        }
                        Some(serde_json::Value::Null) | None => b.append_null(),
                        _ => b.append_null(),
                    }
                }
                fields.push(Field::new(&col_info.name, $dt, true));
                arrays.push(std::sync::Arc::new(b.finish()));
            }};
        }

        match &arrow_type {
            DataType::Boolean => {
                let mut b = BooleanBuilder::with_capacity(num_rows);
                for row in &resp.rows {
                    match row.get(col_idx) {
                        Some(serde_json::Value::Bool(v)) => b.append_value(*v),
                        Some(serde_json::Value::Number(n)) => {
                            b.append_value(n.as_u64().unwrap_or(0) != 0)
                        }
                        Some(serde_json::Value::Null) | None => b.append_null(),
                        _ => b.append_null(),
                    }
                }
                fields.push(Field::new(&col_info.name, DataType::Boolean, true));
                arrays.push(std::sync::Arc::new(b.finish()));
            }
            DataType::Int8 => build_signed!(Int8Builder, DataType::Int8),
            DataType::Int16 => build_signed!(Int16Builder, DataType::Int16),
            DataType::Int32 => build_signed!(Int32Builder, DataType::Int32),
            DataType::Int64 => build_signed!(Int64Builder, DataType::Int64),
            DataType::UInt8 => build_unsigned!(UInt8Builder, DataType::UInt8),
            DataType::UInt16 => build_unsigned!(UInt16Builder, DataType::UInt16),
            DataType::UInt32 => build_unsigned!(UInt32Builder, DataType::UInt32),
            DataType::UInt64 => build_unsigned!(UInt64Builder, DataType::UInt64),
            DataType::Float32 => build_float!(Float32Builder, DataType::Float32),
            DataType::Float64 => build_float!(Float64Builder, DataType::Float64),
            DataType::Timestamp(unit, tz) => {
                let ts_type = DataType::Timestamp(unit.clone(), tz.clone());
                match unit {
                    TimeUnit::Second => {
                        let mut b = TimestampSecondBuilder::with_capacity(num_rows);
                        for row in &resp.rows {
                            match row.get(col_idx) {
                                Some(serde_json::Value::Number(n)) => {
                                    b.append_value(n.as_i64().unwrap_or(0));
                                }
                                Some(serde_json::Value::String(s)) => {
                                    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(
                                        s,
                                        "%Y-%m-%d %H:%M:%S",
                                    ) {
                                        b.append_value(dt.and_utc().timestamp());
                                    } else {
                                        b.append_null();
                                    }
                                }
                                _ => b.append_null(),
                            }
                        }
                        fields.push(Field::new(&col_info.name, ts_type, true));
                        arrays.push(std::sync::Arc::new(b.finish()));
                    }
                    TimeUnit::Millisecond => {
                        let mut b = TimestampMillisecondBuilder::with_capacity(num_rows);
                        for row in &resp.rows {
                            match row.get(col_idx) {
                                Some(serde_json::Value::Number(n)) => {
                                    b.append_value(n.as_i64().unwrap_or(0));
                                }
                                Some(serde_json::Value::String(s)) => {
                                    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(
                                        s,
                                        "%Y-%m-%d %H:%M:%S%.3f",
                                    ) {
                                        b.append_value(dt.and_utc().timestamp_millis());
                                    } else {
                                        b.append_null();
                                    }
                                }
                                _ => b.append_null(),
                            }
                        }
                        fields.push(Field::new(&col_info.name, ts_type, true));
                        arrays.push(std::sync::Arc::new(b.finish()));
                    }
                    TimeUnit::Microsecond => {
                        let mut b = TimestampMicrosecondBuilder::with_capacity(num_rows);
                        for row in &resp.rows {
                            match row.get(col_idx) {
                                Some(serde_json::Value::Number(n)) => {
                                    b.append_value(n.as_i64().unwrap_or(0));
                                }
                                Some(serde_json::Value::String(s)) => {
                                    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(
                                        s,
                                        "%Y-%m-%d %H:%M:%S%.6f",
                                    ) {
                                        b.append_value(dt.and_utc().timestamp_micros());
                                    } else {
                                        b.append_null();
                                    }
                                }
                                _ => b.append_null(),
                            }
                        }
                        fields.push(Field::new(&col_info.name, ts_type, true));
                        arrays.push(std::sync::Arc::new(b.finish()));
                    }
                    TimeUnit::Nanosecond => {
                        let mut b = TimestampNanosecondBuilder::with_capacity(num_rows);
                        for row in &resp.rows {
                            match row.get(col_idx) {
                                Some(serde_json::Value::Number(n)) => {
                                    b.append_value(n.as_i64().unwrap_or(0));
                                }
                                Some(serde_json::Value::String(s)) => {
                                    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(
                                        s,
                                        "%Y-%m-%d %H:%M:%S%.9f",
                                    ) {
                                        b.append_value(
                                            dt.and_utc().timestamp_nanos_opt().unwrap_or(0),
                                        );
                                    } else {
                                        b.append_null();
                                    }
                                }
                                _ => b.append_null(),
                            }
                        }
                        fields.push(Field::new(&col_info.name, ts_type, true));
                        arrays.push(std::sync::Arc::new(b.finish()));
                    }
                }
            }
            DataType::Date32 => {
                let mut b = Date32Builder::with_capacity(num_rows);
                for row in &resp.rows {
                    match row.get(col_idx) {
                        Some(serde_json::Value::Number(n)) => {
                            b.append_value(n.as_i64().unwrap_or(0) as i32);
                        }
                        Some(serde_json::Value::String(s)) => {
                            if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                                b.append_value((d - epoch).num_days() as i32);
                            } else {
                                b.append_null();
                            }
                        }
                        _ => b.append_null(),
                    }
                }
                fields.push(Field::new(&col_info.name, DataType::Date32, true));
                arrays.push(std::sync::Arc::new(b.finish()));
            }
            _ => {
                let mut b = StringBuilder::with_capacity(num_rows, num_rows * 32);
                for row in &resp.rows {
                    match row.get(col_idx) {
                        Some(serde_json::Value::String(s)) => b.append_value(s),
                        Some(serde_json::Value::Null) | None => b.append_null(),
                        Some(other) => b.append_value(other.to_string()),
                    }
                }
                fields.push(Field::new(&col_info.name, DataType::Utf8, true));
                arrays.push(std::sync::Arc::new(b.finish()));
            }
        }
    }

    let schema = std::sync::Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, arrays)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to build RecordBatch: {}", e)))?;

    arrow_ipc_response(vec![batch], resp.execution_time_ms, resp.row_count)
}

/// Execute a warehouse query.
///
/// This endpoint:
/// 1. Validates and parses the SQL query
/// 2. Checks the query cache for cached results
/// 3. Loads table metadata for the project
/// Load project tables, validate access, and rewrite the SQL query for the
/// appropriate storage backend (hot ClickHouse tables or warm
/// R2/S3 Parquet files with skip index optimization).
#[tracing::instrument(name = "warehouse.internal.validate_and_rewrite_query", skip_all)]
async fn validate_and_rewrite_query(
    state: &Arc<PondState>,
    project_id: Uuid,
    sql: &str,
) -> Result<crate::warehouse::query::rewriter::RewriteOutput> {
    use sqlparser::dialect::ClickHouseDialect;
    use sqlparser::parser::Parser;

    let info = get_project_table_info(state, project_id).await?;
    let tables = &info.warm_tables;
    let hot_tables = &info.hot_tables;

    if tables.is_empty() && hot_tables.is_empty() {
        return Err(AppError::BadRequest(
            "No warehouse tables configured for this project. Add a data source and sync data first, or use cold tier for federated queries.".to_string()
        ));
    }

    let rewriter = &*state.table_rewriter;

    crate::warehouse::query::rewriter::TableRewriter::validate_table_access(tables, project_id)
        .map_err(convert_rewrite_error)?;

    // Normalize CROSS JOIN + WHERE equi-join patterns into INNER JOIN ON
    // before the main rewrite pass.
    let sql =
        &crate::warehouse::query::normalize_cross_joins(sql).unwrap_or_else(|_| sql.to_string());

    let dialect = ClickHouseDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| AppError::BadRequest(format!("Query parse error: {}", e)))?;

    let referenced_tables =
        crate::warehouse::query::rewriter::TableRewriter::extract_tables_from_ast(&statements);
    let missing: Vec<&String> = referenced_tables
        .iter()
        .filter(|t| !tables.contains_key(t.as_str()) && !hot_tables.contains_key(t.as_str()))
        .collect();

    if !missing.is_empty() {
        return Err(AppError::BadRequest(format!(
            "Table(s) not found: {}. Available tables: {}",
            missing
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            tables
                .keys()
                .chain(hot_tables.keys())
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }

    let all_hot = referenced_tables.iter().all(|t| hot_tables.contains_key(t));
    let any_hot = referenced_tables.iter().any(|t| hot_tables.contains_key(t));

    if any_hot && !all_hot {
        return Err(AppError::BadRequest(
            "Cannot mix hot and warm tables in the same query.".to_string(),
        ));
    }

    if all_hot {
        let hot_sql = rewrite_hot_query(sql, hot_tables)
            .map_err(|e| AppError::BadRequest(format!("Query rewrite error: {}", e)))?;
        return Ok(crate::warehouse::query::rewriter::RewriteOutput {
            sql: hot_sql,
            pruning_stats: None,
        });
    }

    rewrite_warm_query(state, project_id, &mut statements, tables, rewriter).await
}

/// Validate table access and rewrite a SQL query for the NL query pipeline.
///
/// This is a public wrapper around `validate_and_rewrite_query` for use by
/// the natural language query module. It ensures NL-generated SQL goes through
/// the same security checks as user-submitted SQL.
#[tracing::instrument(name = "warehouse.internal.validate_and_rewrite_nl_query", skip_all)]
pub async fn validate_and_rewrite_nl_query(
    state: &Arc<PondState>,
    project_id: Uuid,
    sql: &str,
) -> Result<String> {
    validate_and_rewrite_query(state, project_id, sql)
        .await
        .map(|o| o.sql)
}

/// Rewrite SQL for warm tables using skip indexes or partition pruning.
///
/// Accepts pre-parsed `statements` (parse-once pipeline). Tables are rewritten
/// to `s3()` function calls pointing at Parquet files on R2.
#[tracing::instrument(name = "warehouse.internal.rewrite_warm_query", skip_all)]
async fn rewrite_warm_query(
    state: &Arc<PondState>,
    project_id: Uuid,
    statements: &mut Vec<sqlparser::ast::Statement>,
    tables: &AHashMap<String, crate::warehouse::types::R2TablePath>,
    rewriter: &crate::warehouse::query::rewriter::TableRewriter,
) -> Result<crate::warehouse::query::rewriter::RewriteOutput> {
    if let Some(output) =
        try_rewrite_with_skip_indexes(state, project_id, statements, tables, rewriter).await?
    {
        return Ok(output);
    }

    let date_predicates =
        crate::warehouse::query::rewriter::TableRewriter::extract_date_predicates_from_ast(
            statements,
        );

    rewriter
        .rewrite_warm_query_ast(statements, tables, &date_predicates, None, None)
        .map_err(|e| AppError::BadRequest(format!("Query rewrite error: {}", e)))
}

/// Attempt to rewrite using skip indexes. Returns `None` if indexes are unavailable.
///
/// If the project's indexes are not in cache (new project after startup, or
/// dirty flag set), loads them synchronously from R2/disk cache before the
/// query executes. No query ever falls through to the non-indexed path
/// unless R2 is unreachable.
async fn try_rewrite_with_skip_indexes(
    state: &Arc<PondState>,
    project_id: Uuid,
    statements: &mut Vec<sqlparser::ast::Statement>,
    tables: &AHashMap<String, crate::warehouse::types::R2TablePath>,
    rewriter: &crate::warehouse::query::rewriter::TableRewriter,
) -> Result<Option<crate::warehouse::query::rewriter::RewriteOutput>> {
    // If the project's cache was invalidated (e.g. by a sync that built new
    // inline indexes), synchronously reload from R2/disk cache.
    let needs_reload = state.skip_index_dirty.remove(&project_id).is_some();

    if needs_reload {
        if let (Some(r2), Some(dc)) = (&state.r2_storage, &state.disk_index_cache) {
            match load_project_skip_indexes(&state.db, r2, dc, project_id).await {
                Ok(indexes) => {
                    let mut cache = state.warehouse_skip_indexes.write().await;
                    cache.insert(project_id, indexes);
                }
                Err(e) => {
                    tracing::warn!(
                        project_id = %project_id,
                        error = %e,
                        "Failed to reload dirty skip indexes, continuing without"
                    );
                }
            }
        }
    }

    // Extract predicates once from the pre-parsed AST for both cache-hit
    // and cache-miss code paths.
    let date_predicates =
        crate::warehouse::query::rewriter::TableRewriter::extract_date_predicates_from_ast(
            statements,
        );
    let skip_predicates =
        crate::warehouse::query::rewriter::TableRewriter::extract_skip_predicates_from_ast(
            statements,
        );

    {
        let guard = match tokio::time::timeout(
            std::time::Duration::from_millis(50),
            state.warehouse_skip_indexes.read(),
        )
        .await
        {
            Ok(g) => g,
            Err(_) => {
                tracing::warn!(
                    project_id = %project_id,
                    "Skip index lock contended for >50ms, proceeding without indexes"
                );
                return Ok(None);
            }
        };

        if let Some(indexes) = guard.get(&project_id) {
            if !indexes.is_empty() {
                let output = rewriter
                    .rewrite_warm_query_ast(
                        statements,
                        tables,
                        &date_predicates,
                        Some(indexes),
                        Some(&skip_predicates),
                    )
                    .map_err(|e| AppError::BadRequest(format!("Query rewrite error: {}", e)))?;

                record_pruning_stats(state, &output);

                return Ok(Some(output));
            }
        }
    }

    // Cache miss: synchronous load for new projects
    if let (Some(r2), Some(dc)) = (&state.r2_storage, &state.disk_index_cache) {
        match load_project_skip_indexes(&state.db, r2, dc, project_id).await {
            Ok(indexes) if !indexes.is_empty() => {
                let mut cache = state.warehouse_skip_indexes.write().await;
                cache.insert(project_id, indexes);
                drop(cache);

                if let Ok(guard) = state.warehouse_skip_indexes.try_read() {
                    if let Some(indexes) = guard.get(&project_id) {
                        let output = rewriter
                            .rewrite_warm_query_ast(
                                statements,
                                tables,
                                &date_predicates,
                                Some(indexes),
                                Some(&skip_predicates),
                            )
                            .map_err(|e| {
                                AppError::BadRequest(format!("Query rewrite error: {}", e))
                            })?;

                        record_pruning_stats(state, &output);

                        return Ok(Some(output));
                    }
                }
            }
            Ok(_) => {} // Empty indexes
            Err(e) => {
                tracing::warn!(
                    project_id = %project_id,
                    error = %e,
                    "Failed to load skip indexes for new project, query proceeds without indexes"
                );
            }
        }
    }

    Ok(None)
}

/// Record pruning metrics from the rewriter output without re-evaluating indexes.
fn record_pruning_stats(
    state: &Arc<PondState>,
    output: &crate::warehouse::query::rewriter::RewriteOutput,
) {
    if let Some(ref stats) = output.pruning_stats {
        let scanned = stats.files_after_pruning as u64;
        let pruned = (stats.total_files as u64).saturating_sub(scanned);
        state
            .warehouse_metrics
            .record_skip_index_lookup(pruned, scanned);
    }
}

/// 4. Rewrites table references to s3() function calls
/// 5. Executes the query via ClickHouse using streaming
/// 6. Caches results for future queries
/// 7. Returns results with column metadata
///
/// SECURITY: All table references are validated against the project's tables
/// to prevent cross-project data access.
///
/// PERFORMANCE: Uses query caching, skip indexes, partition pruning, and streaming
/// execution with memory budget to optimize for TB-scale workloads.
#[tracing::instrument(
    name = "warehouse.api.query",
    skip(state, fp, headers, req),
    fields(
        %project_id,
        query_length = req.sql.len(),
        warehouse.sql_hash = tracing::field::Empty,
        warehouse.execution_path = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
    ),
)]
async fn execute_query(
    state: State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
    fp: Query<QueryFormatParams>,
    headers: HeaderMap,
    req: Json<QueryRequest>,
) -> Result<axum::response::Response> {
    crate::observability::finalize_warehouse_query_response(
        execute_query_inner(state, Path(project_id), fp, headers, req).await,
    )
}

async fn execute_query_inner(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
    Query(format_params): Query<QueryFormatParams>,
    headers: HeaderMap,
    Json(req): Json<QueryRequest>,
) -> Result<axum::response::Response> {
    use futures::StreamExt;

    let user_id = extract_user_id(&headers)?;
    let arrow_format = wants_arrow_format(&headers, &Some(format_params));

    // Validate query input and get configuration with limits applied
    let query_config = validate_query_request(&req)?;
    let sql_hash = crate::observability::warehouse_sql_hash(&query_config.sql);
    crate::observability::record_warehouse_query_fields(&sql_hash, "pending");

    let query_cache = &state.query_cache;
    if let Ok(Some(cached)) = query_cache.get(project_id, &query_config.sql).await {
        crate::observability::set_warehouse_query_execution_path("cached");
        tracing::debug!(
            project_id = %project_id,
            row_count = cached.row_count,
            original_time_ms = cached.original_execution_time_ms,
            "Returning cached query result"
        );

        let mut metrics = QueryMetrics::new();
        metrics.cache_hit = true;
        metrics.cache_tier = Some("redis".to_string());
        metrics.rows_returned = cached.row_count as u64;
        metrics.log();

        {
            let db = state.db.clone();
            let sql = query_config.sql.clone();
            tokio::spawn(async move {
                if let Err(e) = record_query_usage(&db, project_id, user_id, &sql, 0, 0, true).await
                {
                    tracing::warn!(project_id = %project_id, error = %e, "Failed to record query usage");
                }
            });
        }

        let resp = QueryResponse {
            columns: cached
                .columns
                .into_iter()
                .map(|c| ColumnInfo {
                    name: c.name,
                    data_type: c.data_type,
                })
                .collect(),
            row_count: cached.row_count,
            rows: cached.rows,
            execution_time_ms: 0,
        };
        return if arrow_format {
            json_response_to_arrow_ipc(&resp)
        } else {
            Ok(axum::Json(resp).into_response())
        };
    }

    // Load project table metadata from cache (merges cold source check
    // and table metadata load into a single cached lookup).
    let table_info = get_project_table_info(&state, project_id).await?;

    if table_info.has_cold_sources {
        crate::observability::set_warehouse_query_execution_path("federated");
        let json_resp = execute_federated_query_json(
            &state,
            project_id,
            user_id,
            &query_config.sql,
            query_config.limit as usize,
        )
        .await?;
        return if arrow_format {
            json_response_to_arrow_ipc(&json_resp)
        } else {
            Ok(axum::Json(json_resp).into_response())
        };
    }

    let mut metrics = QueryMetrics::new();

    let rewrite_start = std::time::Instant::now();
    // Validate tables and rewrite SQL for the appropriate storage backend
    let rewrite_output = validate_and_rewrite_query(&state, project_id, &query_config.sql).await?;
    metrics.planning_time_ms = rewrite_start.elapsed().as_millis() as u64;
    if let Some(ref ps) = rewrite_output.pruning_stats {
        metrics.total_files = ps.total_files as u32;
        metrics.files_scanned = ps.files_after_pruning as u32;
    }
    let rewritten_sql = rewrite_output.sql;

    tracing::debug!(
        project_id = %project_id,
        original_sql = %query_config.sql,
        rewritten_sql = %rewritten_sql,
        limit = query_config.limit,
        "Executing warehouse query"
    );

    // Acquire a query permit for concurrency limiting
    // PERFORMANCE: Prevents TB-scale queries from overwhelming ClickHouse
    let _query_permit = state
        .warehouse_query_limiter
        .acquire(project_id)
        .await
        .map_err(convert_limiter_error)?;

    // Use shared query executor from PondState (connection pooling)
    let executor = &state.warehouse_query_executor;

    if !executor.is_configured() {
        return Err(AppError::Internal(anyhow::anyhow!(
            "ClickHouse connection not configured"
        )));
    }

    // Execute the query using streaming to avoid buffering entire result in executor
    let execution_options = crate::warehouse::query::executor::ExecutionOptions {
        limit: Some(query_config.limit),
        timeout_secs: Some(query_config.timeout_secs as u32),
        max_memory_bytes: Some(100 * 1024 * 1024),
    };

    let start_time = std::time::Instant::now();

    let has_warm_backing = !table_info.hot_backing_tables.is_empty();
    let ch_is_down = state
        .ch_down_cache
        .get(&())
        .map_or(false, |since| since.elapsed().as_secs() < 60);

    if ch_is_down && has_warm_backing {
        crate::observability::set_warehouse_query_execution_path("datafusion_warm_backing");
        tracing::info!(
            project_id = %project_id,
            "ClickHouse circuit breaker open, falling back to DataFusion with warm backing"
        );
        let json_resp = execute_warm_backing_via_datafusion(
            &state,
            project_id,
            user_id,
            &query_config.sql,
            &table_info.hot_backing_tables,
            query_config.limit as usize,
        )
        .await?;
        return if arrow_format {
            json_response_to_arrow_ipc(&json_resp.0)
        } else {
            Ok(json_resp.into_response())
        };
    }

    // Arrow format: use native block path to avoid JSON intermediate representation.
    // klickhouse blocks -> Arrow RecordBatch -> IPC bytes (no JSON step).
    // Server-side caching is skipped here because the cache stores JSON rows,
    // not Arrow IPC. Cache hits at the top of this handler already cover
    // repeated queries (converting JSON -> Arrow IPC on the fly).
    if arrow_format {
        crate::observability::set_warehouse_query_execution_path("clickhouse_arrow");
        use crate::warehouse::ch_client::block_to_record_batch;

        let arrow_exec_options = crate::warehouse::query::executor::ExecutionOptions {
            limit: Some(query_config.limit),
            timeout_secs: Some(query_config.timeout_secs as u32),
            max_memory_bytes: None,
        };

        let block_result = executor
            .execute_native_blocks(&rewritten_sql, arrow_exec_options.clone())
            .await;

        let mut block_stream = match block_result {
            Ok(stream) => {
                state.ch_down_cache.remove(&());
                stream
            }
            Err(ref e) if e.is_data_error() && has_warm_backing => {
                tracing::warn!(
                    project_id = %project_id,
                    error = %e,
                    "ClickHouse data error on Arrow path, retrying with warm backing s3() paths"
                );
                let warm_sql = rewrite_for_warm_backing(
                    &state,
                    project_id,
                    &query_config.sql,
                    &table_info.hot_backing_tables,
                )
                .await?;
                executor
                    .execute_native_blocks(&warm_sql, arrow_exec_options)
                    .await
                    .map_err(convert_executor_error)?
            }
            Err(ref e) if e.is_connection_error() && has_warm_backing => {
                state.ch_down_cache.insert((), std::time::Instant::now());
                let json_resp = execute_warm_backing_via_datafusion(
                    &state,
                    project_id,
                    user_id,
                    &query_config.sql,
                    &table_info.hot_backing_tables,
                    query_config.limit as usize,
                )
                .await?;
                return json_response_to_arrow_ipc(&json_resp.0);
            }
            Err(e) => return Err(convert_executor_error(e)),
        };

        let bytes_scanned = block_stream.stats.as_ref().map_or(0, |s| s.read_bytes);
        if let Some(ref stats) = block_stream.stats {
            metrics.rows_scanned = stats.read_rows;
            metrics.bytes_scanned = stats.read_bytes;
        }

        let mut batches: Vec<arrow::record_batch::RecordBatch> = Vec::new();
        let mut total_rows: usize = 0;
        let mut memory_used: usize = 0;

        while let Some(block_result) = block_stream.blocks.next().await {
            let block = block_result.map_err(convert_stream_error)?;
            if block.rows == 0 {
                continue;
            }

            let batch = block_to_record_batch(&block).map_err(|e| {
                AppError::Internal(anyhow::anyhow!("Arrow conversion error: {}", e))
            })?;

            memory_used += batch.get_array_memory_size();
            if memory_used > MAX_RESULT_SIZE_BYTES {
                tracing::warn!(
                    project_id = %project_id,
                    memory_used,
                    max_size = MAX_RESULT_SIZE_BYTES,
                    rows_collected = total_rows,
                    "Arrow query result truncated due to memory limit"
                );
                break;
            }

            total_rows += batch.num_rows();
            batches.push(batch);
        }

        let execution_time_ms = start_time.elapsed().as_millis() as u64;

        metrics.execution_time_ms = execution_time_ms;
        metrics.rows_returned = total_rows as u64;
        metrics.bytes_returned = memory_used as u64;
        metrics.log();

        let analyzer = SlowQueryAnalyzer::default();
        if let Some(suggestions) = analyzer.analyze(&metrics) {
            tracing::warn!(project_id = %project_id, suggestions = ?suggestions, "Query optimization suggestions");
        }

        if let Ok(estimate) = state
            .warehouse_cost_estimator
            .write()
            .estimate(&query_config.sql)
        {
            EstimationAccuracy::compute(&estimate, &metrics).log();
        }

        let bytes_for_billing = if bytes_scanned > 0 {
            bytes_scanned
        } else {
            memory_used as u64
        };

        {
            let db = state.db.clone();
            let sql = query_config.sql.clone();
            tokio::spawn(async move {
                if let Err(e) = record_query_usage(
                    &db,
                    project_id,
                    user_id,
                    &sql,
                    bytes_for_billing,
                    execution_time_ms,
                    false,
                )
                .await
                {
                    tracing::warn!(project_id = %project_id, error = %e, "Failed to record query usage");
                }
            });
        }

        return arrow_ipc_response(batches, execution_time_ms, total_rows);
    }

    crate::observability::set_warehouse_query_execution_path("clickhouse_json");
    // JSON format: use streaming executor for standard JSON response.
    let exec_result = executor
        .execute_streaming(&rewritten_sql, execution_options.clone())
        .await;

    let mut streaming_result = match exec_result {
        Ok(stream) => {
            state.ch_down_cache.remove(&());
            stream
        }
        Err(ref e) if e.is_data_error() && has_warm_backing => {
            tracing::warn!(
                project_id = %project_id,
                error = %e,
                "ClickHouse data error, retrying with warm backing s3() paths"
            );
            let warm_sql = rewrite_for_warm_backing(
                &state,
                project_id,
                &query_config.sql,
                &table_info.hot_backing_tables,
            )
            .await?;
            executor
                .execute_streaming(&warm_sql, execution_options)
                .await
                .map_err(convert_executor_error)?
        }
        Err(ref e) if e.is_connection_error() && has_warm_backing => {
            tracing::warn!(
                project_id = %project_id,
                error = %e,
                "ClickHouse connection error, marking down and falling back to DataFusion"
            );
            state.ch_down_cache.insert((), std::time::Instant::now());
            let json_resp = execute_warm_backing_via_datafusion(
                &state,
                project_id,
                user_id,
                &query_config.sql,
                &table_info.hot_backing_tables,
                query_config.limit as usize,
            )
            .await?;
            return Ok(json_resp.into_response());
        }
        Err(e) => return Err(convert_executor_error(e)),
    };

    // Get actual bytes read from ClickHouse statistics (for accurate billing)
    let clickhouse_bytes_read = streaming_result.bytes_read().unwrap_or(0);

    // Collect results with memory budget tracking
    let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();
    let mut memory_used: usize = 0;
    let mut truncated = false;

    while let Some(row_result) = streaming_result.rows.next().await {
        let row = row_result.map_err(convert_stream_error)?;

        let row_size: usize = row.iter().map(|v| estimate_json_value_memory(v)).sum();

        if memory_used + row_size > MAX_RESULT_SIZE_BYTES {
            truncated = true;
            tracing::warn!(
                project_id = %project_id,
                memory_used = memory_used,
                max_size = MAX_RESULT_SIZE_BYTES,
                rows_collected = rows.len(),
                "Query result truncated due to memory limit"
            );
            break;
        }

        memory_used += row_size;
        rows.push(row);
    }

    let execution_time_ms = start_time.elapsed().as_millis() as u64;

    metrics.execution_time_ms = execution_time_ms;
    metrics.rows_returned = rows.len() as u64;
    metrics.bytes_scanned = clickhouse_bytes_read;
    metrics.bytes_returned = memory_used as u64;
    if let Some(ref stats) = streaming_result.stats {
        metrics.rows_scanned = stats.read_rows;
        metrics.clickhouse_time_ms = (stats.elapsed_seconds * 1000.0) as u64;
    }

    if truncated {
        tracing::info!(
            project_id = %project_id,
            rows_returned = rows.len(),
            bytes_used = memory_used,
            "Result truncated - consider adding more specific filters or LIMIT"
        );
    }

    let bytes_for_billing = if clickhouse_bytes_read > 0 {
        clickhouse_bytes_read
    } else {
        memory_used as u64
    };

    {
        let db = state.db.clone();
        let sql = query_config.sql.clone();
        tokio::spawn(async move {
            if let Err(e) = record_query_usage(
                &db,
                project_id,
                user_id,
                &sql,
                bytes_for_billing,
                execution_time_ms,
                false,
            )
            .await
            {
                tracing::warn!(project_id = %project_id, error = %e, "Failed to record query usage");
            }
        });
    }

    let columns: Vec<ColumnInfo> = streaming_result
        .columns
        .into_iter()
        .map(|c| ColumnInfo {
            name: c.name,
            data_type: c.data_type,
        })
        .collect();

    if !truncated {
        let cached_result = crate::warehouse::query::cache::CachedQueryResult {
            columns: columns
                .iter()
                .map(|c| crate::warehouse::query::cache::CachedColumnInfo {
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                })
                .collect(),
            rows: rows.clone(),
            row_count: rows.len(),
            original_execution_time_ms: execution_time_ms,
            cached_at: chrono::Utc::now(),
            bytes_scanned: bytes_for_billing,
        };

        match serde_json::to_vec(&cached_result) {
            Ok(serialized) => {
                let cache = query_cache.clone();
                let sql = query_config.sql.clone();
                tokio::spawn(async move {
                    if let Err(e) = cache.set_preserialized(project_id, &sql, serialized).await {
                        tracing::warn!(
                            project_id = %project_id,
                            error = %e,
                            "Failed to cache query result"
                        );
                    }
                });
            }
            Err(e) => {
                tracing::warn!(project_id = %project_id, error = %e, "Failed to serialize query result for caching");
            }
        }
    }

    metrics.log();

    let analyzer = SlowQueryAnalyzer::default();
    if let Some(suggestions) = analyzer.analyze(&metrics) {
        tracing::warn!(
            project_id = %project_id,
            suggestions = ?suggestions,
            "Query optimization suggestions"
        );
    }

    if let Ok(estimate) = state
        .warehouse_cost_estimator
        .write()
        .estimate(&query_config.sql)
    {
        EstimationAccuracy::compute(&estimate, &metrics).log();
    }

    Ok(axum::Json(QueryResponse {
        columns,
        row_count: rows.len(),
        rows,
        execution_time_ms,
    })
    .into_response())
}

/// Execute a warehouse query with streaming response using Server-Sent Events.
///
/// This endpoint is optimized for large result sets that would exceed memory limits
/// with the regular query endpoint. Results are streamed row by row using SSE.
///
/// SSE Format:
/// - `event: metadata` - Column info and query metadata (sent first)
/// - `event: row` - Each data row (sent as rows are received)
/// - `event: complete` - Final statistics (sent at end)
/// - `event: error` - Error message (sent on failure)
///
/// PERFORMANCE: Uses streaming execution from ClickHouse to avoid buffering
/// the entire result set in memory. Suitable for queries returning millions of rows.
///
/// SECURITY: Same validation as regular query endpoint - table access is validated
/// against project ownership.
#[tracing::instrument(
    name = "warehouse.api.execute_query_stream",
    skip(state, fp, headers, req),
    fields(
        %project_id,
        query_length = req.sql.len(),
        warehouse.sql_hash = tracing::field::Empty,
        warehouse.execution_path = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
    ),
)]
async fn execute_query_stream(
    state: State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
    fp: Query<QueryFormatParams>,
    headers: HeaderMap,
    req: Json<QueryRequest>,
) -> Result<axum::response::Response> {
    crate::observability::finalize_warehouse_query_response(
        execute_query_stream_inner(state, Path(project_id), fp, headers, req).await,
    )
}

async fn execute_query_stream_inner(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
    Query(format_params): Query<QueryFormatParams>,
    headers: HeaderMap,
    Json(req): Json<QueryRequest>,
) -> Result<axum::response::Response> {
    use axum::response::sse::{Event, Sse};
    use futures::stream::StreamExt;
    use tokio_stream::wrappers::ReceiverStream;

    let user_id = extract_user_id(&headers)?;
    let arrow_format = wants_arrow_format(&headers, &Some(format_params));

    // Validate query input
    let query_config = validate_query_request(&req)?;
    let sql_hash = crate::observability::warehouse_sql_hash(&query_config.sql);
    crate::observability::record_warehouse_query_fields(&sql_hash, "pending");

    let stream_table_info = get_project_table_info(&state, project_id).await?;

    if stream_table_info.has_cold_sources {
        crate::observability::set_warehouse_query_execution_path("stream_unsupported_cold");
        return Err(AppError::BadRequest(
            "Streaming queries are not yet supported for cold sources. Use the non-streaming query endpoint instead.".to_string()
        ));
    }

    let tables = &stream_table_info.warm_tables;

    if tables.is_empty() {
        crate::observability::set_warehouse_query_execution_path("stream_no_tables");
        return Err(AppError::BadRequest(
            "No warehouse tables configured for this project. Add a data source and sync data first, or use cold tier for federated queries.".to_string()
        ));
    }

    let rewriter = &*state.table_rewriter;

    crate::warehouse::query::rewriter::TableRewriter::validate_table_access(tables, project_id)
        .map_err(convert_rewrite_error)?;

    // Parse SQL once for the streaming pipeline
    let mut statements = {
        use sqlparser::dialect::ClickHouseDialect;
        use sqlparser::parser::Parser;
        let dialect = ClickHouseDialect {};
        Parser::parse_sql(&dialect, &query_config.sql)
            .map_err(|e| AppError::BadRequest(format!("Query parse error: {}", e)))?
    };

    // Check for missing tables (zero-parse variant)
    let missing = crate::warehouse::query::rewriter::TableRewriter::find_missing_tables_from_ast(
        &statements,
        &tables,
    );

    if !missing.is_empty() {
        return Err(AppError::BadRequest(format!(
            "Table(s) not found: {}. Available tables: {}",
            missing.into_iter().collect::<Vec<_>>().join(", "),
            tables.keys().cloned().collect::<Vec<_>>().join(", ")
        )));
    }

    // Rewrite the query with skip-index optimization or partition pruning fallback (parse-once)
    let rewrite_output =
        rewrite_warm_query(&state, project_id, &mut statements, tables, &rewriter).await?;
    let rewritten_sql = rewrite_output.sql;

    tracing::debug!(
        project_id = %project_id,
        original_sql = %query_config.sql,
        rewritten_sql = %rewritten_sql,
        "Executing streaming warehouse query"
    );

    // Acquire a query permit
    let query_permit = state
        .warehouse_query_limiter
        .acquire(project_id)
        .await
        .map_err(convert_limiter_error)?;

    let executor = state.warehouse_query_executor.clone();

    if !executor.is_configured() {
        return Err(AppError::Internal(anyhow::anyhow!(
            "ClickHouse connection not configured"
        )));
    }

    let original_sql = query_config.sql.clone();
    let db = state.db.clone();

    if arrow_format {
        crate::observability::set_warehouse_query_execution_path("stream_arrow");
        return execute_query_stream_arrow(
            executor,
            rewritten_sql,
            query_config,
            db,
            project_id,
            user_id,
            original_sql,
            query_permit,
        )
        .await;
    }

    crate::observability::set_warehouse_query_execution_path("stream_sse");
    // Create a channel to send SSE events
    let (tx, rx) =
        tokio::sync::mpsc::channel::<std::result::Result<Event, std::convert::Infallible>>(100);

    // Spawn a task to stream results
    tokio::spawn(stream_query_results(StreamQueryContext {
        executor,
        rewritten_sql,
        query_config,
        tx,
        db,
        project_id,
        user_id,
        original_sql,
        _permit: query_permit,
    }));

    // Return the SSE stream
    let stream = ReceiverStream::new(rx);
    Ok(Sse::new(stream)
        .keep_alive(
            axum::response::sse::KeepAlive::new()
                .interval(std::time::Duration::from_secs(15))
                .text("ping"),
        )
        .into_response())
}

/// Stream query results as Arrow IPC binary chunks.
///
/// Uses `execute_native_blocks` to get raw klickhouse blocks, converts each to
/// an Arrow RecordBatch, and writes the complete IPC stream to a binary response.
/// The client can read the full IPC stream with `arrow.ipc.RecordBatchReader`.
///
/// Includes memory tracking to prevent OOM on unexpectedly large results.
#[tracing::instrument(name = "warehouse.internal.stream_query_arrow", skip_all)]
async fn execute_query_stream_arrow(
    executor: Arc<crate::warehouse::query::executor::QueryExecutor>,
    rewritten_sql: String,
    query_config: QueryConfig,
    db: Arc<crate::db::DbPool>,
    project_id: Uuid,
    user_id: Uuid,
    original_sql: String,
    _permit: crate::warehouse::query::limiter::QueryPermit,
) -> Result<axum::response::Response> {
    use crate::warehouse::ch_client::block_to_record_batch;
    use futures::StreamExt;

    let start_time = std::time::Instant::now();
    let mut metrics = QueryMetrics::new();

    let execution_options = crate::warehouse::query::executor::ExecutionOptions {
        limit: Some(query_config.limit),
        timeout_secs: Some(query_config.timeout_secs as u32),
        max_memory_bytes: Some(MAX_RESULT_SIZE_BYTES),
    };

    let mut block_stream = executor
        .execute_native_blocks(&rewritten_sql, execution_options)
        .await
        .map_err(convert_executor_error)?;

    let bytes_scanned = block_stream.stats.as_ref().map_or(0, |s| s.read_bytes);
    if let Some(ref stats) = block_stream.stats {
        metrics.rows_scanned = stats.read_rows;
        metrics.bytes_scanned = stats.read_bytes;
    }

    let mut batches: Vec<arrow::record_batch::RecordBatch> = Vec::new();
    let mut total_rows: usize = 0;
    let mut memory_used: usize = 0;

    while let Some(block_result) = block_stream.blocks.next().await {
        let block = block_result.map_err(convert_stream_error)?;
        if block.rows == 0 {
            continue;
        }
        let batch = block_to_record_batch(&block)
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Arrow conversion error: {}", e)))?;

        memory_used += batch.get_array_memory_size();
        if memory_used > MAX_RESULT_SIZE_BYTES {
            tracing::warn!(
                project_id = %project_id,
                memory_used,
                max_size = MAX_RESULT_SIZE_BYTES,
                rows_collected = total_rows,
                "Arrow streaming result truncated due to memory limit"
            );
            break;
        }

        total_rows += batch.num_rows();
        batches.push(batch);
    }

    let execution_time_ms = start_time.elapsed().as_millis() as u64;

    metrics.execution_time_ms = execution_time_ms;
    metrics.rows_returned = total_rows as u64;
    metrics.bytes_returned = memory_used as u64;
    metrics.log();

    let analyzer = SlowQueryAnalyzer::default();
    if let Some(suggestions) = analyzer.analyze(&metrics) {
        tracing::warn!(project_id = %project_id, suggestions = ?suggestions, "Query optimization suggestions");
    }

    let bytes_for_billing = if bytes_scanned > 0 {
        bytes_scanned
    } else {
        memory_used as u64
    };

    {
        let db = db.clone();
        let sql = original_sql.clone();
        tokio::spawn(async move {
            if let Err(e) = record_query_usage(
                &db,
                project_id,
                user_id,
                &sql,
                bytes_for_billing,
                execution_time_ms,
                false,
            )
            .await
            {
                tracing::warn!(project_id = %project_id, error = %e, "Failed to record query usage");
            }
        });
    }

    arrow_ipc_response(batches, execution_time_ms, total_rows)
}

/// Context for streaming query execution over SSE.
struct StreamQueryContext {
    executor: Arc<crate::warehouse::query::executor::QueryExecutor>,
    rewritten_sql: String,
    query_config: QueryConfig,
    tx: tokio::sync::mpsc::Sender<
        std::result::Result<axum::response::sse::Event, std::convert::Infallible>,
    >,
    db: Arc<crate::db::DbPool>,
    project_id: Uuid,
    user_id: Uuid,
    original_sql: String,
    _permit: crate::warehouse::query::limiter::QueryPermit,
}

/// Execute a streaming query, sending results as SSE events over the channel.
#[tracing::instrument(name = "warehouse.internal.stream_query_results", skip_all)]
async fn stream_query_results(ctx: StreamQueryContext) {
    use axum::response::sse::Event;
    use futures::StreamExt;

    let start_time = std::time::Instant::now();

    let execution_options = crate::warehouse::query::executor::ExecutionOptions {
        limit: Some(ctx.query_config.limit),
        timeout_secs: Some(ctx.query_config.timeout_secs as u32),
        max_memory_bytes: None,
    };

    let mut streaming_result = match ctx
        .executor
        .execute_streaming(&ctx.rewritten_sql, execution_options)
        .await
    {
        Ok(result) => result,
        Err(e) => {
            let _ = ctx
                .tx
                .send(Ok(Event::default().event("error").data(
                    serde_json::json!({"error": format!("Query execution failed: {}", e)})
                        .to_string(),
                )))
                .await;
            return;
        }
    };

    // Send column metadata
    let metadata = serde_json::json!({
        "columns": streaming_result.columns.iter().map(|c| {
            serde_json::json!({"name": c.name, "data_type": c.data_type})
        }).collect::<Vec<_>>(),
        "query_start": Utc::now().to_rfc3339(),
    });
    if ctx
        .tx
        .send(Ok(Event::default()
            .event("metadata")
            .data(metadata.to_string())))
        .await
        .is_err()
    {
        return;
    }

    // Stream rows
    let mut row_count = 0usize;
    while let Some(row_result) = streaming_result.rows.next().await {
        let row = match row_result {
            Ok(row) => row,
            Err(e) => {
                let _ = ctx
                    .tx
                    .send(Ok(Event::default().event("error").data(
                        serde_json::json!({"error": format!("Row error: {}", e)}).to_string(),
                    )))
                    .await;
                break;
            }
        };

        row_count += 1;
        let row_data = serde_json::to_string(&row).unwrap_or_else(|_| "[]".to_string());
        if ctx
            .tx
            .send(Ok(Event::default().event("row").data(row_data)))
            .await
            .is_err()
        {
            break;
        }
    }

    let execution_time_ms = start_time.elapsed().as_millis() as u64;
    let bytes_read = streaming_result.bytes_read().unwrap_or(0);

    let mut metrics = QueryMetrics::new();
    metrics.execution_time_ms = execution_time_ms;
    metrics.rows_returned = row_count as u64;
    metrics.bytes_scanned = bytes_read;
    if let Some(ref stats) = streaming_result.stats {
        metrics.rows_scanned = stats.read_rows;
        metrics.clickhouse_time_ms = (stats.elapsed_seconds * 1000.0) as u64;
    }
    metrics.log();

    let analyzer = SlowQueryAnalyzer::default();
    if let Some(suggestions) = analyzer.analyze(&metrics) {
        tracing::warn!(
            project_id = %ctx.project_id,
            suggestions = ?suggestions,
            "Streaming query optimization suggestions"
        );
    }

    let completion = serde_json::json!({
        "row_count": row_count,
        "execution_time_ms": execution_time_ms,
        "bytes_read": bytes_read,
    });
    let _ = ctx
        .tx
        .send(Ok(Event::default()
            .event("complete")
            .data(completion.to_string())))
        .await;

    let _ = record_query_usage(
        &ctx.db,
        ctx.project_id,
        ctx.user_id,
        &ctx.original_sql,
        bytes_read,
        execution_time_ms,
        false,
    )
    .await;
}

/// Load warehouse tables for a project with tier information.
///
/// Returns three maps:
/// - warm tables (for s3() rewriting)
/// - hot tables (for native ClickHouse table names)
/// - dedup info (per-table deduplication metadata for merge-on-read)
///
/// Returns all sync-enabled tables regardless of sync_state so that
/// tables remain queryable while syncs are in progress.
#[tracing::instrument(name = "warehouse.internal.load_project_tables_with_tier", skip_all)]
pub async fn load_project_tables_with_tier(
    db: &sqlx::PgPool,
    project_id: Uuid,
) -> Result<(
    AHashMap<String, crate::warehouse::types::R2TablePath>,
    AHashMap<String, String>,
    AHashMap<String, crate::warehouse::types::R2TablePath>,
)> {
    let rows = sqlx::query(
        r#"
        SELECT 
            s.name as source_name,
            s.tier as source_tier,
            t.name as table_name,
            t.r2_prefix,
            t.detected_partition_scheme,
            s.backs_source_id,
            s.global_source_id,
            bg.chain as blockchain_chain
        FROM warehouse_tables t
        JOIN warehouse_sources s ON s.id = t.source_id
        LEFT JOIN blockchain_global_sources bg ON bg.id = s.global_source_id
        WHERE s.project_id = $1 
          AND t.sync_enabled = true
          AND s.backs_source_id IS NULL
        "#,
    )
    .bind(project_id)
    .fetch_all(db)
    .await?;

    let row_count = rows.len();
    let mut warm_tables = AHashMap::with_capacity(row_count);
    let mut hot_tables = AHashMap::with_capacity(row_count);

    for row in rows {
        let source_name: String = row.get("source_name");
        let source_tier: StorageTier = row
            .get::<String, _>("source_tier")
            .parse()
            .unwrap_or_default();
        let table_name: String = row.get("table_name");
        let r2_prefix: String = row.get("r2_prefix");

        let detected_partition_scheme: Option<
            crate::warehouse::indexes::external_config::PartitionStrategy,
        > = row
            .try_get::<Option<serde_json::Value>, _>("detected_partition_scheme")
            .ok()
            .flatten()
            .and_then(|v| serde_json::from_value(v).ok());

        let blockchain_chain: Option<String> = row
            .try_get("blockchain_chain")
            .ok()
            .flatten();

        let full_table_name = format!("{}.{}", source_name, table_name);

        match source_tier {
            StorageTier::Hot => {
                let ch_table_name = crate::warehouse::storage::ClickHouseStorage::source_table_name(
                    project_id,
                    &source_name,
                    &table_name,
                );
                hot_tables.insert(full_table_name, ch_table_name);
            }
            StorageTier::Warm => {
                let buffer_ch_table = blockchain_chain.as_ref().map(|chain| {
                    crate::warehouse::sync::blockchain_sync::buffer_table_name(chain, &table_name)
                });

                let r2_path = crate::warehouse::types::R2TablePath {
                    prefix: r2_prefix,
                    project_id: Some(project_id),
                    date_partitioned: false,
                    partition_column: None,
                    detected_partition_scheme,
                    buffer_ch_table,
                };
                warm_tables.insert(full_table_name, r2_path);
            }
            StorageTier::Cold => {}
        }
    }

    // Load warm backing tables for hot sources (for failover).
    // Uses the *backing source's* name (derived from hot source name) to find
    // its tables, but keys the map using the *hot source's* table names so
    // the failover path can look up backing data by the same key the user query references.
    let backing_rows = sqlx::query(
        r#"
        SELECT 
            hot.name as hot_source_name,
            t.name as table_name,
            t.r2_prefix,
            t.detected_partition_scheme
        FROM warehouse_sources backing
        JOIN warehouse_sources hot ON backing.backs_source_id = hot.id
        JOIN warehouse_tables t ON t.source_id = backing.id
        WHERE hot.project_id = $1
          AND backing.tier = 'warm'
          AND t.sync_enabled = true
        "#,
    )
    .bind(project_id)
    .fetch_all(db)
    .await?;

    let mut hot_backing_tables = AHashMap::with_capacity(backing_rows.len());
    for row in backing_rows {
        let hot_source_name: String = row.get("hot_source_name");
        let table_name: String = row.get("table_name");
        let r2_prefix: String = row.get("r2_prefix");
        let detected_partition_scheme: Option<
            crate::warehouse::indexes::external_config::PartitionStrategy,
        > = row
            .try_get::<Option<serde_json::Value>, _>("detected_partition_scheme")
            .ok()
            .flatten()
            .and_then(|v| serde_json::from_value(v).ok());

        let full_table_name = format!("{}.{}", hot_source_name, table_name);
        let r2_path = crate::warehouse::types::R2TablePath {
            prefix: r2_prefix,
            project_id: Some(project_id),
            date_partitioned: false,
            partition_column: None,
            detected_partition_scheme,
            buffer_ch_table: None,
        };
        hot_backing_tables.insert(full_table_name, r2_path);
    }

    Ok((warm_tables, hot_tables, hot_backing_tables))
}

/// Load project table metadata with cold source presence in a single query,
/// using an in-memory cache to avoid hitting Postgres on every request.
///
/// Returns cached data if:
/// - The cache entry exists and is not expired (60s TTL)
/// - The project has not been marked dirty by a sync
///
/// Otherwise fetches fresh data from Postgres and updates the cache.
#[tracing::instrument(name = "warehouse.internal.get_project_table_info", skip_all)]
pub async fn get_project_table_info(
    state: &Arc<PondState>,
    project_id: Uuid,
) -> Result<crate::app_state::ProjectTableInfo> {
    let is_dirty = state.table_cache_dirty.remove(&project_id).is_some();

    if !is_dirty {
        if let Some(cached) = state.project_table_cache.get(&project_id) {
            if !cached.is_expired() {
                return Ok(cached);
            }
        }
    }

    let (warm_tables, hot_tables, hot_backing_tables) =
        load_project_tables_with_tier(&state.db, project_id).await?;

    let has_cold_sources: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM warehouse_sources WHERE project_id = $1 AND tier = 'cold'",
    )
    .bind(project_id)
    .fetch_one(&*state.db)
    .await?;

    let info = crate::app_state::ProjectTableInfo::new(
        warm_tables,
        hot_tables,
        hot_backing_tables,
        has_cold_sources.0 > 0,
    );

    state.project_table_cache.insert(project_id, info.clone());

    Ok(info)
}

/// Extract column names from the warehouse_tables.schema JSONB.
///
/// The schema is stored as `{"columns": [{"name": "col1", ...}, ...]}`.
/// Returns an empty vec if parsing fails.
pub fn extract_column_names_from_schema(schema_json: &Option<serde_json::Value>) -> Vec<String> {
    let Some(schema) = schema_json else {
        return Vec::new();
    };

    if let Some(columns) = schema.get("columns").and_then(|c| c.as_array()) {
        columns
            .iter()
            .filter_map(|col| {
                col.get("name")
                    .and_then(|n| n.as_str())
                    .map(|s| s.to_string())
            })
            .collect()
    } else {
        Vec::new()
    }
}

/// Rewrite a query for hot sources.
///
/// Replaces table references like `source_name.table_name` with native ClickHouse
/// table names like `default.warehouse_{project_id}_{source_name}_{table_name}`.
fn rewrite_hot_query(
    sql: &str,
    hot_tables: &AHashMap<String, String>,
) -> std::result::Result<String, String> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    let mut statements =
        Parser::parse_sql(&dialect, sql).map_err(|e| format!("SQL parse error: {}", e))?;

    if statements.is_empty() {
        return Err("Empty SQL statement".to_string());
    }

    // Rewrite table references in the AST
    for stmt in &mut statements {
        rewrite_hot_statement(stmt, hot_tables);
    }

    // Convert back to SQL string
    let rewritten: Vec<String> = statements.iter().map(|s| s.to_string()).collect();
    Ok(rewritten.join("; "))
}

/// Recursively rewrite table references in a statement for hot tier.
fn rewrite_hot_statement(
    stmt: &mut sqlparser::ast::Statement,
    hot_tables: &AHashMap<String, String>,
) {
    use sqlparser::ast::Statement;

    if let Statement::Query(query) = stmt {
        rewrite_hot_query_ast(query, hot_tables);
    }
}

fn rewrite_hot_query_ast(query: &mut sqlparser::ast::Query, hot_tables: &AHashMap<String, String>) {
    rewrite_hot_set_expr(&mut query.body, hot_tables);

    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            rewrite_hot_query_ast(&mut cte.query, hot_tables);
        }
    }
}

fn rewrite_hot_set_expr(
    set_expr: &mut sqlparser::ast::SetExpr,
    hot_tables: &AHashMap<String, String>,
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
    hot_tables: &AHashMap<String, String>,
) {
    use sqlparser::ast::{Ident, ObjectName, TableFactor};

    match factor {
        TableFactor::Table { name, .. } => {
            // Get the table name in source.table format
            let table_name = if name.0.len() == 2 {
                format!("{}.{}", name.0[0].value, name.0[1].value)
            } else if name.0.len() == 1 {
                name.0[0].value.clone()
            } else {
                return;
            };

            // Check if this is a hot table
            if let Some(ch_table_name) = hot_tables.get(&table_name) {
                // Replace with native ClickHouse table name: default.warehouse_...
                *name = ObjectName(vec![
                    Ident::new("default"),
                    Ident::with_quote('`', ch_table_name),
                ]);
            }
        }
        TableFactor::Derived { subquery, .. } => {
            rewrite_hot_query_ast(subquery, hot_tables);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            rewrite_hot_table_factor(&mut table_with_joins.relation, hot_tables);
            for join in &mut table_with_joins.joins {
                rewrite_hot_table_factor(&mut join.relation, hot_tables);
            }
        }
        _ => {}
    }
}

/// Rewrite a query so that hot table references point at warm backing R2 paths.
///
/// Used during failover when ClickHouse reports a data error (table missing)
/// but the warm backing source has the data on R2.
pub async fn rewrite_for_warm_backing(
    state: &Arc<PondState>,
    project_id: Uuid,
    sql: &str,
    hot_backing_tables: &AHashMap<String, crate::warehouse::types::R2TablePath>,
) -> Result<String> {
    use sqlparser::dialect::ClickHouseDialect;
    use sqlparser::parser::Parser;

    let sql =
        &crate::warehouse::query::normalize_cross_joins(sql).unwrap_or_else(|_| sql.to_string());

    let dialect = ClickHouseDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| AppError::BadRequest(format!("Query parse error: {}", e)))?;

    let rewriter = &*state.table_rewriter;
    let output = rewrite_warm_query(
        state,
        project_id,
        &mut statements,
        hot_backing_tables,
        rewriter,
    )
    .await?;
    Ok(output.sql)
}

/// Execute a query against warm backing data via DataFusion (bypassing ClickHouse).
///
/// Used when ClickHouse is completely down (connection error). Registers the
/// warm backing R2 Parquet paths with DataFusion and executes through it.
async fn execute_warm_backing_via_datafusion(
    state: &Arc<PondState>,
    project_id: Uuid,
    user_id: Uuid,
    sql: &str,
    hot_backing_tables: &AHashMap<String, crate::warehouse::types::R2TablePath>,
    limit: usize,
) -> Result<Json<QueryResponse>> {
    use crate::warehouse::query::federated_query::FederatedQueryExecutor;

    let r2 = state.r2_storage.as_ref().ok_or_else(|| {
        AppError::Internal(anyhow::anyhow!(
            "R2 not configured for warm backing fallback"
        ))
    })?;

    let r2_config = crate::warehouse::query::federated_query::R2SourceConfig {
        endpoint: r2.endpoint().to_string(),
        bucket: r2.bucket().to_string(),
        access_key_id: r2.access_key_id().to_string(),
        secret_access_key: r2.secret_access_key().to_string(),
        region: None,
    };

    let mut federated = FederatedQueryExecutor::new_for_warm_backing(r2_config, project_id);

    for (table_name, r2_path) in hot_backing_tables {
        federated
            .register_warm_table(table_name, &r2_path.prefix)
            .await
            .map_err(|e| {
                AppError::Internal(anyhow::anyhow!(
                    "Failed to register warm backing table: {}",
                    e
                ))
            })?;
    }

    let start = std::time::Instant::now();
    let batches = federated
        .execute_with_limit(sql, limit)
        .await
        .map_err(|e| {
            AppError::Internal(anyhow::anyhow!(
                "Warm backing DataFusion query failed: {}",
                e
            ))
        })?;

    let mut result = record_batches_to_response(batches, limit).map_err(|e| {
        AppError::Internal(anyhow::anyhow!(
            "Failed to convert warm backing results: {}",
            e
        ))
    })?;
    result.execution_time_ms = start.elapsed().as_millis() as u64;

    {
        let db = state.db.clone();
        let sql = sql.to_string();
        let execution_time_ms = result.execution_time_ms;
        tokio::spawn(async move {
            if let Err(e) =
                record_query_usage(&db, project_id, user_id, &sql, 0, execution_time_ms, false)
                    .await
            {
                tracing::warn!(project_id = %project_id, error = %e, "Failed to record query usage");
            }
        });
    }

    Ok(Json(result))
}

/// Rewrite two-part table references to three-part for DataFusion.
///
/// DataFusion resolves `"catalog".table` as `<default_catalog>."catalog".table`,
/// which fails when "catalog" is a registered non-default catalog.
/// This function detects two-part names whose first part matches a registered
/// catalog and injects `public` as the schema, producing `"catalog".public.table`.
pub(crate) fn rewrite_federated_table_refs(
    sql: &str,
    catalog_names: &ahash::AHashSet<String>,
) -> std::result::Result<String, String> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    let mut statements =
        Parser::parse_sql(&dialect, sql).map_err(|e| format!("SQL parse error: {}", e))?;

    if statements.is_empty() {
        return Err("Empty SQL statement".to_string());
    }

    for stmt in &mut statements {
        rewrite_federated_statement(stmt, catalog_names);
    }

    let rewritten: Vec<String> = statements.iter().map(|s| s.to_string()).collect();
    Ok(rewritten.join("; "))
}

fn rewrite_federated_statement(
    stmt: &mut sqlparser::ast::Statement,
    catalog_names: &ahash::AHashSet<String>,
) {
    use sqlparser::ast::Statement;
    if let Statement::Query(query) = stmt {
        rewrite_federated_query_ast(query, catalog_names);
    }
}

fn rewrite_federated_query_ast(
    query: &mut sqlparser::ast::Query,
    catalog_names: &ahash::AHashSet<String>,
) {
    rewrite_federated_set_expr(&mut query.body, catalog_names);
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            rewrite_federated_query_ast(&mut cte.query, catalog_names);
        }
    }
}

fn rewrite_federated_set_expr(
    set_expr: &mut sqlparser::ast::SetExpr,
    catalog_names: &ahash::AHashSet<String>,
) {
    use sqlparser::ast::SetExpr;
    match set_expr {
        SetExpr::Select(select) => {
            for from in &mut select.from {
                rewrite_federated_table_factor(&mut from.relation, catalog_names);
                for join in &mut from.joins {
                    rewrite_federated_table_factor(&mut join.relation, catalog_names);
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_federated_set_expr(left, catalog_names);
            rewrite_federated_set_expr(right, catalog_names);
        }
        _ => {}
    }
}

fn rewrite_federated_table_factor(
    factor: &mut sqlparser::ast::TableFactor,
    catalog_names: &ahash::AHashSet<String>,
) {
    use sqlparser::ast::{Ident, ObjectName, TableFactor};

    match factor {
        TableFactor::Table { name, .. } => {
            // Only rewrite two-part names where the first part is a known catalog
            if name.0.len() == 2 {
                let first = &name.0[0].value;
                if catalog_names.contains(first) {
                    // Insert "public" as the schema between catalog and table
                    let catalog_ident = name.0[0].clone();
                    let table_ident = name.0[1].clone();
                    *name = ObjectName(vec![catalog_ident, Ident::new("public"), table_ident]);
                }
            }
        }
        TableFactor::Derived { subquery, .. } => {
            rewrite_federated_query_ast(subquery, catalog_names);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            rewrite_federated_table_factor(&mut table_with_joins.relation, catalog_names);
            for join in &mut table_with_joins.joins {
                rewrite_federated_table_factor(&mut join.relation, catalog_names);
            }
        }
        _ => {}
    }
}

/// Execute a federated query against cold sources.
///
/// This function handles queries that reference tables from cold (federated) sources.
/// Executes a federated query across multiple data sources using DataFusion.
///
/// Uses datafusion-table-providers for native PostgreSQL and MySQL support with
/// automatic query pushdown optimization.
#[tracing::instrument(name = "warehouse.internal.execute_federated_query", skip_all)]
async fn execute_federated_query(
    state: &Arc<PondState>,
    project_id: Uuid,
    user_id: Uuid,
    sql: &str,
    limit: usize,
) -> Result<Json<QueryResponse>> {
    use crate::warehouse::query::{
        FederatedQueryExecutor, MySqlSourceConfig, PostgresSourceConfig,
    };
    use crate::warehouse::sources::types::StorageTier;

    let start_time = std::time::Instant::now();

    // Load all sources for this project (all modes)
    let rows = sqlx::query(
        "SELECT id, name, source_type, config, tier FROM warehouse_sources WHERE project_id = $1",
    )
    .bind(project_id)
    .fetch_all(&*state.db)
    .await?;

    if rows.is_empty() {
        return Err(AppError::BadRequest("No sources configured".to_string()));
    }

    // Create the federated query executor
    let mut executor = FederatedQueryExecutor::new();

    // Register each source as a catalog with its tier
    for row in &rows {
        let name: String = row.get("name");
        let source_type_str: String = row.get("source_type");
        let encrypted_config: serde_json::Value = row.get("config");
        let tier_str: String = row.get("tier");

        let source_type: SourceType = source_type_str
            .parse()
            .unwrap_or(SourceType::ExternalParquet);
        let tier: StorageTier = tier_str.parse().unwrap_or(StorageTier::Cold);
        let config = decrypt_source_config(&encrypted_config, &state.encryptor)?;

        register_federated_source(&mut executor, &name, source_type, &config, tier).await?;
    }

    // Rewrite two-part table references (e.g. "catalog".table) to three-part
    // (e.g. "catalog".public.table) so DataFusion resolves the correct catalog
    // instead of treating the catalog name as a schema in the default "datafusion" catalog.
    let catalog_names: ahash::AHashSet<String> = executor.list_catalogs().iter().cloned().collect();
    let rewritten_sql =
        rewrite_federated_table_refs(sql, &catalog_names).unwrap_or_else(|_| sql.to_string());

    // Convert CROSS JOIN + WHERE equi-join to INNER JOIN ON so that
    // downstream planning can apply semi-join optimization and better
    // cost estimates.
    let rewritten_sql =
        crate::warehouse::query::normalize_cross_joins(&rewritten_sql).unwrap_or(rewritten_sql);

    tracing::info!(
        original_sql = %sql,
        rewritten_sql = %rewritten_sql,
        catalogs = ?executor.list_catalogs(),
        "Executing federated query"
    );

    // Execute the query with limit
    let result_batches = executor
        .execute_with_limit(&rewritten_sql, limit)
        .await
        .map_err(|e| AppError::BadRequest(format!("Query execution failed: {}", e)))?;

    let mut result =
        record_batches_to_response(result_batches, limit).map_err(|e| AppError::BadRequest(e))?;

    let execution_time_ms = start_time.elapsed().as_millis() as u64;
    result.execution_time_ms = execution_time_ms;

    let mut metrics = QueryMetrics::new();
    metrics.federation_source_count = rows.len() as u32;
    metrics.execution_time_ms = execution_time_ms;
    metrics.rows_returned = result.row_count as u64;
    metrics.log();

    let analyzer = SlowQueryAnalyzer::default();
    if let Some(suggestions) = analyzer.analyze(&metrics) {
        tracing::warn!(
            project_id = %project_id,
            suggestions = ?suggestions,
            "Federated query optimization suggestions"
        );
    }

    // Record query usage
    record_query_usage(
        &state.db,
        project_id,
        user_id,
        sql,
        0,
        execution_time_ms,
        false,
    )
    .await?;

    Ok(Json(result))
}

/// Execute a federated query and return the raw `QueryResponse` (not wrapped in Json).
/// Used by `execute_query` to conditionally format as JSON or Arrow IPC.
#[tracing::instrument(name = "warehouse.internal.execute_federated_query_json", skip_all)]
async fn execute_federated_query_json(
    state: &Arc<PondState>,
    project_id: Uuid,
    user_id: Uuid,
    sql: &str,
    limit: usize,
) -> Result<QueryResponse> {
    let resp = execute_federated_query(state, project_id, user_id, sql, limit).await?;
    Ok(resp.0)
}

/// Register a single source with the federated query executor.
#[tracing::instrument(name = "warehouse.internal.register_federated_source", skip_all)]
async fn register_federated_source(
    executor: &mut crate::warehouse::query::FederatedQueryExecutor,
    name: &str,
    source_type: SourceType,
    config: &serde_json::Value,
    tier: crate::warehouse::sources::types::StorageTier,
) -> Result<()> {
    use crate::warehouse::query::{MySqlSourceConfig, PostgresSourceConfig};

    match source_type {
        SourceType::PostgreSQL => {
            let pg_config = PostgresSourceConfig {
                host: config
                    .get("host")
                    .and_then(|v| v.as_str())
                    .unwrap_or("localhost")
                    .to_string(),
                port: config.get("port").and_then(|v| v.as_u64()).unwrap_or(5432) as u16,
                database: config
                    .get("database")
                    .and_then(|v| v.as_str())
                    .unwrap_or("postgres")
                    .to_string(),
                user: config
                    .get("username")
                    .and_then(|v| v.as_str())
                    .unwrap_or("postgres")
                    .to_string(),
                password: config
                    .get("password")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                sslmode: config
                    .get("sslmode")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
            };
            executor
                .register_postgres_with_tier(name, pg_config, tier)
                .await
                .map_err(|e| {
                    AppError::BadRequest(format!(
                        "Failed to register PostgreSQL source '{}': {}",
                        name, e
                    ))
                })?;
        }
        SourceType::MySQL => {
            let mysql_config = MySqlSourceConfig {
                host: config
                    .get("host")
                    .and_then(|v| v.as_str())
                    .unwrap_or("localhost")
                    .to_string(),
                port: config.get("port").and_then(|v| v.as_u64()).unwrap_or(3306) as u16,
                database: config
                    .get("database")
                    .and_then(|v| v.as_str())
                    .unwrap_or("mysql")
                    .to_string(),
                user: config
                    .get("username")
                    .and_then(|v| v.as_str())
                    .unwrap_or("root")
                    .to_string(),
                password: config
                    .get("password")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                sslmode: config
                    .get("sslmode")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
            };
            executor
                .register_mysql_with_tier(name, mysql_config, tier)
                .await
                .map_err(|e| {
                    AppError::BadRequest(format!(
                        "Failed to register MySQL source '{}': {}",
                        name, e
                    ))
                })?;
        }
        other => {
            tracing::warn!(source_type = %other, name = %name, "Skipping unsupported source type in federated query");
        }
    }
    Ok(())
}

/// Convert Arrow RecordBatches to a QueryResponse with optional limit.
///
/// Uses `arrow::json::writer::ArrayWriter` for zero-downcast serialization.
pub(crate) fn record_batches_to_response(
    batches: Vec<arrow::record_batch::RecordBatch>,
    limit: usize,
) -> std::result::Result<QueryResponse, String> {
    if batches.is_empty() {
        return Ok(QueryResponse {
            columns: vec![],
            row_count: 0,
            rows: vec![],
            execution_time_ms: 0,
        });
    }

    let schema = batches[0].schema();
    let columns: Vec<ColumnInfo> = schema
        .fields()
        .iter()
        .map(|f| ColumnInfo {
            name: f.name().clone(),
            data_type: format!("{:?}", f.data_type()),
        })
        .collect();
    let column_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

    let limited = truncate_batches(&batches, limit);

    let buf = Vec::new();
    let mut writer = arrow::json::ArrayWriter::new(buf);
    writer
        .write_batches(&limited.iter().collect::<Vec<_>>())
        .map_err(|e| format!("JSON encoding failed: {}", e))?;
    writer
        .finish()
        .map_err(|e| format!("JSON finish failed: {}", e))?;
    let json_bytes = writer.into_inner();

    let json_objects: Vec<serde_json::Map<String, serde_json::Value>> =
        serde_json::from_slice(&json_bytes).map_err(|e| format!("JSON parse failed: {}", e))?;

    let rows: Vec<Vec<serde_json::Value>> = json_objects
        .into_iter()
        .map(|mut obj| {
            column_names
                .iter()
                .map(|name| obj.remove(*name).unwrap_or(serde_json::Value::Null))
                .collect()
        })
        .collect();

    Ok(QueryResponse {
        columns,
        row_count: rows.len(),
        rows,
        execution_time_ms: 0,
    })
}

/// Truncate batches to at most `limit` total rows, slicing the last batch if needed.
fn truncate_batches(
    batches: &[arrow::record_batch::RecordBatch],
    limit: usize,
) -> Vec<arrow::record_batch::RecordBatch> {
    let mut result = Vec::new();
    let mut remaining = limit;

    for batch in batches {
        if remaining == 0 {
            break;
        }
        if batch.num_rows() <= remaining {
            result.push(batch.clone());
            remaining -= batch.num_rows();
        } else {
            result.push(batch.slice(0, remaining));
            break;
        }
    }

    result
}

/// Build a `TableRewriter` from R2 environment variables.
///
/// Reads `R2_BUCKET`, `R2_ENDPOINT`, and `R2_ACCOUNT_ID` once. Intended to
/// be called at startup so the rewriter can be stored in `PondState` and
/// reused across all queries without per-request env reads or String allocations.
pub fn build_table_rewriter_from_env() -> crate::warehouse::query::rewriter::TableRewriter {
    let bucket = std::env::var("R2_BUCKET").unwrap_or_else(|_| "warehouse".to_string());
    crate::warehouse::query::rewriter::TableRewriter::from_r2_bucket(&bucket)
}

/// Record query usage for billing.
#[tracing::instrument(name = "warehouse.internal.record_query_usage", skip_all)]
async fn record_query_usage(
    db: &sqlx::PgPool,
    project_id: Uuid,
    _user_id: Uuid,
    sql: &str,
    bytes_scanned: u64,
    execution_time_ms: u64,
    cache_hit: bool,
) -> Result<()> {
    // Note: warehouse_usage schema has: id, project_id, query_id, bytes_scanned,
    // files_read, execution_time_ms, cache_hit, created_at
    sqlx::query(
        r#"
        INSERT INTO warehouse_usage (
            id, project_id, bytes_scanned, files_read,
            execution_time_ms, cache_hit, created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(project_id)
    .bind(bytes_scanned as i64)
    .bind(0i32) // files_read - not tracked for federated queries
    .bind(execution_time_ms as i64)
    .bind(cache_hit)
    .execute(db)
    .await?;

    // Record source access for access-based tier policies (best-effort, non-blocking)
    let db_clone = db.clone();
    let sql_owned = sql.to_string();
    tokio::spawn(async move {
        if let Err(e) = record_source_access(&db_clone, project_id, &sql_owned).await {
            tracing::warn!(
                project_id = %project_id,
                error = %e,
                "Failed to record source access for tier policy"
            );
        }
    });

    Ok(())
}

/// Record source access in `source_access_log` for access-based tier policies.
///
/// Extracts source names from the SQL query (table references like `source.table`),
/// resolves them to source IDs, and inserts one row per source touched.
#[tracing::instrument(name = "warehouse.internal.record_source_access", skip_all)]
async fn record_source_access(
    db: &sqlx::PgPool,
    project_id: Uuid,
    sql: &str,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Extract table references from the SQL to find source names.
    // Table references use the pattern: source_name.table_name
    let source_names = extract_source_names_from_sql(sql);

    if source_names.is_empty() {
        return Ok(());
    }

    // Resolve source names to IDs in a single query
    let rows = sqlx::query(
        "SELECT DISTINCT id, name FROM warehouse_sources WHERE project_id = $1 AND enabled = true",
    )
    .bind(project_id)
    .fetch_all(db)
    .await?;

    let mut source_ids: Vec<Uuid> = Vec::new();
    for row in &rows {
        let name: String = row.get("name");
        if source_names.contains(&name.to_lowercase()) {
            let id: Uuid = row.get("id");
            source_ids.push(id);
        }
    }

    if source_ids.is_empty() {
        // Could not determine which sources were queried — skip recording
        // rather than inflating counts for all sources.
        return Ok(());
    }

    // Batch-insert one row per source into source_access_log
    if source_ids.len() == 1 {
        sqlx::query(
            "INSERT INTO source_access_log (id, source_id, project_id, accessed_at) VALUES ($1, $2, $3, NOW())"
        )
        .bind(Uuid::new_v4())
        .bind(source_ids[0])
        .bind(project_id)
        .execute(db)
        .await?;
    } else {
        // Build a single multi-row INSERT for efficiency
        let mut sql = String::from(
            "INSERT INTO source_access_log (id, source_id, project_id, accessed_at) VALUES ",
        );
        let mut params: Vec<Uuid> = Vec::with_capacity(source_ids.len() * 3);
        for (i, source_id) in source_ids.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            let base = i * 3;
            sql.push_str(&format!(
                "(${}, ${}, ${}, NOW())",
                base + 1,
                base + 2,
                base + 3
            ));
            params.push(Uuid::new_v4());
            params.push(*source_id);
            params.push(project_id);
        }
        let mut query = sqlx::query(&sql);
        for param in &params {
            query = query.bind(param);
        }
        query.execute(db).await?;
    }

    Ok(())
}

/// Extract source names from SQL table references.
///
/// Looks for two-part table references like `source_name.table_name` and returns
/// the set of distinct source names (lowercased).
fn extract_source_names_from_sql(sql: &str) -> std::collections::HashSet<String> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let mut source_names = std::collections::HashSet::new();

    let dialect = GenericDialect {};
    let Ok(statements) = Parser::parse_sql(&dialect, sql) else {
        return source_names;
    };

    for statement in &statements {
        extract_sources_from_statement(statement, &mut source_names);
    }

    source_names
}

/// Recursively extract source names from a SQL statement's table references.
fn extract_sources_from_statement(
    statement: &sqlparser::ast::Statement,
    sources: &mut std::collections::HashSet<String>,
) {
    use sqlparser::ast::Statement;

    match statement {
        Statement::Query(query) => {
            extract_sources_from_query(query, sources);
        }
        _ => {}
    }
}

/// Extract source names from a Query, including CTEs (WITH clauses).
fn extract_sources_from_query(
    query: &sqlparser::ast::Query,
    sources: &mut std::collections::HashSet<String>,
) {
    // Walk CTE definitions — each CTE contains a sub-query that may reference sources
    if let Some(ref with) = query.with {
        for cte in &with.cte_tables {
            extract_sources_from_query(&cte.query, sources);
        }
    }

    // Walk the main query body
    extract_sources_from_set_expr(&query.body, sources);
}

/// Extract source names from a query body (handles SELECT, UNION, etc.).
fn extract_sources_from_set_expr(
    set_expr: &sqlparser::ast::SetExpr,
    sources: &mut std::collections::HashSet<String>,
) {
    use sqlparser::ast::SetExpr;

    match set_expr {
        SetExpr::Select(select) => {
            for from in &select.from {
                extract_sources_from_table_factor(&from.relation, sources);
                for join in &from.joins {
                    extract_sources_from_table_factor(&join.relation, sources);
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            extract_sources_from_set_expr(left, sources);
            extract_sources_from_set_expr(right, sources);
        }
        _ => {}
    }
}

/// Extract the source name from a table factor (e.g. `source_name.table_name`).
fn extract_sources_from_table_factor(
    table_factor: &sqlparser::ast::TableFactor,
    sources: &mut std::collections::HashSet<String>,
) {
    use sqlparser::ast::TableFactor;

    match table_factor {
        TableFactor::Table { name, .. } => {
            // Two-part names: source_name.table_name
            if name.0.len() >= 2 {
                let source_name = name.0[0].value.to_lowercase();
                sources.insert(source_name);
            }
        }
        TableFactor::Derived { subquery, .. } => {
            extract_sources_from_query(subquery, sources);
        }
        _ => {}
    }
}

/// Estimate query cost before execution.
///
/// Uses the pre-populated shared cost estimator for accurate estimates based
/// on real table statistics from sync operations.
#[tracing::instrument(name = "warehouse.api.estimate_query", skip(state, req), fields(project_id = %project_id), err(Display))]
async fn estimate_query(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<QueryCostEstimate>> {
    let query_config = validate_query_request(&req)?;

    // Use the shared cost estimator with pre-populated table statistics
    let mut estimator = state.warehouse_cost_estimator.write();
    let estimate = estimator
        .estimate(&query_config.sql)
        .map_err(|e| AppError::BadRequest(e.to_string()))?;

    Ok(Json(estimate))
}

/// Explain a query's execution plan.
///
/// Uses the pre-populated shared cost estimator for accurate cost information.
#[tracing::instrument(name = "warehouse.api.explain_query", skip(state, req), fields(project_id = %project_id), err(Display))]
async fn explain_query(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<QueryExplain>> {
    let query_config = validate_query_request(&req)?;

    let estimator = state.warehouse_cost_estimator.read();
    let mut cost_estimator = QueryCostEstimator::new();
    for stats in estimator.all_table_stats().values() {
        cost_estimator.add_table_stats(stats.clone());
    }
    drop(estimator);

    let mut explainer = QueryExplainer::new(cost_estimator);
    let explain = explainer.explain(&query_config.sql);

    Ok(Json(explain))
}

/// Execute a natural language query against the warehouse.
///
/// Converts a user question in plain English into a SQL query using an LLM,
/// validates the generated SQL, executes it, and returns both the SQL and results.
///
/// Rate limiting is enforced at the website proxy level (RateLimitType::NlQuery,
/// default 10/min per project) before requests reach this handler.
#[tracing::instrument(name = "warehouse.api.execute_nl_query_handler", skip(state, headers, req), fields(project_id = %project_id), err(Display))]
async fn execute_nl_query_handler(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
    headers: HeaderMap,
    Json(req): Json<crate::warehouse::nl_query::NLQueryRequest>,
) -> Result<Json<crate::warehouse::nl_query::NLQueryResponse>> {
    let user_id = extract_user_id(&headers)?;

    // Validate question
    let question = req.question.trim();
    if question.is_empty() {
        return Err(AppError::BadRequest("Question cannot be empty".to_string()));
    }
    if question.len() > 2000 {
        return Err(AppError::BadRequest(
            "Question too long (max 2000 characters)".to_string(),
        ));
    }

    let model = req.model.as_deref();

    let result = crate::warehouse::nl_query::execute_nl_query(
        &state,
        project_id,
        user_id,
        question,
        model,
        req.conversation_id,
    )
    .await
    .map_err(|e| AppError::Internal(e))?;

    Ok(Json(result))
}

/// Get natural language query suggestions based on the project's schema.
#[tracing::instrument(name = "warehouse.api.nl_query_suggestions", skip(state, headers), fields(project_id = %project_id), err(Display))]
async fn nl_query_suggestions(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Json<Vec<crate::warehouse::nl_query::suggestions::QuerySuggestion>>> {
    let _user_id = extract_user_id(&headers)?;

    let catalog_repo = crate::warehouse::catalog::CatalogRepository::new(state.db.clone());
    let entries = catalog_repo
        .list_entries(project_id)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to load catalog: {}", e)))?;

    let suggestions = crate::warehouse::nl_query::suggestions::generate_suggestions(&entries, 5);
    Ok(Json(suggestions))
}

/// Full-text search across warehouse data content.
#[tracing::instrument(name = "warehouse.api.search", skip(state, headers, req), fields(project_id = %project_id), err(Display))]
async fn search_handler(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
    headers: HeaderMap,
    Json(req): Json<crate::warehouse::search::SearchRequest>,
) -> Result<Json<crate::warehouse::search::SearchResponse>> {
    let _user_id = extract_user_id(&headers)?;

    let query = req.query.trim();
    if query.is_empty() {
        return Err(AppError::BadRequest(
            "Search query cannot be empty".to_string(),
        ));
    }
    if query.len() > 1000 {
        return Err(AppError::BadRequest(
            "Search query too long (max 1000 characters)".to_string(),
        ));
    }

    let result = crate::warehouse::search::execute_search(&state, project_id, &req)
        .await
        .map_err(|e| AppError::Internal(e))?;

    Ok(Json(result))
}

/// Get freshness status for all tables.
#[tracing::instrument(name = "warehouse.api.get_freshness", skip(state), fields(project_id = %project_id), err(Display))]
async fn get_freshness(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
) -> Result<Json<Vec<TableFreshness>>> {
    let rows = sqlx::query(
        r#"
        SELECT 
            t.name as table_name,
            s.name as source_name,
            (SELECT MAX(completed_at) FROM warehouse_syncs WHERE source_id = t.source_id AND table_name = t.name) as last_sync_at,
            sch.next_run_at
        FROM warehouse_tables t
        JOIN warehouse_sources s ON s.id = t.source_id
        LEFT JOIN warehouse_sync_schedules sch ON sch.source_id = t.source_id AND sch.enabled = true
        WHERE s.project_id = $1
        ORDER BY t.name
        "#
    )
    .bind(project_id)
    .fetch_all(&*state.db)
    .await?;

    let now = Utc::now();
    let freshness: Vec<TableFreshness> = rows
        .into_iter()
        .map(|row| {
            let last_sync: Option<DateTime<Utc>> = row.get("last_sync_at");
            let staleness_minutes = last_sync.map(|ls| (now - ls).num_minutes());
            let staleness_level = match staleness_minutes {
                None => "unknown",
                Some(m) if m < 60 => "fresh",
                Some(m) if m < 360 => "moderate",
                Some(m) if m < 1440 => "stale",
                _ => "very_stale",
            };

            TableFreshness {
                table_name: row.get("table_name"),
                source_name: row.get("source_name"),
                last_sync_at: last_sync,
                next_sync_at: row.get("next_run_at"),
                staleness_minutes,
                staleness_level: staleness_level.to_string(),
            }
        })
        .collect();

    Ok(Json(freshness))
}

/// Autocomplete for schema names.
#[tracing::instrument(name = "warehouse.api.autocomplete", skip(state), fields(project_id = %project_id), err(Display))]
async fn autocomplete(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
    axum::extract::Query(params): axum::extract::Query<AutocompleteRequest>,
) -> Result<Json<AutocompleteResponse>> {
    // Validate prefix length
    if params.prefix.len() > 100 {
        return Err(AppError::Validation(
            "Prefix too long (max 100 characters)".to_string(),
        ));
    }

    // TODO: Use FST schema index for autocomplete
    // For now, return empty results
    Ok(Json(AutocompleteResponse {
        suggestions: vec![],
    }))
}

/// List saved views.
#[tracing::instrument(name = "warehouse.api.list_views", skip(state), fields(project_id = %project_id), err(Display))]
async fn list_views(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
) -> Result<Json<Vec<ViewResponse>>> {
    let rows = sqlx::query(
        "SELECT id, name, sql, description, created_at FROM warehouse_views WHERE project_id = $1 ORDER BY name"
    )
    .bind(project_id)
    .fetch_all(&*state.db)
    .await?;

    let views: Vec<ViewResponse> = rows
        .into_iter()
        .map(|row| ViewResponse {
            id: row.get("id"),
            name: row.get("name"),
            sql: row.get("sql"),
            description: row.get("description"),
            created_at: row.get("created_at"),
        })
        .collect();

    Ok(Json(views))
}

/// Create a new view.
#[tracing::instrument(name = "warehouse.api.create_view", skip(state, headers, req), fields(project_id = %project_id), err(Display))]
async fn create_view(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
    headers: HeaderMap,
    Json(req): Json<CreateViewRequest>,
) -> Result<Json<ViewResponse>> {
    let user_id = extract_user_id(&headers)?;

    // Validate the view request including SQL validation
    validate_view_request(&req)?;

    let id = Uuid::new_v4();
    let now = Utc::now();

    sqlx::query(
        "INSERT INTO warehouse_views (id, project_id, name, sql, description, created_by, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $7)"
    )
    .bind(id)
    .bind(project_id)
    .bind(&req.name)
    .bind(&req.sql)
    .bind(&req.description)
    .bind(user_id)
    .bind(now)
    .execute(&*state.db)
    .await?;

    Ok(Json(ViewResponse {
        id,
        name: req.name,
        sql: req.sql,
        description: req.description,
        created_at: now,
    }))
}

/// Get a view by ID.
#[tracing::instrument(name = "warehouse.api.get_view", skip(state), fields(project_id = %path.project_id), err(Display))]
async fn get_view(
    State(state): State<Arc<PondState>>,
    Path(path): Path<ViewPath>,
) -> Result<Json<ViewResponse>> {
    let row = sqlx::query(
        "SELECT id, name, sql, description, created_at FROM warehouse_views WHERE id = $1 AND project_id = $2"
    )
    .bind(path.view_id)
    .bind(path.project_id)
    .fetch_optional(&*state.db)
    .await?
    .ok_or_else(|| AppError::NotFound("View not found".to_string()))?;

    Ok(Json(ViewResponse {
        id: row.get("id"),
        name: row.get("name"),
        sql: row.get("sql"),
        description: row.get("description"),
        created_at: row.get("created_at"),
    }))
}

/// Delete a view.
#[tracing::instrument(name = "warehouse.api.delete_view", skip(state), fields(project_id = %path.project_id), err(Display))]
async fn delete_view(
    State(state): State<Arc<PondState>>,
    Path(path): Path<ViewPath>,
) -> Result<StatusCode> {
    let result = sqlx::query("DELETE FROM warehouse_views WHERE id = $1 AND project_id = $2")
        .bind(path.view_id)
        .bind(path.project_id)
        .execute(&*state.db)
        .await?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound("View not found".to_string()));
    }

    Ok(StatusCode::NO_CONTENT)
}

/// Get usage summary.
#[tracing::instrument(name = "warehouse.api.usage_summary", skip(state), fields(project_id = %project_id), err(Display))]
async fn usage_summary(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
) -> Result<Json<UsageSummary>> {
    let row = sqlx::query(
        r#"
        SELECT 
            COALESCE(SUM(bytes_scanned), 0) as total_bytes,
            COUNT(*) as total_queries,
            COALESCE(AVG(CASE WHEN cache_hit THEN 1.0 ELSE 0.0 END), 0) as cache_hit_rate,
            COALESCE(AVG(execution_time_ms), 0) as avg_time
        FROM warehouse_usage
        WHERE project_id = $1 AND created_at >= date_trunc('month', NOW())
        "#,
    )
    .bind(project_id)
    .fetch_one(&*state.db)
    .await?;

    Ok(Json(UsageSummary {
        period: "current_month".to_string(),
        total_bytes_scanned: row.get::<i64, _>("total_bytes") as u64,
        total_queries: row.get::<i64, _>("total_queries") as u64,
        cache_hit_rate: row.get::<f64, _>("cache_hit_rate"),
        avg_execution_time_ms: row.get::<f64, _>("avg_time") as u64,
    }))
}

/// Get usage by query.
#[tracing::instrument(name = "warehouse.api.usage_by_query", skip(state), fields(project_id = %project_id), err(Display))]
async fn usage_by_query(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
) -> Result<Json<Vec<QueryUsage>>> {
    let rows = sqlx::query(
        r#"
        SELECT 
            qh.id as query_id,
            LEFT(qh.sql, 100) as sql_preview,
            SUM(u.bytes_scanned) as total_bytes,
            COUNT(*) as execution_count,
            MAX(u.created_at) as last_executed
        FROM warehouse_query_history qh
        JOIN warehouse_usage u ON u.query_id = qh.id
        WHERE qh.project_id = $1 AND qh.executed_at >= NOW() - INTERVAL '30 days'
        GROUP BY qh.id, qh.sql
        ORDER BY total_bytes DESC
        LIMIT 50
        "#,
    )
    .bind(project_id)
    .fetch_all(&*state.db)
    .await?;

    let usage: Vec<QueryUsage> = rows
        .into_iter()
        .map(|row| QueryUsage {
            query_id: row.get("query_id"),
            sql_preview: row.get("sql_preview"),
            bytes_scanned: row.get::<i64, _>("total_bytes") as u64,
            execution_count: row.get::<i64, _>("execution_count") as u64,
            last_executed: row.get("last_executed"),
        })
        .collect();

    Ok(Json(usage))
}

/// Get budget configuration.
#[tracing::instrument(name = "warehouse.api.get_budget", skip(state), fields(project_id = %project_id), err(Display))]
async fn get_budget(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
) -> Result<Json<BudgetResponse>> {
    // Get budget config for this project
    let budget_row = sqlx::query(
        "SELECT monthly_bytes_limit, alert_threshold_percent FROM warehouse_budgets WHERE project_id = $1"
    )
    .bind(project_id)
    .fetch_optional(&*state.db)
    .await?;

    // Get current usage for this project
    let usage: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(bytes_scanned), 0) FROM warehouse_usage 
         WHERE project_id = $1 AND created_at >= date_trunc('month', NOW())",
    )
    .bind(project_id)
    .fetch_one(&*state.db)
    .await?;

    let (limit, threshold) = budget_row
        .map(|r| {
            (
                r.get::<Option<i64>, _>("monthly_bytes_limit"),
                r.get::<i32, _>("alert_threshold_percent"),
            )
        })
        .unwrap_or((None, 80));

    let usage_percent = limit
        .map(|l| {
            if l > 0 {
                (usage as f64 / l as f64) * 100.0
            } else {
                0.0
            }
        })
        .unwrap_or(0.0);

    Ok(Json(BudgetResponse {
        monthly_bytes_limit: limit,
        alert_threshold_percent: threshold,
        current_usage_bytes: usage,
        usage_percent,
    }))
}

/// Set budget configuration.
#[tracing::instrument(name = "warehouse.api.set_budget", skip(state, req), fields(project_id = %project_id), err(Display))]
async fn set_budget(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
    Json(req): Json<SetBudgetRequest>,
) -> Result<Json<BudgetResponse>> {
    // Validate budget request
    if let Some(threshold) = req.alert_threshold_percent {
        if !(1..=100).contains(&threshold) {
            return Err(AppError::Validation(
                "Alert threshold must be between 1 and 100".to_string(),
            ));
        }
    }
    if let Some(limit) = req.monthly_bytes_limit {
        if limit < 0 {
            return Err(AppError::Validation(
                "Monthly bytes limit cannot be negative".to_string(),
            ));
        }
    }

    sqlx::query(
        r#"
        INSERT INTO warehouse_budgets (id, project_id, monthly_bytes_limit, alert_threshold_percent, created_at, updated_at)
        VALUES ($1, $2, $3, $4, NOW(), NOW())
        ON CONFLICT (project_id) DO UPDATE SET
            monthly_bytes_limit = COALESCE($3, warehouse_budgets.monthly_bytes_limit),
            alert_threshold_percent = COALESCE($4, warehouse_budgets.alert_threshold_percent),
            updated_at = NOW()
        "#
    )
    .bind(Uuid::new_v4())
    .bind(project_id)
    .bind(req.monthly_bytes_limit)
    .bind(req.alert_threshold_percent)
    .execute(&*state.db)
    .await?;

    // Return updated budget
    get_budget(State(state), Path(project_id)).await
}

// ===== Input Validation Functions =====

/// Maximum allowed length for source names.
const MAX_SOURCE_NAME_LENGTH: usize = 256;
/// Maximum allowed length for view names.
const MAX_VIEW_NAME_LENGTH: usize = 256;
/// Maximum allowed length for SQL queries.
const MAX_SQL_LENGTH: usize = 100_000;
/// Maximum allowed length for cron expressions.
const MAX_CRON_LENGTH: usize = 100;
/// Maximum allowed length for descriptions.
const MAX_DESCRIPTION_LENGTH: usize = 2000;

/// Validate source creation request.
fn validate_source_request(req: &CreateSourceRequest) -> Result<()> {
    if req.name.is_empty() {
        return Err(AppError::Validation(
            "Source name cannot be empty".to_string(),
        ));
    }
    if req.name.len() > MAX_SOURCE_NAME_LENGTH {
        return Err(AppError::Validation(format!(
            "Source name too long (max {} characters)",
            MAX_SOURCE_NAME_LENGTH
        )));
    }
    // Validate name contains only safe characters
    if !req
        .name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == ' ')
    {
        return Err(AppError::Validation(
            "Source name can only contain alphanumeric characters, underscores, hyphens, and spaces".to_string()
        ));
    }
    Ok(())
}

fn validate_cron_expression(expr: &str) -> Result<()> {
    if expr.len() > MAX_CRON_LENGTH {
        return Err(AppError::Validation(format!(
            "Cron expression too long (max {} characters)",
            MAX_CRON_LENGTH
        )));
    }
    croner::Cron::new(expr)
        .parse()
        .map_err(|e| AppError::Validation(format!("Invalid cron expression '{}': {}", expr, e)))?;
    Ok(())
}

/// Maximum number of tables allowed in a single query.
const MAX_TABLES_PER_QUERY: usize = 10;
/// Default query timeout in seconds.
const DEFAULT_QUERY_TIMEOUT_SECS: u64 = 60;
/// Maximum row limit for queries.
const MAX_ROW_LIMIT: u32 = 100_000;
/// Default row limit if not specified.
const DEFAULT_ROW_LIMIT: u32 = 10_000;

/// Query configuration with limits applied.
pub struct QueryConfig {
    pub sql: String,
    pub limit: u32,
    pub timeout_secs: u64,
}

/// Validate query request and return configuration with limits applied.
fn validate_query_request(req: &QueryRequest) -> Result<QueryConfig> {
    if req.sql.is_empty() {
        return Err(AppError::Validation(
            "SQL query cannot be empty".to_string(),
        ));
    }
    if req.sql.len() > MAX_SQL_LENGTH {
        return Err(AppError::Validation(format!(
            "SQL query too long (max {} characters)",
            MAX_SQL_LENGTH
        )));
    }

    // Apply row limit
    let limit = req.limit.unwrap_or(DEFAULT_ROW_LIMIT).min(MAX_ROW_LIMIT);

    let sql = validate_and_limit_query(&req.sql, limit)?;

    Ok(QueryConfig {
        sql,
        limit,
        timeout_secs: DEFAULT_QUERY_TIMEOUT_SECS,
    })
}

/// Validate query complexity and inject/cap LIMIT in a single parse.
fn validate_and_limit_query(sql: &str, max_limit: u32) -> Result<String> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| AppError::Validation(format!("Invalid SQL syntax: {}", e)))?;

    if statements.is_empty() {
        return Err(AppError::Validation(
            "SQL query cannot be empty".to_string(),
        ));
    }

    if statements.len() > 1 {
        return Err(AppError::Validation(
            "Only single-statement queries are allowed".to_string(),
        ));
    }

    match &statements[0] {
        sqlparser::ast::Statement::Query(_) => {}
        _ => {
            return Err(AppError::Validation(
                "Only SELECT statements are allowed".to_string(),
            ));
        }
    }

    let tables =
        crate::warehouse::query::rewriter::TableRewriter::extract_tables_from_ast(&statements);
    if tables.len() > MAX_TABLES_PER_QUERY {
        return Err(AppError::Validation(format!(
            "Query references too many tables ({} > {} max)",
            tables.len(),
            MAX_TABLES_PER_QUERY
        )));
    }

    if let sqlparser::ast::Statement::Query(query) = &statements[0] {
        if let Some(ref with) = query.with {
            if with.recursive {
                return Err(AppError::Validation(
                    "Recursive CTEs are not allowed".to_string(),
                ));
            }
        }
        if let Some(ref with) = query.with {
            for cte in &with.cte_tables {
                if has_cross_join(cte.query.body.as_ref()) {
                    return Err(AppError::Validation(
                        "CROSS JOIN is not allowed due to potential for large result sets"
                            .to_string(),
                    ));
                }
            }
        }
        if has_cross_join(query.body.as_ref()) {
            return Err(AppError::Validation(
                "CROSS JOIN is not allowed due to potential for large result sets".to_string(),
            ));
        }
    }

    let statement = &mut statements[0];
    if let sqlparser::ast::Statement::Query(query) = statement {
        match &mut query.limit {
            Some(limit_expr) => {
                let cap = sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(
                    max_limit.to_string(),
                    false,
                ));
                match limit_expr {
                    sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) => {
                        if let Ok(limit_val) = n.parse::<u32>() {
                            if limit_val > max_limit {
                                *n = max_limit.to_string();
                                tracing::debug!(
                                    original_limit = limit_val,
                                    capped_limit = max_limit,
                                    "Query LIMIT capped to maximum allowed"
                                );
                            }
                        } else {
                            *limit_expr = cap;
                        }
                    }
                    _ => {
                        *limit_expr = cap;
                    }
                }
            }
            None => {
                query.limit = Some(sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(
                    max_limit.to_string(),
                    false,
                )));
            }
        }

        return Ok(statement.to_string());
    }

    Err(AppError::Validation(
        "Only SELECT statements are allowed".to_string(),
    ))
}

fn has_cross_join(body: &sqlparser::ast::SetExpr) -> bool {
    match body {
        sqlparser::ast::SetExpr::Select(select) => select.from.iter().any(|twj| {
            twj.joins
                .iter()
                .any(|j| matches!(j.join_operator, sqlparser::ast::JoinOperator::CrossJoin))
        }),
        sqlparser::ast::SetExpr::Query(q) => has_cross_join(q.body.as_ref()),
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            has_cross_join(left) || has_cross_join(right)
        }
        _ => false,
    }
}

/// Validate view creation request including SQL safety check.
fn validate_view_request(req: &CreateViewRequest) -> Result<()> {
    // Validate name
    if req.name.is_empty() {
        return Err(AppError::Validation(
            "View name cannot be empty".to_string(),
        ));
    }
    if req.name.len() > MAX_VIEW_NAME_LENGTH {
        return Err(AppError::Validation(format!(
            "View name too long (max {} characters)",
            MAX_VIEW_NAME_LENGTH
        )));
    }
    if !req.name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(AppError::Validation(
            "View name can only contain alphanumeric characters and underscores".to_string(),
        ));
    }

    // Validate SQL
    if req.sql.is_empty() {
        return Err(AppError::Validation("View SQL cannot be empty".to_string()));
    }
    if req.sql.len() > MAX_SQL_LENGTH {
        return Err(AppError::Validation(format!(
            "View SQL too long (max {} characters)",
            MAX_SQL_LENGTH
        )));
    }

    // Validate description
    if let Some(desc) = &req.description {
        if desc.len() > MAX_DESCRIPTION_LENGTH {
            return Err(AppError::Validation(format!(
                "Description too long (max {} characters)",
                MAX_DESCRIPTION_LENGTH
            )));
        }
    }

    // SECURITY: Validate that SQL is a SELECT statement only
    validate_view_sql(&req.sql)?;

    Ok(())
}

/// Validate that view SQL only contains SELECT statements.
/// This prevents SQL injection via stored views.
fn validate_view_sql(sql: &str) -> Result<()> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| AppError::Validation(format!("Invalid SQL syntax: {}", e)))?;

    if statements.is_empty() {
        return Err(AppError::Validation(
            "SQL query cannot be empty".to_string(),
        ));
    }

    for statement in &statements {
        match statement {
            sqlparser::ast::Statement::Query(_) => {
                // SELECT queries are allowed
            }
            _ => {
                return Err(AppError::Validation(
                    "Only SELECT statements are allowed in views".to_string(),
                ));
            }
        }
    }

    Ok(())
}

// ===== Credential Encryption Helpers =====

/// Decrypt source config from the database.
///
/// The config is stored encrypted in the database to protect credentials.
/// This function is used internally when a sync job needs to access the credentials.
pub fn decrypt_source_config(
    encrypted_config: &serde_json::Value,
    encryptor: &crate::crypto::RotatingSecretEncryptor,
) -> Result<serde_json::Value> {
    // Check if this is an encrypted config (has "encrypted" field)
    if let Some(encrypted_str) = encrypted_config.get("encrypted").and_then(|v| v.as_str()) {
        let decrypted_json = encryptor
            .decrypt(encrypted_str)
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to decrypt config: {}", e)))?;

        serde_json::from_str(&decrypted_json).map_err(|e| {
            AppError::Internal(anyhow::anyhow!("Invalid decrypted config JSON: {}", e))
        })
    } else {
        // Legacy unencrypted config - return as-is but log a warning
        tracing::warn!("Source config is not encrypted - this is a security risk");
        Ok(encrypted_config.clone())
    }
}

/// Encrypt source config for storage in the database.
///
/// Returns a JSON object with an "encrypted" field containing the encrypted config.
pub fn encrypt_source_config(
    config: &serde_json::Value,
    encryptor: &crate::crypto::RotatingSecretEncryptor,
) -> Result<serde_json::Value> {
    let config_json = serde_json::to_string(config)
        .map_err(|e| AppError::Validation(format!("Invalid config JSON: {}", e)))?;

    let encrypted = encryptor
        .encrypt(&config_json)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to encrypt config: {}", e)))?;

    Ok(serde_json::json!({ "encrypted": encrypted }))
}

// ===== Skip Index Management =====

/// Load skip indexes for a project using the hybrid storage path.
///
/// For each table with a manifest entry:
/// 1. Check local disk cache version against the PG manifest version.
/// 2. If versions match, mmap the local file (no network I/O).
/// 3. If mismatch or missing, download from R2, decompress, store to disk, mmap.
/// 4. Parse the blob into `FileIndexEntry` values and build the `HierarchicalSkipIndex`.
///
/// Tables whose R2 download fails after retries are skipped (logged, not fatal).
#[tracing::instrument(name = "warehouse.internal.load_project_skip_indexes", skip_all)]
pub async fn load_project_skip_indexes(
    db: &sqlx::PgPool,
    storage: &crate::warehouse::storage::r2::R2Storage,
    disk_cache: &crate::warehouse::indexes::disk_cache::DiskIndexCache,
    project_id: Uuid,
) -> Result<AHashMap<String, crate::warehouse::indexes::skip_index::HierarchicalSkipIndex>> {
    use crate::warehouse::indexes::persistence::get_manifests_for_project;

    let manifests = get_manifests_for_project(db, project_id)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get manifests: {}", e))?;

    if manifests.is_empty() {
        return Ok(AHashMap::new());
    }

    let mut indexes: AHashMap<
        String,
        crate::warehouse::indexes::skip_index::HierarchicalSkipIndex,
    > = AHashMap::new();

    let mut r2_downloads = 0u32;
    let mut cache_hits = 0u32;
    let mut load_errors = 0u32;

    for manifest in &manifests {
        // Check local disk cache
        let local_version = disk_cache.load_version(project_id, &manifest.table_name);
        let mmap_result = if local_version == Some(manifest.version) {
            cache_hits += 1;
            disk_cache.mmap(project_id, &manifest.table_name)
        } else {
            // Download from R2 with retries
            let mut downloaded = None;
            let mut last_err = None;
            for attempt in 0..3u32 {
                match storage.download(&manifest.r2_key).await {
                    Ok(data) => {
                        downloaded = Some(data);
                        break;
                    }
                    Err(e) => {
                        last_err = Some(e);
                        if attempt < 2 {
                            let delay = std::time::Duration::from_millis(500 * 2u64.pow(attempt));
                            tokio::time::sleep(delay).await;
                        }
                    }
                }
            }

            let compressed = match downloaded {
                Some(d) => d,
                None => {
                    load_errors += 1;
                    tracing::warn!(
                        project_id = %project_id,
                        table = %manifest.table_name,
                        r2_key = %manifest.r2_key,
                        error = %last_err.map(|e| e.to_string()).unwrap_or_default(),
                        "R2 download failed after 3 retries, skipping table"
                    );
                    continue;
                }
            };

            r2_downloads += 1;

            // Decompress
            let decompressed = match zstd::decode_all(&compressed[..]) {
                Ok(d) => d,
                Err(e) => {
                    load_errors += 1;
                    tracing::warn!(
                        project_id = %project_id,
                        table = %manifest.table_name,
                        error = %e,
                        "Failed to decompress index blob, skipping table"
                    );
                    continue;
                }
            };

            // Store to disk cache
            if let Err(e) = disk_cache.store(
                project_id,
                &manifest.table_name,
                &decompressed,
                manifest.version,
            ) {
                tracing::warn!(
                    project_id = %project_id,
                    table = %manifest.table_name,
                    error = %e,
                    "Failed to write to disk cache, loading from memory instead"
                );
                // Fall back: parse from the decompressed bytes directly
                match build_hierarchical_from_blob(&decompressed) {
                    Ok(idx) => {
                        indexes.insert(manifest.table_name.clone(), idx);
                    }
                    Err(e) => {
                        load_errors += 1;
                        tracing::warn!(
                            table = %manifest.table_name,
                            error = %e,
                            "Failed to parse index blob"
                        );
                    }
                }
                continue;
            }

            disk_cache.mmap(project_id, &manifest.table_name)
        };

        // Parse the mmapped blob (skip the version header) -- zero-copy
        let blob_offset = crate::warehouse::indexes::disk_cache::DiskIndexCache::mmap_blob_offset();
        match mmap_result {
            Ok(mmap) => {
                let mmap_arc = std::sync::Arc::new(mmap);
                match build_hierarchical_from_blob_mmap(mmap_arc, blob_offset) {
                    Ok(idx) => {
                        indexes.insert(manifest.table_name.clone(), idx);
                    }
                    Err(e) => {
                        load_errors += 1;
                        tracing::warn!(
                            table = %manifest.table_name,
                            error = %e,
                            "Failed to parse mmapped index blob"
                        );
                    }
                }
            }
            Err(e) => {
                load_errors += 1;
                tracing::warn!(
                    project_id = %project_id,
                    table = %manifest.table_name,
                    error = %e,
                    "Failed to mmap local cache file, skipping table"
                );
            }
        }
    }

    let total_tables = indexes.len();
    let total_files: usize = indexes
        .values()
        .map(|h: &crate::warehouse::indexes::skip_index::HierarchicalSkipIndex| h.total_files())
        .sum();

    tracing::info!(
        project_id = %project_id,
        tables = total_tables,
        files = total_files,
        cache_hits = cache_hits,
        r2_downloads = r2_downloads,
        errors = load_errors,
        "Loaded skip indexes via hybrid storage"
    );

    Ok(indexes)
}

/// Build a `HierarchicalSkipIndex` from raw (uncompressed) blob bytes.
fn build_hierarchical_from_blob(
    blob: &[u8],
) -> Result<crate::warehouse::indexes::skip_index::HierarchicalSkipIndex> {
    use crate::warehouse::indexes::blob::deserialize_table_index;

    let entries = deserialize_table_index(blob)
        .map_err(|e| anyhow::anyhow!("Blob deserialization error: {}", e))?;

    Ok(build_hierarchical_from_entries(entries.into_iter().map(
        |e| {
            (
                e.partition_key,
                e.file_path,
                e.column_name,
                e.fst_data,
                e.row_count,
            )
        },
    )))
}

/// Build a `HierarchicalSkipIndex` from an mmapped blob (zero-copy).
fn build_hierarchical_from_blob_mmap(
    mmap: std::sync::Arc<memmap2::Mmap>,
    blob_offset: usize,
) -> Result<crate::warehouse::indexes::skip_index::HierarchicalSkipIndex> {
    use crate::warehouse::indexes::blob::deserialize_table_index_mmap;

    let entries = deserialize_table_index_mmap(mmap, blob_offset)
        .map_err(|e| anyhow::anyhow!("Blob deserialization error: {}", e))?;

    Ok(build_hierarchical_from_entries(entries.into_iter().map(
        |e| {
            (
                e.partition_key,
                e.file_path,
                e.column_name,
                e.fst_data,
                e.row_count,
            )
        },
    )))
}

/// Assemble a `HierarchicalSkipIndex` from individual column-level entries.
///
/// Each entry is a tuple of `(partition_key, file_path, column_name, fst_backing, row_count)`.
/// Entries are grouped by `(partition_key, file_path)` to reconstruct multi-column
/// `FileSkipIndex` objects before being added to the hierarchical index.
fn build_hierarchical_from_entries(
    entries: impl Iterator<
        Item = (
            String,
            String,
            String,
            crate::warehouse::indexes::fst_backing::FstBacking,
            u64,
        ),
    >,
) -> crate::warehouse::indexes::skip_index::HierarchicalSkipIndex {
    use crate::warehouse::indexes::skip_index::{FileSkipIndex, HierarchicalSkipIndex};

    let mut file_map: std::collections::HashMap<
        (String, String), // (partition_key, file_path)
        (FileSkipIndex, u64),
    > = std::collections::HashMap::new();

    for (partition_key, file_path, column_name, fst_backing, row_count) in entries {
        let key = (partition_key, file_path.clone());
        match file_map.get_mut(&key) {
            Some((fi, _)) => {
                if let Err(e) = fi.add_column_fst(&column_name, fst_backing) {
                    tracing::warn!(
                        file = %file_path,
                        column = %column_name,
                        error = %e,
                        "Failed to add column FST"
                    );
                }
            }
            None => {
                match FileSkipIndex::from_serialized_fst(&file_path, &column_name, fst_backing) {
                    Ok(fi) => {
                        file_map.insert(key, (fi, row_count));
                    }
                    Err(e) => {
                        tracing::warn!(
                            file = %file_path,
                            column = %column_name,
                            error = %e,
                            "Failed to create FileSkipIndex"
                        );
                    }
                }
            }
        }
    }

    let mut index = HierarchicalSkipIndex::new();
    for ((partition_key, _), (fi, row_count)) in file_map {
        if let Err(e) = index.add_file(&partition_key, fi, row_count) {
            tracing::warn!(error = %e, "Failed to add file to hierarchical index");
        }
    }

    index
}

/// Load skip indexes for a single table (used by the blob serialization write path).
///
/// Returns a `HierarchicalSkipIndex` for the given `(project_id, table_name)`.
pub async fn load_project_skip_indexes_for_table(
    db: &sqlx::PgPool,
    project_id: Uuid,
    table_name: &str,
) -> Result<crate::warehouse::indexes::skip_index::HierarchicalSkipIndex> {
    use sqlx::Row;

    let rows = sqlx::query(
        r#"
        SELECT partition_key, column_name, values_fst, file_path, row_count
        FROM warehouse_skip_indexes
        WHERE project_id = $1 AND table_name = $2
        ORDER BY partition_key, file_path, column_name
        "#,
    )
    .bind(project_id)
    .bind(table_name)
    .fetch_all(db)
    .await?;

    let entries = rows.into_iter().map(|row| {
        let partition_key: String = row.get("partition_key");
        let file_path: String = row.get("file_path");
        let column_name: String = row.get("column_name");
        let values_fst: Vec<u8> = row.get("values_fst");
        let row_count: i64 = row.get("row_count");
        (
            partition_key,
            file_path,
            column_name,
            crate::warehouse::indexes::fst_backing::FstBacking::Owned(values_fst),
            row_count as u64,
        )
    });

    Ok(build_hierarchical_from_entries(entries))
}

/// Refresh skip index cache for a project using the hybrid storage path.
///
/// Downloads from R2 if needed, caches to disk, mmaps, and updates the in-memory cache.
#[tracing::instrument(name = "warehouse.internal.refresh_skip_index_cache", skip_all)]
pub async fn refresh_skip_index_cache(
    state: &crate::app_state::PondState,
    storage: &crate::warehouse::storage::r2::R2Storage,
    disk_cache: &crate::warehouse::indexes::disk_cache::DiskIndexCache,
    project_id: Uuid,
) -> Result<()> {
    let indexes = load_project_skip_indexes(&state.db, storage, disk_cache, project_id).await?;

    let mut cache = state.warehouse_skip_indexes.write().await;
    cache.insert(project_id, indexes);

    tracing::debug!(
        project_id = %project_id,
        "Refreshed skip index cache"
    );

    Ok(())
}

/// Eagerly preload skip indexes for all projects at startup.
///
/// Queries the lightweight manifest table (no blobs), then loads each
/// project's indexes via the hybrid path (local cache -> R2 -> disk).
/// Projects are loaded in parallel with limited concurrency.
///
/// # Returns
/// The number of projects whose indexes were preloaded.
#[tracing::instrument(name = "pond.startup.preload_skip_indexes", skip_all)]
pub async fn preload_skip_indexes_at_startup(
    state: &crate::app_state::PondState,
    storage: &crate::warehouse::storage::r2::R2Storage,
    disk_cache: &crate::warehouse::indexes::disk_cache::DiskIndexCache,
) -> Result<usize> {
    use crate::warehouse::indexes::persistence::get_all_manifests;

    let start = std::time::Instant::now();

    let all_manifests = get_all_manifests(&state.db)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to query manifests: {}", e))?;

    if all_manifests.is_empty() {
        tracing::info!("No skip index manifests found, nothing to preload");
        return Ok(0);
    }

    // Group by project_id
    let mut projects: std::collections::HashMap<Uuid, Vec<_>> = std::collections::HashMap::new();
    for m in all_manifests {
        projects.entry(m.project_id).or_default().push(m);
    }

    let total_projects = projects.len();
    tracing::info!(
        project_count = total_projects,
        "Preloading skip indexes from manifests"
    );

    let project_ids: Vec<Uuid> = projects.keys().copied().collect();

    let preload_concurrency = 16;
    let mut preloaded_count = 0usize;
    let mut error_count = 0usize;
    let mut r2_total = 0u32;
    let mut cache_total = 0u32;

    for chunk in project_ids.chunks(preload_concurrency) {
        let futures: Vec<_> = chunk
            .iter()
            .map(|project_id| {
                let db = state.db.clone();
                let project_id = *project_id;
                async move {
                    let result =
                        load_project_skip_indexes(&db, storage, disk_cache, project_id).await;
                    (project_id, result)
                }
            })
            .collect();

        let results = futures::future::join_all(futures).await;

        let mut cache = state.warehouse_skip_indexes.write().await;

        for (project_id, result) in results {
            match result {
                Ok(indexes) => {
                    if !indexes.is_empty() {
                        cache.insert(project_id, indexes);
                        preloaded_count += 1;
                    }
                }
                Err(e) => {
                    error_count += 1;
                    tracing::warn!(
                        project_id = %project_id,
                        error = %e,
                        "Failed to preload skip indexes for project"
                    );
                }
            }
        }
    }

    let duration_ms = start.elapsed().as_millis() as u64;

    tracing::info!(
        preloaded = preloaded_count,
        errors = error_count,
        total_projects = total_projects,
        duration_ms = duration_ms,
        "Skip index preloading completed"
    );

    Ok(preloaded_count)
}

/// Initialize the warehouse cost estimator with table statistics.
///
/// This loads statistics from warehouse_syncs table to provide accurate
/// cost estimates for queries.
#[tracing::instrument(name = "pond.startup.initialize_cost_estimator", skip(db, estimator))]
pub async fn initialize_cost_estimator(
    db: &sqlx::PgPool,
    estimator: &crate::app_state::SharedCostEstimator,
) -> Result<()> {
    let table_count = refresh_cost_estimator_stats(db, estimator).await?;

    tracing::info!(
        table_count = table_count,
        "Initialized warehouse cost estimator with table statistics"
    );

    Ok(())
}

/// Refresh cost estimator statistics from the database.
///
/// This updates table statistics with the latest row counts and sizes from
/// completed sync operations. Returns the number of tables updated.
#[tracing::instrument(name = "warehouse.internal.refresh_cost_estimator_stats", skip_all)]
pub async fn refresh_cost_estimator_stats(
    db: &sqlx::PgPool,
    estimator: &crate::app_state::SharedCostEstimator,
) -> Result<usize> {
    use crate::warehouse::query::cost_estimator::TableStats;
    use sqlx::Row;

    let rows = sqlx::query(
        r#"
        SELECT 
            t.name as table_name,
            COALESCE(SUM(s.rows_synced), 0)::bigint as total_rows,
            COALESCE(SUM(s.bytes_written), 0)::bigint as total_bytes,
            COUNT(DISTINCT s.id)::bigint as file_count
        FROM warehouse_tables t
        LEFT JOIN warehouse_syncs s ON s.source_id = t.source_id 
            AND s.table_name = t.name 
            AND s.status = 'completed'
        GROUP BY t.name
        "#,
    )
    .fetch_all(db)
    .await?;

    let mut est = estimator.write();
    let table_count = rows.len();

    for row in rows {
        let table_name: String = row.try_get("table_name")?;
        let total_rows: i64 = row.try_get("total_rows")?;
        let total_bytes: i64 = row.try_get("total_bytes")?;
        let file_count: i64 = row.try_get("file_count")?;

        let avg_row_size = if total_rows > 0 {
            total_bytes as u64 / total_rows as u64
        } else {
            100 // Default estimate
        };

        est.add_table_stats(TableStats {
            table_name,
            row_count: total_rows as u64,
            size_bytes: total_bytes as u64,
            file_count: file_count as usize,
            avg_row_size,
            last_updated: Some(chrono::Utc::now()),
        });
    }

    Ok(table_count)
}

/// Background task to periodically refresh stale cost estimator statistics.
///
/// This runs in a loop, checking for stale statistics and refreshing them
/// from the database. Stale statistics are those not updated in the last
/// `stale_threshold` duration.
///
/// PERFORMANCE: By keeping statistics fresh, query cost estimation remains
/// accurate, allowing users to make informed decisions about query costs
/// before execution.
#[tracing::instrument(
    name = "pond.background.cost_estimator_refresh",
    skip(state, shutdown_rx)
)]
pub async fn cost_estimator_refresh_worker(
    state: std::sync::Arc<crate::app_state::PondState>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    const REFRESH_INTERVAL_SECS: u64 = 300; // 5 minutes
    const STALE_THRESHOLD_MINS: i64 = 10;

    let stale_threshold = chrono::Duration::minutes(STALE_THRESHOLD_MINS);

    tracing::info!(
        interval_secs = REFRESH_INTERVAL_SECS,
        stale_threshold_mins = STALE_THRESHOLD_MINS,
        "Starting cost estimator refresh worker"
    );

    loop {
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(REFRESH_INTERVAL_SECS)) => {
                // Check if we have stale tables
                let stale_count = {
                    let est = state.warehouse_cost_estimator.read();
                    est.stale_tables(stale_threshold).len()
                };

                if stale_count > 0 {
                    tracing::debug!(
                        stale_tables = stale_count,
                        "Refreshing stale cost estimator statistics"
                    );

                    match refresh_cost_estimator_stats(&state.db, &state.warehouse_cost_estimator).await {
                        Ok(count) => {
                            tracing::debug!(
                                tables_refreshed = count,
                                "Cost estimator statistics refreshed"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                "Failed to refresh cost estimator statistics"
                            );
                        }
                    }
                }
            }
            result = shutdown_rx.changed() => {
                if result.is_ok() && *shutdown_rx.borrow() {
                    tracing::info!("Cost estimator refresh worker shutting down");
                    break;
                }
            }
        }
    }
}

// ===== Compliance / PII Endpoints =====

#[derive(Debug, Serialize, sqlx::FromRow)]
struct PiiFinding {
    id: Uuid,
    project_id: Uuid,
    source_id: Uuid,
    source_name: String,
    table_name: String,
    column_name: String,
    pii_types: serde_json::Value,
    total_rows_scanned: i64,
    rows_with_pii: i64,
    first_detected_at: DateTime<Utc>,
    last_scanned_at: DateTime<Utc>,
    status: String,
    acknowledged_by: Option<Uuid>,
    acknowledged_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
struct PiiFindingsQuery {
    source_id: Option<Uuid>,
    pii_type: Option<String>,
    status: Option<String>,
}

/// List PII findings for a project.
#[tracing::instrument(name = "warehouse.api.list_pii_findings", skip(state), fields(project_id = %project_id), err(Display))]
async fn list_pii_findings(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
    axum::extract::Query(query): axum::extract::Query<PiiFindingsQuery>,
) -> Result<Json<Vec<PiiFinding>>> {
    let findings = sqlx::query_as::<_, PiiFinding>(
        r#"
        SELECT id, project_id, source_id, source_name, table_name, column_name,
               pii_types, total_rows_scanned, rows_with_pii, first_detected_at,
               last_scanned_at, status, acknowledged_by, acknowledged_at
        FROM warehouse_pii_findings
        WHERE project_id = $1
          AND ($2::uuid IS NULL OR source_id = $2)
          AND ($3::text IS NULL OR pii_types @> to_jsonb($3::text))
          AND ($4::text IS NULL OR status = $4)
        ORDER BY rows_with_pii DESC, last_scanned_at DESC
        "#,
    )
    .bind(project_id)
    .bind(query.source_id)
    .bind(&query.pii_type)
    .bind(&query.status)
    .fetch_all(&*state.db)
    .await?;

    Ok(Json(findings))
}

#[derive(Debug, Deserialize)]
struct UpdatePiiFindingRequest {
    status: String,
}

/// Acknowledge or dismiss a PII finding.
#[tracing::instrument(name = "warehouse.api.update_pii_finding", skip(state, headers), fields(project_id = %project_id, finding_id = %finding_id), err(Display))]
async fn update_pii_finding(
    State(state): State<Arc<PondState>>,
    Path((project_id, finding_id)): Path<(Uuid, Uuid)>,
    headers: HeaderMap,
    Json(body): Json<UpdatePiiFindingRequest>,
) -> Result<Json<PiiFinding>> {
    let user_id = extract_user_id(&headers)?;

    if !["open", "acknowledged", "false_positive"].contains(&body.status.as_str()) {
        return Err(AppError::Validation(format!(
            "Invalid status '{}'. Must be one of: open, acknowledged, false_positive",
            body.status
        )));
    }

    let (ack_by, ack_at) = if body.status == "open" {
        (None, None)
    } else {
        (Some(user_id), Some(Utc::now()))
    };

    let finding = sqlx::query_as::<_, PiiFinding>(
        r#"
        UPDATE warehouse_pii_findings
        SET status = $1,
            acknowledged_by = $2,
            acknowledged_at = $3
        WHERE id = $4 AND project_id = $5
        RETURNING id, project_id, source_id, source_name, table_name, column_name,
                  pii_types, total_rows_scanned, rows_with_pii, first_detected_at,
                  last_scanned_at, status, acknowledged_by, acknowledged_at
        "#,
    )
    .bind(&body.status)
    .bind(ack_by)
    .bind(ack_at)
    .bind(finding_id)
    .bind(project_id)
    .fetch_optional(&*state.db)
    .await?
    .ok_or_else(|| AppError::NotFound("PII finding not found".into()))?;

    Ok(Json(finding))
}

#[derive(Debug, Serialize)]
struct PiiComplianceSummary {
    total_findings: i64,
    open_findings: i64,
    sources_with_pii: i64,
    by_pii_type: Vec<PiiTypeCount>,
    by_source: Vec<PiiSourceCount>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
struct PiiTypeCount {
    pii_type: String,
    count: i64,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
struct PiiSourceCount {
    source_name: String,
    source_id: Uuid,
    finding_count: i64,
}

/// Get compliance summary for a project.
#[tracing::instrument(name = "warehouse.api.pii_compliance_summary", skip(state), fields(project_id = %project_id), err(Display))]
async fn pii_compliance_summary(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
) -> Result<Json<PiiComplianceSummary>> {
    let totals = sqlx::query(
        r#"
        SELECT
            COUNT(*) as total_findings,
            COUNT(*) FILTER (WHERE status = 'open') as open_findings,
            COUNT(DISTINCT source_id) as sources_with_pii
        FROM warehouse_pii_findings
        WHERE project_id = $1
        "#,
    )
    .bind(project_id)
    .fetch_one(&*state.db)
    .await?;

    let by_pii_type = sqlx::query_as::<_, PiiTypeCount>(
        r#"
        SELECT t.pii_type, COUNT(DISTINCT f.id) as count
        FROM warehouse_pii_findings f,
             jsonb_array_elements_text(f.pii_types) AS t(pii_type)
        WHERE f.project_id = $1
        GROUP BY t.pii_type
        ORDER BY count DESC
        "#,
    )
    .bind(project_id)
    .fetch_all(&*state.db)
    .await?;

    let by_source = sqlx::query_as::<_, PiiSourceCount>(
        r#"
        SELECT source_name, source_id, COUNT(*) as finding_count
        FROM warehouse_pii_findings
        WHERE project_id = $1
        GROUP BY source_name, source_id
        ORDER BY finding_count DESC
        "#,
    )
    .bind(project_id)
    .fetch_all(&*state.db)
    .await?;

    Ok(Json(PiiComplianceSummary {
        total_findings: totals.get::<i64, _>("total_findings"),
        open_findings: totals.get::<i64, _>("open_findings"),
        sources_with_pii: totals.get::<i64, _>("sources_with_pii"),
        by_pii_type,
        by_source,
    }))
}

// ===== Full-Text Column Configuration =====

#[derive(Debug, Serialize)]
struct FulltextColumnsResponse {
    source_name: String,
    table_name: String,
    fulltext_columns: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct SetFulltextColumnsRequest {
    fulltext_columns: Vec<String>,
}

/// Get the fulltext-indexed columns for a catalog entry.
async fn get_fulltext_columns(
    State(state): State<Arc<PondState>>,
    Path((project_id, source_name, table_name)): Path<(Uuid, String, String)>,
) -> Result<Json<FulltextColumnsResponse>> {
    let catalog_repo =
        crate::warehouse::catalog::repository::CatalogRepository::new(state.db.clone());
    let entry = catalog_repo
        .get_entry(project_id, &source_name, &table_name)
        .await
        .map_err(|e| AppError::NotFound(format!("Catalog entry not found: {}", e)))?;

    Ok(Json(FulltextColumnsResponse {
        source_name: entry.source_name,
        table_name: entry.table_name,
        fulltext_columns: entry.fulltext_columns,
    }))
}

/// Set which columns should be fulltext-indexed for substring search.
async fn set_fulltext_columns(
    State(state): State<Arc<PondState>>,
    Path((project_id, source_name, table_name)): Path<(Uuid, String, String)>,
    Json(body): Json<SetFulltextColumnsRequest>,
) -> Result<Json<FulltextColumnsResponse>> {
    let catalog_repo =
        crate::warehouse::catalog::repository::CatalogRepository::new(state.db.clone());

    // Validate the entry exists
    let entry = catalog_repo
        .get_entry(project_id, &source_name, &table_name)
        .await
        .map_err(|e| AppError::NotFound(format!("Catalog entry not found: {}", e)))?;

    // Validate that requested columns are string-type columns in the schema
    let string_columns: std::collections::HashSet<&str> = entry
        .schema
        .columns
        .iter()
        .filter(|c| {
            matches!(
                c.arrow_data_type_or_string(),
                arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::LargeUtf8
            )
        })
        .map(|c| c.name.as_str())
        .collect();

    let invalid: Vec<&str> = body
        .fulltext_columns
        .iter()
        .filter(|c| !string_columns.contains(c.as_str()))
        .map(|c| c.as_str())
        .collect();

    if !invalid.is_empty() {
        return Err(AppError::BadRequest(format!(
            "The following columns are not string columns or do not exist: {}",
            invalid.join(", ")
        )));
    }

    catalog_repo
        .update_fulltext_columns(
            project_id,
            &source_name,
            &table_name,
            &body.fulltext_columns,
        )
        .await
        .map_err(|e| {
            AppError::Internal(anyhow::anyhow!("Failed to update fulltext columns: {}", e))
        })?;

    Ok(Json(FulltextColumnsResponse {
        source_name,
        table_name,
        fulltext_columns: body.fulltext_columns,
    }))
}

// ===== Mutation Churn Analysis =====

/// Query parameters for mutation churn endpoint.
#[derive(Debug, Deserialize)]
struct MutationChurnQuery {
    /// Number of days to look back (default: 7)
    days: Option<u32>,
    /// Mutation threshold to flag (default: 100000)
    threshold: Option<u64>,
}

/// Response for mutation churn analysis.
#[derive(Debug, Serialize)]
struct MutationChurnResponse {
    /// Sources exceeding the mutation threshold
    high_churn_sources: Vec<crate::warehouse::storage::clickhouse::HighChurnSource>,
    /// Recommendation message
    recommendation: Option<String>,
}

/// Get mutation churn analysis for a project.
///
/// Returns sources/tables that have high update/delete rates, with a recommendation
/// to promote them to the hot tier for better query performance.
#[tracing::instrument(
    name = "api.warehouse.mutation_churn",
    skip_all,
    fields(%project_id),
)]
async fn get_mutation_churn(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
    axum::extract::Query(params): axum::extract::Query<MutationChurnQuery>,
) -> Result<Json<MutationChurnResponse>> {
    let days = params.days.unwrap_or(7);
    let threshold = params.threshold.unwrap_or(100_000);

    // Query mutation stats via the query executor's HTTP client.
    // Note: project_id (UUID), days (u32), and threshold (u64) are all typed values
    // that cannot cause SQL injection. table_name is not interpolated here.
    let executor = &state.warehouse_query_executor;
    let sql = format!(
        r#"SELECT
            toString(source_id) AS source_id,
            table_name,
            sum(update_count) AS total_updates,
            sum(delete_count) AS total_deletes
        FROM warehouse_mutation_stats
        WHERE project_id = '{}'
          AND stat_date >= today() - {}
        GROUP BY source_id, table_name
        HAVING total_updates + total_deletes > {}
        FORMAT JSONEachRow"#,
        project_id, days, threshold,
    );

    // Execute via the query executor
    let result = executor.execute_raw_query(&sql).await;
    let high_churn = match result {
        Ok(response) => {
            let mut sources = Vec::new();
            for line in response.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                if let Ok(source) = serde_json::from_str::<
                    crate::warehouse::storage::clickhouse::HighChurnSource,
                >(line)
                {
                    sources.push(source);
                }
            }
            sources
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                "Failed to query mutation stats (table may not exist yet)"
            );
            Vec::new()
        }
    };

    let recommendation = if !high_churn.is_empty() {
        Some(format!(
            "{} source table(s) exceeded {} mutations in the last {} days. \
             Consider promoting high-churn sources to the hot tier for faster queries.",
            high_churn.len(),
            threshold,
            days
        ))
    } else {
        None
    };

    Ok(Json(MutationChurnResponse {
        high_churn_sources: high_churn,
        recommendation,
    }))
}

// ===== Blockchain Sources =====

/// Path parameters for blockchain endpoint.
#[derive(Debug, Deserialize)]
struct BlockchainPath {
    project_id: Uuid,
    chain: String,
}

/// Optional request body for enabling a blockchain source.
#[derive(Debug, Deserialize)]
struct EnableBlockchainBody {
    /// Custom name for the source. Defaults to the chain name if omitted.
    name: Option<String>,
}

/// Response after enabling a blockchain source for a project.
#[derive(Debug, Serialize)]
struct BlockchainSourceResponse {
    source_id: Uuid,
    chain: String,
    name: String,
    tables: Vec<String>,
    tier: String,
}

/// Enable a blockchain data source for a project.
///
/// Creates a lightweight reference source that points to globally-synced
/// blockchain data.  No per-project sync job is needed — the global daemon
/// keeps the data up to date.
#[tracing::instrument(
    name = "warehouse.api.enable_blockchain",
    skip(state),
    fields(project_id = %path.project_id, chain = %path.chain),
    err(Display),
)]
async fn enable_blockchain(
    State(state): State<Arc<PondState>>,
    Path(path): Path<BlockchainPath>,
    body: Option<Json<EnableBlockchainBody>>,
) -> Result<Json<BlockchainSourceResponse>> {
    let chain = path.chain.to_lowercase();

    // Validate the chain is supported
    let source_type = match chain.as_str() {
        "bitcoin" => SourceType::Bitcoin,
        "ethereum" => SourceType::Ethereum,
        "solana" => SourceType::Solana,
        "polygon" => SourceType::Polygon,
        _ => {
            return Err(AppError::Validation(format!(
                "Unsupported blockchain: '{}'. Supported chains: bitcoin, ethereum, solana, polygon.",
                chain
            )));
        }
    };

    // Look up the global source
    let global_source = sqlx::query(
        "SELECT id, chain, r2_prefix FROM blockchain_global_sources
         WHERE chain = $1 AND enabled = true",
    )
    .bind(&chain)
    .fetch_optional(&*state.db)
    .await?
    .ok_or_else(|| {
        AppError::NotFound(format!(
            "Blockchain '{}' is not enabled globally. Contact your administrator.",
            chain
        ))
    })?;

    let global_source_id: Uuid = global_source.get("id");
    let r2_prefix: String = global_source.get("r2_prefix");

    // Check if this project already has this blockchain enabled
    let existing: Option<(Uuid,)> = sqlx::query_as(
        "SELECT id FROM warehouse_sources
         WHERE project_id = $1 AND global_source_id = $2",
    )
    .bind(path.project_id)
    .bind(global_source_id)
    .fetch_optional(&*state.db)
    .await?;

    if let Some((existing_id,)) = existing {
        return Err(AppError::Validation(format!(
            "Blockchain '{}' is already enabled for this project (source_id: {}).",
            chain, existing_id
        )));
    }

    let source_name = body.and_then(|b| b.0.name).unwrap_or_else(|| chain.clone());

    // Ensure no other source in the project already uses this name.
    let name_conflict: Option<(Uuid,)> = sqlx::query_as(
        "SELECT id FROM warehouse_sources
         WHERE project_id = $1 AND LOWER(name) = LOWER($2)",
    )
    .bind(path.project_id)
    .bind(&source_name)
    .fetch_optional(&*state.db)
    .await?;

    if let Some((conflicting_id,)) = name_conflict {
        return Err(AppError::Validation(format!(
            "A source named '{}' already exists in this project (source_id: {}).",
            source_name, conflicting_id
        )));
    }

    let source_id = Uuid::new_v4();
    let now = Utc::now();

    // Compute a stable connection hash for the blockchain reference
    let connection_hash = format!("blockchain_{}_{}", chain, global_source_id);

    let mut tx = state.db.begin().await?;

    // Create the reference source
    sqlx::query(
        "INSERT INTO warehouse_sources
            (id, project_id, name, source_type, storage_type, config, tier,
             connection_config_hash, storage_tier_policy, sync_scope,
             global_source_id, supports_cdc, enabled, created_at, updated_at)
         VALUES ($1, $2, $3, $4, 'object_storage', '{}'::jsonb, 'warm',
                 $5, '{\"type\": \"fixed\", \"tier\": \"warm\"}'::jsonb, 'full',
                 $6, false, true, $7, $7)",
    )
    .bind(source_id)
    .bind(path.project_id)
    .bind(&source_name)
    .bind(source_type.to_string())
    .bind(&connection_hash)
    .bind(global_source_id)
    .bind(now)
    .execute(&mut *tx)
    .await?;

    // Determine tables based on chain, with their schemas.
    let table_names: Vec<&str> = match chain.as_str() {
        "bitcoin" => vec!["blocks", "transactions", "inputs", "outputs"],
        "ethereum" => vec!["blocks", "transactions", "logs"],
        _ => vec!["blocks", "transactions"],
    };

    // Create warehouse_tables entries pointing to the global R2 prefix.
    // The schema column is populated from the connector's schema definitions
    // so the query planner and UI have proper column metadata.
    for table_name in &table_names {
        let table_r2_prefix = format!("{}/{}", r2_prefix, table_name);

        let schema_json = match chain.as_str() {
            "bitcoin" => {
                use crate::warehouse::connectors::blockchain::schema as btc_schema;
                btc_schema::schema_for_table(table_name)
                    .and_then(|s| serde_json::to_value(&s).ok())
                    .unwrap_or_else(|| serde_json::json!({}))
            }
            "ethereum" => {
                use crate::warehouse::connectors::blockchain::eth_schema;
                eth_schema::schema_for_table(table_name)
                    .and_then(|s| serde_json::to_value(&s).ok())
                    .unwrap_or_else(|| serde_json::json!({}))
            }
            _ => serde_json::json!({}),
        };

        sqlx::query(
            "INSERT INTO warehouse_tables
                (id, source_id, name, schema, r2_prefix, sync_enabled,
                 sync_state, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, true, 'committed', $6, $6)",
        )
        .bind(Uuid::new_v4())
        .bind(source_id)
        .bind(*table_name)
        .bind(&schema_json)
        .bind(&table_r2_prefix)
        .bind(now)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;

    tracing::info!(
        project_id = %path.project_id,
        chain = %chain,
        source_name = %source_name,
        source_id = %source_id,
        tables = ?table_names,
        "Blockchain source enabled for project"
    );

    Ok(Json(BlockchainSourceResponse {
        source_id,
        chain,
        name: source_name,
        tables: table_names.iter().map(|s| s.to_string()).collect(),
        tier: "warm".to_string(),
    }))
}

// ===== Derived Tables (CTAS / Materialized Views) =====

#[derive(Debug, Deserialize)]
struct CreateDerivedTableApiRequest {
    name: String,
    sql: String,
    description: Option<String>,
    refresh_mode: Option<String>,
    schedule: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AppendDerivedTableRequest {
    sql: String,
}

#[derive(Debug, Deserialize)]
struct SetScheduleRequest {
    schedule: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ListDerivedTablesQuery {
    #[serde(default = "default_list_limit")]
    limit: i64,
    #[serde(default)]
    offset: i64,
}

fn default_list_limit() -> i64 {
    100
}

#[derive(Debug, Serialize)]
struct DerivedTableListResponse {
    items: Vec<DerivedTableResponse>,
    total_count: i64,
}

#[derive(Debug, Serialize)]
struct DerivedTableResponse {
    id: Uuid,
    name: String,
    sql: String,
    description: Option<String>,
    refresh_mode: String,
    schedule: Option<String>,
    last_refreshed_at: Option<DateTime<Utc>>,
    last_refresh_duration_ms: Option<i64>,
    last_refresh_rows: Option<i64>,
    row_count: i64,
    size_bytes: i64,
    last_error: Option<String>,
    created_by: Option<Uuid>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl From<crate::warehouse::derived::DerivedTable> for DerivedTableResponse {
    fn from(dt: crate::warehouse::derived::DerivedTable) -> Self {
        Self {
            id: dt.id,
            name: dt.name,
            sql: dt.sql,
            description: dt.description,
            refresh_mode: dt.refresh_mode.to_string(),
            schedule: dt.schedule,
            last_refreshed_at: dt.last_refreshed_at,
            last_refresh_duration_ms: dt.last_refresh_duration_ms,
            last_refresh_rows: dt.last_refresh_rows,
            row_count: dt.row_count,
            size_bytes: dt.size_bytes,
            last_error: dt.last_error,
            created_by: dt.created_by,
            created_at: dt.created_at,
            updated_at: dt.updated_at,
        }
    }
}

/// Get the shared DerivedTableManager from PondState.
fn derived_table_manager(
    state: &PondState,
) -> Result<&Arc<crate::warehouse::derived::DerivedTableManager>> {
    state.derived_table_manager.as_ref().ok_or_else(|| {
        AppError::Internal(anyhow::anyhow!(
            "Derived table manager not configured (requires R2 and ClickHouse)"
        ))
    })
}

/// POST /projects/:project_id/warehouse/derived-tables
///
/// Create a new derived table from a SQL query (CTAS).
#[tracing::instrument(
    name = "warehouse.api.create_derived_table",
    skip(state, headers, req),
    fields(%project_id),
    err(Display),
)]
async fn create_derived_table(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
    headers: HeaderMap,
    Json(req): Json<CreateDerivedTableApiRequest>,
) -> Result<(StatusCode, Json<DerivedTableResponse>)> {
    let user_id = extract_user_id(&headers)?;

    // Rewrite the SQL through the standard validation pipeline
    let rewritten_sql = validate_and_rewrite_query(&state, project_id, &req.sql)
        .await?
        .sql;

    let manager = derived_table_manager(&state)?;

    let create_req = crate::warehouse::derived::CreateDerivedTableRequest {
        name: req.name,
        sql: req.sql,
        description: req.description,
        refresh_mode: req.refresh_mode,
        schedule: req.schedule,
    };

    let dt = manager
        .create(project_id, Some(user_id), &create_req, &rewritten_sql)
        .await
        .map_err(|e| map_derived_error(e))?;

    Ok((StatusCode::CREATED, Json(DerivedTableResponse::from(dt))))
}

/// GET /projects/:project_id/warehouse/derived-tables
#[tracing::instrument(
    name = "warehouse.api.list_derived_tables",
    skip(state),
    fields(%project_id),
    err(Display),
)]
async fn list_derived_tables(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<ListDerivedTablesQuery>,
) -> Result<Json<DerivedTableListResponse>> {
    let limit = params.limit.clamp(1, 1000);
    let offset = params.offset.max(0);

    let manager = derived_table_manager(&state)?;
    let result = manager
        .list(project_id, limit, offset)
        .await
        .map_err(|e| map_derived_error(e))?;

    Ok(Json(DerivedTableListResponse {
        items: result
            .items
            .into_iter()
            .map(DerivedTableResponse::from)
            .collect(),
        total_count: result.total_count,
    }))
}

/// GET /projects/:project_id/warehouse/derived-tables/:derived_id
#[tracing::instrument(
    name = "warehouse.api.get_derived_table",
    skip(state),
    fields(%project_id, %derived_id),
    err(Display),
)]
async fn get_derived_table(
    State(state): State<Arc<PondState>>,
    Path((project_id, derived_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<DerivedTableResponse>> {
    let manager = derived_table_manager(&state)?;
    let dt = manager
        .get(project_id, derived_id)
        .await
        .map_err(|e| map_derived_error(e))?
        .ok_or_else(|| AppError::NotFound("Derived table not found".to_string()))?;

    Ok(Json(DerivedTableResponse::from(dt)))
}

/// POST /projects/:project_id/warehouse/derived-tables/:derived_id/refresh
#[tracing::instrument(
    name = "warehouse.api.refresh_derived_table",
    skip(state),
    fields(%project_id, %derived_id),
    err(Display),
)]
async fn refresh_derived_table(
    State(state): State<Arc<PondState>>,
    Path((project_id, derived_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<serde_json::Value>> {
    let manager = derived_table_manager(&state)?;

    // Load the derived table to get its SQL
    let dt = manager
        .get(project_id, derived_id)
        .await
        .map_err(|e| map_derived_error(e))?
        .ok_or_else(|| AppError::NotFound("Derived table not found".to_string()))?;

    // For incremental mode, substitute {{last_refresh}} before rewriting
    let sql = if dt.refresh_mode == crate::warehouse::derived::RefreshMode::Incremental {
        crate::warehouse::derived::substitute_last_refresh(&dt.sql, dt.last_refreshed_at)
    } else {
        dt.sql.clone()
    };

    let rewritten_sql = validate_and_rewrite_query(&state, project_id, &sql)
        .await?
        .sql;

    let result = manager
        .refresh(&dt, &rewritten_sql)
        .await
        .map_err(|e| map_derived_error(e))?;

    Ok(Json(serde_json::json!({
        "row_count": result.row_count,
        "bytes_written": result.bytes_written,
        "files_created": result.files_created,
        "duration_ms": result.duration_ms,
    })))
}

/// POST /projects/:project_id/warehouse/derived-tables/:derived_id/append
#[tracing::instrument(
    name = "warehouse.api.append_derived_table",
    skip(state, req),
    fields(%project_id, %derived_id),
    err(Display),
)]
async fn append_derived_table(
    State(state): State<Arc<PondState>>,
    Path((project_id, derived_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<AppendDerivedTableRequest>,
) -> Result<Json<serde_json::Value>> {
    let manager = derived_table_manager(&state)?;

    let dt = manager
        .get(project_id, derived_id)
        .await
        .map_err(|e| map_derived_error(e))?
        .ok_or_else(|| AppError::NotFound("Derived table not found".to_string()))?;

    let rewritten_sql = validate_and_rewrite_query(&state, project_id, &req.sql)
        .await?
        .sql;

    let result = manager
        .append(&dt, &rewritten_sql)
        .await
        .map_err(|e| map_derived_error(e))?;

    Ok(Json(serde_json::json!({
        "row_count": result.row_count,
        "bytes_written": result.bytes_written,
        "files_created": result.files_created,
        "duration_ms": result.duration_ms,
    })))
}

/// POST /projects/:project_id/warehouse/derived-tables/:derived_id/compact
#[tracing::instrument(
    name = "warehouse.api.compact_derived_table",
    skip(state),
    fields(%project_id, %derived_id),
    err(Display),
)]
async fn compact_derived_table(
    State(state): State<Arc<PondState>>,
    Path((project_id, derived_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<serde_json::Value>> {
    let manager = derived_table_manager(&state)?;

    let dt = manager
        .get(project_id, derived_id)
        .await
        .map_err(|e| map_derived_error(e))?
        .ok_or_else(|| AppError::NotFound("Derived table not found".to_string()))?;

    let result = manager
        .compact(&dt)
        .await
        .map_err(|e| map_derived_error(e))?;

    Ok(Json(serde_json::json!({
        "files_before": result.files_before,
        "files_after": result.files_after,
        "rows": result.rows,
        "bytes": result.bytes,
    })))
}

/// PUT /projects/:project_id/warehouse/derived-tables/:derived_id/schedule
#[tracing::instrument(
    name = "warehouse.api.set_derived_table_schedule",
    skip(state, req),
    fields(%project_id, %derived_id),
    err(Display),
)]
async fn set_derived_table_schedule(
    State(state): State<Arc<PondState>>,
    Path((project_id, derived_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<SetScheduleRequest>,
) -> Result<StatusCode> {
    let manager = derived_table_manager(&state)?;
    manager
        .set_schedule(project_id, derived_id, req.schedule.as_deref())
        .await
        .map_err(|e| map_derived_error(e))?;

    Ok(StatusCode::NO_CONTENT)
}

/// DELETE /projects/:project_id/warehouse/derived-tables/:derived_id
#[tracing::instrument(
    name = "warehouse.api.delete_derived_table",
    skip(state),
    fields(%project_id, %derived_id),
    err(Display),
)]
async fn delete_derived_table(
    State(state): State<Arc<PondState>>,
    Path((project_id, derived_id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode> {
    let manager = derived_table_manager(&state)?;
    manager
        .delete(project_id, derived_id)
        .await
        .map_err(|e| map_derived_error(e))?;

    Ok(StatusCode::NO_CONTENT)
}

/// Map DerivedTableManager errors to appropriate HTTP status codes.
///
/// Uses typed `DerivedError` via `downcast_ref` instead of fragile string
/// matching on error messages.
fn map_derived_error(e: anyhow::Error) -> AppError {
    use crate::warehouse::derived::DerivedError;

    if let Some(de) = e.downcast_ref::<DerivedError>() {
        match de {
            DerivedError::NotFound(msg) => AppError::NotFound(msg.clone()),
            DerivedError::Validation(msg) => AppError::BadRequest(msg.clone()),
            DerivedError::Conflict(msg) => AppError::Conflict(msg.clone()),
        }
    } else {
        AppError::Internal(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::warehouse::storage::r2::ObjectInfo;
    use chrono::TimeZone;

    fn make_obj(key: &str, size: u64, last_modified: Option<DateTime<Utc>>) -> ObjectInfo {
        ObjectInfo {
            key: key.to_string(),
            size,
            last_modified,
            etag: None,
        }
    }

    #[test]
    fn test_group_objects_empty() {
        let result = group_objects_into_partitions(&[]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_group_objects_flat_layout() {
        let objects = vec![
            make_obj("data/file1.parquet", 1000, None),
            make_obj("data/file2.parquet", 2000, None),
            make_obj("data/file3.parquet", 500, None),
        ];

        let result = group_objects_into_partitions(&objects);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].partition_key, "/");
        assert_eq!(result[0].file_count, 3);
        assert_eq!(result[0].estimated_size_bytes, Some(3500));
        assert!(result[0].is_mutable);
    }

    #[test]
    fn test_group_objects_hive_partitioned() {
        let objects = vec![
            make_obj("data/year=2024/month=01/a.parquet", 100, None),
            make_obj("data/year=2024/month=01/b.parquet", 200, None),
            make_obj("data/year=2024/month=02/c.parquet", 300, None),
            make_obj("data/year=2025/month=01/d.parquet", 400, None),
        ];

        let result = group_objects_into_partitions(&objects);
        assert_eq!(result.len(), 3);

        let p1 = result
            .iter()
            .find(|p| p.partition_key == "year=2024/month=01")
            .unwrap();
        assert_eq!(p1.file_count, 2);
        assert_eq!(p1.estimated_size_bytes, Some(300));

        let p2 = result
            .iter()
            .find(|p| p.partition_key == "year=2024/month=02")
            .unwrap();
        assert_eq!(p2.file_count, 1);
        assert_eq!(p2.estimated_size_bytes, Some(300));

        let p3 = result
            .iter()
            .find(|p| p.partition_key == "year=2025/month=01")
            .unwrap();
        assert_eq!(p3.file_count, 1);
        assert_eq!(p3.estimated_size_bytes, Some(400));
    }

    #[test]
    fn test_group_objects_last_modified_picks_newest() {
        let t1 = Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).unwrap();
        let t2 = Utc.with_ymd_and_hms(2024, 7, 15, 12, 0, 0).unwrap();

        let objects = vec![
            make_obj("data/year=2024/month=06/a.parquet", 100, Some(t1)),
            make_obj("data/year=2024/month=06/b.parquet", 200, Some(t2)),
        ];

        let result = group_objects_into_partitions(&objects);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].last_modified, Some(t2));
    }

    #[test]
    fn test_group_objects_mixed_non_hive_falls_back_to_flat() {
        let objects = vec![
            make_obj("data/2024/01/file1.parquet", 100, None),
            make_obj("data/2024/02/file2.parquet", 200, None),
            make_obj("data/file3.parquet", 300, None),
        ];

        let result = group_objects_into_partitions(&objects);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].partition_key, "/");
        assert_eq!(result[0].file_count, 3);
    }

    #[test]
    fn test_group_objects_sorted_by_partition_key() {
        let objects = vec![
            make_obj("data/year=2025/month=03/a.parquet", 100, None),
            make_obj("data/year=2024/month=01/b.parquet", 200, None),
            make_obj("data/year=2024/month=12/c.parquet", 300, None),
        ];

        let result = group_objects_into_partitions(&objects);
        let keys: Vec<&str> = result.iter().map(|p| p.partition_key.as_str()).collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted);
    }

    #[test]
    fn test_validate_and_limit_caps_literal() {
        let sql = "SELECT * FROM users LIMIT 50000";
        let result = validate_and_limit_query(sql, 10000).unwrap();
        let lower = result.to_lowercase();
        assert!(
            lower.contains("limit 10000"),
            "Literal LIMIT exceeding max must be capped. Got: {result}"
        );
    }

    #[test]
    fn test_validate_and_limit_adds_when_missing() {
        let sql = "SELECT * FROM users";
        let result = validate_and_limit_query(sql, 10000).unwrap();
        let lower = result.to_lowercase();
        assert!(
            lower.contains("limit"),
            "Missing LIMIT must be injected. Got: {result}"
        );
    }

    #[test]
    fn test_validate_and_limit_non_literal_replaced() {
        let sql = "SELECT * FROM users LIMIT 1+999999";
        let result = validate_and_limit_query(sql, 10000).unwrap();
        let lower = result.to_lowercase();
        assert!(
            !lower.contains("999999"),
            "Non-literal LIMIT expression must be replaced with max. Got: {result}"
        );
        assert!(
            lower.contains("10000"),
            "Non-literal LIMIT must be replaced with the cap. Got: {result}"
        );
    }

    #[test]
    fn test_cross_join_in_union_detected() {
        let sql = "SELECT * FROM a CROSS JOIN b UNION ALL SELECT * FROM c";
        let result = validate_and_limit_query(sql, 10000);
        assert!(
            result.is_err(),
            "CROSS JOIN inside UNION must be detected and rejected, got: {:?}",
            result
        );
    }

    #[test]
    fn test_cross_join_simple_detected() {
        let sql = "SELECT * FROM a CROSS JOIN b";
        let result = validate_and_limit_query(sql, 10000);
        assert!(result.is_err(), "Simple CROSS JOIN must be rejected");
    }

    #[test]
    fn test_no_cross_join_union_passes() {
        let sql = "SELECT * FROM a UNION ALL SELECT * FROM b";
        let result = validate_and_limit_query(sql, 10000);
        assert!(
            result.is_ok(),
            "UNION without CROSS JOIN should pass validation, got: {:?}",
            result
        );
    }

    #[test]
    fn test_cross_join_inside_cte_detected() {
        let sql = "WITH cte AS (SELECT * FROM a CROSS JOIN b) SELECT * FROM cte";
        let result = validate_and_limit_query(sql, 10000);
        assert!(
            result.is_err(),
            "CROSS JOIN inside a CTE must be detected and rejected, got: {:?}",
            result
        );
    }

    #[test]
    fn test_build_postgres_connection_string_from_fields() {
        let config = serde_json::json!({
            "host": "db.example.com",
            "port": 5433,
            "database": "mydb",
            "username": "admin",
            "password": "secret"
        });
        let result = build_postgres_connection_string(&config);
        assert_eq!(result, "postgresql://admin:secret@db.example.com:5433/mydb");
    }

    #[test]
    fn test_build_postgres_connection_string_prefers_connection_string() {
        let config = serde_json::json!({
            "connection_string": "postgresql://user:pass@host:5432/db",
            "host": "other.host",
        });
        let result = build_postgres_connection_string(&config);
        assert_eq!(result, "postgresql://user:pass@host:5432/db");
    }

    #[test]
    fn test_build_postgres_connection_string_default_port() {
        let config = serde_json::json!({
            "host": "localhost",
            "database": "testdb",
            "username": "user"
        });
        let result = build_postgres_connection_string(&config);
        assert_eq!(result, "postgresql://user:@localhost:5432/testdb");
    }
}

// ===== UDF Endpoints =====

#[derive(Debug, Deserialize)]
struct CreateUdfRequest {
    name: String,
    source_code: String,
    #[serde(default = "default_fuel_limit")]
    fuel_limit: Option<u64>,
    #[serde(default)]
    timeout_secs: Option<u32>,
}

fn default_fuel_limit() -> Option<u64> {
    Some(10_000_000)
}

#[derive(Debug, Deserialize)]
struct CreateJobRequest {
    name: String,
    source_code: String,
    source: crate::warehouse::udf::job_executor::JobSourceConfig,
    sink: crate::warehouse::udf::job_executor::JobSinkConfig,
    schedule: Option<String>,
    #[serde(default = "default_fuel_limit")]
    fuel_limit: Option<u64>,
    #[serde(default)]
    timeout_secs: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct UpdateJobRequest {
    schedule: Option<String>,
    source: Option<crate::warehouse::udf::job_executor::JobSourceConfig>,
    sink: Option<crate::warehouse::udf::job_executor::JobSinkConfig>,
    fuel_limit: Option<u64>,
    timeout_secs: Option<u32>,
}

const MAX_UDF_NAME_LEN: usize = 128;
const MAX_FUEL_LIMIT: u64 = 1_000_000_000;
const MIN_FUEL_LIMIT: u64 = 1_000;
const MAX_TIMEOUT_SECS: u32 = 3600;

fn validate_udf_name(name: &str) -> std::result::Result<(), AppError> {
    if name.is_empty() {
        return Err(AppError::Validation("name must not be empty".to_string()));
    }
    if name.len() > MAX_UDF_NAME_LEN {
        return Err(AppError::Validation(format!(
            "name must be at most {} characters",
            MAX_UDF_NAME_LEN
        )));
    }
    if !name.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_') {
        return Err(AppError::Validation(
            "name must start with a letter or underscore".to_string(),
        ));
    }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(AppError::Validation(
            "name must contain only letters, digits, and underscores".to_string(),
        ));
    }
    Ok(())
}

fn validate_udf_params(
    source_code: &str,
    fuel_limit: Option<u64>,
    timeout_secs: Option<u32>,
) -> std::result::Result<(), AppError> {
    if source_code.trim().is_empty() {
        return Err(AppError::Validation(
            "source_code must not be empty".to_string(),
        ));
    }
    if let Some(fuel) = fuel_limit {
        if fuel < MIN_FUEL_LIMIT || fuel > MAX_FUEL_LIMIT {
            return Err(AppError::Validation(format!(
                "fuel_limit must be between {} and {}",
                MIN_FUEL_LIMIT, MAX_FUEL_LIMIT
            )));
        }
    }
    if let Some(timeout) = timeout_secs {
        if timeout == 0 || timeout > MAX_TIMEOUT_SECS {
            return Err(AppError::Validation(format!(
                "timeout_secs must be between 1 and {}",
                MAX_TIMEOUT_SECS
            )));
        }
    }
    Ok(())
}

async fn create_udf(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
    Json(req): Json<CreateUdfRequest>,
) -> Result<Json<serde_json::Value>> {
    validate_udf_name(&req.name)?;
    validate_udf_params(&req.source_code, req.fuel_limit, req.timeout_secs)?;

    let registry = state
        .udf_registry
        .as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("UDF system not initialized")))?;

    let manifest = registry
        .register(
            project_id,
            &req.name,
            &req.source_code,
            crate::warehouse::udf::ExecutionMode::SqlFunction,
            None,
            req.fuel_limit,
            req.timeout_secs,
            None,
        )
        .await
        .map_err(|e| AppError::BadRequest(format!("UDF compilation failed: {}", e)))?;

    Ok(Json(serde_json::json!({
        "name": req.name,
        "manifest": serde_json::to_value(&manifest).unwrap_or_default(),
    })))
}

async fn list_udfs(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>> {
    let registry = state
        .udf_registry
        .as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("UDF system not initialized")))?;

    let udfs = registry
        .list(project_id)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to list UDFs: {}", e)))?;

    Ok(Json(serde_json::json!({ "udfs": udfs })))
}

async fn get_udf(
    State(state): State<Arc<PondState>>,
    Path((project_id, udf_name)): Path<(Uuid, String)>,
) -> Result<Json<serde_json::Value>> {
    let registry = state
        .udf_registry
        .as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("UDF system not initialized")))?;

    let udf = registry
        .get_info(project_id, &udf_name)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to get UDF: {}", e)))?
        .ok_or_else(|| AppError::NotFound(format!("UDF '{}' not found", udf_name)))?;

    Ok(Json(serde_json::json!(udf)))
}

async fn delete_udf(
    State(state): State<Arc<PondState>>,
    Path((project_id, udf_name)): Path<(Uuid, String)>,
) -> Result<Json<serde_json::Value>> {
    let registry = state
        .udf_registry
        .as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("UDF system not initialized")))?;

    registry
        .delete(project_id, &udf_name)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to delete UDF: {}", e)))?;

    Ok(Json(serde_json::json!({"deleted": true})))
}

async fn create_job(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
    Json(req): Json<CreateJobRequest>,
) -> Result<Json<serde_json::Value>> {
    validate_udf_name(&req.name)?;
    validate_udf_params(&req.source_code, req.fuel_limit, req.timeout_secs)?;

    if req.source.name.trim().is_empty() {
        return Err(AppError::Validation(
            "source.name must not be empty".to_string(),
        ));
    }
    if req.sink.name.trim().is_empty() {
        return Err(AppError::Validation(
            "sink.name must not be empty".to_string(),
        ));
    }
    if req.sink.table.trim().is_empty() {
        return Err(AppError::Validation(
            "sink.table must not be empty".to_string(),
        ));
    }
    if let Some(ref schedule) = req.schedule {
        validate_cron_expression(schedule)?;
    }

    let registry = state
        .udf_registry
        .as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("UDF system not initialized")))?;

    let job_config = crate::warehouse::udf::job_executor::JobConfig {
        source: req.source,
        sink: req.sink,
        parallelism: None,
    };

    let manifest = registry
        .register(
            project_id,
            &req.name,
            &req.source_code,
            crate::warehouse::udf::ExecutionMode::Job,
            req.schedule.as_deref(),
            req.fuel_limit,
            req.timeout_secs,
            Some(serde_json::to_value(&job_config).map_err(|e| {
                AppError::Internal(anyhow::anyhow!("Failed to serialize job config: {}", e))
            })?),
        )
        .await
        .map_err(|e| AppError::BadRequest(format!("Job creation failed: {}", e)))?;

    Ok(Json(serde_json::json!({
        "name": req.name,
        "manifest": serde_json::to_value(&manifest).unwrap_or_default(),
    })))
}

async fn get_job(
    State(state): State<Arc<PondState>>,
    Path((project_id, job_name)): Path<(Uuid, String)>,
) -> Result<Json<serde_json::Value>> {
    let registry = state
        .udf_registry
        .as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("UDF system not initialized")))?;

    let job = registry
        .get_info(project_id, &job_name)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to get job: {}", e)))?
        .ok_or_else(|| AppError::NotFound(format!("Job '{}' not found", job_name)))?;

    if job.execution_mode != "job" {
        return Err(AppError::NotFound(format!("Job '{}' not found", job_name)));
    }

    Ok(Json(serde_json::json!(job)))
}

async fn update_job(
    State(state): State<Arc<PondState>>,
    Path((project_id, job_name)): Path<(Uuid, String)>,
    Json(req): Json<UpdateJobRequest>,
) -> Result<Json<serde_json::Value>> {
    let registry = state
        .udf_registry
        .as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("UDF system not initialized")))?;

    let has_any_update = req.schedule.is_some()
        || req.fuel_limit.is_some()
        || req.timeout_secs.is_some()
        || req.source.is_some()
        || req.sink.is_some();

    if !has_any_update {
        return Ok(Json(serde_json::json!({"updated": false})));
    }

    if let Some(fuel) = req.fuel_limit {
        if fuel < MIN_FUEL_LIMIT || fuel > MAX_FUEL_LIMIT {
            return Err(AppError::Validation(format!(
                "fuel_limit must be between {} and {}",
                MIN_FUEL_LIMIT, MAX_FUEL_LIMIT
            )));
        }
    }
    if let Some(timeout) = req.timeout_secs {
        if timeout == 0 || timeout > MAX_TIMEOUT_SECS {
            return Err(AppError::Validation(format!(
                "timeout_secs must be between 1 and {}",
                MAX_TIMEOUT_SECS
            )));
        }
    }
    if let Some(ref schedule) = req.schedule {
        validate_cron_expression(schedule)?;
    }

    let current: Option<(Option<String>, i64, i32, Option<serde_json::Value>)> = sqlx::query_as(
        "SELECT schedule, fuel_limit, timeout_secs, job_config FROM warehouse_udfs \
             WHERE project_id = $1 AND name = $2 AND execution_mode = 'job'",
    )
    .bind(project_id)
    .bind(&job_name)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to load job: {}", e)))?;

    let (cur_schedule, cur_fuel, cur_timeout, cur_config) =
        current.ok_or_else(|| AppError::NotFound(format!("Job '{}' not found", job_name)))?;

    let new_schedule = if req.schedule.is_some() {
        req.schedule
    } else {
        cur_schedule
    };
    let new_fuel = req.fuel_limit.map(|v| v as i64).unwrap_or(cur_fuel);
    let new_timeout = req.timeout_secs.map(|v| v as i32).unwrap_or(cur_timeout);

    let new_config = if req.source.is_some() || req.sink.is_some() {
        let mut config: serde_json::Value = cur_config.unwrap_or(serde_json::json!({}));
        if let Some(ref source) = req.source {
            config["source"] = serde_json::to_value(source).map_err(|e| {
                AppError::Internal(anyhow::anyhow!("Failed to serialize source: {}", e))
            })?;
        }
        if let Some(ref sink) = req.sink {
            config["sink"] = serde_json::to_value(sink).map_err(|e| {
                AppError::Internal(anyhow::anyhow!("Failed to serialize sink: {}", e))
            })?;
        }
        Some(config)
    } else {
        cur_config
    };

    sqlx::query(
        "UPDATE warehouse_udfs \
         SET schedule = $3, fuel_limit = $4, timeout_secs = $5, job_config = $6, updated_at = NOW() \
         WHERE project_id = $1 AND name = $2 AND execution_mode = 'job'",
    )
    .bind(project_id)
    .bind(&job_name)
    .bind(&new_schedule)
    .bind(new_fuel)
    .bind(new_timeout)
    .bind(&new_config)
    .execute(state.db.as_ref())
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to update job: {}", e)))?;

    if let Some(existing) = registry.get(project_id, &job_name) {
        let updated = std::sync::Arc::new(crate::warehouse::udf::registry::CompiledUdf {
            id: existing.id,
            name: existing.name.clone(),
            module: existing.module.clone(),
            manifest: existing.manifest.clone(),
            execution_mode: existing.execution_mode,
            source_hash: existing.source_hash.clone(),
            fuel_limit: new_fuel as u64,
            timeout_secs: new_timeout as u32,
            schedule: new_schedule.clone(),
            job_config: new_config.clone(),
        });
        registry.reload(project_id, &job_name, updated);
    }

    Ok(Json(serde_json::json!({"updated": true})))
}

async fn trigger_job_run(
    State(state): State<Arc<PondState>>,
    Path((project_id, job_name)): Path<(Uuid, String)>,
) -> Result<Json<serde_json::Value>> {
    let event_store = state
        .event_store
        .as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Event system not initialized")))?;

    let event_id = event_store
        .emit(
            project_id,
            crate::warehouse::pipeline::events::EventType::Manual,
            "api",
            serde_json::json!({ "udf_name": job_name }),
        )
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to emit manual event: {}", e)))?;

    Ok(Json(serde_json::json!({
        "event_id": event_id,
        "status": "pending",
    })))
}

async fn list_job_runs(
    State(state): State<Arc<PondState>>,
    Path((project_id, _job_name)): Path<(Uuid, String)>,
) -> Result<Json<serde_json::Value>> {
    let event_store = state
        .event_store
        .as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Event system not initialized")))?;

    let events = event_store
        .list_events(project_id, 50)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to list events: {}", e)))?;

    Ok(Json(serde_json::json!({ "runs": events })))
}

async fn get_pipelines(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>> {
    let mut nodes = Vec::<serde_json::Value>::new();
    let mut edges = Vec::<serde_json::Value>::new();
    let mut has_sync_source = false;

    // Fetch sources
    let source_rows = sqlx::query(
        r#"SELECT s.id, s.name, s.source_type, s.enabled, s.sync_interval, s.last_sync_at,
                  COALESCE(s.tier, 'cold') as tier,
                  j.id as job_id
           FROM warehouse_sources s
           LEFT JOIN LATERAL (
              SELECT id FROM warehouse_jobs
              WHERE source_id = s.id AND status IN ('pending', 'running')
              LIMIT 1
           ) j ON true
           WHERE s.project_id = $1
           ORDER BY s.name"#,
    )
    .bind(project_id)
    .fetch_all(&*state.db)
    .await?;

    let mut source_names = std::collections::HashSet::new();
    let mut sink_names = std::collections::HashSet::new();

    for row in &source_rows {
        let name: String = row.get("name");
        let source_type: String = row.get("source_type");
        let enabled: bool = row.get("enabled");
        let tier: String = row.get("tier");
        let sync_interval: Option<String> = row.get("sync_interval");
        let last_sync_at: Option<chrono::DateTime<chrono::Utc>> = row.get("last_sync_at");
        let job_id: Option<Uuid> = row.get("job_id");

        source_names.insert(name.clone());

        let status = if !enabled {
            "disabled"
        } else if job_id.is_some() {
            "syncing"
        } else {
            "active"
        };

        nodes.push(serde_json::json!({
            "id": format!("source:{}", name),
            "type": "source",
            "label": name,
            "source_type": source_type,
            "status": status,
            "tier": tier,
            "last_sync_at": last_sync_at,
        }));

        let sync_label = if !enabled {
            "sync (disabled)".to_string()
        } else {
            sync_interval
                .as_deref()
                .map(|si| format!("sync ({})", si))
                .unwrap_or_else(|| "sync".to_string())
        };

        edges.push(serde_json::json!({
            "source": format!("source:{}", name),
            "target": "warehouse",
            "label": sync_label,
        }));
        has_sync_source = true;
    }

    if has_sync_source {
        nodes.push(serde_json::json!({
            "id": "warehouse",
            "type": "warehouse",
            "label": "Warehouse",
        }));
    }

    // Fetch job-mode UDFs with their job_config
    let udf_rows = sqlx::query(
        r#"SELECT name, execution_mode, schedule, job_config
           FROM warehouse_udfs
           WHERE project_id = $1 AND execution_mode = 'job' AND job_config IS NOT NULL
           ORDER BY name"#,
    )
    .bind(project_id)
    .fetch_all(&*state.db)
    .await?;

    for row in &udf_rows {
        let name: String = row.get("name");
        let schedule: Option<String> = row.get("schedule");
        let job_config: serde_json::Value = row.get("job_config");

        let source_name = job_config
            .get("source")
            .and_then(|s| s.get("name"))
            .and_then(|n| n.as_str())
            .unwrap_or("unknown");
        let sink_name = job_config
            .get("sink")
            .and_then(|s| s.get("name"))
            .and_then(|n| n.as_str())
            .unwrap_or("unknown");

        nodes.push(serde_json::json!({
            "id": format!("udf:{}", name),
            "type": "udf",
            "label": name,
            "schedule": schedule,
        }));

        // Create source node for the job if it doesn't already exist as a sync source
        let job_source_id = format!("source:{}", source_name);
        if !source_names.contains(source_name) {
            source_names.insert(source_name.to_string());
            nodes.push(serde_json::json!({
                "id": job_source_id,
                "type": "source",
                "label": source_name,
                "status": "external",
            }));
        }

        let sink_id = format!("sink:{}", sink_name);
        if !sink_names.contains(sink_name) {
            sink_names.insert(sink_name.to_string());
            nodes.push(serde_json::json!({
                "id": sink_id,
                "type": "sink",
                "label": sink_name,
            }));
        }

        edges.push(serde_json::json!({
            "source": job_source_id,
            "target": format!("udf:{}", name),
            "label": "read",
        }));
        edges.push(serde_json::json!({
            "source": format!("udf:{}", name),
            "target": sink_id,
            "label": "write",
        }));
    }

    Ok(Json(serde_json::json!({
        "nodes": nodes,
        "edges": edges,
    })))
}

// ===== Pipeline (DAG) Endpoints =====

async fn list_pipelines(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>> {
    let store = state
        .pipeline_store
        .as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Pipeline system not initialized")))?;

    let pipelines = store
        .list(project_id)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to list pipelines: {}", e)))?;

    Ok(Json(serde_json::json!({ "pipelines": pipelines })))
}

async fn create_pipeline(
    State(state): State<Arc<PondState>>,
    Path(project_id): Path<Uuid>,
    Json(payload): Json<crate::warehouse::pipeline::types::PipelineGraphPayload>,
) -> Result<Json<serde_json::Value>> {
    let store = state
        .pipeline_store
        .as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Pipeline system not initialized")))?;

    validate_pipeline_payload(&payload)?;

    let temp_nodes: Vec<crate::warehouse::pipeline::types::PipelineNode> = payload
        .nodes
        .iter()
        .map(|n| crate::warehouse::pipeline::types::PipelineNode {
            id: n.id,
            pipeline_id: Uuid::nil(),
            node_type: n.node_type,
            label: n.label.clone(),
            config: n.config.clone(),
            position_x: n.position_x,
            position_y: n.position_y,
        })
        .collect();
    let temp_edges: Vec<crate::warehouse::pipeline::types::PipelineEdge> = payload
        .edges
        .iter()
        .map(|e| crate::warehouse::pipeline::types::PipelineEdge {
            id: Uuid::nil(),
            pipeline_id: Uuid::nil(),
            from_node_id: e.from_node_id,
            to_node_id: e.to_node_id,
        })
        .collect();

    crate::warehouse::pipeline::dag::topological_sort(&temp_nodes, &temp_edges)
        .map_err(|e| AppError::Validation(format!("Invalid pipeline graph: {}", e)))?;

    let pipeline_id = store
        .create(project_id, &payload)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to create pipeline: {}", e)))?;

    Ok(Json(serde_json::json!({ "id": pipeline_id })))
}

async fn get_pipeline(
    State(state): State<Arc<PondState>>,
    Path((project_id, pipeline_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<serde_json::Value>> {
    let store = state
        .pipeline_store
        .as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Pipeline system not initialized")))?;

    let pipeline = store
        .load(project_id, pipeline_id)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to load pipeline: {}", e)))?
        .ok_or_else(|| AppError::NotFound("Pipeline not found".to_string()))?;

    Ok(Json(serde_json::to_value(&pipeline).unwrap_or_default()))
}

async fn update_pipeline(
    State(state): State<Arc<PondState>>,
    Path((project_id, pipeline_id)): Path<(Uuid, Uuid)>,
    Json(payload): Json<crate::warehouse::pipeline::types::PipelineGraphPayload>,
) -> Result<Json<serde_json::Value>> {
    let store = state
        .pipeline_store
        .as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Pipeline system not initialized")))?;

    validate_pipeline_payload(&payload)?;

    let temp_nodes: Vec<crate::warehouse::pipeline::types::PipelineNode> = payload
        .nodes
        .iter()
        .map(|n| crate::warehouse::pipeline::types::PipelineNode {
            id: n.id,
            pipeline_id,
            node_type: n.node_type,
            label: n.label.clone(),
            config: n.config.clone(),
            position_x: n.position_x,
            position_y: n.position_y,
        })
        .collect();
    let temp_edges: Vec<crate::warehouse::pipeline::types::PipelineEdge> = payload
        .edges
        .iter()
        .map(|e| crate::warehouse::pipeline::types::PipelineEdge {
            id: Uuid::nil(),
            pipeline_id,
            from_node_id: e.from_node_id,
            to_node_id: e.to_node_id,
        })
        .collect();

    crate::warehouse::pipeline::dag::topological_sort(&temp_nodes, &temp_edges)
        .map_err(|e| AppError::Validation(format!("Invalid pipeline graph: {}", e)))?;

    store
        .update(project_id, pipeline_id, &payload)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to update pipeline: {}", e)))?;

    Ok(Json(serde_json::json!({ "ok": true })))
}

async fn delete_pipeline(
    State(state): State<Arc<PondState>>,
    Path((project_id, pipeline_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<serde_json::Value>> {
    let store = state
        .pipeline_store
        .as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Pipeline system not initialized")))?;

    let deleted = store
        .delete(project_id, pipeline_id)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to delete pipeline: {}", e)))?;

    if !deleted {
        return Err(AppError::NotFound("Pipeline not found".to_string()));
    }

    Ok(Json(serde_json::json!({ "ok": true })))
}

async fn trigger_pipeline_run(
    State(state): State<Arc<PondState>>,
    Path((project_id, pipeline_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<serde_json::Value>> {
    let event_store = state
        .event_store
        .as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Event system not initialized")))?;

    let event_id = event_store
        .emit(
            project_id,
            crate::warehouse::pipeline::events::EventType::Manual,
            "api",
            serde_json::json!({ "pipeline_id": pipeline_id }),
        )
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to emit manual event: {}", e)))?;

    Ok(Json(serde_json::json!({ "event_id": event_id, "status": "pending" })))
}

async fn list_pipeline_runs(
    State(state): State<Arc<PondState>>,
    Path((project_id, pipeline_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<serde_json::Value>> {
    let store = state
        .pipeline_store
        .as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Pipeline system not initialized")))?;

    let runs = store
        .get_runs(project_id, pipeline_id)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to get pipeline runs: {}", e)))?;

    Ok(Json(serde_json::json!({ "runs": runs })))
}

#[derive(Debug, serde::Deserialize)]
struct CreateSubscriptionRequest {
    event_type: String,
    #[serde(default)]
    event_filter: serde_json::Value,
}

async fn create_pipeline_subscription(
    State(state): State<Arc<PondState>>,
    Path((_project_id, pipeline_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateSubscriptionRequest>,
) -> Result<Json<serde_json::Value>> {
    let event_store = state
        .event_store
        .as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Event system not initialized")))?;

    if req.event_type.is_empty() {
        return Err(AppError::Validation("event_type must not be empty".to_string()));
    }

    let filter = if req.event_filter.is_null() {
        serde_json::json!({})
    } else {
        req.event_filter
    };

    let sub_id = event_store
        .create_subscription(pipeline_id, &req.event_type, filter)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to create subscription: {}", e)))?;

    Ok(Json(serde_json::json!({ "id": sub_id })))
}

async fn list_pipeline_subscriptions(
    State(state): State<Arc<PondState>>,
    Path((_project_id, pipeline_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<serde_json::Value>> {
    let event_store = state
        .event_store
        .as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Event system not initialized")))?;

    let subs = event_store
        .list_subscriptions(pipeline_id)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to list subscriptions: {}", e)))?;

    Ok(Json(serde_json::json!({ "subscriptions": subs })))
}

async fn delete_pipeline_subscription(
    State(state): State<Arc<PondState>>,
    Path((_project_id, subscription_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<serde_json::Value>> {
    let event_store = state
        .event_store
        .as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Event system not initialized")))?;

    let deleted = event_store
        .delete_subscription(subscription_id)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to delete subscription: {}", e)))?;

    if !deleted {
        return Err(AppError::NotFound("Subscription not found".to_string()));
    }

    Ok(Json(serde_json::json!({ "ok": true })))
}

fn validate_pipeline_payload(
    payload: &crate::warehouse::pipeline::types::PipelineGraphPayload,
) -> Result<()> {
    if payload.name.is_empty() {
        return Err(AppError::Validation("name must not be empty".to_string()));
    }
    if payload.name.len() > 128 {
        return Err(AppError::Validation(
            "name must be 128 characters or less".to_string(),
        ));
    }
    if payload.nodes.is_empty() {
        return Err(AppError::Validation(
            "pipeline must have at least one node".to_string(),
        ));
    }
    if payload.nodes.len() > 100 {
        return Err(AppError::Validation(
            "pipeline must have 100 or fewer nodes".to_string(),
        ));
    }
    if let Some(ref schedule) = payload.schedule {
        validate_cron_expression(schedule)?;
    }
    Ok(())
}
