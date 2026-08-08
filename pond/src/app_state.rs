use std::sync::Arc;

use ahash::AHashMap;
use std::time::Instant;
use dashmap::DashSet;
use tokio::sync::RwLock;
use uuid::Uuid;

pub use reiver_core::app_state::RedisPool;
use reiver_core::config::Config;
use reiver_core::crypto::RotatingSecretEncryptor;
use reiver_core::db::DbPool;

use reiver_core::events::EventPublisher;

use crate::kafka::KafkaProducer;
use crate::warehouse::catalog::CatalogService;
use crate::warehouse::derived::DerivedTableManager;
use crate::warehouse::indexes::disk_cache::DiskIndexCache;
use crate::warehouse::indexes::skip_index::HierarchicalSkipIndex;
use crate::warehouse::query::cost_estimator::QueryCostEstimator;
use crate::warehouse::query::executor::QueryExecutor;
use crate::warehouse::query::limiter::QueryLimiter;
use crate::warehouse::metrics::WarehouseMetrics;
use crate::warehouse::sources::ConnectorRegistryService;
use crate::warehouse::nl_query::cache::NlQueryCache;
use crate::warehouse::query::rewriter::TableRewriter;
use crate::warehouse::storage::r2::R2Storage;
use crate::warehouse::types::R2TablePath;
use crate::warehouse::udf::registry::UdfRegistry;
use crate::warehouse::udf::worker_pool::UdfWorkerPool;
use crate::warehouse::pipeline::cron_emitter::CronEmitter;
use crate::warehouse::pipeline::events::EventStore;
use crate::warehouse::pipeline::executor::PipelineExecutor;
use crate::warehouse::pipeline::store::PipelineStore;

/// Cache for warehouse skip indexes, keyed by project_id -> table_name -> skip_index
pub type SkipIndexCache = Arc<RwLock<AHashMap<Uuid, AHashMap<String, HierarchicalSkipIndex>>>>;

/// Shared cost estimator with table statistics.
/// Uses `parking_lot::RwLock` — the lock is never held across `.await`.
pub type SharedCostEstimator = Arc<parking_lot::RwLock<QueryCostEstimator>>;

/// TTL for the project table metadata cache.
const PROJECT_TABLE_CACHE_TTL_SECS: u64 = 60;

/// Cached metadata about a project's warehouse tables, combining warm tables,
/// hot tables, and cold source presence into a single lookup.
#[derive(Clone)]
pub struct ProjectTableInfo {
    pub warm_tables: AHashMap<String, R2TablePath>,
    pub hot_tables: AHashMap<String, String>,
    /// R2 paths for warm backing sources keyed by hot table name (for failover).
    pub hot_backing_tables: AHashMap<String, R2TablePath>,
    pub has_cold_sources: bool,
    cached_at: Instant,
}

impl ProjectTableInfo {
    pub fn new(
        warm_tables: AHashMap<String, R2TablePath>,
        hot_tables: AHashMap<String, String>,
        hot_backing_tables: AHashMap<String, R2TablePath>,
        has_cold_sources: bool,
    ) -> Self {
        Self {
            warm_tables,
            hot_tables,
            hot_backing_tables,
            has_cold_sources,
            cached_at: Instant::now(),
        }
    }

    pub fn is_expired(&self) -> bool {
        self.cached_at.elapsed().as_secs() >= PROJECT_TABLE_CACHE_TTL_SECS
    }
}

// =============================================================================
// PondState -- Pond (Data Warehouse) product state
// =============================================================================

pub struct PondState {
    pub db: Arc<DbPool>,
    pub redis: Arc<RedisPool>,
    pub config: Arc<Config>,
    pub encryptor: Arc<RotatingSecretEncryptor>,
    pub kafka: Arc<KafkaProducer>,
    /// Platform event publisher for the event subscription system
    pub event_publisher: Arc<EventPublisher>,
    /// Warehouse skip index cache for TB-scale query optimization
    pub warehouse_skip_indexes: SkipIndexCache,
    /// Warehouse cost estimator with pre-populated table statistics
    pub warehouse_cost_estimator: SharedCostEstimator,
    /// Query concurrency limiter for per-project and global limits
    pub warehouse_query_limiter: Arc<QueryLimiter>,
    /// Warehouse query executor with connection pooling
    pub warehouse_query_executor: Arc<QueryExecutor>,
    /// Unified catalog service for schema discovery, lineage, and relationships
    pub catalog_service: Option<Arc<CatalogService>>,
    /// Connector registry service for managing runtime cold source connectors
    pub connector_registry_service: Option<Arc<ConnectorRegistryService>>,
    /// HTTP client for outbound requests (e.g., calling Flow gateway)
    pub http_client: reqwest::Client,
    /// Warehouse metrics collector (exported via OTel when enabled)
    pub warehouse_metrics: Arc<WarehouseMetrics>,
    /// Projects whose skip index cache is stale after a sync or rebuild.
    /// Checked before query execution to trigger a cache reload.
    pub skip_index_dirty: Arc<DashSet<Uuid>>,
    /// R2 storage for downloading skip index blobs
    pub r2_storage: Option<Arc<R2Storage>>,
    /// Local disk cache for mmap-backed skip index blobs
    pub disk_index_cache: Option<Arc<DiskIndexCache>>,
    /// Pre-computed table rewriter for warm-tier queries.
    /// Created once at startup from R2 environment config, avoiding
    /// per-request env::var reads and String allocations.
    pub table_rewriter: Arc<TableRewriter>,
    /// Derived table manager for CTAS / materialized view operations.
    /// None when R2 or ClickHouse are not configured.
    pub derived_table_manager: Option<Arc<DerivedTableManager>>,
    /// In-memory cache for project table metadata (warm tables, hot tables,
    /// cold source presence). Avoids two Postgres queries per request.
    pub project_table_cache: Arc<quick_cache::sync::Cache<Uuid, ProjectTableInfo>>,
    /// Projects whose table metadata cache is stale after a sync.
    pub table_cache_dirty: Arc<DashSet<Uuid>>,
    /// Singleton query result cache (Redis-backed with in-memory generation cache).
    pub query_cache: Arc<crate::warehouse::query::cache::QueryCache>,
    /// Circuit breaker for ClickHouse availability. When a connection error
    /// is detected, a sentinel `Instant` is stored; queries skip the hot path
    /// for 60 seconds to avoid hammering a down server.
    pub ch_down_cache: Arc<quick_cache::sync::Cache<(), std::time::Instant>>,
    /// LRU cache for NL-to-SQL query results. Avoids redundant LLM calls for
    /// repeated or similarly-phrased questions within the same project.
    pub nl_query_cache: Arc<NlQueryCache>,
    /// UDF registry for compiled Go-to-Wasm user-defined functions.
    pub udf_registry: Option<Arc<UdfRegistry>>,
    /// UDF worker pool for CPU-bound Wasm execution on rayon threads.
    pub udf_worker_pool: Option<Arc<UdfWorkerPool>>,
    /// Pipeline store for CRUD operations on transformation DAGs.
    pub pipeline_store: Option<Arc<PipelineStore>>,
    /// Pipeline executor for running transformation DAGs.
    pub pipeline_executor: Option<Arc<PipelineExecutor>>,
    /// Event store for the pipeline event system.
    pub event_store: Option<Arc<EventStore>>,
    /// Cron emitter for scheduled pipeline/job execution via the event system.
    pub cron_emitter: Option<Arc<CronEmitter>>,
}

