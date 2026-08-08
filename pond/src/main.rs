//! Pond -- Reiver Data Warehouse binary
//!
//! Runs only warehouse API routes and warehouse workers.

#![recursion_limit = "256"]

// Use jemalloc as the global allocator for better multi-threaded performance.
// On Linux, heap profiling is enabled via malloc_conf (sampling every 512KiB).
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(target_os = "linux")]
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";

use reiver_pond::{
    api, app_state, clickhouse_db, config::Config, crypto, db, kafka, pgwire, telemetry,
    warehouse,
};

use axum::{
    extract::Extension,
    routing::get,
    Router,
};
use tower_http::trace::TraceLayer;
use clap::{Parser, ValueEnum};
use std::net::SocketAddr;
use std::sync::Arc;

/// Reiver Pond -- Data Warehouse
#[derive(Parser, Debug)]
#[command(name = "reiver-pond")]
#[command(about = "Reiver data warehouse API and workers")]
struct Cli {
    /// Which mode to run in
    #[arg(long, value_enum, default_value = "all")]
    mode: PondMode,
}

/// Available modes for the Pond binary
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum PondMode {
    /// Run API + all warehouse workers
    All,
    /// Run only the HTTP API server (no workers)
    Api,
    /// Run all warehouse workers (no API)
    Workers,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    // Use a temporary thread-local subscriber so that Config::from_env() log
    // calls (security warnings, Kafka config, etc.) are visible on stdout.
    // This does NOT set the global subscriber, so init_telemetry() can still
    // call .init() afterwards without conflict.
    let temp_subscriber = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
        )
        .finish();

    let config = {
        let _guard = tracing::subscriber::set_default(temp_subscriber);
        Config::from_env()?
    };

    // Initialize the real global subscriber with optional OpenTelemetry export
    // (dogfooding). If OTEL_EXPORTER_OTLP_ENDPOINT + OTEL_PROJECT_ID are set,
    // Pond exports traces, metrics, and logs to Watch alongside console logging.
    // This will fail-fast if OTel is configured but any exporter cannot be built.
    let telemetry_providers = telemetry::init_telemetry(&config)?;

    // Start continuous profiler (CPU + heap on Linux, opt-in).
    // Uses the SDK's unified ClientOptions -- only starts when REIVER_API_KEY
    // is set (same authentication path as any external customer).
    let profiler = if config.profiling_enabled {
        let sdk_options = reiver_sdk::ClientOptions {
            api_key: std::env::var("REIVER_API_KEY").ok(),
            api_url: std::env::var("REIVER_API_URL").ok(),
            service_name: Some("reiver-pond".to_string()),
            version: Some(env!("CARGO_PKG_VERSION").to_string()),
            profiling_enabled: true,
            profiling_frequency: config.profiling_frequency,
            profiling_cpu_interval_secs: config.profiling_cpu_interval_secs,
            profiling_heap_interval_secs: config.profiling_heap_interval_secs,
            ..Default::default()
        };
        reiver_sdk::profiling::start(&sdk_options)
    } else {
        None
    };

    let cli = Cli::parse();
    tracing::info!("Starting reiver-pond in {:?} mode", cli.mode);

    // Auth is handled by the website gateway — pond trusts X-User-Id headers.
    // No JWT validation needed here.

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let result = match cli.mode {
        PondMode::All => run_all(config, shutdown_tx, shutdown_rx).await,
        PondMode::Api => run_api_only(config).await,
        PondMode::Workers => run_workers_only(config, shutdown_tx, shutdown_rx).await,
    };

    // Flush all pending telemetry before exit
    if let Some(provider) = telemetry_providers.tracer {
        if let Err(e) = provider.shutdown() {
            eprintln!("Failed to shutdown OpenTelemetry tracer provider: {:?}", e);
        }
    }
    if let Some(provider) = telemetry_providers.meter {
        if let Err(e) = provider.shutdown() {
            eprintln!("Failed to shutdown OpenTelemetry meter provider: {:?}", e);
        }
    }
    if let Some(provider) = telemetry_providers.logger {
        if let Err(e) = provider.shutdown() {
            eprintln!("Failed to shutdown OpenTelemetry logger provider: {:?}", e);
        }
    }

    // Shutdown continuous profiler
    if let Some(p) = profiler {
        p.shutdown(std::time::Duration::from_secs(5)).await;
    }

    result
}

/// Initialize core database connections (PostgreSQL, ClickHouse, Redis)
async fn init_core_connections(config: &Config) -> anyhow::Result<(
    Arc<db::DbPool>,
    Arc<clickhouse_db::ClickHousePool>,
    Arc<app_state::RedisPool>,
)> {
    db::ensure_database_exists(&config.database_url).await?;
    let db_pool = db::create_pool(&config.database_url).await?;

    tracing::info!("Connecting to ClickHouse at {}", config.clickhouse_url);
    let clickhouse_pool = clickhouse_db::create_clickhouse_pool(&config.clickhouse_url)?;

    // Note: ClickHouse schema migrations are handled by the Website service

    tracing::info!("Connecting to Redis at {}", config.redis_url);
    let manager = bb8_redis::RedisConnectionManager::new(config.redis_url.clone())
        .map_err(|e| anyhow::anyhow!("Failed to create Redis connection manager: {}", e))?;

    let redis_pool = bb8::Pool::builder()
        .max_size(15)
        .build(manager)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create Redis connection pool: {}", e))?;

    {
        use bb8_redis::redis::cmd;
        let mut test_conn = redis_pool.get().await
            .map_err(|e| anyhow::anyhow!("Failed to get Redis connection from pool: {}", e))?;
        cmd("PING").query_async::<String>(&mut *test_conn).await
            .map_err(|e| anyhow::anyhow!("Redis PING failed: {}", e))?;
    }
    tracing::info!("Redis connection pool established successfully");

    Ok((Arc::new(db_pool), Arc::new(clickhouse_pool), Arc::new(redis_pool)))
}

/// Create secret encryptor with key rotation support.
fn create_encryptor(config: &Config) -> anyhow::Result<Arc<crypto::RotatingSecretEncryptor>> {
    let is_production = std::env::var("ENVIRONMENT")
        .map(|e| e.to_lowercase() == "production")
        .unwrap_or(false);

    if let Some(ref key) = config.encryption_key {
        let fallback_keys: Vec<String> = std::env::var("ENCRYPTION_KEY_OLD")
            .ok()
            .map(|s| {
                s.split(',')
                    .filter(|k| !k.trim().is_empty())
                    .map(|k| k.trim().to_string())
                    .collect()
            })
            .unwrap_or_default();
        let fallback_refs: Vec<&str> = fallback_keys.iter().map(|s| s.as_str()).collect();

        match crypto::RotatingSecretEncryptor::new(key, fallback_refs) {
            Ok(e) => {
                if e.fallback_key_count() > 0 {
                    tracing::info!(
                        fallback_keys = e.fallback_key_count(),
                        "Key rotation active — encrypting with new key, decrypting with new + old"
                    );
                }
                Ok(Arc::new(e))
            }
            Err(e) => {
                if is_production {
                    return Err(anyhow::anyhow!("Invalid ENCRYPTION_KEY in production: {}", e));
                }
                tracing::warn!("Invalid ENCRYPTION_KEY: {}. Generating temporary key.", e);
                let temp_key = crypto::SecretEncryptor::generate_key();
                Ok(Arc::new(crypto::RotatingSecretEncryptor::single_key(&temp_key).expect("Generated key should be valid")))
            }
        }
    } else {
        if is_production {
            return Err(anyhow::anyhow!("ENCRYPTION_KEY is required in production"));
        }
        tracing::warn!("ENCRYPTION_KEY not set. Generating temporary key.");
        let temp_key = crypto::SecretEncryptor::generate_key();
        Ok(Arc::new(crypto::RotatingSecretEncryptor::single_key(&temp_key).expect("Generated key should be valid")))
    }
}

/// Create PondState with warehouse-specific fields
async fn create_pond_state(
    config_arc: Arc<Config>,
    db_pool_arc: Arc<db::DbPool>,
    _clickhouse_pool_arc: Arc<clickhouse_db::ClickHousePool>,
    redis_pool_arc: Arc<app_state::RedisPool>,
    kafka_producer_arc: Arc<kafka::KafkaProducer>,
    encryptor: Arc<crypto::RotatingSecretEncryptor>,
) -> anyhow::Result<Arc<app_state::PondState>> {
    // Initialize ConnectorRegistryService for federated cold queries
    let connector_registry = Arc::new(warehouse::query::ConnectorRegistry::new());
    let data_source_registry = Arc::new(warehouse::sources::DataSourceRegistry::new(db_pool_arc.clone(), encryptor.clone()));
    let connector_registry_service = Arc::new(
        warehouse::sources::ConnectorRegistryService::new(
            connector_registry,
            data_source_registry,
        )
    );

    // Initialize all cold source connectors (background task)
    let init_service = connector_registry_service.clone();
    tokio::spawn(async move {
        match init_service.initialize().await {
            Ok(result) => tracing::info!(
                loaded = result.loaded,
                failed = result.failed,
                total = result.total,
                "Connector registry initialization completed"
            ),
            Err(e) => tracing::warn!(error = %e, "Failed to initialize connector registry"),
        }
    });

    let warehouse_metrics = Arc::new(warehouse::WarehouseMetrics::new());
    // Register OTel observable instruments (only active when meter provider is configured)
    warehouse_metrics.register_otel_metrics();

    // Initialize R2 storage for the skip index hybrid path (blob uploads/downloads).
    let r2_storage: Option<Arc<warehouse::storage::r2::R2Storage>> = {
        use warehouse::storage::r2::{R2Config, R2Storage};
        match R2Config::from_env() {
            Ok(r2_config) => match R2Storage::new(r2_config).await {
                Ok(s) => {
                    tracing::info!("R2 storage initialized for skip index hybrid path");
                    Some(Arc::new(s))
                }
                Err(e) => {
                    tracing::warn!("Failed to init R2 for skip indexes: {}. Blob uploads disabled.", e);
                    None
                }
            },
            Err(_) => None,
        }
    };

    // Initialize local disk cache for mmap-backed skip index blobs.
    let disk_index_cache: Option<Arc<warehouse::indexes::disk_cache::DiskIndexCache>> =
        match warehouse::indexes::disk_cache::DiskIndexCache::new(std::path::PathBuf::from("data/indexes")) {
            Ok(c) => Some(Arc::new(c)),
            Err(e) => {
                tracing::warn!(error = %e, "Failed to initialize disk index cache");
                None
            }
        };

    // Build the table rewriter once from environment config -- avoids
    // per-request env::var reads and String allocations.
    let table_rewriter = Arc::new(api::warehouse::build_table_rewriter_from_env());

    let query_executor = Arc::new(warehouse::query::executor::QueryExecutor::from_env().await?);

    // Ensure the ClickHouse named collection for R2 exists so s3() queries work.
    if let Some(pool) = query_executor.native_pool() {
        ensure_r2_named_collection(pool).await;
    }

    let derived_table_manager = r2_storage.as_ref().and_then(|r2| {
        query_executor.native_pool().map(|pool| {
            Arc::new(warehouse::derived::DerivedTableManager::new(
                (*db_pool_arc).clone(),
                Arc::clone(r2),
                pool.clone(),
                warehouse_metrics.clone(),
            ))
        })
    });

    let query_cache = Arc::new(
        warehouse::query::cache::QueryCache::with_defaults(redis_pool_arc.clone())
            .with_metrics(warehouse_metrics.clone()),
    );

    // Initialize UDF system
    let (udf_registry, udf_worker_pool, pipeline_store, pipeline_executor, event_store, cron_emitter) = match gno_rs::wasm::runtime::UdfRuntime::new() {
        Ok(udf_runtime) => {
            let runtime = Arc::new(udf_runtime);
            let registry = Arc::new(warehouse::udf::UdfRegistry::new(
                runtime.clone(),
                db_pool_arc.clone(),
            ));
            let pool = Arc::new(warehouse::udf::UdfWorkerPool::new(
                num_cpus::get(),
                num_cpus::get(),
                64 * 1024 * 1024,
                runtime,
            ));
            let p_store = Arc::new(warehouse::pipeline::PipelineStore::new(db_pool_arc.clone()));
            let event_pub_for_pipeline = Arc::new(reiver_core::events::EventPublisher::new(
                kafka_producer_arc.clone(),
                reiver_core::events::EventSource::Pond,
            ));
            let p_executor = Arc::new(warehouse::pipeline::PipelineExecutor::new(
                p_store.clone(),
                registry.clone(),
                pool.clone(),
                connector_registry_service.clone(),
            ).with_event_publisher(event_pub_for_pipeline));
            let evt_store = Arc::new(warehouse::pipeline::EventStore::new(db_pool_arc.clone(), kafka_producer_arc.clone()));
            let cron = Arc::new(warehouse::pipeline::CronEmitter::new(
                evt_store.clone(),
                p_store.clone(),
                Some(registry.clone()),
            ));
            (Some(registry), Some(pool), Some(p_store), Some(p_executor), Some(evt_store), Some(cron))
        }
        Err(e) => {
            tracing::warn!(error = %e, "Failed to initialize UDF runtime, UDF system disabled");
            (None, None, None, None, None, None)
        }
    };

    let pond_state = Arc::new(app_state::PondState {
        db: db_pool_arc,
        redis: redis_pool_arc,
        config: config_arc,
        encryptor,
        kafka: kafka_producer_arc.clone(),
        event_publisher: Arc::new(reiver_core::events::EventPublisher::new(
            kafka_producer_arc,
            reiver_core::events::EventSource::Pond,
        )),
        warehouse_skip_indexes: Arc::new(tokio::sync::RwLock::new(ahash::AHashMap::new())),
        warehouse_cost_estimator: Arc::new(parking_lot::RwLock::new(
            warehouse::query::cost_estimator::QueryCostEstimator::new(),
        )),
        warehouse_query_limiter: Arc::new(
            warehouse::query::limiter::QueryLimiter::with_defaults()
                .with_metrics(warehouse_metrics.clone())
        ),
        warehouse_query_executor: query_executor,
        catalog_service: None,
        connector_registry_service: Some(connector_registry_service),
        http_client: reqwest::Client::new(),
        warehouse_metrics,
        skip_index_dirty: Arc::new(dashmap::DashSet::new()),
        r2_storage: r2_storage.clone(),
        disk_index_cache: disk_index_cache.clone(),
        table_rewriter,
        derived_table_manager,
        project_table_cache: Arc::new(quick_cache::sync::Cache::new(256)),
        table_cache_dirty: Arc::new(dashmap::DashSet::new()),
        query_cache,
        ch_down_cache: Arc::new(quick_cache::sync::Cache::new(1)),
        nl_query_cache: Arc::new(warehouse::nl_query::cache::NlQueryCache::new()),
        udf_registry: udf_registry.clone(),
        udf_worker_pool,
        pipeline_store: pipeline_store.clone(),
        pipeline_executor: pipeline_executor.clone(),
        event_store: event_store.clone(),
        cron_emitter: cron_emitter.clone(),
    });

    // Eagerly preload skip indexes before accepting traffic.
    // Blocks startup so queries never hit a cold cache. A 60-second timeout
    // prevents a broken R2 from stalling startup indefinitely.
    if let (Some(r2), Some(dc)) = (&r2_storage, &disk_index_cache) {
        match tokio::time::timeout(
            std::time::Duration::from_secs(60),
            api::warehouse::preload_skip_indexes_at_startup(&pond_state, r2, dc),
        ).await {
            Ok(Ok(count)) => tracing::info!(preloaded_projects = count, "Skip index preload completed"),
            Ok(Err(e)) => tracing::warn!(error = %e, "Skip index preload failed, continuing without preloaded indexes"),
            Err(_) => tracing::warn!("Skip index preload timed out after 60s, continuing without preloaded indexes"),
        }
    }

    // Initialize cost estimator with table statistics from database
    let cost_state = pond_state.clone();
    tokio::spawn(async move {
        match api::warehouse::initialize_cost_estimator(
            &cost_state.db,
            &cost_state.warehouse_cost_estimator,
        ).await {
            Ok(()) => tracing::debug!("Warehouse cost estimator initialized"),
            Err(e) => tracing::warn!(error = %e, "Failed to initialize cost estimator"),
        }
    });

    // Initialize UDF registry, clean up stale runs, and start event-driven scheduling
    if let Some(ref registry) = udf_registry {
        let init_registry = registry.clone();
        let init_pipeline_store = pipeline_store.clone();
        let init_pipeline_executor = pipeline_executor.clone();
        let init_event_store = event_store.clone();
        let init_cron_emitter = cron_emitter.clone();
        let init_config = pond_state.config.clone();
        let server_start = chrono::Utc::now();
        tokio::spawn(async move {
            match init_registry.initialize().await {
                Ok(result) => {
                    tracing::info!(
                        loaded = result.loaded,
                        failed = result.failed,
                        total = result.total,
                        "UDF registry initialization completed"
                    );
                    if let Some(ref p_store) = init_pipeline_store {
                        if let Err(e) = p_store.cleanup_stale_runs(server_start).await {
                            tracing::warn!(error = %e, "Failed to clean up stale pipeline runs");
                        }
                    }
                    if let Some(ref cron) = init_cron_emitter {
                        if let Err(e) = cron.schedule_all().await {
                            tracing::warn!(error = %e, "Failed to start cron scheduler");
                        }
                    }
                    if let (Some(ref evt_store), Some(ref p_store), Some(ref p_executor)) =
                        (&init_event_store, &init_pipeline_store, &init_pipeline_executor)
                    {
                        let consumer_config = warehouse::pipeline::PipelineEventConsumerConfig {
                            kafka_hosts: init_config.kafka_hosts.clone(),
                            pipeline_events_topic: init_config.kafka_pipeline_events_topic.clone(),
                            client_id: init_config.kafka_client_id.clone(),
                        };
                        match warehouse::pipeline::EventDispatcher::new(
                            consumer_config,
                            evt_store.clone(),
                            p_store.clone(),
                            p_executor.clone(),
                        ) {
                            Ok((mut dispatcher, handle)) => {
                                tokio::spawn(async move {
                                    dispatcher.run().await;
                                });
                                std::mem::forget(handle);
                            }
                            Err(e) => {
                                tracing::error!(error = %e, "Failed to create pipeline event consumer");
                            }
                        }
                    }
                }
                Err(e) => tracing::warn!(error = %e, "Failed to initialize UDF registry"),
            }
        });
    }

    Ok(pond_state)
}

fn create_kafka_producer(config: &Config) -> anyhow::Result<Arc<kafka::KafkaProducer>> {
    let kafka_config = kafka::KafkaProducerConfig {
        hosts: config.kafka_hosts.clone(),
        exceptions_topic: config.kafka_exceptions_topic.clone(),
        spans_topic: config.kafka_spans_topic.clone(),
        logs_otlp_topic: config.kafka_logs_otlp_topic.clone(),
        logs_unstructured_topic: config.kafka_logs_unstructured_topic.clone(),
        llm_chunks_topic: config.kafka_llm_chunks_topic.clone(),
        metrics_topic: config.kafka_metrics_topic.clone(),
        sync_jobs_topic: config.kafka_sync_jobs_topic.clone(),
        pipeline_events_topic: config.kafka_pipeline_events_topic.clone(),
        platform_events_topic: config.kafka_platform_events_topic.clone(),
        session_eval_jobs_topic: config.kafka_session_eval_jobs_topic.clone(),
        client_id: config.kafka_client_id.clone(),
        linger_ms: config.kafka_producer_linger_ms,
        max_retries: config.kafka_producer_max_retries,
        message_timeout_ms: config.kafka_message_timeout_ms,
        socket_timeout_ms: config.kafka_socket_timeout_ms,
        compression_codec: config.kafka_compression_codec.clone(),
        acks: config.kafka_acks.clone(),
    };
    let producer = kafka::KafkaProducer::new(&kafka_config)?;
    Ok(Arc::new(producer))
}

/// Warehouse storage: R2 (object store) + ClickHouse (columnar) + partition manager.
///
/// Returns `None` if R2 is not configured (e.g. in development without S3).
async fn init_warehouse_storage(
    _config: &Config,
    db_pool: Arc<db::DbPool>,
    existing_r2: Option<Arc<warehouse::storage::r2::R2Storage>>,
) -> Option<(
    Arc<warehouse::storage::r2::R2Storage>,
    Arc<warehouse::storage::clickhouse::ClickHouseStorage>,
    Arc<warehouse::indexes::PartitionManager>,
)> {
    use warehouse::storage::clickhouse::{ClickHouseStorage, ClickHouseStorageConfig};
    use warehouse::storage::r2::{R2Config, R2Storage};

    let r2_storage = if let Some(r2) = existing_r2 {
        r2
    } else {
        let r2_config = match R2Config::from_env() {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("R2 not configured: {}. Upgrade jobs will fail.", e);
                return None;
            }
        };

        match R2Storage::new(r2_config).await {
            Ok(s) => {
                tracing::info!("R2 storage initialized");
                Arc::new(s)
            }
            Err(e) => {
                tracing::warn!("Failed to initialize R2 storage: {}. Sync jobs will fail.", e);
                return None;
            }
        }
    };

    let ch_config = ClickHouseStorageConfig {
        host: std::env::var("CLICKHOUSE_HOST").unwrap_or_else(|_| "localhost".to_string()),
        native_port: std::env::var("CLICKHOUSE_NATIVE_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(9000),
        database: "default".to_string(),
        username: Some("default".to_string()),
        password: None,
        table_settings: Default::default(),
    };
    let ch_storage = Arc::new(ClickHouseStorage::try_new(ch_config).await
        .expect("Failed to connect to ClickHouse for storage"));
    let partition_manager = Arc::new(warehouse::indexes::PartitionManager::new(db_pool));

    // Ensure the mutation stats table exists (SummingMergeTree for aggregation)
    if let Err(e) = ch_storage.create_mutation_stats_table().await {
        tracing::warn!("Failed to create warehouse_mutation_stats table: {}. Mutation tracking will be unavailable.", e);
    }

    Some((r2_storage, ch_storage, partition_manager))
}

/// Ensure the ClickHouse named collection for R2/S3 access exists.
///
/// Reads R2 credentials from environment variables and creates the collection
/// via `CREATE NAMED COLLECTION IF NOT EXISTS`. This is idempotent and runs
/// on every startup so that fresh ClickHouse deployments work automatically.
async fn ensure_r2_named_collection(pool: &warehouse::ch_client::NativePool) {
    let (Ok(access_key), Ok(secret_key), Ok(account_id), Ok(bucket)) = (
        std::env::var("R2_ACCESS_KEY_ID"),
        std::env::var("R2_SECRET_ACCESS_KEY"),
        std::env::var("R2_ACCOUNT_ID"),
        std::env::var("R2_BUCKET"),
    ) else {
        tracing::warn!("R2 credentials not fully configured; skipping named collection setup");
        return;
    };

    let conn = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "Failed to checkout connection for named collection setup");
            return;
        }
    };

    let collection_name = format!("r2_{}", bucket.replace('-', "_"));
    let url = format!("https://{}.r2.cloudflarestorage.com/{}/", account_id, bucket);

    let sql = format!(
        "CREATE NAMED COLLECTION IF NOT EXISTS {} AS \
         access_key_id = '{}', \
         secret_access_key = '{}', \
         url = '{}'",
        collection_name, access_key, secret_key, url,
    );

    match conn.execute(&sql).await {
        Ok(()) => tracing::info!(collection = %collection_name, "ClickHouse named collection ensured"),
        Err(e) => tracing::error!(error = %e, collection = %collection_name, "Failed to create ClickHouse named collection"),
    }
}

fn create_pond_router(pond_state: Arc<app_state::PondState>) -> Router {
    Router::new()
        .route("/health", get(health_check))
        .nest("/api", api::create_pond_api_router().with_state(pond_state.clone()))
        .nest("/api/v1", api::create_pond_api_router().with_state(pond_state.clone()))
        .layer(Extension(pond_state.db.clone()))
        .layer(Extension(pond_state.config.clone()))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &axum::http::Request<_>| {
                    let method = request.method().as_str();
                    let path = request.uri().path();
                    let span_name = format!("{method} {path}");
                    tracing::info_span!(
                        "http.request",
                        otel.name = %span_name,
                        otel.kind = "server",
                        otel.status_code = tracing::field::Empty,
                        http.method = %request.method(),
                        http.route = %request.uri().path(),
                        http.target = %request.uri(),
                        http.status_code = tracing::field::Empty,
                    )
                })
                .on_response(|response: &axum::http::Response<_>, latency: std::time::Duration, span: &tracing::Span| {
                    let status = response.status().as_u16();
                    span.record("http.status_code", status);
                    // Set OTel status code so error rate dashboards work correctly
                    if status >= 400 {
                        span.record("otel.status_code", "ERROR");
                    } else {
                        span.record("otel.status_code", "OK");
                    }
                    tracing::info!(
                        latency_ms = latency.as_millis(),
                        status = status,
                        "HTTP response"
                    );
                })
        )
}

/// Run Pond API + warehouse workers
async fn run_all(
    config: Config,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (db_pool_arc, clickhouse_pool_arc, redis_pool_arc) = init_core_connections(&config).await?;
    let config_arc = Arc::new(config.clone());
    let kafka_producer_arc = create_kafka_producer(&config)?;
    let encryptor = create_encryptor(&config)?;

    let pond_state = create_pond_state(
        config_arc.clone(), db_pool_arc.clone(),
        clickhouse_pool_arc.clone(), redis_pool_arc.clone(),
        kafka_producer_arc.clone(), encryptor.clone(),
    ).await?;

    // Start cost estimator refresh worker
    let cost_worker_state = pond_state.clone();
    let cost_shutdown_rx = shutdown_rx.clone();
    let _cost_handle = tokio::spawn(async move {
        api::warehouse::cost_estimator_refresh_worker(cost_worker_state, cost_shutdown_rx).await;
    });

    // Start warehouse sync job consumer, blockchain sync daemon, and flush worker
    let (sync_job_consumer_handle, blockchain_handle) = {
        if let Some((r2_storage, ch_storage, partition_manager)) =
            init_warehouse_storage(&config, db_pool_arc.clone(), pond_state.r2_storage.clone()).await
        {
            // Blockchain sync daemon
            let blockchain_daemon = warehouse::sync::blockchain_sync::BlockchainSyncDaemon::new(
                (*db_pool_arc).clone(),
                Arc::clone(&r2_storage),
                Arc::clone(&ch_storage),
                Arc::clone(&partition_manager),
            );
            let blockchain_shutdown_rx = shutdown_rx.clone();
            let bc_handle = tokio::spawn(async move {
                if let Err(e) = blockchain_daemon.run(blockchain_shutdown_rx).await {
                    tracing::error!("Blockchain sync daemon error: {}", e);
                }
            });

            // Blockchain buffer flush worker
            {
                let flush_db = (*db_pool_arc).clone();
                let flush_ch = Arc::clone(&ch_storage);
                let flush_r2 = Arc::clone(&r2_storage);
                let flush_shutdown = shutdown_rx.clone();
                tokio::spawn(async move {
                    if let Err(e) = warehouse::sync::blockchain_sync::blockchain_buffer_flush_worker(
                        flush_db, flush_ch, flush_r2, flush_shutdown,
                    ).await {
                        tracing::error!("Blockchain buffer flush worker error: {}", e);
                    }
                });
            }

            // Sync job consumer
            let kafka_cfg = warehouse::sync::SyncJobConsumerConfig {
                kafka_hosts: config.kafka_hosts.clone(),
                sync_jobs_topic: config.kafka_sync_jobs_topic.clone(),
                client_id: config.kafka_client_id.clone(),
            };
            let consumer_handle = match warehouse::sync::SyncJobConsumer::new(
                kafka_cfg,
                (*db_pool_arc).clone(),
                encryptor.clone(),
                r2_storage,
                ch_storage,
                partition_manager,
                pond_state.derived_table_manager.clone(),
            ) {
                Ok((consumer, _handle)) => {
                    let mut consumer = consumer
                        .with_event_publisher(pond_state.event_publisher.clone());
                    tracing::info!("Starting warehouse sync job consumer");
                    tokio::spawn(async move {
                        if let Err(e) = consumer.run().await {
                            tracing::error!("Sync job consumer error: {}", e);
                        }
                    })
                }
                Err(e) => {
                    tracing::warn!("Failed to start sync job consumer: {}", e);
                    tokio::spawn(async {})
                }
            };

            (consumer_handle, Some(bc_handle))
        } else {
            tracing::info!("Skipping sync job consumer and blockchain daemon (R2 not configured)");
            (tokio::spawn(async {}), None)
        }
    };

    // Start interval-based sync scheduler
    let interval_sync_handle = {
        use warehouse::sync::IntervalSyncScheduler;
        let mut scheduler = IntervalSyncScheduler::new(
            (*db_pool_arc).clone(),
            kafka_producer_arc.clone(),
        );
        let handle = scheduler.start(Some(std::time::Duration::from_secs(30)));

        let shutdown_rx_clone = shutdown_rx.clone();
        tokio::spawn(async move {
            let mut rx = shutdown_rx_clone;
            while rx.changed().await.is_ok() {
                if *rx.borrow() {
                    scheduler.shutdown();
                    break;
                }
            }
        });
        handle
    };

    // Start storage tier lifecycle worker
    let lifecycle_worker_handle = {
        use warehouse::sync::LifecycleWorker;
        let mut worker = LifecycleWorker::with_defaults(
            (*db_pool_arc).clone(),
            kafka_producer_arc.clone(),
        );
        let handle = worker.start();

        let shutdown_rx_clone = shutdown_rx.clone();
        tokio::spawn(async move {
            let mut rx = shutdown_rx_clone;
            while rx.changed().await.is_ok() {
                if *rx.borrow() {
                    worker.shutdown();
                    break;
                }
            }
        });
        handle
    };

    let app = create_pond_router(pond_state.clone());
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3002").await?;
    tracing::info!("Pond server listening on http://0.0.0.0:3002");

    let mut http_handle = tokio::spawn(async move {
        axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await
    });

    // Start the PgWire server (Postgres wire protocol adapter)
    let pgwire_state = pond_state.clone();
    let mut pgwire_handle = tokio::spawn(async move {
        if let Err(e) = pgwire::start_pgwire_server(pgwire_state).await {
            tracing::error!(error = %e, "PgWire server failed");
        }
    });

    let shutdown_signal = tokio::signal::ctrl_c();
    tokio::pin!(shutdown_signal);

    let shutdown_received = tokio::select! {
        result = &mut http_handle => {
            match result {
                Ok(Ok(())) => tracing::info!("HTTP server stopped normally"),
                Ok(Err(e)) => return Err(anyhow::anyhow!("HTTP server error: {}", e)),
                Err(e) => return Err(anyhow::anyhow!("HTTP server task panicked: {}", e)),
            }
            false
        }
        result = &mut pgwire_handle => {
            match result {
                Ok(()) => tracing::info!("PgWire server stopped normally"),
                Err(e) => tracing::warn!("PgWire server task panicked: {}", e),
            }
            false
        }
        _ = shutdown_signal => {
            tracing::info!("Received shutdown signal");
            true
        }
    };

    if shutdown_received {
        http_handle.abort();
        pgwire_handle.abort();
        if let Err(e) = shutdown_tx.send(true) {
            tracing::warn!("Failed to send shutdown signal: {}", e);
        }

        tracing::info!("Waiting for workers to finish (timeout: 30s)...");
        let _ = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let _ = tokio::join!(sync_job_consumer_handle, interval_sync_handle, lifecycle_worker_handle);
            if let Some(h) = blockchain_handle {
                let _ = h.await;
            }
        }).await;
        tracing::info!("All Pond workers stopped");
    }

    let _ = http_handle.await;
    Ok(())
}

/// Run only the Pond API (no workers)
async fn run_api_only(config: Config) -> anyhow::Result<()> {
    let (db_pool_arc, clickhouse_pool_arc, redis_pool_arc) = init_core_connections(&config).await?;
    let config_arc = Arc::new(config.clone());
    let kafka_producer_arc = create_kafka_producer(&config)?;
    let encryptor = create_encryptor(&config)?;

    let pond_state = create_pond_state(
        config_arc, db_pool_arc, clickhouse_pool_arc,
        redis_pool_arc, kafka_producer_arc, encryptor,
    ).await?;

    let app = create_pond_router(pond_state.clone());
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3002").await?;
    tracing::info!("Pond API server listening on http://0.0.0.0:3002 (workers disabled)");

    // Start the PgWire server alongside the HTTP server
    let pgwire_state = pond_state.clone();
    tokio::spawn(async move {
        if let Err(e) = pgwire::start_pgwire_server(pgwire_state).await {
            tracing::error!(error = %e, "PgWire server failed");
        }
    });

    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await?;
    Ok(())
}

/// Run only Pond workers (no API)
async fn run_workers_only(
    config: Config,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (db_pool_arc, _clickhouse_pool_arc, _redis_pool_arc) = init_core_connections(&config).await?;
    let kafka_producer_arc = create_kafka_producer(&config)?;
    let encryptor = create_encryptor(&config)?;

    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    // Start sync job consumer and blockchain sync daemon
    if let Some((r2_storage, ch_storage, partition_manager)) =
        init_warehouse_storage(&config, db_pool_arc.clone(), None).await
    {
        // Blockchain sync daemon
        let blockchain_daemon = warehouse::sync::blockchain_sync::BlockchainSyncDaemon::new(
            (*db_pool_arc).clone(),
            Arc::clone(&r2_storage),
            Arc::clone(&ch_storage),
            Arc::clone(&partition_manager),
        );
        let blockchain_shutdown_rx = shutdown_rx.clone();
        handles.push(tokio::spawn(async move {
            if let Err(e) = blockchain_daemon.run(blockchain_shutdown_rx).await {
                tracing::error!("Blockchain sync daemon error: {}", e);
            }
        }));

        // Blockchain buffer flush worker
        {
            let flush_db = (*db_pool_arc).clone();
            let flush_ch = Arc::clone(&ch_storage);
            let flush_r2 = Arc::clone(&r2_storage);
            let flush_shutdown = shutdown_rx.clone();
            handles.push(tokio::spawn(async move {
                if let Err(e) = warehouse::sync::blockchain_sync::blockchain_buffer_flush_worker(
                    flush_db, flush_ch, flush_r2, flush_shutdown,
                ).await {
                    tracing::error!("Blockchain buffer flush worker error: {}", e);
                }
            }));
        }

        let query_executor = Arc::new(warehouse::query::executor::QueryExecutor::from_env().await?);
        if let Some(pool) = query_executor.native_pool() {
            ensure_r2_named_collection(pool).await;
        }
        let warehouse_metrics = Arc::new(warehouse::metrics::WarehouseMetrics::new());
        let derived_table_manager = query_executor.native_pool().map(|pool| {
            Arc::new(warehouse::derived::DerivedTableManager::new(
                (*db_pool_arc).clone(),
                Arc::clone(&r2_storage),
                pool.clone(),
                warehouse_metrics,
            ))
        });

        let kafka_cfg = warehouse::sync::SyncJobConsumerConfig {
            kafka_hosts: config.kafka_hosts.clone(),
            sync_jobs_topic: config.kafka_sync_jobs_topic.clone(),
            client_id: config.kafka_client_id.clone(),
        };
        if let Ok((consumer, _handle)) = warehouse::sync::SyncJobConsumer::new(
            kafka_cfg,
            (*db_pool_arc).clone(),
            encryptor.clone(),
            r2_storage,
            ch_storage,
            partition_manager,
            derived_table_manager,
        ) {
            let event_publisher = Arc::new(reiver_core::events::EventPublisher::new(
                kafka_producer_arc.clone(),
                reiver_core::events::EventSource::Pond,
            ));
            let mut consumer = consumer.with_event_publisher(event_publisher);
            handles.push(tokio::spawn(async move {
                if let Err(e) = consumer.run().await {
                    tracing::error!("Sync job consumer error: {}", e);
                }
            }));
        }
    }

    // Start interval sync scheduler
    {
        use warehouse::sync::IntervalSyncScheduler;
        let mut scheduler = IntervalSyncScheduler::new(
            (*db_pool_arc).clone(),
            kafka_producer_arc.clone(),
        );
        let handle = scheduler.start(Some(std::time::Duration::from_secs(30)));
        handles.push(handle);

        let shutdown_rx_clone = shutdown_rx.clone();
        tokio::spawn(async move {
            let mut rx = shutdown_rx_clone;
            while rx.changed().await.is_ok() {
                if *rx.borrow() {
                    scheduler.shutdown();
                    break;
                }
            }
        });
    }

    // Start storage tier lifecycle worker
    {
        use warehouse::sync::LifecycleWorker;
        let mut worker = LifecycleWorker::with_defaults(
            (*db_pool_arc).clone(),
            kafka_producer_arc.clone(),
        );
        let handle = worker.start();
        handles.push(handle);

        let shutdown_rx_clone = shutdown_rx.clone();
        tokio::spawn(async move {
            let mut rx = shutdown_rx_clone;
            while rx.changed().await.is_ok() {
                if *rx.borrow() {
                    worker.shutdown();
                    break;
                }
            }
        });
    }

    tracing::info!("Pond workers started (API disabled)");
    wait_for_shutdown(shutdown_tx, handles).await;
    Ok(())
}

/// Wait for shutdown signal and gracefully stop
async fn wait_for_shutdown(
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    handles: Vec<tokio::task::JoinHandle<()>>,
) {
    tokio::signal::ctrl_c().await.expect("Failed to listen for ctrl-c");
    tracing::info!("Received shutdown signal");

    if let Err(e) = shutdown_tx.send(true) {
        tracing::warn!("Failed to send shutdown signal: {}", e);
    }

    tracing::info!("Waiting for workers to finish (timeout: 30s)...");
    let _ = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        for handle in handles {
            let _ = handle.await;
        }
    }).await;

    tracing::info!("All workers stopped");
}

/// Health check endpoint
async fn health_check() -> &'static str {
    "OK"
}
