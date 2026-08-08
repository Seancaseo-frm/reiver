//! Flow -- Reiver LLM Gateway binary
//!
//! Runs only LLM Gateway routes, LLM observability routes, and LLM workers.

// Use mimalloc as the global allocator for better multi-threaded performance
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use reiver_flow::{
    api, app_state, clickhouse_db, crypto, db, gateway, kafka, llm, rollout_worker, telemetry,
    trusted_proxy,
};
use reiver_flow::gateway::prompt_store::PgPromptStore;

use axum::{
    extract::{Extension, State},
    http::StatusCode,
    middleware,
    routing::get,
    Router,
};
use clap::{Parser, ValueEnum};
use reiver_flow::config::Config;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tower_http::trace::TraceLayer;

const DEFAULT_LISTEN_ADDR: &str = "0.0.0.0:3001";
const WORKER_SHUTDOWN_TIMEOUT_SECS: u64 = 30;
const DEFAULT_REDIS_POOL_MAX_SIZE: u32 = 50;

/// Wraps an `axum::http::HeaderMap` so `opentelemetry::propagation::Extractor`
/// can pull W3C `traceparent` / `tracestate` from incoming HTTP requests.
struct HeaderExtractor<'a>(&'a axum::http::HeaderMap);

impl opentelemetry::propagation::Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

/// Reiver Flow -- LLM Gateway
#[derive(Parser, Debug)]
#[command(name = "reiver-flow")]
#[command(about = "Reiver LLM gateway API and workers")]
struct Cli {
    /// Which mode to run in
    #[arg(long, value_enum, default_value = "all")]
    mode: FlowMode,
}

/// Available modes for the Flow binary
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum FlowMode {
    /// Run API + all LLM workers
    All,
    /// Run only the HTTP API server (no workers)
    Api,
    /// Run all LLM workers (no API)
    Workers,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    // Temp subscriber so Config::from_env() log calls are visible on stdout
    // before the real (possibly OTel-enriched) subscriber is installed.
    let temp_subscriber = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .finish();

    let config = {
        let _guard = tracing::subscriber::set_default(temp_subscriber);
        Config::from_env()?
    };

    let telemetry_providers = telemetry::init_telemetry(&config)?;

    let cli = Cli::parse();
    tracing::info!("Starting reiver-flow in {:?} mode", cli.mode);

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let result = match cli.mode {
        FlowMode::All => run_all(config, shutdown_tx, shutdown_rx).await,
        FlowMode::Api => run_api_only(config).await,
        FlowMode::Workers => run_workers_only(config, shutdown_tx, shutdown_rx).await,
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

    result
}

/// Required PostgreSQL tables that `website` migrations must have created.
///
/// Flow does not run its own migrations — it relies on `website` to have run
/// first. If `flow` is deployed before `website` (or against a fresh DB), it
/// will fail at the first SQL query with a confusing "relation does not exist"
/// error. This check makes the dependency explicit and the failure immediate.
const REQUIRED_TABLES: &[&str] = &[
    "project_settings",
    "llm_sessions_metadata",
    "llm_prompt_configs",
    "llm_prompt_versions",
    "llm_evaluation_scores",
    "saved_sessions",
];

/// Verify that the tables written by `website` migrations exist.
/// Fails fast with a clear error if any table is missing.
async fn validate_required_tables(db: &db::DbPool) -> anyhow::Result<()> {
    for table in REQUIRED_TABLES {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = $1
            )",
        )
        .bind(table)
        .fetch_one(db)
        .await
        .map_err(|e| anyhow::anyhow!("DB error while checking table '{}': {}", table, e))?;

        if !exists {
            anyhow::bail!(
                "Required table '{}' does not exist. \
                 The `website` service must run its database migrations before `flow` starts. \
                 Run the website service first or apply migrations manually.",
                table
            );
        }
    }
    tracing::info!("All required database tables verified");
    Ok(())
}

/// Initialize core database connections (PostgreSQL, ClickHouse, Redis)
async fn init_core_connections(
    config: &Config,
) -> anyhow::Result<(
    Arc<db::DbPool>,
    Arc<clickhouse_db::ClickHousePool>,
    Arc<app_state::RedisPool>,
)> {
    db::ensure_database_exists(&config.database_url).await?;
    let db_pool = db::create_pool(&config.database_url).await?;

    // Fail fast if website migrations haven't been run yet
    validate_required_tables(&db_pool).await?;

    tracing::info!("Connecting to ClickHouse at {}", config.clickhouse_url);
    let clickhouse_pool = clickhouse_db::create_clickhouse_pool(&config.clickhouse_url)?;

    // Fail fast if ClickHouse tables from website migrations are missing
    clickhouse_db::validate_clickhouse_tables(
        &clickhouse_pool,
        clickhouse_db::REQUIRED_CLICKHOUSE_TABLES,
    )
    .await?;
    tracing::info!("Required ClickHouse tables verified");

    // Initialize Flow-specific ClickHouse Kafka Engine (LLM chunks)
    // Note: ClickHouse schema migrations are handled by the Website service
    tracing::info!(
        "Initializing Flow ClickHouse Kafka Engine with Kafka hosts: {}",
        config.clickhouse_kafka_hosts
    );
    clickhouse_db::init_flow_kafka_engine(
        &clickhouse_pool,
        &config.clickhouse_kafka_hosts,
        &config.kafka_llm_chunks_topic,
    )
    .await?;
    tracing::info!("Flow ClickHouse Kafka Engine initialized successfully");

    tracing::info!("Connecting to Redis at {}", config.redis_url);
    let manager = bb8_redis::RedisConnectionManager::new(config.redis_url.clone())
        .map_err(|e| anyhow::anyhow!("Failed to create Redis connection manager: {}", e))?;

    let redis_pool_max_size = match std::env::var("REDIS_POOL_MAX_SIZE") {
        Ok(v) => match v.parse::<u32>() {
            Ok(n) if n >= 1 && n <= 500 => n,
            Ok(n) => {
                tracing::warn!(
                    value = n,
                    "REDIS_POOL_MAX_SIZE out of range (1-500), using default {}",
                    DEFAULT_REDIS_POOL_MAX_SIZE
                );
                DEFAULT_REDIS_POOL_MAX_SIZE
            }
            Err(e) => {
                tracing::warn!(
                    raw = %v,
                    error = %e,
                    "Failed to parse REDIS_POOL_MAX_SIZE, using default {}",
                    DEFAULT_REDIS_POOL_MAX_SIZE
                );
                DEFAULT_REDIS_POOL_MAX_SIZE
            }
        },
        Err(_) => DEFAULT_REDIS_POOL_MAX_SIZE,
    };

    let redis_pool = bb8::Pool::builder()
        .max_size(redis_pool_max_size)
        .build(manager)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create Redis connection pool: {}", e))?;

    {
        use bb8_redis::redis::AsyncCommands;
        let mut test_conn = redis_pool
            .get()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Redis connection from pool: {}", e))?;
        test_conn
            .set::<&str, &str, ()>("test_key", "test_value")
            .await
            .map_err(|e| anyhow::anyhow!("Failed to set test key in Redis: {}", e))?;
        let _: String = test_conn
            .get::<&str, String>("test_key")
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get test key from Redis: {}", e))?;
    }
    tracing::info!("Redis connection pool established successfully");

    Ok((
        Arc::new(db_pool),
        Arc::new(clickhouse_pool),
        Arc::new(redis_pool),
    ))
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
                    return Err(anyhow::anyhow!(
                        "Invalid ENCRYPTION_KEY in production: {}",
                        e
                    ));
                }
                tracing::warn!("Invalid ENCRYPTION_KEY: {}. Generating temporary key.", e);
                let temp_key = crypto::SecretEncryptor::generate_key();
                Ok(Arc::new(
                    crypto::RotatingSecretEncryptor::single_key(&temp_key)
                        .expect("Generated key should be valid"),
                ))
            }
        }
    } else {
        if is_production {
            return Err(anyhow::anyhow!("ENCRYPTION_KEY is required in production"));
        }
        tracing::warn!("ENCRYPTION_KEY not set. Generating temporary key.");
        let temp_key = crypto::SecretEncryptor::generate_key();
        Ok(Arc::new(
            crypto::RotatingSecretEncryptor::single_key(&temp_key)
                .expect("Generated key should be valid"),
        ))
    }
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

/// Create FlowState with gateway/LLM-specific fields
async fn create_flow_state(
    config_arc: Arc<Config>,
    db_pool_arc: Arc<db::DbPool>,
    clickhouse_pool_arc: Arc<clickhouse_db::ClickHousePool>,
    redis_pool_arc: Arc<app_state::RedisPool>,
    kafka_producer_arc: Arc<kafka::KafkaProducer>,
    encryptor: Arc<crypto::RotatingSecretEncryptor>,
) -> Arc<app_state::FlowState> {
    // Initialize LLM cost calculator and processor
    let cost_calculator = llm::CostCalculator::new(db_pool_arc.clone());
    if let Err(e) = cost_calculator.initialize().await {
        tracing::warn!("Failed to initialize LLM cost calculator: {}", e);
    }
    let llm_processor = Arc::new(llm::LlmSpanProcessor::new(cost_calculator));

    // Initialize model catalog cache (in-memory mirror of model_catalog table)
    let model_catalog_cache = Arc::new(
        gateway::model_catalog_cache::ModelCatalogCache::new(db_pool_arc.clone()),
    );
    if let Err(e) = model_catalog_cache.initialize().await {
        tracing::warn!("Failed to initialize model catalog cache: {}", e);
    }

    // Initialize global model stats cache (ClickHouse aggregation for pricing page)
    let global_model_stats = Arc::new(
        gateway::global_model_stats::GlobalModelStatsCache::new(clickhouse_pool_arc.clone()),
    );
    global_model_stats.initialize().await;

    // Initialize latency tracker for adaptive routing (uses ClickHouse for storage)
    let latency_tracker = Arc::new(gateway::latency_tracker::LatencyTracker::new(
        clickhouse_pool_arc.clone(),
    ));

    // Build typed default provider keys from env vars.
    use gateway::provider_types::Provider;
    let mut typed_default_keys = std::collections::HashMap::new();
    if let Some(k) = &config_arc.gateway_default_openai_api_key {
        typed_default_keys.insert(Provider::OpenAi, k.clone());
    }
    if let Some(k) = &config_arc.gateway_default_anthropic_api_key {
        typed_default_keys.insert(Provider::Anthropic, k.clone());
    }
    if let Some(k) = &config_arc.gateway_default_google_api_key {
        typed_default_keys.insert(Provider::Google, k.clone());
    }
    if let Some(k) = &config_arc.gateway_default_theta_api_key {
        typed_default_keys.insert(Provider::Theta, k.clone());
    }
    if let Some(k) = &config_arc.gateway_default_deepseek_api_key {
        typed_default_keys.insert(Provider::DeepSeek, k.clone());
    }
    if !typed_default_keys.is_empty() {
        let providers: Vec<&str> = typed_default_keys.keys().map(|p| p.as_str()).collect();
        tracing::info!(
            providers = ?providers,
            "Platform default API keys loaded for providers"
        );
    }

    // Build ProviderManager -- typed, central provider registry
    let provider_manager = Arc::new(
        gateway::provider_manager::ProviderManager::from_config(
            &config_arc,
            typed_default_keys.clone(),
        )
        .with_latency_tracker(latency_tracker.clone())
        .with_model_catalog_cache((*model_catalog_cache).clone()),
    );

    let circuit_breaker = Arc::new(gateway::circuit_breaker::CircuitBreaker::new());

    let gateway_router = Arc::new(
        gateway::GatewayRouter::from_config(&config_arc)
            .with_latency_tracker(latency_tracker.clone())
            .with_circuit_breaker(circuit_breaker.clone()),
    );
    let gateway_cache = Arc::new(gateway::cache::GatewayCache::new(
        config_arc.gateway_cache_url.clone(),
        config_arc.gateway_cache_ttl_seconds,
        config_arc.gateway_cache_enabled,
    ));

    let fallback_config = Arc::new(gateway::fallback::FallbackConfig::from_config(&config_arc));

    // Spawn background task to refresh latency cache from ClickHouse every minute
    // and log degraded providers / open circuit breakers.
    gateway::latency_sync::spawn_latency_cache_refresh_task(
        latency_tracker.clone(),
        circuit_breaker.clone(),
    );

    // Batched LLM request writes (avoids one insert per request under load)
    let llm_request_tx =
        gateway::llm_request_buffer::spawn(llm_processor.clone(), clickhouse_pool_arc.clone());

    let mut action_registry = reiver_mcp::registry::ActionRegistry::new();
    reiver_mcp::actions::register_all(&mut action_registry);
    tracing::info!(
        tool_count = action_registry.tools_list().len(),
        "MCP action registry initialised for in-app agent"
    );

    let credit_service = Arc::new(reiver_core::billing::credits::CreditService::new(
        db_pool_arc.clone(),
        redis_pool_arc.clone(),
    ));

    let meter_service = Arc::new(
        if let Some(ref api_key) = config_arc.stripe_api_key {
            reiver_core::billing::MeterService::from_api_key(api_key, db_pool_arc.clone())
        } else {
            reiver_core::billing::MeterService::noop()
        },
    );

    if let Some(ref api_key) = config_arc.stripe_api_key {
        reiver_core::billing::credit_balance_sync::spawn_credit_balance_sync_from_key(
            api_key,
            db_pool_arc.clone(),
            redis_pool_arc.clone(),
        );
    }

    let asset_storage: Arc<dyn reiver_core::storage::AssetStorage> = {
        let backend = std::env::var("ASSET_STORAGE_BACKEND").unwrap_or_else(|_| "local".into());
        match backend.as_str() {
            "s3" => {
                let config = reiver_core::storage::S3Storage::from_env()
                    .await
                    .expect("Failed to create S3 asset storage from env");
                Arc::new(config)
            }
            _ => {
                let path = std::env::var("ASSET_STORAGE_LOCAL_PATH")
                    .unwrap_or_else(|_| "/tmp/reiver-assets".into());
                let url = std::env::var("ASSET_STORAGE_LOCAL_URL")
                    .unwrap_or_else(|_| "http://localhost:3001/assets".into());
                Arc::new(reiver_core::storage::LocalFileStorage::new(&path, &url))
            }
        }
    };

    let credits_enabled = config_arc.credits_enabled;

    let prompt_store: Arc<dyn gateway::prompt_store::PromptWriteStore> = Arc::new(
        PgPromptStore::new(db_pool_arc.as_ref().clone(), redis_pool_arc.as_ref().clone()),
    );

    let entitlement_service: Arc<dyn reiver_core::entitlements::EntitlementChecker> = Arc::new(
        reiver_core::entitlements::EntitlementService::new(db_pool_arc.clone()),
    );

    let watch_url =
        std::env::var("WATCH_URL").unwrap_or_else(|_| "http://localhost:3000".into());

    let otel_http_client = reqwest::Client::builder()
        .pool_max_idle_per_host(16)
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("Failed to build OTel publisher HTTP client");
    let otel_publisher = reiver_flow::gateway::otel_publisher::OTelPublisher::start(
        watch_url.clone(),
        otel_http_client,
    );

    Arc::new(app_state::FlowState {
        db: db_pool_arc,
        clickhouse: clickhouse_pool_arc,
        redis: redis_pool_arc,
        config: config_arc,
        credits_enabled,
        encryptor,
        prompt_store,
        http_client: reqwest::Client::builder()
            .pool_max_idle_per_host(64)
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("Failed to build HTTP client"),
        kafka: kafka_producer_arc.clone(),
        event_publisher: Arc::new(reiver_core::events::EventPublisher::new(
            kafka_producer_arc,
            reiver_core::events::EventSource::Flow,
        )),
        provider_manager,
        gateway_router,
        gateway_cache,
        llm_processor,
        llm_request_tx,
        latency_tracker,
        provider_key_cache: quick_cache::sync::Cache::new(256),
        introspection_settings_cache: quick_cache::sync::Cache::new(1024),
        fallback_config,
        metrics: Arc::new(reiver_flow::metrics::FlowMetrics::new()),
        action_registry: Arc::new(action_registry),
        compiler_cancel_tokens: dashmap::DashMap::new(),
        agent_conversation_locks: dashmap::DashMap::new(),
        agent_http_client: reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .pool_idle_timeout(std::time::Duration::from_secs(30))
            .pool_max_idle_per_host(16)
            .tcp_keepalive(std::time::Duration::from_secs(15))
            .build()
            .expect("failed to build agent HTTP client"),
        moodeng_project_id: {
            let pid = std::env::var("MOODENG_PROJECT_ID")
                .ok()
                .and_then(|s| uuid::Uuid::parse_str(&s).ok());
            if pid.is_none() {
                tracing::error!("MOODENG_PROJECT_ID not set: MooDeng fee splitting will not work, all gateway traffic will use gateway_fee_percent");
            }
            pid
        },
        credit_service,
        meter_service,
        asset_storage,
        otel_publisher,
        internal_urls: app_state::InternalServiceUrls {
            website: std::env::var("WEBSITE_URL")
                .unwrap_or_else(|_| "http://localhost:3003".into()),
            flow: std::env::var("FLOW_URL").unwrap_or_else(|_| "http://localhost:3001".into()),
            watch: watch_url,
            herd: std::env::var("HERD_URL").unwrap_or_else(|_| "http://localhost:3003".into()),
        },
        project_org_cache: quick_cache::sync::Cache::new(1024),
        model_catalog_cache,
        global_model_stats,
        entitlements: entitlement_service,
        kb_embedder: Arc::new(
            reiver_core::embeddings::KbEmbedder::new()
                .expect("Failed to initialize knowledge base embedding model"),
        ),
    })
}

fn create_flow_router(flow_state: Arc<app_state::FlowState>) -> anyhow::Result<Router> {
    let is_production = std::env::var("ENVIRONMENT")
        .map(|e| e.to_lowercase() == "production")
        .unwrap_or(false);

    if flow_state.config.trusted_proxy_cidrs.is_empty() {
        if is_production {
            return Err(anyhow::anyhow!(
                "TRUSTED_PROXY_CIDRS must be set in production to prevent header spoofing"
            ));
        }
        tracing::warn!(
            "TRUSTED_PROXY_CIDRS is not set. \
             X-User-Id and X-Project-Id headers are trusted from any IP. \
             Set TRUSTED_PROXY_CIDRS to the CIDR of your website proxy to \
             prevent header spoofing."
        );
    } else {
        tracing::info!(
            cidrs = ?flow_state.config.trusted_proxy_cidrs,
            "Trusted proxy CIDR enforcement enabled"
        );
    }

    let flow_api = api::create_flow_api_router();
    let a2a_receiver = api::a2a_receiver::create_a2a_receiver_router();

    let router = Router::new()
        .route("/health", get(health_check))
        .route("/ready", get(readiness_check))
        .merge(a2a_receiver.with_state(flow_state.clone()))
        .nest("/api", flow_api.clone().with_state(flow_state.clone()))
        .nest("/api/v1", flow_api.with_state(flow_state.clone()))
        .layer(middleware::from_fn_with_state(
            flow_state.config.clone(),
            trusted_proxy::trusted_proxy_middleware,
        ))
        .layer(Extension(flow_state.db.clone()))
        .layer(Extension(flow_state.clickhouse.clone()))
        .layer(Extension(flow_state.config.clone()))
        .layer(axum::middleware::from_fn(
            reiver_core::http_metrics::layer,
        ))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &axum::http::Request<_>| {
                    let method = request.method().as_str();
                    let path = request.uri().path();
                    let span_name = format!("{method} {path}");

                    let is_probe = matches!(path, "/health" | "/ready");

                    // NOTE: every field that may be `.record()`ed on response
                    // must be declared here with `Empty`; otherwise the record
                    // is silently dropped.
                    let span = if is_probe {
                        tracing::debug_span!(
                            "http.request",
                            otel.name = %span_name,
                            otel.kind = "server",
                            otel.status_code = tracing::field::Empty,
                            otel.status_message = tracing::field::Empty,
                            http.method = %request.method(),
                            http.route = %request.uri().path(),
                            http.target = %request.uri(),
                            http.status_code = tracing::field::Empty,
                            error.kind = tracing::field::Empty,
                            error.message = tracing::field::Empty,
                        )
                    } else {
                        tracing::info_span!(
                            "http.request",
                            otel.name = %span_name,
                            otel.kind = "server",
                            otel.status_code = tracing::field::Empty,
                            otel.status_message = tracing::field::Empty,
                            http.method = %request.method(),
                            http.route = %request.uri().path(),
                            http.target = %request.uri(),
                            http.status_code = tracing::field::Empty,
                            error.kind = tracing::field::Empty,
                            error.message = tracing::field::Empty,
                        )
                    };

                    let parent_cx = opentelemetry::global::get_text_map_propagator(|prop| {
                        prop.extract(&HeaderExtractor(request.headers()))
                    });
                    use tracing_opentelemetry::OpenTelemetrySpanExt;
                    let _ = span.set_parent(parent_cx);

                    span
                })
                .on_response(
                    |response: &axum::http::Response<_>,
                     latency: std::time::Duration,
                     span: &tracing::Span| {
                        let status = response.status().as_u16();
                        span.record("http.status_code", status);
                        let canonical_reason =
                            response.status().canonical_reason().unwrap_or("unknown");
                        // `AppError::into_response` stashes the underlying error
                        // kind + message here. Prefer that over the bare HTTP
                        // canonical reason because it contains the actual reason
                        // (e.g. "Missing or invalid X-User-Id header" rather than
                        // just "Unauthorized"). Falls back to `canonical_reason`
                        // for non-`AppError` responses (axum built-ins, manual
                        // `StatusCode`, etc.).
                        let error_info = response
                            .extensions()
                            .get::<reiver_core::error::AppErrorInfo>();
                        let (error_kind, error_message): (&str, &str) = match error_info {
                            Some(info) => (info.kind, info.message.as_str()),
                            None => ("http", canonical_reason),
                        };
                        if status >= 500 {
                            span.record("otel.status_code", "ERROR");
                            span.record("otel.status_message", error_message);
                            span.record("error.kind", error_kind);
                            span.record("error.message", error_message);
                            tracing::error!(
                                latency_ms = latency.as_millis(),
                                status = status,
                                error.kind = error_kind,
                                error.message = error_message,
                                "HTTP response"
                            );
                        } else if status >= 400 {
                            span.record("otel.status_code", "ERROR");
                            span.record("otel.status_message", error_message);
                            span.record("error.kind", error_kind);
                            span.record("error.message", error_message);
                            tracing::warn!(
                                latency_ms = latency.as_millis(),
                                status = status,
                                error.kind = error_kind,
                                error.message = error_message,
                                "HTTP response"
                            );
                        } else {
                            span.record("otel.status_code", "OK");
                            tracing::info!(
                                latency_ms = latency.as_millis(),
                                status = status,
                                "HTTP response"
                            );
                        }
                    },
                ),
        )
        .with_state(flow_state);

    Ok(router)
}

/// Run Flow API + LLM workers
async fn run_all(
    config: Config,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (db_pool_arc, clickhouse_pool_arc, redis_pool_arc) = init_core_connections(&config).await?;
    let config_arc = Arc::new(config.clone());
    let kafka_producer_arc = create_kafka_producer(&config)?;
    let encryptor = create_encryptor(&config)?;

    let flow_state = create_flow_state(
        config_arc.clone(),
        db_pool_arc.clone(),
        clickhouse_pool_arc.clone(),
        redis_pool_arc.clone(),
        kafka_producer_arc.clone(),
        encryptor,
    )
    .await;

    // Register MooDeng as an A2A agent in Herd (best-effort, non-blocking)
    if !flow_state.internal_urls.herd.is_empty() {
        let herd_url = flow_state.internal_urls.herd.clone();
        let flow_url = flow_state.internal_urls.flow.clone();
        let moodeng_pid = flow_state.moodeng_project_id;
        let db = db_pool_arc.clone();
        tokio::spawn(async move {
            register_moodeng_in_herd(&herd_url, &flow_url, moodeng_pid, &db).await;
        });
    }

    // Start LLM rollout worker for auto-promote/rollback
    let rollout_event_publisher = Arc::new(reiver_core::events::EventPublisher::new(
        kafka_producer_arc.clone(),
        reiver_core::events::EventSource::Flow,
    ));
    let rollout_worker_handle = rollout_worker::start_rollout_worker(
        db_pool_arc.clone(),
        clickhouse_pool_arc.clone(),
        redis_pool_arc.clone(),
        rollout_event_publisher,
        shutdown_rx.clone(),
    );

    // Start secret slot expiry cleanup (every 60s)
    {
        let db = db_pool_arc.clone();
        let mut shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        api::secret_slots::cleanup_expired_slots(&db).await;
                    }
                    _ = shutdown.changed() => break,
                }
            }
        });
    }

    // Start session eval producer (finds idle sessions, enqueues to Kafka)
    let session_evaluator_handle = gateway::session_evaluator::spawn(
        clickhouse_pool_arc.clone(),
        kafka_producer_arc.clone(),
        redis_pool_arc.clone(),
        shutdown_rx.clone(),
    );

    // Start session eval consumer (classifies + matches profiles from Kafka)
    let session_eval_consumer_handle =
        gateway::session_eval_consumer::spawn(flow_state.clone(), shutdown_rx.clone());

    let app = create_flow_router(flow_state)?;
    let listener = tokio::net::TcpListener::bind(DEFAULT_LISTEN_ADDR).await?;
    tracing::info!("Flow server listening on http://{}", DEFAULT_LISTEN_ADDR);

    let mut server_handle = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
    });

    let sig = shutdown_signal();
    tokio::pin!(sig);

    let shutdown_received = tokio::select! {
        result = &mut server_handle => {
            match result {
                Ok(Ok(())) => tracing::info!("Server stopped normally"),
                Ok(Err(e)) => return Err(anyhow::anyhow!("Server error: {}", e)),
                Err(e) => return Err(anyhow::anyhow!("Server task panicked: {}", e)),
            }
            false
        }
        _ = &mut sig => {
            true
        }
    };

    if shutdown_received {
        server_handle.abort();
        if let Err(e) = shutdown_tx.send(true) {
            tracing::warn!("Failed to send shutdown signal: {}", e);
        }

        tracing::info!(
            "Waiting for workers to finish (timeout: {}s)...",
            WORKER_SHUTDOWN_TIMEOUT_SECS
        );
        let _ = tokio::time::timeout(Duration::from_secs(WORKER_SHUTDOWN_TIMEOUT_SECS), async {
            let _ = tokio::join!(
                rollout_worker_handle,
                session_evaluator_handle,
                session_eval_consumer_handle
            );
        })
        .await;
        tracing::info!("All Flow workers stopped");
    }

    let _ = server_handle.await;
    Ok(())
}

/// Run only the Flow API (no workers)
async fn run_api_only(config: Config) -> anyhow::Result<()> {
    let (db_pool_arc, clickhouse_pool_arc, redis_pool_arc) = init_core_connections(&config).await?;
    let config_arc = Arc::new(config.clone());
    let kafka_producer_arc = create_kafka_producer(&config)?;
    let encryptor = create_encryptor(&config)?;

    let flow_state = create_flow_state(
        config_arc,
        db_pool_arc,
        clickhouse_pool_arc,
        redis_pool_arc,
        kafka_producer_arc,
        encryptor,
    )
    .await;

    let app = create_flow_router(flow_state)?;
    let listener = tokio::net::TcpListener::bind(DEFAULT_LISTEN_ADDR).await?;
    tracing::info!(
        "Flow API server listening on http://{} (workers disabled)",
        DEFAULT_LISTEN_ADDR
    );

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;

    tracing::info!("Flow API server stopped");
    Ok(())
}

/// Run only Flow workers (no API)
async fn run_workers_only(
    config: Config,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (db_pool_arc, clickhouse_pool_arc, redis_pool_arc) = init_core_connections(&config).await?;
    let kafka_producer_arc = create_kafka_producer(&config)?;

    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    // Start rollout worker
    let rollout_event_publisher = Arc::new(reiver_core::events::EventPublisher::new(
        kafka_producer_arc.clone(),
        reiver_core::events::EventSource::Flow,
    ));
    handles.push(rollout_worker::start_rollout_worker(
        db_pool_arc.clone(),
        clickhouse_pool_arc.clone(),
        redis_pool_arc.clone(),
        rollout_event_publisher,
        shutdown_rx.clone(),
    ));

    // Start session eval producer only (consumer requires FlowState / moodeng,
    // which is only available in the full server mode)
    handles.push(gateway::session_evaluator::spawn(
        clickhouse_pool_arc.clone(),
        kafka_producer_arc.clone(),
        redis_pool_arc.clone(),
        shutdown_rx.clone(),
    ));

    tracing::info!("Flow workers started (API disabled)");
    wait_for_shutdown(shutdown_tx, handles).await;
    Ok(())
}

/// Wait for shutdown signal and gracefully stop
async fn wait_for_shutdown(
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    handles: Vec<tokio::task::JoinHandle<()>>,
) {
    shutdown_signal().await;

    if let Err(e) = shutdown_tx.send(true) {
        tracing::warn!("Failed to send shutdown signal: {}", e);
    }

    tracing::info!(
        "Waiting for workers to finish (timeout: {}s)...",
        WORKER_SHUTDOWN_TIMEOUT_SECS
    );
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(WORKER_SHUTDOWN_TIMEOUT_SECS),
        async {
            for handle in handles {
                let _ = handle.await;
            }
        },
    )
    .await;

    tracing::info!("All workers stopped");
}

/// Wait for either SIGINT (ctrl-c) or SIGTERM (Nomad drain / `docker stop`).
///
/// Both signals trigger a graceful shutdown. Using only `ctrl_c()` would miss
/// SIGTERM, causing Nomad to force-kill the process after its `kill_timeout`
/// and dropping in-flight LLM streaming connections.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("Failed to register SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("Received SIGINT — starting graceful shutdown");
            }
            _ = sigterm.recv() => {
                tracing::info!("Received SIGTERM — starting graceful shutdown");
            }
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for ctrl-c");
        tracing::info!("Received shutdown signal");
    }
}

/// Liveness probe — always returns 200 if the process is running.
async fn health_check() -> &'static str {
    "OK"
}

/// Readiness probe — verifies that all backing services are reachable.
///
/// Returns 200 when PostgreSQL, Redis, ClickHouse, and Kafka all respond.
/// Returns 503 with a JSON body describing which checks failed when any
/// service is down.
async fn readiness_check(
    State(state): State<Arc<app_state::FlowState>>,
) -> (StatusCode, axum::Json<serde_json::Value>) {
    use bb8_redis::redis::AsyncCommands;

    let mut checks: Vec<(&str, bool, String)> = Vec::new();

    // PostgreSQL
    let pg_ok = sqlx::query("SELECT 1")
        .fetch_one(state.db.as_ref())
        .await
        .is_ok();
    checks.push((
        "postgres",
        pg_ok,
        if pg_ok {
            "ok".into()
        } else {
            "unreachable".into()
        },
    ));

    // Redis — PING check via a harmless GET on a non-existent key
    let redis_ok = match state.redis.get().await {
        Ok(mut conn) => {
            let result: Result<Option<String>, _> = conn.get("_ready_ping").await;
            result.is_ok()
        }
        Err(_) => false,
    };
    checks.push((
        "redis",
        redis_ok,
        if redis_ok {
            "ok".into()
        } else {
            "unreachable".into()
        },
    ));

    // ClickHouse
    let ch_ok = state
        .clickhouse
        .query("SELECT 1")
        .fetch_one::<u8>()
        .await
        .is_ok();
    checks.push((
        "clickhouse",
        ch_ok,
        if ch_ok {
            "ok".into()
        } else {
            "unreachable".into()
        },
    ));

    // Kafka
    let kafka_ok = state.kafka.is_healthy();
    checks.push((
        "kafka",
        kafka_ok,
        if kafka_ok {
            "ok".into()
        } else {
            "unreachable".into()
        },
    ));

    let all_ok = checks.iter().all(|(_, ok, _)| *ok);
    let status = if all_ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    let body = serde_json::json!({
        "status": if all_ok { "ready" } else { "not_ready" },
        "checks": checks.iter().map(|(name, ok, msg)| {
            serde_json::json!({ "name": name, "ok": ok, "message": msg })
        }).collect::<Vec<_>>(),
    });

    (status, axum::Json(body))
}

/// Best-effort registration of MooDeng as an A2A agent in Herd.
/// Called once at startup. If Herd is unavailable or the agent already exists,
/// the error is logged and silently ignored.
async fn register_moodeng_in_herd(
    herd_url: &str,
    flow_url: &str,
    moodeng_project_id: Option<uuid::Uuid>,
    db: &sqlx::PgPool,
) {
    use reqwest::Client;

    let project_id = match moodeng_project_id {
        Some(pid) => pid,
        None => {
            tracing::debug!("MOODENG_PROJECT_ID not set, skipping Herd auto-registration");
            return;
        }
    };

    let org_id: Option<uuid::Uuid> =
        sqlx::query_scalar("SELECT organization_id FROM projects WHERE id = $1")
            .bind(project_id)
            .fetch_optional(db)
            .await
            .unwrap_or(None);

    let org_id = match org_id {
        Some(oid) => oid,
        None => {
            tracing::warn!(
                "Could not resolve org for moodeng project {}, skipping Herd registration",
                project_id
            );
            return;
        }
    };

    let key_id: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT id FROM project_keys WHERE project_id = $1 AND key_type = 'agent' ORDER BY created_at LIMIT 1"
    )
    .bind(project_id)
    .fetch_optional(db)
    .await
    .unwrap_or(None);

    let client = Client::new();
    let body = serde_json::json!({
        "name": "moodeng",
        "description": "Reiver's AI assistant — can analyze data, manage dashboards, configure alerts, and more.",
        "endpointUrl": format!("{}/a2a", flow_url),
        "keyId": key_id,
        "visibility": "org",
    });

    let url = format!("{}/api/herd/agents", herd_url);
    let result = client
        .post(&url)
        .header("X-Project-Id", project_id.to_string())
        .header("X-Organization-Id", org_id.to_string())
        .json(&body)
        .send()
        .await;

    match result {
        Ok(resp) if resp.status().is_success() => {
            tracing::info!(
                "Registered MooDeng as A2A agent in Herd for project {}",
                project_id
            );
        }
        Ok(resp) if resp.status().as_u16() == 409 => {
            tracing::debug!(
                "MooDeng already registered in Herd for project {}",
                project_id
            );
        }
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            tracing::warn!("Failed to register MooDeng in Herd: {} - {}", status, body);
        }
        Err(e) => {
            tracing::warn!("Could not reach Herd for MooDeng registration: {}", e);
        }
    }
}
