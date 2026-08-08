#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(target_os = "linux")]
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";

use reiver_watch::{
    aggregation_worker,
    alert_worker,
    api,
    app_state,
    clickhouse_db,
    crypto,
    db,
    event_worker,
    github,
    intern,
    kafka,
    // Workers (APM-specific only)
    kafka_consumer,
    kafka_log_consumer,
    llm,
    metrics_worker,
    spans_worker,
    telemetry,
};

use axum::{extract::Extension, routing::get, Router};
use clap::{Parser, ValueEnum};
use std::sync::Arc;
use tower_http::trace::TraceLayer;

use bb8_redis::redis::AsyncCommands;
use reiver_watch::config::Config;

/// Default listen address for the Watch API server.
const DEFAULT_LISTEN_ADDR: &str = "0.0.0.0:3000";
/// Maximum size of the Redis connection pool.
const REDIS_POOL_MAX_SIZE: u32 = 15;
/// Timeout in seconds for graceful worker shutdown.
const WORKER_SHUTDOWN_TIMEOUT_SECS: u64 = 30;
/// Capacity of the broadcast channel for SSE stats updates.
const STATS_BROADCAST_CAPACITY: usize = 1000;

/// Reiver API server and workers
#[derive(Parser, Debug)]
#[command(name = "reiver-api")]
#[command(about = "Reiver observability platform API and workers")]
struct Cli {
    /// Which mode to run in
    #[arg(long, value_enum, default_value = "all")]
    mode: WorkerMode,
}

/// Available worker modes for independent scaling
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum WorkerMode {
    /// Run everything (API + all workers) - default for development
    All,
    /// Run only the HTTP API server (no workers)
    Api,
    /// Run all workers (no API)
    Workers,
    /// Run only the Kafka exception consumer
    KafkaConsumer,
    /// Run only the Kafka log consumer
    KafkaLogConsumer,
    /// Run only the alert evaluation worker
    AlertWorker,
    /// Run only the aggregation worker
    AggregationWorker,
    // /// Run only the AWS integration worker
    // AwsWorker,
    // /// Run only the Azure integration worker
    // AzureWorker,
    // /// Run only the GCP integration worker
    // GcpWorker,
    // /// Run only the OCI integration worker
    // OciWorker,
    // /// Run only the Snowflake integration worker
    // SnowflakeWorker,
    // /// Run only the LLM pricing sync worker
    // PricingWorker,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Octocrab (GitHub API client) uses rustls which requires a global crypto provider.
    let _ = rustls::crypto::ring::default_provider().install_default();

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

    let profiler = if config.profiling_enabled {
        let api_url = config
            .otel_exporter_endpoint
            .as_deref()
            .map(|ep| ep.trim_end_matches("/api").to_string());
        let sdk_options = reiver_sdk::ClientOptions {
            project_id: config.otel_project_id.clone(),
            api_url,
            service_name: Some("reiver-watch".to_string()),
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

    // Pre-warm string interner with common OTLP attribute keys
    intern::prewarm_common_keys();

    let cli = Cli::parse();
    tracing::info!("Starting reiver-api in {:?} mode", cli.mode);

    // Create shutdown signal channel for graceful worker shutdown
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let result = match cli.mode {
        WorkerMode::All => run_all(config, shutdown_tx, shutdown_rx).await,
        WorkerMode::Api => run_api_only(config, shutdown_tx, shutdown_rx).await,
        WorkerMode::Workers => run_all_workers(config, shutdown_tx, shutdown_rx).await,
        WorkerMode::KafkaConsumer => run_kafka_consumer(config, shutdown_tx, shutdown_rx).await,
        WorkerMode::KafkaLogConsumer => {
            run_kafka_log_consumer(config, shutdown_tx, shutdown_rx).await
        }
        WorkerMode::AlertWorker => run_alert_worker(config, shutdown_tx, shutdown_rx).await,
        WorkerMode::AggregationWorker => {
            run_aggregation_worker(config, shutdown_tx, shutdown_rx).await
        } //WorkerMode::AwsWorker => run_aws_worker(config, shutdown_tx, shutdown_rx).await,
          //WorkerMode::AzureWorker => run_azure_worker(config, shutdown_tx, shutdown_rx).await,
          //WorkerMode::GcpWorker => run_gcp_worker(config, shutdown_tx, shutdown_rx).await,
          //WorkerMode::OciWorker => run_oci_worker(config, shutdown_tx, shutdown_rx).await,
          //WorkerMode::SnowflakeWorker => run_snowflake_worker(config, shutdown_tx, shutdown_rx).await,
          //WorkerMode::PricingWorker => run_pricing_worker(config, shutdown_tx, shutdown_rx).await,
    };

    if let Some(p) = profiler {
        p.shutdown(std::time::Duration::from_secs(5)).await;
    }

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

    result
}

/// Initialize core database connections (PostgreSQL, ClickHouse, Redis)
async fn init_core_connections(
    config: &Config,
) -> anyhow::Result<(
    Arc<db::DbPool>,
    Arc<clickhouse_db::ClickHousePool>,
    Arc<app_state::RedisPool>,
)> {
    // Ensure database exists before connecting
    db::ensure_database_exists(&config.database_url).await?;

    let db_pool = db::create_pool(&config.database_url).await?;

    // Create ClickHouse connection
    tracing::info!("Connecting to ClickHouse at {}", config.clickhouse_url);
    let clickhouse_pool = clickhouse_db::create_clickhouse_pool(&config.clickhouse_url)?;

    // Initialize Watch-specific ClickHouse Kafka Engine (exceptions)
    // Note: ClickHouse schema migrations are handled by the Website service
    tracing::info!(
        "Initializing Watch ClickHouse Kafka Engine with Kafka hosts: {}",
        config.clickhouse_kafka_hosts
    );
    clickhouse_db::init_watch_kafka_engine(
        &clickhouse_pool,
        &config.clickhouse_kafka_hosts,
        &config.kafka_exceptions_topic,
    )
    .await?;
    tracing::info!("Watch ClickHouse Kafka Engine initialized successfully");

    // Create Redis connection pool
    tracing::info!("Connecting to Redis at {}", config.redis_url);
    let manager = bb8_redis::RedisConnectionManager::new(config.redis_url.clone())
        .map_err(|e| anyhow::anyhow!("Failed to create Redis connection manager: {}. Make sure Redis is running and REDIS_URL is correct.", e))?;

    let redis_pool = bb8::Pool::builder()
        .max_size(REDIS_POOL_MAX_SIZE)
        .build(manager)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create Redis connection pool: {}", e))?;

    // Test Redis connection
    {
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
    } // test_conn dropped here, releasing the borrow
    tracing::info!("Redis connection pool established successfully");

    Ok((
        Arc::new(db_pool),
        Arc::new(clickhouse_pool),
        Arc::new(redis_pool),
    ))
}

/// Create secret encryptor for SSO secrets, API tokens, etc.
///
/// # Security
/// - In production (ENVIRONMENT=production), ENCRYPTION_KEY is required
/// - In development, a temporary key is generated with a warning
/// - This prevents accidental deployment without proper encryption key
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
                } else {
                    tracing::info!("Secret encryptor initialized with provided key");
                }
                Ok(Arc::new(e))
            }
            Err(e) => {
                if is_production {
                    tracing::error!("SECURITY: Invalid ENCRYPTION_KEY in production: {}", e);
                    return Err(anyhow::anyhow!(
                        "Invalid ENCRYPTION_KEY in production environment. Generate a valid key with: openssl rand -base64 32"
                    ));
                }
                tracing::error!("Invalid ENCRYPTION_KEY: {}. Generating temporary key (secrets won't persist across restarts!)", e);
                let temp_key = crypto::SecretEncryptor::generate_key();
                Ok(Arc::new(
                    crypto::RotatingSecretEncryptor::single_key(&temp_key)
                        .expect("Generated key should be valid"),
                ))
            }
        }
    } else {
        if is_production {
            tracing::error!("SECURITY: ENCRYPTION_KEY is required in production environment");
            return Err(anyhow::anyhow!(
                "ENCRYPTION_KEY environment variable is required in production. Generate a key with: openssl rand -base64 32"
            ));
        }
        tracing::warn!("ENCRYPTION_KEY not set. Generating temporary key (secrets won't persist across restarts!)");
        tracing::warn!("Generate a key with: openssl rand -base64 32");
        let temp_key = crypto::SecretEncryptor::generate_key();
        Ok(Arc::new(
            crypto::RotatingSecretEncryptor::single_key(&temp_key)
                .expect("Generated key should be valid"),
        ))
    }
}

/// Wait for shutdown signal and gracefully stop
async fn wait_for_shutdown(
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    handles: Vec<tokio::task::JoinHandle<()>>,
) {
    // Wait for shutdown signal
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for ctrl-c");
    tracing::info!("Received shutdown signal");

    // Signal all workers to shutdown gracefully
    tracing::info!("Signaling workers to shutdown gracefully...");
    if let Err(e) = shutdown_tx.send(true) {
        tracing::warn!("Failed to send shutdown signal: {}", e);
    }

    // Give workers time to finish current work
    tracing::info!(
        "Waiting for workers to finish (timeout: {}s)...",
        WORKER_SHUTDOWN_TIMEOUT_SECS
    );
    let shutdown_timeout = std::time::Duration::from_secs(WORKER_SHUTDOWN_TIMEOUT_SECS);

    let _ = tokio::time::timeout(shutdown_timeout, async {
        for handle in handles {
            let _ = handle.await;
        }
    })
    .await;

    tracing::info!("All workers stopped");
}

/// Run everything (API + all workers) - default mode
async fn run_all(
    config: Config,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (db_pool_arc, clickhouse_pool_arc, redis_pool_arc) = init_core_connections(&config).await?;
    let config_arc = Arc::new(config.clone());

    // Initialize Kafka producer
    tracing::info!(
        "Initializing Kafka producer with Kafka hosts: {}",
        config.kafka_hosts
    );
    let kafka_producer = kafka::KafkaProducer::new(&build_kafka_config(&config))?;
    let kafka_producer_arc = Arc::new(kafka_producer);

    let event_publisher = Arc::new(reiver_core::events::EventPublisher::new(
        kafka_producer_arc.clone(),
        reiver_core::events::EventSource::Watch,
    ));

    // Create broadcast channel for stats updates (SSE)
    let (stats_broadcast, _) =
        tokio::sync::broadcast::channel::<app_state::StatsUpdateMessage>(STATS_BROADCAST_CAPACITY);
    let stats_broadcast_clone = stats_broadcast.clone();

    // Create secret encryptor early - needed by cloud integration workers
    let encryptor = create_encryptor(&config)?;

    // Start all workers
    let kafka_consumer_handle = kafka_consumer::start_kafka_error_consumer(
        &config.kafka_hosts,
        &config.kafka_exceptions_topic,
        config.kafka_client_id.as_deref(),
        db_pool_arc.clone(),
        clickhouse_pool_arc.clone(),
        redis_pool_arc.clone(),
        config_arc.clone(),
        stats_broadcast_clone,
        event_publisher.clone(),
        shutdown_rx.clone(),
    )
    .await?;

    let kafka_log_consumer_handle = kafka_log_consumer::start_kafka_log_consumer(
        &config.kafka_hosts,
        &config.kafka_logs_otlp_topic,
        &config.kafka_logs_unstructured_topic,
        config.kafka_client_id.as_deref(),
        db_pool_arc.clone(),
        clickhouse_pool_arc.clone(),
        redis_pool_arc.clone(),
        config_arc.clone(),
        shutdown_rx.clone(),
    )
    .await?;

    // Start spans worker (consumes spans from Kafka, writes to ClickHouse)
    tracing::info!(
        "Starting spans worker with Kafka hosts: {}",
        config.kafka_hosts
    );
    let spans_worker_handle = spans_worker::start_spans_worker(
        &config.kafka_hosts,
        &config.kafka_spans_topic,
        config.kafka_client_id.as_deref(),
        db_pool_arc.clone(),
        clickhouse_pool_arc.clone(),
        redis_pool_arc.clone(),
        config_arc.clone(),
        kafka_producer_arc.clone(),
        shutdown_rx.clone(),
    )
    .await?;

    // Start metrics worker (consumes metrics from Kafka, writes to ClickHouse)
    tracing::info!(
        "Starting metrics worker with Kafka hosts: {}",
        config.kafka_hosts
    );
    let metrics_worker_handle = metrics_worker::start_metrics_worker(
        &config.kafka_hosts,
        &config.kafka_metrics_topic,
        config.kafka_client_id.as_deref(),
        db_pool_arc.clone(),
        clickhouse_pool_arc.clone(),
        config_arc.clone(),
        shutdown_rx.clone(),
    )
    .await?;

    let alert_worker_handle = alert_worker::start_alert_worker(
        db_pool_arc.clone(),
        clickhouse_pool_arc.clone(),
        event_publisher.clone(),
        shutdown_rx.clone(),
    )
    .await?;

    let aggregation_handle = aggregation_worker::start_aggregation_worker(
        redis_pool_arc.clone(),
        clickhouse_pool_arc.clone(),
        shutdown_rx.clone(),
    )
    .await?;

    // let aws_worker_handle = aws_worker::start_aws_worker(
    //     db_pool_arc.clone(),
    //     clickhouse_pool_arc.clone(),
    //     shutdown_rx.clone(),
    // ).await?;
    //
    // let azure_worker_handle = azure_worker::start_azure_worker(
    //     db_pool_arc.clone(),
    //     clickhouse_pool_arc.clone(),
    //     encryptor.clone(),
    //     shutdown_rx.clone(),
    // ).await?;
    //
    // let gcp_worker_handle = gcp_worker::start_gcp_worker(
    //     db_pool_arc.clone(),
    //     clickhouse_pool_arc.clone(),
    //     encryptor.clone(),
    //     shutdown_rx.clone(),
    // ).await?;
    //
    // let oci_worker_handle = oci_worker::start_oci_worker(
    //     db_pool_arc.clone(),
    //     clickhouse_pool_arc.clone(),
    //     encryptor.clone(),
    //     shutdown_rx.clone(),
    // ).await?;
    //
    // let snowflake_worker_handle = snowflake_worker::start_snowflake_worker(
    //     db_pool_arc.clone(),
    //     clickhouse_pool_arc.clone(),
    //     encryptor.clone(),
    //     shutdown_rx.clone(),
    // ).await?;

    let event_worker_flow_url = std::env::var("FLOW_GATEWAY_URL")
        .or_else(|_| std::env::var("FLOW_URL"))
        .unwrap_or_else(|_| "http://localhost:3001".into());
    let event_worker_handle = event_worker::start_event_worker(
        &config.kafka_hosts,
        &config.kafka_platform_events_topic,
        config.kafka_client_id.as_deref(),
        event_worker_flow_url,
        db_pool_arc.clone(),
        redis_pool_arc.clone(),
        shutdown_rx.clone(),
    )
    .await?;

    // Initialize LLM cost calculator
    let cost_calculator = llm::CostCalculator::new(db_pool_arc.clone());
    if let Err(e) = cost_calculator.initialize().await {
        tracing::warn!("Failed to initialize LLM cost calculator: {}", e);
    }
    let llm_processor = Arc::new(llm::LlmSpanProcessor::new(cost_calculator));

    // Initialize GitHub service if configured
    let github_service = github::GitHubService::from_config(&config_arc).map(Arc::new);

    let event_publisher = Arc::new(reiver_core::events::EventPublisher::new(
        kafka_producer_arc.clone(),
        reiver_core::events::EventSource::Watch,
    ));

    // Create WatchState (APM) and start API server
    let entitlements: Arc<dyn reiver_core::entitlements::EntitlementChecker> =
        Arc::new(reiver_core::entitlements::EntitlementService::new(db_pool_arc.clone()));
    let obs_limits = Arc::new(app_state::ObsLimitsCache::new());
    app_state::spawn_obs_limits_refresh_task(
        db_pool_arc.clone(),
        entitlements.clone(),
        obs_limits.clone(),
    );
    let app_state = Arc::new(app_state::WatchState {
        db: db_pool_arc.clone(),
        clickhouse: clickhouse_pool_arc.clone(),
        redis: redis_pool_arc.clone(),
        config: config_arc.clone(),
        kafka: kafka_producer_arc.clone(),
        event_publisher: event_publisher.clone(),
        stats_broadcast,
        llm_processor,
        github_service,
        encryptor: encryptor.clone(),
        http_client: reqwest::Client::new(),
        entitlements,
        obs_limits,
    });

    let app = create_router(app_state);
    let listener = tokio::net::TcpListener::bind(DEFAULT_LISTEN_ADDR).await?;
    tracing::info!("Server listening on http://{}", DEFAULT_LISTEN_ADDR);

    let mut server_handle =
        tokio::spawn(async move { axum::serve(listener, app.into_make_service()).await });

    // Wait for shutdown
    let shutdown_signal = tokio::signal::ctrl_c();
    tokio::pin!(shutdown_signal);

    let shutdown_received = tokio::select! {
        result = &mut server_handle => {
            match result {
                Ok(Ok(())) => tracing::info!("Server stopped normally"),
                Ok(Err(e)) => return Err(anyhow::anyhow!("Server error: {}", e)),
                Err(e) => return Err(anyhow::anyhow!("Server task panicked: {}", e)),
            }
            false
        }
        _ = shutdown_signal => {
            tracing::info!("Received shutdown signal");
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
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(WORKER_SHUTDOWN_TIMEOUT_SECS),
            async {
                let _ = tokio::join!(
                    kafka_consumer_handle,
                    kafka_log_consumer_handle,
                    spans_worker_handle,
                    metrics_worker_handle,
                    alert_worker_handle,
                    aggregation_handle,
                    // aws_worker_handle,
                    // azure_worker_handle,
                    // gcp_worker_handle,
                    // oci_worker_handle,
                    // snowflake_worker_handle,
                    event_worker_handle,
                );
            },
        )
        .await;
        tracing::info!("All workers stopped");
    }

    let _ = server_handle.await;
    Ok(())
}

/// Run only the API server (no workers)
async fn run_api_only(
    config: Config,
    _shutdown_tx: tokio::sync::watch::Sender<bool>,
    _shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (db_pool_arc, clickhouse_pool_arc, redis_pool_arc) = init_core_connections(&config).await?;
    let config_arc = Arc::new(config.clone());

    // Initialize Kafka producer (needed for API to send events)
    let kafka_producer = kafka::KafkaProducer::new(&build_kafka_config(&config))?;
    let kafka_producer_arc = Arc::new(kafka_producer);

    let (stats_broadcast, _) =
        tokio::sync::broadcast::channel::<app_state::StatsUpdateMessage>(STATS_BROADCAST_CAPACITY);

    // Initialize LLM processor for API
    let cost_calculator = llm::CostCalculator::new(db_pool_arc.clone());
    if let Err(e) = cost_calculator.initialize().await {
        tracing::warn!("Failed to initialize LLM cost calculator: {}", e);
    }
    let llm_processor = Arc::new(llm::LlmSpanProcessor::new(cost_calculator));

    // Initialize GitHub service if configured
    let github_service = github::GitHubService::from_config(&config_arc).map(Arc::new);

    let event_publisher = Arc::new(reiver_core::events::EventPublisher::new(
        kafka_producer_arc.clone(),
        reiver_core::events::EventSource::Watch,
    ));

    let encryptor = create_encryptor(&config)?;

    let entitlements: Arc<dyn reiver_core::entitlements::EntitlementChecker> =
        Arc::new(reiver_core::entitlements::EntitlementService::new(db_pool_arc.clone()));
    let obs_limits = Arc::new(app_state::ObsLimitsCache::new());
    app_state::spawn_obs_limits_refresh_task(
        db_pool_arc.clone(),
        entitlements.clone(),
        obs_limits.clone(),
    );
    let app_state = Arc::new(app_state::WatchState {
        db: db_pool_arc,
        clickhouse: clickhouse_pool_arc,
        redis: redis_pool_arc,
        config: config_arc,
        kafka: kafka_producer_arc,
        event_publisher,
        stats_broadcast,
        llm_processor,
        github_service,
        encryptor,
        http_client: reqwest::Client::new(),
        entitlements,
        obs_limits,
    });

    let app = create_router(app_state);
    let listener = tokio::net::TcpListener::bind(DEFAULT_LISTEN_ADDR).await?;
    tracing::info!(
        "API server listening on http://{} (workers disabled)",
        DEFAULT_LISTEN_ADDR
    );

    axum::serve(listener, app.into_make_service()).await?;
    Ok(())
}

/// Run all workers (no API)
async fn run_all_workers(
    config: Config,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (db_pool_arc, clickhouse_pool_arc, redis_pool_arc) = init_core_connections(&config).await?;
    let config_arc = Arc::new(config.clone());

    let (stats_broadcast, _) =
        tokio::sync::broadcast::channel::<app_state::StatsUpdateMessage>(STATS_BROADCAST_CAPACITY);

    // Create secret encryptor for cloud integration workers
    let encryptor = create_encryptor(&config)?;

    // Initialize Kafka producer and event publisher for workers
    let kafka_producer = kafka::KafkaProducer::new(&build_kafka_config(&config))?;
    let kafka_producer_arc = Arc::new(kafka_producer);
    let event_publisher_for_workers = Arc::new(reiver_core::events::EventPublisher::new(
        kafka_producer_arc.clone(),
        reiver_core::events::EventSource::Watch,
    ));

    // Start Watch (APM) workers only
    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    handles.push(
        kafka_consumer::start_kafka_error_consumer(
            &config.kafka_hosts,
            &config.kafka_exceptions_topic,
            config.kafka_client_id.as_deref(),
            db_pool_arc.clone(),
            clickhouse_pool_arc.clone(),
            redis_pool_arc.clone(),
            config_arc.clone(),
            stats_broadcast,
            event_publisher_for_workers.clone(),
            shutdown_rx.clone(),
        )
        .await?,
    );

    handles.push(
        kafka_log_consumer::start_kafka_log_consumer(
            &config.kafka_hosts,
            &config.kafka_logs_otlp_topic,
            &config.kafka_logs_unstructured_topic,
            config.kafka_client_id.as_deref(),
            db_pool_arc.clone(),
            clickhouse_pool_arc.clone(),
            redis_pool_arc.clone(),
            config_arc.clone(),
            shutdown_rx.clone(),
        )
        .await?,
    );

    handles.push(
        spans_worker::start_spans_worker(
            &config.kafka_hosts,
            &config.kafka_spans_topic,
            config.kafka_client_id.as_deref(),
            db_pool_arc.clone(),
            clickhouse_pool_arc.clone(),
            redis_pool_arc.clone(),
            config_arc.clone(),
            kafka_producer_arc.clone(),
            shutdown_rx.clone(),
        )
        .await?,
    );

    handles.push(
        metrics_worker::start_metrics_worker(
            &config.kafka_hosts,
            &config.kafka_metrics_topic,
            config.kafka_client_id.as_deref(),
            db_pool_arc.clone(),
            clickhouse_pool_arc.clone(),
            config_arc.clone(),
            shutdown_rx.clone(),
        )
        .await?,
    );

    handles.push(
        alert_worker::start_alert_worker(
            db_pool_arc.clone(),
            clickhouse_pool_arc.clone(),
            event_publisher_for_workers.clone(),
            shutdown_rx.clone(),
        )
        .await?,
    );

    handles.push(
        aggregation_worker::start_aggregation_worker(
            redis_pool_arc.clone(),
            clickhouse_pool_arc.clone(),
            shutdown_rx.clone(),
        )
        .await?,
    );

    // handles.push(aws_worker::start_aws_worker(
    //     db_pool_arc.clone(),
    //     clickhouse_pool_arc.clone(),
    //     shutdown_rx.clone(),
    // ).await?);
    //
    // handles.push(azure_worker::start_azure_worker(
    //     db_pool_arc.clone(),
    //     clickhouse_pool_arc.clone(),
    //     encryptor.clone(),
    //     shutdown_rx.clone(),
    // ).await?);
    //
    // handles.push(gcp_worker::start_gcp_worker(
    //     db_pool_arc.clone(),
    //     clickhouse_pool_arc.clone(),
    //     encryptor.clone(),
    //     shutdown_rx.clone(),
    // ).await?);
    //
    // handles.push(oci_worker::start_oci_worker(
    //     db_pool_arc.clone(),
    //     clickhouse_pool_arc.clone(),
    //     encryptor.clone(),
    //     shutdown_rx.clone(),
    // ).await?);
    //
    // handles.push(snowflake_worker::start_snowflake_worker(
    //     db_pool_arc.clone(),
    //     clickhouse_pool_arc.clone(),
    //     encryptor.clone(),
    //     shutdown_rx.clone(),
    // ).await?);

    let event_worker_flow_url = std::env::var("FLOW_GATEWAY_URL")
        .or_else(|_| std::env::var("FLOW_URL"))
        .unwrap_or_else(|_| "http://localhost:3001".into());
    handles.push(
        event_worker::start_event_worker(
            &config.kafka_hosts,
            &config.kafka_platform_events_topic,
            config.kafka_client_id.as_deref(),
            event_worker_flow_url,
            db_pool_arc.clone(),
            redis_pool_arc.clone(),
            shutdown_rx.clone(),
        )
        .await?,
    );

    tracing::info!("Watch workers started (API disabled)");
    wait_for_shutdown(shutdown_tx, handles).await;
    Ok(())
}

/// Run only the Kafka exception consumer
async fn run_kafka_consumer(
    config: Config,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (db_pool_arc, clickhouse_pool_arc, redis_pool_arc) = init_core_connections(&config).await?;
    let config_arc = Arc::new(config.clone());
    let (stats_broadcast, _) =
        tokio::sync::broadcast::channel::<app_state::StatsUpdateMessage>(STATS_BROADCAST_CAPACITY);

    let kafka_producer = kafka::KafkaProducer::new(&build_kafka_config(&config))?;
    let event_publisher = Arc::new(reiver_core::events::EventPublisher::new(
        Arc::new(kafka_producer),
        reiver_core::events::EventSource::Watch,
    ));

    let handle = kafka_consumer::start_kafka_error_consumer(
        &config.kafka_hosts,
        &config.kafka_exceptions_topic,
        config.kafka_client_id.as_deref(),
        db_pool_arc,
        clickhouse_pool_arc,
        redis_pool_arc,
        config_arc,
        stats_broadcast,
        event_publisher,
        shutdown_rx,
    )
    .await?;

    tracing::info!("Kafka exception consumer started");
    wait_for_shutdown(shutdown_tx, vec![handle]).await;
    Ok(())
}

/// Run only the Kafka log consumer
async fn run_kafka_log_consumer(
    config: Config,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (db_pool_arc, clickhouse_pool_arc, redis_pool_arc) = init_core_connections(&config).await?;
    let config_arc = Arc::new(config.clone());

    let handle = kafka_log_consumer::start_kafka_log_consumer(
        &config.kafka_hosts,
        &config.kafka_logs_otlp_topic,
        &config.kafka_logs_unstructured_topic,
        config.kafka_client_id.as_deref(),
        db_pool_arc,
        clickhouse_pool_arc,
        redis_pool_arc,
        config_arc,
        shutdown_rx,
    )
    .await?;

    tracing::info!("Kafka log consumer started");
    wait_for_shutdown(shutdown_tx, vec![handle]).await;
    Ok(())
}

/// Run only the alert worker
async fn run_alert_worker(
    config: Config,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (db_pool_arc, clickhouse_pool_arc, _redis_pool_arc) =
        init_core_connections(&config).await?;

    let kafka_producer = kafka::KafkaProducer::new(&build_kafka_config(&config))?;
    let kafka_producer_arc = Arc::new(kafka_producer);
    let event_publisher = Arc::new(reiver_core::events::EventPublisher::new(
        kafka_producer_arc,
        reiver_core::events::EventSource::Watch,
    ));

    let handle = alert_worker::start_alert_worker(
        db_pool_arc,
        clickhouse_pool_arc,
        event_publisher,
        shutdown_rx,
    )
    .await?;

    tracing::info!("Alert worker started");
    wait_for_shutdown(shutdown_tx, vec![handle]).await;
    Ok(())
}

/// Run only the aggregation worker
async fn run_aggregation_worker(
    config: Config,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (_db_pool_arc, clickhouse_pool_arc, redis_pool_arc) =
        init_core_connections(&config).await?;

    let handle = aggregation_worker::start_aggregation_worker(
        redis_pool_arc,
        clickhouse_pool_arc,
        shutdown_rx,
    )
    .await?;

    tracing::info!("Aggregation worker started");
    wait_for_shutdown(shutdown_tx, vec![handle]).await;
    Ok(())
}

/// Run only the AWS worker
async fn run_aws_worker(
    config: Config,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (db_pool_arc, clickhouse_pool_arc, _redis_pool_arc) =
        init_core_connections(&config).await?;

    // let handle = aws_worker::start_aws_worker(
    //     db_pool_arc,
    //     clickhouse_pool_arc,
    //     shutdown_rx,
    // ).await?;

    tracing::info!("AWS worker started");
    //wait_for_shutdown(shutdown_tx, vec![handle]).await;
    Ok(())
}

/// Run only the Azure worker
async fn run_azure_worker(
    config: Config,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (db_pool_arc, clickhouse_pool_arc, _redis_pool_arc) =
        init_core_connections(&config).await?;
    let encryptor = create_encryptor(&config)?;

    // let handle = azure_worker::start_azure_worker(
    //     db_pool_arc,
    //     clickhouse_pool_arc,
    //     encryptor,
    //     shutdown_rx,
    // ).await?;
    //
    // tracing::info!("Azure worker started");
    // wait_for_shutdown(shutdown_tx, vec![handle]).await;
    Ok(())
}

/// Run only the GCP worker
async fn run_gcp_worker(
    config: Config,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (db_pool_arc, clickhouse_pool_arc, _redis_pool_arc) =
        init_core_connections(&config).await?;
    let encryptor = create_encryptor(&config)?;

    // let handle = gcp_worker::start_gcp_worker(
    //     db_pool_arc,
    //     clickhouse_pool_arc,
    //     encryptor,
    //     shutdown_rx,
    // ).await?;
    //
    // tracing::info!("GCP worker started");
    // wait_for_shutdown(shutdown_tx, vec![handle]).await;
    Ok(())
}

/// Run only the OCI worker
async fn run_oci_worker(
    config: Config,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (db_pool_arc, clickhouse_pool_arc, _redis_pool_arc) =
        init_core_connections(&config).await?;
    let encryptor = create_encryptor(&config)?;

    // let handle = oci_worker::start_oci_worker(
    //     db_pool_arc,
    //     clickhouse_pool_arc,
    //     encryptor,
    //     shutdown_rx,
    // ).await?;
    //
    // tracing::info!("OCI worker started");
    // wait_for_shutdown(shutdown_tx, vec![handle]).await;
    Ok(())
}

/// Run only the Snowflake worker
async fn run_snowflake_worker(
    config: Config,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (db_pool_arc, clickhouse_pool_arc, _redis_pool_arc) =
        init_core_connections(&config).await?;
    let encryptor = create_encryptor(&config)?;

    // let handle = snowflake_worker::start_snowflake_worker(
    //     db_pool_arc,
    //     clickhouse_pool_arc,
    //     encryptor,
    //     shutdown_rx,
    // ).await?;
    //
    // tracing::info!("Snowflake worker started");
    // wait_for_shutdown(shutdown_tx, vec![handle]).await;
    Ok(())
}

// /// Run only the pricing sync worker (disabled — pricing_sync removed from core)
// async fn run_pricing_worker(
//     config: Config,
//     shutdown_tx: tokio::sync::watch::Sender<bool>,
//     shutdown_rx: tokio::sync::watch::Receiver<bool>,
// ) -> anyhow::Result<()> {
//     ...
// }

/// Returns true for OTLP ingestion paths that must NOT be traced to avoid
/// recursion (Watch exports its own traces to these same endpoints).
fn is_otlp_ingestion_path(path: &str) -> bool {
    matches!(
        path,
        "/api/v1/traces" | "/api/v1/metrics" | "/api/v1/logs" | "/api/v1/profiles"
    )
}

fn create_router(app_state: Arc<app_state::WatchState>) -> Router {
    Router::new()
        .route("/health", get(health_check))
        .nest(
            "/api",
            api::create_watch_api_router(&app_state.config).with_state(app_state.clone()),
        )
        .nest(
            "/api/projects",
            api::create_watch_projects_router(&app_state.config).with_state(app_state.clone()),
        )
        .nest(
            "/api/projects",
            api::dashboards::create_dashboards_router().with_state(app_state.clone()),
        )
        .layer(Extension(app_state.db.clone()))
        .layer(Extension(app_state.clickhouse.clone()))
        .layer(Extension(app_state.config.clone()))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &axum::http::Request<_>| {
                    let path = request.uri().path();

                    if is_otlp_ingestion_path(path) || path == "/health" {
                        return tracing::Span::none();
                    }

                    let method = request.method().as_str();
                    let span_name = format!("{method} {path}");

                    // NOTE: every field that may be `.record()`ed on response
                    // must be declared here with `Empty`; otherwise the record
                    // is silently dropped.
                    tracing::info_span!(
                        "http.request",
                        otel.name = %span_name,
                        otel.kind = "server",
                        otel.status_code = tracing::field::Empty,
                        otel.status_message = tracing::field::Empty,
                        http.method = %request.method(),
                        http.route = %path,
                        http.target = %request.uri(),
                        http.status_code = tracing::field::Empty,
                        error.kind = tracing::field::Empty,
                        error.message = tracing::field::Empty,
                    )
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
                        // canonical reason so the span carries the actual reason.
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
}

/// Health check endpoint
async fn health_check() -> &'static str {
    "OK"
}

fn build_kafka_config(config: &Config) -> kafka::KafkaProducerConfig {
    kafka::KafkaProducerConfig {
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
    }
}
