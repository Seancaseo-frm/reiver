//! Herd -- Reiver A2A Agent Registry and Message Hub
//!
//! Runs the A2A registry API and message delivery workers.

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use reiver_herd::clickhouse_db::ClickHousePool;
use reiver_herd::{
    access_cache, api, app_state, clickhouse_db, db, kafka, routing_cache, telemetry,
};

use axum::{extract::State, http::StatusCode, routing::get, Router};
use clap::{Parser, ValueEnum};
use reiver_herd::config::Config;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tower_http::trace::TraceLayer;

const DEFAULT_LISTEN_ADDR: &str = "0.0.0.0:3003";
const WORKER_SHUTDOWN_TIMEOUT_SECS: u64 = 30;

#[derive(Parser, Debug)]
#[command(name = "reiver-herd")]
#[command(about = "Reiver A2A agent registry and message hub")]
struct Cli {
    #[arg(long, value_enum, default_value = "all")]
    mode: HerdMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum HerdMode {
    All,
    Api,
    Workers,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

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

    let website_url =
        std::env::var("WEBSITE_URL").unwrap_or_else(|_| "http://localhost:80".to_string());

    let herd_enabled = std::env::var("HERD_ENABLED")
        .map(|v| v != "false" && v != "0")
        .unwrap_or(true);

    if !herd_enabled {
        tracing::warn!("HERD_ENABLED=false — Herd service is disabled, exiting");
        return Ok(());
    }

    let cli = Cli::parse();
    tracing::info!("Starting reiver-herd in {:?} mode", cli.mode);

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let result = match cli.mode {
        HerdMode::All => run_all(config, herd_enabled, website_url, shutdown_tx, shutdown_rx).await,
        HerdMode::Api => run_api_only(config, herd_enabled, website_url).await,
        HerdMode::Workers => run_workers_only(config, shutdown_tx, shutdown_rx).await,
    };

    if let Some(provider) = telemetry_providers.tracer {
        let _ = provider.shutdown();
    }
    if let Some(provider) = telemetry_providers.meter {
        let _ = provider.shutdown();
    }
    if let Some(provider) = telemetry_providers.logger {
        let _ = provider.shutdown();
    }

    result
}

const REQUIRED_TABLES: &[&str] = &["a2a_agents", "a2a_access_grants", "a2a_push_configs"];

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
                 The `website` service must run its database migrations before `herd` starts.",
                table
            );
        }
    }
    tracing::info!("All required database tables verified");
    Ok(())
}

async fn init_connections(config: &Config) -> anyhow::Result<(Arc<db::DbPool>, ClickHousePool)> {
    db::ensure_database_exists(&config.database_url).await?;
    let db_pool = db::create_pool(&config.database_url).await?;
    validate_required_tables(&db_pool).await?;

    tracing::info!("Connecting to ClickHouse at {}", config.clickhouse_url);
    let clickhouse_pool = clickhouse_db::create_clickhouse_pool(&config.clickhouse_url)?;

    Ok((Arc::new(db_pool), clickhouse_pool))
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
    Ok(Arc::new(kafka::KafkaProducer::new(&kafka_config)?))
}

async fn create_herd_state(
    config_arc: Arc<Config>,
    db_pool_arc: Arc<db::DbPool>,
    clickhouse_pool: ClickHousePool,
    kafka_producer_arc: Arc<kafka::KafkaProducer>,
    herd_enabled: bool,
    website_url: String,
) -> Arc<app_state::HerdState> {
    let access_cache = access_cache::AccessCache::load_from_db(db_pool_arc.as_ref())
        .await
        .unwrap_or_else(|e| {
            tracing::warn!("Failed to warm access cache from DB: {e}, starting empty");
            access_cache::AccessCache::new()
        });

    let routing_cache = Arc::new(
        routing_cache::RoutingCache::load_from_db(db_pool_arc.as_ref())
            .await
            .unwrap_or_else(|e| {
                tracing::warn!("Failed to warm routing cache from DB: {e}, starting empty");
                routing_cache::RoutingCache::new()
            }),
    );

    Arc::new(app_state::HerdState {
        db: db_pool_arc,
        clickhouse: clickhouse_pool,
        kafka: kafka_producer_arc,
        config: config_arc,
        http_client: reqwest::Client::builder()
            .pool_max_idle_per_host(32)
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to build HTTP client"),
        herd_enabled,
        website_url,
        access_cache,
        routing_cache,
    })
}

fn create_herd_router(herd_state: Arc<app_state::HerdState>) -> Router {
    let herd_api = api::create_herd_api_router();
    let a2a_jsonrpc = api::messages::router();

    Router::new()
        .route("/health", get(health_check))
        .route("/ready", get(readiness_check))
        .route("/.well-known/agent.json", get(well_known_agent_card))
        .nest("/api/herd", herd_api.clone().with_state(herd_state.clone()))
        .merge(a2a_jsonrpc.with_state(herd_state.clone()))
        .layer(
            TraceLayer::new_for_http().make_span_with(|request: &axum::http::Request<_>| {
                let method = request.method().as_str();
                let path = request.uri().path();
                let span_name = format!("{method} {path}");
                let is_probe = matches!(path, "/health" | "/ready");

                if is_probe {
                    tracing::debug_span!(
                        "http.request",
                        otel.name = %span_name,
                        otel.kind = "server",
                        http.method = %request.method(),
                        http.route = %request.uri().path(),
                        http.status_code = tracing::field::Empty,
                    )
                } else {
                    tracing::info_span!(
                        "http.request",
                        otel.name = %span_name,
                        otel.kind = "server",
                        http.method = %request.method(),
                        http.route = %request.uri().path(),
                        http.status_code = tracing::field::Empty,
                    )
                }
            }),
        )
        .with_state(herd_state)
}

async fn run_all(
    config: Config,
    herd_enabled: bool,
    website_url: String,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (db_pool_arc, clickhouse_pool) = init_connections(&config).await?;
    let config_arc = Arc::new(config.clone());
    let kafka_producer_arc = create_kafka_producer(&config)?;

    let herd_state = create_herd_state(
        config_arc.clone(),
        db_pool_arc.clone(),
        clickhouse_pool.clone(),
        kafka_producer_arc.clone(),
        herd_enabled,
        website_url,
    )
    .await;

    // Start workers
    let message_worker = reiver_herd::worker::message_worker::start_message_worker(
        &config.kafka_hosts,
        config.kafka_client_id.as_deref(),
        herd_state.routing_cache.clone(),
        clickhouse_pool,
        kafka_producer_arc.clone(),
        herd_state.http_client.clone(),
        shutdown_rx.clone(),
    );

    let push_worker = reiver_herd::worker::push_worker::start_push_worker(
        &config.kafka_hosts,
        config.kafka_client_id.as_deref(),
        kafka_producer_arc,
        herd_state.http_client.clone(),
        shutdown_rx.clone(),
    );

    let app = create_herd_router(herd_state);
    let listener = tokio::net::TcpListener::bind(DEFAULT_LISTEN_ADDR).await?;
    tracing::info!("Herd server listening on http://{}", DEFAULT_LISTEN_ADDR);

    let shutdown_tx_clone = shutdown_tx.clone();
    let server_handle = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            let _ = shutdown_tx_clone.send(true);
        })
        .await
    });

    match server_handle.await {
        Ok(Ok(())) => tracing::info!("Server stopped normally"),
        Ok(Err(e)) => tracing::error!("Server error: {}", e),
        Err(e) => tracing::error!("Server task panicked: {}", e),
    }

    tracing::info!(
        "Waiting for workers to finish (timeout: {}s)...",
        WORKER_SHUTDOWN_TIMEOUT_SECS
    );
    match tokio::time::timeout(Duration::from_secs(WORKER_SHUTDOWN_TIMEOUT_SECS), async {
        let _ = tokio::join!(message_worker, push_worker);
    })
    .await
    {
        Ok(_) => tracing::info!("All Herd workers stopped"),
        Err(_) => tracing::warn!(
            "Worker shutdown timed out after {}s",
            WORKER_SHUTDOWN_TIMEOUT_SECS
        ),
    }

    Ok(())
}

async fn run_api_only(
    config: Config,
    herd_enabled: bool,
    website_url: String,
) -> anyhow::Result<()> {
    let (db_pool_arc, clickhouse_pool) = init_connections(&config).await?;
    let config_arc = Arc::new(config.clone());
    let kafka_producer_arc = create_kafka_producer(&config)?;

    let herd_state = create_herd_state(
        config_arc,
        db_pool_arc,
        clickhouse_pool,
        kafka_producer_arc,
        herd_enabled,
        website_url,
    )
    .await;

    let app = create_herd_router(herd_state);
    let listener = tokio::net::TcpListener::bind(DEFAULT_LISTEN_ADDR).await?;
    tracing::info!(
        "Herd API server listening on http://{} (workers disabled)",
        DEFAULT_LISTEN_ADDR
    );

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;

    tracing::info!("Herd API server stopped");
    Ok(())
}

async fn run_workers_only(
    config: Config,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (db_pool_arc, clickhouse_pool) = init_connections(&config).await?;
    let kafka_producer_arc = create_kafka_producer(&config)?;

    let http_client = reqwest::Client::builder()
        .pool_max_idle_per_host(32)
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(30))
        .build()
        .expect("Failed to build HTTP client");

    let routing_cache = Arc::new(
        routing_cache::RoutingCache::load_from_db(db_pool_arc.as_ref())
            .await
            .unwrap_or_else(|e| {
                tracing::warn!("Failed to warm routing cache from DB: {e}, starting empty");
                routing_cache::RoutingCache::new()
            }),
    );

    let message_worker = reiver_herd::worker::message_worker::start_message_worker(
        &config.kafka_hosts,
        config.kafka_client_id.as_deref(),
        routing_cache,
        clickhouse_pool,
        kafka_producer_arc.clone(),
        http_client.clone(),
        shutdown_rx.clone(),
    );

    let push_worker = reiver_herd::worker::push_worker::start_push_worker(
        &config.kafka_hosts,
        config.kafka_client_id.as_deref(),
        kafka_producer_arc,
        http_client,
        shutdown_rx,
    );

    tracing::info!("Herd workers started (API disabled)");

    shutdown_signal().await;
    let _ = shutdown_tx.send(true);

    tracing::info!(
        "Waiting for workers to finish (timeout: {}s)...",
        WORKER_SHUTDOWN_TIMEOUT_SECS
    );
    match tokio::time::timeout(Duration::from_secs(WORKER_SHUTDOWN_TIMEOUT_SECS), async {
        let _ = tokio::join!(message_worker, push_worker);
    })
    .await
    {
        Ok(_) => tracing::info!("All Herd workers stopped"),
        Err(_) => tracing::warn!(
            "Worker shutdown timed out after {}s",
            WORKER_SHUTDOWN_TIMEOUT_SECS
        ),
    }

    Ok(())
}

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

async fn health_check() -> &'static str {
    "OK"
}

async fn well_known_agent_card() -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({
        "name": "Reiver Herd",
        "description": "A2A agent gateway for the Reiver platform",
        "url": "https://reiver.io/a2a",
        "version": "1.0.0",
        "capabilities": {
            "pushNotifications": true,
            "stateTransitionHistory": true,
        },
        "defaultInputModes": ["application/json", "text/plain"],
        "defaultOutputModes": ["application/json", "text/plain"],
        "skills": []
    }))
}

async fn readiness_check(
    State(state): State<Arc<app_state::HerdState>>,
) -> (StatusCode, axum::Json<serde_json::Value>) {
    let mut checks: Vec<(&str, bool, String)> = Vec::new();

    let pg_result = sqlx::query("SELECT 1").fetch_one(state.db.as_ref()).await;
    let pg_ok = pg_result.is_ok();
    if let Err(ref e) = pg_result {
        tracing::warn!("Readiness check: Postgres unreachable: {}", e);
    }
    checks.push((
        "postgres",
        pg_ok,
        if pg_ok {
            "ok".into()
        } else {
            "unreachable".into()
        },
    ));

    let ch_result = state.clickhouse.query("SELECT 1").fetch_one::<u8>().await;
    let ch_ok = ch_result.is_ok();
    if let Err(ref e) = ch_result {
        tracing::warn!("Readiness check: ClickHouse unreachable: {}", e);
    }
    checks.push((
        "clickhouse",
        ch_ok,
        if ch_ok {
            "ok".into()
        } else {
            "unreachable".into()
        },
    ));

    let kafka_ok = state.kafka.is_healthy();
    if !kafka_ok {
        tracing::warn!("Readiness check: Kafka unreachable");
    }
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
