// Use mimalloc as the global allocator for better multi-threaded performance
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use reiver_website::{
    api,
    app_state,
    auth,
    // Workers
    auth_event_worker,
    auth_routes,
    billing,
    billing_worker,
    clickhouse_db,
    crypto,
    db,
    intern,
    proxy,
    sso_worker,
};

use axum::http::{header, HeaderValue, Method};
use axum::middleware::Next;
use axum::{extract::Extension, middleware, routing::get, Router};
use clap::{Parser, ValueEnum};
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};
use tower_http::services::{ServeDir, ServeFile};
use tracing_subscriber;

use bb8_redis::redis::AsyncCommands;
use reiver_website::config::Config;

fn parse_moodeng_project_id() -> Option<uuid::Uuid> {
    std::env::var("MOODENG_PROJECT_ID")
        .ok()
        .and_then(|s| uuid::Uuid::parse_str(&s).ok())
}

fn quickstart_routes<S>(index_html: &str) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new()
        .route_service("/quickstart", ServeFile::new(index_html))
        .route_service("/quickstart/", ServeFile::new(index_html))
}

/// Reiver Website -- auth/identity backend + frontend
#[derive(Parser, Debug)]
#[command(name = "reiver-website")]
#[command(about = "Reiver website: auth, identity, billing, and frontend")]
struct Cli {
    /// Which mode to run in
    #[arg(long, value_enum, default_value = "all")]
    mode: WorkerMode,
}

/// Available worker modes
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum WorkerMode {
    /// Run everything (API + all workers) - default for development
    All,
    /// Run only the HTTP API server (no workers)
    Api,
    /// Run all workers (no API)
    Workers,
    /// Run only the billing worker
    BillingWorker,
    /// Run only the auth event worker
    AuthEventWorker,
    /// Run only the SSO worker
    SsoWorker,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    tracing::info!("Starting reiver-website in {:?} mode", cli.mode);

    let config = Config::from_env()?;

    // Validate JWT secret meets security requirements
    if let Err(e) = auth::validate_jwt_secret(&config.jwt_secret) {
        tracing::error!("JWT secret validation failed: {}", e);
        return Err(anyhow::anyhow!("Security configuration error: {}", e));
    }

    // Create shutdown signal channel for graceful worker shutdown
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    match cli.mode {
        WorkerMode::All => run_all(config, shutdown_tx, shutdown_rx).await,
        WorkerMode::Api => run_api_only(config, shutdown_tx, shutdown_rx).await,
        WorkerMode::Workers => run_all_workers(config, shutdown_tx, shutdown_rx).await,
        WorkerMode::BillingWorker => run_billing_worker(config, shutdown_tx, shutdown_rx).await,
        WorkerMode::AuthEventWorker => {
            run_auth_event_worker_mode(config, shutdown_tx, shutdown_rx).await
        }
        WorkerMode::SsoWorker => run_sso_worker_mode(config, shutdown_tx, shutdown_rx).await,
    }
}

/// Initialize core database connections (PostgreSQL, Redis)
async fn init_core_connections(
    config: &Config,
) -> anyhow::Result<(Arc<db::DbPool>, Arc<app_state::RedisPool>)> {
    // Ensure database exists before connecting
    db::ensure_database_exists(&config.database_url).await?;

    let db_pool = db::create_pool(&config.database_url).await?;

    // Run all PostgreSQL migrations (shared database for all services)
    tracing::info!("Running PostgreSQL migrations...");
    sqlx::migrate!("./migrations").run(&db_pool).await?;
    tracing::info!("PostgreSQL migrations completed successfully");

    // Run all ClickHouse migrations (shared database for all services)
    tracing::info!("Running ClickHouse migrations...");
    {
        mod ch_migrations {
            refinery::embed_migrations!("./clickhouse_migrations");
        }
        let mut ch_client = clickhouse_db::connect_for_migrations(&config.clickhouse_url).await?;
        let mut runner = ch_migrations::migrations::runner();
        runner.set_migration_table_name("refinery_schema_history");
        runner
            .run_async(&mut ch_client)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to run ClickHouse migrations: {}", e))?;
    }
    tracing::info!("ClickHouse migrations completed successfully");

    // Create Redis connection pool
    tracing::info!("Connecting to Redis at {}", config.redis_url);
    let manager = bb8_redis::RedisConnectionManager::new(config.redis_url.clone())
        .map_err(|e| anyhow::anyhow!("Failed to create Redis connection manager: {}", e))?;

    let redis_pool = bb8::Pool::builder()
        .max_size(15)
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
    }
    tracing::info!("Redis connection pool established successfully");

    Ok((Arc::new(db_pool), Arc::new(redis_pool)))
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

        let enc = crypto::RotatingSecretEncryptor::new(key, fallback_refs)?;
        if enc.fallback_key_count() > 0 {
            tracing::info!(
                fallback_keys = enc.fallback_key_count(),
                "Key rotation active — encrypting with new key, decrypting with new + old"
            );
        }
        Ok(Arc::new(enc))
    } else if is_production {
        Err(anyhow::anyhow!("ENCRYPTION_KEY is required in production"))
    } else {
        tracing::warn!("No ENCRYPTION_KEY set -- using temporary key (development only)");
        let temp_key = crypto::SecretEncryptor::generate_key();
        Ok(Arc::new(
            crypto::RotatingSecretEncryptor::single_key(&temp_key)
                .expect("Generated key should be valid"),
        ))
    }
}

/// Create WebsiteState
async fn create_website_state(
    config: &Config,
    db_pool: Arc<db::DbPool>,
    redis_pool: Arc<app_state::RedisPool>,
) -> anyhow::Result<Arc<app_state::WebsiteState>> {
    let encryptor = create_encryptor(config)?;

    // Create ClickHouse connection (for billing usage queries)
    let clickhouse_pool = clickhouse_db::create_clickhouse_pool(&config.clickhouse_url)?;
    let clickhouse_pool = Arc::new(clickhouse_pool);

    let entitlement_service = reiver_core::entitlements::EntitlementService::new(db_pool.clone());
    let entitlements_arc: Arc<dyn reiver_core::entitlements::EntitlementChecker> =
        Arc::new(entitlement_service);

    let moodeng_project_id: Option<uuid::Uuid> = std::env::var("MOODENG_PROJECT_ID")
        .ok()
        .and_then(|s| uuid::Uuid::parse_str(&s).ok());
    if moodeng_project_id.is_none() {
        tracing::error!("MOODENG_PROJECT_ID not set: MooDeng fee splitting will not work, all gateway traffic will use gateway_fee_percent");
    }

    // Create billing service
    let billing_service = billing::BillingService::new(
        db_pool.clone(),
        clickhouse_pool.clone(),
        entitlements_arc.clone(),
        moodeng_project_id,
    );

    // Pond disabled — re-enable when Pond launches
    // let pond_url = std::env::var("POND_URL")
    //     .unwrap_or_else(|_| "http://localhost:3002".to_string());
    let flow_url =
        std::env::var("FLOW_URL").unwrap_or_else(|_| "http://localhost:3001".to_string());
    let watch_url =
        std::env::var("WATCH_URL").unwrap_or_else(|_| "http://localhost:3000".to_string());
    let mcp_url = std::env::var("MCP_URL").unwrap_or_else(|_| "http://localhost:3002".to_string());
    let herd_url =
        std::env::var("HERD_URL").unwrap_or_else(|_| "http://localhost:3003".to_string());

    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(300)) // 5 min for long queries
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create HTTP client: {}", e))?;

    let credit_service = Arc::new(reiver_core::billing::credits::CreditService::new(
        db_pool.clone(),
        redis_pool.clone(),
    ));

    let email_client = match (
        &config.loops_api_key,
        &config.loops_invite_template_id,
        &config.loops_alert_template_id,
        &config.loops_welcome_template_id,
    ) {
        (Some(api_key), Some(invite_id), Some(alert_id), Some(welcome_id)) => {
            tracing::info!("Loops email client configured");
            Some(Arc::new(reiver_core::email::LoopsClient::new(
                api_key.clone(),
                invite_id.clone(),
                alert_id.clone(),
                welcome_id.clone(),
            )))
        }
        _ => {
            tracing::info!("Loops email client disabled (LOOPS_API_KEY not set)");
            None
        }
    };

    let stripe_client = config
        .stripe_api_key
        .as_ref()
        .map(|key| stripe::Client::new(key));

    let state = app_state::WebsiteState {
        db: db_pool,
        clickhouse: clickhouse_pool,
        redis: redis_pool,
        config: Arc::new(config.clone()),
        encryptor,
        billing: Arc::new(billing_service),
        credit_service,
        http_client,
        email: email_client,
        stripe_client,
        // pond_url, // Pond disabled — re-enable when Pond launches
        flow_url,
        watch_url,
        mcp_url,
        herd_url,
        entitlements: entitlements_arc,
        kb_embedder: Arc::new(
            reiver_core::embeddings::KbEmbedder::new()
                .expect("Failed to initialize knowledge base embedding model"),
        ),
    };

    Ok(Arc::new(state))
}

async fn run_all(
    config: Config,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (db_pool, redis_pool) = init_core_connections(&config).await?;
    let state = create_website_state(&config, db_pool.clone(), redis_pool.clone()).await?;

    // Start workers
    let _billing_handle = {
        let state = state.clone();
        let rx = shutdown_rx.clone();
        tokio::spawn(async move {
            let moodeng_pid = parse_moodeng_project_id();
            let _ = billing_worker::start_billing_worker(
                state.db.clone(),
                state.clickhouse.clone(),
                state.redis.clone(),
                state.entitlements.clone(),
                moodeng_pid,
                state.config.clone(),
                rx,
            )
            .await;
        })
    };

    // Start API server
    run_api_server(state, &config).await
}

async fn run_api_only(
    config: Config,
    _shutdown_tx: tokio::sync::watch::Sender<bool>,
    _shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (db_pool, redis_pool) = init_core_connections(&config).await?;
    let state = create_website_state(&config, db_pool, redis_pool).await?;
    run_api_server(state, &config).await
}

async fn run_all_workers(
    config: Config,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (db_pool, redis_pool) = init_core_connections(&config).await?;
    let state = create_website_state(&config, db_pool, redis_pool).await?;

    let _billing_handle = {
        let state = state.clone();
        let rx = shutdown_rx.clone();
        tokio::spawn(async move {
            let moodeng_pid = parse_moodeng_project_id();
            let _ = billing_worker::start_billing_worker(
                state.db.clone(),
                state.clickhouse.clone(),
                state.redis.clone(),
                state.entitlements.clone(),
                moodeng_pid,
                state.config.clone(),
                rx,
            )
            .await;
        })
    };

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    tracing::info!("Shutdown signal received, stopping workers...");
    let _ = shutdown_tx.send(true);

    Ok(())
}

async fn run_billing_worker(
    config: Config,
    _shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (db_pool, redis_pool) = init_core_connections(&config).await?;
    let state = create_website_state(&config, db_pool, redis_pool).await?;

    let moodeng_pid = parse_moodeng_project_id();
    let _ = billing_worker::start_billing_worker(
        state.db.clone(),
        state.clickhouse.clone(),
        state.redis.clone(),
        state.entitlements.clone(),
        moodeng_pid,
        state.config.clone(),
        shutdown_rx,
    )
    .await;

    Ok(())
}

async fn run_auth_event_worker_mode(
    config: Config,
    _shutdown_tx: tokio::sync::watch::Sender<bool>,
    _shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (db_pool, redis_pool) = init_core_connections(&config).await?;
    let _state = create_website_state(&config, db_pool, redis_pool).await?;

    // TODO: Start auth event worker
    tokio::signal::ctrl_c().await?;
    Ok(())
}

async fn run_sso_worker_mode(
    config: Config,
    _shutdown_tx: tokio::sync::watch::Sender<bool>,
    _shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (db_pool, redis_pool) = init_core_connections(&config).await?;
    let _state = create_website_state(&config, db_pool, redis_pool).await?;

    // TODO: Start SSO worker
    tokio::signal::ctrl_c().await?;
    Ok(())
}

async fn security_headers(request: axum::extract::Request, next: Next) -> axum::response::Response {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    headers.insert("x-frame-options", HeaderValue::from_static("DENY"));
    headers.insert(
        "strict-transport-security",
        HeaderValue::from_static("max-age=31536000; includeSubDomains"),
    );
    headers.insert(
        "referrer-policy",
        HeaderValue::from_static("strict-origin-when-cross-origin"),
    );
    headers.insert(
        "permissions-policy",
        HeaderValue::from_static("camera=(), microphone=(), geolocation=()"),
    );
    headers.insert(
        "content-security-policy",
        HeaderValue::from_static(
            "default-src 'self'; \
             script-src 'self' 'unsafe-eval' 'unsafe-inline' https://www.googletagmanager.com; \
             style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; \
             font-src 'self' https://fonts.gstatic.com; \
             img-src 'self' data: blob: https://www.googletagmanager.com; \
             connect-src 'self' https://*.google-analytics.com https://*.analytics.google.com https://*.googletagmanager.com; \
             frame-ancestors 'none'",
        ),
    );
    response
}

async fn run_api_server(
    state: Arc<app_state::WebsiteState>,
    config: &Config,
) -> anyhow::Result<()> {
    let cors = {
        let is_wildcard = config.cors_allowed_origins.iter().any(|o| o == "*");

        let layer = CorsLayer::new()
            .allow_methods([
                Method::GET,
                Method::POST,
                Method::PUT,
                Method::PATCH,
                Method::DELETE,
                Method::OPTIONS,
            ])
            .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION, header::ACCEPT]);

        let layer = if is_wildcard {
            layer.allow_origin(Any)
        } else {
            let origins: Vec<HeaderValue> = config
                .cors_allowed_origins
                .iter()
                .filter_map(|o| o.parse().ok())
                .collect();
            layer.allow_origin(origins)
        };

        // allow_credentials(true) is incompatible with allow_origin(Any) per the
        // CORS spec — browsers reject the response. Only set it for explicit origins.
        if config.cors_allow_credentials && !is_wildcard {
            layer.allow_credentials(true)
        } else {
            layer
        }
    };

    // Create API router
    let api_router = api::create_website_api_router();

    // Pond proxy: disabled — re-enable when Pond launches
    // let pond_proxy = Router::new()
    //     .route(
    //         "/api/projects/{project_id}/warehouse/{*rest}",
    //         axum::routing::any(proxy::proxy_to_pond),
    //     )
    //     .route(
    //         "/api/projects/{project_id}/catalog/{*rest}",
    //         axum::routing::any(proxy::proxy_to_pond),
    //     );

    // Flow proxy: forward LLM management and gateway requests to the flow backend.
    // The project-scoped route handles `/api/projects/{id}/llm/*` and the
    // catch-all handles `/api/llm/*` (where project_id is in the path segment
    // or query parameter).
    //
    // `/api/projects/{id}/gateway/v1/*` is the session-authenticated gateway path
    // used by the internal playground UI. It strips the project prefix and forwards
    // to the same flow gateway as the API-key path, but authenticates via JWT session
    // rather than requiring a project API key.
    let flow_proxy = Router::new()
        .route(
            "/api/projects/{project_id}/llm/{*rest}",
            axum::routing::any(proxy::proxy_to_flow),
        )
        .route(
            "/api/projects/{project_id}/gateway/v1/{*rest}",
            axum::routing::any(proxy::proxy_to_flow),
        )
        .route(
            "/api/projects/{project_id}/agent/{*rest}",
            axum::routing::any(proxy::proxy_to_flow),
        )
        .route(
            "/api/projects/{project_id}/secrets/{*rest}",
            axum::routing::any(proxy::proxy_to_flow),
        )
        .route(
            "/api/llm/{*rest}",
            axum::routing::any(proxy::proxy_to_flow_llm),
        )
        .route(
            "/api/gateway/v1/{*rest}",
            axum::routing::any(proxy::proxy_to_flow_gateway),
        );

    // Watch proxy: forward APM requests to the watch backend.
    // Uses nested router with explicit route for ingestion (API key auth)
    // and fallback for management (JWT auth). GitHub webhooks get passthrough.
    let watch_proxy = Router::new().nest(
        "/api/watch",
        Router::new()
            .route(
                "/ingest/{*rest}",
                axum::routing::any(proxy::proxy_to_watch_ingest),
            )
            .route(
                "/github/webhook",
                axum::routing::post(proxy::proxy_to_watch_passthrough),
            )
            .fallback(axum::routing::any(proxy::proxy_to_watch))
            .with_state(state.clone()),
    );

    // Watch integration proxy: forward integration endpoints (slack, discord, etc.)
    // to the Watch backend. These endpoints live directly under /api/{provider}/...
    // without a /api/watch/ prefix.
    let watch_integration_proxy = Router::new()
        // Slack OAuth: install, callback, events are unauthenticated passthroughs.
        // Install must 302 to slack.com (Slack App Directory requirement).
        // Callback uses CSRF state. Events uses request signature verification.
        .route(
            "/api/slack/oauth/install",
            axum::routing::get(proxy::proxy_to_watch_slack_passthrough),
        )
        .route(
            "/api/slack/oauth/callback",
            axum::routing::get(proxy::proxy_to_watch_slack_passthrough),
        )
        .route(
            "/api/slack/events",
            axum::routing::post(proxy::proxy_to_watch_slack_passthrough),
        )
        .route(
            "/api/slack/interactivity",
            axum::routing::post(proxy::proxy_to_watch_slack_passthrough),
        )
        .route(
            "/api/slack/{*rest}",
            axum::routing::any(proxy::proxy_to_watch_integration),
        )
        .route(
            "/api/pagerduty/{*rest}",
            axum::routing::any(proxy::proxy_to_watch_integration),
        )
        .route(
            "/api/teams/{*rest}",
            axum::routing::any(proxy::proxy_to_watch_integration),
        )
        .route(
            "/api/discord/{*rest}",
            axum::routing::any(proxy::proxy_to_watch_integration),
        )
        .route(
            "/api/servicenow/{*rest}",
            axum::routing::any(proxy::proxy_to_watch_integration),
        )
        .route(
            "/api/aws/{*rest}",
            axum::routing::any(proxy::proxy_to_watch_integration),
        )
        .route(
            "/api/github/installations",
            axum::routing::any(proxy::proxy_to_watch_integration),
        )
        .route(
            "/api/github/installations/{*rest}",
            axum::routing::any(proxy::proxy_to_watch_integration),
        )
        .route(
            "/api/github/install",
            axum::routing::any(proxy::proxy_to_watch_integration),
        )
        .route(
            "/api/github/callback",
            axum::routing::get(proxy::proxy_to_watch_integration),
        )
        .route(
            "/api/health-checks/{*rest}",
            axum::routing::any(proxy::proxy_to_watch_integration),
        );

    // Herd (A2A Agent Registry) proxy routes
    let herd_routes = Router::new()
        .route(
            "/api/projects/{project_id}/herd/{*rest}",
            axum::routing::any(proxy::proxy_to_herd),
        )
        .route("/a2a", axum::routing::post(proxy::proxy_to_herd_a2a))
        .with_state(state.clone());

    // Watch direct routes: frontend calls these paths directly for APM data.
    // Each path is forwarded as-is to the Watch backend (path already includes /api/).
    let watch_direct = Router::new()
        .route(
            "/api/projects/{project_id}/services",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/projects/{project_id}/services/{*rest}",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/projects/{project_id}/traces",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/projects/{project_id}/traces/{*rest}",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/projects/{project_id}/events",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/projects/{project_id}/events/{*rest}",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/projects/{project_id}/logs/{*rest}",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/projects/{project_id}/exceptions",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/projects/{project_id}/exceptions/{*rest}",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/projects/{project_id}/incidents/{*rest}",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/projects/{project_id}/metrics/{*rest}",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/projects/{project_id}/metric-names",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/projects/{project_id}/health-checks",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/projects/{project_id}/health-checks/{*rest}",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/projects/{project_id}/alerts",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/projects/{project_id}/alerts/{*rest}",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/{project_id}/discovered-services",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/{project_id}/widget-query",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/{project_id}/variable-values",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/projects/{project_id}/root-cause-suggestions",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/projects/{project_id}/api-endpoints",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/projects/{project_id}/api-endpoints/{*rest}",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/projects/{project_id}/github",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/projects/{project_id}/github/{*rest}",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/projects/{project_id}/infra/{*rest}",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/projects/{project_id}/mcp",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/projects/{project_id}/mcp/{*rest}",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/profiles/projects/{project_id}/profiles",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/profiles/projects/{project_id}/profiles/{*rest}",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/profiles/projects/{project_id}/traces/{*rest}",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/profiles/projects/{project_id}/services/{*rest}",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/profiles/projects/{project_id}/source",
            axum::routing::any(proxy::proxy_to_watch_direct),
        )
        .route(
            "/api/system-overview/{project_id}/stack",
            axum::routing::get(proxy::proxy_to_watch_with_project),
        )
        .route(
            "/api/system-overview/{project_id}/context",
            axum::routing::post(proxy::proxy_to_watch_with_project),
        );

    let audited_api = api_router;

    let audited_auth = auth_routes::create_auth_router();

    let frontend_dir =
        std::env::var("FRONTEND_DIR").unwrap_or_else(|_| "./frontend-dist".to_string());
    let index_html = format!("{}/index.html", frontend_dir);
    let spa_fallback = ServeDir::new(&frontend_dir).not_found_service(ServeFile::new(&index_html));

    let app = Router::new()
        // .merge(pond_proxy) // Pond disabled — re-enable when Pond launches
        .merge(flow_proxy)
        .merge(watch_proxy)
        .merge(watch_integration_proxy)
        .merge(watch_direct)
        .merge(herd_routes)
        .merge(quickstart_routes(&index_html))
        .route("/mcp", axum::routing::post(proxy::proxy_to_mcp))
        .route(
            "/api/model-catalog",
            get(proxy::proxy_to_flow_model_catalog),
        )
        .nest("/api/auth", audited_auth)
        .nest("/api/auth/oauth", api::oauth::create_oauth_router())
        .route(
            "/api/invite/{token}",
            get(api::invitations::accept_invite_link),
        )
        .nest("/api", audited_api)
        .route("/health", get(|| async { "OK" }))
        .route_service("/p/{*rest}", ServeFile::new(&index_html))
        .fallback_service(spa_fallback)
        .layer(Extension(state.db.clone()))
        .layer(Extension(state.redis.clone()))
        .layer(Extension(state.clickhouse.clone()))
        .layer(cors)
        .layer(middleware::from_fn(security_headers))
        .with_state(state);

    let addr = std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:3003".to_string());
    tracing::info!("Website API server listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::quickstart_routes;
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use std::fs;
    use tower::Service;

    #[tokio::test]
    async fn quickstart_routes_serve_the_spa_entrypoint_with_success() {
        let frontend_dir = std::env::temp_dir().join(format!(
            "reiver-quickstart-route-test-{}",
            uuid::Uuid::new_v4()
        ));
        fs::create_dir_all(&frontend_dir).expect("create frontend fixture directory");

        let index_html = frontend_dir.join("index.html");
        fs::write(&index_html, "quickstart-spa-entrypoint").expect("write frontend fixture");

        let mut app = quickstart_routes::<()>(
            index_html
                .to_str()
                .expect("temporary path should be valid UTF-8"),
        );

        for uri in ["/quickstart", "/quickstart/"] {
            let response = app
                .call(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .expect("quickstart route response");

            assert_eq!(response.status(), StatusCode::OK, "{uri}");
            let body = to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("read quickstart response body");
            assert_eq!(&body[..], b"quickstart-spa-entrypoint", "{uri}");
        }

        fs::remove_dir_all(frontend_dir).expect("remove frontend fixture directory");
    }
}
