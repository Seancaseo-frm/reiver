//! Test support utilities for gateway integration tests.
//!
//! Provides a `TestApp` that spins up the real Axum application on a random
//! port with wiremock servers standing in for LLM provider APIs.
//!
//! All infrastructure dependencies (`DbPool`, `RedisPool`, `ClickHousePool`,
//! `KafkaProducer`) are constructed in lazy/no-op mode so no real services are
//! required. The provider key and introspection caches are pre-populated so the
//! handler never hits the database.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use axum::{Extension, Router};
use tokio::net::TcpListener;
use uuid::Uuid;
use wiremock::MockServer;

use reiver_flow::app_state::{
    CachedIntrospectionSettings, CachedProviderKey, FlowState,
    INTROSPECTION_SETTINGS_CACHE_TTL_SECS, PROVIDER_KEY_CACHE_TTL_SECS,
};
use reiver_flow::config::{Config, TotpAlgorithm};
use reiver_flow::gateway::cache::GatewayCache;
use reiver_flow::gateway::fallback::FallbackConfig;
use reiver_flow::gateway::latency_tracker::LatencyTracker;
use reiver_flow::gateway::provider_manager::{GatewayTimeouts, ProviderConfig, ProviderManager};
use reiver_flow::gateway::GatewayRouter;
use reiver_flow::gateway::prompt_store::PgPromptStore;
use reiver_flow::{api, clickhouse_db, crypto, kafka, llm, trusted_proxy};

/// No-op embedder for gateway tests that don't exercise knowledge base search.
fn shared_test_embedder() -> Arc<reiver_core::embeddings::KbEmbedder> {
    static INSTANCE: OnceLock<Arc<reiver_core::embeddings::KbEmbedder>> = OnceLock::new();
    INSTANCE
        .get_or_init(|| Arc::new(reiver_core::embeddings::KbEmbedder::noop()))
        .clone()
}

pub fn test_project_id() -> Uuid {
    Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap()
}

pub fn test_user_id() -> Uuid {
    Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap()
}

/// A running instance of the flow Axum app with mock LLM provider servers.
pub struct TestApp {
    pub base_url: String,
    pub openai_mock: MockServer,
    pub anthropic_mock: MockServer,
    pub google_mock: MockServer,
    pub state: Arc<FlowState>,
}

impl TestApp {
    pub async fn new() -> Self {
        Self::new_with_prompt_store(None).await
    }

    pub async fn new_with_prompt_store(
        prompt_store: Option<Arc<dyn reiver_flow::gateway::prompt_store::PromptWriteStore>>,
    ) -> Self {
        let openai_mock = MockServer::start().await;
        let anthropic_mock = MockServer::start().await;
        let google_mock = MockServer::start().await;

        let state = build_test_state(
            openai_mock.uri(),
            anthropic_mock.uri(),
            google_mock.uri(),
            prompt_store,
        )
        .await;

        let router = build_test_router(state.clone());

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind test listener");
        let addr = listener.local_addr().expect("failed to get local addr");

        tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("test server stopped unexpectedly");
        });

        Self {
            base_url: format!("http://{}", addr),
            openai_mock,
            anthropic_mock,
            google_mock,
            state,
        }
    }

    /// URL for `POST /api/gateway/v1/chat/completions`.
    pub fn chat_completions_url(&self) -> String {
        format!("{}/api/gateway/v1/chat/completions", self.base_url)
    }

    pub fn client(&self) -> reqwest::Client {
        reqwest::Client::new()
    }

    /// Helper: POST to chat/completions with the standard test project headers.
    pub async fn post_chat(&self, body: serde_json::Value) -> reqwest::Response {
        self.client()
            .post(self.chat_completions_url())
            .header("X-Project-Id", test_project_id().to_string())
            .header("X-User-Id", test_user_id().to_string())
            .json(&body)
            .send()
            .await
            .expect("failed to send chat completion request")
    }

    /// Overwrite the introspection cache entry for the test project.
    pub fn set_introspection(&self, enabled: bool, budget_tokens: u32) {
        let expires_at =
            Instant::now() + Duration::from_secs(INTROSPECTION_SETTINGS_CACHE_TTL_SECS * 10);
        self.state.introspection_settings_cache.insert(
            test_project_id(),
            CachedIntrospectionSettings {
                enabled,
                budget_tokens,
                session_budget_usd: None,
                guardrail_config: reiver_flow::gateway::guardrails::GuardrailConfig::default(),
                agent_enabled: true,
                agent_scopes: Vec::new(),
                judge_sample_rate: None,
                default_fallback_models: Vec::new(),
                provider_preferences: None,
                fallback_enabled: true,
                agent_soul: Default::default(),
                expires_at,
            },
        );
    }

    /// Overwrite the introspection cache with custom routing settings.
    pub fn set_routing(
        &self,
        fallback_enabled: bool,
        default_fallback_models: Vec<String>,
        provider_preferences: Option<reiver_flow::gateway::types::ProviderPreferences>,
    ) {
        let expires_at =
            Instant::now() + Duration::from_secs(INTROSPECTION_SETTINGS_CACHE_TTL_SECS * 10);
        self.state.introspection_settings_cache.insert(
            test_project_id(),
            CachedIntrospectionSettings {
                enabled: false,
                budget_tokens: 10_000,
                session_budget_usd: None,
                guardrail_config: reiver_flow::gateway::guardrails::GuardrailConfig::default(),
                agent_enabled: true,
                agent_scopes: Vec::new(),
                judge_sample_rate: None,
                default_fallback_models,
                provider_preferences,
                fallback_enabled,
                agent_soul: Default::default(),
                expires_at,
            },
        );
    }
}

async fn build_test_state(
    openai_url: String,
    anthropic_url: String,
    google_url: String,
    custom_prompt_store: Option<Arc<dyn reiver_flow::gateway::prompt_store::PromptWriteStore>>,
) -> Arc<FlowState> {
    let config = test_config(
        Some(openai_url.clone()),
        Some(anthropic_url.clone()),
        Some(google_url.clone()),
    );
    let config_arc = Arc::new(config);

    // PgPool::connect_lazy never opens a network connection during construction.
    // The gateway handler pre-populates caches so no DB queries fire in tests.
    let db_pool = sqlx::PgPool::connect_lazy(&config_arc.database_url)
        .expect("PgPool::connect_lazy must not fail on URL parse");
    let db_pool_arc = Arc::new(db_pool);

    // clickhouse-rs is HTTP-based; creating the client makes no network call.
    let clickhouse_pool = clickhouse_db::create_clickhouse_pool(&config_arc.clickhouse_url)
        .expect("ClickHouse pool creation must not fail");
    let clickhouse_pool_arc = Arc::new(clickhouse_pool);

    // bb8 with min_idle=0 (the default) never creates connections during build.
    let redis_manager = bb8_redis::RedisConnectionManager::new(config_arc.redis_url.clone())
        .expect("Redis manager creation must not fail");
    let redis_pool = bb8::Pool::builder()
        .max_size(2)
        .build(redis_manager)
        .await
        .expect("Redis pool build must succeed (connections are lazy)");
    let redis_pool_arc = Arc::new(redis_pool);

    // rdkafka producers connect to brokers asynchronously in the background.
    // Construction succeeds even with an unreachable broker; send errors are
    // non-fatal (logged and discarded) inside LlmSpanProcessor.
    let kafka_config = kafka::KafkaProducerConfig {
        hosts: config_arc.kafka_hosts.clone(),
        exceptions_topic: config_arc.kafka_exceptions_topic.clone(),
        spans_topic: config_arc.kafka_spans_topic.clone(),
        logs_otlp_topic: config_arc.kafka_logs_otlp_topic.clone(),
        logs_unstructured_topic: config_arc.kafka_logs_unstructured_topic.clone(),
        llm_chunks_topic: config_arc.kafka_llm_chunks_topic.clone(),
        metrics_topic: config_arc.kafka_metrics_topic.clone(),
        sync_jobs_topic: config_arc.kafka_sync_jobs_topic.clone(),
        pipeline_events_topic: config_arc.kafka_pipeline_events_topic.clone(),
        platform_events_topic: config_arc.kafka_platform_events_topic.clone(),
        session_eval_jobs_topic: config_arc.kafka_session_eval_jobs_topic.clone(),
        client_id: config_arc.kafka_client_id.clone(),
        linger_ms: config_arc.kafka_producer_linger_ms,
        max_retries: config_arc.kafka_producer_max_retries,
        message_timeout_ms: config_arc.kafka_message_timeout_ms,
        socket_timeout_ms: config_arc.kafka_socket_timeout_ms,
        compression_codec: config_arc.kafka_compression_codec.clone(),
        acks: config_arc.kafka_acks.clone(),
    };
    let kafka_producer =
        kafka::KafkaProducer::new(&kafka_config).expect("Kafka producer creation must not fail");
    let kafka_producer_arc = Arc::new(kafka_producer);

    // Generate a temporary encryption key (not persisted between test runs).
    let temp_key = crypto::SecretEncryptor::generate_key();
    let encryptor = Arc::new(
        crypto::RotatingSecretEncryptor::single_key(&temp_key).expect("generated key must be valid"),
    );

    // Cost calculator uses the DB only for price lookups, which never fire
    // because the span processor runs in the background and errors silently.
    let cost_calculator = llm::CostCalculator::new(db_pool_arc.clone());
    let llm_processor = Arc::new(llm::LlmSpanProcessor::new(cost_calculator));

    let latency_tracker = Arc::new(LatencyTracker::new(clickhouse_pool_arc.clone()));

    let model_catalog_cache = Arc::new(
        reiver_flow::gateway::model_catalog_cache::ModelCatalogCache::new(db_pool_arc.clone()),
    );

    let global_model_stats = Arc::new(
        reiver_flow::gateway::global_model_stats::GlobalModelStatsCache::new(
            clickhouse_pool_arc.clone(),
        ),
    );

    let five_secs = Duration::from_secs(5);
    let provider_manager = Arc::new(
        ProviderManager::new(
            GatewayTimeouts::new(
                five_secs, five_secs, five_secs, five_secs, five_secs, five_secs, five_secs,
            ),
            ProviderConfig::new(
                "2023-06-01",
                Some(openai_url.clone()),
                Some(anthropic_url.clone()),
                Some(google_url.clone()),
                None,
            ),
            HashMap::new(),
        )
        .with_latency_tracker(latency_tracker.clone())
        .with_model_catalog_cache(
            (*model_catalog_cache).clone(),
        ),
    );

    let gateway_router = Arc::new(
        GatewayRouter::with_full_config(
            GatewayTimeouts::new(
                five_secs, five_secs, five_secs, five_secs, five_secs, five_secs, five_secs,
            ),
            ProviderConfig::new(
                "2023-06-01",
                Some(openai_url),
                Some(anthropic_url),
                Some(google_url),
                None,
            ),
        )
        .with_latency_tracker(latency_tracker.clone()),
    );

    // Semantic cache disabled — no semcache service required.
    let gateway_cache = Arc::new(GatewayCache::new(
        "http://127.0.0.1:8080".to_string(),
        86400,
        false,
    ));

    let fallback_config = Arc::new(FallbackConfig::from_config(&config_arc));

    let (llm_request_tx, _llm_request_rx) = tokio::sync::mpsc::channel(100);

    let db_pool_for_credits = db_pool_arc.clone();
    let redis_pool_for_credits = redis_pool_arc.clone();

    let prompt_store: Arc<dyn reiver_flow::gateway::prompt_store::PromptWriteStore> =
        custom_prompt_store.unwrap_or_else(|| {
            Arc::new(PgPromptStore::new(
                db_pool_arc.as_ref().clone(),
                redis_pool_arc.as_ref().clone(),
            ))
        });

    let state = Arc::new(FlowState {
        db: db_pool_arc,
        clickhouse: clickhouse_pool_arc,
        redis: redis_pool_arc,
        config: config_arc,
        encryptor,
        prompt_store,
        http_client: reqwest::Client::new(),
        kafka: kafka_producer_arc.clone(),
        event_publisher: std::sync::Arc::new(reiver_core::events::EventPublisher::new(
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
        action_registry: Arc::new(reiver_mcp::registry::ActionRegistry::new()),
        compiler_cancel_tokens: dashmap::DashMap::new(),
        agent_conversation_locks: dashmap::DashMap::new(),
        agent_http_client: reqwest::Client::new(),
        moodeng_project_id: None,
        credits_enabled: false,
        credit_service: std::sync::Arc::new(reiver_core::billing::credits::CreditService::new(
            db_pool_for_credits,
            redis_pool_for_credits,
        )),
        meter_service: std::sync::Arc::new(reiver_core::billing::MeterService::noop()),
        asset_storage: std::sync::Arc::new(reiver_core::storage::InMemoryStorage::new(
            "http://localhost:3001/assets",
        )),
        internal_urls: reiver_flow::app_state::InternalServiceUrls {
            website: "http://localhost:3003".into(),
            flow: "http://localhost:3001".into(),
            watch: "http://localhost:3000".into(),
            herd: "http://localhost:3004".into(),
        },
        project_org_cache: quick_cache::sync::Cache::new(1024),
        model_catalog_cache,
        global_model_stats,
        entitlements: std::sync::Arc::new(reiver_core::entitlements::MockEntitlementChecker::new()),
        kb_embedder: shared_test_embedder(),
        otel_publisher: reiver_flow::gateway::otel_publisher::OTelPublisher::start(
            "http://localhost:3000".into(),
            reqwest::Client::new(),
        ),
    });

    // Pre-populate provider key cache — prevents any DB round-trips.
    let key_ttl = Instant::now() + Duration::from_secs(PROVIDER_KEY_CACHE_TTL_SECS * 10);
    for provider in &["openai", "anthropic", "google", "bedrock"] {
        state.provider_key_cache.insert(
            (test_project_id(), provider.to_string()),
            CachedProviderKey {
                key: format!("sk-test-{}", provider),
                is_platform: true,
                expires_at: key_ttl,
            },
        );
    }

    // Pre-populate introspection settings (disabled by default).
    let introspection_ttl =
        Instant::now() + Duration::from_secs(INTROSPECTION_SETTINGS_CACHE_TTL_SECS * 10);
    state.introspection_settings_cache.insert(
        test_project_id(),
        CachedIntrospectionSettings {
            enabled: false,
            budget_tokens: 10_000,
            session_budget_usd: None,
            guardrail_config: reiver_flow::gateway::guardrails::GuardrailConfig::default(),
            agent_enabled: true,
            agent_scopes: Vec::new(),
            judge_sample_rate: None,
            default_fallback_models: Vec::new(),
            provider_preferences: None,
            fallback_enabled: true,
            agent_soul: Default::default(),
            expires_at: introspection_ttl,
        },
    );

    state
}

fn build_test_router(state: Arc<FlowState>) -> Router {
    Router::new()
        .nest(
            "/api",
            api::create_flow_api_router().with_state(state.clone()),
        )
        .nest(
            "/api/v1",
            api::create_flow_api_router().with_state(state.clone()),
        )
        .layer(axum::middleware::from_fn_with_state(
            state.config.clone(),
            trusted_proxy::trusted_proxy_middleware,
        ))
        .layer(Extension(state.db.clone()))
        .layer(Extension(state.clickhouse.clone()))
        .layer(Extension(state.config.clone()))
        .with_state(state)
}

/// Construct a `Config` with test-friendly defaults, overriding only the
/// provider base URLs so wiremock servers are used instead of real providers.
pub fn test_config(
    openai_base_url: Option<String>,
    anthropic_base_url: Option<String>,
    google_base_url: Option<String>,
) -> Config {
    Config {
        database_url: "postgresql://postgres:postgres@127.0.0.1:5432/test_unused".to_string(),
        clickhouse_url: "http://default:@127.0.0.1:18123".to_string(),
        redis_url: "redis://127.0.0.1:6399".to_string(),
        jwt_secret: "test-jwt-secret-must-be-at-least-32-chars!!".to_string(),
        jwt_issuer: "reiver-test".to_string(),
        jwt_expiration_hours: 24,
        kafka_hosts: "127.0.0.1:9999".to_string(),
        clickhouse_kafka_hosts: "127.0.0.1:9999".to_string(),
        kafka_exceptions_topic: "test.exceptions".to_string(),
        kafka_spans_topic: "test.spans".to_string(),
        kafka_logs_otlp_topic: "test.logs.otlp".to_string(),
        kafka_logs_unstructured_topic: "test.logs.unstructured".to_string(),
        kafka_llm_chunks_topic: "test.llm.chunks".to_string(),
        kafka_metrics_topic: "test.metrics".to_string(),
        kafka_sync_jobs_topic: "test.sync_jobs".to_string(),
        kafka_pipeline_events_topic: "test.pipeline_events".to_string(),
        kafka_platform_events_topic: "test.platform_events".to_string(),
        kafka_session_eval_jobs_topic: "test.session_eval_jobs".to_string(),
        kafka_client_id: None,
        kafka_producer_linger_ms: 0,
        kafka_producer_max_retries: 0,
        kafka_message_timeout_ms: 1_000,
        kafka_socket_timeout_ms: 1_000,
        kafka_compression_codec: "none".to_string(),
        kafka_acks: "1".to_string(),
        cors_allowed_origins: vec!["*".to_string()],
        cors_allow_credentials: false,
        encryption_key: None,
        clickhouse_max_rows: 1_000,
        clickhouse_default_limit: 100,
        rate_limit_analytics_per_minute: 100_000,
        rate_limit_analytics_per_hour: 1_000_000,
        rate_limit_crud_per_minute: 100_000,
        rate_limit_crud_per_hour: 1_000_000,
        rate_limit_billing_per_minute: 100_000,
        rate_limit_billing_per_hour: 1_000_000,
        rate_limit_gateway_per_minute: 100_000,
        rate_limit_gateway_per_hour: 1_000_000,
        rate_limit_external_api_per_minute: 100_000,
        rate_limit_external_api_per_hour: 1_000_000,
        rate_limit_nl_query_per_minute: 100_000,
        rate_limit_nl_query_per_hour: 1_000_000,
        rate_limit_ingestion_per_minute: 100_000,
        rate_limit_ingestion_per_hour: 1_000_000,
        cookie_domain: None,
        saml_time_skew_seconds: 60,
        totp_algorithm: TotpAlgorithm::Sha1,
        mfa_challenge_ttl_seconds: 180,
        session_ip_binding_enabled: false,
        session_user_agent_binding_enabled: false,
        base_url: "http://localhost:3000".to_string(),
        stripe_api_key: None,
        stripe_webhook_secret: None,
        stripe_allowed_price_ids: vec![],
        stripe_metered_price_id: None,
        stripe_webhook_ip_allowlist_enabled: false,
        stripe_webhook_ip_allowlist: vec![],
        stripe_portal_return_url: "/settings/billing".to_string(),
        credits_enabled: false,
        budget_alert_cooldown_hours: 24,
        // Fallback enabled but zero retries keeps tests fast while still
        // allowing fallback chain tests to exercise the fallback path.
        gateway_fallback_enabled: true,
        gateway_max_retries: 0,
        gateway_initial_retry_delay_ms: 10,
        gateway_max_retry_delay_ms: 50,
        gateway_cache_enabled: false,
        gateway_cache_url: "http://127.0.0.1:8080".to_string(),
        gateway_cache_ttl_seconds: 86_400,
        gateway_log_content: false,
        gateway_timeout_seconds: 5,
        gateway_timeout_openai_seconds: 5,
        gateway_timeout_anthropic_seconds: 5,
        gateway_timeout_google_seconds: 5,
        gateway_timeout_bedrock_seconds: 5,
        gateway_anthropic_api_version: "2023-06-01".to_string(),
        gateway_openai_base_url: openai_base_url,
        gateway_anthropic_base_url: anthropic_base_url,
        gateway_google_base_url: google_base_url,
        gateway_theta_base_url: None,
        gateway_deepseek_base_url: None,
        gateway_xai_base_url: None,
        gateway_mistral_base_url: None,
        gateway_groq_base_url: None,
        gateway_together_base_url: None,
        gateway_fireworks_base_url: None,
        gateway_perplexity_base_url: None,
        gateway_cohere_base_url: None,
        gateway_openrouter_base_url: None,
        gateway_cerebras_base_url: None,
        gateway_deepinfra_base_url: None,
        gateway_alibaba_base_url: None,
        gateway_nvidia_base_url: None,
        gateway_ai21_base_url: None,
        gateway_sambanova_base_url: None,
        gateway_lambda_base_url: None,
        gateway_lepton_base_url: None,
        gateway_hyperbolic_base_url: None,
        gateway_ovhcloud_base_url: None,
        gateway_novita_base_url: None,
        gateway_huggingface_base_url: None,
        gateway_cloudflare_base_url: None,
        gateway_azure_openai_base_url: None,
        gateway_vertex_ai_base_url: None,
        gateway_timeout_theta_seconds: 5,
        gateway_timeout_deepseek_seconds: 5,
        gateway_timeout_openai_compat_seconds: 5,
        gateway_default_openai_api_key: None,
        gateway_default_anthropic_api_key: None,
        gateway_default_google_api_key: None,
        gateway_default_theta_api_key: None,
        gateway_default_deepseek_api_key: None,
        playground_evaluation_model: "gpt-4o-mini".to_string(),
        github_app_id: None,
        github_app_name: None,
        github_app_private_key: None,
        github_app_webhook_secret: None,
        github_webhook_ip_allowlist: vec![],
        trusted_proxy_cidrs: vec![],
        api_base_url: None,
        storage_backend: "memory".to_string(),
        storage_local_path: "/tmp/test-assets".to_string(),
        storage_local_base_url: "http://localhost:3000/api/assets".to_string(),
        storage_s3_bucket: None,
        storage_s3_region: "us-east-1".to_string(),
        storage_s3_endpoint: None,
        storage_s3_path_style: false,
        flow_gateway_url: "http://127.0.0.1:3001".to_string(),
        otel_exporter_endpoint: None,
        otel_project_id: None,
        allow_signup: true,
        allow_password_login: true,
        profiling_enabled: false,
        profiling_frequency: 99,
        profiling_cpu_interval_secs: 600,
        profiling_heap_interval_secs: 600,
        oauth_google_client_id: None,
        oauth_google_client_secret: None,
        oauth_github_client_id: None,
        oauth_github_client_secret: None,
        oauth_microsoft_client_id: None,
        oauth_microsoft_client_secret: None,
        slack_client_id: None,
        slack_client_secret: None,
        slack_signing_secret: None,
        loops_api_key: None,
        loops_invite_template_id: None,
        loops_alert_template_id: None,
        loops_welcome_template_id: None,
        app_url: None,
    }
}
