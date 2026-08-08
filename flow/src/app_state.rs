use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
pub use reiver_core::app_state::RedisPool;
use reiver_core::clickhouse_db::ClickHousePool;
use reiver_core::config::Config;
use reiver_core::crypto::RotatingSecretEncryptor;
use reiver_core::db::DbPool;
use uuid::Uuid;

use crate::gateway::cache::GatewayCache;
use crate::gateway::fallback::FallbackConfig;
use crate::gateway::latency_tracker::LatencyTracker;
use crate::gateway::prompt_store::PromptWriteStore;
use crate::gateway::provider_manager::{ProviderManager, ResolvedKey};
use crate::gateway::provider_types::Provider;
use crate::gateway::GatewayRouter;
use reiver_core::billing::credits::CreditService;
use reiver_core::entitlements::EntitlementChecker;
use reiver_core::events::EventPublisher;
use reiver_core::storage::AssetStorage;

use crate::kafka::KafkaProducer;
use crate::llm::{LlmRequest, LlmSpanProcessor};
use crate::metrics::FlowMetrics;
use tokio::sync::mpsc;

/// Cached provider API key with TTL-based expiry.
#[derive(Clone)]
pub struct CachedProviderKey {
    pub key: String,
    pub is_platform: bool,
    pub expires_at: Instant,
}

/// TTL for cached provider API keys (60 seconds).
pub const PROVIDER_KEY_CACHE_TTL_SECS: u64 = 60;

/// Cached per-project introspection settings with TTL-based expiry.
#[derive(Clone)]
pub struct CachedIntrospectionSettings {
    pub enabled: bool,
    pub budget_tokens: u32,
    /// Per-session cost budget in USD. `None` or `0.0` means disabled.
    pub session_budget_usd: Option<f64>,
    /// Guardrail config for this project. All-default = all checks off.
    pub guardrail_config: crate::gateway::guardrails::GuardrailConfig,
    pub agent_enabled: bool,
    pub agent_scopes: Vec<String>,
    /// Fraction of prompt-config requests to evaluate with LLM-as-judge (0.0-1.0).
    pub judge_sample_rate: Option<f64>,
    pub default_fallback_models: Vec<String>,
    pub provider_preferences: Option<crate::gateway::types::ProviderPreferences>,
    pub fallback_enabled: bool,
    pub agent_soul: crate::api::llm_settings::AgentSoul,
    pub expires_at: Instant,
}

/// TTL for cached introspection settings (60 seconds).
pub const INTROSPECTION_SETTINGS_CACHE_TTL_SECS: u64 = 60;


// =============================================================================
// FlowState -- Flow (LLM Gateway) product state
// =============================================================================

/// Internal service URLs for cross-service communication.
#[derive(Clone)]
pub struct InternalServiceUrls {
    pub website: String,
    pub flow: String,
    pub watch: String,
    pub herd: String,
}

pub struct FlowState {
    pub db: Arc<DbPool>,
    pub clickhouse: Arc<ClickHousePool>,
    pub redis: Arc<RedisPool>,
    pub config: Arc<Config>,
    pub encryptor: Arc<RotatingSecretEncryptor>,
    /// Trait-abstracted prompt hub storage (configs, versions, rollouts).
    pub prompt_store: Arc<dyn PromptWriteStore>,
    pub http_client: reqwest::Client,
    pub kafka: Arc<KafkaProducer>,
    /// Platform event publisher for the event subscription system
    pub event_publisher: Arc<EventPublisher>,
    /// Typed provider manager — owns all LLM providers, handles routing,
    /// key resolution, auto-routing, and fallback execution.
    pub provider_manager: Arc<ProviderManager>,
    /// AI Gateway router for LLM provider routing and prefix-based model dispatch.
    /// Used by all gateway routes for request execution and fallback ordering.
    pub gateway_router: Arc<GatewayRouter>,
    /// Gateway cache for semantic caching of LLM responses
    pub gateway_cache: Arc<GatewayCache>,
    /// LLM span processor for AI observability
    pub llm_processor: Arc<LlmSpanProcessor>,
    /// Sender for batched LLM request writes to ClickHouse (high throughput)
    pub llm_request_tx: mpsc::Sender<LlmRequest>,
    /// Latency tracker for adaptive provider routing
    pub latency_tracker: Arc<LatencyTracker>,
    /// In-memory cache for provider API keys (avoids DB query per request)
    pub provider_key_cache: quick_cache::sync::Cache<(Uuid, String), CachedProviderKey>,
    /// In-memory cache for per-project introspection settings (avoids DB query per request)
    pub introspection_settings_cache: quick_cache::sync::Cache<Uuid, CachedIntrospectionSettings>,
    /// Pre-computed fallback config (avoids HashMap clone per request)
    pub fallback_config: Arc<FallbackConfig>,
    /// OTel metrics counters and histograms for the gateway
    pub metrics: Arc<FlowMetrics>,
    /// Per-project OTel publisher — sends metrics, spans, and logs to the
    /// watch service so they appear in each user's dashboards and billing.
    pub otel_publisher: crate::gateway::otel_publisher::OTelPublisher,
    /// MCP action registry — all platform actions for the in-app AI agent.
    pub action_registry: Arc<reiver_mcp::registry::ActionRegistry>,
    /// In-flight prompt compiler tasks — cancel tokens keyed by `agent_tasks.id`.
    pub compiler_cancel_tokens: dashmap::DashMap<Uuid, tokio_util::sync::CancellationToken>,
    /// Per-conversation mutex to serialize concurrent agent_chat requests.
    pub agent_conversation_locks: dashmap::DashMap<Uuid, Arc<tokio::sync::Mutex<()>>>,
    /// Shared HTTP client for the agent's internal tool calls (connection pooling).
    pub agent_http_client: reqwest::Client,
    /// Project ID for MooDeng's internal prompt configs in the prompt hub.
    pub moodeng_project_id: Option<Uuid>,
    /// Whether the credit system and platform-managed API keys are enabled.
    /// When false, only BYOK keys are resolved and all credit gates are skipped.
    pub credits_enabled: bool,
    /// Credit service for Flow billing (balance checks, deductions, BYOK fees).
    pub credit_service: Arc<CreditService>,
    /// Stripe meter service for reporting usage to Stripe Meters.
    pub meter_service: Arc<reiver_core::billing::MeterService>,
    /// Asset storage for agent file attachments (S3/local/in-memory).
    pub asset_storage: Arc<dyn AssetStorage>,
    /// In-memory cache of the model_catalog table (refreshes every 5 min).
    pub model_catalog_cache: Arc<crate::gateway::model_catalog_cache::ModelCatalogCache>,
    /// Global model performance/security stats from ClickHouse (refreshes every 5 min).
    pub global_model_stats: Arc<crate::gateway::global_model_stats::GlobalModelStatsCache>,
    /// Internal service URLs (website, flow, watch) for cross-service calls.
    pub internal_urls: InternalServiceUrls,
    /// Cache mapping project_id -> organization_id with TTL (avoids DB lookup per gateway request).
    pub project_org_cache: quick_cache::sync::Cache<Uuid, CachedOrgId>,
    /// Entitlement checker for tier-based rate lookups (fee percentages).
    pub entitlements: Arc<dyn EntitlementChecker>,
    /// Local embedding model for knowledge base vector similarity search.
    pub kb_embedder: Arc<reiver_core::embeddings::KbEmbedder>,
}

/// TTL for cached project -> organization mappings (120 seconds).
pub const PROJECT_ORG_CACHE_TTL_SECS: u64 = 120;

#[derive(Clone)]
pub struct CachedOrgId {
    pub org_id: Uuid,
    pub expires_at: Instant,
}

impl FlowState {
    /// Build an `ActionContext` for tool execution with consistent configuration.
    ///
    /// Eliminates the duplicated struct-literal pattern across agent, investigate,
    /// and agent_task handlers.
    pub fn build_action_context(
        &self,
        project_id: Uuid,
        caller: reiver_mcp::action::Caller,
        scopes: Vec<String>,
        origin: (&str, &str, &str),
    ) -> reiver_mcp::action::ActionContext {
        self.build_action_context_with_org(project_id, caller, scopes, origin, None)
    }

    /// Like `build_action_context` but allows passing a pre-resolved org ID.
    pub fn build_action_context_with_org(
        &self,
        project_id: Uuid,
        caller: reiver_mcp::action::Caller,
        scopes: Vec<String>,
        origin: (&str, &str, &str),
        organization_id: Option<Uuid>,
    ) -> reiver_mcp::action::ActionContext {
        let (origin_type, origin_ref, origin_reason) = origin;
        let http = match &caller {
            reiver_mcp::action::Caller::User { user_id, ref jwt } => {
                reiver_mcp::client::InternalClient::new_for_user(
                    self.internal_urls.website.clone(),
                    self.internal_urls.flow.clone(),
                    self.internal_urls.watch.clone(),
                    project_id,
                    self.agent_http_client.clone(),
                    jwt.clone(),
                )
                .with_herd_url(self.internal_urls.herd.clone())
                .with_user_id(*user_id)
                .with_creator("user", "", "")
                .with_origin(origin_type, origin_ref, origin_reason)
            }
            reiver_mcp::action::Caller::System => reiver_mcp::client::InternalClient::new(
                self.internal_urls.website.clone(),
                self.internal_urls.flow.clone(),
                self.internal_urls.watch.clone(),
                project_id,
                String::new(),
            )
            .with_herd_url(self.internal_urls.herd.clone())
            .with_creator("system", &format!("moodeng-{origin_type}"), "")
            .with_origin(origin_type, origin_ref, origin_reason),
            reiver_mcp::action::Caller::ApiKey { .. } => {
                reiver_mcp::client::InternalClient::new(
                    self.internal_urls.website.clone(),
                    self.internal_urls.flow.clone(),
                    self.internal_urls.watch.clone(),
                    project_id,
                    String::new(),
                )
                .with_herd_url(self.internal_urls.herd.clone())
                .with_creator("api_key", "", "")
                .with_origin(origin_type, origin_ref, origin_reason)
            }
        };
        reiver_mcp::action::ActionContext {
            project_id,
            caller,
            scopes,
            http,
            db: Some(self.db.as_ref().clone()),
            clickhouse: Some(self.clickhouse.as_ref().clone()),
            encryptor: Some(self.encryptor.clone()),
            asset_storage: Some(self.asset_storage.clone()),
            kb_embedder: Some(self.kb_embedder.clone()),
            meter_service: Some(self.meter_service.as_ref().clone()),
            organization_id,
            entitlements: self.entitlements.clone(),
            key_prefix: String::new(),
            key_label: String::new(),
        }
    }

    /// Look up the organization that owns a project, with TTL-based in-memory caching.
    /// Returns `Err` on DB failure, `Ok(None)` if the project genuinely has no org.
    pub async fn get_organization_id(&self, project_id: Uuid) -> anyhow::Result<Option<Uuid>> {
        if let Some(cached) = self.project_org_cache.get(&project_id) {
            if cached.expires_at > Instant::now() {
                return Ok(Some(cached.org_id));
            }
        }

        let org_id: Option<Uuid> =
            sqlx::query_scalar("SELECT organization_id FROM projects WHERE id = $1")
                .bind(project_id)
                .fetch_optional(self.db.as_ref())
                .await?;

        if let Some(oid) = org_id {
            self.project_org_cache.insert(
                project_id,
                CachedOrgId {
                    org_id: oid,
                    expires_at: Instant::now()
                        + std::time::Duration::from_secs(PROJECT_ORG_CACHE_TTL_SECS),
                },
            );
        }

        Ok(org_id)
    }

    /// Check whether an organization has Stripe credit balance (synced every 30s).
    /// Uses Redis cache (key `billing:stripe_credits:{org_id}`).
    pub async fn check_has_stripe_credits(&self, org_id: Uuid) -> bool {
        let cache_key = format!("billing:stripe_credits:{}", org_id);
        if let Ok(mut conn) = self.redis.get().await {
            if let Ok(val) = redis::cmd("GET")
                .arg(&cache_key)
                .query_async::<Option<String>>(&mut *conn)
                .await
            {
                if let Some(v) = val {
                    return v == "1";
                }
            }
        }
        false
    }

    /// Check whether an organization has an active subscription.
    /// Uses Redis cache (key `billing:sub:{org_id}`, TTL 60s) for fast lookups.
    pub async fn check_has_active_subscription(&self, org_id: Uuid) -> bool {
        let cache_key = format!("billing:sub:{}", org_id);
        if let Ok(mut conn) = self.redis.get().await {
            if let Ok(val) = redis::cmd("GET")
                .arg(&cache_key)
                .query_async::<Option<String>>(&mut *conn)
                .await
            {
                if let Some(v) = val {
                    return v == "1";
                }
            }
        }

        let has_sub: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM stripe_subscriptions
                WHERE organization_id = $1
                  AND status IN ('active', 'trialing', 'past_due')
            )
            "#,
        )
        .bind(org_id)
        .fetch_one(self.db.as_ref())
        .await
        .unwrap_or(false);

        if let Ok(mut conn) = self.redis.get().await {
            let _ = redis::cmd("SET")
                .arg(&cache_key)
                .arg(if has_sub { "1" } else { "0" })
                .arg("EX")
                .arg(60)
                .query_async::<()>(&mut *conn)
                .await;
        }

        has_sub
    }

    /// Check whether an organization has a payment method on file.
    /// Uses Redis cache (key `billing:pm:{org_id}`, TTL 5 min) for fast lookups.
    pub async fn check_has_payment_method(&self, org_id: Uuid) -> bool {
        let cache_key = format!("billing:pm:{}", org_id);

        // Try Redis first
        if let Ok(mut conn) = self.redis.get().await {
            if let Ok(val) = redis::cmd("GET")
                .arg(&cache_key)
                .query_async::<Option<String>>(&mut *conn)
                .await
            {
                if let Some(v) = val {
                    return v == "1";
                }
            }
        }

        // Fall back to DB
        let has_pm: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM payment_methods pm
                JOIN stripe_customers sc ON sc.stripe_customer_id = pm.provider_customer_id
                WHERE sc.organization_id = $1
                  AND pm.is_default = true
                  AND pm.status = 'active'
            )
            "#,
        )
        .bind(org_id)
        .fetch_one(self.db.as_ref())
        .await
        .unwrap_or(false);

        // Cache result in Redis for 5 minutes
        if let Ok(mut conn) = self.redis.get().await {
            let _ = redis::cmd("SET")
                .arg(&cache_key)
                .arg(if has_pm { "1" } else { "0" })
                .arg("EX")
                .arg(300)
                .query_async::<()>(&mut *conn)
                .await;
        }

        has_pm
    }
}

// ---------------------------------------------------------------------------
// ProviderKeyStore implementation for FlowState
// ---------------------------------------------------------------------------

#[async_trait]
impl crate::gateway::provider_manager::ProviderKeyStore for FlowState {
    #[tracing::instrument(
        name = "gateway.resolve_key",
        skip(self),
        fields(project_id = %project_id, provider = %provider)
    )]
    async fn get_key(&self, project_id: Uuid, provider: Provider) -> Option<ResolvedKey> {
        let cache_key = (project_id, provider.as_str().to_string());

        if let Some(cached) = self.provider_key_cache.get(&cache_key) {
            if cached.expires_at > Instant::now() {
                return Some(ResolvedKey {
                    key: cached.key,
                    is_platform: cached.is_platform,
                    base_url: None,
                });
            }
        }

        let setting_key = format!("gateway_{}_api_key", provider.as_str());

        let key: Option<String> = sqlx::query_scalar(
            r#"
            SELECT ps.value
            FROM project_settings ps
            WHERE ps.project_id = $1 AND ps.key = $2
            "#,
        )
        .bind(project_id)
        .bind(&setting_key)
        .fetch_optional(self.db.as_ref())
        .await
        .ok()?;

        // For ThetaDedicated, also resolve the per-project base URL
        let base_url = if provider == Provider::ThetaDedicated {
            let url_key = format!("gateway_{}_base_url", provider.as_str());
            let raw: Option<String> = sqlx::query_scalar(
                r#"
                SELECT ps.value
                FROM project_settings ps
                WHERE ps.project_id = $1 AND ps.key = $2
                "#,
            )
            .bind(project_id)
            .bind(&url_key)
            .fetch_optional(self.db.as_ref())
            .await
            .ok()?;
            raw
        } else {
            None
        };

        match key {
            Some(encrypted_key) => {
                let decrypted = self.encryptor.decrypt(&encrypted_key).ok()?;
                self.provider_key_cache.insert(
                    cache_key,
                    CachedProviderKey {
                        key: decrypted.clone(),
                        is_platform: false,
                        expires_at: Instant::now()
                            + std::time::Duration::from_secs(PROVIDER_KEY_CACHE_TTL_SECS),
                    },
                );
                Some(ResolvedKey {
                    key: decrypted,
                    is_platform: false,
                    base_url,
                })
            }
            None if provider == Provider::ThetaDedicated => {
                // ThetaDedicated requires a base_url; fall back with empty key if URL is set
                base_url.map(|url| ResolvedKey {
                    key: String::new(),
                    is_platform: false,
                    base_url: Some(url),
                })
            }
            None => {
                if self.credits_enabled {
                    self.provider_manager
                        .default_keys()
                        .get(&provider)
                        .map(|k| ResolvedKey {
                            key: k.clone(),
                            is_platform: true,
                            base_url: None,
                        })
                } else {
                    None
                }
            }
        }
    }

    #[tracing::instrument(
        name = "gateway.resolve_keys_batch",
        skip(self),
        fields(project_id = %project_id)
    )]
    async fn get_keys_batch(
        &self,
        project_id: Uuid,
        providers: &[Provider],
    ) -> anyhow::Result<HashMap<Provider, ResolvedKey>> {
        if providers.is_empty() {
            return Ok(HashMap::new());
        }

        let now = Instant::now();
        let mut result = HashMap::new();
        let mut missing: Vec<Provider> = Vec::new();

        for &provider in providers {
            let cache_key = (project_id, provider.as_str().to_string());
            if let Some(cached) = self.provider_key_cache.get(&cache_key) {
                if cached.expires_at > now {
                    result.insert(
                        provider,
                        ResolvedKey {
                            key: cached.key,
                            is_platform: cached.is_platform,
                            base_url: None,
                        },
                    );
                    continue;
                }
            }
            missing.push(provider);
        }

        if missing.is_empty() {
            return Ok(result);
        }

        let setting_keys: Vec<String> = missing
            .iter()
            .map(|p| format!("gateway_{}_api_key", p.as_str()))
            .collect();

        let rows: Vec<(String, String)> = sqlx::query_as(
            r#"
            SELECT ps.key, ps.value
            FROM project_settings ps
            WHERE ps.project_id = $1 AND ps.key = ANY($2)
            "#,
        )
        .bind(project_id)
        .bind(&setting_keys)
        .fetch_all(self.db.as_ref())
        .await?;

        let expires_at =
            Instant::now() + std::time::Duration::from_secs(PROVIDER_KEY_CACHE_TTL_SECS);
        let mut found_byok: std::collections::HashSet<Provider> = std::collections::HashSet::new();
        for (setting_key, encrypted_value) in rows {
            if let Some(provider_str) = setting_key
                .strip_prefix("gateway_")
                .and_then(|s| s.strip_suffix("_api_key"))
            {
                if let Ok(provider) = provider_str.parse::<Provider>() {
                    if let Ok(decrypted) = self.encryptor.decrypt(&encrypted_value) {
                        self.provider_key_cache.insert(
                            (project_id, provider.as_str().to_string()),
                            CachedProviderKey {
                                key: decrypted.clone(),
                                is_platform: false,
                                expires_at,
                            },
                        );
                        result.insert(
                            provider,
                            ResolvedKey {
                                key: decrypted,
                                is_platform: false,
                                base_url: None,
                            },
                        );
                        found_byok.insert(provider);
                    }
                }
            }
        }

        if self.credits_enabled {
            for provider in &missing {
                if !result.contains_key(provider) {
                    if let Some(default_key) = self.provider_manager.default_keys().get(provider) {
                        result.insert(
                            *provider,
                            ResolvedKey {
                                key: default_key.clone(),
                                is_platform: true,
                                base_url: None,
                            },
                        );
                    }
                }
            }
        }

        Ok(result)
    }

    async fn get_available_providers(&self, project_id: Uuid) -> anyhow::Result<Vec<Provider>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            r#"
            SELECT provider
            FROM llm_provider_integrations
            WHERE project_id = $1 AND enabled = true
            "#,
        )
        .bind(project_id)
        .fetch_all(self.db.as_ref())
        .await?;

        let mut providers: Vec<Provider> = rows
            .iter()
            .filter_map(|(s,)| s.parse::<Provider>().ok())
            .collect();

        if self.credits_enabled {
            for (provider, _) in self.provider_manager.default_keys() {
                if !providers.contains(provider) {
                    providers.push(*provider);
                }
            }
        }

        Ok(providers)
    }
}
