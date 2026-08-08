use std::sync::Arc;
use tokio::sync::broadcast;

pub use reiver_core::app_state::RedisPool;
use reiver_core::clickhouse_db::ClickHousePool;
use reiver_core::config::Config;
use reiver_core::db::DbPool;

use reiver_core::events::EventPublisher;

use reiver_core::crypto::RotatingSecretEncryptor;

use crate::github::GitHubService;
use crate::kafka::KafkaProducer;
use crate::llm::LlmSpanProcessor;
use crate::models::ProjectStatsWithExceptions;
use uuid::Uuid;

/// Message broadcast to SSE subscribers with stats data
#[derive(Debug, Clone)]
pub struct StatsUpdateMessage {
    pub project_id: Uuid,
    pub stats: Option<ProjectStatsWithExceptions>,
}

// =============================================================================
// WatchState -- Watch (APM) product state
// =============================================================================

pub struct WatchState {
    pub db: Arc<DbPool>,
    pub clickhouse: Arc<ClickHousePool>,
    pub redis: Arc<RedisPool>,
    pub config: Arc<Config>,
    pub kafka: Arc<KafkaProducer>,
    /// Platform event publisher for the event subscription system
    pub event_publisher: Arc<EventPublisher>,
    /// Broadcast channel for notifying SSE subscribers when errors are processed
    pub stats_broadcast: broadcast::Sender<StatsUpdateMessage>,
    /// LLM span processor for AI observability (detects LLM spans in traces)
    pub llm_processor: Arc<LlmSpanProcessor>,
    /// GitHub service for GitHub API integration (None if not configured)
    pub github_service: Option<Arc<GitHubService>>,
    /// Secret encryptor for encrypting tokens at rest (e.g. Slack bot tokens)
    pub encryptor: Arc<RotatingSecretEncryptor>,
    /// Shared HTTP client for ClickHouse PromQL queries (connection pooling)
    pub http_client: reqwest::Client,
    /// Entitlement checker for usage enforcement
    pub entitlements: Arc<dyn reiver_core::entitlements::EntitlementChecker>,
    /// Cached per-org observability limits, refreshed periodically by a background task
    pub obs_limits: Arc<ObsLimitsCache>,
}

// =============================================================================
// ObsLimitsCache -- preloaded per-org observability limits
// =============================================================================

use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::RwLock;

/// Cached observability limit for a single organization.
#[derive(Debug, Clone)]
pub struct OrgObsLimit {
    /// Included GB per billing period (-1 = unlimited)
    pub limit_gb: i64,
    /// Whether the org has an active Stripe subscription (paid tiers pass through)
    pub has_subscription: bool,
}

/// In-memory cache of all organization observability limits.
/// Refreshed by `spawn_obs_limits_refresh_task`.
pub struct ObsLimitsCache {
    data: RwLock<HashMap<Uuid, OrgObsLimit>>,
}

impl ObsLimitsCache {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }

    /// Look up an org's cached limit. Returns None if the org isn't in the cache
    /// (treat as allowed — the org will appear after the next refresh).
    pub async fn get(&self, org_id: Uuid) -> Option<OrgObsLimit> {
        self.data.read().await.get(&org_id).cloned()
    }

    /// Replace the entire cache contents.
    pub async fn refresh(&self, new_data: HashMap<Uuid, OrgObsLimit>) {
        let mut data = self.data.write().await;
        *data = new_data;
    }
}

/// Spawn a background task that refreshes the observability limits cache every 60 seconds.
pub fn spawn_obs_limits_refresh_task(
    db: Arc<DbPool>,
    entitlements: Arc<dyn reiver_core::entitlements::EntitlementChecker>,
    cache: Arc<ObsLimitsCache>,
) {
    tokio::spawn(async move {
        const REFRESH_INTERVAL: Duration = Duration::from_secs(60);

        loop {
            if let Err(e) = refresh_obs_limits(&db, entitlements.as_ref(), &cache).await {
                tracing::error!(error = %e, "Failed to refresh obs_limits cache");
            }
            tokio::time::sleep(REFRESH_INTERVAL).await;
        }
    });
}

async fn refresh_obs_limits(
    db: &DbPool,
    entitlements: &dyn reiver_core::entitlements::EntitlementChecker,
    cache: &ObsLimitsCache,
) -> anyhow::Result<()> {
    // Load all organizations
    let orgs: Vec<(Uuid,)> =
        sqlx::query_as("SELECT id FROM organizations")
            .fetch_all(db)
            .await?;

    // Load all orgs with active subscriptions in one query
    let subscribed: Vec<(Uuid,)> = sqlx::query_as(
        "SELECT DISTINCT organization_id FROM stripe_subscriptions WHERE status IN ('active', 'trialing')"
    )
    .fetch_all(db)
    .await?;

    let subscribed_set: std::collections::HashSet<Uuid> =
        subscribed.into_iter().map(|(id,)| id).collect();

    let mut new_data = HashMap::with_capacity(orgs.len());

    for (org_id,) in orgs {
        let limit_gb = match entitlements.get_config(org_id).await {
            Ok(tier) => tier.config.watch.ingestion_gb_included,
            Err(_) => -1, // default to unlimited on error
        };

        new_data.insert(org_id, OrgObsLimit {
            limit_gb,
            has_subscription: subscribed_set.contains(&org_id),
        });
    }

    cache.refresh(new_data).await;
    let count = cache.data.read().await.len();
    tracing::debug!(count, "Refreshed obs_limits cache");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn obs_limits_cache_returns_none_for_unknown_org() {
        let cache = ObsLimitsCache::new();
        assert!(cache.get(Uuid::new_v4()).await.is_none());
    }

    #[tokio::test]
    async fn obs_limits_cache_returns_stored_limit() {
        let cache = ObsLimitsCache::new();
        let org_id = Uuid::new_v4();

        let mut data = HashMap::new();
        data.insert(org_id, OrgObsLimit {
            limit_gb: 50,
            has_subscription: false,
        });
        cache.refresh(data).await;

        let limit = cache.get(org_id).await.unwrap();
        assert_eq!(limit.limit_gb, 50);
        assert!(!limit.has_subscription);
    }

    #[tokio::test]
    async fn obs_limits_cache_refresh_replaces_old_data() {
        let cache = ObsLimitsCache::new();
        let org_a = Uuid::new_v4();
        let org_b = Uuid::new_v4();

        let mut data = HashMap::new();
        data.insert(org_a, OrgObsLimit { limit_gb: 10, has_subscription: false });
        cache.refresh(data).await;

        // Replace with different data
        let mut data2 = HashMap::new();
        data2.insert(org_b, OrgObsLimit { limit_gb: 200, has_subscription: true });
        cache.refresh(data2).await;

        // org_a is gone, org_b is present
        assert!(cache.get(org_a).await.is_none());
        let limit = cache.get(org_b).await.unwrap();
        assert_eq!(limit.limit_gb, 200);
        assert!(limit.has_subscription);
    }

    #[tokio::test]
    async fn obs_limits_unlimited_indicated_by_negative() {
        let cache = ObsLimitsCache::new();
        let org_id = Uuid::new_v4();

        let mut data = HashMap::new();
        data.insert(org_id, OrgObsLimit { limit_gb: -1, has_subscription: true });
        cache.refresh(data).await;

        let limit = cache.get(org_id).await.unwrap();
        assert!(limit.limit_gb < 0); // -1 means unlimited
    }
}
