use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::Value as JsonValue;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::db::DbPool;

use super::checker::EntitlementChecker;
use super::types::{merge_config, ResolvedTier, TierConfig};

const CACHE_TTL: Duration = Duration::from_secs(15);

/// A cached snapshot of a single tier_definitions row.
#[derive(Debug, Clone)]
struct CachedTierDefinition {
    pub name: String,
    pub display_name: String,
    pub stripe_price_id: Option<String>,
    pub config: TierConfig,
}

struct TierCache {
    tiers: HashMap<Uuid, CachedTierDefinition>,
    last_refreshed: Instant,
}

impl TierCache {
    fn empty() -> Self {
        Self {
            tiers: HashMap::new(),
            last_refreshed: Instant::now() - CACHE_TTL - Duration::from_secs(1),
        }
    }

    fn is_stale(&self) -> bool {
        self.last_refreshed.elapsed() > CACHE_TTL
    }
}

/// DB-backed entitlement service with an in-memory tier definition cache.
pub struct EntitlementService {
    db: Arc<DbPool>,
    cache: Arc<RwLock<TierCache>>,
}

impl EntitlementService {
    pub fn new(db: Arc<DbPool>) -> Self {
        Self {
            db,
            cache: Arc::new(RwLock::new(TierCache::empty())),
        }
    }

    /// Ensure the cache is fresh, refreshing from DB if stale.
    async fn ensure_cache_fresh(&self) -> Result<()> {
        {
            let cache = self.cache.read().await;
            if !cache.is_stale() {
                return Ok(());
            }
        }
        EntitlementService::refresh_cache(self).await
    }

    /// Force-refresh the tier definitions cache (called on admin updates).
    pub async fn refresh_cache(&self) -> Result<()> {
        let rows = sqlx::query_as::<_, TierDefinitionRow>(
            "SELECT id, name, display_name, stripe_price_id, config FROM tier_definitions",
        )
        .fetch_all(self.db.as_ref())
        .await
        .context("failed to load tier definitions")?;

        let mut tiers = HashMap::with_capacity(rows.len());
        for row in rows {
            let config: TierConfig =
                serde_json::from_value(row.config).unwrap_or_default();

            tiers.insert(
                row.id,
                CachedTierDefinition {
                    name: row.name,
                    display_name: row.display_name,
                    stripe_price_id: row.stripe_price_id,
                    config,
                },
            );
        }

        let mut cache = self.cache.write().await;
        cache.tiers = tiers;
        cache.last_refreshed = Instant::now();
        Ok(())
    }

    /// Resolve tier config: read tier from cache, load overrides from DB, merge.
    pub async fn resolve(&self, organization_id: Uuid) -> Result<ResolvedTier> {
        self.ensure_cache_fresh().await?;

        let tier_definition_id: Uuid = sqlx::query_scalar(
            "SELECT tier_definition_id FROM organizations WHERE id = $1",
        )
        .bind(organization_id)
        .fetch_optional(self.db.as_ref())
        .await
        .context("failed to look up organization")?
        .ok_or_else(|| anyhow::anyhow!("organization {} not found", organization_id))?;

        let base = {
            let cache = self.cache.read().await;
            cache
                .tiers
                .get(&tier_definition_id)
                .cloned()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "tier definition {} not found in cache",
                        tier_definition_id
                    )
                })?
        };

        let overrides: Option<JsonValue> = sqlx::query_scalar(
            "SELECT config_overrides FROM tier_overrides WHERE organization_id = $1",
        )
        .bind(organization_id)
        .fetch_optional(self.db.as_ref())
        .await
        .context("failed to load tier overrides")?;

        let config = match overrides {
            Some(ov) if ov.is_object() && !ov.as_object().map_or(true, |m| m.is_empty()) => {
                merge_config(&base.config, &ov)
            }
            _ => base.config,
        };

        Ok(ResolvedTier {
            name: base.name,
            display_name: base.display_name,
            stripe_price_id: base.stripe_price_id,
            config,
        })
    }

    /// List all tier definitions from the cache (for admin / public tier listing).
    pub async fn list_tiers(&self) -> Result<Vec<ResolvedTier>> {
        self.ensure_cache_fresh().await?;
        let cache = self.cache.read().await;
        Ok(cache
            .tiers
            .values()
            .map(|t| ResolvedTier {
                name: t.name.clone(),
                display_name: t.display_name.clone(),
                stripe_price_id: t.stripe_price_id.clone(),
                config: t.config.clone(),
            })
            .collect())
    }
}

#[async_trait]
impl EntitlementChecker for EntitlementService {
    async fn get_config(
        &self,
        organization_id: Uuid,
    ) -> Result<ResolvedTier> {
        self.resolve(organization_id).await
    }

    async fn refresh_cache(&self) -> Result<()> {
        EntitlementService::refresh_cache(self).await
    }
}

#[derive(sqlx::FromRow)]
struct TierDefinitionRow {
    id: Uuid,
    name: String,
    display_name: String,
    stripe_price_id: Option<String>,
    config: JsonValue,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    fn sample_base() -> CachedTierDefinition {
        CachedTierDefinition {
            name: "starter".to_string(),
            display_name: "Starter".to_string(),
            stripe_price_id: Some("price_test1234567890ab".to_string()),
            config: TierConfig {
                platform: super::super::types::PlatformConfig {
                    sso: false,
                    audit_log: true,
                    priority_support: false,
                    max_projects: 3,
                },
                gateway: super::super::types::GatewayConfig {
                    requests_per_month: 250_000,
                    fee_percent: Decimal::new(3, 2),
                    moodeng_fee_percent: Decimal::ZERO,
                    agent_credits_included: 10000,
                    agent_credit_overage_usd: Decimal::new(20, 2),
                },
                prompt_hub: super::super::types::PromptHubConfig {
                    enabled: true,
                    prompt_versioning: true,
                    provider_fallback: true,
                    staged_rollouts: false,
                    max_prompts: 50,
                    max_prompt_versions: 50,
                    max_fallback_rules: 1,
                    max_parallel_rollouts: 5,
                    max_session_profiles: 5,
                    max_labels: 50,
                    session_evals_included: 5000,
                    session_eval_overage_usd: Decimal::new(3, 3),
                },
                watch: super::super::types::WatchConfig::default(),
                herd: super::super::types::HerdConfig { enabled: true },
            },
        }
    }

    #[test]
    fn merge_no_overrides() {
        let base = sample_base();
        let resolved = ResolvedTier {
            name: base.name,
            display_name: base.display_name,
            stripe_price_id: base.stripe_price_id,
            config: base.config,
        };
        assert!(!resolved.config.platform.sso);
        assert_eq!(resolved.config.platform.max_projects, 3);
        assert_eq!(resolved.config.gateway.fee_percent, Decimal::new(3, 2));
    }

    #[test]
    fn merge_config_replaces_scalars() {
        let base = sample_base();
        let overrides = serde_json::json!({
            "platform": { "sso": true, "max_projects": 100 }
        });
        let merged = merge_config(&base.config, &overrides);
        assert!(merged.platform.sso);
        assert_eq!(merged.platform.max_projects, 100);
        assert!(merged.platform.audit_log);
    }

    #[test]
    fn merge_config_preserves_other_sections() {
        let base = sample_base();
        let overrides = serde_json::json!({
            "gateway": { "fee_percent": "0.05" }
        });
        let merged = merge_config(&base.config, &overrides);
        assert_eq!(merged.gateway.fee_percent, Decimal::new(5, 2));
        assert!(merged.prompt_hub.enabled);
        assert_eq!(merged.prompt_hub.max_prompts, 50);
    }

    #[test]
    fn merge_config_multiple_sections() {
        let base = sample_base();
        let overrides = serde_json::json!({
            "platform": { "sso": true, "priority_support": true },
            "gateway": { "moodeng_fee_percent": "0.10" },
            "watch": { "enabled": true }
        });
        let merged = merge_config(&base.config, &overrides);
        assert!(merged.platform.sso);
        assert!(merged.platform.priority_support);
        assert_eq!(merged.gateway.moodeng_fee_percent, Decimal::new(10, 2));
        assert!(merged.watch.enabled);
        assert!(merged.herd.enabled);
    }
}
