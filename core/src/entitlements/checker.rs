use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;

use super::types::ResolvedTier;

/// Trait for resolving tier configuration. Mockable for tests.
///
/// Callers access config fields directly on the returned `ResolvedTier`:
///   - Product gates: `config.watch.enabled`, `config.prompt_hub.enabled`
///   - Feature gates: `config.platform.sso`, `config.watch.slack_alerts`
///   - Quota checks: `config.platform.max_projects >= current_count`
///   - Rate lookups: `config.gateway.fee_percent`
///   - Pricing: `config.watch.traces_logs_per_gb_usd`
#[async_trait]
pub trait EntitlementChecker: Send + Sync {
    /// Get the fully resolved tier for an organization (base tier + overrides merged).
    async fn get_config(
        &self,
        organization_id: Uuid,
    ) -> Result<ResolvedTier>;

    /// Force-refresh the tier definitions cache. Called after admin updates.
    /// No-op for mock implementations.
    async fn refresh_cache(&self) -> Result<()> {
        Ok(())
    }
}

/// No-op entitlement checker that returns unlimited for everything.
/// Used by the standalone MCP binary which has no DB.
pub struct UnlimitedEntitlements;

#[async_trait]
impl EntitlementChecker for UnlimitedEntitlements {
    async fn get_config(&self, _organization_id: Uuid) -> Result<ResolvedTier> {
        Ok(ResolvedTier {
            name: "unlimited".to_string(),
            display_name: "Unlimited".to_string(),
            stripe_price_id: None,
            config: super::types::TierConfig::default(),
        })
    }
}
