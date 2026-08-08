#[cfg(any(test, feature = "test-utils"))]
use std::collections::HashMap;
#[cfg(any(test, feature = "test-utils"))]
use std::sync::Arc;

#[cfg(any(test, feature = "test-utils"))]
use anyhow::Result;
#[cfg(any(test, feature = "test-utils"))]
use async_trait::async_trait;
#[cfg(any(test, feature = "test-utils"))]
use tokio::sync::RwLock;
#[cfg(any(test, feature = "test-utils"))]
use uuid::Uuid;

#[cfg(any(test, feature = "test-utils"))]
use super::checker::EntitlementChecker;
#[cfg(any(test, feature = "test-utils"))]
use super::types::{ResolvedTier, TierConfig};

#[cfg(any(test, feature = "test-utils"))]
pub struct MockEntitlementChecker {
    tiers: Arc<RwLock<HashMap<Uuid, ResolvedTier>>>,
}

#[cfg(any(test, feature = "test-utils"))]
impl MockEntitlementChecker {
    pub fn new() -> Self {
        Self {
            tiers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Set the full resolved tier for an org.
    pub async fn set_tier(&self, org_id: Uuid, tier: ResolvedTier) {
        self.tiers.write().await.insert(org_id, tier);
    }

    /// Set the config for an org (creates a default tier if missing).
    pub async fn set_config(&self, org_id: Uuid, config: TierConfig) {
        let mut map = self.tiers.write().await;
        let tier = map.entry(org_id).or_insert_with(default_tier);
        tier.config = config;
    }

    /// Mutate the config for an org via a closure.
    pub async fn update_config<F: FnOnce(&mut TierConfig)>(&self, org_id: Uuid, f: F) {
        let mut map = self.tiers.write().await;
        let tier = map.entry(org_id).or_insert_with(default_tier);
        f(&mut tier.config);
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl Default for MockEntitlementChecker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(any(test, feature = "test-utils"))]
#[async_trait]
impl EntitlementChecker for MockEntitlementChecker {
    async fn get_config(
        &self,
        organization_id: Uuid,
    ) -> Result<ResolvedTier> {
        let map = self.tiers.read().await;
        map.get(&organization_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("no tier configured for org {}", organization_id))
    }
}

#[cfg(any(test, feature = "test-utils"))]
fn default_tier() -> ResolvedTier {
    ResolvedTier {
        name: "mock".to_string(),
        display_name: "Mock".to_string(),
        stripe_price_id: None,
        config: TierConfig::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entitlements::types::Product;

    #[tokio::test]
    async fn mock_get_config_returns_set_tier() {
        let mock = MockEntitlementChecker::new();
        let org = Uuid::new_v4();
        mock.update_config(org, |c| {
            c.prompt_hub.enabled = true;
            c.platform.sso = true;
            c.platform.max_projects = 10;
        })
        .await;

        let tier = mock.get_config(org).await.unwrap();
        assert!(tier.config.prompt_hub.enabled);
        assert!(tier.config.platform.sso);
        assert_eq!(tier.config.platform.max_projects, 10);
    }

    #[tokio::test]
    async fn mock_unknown_org_returns_error() {
        let mock = MockEntitlementChecker::new();
        let unknown = Uuid::new_v4();
        assert!(mock.get_config(unknown).await.is_err());
    }

    #[tokio::test]
    async fn mock_product_enabled_check() {
        let mock = MockEntitlementChecker::new();
        let org = Uuid::new_v4();
        mock.update_config(org, |c| {
            c.watch.enabled = true;
        })
        .await;

        let tier = mock.get_config(org).await.unwrap();
        assert!(tier.config.is_product_enabled(Product::Watch));
        assert!(!tier.config.is_product_enabled(Product::PromptHub));
    }

    #[tokio::test]
    async fn mock_update_config_preserves_existing() {
        let mock = MockEntitlementChecker::new();
        let org = Uuid::new_v4();
        mock.update_config(org, |c| c.watch.enabled = true).await;
        mock.update_config(org, |c| c.platform.sso = true).await;

        let tier = mock.get_config(org).await.unwrap();
        assert!(tier.config.watch.enabled);
        assert!(tier.config.platform.sso);
    }
}
