//! Integration tests for entitlement enforcement.
//!
//! These tests verify that the `MockEntitlementChecker` correctly resolves
//! tier config for all enforced entitlements. They test the checker layer
//! directly since spinning up a full `WebsiteState` requires DB/Redis/ClickHouse.

#[cfg(feature = "test-utils")]
mod enforcement {
    use reiver_core::entitlements::checker::EntitlementChecker;
    use reiver_core::entitlements::types::{Product, ResolvedTier, TierConfig};
    use reiver_core::entitlements::MockEntitlementChecker;
    use rust_decimal::Decimal;
    use uuid::Uuid;

    fn test_org() -> Uuid {
        Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap()
    }

    fn full_tier() -> ResolvedTier {
        ResolvedTier {
            name: "scale".to_string(),
            display_name: "Scale".to_string(),
            stripe_price_id: None,
            config: TierConfig {
                platform: reiver_core::entitlements::types::PlatformConfig {
                    sso: true,
                    audit_log: true,
                    priority_support: true,
                    max_projects: 10,
                },
                gateway: reiver_core::entitlements::types::GatewayConfig {
                    requests_per_month: 2_000_000,
                    fee_percent: Decimal::new(1, 2),
                    moodeng_fee_percent: Decimal::new(5, 2),
                    agent_credits_included: -1,
                    agent_credit_overage_usd: Decimal::ZERO,
                },
                prompt_hub: reiver_core::entitlements::types::PromptHubConfig {
                    enabled: true,
                    prompt_versioning: true,
                    provider_fallback: true,
                    staged_rollouts: true,
                    max_prompts: 500,
                    max_prompt_versions: 500,
                    max_fallback_rules: -1,
                    max_parallel_rollouts: 50,
                    max_session_profiles: 50,
                    max_labels: 500,
                    session_evals_included: -1,
                    session_eval_overage_usd: Decimal::ZERO,
                },
                watch: reiver_core::entitlements::types::WatchConfig {
                    enabled: true,
                    webhook_alerts: true,
                    slack_alerts: true,
                    ingestion_gb_included: -1,
                    traces_logs_per_gb_usd: Decimal::new(20, 2),
                    metrics_per_million_usd: Decimal::new(10, 2),
                },
                herd: reiver_core::entitlements::types::HerdConfig { enabled: true },
            },
        }
    }

    fn free_tier() -> ResolvedTier {
        ResolvedTier {
            name: "free".to_string(),
            display_name: "Free".to_string(),
            stripe_price_id: None,
            config: TierConfig {
                platform: reiver_core::entitlements::types::PlatformConfig {
                    sso: false,
                    audit_log: false,
                    priority_support: false,
                    max_projects: 1,
                },
                gateway: reiver_core::entitlements::types::GatewayConfig {
                    requests_per_month: 50_000,
                    fee_percent: Decimal::ZERO,
                    moodeng_fee_percent: Decimal::ZERO,
                    agent_credits_included: -1,
                    agent_credit_overage_usd: Decimal::ZERO,
                },
                prompt_hub: reiver_core::entitlements::types::PromptHubConfig {
                    enabled: true,
                    prompt_versioning: false,
                    provider_fallback: false,
                    staged_rollouts: false,
                    max_prompts: 10,
                    max_prompt_versions: 5,
                    max_fallback_rules: 0,
                    max_parallel_rollouts: 1,
                    max_session_profiles: 1,
                    max_labels: 5,
                    session_evals_included: -1,
                    session_eval_overage_usd: Decimal::ZERO,
                },
                watch: reiver_core::entitlements::types::WatchConfig::default(),
                herd: reiver_core::entitlements::types::HerdConfig { enabled: true },
            },
        }
    }

    // ── Product enforcement ──────────────────────────────────────────────

    #[tokio::test]
    async fn watch_product_denied_on_free_tier() {
        let mock = MockEntitlementChecker::new();
        let org = test_org();
        mock.set_tier(org, free_tier()).await;

        let tier = mock.get_config(org).await.unwrap();
        assert!(
            !tier.config.is_product_enabled(Product::Watch),
            "Watch should be denied on free tier"
        );
    }

    #[tokio::test]
    async fn watch_product_allowed_on_scale_tier() {
        let mock = MockEntitlementChecker::new();
        let org = test_org();
        mock.set_tier(org, full_tier()).await;

        let tier = mock.get_config(org).await.unwrap();
        assert!(
            tier.config.is_product_enabled(Product::Watch),
            "Watch should be allowed on scale tier"
        );
    }

    #[tokio::test]
    async fn herd_product_allowed_on_free_tier() {
        let mock = MockEntitlementChecker::new();
        let org = test_org();
        mock.set_tier(org, free_tier()).await;

        let tier = mock.get_config(org).await.unwrap();
        assert!(
            tier.config.is_product_enabled(Product::Herd),
            "Herd should be allowed on free tier"
        );
    }

    #[tokio::test]
    async fn prompt_hub_allowed_on_free_tier() {
        let mock = MockEntitlementChecker::new();
        let org = test_org();
        mock.set_tier(org, free_tier()).await;

        let tier = mock.get_config(org).await.unwrap();
        assert!(
            tier.config.is_product_enabled(Product::PromptHub),
            "PromptHub should be allowed on free tier"
        );
    }

    // ── Feature enforcement (SSO) ────────────────────────────────────────

    #[tokio::test]
    async fn sso_feature_denied_on_free_tier() {
        let mock = MockEntitlementChecker::new();
        let org = test_org();
        mock.set_tier(org, free_tier()).await;

        let tier = mock.get_config(org).await.unwrap();
        assert!(
            !tier.config.platform.sso,
            "SSO should be denied on free tier"
        );
    }

    #[tokio::test]
    async fn sso_feature_allowed_on_scale_tier() {
        let mock = MockEntitlementChecker::new();
        let org = test_org();
        mock.set_tier(org, full_tier()).await;

        let tier = mock.get_config(org).await.unwrap();
        assert!(
            tier.config.platform.sso,
            "SSO should be allowed on scale tier"
        );
    }

    // ── Feature enforcement (Audit Log) ──────────────────────────────────

    #[tokio::test]
    async fn audit_log_denied_on_free_tier() {
        let mock = MockEntitlementChecker::new();
        let org = test_org();
        mock.set_tier(org, free_tier()).await;

        let tier = mock.get_config(org).await.unwrap();
        assert!(
            !tier.config.platform.audit_log,
            "AuditLog should be denied on free tier"
        );
    }

    #[tokio::test]
    async fn audit_log_allowed_on_scale_tier() {
        let mock = MockEntitlementChecker::new();
        let org = test_org();
        mock.set_tier(org, full_tier()).await;

        let tier = mock.get_config(org).await.unwrap();
        assert!(
            tier.config.platform.audit_log,
            "AuditLog should be allowed on scale tier"
        );
    }

    // ── Quota enforcement (MaxProjects) ──────────────────────────────────

    #[tokio::test]
    async fn max_projects_denied_when_at_limit() {
        let mock = MockEntitlementChecker::new();
        let org = test_org();
        mock.set_tier(org, free_tier()).await;

        let tier = mock.get_config(org).await.unwrap();
        let limit = tier.config.platform.max_projects;
        let current = 1i64;
        assert!(
            limit >= 0 && current >= limit,
            "Should deny when at max_projects limit"
        );
    }

    #[tokio::test]
    async fn max_projects_allowed_when_under_limit() {
        let mock = MockEntitlementChecker::new();
        let org = test_org();
        mock.set_tier(org, free_tier()).await;

        let tier = mock.get_config(org).await.unwrap();
        let limit = tier.config.platform.max_projects;
        let current = 0i64;
        assert!(
            limit < 0 || current < limit,
            "Should allow when under max_projects limit"
        );
    }

    #[tokio::test]
    async fn unlimited_quota_always_allowed() {
        let mock = MockEntitlementChecker::new();
        let org = test_org();
        mock.set_tier(org, full_tier()).await;

        let tier = mock.get_config(org).await.unwrap();
        let limit = tier.config.prompt_hub.max_fallback_rules;
        assert!(limit < 0, "Unlimited quota (-1) should always allow");
    }

    // ── Downgrade scenario: Scale to Free ────────────────────────────────

    #[tokio::test]
    async fn downgrade_denies_watch_product() {
        let mock = MockEntitlementChecker::new();
        let org = test_org();

        mock.set_tier(org, full_tier()).await;
        let tier = mock.get_config(org).await.unwrap();
        assert!(tier.config.is_product_enabled(Product::Watch));

        mock.set_tier(org, free_tier()).await;
        let tier = mock.get_config(org).await.unwrap();
        assert!(!tier.config.is_product_enabled(Product::Watch));
    }

    #[tokio::test]
    async fn downgrade_denies_sso_feature() {
        let mock = MockEntitlementChecker::new();
        let org = test_org();

        mock.set_tier(org, full_tier()).await;
        let tier = mock.get_config(org).await.unwrap();
        assert!(tier.config.platform.sso);

        mock.set_tier(org, free_tier()).await;
        let tier = mock.get_config(org).await.unwrap();
        assert!(!tier.config.platform.sso);
    }

    #[tokio::test]
    async fn downgrade_blocks_project_creation_over_new_limit() {
        let mock = MockEntitlementChecker::new();
        let org = test_org();

        mock.set_tier(org, full_tier()).await;
        let tier = mock.get_config(org).await.unwrap();
        assert!(tier.config.platform.max_projects < 0 || 5 < tier.config.platform.max_projects);

        mock.set_tier(org, free_tier()).await;
        let tier = mock.get_config(org).await.unwrap();
        assert!(tier.config.platform.max_projects >= 0 && 5 >= tier.config.platform.max_projects);
    }

    // ── Override behavior ────────────────────────────────────────────────

    #[tokio::test]
    async fn override_can_grant_sso_on_free_tier() {
        let mock = MockEntitlementChecker::new();
        let org = test_org();

        let mut tier = free_tier();
        tier.config.platform.sso = true;
        mock.set_tier(org, tier).await;

        let result = mock.get_config(org).await.unwrap();
        assert!(
            result.config.platform.sso,
            "SSO override should grant access on free tier"
        );
    }

    #[tokio::test]
    async fn override_can_add_watch_product() {
        let mock = MockEntitlementChecker::new();
        let org = test_org();

        let mut tier = free_tier();
        tier.config.watch.enabled = true;
        mock.set_tier(org, tier).await;

        let result = mock.get_config(org).await.unwrap();
        assert!(
            result.config.is_product_enabled(Product::Watch),
            "Watch override should grant access on free tier"
        );
    }

    // ── Unknown org returns error ────────────────────────────────────────

    #[tokio::test]
    async fn unknown_org_returns_error() {
        let mock = MockEntitlementChecker::new();
        let unknown = Uuid::new_v4();

        assert!(mock.get_config(unknown).await.is_err());
    }

    // ── refresh_cache is a no-op for mock ────────────────────────────────

    #[tokio::test]
    async fn mock_refresh_cache_is_noop() {
        let mock = MockEntitlementChecker::new();
        assert!(mock.refresh_cache().await.is_ok());
    }

    // ── Rate / pricing lookups ───────────────────────────────────────────

    #[tokio::test]
    async fn rates_available_on_scale_tier() {
        let mock = MockEntitlementChecker::new();
        let org = test_org();
        mock.set_tier(org, full_tier()).await;

        let tier = mock.get_config(org).await.unwrap();
        assert_eq!(tier.config.gateway.fee_percent, Decimal::new(1, 2));
        assert_eq!(tier.config.gateway.moodeng_fee_percent, Decimal::new(5, 2));
    }

    #[tokio::test]
    async fn rates_zero_on_free_tier() {
        let mock = MockEntitlementChecker::new();
        let org = test_org();
        mock.set_tier(org, free_tier()).await;

        let tier = mock.get_config(org).await.unwrap();
        assert_eq!(tier.config.gateway.fee_percent, Decimal::ZERO);
        assert_eq!(tier.config.gateway.moodeng_fee_percent, Decimal::ZERO);
    }

    #[tokio::test]
    async fn tier_aware_fee_rate_lookup() {
        let mock = MockEntitlementChecker::new();
        let org = test_org();
        mock.set_tier(org, full_tier()).await;

        let rate = reiver_core::billing::credits::get_gateway_fee_rate(&mock, org)
            .await
            .unwrap();
        assert_eq!(rate, Decimal::new(1, 2));

        let moodeng_rate = reiver_core::billing::credits::get_moodeng_fee_rate(&mock, org)
            .await
            .unwrap();
        assert_eq!(moodeng_rate, Decimal::new(5, 2));
    }

    #[tokio::test]
    async fn watch_pricing_available() {
        let mock = MockEntitlementChecker::new();
        let org = test_org();
        mock.set_tier(org, full_tier()).await;

        let tl = reiver_core::billing::credits::get_watch_traces_logs_price(&mock, org)
            .await
            .unwrap();
        assert_eq!(tl, Decimal::new(20, 2));

        let m = reiver_core::billing::credits::get_watch_metrics_price(&mock, org)
            .await
            .unwrap();
        assert_eq!(m, Decimal::new(10, 2));
    }

    #[tokio::test]
    async fn fee_rate_errors_for_unknown_org() {
        let mock = MockEntitlementChecker::new();
        let unknown = Uuid::new_v4();

        assert!(
            reiver_core::billing::credits::get_gateway_fee_rate(&mock, unknown)
                .await
                .is_err()
        );
    }
}
