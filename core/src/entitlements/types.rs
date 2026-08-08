use rust_decimal::Decimal;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Serde default for limit fields: -1 means unlimited.
/// Used during the deployment window before a migration seeds the actual value.
fn unlimited() -> i64 {
    -1
}

/// Tier configuration. This struct IS the schema: adding a field here
/// (with `#[serde(default)]`) and a migration to seed the default value
/// is all that's needed to introduce new tier-configurable behavior.
///
/// Sections map to product/domain areas. Each section groups its own
/// boolean toggles, integer limits, percentage rates, and unit prices.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TierConfig {
    #[serde(default)]
    pub platform: PlatformConfig,
    #[serde(default)]
    pub gateway: GatewayConfig,
    #[serde(default)]
    pub prompt_hub: PromptHubConfig,
    #[serde(default)]
    pub watch: WatchConfig,
    #[serde(default)]
    pub herd: HerdConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
pub struct PlatformConfig {
    #[serde(default)]
    pub sso: bool,
    #[serde(default)]
    pub audit_log: bool,
    #[serde(default)]
    pub priority_support: bool,
    /// Maximum number of projects. -1 = unlimited.
    #[serde(default)]
    pub max_projects: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GatewayConfig {
    /// Maximum gateway requests per month. -1 = unlimited.
    #[serde(default)]
    pub requests_per_month: i64,
    /// Platform fee on BYOK gateway traffic (decimal fraction: 0.03 = 3%).
    #[serde(default)]
    pub fee_percent: Decimal,
    /// MooDeng agent fee (decimal fraction: 0.10 = 10%). Charged INSTEAD of fee_percent for MooDeng traffic.
    #[serde(default)]
    pub moodeng_fee_percent: Decimal,
    /// Included agent credits per billing period. -1 = unlimited.
    #[serde(default = "unlimited")]
    pub agent_credits_included: i64,
    /// Overage price per agent credit above allotment (USD).
    #[serde(default)]
    pub agent_credit_overage_usd: Decimal,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            requests_per_month: 0,
            fee_percent: Decimal::ZERO,
            moodeng_fee_percent: Decimal::ZERO,
            agent_credits_included: -1,
            agent_credit_overage_usd: Decimal::ZERO,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PromptHubConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub prompt_versioning: bool,
    #[serde(default)]
    pub provider_fallback: bool,
    #[serde(default)]
    pub staged_rollouts: bool,
    #[serde(default)]
    pub max_prompts: i64,
    #[serde(default)]
    pub max_prompt_versions: i64,
    /// Maximum fallback rules. -1 = unlimited.
    #[serde(default)]
    pub max_fallback_rules: i64,
    #[serde(default)]
    pub max_parallel_rollouts: i64,
    #[serde(default)]
    pub max_session_profiles: i64,
    /// Maximum distinct session label types. -1 = unlimited.
    #[serde(default = "unlimited")]
    pub max_labels: i64,
    /// Included session evaluations per billing period. -1 = unlimited.
    #[serde(default = "unlimited")]
    pub session_evals_included: i64,
    /// Overage price per session evaluation (USD).
    #[serde(default)]
    pub session_eval_overage_usd: Decimal,
}

impl Default for PromptHubConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            prompt_versioning: false,
            provider_fallback: false,
            staged_rollouts: false,
            max_prompts: 0,
            max_prompt_versions: 0,
            max_fallback_rules: 0,
            max_parallel_rollouts: 0,
            max_session_profiles: 0,
            max_labels: -1,
            session_evals_included: -1,
            session_eval_overage_usd: Decimal::ZERO,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WatchConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub webhook_alerts: bool,
    #[serde(default)]
    pub slack_alerts: bool,
    /// Included ingestion GB per billing period. -1 = unlimited.
    #[serde(default = "unlimited")]
    pub ingestion_gb_included: i64,
    /// Overage price per GB above allotment (USD).
    #[serde(default)]
    pub traces_logs_per_gb_usd: Decimal,
    /// Cost per million metric data points (USD).
    #[serde(default)]
    pub metrics_per_million_usd: Decimal,
}

impl Default for WatchConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            webhook_alerts: false,
            slack_alerts: false,
            ingestion_gb_included: -1,
            traces_logs_per_gb_usd: Decimal::ZERO,
            metrics_per_million_usd: Decimal::ZERO,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
pub struct HerdConfig {
    #[serde(default)]
    pub enabled: bool,
}

impl Default for TierConfig {
    fn default() -> Self {
        Self {
            platform: PlatformConfig::default(),
            gateway: GatewayConfig::default(),
            prompt_hub: PromptHubConfig::default(),
            watch: WatchConfig::default(),
            herd: HerdConfig::default(),
        }
    }
}

/// Resolved tier for an organization (tier definition + overrides merged).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedTier {
    pub name: String,
    pub display_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stripe_price_id: Option<String>,
    pub config: TierConfig,
}

/// Product identifiers for proxy routing dispatch.
/// Not stored in the DB -- used only to map incoming requests to the
/// corresponding `enabled` flag in TierConfig.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Product {
    PromptHub,
    Watch,
    Herd,
}

impl TierConfig {
    pub fn is_product_enabled(&self, product: Product) -> bool {
        match product {
            Product::PromptHub => self.prompt_hub.enabled,
            Product::Watch => self.watch.enabled,
            Product::Herd => self.herd.enabled,
        }
    }
}

/// Returns JSON Schema for TierConfig, used by the admin API so the
/// frontend dynamically renders the tier editor.
pub fn tier_schema() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(TierConfig)).unwrap_or_default()
}

/// Deep-merge `overrides` into `base`. Override values replace base values;
/// nested objects are merged recursively.
pub fn deep_merge(base: &mut serde_json::Value, overrides: &serde_json::Value) {
    use serde_json::Value;
    match (base, overrides) {
        (Value::Object(base_map), Value::Object(override_map)) => {
            for (k, v) in override_map {
                let entry = base_map.entry(k.clone()).or_insert(Value::Null);
                deep_merge(entry, v);
            }
        }
        (base, over) => {
            *base = over.clone();
        }
    }
}

/// Merge a base TierConfig with sparse JSON overrides.
pub fn merge_config(base: &TierConfig, overrides: &serde_json::Value) -> TierConfig {
    let mut base_json = serde_json::to_value(base).unwrap_or_default();
    deep_merge(&mut base_json, overrides);
    serde_json::from_value(base_json).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tier_config_serde_roundtrip() {
        let config = TierConfig {
            platform: PlatformConfig {
                sso: true,
                audit_log: true,
                priority_support: false,
                max_projects: 10,
            },
            gateway: GatewayConfig {
                requests_per_month: 2_000_000,
                fee_percent: Decimal::new(3, 2),
                moodeng_fee_percent: Decimal::new(10, 2),
                agent_credits_included: 10000,
                agent_credit_overage_usd: Decimal::new(20, 2),
            },
            prompt_hub: PromptHubConfig {
                enabled: true,
                prompt_versioning: true,
                provider_fallback: true,
                staged_rollouts: true,
                max_prompts: 500,
                max_prompt_versions: 500,
                max_fallback_rules: -1,
                max_parallel_rollouts: 50,
                max_session_profiles: 50,
                max_labels: 5,
                session_evals_included: 5000,
                session_eval_overage_usd: Decimal::new(3, 3),
            },
            watch: WatchConfig {
                enabled: true,
                webhook_alerts: true,
                slack_alerts: true,
                ingestion_gb_included: 200,
                traces_logs_per_gb_usd: Decimal::new(25, 2),
                metrics_per_million_usd: Decimal::new(10, 2),
            },
            herd: HerdConfig { enabled: true },
        };

        let json = serde_json::to_string(&config).unwrap();
        let parsed: TierConfig = serde_json::from_str(&json).unwrap();
        assert!(parsed.platform.sso);
        assert_eq!(parsed.gateway.fee_percent, Decimal::new(3, 2));
        assert_eq!(parsed.gateway.agent_credits_included, 10000);
        assert_eq!(parsed.prompt_hub.max_prompts, 500);
        assert_eq!(parsed.prompt_hub.session_evals_included, 5000);
        assert!(parsed.watch.enabled);
        assert_eq!(parsed.watch.ingestion_gb_included, 200);
        assert!(parsed.herd.enabled);
    }

    #[test]
    fn tier_config_defaults_on_missing_fields() {
        let json = r#"{}"#;
        let parsed: TierConfig = serde_json::from_str(json).unwrap();
        assert!(!parsed.platform.sso);
        assert!(!parsed.prompt_hub.enabled);
        assert_eq!(parsed.gateway.fee_percent, Decimal::ZERO);
        assert_eq!(parsed.platform.max_projects, 0);
        // Limit fields default to unlimited (-1) for deployment safety
        assert_eq!(parsed.gateway.agent_credits_included, -1);
        assert_eq!(parsed.prompt_hub.session_evals_included, -1);
        assert_eq!(parsed.prompt_hub.max_labels, -1);
        assert_eq!(parsed.watch.ingestion_gb_included, -1);
    }

    #[test]
    fn resolved_tier_serde_roundtrip() {
        let tier = ResolvedTier {
            name: "starter".to_string(),
            display_name: "Starter".to_string(),
            stripe_price_id: Some("price_test123".to_string()),
            config: TierConfig::default(),
        };

        let json = serde_json::to_string(&tier).unwrap();
        let parsed: ResolvedTier = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.name, "starter");
        assert_eq!(parsed.stripe_price_id, Some("price_test123".to_string()));
    }

    #[test]
    fn product_enabled_check() {
        let mut config = TierConfig::default();
        assert!(!config.is_product_enabled(Product::Watch));
        config.watch.enabled = true;
        assert!(config.is_product_enabled(Product::Watch));
    }

    #[test]
    fn deep_merge_replaces_scalars() {
        let base = TierConfig::default();
        let overrides = serde_json::json!({
            "platform": { "sso": true, "max_projects": 99 }
        });
        let merged = merge_config(&base, &overrides);
        assert!(merged.platform.sso);
        assert_eq!(merged.platform.max_projects, 99);
        assert!(!merged.platform.audit_log);
    }

    #[test]
    fn deep_merge_preserves_unmentioned_sections() {
        let mut base = TierConfig::default();
        base.watch.enabled = true;
        let overrides = serde_json::json!({
            "gateway": { "fee_percent": "0.05" }
        });
        let merged = merge_config(&base, &overrides);
        assert!(merged.watch.enabled);
        assert_eq!(merged.gateway.fee_percent, Decimal::new(5, 2));
    }

    #[test]
    fn tier_schema_returns_valid_json_schema() {
        let schema = tier_schema();
        assert!(schema.get("properties").is_some());
        let props = schema["properties"].as_object().unwrap();
        assert!(props.contains_key("platform"));
        assert!(props.contains_key("gateway"));
        assert!(props.contains_key("prompt_hub"));
        assert!(props.contains_key("watch"));
        assert!(props.contains_key("herd"));
    }

    #[test]
    fn billing_fields_in_respective_sections() {
        let config = TierConfig {
            gateway: GatewayConfig {
                agent_credits_included: 10000,
                agent_credit_overage_usd: Decimal::new(20, 2),
                ..Default::default()
            },
            prompt_hub: PromptHubConfig {
                max_labels: 5,
                session_evals_included: 5000,
                session_eval_overage_usd: Decimal::new(3, 3),
                ..Default::default()
            },
            watch: WatchConfig {
                ingestion_gb_included: 200,
                traces_logs_per_gb_usd: Decimal::new(25, 2),
                ..Default::default()
            },
            ..Default::default()
        };

        let json = serde_json::to_value(&config).unwrap();
        let parsed: TierConfig = serde_json::from_value(json).unwrap();

        assert_eq!(parsed.gateway.agent_credits_included, 10000);
        assert_eq!(parsed.gateway.agent_credit_overage_usd, Decimal::new(20, 2));
        assert_eq!(parsed.prompt_hub.max_labels, 5);
        assert_eq!(parsed.prompt_hub.session_evals_included, 5000);
        assert_eq!(parsed.prompt_hub.session_eval_overage_usd, Decimal::new(3, 3));
        assert_eq!(parsed.watch.ingestion_gb_included, 200);
        assert_eq!(parsed.watch.traces_logs_per_gb_usd, Decimal::new(25, 2));
    }

    #[test]
    fn negative_one_means_unlimited() {
        let json = serde_json::json!({
            "gateway": { "agent_credits_included": -1 },
            "prompt_hub": { "session_evals_included": -1, "max_labels": -1 },
            "watch": { "ingestion_gb_included": -1 }
        });
        let config: TierConfig = serde_json::from_value(json).unwrap();
        assert_eq!(config.gateway.agent_credits_included, -1);
        assert_eq!(config.prompt_hub.session_evals_included, -1);
        assert_eq!(config.prompt_hub.max_labels, -1);
        assert_eq!(config.watch.ingestion_gb_included, -1);
    }

    #[test]
    fn merge_overrides_billing_fields() {
        let mut base = TierConfig::default();
        base.gateway.agent_credits_included = 100;
        base.watch.ingestion_gb_included = 50;

        let overrides = serde_json::json!({
            "gateway": { "agent_credits_included": 10000 }
        });
        let merged = merge_config(&base, &overrides);
        assert_eq!(merged.gateway.agent_credits_included, 10000);
        assert_eq!(merged.watch.ingestion_gb_included, 50);
    }
}
