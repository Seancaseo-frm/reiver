//! Per-project gateway settings resolution and caching.

use uuid::Uuid;

use super::IntrospectionSettings;
use crate::app_state::FlowState;

pub(crate) async fn get_introspection_settings(
    state: &FlowState,
    project_id: Uuid,
) -> IntrospectionSettings {
    use crate::app_state::{CachedIntrospectionSettings, INTROSPECTION_SETTINGS_CACHE_TTL_SECS};

    const DEFAULT_BUDGET: u32 = 10_000;

    if let Some(cached) = state.introspection_settings_cache.get(&project_id) {
        if cached.expires_at > std::time::Instant::now() {
            return IntrospectionSettings {
                enabled: cached.enabled,
                budget_tokens: cached.budget_tokens,
                session_budget_usd: cached.session_budget_usd,
                guardrail_config: cached.guardrail_config.clone(),
                agent_enabled: cached.agent_enabled,
                agent_scopes: cached.agent_scopes.clone(),
                judge_sample_rate: cached.judge_sample_rate,
                default_fallback_models: cached.default_fallback_models.clone(),
                provider_preferences: cached.provider_preferences.clone(),
                fallback_enabled: cached.fallback_enabled,
                agent_soul: cached.agent_soul.clone(),
            };
        }
    }

    let rows: Vec<(String, String)> = match sqlx::query_as(
        r#"
        SELECT key, value
        FROM project_settings
        WHERE project_id = $1 AND key IN (
            'gateway_introspection_enabled',
            'gateway_thinking_budget_tokens',
            'gateway_session_budget_usd',
            'gateway_guardrails',
            'gateway_agent_enabled',
            'gateway_agent_scopes',
            'gateway_judge_sample_rate',
            'gateway_default_fallback_models',
            'gateway_provider_preferences',
            'gateway_fallback_enabled',
            'gateway_session_profiles',
            'gateway_agent_soul'
        )
        "#,
    )
    .bind(project_id)
    .fetch_all(state.db.as_ref())
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(
                project_id = %project_id,
                error = %e,
                "Failed to fetch gateway settings, using defaults"
            );
            return IntrospectionSettings {
                enabled: false,
                budget_tokens: DEFAULT_BUDGET,
                session_budget_usd: None,
                guardrail_config: crate::gateway::guardrails::GuardrailConfig::default(),
                agent_enabled: true,
                agent_scopes: default_agent_scopes(),
                judge_sample_rate: None,
                default_fallback_models: Vec::new(),
                provider_preferences: None,
                fallback_enabled: true,
                agent_soul: crate::api::llm_settings::AgentSoul::default(),
            };
        }
    };

    let mut enabled = false;
    let mut budget_tokens = DEFAULT_BUDGET;
    let mut session_budget_usd: Option<f64> = None;
    let mut guardrail_config = crate::gateway::guardrails::GuardrailConfig::default();
    let mut agent_enabled = true;
    let mut agent_scopes = default_agent_scopes();
    let mut judge_sample_rate: Option<f64> = None;
    let mut default_fallback_models: Vec<String> = Vec::new();
    let mut provider_preferences: Option<crate::gateway::types::ProviderPreferences> = None;
    let mut fallback_enabled = true;
    let mut agent_soul = crate::api::llm_settings::AgentSoul::default();
    for (key, value) in rows {
        match key.as_str() {
            "gateway_introspection_enabled" => enabled = value == "true",
            "gateway_thinking_budget_tokens" => {
                budget_tokens = value.parse().unwrap_or(DEFAULT_BUDGET);
            }
            "gateway_session_budget_usd" => {
                session_budget_usd = value.parse().ok().filter(|&v: &f64| v > 0.0);
            }
            "gateway_guardrails" => {
                if !value.is_empty() {
                    if let Ok(cfg) = serde_json::from_str(&value) {
                        guardrail_config = cfg;
                    }
                }
            }
            "gateway_agent_enabled" => agent_enabled = value == "true",
            "gateway_agent_scopes" => {
                if !value.is_empty() {
                    if let Ok(parsed) = serde_json::from_str::<Vec<String>>(&value) {
                        agent_scopes = parsed;
                    }
                }
            }
            "gateway_judge_sample_rate" => {
                judge_sample_rate = value.parse().ok().filter(|&v: &f64| v > 0.0);
            }
            "gateway_default_fallback_models" => {
                if !value.is_empty() {
                    if let Ok(models) = serde_json::from_str::<Vec<String>>(&value) {
                        default_fallback_models = models;
                    }
                }
            }
            "gateway_provider_preferences" => {
                if !value.is_empty() {
                    provider_preferences = serde_json::from_str(&value).ok();
                }
            }
            "gateway_fallback_enabled" => {
                fallback_enabled = value == "true";
            }
            "gateway_agent_soul" => {
                if !value.is_empty() {
                    if let Ok(parsed) = serde_json::from_str(&value) {
                        agent_soul = parsed;
                    }
                }
            }
            "gateway_session_profiles" => {}
            _ => {}
        }
    }

    state.introspection_settings_cache.insert(
        project_id,
        CachedIntrospectionSettings {
            enabled,
            budget_tokens,
            session_budget_usd,
            guardrail_config: guardrail_config.clone(),
            agent_enabled,
            agent_scopes: agent_scopes.clone(),
            judge_sample_rate,
            default_fallback_models: default_fallback_models.clone(),
            provider_preferences: provider_preferences.clone(),
            fallback_enabled,
            agent_soul: agent_soul.clone(),
            expires_at: std::time::Instant::now()
                + std::time::Duration::from_secs(INTROSPECTION_SETTINGS_CACHE_TTL_SECS),
        },
    );

    IntrospectionSettings {
        enabled,
        budget_tokens,
        session_budget_usd,
        guardrail_config,
        agent_enabled,
        agent_scopes,
        judge_sample_rate,
        default_fallback_models,
        provider_preferences,
        fallback_enabled,
        agent_soul,
    }
}

fn default_agent_scopes() -> Vec<String> {
    reiver_mcp::scope::READ_ONLY_SCOPES
        .iter()
        .map(|s| s.to_string())
        .collect()
}
