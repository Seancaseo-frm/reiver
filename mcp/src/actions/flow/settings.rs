use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::actions::types::GatewaySettingsInput;
use crate::registry::ActionRegistry;

/// Merge a partial MCP settings object into the freshly fetched canonical
/// settings, including partial nested guardrail objects.
fn deep_merge(base: &mut serde_json::Value, overlay: &serde_json::Value) {
    if let (serde_json::Value::Object(base_map), serde_json::Value::Object(overlay_map)) =
        (base, overlay)
    {
        for (key, overlay_val) in overlay_map {
            if overlay_val.is_null() {
                continue;
            }
            if overlay_val.is_object() {
                if let Some(base_val) = base_map.get_mut(key) {
                    if base_val.is_object() {
                        deep_merge(base_val, overlay_val);
                        continue;
                    }
                }
            }
            base_map.insert(key.clone(), overlay_val.clone());
        }
    }
}

fn build_update_payload(
    patch: serde_json::Value,
    current: Option<&serde_json::Value>,
) -> anyhow::Result<serde_json::Value> {
    let patch = patch
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("gateway settings update must be an object"))?;
    let mut update = serde_json::Map::new();

    for (key, value) in patch {
        if value.is_null() {
            continue;
        }
        if matches!(key.as_str(), "guardrails" | "agent_soul") {
            let mut composite = current
                .and_then(|settings| settings.get(key))
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}));
            deep_merge(&mut composite, value);
            update.insert(key.clone(), composite);
        } else {
            update.insert(key.clone(), value.clone());
        }
    }

    Ok(serde_json::Value::Object(update))
}

// ── Get Gateway Settings ────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetGatewaySettingsInput {}

#[derive(Serialize)]
pub struct GetGatewaySettingsOutput {
    pub settings: serde_json::Value,
}

pub struct GetGatewaySettings;

#[async_trait]
impl PlatformAction for GetGatewaySettings {
    type Input = GetGatewaySettingsInput;
    type Output = GetGatewaySettingsOutput;

    fn name(&self) -> &'static str {
        "get_gateway_settings"
    }
    fn description(&self) -> &'static str {
        "Get the full LLM gateway configuration for the current project. Returns all settings \
         including introspection, fallback/retry behaviour, cost controls, rate limits, \
         guardrails, model preferences, agent configuration, session labels (taxonomy for \
         automatic session classification), and agent soul (personality, custom instructions, \
         key services, playbooks, behavioral rules). \
         Use this before update_gateway_settings to see current values."
    }
    fn required_scope(&self) -> String {
        "llm:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        _input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let resp = ctx
            .http
            .flow_get(&format!("/api/llm/settings?project_id={pid}"))
            .await?;
        let settings = resp.json().await?;
        Ok(GetGatewaySettingsOutput { settings })
    }
}

// ── Update Gateway Settings ─────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct UpdateGatewaySettingsInputWrapper {
    /// Settings to update. Only provided fields are changed; omitted fields keep their current values.
    pub settings: GatewaySettingsInput,
}

#[derive(Serialize)]
pub struct UpdateGatewaySettingsOutput {
    pub settings: serde_json::Value,
}

pub struct UpdateGatewaySettings;

#[async_trait]
impl PlatformAction for UpdateGatewaySettings {
    type Input = UpdateGatewaySettingsInputWrapper;
    type Output = UpdateGatewaySettingsOutput;

    fn name(&self) -> &'static str {
        "update_gateway_settings"
    }
    fn description(&self) -> &'static str {
        "Update LLM gateway settings for the current project. Changes affect all traffic \
         through the LLM gateway. This action should be explicitly requested by the user. \
         Only the provided fields are changed — omitted fields keep their current values. \
         Composite guardrail and agent-soul fields are merged with their current object; \
         unrelated top-level settings are never resubmitted. \
         Covers introspection, fallback/retry, cost controls, rate limits, guardrails, \
         model preferences, agent config, session labels (taxonomy for automatic session \
         classification), and agent soul (project description, tech context, custom \
         instructions, tone, key services, thresholds, known issues, playbooks, \
         always-do/never-do rules)."
    }
    fn required_scope(&self) -> String {
        "llm:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;

        let patch = serde_json::to_value(&input.settings)?;
        let needs_composite = patch.as_object().is_some_and(|map| {
            ["guardrails", "agent_soul"]
                .iter()
                .any(|key| map.get(*key).is_some_and(|value| !value.is_null()))
        });
        let current = if needs_composite {
            let response = ctx
                .http
                .flow_get(&format!("/api/llm/settings?project_id={pid}"))
                .await?;
            Some(response.json::<serde_json::Value>().await?)
        } else {
            None
        };
        let update = build_update_payload(patch, current.as_ref())?;

        let resp = ctx.http.flow_put("/api/llm/settings", &update).await?;
        let settings = resp.json().await?;
        Ok(UpdateGatewaySettingsOutput { settings })
    }
}

// ── Registration ─────────────────────────────────────────────────────

pub fn register(registry: &mut ActionRegistry) {
    registry.register(GetGatewaySettings);
    registry.register(UpdateGatewaySettings);
}

#[cfg(test)]
mod tests {
    use super::{build_update_payload, deep_merge};

    #[test]
    fn deep_merge_skips_null_overlay_fields() {
        let mut base = serde_json::json!({
            "fallback_enabled": true,
            "retry_enabled": true,
        });
        let overlay = serde_json::json!({
            "fallback_enabled": false,
            "retry_enabled": null,
        });
        deep_merge(&mut base, &overlay);
        assert_eq!(base["fallback_enabled"], false);
        assert_eq!(base["retry_enabled"], true);
    }

    #[test]
    fn deep_merge_preserves_nested_object_fields() {
        let mut base = serde_json::json!({
            "guardrails": {
                "trust_mode": "chatbot",
                "prompt_injection_detection": true,
                "spotlighting_enabled": true,
                "blocked_tools": ["send_email"],
            }
        });
        let overlay = serde_json::json!({
            "guardrails": {
                "trust_mode": "agent",
                "prompt_injection_detection": null,
                "block_exfiltration_urls": true,
            }
        });
        deep_merge(&mut base, &overlay);
        let guardrails = &base["guardrails"];
        assert_eq!(guardrails["trust_mode"], "agent");
        assert_eq!(guardrails["prompt_injection_detection"], true);
        assert_eq!(guardrails["spotlighting_enabled"], true);
        assert_eq!(guardrails["block_exfiltration_urls"], true);
        assert_eq!(guardrails["blocked_tools"][0], "send_email");
    }

    #[test]
    fn deep_merge_adds_new_top_level_keys() {
        let mut base = serde_json::json!({ "rate_limit_enabled": false });
        let overlay = serde_json::json!({ "rate_limit_rpm": 120 });
        deep_merge(&mut base, &overlay);
        assert_eq!(base["rate_limit_enabled"], false);
        assert_eq!(base["rate_limit_rpm"], 120);
    }

    #[test]
    fn deep_merge_replaces_non_object_with_object() {
        let mut base = serde_json::json!({ "guardrails": "legacy_string" });
        let overlay = serde_json::json!({ "guardrails": { "trust_mode": "agent" } });
        deep_merge(&mut base, &overlay);
        assert_eq!(base["guardrails"]["trust_mode"], "agent");
    }

    #[test]
    fn deep_merge_preserves_unsupplied_nested_guardrails() {
        let mut current = serde_json::json!({
            "guardrails": {
                "prompt_injection_detection": true,
                "blocked_tools": ["send_email"]
            }
        });
        deep_merge(
            &mut current,
            &serde_json::json!({ "guardrails": { "prompt_injection_detection": false } }),
        );
        assert_eq!(current["guardrails"]["prompt_injection_detection"], false);
        assert_eq!(current["guardrails"]["blocked_tools"][0], "send_email");
    }

    #[test]
    fn gateway_settings_input_serializes_partial_guardrails_correctly() {
        use crate::actions::types::{GatewaySettingsInput, GuardrailConfigInput};
        let input = GatewaySettingsInput {
            introspection_enabled: None,
            thinking_budget_tokens: None,
            fallback_enabled: Some(true),
            fallback_order: None,
            retry_enabled: None,
            retry_max_attempts: None,
            monthly_budget_usd: None,
            budget_alert_enabled: None,
            budget_hard_stop: None,
            per_request_limit_usd: None,
            rate_limit_enabled: None,
            rate_limit_rpm: None,
            session_budget_usd: None,
            agent_enabled: None,
            agent_scopes: None,
            guardrails: Some(GuardrailConfigInput {
                trust_mode: Some("agent".into()),
                blocked_input_topics: None,
                max_prompt_tokens: None,
                pii_block_on_detect: None,
                prompt_injection_detection: Some(true),
                spotlighting_enabled: None,
                mask_output_pii: None,
                blocked_output_topics: None,
                min_quality_score: None,
                blocked_tools: None,
                block_exfiltration_urls: None,
            }),
            session_profiles: None,
            session_labels: None,
            agent_soul: None,
        };

        let val = serde_json::to_value(&input).unwrap();
        let g = &val["guardrails"];
        assert_eq!(g["trust_mode"], "agent");
        assert_eq!(g["prompt_injection_detection"], true);
        assert!(
            g["spotlighting_enabled"].is_null(),
            "unset fields should be null"
        );
    }

    #[test]
    fn ordinary_field_payload_contains_only_requested_field() {
        let update = build_update_payload(
            serde_json::json!({
                "rate_limit_enabled": true,
                "retry_enabled": null,
                "guardrails": null
            }),
            None,
        )
        .unwrap();
        assert_eq!(update, serde_json::json!({ "rate_limit_enabled": true }));
    }

    #[test]
    fn nested_guardrail_payload_preserves_siblings_without_top_level_writeback() {
        let current = serde_json::json!({
            "retry_enabled": true,
            "guardrails": {
                "prompt_injection_detection": true,
                "blocked_tools": ["send_email"]
            }
        });
        let update = build_update_payload(
            serde_json::json!({
                "guardrails": { "prompt_injection_detection": false },
                "retry_enabled": null
            }),
            Some(&current),
        )
        .unwrap();
        assert_eq!(update.as_object().unwrap().len(), 1);
        assert_eq!(update["guardrails"]["prompt_injection_detection"], false);
        assert_eq!(update["guardrails"]["blocked_tools"][0], "send_email");
        assert!(update.get("retry_enabled").is_none());
    }

    #[test]
    fn nested_agent_soul_payload_preserves_siblings_without_top_level_writeback() {
        let current = serde_json::json!({
            "fallback_enabled": true,
            "agent_soul": {
                "project_description": "existing project",
                "tech_context": "Rust",
                "known_issues": ["legacy queue"]
            }
        });
        let update = build_update_payload(
            serde_json::json!({
                "agent_soul": { "tech_context": "Rust and Vue" },
                "fallback_enabled": null
            }),
            Some(&current),
        )
        .unwrap();
        assert_eq!(update.as_object().unwrap().len(), 1);
        assert_eq!(
            update["agent_soul"]["project_description"],
            "existing project"
        );
        assert_eq!(update["agent_soul"]["tech_context"], "Rust and Vue");
        assert_eq!(update["agent_soul"]["known_issues"][0], "legacy queue");
    }

    #[test]
    fn taxonomy_arrays_are_explicit_replacements() {
        let update = build_update_payload(
            serde_json::json!({
                "session_labels": [],
                "session_profiles": [{ "id": "profile-1" }],
                "agent_enabled": null
            }),
            None,
        )
        .unwrap();
        assert_eq!(update.as_object().unwrap().len(), 2);
        assert_eq!(update["session_labels"], serde_json::json!([]));
        assert_eq!(
            update["session_profiles"],
            serde_json::json!([{ "id": "profile-1" }])
        );
    }
}
