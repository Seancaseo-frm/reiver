use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::actions::resolve_slot;
use crate::actions::types::LlmProvider;
use crate::registry::ActionRegistry;

// ── List Integrations ───────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListIntegrationsInput {}

#[derive(Serialize)]
pub struct ListIntegrationsOutput {
    pub integrations: serde_json::Value,
}

pub struct ListIntegrations;

#[async_trait]
impl PlatformAction for ListIntegrations {
    type Input = ListIntegrationsInput;
    type Output = ListIntegrationsOutput;

    fn name(&self) -> &'static str {
        "list_integrations"
    }
    fn description(&self) -> &'static str {
        "List all LLM provider integrations for the project"
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
            .flow_get(&format!("/api/llm/integrations?project_id={pid}"))
            .await?;
        let integrations = resp.json().await?;
        Ok(ListIntegrationsOutput { integrations })
    }
}

// ── Create Secret Slot ──────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct CreateSecretSlotInput {
    /// Human-readable label for what the secret is (e.g. "openai_api_key")
    pub purpose: String,
    /// Optional provider name this slot is intended for
    pub provider: Option<LlmProvider>,
}

#[derive(Serialize)]
pub struct CreateSecretSlotOutput {
    pub slot_id: String,
    pub purpose: String,
    pub provider: Option<String>,
    pub expires_at: String,
}

pub struct CreateSecretSlot;

#[async_trait]
impl PlatformAction for CreateSecretSlot {
    type Input = CreateSecretSlotInput;
    type Output = CreateSecretSlotOutput;

    fn name(&self) -> &'static str {
        "create_secret_slot"
    }
    fn description(&self) -> &'static str {
        "Create a single-use secret slot so the user can securely deposit an API key. \
         A secure input form will appear in the chat for the user to paste their key. \
         NEVER ask the user to provide a secret directly in chat."
    }
    fn required_scope(&self) -> String {
        "llm:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let body = serde_json::json!({
            "purpose": input.purpose,
            "provider": input.provider,
        });
        let provider_str = input.provider.map(|p| format!("{:?}", p).to_lowercase());
        let resp = ctx.http.flow_post("/api/secrets", &body).await?;
        let slot: serde_json::Value = resp.json().await?;
        Ok(CreateSecretSlotOutput {
            slot_id: slot["slot_id"].as_str().unwrap_or_default().to_string(),
            purpose: input.purpose,
            provider: provider_str,
            expires_at: slot["expires_at"].as_str().unwrap_or_default().to_string(),
        })
    }
}

// ── Configure Integration ───────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ConfigureIntegrationInput {
    /// LLM provider to configure
    pub provider: LlmProvider,
    /// Secret slot ID for the API key (required for non-Bedrock providers).
    /// Call create_secret_slot first and have the user deposit the API key.
    pub secret_slot: Option<String>,
    /// Secret slot ID for the AWS access key ID (required for Bedrock).
    /// Call create_secret_slot first and have the user deposit the access key ID.
    pub access_key_slot: Option<String>,
    /// Secret slot ID for the AWS secret access key (required for Bedrock).
    /// Call create_secret_slot first and have the user deposit the secret access key.
    pub secret_key_slot: Option<String>,
    /// AWS region (Bedrock only, defaults to "us-east-1")
    pub region: Option<String>,
    /// Whether the integration is enabled (defaults to true)
    pub enabled: Option<bool>,
}

#[derive(Serialize)]
pub struct ConfigureIntegrationOutput {
    pub integration: serde_json::Value,
}

pub struct ConfigureIntegration;

#[async_trait]
impl PlatformAction for ConfigureIntegration {
    type Input = ConfigureIntegrationInput;
    type Output = ConfigureIntegrationOutput;

    fn name(&self) -> &'static str {
        "configure_integration"
    }
    fn description(&self) -> &'static str {
        "Configure an LLM provider integration using filled secret slot(s). \
         This action sets up provider API keys and should be explicitly requested by the user. \
         For most providers, call create_secret_slot once for the API key. \
         For Bedrock, call create_secret_slot twice (access_key_id and secret_access_key). \
         Wait for the user to deposit all secrets, then call this with the slot IDs."
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
        let provider_str = serde_json::to_value(&input.provider)?;

        let mut body = serde_json::json!({
            "project_id": pid,
            "provider": provider_str,
            "enabled": input.enabled,
        });

        if provider_str.as_str() == Some("bedrock") {
            let ak_slot = input
                .access_key_slot
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("access_key_slot is required for Bedrock"))?;
            let sk_slot = input
                .secret_key_slot
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("secret_key_slot is required for Bedrock"))?;

            let access_key_id = resolve_slot(ctx, ak_slot).await?;
            let secret_access_key = resolve_slot(ctx, sk_slot).await?;

            body["access_key_id"] = serde_json::Value::String(access_key_id);
            body["secret_access_key"] = serde_json::Value::String(secret_access_key);
            if let Some(region) = &input.region {
                body["region"] = serde_json::Value::String(region.clone());
            }
        } else {
            let slot = input.secret_slot.as_deref().ok_or_else(|| {
                anyhow::anyhow!("secret_slot is required for non-Bedrock providers")
            })?;
            let api_key = resolve_slot(ctx, slot).await?;
            body["api_key"] = serde_json::Value::String(api_key);
        }

        let resp = ctx.http.flow_post("/api/llm/integrations", &body).await?;
        let integration = resp.json().await?;
        Ok(ConfigureIntegrationOutput { integration })
    }
}

// ── Update / Rotate LLM Integration Key ─────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct UpdateLlmIntegrationInput {
    /// Provider whose integration to update
    pub provider: LlmProvider,
    /// Secret slot ID for the new API key (non-Bedrock providers).
    /// Call create_secret_slot first and have the user deposit the new key.
    pub secret_slot: Option<String>,
    /// Secret slot ID for the new AWS access key ID (Bedrock only).
    pub access_key_slot: Option<String>,
    /// Secret slot ID for the new AWS secret access key (Bedrock only).
    pub secret_key_slot: Option<String>,
    /// AWS region (Bedrock only)
    pub region: Option<String>,
    /// Whether the integration is enabled (defaults to true)
    pub enabled: Option<bool>,
}

#[derive(Serialize)]
pub struct UpdateLlmIntegrationOutput {
    pub integration: serde_json::Value,
}

pub struct UpdateLlmIntegration;

#[async_trait]
impl PlatformAction for UpdateLlmIntegration {
    type Input = UpdateLlmIntegrationInput;
    type Output = UpdateLlmIntegrationOutput;

    fn name(&self) -> &'static str {
        "update_llm_integration"
    }
    fn description(&self) -> &'static str {
        "Update or rotate the API key for an existing LLM provider integration. \
         Call create_secret_slot first for the new key, wait for the user to deposit it, \
         then call this with the slot ID. For Bedrock, provide both access_key_slot and secret_key_slot."
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
        let provider_str = serde_json::to_value(&input.provider)?;

        let mut body = serde_json::json!({
            "project_id": pid,
        });

        if provider_str.as_str() == Some("bedrock") {
            if let Some(ak_slot) = input.access_key_slot.as_deref() {
                let access_key_id = resolve_slot(ctx, ak_slot).await?;
                body["access_key_id"] = serde_json::Value::String(access_key_id);
            }
            if let Some(sk_slot) = input.secret_key_slot.as_deref() {
                let secret_access_key = resolve_slot(ctx, sk_slot).await?;
                body["secret_access_key"] = serde_json::Value::String(secret_access_key);
            }
            if let Some(region) = &input.region {
                body["region"] = serde_json::Value::String(region.clone());
            }
        } else if let Some(slot) = input.secret_slot.as_deref() {
            let api_key = resolve_slot(ctx, slot).await?;
            body["api_key"] = serde_json::Value::String(api_key);
        }

        if let Some(enabled) = input.enabled {
            body["enabled"] = serde_json::Value::Bool(enabled);
        }

        let provider_path = provider_str.as_str().unwrap_or_default();
        let resp = ctx
            .http
            .flow_put(&format!("/api/llm/integrations/{provider_path}"), &body)
            .await?;
        let integration = resp.json().await?;
        Ok(UpdateLlmIntegrationOutput { integration })
    }
}

// ── Test Integration ────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct TestIntegrationInput {
    /// Provider to test connectivity for
    pub provider: LlmProvider,
}

#[derive(Serialize)]
pub struct TestIntegrationOutput {
    pub result: serde_json::Value,
}

pub struct TestIntegration;

#[async_trait]
impl PlatformAction for TestIntegration {
    type Input = TestIntegrationInput;
    type Output = TestIntegrationOutput;

    fn name(&self) -> &'static str {
        "test_integration"
    }
    fn description(&self) -> &'static str {
        "Test connectivity of an LLM provider integration by sending a small probe request. \
         Returns success/failure and latency. Use after configure_integration to verify the key works."
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
        let provider_str = serde_json::to_value(&input.provider)?;
        let provider_path = provider_str.as_str().unwrap_or_default();
        let body = serde_json::json!({ "project_id": pid });
        let resp = ctx
            .http
            .flow_post(
                &format!("/api/llm/integrations/{provider_path}/test"),
                &body,
            )
            .await?;
        let result = resp.json().await?;
        Ok(TestIntegrationOutput { result })
    }
}

// ── Delete Integration ──────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct DeleteIntegrationInput {
    /// Provider to delete
    pub provider: LlmProvider,
}

#[derive(Serialize)]
pub struct DeleteIntegrationOutput {
    pub result: serde_json::Value,
}

pub struct DeleteIntegration;

#[async_trait]
impl PlatformAction for DeleteIntegration {
    type Input = DeleteIntegrationInput;
    type Output = DeleteIntegrationOutput;

    fn name(&self) -> &'static str {
        "delete_integration"
    }
    fn description(&self) -> &'static str {
        "Delete an LLM provider integration. This action should be explicitly requested by the user."
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
        let provider_str = serde_json::to_value(&input.provider)?;
        let provider_path = provider_str.as_str().unwrap_or_default();
        let resp = ctx
            .http
            .flow_delete(&format!(
                "/api/llm/integrations/{provider_path}?project_id={pid}",
            ))
            .await?;
        let result = resp.json().await?;
        Ok(DeleteIntegrationOutput { result })
    }
}

// ── Registration ─────────────────────────────────────────────────────

pub fn register(registry: &mut ActionRegistry) {
    registry.register(ListIntegrations);
    registry.register(CreateSecretSlot);
    registry.register(ConfigureIntegration);
    registry.register(UpdateLlmIntegration);
    registry.register(TestIntegration);
    registry.register(DeleteIntegration);
}
