use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::actions::resolve_slot;
use crate::registry::ActionRegistry;

#[derive(Deserialize, JsonSchema)]
pub struct ConfigureAzureIntegrationInput {
    /// Human-readable name for this integration
    pub name: String,
    /// Azure service type to monitor (e.g. "vm", "app_service", "sql_database", "storage")
    pub integration_type: String,
    /// Azure subscription ID to monitor
    pub subscription_id: String,
    /// Azure tenant (directory) ID
    pub tenant_id: Option<String>,
    /// Secret slot ID containing the Azure client/application ID.
    /// Call create_secret_slot first and have the user deposit the client ID.
    pub client_id_slot: Option<String>,
    /// Secret slot ID containing the Azure client secret.
    /// Call create_secret_slot first and have the user deposit the client secret.
    pub client_secret_slot: Option<String>,
    /// Whether the integration is enabled (defaults to true)
    pub enabled: Option<bool>,
}

#[derive(Serialize)]
pub struct ConfigureAzureIntegrationOutput {
    pub integration: serde_json::Value,
}

pub struct ConfigureAzureIntegration;

#[async_trait]
impl PlatformAction for ConfigureAzureIntegration {
    type Input = ConfigureAzureIntegrationInput;
    type Output = ConfigureAzureIntegrationOutput;

    fn name(&self) -> &'static str {
        "configure_azure_integration"
    }
    fn description(&self) -> &'static str {
        "Configure an Azure infrastructure monitoring integration. \
         This action should be explicitly requested by the user. \
         Call create_secret_slot twice (one for client_id, one for client_secret), \
         wait for the user to deposit both, then call this with the slot IDs."
    }
    fn required_scope(&self) -> String {
        "observability:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let mut payload = serde_json::json!({
            "name": input.name,
            "integration_type": input.integration_type,
            "subscription_id": input.subscription_id,
            "enabled": input.enabled.unwrap_or(true),
        });
        let obj = payload.as_object_mut().unwrap();

        if let Some(tid) = input.tenant_id {
            obj.insert("tenant_id".into(), serde_json::Value::String(tid));
        }
        if let Some(ref cid_slot) = input.client_id_slot {
            let client_id = resolve_slot(ctx, cid_slot).await?;
            obj.insert("client_id".into(), serde_json::Value::String(client_id));
        }
        if let Some(ref cs_slot) = input.client_secret_slot {
            let client_secret = resolve_slot(ctx, cs_slot).await?;
            obj.insert(
                "client_secret".into(),
                serde_json::Value::String(client_secret),
            );
        }

        let resp = ctx
            .http
            .watch_post("/api/azure/integrations", &payload)
            .await?;
        let integration = resp.json().await?;
        Ok(ConfigureAzureIntegrationOutput { integration })
    }
}

pub fn register(registry: &mut ActionRegistry) {
    registry.register(ConfigureAzureIntegration);
}
