use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::actions::resolve_slot;
use crate::registry::ActionRegistry;

#[derive(Deserialize, JsonSchema)]
pub struct ConfigureGcpIntegrationInput {
    /// Human-readable name for this integration
    pub name: String,
    /// GCP service type to monitor (e.g. "compute_engine", "cloud_sql", "gke", "cloud_run")
    pub integration_type: String,
    /// GCP project ID to monitor
    pub gcp_project_id: String,
    /// Secret slot ID containing the GCP service account JSON key file content.
    /// Call create_secret_slot first and have the user deposit the JSON key.
    pub service_account_json_slot: Option<String>,
    /// Whether the integration is enabled (defaults to true)
    pub enabled: Option<bool>,
}

#[derive(Serialize)]
pub struct ConfigureGcpIntegrationOutput {
    pub integration: serde_json::Value,
}

pub struct ConfigureGcpIntegration;

#[async_trait]
impl PlatformAction for ConfigureGcpIntegration {
    type Input = ConfigureGcpIntegrationInput;
    type Output = ConfigureGcpIntegrationOutput;

    fn name(&self) -> &'static str {
        "configure_gcp_integration"
    }
    fn description(&self) -> &'static str {
        "Configure a GCP infrastructure monitoring integration. \
         This action should be explicitly requested by the user. \
         Call create_secret_slot first, wait for the user to deposit the service account \
         JSON key, then call this with the slot ID."
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
            "gcp_project_id": input.gcp_project_id,
            "enabled": input.enabled.unwrap_or(true),
        });

        if let Some(ref slot) = input.service_account_json_slot {
            let sa_json = resolve_slot(ctx, slot).await?;
            payload.as_object_mut().unwrap().insert(
                "service_account_json".into(),
                serde_json::Value::String(sa_json),
            );
        }

        let resp = ctx
            .http
            .watch_post("/api/gcp/integrations", &payload)
            .await?;
        let integration = resp.json().await?;
        Ok(ConfigureGcpIntegrationOutput { integration })
    }
}

pub fn register(registry: &mut ActionRegistry) {
    registry.register(ConfigureGcpIntegration);
}
