use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::actions::resolve_slot;
use crate::registry::ActionRegistry;

#[derive(Deserialize, JsonSchema)]
pub struct ConfigureOciIntegrationInput {
    /// Human-readable name for this integration
    pub name: String,
    /// OCI service type to monitor (e.g. "compute", "autonomous_database", "object_storage")
    pub integration_type: String,
    /// OCI tenancy OCID
    pub tenancy_ocid: String,
    /// OCI region (e.g. "us-ashburn-1")
    pub region: String,
    /// Secret slot ID containing the OCI user OCID.
    /// Call create_secret_slot first and have the user deposit the user OCID.
    pub user_ocid_slot: String,
    /// Secret slot ID containing the OCI API private key (PEM format).
    /// Call create_secret_slot first and have the user deposit the private key.
    pub private_key_slot: String,
    /// Secret slot ID containing the key fingerprint.
    /// Call create_secret_slot first and have the user deposit the fingerprint.
    pub fingerprint_slot: String,
    /// Secret slot ID containing the passphrase for the private key (if encrypted).
    pub passphrase_slot: Option<String>,
    /// Whether the integration is enabled (defaults to true)
    pub enabled: Option<bool>,
}

#[derive(Serialize)]
pub struct ConfigureOciIntegrationOutput {
    pub integration: serde_json::Value,
}

pub struct ConfigureOciIntegration;

#[async_trait]
impl PlatformAction for ConfigureOciIntegration {
    type Input = ConfigureOciIntegrationInput;
    type Output = ConfigureOciIntegrationOutput;

    fn name(&self) -> &'static str {
        "configure_oci_integration"
    }
    fn description(&self) -> &'static str {
        "Configure an Oracle Cloud Infrastructure (OCI) monitoring integration. \
         This action should be explicitly requested by the user. \
         Call create_secret_slot three times (user_ocid, private_key, fingerprint), \
         wait for deposits, then call this with the slot IDs."
    }
    fn required_scope(&self) -> String {
        "observability:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let user_ocid = resolve_slot(ctx, &input.user_ocid_slot).await?;
        let private_key = resolve_slot(ctx, &input.private_key_slot).await?;
        let fingerprint = resolve_slot(ctx, &input.fingerprint_slot).await?;

        let mut payload = serde_json::json!({
            "name": input.name,
            "integration_type": input.integration_type,
            "tenancy_ocid": input.tenancy_ocid,
            "region": input.region,
            "user_ocid": user_ocid,
            "private_key": private_key,
            "fingerprint": fingerprint,
            "enabled": input.enabled.unwrap_or(true),
        });

        if let Some(ref ps_slot) = input.passphrase_slot {
            let passphrase = resolve_slot(ctx, ps_slot).await?;
            payload
                .as_object_mut()
                .unwrap()
                .insert("passphrase".into(), serde_json::Value::String(passphrase));
        }

        let resp = ctx
            .http
            .watch_post("/api/oci/integrations", &payload)
            .await?;
        let integration = resp.json().await?;
        Ok(ConfigureOciIntegrationOutput { integration })
    }
}

pub fn register(registry: &mut ActionRegistry) {
    registry.register(ConfigureOciIntegration);
}
