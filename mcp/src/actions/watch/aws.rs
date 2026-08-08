use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::actions::resolve_slot;
use crate::actions::types::{AwsAuthMethod, AwsServiceType};
use crate::registry::ActionRegistry;

#[derive(Deserialize, JsonSchema)]
pub struct ConfigureAwsIntegrationInput {
    /// Human-readable name for this integration
    pub name: String,
    /// AWS service to monitor
    pub integration_type: AwsServiceType,
    /// AWS region (e.g. "us-east-1")
    pub region: String,
    /// Authentication method
    pub auth_method: AwsAuthMethod,
    /// IAM role ARN (required when auth_method is "role")
    pub role_arn: Option<String>,
    /// Secret slot ID containing the external ID for cross-account IAM role assumption.
    /// Only needed when auth_method is "role" and the role requires an external ID.
    pub external_id_slot: Option<String>,
    /// Secret slot ID containing the AWS access key ID (required when auth_method is "access_key").
    /// Call create_secret_slot first and have the user deposit the access key ID.
    pub access_key_id_slot: Option<String>,
    /// Secret slot ID containing the AWS secret access key (required when auth_method is "access_key").
    /// Call create_secret_slot first and have the user deposit the secret access key.
    pub secret_access_key_slot: Option<String>,
    /// Whether the integration is enabled (defaults to true)
    pub enabled: Option<bool>,
}

#[derive(Serialize)]
pub struct ConfigureAwsIntegrationOutput {
    pub integration: serde_json::Value,
}

pub struct ConfigureAwsIntegration;

#[async_trait]
impl PlatformAction for ConfigureAwsIntegration {
    type Input = ConfigureAwsIntegrationInput;
    type Output = ConfigureAwsIntegrationOutput;

    fn name(&self) -> &'static str {
        "configure_aws_integration"
    }
    fn description(&self) -> &'static str {
        "Configure an AWS infrastructure monitoring integration. \
         This action should be explicitly requested by the user. \
         For access key auth, call create_secret_slot twice (one for access_key_id, \
         one for secret_access_key), wait for the user to deposit both, \
         then call this with the slot IDs. \
         For IAM role auth, provide role_arn and optionally external_id_slot."
    }
    fn required_scope(&self) -> String {
        "observability:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let integration_type_str = serde_json::to_value(&input.integration_type)?;
        let auth_method_str = serde_json::to_value(&input.auth_method)?;

        let mut payload = serde_json::json!({
            "name": input.name,
            "integration_type": integration_type_str,
            "region": input.region,
            "enabled": input.enabled.unwrap_or(true),
        });

        let method = auth_method_str.as_str().unwrap_or_default();
        match method {
            "access_key" => {
                let ak_slot = input.access_key_id_slot.as_deref().ok_or_else(|| {
                    anyhow::anyhow!("access_key_id_slot is required for access_key auth")
                })?;
                let sk_slot = input.secret_access_key_slot.as_deref().ok_or_else(|| {
                    anyhow::anyhow!("secret_access_key_slot is required for access_key auth")
                })?;

                let access_key_id = resolve_slot(ctx, ak_slot).await?;
                let secret_access_key = resolve_slot(ctx, sk_slot).await?;

                payload["access_key_id"] = serde_json::Value::String(access_key_id);
                payload["secret_access_key"] = serde_json::Value::String(secret_access_key);
            }
            "role" => {
                let role_arn = input
                    .role_arn
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("role_arn is required for role auth"))?;
                payload["role_arn"] = serde_json::Value::String(role_arn.to_string());

                if let Some(eid_slot) = input.external_id_slot.as_deref() {
                    let external_id = resolve_slot(ctx, eid_slot).await?;
                    payload["external_id"] = serde_json::Value::String(external_id);
                }
            }
            other => {
                anyhow::bail!("Unsupported auth_method: {other}. Use \"role\" or \"access_key\".")
            }
        }

        let resp = ctx
            .http
            .watch_post("/api/aws/integrations", &payload)
            .await?;
        let integration = resp.json().await?;
        Ok(ConfigureAwsIntegrationOutput { integration })
    }
}

pub fn register(registry: &mut ActionRegistry) {
    registry.register(ConfigureAwsIntegration);
}
