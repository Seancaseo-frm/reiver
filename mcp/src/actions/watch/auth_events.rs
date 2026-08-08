use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::actions::resolve_slot;
use crate::actions::types::{AuthProviderKind, OneLoginRegion};
use crate::registry::ActionRegistry;

#[derive(Deserialize, JsonSchema)]
pub struct ConfigureAuthProviderInput {
    /// Identity provider to configure
    pub provider: AuthProviderKind,
    /// Human-readable name for this integration
    pub name: String,
    /// Secret slot ID containing the API token (Okta) or client secret (all other providers).
    /// Call create_secret_slot first and have the user deposit the value.
    pub secret_slot: String,
    /// IdP domain or tenant URL (e.g. "mycompany.okta.com")
    pub domain: Option<String>,
    /// OAuth client ID (required for Auth0, OneLogin, Ping, Entra, Keycloak)
    pub client_id: Option<String>,
    /// Entra ID (Azure AD) tenant ID
    pub tenant_id: Option<String>,
    /// Ping Identity environment ID
    pub environment_id: Option<String>,
    /// OneLogin region
    pub region: Option<OneLoginRegion>,
    /// Polling interval in seconds (default: 60)
    #[schemars(range(min = 10, max = 3600))]
    pub poll_interval_seconds: Option<i32>,
    /// Event types to collect (provider-specific)
    pub event_types: Option<Vec<String>>,
    /// Whether the integration is enabled (defaults to true)
    pub enabled: Option<bool>,
}

#[derive(Serialize)]
pub struct ConfigureAuthProviderOutput {
    pub integration: serde_json::Value,
}

pub struct ConfigureAuthProvider;

#[async_trait]
impl PlatformAction for ConfigureAuthProvider {
    type Input = ConfigureAuthProviderInput;
    type Output = ConfigureAuthProviderOutput;

    fn name(&self) -> &'static str {
        "configure_auth_provider"
    }
    fn description(&self) -> &'static str {
        "Configure an identity provider for auth event monitoring \
         (Okta, Auth0, Entra ID, OneLogin, Ping Identity, Keycloak). \
         This action changes authentication settings and should be explicitly requested \
         by the user. The secret_slot should contain the API token (Okta) or client \
         secret (others). Call create_secret_slot first, wait for the user to deposit \
         the value, then call this action with the slot ID."
    }
    fn required_scope(&self) -> String {
        "observability:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let secret_value = resolve_slot(ctx, &input.secret_slot).await?;
        let provider_str = serde_json::to_value(&input.provider)?;
        let provider_name = provider_str.as_str().unwrap_or_default();

        let mut payload = serde_json::json!({
            "project_id": ctx.project_id,
            "provider": provider_name,
            "name": input.name,
            "enabled": input.enabled.unwrap_or(true),
        });

        if let Some(v) = &input.domain {
            payload["domain"] = serde_json::json!(v);
        }
        if let Some(v) = &input.client_id {
            payload["client_id"] = serde_json::json!(v);
        }
        if let Some(v) = &input.tenant_id {
            payload["tenant_id"] = serde_json::json!(v);
        }
        if let Some(v) = &input.environment_id {
            payload["environment_id"] = serde_json::json!(v);
        }
        if let Some(v) = &input.region {
            payload["region"] = serde_json::to_value(v)?;
        }
        if let Some(v) = input.poll_interval_seconds {
            payload["poll_interval_seconds"] = serde_json::json!(v);
        }
        if let Some(v) = &input.event_types {
            payload["event_types"] = serde_json::json!(v);
        }

        match provider_name {
            "okta" => {
                payload["api_token"] = serde_json::Value::String(secret_value);
            }
            "auth0" | "entra_id" | "onelogin" | "ping_identity" | "keycloak" => {
                payload["client_secret"] = serde_json::Value::String(secret_value);
            }
            other => anyhow::bail!(
                "Unsupported auth provider: {other}. Use okta, auth0, entra_id, onelogin, ping_identity, or keycloak."
            ),
        }

        let resp = ctx
            .http
            .website_post("/api/auth-events/integrations", &payload)
            .await?;
        let integration = resp.json().await?;
        Ok(ConfigureAuthProviderOutput { integration })
    }
}

pub fn register(registry: &mut ActionRegistry) {
    registry.register(ConfigureAuthProvider);
}
