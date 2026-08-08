//! Configuration for Azure integrations

use serde::{Deserialize, Serialize};
use tracing::info;
use reqwest::Client;

/// Azure integration configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureConfig {
    /// Azure subscription ID
    pub subscription_id: String,
    /// Azure tenant ID (for service principal authentication)
    pub tenant_id: Option<String>,
    /// Azure client ID (application/client ID for service principal)
    pub client_id: Option<String>,
    /// Azure client secret (for service principal authentication)
    pub client_secret: Option<String>,
}

impl Default for AzureConfig {
    fn default() -> Self {
        Self {
            subscription_id: String::new(),
            tenant_id: None,
            client_id: None,
            client_secret: None,
        }
    }
}

impl AzureConfig {
    /// Get Azure access token for API calls
    /// 
    /// Priority:
    /// 1. Service Principal (if client_id, client_secret, tenant_id are provided)
    /// 2. Default Azure Credential (managed identity, environment variables, Azure CLI)
    /// 
    /// For Service Principal:
    /// - Uses client credentials flow (OAuth2)
    /// - More secure than storing credentials directly
    /// - Suitable for server-to-server authentication
    pub async fn get_access_token(&self, scope: &str) -> Result<String, anyhow::Error> {
        // Service Principal authentication (required)
        let (tenant_id, client_id, client_secret) = match (
            self.tenant_id.as_ref(),
            self.client_id.as_ref(),
            self.client_secret.as_ref(),
        ) {
            (Some(tenant_id), Some(client_id), Some(client_secret)) => {
                (tenant_id.clone(), client_id.clone(), client_secret.clone())
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Service Principal authentication required: tenant_id, client_id, and client_secret must be provided"
                ));
            }
        };

        info!("Using Service Principal authentication: client_id={}", client_id);

        // Azure AD OAuth2 token endpoint
        let token_url = format!(
            "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
            tenant_id
        );

        // Build OAuth2 client credentials request
        let client = Client::new();
        let params = [
            ("client_id", client_id.as_str()),
            ("client_secret", client_secret.as_str()),
            ("scope", scope),
            ("grant_type", "client_credentials"),
        ];

        let response = client
            .post(&token_url)
            .form(&params)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to request Azure access token: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let body: String = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Azure AD token request failed ({}): {}",
                status,
                body
            ));
        }

        // Parse token response
        let token_response: serde_json::Value = response
            .json()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to parse Azure token response: {}", e))?;

        let access_token = token_response
            .get("access_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("No access_token in Azure token response"))?;

        Ok(access_token.to_string())
    }
    
    /// Get Azure Monitor scope for metrics API
    pub fn monitor_scope() -> &'static str {
        "https://management.azure.com/.default"
    }
}
