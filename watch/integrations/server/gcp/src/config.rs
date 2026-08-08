//! Configuration for GCP integrations

use serde::{Deserialize, Serialize};
use tracing::info;
use reqwest::Client;
use jsonwebtoken::{encode, Header, EncodingKey};
use chrono::{Utc, Duration};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};

/// GCP integration configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcpConfig {
    /// GCP project ID
    pub project_id: String,
    /// Service account email (for authentication)
    pub service_account_email: Option<String>,
    /// Service account private key (base64-encoded or PEM format)
    pub private_key: Option<String>,
    /// Optional: Service account JSON (full JSON key file content)
    /// If provided, will be used instead of individual fields
    pub service_account_json: Option<String>,
}

impl Default for GcpConfig {
    fn default() -> Self {
        Self {
            project_id: String::new(),
            service_account_email: None,
            private_key: None,
            service_account_json: None,
        }
    }
}

#[derive(Debug, Serialize)]
struct JwtClaims {
    iss: String,
    scope: String,
    aud: String,
    exp: i64,
    iat: i64,
}

impl GcpConfig {
    /// Get GCP access token for API calls
    /// 
    /// Uses service account authentication with OAuth2:
    /// 1. Creates a JWT signed with the service account private key
    /// 2. Exchanges the JWT for an access token
    /// 
    /// Supports both:
    /// - Individual fields (service_account_email, private_key)
    /// - Full service account JSON key file
    pub async fn get_access_token(&self) -> Result<String, anyhow::Error> {
        let (service_account_email, private_key) = if let Some(json_content) = &self.service_account_json {
            // Parse service account JSON
            let sa_json: serde_json::Value = serde_json::from_str(json_content)
                .map_err(|e| anyhow::anyhow!("Failed to parse service account JSON: {}", e))?;
            
            let email = sa_json.get("client_email")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("Missing client_email in service account JSON"))?;
            
            let key = sa_json.get("private_key")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("Missing private_key in service account JSON"))?;
            
            (email.to_string(), key.to_string())
        } else {
            // Use individual fields
            let email = self.service_account_email.as_ref()
                .ok_or_else(|| anyhow::anyhow!("Service account email required (or provide service_account_json)"))?;
            
            let key = self.private_key.as_ref()
                .ok_or_else(|| anyhow::anyhow!("Private key required (or provide service_account_json)"))?;
            
            // If key is base64-encoded, decode it
            let key_str = if key.starts_with("-----BEGIN") {
                key.clone()
            } else {
                // Try to decode base64
                String::from_utf8(BASE64.decode(key)?)
                    .map_err(|e| anyhow::anyhow!("Failed to decode private key from base64: {}", e))?
            };
            
            (email.clone(), key_str)
        };

        info!("Using GCP Service Account authentication: email={}", service_account_email);

        // Create JWT for OAuth2 token exchange
        let now = Utc::now();
        let claims = JwtClaims {
            iss: service_account_email.clone(),
            scope: "https://www.googleapis.com/auth/cloud-platform https://www.googleapis.com/auth/monitoring.read".to_string(),
            aud: "https://oauth2.googleapis.com/token".to_string(),
            exp: (now + Duration::hours(1)).timestamp(),
            iat: now.timestamp(),
        };

        // Encode JWT
        let token = encode(
            &Header::new(jsonwebtoken::Algorithm::RS256),
            &claims,
            &EncodingKey::from_rsa_pem(private_key.as_bytes())
                .map_err(|e| anyhow::anyhow!("Failed to create encoding key from private key: {}", e))?,
        )
        .map_err(|e| anyhow::anyhow!("Failed to encode JWT: {}", e))?;

        // Exchange JWT for access token
        let client = Client::new();
        let params = [
            ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
            ("assertion", &token),
        ];

        let response = client
            .post("https://oauth2.googleapis.com/token")
            .form(&params)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to request GCP access token: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let body: String = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "GCP OAuth2 token request failed ({}): {}",
                status,
                body
            ));
        }

        // Parse token response
        let token_response: serde_json::Value = response
            .json()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to parse GCP token response: {}", e))?;

        let access_token = token_response
            .get("access_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("No access_token in GCP token response"))?;

        Ok(access_token.to_string())
    }
}
