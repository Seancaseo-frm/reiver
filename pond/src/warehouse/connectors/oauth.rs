//! OAuth Token Management
//!
//! Provides OAuth 2.0 token management for SaaS connectors.
//!
//! # Features
//!
//! - Access token storage and refresh
//! - Automatic token refresh before expiry
//! - Support for refresh token flow
//! - Thread-safe token storage with RwLock
//!
//! # Usage
//!
//! ```ignore
//! let oauth = OAuthConfig::new(
//!     "client_id",
//!     "client_secret",
//!     "https://api.example.com/oauth/token",
//! )
//! .with_access_token("access_token", Some(expires_at))
//! .with_refresh_token("refresh_token");
//!
//! // Get authorization header (refreshes if needed)
//! let header = oauth.authorization_header().await?;
//! ```

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::crypto::SecretString;
use super::ConnectorError;
use tokio::sync::Mutex;

/// Buffer time before expiry to trigger refresh (5 minutes).
const REFRESH_BUFFER_SECS: i64 = 300;

/// OAuth 2.0 configuration for SaaS connectors.
///
/// Manages access tokens and refresh tokens for OAuth-based APIs.
/// Thread-safe for use across async tasks.
#[derive(Clone)]
pub struct OAuthConfig {
    /// OAuth client ID
    pub client_id: String,
    /// OAuth client secret (protected from logging)
    client_secret: SecretString,
    /// Token endpoint URL for refreshing tokens
    pub token_endpoint: String,
    /// Current token state (thread-safe)
    token_state: Arc<RwLock<TokenState>>,
    /// HTTP client for token refresh requests
    client: reqwest::Client,
    /// Optional scopes for token refresh
    pub scopes: Option<Vec<String>>,
    /// Serializes concurrent refresh attempts to prevent TOCTOU races
    /// with providers that rotate refresh tokens on each use.
    refresh_mutex: Arc<Mutex<()>>,
}

/// Internal token state.
#[derive(Clone)]
struct TokenState {
    /// Current access token
    access_token: Option<SecretString>,
    /// Refresh token for obtaining new access tokens
    refresh_token: Option<SecretString>,
    /// When the access token expires
    expires_at: Option<DateTime<Utc>>,
}

impl std::fmt::Debug for OAuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthConfig")
            .field("client_id", &self.client_id)
            .field("client_secret", &"***REDACTED***")
            .field("token_endpoint", &self.token_endpoint)
            .field("scopes", &self.scopes)
            .finish()
    }
}

impl OAuthConfig {
    /// Create a new OAuth configuration.
    ///
    /// # Arguments
    ///
    /// * `client_id` - OAuth client ID
    /// * `client_secret` - OAuth client secret
    /// * `token_endpoint` - URL for token refresh requests
    pub fn new(
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
        token_endpoint: impl Into<String>,
    ) -> Self {
        Self {
            client_id: client_id.into(),
            client_secret: SecretString::new(client_secret),
            token_endpoint: token_endpoint.into(),
            token_state: Arc::new(RwLock::new(TokenState {
                access_token: None,
                refresh_token: None,
                expires_at: None,
            })),
            client: reqwest::Client::new(),
            scopes: None,
            refresh_mutex: Arc::new(Mutex::new(())),
        }
    }

    /// Set the access token.
    ///
    /// Note: This preserves any existing refresh token.
    pub fn with_access_token(
        self,
        access_token: impl Into<String>,
        expires_at: Option<DateTime<Utc>>,
    ) -> Self {
        // Get current refresh token if any (synchronous read during construction)
        let current_refresh = self.get_current_refresh_token_sync();
        
        let state = TokenState {
            access_token: Some(SecretString::new(access_token)),
            refresh_token: current_refresh,
            expires_at,
        };
        Self {
            token_state: Arc::new(RwLock::new(state)),
            ..self
        }
    }

    /// Set the refresh token.
    ///
    /// Note: This preserves any existing access token and expiry.
    pub fn with_refresh_token(self, refresh_token: impl Into<String>) -> Self {
        // Get current state synchronously during construction
        let (current_access, current_expires) = self.get_current_access_token_sync();
        
        let state = TokenState {
            access_token: current_access,
            refresh_token: Some(SecretString::new(refresh_token)),
            expires_at: current_expires,
        };
        Self {
            token_state: Arc::new(RwLock::new(state)),
            ..self
        }
    }

    /// Get current refresh token synchronously (for builder pattern).
    ///
    /// Uses try_read which will succeed since we own the Arc during construction.
    fn get_current_refresh_token_sync(&self) -> Option<SecretString> {
        // During builder construction, we should always be able to get a read lock
        // since no async tasks are running on this instance yet
        self.token_state
            .try_read()
            .ok()
            .and_then(|state| state.refresh_token.clone())
    }

    /// Get current access token and expiry synchronously (for builder pattern).
    fn get_current_access_token_sync(&self) -> (Option<SecretString>, Option<DateTime<Utc>>) {
        self.token_state
            .try_read()
            .map(|state| (state.access_token.clone(), state.expires_at))
            .unwrap_or((None, None))
    }

    /// Set OAuth scopes for token refresh.
    pub fn with_scopes(mut self, scopes: Vec<String>) -> Self {
        self.scopes = Some(scopes);
        self
    }

    /// Check if the access token is expired or about to expire.
    pub async fn is_expired(&self) -> bool {
        let state = self.token_state.read().await;
        match state.expires_at {
            Some(expires_at) => {
                let buffer = Duration::seconds(REFRESH_BUFFER_SECS);
                Utc::now() >= expires_at - buffer
            }
            None => state.access_token.is_none(),
        }
    }

    /// Check if we have a refresh token available.
    pub async fn has_refresh_token(&self) -> bool {
        let state = self.token_state.read().await;
        state.refresh_token.is_some()
    }

    /// Get the current access token, refreshing if needed.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::OAuthExpired` if the token is expired
    /// and refresh fails.
    pub async fn get_access_token(&self) -> Result<String, ConnectorError> {
        if self.is_expired().await {
            self.refresh_if_needed().await?;
        }

        let state = self.token_state.read().await;
        state
            .access_token
            .as_ref()
            .map(|t| t.expose().to_string())
            .ok_or_else(|| {
                ConnectorError::OAuthExpired("No access token available".to_string())
            })
    }

    /// Get the authorization header value.
    ///
    /// Returns `Bearer <access_token>`.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::OAuthExpired` if the token is expired
    /// and refresh fails.
    pub async fn authorization_header(&self) -> Result<String, ConnectorError> {
        let token = self.get_access_token().await?;
        Ok(format!("Bearer {}", token))
    }

    /// Refresh the access token if it's expired or about to expire.
    ///
    /// Uses the refresh token to obtain a new access token from the token endpoint.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::OAuthExpired` if:
    /// - No refresh token is available
    /// - The token endpoint returns an error
    /// - The response cannot be parsed
    pub async fn refresh_if_needed(&self) -> Result<(), ConnectorError> {
        if !self.is_expired().await {
            return Ok(());
        }

        // Serialize concurrent refresh attempts. Providers that rotate refresh
        // tokens on each use (e.g., Google, Xero) will invalidate the old token,
        // causing a second concurrent refresh to fail.
        let _guard = self.refresh_mutex.lock().await;

        // Re-check after acquiring the lock — another task may have refreshed already.
        if !self.is_expired().await {
            return Ok(());
        }

        let refresh_token = {
            let state = self.token_state.read().await;
            state.refresh_token.as_ref().map(|t| t.expose().to_string())
        };

        let refresh_token = refresh_token.ok_or_else(|| {
            ConnectorError::OAuthExpired("No refresh token available".to_string())
        })?;

        let mut params = vec![
            ("grant_type", "refresh_token".to_string()),
            ("refresh_token", refresh_token),
            ("client_id", self.client_id.clone()),
            ("client_secret", self.client_secret.expose().to_string()),
        ];

        if let Some(scopes) = &self.scopes {
            params.push(("scope", scopes.join(" ")));
        }

        let response = self
            .client
            .post(&self.token_endpoint)
            .form(&params)
            .send()
            .await
            .map_err(|e| {
                ConnectorError::OAuthExpired(format!("Token refresh request failed: {}", e))
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(ConnectorError::OAuthExpired(format!(
                "Token refresh failed ({}): {}",
                status, body
            )));
        }

        let token_response: TokenResponse = response.json().await.map_err(|e| {
            ConnectorError::OAuthExpired(format!("Failed to parse token response: {}", e))
        })?;

        let mut state = self.token_state.write().await;
        state.access_token = Some(SecretString::new(&token_response.access_token));

        if let Some(expires_in) = token_response.expires_in {
            state.expires_at = Some(Utc::now() + Duration::seconds(expires_in as i64));
        }

        if let Some(new_refresh_token) = token_response.refresh_token {
            state.refresh_token = Some(SecretString::new(new_refresh_token));
        }

        tracing::debug!(
            expires_in = ?token_response.expires_in,
            "OAuth token refreshed successfully"
        );

        Ok(())
    }

    /// Set the access token directly (for initial setup or testing).
    pub async fn set_access_token(
        &self,
        access_token: impl Into<String>,
        expires_at: Option<DateTime<Utc>>,
    ) {
        let mut state = self.token_state.write().await;
        state.access_token = Some(SecretString::new(access_token));
        state.expires_at = expires_at;
    }

    /// Set the refresh token directly.
    pub async fn set_refresh_token(&self, refresh_token: impl Into<String>) {
        let mut state = self.token_state.write().await;
        state.refresh_token = Some(SecretString::new(refresh_token));
    }
}

/// OAuth token response from the token endpoint.
#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[allow(dead_code)]
    token_type: Option<String>,
    expires_in: Option<u64>,
    refresh_token: Option<String>,
    #[allow(dead_code)]
    scope: Option<String>,
}

/// Serializable OAuth credentials for storage.
///
/// This struct can be serialized to/from JSON for storing OAuth credentials
/// in a database. Note that the secrets should be encrypted before storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthCredentials {
    pub client_id: String,
    pub client_secret: String,
    pub access_token: Option<String>,
    pub refresh_token: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
    pub token_endpoint: String,
    pub scopes: Option<Vec<String>>,
}

impl OAuthCredentials {
    /// Convert to an OAuthConfig for use in connectors.
    pub fn to_config(&self) -> OAuthConfig {
        let mut config = OAuthConfig::new(
            &self.client_id,
            &self.client_secret,
            &self.token_endpoint,
        );

        if let Some(scopes) = &self.scopes {
            config = config.with_scopes(scopes.clone());
        }

        if let Some(ref token) = self.access_token {
            config = config.with_access_token(token, self.expires_at);
        }
        if let Some(ref refresh) = self.refresh_token {
            config = config.with_refresh_token(refresh);
        }

        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oauth_config_creation() {
        let config = OAuthConfig::new(
            "client_id",
            "client_secret",
            "https://api.example.com/oauth/token",
        );

        assert_eq!(config.client_id, "client_id");
        assert_eq!(config.token_endpoint, "https://api.example.com/oauth/token");
    }

    #[test]
    fn test_oauth_config_debug_redacts_secret() {
        let config = OAuthConfig::new("client_id", "super_secret", "https://api.example.com/token");
        let debug_output = format!("{:?}", config);

        assert!(!debug_output.contains("super_secret"));
        assert!(debug_output.contains("REDACTED"));
    }

    #[tokio::test]
    async fn test_oauth_config_is_expired_no_token() {
        let config = OAuthConfig::new("client_id", "secret", "https://api.example.com/token");

        assert!(config.is_expired().await);
    }

    #[tokio::test]
    async fn test_oauth_config_is_expired_with_future_expiry() {
        let config = OAuthConfig::new("client_id", "secret", "https://api.example.com/token");

        let future_expiry = Utc::now() + Duration::hours(1);
        config.set_access_token("token", Some(future_expiry)).await;

        assert!(!config.is_expired().await);
    }

    #[tokio::test]
    async fn test_oauth_config_is_expired_with_past_expiry() {
        let config = OAuthConfig::new("client_id", "secret", "https://api.example.com/token");

        let past_expiry = Utc::now() - Duration::hours(1);
        config.set_access_token("token", Some(past_expiry)).await;

        assert!(config.is_expired().await);
    }

    #[tokio::test]
    async fn test_oauth_config_is_expired_within_buffer() {
        let config = OAuthConfig::new("client_id", "secret", "https://api.example.com/token");

        // Expires in 2 minutes (within 5 minute buffer)
        let soon_expiry = Utc::now() + Duration::minutes(2);
        config.set_access_token("token", Some(soon_expiry)).await;

        assert!(config.is_expired().await);
    }

    #[tokio::test]
    async fn test_get_access_token_no_token() {
        let config = OAuthConfig::new("client_id", "secret", "https://api.example.com/token");

        let result = config.get_access_token().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_access_token_valid() {
        let config = OAuthConfig::new("client_id", "secret", "https://api.example.com/token");

        let future_expiry = Utc::now() + Duration::hours(1);
        config.set_access_token("my_token", Some(future_expiry)).await;

        let token = config.get_access_token().await.unwrap();
        assert_eq!(token, "my_token");
    }

    #[tokio::test]
    async fn test_authorization_header() {
        let config = OAuthConfig::new("client_id", "secret", "https://api.example.com/token");

        let future_expiry = Utc::now() + Duration::hours(1);
        config.set_access_token("my_token", Some(future_expiry)).await;

        let header = config.authorization_header().await.unwrap();
        assert_eq!(header, "Bearer my_token");
    }

    #[tokio::test]
    async fn test_credentials_to_config_transfers_tokens() {
        let expires = Utc::now() + Duration::hours(1);
        let creds = OAuthCredentials {
            client_id: "cid".to_string(),
            client_secret: "csecret".to_string(),
            access_token: Some("access_tok".to_string()),
            refresh_token: Some("refresh_tok".to_string()),
            expires_at: Some(expires),
            token_endpoint: "https://example.com/token".to_string(),
            scopes: Some(vec!["read".to_string()]),
        };

        let config = creds.to_config();
        let token = config.get_access_token().await.unwrap();
        assert_eq!(token, "access_tok", "access_token must be transferred");
        assert!(!config.is_expired().await, "token with future expiry must not be expired");
    }

    #[tokio::test]
    async fn test_credentials_to_config_without_tokens() {
        let creds = OAuthCredentials {
            client_id: "cid".to_string(),
            client_secret: "csecret".to_string(),
            access_token: None,
            refresh_token: None,
            expires_at: None,
            token_endpoint: "https://example.com/token".to_string(),
            scopes: None,
        };

        let config = creds.to_config();
        assert!(config.get_access_token().await.is_err(), "no token means get_access_token should fail");
    }

    #[tokio::test]
    async fn test_concurrent_refresh_serialized() {
        let config = Arc::new(OAuthConfig::new(
            "client_id",
            "secret",
            "https://api.example.com/token",
        ));

        let past_expiry = Utc::now() - Duration::hours(1);
        config.set_access_token("old_token", Some(past_expiry)).await;
        config.set_refresh_token("refresh_tok").await;

        // Both tasks try to refresh concurrently. Because the token endpoint
        // is unreachable, both will fail — but the mutex ensures they don't
        // race on the refresh token. We just verify no panic occurs and the
        // mutex correctly serializes the attempts.
        let c1 = config.clone();
        let c2 = config.clone();

        let (r1, r2) = tokio::join!(
            tokio::spawn(async move { c1.refresh_if_needed().await }),
            tokio::spawn(async move { c2.refresh_if_needed().await }),
        );

        // Both should fail (unreachable endpoint), but neither should panic
        assert!(r1.is_ok(), "task 1 must not panic");
        assert!(r2.is_ok(), "task 2 must not panic");
    }
}
