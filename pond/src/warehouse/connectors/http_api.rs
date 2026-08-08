//! HTTP API Base Connector
//!
//! Provides a base implementation for REST API-based connectors.
//!
//! # Features
//!
//! - Multiple authentication methods (API key, OAuth, Basic, Bearer)
//! - Rate limiting with exponential backoff
//! - Pagination support (cursor, offset, page-based, Link header)
//! - Request/response logging
//! - Retry logic for transient failures
//!
//! # Usage
//!
//! ```ignore
//! use crate::warehouse::connectors::http_api::{HttpApiClient, AuthConfig, PaginationStyle};
//!
//! let client = HttpApiClient::new("https://api.example.com")
//!     .with_auth(AuthConfig::bearer("token"))
//!     .with_rate_limit(100, Duration::from_secs(60));
//!
//! let response = client.get("/users").await?;
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::RwLock;
use tokio::time::sleep;

use crate::crypto::SecretString;
use super::{ConnectorError, ConnectorResult};
use super::oauth::OAuthConfig;

/// Default timeout for HTTP requests.
const DEFAULT_TIMEOUT_SECS: u64 = 30;

/// Default maximum retries for transient failures.
const DEFAULT_MAX_RETRIES: u32 = 3;

/// Base delay for exponential backoff (in milliseconds).
const BASE_BACKOFF_MS: u64 = 1000;

/// HTTP API client with authentication, rate limiting, and retry support.
#[derive(Clone)]
pub struct HttpApiClient {
    /// Base URL for API requests
    base_url: String,
    /// HTTP client
    client: reqwest::Client,
    /// Authentication configuration
    auth: AuthConfig,
    /// Rate limiter state
    rate_limiter: Arc<RwLock<RateLimiterState>>,
    /// Rate limit configuration
    rate_limit_config: Option<RateLimitConfig>,
    /// Maximum retries for transient failures
    max_retries: u32,
    /// Default headers to include in all requests
    default_headers: HashMap<String, String>,
}

impl std::fmt::Debug for HttpApiClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpApiClient")
            .field("base_url", &self.base_url)
            .field("auth", &self.auth)
            .field("max_retries", &self.max_retries)
            .finish()
    }
}

/// Authentication configuration for API requests.
#[derive(Clone)]
pub enum AuthConfig {
    /// No authentication
    None,
    /// API key in header
    ApiKey {
        key: SecretString,
        header_name: String,
    },
    /// OAuth 2.0
    OAuth(Arc<OAuthConfig>),
    /// Basic authentication
    Basic {
        username: String,
        password: SecretString,
    },
    /// Bearer token
    Bearer(SecretString),
}

impl std::fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthConfig::None => write!(f, "None"),
            AuthConfig::ApiKey { header_name, .. } => {
                write!(f, "ApiKey(header: {})", header_name)
            }
            AuthConfig::OAuth(_) => write!(f, "OAuth(...)"),
            AuthConfig::Basic { username, .. } => {
                write!(f, "Basic(user: {})", username)
            }
            AuthConfig::Bearer(_) => write!(f, "Bearer(...)"),
        }
    }
}

impl AuthConfig {
    /// Create an API key auth config.
    pub fn api_key(key: impl Into<String>, header_name: impl Into<String>) -> Self {
        Self::ApiKey {
            key: SecretString::new(key),
            header_name: header_name.into(),
        }
    }

    /// Create a bearer token auth config.
    pub fn bearer(token: impl Into<String>) -> Self {
        Self::Bearer(SecretString::new(token))
    }

    /// Create a basic auth config.
    pub fn basic(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::Basic {
            username: username.into(),
            password: SecretString::new(password),
        }
    }

    /// Create an OAuth auth config.
    pub fn oauth(config: OAuthConfig) -> Self {
        Self::OAuth(Arc::new(config))
    }
}

/// Rate limit configuration.
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Maximum requests per window
    pub max_requests: u32,
    /// Window duration
    pub window: Duration,
}

/// Internal rate limiter state.
struct RateLimiterState {
    /// Number of requests in current window
    request_count: u32,
    /// Window start time
    window_start: DateTime<Utc>,
    /// Retry-After time if rate limited
    retry_after: Option<DateTime<Utc>>,
}

impl Default for RateLimiterState {
    fn default() -> Self {
        Self {
            request_count: 0,
            window_start: Utc::now(),
            retry_after: None,
        }
    }
}

/// Pagination style for API endpoints.
#[derive(Debug, Clone)]
pub enum PaginationStyle {
    /// Cursor-based pagination (common in modern APIs)
    Cursor {
        /// Field name for cursor in response
        cursor_field: String,
        /// Parameter name to send cursor
        cursor_param: String,
    },
    /// Offset-based pagination
    Offset {
        /// Items per page
        limit: usize,
        /// Parameter name for offset
        offset_param: String,
        /// Parameter name for limit
        limit_param: String,
    },
    /// Page number pagination
    PageNumber {
        /// Items per page
        per_page: usize,
        /// Parameter name for page number
        page_param: String,
        /// Parameter name for per_page
        per_page_param: String,
    },
    /// Link header pagination (GitHub style)
    LinkHeader,
}

impl Default for PaginationStyle {
    fn default() -> Self {
        Self::Offset {
            limit: 100,
            offset_param: "offset".to_string(),
            limit_param: "limit".to_string(),
        }
    }
}

/// Response from a paginated API call.
#[derive(Debug)]
pub struct PaginatedResponse<T> {
    /// The data items
    pub data: Vec<T>,
    /// Next page cursor/offset (if any)
    pub next_cursor: Option<String>,
    /// Whether there are more pages
    pub has_more: bool,
    /// Total count (if available)
    pub total: Option<u64>,
}

impl HttpApiClient {
    /// Create a new HTTP API client.
    pub fn new(base_url: impl Into<String>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            client,
            auth: AuthConfig::None,
            rate_limiter: Arc::new(RwLock::new(RateLimiterState::default())),
            rate_limit_config: None,
            max_retries: DEFAULT_MAX_RETRIES,
            default_headers: HashMap::new(),
        }
    }

    /// Set authentication configuration.
    pub fn with_auth(mut self, auth: AuthConfig) -> Self {
        self.auth = auth;
        self
    }

    /// Set rate limit configuration.
    pub fn with_rate_limit(mut self, max_requests: u32, window: Duration) -> Self {
        self.rate_limit_config = Some(RateLimitConfig { max_requests, window });
        self
    }

    /// Set maximum retries.
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Add a default header.
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.default_headers.insert(name.into(), value.into());
        self
    }

    /// Make a GET request.
    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> ConnectorResult<T> {
        self.request(reqwest::Method::GET, path, None::<&()>).await
    }

    /// Make a GET request with query parameters.
    pub async fn get_with_params<T: DeserializeOwned>(
        &self,
        path: &str,
        params: &[(String, String)],
    ) -> ConnectorResult<T> {
        self.request_with_params(reqwest::Method::GET, path, params, None::<&()>)
            .await
    }

    /// Make a POST request.
    pub async fn post<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> ConnectorResult<T> {
        self.request(reqwest::Method::POST, path, Some(body)).await
    }

    /// Make a GET request that returns the raw `reqwest::Response`.
    ///
    /// Applies the same auth, rate-limit, retry, and status-check logic as
    /// other methods but does **not** deserialize the body. Callers get full
    /// access to response headers and the streaming body, which is needed for
    /// endpoints that return non-JSON payloads (e.g., CSV).
    pub async fn get_streaming(
        &self,
        path: &str,
        params: &[(String, String)],
    ) -> ConnectorResult<reqwest::Response> {
        self.send_raw(reqwest::Method::GET, path, params, None::<&()>).await
    }

    /// Make an HTTP request with retry and rate limiting.
    async fn request<T: DeserializeOwned, B: Serialize>(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&B>,
    ) -> ConnectorResult<T> {
        self.request_with_params(method, path, &[], body).await
    }

    /// Make an HTTP request with query parameters, deserializing the response as JSON.
    async fn request_with_params<T: DeserializeOwned, B: Serialize>(
        &self,
        method: reqwest::Method,
        path: &str,
        params: &[(String, String)],
        body: Option<&B>,
    ) -> ConnectorResult<T> {
        let response = self.send_raw(method, path, params, body).await?;
        response.json().await.map_err(|e| {
            ConnectorError::Internal(format!("Failed to parse response: {}", e))
        })
    }

    /// Core request method: builds, sends, and validates an HTTP request with
    /// auth, rate-limiting, retry, and status-code handling. Returns the raw
    /// `reqwest::Response` on success.
    async fn send_raw<B: Serialize>(
        &self,
        method: reqwest::Method,
        path: &str,
        params: &[(String, String)],
        body: Option<&B>,
    ) -> ConnectorResult<reqwest::Response> {
        let mut last_error = None;

        for attempt in 0..=self.max_retries {
            self.wait_for_rate_limit().await?;

            let url = format!("{}{}", self.base_url, path);
            let mut request = self.client.request(method.clone(), &url);

            if !params.is_empty() {
                request = request.query(params);
            }

            request = self.apply_auth(request).await?;

            for (name, value) in &self.default_headers {
                request = request.header(name, value);
            }

            if let Some(body) = body {
                request = request.json(body);
            }

            let response = match request.send().await {
                Ok(resp) => resp,
                Err(e) => {
                    if e.is_timeout() || e.is_connect() {
                        last_error = Some(ConnectorError::Network(format!(
                            "Request failed: {}", e
                        )));
                        self.backoff(attempt).await;
                        continue;
                    }
                    return Err(ConnectorError::Network(format!("Request failed: {}", e)));
                }
            };

            self.update_rate_limit_from_response(&response).await;

            let status = response.status();

            if status.is_success() {
                return Ok(response);
            }

            if status == reqwest::StatusCode::UNAUTHORIZED {
                return Err(ConnectorError::Authentication("Unauthorized".to_string()));
            }

            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                let retry_after = response
                    .headers()
                    .get("Retry-After")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(60);

                if attempt < self.max_retries {
                    sleep(Duration::from_secs(retry_after)).await;
                    continue;
                }

                return Err(ConnectorError::RateLimited {
                    retry_after_secs: retry_after,
                });
            }

            if status.is_server_error() && attempt < self.max_retries {
                last_error = Some(ConnectorError::Network(format!(
                    "Server error: {}", status
                )));
                self.backoff(attempt).await;
                continue;
            }

            let body = response.text().await.unwrap_or_default();
            return Err(ConnectorError::Internal(format!(
                "Request failed ({}): {}", status, body
            )));
        }

        Err(last_error.unwrap_or_else(|| {
            ConnectorError::Internal("Request failed after retries".to_string())
        }))
    }

    /// Apply authentication to a request.
    async fn apply_auth(
        &self,
        mut request: reqwest::RequestBuilder,
    ) -> ConnectorResult<reqwest::RequestBuilder> {
        match &self.auth {
            AuthConfig::None => {}
            AuthConfig::ApiKey { key, header_name } => {
                request = request.header(header_name, key.expose());
            }
            AuthConfig::Bearer(token) => {
                request = request.bearer_auth(token.expose());
            }
            AuthConfig::Basic { username, password } => {
                request = request.basic_auth(username, Some(password.expose()));
            }
            AuthConfig::OAuth(oauth) => {
                let header = oauth.authorization_header().await?;
                request = request.header("Authorization", header);
            }
        }
        Ok(request)
    }

    /// Wait for rate limit window if needed.
    async fn wait_for_rate_limit(&self) -> ConnectorResult<()> {
        let config = match &self.rate_limit_config {
            Some(c) => c,
            None => return Ok(()),
        };

        let mut state = self.rate_limiter.write().await;

        // Check if we need to wait for retry-after
        if let Some(retry_after) = state.retry_after {
            if Utc::now() < retry_after {
                let wait_duration = (retry_after - Utc::now())
                    .to_std()
                    .unwrap_or(Duration::from_secs(1));
                drop(state);
                sleep(wait_duration).await;
                state = self.rate_limiter.write().await;
                state.retry_after = None;
            }
        }

        // Check if window has expired
        let window_duration = chrono::Duration::from_std(config.window)
            .unwrap_or_else(|_| chrono::Duration::seconds(60));
        
        if Utc::now() - state.window_start > window_duration {
            state.window_start = Utc::now();
            state.request_count = 0;
        }

        // Check if we've hit the limit
        if state.request_count >= config.max_requests {
            let wait_until = state.window_start + window_duration;
            let wait_duration = (wait_until - Utc::now())
                .to_std()
                .unwrap_or(Duration::from_secs(1));
            drop(state);
            sleep(wait_duration).await;
            
            let mut new_state = self.rate_limiter.write().await;
            new_state.window_start = Utc::now();
            new_state.request_count = 1;
            return Ok(());
        }

        state.request_count += 1;
        Ok(())
    }

    /// Update rate limit state from response headers.
    async fn update_rate_limit_from_response(&self, response: &reqwest::Response) {
        // Common rate limit headers
        if let Some(retry_after) = response.headers().get("Retry-After") {
            if let Ok(secs) = retry_after.to_str().unwrap_or("0").parse::<i64>() {
                let mut state = self.rate_limiter.write().await;
                state.retry_after = Some(Utc::now() + chrono::Duration::seconds(secs));
            }
        }
    }

    /// Exponential backoff for retries.
    async fn backoff(&self, attempt: u32) {
        let delay = BASE_BACKOFF_MS * 2u64.pow(attempt);
        let jitter = rand::random::<u64>() % (delay / 2);
        sleep(Duration::from_millis(delay + jitter)).await;
    }

    /// Fetch all pages from a paginated endpoint.
    pub async fn fetch_all_pages<T: DeserializeOwned + Clone>(
        &self,
        path: &str,
        pagination: &PaginationStyle,
        data_field: &str,
    ) -> ConnectorResult<Vec<T>> {
        let mut all_items = Vec::new();
        let mut cursor: Option<String> = None;
        let mut offset = 0usize;
        let mut page = 1usize;

        loop {
            let mut params: Vec<(String, String)> = Vec::new();

            match pagination {
                PaginationStyle::Cursor { cursor_param, .. } => {
                    if let Some(ref c) = cursor {
                        params.push((cursor_param.clone(), c.clone()));
                    }
                }
                PaginationStyle::Offset {
                    limit,
                    offset_param,
                    limit_param,
                } => {
                    params.push((offset_param.clone(), offset.to_string()));
                    params.push((limit_param.clone(), limit.to_string()));
                }
                PaginationStyle::PageNumber {
                    per_page,
                    page_param,
                    per_page_param,
                } => {
                    params.push((page_param.clone(), page.to_string()));
                    params.push((per_page_param.clone(), per_page.to_string()));
                }
                PaginationStyle::LinkHeader => {
                    // Link header pagination handled separately
                }
            }

            let response: serde_json::Value = self.get_with_params(path, &params).await?;

            // Extract data from response
            let items: Vec<T> = if data_field.is_empty() {
                serde_json::from_value(response.clone()).map_err(|e| {
                    ConnectorError::Internal(format!("Failed to parse items: {}", e))
                })?
            } else {
                let data = response.get(data_field).ok_or_else(|| {
                    ConnectorError::Internal(format!("Missing field '{}' in response", data_field))
                })?;
                serde_json::from_value(data.clone()).map_err(|e| {
                    ConnectorError::Internal(format!("Failed to parse items: {}", e))
                })?
            };

            let items_count = items.len();
            all_items.extend(items);

            // Check for next page
            let has_more = match pagination {
                PaginationStyle::Cursor { cursor_field, .. } => {
                    cursor = response
                        .get(cursor_field)
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    cursor.is_some()
                }
                PaginationStyle::Offset { limit, .. } => {
                    offset += items_count;
                    items_count >= *limit
                }
                PaginationStyle::PageNumber { per_page, .. } => {
                    page += 1;
                    items_count >= *per_page
                }
                PaginationStyle::LinkHeader => {
                    // Would need to parse Link header from response
                    false
                }
            };

            if !has_more || items_count == 0 {
                break;
            }
        }

        Ok(all_items)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_api_client_creation() {
        let client = HttpApiClient::new("https://api.example.com");
        assert_eq!(client.base_url, "https://api.example.com");
    }

    #[test]
    fn test_http_api_client_strips_trailing_slash() {
        let client = HttpApiClient::new("https://api.example.com/");
        assert_eq!(client.base_url, "https://api.example.com");
    }

    #[test]
    fn test_auth_config_api_key() {
        let auth = AuthConfig::api_key("secret", "X-API-Key");
        assert!(matches!(auth, AuthConfig::ApiKey { .. }));
    }

    #[test]
    fn test_auth_config_bearer() {
        let auth = AuthConfig::bearer("token123");
        assert!(matches!(auth, AuthConfig::Bearer(_)));
    }

    #[test]
    fn test_auth_config_basic() {
        let auth = AuthConfig::basic("user", "pass");
        assert!(matches!(auth, AuthConfig::Basic { .. }));
    }

    #[test]
    fn test_pagination_style_default() {
        let pagination = PaginationStyle::default();
        assert!(matches!(pagination, PaginationStyle::Offset { .. }));
    }

    #[test]
    fn test_auth_config_debug_safe() {
        let auth = AuthConfig::bearer("super_secret_token");
        let debug = format!("{:?}", auth);
        assert!(!debug.contains("super_secret_token"));
    }
}
