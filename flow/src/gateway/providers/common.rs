//! Common utilities shared across LLM provider adapters.
//!
//! This module provides reusable functionality for:
//! - HTTP client creation with timeouts
//! - Error response parsing
//! - Common patterns for provider implementations

use reqwest::Client;
use std::time::Duration;

use crate::gateway::error::GatewayError;
use crate::gateway::provider_types::Provider;

/// Create an HTTP client with the specified timeout, tuned for high-throughput
/// LLM gateway usage.
///
/// # Arguments
/// * `timeout` - Request timeout duration
///
/// # Panics
/// Panics if the HTTP client cannot be created. This is extremely rare and indicates
/// a fundamental system issue (e.g., TLS backend initialization failure).
/// The error is logged before panicking to aid debugging.
pub fn create_http_client(timeout: Duration) -> Client {
    Client::builder()
        .timeout(timeout)
        .connect_timeout(Duration::from_secs(5))
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(64)
        .tcp_keepalive(Duration::from_secs(30))
        .tcp_nodelay(true)
        .http2_keep_alive_interval(Duration::from_secs(15))
        .http2_keep_alive_timeout(Duration::from_secs(10))
        .build()
        .unwrap_or_else(|e| {
            tracing::error!(
                error = %e,
                timeout_secs = timeout.as_secs(),
                "Failed to create HTTP client - this is a fatal error"
            );
            panic!("Failed to create HTTP client: {}", e)
        })
}

/// Parse a provider error response and create a GatewayError.
///
/// Attempts to parse the error as JSON following common provider patterns:
/// - `{"error": {"message": "..."}}`  (OpenAI, Anthropic, Google)
/// - Falls back to the raw error text if JSON parsing fails
///
/// Status 429 is mapped to `GatewayError::RateLimitExceeded` so that the
/// retry and fallback logic treats provider rate limits as retryable.
///
/// # Arguments
/// * `error_text` - Raw error response body
/// * `provider` - Provider name for error attribution
/// * `status` - HTTP status code
pub fn parse_provider_error(error_text: &str, provider: Provider, status: u16) -> GatewayError {
    let message = if let Ok(error_json) = serde_json::from_str::<serde_json::Value>(error_text) {
        error_json
            .get("error")
            .and_then(|e| e.get("message"))
            .and_then(|m| m.as_str())
            .unwrap_or(error_text)
            .to_string()
    } else {
        error_text.to_string()
    };

    record_otel_error(&format!("{provider} returned {status}: {message}"));

    if status == 429 {
        tracing::warn!(
            provider = %provider,
            message = %message,
            "Provider returned 429 rate limit"
        );
        return GatewayError::RateLimitExceeded {
            limit: 0,
            reset_seconds: 30,
        };
    }

    GatewayError::ProviderError {
        provider,
        status,
        message,
    }
}

/// Record an error on the current span's OpenTelemetry status fields.
///
/// Requires the enclosing span to declare `otel.status_code` and
/// `otel.status_message` as `tracing::field::Empty`.
pub fn record_otel_error(message: &str) {
    let span = tracing::Span::current();
    span.record("otel.status_code", "ERROR");
    span.record("otel.status_message", message);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_http_client() {
        let client = create_http_client(Duration::from_secs(30));
        // Just verify it doesn't panic and returns a valid client
        assert!(client.get("http://example.com").build().is_ok());
    }

    #[test]
    fn test_parse_provider_error_json() {
        let error_text =
            r#"{"error": {"message": "Invalid model", "type": "invalid_request_error"}}"#;
        let error = parse_provider_error(error_text, Provider::OpenAi, 400);

        match error {
            GatewayError::ProviderError {
                provider,
                status,
                message,
            } => {
                assert_eq!(provider, Provider::OpenAi);
                assert_eq!(status, 400);
                assert_eq!(message, "Invalid model");
            }
            _ => panic!("Expected ProviderError"),
        }
    }

    /// Regression: provider 429 responses were wrapped as `ProviderError { status: 429 }`
    /// which `is_retryable_error` and `should_fallback` did not treat as retryable.
    /// The fix maps status 429 to `RateLimitExceeded` so the retry/fallback system
    /// handles provider rate limits correctly.
    #[test]
    fn test_parse_provider_error_429_returns_rate_limit_exceeded() {
        let error_text =
            r#"{"error": {"message": "Rate limit exceeded", "type": "rate_limit_error"}}"#;
        let error = parse_provider_error(error_text, Provider::OpenAi, 429);

        assert!(
            matches!(error, GatewayError::RateLimitExceeded { .. }),
            "Status 429 must produce RateLimitExceeded, got: {:?}",
            error
        );
    }

    #[test]
    fn test_parse_provider_error_429_plain_text() {
        let error = parse_provider_error("Too many requests", Provider::Anthropic, 429);
        assert!(
            matches!(error, GatewayError::RateLimitExceeded { .. }),
            "Plain-text 429 must also produce RateLimitExceeded, got: {:?}",
            error
        );
    }

    /// Regression: 429 responses hardcoded `reset_seconds: 0`, causing the
    /// `retry-after` response header to be 0. This told clients to retry
    /// immediately, worsening provider overload.
    #[test]
    fn test_parse_provider_error_429_has_nonzero_retry_after() {
        let error = parse_provider_error("Rate limit exceeded", Provider::OpenAi, 429);
        match error {
            GatewayError::RateLimitExceeded { reset_seconds, .. } => {
                assert!(
                    reset_seconds > 0,
                    "reset_seconds must be > 0 to prevent immediate client retries, got {}",
                    reset_seconds
                );
            }
            _ => panic!("Expected RateLimitExceeded, got: {:?}", error),
        }
    }

    #[test]
    fn test_parse_provider_error_plain_text() {
        let error_text = "Internal server error";
        let error = parse_provider_error(error_text, Provider::Anthropic, 500);

        match error {
            GatewayError::ProviderError {
                provider,
                status,
                message,
            } => {
                assert_eq!(provider, Provider::Anthropic);
                assert_eq!(status, 500);
                assert_eq!(message, "Internal server error");
            }
            _ => panic!("Expected ProviderError"),
        }
    }

    #[test]
    fn test_parse_provider_error_invalid_json() {
        let error_text = "{not valid json}";
        let error = parse_provider_error(error_text, Provider::Google, 400);

        match error {
            GatewayError::ProviderError {
                provider,
                status,
                message,
            } => {
                assert_eq!(provider, Provider::Google);
                assert_eq!(status, 400);
                assert_eq!(message, "{not valid json}");
            }
            _ => panic!("Expected ProviderError"),
        }
    }
}
