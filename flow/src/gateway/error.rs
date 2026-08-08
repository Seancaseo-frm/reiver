//! Gateway-specific error types.
//!
//! These errors are designed to produce OpenAI-compatible error responses.

use crate::gateway::domain_types::GuardrailRule;
use crate::gateway::provider_types::Provider;
use crate::gateway::types::ErrorResponse;
use axum::{
    http::{header::HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};

/// Patterns that indicate sensitive information in error messages.
const SENSITIVE_PATTERNS: &[&str] = &[
    "api_key",
    "api-key",
    "apikey",
    "secret",
    "auth_token",
    "access_token",
    "bearer",
    "authorization",
    "credential",
    "password",
    "access_key",
    "secret_key",
    "session_token",
];

/// Sanitize a provider error message to remove potentially sensitive information.
///
/// This function:
/// 1. Removes lines containing sensitive patterns (case-insensitive)
/// 2. Truncates very long messages that might contain stack traces
/// 3. Replaces internal details with generic messages for certain error types
///
/// # Security
/// Provider error messages may contain sensitive information like:
/// - API keys or tokens in request headers
/// - Internal service URLs
/// - Stack traces with file paths
/// - Database connection strings
fn sanitize_error_message(message: &str, status: u16) -> String {
    // For server errors (5xx), return a generic message
    if status >= 500 {
        return "The AI provider encountered an internal error. Please try again.".to_string();
    }

    // For auth errors (401/403), return a generic message to avoid leaking details
    if status == 401 || status == 403 {
        return "Authentication with the AI provider failed. Please check your API key configuration.".to_string();
    }

    // Check for sensitive patterns (case-insensitive)
    let message_lower = message.to_lowercase();
    for pattern in SENSITIVE_PATTERNS {
        if message_lower.contains(pattern) {
            tracing::warn!(
                "Sanitized provider error message containing sensitive pattern: {}",
                pattern
            );
            return "The AI provider returned an error. Please check your request and try again."
                .to_string();
        }
    }

    // Truncate very long messages (likely contain stack traces or internal details)
    const MAX_MESSAGE_LENGTH: usize = 500;
    if message.len() > MAX_MESSAGE_LENGTH {
        tracing::warn!(
            "Truncated provider error message from {} to {} chars",
            message.len(),
            MAX_MESSAGE_LENGTH
        );
        let truncated: String = message.chars().take(MAX_MESSAGE_LENGTH).collect();
        return format!("{}...", truncated);
    }

    message.to_string()
}

/// Gateway-specific errors.
#[derive(Debug)]
pub enum GatewayError {
    /// The requested model is not supported by any provider.
    UnsupportedModel(String),

    /// Authentication failed (invalid API key).
    AuthenticationFailed(String),

    /// Rate limit exceeded.
    RateLimitExceeded { limit: u32, reset_seconds: u64 },

    /// Provider API returned an error.
    ProviderError {
        provider: Provider,
        status: u16,
        message: String,
    },

    /// Request validation failed.
    ValidationError(String),

    /// Provider API key not configured.
    MissingProviderKey(String),

    /// Network or connection error.
    NetworkError(String),

    /// Internal gateway error.
    InternalError(String),

    /// Request timeout.
    Timeout(String),

    /// Session cost budget exceeded.
    SessionBudgetExceeded {
        limit_usd: f64,
        used_usd: f64,
        session_id: String,
    },

    /// A guardrail rule was triggered and the request was blocked.
    GuardrailViolation {
        rule: GuardrailRule,
        /// Human-readable explanation returned in the error body.
        detail: String,
    },

    /// A prompt template variable failed schema validation.
    PromptVariableValidation {
        /// The name of the variable that failed validation.
        variable: String,
        /// Human-readable reason (type mismatch, not in enum, exceeds max_chars, etc.).
        detail: String,
    },

    /// The LLM response did not conform to the prompt version's output contract (JSON schema).
    OutputContractViolation {
        /// Human-readable description of the schema violation.
        detail: String,
    },

    /// Organization must add a payment method or subscription to continue.
    PaymentRequired,
}

impl std::fmt::Display for GatewayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GatewayError::UnsupportedModel(model) => {
                write!(f, "Model '{}' is not supported", model)
            }
            GatewayError::AuthenticationFailed(msg) => {
                write!(f, "Authentication failed: {}", msg)
            }
            GatewayError::RateLimitExceeded { limit, .. } => {
                write!(f, "Rate limit exceeded: {} requests", limit)
            }
            GatewayError::ProviderError {
                provider, message, ..
            } => {
                write!(f, "Provider '{}' error: {}", provider, message)
            }
            GatewayError::ValidationError(msg) => {
                write!(f, "Validation error: {}", msg)
            }
            GatewayError::MissingProviderKey(msg) => {
                write!(f, "{}", msg)
            }
            GatewayError::NetworkError(msg) => {
                write!(f, "Network error: {}", msg)
            }
            GatewayError::InternalError(msg) => {
                write!(f, "Internal error: {}", msg)
            }
            GatewayError::Timeout(msg) => {
                write!(f, "Request timeout: {}", msg)
            }
            GatewayError::SessionBudgetExceeded {
                limit_usd,
                used_usd,
                session_id,
            } => {
                write!(
                    f,
                    "Session budget exceeded for session '{}': used ${:.6}, limit ${:.6}",
                    session_id, used_usd, limit_usd
                )
            }
            GatewayError::GuardrailViolation { rule, detail } => {
                write!(f, "Guardrail violation [{}]: {}", rule, detail)
            }
            GatewayError::PromptVariableValidation { variable, detail } => {
                write!(
                    f,
                    "Prompt variable validation failed for '{}': {}",
                    variable, detail
                )
            }
            GatewayError::OutputContractViolation { detail } => {
                write!(f, "Output contract violation: {}", detail)
            }
            GatewayError::PaymentRequired => {
                write!(f, "Payment method required")
            }
        }
    }
}

impl std::error::Error for GatewayError {}

impl GatewayError {
    /// Short discriminant label for structured logging / traces.
    pub fn error_type_str(&self) -> &'static str {
        match self {
            GatewayError::UnsupportedModel(_) => "unsupported_model",
            GatewayError::AuthenticationFailed(_) => "authentication_failed",
            GatewayError::RateLimitExceeded { .. } => "rate_limit_exceeded",
            GatewayError::ProviderError { .. } => "provider_error",
            GatewayError::ValidationError(_) => "validation_error",
            GatewayError::MissingProviderKey(_) => "missing_provider_key",
            GatewayError::NetworkError(_) => "network_error",
            GatewayError::InternalError(_) => "internal_error",
            GatewayError::Timeout(_) => "timeout",
            GatewayError::SessionBudgetExceeded { .. } => "session_budget_exceeded",
            GatewayError::GuardrailViolation { .. } => "guardrail_violation",
            GatewayError::PromptVariableValidation { .. } => "prompt_variable_validation",
            GatewayError::OutputContractViolation { .. } => "output_contract_violation",
            GatewayError::PaymentRequired => "payment_required",
        }
    }

    /// Return a sanitized `(error_type, message)` pair suitable for sending
    /// to clients. Mirrors the logic in `IntoResponse` but without constructing
    /// an HTTP response, so it can be used in streaming error events.
    pub fn client_facing_details(&self) -> (&'static str, String) {
        match self {
            GatewayError::UnsupportedModel(model) => (
                "invalid_request_error",
                format!(
                    "The model '{}' does not exist or you do not have access to it.",
                    model
                ),
            ),
            GatewayError::AuthenticationFailed(_) => (
                "authentication_error",
                "Authentication with the AI provider failed.".to_string(),
            ),
            GatewayError::RateLimitExceeded { .. } => (
                "rate_limit_error",
                "Rate limit exceeded. Please retry after some time.".to_string(),
            ),
            GatewayError::ProviderError {
                status, message, ..
            } => {
                let sanitized = sanitize_error_message(message, *status);
                ("api_error", sanitized)
            }
            GatewayError::ValidationError(msg) => ("invalid_request_error", msg.clone()),
            GatewayError::MissingProviderKey(msg) => ("invalid_request_error", format!("{}.", msg)),
            GatewayError::NetworkError(_) => (
                "api_error",
                "Failed to connect to the AI provider. Please try again.".to_string(),
            ),
            GatewayError::InternalError(_) => (
                "server_error",
                "An internal error occurred. Please try again.".to_string(),
            ),
            GatewayError::Timeout(_) => (
                "timeout_error",
                "The request to the AI provider timed out. Please try again.".to_string(),
            ),
            GatewayError::SessionBudgetExceeded {
                limit_usd,
                used_usd,
                ..
            } => (
                "session_budget_exceeded",
                format!(
                    "Session cost budget of ${:.6} exceeded (accumulated: ${:.6}).",
                    limit_usd, used_usd
                ),
            ),
            GatewayError::GuardrailViolation { detail, .. } => {
                ("guardrail_violation", detail.clone())
            }
            GatewayError::PromptVariableValidation { variable, detail } => (
                "prompt_variable_validation",
                format!("Variable '{}': {}", variable, detail),
            ),
            GatewayError::OutputContractViolation { detail } => {
                ("output_contract_violation", detail.clone())
            }
            GatewayError::PaymentRequired => (
                "payment_required",
                "Please add a payment method or subscription to continue using this service."
                    .to_string(),
            ),
        }
    }
}

impl From<reqwest::Error> for GatewayError {
    fn from(err: reqwest::Error) -> Self {
        if err.is_timeout() {
            GatewayError::Timeout(err.to_string())
        } else if err.is_connect() {
            GatewayError::NetworkError(format!("Connection failed: {}", err))
        } else {
            GatewayError::NetworkError(err.to_string())
        }
    }
}

impl IntoResponse for GatewayError {
    fn into_response(self) -> Response {
        let (status, error_type, message) = match &self {
            GatewayError::UnsupportedModel(model) => (
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                format!("The model '{}' does not exist or you do not have access to it.", model),
            ),
            GatewayError::AuthenticationFailed(msg) => (
                StatusCode::UNAUTHORIZED,
                "authentication_error",
                msg.clone(),
            ),
            GatewayError::RateLimitExceeded { .. } => (
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limit_error",
                "Rate limit exceeded. Please retry after some time.".to_string(),
            ),
            GatewayError::ProviderError { status, message, provider } => {
                // Log the full error for debugging, then sanitize for client response
                tracing::warn!(
                    provider = %provider,
                    status = %status,
                    "Provider error (full message logged at debug level)"
                );
                tracing::debug!(
                    provider = %provider,
                    status = %status,
                    message = %message,
                    "Provider error details"
                );

                let http_status = StatusCode::from_u16(*status)
                    .unwrap_or(StatusCode::BAD_GATEWAY);
                let sanitized_message = sanitize_error_message(message, *status);
                (http_status, "api_error", sanitized_message)
            }
            GatewayError::ValidationError(msg) => (
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                msg.clone(),
            ),
            GatewayError::MissingProviderKey(msg) => (
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                format!("{}. Please configure it in your project settings.", msg),
            ),
            GatewayError::NetworkError(msg) => {
                tracing::error!("Gateway network error: {}", msg);
                (
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "Failed to connect to the AI provider. Please try again.".to_string(),
                )
            }
            GatewayError::InternalError(msg) => {
                tracing::error!("Gateway internal error: {}", msg);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "server_error",
                    "An internal error occurred. Please try again.".to_string(),
                )
            }
            GatewayError::Timeout(msg) => {
                tracing::warn!("Gateway request timeout: {}", msg);
                (
                    StatusCode::GATEWAY_TIMEOUT,
                    "timeout_error",
                    "The request to the AI provider timed out. Please try again.".to_string(),
                )
            }
            GatewayError::SessionBudgetExceeded { limit_usd, used_usd, .. } => {
                (
                    StatusCode::TOO_MANY_REQUESTS,
                    "session_budget_exceeded",
                    format!(
                        "Session cost budget of ${:.6} exceeded (accumulated: ${:.6}). Start a new session to continue.",
                        limit_usd, used_usd
                    ),
                )
            }
            GatewayError::GuardrailViolation { detail, .. } => (
                StatusCode::BAD_REQUEST,
                "guardrail_violation",
                detail.clone(),
            ),
            GatewayError::PromptVariableValidation { variable, detail } => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "prompt_variable_validation",
                format!("Variable '{}': {}", variable, detail),
            ),
            GatewayError::OutputContractViolation { detail } => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "output_contract_violation",
                detail.clone(),
            ),
            GatewayError::PaymentRequired => (
                StatusCode::PAYMENT_REQUIRED,
                "payment_required",
                "Please add a payment method or subscription to continue using this service.".to_string(),
            ),
        };

        let error_response = ErrorResponse::new(message, error_type);

        // Add rate limit headers if applicable
        let mut response = (status, Json(error_response)).into_response();

        if let GatewayError::RateLimitExceeded {
            limit,
            reset_seconds,
        } = &self
        {
            let headers = response.headers_mut();
            headers.insert(
                axum::http::header::HeaderName::from_static("x-ratelimit-limit"),
                limit
                    .to_string()
                    .parse()
                    .unwrap_or_else(|_| HeaderValue::from_static("0")),
            );
            headers.insert(
                axum::http::header::HeaderName::from_static("x-ratelimit-remaining"),
                "0".parse()
                    .unwrap_or_else(|_| HeaderValue::from_static("0")),
            );
            headers.insert(
                axum::http::header::HeaderName::from_static("retry-after"),
                reset_seconds
                    .to_string()
                    .parse()
                    .unwrap_or_else(|_| HeaderValue::from_static("0")),
            );
        }

        if let GatewayError::SessionBudgetExceeded {
            limit_usd,
            used_usd,
            ..
        } = &self
        {
            let headers = response.headers_mut();
            if let Ok(v) = format!("{:.6}", limit_usd).parse() {
                headers.insert(
                    axum::http::header::HeaderName::from_static("x-session-budget-limit"),
                    v,
                );
            }
            if let Ok(v) = format!("{:.6}", used_usd).parse() {
                headers.insert(
                    axum::http::header::HeaderName::from_static("x-session-budget-used"),
                    v,
                );
            }
        }

        if let GatewayError::GuardrailViolation { rule, .. } = &self {
            let headers = response.headers_mut();
            headers.insert(
                axum::http::header::HeaderName::from_static("x-guardrail-rule"),
                HeaderValue::from_static(rule.as_str()),
            );
        }

        if let GatewayError::PromptVariableValidation { variable, .. } = &self {
            let headers = response.headers_mut();
            if let Ok(v) = variable.parse() {
                headers.insert(
                    axum::http::header::HeaderName::from_static("x-invalid-variable"),
                    v,
                );
            }
        }

        if let GatewayError::OutputContractViolation { .. } = &self {
            let headers = response.headers_mut();
            headers.insert(
                axum::http::header::HeaderName::from_static("x-output-contract-violation"),
                "true"
                    .parse()
                    .unwrap_or_else(|_| HeaderValue::from_static("true")),
            );
        }

        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unsupported_model_error() {
        let err = GatewayError::UnsupportedModel("unknown-model".to_string());
        assert!(err.to_string().contains("unknown-model"));
    }

    #[test]
    fn test_provider_error() {
        let err = GatewayError::ProviderError {
            provider: Provider::OpenAi,
            status: 429,
            message: "Rate limit exceeded".to_string(),
        };
        assert!(err.to_string().contains("openai"));
    }

    #[test]
    fn test_sanitize_server_error() {
        let message = "Internal server error with stack trace: at line 123 in /var/app/src/main.rs";
        let sanitized = sanitize_error_message(message, 500);
        assert_eq!(
            sanitized,
            "The AI provider encountered an internal error. Please try again."
        );
    }

    #[test]
    fn test_sanitize_auth_error() {
        let message = "Invalid API key: sk-abc123xyz";
        let sanitized = sanitize_error_message(message, 401);
        assert_eq!(
            sanitized,
            "Authentication with the AI provider failed. Please check your API key configuration."
        );
    }

    #[test]
    fn test_sanitize_forbidden_error() {
        let message = "Access denied for user with token xyz123";
        let sanitized = sanitize_error_message(message, 403);
        assert_eq!(
            sanitized,
            "Authentication with the AI provider failed. Please check your API key configuration."
        );
    }

    #[test]
    fn test_sanitize_sensitive_pattern_api_key() {
        let message = "Error: api_key is invalid";
        let sanitized = sanitize_error_message(message, 400);
        assert_eq!(
            sanitized,
            "The AI provider returned an error. Please check your request and try again."
        );
    }

    #[test]
    fn test_sanitize_sensitive_pattern_secret() {
        let message = "Invalid secret provided in request";
        let sanitized = sanitize_error_message(message, 400);
        assert_eq!(
            sanitized,
            "The AI provider returned an error. Please check your request and try again."
        );
    }

    #[test]
    fn test_sanitize_sensitive_pattern_bearer() {
        let message = "Bearer token expired";
        let sanitized = sanitize_error_message(message, 400);
        assert_eq!(
            sanitized,
            "The AI provider returned an error. Please check your request and try again."
        );
    }

    #[test]
    fn test_sanitize_long_message() {
        let message = "a".repeat(600);
        let sanitized = sanitize_error_message(&message, 400);
        assert!(sanitized.ends_with("..."));
        assert!(sanitized.len() <= 503); // 500 + "..."
    }

    #[test]
    fn test_sanitize_safe_message() {
        let message = "Invalid model: gpt-5-turbo does not exist";
        let sanitized = sanitize_error_message(message, 400);
        assert_eq!(sanitized, message);
    }

    #[test]
    fn test_sanitize_rate_limit_message() {
        let message = "Rate limit exceeded. Please retry after 60 seconds.";
        let sanitized = sanitize_error_message(message, 429);
        assert_eq!(sanitized, message);
    }

    /// Regression: the bare pattern "token" matched legitimate LLM error
    /// messages about token limits (e.g. "exceeded token limit of 4096"),
    /// hiding actionable debugging information from users. The fix narrows
    /// the pattern to "auth_token" and "access_token".
    #[test]
    fn test_sanitize_preserves_token_limit_errors() {
        let messages = [
            "exceeded token limit of 4096",
            "maximum token count reached",
            "invalid token count in request",
            "This model's maximum context length is 128000 tokens",
        ];
        for msg in messages {
            let sanitized = sanitize_error_message(msg, 400);
            assert_eq!(
                sanitized, msg,
                "Token-limit message must not be sanitized: '{}'",
                msg
            );
        }
    }

    #[test]
    fn test_sanitize_still_catches_auth_token() {
        let sanitized = sanitize_error_message("Invalid auth_token provided", 400);
        assert_eq!(
            sanitized,
            "The AI provider returned an error. Please check your request and try again."
        );
    }

    #[test]
    fn test_sanitize_still_catches_access_token() {
        let sanitized = sanitize_error_message("Bad access_token in header", 400);
        assert_eq!(
            sanitized,
            "The AI provider returned an error. Please check your request and try again."
        );
    }

    /// Regression: truncating with byte-slicing (`&message[..500]`) panics when
    /// byte 500 falls inside a multi-byte UTF-8 character. The fix uses
    /// `chars().take(N)` to find a safe character boundary.
    #[test]
    fn test_sanitize_long_message_with_multibyte_chars() {
        // Each CJK character is 3 bytes in UTF-8, so 200 chars = 600 bytes.
        let message = "\u{4e16}".repeat(200); // 200 x '世'
        assert_eq!(message.len(), 600);
        let sanitized = sanitize_error_message(&message, 400);
        assert!(sanitized.ends_with("..."));
        assert!(
            sanitized.chars().count() <= 503,
            "Truncated message should be at most 500 chars + '...'"
        );
    }

    #[test]
    fn test_payment_required_display() {
        let err = GatewayError::PaymentRequired;
        assert_eq!(err.to_string(), "Payment method required");
    }

    #[test]
    fn test_payment_required_client_facing() {
        let err = GatewayError::PaymentRequired;
        let (error_type, msg) = err.client_facing_details();
        assert_eq!(error_type, "payment_required");
        assert!(msg.contains("payment method"));
    }

    #[test]
    fn test_payment_required_status_code() {
        let err = GatewayError::PaymentRequired;
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::PAYMENT_REQUIRED);
    }

    #[tokio::test]
    async fn test_payment_required_no_sensitive_info() {
        let err = GatewayError::PaymentRequired;
        let response = err.into_response();
        let (_, body) = response.into_parts();
        let bytes = axum::body::to_bytes(body, 4096).await.unwrap();
        let body_str = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(!body_str.contains("balance"));
        assert!(!body_str.contains("wallet"));
        assert!(!body_str.contains("redis"));
        assert!(!body_str.contains("postgres"));
    }
}
