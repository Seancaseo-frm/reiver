use crate::rate_limit::RateLimitInfo;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use std::fmt;

#[derive(Debug)]
pub enum AppError {
    Database(sqlx::Error),
    DatabaseMsg(String),
    Redis(redis::RedisError),
    Auth(String),
    NotFound(String),
    Validation(String),
    BadRequest(String),
    Forbidden(String),
    Conflict(String),
    Gone(String),
    RateLimitExceeded(RateLimitInfo),
    External(String),
    Internal(anyhow::Error),
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppError::Database(e) => write!(f, "Database error: {}", e),
            AppError::DatabaseMsg(msg) => write!(f, "Database error: {}", msg),
            AppError::Redis(e) => write!(f, "Redis error: {}", e),
            AppError::Auth(msg) => write!(f, "Authentication error: {}", msg),
            AppError::NotFound(msg) => write!(f, "Not found: {}", msg),
            AppError::Validation(msg) => write!(f, "Validation error: {}", msg),
            AppError::BadRequest(msg) => write!(f, "Bad request: {}", msg),
            AppError::Forbidden(msg) => write!(f, "Forbidden: {}", msg),
            AppError::Conflict(msg) => write!(f, "Conflict: {}", msg),
            AppError::Gone(msg) => write!(f, "Gone: {}", msg),
            AppError::RateLimitExceeded(info) => {
                write!(f, "Rate limit exceeded: {}/{}", info.current, info.limit)
            }
            AppError::External(msg) => write!(f, "External service error: {}", msg),
            AppError::Internal(e) => write!(f, "Internal error: {}", e),
        }
    }
}

impl std::error::Error for AppError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            AppError::Database(e) => Some(e),
            AppError::Redis(e) => Some(e),
            AppError::Internal(e) => Some(e.as_ref()),
            _ => None,
        }
    }
}

impl From<sqlx::Error> for AppError {
    fn from(err: sqlx::Error) -> Self {
        AppError::Database(err)
    }
}

impl From<redis::RedisError> for AppError {
    fn from(err: redis::RedisError) -> Self {
        AppError::Redis(err)
    }
}

impl From<anyhow::Error> for AppError {
    fn from(err: anyhow::Error) -> Self {
        AppError::Internal(err)
    }
}

/// Error context stashed in the response's `Extensions` so tower middleware
/// (specifically the `TraceLayer.on_response` closure that owns the
/// `http.request` span) can record the real error kind + message on the
/// outer span.
///
/// Why this exists: `Span::current()` inside `AppError::into_response` is
/// the *innermost* span — usually a handler's own `#[tracing::instrument]`
/// span (e.g. `prompts.create_version`). That span doesn't declare the
/// `otel.status_message` / `error.kind` / `error.message` fields, so
/// `Span::record` on those names is a silent no-op (tracing only honors
/// `record` for fields that were declared as `Empty` at span creation).
///
/// Only the `http.request` span declares those fields. So instead of
/// trying to find and record on it from here, we stash the info in the
/// response and let `on_response` (which is handed the `http.request`
/// span directly) record it in the right place.
#[derive(Clone, Debug)]
pub struct AppErrorInfo {
    pub kind: &'static str,
    pub message: String,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        match &self {
            AppError::RateLimitExceeded(info) => {
                let detail = format!(
                    "Rate limit exceeded ({}/{}, reset {})",
                    info.current,
                    info.limit,
                    info.reset_at.to_rfc3339()
                );
                tracing::warn!(
                    kind = "rate_limit",
                    reason = %detail,
                    "429 rate limited"
                );
                let body = Json(json!({
                    "error": "Rate limit exceeded",
                    "limit": info.limit,
                    "reset_at": info.reset_at.to_rfc3339(),
                    "dropped": info.dropped,
                }));
                let mut response = (StatusCode::TOO_MANY_REQUESTS, body).into_response();
                let headers = response.headers_mut();
                headers.insert(
                    axum::http::header::HeaderName::from_static("x-ratelimit-limit"),
                    info.limit.to_string().parse().unwrap(),
                );
                headers.insert(
                    axum::http::header::HeaderName::from_static("x-ratelimit-remaining"),
                    "0".parse().unwrap(),
                );
                headers.insert(
                    axum::http::header::HeaderName::from_static("x-ratelimit-reset"),
                    info.reset_at.timestamp().to_string().parse().unwrap(),
                );
                response.extensions_mut().insert(AppErrorInfo {
                    kind: "rate_limit",
                    message: detail,
                });
                response
            }
            _ => {
                // `error_message` goes into the response `error` field (legacy
                // contract; internal variants return a generic label here).
                // `detail` is the real underlying reason, used for logs and
                // the span extension — never sent to the client for internal
                // errors.
                let (status, kind, detail, error_message) = match &self {
                    AppError::Database(e) => {
                        let msg = e.to_string();
                        tracing::error!(
                            kind = "database",
                            exception.type = "database",
                            exception.message = %msg,
                            error = %e,
                            "request failed"
                        );
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "database",
                            msg,
                            "Database error".to_string(),
                        )
                    }
                    AppError::DatabaseMsg(msg) => {
                        tracing::error!(
                            kind = "database",
                            exception.type = "database",
                            exception.message = %msg,
                            "request failed"
                        );
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "database",
                            msg.clone(),
                            "Database error".to_string(),
                        )
                    }
                    AppError::Redis(e) => {
                        let msg = e.to_string();
                        tracing::error!(
                            kind = "redis",
                            exception.type = "redis",
                            exception.message = %msg,
                            error = %e,
                            "request failed"
                        );
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "redis",
                            msg,
                            "Cache error".to_string(),
                        )
                    }
                    AppError::Auth(msg) => {
                        // Client-facing but high-signal: log at WARN so ops
                        // can see exactly *why* a 401 was returned.
                        tracing::warn!(kind = "auth", reason = %msg, "401 unauthorized");
                        (StatusCode::UNAUTHORIZED, "auth", msg.clone(), msg.clone())
                    }
                    AppError::NotFound(msg) => {
                        tracing::debug!(kind = "not_found", reason = %msg, "404 not found");
                        (StatusCode::NOT_FOUND, "not_found", msg.clone(), msg.clone())
                    }
                    AppError::Validation(msg) => {
                        tracing::info!(kind = "validation", reason = %msg, "400 validation error");
                        (
                            StatusCode::BAD_REQUEST,
                            "validation",
                            msg.clone(),
                            msg.clone(),
                        )
                    }
                    AppError::BadRequest(msg) => {
                        tracing::info!(kind = "bad_request", reason = %msg, "400 bad request");
                        (
                            StatusCode::BAD_REQUEST,
                            "bad_request",
                            msg.clone(),
                            msg.clone(),
                        )
                    }
                    AppError::Forbidden(msg) => {
                        tracing::warn!(kind = "forbidden", reason = %msg, "403 forbidden");
                        (StatusCode::FORBIDDEN, "forbidden", msg.clone(), msg.clone())
                    }
                    AppError::Conflict(msg) => {
                        tracing::info!(kind = "conflict", reason = %msg, "409 conflict");
                        (StatusCode::CONFLICT, "conflict", msg.clone(), msg.clone())
                    }
                    AppError::Gone(msg) => {
                        tracing::info!(kind = "gone", reason = %msg, "410 gone");
                        (StatusCode::GONE, "gone", msg.clone(), msg.clone())
                    }
                    AppError::External(msg) => {
                        tracing::error!(
                            kind = "external",
                            exception.type = "external",
                            exception.message = %msg,
                            "502 external service error"
                        );
                        (
                            StatusCode::BAD_GATEWAY,
                            "external",
                            msg.clone(),
                            "External service error".to_string(),
                        )
                    }
                    AppError::Internal(e) => {
                        let msg = e.to_string();
                        tracing::error!(
                            kind = "internal",
                            exception.type = "internal",
                            exception.message = %msg,
                            error = %e,
                            "500 internal error"
                        );
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "internal",
                            msg,
                            "Internal server error".to_string(),
                        )
                    }
                    _ => unreachable!(),
                };

                // SECURITY: Only include user-facing message in response
                // Internal error details are logged but not exposed to clients
                let user_message = match &self {
                    // User-facing errors: safe to include the message
                    AppError::Auth(msg) => msg.clone(),
                    AppError::NotFound(msg) => msg.clone(),
                    AppError::Validation(msg) => msg.clone(),
                    AppError::BadRequest(msg) => msg.clone(),
                    AppError::Forbidden(msg) => msg.clone(),
                    AppError::Conflict(msg) => msg.clone(),
                    AppError::Gone(msg) => msg.clone(),
                    // Internal errors: use generic message, don't expose details
                    AppError::Database(_) => "A database error occurred".to_string(),
                    AppError::DatabaseMsg(_) => "A database error occurred".to_string(),
                    AppError::Redis(_) => "A cache error occurred".to_string(),
                    AppError::External(_) => "An external service error occurred".to_string(),
                    AppError::Internal(_) => "An internal server error occurred".to_string(),
                    _ => "An unexpected error occurred".to_string(),
                };

                let body = Json(json!({
                    "error": error_message,
                    "message": user_message,
                }));

                let mut response = (status, body).into_response();
                response.extensions_mut().insert(AppErrorInfo {
                    kind,
                    message: detail,
                });
                response
            }
        }
    }
}

pub type Result<T> = std::result::Result<T, AppError>;

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;
    use chrono::{Duration, Utc};
    use std::error::Error;

    #[test]
    fn test_app_error_display() {
        let err = AppError::Validation("Invalid email".to_string());
        assert_eq!(err.to_string(), "Validation error: Invalid email");

        let err = AppError::NotFound("User not found".to_string());
        assert_eq!(err.to_string(), "Not found: User not found");

        let err = AppError::Auth("Invalid token".to_string());
        assert_eq!(err.to_string(), "Authentication error: Invalid token");
    }

    #[test]
    fn test_validation_error_status_code() {
        let err = AppError::Validation("Invalid input".to_string());
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_not_found_error_status_code() {
        let err = AppError::NotFound("Resource not found".to_string());
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn test_auth_error_status_code() {
        let err = AppError::Auth("Unauthorized".to_string());
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn test_external_error_status_code() {
        let err = AppError::External("Service unavailable".to_string());
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    }

    #[test]
    fn test_internal_error_status_code() {
        let err = AppError::Internal(anyhow::anyhow!("Something went wrong"));
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn test_rate_limit_error_status_and_headers() {
        let reset_at = Utc::now() + Duration::minutes(1);
        let info = RateLimitInfo {
            current: 101,
            limit: 100,
            reset_at,
            dropped: 1,
        };
        let err = AppError::RateLimitExceeded(info);
        let response = err.into_response();

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);

        // Check rate limit headers
        assert!(response.headers().contains_key("x-ratelimit-limit"));
        assert!(response.headers().contains_key("x-ratelimit-remaining"));
        assert!(response.headers().contains_key("x-ratelimit-reset"));

        assert_eq!(
            response
                .headers()
                .get("x-ratelimit-limit")
                .unwrap()
                .to_str()
                .unwrap(),
            "100"
        );
        assert_eq!(
            response
                .headers()
                .get("x-ratelimit-remaining")
                .unwrap()
                .to_str()
                .unwrap(),
            "0"
        );
    }

    #[test]
    fn test_rate_limit_display() {
        let info = RateLimitInfo {
            current: 150,
            limit: 100,
            reset_at: Utc::now(),
            dropped: 50,
        };
        let err = AppError::RateLimitExceeded(info);
        assert!(err.to_string().contains("150/100"));
    }

    #[test]
    fn test_from_anyhow_error() {
        let anyhow_err = anyhow::anyhow!("Test error");
        let app_err: AppError = anyhow_err.into();

        match app_err {
            AppError::Internal(_) => (),
            _ => panic!("Expected Internal error"),
        }
    }

    #[test]
    fn test_error_source() {
        let err = AppError::Validation("test".to_string());
        assert!(err.source().is_none());

        let internal_err = AppError::Internal(anyhow::anyhow!("test"));
        assert!(internal_err.source().is_some());
    }

    #[test]
    fn test_conflict_error_status_code() {
        let err = AppError::Conflict("Resource conflict".to_string());
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[test]
    fn test_conflict_error_display() {
        let err = AppError::Conflict("Already exists".to_string());
        assert_eq!(err.to_string(), "Conflict: Already exists");
    }
}
