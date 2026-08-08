//! Test helpers for API testing
//!
//! Provides utilities for creating test requests and parsing responses.

use axum::body::Body;
use axum::http::Request;
use serde::Serialize;

/// Create a JSON POST request
pub fn json_post(uri: &str, body: &impl Serialize) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(body).unwrap()))
        .unwrap()
}

/// Create a JSON GET request
pub fn json_get(uri: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::empty())
        .unwrap()
}

/// Create an authenticated request with a Bearer token
pub fn with_auth(request: Request<Body>, token: &str) -> Request<Body> {
    let (mut parts, body) = request.into_parts();
    parts.headers.insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    Request::from_parts(parts, body)
}

/// Create a test project key
pub fn test_project_key() -> String {
    format!(
        "pk_test_{}",
        uuid::Uuid::new_v4().to_string().replace("-", "")[..16].to_string()
    )
}

/// Create a test email
pub fn test_email() -> String {
    format!(
        "test-{}@example.com",
        uuid::Uuid::new_v4().to_string()[..8].to_string()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_json_post_creates_valid_request() {
        let body = json!({"key": "value"});
        let req = json_post("/api/test", &body);

        assert_eq!(req.method(), "POST");
        assert_eq!(req.uri(), "/api/test");
        assert_eq!(
            req.headers().get("content-type").unwrap(),
            "application/json"
        );
    }

    #[test]
    fn test_json_get_creates_valid_request() {
        let req = json_get("/api/test");

        assert_eq!(req.method(), "GET");
        assert_eq!(req.uri(), "/api/test");
    }

    #[test]
    fn test_with_auth_adds_header() {
        let req = json_get("/api/test");
        let authed = with_auth(req, "test_token");

        assert_eq!(
            authed.headers().get("authorization").unwrap(),
            "Bearer test_token"
        );
    }

    #[test]
    fn test_project_key_format() {
        let key = test_project_key();
        assert!(key.starts_with("pk_test_"));
        assert!(key.len() > 20);
    }

    #[test]
    fn test_email_format() {
        let email = test_email();
        assert!(email.contains('@'));
        assert!(email.ends_with("@example.com"));
    }
}
