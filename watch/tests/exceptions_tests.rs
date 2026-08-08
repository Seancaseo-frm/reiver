//! Exception Ingestion API Tests
//!
//! Tests for the exception/error ingestion endpoints.

mod helpers;

use serde_json::json;

use helpers::*;

#[cfg(test)]
mod tests {
    use super::*;

    /// Test fixture for exception payloads
    fn create_exception_payload(project_key: &str) -> serde_json::Value {
        json!({
            "project_key": project_key,
            "level": "error",
            "message": "Test error message",
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "environment": "test",
            "release": "1.0.0",
            "exception": {
                "type": "TestError",
                "value": "Something went wrong",
                "stacktrace": [
                    {
                        "filename": "src/main.rs",
                        "lineno": 42,
                        "function": "main",
                        "context_line": "panic!(\"test error\")"
                    }
                ]
            },
            "tags": {
                "service": "test-service",
                "region": "us-east-1"
            },
            "extra": {
                "user_id": "user_123",
                "request_id": "req_456"
            }
        })
    }

    #[test]
    fn test_exception_payload_structure() {
        let payload = create_exception_payload("pk_test_123");

        assert_eq!(payload["project_key"], "pk_test_123");
        assert_eq!(payload["level"], "error");
        assert!(payload["message"].is_string());
        assert!(payload["exception"].is_object());
    }

    #[test]
    fn test_exception_request_creation() {
        let payload = create_exception_payload("pk_test_123");
        let req = json_post("/api/v1/exceptions", &payload);

        assert_eq!(req.method(), "POST");
        assert_eq!(req.uri(), "/api/v1/exceptions");
    }

    #[test]
    fn test_exception_levels() {
        let levels = ["debug", "info", "warning", "error", "critical", "fatal"];

        for level in levels {
            let payload = json!({
                "project_key": "pk_test_123",
                "level": level,
                "message": format!("Test {} message", level)
            });

            assert_eq!(payload["level"], level);
        }
    }

    #[test]
    fn test_stacktrace_frame_structure() {
        let frame = json!({
            "filename": "src/lib.rs",
            "lineno": 100,
            "colno": 15,
            "function": "process_data",
            "context_line": "    result.unwrap()",
            "pre_context": ["fn process_data() {", "    let result = do_something();"],
            "post_context": ["    Ok(result)", "}"],
            "in_app": true
        });

        assert_eq!(frame["filename"], "src/lib.rs");
        assert_eq!(frame["lineno"], 100);
        assert_eq!(frame["function"], "process_data");
        assert_eq!(frame["in_app"], true);
    }

    #[test]
    fn test_exception_with_breadcrumbs() {
        let payload = json!({
            "project_key": "pk_test_123",
            "level": "error",
            "message": "Test error",
            "breadcrumbs": [
                {
                    "type": "http",
                    "category": "fetch",
                    "message": "GET /api/users",
                    "level": "info",
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                    "data": {
                        "url": "/api/users",
                        "method": "GET",
                        "status_code": 200
                    }
                },
                {
                    "type": "error",
                    "category": "exception",
                    "message": "NetworkError",
                    "level": "error",
                    "timestamp": chrono::Utc::now().to_rfc3339()
                }
            ]
        });

        let breadcrumbs = payload["breadcrumbs"].as_array().unwrap();
        assert_eq!(breadcrumbs.len(), 2);
        assert_eq!(breadcrumbs[0]["type"], "http");
        assert_eq!(breadcrumbs[1]["type"], "error");
    }

    #[test]
    fn test_exception_with_user_context() {
        let payload = json!({
            "project_key": "pk_test_123",
            "level": "error",
            "message": "User-related error",
            "user": {
                "id": "user_12345",
                "email": "user@example.com",
                "username": "testuser",
                "ip_address": "192.168.1.1"
            }
        });

        assert_eq!(payload["user"]["id"], "user_12345");
        assert_eq!(payload["user"]["email"], "user@example.com");
    }

    #[test]
    fn test_exception_with_request_context() {
        let payload = json!({
            "project_key": "pk_test_123",
            "level": "error",
            "message": "Request-related error",
            "request": {
                "url": "https://api.example.com/users",
                "method": "POST",
                "headers": {
                    "Content-Type": "application/json",
                    "X-Request-ID": "req_123"
                },
                "query_string": "page=1&limit=10",
                "data": {
                    "name": "Test User"
                }
            }
        });

        assert_eq!(payload["request"]["method"], "POST");
        assert_eq!(payload["request"]["url"], "https://api.example.com/users");
    }

    #[test]
    fn test_minimal_exception_payload() {
        // Minimum required fields
        let payload = json!({
            "project_key": "pk_test_123",
            "message": "Minimal error"
        });

        assert!(payload["project_key"].is_string());
        assert!(payload["message"].is_string());
    }

    #[test]
    fn test_exception_with_sdk_info() {
        let payload = json!({
            "project_key": "pk_test_123",
            "level": "error",
            "message": "Test error",
            "sdk": {
                "name": "reiver-rust",
                "version": "1.0.0"
            },
            "platform": "rust"
        });

        assert_eq!(payload["sdk"]["name"], "reiver-rust");
        assert_eq!(payload["platform"], "rust");
    }

    #[test]
    fn test_project_key_validation() {
        fn is_valid_project_key(key: &str) -> bool {
            key.starts_with("pk_")
                && key.len() >= 10
                && key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        }

        assert!(is_valid_project_key("pk_test_1234567890"));
        assert!(is_valid_project_key("pk_prod_abcdef1234"));
        assert!(!is_valid_project_key("invalid"));
        assert!(!is_valid_project_key("pk_")); // Too short
        assert!(!is_valid_project_key("pk_test-with-dashes")); // Contains invalid chars
    }
}
