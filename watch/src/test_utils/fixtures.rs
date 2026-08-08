//! Test fixtures and data factories
//!
//! Provides factory functions for creating test data with sensible defaults.

use chrono::{DateTime, Utc};
use serde_json::json;
use uuid::Uuid;

/// Create a test user with default values
pub fn create_test_user() -> TestUser {
    TestUser {
        id: Uuid::new_v4(),
        email: format!(
            "user-{}@example.com",
            Uuid::new_v4().to_string()[..8].to_string()
        ),
        password_hash: "$2b$12$test_hash_value_here".to_string(),
        created_at: Utc::now(),
    }
}

/// Create a test user with specific email
pub fn create_test_user_with_email(email: &str) -> TestUser {
    TestUser {
        id: Uuid::new_v4(),
        email: email.to_string(),
        password_hash: "$2b$12$test_hash_value_here".to_string(),
        created_at: Utc::now(),
    }
}

/// Create a test organization
pub fn create_test_organization() -> TestOrganization {
    TestOrganization {
        id: Uuid::new_v4(),
        name: format!("Test Org {}", &Uuid::new_v4().to_string()[..8]),
        created_at: Utc::now(),
    }
}

/// Create a test project
pub fn create_test_project(organization_id: Uuid) -> TestProject {
    TestProject {
        id: Uuid::new_v4(),
        organization_id,
        name: format!("Test Project {}", &Uuid::new_v4().to_string()[..8]),
        project_key: format!("pk_{}", &Uuid::new_v4().to_string().replace("-", "")[..16]),
        created_by: None,
        created_at: Utc::now(),
    }
}

/// Create a test exception payload
pub fn create_test_exception_payload(project_key: &str) -> serde_json::Value {
    json!({
        "project_key": project_key,
        "level": "error",
        "message": "Test error message",
        "timestamp": Utc::now().to_rfc3339(),
        "environment": "test",
        "release": "1.0.0",
        "stack_trace": [{
            "filename": "src/main.rs",
            "lineno": 42,
            "function": "main",
            "context_line": "panic!(\"test error\")"
        }]
    })
}

/// Create a test span payload
pub fn create_test_span_payload(project_id: &str) -> serde_json::Value {
    let trace_id = hex::encode(rand::random::<[u8; 16]>());
    let span_id = hex::encode(rand::random::<[u8; 8]>());

    json!({
        "project_id": project_id,
        "trace_id": trace_id,
        "span_id": span_id,
        "parent_span_id": null,
        "name": "test-span",
        "kind": "INTERNAL",
        "start_time_unix_nano": Utc::now().timestamp_nanos_opt().unwrap_or(0),
        "end_time_unix_nano": Utc::now().timestamp_nanos_opt().unwrap_or(0) + 1000000,
        "attributes": {}
    })
}

/// Create a test LLM span with gen_ai attributes
pub fn create_test_llm_span_payload(project_id: &str, model: &str) -> serde_json::Value {
    let trace_id = hex::encode(rand::random::<[u8; 16]>());
    let span_id = hex::encode(rand::random::<[u8; 8]>());

    json!({
        "project_id": project_id,
        "trace_id": trace_id,
        "span_id": span_id,
        "parent_span_id": null,
        "name": "chat",
        "kind": "CLIENT",
        "start_time_unix_nano": Utc::now().timestamp_nanos_opt().unwrap_or(0),
        "end_time_unix_nano": Utc::now().timestamp_nanos_opt().unwrap_or(0) + 1000000000,
        "attributes": {
            "gen_ai.system": "openai",
            "gen_ai.request.model": model,
            "gen_ai.response.model": model,
            "gen_ai.usage.input_tokens": 100,
            "gen_ai.usage.output_tokens": 50
        }
    })
}

/// Create a test SSO configuration payload
pub fn create_test_sso_config_payload(organization_id: Uuid, provider: &str) -> serde_json::Value {
    json!({
        "organization_id": organization_id,
        "provider_type": provider,
        "provider_name": format!("Test {} SSO", provider),
        "is_enabled": true,
        "client_id": "test_client_id",
        "client_secret": "test_client_secret",
        "issuer_url": format!("https://{}.example.com", provider),
        "enforce_sso": false,
        "allow_idp_initiated": true
    })
}

/// Create test user claims for SSO provisioning
pub fn create_test_user_claims(email: &str, groups: Vec<&str>) -> TestUserClaims {
    TestUserClaims {
        email: email.to_string(),
        name: Some("Test User".to_string()),
        groups: groups.into_iter().map(String::from).collect(),
        attributes: std::collections::HashMap::new(),
    }
}

/// Create a test alert rule
pub fn create_test_alert_rule(project_id: Uuid, threshold: f64) -> serde_json::Value {
    json!({
        "project_id": project_id,
        "name": "Test Alert Rule",
        "description": "Test alert for error rate",
        "enabled": true,
        "condition": {
            "metric": "error_rate",
            "operator": "above",
            "threshold": threshold,
            "duration_seconds": 300
        },
        "channels": []
    })
}

// ============================================================================
// Test Data Types
// ============================================================================

#[derive(Debug, Clone)]
pub struct TestUser {
    pub id: Uuid,
    pub email: String,
    pub password_hash: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct TestOrganization {
    pub id: Uuid,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct TestProject {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub name: String,
    pub project_key: String,
    pub created_by: Option<Uuid>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct TestUserClaims {
    pub email: String,
    pub name: Option<String>,
    pub groups: Vec<String>,
    pub attributes: std::collections::HashMap<String, String>,
}

// ============================================================================
// LLM Test Fixtures
// ============================================================================

/// Create test pricing data
pub fn create_test_pricing() -> Vec<TestPricing> {
    vec![
        TestPricing {
            provider: "openai".to_string(),
            model: "gpt-4o".to_string(),
            input_cost_per_million: 5.0,
            output_cost_per_million: 15.0,
        },
        TestPricing {
            provider: "openai".to_string(),
            model: "gpt-4o-mini".to_string(),
            input_cost_per_million: 0.15,
            output_cost_per_million: 0.6,
        },
        TestPricing {
            provider: "anthropic".to_string(),
            model: "claude-3-5-sonnet".to_string(),
            input_cost_per_million: 3.0,
            output_cost_per_million: 15.0,
        },
    ]
}

#[derive(Debug, Clone)]
pub struct TestPricing {
    pub provider: String,
    pub model: String,
    pub input_cost_per_million: f64,
    pub output_cost_per_million: f64,
}

// ============================================================================
// Random Data Generators
// ============================================================================

/// Generate a random trace ID (32 hex chars)
pub fn random_trace_id() -> String {
    hex::encode(rand::random::<[u8; 16]>())
}

/// Generate a random span ID (16 hex chars)
pub fn random_span_id() -> String {
    hex::encode(rand::random::<[u8; 8]>())
}

/// Generate a random project key
pub fn random_project_key() -> String {
    format!("pk_{}", &Uuid::new_v4().to_string().replace("-", "")[..16])
}

/// Generate a random email
pub fn random_email() -> String {
    format!("user-{}@example.com", &Uuid::new_v4().to_string()[..8])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_test_user() {
        let user = create_test_user();
        assert!(!user.email.is_empty());
        assert!(user.email.contains('@'));
    }

    #[test]
    fn test_create_test_project() {
        let org = create_test_organization();
        let project = create_test_project(org.id);
        assert_eq!(project.organization_id, org.id);
        assert!(project.project_key.starts_with("pk_"));
    }

    #[test]
    fn test_random_ids() {
        let trace_id = random_trace_id();
        let span_id = random_span_id();

        assert_eq!(trace_id.len(), 32);
        assert_eq!(span_id.len(), 16);
    }
}
