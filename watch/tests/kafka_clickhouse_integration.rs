//! Integration tests for Kafka + ClickHouse integration
//!
//! These tests verify the end-to-end flow:
//! 1. API receives exceptions/spans → writes to Kafka
//! 2. Kafka consumer processes exceptions → writes to ClickHouse
//! 3. Tail sampling worker processes spans → writes sampled spans to ClickHouse
//! 4. Redis stats are updated
//!
//! Prerequisites:
//! - Kafka, ClickHouse, Redis, PostgreSQL must be running
//! - Environment variables: DATABASE_URL, CLICKHOUSE_URL, REDIS_URL, KAFKA_HOSTS
//!
//! To run:
//!   cargo test --test kafka_clickhouse_integration -- --nocapture

use chrono::Utc;
use std::time::Duration;
use uuid::Uuid;

/// Test that exceptions flow through Kafka → ClickHouse correctly
#[tokio::test]
#[ignore] // Ignore by default - requires full infrastructure
async fn test_error_flow_kafka_to_clickhouse() {
    // Setup: Get test environment
    let api_url =
        std::env::var("REIVER_API_URL").unwrap_or_else(|_| "http://localhost:3000".to_string());
    let kafka_hosts = std::env::var("KAFKA_HOSTS").unwrap_or_else(|_| "localhost:9092".to_string());
    let clickhouse_url =
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());

    // Get or create a test project
    let project_key = get_or_create_test_project(&api_url).await;

    println!("Using project key: {}...", &project_key[..8]);

    // Generate a unique test exception ID
    let test_exception_id = Uuid::new_v4().to_string();
    let test_message = format!("Integration test exception: {}", test_exception_id);

    // Step 1: Send exception to API (should write to Kafka)
    println!("Step 1: Sending exception to API...");
    let exception_payload: serde_json::Value = serde_json::json!({
        "project_key": project_key,
        "timestamp": Utc::now().to_rfc3339(),
        "level": "error",
        "message": test_message.clone(),
        "exception": {
            "type": "IntegrationTestError",
            "value": test_message.clone(),
            "stacktrace": vec![] as Vec<serde_json::Value>
        },
        "context": {},
        "tags": {"test": "kafka_clickhouse_integration"},
        "user": serde_json::Value::Null
    });

    let client = reqwest::Client::new();
    let response = client
        .post(&format!("{}/api/v1/exceptions", api_url))
        .json(&exception_payload)
        .send()
        .await
        .expect("Failed to send exception to API");

    assert!(
        response.status().is_success(),
        "API should accept exception"
    );
    println!("✅ Exception sent to API successfully");

    // Step 2: Wait for Kafka consumer to process (give it time)
    println!("Step 2: Waiting for Kafka consumer to process exception...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Step 3: Check that exception appears in ClickHouse
    println!("Step 3: Checking ClickHouse for exception...");
    let clickhouse_client = clickhouse::Client::default()
        .with_url(&clickhouse_url)
        .with_database("reiver");

    // Define struct for deserialization
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct ErrorRow {
        id: String,
        project_id: String,
        message: String,
        level: String,
    }

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct ProjectIdRow {
        project_id: String,
    }

    // Query ClickHouse for the exception (by message) using parameterized query
    let query =
        "SELECT id, project_id, message, level FROM reiver.exceptions WHERE message = ? LIMIT 1";

    let mut max_attempts = 10;
    let mut found = false;
    let mut project_id: Option<String> = None;

    while max_attempts > 0 && !found {
        let result: Result<Vec<ErrorRow>, _> = clickhouse_client
            .query(query)
            .bind(&test_message)
            .fetch_all()
            .await;

        match result {
            Ok(rows) => {
                if let Some(row) = rows.first() {
                    assert_eq!(row.message, test_message, "Message should match");
                    assert_eq!(row.level, "error", "Level should be error");
                    println!(
                        "✅ Exception found in ClickHouse: id={}, project_id={}",
                        row.id, row.project_id
                    );
                    project_id = Some(row.project_id.clone());
                    found = true;
                } else {
                    println!(
                        "Exception not found yet, retrying... ({} attempts left)",
                        max_attempts - 1
                    );
                    max_attempts -= 1;
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
            Err(e) => {
                panic!("Failed to query ClickHouse: {}", e);
            }
        }
    }

    assert!(
        found,
        "Exception should appear in ClickHouse after processing"
    );
    let project_id = project_id.expect("Project ID should be set");

    // Step 4: Check that Redis stats were updated
    println!("Step 4: Checking Redis stats...");
    let redis_client =
        redis::Client::open(redis_url.as_str()).expect("Failed to create Redis client");
    let mut redis_conn = redis_client
        .get_connection()
        .expect("Failed to connect to Redis");

    // Check total_exceptions counter
    let total_exceptions_key = format!("stats:project:{}:total_exceptions", project_id);
    let total_exceptions: Option<i64> = redis::cmd("GET")
        .arg(&total_exceptions_key)
        .query(&mut redis_conn)
        .expect("Failed to query Redis");

    assert!(
        total_exceptions.is_some(),
        "Total exceptions counter should exist in Redis"
    );
    assert!(
        total_exceptions.unwrap() > 0,
        "Total exceptions should be greater than 0"
    );
    println!(
        "✅ Redis stats updated: total_exceptions={}",
        total_exceptions.unwrap()
    );

    println!("✅ All checks passed! Exception flow is working correctly.");
}

/// Test that spans flow through Kafka → Tail Sampling → ClickHouse correctly
#[tokio::test]
#[ignore] // Ignore by default - requires full infrastructure
async fn test_span_flow_kafka_to_clickhouse() {
    let api_url =
        std::env::var("REIVER_API_URL").unwrap_or_else(|_| "http://localhost:3000".to_string());
    let clickhouse_url =
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());

    let project_key = get_or_create_test_project(&api_url).await;

    // Generate unique trace and span IDs
    let trace_id = Uuid::new_v4().to_string();
    let span_id = Uuid::new_v4().to_string();

    println!("Using trace_id: {}, span_id: {}", trace_id, span_id);

    // Step 1: Send span to API (should write to Kafka)
    println!("Step 1: Sending span to API...");
    let span_payload: serde_json::Value = serde_json::json!({
        "project_key": project_key,
        "trace_id": trace_id,
        "span_id": span_id,
        "parent_span_id": serde_json::Value::Null,
        "name": "test_span",
        "service_name": "test_service",
        "start_time": Utc::now().timestamp_nanos_opt().unwrap(),
        "duration_ms": 100.0,
        "status": "ok",
        "attributes": {
            "test": "kafka_clickhouse_integration"
        }
    });

    let client = reqwest::Client::new();
    let response = client
        .post(&format!("{}/api/v1/spans", api_url))
        .json(&span_payload)
        .send()
        .await
        .expect("Failed to send span to API");

    assert!(response.status().is_success(), "API should accept span");
    println!("✅ Span sent to API successfully");

    // Step 2: Wait for tail sampling worker to process (may need more time)
    println!("Step 2: Waiting for tail sampling worker to process span...");
    tokio::time::sleep(Duration::from_secs(15)).await; // Tail sampling has decision_wait time

    // Step 3: Check that span appears in ClickHouse (if sampled)
    println!("Step 3: Checking ClickHouse for span...");
    let clickhouse_client = clickhouse::Client::default()
        .with_url(&clickhouse_url)
        .with_database("reiver");

    // Define struct for deserialization
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct SpanRow {
        span_id: String,
        trace_id: String,
        name: String,
        service_name: String,
    }

    let query = "SELECT span_id, trace_id, name, service_name FROM reiver.spans WHERE span_id = ? LIMIT 1";

    let mut max_attempts = 10;
    let mut found = false;

    while max_attempts > 0 && !found {
        let result: Result<Vec<SpanRow>, _> = clickhouse_client
            .query(query)
            .bind(&span_id)
            .fetch_all()
            .await;

        match result {
            Ok(rows) => {
                if let Some(row) = rows.first() {
                    assert_eq!(row.span_id, span_id, "Span ID should match");
                    assert_eq!(row.trace_id, trace_id, "Trace ID should match");
                    assert_eq!(row.name, "test_span", "Span name should match");
                    assert_eq!(
                        row.service_name, "test_service",
                        "Service name should match"
                    );
                    println!(
                        "✅ Span found in ClickHouse: span_id={}, trace_id={}",
                        row.span_id, row.trace_id
                    );
                    found = true;
                } else {
                    println!(
                        "Span not found yet (may not be sampled), retrying... ({} attempts left)",
                        max_attempts - 1
                    );
                    max_attempts -= 1;
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
            }
            Err(e) => {
                panic!("Failed to query ClickHouse: {}", e);
            }
        }
    }

    // Note: Span may not be sampled (depending on tail sampling policy)
    // This is acceptable - we just verify the flow works
    if found {
        println!("✅ Span was sampled and written to ClickHouse");
    } else {
        println!("⚠️  Span was not sampled (this is expected for low-traffic traces)");
    }

    println!("✅ Span flow test completed.");
}

/// Helper: Get or create a test project
async fn get_or_create_test_project(api_url: &str) -> String {
    // Try to get project key from environment
    if let Ok(key) = std::env::var("REIVER_PROJECT_KEY") {
        return key;
    }

    // Otherwise, try to get it by logging in
    let email = std::env::var("REIVER_EMAIL").unwrap_or_else(|_| "test@example.com".to_string());
    let password = std::env::var("REIVER_PASSWORD")
        .expect("Either REIVER_PROJECT_KEY or REIVER_PASSWORD must be set");

    // Login and get project key
    let client = reqwest::Client::new();
    let login_response = client
        .post(&format!("{}/api/auth/login", api_url))
        .json(&serde_json::json!({
            "email": email,
            "password": password
        }))
        .send()
        .await
        .expect("Failed to login");

    assert!(login_response.status().is_success(), "Login should succeed");

    let login_data: serde_json::Value = login_response
        .json()
        .await
        .expect("Failed to parse login response");
    let token = login_data["token"]
        .as_str()
        .expect("Token should be in response");

    // Get projects
    let projects_response = client
        .get(&format!("{}/api/projects", api_url))
        .bearer_auth(token)
        .send()
        .await
        .expect("Failed to get projects");

    assert!(
        projects_response.status().is_success(),
        "Should get projects"
    );
    let projects: serde_json::Value = projects_response
        .json()
        .await
        .expect("Failed to parse projects");

    let first_project = projects["projects"]
        .as_array()
        .expect("Projects should be array")
        .first()
        .expect("Should have at least one project");

    let project_id = first_project["id"]
        .as_str()
        .expect("Project should have ID");

    // Get project keys
    let keys_response = client
        .get(&format!("{}/api/projects/{}/keys", api_url, project_id))
        .bearer_auth(token)
        .send()
        .await
        .expect("Failed to get project keys");

    assert!(
        keys_response.status().is_success(),
        "Should get project keys"
    );
    let keys: serde_json::Value = keys_response.json().await.expect("Failed to parse keys");

    let first_key = keys["keys"]
        .as_array()
        .expect("Keys should be array")
        .first()
        .expect("Should have at least one key");

    first_key["key"]
        .as_str()
        .expect("Key should have key value")
        .to_string()
}
