//! NL Query Integration Tests (Phase 2)
//!
//! Tests for the text-to-SQL pipeline components integrated together:
//! - Catalog entries -> Schema context -> Prompt construction
//! - SQL validation with realistic catalog data
//! - Schema context threshold switching
//! - LlmClient against mock LLM gateway (WireMock)
//! - Full pipeline: catalog -> prompt -> mock LLM -> validate
//! - Error paths: rate limit, auth, timeout, empty content

use reiver_pond::warehouse::nl_query::prompt_builder::PromptBuilder;
use reiver_pond::warehouse::nl_query::validator::SqlValidator;
use reiver_pond::warehouse::nl_query::llm_client::LlmClient;
use reiver_pond::warehouse::catalog::types::{CatalogEntry, FreshnessInfo};
use reiver_pond::warehouse::types::{TypedColumn, TypedSchema};
use uuid::Uuid;
use serde_json::json;

fn make_catalog_entry(
    source: &str,
    table: &str,
    columns: &[(&str, arrow::datatypes::DataType)],
) -> CatalogEntry {
    let mut schema = TypedSchema::new(table, source);
    for (name, dt) in columns {
        let col = TypedColumn::new(*name, dt, true, format!("{:?}", dt), source);
        schema = schema.with_column(col);
    }
    CatalogEntry {
        id: Uuid::new_v4(),
        project_id: Uuid::new_v4(),
        source_id: None,
        source_name: source.to_string(),
        table_name: table.to_string(),
        schema,
        description: Some(format!("{} table", table)),
        tags: vec![],
        freshness: FreshnessInfo {
            row_count_estimate: Some(10_000),
            ..Default::default()
        },
        fulltext_columns: vec![],
        discovered_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    }
}

#[test]
fn test_validator_with_catalog_entries() {
    let entries = vec![
        make_catalog_entry(
            "db",
            "orders",
            &[
                ("id", arrow::datatypes::DataType::Int64),
                ("customer_id", arrow::datatypes::DataType::Int64),
                ("amount", arrow::datatypes::DataType::Float64),
                ("created_at", arrow::datatypes::DataType::Utf8),
            ],
        ),
        make_catalog_entry(
            "db",
            "customers",
            &[
                ("id", arrow::datatypes::DataType::Int64),
                ("name", arrow::datatypes::DataType::Utf8),
                ("email", arrow::datatypes::DataType::Utf8),
            ],
        ),
    ];

    let validator = SqlValidator::new();

    // Valid queries
    assert!(validator
        .validate_sql("SELECT count(*) FROM orders", &entries)
        .is_ok());
    assert!(validator
        .validate_sql(
            "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id",
            &entries,
        )
        .is_ok());

    // Invalid: unknown table
    assert!(validator
        .validate_sql("SELECT * FROM products", &entries)
        .is_err());

    // Invalid: DML
    assert!(validator
        .validate_sql("INSERT INTO orders VALUES (1, 1, 100, '2024-01-01')", &entries)
        .is_err());
}

#[test]
fn test_prompt_builder_full_pipeline() {
    let entries = vec![make_catalog_entry(
        "stripe",
        "charges",
        &[
            ("id", arrow::datatypes::DataType::Utf8),
            ("amount", arrow::datatypes::DataType::Int64),
            ("currency", arrow::datatypes::DataType::Utf8),
        ],
    )];

    // Step 1: Build schema context
    let schema_ctx = PromptBuilder::build_schema_context(&entries);
    assert_eq!(schema_ctx.table_count, 1);
    assert!(schema_ctx.formatted.contains("stripe.charges"));
    assert!(schema_ctx.formatted.contains("10.0K rows"));

    // Step 2: Build prompt messages
    let messages = PromptBuilder::build_prompt(&schema_ctx, "How much revenue?", None);
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].role, "system");
    assert!(messages[0].content.contains("stripe.charges"));
    assert_eq!(messages[1].role, "user");
    assert_eq!(messages[1].content, "How much revenue?");

    // Step 3: Build retry prompt
    let retry_messages = PromptBuilder::build_prompt(
        &schema_ctx,
        "How much revenue?",
        Some("Column 'revenue' not found"),
    );
    assert_eq!(retry_messages.len(), 4);
    assert!(retry_messages[3]
        .content
        .contains("Column 'revenue' not found"));
}

#[test]
fn test_schema_context_switches_at_threshold() {
    // 50 entries -> full schema with columns
    let entries_50: Vec<CatalogEntry> = (0..50)
        .map(|i| {
            make_catalog_entry(
                "db",
                &format!("table_{}", i),
                &[("id", arrow::datatypes::DataType::Int64)],
            )
        })
        .collect();
    let ctx_50 = PromptBuilder::build_schema_context(&entries_50);
    assert_eq!(ctx_50.table_count, 50);
    assert!(ctx_50.formatted.contains("Columns:"));

    // 51 entries -> compact schema without columns
    let entries_51: Vec<CatalogEntry> = (0..51)
        .map(|i| {
            make_catalog_entry(
                "db",
                &format!("table_{}", i),
                &[("id", arrow::datatypes::DataType::Int64)],
            )
        })
        .collect();
    let ctx_51 = PromptBuilder::build_schema_context(&entries_51);
    assert_eq!(ctx_51.table_count, 51);
    assert!(!ctx_51.formatted.contains("Columns:"));
    assert!(ctx_51.formatted.contains("51 tables"));
}

// ============================================================================
// LlmClient Integration Tests (WireMock)
// ============================================================================

mod llm_client_tests {
    use super::*;
    use wiremock::{MockServer, Mock, ResponseTemplate};
    use wiremock::matchers::{method, path};

    fn mock_completion_response(sql: &str) -> serde_json::Value {
        json!({
            "id": "chatcmpl-test",
            "object": "chat.completion",
            "created": 1700000000,
            "model": "gpt-4o",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": sql
                },
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
        })
    }

    fn simple_messages() -> Vec<reiver_pond::warehouse::nl_query::prompt_builder::ChatMessage> {
        vec![reiver_pond::warehouse::nl_query::prompt_builder::ChatMessage {
            role: "user".to_string(),
            content: "How many orders?".to_string(),
        }]
    }

    #[tokio::test]
    async fn test_generate_sql_success() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/gateway/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(mock_completion_response("SELECT count(*) FROM orders"))
            )
            .mount(&mock_server)
            .await;

        let http_client = reqwest::Client::new();
        let uri = mock_server.uri();
        let client = LlmClient::new(&http_client, &uri, "test-key");
        let (sql, model) = client.generate_sql("gpt-4o", simple_messages()).await.unwrap();

        assert_eq!(sql, "SELECT count(*) FROM orders");
        assert_eq!(model, "gpt-4o");
    }

    #[tokio::test]
    async fn test_generate_sql_strips_markdown_fences() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/gateway/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(mock_completion_response("```sql\nSELECT 1\n```"))
            )
            .mount(&mock_server)
            .await;

        let http_client = reqwest::Client::new();
        let uri = mock_server.uri();
        let client = LlmClient::new(&http_client, &uri, "test-key");
        let (sql, _) = client.generate_sql("gpt-4o", simple_messages()).await.unwrap();

        assert_eq!(sql, "SELECT 1");
    }

    #[tokio::test]
    async fn test_generate_sql_empty_content() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/gateway/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({
                        "id": "chatcmpl-test",
                        "object": "chat.completion",
                        "created": 1700000000,
                        "model": "gpt-4o",
                        "choices": [{
                            "index": 0,
                            "message": {
                                "role": "assistant",
                                "content": null
                            },
                            "finish_reason": "stop"
                        }]
                    }))
            )
            .mount(&mock_server)
            .await;

        let http_client = reqwest::Client::new();
        let uri = mock_server.uri();
        let client = LlmClient::new(&http_client, &uri, "test-key");
        let result = client.generate_sql("gpt-4o", simple_messages()).await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("No content"), "Error should mention no content: {}", err_msg);
    }

    #[tokio::test]
    async fn test_generate_sql_rate_limit_429() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/gateway/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(429)
                    .set_body_json(json!({"error": "rate limited"}))
            )
            .mount(&mock_server)
            .await;

        let http_client = reqwest::Client::new();
        let uri = mock_server.uri();
        let client = LlmClient::new(&http_client, &uri, "test-key");
        let result = client.generate_sql("gpt-4o", simple_messages()).await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("rate limit"), "Error should mention rate limit: {}", err_msg);
        // Should NOT contain raw error body
        assert!(!err_msg.contains("rate limited"), "Error should be sanitized, not raw body");
    }

    #[tokio::test]
    async fn test_generate_sql_server_error_500() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/gateway/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(500)
                    .set_body_json(json!({"error": "internal error with secret_key=abc123"}))
            )
            .mount(&mock_server)
            .await;

        let http_client = reqwest::Client::new();
        let uri = mock_server.uri();
        let client = LlmClient::new(&http_client, &uri, "test-key");
        let result = client.generate_sql("gpt-4o", simple_messages()).await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("temporarily unavailable"), "Error should be sanitized: {}", err_msg);
        // Sensitive info must NOT leak
        assert!(!err_msg.contains("secret_key"), "Raw error body should not leak");
    }

    #[tokio::test]
    async fn test_generate_sql_auth_error_401() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/gateway/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(401)
                    .set_body_json(json!({"error": "invalid api key sk-1234"}))
            )
            .mount(&mock_server)
            .await;

        let http_client = reqwest::Client::new();
        let uri = mock_server.uri();
        let client = LlmClient::new(&http_client, &uri, "test-key");
        let result = client.generate_sql("gpt-4o", simple_messages()).await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("authentication failed"), "Error should mention auth failure: {}", err_msg);
        assert!(!err_msg.contains("sk-1234"), "API key must not leak in error");
    }

    #[tokio::test]
    async fn test_generate_sql_connection_failure() {
        // Point to a non-routable address with a short connect timeout.
        // LlmClient sets its own per-request timeout (60s), so we test
        // connection-level failures rather than read timeouts.
        let http_client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_millis(100))
            .build()
            .unwrap();

        // Use a port that's almost certainly not listening
        let uri = "http://127.0.0.1:1".to_string();
        let client = LlmClient::new(&http_client, &uri, "test-key");
        let result = client.generate_sql("gpt-4o", simple_messages()).await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Failed to call Flow gateway"),
            "Error should mention gateway failure: {}", err_msg
        );
    }

    #[tokio::test]
    async fn test_full_pipeline_catalog_to_validation() {
        let mock_server = MockServer::start().await;

        // Mock LLM returns valid SQL referencing known table
        Mock::given(method("POST"))
            .and(path("/api/gateway/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(mock_completion_response("SELECT count(*) FROM orders"))
            )
            .mount(&mock_server)
            .await;

        // Step 1: Build catalog entries
        let entries = vec![make_catalog_entry(
            "db",
            "orders",
            &[
                ("id", arrow::datatypes::DataType::Int64),
                ("amount", arrow::datatypes::DataType::Float64),
            ],
        )];

        // Step 2: Build schema context and prompt
        let schema_ctx = PromptBuilder::build_schema_context(&entries);
        let messages = PromptBuilder::build_prompt(&schema_ctx, "How many orders?", None);

        // Step 3: Call mock LLM
        let http_client = reqwest::Client::new();
        let uri = mock_server.uri();
        let client = LlmClient::new(&http_client, &uri, "test-key");
        let (sql, _model) = client.generate_sql("gpt-4o", messages).await.unwrap();

        // Step 4: Validate generated SQL
        let validator = SqlValidator::new();
        let validated = validator.validate_sql(&sql, &entries);
        assert!(validated.is_ok(), "Generated SQL should validate: {:?}", validated);

        let validated = validated.unwrap();
        assert!(validated.referenced_tables.contains(&"orders".to_string()));
    }

    #[tokio::test]
    async fn test_self_correction_retry_pipeline() {
        let mock_server = MockServer::start().await;

        // First call: LLM returns SQL with unknown table
        // Second call: LLM returns corrected SQL
        Mock::given(method("POST"))
            .and(path("/api/gateway/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(mock_completion_response("SELECT count(*) FROM orders"))
            )
            .mount(&mock_server)
            .await;

        let entries = vec![make_catalog_entry(
            "db",
            "orders",
            &[("id", arrow::datatypes::DataType::Int64)],
        )];

        let validator = SqlValidator::new();
        let schema_ctx = PromptBuilder::build_schema_context(&entries);

        // Simulate first attempt returning SQL with unknown table
        let bad_sql = "SELECT count(*) FROM nonexistent_table";
        let validation_result = validator.validate_sql(bad_sql, &entries);
        assert!(validation_result.is_err());

        let error_message = validation_result.unwrap_err().to_string();

        // Build retry prompt with error context
        let retry_messages = PromptBuilder::build_prompt(
            &schema_ctx,
            "How many orders?",
            Some(&format!("SQL validation failed.\nGenerated SQL: {}\nError: {}", bad_sql, error_message)),
        );

        // Verify retry prompt includes the error
        assert_eq!(retry_messages.len(), 4);
        assert!(retry_messages[3].content.contains("nonexistent_table"));
        assert!(retry_messages[3].content.contains("unknown table"));

        // Second attempt: call mock LLM which returns valid SQL
        let http_client = reqwest::Client::new();
        let uri = mock_server.uri();
        let client = LlmClient::new(&http_client, &uri, "test-key");
        let (sql, _) = client.generate_sql("gpt-4o", retry_messages).await.unwrap();

        // Validate the corrected SQL
        let corrected = validator.validate_sql(&sql, &entries);
        assert!(corrected.is_ok(), "Corrected SQL should validate: {:?}", corrected);
    }
}
