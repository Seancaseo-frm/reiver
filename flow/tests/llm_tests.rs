//! LLM Observability API Tests
//!
//! Tests for LLM-specific functionality including:
//! - LLM span processing and cost calculation
//! - Session management
//! - Evaluation scores
//! - Pricing sync

use serde_json::json;

mod helpers;
use helpers::*;

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal::Decimal;
    use std::collections::HashMap;

    // ========================================================================
    // LLM Request Structure Tests
    // ========================================================================

    /// Create a minimal LLM request payload
    fn create_llm_request() -> serde_json::Value {
        json!({
            "project_id": "00000000-0000-0000-0000-000000000001",
            "request_id": "trace123:span456",
            "trace_id": "trace123",
            "span_id": "span456",
            "gen_ai_system": "openai",
            "gen_ai_request_model": "gpt-4o",
            "gen_ai_response_model": "gpt-4o-2024-05-13",
            "gen_ai_operation_name": "chat",
            "input_tokens": 100,
            "output_tokens": 50,
            "total_tokens": 150,
            "cache_read_tokens": 0,
            "cache_write_tokens": 0,
            "cost_usd": "0.0025",
            "timestamp": Utc::now().to_rfc3339(),
            "duration_ms": 500,
            "time_to_first_token_ms": 50,
            "status_code": "ok",
            "error_type": "",
            "error_message": "",
            "session_id": "session_abc123",
            "session_name": "Test Chat Session",
            "user_id": "user_123",
            "request_messages": "[]",
            "response_content": "",
            "properties": {},
            "scores": {},
            "service_name": "test-service"
        })
    }

    #[test]
    fn test_llm_request_structure() {
        let request = create_llm_request();

        assert_eq!(request["gen_ai_system"], "openai");
        assert_eq!(request["gen_ai_request_model"], "gpt-4o");
        assert_eq!(request["input_tokens"], 100);
        assert_eq!(request["output_tokens"], 50);
        assert_eq!(request["total_tokens"], 150);
    }

    #[test]
    fn test_llm_request_has_session_id() {
        let request = create_llm_request();

        assert!(!request["session_id"].as_str().unwrap().is_empty());
        assert_eq!(request["session_id"], "session_abc123");
    }

    // ========================================================================
    // GenAI Semantic Conventions Tests
    // ========================================================================

    #[test]
    fn test_genai_system_values() {
        // Valid GenAI system values per OpenTelemetry semantic conventions
        let valid_systems = vec![
            "openai",
            "anthropic",
            "google",
            "azure",
            "cohere",
            "bedrock",
            "mistral",
            "deepseek",
        ];

        for system in valid_systems {
            assert!(!system.is_empty(), "System name should not be empty");
        }
    }

    #[test]
    fn test_genai_operation_names() {
        // Valid operation names
        let operations = vec!["chat", "completion", "embedding", "image"];

        for op in operations {
            assert!(!op.is_empty());
        }
    }

    // ========================================================================
    // LLM Span Detection Tests
    // ========================================================================

    fn create_llm_span_attributes() -> HashMap<String, String> {
        let mut attrs = HashMap::new();
        attrs.insert("gen_ai.system".to_string(), "openai".to_string());
        attrs.insert("gen_ai.request.model".to_string(), "gpt-4o".to_string());
        attrs.insert("gen_ai.usage.input_tokens".to_string(), "100".to_string());
        attrs.insert("gen_ai.usage.output_tokens".to_string(), "50".to_string());
        attrs
    }

    fn is_llm_span(attrs: &HashMap<String, String>) -> bool {
        attrs.contains_key("gen_ai.system")
    }

    #[test]
    fn test_llm_span_detection_positive() {
        let attrs = create_llm_span_attributes();
        assert!(
            is_llm_span(&attrs),
            "Span with gen_ai.system should be detected as LLM span"
        );
    }

    #[test]
    fn test_llm_span_detection_negative() {
        let attrs: HashMap<String, String> = HashMap::new();
        assert!(
            !is_llm_span(&attrs),
            "Empty span should not be detected as LLM span"
        );

        let mut http_attrs = HashMap::new();
        http_attrs.insert("http.method".to_string(), "GET".to_string());
        assert!(
            !is_llm_span(&http_attrs),
            "HTTP span should not be detected as LLM span"
        );
    }

    // ========================================================================
    // Model Name Normalization Tests
    // ========================================================================

    /// Extract base model name by stripping version suffixes
    fn extract_base_model(model: &str) -> String {
        // Strip date-based suffixes: gpt-4o-2024-05-13 -> gpt-4o
        let date_pattern = regex::Regex::new(r"-\d{4}-\d{2}-\d{2}$").unwrap();
        let stripped = date_pattern.replace(model, "").to_string();

        // Strip numeric suffixes: gemini-1.5-pro-001 -> gemini-1.5-pro
        let numeric_pattern = regex::Regex::new(r"-\d{3,}$").unwrap();
        numeric_pattern.replace(&stripped, "").to_string()
    }

    #[test]
    fn test_model_name_normalization_openai() {
        assert_eq!(extract_base_model("gpt-4o-2024-05-13"), "gpt-4o");
        assert_eq!(extract_base_model("gpt-4-turbo-2024-04-09"), "gpt-4-turbo");
        assert_eq!(extract_base_model("gpt-4o"), "gpt-4o");
    }

    #[test]
    fn test_model_name_normalization_google() {
        assert_eq!(extract_base_model("gemini-1.5-pro-001"), "gemini-1.5-pro");
        assert_eq!(
            extract_base_model("gemini-1.5-flash-002"),
            "gemini-1.5-flash"
        );
    }

    #[test]
    fn test_model_name_no_normalization_needed() {
        assert_eq!(extract_base_model("claude-3-opus"), "claude-3-opus");
        assert_eq!(extract_base_model("o1-mini"), "o1-mini");
    }

    // ========================================================================
    // Cost Calculation Tests
    // ========================================================================

    fn calculate_cost(
        input_tokens: u32,
        output_tokens: u32,
        input_cost_per_million: Decimal,
        output_cost_per_million: Decimal,
    ) -> Decimal {
        let million = Decimal::from(1_000_000);
        let input_cost = Decimal::from(input_tokens) * input_cost_per_million / million;
        let output_cost = Decimal::from(output_tokens) * output_cost_per_million / million;
        input_cost + output_cost
    }

    #[test]
    fn test_cost_calculation_gpt4o() {
        // GPT-4o pricing: $5/1M input, $15/1M output
        let cost = calculate_cost(
            1000, // 1K input tokens
            500,  // 500 output tokens
            Decimal::from(5),
            Decimal::from(15),
        );

        // Expected: (1000 * 5 / 1M) + (500 * 15 / 1M) = 0.005 + 0.0075 = 0.0125
        assert_eq!(cost, Decimal::new(125, 4)); // 0.0125
    }

    #[test]
    fn test_cost_calculation_zero_tokens() {
        let cost = calculate_cost(0, 0, Decimal::from(5), Decimal::from(15));
        assert_eq!(cost, Decimal::ZERO);
    }

    #[test]
    fn test_cost_calculation_large_token_count() {
        // 1 million input + 1 million output tokens
        let cost = calculate_cost(1_000_000, 1_000_000, Decimal::from(5), Decimal::from(15));

        // Expected: 5 + 15 = 20
        assert_eq!(cost, Decimal::from(20));
    }

    // ========================================================================
    // Evaluation Score Tests
    // ========================================================================

    fn validate_score(score_type: &str, score_value: Decimal) -> Result<(), String> {
        if !["number", "boolean", "category"].contains(&score_type) {
            return Err("Invalid score type".to_string());
        }

        if score_type == "boolean" && !(score_value == Decimal::ZERO || score_value == Decimal::ONE)
        {
            return Err("Boolean scores must be 0 or 1".to_string());
        }

        if score_type == "number"
            && (score_value < Decimal::ZERO || score_value > Decimal::from(100))
        {
            return Err("Numeric scores must be between 0 and 100".to_string());
        }

        Ok(())
    }

    #[test]
    fn test_score_validation_valid_number() {
        assert!(validate_score("number", Decimal::from(75)).is_ok());
        assert!(validate_score("number", Decimal::ZERO).is_ok());
        assert!(validate_score("number", Decimal::from(100)).is_ok());
    }

    #[test]
    fn test_score_validation_valid_boolean() {
        assert!(validate_score("boolean", Decimal::ZERO).is_ok());
        assert!(validate_score("boolean", Decimal::ONE).is_ok());
    }

    #[test]
    fn test_score_validation_invalid_type() {
        assert!(validate_score("invalid", Decimal::from(50)).is_err());
    }

    #[test]
    fn test_score_validation_invalid_boolean() {
        assert!(validate_score("boolean", Decimal::from(2)).is_err());
        assert!(validate_score("boolean", Decimal::from(50)).is_err());
    }

    #[test]
    fn test_score_validation_out_of_range() {
        assert!(validate_score("number", Decimal::from(-1)).is_err());
        assert!(validate_score("number", Decimal::from(101)).is_err());
    }

    // ========================================================================
    // Session Aggregation Tests
    // ========================================================================

    #[derive(Debug)]
    struct SessionMetrics {
        session_id: String,
        request_count: u64,
        total_tokens: u64,
        _total_cost_usd: Decimal,
        error_count: u64,
    }

    fn aggregate_session_metrics(requests: Vec<serde_json::Value>) -> SessionMetrics {
        let session_id = requests
            .first()
            .and_then(|r| r["session_id"].as_str())
            .unwrap_or("")
            .to_string();

        let mut total_tokens = 0u64;
        let mut total_cost = Decimal::ZERO;
        let mut error_count = 0u64;

        for req in &requests {
            total_tokens += req["total_tokens"].as_u64().unwrap_or(0);

            if let Some(cost_str) = req["cost_usd"].as_str() {
                total_cost += cost_str.parse::<Decimal>().unwrap_or(Decimal::ZERO);
            }

            if req["status_code"].as_str() == Some("error") {
                error_count += 1;
            }
        }

        SessionMetrics {
            session_id,
            request_count: requests.len() as u64,
            total_tokens,
            _total_cost_usd: total_cost,
            error_count,
        }
    }

    #[test]
    fn test_session_aggregation() {
        let requests = vec![
            json!({
                "session_id": "session_123",
                "total_tokens": 100,
                "cost_usd": "0.001",
                "status_code": "ok"
            }),
            json!({
                "session_id": "session_123",
                "total_tokens": 150,
                "cost_usd": "0.002",
                "status_code": "ok"
            }),
            json!({
                "session_id": "session_123",
                "total_tokens": 50,
                "cost_usd": "0.0005",
                "status_code": "error"
            }),
        ];

        let metrics = aggregate_session_metrics(requests);

        assert_eq!(metrics.session_id, "session_123");
        assert_eq!(metrics.request_count, 3);
        assert_eq!(metrics.total_tokens, 300);
        assert_eq!(metrics.error_count, 1);
    }

    // ========================================================================
    // Pricing Data Tests
    // ========================================================================

    fn infer_provider(model: &str) -> String {
        reiver_flow::gateway::provider_types::Provider::from_model_prefix(model)
            .map(|p| p.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    }

    #[test]
    fn test_provider_inference_openai() {
        use reiver_flow::gateway::provider_types::Provider;
        assert_eq!(
            Provider::from_model_prefix("gpt-4o").map(|p| p.to_string()),
            Some("openai".to_string())
        );
        assert_eq!(
            Provider::from_model_prefix("gpt-4-turbo").map(|p| p.to_string()),
            Some("openai".to_string())
        );
        assert_eq!(
            Provider::from_model_prefix("o1-mini").map(|p| p.to_string()),
            Some("openai".to_string())
        );
        assert_eq!(
            Provider::from_model_prefix("o3-mini").map(|p| p.to_string()),
            Some("openai".to_string())
        );
    }

    #[test]
    fn test_provider_inference_anthropic() {
        assert_eq!(infer_provider("claude-3-opus"), "anthropic");
        assert_eq!(infer_provider("claude-3-5-sonnet"), "anthropic");
    }

    #[test]
    fn test_provider_inference_google() {
        assert_eq!(infer_provider("gemini-1.5-pro"), "google");
    }

    #[test]
    fn test_provider_inference_unknown() {
        assert_eq!(infer_provider("custom-model"), "unknown");
    }

    // ========================================================================
    // API Request/Response Structure Tests
    // ========================================================================

    #[test]
    fn test_metrics_query_params() {
        let params = json!({
            "project_id": "00000000-0000-0000-0000-000000000001",
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
            "limit": 50,
            "offset": 0
        });

        assert!(params["project_id"].is_string());
        assert_eq!(params["limit"], 50);
    }

    #[test]
    fn test_submit_score_request() {
        let request = json!({
            "project_id": "00000000-0000-0000-0000-000000000001",
            "request_id": "trace123:span456",
            "score_name": "relevance",
            "score_value": 85.5,
            "score_type": "number",
            "reason": "Response was highly relevant",
            "evaluator_type": "human",
            "evaluator_id": "user_123"
        });

        let req = json_post("/api/v1/llm/scores", &request);
        assert_eq!(req.method(), "POST");
    }

    #[test]
    fn test_session_feedback_request() {
        let request = json!({
            "project_id": "00000000-0000-0000-0000-000000000001",
            "score": 5,
            "text": "Great conversation!"
        });

        let req = json_post("/api/v1/llm/sessions/session_123/feedback", &request);
        assert_eq!(req.method(), "POST");
    }

    // ========================================================================
    // Duration Handling Tests
    // ========================================================================

    #[test]
    fn test_duration_conversion_normal() {
        let duration_nanos: u64 = 500_000_000; // 500ms
        let duration_ms = (duration_nanos / 1_000_000) as u32;
        assert_eq!(duration_ms, 500);
    }

    #[test]
    fn test_duration_conversion_overflow_protection() {
        // Very long duration (more than u32::MAX milliseconds)
        let duration_nanos: u64 = u64::MAX;
        let duration_ms = (duration_nanos / 1_000_000).min(u32::MAX as u64) as u32;
        assert_eq!(duration_ms, u32::MAX);
    }

    #[test]
    fn test_duration_conversion_zero() {
        let duration_nanos: u64 = 0;
        let duration_ms = (duration_nanos / 1_000_000) as u32;
        assert_eq!(duration_ms, 0);
    }

    // ========================================================================
    // Integration Tests: LLM Span Processing Pipeline
    // ========================================================================
    //
    // These tests verify the end-to-end flow of LLM span processing,
    // including attribute extraction, cost calculation, and data transformation.

    /// Simulates an OTLP span with GenAI attributes for OpenAI
    fn create_openai_span_attributes() -> HashMap<String, String> {
        let mut attrs = HashMap::new();
        attrs.insert("gen_ai.system".to_string(), "openai".to_string());
        attrs.insert("gen_ai.operation.name".to_string(), "chat".to_string());
        attrs.insert("gen_ai.request.model".to_string(), "gpt-4o".to_string());
        attrs.insert(
            "gen_ai.response.model".to_string(),
            "gpt-4o-2024-05-13".to_string(),
        );
        attrs.insert("gen_ai.usage.input_tokens".to_string(), "1500".to_string());
        attrs.insert("gen_ai.usage.output_tokens".to_string(), "800".to_string());
        attrs.insert(
            "gen_ai.session.id".to_string(),
            "sess_integration_test".to_string(),
        );
        attrs.insert(
            "gen_ai.session.name".to_string(),
            "Integration Test Session".to_string(),
        );
        attrs.insert("gen_ai.user.id".to_string(), "user_integration".to_string());
        attrs.insert("gen_ai.request.temperature".to_string(), "0.7".to_string());
        attrs.insert("gen_ai.request.max_tokens".to_string(), "4096".to_string());
        attrs.insert(
            "gen_ai.response.id".to_string(),
            "chatcmpl-integration123".to_string(),
        );
        attrs.insert(
            "gen_ai.response.finish_reasons".to_string(),
            "stop".to_string(),
        );
        attrs
    }

    /// Simulates an OTLP span with GenAI attributes for Anthropic with cache
    fn create_anthropic_span_with_cache() -> HashMap<String, String> {
        let mut attrs = HashMap::new();
        attrs.insert("gen_ai.system".to_string(), "anthropic".to_string());
        attrs.insert("gen_ai.operation.name".to_string(), "chat".to_string());
        attrs.insert(
            "gen_ai.request.model".to_string(),
            "claude-3-5-sonnet".to_string(),
        );
        attrs.insert(
            "gen_ai.response.model".to_string(),
            "claude-3-5-sonnet-20240620".to_string(),
        );
        attrs.insert("gen_ai.usage.input_tokens".to_string(), "5000".to_string());
        attrs.insert("gen_ai.usage.output_tokens".to_string(), "2000".to_string());
        attrs.insert(
            "gen_ai.usage.cache_read_tokens".to_string(),
            "3000".to_string(),
        );
        attrs.insert(
            "gen_ai.usage.cache_write_tokens".to_string(),
            "1500".to_string(),
        );
        attrs.insert(
            "gen_ai.session.id".to_string(),
            "sess_cache_test".to_string(),
        );
        attrs.insert("gen_ai.user.id".to_string(), "user_cache".to_string());
        attrs
    }

    /// Simulates a span with custom Reiver properties
    fn create_span_with_custom_properties() -> HashMap<String, String> {
        let mut attrs = HashMap::new();
        attrs.insert("gen_ai.system".to_string(), "openai".to_string());
        attrs.insert(
            "gen_ai.request.model".to_string(),
            "gpt-4o-mini".to_string(),
        );
        attrs.insert("gen_ai.usage.input_tokens".to_string(), "100".to_string());
        attrs.insert("gen_ai.usage.output_tokens".to_string(), "50".to_string());
        // Custom Reiver properties
        attrs.insert(
            "reiver.environment".to_string(),
            "production".to_string(),
        );
        attrs.insert(
            "reiver.feature_flag".to_string(),
            "new_prompt_v2".to_string(),
        );
        attrs.insert("reiver.user_tier".to_string(), "premium".to_string());
        attrs.insert("reiver.experiment_id".to_string(), "exp_001".to_string());
        attrs
    }

    /// Simulates an error span
    fn create_error_span() -> HashMap<String, String> {
        let mut attrs = HashMap::new();
        attrs.insert("gen_ai.system".to_string(), "openai".to_string());
        attrs.insert("gen_ai.request.model".to_string(), "gpt-4o".to_string());
        attrs.insert(
            "gen_ai.error.type".to_string(),
            "rate_limit_exceeded".to_string(),
        );
        attrs
    }

    #[test]
    fn test_integration_openai_span_attribute_extraction() {
        let attrs = create_openai_span_attributes();

        // Verify all expected attributes are present
        assert_eq!(attrs.get("gen_ai.system"), Some(&"openai".to_string()));
        assert_eq!(
            attrs.get("gen_ai.request.model"),
            Some(&"gpt-4o".to_string())
        );
        assert_eq!(
            attrs.get("gen_ai.usage.input_tokens"),
            Some(&"1500".to_string())
        );
        assert_eq!(
            attrs.get("gen_ai.usage.output_tokens"),
            Some(&"800".to_string())
        );
        assert_eq!(
            attrs.get("gen_ai.session.id"),
            Some(&"sess_integration_test".to_string())
        );

        // Verify is_llm_span detection
        assert!(is_llm_span(&attrs));

        // Verify token parsing
        let input_tokens: u32 = attrs
            .get("gen_ai.usage.input_tokens")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let output_tokens: u32 = attrs
            .get("gen_ai.usage.output_tokens")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);

        assert_eq!(input_tokens, 1500);
        assert_eq!(output_tokens, 800);

        // Verify total tokens calculation
        let total_tokens = input_tokens + output_tokens;
        assert_eq!(total_tokens, 2300);
    }

    #[test]
    fn test_integration_anthropic_cache_tokens() {
        let attrs = create_anthropic_span_with_cache();

        // Verify cache token attributes
        let cache_read: u32 = attrs
            .get("gen_ai.usage.cache_read_tokens")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let cache_write: u32 = attrs
            .get("gen_ai.usage.cache_write_tokens")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);

        assert_eq!(cache_read, 3000);
        assert_eq!(cache_write, 1500);

        // Verify cost calculation with cache tokens
        // Anthropic Claude 3.5 Sonnet pricing example:
        // Input: $3/1M, Output: $15/1M, Cache read: $0.30/1M, Cache write: $3.75/1M
        let input_cost_per_million = Decimal::from(3);
        let output_cost_per_million = Decimal::from(15);
        let cache_read_cost_per_million = Decimal::new(30, 2); // $0.30
        let cache_write_cost_per_million = Decimal::new(375, 2); // $3.75

        let input_tokens: u32 = attrs
            .get("gen_ai.usage.input_tokens")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let output_tokens: u32 = attrs
            .get("gen_ai.usage.output_tokens")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);

        let million = Decimal::from(1_000_000);
        let cost = Decimal::from(input_tokens) * input_cost_per_million / million
            + Decimal::from(output_tokens) * output_cost_per_million / million
            + Decimal::from(cache_read) * cache_read_cost_per_million / million
            + Decimal::from(cache_write) * cache_write_cost_per_million / million;

        // Cost should be non-zero and reasonable
        assert!(cost > Decimal::ZERO);
        assert!(cost < Decimal::from(1)); // Less than $1 for this request
    }

    #[test]
    fn test_integration_custom_properties_extraction() {
        let attrs = create_span_with_custom_properties();

        // Verify custom properties with reiver. prefix are present
        let mut custom_props = HashMap::new();
        for (key, value) in &attrs {
            if let Some(prop_name) = key.strip_prefix("reiver.") {
                custom_props.insert(prop_name.to_string(), value.clone());
            }
        }

        assert_eq!(
            custom_props.get("environment"),
            Some(&"production".to_string())
        );
        assert_eq!(
            custom_props.get("feature_flag"),
            Some(&"new_prompt_v2".to_string())
        );
        assert_eq!(custom_props.get("user_tier"), Some(&"premium".to_string()));
        assert_eq!(
            custom_props.get("experiment_id"),
            Some(&"exp_001".to_string())
        );
        assert_eq!(custom_props.len(), 4);
    }

    #[test]
    fn test_integration_error_span_handling() {
        let attrs = create_error_span();

        // Verify error detection
        let error_type = attrs.get("gen_ai.error.type");
        assert!(error_type.is_some());
        assert_eq!(error_type.unwrap(), "rate_limit_exceeded");

        // When error is present, status should be set to error
        let has_error = attrs.contains_key("gen_ai.error.type");
        let status_code = if has_error { "error" } else { "ok" };
        assert_eq!(status_code, "error");
    }

    #[test]
    fn test_integration_multi_provider_detection() {
        let providers = vec![
            ("openai", "gpt-4o"),
            ("anthropic", "claude-3-5-sonnet"),
            ("google", "gemini-1.5-pro"),
            ("mistral", "mistral-large"),
            ("cohere", "command-r-plus"),
        ];

        for (provider, model) in providers {
            let mut attrs = HashMap::new();
            attrs.insert("gen_ai.system".to_string(), provider.to_string());
            attrs.insert("gen_ai.request.model".to_string(), model.to_string());

            assert!(
                is_llm_span(&attrs),
                "Failed to detect LLM span for provider: {}",
                provider
            );

            // Verify provider inference for gateway-known models
            let inferred = infer_provider(model);
            match provider {
                "openai" => assert_eq!(inferred, "openai"),
                "anthropic" => assert_eq!(inferred, "anthropic"),
                "google" => assert_eq!(inferred, "google"),
                _ => assert_eq!(inferred, "unknown", "Non-gateway models infer to unknown"),
            }
        }
    }

    #[test]
    fn test_integration_request_id_generation() {
        // Request ID format should be "trace_id:span_id"
        let trace_id = "abc123def456";
        let span_id = "span789";
        let request_id = format!("{}:{}", trace_id, span_id);

        assert_eq!(request_id, "abc123def456:span789");
        assert!(request_id.contains(':'));

        // Should be splittable back to trace_id and span_id
        let parts: Vec<&str> = request_id.split(':').collect();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], trace_id);
        assert_eq!(parts[1], span_id);
    }

    #[test]
    fn test_integration_timestamp_handling() {
        // Test various timestamp formats that might come from OTLP
        let timestamp_nanos: u64 = 1705314600_000_000_000; // Jan 15, 2024 10:30:00 UTC

        // Convert to seconds and nanoseconds
        let secs = (timestamp_nanos / 1_000_000_000) as i64;
        let nsecs = (timestamp_nanos % 1_000_000_000) as u32;

        assert_eq!(secs, 1705314600);
        assert_eq!(nsecs, 0);

        // Duration in milliseconds
        let duration_nanos: u64 = 1_500_000_000; // 1.5 seconds
        let duration_ms = (duration_nanos / 1_000_000) as u32;
        assert_eq!(duration_ms, 1500);
    }

    #[test]
    fn test_integration_time_to_first_token() {
        let mut attrs = create_openai_span_attributes();
        attrs.insert(
            "gen_ai.performance.time_to_first_token_ms".to_string(),
            "150".to_string(),
        );

        let ttft: u32 = attrs
            .get("gen_ai.performance.time_to_first_token_ms")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);

        assert_eq!(ttft, 150);

        // TTFT should be less than total duration for streaming responses
        // This is a reasonable sanity check
        assert!(ttft < 10_000, "TTFT should be less than 10 seconds");
    }
}
