//! OTLP (OpenTelemetry Protocol) API Tests
//!
//! Tests for the OTLP trace, metrics, and logs ingestion endpoints.

mod helpers;

use serde_json::json;

use helpers::*;

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    // ========================================================================
    // OTLP Trace Payload Tests
    // ========================================================================

    /// Create a minimal valid OTLP traces export request
    fn create_otlp_traces_request() -> serde_json::Value {
        let timestamp_nanos = Utc::now().timestamp_nanos_opt().unwrap_or(0);

        json!({
            "resourceSpans": [{
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "test-service"}},
                        {"key": "service.version", "value": {"stringValue": "1.0.0"}}
                    ]
                },
                "scopeSpans": [{
                    "scope": {
                        "name": "reiver-test",
                        "version": "1.0.0"
                    },
                    "spans": [{
                        "traceId": "0123456789abcdef0123456789abcdef",
                        "spanId": "0123456789abcdef",
                        "parentSpanId": "",
                        "name": "test-span",
                        "kind": 2, // SPAN_KIND_SERVER
                        "startTimeUnixNano": timestamp_nanos,
                        "endTimeUnixNano": timestamp_nanos + 1000000000, // 1 second
                        "attributes": [],
                        "status": {
                            "code": 1 // STATUS_CODE_OK
                        }
                    }]
                }]
            }]
        })
    }

    #[test]
    fn test_otlp_traces_request_structure() {
        let request = create_otlp_traces_request();

        assert!(request["resourceSpans"].is_array());
        assert!(request["resourceSpans"][0]["resource"].is_object());
        assert!(request["resourceSpans"][0]["scopeSpans"].is_array());
        assert!(request["resourceSpans"][0]["scopeSpans"][0]["spans"].is_array());
    }

    #[test]
    fn test_traces_endpoint_request_creation() {
        let request = create_otlp_traces_request();
        let req = json_post("/v1/traces", &request);

        assert_eq!(req.method(), "POST");
        assert_eq!(req.uri(), "/v1/traces");
    }

    // ========================================================================
    // LLM Span Detection Tests
    // ========================================================================

    fn create_llm_span() -> serde_json::Value {
        let timestamp_nanos = Utc::now().timestamp_nanos_opt().unwrap_or(0);

        json!({
            "traceId": "0123456789abcdef0123456789abcdef",
            "spanId": "0123456789abcdef",
            "parentSpanId": "",
            "name": "chat.completions",
            "kind": 3, // SPAN_KIND_CLIENT
            "startTimeUnixNano": timestamp_nanos,
            "endTimeUnixNano": timestamp_nanos + 500000000,
            "attributes": [
                {"key": "gen_ai.system", "value": {"stringValue": "openai"}},
                {"key": "gen_ai.request.model", "value": {"stringValue": "gpt-4o"}},
                {"key": "gen_ai.response.model", "value": {"stringValue": "gpt-4o-2024-05-13"}},
                {"key": "gen_ai.usage.input_tokens", "value": {"intValue": 100}},
                {"key": "gen_ai.usage.output_tokens", "value": {"intValue": 50}},
                {"key": "gen_ai.operation.name", "value": {"stringValue": "chat"}}
            ],
            "status": {
                "code": 1
            }
        })
    }

    #[test]
    fn test_llm_span_has_genai_attributes() {
        let span = create_llm_span();
        let attrs = span["attributes"].as_array().unwrap();

        // Find gen_ai.system attribute
        let has_genai_system = attrs.iter().any(|attr| attr["key"] == "gen_ai.system");

        assert!(
            has_genai_system,
            "LLM span should have gen_ai.system attribute"
        );
    }

    #[test]
    fn test_llm_span_token_attributes() {
        let span = create_llm_span();
        let attrs = span["attributes"].as_array().unwrap();

        let input_tokens = attrs
            .iter()
            .find(|attr| attr["key"] == "gen_ai.usage.input_tokens");

        let output_tokens = attrs
            .iter()
            .find(|attr| attr["key"] == "gen_ai.usage.output_tokens");

        assert!(input_tokens.is_some(), "Should have input_tokens");
        assert!(output_tokens.is_some(), "Should have output_tokens");

        assert_eq!(input_tokens.unwrap()["value"]["intValue"], 100);
        assert_eq!(output_tokens.unwrap()["value"]["intValue"], 50);
    }

    // ========================================================================
    // OTLP Attribute Parsing Tests
    // ========================================================================

    fn parse_otlp_attribute_value(value: &serde_json::Value) -> Option<String> {
        if let Some(s) = value.get("stringValue") {
            return s.as_str().map(|s| s.to_string());
        }
        if let Some(i) = value.get("intValue") {
            return Some(i.to_string());
        }
        if let Some(d) = value.get("doubleValue") {
            return Some(d.to_string());
        }
        if let Some(b) = value.get("boolValue") {
            return Some(b.to_string());
        }
        None
    }

    #[test]
    fn test_parse_string_attribute() {
        let value = json!({"stringValue": "test"});
        assert_eq!(parse_otlp_attribute_value(&value), Some("test".to_string()));
    }

    #[test]
    fn test_parse_int_attribute() {
        let value = json!({"intValue": 42});
        assert_eq!(parse_otlp_attribute_value(&value), Some("42".to_string()));
    }

    #[test]
    fn test_parse_double_attribute() {
        let value = json!({"doubleValue": 3.14});
        assert_eq!(parse_otlp_attribute_value(&value), Some("3.14".to_string()));
    }

    #[test]
    fn test_parse_bool_attribute() {
        let value = json!({"boolValue": true});
        assert_eq!(parse_otlp_attribute_value(&value), Some("true".to_string()));
    }

    // ========================================================================
    // Span Kind Tests
    // ========================================================================

    fn span_kind_to_string(kind: i32) -> &'static str {
        match kind {
            0 => "UNSPECIFIED",
            1 => "INTERNAL",
            2 => "SERVER",
            3 => "CLIENT",
            4 => "PRODUCER",
            5 => "CONSUMER",
            _ => "UNKNOWN",
        }
    }

    #[test]
    fn test_span_kind_mapping() {
        assert_eq!(span_kind_to_string(0), "UNSPECIFIED");
        assert_eq!(span_kind_to_string(1), "INTERNAL");
        assert_eq!(span_kind_to_string(2), "SERVER");
        assert_eq!(span_kind_to_string(3), "CLIENT");
        assert_eq!(span_kind_to_string(4), "PRODUCER");
        assert_eq!(span_kind_to_string(5), "CONSUMER");
        assert_eq!(span_kind_to_string(99), "UNKNOWN");
    }

    // ========================================================================
    // Status Code Tests
    // ========================================================================

    fn status_code_to_string(code: i32) -> &'static str {
        match code {
            0 => "STATUS_CODE_UNSET",
            1 => "STATUS_CODE_OK",
            2 => "STATUS_CODE_ERROR",
            _ => "STATUS_CODE_UNKNOWN",
        }
    }

    #[test]
    fn test_status_code_mapping() {
        assert_eq!(status_code_to_string(0), "STATUS_CODE_UNSET");
        assert_eq!(status_code_to_string(1), "STATUS_CODE_OK");
        assert_eq!(status_code_to_string(2), "STATUS_CODE_ERROR");
    }

    // ========================================================================
    // Trace ID and Span ID Validation Tests
    // ========================================================================

    fn is_valid_trace_id(trace_id: &str) -> bool {
        trace_id.len() == 32 && trace_id.chars().all(|c| c.is_ascii_hexdigit())
    }

    fn is_valid_span_id(span_id: &str) -> bool {
        span_id.len() == 16 && span_id.chars().all(|c| c.is_ascii_hexdigit())
    }

    #[test]
    fn test_valid_trace_id() {
        assert!(is_valid_trace_id("0123456789abcdef0123456789abcdef"));
        assert!(is_valid_trace_id("ABCDEF0123456789ABCDEF0123456789"));
    }

    #[test]
    fn test_invalid_trace_id() {
        assert!(!is_valid_trace_id("0123456789abcdef")); // Too short
        assert!(!is_valid_trace_id("0123456789abcdef0123456789abcdefXX")); // Too long
        assert!(!is_valid_trace_id("0123456789ghijkl0123456789ghijkl")); // Invalid chars
    }

    #[test]
    fn test_valid_span_id() {
        assert!(is_valid_span_id("0123456789abcdef"));
        assert!(is_valid_span_id("ABCDEF0123456789"));
    }

    #[test]
    fn test_invalid_span_id() {
        assert!(!is_valid_span_id("01234567")); // Too short
        assert!(!is_valid_span_id("0123456789abcdef01234567")); // Too long
    }

    // ========================================================================
    // OTLP Logs Tests
    // ========================================================================

    fn create_otlp_logs_request() -> serde_json::Value {
        let timestamp_nanos = Utc::now().timestamp_nanos_opt().unwrap_or(0);

        json!({
            "resourceLogs": [{
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "test-service"}}
                    ]
                },
                "scopeLogs": [{
                    "scope": {
                        "name": "reiver-test"
                    },
                    "logRecords": [{
                        "timeUnixNano": timestamp_nanos,
                        "severityNumber": 9, // INFO
                        "severityText": "INFO",
                        "body": {"stringValue": "Test log message"},
                        "attributes": [
                            {"key": "user_id", "value": {"stringValue": "user_123"}}
                        ],
                        "traceId": "0123456789abcdef0123456789abcdef",
                        "spanId": "0123456789abcdef"
                    }]
                }]
            }]
        })
    }

    #[test]
    fn test_otlp_logs_request_structure() {
        let request = create_otlp_logs_request();

        assert!(request["resourceLogs"].is_array());
        assert!(request["resourceLogs"][0]["scopeLogs"].is_array());
        assert!(request["resourceLogs"][0]["scopeLogs"][0]["logRecords"].is_array());
    }

    #[test]
    fn test_logs_endpoint_request_creation() {
        let request = create_otlp_logs_request();
        let req = json_post("/v1/logs", &request);

        assert_eq!(req.method(), "POST");
        assert_eq!(req.uri(), "/v1/logs");
    }

    // ========================================================================
    // Severity Level Tests
    // ========================================================================

    fn severity_number_to_text(severity: i32) -> &'static str {
        match severity {
            1..=4 => "TRACE",
            5..=8 => "DEBUG",
            9..=12 => "INFO",
            13..=16 => "WARN",
            17..=20 => "ERROR",
            21..=24 => "FATAL",
            _ => "UNSPECIFIED",
        }
    }

    #[test]
    fn test_severity_mapping() {
        assert_eq!(severity_number_to_text(1), "TRACE");
        assert_eq!(severity_number_to_text(5), "DEBUG");
        assert_eq!(severity_number_to_text(9), "INFO");
        assert_eq!(severity_number_to_text(13), "WARN");
        assert_eq!(severity_number_to_text(17), "ERROR");
        assert_eq!(severity_number_to_text(21), "FATAL");
    }
}
