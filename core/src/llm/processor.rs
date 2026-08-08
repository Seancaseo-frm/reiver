//! LLM Span Processor
//!
//! Extracts GenAI semantic convention attributes from OTLP spans,
//! calculates costs, and converts to LlmRequest for storage.

use crate::error::AppError;
use crate::llm::cost::CostCalculator;
use crate::llm::types::{genai_attributes, LlmRequest};
use chrono::{DateTime, TimeZone, Utc};
use std::collections::HashMap;
use tracing::debug;

/// Custom property prefix for Reiver-specific attributes
const REIVER_PROPERTY_PREFIX: &str = "reiver.";

/// Status code constants for LLM requests
pub mod status_codes {
    /// OTLP status code for errors
    pub const OTLP_ERROR: &str = "STATUS_CODE_ERROR";
    /// Internal status code for successful requests
    pub const OK: &str = "ok";
    /// Internal status code for failed requests
    pub const ERROR: &str = "error";
}

/// Parse a u32 value from a string attribute, logging debug message on failure.
///
/// Returns the parsed value, or 0 if parsing fails or the attribute is missing.
/// This helps identify malformed span data from instrumentation libraries.
fn parse_u32_attr(attrs: &HashMap<String, String>, key: &str) -> u32 {
    match attrs.get(key) {
        Some(v) => match v.parse::<u32>() {
            Ok(val) => val,
            Err(_) => {
                debug!("Invalid {} value '{}', defaulting to 0", key, v);
                0
            }
        },
        None => 0,
    }
}

/// Try `primary_key` first, fall back to `deprecated_key`.
fn parse_u32_attr_fallback(
    attrs: &HashMap<String, String>,
    primary_key: &str,
    deprecated_key: &str,
) -> u32 {
    let val = parse_u32_attr(attrs, primary_key);
    if val > 0 {
        return val;
    }
    parse_u32_attr(attrs, deprecated_key)
}

/// LLM span processor
pub struct LlmSpanProcessor {
    cost_calculator: CostCalculator,
}

impl LlmSpanProcessor {
    /// Create a new LLM span processor
    pub fn new(cost_calculator: CostCalculator) -> Self {
        Self { cost_calculator }
    }

    /// Get a reference to the cost calculator for external cost calculations.
    pub fn cost_calculator(&self) -> &CostCalculator {
        &self.cost_calculator
    }

    /// Check if a span is an LLM span (has gen_ai.provider.name or deprecated gen_ai.system).
    pub fn is_llm_span(span_attributes: &HashMap<String, String>) -> bool {
        span_attributes.contains_key(genai_attributes::PROVIDER_NAME)
            || span_attributes.contains_key(genai_attributes::SYSTEM_DEPRECATED)
    }

    /// Process an OTLP span and extract LLM request data
    pub async fn process_span(
        &self,
        project_id: &str,
        trace_id: &str,
        span_id: &str,
        _span_name: &str,
        timestamp_nanos: u64,
        duration_nanos: u64,
        status_code: &str,
        status_message: &str,
        span_attributes: &HashMap<String, String>,
        resource_attributes: &HashMap<String, String>,
    ) -> Result<LlmRequest, AppError> {
        let mut request = LlmRequest::default();

        // Basic identifiers
        request.project_id = project_id.to_string();
        request.request_id = format!("{}:{}", trace_id, span_id);
        request.trace_id = trace_id.to_string();
        request.span_id = span_id.to_string();

        // Timing
        request.timestamp = self.nanos_to_datetime(timestamp_nanos);
        // Use saturating conversion to prevent overflow for very long durations (>71 minutes)
        request.duration_ms = (duration_nanos / 1_000_000).min(u32::MAX as u64) as u32;

        // Service name from resource attributes
        request.service_name = resource_attributes
            .get("service.name")
            .cloned()
            .unwrap_or_default();

        // Status
        request.status_code = if status_code == status_codes::OTLP_ERROR {
            status_codes::ERROR.to_string()
        } else {
            status_codes::OK.to_string()
        };
        request.error_message = status_message.to_string();

        // Extract GenAI attributes
        self.extract_genai_attributes(&mut request, span_attributes);

        // Extract custom properties (attributes with reiver. prefix)
        self.extract_custom_properties(&mut request, span_attributes);

        // Calculate cost
        if request.input_tokens > 0 || request.output_tokens > 0 {
            request.cost_usd = self
                .cost_calculator
                .calculate_cost(
                    &request.gen_ai_system,
                    &request.gen_ai_request_model,
                    request.input_tokens,
                    request.output_tokens,
                    request.cache_read_tokens,
                    request.cache_write_tokens,
                )
                .await?;
        }

        // Calculate total tokens
        request.total_tokens = request.input_tokens + request.output_tokens;

        debug!(
            "Processed LLM span: system={}, model={}, tokens={}, cost={}",
            request.gen_ai_system,
            request.gen_ai_request_model,
            request.total_tokens,
            request.cost_usd
        );

        Ok(request)
    }

    /// Extract GenAI semantic convention attributes.
    ///
    /// For renamed attributes (e.g. `gen_ai.system` -> `gen_ai.provider.name`)
    /// the new name is checked first, falling back to the deprecated alias so
    /// both old and new SDK versions work.
    fn extract_genai_attributes(&self, request: &mut LlmRequest, attrs: &HashMap<String, String>) {
        // Provider (new name first, then deprecated gen_ai.system)
        request.gen_ai_system = attrs
            .get(genai_attributes::PROVIDER_NAME)
            .or_else(|| attrs.get(genai_attributes::SYSTEM_DEPRECATED))
            .cloned()
            .unwrap_or_default();
        request.gen_ai_operation_name = attrs
            .get(genai_attributes::OPERATION_NAME)
            .cloned()
            .unwrap_or_default();

        // Model info
        request.gen_ai_request_model = attrs
            .get(genai_attributes::REQUEST_MODEL)
            .cloned()
            .unwrap_or_default();
        request.gen_ai_response_model = attrs
            .get(genai_attributes::RESPONSE_MODEL)
            .cloned()
            .unwrap_or_default();

        // Token usage
        request.input_tokens = parse_u32_attr(attrs, genai_attributes::USAGE_INPUT_TOKENS);
        request.output_tokens = parse_u32_attr(attrs, genai_attributes::USAGE_OUTPUT_TOKENS);

        // Cache tokens (new names first, then deprecated aliases)
        request.cache_read_tokens = parse_u32_attr_fallback(
            attrs,
            genai_attributes::CACHE_READ_INPUT_TOKENS,
            genai_attributes::CACHE_READ_TOKENS_DEPRECATED,
        );
        request.cache_write_tokens = parse_u32_attr_fallback(
            attrs,
            genai_attributes::CACHE_CREATION_INPUT_TOKENS,
            genai_attributes::CACHE_WRITE_TOKENS_DEPRECATED,
        );

        // Performance
        request.time_to_first_token_ms =
            parse_u32_attr(attrs, genai_attributes::TIME_TO_FIRST_TOKEN_MS);

        // Error info
        if let Some(error_type) = attrs.get(genai_attributes::ERROR_TYPE) {
            request.error_type = error_type.clone();
            request.status_code = status_codes::ERROR.to_string();
        }

        // Conversation / session tracking (new name first, then deprecated)
        request.session_id = attrs
            .get(genai_attributes::CONVERSATION_ID)
            .or_else(|| attrs.get(genai_attributes::SESSION_ID_DEPRECATED))
            .cloned()
            .unwrap_or_default();
        request.session_name = attrs
            .get(genai_attributes::SESSION_NAME)
            .cloned()
            .unwrap_or_default();
        request.user_id = attrs
            .get(genai_attributes::USER_ID)
            .cloned()
            .unwrap_or_default();

        // Content (new names first, then deprecated aliases)
        request.request_messages = attrs
            .get(genai_attributes::INPUT_MESSAGES)
            .or_else(|| attrs.get(genai_attributes::REQUEST_MESSAGES_DEPRECATED))
            .cloned()
            .unwrap_or_default();
        request.response_content = attrs
            .get(genai_attributes::OUTPUT_MESSAGES)
            .or_else(|| attrs.get(genai_attributes::RESPONSE_CONTENT_DEPRECATED))
            .cloned()
            .unwrap_or_default();

        // Extract tool call info from request_messages JSON
        if !request.request_messages.is_empty() {
            if let Ok(messages) =
                serde_json::from_str::<Vec<serde_json::Value>>(&request.request_messages)
            {
                let mut count = 0u32;
                let mut names = std::collections::HashSet::new();
                for msg in &messages {
                    if let Some(tool_calls) = msg.get("tool_calls").and_then(|v| v.as_array()) {
                        if !tool_calls.is_empty() {
                            count += tool_calls.len() as u32;
                            for tc in tool_calls {
                                if let Some(name) = tc
                                    .get("function")
                                    .and_then(|f| f.get("name"))
                                    .and_then(|n| n.as_str())
                                {
                                    names.insert(name.to_string());
                                }
                            }
                        }
                    }
                }
                request.tool_call_count = count;
                request.tool_names = names.into_iter().collect();
            }
        }

        // Request parameters -- populate dedicated columns + legacy properties map
        if let Some(v) = attrs.get(genai_attributes::REQUEST_MAX_TOKENS) {
            request.max_tokens = v.parse().ok();
            request
                .properties
                .insert("max_tokens".to_string(), v.clone());
        }
        if let Some(v) = attrs.get(genai_attributes::REQUEST_TEMPERATURE) {
            request.temperature = v.parse().ok();
            request
                .properties
                .insert("temperature".to_string(), v.clone());
        }
        if let Some(v) = attrs.get(genai_attributes::REQUEST_TOP_P) {
            request.top_p = v.parse().ok();
            request.properties.insert("top_p".to_string(), v.clone());
        }
        if let Some(v) = attrs.get(genai_attributes::REQUEST_TOP_K) {
            request.properties.insert("top_k".to_string(), v.clone());
        }
        if let Some(v) = attrs.get(genai_attributes::REQUEST_STOP_SEQUENCES) {
            request
                .properties
                .insert("stop_sequences".to_string(), v.clone());
        }
        if let Some(v) = attrs.get(genai_attributes::REQUEST_FREQUENCY_PENALTY) {
            request.frequency_penalty = v.parse().ok();
            request
                .properties
                .insert("frequency_penalty".to_string(), v.clone());
        }
        if let Some(v) = attrs.get(genai_attributes::REQUEST_PRESENCE_PENALTY) {
            request.presence_penalty = v.parse().ok();
            request
                .properties
                .insert("presence_penalty".to_string(), v.clone());
        }

        // Response metadata (stored in properties map)
        if let Some(v) = attrs.get(genai_attributes::RESPONSE_ID) {
            request
                .properties
                .insert("response_id".to_string(), v.clone());
        }
        if let Some(v) = attrs.get(genai_attributes::RESPONSE_FINISH_REASONS) {
            request
                .properties
                .insert("finish_reasons".to_string(), v.clone());
        }
    }

    /// Extract custom properties (attributes prefixed with "reiver.")
    fn extract_custom_properties(&self, request: &mut LlmRequest, attrs: &HashMap<String, String>) {
        for (key, value) in attrs {
            if let Some(property_name) = key.strip_prefix(REIVER_PROPERTY_PREFIX) {
                request
                    .properties
                    .insert(property_name.to_string(), value.clone());
            }
        }
    }

    /// Convert nanoseconds timestamp to DateTime<Utc>
    fn nanos_to_datetime(&self, nanos: u64) -> DateTime<Utc> {
        let secs = (nanos / 1_000_000_000) as i64;
        let nsecs = (nanos % 1_000_000_000) as u32;
        Utc.timestamp_opt(secs, nsecs)
            .single()
            .unwrap_or_else(Utc::now)
    }

    /// Prepare a gateway request by calculating cost. Used when batching: gateway
    /// calls this then sends the request to a buffer; a flusher does batched insert.
    pub async fn prepare_gateway_request(
        &self,
        mut request: LlmRequest,
    ) -> Result<LlmRequest, AppError> {
        request.cost_usd = self
            .cost_calculator
            .calculate_cost(
                &request.gen_ai_system,
                &request.gen_ai_request_model,
                request.input_tokens,
                request.output_tokens,
                request.cache_read_tokens,
                request.cache_write_tokens,
            )
            .await?;
        Ok(request)
    }

    /// Process a gateway request and store it in ClickHouse (single insert).
    /// Prefer batching via prepare_gateway_request + insert_llm_requests_batch for high throughput.
    pub async fn process_gateway_request(
        &self,
        request: LlmRequest,
        clickhouse: &crate::clickhouse_db::ClickHousePool,
    ) -> Result<(), AppError> {
        let request = self.prepare_gateway_request(request).await?;
        self.insert_llm_request(&request, clickhouse).await
    }

    /// Insert a batch of LLM requests into ClickHouse in one round-trip.
    pub async fn insert_llm_requests_batch(
        &self,
        requests: &[LlmRequest],
        clickhouse: &crate::clickhouse_db::ClickHousePool,
    ) -> Result<(), AppError> {
        if requests.is_empty() {
            return Ok(());
        }
        use clickhouse::Row;
        use serde::Serialize;

        #[derive(Row, Serialize)]
        struct LlmRequestInsert {
            project_id: String,
            request_id: String,
            trace_id: String,
            span_id: String,
            gen_ai_system: String,
            gen_ai_request_model: String,
            gen_ai_response_model: String,
            gen_ai_operation_name: String,
            input_tokens: u32,
            output_tokens: u32,
            total_tokens: u32,
            cache_read_tokens: u32,
            cache_write_tokens: u32,
            cost_usd: f64,
            #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
            timestamp: DateTime<Utc>,
            duration_ms: u32,
            time_to_first_token_ms: u32,
            status_code: String,
            error_type: String,
            error_message: String,
            session_id: String,
            session_name: String,
            user_id: String,
            request_messages: String,
            response_content: String,
            properties: Vec<(String, String)>,
            scores: Vec<(String, f64)>,
            service_name: String,
            fallback_used: u8,
            original_model: String,
            retry_count: u32,
            guardrail_violations: Vec<String>,
            temperature: f32,
            top_p: f32,
            max_tokens: u32,
            frequency_penalty: f32,
            presence_penalty: f32,
            tool_call_count: u32,
            tool_names: Vec<String>,
            is_platform_key: u8,
            rollout_id: String,
            rollout_variant: String,
            prompt_config_id: String,
            prompt_version_id: String,
        }

        let mut inserter = clickhouse
            .insert::<LlmRequestInsert>("llm_requests")
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse insert error: {}", e)))?;

        for request in requests {
            let row = LlmRequestInsert {
                project_id: request.project_id.clone(),
                request_id: request.request_id.clone(),
                trace_id: request.trace_id.clone(),
                span_id: request.span_id.clone(),
                gen_ai_system: request.gen_ai_system.clone(),
                gen_ai_request_model: request.gen_ai_request_model.clone(),
                gen_ai_response_model: request.gen_ai_response_model.clone(),
                gen_ai_operation_name: request.gen_ai_operation_name.clone(),
                input_tokens: request.input_tokens,
                output_tokens: request.output_tokens,
                total_tokens: request.total_tokens,
                cache_read_tokens: request.cache_read_tokens,
                cache_write_tokens: request.cache_write_tokens,
                cost_usd: request.cost_usd.try_into().unwrap_or(0.0),
                timestamp: request.timestamp,
                duration_ms: request.duration_ms,
                time_to_first_token_ms: request.time_to_first_token_ms,
                status_code: request.status_code.clone(),
                error_type: request.error_type.clone(),
                error_message: request.error_message.clone(),
                session_id: request.session_id.clone(),
                session_name: request.session_name.clone(),
                user_id: request.user_id.clone(),
                request_messages: request.request_messages.clone(),
                response_content: request.response_content.clone(),
                properties: request
                    .properties
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                scores: request
                    .scores
                    .iter()
                    .map(|(k, v)| (k.clone(), *v))
                    .collect(),
                service_name: request.service_name.clone(),
                fallback_used: request.fallback_used as u8,
                original_model: request.original_model.clone(),
                retry_count: request.retry_count,
                guardrail_violations: request.guardrail_violations.clone(),
                temperature: request.temperature.unwrap_or(0.0),
                top_p: request.top_p.unwrap_or(0.0),
                max_tokens: request.max_tokens.unwrap_or(0),
                frequency_penalty: request.frequency_penalty.unwrap_or(0.0),
                presence_penalty: request.presence_penalty.unwrap_or(0.0),
                tool_call_count: request.tool_call_count,
                tool_names: request.tool_names.clone(),
                is_platform_key: request.is_platform_key as u8,
                rollout_id: request.rollout_id.clone(),
                rollout_variant: request.rollout_variant.clone(),
                prompt_config_id: request.prompt_config_id.clone(),
                prompt_version_id: request.prompt_version_id.clone(),
            };
            inserter.write(&row).await.map_err(|e| {
                AppError::Internal(anyhow::anyhow!("ClickHouse write error: {}", e))
            })?;
        }

        inserter
            .end()
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse end error: {}", e)))?;
        Ok(())
    }

    /// Insert a single LLM request into ClickHouse.
    async fn insert_llm_request(
        &self,
        request: &LlmRequest,
        clickhouse: &crate::clickhouse_db::ClickHousePool,
    ) -> Result<(), AppError> {
        use clickhouse::Row;
        use serde::Serialize;

        #[derive(Row, Serialize)]
        struct LlmRequestInsert {
            project_id: String,
            request_id: String,
            trace_id: String,
            span_id: String,
            gen_ai_system: String,
            gen_ai_request_model: String,
            gen_ai_response_model: String,
            gen_ai_operation_name: String,
            input_tokens: u32,
            output_tokens: u32,
            total_tokens: u32,
            cache_read_tokens: u32,
            cache_write_tokens: u32,
            cost_usd: f64,
            #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
            timestamp: DateTime<Utc>,
            duration_ms: u32,
            time_to_first_token_ms: u32,
            status_code: String,
            error_type: String,
            error_message: String,
            session_id: String,
            session_name: String,
            user_id: String,
            request_messages: String,
            response_content: String,
            properties: Vec<(String, String)>,
            scores: Vec<(String, f64)>,
            service_name: String,
            fallback_used: u8,
            original_model: String,
            retry_count: u32,
            guardrail_violations: Vec<String>,
            temperature: f32,
            top_p: f32,
            max_tokens: u32,
            frequency_penalty: f32,
            presence_penalty: f32,
            tool_call_count: u32,
            tool_names: Vec<String>,
            is_platform_key: u8,
            rollout_id: String,
            rollout_variant: String,
            prompt_config_id: String,
            prompt_version_id: String,
        }

        let row = LlmRequestInsert {
            project_id: request.project_id.clone(),
            request_id: request.request_id.clone(),
            trace_id: request.trace_id.clone(),
            span_id: request.span_id.clone(),
            gen_ai_system: request.gen_ai_system.clone(),
            gen_ai_request_model: request.gen_ai_request_model.clone(),
            gen_ai_response_model: request.gen_ai_response_model.clone(),
            gen_ai_operation_name: request.gen_ai_operation_name.clone(),
            input_tokens: request.input_tokens,
            output_tokens: request.output_tokens,
            total_tokens: request.total_tokens,
            cache_read_tokens: request.cache_read_tokens,
            cache_write_tokens: request.cache_write_tokens,
            cost_usd: request.cost_usd.try_into().unwrap_or(0.0),
            timestamp: request.timestamp,
            duration_ms: request.duration_ms,
            time_to_first_token_ms: request.time_to_first_token_ms,
            status_code: request.status_code.clone(),
            error_type: request.error_type.clone(),
            error_message: request.error_message.clone(),
            session_id: request.session_id.clone(),
            session_name: request.session_name.clone(),
            user_id: request.user_id.clone(),
            request_messages: request.request_messages.clone(),
            response_content: request.response_content.clone(),
            properties: request
                .properties
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            scores: request
                .scores
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect(),
            service_name: request.service_name.clone(),
            fallback_used: request.fallback_used as u8,
            original_model: request.original_model.clone(),
            retry_count: request.retry_count,
            guardrail_violations: request.guardrail_violations.clone(),
            temperature: request.temperature.unwrap_or(0.0),
            top_p: request.top_p.unwrap_or(0.0),
            max_tokens: request.max_tokens.unwrap_or(0),
            frequency_penalty: request.frequency_penalty.unwrap_or(0.0),
            presence_penalty: request.presence_penalty.unwrap_or(0.0),
            tool_call_count: request.tool_call_count,
            tool_names: request.tool_names.clone(),
            is_platform_key: request.is_platform_key as u8,
            rollout_id: request.rollout_id.clone(),
            rollout_variant: request.rollout_variant.clone(),
            prompt_config_id: request.prompt_config_id.clone(),
            prompt_version_id: request.prompt_version_id.clone(),
        };

        let mut inserter = clickhouse
            .insert::<LlmRequestInsert>("llm_requests")
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse insert error: {}", e)))?;

        inserter
            .write(&row)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse write error: {}", e)))?;

        inserter
            .end()
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse end error: {}", e)))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, Timelike};
    use rust_decimal::Decimal;

    // Create a test processor (requires async context for PgPool)
    async fn create_test_processor() -> LlmSpanProcessor {
        LlmSpanProcessor::new(CostCalculator::new(std::sync::Arc::new(
            sqlx::PgPool::connect_lazy("postgres://localhost/test").unwrap(),
        )))
    }

    #[test]
    fn test_is_llm_span_without_genai_attribute() {
        let attrs = HashMap::new();
        assert!(!LlmSpanProcessor::is_llm_span(&attrs));
    }

    #[test]
    fn test_is_llm_span_with_provider_name() {
        let mut attrs = HashMap::new();
        attrs.insert(
            genai_attributes::PROVIDER_NAME.to_string(),
            "openai".to_string(),
        );
        assert!(LlmSpanProcessor::is_llm_span(&attrs));
    }

    #[test]
    fn test_is_llm_span_with_deprecated_system() {
        let mut attrs = HashMap::new();
        attrs.insert(
            genai_attributes::SYSTEM_DEPRECATED.to_string(),
            "openai".to_string(),
        );
        assert!(LlmSpanProcessor::is_llm_span(&attrs));
    }

    #[test]
    fn test_is_llm_span_various_providers() {
        let providers = [
            "openai",
            "anthropic",
            "google",
            "azure",
            "cohere",
            "bedrock",
        ];

        for provider in providers {
            let mut attrs = HashMap::new();
            attrs.insert(
                genai_attributes::PROVIDER_NAME.to_string(),
                provider.to_string(),
            );
            assert!(
                LlmSpanProcessor::is_llm_span(&attrs),
                "Failed for provider: {}",
                provider
            );
        }
    }

    #[tokio::test]
    async fn test_extract_custom_properties() {
        let processor = create_test_processor().await;

        let mut request = LlmRequest::default();
        let mut attrs = HashMap::new();
        attrs.insert("reiver.user_tier".to_string(), "premium".to_string());
        attrs.insert(
            "reiver.feature_flag".to_string(),
            "new_prompt".to_string(),
        );
        attrs.insert("other.attribute".to_string(), "ignored".to_string());

        processor.extract_custom_properties(&mut request, &attrs);

        assert_eq!(
            request.properties.get("user_tier"),
            Some(&"premium".to_string())
        );
        assert_eq!(
            request.properties.get("feature_flag"),
            Some(&"new_prompt".to_string())
        );
        assert!(!request.properties.contains_key("other.attribute"));
    }

    #[tokio::test]
    async fn test_extract_custom_properties_empty() {
        let processor = create_test_processor().await;

        let mut request = LlmRequest::default();
        let attrs = HashMap::new();

        processor.extract_custom_properties(&mut request, &attrs);

        assert!(request.properties.is_empty());
    }

    #[tokio::test]
    async fn test_extract_genai_attributes_full_deprecated_names() {
        let processor = create_test_processor().await;

        let mut request = LlmRequest::default();
        let mut attrs = HashMap::new();

        attrs.insert(
            genai_attributes::SYSTEM_DEPRECATED.to_string(),
            "openai".to_string(),
        );
        attrs.insert(
            genai_attributes::OPERATION_NAME.to_string(),
            "chat".to_string(),
        );
        attrs.insert(
            genai_attributes::REQUEST_MODEL.to_string(),
            "gpt-4o".to_string(),
        );
        attrs.insert(
            genai_attributes::RESPONSE_MODEL.to_string(),
            "gpt-4o-2024-05-13".to_string(),
        );
        attrs.insert(
            genai_attributes::USAGE_INPUT_TOKENS.to_string(),
            "100".to_string(),
        );
        attrs.insert(
            genai_attributes::USAGE_OUTPUT_TOKENS.to_string(),
            "50".to_string(),
        );
        attrs.insert(
            genai_attributes::SESSION_ID_DEPRECATED.to_string(),
            "session_123".to_string(),
        );
        attrs.insert(
            genai_attributes::USER_ID.to_string(),
            "user_456".to_string(),
        );

        processor.extract_genai_attributes(&mut request, &attrs);

        assert_eq!(request.gen_ai_system, "openai");
        assert_eq!(request.gen_ai_operation_name, "chat");
        assert_eq!(request.gen_ai_request_model, "gpt-4o");
        assert_eq!(request.gen_ai_response_model, "gpt-4o-2024-05-13");
        assert_eq!(request.input_tokens, 100);
        assert_eq!(request.output_tokens, 50);
        assert_eq!(request.session_id, "session_123");
        assert_eq!(request.user_id, "user_456");
    }

    #[tokio::test]
    async fn test_extract_genai_attributes_full_new_names() {
        let processor = create_test_processor().await;

        let mut request = LlmRequest::default();
        let mut attrs = HashMap::new();

        attrs.insert(
            genai_attributes::PROVIDER_NAME.to_string(),
            "openai".to_string(),
        );
        attrs.insert(
            genai_attributes::OPERATION_NAME.to_string(),
            "chat".to_string(),
        );
        attrs.insert(
            genai_attributes::REQUEST_MODEL.to_string(),
            "gpt-4o".to_string(),
        );
        attrs.insert(
            genai_attributes::RESPONSE_MODEL.to_string(),
            "gpt-4o-2024-05-13".to_string(),
        );
        attrs.insert(
            genai_attributes::USAGE_INPUT_TOKENS.to_string(),
            "100".to_string(),
        );
        attrs.insert(
            genai_attributes::USAGE_OUTPUT_TOKENS.to_string(),
            "50".to_string(),
        );
        attrs.insert(
            genai_attributes::CONVERSATION_ID.to_string(),
            "session_123".to_string(),
        );
        attrs.insert(
            genai_attributes::USER_ID.to_string(),
            "user_456".to_string(),
        );

        processor.extract_genai_attributes(&mut request, &attrs);

        assert_eq!(request.gen_ai_system, "openai");
        assert_eq!(request.gen_ai_operation_name, "chat");
        assert_eq!(request.gen_ai_request_model, "gpt-4o");
        assert_eq!(request.gen_ai_response_model, "gpt-4o-2024-05-13");
        assert_eq!(request.input_tokens, 100);
        assert_eq!(request.output_tokens, 50);
        assert_eq!(request.session_id, "session_123");
        assert_eq!(request.user_id, "user_456");
    }

    #[tokio::test]
    async fn test_extract_genai_attributes_with_cache_tokens_deprecated() {
        let processor = create_test_processor().await;

        let mut request = LlmRequest::default();
        let mut attrs = HashMap::new();

        attrs.insert(
            genai_attributes::SYSTEM_DEPRECATED.to_string(),
            "anthropic".to_string(),
        );
        attrs.insert(
            genai_attributes::USAGE_INPUT_TOKENS.to_string(),
            "1000".to_string(),
        );
        attrs.insert(
            genai_attributes::USAGE_OUTPUT_TOKENS.to_string(),
            "500".to_string(),
        );
        attrs.insert(
            genai_attributes::CACHE_READ_TOKENS_DEPRECATED.to_string(),
            "800".to_string(),
        );
        attrs.insert(
            genai_attributes::CACHE_WRITE_TOKENS_DEPRECATED.to_string(),
            "200".to_string(),
        );

        processor.extract_genai_attributes(&mut request, &attrs);

        assert_eq!(request.input_tokens, 1000);
        assert_eq!(request.output_tokens, 500);
        assert_eq!(request.cache_read_tokens, 800);
        assert_eq!(request.cache_write_tokens, 200);
    }

    #[tokio::test]
    async fn test_extract_genai_attributes_with_cache_tokens_new_names() {
        let processor = create_test_processor().await;

        let mut request = LlmRequest::default();
        let mut attrs = HashMap::new();

        attrs.insert(
            genai_attributes::PROVIDER_NAME.to_string(),
            "anthropic".to_string(),
        );
        attrs.insert(
            genai_attributes::USAGE_INPUT_TOKENS.to_string(),
            "1000".to_string(),
        );
        attrs.insert(
            genai_attributes::USAGE_OUTPUT_TOKENS.to_string(),
            "500".to_string(),
        );
        attrs.insert(
            genai_attributes::CACHE_READ_INPUT_TOKENS.to_string(),
            "800".to_string(),
        );
        attrs.insert(
            genai_attributes::CACHE_CREATION_INPUT_TOKENS.to_string(),
            "200".to_string(),
        );

        processor.extract_genai_attributes(&mut request, &attrs);

        assert_eq!(request.input_tokens, 1000);
        assert_eq!(request.output_tokens, 500);
        assert_eq!(request.cache_read_tokens, 800);
        assert_eq!(request.cache_write_tokens, 200);
    }

    #[tokio::test]
    async fn test_extract_genai_attributes_with_error() {
        let processor = create_test_processor().await;

        let mut request = LlmRequest::default();
        let mut attrs = HashMap::new();

        attrs.insert(
            genai_attributes::PROVIDER_NAME.to_string(),
            "openai".to_string(),
        );
        attrs.insert(
            genai_attributes::ERROR_TYPE.to_string(),
            "rate_limit_exceeded".to_string(),
        );

        processor.extract_genai_attributes(&mut request, &attrs);

        assert_eq!(request.error_type, "rate_limit_exceeded");
        assert_eq!(request.status_code, status_codes::ERROR);
    }

    #[tokio::test]
    async fn test_extract_genai_attributes_missing_values() {
        let processor = create_test_processor().await;

        let mut request = LlmRequest::default();
        let attrs = HashMap::new();

        processor.extract_genai_attributes(&mut request, &attrs);

        assert_eq!(request.gen_ai_system, "");
        assert_eq!(request.input_tokens, 0);
        assert_eq!(request.output_tokens, 0);
    }

    #[tokio::test]
    async fn test_extract_genai_attributes_invalid_token_values() {
        let processor = create_test_processor().await;

        let mut request = LlmRequest::default();
        let mut attrs = HashMap::new();

        attrs.insert(
            genai_attributes::USAGE_INPUT_TOKENS.to_string(),
            "not_a_number".to_string(),
        );
        attrs.insert(
            genai_attributes::USAGE_OUTPUT_TOKENS.to_string(),
            "-100".to_string(),
        );

        processor.extract_genai_attributes(&mut request, &attrs);

        // Invalid values should default to 0
        assert_eq!(request.input_tokens, 0);
        assert_eq!(request.output_tokens, 0);
    }

    #[tokio::test]
    async fn test_extract_genai_request_parameters() {
        let processor = create_test_processor().await;

        let mut request = LlmRequest::default();
        let mut attrs = HashMap::new();

        attrs.insert(
            genai_attributes::PROVIDER_NAME.to_string(),
            "openai".to_string(),
        );
        attrs.insert(
            genai_attributes::REQUEST_MAX_TOKENS.to_string(),
            "4096".to_string(),
        );
        attrs.insert(
            genai_attributes::REQUEST_TEMPERATURE.to_string(),
            "0.7".to_string(),
        );
        attrs.insert(
            genai_attributes::REQUEST_TOP_P.to_string(),
            "0.9".to_string(),
        );
        attrs.insert(
            genai_attributes::REQUEST_TOP_K.to_string(),
            "50".to_string(),
        );
        attrs.insert(
            genai_attributes::REQUEST_FREQUENCY_PENALTY.to_string(),
            "0.5".to_string(),
        );
        attrs.insert(
            genai_attributes::REQUEST_PRESENCE_PENALTY.to_string(),
            "0.3".to_string(),
        );
        attrs.insert(
            genai_attributes::RESPONSE_ID.to_string(),
            "chatcmpl-abc123".to_string(),
        );
        attrs.insert(
            genai_attributes::RESPONSE_FINISH_REASONS.to_string(),
            "stop".to_string(),
        );

        processor.extract_genai_attributes(&mut request, &attrs);

        // Additional parameters should be stored in properties map
        assert_eq!(
            request.properties.get("max_tokens"),
            Some(&"4096".to_string())
        );
        assert_eq!(
            request.properties.get("temperature"),
            Some(&"0.7".to_string())
        );
        assert_eq!(request.properties.get("top_p"), Some(&"0.9".to_string()));
        assert_eq!(request.properties.get("top_k"), Some(&"50".to_string()));
        assert_eq!(
            request.properties.get("frequency_penalty"),
            Some(&"0.5".to_string())
        );
        assert_eq!(
            request.properties.get("presence_penalty"),
            Some(&"0.3".to_string())
        );
        assert_eq!(
            request.properties.get("response_id"),
            Some(&"chatcmpl-abc123".to_string())
        );
        assert_eq!(
            request.properties.get("finish_reasons"),
            Some(&"stop".to_string())
        );
    }

    #[tokio::test]
    async fn test_nanos_to_datetime() {
        let processor = create_test_processor().await;

        // Test known timestamp: 2024-01-15 10:30:00 UTC
        let nanos: u64 = 1705314600_000_000_000; // Jan 15, 2024 10:30:00 UTC
        let dt = processor.nanos_to_datetime(nanos);

        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 15);
        assert_eq!(dt.hour(), 10);
        assert_eq!(dt.minute(), 30);
    }

    #[tokio::test]
    async fn test_nanos_to_datetime_with_nanoseconds() {
        let processor = create_test_processor().await;

        let nanos: u64 = 1705314600_123_456_789;
        let dt = processor.nanos_to_datetime(nanos);

        assert_eq!(dt.nanosecond() % 1_000_000_000, 123_456_789);
    }

    #[test]
    fn test_llm_request_default() {
        let request = LlmRequest::default();

        assert!(request.project_id.is_empty());
        assert!(request.trace_id.is_empty());
        assert_eq!(request.input_tokens, 0);
        assert_eq!(request.output_tokens, 0);
        assert_eq!(request.cost_usd, Decimal::ZERO);
    }

    #[test]
    fn test_reiver_property_prefix() {
        assert_eq!(REIVER_PROPERTY_PREFIX, "reiver.");
    }

    #[test]
    fn test_status_code_mapping() {
        // Error status
        let status_code = status_codes::OTLP_ERROR;
        let mapped = if status_code == status_codes::OTLP_ERROR {
            status_codes::ERROR.to_string()
        } else {
            status_codes::OK.to_string()
        };
        assert_eq!(mapped, status_codes::ERROR);

        // OK status
        let status_code = "STATUS_CODE_OK";
        let mapped = if status_code == status_codes::OTLP_ERROR {
            status_codes::ERROR.to_string()
        } else {
            status_codes::OK.to_string()
        };
        assert_eq!(mapped, status_codes::OK);

        // Unset status
        let status_code = "STATUS_CODE_UNSET";
        let mapped = if status_code == status_codes::OTLP_ERROR {
            status_codes::ERROR.to_string()
        } else {
            status_codes::OK.to_string()
        };
        assert_eq!(mapped, status_codes::OK);
    }

    #[test]
    fn test_parse_u32_attr_valid() {
        let mut attrs = HashMap::new();
        attrs.insert("key".to_string(), "12345".to_string());

        assert_eq!(parse_u32_attr(&attrs, "key"), 12345);
    }

    #[test]
    fn test_parse_u32_attr_missing() {
        let attrs = HashMap::new();
        assert_eq!(parse_u32_attr(&attrs, "missing_key"), 0);
    }

    #[test]
    fn test_parse_u32_attr_invalid_string() {
        let mut attrs = HashMap::new();
        attrs.insert("key".to_string(), "not_a_number".to_string());

        // Invalid string should return 0 (and log a debug message)
        assert_eq!(parse_u32_attr(&attrs, "key"), 0);
    }

    #[test]
    fn test_parse_u32_attr_negative() {
        let mut attrs = HashMap::new();
        attrs.insert("key".to_string(), "-100".to_string());

        // Negative number should fail to parse as u32 and return 0
        assert_eq!(parse_u32_attr(&attrs, "key"), 0);
    }

    #[test]
    fn test_parse_u32_attr_overflow() {
        let mut attrs = HashMap::new();
        attrs.insert("key".to_string(), "9999999999999".to_string());

        // Number too large for u32 should return 0
        assert_eq!(parse_u32_attr(&attrs, "key"), 0);
    }

    #[tokio::test]
    async fn test_extract_tool_calls_from_request_messages() {
        let processor = create_test_processor().await;
        let mut request = LlmRequest::default();
        let mut attrs = HashMap::new();

        attrs.insert(
            genai_attributes::PROVIDER_NAME.to_string(),
            "openai".to_string(),
        );
        attrs.insert(genai_attributes::INPUT_MESSAGES.to_string(), serde_json::json!([
            {"role": "user", "content": "Search for cats"},
            {"role": "assistant", "content": null, "tool_calls": [
                {"id": "tc1", "type": "function", "function": {"name": "web_search", "arguments": "{\"q\":\"cats\"}"}},
                {"id": "tc2", "type": "function", "function": {"name": "image_gen", "arguments": "{\"prompt\":\"cat\"}"}}
            ]},
            {"role": "tool", "content": "search results...", "tool_call_id": "tc1"},
            {"role": "tool", "content": "image url...", "tool_call_id": "tc2"},
            {"role": "assistant", "content": "Here are results and an image."}
        ]).to_string());

        processor.extract_genai_attributes(&mut request, &attrs);

        assert_eq!(request.tool_call_count, 2);
        assert_eq!(request.tool_names.len(), 2);
        assert!(request.tool_names.contains(&"web_search".to_string()));
        assert!(request.tool_names.contains(&"image_gen".to_string()));
    }

    #[tokio::test]
    async fn test_extract_tool_calls_no_tools() {
        let processor = create_test_processor().await;
        let mut request = LlmRequest::default();
        let mut attrs = HashMap::new();

        attrs.insert(
            genai_attributes::PROVIDER_NAME.to_string(),
            "openai".to_string(),
        );
        attrs.insert(
            genai_attributes::INPUT_MESSAGES.to_string(),
            serde_json::json!([
                {"role": "user", "content": "Hello"},
                {"role": "assistant", "content": "Hi there!"}
            ])
            .to_string(),
        );

        processor.extract_genai_attributes(&mut request, &attrs);

        assert_eq!(request.tool_call_count, 0);
        assert!(request.tool_names.is_empty());
    }

    #[tokio::test]
    async fn test_extract_tool_calls_multiple_rounds() {
        let processor = create_test_processor().await;
        let mut request = LlmRequest::default();
        let mut attrs = HashMap::new();

        attrs.insert(
            genai_attributes::PROVIDER_NAME.to_string(),
            "openai".to_string(),
        );
        attrs.insert(genai_attributes::INPUT_MESSAGES.to_string(), serde_json::json!([
            {"role": "assistant", "tool_calls": [
                {"id": "tc1", "type": "function", "function": {"name": "search", "arguments": "{}"}}
            ]},
            {"role": "tool", "content": "result1", "tool_call_id": "tc1"},
            {"role": "assistant", "tool_calls": [
                {"id": "tc2", "type": "function", "function": {"name": "search", "arguments": "{}"}},
                {"id": "tc3", "type": "function", "function": {"name": "fetch", "arguments": "{}"}}
            ]},
        ]).to_string());

        processor.extract_genai_attributes(&mut request, &attrs);

        assert_eq!(request.tool_call_count, 3);
        assert_eq!(
            request.tool_names.len(),
            2,
            "Duplicate tool names should be deduplicated"
        );
        assert!(request.tool_names.contains(&"search".to_string()));
        assert!(request.tool_names.contains(&"fetch".to_string()));
    }

    #[tokio::test]
    async fn test_extract_tool_calls_empty_messages() {
        let processor = create_test_processor().await;
        let mut request = LlmRequest::default();
        let mut attrs = HashMap::new();

        attrs.insert(
            genai_attributes::PROVIDER_NAME.to_string(),
            "openai".to_string(),
        );
        // Empty request_messages - should not extract any tools
        processor.extract_genai_attributes(&mut request, &attrs);
        assert_eq!(request.tool_call_count, 0);
        assert!(request.tool_names.is_empty());
    }

    #[tokio::test]
    async fn test_extract_tool_calls_malformed_json() {
        let processor = create_test_processor().await;
        let mut request = LlmRequest::default();
        let mut attrs = HashMap::new();

        attrs.insert(
            genai_attributes::PROVIDER_NAME.to_string(),
            "openai".to_string(),
        );
        attrs.insert(
            genai_attributes::REQUEST_MESSAGES_DEPRECATED.to_string(),
            "not valid json".to_string(),
        );

        processor.extract_genai_attributes(&mut request, &attrs);
        assert_eq!(request.tool_call_count, 0);
        assert!(request.tool_names.is_empty());
    }
}
