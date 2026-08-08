//! Observability for the AI Gateway.
//!
//! Captures OpenTelemetry spans for each LLM request, following the
//! GenAI semantic conventions.

use chrono::Utc;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

use crate::gateway::types::{ChatCompletionRequest, ChatCompletionResponse, Usage};
use crate::llm::types::LlmRequest;

/// Extract the real OTel trace ID and span ID from the current tracing span,
/// falling back to random UUIDs when no OTel context is available (e.g. tests).
pub(crate) fn current_otel_ids() -> (String, String) {
    use opentelemetry::trace::TraceContextExt;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let cx = tracing::Span::current().context();
    let span_ref = cx.span();
    let sc = span_ref.span_context();

    if sc.is_valid() {
        (sc.trace_id().to_string(), sc.span_id().to_string())
    } else {
        let tid = Uuid::new_v4().to_string();
        let sid = Uuid::new_v4()
            .to_string()
            .split('-')
            .next()
            .unwrap_or("")
            .to_string();
        (tid, sid)
    }
}

/// Extract tool call count and unique tool names from ChatCompletionRequest messages.
fn extract_tool_info(request: &ChatCompletionRequest) -> (u32, Vec<String>) {
    let mut count = 0u32;
    let mut names = std::collections::HashSet::new();
    for msg in &request.messages {
        if let Some(tool_calls) = &msg.tool_calls {
            count += tool_calls.len() as u32;
            for tc in tool_calls {
                if !tc.function.name.is_empty() {
                    names.insert(tc.function.name.clone());
                }
            }
        }
    }
    (count, names.into_iter().collect())
}

/// Clamp a `Duration` to `u32` milliseconds, saturating at `u32::MAX` instead
/// of silently wrapping on very large durations.
fn duration_to_ms_u32(d: Duration) -> u32 {
    d.as_millis().min(u32::MAX as u128) as u32
}

/// Average characters per token for estimation.
///
/// This approximation is based on OpenAI's tokenizer statistics for English text.
/// The actual ratio varies significantly by:
/// - **Model**: Different tokenizers (GPT, Claude, Gemini) have different vocabularies
/// - **Language**: Non-English text typically has more tokens per character
/// - **Content type**: Code often has shorter tokens than prose
///
/// # Known Limitations
///
/// | Content Type | Expected Accuracy |
/// |--------------|-------------------|
/// | English prose | ~80% (underestimates by 20-30%) |
/// | English code | ~60% (underestimates, short tokens) |
/// | CJK (Chinese, Japanese, Korean) | ~30-50% (significantly underestimates) |
/// | Cyrillic, Arabic, Hebrew | ~50-70% (underestimates) |
/// | Mixed content | Variable |
///
/// For accurate token counts, use the provider's tokenizer or wait for usage data
/// in the response. This estimation is only used as a fallback when providers
/// don't return token counts in streaming responses.
const CHARS_PER_TOKEN: usize = 4;

/// Estimate token count from text using a simple character-based heuristic.
///
/// This is used as a **fallback only** when providers don't return token usage
/// in streaming responses. The estimation uses ~4 characters per token, which
/// is calibrated for English text.
///
/// # Important Limitations
///
/// **This estimation can be significantly inaccurate for:**
/// - Non-English text (especially CJK scripts which may use 1-2 chars per token)
/// - Code (which often has many short tokens like brackets, operators)
/// - Mixed content (emojis, special characters)
///
/// The `tokens_estimated` property is set to `"true"` in the LlmRequest when
/// this fallback is used, allowing downstream systems to flag these estimates.
///
/// # Accuracy by Content Type
///
/// - **English prose**: typically within 20-30% of actual token count
/// - **English code**: may underestimate by 40-50% (code has shorter tokens)
/// - **CJK text**: may underestimate by 50-70% (each character often = 1+ tokens)
/// - **Mixed content**: highly variable
///
/// # Arguments
/// * `text` - The text to estimate tokens for
///
/// # Returns
/// Estimated token count (minimum of 1 for non-empty text)
pub fn estimate_tokens(text: &str) -> u32 {
    if text.is_empty() {
        return 0;
    }
    // Use character count divided by average chars per token
    // Add 1 to account for rounding and ensure at least 1 token for non-empty text
    let char_count = text.chars().count();
    ((char_count + CHARS_PER_TOKEN - 1) / CHARS_PER_TOKEN).max(1) as u32
}

/// Estimate input tokens from a chat completion request.
///
/// Sums up estimated tokens from all messages including role prefixes.
pub fn estimate_input_tokens(request: &ChatCompletionRequest) -> u32 {
    let mut total = 0u32;

    for message in &request.messages {
        // Count role (adds a few tokens per message for formatting)
        total += 4; // Approximate overhead per message

        // Count content
        if let Some(content) = &message.content {
            let text = content.as_text();
            total += estimate_tokens(&text);
        }

        // Count name if present
        if let Some(name) = &message.name {
            total += estimate_tokens(name) + 1;
        }
    }

    total
}

/// Parameters for building an LlmRequest from a successful gateway request/response.
pub struct LlmRequestParams<'a> {
    pub project_id: Uuid,
    pub request: &'a ChatCompletionRequest,
    pub response: &'a ChatCompletionResponse,
    pub provider: &'a str,
    pub duration: Duration,
    pub log_content: bool,
    pub fallback_used: bool,
    pub original_model: String,
    pub retry_count: u32,
    pub guardrail_violations: Vec<String>,
    pub is_platform_key: bool,
}

/// Build an LlmRequest from gateway request/response for storage and analysis.
///
/// # Arguments
/// * `params` - All parameters bundled in [`LlmRequestParams`]
pub fn build_llm_request(params: LlmRequestParams<'_>) -> LlmRequest {
    let LlmRequestParams {
        project_id,
        request,
        response,
        provider,
        duration,
        log_content,
        fallback_used,
        original_model,
        retry_count,
        guardrail_violations,
        is_platform_key,
    } = params;
    let (trace_id, span_id) = current_otel_ids();

    let usage = response.usage.clone();

    // Extract request messages as JSON (only if content logging is enabled)
    let request_messages = if log_content {
        serde_json::to_string(&request.messages).unwrap_or_default()
    } else {
        String::new()
    };

    // Extract response content (only if content logging is enabled)
    let response_content = if log_content {
        response
            .choices
            .first()
            .and_then(|c| c.message.content.clone())
            .unwrap_or_default()
    } else {
        String::new()
    };

    let (tool_call_count, tool_names) = extract_tool_info(request);

    LlmRequest {
        project_id: project_id.to_string(),
        request_id: format!("{}:{}", trace_id, span_id),
        trace_id: trace_id.clone(),
        span_id,
        gen_ai_system: provider.to_string(),
        gen_ai_request_model: request.model.clone(),
        gen_ai_response_model: response.model.clone(),
        gen_ai_operation_name: "chat".to_string(),
        input_tokens: usage.prompt_tokens,
        output_tokens: usage.completion_tokens,
        total_tokens: usage.total_tokens,
        cache_read_tokens: usage
            .prompt_tokens_details
            .as_ref()
            .map(|d| d.cached_tokens)
            .unwrap_or(0),
        cache_write_tokens: 0,
        cost_usd: Decimal::ZERO,
        timestamp: Utc::now(),
        duration_ms: duration_to_ms_u32(duration),
        time_to_first_token_ms: 0,
        status_code: "ok".to_string(),
        error_type: String::new(),
        error_message: String::new(),
        session_id: String::new(),
        session_name: String::new(),
        user_id: request.user.clone().unwrap_or_default(),
        request_messages,
        response_content,
        properties: HashMap::new(),
        scores: HashMap::new(),
        service_name: "reiver-gateway".to_string(),
        fallback_used,
        original_model,
        retry_count,
        guardrail_violations,
        temperature: request.temperature,
        top_p: request.top_p,
        max_tokens: request.max_tokens,
        frequency_penalty: request.frequency_penalty,
        presence_penalty: request.presence_penalty,
        tool_call_count,
        tool_names,
        is_platform_key,
        rollout_id: String::new(),
        rollout_variant: String::new(),
        prompt_config_id: String::new(),
        prompt_version_id: String::new(),
    }
}

/// Parameters for building an LlmRequest from a failed gateway request.
pub struct ErrorLlmRequestParams<'a> {
    pub project_id: Uuid,
    pub request: &'a ChatCompletionRequest,
    pub provider: &'a str,
    pub duration: Duration,
    pub error_type: &'a str,
    pub error_message: &'a str,
    pub log_content: bool,
    pub fallback_used: bool,
    pub original_model: String,
    pub retry_count: u32,
    pub guardrail_violations: Vec<String>,
    pub is_platform_key: bool,
}

/// Build an LlmRequest for a failed gateway request.
///
/// # Arguments
/// * `params` - All parameters bundled in [`ErrorLlmRequestParams`]
pub fn build_error_llm_request(params: ErrorLlmRequestParams<'_>) -> LlmRequest {
    let ErrorLlmRequestParams {
        project_id,
        request,
        provider,
        duration,
        error_type,
        error_message,
        log_content,
        fallback_used,
        original_model,
        retry_count,
        guardrail_violations,
        is_platform_key,
    } = params;
    let (trace_id, span_id) = current_otel_ids();

    // Only log request messages if content logging is enabled
    let request_messages = if log_content {
        serde_json::to_string(&request.messages).unwrap_or_default()
    } else {
        String::new()
    };

    let (tool_call_count, tool_names) = extract_tool_info(request);

    LlmRequest {
        project_id: project_id.to_string(),
        request_id: format!("{}:{}", trace_id, span_id),
        trace_id,
        span_id,
        gen_ai_system: provider.to_string(),
        gen_ai_request_model: request.model.clone(),
        gen_ai_response_model: String::new(),
        gen_ai_operation_name: "chat".to_string(),
        input_tokens: 0,
        output_tokens: 0,
        total_tokens: 0,
        cache_read_tokens: 0,
        cache_write_tokens: 0,
        cost_usd: Decimal::ZERO,
        timestamp: Utc::now(),
        duration_ms: duration_to_ms_u32(duration),
        time_to_first_token_ms: 0,
        status_code: "error".to_string(),
        error_type: error_type.to_string(),
        error_message: error_message.to_string(),
        session_id: String::new(),
        session_name: String::new(),
        user_id: request.user.clone().unwrap_or_default(),
        request_messages,
        response_content: String::new(),
        properties: HashMap::new(),
        scores: HashMap::new(),
        service_name: "reiver-gateway".to_string(),
        fallback_used,
        original_model,
        retry_count,
        guardrail_violations,
        temperature: request.temperature,
        top_p: request.top_p,
        max_tokens: request.max_tokens,
        frequency_penalty: request.frequency_penalty,
        presence_penalty: request.presence_penalty,
        tool_call_count,
        tool_names,
        is_platform_key,
        rollout_id: String::new(),
        rollout_variant: String::new(),
        prompt_config_id: String::new(),
        prompt_version_id: String::new(),
    }
}

/// Parameters for building an LlmRequest from a streaming gateway request.
/// Uses completion summary only; response_content is empty (aggregate from llm_chunks if needed).
pub struct StreamingLlmRequestParams<'a> {
    pub project_id: Uuid,
    pub request: &'a ChatCompletionRequest,
    pub provider: &'a str,
    pub model: String,
    pub duration: Duration,
    pub time_to_first_token_ms: u32,
    pub usage: Option<Usage>,
    pub log_content: bool,
    pub fallback_used: bool,
    pub original_model: String,
    pub retry_count: u32,
    pub guardrail_violations: Vec<String>,
    pub is_platform_key: bool,
}

/// Build an LlmRequest from streaming gateway request for storage and analysis.
///
/// This is used after a streaming request completes, with data from the completion summary.
/// Response content is not stored here; aggregate from llm_chunks if needed.
///
/// # Token Estimation
/// When the provider doesn't return token usage, we estimate input tokens from the request;
/// output tokens are set to 0 (provider did not report usage).
pub fn build_streaming_llm_request(params: StreamingLlmRequestParams<'_>) -> LlmRequest {
    let StreamingLlmRequestParams {
        project_id,
        request,
        provider,
        model,
        duration,
        time_to_first_token_ms,
        usage,
        log_content,
        fallback_used,
        original_model,
        retry_count,
        guardrail_violations,
        is_platform_key,
    } = params;
    let (trace_id, span_id) = current_otel_ids();

    // Determine if we need to estimate tokens
    let (input_tokens, output_tokens, total_tokens, tokens_estimated) = match &usage {
        Some(u) if u.prompt_tokens > 0 || u.completion_tokens > 0 => {
            (u.prompt_tokens, u.completion_tokens, u.total_tokens, false)
        }
        _ => {
            let estimated_input = estimate_input_tokens(request);
            tracing::debug!(
                provider = %provider,
                model = %model,
                estimated_input = estimated_input,
                "Estimated input tokens for streaming request (no usage from provider)"
            );
            (estimated_input, 0, estimated_input, true)
        }
    };

    let request_messages = if log_content {
        serde_json::to_string(&request.messages).unwrap_or_default()
    } else {
        String::new()
    };

    let mut properties = HashMap::new();
    if tokens_estimated {
        properties.insert("tokens_estimated".to_string(), "true".to_string());
    }

    let (tool_call_count, tool_names) = extract_tool_info(request);

    LlmRequest {
        project_id: project_id.to_string(),
        request_id: format!("{}:{}", trace_id, span_id),
        trace_id: trace_id.clone(),
        span_id,
        gen_ai_system: provider.to_string(),
        gen_ai_request_model: request.model.clone(),
        gen_ai_response_model: model,
        gen_ai_operation_name: "chat".to_string(),
        input_tokens,
        output_tokens,
        total_tokens,
        cache_read_tokens: usage
            .as_ref()
            .and_then(|u| u.prompt_tokens_details.as_ref())
            .map(|d| d.cached_tokens)
            .unwrap_or(0),
        cache_write_tokens: 0,
        cost_usd: Decimal::ZERO,
        timestamp: Utc::now(),
        duration_ms: duration_to_ms_u32(duration),
        time_to_first_token_ms,
        status_code: "ok".to_string(),
        error_type: String::new(),
        error_message: String::new(),
        session_id: String::new(),
        session_name: String::new(),
        user_id: request.user.clone().unwrap_or_default(),
        request_messages,
        response_content: String::new(),
        properties,
        scores: HashMap::new(),
        service_name: "reiver-gateway".to_string(),
        fallback_used,
        original_model,
        retry_count,
        guardrail_violations,
        temperature: request.temperature,
        top_p: request.top_p,
        max_tokens: request.max_tokens,
        frequency_penalty: request.frequency_penalty,
        presence_penalty: request.presence_penalty,
        tool_call_count,
        tool_names,
        is_platform_key,
        rollout_id: String::new(),
        rollout_variant: String::new(),
        prompt_config_id: String::new(),
        prompt_version_id: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::types::{
        AssistantMessage, ChatMessage, Choice, FinishReason, MessageContent, MessageRole,
    };

    #[test]
    fn test_build_llm_request() {
        let project_id = Uuid::new_v4();
        let request = ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text("Hello".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            }],
            user: Some("user-123".to_string()),
            ..Default::default()
        };

        let response = ChatCompletionResponse::new(
            "chatcmpl-123".to_string(),
            "gpt-4o".to_string(),
            vec![Choice {
                index: 0,
                message: AssistantMessage {
                    role: MessageRole::Assistant,
                    content: Some("Hi there!".to_string()),
                    tool_calls: None,
                    thinking: None,
                },
                finish_reason: FinishReason::Stop,
                logprobs: None,
            }],
            Usage {
                prompt_tokens: 10,
                completion_tokens: 5,
                total_tokens: 15,
                thinking_tokens: None,
                completion_tokens_details: None,
                prompt_tokens_details: None,
            },
        );

        let llm_request = build_llm_request(LlmRequestParams {
            project_id,
            request: &request,
            response: &response,
            provider: "openai",
            duration: Duration::from_millis(500),
            log_content: true,
            fallback_used: false,
            original_model: "gpt-4o".to_string(),
            retry_count: 0,
            guardrail_violations: Vec::new(),
            is_platform_key: false,
        });

        assert_eq!(llm_request.project_id, project_id.to_string());
        assert_eq!(llm_request.gen_ai_system, "openai");
        assert_eq!(llm_request.gen_ai_request_model, "gpt-4o");
        assert_eq!(llm_request.input_tokens, 10);
        assert_eq!(llm_request.output_tokens, 5);
        assert_eq!(llm_request.user_id, "user-123");
        assert_eq!(llm_request.status_code, "ok");
    }

    /// Regression: `duration.as_millis() as u32` silently wraps on very large
    /// durations (>49 days). The fix clamps to `u32::MAX` instead.
    #[test]
    fn test_duration_to_ms_u32_clamps_large_values() {
        let huge = Duration::from_millis(u64::MAX);
        assert_eq!(duration_to_ms_u32(huge), u32::MAX);

        let normal = Duration::from_millis(500);
        assert_eq!(duration_to_ms_u32(normal), 500);

        let zero = Duration::ZERO;
        assert_eq!(duration_to_ms_u32(zero), 0);
    }

    #[test]
    fn test_estimate_tokens_empty() {
        assert_eq!(estimate_tokens(""), 0);
    }

    #[test]
    fn test_estimate_tokens_short() {
        // "Hello" = 5 chars, ~1-2 tokens
        let tokens = estimate_tokens("Hello");
        assert!(tokens >= 1 && tokens <= 3);
    }

    #[test]
    fn test_estimate_tokens_sentence() {
        // "Hello, how are you today?" = 26 chars, ~6-7 tokens
        let tokens = estimate_tokens("Hello, how are you today?");
        assert!(tokens >= 4 && tokens <= 10);
    }

    #[test]
    fn test_estimate_tokens_long_text() {
        // 400 chars should be ~100 tokens
        let text = "a".repeat(400);
        let tokens = estimate_tokens(&text);
        assert_eq!(tokens, 100);
    }

    #[test]
    fn test_estimate_input_tokens() {
        let request = ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![
                ChatMessage {
                    role: MessageRole::System,
                    content: Some(MessageContent::Text(
                        "You are a helpful assistant.".to_string(),
                    )),
                    name: None,
                    tool_calls: None,
                    tool_call_id: None,
                    reasoning_content: None,
                },
                ChatMessage {
                    role: MessageRole::User,
                    content: Some(MessageContent::Text("Hello!".to_string())),
                    name: None,
                    tool_calls: None,
                    tool_call_id: None,
                    reasoning_content: None,
                },
            ],
            ..Default::default()
        };

        let tokens = estimate_input_tokens(&request);
        // Should include overhead (4 per message * 2 = 8) plus content tokens
        assert!(tokens >= 10, "Expected at least 10 tokens, got {}", tokens);
        assert!(tokens <= 50, "Expected at most 50 tokens, got {}", tokens);
    }

    use crate::gateway::types::{FunctionCall, ToolCall, ToolType};

    fn empty_request() -> ChatCompletionRequest {
        ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![],
            ..Default::default()
        }
    }

    #[test]
    fn test_extract_tool_info_no_messages() {
        let req = empty_request();
        let (count, names) = extract_tool_info(&req);
        assert_eq!(count, 0);
        assert!(names.is_empty());
    }

    #[test]
    fn test_extract_tool_info_no_tool_calls() {
        let mut req = empty_request();
        req.messages = vec![
            ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text("Hello".into())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            },
            ChatMessage {
                role: MessageRole::Assistant,
                content: Some(MessageContent::Text("Hi".into())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            },
        ];
        let (count, names) = extract_tool_info(&req);
        assert_eq!(count, 0);
        assert!(names.is_empty());
    }

    #[test]
    fn test_extract_tool_info_with_tool_calls() {
        let mut req = empty_request();
        req.messages = vec![ChatMessage {
            role: MessageRole::Assistant,
            content: None,
            name: None,
            tool_calls: Some(vec![
                ToolCall {
                    index: None,
                    id: "tc1".into(),
                    tool_type: ToolType::Function,
                    function: FunctionCall {
                        name: "web_search".into(),
                        arguments: "{}".into(),
                    },
                },
                ToolCall {
                    index: None,
                    id: "tc2".into(),
                    tool_type: ToolType::Function,
                    function: FunctionCall {
                        name: "image_gen".into(),
                        arguments: "{}".into(),
                    },
                },
            ]),
            tool_call_id: None,
            reasoning_content: None,
        }];
        let (count, names) = extract_tool_info(&req);
        assert_eq!(count, 2);
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"web_search".to_string()));
        assert!(names.contains(&"image_gen".to_string()));
    }

    #[test]
    fn test_extract_tool_info_deduplicates_names() {
        let mut req = empty_request();
        req.messages = vec![
            ChatMessage {
                role: MessageRole::Assistant,
                content: None,
                name: None,
                tool_calls: Some(vec![ToolCall {
                    index: None,
                    id: "tc1".into(),
                    tool_type: ToolType::Function,
                    function: FunctionCall {
                        name: "search".into(),
                        arguments: "{}".into(),
                    },
                }]),
                tool_call_id: None,
                reasoning_content: None,
            },
            ChatMessage {
                role: MessageRole::Tool,
                content: Some(MessageContent::Text("result".into())),
                name: None,
                tool_calls: None,
                tool_call_id: Some("tc1".into()),
                reasoning_content: None,
            },
            ChatMessage {
                role: MessageRole::Assistant,
                content: None,
                name: None,
                tool_calls: Some(vec![
                    ToolCall {
                        index: None,
                        id: "tc2".into(),
                        tool_type: ToolType::Function,
                        function: FunctionCall {
                            name: "search".into(),
                            arguments: "{}".into(),
                        },
                    },
                    ToolCall {
                        index: None,
                        id: "tc3".into(),
                        tool_type: ToolType::Function,
                        function: FunctionCall {
                            name: "fetch".into(),
                            arguments: "{}".into(),
                        },
                    },
                ]),
                tool_call_id: None,
                reasoning_content: None,
            },
        ];
        let (count, names) = extract_tool_info(&req);
        assert_eq!(count, 3, "Total tool calls should be 3");
        assert_eq!(
            names.len(),
            2,
            "Unique tool names should be 2 (search, fetch)"
        );
    }

    #[test]
    fn test_extract_tool_info_skips_empty_names() {
        let mut req = empty_request();
        req.messages = vec![ChatMessage {
            role: MessageRole::Assistant,
            content: None,
            name: None,
            tool_calls: Some(vec![
                ToolCall {
                    index: None,
                    id: "tc1".into(),
                    tool_type: ToolType::Function,
                    function: FunctionCall {
                        name: String::new(),
                        arguments: "{}".into(),
                    },
                },
                ToolCall {
                    index: None,
                    id: "tc2".into(),
                    tool_type: ToolType::Function,
                    function: FunctionCall {
                        name: "valid_tool".into(),
                        arguments: "{}".into(),
                    },
                },
            ]),
            tool_call_id: None,
            reasoning_content: None,
        }];
        let (count, names) = extract_tool_info(&req);
        assert_eq!(count, 2, "Count includes all tool calls regardless of name");
        assert_eq!(names.len(), 1, "Only non-empty names are collected");
        assert!(names.contains(&"valid_tool".to_string()));
    }

    #[test]
    fn test_build_llm_request_carries_tool_info() {
        let mut req = empty_request();
        req.messages = vec![ChatMessage {
            role: MessageRole::Assistant,
            content: None,
            name: None,
            tool_calls: Some(vec![ToolCall {
                index: None,
                id: "tc1".into(),
                tool_type: ToolType::Function,
                function: FunctionCall {
                    name: "my_tool".into(),
                    arguments: "{}".into(),
                },
            }]),
            tool_call_id: None,
            reasoning_content: None,
        }];
        let response = ChatCompletionResponse::new(
            "id".into(),
            "gpt-4o".into(),
            vec![Choice {
                index: 0,
                message: AssistantMessage {
                    role: MessageRole::Assistant,
                    content: Some("done".into()),
                    tool_calls: None,
                    thinking: None,
                },
                finish_reason: FinishReason::Stop,
                logprobs: None,
            }],
            Usage {
                prompt_tokens: 10,
                completion_tokens: 5,
                total_tokens: 15,
                thinking_tokens: None,
                completion_tokens_details: None,
                prompt_tokens_details: None,
            },
        );
        let llm_req = build_llm_request(LlmRequestParams {
            project_id: Uuid::new_v4(),
            request: &req,
            response: &response,
            provider: "openai",
            duration: Duration::from_millis(100),
            log_content: true,
            fallback_used: false,
            original_model: "gpt-4o".into(),
            retry_count: 0,
            guardrail_violations: vec![],
            is_platform_key: false,
        });
        assert_eq!(llm_req.tool_call_count, 1);
        assert!(llm_req.tool_names.contains(&"my_tool".to_string()));
    }

    #[test]
    fn test_build_error_llm_request_carries_tool_info() {
        let mut req = empty_request();
        req.messages = vec![ChatMessage {
            role: MessageRole::Assistant,
            content: None,
            name: None,
            tool_calls: Some(vec![ToolCall {
                index: None,
                id: "tc1".into(),
                tool_type: ToolType::Function,
                function: FunctionCall {
                    name: "fail_tool".into(),
                    arguments: "{}".into(),
                },
            }]),
            tool_call_id: None,
            reasoning_content: None,
        }];
        let llm_req = build_error_llm_request(ErrorLlmRequestParams {
            project_id: Uuid::new_v4(),
            request: &req,
            provider: "openai",
            duration: Duration::from_millis(50),
            error_type: "rate_limit",
            error_message: "too many requests",
            log_content: true,
            fallback_used: false,
            original_model: "gpt-4o".into(),
            retry_count: 1,
            guardrail_violations: vec![],
            is_platform_key: true,
        });
        assert_eq!(llm_req.tool_call_count, 1);
        assert!(llm_req.tool_names.contains(&"fail_tool".to_string()));
        assert_eq!(llm_req.status_code, "error");
    }

    #[test]
    fn test_build_llm_request_platform_key_true() {
        let req = empty_request();
        let response = ChatCompletionResponse::new(
            "id".into(),
            "gpt-4o".into(),
            vec![Choice {
                index: 0,
                message: AssistantMessage {
                    role: MessageRole::Assistant,
                    content: Some("ok".into()),
                    tool_calls: None,
                    thinking: None,
                },
                finish_reason: FinishReason::Stop,
                logprobs: None,
            }],
            Usage {
                prompt_tokens: 1,
                completion_tokens: 1,
                total_tokens: 2,
                thinking_tokens: None,
                completion_tokens_details: None,
                prompt_tokens_details: None,
            },
        );
        let llm_req = build_llm_request(LlmRequestParams {
            project_id: Uuid::new_v4(),
            request: &req,
            response: &response,
            provider: "openai",
            duration: Duration::from_millis(10),
            log_content: false,
            fallback_used: false,
            original_model: "gpt-4o".into(),
            retry_count: 0,
            guardrail_violations: vec![],
            is_platform_key: true,
        });
        assert!(llm_req.is_platform_key);
    }

    #[test]
    fn test_build_llm_request_platform_key_false() {
        let req = empty_request();
        let response = ChatCompletionResponse::new(
            "id".into(),
            "gpt-4o".into(),
            vec![Choice {
                index: 0,
                message: AssistantMessage {
                    role: MessageRole::Assistant,
                    content: Some("ok".into()),
                    tool_calls: None,
                    thinking: None,
                },
                finish_reason: FinishReason::Stop,
                logprobs: None,
            }],
            Usage {
                prompt_tokens: 1,
                completion_tokens: 1,
                total_tokens: 2,
                thinking_tokens: None,
                completion_tokens_details: None,
                prompt_tokens_details: None,
            },
        );
        let llm_req = build_llm_request(LlmRequestParams {
            project_id: Uuid::new_v4(),
            request: &req,
            response: &response,
            provider: "anthropic",
            duration: Duration::from_millis(10),
            log_content: false,
            fallback_used: false,
            original_model: "claude-3".into(),
            retry_count: 0,
            guardrail_violations: vec![],
            is_platform_key: false,
        });
        assert!(!llm_req.is_platform_key);
    }

    #[test]
    fn test_build_error_llm_request_platform_key() {
        let req = empty_request();
        let llm_req = build_error_llm_request(ErrorLlmRequestParams {
            project_id: Uuid::new_v4(),
            request: &req,
            provider: "openai",
            duration: Duration::from_millis(10),
            error_type: "timeout",
            error_message: "request timed out",
            log_content: false,
            fallback_used: false,
            original_model: "gpt-4o".into(),
            retry_count: 0,
            guardrail_violations: vec![],
            is_platform_key: true,
        });
        assert!(llm_req.is_platform_key);

        let llm_req_byok = build_error_llm_request(ErrorLlmRequestParams {
            project_id: Uuid::new_v4(),
            request: &req,
            provider: "anthropic",
            duration: Duration::from_millis(10),
            error_type: "timeout",
            error_message: "request timed out",
            log_content: false,
            fallback_used: false,
            original_model: "claude-3".into(),
            retry_count: 0,
            guardrail_violations: vec![],
            is_platform_key: false,
        });
        assert!(!llm_req_byok.is_platform_key);
    }

    #[test]
    fn test_build_streaming_llm_request_platform_key() {
        let req = empty_request();
        let llm_req = build_streaming_llm_request(StreamingLlmRequestParams {
            project_id: Uuid::new_v4(),
            request: &req,
            provider: "openai",
            model: "gpt-4o".into(),
            duration: Duration::from_millis(100),
            time_to_first_token_ms: 50,
            usage: Some(Usage {
                prompt_tokens: 10,
                completion_tokens: 5,
                total_tokens: 15,
                thinking_tokens: None,
                completion_tokens_details: None,
                prompt_tokens_details: None,
            }),
            log_content: false,
            fallback_used: false,
            original_model: "gpt-4o".into(),
            retry_count: 0,
            guardrail_violations: vec![],
            is_platform_key: true,
        });
        assert!(llm_req.is_platform_key);

        let llm_req_byok = build_streaming_llm_request(StreamingLlmRequestParams {
            project_id: Uuid::new_v4(),
            request: &req,
            provider: "anthropic",
            model: "claude-3".into(),
            duration: Duration::from_millis(100),
            time_to_first_token_ms: 50,
            usage: None,
            log_content: false,
            fallback_used: false,
            original_model: "claude-3".into(),
            retry_count: 0,
            guardrail_violations: vec![],
            is_platform_key: false,
        });
        assert!(!llm_req_byok.is_platform_key);
    }
}
