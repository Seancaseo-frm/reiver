//! Shared SSE (Server-Sent Events) parsing utilities for LLM providers.
//!
//! This module provides common functionality for parsing SSE streams from
//! LLM provider APIs. All major providers (OpenAI, Anthropic, Google, Bedrock)
//! use similar SSE formats for streaming responses.
//!
//! ## Utilities Provided
//!
//! - `bytes_to_sse_data_stream`: Converts raw bytes to SSE data payloads
//! - `parse_sse_json`: Type-safe JSON parsing for SSE events
//! - `map_finish_reason_to_openai`: Maps provider-specific stop reasons to OpenAI format

use bytes::Bytes;
use futures::stream::{Stream, StreamExt};
use tokio::io::AsyncBufReadExt;
use tokio_stream::wrappers::LinesStream;

use crate::gateway::error::GatewayError;
use crate::gateway::provider_types::Provider;
use crate::gateway::types::FinishReason;

/// Sentinel string sent by OpenAI-compatible APIs to signal end of SSE stream.
pub const SSE_DONE_SIGNAL: &str = "[DONE]";

/// Convert a bytes stream to a stream of SSE data lines.
///
/// This function handles the common pattern of:
/// 1. Converting a bytes stream to a buffered line reader
/// 2. Filtering for lines with "data: " prefix
/// 3. Stripping the prefix and handling [DONE] markers
///
/// # Arguments
/// * `byte_stream` - The HTTP response body as a bytes stream
///
/// # Returns
/// A stream of SSE data payloads (strings), with "data: " prefix removed.
/// Returns `None` for [DONE] markers and empty data.
pub fn bytes_to_sse_data_stream<S, E>(
    byte_stream: S,
) -> impl Stream<Item = Result<String, GatewayError>>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: std::error::Error + Send + Sync + 'static,
{
    // Convert bytes stream to lines
    let reader = tokio_util::io::StreamReader::new(
        byte_stream
            .map(|result| result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))),
    );
    let lines = tokio::io::BufReader::new(reader).lines();
    let lines_stream = LinesStream::new(lines);

    // Parse SSE format: filter for "data: " lines
    lines_stream.filter_map(|line_result| async move {
        match line_result {
            Ok(line) => {
                // SSE format: "data: {json}" or "data: [DONE]"
                if let Some(data) = line.strip_prefix("data: ") {
                    let data = data.trim();

                    if data == SSE_DONE_SIGNAL {
                        return None;
                    }

                    // Skip empty data
                    if data.is_empty() {
                        return None;
                    }

                    Some(Ok(data.to_string()))
                } else {
                    // Skip non-data lines (event:, id:, empty lines, etc.)
                    None
                }
            }
            Err(e) => Some(Err(GatewayError::InternalError(format!(
                "Stream read error: {}",
                e
            )))),
        }
    })
}

/// Parse a JSON string into a type, wrapping errors in GatewayError.
///
/// # Arguments
/// * `data` - JSON string to parse
/// * `context` - Context for error messages (e.g., "OpenAI chunk")
///
/// # Returns
/// Parsed value or GatewayError
pub fn parse_sse_json<T: serde::de::DeserializeOwned>(
    data: &str,
    context: &str,
) -> Result<T, GatewayError> {
    serde_json::from_str(data)
        .map_err(|e| GatewayError::InternalError(format!("Failed to parse {}: {}", context, e)))
}

/// Map a provider-specific finish/stop reason to OpenAI's standard format.
///
/// This is a common pattern across all providers when converting their
/// streaming responses to OpenAI-compatible format.
///
/// # Standard OpenAI finish reasons:
/// - `"stop"` - Natural end of generation or stop sequence hit
/// - `"length"` - Max tokens limit reached
/// - `"tool_calls"` - Model wants to call a tool/function
/// - `"content_filter"` - Content was filtered for safety
///
/// # Arguments
/// * `reason` - The provider-specific finish reason string
/// * `provider` - Provider name for logging unknown reasons
///
/// # Returns
/// The OpenAI-compatible finish reason string
pub fn map_finish_reason_to_openai(reason: &str, provider: Provider) -> FinishReason {
    match reason.to_lowercase().as_str() {
        "end_turn" | "stop" | "end" | "stop_sequence" => FinishReason::Stop,
        "max_tokens" | "length" => FinishReason::Length,
        "tool_use" | "tool_calls" | "function_call" => FinishReason::ToolCalls,
        "content_filter" | "content_filtered" | "safety" | "guardrail_intervened" => {
            FinishReason::ContentFilter
        }
        other => {
            tracing::debug!(
                provider = %provider,
                reason = %other,
                "Unknown finish reason, defaulting to Stop"
            );
            FinishReason::Stop
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_parse_sse_json_success() {
        #[derive(Debug, serde::Deserialize, PartialEq)]
        struct TestData {
            value: i32,
        }

        let result: Result<TestData, _> = parse_sse_json(r#"{"value": 42}"#, "test");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), TestData { value: 42 });
    }

    #[tokio::test]
    async fn test_parse_sse_json_error() {
        #[derive(Debug, serde::Deserialize)]
        #[allow(dead_code)]
        struct TestData {
            value: i32,
        }

        let result: Result<TestData, _> = parse_sse_json("not json", "test");
        assert!(result.is_err());
    }

    // Note: bytes_to_sse_data_stream is tested through integration with
    // the provider implementations (openai.rs, anthropic.rs, google.rs)
    // which use actual reqwest response streams.

    #[test]
    fn test_map_finish_reason_anthropic() {
        assert_eq!(
            map_finish_reason_to_openai("end_turn", Provider::Anthropic),
            FinishReason::Stop
        );
        assert_eq!(
            map_finish_reason_to_openai("max_tokens", Provider::Anthropic),
            FinishReason::Length
        );
        assert_eq!(
            map_finish_reason_to_openai("stop_sequence", Provider::Anthropic),
            FinishReason::Stop
        );
        assert_eq!(
            map_finish_reason_to_openai("tool_use", Provider::Anthropic),
            FinishReason::ToolCalls
        );
    }

    #[test]
    fn test_map_finish_reason_google() {
        assert_eq!(
            map_finish_reason_to_openai("STOP", Provider::Google),
            FinishReason::Stop
        );
        assert_eq!(
            map_finish_reason_to_openai("MAX_TOKENS", Provider::Google),
            FinishReason::Length
        );
        assert_eq!(
            map_finish_reason_to_openai("SAFETY", Provider::Google),
            FinishReason::ContentFilter
        );
    }

    #[test]
    fn test_map_finish_reason_bedrock() {
        assert_eq!(
            map_finish_reason_to_openai("end_turn", Provider::Bedrock),
            FinishReason::Stop
        );
        assert_eq!(
            map_finish_reason_to_openai("max_tokens", Provider::Bedrock),
            FinishReason::Length
        );
        assert_eq!(
            map_finish_reason_to_openai("content_filtered", Provider::Bedrock),
            FinishReason::ContentFilter
        );
        assert_eq!(
            map_finish_reason_to_openai("guardrail_intervened", Provider::Bedrock),
            FinishReason::ContentFilter
        );
    }

    #[test]
    fn test_map_finish_reason_unknown_defaults_to_stop() {
        assert_eq!(
            map_finish_reason_to_openai("some_unknown_reason", Provider::OpenAi),
            FinishReason::Stop
        );
        assert_eq!(
            map_finish_reason_to_openai("", Provider::OpenAi),
            FinishReason::Stop
        );
    }

    #[test]
    fn test_map_finish_reason_case_insensitive() {
        assert_eq!(
            map_finish_reason_to_openai("END_TURN", Provider::OpenAi),
            FinishReason::Stop
        );
        assert_eq!(
            map_finish_reason_to_openai("End_Turn", Provider::OpenAi),
            FinishReason::Stop
        );
        assert_eq!(
            map_finish_reason_to_openai("Tool_Use", Provider::OpenAi),
            FinishReason::ToolCalls
        );
    }
}
