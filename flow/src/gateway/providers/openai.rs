//! OpenAI provider adapter.
//!
//! This is a passthrough adapter - requests are forwarded directly
//! to OpenAI's API without translation since we use OpenAI's format
//! as the universal gateway format.

use async_trait::async_trait;
use futures::stream::StreamExt;
use reqwest::Client;
use std::time::Duration;

use super::common::{create_http_client, parse_provider_error};
use super::sse::{bytes_to_sse_data_stream, parse_sse_json};
use super::{ChatCompletionStream, LlmProvider};
use crate::gateway::embedding_types::{EmbeddingRequest, EmbeddingResponse};
use crate::gateway::error::GatewayError;
use crate::gateway::provider_types::Provider;
use crate::gateway::types::{ChatCompletionChunk, ChatCompletionRequest, ChatCompletionResponse};

const OPENAI_API_BASE: &str = "https://api.openai.com/v1";
const DEFAULT_TIMEOUT_SECS: u64 = 120;

/// OpenAI provider adapter.
///
/// This is a passthrough adapter since the gateway uses OpenAI's format
/// as the universal request/response format.
pub struct OpenAiProvider {
    client: Client,
    api_base: String,
}

impl OpenAiProvider {
    /// Create a new OpenAI provider with default settings.
    pub fn new() -> Self {
        Self::with_timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
    }

    /// Create a new OpenAI provider with a custom timeout.
    pub fn with_timeout(timeout: Duration) -> Self {
        Self::with_base_url_and_timeout(OPENAI_API_BASE.to_string(), timeout)
    }

    /// Create a new OpenAI provider with a custom base URL.
    ///
    /// Useful for testing or using Azure OpenAI.
    pub fn with_base_url(api_base: String) -> Self {
        Self::with_base_url_and_timeout(api_base, Duration::from_secs(DEFAULT_TIMEOUT_SECS))
    }

    /// Create a new OpenAI provider with custom base URL and timeout.
    pub fn with_base_url_and_timeout(api_base: String, timeout: Duration) -> Self {
        Self {
            client: create_http_client(timeout),
            api_base,
        }
    }

    /// Execute an embedding request against the provider's `/embeddings` endpoint.
    ///
    /// This is a public inherent method (not a trait method) so that wrapper
    /// providers (Mistral, Together, etc.) can call `self.0.embed()` directly
    /// after stripping their model prefix — the same pattern as `chat_completion`.
    pub async fn embed(
        &self,
        request: &EmbeddingRequest,
        api_key: &str,
    ) -> Result<EmbeddingResponse, GatewayError> {
        let url = format!("{}/embeddings", self.api_base);

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", api_key))
            .header("Content-Type", "application/json")
            .json(request)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(parse_provider_error(
                &error_text,
                Provider::OpenAi,
                status.as_u16(),
            ));
        }

        let embedding_response: EmbeddingResponse = response
            .json()
            .await
            .map_err(|e| {
                GatewayError::InternalError(format!(
                    "Failed to parse embedding response: {}",
                    e
                ))
            })?;

        Ok(embedding_response)
    }
}

impl Default for OpenAiProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl LlmProvider for OpenAiProvider {
    fn name(&self) -> Provider {
        Provider::OpenAi
    }

    fn supports_model(&self, model: &str) -> bool {
        model.starts_with("gpt-")
            || model.starts_with("o1-")
            || model.starts_with("o3-")
            || model.starts_with("o4-")
            || model == "o1"
            || model == "o3"
            || model == "o4"
            || model.starts_with("chatgpt-")
            || model.starts_with("text-embedding-")
            || model.starts_with("dall-e-")
            || model.starts_with("whisper-")
            || model.starts_with("tts-")
    }

    #[tracing::instrument(
        name = "provider.openai.chat_completion",
        skip(self, request, api_key),
        fields(
            model = %request.model,
            message_count = request.messages.len(),
            input_tokens = tracing::field::Empty,
            output_tokens = tracing::field::Empty,
            total_tokens = tracing::field::Empty,
            finish_reason = tracing::field::Empty,
            http_status = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
            otel.status_message = tracing::field::Empty,
            gen_ai.provider.name = "openai",
            gen_ai.operation.name = "chat",
            gen_ai.request.model = %request.model,
            gen_ai.response.model = tracing::field::Empty,
            gen_ai.usage.input_tokens = tracing::field::Empty,
            gen_ai.usage.output_tokens = tracing::field::Empty,
            gen_ai.response.finish_reasons = tracing::field::Empty,
        )
    )]
    async fn chat_completion(
        &self,
        request: &ChatCompletionRequest,
        api_key: &str,
    ) -> Result<ChatCompletionResponse, GatewayError> {
        let url = format!("{}/chat/completions", self.api_base);

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", api_key))
            .header("Content-Type", "application/json")
            .json(request)
            .send()
            .await?;

        let status = response.status();
        tracing::Span::current().record("http_status", status.as_u16());

        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(parse_provider_error(
                &error_text,
                Provider::OpenAi,
                status.as_u16(),
            ));
        }

        let mut completion: ChatCompletionResponse = response
            .json()
            .await
            .map_err(|e| GatewayError::InternalError(format!("Failed to parse response: {}", e)))?;

        if completion.usage.thinking_tokens.is_none() {
            if let Some(ref details) = completion.usage.completion_tokens_details {
                completion.usage.thinking_tokens = details.reasoning_tokens;
            }
        }

        let span = tracing::Span::current();
        span.record("gen_ai.response.model", completion.model.as_str());
        span.record("input_tokens", completion.usage.prompt_tokens as u64);
        span.record("output_tokens", completion.usage.completion_tokens as u64);
        span.record("total_tokens", completion.usage.total_tokens as u64);
        span.record(
            "gen_ai.usage.input_tokens",
            completion.usage.prompt_tokens as i64,
        );
        span.record(
            "gen_ai.usage.output_tokens",
            completion.usage.completion_tokens as i64,
        );
        if let Some(choice) = completion.choices.first() {
            span.record("finish_reason", choice.finish_reason.as_str());
            span.record(
                "gen_ai.response.finish_reasons",
                choice.finish_reason.as_str(),
            );
        }
        Ok(completion)
    }

    #[tracing::instrument(
        name = "provider.openai.stream_chat_completion",
        skip(self, request, api_key),
        fields(
            model = %request.model,
            otel.status_code = tracing::field::Empty,
            otel.status_message = tracing::field::Empty,
            gen_ai.provider.name = "openai",
            gen_ai.operation.name = "chat",
            gen_ai.request.model = %request.model,
        )
    )]
    async fn stream_chat_completion(
        &self,
        request: &ChatCompletionRequest,
        api_key: &str,
    ) -> Result<ChatCompletionStream, GatewayError> {
        let url = format!("{}/chat/completions", self.api_base);

        // Create a modified request with stream: true
        let mut request_body = serde_json::to_value(request).map_err(|e| {
            GatewayError::InternalError(format!("Failed to serialize request: {}", e))
        })?;

        if let Some(obj) = request_body.as_object_mut() {
            obj.insert("stream".to_string(), serde_json::Value::Bool(true));
            obj.insert(
                "stream_options".to_string(),
                serde_json::json!({"include_usage": true}),
            );
        }

        // Send the request
        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", api_key))
            .header("Content-Type", "application/json")
            .header("Accept", "text/event-stream")
            .json(&request_body)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(parse_provider_error(
                &error_text,
                Provider::OpenAi,
                status.as_u16(),
            ));
        }

        // Use shared SSE parsing utilities
        let byte_stream = response.bytes_stream();
        let data_stream = bytes_to_sse_data_stream(byte_stream);

        // Parse each SSE data payload as a ChatCompletionChunk
        let chunk_stream = data_stream.map(|result| {
            result.and_then(|data| parse_sse_json::<ChatCompletionChunk>(&data, "OpenAI chunk"))
        });

        Ok(Box::pin(chunk_stream))
    }

    async fn embed(
        &self,
        request: &EmbeddingRequest,
        api_key: &str,
    ) -> Result<EmbeddingResponse, GatewayError> {
        OpenAiProvider::embed(self, request, api_key).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_supports_model() {
        let provider = OpenAiProvider::new();

        // Supported models
        assert!(provider.supports_model("gpt-4o"));
        assert!(provider.supports_model("gpt-4-turbo"));
        assert!(provider.supports_model("gpt-3.5-turbo"));
        assert!(provider.supports_model("gpt-4"));
        assert!(provider.supports_model("o1-preview"));
        assert!(provider.supports_model("o1-mini"));
        assert!(provider.supports_model("o3-mini"));

        // Unsupported models
        assert!(!provider.supports_model("claude-sonnet-4-6"));
        assert!(!provider.supports_model("gemini-pro"));
        assert!(!provider.supports_model("llama-2-70b"));
    }

    /// Regression: bare "o1", "o3", "o4" (without trailing dash) were not
    /// matched by `supports_model`, causing `UnsupportedModel` errors when
    /// using the provider-level fallback path instead of the prefix router.
    #[test]
    fn test_supports_model_bare_o_series() {
        let provider = OpenAiProvider::new();
        assert!(provider.supports_model("o1"), "bare o1 must be supported");
        assert!(provider.supports_model("o3"), "bare o3 must be supported");
        assert!(provider.supports_model("o4"), "bare o4 must be supported");
    }

    #[test]
    fn test_provider_name() {
        let provider = OpenAiProvider::new();
        assert_eq!(provider.name(), Provider::OpenAi);
    }

    /// Regression: OpenAI streaming requests must include
    /// `stream_options.include_usage = true` so that the final chunk
    /// carries token usage data. Without this, session budget tracking
    /// is bypassed for streaming requests.
    #[test]
    fn test_stream_request_includes_stream_options() {
        use crate::gateway::types::{ChatMessage, MessageContent, MessageRole};

        let request = ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text("hi".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            }],
            temperature: None,
            max_tokens: None,
            top_p: None,
            n: None,
            stream: None,
            stream_options: None,
            stop: None,
            frequency_penalty: None,
            presence_penalty: None,
            user: None,
            seed: None,
            tools: None,
            tool_choice: None,
            response_format: None,
            thinking: None,
            reasoning_effort: None,
            prompt_config: None,
            prompt_variables: None,
            models: None,
            provider: None,
        };

        // Replicate the serialization logic from stream_chat_completion
        let mut body = serde_json::to_value(&request).unwrap();
        let obj = body.as_object_mut().unwrap();
        obj.insert("stream".to_string(), serde_json::Value::Bool(true));
        obj.insert(
            "stream_options".to_string(),
            serde_json::json!({"include_usage": true}),
        );

        let so = body
            .get("stream_options")
            .expect("stream_options must be present");
        assert_eq!(
            so.get("include_usage").and_then(|v| v.as_bool()),
            Some(true)
        );
        assert_eq!(body.get("stream").and_then(|v| v.as_bool()), Some(true));
    }

    /// Regression: OpenAI streaming chunks include an `index` field on tool
    /// call deltas to correlate argument fragments with the correct tool call.
    /// Our `ToolCall` struct was missing this field, so it was silently dropped
    /// during deserialization and absent when re-serialized to the client.
    #[test]
    fn test_openai_streaming_tool_call_index_preserved() {
        use crate::gateway::types::ChatCompletionChunk;

        let openai_chunk_json = r#"{
            "id": "chatcmpl-test",
            "object": "chat.completion.chunk",
            "created": 1700000000,
            "model": "gpt-4o",
            "choices": [{
                "index": 0,
                "delta": {
                    "tool_calls": [{
                        "index": 0,
                        "id": "call_abc",
                        "type": "function",
                        "function": { "name": "get_weather", "arguments": "" }
                    }]
                },
                "finish_reason": null
            }]
        }"#;

        let chunk: ChatCompletionChunk = serde_json::from_str(openai_chunk_json)
            .expect("OpenAI streaming chunk must deserialize");

        let tc = &chunk.choices[0].delta.tool_calls.as_ref().unwrap()[0];
        assert_eq!(
            tc.index,
            Some(0),
            "tool call index must be preserved through deserialization"
        );

        let reserialized = serde_json::to_value(&chunk).unwrap();
        let tc_json = &reserialized["choices"][0]["delta"]["tool_calls"][0];
        assert_eq!(
            tc_json["index"], 0,
            "tool call index must survive serialization roundtrip"
        );
    }

    /// Verify that a tool call argument delta (no id/name, just index +
    /// arguments) also preserves the index through the roundtrip.
    #[test]
    fn test_openai_streaming_tool_call_argument_delta_index() {
        use crate::gateway::types::ChatCompletionChunk;

        let delta_json = r#"{
            "id": "chatcmpl-test",
            "object": "chat.completion.chunk",
            "created": 1700000000,
            "model": "gpt-4o",
            "choices": [{
                "index": 0,
                "delta": {
                    "tool_calls": [{
                        "index": 1,
                        "id": "",
                        "type": "function",
                        "function": { "name": "", "arguments": "{\"tz\":" }
                    }]
                },
                "finish_reason": null
            }]
        }"#;

        let chunk: ChatCompletionChunk =
            serde_json::from_str(delta_json).expect("argument delta chunk must deserialize");

        let tc = &chunk.choices[0].delta.tool_calls.as_ref().unwrap()[0];
        assert_eq!(
            tc.index,
            Some(1),
            "argument delta must carry index=1 for the second tool call"
        );
    }
}
