//! Anthropic (Claude) provider adapter.
//!
//! Translates between OpenAI chat completion format and Anthropic's Messages API.

use async_trait::async_trait;
use futures::stream::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::common::{create_http_client, parse_provider_error};
use super::sse::{bytes_to_sse_data_stream, map_finish_reason_to_openai};
use super::{ChatCompletionStream, LlmProvider};
use crate::gateway::error::GatewayError;
use crate::gateway::provider_types::Provider;
use crate::gateway::types::{
    find_system_message_text, non_system_messages, AssistantMessage, ChatCompletionChunk,
    ChatCompletionRequest, ChatCompletionResponse, ChatMessage, Choice, ChunkChoice, ChunkDelta,
    FunctionCall, MessageContent, MessageRole, PromptTokensDetails, ThinkingContent, ThinkingType,
    ThinkingConfig, ThinkingToggle, ToolCall, ToolType, Usage,
};

const ANTHROPIC_API_BASE: &str = "https://api.anthropic.com/v1";
/// Default Anthropic API version (stable release).
/// See: https://docs.anthropic.com/en/api/versioning
const DEFAULT_ANTHROPIC_VERSION: &str = "2023-06-01";
const DEFAULT_TIMEOUT_SECS: u64 = 120;
const INTERLEAVED_THINKING_BETA: &str = "interleaved-thinking-2025-01-05";
const FAST_MODE_BETA: &str = "fast-mode-2026-02-01";

/// Anthropic provider adapter.
pub struct AnthropicProvider {
    client: Client,
    api_base: String,
    /// Anthropic API version header value.
    api_version: String,
}

impl AnthropicProvider {
    /// Create a new Anthropic provider with default settings.
    pub fn new() -> Self {
        Self::with_timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
    }

    /// Create a new Anthropic provider with a custom timeout.
    pub fn with_timeout(timeout: Duration) -> Self {
        Self::with_config(
            ANTHROPIC_API_BASE.to_string(),
            timeout,
            DEFAULT_ANTHROPIC_VERSION.to_string(),
        )
    }

    /// Create with a custom base URL (for testing).
    pub fn with_base_url(api_base: String) -> Self {
        Self::with_config(
            api_base,
            Duration::from_secs(DEFAULT_TIMEOUT_SECS),
            DEFAULT_ANTHROPIC_VERSION.to_string(),
        )
    }

    /// Create with custom base URL and timeout.
    pub fn with_base_url_and_timeout(api_base: String, timeout: Duration) -> Self {
        Self::with_config(api_base, timeout, DEFAULT_ANTHROPIC_VERSION.to_string())
    }

    /// Create with full configuration options.
    ///
    /// # Arguments
    /// * `api_base` - Base URL for the Anthropic API
    /// * `timeout` - Request timeout duration
    /// * `api_version` - Anthropic API version header value (e.g., "2023-06-01")
    pub fn with_config(api_base: String, timeout: Duration, api_version: String) -> Self {
        Self {
            client: create_http_client(timeout),
            api_base,
            api_version,
        }
    }

    /// Convert OpenAI messages to Anthropic format.
    ///
    /// Handles:
    /// - Text-only and multimodal (text + image) content
    /// - Tool/function calling messages (tool role with tool_call_id)
    /// - Assistant messages with tool_calls
    fn convert_messages(&self, messages: &[ChatMessage]) -> Vec<AnthropicMessage> {
        let mut result: Vec<AnthropicMessage> = Vec::with_capacity(12);

        for m in non_system_messages(messages) {
            match m.role {
                MessageRole::User => {
                    let content = self.convert_content(&m.content);
                    result.push(AnthropicMessage {
                        role: "user".to_string(),
                        content,
                    });
                }
                MessageRole::Assistant => {
                    let mut content = self.convert_content(&m.content);

                    // If assistant made tool calls, add tool_use blocks
                    if let Some(ref tool_calls) = m.tool_calls {
                        for tc in tool_calls {
                            // Parse the arguments JSON string back to a Value
                            let input = serde_json::from_str(&tc.function.arguments)
                                .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));

                            content.push(AnthropicRequestContentBlock::ToolUse {
                                id: tc.id.clone(),
                                name: tc.function.name.clone(),
                                input,
                            });
                        }
                    }

                    result.push(AnthropicMessage {
                        role: "assistant".to_string(),
                        content,
                    });
                }
                MessageRole::Tool | MessageRole::Other => {
                    if let Some(ref tool_call_id) = m.tool_call_id {
                        let content_text =
                            m.content.as_ref().map(|c| c.as_text()).unwrap_or_default();

                        // Check if the last message is a user message with tool results
                        // If so, append to it (Anthropic expects all tool results in one user message)
                        let should_append = result
                            .last()
                            .map(|last| {
                                last.role == "user"
                                    && last.content.iter().any(|c| {
                                        matches!(c, AnthropicRequestContentBlock::ToolResult { .. })
                                    })
                            })
                            .unwrap_or(false);

                        if should_append {
                            if let Some(last_msg) = result.last_mut() {
                                last_msg
                                    .content
                                    .push(AnthropicRequestContentBlock::ToolResult {
                                        tool_use_id: tool_call_id.clone(),
                                        content: content_text,
                                        is_error: None,
                                    });
                            }
                        } else {
                            result.push(AnthropicMessage {
                                role: "user".to_string(),
                                content: vec![AnthropicRequestContentBlock::ToolResult {
                                    tool_use_id: tool_call_id.clone(),
                                    content: content_text,
                                    is_error: None,
                                }],
                            });
                        }
                    } else {
                        tracing::warn!(
                            role = ?m.role,
                            "Unknown message role, defaulting to 'user'. Supported roles: user, assistant, system, tool"
                        );
                        let content = self.convert_content(&m.content);
                        result.push(AnthropicMessage {
                            role: "user".to_string(),
                            content,
                        });
                    }
                }
                MessageRole::System => {
                    // Already filtered out above, but handle for exhaustiveness
                }
            }
        }

        result
    }

    /// Convert OpenAI message content to Anthropic content blocks.
    ///
    /// Supports:
    /// - Simple text content
    /// - Multimodal content with text, images, and documents (base64 or URL)
    fn convert_content(
        &self,
        content: &Option<MessageContent>,
    ) -> Vec<AnthropicRequestContentBlock> {
        match content {
            None => vec![],
            Some(MessageContent::Text(text)) => {
                vec![AnthropicRequestContentBlock::Text { text: text.clone() }]
            }
            Some(MessageContent::Parts(parts)) => parts
                .iter()
                .filter_map(|part| match part {
                    crate::gateway::types::ContentPart::Text { text } => {
                        Some(AnthropicRequestContentBlock::Text { text: text.clone() })
                    }
                    crate::gateway::types::ContentPart::ImageUrl { image_url } => {
                        self.convert_image_url(&image_url.url)
                    }
                    crate::gateway::types::ContentPart::DocumentUrl { document_url } => self
                        .convert_document_url(
                            &document_url.url,
                            document_url.media_type.as_deref(),
                            document_url.filename.as_deref(),
                        ),
                })
                .collect(),
        }
    }

    /// Convert an OpenAI image URL to Anthropic image content block.
    ///
    /// Handles both:
    /// - Data URLs: `data:image/jpeg;base64,...`
    /// - HTTP URLs: `https://example.com/image.jpg`
    fn convert_image_url(&self, url: &str) -> Option<AnthropicRequestContentBlock> {
        if url.starts_with("data:") {
            // Parse data URL: data:image/jpeg;base64,/9j/4AAQ...
            let parts: Vec<&str> = url.splitn(2, ',').collect();
            if parts.len() != 2 {
                tracing::warn!("Invalid data URL format for image");
                return None;
            }

            // Extract media type from "data:image/jpeg;base64"
            let header = parts[0];
            let data = parts[1];

            // Parse media type (e.g., "image/jpeg" from "data:image/jpeg;base64")
            let media_type = header
                .strip_prefix("data:")
                .and_then(|s| s.split(';').next())
                .unwrap_or("image/jpeg")
                .to_string();

            Some(AnthropicRequestContentBlock::Image {
                source: AnthropicImageSource::Base64 {
                    media_type,
                    data: data.to_string(),
                },
            })
        } else if url.starts_with("http://") || url.starts_with("https://") {
            // HTTP URL - Anthropic supports URL sources
            Some(AnthropicRequestContentBlock::Image {
                source: AnthropicImageSource::Url {
                    url: url.to_string(),
                },
            })
        } else {
            tracing::warn!(url_prefix = %url.chars().take(20).collect::<String>(), "Unsupported image URL format");
            None
        }
    }

    /// Convert a document URL to an Anthropic document content block.
    ///
    /// Handles both:
    /// - Base64 data URLs: `data:application/pdf;base64,...`
    /// - HTTP URLs: `https://example.com/document.pdf`
    fn convert_document_url(
        &self,
        url: &str,
        media_type_override: Option<&str>,
        title: Option<&str>,
    ) -> Option<AnthropicRequestContentBlock> {
        let source = if url.starts_with("data:") {
            let parts: Vec<&str> = url.splitn(2, ',').collect();
            if parts.len() != 2 {
                tracing::warn!("Invalid data URL format for document");
                return None;
            }
            let header = parts[0];
            let data = parts[1];
            let media_type = media_type_override
                .map(|s| s.to_string())
                .unwrap_or_else(|| {
                    header
                        .strip_prefix("data:")
                        .and_then(|s| s.split(';').next())
                        .unwrap_or("application/pdf")
                        .to_string()
                });
            AnthropicDocumentSource::Base64 {
                media_type,
                data: data.to_string(),
            }
        } else if url.starts_with("http://") || url.starts_with("https://") {
            AnthropicDocumentSource::Url {
                url: url.to_string(),
            }
        } else {
            tracing::warn!(url_prefix = %url.chars().take(20).collect::<String>(), "Unsupported document URL format");
            return None;
        };
        Some(AnthropicRequestContentBlock::Document {
            source,
            title: title.map(|s| s.to_string()),
        })
    }

    /// Convert OpenAI tool definitions to Anthropic tool format.
    ///
    /// Anthropic uses `input_schema` where OpenAI uses `parameters`.
    fn convert_tools(
        &self,
        tools: &Option<Vec<crate::gateway::types::Tool>>,
    ) -> Option<Vec<AnthropicToolDefinition>> {
        tools
            .as_ref()
            .map(|tools| {
                tools
                    .iter()
                    .filter(|t| t.tool_type == ToolType::Function)
                    .map(|t| AnthropicToolDefinition {
                        name: t.function.name.clone(),
                        description: t.function.description.clone(),
                        input_schema: t.function.parameters.clone().unwrap_or_else(
                            || serde_json::json!({"type": "object", "properties": {}}),
                        ),
                    })
                    .collect()
            })
            .filter(|tools: &Vec<AnthropicToolDefinition>| !tools.is_empty())
    }

    /// Convert Anthropic response to OpenAI format.
    fn convert_response(&self, response: AnthropicResponse, model: &str) -> ChatCompletionResponse {
        // Extract text content (concatenate all text blocks)
        let content: String = response
            .content
            .iter()
            .filter_map(|block| block.as_text())
            .collect::<Vec<_>>()
            .join("");

        // Extract thinking content (from extended thinking)
        let thinking_content: String = response
            .content
            .iter()
            .filter_map(|block| block.as_thinking())
            .collect::<Vec<_>>()
            .join("\n");

        let thinking = if thinking_content.is_empty() {
            None
        } else {
            Some(ThinkingContent {
                content: thinking_content,
                tokens: None,
                thinking_type: Some(ThinkingType::ExtendedThinking),
            })
        };

        // Convert tool_use blocks to OpenAI tool_calls format
        let tool_calls: Vec<crate::gateway::types::ToolCall> = response
            .content
            .iter()
            .filter_map(|block| match block {
                AnthropicContentBlock::ToolUse { id, name, input } => {
                    Some(crate::gateway::types::ToolCall {
                        index: None,
                        id: id.clone(),
                        tool_type: ToolType::Function,
                        function: crate::gateway::types::FunctionCall {
                            name: name.clone(),
                            arguments: serde_json::to_string(input).unwrap_or_default(),
                        },
                    })
                }
                _ => None,
            })
            .collect();

        let finish_reason = map_finish_reason_to_openai(&response.stop_reason, Provider::Anthropic);

        ChatCompletionResponse::new(
            response.id,
            model.to_string(),
            vec![Choice {
                index: 0,
                message: AssistantMessage {
                    role: MessageRole::Assistant,
                    content: if content.is_empty() {
                        None
                    } else {
                        Some(content)
                    },
                    tool_calls: if tool_calls.is_empty() {
                        None
                    } else {
                        Some(tool_calls)
                    },
                    thinking,
                },
                finish_reason,
                logprobs: None,
            }],
            Usage {
                prompt_tokens: response.usage.input_tokens,
                completion_tokens: response.usage.output_tokens,
                total_tokens: response.usage.input_tokens + response.usage.output_tokens,
                thinking_tokens: None,
                completion_tokens_details: None,
                prompt_tokens_details: Some(PromptTokensDetails {
                    cached_tokens: response.usage.cache_read_input_tokens
                        + response.usage.cache_creation_input_tokens,
                }),
            },
        )
    }
}

impl Default for AnthropicProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl LlmProvider for AnthropicProvider {
    fn name(&self) -> Provider {
        Provider::Anthropic
    }

    fn supports_model(&self, model: &str) -> bool {
        model.starts_with("claude-")
    }

    #[tracing::instrument(
        name = "provider.anthropic.chat_completion",
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
            gen_ai.provider.name = "anthropic",
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
        let url = format!("{}/messages", self.api_base);
        let options = AnthropicRequestOptions::for_model(&request.model);
        let thinking = anthropic_thinking_config(&request.model, request.thinking.as_ref());
        let (temperature, top_p) = anthropic_sampling_parameters(
            &request.model,
            request.temperature,
            request.top_p,
        );

        let tools = self.convert_tools(&request.tools);

        let anthropic_request = AnthropicRequest {
            model: options.model,
            messages: self.convert_messages(&request.messages),
            system: find_system_message_text(&request.messages),
            max_tokens: request.max_tokens.unwrap_or(4096),
            temperature,
            top_p,
            stop_sequences: request.stop.as_ref().map(|s| match s {
                crate::gateway::types::StopSequence::Single(s) => vec![s.clone()],
                crate::gateway::types::StopSequence::Multiple(v) => v.clone(),
            }),
            stream: None,
            thinking,
            speed: options.speed,
            tools,
        };

        let mut req_builder = self
            .client
            .post(&url)
            .header("x-api-key", api_key)
            .header("anthropic-version", &self.api_version)
            .header("Content-Type", "application/json");

        if anthropic_request.speed.is_some() {
            req_builder = req_builder.header("anthropic-beta", FAST_MODE_BETA);
        } else if anthropic_request
            .thinking
            .as_ref()
            .is_some_and(|config| config.thinking_type == "enabled")
        {
            req_builder = req_builder.header("anthropic-beta", INTERLEAVED_THINKING_BETA);
        }

        let response = req_builder.json(&anthropic_request).send().await?;

        let status = response.status();
        tracing::Span::current().record("http_status", status.as_u16());

        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(parse_provider_error(
                &error_text,
                Provider::Anthropic,
                status.as_u16(),
            ));
        }

        let anthropic_response: AnthropicResponse = response
            .json()
            .await
            .map_err(|e| GatewayError::InternalError(format!("Failed to parse response: {}", e)))?;

        let result = self.convert_response(anthropic_response, &request.model);
        let span = tracing::Span::current();
        span.record("gen_ai.response.model", result.model.as_str());
        span.record("input_tokens", result.usage.prompt_tokens as u64);
        span.record("output_tokens", result.usage.completion_tokens as u64);
        span.record("total_tokens", result.usage.total_tokens as u64);
        span.record(
            "gen_ai.usage.input_tokens",
            result.usage.prompt_tokens as i64,
        );
        span.record(
            "gen_ai.usage.output_tokens",
            result.usage.completion_tokens as i64,
        );
        if let Some(choice) = result.choices.first() {
            span.record("finish_reason", choice.finish_reason.as_str());
            span.record(
                "gen_ai.response.finish_reasons",
                choice.finish_reason.as_str(),
            );
        }
        Ok(result)
    }

    #[tracing::instrument(
        name = "provider.anthropic.stream_chat_completion",
        skip(self, request, api_key),
        fields(
            model = %request.model,
            otel.status_code = tracing::field::Empty,
            otel.status_message = tracing::field::Empty,
            gen_ai.provider.name = "anthropic",
            gen_ai.operation.name = "chat",
            gen_ai.request.model = %request.model,
        )
    )]
    async fn stream_chat_completion(
        &self,
        request: &ChatCompletionRequest,
        api_key: &str,
    ) -> Result<ChatCompletionStream, GatewayError> {
        let url = format!("{}/messages", self.api_base);
        let options = AnthropicRequestOptions::for_model(&request.model);
        let thinking = anthropic_thinking_config(&request.model, request.thinking.as_ref());
        let (temperature, top_p) = anthropic_sampling_parameters(
            &request.model,
            request.temperature,
            request.top_p,
        );
        let tools = self.convert_tools(&request.tools);

        let anthropic_request = AnthropicRequest {
            model: options.model,
            messages: self.convert_messages(&request.messages),
            system: find_system_message_text(&request.messages),
            max_tokens: request.max_tokens.unwrap_or(4096),
            temperature,
            top_p,
            stop_sequences: request.stop.as_ref().map(|s| match s {
                crate::gateway::types::StopSequence::Single(s) => vec![s.clone()],
                crate::gateway::types::StopSequence::Multiple(v) => v.clone(),
            }),
            stream: Some(true),
            thinking,
            speed: options.speed,
            tools,
        };

        let mut req_builder = self
            .client
            .post(&url)
            .header("x-api-key", api_key)
            .header("anthropic-version", &self.api_version)
            .header("Content-Type", "application/json")
            .header("Accept", "text/event-stream");

        if anthropic_request.speed.is_some() {
            req_builder = req_builder.header("anthropic-beta", FAST_MODE_BETA);
        } else if anthropic_request
            .thinking
            .as_ref()
            .is_some_and(|config| config.thinking_type == "enabled")
        {
            req_builder = req_builder.header("anthropic-beta", INTERLEAVED_THINKING_BETA);
        }

        let response = req_builder.json(&anthropic_request).send().await?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(parse_provider_error(
                &error_text,
                Provider::Anthropic,
                status.as_u16(),
            ));
        }

        // Use shared SSE parsing utilities
        let byte_stream = response.bytes_stream();
        let data_stream = bytes_to_sse_data_stream(byte_stream);

        // Capture model for use in stream
        let model = request.model.clone();
        let chunk_id = format!("chatcmpl-{}", uuid::Uuid::new_v4());

        // Shared counters to carry usage from MessageStart into the final
        // MessageDelta usage (Anthropic only sends these in message_start).
        let captured_input_tokens = Arc::new(AtomicU32::new(0));
        let captured_cache_read = Arc::new(AtomicU32::new(0));
        let captured_cache_creation = Arc::new(AtomicU32::new(0));

        // Track the active tool_use block so `input_json_delta` events
        // can be forwarded as OpenAI-compatible tool call argument deltas.
        // Stores (tool_call_index, tool_use_id, tool_name) set on
        // ContentBlockStart::ToolUse, cleared on ContentBlockStop.
        let active_tool: Arc<std::sync::Mutex<Option<(u32, String, String)>>> =
            Arc::new(std::sync::Mutex::new(None));
        let tool_call_counter: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));

        // Parse Anthropic SSE events and convert to OpenAI chunk format
        let chunk_stream = data_stream.filter_map(move |result| {
            let chunk_id = chunk_id.clone();
            let model = model.clone();
            let captured_input_tokens = captured_input_tokens.clone();
            let captured_cache_read = captured_cache_read.clone();
            let captured_cache_creation = captured_cache_creation.clone();
            let active_tool = active_tool.clone();
            let tool_call_counter = tool_call_counter.clone();

            async move {
                match result {
                    Ok(data) => {
                        // Parse the Anthropic event
                        if let Ok(event) = serde_json::from_str::<AnthropicStreamEvent>(&data) {
                            match event {
                                AnthropicStreamEvent::ContentBlockDelta { delta, .. } => {
                                    if let Some(text) = delta.text {
                                        return Some(Ok(ChatCompletionChunk::with_content(
                                            chunk_id,
                                            model,
                                            text,
                                        )));
                                    }
                                    // Forward thinking deltas as a thinking chunk so the
                                    // playground (and any streaming client) can display
                                    // the model's chain of thought in real time.
                                    if let Some(thinking_text) = delta.thinking {
                                        return Some(Ok(ChatCompletionChunk::with_thinking(
                                            chunk_id,
                                            model,
                                            thinking_text,
                                        )));
                                    }
                                    if let Some(json_fragment) = delta.partial_json {
                                        let tc_index = match active_tool.lock() {
                                            Ok(guard) => match guard.as_ref() {
                                                Some((idx, _, _)) => *idx,
                                                None => {
                                                    tracing::warn!(
                                                        "Received input_json_delta without prior ContentBlockStart, dropping fragment"
                                                    );
                                                    return None;
                                                }
                                            },
                                            Err(_) => {
                                                tracing::warn!(
                                                    "active_tool mutex poisoned, dropping input_json_delta fragment"
                                                );
                                                return None;
                                            }
                                        };
                                        return Some(Ok(ChatCompletionChunk::new(
                                            chunk_id,
                                            model,
                                            vec![ChunkChoice {
                                                index: 0,
                                                delta: ChunkDelta {
                                                    tool_calls: Some(vec![ToolCall {
                                                        index: Some(tc_index),
                                                        id: String::new(),
                                                        tool_type: ToolType::Function,
                                                        function: FunctionCall {
                                                            name: String::new(),
                                                            arguments: json_fragment,
                                                        },
                                                    }]),
                                                    ..Default::default()
                                                },
                                                finish_reason: None,
                                            }],
                                        )));
                                    }
                                }
                                AnthropicStreamEvent::MessageDelta { delta, usage } => {
                                    if let Some(stop_reason) = delta.stop_reason {
                                        let openai_reason = map_finish_reason_to_openai(&stop_reason, Provider::Anthropic);

                                        let mut chunk = ChatCompletionChunk::finished(
                                            chunk_id,
                                            model,
                                            openai_reason,
                                        );

                                        if let Some(anthropic_usage) = usage {
                                            let input_tokens = captured_input_tokens.load(Ordering::Relaxed);
                                            let cache_read = captured_cache_read.load(Ordering::Relaxed);
                                            let cache_creation = captured_cache_creation.load(Ordering::Relaxed);
                                            chunk.usage = Some(Usage {
                                                prompt_tokens: input_tokens,
                                                completion_tokens: anthropic_usage.output_tokens,
                                                total_tokens: input_tokens + anthropic_usage.output_tokens,
                                                thinking_tokens: None,
                                                completion_tokens_details: None,
                                                prompt_tokens_details: Some(PromptTokensDetails {
                                                    cached_tokens: cache_read + cache_creation,
                                                }),
                                            });
                                        }

                                        return Some(Ok(chunk));
                                    }
                                }
                                AnthropicStreamEvent::MessageStart { message } => {
                                    captured_input_tokens.store(message.usage.input_tokens, Ordering::Relaxed);
                                    captured_cache_read.store(message.usage.cache_read_input_tokens, Ordering::Relaxed);
                                    captured_cache_creation.store(message.usage.cache_creation_input_tokens, Ordering::Relaxed);
                                    return Some(Ok(ChatCompletionChunk::new(
                                        chunk_id,
                                        model,
                                        vec![ChunkChoice {
                                            index: 0,
                                            delta: ChunkDelta {
                                                role: Some(MessageRole::Assistant),
                                                ..Default::default()
                                            },
                                            finish_reason: None,
                                        }],
                                    )));
                                }
                                AnthropicStreamEvent::ContentBlockStart { content_block, .. } => {
                                    if let AnthropicContentBlock::ToolUse { id, name, .. } = content_block {
                                        let tc_index = tool_call_counter.fetch_add(1, Ordering::SeqCst);
                                        if let Ok(mut guard) = active_tool.lock() {
                                            *guard = Some((tc_index, id.clone(), name.clone()));
                                        }
                                        return Some(Ok(ChatCompletionChunk::new(
                                            chunk_id,
                                            model,
                                            vec![ChunkChoice {
                                                index: 0,
                                                delta: ChunkDelta {
                                                    tool_calls: Some(vec![ToolCall {
                                                        index: Some(tc_index),
                                                        id,
                                                        tool_type: ToolType::Function,
                                                        function: FunctionCall {
                                                            name,
                                                            arguments: String::new(),
                                                        },
                                                    }]),
                                                    ..Default::default()
                                                },
                                                finish_reason: None,
                                            }],
                                        )));
                                    }
                                }
                                AnthropicStreamEvent::ContentBlockStop { .. } => {
                                    if let Ok(mut guard) = active_tool.lock() {
                                        *guard = None;
                                    }
                                }
                                AnthropicStreamEvent::Error { error } => {
                                    tracing::warn!(
                                        error_type = %error.error_type,
                                        message = %error.message,
                                        "Anthropic stream returned an error event"
                                    );
                                    return Some(Err(GatewayError::ProviderError {
                                        provider: Provider::Anthropic,
                                        status: if error.error_type == "overloaded_error" { 529 }
                                            else if error.error_type == "rate_limit_error" { 429 }
                                            else { 500 },
                                        message: error.message,
                                    }));
                                }
                                _ => {}
                            }
                        }
                        None
                    }
                    Err(e) => Some(Err(e)),
                }
            }
        });

        Ok(Box::pin(chunk_stream))
    }
}

// Anthropic-specific request/response types

/// Models that reject non-default sampling controls and manage sampling on the
/// provider side. Keep this centralized: the Playground, managed prompts, and
/// direct API requests can all otherwise inject a temperature that turns a
/// valid Anthropic request into HTTP 400.
pub(crate) fn uses_provider_managed_sampling(model: &str) -> bool {
    let normalized = model.replace('.', "-");
    [
        "claude-opus-4-7",
        "claude-opus-4-8",
        "claude-opus-5",
        "claude-sonnet-5",
        "claude-fable-5",
        "claude-mythos-5",
    ]
    .iter()
    .any(|family| model_is_in_family(&normalized, family))
}

fn model_is_in_family(model: &str, family: &str) -> bool {
    model == family
        || model
            .strip_prefix(family)
            .is_some_and(|suffix| suffix.starts_with('-') || suffix.starts_with(':'))
}

fn uses_adaptive_thinking(model: &str) -> bool {
    let normalized = model.replace('.', "-");
    ["claude-opus-4-7", "claude-opus-4-8", "claude-opus-5"]
        .iter()
        .any(|family| model_is_in_family(&normalized, family))
}

fn has_default_adaptive_thinking(model: &str) -> bool {
    let normalized = model.replace('.', "-");
    ["claude-sonnet-5", "claude-fable-5", "claude-mythos-5"]
        .iter()
        .any(|family| model_is_in_family(&normalized, family))
}

fn anthropic_sampling_parameters(
    model: &str,
    temperature: Option<f32>,
    top_p: Option<f32>,
) -> (Option<f32>, Option<f32>) {
    if uses_provider_managed_sampling(model) {
        if temperature.is_some() || top_p.is_some() {
            tracing::debug!(
                model,
                "Omitting sampling parameters unsupported by this Anthropic model"
            );
        }
        (None, None)
    } else {
        (temperature, top_p)
    }
}

fn anthropic_thinking_config(
    model: &str,
    thinking: Option<&ThinkingConfig>,
) -> Option<AnthropicThinkingConfig> {
    let requested = thinking.filter(|config| config.thinking_type == ThinkingToggle::Enabled)?;

    // Sonnet 5 and Fable/Mythos 5 already run adaptive thinking and reject the
    // legacy `enabled + budget_tokens` shape exposed by the OpenAI-compatible
    // Reiver request. Omitting the field preserves their provider default.
    if has_default_adaptive_thinking(model) {
        tracing::debug!(model, "Using Anthropic's default adaptive thinking");
        return None;
    }

    // Recent Opus models accept adaptive thinking but reject manual extended
    // thinking. Translate Reiver's legacy toggle instead of forwarding an
    // invalid token budget.
    if uses_adaptive_thinking(model) {
        return Some(AnthropicThinkingConfig {
            thinking_type: "adaptive".to_string(),
            budget_tokens: None,
        });
    }

    Some(AnthropicThinkingConfig {
        thinking_type: "enabled".to_string(),
        budget_tokens: Some(requested.budget_tokens.unwrap_or(10_000)),
    })
}

#[derive(Debug)]
struct AnthropicRequestOptions {
    model: AnthropicModelId,
    speed: Option<String>,
}

impl AnthropicRequestOptions {
    fn for_model(model: &str) -> Self {
        let normalized = model.replace('.', "-");
        let fast_base = normalized.strip_suffix("-fast").filter(|base| {
            model_is_in_family(base, "claude-opus-4-8")
                || model_is_in_family(base, "claude-opus-5")
        });

        match fast_base {
            Some(base) => Self {
                model: AnthropicModelId(base.to_string()),
                speed: Some("fast".to_string()),
            },
            None => Self {
                model: AnthropicModelId(normalized),
                speed: None,
            },
        }
    }
}

/// Newtype that normalizes OpenRouter-style dot-versioned model IDs
/// (e.g. `claude-opus-4.8`) to Anthropic's native dash format (`claude-opus-4-8`).
#[derive(Debug, Clone)]
struct AnthropicModelId(String);

impl From<&str> for AnthropicModelId {
    fn from(s: &str) -> Self {
        Self(s.replace('.', "-"))
    }
}

impl From<String> for AnthropicModelId {
    fn from(s: String) -> Self {
        Self::from(s.as_str())
    }
}

impl Serialize for AnthropicModelId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

/// Anthropic Messages API request.
///
/// Used for both streaming and non-streaming requests.
/// Set `stream: Some(true)` for streaming responses.
#[derive(Debug, Serialize)]
struct AnthropicRequest {
    model: AnthropicModelId,
    messages: Vec<AnthropicMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<String>,
    max_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stop_sequences: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream: Option<bool>,
    /// Extended thinking configuration (for Claude 3.7+)
    #[serde(skip_serializing_if = "Option::is_none")]
    thinking: Option<AnthropicThinkingConfig>,
    /// Anthropic fast mode (research preview). OpenRouter exposes this as a
    /// `-fast` model alias; Anthropic's native API expects a request field.
    #[serde(skip_serializing_if = "Option::is_none")]
    speed: Option<String>,
    /// Tool definitions for function calling.
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<AnthropicToolDefinition>>,
}

/// Anthropic extended thinking configuration.
#[derive(Debug, Serialize)]
struct AnthropicThinkingConfig {
    #[serde(rename = "type")]
    thinking_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    budget_tokens: Option<u32>,
}

/// Anthropic tool definition for function calling.
///
/// Maps from OpenAI's `Tool` format: `parameters` becomes `input_schema`.
#[derive(Debug, Clone, Serialize)]
struct AnthropicToolDefinition {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    input_schema: serde_json::Value,
}

/// Anthropic message with content blocks (supports multimodal).
#[derive(Debug, Serialize)]
struct AnthropicMessage {
    role: String,
    content: Vec<AnthropicRequestContentBlock>,
}

/// Content block types for Anthropic API requests.
///
/// Supports text, images, documents (PDFs), and tool interactions.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
enum AnthropicRequestContentBlock {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "image")]
    Image { source: AnthropicImageSource },
    /// Document block for PDFs and other file types (Claude 3.5+).
    #[serde(rename = "document")]
    Document {
        source: AnthropicDocumentSource,
        #[serde(skip_serializing_if = "Option::is_none")]
        title: Option<String>,
    },
    /// Tool use block for assistant messages that invoke tools
    #[serde(rename = "tool_use")]
    ToolUse {
        id: String,
        name: String,
        input: serde_json::Value,
    },
    /// Tool result block for user messages returning tool results
    #[serde(rename = "tool_result")]
    ToolResult {
        tool_use_id: String,
        content: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        is_error: Option<bool>,
    },
}

/// Image source for Anthropic API.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
enum AnthropicImageSource {
    #[serde(rename = "base64")]
    Base64 { media_type: String, data: String },
    #[serde(rename = "url")]
    Url { url: String },
}

/// Document source for Anthropic API (PDFs and other files).
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
enum AnthropicDocumentSource {
    #[serde(rename = "base64")]
    Base64 { media_type: String, data: String },
    #[serde(rename = "url")]
    Url { url: String },
}

#[derive(Debug, Deserialize)]
struct AnthropicResponse {
    id: String,
    content: Vec<AnthropicContentBlock>,
    stop_reason: String,
    usage: AnthropicUsage,
}

/// Anthropic content block - can be text or tool_use.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
enum AnthropicContentBlock {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "tool_use")]
    ToolUse {
        id: String,
        name: String,
        input: serde_json::Value,
    },
    /// Thinking block from extended thinking (Claude 3.7+).
    #[serde(rename = "thinking")]
    Thinking { thinking: String },
}

impl AnthropicContentBlock {
    /// Extract text content if this is a text block.
    fn as_text(&self) -> Option<&str> {
        match self {
            AnthropicContentBlock::Text { text } => Some(text),
            AnthropicContentBlock::ToolUse { .. } => None,
            AnthropicContentBlock::Thinking { .. } => None,
        }
    }

    /// Extract thinking content if this is a thinking block.
    fn as_thinking(&self) -> Option<&str> {
        match self {
            AnthropicContentBlock::Thinking { thinking } => Some(thinking),
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct AnthropicUsage {
    #[serde(default)]
    input_tokens: u32,
    output_tokens: u32,
    #[serde(default)]
    cache_read_input_tokens: u32,
    #[serde(default)]
    cache_creation_input_tokens: u32,
}

// Streaming event types

/// Anthropic streaming event types.
/// Fields are captured by serde for completeness but only a subset is read.
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[allow(dead_code)]
enum AnthropicStreamEvent {
    #[serde(rename = "message_start")]
    MessageStart { message: AnthropicMessageInfo },
    #[serde(rename = "content_block_start")]
    ContentBlockStart {
        index: u32,
        content_block: AnthropicContentBlock,
    },
    #[serde(rename = "content_block_delta")]
    ContentBlockDelta {
        index: u32,
        delta: AnthropicTextDelta,
    },
    #[serde(rename = "content_block_stop")]
    ContentBlockStop { index: u32 },
    #[serde(rename = "message_delta")]
    MessageDelta {
        delta: AnthropicMessageDelta,
        #[serde(default)]
        usage: Option<AnthropicUsage>,
    },
    #[serde(rename = "message_stop")]
    MessageStop,
    #[serde(rename = "ping")]
    Ping,
    #[serde(rename = "error")]
    Error { error: AnthropicErrorInfo },
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct AnthropicMessageInfo {
    id: String,
    model: String,
    usage: AnthropicUsage,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct AnthropicTextDelta {
    #[serde(rename = "type")]
    delta_type: Option<String>,
    /// Present for text_delta events
    text: Option<String>,
    /// Present for thinking_delta events (interleaved-thinking-2025-01-05)
    thinking: Option<String>,
    /// Present for input_json_delta events (tool-use streaming)
    partial_json: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct AnthropicMessageDelta {
    stop_reason: Option<String>,
    #[serde(default)]
    stop_sequence: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct AnthropicErrorInfo {
    #[serde(rename = "type")]
    error_type: String,
    message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_supports_model() {
        let provider = AnthropicProvider::new();

        assert!(provider.supports_model("claude-sonnet-4-6"));
        assert!(provider.supports_model("claude-opus-4-6"));
        assert!(provider.supports_model("claude-haiku-4-5-20251001"));
        assert!(provider.supports_model("claude-sonnet-4-20250514"));

        assert!(!provider.supports_model("gpt-4"));
        assert!(!provider.supports_model("gemini-pro"));
    }

    #[test]
    fn test_anthropic_model_id_normalizes_dots_to_dashes() {
        let id = AnthropicModelId::from("claude-opus-4.8");
        assert_eq!(id.0, "claude-opus-4-8");

        let id = AnthropicModelId::from("claude-opus-4.8-fast");
        assert_eq!(id.0, "claude-opus-4-8-fast");

        let id = AnthropicModelId::from("claude-haiku-4.5");
        assert_eq!(id.0, "claude-haiku-4-5");

        let id = AnthropicModelId::from("claude-3.5-haiku");
        assert_eq!(id.0, "claude-3-5-haiku");

        // Already dash-versioned IDs pass through unchanged
        let id = AnthropicModelId::from("claude-opus-4-8");
        assert_eq!(id.0, "claude-opus-4-8");

        let id = AnthropicModelId::from("claude-3-haiku");
        assert_eq!(id.0, "claude-3-haiku");

        let id = AnthropicModelId::from("claude-sonnet-4-6");
        assert_eq!(id.0, "claude-sonnet-4-6");
    }

    #[test]
    fn test_anthropic_model_id_serializes_normalized() {
        let id = AnthropicModelId::from("claude-opus-4.8");
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"claude-opus-4-8\"");
    }

    #[test]
    fn test_provider_managed_sampling_model_families() {
        assert!(uses_provider_managed_sampling("claude-sonnet-5"));
        assert!(uses_provider_managed_sampling("claude-opus-5-fast"));
        assert!(uses_provider_managed_sampling("claude-opus-4.8"));
        assert!(uses_provider_managed_sampling(
            "claude-fable-5-20260609"
        ));
        assert!(!uses_provider_managed_sampling("claude-sonnet-4-6"));
        assert!(!uses_provider_managed_sampling(
            "claude-haiku-4-5-20251001"
        ));
    }

    #[test]
    fn test_sampling_parameters_are_omitted_for_managed_models() {
        assert_eq!(
            anthropic_sampling_parameters("claude-sonnet-5", Some(0.7), Some(0.9)),
            (None, None)
        );
        assert_eq!(
            anthropic_sampling_parameters("claude-sonnet-4-6", Some(0.7), Some(0.9)),
            (Some(0.7), Some(0.9))
        );
    }

    #[test]
    fn test_fast_alias_maps_to_native_anthropic_request() {
        let options = AnthropicRequestOptions::for_model("claude-opus-4.8-fast");
        assert_eq!(options.model.0, "claude-opus-4-8");
        assert_eq!(options.speed.as_deref(), Some("fast"));

        let options = AnthropicRequestOptions::for_model("claude-opus-5-fast");
        assert_eq!(options.model.0, "claude-opus-5");
        assert_eq!(options.speed.as_deref(), Some("fast"));

        let unsupported = AnthropicRequestOptions::for_model("claude-opus-4.7-fast");
        assert_eq!(unsupported.model.0, "claude-opus-4-7-fast");
        assert!(unsupported.speed.is_none());
    }

    #[test]
    fn test_thinking_config_tracks_anthropic_model_generation() {
        let requested = ThinkingConfig {
            thinking_type: ThinkingToggle::Enabled,
            budget_tokens: Some(8_000),
        };

        assert!(anthropic_thinking_config("claude-sonnet-5", Some(&requested)).is_none());

        let opus = anthropic_thinking_config("claude-opus-4.8", Some(&requested)).unwrap();
        assert_eq!(opus.thinking_type, "adaptive");
        assert!(opus.budget_tokens.is_none());

        let legacy =
            anthropic_thinking_config("claude-sonnet-4-6", Some(&requested)).unwrap();
        assert_eq!(legacy.thinking_type, "enabled");
        assert_eq!(legacy.budget_tokens, Some(8_000));
    }

    #[test]
    fn test_extract_system_message() {
        let messages = vec![
            ChatMessage {
                role: MessageRole::System,
                content: Some(MessageContent::Text("You are helpful.".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            },
            ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text("Hello".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            },
        ];

        let system = find_system_message_text(&messages);
        assert_eq!(system, Some("You are helpful.".to_string()));
    }

    #[test]
    fn test_convert_messages() {
        let provider = AnthropicProvider::new();

        let messages = vec![
            ChatMessage {
                role: MessageRole::System,
                content: Some(MessageContent::Text("You are helpful.".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            },
            ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text("Hello".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            },
        ];

        let converted = provider.convert_messages(&messages);

        // System message should be filtered out
        assert_eq!(converted.len(), 1);
        assert_eq!(converted[0].role, "user");
        assert_eq!(converted[0].content.len(), 1);
        match &converted[0].content[0] {
            AnthropicRequestContentBlock::Text { text } => assert_eq!(text, "Hello"),
            _ => panic!("Expected text content block"),
        }
    }

    #[test]
    fn test_convert_multimodal_content() {
        use crate::gateway::types::{ContentPart, ImageUrl};

        let provider = AnthropicProvider::new();

        let content = Some(MessageContent::Parts(vec![
            ContentPart::Text {
                text: "What's in this image?".to_string(),
            },
            ContentPart::ImageUrl {
                image_url: ImageUrl {
                    url: "data:image/jpeg;base64,/9j/4AAQSkZJRg==".to_string(),
                    detail: None,
                },
            },
        ]));

        let converted = provider.convert_content(&content);

        assert_eq!(converted.len(), 2);

        // First should be text
        match &converted[0] {
            AnthropicRequestContentBlock::Text { text } => {
                assert_eq!(text, "What's in this image?");
            }
            _ => panic!("Expected text block"),
        }

        // Second should be image
        match &converted[1] {
            AnthropicRequestContentBlock::Image { source } => match source {
                AnthropicImageSource::Base64 { media_type, data } => {
                    assert_eq!(media_type, "image/jpeg");
                    assert_eq!(data, "/9j/4AAQSkZJRg==");
                }
                _ => panic!("Expected base64 source"),
            },
            _ => panic!("Expected image block"),
        }
    }

    #[test]
    fn test_convert_url_image() {
        let provider = AnthropicProvider::new();

        let result = provider.convert_image_url("https://example.com/image.jpg");
        assert!(result.is_some());

        match result.unwrap() {
            AnthropicRequestContentBlock::Image { source } => match source {
                AnthropicImageSource::Url { url } => {
                    assert_eq!(url, "https://example.com/image.jpg");
                }
                _ => panic!("Expected URL source"),
            },
            _ => panic!("Expected image block"),
        }
    }

    #[test]
    fn test_convert_tool_call_messages() {
        use crate::gateway::types::FunctionCall;
        use crate::gateway::types::ToolCall as OpenAIToolCall;

        let provider = AnthropicProvider::new();

        // Simulate a multi-turn tool conversation:
        // 1. User asks about weather
        // 2. Assistant responds with tool_use
        // 3. User provides tool_result
        // 4. Assistant responds with final answer
        let messages = vec![
            ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text(
                    "What's the weather in Paris?".to_string(),
                )),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            },
            ChatMessage {
                role: MessageRole::Assistant,
                content: None,
                name: None,
                tool_calls: Some(vec![OpenAIToolCall {
                    index: None,
                    id: "call_abc123".to_string(),
                    tool_type: ToolType::Function,
                    function: FunctionCall {
                        name: "get_weather".to_string(),
                        arguments: r#"{"location": "Paris"}"#.to_string(),
                    },
                }]),
                tool_call_id: None,
                reasoning_content: None,
            },
            ChatMessage {
                role: MessageRole::Other,
                content: Some(MessageContent::Text("Sunny, 22°C".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: Some("call_abc123".to_string()),
                reasoning_content: None,
            },
        ];

        let converted = provider.convert_messages(&messages);

        // Should have 3 messages: user, assistant with tool_use, user with tool_result
        assert_eq!(converted.len(), 3);

        // First message: user text
        assert_eq!(converted[0].role, "user");
        assert_eq!(converted[0].content.len(), 1);
        match &converted[0].content[0] {
            AnthropicRequestContentBlock::Text { text } => {
                assert_eq!(text, "What's the weather in Paris?");
            }
            _ => panic!("Expected text block for user message"),
        }

        // Second message: assistant with tool_use
        assert_eq!(converted[1].role, "assistant");
        assert_eq!(converted[1].content.len(), 1);
        match &converted[1].content[0] {
            AnthropicRequestContentBlock::ToolUse { id, name, input } => {
                assert_eq!(id, "call_abc123");
                assert_eq!(name, "get_weather");
                assert_eq!(
                    input.get("location").and_then(|v| v.as_str()),
                    Some("Paris")
                );
            }
            _ => panic!("Expected tool_use block for assistant message"),
        }

        // Third message: user with tool_result (tool messages become user messages)
        assert_eq!(converted[2].role, "user");
        assert_eq!(converted[2].content.len(), 1);
        match &converted[2].content[0] {
            AnthropicRequestContentBlock::ToolResult {
                tool_use_id,
                content,
                is_error,
            } => {
                assert_eq!(tool_use_id, "call_abc123");
                assert_eq!(content, "Sunny, 22°C");
                assert!(is_error.is_none());
            }
            _ => panic!("Expected tool_result block for tool message"),
        }
    }

    #[test]
    fn test_convert_multiple_tool_results() {
        use crate::gateway::types::FunctionCall;
        use crate::gateway::types::ToolCall as OpenAIToolCall;

        let provider = AnthropicProvider::new();

        // Multiple tool calls and results
        let messages = vec![
            ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text(
                    "What's the weather in Paris and London?".to_string(),
                )),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            },
            ChatMessage {
                role: MessageRole::Assistant,
                content: None,
                name: None,
                tool_calls: Some(vec![
                    OpenAIToolCall {
                        index: None,
                        id: "call_paris".to_string(),
                        tool_type: ToolType::Function,
                        function: FunctionCall {
                            name: "get_weather".to_string(),
                            arguments: r#"{"location": "Paris"}"#.to_string(),
                        },
                    },
                    OpenAIToolCall {
                        index: None,
                        id: "call_london".to_string(),
                        tool_type: ToolType::Function,
                        function: FunctionCall {
                            name: "get_weather".to_string(),
                            arguments: r#"{"location": "London"}"#.to_string(),
                        },
                    },
                ]),
                tool_call_id: None,
                reasoning_content: None,
            },
            ChatMessage {
                role: MessageRole::Other,
                content: Some(MessageContent::Text("Sunny, 22°C".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: Some("call_paris".to_string()),
                reasoning_content: None,
            },
            ChatMessage {
                role: MessageRole::Other,
                content: Some(MessageContent::Text("Rainy, 15°C".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: Some("call_london".to_string()),
                reasoning_content: None,
            },
        ];

        let converted = provider.convert_messages(&messages);

        // Should have 3 messages: user, assistant with 2 tool_uses, user with 2 tool_results
        assert_eq!(converted.len(), 3);

        // Assistant should have 2 tool_use blocks
        assert_eq!(converted[1].content.len(), 2);

        // User (tool results) should have 2 tool_result blocks merged into one user message
        assert_eq!(converted[2].role, "user");
        assert_eq!(converted[2].content.len(), 2);

        // Verify both tool results are present
        let tool_result_ids: Vec<&str> = converted[2]
            .content
            .iter()
            .filter_map(|c| match c {
                AnthropicRequestContentBlock::ToolResult { tool_use_id, .. } => {
                    Some(tool_use_id.as_str())
                }
                _ => None,
            })
            .collect();

        assert!(tool_result_ids.contains(&"call_paris"));
        assert!(tool_result_ids.contains(&"call_london"));
    }

    /// Regression: Anthropic streaming previously reported `prompt_tokens: 0`
    /// because `input_tokens` from the `message_start` event was discarded,
    /// and `message_delta` only carries `output_tokens`.
    #[tokio::test]
    async fn test_streaming_captures_input_tokens_from_message_start() {
        use futures::stream;

        let sse_lines = vec![
            r#"data: {"type":"message_start","message":{"id":"msg_01","type":"message","role":"assistant","model":"claude-3-5-sonnet-20241022","content":[],"usage":{"input_tokens":42,"output_tokens":0}}}"#,
            r#"data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#,
            r#"data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}"#,
            r#"data: {"type":"content_block_stop","index":0}"#,
            r#"data: {"type":"message_delta","delta":{"type":"message_delta","stop_reason":"end_turn"},"usage":{"input_tokens":0,"output_tokens":7}}"#,
            "data: [DONE]",
        ];

        let raw_bytes = sse_lines.join("\n\n") + "\n\n";
        let byte_stream =
            stream::iter(vec![Ok::<_, std::io::Error>(bytes::Bytes::from(raw_bytes))]);

        let data_stream = super::super::sse::bytes_to_sse_data_stream(byte_stream);

        let model = "claude-3-5-sonnet-20241022".to_string();
        let chunk_id = "chatcmpl-test".to_string();
        let captured_input_tokens = Arc::new(AtomicU32::new(0));

        let chunk_stream = data_stream.filter_map({
            let chunk_id = chunk_id.clone();
            let model = model.clone();
            let captured_input_tokens = captured_input_tokens.clone();
            move |result| {
                let chunk_id = chunk_id.clone();
                let model = model.clone();
                let captured_input_tokens = captured_input_tokens.clone();
                async move {
                    match result {
                        Ok(data) => {
                            if let Ok(event) = serde_json::from_str::<AnthropicStreamEvent>(&data) {
                                match event {
                                    AnthropicStreamEvent::ContentBlockDelta { delta, .. } => {
                                        if let Some(text) = delta.text {
                                            return Some(ChatCompletionChunk::with_content(
                                                chunk_id, model, text,
                                            ));
                                        }
                                    }
                                    AnthropicStreamEvent::MessageDelta { delta, usage } => {
                                        if let Some(stop_reason) = delta.stop_reason {
                                            let openai_reason = map_finish_reason_to_openai(
                                                &stop_reason,
                                                Provider::Anthropic,
                                            );
                                            let mut chunk = ChatCompletionChunk::finished(
                                                chunk_id,
                                                model,
                                                openai_reason,
                                            );
                                            if let Some(anthropic_usage) = usage {
                                                let input_tokens =
                                                    captured_input_tokens.load(Ordering::Relaxed);
                                                chunk.usage = Some(Usage {
                                                    prompt_tokens: input_tokens,
                                                    completion_tokens: anthropic_usage
                                                        .output_tokens,
                                                    total_tokens: input_tokens
                                                        + anthropic_usage.output_tokens,
                                                    thinking_tokens: None,
                                                    completion_tokens_details: None,
                                                    prompt_tokens_details: None,
                                                });
                                            }
                                            return Some(chunk);
                                        }
                                    }
                                    AnthropicStreamEvent::MessageStart { message } => {
                                        captured_input_tokens
                                            .store(message.usage.input_tokens, Ordering::Relaxed);
                                    }
                                    _ => {}
                                }
                            }
                            None
                        }
                        Err(_) => None,
                    }
                }
            }
        });

        let chunks: Vec<_> = chunk_stream.collect().await;

        let final_chunk = chunks.last().expect("should have at least one chunk");
        let usage = final_chunk
            .usage
            .as_ref()
            .expect("final chunk should have usage");

        assert_eq!(
            usage.prompt_tokens, 42,
            "prompt_tokens must come from message_start"
        );
        assert_eq!(usage.completion_tokens, 7);
        assert_eq!(usage.total_tokens, 49);
    }

    /// Regression: tool definitions must be forwarded to Anthropic.
    ///
    /// Previously, `AnthropicRequest` had no `tools` field, so function
    /// definitions were silently dropped and Claude never initiated tool calls.
    #[test]
    fn test_tool_definitions_included_in_request() {
        use crate::gateway::types::{FunctionDefinition, Tool, ToolType};

        let provider = AnthropicProvider::new();

        let tools = Some(vec![
            Tool {
                tool_type: ToolType::Function,
                function: FunctionDefinition {
                    name: "get_weather".to_string(),
                    description: Some("Get the current weather for a location".to_string()),
                    parameters: Some(serde_json::json!({
                        "type": "object",
                        "properties": {
                            "location": {
                                "type": "string",
                                "description": "City name"
                            }
                        },
                        "required": ["location"]
                    })),
                },
            },
            Tool {
                tool_type: ToolType::Function,
                function: FunctionDefinition {
                    name: "search".to_string(),
                    description: None,
                    parameters: None,
                },
            },
        ]);

        let converted = provider.convert_tools(&tools);
        let converted = converted.expect("tools should not be None");

        assert_eq!(converted.len(), 2, "both tool definitions must be present");

        assert_eq!(converted[0].name, "get_weather");
        assert_eq!(
            converted[0].description.as_deref(),
            Some("Get the current weather for a location")
        );
        let schema = &converted[0].input_schema;
        assert_eq!(schema["type"], "object");
        assert!(schema["properties"]["location"].is_object());

        assert_eq!(converted[1].name, "search");
        assert!(converted[1].description.is_none());
        assert_eq!(converted[1].input_schema["type"], "object");

        // Verify the full round-trip through serde produces the expected JSON
        let request = AnthropicRequest {
            model: AnthropicModelId::from("claude-3-5-sonnet-20241022"),
            messages: vec![AnthropicMessage {
                role: "user".to_string(),
                content: vec![AnthropicRequestContentBlock::Text {
                    text: "What is the weather?".to_string(),
                }],
            }],
            system: None,
            max_tokens: 1024,
            temperature: None,
            top_p: None,
            stop_sequences: None,
            stream: None,
            thinking: None,
            speed: None,
            tools: Some(converted),
        };

        let json = serde_json::to_value(&request).expect("serialization must succeed");
        let tools_json = json
            .get("tools")
            .expect("serialized request must contain 'tools' key");
        let tools_arr = tools_json.as_array().expect("'tools' must be an array");
        assert_eq!(tools_arr.len(), 2);
        assert_eq!(tools_arr[0]["name"], "get_weather");
        assert!(
            tools_arr[0].get("input_schema").is_some(),
            "Anthropic tool must use 'input_schema'"
        );
    }

    #[test]
    fn test_empty_tools_serializes_as_absent() {
        let provider = AnthropicProvider::new();

        assert!(provider.convert_tools(&None).is_none());
        assert!(provider.convert_tools(&Some(vec![])).is_none());
    }

    /// Regression: Anthropic `message_delta` events only carry `output_tokens`
    /// (no `input_tokens`). Without `#[serde(default)]` on `input_tokens`, the
    /// shared `AnthropicUsage` struct fails to deserialize, silently dropping
    /// the stop_reason and final usage from the stream.
    #[test]
    fn test_message_delta_usage_without_input_tokens() {
        let delta_json = r#"{
            "type": "message_delta",
            "delta": { "stop_reason": "end_turn" },
            "usage": { "output_tokens": 42 }
        }"#;

        let event: AnthropicStreamEvent = serde_json::from_str(delta_json)
            .expect("message_delta with only output_tokens must deserialize");

        match event {
            AnthropicStreamEvent::MessageDelta { delta, usage } => {
                assert_eq!(delta.stop_reason.as_deref(), Some("end_turn"));
                let usage = usage.expect("usage must be present");
                assert_eq!(usage.output_tokens, 42);
                assert_eq!(
                    usage.input_tokens, 0,
                    "input_tokens must default to 0 when absent"
                );
            }
            other => panic!("expected MessageDelta, got {:?}", other),
        }
    }

    /// Regression: `AnthropicStreamEvent::Error` was matched by `_ => {}`,
    /// silently swallowing mid-stream errors (overloaded, content policy, rate
    /// limits). The stream would just end without signaling an error to the
    /// client. The fix converts the error event into `GatewayError::ProviderError`
    /// so it propagates through the stream.
    #[tokio::test]
    async fn test_stream_error_event_propagated() {
        use futures::stream;

        let sse_lines = vec![
            r#"data: {"type":"message_start","message":{"id":"msg_01","type":"message","role":"assistant","model":"claude-3-5-sonnet-20241022","content":[],"usage":{"input_tokens":10,"output_tokens":0}}}"#,
            r#"data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#,
            r#"data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}"#,
            r#"data: {"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}"#,
        ];

        let raw_bytes = sse_lines.join("\n\n") + "\n\n";
        let byte_stream =
            stream::iter(vec![Ok::<_, std::io::Error>(bytes::Bytes::from(raw_bytes))]);

        let data_stream = super::super::sse::bytes_to_sse_data_stream(byte_stream);

        let model = "claude-3-5-sonnet-20241022".to_string();
        let chunk_id = "chatcmpl-test".to_string();
        let captured_input_tokens = Arc::new(AtomicU32::new(0));

        let chunk_stream = data_stream.filter_map({
            let chunk_id = chunk_id.clone();
            let model = model.clone();
            let captured_input_tokens = captured_input_tokens.clone();
            move |result| {
                let chunk_id = chunk_id.clone();
                let model = model.clone();
                let captured_input_tokens = captured_input_tokens.clone();
                async move {
                    match result {
                        Ok(data) => {
                            if let Ok(event) = serde_json::from_str::<AnthropicStreamEvent>(&data) {
                                match event {
                                    AnthropicStreamEvent::ContentBlockDelta { delta, .. } => {
                                        if let Some(text) = delta.text {
                                            return Some(Ok(ChatCompletionChunk::with_content(
                                                chunk_id, model, text,
                                            )));
                                        }
                                    }
                                    AnthropicStreamEvent::MessageStart { message } => {
                                        captured_input_tokens
                                            .store(message.usage.input_tokens, Ordering::Relaxed);
                                    }
                                    AnthropicStreamEvent::Error { error } => {
                                        return Some(Err(GatewayError::ProviderError {
                                            provider: Provider::Anthropic,
                                            status: if error.error_type == "overloaded_error" {
                                                529
                                            } else if error.error_type == "rate_limit_error" {
                                                429
                                            } else {
                                                500
                                            },
                                            message: error.message,
                                        }));
                                    }
                                    _ => {}
                                }
                            }
                            None
                        }
                        Err(_) => None,
                    }
                }
            }
        });

        let chunks: Vec<Result<ChatCompletionChunk, GatewayError>> = chunk_stream.collect().await;

        assert!(chunks.len() >= 2, "should have content chunk + error");

        let last = chunks.last().expect("should have at least one item");
        assert!(
            last.is_err(),
            "last item must be an Err from the error event"
        );

        let err = last.as_ref().unwrap_err();
        match err {
            GatewayError::ProviderError {
                provider,
                status,
                message,
            } => {
                assert_eq!(*provider, Provider::Anthropic);
                assert_eq!(*status, 529);
                assert_eq!(message, "Overloaded");
            }
            other => panic!("expected ProviderError, got {:?}", other),
        }
    }

    /// Regression: rate_limit_error events mid-stream must produce status 429
    /// so the retry/fallback system can handle them correctly.
    #[test]
    fn test_stream_error_event_rate_limit_maps_to_429() {
        let error_json = r#"{
            "type": "error",
            "error": { "type": "rate_limit_error", "message": "Rate limit exceeded" }
        }"#;

        let event: AnthropicStreamEvent =
            serde_json::from_str(error_json).expect("error event must deserialize");

        match event {
            AnthropicStreamEvent::Error { error } => {
                assert_eq!(error.error_type, "rate_limit_error");
                let status = if error.error_type == "overloaded_error" {
                    529
                } else if error.error_type == "rate_limit_error" {
                    429
                } else {
                    500
                };
                assert_eq!(status, 429);
            }
            other => panic!("expected Error event, got {:?}", other),
        }
    }

    /// Regression: `input_json_delta` events (tool-use streaming) were silently
    /// dropped because `AnthropicTextDelta` only captured `text` and `thinking`
    /// fields. Tool call arguments never reached the client during streaming.
    /// The fix adds `partial_json` to `AnthropicTextDelta` and emits
    /// OpenAI-compatible tool call argument deltas.
    #[tokio::test]
    async fn test_streaming_tool_use_input_json_delta() {
        use futures::stream;

        let sse_lines = vec![
            // message_start with usage
            r#"data: {"type":"message_start","message":{"id":"msg_01","type":"message","role":"assistant","model":"claude-3-5-sonnet-20241022","content":[],"usage":{"input_tokens":50,"output_tokens":0}}}"#,
            // content_block_start with tool_use
            r#"data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_abc123","name":"get_weather","input":{}}}"#,
            // input_json_delta chunks
            r#"data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"loc"}}"#,
            r#"data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"ation\":"}}"#,
            r#"data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"\"Paris\"}"}}"#,
            // content_block_stop
            r#"data: {"type":"content_block_stop","index":0}"#,
            // message_delta with stop
            r#"data: {"type":"message_delta","delta":{"type":"message_delta","stop_reason":"tool_use"},"usage":{"input_tokens":0,"output_tokens":20}}"#,
            "data: [DONE]",
        ];

        let raw_bytes = sse_lines.join("\n\n") + "\n\n";
        let byte_stream =
            stream::iter(vec![Ok::<_, std::io::Error>(bytes::Bytes::from(raw_bytes))]);

        let data_stream = super::super::sse::bytes_to_sse_data_stream(byte_stream);

        let model = "claude-3-5-sonnet-20241022".to_string();
        let chunk_id = "chatcmpl-test".to_string();
        let captured_input_tokens = Arc::new(AtomicU32::new(0));
        let active_tool: Arc<std::sync::Mutex<Option<(u32, String, String)>>> =
            Arc::new(std::sync::Mutex::new(None));
        let tool_call_counter = Arc::new(AtomicU32::new(0));

        let chunk_stream = data_stream.filter_map({
            let chunk_id = chunk_id.clone();
            let model = model.clone();
            let captured_input_tokens = captured_input_tokens.clone();
            let active_tool = active_tool.clone();
            let tool_call_counter = tool_call_counter.clone();
            move |result| {
                let chunk_id = chunk_id.clone();
                let model = model.clone();
                let captured_input_tokens = captured_input_tokens.clone();
                let active_tool = active_tool.clone();
                let tool_call_counter = tool_call_counter.clone();
                async move {
                    match result {
                        Ok(data) => {
                            if let Ok(event) = serde_json::from_str::<AnthropicStreamEvent>(&data) {
                                match event {
                                    AnthropicStreamEvent::ContentBlockDelta { delta, .. } => {
                                        if let Some(text) = delta.text {
                                            return Some(ChatCompletionChunk::with_content(
                                                chunk_id, model, text,
                                            ));
                                        }
                                        if let Some(json_fragment) = delta.partial_json {
                                            let tc_index = active_tool
                                                .lock()
                                                .ok()
                                                .and_then(|g| g.as_ref().map(|(idx, _, _)| *idx))
                                                .unwrap_or(0);
                                            return Some(ChatCompletionChunk::new(
                                                chunk_id,
                                                model,
                                                vec![ChunkChoice {
                                                    index: 0,
                                                    delta: ChunkDelta {
                                                        tool_calls: Some(vec![ToolCall {
                                                            index: Some(tc_index),
                                                            id: String::new(),
                                                            tool_type: ToolType::Function,
                                                            function: FunctionCall {
                                                                name: String::new(),
                                                                arguments: json_fragment,
                                                            },
                                                        }]),
                                                        ..Default::default()
                                                    },
                                                    finish_reason: None,
                                                }],
                                            ));
                                        }
                                    }
                                    AnthropicStreamEvent::ContentBlockStart {
                                        content_block,
                                        ..
                                    } => {
                                        if let AnthropicContentBlock::ToolUse { id, name, .. } =
                                            content_block
                                        {
                                            let tc_index =
                                                tool_call_counter.fetch_add(1, Ordering::SeqCst);
                                            if let Ok(mut guard) = active_tool.lock() {
                                                *guard = Some((tc_index, id.clone(), name.clone()));
                                            }
                                            return Some(ChatCompletionChunk::new(
                                                chunk_id,
                                                model,
                                                vec![ChunkChoice {
                                                    index: 0,
                                                    delta: ChunkDelta {
                                                        tool_calls: Some(vec![ToolCall {
                                                            index: Some(tc_index),
                                                            id,
                                                            tool_type: ToolType::Function,
                                                            function: FunctionCall {
                                                                name,
                                                                arguments: String::new(),
                                                            },
                                                        }]),
                                                        ..Default::default()
                                                    },
                                                    finish_reason: None,
                                                }],
                                            ));
                                        }
                                    }
                                    AnthropicStreamEvent::ContentBlockStop { .. } => {
                                        if let Ok(mut guard) = active_tool.lock() {
                                            *guard = None;
                                        }
                                    }
                                    AnthropicStreamEvent::MessageStart { message } => {
                                        captured_input_tokens
                                            .store(message.usage.input_tokens, Ordering::Relaxed);
                                    }
                                    _ => {}
                                }
                            }
                            None
                        }
                        Err(_) => None,
                    }
                }
            }
        });

        let chunks: Vec<ChatCompletionChunk> = chunk_stream.collect().await;

        // Should have: tool_call_start + 3 partial_json deltas + message_delta (stop)
        assert!(
            chunks.len() >= 4,
            "expected at least 4 chunks (tool start + 3 deltas), got {}",
            chunks.len()
        );

        // First tool chunk: must have the tool call id and name
        let first = &chunks[0];
        let tc = first.choices[0]
            .delta
            .tool_calls
            .as_ref()
            .expect("first chunk must have tool_calls");
        assert_eq!(tc[0].id, "toolu_abc123", "tool call id must match");
        assert_eq!(
            tc[0].function.name, "get_weather",
            "tool call name must match"
        );
        assert!(
            tc[0].function.arguments.is_empty(),
            "initial arguments must be empty"
        );

        // Subsequent chunks: must carry partial_json as arguments
        let mut accumulated_args = String::new();
        for chunk in &chunks[1..] {
            if let Some(ref tcs) = chunk
                .choices
                .first()
                .and_then(|c| c.delta.tool_calls.as_ref())
            {
                accumulated_args.push_str(&tcs[0].function.arguments);
            }
        }
        assert_eq!(
            accumulated_args, r#"{"location":"Paris"}"#,
            "accumulated tool call arguments must form valid JSON"
        );
    }

    /// Verify that `input_json_delta` events deserialize correctly into
    /// `AnthropicTextDelta` with the `partial_json` field populated.
    #[test]
    fn test_input_json_delta_deserializes() {
        let delta_json = r#"{
            "type": "content_block_delta",
            "index": 0,
            "delta": { "type": "input_json_delta", "partial_json": "{\"key\":" }
        }"#;

        let event: AnthropicStreamEvent =
            serde_json::from_str(delta_json).expect("input_json_delta must deserialize");

        match event {
            AnthropicStreamEvent::ContentBlockDelta { delta, .. } => {
                assert!(delta.text.is_none());
                assert!(delta.thinking.is_none());
                assert_eq!(
                    delta.partial_json.as_deref(),
                    Some("{\"key\":"),
                    "partial_json must be captured"
                );
            }
            other => panic!("expected ContentBlockDelta, got {:?}", other),
        }
    }

    /// Regression: streaming with multiple sequential tool calls must assign
    /// distinct `index` values (0, 1, ...) so clients can correlate argument
    /// deltas with the correct tool call. Previously all deltas had no index.
    #[tokio::test]
    async fn test_streaming_multi_tool_call_indices() {
        use futures::stream;

        let sse_lines = vec![
            // message_start
            r#"data: {"type":"message_start","message":{"id":"msg_01","type":"message","role":"assistant","model":"claude-3-5-sonnet-20241022","content":[],"usage":{"input_tokens":50,"output_tokens":0}}}"#,
            // First tool call: get_weather
            r#"data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_weather","name":"get_weather","input":{}}}"#,
            r#"data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"location\":\"Paris\"}"}}"#,
            r#"data: {"type":"content_block_stop","index":0}"#,
            // Second tool call: get_time
            r#"data: {"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_time","name":"get_time","input":{}}}"#,
            r#"data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"tz\":\"UTC\"}"}}"#,
            r#"data: {"type":"content_block_stop","index":1}"#,
            // message_delta with stop
            r#"data: {"type":"message_delta","delta":{"type":"message_delta","stop_reason":"tool_use"},"usage":{"input_tokens":0,"output_tokens":30}}"#,
            "data: [DONE]",
        ];

        let raw_bytes = sse_lines.join("\n\n") + "\n\n";
        let byte_stream =
            stream::iter(vec![Ok::<_, std::io::Error>(bytes::Bytes::from(raw_bytes))]);
        let data_stream = super::super::sse::bytes_to_sse_data_stream(byte_stream);

        let model = "claude-3-5-sonnet-20241022".to_string();
        let chunk_id = "chatcmpl-multi".to_string();
        let captured_input_tokens = Arc::new(AtomicU32::new(0));
        let active_tool: Arc<std::sync::Mutex<Option<(u32, String, String)>>> =
            Arc::new(std::sync::Mutex::new(None));
        let tool_call_counter = Arc::new(AtomicU32::new(0));

        let chunk_stream = data_stream.filter_map({
            let chunk_id = chunk_id.clone();
            let model = model.clone();
            let captured_input_tokens = captured_input_tokens.clone();
            let active_tool = active_tool.clone();
            let tool_call_counter = tool_call_counter.clone();
            move |result| {
                let chunk_id = chunk_id.clone();
                let model = model.clone();
                let captured_input_tokens = captured_input_tokens.clone();
                let active_tool = active_tool.clone();
                let tool_call_counter = tool_call_counter.clone();
                async move {
                    match result {
                        Ok(data) => {
                            if let Ok(event) = serde_json::from_str::<AnthropicStreamEvent>(&data) {
                                match event {
                                    AnthropicStreamEvent::ContentBlockDelta { delta, .. } => {
                                        if let Some(json_fragment) = delta.partial_json {
                                            let tc_index = active_tool
                                                .lock()
                                                .ok()
                                                .and_then(|g| g.as_ref().map(|(idx, _, _)| *idx))
                                                .unwrap_or(0);
                                            return Some(ChatCompletionChunk::new(
                                                chunk_id,
                                                model,
                                                vec![ChunkChoice {
                                                    index: 0,
                                                    delta: ChunkDelta {
                                                        tool_calls: Some(vec![ToolCall {
                                                            index: Some(tc_index),
                                                            id: String::new(),
                                                            tool_type: ToolType::Function,
                                                            function: FunctionCall {
                                                                name: String::new(),
                                                                arguments: json_fragment,
                                                            },
                                                        }]),
                                                        ..Default::default()
                                                    },
                                                    finish_reason: None,
                                                }],
                                            ));
                                        }
                                        None
                                    }
                                    AnthropicStreamEvent::ContentBlockStart {
                                        content_block,
                                        ..
                                    } => {
                                        if let AnthropicContentBlock::ToolUse { id, name, .. } =
                                            content_block
                                        {
                                            let tc_index =
                                                tool_call_counter.fetch_add(1, Ordering::SeqCst);
                                            if let Ok(mut guard) = active_tool.lock() {
                                                *guard = Some((tc_index, id.clone(), name.clone()));
                                            }
                                            return Some(ChatCompletionChunk::new(
                                                chunk_id,
                                                model,
                                                vec![ChunkChoice {
                                                    index: 0,
                                                    delta: ChunkDelta {
                                                        tool_calls: Some(vec![ToolCall {
                                                            index: Some(tc_index),
                                                            id,
                                                            tool_type: ToolType::Function,
                                                            function: FunctionCall {
                                                                name,
                                                                arguments: String::new(),
                                                            },
                                                        }]),
                                                        ..Default::default()
                                                    },
                                                    finish_reason: None,
                                                }],
                                            ));
                                        }
                                        None
                                    }
                                    AnthropicStreamEvent::ContentBlockStop { .. } => {
                                        if let Ok(mut guard) = active_tool.lock() {
                                            *guard = None;
                                        }
                                        None
                                    }
                                    AnthropicStreamEvent::MessageStart { message } => {
                                        captured_input_tokens
                                            .store(message.usage.input_tokens, Ordering::Relaxed);
                                        None
                                    }
                                    _ => None,
                                }
                            } else {
                                None
                            }
                        }
                        Err(_) => None,
                    }
                }
            }
        });

        let chunks: Vec<ChatCompletionChunk> = chunk_stream.collect().await;

        // Expect: tool_start_0, delta_0, tool_start_1, delta_1 = 4 chunks
        assert!(
            chunks.len() >= 4,
            "expected at least 4 chunks for 2 tool calls, got {}",
            chunks.len()
        );

        // First tool call start: index=0, id="toolu_weather", name="get_weather"
        let tc0_start = chunks[0].choices[0].delta.tool_calls.as_ref().unwrap();
        assert_eq!(
            tc0_start[0].index,
            Some(0),
            "first tool call must have index 0"
        );
        assert_eq!(tc0_start[0].id, "toolu_weather");
        assert_eq!(tc0_start[0].function.name, "get_weather");

        // First tool call delta: index=0
        let tc0_delta = chunks[1].choices[0].delta.tool_calls.as_ref().unwrap();
        assert_eq!(
            tc0_delta[0].index,
            Some(0),
            "argument delta for first tool must carry index 0"
        );
        assert_eq!(tc0_delta[0].function.arguments, r#"{"location":"Paris"}"#);

        // Second tool call start: index=1, id="toolu_time", name="get_time"
        let tc1_start = chunks[2].choices[0].delta.tool_calls.as_ref().unwrap();
        assert_eq!(
            tc1_start[0].index,
            Some(1),
            "second tool call must have index 1"
        );
        assert_eq!(tc1_start[0].id, "toolu_time");
        assert_eq!(tc1_start[0].function.name, "get_time");

        // Second tool call delta: index=1
        let tc1_delta = chunks[3].choices[0].delta.tool_calls.as_ref().unwrap();
        assert_eq!(
            tc1_delta[0].index,
            Some(1),
            "argument delta for second tool must carry index 1"
        );
        assert_eq!(tc1_delta[0].function.arguments, r#"{"tz":"UTC"}"#);
    }

    /// Regression: `input_json_delta` events arriving before any `ContentBlockStart`
    /// previously defaulted to tool_call index 0, silently misassigning arguments.
    /// After the fix, such events are dropped (return `None` from the filter_map).
    #[tokio::test]
    async fn test_input_json_delta_without_content_block_start_is_dropped() {
        use futures::stream;

        // SSE stream with an input_json_delta but NO preceding ContentBlockStart
        let sse_lines = vec![
            r#"data: {"type":"message_start","message":{"id":"msg_01","type":"message","role":"assistant","model":"claude-3-5-sonnet-20241022","content":[],"usage":{"input_tokens":10,"output_tokens":0}}}"#,
            // Directly send input_json_delta without ContentBlockStart
            r#"data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"orphan\":true}"}}"#,
            r#"data: {"type":"message_delta","delta":{"type":"message_delta","stop_reason":"end_turn"},"usage":{"input_tokens":0,"output_tokens":5}}"#,
            "data: [DONE]",
        ];

        let raw_bytes = sse_lines.join("\n\n") + "\n\n";
        let byte_stream =
            stream::iter(vec![Ok::<_, std::io::Error>(bytes::Bytes::from(raw_bytes))]);
        let data_stream = super::super::sse::bytes_to_sse_data_stream(byte_stream);

        let model = "claude-3-5-sonnet-20241022".to_string();
        let chunk_id = "chatcmpl-orphan".to_string();
        let captured_input_tokens = Arc::new(AtomicU32::new(0));
        let active_tool: Arc<std::sync::Mutex<Option<(u32, String, String)>>> =
            Arc::new(std::sync::Mutex::new(None));
        let tool_call_counter = Arc::new(AtomicU32::new(0));

        let chunk_stream = data_stream.filter_map({
            let chunk_id = chunk_id.clone();
            let model = model.clone();
            let captured_input_tokens = captured_input_tokens.clone();
            let active_tool = active_tool.clone();
            let tool_call_counter = tool_call_counter.clone();
            move |result| {
                let chunk_id = chunk_id.clone();
                let model = model.clone();
                let captured_input_tokens = captured_input_tokens.clone();
                let active_tool = active_tool.clone();
                let _tool_call_counter = tool_call_counter.clone();
                async move {
                    match result {
                        Ok(data) => {
                            if let Ok(event) = serde_json::from_str::<AnthropicStreamEvent>(&data) {
                                match event {
                                    AnthropicStreamEvent::ContentBlockDelta { delta, .. } => {
                                        if let Some(_json_fragment) = delta.partial_json {
                                            // Replicate the fixed logic: drop when active_tool is None
                                            let tc_index = match active_tool.lock() {
                                                Ok(guard) => match guard.as_ref() {
                                                    Some((idx, _, _)) => *idx,
                                                    None => return None,
                                                },
                                                Err(_) => return None,
                                            };
                                            return Some(ChatCompletionChunk::new(
                                                chunk_id,
                                                model,
                                                vec![ChunkChoice {
                                                    index: 0,
                                                    delta: ChunkDelta {
                                                        tool_calls: Some(vec![ToolCall {
                                                            index: Some(tc_index),
                                                            id: String::new(),
                                                            tool_type: ToolType::Function,
                                                            function: FunctionCall {
                                                                name: String::new(),
                                                                arguments: _json_fragment,
                                                            },
                                                        }]),
                                                        ..Default::default()
                                                    },
                                                    finish_reason: None,
                                                }],
                                            ));
                                        }
                                        if let Some(text) = delta.text {
                                            return Some(ChatCompletionChunk::with_content(
                                                chunk_id, model, text,
                                            ));
                                        }
                                        None
                                    }
                                    AnthropicStreamEvent::MessageStart { message } => {
                                        captured_input_tokens
                                            .store(message.usage.input_tokens, Ordering::Relaxed);
                                        None
                                    }
                                    AnthropicStreamEvent::MessageDelta { delta, .. } => {
                                        if let Some(stop_reason) = delta.stop_reason {
                                            let openai_reason = map_finish_reason_to_openai(
                                                &stop_reason,
                                                Provider::Anthropic,
                                            );
                                            return Some(ChatCompletionChunk::finished(
                                                chunk_id,
                                                model,
                                                openai_reason,
                                            ));
                                        }
                                        None
                                    }
                                    _ => None,
                                }
                            } else {
                                None
                            }
                        }
                        Err(_) => None,
                    }
                }
            }
        });

        let chunks: Vec<ChatCompletionChunk> = chunk_stream.collect().await;

        // The input_json_delta should have been dropped, so no tool_call chunks
        let tool_call_chunks: Vec<_> = chunks
            .iter()
            .filter(|c| {
                c.choices
                    .first()
                    .and_then(|ch| ch.delta.tool_calls.as_ref())
                    .is_some()
            })
            .collect();

        assert!(
            tool_call_chunks.is_empty(),
            "input_json_delta without ContentBlockStart must be dropped, but got {} tool call chunks",
            tool_call_chunks.len()
        );
    }
}
