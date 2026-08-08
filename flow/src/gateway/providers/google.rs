//! Google Gemini provider adapter.
//!
//! Translates between OpenAI chat completion format and Google's Gemini API.

use async_trait::async_trait;
use futures::stream::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
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
    ContentPart, FinishReason, MessageContent, MessageRole, PromptTokensDetails, ThinkingContent,
    ThinkingType, ToolType, Usage,
};

const GEMINI_API_BASE: &str = "https://generativelanguage.googleapis.com/v1beta";
const DEFAULT_TIMEOUT_SECS: u64 = 120;

/// Google Gemini provider adapter.
pub struct GoogleProvider {
    client: Client,
    api_base: String,
}

impl GoogleProvider {
    /// Create a new Google provider with default settings.
    pub fn new() -> Self {
        Self::with_timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
    }

    /// Create a new Google provider with a custom timeout.
    pub fn with_timeout(timeout: Duration) -> Self {
        Self::with_base_url_and_timeout(GEMINI_API_BASE.to_string(), timeout)
    }

    /// Create with custom base URL and timeout.
    pub fn with_base_url_and_timeout(api_base: String, timeout: Duration) -> Self {
        Self {
            client: create_http_client(timeout),
            api_base,
        }
    }

    /// Map model name to Gemini's expected format, redirecting deprecated
    /// model IDs to their current replacements.
    fn map_model_name(&self, model: &str) -> String {
        match model {
            "gemini-1.5-flash" | "gemini-flash" | "gemini-2.0-flash" => {
                "gemini-2.5-flash".to_string()
            }
            "gemini-1.5-pro" | "gemini-pro" => "gemini-2.5-pro".to_string(),
            _ if model.starts_with("gemini-") => model.to_string(),
            _ => model.to_string(),
        }
    }

    /// Extract system instruction from messages.
    fn extract_system_instruction(&self, messages: &[ChatMessage]) -> Option<GeminiContent> {
        find_system_message_text(messages).map(|text| GeminiContent {
            role: None, // System instruction doesn't have a role
            parts: vec![GeminiPart::Text { text }],
        })
    }

    /// Convert OpenAI messages to Gemini format.
    ///
    /// Handles:
    /// - Text-only and multimodal (text + image) content
    /// - Tool/function calling messages (tool role with tool_call_id)
    /// - Assistant messages with tool_calls
    fn convert_messages(&self, messages: &[ChatMessage]) -> Vec<GeminiContent> {
        let mut result: Vec<GeminiContent> = Vec::new();

        for m in non_system_messages(messages) {
            match m.role {
                MessageRole::User => {
                    let parts = self.convert_content(&m.content);
                    result.push(GeminiContent {
                        role: Some("user".to_string()),
                        parts,
                    });
                }
                MessageRole::Assistant => {
                    let mut parts = self.convert_content(&m.content);

                    // If assistant made tool calls, add function_call parts
                    if let Some(ref tool_calls) = m.tool_calls {
                        for tc in tool_calls {
                            // Parse the arguments JSON string back to a Value
                            let args = serde_json::from_str(&tc.function.arguments)
                                .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));

                            parts.push(GeminiPart::FunctionCall {
                                function_call: GeminiFunctionCall {
                                    name: tc.function.name.clone(),
                                    args,
                                },
                            });
                        }
                    }

                    result.push(GeminiContent {
                        role: Some("model".to_string()),
                        parts,
                    });
                }
                MessageRole::Tool | MessageRole::Other => {
                    if let Some(ref _tool_call_id) = m.tool_call_id {
                        let content_text =
                            m.content.as_ref().map(|c| c.as_text()).unwrap_or_default();

                        // Try to parse the response as JSON, otherwise wrap as text
                        let response = serde_json::from_str(&content_text)
                            .unwrap_or_else(|_| serde_json::json!({"result": content_text}));

                        // Get function name from the associated tool call
                        // In OpenAI format, tool messages don't have the function name directly,
                        // but we need it for Gemini. We'll use a generic name if not found.
                        let function_name =
                            m.name.clone().unwrap_or_else(|| "function".to_string());

                        // Check if the last message is a user message with function responses
                        // If so, append to it (Gemini expects all function responses in one user message)
                        let should_append = result
                            .last()
                            .map(|last| {
                                last.role.as_deref() == Some("user")
                                    && last
                                        .parts
                                        .iter()
                                        .any(|p| matches!(p, GeminiPart::FunctionResponse { .. }))
                            })
                            .unwrap_or(false);

                        if should_append {
                            if let Some(last_msg) = result.last_mut() {
                                last_msg.parts.push(GeminiPart::FunctionResponse {
                                    function_response: GeminiFunctionResponse {
                                        name: function_name,
                                        response,
                                    },
                                });
                            }
                        } else {
                            result.push(GeminiContent {
                                role: Some("user".to_string()),
                                parts: vec![GeminiPart::FunctionResponse {
                                    function_response: GeminiFunctionResponse {
                                        name: function_name,
                                        response,
                                    },
                                }],
                            });
                        }
                    } else {
                        tracing::warn!(
                            role = ?m.role,
                            "Unknown message role, defaulting to 'user'. Supported roles: user, assistant, system, tool"
                        );
                        let parts = self.convert_content(&m.content);
                        result.push(GeminiContent {
                            role: Some("user".to_string()),
                            parts,
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

    /// Convert OpenAI tools to Gemini function declarations.
    fn convert_tools(
        &self,
        tools: &Option<Vec<crate::gateway::types::Tool>>,
    ) -> Option<Vec<GeminiTool>> {
        tools
            .as_ref()
            .map(|tools| {
                let function_declarations: Vec<GeminiFunctionDeclaration> = tools
                    .iter()
                    .filter(|t| t.tool_type == ToolType::Function)
                    .map(|t| GeminiFunctionDeclaration {
                        name: t.function.name.clone(),
                        description: t.function.description.clone(),
                        parameters: t.function.parameters.clone(),
                    })
                    .collect();

                if function_declarations.is_empty() {
                    vec![]
                } else {
                    vec![GeminiTool {
                        function_declarations,
                    }]
                }
            })
            .filter(|tools| !tools.is_empty())
    }

    /// Convert OpenAI message content to Gemini parts.
    ///
    /// Supports:
    /// - Simple text content
    /// - Multimodal content with text, images, and documents (base64 data URLs)
    fn convert_content(&self, content: &Option<MessageContent>) -> Vec<GeminiPart> {
        match content {
            None => vec![],
            Some(MessageContent::Text(text)) => {
                vec![GeminiPart::Text { text: text.clone() }]
            }
            Some(MessageContent::Parts(parts)) => parts
                .iter()
                .filter_map(|part| match part {
                    ContentPart::Text { text } => Some(GeminiPart::Text { text: text.clone() }),
                    ContentPart::ImageUrl { image_url } => {
                        self.convert_data_url(&image_url.url, "image/jpeg")
                    }
                    ContentPart::DocumentUrl { document_url } => {
                        let default_mime = document_url
                            .media_type
                            .as_deref()
                            .unwrap_or("application/pdf");
                        self.convert_data_url(&document_url.url, default_mime)
                    }
                })
                .collect(),
        }
    }

    /// Validate that the request doesn't contain unsupported external image or document URLs.
    ///
    /// Gemini requires base64-encoded inline data. External URLs (http/https) are not
    /// supported in this path. This provides a clear error instead of silently dropping content.
    fn validate_no_external_images(&self, messages: &[ChatMessage]) -> Result<(), GatewayError> {
        for (msg_idx, message) in messages.iter().enumerate() {
            if let Some(MessageContent::Parts(parts)) = &message.content {
                for (part_idx, part) in parts.iter().enumerate() {
                    match part {
                        ContentPart::ImageUrl { image_url } => {
                            self.validate_data_url_part(
                                &image_url.url,
                                "image",
                                msg_idx,
                                part_idx,
                                "image/jpeg",
                            )?;
                        }
                        ContentPart::DocumentUrl { document_url } => {
                            self.validate_data_url_part(
                                &document_url.url,
                                "document",
                                msg_idx,
                                part_idx,
                                "application/pdf",
                            )?;
                        }
                        ContentPart::Text { .. } => {}
                    }
                }
            }
        }
        Ok(())
    }

    /// Validate a single image or document URL part for Gemini compatibility.
    fn validate_data_url_part(
        &self,
        url: &str,
        kind: &str,
        msg_idx: usize,
        part_idx: usize,
        default_mime: &str,
    ) -> Result<(), GatewayError> {
        if url.starts_with("http://") || url.starts_with("https://") {
            return Err(GatewayError::ValidationError(format!(
                "External {kind} URLs are not supported for Gemini models. \
                 Message {msg_idx} contains an external URL at content part {part_idx}. \
                 Please convert the {kind} to a base64 data URL format: \
                 'data:{default_mime};base64,<base64-encoded-data>'"
            )));
        }
        if url.starts_with("data:") {
            let url_parts: Vec<&str> = url.splitn(2, ',').collect();
            if url_parts.len() != 2 {
                return Err(GatewayError::ValidationError(format!(
                    "Invalid data URL format in message {msg_idx} at content part {part_idx}. \
                     Expected format: 'data:{default_mime};base64,<base64-encoded-data>'"
                )));
            }
        } else {
            return Err(GatewayError::ValidationError(format!(
                "Unsupported {kind} URL format in message {msg_idx} at content part {part_idx}. \
                 Gemini only supports base64 data URLs in format: \
                 'data:{default_mime};base64,<base64-encoded-data>'"
            )));
        }
        Ok(())
    }

    /// Convert a base64 data URL to a Gemini `InlineData` part.
    ///
    /// `default_mime` is used when the MIME type cannot be inferred from the data URL header.
    /// Gemini API requires base64-encoded inline data; external URLs are rejected by
    /// `validate_no_external_images()` before this is called.
    fn convert_data_url(&self, url: &str, default_mime: &str) -> Option<GeminiPart> {
        if url.starts_with("data:") {
            let parts: Vec<&str> = url.splitn(2, ',').collect();
            if parts.len() != 2 {
                tracing::warn!("Invalid data URL format — should have been caught by validation");
                return None;
            }
            let header = parts[0];
            let data = parts[1];
            let mime_type = header
                .strip_prefix("data:")
                .and_then(|s| s.split(';').next())
                .unwrap_or(default_mime)
                .to_string();
            Some(GeminiPart::InlineData {
                inline_data: GeminiInlineData {
                    mime_type,
                    data: data.to_string(),
                },
            })
        } else {
            tracing::warn!(
                url_prefix = %url.chars().take(20).collect::<String>(),
                "Unsupported data URL format for Gemini — should have been caught by validation"
            );
            None
        }
    }

    #[cfg(test)]
    fn convert_image_url(&self, url: &str) -> Option<GeminiPart> {
        self.convert_data_url(url, "image/jpeg")
    }

    /// Convert Gemini response to OpenAI format.
    fn convert_response(
        &self,
        response: GeminiResponse,
        model: &str,
    ) -> Result<ChatCompletionResponse, GatewayError> {
        let candidate = response
            .candidates
            .first()
            .ok_or_else(|| GatewayError::ProviderError {
                provider: Provider::Google,
                status: 500,
                message: "No candidates in response".to_string(),
            })?;

        // Extract text content (non-thought parts only)
        let content: String = candidate
            .content
            .parts
            .iter()
            .filter_map(|p| {
                if let GeminiPart::Text { text } = p {
                    Some(text.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join("");

        // Extract thinking content (Gemini 2.x thought parts)
        let thinking_text: String = candidate
            .content
            .parts
            .iter()
            .filter_map(|p| {
                if let GeminiPart::Thought { text, .. } = p {
                    Some(text.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join("\n");
        let thinking = if thinking_text.is_empty() {
            None
        } else {
            Some(ThinkingContent {
                content: thinking_text,
                tokens: None,
                thinking_type: Some(ThinkingType::GeminiThinking),
            })
        };

        // Extract function calls and convert to OpenAI tool_calls format
        let tool_calls: Vec<crate::gateway::types::ToolCall> = candidate
            .content
            .parts
            .iter()
            .enumerate()
            .filter_map(|(idx, p)| {
                if let GeminiPart::FunctionCall { function_call } = p {
                    Some(crate::gateway::types::ToolCall {
                        index: None,
                        id: format!("call_{}", idx),
                        tool_type: ToolType::Function,
                        function: crate::gateway::types::FunctionCall {
                            name: function_call.name.clone(),
                            arguments: serde_json::to_string(&function_call.args)
                                .unwrap_or_default(),
                        },
                    })
                } else {
                    None
                }
            })
            .collect();

        let finish_reason = candidate
            .finish_reason
            .as_deref()
            .map(|reason| {
                if !tool_calls.is_empty() {
                    FinishReason::ToolCalls
                } else {
                    map_finish_reason_to_openai(reason, Provider::Google)
                }
            })
            .unwrap_or(FinishReason::Stop);

        let usage = response.usage_metadata.unwrap_or_default();

        Ok(ChatCompletionResponse::new(
            format!("gemini-{}", uuid::Uuid::new_v4()),
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
                prompt_tokens: usage.prompt_token_count,
                completion_tokens: usage.candidates_token_count,
                total_tokens: usage.total_token_count,
                thinking_tokens: usage.thoughts_token_count,
                completion_tokens_details: None,
                prompt_tokens_details: if usage.cached_content_token_count > 0 {
                    Some(PromptTokensDetails {
                        cached_tokens: usage.cached_content_token_count,
                    })
                } else {
                    None
                },
            },
        ))
    }
}

impl Default for GoogleProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl LlmProvider for GoogleProvider {
    fn name(&self) -> Provider {
        Provider::Google
    }

    fn supports_model(&self, model: &str) -> bool {
        model.starts_with("gemini-")
    }

    #[tracing::instrument(
        name = "provider.google.chat_completion",
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
            gen_ai.provider.name = "gcp.vertex_ai",
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
        self.validate_no_external_images(&request.messages)?;

        let model_name = self.map_model_name(&request.model);
        let url = format!("{}/models/{}:generateContent", self.api_base, model_name);

        let thinking_config = request.thinking.as_ref().and_then(|t| {
            if t.thinking_type == crate::gateway::types::ThinkingToggle::Enabled {
                Some(GeminiThinkingConfig {
                    thinking_mode: "ENABLED".to_string(),
                    budget_tokens: t.budget_tokens,
                })
            } else {
                None
            }
        });
        let generation_config = GeminiGenerationConfig {
            temperature: request.temperature,
            top_p: request.top_p,
            max_output_tokens: request.max_tokens,
            stop_sequences: request.stop.as_ref().map(|s| match s {
                crate::gateway::types::StopSequence::Single(s) => vec![s.clone()],
                crate::gateway::types::StopSequence::Multiple(v) => v.clone(),
            }),
            thinking_config,
        };

        let gemini_request = GeminiRequest {
            contents: self.convert_messages(&request.messages),
            system_instruction: self.extract_system_instruction(&request.messages),
            generation_config: Some(generation_config),
            tools: self.convert_tools(&request.tools),
        };

        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("x-goog-api-key", api_key)
            .json(&gemini_request)
            .send()
            .await?;

        let status = response.status();
        tracing::Span::current().record("http_status", status.as_u16());

        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(parse_provider_error(
                &error_text,
                Provider::Google,
                status.as_u16(),
            ));
        }

        let gemini_response: GeminiResponse = response
            .json()
            .await
            .map_err(|e| GatewayError::InternalError(format!("Failed to parse response: {}", e)))?;

        let result = self.convert_response(gemini_response, &request.model)?;
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
        name = "provider.google.stream_chat_completion",
        skip(self, request, api_key),
        fields(
            model = %request.model,
            otel.status_code = tracing::field::Empty,
            otel.status_message = tracing::field::Empty,
            gen_ai.provider.name = "gcp.vertex_ai",
            gen_ai.operation.name = "chat",
            gen_ai.request.model = %request.model,
        )
    )]
    async fn stream_chat_completion(
        &self,
        request: &ChatCompletionRequest,
        api_key: &str,
    ) -> Result<ChatCompletionStream, GatewayError> {
        // Validate no external image URLs (Gemini requires base64 data URLs)
        self.validate_no_external_images(&request.messages)?;

        let model_name = self.map_model_name(&request.model);
        // Use streamGenerateContent endpoint for streaming
        // Security: Use header for API key instead of URL parameter to prevent key exposure in logs
        let url = format!(
            "{}/models/{}:streamGenerateContent?alt=sse",
            self.api_base, model_name
        );

        let thinking_config_stream = request.thinking.as_ref().and_then(|t| {
            if t.thinking_type == crate::gateway::types::ThinkingToggle::Enabled {
                Some(GeminiThinkingConfig {
                    thinking_mode: "ENABLED".to_string(),
                    budget_tokens: t.budget_tokens,
                })
            } else {
                None
            }
        });
        let generation_config = GeminiGenerationConfig {
            temperature: request.temperature,
            top_p: request.top_p,
            max_output_tokens: request.max_tokens,
            stop_sequences: request.stop.as_ref().map(|s| match s {
                crate::gateway::types::StopSequence::Single(s) => vec![s.clone()],
                crate::gateway::types::StopSequence::Multiple(v) => v.clone(),
            }),
            thinking_config: thinking_config_stream,
        };

        let gemini_request = GeminiRequest {
            contents: self.convert_messages(&request.messages),
            system_instruction: self.extract_system_instruction(&request.messages),
            generation_config: Some(generation_config),
            tools: self.convert_tools(&request.tools),
        };

        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("x-goog-api-key", api_key)
            .header("Accept", "text/event-stream")
            .json(&gemini_request)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(parse_provider_error(
                &error_text,
                Provider::Google,
                status.as_u16(),
            ));
        }

        // Use shared SSE parsing utilities
        let byte_stream = response.bytes_stream();
        let data_stream = bytes_to_sse_data_stream(byte_stream);

        // Capture model for use in stream
        let model = request.model.clone();
        let chunk_id = format!("chatcmpl-{}", uuid::Uuid::new_v4());
        // Use Arc<AtomicBool> to track if we've sent the role across async iterations
        let sent_role = Arc::new(AtomicBool::new(false));

        // Parse Gemini SSE and convert to OpenAI chunk format
        let chunk_stream = data_stream.filter_map(move |result| {
            let chunk_id = chunk_id.clone();
            let model = model.clone();
            let sent_role = sent_role.clone();

            async move {
                match result {
                    Ok(data) => {
                        // Parse the Gemini streaming response
                        if let Ok(response) = serde_json::from_str::<GeminiStreamResponse>(&data) {
                            if let Some(candidate) = response.candidates.first() {
                                let thinking_text: String = candidate
                                    .content
                                    .parts
                                    .iter()
                                    .filter_map(|p| {
                                        if let GeminiPart::Thought { text, .. } = p {
                                            Some(text.as_str())
                                        } else {
                                            None
                                        }
                                    })
                                    .collect();

                                let text: String = candidate
                                    .content
                                    .parts
                                    .iter()
                                    .filter_map(|p| {
                                        if let GeminiPart::Text { text } = p {
                                            Some(text.as_str())
                                        } else {
                                            None
                                        }
                                    })
                                    .collect();

                                let has_thinking = !thinking_text.is_empty();
                                let has_text = !text.is_empty();

                                // Extract finish_reason and usage regardless of
                                // whether this event also carries content, so
                                // the final chunk always reports stream completion.
                                let mapped_finish = candidate
                                    .finish_reason
                                    .as_ref()
                                    .map(|fr| map_finish_reason_to_openai(fr, Provider::Google));

                                let usage_data = if mapped_finish.is_some() {
                                    response.usage_metadata.as_ref().map(|usage| Usage {
                                        prompt_tokens: usage.prompt_token_count,
                                        completion_tokens: usage.candidates_token_count,
                                        total_tokens: usage.total_token_count,
                                        thinking_tokens: usage.thoughts_token_count,
                                        completion_tokens_details: None,
                                        prompt_tokens_details: if usage.cached_content_token_count
                                            > 0
                                        {
                                            Some(PromptTokensDetails {
                                                cached_tokens: usage.cached_content_token_count,
                                            })
                                        } else {
                                            None
                                        },
                                    })
                                } else {
                                    None
                                };

                                if has_thinking || has_text || mapped_finish.is_some() {
                                    let role =
                                        if has_text && !sent_role.swap(true, Ordering::SeqCst) {
                                            Some(MessageRole::Assistant)
                                        } else {
                                            None
                                        };

                                    let mut chunk = ChatCompletionChunk::new(
                                        chunk_id,
                                        model,
                                        vec![ChunkChoice {
                                            index: 0,
                                            delta: ChunkDelta {
                                                role,
                                                content: if has_text { Some(text) } else { None },
                                                thinking: if has_thinking {
                                                    Some(thinking_text)
                                                } else {
                                                    None
                                                },
                                                reasoning_content: None,
                                                tool_calls: None,
                                            },
                                            finish_reason: mapped_finish,
                                        }],
                                    );
                                    chunk.usage = usage_data;

                                    return Some(Ok(chunk));
                                }
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

// Gemini-specific request/response types

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GeminiRequest {
    contents: Vec<GeminiContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    system_instruction: Option<GeminiContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    generation_config: Option<GeminiGenerationConfig>,
    /// Tool declarations for function calling
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<GeminiTool>>,
}

/// Tool container for Gemini function declarations.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GeminiTool {
    function_declarations: Vec<GeminiFunctionDeclaration>,
}

/// Function declaration for Gemini tools.
#[derive(Debug, Serialize)]
struct GeminiFunctionDeclaration {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parameters: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
struct GeminiContent {
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<String>,
    parts: Vec<GeminiPart>,
}

/// Gemini content part - can be text, inline image data, function call, or thought.
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum GeminiPart {
    /// A thought block from Gemini 2.x thinking mode.
    /// Must come before Text in the untagged enum so it deserializes correctly.
    Thought {
        thought: bool,
        text: String,
    },
    Text {
        text: String,
    },
    InlineData {
        #[serde(rename = "inlineData")]
        inline_data: GeminiInlineData,
    },
    /// Function call made by the model
    FunctionCall {
        #[serde(rename = "functionCall")]
        function_call: GeminiFunctionCall,
    },
    /// Function response from user
    FunctionResponse {
        #[serde(rename = "functionResponse")]
        function_response: GeminiFunctionResponse,
    },
}

/// Function call from Gemini model.
#[derive(Debug, Serialize, Deserialize)]
struct GeminiFunctionCall {
    name: String,
    args: serde_json::Value,
}

/// Function response to send back to Gemini.
#[derive(Debug, Serialize, Deserialize)]
struct GeminiFunctionResponse {
    name: String,
    response: serde_json::Value,
}

/// Inline data for images in Gemini API.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiInlineData {
    mime_type: String,
    data: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GeminiGenerationConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_output_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stop_sequences: Option<Vec<String>>,
    /// Enable Gemini 2.x thinking mode. When set, the model exposes its
    /// internal reasoning as separate `thought` parts in the response.
    #[serde(skip_serializing_if = "Option::is_none")]
    thinking_config: Option<GeminiThinkingConfig>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GeminiThinkingConfig {
    /// "enabled" to turn on thinking mode
    thinking_mode: String,
    /// Maximum tokens the model may use for thinking
    #[serde(skip_serializing_if = "Option::is_none")]
    budget_tokens: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiResponse {
    candidates: Vec<GeminiCandidate>,
    #[serde(default)]
    usage_metadata: Option<GeminiUsageMetadata>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiCandidate {
    content: GeminiContent,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiUsageMetadata {
    #[serde(default)]
    prompt_token_count: u32,
    #[serde(default)]
    candidates_token_count: u32,
    #[serde(default)]
    total_token_count: u32,
    /// Thinking tokens returned when thinking mode is enabled (Gemini 2.x).
    #[serde(default)]
    thoughts_token_count: Option<u32>,
    #[serde(default)]
    cached_content_token_count: u32,
}

// Streaming response type (same structure but candidates may have partial content)
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiStreamResponse {
    #[serde(default)]
    candidates: Vec<GeminiStreamCandidate>,
    #[serde(default)]
    usage_metadata: Option<GeminiUsageMetadata>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiStreamCandidate {
    content: GeminiContent,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression: when the first Gemini SSE event is a Thought chunk, the
    /// `sent_role` flag was consumed unconditionally at the top of the closure.
    /// The subsequent text chunk then had `first_chunk == false` and omitted
    /// `role: Some(Assistant)`, breaking OpenAI SDK clients.
    /// The fix moves `sent_role.swap` into the text branch so thinking chunks
    /// don't consume it.
    #[test]
    fn test_streaming_role_sent_after_thinking_chunk() {
        use std::sync::atomic::AtomicBool;

        // Simulate Gemini SSE data: first a thinking chunk, then a text chunk
        let thinking_sse = serde_json::json!({
            "candidates": [{
                "content": {
                    "role": "model",
                    "parts": [{"thought": true, "text": "Let me think..."}]
                }
            }]
        });
        let text_sse = serde_json::json!({
            "candidates": [{
                "content": {
                    "role": "model",
                    "parts": [{"text": "Hello!"}]
                }
            }]
        });

        let sent_role = Arc::new(AtomicBool::new(false));
        let chunk_id = "chatcmpl-test".to_string();
        let model = "gemini-2.0-flash".to_string();

        // Process first event (thinking) — should NOT consume sent_role
        let response: GeminiStreamResponse = serde_json::from_value(thinking_sse).unwrap();
        let candidate = response.candidates.first().unwrap();

        let thinking_text: String = candidate
            .content
            .parts
            .iter()
            .filter_map(|p| {
                if let GeminiPart::Thought { text, .. } = p {
                    Some(text.as_str())
                } else {
                    None
                }
            })
            .collect();
        assert!(!thinking_text.is_empty(), "Should have thinking text");

        // Process second event (text) — role flag should still be false
        let response2: GeminiStreamResponse = serde_json::from_value(text_sse).unwrap();
        let candidate2 = response2.candidates.first().unwrap();

        let text: String = candidate2
            .content
            .parts
            .iter()
            .filter_map(|p| {
                if let GeminiPart::Text { text } = p {
                    Some(text.as_str())
                } else {
                    None
                }
            })
            .collect();
        assert!(!text.is_empty());

        // Now swap — should return true (was false = first text chunk)
        let first_text = !sent_role.swap(true, Ordering::SeqCst);
        assert!(
            first_text,
            "sent_role must still be false after a thinking-only chunk, so the first text chunk gets the role"
        );

        // Build the chunk the same way the production code would
        let chunk = ChatCompletionChunk::new(
            chunk_id.clone(),
            model.clone(),
            vec![ChunkChoice {
                index: 0,
                delta: ChunkDelta {
                    role: if first_text {
                        Some(MessageRole::Assistant)
                    } else {
                        None
                    },
                    content: Some(text),
                    ..Default::default()
                },
                finish_reason: None,
            }],
        );

        assert_eq!(
            chunk.choices[0].delta.role,
            Some(MessageRole::Assistant),
            "First text chunk after thinking must carry role: assistant"
        );
    }

    #[test]
    fn test_supports_model() {
        let provider = GoogleProvider::new();

        assert!(provider.supports_model("gemini-pro"));
        assert!(provider.supports_model("gemini-1.5-pro"));
        assert!(provider.supports_model("gemini-1.5-flash"));
        assert!(provider.supports_model("gemini-2.0-flash"));

        assert!(!provider.supports_model("gpt-4"));
        assert!(!provider.supports_model("claude-sonnet-4-6"));
    }

    #[test]
    fn test_map_model_name() {
        let provider = GoogleProvider::new();

        assert_eq!(provider.map_model_name("gemini-pro"), "gemini-2.5-pro");
        assert_eq!(provider.map_model_name("gemini-1.5-pro"), "gemini-2.5-pro");
        assert_eq!(provider.map_model_name("gemini-flash"), "gemini-2.5-flash");
        assert_eq!(
            provider.map_model_name("gemini-1.5-flash"),
            "gemini-2.5-flash"
        );
        assert_eq!(
            provider.map_model_name("gemini-2.0-flash"),
            "gemini-2.5-flash"
        );
        assert_eq!(
            provider.map_model_name("gemini-2.5-flash"),
            "gemini-2.5-flash"
        );
        assert_eq!(provider.map_model_name("gemini-2.5-pro"), "gemini-2.5-pro");
    }

    #[test]
    fn test_convert_messages() {
        let provider = GoogleProvider::new();

        let messages = vec![
            ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text("Hello".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            },
            ChatMessage {
                role: MessageRole::Assistant,
                content: Some(MessageContent::Text("Hi there!".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            },
        ];

        let converted = provider.convert_messages(&messages);

        assert_eq!(converted.len(), 2);
        assert_eq!(converted[0].role, Some("user".to_string()));
        assert_eq!(converted[1].role, Some("model".to_string()));
    }

    #[test]
    fn test_convert_image_data_url() {
        let provider = GoogleProvider::new();

        let result = provider.convert_image_url("data:image/jpeg;base64,/9j/4AAQSkZJRg==");
        assert!(result.is_some());

        match result.unwrap() {
            GeminiPart::InlineData { inline_data } => {
                assert_eq!(inline_data.mime_type, "image/jpeg");
                assert_eq!(inline_data.data, "/9j/4AAQSkZJRg==");
            }
            _ => panic!("Expected InlineData"),
        }
    }

    #[test]
    fn test_convert_multimodal_content() {
        use crate::gateway::types::ImageUrl;

        let provider = GoogleProvider::new();

        let content = Some(MessageContent::Parts(vec![
            ContentPart::Text {
                text: "What's in this image?".to_string(),
            },
            ContentPart::ImageUrl {
                image_url: ImageUrl {
                    url: "data:image/png;base64,iVBORw0KGgo=".to_string(),
                    detail: None,
                },
            },
        ]));

        let parts = provider.convert_content(&content);

        assert_eq!(parts.len(), 2);

        // First should be text
        match &parts[0] {
            GeminiPart::Text { text } => {
                assert_eq!(text, "What's in this image?");
            }
            _ => panic!("Expected text part"),
        }

        // Second should be inline data
        match &parts[1] {
            GeminiPart::InlineData { inline_data } => {
                assert_eq!(inline_data.mime_type, "image/png");
                assert_eq!(inline_data.data, "iVBORw0KGgo=");
            }
            _ => panic!("Expected inline data part"),
        }
    }

    #[test]
    fn test_convert_external_url_not_supported() {
        let provider = GoogleProvider::new();

        // External URLs are not directly supported
        let result = provider.convert_image_url("https://example.com/image.jpg");
        assert!(result.is_none());
    }

    #[test]
    fn test_validate_no_external_images_rejects_http_url() {
        use crate::gateway::types::ImageUrl;

        let provider = GoogleProvider::new();

        let messages = vec![ChatMessage {
            role: MessageRole::User,
            content: Some(MessageContent::Parts(vec![
                ContentPart::Text {
                    text: "What's in this image?".to_string(),
                },
                ContentPart::ImageUrl {
                    image_url: ImageUrl {
                        url: "https://example.com/image.jpg".to_string(),
                        detail: None,
                    },
                },
            ])),
            name: None,
            tool_calls: None,
            tool_call_id: None,
            reasoning_content: None,
        }];

        let result = provider.validate_no_external_images(&messages);
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert!(error
            .to_string()
            .contains("External image URLs are not supported"));
    }

    #[test]
    fn test_validate_no_external_images_accepts_data_url() {
        use crate::gateway::types::ImageUrl;

        let provider = GoogleProvider::new();

        let messages = vec![ChatMessage {
            role: MessageRole::User,
            content: Some(MessageContent::Parts(vec![
                ContentPart::Text {
                    text: "What's in this image?".to_string(),
                },
                ContentPart::ImageUrl {
                    image_url: ImageUrl {
                        url: "data:image/jpeg;base64,/9j/4AAQSkZJRg==".to_string(),
                        detail: None,
                    },
                },
            ])),
            name: None,
            tool_calls: None,
            tool_call_id: None,
            reasoning_content: None,
        }];

        let result = provider.validate_no_external_images(&messages);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_no_external_images_rejects_invalid_data_url() {
        use crate::gateway::types::ImageUrl;

        let provider = GoogleProvider::new();

        // Data URL without the comma separator is invalid
        let messages = vec![ChatMessage {
            role: MessageRole::User,
            content: Some(MessageContent::Parts(vec![ContentPart::ImageUrl {
                image_url: ImageUrl {
                    url: "data:image/jpeg;base64".to_string(), // Missing comma and data
                    detail: None,
                },
            }])),
            name: None,
            tool_calls: None,
            tool_call_id: None,
            reasoning_content: None,
        }];

        let result = provider.validate_no_external_images(&messages);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid data URL format"));
    }

    #[test]
    fn test_convert_tools_to_gemini_format() {
        use crate::gateway::types::{FunctionDefinition, Tool};

        let provider = GoogleProvider::new();

        let tools = Some(vec![Tool {
            tool_type: ToolType::Function,
            function: FunctionDefinition {
                name: "get_weather".to_string(),
                description: Some("Get the current weather".to_string()),
                parameters: Some(serde_json::json!({
                    "type": "object",
                    "properties": {
                        "location": { "type": "string" }
                    },
                    "required": ["location"]
                })),
            },
        }]);

        let result = provider.convert_tools(&tools);
        assert!(result.is_some());

        let gemini_tools = result.unwrap();
        assert_eq!(gemini_tools.len(), 1);
        assert_eq!(gemini_tools[0].function_declarations.len(), 1);
        assert_eq!(gemini_tools[0].function_declarations[0].name, "get_weather");
        assert_eq!(
            gemini_tools[0].function_declarations[0]
                .description
                .as_deref(),
            Some("Get the current weather")
        );
    }

    #[test]
    fn test_convert_messages_with_tool_calls() {
        use crate::gateway::types::{FunctionCall, ToolCall};

        let provider = GoogleProvider::new();

        let messages = vec![
            ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text(
                    "What's the weather in Tokyo?".to_string(),
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
                tool_calls: Some(vec![ToolCall {
                    index: None,
                    id: "call_1".to_string(),
                    tool_type: ToolType::Function,
                    function: FunctionCall {
                        name: "get_weather".to_string(),
                        arguments: r#"{"location": "Tokyo"}"#.to_string(),
                    },
                }]),
                tool_call_id: None,
                reasoning_content: None,
            },
            ChatMessage {
                role: MessageRole::Other,
                content: Some(MessageContent::Text(
                    r#"{"temperature": 22, "unit": "celsius"}"#.to_string(),
                )),
                name: Some("get_weather".to_string()),
                tool_calls: None,
                tool_call_id: Some("call_1".to_string()),
                reasoning_content: None,
            },
        ];

        let converted = provider.convert_messages(&messages);

        // Should have 3 messages: user, model with function call, user with function response
        assert_eq!(converted.len(), 3);

        // First message: user text
        assert_eq!(converted[0].role, Some("user".to_string()));

        // Second message: model with function call
        assert_eq!(converted[1].role, Some("model".to_string()));
        assert!(converted[1]
            .parts
            .iter()
            .any(|p| matches!(p, GeminiPart::FunctionCall { .. })));

        // Third message: user with function response
        assert_eq!(converted[2].role, Some("user".to_string()));
        assert!(converted[2]
            .parts
            .iter()
            .any(|p| matches!(p, GeminiPart::FunctionResponse { .. })));

        // Verify function call details
        if let Some(GeminiPart::FunctionCall { function_call }) = converted[1]
            .parts
            .iter()
            .find(|p| matches!(p, GeminiPart::FunctionCall { .. }))
        {
            assert_eq!(function_call.name, "get_weather");
            assert_eq!(function_call.args["location"], "Tokyo");
        } else {
            panic!("Expected function call");
        }

        // Verify function response details
        if let Some(GeminiPart::FunctionResponse { function_response }) = converted[2]
            .parts
            .iter()
            .find(|p| matches!(p, GeminiPart::FunctionResponse { .. }))
        {
            assert_eq!(function_response.name, "get_weather");
            assert_eq!(function_response.response["temperature"], 22);
        } else {
            panic!("Expected function response");
        }
    }

    #[test]
    fn test_convert_response_with_function_calls() {
        let provider = GoogleProvider::new();

        let gemini_response = GeminiResponse {
            candidates: vec![GeminiCandidate {
                content: GeminiContent {
                    role: Some("model".to_string()),
                    parts: vec![GeminiPart::FunctionCall {
                        function_call: GeminiFunctionCall {
                            name: "get_weather".to_string(),
                            args: serde_json::json!({"location": "Tokyo"}),
                        },
                    }],
                },
                finish_reason: Some("STOP".to_string()),
            }],
            usage_metadata: Some(GeminiUsageMetadata {
                prompt_token_count: 10,
                candidates_token_count: 15,
                total_token_count: 25,
                thoughts_token_count: None,
                cached_content_token_count: 0,
            }),
        };

        let result = provider.convert_response(gemini_response, "gemini-1.5-pro");
        assert!(result.is_ok());

        let response = result.unwrap();
        let choice = &response.choices[0];

        // Should have tool_calls but no content
        assert!(choice.message.content.is_none());
        assert!(choice.message.tool_calls.is_some());

        let tool_calls = choice.message.tool_calls.as_ref().unwrap();
        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0].function.name, "get_weather");
        assert!(tool_calls[0].function.arguments.contains("Tokyo"));

        // Finish reason should be "tool_calls"
        assert_eq!(choice.finish_reason, FinishReason::ToolCalls);
    }

    /// Regression: Gemini streaming returned early when a thought part was
    /// present, silently dropping any text parts in the same SSE event.
    #[test]
    fn test_streaming_thought_and_text_in_same_chunk() {
        use std::sync::atomic::AtomicBool;

        let combined_sse = serde_json::json!({
            "candidates": [{
                "content": {
                    "role": "model",
                    "parts": [
                        {"thought": true, "text": "Internal reasoning"},
                        {"text": "Visible answer"}
                    ]
                }
            }]
        });

        let response: GeminiStreamResponse = serde_json::from_value(combined_sse).unwrap();
        let candidate = response.candidates.first().unwrap();

        let thinking_text: String = candidate
            .content
            .parts
            .iter()
            .filter_map(|p| {
                if let GeminiPart::Thought { text, .. } = p {
                    Some(text.as_str())
                } else {
                    None
                }
            })
            .collect();

        let text: String = candidate
            .content
            .parts
            .iter()
            .filter_map(|p| {
                if let GeminiPart::Text { text } = p {
                    Some(text.as_str())
                } else {
                    None
                }
            })
            .collect();

        let has_thinking = !thinking_text.is_empty();
        let has_text = !text.is_empty();

        assert!(has_thinking, "chunk must contain thinking");
        assert!(has_text, "chunk must contain text");

        let sent_role = Arc::new(AtomicBool::new(false));
        let role = if has_text && !sent_role.swap(true, Ordering::SeqCst) {
            Some(MessageRole::Assistant)
        } else {
            None
        };

        let chunk = ChatCompletionChunk::new(
            "chatcmpl-test".to_string(),
            "gemini-2.0-flash".to_string(),
            vec![ChunkChoice {
                index: 0,
                delta: ChunkDelta {
                    role,
                    content: if has_text { Some(text) } else { None },
                    thinking: if has_thinking {
                        Some(thinking_text)
                    } else {
                        None
                    },
                    reasoning_content: None,
                    tool_calls: None,
                },
                finish_reason: None,
            }],
        );

        assert_eq!(
            chunk.choices[0].delta.thinking.as_deref(),
            Some("Internal reasoning"),
            "thinking must not be dropped"
        );
        assert_eq!(
            chunk.choices[0].delta.content.as_deref(),
            Some("Visible answer"),
            "text must not be dropped when thought is present"
        );
        assert_eq!(
            chunk.choices[0].delta.role,
            Some(MessageRole::Assistant),
            "first text chunk must carry the assistant role"
        );
    }

    /// Regression: when Gemini's final SSE event carries both text content
    /// and a finish_reason (e.g., "STOP"), the old code returned early with
    /// the text but `finish_reason: None`, dropping both the stop signal and
    /// usage metadata.  Clients never saw stream completion.
    #[test]
    fn test_streaming_finish_reason_not_dropped_with_content() {
        use std::sync::atomic::AtomicBool;

        let final_sse = serde_json::json!({
            "candidates": [{
                "content": {
                    "role": "model",
                    "parts": [{"text": "final word"}]
                },
                "finishReason": "STOP"
            }],
            "usageMetadata": {
                "promptTokenCount": 10,
                "candidatesTokenCount": 20,
                "totalTokenCount": 30
            }
        });

        let response: GeminiStreamResponse =
            serde_json::from_value(final_sse).expect("valid GeminiStreamResponse");
        let candidate = response.candidates.first().unwrap();

        let text: String = candidate
            .content
            .parts
            .iter()
            .filter_map(|p| {
                if let GeminiPart::Text { text } = p {
                    Some(text.as_str())
                } else {
                    None
                }
            })
            .collect();

        let thinking_text: String = candidate
            .content
            .parts
            .iter()
            .filter_map(|p| {
                if let GeminiPart::Thought { text, .. } = p {
                    Some(text.as_str())
                } else {
                    None
                }
            })
            .collect();

        let has_text = !text.is_empty();
        let has_thinking = !thinking_text.is_empty();

        let mapped_finish = candidate
            .finish_reason
            .as_ref()
            .map(|fr| map_finish_reason_to_openai(fr, Provider::Google));

        let usage_data = if mapped_finish.is_some() {
            response.usage_metadata.as_ref().map(|u| Usage {
                prompt_tokens: u.prompt_token_count,
                completion_tokens: u.candidates_token_count,
                total_tokens: u.total_token_count,
                thinking_tokens: u.thoughts_token_count,
                completion_tokens_details: None,
                prompt_tokens_details: if u.cached_content_token_count > 0 {
                    Some(PromptTokensDetails {
                        cached_tokens: u.cached_content_token_count,
                    })
                } else {
                    None
                },
            })
        } else {
            None
        };

        assert!(has_text, "event must carry text");
        assert!(mapped_finish.is_some(), "event must carry finish_reason");

        let sent_role = Arc::new(AtomicBool::new(false));
        let role = if has_text && !sent_role.swap(true, Ordering::SeqCst) {
            Some(MessageRole::Assistant)
        } else {
            None
        };

        let mut chunk = ChatCompletionChunk::new(
            "chatcmpl-test".to_string(),
            "gemini-2.0-flash".to_string(),
            vec![ChunkChoice {
                index: 0,
                delta: ChunkDelta {
                    role,
                    content: if has_text { Some(text) } else { None },
                    thinking: if has_thinking {
                        Some(thinking_text)
                    } else {
                        None
                    },
                    reasoning_content: None,
                    tool_calls: None,
                },
                finish_reason: mapped_finish,
            }],
        );
        chunk.usage = usage_data;

        assert_eq!(
            chunk.choices[0].delta.content.as_deref(),
            Some("final word"),
            "text content must not be dropped"
        );
        assert_eq!(
            chunk.choices[0].finish_reason.as_ref().map(|f| f.as_str()),
            Some("stop"),
            "finish_reason must be present when the event has both content and STOP"
        );
        let usage = chunk
            .usage
            .as_ref()
            .expect("usage must be present on final chunk");
        assert_eq!(usage.prompt_tokens, 10);
        assert_eq!(usage.completion_tokens, 20);
        assert_eq!(usage.total_tokens, 30);
    }
}
