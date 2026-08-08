//! Theta EdgeCloud On-Demand API provider adapter.
//!
//! Calls the Theta EdgeCloud on-demand inference API at
//! `https://ondemand.thetaedgecloud.com`. This is a serverless,
//! pay-per-use API — users only need an API key (no deployment setup).
//!
//! Model identifiers are prefixed with `theta/` (e.g. `theta/llama_3_1_70b`).
//! The prefix is stripped to derive the on-demand service alias.
//! Aliases use underscores to match Theta's service catalog.
//!
//! ## Configuration
//!
//! | Env var | Description |
//! |---|---|
//! | `GATEWAY_TIMEOUT_THETA_SECONDS` | Overall timeout including poll time (default: 120) |
//!
//! The API key is stored in `project_settings` under `gateway_theta_api_key`
//! via the standard integrations UI.
//!
//! ## API Reference
//!
//! - Run inference: `POST /infer_request/{service_alias}?wait=N`
//! - Check status:  `GET  /infer_request/{request_id}`
//! - List services: `GET  /service/list`
//!
//! Auth: `Authorization: Bearer <api_key>`
//! Docs: <https://docs.thetatoken.org/docs/edgecloud-on-demand-model-apis>

use async_trait::async_trait;
use futures::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use super::sse::{bytes_to_sse_data_stream, parse_sse_json};
use super::{ChatCompletionStream, LlmProvider};
use crate::gateway::error::GatewayError;
use crate::gateway::provider_types::Provider;
use crate::gateway::types::{
    AssistantMessage, ChatCompletionChunk, ChatCompletionRequest, ChatCompletionResponse, Choice,
    FinishReason, MessageRole, Usage,
};

const BASE_URL: &str = "https://ondemand.thetaedgecloud.com";
const DEFAULT_TIMEOUT_SECS: u64 = 120;
/// Seconds to block on the initial inference request before polling.
const INITIAL_WAIT_SECS: u64 = 60;
/// Seconds between poll attempts for a pending inference request.
const POLL_INTERVAL: Duration = Duration::from_secs(3);

/// Theta EdgeCloud on-demand inference provider.
pub struct ThetaProvider {
    client: reqwest::Client,
    base_url: String,
    timeout: Duration,
}

impl ThetaProvider {
    pub fn new(timeout: Duration) -> Self {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .unwrap_or_default();
        Self {
            client,
            base_url: BASE_URL.to_string(),
            timeout,
        }
    }

    pub fn with_default_timeout() -> Self {
        Self::new(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
    }

    /// Create a provider pointing at a custom base URL (for integration tests).
    pub fn with_base_url(base_url: String, timeout: Duration) -> Self {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .unwrap_or_default();
        Self {
            client,
            base_url,
            timeout,
        }
    }

    /// Strip the `theta/` namespace prefix from a model name to get the service alias.
    ///
    /// `"theta/llama_3_1_70b"` → `"llama_3_1_70b"`
    fn service_alias(model: &str) -> &str {
        model.strip_prefix("theta/").unwrap_or(model)
    }
}

// ---------------------------------------------------------------------------
// Theta on-demand API request/response types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct ThetaInferBody {
    input: ThetaInferInput,
}

#[derive(Debug, Serialize)]
struct ThetaInferInput {
    messages: Vec<ThetaMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_options: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct ThetaMessage {
    role: String,
    content: String,
}

#[derive(Debug, Deserialize)]
struct ThetaApiResponse {
    body: ThetaResponseBody,
}

#[derive(Debug, Deserialize)]
struct ThetaResponseBody {
    infer_requests: Vec<ThetaInferRequest>,
}

#[derive(Debug, Deserialize)]
struct ThetaInferRequest {
    id: String,
    state: String,
    #[serde(default)]
    output: Option<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// LlmProvider implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl LlmProvider for ThetaProvider {
    fn name(&self) -> Provider {
        Provider::Theta
    }

    fn supports_model(&self, model: &str) -> bool {
        model.starts_with("theta/")
    }

    fn supports_streaming(&self, _model: &str) -> bool {
        true
    }

    #[tracing::instrument(
        name = "provider.theta.chat_completion",
        skip(self, request, api_key),
        fields(
            model = %request.model,
            message_count = request.messages.len(),
            gen_ai.provider.name = "theta",
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
        let alias = Self::service_alias(&request.model);

        let messages: Vec<ThetaMessage> = request
            .messages
            .iter()
            .map(|m| ThetaMessage {
                role: match m.role {
                    MessageRole::System => "system".to_string(),
                    MessageRole::User => "user".to_string(),
                    MessageRole::Assistant => "assistant".to_string(),
                    MessageRole::Tool => "tool".to_string(),
                    MessageRole::Other => "user".to_string(),
                },
                content: m.content.as_ref().map(|c| c.as_text()).unwrap_or_default(),
            })
            .collect();

        let body = ThetaInferBody {
            input: ThetaInferInput {
                messages,
                max_tokens: request.max_tokens,
                temperature: request.temperature,
                stream: false,
                stream_options: None,
            },
        };

        let url = format!(
            "{}/infer_request/{}?wait={}",
            self.base_url, alias, INITIAL_WAIT_SECS
        );

        let resp = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", api_key))
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    GatewayError::Timeout(format!("Theta inference timed out: {e}"))
                } else {
                    GatewayError::NetworkError(format!("Theta request failed: {e}"))
                }
            })?;

        let status = resp.status().as_u16();
        let resp_text = resp.text().await.unwrap_or_default();

        if status >= 400 {
            return Err(GatewayError::ProviderError {
                provider: Provider::Theta,
                status,
                message: resp_text,
            });
        }

        let api_resp: ThetaApiResponse = serde_json::from_str(&resp_text).map_err(|e| {
            tracing::warn!(
                raw_body_prefix = &resp_text[..resp_text.len().min(500)],
                "Theta response parse failed"
            );
            GatewayError::InternalError(format!("Failed to parse Theta response: {e}"))
        })?;

        let infer_req = api_resp
            .body
            .infer_requests
            .into_iter()
            .next()
            .ok_or_else(|| {
                GatewayError::InternalError("Theta returned empty infer_requests".to_string())
            })?;

        let infer_req = if infer_req.state == "success" {
            infer_req
        } else if infer_req.state == "error" {
            let msg = extract_output_text(&infer_req);
            return Err(GatewayError::ProviderError {
                provider: Provider::Theta,
                status: 500,
                message: format!("Theta inference failed: {msg}"),
            });
        } else {
            self.poll_until_complete(&infer_req.id, api_key).await?
        };

        let output_text = extract_output_text(&infer_req);
        let usage = extract_usage(&infer_req);
        let model_str = format!("theta/{}", alias);

        let span = tracing::Span::current();
        span.record("gen_ai.response.model", model_str.as_str());
        span.record("gen_ai.response.finish_reasons", "stop");
        span.record("gen_ai.usage.input_tokens", usage.prompt_tokens as i64);
        span.record("gen_ai.usage.output_tokens", usage.completion_tokens as i64);

        Ok(ChatCompletionResponse::new(
            format!("theta-{}", infer_req.id),
            model_str,
            vec![Choice {
                index: 0,
                message: AssistantMessage {
                    role: MessageRole::Assistant,
                    content: Some(output_text),
                    tool_calls: None,
                    thinking: None,
                },
                finish_reason: FinishReason::Stop,
                logprobs: None,
            }],
            usage,
        ))
    }

    #[tracing::instrument(
        name = "provider.theta.stream_chat_completion",
        skip(self, request, api_key),
        fields(
            model = %request.model,
            message_count = request.messages.len(),
            gen_ai.provider.name = "theta",
            gen_ai.operation.name = "chat",
            gen_ai.request.model = %request.model,
        )
    )]
    async fn stream_chat_completion(
        &self,
        request: &ChatCompletionRequest,
        api_key: &str,
    ) -> Result<ChatCompletionStream, GatewayError> {
        let alias = Self::service_alias(&request.model);

        let messages: Vec<ThetaMessage> = request
            .messages
            .iter()
            .map(|m| ThetaMessage {
                role: match m.role {
                    MessageRole::System => "system".to_string(),
                    MessageRole::User => "user".to_string(),
                    MessageRole::Assistant => "assistant".to_string(),
                    MessageRole::Tool => "tool".to_string(),
                    MessageRole::Other => "user".to_string(),
                },
                content: m.content.as_ref().map(|c| c.as_text()).unwrap_or_default(),
            })
            .collect();

        let body = ThetaInferBody {
            input: ThetaInferInput {
                messages,
                max_tokens: request.max_tokens,
                temperature: request.temperature,
                stream: true,
                stream_options: Some(serde_json::json!({"include_usage": true})),
            },
        };

        let url = format!(
            "{}/infer_request/{}?wait={}",
            self.base_url, alias, INITIAL_WAIT_SECS
        );

        let resp = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", api_key))
            .header("Accept", "text/event-stream")
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    GatewayError::Timeout(format!("Theta inference timed out: {e}"))
                } else {
                    GatewayError::NetworkError(format!("Theta request failed: {e}"))
                }
            })?;

        let status = resp.status().as_u16();
        if status >= 400 {
            let error_text = resp.text().await.unwrap_or_default();
            return Err(GatewayError::ProviderError {
                provider: Provider::Theta,
                status,
                message: error_text,
            });
        }

        let byte_stream = resp.bytes_stream();
        let data_stream = bytes_to_sse_data_stream(byte_stream);

        let chunk_stream = data_stream.map(|result| {
            result.and_then(|data| parse_sse_json::<ChatCompletionChunk>(&data, "Theta chunk"))
        });

        Ok(Box::pin(chunk_stream))
    }
}

impl ThetaProvider {
    /// Poll `GET /infer_request/{id}` until the request reaches `success` or we time out.
    async fn poll_until_complete(
        &self,
        request_id: &str,
        api_key: &str,
    ) -> Result<ThetaInferRequest, GatewayError> {
        let deadline = tokio::time::Instant::now() + self.timeout;
        let url = format!("{}/infer_request/{}", self.base_url, request_id);

        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(GatewayError::Timeout(format!(
                    "Theta inference request {} did not complete within timeout",
                    request_id
                )));
            }

            tokio::time::sleep(POLL_INTERVAL).await;

            let resp = self
                .client
                .get(&url)
                .header("Authorization", format!("Bearer {}", api_key))
                .send()
                .await
                .map_err(|e| GatewayError::NetworkError(format!("Theta poll failed: {e}")))?;

            let status = resp.status().as_u16();
            let resp_text = resp.text().await.unwrap_or_default();

            if status >= 400 {
                return Err(GatewayError::ProviderError {
                    provider: Provider::Theta,
                    status,
                    message: resp_text,
                });
            }

            let api_resp: ThetaApiResponse = serde_json::from_str(&resp_text).map_err(|e| {
                tracing::warn!(
                    raw_body_prefix = &resp_text[..resp_text.len().min(500)],
                    "Theta poll response parse failed"
                );
                GatewayError::InternalError(format!("Failed to parse Theta poll response: {e}"))
            })?;

            let infer_req = api_resp
                .body
                .infer_requests
                .into_iter()
                .next()
                .ok_or_else(|| {
                    GatewayError::InternalError(
                        "Theta poll returned empty infer_requests".to_string(),
                    )
                })?;

            match infer_req.state.as_str() {
                "success" => return Ok(infer_req),
                "error" => {
                    let msg = extract_output_text(&infer_req);
                    return Err(GatewayError::ProviderError {
                        provider: Provider::Theta,
                        status: 500,
                        message: format!("Theta inference failed: {msg}"),
                    });
                }
                _ => continue,
            }
        }
    }
}

/// Extract token usage from a Theta infer_request's output.
///
/// vLLM responses wrapped in Theta's envelope include an OpenAI-compatible
/// `usage` object with `prompt_tokens`, `completion_tokens`, `total_tokens`.
/// Returns `Usage::default()` if not present.
fn extract_usage(infer_req: &ThetaInferRequest) -> Usage {
    let Some(output) = infer_req.output.as_ref() else {
        return Usage::default();
    };

    let Some(usage_val) = output.get("usage") else {
        return Usage::default();
    };

    let prompt_tokens = usage_val
        .get("prompt_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u32;
    let completion_tokens = usage_val
        .get("completion_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u32;
    let total_tokens = usage_val
        .get("total_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or((prompt_tokens + completion_tokens) as u64) as u32;

    Usage {
        prompt_tokens,
        completion_tokens,
        total_tokens,
        thinking_tokens: None,
        completion_tokens_details: None,
        prompt_tokens_details: None,
    }
}

/// Best-effort extraction of text output from a Theta infer_request.
///
/// The output shape varies by model; we handle the common cases:
/// - `output` is a string
/// - `output` is an object with a `"text"` or `"content"` field
/// - `output` is an object with `"choices"` (OpenAI-like from some vLLM models)
fn extract_output_text(infer_req: &ThetaInferRequest) -> String {
    let Some(output) = infer_req.output.as_ref() else {
        return String::new();
    };

    if let Some(s) = output.as_str() {
        return s.to_string();
    }

    if let Some(text) = output.get("text").and_then(|v| v.as_str()) {
        return text.to_string();
    }
    if let Some(content) = output.get("content").and_then(|v| v.as_str()) {
        return content.to_string();
    }

    if let Some(choices) = output.get("choices").and_then(|v| v.as_array()) {
        if let Some(first) = choices.first() {
            if let Some(msg) = first.get("message") {
                if let Some(c) = msg.get("content").and_then(|v| v.as_str()) {
                    return c.to_string();
                }
            }
            if let Some(text) = first.get("text").and_then(|v| v.as_str()) {
                return text.to_string();
            }
        }
    }

    output.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_alias_strips_prefix() {
        assert_eq!(
            ThetaProvider::service_alias("theta/llama_3_1_70b"),
            "llama_3_1_70b"
        );
    }

    #[test]
    fn service_alias_noop_without_prefix() {
        assert_eq!(
            ThetaProvider::service_alias("llama_3_1_70b"),
            "llama_3_1_70b"
        );
    }

    #[test]
    fn supports_model_only_theta_prefix() {
        let provider = ThetaProvider::with_default_timeout();
        assert!(provider.supports_model("theta/llama_3_1_70b"));
        assert!(!provider.supports_model("llama_3_1_70b"));
        assert!(!provider.supports_model("gpt-4o"));
    }

    #[test]
    fn streaming_supported() {
        let provider = ThetaProvider::with_default_timeout();
        assert!(provider.supports_streaming("theta/llama_3_1_70b"));
    }

    #[test]
    fn extract_output_text_from_string() {
        let req = ThetaInferRequest {
            id: "r1".into(),
            state: "success".into(),
            output: Some(serde_json::json!("Hello world")),
        };
        assert_eq!(extract_output_text(&req), "Hello world");
    }

    #[test]
    fn extract_output_text_from_object_text() {
        let req = ThetaInferRequest {
            id: "r1".into(),
            state: "success".into(),
            output: Some(serde_json::json!({"text": "Hi there"})),
        };
        assert_eq!(extract_output_text(&req), "Hi there");
    }

    #[test]
    fn extract_output_text_from_object_content() {
        let req = ThetaInferRequest {
            id: "r1".into(),
            state: "success".into(),
            output: Some(serde_json::json!({"content": "Response text"})),
        };
        assert_eq!(extract_output_text(&req), "Response text");
    }

    #[test]
    fn extract_output_text_from_openai_choices() {
        let req = ThetaInferRequest {
            id: "r1".into(),
            state: "success".into(),
            output: Some(serde_json::json!({
                "choices": [{"message": {"role": "assistant", "content": "From choices"}}]
            })),
        };
        assert_eq!(extract_output_text(&req), "From choices");
    }

    #[test]
    fn extract_output_text_none_returns_empty() {
        let req = ThetaInferRequest {
            id: "r1".into(),
            state: "success".into(),
            output: None,
        };
        assert_eq!(extract_output_text(&req), "");
    }

    #[test]
    fn extract_usage_from_vllm_output() {
        let req = ThetaInferRequest {
            id: "r1".into(),
            state: "success".into(),
            output: Some(serde_json::json!({
                "choices": [{"message": {"role": "assistant", "content": "hi"}}],
                "usage": {
                    "prompt_tokens": 42,
                    "completion_tokens": 128,
                    "total_tokens": 170
                }
            })),
        };
        let usage = extract_usage(&req);
        assert_eq!(usage.prompt_tokens, 42);
        assert_eq!(usage.completion_tokens, 128);
        assert_eq!(usage.total_tokens, 170);
    }

    #[test]
    fn extract_usage_missing_returns_default() {
        let req = ThetaInferRequest {
            id: "r1".into(),
            state: "success".into(),
            output: Some(serde_json::json!({"text": "hello"})),
        };
        let usage = extract_usage(&req);
        assert_eq!(usage.prompt_tokens, 0);
        assert_eq!(usage.completion_tokens, 0);
    }

    #[test]
    fn extract_usage_none_output_returns_default() {
        let req = ThetaInferRequest {
            id: "r1".into(),
            state: "success".into(),
            output: None,
        };
        let usage = extract_usage(&req);
        assert_eq!(usage.prompt_tokens, 0);
        assert_eq!(usage.completion_tokens, 0);
    }
}
