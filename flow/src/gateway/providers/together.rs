//! Together AI provider adapter.
//!
//! Together AI hosts open-source models (Llama, Qwen, Mixtral, etc.) behind
//! an OpenAI-compatible `/chat/completions` endpoint.
//!
//! ## Configuration
//!
//! | Env var | Description |
//! |---|---|
//! | `GATEWAY_TOGETHER_BASE_URL` | API endpoint (default: `https://api.together.xyz/v1`) |
//! | `GATEWAY_TIMEOUT_TOGETHER_SECONDS` | HTTP timeout (default: 120) |

use async_trait::async_trait;
use std::time::Duration;

use super::{ChatCompletionStream, LlmProvider, OpenAiProvider};
use crate::gateway::embedding_types::{EmbeddingRequest, EmbeddingResponse};
use crate::gateway::error::GatewayError;
use crate::gateway::provider_types::Provider;
use crate::gateway::types::{ChatCompletionRequest, ChatCompletionResponse};

pub struct TogetherProvider(OpenAiProvider);

impl TogetherProvider {
    pub fn new(base_url: String, timeout: Duration) -> Self {
        Self(OpenAiProvider::with_base_url_and_timeout(base_url, timeout))
    }

    fn strip_prefix(model: &str) -> &str {
        model.strip_prefix("together/").unwrap_or(model)
    }
}

#[async_trait]
impl LlmProvider for TogetherProvider {
    fn name(&self) -> Provider {
        Provider::Together
    }

    fn supports_model(&self, model: &str) -> bool {
        model.starts_with("together/")
    }

    #[tracing::instrument(
        name = "provider.together.chat_completion",
        skip(self, request, api_key),
        fields(
            model = %request.model,
            message_count = request.messages.len(),
            gen_ai.provider.name = "together",
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
        let mut req = request.clone();
        req.model = Self::strip_prefix(&req.model).to_string();
        let result = self.0.chat_completion(&req, api_key).await?;
        let span = tracing::Span::current();
        span.record("gen_ai.response.model", result.model.as_str());
        span.record(
            "gen_ai.usage.input_tokens",
            result.usage.prompt_tokens as i64,
        );
        span.record(
            "gen_ai.usage.output_tokens",
            result.usage.completion_tokens as i64,
        );
        if let Some(choice) = result.choices.first() {
            span.record(
                "gen_ai.response.finish_reasons",
                choice.finish_reason.as_str(),
            );
        }
        Ok(result)
    }

    #[tracing::instrument(
        name = "provider.together.stream_chat_completion",
        skip(self, request, api_key),
        fields(
            model = %request.model,
            gen_ai.provider.name = "together",
            gen_ai.operation.name = "chat",
            gen_ai.request.model = %request.model,
        )
    )]
    async fn stream_chat_completion(
        &self,
        request: &ChatCompletionRequest,
        api_key: &str,
    ) -> Result<ChatCompletionStream, GatewayError> {
        let mut req = request.clone();
        req.model = Self::strip_prefix(&req.model).to_string();
        self.0.stream_chat_completion(&req, api_key).await
    }

    async fn embed(
        &self,
        request: &EmbeddingRequest,
        api_key: &str,
    ) -> Result<EmbeddingResponse, GatewayError> {
        let mut req = request.clone();
        req.model = Self::strip_prefix(&req.model).to_string();
        self.0.embed(&req, api_key).await
    }
}
