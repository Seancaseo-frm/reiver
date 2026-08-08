//! xAI (Grok) provider adapter.
//!
//! xAI exposes an OpenAI-compatible `/chat/completions` endpoint. This
//! adapter wraps `OpenAiProvider` with `grok-` model prefix handling.
//!
//! ## Configuration
//!
//! | Env var | Description |
//! |---|---|
//! | `GATEWAY_XAI_BASE_URL` | API endpoint (default: `https://api.x.ai/v1`) |
//! | `GATEWAY_TIMEOUT_XAI_SECONDS` | HTTP timeout (default: 120) |

use async_trait::async_trait;
use std::time::Duration;

use super::{ChatCompletionStream, LlmProvider, OpenAiProvider};
use crate::gateway::error::GatewayError;
use crate::gateway::provider_types::Provider;
use crate::gateway::types::{ChatCompletionRequest, ChatCompletionResponse};

const DEFAULT_TIMEOUT_SECS: u64 = 120;

pub struct XaiProvider(OpenAiProvider);

impl XaiProvider {
    pub fn new(base_url: String, timeout: Duration) -> Self {
        Self(OpenAiProvider::with_base_url_and_timeout(base_url, timeout))
    }

    pub fn with_base_url(base_url: String) -> Self {
        Self::new(base_url, Duration::from_secs(DEFAULT_TIMEOUT_SECS))
    }
}

#[async_trait]
impl LlmProvider for XaiProvider {
    fn name(&self) -> Provider {
        Provider::Xai
    }

    fn supports_model(&self, model: &str) -> bool {
        model.starts_with("grok-")
    }

    #[tracing::instrument(
        name = "provider.xai.chat_completion",
        skip(self, request, api_key),
        fields(
            model = %request.model,
            message_count = request.messages.len(),
            gen_ai.provider.name = "x-ai",
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
        let result = self.0.chat_completion(request, api_key).await?;
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
        name = "provider.xai.stream_chat_completion",
        skip(self, request, api_key),
        fields(
            model = %request.model,
            gen_ai.provider.name = "x-ai",
            gen_ai.operation.name = "chat",
            gen_ai.request.model = %request.model,
        )
    )]
    async fn stream_chat_completion(
        &self,
        request: &ChatCompletionRequest,
        api_key: &str,
    ) -> Result<ChatCompletionStream, GatewayError> {
        self.0.stream_chat_completion(request, api_key).await
    }
}
