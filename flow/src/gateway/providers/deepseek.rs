//! DeepSeek provider adapter.
//!
//! DeepSeek exposes an OpenAI-compatible `/chat/completions` endpoint. This
//! adapter is a thin wrapper over `OpenAiProvider` that:
//!
//! 1. Namespaces model identifiers with a `deepseek/` prefix so they don't
//!    collide with other providers (e.g. `deepseek/deepseek-chat`).
//! 2. Strips the `deepseek/` prefix from the model name before forwarding the
//!    request to the DeepSeek API.
//! 3. Delegates all HTTP, SSE streaming, and error handling to `OpenAiProvider`.
//!
//! ## Configuration
//!
//! | Env var | Description |
//! |---|---|
//! | `GATEWAY_DEEPSEEK_BASE_URL` | API endpoint (default: `https://api.deepseek.com`) |
//! | `GATEWAY_TIMEOUT_DEEPSEEK_SECONDS` | HTTP timeout (default: 120) |
//!
//! The API key is stored in `project_settings` under `gateway_deepseek_api_key`
//! via the standard integrations UI — no special handling required.

use async_trait::async_trait;
use std::time::Duration;

use super::{ChatCompletionStream, LlmProvider, OpenAiProvider};
use crate::gateway::error::GatewayError;
use crate::gateway::provider_types::Provider;
use crate::gateway::types::{ChatCompletionRequest, ChatCompletionResponse};

const DEFAULT_TIMEOUT_SECS: u64 = 120;

/// DeepSeek provider — delegates to the OpenAI-compatible DeepSeek API.
pub struct DeepSeekProvider(OpenAiProvider);

impl DeepSeekProvider {
    pub fn new(base_url: String, timeout: Duration) -> Self {
        Self(OpenAiProvider::with_base_url_and_timeout(base_url, timeout))
    }

    pub fn with_base_url(base_url: String) -> Self {
        Self::new(base_url, Duration::from_secs(DEFAULT_TIMEOUT_SECS))
    }

    /// Strip the `deepseek/` namespace prefix from a model name.
    ///
    /// `"deepseek/deepseek-chat"` → `"deepseek-chat"`
    fn strip_prefix(model: &str) -> &str {
        model.strip_prefix("deepseek/").unwrap_or(model)
    }
}

#[async_trait]
impl LlmProvider for DeepSeekProvider {
    fn name(&self) -> Provider {
        Provider::DeepSeek
    }

    fn supports_model(&self, model: &str) -> bool {
        model.starts_with("deepseek/")
    }

    #[tracing::instrument(
        name = "provider.deepseek.chat_completion",
        skip(self, request, api_key),
        fields(
            model = %request.model,
            message_count = request.messages.len(),
            gen_ai.provider.name = "deepseek",
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
        name = "provider.deepseek.stream_chat_completion",
        skip(self, request, api_key),
        fields(
            model = %request.model,
            gen_ai.provider.name = "deepseek",
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
}
