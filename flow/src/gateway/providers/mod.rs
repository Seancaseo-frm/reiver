//! LLM Provider adapters for the AI Gateway.
//!
//! Each provider implements the `LlmProvider` trait to handle routing and
//! translation between OpenAI format and provider-specific formats.

pub mod ai21;
pub mod alibaba;
pub mod anthropic;
pub mod azure_openai;
pub mod bedrock;
pub mod cerebras;
pub mod cloudflare;
pub mod cohere;
pub mod common;
pub mod deepinfra;
pub mod deepseek;
pub mod fireworks;
pub mod google;
pub mod groq;
pub mod huggingface;
pub mod hyperbolic;
pub mod lambda;
pub mod lepton;
pub mod mistral;
pub mod novita;
pub mod nvidia;
pub mod openai;
pub mod openrouter;
pub mod ovhcloud;
pub mod perplexity;
pub mod sambanova;
pub mod sse;
pub mod theta;
pub mod theta_dedicated;
pub mod together;
pub mod vertex_ai;
pub mod xai;

use crate::gateway::embedding_types::{EmbeddingRequest, EmbeddingResponse};
use crate::gateway::error::GatewayError;
use crate::gateway::provider_types::Provider;
use crate::gateway::types::{ChatCompletionChunk, ChatCompletionRequest, ChatCompletionResponse};
use async_trait::async_trait;
use futures::Stream;
use std::pin::Pin;

/// Type alias for a boxed stream of chat completion chunks.
///
/// This is the return type for streaming chat completions. Each item in the
/// stream is either a chunk of the response or an error.
pub type ChatCompletionStream =
    Pin<Box<dyn Stream<Item = Result<ChatCompletionChunk, GatewayError>> + Send>>;

pub use ai21::Ai21Provider;
pub use alibaba::AlibabaProvider;
pub use anthropic::AnthropicProvider;
pub use azure_openai::AzureOpenAiProvider;
pub use bedrock::BedrockProvider;
pub use cerebras::CerebrasProvider;
pub use cloudflare::CloudflareProvider;
pub use cohere::CohereProvider;
pub use deepinfra::DeepInfraProvider;
pub use deepseek::DeepSeekProvider;
pub use fireworks::FireworksProvider;
pub use google::GoogleProvider;
pub use groq::GroqProvider;
pub use huggingface::HuggingFaceProvider;
pub use hyperbolic::HyperbolicProvider;
pub use lambda::LambdaProvider;
pub use lepton::LeptonProvider;
pub use mistral::MistralProvider;
pub use novita::NovitaProvider;
pub use nvidia::NvidiaProvider;
pub use openai::OpenAiProvider;
pub use openrouter::OpenRouterProvider;
pub use ovhcloud::OvhCloudProvider;
pub use perplexity::PerplexityProvider;
pub use sambanova::SambaNovaProvider;
pub use theta::ThetaProvider;
pub use theta_dedicated::ThetaDedicatedProvider;
pub use together::TogetherProvider;
pub use vertex_ai::VertexAiProvider;
pub use xai::XaiProvider;

/// Trait for LLM provider adapters.
///
/// Each provider implements this trait to:
/// 1. Identify which models it supports
/// 2. Translate OpenAI-format requests to provider-specific format
/// 3. Execute the request against the provider's API
/// 4. Translate the response back to OpenAI format
#[async_trait]
pub trait LlmProvider: Send + Sync {
    /// Returns the typed provider identifier.
    fn name(&self) -> Provider;

    /// Check if this provider handles the given model identifier.
    ///
    /// The gateway iterates through providers and uses the first one
    /// that returns `true` for the requested model.
    fn supports_model(&self, model: &str) -> bool;

    /// Execute a chat completion request.
    ///
    /// The implementation should:
    /// 1. Translate the OpenAI-format request to provider format (if needed)
    /// 2. Call the provider's API
    /// 3. Translate the response back to OpenAI format
    ///
    /// # Arguments
    /// * `request` - The OpenAI-format chat completion request
    /// * `api_key` - The provider's API key
    ///
    /// # Returns
    /// An OpenAI-format chat completion response
    async fn chat_completion(
        &self,
        request: &ChatCompletionRequest,
        api_key: &str,
    ) -> Result<ChatCompletionResponse, GatewayError>;

    /// Check if the provider supports streaming for the given model.
    fn supports_streaming(&self, _model: &str) -> bool {
        true // Most providers support streaming
    }

    /// Execute a streaming chat completion request.
    ///
    /// Returns a stream of `ChatCompletionChunk` that can be forwarded as SSE.
    /// The stream ends with a chunk containing `finish_reason` set.
    ///
    /// # Arguments
    /// * `request` - The OpenAI-format chat completion request (with stream: true)
    /// * `api_key` - The provider's API key
    ///
    /// # Returns
    /// A stream of OpenAI-format chat completion chunks
    ///
    /// # Default Implementation
    /// Returns an error indicating streaming is not implemented.
    /// Providers should override this to enable streaming support.
    async fn stream_chat_completion(
        &self,
        _request: &ChatCompletionRequest,
        _api_key: &str,
    ) -> Result<ChatCompletionStream, GatewayError> {
        Err(GatewayError::InternalError(format!(
            "Streaming not implemented for provider: {}",
            self.name().as_str()
        )))
    }

    /// Execute an embedding request.
    ///
    /// # Default Implementation
    /// Returns `UnsupportedModel` — only providers with an explicit override
    /// accept embedding requests.
    async fn embed(
        &self,
        _request: &EmbeddingRequest,
        _api_key: &str,
    ) -> Result<EmbeddingResponse, GatewayError> {
        Err(GatewayError::UnsupportedModel(format!(
            "{} does not support embeddings",
            self.name().as_str()
        )))
    }
}
