//! Gateway router for dispatching requests to the correct LLM provider.
//!
//! Uses a sorted prefix lookup table for model-to-provider routing.
//! Prefixes are sorted by length (descending) to correctly handle
//! overlapping prefixes.

use std::default::Default;
use std::sync::Arc;
use std::time::Duration;
use strum::IntoEnumIterator;

use crate::config::Config;
use crate::gateway::circuit_breaker::CircuitBreaker;
use crate::gateway::error::GatewayError;
use crate::gateway::latency_tracker::LatencyTracker;
use crate::gateway::provider_manager::{GatewayTimeouts, ProviderConfig};
use crate::gateway::provider_types::Provider;
use crate::gateway::providers::{
    Ai21Provider, AlibabaProvider, AnthropicProvider, AzureOpenAiProvider, BedrockProvider,
    CerebrasProvider, ChatCompletionStream, CloudflareProvider, CohereProvider, DeepInfraProvider,
    DeepSeekProvider, FireworksProvider, GoogleProvider, GroqProvider, HuggingFaceProvider,
    HyperbolicProvider, LambdaProvider, LeptonProvider, LlmProvider, MistralProvider,
    NovitaProvider, NvidiaProvider, OpenAiProvider, OpenRouterProvider, OvhCloudProvider,
    PerplexityProvider, SambaNovaProvider, ThetaDedicatedProvider, ThetaProvider, TogetherProvider,
    VertexAiProvider, XaiProvider,
};
use crate::gateway::embedding_types::{EmbeddingRequest, EmbeddingResponse};
use crate::gateway::types::{ChatCompletionRequest, ChatCompletionResponse};

/// Gateway router that manages LLM providers and routes requests.
///
/// Uses a pre-built prefix lookup table for model-to-provider routing.
/// Prefixes are sorted by length (descending) to correctly handle overlapping
/// prefixes like "anthropic." (bedrock) vs "anthropic" in model names.
pub struct GatewayRouter {
    providers: Vec<Arc<dyn LlmProvider>>,
    /// Mapping from model prefix to provider index.
    /// Sorted by prefix length (descending) for correct overlapping prefix handling.
    prefix_lookup: Vec<(&'static str, usize)>,
    /// Latency tracker for adaptive routing decisions.
    latency_tracker: Arc<LatencyTracker>,
    /// Per-provider circuit breaker for error-rate-based fallback.
    circuit_breaker: Arc<CircuitBreaker>,
}

impl GatewayRouter {
    /// Create a new gateway router with all supported providers using default timeouts.
    pub fn new() -> Self {
        Self::with_full_config(GatewayTimeouts::default(), ProviderConfig::default())
    }

    /// Create a new gateway router with configuration from Config.
    pub fn from_config(config: &Config) -> Self {
        Self::with_full_config(
            GatewayTimeouts::from_config(config),
            ProviderConfig::from_config(config),
        )
    }

    /// Create a new gateway router with full configuration.
    pub fn with_full_config(timeouts: GatewayTimeouts, provider_config: ProviderConfig) -> Self {
        let openai_base = provider_config
            .openai_base_url
            .unwrap_or_else(|| "https://api.openai.com/v1".to_string());
        let anthropic_base = provider_config
            .anthropic_base_url
            .unwrap_or_else(|| "https://api.anthropic.com/v1".to_string());
        let google_base = provider_config
            .google_base_url
            .unwrap_or_else(|| "https://generativelanguage.googleapis.com/v1beta".to_string());
        let deepseek_base = provider_config
            .deepseek_base_url
            .unwrap_or_else(|| "https://api.deepseek.com".to_string());

        let compat_timeout = timeouts.openai_compat;

        let xai_base = provider_config
            .xai_base_url
            .unwrap_or_else(|| "https://api.x.ai/v1".to_string());
        let mistral_base = provider_config
            .mistral_base_url
            .unwrap_or_else(|| "https://api.mistral.ai/v1".to_string());
        let groq_base = provider_config
            .groq_base_url
            .unwrap_or_else(|| "https://api.groq.com/openai/v1".to_string());
        let together_base = provider_config
            .together_base_url
            .unwrap_or_else(|| "https://api.together.xyz/v1".to_string());
        let fireworks_base = provider_config
            .fireworks_base_url
            .unwrap_or_else(|| "https://api.fireworks.ai/inference/v1".to_string());
        let perplexity_base = provider_config
            .perplexity_base_url
            .unwrap_or_else(|| "https://api.perplexity.ai".to_string());
        let cohere_base = provider_config
            .cohere_base_url
            .unwrap_or_else(|| "https://api.cohere.com/compatibility/v1".to_string());
        let openrouter_base = provider_config
            .openrouter_base_url
            .unwrap_or_else(|| "https://openrouter.ai/api/v1".to_string());
        let cerebras_base = provider_config
            .cerebras_base_url
            .unwrap_or_else(|| "https://api.cerebras.ai/v1".to_string());
        let deepinfra_base = provider_config
            .deepinfra_base_url
            .unwrap_or_else(|| "https://api.deepinfra.com/v1/openai".to_string());
        let alibaba_base = provider_config.alibaba_base_url.unwrap_or_else(|| {
            "https://dashscope-intl.aliyuncs.com/compatible-mode/v1".to_string()
        });
        let nvidia_base = provider_config
            .nvidia_base_url
            .unwrap_or_else(|| "https://integrate.api.nvidia.com/v1".to_string());
        let ai21_base = provider_config
            .ai21_base_url
            .unwrap_or_else(|| "https://api.ai21.com/studio/v1".to_string());
        let sambanova_base = provider_config
            .sambanova_base_url
            .unwrap_or_else(|| "https://api.sambanova.ai/v1".to_string());
        let lambda_base = provider_config
            .lambda_base_url
            .unwrap_or_else(|| "https://api.lambdalabs.com/v1".to_string());
        let lepton_base = provider_config
            .lepton_base_url
            .unwrap_or_else(|| "https://api.lepton.ai/v1".to_string());
        let hyperbolic_base = provider_config
            .hyperbolic_base_url
            .unwrap_or_else(|| "https://api.hyperbolic.xyz/v1".to_string());
        let ovhcloud_base = provider_config
            .ovhcloud_base_url
            .unwrap_or_else(|| "https://ovh.hf.space/v1".to_string());
        let novita_base = provider_config
            .novita_base_url
            .unwrap_or_else(|| "https://api.novita.ai/v3/openai".to_string());
        let huggingface_base = provider_config
            .huggingface_base_url
            .unwrap_or_else(|| "https://router.huggingface.co/v1".to_string());
        let cloudflare_base = provider_config.cloudflare_base_url.unwrap_or_else(|| {
            "https://api.cloudflare.com/client/v4/accounts/placeholder/ai/v1".to_string()
        });
        let azure_openai_base = provider_config
            .azure_openai_base_url
            .unwrap_or_else(|| "https://placeholder.openai.azure.com/openai/v1".to_string());
        let vertex_ai_base = provider_config.vertex_ai_base_url.unwrap_or_else(|| {
            "https://us-central1-aiplatform.googleapis.com/v1beta1/openai".to_string()
        });

        let provider_map: Vec<(Provider, Arc<dyn LlmProvider>)> = vec![
            (
                Provider::OpenAi,
                Arc::new(OpenAiProvider::with_base_url_and_timeout(
                    openai_base,
                    timeouts.openai,
                )),
            ),
            (
                Provider::Anthropic,
                Arc::new(AnthropicProvider::with_config(
                    anthropic_base,
                    timeouts.anthropic,
                    provider_config.anthropic_api_version,
                )),
            ),
            (
                Provider::Google,
                Arc::new(GoogleProvider::with_base_url_and_timeout(
                    google_base,
                    timeouts.google,
                )),
            ),
            (
                Provider::Bedrock,
                Arc::new(BedrockProvider::with_timeout(timeouts.bedrock)),
            ),
            (
                Provider::Theta,
                Arc::new(ThetaProvider::new(timeouts.theta)),
            ),
            (
                Provider::ThetaDedicated,
                Arc::new(ThetaDedicatedProvider::with_base_url(
                    "https://placeholder.thetaedgecloud.com/v1".to_string(),
                )),
            ),
            (
                Provider::DeepSeek,
                Arc::new(DeepSeekProvider::new(deepseek_base, timeouts.deepseek)),
            ),
            (
                Provider::Xai,
                Arc::new(XaiProvider::new(xai_base, compat_timeout)),
            ),
            (
                Provider::Mistral,
                Arc::new(MistralProvider::new(mistral_base, compat_timeout)),
            ),
            (
                Provider::Groq,
                Arc::new(GroqProvider::new(groq_base, compat_timeout)),
            ),
            (
                Provider::Together,
                Arc::new(TogetherProvider::new(together_base, compat_timeout)),
            ),
            (
                Provider::Fireworks,
                Arc::new(FireworksProvider::new(fireworks_base, compat_timeout)),
            ),
            (
                Provider::Perplexity,
                Arc::new(PerplexityProvider::new(perplexity_base, compat_timeout)),
            ),
            (
                Provider::Cohere,
                Arc::new(CohereProvider::new(cohere_base, compat_timeout)),
            ),
            (
                Provider::OpenRouter,
                Arc::new(OpenRouterProvider::new(openrouter_base, compat_timeout)),
            ),
            (
                Provider::Cerebras,
                Arc::new(CerebrasProvider::new(cerebras_base, compat_timeout)),
            ),
            (
                Provider::DeepInfra,
                Arc::new(DeepInfraProvider::new(deepinfra_base, compat_timeout)),
            ),
            (
                Provider::Alibaba,
                Arc::new(AlibabaProvider::new(alibaba_base, compat_timeout)),
            ),
            (
                Provider::Nvidia,
                Arc::new(NvidiaProvider::new(nvidia_base, compat_timeout)),
            ),
            (
                Provider::Ai21,
                Arc::new(Ai21Provider::new(ai21_base, compat_timeout)),
            ),
            (
                Provider::SambaNova,
                Arc::new(SambaNovaProvider::new(sambanova_base, compat_timeout)),
            ),
            (
                Provider::Lambda,
                Arc::new(LambdaProvider::new(lambda_base, compat_timeout)),
            ),
            (
                Provider::Lepton,
                Arc::new(LeptonProvider::new(lepton_base, compat_timeout)),
            ),
            (
                Provider::Hyperbolic,
                Arc::new(HyperbolicProvider::new(hyperbolic_base, compat_timeout)),
            ),
            (
                Provider::OvhCloud,
                Arc::new(OvhCloudProvider::new(ovhcloud_base, compat_timeout)),
            ),
            (
                Provider::Novita,
                Arc::new(NovitaProvider::new(novita_base, compat_timeout)),
            ),
            (
                Provider::HuggingFace,
                Arc::new(HuggingFaceProvider::new(huggingface_base, compat_timeout)),
            ),
            (
                Provider::Cloudflare,
                Arc::new(CloudflareProvider::new(cloudflare_base, compat_timeout)),
            ),
            (
                Provider::AzureOpenAi,
                Arc::new(AzureOpenAiProvider::new(azure_openai_base, compat_timeout)),
            ),
            (
                Provider::VertexAi,
                Arc::new(VertexAiProvider::new(vertex_ai_base, compat_timeout)),
            ),
        ];

        let providers: Vec<Arc<dyn LlmProvider>> =
            provider_map.iter().map(|(_, p)| p.clone()).collect();

        // Build prefix lookup table using the explicit provider_map, so each
        // prefix index is tied to the correct provider regardless of enum order.
        let mut prefix_lookup: Vec<(&'static str, usize)> = Vec::new();
        for (idx, (provider, _)) in provider_map.iter().enumerate() {
            for &prefix in provider.model_prefixes() {
                prefix_lookup.push((prefix, idx));
            }
        }

        // Sort by prefix length descending for correct overlapping prefix handling
        // (longer prefixes should match first, e.g., "o1-" before "o1")
        prefix_lookup.sort_by(|a, b| b.0.len().cmp(&a.0.len()));

        Self {
            providers,
            prefix_lookup,
            latency_tracker: Arc::new(LatencyTracker::new(Default::default())),
            circuit_breaker: Arc::new(CircuitBreaker::new()),
        }
    }

    /// Set the latency tracker for adaptive routing decisions.
    pub fn with_latency_tracker(mut self, tracker: Arc<LatencyTracker>) -> Self {
        self.latency_tracker = tracker;
        self
    }

    /// Set the circuit breaker for error-rate-based provider skipping.
    pub fn with_circuit_breaker(mut self, cb: Arc<CircuitBreaker>) -> Self {
        self.circuit_breaker = cb;
        self
    }

    /// Get a reference to the latency tracker.
    pub fn latency_tracker(&self) -> Arc<LatencyTracker> {
        self.latency_tracker.clone()
    }

    /// Get a reference to the circuit breaker.
    pub fn circuit_breaker(&self) -> Arc<CircuitBreaker> {
        self.circuit_breaker.clone()
    }

    /// Get the sorted fallback ordering for candidates based on latency.
    ///
    /// Returns provider names sorted by P95 latency (lowest first).
    /// Providers without latency data are placed at the end.
    pub fn get_latency_sorted_providers(&self, candidates: &[String]) -> Vec<String> {
        let batch = self.latency_tracker.get_latencies_batch(candidates);

        let mut with_latency: Vec<(String, Duration)> = Vec::new();
        let mut without_latency: Vec<String> = Vec::new();

        for (name, latency_opt) in batch {
            if let Some(latency) = latency_opt {
                with_latency.push((name, latency.p95));
            } else {
                without_latency.push(name);
            }
        }

        // Sort by P95 latency (lowest first)
        with_latency.sort_by(|a, b| a.1.cmp(&b.1));

        let mut result: Vec<String> = with_latency.into_iter().map(|(name, _)| name).collect();
        result.extend(without_latency);
        result
    }

    /// Create a gateway router with custom providers (for testing).
    ///
    /// Note: This creates a router without the optimized prefix lookup table.
    /// The router will fall back to linear provider iteration for routing.
    pub fn with_providers(providers: Vec<Arc<dyn LlmProvider>>) -> Self {
        Self {
            providers,
            prefix_lookup: Vec::new(),
            latency_tracker: Arc::new(LatencyTracker::new(Default::default())),
            circuit_breaker: Arc::new(CircuitBreaker::new()),
        }
    }

    /// Find the provider that supports the given model.
    ///
    /// # Performance
    /// Uses the pre-built prefix lookup table for O(num_prefixes) lookup
    /// instead of O(providers * prefixes_per_provider) with linear search.
    /// Prefixes are sorted by length (descending) for correct matching.
    pub fn route(&self, model: &str) -> Option<Arc<dyn LlmProvider>> {
        // Fast path: use prefix lookup table if available
        if !self.prefix_lookup.is_empty() {
            for (prefix, provider_idx) in &self.prefix_lookup {
                if model.starts_with(prefix) {
                    return Some(self.providers[*provider_idx].clone());
                }
            }
            return None;
        }

        // Fallback: linear search through providers (used by with_providers for testing)
        self.providers
            .iter()
            .find(|p| p.supports_model(model))
            .cloned()
    }

    /// Get the provider name for a model (for observability).
    ///
    /// # Performance
    /// Uses the pre-built prefix lookup table for O(num_prefixes) lookup.
    pub fn get_provider_name(&self, model: &str) -> Option<&'static str> {
        // Fast path: use prefix lookup table if available
        if !self.prefix_lookup.is_empty() {
            for (prefix, provider_idx) in &self.prefix_lookup {
                if model.starts_with(prefix) {
                    return Some(self.providers[*provider_idx].name().as_str());
                }
            }
            return None;
        }

        // Fallback: linear search through providers
        self.providers
            .iter()
            .find(|p| p.supports_model(model))
            .map(|p| p.name().as_str())
    }

    /// Return all provider endpoints that can serve `canonical_model`, along
    /// with the provider-specific model ID and the `LlmProvider` implementation.
    ///
    /// This powers multi-provider routing: when a model like `claude-sonnet-4-6`
    /// can be served by both Anthropic and Bedrock, this returns both options.
    pub fn route_all(
        &self,
        canonical_model: &str,
    ) -> Vec<(Arc<dyn LlmProvider>, String, Provider)> {
        use crate::gateway::provider_types::get_model_endpoints;

        get_model_endpoints(canonical_model)
            .into_iter()
            .filter_map(|ep| {
                let llm = self.route(&ep.provider_model_id)?;
                Some((llm, ep.provider_model_id, ep.provider))
            })
            .collect()
    }

    /// Execute a chat completion request, routing to the appropriate provider.
    pub async fn chat_completion(
        &self,
        request: &ChatCompletionRequest,
        api_key: &str,
    ) -> Result<ChatCompletionResponse, GatewayError> {
        let provider = self
            .route(&request.model)
            .ok_or_else(|| GatewayError::UnsupportedModel(request.model.clone()))?;

        provider.chat_completion(request, api_key).await
    }

    /// Execute a streaming chat completion request.
    ///
    /// Returns a stream of ChatCompletionChunk objects that can be
    /// forwarded as SSE events to the client.
    pub async fn stream_chat_completion(
        &self,
        request: &ChatCompletionRequest,
        api_key: &str,
    ) -> Result<ChatCompletionStream, GatewayError> {
        let provider = self
            .route(&request.model)
            .ok_or_else(|| GatewayError::UnsupportedModel(request.model.clone()))?;

        // Check if provider supports streaming
        if !provider.supports_streaming(&request.model) {
            return Err(GatewayError::InternalError(format!(
                "Streaming not supported for model: {}",
                request.model
            )));
        }

        provider.stream_chat_completion(request, api_key).await
    }

    /// Execute an embedding request, routing to the appropriate provider.
    pub async fn embed(
        &self,
        request: &EmbeddingRequest,
        api_key: &str,
    ) -> Result<EmbeddingResponse, GatewayError> {
        let provider = self
            .route(&request.model)
            .ok_or_else(|| GatewayError::UnsupportedModel(request.model.clone()))?;

        provider.embed(request, api_key).await
    }

    /// Execute a chat completion using an explicit provider override.
    ///
    /// Used when the provider must be constructed per-request (e.g. ThetaDedicated
    /// where the base URL is per-project).
    pub async fn chat_completion_with_provider(
        &self,
        request: &ChatCompletionRequest,
        api_key: &str,
        provider: &dyn LlmProvider,
    ) -> Result<ChatCompletionResponse, GatewayError> {
        provider.chat_completion(request, api_key).await
    }

    /// Execute a streaming chat completion using an explicit provider override.
    pub async fn stream_chat_completion_with_provider(
        &self,
        request: &ChatCompletionRequest,
        api_key: &str,
        provider: &dyn LlmProvider,
    ) -> Result<ChatCompletionStream, GatewayError> {
        if !provider.supports_streaming(&request.model) {
            return Err(GatewayError::InternalError(format!(
                "Streaming not supported for model: {}",
                request.model
            )));
        }
        provider.stream_chat_completion(request, api_key).await
    }

    /// List all supported model prefixes, derived from `Provider::model_prefixes()`.
    pub fn supported_model_prefixes(&self) -> Vec<(&'static str, Vec<&'static str>)> {
        Provider::iter()
            .map(|p| (p.as_str(), p.model_prefixes().to_vec()))
            .collect()
    }
}

impl Default for GatewayRouter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::latency_tracker::ProviderLatency;

    #[tokio::test]
    async fn test_route_openai() {
        let router = GatewayRouter::new();

        assert!(router.route("gpt-4o").is_some());
        assert_eq!(router.get_provider_name("gpt-4o"), Some("openai"));
        assert_eq!(router.get_provider_name("gpt-3.5-turbo"), Some("openai"));
        assert_eq!(router.get_provider_name("o1-preview"), Some("openai"));
    }

    #[tokio::test]
    async fn test_route_anthropic() {
        let router = GatewayRouter::new();

        assert!(router.route("claude-sonnet-4-6").is_some());
        assert_eq!(
            router.get_provider_name("claude-sonnet-4-6"),
            Some("anthropic")
        );
        assert_eq!(
            router.get_provider_name("claude-opus-4-6"),
            Some("anthropic")
        );
    }

    #[tokio::test]
    async fn test_route_google() {
        let router = GatewayRouter::new();

        assert!(router.route("gemini-pro").is_some());
        assert_eq!(router.get_provider_name("gemini-pro"), Some("google"));
        assert_eq!(router.get_provider_name("gemini-1.5-flash"), Some("google"));
    }

    #[tokio::test]
    async fn test_route_bedrock() {
        let router = GatewayRouter::new();

        assert!(router
            .route("bedrock/anthropic.claude-3-sonnet-20240229-v1:0")
            .is_some());
        assert_eq!(
            router.get_provider_name("bedrock/anthropic.claude-3-sonnet-20240229-v1:0"),
            Some("bedrock")
        );
        assert_eq!(
            router.get_provider_name("anthropic.claude-3-opus-20240229-v1:0"),
            Some("bedrock")
        );
    }

    /// Regression: bare model names "o1" and "o3" (without trailing dash) were
    /// not in the prefix lookup table, causing requests for these valid OpenAI
    /// models to return UnsupportedModel.
    #[tokio::test]
    async fn test_route_bare_o_models() {
        let router = GatewayRouter::new();

        assert!(router.route("o1").is_some(), "bare 'o1' must be routable");
        assert_eq!(router.get_provider_name("o1"), Some("openai"));

        assert!(router.route("o3").is_some(), "bare 'o3' must be routable");
        assert_eq!(router.get_provider_name("o3"), Some("openai"));
    }

    /// Dashed variants must still match the longer prefix, not the bare one.
    #[tokio::test]
    async fn test_route_dashed_o_variants_not_confused_by_bare() {
        let router = GatewayRouter::new();

        assert_eq!(router.get_provider_name("o1-mini"), Some("openai"));
        assert_eq!(router.get_provider_name("o3-mini"), Some("openai"));
    }

    #[tokio::test]
    async fn test_route_unknown() {
        let router = GatewayRouter::new();

        assert!(router.route("unknown-model").is_none());
        assert!(router.route("llama-2-70b").is_none());
    }

    /// Regression: prefix tables were defined in three separate places that could
    /// drift apart. After consolidation, every prefix in `Provider::model_prefixes()`
    /// must be routable via the gateway router.
    #[tokio::test]
    async fn test_all_provider_prefixes_are_routable() {
        let router = GatewayRouter::new();
        for provider in Provider::iter() {
            for &prefix in provider.model_prefixes() {
                let routed_name = router.get_provider_name(&format!("{}test-model", prefix));
                assert_eq!(
                    routed_name,
                    Some(provider.as_str()),
                    "Prefix '{}' for provider '{}' must be routable",
                    prefix,
                    provider
                );
            }
        }
    }

    #[tokio::test]
    async fn test_supported_model_prefixes() {
        let router = GatewayRouter::new();
        let prefixes = router.supported_model_prefixes();

        assert_eq!(prefixes.len(), 30);
        assert!(prefixes.iter().any(|(name, _)| *name == "openai"));
        assert!(prefixes.iter().any(|(name, _)| *name == "anthropic"));
        assert!(prefixes.iter().any(|(name, _)| *name == "theta"));
        assert!(prefixes.iter().any(|(name, _)| *name == "theta-dedicated"));
        assert!(prefixes.iter().any(|(name, _)| *name == "deepseek"));
        assert!(prefixes.iter().any(|(name, _)| *name == "x-ai"));
        assert!(prefixes.iter().any(|(name, _)| *name == "mistralai"));
        assert!(prefixes.iter().any(|(name, _)| *name == "groq"));
        assert!(prefixes.iter().any(|(name, _)| *name == "together"));
        assert!(prefixes.iter().any(|(name, _)| *name == "fireworks"));
        assert!(prefixes.iter().any(|(name, _)| *name == "perplexity"));
        assert!(prefixes.iter().any(|(name, _)| *name == "cohere"));
        assert!(prefixes.iter().any(|(name, _)| *name == "openrouter"));
        assert!(prefixes.iter().any(|(name, _)| *name == "cerebras"));
        assert!(prefixes.iter().any(|(name, _)| *name == "deepinfra"));
        assert!(prefixes.iter().any(|(name, _)| *name == "qwen"));
        assert!(prefixes.iter().any(|(name, _)| *name == "nvidia"));
        assert!(prefixes.iter().any(|(name, _)| *name == "ai21"));
    }

    // --- Latency-based routing tests ---

    #[tokio::test]
    async fn test_latency_sorted_providers_basic() {
        let tracker = Arc::new(LatencyTracker::new(Default::default()));
        tracker.inject_for_test(
            Provider::OpenAi,
            ProviderLatency {
                p50: Duration::from_millis(50),
                p95: Duration::from_millis(50),
                p99: Duration::from_millis(50),
                sample_count: 5,
            },
        );
        tracker.inject_for_test(
            Provider::Anthropic,
            ProviderLatency {
                p50: Duration::from_millis(500),
                p95: Duration::from_millis(500),
                p99: Duration::from_millis(500),
                sample_count: 5,
            },
        );

        let router = GatewayRouter::new().with_latency_tracker(tracker);

        let candidates = vec!["anthropic".to_string(), "openai".to_string()];
        let sorted = router.get_latency_sorted_providers(&candidates);

        // openai should come first (lower latency)
        assert_eq!(sorted[0], "openai");
        assert_eq!(sorted[1], "anthropic");
    }

    #[tokio::test]
    async fn test_latency_sorted_providers_no_data() {
        let tracker = Arc::new(LatencyTracker::new(Default::default()));
        let router = GatewayRouter::new().with_latency_tracker(tracker);

        let candidates = vec!["openai".to_string(), "anthropic".to_string()];
        let sorted = router.get_latency_sorted_providers(&candidates);

        // No latency data -> all go to "without_latency" bucket -> original order
        assert_eq!(sorted.len(), 2);
        assert_eq!(sorted[0], "openai");
        assert_eq!(sorted[1], "anthropic");
    }

    #[tokio::test]
    async fn test_latency_sorted_providers_mixed() {
        let tracker = Arc::new(LatencyTracker::new(Default::default()));
        tracker.inject_for_test(
            Provider::OpenAi,
            ProviderLatency {
                p50: Duration::from_millis(100),
                p95: Duration::from_millis(100),
                p99: Duration::from_millis(100),
                sample_count: 5,
            },
        );

        let router = GatewayRouter::new().with_latency_tracker(tracker);

        let candidates = vec![
            "google".to_string(),
            "openai".to_string(),
            "anthropic".to_string(),
        ];
        let sorted = router.get_latency_sorted_providers(&candidates);

        // openai (has data) should be first, then those without data
        assert_eq!(sorted[0], "openai");
        assert_eq!(sorted.len(), 3);
        // google and anthropic (no data) come after
        assert!(sorted[1..].contains(&"google".to_string()));
        assert!(sorted[1..].contains(&"anthropic".to_string()));
    }

    // ==================== Router Edge Cases ====================

    #[tokio::test]
    async fn test_latency_sorted_all_unknown_preserves_order() {
        let tracker = Arc::new(LatencyTracker::new(Default::default()));
        let router = GatewayRouter::new().with_latency_tracker(tracker);

        let candidates = vec![
            "openai".to_string(),
            "anthropic".to_string(),
            "google".to_string(),
        ];
        let sorted = router.get_latency_sorted_providers(&candidates);

        // All unknown -> original order preserved
        assert_eq!(sorted, candidates);
    }

    #[tokio::test]
    async fn test_latency_sorted_equal_latency_stable() {
        let tracker = Arc::new(LatencyTracker::new(Default::default()));
        let lat = ProviderLatency {
            p50: Duration::from_millis(100),
            p95: Duration::from_millis(100),
            p99: Duration::from_millis(100),
            sample_count: 10,
        };
        tracker.inject_for_test(Provider::OpenAi, lat.clone());
        tracker.inject_for_test(Provider::Anthropic, lat.clone());
        tracker.inject_for_test(Provider::Google, lat);

        let router = GatewayRouter::new().with_latency_tracker(tracker);

        let candidates = vec![
            "openai".to_string(),
            "anthropic".to_string(),
            "google".to_string(),
        ];
        let sorted = router.get_latency_sorted_providers(&candidates);

        // All should be present; with equal latencies, sort is stable (preserves insertion order)
        assert_eq!(sorted.len(), 3);
        assert!(sorted.contains(&"openai".to_string()));
        assert!(sorted.contains(&"anthropic".to_string()));
        assert!(sorted.contains(&"google".to_string()));
    }

    #[tokio::test]
    async fn test_latency_sorted_empty_candidates() {
        let tracker = Arc::new(LatencyTracker::new(Default::default()));
        let router = GatewayRouter::new().with_latency_tracker(tracker);

        let sorted = router.get_latency_sorted_providers(&[]);
        assert!(sorted.is_empty());
    }

    #[tokio::test]
    async fn test_latency_sorted_single_candidate() {
        let tracker = Arc::new(LatencyTracker::new(Default::default()));
        tracker.inject_for_test(
            Provider::Theta,
            ProviderLatency {
                p50: Duration::from_millis(200),
                p95: Duration::from_millis(200),
                p99: Duration::from_millis(200),
                sample_count: 1,
            },
        );

        let router = GatewayRouter::new().with_latency_tracker(tracker);

        let candidates = vec!["theta".to_string()];
        let sorted = router.get_latency_sorted_providers(&candidates);

        assert_eq!(sorted.len(), 1);
        assert_eq!(sorted[0], "theta");
    }

    #[tokio::test]
    async fn test_latency_sorted_without_tracker() {
        // Router without a latency tracker should return candidates as-is
        let router = GatewayRouter::new();

        let candidates = vec!["openai".to_string(), "anthropic".to_string()];
        let sorted = router.get_latency_sorted_providers(&candidates);

        assert_eq!(sorted, candidates);
    }

    // ==================== route_all Tests ====================

    #[tokio::test]
    async fn test_route_all_claude_returns_anthropic_and_bedrock() {
        let router = GatewayRouter::new();
        let results = router.route_all("claude-sonnet-4-6");
        assert!(
            results.len() >= 2,
            "Claude model should route to at least Anthropic + Bedrock, got {}",
            results.len()
        );
        assert_eq!(
            results[0].2,
            Provider::Anthropic,
            "First endpoint must be native Anthropic"
        );
        assert_eq!(results[0].1, "claude-sonnet-4-6");
        assert_eq!(
            results[1].2,
            Provider::Bedrock,
            "Second endpoint must be Bedrock"
        );
        assert_eq!(results[1].1, "anthropic.claude-sonnet-4-6-v1:0");
    }

    #[tokio::test]
    async fn test_route_all_openai_returns_single() {
        let router = GatewayRouter::new();
        let results = router.route_all("gpt-4o");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].2, Provider::OpenAi);
        assert_eq!(results[0].1, "gpt-4o");
    }

    #[tokio::test]
    async fn test_route_all_google_returns_single() {
        let router = GatewayRouter::new();
        let results = router.route_all("gemini-2.5-flash");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].2, Provider::Google);
    }

    #[tokio::test]
    async fn test_route_all_unknown_model_returns_empty() {
        let router = GatewayRouter::new();
        let results = router.route_all("llama-2-70b");
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_route_all_empty_returns_empty() {
        let router = GatewayRouter::new();
        let results = router.route_all("");
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_route_all_claude_opus_has_bedrock() {
        let router = GatewayRouter::new();
        let results = router.route_all("claude-opus-4-6");
        assert!(results.len() >= 2);
        assert_eq!(results[0].2, Provider::Anthropic);
        assert_eq!(results[1].2, Provider::Bedrock);
    }

    #[tokio::test]
    async fn test_route_all_provides_correct_llm_provider() {
        let router = GatewayRouter::new();
        let results = router.route_all("claude-sonnet-4-6");
        for (provider, model_id, ptype) in &results {
            assert!(
                provider.supports_model(model_id),
                "LlmProvider for {:?} must support model_id '{}'",
                ptype,
                model_id
            );
        }
    }
}
