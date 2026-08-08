use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use uuid::Uuid;

use crate::config::Config;
use crate::gateway::error::GatewayError;
use crate::gateway::latency_tracker::LatencyTracker;
use crate::gateway::model_catalog_cache::ModelCatalogCache;
use crate::gateway::provider_types::{Provider, ResolvedRoute};
use crate::gateway::providers::{
    Ai21Provider, AlibabaProvider, AnthropicProvider, AzureOpenAiProvider, BedrockProvider,
    CerebrasProvider, CloudflareProvider, CohereProvider, DeepInfraProvider, DeepSeekProvider,
    FireworksProvider, GoogleProvider, GroqProvider, HuggingFaceProvider, HyperbolicProvider,
    LambdaProvider, LeptonProvider, LlmProvider, MistralProvider, NovitaProvider, NvidiaProvider,
    OpenAiProvider, OpenRouterProvider, OvhCloudProvider, PerplexityProvider, SambaNovaProvider,
    ThetaDedicatedProvider, ThetaProvider, TogetherProvider, VertexAiProvider, XaiProvider,
};

// ---------------------------------------------------------------------------
// ResolvedKey -- API key with billing type
// ---------------------------------------------------------------------------

/// A resolved API key together with its billing classification.
/// `is_platform = true` means the platform owns the key and the request cost
/// is deducted from the organization's credit wallet.
/// `is_platform = false` means the user supplied the key (BYOK) and only the
/// 3 % platform fee applies.
#[derive(Debug, Clone)]
pub struct ResolvedKey {
    pub key: String,
    pub is_platform: bool,
    /// Per-project base URL override (used by providers with dynamic endpoints,
    /// e.g. `ThetaDedicated` where each project has its own vLLM deployment).
    pub base_url: Option<String>,
}

// ---------------------------------------------------------------------------
// ProviderKeyStore trait
// ---------------------------------------------------------------------------

#[async_trait]
pub trait ProviderKeyStore: Send + Sync {
    async fn get_key(&self, project_id: Uuid, provider: Provider) -> Option<ResolvedKey>;
    async fn get_keys_batch(
        &self,
        project_id: Uuid,
        providers: &[Provider],
    ) -> anyhow::Result<HashMap<Provider, ResolvedKey>>;

    /// Return all providers that have a configured key for this project.
    /// Used by auto-routing to derive a model list when no explicit models
    /// are configured. Default impl returns empty (no integration discovery).
    async fn get_available_providers(&self, _project_id: Uuid) -> anyhow::Result<Vec<Provider>> {
        Ok(vec![])
    }
}

// ---------------------------------------------------------------------------
// GatewayTimeouts
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct GatewayTimeouts {
    pub(crate) openai: Duration,
    pub(crate) anthropic: Duration,
    pub(crate) google: Duration,
    pub(crate) bedrock: Duration,
    pub(crate) theta: Duration,
    pub(crate) theta_dedicated: Duration,
    pub(crate) deepseek: Duration,
    /// Shared timeout for all OpenAI-compatible wrapper providers
    /// (xAI, Mistral, Groq, Together, Fireworks, Perplexity, Cohere,
    /// OpenRouter, Cerebras, DeepInfra, Alibaba, NVIDIA, AI21).
    pub(crate) openai_compat: Duration,
}

impl GatewayTimeouts {
    /// Create timeouts with custom values for each provider.
    pub fn new(
        openai: Duration,
        anthropic: Duration,
        google: Duration,
        bedrock: Duration,
        theta: Duration,
        theta_dedicated: Duration,
        deepseek: Duration,
    ) -> Self {
        Self {
            openai,
            anthropic,
            google,
            bedrock,
            theta,
            theta_dedicated,
            deepseek,
            openai_compat: Duration::from_secs(120),
        }
    }

    pub fn from_config(config: &Config) -> Self {
        Self {
            openai: Duration::from_secs(config.gateway_timeout_openai_seconds),
            anthropic: Duration::from_secs(config.gateway_timeout_anthropic_seconds),
            google: Duration::from_secs(config.gateway_timeout_google_seconds),
            bedrock: Duration::from_secs(config.gateway_timeout_bedrock_seconds),
            theta: Duration::from_secs(config.gateway_timeout_theta_seconds),
            theta_dedicated: Duration::from_secs(config.gateway_timeout_theta_seconds),
            deepseek: Duration::from_secs(config.gateway_timeout_deepseek_seconds),
            openai_compat: Duration::from_secs(config.gateway_timeout_openai_compat_seconds),
        }
    }
}

impl Default for GatewayTimeouts {
    fn default() -> Self {
        Self {
            openai: Duration::from_secs(120),
            anthropic: Duration::from_secs(120),
            google: Duration::from_secs(120),
            bedrock: Duration::from_secs(180),
            theta: Duration::from_secs(120),
            theta_dedicated: Duration::from_secs(120),
            deepseek: Duration::from_secs(120),
            openai_compat: Duration::from_secs(120),
        }
    }
}

/// Provider-specific configuration options.
#[derive(Debug, Clone)]
pub struct ProviderConfig {
    pub(crate) anthropic_api_version: String,
    pub(crate) openai_base_url: Option<String>,
    pub(crate) anthropic_base_url: Option<String>,
    pub(crate) google_base_url: Option<String>,
    pub(crate) deepseek_base_url: Option<String>,
    pub(crate) xai_base_url: Option<String>,
    pub(crate) mistral_base_url: Option<String>,
    pub(crate) groq_base_url: Option<String>,
    pub(crate) together_base_url: Option<String>,
    pub(crate) fireworks_base_url: Option<String>,
    pub(crate) perplexity_base_url: Option<String>,
    pub(crate) cohere_base_url: Option<String>,
    pub(crate) openrouter_base_url: Option<String>,
    pub(crate) cerebras_base_url: Option<String>,
    pub(crate) deepinfra_base_url: Option<String>,
    pub(crate) alibaba_base_url: Option<String>,
    pub(crate) nvidia_base_url: Option<String>,
    pub(crate) ai21_base_url: Option<String>,
    pub(crate) sambanova_base_url: Option<String>,
    pub(crate) lambda_base_url: Option<String>,
    pub(crate) lepton_base_url: Option<String>,
    pub(crate) hyperbolic_base_url: Option<String>,
    pub(crate) ovhcloud_base_url: Option<String>,
    pub(crate) novita_base_url: Option<String>,
    pub(crate) huggingface_base_url: Option<String>,
    pub(crate) cloudflare_base_url: Option<String>,
    pub(crate) azure_openai_base_url: Option<String>,
    pub(crate) vertex_ai_base_url: Option<String>,
}

impl ProviderConfig {
    /// Create provider config with custom values.
    pub fn new(
        anthropic_api_version: impl Into<String>,
        openai_base_url: Option<String>,
        anthropic_base_url: Option<String>,
        google_base_url: Option<String>,
        deepseek_base_url: Option<String>,
    ) -> Self {
        Self {
            anthropic_api_version: anthropic_api_version.into(),
            openai_base_url,
            anthropic_base_url,
            google_base_url,
            deepseek_base_url,
            xai_base_url: None,
            mistral_base_url: None,
            groq_base_url: None,
            together_base_url: None,
            fireworks_base_url: None,
            perplexity_base_url: None,
            cohere_base_url: None,
            openrouter_base_url: None,
            cerebras_base_url: None,
            deepinfra_base_url: None,
            alibaba_base_url: None,
            nvidia_base_url: None,
            ai21_base_url: None,
            sambanova_base_url: None,
            lambda_base_url: None,
            lepton_base_url: None,
            hyperbolic_base_url: None,
            ovhcloud_base_url: None,
            novita_base_url: None,
            huggingface_base_url: None,
            cloudflare_base_url: None,
            azure_openai_base_url: None,
            vertex_ai_base_url: None,
        }
    }

    pub fn from_config(config: &Config) -> Self {
        Self {
            anthropic_api_version: config.gateway_anthropic_api_version.clone(),
            openai_base_url: config.gateway_openai_base_url.clone(),
            anthropic_base_url: config.gateway_anthropic_base_url.clone(),
            google_base_url: config.gateway_google_base_url.clone(),
            deepseek_base_url: config.gateway_deepseek_base_url.clone(),
            xai_base_url: config.gateway_xai_base_url.clone(),
            mistral_base_url: config.gateway_mistral_base_url.clone(),
            groq_base_url: config.gateway_groq_base_url.clone(),
            together_base_url: config.gateway_together_base_url.clone(),
            fireworks_base_url: config.gateway_fireworks_base_url.clone(),
            perplexity_base_url: config.gateway_perplexity_base_url.clone(),
            cohere_base_url: config.gateway_cohere_base_url.clone(),
            openrouter_base_url: config.gateway_openrouter_base_url.clone(),
            cerebras_base_url: config.gateway_cerebras_base_url.clone(),
            deepinfra_base_url: config.gateway_deepinfra_base_url.clone(),
            alibaba_base_url: config.gateway_alibaba_base_url.clone(),
            nvidia_base_url: config.gateway_nvidia_base_url.clone(),
            ai21_base_url: config.gateway_ai21_base_url.clone(),
            sambanova_base_url: config.gateway_sambanova_base_url.clone(),
            lambda_base_url: config.gateway_lambda_base_url.clone(),
            lepton_base_url: config.gateway_lepton_base_url.clone(),
            hyperbolic_base_url: config.gateway_hyperbolic_base_url.clone(),
            ovhcloud_base_url: config.gateway_ovhcloud_base_url.clone(),
            novita_base_url: config.gateway_novita_base_url.clone(),
            huggingface_base_url: config.gateway_huggingface_base_url.clone(),
            cloudflare_base_url: config.gateway_cloudflare_base_url.clone(),
            azure_openai_base_url: config.gateway_azure_openai_base_url.clone(),
            vertex_ai_base_url: config.gateway_vertex_ai_base_url.clone(),
        }
    }
}

impl Default for ProviderConfig {
    fn default() -> Self {
        Self {
            anthropic_api_version: "2023-06-01".to_string(),
            openai_base_url: None,
            anthropic_base_url: None,
            google_base_url: None,
            deepseek_base_url: None,
            xai_base_url: None,
            mistral_base_url: None,
            groq_base_url: None,
            together_base_url: None,
            fireworks_base_url: None,
            perplexity_base_url: None,
            cohere_base_url: None,
            openrouter_base_url: None,
            cerebras_base_url: None,
            deepinfra_base_url: None,
            alibaba_base_url: None,
            nvidia_base_url: None,
            ai21_base_url: None,
            sambanova_base_url: None,
            lambda_base_url: None,
            lepton_base_url: None,
            hyperbolic_base_url: None,
            ovhcloud_base_url: None,
            novita_base_url: None,
            huggingface_base_url: None,
            cloudflare_base_url: None,
            azure_openai_base_url: None,
            vertex_ai_base_url: None,
        }
    }
}

// ---------------------------------------------------------------------------
// ProviderManager
// ---------------------------------------------------------------------------

pub struct ProviderManager {
    providers: HashMap<Provider, Arc<dyn LlmProvider>>,
    latency_tracker: Option<Arc<LatencyTracker>>,
    default_keys: HashMap<Provider, String>,
    model_catalog_cache: Option<ModelCatalogCache>,
}

impl ProviderManager {
    pub fn new(
        timeouts: GatewayTimeouts,
        provider_config: ProviderConfig,
        default_keys: HashMap<Provider, String>,
    ) -> Self {
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

        let mut providers: HashMap<Provider, Arc<dyn LlmProvider>> = HashMap::new();
        providers.insert(
            Provider::OpenAi,
            Arc::new(OpenAiProvider::with_base_url_and_timeout(
                openai_base,
                timeouts.openai,
            )),
        );
        providers.insert(
            Provider::Anthropic,
            Arc::new(AnthropicProvider::with_config(
                anthropic_base,
                timeouts.anthropic,
                provider_config.anthropic_api_version,
            )),
        );
        providers.insert(
            Provider::Google,
            Arc::new(GoogleProvider::with_base_url_and_timeout(
                google_base,
                timeouts.google,
            )),
        );
        providers.insert(
            Provider::Bedrock,
            Arc::new(BedrockProvider::with_timeout(timeouts.bedrock)),
        );
        providers.insert(
            Provider::Theta,
            Arc::new(ThetaProvider::new(timeouts.theta)),
        );
        providers.insert(
            Provider::ThetaDedicated,
            Arc::new(ThetaDedicatedProvider::with_base_url(
                "https://placeholder.thetaedgecloud.com/v1".to_string(),
            )),
        );
        providers.insert(
            Provider::DeepSeek,
            Arc::new(DeepSeekProvider::new(deepseek_base, timeouts.deepseek)),
        );
        providers.insert(
            Provider::Xai,
            Arc::new(XaiProvider::new(xai_base, compat_timeout)),
        );
        providers.insert(
            Provider::Mistral,
            Arc::new(MistralProvider::new(mistral_base, compat_timeout)),
        );
        providers.insert(
            Provider::Groq,
            Arc::new(GroqProvider::new(groq_base, compat_timeout)),
        );
        providers.insert(
            Provider::Together,
            Arc::new(TogetherProvider::new(together_base, compat_timeout)),
        );
        providers.insert(
            Provider::Fireworks,
            Arc::new(FireworksProvider::new(fireworks_base, compat_timeout)),
        );
        providers.insert(
            Provider::Perplexity,
            Arc::new(PerplexityProvider::new(perplexity_base, compat_timeout)),
        );
        providers.insert(
            Provider::Cohere,
            Arc::new(CohereProvider::new(cohere_base, compat_timeout)),
        );
        providers.insert(
            Provider::OpenRouter,
            Arc::new(OpenRouterProvider::new(openrouter_base, compat_timeout)),
        );
        providers.insert(
            Provider::Cerebras,
            Arc::new(CerebrasProvider::new(cerebras_base, compat_timeout)),
        );
        providers.insert(
            Provider::DeepInfra,
            Arc::new(DeepInfraProvider::new(deepinfra_base, compat_timeout)),
        );
        providers.insert(
            Provider::Alibaba,
            Arc::new(AlibabaProvider::new(alibaba_base, compat_timeout)),
        );
        providers.insert(
            Provider::Nvidia,
            Arc::new(NvidiaProvider::new(nvidia_base, compat_timeout)),
        );
        providers.insert(
            Provider::Ai21,
            Arc::new(Ai21Provider::new(ai21_base, compat_timeout)),
        );
        providers.insert(
            Provider::SambaNova,
            Arc::new(SambaNovaProvider::new(sambanova_base, compat_timeout)),
        );
        providers.insert(
            Provider::Lambda,
            Arc::new(LambdaProvider::new(lambda_base, compat_timeout)),
        );
        providers.insert(
            Provider::Lepton,
            Arc::new(LeptonProvider::new(lepton_base, compat_timeout)),
        );
        providers.insert(
            Provider::Hyperbolic,
            Arc::new(HyperbolicProvider::new(hyperbolic_base, compat_timeout)),
        );
        providers.insert(
            Provider::OvhCloud,
            Arc::new(OvhCloudProvider::new(ovhcloud_base, compat_timeout)),
        );
        providers.insert(
            Provider::Novita,
            Arc::new(NovitaProvider::new(novita_base, compat_timeout)),
        );
        providers.insert(
            Provider::HuggingFace,
            Arc::new(HuggingFaceProvider::new(huggingface_base, compat_timeout)),
        );
        providers.insert(
            Provider::Cloudflare,
            Arc::new(CloudflareProvider::new(cloudflare_base, compat_timeout)),
        );
        providers.insert(
            Provider::AzureOpenAi,
            Arc::new(AzureOpenAiProvider::new(azure_openai_base, compat_timeout)),
        );
        providers.insert(
            Provider::VertexAi,
            Arc::new(VertexAiProvider::new(vertex_ai_base, compat_timeout)),
        );

        Self {
            providers,
            latency_tracker: None,
            default_keys,
            model_catalog_cache: None,
        }
    }

    pub fn from_config(config: &Config, default_keys: HashMap<Provider, String>) -> Self {
        Self::new(
            GatewayTimeouts::from_config(config),
            ProviderConfig::from_config(config),
            default_keys,
        )
    }

    pub fn with_latency_tracker(mut self, tracker: Arc<LatencyTracker>) -> Self {
        self.latency_tracker = Some(tracker);
        self
    }

    pub fn with_model_catalog_cache(mut self, cache: ModelCatalogCache) -> Self {
        self.model_catalog_cache = Some(cache);
        self
    }

    pub fn latency_tracker(&self) -> Option<&Arc<LatencyTracker>> {
        self.latency_tracker.as_ref()
    }

    pub fn default_keys(&self) -> &HashMap<Provider, String> {
        &self.default_keys
    }

    /// Get the LlmProvider implementation for a provider.
    pub fn get_llm(&self, provider: Provider) -> Option<&Arc<dyn LlmProvider>> {
        self.providers.get(&provider)
    }

    /// Resolve a model string + provider to a `ResolvedRoute` with everything
    /// needed to execute a request (provider, LLM implementation, API key).
    pub async fn resolve(
        &self,
        model_id: &str,
        provider: Provider,
        project_id: Uuid,
        key_store: &dyn ProviderKeyStore,
    ) -> Result<ResolvedRoute, GatewayError> {
        let llm = self
            .get_llm(provider)
            .ok_or_else(|| {
                GatewayError::InternalError(format!(
                    "No LLM implementation for provider {provider}"
                ))
            })?
            .clone();

        let resolved = key_store
            .get_key(project_id, provider)
            .await
            .ok_or_else(|| {
                GatewayError::MissingProviderKey(format!(
                    "API key not configured for provider '{provider}'"
                ))
            })?;

        Ok(ResolvedRoute {
            provider,
            model_id: model_id.to_string(),
            api_key: resolved.key,
            is_platform_key: resolved.is_platform,
            llm,
        })
    }

    /// Auto-routing: return a `ResolvedRoute` for the first model in the
    /// candidate list that has an available API key.
    ///
    /// Resolution priority:
    /// 1. `candidate_models` — per-request override (the `models` array on the request)
    /// 2. `project_default_models` — project-level setting (`gateway_default_fallback_models`)
    /// 3. Error — no models configured, user must set up defaults in project settings
    ///
    /// `sort_by_latency` triggers P95 latency-based ordering of the final candidate list.
    pub async fn resolve_auto_extended(
        &self,
        project_id: Uuid,
        key_store: &dyn ProviderKeyStore,
        candidate_models: Option<&[String]>,
        project_default_models: Option<&[String]>,
        sort_by_latency: bool,
    ) -> Result<ResolvedRoute, GatewayError> {
        let mut models_to_try: Vec<(Provider, String)> = if let Some(candidates) =
            candidate_models.filter(|c| !c.is_empty())
        {
            candidates
                .iter()
                .filter_map(|s| Provider::from_model_prefix(s).map(|p| (p, s.to_string())))
                .collect()
        } else if let Some(defaults) = project_default_models.filter(|d| !d.is_empty()) {
            defaults
                .iter()
                .filter_map(|s| Provider::from_model_prefix(s).map(|p| (p, s.to_string())))
                .collect()
        } else {
            // Third tier: derive models from the project's configured integrations.
            let available_providers = key_store
                .get_available_providers(project_id)
                .await
                .unwrap_or_default();
            let derived: Vec<(Provider, String)> = available_providers
                .iter()
                .filter_map(|p| {
                    self.model_catalog_cache
                        .as_ref()
                        .and_then(|cache| cache.auto_model_for_provider(p.as_str()))
                        .map(|id| (*p, id))
                })
                .collect();

            if derived.is_empty() {
                tracing::warn!(
                    %project_id,
                    candidate_models = ?candidate_models,
                    project_default_models = ?project_default_models,
                    "Auto-routing failed: no candidate models, no default models, and no provider integrations"
                );
                return Err(GatewayError::MissingProviderKey(
                        "No models configured for auto-routing. \
                         Set default models in project LLM settings, configure a provider integration, \
                         or pass a `models` array in the request."
                            .to_string(),
                    ));
            }

            tracing::info!(
                %project_id,
                derived_models = ?derived.iter().map(|(_, id)| id.as_str()).collect::<Vec<_>>(),
                "Auto-routing: no explicit models configured, derived from available integrations"
            );
            derived
        };

        let providers_needed: Vec<Provider> = models_to_try.iter().map(|(p, _)| *p).collect();
        let available_keys = key_store
            .get_keys_batch(project_id, &providers_needed)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "Failed to fetch provider keys");
                GatewayError::InternalError(format!("Failed to fetch provider keys: {e}"))
            })?;

        // Sort candidates by P95 latency when requested.
        if sort_by_latency {
            if let Some(ref tracker) = self.latency_tracker {
                let provider_slugs: Vec<String> = models_to_try
                    .iter()
                    .map(|(p, _)| p.as_str().to_string())
                    .collect();
                let sorted = tracker.sort_by_p95(&provider_slugs);
                if !sorted.is_empty() {
                    models_to_try.sort_by_key(|(p, _)| {
                        sorted
                            .iter()
                            .position(|s| s == p.as_str())
                            .unwrap_or(usize::MAX)
                    });
                }
            }
        }

        for (provider, model_id) in &models_to_try {
            if let Some(resolved) = available_keys.get(provider) {
                if let Some(llm) = self.get_llm(*provider) {
                    return Ok(ResolvedRoute {
                        provider: *provider,
                        model_id: model_id.clone(),
                        api_key: resolved.key.clone(),
                        is_platform_key: resolved.is_platform,
                        llm: llm.clone(),
                    });
                }
            }
        }

        let tried: Vec<String> = models_to_try
            .iter()
            .map(|(_, id)| id.clone())
            .collect();
        let providers_tried: Vec<String> = models_to_try
            .iter()
            .map(|(p, _)| p.as_str().to_string())
            .collect();
        tracing::warn!(
            %project_id,
            models_tried = ?tried,
            providers_tried = ?providers_tried,
            "Auto-routing failed: models found but no provider keys available"
        );
        Err(GatewayError::MissingProviderKey(
            "No LLM providers configured. Add an API key in project settings or configure GATEWAY_DEFAULT_*_API_KEY environment variables.".to_string(),
        ))
    }

    /// Resolve a model string that may be `"auto"` (or empty) into a
    /// `ResolvedRoute`. Centralises the auto-vs-explicit branching used by
    /// the playground, the in-app agent, and other callers.
    ///
    /// When the model is `"auto"`, routes using `project_default_models` from
    /// the project's gateway settings.
    pub async fn resolve_model_or_auto(
        &self,
        model_str: &str,
        project_id: Uuid,
        key_store: &dyn ProviderKeyStore,
        project_default_models: Option<&[String]>,
    ) -> Result<ResolvedRoute, GatewayError> {
        if model_str == "auto" || model_str.is_empty() {
            self.resolve_auto_extended(project_id, key_store, None, project_default_models, false)
                .await
        } else {
            let provider = Provider::from_model_prefix(model_str)
                .ok_or_else(|| GatewayError::UnsupportedModel(model_str.to_string()))?;
            self.resolve(model_str, provider, project_id, key_store)
                .await
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::model_catalog_cache::CatalogEntry;

    struct MockKeyStore {
        keys: HashMap<Provider, String>,
    }

    fn test_catalog_entry(
        provider_slug: &str,
        model_slug: &str,
        created: i64,
    ) -> CatalogEntry {
        CatalogEntry {
            id: format!("{provider_slug}/{model_slug}"),
            name: model_slug.to_string(),
            provider_slug: provider_slug.to_string(),
            model_slug: model_slug.to_string(),
            context_length: Some(128_000),
            pricing: serde_json::json!({}),
            enabled: true,
            created: Some(created),
        }
    }

    fn test_catalog_cache() -> ModelCatalogCache {
        ModelCatalogCache::new_for_test(vec![
            test_catalog_entry("openai", "gpt-4o", 1700000000),
            test_catalog_entry("anthropic", "claude-sonnet-4-6", 1700000000),
            test_catalog_entry("google", "gemini-2.5-flash", 1700000000),
        ])
    }

    #[async_trait]
    impl ProviderKeyStore for MockKeyStore {
        async fn get_key(&self, _project_id: Uuid, provider: Provider) -> Option<ResolvedKey> {
            self.keys.get(&provider).map(|k| ResolvedKey {
                key: k.clone(),
                is_platform: true,
                base_url: None,
            })
        }

        async fn get_keys_batch(
            &self,
            _project_id: Uuid,
            providers: &[Provider],
        ) -> anyhow::Result<HashMap<Provider, ResolvedKey>> {
            Ok(providers
                .iter()
                .filter_map(|p| {
                    self.keys.get(p).map(|k| {
                        (
                            *p,
                            ResolvedKey {
                                key: k.clone(),
                                is_platform: true,
                                base_url: None,
                            },
                        )
                    })
                })
                .collect())
        }

        async fn get_available_providers(
            &self,
            _project_id: Uuid,
        ) -> anyhow::Result<Vec<Provider>> {
            Ok(self.keys.keys().copied().collect())
        }
    }

    #[test]
    fn provider_manager_has_all_providers() {
        use strum::IntoEnumIterator;

        let pm = ProviderManager::new(
            GatewayTimeouts::default(),
            ProviderConfig::default(),
            HashMap::new(),
        );
        for provider in Provider::iter() {
            assert!(
                pm.get_llm(provider).is_some(),
                "ProviderManager missing LLM for {provider}",
            );
        }
    }

    #[tokio::test]
    async fn resolve_auto_picks_first_with_key_from_project_defaults() {
        let pm = ProviderManager::new(
            GatewayTimeouts::default(),
            ProviderConfig::default(),
            HashMap::new(),
        );

        let key_store = MockKeyStore {
            keys: HashMap::from([(Provider::Google, "google-key".into())]),
        };

        let project_defaults = vec![
            "gpt-4o".to_string(),
            "claude-sonnet-4-6".to_string(),
            "gemini-2.5-flash".to_string(),
        ];

        let route = pm
            .resolve_auto_extended(
                Uuid::new_v4(),
                &key_store,
                None,
                Some(&project_defaults),
                false,
            )
            .await
            .unwrap();

        assert_eq!(route.provider, Provider::Google);
    }

    #[tokio::test]
    async fn resolve_auto_fails_when_no_keys() {
        let pm = ProviderManager::new(
            GatewayTimeouts::default(),
            ProviderConfig::default(),
            HashMap::new(),
        );

        let key_store = MockKeyStore {
            keys: HashMap::new(),
        };

        let project_defaults = vec!["gpt-4o".to_string()];

        let err = pm
            .resolve_auto_extended(
                Uuid::new_v4(),
                &key_store,
                None,
                Some(&project_defaults),
                false,
            )
            .await
            .unwrap_err();

        assert!(matches!(err, GatewayError::MissingProviderKey(_)));
    }

    #[tokio::test]
    async fn resolve_auto_derives_from_integrations_when_no_models_configured() {
        let pm = ProviderManager::new(
            GatewayTimeouts::default(),
            ProviderConfig::default(),
            HashMap::new(),
        )
        .with_model_catalog_cache(test_catalog_cache());

        let key_store = MockKeyStore {
            keys: HashMap::from([(Provider::OpenAi, "openai-key".into())]),
        };

        let route = pm
            .resolve_auto_extended(Uuid::new_v4(), &key_store, None, None, false)
            .await
            .expect("Should derive model from OpenAI integration");

        assert_eq!(route.provider, Provider::OpenAi);
        assert_eq!(route.model_id, "gpt-4o");
    }

    #[tokio::test]
    async fn resolve_auto_fails_when_no_integrations_either() {
        let pm = ProviderManager::new(
            GatewayTimeouts::default(),
            ProviderConfig::default(),
            HashMap::new(),
        );

        let key_store = MockKeyStore {
            keys: HashMap::new(),
        };

        let err = pm
            .resolve_auto_extended(Uuid::new_v4(), &key_store, None, None, false)
            .await
            .unwrap_err();

        assert!(
            matches!(err, GatewayError::MissingProviderKey(_)),
            "Should fail when no candidates, no defaults, and no integrations"
        );
    }

    // ====================================================================
    // resolve_auto_extended tests
    // ====================================================================

    #[tokio::test]
    async fn resolve_auto_extended_uses_candidate_models() {
        let pm = ProviderManager::new(
            GatewayTimeouts::default(),
            ProviderConfig::default(),
            HashMap::new(),
        );

        let key_store = MockKeyStore {
            keys: HashMap::from([
                (Provider::OpenAi, "openai-key".into()),
                (Provider::Anthropic, "anthropic-key".into()),
            ]),
        };

        let candidates = vec!["gpt-4o".to_string()];
        let project_defaults = vec!["claude-sonnet-4-6".to_string()];

        let route = pm
            .resolve_auto_extended(
                Uuid::new_v4(),
                &key_store,
                Some(&candidates),
                Some(&project_defaults),
                false,
            )
            .await
            .unwrap();

        assert_eq!(
            route.provider,
            Provider::OpenAi,
            "Per-request candidates should take priority over project defaults"
        );
    }

    #[tokio::test]
    async fn resolve_auto_extended_falls_back_to_project_defaults_when_no_candidates() {
        let pm = ProviderManager::new(
            GatewayTimeouts::default(),
            ProviderConfig::default(),
            HashMap::new(),
        );

        let key_store = MockKeyStore {
            keys: HashMap::from([(Provider::Anthropic, "anthropic-key".into())]),
        };

        let project_defaults = vec!["gpt-4o".to_string(), "claude-sonnet-4-6".to_string()];

        let route = pm
            .resolve_auto_extended(
                Uuid::new_v4(),
                &key_store,
                None,
                Some(&project_defaults),
                false,
            )
            .await
            .unwrap();

        assert_eq!(
            route.provider,
            Provider::Anthropic,
            "Should use project defaults when no per-request candidates"
        );
    }

    #[tokio::test]
    async fn resolve_auto_extended_with_latency_sort() {
        use crate::gateway::latency_tracker::{LatencyTracker, ProviderLatency};
        use std::time::Duration;

        let tracker = Arc::new(LatencyTracker::new(Default::default()));
        tracker.inject_for_test(
            Provider::OpenAi,
            ProviderLatency {
                p50: Duration::from_millis(50),
                p95: Duration::from_millis(100),
                p99: Duration::from_millis(150),
                sample_count: 10,
            },
        );
        tracker.inject_for_test(
            Provider::Anthropic,
            ProviderLatency {
                p50: Duration::from_millis(500),
                p95: Duration::from_millis(800),
                p99: Duration::from_millis(1000),
                sample_count: 10,
            },
        );

        let pm = ProviderManager::new(
            GatewayTimeouts::default(),
            ProviderConfig::default(),
            HashMap::new(),
        )
        .with_latency_tracker(tracker);

        let key_store = MockKeyStore {
            keys: HashMap::from([
                (Provider::OpenAi, "openai-key".into()),
                (Provider::Anthropic, "anthropic-key".into()),
            ]),
        };

        let candidates = vec!["claude-sonnet-4-6".to_string(), "gpt-4o".to_string()];

        let route = pm
            .resolve_auto_extended(Uuid::new_v4(), &key_store, Some(&candidates), None, true)
            .await
            .unwrap();

        assert_eq!(
            route.provider,
            Provider::OpenAi,
            "Latency sort should pick OpenAi (lower P95) over Anthropic"
        );
    }

    #[tokio::test]
    async fn resolve_auto_extended_empty_candidates_falls_back_to_project_defaults() {
        let pm = ProviderManager::new(
            GatewayTimeouts::default(),
            ProviderConfig::default(),
            HashMap::new(),
        );

        let key_store = MockKeyStore {
            keys: HashMap::from([(Provider::OpenAi, "openai-key".into())]),
        };

        let project_defaults = vec!["gpt-4o".to_string()];

        let route = pm
            .resolve_auto_extended(
                Uuid::new_v4(),
                &key_store,
                Some(&[]),
                Some(&project_defaults),
                false,
            )
            .await
            .unwrap();

        assert_eq!(
            route.provider,
            Provider::OpenAi,
            "Empty candidates should fall through to project defaults"
        );
    }

    #[tokio::test]
    async fn resolve_auto_extended_empty_candidates_and_no_defaults_uses_integrations() {
        let pm = ProviderManager::new(
            GatewayTimeouts::default(),
            ProviderConfig::default(),
            HashMap::new(),
        )
        .with_model_catalog_cache(test_catalog_cache());

        let key_store = MockKeyStore {
            keys: HashMap::from([(Provider::OpenAi, "openai-key".into())]),
        };

        let route = pm
            .resolve_auto_extended(Uuid::new_v4(), &key_store, Some(&[]), None, false)
            .await
            .expect("Empty candidates + no defaults should derive from integrations");

        assert_eq!(route.provider, Provider::OpenAi);
        assert_eq!(route.model_id, "gpt-4o");
    }

    #[tokio::test]
    async fn resolve_auto_extended_empty_candidates_and_no_defaults_and_no_integrations_fails() {
        let pm = ProviderManager::new(
            GatewayTimeouts::default(),
            ProviderConfig::default(),
            HashMap::new(),
        );

        let key_store = MockKeyStore {
            keys: HashMap::new(),
        };

        let result = pm
            .resolve_auto_extended(Uuid::new_v4(), &key_store, Some(&[]), None, false)
            .await;

        assert!(
            result.is_err(),
            "Empty candidates + no defaults + no integrations should fail"
        );
    }

    #[tokio::test]
    async fn resolve_auto_extended_invalid_candidates_fails() {
        let pm = ProviderManager::new(
            GatewayTimeouts::default(),
            ProviderConfig::default(),
            HashMap::new(),
        );

        let key_store = MockKeyStore {
            keys: HashMap::from([(Provider::OpenAi, "openai-key".into())]),
        };

        let candidates = vec!["not-a-real-model".to_string()];

        let result = pm
            .resolve_auto_extended(Uuid::new_v4(), &key_store, Some(&candidates), None, false)
            .await;

        assert!(result.is_err(), "All-invalid candidate list should fail");
    }

    #[tokio::test]
    async fn resolve_auto_extended_latency_sort_without_tracker() {
        let pm = ProviderManager::new(
            GatewayTimeouts::default(),
            ProviderConfig::default(),
            HashMap::new(),
        );

        let key_store = MockKeyStore {
            keys: HashMap::from([(Provider::OpenAi, "openai-key".into())]),
        };

        let candidates = vec!["gpt-4o".to_string()];

        let route = pm
            .resolve_auto_extended(Uuid::new_v4(), &key_store, Some(&candidates), None, true)
            .await
            .unwrap();

        assert_eq!(
            route.provider,
            Provider::OpenAi,
            "Should still work without a latency tracker (no-op sort)"
        );
    }

    #[test]
    fn resolved_key_platform_flag() {
        let platform_key = ResolvedKey {
            key: "sk-platform-123".to_string(),
            is_platform: true,
            base_url: None,
        };
        assert!(platform_key.is_platform);

        let byok_key = ResolvedKey {
            key: "sk-user-456".to_string(),
            is_platform: false,
            base_url: None,
        };
        assert!(!byok_key.is_platform);
    }

    #[tokio::test]
    async fn resolved_route_carries_platform_key_flag() {
        let pm = ProviderManager::new(
            GatewayTimeouts::default(),
            ProviderConfig::default(),
            HashMap::new(),
        );

        let key_store = MockKeyStore {
            keys: HashMap::from([(Provider::OpenAi, "openai-key".into())]),
        };

        let route = pm
            .resolve("gpt-4o", Provider::OpenAi, Uuid::new_v4(), &key_store)
            .await
            .unwrap();

        assert!(
            route.is_platform_key,
            "MockKeyStore returns is_platform=true"
        );
    }

    struct ByokKeyStore {
        keys: HashMap<Provider, String>,
    }

    #[async_trait]
    impl ProviderKeyStore for ByokKeyStore {
        async fn get_key(&self, _project_id: Uuid, provider: Provider) -> Option<ResolvedKey> {
            self.keys.get(&provider).map(|k| ResolvedKey {
                key: k.clone(),
                is_platform: false,
                base_url: None,
            })
        }

        async fn get_keys_batch(
            &self,
            _project_id: Uuid,
            providers: &[Provider],
        ) -> anyhow::Result<HashMap<Provider, ResolvedKey>> {
            Ok(providers
                .iter()
                .filter_map(|p| {
                    self.keys.get(p).map(|k| {
                        (
                            *p,
                            ResolvedKey {
                                key: k.clone(),
                                is_platform: false,
                                base_url: None,
                            },
                        )
                    })
                })
                .collect())
        }
    }

    #[tokio::test]
    async fn resolved_route_byok_flag() {
        let pm = ProviderManager::new(
            GatewayTimeouts::default(),
            ProviderConfig::default(),
            HashMap::new(),
        );

        let key_store = ByokKeyStore {
            keys: HashMap::from([(Provider::OpenAi, "sk-user-key".into())]),
        };

        let route = pm
            .resolve("gpt-4o", Provider::OpenAi, Uuid::new_v4(), &key_store)
            .await
            .unwrap();

        assert!(
            !route.is_platform_key,
            "ByokKeyStore returns is_platform=false"
        );
    }

    #[tokio::test]
    async fn batch_resolution_returns_platform_keys() {
        let key_store = MockKeyStore {
            keys: HashMap::from([
                (Provider::OpenAi, "openai-key".into()),
                (Provider::Anthropic, "anthropic-key".into()),
            ]),
        };

        let result = key_store
            .get_keys_batch(Uuid::new_v4(), &[Provider::OpenAi, Provider::Anthropic])
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
        assert!(result[&Provider::OpenAi].is_platform);
        assert!(result[&Provider::Anthropic].is_platform);
    }

    #[tokio::test]
    async fn resolve_auto_derives_google_model_from_integration() {
        let pm = ProviderManager::new(
            GatewayTimeouts::default(),
            ProviderConfig::default(),
            HashMap::new(),
        )
        .with_model_catalog_cache(test_catalog_cache());

        let key_store = MockKeyStore {
            keys: HashMap::from([(Provider::Google, "google-key".into())]),
        };

        let route = pm
            .resolve_auto_extended(Uuid::new_v4(), &key_store, None, None, false)
            .await
            .expect("Should derive Gemini model from Google integration");

        assert_eq!(route.provider, Provider::Google);
        assert_eq!(route.model_id, "gemini-2.5-flash");
    }

    #[tokio::test]
    async fn resolve_auto_integration_fallback_skips_providers_without_catalog_entry() {
        let pm = ProviderManager::new(
            GatewayTimeouts::default(),
            ProviderConfig::default(),
            HashMap::new(),
        )
        .with_model_catalog_cache(test_catalog_cache());

        let key_store = MockKeyStore {
            keys: HashMap::from([(Provider::Bedrock, "bedrock-creds".into())]),
        };

        let result = pm
            .resolve_auto_extended(Uuid::new_v4(), &key_store, None, None, false)
            .await;

        assert!(
            result.is_err(),
            "Bedrock has no catalog entry, so integration fallback should produce an empty list and fail"
        );
    }

    #[tokio::test]
    async fn resolve_auto_integration_fallback_with_multiple_providers() {
        let pm = ProviderManager::new(
            GatewayTimeouts::default(),
            ProviderConfig::default(),
            HashMap::new(),
        )
        .with_model_catalog_cache(test_catalog_cache());

        let key_store = MockKeyStore {
            keys: HashMap::from([
                (Provider::Bedrock, "bedrock-creds".into()),
                (Provider::Anthropic, "anthropic-key".into()),
            ]),
        };

        let route = pm
            .resolve_auto_extended(Uuid::new_v4(), &key_store, None, None, false)
            .await
            .expect("Should skip Bedrock (no catalog entry) and use Anthropic");

        assert_eq!(route.provider, Provider::Anthropic);
        assert_eq!(route.model_id, "claude-sonnet-4-6");
    }

    #[tokio::test]
    async fn resolve_auto_explicit_defaults_take_precedence_over_integrations() {
        let pm = ProviderManager::new(
            GatewayTimeouts::default(),
            ProviderConfig::default(),
            HashMap::new(),
        );

        let key_store = MockKeyStore {
            keys: HashMap::from([
                (Provider::OpenAi, "openai-key".into()),
                (Provider::Google, "google-key".into()),
            ]),
        };

        let project_defaults = vec!["gemini-2.5-flash".to_string()];
        let route = pm
            .resolve_auto_extended(
                Uuid::new_v4(),
                &key_store,
                None,
                Some(&project_defaults),
                false,
            )
            .await
            .expect("Should use explicit project defaults, not integration fallback");

        assert_eq!(route.provider, Provider::Google);
        assert_eq!(route.model_id, "gemini-2.5-flash");
    }
}
