use std::fmt;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use strum::EnumIter;

use crate::gateway::providers::LlmProvider;

// ---------------------------------------------------------------------------
// Provider
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumIter)]
pub enum Provider {
    OpenAi,
    Anthropic,
    Google,
    Bedrock,
    Theta,
    ThetaDedicated,
    DeepSeek,
    Xai,
    Mistral,
    Groq,
    Together,
    Fireworks,
    Perplexity,
    Cohere,
    OpenRouter,
    Cerebras,
    DeepInfra,
    Alibaba,
    Nvidia,
    Ai21,
    SambaNova,
    Lambda,
    Lepton,
    Hyperbolic,
    OvhCloud,
    Novita,
    HuggingFace,
    Cloudflare,
    AzureOpenAi,
    VertexAi,
}

impl Provider {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::OpenAi => "openai",
            Self::Anthropic => "anthropic",
            Self::Google => "google",
            Self::Bedrock => "bedrock",
            Self::Theta => "theta",
            Self::ThetaDedicated => "theta-dedicated",
            Self::DeepSeek => "deepseek",
            Self::Xai => "x-ai",
            Self::Mistral => "mistralai",
            Self::Groq => "groq",
            Self::Together => "together",
            Self::Fireworks => "fireworks",
            Self::Perplexity => "perplexity",
            Self::Cohere => "cohere",
            Self::OpenRouter => "openrouter",
            Self::Cerebras => "cerebras",
            Self::DeepInfra => "deepinfra",
            Self::Alibaba => "qwen",
            Self::Nvidia => "nvidia",
            Self::Ai21 => "ai21",
            Self::SambaNova => "sambanova",
            Self::Lambda => "lambda",
            Self::Lepton => "lepton",
            Self::Hyperbolic => "hyperbolic",
            Self::OvhCloud => "ovhcloud",
            Self::Novita => "novita",
            Self::HuggingFace => "huggingface",
            Self::Cloudflare => "cloudflare",
            Self::AzureOpenAi => "azure-openai",
            Self::VertexAi => "vertex-ai",
        }
    }

    pub fn display_name(&self) -> &'static str {
        match self {
            Self::OpenAi => "OpenAI",
            Self::Anthropic => "Anthropic",
            Self::Google => "Google (Gemini)",
            Self::Bedrock => "AWS Bedrock",
            Self::Theta => "Theta EdgeCloud",
            Self::ThetaDedicated => "Theta Dedicated",
            Self::DeepSeek => "DeepSeek",
            Self::Xai => "xAI (Grok)",
            Self::Mistral => "Mistral AI",
            Self::Groq => "Groq",
            Self::Together => "Together AI",
            Self::Fireworks => "Fireworks AI",
            Self::Perplexity => "Perplexity",
            Self::Cohere => "Cohere",
            Self::OpenRouter => "OpenRouter",
            Self::Cerebras => "Cerebras",
            Self::DeepInfra => "DeepInfra",
            Self::Alibaba => "Alibaba (Qwen)",
            Self::Nvidia => "NVIDIA NIM",
            Self::Ai21 => "AI21",
            Self::SambaNova => "SambaNova",
            Self::Lambda => "Lambda",
            Self::Lepton => "Lepton AI",
            Self::Hyperbolic => "Hyperbolic",
            Self::OvhCloud => "OVHcloud AI",
            Self::Novita => "Novita AI",
            Self::HuggingFace => "Hugging Face",
            Self::Cloudflare => "Cloudflare Workers AI",
            Self::AzureOpenAi => "Azure OpenAI",
            Self::VertexAi => "Google Vertex AI",
        }
    }

    pub fn description(&self) -> &'static str {
        match self {
            Self::OpenAi => "GPT-4o, o1, o3 reasoning models",
            Self::Anthropic => "Claude Sonnet 4.6, Claude Opus 4.6 with extended thinking",
            Self::Google => "Gemini 2.5 Flash, Gemini 2.5 Pro",
            Self::Bedrock => "Claude, Llama, Mistral, and other models via AWS",
            Self::Theta => "Llama, Qwen, GPT OSS, MiniMax via Theta EdgeCloud on-demand",
            Self::ThetaDedicated => "Custom vLLM deployment on Theta EdgeCloud",
            Self::DeepSeek => "DeepSeek-V3 chat, DeepSeek-R1 reasoning models",
            Self::Xai => "Grok models for reasoning and chat",
            Self::Mistral => "Mistral Large, Codestral, and Mixtral models",
            Self::Groq => "Ultra-fast inference on LPU hardware",
            Self::Together => "Open-source models: Llama, Qwen, Mixtral, and more",
            Self::Fireworks => "Fast open-source model inference",
            Self::Perplexity => "Search-augmented Sonar models",
            Self::Cohere => "Command R+ and enterprise RAG models",
            Self::OpenRouter => "Meta-provider routing to 200+ models",
            Self::Cerebras => "Ultra-fast inference on Wafer-Scale Engine",
            Self::DeepInfra => "Low-cost open-source model hosting",
            Self::Alibaba => "Qwen models via DashScope",
            Self::Nvidia => "Nemotron and enterprise models via NIM",
            Self::Ai21 => "Jamba models for enterprise use",
            Self::SambaNova => "Fast inference on custom RDU hardware",
            Self::Lambda => "GPU cloud with open-source model inference",
            Self::Lepton => "Fast serverless AI model inference",
            Self::Hyperbolic => "Fast open-source model inference",
            Self::OvhCloud => "European cloud AI inference endpoints",
            Self::Novita => "Fast inference with wide model catalog",
            Self::HuggingFace => "Inference across 100k+ models via unified API",
            Self::Cloudflare => "Edge AI inference in 200+ cities",
            Self::AzureOpenAi => "OpenAI models via Microsoft Azure",
            Self::VertexAi => "Enterprise Gemini on Google Cloud",
        }
    }

    pub fn docs_url(&self) -> &'static str {
        match self {
            Self::OpenAi => "https://platform.openai.com/api-keys",
            Self::Anthropic => "https://console.anthropic.com/settings/keys",
            Self::Google => "https://aistudio.google.com/app/apikey",
            Self::Bedrock => "https://docs.aws.amazon.com/bedrock/",
            Self::Theta => "https://www.thetaedgecloud.com/dashboard/api-keys",
            Self::ThetaDedicated => "https://www.thetaedgecloud.com/dashboard/ai/llm",
            Self::DeepSeek => "https://platform.deepseek.com/api_keys",
            Self::Xai => "https://console.x.ai/",
            Self::Mistral => "https://console.mistral.ai/api-keys",
            Self::Groq => "https://console.groq.com/keys",
            Self::Together => "https://api.together.xyz/settings/api-keys",
            Self::Fireworks => "https://fireworks.ai/account/api-keys",
            Self::Perplexity => "https://www.perplexity.ai/settings/api",
            Self::Cohere => "https://dashboard.cohere.com/api-keys",
            Self::OpenRouter => "https://openrouter.ai/settings/keys",
            Self::Cerebras => "https://cloud.cerebras.ai/",
            Self::DeepInfra => "https://deepinfra.com/dash/api_keys",
            Self::Alibaba => "https://dashscope.console.aliyun.com/apiKey",
            Self::Nvidia => "https://build.nvidia.com/",
            Self::Ai21 => "https://studio.ai21.com/account/api-key",
            Self::SambaNova => "https://cloud.sambanova.ai/apis",
            Self::Lambda => "https://docs.lambdalabs.com/",
            Self::Lepton => "https://www.lepton.ai/docs",
            Self::Hyperbolic => "https://docs.hyperbolic.xyz/",
            Self::OvhCloud => "https://endpoints.ai.cloud.ovh.net/",
            Self::Novita => "https://novita.ai/docs",
            Self::HuggingFace => "https://huggingface.co/settings/tokens",
            Self::Cloudflare => "https://dash.cloudflare.com/profile/api-tokens",
            Self::AzureOpenAi => "https://portal.azure.com/",
            Self::VertexAi => "https://console.cloud.google.com/vertex-ai",
        }
    }

    /// Credential type required by this provider.
    ///
    /// Drives the frontend config modal: `"api_key"` shows a single key input,
    /// `"aws_credentials"` shows access-key / secret / region fields.
    pub fn auth_type(&self) -> &'static str {
        match self {
            Self::Bedrock => "aws_credentials",
            Self::ThetaDedicated => "theta_dedicated",
            Self::Cloudflare => "cloudflare",
            Self::AzureOpenAi => "azure_openai",
            Self::VertexAi => "gcp_service_account",
            _ => "api_key",
        }
    }

    /// Whether the provider supports SSE streaming for chat completions.
    pub fn supports_streaming(&self) -> bool {
        true
    }

    /// Resolve a model string to a `Provider` using the static prefix table.
    /// This is the replacement for the old `Model::from_api_string()` — it
    /// returns only the provider, not a typed model enum.
    pub fn from_model_prefix(model_str: &str) -> Option<Self> {
        static PREFIX_TABLE: LazyLock<Vec<(&'static str, Provider)>> = LazyLock::new(|| {
            use strum::IntoEnumIterator;
            let mut entries: Vec<(&'static str, Provider)> = Vec::new();
            for provider in Provider::iter() {
                for &prefix in provider.model_prefixes() {
                    entries.push((prefix, provider));
                }
            }
            entries.sort_by(|a, b| b.0.len().cmp(&a.0.len()));
            entries
        });

        for (prefix, provider) in PREFIX_TABLE.iter() {
            if model_str.starts_with(prefix) {
                return Some(*provider);
            }
        }
        None
    }

    /// Model-string prefixes owned by this provider, sorted longest-first.
    ///
    /// Exact-match entries like `"o1"` must appear *after* their dashed
    /// counterparts (`"o1-"`) so that length-descending sorting picks the
    /// more specific dash prefix first (e.g. `"o1-mini"` matches `"o1-"`).
    pub fn model_prefixes(&self) -> &'static [&'static str] {
        match self {
            Self::OpenAi => &[
                "text-embedding-",
                "chatgpt-",
                "whisper-",
                "dall-e-",
                "gpt-",
                "tts-",
                "o1-",
                "o3-",
                "o4-",
                "o1",
                "o3",
                "o4",
            ],
            Self::Anthropic => &["claude-"],
            Self::Google => &["gemini-"],
            Self::Bedrock => &[
                "anthropic.",
                "mistral.",
                "bedrock/",
                "amazon.",
                "cohere.",
                "meta.",
                "ai21.",
            ],
            Self::Theta => &["theta/"],
            Self::ThetaDedicated => &["theta-dedicated/"],
            Self::DeepSeek => &["deepseek/"],
            Self::Xai => &["grok-"],
            Self::Mistral => &["mistral/"],
            Self::Groq => &["groq/"],
            Self::Together => &["together/"],
            Self::Fireworks => &["fireworks/"],
            Self::Perplexity => &["perplexity/"],
            Self::Cohere => &["cohere/"],
            Self::OpenRouter => &["openrouter/"],
            Self::Cerebras => &["cerebras/"],
            Self::DeepInfra => &["deepinfra/"],
            Self::Alibaba => &["qwen/"],
            Self::Nvidia => &["nvidia/"],
            Self::Ai21 => &["ai21/"],
            Self::SambaNova => &["sambanova/"],
            Self::Lambda => &["lambda/"],
            Self::Lepton => &["lepton/"],
            Self::Hyperbolic => &["hyperbolic/"],
            Self::OvhCloud => &["ovhcloud/"],
            Self::Novita => &["novita/"],
            Self::HuggingFace => &["huggingface/"],
            Self::Cloudflare => &["cloudflare/"],
            Self::AzureOpenAi => &["azure/"],
            Self::VertexAi => &["vertex/"],
        }
    }
}

impl fmt::Display for Provider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Error returned when a string cannot be parsed into a [`Provider`].
#[derive(Debug, Clone, thiserror::Error)]
#[error("unknown provider: {0}")]
pub struct UnknownProviderError(pub String);

impl FromStr for Provider {
    type Err = UnknownProviderError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "openai" => Ok(Self::OpenAi),
            "anthropic" => Ok(Self::Anthropic),
            "google" => Ok(Self::Google),
            "bedrock" => Ok(Self::Bedrock),
            "theta" => Ok(Self::Theta),
            "theta-dedicated" => Ok(Self::ThetaDedicated),
            "deepseek" => Ok(Self::DeepSeek),
            "x-ai" => Ok(Self::Xai),
            "mistralai" => Ok(Self::Mistral),
            "groq" => Ok(Self::Groq),
            "together" => Ok(Self::Together),
            "fireworks" => Ok(Self::Fireworks),
            "perplexity" => Ok(Self::Perplexity),
            "cohere" => Ok(Self::Cohere),
            "openrouter" => Ok(Self::OpenRouter),
            "cerebras" => Ok(Self::Cerebras),
            "deepinfra" => Ok(Self::DeepInfra),
            "qwen" => Ok(Self::Alibaba),
            "nvidia" => Ok(Self::Nvidia),
            "ai21" => Ok(Self::Ai21),
            "sambanova" => Ok(Self::SambaNova),
            "lambda" => Ok(Self::Lambda),
            "lepton" => Ok(Self::Lepton),
            "hyperbolic" => Ok(Self::Hyperbolic),
            "ovhcloud" => Ok(Self::OvhCloud),
            "novita" => Ok(Self::Novita),
            "huggingface" => Ok(Self::HuggingFace),
            "cloudflare" => Ok(Self::Cloudflare),
            "azure-openai" => Ok(Self::AzureOpenAi),
            "vertex-ai" => Ok(Self::VertexAi),
            other => Err(UnknownProviderError(other.to_string())),
        }
    }
}

impl Serialize for Provider {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Provider {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Provider::from_str(&s).map_err(serde::de::Error::custom)
    }
}

// ---------------------------------------------------------------------------
// ResolvedRoute
// ---------------------------------------------------------------------------

pub struct ResolvedRoute {
    pub provider: Provider,
    pub model_id: String,
    pub api_key: String,
    /// Whether the key is a platform-managed key (true) or user's own key / BYOK (false).
    pub is_platform_key: bool,
    pub llm: Arc<dyn LlmProvider>,
}

impl fmt::Debug for ResolvedRoute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedRoute")
            .field("provider", &self.provider)
            .field("model_id", &self.model_id)
            .field("api_key", &"***")
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Multi-provider model endpoint registry
// ---------------------------------------------------------------------------

/// An endpoint that can serve a particular canonical model.
#[derive(Debug, Clone)]
pub struct ModelEndpoint {
    pub provider: Provider,
    /// The model string to send to this provider's API.
    pub provider_model_id: String,
}

/// Return all known provider endpoints that can serve `canonical_model`.
///
/// The first entry is always the "native" provider (e.g., Anthropic for Claude).
/// Additional entries are alternate providers (e.g., Bedrock for Claude).
/// Callers should filter by key availability before use.
pub fn get_model_endpoints(canonical_model: &str) -> Vec<ModelEndpoint> {
    let Some(provider) = Provider::from_model_prefix(canonical_model) else {
        return Vec::new();
    };

    let native = ModelEndpoint {
        provider,
        provider_model_id: canonical_model.to_string(),
    };

    let mut endpoints = vec![native];

    if provider == Provider::Anthropic {
        if let Some(bedrock_id) = anthropic_to_bedrock_model_id(canonical_model) {
            endpoints.push(ModelEndpoint {
                provider: Provider::Bedrock,
                provider_model_id: bedrock_id,
            });
        }
    }

    endpoints
}

/// Map a canonical Anthropic model name to the equivalent Bedrock model ID.
fn anthropic_to_bedrock_model_id(anthropic_model: &str) -> Option<String> {
    let bedrock_id = match anthropic_model {
        "claude-sonnet-4-6" => "anthropic.claude-sonnet-4-6-v1:0",
        "claude-opus-4-6" => "anthropic.claude-opus-4-6-v1:0",
        "claude-haiku-4-5-20251001" | "claude-haiku-4-5" => {
            "anthropic.claude-haiku-4-5-20251001-v1:0"
        }
        "claude-sonnet-4-20250514" | "claude-sonnet-4" => "anthropic.claude-sonnet-4-20250514-v1:0",
        "claude-opus-4-20250514" | "claude-opus-4" => "anthropic.claude-opus-4-20250514-v1:0",
        _ => return None,
    };
    Some(bedrock_id.to_string())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_round_trip() {
        use strum::IntoEnumIterator;
        for p in Provider::iter() {
            assert_eq!(Provider::from_str(p.as_str()).unwrap(), p);
            assert_eq!(p.to_string(), p.as_str());
        }
    }

    #[test]
    fn provider_from_str_unknown() {
        assert!(Provider::from_str("unknown").is_err());
    }

    #[test]
    fn from_model_prefix_known() {
        assert_eq!(Provider::from_model_prefix("gpt-4o"), Some(Provider::OpenAi));
        assert_eq!(Provider::from_model_prefix("claude-sonnet-4-6"), Some(Provider::Anthropic));
        assert_eq!(Provider::from_model_prefix("gemini-2.5-flash"), Some(Provider::Google));
        assert_eq!(Provider::from_model_prefix("deepseek/deepseek-chat"), Some(Provider::DeepSeek));
    }

    #[test]
    fn from_model_prefix_bedrock() {
        assert_eq!(
            Provider::from_model_prefix("bedrock/anthropic.claude-3-sonnet-20240229-v1:0"),
            Some(Provider::Bedrock)
        );
        assert_eq!(
            Provider::from_model_prefix("anthropic.claude-3-opus-20240229-v1:0"),
            Some(Provider::Bedrock)
        );
    }

    #[test]
    fn from_model_prefix_unrecognised() {
        assert!(Provider::from_model_prefix("llama-2-70b").is_none());
        assert!(Provider::from_model_prefix("").is_none());
    }

    #[test]
    fn from_model_prefix_bare_o_models() {
        assert_eq!(Provider::from_model_prefix("o1"), Some(Provider::OpenAi));
        assert_eq!(Provider::from_model_prefix("o3"), Some(Provider::OpenAi));
    }

    #[test]
    fn from_model_prefix_dashed_o_variants() {
        assert_eq!(Provider::from_model_prefix("o1-mini"), Some(Provider::OpenAi));
        assert_eq!(Provider::from_model_prefix("o3-mini"), Some(Provider::OpenAi));
    }

    // Multi-provider model endpoint registry

    #[test]
    fn model_endpoints_claude_sonnet_has_native_and_bedrock() {
        let eps = get_model_endpoints("claude-sonnet-4-6");
        assert!(
            eps.len() >= 2,
            "Claude Sonnet 4.6 should have at least Anthropic + Bedrock"
        );
        assert_eq!(eps[0].provider, Provider::Anthropic);
        assert_eq!(eps[0].provider_model_id, "claude-sonnet-4-6");
        assert_eq!(eps[1].provider, Provider::Bedrock);
        assert_eq!(eps[1].provider_model_id, "anthropic.claude-sonnet-4-6-v1:0");
    }

    #[test]
    fn model_endpoints_claude_opus_has_native_and_bedrock() {
        let eps = get_model_endpoints("claude-opus-4-6");
        assert!(eps.len() >= 2);
        assert_eq!(eps[0].provider, Provider::Anthropic);
        assert_eq!(eps[1].provider, Provider::Bedrock);
        assert_eq!(eps[1].provider_model_id, "anthropic.claude-opus-4-6-v1:0");
    }

    #[test]
    fn model_endpoints_openai_model_only_has_native() {
        let eps = get_model_endpoints("gpt-4o");
        assert_eq!(eps.len(), 1);
        assert_eq!(eps[0].provider, Provider::OpenAi);
        assert_eq!(eps[0].provider_model_id, "gpt-4o");
    }

    #[test]
    fn model_endpoints_unknown_model_returns_empty() {
        let eps = get_model_endpoints("llama-2-70b");
        assert!(eps.is_empty());
    }

    #[test]
    fn model_endpoints_empty_string_returns_empty() {
        let eps = get_model_endpoints("");
        assert!(eps.is_empty());
    }

    #[test]
    fn model_endpoints_native_is_always_first() {
        for model in &[
            "claude-sonnet-4-6",
            "claude-opus-4-6",
            "claude-haiku-4-5-20251001",
            "claude-sonnet-4",
            "claude-opus-4",
        ] {
            let eps = get_model_endpoints(model);
            assert!(!eps.is_empty(), "Expected endpoints for {model}");
            assert_eq!(
                eps[0].provider,
                Provider::Anthropic,
                "First endpoint for {model} must be native Anthropic"
            );
        }
    }

    #[test]
    fn provider_serde_round_trip() {
        use strum::IntoEnumIterator;
        for p in Provider::iter() {
            let json = serde_json::to_value(p).unwrap();
            assert_eq!(
                json.as_str().unwrap(),
                p.as_str(),
                "Serialize mismatch for {:?}",
                p
            );
            let back: Provider = serde_json::from_value(json).unwrap();
            assert_eq!(back, p, "Deserialize mismatch for {:?}", p);
        }
    }

    #[test]
    fn provider_serde_tricky_variants() {
        assert_eq!(serde_json::to_value(Provider::Xai).unwrap(), "x-ai");
        assert_eq!(serde_json::to_value(Provider::AzureOpenAi).unwrap(), "azure-openai");
        assert_eq!(serde_json::to_value(Provider::VertexAi).unwrap(), "vertex-ai");
        assert_eq!(serde_json::to_value(Provider::ThetaDedicated).unwrap(), "theta-dedicated");
        assert_eq!(serde_json::to_value(Provider::Alibaba).unwrap(), "qwen");
        assert_eq!(serde_json::to_value(Provider::Mistral).unwrap(), "mistralai");
    }
}
