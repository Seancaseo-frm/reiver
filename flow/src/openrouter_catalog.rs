//! Parser for the OpenRouter `/api/v1/models` response.
//!
//! This module deserializes the full OpenRouter model catalog and provides
//! helpers for extracting provider/model slugs, converting pricing to our
//! per-1M format, and checking whether a provider has routing code in the
//! gateway.

use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;

/// Top-level response from `GET https://openrouter.ai/api/v1/models`.
#[derive(Debug, Deserialize)]
pub struct OpenRouterResponse {
    pub data: Vec<ModelEntry>,
}

/// A single model entry from the OpenRouter catalog.
#[derive(Debug, Deserialize)]
pub struct ModelEntry {
    /// e.g. `"openai/gpt-4o"`, `"anthropic/claude-sonnet-4.6"`, `"x-ai/grok-4.3"`
    pub id: String,
    pub name: String,
    pub created: Option<i64>,
    pub context_length: Option<u64>,
    pub architecture: Option<Architecture>,
    pub pricing: Option<Pricing>,
    pub top_provider: Option<TopProvider>,
    #[serde(default)]
    pub supported_parameters: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct Architecture {
    pub modality: Option<String>,
    #[serde(default)]
    pub input_modalities: Vec<String>,
    #[serde(default)]
    pub output_modalities: Vec<String>,
    pub tokenizer: Option<String>,
}

/// Pricing values are per-token strings (e.g. `"0.0000025"` or `"0"`).
#[derive(Debug, Deserialize)]
pub struct Pricing {
    pub prompt: Option<String>,
    pub completion: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TopProvider {
    pub context_length: Option<u64>,
    pub max_completion_tokens: Option<u64>,
    pub is_moderated: Option<bool>,
}

impl ModelEntry {
    /// Extracts the provider slug from the model ID.
    /// `"openai/gpt-4o"` -> `"openai"`, `"x-ai/grok-4.3"` -> `"x-ai"`
    pub fn provider_slug(&self) -> &str {
        self.id.split('/').next().unwrap_or(&self.id)
    }

    /// Extracts the model slug from the model ID, stripping the provider prefix.
    /// `"openai/gpt-4o"` -> `"gpt-4o"`, `"anthropic/claude-sonnet-4.6:free"` -> `"claude-sonnet-4.6:free"`
    pub fn model_slug(&self) -> &str {
        self.id
            .split_once('/')
            .map(|(_, model)| model)
            .unwrap_or(&self.id)
    }

    /// Whether this is a free-tier variant (ID ends with `:free`).
    pub fn is_free_variant(&self) -> bool {
        self.id.ends_with(":free")
    }

    /// Whether this is an alias model (provider starts with `~`).
    /// These always point to the latest version of a model family.
    pub fn is_alias(&self) -> bool {
        self.id.starts_with('~')
    }

    /// Input cost per 1M tokens as `Decimal`, converted from the per-token string.
    /// Returns `None` if pricing is absent or unparseable.
    pub fn input_cost_per_1m(&self) -> Option<Decimal> {
        per_token_to_per_million(self.pricing.as_ref()?.prompt.as_deref()?)
    }

    /// Output cost per 1M tokens as `Decimal`, converted from the per-token string.
    /// Returns `None` if pricing is absent or unparseable.
    pub fn output_cost_per_1m(&self) -> Option<Decimal> {
        per_token_to_per_million(self.pricing.as_ref()?.completion.as_deref()?)
    }
}

/// Converts a per-token price string (e.g. `"0.0000025"`) to per-1M tokens.
fn per_token_to_per_million(per_token: &str) -> Option<Decimal> {
    let d = Decimal::from_str(per_token).ok()?;
    Some(d * Decimal::from(1_000_000))
}

/// Providers that have routing code in the gateway today.
/// These use OpenRouter's slug format as the canonical name.
const SUPPORTED_PROVIDERS: &[&str] = &[
    "openai",
    "anthropic",
    "google",
    "deepseek",
    "x-ai",
    "mistralai",
    "cohere",
    "perplexity",
    "nvidia",
    "ai21",
    "qwen",
    "meta-llama",
    "amazon",
    "openrouter",
];

/// Returns `true` if the given OpenRouter provider slug has routing code
/// in the gateway.
pub fn is_supported_provider(slug: &str) -> bool {
    SUPPORTED_PROVIDERS.contains(&slug)
}

/// Returns the full list of supported provider slugs.
pub fn supported_providers() -> &'static [&'static str] {
    SUPPORTED_PROVIDERS
}

impl OpenRouterResponse {
    /// Returns only models from providers we currently have routing code for.
    pub fn supported_models(&self) -> Vec<&ModelEntry> {
        self.data
            .iter()
            .filter(|m| is_supported_provider(m.provider_slug()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn load_fixture() -> OpenRouterResponse {
        let json = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("pricing.json"),
        )
        .expect("pricing.json fixture must exist in flow/");
        serde_json::from_str(&json).expect("pricing.json must be valid OpenRouter response")
    }

    #[test]
    fn parse_full_response() {
        let catalog = load_fixture();
        assert!(
            catalog.data.len() > 300,
            "expected 300+ models, got {}",
            catalog.data.len()
        );
    }

    #[test]
    fn provider_slug_extraction() {
        let catalog = load_fixture();
        let gpt4o = catalog
            .data
            .iter()
            .find(|m| m.id == "openai/gpt-4o")
            .expect("gpt-4o must exist");
        assert_eq!(gpt4o.provider_slug(), "openai");
        assert_eq!(gpt4o.model_slug(), "gpt-4o");
    }

    #[test]
    fn provider_slug_with_hyphen() {
        let catalog = load_fixture();
        let xai = catalog
            .data
            .iter()
            .find(|m| m.provider_slug() == "x-ai")
            .expect("x-ai provider must have models");
        assert_eq!(xai.provider_slug(), "x-ai");
    }

    #[test]
    fn pricing_conversion_gpt4o() {
        let catalog = load_fixture();
        let gpt4o = catalog
            .data
            .iter()
            .find(|m| m.id == "openai/gpt-4o")
            .expect("gpt-4o must exist");

        let input = gpt4o.input_cost_per_1m().expect("must have input pricing");
        let output = gpt4o.output_cost_per_1m().expect("must have output pricing");
        assert_eq!(input, dec!(2.5), "GPT-4o input should be $2.50 / 1M tokens");
        assert_eq!(
            output,
            dec!(10.0),
            "GPT-4o output should be $10.00 / 1M tokens"
        );
    }

    #[test]
    fn pricing_conversion_free_model() {
        let catalog = load_fixture();
        let free = catalog
            .data
            .iter()
            .find(|m| m.is_free_variant())
            .expect("must have at least one :free model");
        assert!(free.id.ends_with(":free"));
        let input = free.input_cost_per_1m().unwrap_or_default();
        assert_eq!(input, dec!(0), "free model should have 0 input cost");
    }

    #[test]
    fn free_variant_detection() {
        let catalog = load_fixture();
        let free_count = catalog.data.iter().filter(|m| m.is_free_variant()).count();
        assert!(
            free_count > 0,
            "should detect at least one :free variant model"
        );

        let non_free = catalog
            .data
            .iter()
            .find(|m| m.id == "openai/gpt-4o")
            .unwrap();
        assert!(!non_free.is_free_variant());
    }

    #[test]
    fn alias_detection() {
        let catalog = load_fixture();
        let aliases: Vec<_> = catalog.data.iter().filter(|m| m.is_alias()).collect();
        assert!(
            aliases.len() > 0,
            "should detect at least one ~ alias model"
        );
        for a in &aliases {
            assert!(a.id.starts_with('~'));
        }
    }

    #[test]
    fn supported_provider_filter() {
        let catalog = load_fixture();
        let supported = catalog.supported_models();
        let total = catalog.data.len();
        assert!(
            supported.len() > 50,
            "expected 50+ supported models, got {}",
            supported.len()
        );
        assert!(
            supported.len() < total,
            "supported ({}) should be fewer than total ({})",
            supported.len(),
            total
        );

        for m in &supported {
            assert!(
                is_supported_provider(m.provider_slug()),
                "{} should be a supported provider",
                m.provider_slug()
            );
        }
    }

    #[test]
    fn unsupported_providers_excluded() {
        let catalog = load_fixture();
        let supported = catalog.supported_models();
        let unsupported_slugs = ["aion-labs", "baidu", "inflection", "writer"];
        for slug in &unsupported_slugs {
            assert!(
                !supported.iter().any(|m| m.provider_slug() == *slug),
                "{} should not be in supported models",
                slug
            );
        }
    }

    #[test]
    fn all_entries_have_id_with_slash() {
        let catalog = load_fixture();
        for m in &catalog.data {
            assert!(
                m.id.contains('/'),
                "model ID '{}' should contain a slash",
                m.id
            );
        }
    }

    #[test]
    fn architecture_fields_present() {
        let catalog = load_fixture();
        let gpt4o = catalog
            .data
            .iter()
            .find(|m| m.id == "openai/gpt-4o")
            .unwrap();
        let arch = gpt4o.architecture.as_ref().expect("must have architecture");
        assert!(arch.input_modalities.contains(&"text".to_string()));
        assert!(arch.output_modalities.contains(&"text".to_string()));
    }

    #[test]
    fn context_length_present() {
        let catalog = load_fixture();
        let gpt4o = catalog
            .data
            .iter()
            .find(|m| m.id == "openai/gpt-4o")
            .unwrap();
        assert_eq!(gpt4o.context_length, Some(128_000));
    }

    #[test]
    fn top_provider_max_completion_tokens() {
        let catalog = load_fixture();
        let gpt4o = catalog
            .data
            .iter()
            .find(|m| m.id == "openai/gpt-4o")
            .unwrap();
        let tp = gpt4o
            .top_provider
            .as_ref()
            .expect("must have top_provider");
        assert!(tp.max_completion_tokens.unwrap_or(0) > 0);
    }

    #[test]
    fn key_models_exist() {
        let catalog = load_fixture();
        let expected = [
            "openai/gpt-4o",
            "anthropic/claude-sonnet-4.6",
            "google/gemini-2.5-flash",
            "deepseek/deepseek-chat",
            "x-ai/grok-4.3",
        ];
        for model_id in &expected {
            assert!(
                catalog.data.iter().any(|m| m.id == *model_id),
                "expected model '{}' to exist in catalog",
                model_id
            );
        }
    }

    #[test]
    fn key_models_have_pricing() {
        let catalog = load_fixture();
        let models_to_check = ["openai/gpt-4o", "deepseek/deepseek-chat"];
        for model_id in &models_to_check {
            let entry = catalog
                .data
                .iter()
                .find(|m| m.id == *model_id)
                .unwrap_or_else(|| panic!("{} must exist", model_id));
            assert!(
                entry.input_cost_per_1m().is_some(),
                "{} must have input pricing",
                model_id
            );
            assert!(
                entry.output_cost_per_1m().is_some(),
                "{} must have output pricing",
                model_id
            );
        }
    }

    #[test]
    fn embedding_models_exist_in_fixture() {
        let catalog = load_fixture();
        for model_id in &[
            "openai/text-embedding-3-small",
            "openai/text-embedding-3-large",
        ] {
            assert!(
                catalog.data.iter().any(|m| m.id == *model_id),
                "embedding model '{}' must exist in pricing.json fixture",
                model_id
            );
        }
    }

    #[test]
    fn embedding_model_pricing_prompt_only() {
        let catalog = load_fixture();
        let small = catalog
            .data
            .iter()
            .find(|m| m.id == "openai/text-embedding-3-small")
            .expect("text-embedding-3-small must exist");

        let input = small
            .input_cost_per_1m()
            .expect("must have input pricing");
        let output = small
            .output_cost_per_1m()
            .expect("must have output pricing");

        assert!(
            input > dec!(0),
            "embedding model prompt cost must be > 0, got {}",
            input
        );
        assert_eq!(
            output,
            dec!(0),
            "embedding model completion cost must be 0, got {}",
            output
        );
    }

    #[test]
    fn embedding_cost_calculation_uses_prompt_rate_only() {
        let catalog = load_fixture();
        let large = catalog
            .data
            .iter()
            .find(|m| m.id == "openai/text-embedding-3-large")
            .expect("text-embedding-3-large must exist");

        let prompt_per_1m = large.input_cost_per_1m().unwrap();
        let completion_per_1m = large.output_cost_per_1m().unwrap();

        // For embeddings: output_tokens = 0, so only prompt rate matters
        let input_tokens = 1_000_000u64;
        let expected_cost = prompt_per_1m;
        let actual_cost = prompt_per_1m * rust_decimal::Decimal::from(input_tokens)
            / rust_decimal::Decimal::from(1_000_000u64)
            + completion_per_1m * rust_decimal::Decimal::ZERO;

        assert_eq!(
            actual_cost, expected_cost,
            "with 1M input tokens and 0 output tokens, cost should equal prompt rate"
        );
    }
}
