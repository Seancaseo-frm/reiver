//! Cost Calculator for LLM Requests
//!
//! Calculates cost based on token usage and dynamic pricing from PostgreSQL.
//! Uses an in-memory cache with automatic refresh for performance.
//!
//! The cache uses `arc_swap` for lock-free reads, ensuring that cost calculations
//! are never blocked by cache refreshes.

use crate::error::AppError;
use crate::llm::types::{ModelCatalogRow, ModelPricing};
use arc_swap::ArcSwap;
use once_cell::sync::Lazy;
use regex::Regex;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

/// Regex patterns for model name normalization (compiled once)
static DATE_SUFFIX_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"-\d{4}-\d{2}-\d{2}$").expect("Invalid date suffix regex"));
static NUMERIC_SUFFIX_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"-\d{3,}$").expect("Invalid numeric suffix regex"));

/// Cache refresh interval (5 minutes)
const CACHE_REFRESH_INTERVAL: Duration = Duration::from_secs(300);

/// Cache entry containing the pricing map
type PricingCache = HashMap<(String, String), ModelPricing>;

/// Cost calculator with in-memory pricing cache
///
/// Uses `ArcSwap` for lock-free reads - cost calculations are never blocked
/// by cache refreshes. Only one refresh can run at a time (via atomic flag).
#[derive(Clone)]
pub struct CostCalculator {
    db: Arc<PgPool>,
    /// In-memory cache: (provider, model) -> ModelPricing
    /// Uses ArcSwap for lock-free reads during cost calculations
    cache: Arc<ArcSwap<PricingCache>>,
    /// Last time cache was refreshed (as Unix timestamp in seconds for atomic access)
    cache_updated_secs: Arc<AtomicU64>,
    /// Flag to prevent concurrent cache refreshes
    refreshing: Arc<AtomicBool>,
}

impl CostCalculator {
    /// Create a new cost calculator
    pub fn new(db: Arc<PgPool>) -> Self {
        Self {
            db,
            cache: Arc::new(ArcSwap::from_pointee(HashMap::new())),
            // Initialize to 0 so first request triggers a refresh
            cache_updated_secs: Arc::new(AtomicU64::new(0)),
            refreshing: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Initialize the cache by loading all pricing data
    pub async fn initialize(&self) -> Result<(), AppError> {
        self.refresh_cache().await?;
        Ok(())
    }

    /// Get current Unix timestamp in seconds
    fn current_timestamp_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs()
    }

    /// Refresh cache if older than CACHE_REFRESH_INTERVAL
    ///
    /// Uses atomic operations for both staleness check and refresh flag,
    /// eliminating the need for double-checking with locks.
    async fn maybe_refresh_cache(&self) -> Result<(), AppError> {
        let now_secs = Self::current_timestamp_secs();
        let last_updated = self.cache_updated_secs.load(Ordering::Relaxed);

        // Fast path: cache is fresh
        if now_secs.saturating_sub(last_updated) <= CACHE_REFRESH_INTERVAL.as_secs() {
            return Ok(());
        }

        // Try to acquire the refresh lock using atomic compare-exchange
        // This ensures only one task performs the refresh at a time
        if self
            .refreshing
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            // We acquired the lock - no need to double-check since we use
            // atomic timestamp update in refresh_cache()
            let result = self.refresh_cache().await;
            self.refreshing.store(false, Ordering::Release);
            return result;
        }

        // Another task is currently refreshing, skip this refresh
        debug!("Cache refresh already in progress, skipping");
        Ok(())
    }

    /// Force refresh the pricing cache from database
    ///
    /// Uses ArcSwap for lock-free cache replacement - readers are never blocked.
    async fn refresh_cache(&self) -> Result<(), AppError> {
        debug!("Refreshing pricing cache from database");

        let catalog_rows: Vec<ModelCatalogRow> = sqlx::query_as(
            "SELECT id, name, created, context_length, \
                    pricing, architecture, top_provider, supported_parameters, \
                    provider_slug, model_slug, enabled \
             FROM model_catalog \
             WHERE enabled = TRUE \
             ORDER BY provider_slug, model_slug",
        )
        .fetch_all(self.db.as_ref())
        .await?;

        let mut new_cache = HashMap::new();
        for row in catalog_rows {
            let key = (row.provider_slug.clone(), row.model_slug.clone());
            let pricing: ModelPricing = row.into();
            new_cache.insert(key, pricing);
        }

        let count = new_cache.len();

        // Atomically swap the cache - readers see either old or new, never blocked
        self.cache.store(Arc::new(new_cache));

        // Update timestamp after successful swap
        self.cache_updated_secs
            .store(Self::current_timestamp_secs(), Ordering::Release);

        info!("Pricing cache refreshed with {} models", count);
        Ok(())
    }

    /// Calculate cost for an LLM request
    ///
    /// # Arguments
    /// * `provider` - The AI provider (e.g., "openai", "anthropic")
    /// * `model` - The model name (e.g., "gpt-4o", "claude-3-opus")
    /// * `input_tokens` - Number of input/prompt tokens
    /// * `output_tokens` - Number of output/completion tokens
    /// * `cache_read_tokens` - Number of cache read tokens (optional)
    /// * `cache_write_tokens` - Number of cache write tokens (optional)
    ///
    /// This method is lock-free for reads - it will never block waiting for a cache refresh.
    pub async fn calculate_cost(
        &self,
        provider: &str,
        model: &str,
        input_tokens: u32,
        output_tokens: u32,
        cache_read_tokens: u32,
        cache_write_tokens: u32,
    ) -> Result<Decimal, AppError> {
        self.maybe_refresh_cache().await?;

        // Load cache reference - this is lock-free via ArcSwap
        let cache = self.cache.load();

        // Normalize inputs for case-insensitive matching
        let provider_lower = provider.to_lowercase();
        let model_lower = model.to_lowercase();

        // Strip provider prefix if present (e.g. "deepseek/deepseek-chat" → "deepseek-chat")
        // Some providers namespace their model IDs with a "provider/" prefix in the API
        // but pricing rows store just the model name.
        let model_lower = match model_lower.split_once('/') {
            Some((_, suffix)) if !suffix.is_empty() => suffix.to_string(),
            _ => model_lower,
        };

        // Try exact match first (case-insensitive)
        if let Some(pricing) = cache.get(&(provider_lower.clone(), model_lower.clone())) {
            return Ok(self.compute_cost(
                pricing,
                input_tokens,
                output_tokens,
                cache_read_tokens,
                cache_write_tokens,
            ));
        }

        // Try model family match (e.g., "gpt-4o-2024-05-13" -> "gpt-4o")
        if let Some(pricing) = self.find_model_family_match(&cache, &provider_lower, &model_lower) {
            return Ok(self.compute_cost(
                &pricing,
                input_tokens,
                output_tokens,
                cache_read_tokens,
                cache_write_tokens,
            ));
        }

        // Try any provider match (model might be reported without provider)
        if let Some(pricing) = self.find_any_provider_match(&cache, &model_lower) {
            return Ok(self.compute_cost(
                &pricing,
                input_tokens,
                output_tokens,
                cache_read_tokens,
                cache_write_tokens,
            ));
        }

        warn!(
            "No pricing found for provider={}, model={} (normalized={})",
            provider, model, model_lower
        );
        Ok(Decimal::ZERO)
    }

    /// Compute cost from pricing and token counts
    fn compute_cost(
        &self,
        pricing: &ModelPricing,
        input_tokens: u32,
        output_tokens: u32,
        cache_read_tokens: u32,
        cache_write_tokens: u32,
    ) -> Decimal {
        let input_cost = pricing.input_cost_per_token * Decimal::from(input_tokens);
        let output_cost = pricing.output_cost_per_token * Decimal::from(output_tokens);
        let cache_read_cost = pricing.cache_read_cost_per_token * Decimal::from(cache_read_tokens);
        let cache_write_cost =
            pricing.cache_write_cost_per_token * Decimal::from(cache_write_tokens);

        input_cost + output_cost + cache_read_cost + cache_write_cost
    }

    /// Find pricing by model family (strips version suffixes)
    ///
    /// This handles common versioning patterns while avoiding false positives:
    /// - `gpt-4o-2024-05-13` -> `gpt-4o` (date suffix)
    /// - `gemini-1.5-pro-001` -> `gemini-1.5-pro` (numeric version)
    /// - `gpt-4-32k` should NOT match `gpt-4` (different model with different pricing)
    fn find_model_family_match(
        &self,
        cache: &HashMap<(String, String), ModelPricing>,
        provider: &str,
        model: &str,
    ) -> Option<ModelPricing> {
        let provider_lower = provider.to_lowercase();
        let model_lower = model.to_lowercase();

        // Common patterns for model versioning
        // gpt-4o-2024-05-13 -> gpt-4o
        // claude-3-opus-20240229 -> claude-3-opus
        // gemini-1.5-pro-001 -> gemini-1.5-pro

        let base_model = self.extract_base_model(&model_lower);
        if base_model != model_lower {
            if let Some(pricing) = cache.get(&(provider_lower.clone(), base_model.to_string())) {
                return Some(pricing.clone());
            }
        }

        // Try prefix matching (longest prefix first)
        // Only match if the suffix is a known version pattern
        let mut best_match: Option<&ModelPricing> = None;
        let mut best_len = 0;

        for ((p, m), pricing) in cache.iter() {
            if p == &provider_lower && model_lower.starts_with(m) && m.len() > best_len {
                // Check if the suffix is a version delimiter, not a different model
                let suffix = &model_lower[m.len()..];

                // Valid version suffixes:
                // - Empty (exact match)
                // - Date pattern: -YYYY-MM-DD (e.g., -2024-05-13)
                // - 3+ digit version: -NNN (e.g., -001, -002)
                // - Colon versioning (some providers)
                //
                // Invalid (these indicate different models):
                // - Single letter: "o" (gpt-4 vs gpt-4o)
                // - Short alphanumeric: "-32k", "-turbo", "-mini"
                let is_version_suffix = if suffix.is_empty() {
                    true
                } else if suffix.starts_with(':') {
                    true
                } else if suffix.starts_with('-') {
                    // Check if it's a date pattern (-YYYY-MM-DD) or 3+ digit version (-NNN)
                    let after_dash = &suffix[1..];
                    DATE_SUFFIX_PATTERN.is_match(&format!("x{}", suffix)) || 
                    NUMERIC_SUFFIX_PATTERN.is_match(&format!("x{}", suffix)) ||
                    // Also allow -preview, -latest (text-only suffixes that start with letter)
                    (after_dash.chars().next().map_or(false, |c| c.is_ascii_digit()) &&
                     after_dash.len() >= 3 &&
                     after_dash.chars().take(4).all(|c| c.is_ascii_digit()))
                } else {
                    false
                };

                if is_version_suffix {
                    best_match = Some(pricing);
                    best_len = m.len();
                }
            }
        }

        best_match.cloned()
    }

    /// Find pricing by model name across all providers
    fn find_any_provider_match(
        &self,
        cache: &HashMap<(String, String), ModelPricing>,
        model: &str,
    ) -> Option<ModelPricing> {
        let model_lower = model.to_lowercase();

        // Direct match on any provider (case-insensitive)
        for ((_, m), pricing) in cache.iter() {
            if m == &model_lower {
                return Some(pricing.clone());
            }
        }

        // Base model match on any provider
        let base_model = self.extract_base_model(&model_lower);
        if base_model != model_lower {
            for ((_, m), pricing) in cache.iter() {
                if m == &base_model {
                    return Some(pricing.clone());
                }
            }
        }

        None
    }

    /// Extract base model name by stripping version suffixes
    fn extract_base_model(&self, model: &str) -> String {
        // Strip date-based suffixes: gpt-4o-2024-05-13 -> gpt-4o
        let stripped = DATE_SUFFIX_PATTERN.replace(model, "").to_string();

        // Strip numeric suffixes: gemini-1.5-pro-001 -> gemini-1.5-pro
        NUMERIC_SUFFIX_PATTERN.replace(&stripped, "").to_string()
    }

    /// Get all cached pricing (for debugging/admin)
    pub async fn get_all_pricing(&self) -> HashMap<(String, String), ModelPricing> {
        let _ = self.maybe_refresh_cache().await;
        let cache = self.cache.load();
        (**cache).clone()
    }

    /// Get pricing for a specific model (for API)
    #[allow(dead_code)] // Available for future single-model lookup API
    pub async fn get_pricing(&self, provider: &str, model: &str) -> Option<ModelPricing> {
        self.maybe_refresh_cache().await.ok()?;
        self.cache
            .load()
            .get(&(provider.to_lowercase(), model.to_string()))
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    // Helper to create a test calculator without database (requires async context for PgPool)
    async fn create_test_calculator() -> CostCalculator {
        // Use a lazy connection that won't actually connect
        let db = Arc::new(PgPool::connect_lazy("postgres://localhost/test").unwrap());
        CostCalculator::new(db)
    }

    // Helper to create a ModelPricing struct
    fn create_pricing(
        provider: &str,
        model: &str,
        input_per_million: f64,
        output_per_million: f64,
    ) -> ((String, String), ModelPricing) {
        let key = (provider.to_lowercase(), model.to_string());
        let pricing = ModelPricing {
            provider: provider.to_string(),
            model: model.to_string(),
            input_cost_per_token: Decimal::from_f64_retain(input_per_million / 1_000_000.0)
                .unwrap_or(Decimal::ZERO),
            output_cost_per_token: Decimal::from_f64_retain(output_per_million / 1_000_000.0)
                .unwrap_or(Decimal::ZERO),
            cache_read_cost_per_token: Decimal::ZERO,
            cache_write_cost_per_token: Decimal::ZERO,
        };
        (key, pricing)
    }

    #[tokio::test]
    async fn test_extract_base_model_date_suffix() {
        let calc = create_test_calculator().await;

        // OpenAI date format
        assert_eq!(calc.extract_base_model("gpt-4o-2024-05-13"), "gpt-4o");
        assert_eq!(
            calc.extract_base_model("gpt-4-turbo-2024-04-09"),
            "gpt-4-turbo"
        );

        // Anthropic date format (no dashes in date)
        // Note: This won't match our regex, so it stays as-is
        assert_eq!(calc.extract_base_model("claude-3-opus"), "claude-3-opus");
    }

    #[tokio::test]
    async fn test_extract_base_model_numeric_suffix() {
        let calc = create_test_calculator().await;

        // Google numeric suffix
        assert_eq!(
            calc.extract_base_model("gemini-1.5-pro-001"),
            "gemini-1.5-pro"
        );
        assert_eq!(
            calc.extract_base_model("gemini-1.5-flash-002"),
            "gemini-1.5-flash"
        );

        // Short numbers should not be stripped
        assert_eq!(calc.extract_base_model("gpt-4"), "gpt-4");
        assert_eq!(calc.extract_base_model("claude-3"), "claude-3");
    }

    #[tokio::test]
    async fn test_extract_base_model_no_suffix() {
        let calc = create_test_calculator().await;

        // Models without suffixes should be unchanged
        assert_eq!(calc.extract_base_model("gpt-4o"), "gpt-4o");
        assert_eq!(
            calc.extract_base_model("claude-3-sonnet"),
            "claude-3-sonnet"
        );
        assert_eq!(calc.extract_base_model("o1-mini"), "o1-mini");
    }

    #[tokio::test]
    async fn test_extract_base_model_combined_suffixes() {
        let calc = create_test_calculator().await;

        // When model has both date-like and numeric suffix, we strip the numeric suffix first
        // (since it's at the end) and then the date suffix
        // Input: model-2024-01-15-001
        // After numeric strip: model-2024-01-15
        // The date pattern now matches at the end
        // This is an edge case that rarely occurs in practice
        assert_eq!(
            calc.extract_base_model("model-2024-01-15-001"),
            "model-2024-01-15"
        );

        // In practice, models have either a date OR a numeric suffix, not both
        assert_eq!(calc.extract_base_model("gpt-4o-2024-05-13"), "gpt-4o");
        assert_eq!(
            calc.extract_base_model("gemini-1.5-pro-001"),
            "gemini-1.5-pro"
        );
    }

    #[tokio::test]
    async fn test_compute_cost_basic() {
        let calc = create_test_calculator().await;

        let pricing = ModelPricing {
            provider: "openai".to_string(),
            model: "gpt-4o".to_string(),
            input_cost_per_token: dec!(0.000005),  // $5/1M
            output_cost_per_token: dec!(0.000015), // $15/1M
            cache_read_cost_per_token: Decimal::ZERO,
            cache_write_cost_per_token: Decimal::ZERO,
        };

        // 1000 input tokens + 500 output tokens
        let cost = calc.compute_cost(&pricing, 1000, 500, 0, 0);

        // Expected: (1000 * 0.000005) + (500 * 0.000015) = 0.005 + 0.0075 = 0.0125
        assert_eq!(cost, dec!(0.0125));
    }

    #[tokio::test]
    async fn test_compute_cost_with_cache_tokens() {
        let calc = create_test_calculator().await;

        let pricing = ModelPricing {
            provider: "anthropic".to_string(),
            model: "claude-3-5-sonnet".to_string(),
            input_cost_per_token: dec!(0.000003),       // $3/1M
            output_cost_per_token: dec!(0.000015),      // $15/1M
            cache_read_cost_per_token: dec!(0.0000003), // $0.30/1M (90% discount)
            cache_write_cost_per_token: dec!(0.00000375), // $3.75/1M (25% markup)
        };

        // 1000 input, 500 output, 2000 cache read, 500 cache write
        let cost = calc.compute_cost(&pricing, 1000, 500, 2000, 500);

        // Expected:
        // input:  1000 * 0.000003 = 0.003
        // output: 500 * 0.000015 = 0.0075
        // cache_read: 2000 * 0.0000003 = 0.0006
        // cache_write: 500 * 0.00000375 = 0.001875
        // Total: 0.003 + 0.0075 + 0.0006 + 0.001875 = 0.012975
        let expected = dec!(0.003) + dec!(0.0075) + dec!(0.0006) + dec!(0.001875);
        assert_eq!(cost, expected);
    }

    #[tokio::test]
    async fn test_compute_cost_zero_tokens() {
        let calc = create_test_calculator().await;

        let pricing = ModelPricing {
            provider: "openai".to_string(),
            model: "gpt-4o".to_string(),
            input_cost_per_token: dec!(0.000005),
            output_cost_per_token: dec!(0.000015),
            cache_read_cost_per_token: Decimal::ZERO,
            cache_write_cost_per_token: Decimal::ZERO,
        };

        let cost = calc.compute_cost(&pricing, 0, 0, 0, 0);
        assert_eq!(cost, Decimal::ZERO);
    }

    #[tokio::test]
    async fn test_compute_cost_large_token_counts() {
        let calc = create_test_calculator().await;

        let pricing = ModelPricing {
            provider: "openai".to_string(),
            model: "gpt-4o".to_string(),
            input_cost_per_token: dec!(0.000005),
            output_cost_per_token: dec!(0.000015),
            cache_read_cost_per_token: Decimal::ZERO,
            cache_write_cost_per_token: Decimal::ZERO,
        };

        // 1 million input tokens + 1 million output tokens
        let cost = calc.compute_cost(&pricing, 1_000_000, 1_000_000, 0, 0);

        // Expected: (1M * 5/1M) + (1M * 15/1M) = 5 + 15 = 20
        assert_eq!(cost, dec!(20));
    }

    #[tokio::test]
    async fn test_find_model_family_match() {
        let calc = create_test_calculator().await;

        // Populate cache manually using ArcSwap
        {
            let mut new_cache = HashMap::new();
            let (key, pricing) = create_pricing("openai", "gpt-4o", 5.0, 15.0);
            new_cache.insert(key, pricing);
            calc.cache.store(Arc::new(new_cache));
        }

        let cache = calc.cache.load();

        // Should find match for versioned model
        let result = calc.find_model_family_match(&cache, "openai", "gpt-4o-2024-05-13");
        assert!(result.is_some());
        assert_eq!(result.unwrap().model, "gpt-4o");
    }

    #[tokio::test]
    async fn test_find_model_family_match_no_false_positive() {
        let calc = create_test_calculator().await;

        // Populate cache with gpt-4 model
        {
            let mut new_cache = HashMap::new();
            let (key, pricing) = create_pricing("openai", "gpt-4", 30.0, 60.0);
            new_cache.insert(key, pricing);
            calc.cache.store(Arc::new(new_cache));
        }

        let cache = calc.cache.load();

        // gpt-4-32k should NOT match gpt-4 (different model with different pricing)
        let result = calc.find_model_family_match(&cache, "openai", "gpt-4-32k");
        assert!(result.is_none(), "gpt-4-32k should not match gpt-4");

        // gpt-4o should NOT match gpt-4 (different model)
        let result = calc.find_model_family_match(&cache, "openai", "gpt-4o");
        assert!(result.is_none(), "gpt-4o should not match gpt-4");

        // gpt-4-turbo should NOT match gpt-4 (different model variant)
        let result = calc.find_model_family_match(&cache, "openai", "gpt-4-turbo");
        assert!(result.is_none(), "gpt-4-turbo should not match gpt-4");
    }

    #[tokio::test]
    async fn test_find_model_family_match_date_version() {
        let calc = create_test_calculator().await;

        // Populate cache with gpt-4-turbo model
        {
            let mut new_cache = HashMap::new();
            let (key, pricing) = create_pricing("openai", "gpt-4-turbo", 10.0, 30.0);
            new_cache.insert(key, pricing);
            calc.cache.store(Arc::new(new_cache));
        }

        let cache = calc.cache.load();

        // gpt-4-turbo-2024-04-09 SHOULD match gpt-4-turbo (date version suffix)
        let result = calc.find_model_family_match(&cache, "openai", "gpt-4-turbo-2024-04-09");
        assert!(
            result.is_some(),
            "gpt-4-turbo-2024-04-09 should match gpt-4-turbo"
        );
        assert_eq!(result.unwrap().model, "gpt-4-turbo");
    }

    #[tokio::test]
    async fn test_find_any_provider_match() {
        let calc = create_test_calculator().await;

        // Populate cache using ArcSwap
        {
            let mut new_cache = HashMap::new();
            let (key, pricing) = create_pricing("openai", "gpt-4o", 5.0, 15.0);
            new_cache.insert(key, pricing);
            calc.cache.store(Arc::new(new_cache));
        }

        let cache = calc.cache.load();

        // Should find match even without provider
        let result = calc.find_any_provider_match(&cache, "gpt-4o");
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_calculate_cost_strips_provider_prefix() {
        let calc = create_test_calculator().await;

        {
            let mut cache = HashMap::new();
            let (key, pricing) = create_pricing("deepseek", "deepseek-chat", 0.14, 0.28);
            cache.insert(key, pricing);
            calc.cache.store(Arc::new(cache));
            calc.cache_updated_secs
                .store(CostCalculator::current_timestamp_secs(), Ordering::Release);
        }

        // Model string with provider prefix (as the gateway stores it)
        let cost = calc
            .calculate_cost("deepseek", "deepseek/deepseek-chat", 1_000_000, 0, 0, 0)
            .await
            .unwrap();
        assert!(
            cost > Decimal::ZERO,
            "cost with provider prefix should be > 0, got {cost}"
        );

        // Without prefix should also work and produce the same result
        let cost_no_prefix = calc
            .calculate_cost("deepseek", "deepseek-chat", 1_000_000, 0, 0, 0)
            .await
            .unwrap();
        assert_eq!(
            cost, cost_no_prefix,
            "prefixed and unprefixed should yield same cost"
        );
    }

    #[test]
    fn test_regex_patterns_compiled() {
        // Ensure regex patterns compile without panic
        let _ = &*DATE_SUFFIX_PATTERN;
        let _ = &*NUMERIC_SUFFIX_PATTERN;
    }

    #[test]
    fn test_date_suffix_regex() {
        assert!(DATE_SUFFIX_PATTERN.is_match("gpt-4o-2024-05-13"));
        assert!(DATE_SUFFIX_PATTERN.is_match("model-2023-12-01"));
        assert!(!DATE_SUFFIX_PATTERN.is_match("gpt-4o"));
        assert!(!DATE_SUFFIX_PATTERN.is_match("claude-20240229")); // No dashes
    }

    #[test]
    fn test_numeric_suffix_regex() {
        assert!(NUMERIC_SUFFIX_PATTERN.is_match("gemini-1.5-pro-001"));
        assert!(NUMERIC_SUFFIX_PATTERN.is_match("model-1234"));
        assert!(!NUMERIC_SUFFIX_PATTERN.is_match("gpt-4")); // Too short
        assert!(!NUMERIC_SUFFIX_PATTERN.is_match("gpt-4o")); // No number
    }
}
