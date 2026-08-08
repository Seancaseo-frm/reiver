//! Model resolution, provider selection, and routing logic.

use std::sync::Arc;
use uuid::Uuid;

use crate::app_state::FlowState;
use crate::gateway::error::GatewayError;
use crate::gateway::provider_manager::ProviderKeyStore;
use crate::gateway::provider_types::Provider;
use crate::gateway::providers::LlmProvider;
use crate::gateway::router::GatewayRouter;

/// A fully-resolved provider candidate ready for execution.
///
/// The execution loop iterates `Vec<ProviderCandidate>` in order —
/// position 0 is the primary, the rest are fallbacks.
pub(crate) struct ProviderCandidate {
    pub provider: Provider,
    pub model: String,
    pub key: String,
    pub is_platform_key: bool,
    pub provider_impl: Arc<dyn LlmProvider>,
}

/// Build the complete, ordered provider chain for a request.
///
/// The returned list has the primary model at position 0 and fallback
/// candidates (if allowed) after it.  Every entry carries a resolved
/// provider implementation and API key so the execution loop can call
/// providers without further resolution.
///
/// Fallback model sources (in priority order):
/// 1. Per-request `models` array
/// 2. Project-level `default_fallback_models`
/// 3. Derived from the project's configured provider integrations
pub(super) async fn resolve_provider_chain(
    state: &FlowState,
    router: &GatewayRouter,
    project_id: Uuid,
    primary_model: &str,
    request_models: Option<Vec<String>>,
    default_fallback_models: &[String],
    provider_prefs: Option<&crate::gateway::types::ProviderPreferences>,
    fallback_allowed: bool,
    theta_dedicated_base_url: Option<&str>,
) -> Result<Vec<ProviderCandidate>, GatewayError> {
    // Build the full model list: primary first, then fallbacks.
    let mut models: Vec<String> = vec![primary_model.to_string()];

    if fallback_allowed {
        let fallback_tail = resolve_fallback_tail(
            state, project_id, request_models, default_fallback_models,
        ).await;

        // Exclude primary, order by latency.
        let tail: Vec<String> = fallback_tail
            .into_iter()
            .filter(|m| m != primary_model)
            .collect();
        let tail = order_models_by_latency(router, &tail).await;
        models.extend(tail);
    }

    // Deduplicate while preserving order (primary stays at index 0).
    let mut seen = std::collections::HashSet::new();
    models.retain(|m| seen.insert(m.clone()));

    // Apply provider preferences (multi-endpoint models like Claude via
    // Anthropic vs Bedrock).  Preferences can change which provider_impl +
    // model id a candidate resolves to.
    let models = apply_preferences(router, &models, provider_prefs);

    // Collect unique provider slugs, batch-fetch keys.
    let provider_slugs: Vec<&str> = models
        .iter()
        .filter_map(|m| router.get_provider_name(m))
        .collect();
    let keys = get_provider_keys_batch(state, project_id, &provider_slugs).await?;

    // Resolve each model into a ProviderCandidate.
    let mut chain: Vec<ProviderCandidate> = Vec::with_capacity(models.len());
    for model in &models {
        let Some(provider) = Provider::from_model_prefix(model) else {
            tracing::warn!(model = %model, reason = "unrecognised model", "Skipping candidate in provider chain");
            continue;
        };
        let provider_slug = provider.as_str();

        // Resolve the provider implementation.
        let provider_impl = if provider == Provider::ThetaDedicated {
            if let Some(base_url) = theta_dedicated_base_url {
                Arc::new(
                    crate::gateway::providers::ThetaDedicatedProvider::with_base_url(
                        base_url.to_string(),
                    ),
                ) as Arc<dyn LlmProvider>
            } else {
                tracing::warn!(model = %model, provider = %provider_slug, reason = "no base URL configured", "Skipping candidate in provider chain");
                continue;
            }
        } else {
            match router.route(model) {
                Some(llm) => llm,
                None => {
                    tracing::warn!(model = %model, provider = %provider_slug, reason = "no provider registered", "Skipping candidate in provider chain");
                    continue;
                }
            }
        };

        // Resolve the API key.
        let (key, is_platform_key) = if let Some(resolved) = keys.get(provider_slug) {
            (resolved.key.clone(), resolved.is_platform)
        } else if let Some(cached) = state.provider_key_cache.get(&(project_id, provider_slug.to_string())) {
            (cached.key.clone(), cached.is_platform)
        } else {
            tracing::warn!(model = %model, provider = %provider_slug, reason = "no API key", "Skipping candidate in provider chain");
            continue;
        };

        chain.push(ProviderCandidate {
            provider,
            model: model.clone(),
            key,
            is_platform_key,
            provider_impl,
        });
    }

    if chain.is_empty() {
        return Err(GatewayError::MissingProviderKey(
            "No provider candidates with valid keys".to_string(),
        ));
    }

    Ok(chain)
}

/// Build the fallback model tail (excludes primary — caller filters).
async fn resolve_fallback_tail(
    state: &FlowState,
    project_id: Uuid,
    request_models: Option<Vec<String>>,
    default_fallback_models: &[String],
) -> Vec<String> {
    // Tier 1: per-request models array
    if let Some(models) = request_models {
        if !models.is_empty() {
            return models;
        }
    }

    // Tier 2: project-level defaults
    if !default_fallback_models.is_empty() {
        return default_fallback_models.to_vec();
    }

    // Tier 3: derive from the project's configured integrations
    let available_providers = state
        .get_available_providers(project_id)
        .await
        .unwrap_or_default();
    let derived: Vec<String> = available_providers
        .iter()
        .filter_map(|p| {
            state
                .model_catalog_cache
                .auto_model_for_provider(p.as_str())
        })
        .collect();
    if !derived.is_empty() {
        tracing::debug!(
            %project_id,
            derived_fallbacks = ?derived,
            "No explicit fallback models; derived from project integrations"
        );
    }
    derived
}

/// Apply provider preferences to a model list, resolving multi-endpoint
/// models to the preferred provider's model ID.
fn apply_preferences(
    router: &GatewayRouter,
    models: &[String],
    prefs: Option<&crate::gateway::types::ProviderPreferences>,
) -> Vec<String> {
    let Some(prefs) = prefs else {
        return models.to_vec();
    };

    models
        .iter()
        .map(|model| {
            let all_endpoints = router.route_all(model);
            if all_endpoints.len() <= 1 {
                return model.clone();
            }
            let filtered = apply_provider_filter(&all_endpoints, prefs);
            filtered
                .first()
                .map(|(_, model_id, _)| model_id.clone())
                .unwrap_or_else(|| model.clone())
        })
        .collect()
}

/// Apply provider preference filters (only, ignore, order) to a set of
/// provider endpoints. Returns a filtered and re-ordered list.
pub(super) fn apply_provider_filter(
    endpoints: &[(Arc<dyn LlmProvider>, String, Provider)],
    prefs: &crate::gateway::types::ProviderPreferences,
) -> Vec<(Arc<dyn LlmProvider>, String, Provider)> {
    let mut filtered: Vec<_> = endpoints
        .iter()
        .filter(|(_, _, provider)| {
            let slug = provider.as_str();
            if let Some(ref only) = prefs.only {
                if !only.is_empty() && !only.iter().any(|o| o == slug) {
                    return false;
                }
            }
            if let Some(ref ignore) = prefs.ignore {
                if ignore.iter().any(|i| i == slug) {
                    return false;
                }
            }
            true
        })
        .cloned()
        .collect();

    if let Some(ref order) = prefs.order {
        filtered.sort_by_key(|(_, _, provider)| {
            let slug = provider.as_str();
            order.iter().position(|o| o == slug).unwrap_or(usize::MAX)
        });
    }

    filtered
}

/// Order fallback models by provider latency (lowest P95 first).
///
/// Uses the gateway router's latency tracker to sort providers, then reorders
/// the fallback models to match. Models whose provider is not in the sorted
/// list are appended at the end.
pub(super) async fn order_models_by_latency(
    router: &GatewayRouter,
    fallback_models: &[String],
) -> Vec<String> {
    let fallback_providers_for_sort: Vec<String> = fallback_models
        .iter()
        .filter_map(|model| router.get_provider_name(model).map(|p| p.to_string()))
        .collect();
    let sorted_providers = router.get_latency_sorted_providers(&fallback_providers_for_sort);

    if sorted_providers.is_empty() {
        return fallback_models.to_vec();
    }

    let mut ordered = Vec::with_capacity(fallback_models.len());
    let mut seen = std::collections::HashSet::with_capacity(fallback_models.len());
    for provider in &sorted_providers {
        for model in fallback_models {
            if let Some(mp) = router.get_provider_name(model) {
                if mp == provider && seen.insert(model) {
                    ordered.push(model.clone());
                }
            }
        }
    }
    for model in fallback_models {
        if seen.insert(model) {
            ordered.push(model.clone());
        }
    }
    ordered
}

/// A resolved provider key with its platform flag.
pub(super) struct ResolvedBatchKey {
    pub key: String,
    pub is_platform: bool,
}

/// Batch fetch provider API keys for multiple providers.
///
/// Checks the in-memory cache first, then fetches missing keys in a single
/// database query. Populates the cache for future requests.
/// Returns each key paired with whether it is a platform-managed key.
pub(super) async fn get_provider_keys_batch(
    state: &FlowState,
    project_id: Uuid,
    providers: &[&str],
) -> Result<std::collections::HashMap<String, ResolvedBatchKey>, GatewayError> {
    use crate::app_state::{CachedProviderKey, PROVIDER_KEY_CACHE_TTL_SECS};
    use std::collections::HashMap;

    if providers.is_empty() {
        return Ok(HashMap::new());
    }

    let now = std::time::Instant::now();
    let mut result = HashMap::new();
    let mut missing_providers: Vec<&str> = Vec::new();

    for &provider in providers {
        let cache_key = (project_id, provider.to_string());
        if let Some(cached) = state.provider_key_cache.get(&cache_key) {
            if cached.expires_at > now {
                result.insert(provider.to_string(), ResolvedBatchKey {
                    key: cached.key,
                    is_platform: cached.is_platform,
                });
                continue;
            }
        }
        missing_providers.push(provider);
    }

    if missing_providers.is_empty() {
        return Ok(result);
    }

    let setting_keys: Vec<String> = missing_providers
        .iter()
        .map(|p| format!("gateway_{}_api_key", p))
        .collect();

    let rows: Vec<(String, String)> = sqlx::query_as(
        r#"
        SELECT ps.key, ps.value
        FROM project_settings ps
        WHERE ps.project_id = $1 AND ps.key = ANY($2)
        "#,
    )
    .bind(project_id)
    .bind(&setting_keys)
    .fetch_all(state.db.as_ref())
    .await
    .map_err(|e| {
        GatewayError::InternalError(format!(
            "Failed to batch fetch provider keys from database: {}",
            e
        ))
    })?;

    let expires_at =
        std::time::Instant::now() + std::time::Duration::from_secs(PROVIDER_KEY_CACHE_TTL_SECS);
    for (setting_key, encrypted_value) in rows {
        if let Some(provider) = setting_key
            .strip_prefix("gateway_")
            .and_then(|s| s.strip_suffix("_api_key"))
        {
            match state.encryptor.decrypt(&encrypted_value) {
                Ok(decrypted) => {
                    state.provider_key_cache.insert(
                        (project_id, provider.to_string()),
                        CachedProviderKey {
                            key: decrypted.clone(),
                            is_platform: false,
                            expires_at,
                        },
                    );
                    result.insert(provider.to_string(), ResolvedBatchKey {
                        key: decrypted,
                        is_platform: false,
                    });
                }
                Err(e) => {
                    tracing::error!(
                        provider = %provider,
                        error = %e,
                        "Failed to decrypt provider key in batch"
                    );
                }
            }
        }
    }

    for provider in &missing_providers {
        if !result.contains_key(*provider) {
            if let Some(default_key) = provider
                .parse::<Provider>()
                .ok()
                .and_then(|p| state.provider_manager.default_keys().get(&p))
            {
                result.insert(provider.to_string(), ResolvedBatchKey {
                    key: default_key.clone(),
                    is_platform: true,
                });
            }
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::provider_types::Provider;
    use crate::gateway::providers::{AnthropicProvider, BedrockProvider, OpenAiProvider};

    fn mock_endpoints() -> Vec<(Arc<dyn LlmProvider>, String, Provider)> {
        vec![
            (
                Arc::new(OpenAiProvider::new()) as Arc<dyn LlmProvider>,
                "gpt-4o".to_string(),
                Provider::OpenAi,
            ),
            (
                Arc::new(AnthropicProvider::new()) as Arc<dyn LlmProvider>,
                "claude-sonnet-4-6".to_string(),
                Provider::Anthropic,
            ),
            (
                Arc::new(BedrockProvider::new()) as Arc<dyn LlmProvider>,
                "anthropic.claude-sonnet-4-6-v1:0".to_string(),
                Provider::Bedrock,
            ),
        ]
    }

    #[test]
    fn test_apply_provider_filter_no_prefs_returns_all() {
        let endpoints = mock_endpoints();
        let prefs = crate::gateway::types::ProviderPreferences::default();
        let result = apply_provider_filter(&endpoints, &prefs);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_apply_provider_filter_only_restricts() {
        let endpoints = mock_endpoints();
        let prefs = crate::gateway::types::ProviderPreferences {
            only: Some(vec!["anthropic".into()]),
            ..Default::default()
        };
        let result = apply_provider_filter(&endpoints, &prefs);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].2, Provider::Anthropic);
    }

    #[test]
    fn test_apply_provider_filter_only_empty_is_no_restriction() {
        let endpoints = mock_endpoints();
        let prefs = crate::gateway::types::ProviderPreferences {
            only: Some(vec![]),
            ..Default::default()
        };
        let result = apply_provider_filter(&endpoints, &prefs);
        assert_eq!(
            result.len(),
            3,
            "Empty only list should be treated as no restriction"
        );
    }

    #[test]
    fn test_apply_provider_filter_ignore_excludes() {
        let endpoints = mock_endpoints();
        let prefs = crate::gateway::types::ProviderPreferences {
            ignore: Some(vec!["openai".into()]),
            ..Default::default()
        };
        let result = apply_provider_filter(&endpoints, &prefs);
        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|(_, _, p)| *p != Provider::OpenAi));
    }

    #[test]
    fn test_apply_provider_filter_ignore_empty_is_no_op() {
        let endpoints = mock_endpoints();
        let prefs = crate::gateway::types::ProviderPreferences {
            ignore: Some(vec![]),
            ..Default::default()
        };
        let result = apply_provider_filter(&endpoints, &prefs);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_apply_provider_filter_order_reorders() {
        let endpoints = mock_endpoints();
        let prefs = crate::gateway::types::ProviderPreferences {
            order: Some(vec!["bedrock".into(), "anthropic".into(), "openai".into()]),
            ..Default::default()
        };
        let result = apply_provider_filter(&endpoints, &prefs);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].2, Provider::Bedrock);
        assert_eq!(result[1].2, Provider::Anthropic);
        assert_eq!(result[2].2, Provider::OpenAi);
    }

    #[test]
    fn test_apply_provider_filter_order_partial() {
        let endpoints = mock_endpoints();
        let prefs = crate::gateway::types::ProviderPreferences {
            order: Some(vec!["bedrock".into()]),
            ..Default::default()
        };
        let result = apply_provider_filter(&endpoints, &prefs);
        assert_eq!(result[0].2, Provider::Bedrock, "Bedrock should be first");
        assert_eq!(result.len(), 3, "All endpoints should still be present");
    }

    #[test]
    fn test_apply_provider_filter_only_and_ignore_combined() {
        let endpoints = mock_endpoints();
        let prefs = crate::gateway::types::ProviderPreferences {
            only: Some(vec!["anthropic".into(), "bedrock".into()]),
            ignore: Some(vec!["bedrock".into()]),
            ..Default::default()
        };
        let result = apply_provider_filter(&endpoints, &prefs);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].2, Provider::Anthropic);
    }

    #[test]
    fn test_apply_provider_filter_only_unknown_slug_excludes_all() {
        let endpoints = mock_endpoints();
        let prefs = crate::gateway::types::ProviderPreferences {
            only: Some(vec!["nonexistent-provider".into()]),
            ..Default::default()
        };
        let result = apply_provider_filter(&endpoints, &prefs);
        assert!(
            result.is_empty(),
            "No endpoints should match an unknown provider slug"
        );
    }

    #[test]
    fn test_apply_provider_filter_ignore_all_results_in_empty() {
        let endpoints = mock_endpoints();
        let prefs = crate::gateway::types::ProviderPreferences {
            ignore: Some(vec!["openai".into(), "anthropic".into(), "bedrock".into()]),
            ..Default::default()
        };
        let result = apply_provider_filter(&endpoints, &prefs);
        assert!(result.is_empty());
    }
}
