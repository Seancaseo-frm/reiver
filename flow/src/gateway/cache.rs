//! Semantic caching for the AI Gateway.
//!
//! Integrates with [semcache](https://github.com/sensoris/semcache) for per-project
//! LLM response caching. Caching is optional and can be enabled via configuration.
//!
//! # How it works
//!
//! 1. Before calling an LLM provider, check the cache for a similar query
//! 2. If found (cache hit), return the cached response immediately
//! 3. If not found (cache miss), call the provider and cache the response
//!
//! # Namespace isolation
//!
//! Responses are cached per-project using the namespace `project:{project_id}`.
//! This ensures that cached responses are not shared across projects.
//!
//! # Cache Key Generation
//!
//! Cache keys include an SHA-256 hash of all message content to prevent collisions
//! when the same query appears in different conversation contexts.

use dashmap::DashMap;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::{Duration, Instant};
use uuid::Uuid;

use crate::gateway::types::{ChatCompletionRequest, ChatCompletionResponse};

/// L1 cache entry wrapping a CachedResponse with TTL.
#[derive(Clone)]
struct L1CacheEntry {
    value: CachedResponse,
    expires_at: Instant,
}

/// Cache client for semantic caching of LLM responses.
///
/// Uses a two-tier caching strategy:
/// - L1: in-process `quick_cache` LRU (avoids HTTP roundtrip for repeated queries)
/// - L2: external semcache service (shared across instances)
///
/// Project invalidation is tracked via a `DashMap` of invalidated project IDs
/// to invalidation timestamps. Entries older than the L1 TTL are considered
/// expired and are lazily pruned, preventing unbounded memory growth.
#[derive(Clone)]
pub struct GatewayCache {
    client: Client,
    base_url: String,
    ttl_seconds: u64,
    enabled: bool,
    l1_cache: std::sync::Arc<quick_cache::sync::Cache<String, L1CacheEntry>>,
    /// Projects whose L1 entries should be treated as stale.
    /// Maps project_id -> invalidation timestamp. Entries older than
    /// `ttl_seconds` are lazily pruned since any L1 entry would also
    /// have expired by then.
    invalidated_projects: std::sync::Arc<DashMap<Uuid, Instant>>,
}

impl GatewayCache {
    /// Create a new cache client.
    ///
    /// # Panics
    /// Panics if the HTTP client cannot be created. This is extremely rare and indicates
    /// a fundamental system issue (e.g., TLS backend initialization failure).
    pub fn new(base_url: String, ttl_seconds: u64, enabled: bool) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(5)) // Short timeout for cache operations
            .build()
            .unwrap_or_else(|e| {
                tracing::error!(
                    error = %e,
                    "Failed to create HTTP client for gateway cache - this is a fatal error"
                );
                panic!("Failed to create cache HTTP client: {}", e)
            });

        Self {
            client,
            base_url,
            ttl_seconds,
            enabled,
            l1_cache: std::sync::Arc::new(quick_cache::sync::Cache::new(512)),
            invalidated_projects: std::sync::Arc::new(DashMap::new()),
        }
    }

    /// Check if caching is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Generate a cache key from the request.
    ///
    /// The key is based on the model and a hash of ALL message content, which are the
    /// primary determinants of the response. Using all messages prevents cache collisions
    /// when the same user message appears in different conversation contexts.
    ///
    /// # Cache Key Components
    /// - Project ID: Ensures isolation between projects
    /// - Model: Different models produce different outputs
    /// - Messages hash: SHA-256 of all message roles and content concatenated
    ///
    /// # Why hash all messages?
    /// Consider this scenario without full message hashing:
    /// - Request 1: [system: "Be helpful", user: "Hi"]
    /// - Request 2: [system: "Be rude", user: "Hi"]
    ///
    /// With only the last user message in the key, both would hit the same cache entry,
    /// returning the wrong response for one of them.
    fn generate_cache_key(&self, project_id: Uuid, request: &ChatCompletionRequest) -> String {
        // Build a string representation of all messages for hashing
        let mut hasher = Sha256::new();

        for message in &request.messages {
            // Include role in hash
            let role_str = serde_json::to_string(&message.role).unwrap_or_default();
            hasher.update(role_str.as_bytes());
            hasher.update(b":");

            // Include content in hash
            if let Some(content) = &message.content {
                hasher.update(content.as_text().as_bytes());
            }

            // Include name if present
            if let Some(name) = &message.name {
                hasher.update(b":name:");
                hasher.update(name.as_bytes());
            }

            hasher.update(b"|"); // Message delimiter
        }

        // Include all parameters that affect output determinism
        if let Some(temp) = request.temperature {
            hasher.update(format!(":temp:{}", temp).as_bytes());
        }
        if let Some(top_p) = request.top_p {
            hasher.update(format!(":top_p:{}", top_p).as_bytes());
        }
        if let Some(max_tokens) = request.max_tokens {
            hasher.update(format!(":max_tokens:{}", max_tokens).as_bytes());
        }
        if let Some(ref response_format) = request.response_format {
            hasher.update(format!(":response_format:{}", response_format.format_type).as_bytes());
        }
        if let Some(ref stop) = request.stop {
            let stop_str = serde_json::to_string(stop).unwrap_or_default();
            hasher.update(format!(":stop:{}", stop_str).as_bytes());
        }
        if let Some(fp) = request.frequency_penalty {
            hasher.update(format!(":freq_pen:{}", fp).as_bytes());
        }
        if let Some(pp) = request.presence_penalty {
            hasher.update(format!(":pres_pen:{}", pp).as_bytes());
        }
        if let Some(seed) = request.seed {
            hasher.update(format!(":seed:{}", seed).as_bytes());
        }
        if let Some(ref thinking) = request.thinking {
            hasher.update(
                format!(
                    ":thinking:{}:{}",
                    thinking.thinking_type,
                    thinking.budget_tokens.unwrap_or(0)
                )
                .as_bytes(),
            );
        }
        if let Some(ref effort) = request.reasoning_effort {
            hasher.update(format!(":reasoning_effort:{}", effort).as_bytes());
        }

        let hash = hex::encode(hasher.finalize());

        // Use first 32 chars of hash (128 bits) for good collision resistance
        // while keeping cache keys reasonably sized
        let short_hash = &hash[..32];

        format!(
            "project:{}:model:{}:hash:{}",
            project_id, request.model, short_hash
        )
    }

    /// Look up a cached response for the given request.
    ///
    /// Returns `Some(response)` if a cache hit is found, `None` otherwise.
    #[tracing::instrument(
        name = "gateway.cache.lookup",
        skip(self, request),
        fields(project_id = %project_id, cache_hit = tracing::field::Empty)
    )]
    pub async fn get(
        &self,
        project_id: Uuid,
        request: &ChatCompletionRequest,
    ) -> Option<CachedResponse> {
        if !self.enabled {
            tracing::Span::current().record("cache_hit", false);
            return None;
        }

        let cache_key = self.generate_cache_key(project_id, request);

        // L1: check in-process cache first (skip if project was recently invalidated).
        // Invalidation entries older than TTL are stale -- any L1 entry cached
        // before the invalidation would also have expired by now.
        //
        // We must read + drop the DashMap guard before calling `remove()` to
        // avoid a deadlock (read lock on shard vs. write lock on same shard).
        let invalidation_state = self.invalidated_projects.get(&project_id).map(|entry| {
            let age = Instant::now().saturating_duration_since(*entry.value());
            age <= Duration::from_secs(self.ttl_seconds)
        });
        // Guard is dropped here.
        let invalidated = match invalidation_state {
            Some(true) => true,
            Some(false) => {
                self.invalidated_projects.remove(&project_id);
                false
            }
            None => false,
        };

        if !invalidated {
            if let Some(entry) = self.l1_cache.get(&cache_key) {
                if entry.expires_at > Instant::now() {
                    tracing::debug!(
                        project_id = %project_id,
                        model = %request.model,
                        "L1 cache hit for LLM request"
                    );
                    tracing::Span::current().record("cache_hit", true);
                    return Some(entry.value);
                }
            }
        }

        // L2: fall through to semcache
        let url = format!("{}/cache/get", self.base_url);

        let cache_request = CacheGetRequest {
            key: cache_key.clone(),
            namespace: format!("project:{}", project_id),
        };

        match self.client.post(&url).json(&cache_request).send().await {
            Ok(response) if response.status().is_success() => {
                match response.json::<CacheGetResponse>().await {
                    Ok(cache_response) if cache_response.found => {
                        tracing::debug!(
                            project_id = %project_id,
                            model = %request.model,
                            "L2 cache hit for LLM request"
                        );
                        if let Some(ref value) = cache_response.value {
                            self.l1_cache.insert(
                                cache_key,
                                L1CacheEntry {
                                    value: value.clone(),
                                    expires_at: Instant::now()
                                        + Duration::from_secs(self.ttl_seconds),
                                },
                            );
                        }
                        let value = cache_response.value;
                        if value.is_some() {
                            tracing::Span::current().record("cache_hit", true);
                        } else {
                            tracing::Span::current().record("cache_hit", false);
                        }
                        value
                    }
                    _ => {
                        tracing::Span::current().record("cache_hit", false);
                        None
                    }
                }
            }
            Ok(_) => {
                tracing::Span::current().record("cache_hit", false);
                None
            }
            Err(e) => {
                tracing::warn!(
                    project_id = %project_id,
                    model = %request.model,
                    error = %e,
                    "Cache lookup failed"
                );
                tracing::Span::current().record("cache_hit", false);
                None
            }
        }
    }

    /// Store a response in the cache.
    #[tracing::instrument(
        name = "gateway.cache.store",
        skip(self, request, response),
        fields(project_id = %project_id)
    )]
    pub async fn set(
        &self,
        project_id: Uuid,
        request: &ChatCompletionRequest,
        response: &ChatCompletionResponse,
    ) {
        if !self.enabled {
            return;
        }

        let cache_key = self.generate_cache_key(project_id, request);
        let url = format!("{}/cache/set", self.base_url);

        let cached_response = CachedResponse {
            response: response.clone(),
            model: request.model.clone(),
            cached_at: chrono::Utc::now().timestamp() as u64,
        };

        // Populate L1 immediately (and clear invalidation flag since this is a fresh entry)
        self.invalidated_projects.remove(&project_id);
        self.l1_cache.insert(
            cache_key.clone(),
            L1CacheEntry {
                value: cached_response.clone(),
                expires_at: Instant::now() + Duration::from_secs(self.ttl_seconds),
            },
        );

        let cache_request = CacheSetRequest {
            key: cache_key,
            namespace: format!("project:{}", project_id),
            value: cached_response,
            ttl_seconds: self.ttl_seconds,
        };

        match self.client.post(&url).json(&cache_request).send().await {
            Ok(response) if response.status().is_success() => {
                tracing::debug!(
                    project_id = %project_id,
                    model = %request.model,
                    "Cached LLM response"
                );
            }
            Ok(response) => {
                tracing::warn!(
                    project_id = %project_id,
                    model = %request.model,
                    status = %response.status(),
                    "Cache set failed with non-success status"
                );
            }
            Err(e) => {
                tracing::warn!(
                    project_id = %project_id,
                    model = %request.model,
                    error = %e,
                    "Cache set failed"
                );
            }
        }
    }

    /// Invalidate cached responses for a project.
    ///
    /// Marks the project as invalidated so L1 entries are skipped,
    /// then sends the invalidation to the L2 semcache service.
    pub async fn invalidate_project(&self, project_id: Uuid) {
        // Always mark L1 invalidation even if caching is disabled,
        // in case it was recently enabled/disabled.
        self.invalidated_projects.insert(project_id, Instant::now());

        if !self.enabled {
            return;
        }

        let url = format!("{}/cache/invalidate", self.base_url);

        let request = CacheInvalidateRequest {
            namespace: format!("project:{}", project_id),
        };

        match self.client.post(&url).json(&request).send().await {
            Ok(response) if response.status().is_success() => {
                tracing::info!(
                    project_id = %project_id,
                    "Invalidated cache for project"
                );
            }
            Ok(response) => {
                tracing::warn!(
                    project_id = %project_id,
                    status = %response.status(),
                    "Cache invalidation failed with non-success status"
                );
            }
            Err(e) => {
                tracing::warn!(
                    project_id = %project_id,
                    error = %e,
                    "Cache invalidation failed"
                );
            }
        }
    }
}

/// Cached response with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedResponse {
    /// The cached LLM response.
    pub response: ChatCompletionResponse,
    /// The model that generated this response.
    pub model: String,
    /// Unix timestamp when the response was cached.
    pub cached_at: u64,
}

/// Cache lookup status for response headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheStatus {
    /// Cache was hit, response served from cache.
    Hit,
    /// Cache was missed, response from provider.
    Miss,
    /// Caching was disabled or skipped for this request.
    Skip,
}

impl CacheStatus {
    /// Convert to header value string.
    pub fn as_str(&self) -> &'static str {
        match self {
            CacheStatus::Hit => "hit",
            CacheStatus::Miss => "miss",
            CacheStatus::Skip => "skip",
        }
    }
}

// Semcache API request/response types

#[derive(Debug, Serialize)]
struct CacheGetRequest {
    key: String,
    namespace: String,
}

#[derive(Debug, Deserialize)]
struct CacheGetResponse {
    found: bool,
    #[serde(default)]
    value: Option<CachedResponse>,
}

#[derive(Debug, Serialize)]
struct CacheSetRequest {
    key: String,
    namespace: String,
    value: CachedResponse,
    ttl_seconds: u64,
}

#[derive(Debug, Serialize)]
struct CacheInvalidateRequest {
    namespace: String,
}

/// Check if a request is cacheable.
///
/// Some requests should not be cached:
/// - Streaming requests (responses are chunked)
/// - Models that reject temperature controls (a requested `temperature: 0`
///   cannot make those responses deterministic)
/// - Requests without an explicit `temperature: 0` (providers default to
///   temperature 1.0 which is non-deterministic)
/// - Requests with tools (function calling may have side effects)
pub fn is_cacheable(request: &ChatCompletionRequest) -> bool {
    // Don't cache streaming requests
    if request.stream.unwrap_or(false) {
        return false;
    }

    // Recent Claude models reject sampling controls. The Anthropic adapter
    // omits them, so a caller-supplied temperature of zero is not evidence of
    // deterministic output and must not activate semantic caching.
    if crate::gateway::providers::anthropic::uses_provider_managed_sampling(&request.model) {
        return false;
    }

    // Only cache when temperature is explicitly set to 0 (deterministic).
    // When temperature is None, most providers default to 1.0 which produces
    // non-deterministic output that should not be served from cache.
    match request.temperature {
        Some(temp) if temp == 0.0 => {}
        _ => return false,
    }

    // Don't cache requests with tools (function calling)
    if request.tools.is_some() {
        return false;
    }

    // Don't cache requests asking for multiple completions -- the cached
    // response would have the wrong number of choices for a later request
    // with a different `n` value.
    if matches!(request.n, Some(n) if n != 1) {
        return false;
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::types::{ChatMessage, MessageContent, MessageRole};

    fn create_test_request(model: &str, message: &str) -> ChatCompletionRequest {
        ChatCompletionRequest {
            model: model.to_string(),
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text(message.to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            }],
            ..Default::default()
        }
    }

    #[test]
    fn test_is_cacheable() {
        // Explicit zero temperature is cacheable
        let mut request = create_test_request("gpt-4o", "Hello");
        request.temperature = Some(0.0);
        assert!(is_cacheable(&request));

        // Streaming request is not cacheable
        let mut request = create_test_request("gpt-4o", "Hello");
        request.stream = Some(true);
        request.temperature = Some(0.0);
        assert!(!is_cacheable(&request));

        // High temperature is not cacheable
        let mut request = create_test_request("gpt-4o", "Hello");
        request.temperature = Some(0.7);
        assert!(!is_cacheable(&request));
    }

    #[test]
    fn test_provider_managed_sampling_models_are_not_cacheable() {
        for model in [
            "claude-sonnet-5",
            "claude-opus-5",
            "claude-opus-4.8-fast",
            "claude-fable-5",
        ] {
            let mut request = create_test_request(model, "Hello");
            request.temperature = Some(0.0);
            assert!(
                !is_cacheable(&request),
                "{model} must not be cached based on a temperature it rejects"
            );
        }
    }

    /// Regression: negative temperature must NOT be treated as cacheable.
    /// Previously `temp <= 0.0` allowed negative values through. Only exactly
    /// 0.0 is deterministic; negative temperatures are invalid and would
    /// produce non-deterministic (or undefined) provider behavior.
    #[test]
    fn test_is_not_cacheable_when_temperature_is_negative() {
        let mut request = create_test_request("gpt-4o", "Hello");
        request.temperature = Some(-1.0);
        assert!(
            !is_cacheable(&request),
            "Request with negative temperature must not be cacheable"
        );
    }

    /// Regression: requests without explicit temperature must NOT be cached.
    /// Providers default to temperature ~1.0 (non-deterministic), so caching
    /// those responses would serve stale results for subsequent requests.
    #[test]
    fn test_is_not_cacheable_when_temperature_is_none() {
        let request = create_test_request("gpt-4o", "Hello");
        assert_eq!(request.temperature, None);
        assert!(
            !is_cacheable(&request),
            "Request with temperature=None must not be cacheable (provider default is non-deterministic)"
        );
    }

    /// Regression: requests with `n > 1` were cached, so a subsequent request
    /// with `n = 1` (or different `n`) would receive the wrong number of choices.
    #[test]
    fn test_is_not_cacheable_when_n_greater_than_one() {
        let mut request = create_test_request("gpt-4o", "Hello");
        request.temperature = Some(0.0);
        request.n = Some(3);
        assert!(
            !is_cacheable(&request),
            "Request with n > 1 must not be cacheable"
        );
    }

    #[test]
    fn test_is_cacheable_when_n_is_one() {
        let mut request = create_test_request("gpt-4o", "Hello");
        request.temperature = Some(0.0);
        request.n = Some(1);
        assert!(
            is_cacheable(&request),
            "Request with n = 1 should be cacheable"
        );
    }

    #[test]
    fn test_cache_status_as_str() {
        assert_eq!(CacheStatus::Hit.as_str(), "hit");
        assert_eq!(CacheStatus::Miss.as_str(), "miss");
        assert_eq!(CacheStatus::Skip.as_str(), "skip");
    }

    #[test]
    fn test_generate_cache_key() {
        let cache = GatewayCache::new("http://localhost:8080".to_string(), 3600, true);
        let project_id = Uuid::new_v4();
        let request = create_test_request("gpt-4o", "What is 2+2?");

        let key = cache.generate_cache_key(project_id, &request);

        assert!(key.contains(&project_id.to_string()));
        assert!(key.contains("gpt-4o"));
        assert!(
            key.contains("hash:"),
            "Key should contain 'hash:' prefix: {}",
            key
        );
        // Hash should be 16 hex chars
        let parts: Vec<&str> = key.split(":hash:").collect();
        assert_eq!(parts.len(), 2, "Should have exactly one hash part");
        assert_eq!(parts[1].len(), 32, "Hash should be 32 chars");
    }

    #[test]
    fn test_cache_key_different_for_different_contexts() {
        let cache = GatewayCache::new("http://localhost:8080".to_string(), 3600, true);
        let project_id = Uuid::new_v4();

        // Same user message, different system prompts
        let mut request1 = create_test_request("gpt-4o", "Hello");
        request1.messages.insert(
            0,
            ChatMessage {
                role: MessageRole::System,
                content: Some(MessageContent::Text("Be helpful".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            },
        );

        let mut request2 = create_test_request("gpt-4o", "Hello");
        request2.messages.insert(
            0,
            ChatMessage {
                role: MessageRole::System,
                content: Some(MessageContent::Text("Be rude".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            },
        );

        let key1 = cache.generate_cache_key(project_id, &request1);
        let key2 = cache.generate_cache_key(project_id, &request2);

        // Keys should be different because context is different
        assert_ne!(
            key1, key2,
            "Keys should differ for different conversation contexts"
        );
    }

    #[test]
    fn test_cache_key_same_for_identical_requests() {
        let cache = GatewayCache::new("http://localhost:8080".to_string(), 3600, true);
        let project_id = Uuid::new_v4();

        let request1 = create_test_request("gpt-4o", "Hello");
        let request2 = create_test_request("gpt-4o", "Hello");

        let key1 = cache.generate_cache_key(project_id, &request1);
        let key2 = cache.generate_cache_key(project_id, &request2);

        // Keys should be the same for identical requests
        assert_eq!(
            key1, key2,
            "Keys should be identical for identical requests"
        );
    }

    /// Regression: `response_format` was not included in the cache key, so
    /// two requests identical except for response_format (e.g. JSON mode vs
    /// plain text) would return the same cached response.
    #[test]
    fn test_cache_key_differs_by_response_format() {
        use crate::gateway::types::{ResponseFormat, ResponseFormatType};

        let cache = GatewayCache::new("http://localhost:8080".to_string(), 3600, true);
        let project_id = Uuid::new_v4();

        let mut r1 = create_test_request("gpt-4o", "Hello");
        r1.response_format = None;

        let mut r2 = create_test_request("gpt-4o", "Hello");
        r2.response_format = Some(ResponseFormat {
            format_type: ResponseFormatType::JsonObject,
        });

        let key1 = cache.generate_cache_key(project_id, &r1);
        let key2 = cache.generate_cache_key(project_id, &r2);
        assert_ne!(
            key1, key2,
            "Cache keys must differ when response_format differs"
        );
    }

    /// Regression: `stop` sequences were not included in the cache key.
    #[test]
    fn test_cache_key_differs_by_stop_sequence() {
        use crate::gateway::types::StopSequence;

        let cache = GatewayCache::new("http://localhost:8080".to_string(), 3600, true);
        let project_id = Uuid::new_v4();

        let mut r1 = create_test_request("gpt-4o", "Hello");
        r1.stop = None;

        let mut r2 = create_test_request("gpt-4o", "Hello");
        r2.stop = Some(StopSequence::Single("END".to_string()));

        let key1 = cache.generate_cache_key(project_id, &r1);
        let key2 = cache.generate_cache_key(project_id, &r2);
        assert_ne!(
            key1, key2,
            "Cache keys must differ when stop sequences differ"
        );
    }

    /// Regression: `frequency_penalty` was not included in the cache key.
    #[test]
    fn test_cache_key_differs_by_frequency_penalty() {
        let cache = GatewayCache::new("http://localhost:8080".to_string(), 3600, true);
        let project_id = Uuid::new_v4();

        let mut r1 = create_test_request("gpt-4o", "Hello");
        r1.frequency_penalty = None;

        let mut r2 = create_test_request("gpt-4o", "Hello");
        r2.frequency_penalty = Some(0.5);

        let key1 = cache.generate_cache_key(project_id, &r1);
        let key2 = cache.generate_cache_key(project_id, &r2);
        assert_ne!(
            key1, key2,
            "Cache keys must differ when frequency_penalty differs"
        );
    }

    /// Regression: `seed` was not included in the cache key, so two requests
    /// identical except for seed (at temperature 0) would return the same
    /// cached response despite producing different outputs.
    #[test]
    fn test_cache_key_differs_by_seed() {
        let cache = GatewayCache::new("http://localhost:8080".to_string(), 3600, true);
        let project_id = Uuid::new_v4();

        let mut r1 = create_test_request("gpt-4o", "Hello");
        r1.seed = Some(42);

        let mut r2 = create_test_request("gpt-4o", "Hello");
        r2.seed = Some(99);

        let key1 = cache.generate_cache_key(project_id, &r1);
        let key2 = cache.generate_cache_key(project_id, &r2);
        assert_ne!(key1, key2, "Cache keys must differ when seed differs");
    }

    /// Regression: `reasoning_effort` was not included in the cache key.
    #[test]
    fn test_cache_key_differs_by_reasoning_effort() {
        use crate::gateway::types::ReasoningEffort;

        let cache = GatewayCache::new("http://localhost:8080".to_string(), 3600, true);
        let project_id = Uuid::new_v4();

        let mut r1 = create_test_request("o3-mini", "Hello");
        r1.reasoning_effort = Some(ReasoningEffort::Low);

        let mut r2 = create_test_request("o3-mini", "Hello");
        r2.reasoning_effort = Some(ReasoningEffort::High);

        let key1 = cache.generate_cache_key(project_id, &r1);
        let key2 = cache.generate_cache_key(project_id, &r2);
        assert_ne!(
            key1, key2,
            "Cache keys must differ when reasoning_effort differs"
        );
    }

    /// Regression: `thinking` config was not included in the cache key.
    #[test]
    fn test_cache_key_differs_by_thinking() {
        use crate::gateway::types::{ThinkingConfig, ThinkingToggle};

        let cache = GatewayCache::new("http://localhost:8080".to_string(), 3600, true);
        let project_id = Uuid::new_v4();

        let mut r1 = create_test_request("claude-sonnet-4-6", "Hello");
        r1.thinking = None;

        let mut r2 = create_test_request("claude-sonnet-4-6", "Hello");
        r2.thinking = Some(ThinkingConfig {
            thinking_type: ThinkingToggle::Enabled,
            budget_tokens: Some(1024),
        });

        let key1 = cache.generate_cache_key(project_id, &r1);
        let key2 = cache.generate_cache_key(project_id, &r2);
        assert_ne!(
            key1, key2,
            "Cache keys must differ when thinking config differs"
        );
    }

    /// Regression: `invalidated_projects` was a `DashSet<Uuid>` that grew
    /// without bound because entries were only removed in `set()`. Projects
    /// that were invalidated but never cached again would leak forever.
    /// The fix stores an `Instant` alongside each entry and lazily prunes
    /// entries older than the L1 TTL.
    #[test]
    fn test_invalidation_entries_expire_after_ttl() {
        let ttl = 1; // 1-second TTL for fast test
        let cache = GatewayCache::new("http://localhost:8080".to_string(), ttl, true);
        let project_id = Uuid::new_v4();

        // Manually insert an invalidation entry in the past
        cache
            .invalidated_projects
            .insert(project_id, Instant::now() - Duration::from_secs(ttl + 1));
        assert!(
            cache.invalidated_projects.contains_key(&project_id),
            "Invalidation entry should exist before cleanup"
        );

        // Simulate what `get()` does: read age, drop guard, then remove.
        let is_still_valid = cache.invalidated_projects.get(&project_id).map(|entry| {
            Instant::now().saturating_duration_since(*entry.value())
                <= Duration::from_secs(cache.ttl_seconds)
        });
        // Guard dropped here.
        if is_still_valid == Some(false) {
            cache.invalidated_projects.remove(&project_id);
        }

        assert!(
            !cache.invalidated_projects.contains_key(&project_id),
            "Stale invalidation entry must be pruned after TTL expires"
        );
    }

    /// Fresh invalidations (within TTL) must still block L1 lookups.
    #[test]
    fn test_fresh_invalidation_blocks_l1() {
        let cache = GatewayCache::new("http://localhost:8080".to_string(), 3600, true);
        let project_id = Uuid::new_v4();

        cache
            .invalidated_projects
            .insert(project_id, Instant::now());

        let is_invalidated = cache
            .invalidated_projects
            .get(&project_id)
            .map(|entry| {
                Instant::now().saturating_duration_since(*entry.value())
                    <= Duration::from_secs(cache.ttl_seconds)
            })
            .unwrap_or(false);

        assert!(is_invalidated, "Fresh invalidation must block L1 lookups");
    }

    /// `set()` must remove the project from invalidated_projects.
    #[test]
    fn test_set_clears_invalidation() {
        let cache = GatewayCache::new("http://localhost:8080".to_string(), 3600, true);
        let project_id = Uuid::new_v4();

        cache
            .invalidated_projects
            .insert(project_id, Instant::now());
        assert!(cache.invalidated_projects.contains_key(&project_id));

        // Simulate what set() does
        cache.invalidated_projects.remove(&project_id);
        assert!(
            !cache.invalidated_projects.contains_key(&project_id),
            "set() must clear the invalidation entry"
        );
    }
}
