use arc_swap::ArcSwap;
use reiver_core::error::AppError;
use reiver_core::llm::types::ModelCatalogRow;
use sqlx::PgPool;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

use super::provider_types::Provider;

const CACHE_REFRESH_INTERVAL: Duration = Duration::from_secs(300);

/// Lightweight view of a `model_catalog` row for API consumers.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CatalogEntry {
    pub id: String,
    pub name: String,
    pub provider_slug: String,
    pub model_slug: String,
    pub context_length: Option<i32>,
    pub pricing: serde_json::Value,
    pub enabled: bool,
    pub created: Option<i64>,
}

impl CatalogEntry {
    /// Returns the gateway-routable model string.
    ///
    /// Slash-prefix providers (e.g. `deepseek/`, `mistral/`, `qwen/`) need the
    /// gateway prefix prepended to the bare model slug. Bare-prefix providers
    /// (e.g. OpenAI's `gpt-`, Anthropic's `claude-`) return the slug as-is.
    pub fn gateway_model_id(&self) -> String {
        match Provider::from_str(&self.provider_slug) {
            Ok(p) => {
                let prefixes = p.model_prefixes();
                if let Some(slash) = prefixes.iter().find(|pfx| pfx.contains('/')) {
                    format!("{slash}{}", self.model_slug)
                } else {
                    self.model_slug.clone()
                }
            }
            Err(_) => self.model_slug.clone(),
        }
    }
}

impl From<ModelCatalogRow> for CatalogEntry {
    fn from(row: ModelCatalogRow) -> Self {
        Self {
            id: row.id,
            name: row.name,
            provider_slug: row.provider_slug,
            model_slug: row.model_slug,
            context_length: row.context_length,
            pricing: row.pricing,
            enabled: row.enabled,
            created: row.created,
        }
    }
}

/// Snapshot atomically swapped via `ArcSwap`.
struct CatalogSnapshot {
    /// Models grouped by provider slug (only enabled models).
    by_provider: HashMap<String, Vec<CatalogEntry>>,
    /// All models (enabled only) in a flat vec for catalog listing.
    all_entries: Vec<CatalogEntry>,
    /// Latest enabled model per provider (by `created` timestamp), used for
    /// auto-routing when no explicit model list is configured.
    auto_model: HashMap<String, String>,
}

/// In-memory cache for `model_catalog` data.
///
/// Uses `ArcSwap` for lock-free reads. Refreshes every 5 minutes.
/// Pattern mirrors [`reiver_core::llm::cost::CostCalculator`].
#[derive(Clone)]
pub struct ModelCatalogCache {
    db: Arc<PgPool>,
    cache: Arc<ArcSwap<CatalogSnapshot>>,
    cache_updated_secs: Arc<AtomicU64>,
    refreshing: Arc<AtomicBool>,
}

impl ModelCatalogCache {
    pub fn new(db: Arc<PgPool>) -> Self {
        let empty = CatalogSnapshot {
            by_provider: HashMap::new(),
            all_entries: Vec::new(),
            auto_model: HashMap::new(),
        };
        Self {
            db,
            cache: Arc::new(ArcSwap::from_pointee(empty)),
            cache_updated_secs: Arc::new(AtomicU64::new(0)),
            refreshing: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn initialize(&self) -> Result<(), AppError> {
        self.refresh_cache().await
    }

    fn current_timestamp_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs()
    }

    async fn maybe_refresh(&self) -> Result<(), AppError> {
        let now = Self::current_timestamp_secs();
        let last = self.cache_updated_secs.load(Ordering::Relaxed);
        if now.saturating_sub(last) <= CACHE_REFRESH_INTERVAL.as_secs() {
            return Ok(());
        }
        if self
            .refreshing
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            let result = self.refresh_cache().await;
            self.refreshing.store(false, Ordering::Release);
            return result;
        }
        debug!("Model catalog cache refresh already in progress, skipping");
        Ok(())
    }

    async fn refresh_cache(&self) -> Result<(), AppError> {
        debug!("Refreshing model catalog cache from database");

        let rows: Vec<ModelCatalogRow> = sqlx::query_as(
            "SELECT id, name, created, context_length, \
                    pricing, architecture, top_provider, supported_parameters, \
                    provider_slug, model_slug, enabled \
             FROM model_catalog \
             WHERE enabled = TRUE \
             ORDER BY provider_slug, model_slug",
        )
        .fetch_all(self.db.as_ref())
        .await?;

        let mut by_provider: HashMap<String, Vec<CatalogEntry>> = HashMap::new();
        let mut all_entries = Vec::with_capacity(rows.len());

        for row in rows {
            let entry = CatalogEntry::from(row);
            by_provider
                .entry(entry.provider_slug.clone())
                .or_default()
                .push(entry.clone());
            all_entries.push(entry);
        }

        let count = all_entries.len();

        let auto_model: HashMap<String, String> = by_provider
            .iter()
            .filter_map(|(slug, entries)| {
                entries
                    .iter()
                    .max_by_key(|e| e.created.unwrap_or(0))
                    .map(|e| (slug.clone(), e.gateway_model_id()))
            })
            .collect();

        let snapshot = CatalogSnapshot {
            by_provider,
            all_entries,
            auto_model,
        };

        self.cache.store(Arc::new(snapshot));
        self.cache_updated_secs
            .store(Self::current_timestamp_secs(), Ordering::Release);

        info!("Model catalog cache refreshed with {} enabled models", count);
        Ok(())
    }

    /// Return all catalog entries for a given provider slug.
    pub async fn models_for_provider(&self, slug: &str) -> Vec<CatalogEntry> {
        if let Err(e) = self.maybe_refresh().await {
            warn!("Failed to refresh model catalog cache: {e}");
        }
        let snapshot = self.cache.load();
        snapshot
            .by_provider
            .get(slug)
            .cloned()
            .unwrap_or_default()
    }

    /// Return all providers that have at least one enabled model, with their entries.
    pub async fn all_providers_with_models(&self) -> Vec<(String, Vec<CatalogEntry>)> {
        if let Err(e) = self.maybe_refresh().await {
            warn!("Failed to refresh model catalog cache: {e}");
        }
        let snapshot = self.cache.load();
        snapshot
            .by_provider
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Return the auto-routing model ID for a provider (the latest enabled model
    /// by `created` timestamp). Lock-free snapshot read, no refresh triggered.
    pub fn auto_model_for_provider(&self, slug: &str) -> Option<String> {
        let snapshot = self.cache.load();
        snapshot.auto_model.get(slug).cloned()
    }

    /// Create a cache pre-seeded with entries, without requiring a database.
    #[cfg(test)]
    pub fn new_for_test(entries: Vec<CatalogEntry>) -> Self {
        let cache = Self::new(Arc::new(
            sqlx::PgPool::connect_lazy("postgres://test:test@localhost/test").unwrap(),
        ));
        cache.seed_for_test(entries);
        cache
    }

    /// Inject test entries into the cache without requiring a database.
    #[cfg(test)]
    pub fn seed_for_test(&self, entries: Vec<CatalogEntry>) {
        let mut by_provider: HashMap<String, Vec<CatalogEntry>> = HashMap::new();
        for entry in &entries {
            by_provider
                .entry(entry.provider_slug.clone())
                .or_default()
                .push(entry.clone());
        }
        let auto_model: HashMap<String, String> = by_provider
            .iter()
            .filter_map(|(slug, entries)| {
                entries
                    .iter()
                    .max_by_key(|e| e.created.unwrap_or(0))
                    .map(|e| (slug.clone(), e.gateway_model_id()))
            })
            .collect();
        let snapshot = CatalogSnapshot {
            by_provider,
            all_entries: entries,
            auto_model,
        };
        self.cache.store(Arc::new(snapshot));
        self.cache_updated_secs
            .store(Self::current_timestamp_secs(), Ordering::Release);
    }

    /// Return all enabled entries (flat list).
    pub async fn all_entries(&self) -> Vec<CatalogEntry> {
        if let Err(e) = self.maybe_refresh().await {
            warn!("Failed to refresh model catalog cache: {e}");
        }
        let snapshot = self.cache.load();
        snapshot.all_entries.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_entry(provider_slug: &str, model_slug: &str) -> CatalogEntry {
        CatalogEntry {
            id: format!("{provider_slug}/{model_slug}"),
            name: model_slug.to_string(),
            provider_slug: provider_slug.to_string(),
            model_slug: model_slug.to_string(),
            context_length: Some(128_000),
            pricing: serde_json::json!({}),
            enabled: true,
            created: Some(1_700_000_000),
        }
    }

    #[test]
    fn gateway_model_id_bare_prefix_provider() {
        let entry = dummy_entry("openai", "gpt-4o");
        assert_eq!(entry.gateway_model_id(), "gpt-4o");

        let entry = dummy_entry("anthropic", "claude-sonnet-4-6");
        assert_eq!(entry.gateway_model_id(), "claude-sonnet-4-6");

        let entry = dummy_entry("google", "gemini-2.5-flash");
        assert_eq!(entry.gateway_model_id(), "gemini-2.5-flash");
    }

    #[test]
    fn gateway_model_id_slash_prefix_provider() {
        let entry = dummy_entry("deepseek", "deepseek-chat");
        assert_eq!(entry.gateway_model_id(), "deepseek/deepseek-chat");

        let entry = dummy_entry("groq", "llama-3.3-70b-versatile");
        assert_eq!(entry.gateway_model_id(), "groq/llama-3.3-70b-versatile");

        let entry = dummy_entry("qwen", "qwen-max");
        assert_eq!(entry.gateway_model_id(), "qwen/qwen-max");
    }

    #[test]
    fn gateway_model_id_unknown_provider_falls_back() {
        let entry = dummy_entry("nonexistent", "some-model");
        assert_eq!(entry.gateway_model_id(), "some-model");
    }
}

