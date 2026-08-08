//! Global model-level stats cache for the public pricing/catalog API.
//!
//! Reads pre-aggregated hourly stats from the ClickHouse
//! `llm_global_model_stats` refreshable MV (refreshes every 2 min).
//! Flow caches the result in-memory via `ArcSwap` and re-queries every 5 min.

use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reiver_core::clickhouse_db::ClickHousePool;
use tracing::{debug, info, warn};

const CACHE_REFRESH_INTERVAL: Duration = Duration::from_secs(300);

/// Per-model performance and security stats aggregated over the last 24 hours.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ModelStats {
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub ttft_p50_ms: f64,
    pub ttft_p95_ms: f64,
    pub error_rate: f64,
    pub guardrail_rate: f64,
    pub pii_rate: f64,
    pub injection_rate: f64,
    pub request_count_24h: u64,
}

/// ClickHouse row from the `llm_global_model_stats` aggregation query.
#[derive(Debug, clickhouse::Row, serde::Deserialize)]
struct StatsRow {
    provider: String,
    model: String,
    request_count: u64,
    error_count: u64,
    p50_duration_ms: f64,
    p95_duration_ms: f64,
    p99_duration_ms: f64,
    p50_ttft_ms: f64,
    p95_ttft_ms: f64,
    guardrail_triggered_count: u64,
    pii_violation_count: u64,
    injection_violation_count: u64,
}

type StatsMap = HashMap<(String, String), ModelStats>;

struct StatsSnapshot {
    by_model: StatsMap,
}

/// In-memory cache for global model stats from ClickHouse.
///
/// Pattern mirrors `ModelCatalogCache`: `ArcSwap` for lock-free reads,
/// atomic timestamp + CAS guard for refresh dedup.
#[derive(Clone)]
pub struct GlobalModelStatsCache {
    clickhouse: Arc<ClickHousePool>,
    cache: Arc<ArcSwap<StatsSnapshot>>,
    cache_updated_secs: Arc<AtomicU64>,
    refreshing: Arc<AtomicBool>,
}

impl GlobalModelStatsCache {
    pub fn new(clickhouse: Arc<ClickHousePool>) -> Self {
        Self {
            clickhouse,
            cache: Arc::new(ArcSwap::from_pointee(StatsSnapshot {
                by_model: HashMap::new(),
            })),
            cache_updated_secs: Arc::new(AtomicU64::new(0)),
            refreshing: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn initialize(&self) {
        if let Err(e) = self.refresh_cache().await {
            warn!("Failed initial load of global model stats: {e}");
        }
    }

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs()
    }

    async fn maybe_refresh(&self) {
        let now = Self::now_secs();
        let last = self.cache_updated_secs.load(Ordering::Relaxed);
        if now.saturating_sub(last) <= CACHE_REFRESH_INTERVAL.as_secs() {
            return;
        }
        if self
            .refreshing
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            if let Err(e) = self.refresh_cache().await {
                warn!("Failed to refresh global model stats cache: {e}");
            }
            self.refreshing.store(false, Ordering::Release);
        } else {
            debug!("Global model stats cache refresh already in progress, skipping");
        }
    }

    async fn refresh_cache(&self) -> anyhow::Result<()> {
        debug!("Refreshing global model stats cache from ClickHouse");

        let query = r#"
            SELECT
                provider,
                model,
                sum(request_count)              AS request_count,
                sum(error_count)                AS error_count,
                avg(p50_duration_ms)            AS p50_duration_ms,
                avg(p95_duration_ms)            AS p95_duration_ms,
                avg(p99_duration_ms)            AS p99_duration_ms,
                avg(p50_ttft_ms)                AS p50_ttft_ms,
                avg(p95_ttft_ms)                AS p95_ttft_ms,
                sum(guardrail_triggered_count)  AS guardrail_triggered_count,
                sum(pii_violation_count)        AS pii_violation_count,
                sum(injection_violation_count)  AS injection_violation_count
            FROM reiver.llm_global_model_stats
            GROUP BY provider, model
        "#;

        let rows: Vec<StatsRow> = self.clickhouse.query(query).fetch_all().await?;

        let mut by_model = HashMap::with_capacity(rows.len());
        for row in &rows {
            let total = row.request_count.max(1) as f64;
            let stats = ModelStats {
                p50_ms: row.p50_duration_ms,
                p95_ms: row.p95_duration_ms,
                p99_ms: row.p99_duration_ms,
                ttft_p50_ms: row.p50_ttft_ms,
                ttft_p95_ms: row.p95_ttft_ms,
                error_rate: row.error_count as f64 / total,
                guardrail_rate: row.guardrail_triggered_count as f64 / total,
                pii_rate: row.pii_violation_count as f64 / total,
                injection_rate: row.injection_violation_count as f64 / total,
                request_count_24h: row.request_count,
            };
            by_model.insert((row.provider.clone(), row.model.clone()), stats);
        }

        let count = by_model.len();
        self.cache
            .store(Arc::new(StatsSnapshot { by_model }));
        self.cache_updated_secs
            .store(Self::now_secs(), Ordering::Release);

        info!("Global model stats cache refreshed with {} model entries", count);
        Ok(())
    }

    /// Look up stats for a specific provider + model pair.
    pub async fn get(&self, provider: &str, model: &str) -> Option<ModelStats> {
        self.maybe_refresh().await;
        let snapshot = self.cache.load();
        snapshot
            .by_model
            .get(&(provider.to_string(), model.to_string()))
            .cloned()
    }

    /// Return the full stats map (provider+model -> stats). Triggers refresh if stale.
    pub async fn all(&self) -> HashMap<(String, String), ModelStats> {
        self.maybe_refresh().await;
        let snapshot = self.cache.load();
        snapshot.by_model.clone()
    }

    /// Create a cache for tests without a real ClickHouse connection.
    #[cfg(test)]
    pub fn new_for_test() -> Self {
        let client = clickhouse::Client::default().with_url("http://localhost:8123");
        let pool: ClickHousePool = Arc::new(client);
        Self::new(Arc::new(pool))
    }
}
