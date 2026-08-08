//! Latency tracker for LLM providers.
//!
//! Stores latency samples in ClickHouse, computes percentiles (P50/P95/P99)
//! in a background task, and serves them from an in-memory cache.
//! Recording is non-blocking (samples are sent to a channel and batched to ClickHouse).

use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::clickhouse_db::ClickHousePool;
use crate::gateway::provider_types::Provider;

const WRITE_CHANNEL_CAP: usize = 10_000;
const FLUSH_BATCH_SIZE: usize = 500;
const FLUSH_BATCH_TIMEOUT_MS: u64 = 1000;

/// Configuration for the latency tracker.
#[derive(Debug, Clone)]
pub struct LatencyTrackerConfig {
    /// How long to keep samples in the rolling window (query window).
    /// Default: 5 minutes.
    pub window_duration: Duration,
    /// How often to refresh the in-memory percentile cache from ClickHouse.
    /// Default: 1 minute.
    pub cache_refresh_interval: Duration,
    /// P99 latency threshold to trigger immediate fallback.
    /// Default: 30 seconds.
    pub p99_fallback_threshold: Duration,
}

impl Default for LatencyTrackerConfig {
    fn default() -> Self {
        Self {
            window_duration: Duration::from_secs(300), // 5 minutes
            cache_refresh_interval: Duration::from_secs(60), // 1 minute
            p99_fallback_threshold: Duration::from_secs(30), // 30 seconds
        }
    }
}

/// Latency percentiles for a provider.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderLatency {
    pub p50: Duration,
    pub p95: Duration,
    pub p99: Duration,
    pub sample_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderLatencySummary {
    pub provider: String,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub sample_count: usize,
    pub is_degraded: bool,
}

/// Sample sent to the flusher (in-memory only).
struct LatencySample {
    prov: Provider,
    ts_ms: i64,
    duration_ms: u64,
}

/// Row for ClickHouse INSERT.
#[derive(clickhouse::Row, serde::Serialize)]
struct ProviderLatencyRow {
    provider: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    ts: DateTime<Utc>,
    duration_ms: u64,
}

/// Row for ClickHouse SELECT (quantiles).
#[derive(Debug, clickhouse::Row, serde::Deserialize)]
struct ProviderPercentileRow {
    provider: String,
    p50: f64,
    p95: f64,
    p99: f64,
    sample_count: u64,
}

/// Compute the index for a given percentile. Used by tests.
#[cfg(test)]
fn percentile_index(len: usize, percentile: usize) -> usize {
    if len == 0 {
        return 0;
    }
    let idx = (len * percentile + 99) / 100;
    idx.saturating_sub(1).min(len - 1)
}

/// Thread-safe latency tracker for all LLM providers.
///
/// Recording is non-blocking: samples are sent to a channel and batched to ClickHouse.
/// Hot-path reads use a pre-computed cache refreshed periodically from ClickHouse.
pub struct LatencyTracker {
    clickhouse: Arc<ClickHousePool>,
    write_tx: mpsc::Sender<LatencySample>,
    cached_percentiles: quick_cache::sync::Cache<Provider, ProviderLatency>,
    /// Providers that have an entry in the cache (for get_all_summaries iteration).
    cached_providers: RwLock<HashSet<Provider>>,
    config: LatencyTrackerConfig,
}

impl LatencyTracker {
    /// Create a new latency tracker with the given ClickHouse pool and default config.
    /// Pass `None` for tests (writes are dropped, refresh is no-op).
    pub fn new(clickhouse: Arc<ClickHousePool>) -> Self {
        Self::with_config(clickhouse, LatencyTrackerConfig::default())
    }

    /// Create a new latency tracker with custom configuration.
    pub fn with_config(clickhouse: Arc<ClickHousePool>, config: LatencyTrackerConfig) -> Self {
        let (write_tx, write_rx) = mpsc::channel(WRITE_CHANNEL_CAP);
        let ch_for_flusher = clickhouse.clone();
        tokio::spawn(async move {
            Self::flusher_loop(write_rx, ch_for_flusher).await;
        });
        Self {
            clickhouse,
            write_tx,
            cached_percentiles: quick_cache::sync::Cache::new(64),
            cached_providers: RwLock::new(HashSet::new()),
            config,
        }
    }

    async fn flusher_loop(mut rx: mpsc::Receiver<LatencySample>, clickhouse: Arc<ClickHousePool>) {
        let mut batch = Vec::with_capacity(FLUSH_BATCH_SIZE);
        let timeout = Duration::from_millis(FLUSH_BATCH_TIMEOUT_MS);
        let mut interval = tokio::time::interval(timeout);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                Some(sample) = rx.recv() => {
                    batch.push(sample);
                    if batch.len() < FLUSH_BATCH_SIZE {
                        continue;
                    }
                    Self::try_flush_or_clear(clickhouse.clone(), &mut batch).await;
                }
                _ = interval.tick() => {
                    if batch.is_empty() {
                        continue
                    }
                    Self::try_flush_or_clear(clickhouse.clone(), &mut batch).await;
                }
                else => {
                    if !batch.is_empty() {
                        Self::try_flush_or_clear(clickhouse.clone(), &mut batch).await;
                    }
                    break;
                }
            }
        }
    }

    async fn try_flush_or_clear(clickhouse: Arc<ClickHousePool>, batch: &mut Vec<LatencySample>) {
        if let Err(e) = Self::flush_batch(clickhouse.as_ref(), batch).await {
            tracing::warn!(error = %e, "latency_tracker: flush batch failed");
        }

        batch.clear()
    }

    async fn flush_batch(
        client: &clickhouse::Client,
        batch: &mut Vec<LatencySample>,
    ) -> Result<(), clickhouse::error::Error> {
        if batch.is_empty() {
            return Ok(());
        }
        let rows: Vec<ProviderLatencyRow> = batch
            .drain(..)
            .map(|s| {
                let ts = DateTime::from_timestamp_millis(s.ts_ms).unwrap_or_else(|| Utc::now());
                ProviderLatencyRow {
                    provider: s.prov.to_string(),
                    ts,
                    duration_ms: s.duration_ms,
                }
            })
            .collect();
        let mut insert = client
            .insert::<ProviderLatencyRow>("provider_latency_samples")
            .await?;

        for row in &rows {
            insert.write(row).await?;
        }
        insert.end().await?;
        Ok(())
    }

    /// Record a latency sample for a provider. Non-blocking: enqueues to the flusher.
    /// Accepts `Provider` or string (e.g. `"openai"`); unknown provider names are skipped.
    pub fn record(&self, p: impl std::fmt::Display, duration: Duration) {
        let Ok(provider) = Provider::from_str(&format!("{p}")) else {
            return;
        };
        let ts_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let sample = LatencySample {
            prov: provider,
            ts_ms,
            duration_ms: duration.as_millis() as u64,
        };
        if let Err(e) = self.write_tx.try_send(sample) {
            tracing::warn!(error = %e, "latency_tracker: record channel full, dropping sample");
        }
    }

    /// Refresh the cached percentile snapshot from ClickHouse.
    /// Called periodically by the background refresh task.
    pub async fn refresh_cached_percentiles(&self) {
        let window_secs = self.config.window_duration.as_secs();
        let query = format!(
            r#"
            SELECT
                provider,
                quantile(0.5)(duration_ms) AS p50,
                quantile(0.95)(duration_ms) AS p95,
                quantile(0.99)(duration_ms) AS p99,
                count() AS sample_count
            FROM reiver.provider_latency_samples
            WHERE ts >= now64(3) - INTERVAL {} SECOND
            GROUP BY provider
            "#,
            window_secs
        );
        let rows: Vec<ProviderPercentileRow> = match self.clickhouse.query(&query).fetch_all().await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "latency_tracker: refresh query failed");
                return;
            }
        };
        let mut providers = HashSet::new();
        for row in rows {
            let provider = match Provider::from_str(&row.provider) {
                Ok(p) => p,
                Err(_) => continue,
            };
            let latency = ProviderLatency {
                p50: Duration::from_millis(row.p50 as u64),
                p95: Duration::from_millis(row.p95 as u64),
                p99: Duration::from_millis(row.p99 as u64),
                sample_count: row.sample_count as usize,
            };
            self.cached_percentiles.insert(provider, latency);
            providers.insert(provider);
        }
        *self.cached_providers.write() = providers;
    }

    /// Get latency percentiles for a provider from the cached snapshot.
    pub fn get_latency(&self, p: Provider) -> Option<ProviderLatency> {
        self.cached_percentiles.get(&p).clone()
    }

    /// Get latency percentiles for multiple providers (by name) from the cached snapshot.
    /// Returns `(candidate_name, Option<ProviderLatency>)` for each candidate.
    pub fn get_latencies_batch(
        &self,
        candidates: &[String],
    ) -> Vec<(String, Option<ProviderLatency>)> {
        candidates
            .iter()
            .map(|s| {
                let lat = Provider::from_str(s).ok().and_then(|p| self.get_latency(p));
                (s.clone(), lat)
            })
            .collect()
    }

    /// Get the best (lowest P95) provider from a list of candidates.
    pub fn get_best_provider(&self, candidates: &[String]) -> Option<String> {
        let mut best: Option<(String, Duration)> = None;
        for candidate in candidates {
            let latency = Provider::from_str(candidate)
                .ok()
                .and_then(|p| self.cached_percentiles.get(&p));
            if let Some(latency) = latency {
                match &best {
                    None => best = Some((candidate.clone(), latency.p95)),
                    Some((_, best_p95)) => {
                        if latency.p95 < *best_p95 {
                            best = Some((candidate.clone(), latency.p95));
                        }
                    }
                }
            }
        }
        best.map(|(provider, _)| provider)
    }

    /// Sort provider slugs by ascending P95 latency. Providers without data
    /// are placed at the end in their original order.
    pub fn sort_by_p95(&self, candidates: &[String]) -> Vec<String> {
        let mut with_latency: Vec<(String, Option<Duration>)> = candidates
            .iter()
            .map(|c| {
                let lat = Provider::from_str(c)
                    .ok()
                    .and_then(|p| self.cached_percentiles.get(&p))
                    .map(|l| l.p95);
                (c.clone(), lat)
            })
            .collect();

        with_latency.sort_by(|a, b| match (&a.1, &b.1) {
            (Some(a_lat), Some(b_lat)) => a_lat.cmp(b_lat),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        });

        with_latency.into_iter().map(|(s, _)| s).collect()
    }

    /// Check if a provider is degraded (P99 exceeds threshold).
    pub fn is_degraded(&self, p: &Provider) -> bool {
        self.cached_percentiles
            .get(p)
            .map(|l| l.p99 > self.config.p99_fallback_threshold)
            .unwrap_or_default()
    }

    /// Get latency summaries for all providers (from cache, for API).
    pub fn get_all_summaries(&self) -> Vec<ProviderLatencySummary> {
        let providers: Vec<Provider> = self.cached_providers.read().iter().copied().collect();
        let mut summaries = Vec::with_capacity(providers.len());
        for provider in providers {
            if let Some(latency) = self.cached_percentiles.get(&provider) {
                summaries.push(ProviderLatencySummary {
                    provider: provider.as_str().to_string(),
                    p50_ms: latency.p50.as_secs_f64() * 1000.0,
                    p95_ms: latency.p95.as_secs_f64() * 1000.0,
                    p99_ms: latency.p99.as_secs_f64() * 1000.0,
                    sample_count: latency.sample_count,
                    is_degraded: latency.p99 > self.config.p99_fallback_threshold,
                });
            }
        }
        summaries
    }

    /// Get the tracker configuration.
    pub fn config(&self) -> &LatencyTrackerConfig {
        &self.config
    }

    /// Inject a cached percentile (for tests without ClickHouse).
    pub fn inject_for_test(&self, p: Provider, latency: ProviderLatency) {
        self.cached_percentiles.insert(p, latency);
        self.cached_providers.write().insert(p);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tracker_for_test() -> LatencyTracker {
        LatencyTracker::new(Arc::new(ClickHousePool::default()))
    }

    #[tokio::test]
    async fn test_record_does_not_block() {
        let tracker = tracker_for_test();
        tracker.record(Provider::OpenAi, Duration::from_millis(100));
        tracker.record(Provider::OpenAi, Duration::from_millis(200));
        tracker.inject_for_test(
            Provider::OpenAi,
            ProviderLatency {
                p50: Duration::from_millis(200),
                p95: Duration::from_millis(250),
                p99: Duration::from_millis(300),
                sample_count: 3,
            },
        );
        let latency = tracker.get_latency(Provider::OpenAi);
        assert!(latency.is_some());
        assert_eq!(latency.unwrap().sample_count, 3);
    }

    #[tokio::test]
    async fn test_unknown_provider_returns_none() {
        let tracker = tracker_for_test();
        assert!(tracker.get_latency(Provider::OpenAi).is_none());
    }

    #[tokio::test]
    async fn test_get_best_provider() {
        let tracker = tracker_for_test();
        tracker.inject_for_test(
            Provider::OpenAi,
            ProviderLatency {
                p50: Duration::from_millis(80),
                p95: Duration::from_millis(100),
                p99: Duration::from_millis(120),
                sample_count: 5,
            },
        );
        tracker.inject_for_test(
            Provider::Anthropic,
            ProviderLatency {
                p50: Duration::from_millis(400),
                p95: Duration::from_millis(500),
                p99: Duration::from_millis(600),
                sample_count: 5,
            },
        );
        let candidates = vec!["openai".to_string(), "anthropic".to_string()];
        let best = tracker.get_best_provider(&candidates);
        assert_eq!(best, Some("openai".to_string()));
    }

    #[tokio::test]
    async fn test_is_degraded() {
        let config = LatencyTrackerConfig {
            p99_fallback_threshold: Duration::from_secs(5),
            ..Default::default()
        };
        let tracker = LatencyTracker::with_config(Default::default(), config);
        tracker.inject_for_test(
            Provider::OpenAi,
            ProviderLatency {
                p50: Duration::from_millis(100),
                p95: Duration::from_millis(200),
                p99: Duration::from_millis(300),
                sample_count: 10,
            },
        );
        assert!(!tracker.is_degraded(&Provider::OpenAi));
        tracker.inject_for_test(
            Provider::Bedrock,
            ProviderLatency {
                p50: Duration::from_secs(5),
                p95: Duration::from_secs(8),
                p99: Duration::from_secs(10),
                sample_count: 10,
            },
        );
        assert!(tracker.is_degraded(&Provider::Bedrock));
    }

    #[tokio::test]
    async fn test_get_all_summaries() {
        let tracker = tracker_for_test();
        tracker.inject_for_test(
            Provider::OpenAi,
            ProviderLatency {
                p50: Duration::from_millis(100),
                p95: Duration::from_millis(150),
                p99: Duration::from_millis(200),
                sample_count: 10,
            },
        );
        tracker.inject_for_test(
            Provider::Anthropic,
            ProviderLatency {
                p50: Duration::from_millis(200),
                p95: Duration::from_millis(250),
                p99: Duration::from_millis(300),
                sample_count: 10,
            },
        );
        let summaries = tracker.get_all_summaries();
        assert_eq!(summaries.len(), 2);
    }

    #[test]
    fn test_percentile_index() {
        assert_eq!(percentile_index(1, 50), 0);
        assert_eq!(percentile_index(1, 99), 0);
        assert_eq!(percentile_index(100, 50), 49);
        assert_eq!(percentile_index(100, 95), 94);
        assert_eq!(percentile_index(100, 99), 98);
    }

    #[tokio::test]
    async fn test_get_latencies_batch() {
        let tracker = tracker_for_test();
        tracker.inject_for_test(
            Provider::OpenAi,
            ProviderLatency {
                p50: Duration::from_millis(100),
                p95: Duration::from_millis(150),
                p99: Duration::from_millis(200),
                sample_count: 5,
            },
        );
        tracker.inject_for_test(
            Provider::Anthropic,
            ProviderLatency {
                p50: Duration::from_millis(200),
                p95: Duration::from_millis(250),
                p99: Duration::from_millis(300),
                sample_count: 5,
            },
        );
        tracker.inject_for_test(
            Provider::Google,
            ProviderLatency {
                p50: Duration::from_millis(300),
                p95: Duration::from_millis(350),
                p99: Duration::from_millis(400),
                sample_count: 5,
            },
        );
        let candidates = vec![
            "openai".to_string(),
            "anthropic".to_string(),
            "google".to_string(),
        ];
        let batch = tracker.get_latencies_batch(&candidates);
        assert_eq!(batch.len(), 3);
        assert!(batch[0].1.is_some());
        assert!(batch[1].1.is_some());
        assert!(batch[2].1.is_some());
    }

    #[tokio::test]
    async fn test_get_latencies_batch_empty() {
        let tracker = tracker_for_test();
        let candidates = vec!["openai".to_string(), "anthropic".to_string()];
        let batch = tracker.get_latencies_batch(&candidates);
        assert_eq!(batch.len(), 2);
        assert!(batch[0].1.is_none());
        assert!(batch[1].1.is_none());
    }

    #[tokio::test]
    async fn test_summaries_reflect_degradation_flag() {
        let config = LatencyTrackerConfig {
            p99_fallback_threshold: Duration::from_secs(5),
            ..Default::default()
        };
        let tracker = LatencyTracker::with_config(Default::default(), config);
        tracker.inject_for_test(
            Provider::OpenAi,
            ProviderLatency {
                p50: Duration::from_millis(100),
                p95: Duration::from_millis(200),
                p99: Duration::from_millis(300),
                sample_count: 5,
            },
        );
        tracker.inject_for_test(
            Provider::Anthropic,
            ProviderLatency {
                p50: Duration::from_secs(10),
                p95: Duration::from_secs(20),
                p99: Duration::from_secs(30),
                sample_count: 5,
            },
        );
        let summaries = tracker.get_all_summaries();
        assert_eq!(summaries.len(), 2);
        let healthy = summaries.iter().find(|s| s.provider == "openai").unwrap();
        let degraded = summaries
            .iter()
            .find(|s| s.provider == "anthropic")
            .unwrap();
        assert!(!healthy.is_degraded);
        assert!(degraded.is_degraded);
    }

    // ====================================================================
    // sort_by_p95 tests
    // ====================================================================

    #[tokio::test]
    async fn test_sort_by_p95_orders_by_ascending_latency() {
        let tracker = tracker_for_test();
        tracker.inject_for_test(
            Provider::OpenAi,
            ProviderLatency {
                p50: Duration::from_millis(80),
                p95: Duration::from_millis(500),
                p99: Duration::from_millis(600),
                sample_count: 10,
            },
        );
        tracker.inject_for_test(
            Provider::Anthropic,
            ProviderLatency {
                p50: Duration::from_millis(50),
                p95: Duration::from_millis(100),
                p99: Duration::from_millis(200),
                sample_count: 10,
            },
        );
        tracker.inject_for_test(
            Provider::Google,
            ProviderLatency {
                p50: Duration::from_millis(60),
                p95: Duration::from_millis(300),
                p99: Duration::from_millis(400),
                sample_count: 10,
            },
        );
        let candidates = vec![
            "openai".to_string(),
            "anthropic".to_string(),
            "google".to_string(),
        ];
        let sorted = tracker.sort_by_p95(&candidates);
        assert_eq!(sorted, vec!["anthropic", "google", "openai"]);
    }

    #[tokio::test]
    async fn test_sort_by_p95_unknown_providers_go_last() {
        let tracker = tracker_for_test();
        tracker.inject_for_test(
            Provider::Google,
            ProviderLatency {
                p50: Duration::from_millis(50),
                p95: Duration::from_millis(100),
                p99: Duration::from_millis(200),
                sample_count: 5,
            },
        );
        let candidates = vec![
            "openai".to_string(),
            "anthropic".to_string(),
            "google".to_string(),
        ];
        let sorted = tracker.sort_by_p95(&candidates);
        assert_eq!(sorted[0], "google", "Google (with data) should be first");
        assert_eq!(
            &sorted[1..],
            &["openai", "anthropic"],
            "Providers without data should keep original order (stable sort)"
        );
    }

    #[tokio::test]
    async fn test_sort_by_p95_all_unknown_preserves_order() {
        let tracker = tracker_for_test();
        let candidates = vec![
            "anthropic".to_string(),
            "openai".to_string(),
            "google".to_string(),
        ];
        let sorted = tracker.sort_by_p95(&candidates);
        assert_eq!(
            sorted, candidates,
            "All unknown should preserve original order"
        );
    }

    #[tokio::test]
    async fn test_sort_by_p95_empty_input() {
        let tracker = tracker_for_test();
        let sorted = tracker.sort_by_p95(&[]);
        assert!(sorted.is_empty());
    }

    #[tokio::test]
    async fn test_sort_by_p95_single_element() {
        let tracker = tracker_for_test();
        tracker.inject_for_test(
            Provider::OpenAi,
            ProviderLatency {
                p50: Duration::from_millis(100),
                p95: Duration::from_millis(200),
                p99: Duration::from_millis(300),
                sample_count: 5,
            },
        );
        let sorted = tracker.sort_by_p95(&["openai".to_string()]);
        assert_eq!(sorted, vec!["openai"]);
    }

    #[tokio::test]
    async fn test_exact_threshold_boundary() {
        let config = LatencyTrackerConfig {
            p99_fallback_threshold: Duration::from_secs(5),
            ..Default::default()
        };
        let tracker = LatencyTracker::with_config(Default::default(), config.clone());
        tracker.inject_for_test(
            Provider::Google,
            ProviderLatency {
                p50: Duration::from_secs(2),
                p95: Duration::from_secs(4),
                p99: Duration::from_secs(5),
                sample_count: 10,
            },
        );
        assert!(
            !tracker.is_degraded(&Provider::Google),
            "P99 exactly at threshold should NOT be degraded"
        );
        let tracker2 = LatencyTracker::with_config(Default::default(), config);
        tracker2.inject_for_test(
            Provider::Theta,
            ProviderLatency {
                p50: Duration::from_millis(1000),
                p95: Duration::from_millis(4000),
                p99: Duration::from_millis(5001),
                sample_count: 10,
            },
        );
        assert!(
            tracker2.is_degraded(&Provider::Theta),
            "P99 just over threshold should be degraded"
        );
    }
}
