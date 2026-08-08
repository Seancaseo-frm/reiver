//! Background task for refreshing the latency percentile cache from ClickHouse.
//!
//! Runs every `cache_refresh_interval` (default 1 minute), queries ClickHouse
//! for the last window, and updates the in-memory cache used by hot-path reads.

use std::sync::Arc;

use tracing::Instrument;

use super::circuit_breaker::CircuitBreaker;
use super::latency_tracker::LatencyTracker;

/// Spawn the background cache refresh task.
///
/// This task runs every `cache_refresh_interval` and refreshes the cached
/// percentile snapshot from ClickHouse. Also logs degraded providers and open
/// circuit breakers on each tick.
pub fn spawn_latency_cache_refresh_task(
    latency_tracker: Arc<LatencyTracker>,
    circuit_breaker: Arc<CircuitBreaker>,
) {
    let interval = latency_tracker.config().cache_refresh_interval;

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticker.tick().await; // skip first immediate tick

        loop {
            ticker.tick().await;
            latency_tracker
                .refresh_cached_percentiles()
                .instrument(tracing::info_span!("gateway.latency.refresh"))
                .await;

            for summary in latency_tracker.get_all_summaries() {
                if summary.is_degraded {
                    tracing::warn!(
                        provider = %summary.provider,
                        p99_ms = %summary.p99_ms,
                        p95_ms = %summary.p95_ms,
                        sample_count = %summary.sample_count,
                        "Provider is degraded: P99 latency exceeds threshold"
                    );
                }
            }

            for status in circuit_breaker.get_all_statuses() {
                if status.state != "closed" {
                    tracing::warn!(
                        provider = %status.provider,
                        state = %status.state,
                        error_rate = format!("{:.0}%", status.error_rate * 100.0),
                        request_count = %status.request_count,
                        "Circuit breaker is not closed"
                    );
                }
            }
        }
    });
}
