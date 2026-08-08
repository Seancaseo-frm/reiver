//! Per-provider circuit breaker for the LLM gateway.
//!
//! Tracks error rates in a sliding window and opens the circuit when a provider
//! starts failing consistently. Complements `LatencyTracker` which catches slow
//! providers but not fast-failing ones.
//!
//! State machine: Closed → Open → HalfOpen → Closed (or back to Open).

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::Mutex;
use serde::Serialize;

use crate::gateway::provider_types::Provider;

const DEFAULT_WINDOW_DURATION_SECS: u64 = 60;
const DEFAULT_ERROR_RATE_THRESHOLD: f64 = 0.5;
const DEFAULT_MIN_REQUESTS: usize = 5;
const DEFAULT_COOLDOWN_SECS: u64 = 30;

#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    pub window_duration: Duration,
    pub error_rate_threshold: f64,
    pub min_requests: usize,
    pub cooldown_duration: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            window_duration: Duration::from_secs(DEFAULT_WINDOW_DURATION_SECS),
            error_rate_threshold: DEFAULT_ERROR_RATE_THRESHOLD,
            min_requests: DEFAULT_MIN_REQUESTS,
            cooldown_duration: Duration::from_secs(DEFAULT_COOLDOWN_SECS),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CircuitState {
    Closed,
    Open { since: Instant },
    HalfOpen,
}

/// A single request outcome in the sliding window.
#[derive(Debug, Clone, Copy)]
struct Outcome {
    at: Instant,
    success: bool,
}

/// Per-provider circuit state and sliding window.
struct ProviderCircuit {
    state: CircuitState,
    window: VecDeque<Outcome>,
    /// Set to true when a half-open probe is in flight, preventing additional
    /// requests from slipping through.
    probe_in_flight: bool,
}

impl ProviderCircuit {
    fn new() -> Self {
        Self {
            state: CircuitState::Closed,
            window: VecDeque::new(),
            probe_in_flight: false,
        }
    }

    fn evict_expired(&mut self, window_duration: Duration) {
        let cutoff = Instant::now() - window_duration;
        while self.window.front().is_some_and(|o| o.at < cutoff) {
            self.window.pop_front();
        }
    }

    fn error_rate(&self) -> (usize, f64) {
        let total = self.window.len();
        if total == 0 {
            return (0, 0.0);
        }
        let failures = self.window.iter().filter(|o| !o.success).count();
        (total, failures as f64 / total as f64)
    }
}

/// Thread-safe per-provider circuit breaker.
pub struct CircuitBreaker {
    circuits: DashMap<Provider, Mutex<ProviderCircuit>>,
    config: CircuitBreakerConfig,
}

impl CircuitBreaker {
    pub fn new() -> Self {
        Self::with_config(CircuitBreakerConfig::default())
    }

    pub fn with_config(config: CircuitBreakerConfig) -> Self {
        Self {
            circuits: DashMap::new(),
            config,
        }
    }

    /// Returns `true` when the circuit is open and requests should skip this
    /// provider. Also returns `true` during half-open when a probe is already
    /// in flight (only one probe at a time).
    pub fn is_open(&self, provider: &Provider) -> bool {
        let Some(entry) = self.circuits.get(provider) else {
            return false;
        };
        let mut circuit = entry.lock();
        match &circuit.state {
            CircuitState::Closed => false,
            CircuitState::Open { since } => {
                if since.elapsed() >= self.config.cooldown_duration {
                    circuit.state = CircuitState::HalfOpen;
                    circuit.probe_in_flight = true;
                    false // allow exactly one probe request
                } else {
                    true
                }
            }
            CircuitState::HalfOpen => {
                if circuit.probe_in_flight {
                    true // block additional requests while probe runs
                } else {
                    circuit.probe_in_flight = true;
                    false // allow exactly one probe
                }
            }
        }
    }

    pub fn record_success(&self, provider: &Provider) {
        let entry = self
            .circuits
            .entry(*provider)
            .or_insert_with(|| Mutex::new(ProviderCircuit::new()));
        let mut circuit = entry.lock();
        circuit.window.push_back(Outcome {
            at: Instant::now(),
            success: true,
        });
        circuit.evict_expired(self.config.window_duration);

        if circuit.state == CircuitState::HalfOpen {
            circuit.state = CircuitState::Closed;
            circuit.probe_in_flight = false;
            circuit.window.clear();
            tracing::info!(provider = %provider, "Circuit breaker closed (probe succeeded)");
        }
    }

    pub fn record_failure(&self, provider: &Provider) {
        let entry = self
            .circuits
            .entry(*provider)
            .or_insert_with(|| Mutex::new(ProviderCircuit::new()));
        let mut circuit = entry.lock();
        circuit.window.push_back(Outcome {
            at: Instant::now(),
            success: false,
        });
        circuit.evict_expired(self.config.window_duration);

        match circuit.state {
            CircuitState::Closed => {
                let (total, rate) = circuit.error_rate();
                if total >= self.config.min_requests && rate >= self.config.error_rate_threshold {
                    circuit.state = CircuitState::Open {
                        since: Instant::now(),
                    };
                    circuit.probe_in_flight = false;
                    tracing::warn!(
                        provider = %provider,
                        error_rate = format!("{:.0}%", rate * 100.0),
                        total_requests = total,
                        "Circuit breaker opened"
                    );
                }
            }
            CircuitState::HalfOpen => {
                circuit.state = CircuitState::Open {
                    since: Instant::now(),
                };
                circuit.probe_in_flight = false;
                tracing::warn!(provider = %provider, "Circuit breaker reopened (probe failed)");
            }
            CircuitState::Open { .. } => {}
        }
    }

    /// Snapshot of all provider circuit states for observability.
    pub fn get_all_statuses(&self) -> Vec<ProviderCircuitStatus> {
        let mut statuses = Vec::new();
        for entry in self.circuits.iter() {
            let provider = *entry.key();
            let circuit = entry.value().lock();
            let (total, rate) = circuit.error_rate();
            let state_str = match &circuit.state {
                CircuitState::Closed => "closed",
                CircuitState::Open { .. } => "open",
                CircuitState::HalfOpen => "half_open",
            };
            statuses.push(ProviderCircuitStatus {
                provider: provider.as_str().to_string(),
                state: state_str.to_string(),
                error_rate: rate,
                request_count: total,
            });
        }
        statuses.sort_by(|a, b| a.provider.cmp(&b.provider));
        statuses
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ProviderCircuitStatus {
    pub provider: String,
    pub state: String,
    pub error_rate: f64,
    pub request_count: usize,
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn cb(min_requests: usize, threshold: f64, cooldown_ms: u64) -> CircuitBreaker {
        CircuitBreaker::with_config(CircuitBreakerConfig {
            window_duration: Duration::from_secs(60),
            error_rate_threshold: threshold,
            min_requests,
            cooldown_duration: Duration::from_millis(cooldown_ms),
        })
    }

    #[test]
    fn stays_closed_below_threshold() {
        let breaker = cb(3, 0.5, 5000);
        let p = Provider::OpenAi;
        breaker.record_success(&p);
        breaker.record_success(&p);
        breaker.record_failure(&p);
        assert!(!breaker.is_open(&p));
    }

    #[test]
    fn stays_closed_below_min_requests() {
        let breaker = cb(5, 0.5, 5000);
        let p = Provider::OpenAi;
        for _ in 0..4 {
            breaker.record_failure(&p);
        }
        assert!(!breaker.is_open(&p), "should not open with fewer than min_requests");
    }

    #[test]
    fn opens_when_threshold_exceeded() {
        let breaker = cb(3, 0.5, 5000);
        let p = Provider::OpenAi;
        breaker.record_failure(&p);
        breaker.record_failure(&p);
        breaker.record_failure(&p);
        assert!(breaker.is_open(&p));
    }

    #[test]
    fn transitions_to_half_open_after_cooldown() {
        let breaker = cb(3, 0.5, 100);
        let p = Provider::OpenAi;
        breaker.record_failure(&p);
        breaker.record_failure(&p);
        breaker.record_failure(&p);
        assert!(breaker.is_open(&p), "should be open before cooldown elapses");

        std::thread::sleep(Duration::from_millis(120));
        // After cooldown, is_open transitions to HalfOpen and allows one probe
        assert!(!breaker.is_open(&p), "should allow probe after cooldown");
        // Second call should block (probe already in flight)
        assert!(breaker.is_open(&p), "should block while probe in flight");
    }

    #[test]
    fn half_open_closes_on_success() {
        let breaker = cb(3, 0.5, 100);
        let p = Provider::OpenAi;
        breaker.record_failure(&p);
        breaker.record_failure(&p);
        breaker.record_failure(&p);

        std::thread::sleep(Duration::from_millis(120));
        assert!(!breaker.is_open(&p)); // transitions to half-open, allows probe

        breaker.record_success(&p);
        assert!(!breaker.is_open(&p), "should be closed after successful probe");
    }

    #[test]
    fn half_open_reopens_on_failure() {
        let breaker = cb(3, 0.5, 100);
        let p = Provider::OpenAi;
        breaker.record_failure(&p);
        breaker.record_failure(&p);
        breaker.record_failure(&p);

        std::thread::sleep(Duration::from_millis(120));
        assert!(!breaker.is_open(&p)); // allows probe

        breaker.record_failure(&p);
        assert!(breaker.is_open(&p), "should reopen after failed probe");
    }

    #[test]
    fn unknown_provider_is_not_open() {
        let breaker = CircuitBreaker::new();
        assert!(!breaker.is_open(&Provider::Anthropic));
    }

    #[test]
    fn independent_providers() {
        let breaker = cb(2, 0.5, 5000);
        let a = Provider::OpenAi;
        let b = Provider::Anthropic;
        breaker.record_failure(&a);
        breaker.record_failure(&a);
        assert!(breaker.is_open(&a));
        assert!(!breaker.is_open(&b));
    }

    #[test]
    fn get_all_statuses_returns_all_providers() {
        let breaker = cb(2, 0.5, 5000);
        breaker.record_success(&Provider::OpenAi);
        breaker.record_failure(&Provider::Anthropic);
        breaker.record_failure(&Provider::Anthropic);
        let statuses = breaker.get_all_statuses();
        assert_eq!(statuses.len(), 2);
        let open = statuses.iter().find(|s| s.provider == "anthropic").unwrap();
        assert_eq!(open.state, "open");
        let closed = statuses.iter().find(|s| s.provider == "openai").unwrap();
        assert_eq!(closed.state, "closed");
    }
}
