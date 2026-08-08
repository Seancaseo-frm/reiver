//! Fallback and retry logic for the AI Gateway.
//!
//! Provides automatic failover when providers fail or are rate-limited,
//! and configurable retry strategies with exponential backoff.

use std::time::Duration;

use crate::config::Config;
use crate::gateway::error::GatewayError;
use crate::gateway::provider_types::Provider;

/// Initial delay for the first retry attempt (milliseconds)
const INITIAL_RETRY_DELAY_MS: u64 = 500;

/// Configuration for retry behavior.
///
/// Fallback models are **not** configured here — they come from the request's
/// `models` array or the project-level `default_fallback_models` setting.
/// This keeps fallback decisions with the user, not hardcoded in the server.
#[derive(Debug, Clone)]
pub struct FallbackConfig {
    /// Maximum retries per provider before falling back.
    pub max_retries: u32,
    /// Initial delay between retries (doubles with each attempt).
    pub initial_retry_delay: Duration,
    /// Maximum delay between retries.
    pub max_retry_delay: Duration,
    /// Whether to enable automatic fallback to alternate providers.
    pub enable_fallback: bool,
}

impl Default for FallbackConfig {
    fn default() -> Self {
        Self {
            max_retries: 2,
            initial_retry_delay: Duration::from_millis(INITIAL_RETRY_DELAY_MS),
            max_retry_delay: Duration::from_secs(10),
            enable_fallback: true,
        }
    }
}

impl FallbackConfig {
    /// Create a new fallback config with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a fallback config from application configuration.
    pub fn from_config(config: &Config) -> Self {
        Self {
            max_retries: config.gateway_max_retries,
            initial_retry_delay: Duration::from_millis(config.gateway_initial_retry_delay_ms),
            max_retry_delay: Duration::from_millis(config.gateway_max_retry_delay_ms),
            enable_fallback: config.gateway_fallback_enabled,
        }
    }

    /// Disable automatic fallback.
    #[must_use]
    pub fn without_fallback(mut self) -> Self {
        self.enable_fallback = false;
        self
    }

    /// Set maximum retries per provider.
    #[must_use]
    pub fn with_max_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }
}

/// Determines if an error is retryable.
pub fn is_retryable_error(error: &GatewayError) -> bool {
    match error {
        // Rate limits are retryable
        GatewayError::RateLimitExceeded { .. } => true,

        // 429 and 5xx are retryable against the same provider (transient).
        // Other 4xx are NOT retryable (same key/request won't fix them)
        // but ARE fallback-eligible via should_fallback().
        GatewayError::ProviderError { status, .. } => {
            *status == 429 || (*status >= 500 && *status < 600)
        }

        // Timeouts are retryable
        GatewayError::Timeout(_) => true,

        // Network errors are retryable
        GatewayError::NetworkError(_) => true,

        // Other errors are not retryable
        _ => false,
    }
}

/// Determines if an error should trigger fallback to another provider.
pub fn should_fallback(error: &GatewayError) -> bool {
    match error {
        // Rate limits should trigger fallback
        GatewayError::RateLimitExceeded { .. } => true,

        // Timeouts should trigger fallback
        GatewayError::Timeout(_) => true,

        // Provider unavailable should trigger fallback
        GatewayError::NetworkError(_) => true,

        // Auth errors should NOT trigger fallback (likely config issue)
        GatewayError::AuthenticationFailed(_) => false,
        GatewayError::MissingProviderKey(_) => false,

        // All 4xx except 400 trigger fallback, plus 5xx.
        // 400 = bad request (broken payload, won't succeed elsewhere).
        // Other 4xx = provider-side issues worth trying another provider.
        GatewayError::ProviderError { status, .. } => {
            (*status >= 401 && *status <= 499) || (*status >= 500 && *status < 600)
        }

        // Other errors should not trigger fallback
        _ => false,
    }
}

/// Minimum retry delay to prevent thundering herd when the computed
/// backoff rounds to zero (e.g. `initial_retry_delay = 0`).
const MIN_RETRY_DELAY_MS: u64 = 50;

/// Calculate retry delay with exponential backoff and equal jitter.
///
/// Uses the "equal jitter" strategy: `delay/2 + rand(0..delay/2)`.
/// This spreads concurrent retries across time to avoid thundering herd,
/// while keeping delays within a predictable range.
///
/// A floor of [`MIN_RETRY_DELAY_MS`] is enforced so that a zero
/// `initial_retry_delay` does not cause immediate, deterministic retries.
pub fn calculate_retry_delay(attempt: u32, config: &FallbackConfig) -> Duration {
    use rand::Rng;

    let base = config.initial_retry_delay.as_millis() as u64;
    let multiplier = 2u64.saturating_pow(attempt);
    let full_delay = base.saturating_mul(multiplier);
    let max_delay = config.max_retry_delay.as_millis() as u64;
    let capped = full_delay.min(max_delay);

    let half = capped / 2;
    let jitter = if half > 0 {
        rand::thread_rng().gen_range(0..=half)
    } else {
        0
    };
    Duration::from_millis((half + jitter).max(MIN_RETRY_DELAY_MS))
}

/// Result of a fallback attempt.
#[derive(Debug, Clone)]
pub struct FallbackResult<T> {
    /// The successful result (if any).
    pub result: T,
    /// The model ID string that was actually used.
    pub model_used: String,
    /// Whether a fallback was used.
    pub fallback_used: bool,
    /// Number of retries attempted.
    pub retry_count: u32,
    /// The provider that served the request.
    pub provider_used: Provider,
}

impl<T> FallbackResult<T> {
    /// Create a new fallback result for a successful primary request.
    pub fn primary(result: T, model_id: String, provider: Provider, retries: u32) -> Self {
        Self {
            result,
            model_used: model_id,
            fallback_used: false,
            retry_count: retries,
            provider_used: provider,
        }
    }

    /// Create a new fallback result for a successful fallback request.
    pub fn fallback(result: T, model_id: String, provider: Provider, retries: u32) -> Self {
        Self {
            result,
            model_used: model_id,
            fallback_used: true,
            retry_count: retries,
            provider_used: provider,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = FallbackConfig::default();
        assert_eq!(config.max_retries, 2);
        assert!(config.enable_fallback);
    }

    #[test]
    fn test_is_retryable_error() {
        assert!(is_retryable_error(&GatewayError::RateLimitExceeded {
            limit: 100,
            reset_seconds: 60,
        }));

        assert!(is_retryable_error(&GatewayError::ProviderError {
            provider: Provider::OpenAi,
            status: 500,
            message: "Internal error".to_string(),
        }));

        assert!(!is_retryable_error(&GatewayError::ProviderError {
            provider: Provider::OpenAi,
            status: 400,
            message: "Bad request".to_string(),
        }));

        assert!(!is_retryable_error(&GatewayError::AuthenticationFailed(
            "Invalid key".to_string()
        )));
    }

    #[test]
    fn test_calculate_retry_delay() {
        let config = FallbackConfig::default();

        let delay = calculate_retry_delay(0, &config);
        assert!(delay >= Duration::from_millis(250) && delay <= Duration::from_millis(500));

        let delay = calculate_retry_delay(1, &config);
        assert!(delay >= Duration::from_millis(500) && delay <= Duration::from_millis(1000));

        let delay = calculate_retry_delay(2, &config);
        assert!(delay >= Duration::from_millis(1000) && delay <= Duration::from_millis(2000));

        let delay = calculate_retry_delay(10, &config);
        assert!(delay >= Duration::from_millis(5000) && delay <= Duration::from_secs(10));
    }

    #[test]
    fn test_should_fallback() {
        assert!(should_fallback(&GatewayError::RateLimitExceeded {
            limit: 100,
            reset_seconds: 60,
        }));

        assert!(!should_fallback(&GatewayError::AuthenticationFailed(
            "Invalid".to_string()
        )));

        assert!(!should_fallback(&GatewayError::MissingProviderKey(
            "openai".to_string()
        )));
    }

    #[test]
    fn test_fallback_config_without_fallback() {
        let config = FallbackConfig::default().without_fallback();
        assert!(!config.enable_fallback);
    }

    // ==================== Backoff Delay Bounds ====================

    #[test]
    fn test_backoff_delay_attempt_zero() {
        let config = FallbackConfig::default();
        let delay = calculate_retry_delay(0, &config);
        let half = config.initial_retry_delay / 2;
        assert!(delay >= half && delay <= config.initial_retry_delay);
    }

    #[test]
    fn test_backoff_delay_never_exceeds_max() {
        let config = FallbackConfig::default();
        for attempt in 0..20 {
            let delay = calculate_retry_delay(attempt, &config);
            assert!(
                delay <= config.max_retry_delay,
                "Attempt {} produced delay {:?} exceeding max {:?}",
                attempt,
                delay,
                config.max_retry_delay
            );
        }
    }

    #[test]
    fn test_backoff_delay_increases_exponentially() {
        let config = FallbackConfig::default();
        let base = config.initial_retry_delay.as_millis() as u64;
        let min0 = base / 2;
        let min1 = base;
        let min2 = base * 2;
        assert!(min1 > min0);
        assert!(min2 > min1);
        let d0 = calculate_retry_delay(0, &config);
        let d2 = calculate_retry_delay(2, &config);
        assert!(d2 >= d0, "Higher attempt must produce >= delay on average");
    }

    #[test]
    fn test_calculate_retry_delay_has_jitter() {
        let config = FallbackConfig::default();
        let mut seen = std::collections::HashSet::new();
        for _ in 0..50 {
            seen.insert(calculate_retry_delay(2, &config).as_millis());
        }
        assert!(
            seen.len() > 1,
            "50 calls to calculate_retry_delay should produce more than one distinct value, got {:?}",
            seen
        );
    }

    // ==================== FallbackResult Metadata ====================

    #[test]
    fn test_fallback_result_primary() {
        let result = FallbackResult::primary("ok", "gpt-4o".to_string(), Provider::OpenAi, 0);
        assert!(!result.fallback_used);
        assert_eq!(result.retry_count, 0);
        assert_eq!(result.model_used, "gpt-4o");
        assert_eq!(result.provider_used, Provider::OpenAi);
        assert_eq!(result.result, "ok");
    }

    #[test]
    fn test_fallback_result_primary_with_retries() {
        let result = FallbackResult::primary("ok", "gpt-4o".to_string(), Provider::OpenAi, 2);
        assert!(!result.fallback_used);
        assert_eq!(
            result.retry_count, 2,
            "primary() must propagate the retry count, not hardcode 0"
        );
    }

    #[test]
    fn test_fallback_result_fallback() {
        let result = FallbackResult::fallback(
            "ok",
            "claude-sonnet-4-6-20250514".to_string(),
            Provider::Anthropic,
            3,
        );
        assert!(result.fallback_used);
        assert_eq!(result.retry_count, 3);
        assert_eq!(result.model_used, "claude-sonnet-4-6-20250514");
        assert_eq!(result.provider_used, Provider::Anthropic);
    }

    #[test]
    fn test_fallback_result_provider_matches_construction() {
        let result = FallbackResult::primary(
            "ok",
            "gemini-2.5-flash".to_string(),
            Provider::Google,
            0,
        );
        assert_eq!(result.provider_used, Provider::Google);
        assert_eq!(result.model_used, "gemini-2.5-flash");
    }

    // ==================== Retryable / Fallback Edge Cases ====================

    #[test]
    fn test_timeout_is_retryable_and_triggers_fallback() {
        let err = GatewayError::Timeout("Request timed out".into());
        assert!(is_retryable_error(&err));
        assert!(should_fallback(&err));
    }

    #[test]
    fn test_network_error_is_retryable_and_triggers_fallback() {
        let err = GatewayError::NetworkError("Connection refused".into());
        assert!(is_retryable_error(&err));
        assert!(should_fallback(&err));
    }

    #[test]
    fn test_provider_400_not_retryable_no_fallback() {
        let err = GatewayError::ProviderError {
            provider: Provider::OpenAi,
            status: 400,
            message: "Bad request".to_string(),
        };
        assert!(!is_retryable_error(&err));
        assert!(!should_fallback(&err));
    }

    #[test]
    fn test_provider_401_not_retryable_but_fallback() {
        let err = GatewayError::ProviderError {
            provider: Provider::OpenAi,
            status: 401,
            message: "Unauthorized".to_string(),
        };
        assert!(!is_retryable_error(&err), "401 should not retry same provider");
        assert!(should_fallback(&err), "401 should fallback to next provider");
    }

    #[test]
    fn test_provider_402_not_retryable_but_fallback() {
        let err = GatewayError::ProviderError {
            provider: Provider::DeepSeek,
            status: 402,
            message: "Insufficient balance".to_string(),
        };
        assert!(!is_retryable_error(&err), "402 should not retry same provider");
        assert!(should_fallback(&err), "402 should fallback to next provider");
    }

    #[test]
    fn test_provider_403_not_retryable_but_fallback() {
        let err = GatewayError::ProviderError {
            provider: Provider::OpenAi,
            status: 403,
            message: "Forbidden".to_string(),
        };
        assert!(!is_retryable_error(&err), "403 should not retry same provider");
        assert!(should_fallback(&err), "403 should fallback to next provider");
    }

    #[test]
    fn test_provider_404_not_retryable_but_fallback() {
        let err = GatewayError::ProviderError {
            provider: Provider::OpenAi,
            status: 404,
            message: "Model not found".to_string(),
        };
        assert!(!is_retryable_error(&err), "404 should not retry same provider");
        assert!(should_fallback(&err), "404 should fallback to next provider");
    }

    #[test]
    fn test_provider_409_not_retryable_but_fallback() {
        let err = GatewayError::ProviderError {
            provider: Provider::Anthropic,
            status: 409,
            message: "Conflict".to_string(),
        };
        assert!(!is_retryable_error(&err), "409 should not retry same provider");
        assert!(should_fallback(&err), "409 should fallback to next provider");
    }

    #[test]
    fn test_provider_422_not_retryable_but_fallback() {
        let err = GatewayError::ProviderError {
            provider: Provider::OpenAi,
            status: 422,
            message: "Unprocessable entity".to_string(),
        };
        assert!(!is_retryable_error(&err), "422 should not retry same provider");
        assert!(should_fallback(&err), "422 should fallback to next provider");
    }

    #[test]
    fn test_provider_429_is_retryable_and_triggers_fallback() {
        let err = GatewayError::ProviderError {
            provider: Provider::OpenAi,
            status: 429,
            message: "Rate limit exceeded".to_string(),
        };
        assert!(
            is_retryable_error(&err),
            "ProviderError with status 429 must be retryable"
        );
        assert!(
            should_fallback(&err),
            "ProviderError with status 429 must trigger fallback"
        );
    }

    #[test]
    fn test_provider_503_retryable_and_fallback() {
        let err = GatewayError::ProviderError {
            provider: Provider::Anthropic,
            status: 503,
            message: "Service unavailable".to_string(),
        };
        assert!(is_retryable_error(&err));
        assert!(should_fallback(&err));
    }

    #[test]
    fn test_calculate_retry_delay_no_overflow_on_large_attempt() {
        let config = FallbackConfig::default();
        let max = config.max_retry_delay;
        let half_max = max / 2;

        let delay = calculate_retry_delay(64, &config);
        assert!(
            delay >= half_max && delay <= max,
            "Attempt 64 should be in [max/2, max], not panic. Got {:?}",
            delay
        );

        let delay = calculate_retry_delay(u32::MAX, &config);
        assert!(
            delay >= half_max && delay <= max,
            "u32::MAX attempt should be in [max/2, max], not panic. Got {:?}",
            delay
        );
    }

    #[test]
    fn test_calculate_retry_delay_floor_prevents_zero_delay() {
        let config = FallbackConfig {
            initial_retry_delay: Duration::from_millis(0),
            max_retry_delay: Duration::from_millis(0),
            ..FallbackConfig::default()
        };

        for attempt in 0..5 {
            let delay = calculate_retry_delay(attempt, &config);
            assert!(
                delay >= Duration::from_millis(MIN_RETRY_DELAY_MS),
                "Attempt {} with zero config must be >= {}ms floor, got {:?}",
                attempt,
                MIN_RETRY_DELAY_MS,
                delay
            );
        }
    }

    #[test]
    fn test_calculate_retry_delay_floor_does_not_affect_normal_delays() {
        let config = FallbackConfig::default();
        let delay = calculate_retry_delay(0, &config);
        assert!(
            delay >= Duration::from_millis(250),
            "Normal delay must remain unaffected by floor, got {:?}",
            delay
        );
    }
}
