//! Shared utility functions for billing operations.

use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;
use uuid::Uuid;

use super::provider::PaymentError;

// ============================================================================
// Retry with Exponential Backoff
// ============================================================================

/// Configuration for retry behavior.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (not including the initial attempt)
    pub max_retries: u32,
    /// Initial delay before first retry
    pub initial_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Multiplier for exponential backoff (e.g., 2.0 doubles delay each retry)
    pub multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            multiplier: 2.0,
        }
    }
}

/// Determines if an error is transient and should be retried.
pub fn is_retriable_error(err: &PaymentError) -> bool {
    match err {
        PaymentError::RateLimited => true,
        PaymentError::ProviderError(msg) => is_transient_message(msg),
        _ => false,
    }
}

/// Check if an error message indicates a transient failure.
fn is_transient_message(msg: &str) -> bool {
    let msg_lower = msg.to_lowercase();
    msg_lower.contains("timeout")
        || msg_lower.contains("connection")
        || msg_lower.contains("temporarily unavailable")
        || msg_lower.contains("service unavailable")
        || msg_lower.contains("too many requests")
        || msg_lower.contains("network")
}

/// Execute an async operation with exponential backoff retry.
///
/// # Arguments
/// * `config` - Retry configuration
/// * `operation_name` - Name for logging purposes
/// * `operation` - The async operation to execute
///
/// # Returns
/// The result of the operation, or the last error if all retries failed.
///
/// # Example
/// ```ignore
/// let result = retry_with_backoff(
///     RetryConfig::default(),
///     "create_customer",
///     || async { stripe_client.create_customer(...).await }
/// ).await;
/// ```
pub async fn retry_with_backoff<F, Fut, T>(
    config: RetryConfig,
    operation_name: &str,
    mut operation: F,
) -> Result<T, PaymentError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, PaymentError>>,
{
    let mut attempt = 0;
    let mut delay = config.initial_delay;

    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(err) => {
                // Check if we should retry
                if attempt >= config.max_retries || !is_retriable_error(&err) {
                    return Err(err);
                }

                attempt += 1;
                warn!(
                    operation = %operation_name,
                    attempt = attempt,
                    max_retries = config.max_retries,
                    delay_ms = delay.as_millis(),
                    error = %err,
                    "Retrying after transient error"
                );

                // Wait before retrying
                sleep(delay).await;

                // Calculate next delay with exponential backoff
                delay =
                    Duration::from_millis((delay.as_millis() as f64 * config.multiplier) as u64)
                        .min(config.max_delay);
            }
        }
    }
}

// ============================================================================
// Safe UUID Formatting for ClickHouse
// ============================================================================

/// Safely format a UUID for use in ClickHouse SQL queries.
///
/// UUIDs are inherently safe from SQL injection as they can only contain
/// hexadecimal characters (0-9, a-f) and dashes. This function uses the
/// hyphenated format which ClickHouse accepts for UUID comparisons.
///
/// # Safety
/// This is safe because:
/// 1. Rust's `Uuid` type guarantees valid UUID format
/// 2. UUIDs only contain hex chars and dashes - no quotes, semicolons, or SQL operators
/// 3. We use single quotes which ClickHouse requires for string/UUID literals
#[inline]
pub fn format_uuid_for_clickhouse(id: &Uuid) -> String {
    // UUID::to_string() returns hyphenated lowercase format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    // This format only contains [0-9a-f-] characters, making it SQL-injection safe
    format!("'{}'", id.as_hyphenated())
}

/// Build a safe IN clause for multiple UUIDs.
///
/// Returns `Some(clause)` with a comma-separated list of quoted UUIDs,
/// or `None` if the input slice is empty.
///
/// This returns `Option` to force callers to handle the empty case explicitly,
/// preventing SQL syntax errors like `WHERE id IN ()`.
///
/// # Example
/// ```ignore
/// let ids = vec![uuid1, uuid2];
/// if let Some(clause) = build_uuid_in_clause(&ids) {
///     let query = format!("SELECT * FROM t WHERE id IN ({})", clause);
///     // ...
/// } else {
///     // Handle empty case - return early, use different query, etc.
/// }
/// ```
pub fn build_uuid_in_clause(ids: &[Uuid]) -> Option<String> {
    if ids.is_empty() {
        return None;
    }
    Some(
        ids.iter()
            .map(format_uuid_for_clickhouse)
            .collect::<Vec<_>>()
            .join(", "),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_uuid_for_clickhouse() {
        let id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let formatted = format_uuid_for_clickhouse(&id);
        assert_eq!(formatted, "'550e8400-e29b-41d4-a716-446655440000'");
    }

    #[test]
    fn test_build_uuid_in_clause() {
        let id1 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let id2 = Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap();
        let clause = build_uuid_in_clause(&[id1, id2]);
        assert_eq!(
            clause,
            Some(
                "'550e8400-e29b-41d4-a716-446655440000', '6ba7b810-9dad-11d1-80b4-00c04fd430c8'"
                    .to_string()
            )
        );
    }

    #[test]
    fn test_empty_uuid_clause_returns_none() {
        let clause = build_uuid_in_clause(&[]);
        assert_eq!(clause, None);
    }

    #[test]
    fn test_single_uuid_clause() {
        let id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let clause = build_uuid_in_clause(&[id]);
        assert_eq!(
            clause,
            Some("'550e8400-e29b-41d4-a716-446655440000'".to_string())
        );
    }
}
