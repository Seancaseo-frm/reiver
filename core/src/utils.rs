//! Common utility functions
//!
//! Shared utilities used across the codebase.

use crate::app_state::RedisPool;
use crate::db::DbPool;
use crate::error::{AppError, Result};
use bb8_redis::redis::AsyncCommands;
use rand::Rng;
use sha2::{Digest, Sha256};
use std::time::Duration;
use tracing::{debug, error};
use uuid::Uuid;

/// Compute the SHA-256 hex digest of an API key for hash-based lookups.
pub fn hash_api_key(key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Cache TTL for project_key -> project_id mappings (5 minutes)
const PROJECT_KEY_CACHE_TTL_SECS: u64 = 300;

/// Maximum random delay in microseconds to add for timing attack mitigation.
/// This adds up to 5ms of jitter to mask timing differences.
const MAX_TIMING_JITTER_MICROS: u64 = 5000;

/// Add a random timing jitter to mitigate timing attacks.
///
/// # Security
/// Timing attacks can reveal information about whether an API key exists
/// by measuring response times. This function adds random delay to mask
/// timing differences between cache hits, cache misses, and invalid keys.
///
/// The jitter is small enough to not significantly impact performance
/// but large enough to make timing attacks impractical.
async fn add_timing_jitter() {
    let jitter_micros = rand::thread_rng().gen_range(0..MAX_TIMING_JITTER_MICROS);
    tokio::time::sleep(Duration::from_micros(jitter_micros)).await;
}

/// Validate a project_key and return the associated project_id
/// Uses Redis caching to reduce database round-trips for high-volume ingestion endpoints
///
/// # Arguments
/// * `redis` - Redis connection pool for caching
/// * `db` - PostgreSQL connection pool for lookups
/// * `project_key` - The project API key to validate
///
/// # Returns
/// * `Ok(Uuid)` - The project_id if the key is valid
/// * `Err(AppError::Auth)` - If the key is invalid
///
/// # Performance
/// - First check: Redis cache (O(1))
/// - Cache miss: PostgreSQL lookup, then cache result for 5 minutes
///
/// # Security
/// Includes random timing jitter to mitigate timing attacks that could
/// reveal whether a key exists in cache or is valid/invalid.
pub async fn validate_project_key_cached(
    redis: &RedisPool,
    db: &DbPool,
    project_key: &str,
) -> Result<Uuid> {
    validate_project_key_cached_inner(redis, db, project_key, None).await
}

/// Validate a project key of a specific stored type and return its project.
///
/// Typed validation uses a separate cache namespace so a generic key lookup
/// cannot make an agent token valid at an SDK-only application endpoint.
pub async fn validate_project_key_type_cached(
    redis: &RedisPool,
    db: &DbPool,
    project_key: &str,
    expected_key_type: &str,
) -> Result<Uuid> {
    validate_project_key_cached_inner(redis, db, project_key, Some(expected_key_type)).await
}

async fn validate_project_key_cached_inner(
    redis: &RedisPool,
    db: &DbPool,
    project_key: &str,
    expected_key_type: Option<&str>,
) -> Result<Uuid> {
    let key_hash = hash_api_key(project_key);
    let cache_key = match expected_key_type {
        Some(key_type) => format!("project_key:{}:{}", key_type, key_hash),
        None => format!("project_key:{}", key_hash),
    };

    // Try cache first
    let mut conn = redis.get().await.map_err(|e| {
        error!(
            "Failed to get Redis connection for project_key cache: {}",
            e
        );
        AppError::Internal(anyhow::anyhow!("Cache connection error"))
    })?;

    // Check cache
    let cached: Option<String> = tokio::time::timeout(Duration::from_secs(1), conn.get(&cache_key))
        .await
        .ok()
        .and_then(|r| r.ok())
        .flatten();

    if let Some(project_id_str) = cached {
        if project_id_str == "INVALID" {
            debug!("Project key cache hit (invalid): {}", &key_hash[..8]);
            add_timing_jitter().await;
            return Err(AppError::Auth("Invalid project key".to_string()));
        }

        if let Ok(project_id) = Uuid::parse_str(&project_id_str) {
            debug!("Project key cache hit: {}", &key_hash[..8]);
            add_timing_jitter().await;
            return Ok(project_id);
        }
    }

    // Cache miss - query database by hash
    debug!("Project key cache miss, querying database");

    let project_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT project_id FROM project_keys
         WHERE key_hash = $1
           AND ($2::text IS NULL OR key_type = $2)
           AND (expires_at IS NULL OR expires_at > NOW())",
    )
    .bind(&key_hash)
    .bind(expected_key_type)
    .fetch_optional(db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    match project_id {
        Some(pid) => {
            let _ = tokio::time::timeout(
                Duration::from_secs(1),
                conn.set_ex::<_, _, ()>(&cache_key, pid.to_string(), PROJECT_KEY_CACHE_TTL_SECS),
            )
            .await;

            debug!("Project key validated and cached: {}", &key_hash[..8]);
            add_timing_jitter().await;
            Ok(pid)
        }
        None => {
            let _ = tokio::time::timeout(
                Duration::from_secs(1),
                conn.set_ex::<_, _, ()>(&cache_key, "INVALID", 60),
            )
            .await;

            error!("Invalid project key: {}", &key_hash[..8]);
            add_timing_jitter().await;
            Err(AppError::Auth("Invalid project key".to_string()))
        }
    }
}

/// Invalidate the cache for a project key by its hash (call when keys are deleted/rotated)
pub async fn invalidate_project_key_cache(redis: &RedisPool, key_hash: &str) -> Result<()> {
    let mut conn = redis
        .get()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Redis error: {}", e)))?;

    for cache_key in [
        format!("project_key:{}", key_hash),
        format!("project_key:sdk:{}", key_hash),
        format!("project_key:agent:{}", key_hash),
    ] {
        let _: () = conn
            .del(&cache_key)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Redis error: {}", e)))?;
    }

    Ok(())
}

/// Escape a string value for safe use in ClickHouse queries.
///
/// This prevents SQL injection by escaping:
/// - Backslashes (`\` -> `\\`)
/// - Single quotes (`'` -> `\'`)
/// - NULL bytes (`\0` -> removed)
/// - Newlines (`\n` -> `\\n`, `\r` -> `\\r`)
/// - Tabs (`\t` -> `\\t`)
///
/// # Security
/// This function is critical for preventing SQL injection in ClickHouse queries
/// that use string interpolation. Always use this for user-provided input.
///
/// # Example
/// ```
/// use reiver_core::utils::escape_clickhouse_string;
///
/// let safe = escape_clickhouse_string("O'Reilly\\Media");
/// assert_eq!(safe, "O\\'Reilly\\\\Media");
/// ```
pub fn escape_clickhouse_string(s: &str) -> String {
    let mut result = String::with_capacity(s.len() * 2);

    for c in s.chars() {
        match c {
            '\\' => result.push_str("\\\\"),
            '\'' => result.push_str("\\'"),
            '\0' => {} // Remove NULL bytes entirely
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            _ => result.push(c),
        }
    }

    result
}

/// Calculate the percentage change from an old value to a new value.
///
/// Returns `(new_value - old_value) / old_value * 100.0`.
/// Returns `0.0` when `old_value` is zero to avoid division by zero.
///
/// # Example
/// ```
/// use reiver_core::utils::percentage_change;
///
/// assert_eq!(percentage_change(120.0, 100.0), 20.0);  // 20% increase
/// assert_eq!(percentage_change(80.0, 100.0), -20.0);  // 20% decrease
/// assert_eq!(percentage_change(50.0, 0.0), 0.0);      // zero baseline
/// ```
pub fn percentage_change(new_value: f64, old_value: f64) -> f64 {
    if old_value == 0.0 {
        return 0.0;
    }
    (new_value - old_value) / old_value * 100.0
}

/// Parse a string to Decimal, returning Decimal::ZERO on failure.
///
/// This is a convenience function for parsing cost values from ClickHouse
/// which returns them as strings. Invalid or empty strings return zero.
///
/// # Example
/// ```
/// use reiver_core::utils::parse_decimal_or_zero;
/// use rust_decimal::Decimal;
///
/// assert_eq!(parse_decimal_or_zero("12.50"), Decimal::new(1250, 2));
/// assert_eq!(parse_decimal_or_zero("invalid"), Decimal::ZERO);
/// assert_eq!(parse_decimal_or_zero(""), Decimal::ZERO);
/// ```
pub fn parse_decimal_or_zero(s: &str) -> rust_decimal::Decimal {
    s.parse().unwrap_or(rust_decimal::Decimal::ZERO)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_single_quote() {
        assert_eq!(escape_clickhouse_string("it's"), "it\\'s");
    }

    #[test]
    fn test_escape_backslash() {
        assert_eq!(
            escape_clickhouse_string("path\\to\\file"),
            "path\\\\to\\\\file"
        );
    }

    #[test]
    fn test_escape_both() {
        assert_eq!(escape_clickhouse_string("it's\\here"), "it\\'s\\\\here");
    }

    #[test]
    fn test_no_escape_needed() {
        assert_eq!(escape_clickhouse_string("hello world"), "hello world");
    }

    #[test]
    fn test_empty_string() {
        assert_eq!(escape_clickhouse_string(""), "");
    }

    #[test]
    fn test_escape_null_byte() {
        assert_eq!(escape_clickhouse_string("before\0after"), "beforeafter");
    }

    #[test]
    fn test_escape_newline() {
        assert_eq!(escape_clickhouse_string("line1\nline2"), "line1\\nline2");
    }

    #[test]
    fn test_escape_carriage_return() {
        assert_eq!(escape_clickhouse_string("line1\rline2"), "line1\\rline2");
    }

    #[test]
    fn test_escape_tab() {
        assert_eq!(escape_clickhouse_string("col1\tcol2"), "col1\\tcol2");
    }

    #[test]
    fn test_escape_complex() {
        assert_eq!(
            escape_clickhouse_string("user's\ninput\twith\\slash"),
            "user\\'s\\ninput\\twith\\\\slash"
        );
    }

    #[test]
    fn test_parse_decimal_valid() {
        use rust_decimal::Decimal;
        assert_eq!(parse_decimal_or_zero("12.50"), Decimal::new(1250, 2));
        assert_eq!(parse_decimal_or_zero("0.005"), Decimal::new(5, 3));
        assert_eq!(parse_decimal_or_zero("100"), Decimal::from(100));
    }

    #[test]
    fn test_parse_decimal_invalid() {
        use rust_decimal::Decimal;
        assert_eq!(parse_decimal_or_zero("invalid"), Decimal::ZERO);
        assert_eq!(parse_decimal_or_zero(""), Decimal::ZERO);
        assert_eq!(parse_decimal_or_zero("12.50.30"), Decimal::ZERO);
    }

    #[test]
    fn test_parse_decimal_negative() {
        use rust_decimal::Decimal;
        // Negative values should parse correctly
        assert_eq!(parse_decimal_or_zero("-5.25"), Decimal::new(-525, 2));
    }

    #[test]
    fn test_percentage_change_increase() {
        let result = percentage_change(120.0, 100.0);
        assert!((result - 20.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_percentage_change_decrease() {
        let result = percentage_change(80.0, 100.0);
        assert!((result - (-20.0)).abs() < f64::EPSILON);
    }

    #[test]
    fn test_percentage_change_zero_baseline() {
        assert_eq!(percentage_change(50.0, 0.0), 0.0);
    }

    #[test]
    fn test_percentage_change_no_change() {
        assert_eq!(percentage_change(100.0, 100.0), 0.0);
    }
}
