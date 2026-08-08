use crate::app_state::RedisPool;
use crate::config::Config;
use crate::error::{AppError, Result};
use bb8_redis::redis::Script;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{debug, error, trace};

/// Build a rate limit Redis key efficiently using itoa for number formatting.
/// This is ~2x faster than using format!() due to avoiding format string parsing overhead.
#[inline]
fn build_rate_limit_key(prefix: &str, limit: i32, user_id: &str) -> String {
    let mut buffer = itoa::Buffer::new();
    let limit_str = buffer.format(limit);

    // Pre-allocate capacity: "rate_limit:" (11) + prefix + ":" (1) + limit_str + ":" (1) + user_id
    let capacity = 11 + prefix.len() + 1 + limit_str.len() + 1 + user_id.len();
    let mut key = String::with_capacity(capacity);
    key.push_str("rate_limit:");
    key.push_str(prefix);
    key.push(':');
    key.push_str(limit_str);
    key.push(':');
    key.push_str(user_id);
    key
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitInfo {
    pub current: i32,
    pub limit: i32,
    pub reset_at: DateTime<Utc>,
    pub dropped: i32,
}

#[derive(Debug, Clone, Copy)]
pub enum RateLimitType {
    /// Analytics/Query endpoints (configurable, defaults: 240/min, 1,200/hour)
    Analytics,
    /// CRUD endpoints (configurable, defaults: 480/min, 4,800/hour)
    Crud,
    /// Billing/Usage endpoints (configurable, defaults: 30/min, 120/hour)
    /// More restrictive since these endpoints query ClickHouse which can be expensive.
    Billing,
    /// AI Gateway endpoints (configurable, defaults: 60/min, 600/hour)
    /// Rate limits LLM requests per project to prevent cost attacks and provider exhaustion.
    Gateway,
    /// External API endpoints (configurable, defaults: 30/min, 300/hour)
    /// Used for endpoints that call third-party APIs (e.g., GitHub).
    /// More restrictive to prevent exhausting third-party rate limits.
    ExternalApi,
    /// Natural Language (Text-to-SQL) queries (configurable, defaults: 10/min, 60/hour).
    /// Very restrictive because each NL query triggers up to 3 LLM calls
    /// plus 3 ClickHouse queries, making it the most expensive single endpoint.
    NlQuery,
}

impl RateLimitType {
    /// Get rate limits from config
    pub fn limits_per_minute(&self, config: &Config) -> i32 {
        match self {
            RateLimitType::Analytics => config.rate_limit_analytics_per_minute,
            RateLimitType::Crud => config.rate_limit_crud_per_minute,
            RateLimitType::Billing => config.rate_limit_billing_per_minute,
            RateLimitType::Gateway => config.rate_limit_gateway_per_minute,
            RateLimitType::ExternalApi => config.rate_limit_external_api_per_minute,
            RateLimitType::NlQuery => config.rate_limit_nl_query_per_minute,
        }
    }

    /// Get rate limits from config
    pub fn limits_per_hour(&self, config: &Config) -> i32 {
        match self {
            RateLimitType::Analytics => config.rate_limit_analytics_per_hour,
            RateLimitType::Crud => config.rate_limit_crud_per_hour,
            RateLimitType::Billing => config.rate_limit_billing_per_hour,
            RateLimitType::Gateway => config.rate_limit_gateway_per_hour,
            RateLimitType::ExternalApi => config.rate_limit_external_api_per_hour,
            RateLimitType::NlQuery => config.rate_limit_nl_query_per_hour,
        }
    }
}

// ============================================================================
// Atomic Rate Limit Helpers
// ============================================================================

/// Lua script for atomic increment with TTL.
///
/// This script atomically:
/// 1. Increments the key by 1
/// 2. If the key is new (result == 1) OR has no TTL (TTL == -1), sets the expiry
///
/// This prevents the race condition where INCR succeeds but EXPIRE fails,
/// which would leave keys without expiry and permanently rate-limit users.
///
/// Returns: [current_count, ttl]
const ATOMIC_INCR_SCRIPT: &str = r#"
local current = redis.call('INCR', KEYS[1])
local ttl = redis.call('TTL', KEYS[1])
if ttl == -1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
    ttl = tonumber(ARGV[1])
end
return {current, ttl}
"#;

/// Lua script for atomic dual-window rate limit check.
///
/// This script atomically increments BOTH minute and hour counters in a single
/// Redis round-trip, reducing network latency by ~50% compared to two separate calls.
///
/// Arguments:
/// - KEYS[1]: minute counter key
/// - KEYS[2]: hour counter key
/// - ARGV[1]: minute TTL (60)
/// - ARGV[2]: hour TTL (3600)
///
/// Returns: [min_current, min_ttl, hour_current, hour_ttl]
const DUAL_WINDOW_INCR_SCRIPT: &str = r#"
-- Increment minute counter
local min_current = redis.call('INCR', KEYS[1])
local min_ttl = redis.call('TTL', KEYS[1])
if min_ttl == -1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
    min_ttl = tonumber(ARGV[1])
end

-- Increment hour counter
local hour_current = redis.call('INCR', KEYS[2])
local hour_ttl = redis.call('TTL', KEYS[2])
if hour_ttl == -1 then
    redis.call('EXPIRE', KEYS[2], ARGV[2])
    hour_ttl = tonumber(ARGV[2])
end

return {min_current, min_ttl, hour_current, hour_ttl}
"#;

/// Result of an atomic rate limit increment operation.
struct RateLimitIncr {
    current: i32,
    ttl: i64,
}

/// Result of a dual-window atomic rate limit increment operation.
/// Contains both minute and hour window results from a single Redis call.
struct DualWindowRateLimitIncr {
    min_current: i32,
    min_ttl: i64,
    hour_current: i32,
    hour_ttl: i64,
}

/// Atomically increment a rate limit counter and set TTL if needed.
///
/// # Security
/// Uses a Lua script to ensure atomicity. If the process crashes during
/// execution, Redis either executes the entire script or none of it.
/// This prevents keys without TTL from being left behind.
async fn atomic_incr_with_ttl(
    conn: &mut bb8_redis::bb8::PooledConnection<'_, bb8_redis::RedisConnectionManager>,
    key: &str,
    ttl_seconds: i64,
) -> Result<RateLimitIncr> {
    let script = Script::new(ATOMIC_INCR_SCRIPT);

    let result: (i32, i64) = tokio::time::timeout(
        Duration::from_secs(5),
        script.key(key).arg(ttl_seconds).invoke_async(&mut **conn),
    )
    .await
    .map_err(|e| {
        error!(key = %key, error = %e, "Rate limit: Lua script timeout");
        AppError::Internal(anyhow::anyhow!("Redis rate limit script timeout: {}", e))
    })?
    .map_err(|e: bb8_redis::redis::RedisError| {
        error!(key = %key, error = %e, "Rate limit: Lua script error");
        AppError::Internal(anyhow::anyhow!("Redis rate limit script error: {}", e))
    })?;

    Ok(RateLimitIncr {
        current: result.0,
        ttl: result.1,
    })
}

/// Atomically increment BOTH minute and hour rate limit counters in a single Redis call.
///
/// This reduces Redis round-trips by 50% compared to two separate calls.
///
/// # Security
/// Uses a Lua script to ensure atomicity. Both counters are incremented together,
/// preventing any race conditions between the minute and hour checks.
async fn dual_window_incr_with_ttl(
    conn: &mut bb8_redis::bb8::PooledConnection<'_, bb8_redis::RedisConnectionManager>,
    key_min: &str,
    key_hour: &str,
) -> Result<DualWindowRateLimitIncr> {
    let script = Script::new(DUAL_WINDOW_INCR_SCRIPT);

    let result: (i32, i64, i32, i64) = tokio::time::timeout(
        Duration::from_secs(5),
        script
            .key(key_min)
            .key(key_hour)
            .arg(60)    // minute TTL
            .arg(3600)  // hour TTL
            .invoke_async(&mut **conn)
    )
    .await
    .map_err(|e| {
        error!(key_min = %key_min, key_hour = %key_hour, error = %e, "Rate limit: dual window Lua script timeout");
        AppError::Internal(anyhow::anyhow!("Redis rate limit script timeout: {}", e))
    })?
    .map_err(|e: bb8_redis::redis::RedisError| {
        error!(key_min = %key_min, key_hour = %key_hour, error = %e, "Rate limit: dual window Lua script error");
        AppError::Internal(anyhow::anyhow!("Redis rate limit script error: {}", e))
    })?;

    Ok(DualWindowRateLimitIncr {
        min_current: result.0,
        min_ttl: result.1,
        hour_current: result.2,
        hour_ttl: result.3,
    })
}

/// Generic organization-based rate limit check.
///
/// # Arguments
/// * `redis` - Redis connection pool
/// * `organization_id` - Organization UUID for rate limiting
/// * `limit_name` - Name for the rate limit (used in Redis key and logging)
/// * `limit_per_hour` - Maximum requests allowed per hour
///
/// # Security
/// Uses atomic Lua script to prevent INCR/EXPIRE race condition.
async fn check_org_hourly_rate_limit(
    redis: &RedisPool,
    organization_id: &uuid::Uuid,
    limit_name: &str,
    limit_per_hour: i32,
) -> Result<RateLimitInfo> {
    let org_id_str = organization_id.to_string();
    let key_hour = format!("rate_limit:{}:hour:{}", limit_name, org_id_str);

    trace!(
        limit_name = %limit_name,
        organization_id = %organization_id,
        "Checking org rate limit"
    );

    let mut conn = redis.get().await.map_err(|e| {
        error!(limit_name = %limit_name, error = %e, "Rate limit: Failed to get Redis connection");
        AppError::Internal(anyhow::anyhow!("Failed to get Redis connection: {}", e))
    })?;

    let result = atomic_incr_with_ttl(&mut conn, &key_hour, 3600).await?;

    let reset_at = if result.ttl > 0 {
        Utc::now() + chrono::Duration::seconds(result.ttl)
    } else {
        Utc::now() + chrono::Duration::seconds(3600)
    };

    let dropped = if result.current > limit_per_hour {
        result.current - limit_per_hour
    } else {
        0
    };

    let info = RateLimitInfo {
        current: result.current,
        limit: limit_per_hour,
        reset_at,
        dropped,
    };

    if result.current > limit_per_hour {
        error!(
            limit_name = %limit_name,
            current = result.current,
            limit = limit_per_hour,
            organization_id = %organization_id,
            "Rate limit exceeded"
        );
        return Err(AppError::RateLimitExceeded(info));
    }

    trace!(
        limit_name = %limit_name,
        current = result.current,
        limit = limit_per_hour,
        "Rate limit check passed"
    );
    Ok(info)
}

/// Check rate limit for authenticated API endpoints with multiple time windows (PostHog-style)
/// Returns RateLimitInfo or RateLimitExceeded error
///
/// # Security
/// Uses atomic Lua script to prevent INCR/EXPIRE race condition that could
/// leave keys without TTL and permanently rate-limit users.
///
/// # Performance
/// Uses a single Lua script to check both minute and hour limits in one Redis round-trip,
/// reducing network latency by ~50% compared to two separate calls.
pub async fn check_authenticated_rate_limit(
    redis: &RedisPool,
    user_id: &uuid::Uuid,
    limit_type: RateLimitType,
    config: &Config,
) -> Result<RateLimitInfo> {
    let user_id_str = user_id.to_string();
    let limit_min = limit_type.limits_per_minute(config);
    let limit_hour = limit_type.limits_per_hour(config);

    // Use itoa-based key builder for ~2x faster key generation
    let key_min = build_rate_limit_key("min", limit_min, &user_id_str);
    let key_hour = build_rate_limit_key("hour", limit_hour, &user_id_str);

    trace!(
        user_id = %user_id,
        limit_type = ?limit_type,
        "Checking authenticated rate limit"
    );

    let mut conn = redis.get().await.map_err(|e| {
        error!(error = %e, "Rate limit: Failed to get Redis connection");
        AppError::Internal(anyhow::anyhow!("Failed to get Redis connection: {}", e))
    })?;

    // Atomically increment both minute and hour counters in a single Redis call
    let result = dual_window_incr_with_ttl(&mut conn, &key_min, &key_hour).await?;

    // Use the stricter limit (whichever is hit first)
    let (current, limit, reset_at) = if result.min_current > limit_min {
        // Per-minute limit exceeded
        let reset_at = if result.min_ttl > 0 {
            Utc::now() + chrono::Duration::seconds(result.min_ttl)
        } else {
            Utc::now() + chrono::Duration::seconds(60)
        };
        (result.min_current, limit_min, reset_at)
    } else if result.hour_current > limit_hour {
        // Per-hour limit exceeded
        let reset_at = if result.hour_ttl > 0 {
            Utc::now() + chrono::Duration::seconds(result.hour_ttl)
        } else {
            Utc::now() + chrono::Duration::seconds(3600)
        };
        (result.hour_current, limit_hour, reset_at)
    } else {
        // Not exceeded, use per-minute values for response
        let reset_at = if result.min_ttl > 0 {
            Utc::now() + chrono::Duration::seconds(result.min_ttl)
        } else {
            Utc::now() + chrono::Duration::seconds(60)
        };
        (result.min_current, limit_min, reset_at)
    };

    let dropped = if current > limit { current - limit } else { 0 };

    let info = RateLimitInfo {
        current,
        limit,
        reset_at,
        dropped,
    };

    if current > limit {
        error!(
            current = current,
            limit = limit,
            limit_type = ?limit_type,
            user_id = %user_id,
            "Rate limit exceeded"
        );
        return Err(AppError::RateLimitExceeded(info));
    }

    trace!(
        current = current,
        limit = limit,
        limit_type = ?limit_type,
        "Authenticated rate limit check passed"
    );
    Ok(info)
}

/// Rate limit for AI Gateway endpoints using project_id.
///
/// # Security
/// - Prevents cost attacks by limiting LLM requests per project
/// - Prevents exhaustion of provider rate limits
/// - Uses project_id as the identifier since gateway auth is project-key based
/// - Uses atomic Lua script to prevent INCR/EXPIRE race condition
///
/// # Performance
/// Uses a single Lua script to check both minute and hour limits in one Redis round-trip.
pub async fn check_gateway_rate_limit(
    redis: &RedisPool,
    project_id: &uuid::Uuid,
    config: &Config,
) -> Result<RateLimitInfo> {
    let limit_min = config.rate_limit_gateway_per_minute;
    let limit_hour = config.rate_limit_gateway_per_hour;
    let project_id_str = project_id.to_string();

    let key_min = format!("rate_limit:gateway:min:{}", project_id_str);
    let key_hour = format!("rate_limit:gateway:hour:{}", project_id_str);

    trace!(
        project_id = %project_id,
        "Checking gateway rate limit"
    );

    let mut conn = redis.get().await.map_err(|e| {
        error!(error = %e, "Rate limit: Failed to get Redis connection");
        AppError::Internal(anyhow::anyhow!("Failed to get Redis connection: {}", e))
    })?;

    // Atomically increment both minute and hour counters in a single Redis call
    let result = dual_window_incr_with_ttl(&mut conn, &key_min, &key_hour).await?;

    // Use the stricter limit (whichever is hit first)
    let (current, limit, reset_at) = if result.min_current > limit_min {
        // Per-minute limit exceeded
        let reset_at = if result.min_ttl > 0 {
            Utc::now() + chrono::Duration::seconds(result.min_ttl)
        } else {
            Utc::now() + chrono::Duration::seconds(60)
        };
        (result.min_current, limit_min, reset_at)
    } else if result.hour_current > limit_hour {
        // Per-hour limit exceeded
        let reset_at = if result.hour_ttl > 0 {
            Utc::now() + chrono::Duration::seconds(result.hour_ttl)
        } else {
            Utc::now() + chrono::Duration::seconds(3600)
        };
        (result.hour_current, limit_hour, reset_at)
    } else {
        // Not exceeded, use per-minute values for response
        let reset_at = if result.min_ttl > 0 {
            Utc::now() + chrono::Duration::seconds(result.min_ttl)
        } else {
            Utc::now() + chrono::Duration::seconds(60)
        };
        (result.min_current, limit_min, reset_at)
    };

    let dropped = if current > limit { current - limit } else { 0 };

    let info = RateLimitInfo {
        current,
        limit,
        reset_at,
        dropped,
    };

    if current > limit {
        debug!(
            current = current,
            limit = limit,
            project_id = %project_id,
            "Gateway rate limit exceeded"
        );
        return Err(AppError::RateLimitExceeded(info));
    }

    trace!(
        current = current,
        limit = limit,
        project_id = %project_id,
        "Gateway rate limit passed"
    );

    Ok(info)
}

/// Check per-project gateway usage limits using customer-configured settings.
///
/// This enforces the spend-protection limits stored in `project_settings`
/// (`gateway_rate_limit_rpm`). Unlike `check_gateway_rate_limit` which uses
/// global infrastructure limits, this enforces per-project limits that
/// customers configure themselves to prevent runaway agents or accidental
/// overspending.
///
/// The hourly limit is derived as `limit_per_minute * 60`.
///
/// Uses a distinct Redis key namespace (`usage_limit:project:`) so it does not
/// interfere with the global infrastructure rate limits.
pub async fn check_project_usage_limit(
    redis: &RedisPool,
    project_id: &uuid::Uuid,
    limit_per_minute: i32,
) -> Result<RateLimitInfo> {
    let limit_per_hour = limit_per_minute.saturating_mul(60);
    let project_id_str = project_id.to_string();

    let key_min = format!("usage_limit:project:min:{}", project_id_str);
    let key_hour = format!("usage_limit:project:hour:{}", project_id_str);

    trace!(
        project_id = %project_id,
        limit_per_minute = limit_per_minute,
        "Checking project usage limit"
    );

    let mut conn = redis.get().await.map_err(|e| {
        error!(error = %e, "Usage limit: Failed to get Redis connection");
        AppError::Internal(anyhow::anyhow!("Failed to get Redis connection: {}", e))
    })?;

    let result = dual_window_incr_with_ttl(&mut conn, &key_min, &key_hour).await?;

    let (current, limit, reset_at) = if result.min_current > limit_per_minute {
        let reset_at = if result.min_ttl > 0 {
            Utc::now() + chrono::Duration::seconds(result.min_ttl)
        } else {
            Utc::now() + chrono::Duration::seconds(60)
        };
        (result.min_current, limit_per_minute, reset_at)
    } else if result.hour_current > limit_per_hour {
        let reset_at = if result.hour_ttl > 0 {
            Utc::now() + chrono::Duration::seconds(result.hour_ttl)
        } else {
            Utc::now() + chrono::Duration::seconds(3600)
        };
        (result.hour_current, limit_per_hour, reset_at)
    } else {
        let reset_at = if result.min_ttl > 0 {
            Utc::now() + chrono::Duration::seconds(result.min_ttl)
        } else {
            Utc::now() + chrono::Duration::seconds(60)
        };
        (result.min_current, limit_per_minute, reset_at)
    };

    let dropped = if current > limit { current - limit } else { 0 };

    let info = RateLimitInfo {
        current,
        limit,
        reset_at,
        dropped,
    };

    if current > limit {
        debug!(
            current = current,
            limit = limit,
            project_id = %project_id,
            "Project usage limit exceeded"
        );
        return Err(AppError::RateLimitExceeded(info));
    }

    trace!(
        current = current,
        limit = limit,
        project_id = %project_id,
        "Project usage limit passed"
    );

    Ok(info)
}

/// Rate limit for telemetry ingestion endpoints using project_id.
///
/// # Security
/// - Prevents DoS attacks via excessive telemetry ingestion
/// - Prevents storage exhaustion in ClickHouse
/// - Prevents cost attacks via infrastructure usage
/// - Uses project_id as the identifier since ingestion auth is project-key based
/// - Uses atomic Lua script to prevent INCR/EXPIRE race condition
///
/// # Performance
/// Uses a single Lua script to check both minute and hour limits in one Redis round-trip,
/// reducing network latency by ~50% compared to two separate calls.
///
/// Higher limits than gateway since telemetry is expected to be high volume.
/// Configurable via RATE_LIMIT_INGESTION_PER_MINUTE and RATE_LIMIT_INGESTION_PER_HOUR.
/// Default limits: 1000/min, 30000/hour per project
pub async fn check_ingestion_rate_limit(
    redis: &RedisPool,
    project_id: &uuid::Uuid,
    config: &Config,
) -> Result<RateLimitInfo> {
    let limit_min = config.rate_limit_ingestion_per_minute;
    let limit_hour = config.rate_limit_ingestion_per_hour;

    let project_id_str = project_id.to_string();

    let key_min = format!("rate_limit:ingestion:min:{}", project_id_str);
    let key_hour = format!("rate_limit:ingestion:hour:{}", project_id_str);

    trace!(
        project_id = %project_id,
        "Checking ingestion rate limit"
    );

    let mut conn = redis.get().await.map_err(|e| {
        error!(error = %e, "Rate limit: Failed to get Redis connection");
        AppError::Internal(anyhow::anyhow!("Failed to get Redis connection: {}", e))
    })?;

    // Atomically increment both minute and hour counters in a single Redis call
    let result = dual_window_incr_with_ttl(&mut conn, &key_min, &key_hour).await?;

    // Use the stricter limit (whichever is hit first)
    let (current, limit, reset_at) = if result.min_current > limit_min {
        let reset_at = if result.min_ttl > 0 {
            Utc::now() + chrono::Duration::seconds(result.min_ttl)
        } else {
            Utc::now() + chrono::Duration::seconds(60)
        };
        (result.min_current, limit_min, reset_at)
    } else if result.hour_current > limit_hour {
        let reset_at = if result.hour_ttl > 0 {
            Utc::now() + chrono::Duration::seconds(result.hour_ttl)
        } else {
            Utc::now() + chrono::Duration::seconds(3600)
        };
        (result.hour_current, limit_hour, reset_at)
    } else {
        let reset_at = if result.min_ttl > 0 {
            Utc::now() + chrono::Duration::seconds(result.min_ttl)
        } else {
            Utc::now() + chrono::Duration::seconds(60)
        };
        (result.min_current, limit_min, reset_at)
    };

    let dropped = if current > limit { current - limit } else { 0 };

    let info = RateLimitInfo {
        current,
        limit,
        reset_at,
        dropped,
    };

    if current > limit {
        debug!(
            current = current,
            limit = limit,
            project_id = %project_id,
            "Ingestion rate limit exceeded"
        );
        return Err(AppError::RateLimitExceeded(info));
    }

    trace!(
        current = current,
        limit = limit,
        project_id = %project_id,
        "Ingestion rate limit check passed"
    );
    Ok(info)
}

/// Rate limit for unauthenticated endpoints (login, SSO, password reset)
/// Uses IP address as the identifier. Stricter limits to prevent brute force attacks.
///
/// # Security
/// - Prevents brute force attacks on login endpoints
/// - Uses IP-based limiting since user is not yet authenticated
/// - Stricter limits: 10 attempts per minute, 30 per hour by default
/// - Uses atomic Lua script to prevent INCR/EXPIRE race condition
///
/// # Performance
/// Uses a single Lua script to check both minute and hour limits in one Redis round-trip.
pub async fn check_unauthenticated_rate_limit(
    redis: &RedisPool,
    client_ip: &str,
    endpoint: &str,
) -> Result<RateLimitInfo> {
    // Stricter limits for unauthenticated endpoints (configurable via env vars)
    let limit_per_minute: i32 = std::env::var("RATE_LIMIT_UNAUTH_PER_MINUTE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);
    let limit_per_hour: i32 = std::env::var("RATE_LIMIT_UNAUTH_PER_HOUR")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);

    // Sanitize IP for use in Redis key
    let sanitized_ip = client_ip.replace([':', '.', '-'], "_");
    let sanitized_endpoint = endpoint.replace(['/', '-'], "_");

    let key_min = format!(
        "rate_limit:unauth:min:{}:{}",
        sanitized_endpoint, sanitized_ip
    );
    let key_hour = format!(
        "rate_limit:unauth:hour:{}:{}",
        sanitized_endpoint, sanitized_ip
    );

    trace!(
        endpoint = %endpoint,
        "Checking unauthenticated rate limit"
    );

    let mut conn = redis.get().await.map_err(|e| {
        error!(error = %e, "Rate limit: Failed to get Redis connection");
        AppError::Internal(anyhow::anyhow!("Failed to get Redis connection: {}", e))
    })?;

    // Atomically increment both minute and hour counters in a single Redis call
    let result = dual_window_incr_with_ttl(&mut conn, &key_min, &key_hour).await?;

    // Determine which limit is exceeded
    let (current, limit, reset_at) = if result.min_current > limit_per_minute {
        let reset_at = if result.min_ttl > 0 {
            Utc::now() + chrono::Duration::seconds(result.min_ttl)
        } else {
            Utc::now() + chrono::Duration::seconds(60)
        };
        (result.min_current, limit_per_minute, reset_at)
    } else if result.hour_current > limit_per_hour {
        let reset_at = if result.hour_ttl > 0 {
            Utc::now() + chrono::Duration::seconds(result.hour_ttl)
        } else {
            Utc::now() + chrono::Duration::seconds(3600)
        };
        (result.hour_current, limit_per_hour, reset_at)
    } else {
        let reset_at = if result.min_ttl > 0 {
            Utc::now() + chrono::Duration::seconds(result.min_ttl)
        } else {
            Utc::now() + chrono::Duration::seconds(60)
        };
        (result.min_current, limit_per_minute, reset_at)
    };

    let dropped = if current > limit { current - limit } else { 0 };

    let info = RateLimitInfo {
        current,
        limit,
        reset_at,
        dropped,
    };

    if current > limit {
        error!(
            current = current,
            limit = limit,
            endpoint = %endpoint,
            "Rate limit exceeded for unauthenticated endpoint"
        );
        return Err(AppError::RateLimitExceeded(info));
    }

    trace!(
        current = current,
        limit = limit,
        "Unauthenticated rate limit check passed"
    );
    Ok(info)
}

/// Rate limit for webhook endpoints (GitHub, Stripe, etc.).
/// Uses IP address as the identifier with lenient limits since:
/// - Webhook signature verification is the primary security measure
/// - Webhooks come from known provider IP ranges
/// - Rate limiting is defense-in-depth against DoS, not the primary control
///
/// # Security
/// - Prevents resource exhaustion from flood of invalid webhook requests
/// - Uses atomic Lua script to prevent INCR/EXPIRE race condition
///
/// # Performance
/// Uses a single Lua script to check both minute and hour limits in one Redis round-trip.
///
/// Limits: 100 requests per minute, 1000 per hour per IP
pub async fn check_webhook_rate_limit(
    redis: &RedisPool,
    client_ip: &str,
    webhook_source: &str,
) -> Result<RateLimitInfo> {
    // Lenient limits for webhooks - signature verification is the primary security
    const LIMIT_PER_MINUTE: i32 = 100;
    const LIMIT_PER_HOUR: i32 = 1000;

    // Sanitize IP for use in Redis key
    let sanitized_ip = client_ip.replace([':', '.', '-'], "_");
    let sanitized_source = webhook_source.replace(['/', '-'], "_");

    let key_min = format!(
        "rate_limit:webhook:min:{}:{}",
        sanitized_source, sanitized_ip
    );
    let key_hour = format!(
        "rate_limit:webhook:hour:{}:{}",
        sanitized_source, sanitized_ip
    );

    trace!(
        webhook_source = %webhook_source,
        "Checking webhook rate limit"
    );

    let mut conn = redis.get().await.map_err(|e| {
        error!(error = %e, "Rate limit: Failed to get Redis connection");
        AppError::Internal(anyhow::anyhow!("Failed to get Redis connection: {}", e))
    })?;

    // Atomically increment both minute and hour counters in a single Redis call
    let result = dual_window_incr_with_ttl(&mut conn, &key_min, &key_hour).await?;

    // Determine which limit is exceeded
    let (current, limit, reset_at) = if result.min_current > LIMIT_PER_MINUTE {
        let reset_at = if result.min_ttl > 0 {
            Utc::now() + chrono::Duration::seconds(result.min_ttl)
        } else {
            Utc::now() + chrono::Duration::seconds(60)
        };
        (result.min_current, LIMIT_PER_MINUTE, reset_at)
    } else if result.hour_current > LIMIT_PER_HOUR {
        let reset_at = if result.hour_ttl > 0 {
            Utc::now() + chrono::Duration::seconds(result.hour_ttl)
        } else {
            Utc::now() + chrono::Duration::seconds(3600)
        };
        (result.hour_current, LIMIT_PER_HOUR, reset_at)
    } else {
        let reset_at = if result.min_ttl > 0 {
            Utc::now() + chrono::Duration::seconds(result.min_ttl)
        } else {
            Utc::now() + chrono::Duration::seconds(60)
        };
        (result.min_current, LIMIT_PER_MINUTE, reset_at)
    };

    let dropped = if current > limit { current - limit } else { 0 };

    let info = RateLimitInfo {
        current,
        limit,
        reset_at,
        dropped,
    };

    if current > limit {
        debug!(
            current = current,
            limit = limit,
            webhook_source = %webhook_source,
            "Webhook rate limit exceeded"
        );
        return Err(AppError::RateLimitExceeded(info));
    }

    trace!(
        current = current,
        limit = limit,
        "Webhook rate limit check passed"
    );
    Ok(info)
}

/// Rate limit for recovery code verification attempts.
///
/// # Security
/// Recovery codes require much stricter rate limiting because:
/// - There are only 10 recovery codes per user
/// - Brute force is a real threat without proper limiting
/// - Recovery codes are backup access, so should be used rarely
/// - Uses atomic Lua script to prevent INCR/EXPIRE race condition
///
/// # Performance
/// Uses a single Lua script to check both minute and hour limits in one Redis round-trip.
///
/// Limits: 3 attempts per minute, 5 per hour
/// At 5/hour, it would take 2+ hours to try all 10 codes.
pub async fn check_recovery_code_rate_limit(
    redis: &RedisPool,
    user_id: &uuid::Uuid,
) -> Result<RateLimitInfo> {
    // Very strict limits for recovery code attempts
    const LIMIT_PER_MINUTE: i32 = 3;
    const LIMIT_PER_HOUR: i32 = 5;

    let user_id_str = user_id.to_string();
    let key_min = format!("rate_limit:recovery_code:min:{}", user_id_str);
    let key_hour = format!("rate_limit:recovery_code:hour:{}", user_id_str);

    trace!(
        user_id = %user_id,
        "Checking recovery code rate limit"
    );

    let mut conn = redis.get().await.map_err(|e| {
        error!(error = %e, "Rate limit: Failed to get Redis connection");
        AppError::Internal(anyhow::anyhow!("Failed to get Redis connection: {}", e))
    })?;

    // Atomically increment both minute and hour counters in a single Redis call
    let result = dual_window_incr_with_ttl(&mut conn, &key_min, &key_hour).await?;

    // Determine which limit is exceeded
    let (current, limit, reset_at) = if result.min_current > LIMIT_PER_MINUTE {
        let reset_at = if result.min_ttl > 0 {
            Utc::now() + chrono::Duration::seconds(result.min_ttl)
        } else {
            Utc::now() + chrono::Duration::seconds(60)
        };
        (result.min_current, LIMIT_PER_MINUTE, reset_at)
    } else if result.hour_current > LIMIT_PER_HOUR {
        let reset_at = if result.hour_ttl > 0 {
            Utc::now() + chrono::Duration::seconds(result.hour_ttl)
        } else {
            Utc::now() + chrono::Duration::seconds(3600)
        };
        (result.hour_current, LIMIT_PER_HOUR, reset_at)
    } else {
        let reset_at = if result.min_ttl > 0 {
            Utc::now() + chrono::Duration::seconds(result.min_ttl)
        } else {
            Utc::now() + chrono::Duration::seconds(60)
        };
        (result.min_current, LIMIT_PER_MINUTE, reset_at)
    };

    let dropped = if current > limit { current - limit } else { 0 };

    let info = RateLimitInfo {
        current,
        limit,
        reset_at,
        dropped,
    };

    if current > limit {
        error!(
            current = current,
            limit = limit,
            user_id = %user_id,
            "Recovery code rate limit exceeded"
        );
        return Err(AppError::RateLimitExceeded(info));
    }

    trace!(
        current = current,
        limit = limit,
        "Recovery code rate limit check passed"
    );
    Ok(info)
}

/// Rate limit for Stripe setup intent creation.
///
/// # Security
/// Setup intents can incur costs at scale and should be rate limited strictly.
/// This prevents abuse where an attacker creates many setup intents.
/// Uses atomic Lua script to prevent INCR/EXPIRE race condition.
///
/// Limits: 5 per hour per organization
/// This is sufficient for legitimate use (adding payment methods) but prevents abuse.
pub async fn check_setup_intent_rate_limit(
    redis: &RedisPool,
    organization_id: &uuid::Uuid,
) -> Result<RateLimitInfo> {
    const LIMIT_PER_HOUR: i32 = 5;
    check_org_hourly_rate_limit(redis, organization_id, "setup_intent", LIMIT_PER_HOUR).await
}

/// Rate limit for Stripe subscription creation.
///
/// # Security
/// Subscription creation can incur costs and create billing issues if abused.
/// This prevents abuse where an attacker rapidly creates/cancels subscriptions.
/// Uses atomic Lua script to prevent INCR/EXPIRE race condition.
///
/// Limits: 3 per hour per organization
/// This is sufficient for legitimate use but prevents abuse.
pub async fn check_subscription_rate_limit(
    redis: &RedisPool,
    organization_id: &uuid::Uuid,
) -> Result<RateLimitInfo> {
    const LIMIT_PER_HOUR: i32 = 3;
    check_org_hourly_rate_limit(redis, organization_id, "subscription", LIMIT_PER_HOUR).await
}

/// Rate limit for payment method confirmation.
///
/// # Security
/// While setup intent creation is already rate limited, we also rate limit confirmation
/// to prevent abuse where an attacker rapidly retries confirmation with different parameters.
/// Uses atomic Lua script to prevent INCR/EXPIRE race condition.
///
/// Limits: 10 per hour per organization
/// Slightly higher than setup intent creation since legitimate users might retry confirmations.
pub async fn check_payment_method_confirm_rate_limit(
    redis: &RedisPool,
    organization_id: &uuid::Uuid,
) -> Result<RateLimitInfo> {
    const LIMIT_PER_HOUR: i32 = 10;
    check_org_hourly_rate_limit(redis, organization_id, "pm_confirm", LIMIT_PER_HOUR).await
}

/// Extract client IP from the actual TCP connection.
///
/// # Security
///
/// This function uses the real connection IP, NOT headers like `X-Forwarded-For`.
/// Headers can be spoofed by attackers to bypass rate limiting.
///
/// ## Proxy Configuration Requirements
///
/// If deployed behind a reverse proxy (nginx, Cloudflare, AWS ALB, etc.), you must:
///
/// 1. **Configure the proxy to terminate at the application** - The proxy should forward
///    the real client IP in a trusted header.
///
/// 2. **For rate limiting to work correctly**, choose one of these options:
///
///    **Option A: Proxy-level rate limiting (recommended)**
///    Configure rate limiting at the proxy layer (e.g., nginx `limit_req`, Cloudflare WAF).
///    This is more efficient and handles the IP extraction correctly.
///    
///    **Option B: Pass real IP via trusted header**
///    Configure your proxy to set `X-Real-IP` or `X-Forwarded-For`, then modify this
///    function to trust headers ONLY when the connection comes from known proxy IPs.
///    
///    Example nginx config:
///    ```nginx
///    proxy_set_header X-Real-IP $remote_addr;
///    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
///    ```
///
/// 3. **For Stripe webhooks specifically**: Using socket IP is intentional for webhook
///    endpoints because:
///    - Stripe provides a separate IP allowlist feature (`stripe_webhook_ip_allowlist`)
///    - Webhook signature verification is the primary security measure
///    - Rate limiting on webhooks is secondary defense-in-depth
///
/// # Warning
///
/// Without proper proxy configuration, all requests behind a load balancer will appear
/// to come from the same IP (the proxy's IP), making rate limiting ineffective.
pub fn extract_client_ip(addr: &std::net::SocketAddr) -> String {
    addr.ip().to_string()
}

// ============================================================================
// Distributed Locking for Billing Operations
// ============================================================================

/// Lua script for acquiring a distributed lock with NX (only if not exists) and EX (expiry).
///
/// This implements a simple distributed lock pattern:
/// - SET NX ensures only one caller acquires the lock
/// - EX sets an expiry to prevent deadlocks if the holder crashes
/// - Returns 1 if lock acquired, 0 if already held
///
/// Note: This is a basic lock without automatic renewal. The TTL should be set
/// long enough to cover the expected operation duration with margin for safety.
const ACQUIRE_LOCK_SCRIPT: &str = r#"
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2]) then
    return 1
else
    return 0
end
"#;

/// Lua script for releasing a distributed lock.
///
/// Only releases the lock if the value matches (i.e., the caller owns the lock).
/// This prevents accidentally releasing a lock acquired by another process
/// after our lock expired and was re-acquired.
///
/// Returns 1 if lock was released, 0 if lock was not held or owned by another.
const RELEASE_LOCK_SCRIPT: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
"#;

/// A distributed lock guard that automatically releases the lock when dropped.
///
/// # Usage
/// ```ignore
/// let lock = acquire_billing_lock(&redis, &org_id, "create_subscription", 30).await?;
/// // ... do work while holding the lock ...
/// drop(lock); // Or let it go out of scope
/// ```
///
/// # Security
/// The lock uses a random token to ensure only the holder can release it.
/// This prevents race conditions where lock A expires, lock B is acquired,
/// and then A's release accidentally frees B's lock.
pub struct DistributedLock {
    redis: std::sync::Arc<RedisPool>,
    key: String,
    token: String,
}

impl DistributedLock {
    /// Explicitly release the lock. Called automatically on drop.
    pub async fn release(self) {
        // Intentionally consume self to prevent double-release
        self.release_internal().await;
    }

    async fn release_internal(&self) {
        if let Ok(mut conn) = self.redis.get().await {
            let script = Script::new(RELEASE_LOCK_SCRIPT);
            let result: std::result::Result<i32, _> = tokio::time::timeout(
                Duration::from_secs(5),
                script
                    .key(&self.key)
                    .arg(&self.token)
                    .invoke_async(&mut *conn),
            )
            .await
            .unwrap_or(Ok(0));

            match result {
                Ok(1) => trace!(key = %self.key, "Distributed lock released"),
                Ok(_) => debug!(key = %self.key, "Lock was already released or expired"),
                Err(e) => {
                    debug!(key = %self.key, error = %e, "Failed to release lock (may have expired)")
                }
            }
        }
    }
}

impl Drop for DistributedLock {
    fn drop(&mut self) {
        // We can't await in drop, so we spawn a task to release the lock.
        // This is best-effort - if the runtime is shutting down, the lock
        // will expire via TTL anyway.
        let redis = self.redis.clone();
        let key = self.key.clone();
        let token = self.token.clone();

        tokio::spawn(async move {
            if let Ok(mut conn) = redis.get().await {
                let script = Script::new(RELEASE_LOCK_SCRIPT);
                let _: std::result::Result<i32, _> = tokio::time::timeout(
                    Duration::from_secs(2),
                    script.key(&key).arg(&token).invoke_async(&mut *conn),
                )
                .await
                .unwrap_or(Ok(0));
            }
        });
    }
}

/// Error type for distributed lock operations.
#[derive(Debug)]
pub enum LockError {
    /// Lock is already held by another process
    AlreadyLocked,
    /// Failed to communicate with Redis
    RedisError(String),
}

impl std::fmt::Display for LockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockError::AlreadyLocked => write!(f, "Lock is already held by another process"),
            LockError::RedisError(msg) => write!(f, "Redis error: {}", msg),
        }
    }
}

impl std::error::Error for LockError {}

/// Acquire a distributed lock for a billing operation.
///
/// # Arguments
/// * `redis` - Redis connection pool
/// * `organization_id` - Organization UUID (used in lock key)
/// * `operation` - Name of the operation (e.g., "create_subscription")
/// * `ttl_seconds` - Lock TTL in seconds (should cover operation duration + margin)
///
/// # Returns
/// * `Ok(DistributedLock)` - Lock acquired, caller should hold the guard
/// * `Err(LockError::AlreadyLocked)` - Another process holds the lock
/// * `Err(LockError::RedisError)` - Redis communication error
///
/// # Example
/// ```ignore
/// match acquire_billing_lock(&redis, &org_id, "create_subscription", 30).await {
///     Ok(lock) => {
///         // Perform the operation
///         let result = create_subscription_impl(...).await;
///         drop(lock); // Release lock (or let it go out of scope)
///         result
///     }
///     Err(LockError::AlreadyLocked) => {
///         // Return "please try again" error to client
///     }
///     Err(LockError::RedisError(e)) => {
///         // Decide whether to proceed without lock or fail
///     }
/// }
/// ```
pub async fn acquire_billing_lock(
    redis: &std::sync::Arc<RedisPool>,
    organization_id: &uuid::Uuid,
    operation: &str,
    ttl_seconds: u64,
) -> std::result::Result<DistributedLock, LockError> {
    let key = format!("billing_lock:{}:{}", operation, organization_id);
    let token = uuid::Uuid::new_v4().to_string();

    let mut conn = redis.get().await.map_err(|e| {
        error!(error = %e, "Failed to get Redis connection for billing lock");
        LockError::RedisError(e.to_string())
    })?;

    let script = Script::new(ACQUIRE_LOCK_SCRIPT);
    let result: i32 = tokio::time::timeout(
        Duration::from_secs(5),
        script
            .key(&key)
            .arg(&token)
            .arg(ttl_seconds)
            .invoke_async(&mut *conn),
    )
    .await
    .map_err(|e| {
        error!(key = %key, error = %e, "Billing lock acquisition timeout");
        LockError::RedisError(format!("Timeout: {}", e))
    })?
    .map_err(|e: bb8_redis::redis::RedisError| {
        error!(key = %key, error = %e, "Billing lock acquisition error");
        LockError::RedisError(e.to_string())
    })?;

    if result == 1 {
        trace!(key = %key, ttl_seconds = ttl_seconds, "Distributed lock acquired");
        Ok(DistributedLock {
            redis: redis.clone(),
            key,
            token,
        })
    } else {
        debug!(key = %key, "Lock already held by another process");
        Err(LockError::AlreadyLocked)
    }
}

/// Try to acquire a billing lock with retries and backoff.
///
/// # Arguments
/// * `redis` - Redis connection pool
/// * `organization_id` - Organization UUID
/// * `operation` - Name of the operation
/// * `ttl_seconds` - Lock TTL
/// * `max_retries` - Maximum number of retry attempts
/// * `retry_delay_ms` - Initial delay between retries (doubles each retry)
///
/// # Returns
/// The lock guard if acquired, or the last error if all retries failed.
pub async fn acquire_billing_lock_with_retry(
    redis: &std::sync::Arc<RedisPool>,
    organization_id: &uuid::Uuid,
    operation: &str,
    ttl_seconds: u64,
    max_retries: u32,
    retry_delay_ms: u64,
) -> std::result::Result<DistributedLock, LockError> {
    let mut delay = Duration::from_millis(retry_delay_ms);
    let max_delay = Duration::from_millis(retry_delay_ms * 8); // Cap at 8x initial delay

    for attempt in 0..=max_retries {
        match acquire_billing_lock(redis, organization_id, operation, ttl_seconds).await {
            Ok(lock) => return Ok(lock),
            Err(LockError::AlreadyLocked) if attempt < max_retries => {
                trace!(
                    organization_id = %organization_id,
                    operation = %operation,
                    attempt = attempt + 1,
                    max_retries = max_retries,
                    delay_ms = delay.as_millis(),
                    "Lock held by another process, retrying"
                );
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(max_delay);
            }
            Err(e) => return Err(e),
        }
    }

    Err(LockError::AlreadyLocked)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TotpAlgorithm;

    /// Create a test config with default rate limit values
    fn test_config() -> Config {
        Config {
            database_url: "postgres://test:test@localhost/test".to_string(),
            clickhouse_url: "http://localhost:8123".to_string(),
            redis_url: "redis://localhost:6379".to_string(),
            jwt_secret: "test_secret_at_least_32_chars_long_for_testing".to_string(),
            jwt_issuer: "reiver".to_string(),
            jwt_expiration_hours: 24,
            kafka_hosts: "localhost:9092".to_string(),
            clickhouse_kafka_hosts: "localhost:9092".to_string(),
            kafka_exceptions_topic: "exceptions".to_string(),
            kafka_spans_topic: "spans".to_string(),
            kafka_logs_otlp_topic: "logs_otlp".to_string(),
            kafka_logs_unstructured_topic: "logs_unstructured".to_string(),
            kafka_llm_chunks_topic: "llm_chunks".to_string(),
            kafka_metrics_topic: "metrics".to_string(),
            kafka_sync_jobs_topic: "sync_jobs".to_string(),
            kafka_client_id: None,
            kafka_producer_linger_ms: 5,
            kafka_producer_max_retries: 3,
            kafka_message_timeout_ms: 30000,
            kafka_socket_timeout_ms: 60000,
            kafka_compression_codec: "lz4".to_string(),
            kafka_acks: "all".to_string(),
            cors_allowed_origins: vec!["*".to_string()],
            cors_allow_credentials: false,
            encryption_key: None,
            clickhouse_max_rows: 10000,
            clickhouse_default_limit: 100,
            rate_limit_analytics_per_minute: 240,
            rate_limit_analytics_per_hour: 1200,
            rate_limit_crud_per_minute: 480,
            rate_limit_crud_per_hour: 4800,
            rate_limit_billing_per_minute: 30,
            rate_limit_billing_per_hour: 120,
            rate_limit_gateway_per_minute: 60,
            rate_limit_gateway_per_hour: 600,
            rate_limit_external_api_per_minute: 30,
            rate_limit_external_api_per_hour: 300,
            rate_limit_ingestion_per_minute: 1000,
            rate_limit_ingestion_per_hour: 30000,
            cookie_domain: None,
            saml_time_skew_seconds: 60,
            totp_algorithm: TotpAlgorithm::Sha1,
            mfa_challenge_ttl_seconds: 180,
            session_ip_binding_enabled: false,
            session_user_agent_binding_enabled: false,
            base_url: "http://localhost:3000".to_string(),
            allow_signup: true,
            allow_password_login: true,
            stripe_api_key: None,
            stripe_webhook_secret: None,
            stripe_allowed_price_ids: vec![],
            stripe_metered_price_id: None,
            stripe_webhook_ip_allowlist_enabled: false,
            stripe_webhook_ip_allowlist: vec![],
            stripe_portal_return_url: "/settings/billing".to_string(),
            budget_alert_cooldown_hours: 24,
            // Gateway configuration
            gateway_fallback_enabled: true,
            gateway_max_retries: 2,
            gateway_initial_retry_delay_ms: 500,
            gateway_max_retry_delay_ms: 10_000,
            gateway_cache_enabled: false,
            gateway_cache_url: "http://localhost:8080".to_string(),
            gateway_cache_ttl_seconds: 86_400,
            gateway_log_content: false,
            gateway_timeout_seconds: 120,
            gateway_timeout_openai_seconds: 120,
            gateway_timeout_anthropic_seconds: 120,
            gateway_timeout_google_seconds: 120,
            gateway_timeout_bedrock_seconds: 180,
            gateway_timeout_theta_seconds: 120,
            gateway_timeout_deepseek_seconds: 120,
            gateway_default_openai_api_key: None,
            gateway_default_anthropic_api_key: None,
            gateway_default_google_api_key: None,
            gateway_default_theta_api_key: None,
            gateway_default_deepseek_api_key: None,
            gateway_anthropic_api_version: "2023-06-01".to_string(),
            gateway_openai_base_url: None,
            gateway_anthropic_base_url: None,
            gateway_google_base_url: None,
            gateway_theta_base_url: None,
            gateway_deepseek_base_url: None,
            // GitHub App integration
            github_app_id: None,
            github_app_name: None,
            github_app_private_key: None,
            github_app_webhook_secret: None,
            github_webhook_ip_allowlist: vec![],
            trusted_proxy_cidrs: vec![],
            api_base_url: None,
            slack_client_id: None,
            slack_client_secret: None,
            playground_evaluation_model: "gpt-4o-mini".to_string(),
            // Storage configuration
            storage_backend: "local".to_string(),
            storage_local_path: "/tmp/reiver-test-assets".to_string(),
            storage_local_base_url: "http://localhost:3000/assets".to_string(),
            storage_s3_bucket: None,
            storage_s3_region: "us-east-1".to_string(),
            storage_s3_endpoint: None,
            storage_s3_path_style: false,
            // NL query rate limits
            rate_limit_nl_query_per_minute: 10,
            rate_limit_nl_query_per_hour: 60,
            // Flow gateway
            flow_gateway_url: "http://localhost:8080".to_string(),
            // Social OAuth login
            oauth_google_client_id: None,
            oauth_google_client_secret: None,
            oauth_github_client_id: None,
            oauth_github_client_secret: None,
            oauth_microsoft_client_id: None,
            oauth_microsoft_client_secret: None,
            // Kafka pipeline events
            kafka_pipeline_events_topic: "pipeline_events".to_string(),
            kafka_platform_events_topic: "test.platform_events".to_string(),
            kafka_session_eval_jobs_topic: "test.session_eval_jobs".to_string(),
            // OpenTelemetry / profiling
            otel_exporter_endpoint: None,
            otel_project_id: None,
            profiling_enabled: false,
            profiling_frequency: 99,
            profiling_cpu_interval_secs: 600,
            profiling_heap_interval_secs: 600,
            slack_signing_secret: None,
            app_url: None,
            credits_enabled: false,
            gateway_ai21_base_url: None,
            gateway_sambanova_base_url: None,
            gateway_lambda_base_url: None,
            gateway_lepton_base_url: None,
            gateway_hyperbolic_base_url: None,
            gateway_ovhcloud_base_url: None,
            gateway_novita_base_url: None,
            gateway_huggingface_base_url: None,
            gateway_cloudflare_base_url: None,
            gateway_azure_openai_base_url: None,
            gateway_vertex_ai_base_url: None,
            gateway_xai_base_url: None,
            gateway_mistral_base_url: None,
            gateway_together_base_url: None,
            gateway_fireworks_base_url: None,
            gateway_perplexity_base_url: None,
            gateway_cohere_base_url: None,
            gateway_openrouter_base_url: None,
            gateway_cerebras_base_url: None,
            gateway_deepinfra_base_url: None,
            gateway_alibaba_base_url: None,
            gateway_nvidia_base_url: None,
            gateway_groq_base_url: None,
            gateway_timeout_openai_compat_seconds: 120,
            loops_api_key: None,
            loops_invite_template_id: None,
            loops_alert_template_id: None,
            loops_welcome_template_id: None,
        }
    }

    #[test]
    fn test_rate_limit_type_analytics_limits() {
        let config = test_config();
        let limit_type = RateLimitType::Analytics;
        assert_eq!(limit_type.limits_per_minute(&config), 240);
        assert_eq!(limit_type.limits_per_hour(&config), 1_200);
    }

    #[test]
    fn test_rate_limit_type_crud_limits() {
        let config = test_config();
        let limit_type = RateLimitType::Crud;
        assert_eq!(limit_type.limits_per_minute(&config), 480);
        assert_eq!(limit_type.limits_per_hour(&config), 4_800);
    }

    #[test]
    fn test_crud_has_higher_limits_than_analytics() {
        let config = test_config();
        assert!(
            RateLimitType::Crud.limits_per_minute(&config)
                > RateLimitType::Analytics.limits_per_minute(&config)
        );
        assert!(
            RateLimitType::Crud.limits_per_hour(&config)
                > RateLimitType::Analytics.limits_per_hour(&config)
        );
    }

    #[test]
    fn test_rate_limit_info_structure() {
        let info = RateLimitInfo {
            current: 50,
            limit: 100,
            reset_at: Utc::now() + chrono::Duration::seconds(30),
            dropped: 0,
        };

        assert_eq!(info.current, 50);
        assert_eq!(info.limit, 100);
        assert_eq!(info.dropped, 0);
        assert!(info.reset_at > Utc::now());
    }

    #[test]
    fn test_rate_limit_info_over_limit() {
        let info = RateLimitInfo {
            current: 150,
            limit: 100,
            reset_at: Utc::now() + chrono::Duration::seconds(30),
            dropped: 50,
        };

        assert!(info.current > info.limit);
        assert_eq!(info.dropped, 50);
    }

    #[test]
    fn test_rate_limit_info_serialization() {
        let info = RateLimitInfo {
            current: 50,
            limit: 100,
            reset_at: Utc::now(),
            dropped: 0,
        };

        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("\"current\":50"));
        assert!(json.contains("\"limit\":100"));
        assert!(json.contains("\"dropped\":0"));
    }

    #[test]
    fn test_rate_limit_info_deserialization() {
        let json = r#"{"current":75,"limit":100,"reset_at":"2024-01-15T10:00:00Z","dropped":0}"#;
        let info: RateLimitInfo = serde_json::from_str(json).unwrap();

        assert_eq!(info.current, 75);
        assert_eq!(info.limit, 100);
        assert_eq!(info.dropped, 0);
    }

    #[test]
    fn test_analytics_hour_limit_is_5x_minute() {
        let config = test_config();
        let limit_type = RateLimitType::Analytics;
        // PostHog uses 5x for hour vs minute
        assert_eq!(
            limit_type.limits_per_hour(&config),
            limit_type.limits_per_minute(&config) * 5
        );
    }

    #[test]
    fn test_crud_hour_limit_is_10x_minute() {
        let config = test_config();
        let limit_type = RateLimitType::Crud;
        // PostHog uses 10x for hour vs minute for CRUD
        assert_eq!(
            limit_type.limits_per_hour(&config),
            limit_type.limits_per_minute(&config) * 10
        );
    }

    // Test helper for calculating dropped requests
    fn calculate_dropped(current: i32, limit: i32) -> i32 {
        if current > limit {
            current - limit
        } else {
            0
        }
    }

    #[test]
    fn test_calculate_dropped_under_limit() {
        assert_eq!(calculate_dropped(50, 100), 0);
        assert_eq!(calculate_dropped(100, 100), 0);
    }

    #[test]
    fn test_calculate_dropped_over_limit() {
        assert_eq!(calculate_dropped(101, 100), 1);
        assert_eq!(calculate_dropped(150, 100), 50);
    }

    #[test]
    fn test_redis_key_format() {
        let config = test_config();
        let user_id = uuid::Uuid::new_v4();
        let limit_type = RateLimitType::Analytics;

        let key_min = format!(
            "rate_limit:min:{}:{}",
            limit_type.limits_per_minute(&config),
            user_id
        );
        let key_hour = format!(
            "rate_limit:hour:{}:{}",
            limit_type.limits_per_hour(&config),
            user_id
        );

        assert!(key_min.starts_with("rate_limit:min:240:"));
        assert!(key_hour.starts_with("rate_limit:hour:1200:"));
        assert!(key_min.contains(&user_id.to_string()));
    }

    // --- NlQuery rate limit tests ---

    #[test]
    fn test_rate_limit_type_nl_query_limits() {
        let config = test_config();
        let limit_type = RateLimitType::NlQuery;
        assert_eq!(limit_type.limits_per_minute(&config), 10);
        assert_eq!(limit_type.limits_per_hour(&config), 60);
    }

    #[test]
    fn test_nl_query_has_lowest_limits() {
        let config = test_config();
        let nl_per_min = RateLimitType::NlQuery.limits_per_minute(&config);
        // NlQuery should have the lowest per-minute limit of all types
        assert!(nl_per_min <= RateLimitType::Analytics.limits_per_minute(&config));
        assert!(nl_per_min <= RateLimitType::Crud.limits_per_minute(&config));
        assert!(nl_per_min <= RateLimitType::Billing.limits_per_minute(&config));
        assert!(nl_per_min <= RateLimitType::Gateway.limits_per_minute(&config));
        assert!(nl_per_min <= RateLimitType::ExternalApi.limits_per_minute(&config));
    }

    #[test]
    fn test_nl_query_hour_limit_is_6x_minute() {
        let config = test_config();
        let limit_type = RateLimitType::NlQuery;
        // NlQuery: 10/min, 60/hour -> 6x ratio
        assert_eq!(
            limit_type.limits_per_hour(&config),
            limit_type.limits_per_minute(&config) * 6
        );
    }
}
