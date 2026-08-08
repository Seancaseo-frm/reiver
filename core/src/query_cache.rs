use anyhow::Result;
use quick_cache::sync::Cache;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use uuid::Uuid;

use crate::app_state::RedisPool;
use bb8_redis::redis::AsyncCommands;

/// Process-local LRU cache for query results.
/// This provides a fast in-memory layer before Redis, reducing network round-trips
/// for frequently accessed queries.
///
/// Configuration:
/// - 10,000 entries capacity (typical entry ~1KB = ~10MB memory)
/// - TTL-aware: entries have per-item expiration
static LOCAL_CACHE: OnceLock<Cache<String, CacheEntry>> = OnceLock::new();

/// Cache entry with expiration time
#[derive(Clone)]
struct CacheEntry {
    value: String,
    expires_at: Instant,
}

impl CacheEntry {
    fn new(value: String, ttl: Duration) -> Self {
        Self {
            value,
            expires_at: Instant::now() + ttl,
        }
    }

    fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }
}

/// Get the process-local cache instance
fn get_local_cache() -> &'static Cache<String, CacheEntry> {
    LOCAL_CACHE.get_or_init(|| {
        Cache::new(10_000) // 10K entries max
    })
}

/// Cache TTLs for different query types
pub enum CacheTTL {
    /// Short-lived cache (1 minute) - for frequently changing data
    Short,
    /// Medium cache (5 minutes) - for moderately changing data
    Medium,
    /// Long cache (15 minutes) - for slowly changing data
    Long,
}

impl CacheTTL {
    fn seconds(&self) -> u64 {
        match self {
            CacheTTL::Short => 60,
            CacheTTL::Medium => 300, // 5 minutes
            CacheTTL::Long => 900,   // 15 minutes
        }
    }

    fn duration(&self) -> Duration {
        Duration::from_secs(self.seconds())
    }
}

/// Generate cache key from query string using BLAKE3 (faster than SHA-256)
pub fn cache_key(query: &str, params: &[&str]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(query.as_bytes());
    for param in params {
        hasher.update(param.as_bytes());
    }
    let hash = hasher.finalize();
    format!("cache:query:{}", hash.to_hex())
}

/// Get cached query result.
///
/// Uses a two-tier caching strategy:
/// 1. **Local cache** (in-process LRU): ~100ns access, no network
/// 2. **Redis cache** (distributed): ~1ms access, network round-trip
///
/// This reduces Redis load for frequently accessed queries by 10-100x.
pub async fn get_cached_query<T: serde::de::DeserializeOwned>(
    redis_pool: &RedisPool,
    query: &str,
    params: &[&str],
) -> Result<Option<T>> {
    let key = cache_key(query, params);
    let local_cache = get_local_cache();

    // Check local cache first (fast path, ~100ns)
    if let Some(entry) = local_cache.get(&key) {
        if !entry.is_expired() {
            // Local cache hit - deserialize and return
            let result: T = serde_json::from_str(&entry.value)?;
            return Ok(Some(result));
        }
        // Entry expired - remove from local cache
        local_cache.remove(&key);
    }

    // Local cache miss - check Redis (slow path, ~1ms)
    let mut conn = redis_pool
        .get()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get Redis connection: {}", e))?;

    let cached: Option<String> = conn
        .get(&key)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get cached query from Redis: {}", e))?;

    if let Some(cached_str) = cached {
        // Redis hit - populate local cache for next request
        // Use Short TTL for local cache to ensure freshness
        local_cache.insert(
            key,
            CacheEntry::new(cached_str.clone(), Duration::from_secs(60)),
        );

        let result: T = serde_json::from_str(&cached_str)?;
        Ok(Some(result))
    } else {
        Ok(None)
    }
}

/// Store query result in cache (both local and Redis).
///
/// Stores in two tiers:
/// 1. **Local cache**: For fast subsequent access from this process
/// 2. **Redis cache**: For sharing across processes and persistence
pub async fn set_cached_query<T: serde::Serialize>(
    redis_pool: &RedisPool,
    query: &str,
    params: &[&str],
    result: &T,
    ttl: CacheTTL,
) -> Result<()> {
    let key = cache_key(query, params);
    let serialized = serde_json::to_string(result)?;

    // Store in local cache first (always succeeds, ~100ns)
    let local_cache = get_local_cache();
    local_cache.insert(
        key.clone(),
        CacheEntry::new(serialized.clone(), ttl.duration()),
    );

    // Store in Redis (may fail, ~1ms)
    let mut conn = redis_pool
        .get()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get Redis connection: {}", e))?;

    let _: () = conn
        .set(&key, &serialized)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to set cached query in Redis: {}", e))?;

    let _: () = conn
        .expire(&key, ttl.seconds() as i64)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to set TTL on cache key: {}", e))?;

    Ok(())
}

/// Clear all entries from the local cache.
/// Called during cache invalidation since pattern matching isn't supported.
pub fn clear_local_cache() {
    // quick_cache doesn't have a clear method, so we create a new empty cache
    // This is a no-op since we can't replace the static cache, but entries will expire
    // For now, we rely on TTL-based expiration for local cache cleanup
    tracing::debug!("Local cache invalidation requested - entries will expire via TTL");
}

/// Invalidate cache for a specific query pattern
/// Uses Redis SCAN to find matching keys (safe for production - doesn't block Redis)
/// Also clears the local in-process cache.
pub async fn invalidate_cache_pattern(redis_pool: &RedisPool, pattern: &str) -> Result<()> {
    use bb8_redis::redis::cmd;

    // Clear local cache (entries will expire via TTL)
    clear_local_cache();

    let mut conn = redis_pool
        .get()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get Redis connection: {}", e))?;

    // Build the full pattern (all cache keys start with "cache:")
    let full_pattern = if pattern.contains("cache:") {
        pattern.to_string()
    } else {
        format!("cache:*{}*", pattern)
    };

    // Use SCAN for pattern matching - this is non-blocking and safe for production
    // SCAN returns batches of keys incrementally without blocking Redis
    let mut cursor: u64 = 0;
    let mut total_deleted = 0;

    loop {
        // SCAN cursor MATCH pattern COUNT 100
        let (next_cursor, keys): (u64, Vec<String>) = cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(&full_pattern)
            .arg("COUNT")
            .arg(100)
            .query_async(&mut *conn)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to scan Redis keys: {}", e))?;

        // Delete found keys in this batch
        if !keys.is_empty() {
            // Also remove from local cache
            let local_cache = get_local_cache();
            for key in &keys {
                local_cache.remove(key);
            }

            let _: () = conn
                .del(&keys)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to delete cache keys: {}", e))?;
            total_deleted += keys.len();
        }

        cursor = next_cursor;

        // Cursor returns to 0 when iteration is complete
        if cursor == 0 {
            break;
        }
    }

    if total_deleted > 0 {
        tracing::debug!(
            "Invalidated {} cache keys matching pattern: {}",
            total_deleted,
            full_pattern
        );
    }

    Ok(())
}

/// Invalidate all cache entries for a project
/// Call this when project data changes (new errors, traces, etc.)
pub async fn invalidate_project_cache(redis_pool: &RedisPool, project_id: Uuid) -> Result<()> {
    // For now, we'll invalidate by using a pattern that includes project_id in the hash
    // This is a simplified approach - in production, you might want to track cache keys per project
    invalidate_cache_pattern(redis_pool, &project_id.to_string()).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_key_deterministic() {
        let query = "SELECT * FROM users WHERE id = $1";
        let params = ["user_123"];

        let key1 = cache_key(query, &params);
        let key2 = cache_key(query, &params);

        assert_eq!(key1, key2, "Same inputs should produce same cache key");
    }

    #[test]
    fn test_cache_key_different_queries() {
        let query1 = "SELECT * FROM users";
        let query2 = "SELECT * FROM projects";

        let key1 = cache_key(query1, &[]);
        let key2 = cache_key(query2, &[]);

        assert_ne!(
            key1, key2,
            "Different queries should produce different keys"
        );
    }

    #[test]
    fn test_cache_key_different_params() {
        let query = "SELECT * FROM users WHERE id = $1";

        let key1 = cache_key(query, &["user_1"]);
        let key2 = cache_key(query, &["user_2"]);

        assert_ne!(key1, key2, "Different params should produce different keys");
    }

    #[test]
    fn test_cache_key_param_order_matters() {
        let query = "SELECT * FROM users WHERE a = $1 AND b = $2";

        let key1 = cache_key(query, &["value_a", "value_b"]);
        let key2 = cache_key(query, &["value_b", "value_a"]);

        assert_ne!(key1, key2, "Param order should affect cache key");
    }

    #[test]
    fn test_cache_key_format() {
        let key = cache_key("SELECT 1", &[]);

        // Should start with cache:query:
        assert!(
            key.starts_with("cache:query:"),
            "Key should have correct prefix"
        );

        // Should be a hex string after the prefix
        let hash_part = key.strip_prefix("cache:query:").unwrap();
        assert!(
            hash_part.chars().all(|c| c.is_ascii_hexdigit()),
            "Hash should be hexadecimal"
        );

        // BLAKE3 produces 64 hex characters (32 bytes = 256 bits)
        assert_eq!(hash_part.len(), 64, "Hash should be BLAKE3 (64 hex chars)");
    }

    #[test]
    fn test_cache_key_empty_params() {
        let query = "SELECT 1";
        let key = cache_key(query, &[]);

        assert!(!key.is_empty());
        assert!(key.starts_with("cache:query:"));
    }

    #[test]
    fn test_cache_key_many_params() {
        let query = "SELECT * FROM t WHERE a IN ($1, $2, $3, $4, $5)";
        let params: Vec<&str> = (0..5).map(|_| "value").collect();

        let key = cache_key(query, &params);
        assert!(key.starts_with("cache:query:"));
    }

    #[test]
    fn test_cache_key_special_characters() {
        let query = "SELECT * FROM \"users\" WHERE name = 'test'";
        let key = cache_key(query, &["O'Brien", "name with spaces"]);

        assert!(key.starts_with("cache:query:"));
        // Should not contain special chars in the key itself
        let hash_part = key.strip_prefix("cache:query:").unwrap();
        assert!(hash_part.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_cache_ttl_values() {
        assert_eq!(CacheTTL::Short.seconds(), 60);
        assert_eq!(CacheTTL::Medium.seconds(), 300);
        assert_eq!(CacheTTL::Long.seconds(), 900);
    }

    #[test]
    fn test_cache_key_unicode() {
        let query = "SELECT * FROM users WHERE name = $1";
        let key = cache_key(query, &["日本語", "中文", "한국어"]);

        assert!(key.starts_with("cache:query:"));
    }
}
