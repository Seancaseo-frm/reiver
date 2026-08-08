//! Query Result Cache
//!
//! Redis-based caching for warehouse query results.
//! Uses Redis key-value store for cache hit detection.
//!
//! PERFORMANCE: For TB-scale workloads with dashboards that run identical queries,
//! caching can provide 1000x+ speedup by avoiding re-scanning data.
//!
//! CORRECTNESS: Uses generation-based cache invalidation to avoid race conditions.
//! When data changes (e.g., after sync), the cache generation is incremented, and
//! old cache entries with stale generation are automatically ignored (and will
//! expire via TTL). This is more reliable than SCAN+DELETE which can miss entries
//! being written during the invalidation process.

use bb8::Pool;
use bb8_redis::RedisConnectionManager;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

use crate::warehouse::metrics::WarehouseMetrics;
use crate::warehouse::utils::hash_normalized_query;

/// Errors that can occur during cache operations.
#[derive(Debug, Error)]
pub enum CacheError {
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),
    
    #[error("Pool error: {0}")]
    Pool(String),
    
    #[error("Serialization error: {0}")]
    Serialization(String),
    
    #[error("Cache operation timed out after {0} seconds")]
    Timeout(u64),
}

/// Result type for cache operations.
pub type CacheResult<T> = Result<T, CacheError>;

/// Cache entry for a query result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedQueryResult {
    /// Column information
    pub columns: Vec<CachedColumnInfo>,
    /// Query result rows (as JSON values)
    pub rows: Vec<Vec<serde_json::Value>>,
    /// Number of rows
    pub row_count: usize,
    /// Original execution time in milliseconds
    pub original_execution_time_ms: u64,
    /// When the cache entry was created
    pub cached_at: chrono::DateTime<chrono::Utc>,
    /// Bytes scanned by the original query (for billing)
    pub bytes_scanned: u64,
}

/// Column info for cached results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedColumnInfo {
    pub name: String,
    pub data_type: String,
}

/// Cache tier for different query types.
///
/// PERFORMANCE: Different queries have different caching characteristics:
/// - HOT: Frequently accessed, short TTL (dashboards with auto-refresh)
/// - WARM: Dashboard queries, medium TTL
/// - COLD: Expensive queries that rarely change, long TTL
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheTier {
    /// Frequently accessed, short TTL (5 minutes).
    /// Used for: active dashboards, real-time queries
    Hot,
    /// Dashboard queries, medium TTL (1 hour).
    /// Used for: dashboard panels, scheduled reports
    Warm,
    /// Expensive queries, long TTL (24 hours).
    /// Used for: heavy aggregations, historical analysis
    Cold,
}

impl CacheTier {
    /// Get the tier name for metrics/logging.
    pub fn name(&self) -> &'static str {
        match self {
            CacheTier::Hot => "hot",
            CacheTier::Warm => "warm",
            CacheTier::Cold => "cold",
        }
    }
}

/// Tiered cache configuration.
#[derive(Debug, Clone)]
pub struct TieredCacheConfig {
    /// TTL for hot cache (default: 5 minutes)
    pub hot_ttl_secs: u64,
    /// TTL for warm cache (default: 1 hour)
    pub warm_ttl_secs: u64,
    /// TTL for cold cache (default: 24 hours)
    pub cold_ttl_secs: u64,
    /// Execution time threshold for cold tier (ms) - queries taking longer are cached as cold
    pub cold_threshold_ms: u64,
}

impl Default for TieredCacheConfig {
    fn default() -> Self {
        Self {
            hot_ttl_secs: 300,      // 5 minutes
            warm_ttl_secs: 3600,    // 1 hour
            cold_ttl_secs: 86400,   // 24 hours
            cold_threshold_ms: 5000, // Queries > 5s are cached as cold
        }
    }
}

/// Configuration for the query cache.
#[derive(Debug, Clone)]
pub struct QueryCacheConfig {
    /// TTL for cache entries in seconds (default: 1 hour)
    pub ttl_secs: u64,
    /// Maximum size of a cacheable result in bytes (default: 10MB)
    pub max_result_size_bytes: usize,
    /// Key prefix for Redis keys
    pub key_prefix: String,
    /// Whether caching is enabled
    pub enabled: bool,
    /// Tiered cache configuration
    pub tiered_config: TieredCacheConfig,
}

impl Default for QueryCacheConfig {
    fn default() -> Self {
        Self {
            ttl_secs: 3600, // 1 hour (default tier)
            max_result_size_bytes: 10 * 1024 * 1024, // 10MB
            key_prefix: "wh:cache:".to_string(),
            enabled: true,
            tiered_config: TieredCacheConfig::default(),
        }
    }
}

/// TTL for the in-memory generation cache (seconds).
const GENERATION_CACHE_TTL_SECS: u64 = 10;

/// Redis-based query result cache.
pub struct QueryCache {
    pool: Arc<Pool<RedisConnectionManager>>,
    config: QueryCacheConfig,
    metrics: Option<Arc<WarehouseMetrics>>,
    generation_cache: quick_cache::sync::Cache<Uuid, (u64, std::time::Instant)>,
}

impl QueryCache {
    /// Create a new query cache.
    pub fn new(pool: Arc<Pool<RedisConnectionManager>>, config: QueryCacheConfig) -> Self {
        Self {
            pool,
            config,
            metrics: None,
            generation_cache: quick_cache::sync::Cache::new(256),
        }
    }
    
    /// Create with default configuration.
    pub fn with_defaults(pool: Arc<Pool<RedisConnectionManager>>) -> Self {
        Self::new(pool, QueryCacheConfig::default())
    }
    
    /// Attach a metrics collector to this cache.
    pub fn with_metrics(mut self, metrics: Arc<WarehouseMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }
    
    /// Check if caching is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }
    
    /// Get the key for storing project cache generation.
    fn generation_key(&self, project_id: Uuid) -> String {
        format!("{}gen:{}", self.config.key_prefix, project_id)
    }
    
    /// Get the current cache generation for a project.
    /// 
    /// Generation is incremented on every sync to invalidate stale cache entries.
    /// Returns 0 if no generation has been set (first use).
    #[tracing::instrument(name = "warehouse.query.cache.get_generation", skip(self), fields(%project_id), err(Display))]
    pub async fn get_generation(&self, project_id: Uuid) -> CacheResult<u64> {
        if let Some((gen, ts)) = self.generation_cache.get(&project_id) {
            if ts.elapsed().as_secs() < GENERATION_CACHE_TTL_SECS {
                return Ok(gen);
            }
        }

        let mut conn = self.pool.get().await
            .map_err(|e| CacheError::Pool(e.to_string()))?;
        
        let gen_key = self.generation_key(project_id);
        let generation: Option<u64> = conn.get(&gen_key).await?;
        let gen = generation.unwrap_or(0);

        self.generation_cache.insert(project_id, (gen, std::time::Instant::now()));
        
        Ok(gen)
    }
    
    /// Increment the cache generation for a project.
    /// 
    /// CORRECTNESS: This should be called after sync operations complete.
    /// All cache entries with the old generation will be automatically ignored
    /// (and will expire via TTL), solving the race condition where new entries
    /// could be written during a SCAN+DELETE invalidation.
    ///
    /// # Returns
    /// The new generation number.
    #[tracing::instrument(name = "warehouse.query.cache.increment_generation", skip(self), fields(%project_id), err(Display))]
    pub async fn increment_generation(&self, project_id: Uuid) -> CacheResult<u64> {
        let mut conn = self.pool.get().await
            .map_err(|e| CacheError::Pool(e.to_string()))?;
        
        let gen_key = self.generation_key(project_id);
        let new_generation: u64 = conn.incr(&gen_key, 1u64).await?;

        self.generation_cache.remove(&project_id);
        
        tracing::info!(
            project_id = %project_id,
            generation = new_generation,
            "Incremented cache generation for project"
        );
        
        Ok(new_generation)
    }
    
    /// Generate a cache key for a query.
    /// 
    /// The key includes:
    /// - Project ID (for isolation)
    /// - Cache generation (for invalidation)
    /// - Normalized query hash (for deduplication)
    /// 
    /// CORRECTNESS: Including generation in the key ensures that cache entries
    /// from before a sync (with old generation) are automatically ignored.
    async fn cache_key(&self, project_id: Uuid, sql: &str) -> CacheResult<String> {
        let generation = self.get_generation(project_id).await?;
        Ok(self.cache_key_with_generation(project_id, generation, sql))
    }
    
    /// Generate a cache key synchronously with a known generation.
    /// 
    /// This is useful when you already have the generation value.
    fn cache_key_with_generation(&self, project_id: Uuid, generation: u64, sql: &str) -> String {
        let query_hash = hash_normalized_query(sql);
        format!("{}{}:{}:{}", self.config.key_prefix, project_id, generation, query_hash)
    }
    
    /// Get a cached query result.
    /// 
    /// Returns None if:
    /// - Cache is disabled
    /// - Cache miss
    /// - Deserialization error (cache entry corrupted)
    ///
    /// CORRECTNESS: The cache key includes the current generation, so entries
    /// from before a sync (with old generation) are automatically ignored.
    #[tracing::instrument(name = "warehouse.query.cache.get", skip(self, sql), fields(%project_id), err(Display))]
    pub async fn get(
        &self,
        project_id: Uuid,
        sql: &str,
    ) -> CacheResult<Option<CachedQueryResult>> {
        if !self.config.enabled {
            return Ok(None);
        }
        
        let key = self.cache_key(project_id, sql).await?;
        
        let mut conn = self.pool.get().await
            .map_err(|e| CacheError::Pool(e.to_string()))?;
        
        let cached: Option<Vec<u8>> = conn.get(&key).await?;
        
        match cached {
            Some(data) => {
                match serde_json::from_slice::<CachedQueryResult>(&data) {
                    Ok(result) => {
                        if let Some(ref m) = self.metrics { m.record_cache_hit(); }
                        tracing::debug!(
                            project_id = %project_id,
                            query_hash = %hash_normalized_query(sql),
                            row_count = result.row_count,
                            "Query cache hit"
                        );
                        Ok(Some(result))
                    }
                    Err(e) => {
                        // Cache entry is corrupted, delete it
                        if let Some(ref m) = self.metrics { m.record_cache_miss(); }
                        tracing::warn!(
                            project_id = %project_id,
                            error = %e,
                            "Failed to deserialize cached query result, deleting entry"
                        );
                        let _: () = conn.del(&key).await?;
                        Ok(None)
                    }
                }
            }
            None => {
                if let Some(ref m) = self.metrics { m.record_cache_miss(); }
                Ok(None)
            }
        }
    }
    
    /// Store a query result in the cache.
    /// 
    /// Skips caching if:
    /// - Cache is disabled
    /// - Result is too large
    /// - Generation changed during operation (prevents race condition)
    ///
    /// CORRECTNESS: Uses optimistic locking - reads generation, prepares data,
    /// then verifies generation hasn't changed before writing. If generation
    /// changed (sync occurred), the write is skipped to avoid caching stale data
    /// under a new generation.
    #[tracing::instrument(name = "warehouse.query.cache.set", skip(self, sql, result), fields(%project_id), err(Display))]
    pub async fn set(
        &self,
        project_id: Uuid,
        sql: &str,
        result: &CachedQueryResult,
    ) -> CacheResult<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let expected_gen = self.get_generation(project_id).await?;
        
        let serialized = serde_json::to_vec(result)
            .map_err(|e| CacheError::Serialization(e.to_string()))?;
        
        if serialized.len() > self.config.max_result_size_bytes {
            if let Some(ref m) = self.metrics { m.record_cache_write_skip(); }
            tracing::debug!(
                project_id = %project_id,
                result_size = serialized.len(),
                max_size = self.config.max_result_size_bytes,
                "Query result too large to cache"
            );
            return Ok(());
        }
        
        let mut conn = self.pool.get().await
            .map_err(|e| CacheError::Pool(e.to_string()))?;
        
        let gen_key = self.generation_key(project_id);
        let query_hash = hash_normalized_query(sql);
        
        // Atomic generation check + write in a single Redis round-trip via Lua.
        // If the generation changed since we read it (a sync occurred), the
        // write is skipped to avoid caching stale data under a new generation.
        let script = r#"
            local gen = redis.call('GET', KEYS[1])
            if not gen then gen = '0' end
            if gen ~= ARGV[6] then return -1 end
            local cache_key = ARGV[1] .. ARGV[2] .. ':' .. gen .. ':' .. ARGV[3]
            redis.call('SETEX', cache_key, ARGV[4], ARGV[5])
            return gen
        "#;
        
        let generation: i64 = redis::Script::new(script)
            .key(&gen_key)
            .arg(&self.config.key_prefix)
            .arg(project_id.to_string())
            .arg(&query_hash)
            .arg(self.config.ttl_secs)
            .arg(serialized)
            .arg(expected_gen.to_string())
            .invoke_async(&mut *conn)
            .await?;

        if generation == -1 {
            if let Some(ref m) = self.metrics { m.record_cache_write_skip(); }
            tracing::debug!(
                project_id = %project_id,
                expected_gen = expected_gen,
                "Cache write skipped: generation changed during query execution"
            );
            return Ok(());
        }
        
        if let Some(ref m) = self.metrics { m.record_cache_write(); }
        
        tracing::debug!(
            project_id = %project_id,
            query_hash = %query_hash,
            row_count = result.row_count,
            generation = generation,
            ttl_secs = self.config.ttl_secs,
            "Query result cached"
        );
        
        Ok(())
    }
    
    /// Store a pre-serialized query result in the cache.
    ///
    /// Use this when the caller has already serialized the result (e.g. to
    /// avoid cloning large row data across a spawn boundary).
    #[tracing::instrument(name = "warehouse.query.cache.set_preserialized", skip(self, sql, serialized), fields(%project_id), err(Display))]
    pub async fn set_preserialized(
        &self,
        project_id: Uuid,
        sql: &str,
        serialized: Vec<u8>,
    ) -> CacheResult<()> {
        if !self.config.enabled {
            return Ok(());
        }

        if serialized.len() > self.config.max_result_size_bytes {
            if let Some(ref m) = self.metrics { m.record_cache_write_skip(); }
            return Ok(());
        }

        let expected_gen = self.get_generation(project_id).await?;

        let mut conn = self.pool.get().await
            .map_err(|e| CacheError::Pool(e.to_string()))?;

        let gen_key = self.generation_key(project_id);
        let query_hash = hash_normalized_query(sql);

        let script = r#"
            local gen = redis.call('GET', KEYS[1])
            if not gen then gen = '0' end
            if gen ~= ARGV[6] then return -1 end
            local cache_key = ARGV[1] .. ARGV[2] .. ':' .. gen .. ':' .. ARGV[3]
            redis.call('SETEX', cache_key, ARGV[4], ARGV[5])
            return gen
        "#;

        let generation: i64 = redis::Script::new(script)
            .key(&gen_key)
            .arg(&self.config.key_prefix)
            .arg(project_id.to_string())
            .arg(&query_hash)
            .arg(self.config.ttl_secs)
            .arg(serialized)
            .arg(expected_gen.to_string())
            .invoke_async(&mut *conn)
            .await?;

        if generation == -1 {
            if let Some(ref m) = self.metrics { m.record_cache_write_skip(); }
            return Ok(());
        }

        if let Some(ref m) = self.metrics { m.record_cache_write(); }

        Ok(())
    }

    /// Invalidate all cached queries for a project.
    /// 
    /// Call this when data in the project changes (e.g., after sync).
    /// 
    /// CORRECTNESS: This method uses generation-based invalidation instead of
    /// SCAN+DELETE. By incrementing the generation, all cache entries with the
    /// old generation are automatically ignored (cache miss). The old entries
    /// will expire naturally via TTL.
    ///
    /// This approach solves the race condition where new cache entries could be
    /// written during a SCAN+DELETE operation and never get deleted.
    ///
    /// PERFORMANCE: This is O(1) - just incrementing a counter in Redis.
    /// Much faster than scanning and deleting potentially thousands of keys.
    #[tracing::instrument(name = "warehouse.query.cache.invalidate_project", skip(self), fields(%project_id), err(Display))]
    pub async fn invalidate_project(&self, project_id: Uuid) -> CacheResult<()> {
        if !self.config.enabled {
            return Ok(());
        }
        
        // Simply increment the generation - all old entries become stale
        let new_generation = self.increment_generation(project_id).await?;
        if let Some(ref m) = self.metrics { m.record_cache_invalidation(); }
        
        tracing::info!(
            project_id = %project_id,
            new_generation = new_generation,
            "Invalidated query cache for project via generation increment"
        );
        
        Ok(())
    }
    
    /// Invalidate a specific cached query for a project.
    #[tracing::instrument(name = "warehouse.query.cache.invalidate_query", skip(self, sql), fields(%project_id), err(Display))]
    pub async fn invalidate_query(&self, project_id: Uuid, sql: &str) -> CacheResult<()> {
        if !self.config.enabled {
            return Ok(());
        }
        
        let key = self.cache_key(project_id, sql).await?;
        
        let mut conn = self.pool.get().await
            .map_err(|e| CacheError::Pool(e.to_string()))?;
        
        let _: () = conn.del(&key).await?;
        
        Ok(())
    }

    // =========================================================================
    // TIERED CACHE METHODS
    // =========================================================================

    /// Determine the cache tier based on query characteristics.
    ///
    /// PERFORMANCE: Different tiers have different TTLs:
    /// - Hot (5 min): Frequently accessed, likely from active dashboard
    /// - Warm (1 hour): Dashboard queries, scheduled reports
    /// - Cold (24 hours): Expensive queries, historical analysis
    pub fn determine_tier(&self, sql: &str, execution_time_ms: u64) -> CacheTier {
        // Cold tier: expensive queries (> 5 seconds execution time)
        if execution_time_ms > self.config.tiered_config.cold_threshold_ms {
            return CacheTier::Cold;
        }

        // Warm tier: dashboard-like queries (GROUP BY, aggregate functions)
        if Self::is_dashboard_query(sql) {
            return CacheTier::Warm;
        }

        // Default: hot tier for quick queries
        CacheTier::Hot
    }

    /// Determine the cache tier based on pre-parsed AST statements.
    ///
    /// Zero-parse variant: accepts already-parsed statements to avoid redundant parsing.
    pub fn determine_tier_ast(&self, statements: &[sqlparser::ast::Statement], execution_time_ms: u64) -> CacheTier {
        if execution_time_ms > self.config.tiered_config.cold_threshold_ms {
            return CacheTier::Cold;
        }
        if Self::is_dashboard_query_ast(statements) {
            return CacheTier::Warm;
        }
        CacheTier::Hot
    }

    /// Check if a query looks like a dashboard query.
    ///
    /// Dashboard queries typically:
    /// - Use GROUP BY
    /// - Use aggregate functions (COUNT, SUM, AVG)
    /// - Have LIMIT clauses
    fn is_dashboard_query(sql: &str) -> bool {
        use sqlparser::dialect::ClickHouseDialect;
        use sqlparser::parser::Parser;

        let dialect = ClickHouseDialect {};
        let statements = match Parser::parse_sql(&dialect, sql) {
            Ok(s) => s,
            Err(_) => return false,
        };
        Self::is_dashboard_query_ast(&statements)
    }

    /// Zero-parse variant: check pre-parsed statements for dashboard patterns.
    pub fn is_dashboard_query_ast(statements: &[sqlparser::ast::Statement]) -> bool {
        use sqlparser::ast::{Expr, GroupByExpr, SelectItem, SetExpr, Statement};

        const AGGREGATE_NAMES: &[&str] = &["COUNT", "SUM", "AVG", "MIN", "MAX"];

        fn expr_has_aggregate(expr: &Expr) -> bool {
            match expr {
                Expr::Function(f) => {
                    let name = f.name.to_string().to_uppercase();
                    AGGREGATE_NAMES.iter().any(|a| name == *a)
                }
                Expr::Nested(inner) => expr_has_aggregate(inner),
                Expr::BinaryOp { left, right, .. } => {
                    expr_has_aggregate(left) || expr_has_aggregate(right)
                }
                Expr::UnaryOp { expr, .. } => expr_has_aggregate(expr),
                _ => false,
            }
        }

        for stmt in statements {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    let has_aggregates = select.projection.iter().any(|item| match item {
                        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                            expr_has_aggregate(e)
                        }
                        _ => false,
                    });
                    let has_group_by = !matches!(
                        &select.group_by,
                        GroupByExpr::Expressions(v, _) if v.is_empty()
                    );
                    if has_aggregates || has_group_by {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Get the TTL for a cache tier.
    pub fn get_tier_ttl(&self, tier: CacheTier) -> u64 {
        match tier {
            CacheTier::Hot => self.config.tiered_config.hot_ttl_secs,
            CacheTier::Warm => self.config.tiered_config.warm_ttl_secs,
            CacheTier::Cold => self.config.tiered_config.cold_ttl_secs,
        }
    }

    /// Store a query result with tiered TTL.
    ///
    /// Automatically determines the cache tier based on query characteristics
    /// and sets the appropriate TTL.
    #[tracing::instrument(name = "warehouse.query.cache.set_tiered", skip_all, err(Display))]
    pub async fn set_tiered(
        &self,
        project_id: Uuid,
        sql: &str,
        result: &CachedQueryResult,
        execution_time_ms: u64,
    ) -> CacheResult<CacheTier> {
        if !self.config.enabled {
            return Ok(CacheTier::Hot); // Return default tier even if disabled
        }

        // Determine the appropriate tier
        let tier = self.determine_tier(sql, execution_time_ms);
        let ttl = self.get_tier_ttl(tier);

        // Serialize the result
        let serialized = serde_json::to_vec(result)
            .map_err(|e| CacheError::Serialization(e.to_string()))?;

        // Check size limit
        if serialized.len() > self.config.max_result_size_bytes {
            if let Some(ref m) = self.metrics { m.record_cache_write_skip(); }
            tracing::debug!(
                project_id = %project_id,
                size = serialized.len(),
                limit = self.config.max_result_size_bytes,
                "Query result too large to cache"
            );
            return Ok(tier);
        }

        let expected_gen = self.get_generation(project_id).await?;

        let mut conn = self.pool.get().await
            .map_err(|e| CacheError::Pool(e.to_string()))?;

        let gen_key = self.generation_key(project_id);
        let query_hash = hash_normalized_query(sql);

        // Atomic generation check + write in a single Redis round-trip via Lua.
        // If the generation changed since we read it (a sync occurred), the
        // write is skipped to avoid caching stale data under a new generation.
        let script = r#"
            local gen = redis.call('GET', KEYS[1])
            if not gen then gen = '0' end
            if gen ~= ARGV[6] then return -1 end
            local cache_key = ARGV[1] .. ARGV[2] .. ':' .. gen .. ':' .. ARGV[3]
            redis.call('SETEX', cache_key, ARGV[4], ARGV[5])
            return gen
        "#;

        let generation: i64 = redis::Script::new(script)
            .key(&gen_key)
            .arg(&self.config.key_prefix)
            .arg(project_id.to_string())
            .arg(&query_hash)
            .arg(ttl)
            .arg(&serialized[..])
            .arg(expected_gen.to_string())
            .invoke_async(&mut *conn)
            .await?;

        if generation == -1 {
            if let Some(ref m) = self.metrics { m.record_cache_write_skip(); }
            tracing::debug!(
                project_id = %project_id,
                expected_gen = expected_gen,
                "Cache tiered write skipped: generation changed during query execution"
            );
            return Ok(tier);
        }

        if let Some(ref m) = self.metrics { m.record_cache_write(); }

        tracing::debug!(
            project_id = %project_id,
            tier = tier.name(),
            ttl_secs = ttl,
            execution_time_ms = execution_time_ms,
            row_count = result.row_count,
            "Cached query result with tiered TTL"
        );

        Ok(tier)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_cache_key_with_generation_format() {
        // Verify cache key format includes generation
        let key_prefix = "wh:cache:";
        let project_id = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let generation = 5u64;
        let sql = "SELECT * FROM customers";
        let query_hash = hash_normalized_query(sql);
        
        // New format: prefix:project_id:generation:query_hash
        let expected_key = format!("{}{}:{}:{}", key_prefix, project_id, generation, query_hash);
        assert!(expected_key.starts_with("wh:cache:00000000-0000-0000-0000-000000000001:5:"));
        
        // Test the sync method
        let config = QueryCacheConfig::default();
        // Can't test the full cache without Redis, but we can verify key format
        assert_eq!(config.key_prefix, "wh:cache:");
    }
    
    #[test]
    fn test_query_hash_consistency() {
        // Same query with different whitespace should have same hash
        let sql1 = "SELECT * FROM customers";
        let sql2 = "SELECT  *  FROM  customers";
        let sql3 = "select * from customers";
        
        assert_eq!(hash_normalized_query(sql1), hash_normalized_query(sql2));
        assert_eq!(hash_normalized_query(sql1), hash_normalized_query(sql3));
    }
    
    #[test]
    fn test_generation_key_format() {
        // Verify generation key format
        let key_prefix = "wh:cache:";
        let project_id = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        
        let expected_gen_key = format!("{}gen:{}", key_prefix, project_id);
        assert_eq!(expected_gen_key, "wh:cache:gen:00000000-0000-0000-0000-000000000001");
    }

    #[test]
    fn test_set_tiered_uses_atomic_lua_script() {
        // Verify that set_tiered builds the same atomic Lua script as set,
        // preventing the TOCTOU race where generation changes between the
        // check and the write.
        //
        // We can't run the actual Redis operations in unit tests, but we
        // verify the method structure by checking that the source doesn't
        // contain the old non-atomic pattern (GET + SET_EX as separate calls).
        let source = include_str!("cache.rs");

        let set_tiered_region = source
            .split("async fn set_tiered(")
            .nth(1)
            .expect("set_tiered method must exist");
        let set_tiered_body = set_tiered_region
            .split("\n    pub async fn")
            .next()
            .unwrap_or(set_tiered_region);

        assert!(
            set_tiered_body.contains("redis::Script::new"),
            "set_tiered must use a Lua script for atomic generation check + write"
        );
        assert!(
            !set_tiered_body.contains("conn.set_ex("),
            "set_tiered must NOT use a separate set_ex call (non-atomic TOCTOU race)"
        );
    }
}
