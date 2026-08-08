//! Skip Index Cache
//!
//! In-memory LRU cache for HierarchicalSkipIndex to avoid reloading from DB/R2 per query.
//!
//! PERFORMANCE: For TB-scale queries, loading skip indexes from the database or R2
//! for every query adds significant latency. This cache stores recently-used indexes
//! in memory with generation-based invalidation for correctness.
//!
//! # Concurrency Optimizations
//!
//! - Uses `DashMap` for sharded concurrent access (no global lock contention)
//! - Uses `AtomicU64` for timestamps to enable lock-free LRU updates
//! - Uses proper memory ordering (`Release`/`Acquire`) for memory tracking

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use dashmap::DashMap;
use uuid::Uuid;

use super::skip_index::HierarchicalSkipIndex;

/// A shareable reference to a HierarchicalSkipIndex.
/// 
/// Since HierarchicalSkipIndex contains FST data that is expensive to clone,
/// we use Arc for cheap sharing between cache and caller.
pub type SharedSkipIndex = Arc<HierarchicalSkipIndex>;

/// Configuration for the skip index cache.
#[derive(Debug, Clone)]
pub struct SkipIndexCacheConfig {
    /// Maximum number of table indexes to cache per project.
    pub max_tables_per_project: usize,
    /// Maximum number of projects to cache.
    pub max_projects: usize,
    /// Maximum total memory for cached indexes (in bytes).
    /// When exceeded, LRU entries are evicted.
    pub max_memory_bytes: usize,
    /// Time-to-live for cached indexes.
    /// After this duration, indexes are considered stale and will be refreshed.
    pub ttl: Duration,
}

impl Default for SkipIndexCacheConfig {
    fn default() -> Self {
        Self {
            max_tables_per_project: 100,
            max_projects: 1000,
            max_memory_bytes: 512 * 1024 * 1024, // 512MB
            ttl: Duration::from_secs(300),        // 5 minutes
        }
    }
}

// =============================================================================
// Atomic Timestamp Helpers
// =============================================================================

/// Baseline instant for computing relative timestamps.
/// Using a static baseline allows us to represent Instants as u64 offsets.
fn baseline_instant() -> Instant {
    use std::sync::OnceLock;
    static BASELINE: OnceLock<Instant> = OnceLock::new();
    *BASELINE.get_or_init(Instant::now)
}

/// Convert an Instant to a u64 (nanoseconds since baseline).
fn instant_to_nanos(instant: Instant) -> u64 {
    instant.saturating_duration_since(baseline_instant()).as_nanos() as u64
}

/// Convert a u64 (nanoseconds since baseline) back to an Instant.
fn nanos_to_instant(nanos: u64) -> Instant {
    baseline_instant() + Duration::from_nanos(nanos)
}

// =============================================================================
// CachedIndex - Per-table cache entry
// =============================================================================

/// A cached skip index entry with metadata.
struct CachedIndex {
    /// The skip index (Arc-wrapped for cheap sharing).
    index: SharedSkipIndex,
    /// When this entry was cached (nanoseconds since baseline).
    cached_at_nanos: u64,
    /// Last time this entry was accessed (atomic for lock-free updates).
    last_accessed_nanos: AtomicU64,
    /// The generation when this entry was cached.
    /// If global generation has advanced, this entry is stale.
    generation: u64,
    /// Estimated memory size of the index.
    estimated_size_bytes: usize,
}

impl CachedIndex {
    fn new(index: SharedSkipIndex, generation: u64, estimated_size_bytes: usize) -> Self {
        let now_nanos = instant_to_nanos(Instant::now());
        Self {
            index,
            cached_at_nanos: now_nanos,
            last_accessed_nanos: AtomicU64::new(now_nanos),
            generation,
            estimated_size_bytes,
        }
    }
    
    fn is_expired(&self, ttl: Duration) -> bool {
        let cached_at = nanos_to_instant(self.cached_at_nanos);
        cached_at.elapsed() > ttl
    }
    
    fn is_stale(&self, current_generation: u64) -> bool {
        self.generation < current_generation
    }
    
    /// Update last accessed time atomically (lock-free).
    fn touch(&self) {
        self.last_accessed_nanos.store(
            instant_to_nanos(Instant::now()),
            Ordering::Release,
        );
    }
    
    /// Get last accessed instant.
    fn last_accessed(&self) -> Instant {
        nanos_to_instant(self.last_accessed_nanos.load(Ordering::Acquire))
    }
}

// =============================================================================
// ProjectCache - Per-project table cache
// =============================================================================

/// Per-project cache with LRU eviction.
struct ProjectCache {
    tables: HashMap<String, CachedIndex>,
    /// Last time this project was accessed (atomic for lock-free updates).
    last_accessed_nanos: AtomicU64,
}

impl ProjectCache {
    fn new() -> Self {
        Self {
            tables: HashMap::new(),
            last_accessed_nanos: AtomicU64::new(instant_to_nanos(Instant::now())),
        }
    }
    
    /// Update last accessed time atomically (lock-free).
    fn touch(&self) {
        self.last_accessed_nanos.store(
            instant_to_nanos(Instant::now()),
            Ordering::Release,
        );
    }
    
    /// Get last accessed instant.
    fn last_accessed(&self) -> Instant {
        nanos_to_instant(self.last_accessed_nanos.load(Ordering::Acquire))
    }
}

// =============================================================================
// SkipIndexCache - Main cache structure
// =============================================================================

/// Thread-safe LRU cache for skip indexes.
///
/// PERFORMANCE: Reduces query latency by caching skip indexes in memory.
/// Uses generation-based invalidation for correctness when data is synced.
///
/// # Generation-Based Invalidation
///
/// When table data changes (after sync), call `invalidate_table()` or
/// `invalidate_project()` to mark cached indexes as stale. The next
/// `get()` call will return `None`, prompting a refresh from the database.
///
/// # Memory Management
///
/// The cache enforces a maximum memory limit. When exceeded, the least
/// recently used entries are evicted until memory usage drops below the limit.
///
/// # Thread Safety
///
/// Uses `DashMap` for sharded concurrent access, eliminating global lock
/// contention. Each shard has its own lock, allowing parallel access to
/// different projects.
pub struct SkipIndexCache {
    /// Per-project caches (sharded by project_id for concurrent access).
    projects: DashMap<Uuid, ProjectCache>,
    /// Global generation counter for invalidation.
    generation: AtomicU64,
    /// Cache configuration.
    config: SkipIndexCacheConfig,
    /// Current estimated memory usage.
    memory_usage: AtomicU64,
}

impl SkipIndexCache {
    /// Create a new skip index cache with the given configuration.
    pub fn new(config: SkipIndexCacheConfig) -> Self {
        Self {
            projects: DashMap::new(),
            generation: AtomicU64::new(0),
            config,
            memory_usage: AtomicU64::new(0),
        }
    }
    
    /// Create a cache with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(SkipIndexCacheConfig::default())
    }
    
    /// Get a cached skip index, if available and not stale.
    ///
    /// Returns `None` if:
    /// - The index is not in the cache
    /// - The index has expired (TTL exceeded)
    /// - The index is stale (generation mismatch)
    ///
    /// The returned Arc can be cheaply cloned and shared.
    ///
    /// # Concurrency
    ///
    /// This method only acquires a read lock on the specific shard containing
    /// the project, allowing concurrent access to other projects.
    pub fn get(&self, project_id: Uuid, table_name: &str) -> Option<SharedSkipIndex> {
        let current_generation = self.generation.load(Ordering::Acquire);
        
        // DashMap::get returns a Ref guard that only locks the relevant shard
        if let Some(project_cache) = self.projects.get(&project_id) {
            if let Some(entry) = project_cache.tables.get(table_name) {
                if !entry.is_expired(self.config.ttl) && !entry.is_stale(current_generation) {
                    // Return Arc clone (cheap, just reference count increment)
                    return Some(Arc::clone(&entry.index));
                }
            }
        }
        
        // Entry not found, expired, or stale - caller should load from DB
        None
    }
    
    /// Get a cached skip index and update its last-accessed time.
    ///
    /// # Concurrency
    ///
    /// Uses atomic timestamp updates, so this only requires a read lock on the
    /// shard (not a write lock). Much more efficient than the previous implementation
    /// which required a global write lock.
    pub fn get_and_touch(&self, project_id: Uuid, table_name: &str) -> Option<SharedSkipIndex> {
        let current_generation = self.generation.load(Ordering::Acquire);
        
        // DashMap::get returns a Ref guard (read lock on shard only)
        if let Some(project_cache) = self.projects.get(&project_id) {
            // Touch project with atomic update (no write lock needed)
            project_cache.touch();
            
            if let Some(entry) = project_cache.tables.get(table_name) {
                if !entry.is_expired(self.config.ttl) && !entry.is_stale(current_generation) {
                    // Touch entry with atomic update (no write lock needed)
                    entry.touch();
                    return Some(Arc::clone(&entry.index));
                }
            }
        }
        
        None
    }
    
    /// Cache a skip index for a table.
    ///
    /// If the cache exceeds memory limits, LRU entries are evicted.
    ///
    /// # Arguments
    /// * `project_id` - The project UUID
    /// * `table_name` - The table name
    /// * `index` - The skip index to cache (will be wrapped in Arc)
    /// * `estimated_size_bytes` - Estimated memory size of the index for cache management
    pub fn put(&self, project_id: Uuid, table_name: &str, index: HierarchicalSkipIndex, estimated_size_bytes: usize) {
        let current_generation = self.generation.load(Ordering::Acquire);
        let entry = CachedIndex::new(Arc::new(index), current_generation, estimated_size_bytes);
        let entry_size = entry.estimated_size_bytes;
        
        // Get or create project cache (write lock on shard only)
        let mut project_cache = self.projects.entry(project_id).or_insert_with(ProjectCache::new);
        project_cache.touch();
        
        // Remove old entry if exists (to update memory accounting)
        if let Some(old_entry) = project_cache.tables.remove(table_name) {
            self.memory_usage.fetch_sub(old_entry.estimated_size_bytes as u64, Ordering::Release);
        }
        
        // Add new entry
        project_cache.tables.insert(table_name.to_string(), entry);
        self.memory_usage.fetch_add(entry_size as u64, Ordering::Release);
        
        // Check if eviction is needed (without holding the entry lock)
        drop(project_cache);
        self.evict_if_needed();
    }
    
    /// Invalidate a specific table's cached index.
    ///
    /// Call this after syncing new data for a table.
    pub fn invalidate_table(&self, project_id: Uuid, table_name: &str) {
        if let Some(mut project_cache) = self.projects.get_mut(&project_id) {
            if let Some(entry) = project_cache.tables.remove(table_name) {
                self.memory_usage.fetch_sub(entry.estimated_size_bytes as u64, Ordering::Release);
                tracing::debug!(
                    project_id = %project_id,
                    table = table_name,
                    "Invalidated skip index cache entry"
                );
            }
        }
    }
    
    /// Invalidate all cached indexes for a project.
    ///
    /// Call this after bulk operations that affect multiple tables.
    pub fn invalidate_project(&self, project_id: Uuid) {
        if let Some((_, project_cache)) = self.projects.remove(&project_id) {
            let freed_memory: usize = project_cache.tables.values()
                .map(|e| e.estimated_size_bytes)
                .sum();
            self.memory_usage.fetch_sub(freed_memory as u64, Ordering::Release);
            tracing::debug!(
                project_id = %project_id,
                tables_evicted = project_cache.tables.len(),
                bytes_freed = freed_memory,
                "Invalidated all skip index cache entries for project"
            );
        }
    }
    
    /// Increment the global generation counter.
    ///
    /// This marks all cached entries as stale without removing them.
    /// They will be refreshed on next access.
    pub fn increment_generation(&self) {
        let new_gen = self.generation.fetch_add(1, Ordering::Release) + 1;
        tracing::debug!(
            generation = new_gen,
            "Skip index cache generation incremented"
        );
    }
    
    /// Get cache statistics.
    pub fn stats(&self) -> SkipIndexCacheStats {
        let project_count = self.projects.len();
        let table_count: usize = self.projects.iter()
            .map(|entry| entry.tables.len())
            .sum();
        
        SkipIndexCacheStats {
            project_count,
            table_count,
            memory_usage_bytes: self.memory_usage.load(Ordering::Acquire) as usize,
            generation: self.generation.load(Ordering::Acquire),
        }
    }
    
    /// Clear the entire cache.
    pub fn clear(&self) {
        self.projects.clear();
        self.memory_usage.store(0, Ordering::Release);
        tracing::info!("Skip index cache cleared");
    }
    
    /// Evict entries if memory or project limits are exceeded.
    ///
    /// # Concurrency Notes
    ///
    /// This method is **best-effort and approximate**. Due to concurrent access:
    ///
    /// - Memory usage readings may be stale by the time evictions occur
    /// - Other threads may add or remove entries during eviction
    /// - The 80% target provides buffer for slight over/under eviction
    ///
    /// This is intentional for performance: using precise locking would create
    /// contention on the hot path. The approximation is acceptable because:
    ///
    /// 1. Memory limits are soft limits, not hard guarantees
    /// 2. Over-eviction is harmless (cache miss, re-populate)
    /// 3. Under-eviction triggers re-eviction on next put()
    fn evict_if_needed(&self) {
        let current_memory = self.memory_usage.load(Ordering::Acquire) as usize;
        
        // Check if eviction is needed
        if current_memory <= self.config.max_memory_bytes && self.projects.len() <= self.config.max_projects {
            return;
        }
        
        // Collect all entries with their last access times for LRU eviction
        let mut candidates: Vec<(Uuid, String, Instant, usize)> = Vec::new();
        for entry in self.projects.iter() {
            let project_id = *entry.key();
            for (table_name, cached_entry) in &entry.tables {
                candidates.push((
                    project_id,
                    table_name.clone(),
                    cached_entry.last_accessed(),
                    cached_entry.estimated_size_bytes,
                ));
            }
        }
        
        // Sort by last accessed time (oldest first)
        candidates.sort_by_key(|(_, _, accessed, _)| *accessed);
        
        // Evict until under limits
        let mut evicted_count = 0;
        let mut evicted_bytes = 0;
        
        for (project_id, table_name, _, size) in candidates {
            // Check if we're back under limits
            let current = self.memory_usage.load(Ordering::Acquire) as usize;
            if current <= self.config.max_memory_bytes * 8 / 10 { // Target 80% utilization
                break;
            }
            
            // Evict this entry
            if let Some(mut project_cache) = self.projects.get_mut(&project_id) {
                if project_cache.tables.remove(&table_name).is_some() {
                    self.memory_usage.fetch_sub(size as u64, Ordering::Release);
                    evicted_count += 1;
                    evicted_bytes += size;
                }
                
                // Remove empty project caches
                if project_cache.tables.is_empty() {
                    drop(project_cache);
                    self.projects.remove(&project_id);
                }
            }
        }
        
        if evicted_count > 0 {
            tracing::info!(
                evicted_entries = evicted_count,
                evicted_bytes = evicted_bytes,
                remaining_memory = self.memory_usage.load(Ordering::Acquire),
                "Evicted skip index cache entries due to memory pressure"
            );
        }
        
        // Also evict empty/stale projects if over project limit
        if self.projects.len() > self.config.max_projects {
            let mut project_access: Vec<(Uuid, Instant)> = self.projects
                .iter()
                .map(|entry| (*entry.key(), entry.last_accessed()))
                .collect();
            project_access.sort_by_key(|(_, accessed)| *accessed);
            
            let to_evict = self.projects.len() - (self.config.max_projects * 8 / 10); // Target 80%
            for (project_id, _) in project_access.into_iter().take(to_evict) {
                if let Some((_, project_cache)) = self.projects.remove(&project_id) {
                    let freed: usize = project_cache.tables.values()
                        .map(|e| e.estimated_size_bytes)
                        .sum();
                    self.memory_usage.fetch_sub(freed as u64, Ordering::Release);
                }
            }
        }
    }
}

/// Statistics about the skip index cache.
#[derive(Debug, Clone)]
pub struct SkipIndexCacheStats {
    /// Number of projects with cached indexes.
    pub project_count: usize,
    /// Total number of cached table indexes.
    pub table_count: usize,
    /// Estimated memory usage in bytes.
    pub memory_usage_bytes: usize,
    /// Current generation counter.
    pub generation: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    fn create_test_index(_partition_count: usize) -> HierarchicalSkipIndex {
        // Create a simple empty index for testing
        HierarchicalSkipIndex::new()
    }
    
    fn estimated_size() -> usize {
        // Approximate size for test indexes
        1024
    }
    
    #[test]
    fn test_cache_get_and_put() {
        let cache = SkipIndexCache::with_defaults();
        let project_id = Uuid::new_v4();
        let table_name = "customers";
        let index = create_test_index(3);
        
        // Initially not in cache
        assert!(cache.get(project_id, table_name).is_none());
        
        // Put and get
        cache.put(project_id, table_name, index, estimated_size());
        assert!(cache.get(project_id, table_name).is_some());
    }
    
    #[test]
    fn test_cache_get_and_touch() {
        let cache = SkipIndexCache::with_defaults();
        let project_id = Uuid::new_v4();
        let table_name = "customers";
        let index = create_test_index(3);
        
        // Put and get_and_touch
        cache.put(project_id, table_name, index, estimated_size());
        
        // get_and_touch should work and update timestamp
        let result = cache.get_and_touch(project_id, table_name);
        assert!(result.is_some());
        
        // Verify we can still get after touch
        assert!(cache.get(project_id, table_name).is_some());
    }
    
    #[test]
    fn test_cache_invalidation() {
        let cache = SkipIndexCache::with_defaults();
        let project_id = Uuid::new_v4();
        let table_name = "customers";
        let index = create_test_index(3);
        
        cache.put(project_id, table_name, index, estimated_size());
        assert!(cache.get(project_id, table_name).is_some());
        
        // Invalidate table
        cache.invalidate_table(project_id, table_name);
        assert!(cache.get(project_id, table_name).is_none());
    }
    
    #[test]
    fn test_cache_project_invalidation() {
        let cache = SkipIndexCache::with_defaults();
        let project_id = Uuid::new_v4();
        
        // Add multiple tables
        cache.put(project_id, "customers", create_test_index(2), estimated_size());
        cache.put(project_id, "orders", create_test_index(3), estimated_size());
        cache.put(project_id, "products", create_test_index(1), estimated_size());
        
        assert!(cache.get(project_id, "customers").is_some());
        assert!(cache.get(project_id, "orders").is_some());
        assert!(cache.get(project_id, "products").is_some());
        
        // Invalidate entire project
        cache.invalidate_project(project_id);
        
        assert!(cache.get(project_id, "customers").is_none());
        assert!(cache.get(project_id, "orders").is_none());
        assert!(cache.get(project_id, "products").is_none());
    }
    
    #[test]
    fn test_cache_generation_invalidation() {
        let cache = SkipIndexCache::with_defaults();
        let project_id = Uuid::new_v4();
        let table_name = "customers";
        
        cache.put(project_id, table_name, create_test_index(3), estimated_size());
        assert!(cache.get(project_id, table_name).is_some());
        
        // Increment generation
        cache.increment_generation();
        
        // Entry is now stale
        assert!(cache.get(project_id, table_name).is_none());
    }
    
    #[test]
    fn test_cache_stats() {
        let cache = SkipIndexCache::with_defaults();
        let project1 = Uuid::new_v4();
        let project2 = Uuid::new_v4();
        
        cache.put(project1, "customers", create_test_index(2), estimated_size());
        cache.put(project1, "orders", create_test_index(3), estimated_size());
        cache.put(project2, "products", create_test_index(1), estimated_size());
        
        let stats = cache.stats();
        assert_eq!(stats.project_count, 2);
        assert_eq!(stats.table_count, 3);
        assert!(stats.memory_usage_bytes > 0);
    }
    
    #[test]
    fn test_cache_clear() {
        let cache = SkipIndexCache::with_defaults();
        let project_id = Uuid::new_v4();
        
        cache.put(project_id, "customers", create_test_index(2), estimated_size());
        cache.put(project_id, "orders", create_test_index(3), estimated_size());
        
        let stats = cache.stats();
        assert!(stats.table_count > 0);
        assert!(stats.memory_usage_bytes > 0);
        
        cache.clear();
        
        let stats = cache.stats();
        assert_eq!(stats.project_count, 0);
        assert_eq!(stats.table_count, 0);
        assert_eq!(stats.memory_usage_bytes, 0);
    }
    
    #[test]
    fn test_atomic_timestamp_roundtrip() {
        // Initialize baseline first by calling the helper
        let _ = baseline_instant();
        
        // Now test the roundtrip
        let now = Instant::now();
        let nanos = instant_to_nanos(now);
        let recovered = nanos_to_instant(nanos);
        
        // The recovered instant should be very close to the original.
        // We allow 1ms tolerance for timing variations during test execution.
        let diff = if recovered > now {
            recovered.duration_since(now)
        } else {
            now.duration_since(recovered)
        };
        assert!(
            diff < Duration::from_millis(1),
            "Roundtrip diff {:?} exceeds 1ms tolerance",
            diff
        );
    }
}
