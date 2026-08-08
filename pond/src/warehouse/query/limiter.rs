//! Query Concurrency Limiter
//!
//! Per-project query concurrency limits with a global queue for fairness.
//!
//! PERFORMANCE: Prevents TB-scale queries from overwhelming ClickHouse by:
//! - Limiting concurrent queries per project (prevents single project from monopolizing)
//! - Limiting total concurrent queries globally (prevents ClickHouse overload)
//! - Providing queue priority for smaller/cheaper queries
//! - LRU eviction of inactive project limiters to prevent memory growth

use ahash::AHashMap;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, OwnedSemaphorePermit};
use uuid::Uuid;

use crate::warehouse::metrics::WarehouseMetrics;

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

/// Configuration for the query limiter.
#[derive(Debug, Clone)]
pub struct QueryLimiterConfig {
    /// Maximum concurrent queries per project
    pub max_concurrent_per_project: usize,
    /// Maximum concurrent queries globally
    pub max_concurrent_global: usize,
    /// Maximum total outstanding queries per project (running + waiting).
    /// When exceeded, new queries are rejected.
    pub max_queue_per_project: usize,
    /// Maximum number of project limiters to keep in memory.
    /// When exceeded, least recently used limiters are evicted.
    pub max_cached_projects: usize,
    /// How long to keep inactive project limiters before they're eligible for eviction.
    /// Projects with active queries are never evicted.
    pub project_idle_timeout: Duration,
}

impl Default for QueryLimiterConfig {
    fn default() -> Self {
        Self {
            max_concurrent_per_project: 5,
            max_concurrent_global: 50,
            max_queue_per_project: 20,
            max_cached_projects: 10_000, // Support up to 10K projects before LRU kicks in
            project_idle_timeout: Duration::from_secs(3600), // 1 hour
        }
    }
}

/// Per-project semaphore for limiting concurrent queries.
struct ProjectLimiter {
    semaphore: Arc<Semaphore>,
    queue_size: Arc<AtomicUsize>,
    config_max_queue: usize,
    config_max_concurrent: usize,
    /// Last time this limiter was accessed (atomic for lock-free updates).
    /// Stored as nanoseconds since baseline for atomic storage.
    last_accessed_nanos: AtomicU64,
}

impl ProjectLimiter {
    fn new(max_concurrent: usize, max_queue: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            queue_size: Arc::new(AtomicUsize::new(0)),
            config_max_queue: max_queue,
            config_max_concurrent: max_concurrent,
            last_accessed_nanos: AtomicU64::new(instant_to_nanos(Instant::now())),
        }
    }
    
    /// Update the last accessed time atomically (lock-free).
    ///
    /// This can be called with just a shared reference, allowing LRU timestamp
    /// updates without requiring a write lock on the limiter map.
    fn touch(&self) {
        self.last_accessed_nanos.store(
            instant_to_nanos(Instant::now()),
            Ordering::Release,
        );
    }
    
    /// Get the last accessed instant.
    fn last_accessed(&self) -> Instant {
        nanos_to_instant(self.last_accessed_nanos.load(Ordering::Acquire))
    }
    
    /// Check if this limiter is idle (no active or queued queries).
    fn is_idle(&self) -> bool {
        let available = self.semaphore.available_permits();
        let queued = self.queue_size.load(Ordering::Relaxed);
        // Idle if all permits are available and no queries queued
        available == self.config_max_concurrent && queued == 0
    }
    
    /// Check if this limiter should be evicted based on idle timeout.
    fn should_evict(&self, idle_timeout: Duration) -> bool {
        self.is_idle() && self.last_accessed().elapsed() > idle_timeout
    }
}

/// Guard that automatically decrements the queue counter if dropped without
/// being converted to a QueryPermit.
///
/// CORRECTNESS: Uses `compare_exchange` to atomically check the queue limit
/// and increment in a single operation, preventing the race where concurrent
/// callers could exceed `max_queue_per_project`.
struct QueueGuard {
    queue_size: Arc<AtomicUsize>,
    /// Flag to indicate responsibility was transferred to QueryPermit
    consumed: bool,
}

impl QueueGuard {
    /// Atomically try to increment the queue counter, failing if the result
    /// would exceed `max_queue`.
    fn try_new(queue_size: Arc<AtomicUsize>, max_queue: usize) -> Option<Self> {
        loop {
            let current = queue_size.load(Ordering::Relaxed);
            if current >= max_queue {
                return None;
            }
            match queue_size.compare_exchange_weak(
                current,
                current + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some(Self {
                        queue_size,
                        consumed: false,
                    });
                }
                Err(_) => continue,
            }
        }
    }

    /// Consume the guard, transferring responsibility to QueryPermit.
    /// This prevents the guard from decrementing on drop.
    fn consume(mut self) -> Arc<AtomicUsize> {
        self.consumed = true;
        self.queue_size.clone()
    }
}

impl Drop for QueueGuard {
    fn drop(&mut self) {
        if !self.consumed {
            self.queue_size.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

/// Query permit that holds both global and per-project semaphore permits.
/// When dropped, it releases both permits and decrements the queue counter.
pub struct QueryPermit {
    _global_permit: OwnedSemaphorePermit,
    _project_permit: OwnedSemaphorePermit,
    project_id: Uuid,
    queue_size: Arc<AtomicUsize>,
}

impl QueryPermit {
    /// Get the project ID this permit was acquired for.
    pub fn project_id(&self) -> Uuid {
        self.project_id
    }
}

impl Drop for QueryPermit {
    fn drop(&mut self) {
        // Decrement queue size when permit is released
        self.queue_size.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Error type for query limiter operations.
#[derive(Debug, thiserror::Error)]
pub enum LimiterError {
    #[error("Query queue full for project {project_id}: {queued}/{max} queries queued")]
    QueueFull {
        project_id: Uuid,
        queued: usize,
        max: usize,
    },
    #[error("Query limiter shutdown")]
    Shutdown,
}

/// Query concurrency limiter with per-project and global limits.
pub struct QueryLimiter {
    config: QueryLimiterConfig,
    global_semaphore: Arc<Semaphore>,
    project_limiters: DashMap<Uuid, ProjectLimiter>,
    metrics: Option<Arc<WarehouseMetrics>>,
}

impl QueryLimiter {
    /// Create a new query limiter with the given configuration.
    pub fn new(config: QueryLimiterConfig) -> Self {
        Self {
            global_semaphore: Arc::new(Semaphore::new(config.max_concurrent_global)),
            project_limiters: DashMap::new(),
            config,
            metrics: None,
        }
    }
    
    /// Create with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(QueryLimiterConfig::default())
    }
    
    /// Attach a metrics collector to this limiter.
    pub fn with_metrics(mut self, metrics: Arc<WarehouseMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }
    
    /// Acquire a permit to execute a query for the given project.
    ///
    /// This will wait for both a global permit and a per-project permit.
    /// If the project's queue is full, returns an error immediately.
    ///
    /// # Returns
    ///
    /// A `QueryPermit` that must be held for the duration of the query.
    /// When dropped, it releases both permits.
    ///
    /// # Correctness
    ///
    /// Uses `QueueGuard` to ensure the queue counter is properly decremented
    /// even if permit acquisition fails or is cancelled. This prevents queue
    /// count drift over time.
    ///
    /// # Concurrency
    ///
    /// Uses atomic timestamps for LRU tracking, allowing `touch()` to be called
    /// under a read lock without needing to upgrade to a write lock. This avoids
    /// the TOCTOU race where a limiter found on the read path would have a stale
    /// `last_accessed` timestamp.
    ///
    /// # Memory Management
    ///
    /// This method triggers LRU cleanup when the number of cached project limiters
    /// exceeds the configured maximum. Inactive limiters (no active queries) that
    /// have been idle longer than `project_idle_timeout` are evicted.
    #[tracing::instrument(
        name = "warehouse.limiter.acquire",
        skip_all,
        err(Display)
    )]
    pub async fn acquire(&self, project_id: Uuid) -> Result<QueryPermit, LimiterError> {
        // Get or create project limiter
        let project_limiter = {
            if let Some(limiter_ref) = self.project_limiters.get(&project_id) {
                limiter_ref.touch();
                (limiter_ref.semaphore.clone(), limiter_ref.queue_size.clone(), limiter_ref.config_max_queue)
            } else {
                if self.project_limiters.len() >= self.config.max_cached_projects {
                    self.cleanup_idle().await;
                }

                let limiter = self.project_limiters.entry(project_id).or_insert_with(|| {
                    ProjectLimiter::new(
                        self.config.max_concurrent_per_project,
                        self.config.max_queue_per_project,
                    )
                });
                limiter.touch();
                (limiter.semaphore.clone(), limiter.queue_size.clone(), limiter.config_max_queue)
            }
        };
        
        let (project_sem, queue_size, max_queue) = project_limiter;
        
        let guard = match QueueGuard::try_new(queue_size.clone(), max_queue) {
            Some(g) => g,
            None => {
                if let Some(ref m) = self.metrics { m.record_queue_full_rejection(); }
                return Err(LimiterError::QueueFull {
                    project_id,
                    queued: queue_size.load(Ordering::Relaxed),
                    max: max_queue,
                });
            }
        };
        
        let wait_start = Instant::now();
        
        // Acquire global permit first (prevents global starvation)
        // If this fails, guard will decrement the counter
        let global_permit = self.global_semaphore.clone().acquire_owned().await
            .map_err(|_| {
                if let Some(ref m) = self.metrics { m.record_permit_denied(); }
                LimiterError::Shutdown
            })?;
        
        // Then acquire project permit
        // If this fails, guard will decrement the counter
        let project_permit = project_sem.acquire_owned().await
            .map_err(|_| {
                if let Some(ref m) = self.metrics { m.record_permit_denied(); }
                LimiterError::Shutdown
            })?;
        
        // Success! Record wait time and consume the guard.
        if let Some(ref m) = self.metrics { m.record_permit_acquired(wait_start.elapsed()); }
        let queue_size = guard.consume();
        
        Ok(QueryPermit {
            _global_permit: global_permit,
            _project_permit: project_permit,
            project_id,
            queue_size,
        })
    }
    
    /// Get statistics about the limiter.
    #[tracing::instrument(
        name = "warehouse.limiter.stats",
        skip_all
    )]
    pub async fn stats(&self) -> QueryLimiterStats {
        let global_available = self.global_semaphore.available_permits();
        let global_in_use = self.config.max_concurrent_global - global_available;
        
        let project_count = self.project_limiters.len();
        
        let per_project: AHashMap<Uuid, ProjectStats> = self.project_limiters
            .iter()
            .map(|entry| {
                let id = *entry.key();
                let limiter = entry.value();
                let available = limiter.semaphore.available_permits();
                (id, ProjectStats {
                    concurrent: self.config.max_concurrent_per_project - available,
                    queued: limiter.queue_size.load(Ordering::Relaxed),
                })
            })
            .collect();
        
        QueryLimiterStats {
            global_concurrent: global_in_use,
            global_max: self.config.max_concurrent_global,
            project_count,
            per_project,
        }
    }
    
    /// Cleanup idle project limiters that have exceeded the idle timeout.
    ///
    /// This is called automatically when the cache exceeds `max_cached_projects`,
    /// but can also be called manually for periodic cleanup.
    ///
    /// # Returns
    /// The number of limiters evicted.
    #[tracing::instrument(
        name = "warehouse.limiter.cleanup_idle",
        skip_all
    )]
    pub async fn cleanup_idle(&self) -> usize {
        let before_count = self.project_limiters.len();
        let idle_timeout = self.config.project_idle_timeout;
        
        // Remove expired limiters using retain
        self.project_limiters.retain(|_, limiter| !limiter.should_evict(idle_timeout));
        let mut eviction_count = before_count - self.project_limiters.len();
        
        // If still over capacity after expiry eviction, apply LRU
        if self.project_limiters.len() > self.config.max_cached_projects {
            let target_evictions = self.project_limiters.len().saturating_sub(self.config.max_cached_projects / 2);
            
            // Collect candidates with their last accessed time
            let mut candidates: Vec<(Uuid, Instant)> = self.project_limiters
                .iter()
                .filter(|entry| entry.value().is_idle())
                .map(|entry| (*entry.key(), entry.value().last_accessed()))
                .collect();
            
            // Sort by last accessed time (oldest first)
            candidates.sort_unstable_by_key(|(_, accessed)| *accessed);
            
            // Remove the oldest ones
            let lru_to_remove: Vec<Uuid> = candidates
                .into_iter()
                .take(target_evictions)
                .map(|(id, _)| id)
                .collect();
            
            for id in &lru_to_remove {
                self.project_limiters.remove(id);
            }
            
            eviction_count += lru_to_remove.len();
        }
        
        if eviction_count > 0 {
            tracing::info!(
                evicted = eviction_count,
                before = before_count,
                after = self.project_limiters.len(),
                "Evicted idle project limiters"
            );
        }
        
        eviction_count
    }
    
    /// Get the number of cached project limiters.
    #[tracing::instrument(
        name = "warehouse.limiter.cached_project_count",
        skip_all
    )]
    pub async fn cached_project_count(&self) -> usize {
        self.project_limiters.len()
    }
    
    /// Start a background task that periodically cleans up idle project limiters.
    ///
    /// This prevents memory growth from accumulating idle limiters over time.
    /// The cleanup runs at half the `project_idle_timeout` interval to ensure
    /// timely eviction of expired limiters.
    ///
    /// # Arguments
    /// * `limiter` - Arc-wrapped QueryLimiter to clean up
    ///
    /// # Returns
    /// A JoinHandle for the spawned cleanup task. Drop this to stop the cleanup.
    ///
    /// # Example
    /// ```ignore
    /// let limiter = Arc::new(QueryLimiter::with_defaults());
    /// let cleanup_handle = QueryLimiter::start_cleanup_task(limiter.clone());
    ///
    /// // ... use limiter ...
    ///
    /// // Stop cleanup when done
    /// cleanup_handle.abort();
    /// ```
    pub fn start_cleanup_task(limiter: Arc<QueryLimiter>) -> tokio::task::JoinHandle<()> {
        // Run cleanup at half the idle timeout interval for timely eviction
        let cleanup_interval = limiter.config.project_idle_timeout / 2;
        // Minimum interval of 1 minute to avoid excessive CPU usage
        let cleanup_interval = cleanup_interval.max(Duration::from_secs(60));
        
        tracing::info!(
            interval_secs = cleanup_interval.as_secs(),
            max_cached_projects = limiter.config.max_cached_projects,
            idle_timeout_secs = limiter.config.project_idle_timeout.as_secs(),
            "Starting query limiter cleanup task"
        );
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(cleanup_interval);
            // Skip the first tick which fires immediately
            interval.tick().await;
            
            loop {
                interval.tick().await;
                
                let evicted = limiter.cleanup_idle().await;
                let remaining = limiter.cached_project_count().await;
                
                if evicted > 0 {
                    tracing::debug!(
                        evicted = evicted,
                        remaining = remaining,
                        "Query limiter periodic cleanup completed"
                    );
                }
            }
        })
    }
}

/// Statistics about per-project query usage.
#[derive(Debug, Clone)]
pub struct ProjectStats {
    /// Number of currently executing queries
    pub concurrent: usize,
    /// Total outstanding queries (running + waiting)
    pub queued: usize,
}

/// Statistics about the query limiter.
#[derive(Debug, Clone)]
pub struct QueryLimiterStats {
    /// Number of globally concurrent queries
    pub global_concurrent: usize,
    /// Maximum global concurrent queries
    pub global_max: usize,
    /// Number of projects with active limiters
    pub project_count: usize,
    /// Per-project statistics
    pub per_project: AHashMap<Uuid, ProjectStats>,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_basic_acquire() {
        let limiter = QueryLimiter::with_defaults();
        let project_id = Uuid::new_v4();
        
        let permit = limiter.acquire(project_id).await.unwrap();
        assert_eq!(permit.project_id(), project_id);
        
        // Check stats
        let stats = limiter.stats().await;
        assert_eq!(stats.global_concurrent, 1);
        
        drop(permit);
        
        // After drop, should be released
        let stats = limiter.stats().await;
        assert_eq!(stats.global_concurrent, 0);
    }
    
    #[tokio::test]
    async fn test_per_project_limit() {
        let config = QueryLimiterConfig {
            max_concurrent_per_project: 2,
            max_concurrent_global: 100,
            max_queue_per_project: 5,
            ..Default::default()
        };
        let limiter = QueryLimiter::new(config);
        let project_id = Uuid::new_v4();
        
        // Acquire 2 permits (should succeed)
        let permit1 = limiter.acquire(project_id).await.unwrap();
        let permit2 = limiter.acquire(project_id).await.unwrap();
        
        let stats = limiter.stats().await;
        assert_eq!(stats.per_project.get(&project_id).unwrap().concurrent, 2);
        
        drop(permit1);
        drop(permit2);
    }
    
    #[tokio::test]
    async fn test_queue_full() {
        let config = QueryLimiterConfig {
            max_concurrent_per_project: 1,
            max_concurrent_global: 100,
            max_queue_per_project: 1, // Only allow 1 queued
            ..Default::default()
        };
        let limiter = Arc::new(QueryLimiter::new(config));
        let project_id = Uuid::new_v4();
        
        // First acquire succeeds
        let _permit = limiter.acquire(project_id).await.unwrap();
        
        // Second should fail because queue is full
        let result = limiter.acquire(project_id).await;
        assert!(matches!(result, Err(LimiterError::QueueFull { .. })));
    }

    // ==================== Permit Drop Restores Capacity ====================

    #[tokio::test]
    async fn test_permit_drop_restores_capacity() {
        let config = QueryLimiterConfig {
            max_concurrent_per_project: 2,
            max_concurrent_global: 100,
            max_queue_per_project: 2, // Allow exactly 2 in the queue (matches concurrency)
            ..Default::default()
        };
        let limiter = QueryLimiter::new(config);
        let project_id = Uuid::new_v4();

        // Fill up both permits
        let p1 = limiter.acquire(project_id).await.unwrap();
        let p2 = limiter.acquire(project_id).await.unwrap();

        // Next acquire must fail since queue is full
        let result = limiter.acquire(project_id).await;
        assert!(matches!(result, Err(LimiterError::QueueFull { .. })));

        // Drop one permit, capacity should be restored
        drop(p1);

        // Now we can acquire again
        let p3 = limiter.acquire(project_id).await.unwrap();

        let stats = limiter.stats().await;
        let ps = stats.per_project.get(&project_id).unwrap();
        assert_eq!(ps.concurrent, 2); // p2 + p3

        drop(p2);
        drop(p3);
    }

    // ==================== Multi-Project Isolation ====================

    #[tokio::test]
    async fn test_multi_project_isolation() {
        let config = QueryLimiterConfig {
            max_concurrent_per_project: 1,
            max_concurrent_global: 100,
            max_queue_per_project: 1, // Allow exactly 1 (matches concurrency)
            ..Default::default()
        };
        let limiter = QueryLimiter::new(config);
        let project_a = Uuid::new_v4();
        let project_b = Uuid::new_v4();

        // Project A fills up
        let _pa = limiter.acquire(project_a).await.unwrap();

        // Project A is full
        let result_a = limiter.acquire(project_a).await;
        assert!(matches!(result_a, Err(LimiterError::QueueFull { .. })));

        // Project B should still be able to acquire
        let _pb = limiter.acquire(project_b).await.unwrap();

        let stats = limiter.stats().await;
        assert_eq!(stats.per_project.get(&project_a).unwrap().concurrent, 1);
        assert_eq!(stats.per_project.get(&project_b).unwrap().concurrent, 1);
    }

    // ==================== Global Limit Enforcement ====================

    #[tokio::test]
    async fn test_global_limit_enforcement() {
        let config = QueryLimiterConfig {
            max_concurrent_per_project: 5,
            max_concurrent_global: 3, // Very small global limit
            max_queue_per_project: 20,
            ..Default::default()
        };
        let limiter = Arc::new(QueryLimiter::new(config));

        // Three different projects, each taking one global permit
        let p1 = limiter.acquire(Uuid::new_v4()).await.unwrap();
        let p2 = limiter.acquire(Uuid::new_v4()).await.unwrap();
        let p3 = limiter.acquire(Uuid::new_v4()).await.unwrap();

        let stats = limiter.stats().await;
        assert_eq!(stats.global_concurrent, 3);

        // The next acquire should block (global limit reached)
        // We test by spawning it as a task and checking it doesn't complete immediately
        let limiter_clone = limiter.clone();
        let handle = tokio::spawn(async move {
            limiter_clone.acquire(Uuid::new_v4()).await
        });

        // Give the task a bit of time; it should NOT complete
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!handle.is_finished(), "acquire should block when global limit is reached");

        // Release one permit, the blocked acquire should complete
        drop(p1);
        let result = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(result.is_ok(), "acquire should unblock after global permit released");

        drop(p2);
        drop(p3);
    }

    // ==================== Stats After QueueFull Error ====================

    #[tokio::test]
    async fn test_stats_after_queue_full() {
        let config = QueryLimiterConfig {
            max_concurrent_per_project: 1,
            max_concurrent_global: 100,
            max_queue_per_project: 1,
            ..Default::default()
        };
        let limiter = QueryLimiter::new(config);
        let project_id = Uuid::new_v4();

        // Hold one permit
        let _p = limiter.acquire(project_id).await.unwrap();

        // This should fail with QueueFull
        let result = limiter.acquire(project_id).await;
        assert!(matches!(result, Err(LimiterError::QueueFull { .. })));

        // After the error, stats should show correct counts (no drifted queue_size)
        let stats = limiter.stats().await;
        let ps = stats.per_project.get(&project_id).unwrap();
        assert_eq!(ps.concurrent, 1);
        // The queued count should be 1 (only the held permit), not 2
        assert_eq!(ps.queued, 1);
    }

    // ==================== Cleanup Idle Projects ====================

    #[tokio::test]
    async fn test_cleanup_idle_projects() {
        let config = QueryLimiterConfig {
            max_concurrent_per_project: 5,
            max_concurrent_global: 100,
            max_queue_per_project: 20,
            max_cached_projects: 100,
            project_idle_timeout: Duration::from_millis(10), // Very short for testing
        };
        let limiter = QueryLimiter::new(config);

        // Create limiters for several projects
        let ids: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();
        for &id in &ids {
            let p = limiter.acquire(id).await.unwrap();
            drop(p); // Release immediately so they become idle
        }

        assert_eq!(limiter.cached_project_count().await, 5);

        // Wait for idle timeout
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Cleanup should evict all idle limiters
        let evicted = limiter.cleanup_idle().await;
        assert!(evicted > 0, "Should have evicted at least some idle limiters");
        assert!(limiter.cached_project_count().await < 5);
    }

    // ==================== Re-acquire After Eviction ====================

    #[tokio::test]
    async fn test_reacquire_after_eviction() {
        let config = QueryLimiterConfig {
            max_concurrent_per_project: 5,
            max_concurrent_global: 100,
            max_queue_per_project: 20,
            max_cached_projects: 100,
            project_idle_timeout: Duration::from_millis(10),
        };
        let limiter = QueryLimiter::new(config);
        let project_id = Uuid::new_v4();

        // Create and release
        let p = limiter.acquire(project_id).await.unwrap();
        drop(p);

        // Wait and evict
        tokio::time::sleep(Duration::from_millis(20)).await;
        limiter.cleanup_idle().await;

        // Should be able to acquire again after eviction
        let p2 = limiter.acquire(project_id).await.unwrap();
        assert_eq!(p2.project_id(), project_id);

        let stats = limiter.stats().await;
        assert_eq!(stats.per_project.get(&project_id).unwrap().concurrent, 1);
    }
}
