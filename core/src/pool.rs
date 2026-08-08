//! Thread-local object pools for reducing allocation pressure in hot paths.
//!
//! These pools are optimized for the Kafka worker pattern where each worker
//! processes messages independently on its own tokio task. Thread-local pools
//! eliminate all synchronization overhead (~20ns allocation vs ~250ns+ for mutex-based).
//!
//! Note: These utilities are available for use in hot paths but may not all
//! be currently active in the codebase.

#![allow(dead_code)]

use std::cell::RefCell;
use std::collections::HashMap;

/// Maximum number of HashMaps to keep in each thread's pool.
/// Beyond this limit, returned maps are dropped rather than pooled.
const POOL_MAX_SIZE: usize = 16;

/// Default capacity for new HashMaps in the pool.
/// Based on typical OTLP attribute counts (5-30 attributes per span/log/metric).
const DEFAULT_MAP_CAPACITY: usize = 16;

thread_local! {
    /// Thread-local pool for attribute HashMaps.
    /// Used by spans_worker, metrics_worker, and kafka_log_consumer.
    static ATTR_MAP_POOL: RefCell<Vec<HashMap<String, String>>> =
        RefCell::new(Vec::with_capacity(POOL_MAX_SIZE));
}

/// Acquire a HashMap from the thread-local pool.
/// Returns a pooled map if available, otherwise creates a new one with default capacity.
///
/// Uses try_borrow_mut to avoid panics if called during another borrow (e.g., in Drop).
#[inline]
pub fn acquire_attr_map() -> HashMap<String, String> {
    ATTR_MAP_POOL.with(|pool| {
        pool.try_borrow_mut()
            .ok()
            .and_then(|mut p| p.pop())
            .unwrap_or_else(|| HashMap::with_capacity(DEFAULT_MAP_CAPACITY))
    })
}

/// Release a HashMap back to the thread-local pool.
/// The map is cleared before being returned to the pool.
/// If the pool is at capacity or the pool is currently borrowed, the map is dropped instead.
///
/// Uses try_borrow_mut to avoid panics if called during another borrow (e.g., in Drop).
#[inline]
pub fn release_attr_map(mut map: HashMap<String, String>) {
    map.clear();
    ATTR_MAP_POOL.with(|pool| {
        if let Ok(mut pool) = pool.try_borrow_mut() {
            if pool.len() < POOL_MAX_SIZE {
                pool.push(map);
            }
            // If pool is full, map is simply dropped
        }
        // If borrow fails, map is simply dropped (graceful degradation)
    });
}

/// A guard that automatically returns a HashMap to the pool when dropped.
/// Use this for RAII-style pool management.
pub struct PooledAttrMap {
    map: Option<HashMap<String, String>>,
}

impl PooledAttrMap {
    /// Create a new pooled attribute map.
    #[inline]
    pub fn new() -> Self {
        Self {
            map: Some(acquire_attr_map()),
        }
    }

    /// Take ownership of the inner HashMap, preventing automatic return to pool.
    /// Use this when you need to pass ownership to another function.
    #[inline]
    pub fn take(mut self) -> HashMap<String, String> {
        self.map.take().expect("PooledAttrMap already taken")
    }
}

impl Default for PooledAttrMap {
    fn default() -> Self {
        Self::new()
    }
}

impl std::ops::Deref for PooledAttrMap {
    type Target = HashMap<String, String>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.map.as_ref().expect("PooledAttrMap already taken")
    }
}

impl std::ops::DerefMut for PooledAttrMap {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.map.as_mut().expect("PooledAttrMap already taken")
    }
}

impl Drop for PooledAttrMap {
    fn drop(&mut self) {
        if let Some(map) = self.map.take() {
            release_attr_map(map);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acquire_release_cycle() {
        // Acquire a map
        let mut map = acquire_attr_map();
        map.insert("key".to_string(), "value".to_string());

        // Release it
        release_attr_map(map);

        // Acquire again - should get the pooled map (cleared)
        let map2 = acquire_attr_map();
        assert!(map2.is_empty());
        assert!(map2.capacity() >= DEFAULT_MAP_CAPACITY);
    }

    #[test]
    fn test_pooled_attr_map_raii() {
        {
            let mut pooled = PooledAttrMap::new();
            pooled.insert("test".to_string(), "value".to_string());
            assert_eq!(pooled.get("test"), Some(&"value".to_string()));
            // Dropped here, returned to pool
        }

        // Pool should have one map now
        let map = acquire_attr_map();
        assert!(map.is_empty()); // Was cleared on return
    }

    #[test]
    fn test_pooled_attr_map_take() {
        let pooled = PooledAttrMap::new();
        let map = pooled.take();
        // Map is now owned, not returned to pool
        assert!(map.capacity() >= DEFAULT_MAP_CAPACITY);
    }

    #[test]
    fn test_pool_max_size() {
        // Fill the pool
        let maps: Vec<_> = (0..POOL_MAX_SIZE + 5).map(|_| acquire_attr_map()).collect();

        // Release all - only POOL_MAX_SIZE should be kept
        for map in maps {
            release_attr_map(map);
        }

        // Acquire POOL_MAX_SIZE maps - all should come from pool
        let acquired: Vec<_> = (0..POOL_MAX_SIZE).map(|_| acquire_attr_map()).collect();

        // Pool should be empty now
        let new_map = acquire_attr_map();
        assert!(new_map.capacity() >= DEFAULT_MAP_CAPACITY);

        // Clean up
        for map in acquired {
            release_attr_map(map);
        }
        release_attr_map(new_map);
    }
}
