//! Warehouse Observability Metrics
//!
//! This module provides centralized metrics for monitoring the warehouse feature,
//! including:
//! - Query cache hit/miss rates
//! - Skip index effectiveness
//! - Query queue wait times and concurrency
//! - Sync operation performance
//!
//! All metrics follow the tagging strategy guidelines - using only low-cardinality
//! values as tags (e.g., source_type, status) and never including IDs.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::warehouse::query::cost_estimator::QueryCostEstimate;

/// Warehouse metrics collector.
///
/// Provides methods to record metrics for cache, skip index, query limiter,
/// and sync operations. Metrics are accumulated and can be retrieved for
/// reporting to monitoring systems.
#[derive(Debug, Default)]
pub struct WarehouseMetrics {
    // Cache metrics
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    cache_writes: AtomicU64,
    cache_write_skips: AtomicU64,
    cache_invalidations: AtomicU64,
    
    // Skip index metrics
    skip_index_lookups: AtomicU64,
    skip_index_files_pruned: AtomicU64,
    skip_index_files_scanned: AtomicU64,
    skip_index_partition_hits: AtomicU64,
    skip_index_partition_misses: AtomicU64,
    
    // Query limiter metrics
    query_permits_acquired: AtomicU64,
    query_permits_denied: AtomicU64,
    query_queue_full_rejections: AtomicU64,
    query_wait_time_ms_total: AtomicU64,
    query_wait_time_samples: AtomicU64,
    
    // Sync metrics
    sync_operations: AtomicU64,
    sync_rows_total: AtomicU64,
    sync_bytes_total: AtomicU64,
    sync_failures: AtomicU64,
    
    // Billing metrics (for per-GB indexed pricing)
    /// Total bytes of source data indexed (for billing)
    indexed_source_bytes: AtomicU64,
    /// Total bytes of FST indexes stored
    indexed_fst_bytes: AtomicU64,
    /// Number of files indexed
    indexed_file_count: AtomicU64,
    
    // Predicate pushdown metrics
    /// Total predicates analyzed for pushdown
    pushdown_predicates_analyzed: AtomicU64,
    /// Predicates successfully pushed to source
    pushdown_predicates_pushed: AtomicU64,
    /// Predicates that required local evaluation
    pushdown_predicates_local: AtomicU64,
    /// Queries that benefited from pushdown
    pushdown_queries_optimized: AtomicU64,
    /// Queries where pushdown was not possible
    pushdown_queries_unoptimized: AtomicU64,
    /// Estimated rows filtered at source (before transfer)
    pushdown_rows_filtered_at_source: AtomicU64,
    /// Estimated bytes saved by pushdown (not transferred)
    pushdown_bytes_saved: AtomicU64,
    /// Warnings generated for pushdown limitations
    pushdown_warnings_generated: AtomicU64,

    // Derived table metrics
    derived_creates: AtomicU64,
    derived_refreshes: AtomicU64,
    derived_appends: AtomicU64,
    derived_deletes: AtomicU64,
    derived_failures: AtomicU64,
    derived_rows_materialized: AtomicU64,
    derived_bytes_materialized: AtomicU64,
    derived_duration_ms_total: AtomicU64,
}

impl WarehouseMetrics {
    /// Create a new metrics collector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register OTel observable instruments that export all warehouse metrics
    /// via the global meter provider.
    ///
    /// Call this once after `init_telemetry()` has configured the meter provider.
    /// The `self` reference must be wrapped in `Arc` so the callbacks can read
    /// the atomic counters during collection.
    pub fn register_otel_metrics(self: &Arc<Self>) {
        use opentelemetry::metrics::MeterProvider;

        let meter = opentelemetry::global::meter_provider().meter("reiver-pond");

        // --- Cache metrics ---
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_counter("pond.cache.hits")
                .with_description("Total cache hits")
                .with_callback(move |obs| { obs.observe(m.cache_hits.load(Ordering::Relaxed), &[]); })
                .build();
        }
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_counter("pond.cache.misses")
                .with_description("Total cache misses")
                .with_callback(move |obs| { obs.observe(m.cache_misses.load(Ordering::Relaxed), &[]); })
                .build();
        }
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_counter("pond.cache.writes")
                .with_description("Total cache writes")
                .with_callback(move |obs| { obs.observe(m.cache_writes.load(Ordering::Relaxed), &[]); })
                .build();
        }
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_counter("pond.cache.invalidations")
                .with_description("Total cache invalidations")
                .with_callback(move |obs| { obs.observe(m.cache_invalidations.load(Ordering::Relaxed), &[]); })
                .build();
        }

        // --- Skip index metrics ---
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_counter("pond.skip_index.lookups")
                .with_description("Total skip index lookups")
                .with_callback(move |obs| { obs.observe(m.skip_index_lookups.load(Ordering::Relaxed), &[]); })
                .build();
        }
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_counter("pond.skip_index.files_pruned")
                .with_description("Total files pruned by skip indexes")
                .with_callback(move |obs| { obs.observe(m.skip_index_files_pruned.load(Ordering::Relaxed), &[]); })
                .build();
        }
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_counter("pond.skip_index.files_scanned")
                .with_description("Total files scanned after skip index")
                .with_callback(move |obs| { obs.observe(m.skip_index_files_scanned.load(Ordering::Relaxed), &[]); })
                .build();
        }

        // --- Query limiter metrics ---
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_counter("pond.query.permits_acquired")
                .with_description("Total query permits acquired")
                .with_callback(move |obs| { obs.observe(m.query_permits_acquired.load(Ordering::Relaxed), &[]); })
                .build();
        }
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_counter("pond.query.permits_denied")
                .with_description("Total query permits denied")
                .with_callback(move |obs| { obs.observe(m.query_permits_denied.load(Ordering::Relaxed), &[]); })
                .build();
        }
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_counter("pond.query.queue_full_rejections")
                .with_description("Total query queue full rejections")
                .with_callback(move |obs| { obs.observe(m.query_queue_full_rejections.load(Ordering::Relaxed), &[]); })
                .build();
        }

        // --- Sync metrics ---
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_counter("pond.sync.operations")
                .with_description("Total sync operations")
                .with_callback(move |obs| { obs.observe(m.sync_operations.load(Ordering::Relaxed), &[]); })
                .build();
        }
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_counter("pond.sync.rows_total")
                .with_description("Total rows synced")
                .with_callback(move |obs| { obs.observe(m.sync_rows_total.load(Ordering::Relaxed), &[]); })
                .build();
        }
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_counter("pond.sync.bytes_total")
                .with_description("Total bytes synced")
                .with_callback(move |obs| { obs.observe(m.sync_bytes_total.load(Ordering::Relaxed), &[]); })
                .build();
        }
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_counter("pond.sync.failures")
                .with_description("Total sync failures")
                .with_callback(move |obs| { obs.observe(m.sync_failures.load(Ordering::Relaxed), &[]); })
                .build();
        }

        // --- Billing metrics ---
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_gauge("pond.billing.indexed_source_bytes")
                .with_description("Total bytes of source data indexed")
                .with_callback(move |obs| { obs.observe(m.indexed_source_bytes.load(Ordering::Relaxed), &[]); })
                .build();
        }
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_gauge("pond.billing.indexed_fst_bytes")
                .with_description("Total bytes of FST indexes stored")
                .with_callback(move |obs| { obs.observe(m.indexed_fst_bytes.load(Ordering::Relaxed), &[]); })
                .build();
        }
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_gauge("pond.billing.indexed_file_count")
                .with_description("Number of files indexed")
                .with_callback(move |obs| { obs.observe(m.indexed_file_count.load(Ordering::Relaxed), &[]); })
                .build();
        }

        // --- Predicate pushdown metrics ---
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_counter("pond.pushdown.predicates_analyzed")
                .with_description("Total predicates analyzed for pushdown")
                .with_callback(move |obs| { obs.observe(m.pushdown_predicates_analyzed.load(Ordering::Relaxed), &[]); })
                .build();
        }
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_counter("pond.pushdown.predicates_pushed")
                .with_description("Predicates successfully pushed to source")
                .with_callback(move |obs| { obs.observe(m.pushdown_predicates_pushed.load(Ordering::Relaxed), &[]); })
                .build();
        }
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_counter("pond.pushdown.queries_optimized")
                .with_description("Queries that benefited from pushdown")
                .with_callback(move |obs| { obs.observe(m.pushdown_queries_optimized.load(Ordering::Relaxed), &[]); })
                .build();
        }
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_counter("pond.pushdown.bytes_saved")
                .with_description("Estimated bytes saved by pushdown")
                .with_callback(move |obs| { obs.observe(m.pushdown_bytes_saved.load(Ordering::Relaxed), &[]); })
                .build();
        }

        // --- Derived table metrics ---
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_counter("pond.derived.creates")
                .with_description("Total derived table creates")
                .with_callback(move |obs| { obs.observe(m.derived_creates.load(Ordering::Relaxed), &[]); })
                .build();
        }
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_counter("pond.derived.refreshes")
                .with_description("Total derived table refreshes")
                .with_callback(move |obs| { obs.observe(m.derived_refreshes.load(Ordering::Relaxed), &[]); })
                .build();
        }
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_counter("pond.derived.appends")
                .with_description("Total derived table appends")
                .with_callback(move |obs| { obs.observe(m.derived_appends.load(Ordering::Relaxed), &[]); })
                .build();
        }
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_counter("pond.derived.deletes")
                .with_description("Total derived table deletes")
                .with_callback(move |obs| { obs.observe(m.derived_deletes.load(Ordering::Relaxed), &[]); })
                .build();
        }
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_counter("pond.derived.failures")
                .with_description("Total derived table operation failures")
                .with_callback(move |obs| { obs.observe(m.derived_failures.load(Ordering::Relaxed), &[]); })
                .build();
        }
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_counter("pond.derived.rows_materialized")
                .with_description("Total rows materialized for derived tables")
                .with_callback(move |obs| { obs.observe(m.derived_rows_materialized.load(Ordering::Relaxed), &[]); })
                .build();
        }
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_counter("pond.derived.bytes_materialized")
                .with_description("Total bytes materialized for derived tables")
                .with_callback(move |obs| { obs.observe(m.derived_bytes_materialized.load(Ordering::Relaxed), &[]); })
                .build();
        }
        {
            let m = Arc::clone(self);
            let _ = meter.u64_observable_counter("pond.derived.duration_ms_total")
                .with_description("Total materialization duration for derived tables (ms)")
                .with_callback(move |obs| { obs.observe(m.derived_duration_ms_total.load(Ordering::Relaxed), &[]); })
                .build();
        }

        tracing::info!("Registered OTel observable metrics for Pond warehouse");
    }

    // =========================================================================
    // Cache Metrics
    // =========================================================================
    
    /// Record a cache hit.
    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Record a cache miss.
    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Record a cache write.
    pub fn record_cache_write(&self) {
        self.cache_writes.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Record a cache write that was skipped (e.g., result too large).
    pub fn record_cache_write_skip(&self) {
        self.cache_write_skips.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Record a cache invalidation.
    pub fn record_cache_invalidation(&self) {
        self.cache_invalidations.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Get the cache hit rate (0.0 to 1.0).
    pub fn cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }
    
    // =========================================================================
    // Skip Index Metrics
    // =========================================================================
    
    /// Record a skip index lookup with results.
    pub fn record_skip_index_lookup(&self, files_pruned: u64, files_scanned: u64) {
        self.skip_index_lookups.fetch_add(1, Ordering::Relaxed);
        self.skip_index_files_pruned.fetch_add(files_pruned, Ordering::Relaxed);
        self.skip_index_files_scanned.fetch_add(files_scanned, Ordering::Relaxed);
    }
    
    /// Record a partition-level skip index result.
    pub fn record_partition_lookup(&self, hit: bool) {
        if hit {
            self.skip_index_partition_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.skip_index_partition_misses.fetch_add(1, Ordering::Relaxed);
        }
    }
    
    /// Get the skip index prune rate (files pruned / total files).
    pub fn skip_index_prune_rate(&self) -> f64 {
        let pruned = self.skip_index_files_pruned.load(Ordering::Relaxed);
        let scanned = self.skip_index_files_scanned.load(Ordering::Relaxed);
        let total = pruned + scanned;
        if total == 0 {
            0.0
        } else {
            pruned as f64 / total as f64
        }
    }
    
    // =========================================================================
    // Query Limiter Metrics
    // =========================================================================
    
    /// Record a successful permit acquisition with wait time.
    pub fn record_permit_acquired(&self, wait_time: Duration) {
        self.query_permits_acquired.fetch_add(1, Ordering::Relaxed);
        self.query_wait_time_ms_total.fetch_add(wait_time.as_millis() as u64, Ordering::Relaxed);
        self.query_wait_time_samples.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Record a denied permit (timeout or shutdown).
    pub fn record_permit_denied(&self) {
        self.query_permits_denied.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Record a queue full rejection.
    pub fn record_queue_full_rejection(&self) {
        self.query_queue_full_rejections.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Get the average query wait time.
    pub fn average_wait_time_ms(&self) -> f64 {
        let total = self.query_wait_time_ms_total.load(Ordering::Relaxed);
        let samples = self.query_wait_time_samples.load(Ordering::Relaxed);
        if samples == 0 {
            0.0
        } else {
            total as f64 / samples as f64
        }
    }
    
    // =========================================================================
    // Sync Metrics
    // =========================================================================
    
    /// Record a sync operation.
    pub fn record_sync(&self, rows: u64, bytes: u64, success: bool) {
        self.sync_operations.fetch_add(1, Ordering::Relaxed);
        if success {
            self.sync_rows_total.fetch_add(rows, Ordering::Relaxed);
            self.sync_bytes_total.fetch_add(bytes, Ordering::Relaxed);
        } else {
            self.sync_failures.fetch_add(1, Ordering::Relaxed);
        }
    }
    
    // =========================================================================
    // Billing Metrics
    // =========================================================================
    
    /// Record indexed data for billing purposes.
    ///
    /// This tracks the amount of source data indexed and the size of the FST indexes
    /// created. Used for per-GB-indexed billing model.
    ///
    /// # Arguments
    /// * `source_bytes` - Size of the source Parquet file indexed
    /// * `fst_bytes` - Size of the FST index created
    pub fn record_indexed_bytes(&self, source_bytes: u64, fst_bytes: u64) {
        self.indexed_source_bytes.fetch_add(source_bytes, Ordering::Relaxed);
        self.indexed_fst_bytes.fetch_add(fst_bytes, Ordering::Relaxed);
        self.indexed_file_count.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Record multiple files indexed in a batch.
    pub fn record_indexed_batch(&self, source_bytes: u64, fst_bytes: u64, file_count: u64) {
        self.indexed_source_bytes.fetch_add(source_bytes, Ordering::Relaxed);
        self.indexed_fst_bytes.fetch_add(fst_bytes, Ordering::Relaxed);
        self.indexed_file_count.fetch_add(file_count, Ordering::Relaxed);
    }
    
    /// Get total source bytes indexed (for billing).
    pub fn total_indexed_source_bytes(&self) -> u64 {
        self.indexed_source_bytes.load(Ordering::Relaxed)
    }
    
    /// Get total FST bytes stored.
    pub fn total_indexed_fst_bytes(&self) -> u64 {
        self.indexed_fst_bytes.load(Ordering::Relaxed)
    }
    
    /// Get total indexed source bytes in GB (for billing display).
    pub fn indexed_source_gb(&self) -> f64 {
        self.indexed_source_bytes.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0 * 1024.0)
    }
    
    // =========================================================================
    // Predicate Pushdown Metrics
    // =========================================================================
    
    /// Record predicate pushdown analysis results.
    ///
    /// # Arguments
    /// * `total_predicates` - Total number of predicates analyzed
    /// * `pushed` - Number of predicates successfully pushed to source
    /// * `local` - Number of predicates requiring local evaluation
    pub fn record_pushdown_analysis(&self, total_predicates: u64, pushed: u64, local: u64) {
        self.pushdown_predicates_analyzed.fetch_add(total_predicates, Ordering::Relaxed);
        self.pushdown_predicates_pushed.fetch_add(pushed, Ordering::Relaxed);
        self.pushdown_predicates_local.fetch_add(local, Ordering::Relaxed);
        
        // Track whether this query benefited from pushdown
        if pushed > 0 {
            self.pushdown_queries_optimized.fetch_add(1, Ordering::Relaxed);
        } else if total_predicates > 0 {
            self.pushdown_queries_unoptimized.fetch_add(1, Ordering::Relaxed);
        }
    }
    
    /// Record estimated data savings from predicate pushdown.
    ///
    /// # Arguments
    /// * `rows_filtered` - Estimated rows filtered at source
    /// * `bytes_saved` - Estimated bytes not transferred due to source filtering
    pub fn record_pushdown_savings(&self, rows_filtered: u64, bytes_saved: u64) {
        self.pushdown_rows_filtered_at_source.fetch_add(rows_filtered, Ordering::Relaxed);
        self.pushdown_bytes_saved.fetch_add(bytes_saved, Ordering::Relaxed);
    }
    
    /// Record a pushdown warning.
    pub fn record_pushdown_warning(&self) {
        self.pushdown_warnings_generated.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Record multiple pushdown warnings.
    pub fn record_pushdown_warnings(&self, count: u64) {
        self.pushdown_warnings_generated.fetch_add(count, Ordering::Relaxed);
    }

    // =========================================================================
    // Derived Table Metrics
    // =========================================================================

    /// Record a derived table create.
    pub fn record_derived_create(&self) {
        self.derived_creates.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a derived table refresh (rows, bytes, duration_ms).
    pub fn record_derived_refresh(&self, rows: u64, bytes: u64, duration_ms: u64) {
        self.derived_refreshes.fetch_add(1, Ordering::Relaxed);
        self.derived_rows_materialized.fetch_add(rows, Ordering::Relaxed);
        self.derived_bytes_materialized.fetch_add(bytes, Ordering::Relaxed);
        self.derived_duration_ms_total.fetch_add(duration_ms, Ordering::Relaxed);
    }

    /// Record a derived table append (rows, bytes, duration_ms).
    pub fn record_derived_append(&self, rows: u64, bytes: u64, duration_ms: u64) {
        self.derived_appends.fetch_add(1, Ordering::Relaxed);
        self.derived_rows_materialized.fetch_add(rows, Ordering::Relaxed);
        self.derived_bytes_materialized.fetch_add(bytes, Ordering::Relaxed);
        self.derived_duration_ms_total.fetch_add(duration_ms, Ordering::Relaxed);
    }

    /// Record a derived table delete.
    pub fn record_derived_delete(&self) {
        self.derived_deletes.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a derived table operation failure.
    pub fn record_derived_failure(&self) {
        self.derived_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the predicate pushdown rate (pushed / total analyzed).
    pub fn pushdown_rate(&self) -> f64 {
        let pushed = self.pushdown_predicates_pushed.load(Ordering::Relaxed);
        let total = self.pushdown_predicates_analyzed.load(Ordering::Relaxed);
        if total == 0 {
            0.0
        } else {
            pushed as f64 / total as f64
        }
    }
    
    /// Get the query optimization rate (optimized / total with predicates).
    pub fn query_optimization_rate(&self) -> f64 {
        let optimized = self.pushdown_queries_optimized.load(Ordering::Relaxed);
        let unoptimized = self.pushdown_queries_unoptimized.load(Ordering::Relaxed);
        let total = optimized + unoptimized;
        if total == 0 {
            0.0
        } else {
            optimized as f64 / total as f64
        }
    }
    
    /// Get estimated bytes saved by pushdown in GB.
    pub fn pushdown_bytes_saved_gb(&self) -> f64 {
        self.pushdown_bytes_saved.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0 * 1024.0)
    }
    
    // =========================================================================
    // Snapshot
    // =========================================================================
    
    /// Get a snapshot of all metrics.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            cache_writes: self.cache_writes.load(Ordering::Relaxed),
            cache_write_skips: self.cache_write_skips.load(Ordering::Relaxed),
            cache_invalidations: self.cache_invalidations.load(Ordering::Relaxed),
            cache_hit_rate: self.cache_hit_rate(),
            
            skip_index_lookups: self.skip_index_lookups.load(Ordering::Relaxed),
            skip_index_files_pruned: self.skip_index_files_pruned.load(Ordering::Relaxed),
            skip_index_files_scanned: self.skip_index_files_scanned.load(Ordering::Relaxed),
            skip_index_partition_hits: self.skip_index_partition_hits.load(Ordering::Relaxed),
            skip_index_partition_misses: self.skip_index_partition_misses.load(Ordering::Relaxed),
            skip_index_prune_rate: self.skip_index_prune_rate(),
            
            query_permits_acquired: self.query_permits_acquired.load(Ordering::Relaxed),
            query_permits_denied: self.query_permits_denied.load(Ordering::Relaxed),
            query_queue_full_rejections: self.query_queue_full_rejections.load(Ordering::Relaxed),
            query_wait_time_avg_ms: self.average_wait_time_ms(),
            
            sync_operations: self.sync_operations.load(Ordering::Relaxed),
            sync_rows_total: self.sync_rows_total.load(Ordering::Relaxed),
            sync_bytes_total: self.sync_bytes_total.load(Ordering::Relaxed),
            sync_failures: self.sync_failures.load(Ordering::Relaxed),
            
            indexed_source_bytes: self.indexed_source_bytes.load(Ordering::Relaxed),
            indexed_fst_bytes: self.indexed_fst_bytes.load(Ordering::Relaxed),
            indexed_file_count: self.indexed_file_count.load(Ordering::Relaxed),
            indexed_source_gb: self.indexed_source_gb(),
            
            pushdown_predicates_analyzed: self.pushdown_predicates_analyzed.load(Ordering::Relaxed),
            pushdown_predicates_pushed: self.pushdown_predicates_pushed.load(Ordering::Relaxed),
            pushdown_predicates_local: self.pushdown_predicates_local.load(Ordering::Relaxed),
            pushdown_queries_optimized: self.pushdown_queries_optimized.load(Ordering::Relaxed),
            pushdown_queries_unoptimized: self.pushdown_queries_unoptimized.load(Ordering::Relaxed),
            pushdown_rows_filtered_at_source: self.pushdown_rows_filtered_at_source.load(Ordering::Relaxed),
            pushdown_bytes_saved: self.pushdown_bytes_saved.load(Ordering::Relaxed),
            pushdown_warnings_generated: self.pushdown_warnings_generated.load(Ordering::Relaxed),
            pushdown_rate: self.pushdown_rate(),
            query_optimization_rate: self.query_optimization_rate(),
            pushdown_bytes_saved_gb: self.pushdown_bytes_saved_gb(),

            derived_creates: self.derived_creates.load(Ordering::Relaxed),
            derived_refreshes: self.derived_refreshes.load(Ordering::Relaxed),
            derived_appends: self.derived_appends.load(Ordering::Relaxed),
            derived_deletes: self.derived_deletes.load(Ordering::Relaxed),
            derived_failures: self.derived_failures.load(Ordering::Relaxed),
            derived_rows_materialized: self.derived_rows_materialized.load(Ordering::Relaxed),
            derived_bytes_materialized: self.derived_bytes_materialized.load(Ordering::Relaxed),
            derived_duration_ms_total: self.derived_duration_ms_total.load(Ordering::Relaxed),
        }
    }
    
    /// Reset all metrics to zero.
    pub fn reset(&self) {
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
        self.cache_writes.store(0, Ordering::Relaxed);
        self.cache_write_skips.store(0, Ordering::Relaxed);
        self.cache_invalidations.store(0, Ordering::Relaxed);
        
        self.skip_index_lookups.store(0, Ordering::Relaxed);
        self.skip_index_files_pruned.store(0, Ordering::Relaxed);
        self.skip_index_files_scanned.store(0, Ordering::Relaxed);
        self.skip_index_partition_hits.store(0, Ordering::Relaxed);
        self.skip_index_partition_misses.store(0, Ordering::Relaxed);
        
        self.query_permits_acquired.store(0, Ordering::Relaxed);
        self.query_permits_denied.store(0, Ordering::Relaxed);
        self.query_queue_full_rejections.store(0, Ordering::Relaxed);
        self.query_wait_time_ms_total.store(0, Ordering::Relaxed);
        self.query_wait_time_samples.store(0, Ordering::Relaxed);
        
        self.sync_operations.store(0, Ordering::Relaxed);
        self.sync_rows_total.store(0, Ordering::Relaxed);
        self.sync_bytes_total.store(0, Ordering::Relaxed);
        self.sync_failures.store(0, Ordering::Relaxed);
        
        self.indexed_source_bytes.store(0, Ordering::Relaxed);
        self.indexed_fst_bytes.store(0, Ordering::Relaxed);
        self.indexed_file_count.store(0, Ordering::Relaxed);
        
        self.pushdown_predicates_analyzed.store(0, Ordering::Relaxed);
        self.pushdown_predicates_pushed.store(0, Ordering::Relaxed);
        self.pushdown_predicates_local.store(0, Ordering::Relaxed);
        self.pushdown_queries_optimized.store(0, Ordering::Relaxed);
        self.pushdown_queries_unoptimized.store(0, Ordering::Relaxed);
        self.pushdown_rows_filtered_at_source.store(0, Ordering::Relaxed);
        self.pushdown_bytes_saved.store(0, Ordering::Relaxed);
        self.pushdown_warnings_generated.store(0, Ordering::Relaxed);

        self.derived_creates.store(0, Ordering::Relaxed);
        self.derived_refreshes.store(0, Ordering::Relaxed);
        self.derived_appends.store(0, Ordering::Relaxed);
        self.derived_deletes.store(0, Ordering::Relaxed);
        self.derived_failures.store(0, Ordering::Relaxed);
        self.derived_rows_materialized.store(0, Ordering::Relaxed);
        self.derived_bytes_materialized.store(0, Ordering::Relaxed);
        self.derived_duration_ms_total.store(0, Ordering::Relaxed);
    }
}

/// A point-in-time snapshot of warehouse metrics.
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    // Cache
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_writes: u64,
    pub cache_write_skips: u64,
    pub cache_invalidations: u64,
    pub cache_hit_rate: f64,
    
    // Skip index
    pub skip_index_lookups: u64,
    pub skip_index_files_pruned: u64,
    pub skip_index_files_scanned: u64,
    pub skip_index_partition_hits: u64,
    pub skip_index_partition_misses: u64,
    pub skip_index_prune_rate: f64,
    
    // Query limiter
    pub query_permits_acquired: u64,
    pub query_permits_denied: u64,
    pub query_queue_full_rejections: u64,
    pub query_wait_time_avg_ms: f64,
    
    // Sync
    pub sync_operations: u64,
    pub sync_rows_total: u64,
    pub sync_bytes_total: u64,
    pub sync_failures: u64,
    
    // Billing (per-GB indexed)
    /// Total bytes of source data indexed
    pub indexed_source_bytes: u64,
    /// Total bytes of FST indexes stored
    pub indexed_fst_bytes: u64,
    /// Total number of files indexed
    pub indexed_file_count: u64,
    /// Indexed source data in GB (for billing)
    pub indexed_source_gb: f64,
    
    // Predicate pushdown metrics
    /// Total predicates analyzed for pushdown
    pub pushdown_predicates_analyzed: u64,
    /// Predicates successfully pushed to source
    pub pushdown_predicates_pushed: u64,
    /// Predicates that required local evaluation
    pub pushdown_predicates_local: u64,
    /// Queries that benefited from pushdown
    pub pushdown_queries_optimized: u64,
    /// Queries where pushdown was not possible
    pub pushdown_queries_unoptimized: u64,
    /// Estimated rows filtered at source
    pub pushdown_rows_filtered_at_source: u64,
    /// Estimated bytes saved by pushdown
    pub pushdown_bytes_saved: u64,
    /// Warnings generated for pushdown limitations
    pub pushdown_warnings_generated: u64,
    /// Predicate pushdown rate (pushed / analyzed)
    pub pushdown_rate: f64,
    /// Query optimization rate (optimized / total with predicates)
    pub query_optimization_rate: f64,
    /// Bytes saved by pushdown in GB
    pub pushdown_bytes_saved_gb: f64,

    // Derived tables
    pub derived_creates: u64,
    pub derived_refreshes: u64,
    pub derived_appends: u64,
    pub derived_deletes: u64,
    pub derived_failures: u64,
    pub derived_rows_materialized: u64,
    pub derived_bytes_materialized: u64,
    pub derived_duration_ms_total: u64,
}

/// Guard for timing operations.
/// 
/// Records the elapsed time when dropped.
pub struct TimingGuard<'a> {
    metrics: &'a WarehouseMetrics,
    start: Instant,
    operation: TimedOperation,
}

/// Operations that can be timed.
#[derive(Debug, Clone, Copy)]
pub enum TimedOperation {
    /// Time spent waiting for a query permit.
    QueryWait,
}

impl<'a> TimingGuard<'a> {
    /// Create a new timing guard.
    pub fn new(metrics: &'a WarehouseMetrics, operation: TimedOperation) -> Self {
        Self {
            metrics,
            start: Instant::now(),
            operation,
        }
    }
    
    /// Finish timing and record as success.
    pub fn finish_success(self) {
        let elapsed = self.start.elapsed();
        match self.operation {
            TimedOperation::QueryWait => {
                self.metrics.record_permit_acquired(elapsed);
            }
        }
        // Prevent drop from running
        std::mem::forget(self);
    }
    
    /// Finish timing and record as failure.
    pub fn finish_failure(self) {
        match self.operation {
            TimedOperation::QueryWait => {
                self.metrics.record_permit_denied();
            }
        }
        // Prevent drop from running
        std::mem::forget(self);
    }
}

impl Drop for TimingGuard<'_> {
    fn drop(&mut self) {
        // If dropped without calling finish, record as failure
        match self.operation {
            TimedOperation::QueryWait => {
                self.metrics.record_permit_denied();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_cache_metrics() {
        let metrics = WarehouseMetrics::new();
        
        metrics.record_cache_hit();
        metrics.record_cache_hit();
        metrics.record_cache_miss();
        
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.cache_hits, 2);
        assert_eq!(snapshot.cache_misses, 1);
        assert!((snapshot.cache_hit_rate - 0.666).abs() < 0.01);
    }
    
    #[test]
    fn test_skip_index_metrics() {
        let metrics = WarehouseMetrics::new();
        
        // Lookup that pruned 80 of 100 files
        metrics.record_skip_index_lookup(80, 20);
        
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.skip_index_files_pruned, 80);
        assert_eq!(snapshot.skip_index_files_scanned, 20);
        assert!((snapshot.skip_index_prune_rate - 0.8).abs() < 0.01);
    }
    
    #[test]
    fn test_reset() {
        let metrics = WarehouseMetrics::new();
        
        metrics.record_cache_hit();
        metrics.record_sync(1000, 10000, true);
        
        metrics.reset();
        
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.cache_hits, 0);
        assert_eq!(snapshot.sync_operations, 0);
    }
    
    #[test]
    fn test_billing_metrics() {
        let metrics = WarehouseMetrics::new();
        
        // Record some indexed data
        metrics.record_indexed_bytes(1024 * 1024 * 100, 1024 * 1024); // 100MB source, 1MB FST
        metrics.record_indexed_bytes(1024 * 1024 * 200, 1024 * 1024 * 2); // 200MB source, 2MB FST
        
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.indexed_source_bytes, 1024 * 1024 * 300); // 300MB total
        assert_eq!(snapshot.indexed_fst_bytes, 1024 * 1024 * 3); // 3MB total
        assert_eq!(snapshot.indexed_file_count, 2);
        
        // Check GB calculation
        assert!((snapshot.indexed_source_gb - 0.293).abs() < 0.01); // ~300MB = 0.293GB
    }
    
    #[test]
    fn test_billing_metrics_batch() {
        let metrics = WarehouseMetrics::new();
        
        // Record a batch of 10 files
        metrics.record_indexed_batch(
            1024 * 1024 * 1024, // 1GB source
            1024 * 1024 * 10,   // 10MB FST
            10                   // 10 files
        );
        
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.indexed_file_count, 10);
        assert!((snapshot.indexed_source_gb - 1.0).abs() < 0.01);
    }
    
    #[test]
    fn test_billing_metrics_reset() {
        let metrics = WarehouseMetrics::new();
        
        metrics.record_indexed_bytes(1024 * 1024 * 100, 1024 * 1024);
        
        metrics.reset();
        
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.indexed_source_bytes, 0);
        assert_eq!(snapshot.indexed_fst_bytes, 0);
        assert_eq!(snapshot.indexed_file_count, 0);
    }
    
    #[test]
    fn test_pushdown_metrics() {
        let metrics = WarehouseMetrics::new();
        
        // Record pushdown analysis for a query with 5 predicates:
        // 3 pushed to source, 2 evaluated locally
        metrics.record_pushdown_analysis(5, 3, 2);
        
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.pushdown_predicates_analyzed, 5);
        assert_eq!(snapshot.pushdown_predicates_pushed, 3);
        assert_eq!(snapshot.pushdown_predicates_local, 2);
        assert_eq!(snapshot.pushdown_queries_optimized, 1);
        assert_eq!(snapshot.pushdown_queries_unoptimized, 0);
        assert!((snapshot.pushdown_rate - 0.6).abs() < 0.01);
    }
    
    #[test]
    fn test_pushdown_metrics_unoptimized_query() {
        let metrics = WarehouseMetrics::new();
        
        // Query where no predicates could be pushed
        metrics.record_pushdown_analysis(3, 0, 3);
        
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.pushdown_queries_optimized, 0);
        assert_eq!(snapshot.pushdown_queries_unoptimized, 1);
        assert!((snapshot.pushdown_rate - 0.0).abs() < 0.01);
    }
    
    #[test]
    fn test_pushdown_savings() {
        let metrics = WarehouseMetrics::new();
        
        // Record savings: 1M rows filtered, 100MB saved
        metrics.record_pushdown_savings(1_000_000, 100 * 1024 * 1024);
        
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.pushdown_rows_filtered_at_source, 1_000_000);
        assert_eq!(snapshot.pushdown_bytes_saved, 100 * 1024 * 1024);
        assert!((snapshot.pushdown_bytes_saved_gb - 0.0977).abs() < 0.01);
    }
    
    #[test]
    fn test_pushdown_warnings() {
        let metrics = WarehouseMetrics::new();
        
        metrics.record_pushdown_warning();
        metrics.record_pushdown_warnings(3);
        
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.pushdown_warnings_generated, 4);
    }
    
    #[test]
    fn test_pushdown_metrics_reset() {
        let metrics = WarehouseMetrics::new();
        
        metrics.record_pushdown_analysis(10, 8, 2);
        metrics.record_pushdown_savings(1000, 10000);
        metrics.record_pushdown_warning();
        
        metrics.reset();
        
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.pushdown_predicates_analyzed, 0);
        assert_eq!(snapshot.pushdown_predicates_pushed, 0);
        assert_eq!(snapshot.pushdown_predicates_local, 0);
        assert_eq!(snapshot.pushdown_queries_optimized, 0);
        assert_eq!(snapshot.pushdown_queries_unoptimized, 0);
        assert_eq!(snapshot.pushdown_rows_filtered_at_source, 0);
        assert_eq!(snapshot.pushdown_bytes_saved, 0);
        assert_eq!(snapshot.pushdown_warnings_generated, 0);
    }
    
    #[test]
    fn test_query_optimization_rate() {
        let metrics = WarehouseMetrics::new();
        
        // 3 optimized queries, 1 unoptimized = 75% rate
        metrics.record_pushdown_analysis(5, 3, 2); // optimized
        metrics.record_pushdown_analysis(2, 2, 0); // optimized
        metrics.record_pushdown_analysis(3, 1, 2); // optimized
        metrics.record_pushdown_analysis(4, 0, 4); // unoptimized
        
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.pushdown_queries_optimized, 3);
        assert_eq!(snapshot.pushdown_queries_unoptimized, 1);
        assert!((snapshot.query_optimization_rate - 0.75).abs() < 0.01);
    }
}

// =============================================================================
// PER-QUERY METRICS
// =============================================================================

/// Metrics for a single query execution.
///
/// Captures detailed performance information about a query, including:
/// - File scanning statistics (files scanned vs skipped)
/// - Cache performance
/// - Execution timing breakdown
///
/// These metrics are useful for:
/// - Query optimization (identify queries that scan too many files)
/// - Capacity planning (track data volumes)
/// - Performance debugging (identify slow query stages)
#[derive(Debug, Clone, Default)]
pub struct QueryMetrics {
    // File scanning metrics
    /// Total number of files that could have been scanned
    pub total_files: u32,
    /// Files actually scanned by the query
    pub files_scanned: u32,
    /// Files skipped by FST skip index
    pub files_skipped_by_fst: u32,
    /// Files skipped by numeric stats index
    pub files_skipped_by_numeric: u32,
    /// Files skipped by partition pruning
    pub files_skipped_by_partition: u32,
    
    // Row group metrics (Parquet-specific)
    /// Total row groups across scanned files
    pub total_row_groups: u32,
    /// Row groups scanned
    pub row_groups_scanned: u32,
    /// Row groups skipped by predicate pushdown
    pub row_groups_skipped: u32,
    
    // Cache metrics
    /// Whether the query was served from cache
    pub cache_hit: bool,
    /// The cache tier used (if cached)
    pub cache_tier: Option<String>,
    
    // Execution timing (milliseconds)
    /// Total query execution time
    pub execution_time_ms: u64,
    /// Time spent in skip index lookup
    pub skip_index_time_ms: u64,
    /// Time spent in query planning/rewriting
    pub planning_time_ms: u64,
    /// Time spent executing the query in ClickHouse
    pub clickhouse_time_ms: u64,
    /// Time spent streaming results
    pub streaming_time_ms: u64,
    
    // Data volume
    /// Rows returned by the query
    pub rows_returned: u64,
    /// Bytes scanned (estimated)
    pub bytes_scanned: u64,
    /// Bytes returned
    pub bytes_returned: u64,
    
    // Predicate pushdown metrics (per-query)
    /// Total predicates in the query
    pub pushdown_total_predicates: u32,
    /// Predicates pushed to source
    pub pushdown_pushed_predicates: u32,
    /// Predicates evaluated locally
    pub pushdown_local_predicates: u32,
    /// Estimated rows filtered at source
    pub pushdown_rows_filtered: u64,
    /// Estimated bytes saved by pushdown
    pub pushdown_bytes_saved: u64,
    /// Warnings generated for this query
    pub pushdown_warnings_count: u32,
    
    // Scan efficiency metrics
    /// Total rows scanned by the query engine
    pub rows_scanned: u64,
    /// Semi-join key reduction ratio (keys_after / keys_before)
    pub semi_join_reduction_ratio: Option<f64>,
    /// Bloom filter estimated false-positive rate
    pub bloom_filter_estimated_fpr: Option<f64>,
    /// Number of federated sources involved
    pub federation_source_count: u32,
    /// Time spent in SQL rewriting (separate from planning)
    pub rewrite_time_ms: u64,
}

impl QueryMetrics {
    /// Create new metrics for a query.
    pub fn new() -> Self {
        Self::default()
    }

    /// Calculate the skip ratio (percentage of files skipped).
    ///
    /// Returns a value between 0.0 (no files skipped) and 1.0 (all files skipped).
    pub fn skip_ratio(&self) -> f32 {
        if self.total_files == 0 {
            return 0.0;
        }
        let total_skipped = self.files_skipped_by_fst
            + self.files_skipped_by_numeric
            + self.files_skipped_by_partition;
        total_skipped as f32 / self.total_files as f32
    }

    /// Check if the query is performing efficiently.
    ///
    /// A query is considered efficient if:
    /// - Skip ratio >= 50% (skipping at least half the files)
    /// - Or it's a cache hit
    pub fn is_efficient(&self) -> bool {
        self.cache_hit || self.skip_ratio() >= 0.5
    }

    /// Check if the query might need optimization.
    ///
    /// Returns true if:
    /// - Scanning more than 1000 files
    /// - Skip ratio < 20%
    /// - Execution time > 5 seconds
    /// - Pushdown rate < 50% (many predicates evaluated locally)
    pub fn needs_optimization(&self) -> bool {
        if self.cache_hit {
            return false;
        }
        
        self.files_scanned > 1000 
            || (self.total_files > 10 && self.skip_ratio() < 0.2)
            || self.execution_time_ms > 5000
            || (self.pushdown_total_predicates > 2 && self.pushdown_ratio() < 0.5)
    }
    
    /// Calculate the pushdown ratio (pushed / total predicates).
    ///
    /// Returns a value between 0.0 (no predicates pushed) and 1.0 (all predicates pushed).
    pub fn pushdown_ratio(&self) -> f32 {
        if self.pushdown_total_predicates == 0 {
            return 1.0; // No predicates = nothing to push
        }
        self.pushdown_pushed_predicates as f32 / self.pushdown_total_predicates as f32
    }
    
    /// Check if the query benefited from predicate pushdown.
    pub fn benefited_from_pushdown(&self) -> bool {
        self.pushdown_pushed_predicates > 0
    }

    /// Scan amplification: bytes_scanned / bytes_returned.
    /// Values close to 1.0 mean efficient scanning; high values mean wasted I/O.
    pub fn scan_amplification(&self) -> f64 {
        if self.bytes_returned == 0 { return 0.0; }
        self.bytes_scanned as f64 / self.bytes_returned as f64
    }

    /// Row selectivity: rows_returned / rows_scanned.
    /// Values close to 1.0 mean most scanned rows were returned.
    pub fn row_selectivity(&self) -> f64 {
        if self.rows_scanned == 0 { return 1.0; }
        self.rows_returned as f64 / self.rows_scanned as f64
    }

    /// Log the metrics at appropriate level.
    pub fn log(&self) {
        if self.needs_optimization() {
            tracing::warn!(
                files_scanned = self.files_scanned,
                total_files = self.total_files,
                skip_ratio = %format!("{:.1}%", self.skip_ratio() * 100.0),
                pushdown_ratio = %format!("{:.1}%", self.pushdown_ratio() * 100.0),
                predicates_pushed = self.pushdown_pushed_predicates,
                predicates_local = self.pushdown_local_predicates,
                execution_time_ms = self.execution_time_ms,
                planning_time_ms = self.planning_time_ms,
                clickhouse_time_ms = self.clickhouse_time_ms,
                rows_scanned = self.rows_scanned,
                rows_returned = self.rows_returned,
                bytes_scanned = self.bytes_scanned,
                scan_amplification = %format!("{:.2}", self.scan_amplification()),
                row_selectivity = %format!("{:.2}", self.row_selectivity()),
                "Query may need optimization - high file scan count, low skip ratio, or low pushdown rate"
            );
        } else {
            tracing::debug!(
                files_scanned = self.files_scanned,
                files_skipped = self.files_skipped_by_fst + self.files_skipped_by_numeric + self.files_skipped_by_partition,
                cache_hit = self.cache_hit,
                execution_time_ms = self.execution_time_ms,
                planning_time_ms = self.planning_time_ms,
                clickhouse_time_ms = self.clickhouse_time_ms,
                rows_scanned = self.rows_scanned,
                rows_returned = self.rows_returned,
                bytes_scanned = self.bytes_scanned,
                scan_amplification = %format!("{:.2}", self.scan_amplification()),
                row_selectivity = %format!("{:.2}", self.row_selectivity()),
                predicates_pushed = self.pushdown_pushed_predicates,
                pushdown_bytes_saved = self.pushdown_bytes_saved,
                "Query execution metrics"
            );
        }
    }

    /// Merge metrics from another query (for aggregate reporting).
    pub fn merge(&mut self, other: &QueryMetrics) {
        self.total_files += other.total_files;
        self.files_scanned += other.files_scanned;
        self.files_skipped_by_fst += other.files_skipped_by_fst;
        self.files_skipped_by_numeric += other.files_skipped_by_numeric;
        self.files_skipped_by_partition += other.files_skipped_by_partition;
        self.total_row_groups += other.total_row_groups;
        self.row_groups_scanned += other.row_groups_scanned;
        self.row_groups_skipped += other.row_groups_skipped;
        self.execution_time_ms += other.execution_time_ms;
        self.skip_index_time_ms += other.skip_index_time_ms;
        self.planning_time_ms += other.planning_time_ms;
        self.clickhouse_time_ms += other.clickhouse_time_ms;
        self.streaming_time_ms += other.streaming_time_ms;
        self.rows_returned += other.rows_returned;
        self.bytes_scanned += other.bytes_scanned;
        self.bytes_returned += other.bytes_returned;
        
        // Pushdown metrics
        self.pushdown_total_predicates += other.pushdown_total_predicates;
        self.pushdown_pushed_predicates += other.pushdown_pushed_predicates;
        self.pushdown_local_predicates += other.pushdown_local_predicates;
        self.pushdown_rows_filtered += other.pushdown_rows_filtered;
        self.pushdown_bytes_saved += other.pushdown_bytes_saved;
        self.pushdown_warnings_count += other.pushdown_warnings_count;
        
        self.rows_scanned += other.rows_scanned;
        self.federation_source_count += other.federation_source_count;
        self.rewrite_time_ms += other.rewrite_time_ms;
        if self.semi_join_reduction_ratio.is_none() {
            self.semi_join_reduction_ratio = other.semi_join_reduction_ratio;
        }
        if self.bloom_filter_estimated_fpr.is_none() {
            self.bloom_filter_estimated_fpr = other.bloom_filter_estimated_fpr;
        }
    }
}

/// Slow query analyzer for identifying queries that need optimization.
#[derive(Debug, Clone)]
pub struct SlowQueryAnalyzer {
    /// Maximum files threshold - queries scanning more are flagged
    pub max_files_threshold: u32,
    /// Minimum acceptable skip ratio (0.0 - 1.0)
    pub min_skip_ratio: f32,
    /// Maximum execution time threshold (ms)
    pub max_execution_time_ms: u64,
    /// Minimum acceptable pushdown ratio (0.0 - 1.0)
    pub min_pushdown_ratio: f32,
}

impl Default for SlowQueryAnalyzer {
    fn default() -> Self {
        Self {
            max_files_threshold: 1000,
            min_skip_ratio: 0.5,
            max_execution_time_ms: 5000,
            min_pushdown_ratio: 0.5,
        }
    }
}

impl SlowQueryAnalyzer {
    /// Create a new slow query analyzer with custom thresholds.
    pub fn new(max_files: u32, min_skip_ratio: f32, max_time_ms: u64) -> Self {
        Self {
            max_files_threshold: max_files,
            min_skip_ratio,
            max_execution_time_ms: max_time_ms,
            min_pushdown_ratio: 0.5,
        }
    }
    
    /// Create a new slow query analyzer with all thresholds.
    pub fn with_pushdown_threshold(mut self, min_pushdown_ratio: f32) -> Self {
        self.min_pushdown_ratio = min_pushdown_ratio;
        self
    }

    /// Analyze a query and return optimization suggestions.
    pub fn analyze(&self, metrics: &QueryMetrics) -> Option<Vec<String>> {
        if metrics.cache_hit {
            return None; // Cache hits don't need optimization
        }

        let mut suggestions = Vec::new();

        // Check file count
        if metrics.files_scanned > self.max_files_threshold {
            suggestions.push(format!(
                "Scanning {} files (threshold: {}). Consider adding partition filters.",
                metrics.files_scanned, self.max_files_threshold
            ));
        }

        // Check skip ratio
        if metrics.total_files > 10 && metrics.skip_ratio() < self.min_skip_ratio {
            suggestions.push(format!(
                "Low skip ratio: {:.1}% (threshold: {:.1}%). Add WHERE clauses on indexed columns.",
                metrics.skip_ratio() * 100.0,
                self.min_skip_ratio * 100.0
            ));
        }

        // Check execution time
        if metrics.execution_time_ms > self.max_execution_time_ms {
            suggestions.push(format!(
                "Query took {}ms (threshold: {}ms). Consider using LIMIT or optimizing predicates.",
                metrics.execution_time_ms, self.max_execution_time_ms
            ));
        }
        
        // Check pushdown ratio
        if metrics.pushdown_total_predicates > 2 && metrics.pushdown_ratio() < self.min_pushdown_ratio {
            suggestions.push(format!(
                "Low pushdown ratio: {:.1}% (threshold: {:.1}%). {} of {} predicates evaluated locally. \
                 Consider using filters supported by the data source.",
                metrics.pushdown_ratio() * 100.0,
                self.min_pushdown_ratio * 100.0,
                metrics.pushdown_local_predicates,
                metrics.pushdown_total_predicates
            ));
        }
        
        // Warn about pushdown warnings
        if metrics.pushdown_warnings_count > 0 {
            suggestions.push(format!(
                "{} pushdown warning(s) generated. Check query plan for details on unsupported filters.",
                metrics.pushdown_warnings_count
            ));
        }

        if suggestions.is_empty() {
            None
        } else {
            Some(suggestions)
        }
    }
}

/// Compares cost-estimator predictions against actual query execution metrics.
#[derive(Debug, Clone)]
pub struct EstimationAccuracy {
    /// estimated_bytes / actual_bytes
    pub bytes_ratio: f64,
    /// estimated_rows / actual_rows
    pub rows_ratio: f64,
    /// estimated_time / actual_time
    pub time_ratio: f64,
}

impl EstimationAccuracy {
    pub fn compute(estimate: &QueryCostEstimate, actual: &QueryMetrics) -> Self {
        let bytes_ratio = if actual.bytes_scanned == 0 {
            0.0
        } else {
            estimate.estimated_bytes_scanned as f64 / actual.bytes_scanned as f64
        };
        let rows_ratio = if actual.rows_returned == 0 {
            0.0
        } else {
            estimate.estimated_rows as f64 / actual.rows_returned as f64
        };
        let time_ratio = if actual.execution_time_ms == 0 {
            0.0
        } else {
            estimate.estimated_time_ms as f64 / actual.execution_time_ms as f64
        };
        Self { bytes_ratio, rows_ratio, time_ratio }
    }

    pub fn log(&self) {
        tracing::info!(
            bytes_ratio = %format!("{:.2}", self.bytes_ratio),
            rows_ratio = %format!("{:.2}", self.rows_ratio),
            time_ratio = %format!("{:.2}", self.time_ratio),
            "Cost estimation accuracy"
        );
    }
}
