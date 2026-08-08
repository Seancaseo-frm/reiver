//! Instrumented Rayon Thread Pool Wrapper
//!
//! Provides [`InstrumentedThreadPool`] — a thin wrapper around [`rayon::ThreadPool`]
//! that tracks task lifecycle via atomic counters and exports them as OpenTelemetry
//! observable instruments.
//!
//! ## Emitted Metrics
//!
//! | Metric                       | OTel Type          | Description                     |
//! |------------------------------|--------------------|---------------------------------|
//! | `rayon.pool.threads`         | Observable Gauge   | Static thread count             |
//! | `rayon.pool.tasks_queued`    | Observable Gauge   | submitted − started             |
//! | `rayon.pool.tasks_active`    | Observable Gauge   | started − completed             |
//! | `rayon.pool.tasks_completed` | Observable Counter | Total tasks finished            |
//! | `rayon.pool.tasks_panicked`  | Observable Counter | Total panicked tasks            |
//!
//! All metrics carry a `pool.name` attribute (the name passed to the builder).
//!
//! ## Example
//!
//! ```no_run
//! use reiver_sdk::InstrumentedThreadPoolBuilder;
//!
//! let pool = InstrumentedThreadPoolBuilder::new("pii-scanner")
//!     .num_threads(4)
//!     .build()
//!     .expect("failed to build pool");
//!
//! pool.spawn(|| {
//!     // CPU-bound work here
//! });
//! ```

use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use opentelemetry::KeyValue;

/// Internal metrics storage for the thread pool.
///
/// All counters use `Relaxed` ordering — they are monotonic counters read by
/// observable instrument callbacks, so strict ordering is unnecessary.
struct PoolMetrics {
    pool_name: String,
    num_threads: u64,
    tasks_submitted: AtomicU64,
    tasks_started: AtomicU64,
    tasks_completed: AtomicU64,
    tasks_panicked: AtomicU64,
}

impl PoolMetrics {
    fn new(pool_name: String, num_threads: u64) -> Self {
        Self {
            pool_name,
            num_threads,
            tasks_submitted: AtomicU64::new(0),
            tasks_started: AtomicU64::new(0),
            tasks_completed: AtomicU64::new(0),
            tasks_panicked: AtomicU64::new(0),
        }
    }

    /// submitted − started (clamped to 0)
    fn tasks_queued(&self) -> u64 {
        let submitted = self.tasks_submitted.load(Ordering::Relaxed);
        let started = self.tasks_started.load(Ordering::Relaxed);
        submitted.saturating_sub(started)
    }

    /// started − completed (clamped to 0)
    fn tasks_active(&self) -> u64 {
        let started = self.tasks_started.load(Ordering::Relaxed);
        let completed = self.tasks_completed.load(Ordering::Relaxed);
        started.saturating_sub(completed)
    }
}

/// An instrumented wrapper around [`rayon::ThreadPool`].
///
/// Every [`spawn`](Self::spawn) and [`spawn_fifo`](Self::spawn_fifo) call
/// tracks task submission, start, completion, and panics via atomic counters
/// that are exported as OTel observable instruments.
pub struct InstrumentedThreadPool {
    pool: rayon::ThreadPool,
    metrics: Arc<PoolMetrics>,
}

impl InstrumentedThreadPool {
    /// Spawn a closure on the thread pool.
    ///
    /// The closure is wrapped to track submitted → started → completed lifecycle.
    /// Panics are caught and recorded. Since free-standing `spawn` has no parent
    /// scope to propagate to (rayon would abort the process), the panic is
    /// absorbed after being counted in `tasks_panicked`.
    pub fn spawn<F: FnOnce() + Send + 'static>(&self, func: F) {
        self.metrics.tasks_submitted.fetch_add(1, Ordering::Relaxed);
        let metrics = Arc::clone(&self.metrics);
        self.pool.spawn(move || {
            metrics.tasks_started.fetch_add(1, Ordering::Relaxed);
            let result = std::panic::catch_unwind(AssertUnwindSafe(func));
            metrics.tasks_completed.fetch_add(1, Ordering::Relaxed);
            if result.is_err() {
                metrics.tasks_panicked.fetch_add(1, Ordering::Relaxed);
                tracing::error!("Task panicked in instrumented rayon pool");
            }
        });
    }

    /// Spawn a closure with FIFO ordering on the thread pool.
    ///
    /// Same instrumentation as [`spawn`](Self::spawn).
    pub fn spawn_fifo<F: FnOnce() + Send + 'static>(&self, func: F) {
        self.metrics.tasks_submitted.fetch_add(1, Ordering::Relaxed);
        let metrics = Arc::clone(&self.metrics);
        self.pool.spawn_fifo(move || {
            metrics.tasks_started.fetch_add(1, Ordering::Relaxed);
            let result = std::panic::catch_unwind(AssertUnwindSafe(func));
            metrics.tasks_completed.fetch_add(1, Ordering::Relaxed);
            if result.is_err() {
                metrics.tasks_panicked.fetch_add(1, Ordering::Relaxed);
                tracing::error!("Task panicked in instrumented rayon pool");
            }
        });
    }

    /// Run a closure inside this pool's scope (blocking).
    ///
    /// Delegates directly to [`rayon::ThreadPool::install`].
    pub fn install<F, R>(&self, func: F) -> R
    where
        F: FnOnce() -> R + Send,
        R: Send,
    {
        self.pool.install(func)
    }

    /// Create a scope on this pool.
    ///
    /// Delegates directly to [`rayon::ThreadPool::scope`].
    pub fn scope<'scope, F, R>(&self, func: F) -> R
    where
        F: FnOnce(&rayon::Scope<'scope>) -> R + Send,
        R: Send,
    {
        self.pool.scope(func)
    }

    /// Create a FIFO scope on this pool.
    ///
    /// Delegates directly to [`rayon::ThreadPool::scope_fifo`].
    pub fn scope_fifo<'scope, F, R>(&self, func: F) -> R
    where
        F: FnOnce(&rayon::ScopeFifo<'scope>) -> R + Send,
        R: Send,
    {
        self.pool.scope_fifo(func)
    }

    /// Return the number of threads in the pool.
    pub fn current_num_threads(&self) -> usize {
        self.pool.current_num_threads()
    }
}

/// Builder for [`InstrumentedThreadPool`].
///
/// Wraps [`rayon::ThreadPoolBuilder`] and registers OTel observable instruments
/// when [`build`](Self::build) is called.
pub struct InstrumentedThreadPoolBuilder {
    name: String,
    inner: rayon::ThreadPoolBuilder,
}

impl InstrumentedThreadPoolBuilder {
    /// Create a new builder with the given pool name.
    ///
    /// The name is used as the `pool.name` attribute on all emitted OTel metrics.
    /// Use a low-cardinality, descriptive name (e.g. `"pii-scanner"`).
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            inner: rayon::ThreadPoolBuilder::new(),
        }
    }

    /// Set the number of threads in the pool.
    pub fn num_threads(mut self, n: usize) -> Self {
        self.inner = self.inner.num_threads(n);
        self
    }

    /// Set a closure that names threads.
    pub fn thread_name<F>(mut self, f: F) -> Self
    where
        F: FnMut(usize) -> String + 'static,
    {
        self.inner = self.inner.thread_name(f);
        self
    }

    /// Set the stack size for spawned threads.
    pub fn stack_size(mut self, s: usize) -> Self {
        self.inner = self.inner.stack_size(s);
        self
    }

    /// Build the instrumented thread pool.
    ///
    /// 1. Builds the inner `rayon::ThreadPool`
    /// 2. Creates `PoolMetrics` with atomic counters
    /// 3. Registers OTel observable instruments on the global meter provider
    pub fn build(self) -> Result<InstrumentedThreadPool, rayon::ThreadPoolBuildError> {
        let pool = self.inner.build()?;
        let num_threads = pool.current_num_threads() as u64;
        let metrics = Arc::new(PoolMetrics::new(self.name, num_threads));

        register_otel_metrics(&metrics);

        Ok(InstrumentedThreadPool { pool, metrics })
    }
}

/// Register OTel observable instruments for the given pool metrics.
fn register_otel_metrics(metrics: &Arc<PoolMetrics>) {
    let meter = opentelemetry::global::meter_provider().meter("reiver-sdk");

    // rayon.pool.threads — static gauge
    {
        let m = Arc::clone(metrics);
        let _ = meter
            .u64_observable_gauge("rayon.pool.threads")
            .with_description("Number of threads in the rayon pool")
            .with_callback(move |obs| {
                obs.observe(
                    m.num_threads,
                    &[KeyValue::new("pool.name", m.pool_name.clone())],
                );
            })
            .build();
    }

    // rayon.pool.tasks_queued — derived gauge (submitted - started)
    {
        let m = Arc::clone(metrics);
        let _ = meter
            .u64_observable_gauge("rayon.pool.tasks_queued")
            .with_description("Tasks waiting in the rayon pool queue (submitted - started)")
            .with_callback(move |obs| {
                obs.observe(
                    m.tasks_queued(),
                    &[KeyValue::new("pool.name", m.pool_name.clone())],
                );
            })
            .build();
    }

    // rayon.pool.tasks_active — derived gauge (started - completed)
    {
        let m = Arc::clone(metrics);
        let _ = meter
            .u64_observable_gauge("rayon.pool.tasks_active")
            .with_description("Tasks currently executing in the rayon pool (started - completed)")
            .with_callback(move |obs| {
                obs.observe(
                    m.tasks_active(),
                    &[KeyValue::new("pool.name", m.pool_name.clone())],
                );
            })
            .build();
    }

    // rayon.pool.tasks_completed — monotonic counter
    {
        let m = Arc::clone(metrics);
        let _ = meter
            .u64_observable_counter("rayon.pool.tasks_completed")
            .with_description("Total tasks completed in the rayon pool")
            .with_callback(move |obs| {
                obs.observe(
                    m.tasks_completed.load(Ordering::Relaxed),
                    &[KeyValue::new("pool.name", m.pool_name.clone())],
                );
            })
            .build();
    }

    // rayon.pool.tasks_panicked — monotonic counter
    {
        let m = Arc::clone(metrics);
        let _ = meter
            .u64_observable_counter("rayon.pool.tasks_panicked")
            .with_description("Total tasks that panicked in the rayon pool")
            .with_callback(move |obs| {
                obs.observe(
                    m.tasks_panicked.load(Ordering::Relaxed),
                    &[KeyValue::new("pool.name", m.pool_name.clone())],
                );
            })
            .build();
    }

    tracing::info!(
        pool_name = metrics.pool_name,
        num_threads = metrics.num_threads,
        "Registered 5 OTel observable metrics for rayon pool"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Barrier;

    fn build_test_pool(name: &str, threads: usize) -> InstrumentedThreadPool {
        InstrumentedThreadPoolBuilder::new(name)
            .num_threads(threads)
            .build()
            .expect("failed to build test pool")
    }

    #[test]
    fn test_spawn_increments_counters() {
        let pool = build_test_pool("test-counters", 4);
        let n: usize = 10;
        let counter = Arc::new(AtomicUsize::new(0));

        for _ in 0..n {
            let c = Arc::clone(&counter);
            pool.spawn(move || {
                c.fetch_add(1, Ordering::Relaxed);
            });
        }

        // Spin-wait until all tasks complete
        while counter.load(Ordering::Relaxed) < n {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        // Small extra wait for counter updates after the last task
        std::thread::sleep(std::time::Duration::from_millis(50));

        assert_eq!(
            pool.metrics.tasks_submitted.load(Ordering::Relaxed),
            n as u64
        );
        assert_eq!(pool.metrics.tasks_started.load(Ordering::Relaxed), n as u64);
        assert_eq!(
            pool.metrics.tasks_completed.load(Ordering::Relaxed),
            n as u64
        );
        assert_eq!(pool.metrics.tasks_panicked.load(Ordering::Relaxed), 0);
        assert_eq!(pool.metrics.tasks_queued(), 0);
        assert_eq!(pool.metrics.tasks_active(), 0);
    }

    #[test]
    fn test_spawn_fifo_increments_counters() {
        let pool = build_test_pool("test-fifo", 4);
        let n: usize = 5;
        let counter = Arc::new(AtomicUsize::new(0));

        for _ in 0..n {
            let c = Arc::clone(&counter);
            pool.spawn_fifo(move || {
                c.fetch_add(1, Ordering::Relaxed);
            });
        }

        while counter.load(Ordering::Relaxed) < n {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        std::thread::sleep(std::time::Duration::from_millis(50));

        assert_eq!(
            pool.metrics.tasks_submitted.load(Ordering::Relaxed),
            n as u64
        );
        assert_eq!(
            pool.metrics.tasks_completed.load(Ordering::Relaxed),
            n as u64
        );
    }

    #[test]
    fn test_panic_tracking() {
        let pool = build_test_pool("test-panic", 2);

        let started = Arc::new(Barrier::new(2));
        let s = Arc::clone(&started);

        // Spawn a panicking task
        pool.spawn(move || {
            s.wait(); // Signal that we started
            panic!("intentional test panic");
        });

        // Wait for the task to start
        started.wait();

        // Give time for the panic to be caught and counters to update
        std::thread::sleep(std::time::Duration::from_millis(200));

        assert_eq!(pool.metrics.tasks_submitted.load(Ordering::Relaxed), 1);
        assert_eq!(pool.metrics.tasks_started.load(Ordering::Relaxed), 1);
        assert_eq!(pool.metrics.tasks_completed.load(Ordering::Relaxed), 1);
        assert_eq!(pool.metrics.tasks_panicked.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_queued_and_active_consistency() {
        let pool = build_test_pool("test-consistency", 1);

        // With 1 thread and a blocking task, subsequent spawns will queue
        let blocker = Arc::new(Barrier::new(2));
        let b = Arc::clone(&blocker);

        pool.spawn(move || {
            b.wait(); // Block until we release
        });

        // Give time for the task to start
        std::thread::sleep(std::time::Duration::from_millis(50));

        // At this point: 1 submitted, 1 started, 0 completed → active = 1
        assert_eq!(pool.metrics.tasks_active(), 1);

        // Spawn another — it should queue since the single thread is blocked
        pool.spawn(|| {});
        std::thread::sleep(std::time::Duration::from_millis(10));

        // 2 submitted, 1 started → queued >= 1
        assert!(pool.metrics.tasks_queued() >= 1);

        // Release the blocker
        blocker.wait();
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Everything should drain
        assert_eq!(pool.metrics.tasks_queued(), 0);
        assert_eq!(pool.metrics.tasks_active(), 0);
        assert_eq!(pool.metrics.tasks_completed.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_install_delegates() {
        let pool = build_test_pool("test-install", 2);
        let result = pool.install(|| 42);
        assert_eq!(result, 42);
    }

    #[test]
    fn test_current_num_threads() {
        let pool = build_test_pool("test-threads", 4);
        assert_eq!(pool.current_num_threads(), 4);
    }
}
