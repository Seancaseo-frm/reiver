//! Reiver Rust SDK - Observability for AI Applications
//!
//! Drop-in replacement for Sentry - just change your imports!
//!
//! # Quick Start
//!
//! ```no_run
//! // Instead of: use sentry;
//! use reiver;
//!
//! // Works exactly like Sentry
//! let _guard = reiver::init("your-project-key");
//!
//! // Capture an exception
//! if let Err(e) = some_operation() {
//!     reiver::capture_exception(&e);
//! }
//!
//! // Capture a message
//! reiver::capture_message("Something went wrong", "error");
//! ```

mod client;
mod error;
mod event;
mod transport;

#[cfg(feature = "profiling")]
pub mod profiling;

#[cfg(feature = "profiling")]
pub use profiling::ContinuousProfiler;

#[cfg(feature = "rayon")]
pub mod thread_pool;

#[cfg(feature = "rayon")]
pub use thread_pool::{InstrumentedThreadPool, InstrumentedThreadPoolBuilder};

#[cfg(feature = "memory")]
pub mod memory;

#[cfg(feature = "memory")]
pub use memory::observe_memory;

pub use client::{Client, ClientOptions, Guard};
pub use error::ReiverError;
pub use event::{Exception, StackFrame};

use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::mpsc;
use transport::ErrorPayload;

struct GlobalState {
    sender: mpsc::UnboundedSender<ErrorPayload>,
    options: client::ClientOptions,
    pending_count: Arc<AtomicU64>,
}

static GLOBAL_STATE: std::sync::Mutex<Option<GlobalState>> = std::sync::Mutex::new(None);

/// Initialize the global Reiver client (drop-in replacement for Sentry's `init`)
///
/// # Examples
///
/// ```no_run
/// // Simple initialization with project key (uses default batching: batch_size=10, batch_timeout_secs=5)
/// let _guard = reiver::init("your-project-key");
///
/// // With options (like Sentry's tuple syntax)
/// let _guard = reiver::init((
///     "your-project-key",
///     reiver::ClientOptions {
///         environment: Some("production".to_string()),
///         ..Default::default()
///     }
/// ));
///
/// // Customize batching behavior for high-throughput scenarios
/// use std::time::Duration;
/// let _guard = reiver::init((
///     "your-project-key",
///     reiver::ClientOptions {
///         batch_size: 50,              // Send when 50 errors are batched
///         batch_timeout: Duration::from_secs(2),  // Or send every 2 seconds, whichever comes first
///         environment: Some("production".to_string()),
///         ..Default::default()
///     }
/// ));
/// ```
///
/// Returns a `Guard` that must be kept alive for the client to work.
/// When the guard is dropped, the client will flush pending events and shutdown.
pub fn init<D>(dsn: D) -> Guard
where
    D: IntoInitParams,
{
    dsn.into_guard()
}

/// Helper trait for init parameters (supports both simple string and tuple like Sentry)
pub(crate) trait IntoInitParams {
    fn into_guard(self) -> Guard;
}

impl IntoInitParams for &str {
    fn into_guard(self) -> Guard {
        init_with_options(client::ClientOptions {
            api_key: Some(self.to_string()),
            ..Default::default()
        })
    }
}

impl IntoInitParams for String {
    fn into_guard(self) -> Guard {
        init_with_options(client::ClientOptions {
            api_key: Some(self),
            ..Default::default()
        })
    }
}

impl IntoInitParams for (&str, client::ClientOptions) {
    fn into_guard(self) -> Guard {
        let mut opts = self.1;
        opts.api_key = Some(self.0.to_string());
        init_with_options(opts)
    }
}

impl IntoInitParams for (String, client::ClientOptions) {
    fn into_guard(self) -> Guard {
        let mut opts = self.1;
        opts.api_key = Some(self.0);
        init_with_options(opts)
    }
}

impl IntoInitParams for client::ClientOptions {
    fn into_guard(self) -> Guard {
        init_with_options(self)
    }
}

/// Initialize the global Reiver client with options
fn init_with_options(options: client::ClientOptions) -> Guard {
    // Ensure api_key is set
    if options.api_key.is_none() {
        panic!("Reiver: api_key must be set in ClientOptions");
    }

    let client = Arc::new(Client::new(options.clone()));

    // Get the sender and pending counter from the client and store them globally
    let sender = client.get_sender();
    let pending_count = client.get_pending_count();
    {
        let mut global = GLOBAL_STATE.lock().unwrap();
        *global = Some(GlobalState {
            sender,
            options: options.clone(),
            pending_count,
        });
    }

    let guard = Guard::new(client);

    // Auto-start profiling when the feature is compiled in and enabled.
    #[cfg(feature = "profiling")]
    let guard = guard.with_profiler(profiling::start(&options));

    guard
}

/// Get the global sender, options, and pending counter
pub(crate) fn get_global_state() -> Option<(
    mpsc::UnboundedSender<ErrorPayload>,
    client::ClientOptions,
    Arc<AtomicU64>,
)> {
    let global = GLOBAL_STATE.lock().unwrap();
    global.as_ref().map(|state| {
        (
            state.sender.clone(),
            state.options.clone(),
            state.pending_count.clone(),
        )
    })
}

/// Capture an exception using the global client (drop-in replacement for Sentry)
///
/// # Example
///
/// ```no_run
/// if let Err(e) = some_operation() {
///     reiver::capture_exception(&e);
/// }
/// ```
pub fn capture_exception(error: &dyn std::error::Error) {
    event::capture_exception(error);
}

/// Capture a message using the global client (drop-in replacement for Sentry)
///
/// # Example
///
/// ```no_run
/// reiver::capture_message("Something went wrong", "error");
/// ```
pub fn capture_message(msg: &str, level: &str) {
    event::capture_message(msg, level);
}
