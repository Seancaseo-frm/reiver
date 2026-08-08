use crate::event::get_trace_context;
use crate::transport::{ErrorPayload, ExceptionPayload, Transport, MAX_BATCH_SIZE};
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Client options for configuring Reiver (similar to Sentry's ClientOptions)
#[derive(Debug, Clone)]
pub struct ClientOptions {
    /// Your Reiver API key (used as Bearer token).
    pub api_key: Option<String>,

    /// Base URL of the Reiver website gateway.
    pub api_url: Option<String>,

    /// Service name reported with every event.
    pub service_name: Option<String>,

    /// Environment name (e.g., "production", "staging")
    pub environment: Option<String>,

    /// Application version (e.g., "1.2.3")
    pub version: Option<String>,

    /// Deployment identifier
    pub deployment_id: Option<String>,

    /// Source repository URL
    pub repository_url: Option<String>,

    /// Default tags to include with all events
    pub tags: Option<HashMap<String, String>>,

    /// Maximum number of events to buffer before dropping
    pub max_queue_size: usize,

    /// Maximum number of events to batch together before sending.
    /// Capped at 100 (Watch server limit).
    /// Default: 10
    pub batch_size: usize,

    /// Maximum time to wait before sending a batch, even if batch_size isn't reached.
    /// Default: 5 seconds
    pub batch_timeout: Duration,

    // --- Infrastructure fields (auto-detected if not set) ---
    /// Region identifier
    pub region: Option<String>,

    /// Hostname (auto-detected from system hostname if not set)
    pub host_name: Option<String>,

    /// Kubernetes pod name (auto-detected from `POD_NAME` or `HOSTNAME` env var)
    pub pod_name: Option<String>,

    /// Kubernetes cluster name (auto-detected from `CLUSTER_NAME` env var)
    pub cluster_name: Option<String>,

    /// Container ID (auto-detected from `CONTAINER_ID` env var)
    pub container_id: Option<String>,

    // --- Profiling (requires the "profiling" feature) ---
    /// Project UUID for internal (non-gateway) profile ingestion.
    /// When set, the profiler sends `X-Project-Id` directly instead of
    /// going through the website gateway.
    pub project_id: Option<String>,
    /// Enable continuous CPU profiling.  Default: `false`.
    pub profiling_enabled: bool,
    /// CPU sampling frequency in Hz (default: 99 -- avoids lock-step with timers).
    pub profiling_frequency: i32,
    /// How often to export a CPU profile snapshot, in seconds (default: 600 = 10 min).
    pub profiling_cpu_interval_secs: u64,
    /// How often to export a heap profile snapshot, in seconds (default: 600 = 10 min).
    /// Only effective on Linux where jemalloc heap profiling is available.
    pub profiling_heap_interval_secs: u64,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            api_key: None,
            api_url: None,
            service_name: None,
            environment: None,
            version: None,
            deployment_id: None,
            repository_url: None,
            tags: None,
            max_queue_size: 100,
            batch_size: 10,
            batch_timeout: Duration::from_secs(5),
            region: None,
            host_name: None,
            pod_name: None,
            cluster_name: None,
            container_id: None,
            project_id: None,
            profiling_enabled: false,
            profiling_frequency: 99,
            profiling_cpu_interval_secs: 600,
            profiling_heap_interval_secs: 600,
        }
    }
}

impl ClientOptions {
    /// Resolve infrastructure fields from environment variables where not explicitly set.
    fn resolve_infra_defaults(&mut self) {
        if self.host_name.is_none() {
            self.host_name = hostname::get().ok().and_then(|h| h.into_string().ok());
        }
        if self.pod_name.is_none() {
            self.pod_name = std::env::var("POD_NAME")
                .ok()
                .or_else(|| std::env::var("HOSTNAME").ok());
        }
        if self.cluster_name.is_none() {
            self.cluster_name = std::env::var("CLUSTER_NAME").ok();
        }
        if self.container_id.is_none() {
            self.container_id = std::env::var("CONTAINER_ID").ok();
        }
        if self.region.is_none() {
            self.region = std::env::var("REGION")
                .ok()
                .or_else(|| std::env::var("AWS_REGION").ok());
        }
    }
}

/// Reiver client (similar to Sentry's Client)
pub struct Client {
    options: ClientOptions,
    transport: Arc<Transport>,
    sender: mpsc::UnboundedSender<ErrorPayload>,
    _handle: tokio::task::JoinHandle<()>,
    pending_count: Arc<AtomicU64>,
    sent_count: Arc<AtomicU64>,
    failed_count: Arc<AtomicU64>,
}

impl Client {
    /// Create a new Reiver client
    pub fn new(mut options: ClientOptions) -> Self {
        options.resolve_infra_defaults();

        let api_key = options.api_key.clone().expect("api_key must be set");

        let api_url = options
            .api_url
            .clone()
            .unwrap_or_else(|| "https://app.reiver.io".to_string());

        let transport = Arc::new(Transport::new(api_url, api_key));
        let (sender, mut receiver) = mpsc::unbounded_channel();

        let transport_clone = transport.clone();
        let pending_count = Arc::new(AtomicU64::new(0));
        let sent_count = Arc::new(AtomicU64::new(0));
        let failed_count = Arc::new(AtomicU64::new(0));

        let pending_clone = pending_count.clone();
        let sent_clone = sent_count.clone();
        let failed_clone = failed_count.clone();
        // Enforce the server-side maximum.
        let batch_size = options.batch_size.min(MAX_BATCH_SIZE);
        let batch_timeout = options.batch_timeout;

        // Background task to send events in batches
        let handle = tokio::spawn(async move {
            let mut batch = Vec::new();
            let mut batch_start_time = std::time::Instant::now();

            loop {
                // Check if timeout has already passed
                let elapsed = batch_start_time.elapsed();
                if elapsed >= batch_timeout {
                    // Timeout already passed, send immediately if batch not empty
                    if !batch.is_empty() {
                        let batch_len = batch.len();
                        let batch_to_send: Vec<ErrorPayload> = batch.drain(..).collect();
                        if let Err(e) = transport_clone.send_batch(batch_to_send).await {
                            tracing::warn!("Reiver: failed to send batch (timeout): {}", e);
                            failed_clone.fetch_add(batch_len as u64, Ordering::Relaxed);
                        } else {
                            sent_clone.fetch_add(batch_len as u64, Ordering::Relaxed);
                        }
                        batch_start_time = std::time::Instant::now();
                    }
                    continue;
                }

                // Calculate how long until we should timeout
                let timeout_remaining = batch_timeout - elapsed;
                let timeout_fut = tokio::time::sleep(timeout_remaining);
                tokio::pin!(timeout_fut);

                tokio::select! {
                    // Receive a new payload
                    payload_opt = receiver.recv() => {
                        match payload_opt {
                            Some(payload) => {
                                pending_clone.fetch_sub(1, Ordering::Relaxed);
                                batch.push(payload);

                                // Send batch if we've reached the batch size
                                if batch.len() >= batch_size {
                                    let batch_len = batch.len();
                                    let batch_to_send: Vec<ErrorPayload> = batch.drain(..).collect();
                                    if let Err(e) = transport_clone.send_batch(batch_to_send).await {
                                        tracing::warn!("Reiver: failed to send batch: {}", e);
                                        failed_clone.fetch_add(batch_len as u64, Ordering::Relaxed);
                                    } else {
                                        sent_clone.fetch_add(batch_len as u64, Ordering::Relaxed);
                                    }
                                    batch_start_time = std::time::Instant::now();
                                }
                            }
                            None => {
                                // Channel closed, flush remaining batch
                                if !batch.is_empty() {
                                    let batch_len = batch.len();
                                    let batch_to_send = batch;
                                    if let Err(e) = transport_clone.send_batch(batch_to_send).await {
                                        tracing::warn!("Reiver: failed to send final batch: {}", e);
                                        failed_clone.fetch_add(batch_len as u64, Ordering::Relaxed);
                                    } else {
                                        sent_clone.fetch_add(batch_len as u64, Ordering::Relaxed);
                                    }
                                }
                                break;
                            }
                        }
                    }
                    // Timeout reached, send batch if not empty
                    _ = timeout_fut => {
                        if !batch.is_empty() {
                            let batch_len = batch.len();
                            let batch_to_send: Vec<ErrorPayload> = batch.drain(..).collect();
                            if let Err(e) = transport_clone.send_batch(batch_to_send).await {
                                tracing::warn!("Reiver: failed to send batch (timeout): {}", e);
                                failed_clone.fetch_add(batch_len as u64, Ordering::Relaxed);
                            } else {
                                sent_clone.fetch_add(batch_len as u64, Ordering::Relaxed);
                            }
                            batch_start_time = std::time::Instant::now();
                        } else {
                            // Reset timeout if batch is empty
                            batch_start_time = std::time::Instant::now();
                        }
                    }
                }
            }
        });

        Self {
            options,
            transport,
            sender,
            _handle: handle,
            pending_count,
            sent_count,
            failed_count,
        }
    }

    /// Flush all pending events and wait for them to be sent
    /// Returns the number of pending events remaining (should be 0 if successful)
    pub async fn flush(&self, timeout_secs: u64) -> u64 {
        use std::time::{Duration, Instant};
        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);

        // Wait until all pending events are sent or timeout
        while start.elapsed() < timeout {
            let pending = self.pending_count.load(Ordering::Relaxed);
            if pending == 0 {
                return 0;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        self.pending_count.load(Ordering::Relaxed)
    }

    /// Get statistics about sent/failed/pending events
    pub fn stats(&self) -> (u64, u64, u64) {
        (
            self.sent_count.load(Ordering::Relaxed),
            self.failed_count.load(Ordering::Relaxed),
            self.pending_count.load(Ordering::Relaxed),
        )
    }

    /// Capture an exception
    pub async fn capture_exception(&self, error: &dyn std::error::Error) {
        // Extract type and value using Sentry's approach
        let dbg = format!("{error:?}");
        let value = error.to_string();
        let ty = if dbg == format!("{value:?}") {
            String::from("Error")
        } else {
            dbg.split(&[' ', '(', '{', '\r', '\n'][..])
                .next()
                .unwrap_or("Error")
                .trim()
                .to_owned()
        };

        // Capture backtrace using the backtrace crate (like Sentry does)
        use crate::event::{extract_stack_frames, stack_frame_to_payload};
        use backtrace::Backtrace;

        let backtrace = Backtrace::new();
        let stacktrace = {
            let frames = extract_stack_frames(&backtrace);
            if !frames.is_empty() {
                Some(
                    frames
                        .into_iter()
                        .map(|f| stack_frame_to_payload(&f))
                        .collect(),
                )
            } else {
                None
            }
        };

        let exception_payload = ExceptionPayload {
            exception_type: ty,
            value: value.clone(),
            stacktrace,
        };

        let payload = self.build_payload(value, "error", Some(exception_payload), None, None, None);

        self.pending_count.fetch_add(1, Ordering::Relaxed);
        let _ = self.sender.send(payload);
    }

    /// Capture a message
    pub async fn capture_message(&self, message: &str, level: &str) {
        let payload = self.build_payload(message.to_string(), level, None, None, None, None);

        self.pending_count.fetch_add(1, Ordering::Relaxed);
        let _ = self.sender.send(payload);
    }

    /// Get the sender channel (for global access)
    pub(crate) fn get_sender(&self) -> mpsc::UnboundedSender<ErrorPayload> {
        self.sender.clone()
    }

    /// Get the pending count (for global access)
    pub(crate) fn get_pending_count(&self) -> Arc<AtomicU64> {
        self.pending_count.clone()
    }

    /// Get client options (for building payloads)
    pub(crate) fn get_options(&self) -> ClientOptions {
        self.options.clone()
    }

    fn build_payload(
        &self,
        message: String,
        level: &str,
        exception: Option<ExceptionPayload>,
        context: Option<serde_json::Value>,
        tags: Option<serde_json::Value>,
        user: Option<serde_json::Value>,
    ) -> ErrorPayload {
        let mut context_value = context.unwrap_or_else(|| json!({}));

        // Add runtime context
        if let Some(obj) = context_value.as_object_mut() {
            obj.insert(
                "runtime".to_string(),
                json!({
                    "name": "rust",
                }),
            );

            if let Some(env) = &self.options.environment {
                obj.insert("environment".to_string(), json!(env));
            }
        }

        // Merge tags
        let mut tags_value = self
            .options
            .tags
            .clone()
            .map(|t| {
                let mut map = json!({});
                for (k, v) in t {
                    map[k] = json!(v);
                }
                map
            })
            .unwrap_or_else(|| json!({}));

        if let Some(custom_tags) = tags {
            if let (Some(obj1), Some(obj2)) = (tags_value.as_object_mut(), custom_tags.as_object())
            {
                for (k, v) in obj2 {
                    obj1.insert(k.clone(), v.clone());
                }
            }
        }

        // Extract trace context from OpenTelemetry (if available)
        let (trace_id, span_id) = get_trace_context();

        ErrorPayload {
            api_key: self.options.api_key.clone().expect("api_key must be set"),
            timestamp: Some(chrono::Utc::now()),
            level: level.to_string(),
            message,
            exception,
            context: Some(context_value),
            tags: Some(tags_value),
            user,
            trace_id,
            span_id,
            service_name: self.options.service_name.clone(),
            environment: self.options.environment.clone(),
            version: self.options.version.clone(),
            deployment_id: self.options.deployment_id.clone(),
            repository_url: self.options.repository_url.clone(),
            region: self.options.region.clone(),
            host_name: self.options.host_name.clone(),
            runtime: Some("rust".to_string()),
            pod_name: self.options.pod_name.clone(),
            cluster_name: self.options.cluster_name.clone(),
            container_id: self.options.container_id.clone(),
            http_method: None,
            http_url: None,
            user_id: None,
        }
    }
}

/// Guard that keeps the client alive (similar to Sentry's Guard).
///
/// When the `profiling` feature is enabled and `profiling_enabled` is `true`,
/// the guard also holds the background profiler handle.
pub struct Guard {
    client: Arc<Client>,
    #[cfg(feature = "profiling")]
    _profiler: Option<crate::profiling::ContinuousProfiler>,
}

impl Guard {
    pub(crate) fn new(client: Arc<Client>) -> Self {
        Self {
            client,
            #[cfg(feature = "profiling")]
            _profiler: None,
        }
    }

    /// Attach a running profiler to this guard so its lifetime is tied to
    /// the SDK's lifecycle.
    #[cfg(feature = "profiling")]
    pub(crate) fn with_profiler(
        mut self,
        profiler: Option<crate::profiling::ContinuousProfiler>,
    ) -> Self {
        self._profiler = profiler;
        self
    }

    /// Flush all pending events
    pub async fn flush(&self, timeout_secs: u64) -> u64 {
        self.client.flush(timeout_secs).await
    }

    /// Get statistics about sent/failed/pending events
    pub fn stats(&self) -> (u64, u64, u64) {
        self.client.stats()
    }
}
