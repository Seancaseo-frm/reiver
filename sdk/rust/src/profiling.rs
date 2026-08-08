//! Continuous profiling module (CPU + heap).
//!
//! When enabled via the `profiling` feature flag and `ClientOptions::profiling_enabled`,
//! this module spawns a background task that periodically collects CPU profiles
//! using `pprof-rs` and (on Linux) heap profiles using `jemalloc_pprof`, then
//! exports them in OTLP format through the Reiver gateway.
//!
//! # Usage
//!
//! Profiling shares the same `ClientOptions` as the rest of the SDK -- no
//! separate configuration is needed:
//!
//! ```no_run
//! let _guard = reiver::init((
//!     "dhp_abc123",
//!     reiver::ClientOptions {
//!         service_name: Some("my-service".to_string()),
//!         version: Some("1.2.3".to_string()),
//!         profiling_enabled: true,
//!         ..Default::default()
//!     },
//! ));
//! ```
//!
//! Profiling is completely opt-in: if `profiling_enabled` is `false` (the
//! default), [`start`] returns `None` and no background work is spawned.
//! All profiling errors are logged at `warn` level and never propagate to
//! the caller -- profiling must never crash the main process.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use opentelemetry_proto::tonic::{
    collector::profiles::v1development::ExportProfilesServiceRequest,
    common::v1::{any_value, AnyValue, KeyValue},
    profiles::v1development::{
        Function as OtlpFunction, Line as OtlpLine, Location as OtlpLocation,
        Mapping as OtlpMapping, Profile as OtlpProfile, ProfilesDictionary, ResourceProfiles,
        Sample as OtlpSample, ScopeProfiles, Stack, ValueType as OtlpValueType,
    },
    resource::v1::Resource,
};
use prost::Message;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::warn;

use crate::client::ClientOptions;

// ---------------------------------------------------------------------------
// Internal configuration (built from ClientOptions)
// ---------------------------------------------------------------------------

/// Internal profiler configuration -- not user-facing.
/// Built by [`start`] from [`ClientOptions`].
#[derive(Debug, Clone)]
struct ProfilerConfig {
    frequency: i32,
    cpu_interval_secs: u64,
    heap_interval_secs: u64,
    url: String,
    auth_header: String,
    /// When set, send `X-Project-Id` directly (internal / non-gateway mode).
    project_id: Option<String>,
    /// `service.name` resource attribute.
    service_name: String,
    /// `service.version` resource attribute.
    service_version: Option<String>,
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Start continuous profiling based on [`ClientOptions`].
///
/// Returns `Some(ContinuousProfiler)` (a shutdown handle) when profiling is
/// enabled, or `None` when it is disabled or the configuration is incomplete
/// (missing `api_key`).
pub fn start(options: &ClientOptions) -> Option<ContinuousProfiler> {
    if !options.profiling_enabled {
        tracing::info!("Continuous profiling is disabled");
        return None;
    }

    let api_url = options
        .api_url
        .as_deref()
        .unwrap_or("https://app.reiver.io")
        .trim_end_matches('/');

    // Internal mode: project_id is set, send X-Project-Id directly to Watch.
    // External mode: api_key is set, send Authorization: Bearer through gateway.
    let (url, auth_header, project_id) = if let Some(pid) = &options.project_id {
        (
            format!("{api_url}/api/v1/profiles"),
            String::new(),
            Some(pid.clone()),
        )
    } else {
        let api_key = match &options.api_key {
            Some(k) if !k.is_empty() => k.clone(),
            _ => {
                warn!("Profiling enabled but neither project_id nor api_key is set; skipping");
                return None;
            }
        };
        (
            format!("{api_url}/api/watch/ingest/v1/profiles"),
            format!("Bearer {api_key}"),
            None,
        )
    };

    let config = ProfilerConfig {
        frequency: options.profiling_frequency,
        cpu_interval_secs: options.profiling_cpu_interval_secs,
        heap_interval_secs: options.profiling_heap_interval_secs,
        url,
        auth_header,
        project_id,
        service_name: options.service_name.clone().unwrap_or_default(),
        service_version: options.version.clone(),
    };

    let mut profiler = ContinuousProfiler::new(config);
    profiler.run();
    Some(profiler)
}

// ---------------------------------------------------------------------------
// ContinuousProfiler
// ---------------------------------------------------------------------------

/// A background continuous CPU (and optionally heap) profiler that
/// periodically exports OTLP profiles.
///
/// Obtain one via [`start`]. Hold onto it for the lifetime of your
/// application and call [`shutdown`](ContinuousProfiler::shutdown) for
/// a clean exit.
pub struct ContinuousProfiler {
    config: ProfilerConfig,
    shutdown_tx: watch::Sender<bool>,
    handle: Option<JoinHandle<()>>,
}

impl ContinuousProfiler {
    fn new(config: ProfilerConfig) -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        Self {
            config,
            shutdown_tx,
            handle: None,
        }
    }

    /// Spawn the background profiling loop.
    fn run(&mut self) {
        let config = self.config.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let cpu_interval = Duration::from_secs(config.cpu_interval_secs);

        let handle = tokio::spawn(async move {
            tracing::info!(
                service_name = %config.service_name,
                frequency = config.frequency,
                cpu_interval_secs = config.cpu_interval_secs,
                heap_interval_secs = config.heap_interval_secs,
                url = %config.url,
                "Starting continuous profiler (CPU + heap)"
            );

            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default();

            // Track the next time each profile type should fire.
            let start = tokio::time::Instant::now();
            let mut next_cpu = start + cpu_interval;

            #[cfg(target_os = "linux")]
            let heap_interval = Duration::from_secs(config.heap_interval_secs);
            #[cfg(target_os = "linux")]
            let mut next_heap = start + heap_interval;

            // Build the CPU profiler guard once for the process lifetime.
            // report().build() consumes (drains) accumulated samples, so
            // each interval gets a fresh per-interval profile automatically.
            let cpu_guard = match build_cpu_guard(config.frequency) {
                Some(g) => g,
                None => {
                    warn!("Failed to build CPU profiler guard; profiler task exiting");
                    return;
                }
            };

            loop {
                // Compute the next wakeup time (earliest of CPU and heap).
                #[allow(unused_mut)]
                let mut sleep_until = next_cpu;
                #[cfg(target_os = "linux")]
                {
                    sleep_until = sleep_until.min(next_heap);
                }

                // Sleep until the next event, checking for shutdown.
                tokio::select! {
                    _ = tokio::time::sleep_until(sleep_until) => {}
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            tracing::info!("Profiler received shutdown signal");
                            return;
                        }
                    }
                }

                if *shutdown_rx.borrow() {
                    tracing::info!("Profiler shutting down");
                    return;
                }

                let now = tokio::time::Instant::now();

                // -------------------------------------------------------
                // CPU profile export
                // -------------------------------------------------------
                if now >= next_cpu {
                    // build() consumes samples; the guard keeps collecting.
                    let otlp_request = match cpu_guard.report().build() {
                        Ok(report) => convert_and_log(&report),
                        Err(e) => {
                            warn!("Failed to build pprof report: {e}");
                            None
                        }
                    };

                    if let Some(request) = otlp_request {
                        export_otlp(&client, &config, request, "CPU").await;
                    }
                    next_cpu = now + cpu_interval;
                }

                // -------------------------------------------------------
                // Heap profile export (Linux only, requires profiling-heap feature)
                // -------------------------------------------------------
                #[cfg(all(target_os = "linux", feature = "profiling-heap"))]
                if now >= next_heap {
                    collect_and_export_heap_profile(&client, &config).await;
                    next_heap = now + heap_interval;
                }
            }
        });

        self.handle = Some(handle);
    }

    /// Gracefully shut down the profiler.
    ///
    /// Sends the shutdown signal and waits up to `timeout` for the background
    /// task to exit.
    pub async fn shutdown(self, timeout: Duration) {
        let _ = self.shutdown_tx.send(true);

        if let Some(handle) = self.handle {
            match tokio::time::timeout(timeout, handle).await {
                Ok(Ok(())) => tracing::info!("Profiler task exited cleanly"),
                Ok(Err(e)) => warn!("Profiler task panicked: {e}"),
                Err(_) => warn!("Profiler task did not exit within {timeout:?}; abandoning"),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Build a fresh `pprof::ProfilerGuard`, logging on failure.
fn build_cpu_guard(frequency: i32) -> Option<pprof::ProfilerGuard<'static>> {
    match pprof::ProfilerGuardBuilder::default()
        .frequency(frequency)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
    {
        Ok(g) => Some(g),
        Err(e) => {
            warn!("Failed to build pprof guard: {e}");
            None
        }
    }
}

/// Convert a pprof report to an OTLP request, logging on failure.
fn convert_and_log(report: &pprof::Report) -> Option<ExportProfilesServiceRequest> {
    match pprof_to_otlp(report) {
        Ok(req) => Some(req),
        Err(e) => {
            warn!("Failed to convert pprof report to OTLP: {e}");
            None
        }
    }
}

/// Encode and POST an OTLP profile request.
///
/// `profile_kind` is used only for log messages (e.g. "CPU", "heap").
async fn export_otlp(
    client: &reqwest::Client,
    config: &ProfilerConfig,
    mut request: ExportProfilesServiceRequest,
    profile_kind: &str,
) {
    // Inject resource attributes into the request.
    inject_resource(
        &mut request,
        &config.service_name,
        config.service_version.as_deref(),
    );

    let mut buf = Vec::with_capacity(request.encoded_len());
    if let Err(e) = request.encode(&mut buf) {
        warn!("Failed to encode OTLP {profile_kind} profile request: {e}");
        return;
    }

    let mut req = client
        .post(&config.url)
        .header("Content-Type", "application/x-protobuf");

    if let Some(pid) = &config.project_id {
        req = req.header("X-Project-Id", pid);
    } else {
        req = req.header("Authorization", &config.auth_header);
    }

    match req.body(buf).send().await {
        Ok(resp) if resp.status().is_success() => {
            tracing::info!("Exported {profile_kind} profile successfully");
        }
        Ok(resp) => {
            warn!(
                status = %resp.status(),
                "OTLP {profile_kind} profile export returned non-success status"
            );
        }
        Err(e) => {
            warn!("Failed to export OTLP {profile_kind} profile: {e}");
        }
    }
}

/// Inject `service.name` and `service.version` resource attributes
/// into the OTLP request.
fn inject_resource(
    request: &mut ExportProfilesServiceRequest,
    service_name: &str,
    service_version: Option<&str>,
) {
    let mut attributes = vec![KeyValue {
        key: "service.name".to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(service_name.to_string())),
        }),
    }];

    if let Some(version) = service_version {
        attributes.push(KeyValue {
            key: "service.version".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(version.to_string())),
            }),
        });
    }

    let resource = Resource {
        attributes,
        dropped_attributes_count: 0,
        entity_refs: vec![],
    };

    for rp in &mut request.resource_profiles {
        rp.resource = Some(resource.clone());
    }
}

/// Collect a heap profile from jemalloc and export it via OTLP (Linux only, requires profiling-heap feature).
#[cfg(all(target_os = "linux", feature = "profiling-heap"))]
async fn collect_and_export_heap_profile(client: &reqwest::Client, config: &ProfilerConfig) {
    use flate2::read::GzDecoder;
    use std::io::Read;

    let prof_ctl = match jemalloc_pprof::PROF_CTL.as_ref() {
        Some(ctl) => ctl,
        None => {
            warn!("jemalloc profiling not available (PROF_CTL is None)");
            return;
        }
    };

    let mut ctl = prof_ctl.lock().await;
    if !ctl.activated() {
        tracing::debug!("jemalloc profiling is not activated, skipping heap profile");
        return;
    }

    let pprof_gz = match ctl.dump_pprof() {
        Ok(data) => data,
        Err(e) => {
            warn!("Failed to dump heap profile: {e}");
            return;
        }
    };
    // Release the lock before doing decompression / network I/O.
    drop(ctl);

    // Decompress the gzipped pprof protobuf.
    let mut decoder = GzDecoder::new(&pprof_gz[..]);
    let mut decompressed = Vec::new();
    if let Err(e) = decoder.read_to_end(&mut decompressed) {
        warn!("Failed to decompress heap profile: {e}");
        return;
    }

    // Deserialize into the pprof Profile protobuf.
    let pprof_profile: pprof::protos::Profile = match Message::decode(&decompressed[..]) {
        Ok(p) => p,
        Err(e) => {
            warn!("Failed to deserialize heap profile protobuf: {e}");
            return;
        }
    };

    // Convert to OTLP.
    let request = match pprof_profile_to_otlp(&pprof_profile, "alloc") {
        Ok(req) => req,
        Err(e) => {
            warn!("Failed to convert heap profile to OTLP: {e}");
            return;
        }
    };

    export_otlp(client, config, request, "heap").await;
}

// ---------------------------------------------------------------------------
// pprof → OTLP conversion
// ---------------------------------------------------------------------------

/// Convert a `pprof::Report` into an OTLP `ExportProfilesServiceRequest`.
///
/// Thin wrapper that extracts the pprof protobuf from the report and delegates
/// to [`pprof_profile_to_otlp`].
fn pprof_to_otlp(report: &pprof::Report) -> Result<ExportProfilesServiceRequest> {
    let pprof_profile = report.pprof()?;
    pprof_profile_to_otlp(&pprof_profile, "cpu")
}

/// Convert a raw pprof `Profile` protobuf into an OTLP `ExportProfilesServiceRequest`.
///
/// `profile_type` is a label such as `"cpu"` or `"alloc"` used for logging.
///
/// The conversion maps the pprof protobuf profile into the OpenTelemetry
/// Profiles v1development wire format.  Key structural differences:
///
/// - pprof uses 1-based IDs for cross-references; OTLP uses 0-based table
///   indices (with index 0 reserved as the "null" / zero-value entry).
/// - OTLP introduces a `Stack` intermediary between `Sample` and `Location`.
/// - The shared dictionary (string table, function table, etc.) lives at the
///   top-level `ExportProfilesServiceRequest`, not inside `Profile`.
fn pprof_profile_to_otlp(
    pprof_profile: &pprof::protos::Profile,
    _profile_type: &str,
) -> Result<ExportProfilesServiceRequest> {
    // ------------------------------------------------------------------
    // Build ID → index maps (pprof 1-based IDs → OTLP 0-based indices).
    //
    // OTLP reserves index 0 as the "null" / zero-value entry, so we
    // prepend a default zero-value element to every table and shift
    // all real entries by +1.
    // ------------------------------------------------------------------

    let function_id_to_idx: HashMap<u64, i32> = pprof_profile
        .function
        .iter()
        .enumerate()
        .map(|(idx, f)| (f.id, (idx + 1) as i32))
        .collect();

    let location_id_to_idx: HashMap<u64, i32> = pprof_profile
        .location
        .iter()
        .enumerate()
        .map(|(idx, l)| (l.id, (idx + 1) as i32))
        .collect();

    let mapping_id_to_idx: HashMap<u64, i32> = pprof_profile
        .mapping
        .iter()
        .enumerate()
        .map(|(idx, m)| (m.id, (idx + 1) as i32))
        .collect();

    // ------------------------------------------------------------------
    // Functions (prepend zero-value entry)
    // ------------------------------------------------------------------

    let mut function_table: Vec<OtlpFunction> =
        Vec::with_capacity(pprof_profile.function.len() + 1);
    function_table.push(OtlpFunction::default()); // index 0 = null
    for f in &pprof_profile.function {
        function_table.push(OtlpFunction {
            name_strindex: f.name as i32,
            system_name_strindex: f.system_name as i32,
            filename_strindex: f.filename as i32,
            start_line: f.start_line,
        });
    }

    // ------------------------------------------------------------------
    // Locations (prepend zero-value entry)
    // ------------------------------------------------------------------

    let mut location_table: Vec<OtlpLocation> =
        Vec::with_capacity(pprof_profile.location.len() + 1);
    location_table.push(OtlpLocation::default()); // index 0 = null
    for l in &pprof_profile.location {
        let lines: Vec<OtlpLine> = l
            .line
            .iter()
            .map(|line| OtlpLine {
                function_index: *function_id_to_idx.get(&line.function_id).unwrap_or(&0),
                line: line.line,
                column: 0,
            })
            .collect();

        location_table.push(OtlpLocation {
            mapping_index: if l.mapping_id > 0 {
                *mapping_id_to_idx.get(&l.mapping_id).unwrap_or(&0)
            } else {
                0 // 0 = null mapping
            },
            address: l.address,
            line: lines,
            attribute_indices: vec![],
        });
    }

    // ------------------------------------------------------------------
    // Mappings (prepend zero-value entry)
    // ------------------------------------------------------------------

    let mut mapping_table: Vec<OtlpMapping> = Vec::with_capacity(pprof_profile.mapping.len() + 1);
    mapping_table.push(OtlpMapping::default()); // index 0 = null
    for m in &pprof_profile.mapping {
        mapping_table.push(OtlpMapping {
            memory_start: m.memory_start,
            memory_limit: m.memory_limit,
            file_offset: m.file_offset,
            filename_strindex: m.filename as i32,
            attribute_indices: vec![],
        });
    }

    // ------------------------------------------------------------------
    // Stacks & Samples
    // ------------------------------------------------------------------

    let mut stack_table: Vec<Stack> = Vec::with_capacity(pprof_profile.sample.len() + 1);
    stack_table.push(Stack::default()); // index 0 = null

    let mut samples: Vec<OtlpSample> = Vec::with_capacity(pprof_profile.sample.len());

    for s in &pprof_profile.sample {
        let location_indices: Vec<i32> = s
            .location_id
            .iter()
            .filter_map(|id| location_id_to_idx.get(id).copied())
            .collect();

        let stack_idx = stack_table.len() as i32;
        stack_table.push(Stack { location_indices });

        samples.push(OtlpSample {
            stack_index: stack_idx,
            values: s.value.clone(),
            ..Default::default()
        });
    }

    // ------------------------------------------------------------------
    // String table — copy as-is from pprof (index 0 is already "")
    // ------------------------------------------------------------------

    let string_table: Vec<String> = pprof_profile
        .string_table
        .iter()
        .map(|s| s.to_string())
        .collect();

    // ------------------------------------------------------------------
    // Sample type & period type
    // ------------------------------------------------------------------

    let sample_type = pprof_profile.sample_type.first().map(|st| OtlpValueType {
        type_strindex: st.ty as i32,
        unit_strindex: st.unit as i32,
        aggregation_temporality: 0,
    });

    let period_type = pprof_profile.period_type.as_ref().map(|pt| OtlpValueType {
        type_strindex: pt.ty as i32,
        unit_strindex: pt.unit as i32,
        aggregation_temporality: 0,
    });

    // ------------------------------------------------------------------
    // Timestamps
    // ------------------------------------------------------------------

    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;

    let duration_nano = if pprof_profile.duration_nanos > 0 {
        pprof_profile.duration_nanos as u64
    } else {
        0
    };

    let time_unix_nano = if pprof_profile.time_nanos > 0 {
        pprof_profile.time_nanos as u64
    } else {
        now_nanos.saturating_sub(duration_nano)
    };

    // ------------------------------------------------------------------
    // Assemble OTLP Profile
    // ------------------------------------------------------------------

    let profile = OtlpProfile {
        profile_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
        sample_type,
        sample: samples,
        time_unix_nano,
        duration_nano,
        period_type,
        period: pprof_profile.period,
        comment_strindices: pprof_profile.comment.iter().map(|&c| c as i32).collect(),
        ..Default::default()
    };

    // ------------------------------------------------------------------
    // Dictionary (shared across all profiles in the request)
    // ------------------------------------------------------------------

    let dictionary = ProfilesDictionary {
        mapping_table,
        location_table,
        function_table,
        string_table,
        stack_table,
        link_table: vec![],
        attribute_table: vec![],
    };

    // ------------------------------------------------------------------
    // Wrap in OTLP envelope (resource will be injected by export_otlp)
    // ------------------------------------------------------------------

    Ok(ExportProfilesServiceRequest {
        resource_profiles: vec![ResourceProfiles {
            resource: None, // filled in by export_otlp → inject_resource
            scope_profiles: vec![ScopeProfiles {
                profiles: vec![profile],
                scope: None,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
        dictionary: Some(dictionary),
    })
}
