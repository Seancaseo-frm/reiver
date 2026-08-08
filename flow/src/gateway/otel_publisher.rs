//! Per-project OTel publisher for the AI Gateway.
//!
//! Uses the OpenTelemetry SDK's `Counter`, `Gauge`, and `Histogram` instruments
//! for metric accumulation — the SDK handles thread-safe cumulative totals
//! internally. A tokio task periodically collects from the SDK via `ManualReader`
//! and routes metrics per project_id to the watch service's OTLP endpoint.
//!
//! Spans and logs are buffered via a channel and flushed on a timer.

use chrono::{DateTime, Utc};
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter, MeterProvider};
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
use opentelemetry_sdk::metrics::reader::MetricReader;
use opentelemetry_sdk::metrics::{ManualReader, Pipeline, SdkMeterProvider, Temporality};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, warn};
use uuid::Uuid;

const CHANNEL_CAPACITY: usize = 8192;
const FLUSH_INTERVAL_SECS: u64 = 15;
const FLUSH_THRESHOLD: usize = 500;

const PROJECT_ID_ATTR: &str = "__project_id";

/// Bucket boundaries for LLM operation duration histograms (in seconds).
/// Covers sub-100ms responses through 2-minute long generations.
const HISTOGRAM_BOUNDARIES_SECONDS: &[f64] = &[
    0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0,
];

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Non-blocking per-project OTel publisher.
///
/// Metrics use SDK instruments (Counter/Gauge) for proper cumulative
/// accumulation. Spans and logs use a channel + background flush task.
#[derive(Clone)]
pub struct OTelPublisher {
    #[allow(dead_code)] // Held to keep the metric pipeline alive
    provider: SdkMeterProvider,
    meter: Meter,
    counters: Arc<Mutex<HashMap<String, Counter<f64>>>>,
    gauges: Arc<Mutex<HashMap<String, Gauge<f64>>>>,
    histograms: Arc<Mutex<HashMap<String, Histogram<f64>>>>,
    tx: mpsc::Sender<OTelItem>,
}

/// A span to publish to a user's project.
#[derive(Debug, Clone, Serialize)]
pub struct SpanData {
    pub project_key: String,
    pub trace_id: String,
    pub span_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_span_id: Option<String>,
    pub span_name: String,
    pub span_kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ns: Option<i64>,
    pub status_code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_message: Option<String>,
    pub span_attributes: HashMap<String, String>,
    pub resource_attributes: HashMap<String, String>,
}

/// A log record to publish to a user's project.
#[derive(Debug, Clone, Serialize)]
pub struct LogRecord {
    pub message: String,
    pub level: String,
    pub timestamp: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span_id: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub attributes: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// Internal types (spans/logs channel)
// ---------------------------------------------------------------------------

enum OTelItem {
    Span { project_id: Uuid, span: SpanData },
    Log { project_id: Uuid, record: LogRecord },
}

#[derive(Default)]
struct ProjectBuffer {
    spans: Vec<SpanData>,
    logs: Vec<LogRecord>,
}

impl ProjectBuffer {
    fn is_empty(&self) -> bool {
        self.spans.is_empty() && self.logs.is_empty()
    }
}

// ---------------------------------------------------------------------------
// SharedReader — cloneable wrapper so both the provider and flush task can hold it
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct SharedReader(Arc<ManualReader>);

impl MetricReader for SharedReader {
    fn register_pipeline(&self, pipeline: std::sync::Weak<Pipeline>) {
        self.0.register_pipeline(pipeline);
    }

    fn collect(&self, rm: &mut ResourceMetrics) -> opentelemetry_sdk::error::OTelSdkResult {
        self.0.collect(rm)
    }

    fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        self.0.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> opentelemetry_sdk::error::OTelSdkResult {
        self.0.shutdown_with_timeout(timeout)
    }

    fn temporality(&self, kind: opentelemetry_sdk::metrics::InstrumentKind) -> Temporality {
        self.0.temporality(kind)
    }
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

impl OTelPublisher {
    /// Start the publisher with background flush tasks for all telemetry.
    ///
    /// Metrics use SDK instruments (Counter/Gauge) collected via a ManualReader
    /// on a tokio interval. Spans and logs use a channel + background task.
    pub fn start(watch_url: String, http_client: reqwest::Client) -> Self {
        let reader = SharedReader(Arc::new(
            ManualReader::builder()
                .with_temporality(Temporality::Cumulative)
                .build(),
        ));
        let provider = SdkMeterProvider::builder()
            .with_reader(reader.clone())
            .build();
        let meter = provider.meter("flow-gateway");

        // Spawn a tokio task that periodically collects and exports metrics
        tokio::spawn(metric_flush_loop(
            reader,
            watch_url.clone(),
            http_client.clone(),
        ));

        // Spans/logs: channel + background task
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        tokio::spawn(flush_loop(rx, watch_url, http_client));

        Self {
            provider,
            meter,
            counters: Arc::new(Mutex::new(HashMap::new())),
            gauges: Arc::new(Mutex::new(HashMap::new())),
            histograms: Arc::new(Mutex::new(HashMap::new())),
            tx,
        }
    }

    /// Emit a monotonic counter increment for a user project. Non-blocking.
    ///
    /// The SDK Counter accumulates a running cumulative total per unique
    /// attribute set. The periodic exporter flushes the current totals to the
    /// watch service grouped by project_id.
    pub fn emit_counter(
        &self,
        project_id: Uuid,
        name: &str,
        value: f64,
        labels: BTreeMap<String, String>,
    ) {
        let counter = self.get_or_create_counter(name);
        let attrs = build_attrs(project_id, &labels);
        counter.add(value, &attrs);
    }

    /// Emit a gauge observation for a user project. Non-blocking.
    pub fn emit_gauge(
        &self,
        project_id: Uuid,
        name: &str,
        value: f64,
        labels: BTreeMap<String, String>,
    ) {
        let gauge = self.get_or_create_gauge(name);
        let attrs = build_attrs(project_id, &labels);
        gauge.record(value, &attrs);
    }

    /// Emit a histogram observation for a user project. Non-blocking.
    ///
    /// The SDK Histogram accumulates bucketed distributions. The periodic
    /// exporter flushes count, sum, and bucket data to Watch which derives
    /// `.count`, `.sum`, and `_bucket` series for PromQL.
    pub fn emit_histogram(
        &self,
        project_id: Uuid,
        name: &str,
        value: f64,
        labels: BTreeMap<String, String>,
    ) {
        let histogram = self.get_or_create_histogram(name);
        let attrs = build_attrs(project_id, &labels);
        histogram.record(value, &attrs);
    }

    /// Emit a span for a user project. Non-blocking.
    pub fn emit_span(&self, project_id: Uuid, span: SpanData) {
        let _ = self.tx.try_send(OTelItem::Span { project_id, span });
    }

    /// Emit a log record for a user project. Non-blocking.
    pub fn emit_log(&self, project_id: Uuid, record: LogRecord) {
        let _ = self.tx.try_send(OTelItem::Log { project_id, record });
    }

    fn get_or_create_counter(&self, name: &str) -> Counter<f64> {
        let map = self.counters.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(c) = map.get(name) {
            return c.clone();
        }
        drop(map);

        let counter = self.meter.f64_counter(name.to_string()).build();
        let mut map = self.counters.lock().unwrap_or_else(|e| e.into_inner());
        map.entry(name.to_string()).or_insert_with(|| counter).clone()
    }

    fn get_or_create_gauge(&self, name: &str) -> Gauge<f64> {
        let map = self.gauges.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(g) = map.get(name) {
            return g.clone();
        }
        drop(map);

        let gauge = self.meter.f64_gauge(name.to_string()).build();
        let mut map = self.gauges.lock().unwrap_or_else(|e| e.into_inner());
        map.entry(name.to_string()).or_insert_with(|| gauge).clone()
    }

    fn get_or_create_histogram(&self, name: &str) -> Histogram<f64> {
        let map = self.histograms.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(h) = map.get(name) {
            return h.clone();
        }
        drop(map);

        let histogram = self
            .meter
            .f64_histogram(name.to_string())
            .with_unit("s")
            .with_boundaries(HISTOGRAM_BOUNDARIES_SECONDS.to_vec())
            .build();
        let mut map = self.histograms.lock().unwrap_or_else(|e| e.into_inner());
        map.entry(name.to_string())
            .or_insert_with(|| histogram)
            .clone()
    }
}

fn build_attrs(project_id: Uuid, labels: &BTreeMap<String, String>) -> Vec<KeyValue> {
    let mut attrs = Vec::with_capacity(labels.len() + 1);
    attrs.push(KeyValue::new(PROJECT_ID_ATTR, project_id.to_string()));
    for (k, v) in labels {
        attrs.push(KeyValue::new(k.clone(), v.clone()));
    }
    attrs
}

// ---------------------------------------------------------------------------
// Metric flush loop — collects from SDK and routes per project_id
// ---------------------------------------------------------------------------

async fn metric_flush_loop(
    reader: SharedReader,
    watch_url: String,
    client: reqwest::Client,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(FLUSH_INTERVAL_SECS));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut rm = ResourceMetrics::default();

    loop {
        interval.tick().await;

        if let Err(e) = reader.collect(&mut rm) {
            warn!(error = %e, "Failed to collect metrics from SDK");
            continue;
        }

        let grouped = group_metrics_by_project(&rm);
        for (project_id, otlp_metrics) in grouped {
            flush_metrics(&client, &watch_url, &project_id, otlp_metrics).await;
        }
    }
}

/// Walk the SDK's ResourceMetrics tree and group OTLP metrics by project_id.
fn group_metrics_by_project(rm: &ResourceMetrics) -> HashMap<String, Vec<OtlpMetric>> {
    let mut grouped: HashMap<String, Vec<OtlpMetric>> = HashMap::new();

    for scope_metrics in rm.scope_metrics() {
        for metric in scope_metrics.metrics() {
            let name = metric.name().to_string();

            match metric.data() {
                AggregatedMetrics::F64(MetricData::Sum(sum)) => {
                    let time_unix_nano = sum
                        .time()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos()
                        .to_string();
                    for dp in sum.data_points() {
                        let (project_id, attrs) = extract_project_and_attrs(dp.attributes());
                        let otlp = OtlpMetric {
                            name: name.clone(),
                            data: OtlpMetricData::Sum(OtlpSum {
                                data_points: vec![OtlpNumberDataPoint {
                                    time_unix_nano: time_unix_nano.clone(),
                                    as_double: dp.value(),
                                    attributes: attrs,
                                }],
                                aggregation_temporality: AGGREGATION_TEMPORALITY_CUMULATIVE,
                                is_monotonic: true,
                            }),
                        };
                        grouped.entry(project_id).or_default().push(otlp);
                    }
                }
                AggregatedMetrics::F64(MetricData::Gauge(gauge)) => {
                    let time_unix_nano = gauge
                        .time()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos()
                        .to_string();
                    for dp in gauge.data_points() {
                        let (project_id, attrs) = extract_project_and_attrs(dp.attributes());
                        let otlp = OtlpMetric {
                            name: name.clone(),
                            data: OtlpMetricData::Gauge(OtlpGauge {
                                data_points: vec![OtlpNumberDataPoint {
                                    time_unix_nano: time_unix_nano.clone(),
                                    as_double: dp.value(),
                                    attributes: attrs,
                                }],
                            }),
                        };
                        grouped.entry(project_id).or_default().push(otlp);
                    }
                }
                AggregatedMetrics::F64(MetricData::Histogram(hist)) => {
                    let time_unix_nano = hist
                        .time()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos()
                        .to_string();
                    let start_time_unix_nano = hist
                        .start_time()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos()
                        .to_string();
                    for dp in hist.data_points() {
                        let (project_id, attrs) = extract_project_and_attrs(dp.attributes());
                        let explicit_bounds: Vec<f64> = dp.bounds().collect();
                        let bucket_counts: Vec<u64> = dp.bucket_counts().collect();
                        let otlp = OtlpMetric {
                            name: name.clone(),
                            data: OtlpMetricData::Histogram(OtlpHistogram {
                                data_points: vec![OtlpHistogramDataPoint {
                                    time_unix_nano: time_unix_nano.clone(),
                                    start_time_unix_nano: start_time_unix_nano.clone(),
                                    count: dp.count(),
                                    sum: dp.sum(),
                                    explicit_bounds,
                                    bucket_counts,
                                    attributes: attrs,
                                }],
                                aggregation_temporality: AGGREGATION_TEMPORALITY_CUMULATIVE,
                            }),
                        };
                        grouped.entry(project_id).or_default().push(otlp);
                    }
                }
                _ => {}
            }
        }
    }

    grouped
}

/// Extract __project_id from attributes, returning it separately and the rest as OTLP attrs.
fn extract_project_and_attrs<'a>(
    attrs: impl Iterator<Item = &'a KeyValue>,
) -> (String, Vec<OtlpKeyValue>) {
    let mut project_id = String::new();
    let mut otlp_attrs = Vec::new();
    for kv in attrs {
        if kv.key.as_str() == PROJECT_ID_ATTR {
            project_id = kv.value.as_str().to_string();
        } else {
            otlp_attrs.push(OtlpKeyValue {
                key: kv.key.as_str().to_string(),
                value: OtlpAnyValue {
                    string_value: kv.value.as_str().to_string(),
                },
            });
        }
    }
    (project_id, otlp_attrs)
}

// ---------------------------------------------------------------------------
// Background flush loop (spans + logs only)
// ---------------------------------------------------------------------------

async fn flush_loop(
    mut rx: mpsc::Receiver<OTelItem>,
    watch_url: String,
    client: reqwest::Client,
) {
    let mut buffers: HashMap<Uuid, ProjectBuffer> = HashMap::new();
    let mut total_buffered: usize = 0;
    let mut interval = tokio::time::interval(Duration::from_secs(FLUSH_INTERVAL_SECS));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            item = rx.recv() => {
                match item {
                    Some(otel_item) => {
                        let project_id = match &otel_item {
                            OTelItem::Span { project_id, .. } => *project_id,
                            OTelItem::Log { project_id, .. } => *project_id,
                        };
                        let buf = buffers.entry(project_id).or_default();
                        match otel_item {
                            OTelItem::Span { span, .. } => buf.spans.push(span),
                            OTelItem::Log { record, .. } => buf.logs.push(record),
                        }
                        total_buffered += 1;

                        if total_buffered >= FLUSH_THRESHOLD {
                            flush_all(&client, &watch_url, &mut buffers).await;
                            total_buffered = 0;
                        }
                    }
                    None => {
                        if total_buffered > 0 {
                            flush_all(&client, &watch_url, &mut buffers).await;
                        }
                        debug!("OTelPublisher channel closed, flush loop exiting");
                        return;
                    }
                }
            }
            _ = interval.tick() => {
                if total_buffered > 0 {
                    flush_all(&client, &watch_url, &mut buffers).await;
                    total_buffered = 0;
                }
            }
        }
    }
}

async fn flush_all(
    client: &reqwest::Client,
    watch_url: &str,
    buffers: &mut HashMap<Uuid, ProjectBuffer>,
) {
    for (project_id, buf) in buffers.iter_mut() {
        if buf.is_empty() {
            continue;
        }

        let project_id_str = project_id.to_string();

        if !buf.spans.is_empty() {
            let spans = std::mem::take(&mut buf.spans);
            flush_spans(client, watch_url, &project_id_str, spans).await;
        }

        if !buf.logs.is_empty() {
            let logs = std::mem::take(&mut buf.logs);
            flush_logs(client, watch_url, &project_id_str, logs).await;
        }
    }

    buffers.retain(|_, buf| !buf.is_empty());
}

// ---------------------------------------------------------------------------
// Flush helpers — POST to watch service OTLP endpoints
// ---------------------------------------------------------------------------

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OtlpResource {
    attributes: Vec<OtlpKeyValue>,
}

#[derive(Serialize)]
struct OtlpScope {
    name: String,
}

#[derive(Serialize)]
struct OtlpKeyValue {
    key: String,
    value: OtlpAnyValue,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OtlpAnyValue {
    string_value: String,
}

// ---------------------------------------------------------------------------
// OTLP Metrics
// ---------------------------------------------------------------------------

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OtlpMetricsRequest {
    resource_metrics: Vec<OtlpResourceMetrics>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OtlpResourceMetrics {
    resource: OtlpResource,
    scope_metrics: Vec<OtlpScopeMetrics>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OtlpScopeMetrics {
    scope: OtlpScope,
    metrics: Vec<OtlpMetric>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OtlpMetric {
    name: String,
    #[serde(flatten)]
    data: OtlpMetricData,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
enum OtlpMetricData {
    Sum(OtlpSum),
    Gauge(OtlpGauge),
    Histogram(OtlpHistogram),
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OtlpSum {
    data_points: Vec<OtlpNumberDataPoint>,
    aggregation_temporality: i32,
    is_monotonic: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OtlpGauge {
    data_points: Vec<OtlpNumberDataPoint>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OtlpNumberDataPoint {
    time_unix_nano: String,
    #[serde(rename = "asDouble")]
    as_double: f64,
    attributes: Vec<OtlpKeyValue>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OtlpHistogram {
    data_points: Vec<OtlpHistogramDataPoint>,
    aggregation_temporality: i32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OtlpHistogramDataPoint {
    time_unix_nano: String,
    start_time_unix_nano: String,
    count: u64,
    sum: f64,
    bucket_counts: Vec<u64>,
    explicit_bounds: Vec<f64>,
    attributes: Vec<OtlpKeyValue>,
}

const AGGREGATION_TEMPORALITY_CUMULATIVE: i32 = 2;

async fn flush_metrics(
    client: &reqwest::Client,
    watch_url: &str,
    project_id: &str,
    metrics: Vec<OtlpMetric>,
) {
    if metrics.is_empty() {
        return;
    }
    let count = metrics.len();
    let url = format!("{}/api/v1/metrics", watch_url.trim_end_matches('/'));

    let payload = OtlpMetricsRequest {
        resource_metrics: vec![OtlpResourceMetrics {
            resource: OtlpResource { attributes: vec![] },
            scope_metrics: vec![OtlpScopeMetrics {
                scope: OtlpScope { name: "flow-gateway".into() },
                metrics,
            }],
        }],
    };

    match client
        .post(&url)
        .header("X-Project-Id", project_id)
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => {
            debug!(project_id = %project_id, count, "Flushed metrics via OTLP");
        }
        Ok(resp) => {
            warn!(project_id = %project_id, status = %resp.status(), count, "Watch rejected metrics");
        }
        Err(e) => {
            error!(project_id = %project_id, error = %e, count, "Failed to flush metrics");
        }
    }
}

// ---------------------------------------------------------------------------
// OTLP Traces
// ---------------------------------------------------------------------------

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OtlpTracesRequest {
    resource_spans: Vec<OtlpResourceSpans>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OtlpResourceSpans {
    resource: OtlpResource,
    scope_spans: Vec<OtlpScopeSpans>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OtlpScopeSpans {
    scope: OtlpScope,
    spans: Vec<OtlpSpan>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OtlpSpan {
    trace_id: String,
    span_id: String,
    name: String,
    kind: i32,
    start_time_unix_nano: String,
    end_time_unix_nano: String,
    attributes: Vec<OtlpKeyValue>,
    status: OtlpStatus,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OtlpStatus {
    code: i32,
    #[serde(skip_serializing_if = "String::is_empty")]
    message: String,
}

const SPAN_KIND_CLIENT: i32 = 3;
const STATUS_CODE_OK: i32 = 1;
const STATUS_CODE_ERROR: i32 = 2;

async fn flush_spans(
    client: &reqwest::Client,
    watch_url: &str,
    project_id: &str,
    spans: Vec<SpanData>,
) {
    let count = spans.len();
    let url = format!("{}/api/v1/traces", watch_url.trim_end_matches('/'));

    let otlp_spans: Vec<OtlpSpan> = spans
        .into_iter()
        .map(|s| {
            let start_nanos = s
                .start_time
                .map(|t| t.timestamp_nanos_opt().unwrap_or(0))
                .unwrap_or(0) as u64;
            let end_nanos = start_nanos + s.duration_ns.unwrap_or(0).max(0) as u64;

            let attributes: Vec<OtlpKeyValue> = s
                .span_attributes
                .iter()
                .map(|(k, v)| OtlpKeyValue {
                    key: k.clone(),
                    value: OtlpAnyValue { string_value: v.clone() },
                })
                .collect();

            let status_code = match s.status_code.as_str() {
                "STATUS_CODE_OK" => STATUS_CODE_OK,
                "STATUS_CODE_ERROR" => STATUS_CODE_ERROR,
                _ => 0,
            };

            OtlpSpan {
                trace_id: s.trace_id,
                span_id: s.span_id,
                name: s.span_name,
                kind: SPAN_KIND_CLIENT,
                start_time_unix_nano: start_nanos.to_string(),
                end_time_unix_nano: end_nanos.to_string(),
                attributes,
                status: OtlpStatus {
                    code: status_code,
                    message: s.status_message.unwrap_or_default(),
                },
            }
        })
        .collect();

    let payload = OtlpTracesRequest {
        resource_spans: vec![OtlpResourceSpans {
            resource: OtlpResource { attributes: vec![] },
            scope_spans: vec![OtlpScopeSpans {
                scope: OtlpScope { name: "flow-gateway".into() },
                spans: otlp_spans,
            }],
        }],
    };

    match client
        .post(&url)
        .header("X-Project-Id", project_id)
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => {
            debug!(project_id = %project_id, count, "Flushed spans via OTLP");
        }
        Ok(resp) => {
            warn!(project_id = %project_id, status = %resp.status(), count, "Watch rejected spans");
        }
        Err(e) => {
            error!(project_id = %project_id, error = %e, count, "Failed to flush spans");
        }
    }
}

// ---------------------------------------------------------------------------
// OTLP Logs
// ---------------------------------------------------------------------------

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OtlpLogsRequest {
    resource_logs: Vec<OtlpResourceLogs>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OtlpResourceLogs {
    resource: OtlpResource,
    scope_logs: Vec<OtlpScopeLogs>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OtlpScopeLogs {
    scope: OtlpScope,
    log_records: Vec<OtlpLogRecord>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OtlpLogRecord {
    time_unix_nano: String,
    severity_number: u8,
    severity_text: String,
    body: OtlpAnyValue,
    attributes: Vec<OtlpKeyValue>,
}

fn severity_to_number(level: &str) -> u8 {
    match level.to_lowercase().as_str() {
        "trace" => 1,
        "debug" => 5,
        "info" => 9,
        "warn" | "warning" => 13,
        "error" => 17,
        "fatal" => 21,
        _ => 9,
    }
}

async fn flush_logs(
    client: &reqwest::Client,
    watch_url: &str,
    project_id: &str,
    logs: Vec<LogRecord>,
) {
    let count = logs.len();
    let url = format!("{}/api/v1/logs", watch_url.trim_end_matches('/'));

    let log_records: Vec<OtlpLogRecord> = logs
        .into_iter()
        .map(|log| {
            let time_unix_nano = log.timestamp.timestamp_nanos_opt().unwrap_or(0).to_string();
            let severity_number = severity_to_number(&log.level);

            let mut attributes: Vec<OtlpKeyValue> = log
                .attributes
                .into_iter()
                .map(|(k, v)| OtlpKeyValue {
                    key: k,
                    value: OtlpAnyValue { string_value: v },
                })
                .collect();

            if let Some(trace_id) = log.trace_id {
                attributes.push(OtlpKeyValue {
                    key: "trace_id".into(),
                    value: OtlpAnyValue { string_value: trace_id },
                });
            }
            if let Some(span_id) = log.span_id {
                attributes.push(OtlpKeyValue {
                    key: "span_id".into(),
                    value: OtlpAnyValue { string_value: span_id },
                });
            }
            if let Some(source) = log.source {
                attributes.push(OtlpKeyValue {
                    key: "source".into(),
                    value: OtlpAnyValue { string_value: source },
                });
            }

            OtlpLogRecord {
                time_unix_nano,
                severity_number,
                severity_text: log.level.to_uppercase(),
                body: OtlpAnyValue { string_value: log.message },
                attributes,
            }
        })
        .collect();

    let payload = OtlpLogsRequest {
        resource_logs: vec![OtlpResourceLogs {
            resource: OtlpResource { attributes: vec![] },
            scope_logs: vec![OtlpScopeLogs {
                scope: OtlpScope { name: "flow-gateway".into() },
                log_records,
            }],
        }],
    };

    match client
        .post(&url)
        .header("X-Project-Id", project_id)
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => {
            debug!(project_id = %project_id, count, "Flushed logs via OTLP");
        }
        Ok(resp) => {
            warn!(project_id = %project_id, status = %resp.status(), count, "Watch rejected logs");
        }
        Err(e) => {
            error!(project_id = %project_id, error = %e, count, "Failed to flush logs");
        }
    }
}
