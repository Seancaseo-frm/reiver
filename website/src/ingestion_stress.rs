//! Minimal OTLP JSON payloads and load generator for admin ingestion stress tests.

use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Debug, serde::Serialize)]
pub struct StressResult {
    pub sent: u64,
    pub errors: u64,
    pub duration_ms: u64,
}

/// OTLP trace id: 16 bytes = 32 hex chars.
fn new_trace_id() -> String {
    format!("{:032x}", Uuid::new_v4().as_u128())
}

/// Span id: 8 bytes = 16 hex chars.
fn new_span_id() -> String {
    format!("{:016x}", (Uuid::new_v4().as_u128() >> 64) as u64)
}

/// ~5KB padding string used to bulk up each span/log record.
fn padding_block(seed: usize) -> String {
    let base = format!(
        "stress-payload-{seed:08}-abcdefghijklmnopqrstuvwxyz0123456789_ABCDEFGHIJKLMNOPQRSTUVWXYZ-"
    );
    base.repeat(4600 / base.len() + 1)[..4600].to_string()
}

/// 200 spans, each ~5KB (padded via a large attribute value).
fn trace_payload(trace_id: &str, _root_span_id: &str) -> serde_json::Value {
    let now_nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

    const HTTP_METHODS: [&str; 5] = ["GET", "POST", "PUT", "DELETE", "PATCH"];

    let spans: Vec<serde_json::Value> = (0..200)
        .map(|i| {
            let span_id = new_span_id();
            let start = now_nanos + (i as i64) * 1_000_000;
            let end = start + 50_000_000;
            let method = HTTP_METHODS[i % 5];
            let status_code = if i % 20 == 0 { 2 } else { 1 };
            let http_status = (200 + (i % 5) * 100).to_string();
            json!({
                "traceId": trace_id,
                "spanId": span_id,
                "name": format!("stress-span-{i}"),
                "kind": (i % 5) + 1,
                "startTimeUnixNano": start.to_string(),
                "endTimeUnixNano": end.to_string(),
                "status": { "code": status_code },
                "attributes": [
                    { "key": "http.method", "value": { "stringValue": method } },
                    { "key": "http.url", "value": { "stringValue": format!("/api/v1/resource/{i}") } },
                    { "key": "http.status_code", "value": { "intValue": http_status } },
                    { "key": "stress.iteration", "value": { "intValue": i.to_string() } },
                    { "key": "stress.padding", "value": { "stringValue": padding_block(i) } }
                ]
            })
        })
        .collect();

    json!({
        "resourceSpans": [{
            "resource": {
                "attributes": [
                    { "key": "service.name", "value": { "stringValue": "admin-stress-test" } },
                    { "key": "deployment.environment", "value": { "stringValue": "stress" } },
                    { "key": "service.version", "value": { "stringValue": "0.0.1" } }
                ]
            },
            "scopeSpans": [{
                "scope": { "name": "stress" },
                "spans": spans
            }]
        }]
    })
}

/// 200 log records, each ~5KB (padded via a large body).
fn log_payload(trace_id: &str) -> serde_json::Value {
    let now_nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let severities = [
        (1, "TRACE"),
        (5, "DEBUG"),
        (9, "INFO"),
        (13, "WARN"),
        (17, "ERROR"),
    ];

    let records: Vec<serde_json::Value> = (0..200)
        .map(|i| {
            let (sev_num, sev_text) = severities[i % severities.len()];
            let ts = now_nanos + (i as i64) * 500_000;
            let body = format!(
                "[{sev_text}] stress-log-{i}: request_id={} user_id=usr_{} action=process_item item_id={} {}",
                Uuid::new_v4(),
                i % 1000,
                i * 7,
                padding_block(i),
            );
            json!({
                "timeUnixNano": ts.to_string(),
                "severityNumber": sev_num,
                "severityText": sev_text,
                "body": { "stringValue": body },
                "traceId": trace_id,
                "spanId": "",
                "attributes": [
                    { "key": "log.source", "value": { "stringValue": "stress-test" } },
                    { "key": "thread.id", "value": { "intValue": (i % 16).to_string() } },
                    { "key": "code.function", "value": { "stringValue": format!("handler_{}", i % 10) } }
                ]
            })
        })
        .collect();

    json!({
        "resourceLogs": [{
            "resource": {
                "attributes": [
                    { "key": "service.name", "value": { "stringValue": "admin-stress-test" } },
                    { "key": "deployment.environment", "value": { "stringValue": "stress" } },
                    { "key": "service.version", "value": { "stringValue": "0.0.1" } }
                ]
            },
            "scopeLogs": [{
                "scope": { "name": "stress" },
                "logRecords": records
            }]
        }]
    })
}

/// 10 metrics × 20 data points each = 200 data points with varied attributes.
/// Mixes gauges, sums, and histograms for realistic ingestion load (~100KB per request).
fn metrics_payload() -> serde_json::Value {
    let now_nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

    let gauge_metrics: Vec<&str> = vec![
        "stress.cpu.utilization",
        "stress.memory.used_bytes",
        "stress.disk.io_time",
        "stress.network.bytes_recv",
    ];
    let sum_metrics: Vec<&str> = vec![
        "stress.http.requests_total",
        "stress.db.queries_total",
        "stress.cache.hits_total",
    ];
    let hist_metrics: Vec<&str> = vec![
        "stress.http.request_duration_ms",
        "stress.db.query_duration_ms",
        "stress.queue.wait_time_ms",
    ];

    let mut metrics = Vec::new();

    const REGIONS: [&str; 4] = ["us-east-1", "us-west-2", "eu-west-1", "ap-south-1"];

    for (mi, name) in gauge_metrics.iter().enumerate() {
        let data_points: Vec<serde_json::Value> = (0..20)
            .map(|i| {
                let ts = now_nanos + (i as i64) * 1_000_000;
                let region = REGIONS[i % 4];
                json!({
                    "timeUnixNano": ts.to_string(),
                    "asDouble": (mi * 20 + i) as f64 * 1.5 + 0.1,
                    "attributes": [
                        { "key": "host.name", "value": { "stringValue": format!("node-{}", i % 4) } },
                        { "key": "region", "value": { "stringValue": region } },
                        { "key": "instance", "value": { "stringValue": format!("i-{:08x}", mi * 1000 + i) } }
                    ]
                })
            })
            .collect();
        metrics.push(json!({
            "name": name,
            "gauge": { "dataPoints": data_points }
        }));
    }

    const SUM_METHODS: [&str; 4] = ["GET", "POST", "PUT", "DELETE"];
    const STATUS_CODES: [u16; 4] = [200, 201, 400, 500];

    for (mi, name) in sum_metrics.iter().enumerate() {
        let data_points: Vec<serde_json::Value> = (0..20)
            .map(|i| {
                let ts = now_nanos + (i as i64) * 1_000_000;
                let method = SUM_METHODS[i % 4];
                let status = STATUS_CODES[i % 4].to_string();
                json!({
                    "timeUnixNano": ts.to_string(),
                    "asDouble": (mi * 20 + i) as f64 * 100.0 + 42.0,
                    "attributes": [
                        { "key": "http.method", "value": { "stringValue": method } },
                        { "key": "http.route", "value": { "stringValue": format!("/api/v{}/resource", (i % 3) + 1) } },
                        { "key": "status_code", "value": { "intValue": status } }
                    ]
                })
            })
            .collect();
        metrics.push(json!({
            "name": name,
            "sum": {
                "dataPoints": data_points,
                "aggregationTemporality": 2,
                "isMonotonic": true
            }
        }));
    }

    for (mi, name) in hist_metrics.iter().enumerate() {
        let data_points: Vec<serde_json::Value> = (0..20)
            .map(|i| {
                let ts = now_nanos + (i as i64) * 1_000_000;
                let count = (mi * 20 + i + 1) as u64 * 10;
                let sum_val = count as f64 * 25.5;
                let hist_method = if i % 2 == 0 { "GET" } else { "POST" };
                let buckets = vec![count/5, count/4, count/3, count/2, count];
                json!({
                    "timeUnixNano": ts.to_string(),
                    "count": count,
                    "sum": sum_val,
                    "bucketCounts": buckets,
                    "explicitBounds": [10.0, 50.0, 100.0, 500.0],
                    "attributes": [
                        { "key": "endpoint", "value": { "stringValue": format!("/api/endpoint-{}", i % 5) } },
                        { "key": "method", "value": { "stringValue": hist_method } }
                    ]
                })
            })
            .collect();
        metrics.push(json!({
            "name": name,
            "histogram": {
                "dataPoints": data_points,
                "aggregationTemporality": 2
            }
        }));
    }

    json!({
        "resourceMetrics": [{
            "resource": {
                "attributes": [
                    { "key": "service.name", "value": { "stringValue": "admin-stress-test" } },
                    { "key": "deployment.environment", "value": { "stringValue": "stress" } },
                    { "key": "service.version", "value": { "stringValue": "0.0.1" } }
                ]
            },
            "scopeMetrics": [{
                "scope": { "name": "stress" },
                "metrics": metrics
            }]
        }]
    })
}

#[derive(Clone, Copy)]
enum Kind {
    Traces,
    Logs,
    Metrics,
}

/// Sends OTLP JSON to Watch at `rps` concurrently, choosing traces vs logs vs metrics
/// at random each request, until `cancel` fires.
///
/// Uses a semaphore-bounded fan-out so in-flight requests don't pile up unbounded.
/// A rate-limiter ticker fires `rps` times per second; each tick spawns one request task.
pub async fn run_stress(
    client: &reqwest::Client,
    watch_base: &str,
    project_id: Uuid,
    rps: u32,
    cancel: &CancellationToken,
) -> StressResult {
    let base: Arc<str> = Arc::from(watch_base.trim_end_matches('/'));
    let project_header = project_id.to_string();

    let started = Instant::now();
    let sent = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let window_sent = Arc::new(AtomicU64::new(0));
    let window_errors = Arc::new(AtomicU64::new(0));

    let max_in_flight = (rps as usize * 4).max(64);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(max_in_flight));

    let tick_interval = tokio::time::Duration::from_secs_f64(1.0 / f64::from(rps.max(1)));
    let mut ticker = tokio::time::interval(tick_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);

    let mut log_interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
    log_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut window_start = Instant::now();

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            _ = log_interval.tick() => {
                let elapsed = window_start.elapsed().as_secs_f64();
                let ws = window_sent.swap(0, Ordering::Relaxed);
                let we = window_errors.swap(0, Ordering::Relaxed);
                let rps_actual = if elapsed > 0.0 { ws as f64 / elapsed } else { 0.0 };
                let in_flight = max_in_flight - semaphore.available_permits();
                tracing::info!(
                    "[STRESS] req/s={:.1} window_sent={} window_errors={} total_sent={} total_errors={} in_flight={} elapsed={:.0}s",
                    rps_actual, ws, we,
                    sent.load(Ordering::Relaxed),
                    errors.load(Ordering::Relaxed),
                    in_flight,
                    started.elapsed().as_secs_f64(),
                );
                window_start = Instant::now();
            }
            _ = ticker.tick() => {
                let permit = match semaphore.clone().try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => {
                        match tokio::select! {
                            p = semaphore.clone().acquire_owned() => Ok(p),
                            _ = cancel.cancelled() => Err(()),
                        } {
                            Ok(Ok(p)) => p,
                            _ => break,
                        }
                    }
                };

                let client = client.clone();
                let base = base.clone();
                let project_header = project_header.clone();
                let sent = sent.clone();
                let errors = errors.clone();
                let ws = window_sent.clone();
                let we = window_errors.clone();
                let cancel = cancel.clone();

                tokio::spawn(async move {
                    let _permit = permit;

                    if cancel.is_cancelled() {
                        return;
                    }

                    let kind = match rand::random::<u8>() % 3 {
                        0 => Kind::Traces,
                        1 => Kind::Logs,
                        _ => Kind::Metrics,
                    };

                    let trace_id = new_trace_id();
                    let span_id = new_span_id();

                    let (path, body) = match kind {
                        Kind::Traces => ("/api/v1/traces", trace_payload(&trace_id, &span_id)),
                        Kind::Logs => ("/api/v1/logs", log_payload(&trace_id)),
                        Kind::Metrics => ("/api/v1/metrics", metrics_payload()),
                    };

                    let url = format!("{base}{path}");
                    let res = client
                        .post(&url)
                        .header("X-Project-Id", &project_header)
                        .header("Content-Type", "application/json")
                        .json(&body)
                        .send()
                        .await;

                    match res {
                        Ok(r) if r.status().is_success() => {
                            sent.fetch_add(1, Ordering::Relaxed);
                            ws.fetch_add(1, Ordering::Relaxed);
                        }
                        _ => {
                            errors.fetch_add(1, Ordering::Relaxed);
                            we.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }
        }
    }

    StressResult {
        sent: sent.load(Ordering::Relaxed),
        errors: errors.load(Ordering::Relaxed),
        duration_ms: started.elapsed().as_millis() as u64,
    }
}
