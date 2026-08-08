use axum::{
    extract::{Path, State},
    response::Json,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::app_state::WatchState;
use crate::error::{AppError, Result};

pub fn create_system_overview_router() -> Router<Arc<WatchState>> {
    Router::new()
        .route("/{project_id}/stack", get(get_stack))
        .route("/{project_id}/context", post(get_context))
}

// ---------------------------------------------------------------------------
// Technology Registry
// ---------------------------------------------------------------------------

struct TechDef {
    prefix: &'static str,
    name: &'static str,
    tier: &'static str,
    golden_signals: &'static [(&'static str, &'static str, &'static str)], // (label, promql, unit)
}

const TECH_REGISTRY: &[TechDef] = &[
    TechDef {
        prefix: "http.server.request",
        name: "HTTP Service",
        tier: "application",
        golden_signals: &[
            ("Request Rate", "rate(http.server.request.duration.count[5m])", "req/s"),
            ("Error Rate", "sum(rate(http.server.request.duration.count{http.response.status_code=~\"5..\"}[5m]))", "err/s"),
            ("Latency P95", "histogram_quantile(0.95, rate(http.server.request.duration_bucket[5m]))", "ms"),
        ],
    },
    TechDef {
        prefix: "rpc.server.duration",
        name: "gRPC Service",
        tier: "application",
        golden_signals: &[
            ("Request Rate", "rate(rpc.server.duration.count[5m])", "req/s"),
            ("Error Rate", "sum(rate(rpc.server.duration.count{rpc.grpc.status_code!=\"0\"}[5m]))", "err/s"),
            ("Latency P95", "histogram_quantile(0.95, rate(rpc.server.duration_bucket[5m]))", "ms"),
        ],
    },
    TechDef {
        prefix: "kafka.",
        name: "Kafka",
        tier: "queue",
        golden_signals: &[
            ("Consumer Lag", "kafka.consumer_group.lag", "messages"),
            ("Partitions", "kafka.topic.partitions", "partitions"),
            ("Messages In/s", "rate(kafka.broker.messages_in[5m])", "msg/s"),
        ],
    },
    TechDef {
        prefix: "postgresql.",
        name: "PostgreSQL",
        tier: "database",
        golden_signals: &[
            ("Active Connections", "postgresql.backends", "connections"),
            ("Commits/s", "rate(postgresql.commits[5m])", "ops/s"),
            ("Rows Returned/s", "rate(postgresql.rows_returned[5m])", "rows/s"),
        ],
    },
    TechDef {
        prefix: "redis.",
        name: "Redis",
        tier: "cache",
        golden_signals: &[
            ("Commands/s", "rate(redis.commands.processed[5m])", "ops/s"),
            ("Connected Clients", "redis.connected_clients", "clients"),
            ("Memory Used", "redis.memory.used", "bytes"),
        ],
    },
    TechDef {
        prefix: "mysql.",
        name: "MySQL",
        tier: "database",
        golden_signals: &[
            ("Connections", "mysql.connections", "connections"),
            ("Queries/s", "rate(mysql.queries[5m])", "queries/s"),
        ],
    },
    TechDef {
        prefix: "mongodb.",
        name: "MongoDB",
        tier: "database",
        golden_signals: &[
            ("Connections", "mongodb.connection.count", "connections"),
            ("Operations/s", "rate(mongodb.operation.count[5m])", "ops/s"),
        ],
    },
    TechDef {
        prefix: "ClickHouseMetrics_",
        name: "ClickHouse",
        tier: "database",
        golden_signals: &[
            ("TCP Connections", "ClickHouseMetrics_TCPConnection", "connections"),
            ("Running Queries", "ClickHouseMetrics_Query", "queries"),
        ],
    },
    TechDef {
        prefix: "system.cpu",
        name: "Host Metrics",
        tier: "infrastructure",
        golden_signals: &[
            ("CPU Utilization", "system.cpu.utilization", "%"),
            ("Memory Utilization", "system.memory.utilization", "%"),
            ("Disk I/O", "rate(system.disk.io[5m])", "bytes/s"),
        ],
    },
    TechDef {
        prefix: "k8s.",
        name: "Kubernetes",
        tier: "infrastructure",
        golden_signals: &[
            ("Pod Count", "k8s.pod.phase", "pods"),
            ("Node Ready", "k8s.node.condition_ready", "nodes"),
            ("Container Restarts", "rate(k8s.container.restarts[5m])", "restarts/s"),
        ],
    },
    TechDef {
        prefix: "runtime.go.",
        name: "Go Runtime",
        tier: "runtime",
        golden_signals: &[
            ("Goroutines", "runtime.go.goroutines", "goroutines"),
            ("Heap Alloc", "runtime.go.mem.heap_alloc", "bytes"),
            ("GC Pause", "rate(runtime.go.gc.pause_ns.count[5m])", "ns/s"),
        ],
    },
    TechDef {
        prefix: "nodejs.",
        name: "Node.js",
        tier: "runtime",
        golden_signals: &[
            ("Event Loop Delay", "nodejs.eventloop.delay.mean", "ms"),
            ("Active Handles", "nodejs.active_handles.total", "handles"),
            ("Heap Used", "nodejs.memory.heap.used", "bytes"),
        ],
    },
];

const TIER_ORDER: &[&str] = &[
    "application",
    "queue",
    "database",
    "cache",
    "infrastructure",
    "runtime",
];

// ---------------------------------------------------------------------------
// Stack Detection
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct StackResponse {
    tiers: Vec<DetectedTier>,
}

#[derive(Serialize)]
struct DetectedTier {
    tier: String,
    technology: String,
    golden_signals: Vec<GoldenSignal>,
}

#[derive(Serialize)]
struct GoldenSignal {
    label: String,
    promql: String,
    unit: String,
}

async fn get_stack(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
) -> Result<Json<StackResponse>> {
    let clickhouse_url =
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());

    let cutoff_ms = (chrono::Utc::now() - chrono::Duration::hours(24)).timestamp_millis();

    let sql = format!(
        r#"SELECT DISTINCT metric_name
FROM reiver.time_series_v1
WHERE project_id = '{}'
  AND unix_milli >= {}
FORMAT JSONEachRow"#,
        project_id, cutoff_ms
    );

    let client = &state.http_client;
    let response = client
        .post(&clickhouse_url)
        .body(sql)
        .send()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse request failed: {}", e)))?;

    if !response.status().is_success() {
        let error_text = response.text().await.unwrap_or_default();
        return Err(AppError::Internal(anyhow::anyhow!(
            "ClickHouse query failed: {}",
            error_text
        )));
    }

    let rows = crate::ch_stream::stream_json_lines(response)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to parse response: {}", e)))?;

    let metric_names: Vec<String> = rows
        .iter()
        .filter_map(|r| r["metric_name"].as_str().map(|s| s.to_string()))
        .collect();

    let mut detected: Vec<DetectedTier> = Vec::new();

    for tech in TECH_REGISTRY {
        let matches = metric_names.iter().any(|m| m.starts_with(tech.prefix));
        if matches {
            detected.push(DetectedTier {
                tier: tech.tier.to_string(),
                technology: tech.name.to_string(),
                golden_signals: tech
                    .golden_signals
                    .iter()
                    .map(|(label, promql, unit)| GoldenSignal {
                        label: label.to_string(),
                        promql: promql.to_string(),
                        unit: unit.to_string(),
                    })
                    .collect(),
            });
        }
    }

    detected.sort_by_key(|d| {
        TIER_ORDER
            .iter()
            .position(|&t| t == d.tier)
            .unwrap_or(usize::MAX)
    });

    Ok(Json(StackResponse { tiers: detected }))
}

// ---------------------------------------------------------------------------
// Correlation Context
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ContextRequest {
    start_ms: i64,
    end_ms: i64,
}

#[derive(Serialize)]
struct ContextResponse {
    traces: Vec<ContextTrace>,
    logs: Vec<ContextLog>,
}

#[derive(Serialize)]
struct ContextTrace {
    trace_id: String,
    service: String,
    operation: String,
    duration_ms: f64,
    status: String,
    timestamp: String,
}

#[derive(Serialize)]
struct ContextLog {
    timestamp: String,
    service: String,
    level: String,
    message: String,
    trace_id: Option<String>,
}

async fn get_context(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Json(payload): Json<ContextRequest>,
) -> Result<Json<ContextResponse>> {
    if payload.end_ms <= payload.start_ms {
        return Err(AppError::Validation(
            "end_ms must be greater than start_ms".to_string(),
        ));
    }

    let clickhouse_url =
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());

    let client = &state.http_client;

    let traces = fetch_context_traces(client, &clickhouse_url, &project_id, &payload).await?;
    let logs = fetch_context_logs(client, &clickhouse_url, &project_id, &payload).await?;

    Ok(Json(ContextResponse { traces, logs }))
}

async fn fetch_context_traces(
    client: &reqwest::Client,
    clickhouse_url: &str,
    project_id: &Uuid,
    payload: &ContextRequest,
) -> Result<Vec<ContextTrace>> {
    let sql = format!(
        r#"SELECT
    trace_id,
    service_name,
    span_name,
    duration_ns / 1000000.0 AS duration_ms,
    status_code,
    toISO8601(start_time) AS timestamp
FROM reiver.spans
WHERE project_id = '{project_id}'
  AND start_time >= fromUnixTimestamp64Milli({start_ms})
  AND start_time < fromUnixTimestamp64Milli({end_ms})
  AND parent_span_id = ''
  AND (status_code = 'ERROR' OR duration_ns > (
    SELECT quantile(0.95)(duration_ns)
    FROM reiver.spans
    WHERE project_id = '{project_id}'
      AND start_time >= fromUnixTimestamp64Milli({start_ms})
      AND start_time < fromUnixTimestamp64Milli({end_ms})
      AND parent_span_id = ''
  ))
ORDER BY duration_ns DESC
LIMIT 50
FORMAT JSONEachRow"#,
        project_id = project_id,
        start_ms = payload.start_ms,
        end_ms = payload.end_ms,
    );

    let response = client
        .post(clickhouse_url)
        .body(sql)
        .send()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse trace query failed: {}", e)))?;

    if !response.status().is_success() {
        let error_text = response.text().await.unwrap_or_default();
        tracing::warn!("Context traces query failed: {}", error_text);
        return Ok(vec![]);
    }

    let rows = crate::ch_stream::stream_json_lines(response)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to parse traces: {}", e)))?;

    let traces = rows
        .iter()
        .map(|r| ContextTrace {
            trace_id: r["trace_id"].as_str().unwrap_or("").to_string(),
            service: r["service_name"].as_str().unwrap_or("").to_string(),
            operation: r["span_name"].as_str().unwrap_or("").to_string(),
            duration_ms: r["duration_ms"].as_f64().unwrap_or(0.0),
            status: r["status_code"].as_str().unwrap_or("OK").to_string(),
            timestamp: r["timestamp"].as_str().unwrap_or("").to_string(),
        })
        .collect();

    Ok(traces)
}

async fn fetch_context_logs(
    client: &reqwest::Client,
    clickhouse_url: &str,
    project_id: &Uuid,
    payload: &ContextRequest,
) -> Result<Vec<ContextLog>> {
    let sql = format!(
        r#"SELECT
    toISO8601(timestamp) AS timestamp,
    service_name,
    severity_text,
    substring(body, 1, 500) AS message,
    trace_id
FROM reiver.logs
WHERE project_id = '{project_id}'
  AND timestamp >= fromUnixTimestamp64Milli({start_ms})
  AND timestamp < fromUnixTimestamp64Milli({end_ms})
  AND severity_text IN ('ERROR', 'WARN', 'error', 'warn', 'Error', 'Warning')
ORDER BY timestamp DESC
LIMIT 100
FORMAT JSONEachRow"#,
        project_id = project_id,
        start_ms = payload.start_ms,
        end_ms = payload.end_ms,
    );

    let response = client
        .post(clickhouse_url)
        .body(sql)
        .send()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse logs query failed: {}", e)))?;

    if !response.status().is_success() {
        let error_text = response.text().await.unwrap_or_default();
        tracing::warn!("Context logs query failed: {}", error_text);
        return Ok(vec![]);
    }

    let rows = crate::ch_stream::stream_json_lines(response)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to parse logs: {}", e)))?;

    let logs = rows
        .iter()
        .map(|r| ContextLog {
            timestamp: r["timestamp"].as_str().unwrap_or("").to_string(),
            service: r["service_name"].as_str().unwrap_or("").to_string(),
            level: r["severity_text"].as_str().unwrap_or("").to_string(),
            message: r["message"].as_str().unwrap_or("").to_string(),
            trace_id: r["trace_id"].as_str().map(|s| s.to_string()).filter(|s| !s.is_empty()),
        })
        .collect();

    Ok(logs)
}
