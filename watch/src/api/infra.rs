//! Kubernetes Infrastructure API endpoints
//!
//! Queries pre-aggregated 1-minute metrics from `k8s_infra_1m` (materialized
//! view on `samples_v1`) to power the Infrastructure Monitoring dashboard.

use axum::{
    extract::{Path, Query, State},
    response::Json,
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use crate::app_state::WatchState;
use crate::error::{AppError, Result};
use crate::utils::escape_clickhouse_string;

pub fn create_infra_router() -> Router<Arc<WatchState>> {
    Router::new()
        .route("/summary", get(get_infra_summary))
        .route("/pods", get(get_infra_pods))
        .route("/pods/{namespace}/{pod_name}", get(get_pod_detail))
        .route("/nodes", get(get_infra_nodes))
        .route("/nodes/{node_name}", get(get_node_detail))
        .route("/deployments", get(get_infra_deployments))
        .route("/services", get(get_infra_services))
}

#[derive(Debug, Deserialize)]
struct InfraQuery {
    time_range: Option<String>,
    cluster: Option<String>,
    namespace: Option<String>,
}

fn time_range_to_ms(time_range: &str) -> i64 {
    let seconds: i64 = match time_range {
        "live" => 120,
        "15m" => 900,
        "1h" => 3600,
        "6h" => 21600,
        "24h" => 86400,
        _ => 3600,
    };
    seconds * 1000
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn build_filters(params: &InfraQuery) -> String {
    let mut filters = String::new();
    if let Some(ref cluster) = params.cluster {
        if !cluster.is_empty() {
            filters.push_str(&format!(
                " AND cluster = '{}'",
                escape_clickhouse_string(cluster)
            ));
        }
    }
    if let Some(ref ns) = params.namespace {
        if !ns.is_empty() {
            filters.push_str(&format!(
                " AND namespace = '{}'",
                escape_clickhouse_string(ns)
            ));
        }
    }
    filters
}

async fn ch_query(state: &Arc<WatchState>, sql: &str) -> Result<Vec<serde_json::Value>> {
    let clickhouse_url =
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());
    let client = reqwest::Client::new();

    let resp = client
        .post(&clickhouse_url)
        .query(&[("default_format", "JSONEachRow")])
        .body(sql.to_string())
        .send()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse request failed: {}", e)))?;

    if !resp.status().is_success() {
        let err = resp.text().await.unwrap_or_default();
        tracing::error!("[INFRA] ClickHouse query failed: {}", err);
        return Err(AppError::Internal(anyhow::anyhow!(
            "ClickHouse query failed: {}",
            err
        )));
    }

    crate::ch_stream::stream_json_lines(resp)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse stream error: {}", e)))
}

// ============================================================================
// GET /infra/summary
// ============================================================================

#[derive(Serialize)]
struct InfraSummary {
    nodes: u64,
    #[serde(rename = "totalPods")]
    total_pods: u64,
    #[serde(rename = "runningPods")]
    running_pods: u64,
    deployments: u64,
    alerts: u64,
    #[serde(rename = "cpuUsage")]
    cpu_usage: f64,
    #[serde(rename = "memoryUsage")]
    memory_usage: f64,
}

#[derive(Serialize)]
struct InfraSummaryResponse {
    summary: InfraSummary,
    clusters: Vec<String>,
    namespaces: Vec<String>,
}

async fn get_infra_summary(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<InfraQuery>,
) -> Result<Json<InfraSummaryResponse>> {
    let time_range = params.time_range.as_deref().unwrap_or("1h");
    let range_ms = time_range_to_ms(time_range);
    let start_ms = now_ms() - range_ms;
    let pid = escape_clickhouse_string(&project_id.to_string());
    let extra = build_filters(&params);

    let summary_sql = format!(
        "SELECT \
          (SELECT uniq(node_name) \
           FROM reiver.k8s_infra_1m \
           WHERE project_id = '{pid}' AND unix_milli >= {start_ms} \
           AND metric_name = 'k8s.node.cpu.usage'{extra}) AS node_count, \
          \
          (SELECT uniq(pod_name) \
           FROM reiver.k8s_infra_1m \
           WHERE project_id = '{pid}' AND unix_milli >= {start_ms} \
           AND metric_name = 'k8s.pod.cpu.usage' \
           AND pod_name != ''{extra}) AS pod_count, \
          \
          (SELECT uniq(deployment_name) \
           FROM reiver.k8s_infra_1m \
           WHERE project_id = '{pid}' AND unix_milli >= {start_ms} \
           AND metric_name = 'k8s.deployment.desired' \
           AND deployment_name != ''{extra}) AS deploy_count, \
          \
          (SELECT groupUniqArray(cluster) \
           FROM reiver.k8s_infra_1m \
           WHERE project_id = '{pid}' AND unix_milli >= {start_ms} \
           AND metric_name = 'k8s.node.cpu.usage' \
           AND cluster != ''{extra}) AS clusters, \
          \
          (SELECT groupUniqArray(namespace) \
           FROM reiver.k8s_infra_1m \
           WHERE project_id = '{pid}' AND unix_milli >= {start_ms} \
           AND metric_name = 'k8s.pod.cpu.usage' \
           AND namespace != ''{extra}) AS namespaces, \
          \
          (SELECT if(cores > 0, (usage / cores) * 100, 0) FROM ( \
             SELECT \
               (SELECT avg(node_usage) FROM ( \
                  SELECT sum(value_sum) / nullIf(sum(value_count), 0) AS node_usage \
                  FROM reiver.k8s_infra_1m \
                  WHERE project_id = '{pid}' AND unix_milli >= {start_ms} \
                  AND metric_name = 'k8s.node.cpu.usage'{extra} \
                  GROUP BY node_name)) AS usage, \
               (SELECT uniq(cpu_id) \
                  FROM reiver.k8s_infra_1m \
                  WHERE project_id = '{pid}' AND unix_milli >= {start_ms} \
                  AND metric_name = 'system.cpu.time' AND cpu_id != '') AS cores \
           )) AS cpu_pct, \
          \
          (SELECT if(s > 0, (u / s) * 100, 0) FROM ( \
             SELECT sum(mem_u) AS u, sum(mem_u) + sum(mem_a) AS s FROM ( \
               SELECT \
                 node_name AS node, \
                 sumIf(value_sum, metric_name = 'k8s.node.memory.working_set') \
                   / nullIf(sumIf(value_count, metric_name = 'k8s.node.memory.working_set'), 0) AS mem_u, \
                 sumIf(value_sum, metric_name = 'k8s.node.memory.available') \
                   / nullIf(sumIf(value_count, metric_name = 'k8s.node.memory.available'), 0) AS mem_a \
               FROM reiver.k8s_infra_1m \
               WHERE project_id = '{pid}' AND unix_milli >= {start_ms} \
               AND metric_name IN ('k8s.node.memory.working_set', 'k8s.node.memory.available'){extra} \
               GROUP BY node HAVING node != '' \
             ) \
           )) AS mem_pct"
    );

    let summary_row = ch_query(&state, &summary_sql)
        .await
        .ok()
        .and_then(|rows| rows.into_iter().next())
        .unwrap_or_default();

    let extract_u64 = |v: &serde_json::Value, key: &str| -> u64 {
        v.get(key)
            .and_then(|n| n.as_u64().or_else(|| n.as_f64().map(|f| f as u64)))
            .unwrap_or(0)
    };

    let nodes = extract_u64(&summary_row, "node_count");
    let total_pods = extract_u64(&summary_row, "pod_count");
    let deployments = extract_u64(&summary_row, "deploy_count");
    let cpu_usage = summary_row
        .get("cpu_pct")
        .and_then(|n| n.as_f64())
        .unwrap_or(0.0);
    let memory_usage = summary_row
        .get("mem_pct")
        .and_then(|n| n.as_f64())
        .unwrap_or(0.0);

    let clusters: Vec<String> = summary_row
        .get("clusters")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|s| s.as_str().map(String::from))
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let namespaces: Vec<String> = summary_row
        .get("namespaces")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|s| s.as_str().map(String::from))
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();

    Ok(Json(InfraSummaryResponse {
        summary: InfraSummary {
            nodes,
            total_pods,
            running_pods: total_pods,
            deployments,
            alerts: 0,
            cpu_usage,
            memory_usage,
        },
        clusters,
        namespaces,
    }))
}

// ============================================================================
// GET /infra/pods
// ============================================================================

#[derive(Serialize)]
struct PodInfo {
    name: String,
    namespace: String,
    status: String,
    #[serde(rename = "readyContainers")]
    ready_containers: u64,
    #[serde(rename = "totalContainers")]
    total_containers: u64,
    restarts: u64,
    #[serde(rename = "cpuPercent")]
    cpu_percent: f64,
    #[serde(rename = "memoryPercent")]
    memory_percent: f64,
    #[serde(rename = "createdAt")]
    created_at: String,
}

async fn get_infra_pods(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<InfraQuery>,
) -> Result<Json<serde_json::Value>> {
    let time_range = params.time_range.as_deref().unwrap_or("1h");
    let range_ms = time_range_to_ms(time_range);
    let start_ms = now_ms() - range_ms;
    let pid = escape_clickhouse_string(&project_id.to_string());
    let extra = build_filters(&params);

    let sql = format!(
        r#"SELECT
            pod_name AS name,
            namespace,
            sumIf(value_sum, metric_name IN ('k8s.pod.cpu.usage', 'k8s.pod.cpu.utilization'))
              / nullIf(sumIf(value_count, metric_name IN ('k8s.pod.cpu.usage', 'k8s.pod.cpu.utilization')), 0) * 100 AS cpuPercent,
            sumIf(value_sum, metric_name IN ('k8s.pod.memory.usage', 'k8s.pod.memory.working_set'))
              / nullIf(sumIf(value_count, metric_name IN ('k8s.pod.memory.usage', 'k8s.pod.memory.working_set')), 0) AS memoryBytes,
            maxIf(value_max, metric_name = 'k8s.container.restarts') AS restarts,
            uniqExact(container_name) AS containerCount,
            min(unix_milli) AS first_seen
        FROM reiver.k8s_infra_1m
        WHERE project_id = '{pid}'
        AND unix_milli >= {start_ms}
        AND pod_name != ''
        AND metric_name IN (
            'k8s.pod.cpu.usage', 'k8s.pod.cpu.utilization',
            'k8s.pod.memory.usage', 'k8s.pod.memory.working_set',
            'k8s.container.restarts',
            'container.cpu.usage', 'container.memory.usage'
        ){extra}
        GROUP BY name, namespace
        ORDER BY name"#
    );

    let node_mem_sql = format!(
        r#"SELECT max(value_max) AS total
        FROM reiver.k8s_infra_1m
        WHERE project_id = '{pid}'
        AND unix_milli >= {start_ms}
        AND metric_name IN ('k8s.node.memory.usage', 'system.memory.usage')"#
    );

    let (pods_result, mem_result) = tokio::join!(
        ch_query(&state, &sql),
        ch_query(&state, &node_mem_sql),
    );
    let rows = pods_result?;
    let node_mem_total: f64 = mem_result
        .ok()
        .and_then(|rows| rows.into_iter().next())
        .and_then(|v| v.get("total").and_then(|n| n.as_f64()))
        .unwrap_or(1.0);

    let pods: Vec<PodInfo> = rows
        .into_iter()
        .map(|row| {
            let first_seen_ms = row.get("first_seen").and_then(|v| v.as_i64()).unwrap_or(0);
            let created_at = if first_seen_ms > 0 {
                chrono::DateTime::from_timestamp_millis(first_seen_ms)
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_default()
            } else {
                String::new()
            };

            let cpu_cores = row
                .get("cpuPercent")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let memory_bytes = row
                .get("memoryBytes")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let memory_pct = if node_mem_total > 0.0 {
                (memory_bytes / node_mem_total) * 100.0
            } else {
                0.0
            };
            let containers = row
                .get("containerCount")
                .and_then(|v| v.as_u64())
                .unwrap_or(1)
                .max(1);

            PodInfo {
                name: row
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                namespace: row
                    .get("namespace")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                status: "Running".to_string(),
                ready_containers: containers,
                total_containers: containers,
                restarts: row
                    .get("restarts")
                    .and_then(|v| v.as_u64().or_else(|| v.as_f64().map(|f| f as u64)))
                    .unwrap_or(0),
                cpu_percent: (cpu_cores * 100.0).round() / 100.0,
                memory_percent: (memory_pct * 100.0).round() / 100.0,
                created_at,
            }
        })
        .collect();

    Ok(Json(serde_json::json!({ "pods": pods })))
}

// ============================================================================
// GET /infra/nodes
// ============================================================================

#[derive(Serialize)]
struct NodeInfo {
    name: String,
    status: String,
    #[serde(rename = "cpuCores")]
    cpu_cores: f64,
    #[serde(rename = "cpuUsed")]
    cpu_used: f64,
    #[serde(rename = "cpuTotal")]
    cpu_total: f64,
    #[serde(rename = "cpuPercent")]
    cpu_percent: f64,
    #[serde(rename = "memoryUsed")]
    memory_used: f64,
    #[serde(rename = "memoryTotal")]
    memory_total: f64,
    #[serde(rename = "memoryPercent")]
    memory_percent: f64,
    #[serde(rename = "diskUsed")]
    disk_used: f64,
    #[serde(rename = "diskTotal")]
    disk_total: f64,
    #[serde(rename = "diskPercent")]
    disk_percent: f64,
    #[serde(rename = "podCount")]
    pod_count: u64,
    #[serde(rename = "kubeletVersion")]
    kubelet_version: String,
}

async fn get_infra_nodes(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<InfraQuery>,
) -> Result<Json<serde_json::Value>> {
    let time_range = params.time_range.as_deref().unwrap_or("1h");
    let range_ms = time_range_to_ms(time_range);
    let start_ms = now_ms() - range_ms;
    let pid = escape_clickhouse_string(&project_id.to_string());
    let extra = build_filters(&params);

    let sql = format!(
        r#"SELECT
            node_name AS name,
            sumIf(value_sum, metric_name = 'k8s.node.cpu.usage')
              / nullIf(sumIf(value_count, metric_name = 'k8s.node.cpu.usage'), 0) AS cpuUsageCores,
            maxIf(value_max, metric_name = 'k8s.node.memory.usage'
              OR (metric_name = 'system.memory.usage' AND memory_state = 'used')) AS memoryUsed,
            maxIf(value_max, metric_name = 'k8s.node.memory.available') AS memoryAvail,
            maxIf(value_max, metric_name = 'k8s.node.memory.working_set') AS memoryWorkingSet,
            maxIf(value_max, metric_name IN ('k8s.node.filesystem.capacity', 'system.filesystem.usage_total')) AS diskTotal,
            maxIf(value_max, metric_name IN ('k8s.node.filesystem.usage', 'system.filesystem.usage_used')) AS diskUsed
        FROM reiver.k8s_infra_1m
        WHERE project_id = '{pid}'
        AND unix_milli >= {start_ms}
        AND node_name != ''
        AND metric_name IN (
            'k8s.node.cpu.usage',
            'k8s.node.memory.usage', 'k8s.node.memory.available',
            'k8s.node.memory.working_set',
            'k8s.node.filesystem.capacity', 'k8s.node.filesystem.usage',
            'system.memory.usage',
            'system.filesystem.usage_total', 'system.filesystem.usage_used'
        ){extra}
        GROUP BY name
        ORDER BY name"#
    );

    let cores_sql = format!(
        r#"SELECT uniqExact(cpu_id) AS cpuCores
        FROM reiver.k8s_infra_1m
        WHERE project_id = '{pid}'
        AND unix_milli >= {start_ms}
        AND metric_name = 'system.cpu.time'
        AND cpu_id != ''"#
    );

    let pod_sql = format!(
        r#"SELECT
            node_name AS node,
            uniqExact(pod_name) AS pod_count
        FROM reiver.k8s_infra_1m
        WHERE project_id = '{pid}'
        AND unix_milli >= {start_ms}
        AND metric_name = 'k8s.pod.cpu.usage'
        AND node_name != ''
        AND pod_name != ''{extra}
        GROUP BY node"#
    );

    let (node_r, pod_r, cores_r) = tokio::join!(
        ch_query(&state, &sql),
        ch_query(&state, &pod_sql),
        ch_query(&state, &cores_sql),
    );

    let node_rows = node_r?;
    let pod_rows = pod_r.unwrap_or_default();
    let cores_rows = cores_r.unwrap_or_default();
    let global_cpu_cores = cores_rows
        .first()
        .and_then(|r| r.get("cpuCores"))
        .and_then(|v| v.as_f64())
        .unwrap_or(1.0);

    let pod_counts: HashMap<String, u64> = pod_rows
        .into_iter()
        .filter_map(|v| {
            let node = v.get("node")?.as_str()?.to_string();
            let count = v.get("pod_count")?.as_u64()?;
            Some((node, count))
        })
        .collect();

    let nodes: Vec<NodeInfo> = node_rows
        .into_iter()
        .map(|row| {
            let name = row
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let cpu_usage_cores = row
                .get("cpuUsageCores")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let cpu_cores = global_cpu_cores;
            let memory_used = row
                .get("memoryUsed")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let memory_avail = row
                .get("memoryAvail")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let memory_working_set = row
                .get("memoryWorkingSet")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let disk_total = row.get("diskTotal").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let disk_used = row.get("diskUsed").and_then(|v| v.as_f64()).unwrap_or(0.0);

            let cpu_total = if cpu_cores > 0.0 { cpu_cores } else { 1.0 };
            let cpu_used = cpu_usage_cores;
            let cpu_percent = if cpu_total > 0.0 {
                (cpu_used / cpu_total) * 100.0
            } else {
                0.0
            };

            let effective_used = if memory_working_set > 0.0 {
                memory_working_set
            } else {
                memory_used
            };
            let memory_total = if memory_avail > 0.0 && effective_used > 0.0 {
                effective_used + memory_avail
            } else {
                effective_used
            };
            let memory_percent = if memory_total > 0.0 {
                (effective_used / memory_total) * 100.0
            } else {
                0.0
            };

            let disk_percent = if disk_total > 0.0 {
                (disk_used / disk_total) * 100.0
            } else {
                0.0
            };

            NodeInfo {
                pod_count: pod_counts.get(&name).copied().unwrap_or(0),
                name,
                status: "Ready".to_string(),
                cpu_cores: cpu_total,
                cpu_used: (cpu_used * 100.0).round() / 100.0,
                cpu_total,
                cpu_percent: (cpu_percent * 100.0).round() / 100.0,
                memory_used: effective_used,
                memory_total,
                memory_percent: (memory_percent * 100.0).round() / 100.0,
                disk_used,
                disk_total,
                disk_percent: (disk_percent * 100.0).round() / 100.0,
                kubelet_version: String::new(),
            }
        })
        .collect();

    Ok(Json(serde_json::json!({ "nodes": nodes })))
}

// ============================================================================
// GET /infra/deployments
// ============================================================================

#[derive(Serialize)]
struct DeploymentInfo {
    name: String,
    namespace: String,
    ready: u64,
    desired: u64,
    #[serde(rename = "upToDate")]
    up_to_date: u64,
    available: u64,
    #[serde(rename = "createdAt")]
    created_at: String,
}

async fn get_infra_deployments(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<InfraQuery>,
) -> Result<Json<serde_json::Value>> {
    let time_range = params.time_range.as_deref().unwrap_or("1h");
    let range_ms = time_range_to_ms(time_range);
    let start_ms = now_ms() - range_ms;
    let pid = escape_clickhouse_string(&project_id.to_string());
    let extra = build_filters(&params);

    let sql = format!(
        r#"SELECT
            arrayStringConcat(
                arraySlice(
                    splitByChar('-', pod_name),
                    1,
                    greatest(toInt32(length(splitByChar('-', pod_name))) - 2, 1)
                ), '-'
            ) AS name,
            namespace,
            uniqExact(pod_name) AS pod_count,
            min(first_seen) AS first_seen
        FROM (
            SELECT
                pod_name,
                namespace,
                min(unix_milli) AS first_seen
            FROM reiver.k8s_infra_1m
            WHERE project_id = '{pid}'
            AND unix_milli >= {start_ms}
            AND pod_name != ''
            AND metric_name = 'k8s.pod.cpu.usage'{extra}
            GROUP BY pod_name, namespace
        )
        WHERE length(splitByChar('-', pod_name)) >= 3
        GROUP BY name, namespace
        ORDER BY name"#
    );

    let rows = ch_query(&state, &sql).await?;

    let deployments: Vec<DeploymentInfo> = rows
        .into_iter()
        .map(|row| {
            let pod_count = row.get("pod_count").and_then(|v| v.as_u64()).unwrap_or(0);

            let first_seen_ms = row.get("first_seen").and_then(|v| v.as_i64()).unwrap_or(0);
            let created_at = chrono::DateTime::from_timestamp_millis(first_seen_ms)
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_default();

            DeploymentInfo {
                name: row
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                namespace: row
                    .get("namespace")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                ready: pod_count,
                desired: pod_count,
                up_to_date: pod_count,
                available: pod_count,
                created_at,
            }
        })
        .collect();

    Ok(Json(serde_json::json!({ "deployments": deployments })))
}

// ============================================================================
// GET /infra/services
// ============================================================================

#[derive(Serialize)]
struct ServiceInfo {
    name: String,
    namespace: String,
    r#type: String,
    #[serde(rename = "clusterIP")]
    cluster_ip: String,
    ports: String,
}

async fn get_infra_services(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<InfraQuery>,
) -> Result<Json<serde_json::Value>> {
    let time_range = params.time_range.as_deref().unwrap_or("1h");
    let range_ms = time_range_to_ms(time_range);
    let start_ms = now_ms() - range_ms;
    let pid = escape_clickhouse_string(&project_id.to_string());
    let extra = build_filters(&params);

    let sql = format!(
        r#"SELECT name, namespace, workload_type FROM (
            SELECT
                arrayStringConcat(
                    arraySlice(
                        splitByChar('-', pod_name),
                        1,
                        greatest(toInt32(length(splitByChar('-', pod_name))) - 2, 1)
                    ), '-'
                ) AS name,
                namespace,
                'Deployment' AS workload_type
            FROM (
                SELECT DISTINCT
                    pod_name,
                    namespace
                FROM reiver.k8s_infra_1m
                WHERE project_id = '{pid}' AND unix_milli >= {start_ms}
                AND pod_name != ''
                AND metric_name = 'k8s.pod.cpu.usage'{extra}
            )
            WHERE length(splitByChar('-', pod_name)) >= 3
            GROUP BY name, namespace
        )
        WHERE name != ''
        ORDER BY workload_type, name"#
    );

    let rows = ch_query(&state, &sql).await?;

    let services: Vec<ServiceInfo> = rows
        .into_iter()
        .map(|row| ServiceInfo {
            name: row
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            namespace: row
                .get("namespace")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            r#type: row
                .get("workload_type")
                .and_then(|v| v.as_str())
                .unwrap_or("Deployment")
                .to_string(),
            cluster_ip: String::new(),
            ports: String::new(),
        })
        .collect();

    Ok(Json(serde_json::json!({ "services": services })))
}

// ============================================================================
// GET /infra/pods/:namespace/:pod_name
// ============================================================================

async fn get_pod_detail(
    State(state): State<Arc<WatchState>>,
    Path((project_id, namespace, pod_name)): Path<(Uuid, String, String)>,
    Query(params): Query<InfraQuery>,
) -> Result<Json<serde_json::Value>> {
    let time_range = params.time_range.as_deref().unwrap_or("1h");
    let range_ms = time_range_to_ms(time_range);
    let start_ms = now_ms() - range_ms;
    let pid = escape_clickhouse_string(&project_id.to_string());
    let ns = escape_clickhouse_string(&namespace);
    let pod = escape_clickhouse_string(&pod_name);

    // "All metrics" query stays on samples_v1 — it fetches every metric type
    // for the pod, not just the 18 captured by the infra MV.
    let sql = format!(
        r#"SELECT
            metric_name,
            avg(value) AS avg_val,
            max(value) AS max_val,
            min(value) AS min_val
        FROM reiver.samples_v1
        WHERE project_id = '{pid}'
        AND unix_milli >= {start_ms}
        AND resource_attributes['k8s.pod.name'] = '{pod}'
        AND resource_attributes['k8s.namespace.name'] = '{ns}'
        GROUP BY metric_name
        ORDER BY metric_name"#
    );

    let ts_sql = format!(
        r#"SELECT
            fromUnixTimestamp64Milli(unix_milli) AS ts,
            sumIf(value_sum, metric_name IN ('k8s.pod.cpu.usage', 'k8s.pod.cpu.utilization'))
              / nullIf(sumIf(value_count, metric_name IN ('k8s.pod.cpu.usage', 'k8s.pod.cpu.utilization')), 0) AS cpu,
            sumIf(value_sum, metric_name IN ('k8s.pod.memory.usage', 'k8s.pod.memory.working_set'))
              / nullIf(sumIf(value_count, metric_name IN ('k8s.pod.memory.usage', 'k8s.pod.memory.working_set')), 0) AS memory
        FROM reiver.k8s_infra_1m
        WHERE project_id = '{pid}'
        AND unix_milli >= {start_ms}
        AND pod_name = '{pod}'
        AND namespace = '{ns}'
        AND metric_name IN (
            'k8s.pod.cpu.usage', 'k8s.pod.cpu.utilization',
            'k8s.pod.memory.usage', 'k8s.pod.memory.working_set'
        )
        GROUP BY unix_milli
        ORDER BY unix_milli"#
    );

    let containers_sql = format!(
        r#"SELECT
            container_name AS name,
            sumIf(value_sum, metric_name IN ('container.cpu.usage', 'k8s.pod.cpu.usage'))
              / nullIf(sumIf(value_count, metric_name IN ('container.cpu.usage', 'k8s.pod.cpu.usage')), 0) AS cpu,
            sumIf(value_sum, metric_name IN ('container.memory.usage', 'k8s.pod.memory.usage'))
              / nullIf(sumIf(value_count, metric_name IN ('container.memory.usage', 'k8s.pod.memory.usage')), 0) AS memory,
            maxIf(value_max, metric_name = 'k8s.container.restarts') AS restarts
        FROM reiver.k8s_infra_1m
        WHERE project_id = '{pid}'
        AND unix_milli >= {start_ms}
        AND pod_name = '{pod}'
        AND namespace = '{ns}'
        AND container_name != ''
        GROUP BY name
        ORDER BY name"#
    );

    let (metrics_r, ts_r, containers_r) = tokio::join!(
        ch_query(&state, &sql),
        ch_query(&state, &ts_sql),
        ch_query(&state, &containers_sql),
    );

    let metrics = metrics_r?;
    let timeseries = ts_r.unwrap_or_default();
    let containers = containers_r.unwrap_or_default();

    Ok(Json(serde_json::json!({
        "pod": {
            "name": pod_name,
            "namespace": namespace,
            "status": "Running",
        },
        "metrics": metrics,
        "timeseries": timeseries.into_iter().map(|row| {
            serde_json::json!({
                "timestamp": row.get("ts").and_then(|v| v.as_str()).unwrap_or(""),
                "cpu": row.get("cpu").and_then(|v| v.as_f64()).unwrap_or(0.0),
                "memory": row.get("memory").and_then(|v| v.as_f64()).unwrap_or(0.0),
            })
        }).collect::<Vec<_>>(),
        "containers": containers.into_iter().map(|row| {
            serde_json::json!({
                "name": row.get("name").and_then(|v| v.as_str()).unwrap_or(""),
                "cpu": row.get("cpu").and_then(|v| v.as_f64()).unwrap_or(0.0),
                "memory": row.get("memory").and_then(|v| v.as_f64()).unwrap_or(0.0),
                "restarts": row.get("restarts").and_then(|v| v.as_u64()).unwrap_or(0),
            })
        }).collect::<Vec<_>>(),
    })))
}

// ============================================================================
// GET /infra/nodes/:node_name
// ============================================================================

async fn get_node_detail(
    State(state): State<Arc<WatchState>>,
    Path((project_id, node_name)): Path<(Uuid, String)>,
    Query(params): Query<InfraQuery>,
) -> Result<Json<serde_json::Value>> {
    let time_range = params.time_range.as_deref().unwrap_or("1h");
    let range_ms = time_range_to_ms(time_range);
    let start_ms = now_ms() - range_ms;
    let pid = escape_clickhouse_string(&project_id.to_string());
    let node = escape_clickhouse_string(&node_name);

    let ts_sql = format!(
        r#"SELECT
            fromUnixTimestamp64Milli(unix_milli) AS ts,
            sumIf(value_sum, metric_name = 'k8s.node.cpu.usage')
              / nullIf(sumIf(value_count, metric_name = 'k8s.node.cpu.usage'), 0) AS cpu,
            sumIf(value_sum, metric_name = 'k8s.node.memory.usage'
              OR (metric_name = 'system.memory.usage' AND memory_state = 'used'))
              / nullIf(sumIf(value_count, metric_name = 'k8s.node.memory.usage'
              OR (metric_name = 'system.memory.usage' AND memory_state = 'used')), 0) AS memory,
            sumIf(value_sum, metric_name IN ('k8s.node.filesystem.usage', 'system.filesystem.usage_used'))
              / nullIf(sumIf(value_count, metric_name IN ('k8s.node.filesystem.usage', 'system.filesystem.usage_used')), 0) AS disk
        FROM reiver.k8s_infra_1m
        WHERE project_id = '{pid}'
        AND unix_milli >= {start_ms}
        AND node_name = '{node}'
        AND metric_name IN (
            'k8s.node.cpu.usage',
            'k8s.node.memory.usage', 'k8s.node.memory.working_set',
            'k8s.node.filesystem.usage',
            'system.memory.usage',
            'system.filesystem.usage_used'
        )
        GROUP BY unix_milli
        ORDER BY unix_milli"#
    );

    let capacity_sql = format!(
        r#"SELECT
            (SELECT uniqExact(cpu_id)
               FROM reiver.k8s_infra_1m
               WHERE project_id = '{pid}' AND unix_milli >= {start_ms}
               AND metric_name = 'system.cpu.time' AND cpu_id != '') AS cpuCores,
            maxIf(value_max, metric_name = 'k8s.node.memory.usage'
              OR (metric_name = 'system.memory.usage' AND memory_state = 'used')) AS memUsed,
            maxIf(value_max, metric_name = 'k8s.node.memory.available') AS memAvail,
            maxIf(value_max, metric_name IN ('k8s.node.filesystem.capacity', 'system.filesystem.usage_total')) AS diskTotal,
            maxIf(value_max, metric_name IN ('k8s.node.filesystem.usage', 'system.filesystem.usage_used')) AS diskUsed
        FROM reiver.k8s_infra_1m
        WHERE project_id = '{pid}'
        AND unix_milli >= {start_ms}
        AND node_name = '{node}'
        AND metric_name IN (
            'k8s.node.memory.usage', 'k8s.node.memory.available',
            'k8s.node.filesystem.capacity', 'k8s.node.filesystem.usage',
            'system.memory.usage', 'system.filesystem.usage_total', 'system.filesystem.usage_used'
        )"#
    );

    let pods_sql = format!(
        r#"SELECT
            pod_name AS name,
            namespace,
            sumIf(value_sum, metric_name IN ('k8s.pod.cpu.usage', 'k8s.pod.cpu.utilization'))
              / nullIf(sumIf(value_count, metric_name IN ('k8s.pod.cpu.usage', 'k8s.pod.cpu.utilization')), 0) AS cpu,
            sumIf(value_sum, metric_name IN ('k8s.pod.memory.usage', 'k8s.pod.memory.working_set'))
              / nullIf(sumIf(value_count, metric_name IN ('k8s.pod.memory.usage', 'k8s.pod.memory.working_set')), 0) AS memory
        FROM reiver.k8s_infra_1m
        WHERE project_id = '{pid}'
        AND unix_milli >= {start_ms}
        AND node_name = '{node}'
        AND pod_name != ''
        GROUP BY name, namespace
        ORDER BY name"#
    );

    let (ts_r, capacity_r, pods_r) = tokio::join!(
        ch_query(&state, &ts_sql),
        ch_query(&state, &capacity_sql),
        ch_query(&state, &pods_sql),
    );

    let timeseries = ts_r?;
    let capacity_row = capacity_r.unwrap_or_default().into_iter().next();
    let pods = pods_r.unwrap_or_default();

    let cap = capacity_row.unwrap_or_default();
    let cpu_cores = cap.get("cpuCores").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let mem_used = cap.get("memUsed").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let mem_avail = cap.get("memAvail").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let mem_total = if mem_avail > 0.0 && mem_used > 0.0 {
        mem_used + mem_avail
    } else {
        mem_used
    };
    let disk_total = cap.get("diskTotal").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let disk_used = cap.get("diskUsed").and_then(|v| v.as_f64()).unwrap_or(0.0);

    Ok(Json(serde_json::json!({
        "node": {
            "name": node_name,
            "status": "Ready",
        },
        "capacity": {
            "cpuCores": cpu_cores,
            "memoryTotal": mem_total,
            "memoryUsed": mem_used,
            "diskTotal": disk_total,
            "diskUsed": disk_used,
        },
        "timeseries": timeseries.into_iter().map(|row| {
            serde_json::json!({
                "timestamp": row.get("ts").and_then(|v| v.as_str()).unwrap_or(""),
                "cpu": row.get("cpu").and_then(|v| v.as_f64()).unwrap_or(0.0),
                "memory": row.get("memory").and_then(|v| v.as_f64()).unwrap_or(0.0),
                "disk": row.get("disk").and_then(|v| v.as_f64()).unwrap_or(0.0),
            })
        }).collect::<Vec<_>>(),
        "pods": pods.into_iter().map(|row| {
            serde_json::json!({
                "name": row.get("name").and_then(|v| v.as_str()).unwrap_or(""),
                "namespace": row.get("namespace").and_then(|v| v.as_str()).unwrap_or(""),
                "cpu": row.get("cpu").and_then(|v| v.as_f64()).unwrap_or(0.0),
                "memory": row.get("memory").and_then(|v| v.as_f64()).unwrap_or(0.0),
            })
        }).collect::<Vec<_>>(),
    })))
}
