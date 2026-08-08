use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    routing::get,
    Json, Router,
};
use chrono::{NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use uuid::Uuid;

use crate::app_state::HerdState;

pub fn router() -> Router<Arc<HerdState>> {
    Router::new().route("/topology", get(get_topology))
}

fn extract_project_and_org(headers: &HeaderMap) -> Result<(Uuid, Uuid), (StatusCode, String)> {
    let project_id = headers
        .get("x-project-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or((
            StatusCode::UNAUTHORIZED,
            "Missing or invalid X-Project-Id header".into(),
        ))?;
    let org_id = headers
        .get("x-organization-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or((
            StatusCode::UNAUTHORIZED,
            "Missing or invalid X-Organization-Id header".into(),
        ))?;
    Ok((project_id, org_id))
}

#[derive(Debug, Deserialize)]
pub struct TopologyQuery {
    pub date: Option<String>,
}

// -- Postgres row types --

#[derive(Debug, sqlx::FromRow)]
struct AgentRow {
    id: Uuid,
    name: String,
}

#[derive(Debug, sqlx::FromRow)]
struct GrantAgentRow {
    target_agent_id: Uuid,
    target_name: String,
    target_org_id: Uuid,
    target_project_id: Uuid,
    granted_agent_id: Uuid,
    granted_name: String,
    granted_org_id: Uuid,
    granted_project_id: Uuid,
    status: String,
}

// -- ClickHouse row type --

#[derive(Debug, clickhouse::Row, serde::Deserialize)]
struct TrafficRow {
    source_agent_id: Uuid,
    target_agent_id: Uuid,
    message_count: u64,
    avg_latency_ms: f64,
    error_count: u64,
    pii_redacted_count: u64,
    injection_flagged_count: u64,
}

// -- Response types --

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopologyResponse {
    pub nodes: Vec<TopologyNode>,
    pub edges: Vec<TopologyEdge>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopologyNode {
    pub id: String,
    pub name: String,
    /// "project" = same project, "org" = same org different project, "external" = different org
    pub kind: &'static str,
    pub grant_status: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopologyEdge {
    pub source: String,
    pub target: String,
    pub grant_status: String,
    pub traffic: Option<TrafficStats>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TrafficStats {
    pub message_count: u64,
    pub avg_latency_ms: f64,
    pub error_count: u64,
    pub pii_redacted_count: u64,
    pub injection_flagged_count: u64,
}

async fn get_topology(
    State(state): State<Arc<HerdState>>,
    headers: HeaderMap,
    Query(query): Query<TopologyQuery>,
) -> Result<Json<TopologyResponse>, (StatusCode, String)> {
    let (project_id, org_id) = extract_project_and_org(&headers)?;

    let date = query
        .date
        .as_deref()
        .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
        .unwrap_or_else(|| Utc::now().date_naive());

    let start = date.and_hms_opt(0, 0, 0).unwrap().and_utc();
    let end = (date + chrono::Duration::days(1))
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc();

    // 1. Fetch all agents in the current project
    let project_agents = sqlx::query_as::<_, AgentRow>(
        "SELECT id, name FROM a2a_agents WHERE project_id = $1 AND enabled = true",
    )
    .bind(project_id)
    .fetch_all(state.db.as_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to fetch project agents: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to fetch topology".into(),
        )
    })?;

    let project_agent_ids: Vec<Uuid> = project_agents.iter().map(|a| a.id).collect();
    let project_agent_set: HashSet<Uuid> = project_agent_ids.iter().copied().collect();

    // 2. Fetch all grants involving any project agent (as target OR source)
    let grant_rows = sqlx::query_as::<_, GrantAgentRow>(
        "SELECT g.target_agent_id, t.name AS target_name,
                t.organization_id AS target_org_id, t.project_id AS target_project_id,
                g.granted_agent_id, ga.name AS granted_name,
                ga.organization_id AS granted_org_id, ga.project_id AS granted_project_id,
                g.status
         FROM a2a_access_grants g
         JOIN a2a_agents t ON t.id = g.target_agent_id
         JOIN a2a_agents ga ON ga.id = g.granted_agent_id
         WHERE g.target_agent_id = ANY($1) OR g.granted_agent_id = ANY($1)",
    )
    .bind(&project_agent_ids)
    .fetch_all(state.db.as_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to fetch grants for topology: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to fetch topology".into(),
        )
    })?;

    // 3. Build nodes and edges
    let mut nodes_map: HashMap<Uuid, TopologyNode> = HashMap::new();
    let mut edge_map: HashMap<(Uuid, Uuid), String> = HashMap::new();

    // Add project agents as nodes
    for a in &project_agents {
        nodes_map.insert(
            a.id,
            TopologyNode {
                id: a.id.to_string(),
                name: a.name.clone(),
                kind: "project",
                grant_status: None,
            },
        );
    }

    // Process grants: add the "other" agent as a node and draw an edge
    for g in &grant_rows {
        let (other_id, other_name, other_org_id, other_project_id) =
            if project_agent_set.contains(&g.target_agent_id) {
                (g.granted_agent_id, &g.granted_name, g.granted_org_id, g.granted_project_id)
            } else {
                (g.target_agent_id, &g.target_name, g.target_org_id, g.target_project_id)
            };

        let kind = if other_project_id == project_id {
            "project"
        } else if other_org_id == org_id {
            "org"
        } else {
            "external"
        };

        nodes_map.entry(other_id).or_insert(TopologyNode {
            id: other_id.to_string(),
            name: other_name.clone(),
            kind,
            grant_status: Some(g.status.clone()),
        });

        // Edge: granted_agent -> target_agent
        let edge_key = (g.granted_agent_id, g.target_agent_id);
        edge_map
            .entry(edge_key)
            .and_modify(|existing| {
                if grant_priority(&g.status) > grant_priority(existing) {
                    *existing = g.status.clone();
                }
            })
            .or_insert_with(|| g.status.clone());
    }

    // 4. ClickHouse traffic overlay for the selected date
    let start_str = start.format("%Y-%m-%d %H:%M:%S").to_string();
    let end_str = end.format("%Y-%m-%d %H:%M:%S").to_string();

    let traffic_rows: Vec<TrafficRow> = state
        .clickhouse
        .query(
            "SELECT
                source_agent_id,
                target_agent_id,
                count() AS message_count,
                avg(latency_ms) AS avg_latency_ms,
                countIf(status_code >= 400) AS error_count,
                countIf(pii_redacted) AS pii_redacted_count,
                countIf(injection_flagged) AS injection_flagged_count
             FROM a2a_request_log
             WHERE (source_org_id = ? OR target_org_id = ?)
               AND timestamp >= ?
               AND timestamp < ?
             GROUP BY source_agent_id, target_agent_id",
        )
        .bind(&org_id)
        .bind(&org_id)
        .bind(start_str.as_str())
        .bind(end_str.as_str())
        .fetch_all()
        .await
        .map_err(|e| {
            tracing::error!("Failed to fetch traffic data: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to fetch topology traffic".into(),
            )
        })?;

    let mut traffic_map: HashMap<(Uuid, Uuid), TrafficStats> = HashMap::new();
    for row in traffic_rows {
        traffic_map.insert(
            (row.source_agent_id, row.target_agent_id),
            TrafficStats {
                message_count: row.message_count,
                avg_latency_ms: row.avg_latency_ms,
                error_count: row.error_count,
                pii_redacted_count: row.pii_redacted_count,
                injection_flagged_count: row.injection_flagged_count,
            },
        );
    }

    // 5. Discover agents from traffic not yet in the graph
    let all_known_ids: HashSet<Uuid> = nodes_map.keys().copied().collect();
    let unknown_agent_ids: Vec<Uuid> = traffic_map
        .keys()
        .flat_map(|(src, tgt)| [*src, *tgt])
        .filter(|id| !all_known_ids.contains(id))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    if !unknown_agent_ids.is_empty() {
        #[derive(Debug, sqlx::FromRow)]
        struct DiscoveredAgent {
            id: Uuid,
            name: String,
            organization_id: Uuid,
            project_id: Uuid,
        }

        let discovered = sqlx::query_as::<_, DiscoveredAgent>(
            "SELECT id, name, organization_id, project_id FROM a2a_agents WHERE id = ANY($1)",
        )
        .bind(&unknown_agent_ids)
        .fetch_all(state.db.as_ref())
        .await
        .unwrap_or_default();

        for agent in discovered {
            let kind = if agent.project_id == project_id {
                "project"
            } else if agent.organization_id == org_id {
                "org"
            } else {
                "external"
            };
            nodes_map.entry(agent.id).or_insert(TopologyNode {
                id: agent.id.to_string(),
                name: agent.name,
                kind,
                grant_status: Some("approved".into()),
            });
        }

    }

    // 5b. Always merge traffic pairs into edge_map (not only for unknown agents)
    for &(src, tgt) in traffic_map.keys() {
        if nodes_map.contains_key(&src) && nodes_map.contains_key(&tgt) {
            edge_map.entry((src, tgt)).or_insert_with(|| "approved".into());
        }
    }

    // 6. Assemble response
    let nodes: Vec<TopologyNode> = nodes_map.into_values().collect();
    let edges: Vec<TopologyEdge> = edge_map
        .into_iter()
        .map(|((src, tgt), status)| TopologyEdge {
            source: src.to_string(),
            target: tgt.to_string(),
            grant_status: status,
            traffic: traffic_map.get(&(src, tgt)).map(|t| TrafficStats {
                message_count: t.message_count,
                avg_latency_ms: t.avg_latency_ms,
                error_count: t.error_count,
                pii_redacted_count: t.pii_redacted_count,
                injection_flagged_count: t.injection_flagged_count,
            }),
        })
        .collect();

    Ok(Json(TopologyResponse { nodes, edges }))
}

fn grant_priority(status: &str) -> u8 {
    match status {
        "approved" => 3,
        "pending" => 2,
        "denied" => 1,
        "revoked" => 0,
        _ => 0,
    }
}
