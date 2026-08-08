use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::{delete, get, post, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::a2a::types::{AgentCard, AgentSkill, AgentVisibility};
use crate::app_state::HerdState;
use crate::routing_cache::AgentRouting;

pub fn router() -> Router<Arc<HerdState>> {
    Router::new()
        .route("/agents", post(register_agent))
        .route("/agents", get(list_agents))
        .route("/agents/{id}", get(get_agent))
        .route("/agents/{id}", put(update_agent))
        .route("/agents/{id}", delete(delete_agent))
        .route("/discover", get(discover_agents))
        .route("/discover/{agent_id}/card", get(get_agent_card))
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
#[serde(rename_all = "camelCase")]
pub struct RegisterAgentRequest {
    pub name: String,
    pub description: Option<String>,
    pub endpoint_url: String,
    pub key_id: Option<Uuid>,
    pub visibility: Option<AgentVisibility>,
    #[serde(default)]
    pub skills: Vec<AgentSkill>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentResponse {
    pub id: Uuid,
    pub project_id: Uuid,
    pub organization_id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub endpoint_url: String,
    pub key_id: Option<Uuid>,
    pub agent_card: AgentCard,
    pub visibility: AgentVisibility,
    pub enabled: bool,
    pub created_at: String,
    pub updated_at: String,
}

impl From<AgentRow> for AgentResponse {
    fn from(row: AgentRow) -> Self {
        let fallback_card = AgentCard {
            name: row.name.clone(),
            description: row.description.clone().unwrap_or_default(),
            supported_interfaces: vec![crate::a2a::types::AgentInterface {
                url: row.endpoint_url.clone(),
                protocol_binding: "jsonrpc/http".into(),
                tenant: None,
                protocol_version: "1.0".into(),
            }],
            provider: None,
            version: "1.0".into(),
            documentation_url: None,
            capabilities: crate::a2a::types::AgentCapabilities {
                streaming: Some(false),
                push_notifications: Some(true),
                extensions: None,
                extended_agent_card: None,
            },
            security_schemes: None,
            security_requirements: None,
            default_input_modes: vec!["application/json".into()],
            default_output_modes: vec!["application/json".into()],
            skills: vec![],
            signatures: None,
            icon_url: None,
        };
        Self {
            id: row.id,
            project_id: row.project_id,
            organization_id: row.organization_id,
            name: row.name,
            description: row.description,
            endpoint_url: row.endpoint_url,
            key_id: row.key_id,
            agent_card: serde_json::from_value(row.agent_card).unwrap_or(fallback_card),
            visibility: match row.visibility.as_str() {
                "private" => AgentVisibility::Private,
                "public" => AgentVisibility::Public,
                _ => AgentVisibility::Org,
            },
            enabled: row.enabled,
            created_at: row.created_at.to_rfc3339(),
            updated_at: row.updated_at.to_rfc3339(),
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
struct AgentRow {
    id: Uuid,
    project_id: Uuid,
    organization_id: Uuid,
    name: String,
    description: Option<String>,
    endpoint_url: String,
    key_id: Option<Uuid>,
    agent_card: serde_json::Value,
    visibility: String,
    enabled: bool,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

async fn register_agent(
    State(state): State<Arc<HerdState>>,
    headers: HeaderMap,
    Json(body): Json<RegisterAgentRequest>,
) -> Result<(StatusCode, Json<AgentResponse>), (StatusCode, String)> {
    let (project_id, org_id) = extract_project_and_org(&headers)?;

    if body.endpoint_url.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "endpoint_url is required".into()));
    }

    if body.name.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "name is required".into()));
    }

    if let Some(key_id) = body.key_id {
        let valid: Option<Uuid> = sqlx::query_scalar(
            "SELECT id FROM project_keys WHERE id = $1 AND project_id = $2 AND key_type = 'agent'",
        )
        .bind(key_id)
        .bind(project_id)
        .fetch_optional(state.db.as_ref())
        .await
        .map_err(|e| {
            tracing::error!("Failed to validate key_id: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to validate agent token".into(),
            )
        })?;

        if valid.is_none() {
            return Err((
                StatusCode::BAD_REQUEST,
                "key_id must reference an agent token in this project".into(),
            ));
        }
    }

    let visibility = body.visibility.unwrap_or(AgentVisibility::Org);

    let card = build_agent_card(
        &body.name,
        body.description.as_deref(),
        &body.endpoint_url,
        body.skills,
    );
    let card_json = serde_json::to_value(&card).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to build agent card: {}", e),
        )
    })?;

    let row = sqlx::query_as::<_, AgentRow>(
        "INSERT INTO a2a_agents (project_id, organization_id, name, description, endpoint_url, key_id, agent_card, visibility)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         RETURNING id, project_id, organization_id, name, description, endpoint_url, key_id, agent_card, visibility, enabled, created_at, updated_at"
    )
    .bind(project_id)
    .bind(org_id)
    .bind(&body.name)
    .bind(&body.description)
    .bind(&body.endpoint_url)
    .bind(body.key_id)
    .bind(&card_json)
    .bind(visibility.to_string())
    .fetch_one(state.db.as_ref())
    .await
    .map_err(|e| {
        if e.to_string().contains("duplicate key") {
            (StatusCode::CONFLICT, format!("Agent '{}' already exists in this project", body.name))
        } else {
            tracing::error!("Failed to register agent: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Failed to register agent".into())
        }
    })?;

    // Populate routing cache for the new agent
    let webhook_secret: Option<String> =
        sqlx::query_scalar("SELECT webhook_secret FROM organizations WHERE id = $1")
            .bind(org_id)
            .fetch_optional(state.db.as_ref())
            .await
            .ok()
            .flatten()
            .flatten();

    state.routing_cache.upsert_agent(
        row.id,
        AgentRouting {
            endpoint_url: row.endpoint_url.clone(),
            enabled: row.enabled,
            webhook_secret,
        },
    );

    Ok((StatusCode::CREATED, Json(AgentResponse::from(row))))
}

fn build_agent_card(
    name: &str,
    description: Option<&str>,
    endpoint_url: &str,
    skills: Vec<AgentSkill>,
) -> AgentCard {
    use crate::a2a::types::*;
    AgentCard {
        name: name.to_string(),
        description: description.unwrap_or("").to_string(),
        supported_interfaces: vec![AgentInterface {
            url: endpoint_url.to_string(),
            protocol_binding: "jsonrpc/http".into(),
            tenant: None,
            protocol_version: "1.0".into(),
        }],
        provider: None,
        version: "1.0".into(),
        documentation_url: None,
        capabilities: AgentCapabilities {
            streaming: Some(false),
            push_notifications: Some(true),
            extensions: None,
            extended_agent_card: None,
        },
        security_schemes: None,
        security_requirements: None,
        default_input_modes: vec!["application/json".into()],
        default_output_modes: vec!["application/json".into()],
        skills,
        signatures: None,
        icon_url: None,
    }
}

async fn list_agents(
    State(state): State<Arc<HerdState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<AgentResponse>>, (StatusCode, String)> {
    let (project_id, _org_id) = extract_project_and_org(&headers)?;

    let rows = sqlx::query_as::<_, AgentRow>(
        "SELECT id, project_id, organization_id, name, description, endpoint_url, key_id, agent_card, visibility, enabled, created_at, updated_at
         FROM a2a_agents
         WHERE project_id = $1
         ORDER BY created_at DESC"
    )
    .bind(project_id)
    .fetch_all(state.db.as_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to list agents: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Failed to list agents".into())
    })?;

    Ok(Json(rows.into_iter().map(AgentResponse::from).collect()))
}

async fn get_agent(
    State(state): State<Arc<HerdState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<AgentResponse>, (StatusCode, String)> {
    let (project_id, _org_id) = extract_project_and_org(&headers)?;

    let row = sqlx::query_as::<_, AgentRow>(
        "SELECT id, project_id, organization_id, name, description, endpoint_url, key_id, agent_card, visibility, enabled, created_at, updated_at
         FROM a2a_agents
         WHERE id = $1 AND project_id = $2"
    )
    .bind(id)
    .bind(project_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to get agent: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Failed to get agent".into())
    })?
    .ok_or((StatusCode::NOT_FOUND, "Agent not found".into()))?;

    Ok(Json(AgentResponse::from(row)))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateAgentRequest {
    pub description: Option<String>,
    pub endpoint_url: Option<String>,
    pub key_id: Option<Uuid>,
    pub visibility: Option<AgentVisibility>,
    pub enabled: Option<bool>,
    pub skills: Option<Vec<AgentSkill>>,
}

async fn update_agent(
    State(state): State<Arc<HerdState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateAgentRequest>,
) -> Result<Json<AgentResponse>, (StatusCode, String)> {
    let (project_id, _org_id) = extract_project_and_org(&headers)?;

    let existing = sqlx::query_as::<_, AgentRow>(
        "SELECT id, project_id, organization_id, name, description, endpoint_url, key_id, agent_card, visibility, enabled, created_at, updated_at
         FROM a2a_agents WHERE id = $1 AND project_id = $2"
    )
    .bind(id)
    .bind(project_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to get agent for update: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Failed to update agent".into())
    })?
    .ok_or((StatusCode::NOT_FOUND, "Agent not found".into()))?;

    let new_endpoint_url = body.endpoint_url.unwrap_or(existing.endpoint_url);
    let new_key_id = if body.key_id.is_some() {
        body.key_id
    } else {
        existing.key_id
    };
    let new_visibility = body
        .visibility
        .map(|v| v.to_string())
        .unwrap_or(existing.visibility);
    let new_description = body.description.or(existing.description);
    let new_enabled = body.enabled.unwrap_or(existing.enabled);

    let existing_card: AgentCard =
        serde_json::from_value(existing.agent_card).unwrap_or_else(|_| AgentCard {
            name: existing.name.clone(),
            description: String::new(),
            supported_interfaces: vec![],
            provider: None,
            version: "1.0".into(),
            documentation_url: None,
            capabilities: crate::a2a::types::AgentCapabilities {
                streaming: Some(false),
                push_notifications: Some(true),
                extensions: None,
                extended_agent_card: None,
            },
            security_schemes: None,
            security_requirements: None,
            default_input_modes: vec!["application/json".into()],
            default_output_modes: vec!["application/json".into()],
            skills: vec![],
            signatures: None,
            icon_url: None,
        });
    let new_skills = body.skills.unwrap_or(existing_card.skills);
    let new_card = build_agent_card(
        &existing.name,
        new_description.as_deref(),
        &new_endpoint_url,
        new_skills,
    );
    let new_card_json = serde_json::to_value(&new_card).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to build agent card: {}", e),
        )
    })?;

    let row = sqlx::query_as::<_, AgentRow>(
        "UPDATE a2a_agents
         SET description = $1, endpoint_url = $2, key_id = $3, agent_card = $4, visibility = $5, enabled = $6, updated_at = NOW()
         WHERE id = $7 AND project_id = $8
         RETURNING id, project_id, organization_id, name, description, endpoint_url, key_id, agent_card, visibility, enabled, created_at, updated_at"
    )
    .bind(&new_description)
    .bind(&new_endpoint_url)
    .bind(new_key_id)
    .bind(&new_card_json)
    .bind(&new_visibility)
    .bind(new_enabled)
    .bind(id)
    .bind(project_id)
    .fetch_one(state.db.as_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to update agent: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Failed to update agent".into())
    })?;

    // Update routing cache (preserve existing webhook_secret)
    let existing_secret = state
        .routing_cache
        .get_agent(row.id)
        .and_then(|r| r.webhook_secret);
    state.routing_cache.upsert_agent(
        row.id,
        AgentRouting {
            endpoint_url: row.endpoint_url.clone(),
            enabled: row.enabled,
            webhook_secret: existing_secret,
        },
    );

    Ok(Json(AgentResponse::from(row)))
}

async fn delete_agent(
    State(state): State<Arc<HerdState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, (StatusCode, String)> {
    let (project_id, _org_id) = extract_project_and_org(&headers)?;

    let result = sqlx::query("DELETE FROM a2a_agents WHERE id = $1 AND project_id = $2")
        .bind(id)
        .bind(project_id)
        .execute(state.db.as_ref())
        .await
        .map_err(|e| {
            tracing::error!("Failed to delete agent: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to delete agent".into(),
            )
        })?;

    if result.rows_affected() == 0 {
        return Err((StatusCode::NOT_FOUND, "Agent not found".into()));
    }

    state.routing_cache.remove_agent(id);

    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoverQuery {
    pub q: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
struct DiscoveryRow {
    id: Uuid,
    project_id: Uuid,
    organization_id: Uuid,
    name: String,
    description: Option<String>,
    endpoint_url: String,
    key_id: Option<Uuid>,
    agent_card: serde_json::Value,
    visibility: String,
    enabled: bool,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
    organization_name: String,
    needs_access: bool,
    access_pending: bool,
    access_granted: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DiscoveryAgentResponse {
    id: Uuid,
    name: String,
    description: Option<String>,
    organization_name: String,
    visibility: AgentVisibility,
    skills: Vec<AgentSkill>,
    needs_access: bool,
    access_pending: bool,
    access_granted: bool,
}

impl From<DiscoveryRow> for DiscoveryAgentResponse {
    fn from(row: DiscoveryRow) -> Self {
        let card: AgentCard = serde_json::from_value(row.agent_card).unwrap_or_else(|_| AgentCard {
            name: row.name.clone(),
            description: row.description.clone().unwrap_or_default(),
            supported_interfaces: vec![],
            provider: None,
            version: "1.0".into(),
            documentation_url: None,
            capabilities: crate::a2a::types::AgentCapabilities {
                streaming: None,
                push_notifications: None,
                extensions: None,
                extended_agent_card: None,
            },
            security_schemes: None,
            security_requirements: None,
            default_input_modes: vec![],
            default_output_modes: vec![],
            skills: vec![],
            signatures: None,
            icon_url: None,
        });
        Self {
            id: row.id,
            name: row.name,
            description: row.description,
            organization_name: row.organization_name,
            visibility: match row.visibility.as_str() {
                "private" => AgentVisibility::Private,
                "public" => AgentVisibility::Public,
                _ => AgentVisibility::Org,
            },
            skills: card.skills,
            needs_access: row.needs_access,
            access_pending: row.access_pending,
            access_granted: row.access_granted,
        }
    }
}

async fn discover_agents(
    State(state): State<Arc<HerdState>>,
    headers: HeaderMap,
    axum::extract::Query(query): axum::extract::Query<DiscoverQuery>,
) -> Result<Json<Vec<DiscoveryAgentResponse>>, (StatusCode, String)> {
    let (project_id, org_id) = extract_project_and_org(&headers)?;

    let search_term = query.q.unwrap_or_default();
    let like_pattern = format!("%{}%", search_term);

    let rows = sqlx::query_as::<_, DiscoveryRow>(
        "SELECT a.id, a.project_id, a.organization_id, a.name, a.description,
                a.endpoint_url, a.key_id, a.agent_card, a.visibility, a.enabled,
                a.created_at, a.updated_at,
                o.name AS organization_name,
                (a.project_id != $2) AS needs_access,
                EXISTS (
                    SELECT 1 FROM a2a_access_grants g
                    WHERE g.target_agent_id = a.id
                      AND g.requesting_project_id = $2
                      AND g.status = 'pending'
                ) AS access_pending,
                EXISTS (
                    SELECT 1 FROM a2a_access_grants g
                    WHERE g.target_agent_id = a.id
                      AND g.requesting_project_id = $2
                      AND g.status = 'approved'
                ) AS access_granted
         FROM a2a_agents a
         JOIN organizations o ON o.id = a.organization_id
         WHERE a.enabled = true
           AND (
             (a.organization_id = $1 AND a.visibility IN ('org', 'public'))
             OR a.visibility = 'public'
           )
           AND (a.name ILIKE $3 OR a.description ILIKE $3 OR $4 = '')
         ORDER BY a.name
         LIMIT 100",
    )
    .bind(org_id)
    .bind(project_id)
    .bind(&like_pattern)
    .bind(&search_term)
    .fetch_all(state.db.as_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to discover agents: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to discover agents".into(),
        )
    })?;

    Ok(Json(
        rows.into_iter().map(DiscoveryAgentResponse::from).collect(),
    ))
}

async fn get_agent_card(
    State(state): State<Arc<HerdState>>,
    headers: HeaderMap,
    Path(agent_id): Path<Uuid>,
) -> Result<Json<AgentCard>, (StatusCode, String)> {
    let (_project_id, org_id) = extract_project_and_org(&headers)?;

    let row = sqlx::query_as::<_, AgentRow>(
        "SELECT a.id, a.project_id, a.organization_id, a.name, a.description,
                a.endpoint_url, a.key_id, a.agent_card, a.visibility, a.enabled, a.created_at, a.updated_at
         FROM a2a_agents a
         WHERE a.id = $1 AND a.enabled = true
           AND (
             a.organization_id = $2
             OR (a.visibility = 'public' AND EXISTS (
               SELECT 1 FROM a2a_access_grants g
               WHERE g.target_agent_id = a.id
                 AND g.granted_org_id = $2
                 AND g.status = 'approved'
             ))
           )"
    )
    .bind(agent_id)
    .bind(org_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to get agent card: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Failed to get agent card".into())
    })?
    .ok_or((StatusCode::NOT_FOUND, "Agent not found or not accessible".into()))?;

    let card: AgentCard = serde_json::from_value(row.agent_card).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Corrupt agent card: {}", e),
        )
    })?;

    Ok(Json(card))
}
