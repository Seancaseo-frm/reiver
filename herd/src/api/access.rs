use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::app_state::HerdState;
use crate::audit::{AuditEventBuilder, AuditEventType};
use crate::verification;

pub fn router() -> Router<Arc<HerdState>> {
    Router::new()
        .route("/access/request", post(request_access))
        .route("/access/incoming", get(list_incoming))
        .route("/access/outgoing", get(list_outgoing))
        .route("/access/{id}/approve", post(approve_access))
        .route("/access/{id}/deny", post(deny_access))
        .route("/access/{id}/revoke", post(revoke_access))
        .route("/access/grants", get(list_grants))
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
pub struct RequestAccessBody {
    pub target_agent_id: Uuid,
    pub granted_agent_id: Uuid,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct AccessGrantRow {
    pub id: Uuid,
    pub target_agent_id: Uuid,
    pub target_org_id: Uuid,
    pub granted_org_id: Option<Uuid>,
    pub granted_agent_id: Uuid,
    pub status: String,
    pub requested_at: chrono::DateTime<chrono::Utc>,
    pub resolved_at: Option<chrono::DateTime<chrono::Utc>>,
    pub resolved_by: Option<Uuid>,
}

async fn request_access(
    State(state): State<Arc<HerdState>>,
    headers: HeaderMap,
    Json(body): Json<RequestAccessBody>,
) -> Result<(StatusCode, Json<AccessGrantRow>), (StatusCode, String)> {
    let (project_id, org_id) = extract_project_and_org(&headers)?;
    let user_id = headers
        .get("x-user-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or((StatusCode::UNAUTHORIZED, "Missing or invalid X-User-Id header".into()))?;

    // Validate that the granted (source) agent belongs to the caller's project
    let source_project_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT project_id FROM a2a_agents WHERE id = $1 AND enabled = true",
    )
    .bind(body.granted_agent_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to lookup source agent: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Failed to request access".into())
    })?;

    match source_project_id {
        Some(pid) if pid == project_id => {}
        Some(_) => {
            return Err((
                StatusCode::FORBIDDEN,
                "Source agent does not belong to your project".into(),
            ));
        }
        None => {
            return Err((StatusCode::NOT_FOUND, "Source agent not found".into()));
        }
    }

    // Look up the target agent to get its org_id and project_id
    let (target_org_id, target_project_id, target_agent_name): (Uuid, Uuid, String) =
        sqlx::query_as(
            "SELECT organization_id, project_id, name FROM a2a_agents WHERE id = $1 AND enabled = true",
        )
        .bind(body.target_agent_id)
        .fetch_optional(state.db.as_ref())
        .await
        .map_err(|e| {
            tracing::error!("Failed to lookup target agent: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to request access".into(),
            )
        })?
        .ok_or((StatusCode::NOT_FOUND, "Target agent not found".into()))?;

    if target_project_id == project_id {
        return Err((
            StatusCode::BAD_REQUEST,
            "Cannot request access to an agent in your own project".into(),
        ));
    }

    let mut row = sqlx::query_as::<_, AccessGrantRow>(
        "INSERT INTO a2a_access_grants (target_agent_id, target_org_id, granted_org_id, granted_agent_id, requesting_project_id, requested_by)
         VALUES ($1, $2, $3, $4, $5, $6)
         RETURNING id, target_agent_id, target_org_id, granted_org_id, granted_agent_id, status, requested_at, resolved_at, resolved_by"
    )
    .bind(body.target_agent_id)
    .bind(target_org_id)
    .bind(org_id)
    .bind(body.granted_agent_id)
    .bind(project_id)
    .bind(user_id)
    .fetch_one(state.db.as_ref())
    .await
    .map_err(|e| {
        if let sqlx::Error::Database(ref db_err) = e {
            if db_err.code().as_deref() == Some("23505") {
                return (StatusCode::CONFLICT, "A pending access request already exists for this agent pair".into());
            }
        }
        tracing::error!("Failed to create access grant: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Failed to request access".into())
    })?;

    // Try to resolve the grant immediately via webhook
    let resolved_status = try_resolve_via_webhook(
        &state,
        org_id,
        body.target_agent_id,
        &target_agent_name,
        target_org_id,
    )
    .await;

    if let Some(new_status) = resolved_status {
        let updated = sqlx::query_as::<_, AccessGrantRow>(
            "UPDATE a2a_access_grants
             SET status = $1, resolved_at = NOW()
             WHERE id = $2 AND status = 'pending'
             RETURNING id, target_agent_id, target_org_id, granted_org_id, granted_agent_id, status, requested_at, resolved_at, resolved_by"
        )
        .bind(new_status)
        .bind(row.id)
        .fetch_optional(state.db.as_ref())
        .await
        .map_err(|e| {
            tracing::error!("Failed to update grant after webhook: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Failed to request access".into())
        })?;

        if let Some(updated_row) = updated {
            row = updated_row;
            if new_status == "approved" {
                state.access_cache.approve(body.granted_agent_id, body.target_agent_id);
            } else {
                state.access_cache.remove(body.granted_agent_id, body.target_agent_id);
            }
        }
    }

    if row.status == "pending" {
        let kafka = state.kafka.clone();
        let event = serde_json::json!({
            "type": "a2a.access_requested",
            "grant_id": row.id,
            "target_agent_id": body.target_agent_id,
            "target_agent_name": target_agent_name,
            "target_org_id": target_org_id,
            "granted_agent_id": body.granted_agent_id,
            "requesting_org_id": org_id,
            "requested_at": row.requested_at.to_rfc3339(),
        });
        tokio::spawn(async move {
            if let Ok(payload) = serde_json::to_vec(&event) {
                let _ = kafka
                    .send_to_topic(
                        "reiver.a2a.events",
                        &target_org_id.to_string(),
                        &payload,
                    )
                    .await;
            }
        });
    }

    let audit_event_type = match row.status.as_str() {
        "approved" => AuditEventType::A2aAccessApproved,
        "denied" => AuditEventType::A2aAccessDenied,
        _ => AuditEventType::A2aAccessRequested,
    };
    AuditEventBuilder::new(audit_event_type)
        .actor(user_id)
        .organization(org_id)
        .resource("a2a_access_grant", row.id)
        .project(&project_id.to_string())
        .details(serde_json::json!({
            "target_agent_id": body.target_agent_id,
            "target_agent_name": target_agent_name,
            "granted_agent_id": body.granted_agent_id,
            "target_org_id": target_org_id,
            "resolved_via": if row.status == "pending" { "none" } else { "webhook" },
        }))
        .success()
        .log(&state.clickhouse)
        .await;

    Ok((StatusCode::CREATED, Json(row)))
}

/// Attempt to resolve an access request via the target org's verification webhook.
async fn try_resolve_via_webhook(
    state: &HerdState,
    source_org_id: Uuid,
    target_agent_id: Uuid,
    target_agent_name: &str,
    target_org_id: Uuid,
) -> Option<&'static str> {
    let org_row: Option<(Option<String>, Option<String>)> =
        sqlx::query_as("SELECT verification_url, webhook_secret FROM organizations WHERE id = $1")
            .bind(target_org_id)
            .fetch_optional(state.db.as_ref())
            .await
            .ok()?;

    let (verification_url, webhook_secret) = match org_row {
        Some((Some(url), Some(secret))) if !url.is_empty() => (url, secret),
        _ => return None,
    };

    let (owner_email, org_name): (String, String) = sqlx::query_as(
        "SELECT u.email, o.name
         FROM users u
         JOIN memberships m ON m.user_id = u.id
         JOIN organizations o ON o.id = m.organization_id
         WHERE m.organization_id = $1 AND m.role = 'owner' AND m.status = 'active'
         LIMIT 1",
    )
    .bind(source_org_id)
    .fetch_optional(state.db.as_ref())
    .await
    .ok()??;

    let payload = verification::VerificationPayload {
        requester_email: owner_email,
        requester_org_name: org_name,
        requester_org_id: source_org_id,
        target_agent_id,
        target_agent_name: target_agent_name.to_string(),
    };

    match verification::call_verification_webhook(
        &state.http_client,
        &verification_url,
        &webhook_secret,
        &payload,
    )
    .await
    {
        Ok(true) => Some("approved"),
        Ok(false) => Some("denied"),
        Err(e) => {
            tracing::warn!("Verification webhook error, leaving grant as pending: {e}");
            None
        }
    }
}

#[derive(Debug, Serialize, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct IncomingGrantRow {
    pub id: Uuid,
    pub target_agent_id: Uuid,
    pub target_agent_name: String,
    pub granted_agent_id: Uuid,
    pub granted_agent_name: String,
    pub granted_org_domain: Option<String>,
    pub requested_by_email: Option<String>,
    pub status: String,
    pub requested_at: chrono::DateTime<chrono::Utc>,
}

async fn list_incoming(
    State(state): State<Arc<HerdState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<IncomingGrantRow>>, (StatusCode, String)> {
    let (_project_id, org_id) = extract_project_and_org(&headers)?;

    let rows = sqlx::query_as::<_, IncomingGrantRow>(
        "SELECT g.id, g.target_agent_id, t.name AS target_agent_name,
                g.granted_agent_id, ga.name AS granted_agent_name,
                o.domain AS granted_org_domain,
                u.email AS requested_by_email,
                g.status, g.requested_at
         FROM a2a_access_grants g
         JOIN a2a_agents t ON t.id = g.target_agent_id
         JOIN a2a_agents ga ON ga.id = g.granted_agent_id
         LEFT JOIN organizations o ON o.id = g.granted_org_id
         LEFT JOIN users u ON u.id = g.requested_by
         WHERE g.target_org_id = $1 AND g.status = 'pending'
         ORDER BY g.requested_at DESC
         LIMIT 200",
    )
    .bind(org_id)
    .fetch_all(state.db.as_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to list incoming grants: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to list incoming grants".into(),
        )
    })?;

    Ok(Json(rows))
}

async fn resolve_access(
    state: &HerdState,
    org_id: Uuid,
    user_id: Option<Uuid>,
    grant_id: Uuid,
    new_status: &str,
) -> Result<Json<AccessGrantRow>, (StatusCode, String)> {
    let required_current_status = match new_status {
        "approved" | "denied" => "pending",
        "revoked" => "approved",
        _ => return Err((StatusCode::BAD_REQUEST, "Invalid status transition".into())),
    };

    let row = sqlx::query_as::<_, AccessGrantRow>(
        "UPDATE a2a_access_grants
         SET status = $1, resolved_at = NOW(), resolved_by = $2
         WHERE id = $3 AND target_org_id = $4 AND status = $5
         RETURNING id, target_agent_id, target_org_id, granted_org_id, granted_agent_id, status, requested_at, resolved_at, resolved_by"
    )
    .bind(new_status)
    .bind(user_id)
    .bind(grant_id)
    .bind(org_id)
    .bind(required_current_status)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to resolve access grant: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Failed to resolve access grant".into())
    })?
    .ok_or((StatusCode::NOT_FOUND, "Access grant not found or invalid state transition".into()))?;

    // Sync the in-memory cache using agent-to-agent key
    match new_status {
        "approved" => {
            state
                .access_cache
                .approve(row.granted_agent_id, row.target_agent_id);
        }
        "denied" | "revoked" => {
            state.access_cache.remove(row.granted_agent_id, row.target_agent_id);
        }
        _ => {}
    }

    let audit_event_type = match new_status {
        "approved" => AuditEventType::A2aAccessApproved,
        "denied" => AuditEventType::A2aAccessDenied,
        "revoked" => AuditEventType::A2aAccessRevoked,
        _ => AuditEventType::A2aAccessDenied,
    };
    let mut builder = AuditEventBuilder::new(audit_event_type)
        .organization(org_id)
        .resource("a2a_access_grant", grant_id)
        .details(serde_json::json!({
            "target_agent_id": row.target_agent_id,
            "granted_agent_id": row.granted_agent_id,
            "previous_status": required_current_status,
        }))
        .success();
    if let Some(uid) = user_id {
        builder = builder.actor(uid);
    }
    builder.log(&state.clickhouse).await;

    Ok(Json(row))
}

async fn approve_access(
    State(state): State<Arc<HerdState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<AccessGrantRow>, (StatusCode, String)> {
    let (_project_id, org_id) = extract_project_and_org(&headers)?;
    let user_id = headers
        .get("x-user-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| Uuid::parse_str(s).ok());
    resolve_access(&state, org_id, user_id, id, "approved").await
}

async fn deny_access(
    State(state): State<Arc<HerdState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<AccessGrantRow>, (StatusCode, String)> {
    let (_project_id, org_id) = extract_project_and_org(&headers)?;
    let user_id = headers
        .get("x-user-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| Uuid::parse_str(s).ok());
    resolve_access(&state, org_id, user_id, id, "denied").await
}

async fn revoke_access(
    State(state): State<Arc<HerdState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<AccessGrantRow>, (StatusCode, String)> {
    let (_project_id, org_id) = extract_project_and_org(&headers)?;
    let user_id = headers
        .get("x-user-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| Uuid::parse_str(s).ok());
    resolve_access(&state, org_id, user_id, id, "revoked").await
}

#[derive(Debug, Serialize, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct OutgoingGrantRow {
    pub id: Uuid,
    pub granted_agent_id: Uuid,
    pub granted_agent_name: String,
    pub target_agent_id: Uuid,
    pub target_agent_name: String,
    pub target_org_domain: Option<String>,
    pub requested_by_email: Option<String>,
    pub status: String,
    pub requested_at: chrono::DateTime<chrono::Utc>,
}

async fn list_outgoing(
    State(state): State<Arc<HerdState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<OutgoingGrantRow>>, (StatusCode, String)> {
    let (project_id, org_id) = extract_project_and_org(&headers)?;

    let rows = sqlx::query_as::<_, OutgoingGrantRow>(
        "SELECT g.id, g.granted_agent_id, ga.name AS granted_agent_name,
                g.target_agent_id, t.name AS target_agent_name,
                o.domain AS target_org_domain,
                u.email AS requested_by_email,
                g.status, g.requested_at
         FROM a2a_access_grants g
         JOIN a2a_agents t ON t.id = g.target_agent_id
         JOIN a2a_agents ga ON ga.id = g.granted_agent_id
         LEFT JOIN organizations o ON o.id = g.target_org_id
         LEFT JOIN users u ON u.id = g.requested_by
         WHERE g.granted_org_id = $1 AND g.requesting_project_id = $2 AND g.status = 'pending'
         ORDER BY g.requested_at DESC
         LIMIT 200",
    )
    .bind(org_id)
    .bind(project_id)
    .fetch_all(state.db.as_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to list outgoing grants: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to list outgoing grants".into(),
        )
    })?;

    Ok(Json(rows))
}

#[derive(Debug, Serialize, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct ActiveGrantRow {
    pub id: Uuid,
    pub granted_agent_id: Uuid,
    pub granted_agent_name: String,
    pub target_agent_id: Uuid,
    pub target_agent_name: String,
    pub granted_org_domain: Option<String>,
    pub status: String,
    pub requested_at: chrono::DateTime<chrono::Utc>,
    pub resolved_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GrantPair {
    agent_a_id: Uuid,
    agent_a_name: String,
    agent_b_id: Uuid,
    agent_b_name: String,
    a_to_b_grant_id: Option<Uuid>,
    b_to_a_grant_id: Option<Uuid>,
    bidirectional: bool,
    org_domain: Option<String>,
    approved_at: Option<chrono::DateTime<chrono::Utc>>,
}

async fn list_grants(
    State(state): State<Arc<HerdState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<GrantPair>>, (StatusCode, String)> {
    let (_project_id, org_id) = extract_project_and_org(&headers)?;

    let rows = sqlx::query_as::<_, ActiveGrantRow>(
        "SELECT g.id, g.granted_agent_id, ga.name AS granted_agent_name,
                g.target_agent_id, t.name AS target_agent_name,
                o.domain AS granted_org_domain,
                g.status, g.requested_at, g.resolved_at
         FROM a2a_access_grants g
         JOIN a2a_agents t ON t.id = g.target_agent_id
         JOIN a2a_agents ga ON ga.id = g.granted_agent_id
         LEFT JOIN organizations o ON o.id = g.granted_org_id
         WHERE (g.target_org_id = $1 OR g.granted_org_id = $1) AND g.status = 'approved'
         ORDER BY g.requested_at DESC
         LIMIT 200",
    )
    .bind(org_id)
    .fetch_all(state.db.as_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to list grants: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to list grants".into(),
        )
    })?;

    use std::collections::HashMap;
    let mut pairs: HashMap<(Uuid, Uuid), GrantPair> = HashMap::new();

    for row in rows {
        let (a, b) = if row.granted_agent_id < row.target_agent_id {
            (row.granted_agent_id, row.target_agent_id)
        } else {
            (row.target_agent_id, row.granted_agent_id)
        };

        let pair = pairs.entry((a, b)).or_insert_with(|| {
            let (a_name, b_name) = if row.granted_agent_id < row.target_agent_id {
                (row.granted_agent_name.clone(), row.target_agent_name.clone())
            } else {
                (row.target_agent_name.clone(), row.granted_agent_name.clone())
            };
            GrantPair {
                agent_a_id: a,
                agent_a_name: a_name,
                agent_b_id: b,
                agent_b_name: b_name,
                a_to_b_grant_id: None,
                b_to_a_grant_id: None,
                bidirectional: false,
                org_domain: row.granted_org_domain.clone(),
                approved_at: row.resolved_at,
            }
        });

        if row.granted_agent_id == a {
            pair.a_to_b_grant_id = Some(row.id);
        } else {
            pair.b_to_a_grant_id = Some(row.id);
        }
        pair.bidirectional = pair.a_to_b_grant_id.is_some() && pair.b_to_a_grant_id.is_some();
    }

    let mut result: Vec<GrantPair> = pairs.into_values().collect();
    result.sort_by(|a, b| b.approved_at.cmp(&a.approved_at));

    Ok(Json(result))
}
