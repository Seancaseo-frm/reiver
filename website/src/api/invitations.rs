use axum::{
    extract::{Path, State},
    http::HeaderMap,
    routing::{delete, get, post, put},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info};
use uuid::Uuid;

use crate::api::auth_helpers::{authenticate, require_admin, ErrorResponse};
use crate::app_state::WebsiteState;
use crate::audit::{AuditCaller, AuditEventBuilder, AuditEventType, AuditOrigin};
use crate::error::{AppError, Result};

const INVITE_EXPIRY_DAYS: i64 = 7;

pub fn create_invitations_router() -> Router<Arc<WebsiteState>> {
    Router::new()
        .route("/", get(list_invitations))
        .route("/", post(create_invitation))
        .route("/{id}", delete(revoke_invitation))
        .route("/info", get(get_org_info))
        .route("/members", get(list_members))
        .route("/members/{user_id}", put(update_member_role))
        .route("/members/{user_id}", delete(remove_member))
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, sqlx::FromRow)]
struct InvitationResponse {
    id: Uuid,
    email: Option<String>,
    role: String,
    invite_token: String,
    invited_by: Uuid,
    expires_at: DateTime<Utc>,
    accepted_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct CreateInvitationRequest {
    #[serde(default)]
    email: Option<String>,
    #[serde(default = "default_role")]
    role: String,
}

fn default_role() -> String {
    "member".to_string()
}

#[derive(Debug, Serialize, sqlx::FromRow)]
struct MemberResponse {
    user_id: Uuid,
    email: String,
    role: String,
    status: String,
    joined_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct UpdateRoleRequest {
    role: String,
}

// ---------------------------------------------------------------------------
// Org info endpoint
// ---------------------------------------------------------------------------

async fn get_org_info(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> std::result::Result<Json<serde_json::Value>, (axum::http::StatusCode, Json<ErrorResponse>)> {
    let ctx = authenticate(&headers, &state).await?;

    let domain: Option<String> =
        sqlx::query_scalar("SELECT domain FROM organizations WHERE id = $1")
            .bind(ctx.organization_id)
            .fetch_optional(state.db.as_ref())
            .await
            .map_err(|e| {
                error!("Failed to get org info: {}", e);
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse::new("Failed to get organization info")),
                )
            })?
            .flatten();

    Ok(Json(serde_json::json!({
        "organization_id": ctx.organization_id,
        "domain": domain,
    })))
}

// ---------------------------------------------------------------------------
// Invitation endpoints
// ---------------------------------------------------------------------------

async fn list_invitations(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> std::result::Result<Json<Vec<InvitationResponse>>, (axum::http::StatusCode, Json<ErrorResponse>)>
{
    let ctx = authenticate(&headers, &state).await?;
    require_admin(&state, ctx.user_id, ctx.organization_id).await?;

    let invitations = sqlx::query_as::<_, InvitationResponse>(
        r#"SELECT id, email, role, invite_token, invited_by, expires_at, accepted_at, created_at
           FROM organization_invitations
           WHERE organization_id = $1 AND accepted_at IS NULL AND expires_at > NOW()
           ORDER BY created_at DESC"#,
    )
    .bind(ctx.organization_id)
    .fetch_all(state.db.as_ref())
    .await
    .map_err(|e| {
        error!("Failed to list invitations: {}", e);
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::new("Failed to list invitations")),
        )
    })?;

    Ok(Json(invitations))
}

async fn create_invitation(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(req): Json<CreateInvitationRequest>,
) -> Result<Json<InvitationResponse>> {
    let ctx = authenticate(&headers, &state)
        .await
        .map_err(|(_, e)| AppError::Auth(e.0.error.clone()))?;
    require_admin(&state, ctx.user_id, ctx.organization_id)
        .await
        .map_err(|(_, e)| AppError::Forbidden(e.0.error.clone()))?;

    let valid_roles = ["member", "admin", "viewer"];
    if !valid_roles.contains(&req.role.as_str()) {
        return Err(AppError::BadRequest(format!(
            "Invalid role: {}. Must be one of: {:?}",
            req.role, valid_roles
        )));
    }

    let email = req.email.map(|e| e.to_lowercase());

    // If email-based, check for existing pending invite
    if let Some(ref email_addr) = email {
        let existing: Option<(Uuid,)> = sqlx::query_as(
            r#"SELECT id FROM organization_invitations
               WHERE organization_id = $1 AND LOWER(email) = $2
                 AND accepted_at IS NULL AND expires_at > NOW()"#,
        )
        .bind(ctx.organization_id)
        .bind(email_addr)
        .fetch_optional(state.db.as_ref())
        .await?;

        if existing.is_some() {
            return Err(AppError::Conflict(
                "An active invitation already exists for this email".to_string(),
            ));
        }

        // Check if user is already a member
        let existing_member: Option<(Uuid,)> = sqlx::query_as(
            r#"SELECT m.id FROM memberships m
               JOIN users u ON u.id = m.user_id
               WHERE m.organization_id = $1 AND LOWER(u.email) = $2 AND m.status = 'active'"#,
        )
        .bind(ctx.organization_id)
        .bind(email_addr)
        .fetch_optional(state.db.as_ref())
        .await?;

        if existing_member.is_some() {
            return Err(AppError::Conflict(
                "This user is already a member of the organization".to_string(),
            ));
        }
    }

    let invite_token = generate_invite_token();
    let expires_at = Utc::now() + chrono::Duration::days(INVITE_EXPIRY_DAYS);

    let invitation = sqlx::query_as::<_, InvitationResponse>(
        r#"INSERT INTO organization_invitations
              (organization_id, email, role, invite_token, invited_by, expires_at)
           VALUES ($1, $2, $3, $4, $5, $6)
           RETURNING id, email, role, invite_token, invited_by, expires_at, accepted_at, created_at"#,
    )
    .bind(ctx.organization_id)
    .bind(&email)
    .bind(&req.role)
    .bind(&invite_token)
    .bind(ctx.user_id)
    .bind(expires_at)
    .fetch_one(state.db.as_ref())
    .await?;

    info!(
        "Invitation created: org={}, email={:?}, role={}, by={}",
        ctx.organization_id, email, req.role, ctx.user_id
    );

    if let (Some(ref mailer), Some(ref addr)) = (&state.email, &email) {
        let inviter_name: String = sqlx::query_scalar("SELECT email FROM users WHERE id = $1")
            .bind(ctx.user_id)
            .fetch_optional(state.db.as_ref())
            .await
            .ok()
            .flatten()
            .unwrap_or_else(|| "A teammate".to_string());

        let org_name: String = sqlx::query_scalar("SELECT name FROM organizations WHERE id = $1")
            .bind(ctx.organization_id)
            .fetch_optional(state.db.as_ref())
            .await
            .ok()
            .flatten()
            .unwrap_or_else(|| "your organization".to_string());

        let app_url = state
            .config
            .app_url
            .as_deref()
            .unwrap_or("https://reiver.ai");
        let invite_url = format!("{}/api/invite/{}", app_url, invitation.invite_token);

        let vars = reiver_core::email::InviteVars {
            inviter_name,
            organization_name: org_name,
            invite_url,
            role: req.role.clone(),
        };
        let mailer = mailer.clone();
        let to = addr.clone();
        tokio::spawn(async move {
            if let Err(e) = mailer.send_invite(&to, vars).await {
                tracing::warn!("Failed to send invite email to {}: {}", to, e);
            }
        });
    }

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::InvitationCreated)
        .actor(ctx.user_id)
        .organization(ctx.organization_id)
        .resource("invitation", invitation.id)
        .details(serde_json::json!({ "created": { "email": &email, "role": &req.role } }))
        .origin(
            &audit_origin.origin_type,
            &audit_origin.origin_ref,
            &audit_origin.origin_reason,
        )
        .caller(
            &audit_caller.caller_type,
            &audit_caller.key_label,
            &audit_caller.key_prefix,
        )
        .success()
        .log(&state.clickhouse)
        .await;

    Ok(Json(invitation))
}

async fn revoke_invitation(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(invitation_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>> {
    let ctx = authenticate(&headers, &state)
        .await
        .map_err(|(_, e)| AppError::Auth(e.0.error.clone()))?;
    require_admin(&state, ctx.user_id, ctx.organization_id)
        .await
        .map_err(|(_, e)| AppError::Forbidden(e.0.error.clone()))?;

    let before_invitation: Option<(Option<String>, String)> = sqlx::query_as(
        "SELECT email, role FROM organization_invitations WHERE id = $1 AND organization_id = $2",
    )
    .bind(invitation_id)
    .bind(ctx.organization_id)
    .fetch_optional(state.db.as_ref())
    .await?;

    let (inv_email, inv_role) =
        before_invitation.ok_or_else(|| AppError::NotFound("Invitation not found".to_string()))?;

    sqlx::query("DELETE FROM organization_invitations WHERE id = $1 AND organization_id = $2")
        .bind(invitation_id)
        .bind(ctx.organization_id)
        .execute(state.db.as_ref())
        .await?;

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::InvitationRevoked)
        .actor(ctx.user_id)
        .organization(ctx.organization_id)
        .resource("invitation", invitation_id)
        .details(serde_json::json!({ "deleted": { "email": &inv_email, "role": &inv_role } }))
        .origin(
            &audit_origin.origin_type,
            &audit_origin.origin_ref,
            &audit_origin.origin_reason,
        )
        .caller(
            &audit_caller.caller_type,
            &audit_caller.key_label,
            &audit_caller.key_prefix,
        )
        .success()
        .log(&state.clickhouse)
        .await;

    Ok(Json(serde_json::json!({ "deleted": true })))
}

// ---------------------------------------------------------------------------
// Member management endpoints
// ---------------------------------------------------------------------------

async fn list_members(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> std::result::Result<Json<Vec<MemberResponse>>, (axum::http::StatusCode, Json<ErrorResponse>)> {
    let ctx = authenticate(&headers, &state).await?;

    let members = sqlx::query_as::<_, MemberResponse>(
        r#"SELECT m.user_id, u.email, m.role, m.status, m.created_at AS joined_at
           FROM memberships m
           JOIN users u ON u.id = m.user_id
           WHERE m.organization_id = $1 AND m.status = 'active'
           ORDER BY m.created_at ASC"#,
    )
    .bind(ctx.organization_id)
    .fetch_all(state.db.as_ref())
    .await
    .map_err(|e| {
        error!("Failed to list members: {}", e);
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::new("Failed to list members")),
        )
    })?;

    Ok(Json(members))
}

async fn update_member_role(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(target_user_id): Path<Uuid>,
    Json(req): Json<UpdateRoleRequest>,
) -> Result<Json<serde_json::Value>> {
    let ctx = authenticate(&headers, &state)
        .await
        .map_err(|(_, e)| AppError::Auth(e.0.error.clone()))?;
    require_admin(&state, ctx.user_id, ctx.organization_id)
        .await
        .map_err(|(_, e)| AppError::Forbidden(e.0.error.clone()))?;

    let valid_roles = ["owner", "admin", "member", "viewer"];
    if !valid_roles.contains(&req.role.as_str()) {
        return Err(AppError::BadRequest(format!(
            "Invalid role: {}. Must be one of: {:?}",
            req.role, valid_roles
        )));
    }

    if target_user_id == ctx.user_id {
        return Err(AppError::BadRequest(
            "Cannot change your own role".to_string(),
        ));
    }

    let before_role: Option<String> = sqlx::query_scalar(
        "SELECT role FROM memberships WHERE user_id = $1 AND organization_id = $2 AND status = 'active'",
    )
    .bind(target_user_id)
    .bind(ctx.organization_id)
    .fetch_optional(state.db.as_ref())
    .await?
    .ok_or_else(|| AppError::NotFound("Member not found".to_string()))?;

    let result = sqlx::query(
        r#"UPDATE memberships SET role = $1
           WHERE user_id = $2 AND organization_id = $3 AND status = 'active'"#,
    )
    .bind(&req.role)
    .bind(target_user_id)
    .bind(ctx.organization_id)
    .execute(state.db.as_ref())
    .await?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound("Member not found".to_string()));
    }

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::MemberRoleUpdated)
        .actor(ctx.user_id)
        .organization(ctx.organization_id)
        .resource("member", target_user_id)
        .details(serde_json::json!({
            "before": { "role": &before_role },
            "after": { "role": &req.role },
        }))
        .origin(
            &audit_origin.origin_type,
            &audit_origin.origin_ref,
            &audit_origin.origin_reason,
        )
        .caller(
            &audit_caller.caller_type,
            &audit_caller.key_label,
            &audit_caller.key_prefix,
        )
        .success()
        .log(&state.clickhouse)
        .await;

    Ok(Json(serde_json::json!({ "updated": true })))
}

async fn remove_member(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(target_user_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>> {
    let ctx = authenticate(&headers, &state)
        .await
        .map_err(|(_, e)| AppError::Auth(e.0.error.clone()))?;
    require_admin(&state, ctx.user_id, ctx.organization_id)
        .await
        .map_err(|(_, e)| AppError::Forbidden(e.0.error.clone()))?;

    if target_user_id == ctx.user_id {
        return Err(AppError::BadRequest(
            "Cannot remove yourself from the organization".to_string(),
        ));
    }

    let before_role: Option<String> = sqlx::query_scalar(
        "SELECT role FROM memberships WHERE user_id = $1 AND organization_id = $2 AND status = 'active'",
    )
    .bind(target_user_id)
    .bind(ctx.organization_id)
    .fetch_optional(state.db.as_ref())
    .await?
    .ok_or_else(|| AppError::NotFound("Member not found".to_string()))?;

    let result = sqlx::query(
        r#"UPDATE memberships SET status = 'suspended'
           WHERE user_id = $1 AND organization_id = $2 AND status = 'active'"#,
    )
    .bind(target_user_id)
    .bind(ctx.organization_id)
    .execute(state.db.as_ref())
    .await?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound("Member not found".to_string()));
    }

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::MemberRemoved)
        .actor(ctx.user_id)
        .organization(ctx.organization_id)
        .resource("member", target_user_id)
        .details(serde_json::json!({ "deleted": { "role": &before_role } }))
        .origin(
            &audit_origin.origin_type,
            &audit_origin.origin_ref,
            &audit_origin.origin_reason,
        )
        .caller(
            &audit_caller.caller_type,
            &audit_caller.key_label,
            &audit_caller.key_prefix,
        )
        .success()
        .log(&state.clickhouse)
        .await;

    Ok(Json(serde_json::json!({ "removed": true })))
}

// ---------------------------------------------------------------------------
// Public invite link acceptance (no auth required)
// ---------------------------------------------------------------------------

pub async fn accept_invite_link(
    State(state): State<Arc<WebsiteState>>,
    Path(token): Path<String>,
) -> axum::response::Redirect {
    let valid: Option<(Uuid,)> = sqlx::query_as(
        r#"SELECT id FROM organization_invitations
           WHERE invite_token = $1 AND accepted_at IS NULL AND expires_at > NOW()"#,
    )
    .bind(&token)
    .fetch_optional(state.db.as_ref())
    .await
    .unwrap_or_else(|e| {
        error!("DB error validating invite token: {}", e);
        None
    });

    if valid.is_some() {
        axum::response::Redirect::temporary(&format!(
            "/login?invite_token={}",
            urlencoding::encode(&token)
        ))
    } else {
        axum::response::Redirect::temporary("/login?error=Invalid%20or%20expired%20invite%20link")
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn generate_invite_token() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let bytes: Vec<u8> = (0..32).map(|_| rng.gen()).collect();
    hex::encode(bytes)
}
