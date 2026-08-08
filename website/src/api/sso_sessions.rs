//! SSO Sessions API
//!
//! Endpoints for managing SSO sessions with revocation and SLO support.

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::Json,
    routing::{get, post},
    Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tracing::{error, info};
use uuid::Uuid;

use crate::app_state::WebsiteState;
use crate::audit::{AuditCaller, AuditEventBuilder, AuditEventType, AuditOrigin};
use crate::auth::extract_user_id;
use crate::authorization::require_user_admin_access;
use crate::error::{AppError, Result};

// Use the shared authorization function for session admin checks
// The require_user_admin_access function handles:
// - Self-access (user managing their own sessions)
// - Admin access (admin managing users in their organizations)
async fn require_session_admin(
    db: &sqlx::PgPool,
    current_user_id: Uuid,
    target_user_id: Option<Uuid>,
) -> Result<()> {
    require_user_admin_access(db, current_user_id, target_user_id).await
}

pub fn create_sso_sessions_router() -> Router<Arc<WebsiteState>> {
    Router::new()
        .route("/", get(list_sessions))
        .route("/my", get(list_my_sessions))
        .route("/{session_id}", get(get_session))
        .route("/{session_id}/revoke", post(revoke_session))
        .route("/revoke-all", post(revoke_all_sessions))
        .route("/logout", post(logout))
}

// ============================================================================
// Types
// ============================================================================

/// SSO session stored in database
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct SsoSession {
    pub id: Uuid,
    pub user_id: Uuid,
    pub sso_config_id: Uuid,
    pub session_token_hash: String,
    pub idp_session_id: Option<String>,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub last_activity_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub revocation_reason: Option<String>,
}

/// Session summary for listing (without sensitive data)
#[derive(Debug, Serialize)]
pub struct SessionSummary {
    pub id: Uuid,
    pub user_id: Uuid,
    pub sso_config_id: Uuid,
    pub sso_config_name: Option<String>,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub last_activity_at: DateTime<Utc>,
    pub is_active: bool,
    pub is_current: bool,
}

#[derive(Debug, sqlx::FromRow)]
struct SessionWithConfig {
    id: Uuid,
    user_id: Uuid,
    sso_config_id: Uuid,
    sso_config_name: Option<String>,
    ip_address: Option<String>,
    user_agent: Option<String>,
    created_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    last_activity_at: DateTime<Utc>,
    revoked_at: Option<DateTime<Utc>>,
}

/// Query parameters for listing sessions
#[derive(Debug, Deserialize)]
pub struct ListSessionsParams {
    pub user_id: Option<Uuid>,
    pub sso_config_id: Option<Uuid>,
    pub include_revoked: Option<bool>,
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub offset: i64,
}

fn default_limit() -> i64 {
    50
}

/// Response for listing sessions
#[derive(Debug, Serialize)]
pub struct ListSessionsResponse {
    pub sessions: Vec<SessionSummary>,
    pub total: i64,
}

/// Request to revoke a session
#[derive(Debug, Deserialize)]
pub struct RevokeSessionRequest {
    pub reason: Option<String>,
}

// ============================================================================
// Endpoints
// ============================================================================

/// List all SSO sessions (admin)
async fn list_sessions(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Query(params): Query<ListSessionsParams>,
) -> Result<Json<ListSessionsResponse>> {
    // Require authentication and admin access
    let current_user_id = extract_user_id(&headers, &state.config.jwt_secret)?;
    require_session_admin(&state.db, current_user_id, params.user_id).await?;

    let include_revoked = params.include_revoked.unwrap_or(false);

    let sessions = sqlx::query_as::<_, SessionWithConfig>(
        r#"
        SELECT 
            s.id, s.user_id, s.sso_config_id, c.name as sso_config_name,
            s.ip_address::text, s.user_agent, s.created_at, s.expires_at,
            s.last_activity_at, s.revoked_at
        FROM sso_sessions s
        LEFT JOIN sso_configurations c ON s.sso_config_id = c.id
        WHERE ($1::uuid IS NULL OR s.user_id = $1)
          AND ($2::uuid IS NULL OR s.sso_config_id = $2)
          AND ($3 OR s.revoked_at IS NULL)
        ORDER BY s.created_at DESC
        LIMIT $4 OFFSET $5
        "#,
    )
    .bind(params.user_id)
    .bind(params.sso_config_id)
    .bind(include_revoked)
    .bind(params.limit)
    .bind(params.offset)
    .fetch_all(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to list sessions: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error"))
    })?;

    // Get total count
    let total: (i64,) = sqlx::query_as(
        r#"
        SELECT COUNT(*)
        FROM sso_sessions s
        WHERE ($1::uuid IS NULL OR s.user_id = $1)
          AND ($2::uuid IS NULL OR s.sso_config_id = $2)
          AND ($3 OR s.revoked_at IS NULL)
        "#,
    )
    .bind(params.user_id)
    .bind(params.sso_config_id)
    .bind(include_revoked)
    .fetch_one(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let now = Utc::now();
    let summaries: Vec<SessionSummary> = sessions
        .into_iter()
        .map(|s| SessionSummary {
            id: s.id,
            user_id: s.user_id,
            sso_config_id: s.sso_config_id,
            sso_config_name: s.sso_config_name,
            ip_address: s.ip_address,
            user_agent: s.user_agent,
            created_at: s.created_at,
            expires_at: s.expires_at,
            last_activity_at: s.last_activity_at,
            is_active: s.revoked_at.is_none() && s.expires_at > now,
            is_current: false, // Will be set by caller if needed
        })
        .collect();

    Ok(Json(ListSessionsResponse {
        sessions: summaries,
        total: total.0,
    }))
}

/// List current user's sessions
async fn list_my_sessions(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Query(params): Query<ListSessionsParams>,
) -> Result<Json<ListSessionsResponse>> {
    let current_user_id = extract_user_id(&headers, &state.config.jwt_secret)?;
    let mut modified_params = params;
    modified_params.user_id = Some(current_user_id);
    list_sessions(State(state), headers, Query(modified_params)).await
}

/// Get a specific session
async fn get_session(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(session_id): Path<Uuid>,
) -> Result<Json<SsoSession>> {
    // Require authentication
    let current_user_id = extract_user_id(&headers, &state.config.jwt_secret)?;

    let session = sqlx::query_as::<_, SsoSession>(
        r#"
        SELECT id, user_id, sso_config_id, session_token_hash, idp_session_id,
               ip_address::text, user_agent, created_at, expires_at,
               last_activity_at, revoked_at, revocation_reason
        FROM sso_sessions
        WHERE id = $1
        "#,
    )
    .bind(session_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?
    .ok_or_else(|| AppError::NotFound("Session not found".to_string()))?;

    // Verify admin access to this session's user
    require_session_admin(&state.db, current_user_id, Some(session.user_id)).await?;

    Ok(Json(session))
}

/// Revoke a specific session
async fn revoke_session(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(session_id): Path<Uuid>,
    Json(req): Json<RevokeSessionRequest>,
) -> Result<StatusCode> {
    // Require authentication
    let current_user_id = extract_user_id(&headers, &state.config.jwt_secret)?;

    // Get session to check ownership/admin access
    let session_user: Option<(Uuid,)> =
        sqlx::query_as("SELECT user_id FROM sso_sessions WHERE id = $1")
            .bind(session_id)
            .fetch_optional(&*state.db)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let target_user_id = session_user
        .ok_or_else(|| AppError::NotFound("Session not found".to_string()))?
        .0;

    // Verify admin access to this session's user
    require_session_admin(&state.db, current_user_id, Some(target_user_id)).await?;

    let reason = req.reason.unwrap_or_else(|| "admin_revoke".to_string());

    let result = sqlx::query(
        r#"
        UPDATE sso_sessions
        SET revoked_at = NOW(), revocation_reason = $1
        WHERE id = $2 AND revoked_at IS NULL
        "#,
    )
    .bind(&reason)
    .bind(session_id)
    .execute(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound(
            "Session not found or already revoked".to_string(),
        ));
    }

    // Log audit event
    let organization_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT c.organization_id FROM sso_sessions s JOIN sso_configurations c ON s.sso_config_id = c.id WHERE s.id = $1"
    )
    .bind(session_id)
    .fetch_one(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::SessionRevoked)
        .organization(organization_id)
        .resource("sso_session", session_id)
        .details(serde_json::json!({ "reason": reason }))
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

    info!("Revoked session: {}", session_id);
    Ok(StatusCode::NO_CONTENT)
}

/// Revoke all sessions for a user
#[derive(Debug, Deserialize)]
pub struct RevokeAllRequest {
    pub user_id: Uuid,
    pub reason: Option<String>,
}

async fn revoke_all_sessions(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(req): Json<RevokeAllRequest>,
) -> Result<Json<serde_json::Value>> {
    // Require authentication and admin access to the target user
    let current_user_id = extract_user_id(&headers, &state.config.jwt_secret)?;
    require_session_admin(&state.db, current_user_id, Some(req.user_id)).await?;

    let reason = req.reason.unwrap_or_else(|| "admin_revoke_all".to_string());

    let result = sqlx::query(
        r#"
        UPDATE sso_sessions
        SET revoked_at = NOW(), revocation_reason = $1
        WHERE user_id = $2 AND revoked_at IS NULL
        "#,
    )
    .bind(&reason)
    .bind(req.user_id)
    .execute(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    // Log audit event
    let organization_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT c.organization_id FROM sso_sessions s JOIN sso_configurations c ON s.sso_config_id = c.id WHERE s.user_id = $1 ORDER BY s.created_at DESC LIMIT 1"
    )
    .bind(req.user_id)
    .fetch_one(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::SessionsRevokedAll)
        .user(req.user_id)
        .organization(organization_id)
        .details(serde_json::json!({
            "reason": reason,
            "sessions_revoked": result.rows_affected()
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

    info!(
        "Revoked {} sessions for user {}",
        result.rows_affected(),
        req.user_id
    );

    Ok(Json(serde_json::json!({
        "success": true,
        "sessions_revoked": result.rows_affected()
    })))
}

/// Logout - revokes all current user's sessions
async fn logout(State(state): State<Arc<WebsiteState>>, headers: HeaderMap) -> Result<StatusCode> {
    let current_user_id = extract_user_id(&headers, &state.config.jwt_secret)?;

    // Revoke all active sessions for the user
    let result = sqlx::query(
        r#"
        UPDATE sso_sessions
        SET revoked_at = NOW(), revocation_reason = 'user_logout'
        WHERE user_id = $1 AND revoked_at IS NULL
        "#,
    )
    .bind(current_user_id)
    .execute(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    // Log audit event
    let organization_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT c.organization_id FROM sso_sessions s JOIN sso_configurations c ON s.sso_config_id = c.id WHERE s.user_id = $1 ORDER BY s.created_at DESC LIMIT 1"
    )
    .bind(current_user_id)
    .fetch_one(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::SsoLogout)
        .user(current_user_id)
        .organization(organization_id)
        .details(serde_json::json!({ "sessions_revoked": result.rows_affected() }))
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

    info!(
        "User {} logged out, {} sessions revoked",
        current_user_id,
        result.rows_affected()
    );

    Ok(StatusCode::NO_CONTENT)
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Create a new SSO session
pub async fn create_session(
    db: &sqlx::PgPool,
    user_id: Uuid,
    sso_config_id: Uuid,
    session_token: &str,
    idp_session_id: Option<&str>,
    ip_address: Option<&str>,
    user_agent: Option<&str>,
    expires_in_hours: i64,
) -> Result<Uuid> {
    // Hash the session token
    let mut hasher = Sha256::new();
    hasher.update(session_token.as_bytes());
    let token_hash = hex::encode(hasher.finalize());

    let expires_at = Utc::now() + chrono::Duration::hours(expires_in_hours);

    let row: (Uuid,) = sqlx::query_as(
        r#"
        INSERT INTO sso_sessions (
            user_id, sso_config_id, session_token_hash, idp_session_id,
            ip_address, user_agent, expires_at
        ) VALUES ($1, $2, $3, $4, $5::inet, $6, $7)
        RETURNING id
        "#,
    )
    .bind(user_id)
    .bind(sso_config_id)
    .bind(&token_hash)
    .bind(idp_session_id)
    .bind(ip_address)
    .bind(user_agent)
    .bind(expires_at)
    .fetch_one(db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to create session: {}", e)))?;

    Ok(row.0)
}
