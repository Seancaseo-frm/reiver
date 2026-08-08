//! Secret Slots API
//!
//! Single-use, time-limited opaque references for depositing secrets without
//! exposing them to the AI agent's LLM context. The agent creates a slot and
//! the UI renders a secure deposit form inline; the user submits the secret
//! directly to the backend. The agent only ever holds an opaque slot ID.

use axum::{
    extract::{Path, State},
    http::HeaderMap,
    routing::post,
    Json, Router,
};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::Arc;
use uuid::Uuid;

use crate::api::{extract_organization_id, extract_project_id, extract_user_id};
use crate::app_state::FlowState;
use crate::audit::{AuditCaller, AuditEventBuilder, AuditEventType, AuditOrigin};
use crate::error::{AppError, Result};

const SLOT_TTL_SECONDS: i64 = 300; // 5 minutes

// ═══════════════════════════════════════════════════════════════════════════
// Router
// ═══════════════════════════════════════════════════════════════════════════

pub fn create_secret_slots_router() -> Router<Arc<FlowState>> {
    Router::new()
        .route("/", post(create_slot))
        .route("/deposit/{slot_id}", post(deposit_secret))
}

// ═══════════════════════════════════════════════════════════════════════════
// Create Slot
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Deserialize)]
pub struct CreateSlotRequest {
    pub purpose: String,
    #[serde(default)]
    pub provider: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CreateSlotResponse {
    pub slot_id: Uuid,
    pub expires_at: DateTime<Utc>,
}

async fn create_slot(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    Json(req): Json<CreateSlotRequest>,
) -> Result<Json<CreateSlotResponse>> {
    let project_id = extract_project_id(&headers)?;
    let user_id = extract_user_id(&headers)?;

    let slot_id = Uuid::new_v4();
    let expires_at = Utc::now() + Duration::seconds(SLOT_TTL_SECONDS);

    sqlx::query(
        r#"
        INSERT INTO secret_slots (id, project_id, created_by, purpose, provider, expires_at)
        VALUES ($1, $2, $3, $4, $5, $6)
        "#,
    )
    .bind(slot_id)
    .bind(project_id)
    .bind(user_id)
    .bind(&req.purpose)
    .bind(&req.provider)
    .bind(expires_at)
    .execute(state.db.as_ref())
    .await?;

    let org_id = extract_organization_id(&headers);
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::SecretSlotCreated)
        .user(user_id)
        .resource("secret_slot", slot_id)
        .project(&project_id.to_string())
        .details(serde_json::json!({
            "created": {
                "purpose": &req.purpose,
                "provider": &req.provider,
            }
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
        );
    if let Some(oid) = org_id {
        audit = audit.organization(oid);
    }
    audit.log(&state.clickhouse).await;

    Ok(Json(CreateSlotResponse {
        slot_id,
        expires_at,
    }))
}

// ═══════════════════════════════════════════════════════════════════════════
// Deposit Secret
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Deserialize)]
pub struct DepositRequest {
    pub value: String,
}

async fn deposit_secret(
    State(state): State<Arc<FlowState>>,
    Path(slot_id): Path<Uuid>,
    headers: HeaderMap,
    Json(req): Json<DepositRequest>,
) -> Result<Json<serde_json::Value>> {
    let project_id = extract_project_id(&headers)?;
    let user_id = extract_user_id(&headers)?;

    if req.value.is_empty() {
        return Err(AppError::BadRequest("Secret value cannot be empty".into()));
    }

    let encrypted = state
        .encryptor
        .encrypt(&req.value)
        .map_err(|e| AppError::External(e.to_string()))?;

    let result = sqlx::query(
        r#"
        UPDATE secret_slots
        SET encrypted_value = $1, status = 'filled', filled_at = now()
        WHERE id = $2 AND project_id = $3 AND status = 'pending' AND expires_at > now()
        "#,
    )
    .bind(&encrypted)
    .bind(slot_id)
    .bind(project_id)
    .execute(state.db.as_ref())
    .await?;

    if result.rows_affected() == 0 {
        return Err(AppError::Gone(
            "Slot has already been used or has expired".into(),
        ));
    }

    let org_id = extract_organization_id(&headers);
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::SecretSlotFilled)
        .user(user_id)
        .resource("secret_slot", slot_id)
        .project(&project_id.to_string())
        .details(serde_json::json!({
            "project_id": project_id,
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
        );
    if let Some(oid) = org_id {
        audit = audit.organization(oid);
    }
    audit.log(&state.clickhouse).await;

    Ok(Json(serde_json::json!({ "ok": true })))
}

// ═══════════════════════════════════════════════════════════════════════════
// Slot Resolution (delegates to core)
// ═══════════════════════════════════════════════════════════════════════════

pub use reiver_core::secret_slots::resolve_secret_slot;

/// Check if a slot has been filled (non-consuming query for polling).
pub async fn is_slot_filled(db: &PgPool, slot_id: Uuid) -> Result<bool> {
    let filled: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM secret_slots WHERE id = $1 AND status = 'filled')",
    )
    .bind(slot_id)
    .fetch_one(db)
    .await?;

    Ok(filled)
}

// ═══════════════════════════════════════════════════════════════════════════
// Expiry cleanup
// ═══════════════════════════════════════════════════════════════════════════

/// Expire pending slots past their TTL and hard-delete terminal slots older
/// than 24 hours.
pub async fn cleanup_expired_slots(db: &PgPool) {
    let expired = sqlx::query(
        r#"
        UPDATE secret_slots
        SET status = 'expired'
        WHERE status = 'pending' AND expires_at < now()
        "#,
    )
    .execute(db)
    .await;

    if let Ok(result) = &expired {
        let count = result.rows_affected();
        if count > 0 {
            tracing::info!(count, "Expired stale secret slots");
        }
    }

    let deleted = sqlx::query(
        r#"
        DELETE FROM secret_slots
        WHERE status IN ('consumed', 'expired')
          AND created_at < now() - interval '24 hours'
        "#,
    )
    .execute(db)
    .await;

    if let Ok(result) = &deleted {
        let count = result.rows_affected();
        if count > 0 {
            tracing::info!(count, "Hard-deleted old terminal secret slots");
        }
    }
}
