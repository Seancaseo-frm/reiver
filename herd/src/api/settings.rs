use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::{delete, get, post, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::app_state::HerdState;
use crate::db::DbPool;

pub fn router() -> Router<Arc<HerdState>> {
    Router::new()
        .route("/settings/verification", get(get_verification))
        .route("/settings/verification", put(put_verification))
        .route("/settings/verification", delete(delete_verification))
        .route(
            "/settings/verification/regenerate-secret",
            post(regenerate_secret),
        )
}

fn extract_org_id(headers: &HeaderMap) -> Result<Uuid, (StatusCode, String)> {
    headers
        .get("x-organization-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or((
            StatusCode::UNAUTHORIZED,
            "Missing or invalid X-Organization-Id header".into(),
        ))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VerificationSettingsResponse {
    pub verification_url: Option<String>,
    pub has_webhook_secret: bool,
}

async fn get_verification(
    State(state): State<Arc<HerdState>>,
    headers: HeaderMap,
) -> Result<Json<VerificationSettingsResponse>, (StatusCode, String)> {
    let org_id = extract_org_id(&headers)?;

    let row: Option<(Option<String>, Option<String>)> =
        sqlx::query_as("SELECT verification_url, webhook_secret FROM organizations WHERE id = $1")
            .bind(org_id)
            .fetch_optional(state.db.as_ref())
            .await
            .map_err(|e| {
                tracing::error!("Failed to fetch verification settings: {e}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to fetch settings".into(),
                )
            })?;

    let (url, secret) = row.ok_or((StatusCode::NOT_FOUND, "Organization not found".into()))?;

    Ok(Json(VerificationSettingsResponse {
        verification_url: url,
        has_webhook_secret: secret.is_some(),
    }))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutVerificationBody {
    pub verification_url: Option<String>,
    pub webhook_secret: Option<String>,
}

async fn put_verification(
    State(state): State<Arc<HerdState>>,
    headers: HeaderMap,
    Json(body): Json<PutVerificationBody>,
) -> Result<Json<VerificationSettingsResponse>, (StatusCode, String)> {
    let org_id = extract_org_id(&headers)?;

    if let Some(ref url) = body.verification_url {
        if !url.starts_with("https://") {
            return Err((
                StatusCode::BAD_REQUEST,
                "verification_url must use HTTPS".into(),
            ));
        }
    }

    if let Some(ref secret) = body.webhook_secret {
        sqlx::query(
            "UPDATE organizations SET verification_url = COALESCE($1, verification_url), webhook_secret = $2 WHERE id = $3",
        )
        .bind(&body.verification_url)
        .bind(secret)
        .bind(org_id)
        .execute(state.db.as_ref())
        .await
        .map_err(|e| {
            tracing::error!("Failed to update verification settings: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to update settings".into(),
            )
        })?;

        sync_org_webhook_secret(&state, state.db.as_ref(), org_id, Some(secret.clone())).await;
    } else {
        sqlx::query("UPDATE organizations SET verification_url = $1 WHERE id = $2")
            .bind(&body.verification_url)
            .bind(org_id)
            .execute(state.db.as_ref())
            .await
            .map_err(|e| {
                tracing::error!("Failed to update verification URL: {e}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to update settings".into(),
                )
            })?;
    }

    let has_secret: bool =
        sqlx::query_scalar("SELECT webhook_secret IS NOT NULL FROM organizations WHERE id = $1")
            .bind(org_id)
            .fetch_one(state.db.as_ref())
            .await
            .unwrap_or(false);

    Ok(Json(VerificationSettingsResponse {
        verification_url: body.verification_url,
        has_webhook_secret: has_secret,
    }))
}

async fn delete_verification(
    State(state): State<Arc<HerdState>>,
    headers: HeaderMap,
) -> Result<StatusCode, (StatusCode, String)> {
    let org_id = extract_org_id(&headers)?;

    sqlx::query(
        "UPDATE organizations SET verification_url = NULL, webhook_secret = NULL WHERE id = $1",
    )
    .bind(org_id)
    .execute(state.db.as_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to clear verification settings: {e}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to clear settings".into(),
        )
    })?;

    sync_org_webhook_secret(&state, state.db.as_ref(), org_id, None).await;

    Ok(StatusCode::NO_CONTENT)
}

fn generate_secret() -> String {
    use rand::RngExt;
    let bytes: [u8; 32] = rand::rng().random();
    hex::encode(bytes)
}

async fn sync_org_webhook_secret(
    state: &HerdState,
    db: &DbPool,
    org_id: Uuid,
    secret: Option<String>,
) {
    let agent_ids: Vec<Uuid> =
        sqlx::query_scalar("SELECT id FROM a2a_agents WHERE organization_id = $1")
            .bind(org_id)
            .fetch_all(db)
            .await
            .unwrap_or_default();

    state
        .routing_cache
        .set_webhook_secret_for_org(secret, &agent_ids);
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RegenerateSecretResponse {
    pub webhook_secret: String,
}

async fn regenerate_secret(
    State(state): State<Arc<HerdState>>,
    headers: HeaderMap,
) -> Result<Json<RegenerateSecretResponse>, (StatusCode, String)> {
    let org_id = extract_org_id(&headers)?;

    let new_secret = generate_secret();

    sqlx::query("UPDATE organizations SET webhook_secret = $1 WHERE id = $2")
        .bind(&new_secret)
        .bind(org_id)
        .execute(state.db.as_ref())
        .await
        .map_err(|e| {
            tracing::error!("Failed to regenerate webhook secret: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to regenerate secret".into(),
            )
        })?;

    sync_org_webhook_secret(&state, state.db.as_ref(), org_id, Some(new_secret.clone())).await;

    Ok(Json(RegenerateSecretResponse {
        webhook_secret: new_secret,
    }))
}
