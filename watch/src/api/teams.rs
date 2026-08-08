//! Microsoft Teams integrations API endpoints
//!
//! Provides endpoints for configuring and managing Microsoft Teams alerting integrations
//! Uses the unified notification_channels table with channel_type = 'teams'

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::Json,
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info};
use uuid::Uuid;

use reiver_core::audit::{AuditCaller, AuditEventBuilder, AuditEventType, AuditOrigin};

use crate::app_state::WatchState;
use crate::error::{AppError, Result};

const CHANNEL_TYPE: &str = "teams";

pub fn create_teams_router() -> Router<Arc<WatchState>> {
    Router::new()
        .route(
            "/integrations",
            get(list_integrations).post(create_integration),
        )
        .route(
            "/integrations/{id}",
            get(get_integration)
                .put(update_integration)
                .delete(delete_integration),
        )
}

#[derive(Debug, Serialize)]
pub struct TeamsIntegration {
    pub id: Uuid,
    pub project_id: Uuid,
    pub name: String,
    pub webhook_url: String, // Masked in responses
    pub enabled: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, sqlx::FromRow)]
struct ChannelRow {
    id: Uuid,
    project_id: Uuid,
    name: String,
    config: serde_json::Value,
    enabled: bool,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<ChannelRow> for TeamsIntegration {
    fn from(row: ChannelRow) -> Self {
        let webhook_url = row
            .config
            .get("webhook_url")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let masked_url = if webhook_url.len() > 8 {
            format!("****{}", &webhook_url[webhook_url.len() - 8..])
        } else {
            "****".to_string()
        };

        Self {
            id: row.id,
            project_id: row.project_id,
            name: row.name,
            webhook_url: masked_url,
            enabled: row.enabled,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateTeamsIntegrationRequest {
    pub name: String,
    pub webhook_url: String,
    pub enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateTeamsIntegrationRequest {
    pub name: Option<String>,
    pub webhook_url: Option<String>,
    pub enabled: Option<bool>,
}

/// List all Microsoft Teams integrations for a project
async fn list_integrations(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<TeamsIntegration>>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    let rows = sqlx::query_as::<_, ChannelRow>(
        r#"
        SELECT id, project_id, name, config, enabled, created_at, updated_at
        FROM notification_channels
        WHERE project_id = $1 AND channel_type = $2
        ORDER BY created_at DESC
        "#,
    )
    .bind(project_id)
    .bind(CHANNEL_TYPE)
    .fetch_all(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to list Teams integrations: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?;

    let integrations: Vec<TeamsIntegration> = rows.into_iter().map(|row| row.into()).collect();
    Ok(Json(integrations))
}

/// Create a new Microsoft Teams integration
async fn create_integration(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<CreateTeamsIntegrationRequest>,
) -> Result<Json<TeamsIntegration>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    if payload.webhook_url.is_empty() {
        return Err(AppError::Validation(
            "webhook_url cannot be empty".to_string(),
        ));
    }

    if !payload.webhook_url.contains("webhook.office.com")
        && !payload.webhook_url.contains("office365.com")
        && !payload.webhook_url.contains("microsoft.com")
    {
        return Err(AppError::Validation(
            "webhook_url must be a valid Microsoft Teams webhook URL".to_string(),
        ));
    }

    let config = serde_json::json!({
        "webhook_url": payload.webhook_url
    });

    let row = sqlx::query_as::<_, ChannelRow>(
        r#"
        INSERT INTO notification_channels (project_id, name, channel_type, config, enabled)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, project_id, name, config, enabled, created_at, updated_at
        "#,
    )
    .bind(project_id)
    .bind(&payload.name)
    .bind(CHANNEL_TYPE)
    .bind(&config)
    .bind(payload.enabled.unwrap_or(true))
    .fetch_one(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to create Teams integration: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?;

    let integration: TeamsIntegration = row.into();
    info!(
        "Created Teams integration: id={}, project_id={}",
        integration.id, project_id
    );

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::IntegrationCreated)
        .resource("teams", integration.id)
        .details(serde_json::json!({ "created": { "name": &payload.name } }))
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

    Ok(Json(integration))
}

/// Get a specific Microsoft Teams integration
async fn get_integration(
    State(state): State<Arc<WatchState>>,
    Path(id): Path<Uuid>,
) -> Result<Json<TeamsIntegration>> {
    let row = sqlx::query_as::<_, ChannelRow>(
        r#"
        SELECT id, project_id, name, config, enabled, created_at, updated_at
        FROM notification_channels
        WHERE id = $1 AND channel_type = $2
        "#,
    )
    .bind(id)
    .bind(CHANNEL_TYPE)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to get Teams integration: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?
    .ok_or_else(|| AppError::NotFound("Teams integration not found".to_string()))?;

    Ok(Json(row.into()))
}

/// Update a Microsoft Teams integration
async fn update_integration(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(payload): Json<UpdateTeamsIntegrationRequest>,
) -> Result<Json<TeamsIntegration>> {
    if let Some(ref url) = payload.webhook_url {
        if !url.contains("webhook.office.com")
            && !url.contains("office365.com")
            && !url.contains("microsoft.com")
        {
            return Err(AppError::Validation(
                "webhook_url must be a valid Microsoft Teams webhook URL".to_string(),
            ));
        }
    }

    let existing = sqlx::query_as::<_, ChannelRow>(
        "SELECT id, project_id, name, config, enabled, created_at, updated_at FROM notification_channels WHERE id = $1 AND channel_type = $2"
    )
    .bind(id)
    .bind(CHANNEL_TYPE)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?
    .ok_or_else(|| AppError::NotFound("Teams integration not found".to_string()))?;

    let mut config = existing.config.clone();
    if let Some(ref url) = payload.webhook_url {
        config["webhook_url"] = serde_json::json!(url);
    }

    let row = sqlx::query_as::<_, ChannelRow>(
        r#"
        UPDATE notification_channels
        SET name = COALESCE($1, name),
            config = $2,
            enabled = COALESCE($3, enabled),
            updated_at = NOW()
        WHERE id = $4 AND channel_type = $5
        RETURNING id, project_id, name, config, enabled, created_at, updated_at
        "#,
    )
    .bind(payload.name.as_deref())
    .bind(&config)
    .bind(payload.enabled)
    .bind(id)
    .bind(CHANNEL_TYPE)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to update Teams integration: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?
    .ok_or_else(|| AppError::NotFound("Teams integration not found".to_string()))?;

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::IntegrationUpdated)
        .resource("teams", id)
        .details(serde_json::json!({
            "before": { "name": &existing.name, "enabled": existing.enabled },
            "after": { "name": &row.name, "enabled": row.enabled }
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

    Ok(Json(row.into()))
}

/// Delete a Microsoft Teams integration
async fn delete_integration(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<StatusCode> {
    let deleted_row = sqlx::query_as::<_, ChannelRow>(
        "SELECT id, project_id, name, config, enabled, created_at, updated_at FROM notification_channels WHERE id = $1 AND channel_type = $2"
    )
    .bind(id)
    .bind(CHANNEL_TYPE)
    .fetch_optional(&*state.db)
    .await
    .ok()
    .flatten();

    let result =
        sqlx::query("DELETE FROM notification_channels WHERE id = $1 AND channel_type = $2")
            .bind(id)
            .bind(CHANNEL_TYPE)
            .execute(&*state.db)
            .await
            .map_err(|e| {
                error!("Failed to delete Teams integration: {}", e);
                AppError::Internal(anyhow::anyhow!("Database error: {}", e))
            })?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound(
            "Teams integration not found".to_string(),
        ));
    }

    info!("Deleted Teams integration: id={}", id);

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::IntegrationDeleted)
        .resource("teams", id)
        .details(
            serde_json::json!({ "deleted": { "name": deleted_row.as_ref().map(|r| &r.name) } }),
        )
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

    Ok(StatusCode::NO_CONTENT)
}
