//! ServiceNow integrations API endpoints
//!
//! Provides endpoints for configuring and managing ServiceNow alerting integrations
//! Uses the unified notification_channels table with channel_type = 'servicenow'

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

const CHANNEL_TYPE: &str = "servicenow";

pub fn create_servicenow_router() -> Router<Arc<WatchState>> {
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
pub struct ServiceNowIntegration {
    pub id: Uuid,
    pub project_id: Uuid,
    pub name: String,
    pub instance_url: String,
    pub username: String, // Masked in responses
    pub enabled: bool,
    pub extra_config: Option<serde_json::Value>,
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

impl From<ChannelRow> for ServiceNowIntegration {
    fn from(row: ChannelRow) -> Self {
        let instance_url = row
            .config
            .get("instance_url")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let username = row
            .config
            .get("username")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        // Mask username in responses (show only first 3 characters)
        let masked_username = if username.len() > 3 {
            format!("{}***", &username[..3])
        } else {
            "***".to_string()
        };

        let extra_config = row.config.get("extra_config").cloned();

        Self {
            id: row.id,
            project_id: row.project_id,
            name: row.name,
            instance_url,
            username: masked_username,
            enabled: row.enabled,
            extra_config,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateServiceNowIntegrationRequest {
    pub name: String,
    pub instance_url: String,
    pub username: String,
    pub password: String,
    pub extra_config: Option<serde_json::Value>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateServiceNowIntegrationRequest {
    pub name: Option<String>,
    pub instance_url: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub extra_config: Option<serde_json::Value>,
    pub enabled: Option<bool>,
}

/// List all ServiceNow integrations for a project
async fn list_integrations(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<ServiceNowIntegration>>> {
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
        error!("Failed to list ServiceNow integrations: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?;

    let integrations: Vec<ServiceNowIntegration> = rows.into_iter().map(|row| row.into()).collect();
    Ok(Json(integrations))
}

/// Create a new ServiceNow integration
async fn create_integration(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<CreateServiceNowIntegrationRequest>,
) -> Result<Json<ServiceNowIntegration>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    if payload.instance_url.is_empty() {
        return Err(AppError::Validation(
            "instance_url cannot be empty".to_string(),
        ));
    }
    if payload.username.is_empty() {
        return Err(AppError::Validation("username cannot be empty".to_string()));
    }
    if payload.password.is_empty() {
        return Err(AppError::Validation("password cannot be empty".to_string()));
    }

    // Normalize instance URL (remove trailing slash)
    let instance_url = payload.instance_url.trim_end_matches('/').to_string();

    let mut config = serde_json::json!({
        "instance_url": instance_url,
        "username": payload.username,
        "password": payload.password  // In production, encrypt before storing
    });
    if let Some(ref extra) = payload.extra_config {
        config["extra_config"] = extra.clone();
    }

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
        error!("Failed to create ServiceNow integration: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?;

    let integration: ServiceNowIntegration = row.into();
    info!(
        "Created ServiceNow integration: id={}, project_id={}",
        integration.id, project_id
    );

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::IntegrationCreated)
        .resource("servicenow", integration.id)
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

/// Get a specific ServiceNow integration
async fn get_integration(
    State(state): State<Arc<WatchState>>,
    Path(id): Path<Uuid>,
) -> Result<Json<ServiceNowIntegration>> {
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
        error!("Failed to get ServiceNow integration: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?
    .ok_or_else(|| AppError::NotFound("ServiceNow integration not found".to_string()))?;

    Ok(Json(row.into()))
}

/// Update a ServiceNow integration
async fn update_integration(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(payload): Json<UpdateServiceNowIntegrationRequest>,
) -> Result<Json<ServiceNowIntegration>> {
    let existing = sqlx::query_as::<_, ChannelRow>(
        "SELECT id, project_id, name, config, enabled, created_at, updated_at FROM notification_channels WHERE id = $1 AND channel_type = $2"
    )
    .bind(id)
    .bind(CHANNEL_TYPE)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?
    .ok_or_else(|| AppError::NotFound("ServiceNow integration not found".to_string()))?;

    let mut config = existing.config.clone();
    if let Some(ref url) = payload.instance_url {
        config["instance_url"] = serde_json::json!(url.trim_end_matches('/'));
    }
    if let Some(ref username) = payload.username {
        config["username"] = serde_json::json!(username);
    }
    if let Some(ref password) = payload.password {
        config["password"] = serde_json::json!(password);
    }
    if let Some(ref extra) = payload.extra_config {
        config["extra_config"] = extra.clone();
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
        error!("Failed to update ServiceNow integration: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?
    .ok_or_else(|| AppError::NotFound("ServiceNow integration not found".to_string()))?;

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::IntegrationUpdated)
        .resource("servicenow", id)
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

/// Delete a ServiceNow integration
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
                error!("Failed to delete ServiceNow integration: {}", e);
                AppError::Internal(anyhow::anyhow!("Database error: {}", e))
            })?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound(
            "ServiceNow integration not found".to_string(),
        ));
    }

    info!("Deleted ServiceNow integration: id={}", id);

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::IntegrationDeleted)
        .resource("servicenow", id)
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
