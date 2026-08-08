//! PagerDuty integrations API endpoints
//!
//! Provides endpoints for configuring and managing PagerDuty alerting integrations
//! Uses the unified notification_channels table with channel_type = 'pagerduty'

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

const CHANNEL_TYPE: &str = "pagerduty";

pub fn create_pagerduty_router() -> Router<Arc<WatchState>> {
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
pub struct PagerDutyIntegration {
    pub id: Uuid,
    pub project_id: Uuid,
    pub name: String,
    pub routing_key: String, // Masked in responses
    pub service_id: Option<String>,
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

impl From<ChannelRow> for PagerDutyIntegration {
    fn from(row: ChannelRow) -> Self {
        let routing_key = row
            .config
            .get("routing_key")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let masked_key = if routing_key.len() > 4 {
            format!("****{}", &routing_key[routing_key.len() - 4..])
        } else {
            "****".to_string()
        };

        let service_id = row
            .config
            .get("service_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        Self {
            id: row.id,
            project_id: row.project_id,
            name: row.name,
            routing_key: masked_key,
            service_id,
            enabled: row.enabled,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CreatePagerDutyIntegrationRequest {
    pub name: String,
    pub routing_key: String,
    pub service_id: Option<String>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct UpdatePagerDutyIntegrationRequest {
    pub name: Option<String>,
    pub routing_key: Option<String>,
    pub service_id: Option<String>,
    pub enabled: Option<bool>,
}

/// List all PagerDuty integrations for a project
async fn list_integrations(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<PagerDutyIntegration>>> {
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
        error!("Failed to list PagerDuty integrations: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?;

    let integrations: Vec<PagerDutyIntegration> = rows.into_iter().map(|row| row.into()).collect();
    Ok(Json(integrations))
}

/// Create a new PagerDuty integration
async fn create_integration(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<CreatePagerDutyIntegrationRequest>,
) -> Result<Json<PagerDutyIntegration>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    if payload.routing_key.is_empty() {
        return Err(AppError::Validation(
            "routing_key cannot be empty".to_string(),
        ));
    }

    let mut config = serde_json::json!({
        "routing_key": payload.routing_key
    });
    if let Some(ref service_id) = payload.service_id {
        config["service_id"] = serde_json::json!(service_id);
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
        error!("Failed to create PagerDuty integration: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?;

    let integration: PagerDutyIntegration = row.into();
    info!(
        "Created PagerDuty integration: id={}, project_id={}",
        integration.id, project_id
    );

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::IntegrationCreated)
        .resource("pagerduty", integration.id)
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

/// Get a specific PagerDuty integration
async fn get_integration(
    State(state): State<Arc<WatchState>>,
    Path(id): Path<Uuid>,
) -> Result<Json<PagerDutyIntegration>> {
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
        error!("Failed to get PagerDuty integration: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?
    .ok_or_else(|| AppError::NotFound("PagerDuty integration not found".to_string()))?;

    Ok(Json(row.into()))
}

/// Update a PagerDuty integration
async fn update_integration(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(payload): Json<UpdatePagerDutyIntegrationRequest>,
) -> Result<Json<PagerDutyIntegration>> {
    let existing = sqlx::query_as::<_, ChannelRow>(
        "SELECT id, project_id, name, config, enabled, created_at, updated_at FROM notification_channels WHERE id = $1 AND channel_type = $2"
    )
    .bind(id)
    .bind(CHANNEL_TYPE)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?
    .ok_or_else(|| AppError::NotFound("PagerDuty integration not found".to_string()))?;

    let mut config = existing.config.clone();
    if let Some(ref key) = payload.routing_key {
        config["routing_key"] = serde_json::json!(key);
    }
    if let Some(ref service_id) = payload.service_id {
        config["service_id"] = serde_json::json!(service_id);
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
        error!("Failed to update PagerDuty integration: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?
    .ok_or_else(|| AppError::NotFound("PagerDuty integration not found".to_string()))?;

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::IntegrationUpdated)
        .resource("pagerduty", id)
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

/// Delete a PagerDuty integration
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
                error!("Failed to delete PagerDuty integration: {}", e);
                AppError::Internal(anyhow::anyhow!("Database error: {}", e))
            })?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound(
            "PagerDuty integration not found".to_string(),
        ));
    }

    info!("Deleted PagerDuty integration: id={}", id);

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::IntegrationDeleted)
        .resource("pagerduty", id)
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
