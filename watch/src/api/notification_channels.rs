//! Unified notification channels API
//!
//! Manages notification channels (Slack, PagerDuty, Teams, Discord, Webhook)

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::Json,
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;
use std::sync::Arc;
use tracing::{error, info};
use uuid::Uuid;

use reiver_core::audit::{AuditCaller, AuditEventBuilder, AuditEventType, AuditOrigin};

use crate::app_state::WatchState;
use crate::error::{AppError, Result};

pub fn create_notification_channels_router() -> Router<Arc<WatchState>> {
    Router::new()
        .route("/", get(list_channels).post(create_channel))
        .route(
            "/{id}",
            get(get_channel).put(update_channel).delete(delete_channel),
        )
}

#[derive(Debug, Serialize)]
pub struct NotificationChannel {
    pub id: Uuid,
    pub project_id: Uuid,
    pub name: String,
    pub channel_type: String,
    pub config: Value, // Masked in responses
    pub enabled: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Deserialize)]
pub struct CreateChannelRequest {
    pub name: String,
    pub channel_type: String,
    pub config: Value,
    pub enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateChannelRequest {
    pub name: Option<String>,
    pub config: Option<Value>,
    pub enabled: Option<bool>,
}

fn mask_config(config: &Value, channel_type: &str) -> Value {
    let mut masked = config.clone();

    // Mask sensitive fields based on channel type
    match channel_type {
        "slack" | "teams" | "discord" | "webhook" => {
            if let Some(url) = masked.get("webhook_url").and_then(|v| v.as_str()) {
                if url.len() > 12 {
                    masked["webhook_url"] = Value::String(format!("****{}", &url[url.len() - 8..]));
                } else {
                    masked["webhook_url"] = Value::String("****".to_string());
                }
            }
            if let Some(url) = masked.get("url").and_then(|v| v.as_str()) {
                if url.len() > 12 {
                    masked["url"] = Value::String(format!("****{}", &url[url.len() - 8..]));
                } else {
                    masked["url"] = Value::String("****".to_string());
                }
            }
        }
        "pagerduty" => {
            if let Some(key) = masked.get("routing_key").and_then(|v| v.as_str()) {
                if key.len() > 8 {
                    masked["routing_key"] = Value::String(format!("****{}", &key[key.len() - 4..]));
                } else {
                    masked["routing_key"] = Value::String("****".to_string());
                }
            }
        }
        _ => {}
    }

    masked
}

fn validate_config(channel_type: &str, config: &Value) -> std::result::Result<(), String> {
    match channel_type {
        "slack" => {
            let url = config
                .get("webhook_url")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if !url.starts_with("https://hooks.slack.com/") {
                return Err(
                    "Slack webhook_url must start with https://hooks.slack.com/".to_string()
                );
            }
        }
        "pagerduty" => {
            if config.get("routing_key").and_then(|v| v.as_str()).is_none() {
                return Err("PagerDuty config must include routing_key".to_string());
            }
        }
        "teams" => {
            let url = config
                .get("webhook_url")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if !url.starts_with("https://") {
                return Err("Teams webhook_url must be a valid HTTPS URL".to_string());
            }
        }
        "discord" => {
            let url = config
                .get("webhook_url")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if !url.starts_with("https://discord.com/api/webhooks/")
                && !url.starts_with("https://discordapp.com/api/webhooks/")
            {
                return Err("Discord webhook_url must be a valid Discord webhook URL".to_string());
            }
        }
        "webhook" => {
            let url = config.get("url").and_then(|v| v.as_str()).unwrap_or("");
            if !url.starts_with("https://") && !url.starts_with("http://") {
                return Err("Webhook url must be a valid HTTP(S) URL".to_string());
            }
        }
        _ => {
            return Err(format!("Unknown channel type: {}", channel_type));
        }
    }
    Ok(())
}

/// List all notification channels for a project
async fn list_channels(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<NotificationChannel>>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    let rows = sqlx::query(
        r#"
        SELECT id, project_id, name, channel_type, config, enabled, created_at, updated_at
        FROM notification_channels
        WHERE project_id = $1
        ORDER BY created_at DESC
        "#,
    )
    .bind(project_id)
    .fetch_all(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to list notification channels: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?;

    let channels: Vec<NotificationChannel> = rows
        .iter()
        .map(|row| {
            let channel_type: String = row.get("channel_type");
            let config: Value = row.get("config");
            NotificationChannel {
                id: row.get("id"),
                project_id: row.get("project_id"),
                name: row.get("name"),
                channel_type: channel_type.clone(),
                config: mask_config(&config, &channel_type),
                enabled: row.get("enabled"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            }
        })
        .collect();

    Ok(Json(channels))
}

/// Create a new notification channel
async fn create_channel(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<CreateChannelRequest>,
) -> Result<Json<NotificationChannel>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    // Validate config based on channel type
    validate_config(&payload.channel_type, &payload.config).map_err(|e| AppError::Validation(e))?;

    let row = sqlx::query(
        r#"
        INSERT INTO notification_channels (project_id, name, channel_type, config, enabled)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, project_id, name, channel_type, config, enabled, created_at, updated_at
        "#,
    )
    .bind(project_id)
    .bind(&payload.name)
    .bind(&payload.channel_type)
    .bind(&payload.config)
    .bind(payload.enabled.unwrap_or(true))
    .fetch_one(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to create notification channel: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?;

    let channel = NotificationChannel {
        id: row.get("id"),
        project_id: row.get("project_id"),
        name: row.get("name"),
        channel_type: row.get("channel_type"),
        config: mask_config(&row.get("config"), &payload.channel_type),
        enabled: row.get("enabled"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    };

    let organization_id =
        sqlx::query_scalar::<_, Uuid>("SELECT organization_id FROM projects WHERE id = $1")
            .bind(project_id)
            .fetch_optional(&*state.db)
            .await
            .ok()
            .flatten();

    info!(
        "Created notification channel: id={}, type={}",
        channel.id, channel.channel_type
    );

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::NotificationChannelCreated)
        .resource("notification_channel", channel.id)
        .details(serde_json::json!({ "created": { "name": &payload.name, "channel_type": &payload.channel_type } }))
        .origin(&audit_origin.origin_type, &audit_origin.origin_ref, &audit_origin.origin_reason)
        .caller(&audit_caller.caller_type, &audit_caller.key_label, &audit_caller.key_prefix)
        .success();
    if let Some(org_id) = organization_id {
        audit = audit.organization(org_id);
    }
    audit.log(&state.clickhouse).await;

    Ok(Json(channel))
}

/// Get a specific notification channel
async fn get_channel(
    State(state): State<Arc<WatchState>>,
    Path(id): Path<Uuid>,
) -> Result<Json<NotificationChannel>> {
    let row = sqlx::query(
        r#"
        SELECT id, project_id, name, channel_type, config, enabled, created_at, updated_at
        FROM notification_channels
        WHERE id = $1
        "#,
    )
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to get notification channel: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?
    .ok_or_else(|| AppError::NotFound("Notification channel not found".to_string()))?;

    let channel_type: String = row.get("channel_type");
    let config: Value = row.get("config");

    Ok(Json(NotificationChannel {
        id: row.get("id"),
        project_id: row.get("project_id"),
        name: row.get("name"),
        channel_type: channel_type.clone(),
        config: mask_config(&config, &channel_type),
        enabled: row.get("enabled"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }))
}

/// Update a notification channel
async fn update_channel(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(payload): Json<UpdateChannelRequest>,
) -> Result<Json<NotificationChannel>> {
    // Get current channel to know the type for validation
    let before_row = sqlx::query("SELECT id, project_id, name, channel_type, config, enabled, created_at, updated_at FROM notification_channels WHERE id = $1")
        .bind(id)
        .fetch_optional(&*state.db)
        .await
        .ok()
        .flatten();
    let current = sqlx::query("SELECT channel_type FROM notification_channels WHERE id = $1")
        .bind(id)
        .fetch_optional(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?
        .ok_or_else(|| AppError::NotFound("Notification channel not found".to_string()))?;

    let channel_type: String = current.get("channel_type");

    // Validate new config if provided
    if let Some(ref config) = payload.config {
        validate_config(&channel_type, config).map_err(|e| AppError::Validation(e))?;
    }

    let row = sqlx::query(
        r#"
        UPDATE notification_channels
        SET 
            name = COALESCE($1, name),
            config = COALESCE($2, config),
            enabled = COALESCE($3, enabled),
            updated_at = NOW()
        WHERE id = $4
        RETURNING id, project_id, name, channel_type, config, enabled, created_at, updated_at
        "#,
    )
    .bind(payload.name.as_deref())
    .bind(&payload.config)
    .bind(payload.enabled)
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to update notification channel: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?
    .ok_or_else(|| AppError::NotFound("Notification channel not found".to_string()))?;

    let channel_type: String = row.get("channel_type");
    let config: Value = row.get("config");

    let nc_project_id: Option<Uuid> = before_row.as_ref().map(|r| r.get("project_id"));
    let organization_id = match nc_project_id {
        Some(pid) => {
            sqlx::query_scalar::<_, Uuid>("SELECT organization_id FROM projects WHERE id = $1")
                .bind(pid)
                .fetch_optional(&*state.db)
                .await
                .ok()
                .flatten()
        }
        None => None,
    };

    let before_name: Option<String> = before_row.as_ref().map(|r| r.get("name"));
    let before_enabled: Option<bool> = before_row.as_ref().map(|r| r.get("enabled"));
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::NotificationChannelUpdated)
        .resource("notification_channel", id)
        .details(serde_json::json!({
            "before": { "name": before_name, "enabled": before_enabled },
            "after": { "name": row.get::<String, _>("name"), "enabled": row.get::<bool, _>("enabled") }
        }))
        .origin(&audit_origin.origin_type, &audit_origin.origin_ref, &audit_origin.origin_reason)
        .caller(&audit_caller.caller_type, &audit_caller.key_label, &audit_caller.key_prefix)
        .success();
    if let Some(org_id) = organization_id {
        audit = audit.organization(org_id);
    }
    audit.log(&state.clickhouse).await;

    Ok(Json(NotificationChannel {
        id: row.get("id"),
        project_id: row.get("project_id"),
        name: row.get("name"),
        channel_type: channel_type.clone(),
        config: mask_config(&config, &channel_type),
        enabled: row.get("enabled"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }))
}

/// Delete a notification channel
async fn delete_channel(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<StatusCode> {
    let deleted_row = sqlx::query(
        "SELECT name, channel_type, project_id FROM notification_channels WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .ok()
    .flatten();

    let result = sqlx::query("DELETE FROM notification_channels WHERE id = $1")
        .bind(id)
        .execute(&*state.db)
        .await
        .map_err(|e| {
            error!("Failed to delete notification channel: {}", e);
            AppError::Internal(anyhow::anyhow!("Database error: {}", e))
        })?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound(
            "Notification channel not found".to_string(),
        ));
    }

    let organization_id = match deleted_row.as_ref().map(|r| r.get::<Uuid, _>("project_id")) {
        Some(pid) => {
            sqlx::query_scalar::<_, Uuid>("SELECT organization_id FROM projects WHERE id = $1")
                .bind(pid)
                .fetch_optional(&*state.db)
                .await
                .ok()
                .flatten()
        }
        None => None,
    };

    info!("Deleted notification channel: id={}", id);

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::NotificationChannelDeleted)
        .resource("notification_channel", id)
        .details(serde_json::json!({ "deleted": { "name": deleted_row.as_ref().map(|r| r.get::<String, _>("name")), "channel_type": deleted_row.as_ref().map(|r| r.get::<String, _>("channel_type")) } }))
        .origin(&audit_origin.origin_type, &audit_origin.origin_ref, &audit_origin.origin_reason)
        .caller(&audit_caller.caller_type, &audit_caller.key_label, &audit_caller.key_prefix)
        .success();
    if let Some(org_id) = organization_id {
        audit = audit.organization(org_id);
    }
    audit.log(&state.clickhouse).await;

    Ok(StatusCode::NO_CONTENT)
}
