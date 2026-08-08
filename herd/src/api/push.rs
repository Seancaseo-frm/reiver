use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, post},
    Json, Router,
};
use serde::Deserialize;
use std::sync::Arc;
use uuid::Uuid;

use crate::a2a::types::*;
use crate::app_state::HerdState;
use crate::routing_cache::CachedPushConfig;

pub fn router() -> Router<Arc<HerdState>> {
    Router::new()
        .route(
            "/tasks/{task_id}/pushNotificationConfigs",
            post(create_config),
        )
        .route(
            "/tasks/{task_id}/pushNotificationConfigs",
            get(list_configs),
        )
        .route(
            "/tasks/{task_id}/pushNotificationConfigs/{config_id}",
            get(get_config),
        )
        .route(
            "/tasks/{task_id}/pushNotificationConfigs/{config_id}",
            delete(delete_config),
        )
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreatePushConfigBody {
    pub url: String,
    pub authentication: Option<AuthenticationInfo>,
}

async fn create_config(
    State(state): State<Arc<HerdState>>,
    Path(task_id): Path<String>,
    Json(body): Json<CreatePushConfigBody>,
) -> Result<(StatusCode, Json<TaskPushNotificationConfig>), (StatusCode, String)> {
    let config_id = Uuid::now_v7();

    if !body.url.starts_with("https://") && !body.url.starts_with("http://") {
        return Err((
            StatusCode::BAD_REQUEST,
            "url must be a valid HTTP(S) URL".into(),
        ));
    }

    // We use a placeholder agent_id; in production the agent_id is resolved from the caller
    sqlx::query(
        "INSERT INTO a2a_push_configs (id, task_id, agent_id, webhook_url, auth_scheme, auth_credentials)
         VALUES ($1, $2, (SELECT id FROM a2a_agents LIMIT 1), $3, $4, $5)",
    )
    .bind(config_id)
    .bind(&task_id)
    .bind(&body.url)
    .bind(body.authentication.as_ref().map(|a| &a.scheme))
    .bind(
        body.authentication
            .as_ref()
            .and_then(|a| a.credentials.as_deref()),
    )
    .execute(state.db.as_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to create push config: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to create push config".into(),
        )
    })?;

    state.routing_cache.insert_push_config(
        task_id.clone(),
        CachedPushConfig {
            id: config_id,
            webhook_url: body.url.clone(),
            auth_scheme: body.authentication.as_ref().map(|a| a.scheme.clone()),
            auth_credentials: body
                .authentication
                .as_ref()
                .and_then(|a| a.credentials.clone()),
        },
    );

    Ok((
        StatusCode::CREATED,
        Json(TaskPushNotificationConfig {
            task_id,
            push_notification_config: PushNotificationConfig {
                id: Some(config_id.to_string()),
                url: body.url,
                token: None,
                authentication: body.authentication,
            },
        }),
    ))
}

async fn list_configs(
    State(state): State<Arc<HerdState>>,
    Path(task_id): Path<String>,
) -> Result<Json<Vec<TaskPushNotificationConfig>>, (StatusCode, String)> {
    let rows: Vec<(Uuid, String, String, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT id, task_id, webhook_url, auth_scheme, auth_credentials
         FROM a2a_push_configs WHERE task_id = $1 ORDER BY created_at",
    )
    .bind(&task_id)
    .fetch_all(state.db.as_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to list push configs: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to list push configs".into(),
        )
    })?;

    let configs: Vec<TaskPushNotificationConfig> = rows
        .into_iter()
        .map(|(id, tid, url, scheme, creds)| TaskPushNotificationConfig {
            task_id: tid,
            push_notification_config: PushNotificationConfig {
                id: Some(id.to_string()),
                url,
                token: None,
                authentication: scheme.map(|s| AuthenticationInfo {
                    scheme: s,
                    credentials: creds,
                }),
            },
        })
        .collect();

    Ok(Json(configs))
}

async fn get_config(
    State(state): State<Arc<HerdState>>,
    Path((task_id, config_id)): Path<(String, Uuid)>,
) -> Result<Json<TaskPushNotificationConfig>, (StatusCode, String)> {
    let row: Option<(Uuid, String, String, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT id, task_id, webhook_url, auth_scheme, auth_credentials
         FROM a2a_push_configs WHERE id = $1 AND task_id = $2",
    )
    .bind(config_id)
    .bind(&task_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to get push config: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to get push config".into(),
        )
    })?;

    let (id, tid, url, scheme, creds) =
        row.ok_or((StatusCode::NOT_FOUND, "Push config not found".into()))?;

    Ok(Json(TaskPushNotificationConfig {
        task_id: tid,
        push_notification_config: PushNotificationConfig {
            id: Some(id.to_string()),
            url,
            token: None,
            authentication: scheme.map(|s| AuthenticationInfo {
                scheme: s,
                credentials: creds,
            }),
        },
    }))
}

async fn delete_config(
    State(state): State<Arc<HerdState>>,
    Path((task_id, config_id)): Path<(String, Uuid)>,
) -> Result<StatusCode, (StatusCode, String)> {
    let result = sqlx::query("DELETE FROM a2a_push_configs WHERE id = $1 AND task_id = $2")
        .bind(config_id)
        .bind(&task_id)
        .execute(state.db.as_ref())
        .await
        .map_err(|e| {
            tracing::error!("Failed to delete push config: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to delete push config".into(),
            )
        })?;

    if result.rows_affected() == 0 {
        return Err((StatusCode::NOT_FOUND, "Push config not found".into()));
    }

    state.routing_cache.remove_push_config(&task_id, config_id);

    Ok(StatusCode::NO_CONTENT)
}
