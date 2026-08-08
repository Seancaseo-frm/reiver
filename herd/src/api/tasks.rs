use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use std::sync::Arc;
use uuid::Uuid;

use crate::a2a::types::*;
use crate::app_state::HerdState;

pub fn router() -> Router<Arc<HerdState>> {
    Router::new()
        .route("/tasks", get(list_tasks))
        .route("/tasks/{id}", get(get_task))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TasksQuery {
    pub page_size: Option<u32>,
    pub history_length: Option<u32>,
}

#[derive(Debug, clickhouse::Row, serde::Deserialize)]
struct ChTaskRow {
    task_id: Uuid,
    context_id: Option<Uuid>,
    status: String,
    metadata: String,
    artifacts: String,
    updated_at: chrono::DateTime<chrono::Utc>,
    #[allow(dead_code)]
    created_at: chrono::DateTime<chrono::Utc>,
}

fn parse_state(s: &str) -> TaskState {
    match s {
        "submitted" => TaskState::Submitted,
        "working" => TaskState::Working,
        "completed" => TaskState::Completed,
        "failed" => TaskState::Failed,
        "canceled" => TaskState::Canceled,
        "input-required" => TaskState::InputRequired,
        "rejected" => TaskState::Rejected,
        "auth-required" => TaskState::AuthRequired,
        _ => TaskState::Unknown,
    }
}

async fn list_tasks(
    State(state): State<Arc<HerdState>>,
    Query(query): Query<TasksQuery>,
) -> Result<Json<Vec<Task>>, (StatusCode, String)> {
    let page_size = query.page_size.unwrap_or(50).min(100);

    let rows: Vec<ChTaskRow> = state
        .clickhouse
        .query(
            "SELECT task_id, context_id, status, metadata, artifacts, updated_at, created_at
             FROM a2a_tasks FINAL
             ORDER BY created_at DESC
             LIMIT ?",
        )
        .bind(page_size)
        .fetch_all()
        .await
        .map_err(|e| {
            tracing::error!("ClickHouse query failed: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to list tasks".into(),
            )
        })?;

    let tasks: Vec<Task> = rows
        .into_iter()
        .map(|r| Task {
            id: r.task_id.to_string(),
            context_id: r.context_id.map(|c| c.to_string()),
            status: TaskStatus {
                state: parse_state(&r.status),
                message: None,
                timestamp: Some(r.updated_at),
            },
            artifacts: None,
            history: None,
            metadata: match serde_json::from_str(&r.metadata) {
                Ok(v) => Some(v),
                Err(e) => {
                    if r.metadata != "{}" && !r.metadata.is_empty() {
                        tracing::warn!(task_id = %r.task_id, "Corrupt task metadata JSON: {}", e);
                    }
                    None
                }
            },
        })
        .collect();

    Ok(Json(tasks))
}

async fn get_task(
    State(state): State<Arc<HerdState>>,
    Path(id): Path<String>,
    Query(query): Query<TasksQuery>,
) -> Result<Json<Task>, (StatusCode, String)> {
    let history_length = query.history_length.unwrap_or(50);

    if uuid::Uuid::parse_str(&id).is_err() {
        return Err((
            StatusCode::BAD_REQUEST,
            "Invalid task ID: must be a valid UUID".into(),
        ));
    }

    let row: ChTaskRow = state
        .clickhouse
        .query(
            "SELECT task_id, context_id, status, metadata, artifacts, updated_at, created_at
             FROM a2a_tasks FINAL
             WHERE task_id = ?
             LIMIT 1",
        )
        .bind(&id)
        .fetch_optional()
        .await
        .map_err(|e| {
            tracing::error!("ClickHouse query failed: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to get task".into(),
            )
        })?
        .ok_or((StatusCode::NOT_FOUND, "Task not found".into()))?;

    #[derive(Debug, clickhouse::Row, serde::Deserialize)]
    struct MsgRow {
        message_id: Uuid,
        #[allow(dead_code)]
        task_id: Uuid,
        context_id: Option<Uuid>,
        role: String,
        parts: String,
        reference_task_ids: Vec<Uuid>,
        metadata: String,
        #[allow(dead_code)]
        created_at: chrono::DateTime<chrono::Utc>,
    }

    let messages: Vec<MsgRow> = state
        .clickhouse
        .query(
            "SELECT message_id, task_id, context_id, role, parts, reference_task_ids, metadata, created_at
             FROM a2a_messages
             WHERE task_id = ?
             ORDER BY created_at DESC
             LIMIT ?",
        )
        .bind(&id)
        .bind(history_length)
        .fetch_all()
        .await
        .map_err(|e| {
            tracing::error!("ClickHouse message history query failed: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Failed to get message history".into())
        })?;

    let history: Vec<Message> = messages
        .into_iter()
        .rev()
        .map(|m| Message {
            message_id: m.message_id.to_string(),
            context_id: m.context_id.map(|c| c.to_string()),
            task_id: Some(id.clone()),
            role: if m.role == "agent" { Role::Agent } else { Role::User },
            parts: serde_json::from_str(&m.parts).unwrap_or_else(|e| {
                tracing::warn!(message_id = %m.message_id, "Corrupt message parts JSON: {}", e);
                vec![]
            }),
            metadata: match serde_json::from_str(&m.metadata) {
                Ok(v) => Some(v),
                Err(e) => {
                    tracing::warn!(message_id = %m.message_id, "Corrupt message metadata JSON: {}", e);
                    None
                }
            },
            extensions: None,
            reference_task_ids: if m.reference_task_ids.is_empty() {
                None
            } else {
                Some(m.reference_task_ids.iter().map(|u| u.to_string()).collect())
            },
        })
        .collect();

    let task = Task {
        id: row.task_id.to_string(),
        context_id: row.context_id.map(|c| c.to_string()),
        status: TaskStatus {
            state: parse_state(&row.status),
            message: None,
            timestamp: Some(row.updated_at),
        },
        artifacts: match serde_json::from_str(&row.artifacts) {
            Ok(v) => Some(v),
            Err(e) => {
                if row.artifacts != "[]" && !row.artifacts.is_empty() {
                    tracing::warn!(task_id = %row.task_id, "Corrupt task artifacts JSON: {}", e);
                }
                None
            }
        },
        history: if history.is_empty() {
            None
        } else {
            Some(history)
        },
        metadata: match serde_json::from_str(&row.metadata) {
            Ok(v) => Some(v),
            Err(e) => {
                if row.metadata != "{}" && !row.metadata.is_empty() {
                    tracing::warn!(task_id = %row.task_id, "Corrupt task metadata JSON: {}", e);
                }
                None
            }
        },
    };

    Ok(Json(task))
}
