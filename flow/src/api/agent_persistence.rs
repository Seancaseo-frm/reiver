//! Conversation and message persistence for the in-app agent.
//!
//! CRUD operations for conversations and messages, plus the save helper
//! used during agent loop execution.

use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use std::sync::Arc;
use uuid::Uuid;

use crate::api::{extract_project_id, extract_user_id};
use crate::app_state::FlowState;
use crate::error::{AppError, Result};

#[derive(Debug, Serialize, FromRow)]
pub struct Conversation {
    pub id: Uuid,
    pub project_id: Uuid,
    pub user_id: Uuid,
    pub title: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct Message {
    pub id: Uuid,
    pub conversation_id: Uuid,
    pub role: String,
    pub content: Option<String>,
    pub tool_calls: Option<serde_json::Value>,
    pub tool_call_id: Option<String>,
    pub tool_name: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct PaginationParams {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct CreateConversationRequest {
    pub title: Option<String>,
}

#[tracing::instrument(
    name = "agent.conversations.list",
    skip_all,
    fields(project_id, user_id)
)]
pub async fn list_conversations(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    Query(params): Query<PaginationParams>,
) -> Result<Json<Vec<Conversation>>> {
    let project_id = extract_project_id(&headers)?;
    let user_id = extract_user_id(&headers)?;
    let span = tracing::Span::current();
    span.record("project_id", tracing::field::display(project_id));
    span.record("user_id", tracing::field::display(user_id));
    let limit = params.limit.unwrap_or(50).min(100).max(1);
    let offset = params.offset.unwrap_or(0).max(0);

    let rows: Vec<Conversation> = sqlx::query_as(
        "SELECT id, project_id, user_id, title, created_at, updated_at \
         FROM agent_conversations \
         WHERE project_id = $1 AND user_id = $2 \
         ORDER BY updated_at DESC \
         LIMIT $3 OFFSET $4",
    )
    .bind(project_id)
    .bind(user_id)
    .bind(limit)
    .bind(offset)
    .fetch_all(state.db.as_ref())
    .await?;

    Ok(Json(rows))
}

#[tracing::instrument(
    name = "agent.conversations.create",
    skip_all,
    fields(project_id, user_id)
)]
pub async fn create_conversation(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    Json(req): Json<CreateConversationRequest>,
) -> Result<Json<Conversation>> {
    let project_id = extract_project_id(&headers)?;
    let user_id = extract_user_id(&headers)?;
    let span = tracing::Span::current();
    span.record("project_id", tracing::field::display(project_id));
    span.record("user_id", tracing::field::display(user_id));

    let row: Conversation = sqlx::query_as(
        "INSERT INTO agent_conversations (project_id, user_id, title) \
         VALUES ($1, $2, $3) \
         RETURNING id, project_id, user_id, title, created_at, updated_at",
    )
    .bind(project_id)
    .bind(user_id)
    .bind(req.title)
    .fetch_one(state.db.as_ref())
    .await?;

    Ok(Json(row))
}

#[tracing::instrument(name = "agent.conversations.delete", skip_all, fields(project_id, user_id, conversation_id = %conv_id))]
pub async fn delete_conversation(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    Path(conv_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>> {
    let project_id = extract_project_id(&headers)?;
    let user_id = extract_user_id(&headers)?;
    let span = tracing::Span::current();
    span.record("project_id", tracing::field::display(project_id));
    span.record("user_id", tracing::field::display(user_id));

    let result = sqlx::query(
        "DELETE FROM agent_conversations \
         WHERE id = $1 AND project_id = $2 AND user_id = $3",
    )
    .bind(conv_id)
    .bind(project_id)
    .bind(user_id)
    .execute(state.db.as_ref())
    .await?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound("Conversation not found".into()));
    }

    Ok(Json(serde_json::json!({ "deleted": true })))
}

#[tracing::instrument(name = "agent.messages.list", skip_all, fields(project_id, user_id, conversation_id = %conv_id))]
pub async fn list_messages(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    Path(conv_id): Path<Uuid>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<Vec<Message>>> {
    let project_id = extract_project_id(&headers)?;
    let user_id = extract_user_id(&headers)?;
    let span = tracing::Span::current();
    span.record("project_id", tracing::field::display(project_id));
    span.record("user_id", tracing::field::display(user_id));
    let limit = params.limit.unwrap_or(200).min(500).max(1);
    let offset = params.offset.unwrap_or(0).max(0);

    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM agent_conversations \
         WHERE id = $1 AND project_id = $2 AND user_id = $3)",
    )
    .bind(conv_id)
    .bind(project_id)
    .bind(user_id)
    .fetch_one(state.db.as_ref())
    .await?;

    if !exists {
        return Err(AppError::NotFound("Conversation not found".into()));
    }

    let rows: Vec<Message> = sqlx::query_as(
        "SELECT id, conversation_id, role, content, tool_calls, \
                tool_call_id, tool_name, metadata, created_at \
         FROM agent_messages \
         WHERE conversation_id = $1 \
         ORDER BY created_at ASC \
         LIMIT $2 OFFSET $3",
    )
    .bind(conv_id)
    .bind(limit)
    .bind(offset)
    .fetch_all(state.db.as_ref())
    .await?;

    Ok(Json(rows))
}

/// Save a message using an owned `Arc<DbPool>`, suitable for `tokio::spawn`.
#[allow(clippy::too_many_arguments)]
pub async fn save_message_owned(
    db: &reiver_core::db::DbPool,
    conversation_id: Uuid,
    role: &str,
    content: Option<&str>,
    tool_calls: Option<&serde_json::Value>,
    tool_call_id: Option<&str>,
    tool_name: Option<&str>,
    metadata: Option<serde_json::Value>,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO agent_messages \
         (conversation_id, role, content, tool_calls, tool_call_id, tool_name, metadata) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(conversation_id)
    .bind(role)
    .bind(content)
    .bind(tool_calls)
    .bind(tool_call_id)
    .bind(tool_name)
    .bind(metadata)
    .execute(db)
    .await?;
    Ok(())
}
