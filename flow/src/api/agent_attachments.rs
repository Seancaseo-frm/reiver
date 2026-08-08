//! File attachment handling for the in-app agent.
//!
//! Manages multipart file uploads and building multimodal user content
//! that includes image and file attachment references.

use axum::{
    extract::{Multipart, State},
    http::HeaderMap,
    Json,
};
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

use crate::api::{extract_project_id, extract_user_id};
use crate::app_state::FlowState;
use crate::error::{AppError, Result};
use crate::gateway::types::{ContentPart, ImageUrl, MessageContent};

pub const MAX_ATTACHMENT_BYTES: usize = 20 * 1024 * 1024; // 20MB

pub const SUPPORTED_ATTACHMENT_TYPES: &[&str] = &[
    "image/png",
    "image/jpeg",
    "image/jpg",
    "image/webp",
    "image/gif",
    "application/pdf",
    "text/plain",
    "text/csv",
    "text/markdown",
    "text/x-sql",
    "application/json",
    "application/x-yaml",
    "text/yaml",
    "text/x-python",
    "text/x-rust",
    "text/x-go",
    "text/x-java",
    "text/javascript",
    "text/typescript",
    "text/html",
    "text/css",
    "application/xml",
    "text/xml",
];

pub fn is_supported_attachment_type(content_type: &str) -> bool {
    SUPPORTED_ATTACHMENT_TYPES.contains(&content_type) || content_type.starts_with("text/")
}

#[derive(Serialize)]
pub struct AttachmentResponse {
    pub id: Uuid,
    pub filename: String,
    pub content_type: String,
    pub size_bytes: i64,
}

/// Build message content for the user turn, incorporating any file attachments.
/// Images become `ContentPart::ImageUrl`; other files become metadata annotations
/// (the agent can use the `get` tool with `resource: "attachment"` to read them).
pub async fn build_user_content(
    state: &FlowState,
    project_id: Uuid,
    user_message: &str,
    attachment_ids: &Option<Vec<Uuid>>,
) -> MessageContent {
    let ids = match attachment_ids {
        Some(ids) if !ids.is_empty() => ids,
        _ => return MessageContent::Text(user_message.to_string()),
    };

    #[derive(sqlx::FromRow)]
    struct AttachmentRow {
        id: Uuid,
        filename: String,
        content_type: String,
        size_bytes: i64,
        storage_key: String,
    }

    let rows: Vec<AttachmentRow> = sqlx::query_as(
        "SELECT id, filename, content_type, size_bytes, storage_key \
         FROM agent_attachments \
         WHERE id = ANY($1) AND project_id = $2",
    )
    .bind(ids)
    .bind(project_id)
    .fetch_all(state.db.as_ref())
    .await
    .unwrap_or_default();

    if rows.is_empty() {
        return MessageContent::Text(user_message.to_string());
    }

    let mut parts: Vec<ContentPart> = Vec::new();
    let mut text_suffix = String::new();

    for row in &rows {
        if row.content_type.starts_with("image/") {
            let url = state
                .asset_storage
                .get_url(&row.storage_key, Duration::from_secs(3600))
                .await
                .unwrap_or_else(|_| format!("attachment://{}", row.id));
            parts.push(ContentPart::ImageUrl {
                image_url: ImageUrl { url, detail: None },
            });
        } else {
            use std::fmt::Write;
            write!(
                &mut text_suffix,
                "\n\n[Attached file: {} ({}, {:.1}KB) — use get tool with resource \"attachment\" and attachment_id \"{}\" to read its content]",
                row.filename,
                row.content_type,
                row.size_bytes as f64 / 1024.0,
                row.id,
            )
            .ok();
        }
    }

    let full_text = format!("{}{}", user_message, text_suffix);
    parts.insert(0, ContentPart::Text { text: full_text });

    MessageContent::Parts(parts)
}

#[tracing::instrument(
    name = "agent.attachments.upload",
    skip_all,
    fields(project_id, user_id)
)]
pub async fn upload_attachment(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Result<Json<AttachmentResponse>> {
    let project_id = extract_project_id(&headers)?;
    let user_id = extract_user_id(&headers)?;
    let span = tracing::Span::current();
    span.record("project_id", tracing::field::display(project_id));
    span.record("user_id", tracing::field::display(user_id));

    let field = match multipart.next_field().await {
        Ok(Some(f)) => f,
        Ok(None) => {
            return Err(AppError::BadRequest(
                "No file field in multipart body".into(),
            ))
        }
        Err(e) => return Err(AppError::BadRequest(format!("Invalid multipart data: {e}"))),
    };

    let filename = field.file_name().unwrap_or("unnamed").to_string();

    let content_type = field
        .content_type()
        .unwrap_or("application/octet-stream")
        .to_string();

    if !is_supported_attachment_type(&content_type) {
        return Err(AppError::BadRequest(format!(
            "Unsupported file type: {content_type}"
        )));
    }

    let data = match field.bytes().await {
        Ok(b) => b,
        Err(e) => {
            return Err(AppError::BadRequest(format!(
                "Failed to read file data: {e}"
            )))
        }
    };

    if data.len() > MAX_ATTACHMENT_BYTES {
        return Err(AppError::BadRequest(format!(
            "File too large: {} bytes exceeds limit of {} bytes",
            data.len(),
            MAX_ATTACHMENT_BYTES
        )));
    }

    let attachment_id = Uuid::new_v4();
    let storage_key = format!("agent/{}/{}/{}", project_id, attachment_id, filename);

    state
        .asset_storage
        .put(&storage_key, &data, &content_type)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to store attachment: {e}")))?;

    let size_bytes = data.len() as i64;

    sqlx::query(
        "INSERT INTO agent_attachments (id, project_id, user_id, filename, content_type, size_bytes, storage_key) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(attachment_id)
    .bind(project_id)
    .bind(user_id)
    .bind(&filename)
    .bind(&content_type)
    .bind(size_bytes)
    .bind(&storage_key)
    .execute(state.db.as_ref())
    .await?;

    Ok(Json(AttachmentResponse {
        id: attachment_id,
        filename,
        content_type,
        size_bytes,
    }))
}
