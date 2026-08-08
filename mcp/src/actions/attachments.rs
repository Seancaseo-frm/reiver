use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};

const MAX_TEXT_CONTENT_BYTES: usize = 100 * 1024; // 100KB

#[derive(Deserialize, JsonSchema)]
pub struct GetAttachmentInput {
    /// The UUID of the attachment to read
    pub attachment_id: String,
}

#[derive(Serialize)]
pub struct GetAttachmentOutput {
    pub filename: String,
    pub content_type: String,
    pub size_bytes: i64,
    pub content: String,
}

pub struct GetAttachment;

#[derive(sqlx::FromRow)]
struct AttachmentRow {
    filename: String,
    content_type: String,
    size_bytes: i64,
    storage_key: String,
}

#[async_trait]
impl PlatformAction for GetAttachment {
    type Input = GetAttachmentInput;
    type Output = GetAttachmentOutput;

    fn name(&self) -> &'static str {
        "get_attachment"
    }
    fn description(&self) -> &'static str {
        "Read the content of a file attachment uploaded to the conversation"
    }
    fn required_scope(&self) -> String {
        "project:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let db = ctx
            .db
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No database available for attachment lookup"))?;

        let storage = ctx
            .asset_storage
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No asset storage available"))?;

        let attachment_id: uuid::Uuid = input
            .attachment_id
            .parse()
            .map_err(|_| anyhow::anyhow!("Invalid attachment_id: not a valid UUID"))?;

        let row: AttachmentRow = sqlx::query_as(
            "SELECT filename, content_type, size_bytes, storage_key \
             FROM agent_attachments \
             WHERE id = $1 AND project_id = $2",
        )
        .bind(attachment_id)
        .bind(ctx.project_id)
        .fetch_optional(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Attachment not found"))?;

        let content = if row.content_type.starts_with("image/") {
            storage
                .get_url(&row.storage_key, std::time::Duration::from_secs(3600))
                .await
                .map_err(|e| anyhow::anyhow!("Failed to generate URL: {e}"))?
        } else {
            let bytes = storage
                .get(&row.storage_key)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to read attachment: {e}"))?;

            let text = String::from_utf8_lossy(&bytes);
            if text.len() > MAX_TEXT_CONTENT_BYTES {
                format!(
                    "{}...\n\n[Content truncated at {}KB of {}KB total]",
                    &text[..MAX_TEXT_CONTENT_BYTES],
                    MAX_TEXT_CONTENT_BYTES / 1024,
                    row.size_bytes / 1024,
                )
            } else {
                text.into_owned()
            }
        };

        Ok(GetAttachmentOutput {
            filename: row.filename,
            content_type: row.content_type,
            size_bytes: row.size_bytes,
            content,
        })
    }
}
