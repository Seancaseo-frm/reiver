//! Secret slot resolution — shared between Flow HTTP handlers and MCP actions.
//!
//! The `resolve_secret_slot` function atomically consumes a filled slot and
//! returns the decrypted secret. It lives in core so both the Flow service
//! (which owns the HTTP endpoints) and the MCP action registry (which runs
//! in-process inside Flow) can call it without a circular dependency.

use crate::audit::{AuditEventBuilder, AuditEventType, AuditOrigin};
use crate::clickhouse_db::ClickHousePool;
use crate::crypto::{RotatingSecretEncryptor, SecretString};
use crate::error::{AppError, Result};
use sqlx::PgPool;
use uuid::Uuid;

/// Atomically resolve and consume a filled secret slot.
///
/// Returns the decrypted secret wrapped in `SecretString` to prevent
/// accidental logging. The slot transitions to `consumed` and cannot be
/// reused.
pub async fn resolve_secret_slot(
    db: &PgPool,
    ch: &ClickHousePool,
    encryptor: &RotatingSecretEncryptor,
    slot_id: Uuid,
    project_id: Uuid,
    origin: Option<&AuditOrigin>,
) -> Result<SecretString> {
    #[derive(sqlx::FromRow)]
    struct SlotRow {
        encrypted_value: Option<String>,
    }

    let row: Option<SlotRow> = sqlx::query_as(
        r#"
        UPDATE secret_slots
        SET status = 'consumed', consumed_at = now()
        WHERE id = $1 AND project_id = $2 AND status = 'filled' AND expires_at > now()
        RETURNING encrypted_value
        "#,
    )
    .bind(slot_id)
    .bind(project_id)
    .fetch_optional(db)
    .await?;

    let row = row.ok_or_else(|| {
        AppError::BadRequest("Secret slot not found, not filled, or expired".into())
    })?;

    let encrypted = row
        .encrypted_value
        .ok_or_else(|| AppError::External("Slot marked as filled but has no value".into()))?;

    let plaintext = encryptor
        .decrypt(&encrypted)
        .map_err(|e| AppError::External(format!("Failed to decrypt slot value: {e}")))?;

    let mut audit = AuditEventBuilder::new(AuditEventType::SecretSlotConsumed)
        .resource("secret_slot", slot_id)
        .details(serde_json::json!({ "project_id": project_id }));
    if let Some(o) = origin {
        audit = audit.origin(&o.origin_type, &o.origin_ref, &o.origin_reason);
    }
    audit.log(ch).await;

    Ok(SecretString::new(plaintext))
}
