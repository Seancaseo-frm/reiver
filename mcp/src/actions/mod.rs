pub mod alerting;
pub mod attachments;
pub mod billing;
pub mod dashboards;
pub mod facade;
pub mod flow;
pub mod internal;
pub mod knowledge_base;
pub mod projects;
pub mod types;
pub mod watch;

use crate::action::ActionContext;
use crate::registry::ActionRegistry;

/// Register the five facade tools with the registry.
pub fn register_all(registry: &mut ActionRegistry) {
    facade::register(registry);
}

/// Resolve a secret slot in-process using the DB and encryptor from the
/// action context. Returns the plaintext secret value.
///
/// This only works when the action is running inside the Flow agent loop
/// (where `ctx.db` and `ctx.encryptor` are populated). The standalone MCP
/// binary does not support slot resolution.
pub async fn resolve_slot(ctx: &ActionContext, slot_id: &str) -> anyhow::Result<String> {
    let db = ctx
        .db
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("No database available for slot resolution"))?;
    let ch = ctx
        .clickhouse
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("No ClickHouse pool available for slot resolution"))?;
    let encryptor = ctx
        .encryptor
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("No encryptor available for slot resolution"))?;
    let slot_uuid = uuid::Uuid::parse_str(slot_id)?;
    let secret = reiver_core::secret_slots::resolve_secret_slot(
        db,
        ch,
        encryptor.as_ref(),
        slot_uuid,
        ctx.project_id,
        None,
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to resolve slot: {e}"))?;
    Ok(secret.expose().to_string())
}
