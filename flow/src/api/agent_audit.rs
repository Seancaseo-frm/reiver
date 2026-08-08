//! Agent-specific audit helpers.
//!
//! `AuditPromptSnapshot` + `resolve_and_render_for_audit`: resolves a prompt
//! config by name and renders its system prompt for audit logging.

use std::collections::HashMap;
use uuid::Uuid;

use crate::app_state::FlowState;

// ─────────────────────────────────────────────────────────────────────────────
// Prompt snapshot for audit
// ─────────────────────────────────────────────────────────────────────────────

pub struct AuditPromptSnapshot {
    pub config_name: String,
    pub config_id: Uuid,
    pub version_id: Uuid,
    pub version_number: i32,
    pub rendered_system_prompt: String,
}

/// Resolve a prompt config by name and render its system prompt with the given
/// variables. Used for audit logging — captures the exact text sent to the LLM.
pub async fn resolve_and_render_for_audit(
    state: &FlowState,
    project_id: Uuid,
    config_name: &str,
    variables: &HashMap<String, serde_json::Value>,
) -> Option<AuditPromptSnapshot> {
    use axum::http::HeaderMap;
    use reiver_core::llm::template::compile_prompt;

    let mut headers = HeaderMap::new();
    headers.insert("x-reiver-prompt-config", config_name.parse().ok()?);

    let (resolution, version_config) = crate::gateway::prompt_resolver::resolve_prompt_config(
        state.prompt_store.as_ref(),
        project_id,
        &headers,
        Some(config_name),
    )
    .await?;

    let system_prompt = version_config.system_prompt.as_deref().unwrap_or("");

    let rendered =
        compile_prompt(system_prompt, variables).unwrap_or_else(|_| system_prompt.to_string());

    let version_number: Option<i32> =
        sqlx::query_scalar("SELECT version_number FROM llm_prompt_versions WHERE id = $1")
            .bind(resolution.version_id)
            .fetch_optional(state.db.as_ref())
            .await
            .ok()
            .flatten();

    Some(AuditPromptSnapshot {
        config_name: config_name.to_string(),
        config_id: resolution.config_id,
        version_id: resolution.version_id,
        version_number: version_number.unwrap_or(0),
        rendered_system_prompt: rendered,
    })
}
