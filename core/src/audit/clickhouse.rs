//! ClickHouse-backed audit event logging.
//!
//! Provides:
//! - `AuditRow` — the ClickHouse row struct matching `reiver.audit_events`
//! - `insert_audit_event` — single-row insert helper

use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

use crate::clickhouse_db::ClickHousePool;

// ─────────────────────────────────────────────────────────────────────────────
// Row struct
// ─────────────────────────────────────────────────────────────────────────────

#[derive(clickhouse::Row, Serialize, Clone, Debug)]
pub struct AuditRow {
    pub event_id: String,
    pub project_id: String,
    pub event_type: String,
    pub action: String,
    pub caller_type: String,
    pub caller_user_id: String,
    pub caller_key_label: String,
    pub caller_key_prefix: String,
    pub service: String,
    pub http_method: String,
    pub http_path: String,
    pub http_status: u16,
    pub source_id: String,
    pub prompt_config_name: String,
    pub prompt_config_id: String,
    pub prompt_version_id: String,
    pub prompt_version_number: u32,
    pub rendered_system_prompt: String,
    pub prompt_variables: String,
    pub model_used: String,
    pub total_input_tokens: u64,
    pub total_output_tokens: u64,
    pub total_turns: u32,
    pub tool_calls_log: String,
    pub mcp_tool_name: String,
    pub mcp_tool_arguments: String,
    pub mcp_tool_success: u8,
    pub mcp_tool_error: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
    pub duration_ms: u64,
    pub organization_id: String,
    pub actor_id: String,
    pub ip_address: String,
    pub user_agent: String,
    pub resource_type: String,
    pub resource_id: String,
    pub details: String,
    pub success: u8,
    pub error_message: String,
    pub origin_type: String,
    pub origin_ref: String,
    pub origin_reason: String,
}

impl Default for AuditRow {
    fn default() -> Self {
        Self {
            event_id: Uuid::new_v4().to_string(),
            project_id: String::new(),
            event_type: String::new(),
            action: String::new(),
            caller_type: String::new(),
            caller_user_id: String::new(),
            caller_key_label: String::new(),
            caller_key_prefix: String::new(),
            service: String::new(),
            http_method: String::new(),
            http_path: String::new(),
            http_status: 0,
            source_id: String::new(),
            prompt_config_name: String::new(),
            prompt_config_id: String::new(),
            prompt_version_id: String::new(),
            prompt_version_number: 0,
            rendered_system_prompt: String::new(),
            prompt_variables: String::new(),
            model_used: String::new(),
            total_input_tokens: 0,
            total_output_tokens: 0,
            total_turns: 0,
            tool_calls_log: "[]".to_string(),
            mcp_tool_name: String::new(),
            mcp_tool_arguments: String::new(),
            mcp_tool_success: 1,
            mcp_tool_error: String::new(),
            timestamp: Utc::now(),
            duration_ms: 0,
            organization_id: String::new(),
            actor_id: String::new(),
            ip_address: String::new(),
            user_agent: String::new(),
            resource_type: String::new(),
            resource_id: String::new(),
            details: String::new(),
            success: 1,
            error_message: String::new(),
            origin_type: String::new(),
            origin_ref: String::new(),
            origin_reason: String::new(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Insert helper
// ─────────────────────────────────────────────────────────────────────────────

pub async fn insert_audit_event(ch: &ClickHousePool, row: AuditRow) {
    if let Err(e) = insert_audit_event_inner(ch, row).await {
        tracing::warn!(error = %e, "Failed to insert audit event into ClickHouse");
    }
}

async fn insert_audit_event_inner(ch: &ClickHousePool, row: AuditRow) -> anyhow::Result<()> {
    let mut ins = ch.insert::<AuditRow>("audit_events").await?;
    ins.write(&row).await?;
    ins.end().await?;
    Ok(())
}
