use axum::{
    extract::{Query, State},
    http::HeaderMap,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{error, instrument};
use uuid::Uuid;

use crate::api::auth_helpers::{authenticate, require_admin, ErrorResponse};
use crate::app_state::WebsiteState;

pub fn create_audit_router() -> Router<Arc<WebsiteState>> {
    Router::new().route("/events", get(list_audit_events))
}

#[derive(Debug, Deserialize)]
struct AuditQuery {
    event_types: Option<String>,
    resource_type: Option<String>,
    project_id: Option<String>,
    caller_type: Option<String>,
    actor_id: Option<String>,
    /// Match events where actor_id OR caller_user_id equals this UUID (org-scoped).
    user_id: Option<String>,
    caller_user_id: Option<String>,
    caller_key_label: Option<String>,
    /// Accepts pasted `dh_...suffix` from Agents tokens UI or a bare suffix.
    caller_key_prefix: Option<String>,
    service: Option<String>,
    from: Option<String>,
    to: Option<String>,
    limit: Option<u64>,
    offset: Option<u64>,
}

#[derive(Debug, clickhouse::Row, Deserialize)]
struct AuditEventRow {
    event_id: String,
    project_id: String,
    event_type: String,
    action: String,
    caller_type: String,
    caller_user_id: String,
    caller_key_label: String,
    caller_key_prefix: String,
    service: String,
    http_method: String,
    http_path: String,
    http_status: u16,
    source_id: String,
    prompt_config_name: String,
    prompt_config_id: String,
    prompt_version_id: String,
    prompt_version_number: u32,
    rendered_system_prompt: String,
    prompt_variables: String,
    model_used: String,
    total_input_tokens: u64,
    total_output_tokens: u64,
    total_turns: u32,
    tool_calls_log: String,
    mcp_tool_name: String,
    mcp_tool_arguments: String,
    mcp_tool_success: u8,
    mcp_tool_error: String,
    timestamp_str: String,
    duration_ms: u64,
    organization_id: String,
    actor_id: String,
    ip_address: String,
    user_agent: String,
    resource_type: String,
    resource_id: String,
    details: String,
    success: u8,
    error_message: String,
    origin_type: String,
    origin_ref: String,
    origin_reason: String,
}

#[derive(Debug, Serialize)]
struct AuditEvent {
    event_id: String,
    event_type: String,
    timestamp: String,
    caller_type: String,
    actor_id: String,
    caller_user_id: String,
    caller_key_label: String,
    caller_key_prefix: String,
    resource_type: String,
    resource_id: String,
    project_id: String,
    organization_id: String,
    details: String,
    success: u8,
    error_message: String,
    service: String,
    source_id: String,
    prompt_config_name: String,
    model_used: String,
    total_input_tokens: u64,
    total_output_tokens: u64,
    mcp_tool_name: String,
    mcp_tool_success: u8,
    duration_ms: u64,
    origin_type: String,
    origin_ref: String,
    origin_reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    actor_email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    caller_user_email: Option<String>,
}

impl From<AuditEventRow> for AuditEvent {
    fn from(row: AuditEventRow) -> Self {
        Self {
            event_id: row.event_id,
            event_type: row.event_type,
            timestamp: row.timestamp_str,
            caller_type: row.caller_type,
            actor_id: row.actor_id,
            caller_user_id: row.caller_user_id,
            caller_key_label: row.caller_key_label,
            caller_key_prefix: row.caller_key_prefix,
            resource_type: row.resource_type,
            resource_id: row.resource_id,
            project_id: row.project_id,
            organization_id: row.organization_id,
            details: row.details,
            success: row.success,
            error_message: row.error_message,
            service: row.service,
            source_id: row.source_id,
            prompt_config_name: row.prompt_config_name,
            model_used: row.model_used,
            total_input_tokens: row.total_input_tokens,
            total_output_tokens: row.total_output_tokens,
            mcp_tool_name: row.mcp_tool_name,
            mcp_tool_success: row.mcp_tool_success,
            duration_ms: row.duration_ms,
            origin_type: row.origin_type,
            origin_ref: row.origin_ref,
            origin_reason: row.origin_reason,
            actor_email: None,
            caller_user_email: None,
        }
    }
}

#[derive(Debug, Serialize)]
struct AuditEventsResponse {
    events: Vec<AuditEvent>,
    total: u64,
}

struct QueryBuilder {
    where_clauses: Vec<String>,
}

impl QueryBuilder {
    fn new() -> Self {
        Self {
            where_clauses: Vec::new(),
        }
    }

    fn add_eq(&mut self, column: &str, value: String) {
        self.where_clauses
            .push(format!("{} = '{}'", column, Self::escape(&value)));
    }

    fn add_in(&mut self, column: &str, values: Vec<String>) {
        let list: Vec<String> = values
            .iter()
            .map(|v| format!("'{}'", Self::escape(v)))
            .collect();
        self.where_clauses
            .push(format!("{} IN ({})", column, list.join(",")));
    }

    fn add_timestamp_gte(&mut self, value: &str) {
        self.where_clauses.push(format!(
            "timestamp >= parseDateTimeBestEffort('{}')",
            Self::escape(value)
        ));
    }

    fn add_timestamp_lte(&mut self, value: &str) {
        self.where_clauses.push(format!(
            "timestamp <= parseDateTimeBestEffort('{}')",
            Self::escape(value)
        ));
    }

    /// `(actor_id = v OR caller_user_id = v)` for a single UUID string.
    fn add_or_actor_caller_user(&mut self, uuid_str: &str) {
        let e = Self::escape(uuid_str);
        self.where_clauses
            .push(format!("(actor_id = '{}' OR caller_user_id = '{}')", e, e));
    }

    fn where_sql(&self) -> String {
        if self.where_clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", self.where_clauses.join(" AND "))
        }
    }

    fn escape(s: &str) -> String {
        s.replace('\\', "\\\\").replace('\'', "\\'")
    }
}

#[derive(Debug, clickhouse::Row, Deserialize)]
struct CountRow {
    total: u64,
}

/// Strips `dh_...` prefix from Agents tokens UI; accepts bare suffix (e.g. `9ToE`).
fn normalize_pasted_token_prefix(raw: &str) -> Option<String> {
    let s = raw.trim();
    if s.is_empty() {
        return None;
    }
    let suffix = if let Some(rest) = s.strip_prefix("dh_...") {
        rest
    } else if let Some(rest) = s.strip_prefix("dh_") {
        rest
    } else {
        s
    };
    let suffix = suffix.trim();
    if suffix.is_empty() {
        return None;
    }
    if suffix.len() > 64 {
        return None;
    }
    if !suffix
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return None;
    }
    Some(suffix.to_string())
}

async fn resolve_user_emails(
    db: &reiver_core::db::DbPool,
    org_id: Uuid,
    ids: &[Uuid],
) -> Result<HashMap<Uuid, String>, sqlx::Error> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows: Vec<(Uuid, String)> = sqlx::query_as(
        r#"SELECT u.id, u.email FROM users u
           INNER JOIN memberships m ON m.user_id = u.id
             AND m.organization_id = $1 AND m.status = 'active'
           WHERE u.id = ANY($2)"#,
    )
    .bind(org_id)
    .bind(ids)
    .fetch_all(db)
    .await?;
    Ok(rows.into_iter().collect())
}

fn collect_user_ids_from_rows(rows: &[AuditEventRow]) -> Vec<Uuid> {
    let mut set: HashSet<Uuid> = HashSet::new();
    for row in rows {
        if let Ok(u) = Uuid::parse_str(row.actor_id.trim()) {
            set.insert(u);
        }
        if let Ok(u) = Uuid::parse_str(row.caller_user_id.trim()) {
            set.insert(u);
        }
    }
    let mut v: Vec<Uuid> = set.into_iter().collect();
    v.sort();
    v
}

fn enrich_events(events: &mut [AuditEvent], email_map: &HashMap<Uuid, String>) {
    for e in events.iter_mut() {
        if let Ok(uid) = Uuid::parse_str(e.actor_id.trim()) {
            e.actor_email = email_map.get(&uid).cloned();
        }
        if let Ok(uid) = Uuid::parse_str(e.caller_user_id.trim()) {
            e.caller_user_email = email_map.get(&uid).cloned();
        }
    }
}

#[instrument(name = "audit.list_events", skip(state, headers))]
async fn list_audit_events(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Query(query): Query<AuditQuery>,
) -> std::result::Result<Json<AuditEventsResponse>, (axum::http::StatusCode, Json<ErrorResponse>)> {
    let auth = authenticate(&headers, &state).await?;
    require_admin(&state, auth.user_id, auth.organization_id).await?;

    let tier = state
        .entitlements
        .get_config(auth.organization_id)
        .await
        .map_err(|_| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new("Entitlement check failed")),
            )
        })?;
    if !tier.config.platform.audit_log {
        return Err((
            axum::http::StatusCode::FORBIDDEN,
            Json(ErrorResponse::new(
                "Audit log is not available on your current plan",
            )),
        ));
    }

    let limit = query.limit.unwrap_or(50).min(200);
    let offset = query.offset.unwrap_or(0);

    let mut qb = QueryBuilder::new();
    qb.add_eq("organization_id", auth.organization_id.to_string());

    if let Some(ref et) = query.event_types {
        let types: Vec<String> = et.split(',').map(|s| s.trim().to_string()).collect();
        if !types.is_empty() {
            qb.add_in("event_type", types);
        }
    }
    if let Some(ref rt) = query.resource_type {
        qb.add_eq("resource_type", rt.clone());
    }
    if let Some(ref pid) = query.project_id {
        qb.add_eq("project_id", pid.clone());
    }
    if let Some(ref ct) = query.caller_type {
        qb.add_eq("caller_type", ct.clone());
    }
    if let Some(ref aid) = query.actor_id {
        qb.add_eq("actor_id", aid.clone());
    }
    if let Some(ref uid) = query.user_id {
        let uid = uid.trim();
        if Uuid::parse_str(uid).is_err() {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new("Invalid user_id")),
            ));
        }
        qb.add_or_actor_caller_user(uid);
    }
    if let Some(ref cuid) = query.caller_user_id {
        let cuid = cuid.trim();
        if Uuid::parse_str(cuid).is_err() {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new("Invalid caller_user_id")),
            ));
        }
        qb.add_eq("caller_user_id", cuid.to_string());
    }
    if let Some(ref label) = query.caller_key_label {
        let label = label.trim();
        if !label.is_empty() {
            qb.add_eq("caller_key_label", label.to_string());
        }
    }
    if let Some(ref raw) = query.caller_key_prefix {
        if let Some(prefix) = normalize_pasted_token_prefix(raw) {
            qb.add_eq("caller_key_prefix", prefix);
        }
    }
    if let Some(ref svc) = query.service {
        qb.add_eq("service", svc.clone());
    }
    if let Some(ref from) = query.from {
        qb.add_timestamp_gte(from);
    }
    if let Some(ref to) = query.to {
        qb.add_timestamp_lte(to);
    }

    let where_clause = qb.where_sql();

    let count_sql = format!(
        "SELECT count() AS total FROM reiver.audit_events {}",
        where_clause
    );
    let data_sql = format!(
        r#"SELECT
            event_id, project_id, event_type, action,
            caller_type, caller_user_id, caller_key_label, caller_key_prefix,
            service, http_method, http_path, http_status,
            source_id,
            prompt_config_name, prompt_config_id, prompt_version_id,
            prompt_version_number, rendered_system_prompt, prompt_variables,
            model_used, total_input_tokens, total_output_tokens, total_turns,
            tool_calls_log,
            mcp_tool_name, mcp_tool_arguments, mcp_tool_success, mcp_tool_error,
            toString(timestamp) AS timestamp_str,
            duration_ms,
            organization_id, actor_id, ip_address, user_agent,
            resource_type, resource_id, details, success, error_message,
            origin_type, origin_ref, origin_reason
        FROM reiver.audit_events
        {}
        ORDER BY timestamp DESC
        LIMIT {} OFFSET {}"#,
        where_clause, limit, offset
    );

    let total = state
        .clickhouse
        .query(&count_sql)
        .fetch_one::<CountRow>()
        .await
        .map(|r| r.total)
        .unwrap_or(0);

    let rows: Vec<AuditEventRow> = state
        .clickhouse
        .query(&data_sql)
        .fetch_all::<AuditEventRow>()
        .await
        .map_err(|e| {
            error!(error = %e, count_sql = %count_sql, data_sql = %data_sql, "Failed to query audit events");
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new("Failed to query audit events")),
            )
        })?;

    let ids = collect_user_ids_from_rows(&rows);
    let email_map = resolve_user_emails(state.db.as_ref(), auth.organization_id, &ids)
        .await
        .map_err(|e| {
            error!(error = %e, "Failed to resolve user emails for audit");
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new("Failed to query audit events")),
            )
        })?;

    let mut events: Vec<AuditEvent> = rows.into_iter().map(AuditEvent::from).collect();
    enrich_events(&mut events, &email_map);

    Ok(Json(AuditEventsResponse { events, total }))
}
