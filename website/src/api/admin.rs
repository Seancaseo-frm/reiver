use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    routing::{get, patch, post, put},
    Json, Router,
};
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::app_state::WebsiteState;
use crate::audit::{AuditCaller, AuditEventBuilder, AuditEventType, AuditOrigin};
use crate::auth::extract_user_id;
use reiver_core::auth::invalidate_approval_cache;
use reiver_core::authorization::require_platform_admin;
use reiver_core::billing::{PaymentProvider, StripePaymentProvider};
use reiver_core::error::{AppError, Result};
use reiver_core::models::{Dashboard, DashboardTab};
use rust_decimal::Decimal;

pub fn create_admin_router() -> Router<Arc<WebsiteState>> {
    Router::new()
        .route("/warehouse/global-sources", get(global_sources))
        .route("/warehouse/sources", get(all_sources))
        .route("/warehouse/jobs", get(jobs))
        .route("/warehouse/syncs", get(syncs))
        .route("/warehouse/otel/errors", get(otel_errors))
        .route("/warehouse/otel/stats", get(otel_stats))
        .route("/agent-settings", get(get_agent_settings))
        .route("/agent-settings", put(update_agent_settings))
        .route("/signup-policy", get(get_signup_policy))
        .route("/signup-policy", put(update_signup_policy))
        .route("/ingestion-stress", post(run_ingestion_stress))
        .route("/users", get(list_users))
        .route("/users/{id}/approve", post(approve_user))
        .route("/users/{id}/disable", post(disable_user))
        .route("/charges", get(list_charges))
        .route("/charges/generate", post(generate_charges))
        .route("/charges/{id}", get(get_charge))
        .route("/charges/{id}/approve", post(approve_charge))
        .route("/charges/{id}/reject", post(reject_charge))
        .route("/charges/{id}/retry", post(retry_charge))
        .route("/model-catalog", get(list_model_catalog))
        .route("/model-catalog/sync", post(sync_model_catalog))
        .route("/model-catalog/{id}", patch(update_model_catalog))
        // Tier management
        .route("/tiers/schema", get(tier_schema))
        .route("/tiers", get(list_tiers))
        .route("/tiers", post(create_tier))
        .route("/tiers/{tier_id}", put(update_tier))
        .route("/tiers/{tier_id}", axum::routing::delete(delete_tier))
        .route("/tiers/organizations", get(list_organizations_with_tiers))
        .route("/tiers/org/{org_id}", get(get_org_entitlements))
        .route("/tiers/org/{org_id}", put(update_org_tier))
        .route(
            "/tiers/org/{org_id}/overrides",
            axum::routing::delete(delete_org_overrides),
        )
        .route("/organizations/{org_id}/members", get(list_org_members))
        .route("/dashboards/reconvert", post(reconvert_dashboards))
        // Knowledge Base (pgvector)
        .route("/knowledge-base", get(list_kb_documents))
        .route("/knowledge-base", post(create_kb_document))
        .route("/knowledge-base/upload", post(upload_kb_document))
        .route("/knowledge-base/{id}", put(update_kb_document))
        .route(
            "/knowledge-base/{id}",
            axum::routing::delete(delete_kb_document),
        )
        .route(
            "/knowledge-base/{id}/reembed",
            post(reembed_kb_document),
        )
        // Dashboard Templates CRUD
        .route("/dashboard-templates", get(list_admin_templates))
        .route("/dashboard-templates", post(create_admin_template))
        .route("/dashboard-templates/{id}", put(update_admin_template))
        .route(
            "/dashboard-templates/{id}",
            axum::routing::delete(delete_admin_template),
        )
}

// ── helpers ──────────────────────────────────────────────────────────────

async fn admin_user_id(state: &WebsiteState, headers: &HeaderMap) -> Result<Uuid> {
    let user_id = extract_user_id(headers, &state.config.jwt_secret)?;
    require_platform_admin(&state.db, user_id).await?;
    Ok(user_id)
}

// ── response types ───────────────────────────────────────────────────────

#[derive(Serialize)]
struct GlobalSourceRow {
    id: Uuid,
    chain: String,
    last_synced_height: i64,
    sync_interval: String,
    enabled: bool,
    updated_at: DateTime<Utc>,
}

#[derive(Serialize)]
struct SourceRow {
    id: Uuid,
    project_name: String,
    name: String,
    source_type: String,
    tier: String,
    last_sync_at: Option<DateTime<Utc>>,
    storage_bytes: i64,
    sync_interval: Option<String>,
    enabled: bool,
    is_global: bool,
}

#[derive(Serialize)]
struct JobRow {
    id: Uuid,
    job_type: String,
    source_name: Option<String>,
    status: String,
    scheduled_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    rows_synced: Option<i64>,
    bytes_written: Option<i64>,
    error: Option<String>,
    retry_count: i32,
}

#[derive(Serialize)]
struct SyncRow {
    id: Uuid,
    source_name: Option<String>,
    table_name: String,
    status: String,
    rows_synced: i64,
    bytes_written: i64,
    duration_ms: i64,
    error: Option<String>,
    completed_at: DateTime<Utc>,
}

#[derive(Serialize)]
struct OtelErrorRow {
    span_name: String,
    status_message: String,
    timestamp: String,
    duration_ns: u64,
    trace_id: String,
}

#[derive(Serialize)]
struct OtelStatsResponse {
    total_spans_1h: u64,
    error_count_1h: u64,
    error_rate_pct: f64,
    p50_duration_ms: f64,
    p95_duration_ms: f64,
}

#[derive(Deserialize)]
struct JobsQuery {
    status: Option<String>,
    limit: Option<i64>,
}

#[derive(Deserialize)]
struct LimitQuery {
    limit: Option<i64>,
}

// ── handlers ─────────────────────────────────────────────────────────────

async fn global_sources(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<GlobalSourceRow>>> {
    admin_user_id(&state, &headers).await?;

    let rows = sqlx::query(
        r#"
        SELECT id, chain, last_synced_height, sync_interval, enabled, updated_at
        FROM blockchain_global_sources
        ORDER BY chain
        "#,
    )
    .fetch_all(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

    let out: Vec<GlobalSourceRow> = rows
        .iter()
        .map(|r| GlobalSourceRow {
            id: r.get("id"),
            chain: r.get("chain"),
            last_synced_height: r.get("last_synced_height"),
            sync_interval: r.get("sync_interval"),
            enabled: r.get("enabled"),
            updated_at: r.get("updated_at"),
        })
        .collect();

    Ok(Json(out))
}

async fn all_sources(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<SourceRow>>> {
    admin_user_id(&state, &headers).await?;

    let rows = sqlx::query(
        r#"
        SELECT
            s.id,
            COALESCE(p.name, 'unknown') AS project_name,
            s.name,
            s.source_type,
            s.tier,
            s.last_sync_at,
            COALESCE(s.storage_bytes, 0) AS storage_bytes,
            s.sync_interval,
            s.enabled,
            s.global_source_id
        FROM warehouse_sources s
        LEFT JOIN projects p ON p.id = s.project_id
        ORDER BY s.updated_at DESC
        LIMIT 500
        "#,
    )
    .fetch_all(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

    let out: Vec<SourceRow> = rows
        .iter()
        .map(|r| {
            let global_source_id: Option<Uuid> = r.get("global_source_id");
            SourceRow {
                id: r.get("id"),
                project_name: r.get("project_name"),
                name: r.get("name"),
                source_type: r.get("source_type"),
                tier: r.get("tier"),
                last_sync_at: r.get("last_sync_at"),
                storage_bytes: r.get("storage_bytes"),
                sync_interval: r.get("sync_interval"),
                enabled: r.get("enabled"),
                is_global: global_source_id.is_some(),
            }
        })
        .collect();

    Ok(Json(out))
}

async fn jobs(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Query(q): Query<JobsQuery>,
) -> Result<Json<Vec<JobRow>>> {
    admin_user_id(&state, &headers).await?;

    let limit = q.limit.unwrap_or(100).min(500);

    let rows = match q.status {
        Some(ref status) => {
            sqlx::query(
                r#"
                SELECT
                    j.id, j.job_type, s.name AS source_name,
                    j.status, j.scheduled_at, j.started_at, j.completed_at,
                    j.rows_synced, j.bytes_written, j.error, j.retry_count
                FROM warehouse_jobs j
                LEFT JOIN warehouse_sources s ON s.id = j.source_id
                WHERE j.status = $1
                ORDER BY j.scheduled_at DESC
                LIMIT $2
                "#,
            )
            .bind(status)
            .bind(limit)
            .fetch_all(&*state.db)
            .await
        }
        None => {
            sqlx::query(
                r#"
                SELECT
                    j.id, j.job_type, s.name AS source_name,
                    j.status, j.scheduled_at, j.started_at, j.completed_at,
                    j.rows_synced, j.bytes_written, j.error, j.retry_count
                FROM warehouse_jobs j
                LEFT JOIN warehouse_sources s ON s.id = j.source_id
                ORDER BY j.scheduled_at DESC
                LIMIT $1
                "#,
            )
            .bind(limit)
            .fetch_all(&*state.db)
            .await
        }
    }
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

    let out: Vec<JobRow> = rows
        .iter()
        .map(|r| JobRow {
            id: r.get("id"),
            job_type: r.get("job_type"),
            source_name: r.get("source_name"),
            status: r.get("status"),
            scheduled_at: r.get("scheduled_at"),
            started_at: r.get("started_at"),
            completed_at: r.get("completed_at"),
            rows_synced: r.get("rows_synced"),
            bytes_written: r.get("bytes_written"),
            error: r.get("error"),
            retry_count: r.get("retry_count"),
        })
        .collect();

    Ok(Json(out))
}

async fn syncs(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Query(q): Query<LimitQuery>,
) -> Result<Json<Vec<SyncRow>>> {
    admin_user_id(&state, &headers).await?;

    let limit = q.limit.unwrap_or(100).min(500);

    let rows = sqlx::query(
        r#"
        SELECT
            ws.id, s.name AS source_name, ws.table_name,
            ws.status, ws.rows_synced, ws.bytes_written,
            ws.duration_ms, ws.error, ws.completed_at
        FROM warehouse_syncs ws
        LEFT JOIN warehouse_sources s ON s.id = ws.source_id
        ORDER BY ws.completed_at DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

    let out: Vec<SyncRow> = rows
        .iter()
        .map(|r| SyncRow {
            id: r.get("id"),
            source_name: r.get("source_name"),
            table_name: r.get("table_name"),
            status: r.get("status"),
            rows_synced: r.get("rows_synced"),
            bytes_written: r.get("bytes_written"),
            duration_ms: r.get("duration_ms"),
            error: r.get("error"),
            completed_at: r.get("completed_at"),
        })
        .collect();

    Ok(Json(out))
}

async fn otel_errors(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Query(q): Query<LimitQuery>,
) -> Result<Json<Vec<OtelErrorRow>>> {
    admin_user_id(&state, &headers).await?;

    let limit = q.limit.unwrap_or(50).min(200) as u32;

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct ChRow {
        span_name: String,
        status_message: String,
        timestamp: String,
        duration: i64,
        trace_id: String,
    }

    let otel_project_id = "07ce3ace-5133-496a-ba55-9bf40bb5d3aa";

    let rows: Vec<ChRow> = state
        .clickhouse
        .query(
            r#"
            SELECT
                span_name,
                status_message,
                toString(timestamp) AS timestamp,
                duration,
                trace_id
            FROM reiver.spans
            WHERE project_id = ?
              AND status_code = 'STATUS_CODE_ERROR'
            ORDER BY timestamp DESC
            LIMIT ?
            "#,
        )
        .bind(otel_project_id)
        .bind(limit)
        .fetch_all::<ChRow>()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse error: {}", e)))?;

    let out: Vec<OtelErrorRow> = rows
        .into_iter()
        .map(|r| OtelErrorRow {
            span_name: r.span_name,
            status_message: r.status_message,
            timestamp: r.timestamp,
            duration_ns: r.duration as u64,
            trace_id: r.trace_id,
        })
        .collect();

    Ok(Json(out))
}

async fn otel_stats(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> Result<Json<OtelStatsResponse>> {
    admin_user_id(&state, &headers).await?;

    let otel_project_id = "07ce3ace-5133-496a-ba55-9bf40bb5d3aa";

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct StatsRow {
        total: u64,
        errors: u64,
        p50_ms: f64,
        p95_ms: f64,
    }

    let row: StatsRow = state
        .clickhouse
        .query(
            r#"
            SELECT
                count()                                        AS total,
                countIf(status_code = 'STATUS_CODE_ERROR')     AS errors,
                quantile(0.5)(duration)  / 1000000.0           AS p50_ms,
                quantile(0.95)(duration) / 1000000.0           AS p95_ms
            FROM reiver.spans
            WHERE project_id = ?
              AND timestamp >= now() - INTERVAL 1 HOUR
            "#,
        )
        .bind(otel_project_id)
        .fetch_one::<StatsRow>()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse error: {}", e)))?;

    let error_rate = if row.total > 0 {
        (row.errors as f64 / row.total as f64) * 100.0
    } else {
        0.0
    };

    Ok(Json(OtelStatsResponse {
        total_spans_1h: row.total,
        error_count_1h: row.errors,
        error_rate_pct: (error_rate * 100.0).round() / 100.0,
        p50_duration_ms: (row.p50_ms * 100.0).round() / 100.0,
        p95_duration_ms: (row.p95_ms * 100.0).round() / 100.0,
    }))
}

// ── Agent Settings (platform-wide) ──────────────────────────────────────

#[derive(Serialize, Deserialize)]
struct AgentSettings {
    agent_model: String,
}

async fn get_agent_settings(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> Result<Json<AgentSettings>> {
    admin_user_id(&state, &headers).await?;

    let model: Option<String> =
        sqlx::query_scalar("SELECT value FROM platform_settings WHERE key = 'agent_model'")
            .fetch_optional(&*state.db)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

    Ok(Json(AgentSettings {
        agent_model: model.unwrap_or_else(|| "auto".to_string()),
    }))
}

async fn update_agent_settings(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(body): Json<AgentSettings>,
) -> Result<Json<AgentSettings>> {
    let admin_id = admin_user_id(&state, &headers).await?;

    let model = body.agent_model.trim().to_string();
    if model.is_empty() {
        return Err(AppError::BadRequest("agent_model cannot be empty".into()));
    }

    let before_model: Option<String> =
        sqlx::query_scalar("SELECT value FROM platform_settings WHERE key = 'agent_model'")
            .fetch_optional(&*state.db)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;
    let before_model = before_model.unwrap_or_else(|| "auto".to_string());

    sqlx::query(
        r#"
        INSERT INTO platform_settings (key, value, updated_at)
        VALUES ('agent_model', $1, NOW())
        ON CONFLICT (key) DO UPDATE SET value = $1, updated_at = NOW()
        "#,
    )
    .bind(&model)
    .execute(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::AgentSettingsUpdated)
        .actor(admin_id)
        .details(serde_json::json!({
            "before": { "agent_model": &before_model },
            "after": { "agent_model": &model },
        }))
        .origin(
            &audit_origin.origin_type,
            &audit_origin.origin_ref,
            &audit_origin.origin_reason,
        )
        .caller(
            &audit_caller.caller_type,
            &audit_caller.key_label,
            &audit_caller.key_prefix,
        )
        .success()
        .log(&state.clickhouse)
        .await;

    Ok(Json(AgentSettings { agent_model: model }))
}

// ── User Management (platform admin) ────────────────────────────────────

#[derive(Serialize, sqlx::FromRow)]
struct AdminUserRow {
    id: Uuid,
    email: String,
    is_approved: bool,
    is_platform_admin: bool,
    created_at: DateTime<Utc>,
}

async fn list_users(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<AdminUserRow>>> {
    admin_user_id(&state, &headers).await?;

    let rows = sqlx::query_as::<_, AdminUserRow>(
        r#"
        SELECT id, email, is_approved, is_platform_admin, created_at
        FROM users
        ORDER BY created_at DESC
        "#,
    )
    .fetch_all(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

    Ok(Json(rows))
}

async fn approve_user(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(user_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>> {
    let admin_id = admin_user_id(&state, &headers).await?;

    let before_approved: Option<bool> =
        sqlx::query_scalar("SELECT is_approved FROM users WHERE id = $1")
            .bind(user_id)
            .fetch_optional(&*state.db)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

    sqlx::query("UPDATE users SET is_approved = true WHERE id = $1")
        .bind(user_id)
        .execute(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

    invalidate_approval_cache(&state.redis, user_id).await;

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::UserApproved)
        .actor(admin_id)
        .resource("user", user_id)
        .details(serde_json::json!({
            "before": { "is_approved": before_approved },
            "after": { "is_approved": true },
        }))
        .origin(
            &audit_origin.origin_type,
            &audit_origin.origin_ref,
            &audit_origin.origin_reason,
        )
        .caller(
            &audit_caller.caller_type,
            &audit_caller.key_label,
            &audit_caller.key_prefix,
        )
        .success()
        .log(&state.clickhouse)
        .await;

    Ok(Json(serde_json::json!({ "success": true })))
}

async fn disable_user(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(user_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>> {
    let admin_id = admin_user_id(&state, &headers).await?;

    if admin_id == user_id {
        return Err(AppError::BadRequest(
            "Cannot disable your own account".into(),
        ));
    }

    let before_approved: Option<bool> =
        sqlx::query_scalar("SELECT is_approved FROM users WHERE id = $1")
            .bind(user_id)
            .fetch_optional(&*state.db)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

    sqlx::query("UPDATE users SET is_approved = false WHERE id = $1")
        .bind(user_id)
        .execute(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

    invalidate_approval_cache(&state.redis, user_id).await;

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::UserDisabled)
        .actor(admin_id)
        .resource("user", user_id)
        .details(serde_json::json!({
            "before": { "is_approved": before_approved },
            "after": { "is_approved": false },
        }))
        .origin(
            &audit_origin.origin_type,
            &audit_origin.origin_ref,
            &audit_origin.origin_reason,
        )
        .caller(
            &audit_caller.caller_type,
            &audit_caller.key_label,
            &audit_caller.key_prefix,
        )
        .success()
        .log(&state.clickhouse)
        .await;

    Ok(Json(serde_json::json!({ "success": true })))
}

// ── Signup policy (platform-wide) ───────────────────────────────────────

#[derive(Serialize)]
struct SignupPolicyResponse {
    require_signup_approval: bool,
}

async fn get_signup_policy(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> Result<Json<SignupPolicyResponse>> {
    admin_user_id(&state, &headers).await?;

    let v: Option<String> = sqlx::query_scalar(
        "SELECT value FROM platform_settings WHERE key = 'require_signup_approval'",
    )
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

    let require_signup_approval = !matches!(v.as_deref(), Some("false"));

    Ok(Json(SignupPolicyResponse {
        require_signup_approval,
    }))
}

#[derive(Deserialize)]
struct SignupPolicyUpdate {
    require_signup_approval: bool,
}

async fn update_signup_policy(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(body): Json<SignupPolicyUpdate>,
) -> Result<Json<SignupPolicyResponse>> {
    admin_user_id(&state, &headers).await?;

    let value = if body.require_signup_approval {
        "true"
    } else {
        "false"
    };

    sqlx::query(
        r#"
        INSERT INTO platform_settings (key, value, updated_at)
        VALUES ('require_signup_approval', $1, NOW())
        ON CONFLICT (key) DO UPDATE SET value = $1, updated_at = NOW()
        "#,
    )
    .bind(value)
    .execute(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

    Ok(Json(SignupPolicyResponse {
        require_signup_approval: body.require_signup_approval,
    }))
}

// ── Ingestion stress (OTLP to Watch) ────────────────────────────────────

/// Cancels the in-flight stress loop when the admin HTTP request is dropped (e.g. client abort).
struct CancelStressOnDrop(CancellationToken);
impl Drop for CancelStressOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

#[derive(Deserialize)]
struct IngestionStressRequest {
    project_id: Uuid,
    rps: u32,
}

async fn run_ingestion_stress(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(req): Json<IngestionStressRequest>,
) -> Result<Json<crate::ingestion_stress::StressResult>> {
    admin_user_id(&state, &headers).await?;

    let rps = req.rps.clamp(1, 200);

    let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM projects WHERE id = $1)")
        .bind(req.project_id)
        .fetch_one(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

    if !exists {
        return Err(AppError::BadRequest("Unknown project_id".into()));
    }

    let token = CancellationToken::new();
    let _cancel_on_drop = CancelStressOnDrop(token.clone());
    let result = crate::ingestion_stress::run_stress(
        &state.http_client,
        &state.watch_url,
        req.project_id,
        rps,
        &token,
    )
    .await;

    Ok(Json(result))
}

// ── Billing Charges (admin approval) ────────────────────────────────────

#[derive(Serialize)]
struct ChargeRow {
    id: Uuid,
    organization_id: Uuid,
    organization_name: Option<String>,
    charge_type: String,
    billing_period_start: chrono::NaiveDate,
    billing_period_end: chrono::NaiveDate,
    amount_usd: Decimal,
    description: Option<String>,
    line_items: Option<serde_json::Value>,
    status: String,
    reviewed_by: Option<Uuid>,
    reviewed_at: Option<DateTime<Utc>>,
    stripe_payment_intent_id: Option<String>,
    error_message: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Deserialize)]
struct ChargesQuery {
    status: Option<String>,
}

#[derive(Deserialize)]
struct RejectBody {
    reason: Option<String>,
}

async fn list_charges(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Query(q): Query<ChargesQuery>,
) -> Result<Json<Vec<ChargeRow>>> {
    admin_user_id(&state, &headers).await?;

    let rows = match &q.status {
        Some(status) => {
            sqlx::query(
                r#"
                SELECT pc.*, o.name AS organization_name
                FROM pending_charges pc
                JOIN organizations o ON o.id = pc.organization_id
                WHERE pc.status = $1
                ORDER BY pc.created_at DESC
                LIMIT 500
                "#,
            )
            .bind(status)
            .fetch_all(&*state.db)
            .await
        }
        None => {
            sqlx::query(
                r#"
                SELECT pc.*, o.name AS organization_name
                FROM pending_charges pc
                JOIN organizations o ON o.id = pc.organization_id
                ORDER BY pc.created_at DESC
                LIMIT 500
                "#,
            )
            .fetch_all(&*state.db)
            .await
        }
    }
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

    let out: Vec<ChargeRow> = rows
        .iter()
        .map(|r| ChargeRow {
            id: r.get("id"),
            organization_id: r.get("organization_id"),
            organization_name: r.get("organization_name"),
            charge_type: r.get("charge_type"),
            billing_period_start: r.get("billing_period_start"),
            billing_period_end: r.get("billing_period_end"),
            amount_usd: r.get("amount_usd"),
            description: r.get("description"),
            line_items: r.get("line_items"),
            status: r.get("status"),
            reviewed_by: r.get("reviewed_by"),
            reviewed_at: r.get("reviewed_at"),
            stripe_payment_intent_id: r.get("stripe_payment_intent_id"),
            error_message: r.get("error_message"),
            created_at: r.get("created_at"),
            updated_at: r.get("updated_at"),
        })
        .collect();

    Ok(Json(out))
}

async fn get_charge(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(charge_id): Path<Uuid>,
) -> Result<Json<ChargeRow>> {
    admin_user_id(&state, &headers).await?;

    let r = sqlx::query(
        r#"
        SELECT pc.*, o.name AS organization_name
        FROM pending_charges pc
        JOIN organizations o ON o.id = pc.organization_id
        WHERE pc.id = $1
        "#,
    )
    .bind(charge_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?
    .ok_or_else(|| AppError::NotFound("Charge not found".into()))?;

    Ok(Json(ChargeRow {
        id: r.get("id"),
        organization_id: r.get("organization_id"),
        organization_name: r.get("organization_name"),
        charge_type: r.get("charge_type"),
        billing_period_start: r.get("billing_period_start"),
        billing_period_end: r.get("billing_period_end"),
        amount_usd: r.get("amount_usd"),
        description: r.get("description"),
        line_items: r.get("line_items"),
        status: r.get("status"),
        reviewed_by: r.get("reviewed_by"),
        reviewed_at: r.get("reviewed_at"),
        stripe_payment_intent_id: r.get("stripe_payment_intent_id"),
        error_message: r.get("error_message"),
        created_at: r.get("created_at"),
        updated_at: r.get("updated_at"),
    }))
}

async fn approve_charge(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(charge_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>> {
    let admin_id = admin_user_id(&state, &headers).await?;

    let row = sqlx::query(
        "SELECT id, organization_id, amount_usd, description, status \
         FROM pending_charges WHERE id = $1",
    )
    .bind(charge_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?
    .ok_or_else(|| AppError::NotFound("Charge not found".into()))?;

    let status: String = row.get("status");
    if status != "pending" {
        return Err(AppError::BadRequest(format!(
            "Charge is in '{}' state, expected 'pending'",
            status
        )));
    }

    let org_id: Uuid = row.get("organization_id");
    let amount: Decimal = row.get("amount_usd");
    let description: Option<String> = row.get("description");

    sqlx::query(
        "UPDATE pending_charges SET status = 'approved', reviewed_by = $2, reviewed_at = NOW() WHERE id = $1",
    )
    .bind(charge_id)
    .bind(admin_id)
    .execute(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

    AuditEventBuilder::new(AuditEventType::ChargeApproved)
        .actor(admin_id)
        .organization(org_id)
        .resource("pending_charge", charge_id)
        .details(serde_json::json!({
            "amount_usd": amount.to_string(),
        }))
        .log(&state.clickhouse)
        .await;

    let api_key = state
        .config
        .stripe_api_key
        .as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Stripe not configured")))?;
    let provider = StripePaymentProvider::new(
        api_key,
        state.db.clone(),
        state.redis.clone(),
        state.config.stripe_webhook_secret.clone(),
        state.config.stripe_metered_price_id.clone(),
    );

    let desc = description.unwrap_or_else(|| "Reiver charge".to_string());
    match provider
        .charge_saved_payment_method(org_id, amount, &desc)
        .await
    {
        Ok(pi_id) => {
            sqlx::query(
                "UPDATE pending_charges SET status = 'paid', stripe_payment_intent_id = $2 WHERE id = $1",
            )
            .bind(charge_id)
            .bind(&pi_id)
            .execute(&*state.db)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

            Ok(Json(
                serde_json::json!({ "success": true, "payment_intent_id": pi_id }),
            ))
        }
        Err(e) => {
            let err_msg = e.to_string();
            sqlx::query(
                "UPDATE pending_charges SET status = 'payment_failed', error_message = $2 WHERE id = $1",
            )
            .bind(charge_id)
            .bind(&err_msg)
            .execute(&*state.db)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

            Err(AppError::BadRequest(format!("Payment failed: {}", err_msg)))
        }
    }
}

async fn reject_charge(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(charge_id): Path<Uuid>,
    Json(body): Json<RejectBody>,
) -> Result<Json<serde_json::Value>> {
    let admin_id = admin_user_id(&state, &headers).await?;

    let row = sqlx::query(
        "SELECT organization_id, status FROM pending_charges WHERE id = $1",
    )
    .bind(charge_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?
    .ok_or_else(|| AppError::NotFound("Charge not found".into()))?;

    let status: String = row.get("status");
    if status != "pending" {
        return Err(AppError::BadRequest(format!("Charge is in '{}' state", status)));
    }
    let org_id: Uuid = row.get("organization_id");

    sqlx::query(
        r#"
        UPDATE pending_charges
        SET status = 'rejected', reviewed_by = $2, reviewed_at = NOW(),
            error_message = $3
        WHERE id = $1
        "#,
    )
    .bind(charge_id)
    .bind(admin_id)
    .bind(body.reason.as_deref())
    .execute(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

    AuditEventBuilder::new(AuditEventType::ChargeRejected)
        .actor(admin_id)
        .organization(org_id)
        .resource("pending_charge", charge_id)
        .details(serde_json::json!({
            "reason": body.reason.as_deref().unwrap_or(""),
        }))
        .log(&state.clickhouse)
        .await;

    Ok(Json(serde_json::json!({ "success": true })))
}

async fn retry_charge(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(charge_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>> {
    let admin_id = admin_user_id(&state, &headers).await?;

    let row = sqlx::query(
        "SELECT id, organization_id, amount_usd, description, status \
         FROM pending_charges WHERE id = $1",
    )
    .bind(charge_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?
    .ok_or_else(|| AppError::NotFound("Charge not found".into()))?;

    let status: String = row.get("status");
    if status != "payment_failed" {
        return Err(AppError::BadRequest(format!(
            "Can only retry 'payment_failed' charges, got '{}'",
            status
        )));
    }

    let org_id: Uuid = row.get("organization_id");
    let amount: Decimal = row.get("amount_usd");
    let description: Option<String> = row.get("description");

    sqlx::query(
        "UPDATE pending_charges SET reviewed_by = $2, reviewed_at = NOW(), error_message = NULL WHERE id = $1",
    )
    .bind(charge_id)
    .bind(admin_id)
    .execute(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

    let api_key = state
        .config
        .stripe_api_key
        .as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Stripe not configured")))?;
    let provider = StripePaymentProvider::new(
        api_key,
        state.db.clone(),
        state.redis.clone(),
        state.config.stripe_webhook_secret.clone(),
        state.config.stripe_metered_price_id.clone(),
    );

    let desc = description.unwrap_or_else(|| "Reiver charge".to_string());
    match provider
        .charge_saved_payment_method(org_id, amount, &desc)
        .await
    {
        Ok(pi_id) => {
            sqlx::query(
                "UPDATE pending_charges SET status = 'paid', stripe_payment_intent_id = $2 WHERE id = $1",
            )
            .bind(charge_id)
            .bind(&pi_id)
            .execute(&*state.db)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

            Ok(Json(
                serde_json::json!({ "success": true, "payment_intent_id": pi_id }),
            ))
        }
        Err(e) => {
            let err_msg = e.to_string();
            sqlx::query(
                "UPDATE pending_charges SET status = 'payment_failed', error_message = $2 WHERE id = $1",
            )
            .bind(charge_id)
            .bind(&err_msg)
            .execute(&*state.db)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

            Err(AppError::BadRequest(format!(
                "Payment failed (retry): {}",
                err_msg
            )))
        }
    }
}

async fn generate_charges(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>> {
    let admin_id = admin_user_id(&state, &headers).await?;

    let api_key = state.config.stripe_api_key.as_deref().unwrap_or("");
    if !api_key.starts_with("sk_test_") {
        return Err(AppError::Forbidden(
            "Trigger billing is only available in Stripe test mode".into(),
        ));
    }

    let org_id: Uuid = sqlx::query_scalar(
        "SELECT organization_id FROM memberships WHERE user_id = $1 AND status = 'active' LIMIT 1",
    )
    .bind(admin_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?
    .ok_or_else(|| AppError::BadRequest("Admin has no active organization".into()))?;

    let now = Utc::now();
    let period_start = NaiveDate::from_ymd_opt(now.year(), now.month(), 1).expect("Day 1 is valid");
    let period_end = now.date_naive() + chrono::Days::new(1);

    let result_msg = match crate::billing_worker::generate_combined_charge(
        &state.db,
        &state.billing,
        state.entitlements.as_ref(),
        org_id,
        period_start,
        period_end,
    )
    .await
    {
        Ok(()) => "platform_usage: generated".to_string(),
        Err(e) => format!("platform_usage: {}", e),
    };

    AuditEventBuilder::new(AuditEventType::ChargeGenerated)
        .actor(admin_id)
        .organization(org_id)
        .details(serde_json::json!({
            "period_start": period_start.to_string(),
            "period_end": period_end.to_string(),
            "result": &result_msg,
        }))
        .log(&state.clickhouse)
        .await;

    Ok(Json(serde_json::json!({
        "success": true,
        "organization_id": org_id,
        "period_start": period_start.to_string(),
        "period_end": period_end.to_string(),
        "result": result_msg,
    })))
}

// ── Model Catalog (admin) ───────────────────────────────────────────────

#[derive(Serialize)]
struct ModelCatalogRow {
    id: String,
    name: String,
    provider_slug: String,
    model_slug: String,
    context_length: Option<i32>,
    pricing: serde_json::Value,
    enabled: bool,
    last_synced_at: DateTime<Utc>,
}

#[derive(Deserialize)]
struct ModelCatalogQuery {
    provider: Option<String>,
    search: Option<String>,
    enabled: Option<bool>,
}

#[derive(Deserialize)]
struct ModelCatalogUpdate {
    enabled: Option<bool>,
    pricing: Option<serde_json::Value>,
}

async fn list_model_catalog(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Query(q): Query<ModelCatalogQuery>,
) -> Result<Json<Vec<ModelCatalogRow>>> {
    admin_user_id(&state, &headers).await?;

    let mut sql = String::from(
        "SELECT id, name, provider_slug, model_slug, context_length, \
         pricing, enabled, last_synced_at \
         FROM model_catalog WHERE 1=1",
    );
    let mut param_idx: u32 = 0;

    if q.provider.is_some() {
        param_idx += 1;
        sql.push_str(&format!(" AND provider_slug = ${param_idx}"));
    }
    if q.search.is_some() {
        param_idx += 1;
        sql.push_str(&format!(
            " AND (name ILIKE ${param_idx} OR model_slug ILIKE ${param_idx})"
        ));
    }
    if q.enabled.is_some() {
        param_idx += 1;
        sql.push_str(&format!(" AND enabled = ${param_idx}"));
    }
    sql.push_str(" ORDER BY provider_slug, model_slug LIMIT 5000");

    let mut query = sqlx::query(&sql);
    if let Some(ref provider) = q.provider {
        query = query.bind(provider);
    }
    if let Some(ref search) = q.search {
        query = query.bind(format!("%{search}%"));
    }
    if let Some(enabled) = q.enabled {
        query = query.bind(enabled);
    }

    let rows = query
        .fetch_all(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

    let out: Vec<ModelCatalogRow> = rows
        .iter()
        .map(|r| ModelCatalogRow {
            id: r.get("id"),
            name: r.get("name"),
            provider_slug: r.get("provider_slug"),
            model_slug: r.get("model_slug"),
            context_length: r.get("context_length"),
            pricing: r.get("pricing"),
            enabled: r.get("enabled"),
            last_synced_at: r.get("last_synced_at"),
        })
        .collect();

    Ok(Json(out))
}

async fn update_model_catalog(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<ModelCatalogUpdate>,
) -> Result<Json<ModelCatalogRow>> {
    admin_user_id(&state, &headers).await?;

    if body.enabled.is_none() && body.pricing.is_none() {
        return Err(AppError::BadRequest(
            "At least one of 'enabled' or 'pricing' must be provided".into(),
        ));
    }

    let mut set_clauses = Vec::new();
    let mut param_idx: u32 = 1; // $1 is the id

    if body.enabled.is_some() {
        param_idx += 1;
        set_clauses.push(format!("enabled = ${param_idx}"));
    }
    if body.pricing.is_some() {
        param_idx += 1;
        set_clauses.push(format!("pricing = ${param_idx}"));
    }

    let sql = format!(
        "UPDATE model_catalog SET {} WHERE id = $1 \
         RETURNING id, name, provider_slug, model_slug, context_length, \
         pricing, enabled, last_synced_at",
        set_clauses.join(", ")
    );

    let mut query = sqlx::query(&sql).bind(&id);
    if let Some(enabled) = body.enabled {
        query = query.bind(enabled);
    }
    if let Some(ref pricing) = body.pricing {
        query = query.bind(pricing);
    }

    let r = query
        .fetch_optional(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?
        .ok_or_else(|| AppError::NotFound("Model not found".into()))?;

    Ok(Json(ModelCatalogRow {
        id: r.get("id"),
        name: r.get("name"),
        provider_slug: r.get("provider_slug"),
        model_slug: r.get("model_slug"),
        context_length: r.get("context_length"),
        pricing: r.get("pricing"),
        enabled: r.get("enabled"),
        last_synced_at: r.get("last_synced_at"),
    }))
}

async fn sync_model_catalog(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>> {
    admin_user_id(&state, &headers).await?;

    let task_ref = Uuid::new_v4().to_string();
    let body = serde_json::json!({
        "project_id": "07ce3ace-5133-496a-ba55-9bf40bb5d3aa",
        "task_type": "pricing_sync",
        "task_ref": task_ref,
        "prompt": "",
        "internal": true,
    });

    let url = format!("{}/api/internal/agent-task", state.flow_url);
    let resp = state
        .http_client
        .post(&url)
        .header(
            "X-Project-Id",
            "07ce3ace-5133-496a-ba55-9bf40bb5d3aa",
        )
        .json(&body)
        .send()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to dispatch sync: {e}")))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(AppError::Internal(anyhow::anyhow!(
            "Sync dispatch failed ({status}): {text}"
        )));
    }

    Ok(Json(serde_json::json!({ "status": "sync_triggered", "task_ref": task_ref })))
}

// ── Tier management ─────────────────────────────────────────────────────

#[derive(Serialize)]
struct TierDefinitionResponse {
    id: Uuid,
    name: String,
    display_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    stripe_price_id: Option<String>,
    config: serde_json::Value,
    is_public: bool,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Deserialize)]
struct CreateTierRequest {
    name: String,
    display_name: String,
    stripe_price_id: Option<String>,
    #[serde(default = "default_empty_object")]
    config: serde_json::Value,
    #[serde(default = "default_true")]
    is_public: bool,
}

#[derive(Deserialize)]
struct UpdateTierRequest {
    display_name: Option<String>,
    stripe_price_id: Option<String>,
    config: Option<serde_json::Value>,
    is_public: Option<bool>,
}

#[derive(Deserialize)]
struct UpdateOrgTierRequest {
    tier_definition_id: Option<Uuid>,
    overrides: Option<OrgOverridesPayload>,
}

#[derive(Deserialize)]
struct OrgOverridesPayload {
    #[serde(default = "default_empty_object")]
    config_overrides: serde_json::Value,
    reason: Option<String>,
}

fn default_empty_object() -> serde_json::Value {
    serde_json::json!({})
}

fn default_true() -> bool {
    true
}

async fn tier_schema(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>> {
    admin_user_id(&state, &headers).await?;
    Ok(Json(reiver_core::entitlements::types::tier_schema()))
}

async fn list_tiers(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<TierDefinitionResponse>>> {
    admin_user_id(&state, &headers).await?;

    let rows = sqlx::query_as::<_, (Uuid, String, String, Option<String>, serde_json::Value, bool, DateTime<Utc>, DateTime<Utc>)>(
        "SELECT id, name, display_name, stripe_price_id, config, is_public, created_at, updated_at \
         FROM tier_definitions ORDER BY created_at",
    )
    .fetch_all(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

    let out: Vec<TierDefinitionResponse> = rows
        .into_iter()
        .map(|(id, name, display_name, stripe_price_id, config, is_public, created_at, updated_at)| {
            TierDefinitionResponse {
                id,
                name,
                display_name,
                stripe_price_id,
                config,
                is_public,
                created_at,
                updated_at,
            }
        })
        .collect();

    Ok(Json(out))
}

async fn create_tier(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(body): Json<CreateTierRequest>,
) -> Result<Json<TierDefinitionResponse>> {
    admin_user_id(&state, &headers).await?;

    if body.name.is_empty() || body.display_name.is_empty() {
        return Err(AppError::BadRequest("name and display_name are required".into()));
    }

    if let Some(ref pid) = body.stripe_price_id {
        if !pid.is_empty() && !pid.starts_with("price_") {
            return Err(AppError::BadRequest("stripe_price_id must start with 'price_'".into()));
        }
    }

    let row = sqlx::query_as::<_, (Uuid, String, String, Option<String>, serde_json::Value, bool, DateTime<Utc>, DateTime<Utc>)>(
        "INSERT INTO tier_definitions (name, display_name, stripe_price_id, config, is_public) \
         VALUES ($1, $2, $3, $4, $5) \
         RETURNING id, name, display_name, stripe_price_id, config, is_public, created_at, updated_at",
    )
    .bind(&body.name)
    .bind(&body.display_name)
    .bind(&body.stripe_price_id)
    .bind(&body.config)
    .bind(body.is_public)
    .fetch_one(&*state.db)
    .await
    .map_err(|e| {
        if e.to_string().contains("duplicate key") {
            AppError::BadRequest(format!("tier '{}' already exists", body.name))
        } else {
            AppError::Internal(anyhow::anyhow!("DB error: {}", e))
        }
    })?;

    state.entitlements.refresh_cache().await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("cache refresh failed: {}", e)))?;

    Ok(Json(TierDefinitionResponse {
        id: row.0,
        name: row.1,
        display_name: row.2,
        stripe_price_id: row.3,
        config: row.4,
        is_public: row.5,
        created_at: row.6,
        updated_at: row.7,
    }))
}

async fn update_tier(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(tier_id): Path<Uuid>,
    Json(body): Json<UpdateTierRequest>,
) -> Result<Json<TierDefinitionResponse>> {
    admin_user_id(&state, &headers).await?;

    if let Some(ref pid) = body.stripe_price_id {
        if !pid.is_empty() && !pid.starts_with("price_") {
            return Err(AppError::BadRequest("stripe_price_id must start with 'price_'".into()));
        }
    }

    let mut set_clauses = vec!["updated_at = NOW()".to_string()];
    let mut param_idx: u32 = 1; // $1 is the id

    if body.display_name.is_some() {
        param_idx += 1;
        set_clauses.push(format!("display_name = ${param_idx}"));
    }
    if body.stripe_price_id.is_some() {
        param_idx += 1;
        set_clauses.push(format!("stripe_price_id = ${param_idx}"));
    }
    if body.config.is_some() {
        param_idx += 1;
        set_clauses.push(format!("config = ${param_idx}"));
    }
    if body.is_public.is_some() {
        param_idx += 1;
        set_clauses.push(format!("is_public = ${param_idx}"));
    }

    if set_clauses.len() == 1 {
        return Err(AppError::BadRequest("no fields to update".into()));
    }

    let sql = format!(
        "UPDATE tier_definitions SET {} WHERE id = $1 \
         RETURNING id, name, display_name, stripe_price_id, config, is_public, created_at, updated_at",
        set_clauses.join(", ")
    );

    let mut query = sqlx::query_as::<_, (Uuid, String, String, Option<String>, serde_json::Value, bool, DateTime<Utc>, DateTime<Utc>)>(&sql)
        .bind(tier_id);

    if let Some(ref display_name) = body.display_name {
        query = query.bind(display_name);
    }
    if let Some(ref stripe_price_id) = body.stripe_price_id {
        let normalized: Option<&str> = if stripe_price_id.is_empty() {
            None
        } else {
            Some(stripe_price_id.as_str())
        };
        query = query.bind(normalized);
    }
    if let Some(ref config) = body.config {
        query = query.bind(config);
    }
    if let Some(is_public) = body.is_public {
        query = query.bind(is_public);
    }

    let row = query
        .fetch_optional(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?
        .ok_or_else(|| AppError::NotFound("tier not found".into()))?;

    state.entitlements.refresh_cache().await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("cache refresh failed: {}", e)))?;

    Ok(Json(TierDefinitionResponse {
        id: row.0,
        name: row.1,
        display_name: row.2,
        stripe_price_id: row.3,
        config: row.4,
        is_public: row.5,
        created_at: row.6,
        updated_at: row.7,
    }))
}

async fn get_org_entitlements(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(org_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>> {
    admin_user_id(&state, &headers).await?;

    let org_exists: Option<Uuid> = sqlx::query_scalar("SELECT id FROM organizations WHERE id = $1")
        .bind(org_id)
        .fetch_optional(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;
    if org_exists.is_none() {
        return Err(AppError::NotFound(format!("organization {} not found", org_id)));
    }

    let resolved = state
        .entitlements
        .get_config(org_id)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("{}", e)))?;

    let overrides_row = sqlx::query_as::<_, (Uuid, serde_json::Value, Option<String>, Option<Uuid>, DateTime<Utc>)>(
        "SELECT id, config_overrides, reason, created_by, created_at \
         FROM tier_overrides WHERE organization_id = $1",
    )
    .bind(org_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

    let overrides_json = overrides_row.map(|r| {
        serde_json::json!({
            "id": r.0,
            "config_overrides": r.1,
            "reason": r.2,
            "created_by": r.3,
            "created_at": r.4,
        })
    });

    Ok(Json(serde_json::json!({
        "resolved": resolved,
        "overrides": overrides_json,
    })))
}

async fn update_org_tier(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(org_id): Path<Uuid>,
    Json(body): Json<UpdateOrgTierRequest>,
) -> Result<Json<serde_json::Value>> {
    let admin_id = admin_user_id(&state, &headers).await?;

    let old_tier_name: Option<String> = sqlx::query_scalar(
        "SELECT td.name FROM organizations o JOIN tier_definitions td ON o.tier_definition_id = td.id WHERE o.id = $1"
    )
    .bind(org_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;
    if old_tier_name.is_none() {
        return Err(AppError::NotFound(format!("organization {} not found", org_id)));
    }
    let old_tier_name = old_tier_name.unwrap();

    if let Some(tier_def_id) = body.tier_definition_id {
        // Sync Stripe subscription BEFORE updating entitlements — if billing
        // fails the tier change must not go through.
        let new_price_id: Option<String> = sqlx::query_scalar(
            "SELECT stripe_price_id FROM tier_definitions WHERE id = $1",
        )
        .bind(tier_def_id)
        .fetch_optional(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?
        .flatten();

        if let Some(ref price_id) = new_price_id {
            if let Some(ref api_key) = state.config.stripe_api_key {
                let provider = StripePaymentProvider::new(
                    api_key,
                    state.db.clone(),
                    state.redis.clone(),
                    state.config.stripe_webhook_secret.clone(),
                    state.config.stripe_metered_price_id.clone(),
                );
                provider
                    .update_subscription(org_id, price_id, None)
                    .await
                    .map_err(|e| {
                        AppError::Internal(anyhow::anyhow!(
                            "Failed to update Stripe subscription: {}",
                            e
                        ))
                    })?;
            }
        }

        let result = sqlx::query("UPDATE organizations SET tier_definition_id = $1 WHERE id = $2")
            .bind(tier_def_id)
            .bind(org_id)
            .execute(&*state.db)
            .await
            .map_err(|e| {
                if e.to_string().contains("violates foreign key") {
                    AppError::BadRequest(format!("tier definition {} does not exist", tier_def_id))
                } else {
                    AppError::Internal(anyhow::anyhow!("DB error: {}", e))
                }
            })?;
        if result.rows_affected() == 0 {
            return Err(AppError::NotFound(format!("organization {} not found", org_id)));
        }
    }

    let overrides_changed = body.overrides.is_some();
    if let Some(ov) = body.overrides {
        sqlx::query(
            "INSERT INTO tier_overrides (organization_id, config_overrides, reason, created_by) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (organization_id) DO UPDATE SET \
                config_overrides = EXCLUDED.config_overrides, \
                reason = EXCLUDED.reason, \
                created_by = EXCLUDED.created_by",
        )
        .bind(org_id)
        .bind(&ov.config_overrides)
        .bind(&ov.reason)
        .bind(admin_id)
        .execute(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;
    }

    state.entitlements.refresh_cache().await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("cache refresh failed: {}", e)))?;

    let resolved = state
        .entitlements
        .get_config(org_id)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("{}", e)))?;

    // Downgrade warnings
    let mut warnings: Vec<String> = Vec::new();

    let project_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM projects WHERE organization_id = $1")
        .bind(org_id)
        .fetch_one(&*state.db)
        .await
        .unwrap_or(0);
    let max_projects = resolved.config.platform.max_projects;
    if max_projects >= 0 && project_count > max_projects {
        warnings.push(format!(
            "Organization has {} projects but new tier allows {}",
            project_count, max_projects
        ));
    }

    if !resolved.config.platform.sso {
        let active_sso: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sso_configurations WHERE organization_id = $1 AND enabled = true"
        )
        .bind(org_id)
        .fetch_one(&*state.db)
        .await
        .unwrap_or(0);
        if active_sso > 0 {
            warnings.push(format!(
                "Organization has {} active SSO configuration(s) but SSO is disabled on the new tier",
                active_sso
            ));
        }

        let scim_active: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sso_configurations WHERE organization_id = $1 AND scim_enabled = true"
        )
        .bind(org_id)
        .fetch_one(&*state.db)
        .await
        .unwrap_or(0);
        if scim_active > 0 {
            warnings.push(format!(
                "Organization has {} active SCIM provisioning configuration(s) but SSO/SCIM is disabled on the new tier",
                scim_active
            ));
        }
    }

    // Audit event
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::TierChanged)
        .actor(admin_id)
        .organization(org_id)
        .details(serde_json::json!({
            "before": { "tier": &old_tier_name },
            "after": { "tier": &resolved.name },
            "tier_definition_changed": body.tier_definition_id.is_some(),
            "overrides_changed": overrides_changed,
            "warnings": &warnings,
        }))
        .origin(
            &audit_origin.origin_type,
            &audit_origin.origin_ref,
            &audit_origin.origin_reason,
        )
        .caller(
            &audit_caller.caller_type,
            &audit_caller.key_label,
            &audit_caller.key_prefix,
        )
        .success()
        .log(&state.clickhouse)
        .await;

    Ok(Json(serde_json::json!({
        "status": "updated",
        "resolved": resolved,
        "warnings": warnings,
    })))
}

async fn delete_org_overrides(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(org_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>> {
    admin_user_id(&state, &headers).await?;

    let org_exists: Option<Uuid> = sqlx::query_scalar("SELECT id FROM organizations WHERE id = $1")
        .bind(org_id)
        .fetch_optional(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;
    if org_exists.is_none() {
        return Err(AppError::NotFound(format!("organization {} not found", org_id)));
    }

    sqlx::query("DELETE FROM tier_overrides WHERE organization_id = $1")
        .bind(org_id)
        .execute(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

    state.entitlements.refresh_cache().await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("cache refresh failed: {}", e)))?;

    let resolved = state
        .entitlements
        .get_config(org_id)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("{}", e)))?;

    Ok(Json(serde_json::json!({
        "status": "overrides_removed",
        "resolved": resolved,
    })))
}

async fn delete_tier(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(tier_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>> {
    admin_user_id(&state, &headers).await?;

    let in_use_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM organizations WHERE tier_definition_id = $1",
    )
    .bind(tier_id)
    .fetch_one(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

    if in_use_count > 0 {
        return Err(AppError::Conflict(format!(
            "Cannot delete tier: {} organization(s) are currently assigned to it",
            in_use_count
        )));
    }

    let result = sqlx::query("DELETE FROM tier_definitions WHERE id = $1")
        .bind(tier_id)
        .execute(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound(format!("tier definition {} not found", tier_id)));
    }

    state.entitlements.refresh_cache().await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("{}", e)))?;

    Ok(Json(serde_json::json!({ "status": "deleted" })))
}

async fn list_organizations_with_tiers(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<serde_json::Value>>> {
    admin_user_id(&state, &headers).await?;

    let rows = sqlx::query(
        r#"
        SELECT
            o.id,
            o.name,
            o.domain,
            o.tier_definition_id,
            td.name AS tier_name,
            td.display_name AS tier_display_name,
            COALESCE(mc.member_count, 0) AS member_count,
            ow.email AS owner_email
        FROM organizations o
        JOIN tier_definitions td ON td.id = o.tier_definition_id
        LEFT JOIN (
            SELECT organization_id, COUNT(*) AS member_count
            FROM memberships
            WHERE status = 'active'
            GROUP BY organization_id
        ) mc ON mc.organization_id = o.id
        LEFT JOIN LATERAL (
            SELECT u.email
            FROM memberships m
            JOIN users u ON u.id = m.user_id
            WHERE m.organization_id = o.id AND m.role = 'owner'
            LIMIT 1
        ) ow ON true
        ORDER BY o.name
        "#,
    )
    .fetch_all(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

    let has_overrides: std::collections::HashSet<Uuid> = sqlx::query_scalar::<_, Uuid>(
        "SELECT organization_id FROM tier_overrides",
    )
    .fetch_all(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?
    .into_iter()
    .collect();

    let out: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| {
            let id: Uuid = r.get("id");
            serde_json::json!({
                "id": id,
                "name": r.get::<String, _>("name"),
                "domain": r.get::<Option<String>, _>("domain"),
                "tier_definition_id": r.get::<Uuid, _>("tier_definition_id"),
                "tier_name": r.get::<String, _>("tier_name"),
                "tier_display_name": r.get::<String, _>("tier_display_name"),
                "member_count": r.get::<i64, _>("member_count"),
                "owner_email": r.get::<Option<String>, _>("owner_email"),
                "has_overrides": has_overrides.contains(&id),
            })
        })
        .collect();

    Ok(Json(out))
}

// ── Organization members (admin) ────────────────────────────────────────

#[derive(Serialize)]
struct OrgMemberRow {
    user_id: Uuid,
    email: String,
    role: String,
    membership_status: String,
    is_approved: bool,
    is_platform_admin: bool,
    joined_at: DateTime<Utc>,
    user_created_at: DateTime<Utc>,
}

async fn list_org_members(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(org_id): Path<Uuid>,
) -> Result<Json<Vec<OrgMemberRow>>> {
    admin_user_id(&state, &headers).await?;

    let org_exists: Option<Uuid> = sqlx::query_scalar("SELECT id FROM organizations WHERE id = $1")
        .bind(org_id)
        .fetch_optional(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;
    if org_exists.is_none() {
        return Err(AppError::NotFound(format!("organization {} not found", org_id)));
    }

    let rows = sqlx::query(
        r#"
        SELECT
            u.id AS user_id,
            u.email,
            m.role,
            m.status AS membership_status,
            u.is_approved,
            u.is_platform_admin,
            m.created_at AS joined_at,
            u.created_at AS user_created_at
        FROM memberships m
        JOIN users u ON u.id = m.user_id
        WHERE m.organization_id = $1
        ORDER BY m.role = 'owner' DESC, u.email
        "#,
    )
    .bind(org_id)
    .fetch_all(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {}", e)))?;

    let out: Vec<OrgMemberRow> = rows
        .iter()
        .map(|r| OrgMemberRow {
            user_id: r.get("user_id"),
            email: r.get("email"),
            role: r.get("role"),
            membership_status: r.get("membership_status"),
            is_approved: r.get("is_approved"),
            is_platform_admin: r.get("is_platform_admin"),
            joined_at: r.get("joined_at"),
            user_created_at: r.get("user_created_at"),
        })
        .collect();

    Ok(Json(out))
}

// ============================================================================
// Dashboard Reconvert
// ============================================================================

#[derive(Serialize)]
struct ReconvertResult {
    reconverted: usize,
    failed: usize,
    errors: Vec<String>,
}

async fn reconvert_dashboards(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> Result<Json<ReconvertResult>> {
    admin_user_id(&state, &headers).await?;

    let dashboards = sqlx::query_as::<_, Dashboard>("SELECT * FROM dashboards")
        .fetch_all(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to fetch dashboards: {}", e)))?;

    let mut reconverted = 0usize;
    let mut failed = 0usize;
    let mut errors = Vec::new();

    for dashboard in &dashboards {
        let source_type = dashboard
            .import_source
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let convert_result = match source_type {
            "grafana" => {
                let payload = match serde_json::from_value::<
                    super::grafana::GrafanaDashboardExport,
                >(
                    dashboard.import_source["payload"].clone(),
                ) {
                    Ok(p) => p,
                    Err(e) => {
                        failed += 1;
                        errors.push(format!(
                            "Dashboard {} ({}): failed to deserialize Grafana payload: {}",
                            dashboard.id, dashboard.name, e
                        ));
                        continue;
                    }
                };
                super::grafana::convert_dashboard(payload)
            }
            "datadog" => {
                let payload = match serde_json::from_value::<
                    super::migration::DatadogDashboard,
                >(
                    dashboard.import_source["payload"].clone(),
                ) {
                    Ok(p) => p,
                    Err(e) => {
                        failed += 1;
                        errors.push(format!(
                            "Dashboard {} ({}): failed to deserialize Datadog payload: {}",
                            dashboard.id, dashboard.name, e
                        ));
                        continue;
                    }
                };
                super::migration::convert_dashboard(payload)
            }
            _ => {
                continue;
            }
        };

        // Delete existing tabs (widgets cascade via FK).
        if let Err(e) =
            sqlx::query("DELETE FROM dashboard_tabs WHERE dashboard_id = $1")
                .bind(dashboard.id)
                .execute(&*state.db)
                .await
        {
            failed += 1;
            errors.push(format!(
                "Dashboard {} ({}): failed to delete old tabs: {}",
                dashboard.id, dashboard.name, e
            ));
            continue;
        }

        // Also delete any widgets not attached to a tab.
        let _ = sqlx::query(
            "DELETE FROM dashboard_widgets WHERE dashboard_id = $1 AND tab_id IS NULL",
        )
        .bind(dashboard.id)
        .execute(&*state.db)
        .await;

        let now = Utc::now();
        let layout_config = serde_json::json!({ "variables": convert_result.variables });

        if let Err(e) = sqlx::query(
            "UPDATE dashboards SET layout_config = $1, updated_at = $2 WHERE id = $3",
        )
        .bind(&layout_config)
        .bind(now)
        .bind(dashboard.id)
        .execute(&*state.db)
        .await
        {
            failed += 1;
            errors.push(format!(
                "Dashboard {} ({}): failed to update layout_config: {}",
                dashboard.id, dashboard.name, e
            ));
            continue;
        }

        let mut tab_ok = true;
        for (tab_index, tab) in convert_result.tabs.iter().enumerate() {
            let created_tab = match sqlx::query_as::<_, DashboardTab>(
                r#"INSERT INTO dashboard_tabs (dashboard_id, name, display_order, icon, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $5)
                RETURNING *"#,
            )
            .bind(dashboard.id)
            .bind(&tab.name)
            .bind(tab_index as i32)
            .bind(tab.icon.as_deref())
            .bind(now)
            .fetch_one(&*state.db)
            .await
            {
                Ok(t) => t,
                Err(e) => {
                    failed += 1;
                    errors.push(format!(
                        "Dashboard {} ({}): failed to insert tab '{}': {}",
                        dashboard.id, dashboard.name, tab.name, e
                    ));
                    tab_ok = false;
                    break;
                }
            };

            for widget in &tab.widgets {
                if let Err(e) = sqlx::query(
                    r#"INSERT INTO dashboard_widgets (dashboard_id, tab_id, widget_type, widget_config, position_x, position_y, width, height, title, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)"#,
                )
                .bind(dashboard.id)
                .bind(created_tab.id)
                .bind(&widget.widget_type)
                .bind(&widget.config)
                .bind(widget.x)
                .bind(widget.y)
                .bind(widget.w)
                .bind(widget.h)
                .bind(widget.title.as_deref())
                .bind(now)
                .execute(&*state.db)
                .await
                {
                    failed += 1;
                    errors.push(format!(
                        "Dashboard {} ({}): failed to insert widget: {}",
                        dashboard.id, dashboard.name, e
                    ));
                    tab_ok = false;
                    break;
                }
            }
            if !tab_ok {
                break;
            }
        }

        if tab_ok {
            reconverted += 1;
        }
    }

    Ok(Json(ReconvertResult {
        reconverted,
        failed,
        errors,
    }))
}

// ── Knowledge Base CRUD (pgvector) ──────────────────────────────────────

use super::chunking;

#[derive(Serialize)]
struct KbDocumentResponse {
    id: Uuid,
    title: String,
    category: String,
    source_type: String,
    original_content: Option<String>,
    original_filename: Option<String>,
    severity: String,
    enabled: bool,
    embedding_status: String,
    embedding_error: Option<String>,
    chunk_count: i64,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Deserialize)]
struct CreateKbDocumentRequest {
    title: String,
    category: String,
    content: String,
    #[serde(default = "default_severity")]
    severity: String,
}

fn default_severity() -> String {
    "info".into()
}

#[derive(Deserialize)]
struct UpdateKbDocumentRequest {
    title: Option<String>,
    category: Option<String>,
    content: Option<String>,
    severity: Option<String>,
    enabled: Option<bool>,
}

/// Run chunking + embedding for a document in the background.
///
/// Uses an atomic CAS on `embedding_status` to prevent concurrent tasks for
/// the same document. Sets status to "processing" only if it was "pending",
/// then to "ready" on success or "failed" (with `embedding_error`) on failure.
fn spawn_embed_task(
    db: Arc<reiver_core::db::DbPool>,
    embedder: Arc<reiver_core::embeddings::KbEmbedder>,
    doc_id: Uuid,
    title: String,
    text: String,
) {
    tokio::spawn(async move {
        // Atomic claim: only proceed if status is still "pending".
        let claimed = sqlx::query(
            "UPDATE knowledge_base_documents \
             SET embedding_status = 'processing', embedding_error = NULL, updated_at = NOW() \
             WHERE id = $1 AND embedding_status = 'pending'",
        )
        .bind(doc_id)
        .execute(&*db)
        .await;

        match claimed {
            Ok(r) if r.rows_affected() == 0 => {
                tracing::debug!(doc_id = %doc_id, "KB embed skipped: not in pending state (concurrent task or deleted)");
                return;
            }
            Err(e) => {
                tracing::error!(doc_id = %doc_id, error = %e, "KB embed: failed to claim document");
                return;
            }
            _ => {}
        }

        match embed_document(&db, &embedder, doc_id, &title, &text).await {
            Ok(_) => {
                if let Err(e) = sqlx::query(
                    "UPDATE knowledge_base_documents \
                     SET embedding_status = 'ready', embedding_error = NULL, updated_at = NOW() \
                     WHERE id = $1",
                )
                .bind(doc_id)
                .execute(&*db)
                .await
                {
                    tracing::error!(doc_id = %doc_id, error = %e, "KB embed: failed to set ready status");
                }
            }
            Err(e) => {
                let msg = format!("{e:#}");
                tracing::error!(doc_id = %doc_id, error = %msg, "KB embedding failed");
                if let Err(db_err) = sqlx::query(
                    "UPDATE knowledge_base_documents \
                     SET embedding_status = 'failed', embedding_error = $2, updated_at = NOW() \
                     WHERE id = $1",
                )
                .bind(doc_id)
                .bind(&msg)
                .execute(&*db)
                .await
                {
                    tracing::error!(doc_id = %doc_id, error = %db_err, "KB embed: failed to set failed status");
                }
            }
        }
    });
}

async fn embed_document(
    db: &sqlx::PgPool,
    embedder: &reiver_core::embeddings::KbEmbedder,
    doc_id: Uuid,
    title: &str,
    text: &str,
) -> anyhow::Result<i64> {
    sqlx::query("DELETE FROM knowledge_base_chunks WHERE document_id = $1")
        .bind(doc_id)
        .execute(db)
        .await?;

    let chunks = chunking::chunk_text(title, text);
    if chunks.is_empty() {
        return Ok(0);
    }

    let texts: Vec<String> = chunks.iter().map(|c| c.text.clone()).collect();
    let embeddings = embedder.embed(texts).await?;

    let count = chunks.len() as i64;
    for (chunk, embedding) in chunks.iter().zip(embeddings.iter()) {
        let vec_str = format!(
            "[{}]",
            embedding
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        );
        sqlx::query(
            "INSERT INTO knowledge_base_chunks (document_id, content, chunk_index, embedding) \
             VALUES ($1, $2, $3, $4::vector)",
        )
        .bind(doc_id)
        .bind(&chunk.text)
        .bind(chunk.index as i32)
        .bind(&vec_str)
        .execute(db)
        .await?;
    }

    Ok(count)
}

fn kb_doc_response(row: &sqlx::postgres::PgRow) -> KbDocumentResponse {
    KbDocumentResponse {
        id: row.get("id"),
        title: row.get("title"),
        category: row.get("category"),
        source_type: row.get("source_type"),
        original_content: row.get("original_content"),
        original_filename: row.get("original_filename"),
        severity: row.get("severity"),
        enabled: row.get("enabled"),
        embedding_status: row.get("embedding_status"),
        embedding_error: row.get("embedding_error"),
        chunk_count: row.get("chunk_count"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

async fn get_document_with_chunk_count(
    db: &sqlx::PgPool,
    doc_id: Uuid,
) -> Result<KbDocumentResponse> {
    let row = sqlx::query(
        "SELECT d.*, \
            (SELECT COUNT(*) FROM knowledge_base_chunks WHERE document_id = d.id) AS chunk_count \
         FROM knowledge_base_documents d WHERE d.id = $1",
    )
    .bind(doc_id)
    .fetch_optional(db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {e}")))?
    .ok_or_else(|| AppError::NotFound("knowledge base document not found".into()))?;

    Ok(kb_doc_response(&row))
}

async fn list_kb_documents(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<KbDocumentResponse>>> {
    admin_user_id(&state, &headers).await?;

    let rows = sqlx::query(
        "SELECT d.*, \
            (SELECT COUNT(*) FROM knowledge_base_chunks WHERE document_id = d.id) AS chunk_count \
         FROM knowledge_base_documents d \
         ORDER BY d.category, d.title",
    )
    .fetch_all(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {e}")))?;

    Ok(Json(rows.iter().map(kb_doc_response).collect()))
}

async fn create_kb_document(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(body): Json<CreateKbDocumentRequest>,
) -> Result<Json<KbDocumentResponse>> {
    admin_user_id(&state, &headers).await?;

    if body.title.trim().is_empty()
        || body.content.trim().is_empty()
        || body.category.trim().is_empty()
    {
        return Err(AppError::BadRequest(
            "title, category, and content are required (cannot be blank)".into(),
        ));
    }

    let doc = sqlx::query_as::<_, reiver_core::models::KnowledgeBaseDocument>(
        "INSERT INTO knowledge_base_documents \
            (title, category, source_type, original_content, severity, embedding_status) \
         VALUES ($1, $2, 'manual', $3, $4, 'pending') \
         RETURNING *",
    )
    .bind(&body.title)
    .bind(&body.category)
    .bind(&body.content)
    .bind(&body.severity)
    .fetch_one(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {e}")))?;

    spawn_embed_task(
        state.db.clone(),
        state.kb_embedder.clone(),
        doc.id,
        doc.title.clone(),
        body.content,
    );

    get_document_with_chunk_count(&state.db, doc.id).await.map(Json)
}

async fn upload_kb_document(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    mut multipart: axum::extract::Multipart,
) -> Result<Json<KbDocumentResponse>> {
    admin_user_id(&state, &headers).await?;

    let mut title = String::new();
    let mut category = String::new();
    let mut severity = "info".to_string();
    let mut file_data: Option<Vec<u8>> = None;
    let mut file_name = String::new();

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| AppError::BadRequest(format!("multipart error: {e}")))?
    {
        let name = field.name().unwrap_or("").to_string();
        match name.as_str() {
            "title" => {
                title = field
                    .text()
                    .await
                    .map_err(|e| AppError::BadRequest(format!("read error: {e}")))?;
            }
            "category" => {
                category = field
                    .text()
                    .await
                    .map_err(|e| AppError::BadRequest(format!("read error: {e}")))?;
            }
            "severity" => {
                severity = field
                    .text()
                    .await
                    .map_err(|e| AppError::BadRequest(format!("read error: {e}")))?;
            }
            "file" => {
                file_name = field.file_name().unwrap_or("unknown").to_string();
                file_data = Some(
                    field
                        .bytes()
                        .await
                        .map_err(|e| AppError::BadRequest(format!("read error: {e}")))?
                        .to_vec(),
                );
            }
            _ => {}
        }
    }

    let data = file_data.ok_or_else(|| AppError::BadRequest("file is required".into()))?;
    if title.trim().is_empty() || category.trim().is_empty() {
        return Err(AppError::BadRequest(
            "title and category are required (cannot be blank)".into(),
        ));
    }

    let lower_name = file_name.to_lowercase();
    let source_type = if lower_name.ends_with(".pdf") {
        "pdf"
    } else {
        "markdown"
    };

    let text = if source_type == "pdf" {
        chunking::extract_text_from_pdf(&data)
            .map_err(|e| AppError::BadRequest(format!("Failed to extract PDF text: {e}")))?
    } else {
        String::from_utf8(data)
            .map_err(|e| AppError::BadRequest(format!("File is not valid UTF-8: {e}")))?
    };

    if text.trim().is_empty() {
        return Err(AppError::BadRequest(
            "Extracted text is empty".into(),
        ));
    }

    let doc = sqlx::query_as::<_, reiver_core::models::KnowledgeBaseDocument>(
        "INSERT INTO knowledge_base_documents \
            (title, category, source_type, original_content, original_filename, severity, embedding_status) \
         VALUES ($1, $2, $3, $4, $5, $6, 'pending') \
         RETURNING *",
    )
    .bind(&title)
    .bind(&category)
    .bind(source_type)
    .bind(&text)
    .bind(&file_name)
    .bind(&severity)
    .fetch_one(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {e}")))?;

    spawn_embed_task(
        state.db.clone(),
        state.kb_embedder.clone(),
        doc.id,
        doc.title.clone(),
        text,
    );

    get_document_with_chunk_count(&state.db, doc.id).await.map(Json)
}

async fn update_kb_document(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateKbDocumentRequest>,
) -> Result<Json<KbDocumentResponse>> {
    admin_user_id(&state, &headers).await?;

    let existing = sqlx::query_as::<_, reiver_core::models::KnowledgeBaseDocument>(
        "SELECT * FROM knowledge_base_documents WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {e}")))?
    .ok_or_else(|| AppError::NotFound("document not found".into()))?;

    let new_title = body.title.unwrap_or_else(|| existing.title.clone());
    let new_category = body.category.as_deref().unwrap_or(&existing.category);
    let new_severity = body.severity.as_deref().unwrap_or(&existing.severity);
    let new_enabled = body.enabled.unwrap_or(existing.enabled);
    let content_changed = body.content.is_some();

    let new_status = if content_changed { "pending" } else { &existing.embedding_status };

    if content_changed {
        sqlx::query(
            "UPDATE knowledge_base_documents \
             SET title = $1, category = $2, severity = $3, enabled = $4, \
                 original_content = COALESCE($5, original_content), \
                 embedding_status = $6, embedding_error = NULL, \
                 updated_at = NOW() \
             WHERE id = $7",
        )
        .bind(&new_title)
        .bind(new_category)
        .bind(new_severity)
        .bind(new_enabled)
        .bind(body.content.as_deref())
        .bind(new_status)
        .bind(id)
        .execute(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {e}")))?;
    } else {
        sqlx::query(
            "UPDATE knowledge_base_documents \
             SET title = $1, category = $2, severity = $3, enabled = $4, \
                 updated_at = NOW() \
             WHERE id = $5",
        )
        .bind(&new_title)
        .bind(new_category)
        .bind(new_severity)
        .bind(new_enabled)
        .bind(id)
        .execute(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {e}")))?;
    }

    if content_changed {
        let new_content = body.content.unwrap();
        spawn_embed_task(
            state.db.clone(),
            state.kb_embedder.clone(),
            id,
            new_title,
            new_content,
        );
    }

    get_document_with_chunk_count(&state.db, id).await.map(Json)
}

async fn delete_kb_document(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>> {
    admin_user_id(&state, &headers).await?;

    let result = sqlx::query("DELETE FROM knowledge_base_documents WHERE id = $1")
        .bind(id)
        .execute(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {e}")))?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound("document not found".into()));
    }

    Ok(Json(serde_json::json!({ "deleted": true })))
}

async fn reembed_kb_document(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<KbDocumentResponse>> {
    admin_user_id(&state, &headers).await?;

    let doc = sqlx::query_as::<_, reiver_core::models::KnowledgeBaseDocument>(
        "SELECT * FROM knowledge_base_documents WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {e}")))?
    .ok_or_else(|| AppError::NotFound("document not found".into()))?;

    let text = doc
        .original_content
        .clone()
        .ok_or_else(|| AppError::BadRequest("document has no stored content to re-embed".into()))?;

    sqlx::query(
        "UPDATE knowledge_base_documents \
         SET embedding_status = 'pending', embedding_error = NULL, updated_at = NOW() \
         WHERE id = $1",
    )
    .bind(id)
    .execute(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {e}")))?;

    spawn_embed_task(
        state.db.clone(),
        state.kb_embedder.clone(),
        id,
        doc.title,
        text,
    );

    get_document_with_chunk_count(&state.db, id).await.map(Json)
}

// ── Dashboard Templates CRUD (admin) ────────────────────────────────────

#[derive(Serialize)]
struct AdminTemplateResponse {
    id: Uuid,
    name: String,
    description: Option<String>,
    category: String,
    thumbnail_url: Option<String>,
    template_config: serde_json::Value,
    tags: Vec<String>,
    is_featured: bool,
    display_order: i32,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Deserialize)]
struct CreateTemplateRequest {
    name: String,
    description: Option<String>,
    #[serde(default = "default_template_category")]
    category: String,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    is_featured: bool,
    #[serde(default)]
    display_order: i32,
    #[serde(default = "default_empty_object")]
    template_config: serde_json::Value,
}

#[derive(Deserialize)]
struct UpdateTemplateRequest {
    name: Option<String>,
    description: Option<String>,
    category: Option<String>,
    tags: Option<Vec<String>>,
    is_featured: Option<bool>,
    display_order: Option<i32>,
    template_config: Option<serde_json::Value>,
}

fn default_template_category() -> String {
    "general".into()
}

fn admin_template_response(row: &sqlx::postgres::PgRow) -> AdminTemplateResponse {
    AdminTemplateResponse {
        id: row.get("id"),
        name: row.get("name"),
        description: row.get("description"),
        category: row.get("category"),
        thumbnail_url: row.get("thumbnail_url"),
        template_config: row.get("template_config"),
        tags: row.get("tags"),
        is_featured: row.get("is_featured"),
        display_order: row.get("display_order"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

async fn list_admin_templates(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<AdminTemplateResponse>>> {
    admin_user_id(&state, &headers).await?;

    let rows = sqlx::query(
        "SELECT * FROM dashboard_templates ORDER BY display_order ASC, name ASC",
    )
    .fetch_all(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {e}")))?;

    Ok(Json(rows.iter().map(admin_template_response).collect()))
}

async fn create_admin_template(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(body): Json<CreateTemplateRequest>,
) -> Result<Json<AdminTemplateResponse>> {
    admin_user_id(&state, &headers).await?;

    if body.name.trim().is_empty() {
        return Err(AppError::BadRequest("name is required".into()));
    }

    let row = sqlx::query(
        "INSERT INTO dashboard_templates (name, description, category, tags, is_featured, display_order, template_config) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) \
         RETURNING *",
    )
    .bind(body.name.trim())
    .bind(&body.description)
    .bind(&body.category)
    .bind(&body.tags)
    .bind(body.is_featured)
    .bind(body.display_order)
    .bind(&body.template_config)
    .fetch_one(&*state.db)
    .await
    .map_err(|e| {
        if e.to_string().contains("duplicate key") || e.to_string().contains("unique") {
            AppError::BadRequest(format!("template '{}' already exists", body.name.trim()))
        } else {
            AppError::Internal(anyhow::anyhow!("DB error: {e}"))
        }
    })?;

    Ok(Json(admin_template_response(&row)))
}

async fn update_admin_template(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateTemplateRequest>,
) -> Result<Json<AdminTemplateResponse>> {
    admin_user_id(&state, &headers).await?;

    let mut set_clauses = vec!["updated_at = NOW()".to_string()];
    let mut param_idx: u32 = 1; // $1 is the id

    if body.name.is_some() {
        param_idx += 1;
        set_clauses.push(format!("name = ${param_idx}"));
    }
    if body.description.is_some() {
        param_idx += 1;
        set_clauses.push(format!("description = ${param_idx}"));
    }
    if body.category.is_some() {
        param_idx += 1;
        set_clauses.push(format!("category = ${param_idx}"));
    }
    if body.tags.is_some() {
        param_idx += 1;
        set_clauses.push(format!("tags = ${param_idx}"));
    }
    if body.is_featured.is_some() {
        param_idx += 1;
        set_clauses.push(format!("is_featured = ${param_idx}"));
    }
    if body.display_order.is_some() {
        param_idx += 1;
        set_clauses.push(format!("display_order = ${param_idx}"));
    }
    if body.template_config.is_some() {
        param_idx += 1;
        set_clauses.push(format!("template_config = ${param_idx}"));
    }

    if set_clauses.len() == 1 {
        return Err(AppError::BadRequest("no fields to update".into()));
    }

    let sql = format!(
        "UPDATE dashboard_templates SET {} WHERE id = $1 RETURNING *",
        set_clauses.join(", ")
    );

    let mut query = sqlx::query(&sql).bind(id);

    if let Some(ref name) = body.name {
        if name.trim().is_empty() {
            return Err(AppError::BadRequest("name cannot be empty".into()));
        }
        query = query.bind(name.trim());
    }
    if let Some(ref description) = body.description {
        query = query.bind(description);
    }
    if let Some(ref category) = body.category {
        query = query.bind(category);
    }
    if let Some(ref tags) = body.tags {
        query = query.bind(tags);
    }
    if let Some(is_featured) = body.is_featured {
        query = query.bind(is_featured);
    }
    if let Some(display_order) = body.display_order {
        query = query.bind(display_order);
    }
    if let Some(ref template_config) = body.template_config {
        query = query.bind(template_config);
    }

    let row = query
        .fetch_optional(&*state.db)
        .await
        .map_err(|e| {
            if e.to_string().contains("duplicate key") || e.to_string().contains("unique") {
                AppError::BadRequest("a template with that name already exists".into())
            } else {
                AppError::Internal(anyhow::anyhow!("DB error: {e}"))
            }
        })?
        .ok_or_else(|| AppError::NotFound("template not found".into()))?;

    Ok(Json(admin_template_response(&row)))
}

async fn delete_admin_template(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>> {
    admin_user_id(&state, &headers).await?;

    let result = sqlx::query("DELETE FROM dashboard_templates WHERE id = $1")
        .bind(id)
        .execute(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("DB error: {e}")))?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound("template not found".into()));
    }

    Ok(Json(serde_json::json!({ "deleted": true })))
}
