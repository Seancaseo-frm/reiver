//! Health Check API endpoints
//!
//! Manages health check configurations and results for synthetic monitoring.

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::Json,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info};
use uuid::Uuid;

use reiver_core::audit::{AuditCaller, AuditEventBuilder, AuditEventType, AuditOrigin};

use crate::app_state::WatchState;
use crate::error::{AppError, Result};

pub fn create_health_checks_router() -> Router<Arc<WatchState>> {
    Router::new()
        .route("/checks", get(list_checks).post(create_check))
        .route(
            "/checks/{id}",
            get(get_check).put(update_check).delete(delete_check),
        )
        .route("/checks/{id}/results", get(get_check_results))
        .route("/results", post(report_result)) // Agent reports results here
        .route("/uptime", get(get_uptime_summary))
}

// ============================================================================
// Types
// ============================================================================

#[derive(Debug, Serialize)]
pub struct HealthCheck {
    pub id: Uuid,
    pub project_id: Uuid,
    pub check_type: String,
    pub name: String,
    pub target_url: Option<String>,
    pub target_host: Option<String>,
    pub target_port: Option<i32>,
    pub http_method: Option<String>,
    pub http_expected_status: Option<Vec<i32>>,
    pub http_expected_body: Option<String>,
    pub ssl_expiry_warning_days: Option<i32>,
    pub check_interval_seconds: i32,
    pub timeout_seconds: Option<i32>,
    // Locations
    pub locations: Vec<String>,
    // Assertions
    pub response_time_threshold_ms: Option<i32>,
    // Alert conditions
    pub min_failing_locations: i32,
    pub alert_after_minutes: i32,
    pub failure_threshold: i32,
    pub success_threshold: i32,
    // Status
    pub enabled: bool,
    pub last_check_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_status: Option<String>,
    pub location_statuses: Option<serde_json::Value>,
    pub consecutive_failures: i32,
    pub consecutive_successes: i32,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)] // Full schema - not all fields used in current implementation
struct HealthCheckRow {
    id: Uuid,
    project_id: Uuid,
    check_type: String,
    name: String,
    target_url: Option<String>,
    target_host: Option<String>,
    target_port: Option<i32>,
    http_method: Option<String>,
    http_headers: Option<serde_json::Value>,
    http_body: Option<String>,
    http_expected_status: Option<Vec<i32>>,
    http_expected_body: Option<String>,
    http_follow_redirects: Option<bool>,
    http_timeout_ms: Option<i32>,
    tcp_send_data: Option<String>,
    tcp_expect_data: Option<String>,
    ssl_check_expiry: Option<bool>,
    ssl_expiry_warning_days: Option<i32>,
    ssl_check_chain: Option<bool>,
    check_interval_seconds: i32,
    timeout_seconds: Option<i32>,
    // Locations
    locations: Option<Vec<String>>,
    // Assertions
    response_time_threshold_ms: Option<i32>,
    // Alert conditions
    min_failing_locations: Option<i32>,
    alert_after_minutes: Option<i32>,
    failure_threshold: Option<i32>,
    success_threshold: Option<i32>,
    // Status
    enabled: bool,
    last_check_at: Option<chrono::DateTime<chrono::Utc>>,
    last_status: Option<String>,
    consecutive_failures: Option<i32>,
    consecutive_successes: Option<i32>,
    location_statuses: Option<serde_json::Value>,
    alert_triggered_at: Option<chrono::DateTime<chrono::Utc>>,
    tags: Option<serde_json::Value>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<HealthCheckRow> for HealthCheck {
    fn from(row: HealthCheckRow) -> Self {
        Self {
            id: row.id,
            project_id: row.project_id,
            check_type: row.check_type,
            name: row.name,
            target_url: row.target_url,
            target_host: row.target_host,
            target_port: row.target_port,
            http_method: row.http_method,
            http_expected_status: row.http_expected_status,
            http_expected_body: row.http_expected_body,
            ssl_expiry_warning_days: row.ssl_expiry_warning_days,
            check_interval_seconds: row.check_interval_seconds,
            timeout_seconds: row.timeout_seconds,
            // Locations
            locations: row.locations.unwrap_or_else(|| vec!["us-east".to_string()]),
            // Assertions
            response_time_threshold_ms: row.response_time_threshold_ms,
            // Alert conditions
            min_failing_locations: row.min_failing_locations.unwrap_or(1),
            alert_after_minutes: row.alert_after_minutes.unwrap_or(0),
            failure_threshold: row.failure_threshold.unwrap_or(3),
            success_threshold: row.success_threshold.unwrap_or(1),
            // Status
            enabled: row.enabled,
            last_check_at: row.last_check_at,
            last_status: row.last_status,
            location_statuses: row.location_statuses,
            consecutive_failures: row.consecutive_failures.unwrap_or(0),
            consecutive_successes: row.consecutive_successes.unwrap_or(0),
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateCheckRequest {
    pub project_id: Uuid,
    pub check_type: String, // 'http', 'tcp', 'udp', 'ssl'
    pub name: String,
    pub target_url: Option<String>,
    pub target_host: Option<String>,
    pub target_port: Option<i32>,
    // HTTP settings
    pub http_method: Option<String>,
    pub http_headers: Option<serde_json::Value>,
    pub http_body: Option<String>,
    pub http_expected_status: Option<Vec<i32>>,
    pub http_expected_body: Option<String>,
    pub http_follow_redirects: Option<bool>,
    pub http_timeout_ms: Option<i32>,
    // TCP settings
    pub tcp_send_data: Option<String>,
    pub tcp_expect_data: Option<String>,
    // SSL settings
    pub ssl_check_expiry: Option<bool>,
    pub ssl_expiry_warning_days: Option<i32>,
    pub ssl_check_chain: Option<bool>,
    // Scheduling
    pub check_interval_seconds: Option<i32>,
    pub timeout_seconds: Option<i32>,
    // Locations (where to run checks from)
    pub locations: Option<Vec<String>>,
    // Assertions
    pub response_time_threshold_ms: Option<i32>,
    // Alert conditions
    pub min_failing_locations: Option<i32>,
    pub alert_after_minutes: Option<i32>,
    pub failure_threshold: Option<i32>,
    pub success_threshold: Option<i32>,
    // Metadata
    pub tags: Option<serde_json::Value>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateCheckRequest {
    pub name: Option<String>,
    pub target_url: Option<String>,
    pub target_host: Option<String>,
    pub target_port: Option<i32>,
    pub http_method: Option<String>,
    pub http_headers: Option<serde_json::Value>,
    pub http_body: Option<String>,
    pub http_expected_status: Option<Vec<i32>>,
    pub http_expected_body: Option<String>,
    pub http_follow_redirects: Option<bool>,
    pub http_timeout_ms: Option<i32>,
    pub tcp_send_data: Option<String>,
    pub tcp_expect_data: Option<String>,
    pub ssl_check_expiry: Option<bool>,
    pub ssl_expiry_warning_days: Option<i32>,
    pub ssl_check_chain: Option<bool>,
    pub check_interval_seconds: Option<i32>,
    pub timeout_seconds: Option<i32>,
    // Locations
    pub locations: Option<Vec<String>>,
    // Assertions
    pub response_time_threshold_ms: Option<i32>,
    // Alert conditions
    pub min_failing_locations: Option<i32>,
    pub alert_after_minutes: Option<i32>,
    pub failure_threshold: Option<i32>,
    pub success_threshold: Option<i32>,
    // Metadata
    pub tags: Option<serde_json::Value>,
    pub enabled: Option<bool>,
}

// ============================================================================
// CRUD Endpoints
// ============================================================================

#[derive(Debug, Deserialize)]
#[allow(dead_code)] // check_type and enabled for future filter implementation
pub struct ListQuery {
    project_id: Option<Uuid>,
    check_type: Option<String>,
    enabled: Option<bool>,
}

async fn list_checks(
    State(state): State<Arc<WatchState>>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Vec<HealthCheck>>> {
    let mut sql = "SELECT * FROM health_check_configs WHERE 1=1".to_string();

    if query.project_id.is_some() {
        sql.push_str(" AND project_id = $1");
    }

    let rows = if let Some(project_id) = query.project_id {
        sqlx::query_as::<_, HealthCheckRow>(&sql)
            .bind(project_id)
            .fetch_all(&*state.db)
            .await
    } else {
        sqlx::query_as::<_, HealthCheckRow>(
            "SELECT * FROM health_check_configs ORDER BY created_at DESC",
        )
        .fetch_all(&*state.db)
        .await
    }
    .map_err(|e| {
        error!("Failed to list health checks: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?;

    let checks: Vec<HealthCheck> = rows.into_iter().map(|r| r.into()).collect();
    Ok(Json(checks))
}

async fn create_check(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<CreateCheckRequest>,
) -> Result<Json<HealthCheck>> {
    // Validate check type
    match payload.check_type.as_str() {
        "http" | "https" => {
            if payload.target_url.is_none() {
                return Err(AppError::Validation(
                    "target_url is required for HTTP checks".to_string(),
                ));
            }
        }
        "tcp" | "udp" => {
            if payload.target_host.is_none() || payload.target_port.is_none() {
                return Err(AppError::Validation(
                    "target_host and target_port are required for TCP/UDP checks".to_string(),
                ));
            }
        }
        "ssl" | "tls" => {
            if payload.target_url.is_none()
                && (payload.target_host.is_none() || payload.target_port.is_none())
            {
                return Err(AppError::Validation(
                    "target_url OR target_host+target_port is required for SSL checks".to_string(),
                ));
            }
        }
        _ => {
            return Err(AppError::Validation(format!(
                "Invalid check_type: {}. Supported: http, tcp, udp, ssl",
                payload.check_type
            )));
        }
    }

    // Default locations if not provided
    let locations = payload
        .locations
        .clone()
        .unwrap_or_else(|| vec!["us-east".to_string()]);

    let row = sqlx::query_as::<_, HealthCheckRow>(
        r#"
        INSERT INTO health_check_configs (
            project_id, check_type, name, target_url, target_host, target_port,
            http_method, http_headers, http_body, http_expected_status, http_expected_body,
            http_follow_redirects, http_timeout_ms, tcp_send_data, tcp_expect_data,
            ssl_check_expiry, ssl_expiry_warning_days, ssl_check_chain,
            check_interval_seconds, timeout_seconds,
            locations, response_time_threshold_ms, min_failing_locations, alert_after_minutes,
            failure_threshold, success_threshold, tags, enabled
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28)
        RETURNING *
        "#
    )
    .bind(payload.project_id)
    .bind(&payload.check_type)
    .bind(&payload.name)
    .bind(&payload.target_url)
    .bind(&payload.target_host)
    .bind(payload.target_port)
    .bind(payload.http_method.as_deref().unwrap_or("GET"))
    .bind(&payload.http_headers)
    .bind(&payload.http_body)
    .bind(&payload.http_expected_status)
    .bind(&payload.http_expected_body)
    .bind(payload.http_follow_redirects.unwrap_or(true))
    .bind(payload.http_timeout_ms.unwrap_or(10000))
    .bind(&payload.tcp_send_data)
    .bind(&payload.tcp_expect_data)
    .bind(payload.ssl_check_expiry.unwrap_or(true))
    .bind(payload.ssl_expiry_warning_days.unwrap_or(30))
    .bind(payload.ssl_check_chain.unwrap_or(true))
    .bind(payload.check_interval_seconds.unwrap_or(300)) // Default: every 5 minutes
    .bind(payload.timeout_seconds.unwrap_or(30))
    .bind(&locations)
    .bind(payload.response_time_threshold_ms)
    .bind(payload.min_failing_locations.unwrap_or(1))
    .bind(payload.alert_after_minutes.unwrap_or(0))
    .bind(payload.failure_threshold.unwrap_or(3))
    .bind(payload.success_threshold.unwrap_or(1))
    .bind(&payload.tags)
    .bind(payload.enabled.unwrap_or(true))
    .fetch_one(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to create health check: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?;

    info!(
        "Created health check: {} ({})",
        payload.name, payload.check_type
    );

    let organization_id =
        sqlx::query_scalar::<_, Uuid>("SELECT organization_id FROM projects WHERE id = $1")
            .bind(payload.project_id)
            .fetch_optional(&*state.db)
            .await
            .ok()
            .flatten();

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::HealthCheckCreated)
        .resource("health_check", row.id)
        .details(serde_json::json!({ "created": { "name": &payload.name, "check_type": &payload.check_type } }))
        .origin(&audit_origin.origin_type, &audit_origin.origin_ref, &audit_origin.origin_reason)
        .caller(&audit_caller.caller_type, &audit_caller.key_label, &audit_caller.key_prefix)
        .success();
    if let Some(org_id) = organization_id {
        audit = audit.organization(org_id);
    }
    audit.log(&state.clickhouse).await;

    Ok(Json(row.into()))
}

async fn get_check(
    State(state): State<Arc<WatchState>>,
    Path(id): Path<Uuid>,
) -> Result<Json<HealthCheck>> {
    let row =
        sqlx::query_as::<_, HealthCheckRow>("SELECT * FROM health_check_configs WHERE id = $1")
            .bind(id)
            .fetch_optional(&*state.db)
            .await
            .map_err(|e| {
                error!("Failed to get health check: {}", e);
                AppError::Internal(anyhow::anyhow!("Database error: {}", e))
            })?
            .ok_or_else(|| AppError::NotFound("Health check not found".to_string()))?;

    Ok(Json(row.into()))
}

async fn update_check(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(payload): Json<UpdateCheckRequest>,
) -> Result<Json<HealthCheck>> {
    let before_row =
        sqlx::query_as::<_, HealthCheckRow>("SELECT * FROM health_check_configs WHERE id = $1")
            .bind(id)
            .fetch_optional(&*state.db)
            .await
            .ok()
            .flatten();

    let row = sqlx::query_as::<_, HealthCheckRow>(
        r#"
        UPDATE health_check_configs
        SET
            name = COALESCE($1, name),
            target_url = COALESCE($2, target_url),
            target_host = COALESCE($3, target_host),
            target_port = COALESCE($4, target_port),
            http_method = COALESCE($5, http_method),
            http_headers = COALESCE($6, http_headers),
            http_body = COALESCE($7, http_body),
            http_expected_status = COALESCE($8, http_expected_status),
            http_expected_body = COALESCE($9, http_expected_body),
            http_follow_redirects = COALESCE($10, http_follow_redirects),
            http_timeout_ms = COALESCE($11, http_timeout_ms),
            tcp_send_data = COALESCE($12, tcp_send_data),
            tcp_expect_data = COALESCE($13, tcp_expect_data),
            ssl_check_expiry = COALESCE($14, ssl_check_expiry),
            ssl_expiry_warning_days = COALESCE($15, ssl_expiry_warning_days),
            ssl_check_chain = COALESCE($16, ssl_check_chain),
            check_interval_seconds = COALESCE($17, check_interval_seconds),
            timeout_seconds = COALESCE($18, timeout_seconds),
            locations = COALESCE($19, locations),
            response_time_threshold_ms = COALESCE($20, response_time_threshold_ms),
            min_failing_locations = COALESCE($21, min_failing_locations),
            alert_after_minutes = COALESCE($22, alert_after_minutes),
            failure_threshold = COALESCE($23, failure_threshold),
            success_threshold = COALESCE($24, success_threshold),
            tags = COALESCE($25, tags),
            enabled = COALESCE($26, enabled),
            updated_at = NOW()
        WHERE id = $27
        RETURNING *
        "#,
    )
    .bind(payload.name.as_deref())
    .bind(payload.target_url.as_deref())
    .bind(payload.target_host.as_deref())
    .bind(payload.target_port)
    .bind(payload.http_method.as_deref())
    .bind(&payload.http_headers)
    .bind(payload.http_body.as_deref())
    .bind(&payload.http_expected_status)
    .bind(payload.http_expected_body.as_deref())
    .bind(payload.http_follow_redirects)
    .bind(payload.http_timeout_ms)
    .bind(payload.tcp_send_data.as_deref())
    .bind(payload.tcp_expect_data.as_deref())
    .bind(payload.ssl_check_expiry)
    .bind(payload.ssl_expiry_warning_days)
    .bind(payload.ssl_check_chain)
    .bind(payload.check_interval_seconds)
    .bind(payload.timeout_seconds)
    .bind(&payload.locations)
    .bind(payload.response_time_threshold_ms)
    .bind(payload.min_failing_locations)
    .bind(payload.alert_after_minutes)
    .bind(payload.failure_threshold)
    .bind(payload.success_threshold)
    .bind(&payload.tags)
    .bind(payload.enabled)
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to update health check: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?
    .ok_or_else(|| AppError::NotFound("Health check not found".to_string()))?;

    info!("Updated health check: {}", id);

    let organization_id =
        sqlx::query_scalar::<_, Uuid>("SELECT organization_id FROM projects WHERE id = $1")
            .bind(row.project_id)
            .fetch_optional(&*state.db)
            .await
            .ok()
            .flatten();

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::HealthCheckUpdated)
        .resource("health_check", id)
        .details(serde_json::json!({
            "before": { "name": before_row.as_ref().map(|r| &r.name), "enabled": before_row.as_ref().map(|r| r.enabled), "check_type": before_row.as_ref().map(|r| &r.check_type) },
            "after": { "name": &row.name, "enabled": row.enabled, "check_type": &row.check_type }
        }))
        .origin(&audit_origin.origin_type, &audit_origin.origin_ref, &audit_origin.origin_reason)
        .caller(&audit_caller.caller_type, &audit_caller.key_label, &audit_caller.key_prefix)
        .success();
    if let Some(org_id) = organization_id {
        audit = audit.organization(org_id);
    }
    audit.log(&state.clickhouse).await;

    Ok(Json(row.into()))
}

async fn delete_check(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<StatusCode> {
    let deleted_row =
        sqlx::query_as::<_, HealthCheckRow>("SELECT * FROM health_check_configs WHERE id = $1")
            .bind(id)
            .fetch_optional(&*state.db)
            .await
            .ok()
            .flatten();

    let result = sqlx::query("DELETE FROM health_check_configs WHERE id = $1")
        .bind(id)
        .execute(&*state.db)
        .await
        .map_err(|e| {
            error!("Failed to delete health check: {}", e);
            AppError::Internal(anyhow::anyhow!("Database error: {}", e))
        })?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound("Health check not found".to_string()));
    }

    info!("Deleted health check: {}", id);

    let organization_id = deleted_row.as_ref().and_then(|r| Some(r.project_id));
    let organization_id = match organization_id {
        Some(pid) => {
            sqlx::query_scalar::<_, Uuid>("SELECT organization_id FROM projects WHERE id = $1")
                .bind(pid)
                .fetch_optional(&*state.db)
                .await
                .ok()
                .flatten()
        }
        None => None,
    };

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::HealthCheckDeleted)
        .resource("health_check", id)
        .details(serde_json::json!({ "deleted": { "name": deleted_row.as_ref().map(|r| &r.name), "check_type": deleted_row.as_ref().map(|r| &r.check_type) } }))
        .origin(&audit_origin.origin_type, &audit_origin.origin_ref, &audit_origin.origin_reason)
        .caller(&audit_caller.caller_type, &audit_caller.key_label, &audit_caller.key_prefix)
        .success();
    if let Some(org_id) = organization_id {
        audit = audit.organization(org_id);
    }
    audit.log(&state.clickhouse).await;

    Ok(StatusCode::NO_CONTENT)
}

// ============================================================================
// Results Endpoints
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct ReportResultRequest {
    pub check_id: String,
    pub check_type: String,
    pub check_name: String,
    pub target: String,
    pub status: String,
    pub success: bool,
    pub response_time_ms: f64,
    pub dns_time_ms: Option<f64>,
    pub connect_time_ms: Option<f64>,
    pub tls_time_ms: Option<f64>,
    pub first_byte_time_ms: Option<f64>,
    pub http_status_code: Option<i32>,
    pub http_response_size: Option<i64>,
    pub ssl_valid: bool,
    pub ssl_days_until_expiry: Option<i32>,
    pub ssl_issuer: Option<String>,
    pub ssl_subject: Option<String>,
    pub ssl_expires_at: Option<i64>,
    pub error_type: Option<String>,
    pub error_message: Option<String>,
    pub response_body: Option<String>,
    pub timestamp: i64,
    pub agent_id: String,
    pub agent_location: String,
}

async fn report_result(
    State(state): State<Arc<WatchState>>,
    Json(payload): Json<ReportResultRequest>,
) -> Result<StatusCode> {
    let check_id: Uuid = payload
        .check_id
        .parse()
        .map_err(|_| AppError::Validation("Invalid check_id".to_string()))?;

    // Get project_id from the check config
    let check =
        sqlx::query_scalar::<_, Uuid>("SELECT project_id FROM health_check_configs WHERE id = $1")
            .bind(check_id)
            .fetch_optional(&*state.db)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?
            .ok_or_else(|| AppError::NotFound("Health check not found".to_string()))?;

    // Insert result into ClickHouse
    #[derive(clickhouse::Row, serde::Serialize)]
    struct ResultInsert {
        id: String,
        check_id: String,
        project_id: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: chrono::DateTime<chrono::Utc>,
        check_type: String,
        check_name: String,
        target: String,
        status: String,
        success: u8,
        response_time_ms: f64,
        dns_time_ms: f64,
        connect_time_ms: f64,
        tls_time_ms: f64,
        first_byte_time_ms: f64,
        http_status_code: i32,
        http_response_size: i64,
        ssl_valid: u8,
        ssl_days_until_expiry: i32,
        ssl_issuer: String,
        ssl_subject: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos::option")]
        ssl_expires_at: Option<chrono::DateTime<chrono::Utc>>,
        error_type: String,
        error_message: String,
        agent_id: String,
        agent_location: String,
        response_body: String,
    }

    let timestamp = chrono::DateTime::from_timestamp_millis(payload.timestamp)
        .unwrap_or_else(|| chrono::Utc::now());

    let ssl_expires_at = payload
        .ssl_expires_at
        .and_then(|ts| chrono::DateTime::from_timestamp_millis(ts));

    let mut inserter = state
        .clickhouse
        .as_ref()
        .inserter::<ResultInsert>("health_check_results")
        .with_period(Some(std::time::Duration::from_millis(100)))
        .with_max_rows(1000);

    inserter
        .write(&ResultInsert {
            id: Uuid::new_v4().to_string(),
            check_id: check_id.to_string(),
            project_id: check.to_string(),
            timestamp,
            check_type: payload.check_type,
            check_name: payload.check_name,
            target: payload.target,
            status: payload.status.clone(),
            success: if payload.success { 1 } else { 0 },
            response_time_ms: payload.response_time_ms,
            dns_time_ms: payload.dns_time_ms.unwrap_or(0.0),
            connect_time_ms: payload.connect_time_ms.unwrap_or(0.0),
            tls_time_ms: payload.tls_time_ms.unwrap_or(0.0),
            first_byte_time_ms: payload.first_byte_time_ms.unwrap_or(0.0),
            http_status_code: payload.http_status_code.unwrap_or(0),
            http_response_size: payload.http_response_size.unwrap_or(0),
            ssl_valid: if payload.ssl_valid { 1 } else { 0 },
            ssl_days_until_expiry: payload.ssl_days_until_expiry.unwrap_or(0),
            ssl_issuer: payload.ssl_issuer.unwrap_or_default(),
            ssl_subject: payload.ssl_subject.unwrap_or_default(),
            ssl_expires_at,
            error_type: payload.error_type.unwrap_or_default(),
            error_message: payload.error_message.unwrap_or_default(),
            agent_id: payload.agent_id,
            agent_location: payload.agent_location,
            response_body: payload.response_body.unwrap_or_default(),
        })
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to write result: {}", e)))?;

    inserter
        .commit()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to commit: {}", e)))?;

    // Update check status in PostgreSQL
    let new_status = if payload.success {
        "healthy"
    } else {
        "unhealthy"
    };
    sqlx::query(
        r#"
        UPDATE health_check_configs
        SET last_check_at = NOW(),
            last_status = $1,
            consecutive_failures = CASE WHEN $2 THEN 0 ELSE consecutive_failures + 1 END,
            consecutive_successes = CASE WHEN $2 THEN consecutive_successes + 1 ELSE 0 END
        WHERE id = $3
        "#,
    )
    .bind(new_status)
    .bind(payload.success)
    .bind(check_id)
    .execute(&*state.db)
    .await
    .ok();

    Ok(StatusCode::ACCEPTED)
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)] // from/to fields for future time range filtering
pub struct ResultsQuery {
    from: Option<String>,
    to: Option<String>,
    limit: Option<i32>,
}

#[derive(Debug, Serialize)]
pub struct CheckResult {
    pub timestamp: String,
    pub status: String,
    pub success: bool,
    pub response_time_ms: f64,
    pub http_status_code: Option<i32>,
    pub ssl_days_until_expiry: Option<i32>,
    pub error_message: Option<String>,
    pub agent_location: String,
}

async fn get_check_results(
    State(state): State<Arc<WatchState>>,
    Path(id): Path<Uuid>,
    Query(query): Query<ResultsQuery>,
) -> Result<Json<Vec<CheckResult>>> {
    let limit = query.limit.unwrap_or(100).min(1000);

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct ResultRow {
        timestamp: String,
        status: String,
        success: u8,
        response_time_ms: f64,
        http_status_code: i32,
        ssl_days_until_expiry: i32,
        error_message: String,
        agent_location: String,
    }

    let rows = state
        .clickhouse
        .query(&format!(
            r#"
        SELECT 
            toString(timestamp) as timestamp,
            status,
            success,
            response_time_ms,
            http_status_code,
            ssl_days_until_expiry,
            error_message,
            agent_location
        FROM health_check_results
        WHERE check_id = '{}'
        ORDER BY timestamp DESC
        LIMIT {}
        "#,
            id, limit
        ))
        .fetch_all::<ResultRow>()
        .await
        .map_err(|e| {
            error!("Failed to query check results: {}", e);
            AppError::Internal(anyhow::anyhow!("Query error: {}", e))
        })?;

    let results: Vec<CheckResult> = rows
        .into_iter()
        .map(|r| CheckResult {
            timestamp: r.timestamp,
            status: r.status,
            success: r.success == 1,
            response_time_ms: r.response_time_ms,
            http_status_code: if r.http_status_code > 0 {
                Some(r.http_status_code)
            } else {
                None
            },
            ssl_days_until_expiry: if r.ssl_days_until_expiry > 0 {
                Some(r.ssl_days_until_expiry)
            } else {
                None
            },
            error_message: if r.error_message.is_empty() {
                None
            } else {
                Some(r.error_message)
            },
            agent_location: r.agent_location,
        })
        .collect();

    Ok(Json(results))
}

#[derive(Debug, Deserialize)]
pub struct UptimeQuery {
    project_id: Uuid,
    hours: Option<i32>,
}

#[derive(Debug, Serialize)]
pub struct UptimeSummary {
    pub check_id: String,
    pub check_name: String,
    pub uptime_percent: f64,
    pub total_checks: i64,
    pub successful_checks: i64,
    pub avg_response_time_ms: f64,
    pub last_status: String,
}

async fn get_uptime_summary(
    State(state): State<Arc<WatchState>>,
    Query(query): Query<UptimeQuery>,
) -> Result<Json<Vec<UptimeSummary>>> {
    let hours = query.hours.unwrap_or(24);

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct UptimeRow {
        check_id: String,
        total_checks: u64,
        successful_checks: u64,
        avg_response_time_ms: f64,
    }

    let rows = state
        .clickhouse
        .query(&format!(
            r#"
        SELECT 
            check_id,
            sum(total_checks) as total_checks,
            sum(successful_checks) as successful_checks,
            avg(avg_response_time_ms) as avg_response_time_ms
        FROM health_check_uptime_hourly_mv
        WHERE project_id = '{}'
          AND hour >= now() - INTERVAL {} HOUR
        GROUP BY check_id
        "#,
            query.project_id, hours
        ))
        .fetch_all::<UptimeRow>()
        .await
        .unwrap_or_default();

    // Get check names and last status from PostgreSQL
    let checks: Vec<(Uuid, String, Option<String>)> = sqlx::query_as(
        "SELECT id, name, last_status FROM health_check_configs WHERE project_id = $1",
    )
    .bind(query.project_id)
    .fetch_all(&*state.db)
    .await
    .unwrap_or_default();

    let mut summaries: Vec<UptimeSummary> = Vec::new();

    for check in checks {
        let uptime_data = rows.iter().find(|r| r.check_id == check.0.to_string());

        let (total, successful, avg_rt) = match uptime_data {
            Some(d) => (
                d.total_checks as i64,
                d.successful_checks as i64,
                d.avg_response_time_ms,
            ),
            None => (0, 0, 0.0),
        };

        let uptime_percent = if total > 0 {
            (successful as f64 / total as f64) * 100.0
        } else {
            0.0
        };

        summaries.push(UptimeSummary {
            check_id: check.0.to_string(),
            check_name: check.1,
            uptime_percent,
            total_checks: total,
            successful_checks: successful,
            avg_response_time_ms: avg_rt,
            last_status: check.2.unwrap_or_else(|| "unknown".to_string()),
        });
    }

    Ok(Json(summaries))
}
