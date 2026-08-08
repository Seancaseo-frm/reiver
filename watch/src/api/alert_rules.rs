//! Alert Rules API endpoints
//!
//! Manages alert rule configurations - simplified HyperDX-style model.

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::Json,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::{error, info};
use uuid::Uuid;

use reiver_core::audit::{AuditCaller, AuditEventBuilder, AuditEventType, AuditOrigin};

use crate::alerts::AlertQueryConfig;
use crate::app_state::WatchState;
use crate::error::{AppError, Result};

pub fn create_alert_rules_router() -> Router<Arc<WatchState>> {
    Router::new()
        .route("/rules", get(list_rules).post(create_rule))
        .route(
            "/rules/{id}",
            get(get_rule).put(update_rule).delete(delete_rule),
        )
        .route("/rules/{id}/alerts", get(get_rule_alerts))
        .route("/alerts", get(list_alerts))
        .route("/test-notification", post(test_notification))
}

// ============================================================================
// Types
// ============================================================================

#[derive(Debug, Serialize)]
pub struct AlertRuleResponse {
    pub id: Uuid,
    pub project_id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub rule_type: String,
    pub query_config: serde_json::Value,
    /// Single threshold value
    pub threshold: f64,
    /// Threshold comparison: 'above' or 'below'
    pub threshold_type: String,
    /// Notification channel UUIDs
    pub notification_channels: Vec<Uuid>,
    pub alert_on_absent: bool,
    pub absent_for_seconds: i32,
    pub eval_window_seconds: i32,
    pub eval_interval_seconds: i32,
    pub labels: serde_json::Value,
    pub annotations: serde_json::Value,
    pub enabled: bool,
    pub last_evaluated_at: Option<chrono::DateTime<chrono::Utc>>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Deserialize)]
pub struct CreateAlertRuleRequest {
    pub project_id: Uuid,
    pub name: String,
    pub description: Option<String>,
    #[serde(default = "default_rule_type")]
    pub rule_type: String,
    pub query_config: AlertQueryConfig,
    /// Single threshold value
    #[serde(default)]
    pub threshold: f64,
    /// Threshold comparison: 'above' or 'below'
    #[serde(default = "default_threshold_type")]
    pub threshold_type: String,
    /// Notification channel UUIDs
    #[serde(default)]
    pub notification_channels: Vec<Uuid>,
    #[serde(default)]
    pub alert_on_absent: bool,
    #[serde(default = "default_absent_for")]
    pub absent_for_seconds: i32,
    #[serde(default = "default_eval_window")]
    pub eval_window_seconds: i32,
    #[serde(default = "default_eval_interval")]
    pub eval_interval_seconds: i32,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub annotations: BTreeMap<String, String>,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_rule_type() -> String {
    "threshold".to_string()
}
fn default_threshold_type() -> String {
    "above".to_string()
}
fn default_absent_for() -> i32 {
    300
}
fn default_eval_window() -> i32 {
    300
}
fn default_eval_interval() -> i32 {
    60
}
fn default_enabled() -> bool {
    true
}

#[derive(Debug, Deserialize)]
pub struct UpdateAlertRuleRequest {
    pub name: Option<String>,
    pub description: Option<String>,
    pub query_config: Option<AlertQueryConfig>,
    pub threshold: Option<f64>,
    pub threshold_type: Option<String>,
    pub notification_channels: Option<Vec<Uuid>>,
    pub alert_on_absent: Option<bool>,
    pub absent_for_seconds: Option<i32>,
    pub eval_window_seconds: Option<i32>,
    pub eval_interval_seconds: Option<i32>,
    pub labels: Option<BTreeMap<String, String>>,
    pub annotations: Option<BTreeMap<String, String>>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct ListRulesQuery {
    pub project_id: Option<Uuid>,
    pub enabled: Option<bool>,
    pub limit: Option<i32>,
}

// ============================================================================
// Validation
// ============================================================================

fn validate_query_config(q: &AlertQueryConfig) -> Result<()> {
    match q {
        AlertQueryConfig::Metrics { metric_name, .. } => {
            if metric_name.trim().is_empty() {
                return Err(AppError::Validation(
                    "metrics query requires a non-empty 'metric_name'".into(),
                ));
            }
        }
        AlertQueryConfig::LogPattern { patterns, .. } => {
            if patterns.is_empty() {
                return Err(AppError::Validation(
                    "log_pattern query requires at least one pattern".into(),
                ));
            }
        }
        AlertQueryConfig::PromQL { promql } => {
            if promql.trim().is_empty() {
                return Err(AppError::Validation(
                    "promql query requires a non-empty 'promql' expression".into(),
                ));
            }
        }
        AlertQueryConfig::Llm { metric_name, .. } => {
            if metric_name.trim().is_empty() {
                return Err(AppError::Validation(
                    "llm query requires a non-empty 'metric_name'".into(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_threshold_type(t: &str) -> Result<()> {
    if t != "above" && t != "below" {
        return Err(AppError::Validation(
            "threshold_type must be 'above' or 'below'".into(),
        ));
    }
    Ok(())
}

// ============================================================================
// Handlers
// ============================================================================

async fn list_rules(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Query(params): Query<ListRulesQuery>,
) -> Result<Json<Vec<AlertRuleResponse>>> {
    let _user_id = crate::api::extract_user_id(&headers)?;

    let limit = params.limit.unwrap_or(100).min(1000);

    let mut query = String::from(
        r#"SELECT id, project_id, name, description, rule_type, query_config,
           threshold, threshold_type, notification_channels,
           alert_on_absent, absent_for_seconds,
           eval_window_seconds, eval_interval_seconds,
           labels, annotations, enabled, last_evaluated_at,
           created_at, updated_at
           FROM alert_rules WHERE 1=1"#,
    );

    if let Some(project_id) = params.project_id {
        query.push_str(&format!(" AND project_id = '{}'", project_id));
    }

    if let Some(enabled) = params.enabled {
        query.push_str(&format!(" AND enabled = {}", enabled));
    }

    query.push_str(&format!(" ORDER BY created_at DESC LIMIT {}", limit));

    let rows = sqlx::query_as::<_, AlertRuleRow>(&query)
        .fetch_all(&*state.db)
        .await
        .map_err(|e| {
            error!("Failed to list alert rules: {}", e);
            AppError::Internal(anyhow::anyhow!("Database error: {}", e))
        })?;

    Ok(Json(rows.into_iter().map(|r| r.into()).collect()))
}

async fn create_rule(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<CreateAlertRuleRequest>,
) -> Result<Json<AlertRuleResponse>> {
    let _user_id = crate::api::extract_user_id(&headers)?;

    validate_query_config(&payload.query_config)?;
    validate_threshold_type(&payload.threshold_type)?;

    let query_config_json = serde_json::to_value(&payload.query_config)
        .map_err(|e| AppError::Validation(format!("Invalid query_config: {}", e)))?;

    let labels_json =
        serde_json::to_value(&payload.labels).unwrap_or_else(|_| serde_json::json!({}));

    let annotations_json =
        serde_json::to_value(&payload.annotations).unwrap_or_else(|_| serde_json::json!({}));

    let row = sqlx::query_as::<_, AlertRuleRow>(
        r#"INSERT INTO alert_rules (
            project_id, name, description, rule_type, query_config,
            threshold, threshold_type, notification_channels,
            alert_on_absent, absent_for_seconds,
            eval_window_seconds, eval_interval_seconds,
            labels, annotations, enabled
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        RETURNING *"#,
    )
    .bind(payload.project_id)
    .bind(&payload.name)
    .bind(&payload.description)
    .bind(&payload.rule_type)
    .bind(&query_config_json)
    .bind(payload.threshold)
    .bind(&payload.threshold_type)
    .bind(&payload.notification_channels)
    .bind(payload.alert_on_absent)
    .bind(payload.absent_for_seconds)
    .bind(payload.eval_window_seconds)
    .bind(payload.eval_interval_seconds)
    .bind(&labels_json)
    .bind(&annotations_json)
    .bind(payload.enabled)
    .fetch_one(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to create alert rule: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?;

    info!(
        "Created alert rule: {} for project {}",
        row.id, payload.project_id
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
    let mut audit = AuditEventBuilder::new(AuditEventType::AlertRuleCreated)
        .actor(_user_id)
        .resource("alert_rule", row.id)
        .details(serde_json::json!({ "created": { "name": &payload.name, "project_id": payload.project_id } }))
        .origin(&audit_origin.origin_type, &audit_origin.origin_ref, &audit_origin.origin_reason)
        .caller(&audit_caller.caller_type, &audit_caller.key_label, &audit_caller.key_prefix)
        .success();
    if let Some(org_id) = organization_id {
        audit = audit.organization(org_id);
    }
    audit.log(&state.clickhouse).await;

    Ok(Json(row.into()))
}

async fn get_rule(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<AlertRuleResponse>> {
    let _user_id = crate::api::extract_user_id(&headers)?;

    let row = sqlx::query_as::<_, AlertRuleRow>(
        r#"SELECT id, project_id, name, description, rule_type, query_config,
           threshold, threshold_type, notification_channels,
           alert_on_absent, absent_for_seconds,
           eval_window_seconds, eval_interval_seconds,
           labels, annotations, enabled, last_evaluated_at,
           created_at, updated_at
           FROM alert_rules WHERE id = $1"#,
    )
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to get alert rule: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?
    .ok_or_else(|| AppError::NotFound("Alert rule not found".to_string()))?;

    Ok(Json(row.into()))
}

async fn update_rule(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(payload): Json<UpdateAlertRuleRequest>,
) -> Result<Json<AlertRuleResponse>> {
    let _user_id = crate::api::extract_user_id(&headers)?;

    if let Some(ref qc) = payload.query_config {
        validate_query_config(qc)?;
    }
    if let Some(ref tt) = payload.threshold_type {
        validate_threshold_type(tt)?;
    }

    let before_row = sqlx::query_as::<_, AlertRuleRow>(
        r#"SELECT id, project_id, name, description, rule_type, query_config,
           threshold, threshold_type, notification_channels,
           alert_on_absent, absent_for_seconds,
           eval_window_seconds, eval_interval_seconds,
           labels, annotations, enabled, last_evaluated_at,
           created_at, updated_at
           FROM alert_rules WHERE id = $1"#,
    )
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .ok()
    .flatten();

    let query_config_json = payload
        .query_config
        .map(|qc| serde_json::to_value(&qc).ok())
        .flatten();

    let labels_json = payload
        .labels
        .map(|l| serde_json::to_value(&l).ok())
        .flatten();

    let annotations_json = payload
        .annotations
        .map(|a| serde_json::to_value(&a).ok())
        .flatten();

    let row = sqlx::query_as::<_, AlertRuleRow>(
        r#"UPDATE alert_rules SET
            name = COALESCE($1, name),
            description = COALESCE($2, description),
            query_config = COALESCE($3, query_config),
            threshold = COALESCE($4, threshold),
            threshold_type = COALESCE($5, threshold_type),
            notification_channels = COALESCE($6, notification_channels),
            alert_on_absent = COALESCE($7, alert_on_absent),
            absent_for_seconds = COALESCE($8, absent_for_seconds),
            eval_window_seconds = COALESCE($9, eval_window_seconds),
            eval_interval_seconds = COALESCE($10, eval_interval_seconds),
            labels = COALESCE($11, labels),
            annotations = COALESCE($12, annotations),
            enabled = COALESCE($13, enabled),
            updated_at = NOW()
        WHERE id = $14
        RETURNING id, project_id, name, description, rule_type, query_config,
           threshold, threshold_type, notification_channels,
           alert_on_absent, absent_for_seconds,
           eval_window_seconds, eval_interval_seconds,
           labels, annotations, enabled, last_evaluated_at,
           created_at, updated_at"#,
    )
    .bind(&payload.name)
    .bind(&payload.description)
    .bind(&query_config_json)
    .bind(payload.threshold)
    .bind(&payload.threshold_type)
    .bind(&payload.notification_channels)
    .bind(payload.alert_on_absent)
    .bind(payload.absent_for_seconds)
    .bind(payload.eval_window_seconds)
    .bind(payload.eval_interval_seconds)
    .bind(&labels_json)
    .bind(&annotations_json)
    .bind(payload.enabled)
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to update alert rule: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?
    .ok_or_else(|| AppError::NotFound("Alert rule not found".to_string()))?;

    info!("Updated alert rule: {}", id);

    let organization_id =
        sqlx::query_scalar::<_, Uuid>("SELECT organization_id FROM projects WHERE id = $1")
            .bind(row.project_id)
            .fetch_optional(&*state.db)
            .await
            .ok()
            .flatten();

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::AlertRuleUpdated)
        .actor(_user_id)
        .resource("alert_rule", id)
        .details(serde_json::json!({
            "before": { "name": before_row.as_ref().map(|r| &r.name), "enabled": before_row.as_ref().map(|r| r.enabled), "threshold": before_row.as_ref().map(|r| r.threshold) },
            "after": { "name": &row.name, "enabled": row.enabled, "threshold": row.threshold }
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

async fn delete_rule(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<StatusCode> {
    let _user_id = crate::api::extract_user_id(&headers)?;

    let deleted_row = sqlx::query_as::<_, AlertRuleRow>(
        r#"SELECT id, project_id, name, description, rule_type, query_config,
           threshold, threshold_type, notification_channels,
           alert_on_absent, absent_for_seconds,
           eval_window_seconds, eval_interval_seconds,
           labels, annotations, enabled, last_evaluated_at,
           created_at, updated_at
           FROM alert_rules WHERE id = $1"#,
    )
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .ok()
    .flatten();

    let result = sqlx::query("DELETE FROM alert_rules WHERE id = $1")
        .bind(id)
        .execute(&*state.db)
        .await
        .map_err(|e| {
            error!("Failed to delete alert rule: {}", e);
            AppError::Internal(anyhow::anyhow!("Database error: {}", e))
        })?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound("Alert rule not found".to_string()));
    }

    info!("Deleted alert rule: {}", id);

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
    let mut audit = AuditEventBuilder::new(AuditEventType::AlertRuleDeleted)
        .actor(_user_id)
        .resource("alert_rule", id)
        .details(serde_json::json!({ "deleted": { "name": deleted_row.as_ref().map(|r| &r.name), "rule_type": deleted_row.as_ref().map(|r| &r.rule_type) } }))
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
// Alert Endpoints - Simplified OK/ALERT states
// ============================================================================

#[derive(Debug, Serialize)]
pub struct AlertResponse {
    pub id: Uuid,
    pub rule_id: Uuid,
    pub fingerprint: String,
    pub labels: serde_json::Value,
    pub annotations: serde_json::Value,
    /// State: OK or ALERT
    pub state: String,
    pub value: Option<f64>,
    pub checked_at: chrono::DateTime<chrono::Utc>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Deserialize)]
pub struct ListAlertsQuery {
    pub project_id: Option<Uuid>,
    pub rule_id: Option<Uuid>,
    pub state: Option<String>,
    pub limit: Option<i32>,
}

async fn get_rule_alerts(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(rule_id): Path<Uuid>,
) -> Result<Json<Vec<AlertResponse>>> {
    let _user_id = crate::api::extract_user_id(&headers)?;

    let rows = sqlx::query_as::<_, AlertRow>(
        r#"SELECT id, rule_id, fingerprint, labels, annotations, state, value, checked_at, created_at
           FROM alerts WHERE rule_id = $1 ORDER BY checked_at DESC LIMIT 100"#
    )
    .bind(rule_id)
    .fetch_all(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to get alerts: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?;

    Ok(Json(rows.into_iter().map(|r| r.into()).collect()))
}

async fn list_alerts(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Query(params): Query<ListAlertsQuery>,
) -> Result<Json<Vec<AlertResponse>>> {
    let _user_id = crate::api::extract_user_id(&headers)?;

    let limit = params.limit.unwrap_or(100).min(1000);

    let mut query = String::from(
        r#"SELECT a.id, a.rule_id, a.fingerprint, a.labels, a.annotations, a.state, a.value, a.checked_at, a.created_at
           FROM alerts a
           JOIN alert_rules r ON a.rule_id = r.id
           WHERE 1=1"#,
    );

    if let Some(project_id) = params.project_id {
        query.push_str(&format!(" AND r.project_id = '{}'", project_id));
    }

    if let Some(rule_id) = params.rule_id {
        query.push_str(&format!(" AND a.rule_id = '{}'", rule_id));
    }

    if let Some(state_filter) = &params.state {
        query.push_str(&format!(" AND a.state = '{}'", state_filter));
    }

    query.push_str(&format!(" ORDER BY a.checked_at DESC LIMIT {}", limit));

    let rows = sqlx::query_as::<_, AlertRow>(&query)
        .fetch_all(&*state.db)
        .await
        .map_err(|e| {
            error!("Failed to list alerts: {}", e);
            AppError::Internal(anyhow::anyhow!("Database error: {}", e))
        })?;

    Ok(Json(rows.into_iter().map(|r| r.into()).collect()))
}

// ============================================================================
// Database Row Types - Simplified
// ============================================================================

#[derive(Debug, sqlx::FromRow)]
struct AlertRuleRow {
    id: Uuid,
    project_id: Uuid,
    name: String,
    description: Option<String>,
    rule_type: String,
    query_config: serde_json::Value,
    threshold: f64,
    threshold_type: String,
    notification_channels: Vec<Uuid>,
    alert_on_absent: bool,
    absent_for_seconds: i32,
    eval_window_seconds: i32,
    eval_interval_seconds: i32,
    labels: serde_json::Value,
    annotations: serde_json::Value,
    enabled: bool,
    last_evaluated_at: Option<chrono::DateTime<chrono::Utc>>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<AlertRuleRow> for AlertRuleResponse {
    fn from(row: AlertRuleRow) -> Self {
        Self {
            id: row.id,
            project_id: row.project_id,
            name: row.name,
            description: row.description,
            rule_type: row.rule_type,
            query_config: row.query_config,
            threshold: row.threshold,
            threshold_type: row.threshold_type,
            notification_channels: row.notification_channels,
            alert_on_absent: row.alert_on_absent,
            absent_for_seconds: row.absent_for_seconds,
            eval_window_seconds: row.eval_window_seconds,
            eval_interval_seconds: row.eval_interval_seconds,
            labels: row.labels,
            annotations: row.annotations,
            enabled: row.enabled,
            last_evaluated_at: row.last_evaluated_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
struct AlertRow {
    id: Uuid,
    rule_id: Uuid,
    fingerprint: String,
    labels: serde_json::Value,
    annotations: serde_json::Value,
    state: String,
    value: Option<f64>,
    checked_at: chrono::DateTime<chrono::Utc>,
    created_at: chrono::DateTime<chrono::Utc>,
}

impl From<AlertRow> for AlertResponse {
    fn from(row: AlertRow) -> Self {
        Self {
            id: row.id,
            rule_id: row.rule_id,
            fingerprint: row.fingerprint,
            labels: row.labels,
            annotations: row.annotations,
            state: row.state,
            value: row.value,
            checked_at: row.checked_at,
            created_at: row.created_at,
        }
    }
}

// ============================================================================
// Auto-create Alert Rule for Health Check
// ============================================================================

/// Create an automatic alert rule when a health check is created
#[allow(dead_code)] // Available for automatic health check alert creation
pub async fn create_health_check_alert_rule(
    db: &sqlx::PgPool,
    project_id: Uuid,
    check_id: Uuid,
    check_name: &str,
    notification_channels: &[Uuid],
) -> anyhow::Result<Uuid> {
    let query_config = serde_json::json!({
        "metric_name": "health_check.success",
        "filters": {
            "check_id": check_id.to_string()
        },
        "time_aggregation": "avg",
        "space_aggregation": "avg"
    });

    let labels = serde_json::json!({
        "check_id": check_id.to_string(),
        "check_name": check_name,
        "source": "health_check"
    });

    let annotations = serde_json::json!({
        "summary": format!("Health check '{}' is failing", check_name),
        "description": "The health check is reporting failures. Check the target service availability."
    });

    let row: (Uuid,) = sqlx::query_as(
        r#"INSERT INTO alert_rules (
            project_id, name, description, rule_type, query_config,
            threshold, threshold_type, notification_channels,
            alert_on_absent, absent_for_seconds,
            eval_window_seconds, eval_interval_seconds,
            labels, annotations
        ) VALUES ($1, $2, $3, 'threshold', $4, $5, $6, $7, true, 300, 300, 60, $8, $9)
        RETURNING id"#,
    )
    .bind(project_id)
    .bind(format!("Health Check: {}", check_name))
    .bind(format!(
        "Auto-created alert for health check '{}'",
        check_name
    ))
    .bind(&query_config)
    .bind(1.0_f64) // threshold
    .bind("below") // threshold_type - alert when success rate below 1.0
    .bind(notification_channels)
    .bind(&labels)
    .bind(&annotations)
    .fetch_one(db)
    .await?;

    info!(
        "Created auto alert rule {} for health check {}",
        row.0, check_id
    );

    Ok(row.0)
}

// ============================================================================
// Test Notification
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct TestNotificationRequest {
    pub channel_ids: Vec<Uuid>,
}

#[derive(Debug, Serialize)]
pub struct TestNotificationResponse {
    pub success: bool,
    pub message: String,
    pub results: Vec<ChannelTestResult>,
}

#[derive(Debug, Serialize)]
pub struct ChannelTestResult {
    pub channel_id: Uuid,
    pub channel_type: String,
    pub success: bool,
    pub error: Option<String>,
}

/// Send test notification to selected channels
/// POST /api/alerting/test-notification
async fn test_notification(
    State(state): State<Arc<WatchState>>,
    Json(payload): Json<TestNotificationRequest>,
) -> Result<Json<TestNotificationResponse>> {
    use crate::alerts::{
        load_notification_channel, send_notification, AlertNotification, AlertState,
    };
    use std::collections::BTreeMap;

    let mut results = Vec::new();

    // Create a test notification
    let test_notification = AlertNotification {
        alert_id: Uuid::new_v4(),
        rule_id: Uuid::new_v4(),
        rule_name: "Test Alert".to_string(),
        state: AlertState::Firing,
        value: Some(100.0),
        threshold: Some(50.0),
        compare_op: "above".to_string(),
        labels: BTreeMap::new(),
        annotations: {
            let mut ann = BTreeMap::new();
            ann.insert(
                "summary".to_string(),
                "This is a test notification".to_string(),
            );
            ann.insert(
                "description".to_string(),
                "Test notification to verify channel configuration".to_string(),
            );
            ann
        },
        fired_at: Some(chrono::Utc::now()),
        resolved_at: None,
        is_missing: false,
    };

    // Send test notification to each channel
    for channel_id in &payload.channel_ids {
        match load_notification_channel(&state.db, *channel_id).await {
            Ok(Some(channel)) => {
                let channel_type_str = channel.channel_type.clone();

                match send_notification(&channel, &test_notification).await {
                    Ok(_) => {
                        results.push(ChannelTestResult {
                            channel_id: *channel_id,
                            channel_type: channel_type_str,
                            success: true,
                            error: None,
                        });
                    }
                    Err(e) => {
                        results.push(ChannelTestResult {
                            channel_id: *channel_id,
                            channel_type: channel_type_str.to_string(),
                            success: false,
                            error: Some(e.to_string()),
                        });
                    }
                }
            }
            Ok(None) => {
                results.push(ChannelTestResult {
                    channel_id: *channel_id,
                    channel_type: "unknown".to_string(),
                    success: false,
                    error: Some("Channel not found".to_string()),
                });
            }
            Err(e) => {
                results.push(ChannelTestResult {
                    channel_id: *channel_id,
                    channel_type: "unknown".to_string(),
                    success: false,
                    error: Some(format!("Failed to load channel: {}", e)),
                });
            }
        }
    }

    let success_count = results.iter().filter(|r| r.success).count();
    let message = if success_count == results.len() {
        format!(
            "Test notification sent successfully to {} channel(s)",
            success_count
        )
    } else {
        format!(
            "Test notification sent to {}/{} channel(s)",
            success_count,
            results.len()
        )
    };

    Ok(Json(TestNotificationResponse {
        success: success_count > 0,
        message,
        results,
    }))
}
