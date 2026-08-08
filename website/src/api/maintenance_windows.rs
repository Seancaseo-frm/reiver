//! Maintenance Windows API endpoints
//!
//! Manages scheduled maintenance windows for silencing alerts and health checks.

use axum::{
    extract::{Path, Query, State},
    response::Json,
    routing::get,
    Router,
};
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

use crate::app_state::WebsiteState;
use crate::audit::{AuditCaller, AuditEventBuilder, AuditEventType, AuditOrigin};
use crate::error::{AppError, Result};
use crate::maintenance::MaintenanceWindow;
use axum::http::HeaderMap;

pub fn create_maintenance_windows_router() -> Router<Arc<WebsiteState>> {
    Router::new()
        .route("/", get(list_windows).post(create_window))
        .route(
            "/{id}",
            get(get_window).put(update_window).delete(delete_window),
        )
        .route("/active", get(list_active_windows))
}

// ============================================================================
// Types
// ============================================================================

#[derive(Debug, Serialize)]
pub struct MaintenanceWindowResponse {
    pub id: Uuid,
    pub project_id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub schedule_type: String,
    // One-time fields
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    // Recurring fields
    pub recurrence_type: Option<String>,
    pub recurrence_days: Option<Vec<i32>>,
    pub recurrence_start_time: Option<String>, // HH:MM format
    pub recurrence_duration_minutes: Option<i32>,
    pub recurrence_timezone: Option<String>,
    pub recurrence_end_date: Option<String>, // YYYY-MM-DD format
    // Status
    pub enabled: bool,
    pub is_active: bool,
    pub next_occurrence: Option<DateTime<Utc>>,
    // Metadata
    pub created_by: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<MaintenanceWindow> for MaintenanceWindowResponse {
    fn from(w: MaintenanceWindow) -> Self {
        let now = Utc::now();
        let is_active = w.is_active(now);
        let next_occurrence = w.next_occurrence(now);

        MaintenanceWindowResponse {
            id: w.id,
            project_id: w.project_id,
            name: w.name,
            description: w.description,
            schedule_type: w.schedule_type,
            start_time: w.start_time,
            end_time: w.end_time,
            recurrence_type: w.recurrence_type,
            recurrence_days: w.recurrence_days,
            recurrence_start_time: w
                .recurrence_start_time
                .map(|t| t.format("%H:%M").to_string()),
            recurrence_duration_minutes: w.recurrence_duration_minutes,
            recurrence_timezone: w.recurrence_timezone,
            recurrence_end_date: w
                .recurrence_end_date
                .map(|d| d.format("%Y-%m-%d").to_string()),
            enabled: w.enabled,
            is_active,
            next_occurrence,
            created_by: w.created_by,
            created_at: w.created_at,
            updated_at: w.updated_at,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateMaintenanceWindowRequest {
    pub project_id: Uuid,
    pub name: String,
    pub description: Option<String>,
    #[serde(default = "default_schedule_type")]
    pub schedule_type: String,
    // One-time fields
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    // Recurring fields
    pub recurrence_type: Option<String>,
    pub recurrence_days: Option<Vec<i32>>,
    pub recurrence_start_time: Option<String>, // HH:MM format
    pub recurrence_duration_minutes: Option<i32>,
    pub recurrence_timezone: Option<String>,
    pub recurrence_end_date: Option<String>, // YYYY-MM-DD format
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

#[derive(Debug, Deserialize)]
pub struct UpdateMaintenanceWindowRequest {
    pub name: Option<String>,
    pub description: Option<String>,
    pub schedule_type: Option<String>,
    // One-time fields
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    // Recurring fields
    pub recurrence_type: Option<String>,
    pub recurrence_days: Option<Vec<i32>>,
    pub recurrence_start_time: Option<String>,
    pub recurrence_duration_minutes: Option<i32>,
    pub recurrence_timezone: Option<String>,
    pub recurrence_end_date: Option<String>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct ListWindowsQuery {
    pub project_id: Uuid,
}

#[derive(Debug, Deserialize)]
pub struct ListActiveQuery {
    pub project_id: Uuid,
}

fn default_schedule_type() -> String {
    "one_time".to_string()
}

fn default_enabled() -> bool {
    true
}

// ============================================================================
// Handlers
// ============================================================================

/// List all maintenance windows for a project
async fn list_windows(
    State(state): State<Arc<WebsiteState>>,
    Query(query): Query<ListWindowsQuery>,
) -> Result<Json<Vec<MaintenanceWindowResponse>>> {
    let windows = sqlx::query_as::<_, MaintenanceWindow>(
        r#"
        SELECT id, project_id, name, description,
               schedule_type, start_time, end_time,
               recurrence_type, recurrence_days, recurrence_start_time,
               recurrence_duration_minutes, recurrence_timezone, recurrence_end_date,
               enabled, created_by, created_at, updated_at
        FROM maintenance_windows
        WHERE project_id = $1
        ORDER BY created_at DESC
        "#,
    )
    .bind(query.project_id)
    .fetch_all(state.db.as_ref())
    .await?;

    let responses: Vec<MaintenanceWindowResponse> = windows.into_iter().map(|w| w.into()).collect();
    Ok(Json(responses))
}

/// List currently active maintenance windows for a project
async fn list_active_windows(
    State(state): State<Arc<WebsiteState>>,
    Query(query): Query<ListActiveQuery>,
) -> Result<Json<Vec<MaintenanceWindowResponse>>> {
    let windows = crate::maintenance::get_active_windows(state.db.as_ref(), query.project_id)
        .await
        .map_err(|e| AppError::Internal(e))?;

    let responses: Vec<MaintenanceWindowResponse> = windows.into_iter().map(|w| w.into()).collect();
    Ok(Json(responses))
}

/// Get a specific maintenance window
async fn get_window(
    State(state): State<Arc<WebsiteState>>,
    Path(id): Path<Uuid>,
) -> Result<Json<MaintenanceWindowResponse>> {
    let window = sqlx::query_as::<_, MaintenanceWindow>(
        r#"
        SELECT id, project_id, name, description,
               schedule_type, start_time, end_time,
               recurrence_type, recurrence_days, recurrence_start_time,
               recurrence_duration_minutes, recurrence_timezone, recurrence_end_date,
               enabled, created_by, created_at, updated_at
        FROM maintenance_windows
        WHERE id = $1
        "#,
    )
    .bind(id)
    .fetch_optional(state.db.as_ref())
    .await?;

    match window {
        Some(w) => Ok(Json(w.into())),
        None => Err(AppError::NotFound(format!(
            "Maintenance window {} not found",
            id
        ))),
    }
}

/// Create a new maintenance window
async fn create_window(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(req): Json<CreateMaintenanceWindowRequest>,
) -> Result<Json<MaintenanceWindowResponse>> {
    // Validate request
    validate_create_request(&req)?;

    // Parse recurrence_start_time if provided
    let recurrence_start_time = req
        .recurrence_start_time
        .as_ref()
        .map(|s| parse_time(s))
        .transpose()
        .map_err(|e| AppError::Validation(format!("Invalid recurrence_start_time: {}", e)))?;

    // Parse recurrence_end_date if provided
    let recurrence_end_date = req
        .recurrence_end_date
        .as_ref()
        .map(|s| parse_date(s))
        .transpose()
        .map_err(|e| AppError::Validation(format!("Invalid recurrence_end_date: {}", e)))?;

    let id = Uuid::new_v4();
    let now = Utc::now();

    sqlx::query(
        r#"
        INSERT INTO maintenance_windows (
            id, project_id, name, description,
            schedule_type, start_time, end_time,
            recurrence_type, recurrence_days, recurrence_start_time,
            recurrence_duration_minutes, recurrence_timezone, recurrence_end_date,
            enabled, created_at, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
        "#,
    )
    .bind(id)
    .bind(req.project_id)
    .bind(&req.name)
    .bind(&req.description)
    .bind(&req.schedule_type)
    .bind(req.start_time)
    .bind(req.end_time)
    .bind(&req.recurrence_type)
    .bind(&req.recurrence_days)
    .bind(recurrence_start_time)
    .bind(req.recurrence_duration_minutes)
    .bind(&req.recurrence_timezone)
    .bind(recurrence_end_date)
    .bind(req.enabled)
    .bind(now)
    .bind(now)
    .execute(state.db.as_ref())
    .await?;

    info!(
        "Created maintenance window {} for project {}",
        id, req.project_id
    );

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::MaintenanceWindowCreated)
        .resource("maintenance_window", id)
        .details(serde_json::json!({
            "created": {
                "name": &req.name,
                "project_id": req.project_id,
                "schedule_type": &req.schedule_type,
                "enabled": req.enabled,
            }
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

    // Fetch and return the created window
    let window = sqlx::query_as::<_, MaintenanceWindow>(
        r#"
        SELECT id, project_id, name, description,
               schedule_type, start_time, end_time,
               recurrence_type, recurrence_days, recurrence_start_time,
               recurrence_duration_minutes, recurrence_timezone, recurrence_end_date,
               enabled, created_by, created_at, updated_at
        FROM maintenance_windows
        WHERE id = $1
        "#,
    )
    .bind(id)
    .fetch_one(state.db.as_ref())
    .await?;

    Ok(Json(window.into()))
}

/// Update a maintenance window
async fn update_window(
    State(state): State<Arc<WebsiteState>>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(req): Json<UpdateMaintenanceWindowRequest>,
) -> Result<Json<MaintenanceWindowResponse>> {
    // Check if window exists
    let existing = sqlx::query_as::<_, MaintenanceWindow>(
        r#"
        SELECT id, project_id, name, description,
               schedule_type, start_time, end_time,
               recurrence_type, recurrence_days, recurrence_start_time,
               recurrence_duration_minutes, recurrence_timezone, recurrence_end_date,
               enabled, created_by, created_at, updated_at
        FROM maintenance_windows
        WHERE id = $1
        "#,
    )
    .bind(id)
    .fetch_optional(state.db.as_ref())
    .await?;

    if existing.is_none() {
        return Err(AppError::NotFound(format!(
            "Maintenance window {} not found",
            id
        )));
    }

    let existing = existing.unwrap();

    let before_name = existing.name.clone();
    let before_schedule_type = existing.schedule_type.clone();
    let before_enabled = existing.enabled;

    // Build update with merged values
    let name = req.name.unwrap_or(existing.name);
    let description = req.description.or(existing.description);
    let schedule_type = req.schedule_type.unwrap_or(existing.schedule_type);
    let start_time = req.start_time.or(existing.start_time);
    let end_time = req.end_time.or(existing.end_time);
    let recurrence_type = req.recurrence_type.or(existing.recurrence_type);
    let recurrence_days = req.recurrence_days.or(existing.recurrence_days);
    let recurrence_duration_minutes = req
        .recurrence_duration_minutes
        .or(existing.recurrence_duration_minutes);
    let recurrence_timezone = req.recurrence_timezone.or(existing.recurrence_timezone);
    let enabled = req.enabled.unwrap_or(existing.enabled);

    // Parse recurrence_start_time if provided, otherwise keep existing
    let recurrence_start_time =
        if let Some(ref time_str) = req.recurrence_start_time {
            Some(parse_time(time_str).map_err(|e| {
                AppError::Validation(format!("Invalid recurrence_start_time: {}", e))
            })?)
        } else {
            existing.recurrence_start_time
        };

    // Parse recurrence_end_date if provided, otherwise keep existing
    let recurrence_end_date = if let Some(ref date_str) = req.recurrence_end_date {
        Some(
            parse_date(date_str)
                .map_err(|e| AppError::Validation(format!("Invalid recurrence_end_date: {}", e)))?,
        )
    } else {
        existing.recurrence_end_date
    };

    sqlx::query(
        r#"
        UPDATE maintenance_windows SET
            name = $2,
            description = $3,
            schedule_type = $4,
            start_time = $5,
            end_time = $6,
            recurrence_type = $7,
            recurrence_days = $8,
            recurrence_start_time = $9,
            recurrence_duration_minutes = $10,
            recurrence_timezone = $11,
            recurrence_end_date = $12,
            enabled = $13,
            updated_at = NOW()
        WHERE id = $1
        "#,
    )
    .bind(id)
    .bind(&name)
    .bind(&description)
    .bind(&schedule_type)
    .bind(start_time)
    .bind(end_time)
    .bind(&recurrence_type)
    .bind(&recurrence_days)
    .bind(recurrence_start_time)
    .bind(recurrence_duration_minutes)
    .bind(&recurrence_timezone)
    .bind(recurrence_end_date)
    .bind(enabled)
    .execute(state.db.as_ref())
    .await?;

    info!("Updated maintenance window {}", id);

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::MaintenanceWindowUpdated)
        .resource("maintenance_window", id)
        .details(serde_json::json!({
            "before": {
                "name": &before_name,
                "schedule_type": &before_schedule_type,
                "enabled": before_enabled,
            },
            "after": {
                "name": &name,
                "schedule_type": &schedule_type,
                "enabled": enabled,
            },
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

    // Fetch and return the updated window
    let window = sqlx::query_as::<_, MaintenanceWindow>(
        r#"
        SELECT id, project_id, name, description,
               schedule_type, start_time, end_time,
               recurrence_type, recurrence_days, recurrence_start_time,
               recurrence_duration_minutes, recurrence_timezone, recurrence_end_date,
               enabled, created_by, created_at, updated_at
        FROM maintenance_windows
        WHERE id = $1
        "#,
    )
    .bind(id)
    .fetch_one(state.db.as_ref())
    .await?;

    Ok(Json(window.into()))
}

/// Delete a maintenance window
async fn delete_window(
    State(state): State<Arc<WebsiteState>>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>> {
    let before: Option<(String, String)> =
        sqlx::query_as("SELECT name, schedule_type FROM maintenance_windows WHERE id = $1")
            .bind(id)
            .fetch_optional(state.db.as_ref())
            .await?;

    let (before_name, before_schedule_type) =
        before.ok_or_else(|| AppError::NotFound(format!("Maintenance window {} not found", id)))?;

    sqlx::query("DELETE FROM maintenance_windows WHERE id = $1")
        .bind(id)
        .execute(state.db.as_ref())
        .await?;

    info!("Deleted maintenance window {}", id);

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::MaintenanceWindowDeleted)
        .resource("maintenance_window", id)
        .details(serde_json::json!({
            "deleted": {
                "name": &before_name,
                "schedule_type": &before_schedule_type,
            }
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

    Ok(Json(serde_json::json!({ "deleted": true, "id": id })))
}

// ============================================================================
// Helpers
// ============================================================================

fn validate_create_request(req: &CreateMaintenanceWindowRequest) -> Result<()> {
    if req.name.trim().is_empty() {
        return Err(AppError::Validation("Name is required".to_string()));
    }

    match req.schedule_type.as_str() {
        "one_time" => {
            if req.start_time.is_none() {
                return Err(AppError::Validation(
                    "start_time is required for one_time schedule".to_string(),
                ));
            }
            if req.end_time.is_none() {
                return Err(AppError::Validation(
                    "end_time is required for one_time schedule".to_string(),
                ));
            }
            if let (Some(start), Some(end)) = (req.start_time, req.end_time) {
                if end <= start {
                    return Err(AppError::Validation(
                        "end_time must be after start_time".to_string(),
                    ));
                }
            }
        }
        "recurring" => {
            if req.recurrence_type.is_none() {
                return Err(AppError::Validation(
                    "recurrence_type is required for recurring schedule".to_string(),
                ));
            }
            if req.recurrence_start_time.is_none() {
                return Err(AppError::Validation(
                    "recurrence_start_time is required for recurring schedule".to_string(),
                ));
            }
            if req.recurrence_duration_minutes.is_none() {
                return Err(AppError::Validation(
                    "recurrence_duration_minutes is required for recurring schedule".to_string(),
                ));
            }

            // Validate recurrence_type and recurrence_days
            if let Some(ref recurrence_type) = req.recurrence_type {
                match recurrence_type.as_str() {
                    "daily" => {} // No days required
                    "weekly" => {
                        if req.recurrence_days.is_none()
                            || req
                                .recurrence_days
                                .as_ref()
                                .map(|d| d.is_empty())
                                .unwrap_or(true)
                        {
                            return Err(AppError::Validation(
                                "recurrence_days is required for weekly schedule".to_string(),
                            ));
                        }
                        // Validate day values (0-6)
                        if let Some(ref days) = req.recurrence_days {
                            for day in days {
                                if *day < 0 || *day > 6 {
                                    return Err(AppError::Validation(format!(
                                        "Invalid day value: {}. Must be 0-6 (Sunday-Saturday)",
                                        day
                                    )));
                                }
                            }
                        }
                    }
                    "monthly" => {
                        if req.recurrence_days.is_none()
                            || req
                                .recurrence_days
                                .as_ref()
                                .map(|d| d.is_empty())
                                .unwrap_or(true)
                        {
                            return Err(AppError::Validation(
                                "recurrence_days is required for monthly schedule".to_string(),
                            ));
                        }
                        // Validate day values (1-31)
                        if let Some(ref days) = req.recurrence_days {
                            for day in days {
                                if *day < 1 || *day > 31 {
                                    return Err(AppError::Validation(format!(
                                        "Invalid day value: {}. Must be 1-31",
                                        day
                                    )));
                                }
                            }
                        }
                    }
                    _ => {
                        return Err(AppError::Validation(format!(
                            "Invalid recurrence_type: {}. Must be daily, weekly, or monthly",
                            recurrence_type
                        )));
                    }
                }
            }

            // Validate duration
            if let Some(duration) = req.recurrence_duration_minutes {
                if duration <= 0 {
                    return Err(AppError::Validation(
                        "recurrence_duration_minutes must be positive".to_string(),
                    ));
                }
                if duration > 24 * 60 {
                    return Err(AppError::Validation(
                        "recurrence_duration_minutes cannot exceed 24 hours (1440 minutes)"
                            .to_string(),
                    ));
                }
            }
        }
        _ => {
            return Err(AppError::Validation(format!(
                "Invalid schedule_type: {}. Must be one_time or recurring",
                req.schedule_type
            )));
        }
    }

    Ok(())
}

fn parse_time(s: &str) -> anyhow::Result<NaiveTime> {
    NaiveTime::parse_from_str(s, "%H:%M")
        .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S"))
        .map_err(|e| anyhow::anyhow!("Failed to parse time '{}': {}", s, e))
}

fn parse_date(s: &str) -> anyhow::Result<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map_err(|e| anyhow::anyhow!("Failed to parse date '{}': {}", s, e))
}
