use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::registry::ActionRegistry;

// ── List Maintenance Windows ────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListMaintenanceWindowsInput {}

#[derive(Serialize)]
pub struct ListMaintenanceWindowsOutput {
    pub maintenance_windows: serde_json::Value,
}

pub struct ListMaintenanceWindows;

#[async_trait]
impl PlatformAction for ListMaintenanceWindows {
    type Input = ListMaintenanceWindowsInput;
    type Output = ListMaintenanceWindowsOutput;

    fn name(&self) -> &'static str {
        "list_maintenance_windows"
    }
    fn description(&self) -> &'static str {
        "List all maintenance windows for the current project. Maintenance windows suppress \
         alerts during planned downtime."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        _input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let path = format!("/api/maintenance-windows?project_id={}", ctx.project_id);
        let resp = ctx.http.watch_get(&path).await?;
        let maintenance_windows = resp.json().await?;
        Ok(ListMaintenanceWindowsOutput {
            maintenance_windows,
        })
    }
}

// ── Get Maintenance Window ──────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetMaintenanceWindowInput {
    /// Maintenance window ID
    pub window_id: String,
}

#[derive(Serialize)]
pub struct GetMaintenanceWindowOutput {
    pub maintenance_window: serde_json::Value,
}

pub struct GetMaintenanceWindow;

#[async_trait]
impl PlatformAction for GetMaintenanceWindow {
    type Input = GetMaintenanceWindowInput;
    type Output = GetMaintenanceWindowOutput;

    fn name(&self) -> &'static str {
        "get_maintenance_window"
    }
    fn description(&self) -> &'static str {
        "Get details of a specific maintenance window including schedule, \
         recurrence configuration, and whether it is currently active."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let path = format!("/api/maintenance-windows/{}", input.window_id);
        let resp = ctx.http.watch_get(&path).await?;
        let maintenance_window = resp.json().await?;
        Ok(GetMaintenanceWindowOutput { maintenance_window })
    }
}

// ── Create Maintenance Window ───────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct CreateMaintenanceWindowInput {
    /// Human-readable name for the maintenance window
    pub name: String,
    /// Optional description
    pub description: Option<String>,
    /// Schedule type: "one_time" or "recurring" (default: "one_time")
    pub schedule_type: Option<String>,
    /// Start time for one-time windows (ISO 8601 timestamp, e.g. "2025-06-15T02:00:00Z")
    pub start_time: Option<String>,
    /// End time for one-time windows (ISO 8601 timestamp)
    pub end_time: Option<String>,
    /// Recurrence type for recurring windows: "daily", "weekly", "monthly"
    pub recurrence_type: Option<String>,
    /// Days of week/month for recurrence (0=Sunday for weekly, 1-31 for monthly)
    pub recurrence_days: Option<Vec<i32>>,
    /// Start time of day for recurring windows (HH:MM format, e.g. "02:00")
    pub recurrence_start_time: Option<String>,
    /// Duration in minutes for recurring windows
    pub recurrence_duration_minutes: Option<i32>,
    /// Timezone for recurring windows (e.g. "America/New_York", "UTC")
    pub recurrence_timezone: Option<String>,
    /// End date for recurring windows (YYYY-MM-DD format). Null = no end date.
    pub recurrence_end_date: Option<String>,
    /// Whether the window is enabled (defaults to true)
    pub enabled: Option<bool>,
}

#[derive(Serialize)]
pub struct CreateMaintenanceWindowOutput {
    pub maintenance_window: serde_json::Value,
}

pub struct CreateMaintenanceWindow;

#[async_trait]
impl PlatformAction for CreateMaintenanceWindow {
    type Input = CreateMaintenanceWindowInput;
    type Output = CreateMaintenanceWindowOutput;

    fn name(&self) -> &'static str {
        "create_maintenance_window"
    }
    fn description(&self) -> &'static str {
        "Create a maintenance window to suppress alerts during planned downtime. \
         For one-time windows, provide start_time and end_time. \
         For recurring windows, set schedule_type to 'recurring' and configure \
         recurrence_type, recurrence_days, recurrence_start_time, and recurrence_duration_minutes."
    }
    fn required_scope(&self) -> String {
        "observability:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let mut body = serde_json::json!({
            "project_id": ctx.project_id,
            "name": input.name,
            "schedule_type": input.schedule_type.unwrap_or_else(|| "one_time".into()),
            "enabled": input.enabled.unwrap_or(true),
        });
        let obj = body.as_object_mut().unwrap();
        if let Some(d) = input.description {
            obj.insert("description".into(), serde_json::Value::String(d));
        }
        if let Some(v) = input.start_time {
            obj.insert("start_time".into(), serde_json::Value::String(v));
        }
        if let Some(v) = input.end_time {
            obj.insert("end_time".into(), serde_json::Value::String(v));
        }
        if let Some(v) = input.recurrence_type {
            obj.insert("recurrence_type".into(), serde_json::Value::String(v));
        }
        if let Some(v) = input.recurrence_days {
            obj.insert("recurrence_days".into(), serde_json::to_value(v)?);
        }
        if let Some(v) = input.recurrence_start_time {
            obj.insert("recurrence_start_time".into(), serde_json::Value::String(v));
        }
        if let Some(v) = input.recurrence_duration_minutes {
            obj.insert("recurrence_duration_minutes".into(), serde_json::json!(v));
        }
        if let Some(v) = input.recurrence_timezone {
            obj.insert("recurrence_timezone".into(), serde_json::Value::String(v));
        }
        if let Some(v) = input.recurrence_end_date {
            obj.insert("recurrence_end_date".into(), serde_json::Value::String(v));
        }
        let resp = ctx
            .http
            .watch_post("/api/maintenance-windows", &body)
            .await?;
        let maintenance_window = resp.json().await?;
        Ok(CreateMaintenanceWindowOutput { maintenance_window })
    }
}

// ── Update Maintenance Window ───────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct UpdateMaintenanceWindowInput {
    /// Maintenance window ID
    pub window_id: String,
    /// Updated name
    pub name: Option<String>,
    /// Updated description
    pub description: Option<String>,
    /// Updated schedule type
    pub schedule_type: Option<String>,
    /// Updated start time (ISO 8601)
    pub start_time: Option<String>,
    /// Updated end time (ISO 8601)
    pub end_time: Option<String>,
    /// Updated recurrence type
    pub recurrence_type: Option<String>,
    /// Updated recurrence days
    pub recurrence_days: Option<Vec<i32>>,
    /// Updated recurrence start time (HH:MM)
    pub recurrence_start_time: Option<String>,
    /// Updated recurrence duration in minutes
    pub recurrence_duration_minutes: Option<i32>,
    /// Updated recurrence timezone
    pub recurrence_timezone: Option<String>,
    /// Updated recurrence end date (YYYY-MM-DD)
    pub recurrence_end_date: Option<String>,
    /// Updated enabled flag
    pub enabled: Option<bool>,
}

#[derive(Serialize)]
pub struct UpdateMaintenanceWindowOutput {
    pub maintenance_window: serde_json::Value,
}

pub struct UpdateMaintenanceWindow;

#[async_trait]
impl PlatformAction for UpdateMaintenanceWindow {
    type Input = UpdateMaintenanceWindowInput;
    type Output = UpdateMaintenanceWindowOutput;

    fn name(&self) -> &'static str {
        "update_maintenance_window"
    }
    fn description(&self) -> &'static str {
        "Update an existing maintenance window's schedule, recurrence, or enabled state."
    }
    fn required_scope(&self) -> String {
        "observability:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let mut body = serde_json::Map::new();
        if let Some(n) = input.name {
            body.insert("name".into(), serde_json::Value::String(n));
        }
        if let Some(d) = input.description {
            body.insert("description".into(), serde_json::Value::String(d));
        }
        if let Some(v) = input.schedule_type {
            body.insert("schedule_type".into(), serde_json::Value::String(v));
        }
        if let Some(v) = input.start_time {
            body.insert("start_time".into(), serde_json::Value::String(v));
        }
        if let Some(v) = input.end_time {
            body.insert("end_time".into(), serde_json::Value::String(v));
        }
        if let Some(v) = input.recurrence_type {
            body.insert("recurrence_type".into(), serde_json::Value::String(v));
        }
        if let Some(v) = input.recurrence_days {
            body.insert("recurrence_days".into(), serde_json::to_value(v)?);
        }
        if let Some(v) = input.recurrence_start_time {
            body.insert("recurrence_start_time".into(), serde_json::Value::String(v));
        }
        if let Some(v) = input.recurrence_duration_minutes {
            body.insert("recurrence_duration_minutes".into(), serde_json::json!(v));
        }
        if let Some(v) = input.recurrence_timezone {
            body.insert("recurrence_timezone".into(), serde_json::Value::String(v));
        }
        if let Some(v) = input.recurrence_end_date {
            body.insert("recurrence_end_date".into(), serde_json::Value::String(v));
        }
        if let Some(v) = input.enabled {
            body.insert("enabled".into(), serde_json::Value::Bool(v));
        }
        let resp = ctx
            .http
            .watch_put(
                &format!("/api/maintenance-windows/{}", input.window_id),
                &serde_json::Value::Object(body),
            )
            .await?;
        let maintenance_window = resp.json().await?;
        Ok(UpdateMaintenanceWindowOutput { maintenance_window })
    }
}

// ── Delete Maintenance Window ───────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct DeleteMaintenanceWindowInput {
    /// Maintenance window ID to delete
    pub window_id: String,
}

#[derive(Serialize)]
pub struct DeleteMaintenanceWindowOutput {
    pub success: bool,
}

pub struct DeleteMaintenanceWindow;

#[async_trait]
impl PlatformAction for DeleteMaintenanceWindow {
    type Input = DeleteMaintenanceWindowInput;
    type Output = DeleteMaintenanceWindowOutput;

    fn name(&self) -> &'static str {
        "delete_maintenance_window"
    }
    fn description(&self) -> &'static str {
        "Delete a maintenance window. Alerts will no longer be suppressed during the window's time range."
    }
    fn required_scope(&self) -> String {
        "observability:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        ctx.http
            .watch_delete(&format!("/api/maintenance-windows/{}", input.window_id))
            .await?;
        Ok(DeleteMaintenanceWindowOutput { success: true })
    }
}

// ── Registration ─────────────────────────────────────────────────────

pub fn register(registry: &mut ActionRegistry) {
    registry.register(ListMaintenanceWindows);
    registry.register(GetMaintenanceWindow);
    registry.register(CreateMaintenanceWindow);
    registry.register(UpdateMaintenanceWindow);
    registry.register(DeleteMaintenanceWindow);
}
