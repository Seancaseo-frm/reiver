use axum::{
    extract::{Extension, Path, State},
    http::HeaderMap,
    response::Json,
    routing::get,
    Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::PgPool as DbPool;
use std::sync::Arc;
use uuid::Uuid;

use crate::app_state::WebsiteState;
use crate::audit::{AuditCaller, AuditEventBuilder, AuditEventType, AuditOrigin};
use crate::auth::{authenticate_request_or_api_key, verify_project_access, AuthIdentity};
use crate::error::{AppError, Result};
use crate::models::{Dashboard, DashboardTab, DashboardWidget};
use crate::rate_limit::RateLimitType;

/// Authorize a caller (user JWT or project API key) for a given project.
/// Returns `Some(user_id)` for JWT callers. For API key callers, returns
/// the `X-User-Id` header value if present (set by internal services like
/// the MCP proxy), otherwise `None`.
async fn authorize_project(
    headers: &HeaderMap,
    state: &WebsiteState,
    project_id: Uuid,
) -> Result<Option<Uuid>> {
    match authenticate_request_or_api_key(headers, state, RateLimitType::Crud).await? {
        AuthIdentity::User(user_id) => {
            verify_project_access(&state.db, project_id, user_id).await?;
            Ok(Some(user_id))
        }
        AuthIdentity::ApiKey {
            project_id: key_pid,
        } => {
            if key_pid != project_id {
                return Err(AppError::Auth(
                    "API key does not belong to this project".into(),
                ));
            }
            let user_id = headers
                .get("X-User-Id")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| Uuid::parse_str(s).ok());
            Ok(user_id)
        }
    }
}

pub fn create_dashboards_router() -> Router<Arc<WebsiteState>> {
    Router::new()
        .route("/dashboard-templates", get(list_templates))
        .route("/dashboard-templates/{template_id}", get(get_template))
        .route(
            "/{project_id}/dashboards",
            get(list_dashboards).post(create_dashboard),
        )
        .route(
            "/{project_id}/dashboards/from-template",
            axum::routing::post(create_from_template),
        )
        .route(
            "/{project_id}/dashboards/import-datadog",
            axum::routing::post(import_datadog_dashboard),
        )
        .route(
            "/{project_id}/dashboards/import-grafana",
            axum::routing::post(import_grafana_dashboard),
        )
        .route(
            "/{project_id}/dashboards/{dashboard_id}",
            get(get_dashboard)
                .put(update_dashboard)
                .delete(delete_dashboard),
        )
        // Tabs
        .route(
            "/{project_id}/dashboards/{dashboard_id}/tabs",
            get(list_tabs).post(create_tab),
        )
        .route(
            "/{project_id}/dashboards/{dashboard_id}/tabs/{tab_id}",
            get(get_tab).put(update_tab).delete(delete_tab),
        )
        .route(
            "/{project_id}/dashboards/{dashboard_id}/tabs/{tab_id}/widgets",
            get(list_tab_widgets),
        )
        // Widgets
        .route(
            "/{project_id}/dashboards/{dashboard_id}/widgets",
            get(list_widgets).post(create_widget),
        )
        .route(
            "/{project_id}/dashboards/{dashboard_id}/widgets/{widget_id}",
            get(get_widget).put(update_widget).delete(delete_widget),
        )
}

#[derive(Debug, Deserialize)]
struct CreateDashboardRequest {
    name: String,
    description: Option<String>,
    layout_config: Option<serde_json::Value>,
    refresh_interval: Option<i32>,
    time_range: Option<String>,
}

#[derive(Debug, Deserialize)]
struct UpdateDashboardRequest {
    name: Option<String>,
    description: Option<String>,
    layout_config: Option<serde_json::Value>,
    refresh_interval: Option<i32>,
    time_range: Option<String>,
    locked: Option<bool>,
}

#[derive(Debug, Serialize)]
struct DashboardResponse {
    id: Uuid,
    project_id: Uuid,
    user_id: Uuid,
    name: String,
    description: Option<String>,
    is_default: bool,
    layout_config: serde_json::Value,
    refresh_interval: Option<i32>,
    time_range: Option<String>,
    locked: bool,
    import_source: serde_json::Value,
    created_at: chrono::DateTime<Utc>,
    updated_at: chrono::DateTime<Utc>,
}

impl From<Dashboard> for DashboardResponse {
    fn from(dashboard: Dashboard) -> Self {
        DashboardResponse {
            id: dashboard.id,
            project_id: dashboard.project_id,
            user_id: dashboard.user_id,
            name: dashboard.name,
            description: dashboard.description,
            is_default: dashboard.is_default,
            layout_config: dashboard.layout_config,
            refresh_interval: dashboard.refresh_interval,
            time_range: dashboard.time_range,
            locked: dashboard.locked,
            import_source: dashboard.import_source,
            created_at: dashboard.created_at,
            updated_at: dashboard.updated_at,
        }
    }
}

// Tab request/response types

#[derive(Debug, Deserialize)]
struct CreateTabRequest {
    name: String,
    display_order: Option<i32>,
    icon: Option<String>,
}

#[derive(Debug, Deserialize)]
struct UpdateTabRequest {
    name: Option<String>,
    display_order: Option<i32>,
    icon: Option<String>,
}

#[derive(Debug, Serialize)]
struct TabResponse {
    id: Uuid,
    dashboard_id: Uuid,
    name: String,
    display_order: i32,
    icon: Option<String>,
    created_at: chrono::DateTime<Utc>,
    updated_at: chrono::DateTime<Utc>,
}

impl From<DashboardTab> for TabResponse {
    fn from(tab: DashboardTab) -> Self {
        TabResponse {
            id: tab.id,
            dashboard_id: tab.dashboard_id,
            name: tab.name,
            display_order: tab.display_order,
            icon: tab.icon,
            created_at: tab.created_at,
            updated_at: tab.updated_at,
        }
    }
}

// Widget request/response types

#[derive(Debug, Deserialize)]
struct CreateWidgetRequest {
    tab_id: Option<Uuid>,
    widget_type: String,
    widget_config: serde_json::Value,
    position_x: i32,
    position_y: i32,
    width: i32,
    height: i32,
    title: Option<String>,
}

#[derive(Debug, Deserialize)]
struct UpdateWidgetRequest {
    tab_id: Option<Uuid>,
    widget_type: Option<String>,
    widget_config: Option<serde_json::Value>,
    position_x: Option<i32>,
    position_y: Option<i32>,
    width: Option<i32>,
    height: Option<i32>,
    title: Option<String>,
}

#[derive(Debug, Serialize)]
struct WidgetResponse {
    id: Uuid,
    dashboard_id: Uuid,
    tab_id: Option<Uuid>,
    widget_type: String,
    widget_config: serde_json::Value,
    position_x: i32,
    position_y: i32,
    width: i32,
    height: i32,
    title: Option<String>,
    created_at: chrono::DateTime<Utc>,
    updated_at: chrono::DateTime<Utc>,
}

impl From<DashboardWidget> for WidgetResponse {
    fn from(widget: DashboardWidget) -> Self {
        WidgetResponse {
            id: widget.id,
            dashboard_id: widget.dashboard_id,
            tab_id: widget.tab_id,
            widget_type: widget.widget_type,
            widget_config: widget.widget_config,
            position_x: widget.position_x,
            position_y: widget.position_y,
            width: widget.width,
            height: widget.height,
            title: widget.title,
            created_at: widget.created_at,
            updated_at: widget.updated_at,
        }
    }
}

// Dashboard Templates

#[derive(Debug, Serialize, sqlx::FromRow)]
struct DashboardTemplate {
    id: Uuid,
    name: String,
    description: Option<String>,
    category: String,
    thumbnail_url: Option<String>,
    template_config: serde_json::Value, // Contains { "tabs": [...], "variables": [...] }
    tags: Vec<String>,
    is_featured: bool,
    display_order: i32,
    created_at: chrono::DateTime<Utc>,
    updated_at: chrono::DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct CreateFromTemplateRequest {
    template_id: Uuid,
    name: Option<String>, // Override template name
}

#[derive(Debug, Deserialize)]
struct TemplateQuery {
    category: Option<String>,
    search: Option<String>,
}

async fn list_templates(
    State(state): State<Arc<WebsiteState>>,
    axum::extract::Query(query): axum::extract::Query<TemplateQuery>,
) -> Result<Json<Vec<DashboardTemplate>>> {
    // Use parameterized queries to prevent SQL injection
    // Build query dynamically but with safe bind parameters
    let templates = match (&query.category, &query.search) {
        (Some(category), Some(search)) => {
            let search_pattern = format!("%{}%", search);
            sqlx::query_as::<_, DashboardTemplate>(
                "SELECT id, name, description, category, thumbnail_url, template_config, tags, is_featured, display_order, created_at, updated_at 
                 FROM dashboard_templates 
                 WHERE category = $1 AND (name ILIKE $2 OR description ILIKE $2 OR EXISTS (SELECT 1 FROM unnest(tags) t WHERE t ILIKE $2))
                 ORDER BY is_featured DESC, display_order ASC, name ASC"
            )
            .bind(category)
            .bind(&search_pattern)
            .fetch_all(&*state.db)
            .await
        }
        (Some(category), None) => {
            sqlx::query_as::<_, DashboardTemplate>(
                "SELECT id, name, description, category, thumbnail_url, template_config, tags, is_featured, display_order, created_at, updated_at 
                 FROM dashboard_templates 
                 WHERE category = $1
                 ORDER BY is_featured DESC, display_order ASC, name ASC"
            )
            .bind(category)
            .fetch_all(&*state.db)
            .await
        }
        (None, Some(search)) => {
            let search_pattern = format!("%{}%", search);
            sqlx::query_as::<_, DashboardTemplate>(
                "SELECT id, name, description, category, thumbnail_url, template_config, tags, is_featured, display_order, created_at, updated_at 
                 FROM dashboard_templates 
                 WHERE name ILIKE $1 OR description ILIKE $1 OR EXISTS (SELECT 1 FROM unnest(tags) t WHERE t ILIKE $1)
                 ORDER BY is_featured DESC, display_order ASC, name ASC"
            )
            .bind(&search_pattern)
            .fetch_all(&*state.db)
            .await
        }
        (None, None) => {
            sqlx::query_as::<_, DashboardTemplate>(
                "SELECT id, name, description, category, thumbnail_url, template_config, tags, is_featured, display_order, created_at, updated_at 
                 FROM dashboard_templates 
                 ORDER BY is_featured DESC, display_order ASC, name ASC"
            )
            .fetch_all(&*state.db)
            .await
        }
    }
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to fetch templates: {}", e)))?;

    Ok(Json(templates))
}

async fn get_template(
    State(state): State<Arc<WebsiteState>>,
    Path(template_id): Path<Uuid>,
) -> Result<Json<DashboardTemplate>> {
    let template = sqlx::query_as::<_, DashboardTemplate>(
        "SELECT id, name, description, category, thumbnail_url, template_config, tags, is_featured, display_order, created_at, updated_at 
         FROM dashboard_templates WHERE id = $1"
    )
    .bind(template_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to fetch template: {}", e)))?
    .ok_or_else(|| AppError::NotFound("Template not found".to_string()))?;

    Ok(Json(template))
}

async fn create_from_template(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Path(project_id): Path<Uuid>,
    Json(payload): Json<CreateFromTemplateRequest>,
) -> Result<Json<DashboardResponse>> {
    let user_id = authorize_project(&headers, &state, project_id)
        .await?
        .ok_or_else(|| {
            AppError::BadRequest("Could not determine user identity. API key callers must include X-User-Id header.".into())
        })?;

    // Fetch template
    let template = sqlx::query_as::<_, DashboardTemplate>(
        "SELECT id, name, description, category, thumbnail_url, template_config, tags, is_featured, display_order, created_at, updated_at 
         FROM dashboard_templates WHERE id = $1"
    )
    .bind(payload.template_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to fetch template: {}", e)))?
        .ok_or_else(|| AppError::NotFound("Template not found".to_string()))?;

    let now = Utc::now();
    let dashboard_name = payload
        .name
        .unwrap_or_else(|| format!("{} ({})", template.name, now.format("%b %d %H:%M")));

    // Extract variables from template config for layout_config
    let variables = template
        .template_config
        .get("variables")
        .cloned()
        .unwrap_or(serde_json::json!([]));
    let layout_config = serde_json::json!({ "variables": variables });

    let import_source = serde_json::json!({ "type": "template", "template_id": template.id });

    // Create dashboard from template
    let dashboard = sqlx::query_as::<_, Dashboard>(
        r#"INSERT INTO dashboards (project_id, user_id, name, description, layout_config, import_source, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $7)
        RETURNING *"#
    )
    .bind(project_id)
    .bind(user_id)
    .bind(&dashboard_name)
    .bind(&template.description)
    .bind(&layout_config)
    .bind(&import_source)
    .bind(now)
    .fetch_one(db.as_ref())
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to create dashboard: {}", e)))?;

    // Create tabs and widgets from template_config
    if let Some(tabs) = template
        .template_config
        .get("tabs")
        .and_then(|v| v.as_array())
    {
        for (tab_index, tab) in tabs.iter().enumerate() {
            let tab_name = tab
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("Untitled");
            let tab_icon = tab.get("icon").and_then(|v| v.as_str());

            // Create the tab
            let created_tab = sqlx::query_as::<_, DashboardTab>(
                r#"INSERT INTO dashboard_tabs (dashboard_id, name, display_order, icon, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $5)
                RETURNING *"#
            )
            .bind(dashboard.id)
            .bind(tab_name)
            .bind(tab_index as i32)
            .bind(tab_icon)
            .bind(now)
            .fetch_one(db.as_ref())
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to create tab: {}", e)))?;

            // Create widgets for this tab
            if let Some(widgets) = tab.get("widgets").and_then(|v| v.as_array()) {
                for widget in widgets {
                    let widget_type = widget
                        .get("type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown");
                    let title = widget.get("title").and_then(|v| v.as_str());
                    let widget_config = widget
                        .get("config")
                        .cloned()
                        .unwrap_or(serde_json::json!({}));
                    let x = widget.get("x").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                    let y = widget.get("y").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                    let w = widget.get("w").and_then(|v| v.as_i64()).unwrap_or(4) as i32;
                    let h = widget.get("h").and_then(|v| v.as_i64()).unwrap_or(3) as i32;

                    sqlx::query(
                        r#"INSERT INTO dashboard_widgets (dashboard_id, tab_id, widget_type, widget_config, position_x, position_y, width, height, title, created_at, updated_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)"#
                    )
                    .bind(dashboard.id)
                    .bind(created_tab.id)
                    .bind(widget_type)
                    .bind(&widget_config)
                    .bind(x)
                    .bind(y)
                    .bind(w)
                    .bind(h)
                    .bind(title)
                    .bind(now)
                    .execute(db.as_ref())
                    .await
                    .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to create widget: {}", e)))?;
                }
            }
        }
    }

    Ok(Json(DashboardResponse::from(dashboard)))
}

async fn list_dashboards(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Path(project_id): Path<Uuid>,
) -> Result<Json<Vec<DashboardResponse>>> {
    authorize_project(&headers, &state, project_id).await?;

    let dashboards = sqlx::query_as::<_, Dashboard>(
        "SELECT * FROM dashboards WHERE project_id = $1 ORDER BY is_default DESC, created_at DESC",
    )
    .bind(project_id)
    .fetch_all(&*db)
    .await?;

    Ok(Json(
        dashboards
            .into_iter()
            .map(DashboardResponse::from)
            .collect(),
    ))
}

async fn get_dashboard(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Path((project_id, dashboard_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<DashboardResponse>> {
    authorize_project(&headers, &state, project_id).await?;

    let dashboard = sqlx::query_as::<_, Dashboard>(
        "SELECT * FROM dashboards WHERE id = $1 AND project_id = $2",
    )
    .bind(dashboard_id)
    .bind(project_id)
    .fetch_optional(&*db)
    .await?
    .ok_or_else(|| AppError::NotFound("Dashboard not found".to_string()))?;

    Ok(Json(DashboardResponse::from(dashboard)))
}

async fn create_dashboard(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Path(project_id): Path<Uuid>,
    Json(payload): Json<CreateDashboardRequest>,
) -> Result<Json<DashboardResponse>> {
    let user_id = authorize_project(&headers, &state, project_id)
        .await?
        .ok_or_else(|| {
            AppError::BadRequest("Could not determine user identity. API key callers must include X-User-Id header.".into())
        })?;

    let import_source = serde_json::json!({ "type": "manual" });

    let dashboard = sqlx::query_as::<_, Dashboard>(
        "INSERT INTO dashboards (project_id, user_id, name, description, layout_config, import_source, refresh_interval, time_range) 
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8) 
         RETURNING *"
    )
    .bind(project_id)
    .bind(user_id)
    .bind(&payload.name)
    .bind(&payload.description)
    .bind(payload.layout_config.unwrap_or_else(|| serde_json::json!({})))
    .bind(&import_source)
    .bind(payload.refresh_interval)
    .bind(&payload.time_range)
    .fetch_one(&*db)
    .await?;

    let organization_id =
        sqlx::query_scalar::<_, Uuid>("SELECT organization_id FROM projects WHERE id = $1")
            .bind(project_id)
            .fetch_optional(&*db)
            .await
            .ok()
            .flatten();

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::DashboardCreated)
        .actor(user_id)
        .resource("dashboard", dashboard.id)
        .details(
            serde_json::json!({ "created": { "name": &payload.name, "project_id": project_id } }),
        )
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
        .success();
    if let Some(org_id) = organization_id {
        audit = audit.organization(org_id);
    }
    audit.log(&state.clickhouse).await;

    Ok(Json(DashboardResponse::from(dashboard)))
}

async fn update_dashboard(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Path((project_id, dashboard_id)): Path<(Uuid, Uuid)>,
    Json(payload): Json<UpdateDashboardRequest>,
) -> Result<Json<DashboardResponse>> {
    let caller_user_id = authorize_project(&headers, &state, project_id).await?;

    let before_dashboard = if let Some(user_id) = caller_user_id {
        sqlx::query_as::<_, Dashboard>(
            "SELECT * FROM dashboards WHERE id = $1 AND project_id = $2 AND user_id = $3",
        )
        .bind(dashboard_id)
        .bind(project_id)
        .bind(user_id)
        .fetch_optional(&*db)
        .await?
        .ok_or_else(|| AppError::NotFound("Dashboard not found".to_string()))?
    } else {
        sqlx::query_as::<_, Dashboard>("SELECT * FROM dashboards WHERE id = $1 AND project_id = $2")
            .bind(dashboard_id)
            .bind(project_id)
            .fetch_optional(&*db)
            .await?
            .ok_or_else(|| AppError::NotFound("Dashboard not found".to_string()))?
    };

    // Enforce locking: if dashboard is locked, only allow toggling the lock itself
    let is_only_unlock = payload.locked == Some(false)
        && payload.name.is_none()
        && payload.description.is_none()
        && payload.layout_config.is_none()
        && payload.refresh_interval.is_none()
        && payload.time_range.is_none();
    if before_dashboard.locked && !is_only_unlock {
        return Err(AppError::BadRequest(
            "Dashboard is locked. Unlock it before making changes.".to_string(),
        ));
    }

    let dashboard = sqlx::query_as::<_, Dashboard>(
        "UPDATE dashboards 
         SET name = COALESCE($1, name),
             description = COALESCE($2, description),
             layout_config = COALESCE($3, layout_config),
             refresh_interval = COALESCE($4, refresh_interval),
             time_range = COALESCE($5, time_range),
             locked = COALESCE($8, locked),
             updated_at = NOW()
         WHERE id = $6 AND project_id = $7
         RETURNING *",
    )
    .bind(payload.name.as_ref())
    .bind(payload.description.as_ref())
    .bind(payload.layout_config.as_ref())
    .bind(payload.refresh_interval)
    .bind(payload.time_range.as_ref())
    .bind(dashboard_id)
    .bind(project_id)
    .bind(payload.locked)
    .fetch_optional(&*db)
    .await?
    .ok_or_else(|| AppError::NotFound("Dashboard not found".to_string()))?;

    if let Some(uid) = caller_user_id {
        let organization_id =
            sqlx::query_scalar::<_, Uuid>("SELECT organization_id FROM projects WHERE id = $1")
                .bind(project_id)
                .fetch_optional(&*db)
                .await
                .ok()
                .flatten();

        let audit_origin = AuditOrigin::from_headers(&headers);
        let audit_caller = AuditCaller::from_headers(&headers);
        let mut audit = AuditEventBuilder::new(AuditEventType::DashboardUpdated)
            .actor(uid)
            .resource("dashboard", dashboard_id)
            .details(serde_json::json!({
                "before": { "name": &before_dashboard.name, "project_id": project_id },
                "after": { "name": &dashboard.name, "project_id": project_id },
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
            .success();
        if let Some(org_id) = organization_id {
            audit = audit.organization(org_id);
        }
        audit.log(&state.clickhouse).await;
    }

    Ok(Json(DashboardResponse::from(dashboard)))
}

async fn delete_dashboard(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Path((project_id, dashboard_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<serde_json::Value>> {
    let caller_user_id = authorize_project(&headers, &state, project_id).await?;

    let dashboard = sqlx::query_as::<_, Dashboard>(
        "SELECT * FROM dashboards WHERE id = $1 AND project_id = $2",
    )
    .bind(dashboard_id)
    .bind(project_id)
    .fetch_optional(&*db)
    .await?
    .ok_or_else(|| AppError::NotFound("Dashboard not found".to_string()))?;

    if dashboard.is_default {
        return Err(AppError::Validation(
            "Cannot delete default dashboard".to_string(),
        ));
    }

    sqlx::query("DELETE FROM dashboards WHERE id = $1")
        .bind(dashboard_id)
        .execute(&*db)
        .await?;

    if let Some(uid) = caller_user_id {
        let organization_id =
            sqlx::query_scalar::<_, Uuid>("SELECT organization_id FROM projects WHERE id = $1")
                .bind(project_id)
                .fetch_optional(&*db)
                .await
                .ok()
                .flatten();

        let audit_origin = AuditOrigin::from_headers(&headers);
        let audit_caller = AuditCaller::from_headers(&headers);
        let mut audit = AuditEventBuilder::new(AuditEventType::DashboardDeleted)
            .actor(uid)
            .resource("dashboard", dashboard_id)
            .details(serde_json::json!({ "deleted": { "name": &dashboard.name, "project_id": project_id } }))
            .origin(&audit_origin.origin_type, &audit_origin.origin_ref, &audit_origin.origin_reason)
        .caller(&audit_caller.caller_type, &audit_caller.key_label, &audit_caller.key_prefix)
            .success();
        if let Some(org_id) = organization_id {
            audit = audit.organization(org_id);
        }
        audit.log(&state.clickhouse).await;
    }

    Ok(Json(serde_json::json!({"success": true})))
}

// ============================================================================
// Tab CRUD Operations
// ============================================================================

async fn list_tabs(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Path((project_id, dashboard_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<Vec<TabResponse>>> {
    authorize_project(&headers, &state, project_id).await?;

    let tabs = sqlx::query_as::<_, DashboardTab>(
        "SELECT * FROM dashboard_tabs WHERE dashboard_id = $1 ORDER BY display_order ASC",
    )
    .bind(dashboard_id)
    .fetch_all(&*db)
    .await?;

    Ok(Json(tabs.into_iter().map(TabResponse::from).collect()))
}

async fn get_tab(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Path((project_id, dashboard_id, tab_id)): Path<(Uuid, Uuid, Uuid)>,
) -> Result<Json<TabResponse>> {
    authorize_project(&headers, &state, project_id).await?;

    let tab = sqlx::query_as::<_, DashboardTab>(
        "SELECT * FROM dashboard_tabs WHERE id = $1 AND dashboard_id = $2",
    )
    .bind(tab_id)
    .bind(dashboard_id)
    .fetch_optional(&*db)
    .await?
    .ok_or_else(|| AppError::NotFound("Tab not found".to_string()))?;

    Ok(Json(TabResponse::from(tab)))
}

async fn create_tab(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Path((project_id, dashboard_id)): Path<(Uuid, Uuid)>,
    Json(payload): Json<CreateTabRequest>,
) -> Result<Json<TabResponse>> {
    authorize_project(&headers, &state, project_id).await?;

    // Get next display_order if not provided
    let display_order = match payload.display_order {
        Some(order) => order,
        None => {
            let result: (i64,) = sqlx::query_as(
                "SELECT COALESCE(MAX(display_order), -1) + 1 FROM dashboard_tabs WHERE dashboard_id = $1"
            )
            .bind(dashboard_id)
            .fetch_one(db.as_ref())
            .await?;
            result.0 as i32
        }
    };

    let tab = sqlx::query_as::<_, DashboardTab>(
        "INSERT INTO dashboard_tabs (dashboard_id, name, display_order, icon) 
         VALUES ($1, $2, $3, $4) 
         RETURNING *",
    )
    .bind(dashboard_id)
    .bind(&payload.name)
    .bind(display_order)
    .bind(&payload.icon)
    .fetch_one(db.as_ref())
    .await?;

    Ok(Json(TabResponse::from(tab)))
}

async fn update_tab(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Path((project_id, dashboard_id, tab_id)): Path<(Uuid, Uuid, Uuid)>,
    Json(payload): Json<UpdateTabRequest>,
) -> Result<Json<TabResponse>> {
    authorize_project(&headers, &state, project_id).await?;

    let tab = sqlx::query_as::<_, DashboardTab>(
        "UPDATE dashboard_tabs 
         SET name = COALESCE($1, name),
             display_order = COALESCE($2, display_order),
             icon = COALESCE($3, icon),
             updated_at = NOW()
         WHERE id = $4 AND dashboard_id = $5
         RETURNING *",
    )
    .bind(payload.name.as_ref())
    .bind(payload.display_order)
    .bind(payload.icon.as_ref())
    .bind(tab_id)
    .bind(dashboard_id)
    .fetch_optional(db.as_ref())
    .await?
    .ok_or_else(|| AppError::NotFound("Tab not found".to_string()))?;

    Ok(Json(TabResponse::from(tab)))
}

async fn delete_tab(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Path((project_id, dashboard_id, tab_id)): Path<(Uuid, Uuid, Uuid)>,
) -> Result<Json<serde_json::Value>> {
    authorize_project(&headers, &state, project_id).await?;

    // Delete the tab (widgets with this tab_id will have their FK set to NULL due to ON DELETE SET NULL,
    // but we defined ON DELETE CASCADE so they'll be deleted)
    sqlx::query("DELETE FROM dashboard_tabs WHERE id = $1 AND dashboard_id = $2")
        .bind(tab_id)
        .bind(dashboard_id)
        .execute(db.as_ref())
        .await?;

    Ok(Json(serde_json::json!({"success": true})))
}

async fn list_tab_widgets(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Path((project_id, dashboard_id, tab_id)): Path<(Uuid, Uuid, Uuid)>,
) -> Result<Json<Vec<WidgetResponse>>> {
    authorize_project(&headers, &state, project_id).await?;

    let _dashboard = sqlx::query_as::<_, Dashboard>(
        "SELECT * FROM dashboards WHERE id = $1 AND project_id = $2",
    )
    .bind(dashboard_id)
    .bind(project_id)
    .fetch_optional(db.as_ref())
    .await?
    .ok_or_else(|| AppError::NotFound("Dashboard not found".to_string()))?;

    let widgets = sqlx::query_as::<_, DashboardWidget>(
        "SELECT * FROM dashboard_widgets WHERE dashboard_id = $1 AND tab_id = $2 ORDER BY position_y, position_x"
    )
    .bind(dashboard_id)
    .bind(tab_id)
    .fetch_all(&*db)
    .await?;

    Ok(Json(
        widgets.into_iter().map(WidgetResponse::from).collect(),
    ))
}

// ============================================================================
// Widget CRUD Operations
// ============================================================================

async fn list_widgets(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Path((project_id, dashboard_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<Vec<WidgetResponse>>> {
    authorize_project(&headers, &state, project_id).await?;

    let widgets = sqlx::query_as::<_, DashboardWidget>(
        "SELECT * FROM dashboard_widgets WHERE dashboard_id = $1 ORDER BY position_y, position_x",
    )
    .bind(dashboard_id)
    .fetch_all(db.as_ref())
    .await?;

    Ok(Json(
        widgets.into_iter().map(WidgetResponse::from).collect(),
    ))
}

async fn get_widget(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Path((project_id, dashboard_id, widget_id)): Path<(Uuid, Uuid, Uuid)>,
) -> Result<Json<WidgetResponse>> {
    authorize_project(&headers, &state, project_id).await?;

    let widget = sqlx::query_as::<_, DashboardWidget>(
        "SELECT * FROM dashboard_widgets WHERE id = $1 AND dashboard_id = $2",
    )
    .bind(widget_id)
    .bind(dashboard_id)
    .fetch_optional(db.as_ref())
    .await?
    .ok_or_else(|| AppError::NotFound("Widget not found".to_string()))?;

    Ok(Json(WidgetResponse::from(widget)))
}

async fn create_widget(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Path((project_id, dashboard_id)): Path<(Uuid, Uuid)>,
    Json(payload): Json<CreateWidgetRequest>,
) -> Result<Json<WidgetResponse>> {
    authorize_project(&headers, &state, project_id).await?;

    let widget = sqlx::query_as::<_, DashboardWidget>(
        "INSERT INTO dashboard_widgets (dashboard_id, tab_id, widget_type, widget_config, position_x, position_y, width, height, title) 
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) 
         RETURNING *"
    )
    .bind(dashboard_id)
    .bind(payload.tab_id)
    .bind(&payload.widget_type)
    .bind(&payload.widget_config)
    .bind(payload.position_x)
    .bind(payload.position_y)
    .bind(payload.width)
    .bind(payload.height)
    .bind(&payload.title)
    .fetch_one(db.as_ref())
    .await?;

    Ok(Json(WidgetResponse::from(widget)))
}

async fn update_widget(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Path((project_id, dashboard_id, widget_id)): Path<(Uuid, Uuid, Uuid)>,
    Json(payload): Json<UpdateWidgetRequest>,
) -> Result<Json<WidgetResponse>> {
    authorize_project(&headers, &state, project_id).await?;

    let widget = sqlx::query_as::<_, DashboardWidget>(
        "UPDATE dashboard_widgets 
         SET tab_id = COALESCE($1, tab_id),
             widget_type = COALESCE($2, widget_type),
             widget_config = COALESCE($3, widget_config),
             position_x = COALESCE($4, position_x),
             position_y = COALESCE($5, position_y),
             width = COALESCE($6, width),
             height = COALESCE($7, height),
             title = COALESCE($8, title),
             updated_at = NOW()
         WHERE id = $9 AND dashboard_id = $10
         RETURNING *",
    )
    .bind(payload.tab_id)
    .bind(payload.widget_type.as_ref())
    .bind(payload.widget_config.as_ref())
    .bind(payload.position_x)
    .bind(payload.position_y)
    .bind(payload.width)
    .bind(payload.height)
    .bind(payload.title.as_ref())
    .bind(widget_id)
    .bind(dashboard_id)
    .fetch_optional(db.as_ref())
    .await?
    .ok_or_else(|| AppError::NotFound("Widget not found".to_string()))?;

    Ok(Json(WidgetResponse::from(widget)))
}

async fn delete_widget(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Path((project_id, dashboard_id, widget_id)): Path<(Uuid, Uuid, Uuid)>,
) -> Result<Json<serde_json::Value>> {
    authorize_project(&headers, &state, project_id).await?;

    sqlx::query("DELETE FROM dashboard_widgets WHERE id = $1 AND dashboard_id = $2")
        .bind(widget_id)
        .bind(dashboard_id)
        .execute(db.as_ref())
        .await?;

    Ok(Json(serde_json::json!({"success": true})))
}

// ============================================================================
// Datadog Import
// ============================================================================

#[derive(Debug, Serialize)]
struct ImportDashboardResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    dashboard: Option<DashboardResponse>,
    warnings: Vec<String>,
    converted_count: usize,
    skipped_count: usize,
}

#[derive(Debug, Deserialize)]
struct ImportQuery {
    #[serde(default)]
    dry_run: Option<bool>,
}

async fn import_datadog_dashboard(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Path(project_id): Path<Uuid>,
    axum::extract::Query(query): axum::extract::Query<ImportQuery>,
    Json(dd_dashboard): Json<super::migration::DatadogDashboard>,
) -> Result<Json<ImportDashboardResponse>> {
    let user_id = authorize_project(&headers, &state, project_id)
        .await?
        .ok_or_else(|| {
            AppError::BadRequest("Could not determine user identity. API key callers must include X-User-Id header.".into())
        })?;

    let import_source = serde_json::json!({
        "type": "datadog",
        "payload": &dd_dashboard,
    });

    let result = super::migration::convert_dashboard(dd_dashboard);
    let is_dry_run = query.dry_run.unwrap_or(false);

    if is_dry_run {
        return Ok(Json(ImportDashboardResponse {
            dashboard: None,
            warnings: result.warnings,
            converted_count: result.converted_count,
            skipped_count: result.skipped_count,
        }));
    }

    let now = Utc::now();
    let layout_config = serde_json::json!({ "variables": result.variables });

    let dashboard = sqlx::query_as::<_, Dashboard>(
        r#"INSERT INTO dashboards (project_id, user_id, name, description, layout_config, import_source, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $7)
        RETURNING *"#
    )
    .bind(project_id)
    .bind(user_id)
    .bind(&result.name)
    .bind(&result.description)
    .bind(&layout_config)
    .bind(&import_source)
    .bind(now)
    .fetch_one(db.as_ref())
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to create dashboard: {}", e)))?;

    for (tab_index, tab) in result.tabs.iter().enumerate() {
        let created_tab = sqlx::query_as::<_, DashboardTab>(
            r#"INSERT INTO dashboard_tabs (dashboard_id, name, display_order, icon, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $5)
            RETURNING *"#
        )
        .bind(dashboard.id)
        .bind(&tab.name)
        .bind(tab_index as i32)
        .bind(tab.icon.as_deref())
        .bind(now)
        .fetch_one(db.as_ref())
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to create tab: {}", e)))?;

        for widget in &tab.widgets {
            sqlx::query(
                r#"INSERT INTO dashboard_widgets (dashboard_id, tab_id, widget_type, widget_config, position_x, position_y, width, height, title, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)"#
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
            .execute(db.as_ref())
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to create widget: {}", e)))?;
        }
    }

    Ok(Json(ImportDashboardResponse {
        dashboard: Some(DashboardResponse::from(dashboard)),
        warnings: result.warnings,
        converted_count: result.converted_count,
        skipped_count: result.skipped_count,
    }))
}

// ============================================================================
// Grafana Import
// ============================================================================

async fn import_grafana_dashboard(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Path(project_id): Path<Uuid>,
    axum::extract::Query(query): axum::extract::Query<ImportQuery>,
    Json(grafana_export): Json<super::grafana::GrafanaDashboardExport>,
) -> Result<Json<ImportDashboardResponse>> {
    let user_id = authorize_project(&headers, &state, project_id)
        .await?
        .ok_or_else(|| {
            AppError::BadRequest("Could not determine user identity. API key callers must include X-User-Id header.".into())
        })?;

    let import_source = serde_json::json!({
        "type": "grafana",
        "payload": &grafana_export,
    });

    let result = super::grafana::convert_dashboard(grafana_export);
    let is_dry_run = query.dry_run.unwrap_or(false);

    if is_dry_run {
        return Ok(Json(ImportDashboardResponse {
            dashboard: None,
            warnings: result.warnings,
            converted_count: result.converted_count,
            skipped_count: result.skipped_count,
        }));
    }

    let now = Utc::now();
    let layout_config = serde_json::json!({ "variables": result.variables });

    let dashboard = sqlx::query_as::<_, Dashboard>(
        r#"INSERT INTO dashboards (project_id, user_id, name, description, layout_config, import_source, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $7)
        RETURNING *"#
    )
    .bind(project_id)
    .bind(user_id)
    .bind(&result.name)
    .bind(&result.description)
    .bind(&layout_config)
    .bind(&import_source)
    .bind(now)
    .fetch_one(db.as_ref())
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to create dashboard: {}", e)))?;

    for (tab_index, tab) in result.tabs.iter().enumerate() {
        let created_tab = sqlx::query_as::<_, DashboardTab>(
            r#"INSERT INTO dashboard_tabs (dashboard_id, name, display_order, icon, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $5)
            RETURNING *"#
        )
        .bind(dashboard.id)
        .bind(&tab.name)
        .bind(tab_index as i32)
        .bind(tab.icon.as_deref())
        .bind(now)
        .fetch_one(db.as_ref())
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to create tab: {}", e)))?;

        for widget in &tab.widgets {
            sqlx::query(
                r#"INSERT INTO dashboard_widgets (dashboard_id, tab_id, widget_type, widget_config, position_x, position_y, width, height, title, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)"#
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
            .execute(db.as_ref())
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to create widget: {}", e)))?;
        }
    }

    let organization_id =
        sqlx::query_scalar::<_, Uuid>("SELECT organization_id FROM projects WHERE id = $1")
            .bind(project_id)
            .fetch_optional(db.as_ref())
            .await
            .ok()
            .flatten();

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::DashboardCreated)
        .actor(user_id)
        .resource("dashboard", dashboard.id)
        .details(serde_json::json!({
            "created": {
                "name": &result.name,
                "project_id": project_id,
                "source": "grafana_import",
                "converted_count": result.converted_count,
                "skipped_count": result.skipped_count,
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
        .success();
    if let Some(org_id) = organization_id {
        audit = audit.organization(org_id);
    }
    audit.log(&state.clickhouse).await;

    Ok(Json(ImportDashboardResponse {
        dashboard: Some(DashboardResponse::from(dashboard)),
        warnings: result.warnings,
        converted_count: result.converted_count,
        skipped_count: result.skipped_count,
    }))
}
