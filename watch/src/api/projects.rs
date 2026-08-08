use anyhow::anyhow;
use axum::body::Body;
use axum::http::{header, StatusCode};
use axum::{
    extract::{Extension, Path, Query, State},
    response::{Json, Response},
    routing::{delete, get, patch, post},
    Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, sync::Arc};
use tracing::Instrument;
use uuid::Uuid;

use reiver_core::audit::{AuditCaller, AuditEventBuilder, AuditEventType, AuditOrigin};

use crate::app_state::WatchState;
use crate::db::DbPool;
use crate::error::{AppError, Result};
use crate::models::{ExceptionGroup, ExceptionGroupDetail, Project, ProjectStats};
use crate::query_cache::{get_cached_query, set_cached_query, CacheTTL};
use crate::utils::escape_clickhouse_string;
use crate::worker::get_stats_from_redis;
use axum::http::HeaderMap;
use bb8_redis::redis::AsyncCommands;

#[derive(Debug, Deserialize)]
struct CreateProjectRequest {
    name: String,
}

#[derive(Debug, Deserialize)]
struct UpdateProjectRequest {
    name: Option<String>,
    pii_masking_enabled: Option<bool>,
    span_metrics_enabled: Option<bool>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
struct ProjectKeyResponse {
    id: Uuid,
    key_prefix: Option<String>,
    created_at: chrono::DateTime<Utc>,
}

pub fn create_projects_router(config: &crate::config::Config) -> Router<Arc<WatchState>> {
    Router::new()
        .route("/", post(create_project))
        .route("/", get(list_projects))
        .route("/{id}", get(get_project))
        .route("/{id}", patch(update_project))
        .route("/{id}/change-tracking", get(get_change_tracking))
        .route("/{id}/feature-flags", get(list_feature_flags))
        .route(
            "/{id}/feature-flags/{flag_id}/changes",
            get(get_flag_changes),
        )
        .route("/{id}/feature-flags/{flag_id}/usage", get(get_flag_usage))
        .route("/{id}", delete(delete_project))
        .route("/{id}/keys", post(create_project_key))
        .route("/{id}/keys", get(list_project_keys))
        .route("/{id}/exceptions", get(list_exception_groups))
        .route("/{id}/exceptions/filter-values", get(get_otlp_attributes))
        .route("/{id}/exceptions/{group_id}", get(get_exception_group))
        .route(
            "/{id}/exceptions/{group_id}",
            patch(update_exception_status),
        )
        .route(
            "/{id}/exceptions/{group_id}/navigate",
            get(get_exception_navigation),
        )
        .route(
            "/{id}/exceptions/{group_id}/history",
            get(get_exception_group_history),
        )
        .route("/{id}/traces", get(list_traces))
        .route("/{id}/traces/filter-values", get(get_otlp_attributes))
        .route("/{id}/traces/attribute-keys", get(list_span_attribute_keys))
        .route(
            "/{id}/traces/attribute-values",
            get(list_span_attribute_values),
        )
        .route("/{id}/traces/{trace_id}", get(get_trace))
        .route("/{id}/services", get(list_services))
        .route("/{id}/services/{service}", get(get_service_detail))
        .route("/{id}/topology", get(get_topology))
        .route(
            "/{id}/services/{service}/versions",
            get(list_service_versions),
        )
        .route(
            "/{id}/services/{service}/versions/compare",
            get(compare_versions),
        )
        .route(
            "/{id}/services/{service}/metrics/version-scoped",
            get(get_version_scoped_metrics),
        )
        .route(
            "/{id}/services/{service}/metrics/time-between-deployments",
            get(get_time_between_deployments),
        )
        .route(
            "/{id}/services/{service}/deployments/faulty-detection",
            get(detect_faulty_deployments),
        )
        .route("/{id}/stats", get(get_project_stats))
        .route("/{id}/stats/stream", get(stream_project_stats))
        .route(
            "/{id}/root-cause-suggestions",
            get(get_root_cause_suggestions),
        )
        .route(
            "/{id}/incidents/exceptions",
            get(crate::api::incidents::list_incident_exceptions),
        )
        .route(
            "/{id}/incidents/context",
            get(crate::api::incidents::get_incident_context),
        )
        .route("/{id}/events", get(list_unified_events))
        .route("/{id}/events/filter-values", get(get_otlp_attributes))
        .route("/{id}/events/attribute-keys", get(list_log_attribute_keys))
        .route(
            "/{id}/events/attribute-values",
            get(list_log_attribute_values),
        )
        .route("/{id}/logs/{log_id}", get(get_log_detail))
        .route("/{id}/logs/context", get(get_log_context))
        .route("/{id}/metrics/names", get(list_project_metric_names))
        .route(
            "/{id}/metrics/{metric_name}/timeseries",
            get(get_metric_timeseries),
        )
        .route(
            "/{id}/metrics/{metric_name}/labels",
            get(get_project_metric_labels),
        )
        .route("/{id}/api-endpoints", get(list_api_endpoints))
        .route("/{id}/api-endpoints/errors", get(list_api_endpoint_errors))
        .route(
            "/{id}/api-endpoints/summary",
            get(get_api_endpoints_summary),
        )
        // Infrastructure monitoring
        .nest("/{id}/infra", super::infra::create_infra_router())
        // MCP / Agent tool analytics
        .nest("/{id}/mcp", super::mcp_tools::create_mcp_tools_router())
        // GitHub integration routes (merged from github module)
        .merge(super::github::create_project_github_router(config))
}

async fn create_project(
    State(state): State<Arc<WatchState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Json(payload): Json<CreateProjectRequest>,
) -> Result<Json<Project>> {
    let user_id = crate::api::extract_user_id(&headers)?;

    // Get or create a default organization for the user
    // First, try to get an existing membership
    let organization_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT organization_id FROM memberships WHERE user_id = $1 AND status = 'active' LIMIT 1",
    )
    .bind(user_id)
    .fetch_optional(&*db)
    .await?;

    let organization_id = if let Some(org_id) = organization_id {
        org_id
    } else {
        let provision =
            reiver_core::org_provision::default_org_provision_for_user(&db, user_id).await?;

        let free_tier_id: Uuid = sqlx::query_scalar(
            "SELECT id FROM tier_definitions WHERE name = 'free'",
        )
        .fetch_one(&*db)
        .await?;

        let org_id: Uuid = if let Some(ref domain) = provision.domain {
            match sqlx::query_scalar::<_, Uuid>(
                "INSERT INTO organizations (name, domain, tier_definition_id) VALUES ($1, $2, $3) RETURNING id",
            )
            .bind(&provision.suggested_name)
            .bind(domain)
            .bind(free_tier_id)
            .fetch_one(&*db)
            .await
            {
                Ok(id) => id,
                Err(_) => {
                    let fb = provision.fallback_without_company_domain();
                    sqlx::query_scalar("INSERT INTO organizations (name, tier_definition_id) VALUES ($1, $2) RETURNING id")
                        .bind(&fb.suggested_name)
                        .bind(free_tier_id)
                        .fetch_one(&*db)
                        .await?
                }
            }
        } else {
            sqlx::query_scalar("INSERT INTO organizations (name, tier_definition_id) VALUES ($1, $2) RETURNING id")
                .bind(&provision.suggested_name)
                .bind(free_tier_id)
                .fetch_one(&*db)
                .await?
        };

        sqlx::query(
            "INSERT INTO memberships (user_id, organization_id, role, status) VALUES ($1, $2, 'owner', 'active')"
        )
        .bind(user_id)
        .bind(org_id)
        .execute(&*db)
        .await?;

        org_id
    };

    let project = sqlx::query_as::<_, Project>(
        "INSERT INTO projects (name, organization_id, created_by) VALUES ($1, $2, $3) RETURNING *",
    )
    .bind(&payload.name)
    .bind(organization_id)
    .bind(user_id)
    .fetch_one(&*db)
    .await?;

    // Create default project key
    let key = generate_api_key();
    let key_hash = crate::utils::hash_api_key(&key);
    let key_encrypted = state
        .encryptor
        .encrypt(&key)
        .map_err(|e| anyhow::anyhow!("Encryption error: {}", e))?;
    let key_prefix = key
        .chars()
        .rev()
        .take(4)
        .collect::<String>()
        .chars()
        .rev()
        .collect::<String>();
    sqlx::query(
        "INSERT INTO project_keys (project_id, key, key_hash, key_prefix) VALUES ($1, $2, $3, $4)",
    )
    .bind(&project.id)
    .bind(&key_encrypted)
    .bind(&key_hash)
    .bind(&key_prefix)
    .execute(&*db)
    .await?;

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::ProjectCreated)
        .actor(user_id)
        .organization(organization_id)
        .resource("project", project.id)
        .details(serde_json::json!({ "created": { "name": &payload.name } }))
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

    Ok(Json(project))
}

async fn list_projects(
    State(state): State<Arc<WatchState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
) -> Result<Json<Vec<Project>>> {
    let user_id = crate::api::extract_user_id(&headers)?;

    let projects = sqlx::query_as::<_, Project>(
        r#"
        SELECT DISTINCT p.* FROM projects p
        INNER JOIN memberships m ON p.organization_id = m.organization_id
        WHERE m.user_id = $1 AND m.status = 'active'
        ORDER BY p.created_at DESC
        "#,
    )
    .bind(user_id)
    .fetch_all(&*db)
    .await?;

    Ok(Json(projects))
}

async fn get_project(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
) -> Result<Json<Project>> {
    let project = sqlx::query_as::<_, Project>("SELECT * FROM projects WHERE id = $1")
        .bind(project_id)
        .fetch_optional(&*state.db)
        .await?
        .ok_or_else(|| AppError::NotFound("Project not found".to_string()))?;

    Ok(Json(project))
}

async fn update_project(
    State(state): State<Arc<WatchState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Path(project_id): Path<Uuid>,
    Json(payload): Json<UpdateProjectRequest>,
) -> Result<Json<Project>> {
    if payload.name.is_none() && payload.pii_masking_enabled.is_none() && payload.span_metrics_enabled.is_none() {
        return Err(AppError::Validation("No fields to update".to_string()));
    }
    if let Some(ref name) = payload.name {
        if name.trim().is_empty() {
            return Err(AppError::Validation(
                "Project name cannot be empty".to_string(),
            ));
        }
    }

    let mut settings_updates = Vec::new();
    if let Some(v) = payload.pii_masking_enabled {
        settings_updates.push(format!("'pii_masking_enabled', '{}'::jsonb", v));
    }
    if let Some(v) = payload.span_metrics_enabled {
        settings_updates.push(format!("'span_metrics_enabled', '{}'::jsonb", v));
    }

    let settings_sql = if settings_updates.is_empty() {
        "COALESCE(settings, '{}')".to_string()
    } else {
        format!(
            "coalesce(settings, '{{}}') || jsonb_build_object({})",
            settings_updates.join(", ")
        )
    };

    let sql = format!(
        "UPDATE projects SET name = COALESCE($1, name), settings = {} WHERE id = $2 RETURNING *",
        settings_sql
    );

    let updated_project = sqlx::query_as::<_, Project>(&sql)
        .bind(payload.name.as_deref().map(|s| s.trim()))
        .bind(project_id)
        .fetch_one(&*db)
        .await?;

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::ProjectUpdated)
        .organization(updated_project.organization_id)
        .resource("project", project_id)
        .details(serde_json::json!({
            "before": { "name": payload.name.as_ref().map(|_| &updated_project.name) },
            "after": { "name": &updated_project.name, "pii_masking_enabled": payload.pii_masking_enabled, "span_metrics_enabled": payload.span_metrics_enabled }
        }))
        .origin(&audit_origin.origin_type, &audit_origin.origin_ref, &audit_origin.origin_reason)
        .caller(&audit_caller.caller_type, &audit_caller.key_label, &audit_caller.key_prefix)
        .success()
        .log(&state.clickhouse)
        .await;

    Ok(Json(updated_project))
}

async fn delete_project(
    State(state): State<Arc<WatchState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Path(project_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>> {
    let user_id = crate::api::extract_user_id(&headers)?;

    let deleted_row = sqlx::query_as::<_, Project>("SELECT * FROM projects WHERE id = $1")
        .bind(project_id)
        .fetch_optional(&*db)
        .await
        .ok()
        .flatten();

    let result = sqlx::query(
        r#"DELETE FROM projects p
        USING memberships m
        WHERE p.id = $1 AND p.organization_id = m.organization_id AND m.user_id = $2 AND m.status = 'active'"#
    )
    .bind(project_id)
    .bind(user_id)
    .execute(&*db)
    .await?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound("Project not found".to_string()));
    }

    let organization_id = deleted_row.as_ref().map(|r| r.organization_id);

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::ProjectDeleted)
        .actor(user_id)
        .resource("project", project_id)
        .details(
            serde_json::json!({ "deleted": { "name": deleted_row.as_ref().map(|r| &r.name) } }),
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

    Ok(Json(serde_json::json!({"message": "Project deleted"})))
}

async fn create_project_key(
    State(state): State<Arc<WatchState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Path(project_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>> {
    let key = generate_api_key();
    let key_hash = crate::utils::hash_api_key(&key);
    let key_encrypted = state
        .encryptor
        .encrypt(&key)
        .map_err(|e| anyhow::anyhow!("Encryption error: {}", e))?;
    let key_prefix = key
        .chars()
        .rev()
        .take(4)
        .collect::<String>()
        .chars()
        .rev()
        .collect::<String>();

    #[derive(sqlx::FromRow)]
    struct InsertedKey {
        id: Uuid,
        created_at: chrono::DateTime<Utc>,
    }

    let inserted = sqlx::query_as::<_, InsertedKey>(
        "INSERT INTO project_keys (project_id, key, key_hash, key_prefix) 
         VALUES ($1, $2, $3, $4) 
         RETURNING id, created_at",
    )
    .bind(project_id)
    .bind(&key_encrypted)
    .bind(&key_hash)
    .bind(&key_prefix)
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
    let mut audit = AuditEventBuilder::new(AuditEventType::ApiKeyCreated)
        .resource("project", project_id)
        .details(serde_json::json!({ "created": { "project_id": project_id } }))
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

    Ok(Json(serde_json::json!({
        "id": inserted.id,
        "key": key,
        "key_prefix": key_prefix,
        "created_at": inserted.created_at,
    })))
}

async fn list_project_keys(
    State(state): State<Arc<WatchState>>,
    Extension(db): Extension<Arc<DbPool>>,
    Path(project_id): Path<Uuid>,
) -> Result<Json<Vec<ProjectKeyResponse>>> {
    let keys = sqlx::query_as::<_, ProjectKeyResponse>(
        "SELECT id, key_prefix, created_at 
         FROM project_keys 
         WHERE project_id = $1 
         ORDER BY created_at DESC",
    )
    .bind(project_id)
    .fetch_all(&*db)
    .await?;

    Ok(Json(keys))
}

async fn list_exception_groups(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Vec<ExceptionGroup>>> {
    let handler_start = std::time::Instant::now();

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct ExceptionGroupRow {
        id: String,
        project_id: String,
        fingerprint: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        first_seen: chrono::DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        last_seen: chrono::DateTime<Utc>,
        count: u64,
        status: String,
        level: String,
        message: String,
        exception_type: Option<String>,
        exception_value: Option<String>,
        service_name: Option<String>,
        // Deployment & environment context
        environment: Option<String>,
        version: Option<String>,
        deployment_id: Option<String>,
        region: Option<String>,
        host_name: Option<String>,
        runtime: Option<String>,
        // Kubernetes / container context
        pod_name: Option<String>,
        cluster_name: Option<String>,
        container_id: Option<String>,
        // HTTP context
        http_method: Option<String>,
        http_url: Option<String>,
        // User context
        user_id: Option<String>,
    }

    // Get sorting parameters from query string
    let sort_by = params.get("sort_by").map(|s| s.as_str()).unwrap_or("count");
    let sort_order = params
        .get("sort_order")
        .map(|s| s.as_str())
        .unwrap_or("desc");

    // Get filter parameters (support comma-separated values)
    let filter_statuses: Vec<String> = params
        .get("status")
        .map(|s| {
            s.split(',')
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let search_query = params
        .get("search")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    // Context filters for deployment tracking (support both single value and comma-separated multi-value)
    let filter_environments: Vec<String> = params
        .get("environments")
        .or_else(|| params.get("environment"))
        .or_else(|| params.get("env"))
        .map(|s| {
            s.split(',')
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let filter_versions: Vec<String> = params
        .get("versions")
        .or_else(|| params.get("version"))
        .map(|s| {
            s.split(',')
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let filter_regions: Vec<String> = params
        .get("regions")
        .or_else(|| params.get("region"))
        .map(|s| {
            s.split(',')
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let filter_host_names: Vec<String> = params
        .get("host_names")
        .or_else(|| params.get("host_name"))
        .map(|s| {
            s.split(',')
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let filter_pod_names: Vec<String> = params
        .get("pod_names")
        .or_else(|| params.get("pod_name"))
        .map(|s| {
            s.split(',')
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let filter_service_names: Vec<String> = params
        .get("service_names")
        .or_else(|| params.get("service"))
        .map(|s| {
            s.split(',')
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .collect()
        })
        .unwrap_or_default();

    let time_range = params.get("time_range").map(|s| s.as_str());
    let hours: u32 = match time_range {
        Some("1h") => 1,
        Some("6h") => 6,
        Some("24h") => 24,
        Some("3d") => 72,
        Some("7d") => 168,
        Some("30d") => 720,
        _ => 24,
    };

    let where_clause = format!(
        "e.project_id = ? AND e.timestamp >= now() - INTERVAL {} HOUR",
        hours
    );

    // Build HAVING clause for aggregate filters
    let mut having_clauses = Vec::new();

    if !filter_statuses.is_empty() {
        let status_list = filter_statuses
            .iter()
            .map(|s| format!("'{}'", escape_clickhouse_string(s))) // Escape for SQL safety
            .collect::<Vec<_>>()
            .join(",");
        having_clauses.push(format!(
            "coalesce(argMax(e.status, e.timestamp), 'unresolved') IN ({})",
            status_list
        ));
    }

    // Add search filter - searches in message and exception_type
    if let Some(search) = &search_query {
        let escaped_search = escape_clickhouse_string(search);
        having_clauses.push(format!(
            "(positionCaseInsensitive(argMax(e.message, e.timestamp), '{}') > 0 OR positionCaseInsensitive(argMax(e.exception_type, e.timestamp), '{}') > 0)",
            escaped_search, escaped_search
        ));
    }

    // Add context filters (multi-value with IN clause for exact match)
    // These filters now use spans table data via trace correlation (matching SELECT expressions)
    if !filter_environments.is_empty() {
        let env_list = filter_environments
            .iter()
            .map(|s| format!("'{}'", escape_clickhouse_string(s)))
            .collect::<Vec<_>>()
            .join(",");
        having_clauses.push(format!(
            "anyLast(s.span_attributes['deployment.environment']) IN ({})",
            env_list
        ));
    }
    if !filter_versions.is_empty() {
        let version_list = filter_versions
            .iter()
            .map(|s| format!("'{}'", escape_clickhouse_string(s)))
            .collect::<Vec<_>>()
            .join(",");
        having_clauses.push(format!(
            "anyLast(s.span_attributes['service.version']) IN ({})",
            version_list
        ));
    }
    if !filter_regions.is_empty() {
        let region_list = filter_regions
            .iter()
            .map(|s| format!("'{}'", escape_clickhouse_string(s)))
            .collect::<Vec<_>>()
            .join(",");
        having_clauses.push(format!(
            "anyLast(s.span_attributes['cloud.region']) IN ({})",
            region_list
        ));
    }
    if !filter_host_names.is_empty() {
        let host_list = filter_host_names
            .iter()
            .map(|s| format!("'{}'", escape_clickhouse_string(s)))
            .collect::<Vec<_>>()
            .join(",");
        having_clauses.push(format!(
            "anyLast(s.span_attributes['host.name']) IN ({})",
            host_list
        ));
    }
    if !filter_pod_names.is_empty() {
        let pod_list = filter_pod_names
            .iter()
            .map(|s| format!("'{}'", escape_clickhouse_string(s)))
            .collect::<Vec<_>>()
            .join(",");
        having_clauses.push(format!(
            "anyLast(s.span_attributes['k8s.pod.name']) IN ({})",
            pod_list
        ));
    }
    if !filter_service_names.is_empty() {
        let service_list = filter_service_names
            .iter()
            .map(|s| format!("'{}'", escape_clickhouse_string(s)))
            .collect::<Vec<_>>()
            .join(",");
        having_clauses.push(format!("anyLast(s.service_name) IN ({})", service_list));
    }

    let having_clause = if having_clauses.is_empty() {
        String::new()
    } else {
        format!("HAVING {}", having_clauses.join(" AND "))
    };

    // Build ORDER BY clause
    let order_by = match sort_by {
        "count" => {
            if sort_order == "asc" {
                "ORDER BY count ASC, max(e.timestamp) DESC"
            } else {
                "ORDER BY count DESC, max(e.timestamp) DESC"
            }
        }
        "last_seen" => {
            if sort_order == "asc" {
                "ORDER BY max(e.timestamp) ASC"
            } else {
                "ORDER BY max(e.timestamp) DESC"
            }
        }
        "message" => {
            if sort_order == "asc" {
                "ORDER BY argMax(e.message, e.timestamp) ASC"
            } else {
                "ORDER BY argMax(e.message, e.timestamp) DESC"
            }
        }
        "status" => {
            if sort_order == "asc" {
                "ORDER BY coalesce(argMax(e.status, e.timestamp), 'unresolved') ASC"
            } else {
                "ORDER BY coalesce(argMax(e.status, e.timestamp), 'unresolved') DESC"
            }
        }
        _ => "ORDER BY count DESC, max(e.timestamp) DESC", // Default
    };

    // Get pagination parameters
    let limit = params
        .get("limit")
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(50)
        .min(200); // Max 200 errors per page
    let offset = params
        .get("offset")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    // Aggregate error groups by fingerprint from exceptions table
    // Query exceptions directly and aggregate at query time
    // Join with spans table to get service_name and other attributes via trace correlation
    // Extract attributes from span tags JSON (OpenTelemetry semantic conventions)
    let query = format!(
        "SELECT argMax(e.id, e.timestamp) as id, e.project_id as project_id, e.fingerprint as fingerprint, \
         min(e.timestamp) as first_seen, max(e.timestamp) as last_seen, count() as count, \
         coalesce(argMax(e.status, e.timestamp), 'unresolved') as status, argMax(e.level, e.timestamp) as level, argMax(e.message, e.timestamp) as message, \
         nullIf(argMax(e.exception_type, e.timestamp), '') as exception_type, nullIf(argMax(e.exception_value, e.timestamp), '') as exception_value, \
         nullIf(toString(coalesce(anyLast(s.service_name), '')), '') as service_name, \
         nullIf(toString(coalesce(anyLast(s.span_attributes['deployment.environment']), '')), '') as environment, \
         nullIf(toString(coalesce(anyLast(s.span_attributes['service.version']), '')), '') as version, \
         nullIf(toString(coalesce(anyLast(s.span_attributes['deployment.id']), '')), '') as deployment_id, \
         nullIf(toString(coalesce(anyLast(s.span_attributes['cloud.region']), '')), '') as region, \
         nullIf(toString(coalesce(anyLast(s.span_attributes['host.name']), '')), '') as host_name, \
         nullIf(toString(coalesce(anyLast(s.span_attributes['process.runtime.description']), '')), '') as runtime, \
         nullIf(toString(coalesce(anyLast(s.span_attributes['k8s.pod.name']), '')), '') as pod_name, \
         nullIf(toString(coalesce(anyLast(s.span_attributes['k8s.cluster.name']), '')), '') as cluster_name, \
         nullIf(toString(coalesce(anyLast(s.span_attributes['container.id']), '')), '') as container_id, \
         nullIf(toString(coalesce(anyLast(s.span_attributes['http.method']), '')), '') as http_method, \
         nullIf(toString(coalesce(anyLast(s.span_attributes['http.url']), '')), '') as http_url, \
         cast(NULL as Nullable(String)) as user_id \
         FROM reiver.exceptions e \
         LEFT JOIN reiver.spans s ON e.project_id = s.project_id AND e.trace_id = s.trace_id \
           AND s.timestamp >= now() - INTERVAL {hours} HOUR \
         WHERE {where_clause} GROUP BY e.project_id, e.fingerprint {having_clause} {order_by} LIMIT ? OFFSET ?",
        hours = hours, where_clause = where_clause, having_clause = having_clause, order_by = order_by
    );

    let groups: Vec<ExceptionGroupRow> = state
        .clickhouse
        .as_ref()
        .query(&query)
        .bind(project_id.to_string())
        .bind(limit as u64)
        .bind(offset)
        .fetch_all()
        .instrument(tracing::info_span!("clickhouse_query", table = "exceptions", otel.name = "CH exceptions+spans JOIN"))
        .await
        .map_err(|e| {
            tracing::error!("ClickHouse query failed: {}", e);
            AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e))
        })?;
    tracing::info!(rows = groups.len(), "exception groups fetched");

    // Convert to ExceptionGroup model
    let exception_groups: Vec<ExceptionGroup> = groups
        .into_iter()
        .map(|row| ExceptionGroup {
            id: uuid::Uuid::parse_str(&row.id).unwrap_or_default(),
            project_id: uuid::Uuid::parse_str(&row.project_id).unwrap_or_default(),
            fingerprint: row.fingerprint,
            first_seen: row.first_seen,
            last_seen: row.last_seen,
            count: row.count as i64,
            status: row.status,
            level: row.level,
            message: row.message,
            exception_type: row.exception_type,
            exception_value: row.exception_value,
            service_name: row.service_name,
            environment: row.environment,
            version: row.version,
            deployment_id: row.deployment_id,
            region: row.region,
            host_name: row.host_name,
            runtime: row.runtime,
            pod_name: row.pod_name,
            cluster_name: row.cluster_name,
            container_id: row.container_id,
            http_method: row.http_method,
            http_url: row.http_url,
            user_id: row.user_id,
        })
        .collect();

    tracing::info!(rows = exception_groups.len(), elapsed_ms = handler_start.elapsed().as_millis() as u64, "list_exception_groups complete");

    Ok(Json(exception_groups))
}

#[derive(Debug, Serialize)]
struct ExceptionFilterValues {
    environments: Vec<String>,
    versions: Vec<String>,
    regions: Vec<String>,
    host_names: Vec<String>,
    pod_names: Vec<String>,
    service_names: Vec<String>,
}

async fn get_otlp_attributes(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
) -> Result<Json<ExceptionFilterValues>> {
    // Query the dedicated filter values table (fast lookup)
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct FilterValueRow {
        attribute_type: String,
        attribute_value: String,
    }

    // Simple query on the lookup table - FINAL ensures deduplication
    let query = r#"
        SELECT attribute_type, attribute_value
        FROM reiver.otlp_attributes FINAL
        WHERE project_id = ?
        ORDER BY attribute_type, attribute_value
    "#;

    let rows: Vec<FilterValueRow> = state
        .clickhouse
        .as_ref()
        .query(query)
        .bind(project_id.to_string())
        .fetch_all()
        .await
        .map_err(|e| {
            tracing::error!("Failed to fetch filter values: {}", e);
            AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e))
        })?;

    // Group values by attribute type
    let mut environments = Vec::new();
    let mut versions = Vec::new();
    let mut regions = Vec::new();
    let mut host_names = Vec::new();
    let mut pod_names = Vec::new();
    let mut service_names = Vec::new();

    for row in rows {
        match row.attribute_type.as_str() {
            "environment" => environments.push(row.attribute_value),
            "version" => versions.push(row.attribute_value),
            "region" => regions.push(row.attribute_value),
            "host_name" => host_names.push(row.attribute_value),
            "pod_name" => pod_names.push(row.attribute_value),
            "service_name" => service_names.push(row.attribute_value),
            _ => {}
        }
    }

    Ok(Json(ExceptionFilterValues {
        environments,
        versions,
        regions,
        host_names,
        pod_names,
        service_names,
    }))
}

// Keys already surfaced as fixed sidebar filters — exclude from attribute discovery.
const EXCLUDED_ATTRIBUTE_KEYS: &[&str] = &[
    "deployment.environment",
    "service.version",
    "cloud.region",
    "host.name",
    "k8s.pod.name",
    "service.name",
    "telemetry.sdk.language",
    "telemetry.sdk.name",
    "telemetry.sdk.version",
];

fn is_valid_attribute_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 256
        && key
            .chars()
            .all(|c| c.is_alphanumeric() || c == '.' || c == '_' || c == '-' || c == '/')
}

// ── Span attribute discovery ────────────────────────────────────────────────

async fn list_span_attribute_keys(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
) -> Result<Json<Vec<String>>> {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct KeyRow {
        key: String,
    }

    let query = r#"
        SELECT DISTINCT key
        FROM reiver.span_attribute_keys
        WHERE project_id = ?
        ORDER BY key
        LIMIT 200
    "#;

    let rows: Vec<KeyRow> = state
        .clickhouse
        .as_ref()
        .query(query)
        .bind(project_id.to_string())
        .fetch_all()
        .await
        .map_err(|e| AppError::Internal(anyhow!("ClickHouse query failed: {}", e)))?;

    let keys: Vec<String> = rows
        .into_iter()
        .map(|r| r.key)
        .filter(|k| !EXCLUDED_ATTRIBUTE_KEYS.contains(&k.as_str()))
        .collect();

    Ok(Json(keys))
}

async fn list_span_attribute_values(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Vec<String>>> {
    let key = params
        .get("key")
        .map(|s| s.trim().to_string())
        .filter(|s| is_valid_attribute_key(s))
        .ok_or_else(|| AppError::Validation("missing or invalid 'key' query parameter".into()))?;

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct ValRow {
        value: String,
    }

    let escaped_key = escape_clickhouse_string(&key);
    let query = format!(
        r#"SELECT DISTINCT value FROM (
            SELECT span_attributes['{k}'] AS value
            FROM reiver.spans
            WHERE project_id = ? AND span_attributes['{k}'] != ''
              AND timestamp >= now() - INTERVAL 24 HOUR
            UNION ALL
            SELECT resource_attributes['{k}'] AS value
            FROM reiver.spans
            WHERE project_id = ? AND resource_attributes['{k}'] != ''
              AND timestamp >= now() - INTERVAL 24 HOUR
        )
        ORDER BY value
        LIMIT 100"#,
        k = escaped_key,
    );

    let rows: Vec<ValRow> = state
        .clickhouse
        .as_ref()
        .query(&query)
        .bind(project_id.to_string())
        .bind(project_id.to_string())
        .fetch_all()
        .await
        .map_err(|e| AppError::Internal(anyhow!("ClickHouse query failed: {}", e)))?;

    Ok(Json(rows.into_iter().map(|r| r.value).collect()))
}

// ── Log attribute discovery ─────────────────────────────────────────────────

async fn list_log_attribute_keys(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
) -> Result<Json<Vec<String>>> {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct KeyRow {
        key: String,
    }

    let query = r#"
        SELECT DISTINCT key
        FROM reiver.log_attribute_keys
        WHERE project_id = ?
        ORDER BY key
        LIMIT 200
    "#;

    let rows: Vec<KeyRow> = state
        .clickhouse
        .as_ref()
        .query(query)
        .bind(project_id.to_string())
        .fetch_all()
        .await
        .map_err(|e| AppError::Internal(anyhow!("ClickHouse query failed: {}", e)))?;

    let keys: Vec<String> = rows
        .into_iter()
        .map(|r| r.key)
        .filter(|k| !EXCLUDED_ATTRIBUTE_KEYS.contains(&k.as_str()))
        .collect();

    Ok(Json(keys))
}

async fn list_log_attribute_values(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Vec<String>>> {
    let key = params
        .get("key")
        .map(|s| s.trim().to_string())
        .filter(|s| is_valid_attribute_key(s))
        .ok_or_else(|| AppError::Validation("missing or invalid 'key' query parameter".into()))?;

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct ValRow {
        value: String,
    }

    let escaped_key = escape_clickhouse_string(&key);
    let query = format!(
        r#"SELECT DISTINCT value FROM (
            SELECT log_attributes['{k}'] AS value
            FROM reiver.logs
            WHERE project_id = ? AND log_attributes['{k}'] != ''
              AND timestamp >= now() - INTERVAL 24 HOUR
            UNION ALL
            SELECT resource_attributes['{k}'] AS value
            FROM reiver.logs
            WHERE project_id = ? AND resource_attributes['{k}'] != ''
              AND timestamp >= now() - INTERVAL 24 HOUR
        )
        ORDER BY value
        LIMIT 100"#,
        k = escaped_key,
    );

    let rows: Vec<ValRow> = state
        .clickhouse
        .as_ref()
        .query(&query)
        .bind(project_id.to_string())
        .bind(project_id.to_string())
        .fetch_all()
        .await
        .map_err(|e| AppError::Internal(anyhow!("ClickHouse query failed: {}", e)))?;

    Ok(Json(rows.into_iter().map(|r| r.value).collect()))
}

async fn list_traces(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Vec<crate::models::Trace>>> {
    let handler_start = std::time::Instant::now();

    let limit = params
        .get("limit")
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(50)
        .min(1000);
    let offset = params
        .get("offset")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    let sort_by = params
        .get("sort_by")
        .map(|s| s.as_str())
        .unwrap_or("start_time");
    let sort_order = params
        .get("sort_order")
        .map(|s| s.as_str())
        .unwrap_or("desc");

    // Version and environment filtering (for deployment tracking)
    let filter_version = params.get("version").cloned();
    let filter_environment = params
        .get("environment")
        .or_else(|| params.get("env"))
        .cloned();
    let filter_service = params
        .get("service")
        .or_else(|| params.get("service_name"))
        .cloned();
    // HTTP server span filters (same attributes as API Monitoring /api-endpoints)
    let filter_http_method = params
        .get("http_method")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let filter_http_route = params
        .get("http_route")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    // Trace outcome: comma-separated `error` and/or `ok` (matches per-trace `status` after grouping spans).
    // Both tokens means "any" — omit outer filter.
    let mut trace_wants_error = false;
    let mut trace_wants_ok = false;
    if let Some(raw) = params.get("trace_status") {
        for part in raw.split(',') {
            match part.trim() {
                "error" => trace_wants_error = true,
                "ok" => trace_wants_ok = true,
                "" => {}
                _ => {} // ignore unknown tokens
            }
        }
    }
    let outer_trace_status_filter = match (trace_wants_error, trace_wants_ok) {
        (false, false) => String::new(),
        (true, true) => String::new(),
        (true, false) => " WHERE status = 'error' ".to_string(),
        (false, true) => " WHERE status = 'ok' ".to_string(),
    };

    // Query ClickHouse for traces (aggregate spans by trace_id)
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct TraceRow {
        trace_id: String,
        project_id: String,
        service_name: String,
        root_span_name: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        start_time: chrono::DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        end_time: chrono::DateTime<Utc>,
        duration_ns: i64, // nanoseconds in ClickHouse
        span_count: u64,
        service_count: u64,
        status: String,
    }

    // Build ORDER BY clause
    let order_by = match sort_by {
        "duration" => {
            if sort_order == "asc" {
                "ORDER BY duration_ns ASC"
            } else {
                "ORDER BY duration_ns DESC"
            }
        }
        "start_time" => {
            if sort_order == "asc" {
                "ORDER BY start_time ASC"
            } else {
                "ORDER BY start_time DESC"
            }
        }
        _ => "ORDER BY start_time DESC",
    };

    // Build WHERE clause for filters
    let mut where_clauses = vec!["project_id = ?".to_string()];
    let mut bindings: Vec<String> = vec![project_id.to_string()];

    if let Some(ref version) = filter_version {
        where_clauses.push("span_attributes['service.version'] = ?".to_string());
        bindings.push(version.clone());
    }

    if let Some(ref env) = filter_environment {
        where_clauses.push("span_attributes['deployment.environment'] = ?".to_string());
        bindings.push(env.clone());
    }

    if let Some(ref service) = filter_service {
        where_clauses.push("service_name = ?".to_string());
        bindings.push(service.clone());
    }

    if let Some(ref m) = filter_http_method {
        where_clauses.push("span_attributes['http.method'] = ?".to_string());
        bindings.push(m.clone());
    }
    if let Some(ref p) = filter_http_route {
        where_clauses.push("span_attributes['http.route'] = ?".to_string());
        bindings.push(p.clone());
    }

    let has_start_time = if let Some(start_str) = params.get("start_time") {
        if let Ok(start) = start_str.parse::<chrono::DateTime<Utc>>().or_else(|_| {
            chrono::DateTime::parse_from_rfc3339(start_str).map(|dt| dt.with_timezone(&Utc))
        }) {
            where_clauses.push("timestamp >= parseDateTime64BestEffort(?)".to_string());
            bindings.push(start.to_rfc3339());
            true
        } else {
            false
        }
    } else {
        false
    };
    if !has_start_time {
        where_clauses.push("timestamp >= now() - INTERVAL 24 HOUR".to_string());
    }
    if let Some(end_str) = params.get("end_time") {
        if let Ok(end) = end_str.parse::<chrono::DateTime<Utc>>().or_else(|_| {
            chrono::DateTime::parse_from_rfc3339(end_str).map(|dt| dt.with_timezone(&Utc))
        }) {
            where_clauses.push("timestamp <= parseDateTime64BestEffort(?)".to_string());
            bindings.push(end.to_rfc3339());
        }
    }

    // Dynamic attribute filters: params like attr.http.status_code=200,500
    for (param_key, param_val) in &params {
        if let Some(attr_key) = param_key.strip_prefix("attr.") {
            if !is_valid_attribute_key(attr_key) || param_val.trim().is_empty() {
                continue;
            }
            let escaped_key = escape_clickhouse_string(attr_key);
            let values: Vec<&str> = param_val
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect();
            if values.is_empty() {
                continue;
            }
            let placeholders: Vec<String> = values.iter().map(|_| "?".to_string()).collect();
            let in_list = placeholders.join(",");
            where_clauses.push(format!(
                "(span_attributes['{k}'] IN ({v}) OR resource_attributes['{k}'] IN ({v}))",
                k = escaped_key,
                v = in_list,
            ));
            // Bind values twice (once for span_attributes IN, once for resource_attributes IN)
            for val in &values {
                bindings.push(val.to_string());
            }
            for val in &values {
                bindings.push(val.to_string());
            }
        }
    }

    let filter_search = params
        .get("search")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    let where_clause = where_clauses.join(" AND ");

    let having_clause = if filter_search.is_some() {
        "HAVING countIf(positionCaseInsensitive(span_name, ?) > 0) > 0"
    } else {
        ""
    };

    // Use a subquery to avoid ClickHouse's nested aggregate detection issue
    // First aggregate basic fields, then calculate end_time in outer query
    // Note: timestamp = span start time, duration = span duration in nanoseconds
    let mut query_builder = state.clickhouse.as_ref().query(&format!(
        "SELECT 
            trace_id,
            project_id,
            svc_name as service_name,
            root_name as root_span_name,
            min_start as start_time,
            max_end as end_time,
            dateDiff('microsecond', min_start, max_end) * 1000 as duration_ns,
            span_count,
            service_count,
            status
        FROM (
            SELECT 
                trace_id,
                project_id,
                any(service_name) as svc_name,
                anyIf(span_name, parent_span_id = '') as root_name,
                min(timestamp) as min_start,
                max(timestamp + toIntervalNanosecond(duration)) as max_end,
                count(*) as span_count,
                uniqExact(service_name) as service_count,
                if(countIf(status_code = 'STATUS_CODE_ERROR') > 0, 'error', 'ok') as status
            FROM reiver.spans
            WHERE {}
            GROUP BY trace_id, project_id
            {}
        )
        {}
        {}
        LIMIT ? OFFSET ?",
        where_clause, having_clause, outer_trace_status_filter, order_by
    ));

    // Bind all filter parameters
    for binding in bindings {
        query_builder = query_builder.bind(binding);
    }
    if let Some(ref search) = filter_search {
        query_builder = query_builder.bind(search.clone());
    }
    query_builder = query_builder.bind(limit as u64).bind(offset);

    let traces: Vec<TraceRow> = query_builder
        .fetch_all()
        .instrument(tracing::info_span!("clickhouse_query", table = "spans", otel.name = "CH list traces"))
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;
    tracing::info!(rows = traces.len(), "traces fetched");

    let traces: Vec<crate::models::Trace> = traces
        .into_iter()
        .map(|row| crate::models::Trace {
            trace_id: row.trace_id,
            project_id: uuid::Uuid::parse_str(&row.project_id).unwrap_or_default(),
            start_time: row.start_time,
            end_time: row.end_time,
            duration_ns: row.duration_ns, // Keep as nanoseconds
            span_count: row.span_count as i64,
            service_count: row.service_count as i64,
            status: row.status,
            service_name: row.service_name,
            root_span_name: row.root_span_name,
        })
        .collect();

    tracing::info!(rows = traces.len(), elapsed_ms = handler_start.elapsed().as_millis() as u64, "list_traces complete");

    Ok(Json(traces))
}

async fn get_trace(
    State(state): State<Arc<WatchState>>,
    Path((project_id, trace_id)): Path<(Uuid, String)>,
) -> Result<Json<crate::models::TraceDetail>> {
    let handler_start = std::time::Instant::now();

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct SpanRow {
        span_id: String,
        trace_id: String,
        parent_span_id: String,
        trace_state: String,
        project_id: String,
        span_name: String,
        span_kind: String,
        service_name: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: chrono::DateTime<Utc>,
        duration_ns: i64, // nanoseconds in ClickHouse
        status_code: String,
        status_message: String,
        span_attributes: String,
        resource_attributes: String,
        events: String,
        links: String,
    }

    let spans: Vec<SpanRow> = state.clickhouse.as_ref()
        .query(
            "SELECT span_id, trace_id, parent_span_id, trace_state, project_id, span_name, span_kind, service_name, 
                    timestamp, duration AS duration_ns, toString(status_code) AS status_code, status_message, 
                    toString(span_attributes) AS span_attributes, toString(resource_attributes) AS resource_attributes, 
                    events, links
             FROM reiver.spans
             WHERE project_id = ? AND trace_id = ?
             ORDER BY timestamp ASC"
        )
        .bind(project_id.to_string())
        .bind(&trace_id)
        .fetch_all()
        .instrument(tracing::info_span!("clickhouse_query", table = "spans", otel.name = "CH get trace spans"))
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;
    tracing::info!(rows = spans.len(), "trace spans fetched");

    // Debug: Log span durations to help diagnose timing issues
    for span in &spans {
        let end_time = span.timestamp + chrono::Duration::nanoseconds(span.duration_ns);
        tracing::debug!(
            "Span: name={}, parent={}, duration_ns={}, start={:?}, end={:?}",
            span.span_name,
            if span.parent_span_id.is_empty() {
                "ROOT"
            } else {
                &span.parent_span_id[..8]
            },
            span.duration_ns,
            span.timestamp,
            end_time
        );
    }

    if spans.is_empty() {
        // Check if any spans exist for this project to help debug
        let total_spans: u64 = state
            .clickhouse
            .as_ref()
            .query("SELECT count() FROM reiver.spans WHERE project_id = ?")
            .bind(project_id.to_string())
            .fetch_one()
            .await
            .unwrap_or(0);
        tracing::warn!(
            "Trace {} not found. Total spans in project {}: {}",
            trace_id,
            project_id,
            total_spans
        );
        return Err(AppError::NotFound(format!("Trace {} not found", trace_id)));
    }

    // Calculate trace metadata
    // Note: spans is guaranteed non-empty due to the is_empty() check above
    let trace_start = spans
        .iter()
        .map(|s| s.timestamp)
        .min()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Trace has no spans")))?;
    let trace_end = spans
        .iter()
        .map(|s| s.timestamp + chrono::Duration::nanoseconds(s.duration_ns))
        .max()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Trace has no spans")))?;
    let trace_duration_ns = (trace_end - trace_start).num_nanoseconds().unwrap_or(0);
    let span_count = spans.len() as i64;
    let service_count = spans
        .iter()
        .filter(|s| !s.service_name.is_empty())
        .map(|s| &s.service_name)
        .collect::<std::collections::HashSet<_>>()
        .len() as i64;
    let status = spans
        .iter()
        .any(|s| s.status_code.contains("ERROR"))
        .then_some("error".to_string())
        .unwrap_or_else(|| "ok".to_string());

    // Pick the root span's service/span name (parent_span_id is empty), or fall back to the first span
    let root_span = spans
        .iter()
        .find(|s| s.parent_span_id.is_empty())
        .or_else(|| spans.first());
    let trace_service_name = root_span
        .map(|s| s.service_name.clone())
        .unwrap_or_default();
    let root_span_name = root_span.map(|s| s.span_name.clone()).unwrap_or_default();

    // Convert spans
    let trace_spans: Vec<crate::models::Span> = spans
        .into_iter()
        .map(|row| {
            let span_attributes: serde_json::Value = serde_json::from_str(&row.span_attributes)
                .unwrap_or_else(|_| serde_json::json!({}));
            let resource_attributes: serde_json::Value =
                serde_json::from_str(&row.resource_attributes)
                    .unwrap_or_else(|_| serde_json::json!({}));
            let events: serde_json::Value =
                serde_json::from_str(&row.events).unwrap_or_else(|_| serde_json::json!([]));
            let links: serde_json::Value =
                serde_json::from_str(&row.links).unwrap_or_else(|_| serde_json::json!([]));

            tracing::debug!(
                "Retrieved span from ClickHouse: span_name={}, status_code={}",
                row.span_name,
                row.status_code
            );

            crate::models::Span {
                span_id: row.span_id,
                trace_id: row.trace_id,
                span_name: row.span_name,
                timestamp: row.timestamp,
                duration_ns: row.duration_ns, // Keep as nanoseconds
                parent_span_id: row.parent_span_id,
                trace_state: row.trace_state,
                span_kind: row.span_kind,
                service_name: row.service_name,
                status_code: row.status_code,
                status_message: row.status_message,
                span_attributes,
                resource_attributes,
                events,
                links,
                project_id: uuid::Uuid::parse_str(&row.project_id).unwrap_or_default(),
            }
        })
        .collect();

    let trace = crate::models::Trace {
        trace_id: trace_id.clone(),
        project_id,
        start_time: trace_start,
        end_time: trace_end,
        duration_ns: trace_duration_ns,
        span_count,
        service_count,
        status,
        service_name: trace_service_name,
        root_span_name,
    };

    let phase_start = std::time::Instant::now();
    #[derive(sqlx::FromRow)]
    struct ErrorTraceRow {
        error_id: String,
        span_id: Option<String>,
    }

    let error_traces: Vec<ErrorTraceRow> = sqlx::query_as::<_, ErrorTraceRow>(
        "SELECT error_id, span_id FROM error_traces
         WHERE trace_id = $1 AND project_id = $2",
    )
    .bind(&trace_id)
    .bind(&project_id)
    .fetch_all(&*state.db)
    .instrument(tracing::info_span!("pg_query", table = "error_traces"))
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to query error_traces: {}", e)))?;

    // Query ClickHouse for related exceptions (via error_traces junction) if any
    let exceptions = if !error_traces.is_empty() {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct ExceptionRow {
            id: String,
            project_id: String,
            fingerprint: String,
            level: String,
            message: String,
            exception_type: String,  // OTel: optional, empty if not present
            exception_value: String, // OTel: optional, empty if not present
            stacktrace: String,      // OTel: optional, empty if not present
            context: String,
            tags: String,
            user_data: String,
            service_name: String, // OTel: optional, empty if not present
            #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
            timestamp: chrono::DateTime<Utc>,
            #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
            created_at: chrono::DateTime<Utc>,
        }

        let error_ids: Vec<&String> = error_traces.iter().map(|et| &et.error_id).collect();
        let placeholders: Vec<String> = (0..error_ids.len()).map(|_| "?".to_string()).collect();
        let query = format!(
            "SELECT id, project_id, fingerprint, level, message, exception_type, exception_value,
             stacktrace, context, tags, user_data, service_name, timestamp, created_at
             FROM reiver.exceptions
             WHERE id IN ({}) AND project_id = ?
             ORDER BY timestamp DESC",
            placeholders.join(", ")
        );

        let mut query_builder = state.clickhouse.as_ref().query(&query);
        for error_id in &error_ids {
            query_builder = query_builder.bind(*error_id);
        }
        query_builder = query_builder.bind(project_id.to_string());

        let rows: Vec<ExceptionRow> = query_builder
            .fetch_all()
            .instrument(tracing::info_span!("clickhouse_query", table = "exceptions", otel.name = "CH trace exceptions"))
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

        let span_id_map: std::collections::HashMap<String, Option<String>> = error_traces
            .into_iter()
            .map(|et| (et.error_id, et.span_id))
            .collect();

        rows.into_iter()
            .map(|row| {
                let span_id = span_id_map.get(&row.id).and_then(|s| s.clone());
                crate::models::ExceptionWithSpan {
                    id: uuid::Uuid::parse_str(&row.id).unwrap_or_default(),
                    project_id: uuid::Uuid::parse_str(&row.project_id).unwrap_or_default(),
                    fingerprint: row.fingerprint,
                    level: row.level,
                    message: row.message,
                    exception_type: if row.exception_type.is_empty() {
                        None
                    } else {
                        Some(row.exception_type)
                    },
                    exception_value: if row.exception_value.is_empty() {
                        None
                    } else {
                        Some(row.exception_value)
                    },
                    stacktrace: if row.stacktrace.is_empty() {
                        serde_json::json!([])
                    } else {
                        serde_json::from_str(&row.stacktrace).unwrap_or(serde_json::json!([]))
                    },
                    context: serde_json::from_str(&row.context).unwrap_or(serde_json::Value::Null),
                    tags: serde_json::from_str(&row.tags).unwrap_or(serde_json::Value::Null),
                    user_data: serde_json::from_str(&row.user_data)
                        .unwrap_or(serde_json::Value::Null),
                    timestamp: row.timestamp,
                    created_at: row.created_at,
                    span_id,
                    service_name: if row.service_name.is_empty() {
                        None
                    } else {
                        Some(row.service_name)
                    },
                }
            })
            .collect()
    } else {
        Vec::new()
    };

    tracing::info!(spans = trace_spans.len(), exceptions = exceptions.len(), elapsed_ms = handler_start.elapsed().as_millis() as u64, "get_trace complete");

    Ok(Json(crate::models::TraceDetail {
        trace,
        spans: trace_spans,
        exceptions,
    }))
}

/// Resolve a group_id (which can be a fingerprint string or exception UUID) to a fingerprint.
/// If group_id looks like a UUID, look up the fingerprint by exception id.
/// Otherwise, treat it as a fingerprint directly.
async fn resolve_group_id_to_fingerprint(
    clickhouse: &clickhouse::Client,
    group_id: &str,
    project_id: &Uuid,
) -> Result<String> {
    if Uuid::parse_str(group_id).is_ok() {
        // Looks like a UUID - find the fingerprint for this exception ID
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct FingerprintRow {
            fingerprint: String,
        }
        let fp_row: Option<FingerprintRow> = clickhouse
            .query("SELECT fingerprint FROM reiver.exceptions WHERE id = ? AND project_id = ? LIMIT 1")
            .bind(group_id.to_string())
            .bind(project_id.to_string())
            .fetch_optional()
            .await
            .map_err(|e| AppError::Internal(anyhow!("ClickHouse query failed: {}", e)))?;
        match fp_row {
            Some(row) => Ok(row.fingerprint),
            None => Err(AppError::NotFound("Exception group not found".to_string())),
        }
    } else {
        // Not a UUID - treat as fingerprint directly
        Ok(group_id.to_string())
    }
}

async fn get_exception_group(
    State(state): State<Arc<WatchState>>,
    Path((project_id, group_id)): Path<(Uuid, String)>,
) -> Result<Json<ExceptionGroupDetail>> {
    let handler_start = std::time::Instant::now();

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct ExceptionGroupRow {
        id: String,
        project_id: String,
        fingerprint: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        first_seen: chrono::DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        last_seen: chrono::DateTime<Utc>,
        count: u64,
        status: String,
        level: String,
        message: String,
        exception_type: Option<String>,
        exception_value: Option<String>,
        service_name: Option<String>,
        // Deployment & environment context
        environment: Option<String>,
        version: Option<String>,
        deployment_id: Option<String>,
        region: Option<String>,
        host_name: Option<String>,
        runtime: Option<String>,
        // Kubernetes / container context
        pod_name: Option<String>,
        cluster_name: Option<String>,
        container_id: Option<String>,
        // HTTP context
        http_method: Option<String>,
        http_url: Option<String>,
        // User context
        user_id: Option<String>,
    }

    let fingerprint =
        resolve_group_id_to_fingerprint(state.clickhouse.as_ref(), &group_id, &project_id)
            .instrument(tracing::info_span!("resolve_fingerprint"))
            .await?;

    let group_row: Option<ExceptionGroupRow> = state.clickhouse.as_ref()
        .query("SELECT argMax(e.id, e.timestamp) as id, e.project_id as project_id, e.fingerprint as fingerprint, \
                min(e.timestamp) as first_seen, max(e.timestamp) as last_seen, count() as count, \
                coalesce(argMax(e.status, e.timestamp), 'unresolved') as status, argMax(e.level, e.timestamp) as level, argMax(e.message, e.timestamp) as message, \
                nullIf(argMax(e.exception_type, e.timestamp), '') as exception_type, nullIf(argMax(e.exception_value, e.timestamp), '') as exception_value, \
                nullIf(toString(anyLast(s.service_name)), '') as service_name, \
                nullIf(toString(anyLast(s.span_attributes['deployment.environment'])), '') as environment, \
                nullIf(toString(anyLast(s.span_attributes['service.version'])), '') as version, \
                nullIf(toString(anyLast(s.span_attributes['deployment.id'])), '') as deployment_id, \
                nullIf(toString(anyLast(s.span_attributes['cloud.region'])), '') as region, \
                nullIf(toString(anyLast(s.span_attributes['host.name'])), '') as host_name, \
                nullIf(toString(anyLast(s.span_attributes['process.runtime.description'])), '') as runtime, \
                nullIf(toString(anyLast(s.span_attributes['k8s.pod.name'])), '') as pod_name, \
                nullIf(toString(anyLast(s.span_attributes['k8s.cluster.name'])), '') as cluster_name, \
                nullIf(toString(anyLast(s.span_attributes['container.id'])), '') as container_id, \
                nullIf(toString(anyLast(s.span_attributes['http.method'])), '') as http_method, \
                nullIf(toString(anyLast(s.span_attributes['http.url'])), '') as http_url, \
                cast(NULL as Nullable(String)) as user_id \
                FROM reiver.exceptions e \
                LEFT JOIN reiver.spans s ON e.project_id = s.project_id AND e.trace_id = s.trace_id \
                WHERE e.project_id = ? AND e.fingerprint = ? \
                GROUP BY e.project_id, e.fingerprint LIMIT 1")
        .bind(project_id.to_string())
        .bind(fingerprint.clone())
        .fetch_optional()
        .instrument(tracing::info_span!("clickhouse_query", table = "exceptions", otel.name = "CH exception group aggregation"))
        .await
        .map_err(|e| AppError::Internal(anyhow!("ClickHouse query failed: {}", e)))?;

    let group = if let Some(row) = group_row {
        ExceptionGroup {
            id: uuid::Uuid::parse_str(&row.id).unwrap_or_default(),
            project_id: uuid::Uuid::parse_str(&row.project_id).unwrap_or_default(),
            fingerprint: row.fingerprint.clone(),
            first_seen: row.first_seen,
            last_seen: row.last_seen,
            count: row.count as i64,
            status: row.status,
            level: row.level,
            message: row.message,
            exception_type: row.exception_type,
            exception_value: row.exception_value,
            service_name: row.service_name,
            environment: row.environment,
            version: row.version,
            deployment_id: row.deployment_id,
            region: row.region,
            host_name: row.host_name,
            runtime: row.runtime,
            pod_name: row.pod_name,
            cluster_name: row.cluster_name,
            container_id: row.container_id,
            http_method: row.http_method,
            http_url: row.http_url,
            user_id: row.user_id,
        }
    } else {
        return Err(AppError::NotFound("Exception group not found".to_string()));
    };

    // Query ClickHouse for recent exceptions
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct ExceptionRow {
        id: String,
        project_id: String,
        fingerprint: String,
        level: String,
        message: String,
        exception_type: String,
        exception_value: String,
        stacktrace: String, // OTel: optional, empty if not present
        context: String,
        tags: String,
        user_data: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: chrono::DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        created_at: chrono::DateTime<Utc>,
    }

    let recent_exceptions = match get_recent_exceptions_from_redis(
        &state.redis,
        project_id,
        &group.fingerprint,
    )
    .await
    {
        Ok(Some(exceptions)) if !exceptions.is_empty() => {
            tracing::debug!(
                "Using {} exceptions from Redis for fingerprint {}",
                exceptions.len(),
                &group.fingerprint
            );
            exceptions
        }
        Ok(Some(_)) => {
            tracing::debug!(
                "Redis returned empty list for fingerprint {}, falling back to ClickHouse",
                &group.fingerprint
            );
            Vec::new()
        }
        Ok(None) => {
            tracing::debug!(
                "No data in Redis for fingerprint {}, falling back to ClickHouse",
                &group.fingerprint
            );
            Vec::new()
        }
        Err(e) => {
            tracing::warn!("Failed to get exceptions from Redis for fingerprint {}: {}, falling back to ClickHouse", &group.fingerprint, e);
            Vec::new()
        }
    };

    // If we don't have exceptions from Redis, query ClickHouse
    let recent_exceptions = if recent_exceptions.is_empty() {
        let exception_rows: Vec<ExceptionRow> = state.clickhouse.as_ref()
                .query("SELECT id, project_id, fingerprint, level, message, exception_type, exception_value, stacktrace, context, tags, user_data, timestamp, created_at FROM reiver.exceptions WHERE project_id = ? AND fingerprint = ? ORDER BY timestamp DESC LIMIT 100")
                .bind(project_id.to_string())
                .bind(&group.fingerprint)
                .fetch_all()
                .instrument(tracing::info_span!("clickhouse_query", table = "exceptions", otel.name = "CH recent exceptions"))
                .await
                .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

        let from_ch: Vec<crate::models::Exception> = exception_rows
            .into_iter()
            .map(|row| crate::models::Exception {
                id: uuid::Uuid::parse_str(&row.id).unwrap_or_default(),
                project_id: uuid::Uuid::parse_str(&row.project_id).unwrap_or_default(),
                fingerprint: row.fingerprint,
                level: row.level,
                message: row.message,
                exception_type: if row.exception_type.is_empty() {
                    None
                } else {
                    Some(row.exception_type)
                },
                exception_value: if row.exception_value.is_empty() {
                    None
                } else {
                    Some(row.exception_value)
                },
                stacktrace: if row.stacktrace.is_empty() {
                    serde_json::json!([])
                } else {
                    serde_json::from_str(&row.stacktrace).unwrap_or(serde_json::json!([]))
                },
                context: serde_json::from_str(&row.context).unwrap_or(serde_json::Value::Null),
                tags: serde_json::from_str(&row.tags).unwrap_or(serde_json::Value::Null),
                user_data: serde_json::from_str(&row.user_data).unwrap_or(serde_json::Value::Null),
                timestamp: row.timestamp,
                created_at: row.created_at,
            })
            .collect();
        tracing::debug!(
            "Fetched {} exceptions from ClickHouse for fingerprint {}",
            from_ch.len(),
            &group.fingerprint
        );
        from_ch
    } else {
        recent_exceptions
    };

    let error_ids: Vec<String> = recent_exceptions.iter().map(|e| e.id.to_string()).collect();

    let traces = if !error_ids.is_empty() {
        // Query junction table for trace_ids
        // Use PostgreSQL-style placeholders ($1, $2, ...) for sqlx
        let placeholders: Vec<String> = (1..=error_ids.len()).map(|i| format!("${}", i)).collect();
        let project_id_param = format!("${}", error_ids.len() + 1);
        let query = format!(
            "SELECT DISTINCT trace_id FROM error_traces 
             WHERE error_id IN ({}) AND project_id = {}",
            placeholders.join(", "),
            project_id_param
        );

        let mut query_builder = sqlx::query_scalar::<_, String>(&query);
        for error_id in &error_ids {
            query_builder = query_builder.bind(error_id);
        }
        query_builder = query_builder.bind(&project_id);

        let trace_ids: Vec<String> = query_builder.fetch_all(&*state.db)
            .instrument(tracing::info_span!("pg_query", table = "error_traces"))
            .await.map_err(|e| {
            AppError::Internal(anyhow::anyhow!("Failed to query error_traces: {}", e))
        })?;

        // Query ClickHouse for those traces
        if !trace_ids.is_empty() {
            let trace_placeholders: Vec<String> =
                (0..trace_ids.len()).map(|_| "?".to_string()).collect();
            let trace_query = format!(
                "SELECT 
                    trace_id,
                    project_id,
                    min(timestamp) as min_start,
                    max(duration) as max_duration_ns,
                    max(timestamp + toIntervalNanosecond(duration)) as max_end_timestamp,
                    count(*) as span_count,
                    uniqExact(service_name) as service_count,
                    if(countIf(status_code = 'STATUS_CODE_ERROR') > 0, 'error', 'ok') as status
                FROM reiver.spans
                WHERE trace_id IN ({}) AND project_id = ?
                GROUP BY trace_id, project_id
                ORDER BY min_start DESC
                LIMIT 50",
                trace_placeholders.join(", ")
            );

            let mut trace_query_builder = state.clickhouse.as_ref().query(&trace_query);
            for trace_id in &trace_ids {
                trace_query_builder = trace_query_builder.bind(trace_id);
            }
            trace_query_builder = trace_query_builder.bind(project_id.to_string());

            #[derive(clickhouse::Row, serde::Deserialize)]
            #[allow(dead_code)] // max_end_timestamp included in SELECT but computed from other fields
            struct TraceRow {
                trace_id: String,
                project_id: String,
                #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
                min_start: chrono::DateTime<Utc>,
                max_duration_ns: i64, // nanoseconds in ClickHouse
                #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
                max_end_timestamp: chrono::DateTime<Utc>,
                span_count: u64,
                service_count: u64,
                status: String,
            }

            let trace_rows: Vec<TraceRow> = trace_query_builder.fetch_all()
                .instrument(tracing::info_span!("clickhouse_query", table = "spans", otel.name = "CH trace aggregation"))
                .await.map_err(|e| {
                AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e))
            })?;

            trace_rows
                .into_iter()
                .map(|row| {
                    let end_time =
                        row.min_start + chrono::Duration::nanoseconds(row.max_duration_ns);
                    crate::models::Trace {
                        trace_id: row.trace_id,
                        project_id: uuid::Uuid::parse_str(&row.project_id).unwrap_or_default(),
                        start_time: row.min_start,
                        end_time,
                        duration_ns: row.max_duration_ns, // Keep as nanoseconds
                        span_count: row.span_count as i64,
                        service_count: row.service_count as i64,
                        status: row.status,
                        service_name: String::new(),
                        root_span_name: String::new(),
                    }
                })
                .collect()
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    let traces = if traces.is_empty() {
        let last_dt = group.last_seen;
        let first_seen_str = group.first_seen.to_rfc3339();
        let inferred_sql = r#"
            SELECT trace_id, project_id,
                   min(timestamp) AS min_start,
                   max(duration) AS max_duration_ns,
                   count(*) AS span_count,
                   uniqExact(service_name) AS service_count
            FROM reiver.spans
            WHERE project_id = ?
            GROUP BY trace_id, project_id
            HAVING min(timestamp) <= parseDateTime64BestEffort(?)
               AND max(timestamp + toIntervalNanosecond(duration)) >= parseDateTime64BestEffort(?)
               AND countIf(status_code = 'STATUS_CODE_ERROR') >= 1
            ORDER BY min_start DESC
            LIMIT 50
        "#;
        let q = state
            .clickhouse
            .as_ref()
            .query(inferred_sql)
            .bind(project_id.to_string())
            .bind(last_dt.to_rfc3339())
            .bind(first_seen_str);
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct InferredTraceRow {
            trace_id: String,
            project_id: String,
            #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
            min_start: chrono::DateTime<Utc>,
            max_duration_ns: i64, // nanoseconds in ClickHouse
            span_count: u64,
            service_count: u64,
        }
        match q.fetch_all::<InferredTraceRow>()
            .instrument(tracing::info_span!("clickhouse_query", table = "spans", otel.name = "CH inferred trace linking"))
            .await {
            Ok(rows) => rows
                .into_iter()
                .map(|r| {
                    let end_time = r.min_start + chrono::Duration::nanoseconds(r.max_duration_ns);
                    crate::models::Trace {
                        trace_id: r.trace_id,
                        project_id: uuid::Uuid::parse_str(&r.project_id).unwrap_or_default(),
                        start_time: r.min_start,
                        end_time,
                        duration_ns: r.max_duration_ns, // Keep as nanoseconds
                        span_count: r.span_count as i64,
                        service_count: r.service_count as i64,
                        status: "error".to_string(),
                        service_name: String::new(),
                        root_span_name: String::new(),
                    }
                })
                .collect(),
            Err(e) => {
                tracing::warn!("Inferred trace–exception query failed: {}", e);
                Vec::new()
            }
        }
    } else {
        traces
    };

    let flag_changes_window_start = group.first_seen - chrono::Duration::hours(1);
    let flag_changes_window_end = group.last_seen + chrono::Duration::hours(1);

    // Get service names from traces (query spans for unique service names)
    let service_names: Vec<String> = if !traces.is_empty() {
        let trace_ids: Vec<String> = traces.iter().map(|t| t.trace_id.clone()).collect();
        let trace_placeholders: Vec<String> =
            (0..trace_ids.len()).map(|_| "?".to_string()).collect();
        let service_query = format!(
            "SELECT DISTINCT service_name
             FROM reiver.spans
             WHERE trace_id IN ({}) AND project_id = ?",
            trace_placeholders.join(", ")
        );

        let mut service_query_builder = state.clickhouse.as_ref().query(&service_query);
        for trace_id in &trace_ids {
            service_query_builder = service_query_builder.bind(trace_id);
        }
        service_query_builder = service_query_builder.bind(project_id.to_string());

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct ServiceRow {
            service_name: String,
        }

        let service_rows: Vec<ServiceRow> =
            service_query_builder.fetch_all()
                .instrument(tracing::info_span!("clickhouse_query", table = "spans", otel.name = "CH service names"))
                .await.unwrap_or_else(|e| {
                tracing::warn!("Failed to query service names for error correlation: {}", e);
                vec![]
            });

        service_rows.into_iter().map(|r| r.service_name).collect()
    } else {
        vec![]
    };

    // =========================================================================
    // Enrich exception group with deployment context from linked spans
    // If version/environment/etc are missing from exception, extract from span tags
    // =========================================================================
    let mut group = group; // Make mutable for enrichment

    if !traces.is_empty() && (group.version.is_none() || group.environment.is_none()) {
        let trace_ids: Vec<String> = traces.iter().map(|t| t.trace_id.clone()).collect();
        let trace_placeholders: Vec<String> =
            (0..trace_ids.len()).map(|_| "?".to_string()).collect();

        // Query for resource attributes from span_attributes
        // These are typically set via OpenTelemetry Resource on the tracer provider
        let resource_attrs_query = format!(
            "SELECT 
                anyIf(span_attributes['service.version'], span_attributes['service.version'] != '') as version,
                anyIf(span_attributes['deployment.environment'], span_attributes['deployment.environment'] != '') as environment,
                anyIf(span_attributes['deployment.id'], span_attributes['deployment.id'] != '') as deployment_id,
                anyIf(span_attributes['cloud.region'], span_attributes['cloud.region'] != '') as region,
                anyIf(span_attributes['host.name'], span_attributes['host.name'] != '') as host_name,
                anyIf(span_attributes['process.runtime.description'], span_attributes['process.runtime.description'] != '') as runtime,
                anyIf(span_attributes['k8s.pod.name'], span_attributes['k8s.pod.name'] != '') as pod_name,
                anyIf(span_attributes['k8s.cluster.name'], span_attributes['k8s.cluster.name'] != '') as cluster_name,
                anyIf(span_attributes['container.id'], span_attributes['container.id'] != '') as container_id
             FROM reiver.spans
             WHERE trace_id IN ({}) AND project_id = ?",
            trace_placeholders.join(", ")
        );

        let mut attrs_query_builder = state.clickhouse.as_ref().query(&resource_attrs_query);
        for trace_id in &trace_ids {
            attrs_query_builder = attrs_query_builder.bind(trace_id);
        }
        attrs_query_builder = attrs_query_builder.bind(project_id.to_string());

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct ResourceAttrsRow {
            version: String,
            environment: String,
            deployment_id: String,
            region: String,
            host_name: String,
            runtime: String,
            pod_name: String,
            cluster_name: String,
            container_id: String,
        }

        if let Ok(attrs) = attrs_query_builder.fetch_one::<ResourceAttrsRow>()
            .instrument(tracing::info_span!("clickhouse_query", table = "spans", otel.name = "CH enrich deployment context"))
            .await {
            // Enrich group with attributes from linked spans (only if not already set)
            if group.version.is_none() && !attrs.version.is_empty() {
                group.version = Some(attrs.version);
            }
            if group.environment.is_none() && !attrs.environment.is_empty() {
                group.environment = Some(attrs.environment);
            }
            if group.deployment_id.is_none() && !attrs.deployment_id.is_empty() {
                group.deployment_id = Some(attrs.deployment_id);
            }
            if group.region.is_none() && !attrs.region.is_empty() {
                group.region = Some(attrs.region);
            }
            if group.host_name.is_none() && !attrs.host_name.is_empty() {
                group.host_name = Some(attrs.host_name);
            }
            if group.runtime.is_none() && !attrs.runtime.is_empty() {
                group.runtime = Some(attrs.runtime);
            }
            if group.pod_name.is_none() && !attrs.pod_name.is_empty() {
                group.pod_name = Some(attrs.pod_name);
            }
            if group.cluster_name.is_none() && !attrs.cluster_name.is_empty() {
                group.cluster_name = Some(attrs.cluster_name);
            }
            if group.container_id.is_none() && !attrs.container_id.is_empty() {
                group.container_id = Some(attrs.container_id);
            }

            tracing::debug!(
                "Enriched exception group {} with deployment context from linked spans: version={:?}, env={:?}",
                group_id, group.version, group.environment
            );
        }
    }

    // Query for flag changes in the time window that affect any of these services
    let flag_changes_query = if !service_names.is_empty() {
        // Use array overlap operator (&&) to check if flag change's impacted_services overlaps with our service list
        // PostgreSQL uses $1, $2, etc. for positional parameters
        // $1 = project_id, $2 = start_time, $3 = end_time, $4+ = service names for ARRAY
        let placeholders: Vec<String> = (0..service_names.len())
            .map(|i| format!("${}", i + 4)) // Start at $4 since $1-$3 are taken
            .collect();
        let query = format!(
            "SELECT id, flag_id, flag_name, environment, change_type, changed_by, 
                    impacted_services, timestamp, metadata
             FROM feature_flag_changes
             WHERE project_id = $1
               AND timestamp >= $2
               AND timestamp <= $3
               AND (impacted_services && ARRAY[{}]::text[] OR impacted_services IS NULL OR array_length(impacted_services, 1) IS NULL)
             ORDER BY timestamp DESC
             LIMIT 10",
            placeholders.join(", ")
        );

        let mut query_builder = sqlx::query_as::<_, crate::models::FlagChange>(&query)
            .bind(project_id)
            .bind(flag_changes_window_start)
            .bind(flag_changes_window_end);

        for service_name in &service_names {
            query_builder = query_builder.bind(service_name);
        }

        query_builder
            .fetch_all(&*state.db)
            .await
            .unwrap_or_else(|e| {
                tracing::warn!("Failed to query flag changes for error correlation: {}", e);
                vec![]
            })
    } else {
        // No service names, just query by time window
        sqlx::query_as::<_, crate::models::FlagChange>(
            "SELECT id, flag_id, flag_name, environment, change_type, changed_by, 
                    impacted_services, timestamp, metadata
             FROM feature_flag_changes
             WHERE project_id = $1
               AND timestamp >= $2
               AND timestamp <= $3
             ORDER BY timestamp DESC
             LIMIT 10",
        )
        .bind(project_id)
        .bind(flag_changes_window_start)
        .bind(flag_changes_window_end)
        .fetch_all(&*state.db)
        .await
        .unwrap_or_else(|e| {
            tracing::warn!("Failed to query flag changes for error correlation: {}", e);
            vec![]
        })
    };

    tracing::info!(elapsed_ms = handler_start.elapsed().as_millis() as u64, "get_exception_group complete");

    Ok(Json(ExceptionGroupDetail {
        group,
        recent_exceptions,
        traces,
        flag_changes: flag_changes_query,
    }))
}

/// Request body for updating exception status
#[derive(Debug, Deserialize)]
struct UpdateExceptionStatusRequest {
    status: String, // "resolved", "unresolved", "ignored"
}

/// Response for update exception status
#[derive(Debug, Serialize)]
struct UpdateExceptionStatusResponse {
    success: bool,
    message: String,
}

/// Update the status of an exception group (resolve, unresolve, ignore)
async fn update_exception_status(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path((project_id, group_id)): Path<(Uuid, String)>,
    Json(payload): Json<UpdateExceptionStatusRequest>,
) -> Result<Json<UpdateExceptionStatusResponse>> {
    // Validate status value
    let valid_statuses = ["resolved", "unresolved", "ignored"];
    if !valid_statuses.contains(&payload.status.as_str()) {
        return Err(AppError::Validation(format!(
            "Invalid status '{}'. Must be one of: {}",
            payload.status,
            valid_statuses.join(", ")
        )));
    }

    // group_id can be either a fingerprint (hex hash) or exception UUID
    let fingerprint =
        resolve_group_id_to_fingerprint(state.clickhouse.as_ref(), &group_id, &project_id).await?;

    // Insert a new exception row using a subquery to atomically get the latest values
    // This avoids race conditions - we get the latest data at insert time, not query time
    let new_id = Uuid::new_v4();

    // Use INSERT ... SELECT to atomically get the latest exception data and insert with updated status
    // This ensures we always get the most recent data, even if a new exception was inserted between our check and insert
    // Use coalesce() to handle any NULL values from the materialized view (which uses nullIf)
    state.clickhouse.as_ref()
        .query("INSERT INTO reiver.exceptions (id, project_id, fingerprint, level, message, exception_type, exception_value, stacktrace, context, tags, user_data, service_name, trace_id, span_id, status, timestamp, created_at) SELECT ?, project_id, fingerprint, level, message, coalesce(exception_type, ''), coalesce(exception_value, ''), coalesce(stacktrace, ''), coalesce(context, ''), coalesce(tags, ''), coalesce(user_data, ''), coalesce(service_name, ''), coalesce(trace_id, ''), coalesce(span_id, ''), ?, now64(), now64() FROM reiver.exceptions WHERE project_id = ? AND fingerprint = ? ORDER BY timestamp DESC LIMIT 1")
        .bind(new_id.to_string())
        .bind(&payload.status)
        .bind(project_id.to_string())
        .bind(&fingerprint)
        .execute()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to insert exception status update: {}", e)))?;

    tracing::info!(
        "Updated exception group status: project_id={}, group_id={}, fingerprint={}, new_status={}",
        project_id,
        group_id,
        fingerprint,
        payload.status
    );

    // Update Redis cache with new status for regression detection
    // When a user resolves an exception, we need Redis to reflect this
    // so that the Kafka consumer can detect regressions when new errors come in
    let project_key = format!("stats:project:{}", project_id);
    let group_hash_key = format!("{}:group:{}", project_key, fingerprint);

    if let Ok(mut redis_conn) = state.redis.get().await {
        use bb8_redis::redis::AsyncCommands;
        // Try to get existing group data from Redis and update status
        if let Ok(json_str) = redis_conn.get::<_, String>(&group_hash_key).await {
            if let Ok(mut group_json) = serde_json::from_str::<serde_json::Value>(&json_str) {
                group_json["status"] = serde_json::json!(&payload.status);
                if let Ok(updated_json) = serde_json::to_string(&group_json) {
                    let _: std::result::Result<(), _> = redis_conn
                        .set_ex(
                            &group_hash_key,
                            &updated_json,
                            3 * 24 * 3600u64, // 3 days TTL
                        )
                        .await;
                    tracing::debug!(
                        "Updated Redis cache with new status for exception group: fingerprint={}, status={}",
                        fingerprint, payload.status
                    );
                }
            }
        }
    }

    let organization_id =
        sqlx::query_scalar::<_, Uuid>("SELECT organization_id FROM projects WHERE id = $1")
            .bind(project_id)
            .fetch_optional(&*state.db)
            .await
            .ok()
            .flatten();

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::ExceptionGroupUpdated)
        .resource("exception_group", project_id)
        .details(serde_json::json!({ "fingerprint": &fingerprint, "status": &payload.status }))
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

    Ok(Json(UpdateExceptionStatusResponse {
        success: true,
        message: format!("Exception status updated to '{}'", payload.status),
    }))
}

/// Get navigation info for traversing between exception instances within a group
/// Used for Older/Newer buttons in the error detail view
async fn get_exception_navigation(
    State(state): State<Arc<WatchState>>,
    Path((project_id, group_id)): Path<(Uuid, String)>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<crate::models::ExceptionNavigation>> {
    // group_id can be either a fingerprint (hex hash) or exception UUID
    let fingerprint =
        resolve_group_id_to_fingerprint(state.clickhouse.as_ref(), &group_id, &project_id).await?;

    // Get the current error_id from query params, or use the most recent one
    let current_error_id: Option<Uuid> =
        params.get("error_id").and_then(|s| Uuid::parse_str(s).ok());

    // Query all exception IDs for this fingerprint, ordered by timestamp DESC (newest first)
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct ExceptionIdRow {
        id: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: chrono::DateTime<Utc>,
    }

    let exception_rows: Vec<ExceptionIdRow> = state.clickhouse.as_ref()
        .query("SELECT id, timestamp FROM reiver.exceptions WHERE fingerprint = ? AND project_id = ? ORDER BY timestamp DESC")
        .bind(&fingerprint)
        .bind(project_id.to_string())
        .fetch_all()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

    if exception_rows.is_empty() {
        return Err(AppError::NotFound(
            "No exceptions found for this group".to_string(),
        ));
    }

    let total_count = exception_rows.len() as i64;

    // Find the current exception index
    let current_index = if let Some(ref error_id) = current_error_id {
        exception_rows
            .iter()
            .position(|r| Uuid::parse_str(&r.id).ok() == Some(*error_id))
            .map(|i| i as i64)
            .unwrap_or(0)
    } else {
        0 // Default to first (most recent)
    };

    let current_row = &exception_rows[current_index as usize];
    let current_id = Uuid::parse_str(&current_row.id).unwrap_or_default();
    let current_ts = current_row.timestamp;

    // Newer = index - 1 (more recent), Older = index + 1 (less recent)
    // Since we're ordered DESC, prev (newer) is before current, next (older) is after
    let (prev_id, prev_ts) = if current_index > 0 {
        let prev_row = &exception_rows[(current_index - 1) as usize];
        (
            Some(Uuid::parse_str(&prev_row.id).unwrap_or_default()),
            Some(prev_row.timestamp),
        )
    } else {
        (None, None)
    };

    let (next_id, next_ts) = if (current_index as usize) < exception_rows.len() - 1 {
        let next_row = &exception_rows[(current_index + 1) as usize];
        (
            Some(Uuid::parse_str(&next_row.id).unwrap_or_default()),
            Some(next_row.timestamp),
        )
    } else {
        (None, None)
    };

    Ok(Json(crate::models::ExceptionNavigation {
        current_error_id: current_id,
        current_timestamp: current_ts,
        prev_error_id: prev_id, // Newer error (more recent)
        prev_timestamp: prev_ts,
        next_error_id: next_id, // Older error (less recent)
        next_timestamp: next_ts,
        total_count,
        current_index: current_index + 1, // 1-based for display
    }))
}

async fn get_exception_group_history(
    State(state): State<Arc<WatchState>>,
    Path((project_id, group_id)): Path<(Uuid, String)>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Vec<crate::models::ExceptionRatePoint>>> {
    // group_id can be either a fingerprint (hex hash) or exception UUID
    let fingerprint =
        resolve_group_id_to_fingerprint(state.clickhouse.as_ref(), &group_id, &project_id).await?;

    // Get time range (default: 24h)
    let time_range = params
        .get("time_range")
        .map(|s| s.as_str())
        .unwrap_or("24h");

    // Determine interval and interval count based on time range
    let (interval, hours) = match time_range {
        "24h" => ("HOUR", 24),
        "7d" => ("DAY", 168),  // 7 days = 168 hours
        "30d" => ("DAY", 720), // 30 days = 720 hours
        _ => ("HOUR", 24),
    };

    // Build query optimized for ClickHouse ORDER BY (project_id, timestamp)
    // Use PREWHERE for fingerprint filter to skip more data
    let query = if interval == "HOUR" {
        format!(
            "SELECT toDateTime64(toStartOfHour(timestamp), 9) as time, count() as count 
             FROM reiver.exceptions 
             PREWHERE fingerprint = ?
             WHERE project_id = ? 
             AND timestamp >= now() - INTERVAL {} HOUR 
             GROUP BY time 
             ORDER BY time",
            hours
        )
    } else {
        format!(
            "SELECT toDateTime64(toStartOfDay(timestamp), 9) as time, count() as count 
             FROM reiver.exceptions 
             PREWHERE fingerprint = ?
             WHERE project_id = ? 
             AND timestamp >= now() - INTERVAL {} HOUR 
             GROUP BY time 
             ORDER BY time",
            hours
        )
    };

    // Build cache key including fingerprint, time_range, and interval
    let cache_params_strs = vec![
        project_id.to_string(),
        group_id.to_string(),
        fingerprint.clone(),
        time_range.to_string(),
        interval.to_string(),
    ];
    let cache_params: Vec<&str> = cache_params_strs.iter().map(|s| s.as_str()).collect();

    // Check cache first (TTL based on time range - shorter ranges cached for less time)
    let cache_ttl = match time_range {
        "24h" => CacheTTL::Short, // 1 minute for recent data
        "7d" => CacheTTL::Medium, // 5 minutes for weekly data
        _ => CacheTTL::Long,      // 15 minutes for monthly data
    };

    let history: Vec<crate::models::ExceptionRatePoint> = if let Some(cached) =
        get_cached_query::<Vec<crate::models::ExceptionRatePoint>>(
            &state.redis,
            &query,
            &cache_params[..],
        )
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Cache query failed: {}", e)))?
    {
        // Cache hit
        cached
    } else {
        // Cache miss - query ClickHouse
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct ErrorHistoryRow {
            #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
            time: chrono::DateTime<Utc>,
            count: u64,
        }

        // Bind parameters: PREWHERE fingerprint comes first, then WHERE project_id
        // Debug: Check if errors exist for this fingerprint (without time filter)
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct DebugRow {
            total_count: u64,
            #[serde(with = "clickhouse::serde::chrono::datetime64::nanos::option")]
            earliest: Option<chrono::DateTime<Utc>>,
            #[serde(with = "clickhouse::serde::chrono::datetime64::nanos::option")]
            latest: Option<chrono::DateTime<Utc>>,
        }

        let debug_query = "SELECT count() as total_count, min(timestamp) as earliest, max(timestamp) as latest FROM reiver.exceptions WHERE fingerprint = ? AND project_id = ?";
        if let Ok(debug_row) = state
            .clickhouse
            .as_ref()
            .query(debug_query)
            .bind(&fingerprint)
            .bind(project_id.to_string())
            .fetch_optional::<DebugRow>()
            .await
        {
            if let Some(row) = debug_row {
                tracing::info!("Error history debug: fingerprint={}, project_id={}, total_errors={}, earliest={:?}, latest={:?}, now()={:?}", 
                    fingerprint, project_id, row.total_count, row.earliest, row.latest, chrono::Utc::now());
            }
        }

        tracing::debug!(
            "Querying error history: fingerprint={}, project_id={}, time_range={}, query={}",
            fingerprint,
            project_id,
            time_range,
            &query
        );
        let history_rows: Vec<ErrorHistoryRow> = state
            .clickhouse
            .as_ref()
            .query(&query)
            .bind(&fingerprint)
            .bind(project_id.to_string())
            .fetch_all()
            .await
            .map_err(|e| {
                tracing::error!(
                    "ClickHouse error history query failed: {}, query: {}",
                    e,
                    &query
                );
                AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e))
            })?;

        tracing::debug!(
            "Found {} history rows for fingerprint {}",
            history_rows.len(),
            fingerprint
        );

        let history: Vec<crate::models::ExceptionRatePoint> = history_rows
            .into_iter()
            .map(|row| crate::models::ExceptionRatePoint {
                time: row.time,
                count: row.count as i64,
            })
            .collect();

        // Store in cache
        let _ =
            set_cached_query(&state.redis, &query, &cache_params[..], &history, cache_ttl).await;

        history
    };

    Ok(Json(history))
}

/// Get recent exceptions from Redis for immediate access (includes stacktraces)
/// Returns None if no exceptions in Redis, or empty vec if Redis has data but it's empty
async fn get_recent_exceptions_from_redis(
    redis_pool: &crate::app_state::RedisPool,
    project_id: Uuid,
    fingerprint: &str,
) -> anyhow::Result<Option<Vec<crate::models::Exception>>> {
    use crate::models::Exception;
    use bb8_redis::redis::AsyncCommands;

    let project_key = format!("stats:project:{}", project_id);
    let recent_key = format!("{}:recent_exceptions:{}", project_key, fingerprint);
    let mut conn = redis_pool
        .get()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get Redis connection: {}", e))?;

    let json_strings: Vec<String> = conn
        .lrange(&recent_key, 0, 99)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get recent exceptions from Redis: {}", e))?;

    if json_strings.is_empty() {
        return Ok(None);
    }

    let mut exceptions = Vec::new();
    for json_str in json_strings {
        match serde_json::from_str::<serde_json::Value>(&json_str) {
            Ok(json) => {
                let id = json["id"]
                    .as_str()
                    .and_then(|s| uuid::Uuid::parse_str(s).ok())
                    .unwrap_or_default();
                let project_id_from_json = json["project_id"]
                    .as_str()
                    .and_then(|s| uuid::Uuid::parse_str(s).ok())
                    .unwrap_or(project_id);
                let timestamp_ms = json["timestamp"].as_i64().unwrap_or(0);
                let created_at_ms = json["created_at"].as_i64().unwrap_or(0);
                let timestamp =
                    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(timestamp_ms)
                        .unwrap_or_else(|| chrono::Utc::now());
                let created_at =
                    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(created_at_ms)
                        .unwrap_or_else(|| chrono::Utc::now());

                exceptions.push(Exception {
                    id,
                    project_id: project_id_from_json,
                    fingerprint: json["fingerprint"]
                        .as_str()
                        .unwrap_or(fingerprint)
                        .to_string(),
                    level: json["level"].as_str().unwrap_or("error").to_string(),
                    message: json["message"].as_str().unwrap_or("").to_string(),
                    exception_type: json["exception_type"].as_str().map(|s| s.to_string()),
                    exception_value: json["exception_value"].as_str().map(|s| s.to_string()),
                    stacktrace: json
                        .get("stacktrace")
                        .cloned()
                        .unwrap_or(serde_json::json!([])),
                    context: json["context"].clone(),
                    tags: json["tags"].clone(),
                    user_data: json["user_data"].clone(),
                    timestamp,
                    created_at,
                });
            }
            Err(e) => {
                tracing::warn!("Failed to parse exception JSON from Redis: {}", e);
            }
        }
    }

    if exceptions.is_empty() {
        return Ok(None);
    }

    exceptions.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

    Ok(Some(exceptions))
}

/// GET /api/projects/{id}/services?time_range=1h
/// List services with health metrics for the service topology page
async fn list_services(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<serde_json::Value>> {
    let time_range = params.get("time_range").map(|s| s.as_str()).unwrap_or("1h");
    let seconds: u64 = match time_range {
        "15m" => 900,
        "1h" => 3600,
        "6h" => 21600,
        "24h" => 86400,
        "7d" => 604800,
        _ => 3600,
    };

    let clickhouse_url =
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());
    let http_client = reqwest::Client::new();

    // Query per-service metrics from spans table
    let metrics_sql = format!(
        r#"SELECT
            service_name,
            count() as total_spans,
            countIf(status_code = 'STATUS_CODE_ERROR') as error_spans,
            if(total_spans > 0, error_spans / total_spans, 0) as error_rate,
            quantile(0.50)(duration) / 1000000.0 as p50_ms,
            quantile(0.99)(duration) / 1000000.0 as p99_ms
        FROM reiver.spans
        WHERE project_id = '{}'
        AND timestamp >= toDateTime64(now() - {}, 9)
        GROUP BY service_name
        ORDER BY total_spans DESC"#,
        escape_clickhouse_string(&project_id.to_string()),
        seconds
    );

    let metrics_resp = http_client
        .post(&clickhouse_url)
        .query(&[("default_format", "JSONEachRow")])
        .body(metrics_sql)
        .send()
        .await
        .map_err(|e| AppError::Internal(anyhow!("ClickHouse request failed: {}", e)))?;

    if !metrics_resp.status().is_success() {
        let err = metrics_resp.text().await.unwrap_or_default();
        return Err(AppError::Internal(anyhow!(
            "ClickHouse metrics query failed: {}",
            err
        )));
    }
    let metrics_rows = crate::ch_stream::stream_json_lines(metrics_resp)
        .await
        .map_err(|e| AppError::Internal(anyhow!("ClickHouse stream error: {}", e)))?;

    // Query dependencies: cross-service calls via parent/child span relationships
    let deps_sql = format!(
        r#"SELECT DISTINCT
            s2.service_name as source,
            s1.service_name as target
        FROM reiver.spans s1
        INNER JOIN reiver.spans s2
            ON s1.parent_span_id = s2.span_id
            AND s1.trace_id = s2.trace_id
            AND s1.project_id = s2.project_id
        WHERE s1.project_id = '{}'
        AND s1.timestamp >= toDateTime64(now() - {}, 9)
        AND s2.timestamp >= toDateTime64(now() - {}, 9)
        AND s1.service_name != s2.service_name
        AND s1.parent_span_id != ''"#,
        escape_clickhouse_string(&project_id.to_string()),
        seconds,
        seconds
    );

    let deps_resp = http_client
        .post(&clickhouse_url)
        .query(&[("default_format", "JSONEachRow")])
        .body(deps_sql)
        .send()
        .await
        .map_err(|e| AppError::Internal(anyhow!("ClickHouse deps request failed: {}", e)))?;

    let deps_rows = if deps_resp.status().is_success() {
        crate::ch_stream::stream_json_lines(deps_resp)
            .await
            .unwrap_or_default()
    } else {
        Vec::new()
    };

    let mut services = Vec::new();
    let mut dependency_counts: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();

    let mut dependencies = Vec::new();
    for row in &deps_rows {
        let source = row["source"].as_str().unwrap_or("").to_string();
        let target = row["target"].as_str().unwrap_or("").to_string();
        if !source.is_empty() && !target.is_empty() {
            *dependency_counts.entry(source.clone()).or_insert(0) += 1;
            *dependency_counts.entry(target.clone()).or_insert(0) += 1;
            dependencies.push(serde_json::json!({
                "source": source,
                "target": target,
            }));
        }
    }

    for row in &metrics_rows {
        let service_name = row["service_name"].as_str().unwrap_or("").to_string();
        let total_spans = row["total_spans"].as_u64().unwrap_or(0);
        let error_rate = row["error_rate"].as_f64().unwrap_or(0.0);
        let p50_ms = row["p50_ms"].as_f64().unwrap_or(0.0);
        let p99_ms = row["p99_ms"].as_f64().unwrap_or(0.0);

        let health = if error_rate > 0.05 {
            "unhealthy"
        } else if error_rate > 0.01 {
            "degraded"
        } else {
            "healthy"
        };

        let request_rate = if seconds > 0 {
            total_spans as f64 / seconds as f64
        } else {
            0.0
        };

        let dep_count = dependency_counts.get(&service_name).copied().unwrap_or(0);

        services.push(serde_json::json!({
            "name": service_name,
            "health": health,
            "environment": serde_json::Value::Null,
            "requestRate": request_rate,
            "errorRate": error_rate,
            "p50Latency": p50_ms,
            "p99Latency": p99_ms,
            "dependencyCount": dep_count,
        }));
    }

    // If no spans data but discovered_services exist, fall back to discovered_services
    if services.is_empty() {
        let discovered_sql = format!(
            r#"SELECT
                service_name,
                sum(span_count) as span_count,
                sum(error_count) as error_count
            FROM reiver.discovered_services_agg
            WHERE project_id = '{}'
            GROUP BY service_name
            ORDER BY span_count DESC"#,
            escape_clickhouse_string(&project_id.to_string())
        );

        let disc_resp = http_client
            .post(&clickhouse_url)
            .query(&[("default_format", "JSONEachRow")])
            .body(discovered_sql)
            .send()
            .await
            .map_err(|e| {
                AppError::Internal(anyhow!(
                    "ClickHouse discovered services query failed: {}",
                    e
                ))
            })?;

        if disc_resp.status().is_success() {
            let disc_rows = crate::ch_stream::stream_json_lines(disc_resp)
                .await
                .unwrap_or_default();
            for row in disc_rows {
                let service_name = row["service_name"].as_str().unwrap_or("").to_string();
                let span_count = row["span_count"].as_u64().unwrap_or(0);
                let error_count = row["error_count"].as_u64().unwrap_or(0);
                let error_rate = if span_count > 0 {
                    error_count as f64 / span_count as f64
                } else {
                    0.0
                };
                let health = if error_rate > 0.05 {
                    "unhealthy"
                } else if error_rate > 0.01 {
                    "degraded"
                } else {
                    "healthy"
                };

                services.push(serde_json::json!({
                    "name": service_name,
                    "health": health,
                    "environment": serde_json::Value::Null,
                    "requestRate": 0.0,
                    "errorRate": error_rate,
                    "p50Latency": 0.0,
                    "p99Latency": 0.0,
                    "dependencyCount": 0,
                }));
            }
        }
    }

    Ok(Json(serde_json::json!({
        "services": services,
        "dependencies": dependencies,
    })))
}

/// GET /api/projects/{id}/topology?time_range=1h
/// Unified topology: all components across traces, logs, and metrics with dependency edges.
async fn get_topology(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<serde_json::Value>> {
    let time_range = params.get("time_range").map(|s| s.as_str()).unwrap_or("1h");
    let seconds: u64 = match time_range {
        "15m" => 900,
        "1h" => 3600,
        "6h" => 21600,
        "24h" => 86400,
        "7d" => 604800,
        _ => 3600,
    };
    let pid = project_id.to_string();

    let cache_query = "topology";
    let cache_params: [&str; 2] = [&pid, time_range];
    let cache_ttl = match time_range {
        "15m" | "1h" => CacheTTL::Short,
        _ => CacheTTL::Medium,
    };

    if let Some(cached) =
        get_cached_query::<serde_json::Value>(&state.redis, cache_query, &cache_params)
            .await
            .unwrap_or(None)
    {
        return Ok(Json(cached));
    }

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct TraceSvcRow {
        service_name: String,
        total_spans: u64,
        error_spans: u64,
    }

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct SvcNameRow {
        service_name: String,
    }

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct InfraRow {
        service_name: String,
        statefulset: String,
    }

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct EdgeRow {
        source: String,
        target: String,
        request_count: u64,
    }

    let ch = state.clickhouse.as_ref();
    let escaped_pid = escape_clickhouse_string(&pid);

    let trace_fut = ch
        .query(&format!(
            "SELECT service_name, sum(span_count) AS total_spans, sum(error_count) AS error_spans \
             FROM reiver.discovered_services_agg \
             WHERE project_id = '{}' \
             GROUP BY service_name \
             ORDER BY total_spans DESC",
            escaped_pid
        ))
        .fetch_all::<TraceSvcRow>();

    let log_fut = ch
        .query(&format!(
            "SELECT DISTINCT service_name \
             FROM reiver.logs \
             WHERE project_id = '{}' \
               AND timestamp >= now() - INTERVAL {} SECOND \
               AND service_name != ''",
            escaped_pid, seconds
        ))
        .fetch_all::<SvcNameRow>();

    let metric_fut = ch
        .query(&format!(
            "SELECT DISTINCT resource_attributes['service.name'] AS service_name \
             FROM reiver.time_series_v1 \
             WHERE project_id = '{}' \
               AND unix_milli >= toUnixTimestamp(now() - INTERVAL {} SECOND) * 1000 \
               AND resource_attributes['service.name'] != ''",
            escaped_pid, seconds
        ))
        .fetch_all::<SvcNameRow>();

    let infra_fut = ch
        .query(&format!(
            "SELECT DISTINCT service_name, \
                    resource_attributes['k8s.statefulset.name'] AS statefulset \
             FROM reiver.logs \
             WHERE project_id = '{}' \
               AND timestamp >= now() - INTERVAL {} SECOND \
               AND resource_attributes['k8s.statefulset.name'] != ''",
            escaped_pid, seconds
        ))
        .fetch_all::<InfraRow>();

    let edge_fut = ch
        .query(&format!(
            "SELECT s2.service_name AS source, s1.service_name AS target, \
                    count() AS request_count \
             FROM reiver.spans s1 \
             INNER JOIN reiver.spans s2 \
                 ON s1.parent_span_id = s2.span_id \
                 AND s1.trace_id = s2.trace_id \
                 AND s1.project_id = s2.project_id \
             WHERE s1.project_id = '{}' \
               AND s1.timestamp >= toDateTime64(now() - {}, 9) \
               AND s2.timestamp >= toDateTime64(now() - {}, 9) \
               AND s1.service_name != s2.service_name \
               AND s1.parent_span_id != '' \
             GROUP BY source, target \
             ORDER BY request_count DESC",
            escaped_pid, seconds, seconds
        ))
        .fetch_all::<EdgeRow>();

    let (trace_res, log_res, metric_res, infra_res, edge_res) =
        tokio::join!(trace_fut, log_fut, metric_fut, infra_fut, edge_fut);

    let trace_svcs = trace_res.unwrap_or_default();
    let log_svcs = log_res.unwrap_or_default();
    let metric_svcs = metric_res.unwrap_or_default();
    let infra_rows = infra_res.unwrap_or_default();
    let edges = edge_res.unwrap_or_default();

    use std::collections::BTreeMap;

    #[derive(Default)]
    struct NodeEntry {
        signals: Vec<&'static str>,
        total_spans: u64,
        error_spans: u64,
        statefulsets: Vec<String>,
    }

    let mut nodes: BTreeMap<String, NodeEntry> = BTreeMap::new();

    for svc in &trace_svcs {
        let e = nodes.entry(svc.service_name.clone()).or_default();
        if !e.signals.contains(&"traces") {
            e.signals.push("traces");
        }
        e.total_spans = svc.total_spans;
        e.error_spans = svc.error_spans;
    }

    for svc in &log_svcs {
        let e = nodes.entry(svc.service_name.clone()).or_default();
        if !e.signals.contains(&"logs") {
            e.signals.push("logs");
        }
    }

    for svc in &metric_svcs {
        let e = nodes.entry(svc.service_name.clone()).or_default();
        if !e.signals.contains(&"metrics") {
            e.signals.push("metrics");
        }
    }

    for row in &infra_rows {
        let e = nodes.entry(row.service_name.clone()).or_default();
        if !e.statefulsets.contains(&row.statefulset) {
            e.statefulsets.push(row.statefulset.clone());
        }
    }
    for e in nodes.values_mut() {
        e.statefulsets.sort();
    }

    let nodes_json: Vec<serde_json::Value> = nodes
        .iter()
        .map(|(name, e)| {
            let health = if e.signals.contains(&"traces") {
                let error_pct = if e.total_spans > 0 {
                    e.error_spans as f64 / e.total_spans as f64
                } else {
                    0.0
                };
                Some(serde_json::json!({
                    "error_pct": (error_pct * 100.0 * 10.0).round() / 10.0,
                    "span_count": e.total_spans,
                }))
            } else {
                None
            };

            let k8s = if !e.statefulsets.is_empty() {
                Some(serde_json::json!({ "statefulsets": e.statefulsets }))
            } else {
                None
            };

            serde_json::json!({
                "id": name,
                "signals": e.signals,
                "health": health,
                "k8s": k8s,
            })
        })
        .collect();

    let edges_json: Vec<serde_json::Value> = edges
        .iter()
        .map(|e| {
            serde_json::json!({
                "source": e.source,
                "target": e.target,
                "request_count": e.request_count,
            })
        })
        .collect();

    let result = serde_json::json!({
        "nodes": nodes_json,
        "edges": edges_json,
    });

    let _ = set_cached_query(&state.redis, cache_query, &cache_params, &result, cache_ttl).await;

    Ok(Json(result))
}

/// GET /api/projects/{id}/services/{service}?time_range=1h
async fn get_service_detail(
    State(_state): State<Arc<WatchState>>,
    Path((project_id, service_name)): Path<(Uuid, String)>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<serde_json::Value>> {
    let service_name = urlencoding::decode(&service_name)
        .unwrap_or(std::borrow::Cow::Borrowed(&service_name))
        .into_owned();

    let time_range = params.get("time_range").map(|s| s.as_str()).unwrap_or("1h");
    let seconds: u64 = match time_range {
        "15m" => 900,
        "1h" => 3600,
        "6h" => 21600,
        "24h" => 86400,
        "7d" => 604800,
        _ => 3600,
    };

    let bucket_interval_seconds: u64 = match time_range {
        "15m" => 30,
        "1h" => 60,
        "6h" => 300,
        "24h" => 900,
        "7d" => 3600,
        _ => 60,
    };

    let clickhouse_url =
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());
    let http = reqwest::Client::new();
    let pid = escape_clickhouse_string(&project_id.to_string());
    let svc = escape_clickhouse_string(&service_name);

    // 1. Service-level RED metrics + Apdex
    let metrics_sql = format!(
        r#"SELECT
            count() as total_spans,
            countIf(status_code = 'STATUS_CODE_ERROR') as error_spans,
            if(total_spans > 0, error_spans / total_spans, 0) as error_rate,
            quantile(0.50)(duration) / 1000000.0 as p50_ms,
            quantile(0.90)(duration) / 1000000.0 as p90_ms,
            quantile(0.99)(duration) / 1000000.0 as p99_ms,
            if(total_spans > 0,
               (countIf(duration <= 500000000) + countIf(duration > 500000000 AND duration <= 2000000000) * 0.5) / total_spans,
               1.0) as apdex
        FROM reiver.spans
        WHERE project_id = '{pid}'
          AND service_name = '{svc}'
          AND timestamp >= toDateTime64(now() - {seconds}, 9)"#
    );

    let metrics_rows = execute_ch_query(&http, &clickhouse_url, &metrics_sql).await?;
    let metrics_row: serde_json::Value = metrics_rows
        .into_iter()
        .next()
        .unwrap_or(serde_json::json!({}));

    let total_spans = metrics_row["total_spans"].as_u64().unwrap_or(0);
    let error_rate = metrics_row["error_rate"].as_f64().unwrap_or(0.0);
    let p50 = metrics_row["p50_ms"].as_f64().unwrap_or(0.0);
    let p90 = metrics_row["p90_ms"].as_f64().unwrap_or(0.0);
    let p99 = metrics_row["p99_ms"].as_f64().unwrap_or(0.0);
    let apdex = metrics_row["apdex"].as_f64().unwrap_or(1.0);
    let request_rate = if seconds > 0 {
        total_spans as f64 / seconds as f64
    } else {
        0.0
    };

    let health = if error_rate > 0.05 {
        "unhealthy"
    } else if error_rate > 0.01 {
        "degraded"
    } else {
        "healthy"
    };

    // 2. Operations breakdown (top 50 by request count)
    let ops_sql = format!(
        r#"SELECT
            span_name,
            count() as request_count,
            countIf(status_code = 'STATUS_CODE_ERROR') as errors,
            if(request_count > 0, errors / request_count, 0) as error_rate,
            quantile(0.50)(duration) / 1000000.0 as p50_ms,
            quantile(0.99)(duration) / 1000000.0 as p99_ms
        FROM reiver.spans
        WHERE project_id = '{pid}'
          AND service_name = '{svc}'
          AND timestamp >= toDateTime64(now() - {seconds}, 9)
        GROUP BY span_name
        ORDER BY request_count DESC
        LIMIT 50"#
    );

    let ops_rows = execute_ch_query(&http, &clickhouse_url, &ops_sql).await?;
    let operations: Vec<serde_json::Value> = ops_rows
        .into_iter()
        .map(|r| {
            serde_json::json!({
                "name": r["span_name"].as_str().unwrap_or(""),
                "requestCount": r["request_count"].as_u64().unwrap_or(0),
                "errorRate": r["error_rate"].as_f64().unwrap_or(0.0),
                "p50Latency": r["p50_ms"].as_f64().unwrap_or(0.0),
                "p99Latency": r["p99_ms"].as_f64().unwrap_or(0.0),
            })
        })
        .collect();

    // 3. Dependencies (services this service calls or is called by)
    let deps_sql = format!(
        r#"SELECT
            peer_service,
            count() as req_count,
            countIf(status_code = 'STATUS_CODE_ERROR') as err_count,
            if(req_count > 0, err_count / req_count, 0) as error_rate
        FROM (
            SELECT s2.service_name as peer_service,
                   s1.status_code as status_code
            FROM reiver.spans s1
            INNER JOIN reiver.spans s2
                ON s1.parent_span_id = s2.span_id
                AND s1.trace_id = s2.trace_id
                AND s1.project_id = s2.project_id
            WHERE s1.project_id = '{pid}'
              AND s1.service_name = '{svc}'
              AND s1.timestamp >= toDateTime64(now() - {seconds}, 9)
              AND s2.timestamp >= toDateTime64(now() - {seconds}, 9)
              AND s1.service_name != s2.service_name
              AND s1.parent_span_id != ''
            UNION ALL
            SELECT s1.service_name as peer_service,
                   s1.status_code as status_code
            FROM reiver.spans s1
            INNER JOIN reiver.spans s2
                ON s1.parent_span_id = s2.span_id
                AND s1.trace_id = s2.trace_id
                AND s1.project_id = s2.project_id
            WHERE s2.project_id = '{pid}'
              AND s2.service_name = '{svc}'
              AND s1.timestamp >= toDateTime64(now() - {seconds}, 9)
              AND s2.timestamp >= toDateTime64(now() - {seconds}, 9)
              AND s1.service_name != s2.service_name
              AND s1.parent_span_id != ''
        )
        GROUP BY peer_service
        ORDER BY req_count DESC"#
    );

    let deps_rows = execute_ch_query(&http, &clickhouse_url, &deps_sql).await?;
    let deps: Vec<serde_json::Value> = deps_rows
        .into_iter()
        .map(|r| {
            let er = r["error_rate"].as_f64().unwrap_or(0.0);
            let h = if er > 0.05 {
                "unhealthy"
            } else if er > 0.01 {
                "degraded"
            } else {
                "healthy"
            };
            let req = r["req_count"].as_u64().unwrap_or(0);
            serde_json::json!({
                "name": r["peer_service"].as_str().unwrap_or(""),
                "health": h,
                "requestRate": if seconds > 0 { req as f64 / seconds as f64 } else { 0.0 },
                "errorRate": er,
            })
        })
        .collect();

    // 4. Recent errors (top 10 error span names)
    let errors_sql = format!(
        r#"SELECT
            span_name as message,
            count() as cnt
        FROM reiver.spans
        WHERE project_id = '{pid}'
          AND service_name = '{svc}'
          AND status_code = 'STATUS_CODE_ERROR'
          AND timestamp >= toDateTime64(now() - {seconds}, 9)
        GROUP BY span_name
        ORDER BY cnt DESC
        LIMIT 10"#
    );

    let errors_rows = execute_ch_query(&http, &clickhouse_url, &errors_sql).await?;
    let recent_errors: Vec<serde_json::Value> = errors_rows
        .into_iter()
        .enumerate()
        .map(|(i, r)| {
            serde_json::json!({
                "id": format!("err-{}", i),
                "message": r["message"].as_str().unwrap_or("Unknown error"),
                "count": r["cnt"].as_u64().unwrap_or(0),
            })
        })
        .collect();

    // 5. Timeseries (rate, error rate, latency) bucketed by interval
    let ts_sql = format!(
        r#"SELECT
            toStartOfInterval(timestamp, INTERVAL {bucket_interval_seconds} SECOND) as bucket,
            count() as total,
            countIf(status_code = 'STATUS_CODE_ERROR') as errors,
            avg(duration) / 1000000.0 as avg_latency_ms
        FROM reiver.spans
        WHERE project_id = '{pid}'
          AND service_name = '{svc}'
          AND timestamp >= toDateTime64(now() - {seconds}, 9)
        GROUP BY bucket
        ORDER BY bucket ASC"#
    );

    let ts_rows = execute_ch_query(&http, &clickhouse_url, &ts_sql).await?;
    let mut rate_ts = Vec::new();
    let mut error_ts = Vec::new();
    let mut latency_ts = Vec::new();

    for r in ts_rows {
        let total = r["total"].as_f64().unwrap_or(0.0);
        let errors = r["errors"].as_f64().unwrap_or(0.0);
        let avg_lat = r["avg_latency_ms"].as_f64().unwrap_or(0.0);
        let er = if total > 0.0 { errors / total } else { 0.0 };

        rate_ts.push(serde_json::json!({ "value": total / bucket_interval_seconds as f64 }));
        error_ts.push(serde_json::json!({ "value": er }));
        latency_ts.push(serde_json::json!({ "value": avg_lat }));
    }

    Ok(Json(serde_json::json!({
        "service": {
            "environment": "default",
            "health": health,
            "requestRate": request_rate,
            "errorRate": error_rate,
            "p50Latency": p50,
            "p90Latency": p90,
            "p99Latency": p99,
            "apdex": apdex,
        },
        "dependencies": deps,
        "operations": operations,
        "recentErrors": recent_errors,
        "rateTimeseries": rate_ts,
        "errorTimeseries": error_ts,
        "latencyTimeseries": latency_ts,
    })))
}

/// Helper to execute a ClickHouse query, streaming the response into parsed rows.
async fn execute_ch_query(
    http: &reqwest::Client,
    clickhouse_url: &str,
    sql: &str,
) -> Result<Vec<serde_json::Value>> {
    let resp = http
        .post(clickhouse_url)
        .query(&[("default_format", "JSONEachRow")])
        .body(sql.to_string())
        .send()
        .await
        .map_err(|e| AppError::Internal(anyhow!("ClickHouse request failed: {}", e)))?;

    if !resp.status().is_success() {
        let err = resp.text().await.unwrap_or_default();
        return Err(AppError::Internal(anyhow!(
            "ClickHouse query failed: {}",
            err
        )));
    }

    crate::ch_stream::stream_json_lines(resp)
        .await
        .map_err(|e| AppError::Internal(anyhow!("ClickHouse stream error: {}", e)))
}

async fn get_project_stats(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
) -> Result<Json<ProjectStats>> {
    // Query ClickHouse for stats (with query caching)

    // Create owned string for cache params (needed for Send trait)
    let project_id_str = project_id.to_string();

    // Try to get cached counts first
    let query = "SELECT COUNT(*) FROM reiver.exceptions WHERE project_id = ?";
    let params: [&str; 1] = [&project_id_str];

    let total_exceptions: u64 = if let Some(cached) =
        get_cached_query::<u64>(&state.redis, query, &params)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Cache query failed: {}", e)))?
    {
        cached
    } else {
        // Cache miss - query ClickHouse
        let count: u64 = state
            .clickhouse
            .as_ref()
            .query(query)
            .bind(project_id.to_string())
            .fetch_one()
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

        // Only cache non-zero counts to avoid poisoning the cache when data hasn't been committed yet
        if count > 0 {
            let _ = set_cached_query(&state.redis, query, &params, &count, CacheTTL::Medium).await;
        }
        count
    };

    // Count distinct groups with unresolved status (cached)
    let query_unresolved = "SELECT count() FROM (SELECT fingerprint FROM reiver.exceptions WHERE project_id = ? GROUP BY fingerprint HAVING argMax(status, timestamp) = 'unresolved')";
    let unresolved_exceptions: u64 = if let Some(cached) =
        get_cached_query::<u64>(&state.redis, query_unresolved, &params)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Cache query failed: {}", e)))?
    {
        cached
    } else {
        let count: u64 = state
            .clickhouse
            .as_ref()
            .query(query_unresolved)
            .bind(project_id.to_string())
            .fetch_one()
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;
        if count > 0 {
            let _ = set_cached_query(
                &state.redis,
                query_unresolved,
                &params,
                &count,
                CacheTTL::Medium,
            )
            .await;
        }
        count
    };

    // Count distinct groups with resolved status (cached)
    let query_resolved = "SELECT count() FROM (SELECT fingerprint FROM reiver.exceptions WHERE project_id = ? GROUP BY fingerprint HAVING argMax(status, timestamp) = 'resolved')";
    let resolved_exceptions: u64 = if let Some(cached) =
        get_cached_query::<u64>(&state.redis, query_resolved, &params)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Cache query failed: {}", e)))?
    {
        cached
    } else {
        let count: u64 = state
            .clickhouse
            .as_ref()
            .query(query_resolved)
            .bind(project_id.to_string())
            .fetch_one()
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;
        if count > 0 {
            let _ = set_cached_query(
                &state.redis,
                query_resolved,
                &params,
                &count,
                CacheTTL::Medium,
            )
            .await;
        }
        count
    };

    // Get exception rate for last 24 hours from Redis (pre-computed by aggregation worker)
    let project_key = format!("stats:project:{}", project_id);
    let mut redis_conn = state.redis.get().await.map_err(|e| {
        AppError::Internal(anyhow::anyhow!("Failed to get Redis connection: {}", e))
    })?;

    let now = chrono::Utc::now();
    let mut exception_rate_24h = Vec::new();

    for i in 0..24 {
        let hour_time = now - chrono::Duration::hours(i);
        let hour_timestamp = (hour_time.timestamp() / 3600) * 3600;
        let rate_key = format!("{}:exception_rate:{}", project_key, hour_timestamp);

        let count: Option<i64> = redis_conn.get(&rate_key).await.map_err(|e| {
            AppError::Internal(anyhow::anyhow!(
                "Failed to get exception rate from Redis: {}",
                e
            ))
        })?;

        let count = count.unwrap_or(0);
        if count > 0 {
            let hour_datetime = chrono::DateTime::<chrono::Utc>::from_timestamp(hour_timestamp, 0)
                .unwrap_or_else(|| chrono::Utc::now());
            exception_rate_24h.push(crate::models::ExceptionRatePoint {
                time: hour_datetime,
                count,
            });
        }
    }

    exception_rate_24h.sort_by_key(|p| p.time);

    Ok(Json(ProjectStats {
        total_exceptions: total_exceptions as i64,
        unresolved_exceptions: unresolved_exceptions as i64,
        resolved_exceptions: resolved_exceptions as i64,
        exception_rate_24h,
    }))
}

/// Stream project stats updates via Server-Sent Events (SSE)
/// Clients will receive stats data directly in the SSE events, no additional API calls needed
async fn stream_project_stats(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
) -> Result<Response> {
    // Subscribe to stats updates
    let mut rx = state.stats_broadcast.subscribe();
    let project_id_clone = project_id;

    // Create SSE stream that sends stats + errors data when this project is updated
    // Stats are now maintained in Redis (updated incrementally, no ClickHouse queries!)
    // Note: error_rate_24h is skipped in SSE to avoid memory-intensive queries
    // Use /api/projects/{id}/stats endpoint for complete stats including error_rate_24h
    let redis_clone = state.redis.clone();
    let stream = async_stream::stream! {
        // Send initial stats and error groups from Redis (fast, no ClickHouse query)
        match get_stats_from_redis(&redis_clone, project_id_clone).await {
            Ok(Some(data)) => {
                if let Ok(json) = serde_json::to_string(&data) {
                    yield Ok::<Vec<u8>, Infallible>(format!("data: {}\n\n", json).into_bytes());
                } else {
                    tracing::error!("Failed to serialize initial stats");
                    yield Ok::<Vec<u8>, Infallible>(b"data: {}\n\n".to_vec());
                }
            }
            Ok(None) => {
                // No stats in Redis yet (fresh project), send empty data
                yield Ok::<Vec<u8>, Infallible>(b"data: {}\n\n".to_vec());
            }
            Err(e) => {
                tracing::error!("Failed to fetch initial stats from Redis for SSE: {}", e);
                // Send empty data so client doesn't wait forever
                yield Ok::<Vec<u8>, Infallible>(b"data: {}\n\n".to_vec());
            }
        }

        // Then listen for updates with keep-alive
        // Stats are pre-computed in worker, so no need to query ClickHouse here
        let (keepalive_tx, mut keepalive_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let _keepalive_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let _ = keepalive_tx.send(());
            }
        });

        loop {
            // Use tokio::select to handle broadcast messages and keep-alive
            tokio::select! {
                result = rx.recv() => {
                    match result {
                        Ok(msg) if msg.project_id == project_id_clone => {
                            // Use pre-computed stats from worker if available (read from Redis, no ClickHouse query!)
                            if let Some(stats) = msg.stats {
                                if let Ok(json) = serde_json::to_string(&stats) {
                                    yield Ok::<Vec<u8>, Infallible>(format!("data: {}\n\n", json).into_bytes());
                                } else {
                                    tracing::error!("Failed to serialize stats update from broadcast");
                                }
                            } else {
                                // Fallback: stats not in broadcast (throttled), read from Redis
                                match get_stats_from_redis(&redis_clone, project_id_clone).await {
                                    Ok(Some(data)) => {
                                        if let Ok(json) = serde_json::to_string(&data) {
                                            yield Ok::<Vec<u8>, Infallible>(format!("data: {}\n\n", json).into_bytes());
                                        } else {
                                            tracing::error!("Failed to serialize stats update from Redis");
                                        }
                                    }
                                    Ok(None) => {
                                        // No stats in Redis yet, skip this update
                                    }
                                    Err(e) => {
                                        tracing::error!("Failed to fetch stats from Redis for SSE update: {}", e);
                                        // Continue even if fetch fails - don't break the stream
                                    }
                                }
                            }
                        }
                        Ok(_) => {
                            // Different project, ignore
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                            // If messages are lagging, we'll catch up on the next update
                            // Only log if we're lagging significantly (more than 10 messages)
                            if skipped > 10 {
                                tracing::debug!("SSE broadcast channel lagged, skipped {} messages (will catch up on next update)", skipped);
                            }
                            // Continue - don't break on lag
                        }
                        Err(_) => {
                            // Channel closed, break
                            tracing::info!("SSE broadcast channel closed, ending stream");
                            break;
                        }
                    }
                }
                _ = keepalive_rx.recv() => {
                    // Send keep-alive comment every 30 seconds to prevent connection timeout
                    yield Ok::<Vec<u8>, Infallible>(b": keep-alive\n\n".to_vec());
                }
            }
        }
    };

    let body = Body::from_stream(stream);
    let response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .header(header::CONNECTION, "keep-alive")
        .body(body)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to create SSE response: {}", e)))?;

    Ok(response)
}

#[derive(Debug, Deserialize)]
struct RootCauseSuggestionsQuery {
    start: i64,
    end: i64,
}

/// GET /api/projects/{id}/root-cause-suggestions?start=&end=
/// start/end: unix timestamp in milliseconds.
async fn get_root_cause_suggestions(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<RootCauseSuggestionsQuery>,
) -> Result<Json<crate::root_cause::RootCauseSuggestionsResponse>> {
    let out = crate::root_cause::fetch_root_cause_suggestions(
        &state.clickhouse,
        project_id,
        params.start,
        params.end,
    )
    .await?;
    Ok(Json(out))
}

/// Get change tracking timeline for a project
/// GET /api/projects/{id}/change-tracking?start_time=&end_time=&type=
#[derive(Debug, Deserialize)]
#[allow(dead_code)] // event_type field for future filter implementation
struct ChangeTrackingQuery {
    start_time: Option<chrono::DateTime<Utc>>,
    end_time: Option<chrono::DateTime<Utc>>,
    #[serde(rename = "type")]
    event_type: Option<String>, // "feature_flag", "deployment"
    flag_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct ChangeTrackingEvent {
    id: Uuid,
    event_type: String,
    flag_id: Option<String>,
    flag_name: Option<String>,
    environment: Option<String>,
    change_type: Option<String>,
    changed_by: Option<serde_json::Value>,
    impacted_services: Option<Vec<String>>,
    timestamp: chrono::DateTime<Utc>,
    metadata: Option<serde_json::Value>,
}

async fn get_change_tracking(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<ChangeTrackingQuery>,
) -> Result<Json<Vec<ChangeTrackingEvent>>> {
    let start_time = params
        .start_time
        .unwrap_or_else(|| chrono::Utc::now() - chrono::Duration::days(7));
    let end_time = params.end_time.unwrap_or_else(chrono::Utc::now);

    let mut query = sqlx::QueryBuilder::new(
        "SELECT id, flag_id, flag_name, environment, change_type, changed_by, impacted_services, timestamp, metadata
         FROM feature_flag_changes
         WHERE project_id = "
    );
    query.push_bind(project_id);
    query.push(" AND timestamp >= ");
    query.push_bind(start_time);
    query.push(" AND timestamp <= ");
    query.push_bind(end_time);

    if let Some(ref flag_id) = params.flag_id {
        query.push(" AND flag_id = ");
        query.push_bind(flag_id);
    }

    query.push(" ORDER BY timestamp DESC LIMIT 100");

    #[derive(Debug, Serialize, sqlx::FromRow)]
    struct FlagChangeRow {
        id: Uuid,
        flag_id: String,
        flag_name: Option<String>,
        environment: Option<String>,
        change_type: String,
        changed_by: Option<serde_json::Value>,
        impacted_services: Option<Vec<String>>,
        timestamp: chrono::DateTime<Utc>,
        metadata: Option<serde_json::Value>,
    }

    let rows: Vec<FlagChangeRow> = query.build_query_as().fetch_all(&*state.db).await?;

    let events: Vec<ChangeTrackingEvent> = rows
        .into_iter()
        .map(|row| ChangeTrackingEvent {
            id: row.id,
            event_type: "feature_flag".to_string(),
            flag_id: Some(row.flag_id),
            flag_name: row.flag_name,
            environment: row.environment,
            change_type: Some(row.change_type),
            changed_by: row.changed_by,
            impacted_services: row.impacted_services,
            timestamp: row.timestamp,
            metadata: row.metadata,
        })
        .collect();

    Ok(Json(events))
}

/// List all feature flags used in a project (extracted from traces)
/// GET /api/projects/{id}/feature-flags
async fn list_feature_flags(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
) -> Result<Json<Vec<serde_json::Value>>> {
    // Query ClickHouse for unique flag IDs from spans
    let lookback_start = (chrono::Utc::now() - chrono::Duration::days(7)).to_rfc3339();

    let query = format!(
        r#"
        SELECT DISTINCT span_attributes['experiment.id'] as flag_id,
               any(service_name) as service_name,
               count() as evaluation_count,
               max(timestamp) as last_seen
        FROM reiver.spans
        WHERE project_id = toString('{}')
          AND timestamp >= parseDateTime64BestEffort('{}', 3)
          AND span_name = 'experiments.IsEnabled'
          AND span_attributes['experiment.id'] != ''
        GROUP BY flag_id
        ORDER BY last_seen DESC
        LIMIT 100
        "#,
        project_id, lookback_start
    );

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct FlagUsageRow {
        flag_id: String,
        service_name: String,
        evaluation_count: u64,
        last_seen: chrono::DateTime<Utc>,
    }

    let rows: Vec<FlagUsageRow> = state
        .clickhouse
        .as_ref()
        .query(&query)
        .fetch_all()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

    let flags: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|row| {
            serde_json::json!({
                "flag_id": row.flag_id,
                "service_name": row.service_name,
                "evaluation_count": row.evaluation_count,
                "last_seen": row.last_seen,
            })
        })
        .collect();

    Ok(Json(flags))
}

/// Get change history for a specific flag
/// GET /api/projects/{id}/feature-flags/{flag_id}/changes
async fn get_flag_changes(
    State(state): State<Arc<WatchState>>,
    Path((project_id, flag_id)): Path<(Uuid, String)>,
) -> Result<Json<Vec<ChangeTrackingEvent>>> {
    #[derive(Debug, Serialize, sqlx::FromRow)]
    struct FlagChangeRow {
        id: Uuid,
        flag_id: String,
        flag_name: Option<String>,
        environment: Option<String>,
        change_type: String,
        changed_by: Option<serde_json::Value>,
        impacted_services: Option<Vec<String>>,
        timestamp: chrono::DateTime<Utc>,
        metadata: Option<serde_json::Value>,
    }

    let rows: Vec<FlagChangeRow> = sqlx::query_as(
        "SELECT id, flag_id, flag_name, environment, change_type, changed_by, impacted_services, timestamp, metadata
         FROM feature_flag_changes
         WHERE project_id = $1 AND flag_id = $2
         ORDER BY timestamp DESC
         LIMIT 50"
    )
    .bind(project_id)
    .bind(&flag_id)
    .fetch_all(&*state.db)
    .await?;

    let events: Vec<ChangeTrackingEvent> = rows
        .into_iter()
        .map(|row| ChangeTrackingEvent {
            id: row.id,
            event_type: "feature_flag".to_string(),
            flag_id: Some(row.flag_id),
            flag_name: row.flag_name,
            environment: row.environment,
            change_type: Some(row.change_type),
            changed_by: row.changed_by,
            impacted_services: row.impacted_services,
            timestamp: row.timestamp,
            metadata: row.metadata,
        })
        .collect();

    Ok(Json(events))
}

/// Get usage stats for a specific flag
/// GET /api/projects/{id}/feature-flags/{flag_id}/usage
async fn get_flag_usage(
    State(state): State<Arc<WatchState>>,
    Path((project_id, flag_id)): Path<(Uuid, String)>,
) -> Result<Json<serde_json::Value>> {
    // Query ClickHouse for usage stats
    let lookback_start = (chrono::Utc::now() - chrono::Duration::days(7)).to_rfc3339();

    let query = format!(
        r#"
        SELECT service_name,
               count() as evaluation_count,
               countIf(span_attributes['experiment.value'] = 'true') as enabled_count,
               countIf(span_attributes['experiment.value'] = 'false') as disabled_count,
               min(timestamp) as first_seen,
               max(timestamp) as last_seen
        FROM reiver.spans
        WHERE project_id = toString('{}')
          AND timestamp >= parseDateTime64BestEffort('{}', 3)
          AND span_name = 'experiments.IsEnabled'
          AND span_attributes['experiment.id'] = '{}'
        GROUP BY service_name
        ORDER BY evaluation_count DESC
        "#,
        project_id, lookback_start, flag_id
    );

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct UsageRow {
        service_name: String,
        evaluation_count: u64,
        enabled_count: u64,
        disabled_count: u64,
        first_seen: chrono::DateTime<Utc>,
        last_seen: chrono::DateTime<Utc>,
    }

    let rows: Vec<UsageRow> = state
        .clickhouse
        .as_ref()
        .query(&query)
        .fetch_all()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

    let services: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|row| {
            serde_json::json!({
                "service_name": row.service_name,
                "evaluation_count": row.evaluation_count,
                "enabled_count": row.enabled_count,
                "disabled_count": row.disabled_count,
                "first_seen": row.first_seen,
                "last_seen": row.last_seen,
            })
        })
        .collect();

    Ok(Json(serde_json::json!({
        "flag_id": flag_id,
        "services": services,
    })))
}

/// List all versions deployed for a service
/// GET /api/projects/{id}/services/{service}/versions
/// Query params: start_time (ISO 8601), end_time (ISO 8601), environment (optional)
async fn list_service_versions(
    State(state): State<Arc<WatchState>>,
    Path((project_id, service_name)): Path<(Uuid, String)>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<serde_json::Value>> {
    // Get time range (default: last 24 hours)
    let end_time = params
        .get("end_time")
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .unwrap_or_else(chrono::Utc::now);

    let start_time = params
        .get("start_time")
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .unwrap_or_else(|| end_time - chrono::Duration::hours(24));

    let filter_environment = params
        .get("environment")
        .or_else(|| params.get("env"))
        .cloned();

    // Query ClickHouse for distinct versions
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct VersionRow {
        version: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        first_seen: chrono::DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        last_seen: chrono::DateTime<Utc>,
        total_requests: u64,
        total_errors: u64,
        error_rate: f64,
        rps: f64,
        p50_latency: f64,
        p75_latency: f64,
        p90_latency: f64,
        p95_latency: f64,
        p99_latency: f64,
    }

    let mut where_clauses = vec![
        format!("project_id = '{}'", project_id),
        format!(
            "service_name = '{}'",
            escape_clickhouse_string(&service_name)
        ),
        format!(
            "timestamp >= parseDateTime64BestEffort('{}')",
            start_time.to_rfc3339()
        ),
        format!(
            "timestamp <= parseDateTime64BestEffort('{}')",
            end_time.to_rfc3339()
        ),
        "span_attributes['service.version'] != ''".to_string(),
    ];

    if let Some(ref env) = filter_environment {
        where_clauses.push(format!(
            "span_attributes['deployment.environment'] = '{}'",
            escape_clickhouse_string(&env)
        ));
    }

    let where_clause = where_clauses.join(" AND ");

    // Aggregate by version: get requests, errors, latency percentiles, RPS
    let query = format!(
        r#"
        SELECT
            span_attributes['service.version'] as version,
            min(timestamp) as first_seen,
            max(timestamp) as last_seen,
            count(*) as total_requests,
            countIf(status_code = 'STATUS_CODE_ERROR') as total_errors,
            (countIf(status_code = 'STATUS_CODE_ERROR') * 100.0 / count(*)) as error_rate,
            count(*) / greatest(dateDiff('second', min(timestamp), max(timestamp)), 1) as rps,
            quantile(0.50)(duration / 1000000) as p50_latency,
            quantile(0.75)(duration / 1000000) as p75_latency,
            quantile(0.90)(duration / 1000000) as p90_latency,
            quantile(0.95)(duration / 1000000) as p95_latency,
            quantile(0.99)(duration / 1000000) as p99_latency
        FROM reiver.spans
        WHERE {}
        GROUP BY version
        ORDER BY last_seen DESC
        "#,
        where_clause
    );

    let versions: Vec<VersionRow> = state
        .clickhouse
        .as_ref()
        .query(&query)
        .fetch_all()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

    // Get error types per version (simplified - just count new errors)
    // In a full implementation, we'd compare with previous version
    let mut versions_with_errors = Vec::new();
    for version in versions {
        versions_with_errors.push(serde_json::json!({
            "version": version.version,
            "first_seen": version.first_seen,
            "last_seen": version.last_seen,
            "total_requests": version.total_requests,
            "total_errors": version.total_errors,
            "error_rate": version.error_rate,
            "rps": version.rps,
            "p50_latency_ms": version.p50_latency,
            "p75_latency_ms": version.p75_latency,
            "p90_latency_ms": version.p90_latency,
            "p95_latency_ms": version.p95_latency,
            "p99_latency_ms": version.p99_latency,
            "new_error_types": 0, // TODO: Implement comparison with previous version
        }));
    }

    Ok(Json(serde_json::json!({
        "service": service_name,
        "versions": versions_with_errors,
    })))
}

/// Compare two versions of a service
/// GET /api/projects/{id}/services/{service}/versions/compare?version1=1.2.3&version2=1.2.4
/// Query params: version1, version2, start_time, end_time, environment (optional)
async fn compare_versions(
    State(state): State<Arc<WatchState>>,
    Path((project_id, service_name)): Path<(Uuid, String)>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<serde_json::Value>> {
    // Get versions to compare
    let version1 = params
        .get("version1")
        .ok_or_else(|| AppError::Validation("version1 parameter required".to_string()))?;
    let version2 = params
        .get("version2")
        .ok_or_else(|| AppError::Validation("version2 parameter required".to_string()))?;

    // Get time range (default: last 24 hours)
    let end_time = params
        .get("end_time")
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .unwrap_or_else(chrono::Utc::now);

    let start_time = params
        .get("start_time")
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .unwrap_or_else(|| end_time - chrono::Duration::hours(24));

    let filter_environment = params
        .get("environment")
        .or_else(|| params.get("env"))
        .cloned();

    // Build WHERE clause
    let mut where_base = vec![
        format!("project_id = '{}'", project_id),
        format!(
            "service_name = '{}'",
            escape_clickhouse_string(&service_name)
        ),
        format!(
            "timestamp >= parseDateTime64BestEffort('{}')",
            start_time.to_rfc3339()
        ),
        format!(
            "timestamp <= parseDateTime64BestEffort('{}')",
            end_time.to_rfc3339()
        ),
    ];

    if let Some(ref env) = filter_environment {
        where_base.push(format!(
            "span_attributes['deployment.environment'] = '{}'",
            escape_clickhouse_string(&env)
        ));
    }

    let where_clause = where_base.join(" AND ");

    // Query metrics for both versions
    let compare_query = format!(
        r#"
        SELECT
            span_attributes['service.version'] as version,
            count(*) as total_requests,
            countIf(status_code = 'STATUS_CODE_ERROR') as total_errors,
            (countIf(status_code = 'STATUS_CODE_ERROR') * 100.0 / count(*)) as error_rate,
            count(*) / greatest(dateDiff('second', min(timestamp), max(timestamp)), 1) as rps,
            quantile(0.50)(duration / 1000000) as p50_latency,
            quantile(0.75)(duration / 1000000) as p75_latency,
            quantile(0.90)(duration / 1000000) as p90_latency,
            quantile(0.95)(duration / 1000000) as p95_latency,
            quantile(0.99)(duration / 1000000) as p99_latency
        FROM reiver.spans
        WHERE {}
          AND span_attributes['service.version'] IN ('{}', '{}')
        GROUP BY version
        "#,
        where_clause,
        escape_clickhouse_string(&version1),
        escape_clickhouse_string(&version2),
    );

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct VersionMetricsRow {
        version: String,
        total_requests: u64,
        total_errors: u64,
        error_rate: f64,
        rps: f64,
        p50_latency: f64,
        p75_latency: f64,
        p90_latency: f64,
        p95_latency: f64,
        p99_latency: f64,
    }

    let metrics: Vec<VersionMetricsRow> = state
        .clickhouse
        .as_ref()
        .query(&compare_query)
        .fetch_all()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

    let v1_metrics = metrics.iter().find(|m| m.version == *version1);
    let v2_metrics = metrics.iter().find(|m| m.version == *version2);

    // Get endpoint comparison
    let endpoint_comparison_query = format!(
        r#"
        SELECT
            resource_attributes['service.version'] as version,
            span_name as endpoint,
            count(*) as requests,
            countIf(status_code = 'STATUS_CODE_ERROR') as errors,
            (countIf(status_code = 'STATUS_CODE_ERROR') * 100.0 / count(*)) as error_rate,
            quantile(0.50)(duration / 1000000) as p50_latency,
            quantile(0.90)(duration / 1000000) as p90_latency,
            quantile(0.99)(duration / 1000000) as p99_latency
        FROM reiver.spans
        WHERE {}
          AND resource_attributes['service.version'] IN ('{}', '{}')
        GROUP BY version, endpoint
        ORDER BY requests DESC
        LIMIT 100
        "#,
        where_clause,
        escape_clickhouse_string(&version1),
        escape_clickhouse_string(&version2),
    );

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct EndpointComparisonRow {
        version: String,
        endpoint: String,
        requests: u64,
        errors: u64,
        error_rate: f64,
        p50_latency: f64,
        p90_latency: f64,
        p99_latency: f64,
    }

    let endpoint_data: Vec<EndpointComparisonRow> = state
        .clickhouse
        .as_ref()
        .query(&endpoint_comparison_query)
        .fetch_all()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

    // Group endpoints by name
    let mut endpoint_map: std::collections::HashMap<
        String,
        (
            Option<&EndpointComparisonRow>,
            Option<&EndpointComparisonRow>,
        ),
    > = std::collections::HashMap::new();
    for row in &endpoint_data {
        let entry = endpoint_map
            .entry(row.endpoint.clone())
            .or_insert((None, None));
        if row.version == *version1 {
            entry.0 = Some(row);
        } else if row.version == *version2 {
            entry.1 = Some(row);
        }
    }

    let endpoints: Vec<serde_json::Value> = endpoint_map
        .into_iter()
        .map(|(endpoint, (v1_row, v2_row))| {
            serde_json::json!({
                "endpoint": endpoint,
                "version1": v1_row.map(|r| serde_json::json!({
                    "requests": r.requests,
                    "errors": r.errors,
                    "error_rate": r.error_rate,
                    "p50_latency_ms": r.p50_latency,
                    "p90_latency_ms": r.p90_latency,
                    "p99_latency_ms": r.p99_latency,
                })),
                "version2": v2_row.map(|r| serde_json::json!({
                    "requests": r.requests,
                    "errors": r.errors,
                    "error_rate": r.error_rate,
                    "p50_latency_ms": r.p50_latency,
                    "p90_latency_ms": r.p90_latency,
                    "p99_latency_ms": r.p99_latency,
                })),
            })
        })
        .collect();

    Ok(Json(serde_json::json!({
        "service": service_name,
        "version1": version1,
        "version2": version2,
        "comparison": {
            "version1": v1_metrics.map(|m| serde_json::json!({
                "total_requests": m.total_requests,
                "total_errors": m.total_errors,
                "error_rate": m.error_rate,
                "rps": m.rps,
                "p50_latency_ms": m.p50_latency,
                "p75_latency_ms": m.p75_latency,
                "p90_latency_ms": m.p90_latency,
                "p95_latency_ms": m.p95_latency,
                "p99_latency_ms": m.p99_latency,
            })),
            "version2": v2_metrics.map(|m| serde_json::json!({
                "total_requests": m.total_requests,
                "total_errors": m.total_errors,
                "error_rate": m.error_rate,
                "rps": m.rps,
                "p50_latency_ms": m.p50_latency,
                "p75_latency_ms": m.p75_latency,
                "p90_latency_ms": m.p90_latency,
                "p95_latency_ms": m.p95_latency,
                "p99_latency_ms": m.p99_latency,
            })),
        },
        "endpoint_comparison": endpoints,
    })))
}

/// Get version-scoped metrics (requests/errors by version) for widgets
/// GET /api/projects/{id}/services/{service}/metrics/version-scoped
/// Query params: start_time, end_time, metric_type (requests|errors|error_rate), interval (optional, for time series)
async fn get_version_scoped_metrics(
    State(state): State<Arc<WatchState>>,
    Path((project_id, service_name)): Path<(Uuid, String)>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<serde_json::Value>> {
    // Get time range (default: last 24 hours)
    let end_time = params
        .get("end_time")
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .unwrap_or_else(chrono::Utc::now);

    let start_time = params
        .get("start_time")
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .unwrap_or_else(|| end_time - chrono::Duration::hours(24));

    let metric_type = params
        .get("metric_type")
        .map(|s| s.as_str())
        .unwrap_or("requests");
    let interval = params.get("interval"); // Optional: for time series (e.g., "1h", "5m")
    let filter_environment = params
        .get("environment")
        .or_else(|| params.get("env"))
        .cloned();

    // Build WHERE clause
    let mut where_clauses = vec![
        format!("project_id = '{}'", project_id),
        format!(
            "service_name = '{}'",
            escape_clickhouse_string(&service_name)
        ),
        format!(
            "timestamp >= parseDateTime64BestEffort('{}')",
            start_time.to_rfc3339()
        ),
        format!(
            "timestamp <= parseDateTime64BestEffort('{}')",
            end_time.to_rfc3339()
        ),
        "span_attributes['service.version'] != ''".to_string(),
    ];

    if let Some(ref env) = filter_environment {
        where_clauses.push(format!(
            "span_attributes['deployment.environment'] = '{}'",
            escape_clickhouse_string(&env)
        ));
    }

    let where_clause = where_clauses.join(" AND ");

    if let Some(ref interval_str) = interval {
        // Time series data: group by version and time bucket
        let time_bucket = match interval_str.as_str() {
            "1m" => "toStartOfMinute(timestamp)",
            "5m" => "toStartOfFiveMinute(timestamp)",
            "15m" => "toStartOfFifteenMinute(timestamp)",
            "1h" => "toStartOfHour(timestamp)",
            "1d" => "toStartOfDay(timestamp)",
            _ => "toStartOfHour(timestamp)", // Default to 1 hour
        };

        let metric_select = match metric_type {
            "requests" => "count(*) as value",
            "errors" => "countIf(status_code = 'STATUS_CODE_ERROR') as value",
            "error_rate" => "(countIf(status_code = 'STATUS_CODE_ERROR') * 100.0 / count(*)) as value",
            "rps" => "count(*) / greatest(dateDiff('second', min(timestamp), max(timestamp)), 1) as value",
            _ => "count(*) as value",
        };

        let query = format!(
            r#"
            SELECT
                {} as time_bucket,
                span_attributes['service.version'] as version,
                {}
            FROM reiver.spans
            WHERE {}
            GROUP BY time_bucket, version
            ORDER BY time_bucket ASC, version ASC
            "#,
            time_bucket, metric_select, where_clause
        );

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct TimeSeriesRow {
            #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
            time_bucket: chrono::DateTime<Utc>,
            version: String,
            value: f64,
        }

        let rows: Vec<TimeSeriesRow> = state
            .clickhouse
            .query(&query)
            .fetch_all()
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

        // Group by version for easier consumption
        let mut by_version: std::collections::HashMap<String, Vec<serde_json::Value>> =
            std::collections::HashMap::new();
        for row in rows {
            by_version
                .entry(row.version.clone())
                .or_insert_with(Vec::new)
                .push(serde_json::json!({
                    "timestamp": row.time_bucket,
                    "value": row.value,
                }));
        }

        let series: Vec<serde_json::Value> = by_version
            .into_iter()
            .map(|(version, data_points)| {
                serde_json::json!({
                    "version": version,
                    "data": data_points,
                })
            })
            .collect();

        Ok(Json(serde_json::json!({
            "service": service_name,
            "metric_type": metric_type,
            "interval": interval_str,
            "series": series,
        })))
    } else {
        // Aggregate data: total by version (no time series)
        let metric_select = match metric_type {
            "requests" => "count(*) as value",
            "errors" => "countIf(status_code = 'STATUS_CODE_ERROR') as value",
            "error_rate" => "(countIf(status_code = 'STATUS_CODE_ERROR') * 100.0 / count(*)) as value",
            "rps" => "count(*) / greatest(dateDiff('second', min(timestamp), max(timestamp)), 1) as value",
            _ => "count(*) as value",
        };

        let query = format!(
            r#"
            SELECT
                span_attributes['service.version'] as version,
                {} as value
            FROM reiver.spans
            WHERE {}
            GROUP BY version
            ORDER BY value DESC
            "#,
            metric_select, where_clause
        );

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct AggregateRow {
            version: String,
            value: f64,
        }

        let rows: Vec<AggregateRow> = state
            .clickhouse
            .query(&query)
            .fetch_all()
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

        let data: Vec<serde_json::Value> = rows
            .into_iter()
            .map(|row| {
                serde_json::json!({
                    "version": row.version,
                    "value": row.value,
                })
            })
            .collect();

        Ok(Json(serde_json::json!({
            "service": service_name,
            "metric_type": metric_type,
            "data": data,
        })))
    }
}

/// Get time between deployments metric
/// GET /api/projects/{id}/services/{service}/metrics/time-between-deployments
/// Returns the time_between_deployments metric for a service
/// This metric shows the duration in seconds between each deployment and the previous one
async fn get_time_between_deployments(
    State(state): State<Arc<WatchState>>,
    Path((project_id, service_name)): Path<(Uuid, String)>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<serde_json::Value>> {
    // Get time range (default: last 30 days)
    let end_time = params
        .get("end_time")
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .unwrap_or_else(chrono::Utc::now);

    let start_time = params
        .get("start_time")
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .unwrap_or_else(|| end_time - chrono::Duration::days(30));

    let filter_environment = params
        .get("environment")
        .or_else(|| params.get("env"))
        .cloned();

    // Query for versions with their first_seen timestamps
    let mut where_clauses = vec![
        format!("project_id = '{}'", project_id),
        format!(
            "service_name = '{}'",
            escape_clickhouse_string(&service_name)
        ),
        format!(
            "timestamp >= parseDateTime64BestEffort('{}')",
            start_time.to_rfc3339()
        ),
        format!(
            "timestamp <= parseDateTime64BestEffort('{}')",
            end_time.to_rfc3339()
        ),
        "span_attributes['service.version'] != ''".to_string(),
    ];

    if let Some(ref env) = filter_environment {
        where_clauses.push(format!(
            "span_attributes['deployment.environment'] = '{}'",
            escape_clickhouse_string(&env)
        ));
    }

    let where_clause = where_clauses.join(" AND ");

    // Get first_seen per version (ordered by first_seen)
    let query = format!(
        r#"
        SELECT
            span_attributes['service.version'] as version,
            min(timestamp) as first_seen
        FROM reiver.spans
        WHERE {}
        GROUP BY version
        ORDER BY first_seen ASC
        "#,
        where_clause
    );

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct VersionTimingRow {
        version: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        first_seen: chrono::DateTime<Utc>,
    }

    let versions: Vec<VersionTimingRow> = state
        .clickhouse
        .query(&query)
        .fetch_all()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

    // Calculate time between deployments
    // For each version (starting from the second one), calculate time since previous version
    let mut deployment_metrics = Vec::new();
    for i in 1..versions.len() {
        let current = &versions[i];
        let previous = &versions[i - 1];

        let time_between = current
            .first_seen
            .signed_duration_since(previous.first_seen)
            .num_seconds();

        // Get environment for tagging (if available)
        let env_tag = if let Some(ref env) = filter_environment {
            env.clone()
        } else {
            // Try to extract from a sample span
            let env_query = format!(
                r#"
                SELECT span_attributes['deployment.environment'] as env
                FROM reiver.spans
                WHERE project_id = '{}'
                  AND service_name = '{}'
                  AND span_attributes['service.version'] = '{}'
                LIMIT 1
                "#,
                project_id,
                escape_clickhouse_string(&service_name),
                escape_clickhouse_string(&current.version),
            );

            #[derive(clickhouse::Row, serde::Deserialize)]
            struct EnvRow {
                env: Option<String>,
            }

            state
                .clickhouse
                .query(&env_query)
                .fetch_optional()
                .await
                .ok()
                .flatten()
                .and_then(|r: EnvRow| r.env)
                .unwrap_or_default()
        };

        deployment_metrics.push(serde_json::json!({
            "version": current.version,
            "previous_version": previous.version,
            "time_between_deployments_seconds": time_between,
            "deployment_time": current.first_seen,
            "previous_deployment_time": previous.first_seen,
            "service": service_name,
            "environment": env_tag,
        }));
    }

    Ok(Json(serde_json::json!({
        "service": service_name,
        "metric_name": "reiver.service.time_between_deployments",
        "deployments": deployment_metrics,
    })))
}

/// Detect faulty deployments (deployments with error spikes)
/// GET /api/projects/{id}/services/{service}/deployments/faulty-detection
/// Query params: start_time, end_time, error_rate_threshold (default: 2.0 = 2x increase), min_error_rate (default: 1.0%)
/// Returns deployments that show a significant error rate increase compared to previous version
async fn detect_faulty_deployments(
    State(state): State<Arc<WatchState>>,
    Path((project_id, service_name)): Path<(Uuid, String)>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<serde_json::Value>> {
    // Get time range (default: last 7 days)
    let end_time = params
        .get("end_time")
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .unwrap_or_else(chrono::Utc::now);

    let start_time = params
        .get("start_time")
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .unwrap_or_else(|| end_time - chrono::Duration::days(7));

    let error_rate_threshold = params
        .get("error_rate_threshold")
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(2.0); // Default: 2x increase in error rate

    let min_error_rate = params
        .get("min_error_rate")
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(1.0); // Default: minimum 1% error rate to be considered faulty

    let filter_environment = params
        .get("environment")
        .or_else(|| params.get("env"))
        .cloned();

    // Build WHERE clause
    let mut where_clauses = vec![
        format!("project_id = '{}'", project_id),
        format!(
            "service_name = '{}'",
            escape_clickhouse_string(&service_name)
        ),
        format!(
            "timestamp >= parseDateTime64BestEffort('{}')",
            start_time.to_rfc3339()
        ),
        format!(
            "timestamp <= parseDateTime64BestEffort('{}')",
            end_time.to_rfc3339()
        ),
        "span_attributes['service.version'] != ''".to_string(),
    ];

    if let Some(ref env) = filter_environment {
        where_clauses.push(format!(
            "span_attributes['deployment.environment'] = '{}'",
            escape_clickhouse_string(&env)
        ));
    }

    let where_clause = where_clauses.join(" AND ");

    // Query for versions with error rates
    let query = format!(
        r#"
        SELECT
            span_attributes['service.version'] as version,
            min(timestamp) as first_seen,
            max(timestamp) as last_seen,
            count(*) as total_requests,
            countIf(status_code = 'STATUS_CODE_ERROR') as total_errors,
            (countIf(status_code = 'STATUS_CODE_ERROR') * 100.0 / count(*)) as error_rate,
            quantile(0.50)(duration / 1000000) as p50_latency,
            quantile(0.90)(duration / 1000000) as p90_latency,
            quantile(0.99)(duration / 1000000) as p99_latency
        FROM reiver.spans
        WHERE {}
        GROUP BY version
        ORDER BY first_seen ASC
        "#,
        where_clause
    );

    #[derive(clickhouse::Row, serde::Deserialize)]
    #[allow(dead_code)] // last_seen included in SELECT for completeness
    struct VersionMetricsRow {
        version: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        first_seen: chrono::DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        last_seen: chrono::DateTime<Utc>,
        total_requests: u64,
        total_errors: u64,
        error_rate: f64,
        p50_latency: f64,
        p90_latency: f64,
        p99_latency: f64,
    }

    let versions: Vec<VersionMetricsRow> = state
        .clickhouse
        .query(&query)
        .fetch_all()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

    // Compare each version with the previous one to detect faulty deployments
    let mut faulty_deployments = Vec::new();
    for i in 1..versions.len() {
        let current = &versions[i];
        let previous = &versions[i - 1];

        // Calculate if this deployment is faulty:
        // 1. Error rate must be above minimum threshold
        // 2. Error rate must be X times higher than previous version
        // 3. Or error rate increased significantly (if previous was very low)
        let is_faulty = current.error_rate >= min_error_rate
            && (
                // Case 1: Previous version had errors, current is X times worse
                (previous.error_rate > 0.0 && current.error_rate >= previous.error_rate * error_rate_threshold) ||
            // Case 2: Previous version had no/very few errors, but current has significant errors
            (previous.error_rate < 0.1 && current.error_rate >= min_error_rate)
            );

        if is_faulty {
            let error_rate_increase = if previous.error_rate > 0.0 {
                ((current.error_rate - previous.error_rate) / previous.error_rate) * 100.0
            } else {
                100.0 // Infinite increase (no previous errors)
            };

            faulty_deployments.push(serde_json::json!({
                "version": current.version,
                "previous_version": previous.version,
                "deployment_time": current.first_seen,
                "previous_deployment_time": previous.first_seen,
                "current_error_rate": current.error_rate,
                "previous_error_rate": previous.error_rate,
                "error_rate_increase_percent": error_rate_increase,
                "total_requests": current.total_requests,
                "total_errors": current.total_errors,
                "p50_latency_ms": current.p50_latency,
                "p90_latency_ms": current.p90_latency,
                "p99_latency_ms": current.p99_latency,
                "severity": if error_rate_increase > 500.0 { "critical" } else if error_rate_increase > 200.0 { "high" } else { "medium" },
            }));
        }
    }

    Ok(Json(serde_json::json!({
        "service": service_name,
        "detection_period": {
            "start_time": start_time,
            "end_time": end_time,
        },
        "thresholds": {
            "error_rate_threshold": error_rate_threshold,
            "min_error_rate": min_error_rate,
        },
        "faulty_deployments": faulty_deployments,
        "total_deployments_analyzed": versions.len(),
    })))
}

// Unified events endpoint - combines errors, traces, and logs
async fn list_unified_events(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Vec<serde_json::Value>>> {
    let handler_start = std::time::Instant::now();

    let event_type = params.get("event_type").map(|s| s.as_str());
    let time_range = params
        .get("time_range")
        .map(|s| s.as_str())
        .unwrap_or("24h");
    let severity_filter: Vec<String> = params
        .get("severity")
        .map(|s| {
            s.split(',')
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let status_filter: Vec<String> = params
        .get("status")
        .map(|s| {
            s.split(',')
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let service_filter = params.get("service").map(|s| s.as_str()).unwrap_or("");
    let service_names_filter: Vec<String> = params
        .get("service_names")
        .map(|s| {
            s.split(',')
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let search_query = params.get("search").map(|s| s.as_str()).unwrap_or("");

    // trace_id filter for log correlation (can be comma-separated for multiple trace_ids)
    let trace_id_filter: Vec<String> = params
        .get("trace_id")
        .map(|s| {
            s.split(',')
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .collect()
        })
        .unwrap_or_default();

    // Duration filter for traces
    let duration_op = params.get("duration_op").map(|s| s.as_str());
    let duration_min = params
        .get("duration_min")
        .and_then(|s| s.parse::<i64>().ok());
    let duration_max = params
        .get("duration_max")
        .and_then(|s| s.parse::<i64>().ok());

    // Calculate time range - check for custom start_time/end_time first
    let now = Utc::now();
    let (start_time, end_time) = if let (Some(start_str), Some(end_str)) =
        (params.get("start_time"), params.get("end_time"))
    {
        // Use custom start_time and end_time if provided
        let start = start_str
            .parse::<chrono::DateTime<Utc>>()
            .or_else(|_| {
                chrono::DateTime::parse_from_rfc3339(start_str).map(|dt| dt.with_timezone(&Utc))
            })
            .unwrap_or_else(|_| now - chrono::Duration::hours(24));
        let end = end_str
            .parse::<chrono::DateTime<Utc>>()
            .or_else(|_| {
                chrono::DateTime::parse_from_rfc3339(end_str).map(|dt| dt.with_timezone(&Utc))
            })
            .unwrap_or(now);
        (start, end)
    } else {
        // Use time_range to calculate start_time, end_time is now
        let start = match time_range {
            "15m" => now - chrono::Duration::minutes(15),
            "1h" => now - chrono::Duration::hours(1),
            "24h" => now - chrono::Duration::hours(24),
            "7d" => now - chrono::Duration::days(7),
            "30d" => now - chrono::Duration::days(30),
            _ => now - chrono::Duration::hours(24),
        };
        (start, now)
    };

    let mut all_events: Vec<serde_json::Value> = Vec::new();

    if event_type.is_none() || event_type == Some("errors") {
        #[derive(clickhouse::Row, serde::Deserialize, serde::Serialize)]
        struct ErrorEvent {
            id: String,
            fingerprint: String,
            message: String,
            exception_type: Option<String>,
            exception_value: Option<String>,
            status: String,
            service_name: Option<String>,
            count: u64,
            #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
            first_seen: chrono::DateTime<Utc>,
            #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
            last_seen: chrono::DateTime<Utc>,
        }

        let mut where_extra = String::new();

        // trace_id is a per-row column, so filter in WHERE before GROUP BY
        if !trace_id_filter.is_empty() {
            let trace_ids = trace_id_filter
                .iter()
                .map(|t| format!("'{}'", escape_clickhouse_string(t)))
                .collect::<Vec<_>>()
                .join(",");
            where_extra.push_str(&format!(" AND e.trace_id IN ({})", trace_ids));
        }

        let mut query = format!(
            "SELECT argMax(e.id, e.timestamp) as id, e.fingerprint, argMax(e.message, e.timestamp) as message,
                    nullIf(argMax(e.exception_type, e.timestamp), '') as exception_type, nullIf(argMax(e.exception_value, e.timestamp), '') as exception_value,
                    coalesce(argMax(e.status, e.timestamp), 'unresolved') as status, nullIf(argMax(e.service_name, e.timestamp), '') as service_name,
                    count() as count, min(e.timestamp) as first_seen, max(e.timestamp) as last_seen
             FROM reiver.exceptions e
             WHERE e.project_id = ?{}
             GROUP BY e.project_id, e.fingerprint",
            where_extra
        );

        // Aggregate functions (e.g. max/argMax) cannot be in WHERE; use HAVING.
        let mut having_clauses = vec![
            "max(e.timestamp) >= parseDateTime64BestEffort(?)".to_string(),
            "max(e.timestamp) <= parseDateTime64BestEffort(?)".to_string(),
        ];

        if !status_filter.is_empty() {
            let statuses = status_filter
                .iter()
                .map(|s| format!("'{}'", escape_clickhouse_string(s)))
                .collect::<Vec<_>>()
                .join(",");
            having_clauses.push(format!(
                "coalesce(argMax(e.status, e.timestamp), 'unresolved') IN ({})",
                statuses
            ));
        }

        if !service_names_filter.is_empty() {
            let names = service_names_filter
                .iter()
                .map(|n| format!("'{}'", escape_clickhouse_string(n)))
                .collect::<Vec<_>>()
                .join(",");
            having_clauses.push(format!(
                "argMax(e.service_name, e.timestamp) IN ({})",
                names
            ));
        } else if !service_filter.is_empty() {
            having_clauses.push(format!(
                "positionCaseInsensitive(argMax(e.service_name, e.timestamp), '{}') > 0",
                escape_clickhouse_string(&service_filter)
            ));
        }

        if !search_query.is_empty() {
            having_clauses.push(format!(
                "(positionCaseInsensitive(argMax(e.message, e.timestamp), '{}') > 0 OR positionCaseInsensitive(argMax(e.exception_type, e.timestamp), '{}') > 0)",
                escape_clickhouse_string(&search_query), escape_clickhouse_string(&search_query)
            ));
        }

        if !having_clauses.is_empty() {
            query.push_str(&format!(" HAVING {}", having_clauses.join(" AND ")));
        }

        query.push_str(" ORDER BY last_seen DESC LIMIT 100");

        let errors: Vec<ErrorEvent> = state
            .clickhouse
            .as_ref()
            .query(&query)
            .bind(project_id.to_string())
            .bind(start_time.to_rfc3339())
            .bind(end_time.to_rfc3339())
            .fetch_all()
            .instrument(tracing::info_span!("fetch_errors", table = "exceptions"))
            .await
            .map_err(|e| {
                tracing::error!("ClickHouse error fetching errors: {}", e);
                AppError::Internal(anyhow!("Failed to fetch errors: {}", e))
            })?;

        for error in errors {
            let ts = error.last_seen.to_rfc3339();
            let first = error.first_seen.to_rfc3339();
            all_events.push(serde_json::json!({
                "id": error.id,
                "type": "error",
                "fingerprint": error.fingerprint,
                "message": error.message,
                "exception_type": error.exception_type,
                "exception_value": error.exception_value,
                "status": error.status,
                "service_name": error.service_name,
                "count": error.count,
                "first_seen": first,
                "last_seen": ts,
                "timestamp": ts,
            }));
        }
    }

    if event_type.is_none() || event_type == Some("traces") {
        #[derive(clickhouse::Row, serde::Deserialize, serde::Serialize)]
        struct TraceEvent {
            trace_id: String,
            name: String,
            service_name: String,
            duration_ns: i64, // nanoseconds in ClickHouse
            status: String,
            #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
            timestamp: chrono::DateTime<Utc>,
        }

        let mut query = format!(
            "SELECT trace_id, span_name AS name, service_name, duration AS duration_ns, status_code AS status, timestamp
             FROM reiver.spans
             WHERE project_id = ?
               AND timestamp >= parseDateTime64BestEffort(?)
               AND timestamp <= parseDateTime64BestEffort(?)
               AND (parent_span_id = '' OR parent_span_id IS NULL)"
        );

        if !service_names_filter.is_empty() {
            let names = service_names_filter
                .iter()
                .map(|n| format!("'{}'", escape_clickhouse_string(n)))
                .collect::<Vec<_>>()
                .join(",");
            query.push_str(&format!(" AND service_name IN ({})", names));
        } else if !service_filter.is_empty() {
            query.push_str(&format!(
                " AND positionCaseInsensitive(service_name, '{}') > 0",
                escape_clickhouse_string(&service_filter)
            ));
        }

        // Apply duration filter (convert ms threshold to nanoseconds for comparison)
        match (duration_op, duration_min, duration_max) {
            (Some("gt"), Some(min), _) => {
                query.push_str(&format!(" AND duration > {}", min * 1000000));
            }
            (Some("lt"), Some(min), _) => {
                query.push_str(&format!(" AND duration < {}", min * 1000000));
            }
            (Some("between"), Some(min), Some(max)) => {
                query.push_str(&format!(
                    " AND duration >= {} AND duration <= {}",
                    min * 1000000,
                    max * 1000000
                ));
            }
            _ => {}
        }

        query.push_str(" ORDER BY timestamp DESC LIMIT 100");

        let traces: Vec<TraceEvent> = state
            .clickhouse
            .as_ref()
            .query(&query)
            .bind(project_id.to_string())
            .bind(start_time.to_rfc3339())
            .bind(end_time.to_rfc3339())
            .fetch_all()
            .instrument(tracing::info_span!("fetch_traces", table = "spans"))
            .await
            .map_err(|e| {
                tracing::error!("ClickHouse error fetching traces: {}", e);
                AppError::Internal(anyhow!("Failed to fetch traces: {}", e))
            })?;

        for trace in traces {
            let ts = trace.timestamp.to_rfc3339();
            all_events.push(serde_json::json!({
                "id": trace.trace_id.clone(),
                "type": "trace",
                "trace_id": trace.trace_id,
                "name": trace.name,
                "message": trace.name,
                "service_name": trace.service_name,
                "duration": trace.duration_ns,
                "status": trace.status,
                "timestamp": ts,
            }));
        }
    }

    if event_type.is_none() || event_type == Some("logs") {
        #[derive(clickhouse::Row, serde::Deserialize, serde::Serialize)]
        struct LogEvent {
            id: String,
            body: String,
            severity_text: String,
            service_name: String,
            trace_id: String,
            #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
            timestamp: chrono::DateTime<Utc>,
        }

        let mut query = format!(
            "SELECT toString(sipHash64(concat(toString(timestamp), trace_id, body))) AS id, body, severity_text, service_name, trace_id, timestamp
             FROM reiver.logs
             WHERE project_id = ?
               AND timestamp >= parseDateTime64BestEffort(?)
               AND timestamp <= parseDateTime64BestEffort(?)"
        );

        if !severity_filter.is_empty() {
            let levels = severity_filter
                .iter()
                .map(|l| match l.as_str() {
                    "error" => vec!["'ERROR'", "'FATAL'", "'error'", "'fatal'"],
                    "warning" => vec!["'WARN'", "'WARNING'", "'warning'", "'warn'"],
                    "info" => vec!["'INFO'", "'info'"],
                    _ => vec![],
                })
                .flatten()
                .collect::<Vec<_>>()
                .join(",");
            if !levels.is_empty() {
                query.push_str(&format!(" AND severity_text IN ({})", levels));
            }
        }

        if !service_names_filter.is_empty() {
            let names = service_names_filter
                .iter()
                .map(|n| format!("'{}'", escape_clickhouse_string(n)))
                .collect::<Vec<_>>()
                .join(",");
            query.push_str(&format!(" AND service_name IN ({})", names));
        } else if !service_filter.is_empty() {
            query.push_str(&format!(
                " AND positionCaseInsensitive(service_name, '{}') > 0",
                escape_clickhouse_string(&service_filter)
            ));
        }

        // Filter by trace_id(s) for log-trace correlation
        if !trace_id_filter.is_empty() {
            let trace_ids = trace_id_filter
                .iter()
                .map(|t| format!("'{}'", escape_clickhouse_string(t)))
                .collect::<Vec<_>>()
                .join(",");
            query.push_str(&format!(" AND trace_id IN ({})", trace_ids));
        }

        if !search_query.is_empty() {
            let common_words = [
                "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for", "of", "with",
                "by", "per",
            ];
            let search_terms: Vec<String> = search_query
                .split_whitespace()
                .filter(|s| {
                    let lower = s.to_lowercase();
                    s.len() > 3 && !common_words.contains(&lower.as_str())
                })
                .map(|s| escape_clickhouse_string(s))
                .collect();

            if !search_terms.is_empty() {
                if search_terms.len() > 1 {
                    let conditions: Vec<String> = search_terms
                        .iter()
                        .map(|term| format!("positionCaseInsensitive(body, '{}') > 0", term))
                        .collect();
                    query.push_str(&format!(" AND ({})", conditions.join(" AND ")));
                } else {
                    query.push_str(&format!(
                        " AND positionCaseInsensitive(body, '{}') > 0",
                        search_terms[0]
                    ));
                }
            } else {
                query.push_str(&format!(
                    " AND positionCaseInsensitive(body, '{}') > 0",
                    escape_clickhouse_string(&search_query)
                ));
            }
        }

        // Dynamic attribute filters: params like attr.http.target=/api/v1
        for (param_key, param_val) in &params {
            if let Some(attr_key) = param_key.strip_prefix("attr.") {
                if !is_valid_attribute_key(attr_key) || param_val.trim().is_empty() {
                    continue;
                }
                let escaped_key = escape_clickhouse_string(attr_key);
                let values: Vec<String> = param_val
                    .split(',')
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .map(|s| format!("'{}'", escape_clickhouse_string(s)))
                    .collect();
                if values.is_empty() {
                    continue;
                }
                let in_list = values.join(",");
                query.push_str(&format!(
                    " AND (log_attributes['{k}'] IN ({v}) OR resource_attributes['{k}'] IN ({v}))",
                    k = escaped_key,
                    v = in_list,
                ));
            }
        }

        query.push_str(" ORDER BY timestamp DESC LIMIT 100");

        let logs: Vec<LogEvent> = state
            .clickhouse
            .as_ref()
            .query(&query)
            .bind(project_id.to_string())
            .bind(start_time.to_rfc3339())
            .bind(end_time.to_rfc3339())
            .fetch_all()
            .instrument(tracing::info_span!("fetch_logs", table = "logs"))
            .await
            .map_err(|e| {
                tracing::error!("ClickHouse error fetching logs: {}", e);
                AppError::Internal(anyhow!("Failed to fetch logs: {}", e))
            })?;

        for log in logs {
            let ts = log.timestamp.to_rfc3339();
            all_events.push(serde_json::json!({
                "id": log.id,
                "type": "log",
                "body": log.body,
                "message": log.body,
                "severity_text": log.severity_text,
                "service_name": log.service_name,
                "trace_id": log.trace_id,
                "timestamp": ts,
            }));
        }

        // trace_id correlation is handled by the WHERE clause above
        // (the logs query from reiver.logs already includes trace_id IN (...) filter)
    }

    // Fetch feature flag events from PostgreSQL if requested
    if event_type.is_none() || event_type == Some("feature_flag") {
        #[derive(sqlx::FromRow)]
        struct FlagEventRow {
            id: Uuid,
            flag_id: String,
            flag_name: Option<String>,
            environment: Option<String>,
            change_type: String,
            changed_by: Option<serde_json::Value>,
            impacted_services: Option<Vec<String>>,
            timestamp: chrono::DateTime<Utc>,
            metadata: Option<serde_json::Value>,
        }

        let flag_events: Vec<FlagEventRow> = sqlx::query_as(
            "SELECT id, flag_id, flag_name, environment, change_type, changed_by, impacted_services, timestamp, metadata
             FROM feature_flag_changes
             WHERE project_id = $1
               AND timestamp >= $2
               AND timestamp <= $3
             ORDER BY timestamp DESC
             LIMIT 100"
        )
        .bind(project_id)
        .bind(start_time)
        .bind(end_time)
        .fetch_all(&*state.db)
        .await
        .unwrap_or_else(|e| {
            tracing::warn!("Failed to fetch feature flag events: {}", e);
            vec![]
        });

        for fe in flag_events {
            let ts = fe.timestamp.to_rfc3339();
            let display_name = fe.flag_name.as_deref().unwrap_or(&fe.flag_id).to_string();
            let msg = format!("Feature flag '{}' {}", display_name, fe.change_type);
            all_events.push(serde_json::json!({
                "id": fe.id.to_string(),
                "type": "feature_flag",
                "flag_id": fe.flag_id,
                "flag_name": fe.flag_name,
                "environment": fe.environment,
                "change_type": fe.change_type,
                "changed_by": fe.changed_by,
                "impacted_services": fe.impacted_services,
                "metadata": fe.metadata,
                "message": msg,
                "timestamp": ts,
            }));
        }
    }

    // Sort all events by timestamp descending
    all_events.sort_by(|a, b| {
        let a_time = a.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
        let b_time = b.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
        b_time.cmp(a_time)
    });

    all_events.truncate(100);

    tracing::info!(rows = all_events.len(), elapsed_ms = handler_start.elapsed().as_millis() as u64, "list_unified_events complete");

    Ok(Json(all_events))
}

// Get single log detail by ID
#[derive(Debug, Deserialize)]
struct LogDetailQuery {
    /// Approximate timestamp (ISO-8601) to avoid a full table scan.
    #[serde(default)]
    timestamp: Option<String>,
}

async fn get_log_detail(
    State(state): State<Arc<WatchState>>,
    Path((project_id, log_id)): Path<(Uuid, String)>,
    Query(qs): Query<LogDetailQuery>,
) -> Result<Json<serde_json::Value>> {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct LogRow {
        id: String,
        body: String,
        severity_text: String,
        service_name: String,
        trace_id: String,
        span_id: String,
        source: String, // From log_attributes, empty if not present
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: chrono::DateTime<Utc>,
    }

    let log: Option<LogRow> = if let Some(ref ts) = qs.timestamp {
        state
            .clickhouse
            .as_ref()
            .query(
                "SELECT toString(sipHash64(concat(toString(timestamp), trace_id, body))) AS id,
                        body, severity_text, service_name, trace_id, span_id,
                        log_attributes['source'] AS source, timestamp
                 FROM reiver.logs
                 WHERE project_id = ?
                   AND timestamp >= parseDateTime64BestEffort(?) - INTERVAL 5 SECOND
                   AND timestamp <= parseDateTime64BestEffort(?) + INTERVAL 5 SECOND
                   AND toString(sipHash64(concat(toString(timestamp), trace_id, body))) = ?
                 LIMIT 1",
            )
            .bind(project_id.to_string())
            .bind(ts)
            .bind(ts)
            .bind(&log_id)
            .fetch_optional()
            .await
            .map_err(|e| AppError::Internal(anyhow!("ClickHouse query failed: {}", e)))?
    } else {
        state.clickhouse.as_ref()
            .query(
                "SELECT toString(sipHash64(concat(toString(timestamp), trace_id, body))) AS id, 
                        body, severity_text, service_name, trace_id, span_id, 
                        log_attributes['source'] AS source, timestamp
                 FROM reiver.logs
                 WHERE project_id = ? AND toString(sipHash64(concat(toString(timestamp), trace_id, body))) = ?
                 LIMIT 1"
            )
            .bind(project_id.to_string())
            .bind(&log_id)
            .fetch_optional()
            .await
            .map_err(|e| AppError::Internal(anyhow!("ClickHouse query failed: {}", e)))?
    };

    let log = log.ok_or_else(|| AppError::NotFound(format!("Log {} not found", log_id)))?;

    Ok(Json(serde_json::json!({
        "id": log.id,
        "body": log.body,
        "severity_text": log.severity_text,
        "service_name": log.service_name,
        "trace_id": log.trace_id,
        "span_id": log.span_id,
        "source": if log.source.is_empty() { "direct".to_string() } else { log.source },
        "timestamp": log.timestamp.to_rfc3339(),
    })))
}

// Get surrounding logs for context
#[derive(Debug, Deserialize)]
struct LogContextParams {
    log_id: String,
    trace_id: Option<String>,
    /// Approximate timestamp of the log (ISO-8601). When provided, avoids a
    /// full table scan by narrowing the sipHash64 lookup to a tight window.
    timestamp: Option<String>,
    #[serde(default = "default_time_range")]
    time_range: String,
}

fn default_time_range() -> String {
    "2m".to_string()
}

async fn get_log_context(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<LogContextParams>,
) -> Result<Json<Vec<serde_json::Value>>> {
    // First, get the target log's timestamp and service_name.
    // When the caller supplies the log's timestamp we can restrict the
    // expensive sipHash64 scan to a ±5-second window instead of the
    // entire table.
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct LogTimestamp {
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: chrono::DateTime<Utc>,
        service_name: String,
    }

    let target: Option<LogTimestamp> = if let Some(ref ts) = params.timestamp {
        state
            .clickhouse
            .as_ref()
            .query(
                "SELECT timestamp, service_name FROM reiver.logs
                 WHERE project_id = ?
                   AND timestamp >= parseDateTime64BestEffort(?) - INTERVAL 5 SECOND
                   AND timestamp <= parseDateTime64BestEffort(?) + INTERVAL 5 SECOND
                   AND toString(sipHash64(concat(toString(timestamp), trace_id, body))) = ?
                 LIMIT 1",
            )
            .bind(project_id.to_string())
            .bind(ts)
            .bind(ts)
            .bind(&params.log_id)
            .fetch_optional()
            .await
            .map_err(|e| AppError::Internal(anyhow!("ClickHouse query failed: {}", e)))?
    } else {
        state.clickhouse.as_ref()
            .query(
                "SELECT timestamp, service_name FROM reiver.logs
                 WHERE project_id = ? AND toString(sipHash64(concat(toString(timestamp), trace_id, body))) = ? LIMIT 1"
            )
            .bind(project_id.to_string())
            .bind(&params.log_id)
            .fetch_optional()
            .await
            .map_err(|e| AppError::Internal(anyhow!("ClickHouse query failed: {}", e)))?
    };

    let target = match target {
        Some(t) => t,
        None => return Ok(Json(vec![])), // Log not found, return empty context
    };

    // Parse time range (default 2 minutes)
    let range_minutes: i64 = match params.time_range.as_str() {
        "1m" => 1,
        "2m" => 2,
        "5m" => 5,
        "10m" => 10,
        _ => 2,
    };

    let start_time = target.timestamp - chrono::Duration::minutes(range_minutes);
    let end_time = target.timestamp + chrono::Duration::minutes(range_minutes);

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct ContextLog {
        id: String,
        body: String,
        severity_text: String,
        service_name: String,
        trace_id: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: chrono::DateTime<Utc>,
    }

    // Build query - prioritize logs with the same trace_id, but also include time-based context
    let logs: Vec<ContextLog> = if let Some(ref trace_id) = params.trace_id {
        // If trace_id provided, get logs from same trace OR same time window
        state.clickhouse.as_ref()
            .query(
                "SELECT toString(sipHash64(concat(toString(timestamp), trace_id, body))) AS id, 
                        body, severity_text, service_name, trace_id, timestamp
                 FROM reiver.logs
                 WHERE project_id = ? AND (
                   trace_id = ? OR (timestamp >= parseDateTime64BestEffort(?) AND timestamp <= parseDateTime64BestEffort(?))
                 )
                 ORDER BY timestamp ASC
                 LIMIT 100"
            )
            .bind(project_id.to_string())
            .bind(trace_id)
            .bind(start_time)
            .bind(end_time)
            .fetch_all()
            .await
            .map_err(|e| AppError::Internal(anyhow!("ClickHouse query failed: {}", e)))?
    } else {
        // No trace_id, just use time window and optionally service
        let service_filter = match target.service_name.as_str() {
            "" => "".to_string(),
            s => format!(" AND service_name = '{}'", escape_clickhouse_string(s)),
        };

        state.clickhouse.as_ref()
            .query(&format!(
                "SELECT toString(sipHash64(concat(toString(timestamp), trace_id, body))) AS id, 
                        body, severity_text, service_name, trace_id, timestamp
                 FROM reiver.logs
                 WHERE project_id = ? AND timestamp >= parseDateTime64BestEffort(?) AND timestamp <= parseDateTime64BestEffort(?){}
                 ORDER BY timestamp ASC
                 LIMIT 100",
                service_filter
            ))
            .bind(project_id.to_string())
            .bind(start_time)
            .bind(end_time)
            .fetch_all()
            .await
            .map_err(|e| AppError::Internal(anyhow!("ClickHouse query failed: {}", e)))?
    };

    let result: Vec<serde_json::Value> = logs
        .into_iter()
        .map(|log| {
            serde_json::json!({
                "id": log.id,
                "body": log.body,
                "severity_text": log.severity_text,
                "service_name": log.service_name,
                "timestamp": log.timestamp.to_rfc3339(),
                "trace_id": log.trace_id,
            })
        })
        .collect();

    Ok(Json(result))
}

/// Query parameters for listing project metrics
#[derive(Debug, Deserialize)]
struct ListProjectMetricsQuery {
    time_range: Option<String>,
    prefix: Option<String>,
}

/// GET /{id}/metrics/names - list metric names for a project
async fn list_project_metric_names(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<ListProjectMetricsQuery>,
) -> Result<Json<serde_json::Value>> {
    let handler_start = std::time::Instant::now();

    let mut sql = format!(
        r#"SELECT 
            metric_name,
            anyLast(metric_type) as metric_type,
            anyLast(temporality) as temporality,
            count(DISTINCT fingerprint) as series_count,
            anyLast(labels) as labels,
            max(unix_milli) as last_seen_ms,
            anyLast(metric_attributes['unit']) as unit
        FROM reiver.time_series_v1
        WHERE project_id = '{}'"#,
        project_id
    );

    let time_range_ms = match params.time_range.as_deref() {
        Some("1h") => 3_600_000i64,
        Some("6h") => 6 * 3_600_000,
        Some("24h") => 24 * 3_600_000,
        Some("3d") => 3 * 24 * 3_600_000,
        Some("7d") => 7 * 24 * 3_600_000,
        Some("30d") | None => 30 * 24 * 3_600_000,
        Some(_) => 24 * 3_600_000,
    };
    sql.push_str(&format!(
        " AND unix_milli >= (toUnixTimestamp(now()) * 1000 - {})",
        time_range_ms
    ));

    if let Some(prefix) = &params.prefix {
        sql.push_str(&format!(
            " AND metric_name LIKE '{}%'",
            escape_clickhouse_string(prefix)
        ));
    }

    sql.push_str(" GROUP BY metric_name ORDER BY metric_name");

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct MetricRow {
        metric_name: String,
        metric_type: String,
        temporality: String,
        series_count: u64,
        labels: String,
        last_seen_ms: i64,
        unit: String,
    }

    let rows: Vec<MetricRow> = state
        .clickhouse
        .query(&sql)
        .fetch_all()
        .instrument(tracing::info_span!("clickhouse_query", table = "time_series_v1", otel.name = "CH list metric names"))
        .await
        .map_err(|e| {
            tracing::error!("Failed to list metric names: {}", e);
            AppError::Internal(anyhow!("Failed to list metrics: {}", e))
        })?;

    let metrics: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|r| {
            let label_keys: Vec<String> = serde_json::from_str::<serde_json::Value>(&r.labels)
                .ok()
                .and_then(|v| v.as_object().map(|obj| obj.keys().cloned().collect()))
                .unwrap_or_default();

            let last_seen = if r.last_seen_ms > 0 {
                chrono::DateTime::from_timestamp_millis(r.last_seen_ms)
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_default()
            } else {
                String::new()
            };

            serde_json::json!({
                "name": r.metric_name,
                "metric_type": r.metric_type,
                "temporality": r.temporality,
                "series_count": r.series_count,
                "label_keys": label_keys,
                "unit": if r.unit.is_empty() { None::<&str> } else { Some(r.unit.as_str()) },
                "last_seen": last_seen,
            })
        })
        .collect();

    tracing::info!(rows = metrics.len(), elapsed_ms = handler_start.elapsed().as_millis() as u64, "list_project_metric_names complete");

    Ok(Json(serde_json::json!({ "metrics": metrics })))
}

// =============================================================================
// Per-Metric Endpoints (project-scoped)
// =============================================================================

#[derive(Debug, Deserialize)]
struct MetricTimeseriesQuery {
    time_range: Option<String>,
    #[serde(default)]
    include_exemplars: bool,
}

/// GET /{id}/metrics/{metric_name}/timeseries
async fn get_metric_timeseries(
    State(state): State<Arc<WatchState>>,
    Path((project_id, metric_name)): Path<(Uuid, String)>,
    Query(params): Query<MetricTimeseriesQuery>,
) -> Result<Json<serde_json::Value>> {
    let handler_start = std::time::Instant::now();

    let time_range = params.time_range.as_deref().unwrap_or("1h");
    let (interval_clause, step_ms) = match time_range {
        "5m" => ("5 MINUTE", 10_000i64),
        "15m" => ("15 MINUTE", 30_000),
        "30m" => ("30 MINUTE", 60_000),
        "1h" => ("1 HOUR", 60_000),
        "3h" => ("3 HOUR", 180_000),
        "6h" => ("6 HOUR", 360_000),
        "12h" => ("12 HOUR", 720_000),
        "24h" | "1d" => ("24 HOUR", 1_440_000),
        "7d" => ("7 DAY", 10_080_000),
        "30d" => ("30 DAY", 43_200_000),
        _ => ("1 HOUR", 60_000),
    };

    let decoded_name =
        urlencoding::decode(&metric_name).unwrap_or(std::borrow::Cow::Borrowed(&metric_name));
    let safe_name = escape_clickhouse_string(&decoded_name);

    let sql = format!(
        r#"SELECT
            intDiv(unix_milli, {step}) * {step} AS bucket,
            avg(value) AS value
        FROM reiver.samples_v1
        WHERE project_id = '{pid}'
          AND metric_name = '{name}'
          AND unix_milli >= toInt64(toUnixTimestamp(now() - INTERVAL {interval}) * 1000)
        GROUP BY bucket
        ORDER BY bucket"#,
        step = step_ms,
        pid = project_id,
        name = safe_name,
        interval = interval_clause,
    );

    #[derive(clickhouse::Row, serde::Deserialize, serde::Serialize)]
    struct TsBucket {
        bucket: i64,
        value: f64,
    }

    let rows: Vec<TsBucket> = state
        .clickhouse
        .query(&sql)
        .fetch_all()
        .instrument(tracing::info_span!("clickhouse_query", table = "samples_v1", otel.name = "CH metric timeseries"))
        .await
        .map_err(|e| {
            tracing::error!("Metric timeseries query failed: {}", e);
            AppError::Internal(anyhow!("Metric timeseries query failed: {}", e))
        })?;

    let data: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| serde_json::json!({ "timestamp_ms": r.bucket, "value": r.value }))
        .collect();

    // Optionally fetch exemplars
    let exemplars = if params.include_exemplars {
        let ex_sql = format!(
            r#"SELECT
                exemplar_time_unix_nano,
                trace_id,
                span_id,
                value,
                filtered_attributes
            FROM reiver.metric_exemplars
            WHERE project_id = '{pid}'
              AND metric_name = '{name}'
              AND exemplar_time_unix_nano >= toInt64(toUnixTimestamp(now() - INTERVAL {interval}) * 1000000000)
            ORDER BY exemplar_time_unix_nano
            LIMIT 200"#,
            pid = project_id,
            name = safe_name,
            interval = interval_clause,
        );

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct ExemplarRow {
            exemplar_time_unix_nano: i64,
            trace_id: String,
            span_id: String,
            value: f64,
            filtered_attributes: Vec<(String, String)>,
        }

        match state
            .clickhouse
            .query(&ex_sql)
            .fetch_all::<ExemplarRow>()
            .instrument(tracing::info_span!("clickhouse_query", table = "metric_exemplars", otel.name = "CH fetch exemplars"))
            .await
        {
            Ok(ex_rows) => ex_rows
                .into_iter()
                .map(|r| {
                    let attrs: serde_json::Map<String, serde_json::Value> = r
                        .filtered_attributes
                        .into_iter()
                        .map(|(k, v)| (k, serde_json::Value::String(v)))
                        .collect();
                    serde_json::json!({
                        "timestamp_ms": r.exemplar_time_unix_nano / 1_000_000,
                        "value": r.value,
                        "trace_id": r.trace_id,
                        "span_id": r.span_id,
                        "filtered_attributes": attrs,
                    })
                })
                .collect::<Vec<_>>(),
            Err(e) => {
                tracing::warn!("Failed to fetch exemplars: {}", e);
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    tracing::info!(data_points = data.len(), exemplars = exemplars.len(), elapsed_ms = handler_start.elapsed().as_millis() as u64, "get_metric_timeseries complete");

    Ok(Json(serde_json::json!({
        "data": data,
        "exemplars": exemplars,
    })))
}

/// GET /{id}/metrics/{metric_name}/labels
async fn get_project_metric_labels(
    State(state): State<Arc<WatchState>>,
    Path((project_id, metric_name)): Path<(Uuid, String)>,
    Query(_params): Query<ListProjectMetricsQuery>,
) -> Result<Json<serde_json::Value>> {
    let decoded_name =
        urlencoding::decode(&metric_name).unwrap_or(std::borrow::Cow::Borrowed(&metric_name));
    let safe_name = escape_clickhouse_string(&decoded_name);

    let sql = format!(
        r#"SELECT labels
        FROM reiver.time_series_v1
        WHERE project_id = '{}'
          AND metric_name = '{}'"#,
        project_id, safe_name,
    );

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct LabelRow {
        labels: String,
    }

    let rows: Vec<LabelRow> = state
        .clickhouse
        .query(&sql)
        .fetch_all()
        .await
        .map_err(|e| {
            tracing::error!("Metric labels query failed: {}", e);
            AppError::Internal(anyhow!("Metric labels query failed: {}", e))
        })?;

    // Aggregate label keys and their values
    let mut label_values: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    for row in &rows {
        if let Ok(obj) = serde_json::from_str::<serde_json::Value>(&row.labels) {
            if let Some(map) = obj.as_object() {
                for (key, val) in map {
                    let v = val.as_str().unwrap_or("").to_string();
                    let entry = label_values.entry(key.clone()).or_default();
                    if !entry.contains(&v) && !v.is_empty() {
                        entry.push(v);
                    }
                }
            }
        }
    }

    Ok(Json(serde_json::json!({ "label_values": label_values })))
}

fn generate_api_key() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();
    (0..32)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

// =============================================================================
// API Monitoring Endpoints
// =============================================================================

/// Extract a numeric value from a serde_json::Value that may be a number or a string.
/// ClickHouse JSONEachRow returns integers as strings (e.g. `"41"` not `41`).
fn json_to_f64(v: &serde_json::Value) -> f64 {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
        .unwrap_or(0.0)
}

fn time_range_to_seconds(time_range: &str) -> u64 {
    match time_range {
        "15m" => 900,
        "1h" => 3600,
        "6h" => 21600,
        "24h" => 86400,
        "7d" => 604800,
        _ => 3600,
    }
}

/// GET /api/projects/{id}/api-endpoints?time_range=1h
/// List all HTTP endpoints with per-endpoint metrics.
async fn list_api_endpoints(
    State(_state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<serde_json::Value>> {
    let time_range = params.get("time_range").map(|s| s.as_str()).unwrap_or("1h");
    let seconds = time_range_to_seconds(time_range);

    let clickhouse_url =
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());
    let http_client = reqwest::Client::new();

    let pid = escape_clickhouse_string(&project_id.to_string());

    // Main per-endpoint metrics
    let sql = format!(
        r#"SELECT
            span_attributes['http.method'] AS method,
            span_attributes['http.route'] AS path,
            count() AS request_count,
            countIf(status_code = 'STATUS_CODE_ERROR') AS error_count,
            if(request_count > 0, error_count / request_count, 0) AS error_rate,
            avg(duration) / 1000000.0 AS avg_latency,
            quantile(0.99)(duration) / 1000000.0 AS p99_latency
        FROM reiver.spans
        WHERE project_id = '{pid}'
            AND timestamp >= toDateTime64(now() - {seconds}, 9)
            AND span_kind = 'SPAN_KIND_SERVER'
            AND span_attributes['http.route'] != ''
        GROUP BY method, path
        ORDER BY request_count DESC"#
    );

    let resp = http_client
        .post(&clickhouse_url)
        .query(&[("default_format", "JSONEachRow")])
        .body(sql)
        .send()
        .await
        .map_err(|e| AppError::Internal(anyhow!("ClickHouse request failed: {}", e)))?;

    if !resp.status().is_success() {
        let err = resp.text().await.unwrap_or_default();
        return Err(AppError::Internal(anyhow!(
            "ClickHouse query failed: {}",
            err
        )));
    }
    let rows = crate::ch_stream::stream_json_lines(resp)
        .await
        .map_err(|e| AppError::Internal(anyhow!("ClickHouse stream error: {}", e)))?;

    // Also fetch mini-trend data per endpoint (last N buckets)
    let bucket_seconds = std::cmp::max(seconds / 12, 60); // ~12 data points
    let trend_sql = format!(
        r#"SELECT
            span_attributes['http.method'] AS method,
            span_attributes['http.route'] AS path,
            toStartOfInterval(timestamp, toIntervalSecond({bucket_seconds})) AS ts,
            count() AS cnt
        FROM reiver.spans
        WHERE project_id = '{pid}'
            AND timestamp >= toDateTime64(now() - {seconds}, 9)
            AND span_kind = 'SPAN_KIND_SERVER'
            AND span_attributes['http.route'] != ''
        GROUP BY method, path, ts
        ORDER BY method, path, ts"#
    );

    let trend_resp = http_client
        .post(&clickhouse_url)
        .query(&[("default_format", "JSONEachRow")])
        .body(trend_sql)
        .send()
        .await
        .ok();

    let trend_rows = if let Some(r) = trend_resp {
        if r.status().is_success() {
            crate::ch_stream::stream_json_lines(r)
                .await
                .unwrap_or_default()
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    // Build trend map: (method, path) -> Vec<count>
    let mut trend_map: std::collections::HashMap<(String, String), Vec<f64>> =
        std::collections::HashMap::new();
    for row in trend_rows {
        let method = row["method"].as_str().unwrap_or("").to_string();
        let path = row["path"].as_str().unwrap_or("").to_string();
        let cnt = json_to_f64(&row["cnt"]);
        trend_map.entry((method, path)).or_default().push(cnt);
    }

    // Parse endpoint rows
    let mut endpoints = Vec::new();
    for row in rows {
        let method = row["method"].as_str().unwrap_or("GET").to_string();
        let path = row["path"].as_str().unwrap_or("").to_string();
        let trend = trend_map
            .get(&(method.clone(), path.clone()))
            .cloned()
            .unwrap_or_default();

        endpoints.push(serde_json::json!({
            "method": method,
            "path": path,
            "requestCount": json_to_f64(&row["request_count"]) as u64,
            "errorRate": json_to_f64(&row["error_rate"]),
            "avgLatency": json_to_f64(&row["avg_latency"]),
            "p99Latency": json_to_f64(&row["p99_latency"]),
            "trend": trend,
        }));
    }

    Ok(Json(serde_json::json!({ "endpoints": endpoints })))
}

/// GET /api/projects/{id}/api-endpoints/errors?time_range=1h&limit=10
/// List top errors grouped by endpoint + status code.
async fn list_api_endpoint_errors(
    State(_state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<serde_json::Value>> {
    let time_range = params.get("time_range").map(|s| s.as_str()).unwrap_or("1h");
    let limit: u32 = params
        .get("limit")
        .and_then(|l| l.parse().ok())
        .unwrap_or(10);
    let seconds = time_range_to_seconds(time_range);

    let clickhouse_url =
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());
    let http_client = reqwest::Client::new();

    let pid = escape_clickhouse_string(&project_id.to_string());

    let sql = format!(
        r#"SELECT
            span_attributes['http.method'] AS method,
            span_attributes['http.route'] AS path,
            toUInt16OrZero(span_attributes['http.status_code']) AS http_code,
            anyLast(span_name) AS message,
            count() AS error_count,
            max(timestamp) AS last_seen
        FROM reiver.spans
        WHERE project_id = '{pid}'
            AND timestamp >= toDateTime64(now() - {seconds}, 9)
            AND span_kind = 'SPAN_KIND_SERVER'
            AND status_code = 'STATUS_CODE_ERROR'
            AND span_attributes['http.route'] != ''
        GROUP BY method, path, http_code
        ORDER BY error_count DESC
        LIMIT {limit}"#
    );

    let resp = http_client
        .post(&clickhouse_url)
        .query(&[("default_format", "JSONEachRow")])
        .body(sql)
        .send()
        .await
        .map_err(|e| AppError::Internal(anyhow!("ClickHouse request failed: {}", e)))?;

    if !resp.status().is_success() {
        let err = resp.text().await.unwrap_or_default();
        return Err(AppError::Internal(anyhow!(
            "ClickHouse query failed: {}",
            err
        )));
    }
    let rows = crate::ch_stream::stream_json_lines(resp)
        .await
        .map_err(|e| AppError::Internal(anyhow!("ClickHouse stream error: {}", e)))?;

    let errors: Vec<serde_json::Value> = rows
        .into_iter()
        .enumerate()
        .map(|(i, row)| {
            serde_json::json!({
                "id": format!("err-{}", i),
                "method": row["method"].as_str().unwrap_or("GET"),
                "path": row["path"].as_str().unwrap_or(""),
                "statusCode": json_to_f64(&row["http_code"]) as u64,
                "message": row["message"].as_str().unwrap_or(""),
                "count": json_to_f64(&row["error_count"]) as u64,
                "lastSeen": row["last_seen"].as_str().unwrap_or(""),
            })
        })
        .collect();

    Ok(Json(serde_json::json!({ "errors": errors })))
}

/// GET /api/projects/{id}/api-endpoints/summary?time_range=1h
/// Overall summary stats + status code distribution + request volume over time.
async fn get_api_endpoints_summary(
    State(_state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<serde_json::Value>> {
    let time_range = params.get("time_range").map(|s| s.as_str()).unwrap_or("1h");
    let seconds = time_range_to_seconds(time_range);

    let clickhouse_url =
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());
    let http_client = reqwest::Client::new();

    let pid = escape_clickhouse_string(&project_id.to_string());

    // 1. Overall summary
    let summary_sql = format!(
        r#"SELECT
            count() AS total_requests,
            avg(duration) / 1000000.0 AS avg_latency,
            countIf(status_code = 'STATUS_CODE_ERROR') AS error_count,
            if(total_requests > 0, error_count / total_requests, 0) AS error_rate,
            quantile(0.99)(duration) / 1000000.0 AS p99_latency
        FROM reiver.spans
        WHERE project_id = '{pid}'
            AND timestamp >= toDateTime64(now() - {seconds}, 9)
            AND span_kind = 'SPAN_KIND_SERVER'"#
    );

    // 2. Status code distribution
    let status_sql = format!(
        r#"SELECT
            toUInt16OrZero(span_attributes['http.status_code']) AS code,
            count() AS cnt
        FROM reiver.spans
        WHERE project_id = '{pid}'
            AND timestamp >= toDateTime64(now() - {seconds}, 9)
            AND span_kind = 'SPAN_KIND_SERVER'
            AND span_attributes['http.status_code'] != ''
        GROUP BY code
        ORDER BY code"#
    );

    // 3. Request volume over time
    let bucket_seconds = std::cmp::max(seconds / 20, 60);
    let volume_sql = format!(
        r#"SELECT
            toStartOfInterval(timestamp, toIntervalSecond({bucket_seconds})) AS ts,
            count() AS cnt,
            countIf(status_code = 'STATUS_CODE_ERROR') AS errors
        FROM reiver.spans
        WHERE project_id = '{pid}'
            AND timestamp >= toDateTime64(now() - {seconds}, 9)
            AND span_kind = 'SPAN_KIND_SERVER'
        GROUP BY ts
        ORDER BY ts"#
    );

    // Execute all three in parallel
    let (summary_resp, status_resp, volume_resp) = tokio::try_join!(
        http_client
            .post(&clickhouse_url)
            .query(&[("default_format", "JSONEachRow")])
            .body(summary_sql)
            .send(),
        http_client
            .post(&clickhouse_url)
            .query(&[("default_format", "JSONEachRow")])
            .body(status_sql)
            .send(),
        http_client
            .post(&clickhouse_url)
            .query(&[("default_format", "JSONEachRow")])
            .body(volume_sql)
            .send(),
    )
    .map_err(|e| AppError::Internal(anyhow!("ClickHouse request failed: {}", e)))?;

    // Parse summary
    let summary_rows = if summary_resp.status().is_success() {
        crate::ch_stream::stream_json_lines(summary_resp)
            .await
            .unwrap_or_default()
    } else {
        Vec::new()
    };

    let summary = if let Some(row) = summary_rows.into_iter().next() {
        serde_json::json!({
            "totalRequests": json_to_f64(&row["total_requests"]) as u64,
            "avgLatency": json_to_f64(&row["avg_latency"]),
            "errorRate": json_to_f64(&row["error_rate"]),
            "p99Latency": json_to_f64(&row["p99_latency"]),
        })
    } else {
        serde_json::json!({"totalRequests": 0, "avgLatency": 0, "errorRate": 0, "p99Latency": 0})
    };

    // Parse status codes
    let status_rows = if status_resp.status().is_success() {
        crate::ch_stream::stream_json_lines(status_resp)
            .await
            .unwrap_or_default()
    } else {
        Vec::new()
    };

    let status_codes: Vec<serde_json::Value> = status_rows
        .into_iter()
        .map(|row| {
            serde_json::json!({
                "code": json_to_f64(&row["code"]) as u64,
                "count": json_to_f64(&row["cnt"]) as u64,
            })
        })
        .collect();

    // Parse request volume
    let volume_rows = if volume_resp.status().is_success() {
        crate::ch_stream::stream_json_lines(volume_resp)
            .await
            .unwrap_or_default()
    } else {
        Vec::new()
    };

    let request_volume: Vec<serde_json::Value> = volume_rows
        .into_iter()
        .map(|row| {
            serde_json::json!({
                "timestamp": row["ts"].as_str().unwrap_or(""),
                "requests": json_to_f64(&row["cnt"]) as u64,
                "errors": json_to_f64(&row["errors"]) as u64,
            })
        })
        .collect();

    Ok(Json(serde_json::json!({
        "summary": summary,
        "statusCodes": status_codes,
        "requestVolume": request_volume,
    })))
}

#[cfg(test)]
mod api_monitoring_tests {
    use super::*;

    // =========================================================================
    // json_to_f64 tests
    // =========================================================================

    #[test]
    fn test_json_to_f64_from_number() {
        let v = serde_json::json!(42.5);
        assert_eq!(json_to_f64(&v), 42.5);
    }

    #[test]
    fn test_json_to_f64_from_integer() {
        let v = serde_json::json!(100);
        assert_eq!(json_to_f64(&v), 100.0);
    }

    #[test]
    fn test_json_to_f64_from_string_integer() {
        // ClickHouse JSONEachRow returns integers as strings
        let v = serde_json::json!("41");
        assert_eq!(json_to_f64(&v), 41.0);
    }

    #[test]
    fn test_json_to_f64_from_string_float() {
        let v = serde_json::json!("3.14");
        assert_eq!(json_to_f64(&v), 3.14);
    }

    #[test]
    fn test_json_to_f64_from_empty_string() {
        let v = serde_json::json!("");
        assert_eq!(json_to_f64(&v), 0.0);
    }

    #[test]
    fn test_json_to_f64_from_null() {
        let v = serde_json::Value::Null;
        assert_eq!(json_to_f64(&v), 0.0);
    }

    #[test]
    fn test_json_to_f64_from_non_numeric_string() {
        let v = serde_json::json!("not-a-number");
        assert_eq!(json_to_f64(&v), 0.0);
    }

    // =========================================================================
    // time_range_to_seconds tests
    // =========================================================================

    #[test]
    fn test_time_range_to_seconds() {
        assert_eq!(time_range_to_seconds("15m"), 900);
        assert_eq!(time_range_to_seconds("1h"), 3600);
        assert_eq!(time_range_to_seconds("6h"), 21600);
        assert_eq!(time_range_to_seconds("24h"), 86400);
        assert_eq!(time_range_to_seconds("7d"), 604800);
        assert_eq!(time_range_to_seconds("unknown"), 3600); // default
    }

    // =========================================================================
    // Response parsing tests: simulate ClickHouse JSONEachRow output
    // =========================================================================

    /// Simulate parsing endpoint list from ClickHouse JSONEachRow output.
    /// Verifies that string-encoded numbers are correctly parsed.
    #[test]
    fn test_parse_endpoints_response() {
        // This is what ClickHouse actually returns for the endpoints query
        let ch_lines = vec![
            r#"{"method":"GET","path":"/api/projects/abc/warehouse/sources","request_count":"24","error_count":"0","error_rate":0,"avg_latency":5.123,"p99_latency":6.456}"#,
            r#"{"method":"POST","path":"/api/projects/abc/warehouse/query","request_count":"17","error_count":"3","error_rate":0.17647058823529413,"avg_latency":1190.5,"p99_latency":4850.0}"#,
        ];

        let mut endpoints = Vec::new();
        for line in &ch_lines {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            let method = row["method"].as_str().unwrap_or("GET").to_string();
            let path = row["path"].as_str().unwrap_or("").to_string();

            endpoints.push(serde_json::json!({
                "method": method,
                "path": path,
                "requestCount": json_to_f64(&row["request_count"]) as u64,
                "errorRate": json_to_f64(&row["error_rate"]),
                "avgLatency": json_to_f64(&row["avg_latency"]),
                "p99Latency": json_to_f64(&row["p99_latency"]),
            }));
        }

        assert_eq!(endpoints.len(), 2);

        // First endpoint: GET with 24 requests, no errors
        assert_eq!(endpoints[0]["method"], "GET");
        assert_eq!(endpoints[0]["requestCount"], 24);
        assert_eq!(endpoints[0]["errorRate"], 0.0);
        assert!((endpoints[0]["avgLatency"].as_f64().unwrap() - 5.123).abs() < 0.001);

        // Second endpoint: POST with 17 requests, 3 errors (~17.6% error rate)
        assert_eq!(endpoints[1]["method"], "POST");
        assert_eq!(endpoints[1]["requestCount"], 17);
        assert!(endpoints[1]["errorRate"].as_f64().unwrap() > 0.17);
        assert!(endpoints[1]["avgLatency"].as_f64().unwrap() > 1000.0);
        assert!(endpoints[1]["p99Latency"].as_f64().unwrap() > 4000.0);
    }

    /// Simulate parsing errors response from ClickHouse.
    #[test]
    fn test_parse_errors_response() {
        let ch_lines = vec![
            r#"{"method":"POST","path":"/api/projects/abc/warehouse/query","http_code":"400","message":"http.request","error_count":"3","last_seen":"2026-02-14 22:51:51.000000000"}"#,
        ];

        let mut errors = Vec::new();
        for (i, line) in ch_lines.iter().enumerate() {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            errors.push(serde_json::json!({
                "id": format!("err-{}", i),
                "method": row["method"].as_str().unwrap_or("GET"),
                "path": row["path"].as_str().unwrap_or(""),
                "statusCode": json_to_f64(&row["http_code"]) as u64,
                "message": row["message"].as_str().unwrap_or(""),
                "count": json_to_f64(&row["error_count"]) as u64,
                "lastSeen": row["last_seen"].as_str().unwrap_or(""),
            }));
        }

        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0]["method"], "POST");
        assert_eq!(errors[0]["statusCode"], 400);
        assert_eq!(errors[0]["count"], 3);
        assert_eq!(errors[0]["lastSeen"], "2026-02-14 22:51:51.000000000");
    }

    /// Simulate parsing summary response from ClickHouse.
    #[test]
    fn test_parse_summary_response() {
        // ClickHouse returns count() as a string, but floats as numbers
        let ch_line = r#"{"total_requests":"41","error_count":"22","error_rate":0.5365853658536586,"avg_latency":563.722,"p99_latency":4850.123}"#;
        let row: serde_json::Value = serde_json::from_str(ch_line).unwrap();

        let summary = serde_json::json!({
            "totalRequests": json_to_f64(&row["total_requests"]) as u64,
            "avgLatency": json_to_f64(&row["avg_latency"]),
            "errorRate": json_to_f64(&row["error_rate"]),
            "p99Latency": json_to_f64(&row["p99_latency"]),
        });

        assert_eq!(summary["totalRequests"], 41);
        assert!((summary["avgLatency"].as_f64().unwrap() - 563.722).abs() < 0.001);
        assert!((summary["errorRate"].as_f64().unwrap() - 0.5366).abs() < 0.01);
        assert!(summary["p99Latency"].as_f64().unwrap() > 4800.0);
    }

    /// Simulate parsing status code distribution from ClickHouse.
    #[test]
    fn test_parse_status_code_distribution() {
        let ch_lines = vec![
            r#"{"code":"200","cnt":"19"}"#,
            r#"{"code":"400","cnt":"3"}"#,
            r#"{"code":"500","cnt":"2"}"#,
        ];

        let mut status_codes = Vec::new();
        for line in &ch_lines {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            status_codes.push(serde_json::json!({
                "code": json_to_f64(&row["code"]) as u64,
                "count": json_to_f64(&row["cnt"]) as u64,
            }));
        }

        assert_eq!(status_codes.len(), 3);
        assert_eq!(status_codes[0]["code"], 200);
        assert_eq!(status_codes[0]["count"], 19);
        assert_eq!(status_codes[1]["code"], 400);
        assert_eq!(status_codes[1]["count"], 3);
        assert_eq!(status_codes[2]["code"], 500);
        assert_eq!(status_codes[2]["count"], 2);
    }

    /// Simulate parsing request volume time series from ClickHouse.
    #[test]
    fn test_parse_request_volume() {
        let ch_lines = vec![
            r#"{"ts":"2026-02-14 22:15:00","cnt":"5","errors":"1"}"#,
            r#"{"ts":"2026-02-14 22:18:00","cnt":"12","errors":"0"}"#,
            r#"{"ts":"2026-02-14 22:21:00","cnt":"8","errors":"2"}"#,
        ];

        let mut volume = Vec::new();
        for line in &ch_lines {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            volume.push(serde_json::json!({
                "timestamp": row["ts"].as_str().unwrap_or(""),
                "requests": json_to_f64(&row["cnt"]) as u64,
                "errors": json_to_f64(&row["errors"]) as u64,
            }));
        }

        assert_eq!(volume.len(), 3);
        assert_eq!(volume[0]["timestamp"], "2026-02-14 22:15:00");
        assert_eq!(volume[0]["requests"], 5);
        assert_eq!(volume[0]["errors"], 1);
        assert_eq!(volume[1]["requests"], 12);
        assert_eq!(volume[1]["errors"], 0);
        assert_eq!(volume[2]["requests"], 8);
        assert_eq!(volume[2]["errors"], 2);
    }

    /// Verify that string-encoded zero values are handled correctly.
    #[test]
    fn test_parse_zero_values_as_strings() {
        let ch_line =
            r#"{"request_count":"0","error_rate":"0","avg_latency":"0","p99_latency":"0"}"#;
        let row: serde_json::Value = serde_json::from_str(ch_line).unwrap();

        assert_eq!(json_to_f64(&row["request_count"]) as u64, 0);
        assert_eq!(json_to_f64(&row["error_rate"]), 0.0);
        assert_eq!(json_to_f64(&row["avg_latency"]), 0.0);
        assert_eq!(json_to_f64(&row["p99_latency"]), 0.0);
    }

    /// Verify that large numeric strings from ClickHouse are parsed correctly.
    #[test]
    fn test_parse_large_numbers_as_strings() {
        let v = serde_json::json!("1234567890");
        assert_eq!(json_to_f64(&v) as u64, 1234567890);
    }
}
