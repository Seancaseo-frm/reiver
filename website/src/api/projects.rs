use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::Json,
    routing::{delete, get, patch, post},
    Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::app_state::WebsiteState;
use crate::audit::{AuditCaller, AuditEventBuilder, AuditEventType, AuditOrigin};
use crate::auth::{
    authenticate_and_verify_project, authenticate_request_or_api_key, verify_project_access,
    verify_project_access_with_role, AuthIdentity,
};
use crate::db::DbPool;
use crate::error::{AppError, Result};
use crate::models::{Project, ProjectStats};
use crate::query_cache::{get_cached_query, set_cached_query, CacheTTL};
use crate::rate_limit::RateLimitType;
use axum::http::HeaderMap;
use bb8_redis::redis::AsyncCommands;

#[derive(Debug, Deserialize)]
struct CreateProjectRequest {
    name: String,
    organization_id: Option<Uuid>,
}

#[derive(Debug, Deserialize)]
struct UpdateProjectRequest {
    name: Option<String>,
    pii_masking_enabled: Option<bool>,
    span_metrics_enabled: Option<bool>,
}

/// Returned once on creation -- includes the full key.
#[derive(Debug, Serialize)]
struct ProjectKeyCreatedResponse {
    id: Uuid,
    key: String,
    label: Option<String>,
    scopes: Vec<String>,
    key_type: String,
    expires_at: Option<chrono::DateTime<Utc>>,
    created_at: chrono::DateTime<Utc>,
}

/// Returned by list/get endpoints -- key is masked.
#[derive(Debug, Serialize, sqlx::FromRow)]
struct ProjectKeyListResponse {
    id: Uuid,
    key_prefix: Option<String>,
    label: Option<String>,
    scopes: serde_json::Value,
    key_type: String,
    expires_at: Option<chrono::DateTime<Utc>>,
    created_by: Option<Uuid>,
    created_at: chrono::DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct CreateKeyRequest {
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    scopes: Vec<String>,
    #[serde(default)]
    expires_at: Option<chrono::DateTime<Utc>>,
    #[serde(default = "default_key_type")]
    key_type: String,
}

fn default_key_type() -> String {
    "sdk".to_string()
}

fn generate_project_slug() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();
    (0..8)
        .map(|_| CHARSET[rng.gen_range(0..CHARSET.len())] as char)
        .collect()
}

#[derive(Debug, Deserialize)]
struct ListKeysQuery {
    key_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct UpdateKeyRequest {
    label: Option<String>,
    scopes: Option<Vec<String>>,
    expires_at: Option<chrono::DateTime<Utc>>,
}

pub fn create_projects_router() -> Router<Arc<WebsiteState>> {
    Router::new()
        .route("/", post(create_project))
        .route("/", get(list_projects))
        .route("/{id}", get(get_project))
        .route("/{id}", patch(update_project))
        .route("/{id}", delete(delete_project))
        .route("/{id}/keys", post(create_project_key))
        .route("/{id}/keys", get(list_project_keys))
        .route("/{id}/keys/{key_id}", patch(update_project_key))
        .route("/{id}/keys/{key_id}", delete(delete_project_key))
        .route("/{id}/stats", get(get_project_stats))
        .route("/{id}/change-tracking", get(get_change_tracking))
        .route("/{id}/feature-flags", get(list_feature_flags))
        .route(
            "/{id}/feature-flags/{flag_id}/changes",
            get(get_flag_changes),
        )
        .route("/{id}/feature-flags/{flag_id}/usage", get(get_flag_usage))
        .route("/{id}/entitlements", get(get_project_entitlements))
}

async fn create_project(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Json(payload): Json<CreateProjectRequest>,
) -> Result<Json<Project>> {
    let user_id = match authenticate_request_or_api_key(&headers, &state, RateLimitType::Crud).await? {
        AuthIdentity::User(uid) => uid,
        AuthIdentity::ApiKey { .. } => {
            return Err(AppError::Auth("Creating projects requires user authentication".into()));
        }
    };

    let organization_id = if let Some(requested_org_id) = payload.organization_id {
        // Verify the user has an active membership in the requested org
        let has_membership: Option<(Uuid,)> = sqlx::query_as(
            "SELECT organization_id FROM memberships WHERE user_id = $1 AND organization_id = $2 AND status = 'active'"
        )
        .bind(user_id)
        .bind(requested_org_id)
        .fetch_optional(&*db)
        .await?;

        if has_membership.is_none() {
            return Err(AppError::Auth(
                "Not a member of the specified organization".to_string(),
            ));
        }
        requested_org_id
    } else {
        // Fallback: use the first active org or create a new one
        let existing_org_id: Option<Uuid> = sqlx::query_scalar(
            "SELECT organization_id FROM memberships WHERE user_id = $1 AND status = 'active' LIMIT 1"
        )
        .bind(user_id)
        .fetch_optional(&*db)
        .await?;

        if let Some(org_id) = existing_org_id {
            org_id
        } else {
            let provision =
                reiver_core::org_provision::default_org_provision_for_user(&db, user_id).await?;

            let free_tier_id: Uuid = sqlx::query_scalar(
                "SELECT id FROM tier_definitions WHERE name = 'free'",
            )
            .fetch_one(&*db)
            .await?;

            // Try with domain first; if it conflicts (another org already has it), retry personal name without domain
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
                        sqlx::query_scalar(
                            "INSERT INTO organizations (name, tier_definition_id) VALUES ($1, $2) RETURNING id",
                        )
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
        }
    };

    let current_project_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM projects WHERE organization_id = $1",
    )
    .bind(organization_id)
    .fetch_one(&*db)
    .await?;

    let tier = state
        .entitlements
        .get_config(organization_id)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("{}", e)))?;
    let limit = tier.config.platform.max_projects;
    if limit >= 0 && current_project_count >= limit {
        return Err(AppError::Forbidden(
            "Project limit reached for your current plan. Upgrade to create more projects.".into(),
        ));
    }

    let slug = generate_project_slug();
    let project = sqlx::query_as::<_, Project>(
        "INSERT INTO projects (name, organization_id, created_by, slug) VALUES ($1, $2, $3, $4) RETURNING *"
    )
    .bind(&payload.name)
    .bind(organization_id)
    .bind(user_id)
    .bind(&slug)
    .fetch_one(&*db)
    .await?;

    // Create default project key with full access scopes
    let key = generate_api_key();
    let key_hash = crate::utils::hash_api_key(&key);
    let key_encrypted = state
        .encryptor
        .encrypt(&key)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Encryption error: {}", e)))?;
    let key_prefix = key
        .chars()
        .rev()
        .take(4)
        .collect::<String>()
        .chars()
        .rev()
        .collect::<String>();
    let all_scopes = serde_json::to_value(
        reiver_mcp::scope::ALL_SCOPES
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>(),
    )
    .unwrap_or_default();
    sqlx::query(
        "INSERT INTO project_keys (project_id, key, key_hash, label, created_by, scopes, key_type, key_prefix) VALUES ($1, $2, $3, 'Default', $4, $5, 'sdk', $6)"
    )
    .bind(&project.id)
    .bind(&key_encrypted)
    .bind(&key_hash)
    .bind(user_id)
    .bind(&all_scopes)
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

#[derive(Debug, Deserialize)]
struct ListProjectsQuery {
    organization_id: Option<Uuid>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
struct ProjectWithOrg {
    id: Uuid,
    organization_id: Uuid,
    name: String,
    slug: String,
    created_by: Option<Uuid>,
    created_at: chrono::DateTime<Utc>,
    settings: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    github_repo_url: Option<String>,
    organization_name: String,
}

async fn list_projects(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Query(query): Query<ListProjectsQuery>,
) -> Result<Json<Vec<ProjectWithOrg>>> {
    match authenticate_request_or_api_key(&headers, &state, RateLimitType::Crud).await? {
        AuthIdentity::User(user_id) => {
            let projects = if let Some(org_id) = query.organization_id {
                sqlx::query_as::<_, ProjectWithOrg>(
                    r#"
                    SELECT DISTINCT p.id, p.organization_id, p.name, p.slug, p.created_by, p.created_at,
                           p.settings, p.github_repo_url, o.name as organization_name
                    FROM projects p
                    INNER JOIN memberships m ON p.organization_id = m.organization_id
                    INNER JOIN organizations o ON p.organization_id = o.id
                    WHERE m.user_id = $1 AND m.status = 'active' AND p.organization_id = $2
                    ORDER BY p.created_at DESC
                    "#,
                )
                .bind(user_id)
                .bind(org_id)
                .fetch_all(&*db)
                .await?
            } else {
                sqlx::query_as::<_, ProjectWithOrg>(
                    r#"
                    SELECT DISTINCT p.id, p.organization_id, p.name, p.slug, p.created_by, p.created_at,
                           p.settings, p.github_repo_url, o.name as organization_name
                    FROM projects p
                    INNER JOIN memberships m ON p.organization_id = m.organization_id
                    INNER JOIN organizations o ON p.organization_id = o.id
                    WHERE m.user_id = $1 AND m.status = 'active'
                    ORDER BY p.created_at DESC
                    "#,
                )
                .bind(user_id)
                .fetch_all(&*db)
                .await?
            };
            Ok(Json(projects))
        }
        AuthIdentity::ApiKey { project_id } => {
            let projects = sqlx::query_as::<_, ProjectWithOrg>(
                r#"
                SELECT p.id, p.organization_id, p.name, p.slug, p.created_by, p.created_at,
                       p.settings, p.github_repo_url, o.name as organization_name
                FROM projects p
                INNER JOIN organizations o ON p.organization_id = o.id
                WHERE p.id = $1
                "#,
            )
            .bind(project_id)
            .fetch_all(&*db)
            .await?;
            Ok(Json(projects))
        }
    }
}

async fn get_project(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(id_or_slug): Path<String>,
) -> Result<Json<Project>> {
    match authenticate_request_or_api_key(&headers, &state, RateLimitType::Crud).await? {
        AuthIdentity::User(user_id) => {
            let project = if let Ok(uuid) = id_or_slug.parse::<Uuid>() {
                verify_project_access(&state.db, uuid, user_id).await?
            } else {
                resolve_project_by_slug(&state.db, &id_or_slug, user_id).await?
            };
            Ok(Json(project))
        }
        AuthIdentity::ApiKey { project_id } => {
            let lookup_id = id_or_slug
                .parse::<Uuid>()
                .map_err(|_| AppError::Auth("API keys can only access projects by ID".into()))?;
            if lookup_id != project_id {
                return Err(AppError::Auth(
                    "API key does not belong to this project".into(),
                ));
            }
            let project = sqlx::query_as::<_, Project>("SELECT * FROM projects WHERE id = $1")
                .bind(project_id)
                .fetch_optional(&*state.db)
                .await?
                .ok_or_else(|| AppError::NotFound("Project not found".into()))?;
            Ok(Json(project))
        }
    }
}

async fn get_project_entitlements(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(id_or_slug): Path<String>,
) -> Result<Json<serde_json::Value>> {
    let project = match authenticate_request_or_api_key(&headers, &state, RateLimitType::Crud).await? {
        AuthIdentity::User(user_id) => {
            if let Ok(uuid) = id_or_slug.parse::<Uuid>() {
                verify_project_access(&state.db, uuid, user_id).await?
            } else {
                resolve_project_by_slug(&state.db, &id_or_slug, user_id).await?
            }
        }
        AuthIdentity::ApiKey { project_id } => {
            sqlx::query_as::<_, Project>("SELECT * FROM projects WHERE id = $1")
                .bind(project_id)
                .fetch_optional(&*state.db)
                .await?
                .ok_or_else(|| AppError::NotFound("Project not found".into()))?
        }
    };

    let tier = state
        .entitlements
        .get_config(project.organization_id)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("{}", e)))?;

    Ok(Json(serde_json::json!({
        "name": tier.name,
        "display_name": tier.display_name,
        "stripe_price_id": tier.stripe_price_id,
        "config": tier.config,
    })))
}

async fn update_project(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Path(project_id): Path<Uuid>,
    Json(payload): Json<UpdateProjectRequest>,
) -> Result<Json<Project>> {
    let user_id = authenticate_and_verify_project(&headers, &state, &state.db, project_id, RateLimitType::Crud).await?;

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

    let before_project = sqlx::query_as::<_, Project>("SELECT * FROM projects WHERE id = $1")
        .bind(project_id)
        .fetch_one(&*db)
        .await?;
    let before_settings = before_project.settings.as_ref();
    let before_pii = before_settings
        .and_then(|s| s.get("pii_masking_enabled"))
        .and_then(|v| v.as_bool());
    let before_span_metrics = before_settings
        .and_then(|s| s.get("span_metrics_enabled"))
        .and_then(|v| v.as_bool());

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

    let after_settings = updated_project.settings.as_ref();
    let after_pii = after_settings
        .and_then(|s| s.get("pii_masking_enabled"))
        .and_then(|v| v.as_bool());
    let after_span_metrics = after_settings
        .and_then(|s| s.get("span_metrics_enabled"))
        .and_then(|v| v.as_bool());
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::ProjectUpdated)
        .actor(user_id)
        .organization(updated_project.organization_id)
        .resource("project", project_id)
        .details(serde_json::json!({
            "before": { "name": &before_project.name, "pii_masking_enabled": before_pii, "span_metrics_enabled": before_span_metrics },
            "after": { "name": &updated_project.name, "pii_masking_enabled": after_pii, "span_metrics_enabled": after_span_metrics },
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

    Ok(Json(updated_project))
}

async fn delete_project(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Path(project_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>> {
    let user_id = match authenticate_request_or_api_key(&headers, &state, RateLimitType::Crud).await? {
        AuthIdentity::User(uid) => uid,
        AuthIdentity::ApiKey { .. } => {
            return Err(AppError::Auth("Deleting projects requires user authentication via the UI".into()));
        }
    };
    verify_project_access(&state.db, project_id, user_id).await?;

    let before_name: Option<String> = sqlx::query_scalar("SELECT name FROM projects WHERE id = $1")
        .bind(project_id)
        .fetch_optional(&*db)
        .await?;

    let organization_id =
        sqlx::query_scalar::<_, Uuid>("SELECT organization_id FROM projects WHERE id = $1")
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

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::ProjectDeleted)
        .actor(user_id)
        .resource("project", project_id)
        .details(serde_json::json!({ "deleted": { "name": before_name } }))
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
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Path(project_id): Path<Uuid>,
    Json(payload): Json<CreateKeyRequest>,
) -> Result<Json<ProjectKeyCreatedResponse>> {
    let user_id = authenticate_and_verify_project(&headers, &state, &state.db, project_id, RateLimitType::Crud).await?;
    let (project, role) = if !user_id.is_nil() {
        let (p, r) = verify_project_access_with_role(&state.db, project_id, user_id).await?;
        (Some(p), r)
    } else {
        (None, "admin".to_string())
    };

    if !["sdk", "agent"].contains(&payload.key_type.as_str()) {
        return Err(AppError::Validation(
            "key_type must be 'sdk' or 'agent'".into(),
        ));
    }

    if payload.scopes.is_empty() {
        return Err(AppError::Validation(
            "scopes must contain at least one scope".into(),
        ));
    }
    if let Some(ref expires_at) = payload.expires_at {
        if *expires_at <= Utc::now() {
            return Err(AppError::Validation(
                "expires_at must be in the future".into(),
            ));
        }
    }
    if let Err(e) = reiver_mcp::scope::validate_scope_names(&payload.scopes) {
        return Err(AppError::Validation(e));
    }
    if let Err(e) = reiver_mcp::scope::validate_scopes_within_ceiling(&payload.scopes, &role) {
        return Err(AppError::Validation(e));
    }

    let key = generate_api_key();
    let key_hash = crate::utils::hash_api_key(&key);
    let key_encrypted = state
        .encryptor
        .encrypt(&key)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Encryption error: {}", e)))?;
    let key_prefix = key
        .chars()
        .rev()
        .take(4)
        .collect::<String>()
        .chars()
        .rev()
        .collect::<String>();
    let scopes_json = serde_json::to_value(&payload.scopes).unwrap_or_default();

    #[derive(sqlx::FromRow)]
    struct InsertedKey {
        id: Uuid,
        created_at: chrono::DateTime<Utc>,
    }

    let created_by: Option<Uuid> = if user_id.is_nil() { None } else { Some(user_id) };

    let inserted: InsertedKey = sqlx::query_as(
        r#"INSERT INTO project_keys (project_id, key, key_hash, label, created_by, scopes, expires_at, key_type, key_prefix)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         RETURNING id, created_at"#
    )
    .bind(project_id)
    .bind(&key_encrypted)
    .bind(&key_hash)
    .bind(&payload.label)
    .bind(created_by)
    .bind(&scopes_json)
    .bind(payload.expires_at)
    .bind(&payload.key_type)
    .bind(&key_prefix)
    .fetch_one(&*db)
    .await?;

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::ApiKeyCreated)
        .actor(user_id)
        .resource("api_key", inserted.id)
        .details(serde_json::json!({
            "created": {
                "key_type": &payload.key_type,
                "label": &payload.label,
            }
        }));
    if let Some(ref p) = project {
        audit = audit.organization(p.organization_id);
    }
    audit
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

    Ok(Json(ProjectKeyCreatedResponse {
        id: inserted.id,
        key,
        label: payload.label,
        scopes: payload.scopes,
        key_type: payload.key_type,
        expires_at: payload.expires_at,
        created_at: inserted.created_at,
    }))
}

async fn list_project_keys(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Path(project_id): Path<Uuid>,
    Query(query): Query<ListKeysQuery>,
) -> Result<Json<Vec<ProjectKeyListResponse>>> {
    authenticate_and_verify_project(&headers, &state, &state.db, project_id, RateLimitType::Crud).await?;

    let keys = if let Some(kt) = &query.key_type {
        sqlx::query_as::<_, ProjectKeyListResponse>(
            "SELECT id, key_prefix, label, scopes, key_type, expires_at, created_by, created_at
             FROM project_keys
             WHERE project_id = $1 AND key_type = $2
             ORDER BY created_at DESC",
        )
        .bind(project_id)
        .bind(kt)
        .fetch_all(&*db)
        .await?
    } else {
        sqlx::query_as::<_, ProjectKeyListResponse>(
            "SELECT id, key_prefix, label, scopes, key_type, expires_at, created_by, created_at
             FROM project_keys
             WHERE project_id = $1
             ORDER BY created_at DESC",
        )
        .bind(project_id)
        .fetch_all(&*db)
        .await?
    };

    Ok(Json(keys))
}

async fn update_project_key(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Path((project_id, key_id)): Path<(Uuid, Uuid)>,
    Json(payload): Json<UpdateKeyRequest>,
) -> Result<Json<serde_json::Value>> {
    let user_id = authenticate_and_verify_project(&headers, &state, &state.db, project_id, RateLimitType::Crud).await?;
    let (project, role) = if !user_id.is_nil() {
        let (p, r) = verify_project_access_with_role(&state.db, project_id, user_id).await?;
        (Some(p), r)
    } else {
        (None, "admin".to_string())
    };

    if let Some(ref scopes) = payload.scopes {
        if scopes.is_empty() {
            return Err(AppError::Validation(
                "scopes must contain at least one scope".into(),
            ));
        }
        if let Err(e) = reiver_mcp::scope::validate_scope_names(scopes) {
            return Err(AppError::Validation(e));
        }
        if let Err(e) = reiver_mcp::scope::validate_scopes_within_ceiling(scopes, &role) {
            return Err(AppError::Validation(e));
        }
    }

    if let Some(ref expires_at) = payload.expires_at {
        if *expires_at <= Utc::now() {
            return Err(AppError::Validation(
                "expires_at must be in the future".into(),
            ));
        }
    }

    let mut set_clauses = Vec::new();
    let mut param_idx = 3u32; // $1 = key_id, $2 = project_id

    if payload.label.is_some() {
        set_clauses.push(format!("label = ${param_idx}"));
        param_idx += 1;
    }
    if payload.scopes.is_some() {
        set_clauses.push(format!("scopes = ${param_idx}"));
        param_idx += 1;
    }
    if payload.expires_at.is_some() {
        set_clauses.push(format!("expires_at = ${param_idx}"));
        // param_idx += 1; // not needed after last
    }

    if set_clauses.is_empty() {
        return Ok(Json(serde_json::json!({"message": "No fields to update"})));
    }

    #[derive(sqlx::FromRow)]
    struct KeyBefore {
        label: Option<String>,
        scopes: Option<serde_json::Value>,
    }
    let before_key = sqlx::query_as::<_, KeyBefore>(
        "SELECT label, scopes FROM project_keys WHERE id = $1 AND project_id = $2",
    )
    .bind(key_id)
    .bind(project_id)
    .fetch_one(&*db)
    .await?;

    let sql = format!(
        "UPDATE project_keys SET {} WHERE id = $1 AND project_id = $2",
        set_clauses.join(", ")
    );

    let mut query = sqlx::query(&sql).bind(key_id).bind(project_id);

    if let Some(ref label) = payload.label {
        query = query.bind(label);
    }
    if let Some(ref scopes) = payload.scopes {
        let scopes_json = serde_json::to_value(scopes).unwrap_or_default();
        query = query.bind(scopes_json);
    }
    if let Some(ref expires_at) = payload.expires_at {
        query = query.bind(expires_at);
    }

    let result = query.execute(&*db).await?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound("API key not found".into()));
    }

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::ApiKeyUpdated)
        .actor(user_id)
        .resource("api_key", key_id)
        .details(serde_json::json!({
            "before": { "label": before_key.label, "scopes": before_key.scopes },
            "after": { "label": payload.label.as_deref().unwrap_or_else(|| before_key.label.as_deref().unwrap_or_default()), "scopes": payload.scopes.as_ref().map(|s| serde_json::to_value(s).unwrap_or_default()).unwrap_or_else(|| before_key.scopes.clone().unwrap_or_default()) },
        }));
    if let Some(ref p) = project {
        audit = audit.organization(p.organization_id);
    }
    audit
        .origin(&audit_origin.origin_type, &audit_origin.origin_ref, &audit_origin.origin_reason)
        .caller(&audit_caller.caller_type, &audit_caller.key_label, &audit_caller.key_prefix)
        .success()
        .log(&state.clickhouse)
        .await;

    Ok(Json(serde_json::json!({"message": "Key updated"})))
}

async fn delete_project_key(
    State(state): State<Arc<WebsiteState>>,
    Extension(db): Extension<Arc<DbPool>>,
    headers: HeaderMap,
    Path((project_id, key_id)): Path<(Uuid, Uuid)>,
) -> Result<(StatusCode, ())> {
    let user_id = authenticate_and_verify_project(&headers, &state, &state.db, project_id, RateLimitType::Crud).await?;
    let organization_id: Uuid = sqlx::query_scalar("SELECT organization_id FROM projects WHERE id = $1")
        .bind(project_id)
        .fetch_optional(&*state.db)
        .await?
        .ok_or_else(|| AppError::NotFound("Project not found".into()))?;

    let key_hash: Option<String> =
        sqlx::query_scalar("SELECT key_hash FROM project_keys WHERE id = $1 AND project_id = $2")
            .bind(key_id)
            .bind(project_id)
            .fetch_optional(&*db)
            .await?;

    let key_hash = key_hash.ok_or_else(|| AppError::NotFound("API key not found".to_string()))?;

    crate::utils::invalidate_project_key_cache(&state.redis, &key_hash).await?;

    let scopes_cache_key = format!("key_scopes:{}", key_hash);
    if let Ok(mut conn) = state.redis.get().await {
        let _ = bb8_redis::redis::AsyncCommands::del::<_, ()>(&mut *conn, &scopes_cache_key).await;
    }

    let result = sqlx::query("DELETE FROM project_keys WHERE id = $1 AND project_id = $2")
        .bind(key_id)
        .bind(project_id)
        .execute(&*db)
        .await?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound("API key not found".to_string()));
    }

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::ApiKeyDeleted)
        .actor(user_id)
        .organization(organization_id)
        .resource("api_key", key_id)
        .details(serde_json::json!({ "deleted": { "project_id": project_id } }))
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

    Ok((StatusCode::NO_CONTENT, ()))
}

async fn get_project_stats(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(project_id): Path<Uuid>,
) -> Result<Json<ProjectStats>> {
    authenticate_and_verify_project(&headers, &state, &state.db, project_id, RateLimitType::Analytics).await?;

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

        // Cache result (medium TTL - counts change frequently but not every second)
        let _ = set_cached_query(&state.redis, query, &params, &count, CacheTTL::Medium).await;
        count
    };

    // Count distinct groups with unresolved status (cached)
    let query_unresolved = "SELECT count() FROM (SELECT fingerprint FROM reiver.exceptions WHERE project_id = ? GROUP BY fingerprint HAVING anyLast(status) = 'unresolved')";
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
        let _ = set_cached_query(
            &state.redis,
            query_unresolved,
            &params,
            &count,
            CacheTTL::Medium,
        )
        .await;
        count
    };

    // Count distinct groups with resolved status (cached)
    let query_resolved = "SELECT count() FROM (SELECT fingerprint FROM reiver.exceptions WHERE project_id = ? GROUP BY fingerprint HAVING anyLast(status) = 'resolved')";
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
        let _ = set_cached_query(
            &state.redis,
            query_resolved,
            &params,
            &count,
            CacheTTL::Medium,
        )
        .await;
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
    State(state): State<Arc<WebsiteState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<ChangeTrackingQuery>,
    headers: HeaderMap,
) -> Result<Json<Vec<ChangeTrackingEvent>>> {
    authenticate_and_verify_project(&headers, &state, &state.db, project_id, RateLimitType::Analytics).await?;

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
    State(state): State<Arc<WebsiteState>>,
    Path(project_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Json<Vec<serde_json::Value>>> {
    authenticate_and_verify_project(&headers, &state, &state.db, project_id, RateLimitType::Analytics).await?;

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
    State(state): State<Arc<WebsiteState>>,
    Path((project_id, flag_id)): Path<(Uuid, String)>,
    headers: HeaderMap,
) -> Result<Json<Vec<ChangeTrackingEvent>>> {
    authenticate_and_verify_project(&headers, &state, &state.db, project_id, RateLimitType::Analytics).await?;

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
    State(state): State<Arc<WebsiteState>>,
    Path((project_id, flag_id)): Path<(Uuid, String)>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>> {
    authenticate_and_verify_project(&headers, &state, &state.db, project_id, RateLimitType::Analytics).await?;

    // Query ClickHouse for usage stats
    let lookback_start = (chrono::Utc::now() - chrono::Duration::days(7)).to_rfc3339();
    let safe_flag_id = crate::utils::escape_clickhouse_string(&flag_id);

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
        project_id, lookback_start, safe_flag_id
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

fn generate_api_key() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();
    let random_part: String = (0..32)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect();
    format!("dh_{random_part}")
}

async fn resolve_project_by_slug(db: &DbPool, slug: &str, user_id: Uuid) -> Result<Project> {
    sqlx::query_as::<_, Project>(
        r#"SELECT DISTINCT p.* FROM projects p
        INNER JOIN memberships m ON p.organization_id = m.organization_id
        WHERE p.slug = $1 AND m.user_id = $2 AND m.status = 'active'"#,
    )
    .bind(slug)
    .bind(user_id)
    .fetch_optional(db)
    .await?
    .ok_or_else(|| AppError::NotFound("Project not found or access denied".to_string()))
}
