//! SCIM 2.0 API endpoints for enterprise user provisioning
//!
//! Implements the SCIM 2.0 specification (RFC 7643/7644) for:
//! - User provisioning (create, read, update, delete)
//! - Group management and role mapping
//!
//! Identity providers like Okta, Azure AD, OneLogin push user changes here.

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::Json,
    routing::{delete, get},
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{info, warn};
use uuid::Uuid;

use crate::app_state::WebsiteState;
use crate::audit::{AuditCaller, AuditEventBuilder, AuditEventType, AuditOrigin};
use crate::auth::authenticate_request;
use crate::authorization::require_any_org_admin;
use crate::error::{AppError, Result};
use crate::rate_limit::RateLimitType;

pub fn create_scim_router() -> Router<Arc<WebsiteState>> {
    Router::new()
        // SCIM Discovery endpoints
        .route("/ServiceProviderConfig", get(get_service_provider_config))
        .route("/Schemas", get(get_schemas))
        .route("/ResourceTypes", get(get_resource_types))
        // User endpoints
        .route("/Users", get(list_users).post(create_user))
        .route(
            "/Users/{id}",
            get(get_user)
                .put(replace_user)
                .patch(update_user)
                .delete(delete_user),
        )
        // Group endpoints
        .route("/Groups", get(list_groups).post(create_group))
        .route(
            "/Groups/{id}",
            get(get_group)
                .put(replace_group)
                .patch(update_group)
                .delete(delete_group),
        )
        // Group mappings (admin)
        .route(
            "/GroupMappings",
            get(list_group_mappings).post(create_group_mapping),
        )
        .route("/GroupMappings/{id}", delete(delete_group_mapping))
}

/// Settings admin router mounted at `/api/settings/scim`.
pub fn create_scim_settings_router() -> Router<Arc<WebsiteState>> {
    Router::new()
        .route("/token", get(get_token_status).post(rotate_token))
        .route("/users", get(list_provisioned_users))
}

// ============================================================================
// SCIM Types
// ============================================================================

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScimUser {
    pub schemas: Vec<String>,
    pub id: Option<String>,
    pub external_id: Option<String>,
    pub user_name: String,
    pub name: Option<ScimName>,
    pub display_name: Option<String>,
    pub emails: Option<Vec<ScimEmail>>,
    pub active: Option<bool>,
    pub groups: Option<Vec<ScimGroupRef>>,
    pub meta: Option<ScimMeta>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScimName {
    pub formatted: Option<String>,
    pub family_name: Option<String>,
    pub given_name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ScimEmail {
    pub value: String,
    pub primary: Option<bool>,
    #[serde(rename = "type")]
    pub email_type: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ScimGroupRef {
    pub value: String,
    pub display: Option<String>,
    #[serde(rename = "$ref")]
    pub ref_url: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScimMeta {
    pub resource_type: String,
    pub created: Option<String>,
    pub last_modified: Option<String>,
    pub location: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScimGroup {
    pub schemas: Vec<String>,
    pub id: Option<String>,
    pub display_name: String,
    pub members: Option<Vec<ScimMember>>,
    pub meta: Option<ScimMeta>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ScimMember {
    pub value: String,
    pub display: Option<String>,
    #[serde(rename = "$ref")]
    pub ref_url: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScimListResponse<T> {
    pub schemas: Vec<String>,
    pub total_results: i64,
    pub start_index: Option<i64>,
    pub items_per_page: Option<i64>,
    #[serde(rename = "Resources")]
    pub resources: Vec<T>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)] // Used for SCIM protocol error responses
pub struct ScimError {
    pub schemas: Vec<String>,
    pub status: String,
    pub detail: Option<String>,
    pub scim_type: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)] // schemas field required by SCIM spec for deserialization
pub struct ScimPatchOp {
    pub schemas: Vec<String>,
    #[serde(rename = "Operations")]
    pub operations: Vec<PatchOperation>,
}

#[derive(Debug, Deserialize)]
pub struct PatchOperation {
    pub op: String, // add, remove, replace
    pub path: Option<String>,
    pub value: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)] // filter field for future SCIM filter support
pub struct ScimListQuery {
    pub filter: Option<String>,
    #[serde(rename = "startIndex")]
    pub start_index: Option<i64>,
    pub count: Option<i64>,
}

// ============================================================================
// SCIM Discovery Endpoints
// ============================================================================

/// GET /scim/v2/ServiceProviderConfig
async fn get_service_provider_config() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServiceProviderConfig"],
        "documentationUri": "https://docs.reiver.dev/scim",
        "patch": { "supported": true },
        "bulk": { "supported": false, "maxOperations": 0, "maxPayloadSize": 0 },
        "filter": { "supported": true, "maxResults": 100 },
        "changePassword": { "supported": false },
        "sort": { "supported": false },
        "etag": { "supported": false },
        "authenticationSchemes": [{
            "name": "OAuth Bearer Token",
            "description": "Authentication scheme using the OAuth Bearer Token Standard",
            "specUri": "http://www.rfc-editor.org/info/rfc6750",
            "type": "oauthbearertoken",
            "primary": true
        }]
    }))
}

/// GET /scim/v2/Schemas
async fn get_schemas() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],
        "totalResults": 2,
        "Resources": [
            {
                "id": "urn:ietf:params:scim:schemas:core:2.0:User",
                "name": "User",
                "description": "User Account",
                "attributes": [
                    { "name": "userName", "type": "string", "required": true, "uniqueness": "server" },
                    { "name": "name", "type": "complex", "required": false },
                    { "name": "displayName", "type": "string", "required": false },
                    { "name": "emails", "type": "complex", "multiValued": true, "required": false },
                    { "name": "active", "type": "boolean", "required": false }
                ]
            },
            {
                "id": "urn:ietf:params:scim:schemas:core:2.0:Group",
                "name": "Group",
                "description": "Group",
                "attributes": [
                    { "name": "displayName", "type": "string", "required": true },
                    { "name": "members", "type": "complex", "multiValued": true, "required": false }
                ]
            }
        ]
    }))
}

/// GET /scim/v2/ResourceTypes
async fn get_resource_types() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],
        "totalResults": 2,
        "Resources": [
            {
                "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ResourceType"],
                "id": "User",
                "name": "User",
                "endpoint": "/scim/v2/Users",
                "schema": "urn:ietf:params:scim:schemas:core:2.0:User"
            },
            {
                "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ResourceType"],
                "id": "Group",
                "name": "Group",
                "endpoint": "/scim/v2/Groups",
                "schema": "urn:ietf:params:scim:schemas:core:2.0:Group"
            }
        ]
    }))
}

// ============================================================================
// Authentication Helper
// ============================================================================

/// Extract and validate SCIM bearer token, returns SSO config ID
async fn validate_scim_token(state: &WebsiteState, headers: &HeaderMap) -> Result<(Uuid, Uuid)> {
    let auth_header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| AppError::Validation("Missing Authorization header".to_string()))?;

    if !auth_header.starts_with("Bearer ") {
        return Err(AppError::Validation(
            "Invalid Authorization header format".to_string(),
        ));
    }

    let token = &auth_header[7..];

    // Hash the token for comparison
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    let token_hash = format!("{:x}", hasher.finalize());

    // Find SSO config with matching token
    #[derive(sqlx::FromRow)]
    struct ConfigRow {
        id: Uuid,
        organization_id: Uuid,
    }

    let config = sqlx::query_as::<_, ConfigRow>(
        "SELECT id, organization_id FROM sso_configurations WHERE scim_enabled = true AND scim_bearer_token_hash = $1"
    )
    .bind(&token_hash)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?
    .ok_or_else(|| AppError::Validation("Invalid SCIM token".to_string()))?;

    let tier = state.entitlements.get_config(config.organization_id).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("{}", e)))?;
    if !tier.config.platform.sso {
        return Err(AppError::Forbidden("SSO/SCIM is not available on your current plan".into()));
    }

    Ok((config.id, config.organization_id))
}

// ============================================================================
// User Endpoints
// ============================================================================

/// GET /scim/v2/Users
async fn list_users(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Query(query): Query<ScimListQuery>,
) -> Result<Json<ScimListResponse<ScimUser>>> {
    let (sso_config_id, _) = validate_scim_token(&state, &headers).await?;

    let start_index = query.start_index.unwrap_or(1);
    let count = query.count.unwrap_or(100).min(100);
    let offset = (start_index - 1).max(0);

    // Get users provisioned via SCIM for this SSO config
    #[derive(sqlx::FromRow)]
    #[allow(dead_code)] // user_id included in SELECT for potential future use
    struct UserRow {
        user_id: Uuid,
        external_id: String,
        external_email: Option<String>,
        scim_id: Option<String>,
        scim_active: bool,
    }

    let users = sqlx::query_as::<_, UserRow>(
        r#"
        SELECT user_id, external_id, external_email, scim_id, scim_active
        FROM sso_user_mappings
        WHERE sso_config_id = $1 AND provisioned_via_scim = true
        ORDER BY created_at
        LIMIT $2 OFFSET $3
        "#,
    )
    .bind(sso_config_id)
    .bind(count)
    .bind(offset)
    .fetch_all(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    // Get total count
    let total: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM sso_user_mappings WHERE sso_config_id = $1 AND provisioned_via_scim = true"
    )
    .bind(sso_config_id)
    .fetch_one(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let resources: Vec<ScimUser> = users
        .iter()
        .map(|u| ScimUser {
            schemas: vec!["urn:ietf:params:scim:schemas:core:2.0:User".to_string()],
            id: u.scim_id.clone(),
            external_id: Some(u.external_id.clone()),
            user_name: u.external_email.clone().unwrap_or_default(),
            name: None,
            display_name: None,
            emails: u.external_email.as_ref().map(|e| {
                vec![ScimEmail {
                    value: e.clone(),
                    primary: Some(true),
                    email_type: Some("work".to_string()),
                }]
            }),
            active: Some(u.scim_active),
            groups: None,
            meta: Some(ScimMeta {
                resource_type: "User".to_string(),
                created: None,
                last_modified: None,
                location: u
                    .scim_id
                    .as_ref()
                    .map(|id| format!("/scim/v2/Users/{}", id)),
            }),
        })
        .collect();

    Ok(Json(ScimListResponse {
        schemas: vec!["urn:ietf:params:scim:api:messages:2.0:ListResponse".to_string()],
        total_results: total.0,
        start_index: Some(start_index),
        items_per_page: Some(count),
        resources,
    }))
}

/// POST /scim/v2/Users - Create a new user
async fn create_user(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(payload): Json<ScimUser>,
) -> Result<(StatusCode, Json<ScimUser>)> {
    let (sso_config_id, organization_id) = validate_scim_token(&state, &headers).await?;

    // Extract email
    let email = payload
        .emails
        .as_ref()
        .and_then(|emails| {
            emails
                .iter()
                .find(|e| e.primary.unwrap_or(false))
                .or(emails.first())
        })
        .map(|e| e.value.clone())
        .unwrap_or_else(|| payload.user_name.clone());

    // Extract name
    let name = payload
        .display_name
        .clone()
        .or_else(|| payload.name.as_ref().and_then(|n| n.formatted.clone()))
        .or_else(|| {
            payload.name.as_ref().map(|n| {
                format!(
                    "{} {}",
                    n.given_name.as_deref().unwrap_or(""),
                    n.family_name.as_deref().unwrap_or("")
                )
                .trim()
                .to_string()
            })
        })
        .unwrap_or_else(|| email.clone());

    // Get default role from SSO config
    #[derive(sqlx::FromRow)]
    struct ConfigRow {
        default_role: String,
    }
    let config =
        sqlx::query_as::<_, ConfigRow>("SELECT default_role FROM sso_configurations WHERE id = $1")
            .bind(sso_config_id)
            .fetch_one(&*state.db)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    // Determine role from group mappings if groups provided
    let role = if let Some(ref groups) = payload.groups {
        determine_role_from_groups(&state.db, sso_config_id, groups)
            .await?
            .unwrap_or(config.default_role)
    } else {
        config.default_role
    };

    // Create user in database
    #[derive(sqlx::FromRow)]
    struct UserRow {
        id: Uuid,
    }

    let user = sqlx::query_as::<_, UserRow>(
        r#"
        INSERT INTO users (email, name, password_hash, role, is_approved)
        VALUES ($1, $2, '', $3, true)
        ON CONFLICT (email) DO UPDATE SET
            name = EXCLUDED.name,
            role = EXCLUDED.role,
            is_approved = true
        RETURNING id
        "#,
    )
    .bind(&email)
    .bind(&name)
    .bind(&role)
    .fetch_one(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to create user: {}", e)))?;

    // Generate SCIM ID
    let scim_id = Uuid::new_v4().to_string();

    // Create SSO mapping
    sqlx::query(
        r#"
        INSERT INTO sso_user_mappings (user_id, sso_config_id, external_id, external_email, scim_id, provisioned_via_scim, scim_active)
        VALUES ($1, $2, $3, $4, $5, true, $6)
        ON CONFLICT (sso_config_id, external_id) DO UPDATE SET
            scim_id = EXCLUDED.scim_id,
            scim_active = EXCLUDED.scim_active,
            updated_at = NOW()
        "#
    )
    .bind(user.id)
    .bind(sso_config_id)
    .bind(payload.external_id.as_deref().unwrap_or(&payload.user_name))
    .bind(&email)
    .bind(&scim_id)
    .bind(payload.active.unwrap_or(true))
    .execute(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to create SSO mapping: {}", e)))?;

    info!("SCIM: Created user {} ({})", email, scim_id);

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::ScimUserCreated)
        .organization(organization_id)
        .resource("scim_user", user.id)
        .details(serde_json::json!({ "created": {
            "email": &email,
            "display_name": &name,
            "role": &role,
            "active": payload.active.unwrap_or(true),
        }}))
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

    let response = ScimUser {
        schemas: vec!["urn:ietf:params:scim:schemas:core:2.0:User".to_string()],
        id: Some(scim_id.clone()),
        external_id: payload.external_id,
        user_name: email.clone(),
        name: payload.name,
        display_name: Some(name),
        emails: Some(vec![ScimEmail {
            value: email,
            primary: Some(true),
            email_type: Some("work".to_string()),
        }]),
        active: payload.active.or(Some(true)),
        groups: payload.groups,
        meta: Some(ScimMeta {
            resource_type: "User".to_string(),
            created: Some(chrono::Utc::now().to_rfc3339()),
            last_modified: Some(chrono::Utc::now().to_rfc3339()),
            location: Some(format!("/scim/v2/Users/{}", scim_id)),
        }),
    };

    Ok((StatusCode::CREATED, Json(response)))
}

/// GET /scim/v2/Users/{id}
async fn get_user(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(scim_id): Path<String>,
) -> Result<Json<ScimUser>> {
    let (sso_config_id, _) = validate_scim_token(&state, &headers).await?;

    #[derive(sqlx::FromRow)]
    struct UserRow {
        user_id: Uuid,
        external_id: String,
        external_email: Option<String>,
        scim_active: bool,
        created_at: chrono::DateTime<chrono::Utc>,
        updated_at: chrono::DateTime<chrono::Utc>,
    }

    let mapping = sqlx::query_as::<_, UserRow>(
        "SELECT user_id, external_id, external_email, scim_active, created_at, updated_at 
         FROM sso_user_mappings WHERE sso_config_id = $1 AND scim_id = $2",
    )
    .bind(sso_config_id)
    .bind(&scim_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?
    .ok_or_else(|| AppError::NotFound("User not found".to_string()))?;

    // Get user details
    #[derive(sqlx::FromRow)]
    struct UserDetailRow {
        name: Option<String>,
    }

    let user = sqlx::query_as::<_, UserDetailRow>("SELECT name FROM users WHERE id = $1")
        .bind(mapping.user_id)
        .fetch_optional(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    Ok(Json(ScimUser {
        schemas: vec!["urn:ietf:params:scim:schemas:core:2.0:User".to_string()],
        id: Some(scim_id.clone()),
        external_id: Some(mapping.external_id),
        user_name: mapping.external_email.clone().unwrap_or_default(),
        name: user.and_then(|u| u.name).map(|n| ScimName {
            formatted: Some(n),
            family_name: None,
            given_name: None,
        }),
        display_name: None,
        emails: mapping.external_email.map(|e| {
            vec![ScimEmail {
                value: e,
                primary: Some(true),
                email_type: Some("work".to_string()),
            }]
        }),
        active: Some(mapping.scim_active),
        groups: None,
        meta: Some(ScimMeta {
            resource_type: "User".to_string(),
            created: Some(mapping.created_at.to_rfc3339()),
            last_modified: Some(mapping.updated_at.to_rfc3339()),
            location: Some(format!("/scim/v2/Users/{}", scim_id)),
        }),
    }))
}

/// PUT /scim/v2/Users/{id} - Replace user
async fn replace_user(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(scim_id): Path<String>,
    Json(payload): Json<ScimUser>,
) -> Result<Json<ScimUser>> {
    let (sso_config_id, organization_id) = validate_scim_token(&state, &headers).await?;

    // Get existing mapping and before-state for audit
    #[derive(sqlx::FromRow)]
    struct MappingRow {
        user_id: Uuid,
        scim_active: bool,
    }

    let mapping = sqlx::query_as::<_, MappingRow>(
        "SELECT user_id, scim_active FROM sso_user_mappings WHERE sso_config_id = $1 AND scim_id = $2"
    )
    .bind(sso_config_id)
    .bind(&scim_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?
    .ok_or_else(|| AppError::NotFound("User not found".to_string()))?;

    #[derive(sqlx::FromRow)]
    struct BeforeRow {
        email: String,
        name: Option<String>,
        role: String,
    }

    let before =
        sqlx::query_as::<_, BeforeRow>("SELECT email, name, role FROM users WHERE id = $1")
            .bind(mapping.user_id)
            .fetch_optional(&*state.db)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    // Extract email and name
    let email = payload
        .emails
        .as_ref()
        .and_then(|emails| {
            emails
                .iter()
                .find(|e| e.primary.unwrap_or(false))
                .or(emails.first())
        })
        .map(|e| e.value.clone())
        .unwrap_or_else(|| payload.user_name.clone());

    let name = payload
        .display_name
        .clone()
        .or_else(|| payload.name.as_ref().and_then(|n| n.formatted.clone()))
        .unwrap_or_else(|| email.clone());

    // Determine role from groups if provided
    let role = if let Some(ref groups) = payload.groups {
        determine_role_from_groups(&state.db, sso_config_id, groups).await?
    } else {
        None
    };

    // Update user
    if let Some(ref role) = role {
        sqlx::query("UPDATE users SET email = $1, name = $2, role = $3 WHERE id = $4")
            .bind(&email)
            .bind(&name)
            .bind(role)
            .bind(mapping.user_id)
            .execute(&*state.db)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to update user: {}", e)))?;
    } else {
        sqlx::query("UPDATE users SET email = $1, name = $2 WHERE id = $3")
            .bind(&email)
            .bind(&name)
            .bind(mapping.user_id)
            .execute(&*state.db)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to update user: {}", e)))?;
    }

    // Update SSO mapping
    sqlx::query(
        "UPDATE sso_user_mappings SET external_email = $1, scim_active = $2, updated_at = NOW() WHERE scim_id = $3"
    )
    .bind(&email)
    .bind(payload.active.unwrap_or(true))
    .bind(&scim_id)
    .execute(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to update mapping: {}", e)))?;

    info!("SCIM: Updated user {}", scim_id);

    let after_role = role
        .as_deref()
        .or(before.as_ref().map(|b| b.role.as_str()))
        .unwrap_or("unknown");
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::ScimUserUpdated)
        .organization(organization_id)
        .resource("scim_user", mapping.user_id)
        .details(serde_json::json!({
            "before": {
                "email": before.as_ref().map(|b| &b.email),
                "display_name": before.as_ref().and_then(|b| b.name.as_deref()),
                "role": before.as_ref().map(|b| &b.role),
                "active": mapping.scim_active,
            },
            "after": {
                "email": &email,
                "display_name": &name,
                "role": after_role,
                "active": payload.active.unwrap_or(true),
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

    Ok(Json(ScimUser {
        schemas: vec!["urn:ietf:params:scim:schemas:core:2.0:User".to_string()],
        id: Some(scim_id.clone()),
        external_id: payload.external_id,
        user_name: email.clone(),
        name: payload.name,
        display_name: Some(name),
        emails: Some(vec![ScimEmail {
            value: email,
            primary: Some(true),
            email_type: Some("work".to_string()),
        }]),
        active: payload.active,
        groups: payload.groups,
        meta: Some(ScimMeta {
            resource_type: "User".to_string(),
            created: None,
            last_modified: Some(chrono::Utc::now().to_rfc3339()),
            location: Some(format!("/scim/v2/Users/{}", scim_id)),
        }),
    }))
}

/// PATCH /scim/v2/Users/{id} - Partial update
async fn update_user(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(scim_id): Path<String>,
    Json(patch): Json<ScimPatchOp>,
) -> Result<Json<ScimUser>> {
    let (sso_config_id, _) = validate_scim_token(&state, &headers).await?;

    // Get existing mapping
    #[derive(sqlx::FromRow)]
    #[allow(dead_code)] // user_id included in SELECT for potential future use
    struct MappingRow {
        user_id: Uuid,
        scim_active: bool,
    }

    let mapping = sqlx::query_as::<_, MappingRow>(
        "SELECT user_id, scim_active FROM sso_user_mappings WHERE sso_config_id = $1 AND scim_id = $2"
    )
    .bind(sso_config_id)
    .bind(&scim_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?
    .ok_or_else(|| AppError::NotFound("User not found".to_string()))?;

    let mut active = mapping.scim_active;

    // Apply patch operations
    for op in patch.operations {
        match op.op.to_lowercase().as_str() {
            "replace" => {
                if let Some(path) = &op.path {
                    if path == "active" {
                        if let Some(value) = op.value {
                            active = value.as_bool().unwrap_or(active);
                        }
                    }
                } else if let Some(value) = op.value {
                    // Direct object replacement
                    if let Some(a) = value.get("active").and_then(|v| v.as_bool()) {
                        active = a;
                    }
                }
            }
            _ => {
                warn!("SCIM: Unsupported patch operation: {}", op.op);
            }
        }
    }

    // Update mapping
    sqlx::query(
        "UPDATE sso_user_mappings SET scim_active = $1, updated_at = NOW() WHERE scim_id = $2",
    )
    .bind(active)
    .bind(&scim_id)
    .execute(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to update: {}", e)))?;

    info!("SCIM: Patched user {} (active={})", scim_id, active);

    // Return updated user
    get_user(State(state), headers, Path(scim_id)).await
}

/// DELETE /scim/v2/Users/{id}
async fn delete_user(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(scim_id): Path<String>,
) -> Result<StatusCode> {
    let (sso_config_id, organization_id) = validate_scim_token(&state, &headers).await?;

    let mut tx = state
        .db
        .begin()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let user_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT user_id FROM sso_user_mappings WHERE sso_config_id = $1 AND scim_id = $2",
    )
    .bind(sso_config_id)
    .bind(&scim_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let Some(user_id) = user_id else {
        tx.rollback()
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;
        return Err(AppError::NotFound("User not found".to_string()));
    };

    #[derive(sqlx::FromRow)]
    struct DeletedUserRow {
        email: String,
        name: Option<String>,
        role: String,
    }

    let deleted_user =
        sqlx::query_as::<_, DeletedUserRow>("SELECT email, name, role FROM users WHERE id = $1")
            .bind(user_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let key_hashes: Vec<String> =
        sqlx::query_scalar("SELECT key_hash FROM project_keys WHERE created_by = $1")
            .bind(user_id)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let keys = sqlx::query("DELETE FROM project_keys WHERE created_by = $1")
        .bind(user_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    tracing::info!(
        user_id = %user_id,
        scim_id = %scim_id,
        revoked_project_keys = keys.rows_affected(),
        "SCIM: revoked API keys for deprovisioned user"
    );

    for kh in &key_hashes {
        if let Err(e) = crate::utils::invalidate_project_key_cache(&state.redis, kh).await {
            warn!(
                key_hash = &kh[..8.min(kh.len())],
                "Failed to invalidate Redis key cache: {}", e
            );
        }
        let scopes_key = format!("key_scopes:{}", kh);
        if let Ok(mut conn) = state.redis.get().await {
            let _ = bb8_redis::redis::AsyncCommands::del::<_, ()>(&mut *conn, &scopes_key).await;
        }
    }

    // Soft delete: just mark as inactive
    let result = sqlx::query(
        "UPDATE sso_user_mappings SET scim_active = false, updated_at = NOW() WHERE sso_config_id = $1 AND scim_id = $2",
    )
    .bind(sso_config_id)
    .bind(&scim_id)
    .execute(&mut *tx)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    if result.rows_affected() == 0 {
        tx.rollback()
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;
        return Err(AppError::NotFound("User not found".to_string()));
    }

    tx.commit()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    info!("SCIM: Deactivated user {}", scim_id);

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::ScimUserDeleted)
        .organization(organization_id)
        .resource("scim_user", user_id)
        .details(serde_json::json!({ "deleted": {
            "email": deleted_user.as_ref().map(|u| &u.email),
            "display_name": deleted_user.as_ref().and_then(|u| u.name.as_deref()),
            "role": deleted_user.as_ref().map(|u| &u.role),
            "active": false,
        }}))
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

    Ok(StatusCode::NO_CONTENT)
}

// ============================================================================
// Group Endpoints (simplified - mainly for role mapping)
// ============================================================================

/// GET /scim/v2/Groups
async fn list_groups(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Query(_query): Query<ScimListQuery>,
) -> Result<Json<ScimListResponse<ScimGroup>>> {
    let (sso_config_id, _) = validate_scim_token(&state, &headers).await?;

    #[derive(sqlx::FromRow)]
    struct GroupRow {
        external_group_id: String,
        external_group_name: String,
    }

    let groups = sqlx::query_as::<_, GroupRow>(
        "SELECT external_group_id, external_group_name FROM scim_group_mappings WHERE sso_config_id = $1"
    )
    .bind(sso_config_id)
    .fetch_all(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let resources: Vec<ScimGroup> = groups
        .iter()
        .map(|g| ScimGroup {
            schemas: vec!["urn:ietf:params:scim:schemas:core:2.0:Group".to_string()],
            id: Some(g.external_group_id.clone()),
            display_name: g.external_group_name.clone(),
            members: None,
            meta: Some(ScimMeta {
                resource_type: "Group".to_string(),
                created: None,
                last_modified: None,
                location: Some(format!("/scim/v2/Groups/{}", g.external_group_id)),
            }),
        })
        .collect();

    Ok(Json(ScimListResponse {
        schemas: vec!["urn:ietf:params:scim:api:messages:2.0:ListResponse".to_string()],
        total_results: resources.len() as i64,
        start_index: Some(1),
        items_per_page: Some(100),
        resources,
    }))
}

/// POST /scim/v2/Groups
async fn create_group(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(payload): Json<ScimGroup>,
) -> Result<(StatusCode, Json<ScimGroup>)> {
    let sso_config_id = validate_scim_token(&state, &headers).await?;

    let group_id = Uuid::new_v4().to_string();

    info!(
        "SCIM: Created group {} ({})",
        payload.display_name, group_id
    );

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::ScimGroupCreated)
        .details(serde_json::json!({ "created": {
            "display_name": &payload.display_name,
            "sso_config_id": sso_config_id,
        }}))
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

    Ok((
        StatusCode::CREATED,
        Json(ScimGroup {
            schemas: vec!["urn:ietf:params:scim:schemas:core:2.0:Group".to_string()],
            id: Some(group_id),
            display_name: payload.display_name,
            members: None,
            meta: Some(ScimMeta {
                resource_type: "Group".to_string(),
                created: Some(chrono::Utc::now().to_rfc3339()),
                last_modified: Some(chrono::Utc::now().to_rfc3339()),
                location: None,
            }),
        }),
    ))
}

/// GET /scim/v2/Groups/{id}
async fn get_group(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(group_id): Path<String>,
) -> Result<Json<ScimGroup>> {
    let (sso_config_id, _) = validate_scim_token(&state, &headers).await?;

    #[derive(sqlx::FromRow)]
    struct GroupRow {
        external_group_name: String,
    }

    let group = sqlx::query_as::<_, GroupRow>(
        "SELECT external_group_name FROM scim_group_mappings WHERE sso_config_id = $1 AND external_group_id = $2"
    )
    .bind(sso_config_id)
    .bind(&group_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?
    .ok_or_else(|| AppError::NotFound("Group not found".to_string()))?;

    Ok(Json(ScimGroup {
        schemas: vec!["urn:ietf:params:scim:schemas:core:2.0:Group".to_string()],
        id: Some(group_id.clone()),
        display_name: group.external_group_name,
        members: None,
        meta: Some(ScimMeta {
            resource_type: "Group".to_string(),
            created: None,
            last_modified: None,
            location: Some(format!("/scim/v2/Groups/{}", group_id)),
        }),
    }))
}

/// PUT /scim/v2/Groups/{id}
async fn replace_group(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(group_id): Path<String>,
    Json(payload): Json<ScimGroup>,
) -> Result<Json<ScimGroup>> {
    let (sso_config_id, organization_id) = validate_scim_token(&state, &headers).await?;

    #[derive(sqlx::FromRow)]
    struct BeforeGroupRow {
        external_group_name: String,
        member_count: i64,
    }

    let before = sqlx::query_as::<_, BeforeGroupRow>(
        r#"
        SELECT g.external_group_name,
               (SELECT COUNT(*) FROM scim_group_members m WHERE m.group_id = g.external_group_id AND m.sso_config_id = g.sso_config_id) AS member_count
        FROM scim_group_mappings g
        WHERE g.sso_config_id = $1 AND g.external_group_id = $2
        "#
    )
    .bind(sso_config_id)
    .bind(&group_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    sqlx::query(
        "UPDATE scim_group_mappings SET external_group_name = $1, updated_at = NOW() WHERE sso_config_id = $2 AND external_group_id = $3"
    )
    .bind(&payload.display_name)
    .bind(sso_config_id)
    .bind(&group_id)
    .execute(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let member_count = payload
        .members
        .as_ref()
        .map(|m| m.len() as i64)
        .unwrap_or_else(|| before.as_ref().map(|b| b.member_count).unwrap_or(0));

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::ScimGroupUpdated)
        .organization(organization_id)
        .details(serde_json::json!({
            "before": {
                "display_name": before.as_ref().map(|b| &b.external_group_name),
                "member_count": before.as_ref().map(|b| b.member_count),
            },
            "after": {
                "display_name": &payload.display_name,
                "member_count": member_count,
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

    get_group(State(state), headers, Path(group_id)).await
}

/// PATCH /scim/v2/Groups/{id}
async fn update_group(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(group_id): Path<String>,
    Json(_patch): Json<ScimPatchOp>,
) -> Result<Json<ScimGroup>> {
    // Simplified: just return existing group
    get_group(State(state), headers, Path(group_id)).await
}

/// DELETE /scim/v2/Groups/{id}
async fn delete_group(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(group_id): Path<String>,
) -> Result<StatusCode> {
    let (sso_config_id, organization_id) = validate_scim_token(&state, &headers).await?;

    #[derive(sqlx::FromRow)]
    struct DeletedGroupRow {
        external_group_name: String,
        member_count: i64,
    }

    let deleted_group = sqlx::query_as::<_, DeletedGroupRow>(
        r#"
        SELECT g.external_group_name,
               (SELECT COUNT(*) FROM scim_group_members m WHERE m.group_id = g.external_group_id AND m.sso_config_id = g.sso_config_id) AS member_count
        FROM scim_group_mappings g
        WHERE g.sso_config_id = $1 AND g.external_group_id = $2
        "#
    )
    .bind(sso_config_id)
    .bind(&group_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    sqlx::query(
        "DELETE FROM scim_group_mappings WHERE sso_config_id = $1 AND external_group_id = $2",
    )
    .bind(sso_config_id)
    .bind(&group_id)
    .execute(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::ScimGroupDeleted)
        .organization(organization_id)
        .details(serde_json::json!({ "deleted": {
            "display_name": deleted_group.as_ref().map(|g| &g.external_group_name),
            "member_count": deleted_group.as_ref().map(|g| g.member_count),
        }}))
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

    Ok(StatusCode::NO_CONTENT)
}

// ============================================================================
// Group Mapping Admin Endpoints
// ============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct GroupMapping {
    pub id: Uuid,
    pub sso_config_id: Uuid,
    pub external_group_id: String,
    pub external_group_name: String,
    pub reiver_role: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateGroupMappingRequest {
    pub sso_config_id: Uuid,
    pub external_group_id: String,
    pub external_group_name: String,
    pub reiver_role: String, // 'admin', 'member', 'viewer'
}

/// GET /scim/v2/GroupMappings - List all group mappings (admin, session-authenticated)
async fn list_group_mappings(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<GroupMapping>>> {
    let user_id = authenticate_request(&headers, &state, RateLimitType::Crud).await?;
    require_any_org_admin(&state.db, user_id).await?;
    #[derive(sqlx::FromRow)]
    struct Row {
        id: Uuid,
        sso_config_id: Uuid,
        external_group_id: String,
        external_group_name: String,
        reiver_role: String,
    }

    let mappings = sqlx::query_as::<_, Row>(
        "SELECT id, sso_config_id, external_group_id, external_group_name, reiver_role FROM scim_group_mappings"
    )
    .fetch_all(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    Ok(Json(
        mappings
            .into_iter()
            .map(|r| GroupMapping {
                id: r.id,
                sso_config_id: r.sso_config_id,
                external_group_id: r.external_group_id,
                external_group_name: r.external_group_name,
                reiver_role: r.reiver_role,
            })
            .collect(),
    ))
}

/// POST /scim/v2/GroupMappings - Create group mapping (admin, session-authenticated)
async fn create_group_mapping(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(payload): Json<CreateGroupMappingRequest>,
) -> Result<Json<GroupMapping>> {
    let user_id = authenticate_request(&headers, &state, RateLimitType::Crud).await?;
    require_any_org_admin(&state.db, user_id).await?;
    // Validate role
    let valid_roles = ["admin", "member", "viewer"];
    if !valid_roles.contains(&payload.reiver_role.as_str()) {
        return Err(AppError::Validation(format!(
            "Invalid role. Must be one of: {:?}",
            valid_roles
        )));
    }

    #[derive(sqlx::FromRow)]
    struct Row {
        id: Uuid,
    }

    let result = sqlx::query_as::<_, Row>(
        r#"
        INSERT INTO scim_group_mappings (sso_config_id, external_group_id, external_group_name, reiver_role)
        VALUES ($1, $2, $3, $4)
        RETURNING id
        "#
    )
    .bind(payload.sso_config_id)
    .bind(&payload.external_group_id)
    .bind(&payload.external_group_name)
    .bind(&payload.reiver_role)
    .fetch_one(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    info!(
        "Created group mapping: {} -> {}",
        payload.external_group_name, payload.reiver_role
    );

    let organization_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT organization_id FROM sso_configurations WHERE id = $1",
    )
    .bind(payload.sso_config_id)
    .fetch_one(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::ScimGroupMappingCreated)
        .actor(user_id)
        .organization(organization_id)
        .resource("scim_group_mapping", result.id)
        .details(serde_json::json!({ "created": {
            "provider": "scim",
            "name": &payload.external_group_name,
            "sso_type": &payload.reiver_role,
        }}))
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

    Ok(Json(GroupMapping {
        id: result.id,
        sso_config_id: payload.sso_config_id,
        external_group_id: payload.external_group_id,
        external_group_name: payload.external_group_name,
        reiver_role: payload.reiver_role,
    }))
}

/// DELETE /scim/v2/GroupMappings/{id} - Delete group mapping (admin, session-authenticated)
async fn delete_group_mapping(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<StatusCode> {
    let user_id = authenticate_request(&headers, &state, RateLimitType::Crud).await?;
    require_any_org_admin(&state.db, user_id).await?;

    #[derive(sqlx::FromRow)]
    struct DeletedMappingRow {
        external_group_name: String,
        reiver_role: String,
        sso_config_id: Uuid,
    }

    let deleted_mapping = sqlx::query_as::<_, DeletedMappingRow>(
        "SELECT external_group_name, reiver_role, sso_config_id FROM scim_group_mappings WHERE id = $1"
    )
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    sqlx::query("DELETE FROM scim_group_mappings WHERE id = $1")
        .bind(id)
        .execute(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let organization_id = match deleted_mapping.as_ref() {
        Some(m) => sqlx::query_scalar::<_, Uuid>(
            "SELECT organization_id FROM sso_configurations WHERE id = $1"
        )
        .bind(m.sso_config_id)
        .fetch_one(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?,
        None => sqlx::query_scalar::<_, Uuid>(
            "SELECT organization_id FROM memberships WHERE user_id = $1 AND status = 'active' LIMIT 1"
        )
        .bind(user_id)
        .fetch_one(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?,
    };

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::ScimGroupMappingDeleted)
        .actor(user_id)
        .organization(organization_id)
        .resource("scim_group_mapping", id)
        .details(serde_json::json!({ "deleted": {
            "provider": "scim",
            "name": deleted_mapping.as_ref().map(|m| &m.external_group_name),
            "sso_type": deleted_mapping.as_ref().map(|m| &m.reiver_role),
        }}))
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

    Ok(StatusCode::NO_CONTENT)
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Determine Reiver role from IdP groups
async fn determine_role_from_groups(
    db: &sqlx::PgPool,
    sso_config_id: Uuid,
    groups: &[ScimGroupRef],
) -> Result<Option<String>> {
    if groups.is_empty() {
        return Ok(None);
    }

    let group_ids: Vec<&str> = groups.iter().map(|g| g.value.as_str()).collect();

    // Priority: admin > member > viewer
    #[derive(sqlx::FromRow)]
    struct RoleRow {
        reiver_role: String,
    }

    let roles = sqlx::query_as::<_, RoleRow>(
        "SELECT reiver_role FROM scim_group_mappings WHERE sso_config_id = $1 AND external_group_id = ANY($2)"
    )
    .bind(sso_config_id)
    .bind(&group_ids)
    .fetch_all(db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    // Return highest priority role
    let role_priority = |r: &str| match r {
        "admin" => 3,
        "member" => 2,
        "viewer" => 1,
        _ => 0,
    };

    let best_role = roles
        .iter()
        .max_by_key(|r| role_priority(&r.reiver_role))
        .map(|r| r.reiver_role.clone());

    Ok(best_role)
}

// ============================================================================
// Settings Admin Endpoints (/api/settings/scim/*)
// ============================================================================

/// GET /api/settings/scim/token — check whether a SCIM bearer token exists.
async fn get_token_status(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>> {
    let user_id = authenticate_request(&headers, &state, RateLimitType::Crud).await?;
    require_any_org_admin(&state.db, user_id).await?;

    let org_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT organization_id FROM memberships WHERE user_id = $1 AND status = 'active' LIMIT 1",
    )
    .bind(user_id)
    .fetch_one(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let tier = state.entitlements.get_config(org_id).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("{}", e)))?;
    if !tier.config.platform.sso {
        return Err(AppError::Forbidden("SSO/SCIM is not available on your current plan".into()));
    }

    #[derive(sqlx::FromRow)]
    struct TokenRow {
        has_token: bool,
        prefix: Option<String>,
        token_created_at: Option<chrono::DateTime<chrono::Utc>>,
    }

    let row = sqlx::query_as::<_, TokenRow>(
        r#"
        SELECT
            scim_bearer_token_hash IS NOT NULL AS has_token,
            scim_bearer_token_prefix,
            scim_bearer_token_created_at AS token_created_at
        FROM sso_configurations
        WHERE organization_id = $1 AND scim_enabled = true
        LIMIT 1
        "#,
    )
    .bind(org_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    match row {
        Some(r) => Ok(Json(serde_json::json!({
            "exists": r.has_token,
            "masked": r.prefix.map(|p| format!("{}…", p)),
            "created_at": r.token_created_at,
        }))),
        None => Ok(Json(serde_json::json!({
            "exists": false,
            "masked": null,
            "created_at": null,
        }))),
    }
}

/// POST /api/settings/scim/token — generate or rotate the SCIM bearer token.
async fn rotate_token(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>> {
    let user_id = authenticate_request(&headers, &state, RateLimitType::Crud).await?;
    require_any_org_admin(&state.db, user_id).await?;

    let org_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT organization_id FROM memberships WHERE user_id = $1 AND status = 'active' LIMIT 1",
    )
    .bind(user_id)
    .fetch_one(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let tier = state.entitlements.get_config(org_id).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("{}", e)))?;
    if !tier.config.platform.sso {
        return Err(AppError::Forbidden("SSO/SCIM is not available on your current plan".into()));
    }

    let sso_config_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM sso_configurations WHERE organization_id = $1 AND scim_enabled = true LIMIT 1",
    )
    .bind(org_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?
    .ok_or_else(|| {
        AppError::Validation("No SCIM-enabled SSO configuration found for this organization".into())
    })?;

    use rand::Rng;
    let raw_token: String = rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(48)
        .map(char::from)
        .collect();

    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(raw_token.as_bytes());
    let token_hash = format!("{:x}", hasher.finalize());
    let prefix = &raw_token[..8];

    sqlx::query(
        r#"
        UPDATE sso_configurations
        SET scim_bearer_token_hash = $1,
            scim_bearer_token_prefix = $2,
            scim_bearer_token_created_at = NOW()
        WHERE id = $3
        "#,
    )
    .bind(&token_hash)
    .bind(prefix)
    .bind(sso_config_id)
    .execute(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    info!("SCIM bearer token rotated for SSO config {}", sso_config_id);

    Ok(Json(serde_json::json!({
        "token": raw_token,
        "masked": format!("{}…", prefix),
    })))
}

/// GET /api/settings/scim/users — list users provisioned via SCIM.
async fn list_provisioned_users(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>> {
    let user_id = authenticate_request(&headers, &state, RateLimitType::Crud).await?;
    require_any_org_admin(&state.db, user_id).await?;

    let org_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT organization_id FROM memberships WHERE user_id = $1 AND status = 'active' LIMIT 1",
    )
    .bind(user_id)
    .fetch_one(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let tier = state.entitlements.get_config(org_id).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("{}", e)))?;
    if !tier.config.platform.sso {
        return Err(AppError::Forbidden("SSO/SCIM is not available on your current plan".into()));
    }

    #[derive(sqlx::FromRow, Serialize)]
    struct ProvisionedUser {
        id: Uuid,
        email: String,
        external_id: String,
        role: String,
        active: bool,
        created_at: chrono::DateTime<chrono::Utc>,
    }

    let users = sqlx::query_as::<_, ProvisionedUser>(
        r#"
        SELECT
            u.id,
            u.email,
            m.external_id,
            mem.role,
            m.scim_active AS active,
            m.created_at
        FROM sso_user_mappings m
        JOIN users u ON u.id = m.user_id
        JOIN sso_configurations sc ON sc.id = m.sso_config_id
        LEFT JOIN memberships mem ON mem.user_id = u.id AND mem.organization_id = sc.organization_id AND mem.status = 'active'
        WHERE sc.organization_id = $1 AND m.provisioned_via_scim = true
        ORDER BY m.created_at DESC
        LIMIT 500
        "#,
    )
    .bind(org_id)
    .fetch_all(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    Ok(Json(serde_json::json!({ "users": users })))
}
