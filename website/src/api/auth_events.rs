//! Auth Event Integration API endpoints
//!
//! Manages IdP (Identity Provider) integrations for ingesting authentication events.
//! Supports: Okta, Auth0, Entra ID (Azure AD), OneLogin, Ping Identity

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info};
use uuid::Uuid;

use crate::app_state::WebsiteState;
use crate::audit::{AuditCaller, AuditEventBuilder, AuditEventType, AuditOrigin};
use crate::error::{AppError, Result};
use crate::utils::escape_clickhouse_string;
use axum::http::HeaderMap;

pub fn create_auth_events_router() -> Router<Arc<WebsiteState>> {
    Router::new()
        .route(
            "/integrations",
            get(list_integrations).post(create_integration),
        )
        .route(
            "/integrations/{id}",
            get(get_integration)
                .put(update_integration)
                .delete(delete_integration),
        )
        .route("/integrations/{id}/test", post(test_integration))
        .route("/events", get(list_events))
        .route("/events/stats", get(get_event_stats))
}

// ============================================================================
// Types
// ============================================================================

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)] // Used for API type definition
pub enum AuthEventProvider {
    Okta,
    Auth0,
    EntraId,
    OneLogin,
    PingIdentity,
}

impl std::fmt::Display for AuthEventProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthEventProvider::Okta => write!(f, "okta"),
            AuthEventProvider::Auth0 => write!(f, "auth0"),
            AuthEventProvider::EntraId => write!(f, "entra_id"),
            AuthEventProvider::OneLogin => write!(f, "onelogin"),
            AuthEventProvider::PingIdentity => write!(f, "ping_identity"),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct AuthEventIntegration {
    pub id: Uuid,
    pub project_id: Uuid,
    pub provider: String,
    pub name: String,
    pub domain: Option<String>,
    pub tenant_id: Option<String>,
    pub environment_id: Option<String>,
    pub region: Option<String>,
    pub client_id: Option<String>,
    // Secrets are never returned
    pub poll_interval_seconds: i32,
    pub event_types: Vec<String>,
    pub last_poll_at: Option<chrono::DateTime<chrono::Utc>>,
    pub enabled: bool,
    pub error_message: Option<String>,
    pub consecutive_errors: i32,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)] // Some fields reserved for future polling implementation
struct IntegrationRow {
    id: Uuid,
    project_id: Uuid,
    provider: String,
    name: String,
    domain: Option<String>,
    tenant_id: Option<String>,
    environment_id: Option<String>,
    region: Option<String>,
    api_token_encrypted: Option<String>,
    client_id: Option<String>,
    client_secret_encrypted: Option<String>,
    poll_interval_seconds: i32,
    event_types: Vec<String>,
    last_poll_at: Option<chrono::DateTime<chrono::Utc>>,
    last_event_id: Option<String>,
    last_event_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    enabled: bool,
    error_message: Option<String>,
    consecutive_errors: Option<i32>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<IntegrationRow> for AuthEventIntegration {
    fn from(row: IntegrationRow) -> Self {
        Self {
            id: row.id,
            project_id: row.project_id,
            provider: row.provider,
            name: row.name,
            domain: row.domain,
            tenant_id: row.tenant_id,
            environment_id: row.environment_id,
            region: row.region,
            client_id: row.client_id,
            poll_interval_seconds: row.poll_interval_seconds,
            event_types: row.event_types,
            last_poll_at: row.last_poll_at,
            enabled: row.enabled,
            error_message: row.error_message,
            consecutive_errors: row.consecutive_errors.unwrap_or(0),
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateIntegrationRequest {
    pub project_id: Uuid,
    pub provider: String,
    pub name: String,
    // Okta
    pub domain: Option<String>,
    pub api_token: Option<String>,
    // Auth0, OneLogin, Ping, Entra
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    // Entra ID specific
    pub tenant_id: Option<String>,
    // Ping Identity specific
    pub environment_id: Option<String>,
    // OneLogin specific
    pub region: Option<String>,
    // Common
    pub poll_interval_seconds: Option<i32>,
    pub event_types: Option<Vec<String>>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateIntegrationRequest {
    pub name: Option<String>,
    pub domain: Option<String>,
    pub api_token: Option<String>,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub tenant_id: Option<String>,
    pub environment_id: Option<String>,
    pub region: Option<String>,
    pub poll_interval_seconds: Option<i32>,
    pub event_types: Option<Vec<String>>,
    pub enabled: Option<bool>,
}

// ============================================================================
// CRUD Endpoints
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    project_id: Option<Uuid>,
}

async fn list_integrations(
    State(state): State<Arc<WebsiteState>>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Vec<AuthEventIntegration>>> {
    let rows = if let Some(project_id) = query.project_id {
        sqlx::query_as::<_, IntegrationRow>(
            "SELECT * FROM auth_event_integration_configs WHERE project_id = $1 ORDER BY created_at DESC"
        )
        .bind(project_id)
        .fetch_all(&*state.db)
        .await
    } else {
        sqlx::query_as::<_, IntegrationRow>(
            "SELECT * FROM auth_event_integration_configs ORDER BY created_at DESC"
        )
        .fetch_all(&*state.db)
        .await
    }.map_err(|e| {
        error!("Failed to list auth event integrations: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?;

    let integrations: Vec<AuthEventIntegration> = rows.into_iter().map(|r| r.into()).collect();
    Ok(Json(integrations))
}

async fn create_integration(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(payload): Json<CreateIntegrationRequest>,
) -> Result<Json<AuthEventIntegration>> {
    // Validate provider-specific requirements
    let provider = payload.provider.to_lowercase();
    match provider.as_str() {
        "okta" => {
            if payload.domain.is_none() || payload.api_token.is_none() {
                return Err(AppError::Validation(
                    "Okta requires 'domain' and 'api_token'".to_string(),
                ));
            }
        }
        "auth0" => {
            if payload.domain.is_none()
                || payload.client_id.is_none()
                || payload.client_secret.is_none()
            {
                return Err(AppError::Validation(
                    "Auth0 requires 'domain', 'client_id', and 'client_secret'".to_string(),
                ));
            }
        }
        "entra_id" => {
            if payload.tenant_id.is_none()
                || payload.client_id.is_none()
                || payload.client_secret.is_none()
            {
                return Err(AppError::Validation(
                    "Entra ID requires 'tenant_id', 'client_id', and 'client_secret'".to_string(),
                ));
            }
        }
        "onelogin" => {
            if payload.region.is_none()
                || payload.client_id.is_none()
                || payload.client_secret.is_none()
            {
                return Err(AppError::Validation(
                    "OneLogin requires 'region', 'client_id', and 'client_secret'".to_string(),
                ));
            }
        }
        "ping_identity" => {
            if payload.environment_id.is_none()
                || payload.client_id.is_none()
                || payload.client_secret.is_none()
            {
                return Err(AppError::Validation(
                    "Ping Identity requires 'environment_id', 'client_id', and 'client_secret'"
                        .to_string(),
                ));
            }
        }
        "keycloak" => {
            if payload.domain.is_none()
                || payload.tenant_id.is_none()
                || payload.client_id.is_none()
                || payload.client_secret.is_none()
            {
                return Err(AppError::Validation(
                    "Keycloak requires 'domain' (Keycloak URL), 'tenant_id' (realm), 'client_id', and 'client_secret'".to_string()
                ));
            }
        }
        _ => {
            return Err(AppError::Validation(format!(
                "Unknown provider: {}. Supported: okta, auth0, entra_id, onelogin, ping_identity, keycloak",
                provider
            )));
        }
    }

    // Encrypt secrets before storing
    let api_token_encrypted = match &payload.api_token {
        Some(token) => Some(state.encryptor.encrypt(token).map_err(|e| {
            error!("Failed to encrypt api_token: {}", e);
            AppError::Internal(anyhow::anyhow!("Encryption error: {}", e))
        })?),
        None => None,
    };

    let client_secret_encrypted = match &payload.client_secret {
        Some(secret) => Some(state.encryptor.encrypt(secret).map_err(|e| {
            error!("Failed to encrypt client_secret: {}", e);
            AppError::Internal(anyhow::anyhow!("Encryption error: {}", e))
        })?),
        None => None,
    };

    let row = sqlx::query_as::<_, IntegrationRow>(
        r#"
        INSERT INTO auth_event_integration_configs (
            project_id, provider, name, domain, tenant_id, environment_id, region,
            api_token_encrypted, client_id, client_secret_encrypted,
            poll_interval_seconds, event_types, enabled
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        RETURNING *
        "#,
    )
    .bind(payload.project_id)
    .bind(&provider)
    .bind(&payload.name)
    .bind(&payload.domain)
    .bind(&payload.tenant_id)
    .bind(&payload.environment_id)
    .bind(&payload.region)
    .bind(&api_token_encrypted)
    .bind(&payload.client_id)
    .bind(&client_secret_encrypted)
    .bind(payload.poll_interval_seconds.unwrap_or(60))
    .bind(payload.event_types.unwrap_or_default())
    .bind(payload.enabled.unwrap_or(true))
    .fetch_one(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to create auth event integration: {}", e);
        if e.to_string().contains("duplicate key") {
            AppError::Validation(format!(
                "Integration for {} already exists in this project",
                provider
            ))
        } else {
            AppError::Internal(anyhow::anyhow!("Database error: {}", e))
        }
    })?;

    info!(
        "Created auth event integration: provider={}, project={}",
        provider, payload.project_id
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
    let mut audit = AuditEventBuilder::new(AuditEventType::AuthEventIntegrationCreated)
        .resource("auth_event_integration", row.id)
        .details(serde_json::json!({
            "created": {
                "name": &payload.name,
                "provider": &provider,
                "project_id": payload.project_id,
                "enabled": row.enabled,
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

    Ok(Json(row.into()))
}

async fn get_integration(
    State(state): State<Arc<WebsiteState>>,
    Path(id): Path<Uuid>,
) -> Result<Json<AuthEventIntegration>> {
    let row = sqlx::query_as::<_, IntegrationRow>(
        "SELECT * FROM auth_event_integration_configs WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to get auth event integration: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?
    .ok_or_else(|| AppError::NotFound("Integration not found".to_string()))?;

    Ok(Json(row.into()))
}

async fn update_integration(
    State(state): State<Arc<WebsiteState>>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(payload): Json<UpdateIntegrationRequest>,
) -> Result<Json<AuthEventIntegration>> {
    // Fetch before-state for audit
    let before: (String, String, bool) = sqlx::query_as(
        "SELECT name, provider, enabled FROM auth_event_integration_configs WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to fetch integration before update: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?
    .ok_or_else(|| AppError::NotFound("Integration not found".to_string()))?;

    let (before_name, before_provider, before_enabled) = before;

    // Encrypt secrets if provided
    let api_token_encrypted = match &payload.api_token {
        Some(token) => Some(state.encryptor.encrypt(token).map_err(|e| {
            error!("Failed to encrypt api_token: {}", e);
            AppError::Internal(anyhow::anyhow!("Encryption error: {}", e))
        })?),
        None => None,
    };

    let client_secret_encrypted = match &payload.client_secret {
        Some(secret) => Some(state.encryptor.encrypt(secret).map_err(|e| {
            error!("Failed to encrypt client_secret: {}", e);
            AppError::Internal(anyhow::anyhow!("Encryption error: {}", e))
        })?),
        None => None,
    };

    let row = sqlx::query_as::<_, IntegrationRow>(
        r#"
        UPDATE auth_event_integration_configs
        SET
            name = COALESCE($1, name),
            domain = COALESCE($2, domain),
            api_token_encrypted = COALESCE($3, api_token_encrypted),
            client_id = COALESCE($4, client_id),
            client_secret_encrypted = COALESCE($5, client_secret_encrypted),
            tenant_id = COALESCE($6, tenant_id),
            environment_id = COALESCE($7, environment_id),
            region = COALESCE($8, region),
            poll_interval_seconds = COALESCE($9, poll_interval_seconds),
            event_types = COALESCE($10, event_types),
            enabled = COALESCE($11, enabled),
            updated_at = NOW()
        WHERE id = $12
        RETURNING *
        "#,
    )
    .bind(payload.name.as_deref())
    .bind(payload.domain.as_deref())
    .bind(api_token_encrypted.as_deref())
    .bind(payload.client_id.as_deref())
    .bind(client_secret_encrypted.as_deref())
    .bind(payload.tenant_id.as_deref())
    .bind(payload.environment_id.as_deref())
    .bind(payload.region.as_deref())
    .bind(payload.poll_interval_seconds)
    .bind(payload.event_types)
    .bind(payload.enabled)
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to update auth event integration: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?
    .ok_or_else(|| AppError::NotFound("Integration not found".to_string()))?;

    info!("Updated auth event integration: id={}", id);

    let organization_id =
        sqlx::query_scalar::<_, Uuid>("SELECT organization_id FROM projects WHERE id = $1")
            .bind(row.project_id)
            .fetch_optional(&*state.db)
            .await
            .ok()
            .flatten();

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::AuthEventIntegrationUpdated)
        .resource("auth_event_integration", id)
        .details(serde_json::json!({
            "before": {
                "name": &before_name,
                "provider": &before_provider,
                "enabled": before_enabled,
            },
            "after": {
                "name": &row.name,
                "provider": &row.provider,
                "enabled": row.enabled,
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
        .success();
    if let Some(org_id) = organization_id {
        audit = audit.organization(org_id);
    }
    audit.log(&state.clickhouse).await;

    Ok(Json(row.into()))
}

async fn delete_integration(
    State(state): State<Arc<WebsiteState>>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<StatusCode> {
    // Fetch before-state for audit
    let before: Option<(String, String, Uuid)> = sqlx::query_as(
        "SELECT name, provider, project_id FROM auth_event_integration_configs WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to fetch integration before delete: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?;

    let (before_name, before_provider, before_project_id) =
        before.ok_or_else(|| AppError::NotFound("Integration not found".to_string()))?;

    let organization_id =
        sqlx::query_scalar::<_, Uuid>("SELECT organization_id FROM projects WHERE id = $1")
            .bind(before_project_id)
            .fetch_optional(&*state.db)
            .await
            .ok()
            .flatten();

    let result = sqlx::query("DELETE FROM auth_event_integration_configs WHERE id = $1")
        .bind(id)
        .execute(&*state.db)
        .await
        .map_err(|e| {
            error!("Failed to delete auth event integration: {}", e);
            AppError::Internal(anyhow::anyhow!("Database error: {}", e))
        })?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound("Integration not found".to_string()));
    }

    info!("Deleted auth event integration: id={}", id);

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::AuthEventIntegrationDeleted)
        .resource("auth_event_integration", id)
        .details(serde_json::json!({
            "deleted": {
                "name": &before_name,
                "provider": &before_provider,
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

    Ok(StatusCode::NO_CONTENT)
}

// ============================================================================
// Test Connection
// ============================================================================

#[derive(Debug, Serialize)]
pub struct TestResult {
    pub success: bool,
    pub message: String,
    pub sample_events: Option<i64>,
}

async fn test_integration(
    State(state): State<Arc<WebsiteState>>,
    Path(id): Path<Uuid>,
) -> Result<Json<TestResult>> {
    let row = sqlx::query_as::<_, IntegrationRow>(
        "SELECT * FROM auth_event_integration_configs WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?
    .ok_or_else(|| AppError::NotFound("Integration not found".to_string()))?;

    let client = reqwest::Client::new();

    let result = match row.provider.as_str() {
        "okta" => test_okta_connection(&client, &row).await,
        "auth0" => test_auth0_connection(&client, &row).await,
        "entra_id" => test_entra_connection(&client, &row).await,
        "onelogin" => test_onelogin_connection(&client, &row).await,
        "ping_identity" => test_ping_connection(&client, &row).await,
        "keycloak" => test_keycloak_connection(&client, &row).await,
        _ => Err(format!("Unknown provider: {}", row.provider)),
    };

    match result {
        Ok(count) => {
            // Clear error state on success
            sqlx::query(
                "UPDATE auth_event_integration_configs SET error_message = NULL, consecutive_errors = 0 WHERE id = $1"
            )
            .bind(id)
            .execute(&*state.db)
            .await
            .ok();

            Ok(Json(TestResult {
                success: true,
                message: "Connection successful".to_string(),
                sample_events: Some(count),
            }))
        }
        Err(msg) => {
            // Record error
            sqlx::query(
                "UPDATE auth_event_integration_configs SET error_message = $1, consecutive_errors = consecutive_errors + 1 WHERE id = $2"
            )
            .bind(&msg)
            .bind(id)
            .execute(&*state.db)
            .await
            .ok();

            Ok(Json(TestResult {
                success: false,
                message: msg,
                sample_events: None,
            }))
        }
    }
}

async fn test_okta_connection(
    client: &reqwest::Client,
    config: &IntegrationRow,
) -> std::result::Result<i64, String> {
    let domain = config.domain.as_ref().ok_or("Domain not configured")?;
    let api_token = config
        .api_token_encrypted
        .as_ref()
        .ok_or("API token not configured")?;

    let url = format!("https://{}/api/v1/logs?limit=1", domain);

    let response = client
        .get(&url)
        .header("Authorization", format!("SSWS {}", api_token))
        .header("Accept", "application/json")
        .send()
        .await
        .map_err(|e| format!("Request failed: {}", e))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("Okta API error ({}): {}", status, body));
    }

    let events: Vec<serde_json::Value> = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse response: {}", e))?;

    Ok(events.len() as i64)
}

async fn test_auth0_connection(
    client: &reqwest::Client,
    config: &IntegrationRow,
) -> std::result::Result<i64, String> {
    let domain = config.domain.as_ref().ok_or("Domain not configured")?;
    let client_id = config
        .client_id
        .as_ref()
        .ok_or("Client ID not configured")?;
    let client_secret = config
        .client_secret_encrypted
        .as_ref()
        .ok_or("Client secret not configured")?;

    // Get access token
    let token_url = format!("https://{}/oauth/token", domain);
    let token_response = client
        .post(&token_url)
        .json(&serde_json::json!({
            "client_id": client_id,
            "client_secret": client_secret,
            "audience": format!("https://{}/api/v2/", domain),
            "grant_type": "client_credentials"
        }))
        .send()
        .await
        .map_err(|e| format!("Token request failed: {}", e))?;

    if !token_response.status().is_success() {
        let body = token_response.text().await.unwrap_or_default();
        return Err(format!("Auth0 token error: {}", body));
    }

    let token_data: serde_json::Value = token_response
        .json()
        .await
        .map_err(|e| format!("Failed to parse token: {}", e))?;
    let access_token = token_data["access_token"]
        .as_str()
        .ok_or("No access token in response")?;

    // Fetch logs
    let logs_url = format!("https://{}/api/v2/logs?per_page=1", domain);
    let response = client
        .get(&logs_url)
        .header("Authorization", format!("Bearer {}", access_token))
        .send()
        .await
        .map_err(|e| format!("Logs request failed: {}", e))?;

    if !response.status().is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(format!("Auth0 logs error: {}", body));
    }

    let events: Vec<serde_json::Value> = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse logs: {}", e))?;

    Ok(events.len() as i64)
}

async fn test_entra_connection(
    client: &reqwest::Client,
    config: &IntegrationRow,
) -> std::result::Result<i64, String> {
    let tenant_id = config
        .tenant_id
        .as_ref()
        .ok_or("Tenant ID not configured")?;
    let client_id = config
        .client_id
        .as_ref()
        .ok_or("Client ID not configured")?;
    let client_secret = config
        .client_secret_encrypted
        .as_ref()
        .ok_or("Client secret not configured")?;

    // Get access token
    let token_url = format!(
        "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
        tenant_id
    );
    let token_response = client
        .post(&token_url)
        .form(&[
            ("client_id", client_id.as_str()),
            ("client_secret", client_secret.as_str()),
            ("scope", "https://graph.microsoft.com/.default"),
            ("grant_type", "client_credentials"),
        ])
        .send()
        .await
        .map_err(|e| format!("Token request failed: {}", e))?;

    if !token_response.status().is_success() {
        let body = token_response.text().await.unwrap_or_default();
        return Err(format!("Entra token error: {}", body));
    }

    let token_data: serde_json::Value = token_response
        .json()
        .await
        .map_err(|e| format!("Failed to parse token: {}", e))?;
    let access_token = token_data["access_token"]
        .as_str()
        .ok_or("No access token in response")?;

    // Fetch sign-in logs
    let logs_url = "https://graph.microsoft.com/v1.0/auditLogs/signIns?$top=1";
    let response = client
        .get(logs_url)
        .header("Authorization", format!("Bearer {}", access_token))
        .send()
        .await
        .map_err(|e| format!("Logs request failed: {}", e))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("Entra logs error ({}): {}", status, body));
    }

    let data: serde_json::Value = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse logs: {}", e))?;
    let count = data["value"].as_array().map(|a| a.len()).unwrap_or(0);

    Ok(count as i64)
}

async fn test_onelogin_connection(
    client: &reqwest::Client,
    config: &IntegrationRow,
) -> std::result::Result<i64, String> {
    let region = config.region.as_ref().ok_or("Region not configured")?;
    let client_id = config
        .client_id
        .as_ref()
        .ok_or("Client ID not configured")?;
    let client_secret = config
        .client_secret_encrypted
        .as_ref()
        .ok_or("Client secret not configured")?;

    let api_base = match region.to_lowercase().as_str() {
        "us" => "https://api.us.onelogin.com",
        "eu" => "https://api.eu.onelogin.com",
        _ => return Err(format!("Invalid region: {}. Use 'us' or 'eu'", region)),
    };

    // Get access token
    let token_url = format!("{}/auth/oauth2/v2/token", api_base);
    let token_response = client
        .post(&token_url)
        .header(
            "Authorization",
            format!("client_id:{}, client_secret:{}", client_id, client_secret),
        )
        .json(&serde_json::json!({"grant_type": "client_credentials"}))
        .send()
        .await
        .map_err(|e| format!("Token request failed: {}", e))?;

    if !token_response.status().is_success() {
        let body = token_response.text().await.unwrap_or_default();
        return Err(format!("OneLogin token error: {}", body));
    }

    let token_data: serde_json::Value = token_response
        .json()
        .await
        .map_err(|e| format!("Failed to parse token: {}", e))?;
    let access_token = token_data["access_token"]
        .as_str()
        .ok_or("No access token in response")?;

    // Fetch events
    let events_url = format!("{}/api/1/events?limit=1", api_base);
    let response = client
        .get(&events_url)
        .header("Authorization", format!("bearer:{}", access_token))
        .send()
        .await
        .map_err(|e| format!("Events request failed: {}", e))?;

    if !response.status().is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(format!("OneLogin events error: {}", body));
    }

    let data: serde_json::Value = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse events: {}", e))?;
    let count = data["data"].as_array().map(|a| a.len()).unwrap_or(0);

    Ok(count as i64)
}

async fn test_keycloak_connection(
    client: &reqwest::Client,
    config: &IntegrationRow,
) -> std::result::Result<i64, String> {
    let domain = config
        .domain
        .as_ref()
        .ok_or("Domain (Keycloak URL) not configured")?;
    let client_id = config
        .client_id
        .as_ref()
        .ok_or("Client ID not configured")?;
    let client_secret = config
        .client_secret_encrypted
        .as_ref()
        .ok_or("Client secret not configured")?;
    let realm = config
        .tenant_id
        .as_ref()
        .ok_or("Realm not configured (use tenant_id field)")?;

    // Get access token from Keycloak
    let token_url = format!("{}/realms/{}/protocol/openid-connect/token", domain, realm);
    let token_response = client
        .post(&token_url)
        .form(&[
            ("client_id", client_id.as_str()),
            ("client_secret", client_secret.as_str()),
            ("grant_type", "client_credentials"),
        ])
        .send()
        .await
        .map_err(|e| format!("Token request failed: {}", e))?;

    if !token_response.status().is_success() {
        let body = token_response.text().await.unwrap_or_default();
        return Err(format!("Keycloak token error: {}", body));
    }

    let token_data: serde_json::Value = token_response
        .json()
        .await
        .map_err(|e| format!("Failed to parse token: {}", e))?;
    let access_token = token_data["access_token"]
        .as_str()
        .ok_or("No access token in response")?;

    // Fetch events from Admin API
    let events_url = format!("{}/admin/realms/{}/events?max=1", domain, realm);
    let response = client
        .get(&events_url)
        .header("Authorization", format!("Bearer {}", access_token))
        .send()
        .await
        .map_err(|e| format!("Events request failed: {}", e))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("Keycloak events error ({}): {}", status, body));
    }

    let events: Vec<serde_json::Value> = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse events: {}", e))?;

    Ok(events.len() as i64)
}

async fn test_ping_connection(
    client: &reqwest::Client,
    config: &IntegrationRow,
) -> std::result::Result<i64, String> {
    let env_id = config
        .environment_id
        .as_ref()
        .ok_or("Environment ID not configured")?;
    let client_id = config
        .client_id
        .as_ref()
        .ok_or("Client ID not configured")?;
    let client_secret = config
        .client_secret_encrypted
        .as_ref()
        .ok_or("Client secret not configured")?;

    // Get access token
    let token_url = format!("https://auth.pingone.com/{}/as/token", env_id);
    let token_response = client
        .post(&token_url)
        .basic_auth(client_id, Some(client_secret))
        .form(&[("grant_type", "client_credentials")])
        .send()
        .await
        .map_err(|e| format!("Token request failed: {}", e))?;

    if !token_response.status().is_success() {
        let body = token_response.text().await.unwrap_or_default();
        return Err(format!("PingOne token error: {}", body));
    }

    let token_data: serde_json::Value = token_response
        .json()
        .await
        .map_err(|e| format!("Failed to parse token: {}", e))?;
    let access_token = token_data["access_token"]
        .as_str()
        .ok_or("No access token in response")?;

    // Fetch audit events
    let events_url = format!(
        "https://api.pingone.com/v1/environments/{}/activities?limit=1",
        env_id
    );
    let response = client
        .get(&events_url)
        .header("Authorization", format!("Bearer {}", access_token))
        .send()
        .await
        .map_err(|e| format!("Activities request failed: {}", e))?;

    if !response.status().is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(format!("PingOne activities error: {}", body));
    }

    let data: serde_json::Value = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse activities: {}", e))?;
    let count = data["_embedded"]["activities"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);

    Ok(count as i64)
}

// ============================================================================
// Query Events
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct EventsQuery {
    project_id: Uuid,
    provider: Option<String>,
    event_type: Option<String>,
    outcome: Option<String>,
    actor_email: Option<String>,
    from: Option<String>,
    to: Option<String>,
    limit: Option<i32>,
}

#[derive(Debug, Serialize)]
pub struct AuthEvent {
    pub event_id: String,
    pub provider: String,
    pub timestamp: String,
    pub event_type: String,
    pub event_category: String,
    pub outcome: String,
    pub actor_email: String,
    pub actor_display_name: String,
    pub client_ip: String,
    pub geo_country: String,
    pub auth_method: String,
    pub application_name: String,
    pub risk_level: String,
    pub error_message: Option<String>,
}

async fn list_events(
    State(state): State<Arc<WebsiteState>>,
    Query(query): Query<EventsQuery>,
) -> Result<Json<Vec<AuthEvent>>> {
    let mut sql = String::from(
        r#"
        SELECT 
            event_id, provider, toString(timestamp) as timestamp,
            event_type, event_category, outcome,
            actor_email, actor_display_name, client_ip,
            geo_country, auth_method, application_name,
            risk_level, error_message
        FROM auth_events
        WHERE project_id = ?
        "#,
    );

    let mut params: Vec<String> = vec![query.project_id.to_string()];

    if let Some(ref provider) = query.provider {
        sql.push_str(" AND provider = ?");
        params.push(provider.clone());
    }
    if let Some(ref event_type) = query.event_type {
        sql.push_str(" AND event_type = ?");
        params.push(event_type.clone());
    }
    if let Some(ref outcome) = query.outcome {
        sql.push_str(" AND outcome = ?");
        params.push(outcome.clone());
    }
    if let Some(ref actor_email) = query.actor_email {
        sql.push_str(" AND actor_email = ?");
        params.push(actor_email.clone());
    }
    if let Some(ref from) = query.from {
        sql.push_str(" AND timestamp >= parseDateTimeBestEffort(?)");
        params.push(from.clone());
    }
    if let Some(ref to) = query.to {
        sql.push_str(" AND timestamp <= parseDateTimeBestEffort(?)");
        params.push(to.clone());
    }

    sql.push_str(" ORDER BY timestamp DESC LIMIT ?");
    params.push(query.limit.unwrap_or(100).to_string());

    // Execute against ClickHouse
    let events = state
        .clickhouse
        .query(&sql)
        .fetch_all::<AuthEventRow>()
        .await
        .map_err(|e| {
            error!("Failed to query auth events: {}", e);
            AppError::Internal(anyhow::anyhow!("Query error: {}", e))
        })?;

    let result: Vec<AuthEvent> = events
        .into_iter()
        .map(|e| AuthEvent {
            event_id: e.event_id,
            provider: e.provider,
            timestamp: e.timestamp,
            event_type: e.event_type,
            event_category: e.event_category,
            outcome: e.outcome,
            actor_email: e.actor_email,
            actor_display_name: e.actor_display_name,
            client_ip: e.client_ip,
            geo_country: e.geo_country,
            auth_method: e.auth_method,
            application_name: e.application_name,
            risk_level: e.risk_level,
            error_message: if e.error_message.is_empty() {
                None
            } else {
                Some(e.error_message)
            },
        })
        .collect();

    Ok(Json(result))
}

#[derive(Debug, clickhouse::Row, serde::Deserialize)]
struct AuthEventRow {
    event_id: String,
    provider: String,
    timestamp: String,
    event_type: String,
    event_category: String,
    outcome: String,
    actor_email: String,
    actor_display_name: String,
    client_ip: String,
    geo_country: String,
    auth_method: String,
    application_name: String,
    risk_level: String,
    error_message: String,
}

#[derive(Debug, Serialize)]
pub struct EventStats {
    pub total_events: i64,
    pub by_outcome: Vec<OutcomeCount>,
    pub by_event_type: Vec<EventTypeCount>,
    pub by_provider: Vec<ProviderCount>,
    pub suspicious_count: i64,
    pub unique_actors: i64,
}

#[derive(Debug, Serialize, clickhouse::Row, serde::Deserialize)]
pub struct OutcomeCount {
    pub outcome: String,
    pub count: u64,
}

#[derive(Debug, Serialize, clickhouse::Row, serde::Deserialize)]
pub struct EventTypeCount {
    pub event_type: String,
    pub count: u64,
}

#[derive(Debug, Serialize, clickhouse::Row, serde::Deserialize)]
pub struct ProviderCount {
    pub provider: String,
    pub count: u64,
}

#[derive(Debug, Deserialize)]
pub struct StatsQuery {
    project_id: Uuid,
    from: Option<String>,
    to: Option<String>,
}

async fn get_event_stats(
    State(state): State<Arc<WebsiteState>>,
    Query(query): Query<StatsQuery>,
) -> Result<Json<EventStats>> {
    let time_filter = if query.from.is_some() || query.to.is_some() {
        let from = query.from.as_deref().unwrap_or("now() - INTERVAL 24 HOUR");
        let to = query.to.as_deref().unwrap_or("now()");
        format!(
            "AND timestamp BETWEEN parseDateTimeBestEffort('{}') AND parseDateTimeBestEffort('{}')",
            escape_clickhouse_string(from),
            escape_clickhouse_string(to)
        )
    } else {
        "AND timestamp >= now() - INTERVAL 24 HOUR".to_string()
    };

    let project_id = query.project_id.to_string();

    // Total and suspicious count
    #[derive(Debug, clickhouse::Row, serde::Deserialize)]
    struct TotalRow {
        total: u64,
        suspicious: u64,
        unique_actors: u64,
    }

    let totals = state.clickhouse.query(&format!(
        "SELECT count() as total, countIf(is_suspicious = 1) as suspicious, uniqExact(actor_id) as unique_actors 
         FROM auth_events WHERE project_id = '{}' {}",
        project_id, time_filter
    ))
    .fetch_one::<TotalRow>()
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Query error: {}", e)))?;

    // By outcome
    let by_outcome = state
        .clickhouse
        .query(&format!(
            "SELECT outcome, count() as count FROM auth_events 
         WHERE project_id = '{}' {} GROUP BY outcome ORDER BY count DESC",
            project_id, time_filter
        ))
        .fetch_all::<OutcomeCount>()
        .await
        .unwrap_or_default();

    // By event type
    let by_event_type = state
        .clickhouse
        .query(&format!(
            "SELECT event_type, count() as count FROM auth_events 
         WHERE project_id = '{}' {} GROUP BY event_type ORDER BY count DESC LIMIT 10",
            project_id, time_filter
        ))
        .fetch_all::<EventTypeCount>()
        .await
        .unwrap_or_default();

    // By provider
    let by_provider = state
        .clickhouse
        .query(&format!(
            "SELECT provider, count() as count FROM auth_events 
         WHERE project_id = '{}' {} GROUP BY provider ORDER BY count DESC",
            project_id, time_filter
        ))
        .fetch_all::<ProviderCount>()
        .await
        .unwrap_or_default();

    Ok(Json(EventStats {
        total_events: totals.total as i64,
        by_outcome,
        by_event_type,
        by_provider,
        suspicious_count: totals.suspicious as i64,
        unique_actors: totals.unique_actors as i64,
    }))
}
