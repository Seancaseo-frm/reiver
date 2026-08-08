//! GCP integrations API endpoints
//!
//! Provides endpoints for configuring and managing GCP service integrations

use axum::{
    extract::{Path, State},
    http::HeaderMap,
    response::Json,
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info};
use uuid::Uuid;

use reiver_core::audit::{AuditCaller, AuditEventBuilder, AuditEventType, AuditOrigin};

use crate::app_state::WatchState;
use crate::error::{AppError, Result};

pub fn create_gcp_router() -> Router<Arc<WatchState>> {
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
}

#[derive(Debug, Serialize)]
pub struct GcpIntegration {
    pub id: Uuid,
    pub project_id: Uuid,
    pub name: String,
    pub integration_type: String,
    pub gcp_project_id: String,
    pub enabled: bool,
    pub collection_interval_seconds: i32,
    pub config_jsonb: serde_json::Value,
    pub service_account_email: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, sqlx::FromRow)]
struct IntegrationRow {
    id: Uuid,
    project_id: Uuid,
    name: String,
    integration_type: String,
    gcp_project_id: String,
    enabled: bool,
    collection_interval_seconds: i32,
    config_jsonb: serde_json::Value,
    service_account_email: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<IntegrationRow> for GcpIntegration {
    fn from(row: IntegrationRow) -> Self {
        GcpIntegration {
            id: row.id,
            project_id: row.project_id,
            name: row.name,
            integration_type: row.integration_type,
            gcp_project_id: row.gcp_project_id,
            enabled: row.enabled,
            collection_interval_seconds: row.collection_interval_seconds,
            config_jsonb: row.config_jsonb,
            service_account_email: row.service_account_email,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateGcpIntegrationRequest {
    pub name: String,
    pub integration_type: String, // 'compute_engine', etc.
    pub gcp_project_id: String,

    // Service Account authentication (either provide service_account_json OR service_account_email + private_key)
    pub service_account_email: Option<String>,
    pub private_key: Option<String>,
    pub service_account_json: Option<String>, // Full JSON key file content

    pub config_jsonb: Option<serde_json::Value>,
    pub enabled: Option<bool>,
    pub collection_interval_seconds: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateGcpIntegrationRequest {
    pub name: Option<String>,
    pub gcp_project_id: Option<String>,

    // Service Account authentication
    pub service_account_email: Option<String>,
    pub private_key: Option<String>,
    pub service_account_json: Option<String>,

    pub config_jsonb: Option<serde_json::Value>,
    pub enabled: Option<bool>,
    pub collection_interval_seconds: Option<i32>,
}

/// List all GCP integrations for a project
/// GET /api/gcp/integrations?project_id=...
async fn list_integrations(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<GcpIntegration>>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    let rows = sqlx::query_as::<_, IntegrationRow>(
        r#"
        SELECT 
            id,
            project_id,
            name,
            integration_type,
            gcp_project_id,
            enabled,
            collection_interval_seconds,
            config_jsonb,
            service_account_email,
            created_at,
            updated_at
        FROM gcp_integration_configs
        WHERE project_id = $1
        ORDER BY created_at DESC
        "#,
    )
    .bind(project_id)
    .fetch_all(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to list GCP integrations: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?;

    let integrations: Vec<GcpIntegration> = rows.into_iter().map(|r| r.into()).collect();

    Ok(Json(integrations))
}

/// Get a specific GCP integration
/// GET /api/gcp/integrations/{id}
async fn get_integration(
    State(state): State<Arc<WatchState>>,
    Path(id): Path<Uuid>,
) -> Result<Json<GcpIntegration>> {
    let row = sqlx::query_as::<_, IntegrationRow>(
        r#"
        SELECT 
            id,
            project_id,
            name,
            integration_type,
            gcp_project_id,
            enabled,
            collection_interval_seconds,
            config_jsonb,
            service_account_email,
            created_at,
            updated_at
        FROM gcp_integration_configs
        WHERE id = $1
        "#,
    )
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to get GCP integration: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?;

    let integration =
        row.ok_or_else(|| AppError::NotFound(format!("GCP integration with id {} not found", id)))?;

    Ok(Json(integration.into()))
}

/// Create a new GCP integration
/// POST /api/gcp/integrations
async fn create_integration(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<CreateGcpIntegrationRequest>,
) -> Result<Json<GcpIntegration>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    // Validate integration_type
    match payload.integration_type.as_str() {
        "compute_engine" | "cloud_functions" | "cloud_storage" | "cloudsql" | "cloud_spanner"
        | "spanner" | "cloud_redis" | "redis" | "cloud_run" | "run" | "gke"
        | "kubernetes_engine" | "pubsub" | "pub_sub" | "load_balancing" | "load_balancer"
        | "monitoring" | "cloud_monitoring" | "api_gateway" | "apigateway" => {}
        _ => {
            return Err(AppError::Validation(format!(
                "Invalid integration_type: {}",
                payload.integration_type
            )));
        }
    }

    // Validate authentication (either service_account_json OR service_account_email + private_key)
    if payload.service_account_json.is_none() {
        if payload.service_account_email.is_none() || payload.private_key.is_none() {
            return Err(AppError::Validation(
                "Either service_account_json OR (service_account_email + private_key) must be provided".to_string()
            ));
        }
    }

    let id = Uuid::new_v4();
    let enabled = payload.enabled.unwrap_or(true);
    let collection_interval_seconds = payload.collection_interval_seconds.unwrap_or(300); // Default 5 minutes
    let config_jsonb = payload
        .config_jsonb
        .unwrap_or_else(|| serde_json::json!({}));

    sqlx::query(
        r#"
        INSERT INTO gcp_integration_configs (
            id,
            project_id,
            name,
            integration_type,
            gcp_project_id,
            service_account_email,
            private_key,
            service_account_json,
            enabled,
            collection_interval_seconds,
            config_jsonb
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        "#,
    )
    .bind(id)
    .bind(project_id)
    .bind(&payload.name)
    .bind(&payload.integration_type)
    .bind(&payload.gcp_project_id)
    .bind(&payload.service_account_email)
    .bind(&payload.private_key)
    .bind(&payload.service_account_json)
    .bind(enabled)
    .bind(collection_interval_seconds)
    .bind(&config_jsonb)
    .execute(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to create GCP integration: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?;

    info!("Created GCP integration: {} ({})", id, payload.name);

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::IntegrationCreated)
        .resource("gcp", id)
        .details(serde_json::json!({ "created": { "name": &payload.name, "integration_type": &payload.integration_type } }))
        .origin(&audit_origin.origin_type, &audit_origin.origin_ref, &audit_origin.origin_reason)
        .caller(&audit_caller.caller_type, &audit_caller.key_label, &audit_caller.key_prefix)
        .success()
        .log(&state.clickhouse)
        .await;

    // Fetch and return the created integration
    get_integration(State(state), Path(id)).await
}

/// Update an existing GCP integration
/// PUT /api/gcp/integrations/{id}
async fn update_integration(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(payload): Json<UpdateGcpIntegrationRequest>,
) -> Result<Json<GcpIntegration>> {
    let before_row = sqlx::query_as::<_, IntegrationRow>(
        "SELECT id, project_id, name, integration_type, gcp_project_id, enabled, collection_interval_seconds, config_jsonb, service_account_email, created_at, updated_at FROM gcp_integration_configs WHERE id = $1"
    )
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .ok()
    .flatten();

    // Build UPDATE query with all optional fields
    // Using COALESCE to only update fields that are provided
    sqlx::query(
        r#"
        UPDATE gcp_integration_configs
        SET 
            name = COALESCE($1, name),
            gcp_project_id = COALESCE($2, gcp_project_id),
            service_account_email = COALESCE($3, service_account_email),
            private_key = COALESCE($4, private_key),
            service_account_json = COALESCE($5, service_account_json),
            enabled = COALESCE($6, enabled),
            collection_interval_seconds = COALESCE($7, collection_interval_seconds),
            config_jsonb = COALESCE($8, config_jsonb),
            updated_at = NOW()
        WHERE id = $9
        "#,
    )
    .bind(&payload.name)
    .bind(&payload.gcp_project_id)
    .bind(&payload.service_account_email)
    .bind(&payload.private_key)
    .bind(&payload.service_account_json)
    .bind(&payload.enabled)
    .bind(&payload.collection_interval_seconds)
    .bind(&payload.config_jsonb)
    .bind(id)
    .execute(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to update GCP integration: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?;

    info!("Updated GCP integration: {}", id);

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::IntegrationUpdated)
        .resource("gcp", id)
        .details(serde_json::json!({
            "before": { "name": before_row.as_ref().map(|r| &r.name), "enabled": before_row.as_ref().map(|r| r.enabled) },
            "after": { "name": &payload.name, "enabled": &payload.enabled }
        }))
        .origin(&audit_origin.origin_type, &audit_origin.origin_ref, &audit_origin.origin_reason)
        .caller(&audit_caller.caller_type, &audit_caller.key_label, &audit_caller.key_prefix)
        .success()
        .log(&state.clickhouse)
        .await;

    // Fetch and return the updated integration
    get_integration(State(state), Path(id)).await
}

/// Delete a GCP integration
/// DELETE /api/gcp/integrations/{id}
async fn delete_integration(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>> {
    let deleted_row = sqlx::query_as::<_, IntegrationRow>(
        "SELECT id, project_id, name, integration_type, gcp_project_id, enabled, collection_interval_seconds, config_jsonb, service_account_email, created_at, updated_at FROM gcp_integration_configs WHERE id = $1"
    )
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .ok()
    .flatten();

    let result = sqlx::query("DELETE FROM gcp_integration_configs WHERE id = $1")
        .bind(id)
        .execute(&*state.db)
        .await
        .map_err(|e| {
            error!("Failed to delete GCP integration: {}", e);
            AppError::Internal(anyhow::anyhow!("Database error: {}", e))
        })?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound(format!(
            "GCP integration with id {} not found",
            id
        )));
    }

    info!("Deleted GCP integration: {}", id);

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::IntegrationDeleted)
        .resource("gcp", id)
        .details(serde_json::json!({ "deleted": { "name": deleted_row.as_ref().map(|r| &r.name), "integration_type": deleted_row.as_ref().map(|r| &r.integration_type) } }))
        .origin(&audit_origin.origin_type, &audit_origin.origin_ref, &audit_origin.origin_reason)
        .caller(&audit_caller.caller_type, &audit_caller.key_label, &audit_caller.key_prefix)
        .success()
        .log(&state.clickhouse)
        .await;

    Ok(Json(serde_json::json!({
        "message": format!("GCP integration {} deleted", id)
    })))
}
