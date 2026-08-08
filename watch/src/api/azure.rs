//! Azure integrations API endpoints
//!
//! Provides endpoints for configuring and managing Azure service integrations

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

pub fn create_azure_router() -> Router<Arc<WatchState>> {
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
pub struct AzureIntegration {
    pub id: Uuid,
    pub project_id: Uuid,
    pub name: String,
    pub integration_type: String,
    pub subscription_id: String,
    pub enabled: bool,
    pub collection_interval_seconds: i32,
    pub config_jsonb: serde_json::Value,
    pub tenant_id: Option<String>,
    pub client_id: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, sqlx::FromRow)]
struct IntegrationRow {
    id: Uuid,
    project_id: Uuid,
    name: String,
    integration_type: String,
    subscription_id: String,
    enabled: bool,
    collection_interval_seconds: i32,
    config_jsonb: serde_json::Value,
    tenant_id: Option<String>,
    client_id: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<IntegrationRow> for AzureIntegration {
    fn from(row: IntegrationRow) -> Self {
        AzureIntegration {
            id: row.id,
            project_id: row.project_id,
            name: row.name,
            integration_type: row.integration_type,
            subscription_id: row.subscription_id,
            enabled: row.enabled,
            collection_interval_seconds: row.collection_interval_seconds,
            config_jsonb: row.config_jsonb,
            tenant_id: row.tenant_id,
            client_id: row.client_id,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateAzureIntegrationRequest {
    pub name: String,
    pub integration_type: String, // 'vm', etc.
    pub subscription_id: String,

    // Service Principal (preferred method)
    pub tenant_id: Option<String>,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,

    pub config_jsonb: Option<serde_json::Value>,
    pub enabled: Option<bool>,
    pub collection_interval_seconds: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateAzureIntegrationRequest {
    pub name: Option<String>,
    pub subscription_id: Option<String>,

    // Service Principal (preferred method)
    pub tenant_id: Option<String>,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,

    pub config_jsonb: Option<serde_json::Value>,
    pub enabled: Option<bool>,
    pub collection_interval_seconds: Option<i32>,
}

/// List all Azure integrations for a project
/// GET /api/azure/integrations?project_id=...
async fn list_integrations(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<AzureIntegration>>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    let rows = sqlx::query_as::<_, IntegrationRow>(
        r#"
        SELECT 
            id,
            project_id,
            name,
            integration_type,
            subscription_id,
            enabled,
            collection_interval_seconds,
            config_jsonb,
            tenant_id,
            client_id,
            created_at,
            updated_at
        FROM azure_integration_configs
        WHERE project_id = $1
        ORDER BY created_at DESC
        "#,
    )
    .bind(project_id)
    .fetch_all(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to list Azure integrations: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?;

    let integrations: Vec<AzureIntegration> = rows.into_iter().map(|row| row.into()).collect();

    Ok(Json(integrations))
}

/// Create a new Azure integration
/// POST /api/azure/integrations
async fn create_integration(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<CreateAzureIntegrationRequest>,
) -> Result<Json<AzureIntegration>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    // Validate integration_type
    match payload.integration_type.as_str() {
        "vm"
        | "functions"
        | "blob_storage"
        | "sql_database"
        | "cosmosdb"
        | "redis_cache"
        | "container_instances"
        | "aks"
        | "app_services"
        | "service_bus"
        | "event_hub"
        | "api_management"
        | "application_gateway" => {}
        _ => {
            return Err(AppError::Validation(format!(
                "Invalid integration_type: {}",
                payload.integration_type
            )));
        }
    }

    let row = sqlx::query_as::<_, IntegrationRow>(
        r#"
        INSERT INTO azure_integration_configs (
            project_id, name, integration_type, subscription_id,
            tenant_id, client_id, client_secret,
            config_jsonb, enabled, collection_interval_seconds
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        RETURNING 
            id,
            project_id,
            name,
            integration_type,
            subscription_id,
            enabled,
            collection_interval_seconds,
            config_jsonb,
            tenant_id,
            client_id,
            created_at,
            updated_at
        "#,
    )
    .bind(project_id)
    .bind(&payload.name)
    .bind(&payload.integration_type)
    .bind(&payload.subscription_id)
    .bind(payload.tenant_id.as_deref())
    .bind(payload.client_id.as_deref())
    .bind(payload.client_secret.as_deref()) // In production, encrypt this
    .bind(
        payload
            .config_jsonb
            .unwrap_or_else(|| serde_json::json!({})),
    )
    .bind(payload.enabled.unwrap_or(true))
    .bind(payload.collection_interval_seconds.unwrap_or(300))
    .fetch_one(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to create Azure integration: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?;

    let integration: AzureIntegration = row.into();

    info!(
        "Created Azure integration: id={}, type={}, project_id={}",
        integration.id, integration.integration_type, project_id
    );

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::IntegrationCreated)
        .resource("azure", integration.id)
        .details(serde_json::json!({ "created": { "name": &payload.name, "integration_type": &payload.integration_type } }))
        .origin(&audit_origin.origin_type, &audit_origin.origin_ref, &audit_origin.origin_reason)
        .caller(&audit_caller.caller_type, &audit_caller.key_label, &audit_caller.key_prefix)
        .success()
        .log(&state.clickhouse)
        .await;

    Ok(Json(integration))
}

/// Get a specific Azure integration
/// GET /api/azure/integrations/{id}
async fn get_integration(
    State(state): State<Arc<WatchState>>,
    Path(id): Path<Uuid>,
) -> Result<Json<AzureIntegration>> {
    let row = sqlx::query_as::<_, IntegrationRow>(
        r#"
        SELECT 
            id,
            project_id,
            name,
            integration_type,
            subscription_id,
            enabled,
            collection_interval_seconds,
            config_jsonb,
            tenant_id,
            client_id,
            created_at,
            updated_at
        FROM azure_integration_configs
        WHERE id = $1
        "#,
    )
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to get Azure integration: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?
    .ok_or_else(|| AppError::NotFound("Azure integration not found".to_string()))?;

    Ok(Json(row.into()))
}

/// Update an Azure integration
/// PUT /api/azure/integrations/{id}
async fn update_integration(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(payload): Json<UpdateAzureIntegrationRequest>,
) -> Result<Json<AzureIntegration>> {
    let before_row = sqlx::query_as::<_, IntegrationRow>(
        "SELECT id, project_id, name, integration_type, subscription_id, enabled, collection_interval_seconds, config_jsonb, tenant_id, client_id, created_at, updated_at FROM azure_integration_configs WHERE id = $1"
    )
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .ok()
    .flatten();

    let row = sqlx::query_as::<_, IntegrationRow>(
        r#"
        UPDATE azure_integration_configs
        SET 
            name = COALESCE($1, name),
            subscription_id = COALESCE($2, subscription_id),
            tenant_id = COALESCE($3, tenant_id),
            client_id = COALESCE($4, client_id),
            client_secret = COALESCE($5, client_secret),
            config_jsonb = COALESCE($6, config_jsonb),
            enabled = COALESCE($7, enabled),
            collection_interval_seconds = COALESCE($8, collection_interval_seconds),
            updated_at = NOW()
        WHERE id = $9
        RETURNING 
            id,
            project_id,
            name,
            integration_type,
            subscription_id,
            enabled,
            collection_interval_seconds,
            config_jsonb,
            tenant_id,
            client_id,
            created_at,
            updated_at
        "#,
    )
    .bind(payload.name.as_deref())
    .bind(payload.subscription_id.as_deref())
    .bind(payload.tenant_id.as_deref())
    .bind(payload.client_id.as_deref())
    .bind(payload.client_secret.as_deref())
    .bind(payload.config_jsonb.as_ref())
    .bind(payload.enabled)
    .bind(payload.collection_interval_seconds)
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to update Azure integration: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?
    .ok_or_else(|| AppError::NotFound("Azure integration not found".to_string()))?;

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::IntegrationUpdated)
        .resource("azure", id)
        .details(serde_json::json!({
            "before": { "name": before_row.as_ref().map(|r| &r.name), "enabled": before_row.as_ref().map(|r| r.enabled) },
            "after": { "name": &row.name, "enabled": row.enabled }
        }))
        .origin(&audit_origin.origin_type, &audit_origin.origin_ref, &audit_origin.origin_reason)
        .caller(&audit_caller.caller_type, &audit_caller.key_label, &audit_caller.key_prefix)
        .success()
        .log(&state.clickhouse)
        .await;

    Ok(Json(row.into()))
}

/// Delete an Azure integration
/// DELETE /api/azure/integrations/{id}
async fn delete_integration(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>> {
    let deleted_row = sqlx::query_as::<_, IntegrationRow>(
        "SELECT id, project_id, name, integration_type, subscription_id, enabled, collection_interval_seconds, config_jsonb, tenant_id, client_id, created_at, updated_at FROM azure_integration_configs WHERE id = $1"
    )
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .ok()
    .flatten();

    let rows_affected = sqlx::query("DELETE FROM azure_integration_configs WHERE id = $1")
        .bind(id)
        .execute(&*state.db)
        .await
        .map_err(|e| {
            error!("Failed to delete Azure integration: {}", e);
            AppError::Internal(anyhow::anyhow!("Database error: {}", e))
        })?
        .rows_affected();

    if rows_affected == 0 {
        return Err(AppError::NotFound(
            "Azure integration not found".to_string(),
        ));
    }

    info!("Deleted Azure integration: id={}", id);

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::IntegrationDeleted)
        .resource("azure", id)
        .details(serde_json::json!({ "deleted": { "name": deleted_row.as_ref().map(|r| &r.name), "integration_type": deleted_row.as_ref().map(|r| &r.integration_type) } }))
        .origin(&audit_origin.origin_type, &audit_origin.origin_ref, &audit_origin.origin_reason)
        .caller(&audit_caller.caller_type, &audit_caller.key_label, &audit_caller.key_prefix)
        .success()
        .log(&state.clickhouse)
        .await;

    Ok(Json(serde_json::json!({
        "id": id,
        "message": "Azure integration deleted"
    })))
}
