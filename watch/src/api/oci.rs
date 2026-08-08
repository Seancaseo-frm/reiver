//! OCI integrations API endpoints
//!
//! Provides endpoints for configuring and managing OCI service integrations

use axum::{
    extract::{Path, State},
    http::HeaderMap,
    response::Json,
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

use reiver_core::audit::{AuditCaller, AuditEventBuilder, AuditEventType, AuditOrigin};

use crate::app_state::WatchState;
use crate::error::{AppError, Result};

pub fn create_oci_router() -> Router<Arc<WatchState>> {
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
pub struct OciIntegration {
    pub id: Uuid,
    pub project_id: Uuid,
    pub name: String,
    pub integration_type: String,
    pub tenancy_ocid: String,
    pub user_ocid: String,
    pub fingerprint: String,
    pub region: String,
    pub enabled: bool,
    pub collection_interval_seconds: i32,
    pub config_jsonb: serde_json::Value,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, sqlx::FromRow)]
struct IntegrationRow {
    id: Uuid,
    project_id: Uuid,
    name: String,
    integration_type: String,
    tenancy_ocid: String,
    user_ocid: String,
    fingerprint: String,
    region: String,
    enabled: bool,
    collection_interval_seconds: i32,
    config_jsonb: serde_json::Value,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<IntegrationRow> for OciIntegration {
    fn from(row: IntegrationRow) -> Self {
        OciIntegration {
            id: row.id,
            project_id: row.project_id,
            name: row.name,
            integration_type: row.integration_type,
            tenancy_ocid: row.tenancy_ocid,
            user_ocid: row.user_ocid,
            fingerprint: row.fingerprint,
            region: row.region,
            enabled: row.enabled,
            collection_interval_seconds: row.collection_interval_seconds,
            config_jsonb: row.config_jsonb,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateOciIntegrationRequest {
    pub name: String,
    pub integration_type: String, // 'compute', etc.
    pub tenancy_ocid: String,
    pub user_ocid: String,
    pub fingerprint: String,
    pub private_key: String,
    pub region: String,
    pub passphrase: Option<String>,
    pub config_jsonb: Option<serde_json::Value>,
    pub enabled: Option<bool>,
    pub collection_interval_seconds: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateOciIntegrationRequest {
    pub name: Option<String>,
    pub tenancy_ocid: Option<String>,
    pub user_ocid: Option<String>,
    pub fingerprint: Option<String>,
    pub private_key: Option<String>,
    pub region: Option<String>,
    pub passphrase: Option<String>,
    pub config_jsonb: Option<serde_json::Value>,
    pub enabled: Option<bool>,
    pub collection_interval_seconds: Option<i32>,
}

/// GET /api/oci/integrations
async fn list_integrations(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<OciIntegration>>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    let rows: Vec<IntegrationRow> = sqlx::query_as::<_, IntegrationRow>(
        r#"
        SELECT 
            id,
            project_id,
            name,
            integration_type,
            tenancy_ocid,
            user_ocid,
            fingerprint,
            region,
            enabled,
            collection_interval_seconds,
            config_jsonb,
            created_at,
            updated_at
        FROM oci_integration_configs
        WHERE project_id = $1
        ORDER BY created_at DESC
        "#,
    )
    .bind(project_id)
    .fetch_all(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to fetch OCI integrations: {}", e)))?;

    let integrations: Vec<OciIntegration> = rows.into_iter().map(|r| r.into()).collect();
    Ok(Json(integrations))
}

/// POST /api/oci/integrations
async fn create_integration(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<CreateOciIntegrationRequest>,
) -> Result<Json<OciIntegration>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    // Validate integration_type
    match payload.integration_type.as_str() {
        "compute"
        | "functions"
        | "object_storage"
        | "objectstorage"
        | "database"
        | "autonomous_database"
        | "autonomousdatabase"
        | "container_instances"
        | "containerinstances"
        | "oke"
        | "oke_cluster"
        | "kubernetes"
        | "kubernetes_engine"
        | "load_balancer"
        | "loadbalancer"
        | "lbaas" => {}
        _ => {
            return Err(AppError::Validation(format!(
                "Invalid integration_type: {}",
                payload.integration_type
            )));
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
        INSERT INTO oci_integration_configs (
            id,
            project_id,
            name,
            integration_type,
            tenancy_ocid,
            user_ocid,
            fingerprint,
            private_key,
            region,
            passphrase,
            enabled,
            collection_interval_seconds,
            config_jsonb
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        "#,
    )
    .bind(&id)
    .bind(&project_id)
    .bind(&payload.name)
    .bind(&payload.integration_type)
    .bind(&payload.tenancy_ocid)
    .bind(&payload.user_ocid)
    .bind(&payload.fingerprint)
    .bind(&payload.private_key)
    .bind(&payload.region)
    .bind(&payload.passphrase)
    .bind(enabled)
    .bind(collection_interval_seconds)
    .bind(&config_jsonb)
    .execute(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to create OCI integration: {}", e)))?;

    // Fetch the created integration
    let row: IntegrationRow = sqlx::query_as::<_, IntegrationRow>(
        r#"
        SELECT 
            id,
            project_id,
            name,
            integration_type,
            tenancy_ocid,
            user_ocid,
            fingerprint,
            region,
            enabled,
            collection_interval_seconds,
            config_jsonb,
            created_at,
            updated_at
        FROM oci_integration_configs
        WHERE id = $1
        "#,
    )
    .bind(&id)
    .fetch_one(&*state.db)
    .await
    .map_err(|e| {
        AppError::Internal(anyhow::anyhow!(
            "Failed to fetch created OCI integration: {}",
            e
        ))
    })?;

    info!("Created OCI integration: {} ({})", row.name, row.id);

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::IntegrationCreated)
        .resource("oci", row.id)
        .details(serde_json::json!({ "created": { "name": &row.name, "integration_type": &row.integration_type } }))
        .origin(&audit_origin.origin_type, &audit_origin.origin_ref, &audit_origin.origin_reason)
        .caller(&audit_caller.caller_type, &audit_caller.key_label, &audit_caller.key_prefix)
        .success()
        .log(&state.clickhouse)
        .await;

    Ok(Json(row.into()))
}

/// GET /api/oci/integrations/{id}
async fn get_integration(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<OciIntegration>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    let row: IntegrationRow = sqlx::query_as::<_, IntegrationRow>(
        r#"
        SELECT 
            id,
            project_id,
            name,
            integration_type,
            tenancy_ocid,
            user_ocid,
            fingerprint,
            region,
            enabled,
            collection_interval_seconds,
            config_jsonb,
            created_at,
            updated_at
        FROM oci_integration_configs
        WHERE id = $1 AND project_id = $2
        "#,
    )
    .bind(&id)
    .bind(&project_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to fetch OCI integration: {}", e)))?
    .ok_or_else(|| AppError::NotFound(format!("OCI integration not found: {}", id)))?;

    Ok(Json(row.into()))
}

/// PUT /api/oci/integrations/{id}
async fn update_integration(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(payload): Json<UpdateOciIntegrationRequest>,
) -> Result<Json<OciIntegration>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    let before_row = sqlx::query_as::<_, IntegrationRow>(
        "SELECT id, project_id, name, integration_type, tenancy_ocid, user_ocid, fingerprint, region, enabled, collection_interval_seconds, config_jsonb, created_at, updated_at FROM oci_integration_configs WHERE id = $1 AND project_id = $2"
    )
    .bind(&id)
    .bind(&project_id)
    .fetch_optional(&*state.db)
    .await
    .ok()
    .flatten();

    // Build UPDATE query dynamically based on what's provided
    let mut updates = Vec::new();
    let mut bind_idx = 1;

    if payload.name.is_some() {
        updates.push(format!("name = ${}", bind_idx));
        bind_idx += 1;
    }
    if payload.tenancy_ocid.is_some() {
        updates.push(format!("tenancy_ocid = ${}", bind_idx));
        bind_idx += 1;
    }
    if payload.user_ocid.is_some() {
        updates.push(format!("user_ocid = ${}", bind_idx));
        bind_idx += 1;
    }
    if payload.fingerprint.is_some() {
        updates.push(format!("fingerprint = ${}", bind_idx));
        bind_idx += 1;
    }
    if payload.private_key.is_some() {
        updates.push(format!("private_key = ${}", bind_idx));
        bind_idx += 1;
    }
    if payload.region.is_some() {
        updates.push(format!("region = ${}", bind_idx));
        bind_idx += 1;
    }
    if payload.passphrase.is_some() {
        updates.push(format!("passphrase = ${}", bind_idx));
        bind_idx += 1;
    }
    if payload.config_jsonb.is_some() {
        updates.push(format!("config_jsonb = ${}", bind_idx));
        bind_idx += 1;
    }
    if payload.enabled.is_some() {
        updates.push(format!("enabled = ${}", bind_idx));
        bind_idx += 1;
    }
    if payload.collection_interval_seconds.is_some() {
        updates.push(format!("collection_interval_seconds = ${}", bind_idx));
        bind_idx += 1;
    }

    if updates.is_empty() {
        return Err(AppError::Validation("No fields to update".to_string()));
    }

    updates.push(format!("updated_at = ${}", bind_idx));
    bind_idx += 1;

    let update_sql = format!(
        "UPDATE oci_integration_configs SET {} WHERE id = ${} AND project_id = ${}",
        updates.join(", "),
        bind_idx,
        bind_idx + 1
    );

    let mut query = sqlx::query(&update_sql);

    if let Some(ref name) = payload.name {
        query = query.bind(name);
    }
    if let Some(ref tenancy_ocid) = payload.tenancy_ocid {
        query = query.bind(tenancy_ocid);
    }
    if let Some(ref user_ocid) = payload.user_ocid {
        query = query.bind(user_ocid);
    }
    if let Some(ref fingerprint) = payload.fingerprint {
        query = query.bind(fingerprint);
    }
    if let Some(ref private_key) = payload.private_key {
        query = query.bind(private_key);
    }
    if let Some(ref region) = payload.region {
        query = query.bind(region);
    }
    if let Some(ref passphrase) = payload.passphrase {
        query = query.bind(passphrase);
    }
    if let Some(ref config_jsonb) = payload.config_jsonb {
        query = query.bind(config_jsonb);
    }
    if let Some(enabled) = payload.enabled {
        query = query.bind(enabled);
    }
    if let Some(collection_interval_seconds) = payload.collection_interval_seconds {
        query = query.bind(collection_interval_seconds);
    }
    query = query.bind(chrono::Utc::now());
    query = query.bind(&id);
    query = query.bind(&project_id);

    let rows_affected = query
        .execute(&*state.db)
        .await
        .map_err(|e| {
            AppError::Internal(anyhow::anyhow!("Failed to update OCI integration: {}", e))
        })?
        .rows_affected();

    if rows_affected == 0 {
        return Err(AppError::NotFound(format!(
            "OCI integration not found: {}",
            id
        )));
    }

    // Fetch the updated integration
    let row: IntegrationRow = sqlx::query_as::<_, IntegrationRow>(
        r#"
        SELECT 
            id,
            project_id,
            name,
            integration_type,
            tenancy_ocid,
            user_ocid,
            fingerprint,
            region,
            enabled,
            collection_interval_seconds,
            config_jsonb,
            created_at,
            updated_at
        FROM oci_integration_configs
        WHERE id = $1
        "#,
    )
    .bind(&id)
    .fetch_one(&*state.db)
    .await
    .map_err(|e| {
        AppError::Internal(anyhow::anyhow!(
            "Failed to fetch updated OCI integration: {}",
            e
        ))
    })?;

    info!("Updated OCI integration: {} ({})", row.name, row.id);

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::IntegrationUpdated)
        .resource("oci", id)
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

/// DELETE /api/oci/integrations/{id}
async fn delete_integration(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<axum::http::StatusCode> {
    let project_id = crate::api::extract_project_id(&headers)?;

    let deleted_row = sqlx::query_as::<_, IntegrationRow>(
        "SELECT id, project_id, name, integration_type, tenancy_ocid, user_ocid, fingerprint, region, enabled, collection_interval_seconds, config_jsonb, created_at, updated_at FROM oci_integration_configs WHERE id = $1 AND project_id = $2"
    )
    .bind(&id)
    .bind(&project_id)
    .fetch_optional(&*state.db)
    .await
    .ok()
    .flatten();

    let rows_affected =
        sqlx::query("DELETE FROM oci_integration_configs WHERE id = $1 AND project_id = $2")
            .bind(&id)
            .bind(&project_id)
            .execute(&*state.db)
            .await
            .map_err(|e| {
                AppError::Internal(anyhow::anyhow!("Failed to delete OCI integration: {}", e))
            })?
            .rows_affected();

    if rows_affected == 0 {
        return Err(AppError::NotFound(format!(
            "OCI integration not found: {}",
            id
        )));
    }

    info!("Deleted OCI integration: {}", id);

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::IntegrationDeleted)
        .resource("oci", id)
        .details(serde_json::json!({ "deleted": { "name": deleted_row.as_ref().map(|r| &r.name), "integration_type": deleted_row.as_ref().map(|r| &r.integration_type) } }))
        .origin(&audit_origin.origin_type, &audit_origin.origin_ref, &audit_origin.origin_reason)
        .caller(&audit_caller.caller_type, &audit_caller.key_label, &audit_caller.key_prefix)
        .success()
        .log(&state.clickhouse)
        .await;

    Ok(axum::http::StatusCode::NO_CONTENT)
}
