//! AWS integrations API endpoints
//!
//! Provides endpoints for configuring and managing AWS service integrations

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
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

pub fn create_aws_router() -> Router<Arc<WatchState>> {
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
pub struct AwsIntegration {
    pub id: Uuid,
    pub project_id: Uuid,
    pub name: String,
    pub integration_type: String,
    pub region: String,
    pub enabled: bool,
    pub collection_interval_seconds: i32,
    pub config_jsonb: serde_json::Value,
    pub role_arn: Option<String>,    // IAM role ARN (preferred)
    pub external_id: Option<String>, // External ID for role assumption
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, sqlx::FromRow)]
struct IntegrationRow {
    id: Uuid,
    project_id: Uuid,
    name: String,
    integration_type: String,
    region: String,
    enabled: bool,
    collection_interval_seconds: i32,
    config_jsonb: serde_json::Value,
    role_arn: Option<String>,    // IAM role ARN (preferred)
    external_id: Option<String>, // External ID for role assumption
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<IntegrationRow> for AwsIntegration {
    fn from(row: IntegrationRow) -> Self {
        AwsIntegration {
            id: row.id,
            project_id: row.project_id,
            name: row.name,
            integration_type: row.integration_type,
            region: row.region,
            enabled: row.enabled,
            collection_interval_seconds: row.collection_interval_seconds,
            config_jsonb: row.config_jsonb,
            role_arn: row.role_arn,
            external_id: row.external_id,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateAwsIntegrationRequest {
    pub name: String,
    pub integration_type: String, // 'ec2', 'lambda', etc.
    pub region: String,

    // IAM Role Delegation (preferred method, like Datadog)
    pub role_arn: Option<String>,    // IAM role ARN to assume
    pub external_id: Option<String>, // External ID for role assumption

    pub config_jsonb: Option<serde_json::Value>,
    pub enabled: Option<bool>,
    pub collection_interval_seconds: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateAwsIntegrationRequest {
    pub name: Option<String>,
    pub region: Option<String>,

    // IAM Role Delegation (preferred method, like Datadog)
    pub role_arn: Option<String>,    // IAM role ARN to assume
    pub external_id: Option<String>, // External ID for role assumption

    pub config_jsonb: Option<serde_json::Value>,
    pub enabled: Option<bool>,
    pub collection_interval_seconds: Option<i32>,
}

/// List all AWS integrations for a project
/// GET /api/aws/integrations?project_id=...
async fn list_integrations(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<AwsIntegration>>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    let rows = sqlx::query_as::<_, IntegrationRow>(
        r#"
               SELECT 
                   id,
                   project_id,
                   name,
                   integration_type,
                   region,
                   enabled,
                   collection_interval_seconds,
                   config_jsonb,
                   role_arn,
                   external_id,
                   created_at,
                   updated_at
               FROM aws_integration_configs
               WHERE project_id = $1
               ORDER BY created_at DESC
               "#,
    )
    .bind(project_id)
    .fetch_all(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to list AWS integrations: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?;

    let integrations: Vec<AwsIntegration> = rows.into_iter().map(|row| row.into()).collect();

    Ok(Json(integrations))
}

/// Create a new AWS integration
/// POST /api/aws/integrations
async fn create_integration(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<CreateAwsIntegrationRequest>,
) -> Result<Json<AwsIntegration>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    // Validate integration_type
    match payload.integration_type.as_str() {
        "ec2"
        | "lambda"
        | "s3"
        | "rds"
        | "dynamodb"
        | "elasticache"
        | "ecs"
        | "eks"
        | "sqs"
        | "sns"
        | "kinesis"
        | "apigateway"
        | "cloudfront"
        | "route53"
        | "cloudtrail"
        | "iam_access_analyzer" => {}
        _ => {
            return Err(AppError::Validation(format!(
                "Invalid integration_type: {}",
                payload.integration_type
            )));
        }
    }

    let row = sqlx::query_as::<_, IntegrationRow>(
        r#"
        INSERT INTO aws_integration_configs (
            project_id, name, integration_type, region,
            role_arn, external_id,
            config_jsonb, enabled, collection_interval_seconds
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING 
            id,
            project_id,
            name,
            integration_type,
            region,
            enabled,
            collection_interval_seconds,
            config_jsonb,
            role_arn,
            external_id,
            created_at,
            updated_at
        "#,
    )
    .bind(project_id)
    .bind(&payload.name)
    .bind(&payload.integration_type)
    .bind(&payload.region)
    .bind(payload.role_arn.as_deref())
    .bind(payload.external_id.as_deref())
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
        error!("Failed to create AWS integration: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?;

    let integration: AwsIntegration = row.into();

    info!(
        "Created AWS integration: id={}, type={}, project_id={}",
        integration.id, integration.integration_type, project_id
    );

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::IntegrationCreated)
        .resource("aws", integration.id)
        .details(serde_json::json!({ "created": { "name": &payload.name, "integration_type": &payload.integration_type } }))
        .origin(&audit_origin.origin_type, &audit_origin.origin_ref, &audit_origin.origin_reason)
        .caller(&audit_caller.caller_type, &audit_caller.key_label, &audit_caller.key_prefix)
        .success()
        .log(&state.clickhouse)
        .await;

    Ok(Json(integration))
}

/// Get a specific AWS integration
/// GET /api/aws/integrations/{id}
async fn get_integration(
    State(state): State<Arc<WatchState>>,
    Path(id): Path<Uuid>,
) -> Result<Json<AwsIntegration>> {
    let row = sqlx::query_as::<_, IntegrationRow>(
        r#"
               SELECT 
                   id,
                   project_id,
                   name,
                   integration_type,
                   region,
                   enabled,
                   collection_interval_seconds,
                   config_jsonb,
                   role_arn,
                   external_id,
                   created_at,
                   updated_at
               FROM aws_integration_configs
               WHERE id = $1
               "#,
    )
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to get AWS integration: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?
    .ok_or_else(|| AppError::NotFound("AWS integration not found".to_string()))?;

    Ok(Json(row.into()))
}

/// Update an AWS integration
/// PUT /api/aws/integrations/{id}
async fn update_integration(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(payload): Json<UpdateAwsIntegrationRequest>,
) -> Result<Json<AwsIntegration>> {
    let before_row = sqlx::query_as::<_, IntegrationRow>(
        "SELECT id, project_id, name, integration_type, region, enabled, collection_interval_seconds, config_jsonb, role_arn, external_id, created_at, updated_at FROM aws_integration_configs WHERE id = $1"
    )
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .ok()
    .flatten();

    let row = sqlx::query_as::<_, IntegrationRow>(
        r#"
               UPDATE aws_integration_configs
               SET 
                   name = COALESCE($1, name),
                   region = COALESCE($2, region),
                   role_arn = COALESCE($3, role_arn),
                   external_id = COALESCE($4, external_id),
                   config_jsonb = COALESCE($5, config_jsonb),
                   enabled = COALESCE($6, enabled),
                   collection_interval_seconds = COALESCE($7, collection_interval_seconds),
                   updated_at = NOW()
               WHERE id = $8
               RETURNING 
                   id,
                   project_id,
                   name,
                   integration_type,
                   region,
                   enabled,
                   collection_interval_seconds,
                   config_jsonb,
                   role_arn,
                   external_id,
                   created_at,
                   updated_at
               "#,
    )
    .bind(payload.name.as_deref())
    .bind(payload.region.as_deref())
    .bind(payload.role_arn.as_deref())
    .bind(payload.external_id.as_deref())
    .bind(payload.config_jsonb)
    .bind(payload.enabled)
    .bind(payload.collection_interval_seconds)
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to update AWS integration: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?
    .ok_or_else(|| AppError::NotFound("AWS integration not found".to_string()))?;

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::IntegrationUpdated)
        .resource("aws", id)
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

/// Delete an AWS integration
/// DELETE /api/aws/integrations/{id}
async fn delete_integration(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<StatusCode> {
    let deleted_row = sqlx::query_as::<_, IntegrationRow>(
        "SELECT id, project_id, name, integration_type, region, enabled, collection_interval_seconds, config_jsonb, role_arn, external_id, created_at, updated_at FROM aws_integration_configs WHERE id = $1"
    )
    .bind(id)
    .fetch_optional(&*state.db)
    .await
    .ok()
    .flatten();

    let result = sqlx::query("DELETE FROM aws_integration_configs WHERE id = $1")
        .bind(id)
        .execute(&*state.db)
        .await
        .map_err(|e| {
            error!("Failed to delete AWS integration: {}", e);
            AppError::Internal(anyhow::anyhow!("Database error: {}", e))
        })?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound("AWS integration not found".to_string()));
    }

    info!("Deleted AWS integration: id={}", id);

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::IntegrationDeleted)
        .resource("aws", id)
        .details(serde_json::json!({ "deleted": { "name": deleted_row.as_ref().map(|r| &r.name), "integration_type": deleted_row.as_ref().map(|r| &r.integration_type) } }))
        .origin(&audit_origin.origin_type, &audit_origin.origin_ref, &audit_origin.origin_reason)
        .caller(&audit_caller.caller_type, &audit_caller.key_label, &audit_caller.key_prefix)
        .success()
        .log(&state.clickhouse)
        .await;

    Ok(StatusCode::NO_CONTENT)
}
