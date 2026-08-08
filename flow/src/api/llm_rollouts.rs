//! LLM Prompt Rollouts API
//!
//! Endpoints for managing prompt configurations, versions, and progressive rollouts.
//! Supports A/B testing with automatic promotion and rollback based on metrics.
//!
//! # SQL Injection Safety
//!
//! This module uses parameterized queries for PostgreSQL. ClickHouse queries use
//! string interpolation with strongly-typed UUIDs and escaped strings.

use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use crate::api::{extract_organization_id, extract_user_id};
use crate::app_state::FlowState;
use crate::audit::{AuditCaller, AuditEventBuilder, AuditEventType, AuditOrigin};
use crate::error::{AppError, Result};
use crate::gateway::domain_types::{ComparisonStatus, RolloutStageStatus, RolloutStatus};
use crate::gateway::VariableDefinition;
use crate::llm::cache::{invalidate_prompt_version_cache, invalidate_rollout_cache};
use crate::llm::types::{RolloutVariant, VariantMetrics};
use crate::rollout_worker::{DEFAULT_MAX_ERROR_RATE_INCREASE, DEFAULT_MAX_LATENCY_INCREASE_PCT};

const MAX_NAME_LENGTH: usize = 255;
const DEFAULT_MIN_REQUESTS: i32 = 100;
const COMPLETED_WEIGHT: i32 = 100;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RolloutMode {
    Auto,
    Manual,
}

impl Default for RolloutMode {
    fn default() -> Self {
        RolloutMode::Auto
    }
}

impl RolloutMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            RolloutMode::Auto => "auto",
            RolloutMode::Manual => "manual",
        }
    }
}

pub use crate::gateway::domain_types::AllocationType;

pub fn create_llm_rollouts_router() -> Router<Arc<FlowState>> {
    Router::new()
        // Prompt configs
        .route("/configs", post(create_config).get(list_configs))
        .route(
            "/configs/{config_id}",
            get(get_config).put(update_config).delete(delete_config),
        )
        .route(
            "/configs/{config_id}/versions",
            post(create_version).get(list_versions),
        )
        .route(
            "/configs/{config_id}/versions/{version_id}",
            get(get_version),
        )
        // Prompt Compiler
        .route("/configs/{config_id}/compile", post(trigger_compile))
        // Rollouts
        .route("/rollouts", post(create_rollout).get(list_rollouts))
        .route("/rollouts/{rollout_id}", get(get_rollout))
        .route("/rollouts/{rollout_id}/start", post(start_rollout))
        .route("/rollouts/{rollout_id}/pause", post(pause_rollout))
        .route("/rollouts/{rollout_id}/promote", post(promote_rollout))
        .route("/rollouts/{rollout_id}/rollback", post(rollback_rollout))
        .route("/rollouts/{rollout_id}/complete", post(complete_rollout))
        .route("/rollouts/{rollout_id}/metrics", get(get_rollout_metrics))
}

// ============================================================================
// Prompt Configs
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct CreateConfigRequest {
    pub project_id: Uuid,
    pub name: String,
    pub description: Option<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct PromptConfig {
    pub id: Uuid,
    pub project_id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub active_version_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct PromptConfigWithVersion {
    #[serde(flatten)]
    pub config: PromptConfig,
    pub active_version: Option<i32>,
    pub version_count: i64,
    pub has_active_rollout: bool,
}

/// Database row type for prompt config queries with version info.
/// Used by list_configs and get_config to avoid duplicate struct definitions.
#[derive(sqlx::FromRow)]
struct PromptConfigRow {
    id: Uuid,
    project_id: Uuid,
    name: String,
    description: Option<String>,
    active_version_id: Option<Uuid>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    active_version: Option<i32>,
    version_count: i64,
    has_active_rollout: bool,
}

impl From<PromptConfigRow> for PromptConfigWithVersion {
    fn from(row: PromptConfigRow) -> Self {
        PromptConfigWithVersion {
            config: PromptConfig {
                id: row.id,
                project_id: row.project_id,
                name: row.name,
                description: row.description,
                active_version_id: row.active_version_id,
                created_at: row.created_at,
                updated_at: row.updated_at,
            },
            active_version: row.active_version,
            version_count: row.version_count,
            has_active_rollout: row.has_active_rollout,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ListConfigsParams {
    pub project_id: Uuid,
    #[serde(default = "crate::api::default_list_limit")]
    pub limit: u32,
    #[serde(default)]
    pub offset: u32,
}

const MAX_LIMIT: u32 = 100;

/// Create a new prompt config
async fn create_config(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    Json(req): Json<CreateConfigRequest>,
) -> Result<Json<PromptConfig>> {
    let user_id = extract_user_id(&headers).ok();

    // Validate name
    if req.name.trim().is_empty() {
        return Err(AppError::Validation("Name cannot be empty".to_string()));
    }
    if req.name.len() > MAX_NAME_LENGTH {
        return Err(AppError::Validation(format!(
            "Name cannot exceed {} characters",
            MAX_NAME_LENGTH
        )));
    }

    let config: PromptConfig = sqlx::query_as(
        r#"
        INSERT INTO llm_prompt_configs (project_id, name, description)
        VALUES ($1, $2, $3)
        RETURNING id, project_id, name, description, active_version_id, created_at, updated_at
        "#,
    )
    .bind(req.project_id)
    .bind(&req.name)
    .bind(&req.description)
    .fetch_one(state.db.as_ref())
    .await
    .map_err(|e| {
        if let sqlx::Error::Database(ref db_err) = e {
            if db_err.constraint() == Some("llm_prompt_configs_project_id_name_key") {
                return AppError::Validation(format!(
                    "A prompt config with name '{}' already exists in this project",
                    req.name
                ));
            }
        }
        AppError::Database(e)
    })?;

    let org_id = extract_organization_id(&headers);
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::PromptConfigCreated)
        .resource("prompt_config", config.id)
        .project(&req.project_id.to_string())
        .details(serde_json::json!({
            "created": {
                "name": &config.name,
                "description": &config.description,
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
    if let Some(uid) = user_id {
        audit = audit.user(uid);
    }
    if let Some(oid) = org_id {
        audit = audit.organization(oid);
    }
    audit.log(&state.clickhouse).await;

    Ok(Json(config))
}

/// List prompt configs for a project
async fn list_configs(
    State(state): State<Arc<FlowState>>,
    Query(params): Query<ListConfigsParams>,
) -> Result<Json<Vec<PromptConfigWithVersion>>> {
    let limit = params.limit.min(MAX_LIMIT);

    let configs: Vec<PromptConfigRow> = sqlx::query_as(
        &format!(
            r#"
            SELECT 
                c.id, c.project_id, c.name, c.description, c.active_version_id, c.created_at, c.updated_at,
                v.version as active_version,
                (SELECT COUNT(*) FROM llm_prompt_versions WHERE config_id = c.id) as version_count,
                EXISTS(SELECT 1 FROM llm_rollouts WHERE config_id = c.id AND status = '{}') as has_active_rollout
            FROM llm_prompt_configs c
            LEFT JOIN llm_prompt_versions v ON c.active_version_id = v.id
            WHERE c.project_id = $1
            ORDER BY c.updated_at DESC
            LIMIT $2 OFFSET $3
            "#,
            RolloutStatus::Running.as_str()
        ),
    )
    .bind(params.project_id)
    .bind(limit as i64)
    .bind(params.offset as i64)
    .fetch_all(state.db.as_ref())
    .await
    .map_err(AppError::Database)?;

    let result: Vec<PromptConfigWithVersion> = configs
        .into_iter()
        .map(PromptConfigWithVersion::from)
        .collect();

    Ok(Json(result))
}

#[derive(Debug, Deserialize)]
pub struct GetConfigParams {
    pub project_id: Uuid,
}

/// Get a single prompt config
async fn get_config(
    State(state): State<Arc<FlowState>>,
    Path(config_id): Path<Uuid>,
    Query(params): Query<GetConfigParams>,
) -> Result<Json<PromptConfigWithVersion>> {
    let config: PromptConfigRow = sqlx::query_as(
        &format!(
            r#"
            SELECT 
                c.id, c.project_id, c.name, c.description, c.active_version_id, c.created_at, c.updated_at,
                v.version as active_version,
                (SELECT COUNT(*) FROM llm_prompt_versions WHERE config_id = c.id) as version_count,
                EXISTS(SELECT 1 FROM llm_rollouts WHERE config_id = c.id AND status = '{}') as has_active_rollout
            FROM llm_prompt_configs c
            LEFT JOIN llm_prompt_versions v ON c.active_version_id = v.id
            WHERE c.id = $1 AND c.project_id = $2
            "#,
            RolloutStatus::Running.as_str()
        ),
    )
    .bind(config_id)
    .bind(params.project_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(AppError::Database)?
    .ok_or_else(|| AppError::NotFound("Prompt config not found".to_string()))?;

    Ok(Json(PromptConfigWithVersion::from(config)))
}

#[derive(Debug, Deserialize)]
pub struct UpdateConfigRequest {
    pub project_id: Uuid,
    pub name: Option<String>,
    pub description: Option<String>,
}

/// Update a prompt config
async fn update_config(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    Path(config_id): Path<Uuid>,
    Json(req): Json<UpdateConfigRequest>,
) -> Result<Json<PromptConfig>> {
    let user_id = extract_user_id(&headers).ok();
    let org_id = extract_organization_id(&headers);
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);

    let before: Option<PromptConfig> = sqlx::query_as(
        "SELECT id, project_id, name, description, active_version_id, created_at, updated_at FROM llm_prompt_configs WHERE id = $1 AND project_id = $2",
    )
    .bind(config_id)
    .bind(req.project_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(AppError::Database)?;

    // Validate name if provided
    if let Some(ref name) = req.name {
        if name.trim().is_empty() {
            return Err(AppError::Validation("Name cannot be empty".to_string()));
        }
        if name.len() > MAX_NAME_LENGTH {
            return Err(AppError::Validation(format!(
                "Name cannot exceed {} characters",
                MAX_NAME_LENGTH
            )));
        }
    }

    let config: PromptConfig = sqlx::query_as(
        r#"
        UPDATE llm_prompt_configs
        SET 
            name = COALESCE($3, name),
            description = COALESCE($4, description),
            updated_at = NOW()
        WHERE id = $1 AND project_id = $2
        RETURNING id, project_id, name, description, active_version_id, created_at, updated_at
        "#,
    )
    .bind(config_id)
    .bind(req.project_id)
    .bind(&req.name)
    .bind(&req.description)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(AppError::Database)?
    .ok_or_else(|| AppError::NotFound("Prompt config not found".to_string()))?;

    let mut audit = AuditEventBuilder::new(AuditEventType::PromptConfigUpdated)
        .resource("prompt_config", config_id)
        .project(&req.project_id.to_string())
        .details(serde_json::json!({
            "before": {
                "name": before.as_ref().map(|b| &b.name),
                "description": before.as_ref().and_then(|b| b.description.as_ref()),
            },
            "after": {
                "name": &config.name,
                "description": &config.description,
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
    if let Some(uid) = user_id {
        audit = audit.user(uid);
    }
    if let Some(oid) = org_id {
        audit = audit.organization(oid);
    }
    audit.log(&state.clickhouse).await;

    Ok(Json(config))
}

/// Delete a prompt config
async fn delete_config(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    Path(config_id): Path<Uuid>,
    Query(params): Query<GetConfigParams>,
) -> Result<Json<serde_json::Value>> {
    let user_id = extract_user_id(&headers).ok();
    let org_id = extract_organization_id(&headers);
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);

    let before_config: Option<PromptConfig> = sqlx::query_as(
        "SELECT id, project_id, name, description, active_version_id, created_at, updated_at FROM llm_prompt_configs WHERE id = $1 AND project_id = $2",
    )
    .bind(config_id)
    .bind(params.project_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(AppError::Database)?;

    // Check for active rollouts
    let has_active: bool = sqlx::query_scalar(&format!(
        "SELECT EXISTS(SELECT 1 FROM llm_rollouts WHERE config_id = $1 AND status = '{}')",
        RolloutStatus::Running.as_str()
    ))
    .bind(config_id)
    .fetch_one(state.db.as_ref())
    .await
    .map_err(AppError::Database)?;

    if has_active {
        return Err(AppError::Validation(
            "Cannot delete config with active rollout. Stop the rollout first.".to_string(),
        ));
    }

    // Collect version IDs before the CASCADE delete removes them, so we can
    // purge their Redis cache entries and avoid serving stale prompt data.
    let version_ids: Vec<Uuid> =
        sqlx::query_scalar("SELECT id FROM llm_prompt_versions WHERE config_id = $1")
            .bind(config_id)
            .fetch_all(state.db.as_ref())
            .await
            .map_err(AppError::Database)?;

    let result = sqlx::query("DELETE FROM llm_prompt_configs WHERE id = $1 AND project_id = $2")
        .bind(config_id)
        .bind(params.project_id)
        .execute(state.db.as_ref())
        .await
        .map_err(AppError::Database)?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound("Prompt config not found".to_string()));
    }

    invalidate_rollout_cache(&state.redis, params.project_id, config_id).await;
    invalidate_prompt_version_cache(&state.redis, &version_ids).await;

    let mut audit = AuditEventBuilder::new(AuditEventType::PromptConfigDeleted)
        .resource("prompt_config", config_id)
        .project(&params.project_id.to_string())
        .details(serde_json::json!({
            "deleted": {
                "name": before_config.as_ref().map(|c| &c.name),
                "description": before_config.as_ref().and_then(|c| c.description.as_ref()),
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
    if let Some(uid) = user_id {
        audit = audit.user(uid);
    }
    if let Some(oid) = org_id {
        audit = audit.organization(oid);
    }
    audit.log(&state.clickhouse).await;

    Ok(Json(serde_json::json!({ "success": true })))
}

// ============================================================================
// Prompt Versions
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct CreateVersionRequest {
    pub project_id: Uuid,
    pub system_prompt: Option<String>,
    pub model: Option<String>,
    pub temperature: Decimal,
    pub max_tokens: Option<i32>,
    pub parameters: Option<serde_json::Value>,
    /// Template variable definitions: [{name, description?, type, required, default?}]
    pub variables: Option<serde_json::Value>,
    /// OpenAI-compatible tool/function definitions for function-calling
    pub tools: Option<serde_json::Value>,
    /// JSON schema for structured output (response_format parameter)
    pub response_format: Option<serde_json::Value>,
    pub commit_message: String,
    /// Tool name whitelist. `null` = no restriction; `[]` = no tools allowed.
    pub allowed_tools: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct PromptVersion {
    pub id: Uuid,
    pub config_id: Uuid,
    pub version: i32,
    pub system_prompt: Option<String>,
    pub model: Option<String>,
    pub temperature: Decimal,
    pub max_tokens: Option<i32>,
    pub parameters: serde_json::Value,
    /// Template variable definitions: [{name, description?, type, required, default?}]
    #[serde(default)]
    pub variables: serde_json::Value,
    /// OpenAI-compatible tool/function definitions for function-calling
    pub tools: Option<serde_json::Value>,
    /// JSON schema for structured output (response_format parameter)
    pub response_format: Option<serde_json::Value>,
    pub commit_message: String,
    pub created_by: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    /// Tool name whitelist. `null` = no restriction; `[]` = no tools allowed.
    pub allowed_tools: Option<serde_json::Value>,
    /// "user", "agent", or "system"
    #[serde(default = "default_creator_type")]
    pub created_by_type: String,
    /// Human-readable label of the agent key that created this version.
    pub created_by_key_label: Option<String>,
}

#[allow(dead_code)]
fn default_creator_type() -> String {
    "user".to_string()
}

/// Maximum system prompt size (5MB, matches Langfuse)
const MAX_SYSTEM_PROMPT_SIZE: usize = 5 * 1024 * 1024;
/// Maximum tools definition size (256KB)
const MAX_TOOLS_SIZE: usize = 256 * 1024;
/// Maximum response_format schema size (64KB)
const MAX_RESPONSE_FORMAT_SIZE: usize = 64 * 1024;

/// Validate a CreateVersionRequest's fields before touching the database.
fn validate_version_request(req: &CreateVersionRequest) -> Result<()> {
    if let Some(ref prompt) = req.system_prompt {
        if prompt.len() > MAX_SYSTEM_PROMPT_SIZE {
            return Err(AppError::Validation(
                "System prompt exceeds 5MB limit".to_string(),
            ));
        }
    }

    if req.temperature < Decimal::ZERO || req.temperature > Decimal::ONE {
        return Err(AppError::Validation(
            "Temperature must be between 0 and 1".to_string(),
        ));
    }

    if req.commit_message.trim().is_empty() {
        return Err(AppError::Validation(
            "Commit message is required".to_string(),
        ));
    }

    if let Some(ref tools) = req.tools {
        let json_str = serde_json::to_string(tools).unwrap_or_default();
        if json_str.len() > MAX_TOOLS_SIZE {
            return Err(AppError::Validation(
                "Tools definition exceeds 256KB limit".to_string(),
            ));
        }
    }

    if let Some(ref response_format) = req.response_format {
        let json_str = serde_json::to_string(response_format).unwrap_or_default();
        if json_str.len() > MAX_RESPONSE_FORMAT_SIZE {
            return Err(AppError::Validation(
                "Response format schema exceeds 64KB limit".to_string(),
            ));
        }
    }

    if let Some(ref variables) = req.variables {
        let defs: Vec<VariableDefinition> = serde_json::from_value(variables.clone())
            .map_err(|e| AppError::Validation(format!("Invalid variables format: {}", e)))?;
        for def in &defs {
            def.validate_name().map_err(|e| AppError::Validation(e))?;
        }
    }

    if let Some(max_tokens) = req.max_tokens {
        if max_tokens < 1 || max_tokens > 1_000_000 {
            return Err(AppError::Validation(
                "max_tokens must be between 1 and 1,000,000".to_string(),
            ));
        }
    }

    Ok(())
}

/// Create a new prompt version.
///
/// Accepts callers without a human user (MCP agents, system callers).
/// `X-User-Id` is optional — when absent, `created_by` is NULL and
/// `created_by_type` comes from `X-Creator-Type` (e.g. "agent").
#[tracing::instrument(
    name = "prompts.create_version",
    skip_all,
    fields(config_id = %config_id, project_id = %req.project_id, caller_type, has_user_id)
)]
async fn create_version(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    Path(config_id): Path<Uuid>,
    Json(req): Json<CreateVersionRequest>,
) -> Result<Json<PromptVersion>> {
    let user_id = extract_user_id(&headers).ok();

    let created_by_type = headers
        .get("X-Creator-Type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("user")
        .to_string();
    let created_by_key_label: Option<String> = headers
        .get("X-Creator-Key-Label")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    tracing::Span::current().record("caller_type", created_by_type.as_str());
    tracing::Span::current().record("has_user_id", user_id.is_some());
    tracing::info!(
        caller_type = %created_by_type,
        has_user_id = user_id.is_some(),
        key_label = created_by_key_label.as_deref().unwrap_or(""),
        "creating prompt version"
    );

    validate_version_request(&req)?;

    // Use transaction with row locking to prevent race conditions in version numbering
    let mut tx = state.db.begin().await.map_err(AppError::Database)?;

    // Verify config exists and belongs to project, lock the row to prevent concurrent version creation
    let config_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM llm_prompt_configs WHERE id = $1 AND project_id = $2 FOR UPDATE)"
    )
    .bind(config_id)
    .bind(req.project_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(AppError::Database)?;

    if !config_exists {
        return Err(AppError::NotFound("Prompt config not found".to_string()));
    }

    // Get next version number (safe now due to row lock on config)
    let next_version: i32 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(version), 0) + 1 FROM llm_prompt_versions WHERE config_id = $1",
    )
    .bind(config_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(AppError::Database)?;

    let version: PromptVersion = sqlx::query_as(
        r#"
        INSERT INTO llm_prompt_versions 
        (config_id, version, system_prompt, model, temperature, max_tokens, parameters, variables, tools, response_format, commit_message, created_by, allowed_tools, created_by_type, created_by_key_label)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        RETURNING id, config_id, version, system_prompt, model, temperature, max_tokens, parameters, variables, tools, response_format, commit_message, created_by, created_at, allowed_tools, created_by_type, created_by_key_label
        "#,
    )
    .bind(config_id)
    .bind(next_version)
    .bind(&req.system_prompt)
    .bind(&req.model)
    .bind(req.temperature)
    .bind(req.max_tokens)
    .bind(req.parameters.clone().unwrap_or(serde_json::json!({})))
    .bind(req.variables.clone().unwrap_or(serde_json::json!([])))
    .bind(&req.tools)
    .bind(&req.response_format)
    .bind(&req.commit_message)
    .bind(user_id)
    .bind(&req.allowed_tools)
    .bind(&created_by_type)
    .bind(&created_by_key_label)
    .fetch_one(&mut *tx)
    .await
    .map_err(AppError::Database)?;

    // If this is the first version, set it as active
    if next_version == 1 {
        sqlx::query("UPDATE llm_prompt_configs SET active_version_id = $1 WHERE id = $2")
            .bind(version.id)
            .bind(config_id)
            .execute(&mut *tx)
            .await
            .map_err(AppError::Database)?;
    }

    tx.commit().await.map_err(AppError::Database)?;

    let org_id = extract_organization_id(&headers);
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::PromptVersionCreated)
        .resource("prompt_version", version.id)
        .project(&req.project_id.to_string())
        .details(serde_json::json!({
            "created": {
                "config_id": config_id,
                "version": version.version,
                "model": &version.model,
                "commit_message": &version.commit_message,
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
    if let Some(uid) = user_id {
        audit = audit.user(uid);
    }
    if let Some(oid) = org_id {
        audit = audit.organization(oid);
    }
    audit.log(&state.clickhouse).await;

    Ok(Json(version))
}

#[derive(Debug, Deserialize)]
pub struct ListVersionsParams {
    pub project_id: Uuid,
    #[serde(default = "crate::api::default_list_limit")]
    pub limit: u32,
    #[serde(default)]
    pub offset: u32,
}

/// List versions for a prompt config
async fn list_versions(
    State(state): State<Arc<FlowState>>,
    Path(config_id): Path<Uuid>,
    Query(params): Query<ListVersionsParams>,
) -> Result<Json<Vec<PromptVersion>>> {
    let limit = params.limit.min(MAX_LIMIT);

    let versions: Vec<PromptVersion> = sqlx::query_as(
        r#"
        SELECT v.id, v.config_id, v.version, v.system_prompt, v.model, v.temperature,
               v.max_tokens, v.parameters, v.variables, v.tools, v.response_format,
               v.commit_message, v.created_by, v.created_at, v.allowed_tools,
               v.created_by_type, v.created_by_key_label
        FROM llm_prompt_versions v
        JOIN llm_prompt_configs c ON v.config_id = c.id
        WHERE v.config_id = $1 AND c.project_id = $2
        ORDER BY v.version DESC
        LIMIT $3 OFFSET $4
        "#,
    )
    .bind(config_id)
    .bind(params.project_id)
    .bind(limit as i64)
    .bind(params.offset as i64)
    .fetch_all(state.db.as_ref())
    .await
    .map_err(AppError::Database)?;

    Ok(Json(versions))
}

/// Get a single version
async fn get_version(
    State(state): State<Arc<FlowState>>,
    Path((config_id, version_id)): Path<(Uuid, Uuid)>,
    Query(params): Query<GetConfigParams>,
) -> Result<Json<PromptVersion>> {
    let version: PromptVersion = sqlx::query_as(
        r#"
        SELECT v.id, v.config_id, v.version, v.system_prompt, v.model, v.temperature,
               v.max_tokens, v.parameters, v.variables, v.tools, v.response_format,
               v.commit_message, v.created_by, v.created_at, v.allowed_tools,
               v.created_by_type, v.created_by_key_label
        FROM llm_prompt_versions v
        JOIN llm_prompt_configs c ON v.config_id = c.id
        WHERE v.id = $1 AND v.config_id = $2 AND c.project_id = $3
        "#,
    )
    .bind(version_id)
    .bind(config_id)
    .bind(params.project_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(AppError::Database)?
    .ok_or_else(|| AppError::NotFound("Version not found".to_string()))?;

    Ok(Json(version))
}

// ============================================================================
// Rollouts
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct CreateRolloutRequest {
    pub project_id: Uuid,
    pub config_id: Uuid,
    pub target_version_id: Uuid,
    pub name: Option<String>,
    #[serde(default)]
    pub mode: RolloutMode,
    #[serde(default)]
    pub allocation_type: AllocationType,
    pub stages: Option<Vec<StageConfig>>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct StageConfig {
    pub weight: i32,
    pub min_duration_minutes: Option<i32>,
    pub min_requests: Option<i32>,
    pub max_error_rate_increase: Option<Decimal>,
    pub max_latency_increase_pct: Option<Decimal>,
    pub min_quality_score: Option<Decimal>,
}

fn default_stages() -> Vec<StageConfig> {
    // Conservative 7-stage progression following industry best practices
    // Gradual increases minimize blast radius: 1% → 5% → 10% → 25% → 50% → 75% → 100%
    vec![
        StageConfig {
            weight: 1,
            min_duration_minutes: Some(10),
            min_requests: Some(DEFAULT_MIN_REQUESTS),
            max_error_rate_increase: None,
            max_latency_increase_pct: None,
            min_quality_score: None,
        },
        StageConfig {
            weight: 5,
            min_duration_minutes: Some(10),
            min_requests: Some(DEFAULT_MIN_REQUESTS),
            max_error_rate_increase: None,
            max_latency_increase_pct: None,
            min_quality_score: None,
        },
        StageConfig {
            weight: 10,
            min_duration_minutes: Some(10),
            min_requests: Some(DEFAULT_MIN_REQUESTS),
            max_error_rate_increase: None,
            max_latency_increase_pct: None,
            min_quality_score: None,
        },
        StageConfig {
            weight: 25,
            min_duration_minutes: Some(15),
            min_requests: Some(DEFAULT_MIN_REQUESTS),
            max_error_rate_increase: None,
            max_latency_increase_pct: None,
            min_quality_score: None,
        },
        StageConfig {
            weight: 50,
            min_duration_minutes: Some(15),
            min_requests: Some(DEFAULT_MIN_REQUESTS),
            max_error_rate_increase: None,
            max_latency_increase_pct: None,
            min_quality_score: None,
        },
        StageConfig {
            weight: 75,
            min_duration_minutes: Some(15),
            min_requests: Some(DEFAULT_MIN_REQUESTS),
            max_error_rate_increase: None,
            max_latency_increase_pct: None,
            min_quality_score: None,
        },
        StageConfig {
            weight: COMPLETED_WEIGHT,
            min_duration_minutes: Some(0),
            min_requests: Some(0),
            max_error_rate_increase: None,
            max_latency_increase_pct: None,
            min_quality_score: None,
        },
    ]
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct Rollout {
    pub id: Uuid,
    pub project_id: Uuid,
    pub config_id: Uuid,
    pub target_version_id: Uuid,
    pub baseline_version_id: Option<Uuid>,
    pub name: Option<String>,
    pub status: String,
    pub mode: String,
    pub allocation_type: String,
    pub current_stage: i32,
    pub current_weight: i32,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub last_stage_change_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct RolloutStage {
    pub id: Uuid,
    pub rollout_id: Uuid,
    pub stage_order: i32,
    pub weight: i32,
    pub min_duration_minutes: Option<i32>,
    pub min_requests: Option<i32>,
    pub max_error_rate_increase: Option<Decimal>,
    pub max_latency_increase_pct: Option<Decimal>,
    pub min_quality_score: Option<Decimal>,
    pub status: String,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

/// Insert default canary stages for a new rollout (prompt compiler commit).
pub(crate) async fn insert_default_rollout_stages_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    rollout_id: Uuid,
) -> std::result::Result<(), sqlx::Error> {
    insert_rollout_stages_tx(tx, rollout_id, &default_stages()).await?;
    Ok(())
}

async fn insert_rollout_stages_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    rollout_id: Uuid,
    stages_config: &[StageConfig],
) -> std::result::Result<Vec<RolloutStage>, sqlx::Error> {
    let mut stages = Vec::new();
    for (order, stage_cfg) in stages_config.iter().enumerate() {
        let stage: RolloutStage = sqlx::query_as(
            r#"
            INSERT INTO llm_rollout_stages 
            (rollout_id, stage_order, weight, min_duration_minutes, min_requests, 
             max_error_rate_increase, max_latency_increase_pct, min_quality_score)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING id, rollout_id, stage_order, weight, min_duration_minutes, min_requests,
                      max_error_rate_increase, max_latency_increase_pct, min_quality_score, status,
                      started_at, completed_at
            "#,
        )
        .bind(rollout_id)
        .bind(order as i32)
        .bind(stage_cfg.weight)
        .bind(stage_cfg.min_duration_minutes.unwrap_or(10))
        .bind(stage_cfg.min_requests.unwrap_or(DEFAULT_MIN_REQUESTS))
        .bind(stage_cfg.max_error_rate_increase)
        .bind(stage_cfg.max_latency_increase_pct)
        .bind(stage_cfg.min_quality_score)
        .fetch_one(&mut **tx)
        .await?;

        stages.push(stage);
    }
    Ok(stages)
}

#[derive(Debug, Serialize)]
pub struct RolloutWithStages {
    #[serde(flatten)]
    pub rollout: Rollout,
    pub stages: Vec<RolloutStage>,
    pub config_name: String,
    pub target_version: i32,
    pub baseline_version: Option<i32>,
    /// Default maximum error rate increase before auto-rollback (as percentage, e.g., 0.05 = 5%).
    /// Applied when stage doesn't specify `max_error_rate_increase`.
    pub default_max_error_rate_increase: f64,
    /// Default maximum latency increase percentage before auto-rollback.
    /// Applied when stage doesn't specify `max_latency_increase_pct`.
    pub default_max_latency_increase_pct: f64,
}

/// Validate custom rollout stage configs.
///
/// Returns `Ok(())` if stages are valid, or an error message describing
/// the first constraint violation.
fn validate_rollout_stages(stages: &[StageConfig]) -> Result<()> {
    if stages.is_empty() {
        return Err(AppError::Validation(
            "At least one stage is required".to_string(),
        ));
    }

    if stages.iter().any(|s| s.weight <= 0) {
        return Err(AppError::Validation(
            "Stage weights must be positive".to_string(),
        ));
    }

    for i in 1..stages.len() {
        if stages[i].weight <= stages[i - 1].weight {
            return Err(AppError::Validation(
                "Stage weights must be in ascending order".to_string(),
            ));
        }
    }

    if stages.last().map(|s| s.weight) != Some(COMPLETED_WEIGHT) {
        return Err(AppError::Validation(
            "Final stage must have weight 100".to_string(),
        ));
    }

    Ok(())
}

/// Create a new rollout
async fn create_rollout(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    Json(req): Json<CreateRolloutRequest>,
) -> Result<Json<RolloutWithStages>> {
    let user_id = extract_user_id(&headers).ok();

    if let Some(ref stages) = req.stages {
        validate_rollout_stages(stages)?;
    }

    // Start transaction early to prevent race conditions
    let mut tx = state.db.begin().await.map_err(AppError::Database)?;

    // Get config and verify it exists, locking the row to prevent concurrent rollout creation
    // This lock serializes rollout creation for the same config
    #[derive(sqlx::FromRow)]
    struct ConfigInfo {
        name: String,
        active_version_id: Option<Uuid>,
    }

    let config: ConfigInfo = sqlx::query_as(
        "SELECT name, active_version_id FROM llm_prompt_configs WHERE id = $1 AND project_id = $2 FOR UPDATE"
    )
    .bind(req.config_id)
    .bind(req.project_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(AppError::Database)?
    .ok_or_else(|| AppError::NotFound("Prompt config not found".to_string()))?;

    // Prevent deploying the version that is already active
    if Some(req.target_version_id) == config.active_version_id {
        return Err(AppError::Validation(
            "Target version is already the active version".to_string(),
        ));
    }

    // Now check for running rollout - this is safe because we hold the config lock
    let has_running: bool = sqlx::query_scalar(&format!(
        "SELECT EXISTS(SELECT 1 FROM llm_rollouts WHERE config_id = $1 AND status = '{}')",
        RolloutStatus::Running.as_str()
    ))
    .bind(req.config_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(AppError::Database)?;

    if has_running {
        return Err(AppError::Validation(
            "Config already has a running rollout. Stop it first.".to_string(),
        ));
    }

    // Verify target version exists
    let target_version: i32 = sqlx::query_scalar(
        "SELECT version FROM llm_prompt_versions WHERE id = $1 AND config_id = $2",
    )
    .bind(req.target_version_id)
    .bind(req.config_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(AppError::Database)?
    .ok_or_else(|| AppError::NotFound("Target version not found".to_string()))?;

    // Get baseline version info
    let baseline_version: Option<i32> = if let Some(baseline_id) = config.active_version_id {
        sqlx::query_scalar("SELECT version FROM llm_prompt_versions WHERE id = $1")
            .bind(baseline_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(AppError::Database)?
    } else {
        None
    };

    // Create rollout
    let rollout: Rollout = sqlx::query_as(
        r#"
        INSERT INTO llm_rollouts 
        (project_id, config_id, target_version_id, baseline_version_id, name, mode, allocation_type)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id, project_id, config_id, target_version_id, baseline_version_id, name, status, 
                  mode, allocation_type, current_stage, current_weight, created_at, started_at, 
                  completed_at, last_stage_change_at
        "#,
    )
    .bind(req.project_id)
    .bind(req.config_id)
    .bind(req.target_version_id)
    .bind(config.active_version_id)
    .bind(&req.name)
    .bind(req.mode.as_str())
    .bind(req.allocation_type.as_str())
    .fetch_one(&mut *tx)
    .await
    .map_err(AppError::Database)?;

    // Create stages
    let stages_config = req.stages.clone().unwrap_or_else(default_stages);
    let stages = insert_rollout_stages_tx(&mut tx, rollout.id, &stages_config)
        .await
        .map_err(AppError::Database)?;

    tx.commit().await.map_err(AppError::Database)?;

    let org_id = extract_organization_id(&headers);
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::RolloutCreated)
        .resource("rollout", rollout.id)
        .project(&req.project_id.to_string())
        .details(serde_json::json!({
            "created": {
                "config_name": &config.name,
                "config_id": req.config_id,
                "target_version": target_version,
                "baseline_version": baseline_version,
                "mode": req.mode.as_str(),
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
    if let Some(uid) = user_id {
        audit = audit.user(uid);
    }
    if let Some(oid) = org_id {
        audit = audit.organization(oid);
    }
    audit.log(&state.clickhouse).await;

    Ok(Json(RolloutWithStages {
        rollout,
        stages,
        config_name: config.name,
        target_version,
        baseline_version,
        default_max_error_rate_increase: DEFAULT_MAX_ERROR_RATE_INCREASE,
        default_max_latency_increase_pct: DEFAULT_MAX_LATENCY_INCREASE_PCT,
    }))
}

#[derive(Debug, Deserialize)]
pub struct ListRolloutsParams {
    pub project_id: Uuid,
    pub config_id: Option<Uuid>,
    pub status: Option<String>,
    #[serde(default = "crate::api::default_list_limit")]
    pub limit: u32,
    #[serde(default)]
    pub offset: u32,
}

/// List rollouts
async fn list_rollouts(
    State(state): State<Arc<FlowState>>,
    Query(params): Query<ListRolloutsParams>,
) -> Result<Json<Vec<RolloutWithStages>>> {
    let limit = params.limit.min(MAX_LIMIT);

    // Build query with optional filters
    let mut query = String::from(
        r#"
        SELECT r.id, r.project_id, r.config_id, r.target_version_id, r.baseline_version_id, 
               r.name, r.status, r.mode, r.allocation_type, r.current_stage, r.current_weight, 
               r.created_at, r.started_at, r.completed_at, r.last_stage_change_at,
               c.name as config_name,
               tv.version as target_version,
               bv.version as baseline_version
        FROM llm_rollouts r
        JOIN llm_prompt_configs c ON r.config_id = c.id
        JOIN llm_prompt_versions tv ON r.target_version_id = tv.id
        LEFT JOIN llm_prompt_versions bv ON r.baseline_version_id = bv.id
        WHERE r.project_id = $1
        "#,
    );

    if params.config_id.is_some() {
        query.push_str(" AND r.config_id = $4");
    }
    if params.status.is_some() {
        query.push_str(if params.config_id.is_some() {
            " AND r.status = $5"
        } else {
            " AND r.status = $4"
        });
    }
    query.push_str(" ORDER BY r.created_at DESC LIMIT $2 OFFSET $3");

    #[derive(sqlx::FromRow)]
    struct RolloutRow {
        id: Uuid,
        project_id: Uuid,
        config_id: Uuid,
        target_version_id: Uuid,
        baseline_version_id: Option<Uuid>,
        name: Option<String>,
        status: String,
        mode: String,
        allocation_type: String,
        current_stage: i32,
        current_weight: i32,
        created_at: DateTime<Utc>,
        started_at: Option<DateTime<Utc>>,
        completed_at: Option<DateTime<Utc>>,
        last_stage_change_at: Option<DateTime<Utc>>,
        config_name: String,
        target_version: i32,
        baseline_version: Option<i32>,
    }

    let mut query_builder = sqlx::query_as::<_, RolloutRow>(&query)
        .bind(params.project_id)
        .bind(limit as i64)
        .bind(params.offset as i64);

    if let Some(config_id) = params.config_id {
        query_builder = query_builder.bind(config_id);
    }
    if let Some(ref status) = params.status {
        query_builder = query_builder.bind(status);
    }

    let rollouts: Vec<RolloutRow> = query_builder
        .fetch_all(state.db.as_ref())
        .await
        .map_err(AppError::Database)?;

    // Batch fetch all stages in a single query to avoid N+1 problem
    let rollout_ids: Vec<Uuid> = rollouts.iter().map(|r| r.id).collect();

    let all_stages: Vec<RolloutStage> = if !rollout_ids.is_empty() {
        sqlx::query_as(
            r#"
            SELECT id, rollout_id, stage_order, weight, min_duration_minutes, min_requests,
                   max_error_rate_increase, max_latency_increase_pct, min_quality_score, status,
                   started_at, completed_at
            FROM llm_rollout_stages
            WHERE rollout_id = ANY($1)
            ORDER BY rollout_id, stage_order
            "#,
        )
        .bind(&rollout_ids)
        .fetch_all(state.db.as_ref())
        .await
        .map_err(AppError::Database)?
    } else {
        Vec::new()
    };

    // Group stages by rollout_id
    let mut stages_by_rollout: HashMap<Uuid, Vec<RolloutStage>> = HashMap::new();
    for stage in all_stages {
        stages_by_rollout
            .entry(stage.rollout_id)
            .or_default()
            .push(stage);
    }

    // Build result with pre-fetched stages
    let result: Vec<RolloutWithStages> = rollouts
        .into_iter()
        .map(|row| {
            let stages = stages_by_rollout.remove(&row.id).unwrap_or_default();
            RolloutWithStages {
                rollout: Rollout {
                    id: row.id,
                    project_id: row.project_id,
                    config_id: row.config_id,
                    target_version_id: row.target_version_id,
                    baseline_version_id: row.baseline_version_id,
                    name: row.name,
                    status: row.status,
                    mode: row.mode,
                    allocation_type: row.allocation_type,
                    current_stage: row.current_stage,
                    current_weight: row.current_weight,
                    created_at: row.created_at,
                    started_at: row.started_at,
                    completed_at: row.completed_at,
                    last_stage_change_at: row.last_stage_change_at,
                },
                stages,
                config_name: row.config_name,
                target_version: row.target_version,
                baseline_version: row.baseline_version,
                default_max_error_rate_increase: DEFAULT_MAX_ERROR_RATE_INCREASE,
                default_max_latency_increase_pct: DEFAULT_MAX_LATENCY_INCREASE_PCT,
            }
        })
        .collect();

    Ok(Json(result))
}

/// Get a single rollout
async fn get_rollout(
    State(state): State<Arc<FlowState>>,
    Path(rollout_id): Path<Uuid>,
    Query(params): Query<GetConfigParams>,
) -> Result<Json<RolloutWithStages>> {
    #[derive(sqlx::FromRow)]
    struct RolloutRow {
        id: Uuid,
        project_id: Uuid,
        config_id: Uuid,
        target_version_id: Uuid,
        baseline_version_id: Option<Uuid>,
        name: Option<String>,
        status: String,
        mode: String,
        allocation_type: String,
        current_stage: i32,
        current_weight: i32,
        created_at: DateTime<Utc>,
        started_at: Option<DateTime<Utc>>,
        completed_at: Option<DateTime<Utc>>,
        last_stage_change_at: Option<DateTime<Utc>>,
        config_name: String,
        target_version: i32,
        baseline_version: Option<i32>,
    }

    let row: RolloutRow = sqlx::query_as(
        r#"
        SELECT r.id, r.project_id, r.config_id, r.target_version_id, r.baseline_version_id,
               r.name, r.status, r.mode, r.allocation_type, r.current_stage, r.current_weight,
               r.created_at, r.started_at, r.completed_at, r.last_stage_change_at,
               c.name as config_name,
               tv.version as target_version,
               bv.version as baseline_version
        FROM llm_rollouts r
        JOIN llm_prompt_configs c ON r.config_id = c.id
        JOIN llm_prompt_versions tv ON r.target_version_id = tv.id
        LEFT JOIN llm_prompt_versions bv ON r.baseline_version_id = bv.id
        WHERE r.id = $1 AND r.project_id = $2
        "#,
    )
    .bind(rollout_id)
    .bind(params.project_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(AppError::Database)?
    .ok_or_else(|| AppError::NotFound("Rollout not found".to_string()))?;

    let stages: Vec<RolloutStage> = sqlx::query_as(
        r#"
        SELECT id, rollout_id, stage_order, weight, min_duration_minutes, min_requests,
               max_error_rate_increase, max_latency_increase_pct, min_quality_score, status,
               started_at, completed_at
        FROM llm_rollout_stages
        WHERE rollout_id = $1
        ORDER BY stage_order
        "#,
    )
    .bind(rollout_id)
    .fetch_all(state.db.as_ref())
    .await
    .map_err(AppError::Database)?;

    Ok(Json(RolloutWithStages {
        rollout: Rollout {
            id: row.id,
            project_id: row.project_id,
            config_id: row.config_id,
            target_version_id: row.target_version_id,
            baseline_version_id: row.baseline_version_id,
            name: row.name,
            status: row.status,
            mode: row.mode,
            allocation_type: row.allocation_type,
            current_stage: row.current_stage,
            current_weight: row.current_weight,
            created_at: row.created_at,
            started_at: row.started_at,
            completed_at: row.completed_at,
            last_stage_change_at: row.last_stage_change_at,
        },
        stages,
        config_name: row.config_name,
        target_version: row.target_version,
        baseline_version: row.baseline_version,
        default_max_error_rate_increase: DEFAULT_MAX_ERROR_RATE_INCREASE,
        default_max_latency_increase_pct: DEFAULT_MAX_LATENCY_INCREASE_PCT,
    }))
}

#[derive(Debug, Deserialize)]
pub struct RolloutActionRequest {
    pub project_id: Uuid,
}

/// Start a rollout
async fn start_rollout(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    Path(rollout_id): Path<Uuid>,
    Json(req): Json<RolloutActionRequest>,
) -> Result<Json<Rollout>> {
    let user_id = extract_user_id(&headers).ok();

    // Get the first stage weight
    let first_stage_weight: i32 = sqlx::query_scalar(
        "SELECT weight FROM llm_rollout_stages WHERE rollout_id = $1 AND stage_order = 0",
    )
    .bind(rollout_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(AppError::Database)?
    .unwrap_or(1);

    let mut tx = state.db.begin().await.map_err(AppError::Database)?;

    // Update rollout status
    let rollout: Rollout = sqlx::query_as(
        &format!(
            r#"
            UPDATE llm_rollouts
            SET status = '{}', 
                started_at = NOW(), 
                last_stage_change_at = NOW(),
                current_stage = 0,
                current_weight = $3
            WHERE id = $1 AND project_id = $2 AND status = '{}'
            RETURNING id, project_id, config_id, target_version_id, baseline_version_id, name, status,
                      mode, allocation_type, current_stage, current_weight, created_at, started_at,
                      completed_at, last_stage_change_at
            "#,
            RolloutStatus::Running.as_str(),
            RolloutStatus::Pending.as_str(),
        )
    )
    .bind(rollout_id)
    .bind(req.project_id)
    .bind(first_stage_weight)
    .fetch_optional(&mut *tx)
    .await
    .map_err(AppError::Database)?
    .ok_or_else(|| AppError::Validation("Rollout not found or not in pending state".to_string()))?;

    // Mark first stage as active
    sqlx::query(
        &format!(
            "UPDATE llm_rollout_stages SET status = '{}', started_at = NOW() WHERE rollout_id = $1 AND stage_order = 0",
            RolloutStageStatus::Active.as_str()
        )
    )
    .bind(rollout_id)
    .execute(&mut *tx)
    .await
    .map_err(AppError::Database)?;

    tx.commit().await.map_err(AppError::Database)?;

    // Invalidate cache so gateway picks up the new rollout
    invalidate_rollout_cache(state.redis.as_ref(), rollout.project_id, rollout.config_id).await;

    let org_id = extract_organization_id(&headers);
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::RolloutStarted)
        .resource("rollout", rollout_id)
        .project(&req.project_id.to_string())
        .details(serde_json::json!({
            "config_id": rollout.config_id,
            "initial_weight": first_stage_weight,
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
    if let Some(uid) = user_id {
        audit = audit.user(uid);
    }
    if let Some(oid) = org_id {
        audit = audit.organization(oid);
    }
    audit.log(&state.clickhouse).await;

    Ok(Json(rollout))
}

/// Pause a rollout
async fn pause_rollout(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    Path(rollout_id): Path<Uuid>,
    Json(req): Json<RolloutActionRequest>,
) -> Result<Json<Rollout>> {
    let user_id = extract_user_id(&headers).ok();

    let rollout: Rollout = sqlx::query_as(
        &format!(
            r#"
            UPDATE llm_rollouts
            SET status = '{}'
            WHERE id = $1 AND project_id = $2 AND status = '{}'
            RETURNING id, project_id, config_id, target_version_id, baseline_version_id, name, status,
                      mode, allocation_type, current_stage, current_weight, created_at, started_at,
                      completed_at, last_stage_change_at
            "#,
            RolloutStatus::Paused.as_str(),
            RolloutStatus::Running.as_str(),
        )
    )
    .bind(rollout_id)
    .bind(req.project_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(AppError::Database)?
    .ok_or_else(|| AppError::Validation("Rollout not found or not running".to_string()))?;

    // Invalidate cache so gateway stops routing to this rollout
    invalidate_rollout_cache(state.redis.as_ref(), rollout.project_id, rollout.config_id).await;

    let org_id = extract_organization_id(&headers);
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::RolloutPaused)
        .resource("rollout", rollout_id)
        .project(&req.project_id.to_string())
        .details(serde_json::json!({
            "config_id": rollout.config_id,
            "paused_at_stage": rollout.current_stage,
            "paused_at_weight": rollout.current_weight,
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
    if let Some(uid) = user_id {
        audit = audit.user(uid);
    }
    if let Some(oid) = org_id {
        audit = audit.organization(oid);
    }
    audit.log(&state.clickhouse).await;

    Ok(Json(rollout))
}

/// Manually promote to next stage
async fn promote_rollout(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    Path(rollout_id): Path<Uuid>,
    Json(req): Json<RolloutActionRequest>,
) -> Result<Json<Rollout>> {
    let user_id = extract_user_id(&headers).ok();

    // Get current rollout state
    let rollout: Rollout = sqlx::query_as(&format!(
        r#"
            SELECT id, project_id, config_id, target_version_id, baseline_version_id, name, status,
                   mode, allocation_type, current_stage, current_weight, created_at, started_at,
                   completed_at, last_stage_change_at
            FROM llm_rollouts
            WHERE id = $1 AND project_id = $2 AND status = '{}'
            "#,
        RolloutStatus::Running.as_str()
    ))
    .bind(rollout_id)
    .bind(req.project_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(AppError::Database)?
    .ok_or_else(|| AppError::Validation("Rollout not found or not running".to_string()))?;

    // Get next stage
    #[derive(sqlx::FromRow)]
    struct NextStage {
        stage_order: i32,
        weight: i32,
    }

    let next_stage: Option<NextStage> = sqlx::query_as(
        "SELECT stage_order, weight FROM llm_rollout_stages WHERE rollout_id = $1 AND stage_order = $2"
    )
    .bind(rollout_id)
    .bind(rollout.current_stage + 1)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(AppError::Database)?;

    let mut tx = state.db.begin().await.map_err(AppError::Database)?;

    // Mark current stage as passed
    sqlx::query(
        &format!(
            "UPDATE llm_rollout_stages SET status = '{}', completed_at = NOW() WHERE rollout_id = $1 AND stage_order = $2",
            RolloutStageStatus::Passed.as_str()
        )
    )
    .bind(rollout_id)
    .bind(rollout.current_stage)
    .execute(&mut *tx)
    .await
    .map_err(AppError::Database)?;

    let updated_rollout: Rollout = if let Some(next) = next_stage {
        // Promote to next stage
        sqlx::query(
            &format!(
                "UPDATE llm_rollout_stages SET status = '{}', started_at = NOW() WHERE rollout_id = $1 AND stage_order = $2",
                RolloutStageStatus::Active.as_str()
            )
        )
        .bind(rollout_id)
        .bind(next.stage_order)
        .execute(&mut *tx)
        .await
        .map_err(AppError::Database)?;

        sqlx::query_as(
            r#"
            UPDATE llm_rollouts
            SET current_stage = $3, current_weight = $4, last_stage_change_at = NOW()
            WHERE id = $1 AND project_id = $2
            RETURNING id, project_id, config_id, target_version_id, baseline_version_id, name, status,
                      mode, allocation_type, current_stage, current_weight, created_at, started_at,
                      completed_at, last_stage_change_at
            "#
        )
        .bind(rollout_id)
        .bind(req.project_id)
        .bind(next.stage_order)
        .bind(next.weight)
        .fetch_one(&mut *tx)
        .await
        .map_err(AppError::Database)?
    } else {
        // No more stages - complete the rollout
        // Update config's active version
        sqlx::query("UPDATE llm_prompt_configs SET active_version_id = $1 WHERE id = $2")
            .bind(rollout.target_version_id)
            .bind(rollout.config_id)
            .execute(&mut *tx)
            .await
            .map_err(AppError::Database)?;

        sqlx::query_as(
            &format!(
                r#"
                UPDATE llm_rollouts
                SET status = '{}', completed_at = NOW(), current_weight = {}
                WHERE id = $1 AND project_id = $2
                RETURNING id, project_id, config_id, target_version_id, baseline_version_id, name, status,
                          mode, allocation_type, current_stage, current_weight, created_at, started_at,
                          completed_at, last_stage_change_at
                "#,
                RolloutStatus::Completed.as_str(),
                COMPLETED_WEIGHT,
            )
        )
        .bind(rollout_id)
        .bind(req.project_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(AppError::Database)?
    };

    tx.commit().await.map_err(AppError::Database)?;

    // Invalidate cache so gateway picks up the weight change or completion
    invalidate_rollout_cache(
        state.redis.as_ref(),
        updated_rollout.project_id,
        updated_rollout.config_id,
    )
    .await;

    let org_id = extract_organization_id(&headers);
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::RolloutPromoted)
        .resource("rollout", rollout_id)
        .project(&req.project_id.to_string())
        .details(serde_json::json!({
            "config_id": updated_rollout.config_id,
            "before": { "stage": rollout.current_stage, "weight": rollout.current_weight },
            "after": { "stage": updated_rollout.current_stage, "weight": updated_rollout.current_weight },
        }))
        .origin(&audit_origin.origin_type, &audit_origin.origin_ref, &audit_origin.origin_reason)
        .caller(&audit_caller.caller_type, &audit_caller.key_label, &audit_caller.key_prefix)
        .success();
    if let Some(uid) = user_id {
        audit = audit.user(uid);
    }
    if let Some(oid) = org_id {
        audit = audit.organization(oid);
    }
    audit.log(&state.clickhouse).await;

    Ok(Json(updated_rollout))
}

/// Rollback a rollout
async fn rollback_rollout(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    Path(rollout_id): Path<Uuid>,
    Json(req): Json<RolloutActionRequest>,
) -> Result<Json<Rollout>> {
    let user_id = extract_user_id(&headers).ok();

    let mut tx = state.db.begin().await.map_err(AppError::Database)?;

    // Mark current stage as failed
    sqlx::query(
        &format!(
            "UPDATE llm_rollout_stages SET status = '{}', completed_at = NOW() WHERE rollout_id = $1 AND status = '{}'",
            RolloutStageStatus::Failed.as_str(),
            RolloutStageStatus::Active.as_str(),
        )
    )
    .bind(rollout_id)
    .execute(&mut *tx)
    .await
    .map_err(AppError::Database)?;

    // Update rollout status
    let rollout: Rollout = sqlx::query_as(
        &format!(
            r#"
            UPDATE llm_rollouts
            SET status = '{}', completed_at = NOW(), current_weight = 0
            WHERE id = $1 AND project_id = $2 AND status IN ('{}', '{}')
            RETURNING id, project_id, config_id, target_version_id, baseline_version_id, name, status,
                      mode, allocation_type, current_stage, current_weight, created_at, started_at,
                      completed_at, last_stage_change_at
            "#,
            RolloutStatus::RolledBack.as_str(),
            RolloutStatus::Running.as_str(),
            RolloutStatus::Paused.as_str(),
        )
    )
    .bind(rollout_id)
    .bind(req.project_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(AppError::Database)?
    .ok_or_else(|| AppError::Validation("Rollout not found or not in running/paused state".to_string()))?;

    tx.commit().await.map_err(AppError::Database)?;

    // Invalidate cache so gateway stops routing to this rollout
    invalidate_rollout_cache(state.redis.as_ref(), rollout.project_id, rollout.config_id).await;

    let org_id = extract_organization_id(&headers);
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::RolloutRolledBack)
        .resource("rollout", rollout_id)
        .project(&req.project_id.to_string())
        .details(serde_json::json!({
            "config_id": rollout.config_id,
            "rolled_back_at_stage": rollout.current_stage,
            "rolled_back_at_weight": rollout.current_weight,
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
    if let Some(uid) = user_id {
        audit = audit.user(uid);
    }
    if let Some(oid) = org_id {
        audit = audit.organization(oid);
    }
    audit.log(&state.clickhouse).await;

    Ok(Json(rollout))
}

/// Complete a rollout immediately (skip to 100%)
async fn complete_rollout(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    Path(rollout_id): Path<Uuid>,
    Json(req): Json<RolloutActionRequest>,
) -> Result<Json<Rollout>> {
    let user_id = extract_user_id(&headers).ok();

    let mut tx = state.db.begin().await.map_err(AppError::Database)?;

    // Get rollout to update config
    let rollout: Rollout = sqlx::query_as(&format!(
        r#"
            SELECT id, project_id, config_id, target_version_id, baseline_version_id, name, status,
                   mode, allocation_type, current_stage, current_weight, created_at, started_at,
                   completed_at, last_stage_change_at
            FROM llm_rollouts
            WHERE id = $1 AND project_id = $2 AND status IN ('{}', '{}')
            "#,
        RolloutStatus::Running.as_str(),
        RolloutStatus::Paused.as_str(),
    ))
    .bind(rollout_id)
    .bind(req.project_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(AppError::Database)?
    .ok_or_else(|| {
        AppError::Validation("Rollout not found or not in running/paused state".to_string())
    })?;

    // Mark all remaining stages as passed
    sqlx::query(
        &format!(
            "UPDATE llm_rollout_stages SET status = '{}', completed_at = NOW() WHERE rollout_id = $1 AND status IN ('{}', '{}')",
            RolloutStageStatus::Passed.as_str(),
            RolloutStageStatus::Pending.as_str(),
            RolloutStageStatus::Active.as_str(),
        )
    )
    .bind(rollout_id)
    .execute(&mut *tx)
    .await
    .map_err(AppError::Database)?;

    // Update config's active version
    sqlx::query("UPDATE llm_prompt_configs SET active_version_id = $1 WHERE id = $2")
        .bind(rollout.target_version_id)
        .bind(rollout.config_id)
        .execute(&mut *tx)
        .await
        .map_err(AppError::Database)?;

    // Complete rollout
    let updated_rollout: Rollout = sqlx::query_as(
        &format!(
            r#"
            UPDATE llm_rollouts
            SET status = '{}', completed_at = NOW(), current_weight = {}
            WHERE id = $1 AND project_id = $2
            RETURNING id, project_id, config_id, target_version_id, baseline_version_id, name, status,
                      mode, allocation_type, current_stage, current_weight, created_at, started_at,
                      completed_at, last_stage_change_at
            "#,
            RolloutStatus::Completed.as_str(),
            COMPLETED_WEIGHT,
        )
    )
    .bind(rollout_id)
    .bind(req.project_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(AppError::Database)?;

    tx.commit().await.map_err(AppError::Database)?;

    // Invalidate cache so gateway stops routing to this rollout
    invalidate_rollout_cache(
        state.redis.as_ref(),
        updated_rollout.project_id,
        updated_rollout.config_id,
    )
    .await;

    let org_id = extract_organization_id(&headers);
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::RolloutCompleted)
        .resource("rollout", rollout_id)
        .project(&req.project_id.to_string())
        .details(serde_json::json!({
            "config_id": updated_rollout.config_id,
            "completed_from_stage": rollout.current_stage,
            "target_version_id": rollout.target_version_id,
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
    if let Some(uid) = user_id {
        audit = audit.user(uid);
    }
    if let Some(oid) = org_id {
        audit = audit.organization(oid);
    }
    audit.log(&state.clickhouse).await;

    Ok(Json(updated_rollout))
}

// ============================================================================
// Rollout Metrics
// ============================================================================

#[derive(Debug, Serialize)]
pub struct RolloutMetrics {
    pub rollout_id: Uuid,
    pub stage_order: i32,
    pub target: VariantMetrics,
    pub baseline: VariantMetrics,
    pub comparison: MetricsComparison,
    pub quality_scores: Option<QualityScoreComparison>,
    pub recent_summaries: Vec<JudgeSummary>,
}

// VariantMetrics is imported from crate::llm::types

#[derive(Debug, Serialize)]
pub struct MetricsComparison {
    pub error_rate_diff: f64,
    pub latency_diff_pct: f64,
    pub cost_diff_pct: f64,
    pub status: ComparisonStatus,
}

#[derive(Debug, Serialize)]
pub struct QualityScoreComparison {
    pub target: DimensionScores,
    pub baseline: DimensionScores,
}

#[derive(Debug, Default, Serialize)]
pub struct DimensionScores {
    pub relevance: Option<f64>,
    pub coherence: Option<f64>,
    pub helpfulness: Option<f64>,
    pub average: Option<f64>,
    pub sample_count: u64,
}

#[derive(Debug, Serialize)]
pub struct JudgeSummary {
    pub request_id: String,
    pub variant: String,
    pub summary: String,
    pub score: f64,
    pub created_at: DateTime<Utc>,
}

/// Get rollout metrics comparison
async fn get_rollout_metrics(
    State(state): State<Arc<FlowState>>,
    Path(rollout_id): Path<Uuid>,
    Query(params): Query<GetConfigParams>,
) -> Result<Json<RolloutMetrics>> {
    // Get rollout info
    let rollout: Rollout = sqlx::query_as(
        r#"
        SELECT id, project_id, config_id, target_version_id, baseline_version_id, name, status,
               mode, allocation_type, current_stage, current_weight, created_at, started_at,
               completed_at, last_stage_change_at
        FROM llm_rollouts
        WHERE id = $1 AND project_id = $2
        "#,
    )
    .bind(rollout_id)
    .bind(params.project_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(AppError::Database)?
    .ok_or_else(|| AppError::NotFound("Rollout not found".to_string()))?;

    // Get metrics from ClickHouse
    let query = format!(
        r#"
        SELECT
            rollout_variant,
            count() as request_count,
            countIf(status_code = 'error') as error_count,
            if(count() > 0, countIf(status_code = 'error') / count(), 0) as error_rate,
            avg(duration_ms) as avg_latency_ms,
            quantile(0.95)(duration_ms) as p95_latency_ms,
            avg(cost_usd) as avg_cost_usd
        FROM reiver.llm_requests
        WHERE rollout_id = '{}'
          AND timestamp >= now() - INTERVAL 1 HOUR
        GROUP BY rollout_variant
        "#,
        rollout_id
    );

    #[derive(Debug, clickhouse::Row, serde::Deserialize)]
    struct MetricRow {
        rollout_variant: String,
        request_count: u64,
        error_count: u64,
        error_rate: f64,
        avg_latency_ms: f64,
        p95_latency_ms: f64,
        avg_cost_usd: f64,
    }

    let rows: Vec<MetricRow> = match state.clickhouse.query(&query).fetch_all().await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(
                rollout_id = %rollout_id,
                error = %e,
                "Failed to fetch rollout metrics from ClickHouse"
            );
            Vec::new()
        }
    };

    let mut target = VariantMetrics::default();
    let mut baseline = VariantMetrics::default();

    for row in rows {
        let metrics = VariantMetrics {
            request_count: row.request_count,
            error_count: row.error_count,
            error_rate: row.error_rate,
            avg_latency_ms: row.avg_latency_ms,
            p95_latency_ms: row.p95_latency_ms,
            avg_cost_usd: Decimal::try_from(row.avg_cost_usd).unwrap_or(Decimal::ZERO),
            avg_quality_score: None,
        };

        match RolloutVariant::from_str(&row.rollout_variant) {
            Some(RolloutVariant::Target) => target = metrics,
            Some(RolloutVariant::Baseline) => baseline = metrics,
            None => {}
        }
    }

    // Fetch per-dimension quality scores and summaries from PostgreSQL.
    // We get request_ids per variant from ClickHouse, then query scores.
    let quality_scores = fetch_quality_score_comparison(
        &state, rollout_id, params.project_id,
    ).await;
    let recent_summaries = fetch_recent_summaries(
        &state, rollout_id, params.project_id,
    ).await;

    // Calculate comparison
    let error_rate_diff = target.error_rate - baseline.error_rate;
    let latency_diff_pct =
        crate::utils::percentage_change(target.avg_latency_ms, baseline.avg_latency_ms);
    let cost_diff_pct = {
        let target_f = target.avg_cost_usd.to_f64().unwrap_or(0.0);
        let baseline_f = baseline.avg_cost_usd.to_f64().unwrap_or(0.0);
        crate::utils::percentage_change(target_f, baseline_f)
    };

    // NOTE: Auto-promote/rollback based on thresholds is disabled. The user
    // should manually decide whether to promote or rollback. In the future we
    // will implement user-defined conditions (similar to session profiles) so
    // users can create custom promotion/rollback logic.
    let status = if target.request_count < 10 || baseline.request_count < 10 {
        ComparisonStatus::Inconclusive
    } else {
        ComparisonStatus::Passing
    };

    Ok(Json(RolloutMetrics {
        rollout_id,
        stage_order: rollout.current_stage,
        target,
        baseline,
        comparison: MetricsComparison {
            error_rate_diff,
            latency_diff_pct,
            cost_diff_pct,
            status,
        },
        quality_scores,
        recent_summaries,
    }))
}

/// Fetch per-dimension quality scores for each rollout variant.
async fn fetch_quality_score_comparison(
    state: &FlowState,
    rollout_id: Uuid,
    project_id: Uuid,
) -> Option<QualityScoreComparison> {
    // Get request_ids grouped by variant from ClickHouse
    let request_ids_query = format!(
        r#"
        SELECT rollout_variant, groupArray(request_id) as request_ids
        FROM reiver.llm_requests
        WHERE rollout_id = '{}'
          AND rollout_variant != ''
        GROUP BY rollout_variant
        "#,
        rollout_id
    );

    #[derive(Debug, clickhouse::Row, serde::Deserialize)]
    struct RequestIdRow {
        rollout_variant: String,
        request_ids: Vec<String>,
    }

    let variant_rows: Vec<RequestIdRow> = state
        .clickhouse
        .query(&request_ids_query)
        .fetch_all()
        .await
        .ok()?;

    if variant_rows.is_empty() {
        return None;
    }

    let mut target_scores = DimensionScores::default();
    let mut baseline_scores = DimensionScores::default();
    let mut has_any = false;

    for row in &variant_rows {
        if row.request_ids.is_empty() {
            continue;
        }

        #[derive(Debug, sqlx::FromRow)]
        struct ScoreRow {
            score_name: String,
            avg_value: Option<Decimal>,
            cnt: i64,
        }

        let scores: Vec<ScoreRow> = sqlx::query_as(
            r#"
            SELECT score_name, AVG(score_value) as avg_value, COUNT(*)::bigint as cnt
            FROM llm_evaluation_scores
            WHERE project_id = $1
              AND request_id = ANY($2)
              AND score_type = 'number'
              AND evaluator_type = 'llm_judge'
            GROUP BY score_name
            "#,
        )
        .bind(project_id)
        .bind(&row.request_ids)
        .fetch_all(state.db.as_ref())
        .await
        .unwrap_or_default();

        if scores.is_empty() {
            continue;
        }
        has_any = true;

        let dim = match RolloutVariant::from_str(&row.rollout_variant) {
            Some(RolloutVariant::Target) => &mut target_scores,
            Some(RolloutVariant::Baseline) => &mut baseline_scores,
            None => continue,
        };

        for s in &scores {
            let val = s.avg_value.and_then(|d| d.to_f64());
            match s.score_name.as_str() {
                "relevance" => dim.relevance = val,
                "coherence" => dim.coherence = val,
                "helpfulness" => dim.helpfulness = val,
                "average" => dim.average = val,
                _ => {}
            }
            dim.sample_count = s.cnt as u64;
        }
    }

    if has_any {
        Some(QualityScoreComparison {
            target: target_scores,
            baseline: baseline_scores,
        })
    } else {
        None
    }
}

/// Fetch recent judge summaries for display on the rollout page.
async fn fetch_recent_summaries(
    state: &FlowState,
    rollout_id: Uuid,
    project_id: Uuid,
) -> Vec<JudgeSummary> {
    // Get request_ids with their variant from ClickHouse
    let request_ids_query = format!(
        r#"
        SELECT request_id, rollout_variant
        FROM reiver.llm_requests
        WHERE rollout_id = '{}'
          AND rollout_variant != ''
        ORDER BY timestamp DESC
        LIMIT 200
        "#,
        rollout_id
    );

    #[derive(Debug, clickhouse::Row, serde::Deserialize)]
    struct ReqVariantRow {
        request_id: String,
        rollout_variant: String,
    }

    let req_rows: Vec<ReqVariantRow> = match state
        .clickhouse
        .query(&request_ids_query)
        .fetch_all()
        .await
    {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };

    if req_rows.is_empty() {
        return Vec::new();
    }

    let request_ids: Vec<&str> = req_rows.iter().map(|r| r.request_id.as_str()).collect();
    let variant_map: HashMap<&str, &str> = req_rows
        .iter()
        .map(|r| (r.request_id.as_str(), r.rollout_variant.as_str()))
        .collect();

    #[derive(Debug, sqlx::FromRow)]
    struct SummaryRow {
        request_id: String,
        reason: Option<String>,
        score_value: Decimal,
        created_at: DateTime<Utc>,
    }

    let summaries: Vec<SummaryRow> = sqlx::query_as(
        r#"
        SELECT request_id, reason, score_value, created_at
        FROM llm_evaluation_scores
        WHERE project_id = $1
          AND request_id = ANY($2)
          AND score_name = 'summary'
          AND evaluator_type = 'llm_judge'
        ORDER BY created_at DESC
        LIMIT 10
        "#,
    )
    .bind(project_id)
    .bind(&request_ids)
    .fetch_all(state.db.as_ref())
    .await
    .unwrap_or_default();

    summaries
        .into_iter()
        .filter_map(|s| {
            let variant = variant_map
                .get(s.request_id.as_str())
                .unwrap_or(&"unknown");
            Some(JudgeSummary {
                request_id: s.request_id,
                variant: variant.to_string(),
                summary: s.reason?,
                score: s.score_value.to_f64().unwrap_or(0.0),
                created_at: s.created_at,
            })
        })
        .collect()
}

// ============================================================================
// Prompt Compiler — trigger compile
// ============================================================================

#[derive(Debug, Deserialize)]
struct TriggerCompileRequest {
    project_id: Uuid,
    #[serde(default)]
    hint: Option<String>,
}

#[derive(Debug, Serialize)]
struct TriggerCompileResponse {
    status: String,
    proposal: Option<serde_json::Value>,
    message: String,
}

async fn trigger_compile(
    State(state): State<Arc<FlowState>>,
    Path(config_id): Path<Uuid>,
    Json(req): Json<TriggerCompileRequest>,
) -> Result<Json<TriggerCompileResponse>> {
    use crate::api::prompt_compiler;

    #[derive(sqlx::FromRow)]
    struct ActiveVersionRow {
        system_prompt: Option<String>,
        model: Option<String>,
        temperature: Decimal,
    }

    let active_version: ActiveVersionRow = sqlx::query_as(
        "SELECT v.system_prompt, v.model, v.temperature \
         FROM llm_prompt_configs c \
         JOIN llm_prompt_versions v ON v.id = c.active_version_id \
         WHERE c.id = $1 AND c.project_id = $2",
    )
    .bind(config_id)
    .bind(req.project_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(AppError::Database)?
    .ok_or_else(|| AppError::NotFound("Prompt config not found or no active version".into()))?;

    let source_prompt = active_version.system_prompt.unwrap_or_default();
    if source_prompt.is_empty() {
        return Err(AppError::Validation(
            "Active version has no system prompt to compile".into(),
        ));
    }

    let gen_prompt =
        prompt_compiler::build_generation_prompt(&source_prompt, req.hint.as_deref(), &[], None);
    let moodeng = crate::moodeng::MoodengClient::new(&state, req.project_id);
    let candidates = prompt_compiler::call_llm_for_candidates(&moodeng, &gen_prompt)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Compilation failed: {e}")))?;

    if candidates.is_empty() {
        return Ok(Json(TriggerCompileResponse {
            status: "no_improvement".into(),
            proposal: None,
            message: "Compiler generated no candidates.".into(),
        }));
    }

    let best = &candidates[0];

    let proposal: crate::api::llm_proposals::PromptProposal = sqlx::query_as(
        "INSERT INTO llm_prompt_proposals \
         (project_id, config_id, system_prompt, model, temperature, \
          reasoning, comparison, session_ids, proposed_by) \
         VALUES ($1, $2, $3, $4, $5, $6, '{}', '{}', 'prompt-compiler') \
         RETURNING id, project_id, config_id, system_prompt, model, temperature, max_tokens, \
                   parameters, variables, tools, response_format, allowed_tools, reasoning, \
                   comparison, session_ids, proposed_by, task_id, created_at",
    )
    .bind(req.project_id)
    .bind(config_id)
    .bind(&best.system_prompt)
    .bind(&active_version.model)
    .bind(active_version.temperature)
    .bind(&best.reasoning)
    .fetch_one(state.db.as_ref())
    .await
    .map_err(AppError::Database)?;

    reiver_core::audit::AuditEventBuilder::new(
        reiver_core::audit::AuditEventType::PromptProposalCreated,
    )
    .resource("prompt_proposal", proposal.id)
    .project(&req.project_id.to_string())
    .details(serde_json::json!({
        "config_id": config_id,
        "proposed_by": "prompt-compiler",
    }))
    .success()
    .log(&state.clickhouse)
    .await;

    let proposal_json = serde_json::to_value(&proposal)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Serialization error: {e}")))?;

    Ok(Json(TriggerCompileResponse {
        status: "proposal_created".into(),
        proposal: Some(proposal_json),
        message: format!("Generated a candidate prompt and created a proposal."),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn make_version_request() -> CreateVersionRequest {
        CreateVersionRequest {
            project_id: Uuid::new_v4(),
            system_prompt: Some("You are helpful.".to_string()),
            model: None,
            temperature: dec!(0.5),
            max_tokens: None,
            parameters: None,
            variables: None,
            tools: None,
            response_format: None,
            commit_message: "Initial version".to_string(),
            allowed_tools: None,
        }
    }

    // -- temperature validation --

    #[test]
    fn test_version_temperature_default_is_valid() {
        let req = make_version_request();
        assert!(validate_version_request(&req).is_ok());
    }

    #[test]
    fn test_version_temperature_zero_is_valid() {
        let mut req = make_version_request();
        req.temperature = Decimal::ZERO;
        assert!(validate_version_request(&req).is_ok());
    }

    #[test]
    fn test_version_temperature_mid_is_valid() {
        let mut req = make_version_request();
        req.temperature = dec!(0.5);
        assert!(validate_version_request(&req).is_ok());
    }

    #[test]
    fn test_version_temperature_one_is_valid() {
        let mut req = make_version_request();
        req.temperature = Decimal::ONE;
        assert!(validate_version_request(&req).is_ok());
    }

    #[test]
    fn test_version_temperature_above_one_rejected() {
        let mut req = make_version_request();
        req.temperature = dec!(1.1);
        let err = validate_version_request(&req).unwrap_err();
        assert!(matches!(err, AppError::Validation(msg) if msg.contains("Temperature")));
    }

    #[test]
    fn test_version_temperature_1_9_rejected() {
        let mut req = make_version_request();
        req.temperature = dec!(1.9);
        let err = validate_version_request(&req).unwrap_err();
        assert!(matches!(err, AppError::Validation(msg) if msg.contains("Temperature")));
    }

    #[test]
    fn test_version_temperature_negative_rejected() {
        let mut req = make_version_request();
        req.temperature = dec!(-0.1);
        let err = validate_version_request(&req).unwrap_err();
        assert!(matches!(err, AppError::Validation(msg) if msg.contains("Temperature")));
    }

    // -- commit_message validation --

    #[test]
    fn test_version_commit_message_valid() {
        let req = make_version_request();
        assert!(validate_version_request(&req).is_ok());
    }

    #[test]
    fn test_version_commit_message_empty_rejected() {
        let mut req = make_version_request();
        req.commit_message = "".to_string();
        let err = validate_version_request(&req).unwrap_err();
        assert!(matches!(err, AppError::Validation(msg) if msg.contains("Commit message")));
    }

    #[test]
    fn test_version_commit_message_blank_rejected() {
        let mut req = make_version_request();
        req.commit_message = "   ".to_string();
        let err = validate_version_request(&req).unwrap_err();
        assert!(matches!(err, AppError::Validation(msg) if msg.contains("Commit message")));
    }

    // -- max_tokens validation --

    #[test]
    fn test_version_max_tokens_none_is_valid() {
        let req = make_version_request();
        assert!(validate_version_request(&req).is_ok());
    }

    #[test]
    fn test_version_max_tokens_one_is_valid() {
        let mut req = make_version_request();
        req.max_tokens = Some(1);
        assert!(validate_version_request(&req).is_ok());
    }

    #[test]
    fn test_version_max_tokens_million_is_valid() {
        let mut req = make_version_request();
        req.max_tokens = Some(1_000_000);
        assert!(validate_version_request(&req).is_ok());
    }

    #[test]
    fn test_version_max_tokens_zero_rejected() {
        let mut req = make_version_request();
        req.max_tokens = Some(0);
        let err = validate_version_request(&req).unwrap_err();
        assert!(matches!(err, AppError::Validation(msg) if msg.contains("max_tokens")));
    }

    #[test]
    fn test_version_max_tokens_negative_rejected() {
        let mut req = make_version_request();
        req.max_tokens = Some(-1);
        let err = validate_version_request(&req).unwrap_err();
        assert!(matches!(err, AppError::Validation(msg) if msg.contains("max_tokens")));
    }

    #[test]
    fn test_version_max_tokens_above_million_rejected() {
        let mut req = make_version_request();
        req.max_tokens = Some(1_000_001);
        let err = validate_version_request(&req).unwrap_err();
        assert!(matches!(err, AppError::Validation(msg) if msg.contains("max_tokens")));
    }

    // -- rollout stage validation --

    fn stage(weight: i32) -> StageConfig {
        StageConfig {
            weight,
            min_duration_minutes: Some(10),
            min_requests: Some(100),
            max_error_rate_increase: None,
            max_latency_increase_pct: None,
            min_quality_score: None,
        }
    }

    #[test]
    fn test_stages_valid_three_stage() {
        let stages = vec![stage(25), stage(50), stage(100)];
        assert!(validate_rollout_stages(&stages).is_ok());
    }

    #[test]
    fn test_stages_single_stage_at_100() {
        let stages = vec![stage(100)];
        assert!(validate_rollout_stages(&stages).is_ok());
    }

    #[test]
    fn test_stages_empty_rejected() {
        let err = validate_rollout_stages(&[]).unwrap_err();
        assert!(matches!(err, AppError::Validation(msg) if msg.contains("At least one stage")));
    }

    #[test]
    fn test_stages_non_ascending_rejected() {
        let stages = vec![stage(50), stage(25), stage(100)];
        let err = validate_rollout_stages(&stages).unwrap_err();
        assert!(matches!(err, AppError::Validation(msg) if msg.contains("ascending")));
    }

    #[test]
    fn test_stages_duplicate_weights_rejected() {
        let stages = vec![stage(50), stage(50), stage(100)];
        let err = validate_rollout_stages(&stages).unwrap_err();
        assert!(matches!(err, AppError::Validation(msg) if msg.contains("ascending")));
    }

    #[test]
    fn test_stages_final_not_100_rejected() {
        let stages = vec![stage(25), stage(75)];
        let err = validate_rollout_stages(&stages).unwrap_err();
        assert!(matches!(err, AppError::Validation(msg) if msg.contains("weight 100")));
    }

    #[test]
    fn test_stages_zero_weight_rejected() {
        let stages = vec![stage(0), stage(50), stage(100)];
        let err = validate_rollout_stages(&stages).unwrap_err();
        assert!(matches!(err, AppError::Validation(msg) if msg.contains("positive")));
    }

    #[test]
    fn test_stages_negative_weight_rejected() {
        let stages = vec![stage(-5), stage(50), stage(100)];
        let err = validate_rollout_stages(&stages).unwrap_err();
        assert!(matches!(err, AppError::Validation(msg) if msg.contains("positive")));
    }

    // -- PromptWriteStore via InMemoryPromptStore --

    use crate::gateway::prompt_store::{
        InMemoryPromptStore, PromptConfigRow as StoreConfigRow, PromptWriteStore,
    };

    #[tokio::test]
    async fn test_in_memory_create_config_succeeds() {
        let store = InMemoryPromptStore::new();
        let project_id = Uuid::new_v4();
        let result = store
            .create_config(project_id, "test-config", Some("A test config"))
            .await;
        assert!(result.is_ok());
        let cfg = result.unwrap();
        assert_eq!(cfg.name, "test-config");
        assert_eq!(cfg.project_id, project_id);
        assert!(cfg.active_version_id.is_none());
    }

    #[tokio::test]
    async fn test_in_memory_duplicate_config_name_fails() {
        let mut store = InMemoryPromptStore::new();
        let project_id = Uuid::new_v4();
        store.add_config(
            project_id,
            "dup",
            StoreConfigRow {
                id: Uuid::new_v4(),
                active_version_id: None,
            },
        );
        let result = store.create_config(project_id, "dup", None).await;
        assert!(result.is_err());
    }
}
