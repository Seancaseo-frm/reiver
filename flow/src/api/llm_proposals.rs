//! Prompt Compiler proposal endpoints.
//!
//! Proposals are transient: they exist in `llm_prompt_proposals` until
//! accepted (→ new version + rollout) or dismissed (→ deleted).

use axum::extract::{Path, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::app_state::FlowState;
use crate::error::{AppError, Result};

pub fn create_llm_proposals_router() -> Router<Arc<FlowState>> {
    Router::new()
        .route(
            "/configs/{config_id}/proposals",
            get(list_proposals).post(create_proposal),
        )
        .route("/proposals/{proposal_id}/accept", post(accept_proposal))
        .route("/proposals/{proposal_id}/dismiss", post(dismiss_proposal))
}

// ============================================================================
// Types
// ============================================================================

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct PromptProposal {
    pub id: Uuid,
    pub project_id: Uuid,
    pub config_id: Uuid,
    pub system_prompt: Option<String>,
    pub model: Option<String>,
    pub temperature: Decimal,
    pub max_tokens: Option<i32>,
    pub parameters: serde_json::Value,
    pub variables: serde_json::Value,
    pub tools: Option<serde_json::Value>,
    pub response_format: Option<serde_json::Value>,
    pub allowed_tools: Option<serde_json::Value>,
    pub reasoning: String,
    pub comparison: serde_json::Value,
    pub session_ids: Vec<String>,
    pub proposed_by: String,
    pub task_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct CreateProposalRequest {
    pub project_id: Uuid,
    pub config_id: String,
    pub system_prompt: Option<String>,
    pub model: Option<String>,
    #[serde(default = "default_temperature")]
    pub temperature: Decimal,
    pub max_tokens: Option<i32>,
    pub variables: Option<serde_json::Value>,
    pub tools: Option<serde_json::Value>,
    pub response_format: Option<serde_json::Value>,
    pub allowed_tools: Option<serde_json::Value>,
    pub reasoning: String,
    pub comparison: serde_json::Value,
    pub session_ids: Vec<String>,
    pub proposed_by: Option<String>,
    pub task_id: Option<Uuid>,
}

fn default_temperature() -> Decimal {
    Decimal::new(5, 1)
}

#[derive(Debug, Deserialize)]
pub struct AcceptRequest {
    pub project_id: Uuid,
}

#[derive(Debug, Deserialize)]
pub struct DismissRequest {
    pub project_id: Uuid,
}

// ============================================================================
// Handlers
// ============================================================================

async fn list_proposals(
    State(state): State<Arc<FlowState>>,
    Path(config_id): Path<Uuid>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Vec<PromptProposal>>> {
    let project_id: Uuid = params
        .get("project_id")
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| AppError::Validation("project_id is required".into()))?;

    let proposals: Vec<PromptProposal> = sqlx::query_as(
        "SELECT id, project_id, config_id, system_prompt, model, temperature, max_tokens, \
         parameters, variables, tools, response_format, allowed_tools, reasoning, comparison, \
         session_ids, proposed_by, task_id, created_at \
         FROM llm_prompt_proposals \
         WHERE config_id = $1 AND project_id = $2 \
         ORDER BY created_at DESC",
    )
    .bind(config_id)
    .bind(project_id)
    .fetch_all(state.db.as_ref())
    .await
    .map_err(AppError::Database)?;

    Ok(Json(proposals))
}

async fn create_proposal(
    State(state): State<Arc<FlowState>>,
    Path(config_id): Path<Uuid>,
    Json(req): Json<CreateProposalRequest>,
) -> Result<Json<PromptProposal>> {
    let config_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM llm_prompt_configs WHERE id = $1 AND project_id = $2)",
    )
    .bind(config_id)
    .bind(req.project_id)
    .fetch_one(state.db.as_ref())
    .await
    .map_err(AppError::Database)?;

    if !config_exists {
        return Err(AppError::NotFound("Prompt config not found".into()));
    }

    let proposal: PromptProposal = sqlx::query_as(
        "INSERT INTO llm_prompt_proposals \
         (project_id, config_id, system_prompt, model, temperature, max_tokens, \
          variables, tools, response_format, allowed_tools, reasoning, comparison, \
          session_ids, proposed_by, task_id) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15) \
         RETURNING id, project_id, config_id, system_prompt, model, temperature, max_tokens, \
                   parameters, variables, tools, response_format, allowed_tools, reasoning, \
                   comparison, session_ids, proposed_by, task_id, created_at",
    )
    .bind(req.project_id)
    .bind(config_id)
    .bind(&req.system_prompt)
    .bind(&req.model)
    .bind(req.temperature)
    .bind(req.max_tokens)
    .bind(req.variables.as_ref().unwrap_or(&serde_json::json!([])))
    .bind(&req.tools)
    .bind(&req.response_format)
    .bind(&req.allowed_tools)
    .bind(&req.reasoning)
    .bind(&req.comparison)
    .bind(&req.session_ids)
    .bind(req.proposed_by.as_deref().unwrap_or("agent"))
    .bind(req.task_id)
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
        "proposed_by": req.proposed_by.as_deref().unwrap_or("agent"),
        "session_count": req.session_ids.len(),
    }))
    .success()
    .log(&state.clickhouse)
    .await;

    Ok(Json(proposal))
}

async fn accept_proposal(
    State(state): State<Arc<FlowState>>,
    Path(proposal_id): Path<Uuid>,
    Json(req): Json<AcceptRequest>,
) -> Result<Json<serde_json::Value>> {
    let proposal: PromptProposal = sqlx::query_as(
        "SELECT id, project_id, config_id, system_prompt, model, temperature, max_tokens, \
         parameters, variables, tools, response_format, allowed_tools, reasoning, comparison, \
         session_ids, proposed_by, task_id, created_at \
         FROM llm_prompt_proposals WHERE id = $1 AND project_id = $2",
    )
    .bind(proposal_id)
    .bind(req.project_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(AppError::Database)?
    .ok_or_else(|| AppError::NotFound("Proposal not found".into()))?;

    let mut tx = state.db.begin().await.map_err(AppError::Database)?;

    let next_version: i32 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(version), 0) + 1 FROM llm_prompt_versions WHERE config_id = $1",
    )
    .bind(proposal.config_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(AppError::Database)?;

    let reasoning_summary = if proposal.reasoning.len() > 100 {
        format!("{}...", &proposal.reasoning[..100])
    } else {
        proposal.reasoning.clone()
    };

    let version_id: Uuid = sqlx::query_scalar(
        "INSERT INTO llm_prompt_versions \
         (config_id, version, system_prompt, model, temperature, max_tokens, \
          parameters, variables, tools, response_format, commit_message, \
          allowed_tools, created_by_type, created_by_key_label) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 'agent', 'prompt-compiler') \
         RETURNING id",
    )
    .bind(proposal.config_id)
    .bind(next_version)
    .bind(&proposal.system_prompt)
    .bind(&proposal.model)
    .bind(proposal.temperature)
    .bind(proposal.max_tokens)
    .bind(&proposal.parameters)
    .bind(&proposal.variables)
    .bind(&proposal.tools)
    .bind(&proposal.response_format)
    .bind(format!("Prompt Compiler: {}", reasoning_summary))
    .bind(&proposal.allowed_tools)
    .fetch_one(&mut *tx)
    .await
    .map_err(AppError::Database)?;

    let rollout_id: Uuid = sqlx::query_scalar(
        "INSERT INTO llm_rollouts \
         (project_id, config_id, target_version_id, name, status, mode) \
         VALUES ($1, $2, $3, $4, 'pending', 'auto') \
         RETURNING id",
    )
    .bind(proposal.project_id)
    .bind(proposal.config_id)
    .bind(version_id)
    .bind(format!("Prompt Compiler rollout v{}", next_version))
    .fetch_one(&mut *tx)
    .await
    .map_err(AppError::Database)?;

    sqlx::query("DELETE FROM llm_prompt_proposals WHERE id = $1")
        .bind(proposal_id)
        .execute(&mut *tx)
        .await
        .map_err(AppError::Database)?;

    tx.commit().await.map_err(AppError::Database)?;

    reiver_core::audit::AuditEventBuilder::new(
        reiver_core::audit::AuditEventType::PromptProposalAccepted,
    )
    .resource("prompt_proposal", proposal_id)
    .project(&req.project_id.to_string())
    .details(serde_json::json!({
        "proposal_id": proposal_id,
        "new_version_id": version_id,
        "new_version": next_version,
        "rollout_id": rollout_id,
        "config_id": proposal.config_id,
    }))
    .success()
    .log(&state.clickhouse)
    .await;

    Ok(Json(serde_json::json!({
        "accepted": true,
        "version_id": version_id,
        "version": next_version,
        "rollout_id": rollout_id,
    })))
}

async fn dismiss_proposal(
    State(state): State<Arc<FlowState>>,
    Path(proposal_id): Path<Uuid>,
    Json(req): Json<DismissRequest>,
) -> Result<Json<serde_json::Value>> {
    let proposal: Option<(Uuid, Uuid, String)> = sqlx::query_as(
        "SELECT id, config_id, reasoning FROM llm_prompt_proposals \
         WHERE id = $1 AND project_id = $2",
    )
    .bind(proposal_id)
    .bind(req.project_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(AppError::Database)?;

    let (_, config_id, reasoning) =
        proposal.ok_or_else(|| AppError::NotFound("Proposal not found".into()))?;

    sqlx::query("DELETE FROM llm_prompt_proposals WHERE id = $1")
        .bind(proposal_id)
        .execute(state.db.as_ref())
        .await
        .map_err(AppError::Database)?;

    reiver_core::audit::AuditEventBuilder::new(
        reiver_core::audit::AuditEventType::PromptProposalDismissed,
    )
    .resource("prompt_proposal", proposal_id)
    .project(&req.project_id.to_string())
    .details(serde_json::json!({
        "proposal_id": proposal_id,
        "config_id": config_id,
        "reasoning": reasoning,
    }))
    .success()
    .log(&state.clickhouse)
    .await;

    Ok(Json(serde_json::json!({ "dismissed": true })))
}
