//! Internal auto-investigation endpoint.
//!
//! Called by Watch when an alert fires or an exception is detected
//! (if `gateway_auto_investigate` is enabled for the project).
//! Runs MooDeng headlessly, stores an audit trail in `agent_investigations`,
//! then emits an `InvestigationCompleted` event to Kafka. The event worker
//! handles notification dispatch.

use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::api::agent_executor::run_tool_loop;
use crate::app_state::FlowState;

const MAX_INVESTIGATION_TURNS: usize = 10;

#[derive(Debug, Deserialize)]
pub struct InvestigateRequest {
    pub project_id: Uuid,
    pub trigger_type: String,
    pub trigger_ref: String,
    pub trigger_summary: String,
    pub trigger_context: serde_json::Value,
    pub notification_channel_ids: Vec<Uuid>,
}

#[derive(Debug, Serialize)]
struct InvestigateResponse {
    investigation_id: Uuid,
}



pub fn create_investigate_router() -> Router<Arc<FlowState>> {
    Router::new().route("/investigate", post(investigate))
}

async fn investigate(
    State(state): State<Arc<FlowState>>,
    Json(req): Json<InvestigateRequest>,
) -> Result<(StatusCode, Json<InvestigateResponse>), StatusCode> {
    let project_id = req.project_id;

    // Check that auto-investigate is enabled for this project
    let enabled: Option<String> = sqlx::query_scalar(
        "SELECT value FROM project_settings WHERE project_id = $1 AND key = 'gateway_auto_investigate'",
    )
    .bind(project_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(|e| {
        tracing::error!(error = %e, "Failed to check auto_investigate setting");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    if enabled.as_deref() != Some("true") {
        tracing::debug!(%project_id, "auto_investigate disabled, skipping");
        return Err(StatusCode::CONFLICT);
    }

    // Also verify agent_enabled (kill switch)
    let agent_enabled: Option<String> = sqlx::query_scalar(
        "SELECT value FROM project_settings WHERE project_id = $1 AND key = 'gateway_agent_enabled'",
    )
    .bind(project_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(|e| {
        tracing::error!(error = %e, "Failed to check agent_enabled setting");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    if agent_enabled.as_deref() == Some("false") {
        tracing::debug!(%project_id, "agent disabled, skipping investigation");
        return Err(StatusCode::CONFLICT);
    }

    // Cooldown: skip if a running investigation exists for the same trigger_ref
    let running: Option<(Uuid,)> = sqlx::query_as(
        "SELECT id FROM agent_investigations \
         WHERE project_id = $1 AND trigger_ref = $2 AND status = 'running' \
         AND created_at > NOW() - INTERVAL '10 minutes' \
         LIMIT 1",
    )
    .bind(project_id)
    .bind(&req.trigger_ref)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(|e| {
        tracing::error!(error = %e, "Failed to check investigation cooldown");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    if running.is_some() {
        tracing::info!(%project_id, trigger_ref = %req.trigger_ref, "Investigation already running, skipping");
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }

    // Insert investigation row
    let investigation_id: Uuid = sqlx::query_scalar(
        "INSERT INTO agent_investigations \
         (project_id, trigger_type, trigger_ref, trigger_summary, status) \
         VALUES ($1, $2, $3, $4, 'running') \
         RETURNING id",
    )
    .bind(project_id)
    .bind(&req.trigger_type)
    .bind(&req.trigger_ref)
    .bind(&req.trigger_summary)
    .fetch_one(state.db.as_ref())
    .await
    .map_err(|e| {
        tracing::error!(error = %e, "Failed to insert investigation row");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    tracing::info!(
        %investigation_id,
        %project_id,
        trigger_type = %req.trigger_type,
        trigger_ref = %req.trigger_ref,
        "Starting auto-investigation"
    );

    // Spawn background task
    let state_clone = state.clone();
    tokio::spawn(async move {
        if let Err(e) = run_investigation_task(&state_clone, investigation_id, req).await {
            tracing::error!(
                %investigation_id,
                error = %e,
                "Auto-investigation failed"
            );
        }
    });

    Ok((
        StatusCode::ACCEPTED,
        Json(InvestigateResponse { investigation_id }),
    ))
}

async fn run_investigation_task(
    state: &FlowState,
    investigation_id: Uuid,
    req: InvestigateRequest,
) -> anyhow::Result<()> {
    let project_id = req.project_id;

    // Load agent scopes from project settings
    let settings = crate::gateway::routes::get_introspection_settings(state, project_id).await;
    let agent_scopes = settings.agent_scopes.clone();

    let action_ctx = state.build_action_context(
        project_id,
        reiver_mcp::action::Caller::System,
        agent_scopes,
        (
            "agent_investigation",
            &investigation_id.to_string(),
            &req.trigger_summary,
        ),
    );

    let mut prompt_variables = std::collections::HashMap::new();
    prompt_variables.insert(
        "trigger_type".to_string(),
        serde_json::Value::String(req.trigger_type.clone()),
    );
    prompt_variables.insert("trigger_context".to_string(), req.trigger_context.clone());

    if !settings.agent_soul.is_empty() {
        if let Ok(soul_json) = serde_json::to_value(&settings.agent_soul) {
            prompt_variables.insert("soul".into(), soul_json);
        }
    }

    let user_message = req.trigger_summary.clone();

    let loop_start = std::time::Instant::now();

    let result = run_tool_loop(
        state,
        project_id,
        action_ctx,
        Some("moodeng-investigation".to_string()),
        prompt_variables.clone(),
        user_message,
        MAX_INVESTIGATION_TURNS,
        None,
        Some(investigation_id.to_string()),
    )
    .await;

    match result {
        Ok(loop_result) => {
            let tool_log = serde_json::to_value(&loop_result.tool_calls_log).unwrap_or_default();
            let total_tokens = loop_result.total_input_tokens + loop_result.total_output_tokens;

            if !loop_result.outcome.is_success() {
                let detail = loop_result.outcome.error_detail();
                let text = loop_result.outcome.assistant_text();
                let fail_text = if !detail.is_empty() {
                    format!(
                        "Investigation stopped ({}): {}",
                        loop_result.outcome.status_str(),
                        detail
                    )
                } else if !text.is_empty() {
                    format!(
                        "Investigation stopped ({}): {}",
                        loop_result.outcome.status_str(),
                        text
                    )
                } else {
                    format!(
                        "Investigation stopped ({})",
                        loop_result.outcome.status_str()
                    )
                };
                sqlx::query(
                    "UPDATE agent_investigations SET \
                     status = 'failed', \
                     findings = $2, \
                     tool_calls_log = $3, \
                     model_used = $4, \
                     tokens_used = $5, \
                     completed_at = NOW() \
                     WHERE id = $1",
                )
                .bind(investigation_id)
                .bind(&fail_text)
                .bind(&tool_log)
                .bind(&loop_result.model_used)
                .bind(total_tokens as i32)
                .execute(state.db.as_ref())
                .await?;

                tracing::warn!(
                    %investigation_id,
                    outcome = loop_result.outcome.status_str(),
                    "Investigation ended with non-success outcome"
                );
                return Err(anyhow::anyhow!("{}", fail_text));
            }

            sqlx::query(
                "UPDATE agent_investigations SET \
                 status = 'completed', \
                 findings = $2, \
                 tool_calls_log = $3, \
                 model_used = $4, \
                 tokens_used = $5, \
                 completed_at = NOW() \
                 WHERE id = $1",
            )
            .bind(investigation_id)
            .bind(loop_result.outcome.assistant_text())
            .bind(&tool_log)
            .bind(&loop_result.model_used)
            .bind(total_tokens as i32)
            .execute(state.db.as_ref())
            .await?;

            tracing::info!(
                %investigation_id,
                model = %loop_result.model_used,
                tool_calls = loop_result.tool_calls_log.len(),
                tokens = total_tokens,
                "Investigation completed"
            );

            // Emit platform event for the subscription system
            if let Err(e) = state
                .event_publisher
                .emit(
                    reiver_core::events::PlatformEventType::AgentInvestigationCompleted,
                    project_id,
                    format!("investigation:{}", investigation_id),
                    serde_json::json!({
                        "investigation_id": investigation_id,
                        "trigger_type": req.trigger_type,
                        "trigger_ref": req.trigger_ref,
                        "model": loop_result.model_used,
                        "tool_calls": loop_result.tool_calls_log.len(),
                        "tokens": total_tokens,
                    }),
                )
                .await
            {
                tracing::warn!(%investigation_id, "Failed to emit AgentInvestigationCompleted event: {}", e);
            }

            // Emit InvestigationCompleted event — the event worker dispatches
            // findings to the project's notification channels.
            if let Err(e) = state
                .event_publisher
                .emit(
                    reiver_core::events::PlatformEventType::InvestigationCompleted,
                    project_id,
                    format!("investigation_completed:{}", investigation_id),
                    serde_json::json!({
                        "investigation_id": investigation_id,
                        "trigger_type": req.trigger_type,
                        "trigger_summary": req.trigger_summary,
                        "findings": loop_result.outcome.assistant_text(),
                    }),
                )
                .await
            {
                tracing::warn!(%investigation_id, "Failed to emit InvestigationCompleted event: {}", e);
            }
        }
        Err(e) => {
            if let Err(db_err) = sqlx::query(
                "UPDATE agent_investigations SET \
                 status = 'failed', \
                 findings = $2, \
                 completed_at = NOW() \
                 WHERE id = $1",
            )
            .bind(investigation_id)
            .bind(format!("Investigation failed: {e}"))
            .execute(state.db.as_ref())
            .await
            {
                tracing::error!(
                    %investigation_id,
                    error = %db_err,
                    "Failed to mark investigation as failed in DB"
                );
            }

            return Err(e);
        }
    }

    Ok(())
}
