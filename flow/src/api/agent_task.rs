//! Internal agent-task endpoint.
//!
//! Runs MooDeng headlessly for scheduled/operational tasks (separate from
//! investigations which handle incident response). Stores an audit trail
//! in `agent_tasks` and optionally dispatches findings to notification channels.

use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use tracing::Instrument;

use crate::api::agent_executor::{run_tool_loop, StopHook};
use crate::app_state::FlowState;
use crate::error::AppError;

const MAX_TASK_TURNS: usize = 15;

#[derive(Debug, Deserialize)]
pub struct AgentTaskRequest {
    pub project_id: Uuid,
    pub task_type: String,
    pub task_ref: String,
    #[serde(default)]
    pub prompt: String,
    #[serde(default)]
    pub context: serde_json::Value,
    #[serde(default)]
    pub internal: bool,
    #[serde(default)]
    pub notification_channel_ids: Vec<Uuid>,
}

#[derive(Debug, Serialize)]
struct AgentTaskResponse {
    task_id: Uuid,
}


pub fn create_agent_task_router() -> Router<Arc<FlowState>> {
    Router::new().route("/agent-task", post(handle_agent_task))
}

async fn handle_agent_task(
    State(state): State<Arc<FlowState>>,
    Json(req): Json<AgentTaskRequest>,
) -> Result<(StatusCode, Json<AgentTaskResponse>), AppError> {
    let project_id = req.project_id;

    let running: Option<(Uuid,)> = sqlx::query_as(
        "SELECT id FROM agent_tasks \
         WHERE project_id = $1 AND task_ref = $2 AND status = 'running' \
         AND created_at > NOW() - INTERVAL '10 minutes' \
         LIMIT 1",
    )
    .bind(project_id)
    .bind(&req.task_ref)
    .fetch_optional(state.db.as_ref())
    .await?;

    if running.is_some() {
        tracing::info!(%project_id, task_ref = %req.task_ref, "Agent task already running, skipping");
        return Err(AppError::Conflict("Agent task already running".into()));
    }

    let task_id: Uuid = sqlx::query_scalar(
        "INSERT INTO agent_tasks \
         (project_id, task_type, task_ref, prompt, status, internal) \
         VALUES ($1, $2, $3, $4, 'running', $5) \
         RETURNING id",
    )
    .bind(project_id)
    .bind(&req.task_type)
    .bind(&req.task_ref)
    .bind(&req.prompt)
    .bind(req.internal)
    .fetch_one(state.db.as_ref())
    .await?;

    tracing::info!(
        %task_id,
        %project_id,
        task_type = %req.task_type,
        internal = req.internal,
        "Starting agent task"
    );

    let task_span = tracing::info_span!(
        "agent_task",
        %task_id,
        %project_id,
        task_type = %req.task_type,
        task_ref = %req.task_ref,
        internal = req.internal,
        status = tracing::field::Empty,
        model = tracing::field::Empty,
        tool_calls = tracing::field::Empty,
        tokens = tracing::field::Empty,
    );

    tokio::spawn(
        async move {
            if req.task_type == "pricing_sync" {
                match sync_model_catalog(state.as_ref()).await {
                    Ok(summary) => {
                        tracing::Span::current().record("status", "completed");
                        let _ = sqlx::query(
                            "UPDATE agent_tasks SET status = 'completed', result = $2, completed_at = NOW() WHERE id = $1",
                        )
                        .bind(task_id)
                        .bind(&summary)
                        .execute(state.db.as_ref())
                        .await;
                        tracing::info!(%task_id, "{}", summary);
                    }
                    Err(e) => {
                        tracing::Span::current().record("status", "failed");
                        let msg = format!("Model catalog sync failed: {e}");
                        let _ = sqlx::query(
                            "UPDATE agent_tasks SET status = 'failed', result = $2, completed_at = NOW() WHERE id = $1",
                        )
                        .bind(task_id)
                        .bind(&msg)
                        .execute(state.db.as_ref())
                        .await;
                        tracing::error!(%task_id, error = %e, "Model catalog sync failed");
                    }
                }
                return;
            }

            let result = match run_agent_task(state.as_ref(), task_id, &req).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::Span::current().record("status", "failed");
                    tracing::error!(%task_id, error = %e, "Agent task failed");
                    return;
                }
            };

            tracing::Span::current().record("status", "completed");
            dispatch_notifications(state.as_ref(), task_id, &req, &result).await;
        }
        .instrument(task_span),
    );

    Ok((StatusCode::ACCEPTED, Json(AgentTaskResponse { task_id })))
}

// ---------------------------------------------------------------------------
// Generic agent runner — returns the assistant's final text
// ---------------------------------------------------------------------------

async fn run_agent_task(
    state: &FlowState,
    task_id: Uuid,
    req: &AgentTaskRequest,
) -> anyhow::Result<String> {
    let project_id = req.project_id;

    let settings = crate::gateway::routes::get_introspection_settings(state, project_id).await;
    let mut scopes = settings.agent_scopes.clone();

    if req.internal {
        scopes.push(reiver_mcp::scope::INTERNAL_READ.to_string());
        scopes.push(reiver_mcp::scope::INTERNAL_WRITE.to_string());
    }

    let action_ctx = state.build_action_context(
        project_id,
        reiver_mcp::action::Caller::System,
        scopes,
        ("agent_task", &task_id.to_string(), &req.task_type),
    );

    // Resolve prompt config from the prompt hub
    let (prompt_config, mut prompt_variables) =
        build_prompt_hub_params(&req.task_type, &req.context);

    if !settings.agent_soul.is_empty() {
        if let Ok(soul_json) = serde_json::to_value(&settings.agent_soul) {
            prompt_variables.insert("soul".into(), soul_json);
        }
    }

    let user_message = if req.prompt.is_empty() {
        format!("Execute the {} task.", req.task_type)
    } else {
        req.prompt.clone()
    };

    let stop_hook: Option<StopHook> = None;

    let loop_start = std::time::Instant::now();

    let result = run_tool_loop(
        state,
        project_id,
        action_ctx,
        prompt_config,
        prompt_variables.clone(),
        user_message,
        MAX_TASK_TURNS,
        stop_hook.as_ref(),
        Some(task_id.to_string()),
    )
    .await;

    match result {
        Ok(loop_result) => {
            let tool_log = serde_json::to_value(&loop_result.tool_calls_log).unwrap_or_default();
            let total_tokens = loop_result.total_input_tokens + loop_result.total_output_tokens;

            let span = tracing::Span::current();
            span.record("model", &loop_result.model_used.as_str());
            span.record("tool_calls", loop_result.tool_calls_log.len());
            span.record("tokens", total_tokens);

            if !loop_result.outcome.is_success() {
                let detail = loop_result.outcome.error_detail();
                let text = loop_result.outcome.assistant_text();
                let fail_text = if detail.is_empty() && text.is_empty() {
                    format!("Task stopped ({})", loop_result.outcome.status_str())
                } else if !detail.is_empty() {
                    format!(
                        "Task stopped ({}): {}",
                        loop_result.outcome.status_str(),
                        detail
                    )
                } else {
                    format!(
                        "Task stopped ({}): {}",
                        loop_result.outcome.status_str(),
                        text
                    )
                };
                sqlx::query(
                    "UPDATE agent_tasks SET \
                     status = 'failed', \
                     result = $2, \
                     tool_calls_log = $3, \
                     model_used = $4, \
                     tokens_used = $5, \
                     completed_at = NOW() \
                     WHERE id = $1",
                )
                .bind(task_id)
                .bind(&fail_text)
                .bind(&tool_log)
                .bind(&loop_result.model_used)
                .bind(total_tokens as i32)
                .execute(state.db.as_ref())
                .await?;

                tracing::warn!(
                    %task_id,
                    outcome = loop_result.outcome.status_str(),
                    "Agent task ended with non-success outcome"
                );
                return Err(anyhow::anyhow!("{}", fail_text));
            }

            sqlx::query(
                "UPDATE agent_tasks SET \
                 status = 'completed', \
                 result = $2, \
                 tool_calls_log = $3, \
                 model_used = $4, \
                 tokens_used = $5, \
                 completed_at = NOW() \
                 WHERE id = $1",
            )
            .bind(task_id)
            .bind(loop_result.outcome.assistant_text())
            .bind(&tool_log)
            .bind(&loop_result.model_used)
            .bind(total_tokens as i32)
            .execute(state.db.as_ref())
            .await?;

            tracing::info!(
                %task_id,
                model = %loop_result.model_used,
                outcome = loop_result.outcome.status_str(),
                tool_calls = loop_result.tool_calls_log.len(),
                tokens = total_tokens,
                "Agent task completed"
            );

            if let Err(e) = state
                .event_publisher
                .emit(
                    reiver_core::events::PlatformEventType::AgentInvestigationCompleted,
                    project_id,
                    format!("investigation:{}", task_id),
                    serde_json::json!({
                        "task_id": task_id,
                        "task_type": req.task_type,
                        "model": loop_result.model_used,
                        "tool_calls": loop_result.tool_calls_log.len(),
                        "tokens": total_tokens,
                    }),
                )
                .await
            {
                tracing::warn!(%task_id, "Failed to emit task completed event: {}", e);
            }

            Ok(loop_result.outcome.assistant_text().to_string())
        }
        Err(e) => {
            let _ = sqlx::query(
                "UPDATE agent_tasks SET \
                 status = 'failed', \
                 result = $2, \
                 completed_at = NOW() \
                 WHERE id = $1",
            )
            .bind(task_id)
            .bind(format!("Task failed: {e}"))
            .execute(state.db.as_ref())
            .await;

            Err(e)
        }
    }
}

// ---------------------------------------------------------------------------
// Notification dispatch (shared by all task handlers)
// ---------------------------------------------------------------------------

async fn dispatch_notifications(
    state: &FlowState,
    task_id: Uuid,
    req: &AgentTaskRequest,
    findings: &str,
) {
    if let Err(e) = state
        .event_publisher
        .emit(
            reiver_core::events::PlatformEventType::InvestigationCompleted,
            req.project_id,
            format!("investigation_completed:{}", task_id),
            serde_json::json!({
                "investigation_id": task_id,
                "trigger_type": req.task_type,
                "trigger_summary": "Agent task completed",
                "findings": findings,
            }),
        )
        .await
    {
        tracing::warn!(%task_id, "Failed to emit InvestigationCompleted event: {}", e);
    }
}

// ---------------------------------------------------------------------------
// Model catalog sync (replaces old LLM-agent-based pricing sync)
// ---------------------------------------------------------------------------

const OPENROUTER_MODELS_URL: &str = "https://openrouter.ai/api/v1/models";

async fn sync_model_catalog(state: &FlowState) -> anyhow::Result<String> {
    tracing::info!("Fetching model catalog from OpenRouter");

    let resp = state
        .http_client
        .get(OPENROUTER_MODELS_URL)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("OpenRouter request failed: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!(
            "OpenRouter returned {status}: {body}"
        ));
    }

    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to parse OpenRouter response: {e}"))?;

    let entries = body
        .get("data")
        .and_then(|d| d.as_array())
        .ok_or_else(|| anyhow::anyhow!("OpenRouter response missing 'data' array"))?;

    let total = entries.len();
    let mut upserted = 0usize;
    let mut errors = 0usize;

    for entry in entries {
        let id = match entry.get("id").and_then(|v| v.as_str()) {
            Some(id) => id,
            None => {
                errors += 1;
                continue;
            }
        };

        let (provider_slug, model_slug) = match id.split_once('/') {
            Some((p, m)) => (p, m),
            None => {
                errors += 1;
                tracing::warn!(id, "Skipping model with no slash in ID");
                continue;
            }
        };

        let name = entry.get("name").and_then(|v| v.as_str()).unwrap_or(id);
        let created = entry.get("created").and_then(|v| v.as_i64());
        let description = entry.get("description").and_then(|v| v.as_str());
        let context_length = entry
            .get("context_length")
            .and_then(|v| v.as_i64())
            .map(|v| v as i32);
        let canonical_slug = entry.get("canonical_slug").and_then(|v| v.as_str());
        let hugging_face_id = entry.get("hugging_face_id").and_then(|v| v.as_str());
        let knowledge_cutoff = entry.get("knowledge_cutoff").and_then(|v| v.as_str());
        let expiration_date = entry.get("expiration_date").and_then(|v| v.as_str());

        let pricing = entry.get("pricing").cloned().unwrap_or(serde_json::json!({}));
        let architecture = entry.get("architecture").cloned().unwrap_or(serde_json::json!({}));
        let top_provider = entry.get("top_provider").cloned().unwrap_or(serde_json::json!({}));
        let default_parameters = entry.get("default_parameters").cloned();
        let supported_parameters = entry
            .get("supported_parameters")
            .cloned()
            .unwrap_or(serde_json::json!([]));

        let enabled = crate::openrouter_catalog::is_supported_provider(provider_slug);

        let result = sqlx::query(
            "INSERT INTO model_catalog \
             (id, name, created, description, context_length, \
              canonical_slug, hugging_face_id, knowledge_cutoff, expiration_date, \
              pricing, architecture, top_provider, default_parameters, supported_parameters, \
              provider_slug, model_slug, enabled, last_synced_at) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,NOW()) \
             ON CONFLICT (id) DO UPDATE SET \
              name = EXCLUDED.name, \
              created = EXCLUDED.created, \
              description = EXCLUDED.description, \
              context_length = EXCLUDED.context_length, \
              canonical_slug = EXCLUDED.canonical_slug, \
              hugging_face_id = EXCLUDED.hugging_face_id, \
              knowledge_cutoff = EXCLUDED.knowledge_cutoff, \
              expiration_date = EXCLUDED.expiration_date, \
              pricing = EXCLUDED.pricing, \
              architecture = EXCLUDED.architecture, \
              top_provider = EXCLUDED.top_provider, \
              default_parameters = EXCLUDED.default_parameters, \
              supported_parameters = EXCLUDED.supported_parameters, \
              provider_slug = EXCLUDED.provider_slug, \
              model_slug = EXCLUDED.model_slug, \
              last_synced_at = NOW()",
        )
        .bind(id)
        .bind(name)
        .bind(created)
        .bind(description)
        .bind(context_length)
        .bind(canonical_slug)
        .bind(hugging_face_id)
        .bind(knowledge_cutoff)
        .bind(expiration_date)
        .bind(&pricing)
        .bind(&architecture)
        .bind(&top_provider)
        .bind(&default_parameters)
        .bind(&supported_parameters)
        .bind(provider_slug)
        .bind(model_slug)
        .bind(enabled)
        .execute(state.db.as_ref())
        .await;

        match result {
            Ok(_) => upserted += 1,
            Err(e) => {
                errors += 1;
                tracing::warn!(id, error = %e, "Failed to upsert model");
            }
        }
    }

    let summary = format!(
        "Model catalog sync complete: {total} fetched, {upserted} upserted, {errors} errors"
    );
    tracing::info!("{}", summary);
    Ok(summary)
}

// ---------------------------------------------------------------------------
// Prompt hub params
// ---------------------------------------------------------------------------

fn build_prompt_hub_params(
    task_type: &str,
    context: &serde_json::Value,
) -> (
    Option<String>,
    std::collections::HashMap<String, serde_json::Value>,
) {
    match task_type {
        "prompt_compiler" => {
            let mut vars = std::collections::HashMap::new();
            if let Some(config_id) = context.get("config_id") {
                vars.insert("config_id".to_string(), config_id.clone());
            }
            (Some("moodeng-prompt-compiler".to_string()), vars)
        }
        _ => {
            let mut vars = std::collections::HashMap::new();
            vars.insert("context".to_string(), context.clone());
            (Some("moodeng-agent-task".to_string()), vars)
        }
    }
}
