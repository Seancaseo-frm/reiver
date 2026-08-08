//! Prompt Compiler — a stateless prompt rewriting engine.
//!
//! Takes a system prompt (and optional hint/history) and returns candidate
//! rewrites via an LLM call. The compiler has no knowledge of sessions,
//! evaluation, proposals, or optimization targets — those are orchestrated
//! by Moodeng's agent tool loop.
//!
//! Additionally provides the full algorithmic compilation loop (compile-report)
//! that generates a candidate, replays sessions, evaluates with LLM-as-judge,
//! and returns a detailed performance report.

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use reiver_core::db::DbPool;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::api::llm_rollouts::PromptVersion;
use crate::app_state::FlowState;
use crate::error::{AppError, Result};
use crate::gateway::domain_types::RolloutStatus;
use crate::gateway::types::{
    ChatCompletionRequest, ChatMessage, MessageContent, MessageRole, ResponseFormat,
    ResponseFormatType,
};

const MAX_ROUNDS: usize = 3;
const CANDIDATES_PER_ROUND: usize = 3;
const COMPILATION_CANCELLED: &str = "compilation_cancelled";

// ============================================================================
// Types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandidatePrompt {
    pub system_prompt: String,
    pub reasoning: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreviousAttempt {
    pub system_prompt: String,
    pub reasoning: String,
    pub score: f64,
}

// ============================================================================
// Decomposition types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecompStep {
    pub name: String,
    pub description: String,
    pub inputs: String,
    pub output: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Decomposition {
    pub steps: Vec<DecompStep>,
}

// ============================================================================
// Compilation Report types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OptimizationTarget {
    Quality,
    ErrorRate,
    Cost,
    Latency,
}

impl std::fmt::Display for OptimizationTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OptimizationTarget::Quality => write!(f, "quality"),
            OptimizationTarget::ErrorRate => write!(f, "error_rate"),
            OptimizationTarget::Cost => write!(f, "cost"),
            OptimizationTarget::Latency => write!(f, "latency"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionEvalDetail {
    pub session_id: String,
    pub judge_score: f64,
    pub cost_usd: f64,
    pub latency_ms: f64,
    pub error_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalSummary {
    pub avg_judge_score: f64,
    pub total_cost: f64,
    pub avg_latency_ms: f64,
    pub error_count: usize,
    pub per_session: Vec<SessionEvalDetail>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompilationReport {
    pub original_prompt: String,
    pub compiled_prompt: String,
    pub reasoning: String,
    pub optimization_target: OptimizationTarget,
    pub baseline: EvalSummary,
    pub candidate: EvalSummary,
    pub improvement_pct: f64,
    pub sessions_tested: usize,
    pub session_ids: Vec<String>,
    pub config_id: Uuid,
    pub config_name: String,
    pub rounds_used: usize,
    pub candidates_evaluated: usize,
    pub decomposition: Vec<DecompStep>,
}

// ============================================================================
// Routers
// ============================================================================

/// Internal router (server-to-server calls, mounted under /internal)
pub fn create_prompt_compiler_router() -> Router<Arc<FlowState>> {
    Router::new().route("/prompt-compiler/compile", post(handle_compile))
}

#[derive(Debug, Serialize, Deserialize)]
struct CompilerProgressEnvelope {
    compiler_progress: CompilerProgressFields,
}

#[derive(Debug, Serialize, Deserialize)]
struct CompilerProgressFields {
    pct: f32,
    message: String,
}

async fn update_compiler_task_progress(db: &DbPool, task_id: Uuid, pct: f32, message: &str) {
    let env = CompilerProgressEnvelope {
        compiler_progress: CompilerProgressFields {
            pct,
            message: message.to_string(),
        },
    };
    let Ok(json) = serde_json::to_string(&env) else {
        return;
    };
    let _ = sqlx::query(
        "UPDATE agent_tasks SET result = $1 \
         WHERE id = $2 AND task_type = 'compiler' AND status = 'running'",
    )
    .bind(&json)
    .bind(task_id)
    .execute(db)
    .await;
}

#[inline]
fn check_compiler_cancel(cancel: &CancellationToken) -> anyhow::Result<()> {
    if cancel.is_cancelled() {
        anyhow::bail!(COMPILATION_CANCELLED);
    }
    Ok(())
}

fn is_compilation_cancelled_err(e: &anyhow::Error) -> bool {
    e.chain().any(|c| c.to_string() == COMPILATION_CANCELLED)
}

/// User-facing router (frontend calls, mounted under /llm/compiler)
pub fn create_compiler_page_router() -> Router<Arc<FlowState>> {
    use axum::routing::get;
    Router::new()
        .route("/compile-report", post(handle_compile_report))
        .route("/status/{task_id}", get(handle_compilation_status))
        .route("/cancel/{task_id}", post(handle_cancel_compilation))
        .route("/commit", post(handle_commit))
}

// ── Compile endpoint ────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct CompileRequest {
    project_id: Uuid,
    source_prompt: String,
    #[serde(default)]
    hint: Option<String>,
    #[serde(default)]
    rounds: Option<usize>,
    #[serde(default)]
    previous_attempts: Vec<PreviousAttempt>,
}

#[derive(Debug, Serialize)]
struct CompileResponse {
    candidates: Vec<CandidatePrompt>,
    rounds_used: usize,
}

async fn handle_compile(
    State(state): State<Arc<FlowState>>,
    Json(req): Json<CompileRequest>,
) -> Result<Json<CompileResponse>> {
    let moodeng = crate::moodeng::MoodengClient::new(&state, req.project_id);
    let rounds = req.rounds.unwrap_or(1).min(MAX_ROUNDS);
    tracing::info!(
        project_id = %req.project_id,
        rounds,
        hint = ?req.hint,
        history_len = req.previous_attempts.len(),
        "Internal compile started"
    );

    let mut all_candidates: Vec<CandidatePrompt> = Vec::new();
    let mut history = req.previous_attempts;

    for round_idx in 0..rounds {
        let prompt =
            build_generation_prompt(&req.source_prompt, req.hint.as_deref(), &history, None);

        let candidates = call_llm_for_candidates(&moodeng, &prompt).await;
        let candidates = match candidates {
            Ok(c) if !c.is_empty() => {
                tracing::info!(
                    round = round_idx + 1,
                    count = c.len(),
                    "Round produced candidates"
                );
                c
            }
            Ok(_) => {
                tracing::info!(
                    round = round_idx + 1,
                    "Round produced no candidates, stopping"
                );
                break;
            }
            Err(e) => {
                tracing::warn!(round = round_idx + 1, error = %e, "Candidate generation failed");
                break;
            }
        };

        for c in &candidates {
            history.push(PreviousAttempt {
                system_prompt: c.system_prompt.clone(),
                reasoning: c.reasoning.clone(),
                score: 0.0,
            });
        }

        all_candidates.extend(candidates);
    }

    Ok(Json(CompileResponse {
        rounds_used: (all_candidates.len().div_ceil(CANDIDATES_PER_ROUND))
            .max(if all_candidates.is_empty() { 0 } else { 1 }),
        candidates: all_candidates,
    }))
}

pub async fn call_llm_for_candidates(
    moodeng: &crate::moodeng::MoodengClient<'_>,
    prompt: &str,
) -> anyhow::Result<Vec<CandidatePrompt>> {
    tracing::info!(
        prompt_len = prompt.len(),
        "Calling LLM for candidate generation via moodeng-compiler-generate"
    );

    let gen_request = ChatCompletionRequest {
        model: String::new(),
        messages: vec![ChatMessage {
            role: MessageRole::User,
            content: Some(MessageContent::Text(prompt.to_string())),
            name: None,
            tool_calls: None,
            tool_call_id: None,
            reasoning_content: None,
        }],
        temperature: Some(0.7),
        max_tokens: Some(4096),
        stream: Some(false),
        prompt_config: Some("moodeng-compiler-generate".to_string()),
        prompt_variables: None,
        models: None,
        provider: None,
        ..Default::default()
    };

    let result = moodeng
        .call_llm(&gen_request, None)
        .await
        .map_err(|e| anyhow::anyhow!("Candidate generation gateway call failed: {e}"))?;

    let candidates = parse_candidates_response(&result.content);
    tracing::info!(
        candidate_count = candidates.len(),
        response_len = result.content.len(),
        "LLM candidate generation complete"
    );
    Ok(candidates)
}

async fn call_llm_for_candidates_cancelable(
    moodeng: &crate::moodeng::MoodengClient<'_>,
    prompt: &str,
    cancel: &CancellationToken,
) -> anyhow::Result<Vec<CandidatePrompt>> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => anyhow::bail!(COMPILATION_CANCELLED),
        r = call_llm_for_candidates(moodeng, prompt) => r,
    }
}

// ============================================================================
// Compile Report endpoint — full algorithmic compilation loop
// ============================================================================

#[derive(Debug, Deserialize, Clone)]
struct CompileReportRequest {
    project_id: Uuid,
    config_id: Uuid,
    #[serde(default)]
    hint: Option<String>,
}

#[derive(Debug, Serialize)]
struct CompileReportAccepted {
    task_id: Uuid,
}

async fn handle_compile_report(
    State(state): State<Arc<FlowState>>,
    Json(req): Json<CompileReportRequest>,
) -> Result<(axum::http::StatusCode, Json<CompileReportAccepted>)> {
    // Validate config exists before spawning
    let config_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM llm_prompt_configs WHERE id = $1 AND project_id = $2)",
    )
    .bind(req.config_id)
    .bind(req.project_id)
    .fetch_one(state.db.as_ref())
    .await
    .map_err(AppError::Database)?;

    if !config_exists {
        return Err(AppError::NotFound("Prompt config not found".into()));
    }

    // Dedup: check if a compilation is already running for this config
    let running: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM agent_tasks \
         WHERE project_id = $1 AND task_type = 'compiler' AND task_ref = $2 \
         AND status = 'running' AND created_at > NOW() - INTERVAL '10 minutes' \
         LIMIT 1",
    )
    .bind(req.project_id)
    .bind(req.config_id.to_string())
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(AppError::Database)?;

    if let Some(existing_id) = running {
        return Ok((
            axum::http::StatusCode::ACCEPTED,
            Json(CompileReportAccepted {
                task_id: existing_id,
            }),
        ));
    }

    let task_id: Uuid = sqlx::query_scalar(
        "INSERT INTO agent_tasks \
         (project_id, task_type, task_ref, prompt, status, internal) \
         VALUES ($1, 'compiler', $2, $3, 'running', true) \
         RETURNING id",
    )
    .bind(req.project_id)
    .bind(req.config_id.to_string())
    .bind(req.hint.as_deref().unwrap_or(""))
    .fetch_one(state.db.as_ref())
    .await
    .map_err(AppError::Database)?;

    tracing::info!(
        task_id = %task_id,
        project_id = %req.project_id,
        config_id = %req.config_id,
        hint = ?req.hint,
        "Compile-report task created, spawning background compilation"
    );

    let cancel = CancellationToken::new();
    state.compiler_cancel_tokens.insert(task_id, cancel.clone());

    let state_clone = state.clone();
    let req_clone = req.clone();
    tokio::spawn(async move {
        let result = run_compilation_loop(&state_clone, &req_clone, task_id, cancel).await;
        state_clone.compiler_cancel_tokens.remove(&task_id);
        match result {
            Ok(report) => {
                let report_json = serde_json::to_string(&report).unwrap_or_default();
                let _ = sqlx::query(
                    "UPDATE agent_tasks SET \
                     status = 'completed', \
                     result = $2, \
                     completed_at = NOW() \
                     WHERE id = $1 AND status = 'running'",
                )
                .bind(task_id)
                .bind(&report_json)
                .execute(state_clone.db.as_ref())
                .await;
                tracing::info!(%task_id, "Compilation completed successfully");
            }
            Err(e) => {
                if is_compilation_cancelled_err(&e) {
                    let _ = sqlx::query(
                        "UPDATE agent_tasks SET \
                         status = 'cancelled', \
                         result = $2, \
                         completed_at = NOW() \
                         WHERE id = $1 AND status = 'running'",
                    )
                    .bind(task_id)
                    .bind(COMPILATION_CANCELLED)
                    .execute(state_clone.db.as_ref())
                    .await;
                    tracing::info!(%task_id, "Compilation cancelled");
                } else {
                    let _ = sqlx::query(
                        "UPDATE agent_tasks SET \
                         status = 'failed', \
                         result = $2, \
                         completed_at = NOW() \
                         WHERE id = $1 AND status = 'running'",
                    )
                    .bind(task_id)
                    .bind(format!("{e}"))
                    .execute(state_clone.db.as_ref())
                    .await;
                    tracing::error!(%task_id, error = %e, "Compilation failed");
                }
            }
        }
    });

    Ok((
        axum::http::StatusCode::ACCEPTED,
        Json(CompileReportAccepted { task_id }),
    ))
}

#[derive(Debug, Deserialize)]
struct CancelCompilationRequest {
    project_id: Uuid,
}

async fn handle_cancel_compilation(
    State(state): State<Arc<FlowState>>,
    axum::extract::Path(task_id): axum::extract::Path<Uuid>,
    Json(body): Json<CancelCompilationRequest>,
) -> Result<Json<serde_json::Value>> {
    let row: Option<(Uuid,)> = sqlx::query_as(
        "SELECT project_id FROM agent_tasks WHERE id = $1 AND task_type = 'compiler'",
    )
    .bind(task_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(AppError::Database)?;

    let Some((task_project,)) = row else {
        return Err(AppError::NotFound("Compilation task not found".into()));
    };
    if task_project != body.project_id {
        return Err(AppError::NotFound("Compilation task not found".into()));
    }

    if let Some((_, token)) = state.compiler_cancel_tokens.remove(&task_id) {
        token.cancel();
    }

    let _ = sqlx::query(
        "UPDATE agent_tasks SET \
         status = 'cancelled', \
         result = $2, \
         completed_at = NOW() \
         WHERE id = $1 AND task_type = 'compiler' AND status = 'running'",
    )
    .bind(task_id)
    .bind("cancelled by client")
    .execute(state.db.as_ref())
    .await
    .map_err(AppError::Database)?;

    Ok(Json(serde_json::json!({ "cancelled": true })))
}

/// The actual compilation logic, extracted so it can run inside `tokio::spawn`.
async fn run_compilation_loop(
    state: &FlowState,
    req: &CompileReportRequest,
    task_id: Uuid,
    cancel: CancellationToken,
) -> anyhow::Result<CompilationReport> {
    let db = state.db.as_ref();
    let moodeng = crate::moodeng::MoodengClient::new(state, req.project_id);

    #[derive(sqlx::FromRow)]
    struct VersionRow {
        system_prompt: Option<String>,
        temperature: Decimal,
    }
    #[derive(sqlx::FromRow)]
    struct ConfigRow {
        name: String,
    }

    check_compiler_cancel(&cancel)?;
    update_compiler_task_progress(db, task_id, 0.03, "Loading prompt configuration…").await;

    let config: ConfigRow = sqlx::query_as::<_, ConfigRow>(
        "SELECT name FROM llm_prompt_configs WHERE id = $1 AND project_id = $2",
    )
    .bind(req.config_id)
    .bind(req.project_id)
    .fetch_optional(state.db.as_ref())
    .await?
    .ok_or_else(|| anyhow::anyhow!("Prompt config not found"))?;

    tracing::info!(config_name = %config.name, "Loaded prompt config");

    let version: VersionRow = sqlx::query_as::<_, VersionRow>(
        "SELECT v.system_prompt, v.temperature \
         FROM llm_prompt_configs c \
         JOIN llm_prompt_versions v ON v.id = c.active_version_id \
         WHERE c.id = $1 AND c.project_id = $2",
    )
    .bind(req.config_id)
    .bind(req.project_id)
    .fetch_optional(state.db.as_ref())
    .await?
    .ok_or_else(|| anyhow::anyhow!("No active version found"))?;

    let source_prompt = version.system_prompt.unwrap_or_default();
    if source_prompt.is_empty() {
        anyhow::bail!("Active version has no system prompt");
    }

    tracing::info!(prompt_len = source_prompt.len(), "Loaded active version");
    update_compiler_task_progress(db, task_id, 0.06, "Resolving optimization target…").await;

    let target = derive_optimization_target(state, req.project_id).await;
    tracing::info!(optimization_target = %target, "Derived optimization target");

    #[derive(sqlx::FromRow)]
    struct SessionRow {
        session_id: String,
    }
    check_compiler_cancel(&cancel)?;
    let sessions: Vec<SessionRow> = sqlx::query_as(
        "SELECT DISTINCT session_id FROM session_request_content \
         WHERE project_id = $1 \
         ORDER BY session_id DESC LIMIT 5",
    )
    .bind(req.project_id)
    .fetch_all(state.db.as_ref())
    .await?;

    if sessions.is_empty() {
        anyhow::bail!(
            "No sessions with saved content found for evaluation. \
             Configure session profiles to save session content first."
        );
    }

    let session_ids: Vec<String> = sessions.iter().map(|s| s.session_id.clone()).collect();
    let temperature = version.temperature.to_f64().unwrap_or(0.5);
    tracing::info!(
        session_count = session_ids.len(),
        temperature,
        "Starting baseline evaluation"
    );
    update_compiler_task_progress(db, task_id, 0.08, "Decomposing prompt…").await;

    // Phase 1: Decompose the source prompt into sub-tasks
    check_compiler_cancel(&cancel)?;
    let decomposition = match decompose_prompt_cancelable(&moodeng, &source_prompt, &cancel).await {
        Ok(d) if !d.steps.is_empty() => {
            tracing::info!(steps = d.steps.len(), "Decomposition produced steps");
            Some(d)
        }
        Ok(_) => {
            tracing::warn!("Decomposition returned no steps, proceeding without");
            None
        }
        Err(e) if is_compilation_cancelled_err(&e) => return Err(e),
        Err(e) => {
            tracing::warn!(error = %e, "Decomposition failed, proceeding without");
            None
        }
    };

    update_compiler_task_progress(db, task_id, 0.14, "Running baseline session replay…").await;

    // Baseline evaluation
    check_compiler_cancel(&cancel)?;
    let baseline = tokio::select! {
        biased;
        _ = cancel.cancelled() => anyhow::bail!(COMPILATION_CANCELLED),
        ev = evaluate_prompt_against_sessions(
            &moodeng,
            &source_prompt,
            temperature,
            &session_ids,
        ) => ev,
    };

    let baseline_composite = compute_composite_score(&target, &baseline, session_ids.len());
    tracing::info!(
        baseline_score = baseline.avg_judge_score,
        baseline_cost = baseline.total_cost,
        baseline_errors = baseline.error_count,
        baseline_composite,
        "Baseline evaluation complete"
    );
    update_compiler_task_progress(db, task_id, 0.34, "Baseline evaluation complete").await;

    let hint_text = match (&req.hint, &target) {
        (Some(h), _) => h.clone(),
        (None, OptimizationTarget::Quality) => "Maximize response quality and helpfulness".into(),
        (None, OptimizationTarget::ErrorRate) => {
            "Minimize error rate and improve reliability".into()
        }
        (None, OptimizationTarget::Cost) => {
            "Reduce token usage and cost while maintaining quality".into()
        }
        (None, OptimizationTarget::Latency) => {
            "Reduce response latency by being more concise".into()
        }
    };

    // Phase 2: Iterative candidate generation + evaluation
    let mut history: Vec<PreviousAttempt> = Vec::new();
    let mut best_prompt = String::new();
    let mut best_reasoning = String::new();
    let mut best_eval: Option<EvalSummary> = None;
    let mut best_composite = f64::NEG_INFINITY;
    let mut total_candidates_evaluated: usize = 0;
    let mut rounds_used: usize = 0;

    for round_idx in 0..MAX_ROUNDS {
        check_compiler_cancel(&cancel)?;
        tracing::info!(round = round_idx + 1, hint = %hint_text, "Starting generation round");

        let pct_base = 0.36 + (round_idx as f32) * 0.16;
        update_compiler_task_progress(
            db,
            task_id,
            pct_base,
            &format!("Generation round {}: calling LLM…", round_idx + 1),
        )
        .await;

        let gen_prompt = build_generation_prompt(
            &source_prompt,
            Some(&hint_text),
            &history,
            decomposition.as_ref(),
        );
        let candidates = match call_llm_for_candidates_cancelable(&moodeng, &gen_prompt, &cancel)
            .await
        {
            Ok(c) if !c.is_empty() => c,
            Err(e) if is_compilation_cancelled_err(&e) => return Err(e),
            Ok(_) => {
                tracing::warn!(round = round_idx + 1, "No candidates generated, stopping");
                break;
            }
            Err(e) => {
                tracing::warn!(round = round_idx + 1, error = %e, "Candidate generation failed, stopping");
                break;
            }
        };

        update_compiler_task_progress(
            db,
            task_id,
            pct_base + 0.06,
            &format!(
                "Round {}: evaluating {} candidates…",
                round_idx + 1,
                candidates.len()
            ),
        )
        .await;

        tracing::info!(
            round = round_idx + 1,
            count = candidates.len(),
            "Evaluating candidates"
        );
        let mut round_best_composite = f64::NEG_INFINITY;

        let eval_futs: Vec<_> = candidates
            .iter()
            .map(|c| {
                evaluate_prompt_against_sessions(
                    &moodeng,
                    &c.system_prompt,
                    temperature,
                    &session_ids,
                )
            })
            .collect();
        check_compiler_cancel(&cancel)?;
        let evals = tokio::select! {
            biased;
            _ = cancel.cancelled() => anyhow::bail!(COMPILATION_CANCELLED),
            ev = futures::future::join_all(eval_futs) => ev,
        };

        for (ci, (candidate, eval)) in candidates.iter().zip(evals).enumerate() {
            check_compiler_cancel(&cancel)?;
            let composite = compute_composite_score(&target, &eval, session_ids.len());
            total_candidates_evaluated += 1;

            tracing::info!(
                round = round_idx + 1,
                candidate = ci + 1,
                composite,
                judge = eval.avg_judge_score,
                cost = eval.total_cost,
                errors = eval.error_count,
                "Candidate evaluated"
            );

            history.push(PreviousAttempt {
                system_prompt: candidate.system_prompt.clone(),
                reasoning: candidate.reasoning.clone(),
                score: composite,
            });

            if composite > best_composite {
                best_composite = composite;
                best_prompt = candidate.system_prompt.clone();
                best_reasoning = candidate.reasoning.clone();
                best_eval = Some(eval);
            }

            if composite > round_best_composite {
                round_best_composite = composite;
            }
        }

        rounds_used = round_idx + 1;
        update_compiler_task_progress(
            db,
            task_id,
            (pct_base + 0.14).min(0.92),
            &format!("Round {} complete", round_idx + 1),
        )
        .await;

        // Early stop: if this round's best didn't improve over previous round's best
        if round_idx > 0 {
            let prev_round_best = history[..history.len() - candidates.len()]
                .iter()
                .map(|a| a.score)
                .fold(f64::NEG_INFINITY, f64::max);

            if round_best_composite <= prev_round_best {
                tracing::info!(
                    round = round_idx + 1,
                    round_best = round_best_composite,
                    prev_best = prev_round_best,
                    "No improvement this round, early stopping"
                );
                break;
            }
        }
    }

    check_compiler_cancel(&cancel)?;
    let candidate_eval =
        best_eval.ok_or_else(|| anyhow::anyhow!("No candidates were successfully evaluated"))?;
    let improvement_pct = compute_improvement(&target, &baseline, &candidate_eval);

    tracing::info!(
        rounds_used,
        total_candidates_evaluated,
        best_composite,
        candidate_score = candidate_eval.avg_judge_score,
        improvement_pct,
        "Compile-report complete"
    );

    Ok(CompilationReport {
        original_prompt: source_prompt.clone(),
        compiled_prompt: best_prompt,
        reasoning: best_reasoning,
        optimization_target: target,
        baseline,
        candidate: candidate_eval,
        improvement_pct,
        sessions_tested: session_ids.len(),
        session_ids,
        config_id: req.config_id,
        config_name: config.name,
        rounds_used,
        candidates_evaluated: total_candidates_evaluated,
        decomposition: decomposition.map(|d| d.steps).unwrap_or_default(),
    })
}

async fn decompose_prompt_cancelable(
    moodeng: &crate::moodeng::MoodengClient<'_>,
    source_prompt: &str,
    cancel: &CancellationToken,
) -> anyhow::Result<Decomposition> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => anyhow::bail!(COMPILATION_CANCELLED),
        r = decompose_prompt(moodeng, source_prompt) => r,
    }
}

// ============================================================================
// Compilation status polling endpoint
// ============================================================================

#[derive(Debug, Serialize)]
struct CompilationStatusResponse {
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    report: Option<CompilationReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    progress_pct: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    progress_message: Option<String>,
}

async fn handle_compilation_status(
    State(state): State<Arc<FlowState>>,
    axum::extract::Path(task_id): axum::extract::Path<Uuid>,
) -> Result<Json<CompilationStatusResponse>> {
    #[derive(sqlx::FromRow)]
    struct TaskRow {
        status: String,
        result: Option<String>,
    }

    let row: TaskRow = sqlx::query_as(
        "SELECT status, result FROM agent_tasks WHERE id = $1 AND task_type = 'compiler'",
    )
    .bind(task_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(AppError::Database)?
    .ok_or_else(|| AppError::NotFound("Compilation task not found".into()))?;

    match row.status.as_str() {
        "completed" => {
            let report: Option<CompilationReport> = row
                .result
                .as_deref()
                .and_then(|r| serde_json::from_str(r).ok());
            Ok(Json(CompilationStatusResponse {
                status: "completed".into(),
                report,
                error: None,
                progress_pct: None,
                progress_message: None,
            }))
        }
        "failed" => Ok(Json(CompilationStatusResponse {
            status: "failed".into(),
            report: None,
            error: row.result,
            progress_pct: None,
            progress_message: None,
        })),
        "cancelled" => Ok(Json(CompilationStatusResponse {
            status: "cancelled".into(),
            report: None,
            error: row.result,
            progress_pct: None,
            progress_message: None,
        })),
        "running" => {
            let (pct, msg) = row
                .result
                .as_deref()
                .and_then(|r| serde_json::from_str::<CompilerProgressEnvelope>(r).ok())
                .map(|p| {
                    (
                        Some(p.compiler_progress.pct),
                        Some(p.compiler_progress.message),
                    )
                })
                .unwrap_or((None, None));
            Ok(Json(CompilationStatusResponse {
                status: "running".into(),
                report: None,
                error: None,
                progress_pct: pct,
                progress_message: msg,
            }))
        }
        _ => Ok(Json(CompilationStatusResponse {
            status: row.status,
            report: None,
            error: row.result,
            progress_pct: None,
            progress_message: None,
        })),
    }
}

/// Derive optimization target from session profiles.
/// Maps profile filter fields to a target: errors → ErrorRate, cost → Cost,
/// latency → Latency, default → Quality.
async fn derive_optimization_target(state: &FlowState, project_id: Uuid) -> OptimizationTarget {
    let value: Option<String> = sqlx::query_scalar(
        "SELECT value FROM project_settings WHERE project_id = $1 AND key = 'gateway_session_profiles'",
    )
    .bind(project_id)
    .fetch_optional(state.db.as_ref())
    .await
    .ok()
    .flatten();

    let Some(value) = value else {
        return OptimizationTarget::Quality;
    };

    let profiles: Vec<serde_json::Value> = serde_json::from_str(&value).unwrap_or_default();

    for profile in &profiles {
        if let Some(filters) = profile.get("filters").and_then(|f| f.as_array()) {
            for filter in filters {
                let field = filter
                    .get("field")
                    .or_else(|| filter.get("type"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if field.contains("error") {
                    return OptimizationTarget::ErrorRate;
                }
                if field.contains("cost") {
                    return OptimizationTarget::Cost;
                }
                if field.contains("latency") {
                    return OptimizationTarget::Latency;
                }
            }
        }
    }

    OptimizationTarget::Quality
}

/// Replay a prompt against a set of sessions and aggregate results.
async fn evaluate_prompt_against_sessions(
    moodeng: &crate::moodeng::MoodengClient<'_>,
    prompt: &str,
    temperature: f64,
    session_ids: &[String],
) -> EvalSummary {
    tracing::info!(
        session_count = session_ids.len(),
        prompt_len = prompt.len(),
        "Evaluating prompt against sessions"
    );

    let session_futs: Vec<_> = session_ids
        .iter()
        .map(|sid| evaluate_single_session(moodeng, prompt, temperature, sid))
        .collect();
    let per_session: Vec<SessionEvalDetail> = futures::future::join_all(session_futs).await;

    let mut total_judge = 0.0f64;
    let mut total_cost = 0.0f64;
    let mut total_latency = 0.0f64;
    let mut total_errors = 0usize;
    let mut judge_count = 0usize;

    for detail in &per_session {
        if detail.judge_score > 0.0 {
            total_judge += detail.judge_score;
            judge_count += 1;
        }
        total_cost += detail.cost_usd;
        total_latency += detail.latency_ms;
        total_errors += detail.error_count;
    }

    let avg_judge = if judge_count > 0 {
        total_judge / judge_count as f64
    } else {
        0.0
    };
    let avg_latency = if judge_count > 0 {
        total_latency / judge_count as f64
    } else {
        0.0
    };

    EvalSummary {
        avg_judge_score: avg_judge,
        total_cost,
        avg_latency_ms: avg_latency,
        error_count: total_errors,
        per_session,
    }
}

async fn evaluate_single_session(
    moodeng: &crate::moodeng::MoodengClient<'_>,
    prompt: &str,
    temperature: f64,
    sid: &str,
) -> SessionEvalDetail {
    use crate::api::session_replay::SessionReplayer;

    let state = moodeng.state();
    let project_id = moodeng.billing_project_id();
    let flow_url = &state.internal_urls.flow;
    let replayer = SessionReplayer::new(state.db.as_ref(), &state.agent_http_client, flow_url);

    tracing::debug!(session_id = %sid, "Replaying session");

    let requests = match replayer.load_requests(project_id, sid, Some(10)).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(session_id = %sid, error = %e, "Failed to load session requests");
            return SessionEvalDetail {
                session_id: sid.to_string(),
                judge_score: 0.0,
                cost_usd: 0.0,
                latency_ms: 0.0,
                error_count: 1,
            };
        }
    };

    let mut session_judge = 0.0f64;
    let mut session_judge_count = 0usize;
    let mut session_cost = 0.0f64;
    let mut session_latency = 0.0f64;
    let mut session_errors = 0usize;

    for saved in &requests {
        let prepared = match SessionReplayer::prepare_messages(saved, Some(prompt)) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(session_id = %sid, error = %e, "Failed to prepare messages");
                session_errors += 1;
                continue;
            }
        };

        let model = if saved.gen_ai_request_model.is_empty() {
            "gpt-4o-mini"
        } else {
            &saved.gen_ai_request_model
        };

        match replayer
            .replay_request(
                moodeng.key_project_id(),
                prepared,
                model,
                Some(temperature as f32),
                None,
                None,
                Some(project_id),
            )
            .await
        {
            Ok(replayed) => {
                let cost = state
                    .llm_processor
                    .cost_calculator()
                    .calculate_cost(
                        replayed.model.split('/').next().unwrap_or("openai"),
                        &replayed.model,
                        replayed.prompt_tokens,
                        replayed.completion_tokens,
                        0,
                        0,
                    )
                    .await
                    .ok()
                    .and_then(|d| d.to_f64())
                    .unwrap_or(0.0);

                let score =
                    run_judge_via_gateway(moodeng, &replayed.last_user_query, &replayed.content)
                        .await;

                if let Some(s) = score {
                    session_judge += s;
                    session_judge_count += 1;
                }
                session_cost += cost;
                session_latency += replayed.latency_ms as f64;
            }
            Err(e) => {
                tracing::warn!(session_id = %sid, error = %e, "Replay gateway call failed");
                session_errors += 1;
            }
        }
    }

    let avg_score = if session_judge_count > 0 {
        session_judge / session_judge_count as f64
    } else {
        0.0
    };

    tracing::debug!(
        session_id = %sid,
        requests_replayed = requests.len(),
        avg_judge = avg_score,
        cost = session_cost,
        errors = session_errors,
        "Session replay complete"
    );

    SessionEvalDetail {
        session_id: sid.to_string(),
        judge_score: avg_score,
        cost_usd: session_cost,
        latency_ms: session_latency,
        error_count: session_errors,
    }
}

fn compute_improvement(
    target: &OptimizationTarget,
    baseline: &EvalSummary,
    candidate: &EvalSummary,
) -> f64 {
    match target {
        OptimizationTarget::Quality => {
            if baseline.avg_judge_score > 0.0 {
                ((candidate.avg_judge_score - baseline.avg_judge_score) / baseline.avg_judge_score)
                    * 100.0
            } else if candidate.avg_judge_score > 0.0 {
                100.0
            } else {
                0.0
            }
        }
        OptimizationTarget::ErrorRate => {
            let b = baseline.error_count as f64;
            let c = candidate.error_count as f64;
            if b > 0.0 {
                ((b - c) / b) * 100.0
            } else if c == 0.0 {
                0.0
            } else {
                -100.0
            }
        }
        OptimizationTarget::Cost => {
            if baseline.total_cost > 0.0 {
                ((baseline.total_cost - candidate.total_cost) / baseline.total_cost) * 100.0
            } else {
                0.0
            }
        }
        OptimizationTarget::Latency => {
            if baseline.avg_latency_ms > 0.0 {
                ((baseline.avg_latency_ms - candidate.avg_latency_ms) / baseline.avg_latency_ms)
                    * 100.0
            } else {
                0.0
            }
        }
    }
}

// ============================================================================
// Commit endpoint — create a new prompt version from compiled result
// ============================================================================

#[derive(Debug, Deserialize)]
struct CommitRequest {
    project_id: Uuid,
    config_id: Uuid,
    system_prompt: String,
    reasoning: String,
}

#[derive(Debug, Serialize)]
struct CommitResponse {
    version_id: Uuid,
    version: i32,
    rollout_id: Uuid,
}

async fn handle_commit(
    State(state): State<Arc<FlowState>>,
    Json(req): Json<CommitRequest>,
) -> Result<Json<CommitResponse>> {
    tracing::info!(
        project_id = %req.project_id,
        config_id = %req.config_id,
        prompt_len = req.system_prompt.len(),
        "Committing compiled prompt as new version and pending rollout"
    );

    let mut tx = state.db.begin().await.map_err(AppError::Database)?;

    #[derive(sqlx::FromRow)]
    struct ConfigLockRow {
        active_version_id: Option<Uuid>,
    }

    let config_row: ConfigLockRow = sqlx::query_as(
        "SELECT active_version_id FROM llm_prompt_configs \
             WHERE id = $1 AND project_id = $2 FOR UPDATE",
    )
    .bind(req.config_id)
    .bind(req.project_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(AppError::Database)?
    .ok_or_else(|| AppError::NotFound("Prompt config not found".into()))?;

    let baseline_vid = config_row
        .active_version_id
        .ok_or_else(|| AppError::Validation("Prompt config has no active version".into()))?;

    let active: PromptVersion = sqlx::query_as(
        r#"SELECT id, config_id, version, system_prompt, model, temperature, max_tokens, parameters,
                  variables, tools, response_format, commit_message, created_by, created_at,
                  allowed_tools, created_by_type, created_by_key_label
           FROM llm_prompt_versions WHERE id = $1 AND config_id = $2"#,
    )
    .bind(baseline_vid)
    .bind(req.config_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(AppError::Database)?
    .ok_or_else(|| AppError::NotFound("Active prompt version not found".into()))?;

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
            "This prompt already has a running rollout. Stop it before creating a new one.".into(),
        ));
    }

    let next_version: i32 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(version), 0) + 1 FROM llm_prompt_versions WHERE config_id = $1",
    )
    .bind(req.config_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(AppError::Database)?;

    let commit_message = format!("[Compiler] {}", req.reasoning);

    #[derive(sqlx::FromRow)]
    struct NewVersionRow {
        id: Uuid,
        version: i32,
    }

    let version: NewVersionRow = sqlx::query_as(
        r#"
        INSERT INTO llm_prompt_versions
        (config_id, version, system_prompt, model, temperature, max_tokens, parameters, variables,
         tools, response_format, commit_message, created_by, allowed_tools, created_by_type, created_by_key_label)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        RETURNING id, version
        "#,
    )
    .bind(req.config_id)
    .bind(next_version)
    .bind(&req.system_prompt)
    .bind(&active.model)
    .bind(active.temperature)
    .bind(active.max_tokens)
    .bind(&active.parameters)
    .bind(&active.variables)
    .bind(&active.tools)
    .bind(&active.response_format)
    .bind(&commit_message)
    .bind(None::<Uuid>)
    .bind(&active.allowed_tools)
    .bind("system")
    .bind(Some("prompt-compiler".to_string()))
    .fetch_one(&mut *tx)
    .await
    .map_err(AppError::Database)?;

    let rollout_name = format!("Prompt Compiler rollout v{}", next_version);
    let rollout_id: Uuid = sqlx::query_scalar(
        r#"
        INSERT INTO llm_rollouts
        (project_id, config_id, target_version_id, baseline_version_id, name, status, mode)
        VALUES ($1, $2, $3, $4, $5, 'pending', 'auto')
        RETURNING id
        "#,
    )
    .bind(req.project_id)
    .bind(req.config_id)
    .bind(version.id)
    .bind(Some(baseline_vid))
    .bind(&rollout_name)
    .fetch_one(&mut *tx)
    .await
    .map_err(AppError::Database)?;

    crate::api::llm_rollouts::insert_default_rollout_stages_tx(&mut tx, rollout_id)
        .await
        .map_err(AppError::Database)?;

    tx.commit().await.map_err(AppError::Database)?;

    tracing::info!(
        config_id = %req.config_id,
        version_id = %version.id,
        version = version.version,
        rollout_id = %rollout_id,
        "Committed compiler version and created pending rollout"
    );

    reiver_core::audit::AuditEventBuilder::new(
        reiver_core::audit::AuditEventType::PromptVersionCreated,
    )
    .resource("prompt_version", version.id)
    .project(&req.project_id.to_string())
    .details(serde_json::json!({
        "config_id": req.config_id,
        "version": version.version,
        "created_by": "prompt-compiler",
        "commit_message": commit_message,
        "rollout_id": rollout_id,
    }))
    .success()
    .log(&state.clickhouse)
    .await;

    Ok(Json(CommitResponse {
        version_id: version.id,
        version: version.version,
        rollout_id,
    }))
}

// ============================================================================
// LLM-as-judge via gateway (billed)
// ============================================================================

/// Run LLM-as-judge evaluation through the gateway so the tokens are billed.
/// Returns the average score (0.0-1.0) or None if evaluation fails.
async fn run_judge_via_gateway(
    moodeng: &crate::moodeng::MoodengClient<'_>,
    user_query: &str,
    response_text: &str,
) -> Option<f64> {
    if user_query.is_empty() || response_text.is_empty() {
        return None;
    }

    let user_message = format!(
        "User Query:\n{}\n\nAI Response:\n{}",
        user_query, response_text
    );

    let judge_request = ChatCompletionRequest {
        model: String::new(),
        messages: vec![ChatMessage {
            role: MessageRole::User,
            content: Some(MessageContent::Text(user_message)),
            name: None,
            tool_calls: None,
            tool_call_id: None,
            reasoning_content: None,
        }],
        temperature: Some(0.0),
        max_tokens: Some(200),
        stream: Some(false),
        prompt_config: Some("moodeng-compiler-judge".to_string()),
        response_format: Some(ResponseFormat {
            format_type: ResponseFormatType::JsonObject,
        }),
        ..Default::default()
    };

    let result = moodeng
        .call_llm(&judge_request, None)
        .await
        .map_err(|e| {
            tracing::warn!(error = %e, "Judge gateway call failed");
            e
        })
        .ok()?;

    #[derive(serde::Deserialize)]
    struct EvalJson {
        relevance: f64,
        coherence: f64,
        helpfulness: f64,
    }

    let eval: EvalJson = serde_json::from_str(result.content.trim())
        .map_err(|e| {
            tracing::warn!(error = %e, raw = %result.content, "Failed to parse judge JSON");
            e
        })
        .ok()?;

    Some((eval.relevance + eval.coherence + eval.helpfulness) / 3.0)
}

// ============================================================================
// Decomposition
// ============================================================================

/// Analyze a source prompt and decompose it into logical sub-tasks.
async fn decompose_prompt(
    moodeng: &crate::moodeng::MoodengClient<'_>,
    source_prompt: &str,
) -> anyhow::Result<Decomposition> {
    tracing::info!(
        prompt_len = source_prompt.len(),
        "Decomposing prompt into sub-tasks"
    );

    let request = ChatCompletionRequest {
        model: String::new(),
        messages: vec![ChatMessage {
            role: MessageRole::User,
            content: Some(MessageContent::Text(source_prompt.to_string())),
            name: None,
            tool_calls: None,
            tool_call_id: None,
            reasoning_content: None,
        }],
        temperature: Some(0.3),
        max_tokens: Some(2048),
        stream: Some(false),
        prompt_config: Some("moodeng-compiler-decompose".to_string()),
        ..Default::default()
    };

    let result = moodeng
        .call_llm(&request, None)
        .await
        .map_err(|e| anyhow::anyhow!("Decomposition gateway call failed: {e}"))?;

    let decomp = parse_decomposition(&result.content);
    tracing::info!(step_count = decomp.steps.len(), "Decomposition complete");
    Ok(decomp)
}

pub fn parse_decomposition(content: &str) -> Decomposition {
    let trimmed = content.trim();

    // Strip markdown fences if present
    let stripped = if let Some(rest) = trimmed.strip_prefix("```") {
        let rest = rest.strip_prefix("json").unwrap_or(rest);
        let rest = rest.trim_start_matches(|c: char| c == '\r' || c == '\n');
        if let Some(end) = rest.rfind("```") {
            rest[..end].trim()
        } else {
            rest.trim()
        }
    } else {
        trimmed
    };

    // Try as { "steps": [...] } object
    if let Some(start) = stripped.find('{') {
        if let Some(end) = stripped.rfind('}') {
            if start < end {
                if let Ok(d) = serde_json::from_str::<Decomposition>(&stripped[start..=end]) {
                    return d;
                }
            }
        }
    }

    // Fallback: try as bare [...] array
    if let Some(start) = stripped.find('[') {
        if let Some(end) = stripped.rfind(']') {
            if start < end {
                if let Ok(steps) = serde_json::from_str::<Vec<DecompStep>>(&stripped[start..=end]) {
                    return Decomposition { steps };
                }
            }
        }
    }

    Decomposition { steps: vec![] }
}

// ============================================================================
// Composite scoring
// ============================================================================

/// Reduce an EvalSummary to a single comparable score (higher is always better).
pub fn compute_composite_score(
    target: &OptimizationTarget,
    eval: &EvalSummary,
    session_count: usize,
) -> f64 {
    match target {
        OptimizationTarget::Quality => eval.avg_judge_score,
        OptimizationTarget::ErrorRate => {
            if session_count > 0 {
                1.0 - (eval.error_count as f64 / session_count as f64)
            } else {
                1.0
            }
        }
        OptimizationTarget::Cost => 1.0 / (1.0 + eval.total_cost),
        OptimizationTarget::Latency => 1.0 / (1.0 + eval.avg_latency_ms / 1000.0),
    }
}

// ============================================================================
// Prompt construction and parsing (public for tests)
// ============================================================================

/// Build the user-message portion for candidate generation.
/// The system-level instruction lives in the Prompt Hub config
/// `moodeng-compiler-generate`, so this only assembles the data.
/// When a decomposition is provided, it's included so the generator
/// can produce chain-structured prompts.
pub fn build_generation_prompt(
    source_prompt: &str,
    hint: Option<&str>,
    history: &[PreviousAttempt],
    decomposition: Option<&Decomposition>,
) -> String {
    let target_line = match hint {
        Some(h) => format!("OPTIMIZATION HINT: {h}\n"),
        None => "Generate generally improved variants with better clarity, structure, and specificity.\n".to_string(),
    };

    let mut prompt = format!("CURRENT SYSTEM PROMPT:\n```\n{source_prompt}\n```\n\n{target_line}",);

    if let Some(decomp) = decomposition {
        if !decomp.steps.is_empty() {
            prompt.push_str(
                "\nDECOMPOSITION (identified sub-tasks to structure the prompt around):\n",
            );
            for (i, step) in decomp.steps.iter().enumerate() {
                prompt.push_str(&format!(
                    "\nStep {} — {}:\n  Description: {}\n  Inputs: {}\n  Output: {}\n",
                    i + 1,
                    step.name,
                    step.description,
                    step.inputs,
                    step.output,
                ));
            }
            prompt.push_str("\nRewrite the prompt with explicit step-by-step structure based on the decomposition above.\n");
        }
    }

    if !history.is_empty() {
        prompt.push_str("\nPREVIOUS ATTEMPTS (sorted worst to best):\n");
        let mut sorted: Vec<_> = history.iter().collect();
        sorted.sort_by(|a, b| {
            a.score
                .partial_cmp(&b.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        for (i, attempt) in sorted.iter().enumerate() {
            prompt.push_str(&format!(
                "\n--- Attempt {} (score: {:.3}) ---\nReasoning: {}\nPrompt excerpt: {}...\n",
                i + 1,
                attempt.score,
                attempt.reasoning,
                attempt.system_prompt.chars().take(200).collect::<String>(),
            ));
        }
    }

    prompt
}

pub fn parse_candidates_response(content: &str) -> Vec<CandidatePrompt> {
    let trimmed = content.trim();

    let json_str = if let Some(rest) = trimmed.strip_prefix("```") {
        let rest = rest.strip_prefix("json").unwrap_or(rest);
        let rest = rest.trim_start_matches(|c: char| c == '\r' || c == '\n');
        if let Some(end) = rest.rfind("```") {
            &rest[..end]
        } else {
            rest
        }
    } else {
        trimmed
    };

    let json_str = if let (Some(start), Some(end)) = (json_str.find('['), json_str.rfind(']')) {
        if start < end {
            &json_str[start..=end]
        } else {
            json_str
        }
    } else {
        json_str
    };

    serde_json::from_str::<Vec<CandidatePrompt>>(json_str).unwrap_or_default()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ── build_generation_prompt ─────────────────────────────────────

    #[test]
    fn gen_prompt_contains_source() {
        let prompt = build_generation_prompt("You are a helpful assistant.", None, &[], None);
        assert!(prompt.contains("You are a helpful assistant."));
        assert!(prompt.contains("generally improved variants"));
    }

    #[test]
    fn gen_prompt_with_hint() {
        let prompt = build_generation_prompt("test", Some("Minimize error rate"), &[], None);
        assert!(prompt.contains("Minimize error rate"));
        assert!(!prompt.contains("generally improved"));
    }

    #[test]
    fn gen_prompt_includes_history() {
        let history = vec![
            PreviousAttempt {
                system_prompt: "attempt one".into(),
                reasoning: "tried adding examples".into(),
                score: 0.6,
            },
            PreviousAttempt {
                system_prompt: "attempt two".into(),
                reasoning: "simplified instructions".into(),
                score: 0.8,
            },
        ];
        let prompt = build_generation_prompt("original", None, &history, None);
        assert!(prompt.contains("PREVIOUS ATTEMPTS"));
        assert!(prompt.contains("tried adding examples"));
        assert!(prompt.contains("simplified instructions"));
        let pos_06 = prompt.find("0.600").unwrap();
        let pos_08 = prompt.find("0.800").unwrap();
        assert!(pos_06 < pos_08);
    }

    #[test]
    fn gen_prompt_no_history_no_attempts_section() {
        let prompt = build_generation_prompt("test", Some("Minimize cost"), &[], None);
        assert!(!prompt.contains("PREVIOUS ATTEMPTS"));
        assert!(prompt.contains("Minimize cost"));
    }

    #[test]
    fn gen_prompt_with_decomposition() {
        let decomp = Decomposition {
            steps: vec![
                DecompStep {
                    name: "extract_intent".into(),
                    description: "Identify what the user wants".into(),
                    inputs: "user message".into(),
                    output: "intent classification".into(),
                },
                DecompStep {
                    name: "generate_response".into(),
                    description: "Produce the answer".into(),
                    inputs: "intent + context".into(),
                    output: "final response".into(),
                },
            ],
        };
        let prompt =
            build_generation_prompt("test prompt", Some("Improve quality"), &[], Some(&decomp));
        assert!(prompt.contains("DECOMPOSITION"));
        assert!(prompt.contains("extract_intent"));
        assert!(prompt.contains("generate_response"));
        assert!(prompt.contains("step-by-step structure"));
    }

    #[test]
    fn gen_prompt_empty_decomposition_no_section() {
        let decomp = Decomposition { steps: vec![] };
        let prompt = build_generation_prompt("test", None, &[], Some(&decomp));
        assert!(!prompt.contains("DECOMPOSITION"));
    }

    // ── parse_candidates_response ───────────────────────────────────

    #[test]
    fn parse_valid_json() {
        let input = r#"[{"system_prompt": "hello", "reasoning": "simplified"}]"#;
        let candidates = parse_candidates_response(input);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].system_prompt, "hello");
        assert_eq!(candidates[0].reasoning, "simplified");
    }

    #[test]
    fn parse_fenced_json() {
        let input = "```json\n[{\"system_prompt\": \"hi\", \"reasoning\": \"test\"}]\n```";
        let candidates = parse_candidates_response(input);
        assert_eq!(candidates.len(), 1);
    }

    #[test]
    fn parse_malformed_returns_empty() {
        let candidates = parse_candidates_response("this is not json");
        assert!(candidates.is_empty());
    }

    #[test]
    fn parse_wrong_structure_returns_empty() {
        let input = r#"{"system_prompt": "single object, not array"}"#;
        let candidates = parse_candidates_response(input);
        assert!(candidates.is_empty());
    }

    #[test]
    fn parse_json_with_preamble() {
        let input =
            "Sure! Here are the candidates:\n[{\"system_prompt\": \"ok\", \"reasoning\": \"r\"}]";
        let candidates = parse_candidates_response(input);
        assert_eq!(candidates.len(), 1);
    }

    // ── parse_decomposition ─────────────────────────────────────────

    #[test]
    fn parse_decomposition_valid() {
        let input =
            r#"{"steps": [{"name": "extract", "description": "d", "inputs": "i", "output": "o"}]}"#;
        let d = parse_decomposition(input);
        assert_eq!(d.steps.len(), 1);
        assert_eq!(d.steps[0].name, "extract");
    }

    #[test]
    fn parse_decomposition_fenced() {
        let input = "```json\n{\"steps\": [{\"name\": \"a\", \"description\": \"b\", \"inputs\": \"c\", \"output\": \"d\"}]}\n```";
        let d = parse_decomposition(input);
        assert_eq!(d.steps.len(), 1);
    }

    #[test]
    fn parse_decomposition_bare_array() {
        let input = r#"[{"name": "step1", "description": "d", "inputs": "i", "output": "o"}]"#;
        let d = parse_decomposition(input);
        assert_eq!(d.steps.len(), 1);
        assert_eq!(d.steps[0].name, "step1");
    }

    #[test]
    fn parse_decomposition_malformed() {
        let d = parse_decomposition("not json at all");
        assert!(d.steps.is_empty());
    }

    // ── compute_composite_score ─────────────────────────────────────

    #[test]
    fn composite_quality() {
        let eval = EvalSummary {
            avg_judge_score: 0.85,
            total_cost: 0.01,
            avg_latency_ms: 500.0,
            error_count: 0,
            per_session: vec![],
        };
        let score = compute_composite_score(&OptimizationTarget::Quality, &eval, 5);
        assert!((score - 0.85).abs() < 1e-10);
    }

    #[test]
    fn composite_error_rate() {
        let eval = EvalSummary {
            avg_judge_score: 0.5,
            total_cost: 0.0,
            avg_latency_ms: 0.0,
            error_count: 1,
            per_session: vec![],
        };
        let score = compute_composite_score(&OptimizationTarget::ErrorRate, &eval, 5);
        assert!((score - 0.8).abs() < 1e-10);
    }

    #[test]
    fn composite_error_rate_zero_sessions() {
        let eval = EvalSummary {
            avg_judge_score: 0.0,
            total_cost: 0.0,
            avg_latency_ms: 0.0,
            error_count: 0,
            per_session: vec![],
        };
        let score = compute_composite_score(&OptimizationTarget::ErrorRate, &eval, 0);
        assert!((score - 1.0).abs() < 1e-10);
    }

    #[test]
    fn composite_cost_lower_is_better() {
        let cheap = EvalSummary {
            avg_judge_score: 0.5,
            total_cost: 0.01,
            avg_latency_ms: 0.0,
            error_count: 0,
            per_session: vec![],
        };
        let expensive = EvalSummary {
            avg_judge_score: 0.5,
            total_cost: 1.0,
            avg_latency_ms: 0.0,
            error_count: 0,
            per_session: vec![],
        };
        let s_cheap = compute_composite_score(&OptimizationTarget::Cost, &cheap, 1);
        let s_expensive = compute_composite_score(&OptimizationTarget::Cost, &expensive, 1);
        assert!(s_cheap > s_expensive);
    }

    #[test]
    fn composite_latency_lower_is_better() {
        let fast = EvalSummary {
            avg_judge_score: 0.5,
            total_cost: 0.0,
            avg_latency_ms: 100.0,
            error_count: 0,
            per_session: vec![],
        };
        let slow = EvalSummary {
            avg_judge_score: 0.5,
            total_cost: 0.0,
            avg_latency_ms: 5000.0,
            error_count: 0,
            per_session: vec![],
        };
        let s_fast = compute_composite_score(&OptimizationTarget::Latency, &fast, 1);
        let s_slow = compute_composite_score(&OptimizationTarget::Latency, &slow, 1);
        assert!(s_fast > s_slow);
    }
}
