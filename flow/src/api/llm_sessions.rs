//! LLM Sessions API
//!
//! Endpoints for viewing and managing LLM conversation sessions.
//! Sessions group related LLM requests together for analysis.
//! All session data is served from Postgres (precomputed at evaluation time).

use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    response::sse::{Event, Sse},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use futures::stream::StreamExt;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::sync::Arc;
use uuid::Uuid;

use crate::app_state::FlowState;
use crate::error::{AppError, Result};
use crate::gateway::types::{MessageContent, MessageRole};

pub fn create_llm_sessions_router() -> Router<Arc<FlowState>> {
    Router::new()
        .route("/", get(list_sessions))
        .route("/{session_id}", get(get_session_detail))
        .route("/{session_id}/requests", get(get_session_requests))
        .route("/{session_id}/feedback", post(submit_session_feedback))
        .route("/{session_id}/replay", post(replay_session))
}

/// Query parameters for listing sessions
#[derive(Debug, Deserialize)]
pub struct ListSessionsParams {
    #[serde(default = "crate::api::default_list_limit")]
    pub limit: u32,
    #[serde(default)]
    pub offset: u32,
    /// Filter by session name pattern
    pub name_pattern: Option<String>,
    /// Filter by start date
    pub start_date: Option<DateTime<Utc>>,
    /// Filter by end date
    pub end_date: Option<DateTime<Utc>>,
    /// Filter by session profile ID
    pub profile_id: Option<Uuid>,
}

/// Maximum allowed limit for query results to prevent expensive queries
const MAX_LIMIT: u32 = 1000;

/// Matched profile info returned in session summaries.
#[derive(Clone, Debug, Serialize)]
pub struct MatchedProfile {
    pub profile_id: Uuid,
    pub profile_name: String,
}

/// Session summary returned in list view
#[derive(Debug, Serialize)]
pub struct SessionSummary {
    pub session_id: String,
    pub user_id: String,
    pub session_name: String,
    pub first_request_time: DateTime<Utc>,
    pub last_request_time: DateTime<Utc>,
    pub request_count: u64,
    pub total_tokens: u64,
    pub total_cost_usd: Decimal,
    pub error_count: u64,
    pub feedback_score: Option<i32>,
    pub matched_profiles: Vec<MatchedProfile>,
    pub labels: Vec<String>,
}

/// List sessions response
#[derive(Debug, Serialize)]
pub struct ListSessionsResponse {
    pub sessions: Vec<SessionSummary>,
    pub total: u64,
    pub limit: u32,
    pub offset: u32,
}

/// List saved sessions from the precomputed saved_sessions table.
async fn list_sessions(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    Query(params): Query<ListSessionsParams>,
) -> Result<Json<ListSessionsResponse>> {
    let project_id = crate::api::extract_project_id(&headers)?;
    let limit = params.limit.min(MAX_LIMIT);

    #[derive(Debug, sqlx::FromRow)]
    struct Row {
        session_id: String,
        user_id: String,
        session_name: String,
        first_request_time: DateTime<Utc>,
        last_request_time: DateTime<Utc>,
        request_count: i32,
        total_input_tokens: i64,
        total_output_tokens: i64,
        total_cost_usd: f64,
        error_count: i32,
        labels: Vec<String>,
    }

    let mut conditions = vec!["s.project_id = $1".to_string()];
    let mut param_idx = 1u32;

    if params.name_pattern.is_some() {
        param_idx += 1;
        conditions.push(format!("s.session_name ILIKE '%' || ${param_idx} || '%'"));
    }
    if params.start_date.is_some() {
        param_idx += 1;
        conditions.push(format!("s.last_request_time >= ${param_idx}"));
    }
    if params.end_date.is_some() {
        param_idx += 1;
        conditions.push(format!("s.last_request_time <= ${param_idx}"));
    }
    if params.profile_id.is_some() {
        param_idx += 1;
        conditions.push(format!(
            "s.session_id IN (SELECT spm.session_id FROM session_profile_matches spm WHERE spm.project_id = $1 AND spm.profile_id = ${param_idx})"
        ));
    }

    let where_clause = conditions.join(" AND ");
    let query_str = format!(
        r#"
        SELECT session_id, user_id, session_name,
               first_request_time, last_request_time,
               request_count, total_input_tokens, total_output_tokens,
               total_cost_usd, error_count,
               COALESCE(labels, ARRAY[]::TEXT[]) as labels
        FROM saved_sessions s
        WHERE {where_clause}
        ORDER BY s.last_request_time DESC
        LIMIT ${} OFFSET ${}
        "#,
        param_idx + 1,
        param_idx + 2,
    );

    let mut q = sqlx::query_as::<_, Row>(&query_str).bind(project_id);
    if let Some(ref name_pattern) = params.name_pattern {
        q = q.bind(name_pattern);
    }
    if let Some(start_date) = params.start_date {
        q = q.bind(start_date);
    }
    if let Some(end_date) = params.end_date {
        q = q.bind(end_date);
    }
    if let Some(profile_id) = params.profile_id {
        q = q.bind(profile_id);
    }
    q = q.bind(limit as i64).bind(params.offset as i64);

    let rows: Vec<Row> = q
        .fetch_all(state.db.as_ref())
        .await
        .map_err(|e| AppError::Database(e))?;

    let session_ids: Vec<String> = rows.iter().map(|r| r.session_id.clone()).collect();
    let feedback_map = get_session_feedback_map(&state, project_id, &session_ids).await?;
    let profile_matches = get_session_profile_matches(&state, project_id, &session_ids).await?;
    let profile_names = load_profile_names(&state, project_id).await?;

    let sessions: Vec<SessionSummary> = rows
        .into_iter()
        .map(|r| {
            let feedback_score = feedback_map.get(&r.session_id).copied().flatten();
            let matched_profiles = profile_matches
                .get(&r.session_id)
                .map(|pids| {
                    pids.iter()
                        .map(|pid| MatchedProfile {
                            profile_id: *pid,
                            profile_name: profile_names.get(pid).cloned().unwrap_or_default(),
                        })
                        .collect()
                })
                .unwrap_or_default();
            SessionSummary {
                session_id: r.session_id,
                user_id: r.user_id,
                session_name: r.session_name,
                first_request_time: r.first_request_time,
                last_request_time: r.last_request_time,
                request_count: r.request_count as u64,
                total_tokens: (r.total_input_tokens + r.total_output_tokens) as u64,
                total_cost_usd: Decimal::from_f64_retain(r.total_cost_usd).unwrap_or(Decimal::ZERO),
                error_count: r.error_count as u64,
                feedback_score,
                matched_profiles,
                labels: r.labels,
            }
        })
        .collect();

    // Total count with same filters
    let count_str = format!("SELECT COUNT(*) FROM saved_sessions s WHERE {where_clause}");
    let mut cq = sqlx::query_scalar::<_, i64>(&count_str).bind(project_id);
    if let Some(ref name_pattern) = params.name_pattern {
        cq = cq.bind(name_pattern);
    }
    if let Some(start_date) = params.start_date {
        cq = cq.bind(start_date);
    }
    if let Some(end_date) = params.end_date {
        cq = cq.bind(end_date);
    }
    if let Some(profile_id) = params.profile_id {
        cq = cq.bind(profile_id);
    }
    let total = cq
        .fetch_one(state.db.as_ref())
        .await
        .map(|v| v as u64)
        .unwrap_or(0);

    Ok(Json(ListSessionsResponse {
        sessions,
        total,
        limit,
        offset: params.offset,
    }))
}

/// Session detail with all requests
#[derive(Debug, Serialize)]
pub struct SessionDetail {
    pub session_id: String,
    pub user_id: String,
    pub session_name: String,
    pub first_request_time: DateTime<Utc>,
    pub last_request_time: DateTime<Utc>,
    pub request_count: u64,
    pub total_input_tokens: u64,
    pub total_output_tokens: u64,
    pub total_cost_usd: Decimal,
    pub avg_latency_ms: u32,
    pub error_count: u64,
    pub models: Vec<String>,
    pub feedback_score: Option<i32>,
    pub feedback_text: Option<String>,
    pub fallback_count: u64,
    pub guardrail_count: u64,
}

/// Get detailed session information from precomputed saved_sessions.
async fn get_session_detail(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<SessionDetail>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    #[derive(Debug, sqlx::FromRow)]
    struct Row {
        user_id: String,
        session_name: String,
        first_request_time: DateTime<Utc>,
        last_request_time: DateTime<Utc>,
        request_count: i32,
        total_input_tokens: i64,
        total_output_tokens: i64,
        total_cost_usd: f64,
        avg_latency_ms: i32,
        error_count: i32,
        models: Vec<String>,
        fallback_count: i32,
        guardrail_count: i32,
    }

    let row: Row = sqlx::query_as(
        r#"
        SELECT user_id, session_name,
               first_request_time, last_request_time,
               request_count, total_input_tokens, total_output_tokens,
               total_cost_usd, avg_latency_ms, error_count, models,
               fallback_count, guardrail_count
        FROM saved_sessions
        WHERE project_id = $1 AND session_id = $2
        "#,
    )
    .bind(project_id)
    .bind(&session_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(|e| AppError::Database(e))?
    .ok_or_else(|| AppError::NotFound(format!("Session not found: {}", session_id)))?;

    let feedback = get_session_feedback(&state, project_id, &session_id).await?;

    Ok(Json(SessionDetail {
        session_id,
        user_id: row.user_id,
        session_name: row.session_name,
        first_request_time: row.first_request_time,
        last_request_time: row.last_request_time,
        request_count: row.request_count as u64,
        total_input_tokens: row.total_input_tokens as u64,
        total_output_tokens: row.total_output_tokens as u64,
        total_cost_usd: Decimal::from_f64_retain(row.total_cost_usd).unwrap_or(Decimal::ZERO),
        avg_latency_ms: row.avg_latency_ms as u32,
        error_count: row.error_count as u64,
        models: row.models,
        feedback_score: feedback.as_ref().and_then(|f| f.feedback_score),
        feedback_text: feedback.and_then(|f| f.feedback_text),
        fallback_count: row.fallback_count as u64,
        guardrail_count: row.guardrail_count as u64,
    }))
}

/// LLM request in a session
#[derive(Debug, Serialize)]
pub struct SessionRequest {
    pub request_id: String,
    pub trace_id: String,
    pub gen_ai_system: String,
    pub gen_ai_request_model: String,
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub cost_usd: Decimal,
    pub duration_ms: u32,
    pub status_code: String,
    pub timestamp: DateTime<Utc>,
    /// Whether this request used a platform-managed key (`true`) or a BYOK key (`false`).
    pub is_platform_key: bool,
    /// JSON-encoded request messages (if content logging was enabled).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_messages: Option<String>,
    /// Assistant response content (if content logging was enabled).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_content: Option<String>,
    pub fallback_used: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub original_model: String,
    pub retry_count: u32,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub guardrail_violations: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frequency_penalty: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub presence_penalty: Option<f32>,
}

#[derive(Debug, Deserialize)]
pub struct SessionRequestsParams {
    #[serde(default = "crate::api::default_list_limit")]
    pub limit: u32,
    #[serde(default)]
    pub offset: u32,
}

/// Get requests in a session from Postgres.
async fn get_session_requests(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Query(params): Query<SessionRequestsParams>,
) -> Result<Json<Vec<SessionRequest>>> {
    let project_id = crate::api::extract_project_id(&headers)?;
    let limit = params.limit.min(MAX_LIMIT);

    #[derive(Debug, sqlx::FromRow)]
    struct PgRow {
        request_id: String,
        request_messages: String,
        response_content: String,
        gen_ai_request_model: String,
        gen_ai_system: String,
        input_tokens: i32,
        output_tokens: i32,
        cost_usd: f64,
        duration_ms: i32,
        status_code: String,
        timestamp: DateTime<Utc>,
        fallback_used: bool,
        original_model: String,
        retry_count: i32,
        guardrail_violations: Vec<String>,
        temperature: Option<f32>,
        top_p: Option<f32>,
        max_tokens: Option<i32>,
        frequency_penalty: Option<f32>,
        presence_penalty: Option<f32>,
        is_platform_key: bool,
    }

    let rows: Vec<PgRow> = sqlx::query_as(
        r#"
        SELECT request_id, request_messages, response_content,
               gen_ai_request_model, gen_ai_system, input_tokens, output_tokens,
               cost_usd, duration_ms, status_code, timestamp,
               fallback_used, original_model, retry_count, guardrail_violations,
               temperature, top_p, max_tokens, frequency_penalty, presence_penalty,
               is_platform_key
        FROM session_request_content
        WHERE project_id = $1 AND session_id = $2
        ORDER BY timestamp ASC
        LIMIT $3 OFFSET $4
        "#,
    )
    .bind(project_id)
    .bind(&session_id)
    .bind(limit as i64)
    .bind(params.offset as i64)
    .fetch_all(state.db.as_ref())
    .await
    .map_err(|e| AppError::Database(e))?;

    let requests: Vec<SessionRequest> = rows
        .into_iter()
        .map(|r| {
            let request_messages = if r.request_messages.is_empty() {
                None
            } else {
                Some(r.request_messages)
            };
            let response_content = if r.response_content.is_empty() {
                None
            } else {
                Some(r.response_content)
            };
            SessionRequest {
                request_id: r.request_id,
                trace_id: String::new(),
                gen_ai_system: r.gen_ai_system,
                gen_ai_request_model: r.gen_ai_request_model,
                input_tokens: r.input_tokens as u32,
                output_tokens: r.output_tokens as u32,
                cost_usd: Decimal::from_f64_retain(r.cost_usd).unwrap_or(Decimal::ZERO),
                duration_ms: r.duration_ms as u32,
                status_code: r.status_code,
                timestamp: r.timestamp,
                request_messages,
                response_content,
                fallback_used: r.fallback_used,
                original_model: r.original_model,
                retry_count: r.retry_count as u32,
                guardrail_violations: r.guardrail_violations,
                temperature: r.temperature,
                top_p: r.top_p,
                max_tokens: r.max_tokens.map(|v| v as u32),
                frequency_penalty: r.frequency_penalty,
                presence_penalty: r.presence_penalty,
                is_platform_key: r.is_platform_key,
            }
        })
        .collect();

    Ok(Json(requests))
}

/// Feedback submission request
#[derive(Debug, Deserialize)]
pub struct SubmitFeedbackRequest {
    pub score: Option<i32>,
    pub text: Option<String>,
}

/// Submit feedback for a session
async fn submit_session_feedback(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(req): Json<SubmitFeedbackRequest>,
) -> Result<Json<serde_json::Value>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    let has_text = req.text.as_ref().map_or(false, |t| !t.trim().is_empty());
    if req.score.is_none() && !has_text {
        return Err(AppError::Validation(
            "At least one of 'score' or 'text' must be provided".to_string(),
        ));
    }

    if let Some(score) = req.score {
        if !(1..=5).contains(&score) {
            return Err(AppError::Validation(
                "Score must be between 1 and 5".to_string(),
            ));
        }
    }

    sqlx::query(
        r#"
        INSERT INTO llm_sessions_metadata (id, project_id, session_id, feedback_score, feedback_text, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
        ON CONFLICT (project_id, session_id)
        DO UPDATE SET 
            feedback_score = COALESCE($4, llm_sessions_metadata.feedback_score),
            feedback_text = COALESCE($5, llm_sessions_metadata.feedback_text),
            updated_at = NOW()
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(project_id)
    .bind(&session_id)
    .bind(req.score)
    .bind(&req.text)
    .execute(state.db.as_ref())
    .await
    .map_err(|e| AppError::Database(e))?;

    Ok(Json(serde_json::json!({
        "success": true,
        "session_id": session_id
    })))
}

/// Helper: Get feedback for a single session
async fn get_session_feedback(
    state: &Arc<FlowState>,
    project_id: Uuid,
    session_id: &str,
) -> Result<Option<SessionFeedback>> {
    let feedback: Option<SessionFeedback> = sqlx::query_as(
        r#"
        SELECT feedback_score, feedback_text
        FROM llm_sessions_metadata
        WHERE project_id = $1 AND session_id = $2
        "#,
    )
    .bind(project_id)
    .bind(session_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(|e| AppError::Database(e))?;

    Ok(feedback)
}

#[derive(Debug, sqlx::FromRow)]
struct SessionFeedback {
    feedback_score: Option<i32>,
    feedback_text: Option<String>,
}

/// Helper: Get matched profile IDs for multiple sessions
async fn get_session_profile_matches(
    state: &Arc<FlowState>,
    project_id: Uuid,
    session_ids: &[String],
) -> Result<std::collections::HashMap<String, Vec<Uuid>>> {
    if session_ids.is_empty() {
        return Ok(std::collections::HashMap::new());
    }

    #[derive(Debug, sqlx::FromRow)]
    struct MatchRow {
        session_id: String,
        profile_id: Uuid,
    }

    let rows: Vec<MatchRow> = sqlx::query_as(
        "SELECT session_id, profile_id FROM session_profile_matches WHERE project_id = $1 AND session_id = ANY($2)",
    )
    .bind(project_id)
    .bind(session_ids)
    .fetch_all(state.db.as_ref())
    .await
    .map_err(|e| AppError::Database(e))?;

    let mut map: std::collections::HashMap<String, Vec<Uuid>> = std::collections::HashMap::new();
    for row in rows {
        map.entry(row.session_id).or_default().push(row.profile_id);
    }
    Ok(map)
}

/// Helper: Load profile names for a project (from gateway_session_profiles setting)
async fn load_profile_names(
    state: &Arc<FlowState>,
    project_id: Uuid,
) -> Result<std::collections::HashMap<Uuid, String>> {
    let value: Option<String> = sqlx::query_scalar(
        "SELECT value FROM project_settings WHERE project_id = $1 AND key = 'gateway_session_profiles'",
    )
    .bind(project_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(|e| AppError::Database(e))?;

    let mut map = std::collections::HashMap::new();
    if let Some(json) = value {
        if let Ok(profiles) =
            serde_json::from_str::<Vec<crate::api::session_profiles::SessionProfile>>(&json)
        {
            for p in profiles {
                map.insert(p.id, p.name);
            }
        }
    }
    Ok(map)
}

/// Helper: Get feedback for multiple sessions
async fn get_session_feedback_map(
    state: &Arc<FlowState>,
    project_id: Uuid,
    session_ids: &[String],
) -> Result<std::collections::HashMap<String, Option<i32>>> {
    if session_ids.is_empty() {
        return Ok(std::collections::HashMap::new());
    }

    #[derive(Debug, sqlx::FromRow)]
    struct FeedbackRow {
        session_id: String,
        feedback_score: Option<i32>,
    }

    let rows: Vec<FeedbackRow> = sqlx::query_as(
        r#"
        SELECT session_id, feedback_score
        FROM llm_sessions_metadata
        WHERE project_id = $1 AND session_id = ANY($2)
        "#,
    )
    .bind(project_id)
    .bind(session_ids)
    .fetch_all(state.db.as_ref())
    .await
    .map_err(|e| AppError::Database(e))?;

    let map: std::collections::HashMap<String, Option<i32>> = rows
        .into_iter()
        .map(|r| (r.session_id, r.feedback_score))
        .collect();

    Ok(map)
}

// ========================================================================
// Session Replay
// ========================================================================

/// Request body for session replay with modified settings.
#[derive(Debug, Deserialize)]
pub struct ReplayRequest {
    /// Index of the message to fork from (0-based into the session's request list).
    pub fork_from_index: usize,
    /// Model override (e.g. "gpt-4o", "claude-sonnet-4-6").
    pub model: String,
    /// Temperature override.
    #[serde(default)]
    pub temperature: Option<f64>,
    /// Max tokens override.
    #[serde(default)]
    pub max_tokens: Option<u32>,
    /// Top-p override.
    #[serde(default)]
    pub top_p: Option<f64>,
}

/// A single replay step sent as an SSE event.
#[derive(Debug, Serialize)]
struct ReplayStepEvent {
    index: usize,
    model: String,
    response_content: String,
    input_tokens: u32,
    output_tokens: u32,
    duration_ms: u64,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Replay a session from a given fork point with modified LLM parameters.
///
/// Loads the original session requests, re-sends each one from `fork_from_index`
/// onward with the new model/settings, and streams results as SSE events.
/// New responses cascade into subsequent messages' context.
async fn replay_session(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(req): Json<ReplayRequest>,
) -> Result<Sse<impl futures::Stream<Item = std::result::Result<Event, Infallible>>>> {
    use crate::api::session_replay::SessionReplayer;

    let project_id = crate::api::extract_project_id(&headers)?;
    let flow_url = state.internal_urls.flow.clone();

    let replayer = SessionReplayer::new(state.db.as_ref(), &state.agent_http_client, &flow_url);
    let rows = replayer
        .load_requests(project_id, &session_id, None)
        .await
        .map_err(|e| AppError::Database(sqlx::Error::Protocol(e.to_string())))?;

    if rows.is_empty() {
        return Err(AppError::NotFound(format!(
            "No request content found for session: {}",
            session_id
        )));
    }

    if req.fork_from_index >= rows.len() {
        return Err(AppError::Validation(format!(
            "fork_from_index {} exceeds session length {}",
            req.fork_from_index,
            rows.len()
        )));
    }

    let http_client = state.agent_http_client.clone();
    let model_str = req.model.clone();
    let temperature = req.temperature;
    let max_tokens = req.max_tokens;
    let top_p = req.top_p;
    let fork_from = req.fork_from_index;

    let stream = futures::stream::unfold(
        (rows, fork_from, Vec::<(String, String)>::new()),
        move |(rows, current_idx, mut overridden_responses)| {
            let http_client = http_client.clone();
            let flow_url = flow_url.clone();
            let model_str = model_str.clone();

            async move {
                if current_idx >= rows.len() {
                    return None;
                }

                let original = &rows[current_idx];

                let mut prepared = match SessionReplayer::prepare_messages(original, None) {
                    Ok(p) => p,
                    Err(e) => {
                        let event = ReplayStepEvent {
                            index: current_idx,
                            model: model_str.clone(),
                            response_content: String::new(),
                            input_tokens: 0,
                            output_tokens: 0,
                            duration_ms: 0,
                            status: "error".to_string(),
                            error: Some(format!("Failed to parse request messages: {}", e)),
                        };
                        let sse = Event::default()
                            .event("replay_step")
                            .json_data(&event)
                            .unwrap_or_else(|_| Event::default().data("{}"));
                        return Some((Ok(sse), (rows, current_idx + 1, overridden_responses)));
                    }
                };

                // Cascade: replace earlier assistant responses with replayed ones
                for (orig_resp, new_resp) in &overridden_responses {
                    for msg in prepared.messages.iter_mut() {
                        if msg.role == MessageRole::Assistant {
                            if let Some(MessageContent::Text(ref text)) = msg.content {
                                if text == orig_resp {
                                    msg.content = Some(MessageContent::Text(new_resp.clone()));
                                }
                            }
                        }
                    }
                }

                let result = SessionReplayer::execute_gateway_request(
                    &http_client,
                    &flow_url,
                    project_id,
                    prepared,
                    &model_str,
                    temperature.map(|t| t as f32),
                    max_tokens,
                    top_p.map(|t| t as f32),
                    None,
                )
                .await;

                let event = match result {
                    Ok(replayed) => {
                        overridden_responses
                            .push((original.response_content.clone(), replayed.content.clone()));

                        ReplayStepEvent {
                            index: current_idx,
                            model: replayed.model,
                            response_content: replayed.content,
                            input_tokens: replayed.prompt_tokens,
                            output_tokens: replayed.completion_tokens,
                            duration_ms: replayed.latency_ms,
                            status: "ok".to_string(),
                            error: None,
                        }
                    }
                    Err(e) => {
                        overridden_responses
                            .push((original.response_content.clone(), String::new()));

                        ReplayStepEvent {
                            index: current_idx,
                            model: model_str.clone(),
                            response_content: String::new(),
                            input_tokens: 0,
                            output_tokens: 0,
                            duration_ms: 0,
                            status: "error".to_string(),
                            error: Some(e.to_string()),
                        }
                    }
                };

                let sse = Event::default()
                    .event("replay_step")
                    .json_data(&event)
                    .unwrap_or_else(|_| Event::default().data("{}"));

                Some((Ok(sse), (rows, current_idx + 1, overridden_responses)))
            }
        },
    );

    let done_stream = futures::stream::once(async {
        Ok::<_, Infallible>(Event::default().event("replay_done").data("{}"))
    });

    let combined = stream.chain(done_stream);
    Ok(Sse::new(combined))
}
