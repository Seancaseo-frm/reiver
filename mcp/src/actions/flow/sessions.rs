use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::registry::ActionRegistry;

// ── List LLM Sessions ───────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListLlmSessionsInput {
    /// Maximum number of sessions to return (default: 50)
    #[schemars(range(min = 1, max = 1000))]
    pub limit: Option<u32>,
    /// Pagination offset (0-based)
    pub offset: Option<u32>,
    /// Filter by session name pattern (substring match)
    pub name_pattern: Option<String>,
    /// Filter by session profile ID
    pub profile_id: Option<String>,
}

#[derive(Serialize)]
pub struct ListLlmSessionsOutput {
    pub sessions: serde_json::Value,
}

pub struct ListLlmSessions;

#[async_trait]
impl PlatformAction for ListLlmSessions {
    type Input = ListLlmSessionsInput;
    type Output = ListLlmSessionsOutput;

    fn name(&self) -> &'static str {
        "list_llm_sessions"
    }
    fn description(&self) -> &'static str {
        "List LLM sessions for the current project. A session groups related LLM requests \
         sharing a session_id (typically one conversation). Filter by name_pattern (substring \
         search) or profile_id (session profile). Supports pagination with limit and offset. \
         Returns session summaries with request counts, total tokens, and timespan. \
         Use get_llm_session for details."
    }
    fn required_scope(&self) -> String {
        "llm:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let mut path = format!("/api/llm/sessions?project_id={pid}");
        if let Some(limit) = input.limit {
            path.push_str(&format!("&limit={limit}"));
        }
        if let Some(offset) = input.offset {
            path.push_str(&format!("&offset={offset}"));
        }
        if let Some(ref s) = input.name_pattern {
            path.push_str(&format!("&name_pattern={}", urlencoding::encode(s)));
        }
        if let Some(ref s) = input.profile_id {
            path.push_str(&format!("&profile_id={}", urlencoding::encode(s)));
        }
        let resp = ctx.http.flow_get(&path).await?;
        let sessions = resp.json().await?;
        Ok(ListLlmSessionsOutput { sessions })
    }
}

// ── Get LLM Session ─────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetLlmSessionInput {
    /// The session ID
    pub session_id: String,
}

#[derive(Serialize)]
pub struct GetLlmSessionOutput {
    pub session: serde_json::Value,
}

pub struct GetLlmSession;

#[async_trait]
impl PlatformAction for GetLlmSession {
    type Input = GetLlmSessionInput;
    type Output = GetLlmSessionOutput;

    fn name(&self) -> &'static str {
        "get_llm_session"
    }
    fn description(&self) -> &'static str {
        "Get metadata and aggregate stats for a specific LLM session (request count, total \
         tokens, cost, time range). Use get_session_requests to see individual LLM calls."
    }
    fn required_scope(&self) -> String {
        "llm:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let resp = ctx
            .http
            .flow_get(&format!(
                "/api/llm/sessions/{}?project_id={pid}",
                input.session_id
            ))
            .await?;
        let session = resp.json().await?;
        Ok(GetLlmSessionOutput { session })
    }
}

// ── Get Session Requests ────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetSessionRequestsInput {
    /// The session ID
    pub session_id: String,
}

#[derive(Serialize)]
pub struct GetSessionRequestsOutput {
    pub requests: serde_json::Value,
}

pub struct GetSessionRequests;

#[async_trait]
impl PlatformAction for GetSessionRequests {
    type Input = GetSessionRequestsInput;
    type Output = GetSessionRequestsOutput;

    fn name(&self) -> &'static str {
        "get_session_requests"
    }
    fn description(&self) -> &'static str {
        "Get the individual LLM API calls within a session, ordered chronologically. \
         Each request includes model, prompt, completion, tokens, latency, and cost."
    }
    fn required_scope(&self) -> String {
        "llm:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let resp = ctx
            .http
            .flow_get(&format!(
                "/api/llm/sessions/{}/requests?project_id={pid}",
                input.session_id
            ))
            .await?;
        let requests = resp.json().await?;
        Ok(GetSessionRequestsOutput { requests })
    }
}

// ── Submit Session Feedback ─────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct SubmitSessionFeedbackInput {
    /// The session ID to submit feedback for
    pub session_id: String,
    /// Feedback score: 1 for thumbs up, -1 for thumbs down, null to clear
    pub score: Option<i8>,
}

#[derive(Serialize)]
pub struct SubmitSessionFeedbackOutput {
    pub success: bool,
}

pub struct SubmitSessionFeedback;

#[async_trait]
impl PlatformAction for SubmitSessionFeedback {
    type Input = SubmitSessionFeedbackInput;
    type Output = SubmitSessionFeedbackOutput;

    fn name(&self) -> &'static str {
        "submit_session_feedback"
    }
    fn description(&self) -> &'static str {
        "Submit thumbs-up (1) or thumbs-down (-1) feedback for an LLM session. \
         Pass null score to clear existing feedback."
    }
    fn required_scope(&self) -> String {
        "llm:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let body = serde_json::json!({ "score": input.score });
        ctx.http
            .flow_post(
                &format!(
                    "/api/llm/sessions/{}/feedback?project_id={pid}",
                    input.session_id
                ),
                &body,
            )
            .await?;
        Ok(SubmitSessionFeedbackOutput { success: true })
    }
}

// ── Replay Session (Prompt Compiler) ───────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ReplaySessionInput {
    /// The session ID to replay
    pub session_id: String,
    /// Candidate system prompt text to test
    pub candidate_prompt: String,
    /// Model to use for replay (optional, uses session's original model if omitted)
    pub candidate_model: Option<String>,
    /// Temperature to use for replay (optional, uses 0.5 if omitted)
    pub candidate_temperature: Option<f64>,
}

#[derive(Serialize)]
pub struct ReplaySessionOutput {
    pub result: serde_json::Value,
}

pub struct ReplaySession;

#[async_trait]
impl PlatformAction for ReplaySession {
    type Input = ReplaySessionInput;
    type Output = ReplaySessionOutput;

    fn name(&self) -> &'static str {
        "replay_session"
    }
    fn description(&self) -> &'static str {
        "Replay a saved session against a candidate prompt. Sends the session's original \
         requests through the playground with the candidate system prompt, then runs \
         LLM-as-judge scoring. Returns per-request scores, total cost, latency, and \
         error count. Use to compare candidate prompts against the baseline."
    }
    fn required_scope(&self) -> String {
        "internal:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let body = serde_json::json!({
            "project_id": pid,
            "session_id": input.session_id,
            "candidate_prompt": input.candidate_prompt,
            "candidate_model": input.candidate_model,
            "candidate_temperature": input.candidate_temperature.unwrap_or(0.5),
        });
        let resp = ctx
            .http
            .flow_post("/api/internal/prompt-compiler/replay-session", &body)
            .await?;
        let result = resp.json().await?;
        Ok(ReplaySessionOutput { result })
    }
}

// ── End Session ─────────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct EndSessionInput {
    /// The session ID to mark as ended
    pub session_id: String,
}

#[derive(Serialize)]
pub struct EndSessionOutput {
    pub session_id: String,
    pub status: String,
}

pub struct EndSession;

#[async_trait]
impl PlatformAction for EndSession {
    type Input = EndSessionInput;
    type Output = EndSessionOutput;

    fn name(&self) -> &'static str {
        "end_session"
    }
    fn description(&self) -> &'static str {
        "Mark an LLM session as ended, scheduling evaluation after an approximately \
         30-second ingestion buffer instead of waiting for idle discovery. The session will be \
         classified, matched against session profiles, and persisted. \
         Safe to call multiple times (idempotent). Returns 'evaluation_scheduled' \
         on first call, 'already_enqueued' on subsequent calls. A successful call \
         proves scheduling, not completed evaluation; verify that the session becomes queryable."
    }
    fn required_scope(&self) -> String {
        "llm:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let body = serde_json::json!({});
        let resp = ctx
            .http
            .flow_post(
                &format!("/api/gateway/v1/sessions/{}/end", input.session_id),
                &body,
            )
            .await?;
        let result: serde_json::Value = resp.json().await?;
        Ok(EndSessionOutput {
            session_id: result["session_id"]
                .as_str()
                .unwrap_or(&input.session_id)
                .to_string(),
            status: result["status"]
                .as_str()
                .unwrap_or("evaluation_scheduled")
                .to_string(),
        })
    }
}

// ── Registration ─────────────────────────────────────────────────────

pub fn register(registry: &mut ActionRegistry) {
    registry.register(ListLlmSessions);
    registry.register(GetLlmSession);
    registry.register(GetSessionRequests);
    registry.register(SubmitSessionFeedback);
    registry.register(ReplaySession);
    registry.register(EndSession);
}
