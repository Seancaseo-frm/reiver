//! LLM-as-judge quality evaluator, shared between the playground and the
//! background prompt-config quality scoring.
//!
//! The core function [`run_llm_judge`] is always called inside a `tokio::spawn`
//! so it never blocks the request path.

use std::sync::Arc;

use serde::Deserialize;
use uuid::Uuid;

use crate::app_state::FlowState;
use crate::gateway::types::{
    ChatCompletionRequest, ChatMessage, MessageContent, MessageRole, ResponseFormat,
    ResponseFormatType,
};
use reiver_core::db::DbPool;

/// Default max tokens for LLM-as-judge evaluation responses.
const DEFAULT_EVAL_MAX_TOKENS: u32 = 200;

/// Prompt config name for the quality judge stored in the platform prompt hub.
const JUDGE_PROMPT_CONFIG: &str = "moodeng-quality-judge";

/// Individual dimension scores from the LLM-as-judge.
#[derive(Debug, Clone)]
pub struct JudgeScores {
    pub relevance: f64,
    pub coherence: f64,
    pub helpfulness: f64,
    pub summary: String,
    /// Average of the three dimension scores.
    pub average: f64,
}

/// Run the LLM-as-judge evaluator and return scores for a response.
///
/// Accepts the user-facing messages and the response text. The judge prompt
/// is built from the last user message(s) for context.
///
/// Returns `None` on any failure (network error, parse error, missing provider
/// key) — callers should treat `None` as "could not evaluate" and skip logging
/// rather than surfacing an error.
///
/// This function is intended to be called inside `tokio::spawn`. It has no
/// timeout — since the client already has their response, latency here does
/// not matter.
#[tracing::instrument(
    name = "gateway.llm_judge",
    skip(state, response_text),
    fields(project_id = %project_id)
)]
pub(crate) async fn run_llm_judge(
    state: &FlowState,
    project_id: Uuid,
    user_query: &str,
    response_text: &str,
) -> Option<JudgeScores> {
    let moodeng = crate::moodeng::MoodengClient::new(state, project_id);

    let user_message = format!(
        "User Query:\n{}\n\nAI Response:\n{}",
        user_query, response_text
    );

    let eval_request = ChatCompletionRequest {
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
        max_tokens: Some(DEFAULT_EVAL_MAX_TOKENS),
        stream: Some(false),
        prompt_config: Some(JUDGE_PROMPT_CONFIG.to_string()),
        response_format: Some(ResponseFormat {
            format_type: ResponseFormatType::JsonObject,
        }),
        ..Default::default()
    };

    let gw_result = moodeng
        .call_llm(&eval_request, None)
        .await
        .map_err(|e| {
            tracing::warn!(error = %e, "LLM-as-judge gateway call failed");
            e
        })
        .ok()?;

    let eval_content = gw_result.content;

    #[derive(Deserialize)]
    struct EvalJson {
        relevance: f64,
        coherence: f64,
        helpfulness: f64,
        summary: String,
    }

    let eval: EvalJson = serde_json::from_str(eval_content.trim())
        .map_err(|e| {
            tracing::warn!(
                project_id = %project_id,
                error = %e,
                raw_content = %eval_content,
                "LLM-as-judge: failed to parse evaluation JSON"
            );
        })
        .ok()?;

    let average = (eval.relevance + eval.coherence + eval.helpfulness) / 3.0;

    Some(JudgeScores {
        relevance: eval.relevance,
        coherence: eval.coherence,
        helpfulness: eval.helpfulness,
        summary: eval.summary,
        average,
    })
}

/// Persist judge scores to the `llm_evaluation_scores` table.
///
/// Writes four rows (relevance, coherence, helpfulness, average) for the given
/// request. Intended to be called from a `tokio::spawn` context — failures are
/// logged but do not propagate.
pub(crate) async fn persist_judge_scores(
    db: &Arc<DbPool>,
    project_id: Uuid,
    request_id: &str,
    scores: &JudgeScores,
) {
    for (name, value) in [
        ("relevance", scores.relevance),
        ("coherence", scores.coherence),
        ("helpfulness", scores.helpfulness),
        ("average", scores.average),
    ] {
        let id = Uuid::new_v4();
        if let Err(e) = sqlx::query(
            r#"INSERT INTO llm_evaluation_scores
               (id, project_id, request_id, score_name, score_value,
                score_type, reason, evaluator_type, evaluator_id, created_at)
               VALUES ($1, $2, $3, $4, $5, 'number', NULL, 'llm_judge', 'prompt_quality', NOW())"#,
        )
        .bind(id)
        .bind(project_id)
        .bind(request_id)
        .bind(name)
        .bind(value * 100.0)
        .execute(db.as_ref())
        .await
        {
            tracing::warn!(
                request_id = %request_id,
                score_name = %name,
                error = %e,
                "Failed to persist LLM-as-judge score"
            );
        }
    }

    // Persist the judge's text summary for display in the rollout UI.
    let summary_id = Uuid::new_v4();
    if let Err(e) = sqlx::query(
        r#"INSERT INTO llm_evaluation_scores
           (id, project_id, request_id, score_name, score_value,
            score_type, reason, evaluator_type, evaluator_id, created_at)
           VALUES ($1, $2, $3, 'summary', $4, 'text', $5, 'llm_judge', 'prompt_quality', NOW())"#,
    )
    .bind(summary_id)
    .bind(project_id)
    .bind(request_id)
    .bind(scores.average * 100.0)
    .bind(&scores.summary)
    .execute(db.as_ref())
    .await
    {
        tracing::warn!(
            request_id = %request_id,
            error = %e,
            "Failed to persist LLM-as-judge summary"
        );
    }
}

