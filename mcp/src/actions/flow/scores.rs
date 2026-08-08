use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::registry::ActionRegistry;

// ── Submit LLM Score ────────────────────────────────────────────────

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ScoreType {
    Number,
    Boolean,
}

impl Default for ScoreType {
    fn default() -> Self {
        Self::Number
    }
}

#[derive(Deserialize, JsonSchema)]
pub struct SubmitLlmScoreInput {
    /// LLM request ID to attach the score to
    pub request_id: String,
    /// Score name (e.g. "relevance", "faithfulness", "helpfulness")
    pub score_name: String,
    /// Score value: 0-100 for numeric scores, 0 or 1 for boolean scores
    #[schemars(range(min = 0.0, max = 100.0))]
    pub score_value: f64,
    /// Score type: "number" (0-100) or "boolean" (0/1). Defaults to "number".
    #[serde(default)]
    pub score_type: Option<ScoreType>,
    /// Optional explanation of the score
    pub reason: Option<String>,
    /// Who/what produced the score: "human", "llm", "heuristic"
    pub evaluator_type: Option<String>,
    /// Identifier for the evaluator (e.g. model name or user ID)
    pub evaluator_id: Option<String>,
}

#[derive(Serialize)]
pub struct SubmitLlmScoreOutput {
    pub score: serde_json::Value,
}

pub struct SubmitLlmScore;

#[async_trait]
impl PlatformAction for SubmitLlmScore {
    type Input = SubmitLlmScoreInput;
    type Output = SubmitLlmScoreOutput;

    fn name(&self) -> &'static str {
        "submit_llm_score"
    }
    fn description(&self) -> &'static str {
        "Submit a quality score for an LLM request. Scores are used for quality monitoring, \
         rollout comparisons, and guardrail evaluation. Numeric scores: 0-100, boolean: 0 or 1."
    }
    fn required_scope(&self) -> String {
        "llm:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let mut body = serde_json::json!({
            "project_id": ctx.project_id,
            "request_id": input.request_id,
            "score_name": input.score_name,
            "score_value": input.score_value,
        });
        let obj = body.as_object_mut().unwrap();
        if let Some(st) = input.score_type {
            obj.insert("score_type".into(), serde_json::to_value(st)?);
        }
        if let Some(r) = input.reason {
            obj.insert("reason".into(), serde_json::Value::String(r));
        }
        if let Some(et) = input.evaluator_type {
            obj.insert("evaluator_type".into(), serde_json::Value::String(et));
        }
        if let Some(ei) = input.evaluator_id {
            obj.insert("evaluator_id".into(), serde_json::Value::String(ei));
        }
        let resp = ctx.http.flow_post("/api/llm/scores", &body).await?;
        let score = resp.json().await?;
        Ok(SubmitLlmScoreOutput { score })
    }
}

// ── List LLM Scores ────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListLlmScoresInput {
    /// Filter by score name
    pub score_name: Option<String>,
    /// Filter by evaluator type
    pub evaluator_type: Option<String>,
    /// Maximum number of results (default: 50)
    #[schemars(range(min = 1, max = 1000))]
    pub limit: Option<u32>,
}

#[derive(Serialize)]
pub struct ListLlmScoresOutput {
    pub scores: serde_json::Value,
}

pub struct ListLlmScores;

#[async_trait]
impl PlatformAction for ListLlmScores {
    type Input = ListLlmScoresInput;
    type Output = ListLlmScoresOutput;

    fn name(&self) -> &'static str {
        "list_llm_scores"
    }
    fn description(&self) -> &'static str {
        "List quality scores across LLM requests. Optionally filter by score name or \
         evaluator type. Returns score values with associated request IDs and timestamps."
    }
    fn required_scope(&self) -> String {
        "llm:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let mut path = format!("/api/llm/scores?project_id={}", ctx.project_id);
        if let Some(ref n) = input.score_name {
            path.push_str(&format!("&score_name={}", urlencoding::encode(n)));
        }
        if let Some(ref et) = input.evaluator_type {
            path.push_str(&format!("&evaluator_type={}", urlencoding::encode(et)));
        }
        if let Some(l) = input.limit {
            path.push_str(&format!("&limit={l}"));
        }
        let resp = ctx.http.flow_get(&path).await?;
        let scores = resp.json().await?;
        Ok(ListLlmScoresOutput { scores })
    }
}

// ── Get Request Scores ──────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetRequestScoresInput {
    /// LLM request ID
    pub request_id: String,
}

#[derive(Serialize)]
pub struct GetRequestScoresOutput {
    pub scores: serde_json::Value,
}

pub struct GetRequestScores;

#[async_trait]
impl PlatformAction for GetRequestScores {
    type Input = GetRequestScoresInput;
    type Output = GetRequestScoresOutput;

    fn name(&self) -> &'static str {
        "get_request_scores"
    }
    fn description(&self) -> &'static str {
        "Get all quality scores for a specific LLM request. Returns each score's name, \
         value, type, reason, and evaluator."
    }
    fn required_scope(&self) -> String {
        "llm:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let path = format!(
            "/api/llm/scores/request/{}?project_id={}",
            input.request_id, ctx.project_id
        );
        let resp = ctx.http.flow_get(&path).await?;
        let scores = resp.json().await?;
        Ok(GetRequestScoresOutput { scores })
    }
}

// ── Registration ─────────────────────────────────────────────────────

pub fn register(registry: &mut ActionRegistry) {
    registry.register(SubmitLlmScore);
    registry.register(ListLlmScores);
    registry.register(GetRequestScores);
}
