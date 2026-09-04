use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::registry::ActionRegistry;

// ── Search LLM Requests ─────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct SearchLlmRequestsInput {
    /// Text search query across prompts and completions (required, non-empty)
    pub query: String,
    /// Maximum number of results (default: 10)
    #[schemars(range(min = 1, max = 100))]
    pub limit: Option<u32>,
    /// Filter by an actual model ID observed in gateway data.
    pub model: Option<String>,
    /// Filter by user ID
    pub user_id: Option<String>,
    /// Filter by session ID
    pub session_id: Option<String>,
    /// Start of time range (ISO 8601 timestamp)
    pub start_time: Option<String>,
    /// End of time range (ISO 8601 timestamp)
    pub end_time: Option<String>,
}

#[derive(Serialize)]
pub struct SearchLlmRequestsOutput {
    pub results: serde_json::Value,
}

pub struct SearchLlmRequests;

#[async_trait]
impl PlatformAction for SearchLlmRequests {
    type Input = SearchLlmRequestsInput;
    type Output = SearchLlmRequestsOutput;

    fn name(&self) -> &'static str {
        "search_llm_requests"
    }
    fn description(&self) -> &'static str {
        "Text search across LLM gateway requests. Finds requests where prompts or \
         completions contain the query text. Useful for finding examples, \
         debugging prompt issues, or auditing responses."
    }
    fn required_scope(&self) -> String {
        "llm:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let mut body = serde_json::json!({
            "project_id": ctx.project_id,
            "query": input.query,
        });
        let obj = body
            .as_object_mut()
            .expect("json! macro always returns an object");
        if let Some(l) = input.limit {
            obj.insert("limit".into(), serde_json::json!(l));
        }
        if let Some(m) = input.model {
            obj.insert("model".into(), serde_json::Value::String(m));
        }
        if let Some(u) = input.user_id {
            obj.insert("user_id".into(), serde_json::Value::String(u));
        }
        if let Some(s) = input.session_id {
            obj.insert("session_id".into(), serde_json::Value::String(s));
        }
        if let Some(st) = input.start_time {
            obj.insert("start_time".into(), serde_json::Value::String(st));
        }
        if let Some(et) = input.end_time {
            obj.insert("end_time".into(), serde_json::Value::String(et));
        }
        let resp = ctx.http.flow_post("/api/llm/search", &body).await?;
        let results = resp.json().await?;
        Ok(SearchLlmRequestsOutput { results })
    }
}

// ── Registration ─────────────────────────────────────────────────────

pub fn register(registry: &mut ActionRegistry) {
    registry.register(SearchLlmRequests);
}
