use async_trait::async_trait;
use schemars::JsonSchema;
use serde::Deserialize;

use crate::action::{ActionContext, PlatformAction};
use crate::actions::flow::search::{SearchLlmRequests, SearchLlmRequestsInput};
use crate::actions::internal::web_search::{WebSearch, WebSearchInput};
use crate::actions::knowledge_base::{SearchKnowledgeBase, SearchKnowledgeBaseInput};
use crate::actions::watch::logs::{SearchLogs, SearchLogsInput};

macro_rules! dispatch {
    ($ctx:expr, $scope:literal, $action:expr, $p:expr) => {{
        super::require_scope($ctx, $scope)?;
        Ok(serde_json::to_value($action.execute($ctx, $p).await?)?)
    }};
}

/// Discriminated input for the unified `search` tool.
#[derive(Deserialize, JsonSchema)]
#[serde(tag = "source")]
pub enum SearchInput {
    /// Search LLM gateway requests by text matching on prompts and completions
    #[serde(rename = "llm_requests")]
    LlmRequests(SearchLlmRequestsInput),
    /// Search ingested logs by text query and severity
    #[serde(rename = "logs")]
    Logs(SearchLogsInput),
    /// Search the web for real-time information
    #[serde(rename = "web")]
    Web(WebSearchInput),
    /// Search the platform knowledge base for known patterns, common issues, and operational quirks
    #[serde(rename = "knowledge_base")]
    KnowledgeBase(SearchKnowledgeBaseInput),
}

pub struct SearchTool;

#[async_trait]
impl PlatformAction for SearchTool {
    type Input = SearchInput;
    type Output = serde_json::Value;

    fn name(&self) -> &'static str {
        "search"
    }
    fn description(&self) -> &'static str {
        "Search across different data sources. Set 'source' to: 'llm_requests' (text search \
         over LLM prompts/completions — supports filters: model, user_id, session_id, \
         start_time, end_time), 'logs' (search logs — supports: level, service, trace_id, \
         time_range, start_time, end_time, query, attributes for key-value filtering), \
         'web' (web search for real-time information), or 'knowledge_base' (semantic search \
         of the platform knowledge base for known patterns, common issues, and operational \
         quirks — provide a natural language query, optionally filter by category). \
         Returns matching results with metadata. \
         Use list trace_attribute_keys/log_attribute_keys to discover available attribute filters."
    }
    fn required_scope(&self) -> String {
        "project:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        match input {
            SearchInput::LlmRequests(p) => dispatch!(ctx, "llm:read", SearchLlmRequests, p),
            SearchInput::Logs(p) => dispatch!(ctx, "observability:read", SearchLogs, p),
            SearchInput::Web(p) => dispatch!(ctx, "internal:write", WebSearch, p),
            SearchInput::KnowledgeBase(p) => {
                dispatch!(ctx, "project:read", SearchKnowledgeBase, p)
            }
        }
    }
}
