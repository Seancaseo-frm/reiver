use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};

#[derive(Debug, Deserialize, JsonSchema)]
pub struct WebSearchInput {
    /// The search query
    pub query: String,
    /// Maximum number of results to return (default 5, max 10)
    pub max_results: Option<u8>,
}

#[derive(Debug, Serialize)]
pub struct WebSearchOutput {
    pub results: Vec<SearchResult>,
}

#[derive(Debug, Serialize)]
pub struct SearchResult {
    pub title: String,
    pub url: String,
    pub content: String,
}

#[derive(Debug, Deserialize)]
struct TavilyResponse {
    results: Vec<TavilyResult>,
}

#[derive(Debug, Deserialize)]
struct TavilyResult {
    title: String,
    url: String,
    content: String,
}

pub struct WebSearch;

#[async_trait]
impl PlatformAction for WebSearch {
    type Input = WebSearchInput;
    type Output = WebSearchOutput;

    fn name(&self) -> &'static str {
        "web_search"
    }

    fn description(&self) -> &'static str {
        "Search the web for information. Returns titles, URLs, and content snippets. \
         Use for looking up current pricing, documentation, or any real-time information."
    }

    fn required_scope(&self) -> String {
        "internal:write".into()
    }

    async fn execute(
        &self,
        _ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let api_key = std::env::var("TAVILY_API_KEY")
            .map_err(|_| anyhow::anyhow!("TAVILY_API_KEY not set"))?;

        let max_results = input.max_results.unwrap_or(5).min(10);

        let client = reqwest::Client::new();
        let resp = client
            .post("https://api.tavily.com/search")
            .json(&serde_json::json!({
                "api_key": api_key,
                "query": input.query,
                "max_results": max_results,
                "include_answer": false,
            }))
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Tavily API error {status}: {body}");
        }

        let tavily: TavilyResponse = resp.json().await?;

        let results = tavily
            .results
            .into_iter()
            .map(|r| SearchResult {
                title: r.title,
                url: r.url,
                content: r.content,
            })
            .collect();

        Ok(WebSearchOutput { results })
    }
}

pub fn register(registry: &mut crate::registry::ActionRegistry) {
    registry.register(WebSearch);
}
