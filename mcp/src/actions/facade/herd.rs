use async_trait::async_trait;
use schemars::JsonSchema;
use serde::Deserialize;

use crate::action::{ActionContext, PlatformAction};

#[derive(Deserialize, JsonSchema)]
pub struct RegisterAgentInput {
    /// Display name for the agent
    pub name: String,
    /// Optional description of what this agent does
    pub description: Option<String>,
    /// The A2A endpoint URL where this agent receives messages
    pub endpoint_url: String,
    /// Visibility: "private" (project only), "org" (organization), or "public"
    pub visibility: Option<String>,
}

pub struct RegisterAgent;

#[async_trait]
impl PlatformAction for RegisterAgent {
    type Input = RegisterAgentInput;
    type Output = serde_json::Value;

    fn name(&self) -> &'static str {
        "register_a2a_agent"
    }
    fn description(&self) -> &'static str {
        "Register an A2A agent in the Herd registry."
    }
    fn required_scope(&self) -> String {
        "project:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        if ctx.http.herd_url().is_empty() {
            anyhow::bail!("Herd service URL not configured");
        }
        let body = serde_json::json!({
            "name": input.name,
            "description": input.description,
            "endpointUrl": input.endpoint_url,
            "visibility": input.visibility.unwrap_or_else(|| "org".to_string()),
        });
        let resp = ctx.http.herd_post("/api/herd/agents", &body).await?;
        Ok(resp.json::<serde_json::Value>().await?)
    }
}
