use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::actions::types::MessageRole;
use crate::registry::ActionRegistry;

// ── Run Playground ──────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct PlaygroundMessage {
    /// Message role
    pub role: MessageRole,
    /// Message content
    pub content: String,
}

#[derive(Deserialize, JsonSchema)]
pub struct RunPlaygroundInput {
    /// Exact model ID from `list` resource `model_catalog`. Omit this to use the
    /// project's Reiver-owned routing defaults.
    /// When `prompt_config` is set, the model from the prompt version overrides this.
    pub model: Option<String>,
    /// Conversation messages
    pub messages: Vec<PlaygroundMessage>,
    /// Sampling temperature, 0.0-1.0 (default: 1.0)
    #[schemars(range(min = 0.0, max = 1.0))]
    pub temperature: Option<f64>,
    /// Maximum tokens to generate (default: model's maximum)
    #[schemars(range(min = 1, max = 128000))]
    pub max_tokens: Option<u32>,
    /// Managed prompt config name from the Prompt Hub. When set, the active version's
    /// system_prompt (with variables compiled), model, temperature, and max_tokens are
    /// applied automatically. Use with `prompt_variables` to fill template placeholders.
    pub prompt_config: Option<String>,
    /// Runtime template variables for Handlebars `{{name}}` placeholders in the prompt's
    /// system_prompt. Only used when `prompt_config` is set.
    pub prompt_variables: Option<std::collections::HashMap<String, serde_json::Value>>,
}

#[derive(Serialize)]
pub struct RunPlaygroundOutput {
    pub result: serde_json::Value,
}

pub struct RunPlayground;

fn playground_model_or_auto(model: Option<String>) -> String {
    model
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "auto".to_string())
}

#[async_trait]
impl PlatformAction for RunPlayground {
    type Input = RunPlaygroundInput;
    type Output = RunPlaygroundOutput;

    fn name(&self) -> &'static str {
        "run_playground"
    }
    fn description(&self) -> &'static str {
        "Send a prompt through the LLM gateway and return the model's response. \
         Routes through all configured guardrails, provider fallback, and cost tracking. \
         Returns the completion text, token usage, latency, and cost. \
         Omit `model` to exercise the project's Reiver-owned auto routing and fallback chain; \
         use an exact live catalogue ID only for an explicitly pinned-model test. \
         Supports managed prompts: set `prompt_config` to a Prompt Hub config name and \
         `prompt_variables` to fill template placeholders — the system prompt, model, and \
         settings are resolved from the active version automatically."
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
        let model = playground_model_or_auto(input.model);
        let messages: Vec<serde_json::Value> = input
            .messages
            .into_iter()
            .map(|m| serde_json::json!({ "role": m.role, "content": m.content }))
            .collect();
        let body = serde_json::json!({
            "project_id": pid,
            "model": model,
            "messages": messages,
            "temperature": input.temperature,
            "max_tokens": input.max_tokens,
            "prompt_config": input.prompt_config,
            "prompt_variables": input.prompt_variables,
        });
        let resp = ctx.http.flow_post("/api/llm/playground", &body).await?;
        let result = resp.json().await?;
        Ok(RunPlaygroundOutput { result })
    }
}

// ── Compare Models ──────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct CompareModelsInput {
    /// Conversation messages to send to each model
    pub messages: Vec<PlaygroundMessage>,
    /// Exact model IDs returned by `list` resource `model_catalog`.
    pub compare_models: Vec<String>,
}

#[derive(Serialize)]
pub struct CompareModelsOutput {
    pub result: serde_json::Value,
}

pub struct CompareModels;

#[async_trait]
impl PlatformAction for CompareModels {
    type Input = CompareModelsInput;
    type Output = CompareModelsOutput;

    fn name(&self) -> &'static str {
        "compare_models"
    }
    fn description(&self) -> &'static str {
        "Send the same prompt to multiple models and return all responses side-by-side. \
         Use only IDs returned by the live `model_catalog` list resource. Useful for evaluating \
         model quality, latency, and cost before choosing a project routing policy."
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
        let messages: Vec<serde_json::Value> = input
            .messages
            .into_iter()
            .map(|m| serde_json::json!({ "role": m.role, "content": m.content }))
            .collect();
        let body = serde_json::json!({
            "project_id": pid,
            "messages": messages,
            "compare_models": input.compare_models,
        });
        let resp = ctx
            .http
            .flow_post("/api/llm/playground/compare", &body)
            .await?;
        let result = resp.json().await?;
        Ok(CompareModelsOutput { result })
    }
}

// ── Registration ─────────────────────────────────────────────────────

pub fn register(registry: &mut ActionRegistry) {
    registry.register(RunPlayground);
    registry.register(CompareModels);
}

#[cfg(test)]
mod tests {
    use super::playground_model_or_auto;

    #[test]
    fn omitted_or_empty_model_uses_project_auto_routing() {
        assert_eq!(playground_model_or_auto(None), "auto");
        assert_eq!(playground_model_or_auto(Some(String::new())), "auto");
        assert_eq!(playground_model_or_auto(Some("  ".into())), "auto");
    }

    #[test]
    fn explicit_live_model_id_is_preserved() {
        assert_eq!(
            playground_model_or_auto(Some("live-model-id".into())),
            "live-model-id"
        );
    }
}
