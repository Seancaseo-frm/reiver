use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::registry::ActionRegistry;

// ── Get LLM Overview ────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetLlmOverviewInput {}

#[derive(Serialize)]
pub struct GetLlmOverviewOutput {
    pub overview: serde_json::Value,
}

pub struct GetLlmOverview;

#[async_trait]
impl PlatformAction for GetLlmOverview {
    type Input = GetLlmOverviewInput;
    type Output = GetLlmOverviewOutput;

    fn name(&self) -> &'static str {
        "get_llm_overview"
    }
    fn description(&self) -> &'static str {
        "Get a high-level overview of LLM gateway usage for the project. Returns total requests, \
         total tokens, total cost, active models, and error rate for the current billing period."
    }
    fn required_scope(&self) -> String {
        "llm:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        _input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let resp = ctx
            .http
            .flow_get(&format!("/api/llm/metrics/overview?project_id={pid}"))
            .await?;
        let overview = resp.json().await?;
        Ok(GetLlmOverviewOutput { overview })
    }
}

// ── Get LLM Model Metrics ───────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetLlmModelMetricsInput {}

#[derive(Serialize)]
pub struct GetLlmModelMetricsOutput {
    pub models: serde_json::Value,
}

pub struct GetLlmModelMetrics;

#[async_trait]
impl PlatformAction for GetLlmModelMetrics {
    type Input = GetLlmModelMetricsInput;
    type Output = GetLlmModelMetricsOutput;

    fn name(&self) -> &'static str {
        "get_llm_model_metrics"
    }
    fn description(&self) -> &'static str {
        "Get per-model LLM usage metrics: request count, average latency, token usage, and cost \
         broken down by model identifier. Useful for identifying which models are most \
         used or expensive."
    }
    fn required_scope(&self) -> String {
        "llm:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        _input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let resp = ctx
            .http
            .flow_get(&format!("/api/llm/metrics/models?project_id={pid}"))
            .await?;
        let models = resp.json().await?;
        Ok(GetLlmModelMetricsOutput { models })
    }
}

// ── Get LLM Cost Daily ──────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetLlmCostDailyInput {}

#[derive(Serialize)]
pub struct GetLlmCostDailyOutput {
    pub costs: serde_json::Value,
}

pub struct GetLlmCostDaily;

#[async_trait]
impl PlatformAction for GetLlmCostDaily {
    type Input = GetLlmCostDailyInput;
    type Output = GetLlmCostDailyOutput;

    fn name(&self) -> &'static str {
        "get_llm_cost_daily"
    }
    fn description(&self) -> &'static str {
        "Get daily LLM cost breakdown for the current billing period. Returns an array of \
         daily entries with date, total cost, and per-model cost breakdown."
    }
    fn required_scope(&self) -> String {
        "llm:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        _input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let resp = ctx
            .http
            .flow_get(&format!("/api/llm/metrics/cost/daily?project_id={pid}"))
            .await?;
        let costs = resp.json().await?;
        Ok(GetLlmCostDailyOutput { costs })
    }
}

// ── Get LLM User Metrics ────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetLlmUserMetricsInput {}

#[derive(Serialize)]
pub struct GetLlmUserMetricsOutput {
    pub users: serde_json::Value,
}

pub struct GetLlmUserMetrics;

#[async_trait]
impl PlatformAction for GetLlmUserMetrics {
    type Input = GetLlmUserMetricsInput;
    type Output = GetLlmUserMetricsOutput;

    fn name(&self) -> &'static str {
        "get_llm_user_metrics"
    }
    fn description(&self) -> &'static str {
        "Get per-user LLM usage metrics: request count, tokens, and cost broken down by \
         user or API key. Useful for identifying heavy users or enforcing per-user quotas."
    }
    fn required_scope(&self) -> String {
        "llm:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        _input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let resp = ctx
            .http
            .flow_get(&format!("/api/llm/metrics/users?project_id={pid}"))
            .await?;
        let users = resp.json().await?;
        Ok(GetLlmUserMetricsOutput { users })
    }
}

// ── Registration ─────────────────────────────────────────────────────

pub fn register(registry: &mut ActionRegistry) {
    registry.register(GetLlmOverview);
    registry.register(GetLlmModelMetrics);
    registry.register(GetLlmCostDaily);
    registry.register(GetLlmUserMetrics);
}
