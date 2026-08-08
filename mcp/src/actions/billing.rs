use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::registry::ActionRegistry;

// ── Get Usage ───────────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetUsageInput {}

#[derive(Serialize)]
pub struct GetUsageOutput {
    pub usage: serde_json::Value,
}

pub struct GetUsage;

#[async_trait]
impl PlatformAction for GetUsage {
    type Input = GetUsageInput;
    type Output = GetUsageOutput;

    fn name(&self) -> &'static str {
        "get_usage"
    }
    fn description(&self) -> &'static str {
        "Get organization-wide billing usage for the current period: events ingested, LLM tokens \
         processed, storage consumed, and current cost versus plan limit."
    }
    fn required_scope(&self) -> String {
        "billing:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        _input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let resp = ctx.http.website_get("/api/billing/usage").await?;
        let usage = resp.json().await?;
        Ok(GetUsageOutput { usage })
    }
}

// ── Get Usage By Project ────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetUsageByProjectInput {}

#[derive(Serialize)]
pub struct GetUsageByProjectOutput {
    pub usage: serde_json::Value,
}

pub struct GetUsageByProject;

#[async_trait]
impl PlatformAction for GetUsageByProject {
    type Input = GetUsageByProjectInput;
    type Output = GetUsageByProjectOutput;

    fn name(&self) -> &'static str {
        "get_usage_by_project"
    }
    fn description(&self) -> &'static str {
        "Get billing usage broken down by project. Returns the same metrics as get_usage \
         (events, tokens, storage, cost) attributed to each individual project."
    }
    fn required_scope(&self) -> String {
        "billing:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        _input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let resp = ctx
            .http
            .website_get("/api/billing/usage/by-project")
            .await?;
        let usage = resp.json().await?;
        Ok(GetUsageByProjectOutput { usage })
    }
}

// ── Get Budget Status ───────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetBudgetStatusInput {}

#[derive(Serialize)]
pub struct GetBudgetStatusOutput {
    pub status: serde_json::Value,
}

pub struct GetBudgetStatus;

#[async_trait]
impl PlatformAction for GetBudgetStatus {
    type Input = GetBudgetStatusInput;
    type Output = GetBudgetStatusOutput;

    fn name(&self) -> &'static str {
        "get_budget_status"
    }
    fn description(&self) -> &'static str {
        "Get current budget status: configured thresholds, current spend, remaining budget, \
         and whether hard-stop is enabled. Use update_gateway_settings to change budget limits."
    }
    fn required_scope(&self) -> String {
        "billing:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        _input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let resp = ctx.http.website_get("/api/billing/budget/status").await?;
        let status = resp.json().await?;
        Ok(GetBudgetStatusOutput { status })
    }
}

// ── Registration ─────────────────────────────────────────────────────

pub fn register(registry: &mut ActionRegistry) {
    registry.register(GetUsage);
    registry.register(GetUsageByProject);
    registry.register(GetBudgetStatus);
}
