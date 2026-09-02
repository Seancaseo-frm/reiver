use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::registry::ActionRegistry;

#[derive(Deserialize, JsonSchema)]
pub struct ListModelPricingInput {}

#[derive(Serialize)]
pub struct ListModelPricingOutput {
    pub catalog: serde_json::Value,
}

pub struct ListModelPricing;

#[async_trait]
impl PlatformAction for ListModelPricing {
    type Input = ListModelPricingInput;
    type Output = ListModelPricingOutput;

    fn name(&self) -> &'static str {
        "list_model_pricing"
    }
    fn description(&self) -> &'static str {
        "List all available LLM models with pricing, latency percentiles, error rates, and \
         security stats (guardrail/PII/injection violation rates). Data is aggregated across \
         the platform over the last 24 hours. Models are grouped by provider."
    }
    fn required_scope(&self) -> String {
        "llm:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        _input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let resp = ctx.http.flow_get("/api/llm/models/pricing").await?;
        let catalog = resp.json().await?;
        Ok(ListModelPricingOutput { catalog })
    }
}

#[derive(Deserialize, JsonSchema)]
pub struct ListModelCatalogInput {}

#[derive(Serialize)]
pub struct ListModelCatalogOutput {
    pub catalog: serde_json::Value,
}

/// Project-filtered live model catalogue for agent-led onboarding.
pub struct ListModelCatalog;

#[async_trait]
impl PlatformAction for ListModelCatalog {
    type Input = ListModelCatalogInput;
    type Output = ListModelCatalogOutput;

    fn name(&self) -> &'static str {
        "list_model_catalog"
    }

    fn description(&self) -> &'static str {
        "List the current interactive LLM models available through this project's enabled \
         provider integrations. This live catalogue is the source of truth for model IDs; \
         do not copy model IDs from static documentation or guess newer names."
    }

    fn required_scope(&self) -> String {
        "llm:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        _input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let resp = ctx.http.flow_get("/api/llm/settings/models").await?;
        let catalog = resp.json().await?;
        Ok(ListModelCatalogOutput { catalog })
    }
}

pub fn register(registry: &mut ActionRegistry) {
    registry.register(ListModelPricing);
    registry.register(ListModelCatalog);
}
