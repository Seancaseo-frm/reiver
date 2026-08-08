use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListLlmPricingInput {
    /// Filter by provider slug (e.g. "openai", "anthropic")
    pub provider: Option<String>,
    /// Search model slugs containing this string
    pub model: Option<String>,
    /// Only return enabled models (default true)
    pub enabled_only: Option<bool>,
    /// Max rows to return (default 50)
    pub limit: Option<i32>,
}

#[derive(Debug, Serialize)]
pub struct ListLlmPricingOutput {
    pub models: Vec<PricingRow>,
    pub total: i64,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct PricingRow {
    pub id: String,
    pub name: String,
    pub provider_slug: String,
    pub model_slug: String,
    pub prompt_cost: Option<String>,
    pub completion_cost: Option<String>,
    pub context_length: Option<i32>,
    pub enabled: bool,
    pub last_synced_at: chrono::DateTime<chrono::Utc>,
}

pub struct ListLlmPricing;

#[async_trait]
impl PlatformAction for ListLlmPricing {
    type Input = ListLlmPricingInput;
    type Output = ListLlmPricingOutput;

    fn name(&self) -> &'static str {
        "list_llm_pricing"
    }

    fn description(&self) -> &'static str {
        "List LLM model pricing from the model catalog (synced from OpenRouter). \
         Optionally filter by provider or model name. Returns per-token costs \
         (prompt/completion), context length, enabled status, and last sync time."
    }

    fn required_scope(&self) -> String {
        "internal:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let db = ctx
            .db
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No database available"))?;

        let limit = input.limit.unwrap_or(50).min(200);
        let enabled_only = input.enabled_only.unwrap_or(true);

        let mut query = String::from(
            "SELECT id, name, provider_slug, model_slug, \
             pricing->>'prompt' AS prompt_cost, \
             pricing->>'completion' AS completion_cost, \
             context_length, enabled, last_synced_at \
             FROM model_catalog WHERE 1=1",
        );
        let mut param_idx = 1u32;

        if enabled_only {
            query.push_str(" AND enabled = TRUE");
        }

        if input.provider.is_some() {
            query.push_str(&format!(" AND provider_slug = ${param_idx}"));
            param_idx += 1;
        }
        if input.model.is_some() {
            query.push_str(&format!(
                " AND model_slug ILIKE '%' || ${param_idx} || '%'"
            ));
            param_idx += 1;
        }
        query.push_str(&format!(
            " ORDER BY provider_slug, model_slug LIMIT ${param_idx}"
        ));

        let mut q = sqlx::query_as::<_, PricingRow>(&query);
        if let Some(ref provider) = input.provider {
            q = q.bind(provider);
        }
        if let Some(ref model) = input.model {
            q = q.bind(model);
        }
        q = q.bind(limit);

        let models: Vec<PricingRow> = q.fetch_all(db).await?;
        let total = models.len() as i64;

        Ok(ListLlmPricingOutput { models, total })
    }
}

pub fn register(registry: &mut crate::registry::ActionRegistry) {
    registry.register(ListLlmPricing);
}
