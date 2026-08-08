use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::registry::ActionRegistry;

// ── List API Endpoints ──────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListApiEndpointsInput {
    /// Filter by service name
    pub service: Option<String>,
    /// Maximum number of results (default: 50)
    #[schemars(range(min = 1, max = 1000))]
    pub limit: Option<u32>,
}

#[derive(Serialize)]
pub struct ListApiEndpointsOutput {
    pub endpoints: serde_json::Value,
}

pub struct ListApiEndpoints;

#[async_trait]
impl PlatformAction for ListApiEndpoints {
    type Input = ListApiEndpointsInput;
    type Output = ListApiEndpointsOutput;

    fn name(&self) -> &'static str {
        "list_api_endpoints"
    }
    fn description(&self) -> &'static str {
        "List auto-discovered API endpoints from trace data. Returns each endpoint's \
         route, method, average latency, error rate, and request volume."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let mut path = format!("/api/projects/{}/api-endpoints", ctx.project_id);
        let mut params = vec![];
        if let Some(ref s) = input.service {
            params.push(format!("service={}", urlencoding::encode(s)));
        }
        if let Some(l) = input.limit {
            params.push(format!("limit={l}"));
        }
        if !params.is_empty() {
            path.push_str(&format!("?{}", params.join("&")));
        }
        let resp = ctx.http.watch_get(&path).await?;
        let endpoints = resp.json().await?;
        Ok(ListApiEndpointsOutput { endpoints })
    }
}

// ── List API Endpoint Errors ────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListApiEndpointErrorsInput {
    /// Filter by route pattern (e.g. "GET /api/users")
    pub route: Option<String>,
    /// Filter by service name
    pub service: Option<String>,
    /// Maximum number of results (default: 50)
    #[schemars(range(min = 1, max = 1000))]
    pub limit: Option<u32>,
}

#[derive(Serialize)]
pub struct ListApiEndpointErrorsOutput {
    pub errors: serde_json::Value,
}

pub struct ListApiEndpointErrors;

#[async_trait]
impl PlatformAction for ListApiEndpointErrors {
    type Input = ListApiEndpointErrorsInput;
    type Output = ListApiEndpointErrorsOutput;

    fn name(&self) -> &'static str {
        "list_api_endpoint_errors"
    }
    fn description(&self) -> &'static str {
        "List errors for API endpoints, optionally filtered by route or service. \
         Returns error types, counts, and sample traces."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let mut path = format!("/api/projects/{}/api-endpoints/errors", ctx.project_id);
        let mut params = vec![];
        if let Some(ref r) = input.route {
            params.push(format!("route={}", urlencoding::encode(r)));
        }
        if let Some(ref s) = input.service {
            params.push(format!("service={}", urlencoding::encode(s)));
        }
        if let Some(l) = input.limit {
            params.push(format!("limit={l}"));
        }
        if !params.is_empty() {
            path.push_str(&format!("?{}", params.join("&")));
        }
        let resp = ctx.http.watch_get(&path).await?;
        let errors = resp.json().await?;
        Ok(ListApiEndpointErrorsOutput { errors })
    }
}

// ── Get API Endpoints Summary ───────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetApiEndpointsSummaryInput {
    /// Filter by service name
    pub service: Option<String>,
}

#[derive(Serialize)]
pub struct GetApiEndpointsSummaryOutput {
    pub summary: serde_json::Value,
}

pub struct GetApiEndpointsSummary;

#[async_trait]
impl PlatformAction for GetApiEndpointsSummary {
    type Input = GetApiEndpointsSummaryInput;
    type Output = GetApiEndpointsSummaryOutput;

    fn name(&self) -> &'static str {
        "get_api_endpoints_summary"
    }
    fn description(&self) -> &'static str {
        "Get an aggregate summary of all API endpoints: total count, overall error rate, \
         average latency, and top offenders by error rate and latency."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let mut path = format!("/api/projects/{}/api-endpoints/summary", ctx.project_id);
        if let Some(ref s) = input.service {
            path.push_str(&format!("?service={}", urlencoding::encode(s)));
        }
        let resp = ctx.http.watch_get(&path).await?;
        let summary = resp.json().await?;
        Ok(GetApiEndpointsSummaryOutput { summary })
    }
}

// ── Registration ─────────────────────────────────────────────────────

pub fn register(registry: &mut ActionRegistry) {
    registry.register(ListApiEndpoints);
    registry.register(ListApiEndpointErrors);
    registry.register(GetApiEndpointsSummary);
}
