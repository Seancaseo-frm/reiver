use std::collections::HashMap;

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::registry::ActionRegistry;

// ── List Traces ─────────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListTracesInput {
    /// Filter by trace status: "error" or "ok"
    pub status: Option<String>,
    /// Filter by service name
    pub service: Option<String>,
    /// Filter by deployment environment
    pub environment: Option<String>,
    /// Filter by service version
    pub service_version: Option<String>,
    /// Filter by HTTP method (GET, POST, PUT, DELETE, etc.)
    pub http_method: Option<String>,
    /// Filter by HTTP route pattern (e.g. "/api/users/:id")
    pub http_route: Option<String>,
    /// Substring search on span names
    pub search: Option<String>,
    /// Start of time range (ISO 8601 timestamp). Use with end_time for time-bounded queries.
    pub start_time: Option<String>,
    /// End of time range (ISO 8601 timestamp). Use with start_time for time-bounded queries.
    pub end_time: Option<String>,
    /// Sort field: "start_time" (default) or "duration"
    pub sort_by: Option<String>,
    /// Sort direction: "desc" (default) or "asc"
    pub sort_order: Option<String>,
    /// Maximum number of results to return (default: 50)
    #[schemars(range(min = 1, max = 1000))]
    pub limit: Option<u32>,
    /// Filter by span or resource attributes. Keys are OTel attribute names
    /// (e.g. "http.status_code", "db.system"), values are comma-separated
    /// match lists (e.g. "200,500"). Searches both span_attributes and
    /// resource_attributes.
    pub attributes: Option<HashMap<String, String>>,
}

#[derive(Serialize)]
pub struct ListTracesOutput {
    pub traces: serde_json::Value,
}

pub struct ListTraces;

#[async_trait]
impl PlatformAction for ListTraces {
    type Input = ListTracesInput;
    type Output = ListTracesOutput;

    fn name(&self) -> &'static str {
        "list_traces"
    }
    fn description(&self) -> &'static str {
        "List distributed traces. Filter by status (error/ok), service, environment, \
         HTTP method/route, time range (start_time/end_time), search by span name, or \
         arbitrary span/resource attributes (e.g. {\"http.status_code\": \"500\", \"db.system\": \"postgresql\"}). \
         Sort by start_time or duration. Returns trace summaries with trace ID, root span \
         name, duration, service, status, and timestamp. Use get_trace for the full span tree. \
         Use search logs with trace_id to find correlated logs."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let mut path = format!("/api/projects/{}/traces", ctx.project_id);
        let mut params = vec![];
        if let Some(ref s) = input.status {
            params.push(format!("trace_status={}", urlencoding::encode(s)));
        }
        if let Some(ref s) = input.service {
            params.push(format!("service={}", urlencoding::encode(s)));
        }
        if let Some(ref s) = input.environment {
            params.push(format!("environment={}", urlencoding::encode(s)));
        }
        if let Some(ref s) = input.service_version {
            params.push(format!("version={}", urlencoding::encode(s)));
        }
        if let Some(ref s) = input.http_method {
            params.push(format!("http_method={}", urlencoding::encode(s)));
        }
        if let Some(ref s) = input.http_route {
            params.push(format!("http_route={}", urlencoding::encode(s)));
        }
        if let Some(ref s) = input.search {
            params.push(format!("search={}", urlencoding::encode(s)));
        }
        if let Some(ref s) = input.start_time {
            params.push(format!("start_time={}", urlencoding::encode(s)));
        }
        if let Some(ref s) = input.end_time {
            params.push(format!("end_time={}", urlencoding::encode(s)));
        }
        if let Some(ref s) = input.sort_by {
            params.push(format!("sort_by={}", urlencoding::encode(s)));
        }
        if let Some(ref s) = input.sort_order {
            params.push(format!("sort_order={}", urlencoding::encode(s)));
        }
        if let Some(l) = input.limit {
            params.push(format!("limit={l}"));
        }
        if let Some(ref attrs) = input.attributes {
            for (k, v) in attrs {
                params.push(format!(
                    "attr.{}={}",
                    urlencoding::encode(k),
                    urlencoding::encode(v)
                ));
            }
        }
        if !params.is_empty() {
            path.push_str(&format!("?{}", params.join("&")));
        }

        let resp = ctx.http.watch_get(&path).await?;
        let traces = resp.json().await?;
        Ok(ListTracesOutput { traces })
    }
}

// ── List Trace Attribute Keys ────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListTraceAttributeKeysInput {}

#[derive(Serialize)]
pub struct ListTraceAttributeKeysOutput {
    pub keys: serde_json::Value,
}

pub struct ListTraceAttributeKeys;

#[async_trait]
impl PlatformAction for ListTraceAttributeKeys {
    type Input = ListTraceAttributeKeysInput;
    type Output = ListTraceAttributeKeysOutput;

    fn name(&self) -> &'static str {
        "list_trace_attribute_keys"
    }
    fn description(&self) -> &'static str {
        "Discover available span and resource attribute keys from recent traces. \
         Use this to find which attribute keys can be passed to list_traces attributes filter."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        _input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let path = format!("/api/projects/{}/traces/attribute-keys", ctx.project_id);
        let resp = ctx.http.watch_get(&path).await?;
        let keys = resp.json().await?;
        Ok(ListTraceAttributeKeysOutput { keys })
    }
}

// ── List Trace Attribute Values ─────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListTraceAttributeValuesInput {
    /// The attribute key to retrieve values for (e.g. "http.status_code")
    pub key: String,
}

#[derive(Serialize)]
pub struct ListTraceAttributeValuesOutput {
    pub values: serde_json::Value,
}

pub struct ListTraceAttributeValues;

#[async_trait]
impl PlatformAction for ListTraceAttributeValues {
    type Input = ListTraceAttributeValuesInput;
    type Output = ListTraceAttributeValuesOutput;

    fn name(&self) -> &'static str {
        "list_trace_attribute_values"
    }
    fn description(&self) -> &'static str {
        "Get the distinct values for a specific span/resource attribute key from recent traces. \
         Use after list_trace_attribute_keys to discover filterable values."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let path = format!(
            "/api/projects/{}/traces/attribute-values?key={}",
            ctx.project_id,
            urlencoding::encode(&input.key),
        );
        let resp = ctx.http.watch_get(&path).await?;
        let values = resp.json().await?;
        Ok(ListTraceAttributeValuesOutput { values })
    }
}

// ── Get Trace ───────────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetTraceInput {
    /// The trace ID to retrieve
    pub trace_id: String,
}

#[derive(Serialize)]
pub struct GetTraceOutput {
    pub trace: serde_json::Value,
}

pub struct GetTrace;

#[async_trait]
impl PlatformAction for GetTrace {
    type Input = GetTraceInput;
    type Output = GetTraceOutput;

    fn name(&self) -> &'static str {
        "get_trace"
    }
    fn description(&self) -> &'static str {
        "Get the full span tree for a specific distributed trace. Returns all spans with their \
         parent-child relationships, durations, attributes, and status codes."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let path = format!("/api/projects/{}/traces/{}", ctx.project_id, input.trace_id);
        let resp = ctx.http.watch_get(&path).await?;
        let trace = resp.json().await?;
        Ok(GetTraceOutput { trace })
    }
}

// ── Registration ─────────────────────────────────────────────────────

pub fn register(registry: &mut ActionRegistry) {
    registry.register(ListTraces);
    registry.register(ListTraceAttributeKeys);
    registry.register(ListTraceAttributeValues);
    registry.register(GetTrace);
}
