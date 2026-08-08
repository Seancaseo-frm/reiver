use std::collections::HashMap;

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::actions::types::LogLevel;
use crate::registry::ActionRegistry;

// ── Search Logs ─────────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct SearchLogsInput {
    /// Full-text search query (substring match against log body)
    pub query: Option<String>,
    /// Filter by log severity levels (comma-separated, e.g. "error,warn")
    pub severity: Option<String>,
    /// Filter by log severity level (single value, use severity for multi)
    pub level: Option<LogLevel>,
    /// Filter by a single service name
    pub service: Option<String>,
    /// Filter by multiple service names (comma-separated)
    pub service_names: Option<String>,
    /// Filter by deployment environments (comma-separated, e.g. "production,staging")
    pub environments: Option<String>,
    /// Filter by service versions (comma-separated)
    pub versions: Option<String>,
    /// Filter by regions (comma-separated)
    pub regions: Option<String>,
    /// Filter by host names (comma-separated)
    pub host_names: Option<String>,
    /// Filter by Kubernetes pod names (comma-separated)
    pub pod_names: Option<String>,
    /// Find logs correlated with a specific distributed trace
    pub trace_id: Option<String>,
    /// Relative time range: "15m", "1h", "24h" (default), "7d", "30d"
    pub time_range: Option<String>,
    /// Absolute start time (ISO 8601). Overrides time_range when both start_time and end_time are set.
    pub start_time: Option<String>,
    /// Absolute end time (ISO 8601). Overrides time_range when both start_time and end_time are set.
    pub end_time: Option<String>,
    /// Maximum number of results to return (default: 100)
    #[schemars(range(min = 1, max = 1000))]
    pub limit: Option<u32>,
    /// Filter by log or resource attributes. Keys are OTel attribute names
    /// (e.g. "k8s.pod.name", "exception.type"), values are comma-separated
    /// match lists (e.g. "NullPointerException,IOException"). Searches both
    /// log_attributes and resource_attributes.
    pub attributes: Option<HashMap<String, String>>,
}

#[derive(Serialize)]
pub struct SearchLogsOutput {
    pub logs: serde_json::Value,
}

pub struct SearchLogs;

#[async_trait]
impl PlatformAction for SearchLogs {
    type Input = SearchLogsInput;
    type Output = SearchLogsOutput;

    fn name(&self) -> &'static str {
        "search_logs"
    }
    fn description(&self) -> &'static str {
        "Search logs for the current project. Filter by severity (multi: 'error,warn'), \
         service name(s), environment(s), version(s), region(s), host(s), pod name(s), \
         text query, time range, or arbitrary log/resource attributes \
         (e.g. {\"exception.type\": \"NullPointerException\"}). Use trace_id to find logs \
         correlated with a specific distributed trace. Returns log entries with timestamp, \
         level, body, service name, and trace/span IDs. Results are ordered by timestamp descending."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let mut path = format!("/api/projects/{}/events", ctx.project_id);
        let mut params = vec!["event_type=logs".to_string()];
        if let Some(ref q) = input.query {
            params.push(format!("search={}", urlencoding::encode(q)));
        }
        if let Some(ref sev) = input.severity {
            params.push(format!("severity={}", urlencoding::encode(sev)));
        } else if let Some(ref l) = input.level {
            let level_str = serde_json::to_value(l)?;
            if let Some(s) = level_str.as_str() {
                params.push(format!("severity={}", urlencoding::encode(s)));
            }
        }
        if let Some(ref s) = input.service_names {
            params.push(format!("service_names={}", urlencoding::encode(s)));
        } else if let Some(ref s) = input.service {
            params.push(format!("service={}", urlencoding::encode(s)));
        }
        if let Some(ref s) = input.environments {
            params.push(format!("environments={}", urlencoding::encode(s)));
        }
        if let Some(ref s) = input.versions {
            params.push(format!("versions={}", urlencoding::encode(s)));
        }
        if let Some(ref s) = input.regions {
            params.push(format!("regions={}", urlencoding::encode(s)));
        }
        if let Some(ref s) = input.host_names {
            params.push(format!("host_names={}", urlencoding::encode(s)));
        }
        if let Some(ref s) = input.pod_names {
            params.push(format!("pod_names={}", urlencoding::encode(s)));
        }
        if let Some(ref t) = input.trace_id {
            params.push(format!("trace_id={}", urlencoding::encode(t)));
        }
        if let Some(ref tr) = input.time_range {
            params.push(format!("time_range={}", urlencoding::encode(tr)));
        }
        if let Some(ref st) = input.start_time {
            params.push(format!("start_time={}", urlencoding::encode(st)));
        }
        if let Some(ref et) = input.end_time {
            params.push(format!("end_time={}", urlencoding::encode(et)));
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
        path.push_str(&format!("?{}", params.join("&")));

        let resp = ctx.http.watch_get(&path).await?;
        let logs = resp.json().await?;
        Ok(SearchLogsOutput { logs })
    }
}

// ── List Log Attribute Keys ──────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListLogAttributeKeysInput {}

#[derive(Serialize)]
pub struct ListLogAttributeKeysOutput {
    pub keys: serde_json::Value,
}

pub struct ListLogAttributeKeys;

#[async_trait]
impl PlatformAction for ListLogAttributeKeys {
    type Input = ListLogAttributeKeysInput;
    type Output = ListLogAttributeKeysOutput;

    fn name(&self) -> &'static str {
        "list_log_attribute_keys"
    }
    fn description(&self) -> &'static str {
        "Discover available log and resource attribute keys from recent log entries. \
         Use this to find which attribute keys can be passed to search_logs attributes filter."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        _input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let path = format!("/api/projects/{}/events/attribute-keys", ctx.project_id);
        let resp = ctx.http.watch_get(&path).await?;
        let keys = resp.json().await?;
        Ok(ListLogAttributeKeysOutput { keys })
    }
}

// ── List Log Attribute Values ───────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListLogAttributeValuesInput {
    /// The attribute key to retrieve values for (e.g. "exception.type")
    pub key: String,
}

#[derive(Serialize)]
pub struct ListLogAttributeValuesOutput {
    pub values: serde_json::Value,
}

pub struct ListLogAttributeValues;

#[async_trait]
impl PlatformAction for ListLogAttributeValues {
    type Input = ListLogAttributeValuesInput;
    type Output = ListLogAttributeValuesOutput;

    fn name(&self) -> &'static str {
        "list_log_attribute_values"
    }
    fn description(&self) -> &'static str {
        "Get the distinct values for a specific log/resource attribute key from recent log entries. \
         Use after list_log_attribute_keys to discover filterable values."
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
            "/api/projects/{}/events/attribute-values?key={}",
            ctx.project_id,
            urlencoding::encode(&input.key),
        );
        let resp = ctx.http.watch_get(&path).await?;
        let values = resp.json().await?;
        Ok(ListLogAttributeValuesOutput { values })
    }
}

// ── Get Log ─────────────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetLogInput {
    /// Log entry ID
    pub log_id: String,
}

#[derive(Serialize)]
pub struct GetLogOutput {
    pub log: serde_json::Value,
}

pub struct GetLog;

#[async_trait]
impl PlatformAction for GetLog {
    type Input = GetLogInput;
    type Output = GetLogOutput;

    fn name(&self) -> &'static str {
        "get_log"
    }
    fn description(&self) -> &'static str {
        "Get a specific log entry by ID, including the full body, all attributes, \
         resource metadata, and associated trace/span IDs."
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
            "/api/projects/{}/logs/{}",
            ctx.project_id,
            urlencoding::encode(&input.log_id),
        );
        let resp = ctx.http.watch_get(&path).await?;
        let log = resp.json().await?;
        Ok(GetLogOutput { log })
    }
}

// ── Get Log Context ─────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetLogContextInput {
    /// Log entry ID to get context around
    pub log_id: String,
    /// Number of log lines before the target (default: 20)
    #[schemars(range(min = 1, max = 200))]
    pub lines_before: Option<u32>,
    /// Number of log lines after the target (default: 20)
    #[schemars(range(min = 1, max = 200))]
    pub lines_after: Option<u32>,
}

#[derive(Serialize)]
pub struct GetLogContextOutput {
    pub logs: serde_json::Value,
}

pub struct GetLogContext;

#[async_trait]
impl PlatformAction for GetLogContext {
    type Input = GetLogContextInput;
    type Output = GetLogContextOutput;

    fn name(&self) -> &'static str {
        "get_log_context"
    }
    fn description(&self) -> &'static str {
        "Get surrounding log lines for a specific log entry. Useful for understanding \
         what happened before and after an error or event."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let mut path = format!(
            "/api/projects/{}/logs/context?log_id={}",
            ctx.project_id,
            urlencoding::encode(&input.log_id),
        );
        if let Some(b) = input.lines_before {
            path.push_str(&format!("&lines_before={b}"));
        }
        if let Some(a) = input.lines_after {
            path.push_str(&format!("&lines_after={a}"));
        }
        let resp = ctx.http.watch_get(&path).await?;
        let logs = resp.json().await?;
        Ok(GetLogContextOutput { logs })
    }
}

// ── Registration ─────────────────────────────────────────────────────

pub fn register(registry: &mut ActionRegistry) {
    registry.register(SearchLogs);
    registry.register(ListLogAttributeKeys);
    registry.register(ListLogAttributeValues);
    registry.register(GetLog);
    registry.register(GetLogContext);
}
