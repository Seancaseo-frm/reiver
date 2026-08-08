use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::registry::ActionRegistry;

// ── System Overview (Stack Detection) ───────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetSystemOverviewInput {}

#[derive(Serialize)]
pub struct GetSystemOverviewOutput {
    pub stack: serde_json::Value,
}

pub struct GetSystemOverview;

#[async_trait]
impl PlatformAction for GetSystemOverview {
    type Input = GetSystemOverviewInput;
    type Output = GetSystemOverviewOutput;

    fn name(&self) -> &'static str {
        "get_system_overview"
    }
    fn description(&self) -> &'static str {
        "Detect the technology stack for the current project by analyzing ingested metrics. \
         Returns detected tiers (application, database, queue, cache, infrastructure, runtime) \
         with golden signal PromQL queries for each technology."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        _input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let path = format!("/api/system-overview/{}/stack", ctx.project_id);
        let resp = ctx.http.watch_get(&path).await?;
        let stack = resp.json().await?;
        Ok(GetSystemOverviewOutput { stack })
    }
}

// ── System Overview Context (Correlation) ───────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetSystemOverviewContextInput {
    /// Start of the time window (ISO 8601 timestamp, e.g. "2024-06-28T10:00:00Z")
    pub start_time: String,
    /// End of the time window (ISO 8601 timestamp, e.g. "2024-06-28T10:05:00Z")
    pub end_time: String,
}

#[derive(Serialize)]
pub struct GetSystemOverviewContextOutput {
    pub context: serde_json::Value,
}

pub struct GetSystemOverviewContext;

#[async_trait]
impl PlatformAction for GetSystemOverviewContext {
    type Input = GetSystemOverviewContextInput;
    type Output = GetSystemOverviewContextOutput;

    fn name(&self) -> &'static str {
        "get_system_overview_context"
    }
    fn description(&self) -> &'static str {
        "Get correlated traces and logs for a specific time window across all services. \
         Returns slow/error traces and error/warn logs grouped by service, useful for \
         diagnosing cross-stack issues during a time period of interest."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let start_ms = parse_timestamp_ms(&input.start_time)
            .ok_or_else(|| anyhow::anyhow!(
                "Invalid start_time '{}'. Provide ISO 8601 (e.g. '2024-06-28T10:00:00Z') or epoch milliseconds.",
                input.start_time
            ))?;
        let end_ms = parse_timestamp_ms(&input.end_time)
            .ok_or_else(|| anyhow::anyhow!(
                "Invalid end_time '{}'. Provide ISO 8601 (e.g. '2024-06-28T10:05:00Z') or epoch milliseconds.",
                input.end_time
            ))?;

        if end_ms <= start_ms {
            anyhow::bail!("end_time must be after start_time");
        }

        let path = format!("/api/system-overview/{}/context", ctx.project_id);
        let body = serde_json::json!({
            "start_ms": start_ms,
            "end_ms": end_ms,
        });
        let resp = ctx.http.watch_post(&path, &body).await?;
        let context = resp.json().await?;
        Ok(GetSystemOverviewContextOutput { context })
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

fn parse_timestamp_ms(s: &str) -> Option<i64> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp_millis());
    }
    if let Ok(ms) = s.parse::<i64>() {
        return Some(ms);
    }
    None
}

// ── Registration ─────────────────────────────────────────────────────

pub fn register(registry: &mut ActionRegistry) {
    registry.register(GetSystemOverview);
    registry.register(GetSystemOverviewContext);
}
