use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::actions::types::IncidentStatus;
use crate::registry::ActionRegistry;

// ── List Incidents ──────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListIncidentsInput {
    /// Filter by incident status
    pub status: Option<IncidentStatus>,
    /// Maximum number of results to return (default: 50)
    #[schemars(range(min = 1, max = 1000))]
    pub limit: Option<u32>,
}

#[derive(Serialize)]
pub struct ListIncidentsOutput {
    pub incidents: serde_json::Value,
}

pub struct ListIncidents;

#[async_trait]
impl PlatformAction for ListIncidents {
    type Input = ListIncidentsInput;
    type Output = ListIncidentsOutput;

    fn name(&self) -> &'static str {
        "list_incidents"
    }
    fn description(&self) -> &'static str {
        "List incidents for the current project. Returns triggered incidents with timestamps, \
         severity, affected services, related alert rules, and current status (open/closed)."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let mut path = format!("/api/projects/{}/incidents", ctx.project_id);
        let mut params = vec![];
        if let Some(ref s) = input.status {
            let status_str = serde_json::to_value(s)?;
            if let Some(sv) = status_str.as_str() {
                params.push(format!("status={}", urlencoding::encode(sv)));
            }
        }
        if let Some(l) = input.limit {
            params.push(format!("limit={l}"));
        }
        if !params.is_empty() {
            path.push_str(&format!("?{}", params.join("&")));
        }

        let resp = ctx.http.watch_get(&path).await?;
        let incidents = resp.json().await?;
        Ok(ListIncidentsOutput { incidents })
    }
}

// ── Get Incident ────────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetIncidentInput {
    /// Incident ID
    pub incident_id: String,
}

#[derive(Serialize)]
pub struct GetIncidentOutput {
    pub incident: serde_json::Value,
}

pub struct GetIncident;

#[async_trait]
impl PlatformAction for GetIncident {
    type Input = GetIncidentInput;
    type Output = GetIncidentOutput;

    fn name(&self) -> &'static str {
        "get_incident"
    }
    fn description(&self) -> &'static str {
        "Get detailed context for a specific incident including timeline, affected services, \
         triggering alert rule, correlated traces, and resolution status."
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
            "/api/projects/{}/incidents/{}/context",
            ctx.project_id, input.incident_id
        );
        let resp = ctx.http.watch_get(&path).await?;
        let incident = resp.json().await?;
        Ok(GetIncidentOutput { incident })
    }
}

// ── List Incident Errors ────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListIncidentErrorsInput {
    /// Incident ID
    pub incident_id: String,
    /// Maximum number of results (default: 50)
    #[schemars(range(min = 1, max = 1000))]
    pub limit: Option<u32>,
}

#[derive(Serialize)]
pub struct ListIncidentErrorsOutput {
    pub errors: serde_json::Value,
}

pub struct ListIncidentErrors;

#[async_trait]
impl PlatformAction for ListIncidentErrors {
    type Input = ListIncidentErrorsInput;
    type Output = ListIncidentErrorsOutput;

    fn name(&self) -> &'static str {
        "list_incident_errors"
    }
    fn description(&self) -> &'static str {
        "List error events associated with an incident. Returns individual error occurrences \
         with stack traces, affected spans, and timestamps."
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
            "/api/projects/{}/incidents/{}/errors",
            ctx.project_id, input.incident_id
        );
        if let Some(l) = input.limit {
            path.push_str(&format!("?limit={l}"));
        }
        let resp = ctx.http.watch_get(&path).await?;
        let errors = resp.json().await?;
        Ok(ListIncidentErrorsOutput { errors })
    }
}

// ── Registration ─────────────────────────────────────────────────────

pub fn register(registry: &mut ActionRegistry) {
    registry.register(ListIncidents);
    registry.register(GetIncident);
    registry.register(ListIncidentErrors);
}
