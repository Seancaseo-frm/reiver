use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::actions::alerting::{GetAlertRule, GetAlertRuleInput};
use crate::actions::attachments::{GetAttachment, GetAttachmentInput};
use crate::actions::dashboards::{GetDashboard, GetDashboardInput};
use crate::actions::flow::prompts::{
    GetPromptConfig, GetPromptConfigInput, GetPromptVersion, GetPromptVersionInput, GetRollout,
    GetRolloutInput, GetRolloutMetrics, GetRolloutMetricsInput,
};
use crate::actions::flow::scores::{GetRequestScores, GetRequestScoresInput};
use crate::actions::flow::session_profiles::{
    GetSessionProfileFilterFields, GetSessionProfileFilterFieldsInput,
};
use crate::actions::flow::sessions::{
    GetLlmSession, GetLlmSessionInput, GetSessionRequests, GetSessionRequestsInput,
};
use crate::actions::flow::settings::{GetGatewaySettings, GetGatewaySettingsInput};
use crate::actions::projects::{GetProject, GetProjectInput};
use crate::actions::watch::exceptions::{GetException, GetExceptionInput};
use crate::actions::watch::health_checks::{
    GetHealthCheck, GetHealthCheckInput, GetHealthCheckResults, GetHealthCheckResultsInput,
};
use crate::actions::watch::incidents::{
    GetIncident, GetIncidentInput, ListIncidentErrors, ListIncidentErrorsInput,
};
use crate::actions::watch::logs::{GetLog, GetLogContext, GetLogContextInput, GetLogInput};
use crate::actions::watch::maintenance_windows::{GetMaintenanceWindow, GetMaintenanceWindowInput};
use crate::actions::watch::profiles::{GetProfile, GetProfileInput};
use crate::actions::watch::traces::{GetTrace, GetTraceInput};

#[derive(Deserialize, JsonSchema)]
pub struct GetA2aTaskInput {
    /// The task ID to retrieve
    pub task_id: String,
}

#[derive(Serialize)]
struct JsonRpcRequest {
    jsonrpc: &'static str,
    id: u64,
    method: &'static str,
    params: serde_json::Value,
}

macro_rules! dispatch {
    ($ctx:expr, $scope:literal, $action:expr, $p:expr) => {{
        super::require_scope($ctx, $scope)?;
        Ok(serde_json::to_value($action.execute($ctx, $p).await?)?)
    }};
}

/// Discriminated input for the unified `get` tool.
#[derive(Deserialize, JsonSchema)]
#[serde(tag = "resource")]
pub enum GetInput {
    /// Get a distributed trace by ID
    #[serde(rename = "trace")]
    Trace(GetTraceInput),
    /// Get an LLM session with conversation metadata
    #[serde(rename = "session")]
    Session(GetLlmSessionInput),
    /// Get all requests within an LLM session
    #[serde(rename = "session_requests")]
    SessionRequests(GetSessionRequestsInput),
    /// Get a single log entry by ID
    #[serde(rename = "log")]
    Log(GetLogInput),
    /// Get surrounding log lines for context
    #[serde(rename = "log_context")]
    LogContext(GetLogContextInput),
    /// Get exception details with stack trace
    #[serde(rename = "exception")]
    Exception(GetExceptionInput),
    /// Get incident details and timeline
    #[serde(rename = "incident")]
    Incident(GetIncidentInput),
    /// List error events for a specific incident
    #[serde(rename = "incident_errors")]
    IncidentErrors(ListIncidentErrorsInput),
    /// Get alert rule configuration
    #[serde(rename = "alert_rule")]
    AlertRule(GetAlertRuleInput),
    /// Get dashboard layout and widgets
    #[serde(rename = "dashboard")]
    Dashboard(GetDashboardInput),
    /// Get health check configuration
    #[serde(rename = "health_check")]
    HealthCheck(GetHealthCheckInput),
    /// Get recent health check probe results
    #[serde(rename = "health_check_results")]
    HealthCheckResults(GetHealthCheckResultsInput),
    /// Get maintenance window details
    #[serde(rename = "maintenance_window")]
    MaintenanceWindow(GetMaintenanceWindowInput),
    /// Get prompt configuration
    #[serde(rename = "prompt_config")]
    PromptConfig(GetPromptConfigInput),
    /// Get a specific prompt version
    #[serde(rename = "prompt_version")]
    PromptVersion(GetPromptVersionInput),
    /// Get rollout details
    #[serde(rename = "rollout")]
    Rollout(GetRolloutInput),
    /// Get rollout performance metrics
    #[serde(rename = "rollout_metrics")]
    RolloutMetrics(GetRolloutMetricsInput),
    /// Get performance profile
    #[serde(rename = "profile")]
    Profile(GetProfileInput),
    /// Get project details
    #[serde(rename = "project")]
    Project(GetProjectInput),
    /// Get LLM request scores
    #[serde(rename = "request_scores")]
    RequestScores(GetRequestScoresInput),
    /// Get LLM gateway settings
    #[serde(rename = "gateway_settings")]
    GatewaySettings(GetGatewaySettingsInput),
    /// Get available filter fields for session profile conditions
    #[serde(rename = "session_profile_filter_fields")]
    SessionProfileFilterFields(GetSessionProfileFilterFieldsInput),
    /// Read the content of a file attachment by ID
    #[serde(rename = "attachment")]
    Attachment(GetAttachmentInput),
    /// Get the current state and messages for an A2A task
    #[serde(rename = "a2a_task")]
    A2aTask(GetA2aTaskInput),
}

pub struct GetTool;

#[async_trait]
impl PlatformAction for GetTool {
    type Input = GetInput;
    type Output = serde_json::Value;

    fn name(&self) -> &'static str {
        "get"
    }
    fn description(&self) -> &'static str {
        "Retrieve a specific resource by type and ID. Set 'resource' to one of: trace, session, \
         session_requests, log, log_context, exception, incident, incident_errors, alert_rule, \
         dashboard, health_check, health_check_results, maintenance_window, prompt_config, \
         prompt_version, rollout, rollout_metrics, profile, project, request_scores, \
         gateway_settings, session_profile_filter_fields, attachment, a2a_task. Each resource \
         type has its own ID field. Use 'list' to discover IDs and 'analyze' for aggregated \
         metrics."
    }
    fn required_scope(&self) -> String {
        "project:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        match input {
            GetInput::Trace(p) => dispatch!(ctx, "observability:read", GetTrace, p),
            GetInput::Session(p) => dispatch!(ctx, "llm:read", GetLlmSession, p),
            GetInput::SessionRequests(p) => dispatch!(ctx, "llm:read", GetSessionRequests, p),
            GetInput::Log(p) => dispatch!(ctx, "observability:read", GetLog, p),
            GetInput::LogContext(p) => dispatch!(ctx, "observability:read", GetLogContext, p),
            GetInput::Exception(p) => dispatch!(ctx, "observability:read", GetException, p),
            GetInput::Incident(p) => dispatch!(ctx, "observability:read", GetIncident, p),
            GetInput::IncidentErrors(p) => {
                dispatch!(ctx, "observability:read", ListIncidentErrors, p)
            }
            GetInput::AlertRule(p) => dispatch!(ctx, "observability:read", GetAlertRule, p),
            GetInput::Dashboard(p) => dispatch!(ctx, "observability:read", GetDashboard, p),
            GetInput::HealthCheck(p) => dispatch!(ctx, "observability:read", GetHealthCheck, p),
            GetInput::HealthCheckResults(p) => {
                dispatch!(ctx, "observability:read", GetHealthCheckResults, p)
            }
            GetInput::MaintenanceWindow(p) => {
                dispatch!(ctx, "observability:read", GetMaintenanceWindow, p)
            }
            GetInput::PromptConfig(p) => dispatch!(ctx, "llm:read", GetPromptConfig, p),
            GetInput::PromptVersion(p) => dispatch!(ctx, "llm:read", GetPromptVersion, p),
            GetInput::Rollout(p) => dispatch!(ctx, "llm:read", GetRollout, p),
            GetInput::RolloutMetrics(p) => dispatch!(ctx, "llm:read", GetRolloutMetrics, p),
            GetInput::Profile(p) => dispatch!(ctx, "observability:read", GetProfile, p),
            GetInput::Project(p) => dispatch!(ctx, "project:read", GetProject, p),
            GetInput::RequestScores(p) => dispatch!(ctx, "llm:read", GetRequestScores, p),
            GetInput::GatewaySettings(p) => dispatch!(ctx, "llm:read", GetGatewaySettings, p),
            GetInput::SessionProfileFilterFields(p) => {
                dispatch!(ctx, "llm:read", GetSessionProfileFilterFields, p)
            }
            GetInput::Attachment(p) => dispatch!(ctx, "project:read", GetAttachment, p),
            GetInput::A2aTask(p) => {
                super::require_scope(ctx, "project:read")?;
                if ctx.http.herd_url().is_empty() {
                    anyhow::bail!("Herd service URL not configured");
                }
                let rpc = JsonRpcRequest {
                    jsonrpc: "2.0",
                    id: 1,
                    method: "GetTask",
                    params: serde_json::json!({ "id": p.task_id }),
                };
                let resp = ctx.http.herd_post("/a2a", &rpc).await?;
                Ok(resp.json::<serde_json::Value>().await?)
            }
        }
    }
}
