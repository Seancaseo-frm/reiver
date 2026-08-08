use async_trait::async_trait;
use schemars::JsonSchema;
use serde::Deserialize;

use crate::action::{ActionContext, PlatformAction};
use crate::actions::alerting::{
    ListAlertRules, ListAlertRulesInput, ListAlerts, ListAlertsInput, ListNotificationChannels,
    ListNotificationChannelsInput,
};
use crate::actions::dashboards::{
    ListDashboardTemplates, ListDashboardTemplatesInput, ListDashboards, ListDashboardsInput,
    ListWidgets, ListWidgetsInput,
};
use crate::actions::flow::integrations::{ListIntegrations, ListIntegrationsInput};
use crate::actions::flow::prompts::{
    ListPromptConfigs, ListPromptConfigsInput, ListPromptVersions, ListPromptVersionsInput,
    ListRollouts, ListRolloutsInput,
};
use crate::actions::flow::scores::{ListLlmScores, ListLlmScoresInput};
use crate::actions::flow::session_profiles::{ListSessionProfiles, ListSessionProfilesInput};
use crate::actions::flow::sessions::{ListLlmSessions, ListLlmSessionsInput};
use crate::actions::internal::llm_pricing::{ListLlmPricing, ListLlmPricingInput};
use crate::actions::projects::{ListApiKeys, ListApiKeysInput, ListProjects, ListProjectsInput};
use crate::actions::watch::api_endpoints::{
    ListApiEndpointErrors, ListApiEndpointErrorsInput, ListApiEndpoints, ListApiEndpointsInput,
};
use crate::actions::watch::exceptions::{ListExceptions, ListExceptionsInput};
use crate::actions::watch::health_checks::{ListHealthChecks, ListHealthChecksInput};
use crate::actions::watch::incidents::{ListIncidents, ListIncidentsInput};
use crate::actions::watch::logs::{
    ListLogAttributeKeys, ListLogAttributeKeysInput, ListLogAttributeValues,
    ListLogAttributeValuesInput,
};
use crate::actions::watch::maintenance_windows::{
    ListMaintenanceWindows, ListMaintenanceWindowsInput,
};
use crate::actions::watch::metrics::{ListMetricNames, ListMetricNamesInput};
use crate::actions::watch::profiles::{
    ListProfiles, ListProfilesInput, ListServiceProfiles, ListServiceProfilesInput,
};
use crate::actions::watch::services::{
    ListServiceVersions, ListServiceVersionsInput, ListServices, ListServicesInput,
};
use crate::actions::watch::traces::{
    ListTraceAttributeKeys, ListTraceAttributeKeysInput, ListTraceAttributeValues,
    ListTraceAttributeValuesInput, ListTraces, ListTracesInput,
};

#[derive(Deserialize, JsonSchema)]
pub struct ListA2aAgentsInput {
    /// Optional text query to filter agents
    pub query: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
pub struct ListA2aTasksInput {
    /// Optional status filter
    pub status: Option<String>,
    /// Max results (default 10)
    pub limit: Option<u32>,
}

macro_rules! dispatch {
    ($ctx:expr, $scope:literal, $action:expr, $p:expr) => {{
        super::require_scope($ctx, $scope)?;
        Ok(serde_json::to_value($action.execute($ctx, $p).await?)?)
    }};
}

/// Discriminated input for the unified `list` tool.
#[derive(Deserialize, JsonSchema)]
#[serde(tag = "resource")]
pub enum ListInput {
    /// List distributed traces
    #[serde(rename = "traces")]
    Traces(ListTracesInput),
    /// List monitored services
    #[serde(rename = "services")]
    Services(ListServicesInput),
    /// List service deployment versions
    #[serde(rename = "service_versions")]
    ServiceVersions(ListServiceVersionsInput),
    /// List LLM sessions
    #[serde(rename = "sessions")]
    Sessions(ListLlmSessionsInput),
    /// List LLM session profiles (filter sets for session content preservation)
    #[serde(rename = "session_profiles")]
    SessionProfiles(ListSessionProfilesInput),
    /// List exception groups
    #[serde(rename = "exceptions")]
    Exceptions(ListExceptionsInput),
    /// List incidents
    #[serde(rename = "incidents")]
    Incidents(ListIncidentsInput),
    /// List API endpoints
    #[serde(rename = "api_endpoints")]
    ApiEndpoints(ListApiEndpointsInput),
    /// List errors for a specific API endpoint
    #[serde(rename = "api_endpoint_errors")]
    ApiEndpointErrors(ListApiEndpointErrorsInput),
    /// List alert rules
    #[serde(rename = "alert_rules")]
    AlertRules(ListAlertRulesInput),
    /// List triggered alerts
    #[serde(rename = "alerts")]
    Alerts(ListAlertsInput),
    /// List notification channels
    #[serde(rename = "notification_channels")]
    NotificationChannels(ListNotificationChannelsInput),
    /// List dashboards
    #[serde(rename = "dashboards")]
    Dashboards(ListDashboardsInput),
    /// List dashboard templates
    #[serde(rename = "dashboard_templates")]
    DashboardTemplates(ListDashboardTemplatesInput),
    /// List widgets on a dashboard
    #[serde(rename = "widgets")]
    Widgets(ListWidgetsInput),
    /// List health checks
    #[serde(rename = "health_checks")]
    HealthChecks(ListHealthChecksInput),
    /// List maintenance windows
    #[serde(rename = "maintenance_windows")]
    MaintenanceWindows(ListMaintenanceWindowsInput),
    /// List LLM provider integrations
    #[serde(rename = "integrations")]
    Integrations(ListIntegrationsInput),
    /// List prompt configurations
    #[serde(rename = "prompt_configs")]
    PromptConfigs(ListPromptConfigsInput),
    /// List prompt versions for a config
    #[serde(rename = "prompt_versions")]
    PromptVersions(ListPromptVersionsInput),
    /// List prompt rollouts
    #[serde(rename = "rollouts")]
    Rollouts(ListRolloutsInput),
    /// List performance profiles
    #[serde(rename = "profiles")]
    Profiles(ListProfilesInput),
    /// List profiles for a specific service
    #[serde(rename = "service_profiles")]
    ServiceProfiles(ListServiceProfilesInput),
    /// List projects
    #[serde(rename = "projects")]
    Projects(ListProjectsInput),
    /// List API keys
    #[serde(rename = "api_keys")]
    ApiKeys(ListApiKeysInput),
    /// List LLM scores
    #[serde(rename = "llm_scores")]
    LlmScores(ListLlmScoresInput),
    /// List LLM model pricing (internal)
    #[serde(rename = "llm_pricing")]
    LlmPricing(ListLlmPricingInput),
    /// List available OpenTelemetry metric names
    #[serde(rename = "metric_names")]
    MetricNames(ListMetricNamesInput),
    /// Discover available span/resource attribute keys from recent traces
    #[serde(rename = "trace_attribute_keys")]
    TraceAttributeKeys(ListTraceAttributeKeysInput),
    /// Get distinct values for a trace attribute key
    #[serde(rename = "trace_attribute_values")]
    TraceAttributeValues(ListTraceAttributeValuesInput),
    /// Discover available log/resource attribute keys from recent log entries
    #[serde(rename = "log_attribute_keys")]
    LogAttributeKeys(ListLogAttributeKeysInput),
    /// Get distinct values for a log attribute key
    #[serde(rename = "log_attribute_values")]
    LogAttributeValues(ListLogAttributeValuesInput),
    /// Discover available A2A agents in the Herd registry
    #[serde(rename = "a2a_agents")]
    A2aAgents(ListA2aAgentsInput),
    /// List recent A2A tasks
    #[serde(rename = "a2a_tasks")]
    A2aTasks(ListA2aTasksInput),
}

pub struct ListTool;

#[async_trait]
impl PlatformAction for ListTool {
    type Input = ListInput;
    type Output = serde_json::Value;

    fn name(&self) -> &'static str {
        "list"
    }
    fn description(&self) -> &'static str {
        "Browse and list resources with optional filters. Set 'resource' to one of: traces, \
         services, service_versions, sessions, session_profiles, exceptions, incidents, \
         api_endpoints, api_endpoint_errors, alert_rules, alerts, notification_channels, \
         dashboards, dashboard_templates, widgets, health_checks, maintenance_windows, \
         integrations, prompt_configs, prompt_versions, rollouts, profiles, service_profiles, \
         projects, api_keys, llm_scores, llm_pricing, metric_names, trace_attribute_keys, \
         trace_attribute_values, log_attribute_keys, log_attribute_values, a2a_agents, \
         a2a_tasks. Most support limit/offset and resource-specific filters. Use 'get' with \
         a specific ID for full details. Use trace_attribute_keys/values and \
         log_attribute_keys/values to discover filterable attributes, then pass them to \
         traces or logs list/search."
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
            ListInput::Traces(p) => dispatch!(ctx, "observability:read", ListTraces, p),
            ListInput::Services(p) => dispatch!(ctx, "observability:read", ListServices, p),
            ListInput::ServiceVersions(p) => {
                dispatch!(ctx, "observability:read", ListServiceVersions, p)
            }
            ListInput::Sessions(p) => dispatch!(ctx, "llm:read", ListLlmSessions, p),
            ListInput::SessionProfiles(p) => dispatch!(ctx, "llm:read", ListSessionProfiles, p),
            ListInput::Exceptions(p) => dispatch!(ctx, "observability:read", ListExceptions, p),
            ListInput::Incidents(p) => dispatch!(ctx, "observability:read", ListIncidents, p),
            ListInput::ApiEndpoints(p) => dispatch!(ctx, "observability:read", ListApiEndpoints, p),
            ListInput::ApiEndpointErrors(p) => {
                dispatch!(ctx, "observability:read", ListApiEndpointErrors, p)
            }
            ListInput::AlertRules(p) => dispatch!(ctx, "observability:read", ListAlertRules, p),
            ListInput::Alerts(p) => dispatch!(ctx, "observability:read", ListAlerts, p),
            ListInput::NotificationChannels(p) => {
                dispatch!(ctx, "observability:read", ListNotificationChannels, p)
            }
            ListInput::Dashboards(p) => dispatch!(ctx, "observability:read", ListDashboards, p),
            ListInput::DashboardTemplates(p) => {
                dispatch!(ctx, "observability:read", ListDashboardTemplates, p)
            }
            ListInput::Widgets(p) => dispatch!(ctx, "observability:read", ListWidgets, p),
            ListInput::HealthChecks(p) => dispatch!(ctx, "observability:read", ListHealthChecks, p),
            ListInput::MaintenanceWindows(p) => {
                dispatch!(ctx, "observability:read", ListMaintenanceWindows, p)
            }
            ListInput::Integrations(p) => dispatch!(ctx, "llm:read", ListIntegrations, p),
            ListInput::PromptConfigs(p) => dispatch!(ctx, "llm:read", ListPromptConfigs, p),
            ListInput::PromptVersions(p) => dispatch!(ctx, "llm:read", ListPromptVersions, p),
            ListInput::Rollouts(p) => dispatch!(ctx, "llm:read", ListRollouts, p),
            ListInput::Profiles(p) => dispatch!(ctx, "observability:read", ListProfiles, p),
            ListInput::ServiceProfiles(p) => {
                dispatch!(ctx, "observability:read", ListServiceProfiles, p)
            }
            ListInput::Projects(p) => dispatch!(ctx, "project:read", ListProjects, p),
            ListInput::ApiKeys(p) => dispatch!(ctx, "project:read", ListApiKeys, p),
            ListInput::LlmScores(p) => dispatch!(ctx, "llm:read", ListLlmScores, p),
            ListInput::LlmPricing(p) => dispatch!(ctx, "internal:read", ListLlmPricing, p),
            ListInput::MetricNames(p) => dispatch!(ctx, "observability:read", ListMetricNames, p),
            ListInput::TraceAttributeKeys(p) => {
                dispatch!(ctx, "observability:read", ListTraceAttributeKeys, p)
            }
            ListInput::TraceAttributeValues(p) => {
                dispatch!(ctx, "observability:read", ListTraceAttributeValues, p)
            }
            ListInput::LogAttributeKeys(p) => {
                dispatch!(ctx, "observability:read", ListLogAttributeKeys, p)
            }
            ListInput::LogAttributeValues(p) => {
                dispatch!(ctx, "observability:read", ListLogAttributeValues, p)
            }
            ListInput::A2aAgents(p) => {
                super::require_scope(ctx, "project:read")?;
                if ctx.http.herd_url().is_empty() {
                    anyhow::bail!("Herd service URL not configured");
                }
                let mut path = "/api/herd/discover".to_string();
                if let Some(ref q) = p.query {
                    path = format!("{}?q={}", path, urlencoding::encode(q));
                }
                let resp = ctx.http.herd_get(&path).await?;
                Ok(resp.json::<serde_json::Value>().await?)
            }
            ListInput::A2aTasks(p) => {
                super::require_scope(ctx, "project:read")?;
                if ctx.http.herd_url().is_empty() {
                    anyhow::bail!("Herd service URL not configured");
                }
                let limit = p.limit.unwrap_or(10);
                let mut query = format!("/api/herd/tasks?limit={}", limit);
                if let Some(ref status) = p.status {
                    query = format!("{}&status={}", query, urlencoding::encode(status));
                }
                let resp = ctx.http.herd_get(&query).await?;
                Ok(resp.json::<serde_json::Value>().await?)
            }
        }
    }
}
