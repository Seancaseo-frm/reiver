use async_trait::async_trait;
use schemars::JsonSchema;
use serde::Deserialize;

use crate::action::{ActionContext, PlatformAction};
use crate::actions::billing::{
    GetBudgetStatus, GetBudgetStatusInput, GetUsage, GetUsageByProject, GetUsageByProjectInput,
    GetUsageInput,
};
use crate::actions::dashboards::{
    DashboardSnapshot, DashboardSnapshotInput, QueryWidget, QueryWidgetInput,
};
use crate::actions::flow::metrics::{
    GetLlmCostDaily, GetLlmCostDailyInput, GetLlmModelMetrics, GetLlmModelMetricsInput,
    GetLlmOverview, GetLlmOverviewInput, GetLlmUserMetrics, GetLlmUserMetricsInput,
};
use crate::actions::flow::playground::{
    CompareModels, CompareModelsInput, RunPlayground, RunPlaygroundInput,
};
use crate::actions::projects::{GetProjectStats, GetProjectStatsInput};
use crate::actions::watch::api_endpoints::{GetApiEndpointsSummary, GetApiEndpointsSummaryInput};
use crate::actions::watch::metrics::{QueryMetrics, QueryMetricsInput};
use crate::actions::watch::profiles::{CompareProfiles, CompareProfilesInput};
use crate::actions::watch::services::{
    CompareServiceVersions, CompareServiceVersionsInput, DetectFaultyDeployments,
    DetectFaultyDeploymentsInput, GetRootCause, GetRootCauseInput,
};
use crate::actions::watch::system_overview::{
    GetSystemOverview, GetSystemOverviewContext, GetSystemOverviewContextInput,
    GetSystemOverviewInput,
};

macro_rules! dispatch {
    ($ctx:expr, $scope:literal, $action:expr, $p:expr) => {{
        super::require_scope($ctx, $scope)?;
        Ok(serde_json::to_value($action.execute($ctx, $p).await?)?)
    }};
}

/// Discriminated input for the unified `analyze` tool.
#[derive(Deserialize, JsonSchema)]
#[serde(tag = "analysis")]
pub enum AnalyzeInput {
    /// LLM gateway overview metrics (requests, latency, errors, costs)
    #[serde(rename = "llm_overview")]
    LlmOverview(GetLlmOverviewInput),
    /// Per-model LLM metrics breakdown
    #[serde(rename = "llm_model_metrics")]
    LlmModelMetrics(GetLlmModelMetricsInput),
    /// Daily LLM cost breakdown
    #[serde(rename = "llm_cost_daily")]
    LlmCostDaily(GetLlmCostDailyInput),
    /// Per-user LLM usage metrics
    #[serde(rename = "llm_user_metrics")]
    LlmUserMetrics(GetLlmUserMetricsInput),
    /// Run a PromQL query on observability data
    #[serde(rename = "widget_query")]
    WidgetQuery(QueryWidgetInput),
    /// Get a full dashboard snapshot — executes all widget queries and returns their data
    #[serde(rename = "dashboard_snapshot")]
    DashboardSnapshot(DashboardSnapshotInput),
    /// Query OpenTelemetry metrics
    #[serde(rename = "otel_metrics")]
    OtelMetrics(QueryMetricsInput),
    /// Run a prompt in the LLM playground
    #[serde(rename = "playground")]
    Playground(RunPlaygroundInput),
    /// Compare two LLM models side-by-side
    #[serde(rename = "compare_models")]
    CompareModels(CompareModelsInput),
    /// Compare two service deployment versions
    #[serde(rename = "compare_versions")]
    CompareVersions(CompareServiceVersionsInput),
    /// Compare performance profiles
    #[serde(rename = "compare_profiles")]
    CompareProfiles(CompareProfilesInput),
    /// Detect faulty deployments via anomaly detection
    #[serde(rename = "detect_faults")]
    DetectFaults(DetectFaultyDeploymentsInput),
    /// AI-powered root cause analysis for an exception
    #[serde(rename = "root_cause")]
    RootCause(GetRootCauseInput),
    /// Get project-level statistics
    #[serde(rename = "project_stats")]
    ProjectStats(GetProjectStatsInput),
    /// Get API endpoint summary metrics
    #[serde(rename = "endpoint_summary")]
    EndpointSummary(GetApiEndpointsSummaryInput),
    /// Get overall platform usage
    #[serde(rename = "usage")]
    Usage(GetUsageInput),
    /// Get usage broken down by project
    #[serde(rename = "usage_by_project")]
    UsageByProject(GetUsageByProjectInput),
    /// Get budget status and spend tracking
    #[serde(rename = "budget_status")]
    BudgetStatus(GetBudgetStatusInput),
    /// Detect project technology stack and get golden signal queries per tier
    #[serde(rename = "system_overview")]
    SystemOverview(GetSystemOverviewInput),
    /// Get correlated traces and logs for a time window across all services
    #[serde(rename = "system_overview_context")]
    SystemOverviewContext(GetSystemOverviewContextInput),
}

pub struct AnalyzeTool;

#[async_trait]
impl PlatformAction for AnalyzeTool {
    type Input = AnalyzeInput;
    type Output = serde_json::Value;

    fn name(&self) -> &'static str {
        "analyze"
    }
    fn description(&self) -> &'static str {
        "Run analytics, queries, comparisons, and diagnostics. Set 'analysis' to one of: \
         llm_overview, llm_model_metrics, llm_cost_daily, llm_user_metrics (LLM metrics); \
         widget_query, otel_metrics, dashboard_snapshot (observability data); playground, \
         compare_models (LLM testing); compare_versions, compare_profiles, detect_faults, \
         root_cause (service diagnostics); project_stats, endpoint_summary (project analytics); \
         usage, usage_by_project, budget_status (billing); system_overview (detect project \
         technology stack with golden signal queries), system_overview_context (get correlated \
         traces/logs for a time window). Use dashboard_snapshot to see what a dashboard \
         currently shows — start here when the user asks about application health. Use \
         system_overview to detect the project's stack and system_overview_context to \
         investigate cross-stack issues in a specific time range. Use widget_query to run \
         ad-hoc PromQL queries. Use 'get' for individual resources and 'list' for browsing \
         collections."
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
            AnalyzeInput::LlmOverview(p) => dispatch!(ctx, "llm:read", GetLlmOverview, p),
            AnalyzeInput::LlmModelMetrics(p) => dispatch!(ctx, "llm:read", GetLlmModelMetrics, p),
            AnalyzeInput::LlmCostDaily(p) => dispatch!(ctx, "llm:read", GetLlmCostDaily, p),
            AnalyzeInput::LlmUserMetrics(p) => dispatch!(ctx, "llm:read", GetLlmUserMetrics, p),
            AnalyzeInput::WidgetQuery(p) => dispatch!(ctx, "observability:read", QueryWidget, p),
            AnalyzeInput::DashboardSnapshot(p) => {
                dispatch!(ctx, "observability:read", DashboardSnapshot, p)
            }
            AnalyzeInput::OtelMetrics(p) => dispatch!(ctx, "observability:read", QueryMetrics, p),
            AnalyzeInput::Playground(p) => dispatch!(ctx, "llm:write", RunPlayground, p),
            AnalyzeInput::CompareModels(p) => dispatch!(ctx, "llm:read", CompareModels, p),
            AnalyzeInput::CompareVersions(p) => {
                dispatch!(ctx, "observability:read", CompareServiceVersions, p)
            }
            AnalyzeInput::CompareProfiles(p) => {
                dispatch!(ctx, "observability:read", CompareProfiles, p)
            }
            AnalyzeInput::DetectFaults(p) => {
                dispatch!(ctx, "observability:read", DetectFaultyDeployments, p)
            }
            AnalyzeInput::RootCause(p) => dispatch!(ctx, "observability:read", GetRootCause, p),
            AnalyzeInput::ProjectStats(p) => dispatch!(ctx, "project:read", GetProjectStats, p),
            AnalyzeInput::EndpointSummary(p) => {
                dispatch!(ctx, "observability:read", GetApiEndpointsSummary, p)
            }
            AnalyzeInput::Usage(p) => dispatch!(ctx, "billing:read", GetUsage, p),
            AnalyzeInput::UsageByProject(p) => dispatch!(ctx, "billing:read", GetUsageByProject, p),
            AnalyzeInput::BudgetStatus(p) => dispatch!(ctx, "billing:read", GetBudgetStatus, p),
            AnalyzeInput::SystemOverview(p) => {
                dispatch!(ctx, "observability:read", GetSystemOverview, p)
            }
            AnalyzeInput::SystemOverviewContext(p) => {
                dispatch!(ctx, "observability:read", GetSystemOverviewContext, p)
            }
        }
    }
}
