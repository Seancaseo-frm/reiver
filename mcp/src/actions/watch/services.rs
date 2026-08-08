use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::registry::ActionRegistry;

// ── List Services ───────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListServicesInput {}

#[derive(Serialize)]
pub struct ListServicesOutput {
    pub services: serde_json::Value,
}

pub struct ListServices;

#[async_trait]
impl PlatformAction for ListServices {
    type Input = ListServicesInput;
    type Output = ListServicesOutput;

    fn name(&self) -> &'static str {
        "list_services"
    }
    fn description(&self) -> &'static str {
        "List services auto-discovered from OpenTelemetry traces and metrics. Returns each \
         service's name, type, last seen timestamp, and health status. Use service names \
         to filter traces and logs."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        _input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let path = format!("/api/projects/{}/services", ctx.project_id);
        let resp = ctx.http.watch_get(&path).await?;
        let services = resp.json().await?;
        Ok(ListServicesOutput { services })
    }
}

// ── Get Root Cause ──────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetRootCauseInput {
    /// Optional exception ID to scope the root-cause analysis
    pub exception_id: Option<String>,
}

#[derive(Serialize)]
pub struct GetRootCauseOutput {
    pub suggestions: serde_json::Value,
}

pub struct GetRootCause;

#[async_trait]
impl PlatformAction for GetRootCause {
    type Input = GetRootCauseInput;
    type Output = GetRootCauseOutput;

    fn name(&self) -> &'static str {
        "get_root_cause"
    }
    fn description(&self) -> &'static str {
        "Get AI-powered root-cause analysis suggestions for the current project. Optionally \
         scope to a specific exception ID for targeted analysis. Returns ranked suggestions \
         with confidence scores and supporting evidence from traces and logs."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let mut path = format!("/api/projects/{}/root-cause-suggestions", ctx.project_id);
        let mut params = vec![];
        if let Some(ref eid) = input.exception_id {
            params.push(format!("exception_id={}", urlencoding::encode(eid)));
        }
        if !params.is_empty() {
            path.push_str(&format!("?{}", params.join("&")));
        }

        let resp = ctx.http.watch_get(&path).await?;
        let suggestions = resp.json().await?;
        Ok(GetRootCauseOutput { suggestions })
    }
}

// ── List Service Versions ────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListServiceVersionsInput {
    /// Service name
    pub service: String,
    /// Maximum number of versions to return (default: 20)
    #[schemars(range(min = 1, max = 100))]
    pub limit: Option<u32>,
}

#[derive(Serialize)]
pub struct ListServiceVersionsOutput {
    pub versions: serde_json::Value,
}

pub struct ListServiceVersions;

#[async_trait]
impl PlatformAction for ListServiceVersions {
    type Input = ListServiceVersionsInput;
    type Output = ListServiceVersionsOutput;

    fn name(&self) -> &'static str {
        "list_service_versions"
    }
    fn description(&self) -> &'static str {
        "List deployed versions of a service detected from trace data. Returns each version's \
         identifier, first/last seen timestamps, and traffic share."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let svc = urlencoding::encode(&input.service);
        let mut path = format!("/api/projects/{}/services/{}/versions", ctx.project_id, svc);
        if let Some(l) = input.limit {
            path.push_str(&format!("?limit={l}"));
        }
        let resp = ctx.http.watch_get(&path).await?;
        let versions = resp.json().await?;
        Ok(ListServiceVersionsOutput { versions })
    }
}

// ── Compare Service Versions ────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct CompareServiceVersionsInput {
    /// Service name
    pub service: String,
    /// Baseline version identifier
    pub baseline: String,
    /// Comparison version identifier
    pub comparison: String,
}

#[derive(Serialize)]
pub struct CompareServiceVersionsOutput {
    pub comparison: serde_json::Value,
}

pub struct CompareServiceVersions;

#[async_trait]
impl PlatformAction for CompareServiceVersions {
    type Input = CompareServiceVersionsInput;
    type Output = CompareServiceVersionsOutput;

    fn name(&self) -> &'static str {
        "compare_service_versions"
    }
    fn description(&self) -> &'static str {
        "Compare two service versions side-by-side. Shows latency, error rate, \
         throughput, and new error types introduced in the comparison version."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let svc = urlencoding::encode(&input.service);
        let path = format!(
            "/api/projects/{}/services/{}/versions/compare?baseline={}&comparison={}",
            ctx.project_id,
            svc,
            urlencoding::encode(&input.baseline),
            urlencoding::encode(&input.comparison),
        );
        let resp = ctx.http.watch_get(&path).await?;
        let comparison = resp.json().await?;
        Ok(CompareServiceVersionsOutput { comparison })
    }
}

// ── Detect Faulty Deployments ───────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct DetectFaultyDeploymentsInput {
    /// Service name
    pub service: String,
}

#[derive(Serialize)]
pub struct DetectFaultyDeploymentsOutput {
    pub analysis: serde_json::Value,
}

pub struct DetectFaultyDeployments;

#[async_trait]
impl PlatformAction for DetectFaultyDeployments {
    type Input = DetectFaultyDeploymentsInput;
    type Output = DetectFaultyDeploymentsOutput;

    fn name(&self) -> &'static str {
        "detect_faulty_deployments"
    }
    fn description(&self) -> &'static str {
        "Analyse recent deployments of a service and detect versions that introduced \
         elevated error rates or latency regressions."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let svc = urlencoding::encode(&input.service);
        let path = format!(
            "/api/projects/{}/services/{}/deployments/faulty-detection",
            ctx.project_id, svc
        );
        let resp = ctx.http.watch_get(&path).await?;
        let analysis = resp.json().await?;
        Ok(DetectFaultyDeploymentsOutput { analysis })
    }
}

// ── Registration ─────────────────────────────────────────────────────

pub fn register(registry: &mut ActionRegistry) {
    registry.register(ListServices);
    registry.register(GetRootCause);
    registry.register(ListServiceVersions);
    registry.register(CompareServiceVersions);
    registry.register(DetectFaultyDeployments);
}
