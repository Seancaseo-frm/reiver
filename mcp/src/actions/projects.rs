use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::registry::ActionRegistry;

// ── List Projects ────────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListProjectsInput {}

#[derive(Serialize)]
pub struct ListProjectsOutput {
    pub projects: serde_json::Value,
}

pub struct ListProjects;

#[async_trait]
impl PlatformAction for ListProjects {
    type Input = ListProjectsInput;
    type Output = ListProjectsOutput;

    fn name(&self) -> &'static str {
        "list_projects"
    }
    fn description(&self) -> &'static str {
        "List projects accessible to the current agent token. Agent tokens are scoped to a \
         single project, so this returns the bound project's details."
    }
    fn required_scope(&self) -> String {
        "project:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        _input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let resp = ctx
            .http
            .website_get(&format!("/api/projects/{pid}"))
            .await?;
        let project: serde_json::Value = resp.json().await?;
        Ok(ListProjectsOutput {
            projects: serde_json::json!([project]),
        })
    }
}

// ── Get Project ──────────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetProjectInput {
    /// Project ID (UUID). If omitted, uses the project bound to the API key.
    pub project_id: Option<String>,
}

#[derive(Serialize)]
pub struct GetProjectOutput {
    pub project: serde_json::Value,
}

pub struct GetProject;

#[async_trait]
impl PlatformAction for GetProject {
    type Input = GetProjectInput;
    type Output = GetProjectOutput;

    fn name(&self) -> &'static str {
        "get_project"
    }
    fn description(&self) -> &'static str {
        "Get details for a project including name, creation date, and configuration. \
         If project_id is omitted, returns the project bound to the current API key."
    }
    fn required_scope(&self) -> String {
        "project:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = input
            .project_id
            .unwrap_or_else(|| ctx.project_id.to_string());
        let resp = ctx
            .http
            .website_get(&format!("/api/projects/{pid}"))
            .await?;
        let project = resp.json().await?;
        Ok(GetProjectOutput { project })
    }
}

// ── Get Project Stats ────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetProjectStatsInput {}

#[derive(Serialize)]
pub struct GetProjectStatsOutput {
    pub stats: serde_json::Value,
}

pub struct GetProjectStats;

#[async_trait]
impl PlatformAction for GetProjectStats {
    type Input = GetProjectStatsInput;
    type Output = GetProjectStatsOutput;

    fn name(&self) -> &'static str {
        "get_project_stats"
    }
    fn description(&self) -> &'static str {
        "Get exception statistics for the current project: counts by severity, trends \
         compared to the previous period, top exception types, and affected services."
    }
    fn required_scope(&self) -> String {
        "project:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        _input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let resp = ctx
            .http
            .website_get(&format!("/api/projects/{pid}/stats"))
            .await?;
        let stats = resp.json().await?;
        Ok(GetProjectStatsOutput { stats })
    }
}

// ── List API Keys ────────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListApiKeysInput {}

#[derive(Serialize)]
pub struct ListApiKeysOutput {
    pub keys: serde_json::Value,
}

pub struct ListApiKeys;

#[async_trait]
impl PlatformAction for ListApiKeys {
    type Input = ListApiKeysInput;
    type Output = ListApiKeysOutput;

    fn name(&self) -> &'static str {
        "list_api_keys"
    }
    fn description(&self) -> &'static str {
        "List API keys for the current project. Returns key metadata (ID, prefix, created_at, \
         last_used, scopes). Full key values are masked — they are only shown once at creation."
    }
    fn required_scope(&self) -> String {
        "project:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        _input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let resp = ctx
            .http
            .website_get(&format!("/api/projects/{pid}/keys"))
            .await?;
        let keys = resp.json().await?;
        Ok(ListApiKeysOutput { keys })
    }
}

// ── Create Project ───────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct CreateProjectInput {
    /// Name for the new project
    pub name: String,
}

#[derive(Serialize)]
pub struct CreateProjectOutput {
    pub project: serde_json::Value,
}

pub struct CreateProject;

#[async_trait]
impl PlatformAction for CreateProject {
    type Input = CreateProjectInput;
    type Output = CreateProjectOutput;

    fn name(&self) -> &'static str {
        "create_project"
    }
    fn description(&self) -> &'static str {
        "Create a new project under the current organization. Returns the project with its UUID. \
         Next steps: configure LLM integrations and create API keys."
    }
    fn required_scope(&self) -> String {
        "project:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let body = serde_json::json!({ "name": input.name });
        let resp = ctx.http.website_post("/api/projects", &body).await?;
        let project = resp.json().await?;
        Ok(CreateProjectOutput { project })
    }
}

// ── Update Project ───────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct UpdateProjectInput {
    /// New name for the project
    pub name: Option<String>,
}

#[derive(Serialize)]
pub struct UpdateProjectOutput {
    pub project: serde_json::Value,
}

pub struct UpdateProject;

#[async_trait]
impl PlatformAction for UpdateProject {
    type Input = UpdateProjectInput;
    type Output = UpdateProjectOutput;

    fn name(&self) -> &'static str {
        "update_project"
    }
    fn description(&self) -> &'static str {
        "Rename the current project. Only the project name can be updated via this tool."
    }
    fn required_scope(&self) -> String {
        "project:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let body = serde_json::json!({ "name": input.name });
        let resp = ctx
            .http
            .website_patch(&format!("/api/projects/{pid}"), &body)
            .await?;
        let project = resp.json().await?;
        Ok(UpdateProjectOutput { project })
    }
}

// ── Create API Key ───────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct CreateApiKeyInput {}

#[derive(Serialize)]
pub struct CreateApiKeyOutput {
    pub key: serde_json::Value,
}

pub struct CreateApiKey;

#[async_trait]
impl PlatformAction for CreateApiKey {
    type Input = CreateApiKeyInput;
    type Output = CreateApiKeyOutput;

    fn name(&self) -> &'static str {
        "create_api_key"
    }
    fn description(&self) -> &'static str {
        "Create a new API key for the current project. The full key value is returned only once \
         in the response — it cannot be retrieved again. Store it securely."
    }
    fn required_scope(&self) -> String {
        "project:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        _input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let body = serde_json::json!({});
        let resp = ctx
            .http
            .website_post(&format!("/api/projects/{pid}/keys"), &body)
            .await?;
        let key = resp.json().await?;
        Ok(CreateApiKeyOutput { key })
    }
}

// ── Registration ─────────────────────────────────────────────────────

pub fn register(registry: &mut ActionRegistry) {
    registry.register(ListProjects);
    registry.register(GetProject);
    registry.register(GetProjectStats);
    registry.register(ListApiKeys);
    registry.register(CreateProject);
    registry.register(UpdateProject);
    registry.register(CreateApiKey);
}
