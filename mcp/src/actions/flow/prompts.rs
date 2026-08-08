use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::registry::ActionRegistry;

// ── List Prompt Configs ─────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListPromptConfigsInput {}

#[derive(Serialize)]
pub struct ListPromptConfigsOutput {
    pub configs: serde_json::Value,
}

pub struct ListPromptConfigs;

#[async_trait]
impl PlatformAction for ListPromptConfigs {
    type Input = ListPromptConfigsInput;
    type Output = ListPromptConfigsOutput;

    fn name(&self) -> &'static str {
        "list_prompt_configs"
    }
    fn description(&self) -> &'static str {
        "List all prompt configurations for the current project. Each config has a unique name \
         used to reference it in gateway API calls via the X-Reiver-Prompt-Config header or \
         the prompt_config body field."
    }
    fn required_scope(&self) -> String {
        "llm:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        _input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let resp = ctx
            .http
            .flow_get(&format!("/api/llm/prompts/configs?project_id={pid}"))
            .await?;
        let configs = resp.json().await?;
        Ok(ListPromptConfigsOutput { configs })
    }
}

// ── Get Prompt Config ───────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetPromptConfigInput {
    /// The prompt configuration ID
    pub config_id: String,
}

#[derive(Serialize)]
pub struct GetPromptConfigOutput {
    pub config: serde_json::Value,
}

pub struct GetPromptConfig;

#[async_trait]
impl PlatformAction for GetPromptConfig {
    type Input = GetPromptConfigInput;
    type Output = GetPromptConfigOutput;

    fn name(&self) -> &'static str {
        "get_prompt_config"
    }
    fn description(&self) -> &'static str {
        "Get a specific prompt configuration by ID. The config's name field is used to select it \
         in gateway requests: set header X-Reiver-Prompt-Config to the name, or include \
         prompt_config in the request body."
    }
    fn required_scope(&self) -> String {
        "llm:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let resp = ctx
            .http
            .flow_get(&format!(
                "/api/llm/prompts/configs/{}?project_id={pid}",
                input.config_id
            ))
            .await?;
        let config = resp.json().await?;
        Ok(GetPromptConfigOutput { config })
    }
}

// ── List Prompt Versions ────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListPromptVersionsInput {
    /// The prompt configuration ID
    pub config_id: String,
}

#[derive(Serialize)]
pub struct ListPromptVersionsOutput {
    pub versions: serde_json::Value,
}

pub struct ListPromptVersions;

#[async_trait]
impl PlatformAction for ListPromptVersions {
    type Input = ListPromptVersionsInput;
    type Output = ListPromptVersionsOutput;

    fn name(&self) -> &'static str {
        "list_prompt_versions"
    }
    fn description(&self) -> &'static str {
        "List all versions of a prompt configuration"
    }
    fn required_scope(&self) -> String {
        "llm:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let resp = ctx
            .http
            .flow_get(&format!(
                "/api/llm/prompts/configs/{}/versions?project_id={pid}",
                input.config_id
            ))
            .await?;
        let versions = resp.json().await?;
        Ok(ListPromptVersionsOutput { versions })
    }
}

// ── Get Prompt Version ──────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetPromptVersionInput {
    /// The prompt configuration ID
    pub config_id: String,
    /// The version ID
    pub version_id: String,
}

#[derive(Serialize)]
pub struct GetPromptVersionOutput {
    pub version: serde_json::Value,
}

pub struct GetPromptVersion;

#[async_trait]
impl PlatformAction for GetPromptVersion {
    type Input = GetPromptVersionInput;
    type Output = GetPromptVersionOutput;

    fn name(&self) -> &'static str {
        "get_prompt_version"
    }
    fn description(&self) -> &'static str {
        "Get a specific version of a prompt configuration"
    }
    fn required_scope(&self) -> String {
        "llm:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let resp = ctx
            .http
            .flow_get(&format!(
                "/api/llm/prompts/configs/{}/versions/{}?project_id={pid}",
                input.config_id, input.version_id
            ))
            .await?;
        let version = resp.json().await?;
        Ok(GetPromptVersionOutput { version })
    }
}

// ── Create Prompt Config ────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct CreatePromptConfigInput {
    /// Name for the prompt configuration (must be unique within the project)
    pub name: String,
    /// What this prompt configuration is used for
    pub description: Option<String>,
}

#[derive(Serialize)]
pub struct CreatePromptConfigOutput {
    pub config: serde_json::Value,
}

pub struct CreatePromptConfig;

#[async_trait]
impl PlatformAction for CreatePromptConfig {
    type Input = CreatePromptConfigInput;
    type Output = CreatePromptConfigOutput;

    fn name(&self) -> &'static str {
        "create_prompt_config"
    }
    fn description(&self) -> &'static str {
        "Create a new prompt configuration. The name must be unique within the project and is used \
         to reference it in gateway API calls via the X-Reiver-Prompt-Config header or the \
         prompt_config body field."
    }
    fn required_scope(&self) -> String {
        "llm:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let body = serde_json::json!({
            "project_id": pid,
            "name": input.name,
            "description": input.description,
        });
        let resp = ctx
            .http
            .flow_post("/api/llm/prompts/configs", &body)
            .await?;
        let config = resp.json().await?;
        Ok(CreatePromptConfigOutput { config })
    }
}

fn default_temperature() -> f64 {
    0.5
}

// ── Create Prompt Version ───────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct CreatePromptVersionInput {
    /// The prompt configuration ID
    pub config_id: String,
    /// System prompt text
    pub system_prompt: Option<String>,
    /// Model override — if set, ALL gateway requests using this prompt version will be
    /// routed to this model regardless of the model specified in the API request payload.
    /// Leave empty/null to let the caller's request model take precedence.
    pub model: Option<String>,
    /// Sampling temperature, 0.0-1.0 (default: 0.5)
    #[schemars(range(min = 0.0, max = 1.0))]
    #[serde(default = "default_temperature")]
    pub temperature: f64,
    /// Maximum tokens to generate (default: model's maximum)
    #[schemars(range(min = 1, max = 128000))]
    pub max_tokens: Option<u32>,
    /// Template variable definitions: [{name, type, required, ...}]. Use JSON key `type` (alias `var_type`). Preserves existing variables if omitted.
    pub variables: Option<serde_json::Value>,
    /// Commit message describing what changed in this version
    pub commit_message: String,
}

#[derive(Serialize)]
pub struct CreatePromptVersionOutput {
    pub version: serde_json::Value,
}

pub struct CreatePromptVersion;

#[async_trait]
impl PlatformAction for CreatePromptVersion {
    type Input = CreatePromptVersionInput;
    type Output = CreatePromptVersionOutput;

    fn name(&self) -> &'static str {
        "create_prompt_version"
    }
    fn description(&self) -> &'static str {
        "Create a new immutable version of a prompt configuration. The system_prompt field is \
         the prompt content sent to the model and must contain the complete prompt text. \
         Read the created version back to verify content before deploying."
    }
    fn required_scope(&self) -> String {
        "llm:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        match &input.system_prompt {
            None => anyhow::bail!("system_prompt is required and must contain the prompt text"),
            Some(s) if s.trim().is_empty() => {
                anyhow::bail!("system_prompt is required and must contain the prompt text")
            }
            _ => {}
        }
        let pid = ctx.project_id;
        let body = serde_json::json!({
            "project_id": pid,
            "system_prompt": input.system_prompt,
            "model": input.model,
            "temperature": input.temperature,
            "max_tokens": input.max_tokens,
            "variables": input.variables,
            "commit_message": input.commit_message,
        });
        let resp = ctx
            .http
            .flow_post(
                &format!("/api/llm/prompts/configs/{}/versions", input.config_id),
                &body,
            )
            .await?;
        let version = resp.json().await?;
        Ok(CreatePromptVersionOutput { version })
    }
}

// ── Deploy Prompt ───────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct DeployPromptInput {
    /// The prompt configuration ID to deploy
    pub config_id: String,
    /// The target version ID to roll out
    pub target_version_id: String,
}

#[derive(Serialize)]
pub struct DeployPromptOutput {
    pub rollout: serde_json::Value,
}

pub struct DeployPrompt;

#[async_trait]
impl PlatformAction for DeployPrompt {
    type Input = DeployPromptInput;
    type Output = DeployPromptOutput;

    fn name(&self) -> &'static str {
        "deploy_prompt"
    }
    fn description(&self) -> &'static str {
        "Deploy a prompt version by creating and starting a progressive rollout. \
         Live traffic begins flowing to the new version immediately. \
         This action should be explicitly requested by the user. \
         Use get_rollout and get_rollout_metrics to monitor progress."
    }
    fn required_scope(&self) -> String {
        "llm:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;

        let create_body = serde_json::json!({
            "project_id": pid,
            "config_id": input.config_id,
            "target_version_id": input.target_version_id,
            "mode": "auto",
        });
        let create_resp = ctx
            .http
            .flow_post("/api/llm/prompts/rollouts", &create_body)
            .await?;
        let created: serde_json::Value = create_resp.json().await?;

        let rollout_id = created["id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("rollout response missing 'id' field"))?;

        let start_body = serde_json::json!({ "project_id": pid });
        let start_resp = ctx
            .http
            .flow_post(
                &format!("/api/llm/prompts/rollouts/{rollout_id}/start"),
                &start_body,
            )
            .await?;
        let rollout = start_resp.json().await?;
        Ok(DeployPromptOutput { rollout })
    }
}

// ── List Rollouts ───────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListRolloutsInput {
    /// Optional config ID to filter rollouts
    pub config_id: Option<String>,
}

#[derive(Serialize)]
pub struct ListRolloutsOutput {
    pub rollouts: serde_json::Value,
}

pub struct ListRollouts;

#[async_trait]
impl PlatformAction for ListRollouts {
    type Input = ListRolloutsInput;
    type Output = ListRolloutsOutput;

    fn name(&self) -> &'static str {
        "list_rollouts"
    }
    fn description(&self) -> &'static str {
        "List prompt rollouts, optionally filtered by config"
    }
    fn required_scope(&self) -> String {
        "llm:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let mut path = format!("/api/llm/prompts/rollouts?project_id={pid}");
        if let Some(config_id) = &input.config_id {
            path.push_str(&format!("&config_id={config_id}"));
        }
        let resp = ctx.http.flow_get(&path).await?;
        let rollouts = resp.json().await?;
        Ok(ListRolloutsOutput { rollouts })
    }
}

// ── Get Rollout ─────────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetRolloutInput {
    /// The rollout ID
    pub rollout_id: String,
}

#[derive(Serialize)]
pub struct GetRolloutOutput {
    pub rollout: serde_json::Value,
}

pub struct GetRollout;

#[async_trait]
impl PlatformAction for GetRollout {
    type Input = GetRolloutInput;
    type Output = GetRolloutOutput;

    fn name(&self) -> &'static str {
        "get_rollout"
    }
    fn description(&self) -> &'static str {
        "Get details of a specific rollout including status, current and target version IDs, \
         traffic split percentage, and timestamps. Rollout states: pending, active, paused, \
         completed, rolled_back."
    }
    fn required_scope(&self) -> String {
        "llm:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let resp = ctx
            .http
            .flow_get(&format!(
                "/api/llm/prompts/rollouts/{}?project_id={pid}",
                input.rollout_id
            ))
            .await?;
        let rollout = resp.json().await?;
        Ok(GetRolloutOutput { rollout })
    }
}

// ── Get Rollout Metrics ─────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetRolloutMetricsInput {
    /// The rollout ID
    pub rollout_id: String,
}

#[derive(Serialize)]
pub struct GetRolloutMetricsOutput {
    pub metrics: serde_json::Value,
}

pub struct GetRolloutMetrics;

#[async_trait]
impl PlatformAction for GetRolloutMetrics {
    type Input = GetRolloutMetricsInput;
    type Output = GetRolloutMetricsOutput;

    fn name(&self) -> &'static str {
        "get_rollout_metrics"
    }
    fn description(&self) -> &'static str {
        "Get performance metrics comparing the baseline and target versions during a rollout. \
         Includes latency, error rate, token usage, and cost. Use to decide whether to \
         proceed with the rollout or rollback."
    }
    fn required_scope(&self) -> String {
        "llm:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let resp = ctx
            .http
            .flow_get(&format!(
                "/api/llm/prompts/rollouts/{}/metrics?project_id={pid}",
                input.rollout_id
            ))
            .await?;
        let metrics = resp.json().await?;
        Ok(GetRolloutMetricsOutput { metrics })
    }
}

// ── Pause Rollout ───────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct PauseRolloutInput {
    /// The rollout ID to pause
    pub rollout_id: String,
}

#[derive(Serialize)]
pub struct PauseRolloutOutput {
    pub rollout: serde_json::Value,
}

pub struct PauseRollout;

#[async_trait]
impl PlatformAction for PauseRollout {
    type Input = PauseRolloutInput;
    type Output = PauseRolloutOutput;

    fn name(&self) -> &'static str {
        "pause_rollout"
    }
    fn description(&self) -> &'static str {
        "Pause an active rollout, freezing traffic at the current split ratio. \
         This action should be explicitly requested by the user. \
         Use get_rollout_metrics to evaluate before deciding to resume or rollback."
    }
    fn required_scope(&self) -> String {
        "llm:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let body = serde_json::json!({ "project_id": pid });
        let resp = ctx
            .http
            .flow_post(
                &format!("/api/llm/prompts/rollouts/{}/pause", input.rollout_id),
                &body,
            )
            .await?;
        let rollout = resp.json().await?;
        Ok(PauseRolloutOutput { rollout })
    }
}

// ── Rollback Rollout ────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct RollbackRolloutInput {
    /// The rollout ID to rollback
    pub rollout_id: String,
}

#[derive(Serialize)]
pub struct RollbackRolloutOutput {
    pub rollout: serde_json::Value,
}

pub struct RollbackRollout;

#[async_trait]
impl PlatformAction for RollbackRollout {
    type Input = RollbackRolloutInput;
    type Output = RollbackRolloutOutput;

    fn name(&self) -> &'static str {
        "rollback_rollout"
    }
    fn description(&self) -> &'static str {
        "Rollback a rollout to the previous version. \
         This action should be explicitly requested by the user."
    }
    fn required_scope(&self) -> String {
        "llm:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let body = serde_json::json!({ "project_id": pid });
        let resp = ctx
            .http
            .flow_post(
                &format!("/api/llm/prompts/rollouts/{}/rollback", input.rollout_id),
                &body,
            )
            .await?;
        let rollout = resp.json().await?;
        Ok(RollbackRolloutOutput { rollout })
    }
}

// ── Update Prompt Config ────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct UpdatePromptConfigInput {
    /// ID of the prompt configuration to update
    pub config_id: String,
    /// New name for the prompt config
    pub name: Option<String>,
    /// Updated description
    pub description: Option<String>,
}

#[derive(Serialize)]
pub struct UpdatePromptConfigOutput {
    pub config: serde_json::Value,
}

pub struct UpdatePromptConfig;

#[async_trait]
impl PlatformAction for UpdatePromptConfig {
    type Input = UpdatePromptConfigInput;
    type Output = UpdatePromptConfigOutput;

    fn name(&self) -> &'static str {
        "update_prompt_config"
    }
    fn description(&self) -> &'static str {
        "Update a prompt configuration's name or description."
    }
    fn required_scope(&self) -> String {
        "llm:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let mut body = serde_json::Map::new();
        body.insert("project_id".into(), serde_json::json!(pid));
        if let Some(n) = input.name {
            body.insert("name".into(), serde_json::Value::String(n));
        }
        if let Some(d) = input.description {
            body.insert("description".into(), serde_json::Value::String(d));
        }
        let resp = ctx
            .http
            .flow_put(
                &format!("/api/llm/prompts/configs/{}", input.config_id),
                &serde_json::Value::Object(body),
            )
            .await?;
        let config = resp.json().await?;
        Ok(UpdatePromptConfigOutput { config })
    }
}

// ── Promote Rollout ─────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct PromoteRolloutInput {
    /// ID of the rollout to promote
    pub rollout_id: String,
}

#[derive(Serialize)]
pub struct PromoteRolloutOutput {
    pub rollout: serde_json::Value,
}

pub struct PromoteRollout;

#[async_trait]
impl PlatformAction for PromoteRollout {
    type Input = PromoteRolloutInput;
    type Output = PromoteRolloutOutput;

    fn name(&self) -> &'static str {
        "promote_rollout"
    }
    fn description(&self) -> &'static str {
        "Advance a rollout to the next traffic split step, sending more traffic to the \
         target version. This action should be explicitly requested by the user. \
         Use get_rollout_metrics to compare performance before promoting."
    }
    fn required_scope(&self) -> String {
        "llm:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let body = serde_json::json!({ "project_id": pid });
        let resp = ctx
            .http
            .flow_post(
                &format!("/api/llm/prompts/rollouts/{}/promote", input.rollout_id),
                &body,
            )
            .await?;
        let rollout = resp.json().await?;
        Ok(PromoteRolloutOutput { rollout })
    }
}

// ── Complete Rollout ────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct CompleteRolloutInput {
    /// ID of the rollout to complete
    pub rollout_id: String,
}

#[derive(Serialize)]
pub struct CompleteRolloutOutput {
    pub rollout: serde_json::Value,
}

pub struct CompleteRollout;

#[async_trait]
impl PlatformAction for CompleteRollout {
    type Input = CompleteRolloutInput;
    type Output = CompleteRolloutOutput;

    fn name(&self) -> &'static str {
        "complete_rollout"
    }
    fn description(&self) -> &'static str {
        "Complete a rollout, making the target version the new active version at 100% traffic. \
         This finalises the deployment and should be explicitly requested by the user."
    }
    fn required_scope(&self) -> String {
        "llm:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let body = serde_json::json!({ "project_id": pid });
        let resp = ctx
            .http
            .flow_post(
                &format!("/api/llm/prompts/rollouts/{}/complete", input.rollout_id),
                &body,
            )
            .await?;
        let rollout = resp.json().await?;
        Ok(CompleteRolloutOutput { rollout })
    }
}

// ── Registration ─────────────────────────────────────────────────────

// ── Delete Prompt Config ────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct DeletePromptConfigInput {
    /// The prompt config ID to delete
    pub config_id: String,
}

#[derive(Serialize)]
pub struct DeletePromptConfigOutput {
    pub success: bool,
}

pub struct DeletePromptConfig;

#[async_trait]
impl PlatformAction for DeletePromptConfig {
    type Input = DeletePromptConfigInput;
    type Output = DeletePromptConfigOutput;

    fn name(&self) -> &'static str {
        "delete_prompt_config"
    }
    fn description(&self) -> &'static str {
        "Delete a prompt configuration. Cannot delete configs with active rollouts — \
         stop the rollout first."
    }
    fn required_scope(&self) -> String {
        "llm:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        ctx.http
            .flow_delete(&format!(
                "/api/llm/configs/{}?project_id={pid}",
                input.config_id
            ))
            .await?;
        Ok(DeletePromptConfigOutput { success: true })
    }
}

// ── Create Prompt Proposal (Prompt Compiler) ───────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct CreatePromptProposalInput {
    /// The prompt configuration ID this proposal is for
    pub config_id: String,
    /// Candidate system prompt text
    pub system_prompt: Option<String>,
    /// Model identifier for the candidate
    pub model: Option<String>,
    /// Sampling temperature (default: 0.5)
    #[schemars(range(min = 0.0, max = 1.0))]
    #[serde(default = "default_temperature")]
    pub temperature: f64,
    /// Maximum tokens to generate
    pub max_tokens: Option<u32>,
    /// Template variable definitions
    pub variables: Option<serde_json::Value>,
    /// Tool definitions
    pub tools: Option<serde_json::Value>,
    /// JSON schema for structured output
    pub response_format: Option<serde_json::Value>,
    /// Tool name whitelist
    pub allowed_tools: Option<serde_json::Value>,
    /// Explanation of the proposed changes
    pub reasoning: String,
    /// Comparison data: baseline vs candidate scores, per-session breakdowns
    pub comparison: serde_json::Value,
    /// Session IDs used for replay testing
    pub session_ids: Vec<String>,
}

#[derive(Serialize)]
pub struct CreatePromptProposalOutput {
    pub proposal: serde_json::Value,
}

pub struct CreatePromptProposal;

#[async_trait]
impl PlatformAction for CreatePromptProposal {
    type Input = CreatePromptProposalInput;
    type Output = CreatePromptProposalOutput;

    fn name(&self) -> &'static str {
        "create_prompt_proposal"
    }
    fn description(&self) -> &'static str {
        "Create a prompt improvement proposal. Stores the candidate prompt configuration \
         along with comparison scores and reasoning. The proposal is transient — it will be \
         deleted when the user accepts or dismisses it. Accepting creates a new prompt \
         version and starts a rollout."
    }
    fn required_scope(&self) -> String {
        "internal:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let body = serde_json::json!({
            "project_id": pid,
            "config_id": input.config_id,
            "system_prompt": input.system_prompt,
            "model": input.model,
            "temperature": input.temperature,
            "max_tokens": input.max_tokens,
            "variables": input.variables,
            "tools": input.tools,
            "response_format": input.response_format,
            "allowed_tools": input.allowed_tools,
            "reasoning": input.reasoning,
            "comparison": input.comparison,
            "session_ids": input.session_ids,
        });
        let resp = ctx
            .http
            .flow_post(
                &format!("/api/llm/prompts/configs/{}/proposals", input.config_id),
                &body,
            )
            .await?;
        let proposal = resp.json().await?;
        Ok(CreatePromptProposalOutput { proposal })
    }
}

// ── Compile Prompt (Prompt Compiler) ───────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct CompilePromptInput {
    /// The current system prompt text to improve
    pub source_prompt: String,
    /// Optional hint describing the optimization goal (e.g. "Minimize error rate",
    /// "Improve clarity and reduce cost"). Helps guide the rewriting.
    pub hint: Option<String>,
    /// Number of generation rounds (1-3, default 1). More rounds = more candidates
    /// but slower. Use 1 for a quick pass, 3 for thorough exploration.
    pub rounds: Option<u32>,
}

#[derive(Serialize)]
pub struct CompilePromptOutput {
    pub candidates: serde_json::Value,
    pub rounds_used: u32,
}

pub struct CompilePrompt;

#[async_trait]
impl PlatformAction for CompilePrompt {
    type Input = CompilePromptInput;
    type Output = CompilePromptOutput;

    fn name(&self) -> &'static str {
        "compile_prompt"
    }
    fn description(&self) -> &'static str {
        "Generate improved candidate rewrites of a system prompt. Returns 3 candidates per \
         round, each with a full rewritten system_prompt and reasoning. The compiler does not \
         evaluate candidates — use replay_session to test them against real sessions, then \
         create_prompt_proposal if a candidate is better."
    }
    fn required_scope(&self) -> String {
        "internal:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let body = serde_json::json!({
            "project_id": pid,
            "source_prompt": input.source_prompt,
            "hint": input.hint,
            "rounds": input.rounds.unwrap_or(1),
        });
        let resp = ctx
            .http
            .flow_post("/api/internal/prompt-compiler/compile", &body)
            .await?;
        let result: serde_json::Value = resp.json().await?;
        let candidates = result
            .get("candidates")
            .cloned()
            .unwrap_or(serde_json::json!([]));
        let rounds_used = result
            .get("rounds_used")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;
        Ok(CompilePromptOutput {
            candidates,
            rounds_used,
        })
    }
}

pub fn register(registry: &mut ActionRegistry) {
    registry.register(ListPromptConfigs);
    registry.register(GetPromptConfig);
    registry.register(ListPromptVersions);
    registry.register(GetPromptVersion);
    registry.register(CreatePromptConfig);
    registry.register(CreatePromptVersion);
    registry.register(DeployPrompt);
    registry.register(ListRollouts);
    registry.register(GetRollout);
    registry.register(GetRolloutMetrics);
    registry.register(PauseRollout);
    registry.register(RollbackRollout);
    registry.register(UpdatePromptConfig);
    registry.register(PromoteRollout);
    registry.register(CompleteRollout);
    registry.register(DeletePromptConfig);
    registry.register(CreatePromptProposal);
    registry.register(CompilePrompt);
}
