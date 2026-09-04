use async_trait::async_trait;
use schemars::JsonSchema;
use serde::Deserialize;

use crate::action::{ActionContext, PlatformAction};

/// Input for the unified `execute` tool.
///
/// The `resource`/`action` pair routes to the correct underlying operation.
/// `params` is a JSON object whose fields depend on the chosen resource and action.
#[derive(Deserialize, JsonSchema)]
pub struct ExecuteInput {
    /// Resource type (e.g. "project", "prompt", "dashboard", "alert_rule", "health_check",
    /// "maintenance_window", "gateway", "integration", "notification_channel",
    /// "cloud_integration", "auth_provider", "github", "exception", "llm_score", "a2a")
    pub resource: String,
    /// Action to perform on the resource (e.g. "create", "update", "deploy", "configure",
    /// "test", "pause", "rollback", "promote", "complete")
    pub action: String,
    /// Action-specific parameters as a JSON object. The required fields depend on the
    /// resource/action combination.
    pub params: serde_json::Value,
}

pub struct ExecuteTool;

macro_rules! exec {
    ($ctx:expr, $scope:literal, $action:expr, $params:expr) => {{
        super::require_scope($ctx, $scope)?;
        let p = serde_json::from_value($params)?;
        Ok(serde_json::to_value($action.execute($ctx, p).await?)?)
    }};
}

#[async_trait]
impl PlatformAction for ExecuteTool {
    type Input = ExecuteInput;
    type Output = serde_json::Value;

    fn name(&self) -> &'static str {
        "execute"
    }
    fn description(&self) -> &'static str {
        "Create, update, configure, deploy, test, and run operations. Provide 'resource' \
         (the entity type), 'action' (what to do), and 'params' (action-specific fields).\n\n\
         \
         PARAM SCHEMAS PER RESOURCE/ACTION (? = optional field):\n\n\
         \
         PROJECT:\n\
         project create: { name: string }\n\
         project update: { name?: string }\n\
         project create_api_key: {} (no params)\n\n\
         \
         PROMPT (see agent://flow/prompt-management for full workflow):\n\
         Note: list uses \"prompt_configs\", get uses \"prompt_config\", execute uses \"prompt\".\n\
         prompt create_config: { name: string, description?: string }\n\
         prompt update_config: { config_id: string, name?: string, description?: string }\n\
         prompt create_version: { config_id: string, system_prompt?: string, \
         model?: string (MODEL OVERRIDE — if set, ALL requests using this prompt version will be \
         routed to this model regardless of the model in the API payload; leave null to let the \
         caller's model take precedence), \
         temperature?: number (0-1, default 0.5), max_tokens?: number, variables?: array, \
         commit_message: string }\n\
         prompt deploy: { config_id: string, target_version_id: string }\n\
         prompt promote: { rollout_id: string }\n\
         prompt complete: { rollout_id: string }\n\
         prompt pause: { rollout_id: string }\n\
         prompt rollback: { rollout_id: string }\n\n\
         \
         DASHBOARD:\n\
         dashboard create: { name: string, description?: string }\n\
         dashboard create_from_template: { template_id: string }\n\
         dashboard update: { dashboard_id: string, name?: string, description?: string }\n\
         dashboard create_widget: { dashboard_id: string, title: string, \
         widget_type: \"timeseries\"|\"stat\"|\"table\"|\"bar\"|\"pie\"|\"heatmap\"|\"top_list\", \
         query: { promql: string, legend_format?: string, queries?: [{ promql: string, \
         legend_format?: string }], instant?: bool }, \
         x?: number, y?: number, w?: number (default 6), h?: number (default 4) }\n\
         dashboard import_grafana: { grafana_json: object (full Grafana export JSON), \
         dry_run?: bool (preview without creating) }\n\
         dashboard update_widget: { dashboard_id: string, widget_id: string, title?: string, \
         widget_type?: string, query?: { promql: string, ... }, \
         x?: number, y?: number, w?: number, h?: number }\n\n\
         \
         ALERT RULE:\n\
         alert_rule create: { rule: { name: string, description?: string, \
         query_config: { query_type: \"metrics\"|\"log_pattern\"|\"promql\"|\"llm\" (required), \
         metric_name?: string (for metrics/llm; use \"llm.\" prefix for LLM: \
         \"llm.error_rate\", \"llm.latency_p95\", \"llm.latency_avg\", \"llm.cost_daily\", \
         \"llm.token_usage\", \"llm.request_count\"), \
         filters?: {} (e.g. {\"model\": \"<observed-model-id>\"} for LLM model filter), \
         group_by?: [string], \
         time_aggregation?: string (avg/sum/min/max/count/p50/p95/p99), \
         space_aggregation?: string, \
         patterns?: [string] (for log_pattern), log_source?: \"all\"|\"otlp\"|\"unstructured\", \
         promql?: string (for promql type) }, \
         threshold?: number (default 0), threshold_type?: \"above\"|\"below\" (default \"above\"), \
         notification_channels?: [string (UUIDs)], alert_on_absent?: bool, \
         absent_for_seconds?: number (default 300), eval_window_seconds?: number (default 300), \
         eval_interval_seconds?: number (default 60), labels?: {}, annotations?: {}, \
         enabled?: bool (default true) } }\n\
         alert_rule update: { rule_id: string, rule: { name?: string, description?: string, \
         query_config?: {...}, threshold?: number, threshold_type?: string, \
         notification_channels?: [string], alert_on_absent?: bool, \
         absent_for_seconds?: number, eval_window_seconds?: number, \
         eval_interval_seconds?: number, labels?: {}, annotations?: {}, enabled?: bool } }\n\n\
         \
         INTEGRATION:\n\
         integration configure: { provider: \"openai\"|\"anthropic\"|\"google\"|\"theta\"|\"bedrock\", \
         secret_slot?: string (API key slot, required for non-Bedrock), \
         access_key_slot?: string (Bedrock), secret_key_slot?: string (Bedrock), \
         region?: string (Bedrock, default \"us-east-1\"), enabled?: bool }\n\
         integration update: { provider: string, secret_slot?: string, \
         access_key_slot?: string, secret_key_slot?: string, region?: string, enabled?: bool }\n\
         integration test: { provider: string }\n\
         integration create_secret_slot: { purpose: string, provider?: string }\n\n\
         \
         HEALTH CHECK:\n\
         health_check create: { name: string, \
         check_type: \"http\"|\"tcp\"|\"udp\"|\"ssl\", target_url?: string (http/ssl), \
         target_host?: string (tcp/udp), target_port?: number (tcp/udp), \
         http_method?: \"GET\"|\"POST\"|\"HEAD\", http_headers?: {}, \
         http_expected_status?: [number] (default [200]), http_timeout_ms?: number, \
         check_interval_seconds?: number (default 60), timeout_seconds?: number (default 30), \
         locations?: [string], enabled?: bool }\n\
         health_check update: { check_id: string, name?: string, target_url?: string, \
         target_host?: string, target_port?: number, http_method?: string, http_headers?: {}, \
         http_expected_status?: [number], http_timeout_ms?: number, \
         check_interval_seconds?: number, timeout_seconds?: number, \
         locations?: [string], enabled?: bool }\n\n\
         \
         MAINTENANCE WINDOW:\n\
         maintenance_window create: { name: string, description?: string, \
         schedule_type?: \"one_time\"|\"recurring\" (default \"one_time\"), \
         start_time?: string (ISO 8601), end_time?: string (ISO 8601), \
         recurrence_type?: \"daily\"|\"weekly\"|\"monthly\", recurrence_days?: [number], \
         recurrence_start_time?: string (HH:MM), recurrence_duration_minutes?: number, \
         recurrence_timezone?: string, recurrence_end_date?: string (YYYY-MM-DD), \
         enabled?: bool }\n\
         maintenance_window update: { window_id: string, name?: string, description?: string, \
         schedule_type?: string, start_time?: string, end_time?: string, \
         recurrence_type?: string, recurrence_days?: [number], recurrence_start_time?: string, \
         recurrence_duration_minutes?: number, recurrence_timezone?: string, \
         recurrence_end_date?: string, enabled?: bool }\n\n\
         \
         NOTIFICATION CHANNEL:\n\
         notification_channel configure: { \
         channel_type: \"slack\"|\"teams\"|\"discord\"|\"pagerduty\"|\"webhook\", \
         name: string, secret_slot: string (webhook URL or routing key), enabled?: bool }\n\
         notification_channel update: { channel_id: string, name?: string, enabled?: bool, \
         config?: {} }\n\
         notification_channel test: { channel_id: string }\n\
         notification_channel configure_servicenow: { name: string, instance_url: string, \
         username: string, password_slot: string, enabled?: bool }\n\n\
         \
         CLOUD INTEGRATION:\n\
         cloud_integration configure_aws: { name: string, \
         integration_type: \"ec2\"|\"rds\"|\"lambda\"|\"s3\"|\"ecs\"|\"eks\"|\"dynamodb\"|\"sqs\"|\"sns\", \
         region: string, auth_method: \"role\"|\"access_key\", \
         role_arn?: string, external_id_slot?: string, \
         access_key_id_slot?: string, secret_access_key_slot?: string, enabled?: bool }\n\
         cloud_integration configure_gcp: { name: string, integration_type: string, \
         gcp_project_id: string, service_account_json_slot?: string, enabled?: bool }\n\
         cloud_integration configure_azure: { name: string, integration_type: string, \
         subscription_id: string, tenant_id?: string, client_id_slot?: string, \
         client_secret_slot?: string, enabled?: bool }\n\
         cloud_integration configure_oci: { name: string, integration_type: string, \
         tenancy_ocid: string, region: string, user_ocid_slot: string, \
         private_key_slot: string, fingerprint_slot: string, \
         passphrase_slot?: string, enabled?: bool }\n\n\
         \
         AUTH PROVIDER:\n\
         auth_provider configure: { \
         provider: \"okta\"|\"auth0\"|\"entra_id\"|\"onelogin\"|\"ping_identity\"|\"keycloak\", \
         name: string, secret_slot: string, domain?: string, client_id?: string, \
         tenant_id?: string, environment_id?: string, region?: \"us\"|\"eu\", \
         poll_interval_seconds?: number (10-3600), event_types?: [string], enabled?: bool }\n\n\
         \
         GITHUB:\n\
         github list_installations: {} (no params)\n\
         github list_commits: { branch?: string, limit?: number (max 30) }\n\
         github get_commit: { sha: string }\n\
         github list_directory: { path?: string, ref?: string }\n\
         github read_file: { path: string, ref?: string }\n\
         github search_code: { query: string }\n\
         github link_repo: { repository_url: string }\n\
         github unlink_repo: {} (no params)\n\n\
         \
         EXCEPTION:\n\
         exception update_status: { exception_id: string, \
         status: \"unresolved\"|\"resolved\"|\"ignored\" }\n\n\
         \
         LLM SCORE:\n\
         llm_score submit: { request_id: string, score_name: string, score_value: number (0-100), \
         score_type?: \"number\"|\"boolean\", reason?: string, evaluator_type?: string, \
         evaluator_id?: string }\n\n\
         \
         SESSION:\n\
         session feedback: { session_id: string, score?: 1|-1|null }\n\
         session end: { session_id: string } — mark session as ended, triggers evaluation ~30s later \
         instead of waiting for the 30min idle timeout. Idempotent.\n\n\
         \
         SESSION PROFILE:\n\
         session_profile create: { name: string, logic?: \"AND\"|\"OR\", \
         filters: [{ field: string, op?: \"lt\"|\"lte\"|\"gt\"|\"gte\", value: number|string }] }\n\
         session_profile update: { id: string, name?: string, logic?: \"AND\"|\"OR\", \
         filters?: [{ field: string, op?: \"lt\"|\"lte\"|\"gt\"|\"gte\", value: number|string }] }\n\
         session_profile delete: { id: string }\n\
         Filter fields — numeric (use op + number): errors.count, latency.avg_ms, latency.max_ms, \
         cost.total, cost.avg_per_call, fallback.count, guardrail.count, tools.count. \
         Set (string value, no op): model.names, provider.names, prompt.ids, tools.names, labels.names. \
         To filter by labels: first define labels via gateway update_settings session_labels, \
         then use field \"labels.names\" with the label name as value. \
         Discover all via get resource \"session_profile_filter_fields\".\n\n\
         \
         A2A (Agent-to-Agent):\n\
         a2a register_agent: { name: string, description?: string, endpoint_url: string, \
         visibility?: \"private\"|\"org\"|\"public\" }\n\
         Use list resource \"a2a_agents\" to discover agents, list resource \"a2a_tasks\" \
         to see recent tasks, and get resource \"a2a_task\" to check a task's state.\n\n\
         \
         GATEWAY:\n\
         gateway update_settings: { settings: { introspection_enabled?: bool, \
         thinking_budget_tokens?: number (0-200000), fallback_enabled?: bool, \
         fallback_order?: [string], default_fallback_models?: [string] \
         (ordered project defaults; use IDs returned by list resource \"model_catalog\"; \
         applications normally send model \"auto\" and omit request-level fallback arrays), \
         provider_preferences?: { order?: [string], only?: [string], ignore?: [string], \
         allow_fallbacks?: bool, sort?: \"latency\" }, \
         retry_enabled?: bool, retry_max_attempts?: number (1-10), \
         monthly_budget_usd?: number, budget_alert_enabled?: bool, budget_hard_stop?: bool, \
         per_request_limit_usd?: number, rate_limit_enabled?: bool, rate_limit_rpm?: number, \
         session_budget_usd?: number, agent_enabled?: bool, \
         agent_scopes?: [string], guardrails?: { trust_mode?: \"agent\"|\"chatbot\", \
         blocked_input_topics?: [string], max_prompt_tokens?: number, \
         pii_block_on_detect?: bool, prompt_injection_detection?: bool, \
         spotlighting_enabled?: bool, mask_output_pii?: bool, \
         blocked_output_topics?: [string], min_quality_score?: number (0-1), \
         blocked_tools?: [string], block_exfiltration_urls?: bool }, \
         session_profiles?: [{ id: string, name: string, logic?: \"AND\"|\"OR\", \
         filters: [{ field: string, op?: string, value?: any }] }], \
         session_labels?: [{ name: string, definition?: string }] \
         (taxonomy for automatic session classification, max 50, unique names), \
         agent_soul?: { project_description?: string, tech_context?: string, \
         custom_instructions?: string, tone?: \"concise\"|\"detailed\"|\"casual\"|\"formal\", \
         key_services?: [{ name: string, description?: string, owner?: string }], \
         important_thresholds?: [string], known_issues?: [string], \
         playbooks?: [{ trigger: string, instructions: string }], \
         never_do?: [string], always_do?: [string] } } }\n\n\
         \
         EXAMPLES:\n\
         Create prompt and roll out (multi-step):\n\
         1) resource=\"prompt\", action=\"create_config\", \
         params={\"name\": \"my-prompt\", \"description\": \"...\"}\n\
         2) resource=\"prompt\", action=\"create_version\", \
         params={\"config_id\": \"<id>\", \"system_prompt\": \"...\", \
         \"commit_message\": \"v1\"}\n\
         3) Create a second version (v1 is auto-deployed): same as step 2 with updated content\n\
         4) resource=\"prompt\", action=\"deploy\", \
         params={\"config_id\": \"<id>\", \"target_version_id\": \"<v2-id>\"}\n\
         5) resource=\"prompt\", action=\"complete\", params={\"rollout_id\": \"<id>\"}\n\n\
         Create session profile: resource=\"session_profile\", action=\"create\", \
         params={\"name\": \"High cost errors\", \"logic\": \"AND\", \
         \"filters\": [{\"field\": \"errors.count\", \"op\": \"gte\", \"value\": 1}, \
         {\"field\": \"cost.total\", \"op\": \"gte\", \"value\": 5.0}]}\n\
         Create label-based profile (2 steps): \
         1) resource=\"gateway\", action=\"update_settings\", \
         params={\"settings\": {\"session_labels\": [{\"name\": \"billing-issue\", \
         \"definition\": \"Session involves billing disputes\"}]}} \
         2) resource=\"session_profile\", action=\"create\", \
         params={\"name\": \"Billing issues\", \
         \"filters\": [{\"field\": \"labels.names\", \"value\": \"billing-issue\"}]}\n\
         Create dashboard widget: resource=\"dashboard\", action=\"create_widget\", \
         params={\"dashboard_id\": \"<id>\", \"title\": \"Request Rate\", \
         \"widget_type\": \"timeseries\", \
         \"query\": {\"promql\": \"rate(http_requests_total[5m])\"}}\n\
         Create alert rule (metrics): resource=\"alert_rule\", action=\"create\", \
         params={\"rule\": {\"name\": \"High error rate\", \
         \"query_config\": {\"query_type\": \"metrics\", \"metric_name\": \"http.server.duration\", \
         \"time_aggregation\": \"count\", \"filters\": {\"http.status_code\": \"500\"}}, \
         \"threshold\": 10, \"threshold_type\": \"above\"}}\n\
         Create alert rule (LLM): resource=\"alert_rule\", action=\"create\", \
         params={\"rule\": {\"name\": \"Gateway P95 latency\", \
         \"query_config\": {\"query_type\": \"llm\", \"metric_name\": \"llm.latency_p95\"}, \
         \"threshold\": 30000, \"threshold_type\": \"above\", \
         \"notification_channels\": [\"<channel-uuid>\"], \
         \"eval_window_seconds\": 300}}\n\
         Create alert rule (PromQL): resource=\"alert_rule\", action=\"create\", \
         params={\"rule\": {\"name\": \"Service down\", \
         \"query_config\": {\"query_type\": \"promql\", \"promql\": \"up == 0\"}, \
         \"threshold\": 1, \"threshold_type\": \"above\"}}\n\
         Register A2A agent: resource=\"a2a\", action=\"register_agent\", \
         params={\"name\": \"my-agent\", \"endpoint_url\": \"https://example.com/a2a\"}"
    }
    fn required_scope(&self) -> String {
        "project:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        use crate::actions;

        let params = match input.params {
            serde_json::Value::String(ref s) => serde_json::from_str(s).unwrap_or(input.params),
            other => other,
        };

        let input = ExecuteInput {
            resource: input.resource,
            action: input.action,
            params,
        };

        match (input.resource.as_str(), input.action.as_str()) {
            // ── project ─────────────────────────────────────────────
            ("project", "create") => exec!(
                ctx,
                "project:write",
                actions::projects::CreateProject,
                input.params
            ),
            ("project", "update") => exec!(
                ctx,
                "project:write",
                actions::projects::UpdateProject,
                input.params
            ),
            ("project", "create_api_key") => exec!(
                ctx,
                "project:write",
                actions::projects::CreateApiKey,
                input.params
            ),

            // ── prompt ──────────────────────────────────────────────
            ("prompt", "create_config") => exec!(
                ctx,
                "llm:write",
                actions::flow::prompts::CreatePromptConfig,
                input.params
            ),
            ("prompt", "update_config") => exec!(
                ctx,
                "llm:write",
                actions::flow::prompts::UpdatePromptConfig,
                input.params
            ),
            ("prompt", "create_version") => exec!(
                ctx,
                "llm:write",
                actions::flow::prompts::CreatePromptVersion,
                input.params
            ),
            ("prompt", "deploy") => exec!(
                ctx,
                "llm:write",
                actions::flow::prompts::DeployPrompt,
                input.params
            ),
            ("prompt", "promote") => exec!(
                ctx,
                "llm:write",
                actions::flow::prompts::PromoteRollout,
                input.params
            ),
            ("prompt", "complete") => exec!(
                ctx,
                "llm:write",
                actions::flow::prompts::CompleteRollout,
                input.params
            ),
            ("prompt", "pause") => exec!(
                ctx,
                "llm:write",
                actions::flow::prompts::PauseRollout,
                input.params
            ),
            ("prompt", "rollback") => exec!(
                ctx,
                "llm:write",
                actions::flow::prompts::RollbackRollout,
                input.params
            ),

            // ── gateway ─────────────────────────────────────────────
            ("gateway", "update_settings") => exec!(
                ctx,
                "llm:write",
                actions::flow::settings::UpdateGatewaySettings,
                input.params
            ),

            // ── integration ─────────────────────────────────────────
            ("integration", "configure") => exec!(
                ctx,
                "llm:write",
                actions::flow::integrations::ConfigureIntegration,
                input.params
            ),
            ("integration", "update") => exec!(
                ctx,
                "llm:write",
                actions::flow::integrations::UpdateLlmIntegration,
                input.params
            ),
            ("integration", "test") => exec!(
                ctx,
                "llm:write",
                actions::flow::integrations::TestIntegration,
                input.params
            ),
            ("integration", "create_secret_slot") => exec!(
                ctx,
                "llm:write",
                actions::flow::integrations::CreateSecretSlot,
                input.params
            ),

            // ── dashboard ───────────────────────────────────────────
            ("dashboard", "create") => exec!(
                ctx,
                "observability:write",
                actions::dashboards::CreateDashboard,
                input.params
            ),
            ("dashboard", "create_from_template") => exec!(
                ctx,
                "observability:write",
                actions::dashboards::CreateDashboardFromTemplate,
                input.params
            ),
            ("dashboard", "update") => exec!(
                ctx,
                "observability:write",
                actions::dashboards::UpdateDashboard,
                input.params
            ),
            ("dashboard", "create_widget") => exec!(
                ctx,
                "observability:write",
                actions::dashboards::CreateWidget,
                input.params
            ),
            ("dashboard", "update_widget") => exec!(
                ctx,
                "observability:write",
                actions::dashboards::UpdateWidget,
                input.params
            ),
            ("dashboard", "import_grafana") => exec!(
                ctx,
                "observability:write",
                actions::dashboards::ImportGrafanaDashboard,
                input.params
            ),

            // ── alert_rule ──────────────────────────────────────────
            ("alert_rule", "create") => exec!(
                ctx,
                "observability:write",
                actions::alerting::CreateAlertRule,
                input.params
            ),
            ("alert_rule", "update") => exec!(
                ctx,
                "observability:write",
                actions::alerting::UpdateAlertRule,
                input.params
            ),

            // ── health_check ────────────────────────────────────────
            ("health_check", "create") => exec!(
                ctx,
                "observability:write",
                actions::watch::health_checks::CreateHealthCheck,
                input.params
            ),
            ("health_check", "update") => exec!(
                ctx,
                "observability:write",
                actions::watch::health_checks::UpdateHealthCheck,
                input.params
            ),

            // ── maintenance_window ──────────────────────────────────
            ("maintenance_window", "create") => exec!(
                ctx,
                "observability:write",
                actions::watch::maintenance_windows::CreateMaintenanceWindow,
                input.params
            ),
            ("maintenance_window", "update") => exec!(
                ctx,
                "observability:write",
                actions::watch::maintenance_windows::UpdateMaintenanceWindow,
                input.params
            ),

            // ── notification_channel ────────────────────────────────
            ("notification_channel", "configure") => exec!(
                ctx,
                "observability:write",
                actions::watch::notification_channels::ConfigureNotificationChannel,
                input.params
            ),
            ("notification_channel", "update") => exec!(
                ctx,
                "observability:write",
                actions::watch::notification_channels::UpdateNotificationChannel,
                input.params
            ),
            ("notification_channel", "test") => exec!(
                ctx,
                "observability:write",
                actions::alerting::TestNotification,
                input.params
            ),
            ("notification_channel", "configure_servicenow") => exec!(
                ctx,
                "observability:write",
                actions::watch::notification_channels::ConfigureServiceNow,
                input.params
            ),

            // ── cloud_integration ───────────────────────────────────
            ("cloud_integration", "configure_aws") => exec!(
                ctx,
                "observability:write",
                actions::watch::aws::ConfigureAwsIntegration,
                input.params
            ),
            ("cloud_integration", "configure_gcp") => exec!(
                ctx,
                "observability:write",
                actions::watch::gcp::ConfigureGcpIntegration,
                input.params
            ),
            ("cloud_integration", "configure_azure") => exec!(
                ctx,
                "observability:write",
                actions::watch::azure::ConfigureAzureIntegration,
                input.params
            ),
            ("cloud_integration", "configure_oci") => exec!(
                ctx,
                "observability:write",
                actions::watch::oci::ConfigureOciIntegration,
                input.params
            ),

            // ── auth_provider ───────────────────────────────────────
            ("auth_provider", "configure") => exec!(
                ctx,
                "observability:write",
                actions::watch::auth_events::ConfigureAuthProvider,
                input.params
            ),

            // ── github ──────────────────────────────────────────────
            ("github", _) => {
                super::require_scope(ctx, "observability:read")?;
                let mut map = match input.params {
                    serde_json::Value::Object(m) => m,
                    serde_json::Value::Null => serde_json::Map::new(),
                    other => serde_json::from_value(other)?,
                };
                map.insert("action".into(), serde_json::Value::String(input.action));
                let p = serde_json::from_value(serde_json::Value::Object(map))?;
                Ok(serde_json::to_value(
                    actions::watch::github::Github.execute(ctx, p).await?,
                )?)
            }

            // ── exception ───────────────────────────────────────────
            ("exception", "update_status") => exec!(
                ctx,
                "observability:write",
                actions::watch::exceptions::UpdateExceptionStatus,
                input.params
            ),

            // ── llm_score ───────────────────────────────────────────
            ("llm_score", "submit") => exec!(
                ctx,
                "llm:write",
                actions::flow::scores::SubmitLlmScore,
                input.params
            ),

            // ── session ─────────────────────────────────────────────
            ("session", "feedback") => exec!(
                ctx,
                "llm:write",
                actions::flow::sessions::SubmitSessionFeedback,
                input.params
            ),
            ("session", "end") => exec!(
                ctx,
                "llm:write",
                actions::flow::sessions::EndSession,
                input.params
            ),

            // ── session_profile ──────────────────────────────────────
            ("session_profile", "create") => exec!(
                ctx,
                "llm:write",
                actions::flow::session_profiles::CreateSessionProfile,
                input.params
            ),
            ("session_profile", "update") => exec!(
                ctx,
                "llm:write",
                actions::flow::session_profiles::UpdateSessionProfile,
                input.params
            ),
            ("session_profile", "delete") => exec!(
                ctx,
                "llm:write",
                actions::flow::session_profiles::DeleteSessionProfile,
                input.params
            ),

            // ── a2a ─────────────────────────────────────────────────
            ("a2a", "register_agent") => exec!(
                ctx,
                "project:write",
                crate::actions::facade::herd::RegisterAgent,
                input.params
            ),

            // ── unknown ─────────────────────────────────────────────
            (r, a) => anyhow::bail!(
                "Unknown resource/action: '{r}/{a}'. See tool description for valid combinations."
            ),
        }
    }
}
