use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::actions::types::{CreateAlertRuleData, UpdateAlertRuleData};
use crate::registry::ActionRegistry;

// ── List Alert Rules ────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListAlertRulesInput {}

#[derive(Serialize)]
pub struct ListAlertRulesOutput {
    pub rules: serde_json::Value,
}

pub struct ListAlertRules;

#[async_trait]
impl PlatformAction for ListAlertRules {
    type Input = ListAlertRulesInput;
    type Output = ListAlertRulesOutput;

    fn name(&self) -> &'static str {
        "list_alert_rules"
    }
    fn description(&self) -> &'static str {
        "List all alert rules for the current project. Returns each rule's name, condition, \
         threshold, enabled status, and linked notification channel IDs. \
         Use get_alert_rule to see full details for a specific rule."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        _input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let resp = ctx
            .http
            .website_get(&format!("/api/alerting/rules?project_id={pid}"))
            .await?;
        let rules = resp.json().await?;
        Ok(ListAlertRulesOutput { rules })
    }
}

// ── Get Alert Rule ──────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetAlertRuleInput {
    /// ID of the alert rule to retrieve
    pub rule_id: String,
}

#[derive(Serialize)]
pub struct GetAlertRuleOutput {
    pub rule: serde_json::Value,
}

pub struct GetAlertRule;

#[async_trait]
impl PlatformAction for GetAlertRule {
    type Input = GetAlertRuleInput;
    type Output = GetAlertRuleOutput;

    fn name(&self) -> &'static str {
        "get_alert_rule"
    }
    fn description(&self) -> &'static str {
        "Get the full definition of a specific alert rule by ID. Returns the query config, \
         threshold, evaluation window, notification channels, and current state. \
         Use this before update_alert_rule to see current values."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let resp = ctx
            .http
            .website_get(&format!(
                "/api/alerting/rules/{}?project_id={pid}",
                input.rule_id
            ))
            .await?;
        let rule = resp.json().await?;
        Ok(GetAlertRuleOutput { rule })
    }
}

// ── Create Alert Rule ───────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct CreateAlertRuleInput {
    /// The alert rule definition
    pub rule: CreateAlertRuleData,
}

#[derive(Serialize)]
pub struct CreateAlertRuleOutput {
    pub rule: serde_json::Value,
}

pub struct CreateAlertRule;

#[async_trait]
impl PlatformAction for CreateAlertRule {
    type Input = CreateAlertRuleInput;
    type Output = CreateAlertRuleOutput;

    fn name(&self) -> &'static str {
        "create_alert_rule"
    }
    fn description(&self) -> &'static str {
        "Create a new alert rule that monitors a metric and fires when the threshold is breached. \
         Requires a name, query_config (defining what to monitor), and threshold. \
         Link notification channels by UUID from list_notification_channels to receive alerts \
         via Slack, Teams, PagerDuty, etc."
    }
    fn required_scope(&self) -> String {
        "observability:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let mut body = serde_json::to_value(&input.rule)?;
        if let serde_json::Value::Object(ref mut map) = body {
            map.insert("project_id".to_string(), serde_json::json!(pid));
        }
        let resp = ctx.http.website_post("/api/alerting/rules", &body).await?;
        let rule = resp.json().await?;
        Ok(CreateAlertRuleOutput { rule })
    }
}

// ── Update Alert Rule ───────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct UpdateAlertRuleInput {
    /// ID of the alert rule to update
    pub rule_id: String,
    /// Fields to update (only provided fields are changed)
    pub rule: UpdateAlertRuleData,
}

#[derive(Serialize)]
pub struct UpdateAlertRuleOutput {
    pub rule: serde_json::Value,
}

pub struct UpdateAlertRule;

#[async_trait]
impl PlatformAction for UpdateAlertRule {
    type Input = UpdateAlertRuleInput;
    type Output = UpdateAlertRuleOutput;

    fn name(&self) -> &'static str {
        "update_alert_rule"
    }
    fn description(&self) -> &'static str {
        "Update an existing alert rule. Only the provided fields are changed; omitted fields \
         keep their current values. Use get_alert_rule first to see the current configuration."
    }
    fn required_scope(&self) -> String {
        "observability:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let body = serde_json::to_value(&input.rule)?;
        let resp = ctx
            .http
            .website_put(&format!("/api/alerting/rules/{}", input.rule_id), &body)
            .await?;
        let rule = resp.json().await?;
        Ok(UpdateAlertRuleOutput { rule })
    }
}

// ── Delete Alert Rule ───────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct DeleteAlertRuleInput {
    /// ID of the alert rule to delete
    pub rule_id: String,
}

#[derive(Serialize)]
pub struct DeleteAlertRuleOutput {
    pub result: serde_json::Value,
}

pub struct DeleteAlertRule;

#[async_trait]
impl PlatformAction for DeleteAlertRule {
    type Input = DeleteAlertRuleInput;
    type Output = DeleteAlertRuleOutput;

    fn name(&self) -> &'static str {
        "delete_alert_rule"
    }
    fn description(&self) -> &'static str {
        "Permanently delete an alert rule. This stops all evaluation and removes the rule's history. \
         This action should be explicitly requested by the user."
    }
    fn required_scope(&self) -> String {
        "observability:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let resp = ctx
            .http
            .website_delete(&format!(
                "/api/alerting/rules/{}?project_id={pid}",
                input.rule_id
            ))
            .await?;
        let result = resp.json().await?;
        Ok(DeleteAlertRuleOutput { result })
    }
}

// ── List Alerts ─────────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListAlertsInput {}

#[derive(Serialize)]
pub struct ListAlertsOutput {
    pub alerts: serde_json::Value,
}

pub struct ListAlerts;

#[async_trait]
impl PlatformAction for ListAlerts {
    type Input = ListAlertsInput;
    type Output = ListAlertsOutput;

    fn name(&self) -> &'static str {
        "list_alerts"
    }
    fn description(&self) -> &'static str {
        "List all fired alerts for the current project. Returns triggered alert instances \
         with timestamps, the rule that fired, severity, and current state (active/resolved)."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        _input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let resp = ctx
            .http
            .website_get(&format!("/api/alerting/alerts?project_id={pid}"))
            .await?;
        let alerts = resp.json().await?;
        Ok(ListAlertsOutput { alerts })
    }
}

// ── List Notification Channels ──────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListNotificationChannelsInput {}

#[derive(Serialize)]
pub struct ListNotificationChannelsOutput {
    pub channels: serde_json::Value,
}

pub struct ListNotificationChannels;

#[async_trait]
impl PlatformAction for ListNotificationChannels {
    type Input = ListNotificationChannelsInput;
    type Output = ListNotificationChannelsOutput;

    fn name(&self) -> &'static str {
        "list_notification_channels"
    }
    fn description(&self) -> &'static str {
        "List notification channels configured for the current project. Returns each channel's \
         ID, type (Slack/Teams/etc.), name, and enabled status. Use the channel IDs when \
         creating alert rules to specify where alerts are sent."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        _input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let resp = ctx
            .http
            .website_get(&format!("/api/notification-channels?project_id={pid}"))
            .await?;
        let channels = resp.json().await?;
        Ok(ListNotificationChannelsOutput { channels })
    }
}

// ── Test Notification ───────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct TestNotificationInput {
    /// Notification channel ID to send the test message to
    pub channel_id: String,
}

#[derive(Serialize)]
pub struct TestNotificationOutput {
    pub success: bool,
}

pub struct TestNotification;

#[async_trait]
impl PlatformAction for TestNotification {
    type Input = TestNotificationInput;
    type Output = TestNotificationOutput;

    fn name(&self) -> &'static str {
        "test_notification"
    }
    fn description(&self) -> &'static str {
        "Send a test message to a notification channel to verify it is configured correctly. \
         Returns success if the test message was delivered."
    }
    fn required_scope(&self) -> String {
        "observability:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let body = serde_json::json!({
            "project_id": ctx.project_id,
            "channel_id": input.channel_id,
        });
        ctx.http
            .watch_post("/api/alerting/test-notification", &body)
            .await?;
        Ok(TestNotificationOutput { success: true })
    }
}

// ── Registration ─────────────────────────────────────────────────────

pub fn register(registry: &mut ActionRegistry) {
    registry.register(ListAlertRules);
    registry.register(GetAlertRule);
    registry.register(CreateAlertRule);
    registry.register(UpdateAlertRule);
    registry.register(DeleteAlertRule);
    registry.register(ListAlerts);
    registry.register(ListNotificationChannels);
    registry.register(TestNotification);
}
