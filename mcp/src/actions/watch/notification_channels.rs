use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::actions::resolve_slot;
use crate::actions::types::NotificationChannelType;
use crate::registry::ActionRegistry;

// ── Configure Notification Channel ──────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ConfigureNotificationChannelInput {
    /// Channel type
    pub channel_type: NotificationChannelType,
    /// Human-readable name for this channel
    pub name: String,
    /// Secret slot ID containing the webhook URL (Slack/Teams/Discord/webhook) or routing key (PagerDuty).
    /// Call create_secret_slot first and have the user deposit the value.
    pub secret_slot: String,
    /// Whether the channel is enabled (defaults to true)
    pub enabled: Option<bool>,
}

#[derive(Serialize)]
pub struct ConfigureNotificationChannelOutput {
    pub channel: serde_json::Value,
}

pub struct ConfigureNotificationChannel;

#[async_trait]
impl PlatformAction for ConfigureNotificationChannel {
    type Input = ConfigureNotificationChannelInput;
    type Output = ConfigureNotificationChannelOutput;

    fn name(&self) -> &'static str {
        "configure_notification_channel"
    }
    fn description(&self) -> &'static str {
        "Configure a notification channel (Slack, Teams, Discord, PagerDuty, or webhook). \
         Requires a filled secret_slot containing the webhook URL or routing key. \
         Call create_secret_slot first for each secret, wait for the user to deposit it, \
         then call this action with the slot ID."
    }
    fn required_scope(&self) -> String {
        "observability:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let resolved = resolve_slot(ctx, &input.secret_slot).await?;
        let enabled = input.enabled.unwrap_or(true);
        let channel_str = serde_json::to_value(&input.channel_type)?;
        let channel_name = channel_str.as_str().unwrap_or_default();

        let payload = match channel_name {
            "pagerduty" => serde_json::json!({
                "name": input.name,
                "routing_key": resolved,
                "enabled": enabled,
            }),
            "slack" | "teams" | "discord" | "webhook" => serde_json::json!({
                "name": input.name,
                "webhook_url": resolved,
                "enabled": enabled,
            }),
            other => anyhow::bail!("Unsupported channel type: {other}. Use slack, teams, discord, pagerduty, or webhook."),
        };

        let path = format!("/api/{}/integrations", channel_name);
        let resp = ctx.http.watch_post(&path, &payload).await?;
        let channel = resp.json().await?;
        Ok(ConfigureNotificationChannelOutput { channel })
    }
}

// ── Configure ServiceNow Integration ────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ConfigureServiceNowInput {
    /// Human-readable name for this integration
    pub name: String,
    /// ServiceNow instance URL (e.g. "https://mycompany.service-now.com")
    pub instance_url: String,
    /// ServiceNow username
    pub username: String,
    /// Secret slot ID containing the ServiceNow password.
    /// Call create_secret_slot first and have the user deposit the password.
    pub password_slot: String,
    /// Whether the integration is enabled (defaults to true)
    pub enabled: Option<bool>,
}

#[derive(Serialize)]
pub struct ConfigureServiceNowOutput {
    pub integration: serde_json::Value,
}

pub struct ConfigureServiceNow;

#[async_trait]
impl PlatformAction for ConfigureServiceNow {
    type Input = ConfigureServiceNowInput;
    type Output = ConfigureServiceNowOutput;

    fn name(&self) -> &'static str {
        "configure_servicenow_integration"
    }
    fn description(&self) -> &'static str {
        "Configure a ServiceNow integration for incident management. \
         Requires a filled password_slot containing the ServiceNow password. \
         Call create_secret_slot first for the password, wait for the user to deposit it, \
         then call this action with the slot ID."
    }
    fn required_scope(&self) -> String {
        "observability:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let password = resolve_slot(ctx, &input.password_slot).await?;

        let payload = serde_json::json!({
            "name": input.name,
            "instance_url": input.instance_url,
            "username": input.username,
            "password": password,
            "enabled": input.enabled.unwrap_or(true),
        });

        let resp = ctx
            .http
            .watch_post("/api/servicenow/integrations", &payload)
            .await?;
        let integration = resp.json().await?;
        Ok(ConfigureServiceNowOutput { integration })
    }
}

// ── Update Notification Channel ──────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct UpdateNotificationChannelInput {
    /// ID of the channel to update
    pub channel_id: String,
    /// Updated channel name
    pub name: Option<String>,
    /// Whether the channel is enabled
    pub enabled: Option<bool>,
    /// Updated channel configuration (webhook_url, routing_key, etc. — varies by channel type)
    pub config: Option<serde_json::Value>,
}

#[derive(Serialize)]
pub struct UpdateNotificationChannelOutput {
    pub channel: serde_json::Value,
}

pub struct UpdateNotificationChannel;

#[async_trait]
impl PlatformAction for UpdateNotificationChannel {
    type Input = UpdateNotificationChannelInput;
    type Output = UpdateNotificationChannelOutput;

    fn name(&self) -> &'static str {
        "update_notification_channel"
    }
    fn description(&self) -> &'static str {
        "Update a notification channel's name, enabled status, or configuration. \
         Use list_notification_channels to get channel IDs and current config."
    }
    fn required_scope(&self) -> String {
        "observability:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let mut body = serde_json::Map::new();
        if let Some(n) = input.name {
            body.insert("name".into(), serde_json::Value::String(n));
        }
        if let Some(e) = input.enabled {
            body.insert("enabled".into(), serde_json::Value::Bool(e));
        }
        if let Some(c) = input.config {
            body.insert("config".into(), c);
        }
        let resp = ctx
            .http
            .watch_put(
                &format!("/api/notification-channels/{}", input.channel_id),
                &serde_json::Value::Object(body),
            )
            .await?;
        let channel = resp.json().await?;
        Ok(UpdateNotificationChannelOutput { channel })
    }
}

// ── Registration ─────────────────────────────────────────────────────

pub fn register(registry: &mut ActionRegistry) {
    registry.register(ConfigureNotificationChannel);
    registry.register(ConfigureServiceNow);
    registry.register(UpdateNotificationChannel);
}
