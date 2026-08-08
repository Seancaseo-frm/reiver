//! Alert notification dispatch to configured channels.

use crate::db::DbPool;
use serde::Serialize;
use serde_json::Value;
use sqlx::Row;
use std::collections::BTreeMap;
use tracing::{info, warn};
use uuid::Uuid;

use super::types::AlertState;

/// Notification channel from database
#[derive(Debug, Clone)]
pub struct NotificationChannel {
    pub id: Uuid,
    pub channel_type: String,
    pub config: Value,
    pub enabled: bool,
}

/// Notification payload
#[derive(Debug, Clone, Serialize)]
pub struct AlertNotification {
    pub alert_id: Uuid,
    pub rule_id: Uuid,
    pub rule_name: String,
    pub state: AlertState,
    pub value: Option<f64>,
    pub threshold: Option<f64>,
    pub compare_op: String,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
    pub fired_at: Option<chrono::DateTime<chrono::Utc>>,
    pub resolved_at: Option<chrono::DateTime<chrono::Utc>>,
    pub is_missing: bool,
}

/// Load a notification channel by ID
pub async fn load_notification_channel(
    db: &DbPool,
    channel_id: Uuid,
) -> Result<Option<NotificationChannel>, anyhow::Error> {
    let row = sqlx::query(
        "SELECT id, channel_type, config, enabled FROM notification_channels WHERE id = $1",
    )
    .bind(channel_id)
    .fetch_optional(db)
    .await?;

    Ok(row.map(|r| NotificationChannel {
        id: r.get("id"),
        channel_type: r.get("channel_type"),
        config: r.get("config"),
        enabled: r.get("enabled"),
    }))
}

/// Send notification to a channel
pub async fn send_notification(
    channel: &NotificationChannel,
    notification: &AlertNotification,
) -> Result<(), anyhow::Error> {
    if !channel.enabled {
        warn!("Notification channel {} is disabled, skipping", channel.id);
        return Ok(());
    }

    let (url, payload, auth_header) = match channel.channel_type.as_str() {
        "slack" => {
            let encrypted_token = channel.config["bot_token"].as_str().ok_or_else(|| {
                anyhow::anyhow!("Slack channel missing bot_token — was it installed via OAuth?")
            })?;
            let channel_id = channel.config["channel_id"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("Slack channel missing channel_id"))?;

            // Decrypt bot token (requires ENCRYPTION_KEY env var)
            let bot_token = reiver_core::crypto::RotatingSecretEncryptor::from_env()
                .map_err(|e| anyhow::anyhow!("{}", e))
                .and_then(|enc| {
                    enc.decrypt(encrypted_token)
                        .map_err(|e| anyhow::anyhow!("{}", e))
                })
                .unwrap_or_else(|e| {
                    warn!("Failed to decrypt Slack bot token, using raw value: {}", e);
                    encrypted_token.to_string()
                });

            let mut slack_payload = build_slack_payload(notification);
            slack_payload["channel"] = serde_json::Value::String(channel_id.to_string());

            (
                "https://slack.com/api/chat.postMessage".to_string(),
                slack_payload,
                Some(format!("Bearer {}", bot_token)),
            )
        }
        "pagerduty" => {
            let routing_key = channel.config["routing_key"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("PagerDuty channel missing routing_key"))?;
            (
                "https://events.pagerduty.com/v2/enqueue".to_string(),
                build_pagerduty_payload(notification, routing_key),
                None,
            )
        }
        "teams" => {
            let webhook_url = channel.config["webhook_url"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("Teams channel missing webhook_url"))?;
            (
                webhook_url.to_string(),
                build_teams_payload(notification),
                None,
            )
        }
        "discord" => {
            let webhook_url = channel.config["webhook_url"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("Discord channel missing webhook_url"))?;
            (
                webhook_url.to_string(),
                build_discord_payload(notification),
                None,
            )
        }
        "webhook" => {
            let url = channel.config["url"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("Webhook channel missing url"))?;
            (url.to_string(), build_generic_payload(notification), None)
        }
        other => {
            warn!("Unknown channel type: {}", other);
            return Ok(());
        }
    };

    let client = reqwest::Client::new();
    let mut req = client.post(&url).json(&payload);
    if let Some(ref auth) = auth_header {
        req = req.header("Authorization", auth);
    }
    let response = req.send().await?;

    let status = response.status();
    let body = response.text().await.unwrap_or_default();

    if !status.is_success() {
        return Err(anyhow::anyhow!(
            "{} notification failed: {} - {}",
            channel.channel_type,
            status,
            body
        ));
    }

    // Slack API returns 200 even on errors — check the JSON `ok` field
    if channel.channel_type == "slack" {
        if let Ok(resp_json) = serde_json::from_str::<serde_json::Value>(&body) {
            if resp_json["ok"].as_bool() != Some(true) {
                let err = resp_json["error"].as_str().unwrap_or("unknown");
                return Err(anyhow::anyhow!("Slack API error: {}", err));
            }
        }
    }

    info!(
        "Sent {} notification for alert {}",
        channel.channel_type, notification.alert_id
    );
    Ok(())
}

// --- Payload builders ---

fn build_slack_payload(n: &AlertNotification) -> Value {
    let emoji = match n.state {
        AlertState::Firing => "🔴",
        AlertState::Ok => "🟢",
    };

    let title = format!("{} {}: {}", emoji, n.state.as_str(), n.rule_name);
    let value_text = format_value(n);

    let mut label_parts: Vec<String> = vec![
        format!("*Value:* {}", value_text),
        format!("*State:* {}", n.state.as_str()),
    ];
    for (key, value) in &n.labels {
        label_parts.push(format!("*{}:* {}", key, value));
    }

    let mut blocks: Vec<Value> = vec![
        serde_json::json!({"type": "header", "text": {"type": "plain_text", "text": title, "emoji": true}}),
        serde_json::json!({"type": "section", "text": {"type": "mrkdwn", "text": label_parts.join("\n")}}),
        serde_json::json!({"type": "context", "elements": [{"type": "mrkdwn", "text": format!("Alert ID: {}", n.alert_id)}]}),
    ];

    if !n.annotations.is_empty() {
        let ann_text = n
            .annotations
            .iter()
            .map(|(k, v)| format!("*{}:* {}", k, v))
            .collect::<Vec<_>>()
            .join("\n");
        blocks.insert(
            2,
            serde_json::json!({"type": "section", "text": {"type": "mrkdwn", "text": ann_text}}),
        );
    }

    serde_json::json!({
        "text": title,
        "blocks": blocks,
    })
}

fn build_pagerduty_payload(n: &AlertNotification, routing_key: &str) -> Value {
    let event_action = match n.state {
        AlertState::Firing => "trigger",
        AlertState::Ok => "resolve",
    };

    let severity = if n.is_missing { "warning" } else { "critical" };
    let dedup_key = format!("{}:{}", n.rule_id, n.alert_id);

    serde_json::json!({
        "routing_key": routing_key,
        "event_action": event_action,
        "dedup_key": dedup_key,
        "payload": {
            "summary": format!("{}: {}", n.state.as_str().to_uppercase(), n.rule_name),
            "severity": severity,
            "source": "reiver",
            "custom_details": {
                "value": n.value,
                "threshold": n.threshold,
                "labels": n.labels,
                "is_missing": n.is_missing
            }
        }
    })
}

fn build_teams_payload(n: &AlertNotification) -> Value {
    let (color, emoji) = match n.state {
        AlertState::Firing => ("FF0000", "🔴"),
        AlertState::Ok => ("00FF00", "🟢"),
    };

    let title = format!("{} {}: {}", emoji, n.state.as_str(), n.rule_name);
    let value_text = format_value(n);

    let mut facts: Vec<Value> = vec![
        serde_json::json!({"name": "Value", "value": value_text}),
        serde_json::json!({"name": "State", "value": n.state.as_str()}),
    ];

    for (key, value) in &n.labels {
        facts.push(serde_json::json!({"name": key, "value": value}));
    }

    serde_json::json!({
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": color,
        "summary": title,
        "sections": [{"activityTitle": title, "facts": facts}]
    })
}

fn build_discord_payload(n: &AlertNotification) -> Value {
    let (color, emoji) = match n.state {
        AlertState::Firing => (0xFF0000, "🔴"),
        AlertState::Ok => (0x00FF00, "🟢"),
    };

    let title = format!("{} {}: {}", emoji, n.state.as_str(), n.rule_name);
    let value_text = format_value(n);

    let mut fields: Vec<Value> = vec![
        serde_json::json!({"name": "Value", "value": value_text, "inline": true}),
        serde_json::json!({"name": "State", "value": n.state.as_str(), "inline": true}),
    ];

    for (key, value) in &n.labels {
        fields.push(serde_json::json!({"name": key, "value": value, "inline": true}));
    }

    serde_json::json!({
        "embeds": [{
            "title": title,
            "color": color,
            "fields": fields,
            "footer": {"text": format!("Alert ID: {}", n.alert_id)},
            "timestamp": chrono::Utc::now().to_rfc3339()
        }]
    })
}

fn build_generic_payload(n: &AlertNotification) -> Value {
    serde_json::json!({
        "alert_id": n.alert_id,
        "rule_id": n.rule_id,
        "rule_name": n.rule_name,
        "state": n.state.as_str(),
        "value": n.value,
        "threshold": n.threshold,
        "compare_op": n.compare_op,
        "labels": n.labels,
        "annotations": n.annotations,
        "is_missing": n.is_missing,
        "fired_at": n.fired_at,
        "resolved_at": n.resolved_at,
        "timestamp": chrono::Utc::now().to_rfc3339()
    })
}

fn format_value(n: &AlertNotification) -> String {
    if n.is_missing {
        "No data".to_string()
    } else {
        let value = n
            .value
            .map(|v| format!("{:.2}", v))
            .unwrap_or("N/A".to_string());
        let threshold = n
            .threshold
            .map(|t| format!(" {} {:.2}", n.compare_op, t))
            .unwrap_or_default();
        format!("{}{}", value, threshold)
    }
}
