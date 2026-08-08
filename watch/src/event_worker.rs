//! Platform event worker — the single notification dispatcher for all
//! event-driven notifications.
//!
//! All services (Flow, Watch, Pond) emit `PlatformEvent`s to Kafka.
//! This worker consumes them, deduplicates using Redis `SET NX EX`
//! (keyed by the emitter-defined `dedup_key`), and dispatches
//! notifications to the project's configured channels.
//!
//! Threshold-based alerts (ClickHouse polling) are handled separately
//! by the alert worker — see `alert_worker.rs`.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tracing::{error, info, warn};
use uuid::Uuid;

use reiver_core::events::{PlatformEvent, PlatformEventType};

use crate::alerts::{send_notification, AlertNotification, AlertState, NotificationChannel};
use crate::app_state::RedisPool;
use crate::db::DbPool;

/// Default cooldown for notification dedup (seconds).
const NOTIFICATION_COOLDOWN_SECONDS: u64 = 3600;

static HTTP_CLIENT: std::sync::LazyLock<reqwest::Client> = std::sync::LazyLock::new(|| {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("Failed to create HTTP client")
});

struct KafkaConsumerContext;
impl rdkafka::ClientContext for KafkaConsumerContext {}
impl rdkafka::consumer::ConsumerContext for KafkaConsumerContext {}

pub async fn start_event_worker(
    kafka_hosts: &str,
    platform_events_topic: &str,
    client_id: Option<&str>,
    flow_url: String,
    db_pool: Arc<DbPool>,
    redis_pool: Arc<RedisPool>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<JoinHandle<()>> {
    info!(
        "Creating Kafka consumer for platform events topic: {}",
        platform_events_topic
    );

    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", kafka_hosts)
        .set("group.id", "reiver-event-worker")
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "5000")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "30000")
        .set("enable.partition.eof", "false");

    if let Some(client_id) = client_id {
        client_config.set("client.id", client_id);
    }

    let consumer: StreamConsumer<KafkaConsumerContext> =
        client_config.create_with_context(KafkaConsumerContext)?;

    consumer.subscribe(&[platform_events_topic])?;
    info!(
        "Subscribed to platform events topic: {}",
        platform_events_topic
    );

    let handle = tokio::spawn(async move {
        info!("Platform event worker started");

        let mut message_stream = consumer.stream();

        loop {
            tokio::select! {
                message_opt = message_stream.next() => {
                    let Some(message) = message_opt else { break; };
                    match message {
                        Ok(m) => {
                            let Some(payload) = m.payload() else { continue; };
                            let event: PlatformEvent = match serde_json::from_slice(payload) {
                                Ok(e) => e,
                                Err(e) => {
                                    warn!("Failed to deserialize platform event: {}", e);
                                    continue;
                                }
                            };

                            if let Err(e) = process_event(&event, &flow_url, &db_pool, &redis_pool).await {
                                error!(
                                    event_id = %event.id,
                                    event_type = %event.event_type,
                                    "Failed to process platform event: {}", e
                                );
                            }
                        }
                        Err(e) => {
                            error!("Kafka consumer error: {}", e);
                        }
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Event worker received shutdown signal");
                        break;
                    }
                }
            }
        }
        info!("Platform event worker stopped");
    });

    Ok(handle)
}

// ============================================================================
// Event routing
// ============================================================================

async fn process_event(
    event: &PlatformEvent,
    flow_url: &str,
    db_pool: &DbPool,
    redis: &RedisPool,
) -> Result<()> {
    match &event.event_type {
        PlatformEventType::ScheduledPricingSync => {
            handle_scheduled_pricing_sync(event, flow_url).await
        }

        PlatformEventType::ProviderKeyError => {
            if is_duplicate_notification(redis, event.project_id, &event.dedup_key).await {
                return Ok(());
            }
            handle_provider_key_error(event, db_pool).await
        }

        PlatformEventType::ExceptionGroupCreated => {
            if is_duplicate_notification(redis, event.project_id, &event.dedup_key).await {
                return Ok(());
            }
            handle_exception_event(event, db_pool, false).await
        }

        PlatformEventType::ExceptionGroupRegressed => {
            if is_duplicate_notification(redis, event.project_id, &event.dedup_key).await {
                return Ok(());
            }
            handle_exception_event(event, db_pool, true).await
        }

        PlatformEventType::RolloutRolledBack => {
            if is_duplicate_notification(redis, event.project_id, &event.dedup_key).await {
                return Ok(());
            }
            handle_rollout_rolled_back(event, db_pool).await
        }

        PlatformEventType::InvestigationCompleted => {
            if is_duplicate_notification(redis, event.project_id, &event.dedup_key).await {
                return Ok(());
            }
            handle_investigation_completed(event, db_pool).await
        }

        _ => Ok(()),
    }
}

// ============================================================================
// Redis dedup
// ============================================================================

/// Check if a notification with this dedup key was already sent recently.
///
/// Uses Redis `SET NX EX` — atomic check-and-set with TTL. Returns `true`
/// if the key already existed (duplicate), `false` if this is a new event
/// (key was set).
///
/// On Redis errors, returns `false` (fail open — better to send a duplicate
/// than silently drop a notification).
async fn is_duplicate_notification(redis: &RedisPool, project_id: Uuid, dedup_key: &str) -> bool {
    if dedup_key.is_empty() {
        return false;
    }

    let key = format!("notify_dedup:{}:{}", project_id, dedup_key);

    let mut conn = match redis.get().await {
        Ok(c) => c,
        Err(e) => {
            warn!("Redis connection failed for dedup check, proceeding: {}", e);
            return false;
        }
    };

    let result: Option<()> = redis::cmd("SET")
        .arg(&key)
        .arg("1")
        .arg("NX")
        .arg("EX")
        .arg(NOTIFICATION_COOLDOWN_SECONDS)
        .query_async(&mut *conn)
        .await
        .unwrap_or(None);

    if result.is_none() {
        info!(
            project_id = %project_id,
            dedup_key,
            "Suppressing duplicate notification (cooldown)"
        );
        return true;
    }

    false
}

// ============================================================================
// Shared notification dispatch
// ============================================================================

/// Load all enabled notification channels for a project and send a notification
/// to each one. Errors on individual channels are logged but don't fail the
/// overall dispatch.
async fn send_to_project_channels(
    db_pool: &DbPool,
    project_id: Uuid,
    notification: &AlertNotification,
) -> Result<()> {
    let channels: Vec<(Uuid, String, serde_json::Value, bool)> = sqlx::query_as(
        "SELECT id, channel_type, config, enabled \
         FROM notification_channels \
         WHERE project_id = $1 AND enabled = true",
    )
    .bind(project_id)
    .fetch_all(db_pool)
    .await
    .context("Failed to load notification channels")?;

    if channels.is_empty() {
        info!(
            project_id = %project_id,
            "No notification channels configured, skipping notification"
        );
        return Ok(());
    }

    for (id, channel_type, config, enabled) in channels {
        let channel = NotificationChannel {
            id,
            channel_type: channel_type.clone(),
            config,
            enabled,
        };

        if let Err(e) = send_notification(&channel, &notification).await {
            warn!(
                channel_id = %id,
                channel_type,
                "Failed to send notification: {}", e
            );
        }
    }

    Ok(())
}

/// Build an `AlertNotification` with common fields populated.
fn build_notification(
    event: &PlatformEvent,
    rule_name: String,
    labels: BTreeMap<String, String>,
    annotations: BTreeMap<String, String>,
) -> AlertNotification {
    AlertNotification {
        alert_id: event.id,
        rule_id: event.id,
        rule_name,
        state: AlertState::Firing,
        value: None,
        threshold: None,
        compare_op: String::new(),
        labels,
        annotations,
        fired_at: Some(event.timestamp),
        resolved_at: None,
        is_missing: false,
    }
}

/// Truncate a string to at most `max_bytes` bytes without splitting a
/// multi-byte UTF-8 character.
fn truncate_utf8(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while !s.is_char_boundary(end) && end > 0 {
        end -= 1;
    }
    &s[..end]
}

// ============================================================================
// Event handlers
// ============================================================================

async fn handle_scheduled_pricing_sync(event: &PlatformEvent, flow_url: &str) -> Result<()> {
    info!(
        event_id = %event.id,
        project_id = %event.project_id,
        "Dispatching scheduled pricing sync to agent-task"
    );
    let body = serde_json::json!({
        "project_id": event.project_id,
        "task_type": "pricing_sync",
        "task_ref": event.id.to_string(),
        "prompt": "",
        "context": event.payload,
        "internal": true,
    });
    let url = format!("{flow_url}/api/internal/agent-task");
    let resp = HTTP_CLIENT
        .post(&url)
        .header("X-Project-Id", event.project_id.to_string())
        .json(&body)
        .send()
        .await
        .context("agent-task HTTP request to Flow failed")?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        warn!(
            event_id = %event.id,
            %status,
            "agent-task returned non-OK: {}", body
        );
    }
    Ok(())
}

async fn handle_provider_key_error(event: &PlatformEvent, db_pool: &DbPool) -> Result<()> {
    let provider = event.payload["provider"].as_str().unwrap_or("unknown");
    let model = event.payload["model"].as_str().unwrap_or("unknown");
    let status = event.payload["status"].as_u64().unwrap_or(0) as u16;
    let message = event.payload["message"].as_str().unwrap_or("");

    info!(
        event_id = %event.id,
        project_id = %event.project_id,
        provider,
        model,
        status,
        "Processing provider key error event"
    );

    let summary = match status {
        401 => format!(
            "Your API key for {} is invalid or expired. Update it in project settings.",
            provider
        ),
        402 => format!(
            "Your {} account has insufficient balance. Top up your account or switch providers.",
            provider
        ),
        403 => format!(
            "Your API key for {} lacks required permissions. Check your provider account settings.",
            provider
        ),
        404 => format!(
            "Model '{}' was not found at {}. Verify the model name is correct.",
            model, provider
        ),
        _ => format!("Provider {} returned error {}: {}", provider, status, message),
    };

    let mut labels = BTreeMap::new();
    labels.insert("provider".to_string(), provider.to_string());
    labels.insert("model".to_string(), model.to_string());
    labels.insert("status".to_string(), status.to_string());

    let mut annotations = BTreeMap::new();
    annotations.insert("summary".to_string(), summary);
    if !message.is_empty() {
        annotations.insert("provider_message".to_string(), message.to_string());
    }

    let notification = build_notification(
        event,
        format!("Provider Key Error: {} ({})", provider, status),
        labels,
        annotations,
    );

    send_to_project_channels(db_pool, event.project_id, &notification).await
}

async fn handle_exception_event(
    event: &PlatformEvent,
    db_pool: &DbPool,
    is_regression: bool,
) -> Result<()> {
    let fingerprint = event.payload["fingerprint"].as_str().unwrap_or("unknown");
    let message = event.payload["message"].as_str().unwrap_or("Unknown error");
    let exception_type = event.payload["exception_type"].as_str();
    let exception_value = event.payload["exception_value"].as_str();

    let kind = if is_regression { "Regression" } else { "New" };
    info!(
        event_id = %event.id,
        project_id = %event.project_id,
        fingerprint,
        kind,
        "Processing exception event"
    );

    let summary = if is_regression {
        format!("Previously resolved error has reoccurred: {}", message)
    } else {
        format!("New error detected: {}", message)
    };

    let mut labels = BTreeMap::new();
    labels.insert("fingerprint".to_string(), fingerprint.to_string());
    if let Some(t) = exception_type {
        labels.insert("exception_type".to_string(), t.to_string());
    }

    let mut annotations = BTreeMap::new();
    annotations.insert("summary".to_string(), summary);
    if let Some(v) = exception_value {
        annotations.insert("exception_value".to_string(), v.to_string());
    }

    let rule_name = if is_regression {
        format!("Exception Regression: {}", message)
    } else {
        format!("New Exception: {}", message)
    };

    let notification = build_notification(event, rule_name, labels, annotations);
    send_to_project_channels(db_pool, event.project_id, &notification).await
}

async fn handle_rollout_rolled_back(event: &PlatformEvent, db_pool: &DbPool) -> Result<()> {
    let rollout_name = event.payload["rollout_name"].as_str().unwrap_or("unknown");
    let reason = event.payload["reason"].as_str().unwrap_or("unknown");
    let rollout_id = event.payload["rollout_id"].as_str().unwrap_or("unknown");

    info!(
        event_id = %event.id,
        project_id = %event.project_id,
        rollout_name,
        "Processing rollout rollback event"
    );

    let mut labels = BTreeMap::new();
    labels.insert("rollout_id".to_string(), rollout_id.to_string());
    labels.insert("rollout_name".to_string(), rollout_name.to_string());

    let mut annotations = BTreeMap::new();
    annotations.insert(
        "summary".to_string(),
        format!(
            "Prompt rollout '{}' was automatically rolled back: {}",
            rollout_name, reason
        ),
    );
    annotations.insert("reason".to_string(), reason.to_string());

    if let Some(target_error_rate) = event.payload["target_error_rate"].as_f64() {
        labels.insert(
            "target_error_rate".to_string(),
            format!("{:.2}%", target_error_rate),
        );
    }
    if let Some(baseline_error_rate) = event.payload["baseline_error_rate"].as_f64() {
        labels.insert(
            "baseline_error_rate".to_string(),
            format!("{:.2}%", baseline_error_rate),
        );
    }

    let notification = build_notification(
        event,
        format!("Rollout Auto-Rollback: {}", rollout_name),
        labels,
        annotations,
    );

    send_to_project_channels(db_pool, event.project_id, &notification).await
}

async fn handle_investigation_completed(event: &PlatformEvent, db_pool: &DbPool) -> Result<()> {
    let investigation_id = event.payload["investigation_id"]
        .as_str()
        .unwrap_or("unknown");
    let trigger_summary = event.payload["trigger_summary"]
        .as_str()
        .unwrap_or("Investigation");
    let findings = event.payload["findings"].as_str().unwrap_or("");

    info!(
        event_id = %event.id,
        project_id = %event.project_id,
        investigation_id,
        "Processing investigation completed event"
    );

    let mut labels = BTreeMap::new();
    labels.insert("investigation_id".to_string(), investigation_id.to_string());

    let mut annotations = BTreeMap::new();
    annotations.insert(
        "summary".to_string(),
        format!("Investigation complete: {}", trigger_summary),
    );
    if !findings.is_empty() {
        let truncated = truncate_utf8(findings, 2000);
        annotations.insert("findings".to_string(), truncated.to_string());
    }

    let notification = build_notification(
        event,
        format!("MooDeng Investigation: {}", trigger_summary),
        labels,
        annotations,
    );

    send_to_project_channels(db_pool, event.project_id, &notification).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_notification_sets_fields() {
        let event = PlatformEvent {
            id: Uuid::new_v4(),
            event_type: PlatformEventType::ProviderKeyError,
            project_id: Uuid::new_v4(),
            timestamp: chrono::Utc::now(),
            source: reiver_core::events::EventSource::Flow,
            payload: serde_json::json!({}),
            dedup_key: "test:key".to_string(),
        };

        let mut labels = BTreeMap::new();
        labels.insert("k".to_string(), "v".to_string());
        let mut annotations = BTreeMap::new();
        annotations.insert("summary".to_string(), "test".to_string());

        let n = build_notification(&event, "Test Rule".to_string(), labels, annotations);

        assert_eq!(n.alert_id, event.id);
        assert_eq!(n.rule_name, "Test Rule");
        assert_eq!(n.labels["k"], "v");
        assert_eq!(n.annotations["summary"], "test");
        assert!(matches!(n.state, AlertState::Firing));
        assert!(n.fired_at.is_some());
        assert!(n.resolved_at.is_none());
        assert!(!n.is_missing);
    }

    #[test]
    fn test_truncate_utf8_ascii() {
        let s = "hello world";
        assert_eq!(truncate_utf8(s, 5), "hello");
        assert_eq!(truncate_utf8(s, 100), "hello world");
    }

    #[test]
    fn test_truncate_utf8_multibyte_boundary() {
        let s = "aé"; // 'a' = 1 byte, 'é' = 2 bytes, total 3 bytes
        assert_eq!(truncate_utf8(s, 3), "aé");
        assert_eq!(truncate_utf8(s, 2), "a"); // byte 2 is mid-char, backs up to 1
        assert_eq!(truncate_utf8(s, 1), "a");
    }

    #[test]
    fn test_truncate_utf8_emoji() {
        let s = "x🐛y"; // 'x' 1B, '🐛' 4B, 'y' 1B = 6B total
        assert_eq!(truncate_utf8(s, 6), "x🐛y");
        assert_eq!(truncate_utf8(s, 5), "x🐛");
        assert_eq!(truncate_utf8(s, 4), "x"); // bytes 2-4 are mid-emoji
        assert_eq!(truncate_utf8(s, 1), "x");
    }
}
