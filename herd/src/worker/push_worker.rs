//! Push notification delivery worker.
//!
//! Subscribes to both `reiver.a2a.push` and `reiver.a2a.push.retry`.
//! Uses consumer-side pause/resume for retry delays (Redpanda has no native delay).

use chrono::Utc;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

use crate::kafka::KafkaProducer;

const A2A_PUSH_TOPIC: &str = "reiver.a2a.push";
const A2A_PUSH_RETRY_TOPIC: &str = "reiver.a2a.push.retry";
const CONSUMER_GROUP: &str = "reiver-herd-push-worker";
const MAX_BACKOFF_SECS: u64 = 3600; // 1 hour cap
const TTL_DAYS: i64 = 7;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PushTrigger {
    task_id: String,
    webhook_url: String,
    auth_scheme: Option<String>,
    auth_credentials: Option<String>,
    payload: serde_json::Value,
    retry_after: chrono::DateTime<Utc>,
    attempt: u32,
    created_at: chrono::DateTime<Utc>,
}

pub fn start_push_worker(
    kafka_hosts: &str,
    client_id: Option<&str>,
    kafka_producer: Arc<KafkaProducer>,
    http_client: reqwest::Client,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> JoinHandle<()> {
    let hosts = kafka_hosts.to_string();
    let cid = client_id.map(|s| s.to_string());

    tokio::spawn(async move {
        let consumer = create_consumer(&hosts, cid.as_deref());
        let consumer = match consumer {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("Failed to create push worker consumer: {}", e);
                return;
            }
        };

        if let Err(e) = consumer.subscribe(&[A2A_PUSH_TOPIC, A2A_PUSH_RETRY_TOPIC]) {
            tracing::error!("Failed to subscribe to push topics: {}", e);
            return;
        }

        tracing::info!("Push notification worker started");

        let mut stream = consumer.stream();
        loop {
            tokio::select! {
                msg = stream.next() => {
                    match msg {
                        Some(Ok(borrowed)) => {
                            if let Some(payload) = borrowed.payload() {
                                process_push(payload, &kafka_producer, &http_client).await;
                            }
                        }
                        Some(Err(e)) => {
                            tracing::error!("Push worker consumer error: {}", e);
                        }
                        None => break,
                    }
                }
                _ = shutdown_rx.changed() => {
                    tracing::info!("Push worker shutting down");
                    break;
                }
            }
        }
    })
}

fn create_consumer(
    hosts: &str,
    client_id: Option<&str>,
) -> Result<StreamConsumer, rdkafka::error::KafkaError> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", hosts)
        .set("group.id", CONSUMER_GROUP)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "5000")
        .set("fetch.min.bytes", "1")
        .set("fetch.wait.max.ms", "500");

    if let Some(id) = client_id {
        config.set("client.id", id);
    }

    config.create()
}

async fn process_push(
    payload: &[u8],
    kafka_producer: &KafkaProducer,
    http_client: &reqwest::Client,
) {
    let trigger: PushTrigger = match serde_json::from_slice(payload) {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!("Failed to deserialize push trigger: {}", e);
            return;
        }
    };

    // Check TTL: drop if older than 7 days
    let age = Utc::now() - trigger.created_at;
    if age.num_days() >= TTL_DAYS {
        tracing::debug!(
            task_id = %trigger.task_id,
            age_days = age.num_days(),
            "Push notification expired (TTL), dropping"
        );
        return;
    }

    // Check retry_after: if not yet due, sleep and retry
    let now = Utc::now();
    if trigger.retry_after > now {
        let delay = (trigger.retry_after - now)
            .to_std()
            .unwrap_or(Duration::from_secs(1));
        let capped_delay = delay.min(Duration::from_secs(60));
        tokio::time::sleep(capped_delay).await;
    }

    // Attempt delivery
    let mut request = http_client
        .post(&trigger.webhook_url)
        .header("Content-Type", "application/json");

    if let (Some(ref scheme), Some(ref creds)) = (&trigger.auth_scheme, &trigger.auth_credentials) {
        request = request.header("Authorization", format!("{} {}", scheme, creds));
    }

    // Add HMAC signature
    let payload_bytes = match serde_json::to_vec(&trigger.payload) {
        Ok(bytes) => bytes,
        Err(e) => {
            tracing::error!(task_id = %trigger.task_id, "Failed to serialize push payload: {}", e);
            return;
        }
    };
    let signature = compute_hmac(&trigger.task_id, &payload_bytes);
    request = request.header("X-A2A-Signature", &signature);

    match request
        .body(payload_bytes)
        .timeout(Duration::from_secs(30))
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => {
            tracing::debug!(
                task_id = %trigger.task_id,
                attempt = trigger.attempt,
                status = resp.status().as_u16(),
                "Push notification delivered"
            );
        }
        Ok(resp) => {
            tracing::warn!(
                task_id = %trigger.task_id,
                attempt = trigger.attempt,
                status = resp.status().as_u16(),
                "Push notification failed, scheduling retry"
            );
            schedule_retry(kafka_producer, trigger).await;
        }
        Err(e) => {
            tracing::warn!(
                task_id = %trigger.task_id,
                attempt = trigger.attempt,
                error = %e,
                "Push notification delivery error, scheduling retry"
            );
            schedule_retry(kafka_producer, trigger).await;
        }
    }
}

async fn schedule_retry(kafka_producer: &KafkaProducer, mut trigger: PushTrigger) {
    // Exponential backoff: 5s, 10s, 20s, 40s, ... capped at 1h
    let backoff_secs = (5u64 * 2u64.pow(trigger.attempt)).min(MAX_BACKOFF_SECS);
    trigger.attempt += 1;
    trigger.retry_after = Utc::now() + chrono::Duration::seconds(backoff_secs as i64);

    if let Ok(bytes) = serde_json::to_vec(&trigger) {
        if let Err(e) = kafka_producer
            .send_to_topic(A2A_PUSH_RETRY_TOPIC, &trigger.task_id, &bytes)
            .await
        {
            tracing::error!(
                task_id = %trigger.task_id,
                "Failed to schedule push retry: {}", e
            );
        }
    }
}

fn compute_hmac(task_id: &str, payload: &[u8]) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    // In production, this key should come from configuration
    let key = std::env::var("HERD_PUSH_HMAC_KEY").unwrap_or_else(|_| task_id.to_string());

    let mut mac =
        Hmac::<Sha256>::new_from_slice(key.as_bytes()).expect("HMAC can take key of any size");
    mac.update(payload);
    hex::encode(mac.finalize().into_bytes())
}
