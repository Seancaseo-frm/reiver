//! Redpanda consumer for A2A messages.
//!
//! Consumes from `reiver.a2a.messages`:
//! 1. Batch-INSERT tasks + messages + request log to ClickHouse via Inserter
//! 2. Forward to target agent
//! 3. For tasks with push configs, produce to `reiver.a2a.push`

use chrono::Utc;
use clickhouse::inserter::Inserter;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use uuid::Uuid;

use crate::clickhouse_db::ClickHousePool;
use crate::kafka::KafkaProducer;
use crate::routing_cache::RoutingCache;

const A2A_MESSAGES_TOPIC: &str = "reiver.a2a.messages";
const A2A_PUSH_TOPIC: &str = "reiver.a2a.push";
const CONSUMER_GROUP: &str = "reiver-herd-message-worker";

const BATCH_MAX_ROWS: u64 = 500;
const BATCH_PERIOD: Duration = Duration::from_secs(1);

// ── Kafka envelope types ──

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct A2aMessageEnvelope {
    task_id: String,
    context_id: Option<String>,
    source_agent_id: Uuid,
    target_agent_id: Uuid,
    source_org_id: Uuid,
    target_org_id: Uuid,
    method: String,
    message: serde_json::Value,
    #[allow(dead_code)]
    configuration: Option<serde_json::Value>,
    metadata: Option<serde_json::Value>,
    pipeline_flags: Option<PipelineFlags>,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PipelineFlags {
    pii_redacted: bool,
    injection_flagged: bool,
}

/// Push notification trigger envelope.
#[derive(Debug, Serialize)]
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

// ── ClickHouse row structs for batch inserts ──

#[derive(Debug, clickhouse::Row, Serialize)]
struct TaskRow {
    task_id: Uuid,
    context_id: Option<Uuid>,
    source_agent_id: Uuid,
    target_agent_id: Uuid,
    source_org_id: Uuid,
    target_org_id: Uuid,
    status: String,
    metadata: String,
    artifacts: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    updated_at: chrono::DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    created_at: chrono::DateTime<Utc>,
}

#[derive(Debug, Clone, clickhouse::Row, Serialize)]
struct MessageRow {
    message_id: Uuid,
    task_id: Uuid,
    context_id: Option<Uuid>,
    role: String,
    parts: String,
    reference_task_ids: Vec<Uuid>,
    metadata: String,
    pipeline_flags: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    created_at: chrono::DateTime<Utc>,
}

#[derive(Debug, clickhouse::Row, Serialize)]
struct RequestLogRow {
    request_id: Uuid,
    task_id: Uuid,
    source_agent_id: Uuid,
    target_agent_id: Uuid,
    source_org_id: Uuid,
    target_org_id: Uuid,
    method: String,
    status_code: u16,
    latency_ms: u32,
    message_parts_count: u16,
    pii_redacted: bool,
    injection_flagged: bool,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    timestamp: chrono::DateTime<Utc>,
}

// ── Batch writer wrapping 3 Inserters ──

struct BatchWriter {
    tasks: Inserter<TaskRow>,
    messages: Inserter<MessageRow>,
    request_log: Inserter<RequestLogRow>,
}

impl BatchWriter {
    fn new(ch: &ClickHousePool) -> Self {
        let tasks = ch
            .inserter::<TaskRow>("a2a_tasks")
            .with_max_rows(BATCH_MAX_ROWS)
            .with_period(Some(BATCH_PERIOD));
        let messages = ch
            .inserter::<MessageRow>("a2a_messages")
            .with_max_rows(BATCH_MAX_ROWS)
            .with_period(Some(BATCH_PERIOD));
        let request_log = ch
            .inserter::<RequestLogRow>("a2a_request_log")
            .with_max_rows(BATCH_MAX_ROWS)
            .with_period(Some(BATCH_PERIOD));
        Self {
            tasks,
            messages,
            request_log,
        }
    }

    async fn write_task(&mut self, row: TaskRow) -> Result<(), clickhouse::error::Error> {
        self.tasks.write(&row).await
    }

    async fn write_message(&mut self, row: MessageRow) -> Result<(), clickhouse::error::Error> {
        self.messages.write(&row).await
    }

    async fn write_request_log(
        &mut self,
        row: RequestLogRow,
    ) -> Result<(), clickhouse::error::Error> {
        self.request_log.write(&row).await
    }

    /// Conditionally flush each inserter if its period or row-count threshold is reached.
    async fn commit(&mut self) -> Result<(), clickhouse::error::Error> {
        self.tasks.commit().await?;
        self.messages.commit().await?;
        self.request_log.commit().await?;
        Ok(())
    }

    /// Force-flush all remaining rows (called on shutdown).
    async fn end(self) -> Result<(), clickhouse::error::Error> {
        self.tasks.end().await?;
        self.messages.end().await?;
        self.request_log.end().await?;
        Ok(())
    }
}

// ── Worker entrypoint ──

pub fn start_message_worker(
    kafka_hosts: &str,
    client_id: Option<&str>,
    routing_cache: Arc<RoutingCache>,
    clickhouse_pool: ClickHousePool,
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
                tracing::error!("Failed to create Kafka consumer: {}", e);
                return;
            }
        };

        if let Err(e) = consumer.subscribe(&[A2A_MESSAGES_TOPIC]) {
            tracing::error!("Failed to subscribe to {}: {}", A2A_MESSAGES_TOPIC, e);
            return;
        }

        let mut writer = BatchWriter::new(&clickhouse_pool);

        tracing::info!(
            "A2A message worker started, consuming from {}",
            A2A_MESSAGES_TOPIC
        );

        let mut stream = consumer.stream();
        loop {
            tokio::select! {
                msg = stream.next() => {
                    match msg {
                        Some(Ok(borrowed)) => {
                            if let Some(payload) = borrowed.payload() {
                                if let Err(e) = process_message(
                                    payload,
                                    &routing_cache,
                                    &mut writer,
                                    &kafka_producer,
                                    &http_client,
                                ).await {
                                    tracing::error!("Failed to process A2A message: {}", e);
                                }
                                if let Err(e) = writer.commit().await {
                                    tracing::error!("Failed to commit ClickHouse batch: {}", e);
                                }
                            }
                        }
                        Some(Err(e)) => {
                            tracing::error!("Kafka consumer error: {}", e);
                        }
                        None => break,
                    }
                }
                _ = shutdown_rx.changed() => {
                    tracing::info!("A2A message worker shutting down, flushing ClickHouse batches");
                    if let Err(e) = writer.end().await {
                        tracing::error!("Failed to flush ClickHouse batches on shutdown: {}", e);
                    }
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
        .set("max.poll.interval.ms", "300000")
        .set("fetch.min.bytes", "1")
        .set("fetch.wait.max.ms", "500");

    if let Some(id) = client_id {
        config.set("client.id", id);
    }

    config.create()
}

const FORWARD_TIMEOUT: Duration = Duration::from_secs(120);

async fn process_message(
    payload: &[u8],
    routing_cache: &RoutingCache,
    writer: &mut BatchWriter,
    kafka_producer: &KafkaProducer,
    http_client: &reqwest::Client,
) -> Result<(), anyhow::Error> {
    let envelope: A2aMessageEnvelope = serde_json::from_slice(payload)
        .map_err(|e| anyhow::anyhow!("Failed to deserialize A2A message envelope: {}", e))?;

    let task_id = Uuid::parse_str(&envelope.task_id)
        .map_err(|e| anyhow::anyhow!("Invalid task_id '{}': {}", envelope.task_id, e))?;

    tracing::info!(
        task_id = %task_id,
        method = %envelope.method,
        source_agent_id = %envelope.source_agent_id,
        target_agent_id = %envelope.target_agent_id,
        "Processing A2A message"
    );

    let context_id = envelope
        .context_id
        .as_deref()
        .and_then(|s| Uuid::parse_str(s).ok());
    let flags = envelope.pipeline_flags.clone().unwrap_or_default();
    let now = Utc::now();

    // 1. Buffer task state (status: submitted)
    writer
        .write_task(TaskRow {
            task_id,
            context_id,
            source_agent_id: envelope.source_agent_id,
            target_agent_id: envelope.target_agent_id,
            source_org_id: envelope.source_org_id,
            target_org_id: envelope.target_org_id,
            status: "submitted".into(),
            metadata: serde_json::to_string(
                &envelope
                    .metadata
                    .clone()
                    .unwrap_or(serde_json::Value::Object(Default::default())),
            )
            .unwrap_or_default(),
            artifacts: "[]".into(),
            updated_at: now,
            created_at: envelope.timestamp,
        })
        .await
        .map_err(|e| anyhow::anyhow!("Failed to buffer task row: {}", e))?;

    // 2. Buffer inbound message
    let message_id = envelope
        .message
        .get("messageId")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
        .unwrap_or_else(Uuid::now_v7);

    let role = envelope
        .message
        .get("role")
        .and_then(|v| v.as_str())
        .unwrap_or("user")
        .to_string();

    let parts = envelope
        .message
        .get("parts")
        .map(|v| v.to_string())
        .unwrap_or_else(|| "[]".into());

    let pipeline_flags_json = serde_json::json!({
        "piiRedacted": flags.pii_redacted,
        "injectionFlagged": flags.injection_flagged,
    })
    .to_string();

    writer
        .write_message(MessageRow {
            message_id,
            task_id,
            context_id,
            role,
            parts,
            reference_task_ids: Vec::new(),
            metadata: "{}".into(),
            pipeline_flags: pipeline_flags_json,
            created_at: now,
        })
        .await
        .map_err(|e| anyhow::anyhow!("Failed to buffer message row: {}", e))?;

    // 3. Buffer request log
    writer
        .write_request_log(RequestLogRow {
            request_id: Uuid::now_v7(),
            task_id,
            source_agent_id: envelope.source_agent_id,
            target_agent_id: envelope.target_agent_id,
            source_org_id: envelope.source_org_id,
            target_org_id: envelope.target_org_id,
            method: envelope.method.clone(),
            status_code: 200,
            latency_ms: 0,
            message_parts_count: 0,
            pii_redacted: flags.pii_redacted,
            injection_flagged: flags.injection_flagged,
            timestamp: now,
        })
        .await
        .map_err(|e| anyhow::anyhow!("Failed to buffer request log row: {}", e))?;

    // 4. Forward message to target agent's endpointUrl
    if envelope.method != "tasks/cancel" {
        let forward_result =
            forward_to_agent(http_client, routing_cache, &envelope, &task_id).await;

        let final_status = match &forward_result {
            Ok(_) => "completed",
            Err(_) => "failed",
        };

        let updated_at = Utc::now();
        let artifacts_json = match &forward_result {
            Ok(Some(ref fwd)) => {
                serde_json::to_string(&fwd.artifacts).unwrap_or_else(|_| "[]".into())
            }
            _ => "[]".into(),
        };

        // Buffer task status update
        let _ = writer
            .write_task(TaskRow {
                task_id,
                context_id,
                source_agent_id: envelope.source_agent_id,
                target_agent_id: envelope.target_agent_id,
                source_org_id: envelope.source_org_id,
                target_org_id: envelope.target_org_id,
                status: final_status.into(),
                metadata: "{}".into(),
                artifacts: artifacts_json,
                updated_at,
                created_at: envelope.timestamp,
            })
            .await
            .map_err(|e| {
                tracing::error!(task_id = %task_id, "Failed to buffer task status update: {}", e);
            });

        // Buffer agent response messages
        if let Ok(Some(ref fwd)) = forward_result {
            for msg in &fwd.response_messages {
                let _ = writer.write_message(msg.clone()).await.map_err(|e| {
                    tracing::error!(task_id = %task_id, "Failed to buffer agent response: {}", e);
                });
            }
        }

        if let Err(ref e) = forward_result {
            tracing::error!(task_id = %task_id, "Failed to forward message to agent: {}", e);
        }
    }

    // 5. Check push configs and trigger push notifications
    let final_status = "completed";
    let configs = routing_cache.get_push_configs(&envelope.task_id);

    for config in configs {
        let webhook_url = config.webhook_url;
        let auth_scheme = config.auth_scheme;
        let auth_credentials = config.auth_credentials;
        let push_payload = serde_json::json!({
            "statusUpdate": {
                "taskId": envelope.task_id,
                "contextId": envelope.context_id,
                "status": {
                    "state": final_status,
                    "timestamp": Utc::now().to_rfc3339(),
                }
            }
        });

        let trigger = PushTrigger {
            task_id: envelope.task_id.clone(),
            webhook_url,
            auth_scheme,
            auth_credentials,
            payload: push_payload,
            retry_after: Utc::now(),
            attempt: 0,
            created_at: Utc::now(),
        };

        let bytes = serde_json::to_vec(&trigger)
            .map_err(|e| anyhow::anyhow!("Failed to serialize push trigger: {}", e))?;
        kafka_producer
            .send_to_topic(A2A_PUSH_TOPIC, &envelope.task_id, &bytes)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to produce push trigger: {}", e))?;
    }

    tracing::debug!(
        task_id = %task_id,
        method = %envelope.method,
        "Processed A2A message"
    );

    Ok(())
}

/// Result from forwarding a message to a target agent.
struct ForwardResult {
    artifacts: Option<serde_json::Value>,
    response_messages: Vec<MessageRow>,
}

/// Look up the target agent's endpoint_url, sign the request with the
/// org's webhook secret, and forward the JSON-RPC SendMessage.
/// Returns the artifacts and any response messages to be batched.
async fn forward_to_agent(
    http_client: &reqwest::Client,
    routing_cache: &RoutingCache,
    envelope: &A2aMessageEnvelope,
    task_id: &Uuid,
) -> Result<Option<ForwardResult>, anyhow::Error> {
    let routing = match routing_cache.get_agent(envelope.target_agent_id) {
        Some(r) if r.enabled && !r.endpoint_url.is_empty() => r,
        Some(r) if !r.enabled => {
            tracing::warn!(
                task_id = %task_id,
                target_agent_id = %envelope.target_agent_id,
                "Target agent is disabled, cannot forward"
            );
            return Err(anyhow::anyhow!("Target agent not found or disabled"));
        }
        Some(_) => {
            return Err(anyhow::anyhow!(
                "Target agent has no endpoint_url configured"
            ))
        }
        None => {
            tracing::warn!(
                task_id = %task_id,
                target_agent_id = %envelope.target_agent_id,
                "Target agent not found in routing cache, cannot forward"
            );
            return Err(anyhow::anyhow!("Target agent not found or disabled"));
        }
    };

    let endpoint_url = routing.endpoint_url;
    let webhook_secret = routing.webhook_secret;

    let jsonrpc_body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": task_id.to_string(),
        "method": "SendMessage",
        "params": {
            "message": envelope.message,
            "metadata": {
                "sourceAgentId": envelope.source_agent_id.to_string(),
                "sourceOrgId": envelope.source_org_id.to_string(),
                "herdTaskId": task_id.to_string(),
            }
        }
    });

    let body_bytes = serde_json::to_vec(&jsonrpc_body)
        .map_err(|e| anyhow::anyhow!("Failed to serialize forward body: {}", e))?;

    tracing::info!(
        task_id = %task_id,
        endpoint_url = %endpoint_url,
        "Forwarding A2A message to target agent"
    );

    let forward_start = std::time::Instant::now();

    let mut request = http_client
        .post(&endpoint_url)
        .header("Content-Type", "application/json")
        .timeout(FORWARD_TIMEOUT);

    if let Some(ref secret) = webhook_secret {
        let signature = crate::verification::sign_payload(secret, &body_bytes);
        let timestamp = chrono::Utc::now().timestamp().to_string();
        request = request
            .header("X-Herd-Signature", signature)
            .header("X-Herd-Timestamp", timestamp);
    }

    let resp = request
        .body(body_bytes)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("HTTP error forwarding to {}: {}", endpoint_url, e))?;

    let latency_ms = forward_start.elapsed().as_millis() as u32;
    let status_code = resp.status().as_u16();

    tracing::info!(
        task_id = %task_id,
        endpoint_url = %endpoint_url,
        status_code = status_code,
        latency_ms = latency_ms,
        "Target agent responded"
    );

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!(
            "Target agent returned HTTP {}: {}",
            status_code,
            body
        ));
    }

    let response_body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to parse agent response: {}", e))?;

    let artifacts = response_body
        .get("result")
        .and_then(|r| r.get("artifacts"))
        .cloned();

    let context_id = envelope
        .context_id
        .as_deref()
        .and_then(|s| Uuid::parse_str(s).ok());

    // Collect response messages to be batched by the caller
    let mut response_messages = Vec::new();
    if let Some(result) = response_body.get("result") {
        if let Some(agent_artifacts) = result.get("artifacts").and_then(|a| a.as_array()) {
            for artifact in agent_artifacts {
                let response_parts = artifact
                    .get("parts")
                    .map(|p| p.to_string())
                    .unwrap_or_else(|| "[]".into());

                response_messages.push(MessageRow {
                    message_id: Uuid::now_v7(),
                    task_id: *task_id,
                    context_id,
                    role: "agent".into(),
                    parts: response_parts,
                    reference_task_ids: Vec::new(),
                    metadata: "{}".into(),
                    pipeline_flags: "{}".into(),
                    created_at: Utc::now(),
                });
            }
        }
    }

    Ok(Some(ForwardResult {
        artifacts,
        response_messages,
    }))
}
