use anyhow::Result;
use futures_util::future::join_all;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};
use uuid::Uuid;

/// Exception message to be sent to Kafka, includes all fields needed by ClickHouse
#[derive(Debug, Serialize)]
pub struct ExceptionKafkaMessage {
    pub id: String,
    pub project_id: String,
    pub fingerprint: String,
    pub level: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exception_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exception_value: Option<String>,
    pub stacktrace: String,
    pub context: String,
    pub tags: String,
    pub user_data: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
    pub timestamp: String, // ISO 8601 string for parseDateTime64BestEffort
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span_id: Option<String>,
    // Deploy version tracking (for GitHub integration)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_version: Option<String>, // Git commit SHA or release tag
    #[serde(skip_serializing_if = "Option::is_none")]
    pub environment: Option<String>, // Deployment environment
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repository_url: Option<String>, // VCS repository URL
}

// Note: OtlpLogKafkaMessage removed - now using LogPayload from models.rs

/// Unstructured log message to be sent to Kafka (for raw logs from CloudWatch, Azure, GCP, direct)
#[derive(Debug, Serialize, serde::Deserialize, Clone)]
pub struct UnstructuredLogKafkaMessage {
    pub id: String,
    pub project_id: String,
    pub message: String,
    pub level: String,
    pub service_name: String,
    pub source: String,
    pub timestamp: String, // ISO 8601 string
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span_id: Option<String>,
}

/// LLM streaming chunk message to be sent to Kafka
/// Each SSE chunk from the LLM provider is sent as a separate message
/// Chunks are grouped by request_id and ordered by chunk_index
#[derive(Debug, Serialize, serde::Deserialize, Clone)]
pub struct LlmChunkKafkaMessage {
    pub project_id: String,
    pub request_id: String, // chatcmpl-xxx, groups all chunks of one request
    pub chunk_index: u32,   // Order of chunk in stream (0-based)
    pub content: String,    // The text fragment from delta.content
    pub model: String,
    pub provider: String,
    pub timestamp: String, // ISO 8601 string
    pub is_final: bool,    // True for last chunk with finish_reason
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u32>,
}

/// Warehouse sync job message to be sent to Kafka
/// Used for upgrade_to_warm, upgrade_to_hot, and sync jobs
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SyncJobKafkaMessage {
    pub job_id: Uuid,
    /// Job type: "upgrade_to_warm", "upgrade_to_hot", "downgrade", "remove_cache", "index_build", "sync"
    pub job_type: String,
    pub source_id: Uuid,
    pub project_id: Uuid,
    /// Optional table name for single-table syncs
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_name: Option<String>,
    pub created_at: String, // ISO 8601
}

/// Pipeline event message for the event-driven pipeline system.
/// Produced when cron fires, manual trigger, or data change events occur.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PipelineEventKafkaMessage {
    pub event_id: Uuid,
    pub project_id: Uuid,
    pub event_type: String,
    pub source: String,
    pub payload: serde_json::Value,
}

/// Session evaluation job message sent to Kafka when an idle session is
/// discovered. Consumed by the session evaluator consumer group for
/// classification (via moodeng) and profile matching.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SessionEvalJobKafkaMessage {
    pub project_id: String,
    pub session_id: String,
    pub enqueued_at: String, // ISO 8601
}

/// Simple Kafka context (no-op for now, can add metrics later)
pub struct KafkaContext;

impl rdkafka::ClientContext for KafkaContext {
    fn stats(&self, _stats: rdkafka::Statistics) {
        // Can add metrics/stats reporting here later
    }
}

/// Configuration for creating a Kafka producer.
#[derive(Debug, Clone)]
pub struct KafkaProducerConfig {
    pub hosts: String,
    pub exceptions_topic: String,
    pub spans_topic: String,
    pub logs_otlp_topic: String,
    pub logs_unstructured_topic: String,
    pub llm_chunks_topic: String,
    pub metrics_topic: String,
    pub sync_jobs_topic: String,
    pub pipeline_events_topic: String,
    pub platform_events_topic: String,
    pub session_eval_jobs_topic: String,
    pub client_id: Option<String>,
    pub linger_ms: i32,
    pub max_retries: i32,
    pub message_timeout_ms: i32,
    pub socket_timeout_ms: i32,
    pub compression_codec: String,
    pub acks: String,
}

pub struct KafkaProducer {
    producer: Arc<FutureProducer<KafkaContext>>,
    exceptions_topic: String,
    spans_topic: String,
    logs_otlp_topic: String,
    logs_unstructured_topic: String,
    llm_chunks_topic: String,
    metrics_topic: String,
    sync_jobs_topic: String,
    pipeline_events_topic: String,
    platform_events_topic: String,
    session_eval_jobs_topic: String,
}

impl KafkaProducer {
    pub fn new(config: &KafkaProducerConfig) -> Result<Self> {
        info!("Connecting to Kafka brokers at {}...", config.hosts);

        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &config.hosts)
            .set("statistics.interval.ms", "10000")
            .set("partitioner", "murmur2")
            .set("message.send.max.retries", config.max_retries.to_string())
            .set("linger.ms", config.linger_ms.to_string())
            .set("message.timeout.ms", config.message_timeout_ms.to_string())
            .set("socket.timeout.ms", config.socket_timeout_ms.to_string())
            .set("compression.codec", &config.compression_codec)
            .set("acks", &config.acks)
            .set("queue.buffering.max.kbytes", "65536")
            .set("message.max.bytes", "4194304");

        if let Some(ref client_id) = config.client_id {
            client_config.set("client.id", client_id);
        }

        let producer: FutureProducer<KafkaContext> =
            client_config.create_with_context(KafkaContext)?;

        // Verify connection by fetching metadata
        if producer
            .client()
            .fetch_metadata(
                Some(&config.exceptions_topic),
                Timeout::After(Duration::new(10, 0)),
            )
            .is_ok()
        {
            info!("Successfully connected to Kafka brokers");
        } else {
            warn!("Could not fetch Kafka metadata - topics may not exist yet");
        }

        Ok(KafkaProducer {
            producer: Arc::new(producer),
            exceptions_topic: config.exceptions_topic.clone(),
            spans_topic: config.spans_topic.clone(),
            logs_otlp_topic: config.logs_otlp_topic.clone(),
            logs_unstructured_topic: config.logs_unstructured_topic.clone(),
            llm_chunks_topic: config.llm_chunks_topic.clone(),
            metrics_topic: config.metrics_topic.clone(),
            sync_jobs_topic: config.sync_jobs_topic.clone(),
            pipeline_events_topic: config.pipeline_events_topic.clone(),
            platform_events_topic: config.platform_events_topic.clone(),
            session_eval_jobs_topic: config.session_eval_jobs_topic.clone(),
        })
    }

    /// Send an exception to Kafka. Key: project_id (for partitioning by project)
    /// The message includes id, project_id, and fingerprint for ClickHouse consumption
    pub async fn send_exception(&self, message: &ExceptionKafkaMessage) -> Result<()> {
        let key = message.project_id.clone();
        let value = serde_json::to_string(message)?;

        let record = FutureRecord::to(&self.exceptions_topic)
            .key(&key)
            .payload(&value);

        match self.producer.send(record, Duration::from_secs(0)).await {
            Ok(_) => Ok(()),
            Err((e, _)) => {
                error!("Failed to send exception to Kafka: {:?}", e);
                Err(anyhow::anyhow!("Kafka send exception: {:?}", e))
            }
        }
    }

    /// Send a span to Kafka
    /// Key: trace_id (for partitioning by trace - required for tail sampling)
    pub async fn send_span(
        &self,
        trace_id: &str,
        span_payload: &crate::models::SpanPayload,
    ) -> Result<()> {
        let value = serde_json::to_string(span_payload)?;

        // Use trace_id as key for partitioning (ensures same trace → same partition)
        let record = FutureRecord::to(&self.spans_topic)
            .key(trace_id)
            .payload(&value);

        match self.producer.send(record, Duration::from_secs(0)).await {
            Ok(_) => Ok(()),
            Err((e, _)) => {
                error!("Failed to send span to Kafka: {:?}", e);
                Err(anyhow::anyhow!("Kafka send error: {:?}", e))
            }
        }
    }

    /// Send multiple exceptions in batch (for batch endpoint)
    /// Uses parallel sends for better throughput
    pub async fn send_exceptions_batch(&self, messages: &[ExceptionKafkaMessage]) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        // Serialize messages first (this could fail)
        let serialized: Result<Vec<(String, String)>> = messages
            .iter()
            .map(|msg| {
                let key = msg.project_id.clone();
                let value = serde_json::to_string(msg)?;
                Ok((key, value))
            })
            .collect();
        let serialized = serialized?;

        // Create all send futures
        let futures: Vec<_> = serialized
            .iter()
            .map(|(key, value)| async move {
                let record = FutureRecord::to(&self.exceptions_topic)
                    .key(key.as_str())
                    .payload(value.as_str());
                self.producer.send(record, Duration::from_secs(0)).await
            })
            .collect();

        // Execute all sends in parallel
        let results = join_all(futures).await;

        // Check for any failures
        for result in results {
            if let Err((e, _)) = result {
                error!("Failed to send exception to Kafka in batch: {:?}", e);
                return Err(anyhow::anyhow!("Kafka send error: {:?}", e));
            }
        }

        Ok(())
    }

    /// Send multiple spans in batch (for batch endpoint)
    /// Uses parallel sends for better throughput
    pub async fn send_spans_batch(
        &self,
        spans: &[(String, crate::models::SpanPayload)], // (trace_id, payload)
    ) -> Result<()> {
        if spans.is_empty() {
            return Ok(());
        }

        // Create all send futures
        let futures: Vec<_> = spans
            .iter()
            .map(|(trace_id, span_payload)| self.send_span(trace_id, span_payload))
            .collect();

        // Execute all sends in parallel
        let results = join_all(futures).await;

        // Check for any failures
        let mut first_error: Option<anyhow::Error> = None;
        let mut error_count = 0;

        for result in results {
            if let Err(e) = result {
                error_count += 1;
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }

        if let Some(e) = first_error {
            error!(
                "Failed to send {} spans in batch, first error: {}",
                error_count, e
            );
            return Err(e);
        }

        Ok(())
    }

    /// Enqueue raw OTLP log payload to Kafka without waiting for broker ack.
    /// Returns immediately after the message is buffered in librdkafka.
    /// Delivery errors are logged asynchronously.
    pub fn enqueue_raw_otlp_log(&self, message: &crate::models::RawOtlpLogPayload) -> Result<()> {
        let topic = self.logs_otlp_topic.clone();
        let key = message.project_key.clone();
        let value = serde_json::to_string(message)?;
        let producer = self.producer.clone();

        tokio::spawn(async move {
            let record = FutureRecord::to(&topic).key(&key).payload(&value);
            if let Err((e, _)) = producer.send(record, Duration::from_secs(5)).await {
                error!("Kafka OTLP log delivery failed: {:?}", e);
            }
        });

        Ok(())
    }

    /// Enqueue raw OTLP trace payload to Kafka without waiting for broker ack.
    pub fn enqueue_raw_otlp_trace(
        &self,
        message: &crate::models::RawOtlpTracePayload,
    ) -> Result<()> {
        let topic = self.spans_topic.clone();
        let key = message.project_key.clone();
        let value = serde_json::to_string(message)?;
        let producer = self.producer.clone();

        tokio::spawn(async move {
            let record = FutureRecord::to(&topic).key(&key).payload(&value);
            if let Err((e, _)) = producer.send(record, Duration::from_secs(5)).await {
                error!("Kafka OTLP trace delivery failed: {:?}", e);
            }
        });

        Ok(())
    }

    /// Enqueue raw OTLP metrics payload to Kafka without waiting for broker ack.
    pub fn enqueue_raw_otlp_metrics(
        &self,
        message: &crate::models::RawOtlpMetricsPayload,
    ) -> Result<()> {
        let topic = self.metrics_topic.clone();
        let key = message.project_key.clone();
        let value = serde_json::to_string(message)?;
        let producer = self.producer.clone();

        tokio::spawn(async move {
            let record = FutureRecord::to(&topic).key(&key).payload(&value);
            if let Err((e, _)) = producer.send(record, Duration::from_secs(5)).await {
                error!("Kafka OTLP metrics delivery failed: {:?}", e);
            }
        });

        Ok(())
    }

    /// Send an unstructured log to Kafka. Key: project_id (for partitioning by project)
    pub async fn send_unstructured_log(&self, message: &UnstructuredLogKafkaMessage) -> Result<()> {
        let key = message.project_id.clone();
        let value = serde_json::to_string(message)?;

        let record = FutureRecord::to(&self.logs_unstructured_topic)
            .key(&key)
            .payload(&value);

        match self.producer.send(record, Duration::from_secs(0)).await {
            Ok(_) => Ok(()),
            Err((e, _)) => {
                error!("Failed to send unstructured log to Kafka: {:?}", e);
                Err(anyhow::anyhow!("Kafka send unstructured log: {:?}", e))
            }
        }
    }

    /// Send multiple unstructured logs in batch
    /// Uses parallel sends for better throughput
    #[allow(dead_code)] // Method reserved for future log batch processing
    pub async fn send_unstructured_logs_batch(
        &self,
        messages: &[UnstructuredLogKafkaMessage],
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let futures: Vec<_> = messages
            .iter()
            .map(|msg| self.send_unstructured_log(msg))
            .collect();

        let results = join_all(futures).await;

        for result in results {
            result?;
        }

        Ok(())
    }

    /// Send an LLM streaming chunk to Kafka.
    /// Key: request_id (for partitioning - all chunks of same request go to same partition)
    /// This ensures chunks are processed in order within the same request.
    pub async fn send_llm_chunk(&self, message: &LlmChunkKafkaMessage) -> Result<()> {
        let key = message.request_id.clone();
        let value = serde_json::to_string(message)?;

        let record = FutureRecord::to(&self.llm_chunks_topic)
            .key(&key)
            .payload(&value);

        match self.producer.send(record, Duration::from_secs(0)).await {
            Ok(_) => Ok(()),
            Err((e, _)) => {
                // Log at debug level since this is fire-and-forget and shouldn't affect user
                tracing::debug!(
                    request_id = %message.request_id,
                    chunk_index = message.chunk_index,
                    "Failed to send LLM chunk to Kafka: {:?}", e
                );
                Err(anyhow::anyhow!("Kafka send LLM chunk: {:?}", e))
            }
        }
    }

    /// Send a warehouse sync job to Kafka for async processing.
    /// Key: project_id (for partitioning by project)
    /// Used for upgrade_to_warm, upgrade_to_hot, downgrade, remove_cache, index_build, and sync jobs.
    pub async fn send_sync_job(&self, message: &SyncJobKafkaMessage) -> Result<()> {
        let key = message.project_id.to_string();
        let value = serde_json::to_string(message)?;

        let record = FutureRecord::to(&self.sync_jobs_topic)
            .key(&key)
            .payload(&value);

        match self.producer.send(record, Duration::from_secs(0)).await {
            Ok(_) => {
                tracing::info!(
                    job_id = %message.job_id,
                    job_type = %message.job_type,
                    source_id = %message.source_id,
                    "Sent sync job to Kafka"
                );
                Ok(())
            }
            Err((e, _)) => {
                error!(
                    job_id = %message.job_id,
                    job_type = %message.job_type,
                    "Failed to send sync job to Kafka: {:?}", e
                );
                Err(anyhow::anyhow!("Kafka send sync job: {:?}", e))
            }
        }
    }

    /// Send a pipeline event to Kafka for async dispatch.
    /// Key: project_id (for partition affinity per project)
    pub async fn send_pipeline_event(&self, message: &PipelineEventKafkaMessage) -> Result<()> {
        let key = message.project_id.to_string();
        let value = serde_json::to_string(message)?;

        let record = FutureRecord::to(&self.pipeline_events_topic)
            .key(&key)
            .payload(&value);

        match self.producer.send(record, Duration::from_secs(0)).await {
            Ok(_) => {
                tracing::info!(
                    event_id = %message.event_id,
                    event_type = %message.event_type,
                    project_id = %message.project_id,
                    "Sent pipeline event to Kafka"
                );
                Ok(())
            }
            Err((e, _)) => {
                error!(
                    event_id = %message.event_id,
                    event_type = %message.event_type,
                    "Failed to send pipeline event to Kafka: {:?}", e
                );
                Err(anyhow::anyhow!("Kafka send pipeline event: {:?}", e))
            }
        }
    }

    /// Send a platform event to Kafka for the event subscription system.
    /// Key: project_id (partition-level ordering per project)
    pub async fn send_platform_event(&self, event: &crate::events::PlatformEvent) -> Result<()> {
        let key = event.project_id.to_string();
        let value = serde_json::to_vec(event)?;

        let record = FutureRecord::to(&self.platform_events_topic)
            .key(&key)
            .payload(&value);

        match self.producer.send(record, Duration::from_secs(0)).await {
            Ok(_) => {
                tracing::debug!(
                    event_id = %event.id,
                    event_type = %event.event_type,
                    project_id = %event.project_id,
                    "Sent platform event to Kafka"
                );
                Ok(())
            }
            Err((e, _)) => {
                error!(
                    event_id = %event.id,
                    event_type = %event.event_type,
                    "Failed to send platform event to Kafka: {:?}", e
                );
                Err(anyhow::anyhow!("Kafka send platform event: {:?}", e))
            }
        }
    }

    /// Send a session evaluation job to Kafka for async classification + profile matching.
    /// Key: project_id (partition affinity -- all sessions for a project go to the same consumer)
    pub async fn send_session_eval_job(&self, message: &SessionEvalJobKafkaMessage) -> Result<()> {
        let key = message.project_id.clone();
        let value = serde_json::to_string(message)?;

        let record = FutureRecord::to(&self.session_eval_jobs_topic)
            .key(&key)
            .payload(&value);

        match self.producer.send(record, Duration::from_secs(0)).await {
            Ok(_) => {
                tracing::debug!(
                    project_id = %message.project_id,
                    session_id = %message.session_id,
                    "Sent session eval job to Kafka"
                );
                Ok(())
            }
            Err((e, _)) => {
                error!(
                    project_id = %message.project_id,
                    session_id = %message.session_id,
                    "Failed to send session eval job to Kafka: {:?}", e
                );
                Err(anyhow::anyhow!("Kafka send session eval job: {:?}", e))
            }
        }
    }

    /// Send an arbitrary JSON payload to a named topic with a string key.
    /// Used by Herd (and any future service) that manages its own topic names.
    pub async fn send_to_topic(&self, topic: &str, key: &str, payload: &[u8]) -> Result<()> {
        let record = FutureRecord::to(topic).key(key).payload(payload);

        match self.producer.send(record, Duration::from_secs(0)).await {
            Ok(_) => Ok(()),
            Err((e, _)) => {
                error!(topic = %topic, "Failed to send message to Kafka: {:?}", e);
                Err(anyhow::anyhow!("Kafka send to {}: {:?}", topic, e))
            }
        }
    }

    /// Returns `true` if the Kafka broker cluster is reachable.
    ///
    /// Performs a lightweight metadata fetch with a 2-second timeout — the same
    /// call used at startup to verify connectivity.
    pub fn is_healthy(&self) -> bool {
        self.producer
            .client()
            .fetch_metadata(None, Timeout::After(Duration::from_secs(2)))
            .is_ok()
    }

    /// Flush all pending messages
    #[allow(dead_code)]
    pub fn flush(&self, timeout: Duration) -> Result<()> {
        self.producer.flush(timeout)?;
        Ok(())
    }
}
