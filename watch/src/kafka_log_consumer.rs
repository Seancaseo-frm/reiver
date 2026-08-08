//! Kafka consumer for logs - consumes from Kafka and writes to ClickHouse
//!
//! Handles both OTLP logs (raw protobuf/JSON) and unstructured logs.
//! All parsing, transformation, and ClickHouse writes happen here.
//!
//! Performance optimizations:
//! - Uses persistent ClickHouse inserter with batching instead of per-message inserters
//! - Uses simd-json for SIMD-accelerated JSON parsing on hot paths
//! - Collects Kafka messages into batches (by time or byte size) before processing
//! - PII scanning runs on the blocking thread pool with rayon parallel iterators

use anyhow::Result;
use chrono::{DateTime, Utc};
use clickhouse::Row;
use quick_cache::sync::Cache;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::StreamConsumer;
use rdkafka::message::Message;
use serde::Serialize;
use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tracing::{error, info, instrument, warn, Instrument};
use uuid::Uuid;

use crate::simd_json_utils::parse_json_simd;

use crate::app_state::RedisPool;
use crate::clickhouse_db::ClickHousePool;
use crate::config::Config;
use crate::db::DbPool;
use crate::kafka::UnstructuredLogKafkaMessage;
use crate::models::RawOtlpLogPayload;

#[derive(Row, Serialize, Clone)]
pub struct LogInsert {
    pub project_id: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub timestamp: DateTime<Utc>,
    pub trace_id: String,
    pub span_id: String,
    pub severity_text: String,
    pub severity_number: u8,
    pub service_name: String,
    pub body: String,
    pub resource_attributes: Vec<(Cow<'static, str>, Cow<'static, str>)>,
    pub log_attributes: Vec<(Cow<'static, str>, Cow<'static, str>)>,
}

#[derive(Row, Serialize, Clone)]
struct UsageInsert {
    project_id: String,
    event_type: String,
    #[serde(with = "clickhouse::serde::chrono::date")]
    date: chrono::NaiveDate,
    value: u64,
}

#[derive(Debug, Clone)]
struct ProjectIdCacheEntry {
    project_id: Uuid,
    expires_at: Instant,
}

type ProjectIdCache = Arc<Cache<String, ProjectIdCacheEntry>>;

pub struct KafkaLogConsumerContext;

impl rdkafka::ClientContext for KafkaLogConsumerContext {
    fn stats(&self, _stats: rdkafka::Statistics) {}
}

impl rdkafka::consumer::ConsumerContext for KafkaLogConsumerContext {}

const BATCH_TIMEOUT: Duration = Duration::from_millis(500);
const MAX_BATCH_BYTES: usize = 1024 * 1024; // 1 MB

pub async fn start_kafka_log_consumer(
    kafka_hosts: &str,
    otlp_topic: &str,
    unstructured_topic: &str,
    client_id: Option<&str>,
    db_pool: Arc<DbPool>,
    clickhouse_pool: Arc<ClickHousePool>,
    redis_pool: Arc<RedisPool>,
    _config: Arc<Config>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<JoinHandle<()>> {
    info!(
        "Creating Kafka consumer for log topics: {} and {}",
        otlp_topic, unstructured_topic
    );

    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", kafka_hosts)
        .set("group.id", "reiver-logs-processor")
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "5000")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "30000")
        .set("enable.partition.eof", "false");

    if let Some(client_id) = client_id {
        client_config.set("client.id", client_id);
    }

    let consumer: StreamConsumer<KafkaLogConsumerContext> =
        client_config.create_with_context(KafkaLogConsumerContext)?;

    consumer.subscribe(&[otlp_topic, unstructured_topic])?;
    info!(
        "Subscribed to Kafka log topics: {} and {}",
        otlp_topic, unstructured_topic
    );

    let project_id_cache: ProjectIdCache = Arc::new(Cache::new(10_000));
    let otlp_topic_owned = otlp_topic.to_string();

    let handle = tokio::spawn(async move {
        info!(
            "[KAFKA_LOG_CONSUMER] Started, batch_timeout={}ms max_batch_bytes={}KB",
            BATCH_TIMEOUT.as_millis(),
            MAX_BATCH_BYTES / 1024,
        );

        let mut inserter = clickhouse_pool
            .as_ref()
            .inserter::<LogInsert>("logs")
            .with_period(Some(Duration::from_secs(30)))
            .with_max_rows(50_000);

        let mut usage_inserter = clickhouse_pool
            .as_ref()
            .inserter::<UsageInsert>("usage")
            .with_period(Some(Duration::from_secs(30)))
            .with_max_rows(50_000);

        let mut message_stream = consumer.stream();
        let mut message_count = 0u64;
        let mut error_count = 0u64;
        let mut total_logs_written = 0u64;
        let mut pending_writes = 0u64;
        let mut flush_interval = tokio::time::interval(Duration::from_secs(30));
        flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut throughput_interval = tokio::time::interval(Duration::from_secs(10));
        throughput_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut window_msg_count = 0u64;
        let mut window_start = Instant::now();

        loop {
            // Phase 1: Collect messages until timeout or byte budget
            let mut raw_payloads: Vec<(String, Vec<u8>)> = Vec::new();
            let mut batch_bytes: usize = 0;
            let batch_deadline = tokio::time::Instant::now() + BATCH_TIMEOUT;
            let mut should_shutdown = false;
            let mut stream_ended = false;

            loop {
                tokio::select! {
                    biased;

                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            should_shutdown = true;
                            break;
                        }
                    }

                    _ = flush_interval.tick() => {
                        if pending_writes > 0 {
                            let flush_start = Instant::now();
                            if let Err(e) = inserter.commit().await {
                                error!("[KAFKA_LOG_CONSUMER] Periodic flush: failed to commit: {}", e);
                            } else {
                                info!(
                                    "[KAFKA_LOG_CONSUMER] Flushed {} logs to ClickHouse in {:.1}ms",
                                    pending_writes, flush_start.elapsed().as_secs_f64() * 1000.0,
                                );
                                total_logs_written += pending_writes;
                                pending_writes = 0;
                            }
                        }
                        if let Err(e) = usage_inserter.commit().await {
                            error!("[KAFKA_LOG_CONSUMER] Periodic flush: failed to commit usage: {}", e);
                        }
                    }

                    _ = throughput_interval.tick() => {
                        let elapsed = window_start.elapsed();
                        if window_msg_count > 0 || message_count > 0 {
                            let msg_per_sec = if elapsed.as_secs_f64() > 0.0 {
                                window_msg_count as f64 / elapsed.as_secs_f64()
                            } else { 0.0 };
                            info!(
                                "[KAFKA_LOG_CONSUMER] total_msgs={} window_msgs={} msg/s={:.1} total_logs={} pending={} errors={}",
                                message_count, window_msg_count, msg_per_sec,
                                total_logs_written, pending_writes, error_count,
                            );
                        }
                        window_msg_count = 0;
                        window_start = Instant::now();
                    }

                    _ = tokio::time::sleep_until(batch_deadline) => break,

                    message_opt = message_stream.next() => {
                        match message_opt {
                            Some(Ok(m)) => {
                                let payload = m.payload().unwrap_or_default().to_vec();
                                batch_bytes += payload.len();
                                raw_payloads.push((m.topic().to_string(), payload));
                                if batch_bytes >= MAX_BATCH_BYTES { break; }
                            }
                            Some(Err(e)) => {
                                error_count += 1;
                                error!("[KAFKA_LOG_CONSUMER] Error receiving message: {}", e);
                            }
                            None => { stream_ended = true; break; }
                        }
                    }
                }
            }

            if should_shutdown {
                info!("[KAFKA_LOG_CONSUMER] Received shutdown signal");
                break;
            }

            if !raw_payloads.is_empty() {
                let batch_msg_count = raw_payloads.len() as u64;

                // Phase 2: Process the entire batch
                match process_log_batch(
                    &db_pool,
                    &redis_pool,
                    &project_id_cache,
                    &otlp_topic_owned,
                    raw_payloads,
                    batch_bytes,
                )
                .await
                {
                    Ok((log_rows, usage_bytes)) => {
                        for row in &log_rows {
                            if let Err(e) = inserter.write(row).await {
                                error!(
                                    "[KAFKA_LOG_CONSUMER] Failed to write log to inserter: {}",
                                    e
                                );
                            } else {
                                pending_writes += 1;
                            }
                        }
                        let today = Utc::now().date_naive();
                        for (project_id, bytes) in &usage_bytes {
                            if let Err(e) = usage_inserter
                                .write(&UsageInsert {
                                    project_id: project_id.clone(),
                                    event_type: "log".to_string(),
                                    date: today,
                                    value: *bytes,
                                })
                                .await
                            {
                                error!("[KAFKA_LOG_CONSUMER] Failed to write usage: {}", e);
                            }
                        }
                        message_count += batch_msg_count;
                        window_msg_count += batch_msg_count;

                        if pending_writes >= 10_000 {
                            let flush_start = Instant::now();
                            if let Err(e) = inserter.commit().await {
                                error!("[KAFKA_LOG_CONSUMER] Failed to commit inserter: {}", e);
                            } else {
                                info!(
                                    "[KAFKA_LOG_CONSUMER] Batch commit: {} logs in {:.1}ms",
                                    pending_writes,
                                    flush_start.elapsed().as_secs_f64() * 1000.0,
                                );
                                total_logs_written += pending_writes;
                                pending_writes = 0;
                            }
                        }
                    }
                    Err(e) => {
                        error_count += batch_msg_count;
                        error!(
                            "[KAFKA_LOG_CONSUMER] Failed to process batch ({} msgs, {} bytes): {}",
                            batch_msg_count, batch_bytes, e
                        );
                    }
                }
            }

            if stream_ended {
                break;
            }
        }

        if pending_writes > 0 {
            if let Err(e) = inserter.commit().await {
                error!("[KAFKA_LOG_CONSUMER] Failed to final commit: {}", e);
            }
        }
        if let Err(e) = inserter.end().await {
            error!("[KAFKA_LOG_CONSUMER] Failed to end inserter: {}", e);
        }
        if let Err(e) = usage_inserter.commit().await {
            error!(
                "[KAFKA_LOG_CONSUMER] Failed to commit usage inserter: {}",
                e
            );
        }
        if let Err(e) = usage_inserter.end().await {
            error!("[KAFKA_LOG_CONSUMER] Failed to end usage inserter: {}", e);
        }
        info!(
            "[KAFKA_LOG_CONSUMER] Stopped (total_msgs={}, total_logs={})",
            message_count, total_logs_written
        );
    });

    Ok(handle)
}

async fn resolve_project_id_cached(
    db_pool: &DbPool,
    cache: &ProjectIdCache,
    project_key: &str,
) -> Result<Option<Uuid>> {
    if let Ok(project_id) = Uuid::parse_str(project_key) {
        return Ok(Some(project_id));
    }

    let now = Instant::now();

    if let Some(entry) = cache.get(project_key) {
        if entry.expires_at > now {
            return Ok(Some(entry.project_id));
        }
    }

    let key_hash = crate::utils::hash_api_key(project_key);
    let result = sqlx::query_as::<_, (Uuid,)>(
        "SELECT pk.project_id FROM project_keys pk WHERE pk.key_hash = $1 LIMIT 1",
    )
    .bind(&key_hash)
    .fetch_optional(&*db_pool)
    .await?;

    if let Some((project_id,)) = result {
        cache.insert(
            project_key.to_string(),
            ProjectIdCacheEntry {
                project_id,
                expires_at: Instant::now() + Duration::from_secs(3600),
            },
        );
        Ok(Some(project_id))
    } else {
        Ok(None)
    }
}

/// Process a batch of Kafka messages collected over a time window.
///
/// Parses all OTLP + unstructured payloads, resolves project IDs (deduplicated),
/// builds LogInsert rows, and runs PII scanning across the entire batch.
#[instrument(name = "process_logs", skip_all, err, fields(
    messages_count = raw_payloads.len(),
    batch_bytes = batch_bytes,
    logs_count = tracing::field::Empty,
))]
async fn process_log_batch(
    db_pool: &DbPool,
    redis_pool: &RedisPool,
    project_id_cache: &ProjectIdCache,
    otlp_topic: &str,
    raw_payloads: Vec<(String, Vec<u8>)>,
    batch_bytes: usize,
) -> Result<(Vec<LogInsert>, std::collections::HashMap<String, u64>)> {
    use prost::Message;

    let mut usage_bytes: std::collections::HashMap<String, u64> = std::collections::HashMap::new();

    // Step 1: Parse Kafka JSON envelopes and separate OTLP vs unstructured
    let (otlp_payloads, unstructured_rows) = {
        let _guard = tracing::info_span!("parse_messages", count = raw_payloads.len()).entered();
        let mut otlp = Vec::new();
        let mut unstructured = Vec::new();

        for (topic, payload) in &raw_payloads {
            let kafka_msg_bytes = payload.len() as u64;
            if *topic == otlp_topic {
                match parse_json_simd::<RawOtlpLogPayload>(payload) {
                    Ok(p) => otlp.push((p, kafka_msg_bytes)),
                    Err(e) => warn!("[KAFKA_LOG_CONSUMER] Failed to parse OTLP envelope: {}", e),
                }
            } else {
                match parse_json_simd::<UnstructuredLogKafkaMessage>(payload) {
                    Ok(log_msg) => {
                        let pid = log_msg.project_id.clone();
                        match process_unstructured_log_batched(log_msg) {
                            Ok(row) => {
                                *usage_bytes.entry(pid).or_default() += kafka_msg_bytes;
                                unstructured.push(row);
                            }
                            Err(e) => warn!(
                                "[KAFKA_LOG_CONSUMER] Failed to process unstructured log: {}",
                                e
                            ),
                        }
                    }
                    Err(e) => warn!(
                        "[KAFKA_LOG_CONSUMER] Failed to parse unstructured log: {}",
                        e
                    ),
                }
            }
        }
        (otlp, unstructured)
    };

    // Step 2: Resolve unique project IDs and check PII
    let mut project_map: std::collections::HashMap<String, Uuid> = std::collections::HashMap::new();
    let mut pii_projects: HashSet<String> = HashSet::new();

    {
        let unique_keys: HashSet<&str> = otlp_payloads
            .iter()
            .map(|(p, _)| p.project_key.as_str())
            .collect();

        for key in unique_keys {
            let project_id = match resolve_project_id_cached(db_pool, project_id_cache, key)
                .instrument(tracing::info_span!("resolve_project_id"))
                .await?
            {
                Some(id) => id,
                None => {
                    warn!("[KAFKA_LOG_CONSUMER] Project key not found: {}", key);
                    continue;
                }
            };
            let pid_str = project_id.to_string();
            if crate::pii::get_pii_masking_enabled_cached(redis_pool, db_pool, project_id).await {
                pii_projects.insert(pid_str.clone());
            }
            project_map.insert(key.to_string(), project_id);
        }
    }

    // Step 3: Parse OTLP payloads and build all LogInsert rows
    let mut all_rows = {
        let _guard = tracing::info_span!("build_log_rows").entered();
        let mut rows = Vec::with_capacity(otlp_payloads.len() * 8);
        rows.extend(unstructured_rows);

        for (raw_payload, kafka_msg_bytes) in otlp_payloads {
            let Some(&project_id) = project_map.get(&raw_payload.project_key) else {
                continue;
            };
            let project_id_str = project_id.to_string();
            let bytes = if raw_payload.ingested_bytes > 0 {
                raw_payload.ingested_bytes
            } else {
                kafka_msg_bytes
            };
            *usage_bytes.entry(project_id_str.clone()).or_default() += bytes;

            let export_request = if raw_payload.content_type == "json" {
                match parse_logs_request_json(&raw_payload.raw_bytes) {
                    Ok(r) => r,
                    Err(e) => {
                        warn!("[KAFKA_LOG_CONSUMER] Failed to parse JSON logs: {}", e);
                        continue;
                    }
                }
            } else {
                match opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest::decode(
                    raw_payload.raw_bytes.as_slice()
                ) {
                    Ok(r) => r,
                    Err(e) => { warn!("[KAFKA_LOG_CONSUMER] Failed to parse protobuf: {}", e); continue; }
                }
            };

            for resource_log in export_request.resource_logs {
                let resource = resource_log.resource.unwrap_or_default();
                let service_name = extract_service_name(&resource.attributes)
                    .unwrap_or_else(|| "unknown".to_string());
                let resource_attrs_vec = convert_attributes_to_vec(&resource.attributes);

                for scope_log in resource_log.scope_logs {
                    for log_record in scope_log.log_records {
                        let body = extract_log_body(&log_record);
                        if body.is_empty() {
                            continue;
                        }

                        rows.push(LogInsert {
                            project_id: project_id_str.clone(),
                            timestamp: extract_timestamp(&log_record),
                            trace_id: format_trace_id(&log_record.trace_id),
                            span_id: format_span_id(&log_record.span_id),
                            severity_text: extract_severity_text(&log_record),
                            severity_number: log_record.severity_number as u8,
                            service_name: service_name.clone(),
                            body,
                            resource_attributes: resource_attrs_vec.clone(),
                            log_attributes: convert_attributes_to_vec(&log_record.attributes),
                        });
                    }
                }
            }
        }
        rows
    };

    // Step 4: PII scan (only rows from PII-enabled projects)
    if !pii_projects.is_empty() && !all_rows.is_empty() {
        let row_count = all_rows.len();
        all_rows = tokio::task::spawn_blocking(move || {
            use rayon::prelude::*;
            all_rows
                .into_par_iter()
                .map(|mut row| {
                    if pii_projects.contains(&row.project_id) {
                        if let Some(redacted) = crate::pii::redact_if_changed(&row.body) {
                            row.body = redacted;
                        }
                    }
                    row
                })
                .collect()
        })
        .instrument(tracing::info_span!("pii_scan", logs_count = row_count))
        .await
        .map_err(|e| anyhow::anyhow!("PII scanning task panicked: {}", e))?;
    }

    tracing::Span::current().record("logs_count", all_rows.len());
    Ok((all_rows, usage_bytes))
}

fn process_unstructured_log_batched(log_msg: UnstructuredLogKafkaMessage) -> Result<LogInsert> {
    let project_id: Uuid = log_msg
        .project_id
        .parse()
        .map_err(|e| anyhow::anyhow!("Invalid project_id: {}", e))?;

    let timestamp: DateTime<Utc> = log_msg.timestamp.parse().unwrap_or_else(|_| Utc::now());

    let severity_number = match log_msg.level.to_uppercase().as_str() {
        "TRACE" => 1u8,
        "DEBUG" => 5u8,
        "INFO" => 9u8,
        "WARN" | "WARNING" => 13u8,
        "ERROR" => 17u8,
        "FATAL" | "CRITICAL" => 21u8,
        _ => 9u8,
    };

    Ok(LogInsert {
        project_id: project_id.to_string(),
        timestamp,
        trace_id: log_msg.trace_id.unwrap_or_default(),
        span_id: log_msg.span_id.unwrap_or_default(),
        severity_text: log_msg.level,
        severity_number,
        service_name: log_msg.service_name,
        body: log_msg.message,
        resource_attributes: vec![(Cow::Borrowed("log.source"), Cow::Owned(log_msg.source))],
        log_attributes: Vec::new(),
    })
}

// ============================================================================
// OTLP Parsing Helpers
// ============================================================================

fn parse_logs_request_json(
    body: &[u8],
) -> Result<opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest> {
    let json_value: serde_json::Value =
        serde_json::from_slice(body).map_err(|e| anyhow::anyhow!("Failed to parse JSON: {}", e))?;

    let mut request =
        opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest::default();

    if let Some(resource_logs_json) = json_value
        .get("resourceLogs")
        .or_else(|| json_value.get("resource_logs"))
    {
        if let serde_json::Value::Array(resource_logs_array) = resource_logs_json {
            for rl_json in resource_logs_array {
                let resource_log = parse_resource_logs_json(rl_json)?;
                request.resource_logs.push(resource_log);
            }
        }
    }

    Ok(request)
}

fn parse_resource_logs_json(
    json: &serde_json::Value,
) -> Result<opentelemetry_proto::tonic::logs::v1::ResourceLogs> {
    use opentelemetry_proto::tonic::logs::v1::{ResourceLogs, ScopeLogs};

    let mut resource_logs = ResourceLogs::default();

    if let Some(resource_json) = json.get("resource") {
        resource_logs.resource = Some(parse_resource_json(resource_json)?);
    }

    if let Some(scope_logs_json) = json.get("scopeLogs").or_else(|| json.get("scope_logs")) {
        if let serde_json::Value::Array(scope_logs_array) = scope_logs_json {
            for sl_json in scope_logs_array {
                let mut scope_log = ScopeLogs::default();

                if let Some(log_records_json) = sl_json
                    .get("logRecords")
                    .or_else(|| sl_json.get("log_records"))
                {
                    if let serde_json::Value::Array(log_records_array) = log_records_json {
                        for lr_json in log_records_array {
                            let log_record = parse_log_record_json(lr_json)?;
                            scope_log.log_records.push(log_record);
                        }
                    }
                }

                resource_logs.scope_logs.push(scope_log);
            }
        }
    }

    Ok(resource_logs)
}

fn parse_resource_json(
    json: &serde_json::Value,
) -> Result<opentelemetry_proto::tonic::resource::v1::Resource> {
    use opentelemetry_proto::tonic::resource::v1::Resource;

    let mut resource = Resource::default();

    if let Some(attrs_json) = json.get("attributes") {
        resource.attributes = parse_attributes_json(attrs_json)?;
    }

    Ok(resource)
}

fn parse_log_record_json(
    json: &serde_json::Value,
) -> Result<opentelemetry_proto::tonic::logs::v1::LogRecord> {
    use opentelemetry_proto::tonic::logs::v1::LogRecord;

    let mut log_record = LogRecord::default();

    if let Some(time) = json
        .get("timeUnixNano")
        .or_else(|| json.get("time_unix_nano"))
    {
        if let Some(time_str) = time.as_str() {
            log_record.time_unix_nano = time_str.parse().unwrap_or(0);
        } else if let Some(time_num) = time.as_u64() {
            log_record.time_unix_nano = time_num;
        }
    }

    if let Some(time) = json
        .get("observedTimeUnixNano")
        .or_else(|| json.get("observed_time_unix_nano"))
    {
        if let Some(time_str) = time.as_str() {
            log_record.observed_time_unix_nano = time_str.parse().unwrap_or(0);
        } else if let Some(time_num) = time.as_u64() {
            log_record.observed_time_unix_nano = time_num;
        }
    }

    if let Some(sev) = json
        .get("severityNumber")
        .or_else(|| json.get("severity_number"))
    {
        log_record.severity_number = sev.as_i64().unwrap_or(0) as i32;
    }

    if let Some(sev_text) = json
        .get("severityText")
        .or_else(|| json.get("severity_text"))
    {
        log_record.severity_text = sev_text.as_str().unwrap_or("").to_string();
    }

    if let Some(body_json) = json.get("body") {
        log_record.body = Some(parse_anyvalue_json(body_json));
    }

    if let Some(attrs_json) = json.get("attributes") {
        log_record.attributes = parse_attributes_json(attrs_json)?;
    }

    if let Some(trace_id) = json.get("traceId").or_else(|| json.get("trace_id")) {
        if let Some(trace_id_str) = trace_id.as_str() {
            log_record.trace_id = hex::decode(trace_id_str).unwrap_or_default();
        }
    }

    if let Some(span_id) = json.get("spanId").or_else(|| json.get("span_id")) {
        if let Some(span_id_str) = span_id.as_str() {
            log_record.span_id = hex::decode(span_id_str).unwrap_or_default();
        }
    }

    Ok(log_record)
}

fn parse_attributes_json(
    json: &serde_json::Value,
) -> Result<Vec<opentelemetry_proto::tonic::common::v1::KeyValue>> {
    use opentelemetry_proto::tonic::common::v1::KeyValue;

    let mut attributes = Vec::new();

    if let serde_json::Value::Array(attrs_array) = json {
        for attr_json in attrs_array {
            if let (Some(key), Some(value_json)) = (attr_json.get("key"), attr_json.get("value")) {
                let key_str = key.as_str().unwrap_or("").to_string();
                let any_value = parse_anyvalue_json(value_json);
                attributes.push(KeyValue {
                    key: key_str,
                    value: Some(any_value),
                });
            }
        }
    }

    Ok(attributes)
}

fn parse_anyvalue_json(
    json: &serde_json::Value,
) -> opentelemetry_proto::tonic::common::v1::AnyValue {
    use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue};

    let value = match json {
        serde_json::Value::String(s) => any_value::Value::StringValue(s.clone()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                any_value::Value::IntValue(i)
            } else if let Some(f) = n.as_f64() {
                any_value::Value::DoubleValue(f)
            } else {
                any_value::Value::StringValue(n.to_string())
            }
        }
        serde_json::Value::Bool(b) => any_value::Value::BoolValue(*b),
        _ => {
            if let Some(s) = json.get("stringValue").or_else(|| json.get("string_value")) {
                any_value::Value::StringValue(s.as_str().unwrap_or("").to_string())
            } else if let Some(i) = json.get("intValue").or_else(|| json.get("int_value")) {
                let int_val = i
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .or_else(|| i.as_i64())
                    .unwrap_or(0);
                any_value::Value::IntValue(int_val)
            } else if let Some(d) = json.get("doubleValue").or_else(|| json.get("double_value")) {
                any_value::Value::DoubleValue(d.as_f64().unwrap_or(0.0))
            } else if let Some(b) = json.get("boolValue").or_else(|| json.get("bool_value")) {
                any_value::Value::BoolValue(b.as_bool().unwrap_or(false))
            } else {
                any_value::Value::StringValue(json.to_string())
            }
        }
    };

    AnyValue { value: Some(value) }
}

fn extract_service_name(
    attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
) -> Option<String> {
    for attr in attributes {
        if attr.key == "service.name" {
            if let Some(value) = &attr.value {
                if let Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s),
                ) = &value.value
                {
                    return Some(s.clone());
                }
            }
        }
    }
    None
}

fn convert_attributes_to_vec(
    attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
) -> Vec<(Cow<'static, str>, Cow<'static, str>)> {
    let mut result = Vec::with_capacity(attributes.len());
    let mut itoa_buf = itoa::Buffer::new();

    for attr in attributes {
        if let Some(value) = &attr.value {
            let value_cow: Cow<'static, str> = match &value.value {
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => {
                    if let Some(spur) = crate::intern::try_get(s) {
                        Cow::Borrowed(crate::intern::resolve(spur))
                    } else {
                        Cow::Owned(s.clone())
                    }
                }
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => {
                    Cow::Owned(itoa_buf.format(*i).to_owned())
                }
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d)) => {
                    Cow::Owned(d.to_string())
                }
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b)) => {
                    Cow::Borrowed(if *b { "true" } else { "false" })
                }
                _ => continue,
            };
            let key: Cow<'static, str> = if let Some(spur) = crate::intern::try_get(&attr.key) {
                Cow::Borrowed(crate::intern::resolve(spur))
            } else {
                Cow::Owned(attr.key.clone())
            };
            result.push((key, value_cow));
        }
    }
    result
}

fn extract_log_body(log_record: &opentelemetry_proto::tonic::logs::v1::LogRecord) -> String {
    if let Some(body) = &log_record.body {
        match &body.value {
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => {
                return s.clone();
            }
            Some(v) => {
                return format!("{:?}", v);
            }
            None => {}
        }
    }
    for attr in &log_record.attributes {
        if attr.key == "message" {
            if let Some(value) = &attr.value {
                if let Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s),
                ) = &value.value
                {
                    return s.clone();
                }
            }
        }
    }
    String::new()
}

fn extract_severity_text(log_record: &opentelemetry_proto::tonic::logs::v1::LogRecord) -> String {
    if !log_record.severity_text.is_empty() {
        return log_record.severity_text.clone();
    }
    match log_record.severity_number {
        1..=4 => "TRACE".to_string(),
        5..=8 => "DEBUG".to_string(),
        9..=12 => "INFO".to_string(),
        13..=16 => "WARN".to_string(),
        17..=20 => "ERROR".to_string(),
        21..=24 => "FATAL".to_string(),
        _ => "INFO".to_string(),
    }
}

fn extract_timestamp(
    log_record: &opentelemetry_proto::tonic::logs::v1::LogRecord,
) -> DateTime<Utc> {
    let nanos = if log_record.time_unix_nano > 0 {
        log_record.time_unix_nano as u64
    } else if log_record.observed_time_unix_nano > 0 {
        log_record.observed_time_unix_nano as u64
    } else {
        return Utc::now();
    };

    chrono::DateTime::from_timestamp(
        (nanos / 1_000_000_000) as i64,
        (nanos % 1_000_000_000) as u32,
    )
    .unwrap_or_else(Utc::now)
}

fn format_trace_id(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return String::new();
    }
    hex::encode(bytes)
}

fn format_span_id(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return String::new();
    }
    hex::encode(bytes)
}
