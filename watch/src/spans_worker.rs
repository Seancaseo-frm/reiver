//! Spans ingestion worker - consumes raw OTLP traces from Kafka and writes to ClickHouse
//!
//! Handles:
//! - OTLP parsing (protobuf or JSON)
//! - project_key → project_id resolution
//! - LLM span detection and processing
//! - Writing spans to ClickHouse
//!
//! Performance optimizations:
//! - Uses persistent ClickHouse inserters with batching
//! - Uses lockless DashMap cache for project IDs
//! - Uses simd-json for SIMD-accelerated JSON parsing
//! - Collects Kafka messages into batches (by time or byte size) before processing

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
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, instrument, warn, Instrument};
use uuid::Uuid;

use crate::simd_json_utils::parse_json_simd;

use crate::app_state::RedisPool;
use crate::clickhouse_db::ClickHousePool;
use crate::config::Config;
use crate::db::DbPool;
use crate::llm::{LlmRequest, LlmSpanProcessor};
use crate::models::{RawOtlpTracePayload, SpanKind, SpanPayload, StatusCode as SpanStatusCode};
use reiver_core::kafka::KafkaProducer;

#[derive(Row, Serialize, Clone)]
pub struct SpanInsert {
    pub project_id: String,
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: String,
    pub trace_state: String,
    pub span_name: String,
    pub span_kind: String,
    pub service_name: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub timestamp: DateTime<Utc>,
    pub duration: i64,
    pub status_code: String,
    pub status_message: String,
    pub span_attributes: Vec<(Cow<'static, str>, Cow<'static, str>)>,
    pub resource_attributes: Vec<(Cow<'static, str>, Cow<'static, str>)>,
    pub events: String,
    pub links: String,
}

#[derive(Row, Serialize, Clone)]
pub struct FilterValueInsert {
    pub project_id: String,
    pub attribute_type: String,
    pub attribute_value: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub last_seen: DateTime<Utc>,
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

struct KafkaConsumerContext;

impl rdkafka::ClientContext for KafkaConsumerContext {
    fn stats(&self, _stats: rdkafka::Statistics) {}
}

impl rdkafka::consumer::ConsumerContext for KafkaConsumerContext {}

const BATCH_TIMEOUT: Duration = Duration::from_millis(500);
const MAX_BATCH_BYTES: usize = 1024 * 1024; // 1 MB

pub async fn start_spans_worker(
    kafka_hosts: &str,
    spans_topic: &str,
    client_id: Option<&str>,
    db_pool: Arc<DbPool>,
    clickhouse_pool: Arc<ClickHousePool>,
    redis_pool: Arc<RedisPool>,
    config: Arc<Config>,
    kafka_producer: Arc<KafkaProducer>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<JoinHandle<()>> {
    info!("Creating spans ingestion worker for topic: {}", spans_topic);

    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", kafka_hosts)
        .set("group.id", "reiver-spans-worker")
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

    consumer.subscribe(&[spans_topic])?;
    info!("Subscribed to Kafka topic: {}", spans_topic);

    let cost_calculator = crate::llm::CostCalculator::new(db_pool.clone());
    let llm_processor = Arc::new(LlmSpanProcessor::new(cost_calculator));

    let project_id_cache: ProjectIdCache = Arc::new(Cache::new(10_000));

    let handle = tokio::spawn(async move {
        info!(
            "[SPANS_WORKER] Started, batch_timeout={}ms max_batch_bytes={}KB",
            BATCH_TIMEOUT.as_millis(),
            MAX_BATCH_BYTES / 1024,
        );

        let mut span_inserter = clickhouse_pool
            .as_ref()
            .inserter::<SpanInsert>("spans")
            .with_period(Some(Duration::from_secs(30)))
            .with_max_rows(50_000);

        let mut filter_inserter = clickhouse_pool
            .as_ref()
            .inserter::<FilterValueInsert>("otlp_attributes")
            .with_period(Some(Duration::from_secs(30)))
            .with_max_rows(50_000);

        let mut usage_inserter = clickhouse_pool
            .as_ref()
            .inserter::<UsageInsert>("usage")
            .with_period(Some(Duration::from_secs(30)))
            .with_max_rows(50_000);

        let metrics_topic = config.kafka_metrics_topic.clone();

        let mut message_stream = consumer.stream();
        let mut message_count = 0u64;
        let mut error_count = 0u64;
        let mut total_spans_written = 0u64;
        let mut pending_span_writes = 0u64;
        let mut pending_filter_writes = 0u64;
        let mut flush_interval = tokio::time::interval(Duration::from_secs(30));
        flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut throughput_interval = tokio::time::interval(Duration::from_secs(10));
        throughput_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut window_msg_count = 0u64;
        let mut window_start = Instant::now();

        loop {
            // Phase 1: Collect messages until timeout or byte budget
            let mut raw_payloads: Vec<Vec<u8>> = Vec::new();
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
                        if pending_span_writes > 0 {
                            let flush_start = Instant::now();
                            if let Err(e) = span_inserter.commit().await {
                                error!("[SPANS_WORKER] Periodic flush: failed to commit spans: {}", e);
                            } else {
                                info!(
                                    "[SPANS_WORKER] Flushed {} spans to ClickHouse in {:.1}ms",
                                    pending_span_writes, flush_start.elapsed().as_secs_f64() * 1000.0,
                                );
                                total_spans_written += pending_span_writes;
                                pending_span_writes = 0;
                            }
                        }
                        if pending_filter_writes > 0 {
                            let flush_start = Instant::now();
                            if let Err(e) = filter_inserter.commit().await {
                                error!("[SPANS_WORKER] Periodic flush: failed to commit filters: {}", e);
                            } else {
                                debug!(
                                    "[SPANS_WORKER] Flushed {} filters to ClickHouse in {:.1}ms",
                                    pending_filter_writes, flush_start.elapsed().as_secs_f64() * 1000.0,
                                );
                                pending_filter_writes = 0;
                            }
                        }
                        if let Err(e) = usage_inserter.commit().await {
                            error!("[SPANS_WORKER] Periodic flush: failed to commit usage: {}", e);
                        }
                    }

                    _ = throughput_interval.tick() => {
                        let elapsed = window_start.elapsed();
                        if window_msg_count > 0 || message_count > 0 {
                            let msg_per_sec = if elapsed.as_secs_f64() > 0.0 {
                                window_msg_count as f64 / elapsed.as_secs_f64()
                            } else { 0.0 };
                            info!(
                                "[SPANS_WORKER] total_msgs={} window_msgs={} msg/s={:.1} total_spans={} pending_spans={} pending_filters={} errors={}",
                                message_count, window_msg_count, msg_per_sec,
                                total_spans_written, pending_span_writes, pending_filter_writes, error_count,
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
                                raw_payloads.push(payload);
                                if batch_bytes >= MAX_BATCH_BYTES { break; }
                            }
                            Some(Err(e)) => {
                                error_count += 1;
                                error!("[SPANS_WORKER] Error receiving message: {}", e);
                            }
                            None => { stream_ended = true; break; }
                        }
                    }
                }
            }

            if should_shutdown {
                info!("[SPANS_WORKER] Received shutdown signal");
                if pending_span_writes > 0 {
                    if let Err(e) = span_inserter.commit().await {
                        error!(
                            "[SPANS_WORKER] Failed to commit span inserter on shutdown: {}",
                            e
                        );
                    }
                }
                if pending_filter_writes > 0 {
                    if let Err(e) = filter_inserter.commit().await {
                        error!(
                            "[SPANS_WORKER] Failed to commit filter inserter on shutdown: {}",
                            e
                        );
                    }
                }
                if let Err(e) = usage_inserter.commit().await {
                    error!(
                        "[SPANS_WORKER] Failed to commit usage inserter on shutdown: {}",
                        e
                    );
                }
                break;
            }

            if !raw_payloads.is_empty() {
                let batch_msg_count = raw_payloads.len() as u64;

                // Phase 2: Process the entire batch
                match process_trace_batch(
                    &db_pool,
                    &clickhouse_pool,
                    &redis_pool,
                    &project_id_cache,
                    &llm_processor,
                    raw_payloads,
                    batch_bytes,
                )
                .await
                {
                    Ok((span_rows, filter_rows, usage_bytes)) => {
                        for row in &span_rows {
                            if let Err(e) = span_inserter.write(row).await {
                                error!("[SPANS_WORKER] Failed to write span: {}", e);
                            } else {
                                pending_span_writes += 1;
                            }
                        }
                        for row in &filter_rows {
                            if let Err(e) = filter_inserter.write(row).await {
                                error!("[SPANS_WORKER] Failed to write filter: {}", e);
                            } else {
                                pending_filter_writes += 1;
                            }
                        }
                        let today = Utc::now().date_naive();
                        for (project_id, bytes) in &usage_bytes {
                            if let Err(e) = usage_inserter
                                .write(&UsageInsert {
                                    project_id: project_id.clone(),
                                    event_type: "span".to_string(),
                                    date: today,
                                    value: *bytes,
                                })
                                .await
                            {
                                error!("[SPANS_WORKER] Failed to write usage: {}", e);
                            }
                        }
                        // Phase 3: Generate RED metrics from server spans
                        // and produce them to the Kafka metrics topic so the
                        // metrics_worker handles insertion, usage tracking, and billing.
                        produce_red_metrics(
                            &redis_pool,
                            &db_pool,
                            &kafka_producer,
                            &metrics_topic,
                            &span_rows,
                        )
                        .await;

                        message_count += batch_msg_count;
                        window_msg_count += batch_msg_count;

                        if pending_span_writes >= 10_000 {
                            let flush_start = Instant::now();
                            if let Err(e) = span_inserter.commit().await {
                                error!("[SPANS_WORKER] Failed to commit span inserter: {}", e);
                            } else {
                                info!(
                                    "[SPANS_WORKER] Batch commit: {} spans in {:.1}ms",
                                    pending_span_writes,
                                    flush_start.elapsed().as_secs_f64() * 1000.0,
                                );
                                total_spans_written += pending_span_writes;
                                pending_span_writes = 0;
                            }
                        }
                        if pending_filter_writes >= 10_000 {
                            if let Err(e) = filter_inserter.commit().await {
                                error!("[SPANS_WORKER] Failed to commit filter inserter: {}", e);
                            } else {
                                pending_filter_writes = 0;
                            }
                        }
                    }
                    Err(e) => {
                        error_count += batch_msg_count;
                        error!(
                            "[SPANS_WORKER] Failed to process batch ({} msgs, {} bytes): {}",
                            batch_msg_count, batch_bytes, e
                        );
                    }
                }
            }

            if stream_ended {
                break;
            }
        }

        if pending_span_writes > 0 {
            if let Err(e) = span_inserter.commit().await {
                error!("[SPANS_WORKER] Failed to final commit spans: {}", e);
            }
        }
        if pending_filter_writes > 0 {
            if let Err(e) = filter_inserter.commit().await {
                error!("[SPANS_WORKER] Failed to final commit filters: {}", e);
            }
        }
        if let Err(e) = span_inserter.end().await {
            error!("[SPANS_WORKER] Failed to end span inserter: {}", e);
        }
        if let Err(e) = filter_inserter.end().await {
            error!("[SPANS_WORKER] Failed to end filter inserter: {}", e);
        }
        info!(
            "[SPANS_WORKER] Stopped (total_msgs={}, total_spans={})",
            message_count, total_spans_written
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

enum ParsedTracePayload {
    Otlp(RawOtlpTracePayload),
    Span(SpanPayload),
}

/// Process a batch of Kafka messages collected over a time window.
///
/// Parses all OTLP + SpanPayload messages, resolves project IDs (deduplicated),
/// builds SpanInsert/FilterValueInsert rows, and handles LLM span processing.
#[instrument(name = "process_traces", skip_all, err, fields(
    messages_count = raw_payloads.len(),
    batch_bytes = batch_bytes,
    spans_count = tracing::field::Empty,
))]
async fn process_trace_batch(
    db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
    redis_pool: &RedisPool,
    project_id_cache: &ProjectIdCache,
    llm_processor: &LlmSpanProcessor,
    raw_payloads: Vec<Vec<u8>>,
    batch_bytes: usize,
) -> Result<(
    Vec<SpanInsert>,
    Vec<FilterValueInsert>,
    HashMap<String, u64>,
)> {
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use prost::Message;

    // Step 1: Parse all Kafka messages, capturing the Kafka message size for usage tracking
    let parsed: Vec<(ParsedTracePayload, usize)> = {
        let _guard = tracing::info_span!("parse_messages", count = raw_payloads.len()).entered();
        let mut parsed = Vec::with_capacity(raw_payloads.len());
        for payload in &raw_payloads {
            let kafka_msg_bytes = payload.len();
            match parse_json_simd::<RawOtlpTracePayload>(payload) {
                Ok(otlp) => parsed.push((ParsedTracePayload::Otlp(otlp), kafka_msg_bytes)),
                Err(_otlp_err) => match parse_json_simd::<SpanPayload>(payload) {
                    Ok(sp) => parsed.push((ParsedTracePayload::Span(sp), kafka_msg_bytes)),
                    Err(span_err) => warn!(
                        "[SPANS_WORKER] Failed to parse payload ({} bytes) as OTLP: {} / as SpanPayload: {}",
                        payload.len(), _otlp_err, span_err,
                    ),
                },
            }
        }
        parsed
    };

    // Step 2: Resolve unique project IDs and check PII
    let mut project_map: HashMap<String, Uuid> = HashMap::new();
    let mut pii_projects: HashSet<String> = HashSet::new();

    {
        let unique_keys: HashSet<&str> = parsed
            .iter()
            .map(|(p, _)| match p {
                ParsedTracePayload::Otlp(o) => o.project_key.as_str(),
                ParsedTracePayload::Span(s) => s.project_key.as_str(),
            })
            .collect();

        for key in unique_keys {
            let project_id = match resolve_project_id_cached(db_pool, project_id_cache, key)
                .instrument(tracing::info_span!("resolve_project_id"))
                .await?
            {
                Some(id) => id,
                None => {
                    warn!("[SPANS_WORKER] Project key not found: {}", key);
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

    // Step 3: Build span rows, filter rows, and collect LLM requests
    let mut span_rows: Vec<SpanInsert> = Vec::new();
    let mut filter_rows: Vec<FilterValueInsert> = Vec::new();
    let mut llm_requests_by_project: HashMap<Uuid, Vec<LlmRequest>> = HashMap::new();
    let mut usage_bytes: HashMap<String, u64> = HashMap::new();
    let now = Utc::now();

    for (payload, kafka_msg_bytes) in parsed {
        match payload {
            ParsedTracePayload::Otlp(raw_payload) => {
                let Some(&project_id) = project_map.get(&raw_payload.project_key) else {
                    continue;
                };
                let project_id_str = project_id.to_string();
                let bytes = if raw_payload.ingested_bytes > 0 {
                    raw_payload.ingested_bytes
                } else {
                    kafka_msg_bytes as u64
                };
                *usage_bytes.entry(project_id_str.clone()).or_default() += bytes;
                let pii_enabled = pii_projects.contains(&project_id_str);

                let export_request = if raw_payload.content_type == "json" {
                    match serde_json::from_slice::<ExportTraceServiceRequest>(
                        &raw_payload.raw_bytes,
                    ) {
                        Ok(r) => r,
                        Err(e) => {
                            warn!("[SPANS_WORKER] Failed to parse JSON traces: {}", e);
                            continue;
                        }
                    }
                } else {
                    match ExportTraceServiceRequest::decode(&raw_payload.raw_bytes[..]) {
                        Ok(r) => r,
                        Err(e) => {
                            warn!("[SPANS_WORKER] Failed to parse protobuf traces: {}", e);
                            continue;
                        }
                    }
                };

                for resource_span in export_request.resource_spans {
                    let resource = resource_span.resource.unwrap_or_default();
                    let service_name = extract_service_name(&resource.attributes);
                    let resource_attrs = convert_attributes_to_hashmap(&resource.attributes);
                    let resource_attrs_vec: Vec<(Cow<'static, str>, Cow<'static, str>)> =
                        resource_attrs
                            .iter()
                            .map(|(k, v)| {
                                let key: Cow<'static, str> =
                                    if let Some(spur) = crate::intern::try_get(k) {
                                        Cow::Borrowed(crate::intern::resolve(spur))
                                    } else {
                                        Cow::Owned(k.clone())
                                    };
                                let value: Cow<'static, str> =
                                    if let Some(spur) = crate::intern::try_get(v) {
                                        Cow::Borrowed(crate::intern::resolve(spur))
                                    } else {
                                        Cow::Owned(v.clone())
                                    };
                                (key, value)
                            })
                            .collect();

                    let mut filter_values: HashSet<(String, String)> = HashSet::new();
                    let service_name_str = service_name
                        .clone()
                        .unwrap_or_else(|| "unknown".to_string());
                    if !service_name_str.is_empty() && service_name_str != "unknown" {
                        filter_values
                            .insert(("service_name".to_string(), service_name_str.clone()));
                    }

                    let attribute_mappings = [
                        ("deployment.environment", "environment"),
                        ("service.version", "version"),
                        ("cloud.region", "region"),
                        ("host.name", "host_name"),
                        ("k8s.pod.name", "pod_name"),
                    ];

                    for (otel_key, filter_key) in attribute_mappings {
                        if let Some(value) = resource_attrs.get(otel_key) {
                            if !value.is_empty() {
                                filter_values.insert((filter_key.to_string(), value.clone()));
                            }
                        }
                    }

                    for (attr_type, attr_value) in filter_values {
                        filter_rows.push(FilterValueInsert {
                            project_id: project_id_str.clone(),
                            attribute_type: attr_type,
                            attribute_value: attr_value,
                            last_seen: now,
                        });
                    }

                    for scope_span in resource_span.scope_spans {
                        for span in scope_span.spans {
                            let span_attrs = convert_attributes_to_hashmap(&span.attributes);

                            if LlmSpanProcessor::is_llm_span(&span_attrs) {
                                let trace_id = format_trace_id(&span.trace_id);
                                let span_id = format_span_id(&span.span_id);
                                let duration_nanos =
                                    if span.end_time_unix_nano > span.start_time_unix_nano {
                                        span.end_time_unix_nano - span.start_time_unix_nano
                                    } else {
                                        0
                                    };

                                let status = span.status.as_ref();
                                let status_code = status
                                    .map(|s| if s.code() == opentelemetry_proto::tonic::trace::v1::status::StatusCode::Error {
                                        "STATUS_CODE_ERROR"
                                    } else {
                                        "STATUS_CODE_OK"
                                    })
                                    .unwrap_or("STATUS_CODE_OK");
                                let status_message =
                                    status.map(|s| s.message.as_str()).unwrap_or("");

                                match llm_processor
                                    .process_span(
                                        &raw_payload.project_key,
                                        &trace_id,
                                        &span_id,
                                        &span.name,
                                        span.start_time_unix_nano,
                                        duration_nanos,
                                        status_code,
                                        status_message,
                                        &span_attrs,
                                        &resource_attrs,
                                    )
                                    .await
                                {
                                    Ok(llm_request) => {
                                        llm_requests_by_project
                                            .entry(project_id)
                                            .or_default()
                                            .push(llm_request);
                                    }
                                    Err(e) => {
                                        warn!("[SPANS_WORKER] Failed to process LLM span: {}", e)
                                    }
                                }
                            }

                            let timestamp = if span.start_time_unix_nano > 0 {
                                DateTime::from_timestamp(
                                    (span.start_time_unix_nano / 1_000_000_000) as i64,
                                    (span.start_time_unix_nano % 1_000_000_000) as u32,
                                )
                                .unwrap_or(now)
                            } else {
                                now
                            };

                            let duration_ns = if span.end_time_unix_nano > span.start_time_unix_nano
                            {
                                (span.end_time_unix_nano - span.start_time_unix_nano) as i64
                            } else {
                                0
                            };

                            let span_kind_str = match span.kind() {
                                opentelemetry_proto::tonic::trace::v1::span::SpanKind::Unspecified => "SPAN_KIND_UNSPECIFIED",
                                opentelemetry_proto::tonic::trace::v1::span::SpanKind::Internal => "SPAN_KIND_INTERNAL",
                                opentelemetry_proto::tonic::trace::v1::span::SpanKind::Server => "SPAN_KIND_SERVER",
                                opentelemetry_proto::tonic::trace::v1::span::SpanKind::Client => "SPAN_KIND_CLIENT",
                                opentelemetry_proto::tonic::trace::v1::span::SpanKind::Producer => "SPAN_KIND_PRODUCER",
                                opentelemetry_proto::tonic::trace::v1::span::SpanKind::Consumer => "SPAN_KIND_CONSUMER",
                            };

                            let (status_code_str, status_message_str) = if let Some(status) =
                                &span.status
                            {
                                let code = match status.code() {
                                    opentelemetry_proto::tonic::trace::v1::status::StatusCode::Unset => "STATUS_CODE_UNSET",
                                    opentelemetry_proto::tonic::trace::v1::status::StatusCode::Ok => "STATUS_CODE_OK",
                                    opentelemetry_proto::tonic::trace::v1::status::StatusCode::Error => "STATUS_CODE_ERROR",
                                };
                                (code, status.message.clone())
                            } else {
                                ("STATUS_CODE_UNSET", String::new())
                            };

                            let span_attrs_vec = convert_attributes_to_vec(&span.attributes);
                            let events_json = serde_json::to_string(&span.events)
                                .unwrap_or_else(|_| "[]".to_string());
                            let links_json = serde_json::to_string(&span.links)
                                .unwrap_or_else(|_| "[]".to_string());

                            let mut span_name = span.name.clone();
                            if pii_enabled {
                                span_name = crate::pii::mask_pii(&span_name).into_owned();
                            }

                            span_rows.push(SpanInsert {
                                project_id: project_id_str.clone(),
                                trace_id: format_trace_id(&span.trace_id),
                                span_id: format_span_id(&span.span_id),
                                parent_span_id: if span.parent_span_id.is_empty() {
                                    String::new()
                                } else {
                                    format_span_id(&span.parent_span_id)
                                },
                                trace_state: span.trace_state.clone(),
                                span_name,
                                span_kind: span_kind_str.to_string(),
                                service_name: service_name_str.clone(),
                                timestamp,
                                duration: duration_ns,
                                status_code: status_code_str.to_string(),
                                status_message: status_message_str,
                                span_attributes: span_attrs_vec,
                                resource_attributes: resource_attrs_vec.clone(),
                                events: events_json,
                                links: links_json,
                            });
                        }
                    }
                }
            }
            ParsedTracePayload::Span(span) => {
                let Some(&project_id) = project_map.get(&span.project_key) else {
                    continue;
                };
                let project_id_str = project_id.to_string();
                *usage_bytes.entry(project_id_str.clone()).or_default() += kafka_msg_bytes as u64;

                let timestamp = span.start_time.unwrap_or(now);
                let duration_ns = span.duration_ns.unwrap_or(0);
                let service_name = span
                    .service_name
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string());

                let span_kind_str = match span.span_kind {
                    SpanKind::SpanKindUnspecified => "SPAN_KIND_UNSPECIFIED",
                    SpanKind::SpanKindInternal => "SPAN_KIND_INTERNAL",
                    SpanKind::SpanKindServer => "SPAN_KIND_SERVER",
                    SpanKind::SpanKindClient => "SPAN_KIND_CLIENT",
                    SpanKind::SpanKindProducer => "SPAN_KIND_PRODUCER",
                    SpanKind::SpanKindConsumer => "SPAN_KIND_CONSUMER",
                };

                let status_code_str = match span.status_code {
                    SpanStatusCode::StatusCodeUnset => "STATUS_CODE_UNSET",
                    SpanStatusCode::StatusCodeOk => "STATUS_CODE_OK",
                    SpanStatusCode::StatusCodeError => "STATUS_CODE_ERROR",
                };

                if !service_name.is_empty() && service_name != "unknown" {
                    filter_rows.push(FilterValueInsert {
                        project_id: project_id_str.clone(),
                        attribute_type: "service_name".to_string(),
                        attribute_value: service_name.clone(),
                        last_seen: now,
                    });
                }

                let span_attrs_vec: Vec<(Cow<'static, str>, Cow<'static, str>)> = span
                    .span_attributes
                    .iter()
                    .map(|(k, v)| (Cow::Owned(k.clone()), Cow::Owned(v.clone())))
                    .collect();
                let resource_attrs_vec: Vec<(Cow<'static, str>, Cow<'static, str>)> = span
                    .resource_attributes
                    .iter()
                    .map(|(k, v)| (Cow::Owned(k.clone()), Cow::Owned(v.clone())))
                    .collect();
                let events_json = span
                    .events
                    .as_ref()
                    .map(|e| serde_json::to_string(e).unwrap_or_else(|_| "[]".to_string()))
                    .unwrap_or_else(|| "[]".to_string());
                let links_json = span
                    .links
                    .as_ref()
                    .map(|l| serde_json::to_string(l).unwrap_or_else(|_| "[]".to_string()))
                    .unwrap_or_else(|| "[]".to_string());

                span_rows.push(SpanInsert {
                    project_id: project_id_str,
                    trace_id: span.trace_id,
                    span_id: span.span_id,
                    parent_span_id: span.parent_span_id.unwrap_or_default(),
                    trace_state: span.trace_state.unwrap_or_default(),
                    span_name: span.span_name,
                    span_kind: span_kind_str.to_string(),
                    service_name,
                    timestamp,
                    duration: duration_ns,
                    status_code: status_code_str.to_string(),
                    status_message: span.status_message.unwrap_or_default(),
                    span_attributes: span_attrs_vec,
                    resource_attributes: resource_attrs_vec,
                    events: events_json,
                    links: links_json,
                });
            }
        }
    }

    // Step 4: Write LLM requests (grouped by project)
    let total_llm: usize = llm_requests_by_project.values().map(|v| v.len()).sum();
    if total_llm > 0 {
        async {
            for (project_id, requests) in &llm_requests_by_project {
                if let Err(e) =
                    write_llm_requests_to_clickhouse(clickhouse_pool, *project_id, requests).await
                {
                    error!(
                        "[SPANS_WORKER] Failed to write LLM requests for project {}: {}",
                        project_id, e
                    );
                } else {
                    debug!(
                        "[SPANS_WORKER] Wrote {} LLM requests for project {}",
                        requests.len(),
                        project_id
                    );
                }
            }
        }
        .instrument(tracing::info_span!("write_llm_requests", count = total_llm))
        .await;
    }

    tracing::Span::current().record("spans_count", span_rows.len());
    Ok((span_rows, filter_rows, usage_bytes))
}

async fn write_llm_requests_to_clickhouse(
    clickhouse_pool: &ClickHousePool,
    project_id: Uuid,
    requests: &[LlmRequest],
) -> Result<()> {
    use clickhouse::Row;
    use serde::Serialize;

    #[derive(Row, Serialize)]
    struct LlmRequestInsert {
        project_id: String,
        request_id: String,
        trace_id: String,
        span_id: String,
        gen_ai_system: String,
        gen_ai_request_model: String,
        gen_ai_response_model: String,
        gen_ai_operation_name: String,
        input_tokens: u32,
        output_tokens: u32,
        total_tokens: u32,
        cache_read_tokens: u32,
        cache_write_tokens: u32,
        cost_usd: f64,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: DateTime<Utc>,
        duration_ms: u32,
        time_to_first_token_ms: u32,
        status_code: String,
        error_type: String,
        error_message: String,
        session_id: String,
        session_name: String,
        user_id: String,
        request_messages: String,
        response_content: String,
        properties: Vec<(String, String)>,
        scores: Vec<(String, f64)>,
        service_name: String,
        rollout_id: String,
        rollout_variant: String,
        prompt_config_id: String,
        prompt_version_id: String,
    }

    let mut inserter = clickhouse_pool
        .as_ref()
        .inserter::<LlmRequestInsert>("llm_requests")
        .with_period(Some(Duration::from_secs(30)))
        .with_max_rows(500_000);

    let project_id_str = project_id.to_string();

    for req in requests {
        let row = LlmRequestInsert {
            project_id: project_id_str.clone(),
            request_id: req.request_id.clone(),
            trace_id: req.trace_id.clone(),
            span_id: req.span_id.clone(),
            gen_ai_system: req.gen_ai_system.clone(),
            gen_ai_request_model: req.gen_ai_request_model.clone(),
            gen_ai_response_model: req.gen_ai_response_model.clone(),
            gen_ai_operation_name: req.gen_ai_operation_name.clone(),
            input_tokens: req.input_tokens,
            output_tokens: req.output_tokens,
            total_tokens: req.total_tokens,
            cache_read_tokens: req.cache_read_tokens,
            cache_write_tokens: req.cache_write_tokens,
            cost_usd: req.cost_usd.try_into().unwrap_or(0.0),
            timestamp: req.timestamp,
            duration_ms: req.duration_ms,
            time_to_first_token_ms: req.time_to_first_token_ms,
            status_code: req.status_code.clone(),
            error_type: req.error_type.clone(),
            error_message: req.error_message.clone(),
            session_id: req.session_id.clone(),
            session_name: req.session_name.clone(),
            user_id: req.user_id.clone(),
            request_messages: req.request_messages.clone(),
            response_content: req.response_content.clone(),
            properties: req
                .properties
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            scores: req.scores.iter().map(|(k, v)| (k.clone(), *v)).collect(),
            service_name: req.service_name.clone(),
            rollout_id: req.rollout_id.clone(),
            rollout_variant: req.rollout_variant.clone(),
            prompt_config_id: req.prompt_config_id.clone(),
            prompt_version_id: req.prompt_version_id.clone(),
        };

        inserter.write(&row).await?;
    }

    inserter.commit().await?;
    inserter.end().await?;

    Ok(())
}

// ============================================================================
// Helper Functions
// ============================================================================

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

fn convert_attributes_to_hashmap(
    attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
) -> HashMap<String, String> {
    let mut map = HashMap::with_capacity(attributes.len());
    let mut itoa_buf = itoa::Buffer::new();

    for attr in attributes {
        if let Some(value) = &attr.value {
            let value_str = match &value.value {
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => {
                    s.clone()
                }
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => {
                    itoa_buf.format(*i).to_owned()
                }
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d)) => {
                    d.to_string()
                }
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b)) => {
                    (if *b { "true" } else { "false" }).to_owned()
                }
                _ => continue,
            };
            map.insert(attr.key.clone(), value_str);
        }
    }
    map
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

fn format_trace_id(bytes: &[u8]) -> String {
    if bytes.len() == 16 {
        format!(
            "{:032x}",
            u128::from_be_bytes(bytes.try_into().unwrap_or([0u8; 16]))
        )
    } else {
        hex::encode(bytes)
    }
}

fn format_span_id(bytes: &[u8]) -> String {
    if bytes.len() == 8 {
        format!(
            "{:016x}",
            u64::from_be_bytes(bytes.try_into().unwrap_or([0u8; 8]))
        )
    } else {
        hex::encode(bytes)
    }
}

// ── Span-derived RED metrics ────────────────────────────────────────────────

// TODO: This is a hardcoded RED metric generator for HTTP server spans.
// We want to evolve this into a general "Generate Metrics from Spans" feature
// where users can define custom metrics from arbitrary span attributes via the UI,
// similar to Datadog's "Generate Metrics" (APM > Generate Metrics).
// See: https://docs.datadoghq.com/tracing/trace_pipeline/generate_metrics/

const RED_METRIC_NAME: &str = "http.server.request.duration";

const HISTOGRAM_BOUNDS_MS: &[f64] = &[
    2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0, 2048.0, 4096.0, 8192.0,
    16384.0,
];

/// Produces OTLP metric payloads to the Kafka metrics topic for each
/// `SPAN_KIND_SERVER` span in projects that have `span_metrics_enabled`.
///
/// The payloads are picked up by the existing `metrics_worker`, which handles
/// ClickHouse insertion, usage tracking, and billing — no logic is duplicated.
async fn produce_red_metrics(
    redis_pool: &RedisPool,
    db_pool: &DbPool,
    kafka_producer: &KafkaProducer,
    metrics_topic: &str,
    span_rows: &[SpanInsert],
) {
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use opentelemetry_proto::tonic::metrics::v1::{
        metric, AggregationTemporality, Histogram, HistogramDataPoint, Metric, ResourceMetrics,
        ScopeMetrics,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use prost::Message;

    let server_spans: Vec<&SpanInsert> = span_rows
        .iter()
        .filter(|s| s.span_kind == "SPAN_KIND_SERVER")
        .collect();

    if server_spans.is_empty() {
        return;
    }

    // Group by project_id to check the setting once per project
    let mut by_project: HashMap<&str, Vec<&SpanInsert>> = HashMap::new();
    for span in &server_spans {
        by_project
            .entry(span.project_id.as_str())
            .or_default()
            .push(span);
    }

    for (project_id_str, spans) in &by_project {
        let project_id = match Uuid::parse_str(project_id_str) {
            Ok(id) => id,
            Err(_) => continue,
        };

        if !reiver_core::project_settings::get_span_metrics_enabled_cached(
            redis_pool, db_pool, project_id,
        )
        .await
        {
            continue;
        }

        // Build one OTLP ExportMetricsServiceRequest per (project, service_name)
        let mut by_service: HashMap<&str, Vec<&SpanInsert>> = HashMap::new();
        for span in spans {
            by_service
                .entry(span.service_name.as_str())
                .or_default()
                .push(span);
        }

        for (service_name, service_spans) in &by_service {
            let resource = Resource {
                attributes: vec![kv("service.name", service_name)],
                ..Default::default()
            };

            let mut metrics: Vec<Metric> = Vec::new();

            for span in service_spans {
                let duration_ms = span.duration as f64 / 1_000_000.0;
                let time_unix_nano = span.timestamp.timestamp_nanos_opt().unwrap_or(0) as u64;

                let mut attrs = Vec::new();
                for (k, v) in &span.span_attributes {
                    match k.as_ref() {
                        "http.method" | "http.request.method" => {
                            attrs.push(kv("http.method", v.as_ref()));
                        }
                        "http.route" => {
                            attrs.push(kv("http.route", v.as_ref()));
                        }
                        "http.response.status_code" | "http.status_code" => {
                            attrs.push(kv("http.response.status_code", v.as_ref()));
                        }
                        _ => {}
                    }
                }

                // Build OTLP histogram bucket counts.
                // bucket_counts has len = explicit_bounds.len() + 1
                let mut bucket_counts = vec![0u64; HISTOGRAM_BOUNDS_MS.len() + 1];
                let mut placed = false;
                for (i, &bound) in HISTOGRAM_BOUNDS_MS.iter().enumerate() {
                    if !placed && duration_ms <= bound {
                        bucket_counts[i] = 1;
                        placed = true;
                        break;
                    }
                }
                if !placed {
                    // Falls in the +Inf bucket
                    *bucket_counts.last_mut().unwrap() = 1;
                }

                let data_point = HistogramDataPoint {
                    attributes: attrs,
                    start_time_unix_nano: time_unix_nano,
                    time_unix_nano,
                    count: 1,
                    sum: Some(duration_ms),
                    bucket_counts,
                    explicit_bounds: HISTOGRAM_BOUNDS_MS.to_vec(),
                    exemplars: vec![],
                    flags: 0,
                    min: Some(duration_ms),
                    max: Some(duration_ms),
                };

                metrics.push(Metric {
                    name: RED_METRIC_NAME.to_string(),
                    description: String::new(),
                    unit: "ms".to_string(),
                    metadata: vec![],
                    data: Some(metric::Data::Histogram(Histogram {
                        data_points: vec![data_point],
                        aggregation_temporality:
                            AggregationTemporality::Delta as i32,
                    })),
                });
            }

            let request = ExportMetricsServiceRequest {
                resource_metrics: vec![ResourceMetrics {
                    resource: Some(resource),
                    scope_metrics: vec![ScopeMetrics {
                        scope: None,
                        metrics,
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                }],
            };

            let raw_bytes = request.encode_to_vec();

            let envelope = reiver_core::models::RawOtlpMetricsPayload {
                project_key: project_id_str.to_string(),
                content_type: "protobuf".to_string(),
                raw_bytes,
            };

            match serde_json::to_vec(&envelope) {
                Ok(payload) => {
                    if let Err(e) = kafka_producer
                        .send_to_topic(metrics_topic, project_id_str, &payload)
                        .await
                    {
                        error!(
                            "[SPANS_WORKER] Failed to produce RED metrics to Kafka for project {}: {}",
                            project_id_str, e
                        );
                    } else {
                        debug!(
                            "[SPANS_WORKER] Produced {} RED metric data points for project {} service {}",
                            service_spans.len(),
                            project_id_str,
                            service_name,
                        );
                    }
                }
                Err(e) => {
                    error!(
                        "[SPANS_WORKER] Failed to serialize RED metrics envelope: {}",
                        e
                    );
                }
            }
        }
    }
}

fn kv(key: &str, value: &str) -> opentelemetry_proto::tonic::common::v1::KeyValue {
    opentelemetry_proto::tonic::common::v1::KeyValue {
        key: key.to_string(),
        value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
            value: Some(
                opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                    value.to_string(),
                ),
            ),
        }),
    }
}
