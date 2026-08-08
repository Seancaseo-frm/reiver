//! Metrics ingestion worker - consumes raw OTLP metrics from Kafka and writes to ClickHouse
//!
//! Handles:
//! - OTLP parsing (protobuf or JSON)
//! - project_key → project_id resolution
//! - Converting OTLP metrics to internal format
//! - Writing metrics to ClickHouse
//!
//! Performance optimizations:
//! - Uses persistent ClickHouse inserters with batching
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
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, instrument, warn, Instrument};
use uuid::Uuid;

use std::cell::RefCell;

thread_local! {
    static SIMD_JSON_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(65536));
}

#[inline]
fn parse_json_simd<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, simd_json::Error> {
    SIMD_JSON_BUFFER.with(|buf| {
        let mut buf = buf.borrow_mut();
        buf.clear();
        buf.extend_from_slice(bytes);
        simd_json::from_slice(&mut buf)
    })
}

use crate::clickhouse_db::ClickHousePool;
use crate::config::Config;
use crate::db::DbPool;
use crate::metrics::{compute_fingerprint, MetricType, Temporality};
use crate::models::RawOtlpMetricsPayload;
use reiver_core::promql::metric_names::{
    resolve_label_name, resolve_storage_name, synthetic_labels_for,
};

use crate::metrics::insert_types::{
    ExemplarInsert, FilterValueInsert, SampleInsert, TimeSeriesInsert,
};

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

pub async fn start_metrics_worker(
    kafka_hosts: &str,
    metrics_topic: &str,
    client_id: Option<&str>,
    db_pool: Arc<DbPool>,
    clickhouse_pool: Arc<ClickHousePool>,
    _config: Arc<Config>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<JoinHandle<()>> {
    info!(
        "Creating metrics ingestion worker for topic: {}",
        metrics_topic
    );

    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", kafka_hosts)
        .set("group.id", "reiver-metrics-worker")
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

    consumer.subscribe(&[metrics_topic])?;
    info!("Subscribed to Kafka topic: {}", metrics_topic);

    let project_id_cache: ProjectIdCache = Arc::new(Cache::new(10_000));

    let handle = tokio::spawn(async move {
        info!(
            "[METRICS_WORKER] Started, batch_timeout={}ms max_batch_bytes={}KB",
            BATCH_TIMEOUT.as_millis(),
            MAX_BATCH_BYTES / 1024,
        );

        let mut samples_inserter = clickhouse_pool
            .as_ref()
            .inserter::<SampleInsert>("samples_v1")
            .with_period(Some(Duration::from_secs(30)))
            .with_max_rows(50_000);

        let mut time_series_inserter = clickhouse_pool
            .as_ref()
            .inserter::<TimeSeriesInsert>("time_series_v1")
            .with_period(Some(Duration::from_secs(30)))
            .with_max_rows(50_000);

        let mut filter_inserter = clickhouse_pool
            .as_ref()
            .inserter::<FilterValueInsert>("otlp_attributes")
            .with_period(Some(Duration::from_secs(30)))
            .with_max_rows(50_000);

        let mut exemplar_inserter = clickhouse_pool
            .as_ref()
            .inserter::<ExemplarInsert>("metric_exemplars")
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
        let mut total_samples_written = 0u64;
        let mut pending_sample_writes = 0u64;
        let mut pending_ts_writes = 0u64;
        let mut pending_filter_writes = 0u64;
        let mut pending_exemplar_writes = 0u64;
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
                        if pending_sample_writes > 0 {
                            let flush_start = Instant::now();
                            if let Err(e) = samples_inserter.commit().await {
                                error!("[METRICS_WORKER] Periodic flush: failed to commit samples: {}", e);
                            } else {
                                info!(
                                    "[METRICS_WORKER] Flushed {} samples to ClickHouse in {:.1}ms",
                                    pending_sample_writes, flush_start.elapsed().as_secs_f64() * 1000.0,
                                );
                                total_samples_written += pending_sample_writes;
                                pending_sample_writes = 0;
                            }
                        }
                        if pending_ts_writes > 0 {
                            if let Err(e) = time_series_inserter.commit().await {
                                error!("[METRICS_WORKER] Periodic flush: failed to commit time_series: {}", e);
                            } else {
                                debug!("[METRICS_WORKER] Flushed {} time_series", pending_ts_writes);
                                pending_ts_writes = 0;
                            }
                        }
                        if pending_filter_writes > 0 {
                            if let Err(e) = filter_inserter.commit().await {
                                error!("[METRICS_WORKER] Periodic flush: failed to commit filters: {}", e);
                            } else {
                                debug!("[METRICS_WORKER] Flushed {} filters", pending_filter_writes);
                                pending_filter_writes = 0;
                            }
                        }
                        if pending_exemplar_writes > 0 {
                            if let Err(e) = exemplar_inserter.commit().await {
                                error!("[METRICS_WORKER] Periodic flush: failed to commit exemplars: {}", e);
                            } else {
                                debug!("[METRICS_WORKER] Flushed {} exemplars", pending_exemplar_writes);
                                pending_exemplar_writes = 0;
                            }
                        }
                        if let Err(e) = usage_inserter.commit().await {
                            error!("[METRICS_WORKER] Periodic flush: failed to commit usage: {}", e);
                        }
                    }

                    _ = throughput_interval.tick() => {
                        let elapsed = window_start.elapsed();
                        if window_msg_count > 0 || message_count > 0 {
                            let msg_per_sec = if elapsed.as_secs_f64() > 0.0 {
                                window_msg_count as f64 / elapsed.as_secs_f64()
                            } else { 0.0 };
                            info!(
                                "[METRICS_WORKER] total_msgs={} window_msgs={} msg/s={:.1} total_samples={} pending_samples={} pending_ts={} errors={}",
                                message_count, window_msg_count, msg_per_sec,
                                total_samples_written, pending_sample_writes, pending_ts_writes, error_count,
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
                                error!("[METRICS_WORKER] Error receiving message: {}", e);
                            }
                            None => { stream_ended = true; break; }
                        }
                    }
                }
            }

            if should_shutdown {
                info!("[METRICS_WORKER] Received shutdown signal");
                if pending_sample_writes > 0 {
                    if let Err(e) = samples_inserter.commit().await {
                        error!(
                            "[METRICS_WORKER] Failed to commit samples inserter on shutdown: {}",
                            e
                        );
                    }
                }
                if pending_ts_writes > 0 {
                    if let Err(e) = time_series_inserter.commit().await {
                        error!("[METRICS_WORKER] Failed to commit time_series inserter on shutdown: {}", e);
                    }
                }
                if pending_filter_writes > 0 {
                    if let Err(e) = filter_inserter.commit().await {
                        error!(
                            "[METRICS_WORKER] Failed to commit filter inserter on shutdown: {}",
                            e
                        );
                    }
                }
                if pending_exemplar_writes > 0 {
                    if let Err(e) = exemplar_inserter.commit().await {
                        error!(
                            "[METRICS_WORKER] Failed to commit exemplar inserter on shutdown: {}",
                            e
                        );
                    }
                }
                if let Err(e) = usage_inserter.commit().await {
                    error!(
                        "[METRICS_WORKER] Failed to commit usage inserter on shutdown: {}",
                        e
                    );
                }
                break;
            }

            if !raw_payloads.is_empty() {
                let batch_msg_count = raw_payloads.len() as u64;

                // Phase 2: Process the entire batch
                match process_metrics_batch(&db_pool, &project_id_cache, raw_payloads, batch_bytes)
                    .await
                {
                    Ok((sample_rows, ts_rows, filter_rows, exemplar_rows, usage_counts)) => {
                        for row in &sample_rows {
                            if let Err(e) = samples_inserter.write(row).await {
                                error!("[METRICS_WORKER] Failed to write sample: {}", e);
                            } else {
                                pending_sample_writes += 1;
                            }
                        }
                        for row in &ts_rows {
                            if let Err(e) = time_series_inserter.write(row).await {
                                error!("[METRICS_WORKER] Failed to write time_series: {}", e);
                            } else {
                                pending_ts_writes += 1;
                            }
                        }
                        for row in &filter_rows {
                            if let Err(e) = filter_inserter.write(row).await {
                                error!("[METRICS_WORKER] Failed to write filter: {}", e);
                            } else {
                                pending_filter_writes += 1;
                            }
                        }
                        for row in &exemplar_rows {
                            if let Err(e) = exemplar_inserter.write(row).await {
                                error!("[METRICS_WORKER] Failed to write exemplar: {}", e);
                            } else {
                                pending_exemplar_writes += 1;
                            }
                        }
                        let today = Utc::now().date_naive();
                        for (project_id, count) in &usage_counts {
                            if let Err(e) = usage_inserter
                                .write(&UsageInsert {
                                    project_id: project_id.clone(),
                                    event_type: "metric".to_string(),
                                    date: today,
                                    value: *count,
                                })
                                .await
                            {
                                error!("[METRICS_WORKER] Failed to write usage: {}", e);
                            }
                        }

                        message_count += batch_msg_count;
                        window_msg_count += batch_msg_count;

                        if pending_sample_writes >= 10_000 {
                            let flush_start = Instant::now();
                            if let Err(e) = samples_inserter.commit().await {
                                error!("[METRICS_WORKER] Failed to commit samples inserter: {}", e);
                            } else {
                                info!(
                                    "[METRICS_WORKER] Batch commit: {} samples in {:.1}ms",
                                    pending_sample_writes,
                                    flush_start.elapsed().as_secs_f64() * 1000.0,
                                );
                                total_samples_written += pending_sample_writes;
                                pending_sample_writes = 0;
                            }
                        }
                        if pending_ts_writes >= 10_000 {
                            if let Err(e) = time_series_inserter.commit().await {
                                error!(
                                    "[METRICS_WORKER] Failed to commit time_series inserter: {}",
                                    e
                                );
                            } else {
                                pending_ts_writes = 0;
                            }
                        }
                        if pending_filter_writes >= 10_000 {
                            if let Err(e) = filter_inserter.commit().await {
                                error!("[METRICS_WORKER] Failed to commit filter inserter: {}", e);
                            } else {
                                pending_filter_writes = 0;
                            }
                        }
                        if pending_exemplar_writes >= 10_000 {
                            if let Err(e) = exemplar_inserter.commit().await {
                                error!(
                                    "[METRICS_WORKER] Failed to commit exemplar inserter: {}",
                                    e
                                );
                            } else {
                                debug!(
                                    "[METRICS_WORKER] Committed {} exemplars",
                                    pending_exemplar_writes
                                );
                                pending_exemplar_writes = 0;
                            }
                        }
                    }
                    Err(e) => {
                        error_count += batch_msg_count;
                        error!(
                            "[METRICS_WORKER] Failed to process batch ({} msgs, {} bytes): {}",
                            batch_msg_count, batch_bytes, e
                        );
                    }
                }
            }

            if stream_ended {
                break;
            }
        }

        if pending_sample_writes > 0 {
            if let Err(e) = samples_inserter.commit().await {
                error!("[METRICS_WORKER] Failed to final commit samples: {}", e);
            }
        }
        if pending_ts_writes > 0 {
            if let Err(e) = time_series_inserter.commit().await {
                error!("[METRICS_WORKER] Failed to final commit time_series: {}", e);
            }
        }
        if pending_filter_writes > 0 {
            if let Err(e) = filter_inserter.commit().await {
                error!("[METRICS_WORKER] Failed to final commit filters: {}", e);
            }
        }
        if pending_exemplar_writes > 0 {
            if let Err(e) = exemplar_inserter.commit().await {
                error!("[METRICS_WORKER] Failed to final commit exemplars: {}", e);
            }
        }
        if let Err(e) = samples_inserter.end().await {
            error!("[METRICS_WORKER] Failed to end samples inserter: {}", e);
        }
        if let Err(e) = time_series_inserter.end().await {
            error!("[METRICS_WORKER] Failed to end time_series inserter: {}", e);
        }
        if let Err(e) = filter_inserter.end().await {
            error!("[METRICS_WORKER] Failed to end filter inserter: {}", e);
        }
        if let Err(e) = exemplar_inserter.end().await {
            error!("[METRICS_WORKER] Failed to end exemplar inserter: {}", e);
        }
        if let Err(e) = usage_inserter.commit().await {
            error!("[METRICS_WORKER] Failed to final commit usage: {}", e);
        }
        if let Err(e) = usage_inserter.end().await {
            error!("[METRICS_WORKER] Failed to end usage inserter: {}", e);
        }
        info!(
            "[METRICS_WORKER] Stopped (total_msgs={}, total_samples={})",
            message_count, total_samples_written
        );
    });

    Ok(handle)
}

/// Process a batch of Kafka messages collected over a time window.
///
/// Parses all OTLP metric payloads, resolves project IDs (deduplicated),
/// and builds sample/time_series/filter/exemplar rows for the entire batch.
#[instrument(name = "process_metrics", skip_all, err, fields(
    messages_count = raw_payloads.len(),
    batch_bytes = batch_bytes,
    samples_count = tracing::field::Empty,
))]
async fn process_metrics_batch(
    db_pool: &DbPool,
    project_id_cache: &ProjectIdCache,
    raw_payloads: Vec<Vec<u8>>,
    batch_bytes: usize,
) -> Result<(
    Vec<SampleInsert>,
    Vec<TimeSeriesInsert>,
    Vec<FilterValueInsert>,
    Vec<ExemplarInsert>,
    HashMap<String, u64>,
)> {
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use prost::Message;

    // Step 1: Parse Kafka JSON envelopes
    let otlp_payloads = {
        let _guard = tracing::info_span!("parse_messages", count = raw_payloads.len()).entered();
        let mut parsed = Vec::with_capacity(raw_payloads.len());
        for payload in &raw_payloads {
            match parse_json_simd::<RawOtlpMetricsPayload>(payload) {
                Ok(p) => parsed.push(p),
                Err(e) => warn!("[METRICS_WORKER] Failed to parse payload: {}", e),
            }
        }
        parsed
    };

    // Step 2: Resolve unique project IDs
    let mut project_map: HashMap<String, Uuid> = HashMap::new();

    {
        let unique_keys: HashSet<&str> = otlp_payloads
            .iter()
            .map(|p| p.project_key.as_str())
            .collect();

        for key in unique_keys {
            let project_id = match resolve_project_id_cached(db_pool, project_id_cache, key)
                .instrument(tracing::info_span!("resolve_project_id"))
                .await?
            {
                Some(id) => id,
                None => {
                    warn!("[METRICS_WORKER] Project key not found: {}", key);
                    continue;
                }
            };
            project_map.insert(key.to_string(), project_id);
        }
    }

    // Step 3: Parse OTLP and build all rows
    let (sample_rows, ts_rows, filter_rows, exemplar_rows) = {
        let _guard = tracing::info_span!("build_sample_rows").entered();
        let now = Utc::now();

        let mut sample_rows: Vec<SampleInsert> = Vec::with_capacity(128);
        let mut ts_rows: Vec<TimeSeriesInsert> = Vec::with_capacity(128);
        let mut filter_rows: Vec<FilterValueInsert> = Vec::new();
        let mut exemplar_rows: Vec<ExemplarInsert> = Vec::new();
        let mut filter_values: HashSet<(String, String)> = HashSet::with_capacity(16);

        for raw_payload in otlp_payloads {
            let Some(&project_id) = project_map.get(&raw_payload.project_key) else {
                continue;
            };
            let project_id_str = project_id.to_string();

            let export_request = if raw_payload.content_type == "json" {
                match serde_json::from_slice::<ExportMetricsServiceRequest>(&raw_payload.raw_bytes)
                {
                    Ok(r) => r,
                    Err(e) => {
                        warn!("[METRICS_WORKER] Failed to parse JSON metrics: {}", e);
                        continue;
                    }
                }
            } else {
                match ExportMetricsServiceRequest::decode(&raw_payload.raw_bytes[..]) {
                    Ok(r) => r,
                    Err(e) => {
                        warn!("[METRICS_WORKER] Failed to parse protobuf metrics: {}", e);
                        continue;
                    }
                }
            };

            for resource_metric in export_request.resource_metrics {
                let resource = resource_metric.resource.unwrap_or_default();
                let resource_tags = convert_attributes_to_key_values(&resource.attributes);
                let resource_attrs_map: HashMap<String, String> = resource_tags
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.clone()))
                    .collect();
                let resource_attrs_vec: Vec<(String, String)> = resource_attrs_map
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();

                if let Some(service_name) = extract_service_name(&resource.attributes) {
                    if !service_name.is_empty() {
                        filter_values.insert(("service_name".to_string(), service_name));
                    }
                }

                let attribute_mappings = [
                    ("deployment.environment", "environment"),
                    ("service.version", "version"),
                    ("cloud.region", "region"),
                    ("host.name", "host_name"),
                    ("k8s.pod.name", "pod_name"),
                ];
                for (otel_key, filter_key) in attribute_mappings {
                    if let Some(value) = extract_attribute_string(&resource.attributes, otel_key) {
                        if !value.is_empty() {
                            filter_values.insert((filter_key.to_string(), value));
                        }
                    }
                }

                for scope_metrics in resource_metric.scope_metrics {
                    for metric in scope_metrics.metrics {
                        let (metric_name, synthetic_labels) = map_metric(&metric.name);

                        match metric.data {
                            Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(
                                gauge,
                            )) => {
                                for data_point in gauge.data_points {
                                    let value = extract_number_value(&data_point.value);
                                    let timestamp = extract_timestamp(data_point.time_unix_nano);
                                    let unix_milli = timestamp.timestamp_millis();
                                    let metric_attrs =
                                        convert_attributes_to_key_values(&data_point.attributes);
                                    let metric_attrs_map: HashMap<String, String> = metric_attrs
                                        .iter()
                                        .map(|(k, v)| (k.to_string(), v.clone()))
                                        .collect();
                                    let metric_attrs_vec: Vec<(String, String)> = metric_attrs_map
                                        .iter()
                                        .map(|(k, v)| (k.clone(), v.clone()))
                                        .collect();

                                    let mut labels = resource_tags.clone();
                                    labels.extend(metric_attrs);
                                    let raw_btree: BTreeMap<String, String> = labels
                                        .iter()
                                        .map(|(k, v)| (k.to_string(), v.clone()))
                                        .collect();
                                    let mut labels_btree = map_label_keys(&raw_btree);
                                    for &(k, v) in synthetic_labels {
                                        labels_btree.insert(k.to_string(), v.to_string());
                                    }
                                    let fingerprint =
                                        compute_fingerprint(&metric_name, &labels_btree);
                                    let labels_json = serde_json::to_string(&labels_btree)
                                        .unwrap_or_else(|_| "{}".to_string());

                                    sample_rows.push(SampleInsert {
                                        project_id,
                                        metric_name: metric_name.clone(),
                                        fingerprint,
                                        unix_milli,
                                        value,
                                        temporality: Temporality::Unspecified.as_str().to_string(),
                                        metric_type: MetricType::Gauge.as_str().to_string(),
                                        flags: 0,
                                        resource_attributes: resource_attrs_vec.clone(),
                                        metric_attributes: metric_attrs_vec.clone(),
                                        labels: labels_json.clone(),
                                    });

                                    ts_rows.push(TimeSeriesInsert {
                                        project_id,
                                        metric_name: metric_name.clone(),
                                        fingerprint,
                                        labels: labels_json,
                                        temporality: Temporality::Unspecified.as_str().to_string(),
                                        metric_type: MetricType::Gauge.as_str().to_string(),
                                        unix_milli,
                                        resource_attributes: resource_attrs_vec.clone(),
                                        metric_attributes: metric_attrs_vec,
                                    });

                                    extract_exemplars(
                                        &data_point.exemplars,
                                        project_id,
                                        &metric_name,
                                        fingerprint,
                                        data_point.time_unix_nano,
                                        &mut exemplar_rows,
                                    );
                                }
                            }
                            Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(
                                sum,
                            )) => {
                                let is_monotonic = sum.aggregation_temporality
                                    == opentelemetry_proto::tonic::metrics::v1::AggregationTemporality::Cumulative as i32
                                    && sum.is_monotonic;
                                let temporality = if is_monotonic {
                                    Temporality::Cumulative
                                } else {
                                    Temporality::Delta
                                };

                                for data_point in sum.data_points {
                                    let value = extract_number_value(&data_point.value);
                                    let timestamp = extract_timestamp(data_point.time_unix_nano);
                                    let unix_milli = timestamp.timestamp_millis();
                                    let metric_attrs =
                                        convert_attributes_to_key_values(&data_point.attributes);
                                    let metric_attrs_map: HashMap<String, String> = metric_attrs
                                        .iter()
                                        .map(|(k, v)| (k.to_string(), v.clone()))
                                        .collect();
                                    let metric_attrs_vec: Vec<(String, String)> = metric_attrs_map
                                        .iter()
                                        .map(|(k, v)| (k.clone(), v.clone()))
                                        .collect();

                                    let mut labels = resource_tags.clone();
                                    labels.extend(metric_attrs);
                                    let raw_btree: BTreeMap<String, String> = labels
                                        .iter()
                                        .map(|(k, v)| (k.to_string(), v.clone()))
                                        .collect();
                                    let mut labels_btree = map_label_keys(&raw_btree);
                                    for &(k, v) in synthetic_labels {
                                        labels_btree.insert(k.to_string(), v.to_string());
                                    }
                                    let fingerprint =
                                        compute_fingerprint(&metric_name, &labels_btree);
                                    let labels_json = serde_json::to_string(&labels_btree)
                                        .unwrap_or_else(|_| "{}".to_string());

                                    sample_rows.push(SampleInsert {
                                        project_id,
                                        metric_name: metric_name.clone(),
                                        fingerprint,
                                        unix_milli,
                                        value,
                                        temporality: temporality.as_str().to_string(),
                                        metric_type: MetricType::Sum.as_str().to_string(),
                                        flags: 0,
                                        resource_attributes: resource_attrs_vec.clone(),
                                        metric_attributes: metric_attrs_vec.clone(),
                                        labels: labels_json.clone(),
                                    });

                                    ts_rows.push(TimeSeriesInsert {
                                        project_id,
                                        metric_name: metric_name.clone(),
                                        fingerprint,
                                        labels: labels_json,
                                        temporality: temporality.as_str().to_string(),
                                        metric_type: MetricType::Sum.as_str().to_string(),
                                        unix_milli,
                                        resource_attributes: resource_attrs_vec.clone(),
                                        metric_attributes: metric_attrs_vec,
                                    });

                                    extract_exemplars(
                                        &data_point.exemplars,
                                        project_id,
                                        &metric_name,
                                        fingerprint,
                                        data_point.time_unix_nano,
                                        &mut exemplar_rows,
                                    );
                                }
                            }
                            Some(
                                opentelemetry_proto::tonic::metrics::v1::metric::Data::Histogram(
                                    hist,
                                ),
                            ) => {
                                for data_point in hist.data_points {
                                    let count = data_point.count as f64;
                                    let sum = data_point.sum.unwrap_or(0.0);
                                    let timestamp = extract_timestamp(data_point.time_unix_nano);
                                    let unix_milli = timestamp.timestamp_millis();
                                    let metric_attrs =
                                        convert_attributes_to_key_values(&data_point.attributes);
                                    let metric_attrs_map: HashMap<String, String> = metric_attrs
                                        .iter()
                                        .map(|(k, v)| (k.to_string(), v.clone()))
                                        .collect();
                                    let metric_attrs_vec: Vec<(String, String)> = metric_attrs_map
                                        .iter()
                                        .map(|(k, v)| (k.clone(), v.clone()))
                                        .collect();

                                    let mut labels = resource_tags.clone();
                                    labels.extend(metric_attrs.clone());
                                    let raw_btree: BTreeMap<String, String> = labels
                                        .iter()
                                        .map(|(k, v)| (k.to_string(), v.clone()))
                                        .collect();
                                    let mut labels_btree = map_label_keys(&raw_btree);
                                    for &(k, v) in synthetic_labels {
                                        labels_btree.insert(k.to_string(), v.to_string());
                                    }

                                    let count_name = format!("{}.count", metric_name);
                                    let count_fingerprint =
                                        compute_fingerprint(&count_name, &labels_btree);
                                    let labels_json = serde_json::to_string(&labels_btree)
                                        .unwrap_or_else(|_| "{}".to_string());

                                    sample_rows.push(SampleInsert {
                                        project_id,
                                        metric_name: count_name.clone(),
                                        fingerprint: count_fingerprint,
                                        unix_milli,
                                        value: count,
                                        temporality: Temporality::Delta.as_str().to_string(),
                                        metric_type: MetricType::Histogram.as_str().to_string(),
                                        flags: 0,
                                        resource_attributes: resource_attrs_vec.clone(),
                                        metric_attributes: metric_attrs_vec.clone(),
                                        labels: labels_json.clone(),
                                    });

                                    ts_rows.push(TimeSeriesInsert {
                                        project_id,
                                        metric_name: count_name,
                                        fingerprint: count_fingerprint,
                                        labels: labels_json.clone(),
                                        temporality: Temporality::Delta.as_str().to_string(),
                                        metric_type: MetricType::Histogram.as_str().to_string(),
                                        unix_milli,
                                        resource_attributes: resource_attrs_vec.clone(),
                                        metric_attributes: metric_attrs_vec.clone(),
                                    });

                                    let sum_name = format!("{}.sum", metric_name);
                                    let sum_fingerprint =
                                        compute_fingerprint(&sum_name, &labels_btree);

                                    sample_rows.push(SampleInsert {
                                        project_id,
                                        metric_name: sum_name.clone(),
                                        fingerprint: sum_fingerprint,
                                        unix_milli,
                                        value: sum,
                                        temporality: Temporality::Delta.as_str().to_string(),
                                        metric_type: MetricType::Histogram.as_str().to_string(),
                                        flags: 0,
                                        resource_attributes: resource_attrs_vec.clone(),
                                        metric_attributes: metric_attrs_vec.clone(),
                                        labels: labels_json.clone(),
                                    });

                                    ts_rows.push(TimeSeriesInsert {
                                        project_id,
                                        metric_name: sum_name,
                                        fingerprint: sum_fingerprint,
                                        labels: labels_json,
                                        temporality: Temporality::Delta.as_str().to_string(),
                                        metric_type: MetricType::Histogram.as_str().to_string(),
                                        unix_milli,
                                        resource_attributes: resource_attrs_vec.clone(),
                                        metric_attributes: metric_attrs_vec,
                                    });

                                    // Emit _bucket series so histogram_quantile() works.
                                    // OTEL bucket_counts has N+1 entries for N explicit_bounds
                                    // (the last entry is the +Inf bucket). Prometheus _bucket
                                    // values are cumulative.
                                    if !data_point.explicit_bounds.is_empty()
                                        && data_point.bucket_counts.len()
                                            == data_point.explicit_bounds.len() + 1
                                    {
                                        let bucket_name = format!("{}_bucket", metric_name);
                                        let mut cumulative: f64 = 0.0;

                                        for (i, bound) in
                                            data_point.explicit_bounds.iter().enumerate()
                                        {
                                            cumulative += data_point.bucket_counts[i] as f64;
                                            let le_str = format_le(*bound);
                                            let mut bucket_labels = labels_btree.clone();
                                            bucket_labels.insert("le".to_string(), le_str);
                                            let bucket_fp =
                                                compute_fingerprint(&bucket_name, &bucket_labels);
                                            let bucket_labels_json =
                                                serde_json::to_string(&bucket_labels)
                                                    .unwrap_or_else(|_| "{}".to_string());
                                            let bucket_attrs: Vec<(String, String)> = bucket_labels
                                                .iter()
                                                .map(|(k, v)| (k.clone(), v.clone()))
                                                .collect();

                                            sample_rows.push(SampleInsert {
                                                project_id,
                                                metric_name: bucket_name.clone(),
                                                fingerprint: bucket_fp,
                                                unix_milli,
                                                value: cumulative,
                                                temporality: Temporality::Delta
                                                    .as_str()
                                                    .to_string(),
                                                metric_type: MetricType::Histogram
                                                    .as_str()
                                                    .to_string(),
                                                flags: 0,
                                                resource_attributes: resource_attrs_vec.clone(),
                                                metric_attributes: bucket_attrs.clone(),
                                                labels: bucket_labels_json.clone(),
                                            });

                                            ts_rows.push(TimeSeriesInsert {
                                                project_id,
                                                metric_name: bucket_name.clone(),
                                                fingerprint: bucket_fp,
                                                labels: bucket_labels_json,
                                                temporality: Temporality::Delta
                                                    .as_str()
                                                    .to_string(),
                                                metric_type: MetricType::Histogram
                                                    .as_str()
                                                    .to_string(),
                                                unix_milli,
                                                resource_attributes: resource_attrs_vec.clone(),
                                                metric_attributes: bucket_attrs,
                                            });
                                        }

                                        // +Inf bucket
                                        cumulative +=
                                            *data_point.bucket_counts.last().unwrap() as f64;
                                        let mut inf_labels = labels_btree.clone();
                                        inf_labels.insert("le".to_string(), "+Inf".to_string());
                                        let inf_fp = compute_fingerprint(&bucket_name, &inf_labels);
                                        let inf_labels_json = serde_json::to_string(&inf_labels)
                                            .unwrap_or_else(|_| "{}".to_string());
                                        let inf_attrs: Vec<(String, String)> = inf_labels
                                            .iter()
                                            .map(|(k, v)| (k.clone(), v.clone()))
                                            .collect();

                                        sample_rows.push(SampleInsert {
                                            project_id,
                                            metric_name: bucket_name.clone(),
                                            fingerprint: inf_fp,
                                            unix_milli,
                                            value: cumulative,
                                            temporality: Temporality::Delta.as_str().to_string(),
                                            metric_type: MetricType::Histogram.as_str().to_string(),
                                            flags: 0,
                                            resource_attributes: resource_attrs_vec.clone(),
                                            metric_attributes: inf_attrs.clone(),
                                            labels: inf_labels_json.clone(),
                                        });

                                        ts_rows.push(TimeSeriesInsert {
                                            project_id,
                                            metric_name: bucket_name,
                                            fingerprint: inf_fp,
                                            labels: inf_labels_json,
                                            temporality: Temporality::Delta.as_str().to_string(),
                                            metric_type: MetricType::Histogram.as_str().to_string(),
                                            unix_milli,
                                            resource_attributes: resource_attrs_vec.clone(),
                                            metric_attributes: inf_attrs,
                                        });
                                    }

                                    let base_fingerprint =
                                        compute_fingerprint(&metric_name, &labels_btree);
                                    extract_exemplars(
                                        &data_point.exemplars,
                                        project_id,
                                        &metric_name,
                                        base_fingerprint,
                                        data_point.time_unix_nano,
                                        &mut exemplar_rows,
                                    );
                                }
                            }
                            _ => {
                                debug!(
                                    "[METRICS_WORKER] Unsupported metric type for: {}",
                                    metric_name
                                );
                            }
                        }
                    }
                }
            }

            for (attr_type, attr_value) in &filter_values {
                filter_rows.push(FilterValueInsert {
                    project_id: project_id_str.clone(),
                    attribute_type: attr_type.clone(),
                    attribute_value: attr_value.clone(),
                    last_seen: now,
                });
            }
            filter_values.clear();
        }

        (sample_rows, ts_rows, filter_rows, exemplar_rows)
    };

    tracing::Span::current().record("samples_count", sample_rows.len());

    let mut usage_counts: HashMap<String, u64> = HashMap::new();
    for row in &sample_rows {
        *usage_counts.entry(row.project_id.to_string()).or_default() += 1;
    }

    Ok((
        sample_rows,
        ts_rows,
        filter_rows,
        exemplar_rows,
        usage_counts,
    ))
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

// ============================================================================
// OTEL Name Mapping Helpers
// ============================================================================

/// Map Prometheus-style label keys to OTEL attribute names in a BTreeMap.
fn map_label_keys(labels: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    labels
        .iter()
        .map(|(k, v)| {
            let mapped = resolve_label_name(k)
                .map(|s| s.to_string())
                .unwrap_or_else(|| k.clone());
            (mapped, v.clone())
        })
        .collect()
}

/// Resolve a metric name to its storage name and any synthetic labels
/// that must be injected (e.g. merging per-resource OTEL metrics into
/// a single metric with a `resource` label).
fn map_metric(name: &str) -> (String, &'static [(&'static str, &'static str)]) {
    let extra = synthetic_labels_for(name);
    let mapped = resolve_storage_name(name)
        .map(|s| s.to_string())
        .unwrap_or_else(|| name.to_string());
    (mapped, extra)
}

// ============================================================================
// Helper Functions
// ============================================================================

fn extract_exemplars(
    exemplars: &[opentelemetry_proto::tonic::metrics::v1::Exemplar],
    project_id: Uuid,
    metric_name: &str,
    fingerprint: u64,
    fallback_time_nano: u64,
    out: &mut Vec<ExemplarInsert>,
) {
    for ex in exemplars {
        let trace_id = hex::encode(&ex.trace_id);
        let span_id = hex::encode(&ex.span_id);

        if trace_id.chars().all(|c| c == '0') && span_id.chars().all(|c| c == '0') {
            continue;
        }

        let ts = if ex.time_unix_nano > 0 {
            ex.time_unix_nano as i64
        } else {
            fallback_time_nano as i64
        };

        let value = match &ex.value {
            Some(opentelemetry_proto::tonic::metrics::v1::exemplar::Value::AsDouble(d)) => *d,
            Some(opentelemetry_proto::tonic::metrics::v1::exemplar::Value::AsInt(i)) => *i as f64,
            None => 0.0,
        };

        let filtered_attributes: Vec<(String, String)> = ex
            .filtered_attributes
            .iter()
            .filter_map(|kv| {
                let v = kv.value.as_ref().and_then(|v| match &v.value {
                    Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s),
                    ) => Some(s.clone()),
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => {
                        Some(i.to_string())
                    }
                    Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d),
                    ) => Some(d.to_string()),
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(
                        b,
                    )) => Some(b.to_string()),
                    _ => None,
                })?;
                Some((kv.key.clone(), v))
            })
            .collect();

        out.push(ExemplarInsert {
            project_id,
            metric_name: metric_name.to_string(),
            fingerprint,
            exemplar_time_unix_nano: ts,
            trace_id,
            span_id,
            value,
            filtered_attributes,
        });
    }
}

/// Format a histogram bucket boundary the same way Prometheus does:
/// integers as "1", clean decimals as "0.005", no trailing zeros.
fn format_le(bound: f64) -> String {
    if bound == bound.trunc() && bound.abs() < 1e15 {
        format!("{}", bound as i64)
    } else {
        let s = format!("{}", bound);
        s
    }
}

fn extract_number_value(
    value: &Option<opentelemetry_proto::tonic::metrics::v1::number_data_point::Value>,
) -> f64 {
    value
        .as_ref()
        .map(|v| match v {
            opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(d) => *d,
            opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(i) => {
                *i as f64
            }
        })
        .unwrap_or(0.0)
}

fn extract_timestamp(time_unix_nano: u64) -> DateTime<Utc> {
    DateTime::from_timestamp(
        (time_unix_nano / 1_000_000_000) as i64,
        (time_unix_nano % 1_000_000_000) as u32,
    )
    .unwrap_or_else(Utc::now)
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

fn extract_attribute_string(
    attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
    key: &str,
) -> Option<String> {
    for attr in attributes {
        if attr.key == key {
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

fn convert_attributes_to_key_values(
    attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
) -> Vec<(Cow<'static, str>, String)> {
    let mut itoa_buf = itoa::Buffer::new();

    attributes
        .iter()
        .filter_map(|kv| {
            let key: Cow<'static, str> = if let Some(spur) = crate::intern::try_get(&kv.key) {
                Cow::Borrowed(crate::intern::resolve(spur))
            } else {
                Cow::Owned(kv.key.clone())
            };
            let value = kv
                .value
                .as_ref()
                .map(|v| match &v.value {
                    Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s),
                    ) => s.clone(),
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => {
                        itoa_buf.format(*i).to_owned()
                    }
                    Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d),
                    ) => d.to_string(),
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(
                        b,
                    )) => (if *b { "true" } else { "false" }).to_owned(),
                    _ => String::new(),
                })
                .unwrap_or_default();
            Some((key, value))
        })
        .collect()
}
