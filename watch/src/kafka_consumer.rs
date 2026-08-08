use anyhow::Result;
use chrono::{DateTime, Utc};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::StreamConsumer;
use rdkafka::message::Message;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, instrument};
use uuid::Uuid;

use crate::app_state::RedisPool;
use crate::app_state::StatsUpdateMessage;
use crate::clickhouse_db::ClickHousePool;
use crate::config::Config;
use crate::db::DbPool;
use crate::query_cache::invalidate_project_cache;
use crate::simd_json_utils::parse_json_simd;
use crate::worker::{
    check_alerts, check_regression_alerts, get_stats_from_redis,
    store_recent_exception_in_redis_v2, update_stats_in_redis, ExceptionGroupContext,
};
use reiver_core::events::EventPublisher;

#[derive(clickhouse::Row, serde::Serialize)]
struct FilterValueInsert {
    project_id: String,
    attribute_type: String,
    attribute_value: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    last_seen: chrono::DateTime<Utc>,
}

/// Exception message received from Kafka (matches ExceptionKafkaMessage from kafka.rs)
#[derive(Debug, Deserialize)]
#[allow(dead_code)] // project_id included in message for routing but looked up separately
pub struct ExceptionKafkaMessage {
    pub id: String,
    pub project_id: String,
    pub fingerprint: String,
    pub level: String,
    pub message: String,
    pub exception_type: Option<String>,
    pub exception_value: Option<String>,
    pub stacktrace: String,
    pub context: String,
    pub tags: String,
    pub user_data: String,
    pub service_name: Option<String>,
    pub timestamp: String, // ISO 8601 string
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    // Deploy version tracking (for GitHub integration)
    pub service_version: Option<String>, // Git commit SHA or release tag
    pub environment: Option<String>,     // Deployment environment
    pub repository_url: Option<String>,  // VCS repository URL
}

/// Simple Kafka context for consumer (no-op for now, can add metrics later)
pub struct KafkaConsumerContext;

impl rdkafka::ClientContext for KafkaConsumerContext {
    fn stats(&self, _stats: rdkafka::Statistics) {
        // Can add metrics/stats reporting here later
    }
}

impl rdkafka::consumer::ConsumerContext for KafkaConsumerContext {}

/// Start Kafka consumer for exceptions
/// Consumes from reiver.exceptions topic and processes:
/// - Maintains exception_groups table
/// - Updates Redis stats
/// - Broadcasts SSE updates
/// Note: Writing to ClickHouse exceptions table is handled by ClickHouse Kafka Engine
pub async fn start_kafka_error_consumer(
    kafka_hosts: &str,
    exceptions_topic: &str,
    client_id: Option<&str>,
    db_pool: Arc<DbPool>,
    clickhouse_pool: Arc<ClickHousePool>,
    redis_pool: Arc<RedisPool>,
    config: Arc<Config>,
    stats_broadcast: broadcast::Sender<StatsUpdateMessage>,
    event_publisher: Arc<EventPublisher>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<JoinHandle<()>> {
    info!(
        "Creating Kafka consumer for exceptions topic: {}",
        exceptions_topic
    );

    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", kafka_hosts)
        .set("group.id", "reiver-error-processor")
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

    consumer.subscribe(&[exceptions_topic])?;
    info!("Subscribed to Kafka topic: {}", exceptions_topic);

    let handle = tokio::spawn(async move {
        info!("[KAFKA_CONSUMER] Started, consuming exceptions from Kafka");

        let mut last_stats_update: HashMap<Uuid, Instant> = HashMap::new();
        let stats_update_throttle = std::time::Duration::from_millis(200);

        let mut filter_inserter = clickhouse_pool
            .as_ref()
            .inserter::<FilterValueInsert>("otlp_attributes")
            .with_period(Some(std::time::Duration::from_secs(30)))
            .with_max_rows(50_000);

        let mut message_stream = consumer.stream();
        let mut message_count = 0u64;
        let mut error_count = 0u64;
        let mut window_msg_count = 0u64;
        let mut window_start = Instant::now();
        let mut flush_interval = tokio::time::interval(std::time::Duration::from_secs(30));
        flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut throughput_interval = tokio::time::interval(std::time::Duration::from_secs(10));
        throughput_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = flush_interval.tick() => {
                    if let Err(e) = filter_inserter.commit().await {
                        error!("[KAFKA_CONSUMER] Failed to commit filter inserter: {}", e);
                    }
                }
                _ = throughput_interval.tick() => {
                    let elapsed = window_start.elapsed();
                    if window_msg_count > 0 || message_count > 0 {
                        let msg_per_sec = if elapsed.as_secs_f64() > 0.0 {
                            window_msg_count as f64 / elapsed.as_secs_f64()
                        } else { 0.0 };
                        info!(
                            "[KAFKA_CONSUMER] total_exceptions={} window={} msg/s={:.1} errors={}",
                            message_count, window_msg_count, msg_per_sec, error_count,
                        );
                    }
                    window_msg_count = 0;
                    window_start = Instant::now();
                }
                message_opt = message_stream.next() => {
                    let Some(message) = message_opt else { break; };
                    match message {
                        Ok(m) => {
                    // Extract project_id from key (we use project_id as key)
                    let project_id = if let Some(key) = m.key() {
                        match String::from_utf8(key.to_vec()) {
                            Ok(key_str) => {
                                match Uuid::parse_str(&key_str) {
                                    Ok(uuid) => uuid,
                                    Err(e) => {
                                        error!("Failed to parse project_id from Kafka message key: {}", e);
                                        continue;
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Failed to parse Kafka message key as UTF-8: {}", e);
                                continue;
                            }
                        }
                    } else {
                        error!("Kafka message missing key (project_id)");
                        continue;
                    };

                    let kafka_msg: ExceptionKafkaMessage = if let Some(payload_bytes) = m.payload() {
                        match parse_json_simd(payload_bytes) {
                            Ok(p) => p,
                            Err(e) => {
                                error!("Failed to parse ExceptionKafkaMessage from Kafka message: {}", e);
                                continue;
                            }
                        }
                    } else {
                        error!("Kafka message missing payload");
                        continue;
                    };

                    // Use the exception_id from the Kafka message (generated by API)
                    let exception_id = Uuid::parse_str(&kafka_msg.id).unwrap_or_else(|_| Uuid::new_v4());

                    debug!("[KAFKA_CONSUMER] Processing exception exception_id={}, project_id={}, fingerprint={}...",
                        exception_id, project_id, &kafka_msg.fingerprint[..std::cmp::min(16, kafka_msg.fingerprint.len())]);

                    match process_exception_from_kafka(
                        &db_pool,
                        &clickhouse_pool,
                        &redis_pool,
                        &mut filter_inserter,
                        exception_id,
                        project_id,
                        &kafka_msg,
                        &config,
                        &event_publisher,
                    ).await {
                        Ok(_is_new_group) => {
                            message_count += 1;
                            window_msg_count += 1;
                            if message_count % 100 == 0 {
                                let elapsed = window_start.elapsed();
                                let rate = if elapsed.as_secs_f64() > 0.0 {
                                    window_msg_count as f64 / elapsed.as_secs_f64()
                                } else { 0.0 };
                                info!(
                                    "[KAFKA_CONSUMER] Processed {} exceptions ({:.1} msg/s), {} errors",
                                    message_count, rate, error_count,
                                );
                            }
                            let should_broadcast = last_stats_update
                                .get(&project_id)
                                .map(|&last| last.elapsed() >= stats_update_throttle)
                                .unwrap_or(true);

                            if should_broadcast {
                                // Read stats from Redis (fast, no ClickHouse query)
                                match get_stats_from_redis(&redis_pool, project_id).await {
                                    Ok(Some(stats)) => {
                                        let _ = stats_broadcast.send(StatsUpdateMessage {
                                            project_id,
                                            stats: Some(stats),
                                        });
                                        last_stats_update.insert(project_id, Instant::now());
                                    }
                                    Ok(None) => {
                                        // No stats in Redis yet, just notify
                                        let _ = stats_broadcast.send(StatsUpdateMessage {
                                            project_id,
                                            stats: None,
                                        });
                                    }
                                    Err(e) => {
                                        error!("Failed to get stats from Redis for project {}: {}", project_id, e);
                                        let _ = stats_broadcast.send(StatsUpdateMessage {
                                            project_id,
                                            stats: None,
                                        });
                                    }
                                }
                            } else {
                                // Throttled: just send notification
                                let _ = stats_broadcast.send(StatsUpdateMessage {
                                    project_id,
                                    stats: None,
                                });
                            }
                        }
                        Err(e) => {
                            error_count += 1;
                            message_count += 1;
                            window_msg_count += 1;
                            error!("[KAFKA_CONSUMER] Error processing exception: {}", e);
                        }
                    }
                        }
                        Err(e) => {
                            error_count += 1;
                            error!("[KAFKA_CONSUMER] Error receiving message from Kafka: {:?}", e);
                        }
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Kafka error consumer received shutdown signal, stopping gracefully");
                        break;
                    }
                }
            }
        }

        if let Err(e) = filter_inserter.commit().await {
            error!("[KAFKA_CONSUMER] Failed to final commit filter inserter: {}", e);
        }
        if let Err(e) = filter_inserter.end().await {
            error!("[KAFKA_CONSUMER] Failed to end filter inserter: {}", e);
        }
        info!("Kafka error consumer stopped");
    });

    Ok(handle)
}

/// Process exception from Kafka message
/// ClickHouse Kafka Engine writes to exceptions table; we aggregate at query time
#[instrument(name = "process_exception", skip_all, fields(exception_id = %exception_id, project_id = %project_id))]
async fn process_exception_from_kafka(
    db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
    redis_pool: &RedisPool,
    filter_inserter: &mut clickhouse::inserter::Inserter<FilterValueInsert>,
    exception_id: Uuid,
    project_id: Uuid,
    kafka_msg: &ExceptionKafkaMessage,
    config: &Config,
    event_publisher: &EventPublisher,
) -> Result<bool> {
    // Parse timestamp from ISO 8601 string
    let timestamp = DateTime::parse_from_rfc3339(&kafka_msg.timestamp)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now());

    debug!(
        "[KAFKA_CONSUMER] Processing exception exception_id={}, project_id={}",
        exception_id, project_id
    );

    // Use fingerprint from Kafka message (generated by API)
    let fingerprint = kafka_msg.fingerprint.clone();

    let project_key = format!("stats:project:{}", project_id);
    let group_hash_key = format!("{}:group:{}", project_key, fingerprint);

    // Check Redis cache first to avoid expensive ClickHouse aggregation query per message.
    // The group data is maintained in Redis by update_stats_in_redis().
    let cached_group: Option<String> = {
        let mut conn = redis_pool
            .get()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Redis connection: {}", e))?;
        bb8_redis::redis::AsyncCommands::get::<_, Option<String>>(&mut *conn, &group_hash_key)
            .await
            .unwrap_or(None)
    };

    struct GroupInfo {
        status: String,
        message: String,
        level: String,
        exception_type: Option<String>,
        exception_value: Option<String>,
        first_seen: chrono::DateTime<Utc>,
        last_seen: chrono::DateTime<Utc>,
    }

    let existing_group: Option<GroupInfo> = if let Some(json_str) = cached_group {
        // Fast path: found in Redis cache
        if let Ok(group_json) = serde_json::from_str::<serde_json::Value>(&json_str) {
            Some(GroupInfo {
                status: group_json["status"]
                    .as_str()
                    .unwrap_or("unresolved")
                    .to_string(),
                message: group_json["message"].as_str().unwrap_or("").to_string(),
                level: group_json["level"].as_str().unwrap_or("error").to_string(),
                exception_type: group_json
                    .get("exception_type")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                exception_value: group_json
                    .get("exception_value")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                first_seen: chrono::TimeZone::timestamp_millis_opt(
                    &Utc,
                    group_json["first_seen"].as_i64().unwrap_or(0),
                )
                .single()
                .unwrap_or(timestamp),
                last_seen: chrono::TimeZone::timestamp_millis_opt(
                    &Utc,
                    group_json["last_seen"].as_i64().unwrap_or(0),
                )
                .single()
                .unwrap_or(timestamp),
            })
        } else {
            None
        }
    } else {
        // Slow path: cache miss, fall back to ClickHouse
        #[derive(clickhouse::Row, serde::Deserialize)]
        #[allow(dead_code)]
        struct ExceptionGroupStatus {
            status: String,
            count: u64,
            #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
            first_seen: chrono::DateTime<Utc>,
            #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
            last_seen: chrono::DateTime<Utc>,
            message: String,
            level: String,
            exception_type: Option<String>,
            exception_value: Option<String>,
        }

        let ch_group: Option<ExceptionGroupStatus> = clickhouse_pool.as_ref()
            .query("SELECT coalesce(argMax(status, timestamp), 'unresolved') as status, count() as count, min(timestamp) as first_seen, max(timestamp) as last_seen, argMax(message, timestamp) as message, argMax(level, timestamp) as level, argMax(exception_type, timestamp) as exception_type, argMax(exception_value, timestamp) as exception_value FROM reiver.exceptions WHERE project_id = ? AND fingerprint = ? GROUP BY project_id, fingerprint")
            .bind(project_id.to_string())
            .bind(&fingerprint)
            .fetch_optional()
            .await?;

        ch_group.map(|row| GroupInfo {
            status: row.status,
            message: row.message,
            level: row.level,
            exception_type: row.exception_type,
            exception_value: row.exception_value,
            first_seen: row.first_seen,
            last_seen: row.last_seen,
        })
    };

    let is_new_group = existing_group.is_none();
    let is_regression = existing_group
        .as_ref()
        .map(|g| g.status == "resolved")
        .unwrap_or(false);

    if let Some(ref group) = existing_group {
        debug!(
            "[KAFKA_CONSUMER] Found existing group: fingerprint={}, status={}",
            fingerprint, group.status
        );
    } else {
        debug!("[KAFKA_CONSUMER] New group: fingerprint={}", fingerprint);
    }

    // Extract context fields from JSON (if available)
    let context_json: serde_json::Value =
        serde_json::from_str(&kafka_msg.context).unwrap_or_default();
    let environment = context_json
        .get("environment")
        .and_then(|v| v.as_str())
        .map(String::from)
        .or_else(|| kafka_msg.environment.clone());

    // Get group info for Redis
    let (
        group_id,
        group_message,
        group_level,
        group_exception_type,
        group_exception_value,
        group_first_seen,
        group_last_seen,
    ) = if let Some(group) = existing_group {
        let group_id = format!("{}:{}", project_id, fingerprint);
        (
            group_id,
            group.message,
            group.level,
            group.exception_type,
            group.exception_value,
            group.first_seen,
            group.last_seen,
        )
    } else {
        let group_id = format!("{}:{}", project_id, fingerprint);
        let msg = kafka_msg.message.clone();
        let lvl = kafka_msg.level.clone();
        let exc_type = kafka_msg.exception_type.clone();
        let exc_val = kafka_msg.exception_value.clone();
        (group_id, msg, lvl, exc_type, exc_val, timestamp, timestamp)
    };

    debug!("[KAFKA_CONSUMER] Successfully processed exception exception_id={}, project_id={}, fingerprint={}, is_new_group={}", 
        exception_id, project_id, fingerprint, is_new_group);

    store_recent_exception_in_redis_v2(
        redis_pool,
        project_id,
        &fingerprint,
        &exception_id,
        kafka_msg,
        &timestamp,
    )
    .await?;

    let str_field = |key: &str| {
        context_json
            .get(key)
            .and_then(|v| v.as_str())
            .map(String::from)
    };

    let group_context = ExceptionGroupContext {
        environment: environment.clone(),
        version: kafka_msg
            .service_version
            .clone()
            .or_else(|| str_field("version")),
        deployment_id: str_field("deployment_id"),
        region: str_field("region"),
        host_name: str_field("host_name").or_else(|| str_field("hostname")),
        runtime: str_field("runtime"),
        pod_name: str_field("pod_name").or_else(|| str_field("k8s.pod.name")),
        cluster_name: str_field("cluster_name").or_else(|| str_field("k8s.cluster.name")),
        container_id: str_field("container_id").or_else(|| str_field("container.id")),
        http_method: str_field("http_method").or_else(|| str_field("http.method")),
        http_url: str_field("http_url").or_else(|| str_field("http.url")),
        user_id: str_field("user_id"),
    };

    // Update Redis stats incrementally
    update_stats_in_redis(
        redis_pool,
        project_id,
        is_new_group,
        &fingerprint,
        &group_id,
        &group_message,
        &group_level,
        &group_exception_type,
        &group_exception_value,
        &kafka_msg.service_name,
        group_first_seen,
        group_last_seen,
        Some(&group_context),
    )
    .await?;

    // Insert filter values for fast lookups (batched via persistent inserter)
    // ReplacingMergeTree will deduplicate by (project_id, attribute_type, attribute_value)
    let proj_id = project_id.to_string();
    for (attr_type, value) in [("environment", &environment), ("service_name", &kafka_msg.service_name)] {
        if let Some(v) = value {
            if !v.is_empty() {
                filter_inserter.write(&FilterValueInsert {
                    project_id: proj_id.clone(),
                    attribute_type: attr_type.to_string(),
                    attribute_value: v.clone(),
                    last_seen: timestamp,
                }).await?;
            }
        }
    }

    // Create exception-to-trace correlation entry if trace_id is present
    if let Some(trace_id) = &kafka_msg.trace_id {
        if !trace_id.is_empty() {
            let span_id_opt: Option<&String> = kafka_msg.span_id.as_ref().filter(|s| !s.is_empty());
            if let Err(e) = sqlx::query(
                "INSERT INTO error_traces (error_id, trace_id, project_id, span_id) 
                 VALUES ($1, $2, $3, $4) 
                 ON CONFLICT (project_id, error_id, trace_id) DO NOTHING",
            )
            .bind(exception_id.to_string())
            .bind(trace_id)
            .bind(project_id)
            .bind(span_id_opt)
            .execute(db_pool)
            .await
            {
                error!(
                    "[KAFKA_CONSUMER] Failed to create exception-trace correlation: {}",
                    e
                );
            } else {
                debug!("[KAFKA_CONSUMER] Created exception-trace correlation: exception_id={}, trace_id={}", exception_id, trace_id);
            }
        }
    }

    // Check for alerts (new error group or regression)
    if is_new_group {
        info!(
            "[KAFKA_CONSUMER] New error group detected, checking alerts for group_id={}",
            group_id
        );
        check_alerts(
            db_pool,
            clickhouse_pool,
            project_id,
            &fingerprint,
            &group_id,
            config,
            event_publisher,
        )
        .await?;
    } else if is_regression {
        // A previously resolved exception has regressed (new error came in)
        info!("[KAFKA_CONSUMER] Exception regression detected! group_id={} was resolved but received new error", group_id);
        check_regression_alerts(
            db_pool,
            clickhouse_pool,
            project_id,
            &fingerprint,
            &group_id,
            config,
            event_publisher,
        )
        .await?;
    }

    // Invalidate query cache for this project so dashboards show fresh data
    if let Err(e) = invalidate_project_cache(redis_pool, project_id).await {
        error!("[KAFKA_CONSUMER] Failed to invalidate project cache: {}", e);
        // Don't fail the entire operation for cache invalidation failure
    }

    Ok(is_new_group)
}
