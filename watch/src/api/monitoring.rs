//! Monitoring endpoints for Kafka lag and ClickHouse ingestion rates
//!
//! Provides metrics about:
//! - Kafka consumer lag (how many messages are behind)
//! - ClickHouse ingestion rates (messages per second)
//! - Kafka topic sizes and partition info
//! - Consumer group status

use axum::{extract::State, response::Json, routing::get, Router};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;

use crate::app_state::WatchState;
use crate::error::{AppError, Result};
use anyhow;

#[derive(Debug, Serialize)]
pub struct KafkaLagMetrics {
    pub consumer_group: String,
    pub topic: String,
    pub partition: i32,
    pub current_offset: i64,
    pub lag: i64,
    pub partition_leader: Option<i32>,
}

#[derive(Debug, Serialize)]
pub struct KafkaTopicInfo {
    pub topic: String,
    pub partitions: Vec<KafkaPartitionInfo>,
    pub total_messages: i64,
}

#[derive(Debug, Serialize)]
pub struct KafkaPartitionInfo {
    pub partition: i32,
    pub leader: Option<i32>,
    pub replicas: Vec<i32>,
    pub isrs: Vec<i32>, // In-Sync Replicas
}

#[derive(Debug, Serialize)]
pub struct ClickHouseIngestionMetrics {
    pub table: String,
    pub ingestion_rate_per_sec: f64,
    pub total_rows: u64,
    pub last_ingestion_time: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct MonitoringStatus {
    pub kafka_consumer_groups: Vec<KafkaConsumerGroupStatus>,
    pub clickhouse_tables: Vec<ClickHouseIngestionMetrics>,
    pub timestamp: String,
}

#[derive(Debug, Serialize)]
#[allow(dead_code)] // Response type for future materialized view error API
pub struct MaterializedViewError {
    pub name: String,
    pub code: u32,
    pub count: u64,
    pub last_error_time: String,
    pub last_error_message: String,
}

#[derive(Debug, Serialize)]
pub struct KafkaConsumerGroupStatus {
    pub group_id: String,
    pub topic: String,
    pub lag_by_partition: Vec<KafkaLagMetrics>,
    pub total_lag: i64,
}

/// Create monitoring router
pub fn create_monitoring_router() -> Router<Arc<WatchState>> {
    Router::new()
        .route("/kafka/lag", get(get_kafka_lag))
        .route("/kafka/topics", get(get_kafka_topics))
        .route("/clickhouse/ingestion", get(get_clickhouse_ingestion))
        .route("/status", get(get_monitoring_status))
}

/// Get Kafka consumer lag for all consumer groups
///
/// Returns lag metrics for:
/// - reiver-error-processor (error processing)
/// - reiver-tail-sampling-worker (tail sampling worker)
///
/// Note: Blocking rdkafka operations are wrapped in spawn_blocking to avoid blocking the async runtime.
async fn get_kafka_lag(
    State(state): State<Arc<WatchState>>,
) -> Result<Json<Vec<KafkaConsumerGroupStatus>>> {
    // Consumer groups we monitor
    let consumer_groups = vec![
        (
            "reiver-exception-processor",
            state.config.kafka_exceptions_topic.as_str(),
        ),
        (
            "reiver-tail-sampling-worker",
            state.config.kafka_spans_topic.as_str(),
        ),
    ];

    let kafka_hosts = state.config.kafka_hosts.clone();

    let mut results = Vec::new();

    for (group_id, topic) in consumer_groups {
        let kafka_hosts_clone = kafka_hosts.clone();
        let group_id_str = group_id.to_string();
        let topic_str = topic.to_string();

        // Execute all blocking operations for this consumer group in a single spawn_blocking
        let group_status = tokio::task::spawn_blocking(
            move || -> std::result::Result<KafkaConsumerGroupStatus, rdkafka::error::KafkaError> {
                // Create consumer for getting committed offsets
                let mut offset_config = ClientConfig::new();
                offset_config.set("bootstrap.servers", &kafka_hosts_clone);
                offset_config.set("group.id", &group_id_str);
                offset_config.set(
                    "client.id",
                    &format!("reiver-monitoring-{}", group_id_str),
                );
                offset_config.set("session.timeout.ms", "30000");
                offset_config.set("enable.auto.commit", "false");
                offset_config.set("enable.partition.eof", "false");
                let offset_consumer: StreamConsumer = offset_config.create()?;

                // Create consumer for getting high water marks (no group.id)
                let mut watermark_config = ClientConfig::new();
                watermark_config.set("bootstrap.servers", &kafka_hosts_clone);
                watermark_config.set("client.id", "reiver-monitoring-watermarks");
                watermark_config.set("enable.partition.eof", "false");
                let watermark_consumer: StreamConsumer = watermark_config.create()?;

                // Subscribe to get partition assignments
                offset_consumer.subscribe(&[&topic_str])?;

                // Wait for assignment
                std::thread::sleep(Duration::from_millis(500));

                // Get assignment
                let assignment = offset_consumer.assignment()?;

                // Get committed offsets
                let committed = offset_consumer
                    .committed(rdkafka::util::Timeout::After(Duration::from_secs(5)))?;

                // Process each partition
                let mut lag_by_partition = Vec::new();
                let mut total_lag: i64 = 0;

                for partition_info in assignment.elements() {
                    let partition_id = partition_info.partition();

                    // Get committed offset for this partition
                    let committed_offset = committed
                        .elements()
                        .iter()
                        .find(|p| p.topic() == &topic_str && p.partition() == partition_id)
                        .and_then(|p| p.offset().to_raw())
                        .unwrap_or(-1);

                    // Get high water mark
                    let (_, high_water_mark) = watermark_consumer.fetch_watermarks(
                        &topic_str,
                        partition_id,
                        Duration::from_secs(10),
                    )?;

                    // Get partition leader from metadata
                    let leader = watermark_consumer
                        .fetch_metadata(Some(&topic_str), Duration::from_secs(5))
                        .ok()
                        .and_then(|metadata| {
                            metadata
                                .topics()
                                .iter()
                                .find(|t| t.name() == &topic_str)
                                .and_then(|t| {
                                    t.partitions().iter().find(|p| p.id() == partition_id)
                                })
                                .map(|p| p.leader())
                        });

                    let lag = if committed_offset >= 0 && high_water_mark >= 0 {
                        (high_water_mark as i64) - committed_offset
                    } else {
                        0
                    };

                    total_lag += lag.max(0);

                    lag_by_partition.push(KafkaLagMetrics {
                        consumer_group: group_id_str.clone(),
                        topic: topic_str.clone(),
                        partition: partition_id,
                        current_offset: committed_offset,
                        lag: lag.max(0),
                        partition_leader: leader,
                    });
                }

                Ok(KafkaConsumerGroupStatus {
                    group_id: group_id_str,
                    topic: topic_str,
                    lag_by_partition,
                    total_lag,
                })
            },
        )
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to spawn blocking task: {}", e)))?
        .map_err(|e| {
            AppError::Internal(anyhow::anyhow!("Failed to get lag for {}: {}", group_id, e))
        })?;

        results.push(group_status);
    }

    Ok(Json(results))
}

/// Get Kafka topic information (partitions, leaders, replicas)
async fn get_kafka_topics(
    State(state): State<Arc<WatchState>>,
) -> Result<Json<Vec<KafkaTopicInfo>>> {
    let kafka_hosts = state.config.kafka_hosts.clone();
    let topics = vec![
        state.config.kafka_exceptions_topic.clone(),
        state.config.kafka_spans_topic.clone(),
    ];

    // Execute all blocking operations in spawn_blocking
    let results = tokio::task::spawn_blocking(
        move || -> std::result::Result<Vec<KafkaTopicInfo>, rdkafka::error::KafkaError> {
            let mut config = ClientConfig::new();
            config.set("bootstrap.servers", &kafka_hosts);
            config.set("client.id", "reiver-monitoring-topics");
            config.set("enable.partition.eof", "false");
            let consumer: StreamConsumer = config.create()?;

            let mut topic_infos = Vec::new();

            for topic in topics {
                let metadata = consumer.fetch_metadata(Some(&topic), Duration::from_secs(10))?;

                let topic_metadata = metadata.topics().iter().find(|t| t.name() == topic);

                if let Some(topic_metadata) = topic_metadata {
                    let mut partitions = Vec::new();
                    let mut total_messages: i64 = 0;

                    for partition in topic_metadata.partitions() {
                        let partition_id = partition.id();

                        // Get high water mark to estimate message count
                        if let Ok((_, high)) =
                            consumer.fetch_watermarks(&topic, partition_id, Duration::from_secs(5))
                        {
                            total_messages += high as i64;
                        }

                        partitions.push(KafkaPartitionInfo {
                            partition: partition_id,
                            leader: Some(partition.leader()),
                            replicas: partition.replicas().iter().map(|&r| r as i32).collect(),
                            isrs: partition.isr().iter().map(|&r| r as i32).collect(),
                        });
                    }

                    topic_infos.push(KafkaTopicInfo {
                        topic: topic.clone(),
                        partitions,
                        total_messages,
                    });
                }
            }

            Ok(topic_infos)
        },
    )
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to spawn blocking task: {}", e)))?
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to get topic info: {}", e)))?;

    Ok(Json(results))
}

/// Get ClickHouse ingestion rates
///
/// Queries ClickHouse system tables to get:
/// - Rows inserted per second for errors and spans tables
/// - Total row counts
/// - Last insertion timestamps
async fn get_clickhouse_ingestion(
    State(state): State<Arc<WatchState>>,
) -> Result<Json<Vec<ClickHouseIngestionMetrics>>> {
    // Query ClickHouse system.parts table to get ingestion stats
    // This gives us partition-level stats that we can aggregate

    let mut metrics = Vec::new();

    // Get stats for errors table
    let errors_query = r#"
        SELECT 
            count() as total_rows,
            max(modification_time) as last_modification
        FROM system.parts
        WHERE database = 'reiver' AND table = 'exceptions_local' AND active = 1
    "#;

    // system.parts.modification_time is DateTime (seconds), not DateTime64 (nanos).
    // Use clickhouse::serde::chrono::datetime to match the wire type.
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct TableStatsRow {
        total_rows: u64,
        #[serde(with = "clickhouse::serde::chrono::datetime")]
        last_modification: chrono::DateTime<chrono::Utc>,
    }

    match state
        .clickhouse
        .as_ref()
        .query(errors_query)
        .fetch_one::<TableStatsRow>()
        .await
    {
        Ok(stats) => {
            let last_time = if stats.last_modification.timestamp() == 0 {
                None
            } else {
                Some(stats.last_modification.to_rfc3339())
            };
            metrics.push(ClickHouseIngestionMetrics {
                table: "errors".to_string(),
                ingestion_rate_per_sec: 0.0, // TODO: Calculate from time-series data
                total_rows: stats.total_rows,
                last_ingestion_time: last_time,
            });
        }
        Err(e) => {
            tracing::warn!("Failed to get ClickHouse errors table stats: {}", e);
        }
    }

    // Get stats for spans table
    let spans_query = r#"
        SELECT 
            count() as total_rows,
            max(modification_time) as last_modification
        FROM system.parts
        WHERE database = 'reiver' AND table = 'spans_local' AND active = 1
    "#;

    match state
        .clickhouse
        .as_ref()
        .query(spans_query)
        .fetch_one::<TableStatsRow>()
        .await
    {
        Ok(stats) => {
            let last_time = if stats.last_modification.timestamp() == 0 {
                None
            } else {
                Some(stats.last_modification.to_rfc3339())
            };
            metrics.push(ClickHouseIngestionMetrics {
                table: "spans".to_string(),
                ingestion_rate_per_sec: 0.0,
                total_rows: stats.total_rows,
                last_ingestion_time: last_time,
            });
        }
        Err(e) => {
            tracing::warn!("Failed to get ClickHouse spans table stats: {}", e);
        }
    }

    Ok(Json(metrics))
}

/// Get overall monitoring status (combined Kafka + ClickHouse metrics)
async fn get_monitoring_status(
    State(state): State<Arc<WatchState>>,
) -> Result<Json<MonitoringStatus>> {
    use chrono::Utc;

    let kafka_groups = get_kafka_lag(State(state.clone())).await?.0;
    let clickhouse_metrics = get_clickhouse_ingestion(State(state)).await?.0;

    Ok(Json(MonitoringStatus {
        kafka_consumer_groups: kafka_groups,
        clickhouse_tables: clickhouse_metrics,
        timestamp: Utc::now().to_rfc3339(),
    }))
}
