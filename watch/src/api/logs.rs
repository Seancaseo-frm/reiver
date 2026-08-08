//! Log ingestion API endpoints
//!
//! This module provides endpoints for ingesting logs from various sources, including
//! AWS CloudWatch Logs (via Kinesis Firehose), Azure Monitor Logs, Google Cloud Logging, direct HTTP ingestion, etc.

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Json,
    routing::post,
    Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info};
use uuid::Uuid;

use crate::app_state::WatchState;
use crate::error::{AppError, Result};
use crate::kafka::{KafkaProducer, UnstructuredLogKafkaMessage};

#[derive(Debug, Serialize)]
#[allow(dead_code)] // Response type for future log ingestion response
struct LogResponse {
    id: String,
    message: String,
}

/// CloudWatch Logs record format (from Kinesis Firehose)
/// Kinesis Firehose sends records with this structure when delivering CloudWatch Logs
#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
#[allow(dead_code)] // Fields used by serde deserialization
struct KinesisFirehoseRecord {
    recordId: String,
    #[serde(with = "base64_bytes")]
    data: Vec<u8>,
    approximateArrivalTimestamp: Option<u64>,
}

/// CloudWatch Logs event format (decoded from Kinesis Firehose record data)
#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
#[allow(dead_code)] // Fields used by serde deserialization
struct CloudWatchLogsEvent {
    messageType: String,
    owner: String,
    logGroup: String,
    logStream: String,
    subscriptionFilters: Vec<String>,
    logEvents: Vec<CloudWatchLogEvent>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)] // Fields used by serde deserialization
struct CloudWatchLogEvent {
    id: String,
    #[serde(rename = "timestamp")]
    timestamp_millis: i64,
    message: String,
}

impl CloudWatchLogEvent {
    fn timestamp(&self) -> DateTime<Utc> {
        DateTime::from_timestamp(
            self.timestamp_millis / 1000,
            (self.timestamp_millis % 1000 * 1_000_000) as u32,
        )
        .unwrap_or_else(Utc::now)
    }
}

/// Kinesis Firehose request format
#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
struct KinesisFirehoseRequest {
    requestId: String,
    timestamp: u64,
    records: Vec<KinesisFirehoseRecord>,
}

/// Kinesis Firehose response format
#[derive(Debug, Serialize)]
#[allow(non_snake_case)]
struct KinesisFirehoseResponse {
    requestId: String,
    timestamp: u64,
    records: Vec<KinesisFirehoseResponseRecord>,
}

#[derive(Debug, Serialize)]
#[allow(non_snake_case)]
struct KinesisFirehoseResponseRecord {
    recordId: String,
    result: String, // "Ok", "ProcessingFailed", "Dropped"
    #[serde(skip_serializing_if = "Option::is_none")]
    errorMessage: Option<String>,
}

pub fn create_logs_router() -> Router<Arc<WatchState>> {
    Router::new()
        .route("/ingest", post(ingest_logs))
        .route("/cloudwatch/kinesis", post(ingest_cloudwatch_logs_kinesis))
        .route("/azure", post(ingest_azure_monitor_logs))
        .route("/gcp", post(ingest_gcp_logs))
        .route("/google", post(ingest_gcp_logs))
}

/// Direct log ingestion endpoint
/// POST /api/logs/ingest
async fn ingest_logs(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<serde_json::Value>,
) -> Result<StatusCode> {
    let project_id = crate::api::extract_project_id(&headers)?;

    info!("Received log from project_id: {}", project_id);

    // Extract log fields from payload
    let log_message = payload
        .get("message")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let log_level = payload
        .get("level")
        .and_then(|v| v.as_str())
        .unwrap_or("info")
        .to_string();

    let log_timestamp = payload
        .get("timestamp")
        .and_then(|v| v.as_str())
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(Utc::now);

    let service_name = payload
        .get("service")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();

    let source = payload
        .get("source")
        .and_then(|v| v.as_str())
        .unwrap_or("direct")
        .to_string();

    let trace_id = payload
        .get("trace_id")
        .or_else(|| payload.get("traceId"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let span_id = payload
        .get("span_id")
        .or_else(|| payload.get("spanId"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // Send log to Kafka
    send_log_to_kafka(
        &state.redis,
        &*state.db,
        &state.kafka,
        project_id,
        &log_message,
        &log_level,
        &service_name,
        &source,
        log_timestamp,
        trace_id.as_deref(),
        span_id.as_deref(),
    )
    .await?;

    Ok(StatusCode::OK)
}

/// CloudWatch Logs ingestion endpoint (via Kinesis Firehose)
/// POST /api/logs/cloudwatch/kinesis
///
/// This endpoint receives logs from AWS CloudWatch Logs via Kinesis Firehose.
/// Kinesis Firehose sends logs in a specific format with base64-encoded, gzipped data.
async fn ingest_cloudwatch_logs_kinesis(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<KinesisFirehoseRequest>,
) -> Result<Json<KinesisFirehoseResponse>> {
    info!(
        "Received CloudWatch Logs via Kinesis Firehose: {} records",
        payload.records.len()
    );

    let project_id = crate::api::extract_project_id(&headers)?;

    let mut response_records = Vec::new();

    for record in payload.records {
        match process_cloudwatch_log_record(&state, project_id, &record).await {
            Ok(_) => {
                response_records.push(KinesisFirehoseResponseRecord {
                    recordId: record.recordId,
                    result: "Ok".to_string(),
                    errorMessage: None,
                });
            }
            Err(e) => {
                error!(
                    "Failed to process CloudWatch Log record {}: {}",
                    record.recordId, e
                );
                response_records.push(KinesisFirehoseResponseRecord {
                    recordId: record.recordId,
                    result: "ProcessingFailed".to_string(),
                    errorMessage: Some(format!("{}", e)),
                });
            }
        }
    }

    Ok(Json(KinesisFirehoseResponse {
        requestId: payload.requestId,
        timestamp: payload.timestamp,
        records: response_records,
    }))
}

/// Process a single CloudWatch Logs record from Kinesis Firehose
async fn process_cloudwatch_log_record(
    state: &WatchState,
    project_id: Uuid,
    record: &KinesisFirehoseRecord,
) -> Result<()> {
    // Kinesis Firehose sends CloudWatch Logs data as gzipped, base64-encoded JSON
    // The data field is already base64-decoded, but may be gzipped

    // Try to decompress (gzip)
    let decompressed = if record.data.starts_with(&[0x1f, 0x8b]) {
        // Gzip magic number
        use flate2::read::GzDecoder;
        use std::io::Read;
        let mut decoder = GzDecoder::new(record.data.as_slice());
        let mut decompressed_data = Vec::new();
        decoder
            .read_to_end(&mut decompressed_data)
            .map_err(|e| anyhow::anyhow!("Failed to decompress gzip data: {}", e))?;
        decompressed_data
    } else {
        record.data.clone()
    };

    // Parse as CloudWatch Logs event
    let cw_event: CloudWatchLogsEvent = serde_json::from_slice(&decompressed)
        .map_err(|e| anyhow::anyhow!("Failed to parse CloudWatch Logs event: {}", e))?;

    info!(
        "Processing CloudWatch Logs event: logGroup={}, logStream={}, events={}",
        cw_event.logGroup,
        cw_event.logStream,
        cw_event.logEvents.len()
    );

    // Extract service name from log group (format: /aws/service/name or /aws/ecs/cluster/service)
    let service_name = extract_service_from_log_group(&cw_event.logGroup);

    // Process each log event
    for log_event in cw_event.logEvents {
        // Send log to Kafka
        send_log_to_kafka(
            &state.redis,
            &*state.db,
            &state.kafka,
            project_id,
            &log_event.message,
            "info", // CloudWatch Logs doesn't provide level, default to info
            &service_name,
            "aws_cloudwatch",
            log_event.timestamp(),
            None, // CloudWatch logs don't include trace_id by default
            None, // CloudWatch logs don't include span_id by default
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to send log to Kafka: {}", e))?;
    }

    Ok(())
}

/// Azure Monitor Logs ingestion endpoint
/// POST /api/logs/azure
///
/// This endpoint receives logs from Azure Monitor Logs.
/// Azure Monitor can send logs via Data Collection Rules (DCR) or custom ingestion.
/// The format is typically an array of log entries with TimeGenerated and other fields.
async fn ingest_azure_monitor_logs(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<serde_json::Value>,
) -> Result<StatusCode> {
    info!("Received Azure Monitor Logs ingestion request");

    let project_id = crate::api::extract_project_id(&headers)?;

    // Azure Monitor Logs format: array of log entries
    // Each entry has TimeGenerated (ISO 8601), and various fields like Message, Level, etc.
    let log_entries = payload
        .as_array()
        .ok_or_else(|| AppError::Validation("Expected array of log entries".to_string()))?;

    info!(
        "Processing {} Azure Monitor log entries for project_id: {}",
        log_entries.len(),
        project_id
    );

    for entry in log_entries {
        // Extract log fields from Azure Monitor log entry
        let log_message = entry
            .get("Message")
            .or_else(|| entry.get("message"))
            .or_else(|| entry.get("msg"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        // If message is empty, try to serialize the entire entry as JSON
        let final_message = if log_message.is_empty() {
            serde_json::to_string(entry).unwrap_or_else(|_| "{}".to_string())
        } else {
            log_message
        };

        let log_level = entry
            .get("Level")
            .or_else(|| entry.get("level"))
            .or_else(|| entry.get("Severity"))
            .or_else(|| entry.get("severity"))
            .and_then(|v| v.as_str())
            .unwrap_or("info")
            .to_string()
            .to_lowercase();

        // Azure Monitor uses TimeGenerated field
        let log_timestamp = entry
            .get("TimeGenerated")
            .or_else(|| entry.get("timeGenerated"))
            .or_else(|| entry.get("timestamp"))
            .and_then(|v| {
                // Try to parse as ISO 8601 string first
                if let Some(s) = v.as_str() {
                    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                        return Some(dt.with_timezone(&Utc));
                    }
                }
                // Try Unix timestamp (milliseconds or seconds)
                if let Some(ts) = v.as_i64() {
                    let secs = if ts > 1_000_000_000_000 {
                        // Milliseconds
                        ts / 1000
                    } else {
                        // Seconds
                        ts
                    };
                    return DateTime::from_timestamp(secs, 0).map(|dt| dt.with_timezone(&Utc));
                }
                None
            })
            .unwrap_or_else(Utc::now);

        // Extract service/resource information
        let service_name = entry
            .get("ResourceId")
            .or_else(|| entry.get("resourceId"))
            .or_else(|| entry.get("Computer"))
            .or_else(|| entry.get("computer"))
            .or_else(|| entry.get("ServiceName"))
            .or_else(|| entry.get("serviceName"))
            .and_then(|v| v.as_str())
            .map(|s| extract_service_from_azure_resource_id(s))
            .unwrap_or_else(|| "unknown".to_string());

        // Send log to Kafka
        if let Err(e) = send_log_to_kafka(
            &state.redis,
            &*state.db,
            &state.kafka,
            project_id,
            &final_message,
            &log_level,
            &service_name,
            "azure_monitor",
            log_timestamp,
            None, // Azure Monitor logs don't include trace_id by default
            None, // Azure Monitor logs don't include span_id by default
        )
        .await
        {
            error!("Failed to send Azure Monitor log to Kafka: {}", e);
            // Continue processing other entries even if one fails
        }
    }

    Ok(StatusCode::OK)
}

/// Google Cloud Logging ingestion endpoint
/// POST /api/logs/gcp or /api/logs/google
///
/// This endpoint receives logs from Google Cloud Logging.
/// Google Cloud Logging can send logs via HTTP endpoints using the Cloud Logging API format.
/// The format is typically an object with "entries" array, where each entry has:
/// - timestamp (RFC3339)
/// - severity (DEBUG, INFO, WARNING, ERROR, CRITICAL)
/// - textPayload or jsonPayload
/// - resource (resource type and labels)
/// - logName
async fn ingest_gcp_logs(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<serde_json::Value>,
) -> Result<StatusCode> {
    info!("Received Google Cloud Logging ingestion request");

    let project_id = crate::api::extract_project_id(&headers)?;

    // Google Cloud Logging format: object with "entries" array
    // Each entry has timestamp, severity, textPayload/jsonPayload, resource, logName
    let log_entries = payload
        .get("entries")
        .and_then(|v| v.as_array())
        .ok_or_else(|| AppError::Validation("Expected object with 'entries' array".to_string()))?;

    info!(
        "Processing {} Google Cloud Logging entries for project_id: {}",
        log_entries.len(),
        project_id
    );

    for entry in log_entries {
        // Extract log message from textPayload or jsonPayload
        let log_message = entry
            .get("textPayload")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| {
                entry
                    .get("jsonPayload")
                    .and_then(|v| serde_json::to_string(v).ok())
            })
            .unwrap_or_else(|| {
                // Fallback: serialize entire entry as JSON
                serde_json::to_string(entry).unwrap_or_else(|_| "{}".to_string())
            });

        // Extract severity/level
        let log_level = entry
            .get("severity")
            .and_then(|v| v.as_str())
            .unwrap_or("INFO")
            .to_string()
            .to_lowercase();

        // Extract timestamp (RFC3339 format)
        let log_timestamp = entry
            .get("timestamp")
            .and_then(|v| {
                if let Some(s) = v.as_str() {
                    DateTime::parse_from_rfc3339(s)
                        .ok()
                        .map(|dt| dt.with_timezone(&Utc))
                } else {
                    None
                }
            })
            .unwrap_or_else(|| {
                // Fallback: try receiveTimestamp
                entry
                    .get("receiveTimestamp")
                    .and_then(|v| v.as_str())
                    .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(Utc::now)
            });

        // Extract service/resource information
        let service_name = entry
            .get("resource")
            .and_then(|r| {
                // Extract resource type
                r.get("type")
                    .and_then(|v| v.as_str())
                    .map(|t| t.to_string())
                    .or_else(|| {
                        // Try labels for more specific resource
                        r.get("labels").and_then(|l| {
                            l.get("instance_name")
                                .or_else(|| l.get("container_name"))
                                .or_else(|| l.get("function_name"))
                                .or_else(|| l.get("service_name"))
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string())
                        })
                    })
            })
            .or_else(|| {
                // Extract from logName (format: projects/{project}/logs/{log_name})
                entry
                    .get("logName")
                    .and_then(|v| v.as_str())
                    .and_then(|log_name| log_name.split('/').last().map(|s| s.to_string()))
            })
            .unwrap_or_else(|| "unknown".to_string());

        // Send log to Kafka
        if let Err(e) = send_log_to_kafka(
            &state.redis,
            &*state.db,
            &state.kafka,
            project_id,
            &log_message,
            &log_level,
            &service_name,
            "gcp_cloud_logging",
            log_timestamp,
            None, // GCP Cloud Logging logs don't include trace_id by default
            None, // GCP Cloud Logging logs don't include span_id by default
        )
        .await
        {
            error!("Failed to send Google Cloud Logging entry to Kafka: {}", e);
            // Continue processing other entries even if one fails
        }
    }

    Ok(StatusCode::OK)
}

/// Extract service name from Azure Monitor Logs log group name
fn extract_service_from_log_group(log_group: &str) -> String {
    // Examples:
    // /aws/lambda/my-function -> my-function
    // /aws/ecs/my-cluster/my-service -> my-service
    // /aws/eks/my-cluster -> my-cluster
    // /aws/apigateway/my-api -> my-api

    let parts: Vec<&str> = log_group.split('/').collect();
    if parts.len() >= 2 {
        parts.last().unwrap_or(&"unknown").to_string()
    } else {
        log_group.to_string()
    }
}

/// Extract service name from Azure Resource ID
fn extract_service_from_azure_resource_id(resource_id: &str) -> String {
    // Examples:
    // /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Web/sites/{name} -> {name}
    // /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Compute/virtualMachines/{name} -> {name}

    let parts: Vec<&str> = resource_id.split('/').collect();
    if let Some(last_part) = parts.last() {
        if !last_part.is_empty() {
            return last_part.to_string();
        }
    }

    // Fallback: extract from provider type
    for (i, part) in parts.iter().enumerate() {
        if part == &"providers" && i + 1 < parts.len() {
            // Get the provider name (Microsoft.xxx/type)
            if i + 2 < parts.len() {
                let resource_type = parts[i + 2];
                // Extract type name (e.g., "virtualMachines" -> "virtualMachines")
                return resource_type.to_string();
            }
        }
    }

    "unknown".to_string()
}

/// Send unstructured log to Kafka for processing
pub(crate) async fn send_log_to_kafka(
    redis: &crate::app_state::RedisPool,
    db: &crate::db::DbPool,
    kafka: &KafkaProducer,
    project_id: Uuid,
    message: &str,
    level: &str,
    service_name: &str,
    source: &str,
    timestamp: DateTime<Utc>,
    trace_id: Option<&str>,
    span_id: Option<&str>,
) -> Result<()> {
    let pii_enabled = crate::pii::get_pii_masking_enabled_cached(redis, db, project_id).await;
    // mask_pii returns Cow<str> - use into_owned() to convert to String
    let message = if pii_enabled {
        crate::pii::mask_pii(message).into_owned()
    } else {
        message.to_string()
    };

    let log_msg = UnstructuredLogKafkaMessage {
        id: Uuid::new_v4().to_string(),
        project_id: project_id.to_string(),
        message,
        level: level.to_string(),
        service_name: service_name.to_string(),
        source: source.to_string(),
        timestamp: timestamp.to_rfc3339(),
        trace_id: trace_id.map(|s| s.to_string()),
        span_id: span_id.map(|s| s.to_string()),
    };

    kafka.send_unstructured_log(&log_msg).await.map_err(|e| {
        error!("Failed to send log to Kafka: {}", e);
        AppError::Internal(anyhow::anyhow!("Kafka send failed: {}", e))
    })?;

    Ok(())
}

// Helper module for base64 deserialization
mod base64_bytes {
    use base64::Engine;
    use serde::{Deserialize, Deserializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        base64::engine::general_purpose::STANDARD
            .decode(s)
            .map_err(serde::de::Error::custom)
    }
}
