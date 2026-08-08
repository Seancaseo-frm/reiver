use chrono::Utc;
use reiver_core::events::{EventPublisher, PlatformEventType};
use reqwest;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::app_state::RedisPool;
use crate::clickhouse_db::ClickHousePool;
use crate::config::Config;
use crate::maintenance::is_project_in_maintenance;
use crate::models::ProjectStatsWithExceptions;
use bb8_redis::redis::AsyncCommands;

// Old channel-based worker code has been removed.
// Error processing is now handled by the Kafka consumer (see src/kafka_consumer.rs).
// This file now only contains helper functions used by the Kafka consumer.

/// Store the most recent error with stacktrace in Redis for immediate access
/// This allows the error detail page to show stacktraces immediately without waiting for ClickHouse
pub async fn store_recent_exception_in_redis_v2(
    redis_pool: &RedisPool,
    project_id: Uuid,
    fingerprint: &str,
    exception_id: &Uuid,
    kafka_msg: &crate::kafka_consumer::ExceptionKafkaMessage,
    timestamp: &chrono::DateTime<Utc>,
) -> anyhow::Result<()> {
    let project_key = format!("stats:project:{}", project_id);
    let recent_key = format!("{}:recent_exceptions:{}", project_key, fingerprint);
    let mut conn = redis_pool
        .get()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get Redis connection: {}", e))?;

    // Parse stacktrace from JSON string
    let stacktrace: serde_json::Value =
        serde_json::from_str(&kafka_msg.stacktrace).unwrap_or(serde_json::json!([]));

    // Parse context, tags, user_data from JSON strings
    let context: serde_json::Value =
        serde_json::from_str(&kafka_msg.context).unwrap_or(serde_json::json!({}));
    let tags: serde_json::Value =
        serde_json::from_str(&kafka_msg.tags).unwrap_or(serde_json::json!({}));
    let user_data: serde_json::Value =
        serde_json::from_str(&kafka_msg.user_data).unwrap_or(serde_json::json!({}));

    let error_json = serde_json::json!({
        "id": exception_id.to_string(),
        "project_id": project_id.to_string(),
        "fingerprint": fingerprint,
        "level": kafka_msg.level,
        "message": kafka_msg.message,
        "exception_type": kafka_msg.exception_type,
        "exception_value": kafka_msg.exception_value,
        "stacktrace": stacktrace,
        "context": context,
        "tags": tags,
        "user_data": user_data,
        "timestamp": timestamp.timestamp_millis(),
        "created_at": chrono::Utc::now().timestamp_millis(),
    });

    let error_json_str = serde_json::to_string(&error_json)?;

    // Store in a list, keeping only the most recent 100 errors per fingerprint
    // Use LPUSH to add to the front, then LTRIM to keep only top 100
    let _: () = conn
        .lpush(&recent_key, &error_json_str)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to push recent exception to Redis: {}", e))?;

    let _: () = conn
        .ltrim(&recent_key, 0, 99)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to trim recent exceptions in Redis: {}", e))?;

    let _: () = conn
        .expire(&recent_key, 3 * 24 * 3600)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to set expiry on recent exceptions key: {}", e))?;

    Ok(())
}

/// Struct to pass context data for Redis caching
#[derive(Clone)]
pub struct ExceptionGroupContext {
    pub environment: Option<String>,
    pub version: Option<String>,
    pub deployment_id: Option<String>,
    pub region: Option<String>,
    pub host_name: Option<String>,
    pub runtime: Option<String>,
    pub pod_name: Option<String>,
    pub cluster_name: Option<String>,
    pub container_id: Option<String>,
    pub http_method: Option<String>,
    pub http_url: Option<String>,
    pub user_id: Option<String>,
}

/// Update stats in Redis incrementally after processing an error
/// Update stats in Redis incrementally after processing an error
/// Uses Redis pipelining to batch all operations into a single round-trip
pub async fn update_stats_in_redis(
    redis_pool: &RedisPool,
    project_id: Uuid,
    is_new_group: bool,
    fingerprint: &str,
    group_id: &str,
    message: &str,
    level: &str,
    exception_type: &Option<String>,
    exception_value: &Option<String>,
    service_name: &Option<String>,
    first_seen: chrono::DateTime<Utc>,
    last_seen: chrono::DateTime<Utc>,
    context: Option<&ExceptionGroupContext>,
) -> anyhow::Result<()> {
    use bb8_redis::redis::{pipe, Value};

    let project_key = format!("stats:project:{}", project_id);
    let mut conn = redis_pool
        .get()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get Redis connection: {}", e))?;

    let total_key = format!("{}:total_exceptions", project_key);
    let unresolved_key = format!("{}:unresolved_groups", project_key);
    let count_key = format!("{}:count:{}", project_key, fingerprint);
    let groups_key = format!("{}:exception_groups", project_key);
    let group_hash_key = format!("{}:group:{}", project_key, fingerprint);

    // Build group JSON
    let mut group_json = serde_json::json!({
        "id": group_id,
        "project_id": project_id.to_string(),
        "fingerprint": fingerprint,
        "first_seen": first_seen.timestamp_millis(),
        "last_seen": last_seen.timestamp_millis(),
        "status": "unresolved",
        "level": level,
        "message": message,
        "exception_type": exception_type,
        "exception_value": exception_value,
        "service_name": service_name,
    });

    // Add context fields if present
    if let Some(ctx) = context {
        if let Some(v) = &ctx.environment {
            group_json["environment"] = serde_json::json!(v);
        }
        if let Some(v) = &ctx.version {
            group_json["version"] = serde_json::json!(v);
        }
        if let Some(v) = &ctx.deployment_id {
            group_json["deployment_id"] = serde_json::json!(v);
        }
        if let Some(v) = &ctx.region {
            group_json["region"] = serde_json::json!(v);
        }
        if let Some(v) = &ctx.host_name {
            group_json["host_name"] = serde_json::json!(v);
        }
        if let Some(v) = &ctx.runtime {
            group_json["runtime"] = serde_json::json!(v);
        }
        if let Some(v) = &ctx.pod_name {
            group_json["pod_name"] = serde_json::json!(v);
        }
        if let Some(v) = &ctx.cluster_name {
            group_json["cluster_name"] = serde_json::json!(v);
        }
        if let Some(v) = &ctx.container_id {
            group_json["container_id"] = serde_json::json!(v);
        }
        if let Some(v) = &ctx.http_method {
            group_json["http_method"] = serde_json::json!(v);
        }
        if let Some(v) = &ctx.http_url {
            group_json["http_url"] = serde_json::json!(v);
        }
        if let Some(v) = &ctx.user_id {
            group_json["user_id"] = serde_json::json!(v);
        }
    }

    let group_json_str = serde_json::to_string(&group_json)?;

    // First pipeline: get the count so we can compute the score
    // We need the count first because the score depends on it
    let count: i64 = {
        let mut p = pipe();
        p.atomic()
            .cmd("INCR")
            .arg(&count_key)
            .cmd("EXPIRE")
            .arg(&count_key)
            .arg(3 * 24 * 3600)
            .ignore();

        let results: Vec<Value> = p
            .query_async(&mut *conn)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to execute first Redis pipeline: {}", e))?;

        // First result is the INCR result
        match results.first() {
            Some(Value::Int(n)) => *n,
            _ => 1, // Default to 1 if we can't get the count
        }
    };

    // Compute score for sorted set
    // Sort by count (descending) - groups with most errors stay at top
    // Use large multiplier (1e12) and subtract last_seen_ms to break ties by most recent
    let last_seen_ms = last_seen.timestamp_millis();
    let score = (count as f64 * 1e12) - (last_seen_ms as f64);

    // Second pipeline: all remaining operations in a single round-trip
    let mut p = pipe();
    p.atomic();

    // Increment total exceptions + TTL
    p.cmd("INCR")
        .arg(&total_key)
        .ignore()
        .cmd("EXPIRE")
        .arg(&total_key)
        .arg(3 * 24 * 3600)
        .ignore();

    // If new group, increment unresolved counter
    if is_new_group {
        p.cmd("INCR")
            .arg(&unresolved_key)
            .ignore()
            .cmd("EXPIRE")
            .arg(&unresolved_key)
            .arg(3 * 24 * 3600)
            .ignore();
    }

    // Add to sorted set, store group data, trim old entries, set TTLs
    p.cmd("ZADD")
        .arg(&groups_key)
        .arg(score)
        .arg(fingerprint)
        .ignore()
        .cmd("SET")
        .arg(&group_hash_key)
        .arg(&group_json_str)
        .ignore()
        .cmd("ZREMRANGEBYRANK")
        .arg(&groups_key)
        .arg(0)
        .arg(-101)
        .ignore()
        .cmd("EXPIRE")
        .arg(&groups_key)
        .arg(24 * 3600)
        .ignore()
        .cmd("EXPIRE")
        .arg(&group_hash_key)
        .arg(24 * 3600)
        .ignore();

    // Use () to skip parsing results since we're ignoring all of them
    p.query_async::<()>(&mut *conn)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to execute second Redis pipeline: {}", e))?;

    Ok(())
}

/// Get stats from Redis (fast, no ClickHouse query)
/// Uses batched MGET operations to minimize Redis round-trips
/// error_rate_24h is skipped for SSE to avoid memory-intensive queries
/// Use get_project_stats endpoint for complete stats including error_rate_24h
pub async fn get_stats_from_redis(
    redis_pool: &RedisPool,
    project_id: Uuid,
) -> anyhow::Result<Option<ProjectStatsWithExceptions>> {
    use crate::models::{ExceptionGroup, ProjectStats};
    use bb8_redis::redis::cmd;
    use chrono::{TimeZone, Utc};

    let project_key = format!("stats:project:{}", project_id);
    let mut conn = redis_pool
        .get()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get Redis connection: {}", e))?;

    // Batch 1: Get all stats counters in one MGET
    let total_key = format!("{}:total_exceptions", project_key);
    let unresolved_key = format!("{}:unresolved_groups", project_key);
    let resolved_key = format!("{}:resolved_groups", project_key);

    let stats_values: Vec<Option<i64>> = cmd("MGET")
        .arg(&total_key)
        .arg(&unresolved_key)
        .arg(&resolved_key)
        .query_async(&mut *conn)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get stats from Redis: {}", e))?;

    let total_exceptions = stats_values.get(0).and_then(|v| *v).unwrap_or(0);
    let unresolved_exceptions = stats_values.get(1).and_then(|v| *v).unwrap_or(0);
    let resolved_exceptions = stats_values.get(2).and_then(|v| *v).unwrap_or(0);

    if total_exceptions == 0 && unresolved_exceptions == 0 && resolved_exceptions == 0 {
        return Ok(None);
    }

    // Get fingerprints from sorted set
    let groups_key = format!("{}:exception_groups", project_key);
    let fingerprints: Vec<String> = conn.zrevrange(&groups_key, 0, 99).await.map_err(|e| {
        anyhow::anyhow!(
            "Failed to get exception group fingerprints from Redis: {}",
            e
        )
    })?;

    // Batch 2: Get all group data and counts in one MGET
    let mut group_keys: Vec<String> = Vec::with_capacity(fingerprints.len() * 2);
    for fingerprint in &fingerprints {
        group_keys.push(format!("{}:group:{}", project_key, fingerprint));
        group_keys.push(format!("{}:count:{}", project_key, fingerprint));
    }

    let group_values: Vec<Option<String>> = if !group_keys.is_empty() {
        let mut mget_cmd = cmd("MGET");
        for key in &group_keys {
            mget_cmd.arg(key);
        }
        mget_cmd
            .query_async(&mut *conn)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get group data from Redis: {}", e))?
    } else {
        Vec::new()
    };

    // Parse group data from batched results
    let mut exception_groups = Vec::new();
    for (i, _fingerprint) in fingerprints.iter().enumerate() {
        let group_json_str = group_values.get(i * 2).and_then(|v| v.clone());
        let count_str = group_values.get(i * 2 + 1).and_then(|v| v.clone());

        if let Some(json_str) = group_json_str {
            if let Ok(group_json) = serde_json::from_str::<serde_json::Value>(&json_str) {
                let count = count_str.and_then(|s| s.parse::<i64>().ok()).unwrap_or(1);

                exception_groups.push(ExceptionGroup {
                    id: uuid::Uuid::parse_str(group_json["id"].as_str().unwrap_or(""))
                        .unwrap_or_default(),
                    project_id: uuid::Uuid::parse_str(
                        group_json["project_id"].as_str().unwrap_or(""),
                    )
                    .unwrap_or_default(),
                    fingerprint: group_json["fingerprint"].as_str().unwrap_or("").to_string(),
                    first_seen: Utc
                        .timestamp_millis_opt(group_json["first_seen"].as_i64().unwrap_or(0))
                        .single()
                        .unwrap_or_else(|| Utc::now()),
                    last_seen: Utc
                        .timestamp_millis_opt(group_json["last_seen"].as_i64().unwrap_or(0))
                        .single()
                        .unwrap_or_else(|| Utc::now()),
                    count,
                    status: group_json["status"]
                        .as_str()
                        .unwrap_or("unresolved")
                        .to_string(),
                    level: group_json["level"].as_str().unwrap_or("error").to_string(),
                    message: group_json["message"].as_str().unwrap_or("").to_string(),
                    exception_type: group_json
                        .get("exception_type")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    exception_value: group_json
                        .get("exception_value")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    service_name: group_json
                        .get("service_name")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    // Deployment & environment context
                    environment: group_json
                        .get("environment")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    version: group_json
                        .get("version")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    deployment_id: group_json
                        .get("deployment_id")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    region: group_json
                        .get("region")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    host_name: group_json
                        .get("host_name")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    runtime: group_json
                        .get("runtime")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    // Kubernetes / container context
                    pod_name: group_json
                        .get("pod_name")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    cluster_name: group_json
                        .get("cluster_name")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    container_id: group_json
                        .get("container_id")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    // HTTP context
                    http_method: group_json
                        .get("http_method")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    http_url: group_json
                        .get("http_url")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    // User context
                    user_id: group_json
                        .get("user_id")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                });
            }
        }
    }

    // Batch 3: Get all 24h rate keys in one MGET
    let now = chrono::Utc::now();
    let mut rate_keys: Vec<String> = Vec::with_capacity(24);
    let mut rate_timestamps: Vec<i64> = Vec::with_capacity(24);

    for i in 0..24 {
        let hour_time = now - chrono::Duration::hours(i);
        let hour_timestamp = (hour_time.timestamp() / 3600) * 3600;
        rate_keys.push(format!("{}:exception_rate:{}", project_key, hour_timestamp));
        rate_timestamps.push(hour_timestamp);
    }

    let rate_values: Vec<Option<String>> = {
        let mut mget_cmd = cmd("MGET");
        for key in &rate_keys {
            mget_cmd.arg(key);
        }
        mget_cmd
            .query_async(&mut *conn)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get exception rates from Redis: {}", e))?
    };

    let mut exception_rate_24h = Vec::new();
    for (i, count_str) in rate_values.iter().enumerate() {
        if let Some(s) = count_str {
            if let Ok(count) = s.parse::<i64>() {
                if count > 0 {
                    let hour_datetime =
                        chrono::DateTime::<chrono::Utc>::from_timestamp(rate_timestamps[i], 0)
                            .unwrap_or_else(|| chrono::Utc::now());
                    exception_rate_24h.push(crate::models::ExceptionRatePoint {
                        time: hour_datetime,
                        count,
                    });
                }
            }
        }
    }

    exception_rate_24h.sort_by_key(|p| p.time);

    let stats = ProjectStats {
        total_exceptions,
        unresolved_exceptions,
        resolved_exceptions,
        exception_rate_24h,
    };

    Ok(Some(ProjectStatsWithExceptions {
        stats,
        exception_groups,
    }))
}

pub(crate) async fn check_alerts(
    db_pool: &crate::db::DbPool,
    clickhouse_pool: &ClickHousePool,
    project_id: uuid::Uuid,
    fingerprint: &str,
    error_group_id: &str,
    _config: &Config,
    event_publisher: &EventPublisher,
) -> anyhow::Result<()> {
    // Skip notifications if project is in maintenance window
    if is_project_in_maintenance(db_pool, project_id).await? {
        debug!(
            "Project {} is in maintenance window, skipping exception notification for group {}",
            project_id, error_group_id
        );
        return Ok(());
    }

    // Get error group details from ClickHouse (for alert messages)
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct ErrorGroupDetailRow {
        message: String,
        exception_type: Option<String>,
        exception_value: Option<String>,
    }

    let error_details: Option<ErrorGroupDetailRow> = clickhouse_pool.as_ref()
        .query("SELECT argMax(message, timestamp) as message, argMax(exception_type, timestamp) as exception_type, argMax(exception_value, timestamp) as exception_value FROM reiver.exceptions WHERE project_id = ? AND fingerprint = ? GROUP BY project_id, fingerprint LIMIT 1")
        .bind(project_id.to_string())
        .bind(fingerprint)
        .fetch_optional()
        .await?;

    let (message, exception_type, exception_value) = if let Some(row) = error_details {
        (row.message, row.exception_type, row.exception_value)
    } else {
        (format!("Error group: {}", fingerprint), None, None)
    };

    info!(
        "New error group detected for project_id={}, group_id={}.",
        project_id, error_group_id
    );

    // Emit platform event — the event worker handles notifications
    if let Err(e) = event_publisher
        .emit(
            PlatformEventType::ExceptionGroupCreated,
            project_id,
            format!("exception:{}", fingerprint),
            serde_json::json!({
                "fingerprint": fingerprint,
                "error_group_id": error_group_id,
                "message": message,
                "exception_type": exception_type,
                "exception_value": exception_value,
            }),
        )
        .await
    {
        warn!("Failed to emit ExceptionGroupCreated event: {}", e);
    }

    // Trigger MooDeng auto-investigation
    trigger_exception_investigation(
        db_pool,
        project_id,
        fingerprint,
        exception_type.as_deref(),
        exception_value.as_deref(),
        false,
    )
    .await;

    Ok(())
}

/// Check and send regression alerts when a resolved exception receives new errors
/// This is triggered when an exception group that was marked as "resolved" gets a new error
pub(crate) async fn check_regression_alerts(
    db_pool: &crate::db::DbPool,
    clickhouse_pool: &ClickHousePool,
    project_id: uuid::Uuid,
    fingerprint: &str,
    error_group_id: &str,
    _config: &Config,
    event_publisher: &EventPublisher,
) -> anyhow::Result<()> {
    // Skip notifications if project is in maintenance window
    if is_project_in_maintenance(db_pool, project_id).await? {
        debug!(
            "Project {} is in maintenance window, skipping regression notification for group {}",
            project_id, error_group_id
        );
        return Ok(());
    }

    // Get error group details from ClickHouse (for alert messages)
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct ErrorGroupDetailRow {
        message: String,
        exception_type: Option<String>,
        exception_value: Option<String>,
    }

    let error_details: Option<ErrorGroupDetailRow> = clickhouse_pool.as_ref()
        .query("SELECT argMax(message, timestamp) as message, argMax(exception_type, timestamp) as exception_type, argMax(exception_value, timestamp) as exception_value FROM reiver.exceptions WHERE project_id = ? AND fingerprint = ? GROUP BY project_id, fingerprint LIMIT 1")
        .bind(project_id.to_string())
        .bind(fingerprint)
        .fetch_optional()
        .await?;

    let (message, exception_type, exception_value) = if let Some(row) = error_details {
        (row.message, row.exception_type, row.exception_value)
    } else {
        (format!("Error group: {}", fingerprint), None, None)
    };

    info!(
        "Exception regression detected for project_id={}, group_id={}.",
        project_id, error_group_id
    );

    // Emit platform event — the event worker handles notifications
    if let Err(e) = event_publisher
        .emit(
            PlatformEventType::ExceptionGroupRegressed,
            project_id,
            format!("exception_regressed:{}", fingerprint),
            serde_json::json!({
                "fingerprint": fingerprint,
                "error_group_id": error_group_id,
                "message": message,
                "exception_type": exception_type,
                "exception_value": exception_value,
            }),
        )
        .await
    {
        warn!("Failed to emit ExceptionGroupRegressed event: {}", e);
    }

    // Trigger MooDeng auto-investigation
    trigger_exception_investigation(
        db_pool,
        project_id,
        fingerprint,
        exception_type.as_deref(),
        exception_value.as_deref(),
        true,
    )
    .await;

    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════
// MooDeng auto-investigation trigger for exceptions
// ═══════════════════════════════════════════════════════════════════════════

async fn trigger_exception_investigation(
    db_pool: &crate::db::DbPool,
    project_id: uuid::Uuid,
    fingerprint: &str,
    exception_type: Option<&str>,
    exception_value: Option<&str>,
    is_regression: bool,
) {
    use sqlx::Row;

    // Check project setting
    let enabled: bool = match sqlx::query_scalar::<_, String>(
        "SELECT value FROM project_settings WHERE project_id = $1 AND key = 'gateway_auto_investigate'",
    )
    .bind(project_id)
    .fetch_optional(db_pool)
    .await
    {
        Ok(Some(v)) => v == "true",
        _ => false,
    };

    if !enabled {
        return;
    }

    let flow_url = std::env::var("FLOW_GATEWAY_URL")
        .or_else(|_| std::env::var("FLOW_URL"))
        .unwrap_or_else(|_| "http://localhost:3001".into());

    // Get all enabled notification channel IDs for the project
    let channel_ids: Vec<uuid::Uuid> = match sqlx::query(
        "SELECT id FROM notification_channels WHERE project_id = $1 AND enabled = true",
    )
    .bind(project_id)
    .fetch_all(db_pool)
    .await
    {
        Ok(rows) => rows.iter().map(|r| r.get::<uuid::Uuid, _>("id")).collect(),
        Err(e) => {
            tracing::warn!(error = %e, "Failed to load channels for investigation");
            Vec::new()
        }
    };

    let trigger_type = if is_regression {
        "regression"
    } else {
        "exception"
    };
    let exc_type_str = exception_type.unwrap_or("Unknown");
    let exc_val_str = exception_value.unwrap_or("");

    let trigger_summary = if is_regression {
        format!("Exception regression: {} — {}", exc_type_str, exc_val_str)
    } else {
        format!("New exception: {} — {}", exc_type_str, exc_val_str)
    };

    let trigger_context = serde_json::json!({
        "fingerprint": fingerprint,
        "exception_type": exception_type,
        "exception_value": exception_value,
        "is_regression": is_regression,
    });

    let payload = serde_json::json!({
        "project_id": project_id,
        "trigger_type": trigger_type,
        "trigger_ref": fingerprint,
        "trigger_summary": trigger_summary,
        "trigger_context": trigger_context,
        "notification_channel_ids": channel_ids,
    });

    let url = format!("{}/api/internal/investigate", flow_url);

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());

    match client
        .post(&url)
        .header("X-Project-Id", project_id.to_string())
        .json(&payload)
        .send()
        .await
    {
        Ok(resp)
            if resp.status().is_success() || resp.status() == reqwest::StatusCode::ACCEPTED =>
        {
            tracing::info!(%project_id, %fingerprint, "Triggered auto-investigation for {trigger_type}");
        }
        Ok(resp) => {
            tracing::debug!(
                %project_id,
                %fingerprint,
                status = %resp.status(),
                "Auto-investigation request returned non-OK for {trigger_type}",
            );
        }
        Err(e) => {
            tracing::warn!(%project_id, %fingerprint, error = %e, "Failed to trigger auto-investigation for {trigger_type}");
        }
    }
}
