//! Worker helpers for the website project.
//! Contains only the functions needed by project API handlers.

use crate::app_state::RedisPool;
use crate::models::ProjectStatsWithExceptions;
use bb8_redis::redis::AsyncCommands;
use uuid::Uuid;

/// Get stats from Redis (fast, no ClickHouse query)
/// Uses batched MGET operations to minimize Redis round-trips
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
                    id: Uuid::parse_str(group_json["id"].as_str().unwrap_or(""))
                        .unwrap_or_default(),
                    project_id: Uuid::parse_str(group_json["project_id"].as_str().unwrap_or(""))
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
                    http_method: group_json
                        .get("http_method")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    http_url: group_json
                        .get("http_url")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    user_id: group_json
                        .get("user_id")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                });
            }
        }
    }

    // Batch 3: Get all 24h rate keys in one MGET
    let now = Utc::now();
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
                        chrono::DateTime::<Utc>::from_timestamp(rate_timestamps[i], 0)
                            .unwrap_or_else(|| Utc::now());
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
