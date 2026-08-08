use axum::{
    extract::{Path, Query, State},
    response::Json,
    routing::get,
    Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

use crate::app_state::WatchState;
use crate::error::{AppError, Result};
use crate::models::ExceptionRatePoint;
use crate::query_cache::{get_cached_query, set_cached_query, CacheTTL};

pub fn create_historical_router() -> Router<Arc<WatchState>> {
    Router::new()
        .route(
            "/projects/{project_id}/error-rate",
            get(get_error_rate_history),
        )
        .route(
            "/projects/{project_id}/trace-duration",
            get(get_trace_duration_history),
        )
        .route(
            "/projects/{project_id}/service-latency",
            get(get_service_latency_history),
        )
        .route(
            "/projects/{project_id}/error-counts",
            get(get_error_counts_history),
        )
}

/// Get error rate history for a project (polling endpoint)
/// GET /api/historical/projects/{project_id}/error-rate?time_range=24h&interval=hour
///
/// Query parameters:
/// - time_range: 24h, 7d, 30d (default: 24h)
/// - interval: hour, day (default: auto-based on time_range)
/// - fingerprint: optional filter by error fingerprint
async fn get_error_rate_history(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Vec<ExceptionRatePoint>>> {
    // Authenticated by website proxy via trusted headers

    // Get query parameters
    let time_range = params
        .get("time_range")
        .map(|s| s.as_str())
        .unwrap_or("24h");
    let interval_param = params.get("interval");
    let fingerprint = params.get("fingerprint");

    // Determine interval and hours based on time range
    let (interval, hours) = match time_range {
        "24h" => ("HOUR", 24),
        "7d" => ("DAY", 168),  // 7 days = 168 hours
        "30d" => ("DAY", 720), // 30 days = 720 hours
        _ => ("HOUR", 24),
    };

    // Override interval if explicitly provided
    let interval = if let Some(interval_override) = interval_param {
        match interval_override.as_str() {
            "hour" | "HOUR" => "HOUR",
            "day" | "DAY" => "DAY",
            _ => interval,
        }
    } else {
        interval
    };

    // Build query - optimized for ClickHouse ORDER BY (project_id, timestamp)
    // Always filter by project_id first (ORDER BY key) for better performance
    // Use PREWHERE for fingerprint filter when available (can skip more data)
    let query = if let Some(_fingerprint) = fingerprint {
        if interval == "HOUR" {
            format!(
                "SELECT toDateTime64(toStartOfHour(timestamp), 9) as time, count() as count 
                 FROM reiver.exceptions 
                 PREWHERE fingerprint = ?
                 WHERE project_id = ? 
                 AND timestamp >= now() - INTERVAL {} HOUR 
                 GROUP BY time 
                 ORDER BY time",
                hours
            )
        } else {
            format!(
                "SELECT toDateTime64(toStartOfDay(timestamp), 9) as time, count() as count 
                 FROM reiver.exceptions 
                 PREWHERE fingerprint = ?
                 WHERE project_id = ? 
                 AND timestamp >= now() - INTERVAL {} HOUR 
                 GROUP BY time 
                 ORDER BY time",
                hours
            )
        }
    } else {
        if interval == "HOUR" {
            format!(
                "SELECT toDateTime64(toStartOfHour(timestamp), 9) as time, count() as count 
                 FROM reiver.exceptions 
                 WHERE project_id = ? 
                 AND timestamp >= now() - INTERVAL {} HOUR 
                 GROUP BY time 
                 ORDER BY time",
                hours
            )
        } else {
            format!(
                "SELECT toDateTime64(toStartOfDay(timestamp), 9) as time, count() as count 
                 FROM reiver.exceptions 
                 WHERE project_id = ? 
                 AND timestamp >= now() - INTERVAL {} HOUR 
                 GROUP BY time 
                 ORDER BY time",
                hours
            )
        }
    };

    // Build cache key
    let mut cache_params_strs = vec![
        project_id.to_string(),
        time_range.to_string(),
        interval.to_string(),
    ];
    if let Some(fingerprint) = fingerprint {
        cache_params_strs.push(fingerprint.clone());
    }
    let cache_params: Vec<&str> = cache_params_strs.iter().map(|s| s.as_str()).collect();

    // Determine cache TTL based on time range
    let cache_ttl = match time_range {
        "24h" => CacheTTL::Short, // 1 minute for recent data
        "7d" => CacheTTL::Medium, // 5 minutes for weekly data
        _ => CacheTTL::Long,      // 15 minutes for monthly data
    };

    // Check cache first
    let history: Vec<ExceptionRatePoint> = if let Some(cached) =
        get_cached_query::<Vec<ExceptionRatePoint>>(&state.redis, &query, &cache_params[..])
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Cache query failed: {}", e)))?
    {
        // Cache hit
        info!(
            "[Historical] Cache hit for error rate history: project_id={}, time_range={}",
            project_id, time_range
        );
        cached
    } else {
        // Cache miss - query ClickHouse
        info!(
            "[Historical] Cache miss for error rate history: project_id={}, time_range={}",
            project_id, time_range
        );

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct ErrorHistoryRow {
            #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
            time: chrono::DateTime<Utc>,
            count: u64,
        }

        let mut query_builder = state.clickhouse.as_ref().query(&query);
        // Bind parameters in the order they appear in the query
        // PREWHERE comes before WHERE, so fingerprint is bound first if present
        if let Some(fingerprint) = fingerprint {
            query_builder = query_builder.bind(fingerprint);
            query_builder = query_builder.bind(project_id.to_string());
        } else {
            query_builder = query_builder.bind(project_id.to_string());
        }

        let history_rows: Vec<ErrorHistoryRow> = query_builder
            .fetch_all()
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

        let history: Vec<ExceptionRatePoint> = history_rows
            .into_iter()
            .map(|row| ExceptionRatePoint {
                time: row.time,
                count: row.count as i64,
            })
            .collect();

        // Store in cache
        let _ =
            set_cached_query(&state.redis, &query, &cache_params[..], &history, cache_ttl).await;

        history
    };

    Ok(Json(history))
}

/// Get trace duration history for a project (polling endpoint)
/// GET /api/historical/projects/{project_id}/trace-duration?time_range=24h&interval=hour
#[derive(Debug, Serialize, Deserialize)]
struct TraceDurationPoint {
    time: DateTime<Utc>,
    p50_duration_ms: f64,
    p95_duration_ms: f64,
    p99_duration_ms: f64,
    avg_duration_ms: f64,
}

async fn get_trace_duration_history(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Vec<TraceDurationPoint>>> {
    // Authenticated by website proxy via trusted headers

    let time_range = params
        .get("time_range")
        .map(|s| s.as_str())
        .unwrap_or("24h");
    let (interval, hours) = match time_range {
        "24h" => ("HOUR", 24),
        "7d" => ("DAY", 168),
        "30d" => ("DAY", 720),
        _ => ("HOUR", 24),
    };

    let query = if interval == "HOUR" {
        format!(
            r#"
            SELECT 
                toDateTime64(toStartOfHour(span_time), 9) as time,
                quantile(0.5)(duration_ms) as p50_duration_ms,
                quantile(0.95)(duration_ms) as p95_duration_ms,
                quantile(0.99)(duration_ms) as p99_duration_ms,
                avg(duration_ms) as avg_duration_ms
            FROM (
                SELECT 
                    trace_id,
                    min(timestamp) as span_time,
                    max(duration / 1000000) as duration_ms
                FROM reiver.spans
                WHERE project_id = ?
                AND timestamp >= now() - INTERVAL {} HOUR
                GROUP BY trace_id
            )
            GROUP BY time
            ORDER BY time
            "#,
            hours
        )
    } else {
        format!(
            r#"
            SELECT 
                toDateTime64(toStartOfDay(span_time), 9) as time,
                quantile(0.5)(duration_ms) as p50_duration_ms,
                quantile(0.95)(duration_ms) as p95_duration_ms,
                quantile(0.99)(duration_ms) as p99_duration_ms,
                avg(duration_ms) as avg_duration_ms
            FROM (
                SELECT 
                    trace_id,
                    min(timestamp) as span_time,
                    max(duration / 1000000) as duration_ms
                FROM reiver.spans
                WHERE project_id = ?
                AND timestamp >= now() - INTERVAL {} HOUR
                GROUP BY trace_id
            )
            GROUP BY time
            ORDER BY time
            "#,
            hours
        )
    };

    let cache_params_strs = vec![
        project_id.to_string(),
        time_range.to_string(),
        interval.to_string(),
    ];
    let cache_params: Vec<&str> = cache_params_strs.iter().map(|s| s.as_str()).collect();
    let cache_ttl = match time_range {
        "24h" => CacheTTL::Short,
        "7d" => CacheTTL::Medium,
        _ => CacheTTL::Long,
    };

    let history: Vec<TraceDurationPoint> = if let Some(cached) =
        get_cached_query::<Vec<TraceDurationPoint>>(&state.redis, &query, &cache_params[..])
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Cache query failed: {}", e)))?
    {
        cached
    } else {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct TraceDurationRow {
            #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
            time: chrono::DateTime<Utc>,
            p50_duration_ms: f64,
            p95_duration_ms: f64,
            p99_duration_ms: f64,
            avg_duration_ms: f64,
        }

        let history_rows: Vec<TraceDurationRow> = state
            .clickhouse
            .as_ref()
            .query(&query)
            .bind(project_id.to_string())
            .fetch_all()
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

        let history: Vec<TraceDurationPoint> = history_rows
            .into_iter()
            .map(|row| TraceDurationPoint {
                time: row.time,
                p50_duration_ms: row.p50_duration_ms,
                p95_duration_ms: row.p95_duration_ms,
                p99_duration_ms: row.p99_duration_ms,
                avg_duration_ms: row.avg_duration_ms,
            })
            .collect();

        let _ =
            set_cached_query(&state.redis, &query, &cache_params[..], &history, cache_ttl).await;

        history
    };

    Ok(Json(history))
}

/// Get service latency history (polling endpoint)
/// GET /api/historical/projects/{project_id}/service-latency?time_range=24h&service_name=api
#[derive(Debug, Serialize, Deserialize)]
struct ServiceLatencyPoint {
    time: DateTime<Utc>,
    service_name: String,
    avg_latency_ms: f64,
    p95_latency_ms: f64,
    request_count: u64,
}

async fn get_service_latency_history(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Vec<ServiceLatencyPoint>>> {
    // Authenticated by website proxy via trusted headers

    let time_range = params
        .get("time_range")
        .map(|s| s.as_str())
        .unwrap_or("24h");
    let service_name = params.get("service_name");
    let (interval, hours) = match time_range {
        "24h" => ("HOUR", 24),
        "7d" => ("DAY", 168),
        "30d" => ("DAY", 720),
        _ => ("HOUR", 24),
    };

    let query = if let Some(_service_name) = service_name {
        if interval == "HOUR" {
            format!(
                r#"
                SELECT 
                    toDateTime64(toStartOfHour(timestamp), 9) as time,
                    service_name,
                    avg(duration / 1000000) as avg_latency_ms,
                    quantile(0.95)(duration / 1000000) as p95_latency_ms,
                    count() as request_count
                FROM reiver.spans
                WHERE project_id = ? AND service_name = ?
                AND timestamp >= now() - INTERVAL {} HOUR
                GROUP BY time, service_name
                ORDER BY time, service_name
                "#,
                hours
            )
        } else {
            format!(
                r#"
                SELECT 
                    toDateTime64(toStartOfDay(timestamp), 9) as time,
                    service_name,
                    avg(duration / 1000000) as avg_latency_ms,
                    quantile(0.95)(duration / 1000000) as p95_latency_ms,
                    count() as request_count
                FROM reiver.spans
                WHERE project_id = ? AND service_name = ?
                AND timestamp >= now() - INTERVAL {} HOUR
                GROUP BY time, service_name
                ORDER BY time, service_name
                "#,
                hours
            )
        }
    } else {
        if interval == "HOUR" {
            format!(
                r#"
                SELECT 
                    toDateTime64(toStartOfHour(timestamp), 9) as time,
                    service_name,
                    avg(duration / 1000000) as avg_latency_ms,
                    quantile(0.95)(duration / 1000000) as p95_latency_ms,
                    count() as request_count
                FROM reiver.spans
                WHERE project_id = ?
                AND timestamp >= now() - INTERVAL {} HOUR
                GROUP BY time, service_name
                ORDER BY time, service_name
                "#,
                hours
            )
        } else {
            format!(
                r#"
                SELECT 
                    toDateTime64(toStartOfDay(timestamp), 9) as time,
                    service_name,
                    avg(duration / 1000000) as avg_latency_ms,
                    quantile(0.95)(duration / 1000000) as p95_latency_ms,
                    count() as request_count
                FROM reiver.spans
                WHERE project_id = ?
                AND timestamp >= now() - INTERVAL {} HOUR
                GROUP BY time, service_name
                ORDER BY time, service_name
                "#,
                hours
            )
        }
    };

    let mut cache_params_strs = vec![
        project_id.to_string(),
        time_range.to_string(),
        interval.to_string(),
    ];
    if let Some(service_name) = service_name {
        cache_params_strs.push(service_name.clone());
    }
    let cache_params: Vec<&str> = cache_params_strs.iter().map(|s| s.as_str()).collect();

    let cache_ttl = match time_range {
        "24h" => CacheTTL::Short,
        "7d" => CacheTTL::Medium,
        _ => CacheTTL::Long,
    };

    let history: Vec<ServiceLatencyPoint> = if let Some(cached) =
        get_cached_query::<Vec<ServiceLatencyPoint>>(&state.redis, &query, &cache_params[..])
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Cache query failed: {}", e)))?
    {
        cached
    } else {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct ServiceLatencyRow {
            #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
            time: chrono::DateTime<Utc>,
            service_name: String,
            avg_latency_ms: f64,
            p95_latency_ms: f64,
            request_count: u64,
        }

        let mut query_builder = state.clickhouse.as_ref().query(&query);
        query_builder = query_builder.bind(project_id.to_string());
        if let Some(service_name) = service_name {
            query_builder = query_builder.bind(service_name);
        }

        let history_rows: Vec<ServiceLatencyRow> = query_builder
            .fetch_all()
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

        let history: Vec<ServiceLatencyPoint> = history_rows
            .into_iter()
            .map(|row| ServiceLatencyPoint {
                time: row.time,
                service_name: row.service_name,
                avg_latency_ms: row.avg_latency_ms,
                p95_latency_ms: row.p95_latency_ms,
                request_count: row.request_count,
            })
            .collect();

        let _ =
            set_cached_query(&state.redis, &query, &cache_params[..], &history, cache_ttl).await;

        history
    };

    Ok(Json(history))
}

/// Get error counts by level/type (polling endpoint)
/// GET /api/historical/projects/{project_id}/error-counts?time_range=24h
#[derive(Debug, Serialize, Deserialize)]
struct ErrorCountsPoint {
    time: DateTime<Utc>,
    level: String,
    count: u64,
}

async fn get_error_counts_history(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Vec<ErrorCountsPoint>>> {
    // Authenticated by website proxy via trusted headers

    let time_range = params
        .get("time_range")
        .map(|s| s.as_str())
        .unwrap_or("24h");
    let (interval, hours) = match time_range {
        "24h" => ("HOUR", 24),
        "7d" => ("DAY", 168),
        "30d" => ("DAY", 720),
        _ => ("HOUR", 24),
    };

    let query = if interval == "HOUR" {
        format!(
            r#"
            SELECT 
                toDateTime64(toStartOfHour(timestamp), 9) as time,
                level,
                count() as count
            FROM reiver.exceptions
            WHERE project_id = ?
            AND timestamp >= now() - INTERVAL {} HOUR
            GROUP BY time, level
            ORDER BY time, level
            "#,
            hours
        )
    } else {
        format!(
            r#"
            SELECT 
                toDateTime64(toStartOfDay(timestamp), 9) as time,
                level,
                count() as count
            FROM reiver.exceptions
            WHERE project_id = ?
            AND timestamp >= now() - INTERVAL {} HOUR
            GROUP BY time, level
            ORDER BY time, level
            "#,
            hours
        )
    };

    let cache_params_strs = vec![
        project_id.to_string(),
        time_range.to_string(),
        interval.to_string(),
    ];
    let cache_params: Vec<&str> = cache_params_strs.iter().map(|s| s.as_str()).collect();
    let cache_ttl = match time_range {
        "24h" => CacheTTL::Short,
        "7d" => CacheTTL::Medium,
        _ => CacheTTL::Long,
    };

    let history: Vec<ErrorCountsPoint> = if let Some(cached) =
        get_cached_query::<Vec<ErrorCountsPoint>>(&state.redis, &query, &cache_params[..])
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Cache query failed: {}", e)))?
    {
        cached
    } else {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct ErrorCountsRow {
            #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
            time: chrono::DateTime<Utc>,
            level: String,
            count: u64,
        }

        let history_rows: Vec<ErrorCountsRow> = state
            .clickhouse
            .as_ref()
            .query(&query)
            .bind(project_id.to_string())
            .fetch_all()
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

        let history: Vec<ErrorCountsPoint> = history_rows
            .into_iter()
            .map(|row| ErrorCountsPoint {
                time: row.time,
                level: row.level,
                count: row.count,
            })
            .collect();

        let _ =
            set_cached_query(&state.redis, &query, &cache_params[..], &history, cache_ttl).await;

        history
    };

    Ok(Json(history))
}
