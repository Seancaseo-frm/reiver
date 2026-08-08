//! Incidents API: unique exceptions and OTLP errors in a time range, plus context (logs, traces, alerts).

use axum::http::HeaderMap;
use axum::{
    extract::{Path, Query, State},
    response::Json,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::app_state::WebsiteState;
use crate::auth::{authenticate_request, verify_project_access};
use crate::error::{AppError, Result};
use crate::rate_limit::RateLimitType;

#[derive(Debug, Serialize)]
pub struct IncidentExceptionSummary {
    pub id: Uuid,
    pub fingerprint: String,
    pub message: String,
    pub exception_type: Option<String>,
    pub count: i64,
    pub first_seen: chrono::DateTime<Utc>,
    pub last_seen: chrono::DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct IncidentContextLog {
    pub body: String,
    pub severity_text: String,
    pub service_name: String,
    pub source: String,
    pub timestamp: chrono::DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct IncidentContextTrace {
    pub trace_id: String,
    pub start_time: chrono::DateTime<Utc>,
    pub duration_ms: i64,
    pub span_count: u64,
    pub status: String,
}

#[derive(Debug, Serialize)]
pub struct IncidentContextAlert {
    pub id: Uuid,
    pub rule_id: Uuid,
    pub rule_name: String,
    pub state: String,
    pub fired_at: Option<chrono::DateTime<Utc>>,
    pub value: Option<f64>,
}

/// One event for the unified incident timeline (log, trace, or alert).
#[derive(Debug, Serialize)]
pub struct TimelineEvent {
    pub r#type: String, // "log" | "trace" | "alert"
    pub time: chrono::DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub level: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rule_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alert_state: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct LogsAround {
    pub logs: Vec<IncidentContextLog>,
}

#[derive(Debug, Serialize)]
pub struct IncidentContextResponse {
    pub logs: Vec<IncidentContextLog>,
    pub traces: Vec<IncidentContextTrace>,
    pub alerts: Vec<IncidentContextAlert>,
    /// Unified timeline: all events in time order (log, trace, alert).
    pub timeline: Vec<TimelineEvent>,
    /// When `around_ms` is passed: logs in [around_ms − 2m, around_ms + 2m], optionally by service.
    pub logs_around: Option<LogsAround>,
}

#[derive(Debug, Deserialize)]
pub struct IncidentsTimeQuery {
    pub start_ms: i64,
    pub end_ms: i64,
    #[serde(default)]
    pub around_ms: Option<i64>,
    #[serde(default)]
    pub service_name: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ProjectIdPath {
    pub id: Uuid,
}

/// GET /api/projects/:id/incidents/exceptions?start_ms=&end_ms=
pub async fn list_incident_exceptions(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(p): Path<ProjectIdPath>,
    Query(q): Query<IncidentsTimeQuery>,
) -> Result<Json<Vec<IncidentExceptionSummary>>> {
    let project_id = p.id;
    let user_id = authenticate_request(&headers, &state, RateLimitType::Analytics).await?;
    verify_project_access(&state.db, project_id, user_id).await?;

    let start_dt = chrono::DateTime::from_timestamp_millis(q.start_ms)
        .unwrap_or_else(|| Utc::now() - chrono::Duration::days(1));
    let end_dt = chrono::DateTime::from_timestamp_millis(q.end_ms).unwrap_or_else(Utc::now);
    let pid = project_id.to_string();

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct Row {
        id: String,
        fingerprint: String,
        message: String,
        exception_type: Option<String>,
        count: u64,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        first_seen: chrono::DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        last_seen: chrono::DateTime<Utc>,
    }

    let sql = r#"
        SELECT argMax(e.id, e.timestamp) AS id, e.fingerprint, argMax(e.message, e.timestamp) AS message,
               argMax(e.exception_type, e.timestamp) AS exception_type,
               count() AS count,
               min(e.timestamp) AS first_seen, max(e.timestamp) AS last_seen
        FROM reiver.exceptions e
        WHERE e.project_id = ?
        GROUP BY e.project_id, e.fingerprint
        HAVING min(e.timestamp) <= parseDateTime64BestEffort(?) AND max(e.timestamp) >= parseDateTime64BestEffort(?)
        ORDER BY last_seen DESC
        LIMIT 100
    "#;

    let rows: Vec<Row> = state
        .clickhouse
        .as_ref()
        .query(sql)
        .bind(&pid)
        .bind(end_dt.to_rfc3339())
        .bind(start_dt.to_rfc3339())
        .fetch_all()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse: {}", e)))?;

    let out: Vec<IncidentExceptionSummary> = rows
        .into_iter()
        .map(|r| IncidentExceptionSummary {
            id: Uuid::parse_str(&r.id).unwrap_or_default(),
            fingerprint: r.fingerprint,
            message: r.message,
            exception_type: r.exception_type,
            count: r.count as i64,
            first_seen: r.first_seen,
            last_seen: r.last_seen,
        })
        .collect();

    Ok(Json(out))
}

/// GET /api/projects/:id/incidents/context?start_ms=&end_ms=
pub async fn get_incident_context(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(p): Path<ProjectIdPath>,
    Query(q): Query<IncidentsTimeQuery>,
) -> Result<Json<IncidentContextResponse>> {
    let project_id = p.id;
    let user_id = authenticate_request(&headers, &state, RateLimitType::Analytics).await?;
    verify_project_access(&state.db, project_id, user_id).await?;

    let start_dt = chrono::DateTime::from_timestamp_millis(q.start_ms)
        .unwrap_or_else(|| Utc::now() - chrono::Duration::days(1));
    let end_dt = chrono::DateTime::from_timestamp_millis(q.end_ms).unwrap_or_else(Utc::now);
    let pid = project_id.to_string();

    // Logs from OTel-compatible logs table – last 100
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct LogRow {
        body: String,
        severity_text: String,
        service_name: String,
        source: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: chrono::DateTime<Utc>,
    }
    let log_sql = r#"
        SELECT body, severity_text, service_name, 
               log_attributes['source'] AS source, timestamp
        FROM reiver.logs
        WHERE project_id = ? AND timestamp >= ? AND timestamp < ?
        ORDER BY timestamp DESC LIMIT 100
    "#;
    let logs_data: Vec<IncidentContextLog> = state
        .clickhouse
        .as_ref()
        .query(log_sql)
        .bind(&pid)
        .bind(start_dt)
        .bind(end_dt)
        .fetch_all::<LogRow>()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse logs: {}", e)))?
        .into_iter()
        .map(|r| IncidentContextLog {
            body: r.body,
            severity_text: r.severity_text,
            service_name: r.service_name,
            source: r.source,
            timestamp: r.timestamp,
        })
        .collect();

    // Traces – from spans, group by trace_id, last 50
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct TraceRow {
        trace_id: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        min_start: chrono::DateTime<Utc>,
        sum_duration: i64,
        cnt: u64,
        status: String,
    }
    let trace_sql = r#"
        SELECT trace_id, min(timestamp) AS min_start,
               toInt64(sum(duration) / 1000000) AS sum_duration, count() AS cnt, anyLast(status_code) AS status
        FROM reiver.spans
        WHERE project_id = ? AND timestamp >= ? AND timestamp < ?
        GROUP BY trace_id, project_id
        ORDER BY min_start DESC LIMIT 50
    "#;
    let traces: Vec<IncidentContextTrace> = state
        .clickhouse
        .as_ref()
        .query(trace_sql)
        .bind(&pid)
        .bind(start_dt)
        .bind(end_dt)
        .fetch_all::<TraceRow>()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse traces: {}", e)))?
        .into_iter()
        .map(|r| IncidentContextTrace {
            trace_id: r.trace_id,
            start_time: r.min_start,
            duration_ms: r.sum_duration,
            span_count: r.cnt,
            status: r.status,
        })
        .collect();

    // Alerts – fired in the window (fired_at between start and end)
    #[derive(Debug, sqlx::FromRow)]
    struct AlertRow {
        id: Uuid,
        rule_id: Uuid,
        rule_name: String,
        state: String,
        fired_at: Option<chrono::DateTime<Utc>>,
        value: Option<f64>,
    }
    let alerts: Vec<IncidentContextAlert> = sqlx::query_as::<_, AlertRow>(
        r#"
        SELECT a.id, a.rule_id, r.name AS rule_name, a.state, a.fired_at, a.value
        FROM alerts a
        JOIN alert_rules r ON a.rule_id = r.id
        WHERE r.project_id = $1
          AND (a.fired_at IS NOT NULL AND a.fired_at >= $2 AND a.fired_at <= $3)
        ORDER BY a.fired_at DESC NULLS LAST
        LIMIT 50
        "#,
    )
    .bind(project_id)
    .bind(start_dt)
    .bind(end_dt)
    .fetch_all(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Alerts: {}", e)))?
    .into_iter()
    .map(|r| IncidentContextAlert {
        id: r.id,
        rule_id: r.rule_id,
        rule_name: r.rule_name,
        state: r.state,
        fired_at: r.fired_at,
        value: r.value,
    })
    .collect();

    // Build unified timeline (log, trace, alert) and sort by time
    let mut timeline: Vec<TimelineEvent> = Vec::new();
    for u in &logs_data {
        timeline.push(TimelineEvent {
            r#type: "log".into(),
            time: u.timestamp,
            trace_id: None,
            template: None,
            message: Some(u.body.clone()),
            level: Some(u.severity_text.clone()),
            service_name: Some(u.service_name.clone()),
            duration_ms: None,
            span_count: None,
            status: None,
            rule_name: None,
            alert_state: None,
        });
    }
    for t in &traces {
        timeline.push(TimelineEvent {
            r#type: "trace".into(),
            time: t.start_time,
            trace_id: Some(t.trace_id.clone()),
            template: None,
            message: None,
            level: None,
            service_name: None,
            duration_ms: Some(t.duration_ms),
            span_count: Some(t.span_count),
            status: Some(t.status.clone()),
            rule_name: None,
            alert_state: None,
        });
    }
    for a in &alerts {
        if let Some(at) = a.fired_at {
            timeline.push(TimelineEvent {
                r#type: "alert".into(),
                time: at,
                trace_id: None,
                template: None,
                message: None,
                level: None,
                service_name: None,
                duration_ms: None,
                span_count: None,
                status: None,
                rule_name: Some(a.rule_name.clone()),
                alert_state: Some(a.state.clone()),
            });
        }
    }
    timeline.sort_by_key(|e| e.time);

    // Query logs around the exception time (when around_ms is provided)
    let logs_around = if let Some(around_ms) = q.around_ms {
        let around_dt =
            chrono::DateTime::from_timestamp_millis(around_ms).unwrap_or_else(|| Utc::now());
        let start_around = around_dt - chrono::Duration::minutes(2);
        let end_around = around_dt + chrono::Duration::minutes(2);

        // Logs around (from OTel-compatible logs table)
        let mut logs_around_query = state.clickhouse.as_ref().query(
            if q.service_name.is_some() {
                "SELECT body, severity_text, service_name, log_attributes['source'] AS source, timestamp
                 FROM reiver.logs
                 WHERE project_id = ? AND service_name = ? AND timestamp >= ? AND timestamp < ?
                 ORDER BY timestamp DESC LIMIT 100"
            } else {
                "SELECT body, severity_text, service_name, log_attributes['source'] AS source, timestamp
                 FROM reiver.logs
                 WHERE project_id = ? AND timestamp >= ? AND timestamp < ?
                 ORDER BY timestamp DESC LIMIT 100"
            }
        ).bind(&pid);

        if let Some(ref sn) = q.service_name {
            logs_around_query = logs_around_query.bind(sn);
        }
        logs_around_query = logs_around_query.bind(start_around).bind(end_around);

        let mut logs_around_data = Vec::new();
        if let Ok(rows) = logs_around_query.fetch_all::<LogRow>().await {
            for row in rows {
                logs_around_data.push(IncidentContextLog {
                    body: row.body,
                    severity_text: row.severity_text,
                    service_name: row.service_name,
                    source: row.source,
                    timestamp: row.timestamp,
                });
            }
        }

        Some(LogsAround {
            logs: logs_around_data,
        })
    } else {
        None
    };

    Ok(Json(IncidentContextResponse {
        logs: logs_data,
        traces,
        alerts,
        timeline,
        logs_around,
    }))
}
