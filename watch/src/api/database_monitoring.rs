use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Json,
    routing::post,
    Router,
};
use chrono::Utc;
use serde::Deserialize;
use std::sync::Arc;
use tracing::{error, info};
use uuid::Uuid;

use crate::app_state::WatchState;
use crate::error::{AppError, Result};

pub fn create_database_monitoring_router() -> Router<Arc<WatchState>> {
    Router::new()
        .route("/explain-plans", post(store_explain_plan))
        .route("/query-metrics", post(store_query_metrics))
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)] // database_type field for future type-specific handling
struct ExplainPlanPayload {
    database_name: String, // Name from agent config
    database_host: String,
    database_type: String, // postgresql, mysql, etc.
    query_template: String,
    query_parameters: Option<serde_json::Value>,
    explain_plan: serde_json::Value,
    execution_time_ms: Option<f64>,
    planning_time_ms: Option<f64>,
    total_cost: Option<f64>,
    rows_estimated: Option<i64>,
    rows_actual: Option<i64>,
    has_full_table_scan: Option<bool>,
    has_missing_index: Option<bool>,
    has_sequential_scan: Option<bool>,
    trace_id: Option<String>,
    query_fingerprint: Option<String>, // For linking to query_metric_id if available
}

/// Store explain plan in database
/// POST /api/database-monitoring/explain-plans
async fn store_explain_plan(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<ExplainPlanPayload>,
) -> Result<StatusCode> {
    let project_id = crate::api::extract_project_id(&headers)?;

    // Try to find query_metric_id if query_fingerprint is provided
    let query_metric_id: Option<Uuid> = if let Some(ref fingerprint) = payload.query_fingerprint {
        sqlx::query_scalar(
            r#"
            SELECT id FROM database_query_metrics
            WHERE project_id = $1
              AND database_host = $2
              AND database_name = $3
              AND query_fingerprint = $4
            ORDER BY collected_at DESC
            LIMIT 1
            "#,
        )
        .bind(project_id)
        .bind(&payload.database_host)
        .bind(&payload.database_name)
        .bind(fingerprint)
        .fetch_optional(state.db.as_ref())
        .await
        .ok()
        .flatten()
    } else {
        None
    };

    // Store explain plan
    sqlx::query(
        r#"
        INSERT INTO database_explain_plans (
            project_id, query_metric_id, database_host, database_name,
            query_template, query_parameters,
            explain_plan,
            execution_time_ms, planning_time_ms, total_cost,
            rows_estimated, rows_actual,
            has_full_table_scan, has_missing_index, has_sequential_scan,
            trace_id, collected_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
        "#,
    )
    .bind(project_id)
    .bind(query_metric_id)
    .bind(&payload.database_host)
    .bind(&payload.database_name)
    .bind(&payload.query_template)
    .bind(&payload.query_parameters)
    .bind(&payload.explain_plan)
    .bind(payload.execution_time_ms)
    .bind(payload.planning_time_ms)
    .bind(payload.total_cost)
    .bind(payload.rows_estimated)
    .bind(payload.rows_actual)
    .bind(payload.has_full_table_scan.unwrap_or(false))
    .bind(payload.has_missing_index.unwrap_or(false))
    .bind(payload.has_sequential_scan.unwrap_or(false))
    .bind(payload.trace_id)
    .bind(Utc::now())
    .execute(state.db.as_ref())
    .await
    .map_err(|e| {
        error!("[Explain Plan] Failed to store explain plan: {}", e);
        AppError::Internal(anyhow::anyhow!("Failed to store explain plan: {}", e))
    })?;

    info!(
        "[Explain Plan] Stored explain plan for database: {} (project_id: {})",
        payload.database_name, project_id
    );

    Ok(StatusCode::CREATED)
}

#[derive(Debug, Deserialize)]
struct QueryMetricsPayload {
    database_name: String,
    database_host: String,
    database_type: String,
    query_fingerprint: String,
    query_template: String,
    calls: i64,
    total_time_ms: f64,
    mean_time_ms: f64,
    min_time_ms: f64,
    max_time_ms: f64,
    stddev_time_ms: Option<f64>,
    rows_affected: Option<i64>,
    rows_returned: Option<i64>,
    first_seen: chrono::DateTime<chrono::Utc>,
    last_seen: chrono::DateTime<chrono::Utc>,
}

/// Store query metrics in database
/// POST /api/database-monitoring/query-metrics
async fn store_query_metrics(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(payload): Json<QueryMetricsPayload>,
) -> Result<StatusCode> {
    let project_id = crate::api::extract_project_id(&headers)?;

    // Store query metrics (upsert based on unique constraint)
    sqlx::query(
        r#"
        INSERT INTO database_query_metrics (
            project_id, database_host, database_name, database_type,
            query_fingerprint, query_template,
            calls, total_time_ms, mean_time_ms, min_time_ms, max_time_ms, stddev_time_ms,
            rows_affected, rows_returned,
            first_seen, last_seen, collected_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
        ON CONFLICT (project_id, database_host, database_name, query_fingerprint, collected_at)
        DO UPDATE SET
            calls = EXCLUDED.calls,
            total_time_ms = EXCLUDED.total_time_ms,
            mean_time_ms = EXCLUDED.mean_time_ms,
            min_time_ms = EXCLUDED.min_time_ms,
            max_time_ms = EXCLUDED.max_time_ms,
            stddev_time_ms = EXCLUDED.stddev_time_ms,
            rows_affected = EXCLUDED.rows_affected,
            rows_returned = EXCLUDED.rows_returned,
            last_seen = EXCLUDED.last_seen
        "#,
    )
    .bind(project_id)
    .bind(&payload.database_host)
    .bind(&payload.database_name)
    .bind(&payload.database_type)
    .bind(&payload.query_fingerprint)
    .bind(&payload.query_template)
    .bind(payload.calls)
    .bind(payload.total_time_ms)
    .bind(payload.mean_time_ms)
    .bind(payload.min_time_ms)
    .bind(payload.max_time_ms)
    .bind(payload.stddev_time_ms)
    .bind(payload.rows_affected)
    .bind(payload.rows_returned)
    .bind(payload.first_seen)
    .bind(payload.last_seen)
    .bind(Utc::now())
    .execute(state.db.as_ref())
    .await
    .map_err(|e| {
        error!("[Query Metrics] Failed to store query metrics: {}", e);
        AppError::Internal(anyhow::anyhow!("Failed to store query metrics: {}", e))
    })?;

    Ok(StatusCode::CREATED)
}
