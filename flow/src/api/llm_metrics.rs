//! LLM User Metrics API
//!
//! Endpoints for querying per-user and per-model LLM usage analytics.
//! Uses pre-aggregated materialized views for optimal query performance.
//!
//! # SQL Injection Safety
//!
//! This module uses string interpolation for ClickHouse queries. All interpolated
//! values are guaranteed safe by their types:
//!
//! - `project_id`: Strongly-typed `Uuid` - only valid UUID format allowed
//! - `start`/`end` dates: Parsed through `NaiveDate` - guarantees `YYYY-MM-DD` format
//! - `limit`/`offset`: `u32` integers capped with `.min(MAX_LIMIT)` - no string injection possible
//!
//! User-provided string inputs (like `user_id` in session queries) use
//! `escape_clickhouse_string()` from `crate::utils`.

use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use chrono::{NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::app_state::FlowState;
use crate::error::{AppError, Result};

/// Default minimum date for queries with no start date specified
const MIN_DATE: &str = "1970-01-01";
/// Default maximum date for queries with no end date specified
const MAX_DATE: &str = "2100-01-01";
/// Maximum allowed limit for query results to prevent expensive queries
const MAX_LIMIT: u32 = 1000;
/// Default number of days for overview query (reduced from 30 for better performance)
const DEFAULT_OVERVIEW_DAYS: i64 = 7;
/// Maximum number of days allowed for overview query to prevent expensive full-table scans
const MAX_OVERVIEW_DAYS: i64 = 30;

/// Helper function to format date range for ClickHouse queries.
///
/// Returns (start_date, end_date) as formatted strings in `YYYY-MM-DD` format.
///
/// # Safety (SQL Injection)
///
/// The returned strings are safe for direct interpolation in SQL queries because:
/// - When `start`/`end` are `Some(NaiveDate)`, they produce `YYYY-MM-DD` format (no special chars)
/// - When `start`/`end` are `None`, the defaults are hardcoded constants (`MIN_DATE`, `MAX_DATE`)
///   or generated from `Utc::now()` which also produces safe date strings
fn format_date_range(
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
    default_start: &str,
    default_end: &str,
) -> (String, String) {
    let start_str = start
        .map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| default_start.to_string());
    let end_str = end
        .map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| default_end.to_string());
    (start_str, end_str)
}

pub fn create_llm_metrics_router() -> Router<Arc<FlowState>> {
    Router::new()
        .route("/users", get(get_user_metrics))
        .route("/models", get(get_model_metrics))
        .route("/cost/daily", get(get_daily_cost))
        .route("/cost/by-model", get(get_cost_by_model))
        .route("/overview", get(get_overview))
        .route("/provider-latency", get(get_provider_latency))
        .route("/provider-health", get(get_provider_health))
}

/// Query parameters for user metrics
#[derive(Debug, Deserialize)]
pub struct UserMetricsParams {
    pub project_id: Uuid,
    pub start_date: Option<NaiveDate>,
    pub end_date: Option<NaiveDate>,
    #[serde(default = "crate::api::default_list_limit")]
    pub limit: u32,
    #[serde(default)]
    pub offset: u32,
}

/// User metrics response
#[derive(Debug, Serialize)]
pub struct UserMetric {
    pub user_id: String,
    pub request_count: u64,
    pub session_count: u64,
    pub total_input_tokens: u64,
    pub total_output_tokens: u64,
    pub total_cost_usd: Decimal,
    pub error_count: u64,
    pub models: Vec<String>,
}

/// Get per-user metrics
/// Uses pre-aggregated llm_user_metrics_agg table for better performance
async fn get_user_metrics(
    State(state): State<Arc<FlowState>>,
    Query(params): Query<UserMetricsParams>,
) -> Result<Json<Vec<UserMetric>>> {
    let default_start = (Utc::now() - chrono::Duration::days(30))
        .format("%Y-%m-%d")
        .to_string();
    let default_end = Utc::now().format("%Y-%m-%d").to_string();
    let (start, end) = format_date_range(
        params.start_date,
        params.end_date,
        &default_start,
        &default_end,
    );

    // Cap limit to prevent expensive queries
    let limit = params.limit.min(MAX_LIMIT);

    // Use pre-aggregated llm_user_metrics_agg table for better performance
    let query = format!(
        r#"
        SELECT
            user_id,
            sum(request_count) as request_count,
            uniqMerge(session_count) as session_count,
            sum(total_input_tokens) as total_input_tokens,
            sum(total_output_tokens) as total_output_tokens,
            toFloat64(sum(total_cost_usd)) as total_cost_usd,
            sum(error_count) as error_count,
            groupUniqArrayMerge(models) as models
        FROM reiver.llm_user_metrics_agg
        WHERE project_id = '{}'
            AND user_id != ''
            AND date >= toDate('{}')
            AND date <= toDate('{}')
        GROUP BY user_id
        ORDER BY total_cost_usd DESC
        LIMIT {} OFFSET {}
        "#,
        params.project_id, start, end, limit, params.offset
    );

    #[derive(Debug, clickhouse::Row, serde::Deserialize)]
    struct UserRow {
        user_id: String,
        request_count: u64,
        session_count: u64,
        total_input_tokens: u64,
        total_output_tokens: u64,
        total_cost_usd: f64,
        error_count: u64,
        models: Vec<String>,
    }

    let rows: Vec<UserRow> = state
        .clickhouse
        .query(&query)
        .fetch_all()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query error: {}", e)))?;

    let metrics: Vec<UserMetric> = rows
        .into_iter()
        .map(|r| UserMetric {
            user_id: r.user_id,
            request_count: r.request_count,
            session_count: r.session_count,
            total_input_tokens: r.total_input_tokens,
            total_output_tokens: r.total_output_tokens,
            total_cost_usd: Decimal::from_f64_retain(r.total_cost_usd).unwrap_or(Decimal::ZERO),
            error_count: r.error_count,
            models: r.models,
        })
        .collect();

    Ok(Json(metrics))
}

/// Model metrics response
#[derive(Debug, Serialize)]
pub struct ModelMetric {
    pub gen_ai_system: String,
    pub gen_ai_request_model: String,
    pub request_count: u64,
    pub total_input_tokens: u64,
    pub total_output_tokens: u64,
    pub total_cost_usd: Decimal,
    pub avg_latency_ms: f64,
    pub p50_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub error_count: u64,
    pub error_rate: f64,
}

/// Get per-model metrics
/// Uses pre-aggregated llm_model_metrics_agg table for better performance
async fn get_model_metrics(
    State(state): State<Arc<FlowState>>,
    Query(params): Query<UserMetricsParams>,
) -> Result<Json<Vec<ModelMetric>>> {
    let default_start = (Utc::now() - chrono::Duration::days(30))
        .format("%Y-%m-%d")
        .to_string();
    let default_end = Utc::now().format("%Y-%m-%d").to_string();
    let (start, end) = format_date_range(
        params.start_date,
        params.end_date,
        &default_start,
        &default_end,
    );

    // Cap limit to prevent expensive queries
    let limit = params.limit.min(MAX_LIMIT);

    // Use pre-aggregated llm_model_metrics_agg table for better performance.
    // Inner subquery merges aggregate states; outer query derives avg_latency
    // and error_rate from the materialized aliases (avoids ClickHouse 26+
    // "aggregate inside aggregate" error when an alias shadows a column name).
    let query = format!(
        r#"
        SELECT
            gen_ai_system,
            gen_ai_request_model,
            request_count,
            total_input_tokens,
            total_output_tokens,
            total_cost_usd,
            if(total_duration_ms > 0 AND request_count > 0,
               total_duration_ms / request_count,
               p50_latency_ms) as avg_latency_ms,
            p50_latency_ms,
            p95_latency_ms,
            p99_latency_ms,
            error_count,
            if(request_count > 0, error_count / request_count * 100, 0) as error_rate
        FROM (
            SELECT
                gen_ai_system,
                gen_ai_request_model,
                sum(request_count) as request_count,
                sum(total_input_tokens) as total_input_tokens,
                sum(total_output_tokens) as total_output_tokens,
                toFloat64(sum(total_cost_usd)) as total_cost_usd,
                sum(total_duration_ms) as total_duration_ms,
                quantilesMerge(0.5)(duration_quantiles)[1] as p50_latency_ms,
                quantilesMerge(0.95)(duration_quantiles)[1] as p95_latency_ms,
                quantilesMerge(0.99)(duration_quantiles)[1] as p99_latency_ms,
                sum(error_count) as error_count
            FROM reiver.llm_model_metrics_agg
            WHERE project_id = '{}'
                AND hour >= toDateTime('{}')
                AND hour < toDateTime('{}') + INTERVAL 1 DAY
            GROUP BY gen_ai_system, gen_ai_request_model
        )
        ORDER BY total_cost_usd DESC
        LIMIT {} OFFSET {}
        "#,
        params.project_id, start, end, limit, params.offset
    );

    #[derive(Debug, clickhouse::Row, serde::Deserialize)]
    struct ModelRow {
        gen_ai_system: String,
        gen_ai_request_model: String,
        request_count: u64,
        total_input_tokens: u64,
        total_output_tokens: u64,
        total_cost_usd: f64,
        avg_latency_ms: f64,
        p50_latency_ms: f64,
        p95_latency_ms: f64,
        p99_latency_ms: f64,
        error_count: u64,
        error_rate: f64,
    }

    let rows: Vec<ModelRow> = state
        .clickhouse
        .query(&query)
        .fetch_all()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query error: {}", e)))?;

    let metrics: Vec<ModelMetric> = rows
        .into_iter()
        .map(|r| ModelMetric {
            gen_ai_system: r.gen_ai_system,
            gen_ai_request_model: r.gen_ai_request_model,
            request_count: r.request_count,
            total_input_tokens: r.total_input_tokens,
            total_output_tokens: r.total_output_tokens,
            total_cost_usd: Decimal::from_f64_retain(r.total_cost_usd).unwrap_or(Decimal::ZERO),
            avg_latency_ms: r.avg_latency_ms,
            p50_latency_ms: r.p50_latency_ms,
            p95_latency_ms: r.p95_latency_ms,
            p99_latency_ms: r.p99_latency_ms,
            error_count: r.error_count,
            error_rate: r.error_rate,
        })
        .collect();

    Ok(Json(metrics))
}

/// Daily cost response
#[derive(Debug, Serialize)]
pub struct DailyCost {
    pub date: NaiveDate,
    pub request_count: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_cost_usd: Decimal,
}

/// Get daily cost breakdown
/// Uses pre-aggregated llm_cost_daily table for optimal performance
async fn get_daily_cost(
    State(state): State<Arc<FlowState>>,
    Query(params): Query<UserMetricsParams>,
) -> Result<Json<Vec<DailyCost>>> {
    // Default to last 30 days
    let default_start = (Utc::now() - chrono::Duration::days(30))
        .format("%Y-%m-%d")
        .to_string();
    let default_end = Utc::now().format("%Y-%m-%d").to_string();

    let (start, end) = format_date_range(
        params.start_date,
        params.end_date,
        &default_start,
        &default_end,
    );

    let query = format!(
        r#"
        SELECT
            toString(date) as date_str,
            sum(request_count) as request_count,
            sum(input_tokens) as input_tokens,
            sum(output_tokens) as output_tokens,
            toFloat64(sum(total_cost_usd)) as total_cost_usd
        FROM reiver.llm_cost_daily
        WHERE project_id = '{}'
            AND date >= toDate('{}')
            AND date <= toDate('{}')
        GROUP BY date
        ORDER BY date ASC
        "#,
        params.project_id, start, end
    );

    #[derive(Debug, clickhouse::Row, serde::Deserialize)]
    struct DailyRow {
        date_str: String,
        request_count: u64,
        input_tokens: u64,
        output_tokens: u64,
        total_cost_usd: f64,
    }

    let rows: Vec<DailyRow> = state
        .clickhouse
        .query(&query)
        .fetch_all()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query error: {}", e)))?;

    let costs: Vec<DailyCost> = rows
        .into_iter()
        .filter_map(|r| {
            let date = NaiveDate::parse_from_str(&r.date_str, "%Y-%m-%d").ok()?;
            Some(DailyCost {
                date,
                request_count: r.request_count,
                input_tokens: r.input_tokens,
                output_tokens: r.output_tokens,
                total_cost_usd: Decimal::from_f64_retain(r.total_cost_usd).unwrap_or(Decimal::ZERO),
            })
        })
        .collect();

    Ok(Json(costs))
}

/// Cost by model response
#[derive(Debug, Serialize)]
pub struct ModelCost {
    pub gen_ai_system: String,
    pub gen_ai_request_model: String,
    pub request_count: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_cost_usd: Decimal,
    pub percentage: f64,
}

/// Get cost breakdown by model
/// Uses pre-aggregated llm_cost_daily table for better performance
async fn get_cost_by_model(
    State(state): State<Arc<FlowState>>,
    Query(params): Query<UserMetricsParams>,
) -> Result<Json<Vec<ModelCost>>> {
    let (start, end) = format_date_range(params.start_date, params.end_date, MIN_DATE, MAX_DATE);

    // Cap limit to prevent expensive queries
    let limit = params.limit.min(MAX_LIMIT);

    // Use pre-aggregated llm_cost_daily table with window function for percentage.
    // Two-level subquery: inner aggregates, middle adds window sum, outer computes %.
    let query = format!(
        r#"
        SELECT
            gen_ai_system,
            gen_ai_request_model,
            request_count,
            input_tokens,
            output_tokens,
            total_cost_usd,
            if(total_sum > 0, total_cost_usd / total_sum * 100, 0) as percentage
        FROM (
            SELECT
                *,
                sum(total_cost_usd) OVER () as total_sum
            FROM (
                SELECT
                    gen_ai_system,
                    gen_ai_request_model,
                    sum(request_count) as request_count,
                    sum(input_tokens) as input_tokens,
                    sum(output_tokens) as output_tokens,
                    toFloat64(sum(total_cost_usd)) as total_cost_usd
                FROM reiver.llm_cost_daily
                WHERE project_id = '{}'
                    AND date >= toDate('{}')
                    AND date <= toDate('{}')
                GROUP BY gen_ai_system, gen_ai_request_model
            )
        )
        ORDER BY total_cost_usd DESC
        LIMIT {}
        "#,
        params.project_id, start, end, limit
    );

    #[derive(Debug, clickhouse::Row, serde::Deserialize)]
    struct ModelCostRow {
        gen_ai_system: String,
        gen_ai_request_model: String,
        request_count: u64,
        input_tokens: u64,
        output_tokens: u64,
        total_cost_usd: f64,
        percentage: f64,
    }

    let rows: Vec<ModelCostRow> = state
        .clickhouse
        .query(&query)
        .fetch_all()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query error: {}", e)))?;

    let costs: Vec<ModelCost> = rows
        .into_iter()
        .map(|r| ModelCost {
            gen_ai_system: r.gen_ai_system,
            gen_ai_request_model: r.gen_ai_request_model,
            request_count: r.request_count,
            input_tokens: r.input_tokens,
            output_tokens: r.output_tokens,
            total_cost_usd: Decimal::from_f64_retain(r.total_cost_usd).unwrap_or(Decimal::ZERO),
            percentage: r.percentage,
        })
        .collect();

    Ok(Json(costs))
}

/// Overview metrics
#[derive(Debug, Serialize)]
pub struct LlmOverview {
    pub total_requests: u64,
    pub total_sessions: u64,
    pub total_users: u64,
    pub total_input_tokens: u64,
    pub total_output_tokens: u64,
    pub total_cost_usd: Decimal,
    pub error_count: u64,
    pub error_rate: f64,
    pub avg_latency_ms: f64,
    pub top_models: Vec<String>,
    /// Whether the credit system and platform-managed API keys are active.
    pub credits_enabled: bool,
    /// Current credit wallet balance for the organization (USD).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credit_balance_usd: Option<Decimal>,
    /// Total uninvoiced platform fees accrued for BYOK usage (USD).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform_fee_total_usd: Option<Decimal>,
}

#[derive(Debug, Deserialize)]
pub struct OverviewParams {
    pub project_id: Uuid,
    pub start_date: Option<NaiveDate>,
    pub end_date: Option<NaiveDate>,
}

/// Get LLM overview metrics
///
/// Note: Uses raw llm_requests table for top_models and avg_latency which require
/// access to individual records. The date range is limited to MAX_OVERVIEW_DAYS
/// to prevent expensive full-table scans on large datasets.
async fn get_overview(
    State(state): State<Arc<FlowState>>,
    Query(params): Query<OverviewParams>,
) -> Result<Json<LlmOverview>> {
    // Default to last DEFAULT_OVERVIEW_DAYS days
    let default_start = (Utc::now() - chrono::Duration::days(DEFAULT_OVERVIEW_DAYS))
        .format("%Y-%m-%d")
        .to_string();
    let default_end = Utc::now().format("%Y-%m-%d").to_string();

    let (start, end) = format_date_range(
        params.start_date,
        params.end_date,
        &default_start,
        &default_end,
    );

    // Validate date range to prevent expensive queries
    // Parse dates and check the range is within MAX_OVERVIEW_DAYS
    let start_date = NaiveDate::parse_from_str(&start, "%Y-%m-%d")
        .map_err(|_| AppError::Validation("Invalid start date format".to_string()))?;
    let end_date = NaiveDate::parse_from_str(&end, "%Y-%m-%d")
        .map_err(|_| AppError::Validation("Invalid end date format".to_string()))?;

    let date_diff = end_date.signed_duration_since(start_date).num_days();
    if date_diff > MAX_OVERVIEW_DAYS {
        return Err(AppError::Validation(format!(
            "Date range exceeds maximum of {} days for overview query. Use the daily cost endpoint for longer ranges.",
            MAX_OVERVIEW_DAYS
        )));
    }

    // This query requires raw table for topK and avg which can't be merged from aggregated states
    let query = format!(
        r#"
        SELECT
            count() as total_requests,
            uniq(session_id) as total_sessions,
            uniq(user_id) as total_users,
            sum(input_tokens) as total_input_tokens,
            sum(output_tokens) as total_output_tokens,
            toFloat64(sum(cost_usd)) as total_cost_usd,
            countIf(status_code = 'error') as error_count,
            if(count() > 0, countIf(status_code = 'error') / count() * 100, 0) as error_rate,
            avg(duration_ms) as avg_latency_ms,
            topK(5)(gen_ai_request_model) as top_models
        FROM reiver.llm_requests
        WHERE project_id = '{}'
            AND timestamp >= toDateTime64('{} 00:00:00', 9)
            AND timestamp < toDateTime64('{} 00:00:00', 9) + INTERVAL 1 DAY
        "#,
        params.project_id, start, end
    );

    #[derive(Debug, clickhouse::Row, serde::Deserialize)]
    struct OverviewRow {
        total_requests: u64,
        total_sessions: u64,
        total_users: u64,
        total_input_tokens: u64,
        total_output_tokens: u64,
        total_cost_usd: f64,
        error_count: u64,
        error_rate: f64,
        avg_latency_ms: f64,
        top_models: Vec<String>,
    }

    let row: OverviewRow = state
        .clickhouse
        .query(&query)
        .fetch_one()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query error: {}", e)))?;

    // Fetch credit balance and current-month BYOK fees.
    let (credit_balance_usd, platform_fee_total_usd) = if state.credits_enabled {
        match state.get_organization_id(params.project_id).await {
            Ok(Some(org_id)) => {
                let balance: Option<Decimal> = {
                    let cache_key = format!("billing:stripe_credits:{}", org_id);
                    let has_credits = if let Ok(mut conn) = state.redis.get().await {
                        redis::cmd("GET")
                            .arg(&cache_key)
                            .query_async::<Option<String>>(&mut *conn)
                            .await
                            .ok()
                            .flatten()
                            .map(|v| v == "1")
                            .unwrap_or(false)
                    } else {
                        false
                    };
                    if has_credits { Some(Decimal::ONE) } else { None }
                };

                let fee_query = format!(
                    r#"
                    SELECT toFloat64(sum(total_cost_usd)) as total_cost_usd
                    FROM reiver.llm_cost_daily
                    WHERE project_id = '{}'
                      AND date >= toStartOfMonth(today())
                      AND is_platform_key = 0
                    "#,
                    params.project_id.as_hyphenated()
                );

                #[derive(clickhouse::Row, serde::Deserialize)]
                struct CostRow {
                    total_cost_usd: f64,
                }

                let gateway_rate = reiver_core::billing::credits::get_gateway_fee_rate(
                    state.entitlements.as_ref(),
                    org_id,
                )
                .await
                .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to resolve gateway fee rate: {}", e)))?;

                let fees: Option<Decimal> = state
                    .clickhouse
                    .query(&fee_query)
                    .fetch_one::<CostRow>()
                    .await
                    .ok()
                    .and_then(|r| Decimal::from_f64_retain(r.total_cost_usd))
                    .map(|cost| cost * gateway_rate);

                (balance, fees)
            }
            Ok(None) => (None, None),
            Err(e) => {
                tracing::warn!(error = %e, "Failed to resolve org for credit balance");
                (None, None)
            }
        }
    } else {
        (None, None)
    };

    Ok(Json(LlmOverview {
        total_requests: row.total_requests,
        total_sessions: row.total_sessions,
        total_users: row.total_users,
        total_input_tokens: row.total_input_tokens,
        total_output_tokens: row.total_output_tokens,
        total_cost_usd: Decimal::from_f64_retain(row.total_cost_usd).unwrap_or(Decimal::ZERO),
        error_count: row.error_count,
        error_rate: row.error_rate,
        avg_latency_ms: row.avg_latency_ms,
        top_models: row.top_models,
        credits_enabled: state.credits_enabled,
        credit_balance_usd,
        platform_fee_total_usd,
    }))
}

// =============================================================================
// Real-time Provider Latency Metrics
// =============================================================================

/// Real-time latency response for all tracked providers.
#[derive(Debug, Serialize)]
pub struct ProviderLatencyResponse {
    pub providers: Vec<ProviderLatencyEntry>,
}

/// Latency entry for a single provider.
#[derive(Debug, Serialize)]
pub struct ProviderLatencyEntry {
    pub provider: String,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub sample_count: usize,
    pub is_degraded: bool,
}

/// GET /metrics/provider-latency
///
/// Returns real-time latency percentiles per provider from the in-memory
/// latency tracker (not ClickHouse). This is the fast, live view.
async fn get_provider_latency(
    State(state): State<Arc<FlowState>>,
) -> Result<Json<ProviderLatencyResponse>> {
    let summaries = state.latency_tracker.get_all_summaries();

    let providers: Vec<ProviderLatencyEntry> = summaries
        .into_iter()
        .map(|s| ProviderLatencyEntry {
            provider: s.provider,
            p50_ms: s.p50_ms,
            p95_ms: s.p95_ms,
            p99_ms: s.p99_ms,
            sample_count: s.sample_count,
            is_degraded: s.is_degraded,
        })
        .collect();

    Ok(Json(ProviderLatencyResponse { providers }))
}

// =============================================================================
// Provider Health (latency + circuit breaker combined view)
// =============================================================================

/// Combined health status for a single provider.
#[derive(Debug, Serialize)]
pub struct ProviderHealthEntry {
    pub provider: String,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub sample_count: usize,
    pub is_degraded: bool,
    pub circuit_state: String,
    pub circuit_error_rate: f64,
    pub circuit_request_count: usize,
}

#[derive(Debug, Serialize)]
pub struct ProviderHealthResponse {
    pub providers: Vec<ProviderHealthEntry>,
}

/// GET /metrics/provider-health
///
/// Returns combined latency percentiles and circuit breaker state per provider.
async fn get_provider_health(
    State(state): State<Arc<FlowState>>,
) -> Result<Json<ProviderHealthResponse>> {
    let latency_summaries = state.latency_tracker.get_all_summaries();
    let cb_statuses = state.gateway_router.circuit_breaker().get_all_statuses();

    let mut cb_map: std::collections::HashMap<String, _> = cb_statuses
        .into_iter()
        .map(|s| (s.provider.clone(), s))
        .collect();

    let mut providers: Vec<ProviderHealthEntry> = latency_summaries
        .into_iter()
        .map(|s| {
            let cb = cb_map.remove(&s.provider);
            ProviderHealthEntry {
                provider: s.provider,
                p50_ms: s.p50_ms,
                p95_ms: s.p95_ms,
                p99_ms: s.p99_ms,
                sample_count: s.sample_count,
                is_degraded: s.is_degraded,
                circuit_state: cb.as_ref().map_or("closed".into(), |c| c.state.clone()),
                circuit_error_rate: cb.as_ref().map_or(0.0, |c| c.error_rate),
                circuit_request_count: cb.as_ref().map_or(0, |c| c.request_count),
            }
        })
        .collect();

    // Include providers that have circuit breaker data but no latency data
    for (_, cb) in cb_map {
        providers.push(ProviderHealthEntry {
            provider: cb.provider,
            p50_ms: 0.0,
            p95_ms: 0.0,
            p99_ms: 0.0,
            sample_count: 0,
            is_degraded: false,
            circuit_state: cb.state,
            circuit_error_rate: cb.error_rate,
            circuit_request_count: cb.request_count,
        });
    }

    providers.sort_by(|a, b| a.provider.cmp(&b.provider));

    Ok(Json(ProviderHealthResponse { providers }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build the model-metrics query for a dummy project so we can inspect
    /// its SQL text without touching ClickHouse.
    fn build_model_metrics_query() -> String {
        let project_id = Uuid::nil();
        let start = "2024-01-01".to_string();
        let end = "2024-01-31".to_string();
        let limit: u32 = 100;
        let offset: u32 = 0;

        format!(
            r#"
        SELECT
            gen_ai_system,
            gen_ai_request_model,
            request_count,
            total_input_tokens,
            total_output_tokens,
            total_cost_usd,
            if(total_duration_ms > 0 AND request_count > 0,
               total_duration_ms / request_count,
               p50_latency_ms) as avg_latency_ms,
            p50_latency_ms,
            p95_latency_ms,
            p99_latency_ms,
            error_count,
            if(request_count > 0, error_count / request_count * 100, 0) as error_rate
        FROM (
            SELECT
                gen_ai_system,
                gen_ai_request_model,
                sum(request_count) as request_count,
                sum(total_input_tokens) as total_input_tokens,
                sum(total_output_tokens) as total_output_tokens,
                toFloat64(sum(total_cost_usd)) as total_cost_usd,
                sum(total_duration_ms) as total_duration_ms,
                quantilesMerge(0.5)(duration_quantiles)[1] as p50_latency_ms,
                quantilesMerge(0.95)(duration_quantiles)[1] as p95_latency_ms,
                quantilesMerge(0.99)(duration_quantiles)[1] as p99_latency_ms,
                sum(error_count) as error_count
            FROM reiver.llm_model_metrics_agg
            WHERE project_id = '{}'
                AND hour >= toDateTime('{}')
                AND hour < toDateTime('{}') + INTERVAL 1 DAY
            GROUP BY gen_ai_system, gen_ai_request_model
        )
        ORDER BY total_cost_usd DESC
        LIMIT {} OFFSET {}
        "#,
            project_id, start, end, limit, offset
        )
    }

    /// Regression: `avg_latency_ms` previously used `quantilesMerge(0.5)`
    /// which returns the MEDIAN (P50), not the average. The query must use
    /// `total_duration_ms / request_count` for the true average.
    #[test]
    fn test_model_metrics_query_uses_true_average() {
        let query = build_model_metrics_query();

        assert!(
            query.contains("total_duration_ms / request_count"),
            "avg_latency_ms must divide total duration by request count"
        );
    }

    /// The query must still fall back to P50 for rows that predate the V9
    /// migration (where total_duration_ms is 0).
    #[test]
    fn test_model_metrics_query_falls_back_to_p50_for_legacy_data() {
        let query = build_model_metrics_query();

        assert!(
            query.contains("if(total_duration_ms > 0"),
            "query must check whether total_duration_ms data exists before using it"
        );
        assert!(
            query.contains("p50_latency_ms) as avg_latency_ms"),
            "query must fall back to P50 when total_duration_ms is 0"
        );
    }

    #[test]
    fn test_format_date_range_defaults() {
        let (start, end) = format_date_range(None, None, MIN_DATE, MAX_DATE);
        assert_eq!(start, MIN_DATE);
        assert_eq!(end, MAX_DATE);
    }

    #[test]
    fn test_format_date_range_with_values() {
        let s = NaiveDate::from_ymd_opt(2024, 3, 1).unwrap();
        let e = NaiveDate::from_ymd_opt(2024, 3, 31).unwrap();
        let (start, end) = format_date_range(Some(s), Some(e), MIN_DATE, MAX_DATE);
        assert_eq!(start, "2024-03-01");
        assert_eq!(end, "2024-03-31");
    }
}
