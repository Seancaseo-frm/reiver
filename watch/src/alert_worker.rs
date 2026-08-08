//! Alert evaluation worker — evaluation-based threshold alerts only.
//!
//! This worker handles alerts that require periodic ClickHouse polling and
//! threshold comparison (e.g. error rate > X, latency p99 > Y). It uses a
//! state machine (OK ↔ ALERT) with built-in dedup (only notifies on
//! transitions).
//!
//! All other event-driven notifications (provider key errors, exception
//! alerts, rollout rollbacks, investigation findings) are handled by the
//! event worker via Kafka — see `event_worker.rs`.
//!
//! This worker:
//! 1. Runs every minute via tokio interval
//! 2. Loads enabled alert rules that are due for evaluation
//! 3. Queries ClickHouse for aggregated metric/log data
//! 4. Compares values against thresholds
//! 5. Updates alert states (OK ↔ ALERT)
//! 6. Sends notifications on state transitions
//! 7. Records evaluation history

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use reiver_core::events::{EventPublisher, PlatformEventType};
use serde_json::Value;
use sqlx::{types::Json, Row};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::alerts::{
    load_notification_channel, send_notification, validate_aggregation_function,
    AlertNotification, AlertQueryConfig, AlertRule, AlertState,
};
use crate::clickhouse_db::ClickHousePool;
use crate::db::DbPool;
use crate::maintenance::is_project_in_maintenance;
use crate::utils::escape_clickhouse_string;

/// Start the alert evaluation worker
/// Polls every 60 seconds and evaluates all due alerts
///
/// # Arguments
/// * `db_pool` - Database connection pool
/// * `clickhouse_pool` - ClickHouse connection pool
/// * `shutdown_rx` - Shutdown signal receiver for graceful shutdown
pub async fn start_alert_worker(
    db_pool: Arc<DbPool>,
    clickhouse_pool: Arc<ClickHousePool>,
    event_publisher: Arc<EventPublisher>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<JoinHandle<()>> {
    info!("Starting alert evaluation worker (polling every 60 seconds)");

    let mut interval = time::interval(time::Duration::from_secs(60));

    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let now = Utc::now();
                    debug!("Alert worker tick at {}", now);

                    if let Err(e) = evaluate_alerts(&db_pool, &clickhouse_pool, &event_publisher).await {
                        error!("Alert evaluation failed: {}", e);
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Alert worker received shutdown signal, stopping gracefully");
                        break;
                    }
                }
            }
        }
        info!("Alert worker stopped");
    });

    Ok(handle)
}

/// Main evaluation function - loads and evaluates all due alerts
async fn evaluate_alerts(
    db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
    event_publisher: &EventPublisher,
) -> Result<()> {
    // Load enabled alerts that are due for evaluation
    let rules = load_alerts_due_for_evaluation(db_pool).await?;

    if rules.is_empty() {
        debug!("No alerts due for evaluation");
        return Ok(());
    }

    info!("Evaluating {} alert rule(s)", rules.len());

    // Evaluate each rule
    let mut success_count = 0;
    let mut error_count = 0;
    let mut skipped_maintenance = 0;

    for rule in rules {
        // Skip if project is in maintenance window
        match is_project_in_maintenance(db_pool, rule.project_id).await {
            Ok(true) => {
                debug!(
                    "Project {} is in maintenance window, skipping alert rule '{}'",
                    rule.project_id, rule.name
                );
                skipped_maintenance += 1;
                continue;
            }
            Err(e) => {
                warn!(
                    "Failed to check maintenance status for project {}: {}",
                    rule.project_id, e
                );
                // Continue with evaluation if maintenance check fails
            }
            _ => {}
        }

        match evaluate_rule(&rule, db_pool, clickhouse_pool, event_publisher).await {
            Ok(_) => success_count += 1,
            Err(e) => {
                error!(
                    "Failed to evaluate rule '{}' ({}): {}",
                    rule.name, rule.id, e
                );
                error_count += 1;
                // Still update last_evaluated_at to prevent infinite retry loops
                if let Err(update_err) = update_rule_last_evaluated(db_pool, rule.id).await {
                    error!(
                        "Failed to update last_evaluated_at for rule '{}': {}",
                        rule.name, update_err
                    );
                }
            }
        }
    }

    if skipped_maintenance > 0 {
        info!(
            "Alert evaluation complete: {} succeeded, {} failed, {} skipped (maintenance)",
            success_count, error_count, skipped_maintenance
        );
    } else {
        info!(
            "Alert evaluation complete: {} succeeded, {} failed",
            success_count, error_count
        );
    }

    Ok(())
}

/// Load alert rules that are due for evaluation
async fn load_alerts_due_for_evaluation(db_pool: &DbPool) -> Result<Vec<AlertRule>> {
    let now = Utc::now();

    let rows = sqlx::query(
        r#"
        SELECT 
            ar.id, ar.project_id, ar.name, ar.description, ar.rule_type, ar.query_config,
            ar.threshold, ar.threshold_type, ar.notification_channels,
            ar.alert_on_absent, ar.absent_for_seconds,
            ar.eval_window_seconds, ar.eval_interval_seconds,
            ar.labels, ar.annotations, ar.enabled, ar.last_evaluated_at,
            ar.created_at, ar.updated_at
        FROM alert_rules ar
        WHERE ar.enabled = true
          AND (
            ar.last_evaluated_at IS NULL 
            OR ar.last_evaluated_at <= $1
          )
        ORDER BY ar.last_evaluated_at ASC NULLS FIRST
        "#,
    )
    .bind(now - Duration::seconds(60)) // Evaluate if last check was more than 60 seconds ago
    .fetch_all(db_pool)
    .await
    .context("Failed to load alert rules")?;

    let mut rules = Vec::new();

    for row in rows {
        let query_config: Value = row.get("query_config");
        let labels: Value = row.get("labels");
        let annotations: Value = row.get("annotations");

        // rule_type is VARCHAR, not JSONB
        let rule_type_str: String = row.get("rule_type");
        let rule_type = match rule_type_str.as_str() {
            "threshold" => crate::alerts::RuleType::Threshold,
            _ => crate::alerts::RuleType::Threshold,
        };

        let rule = AlertRule {
            id: row.get("id"),
            project_id: row.get("project_id"),
            name: row.get("name"),
            description: row.get("description"),
            rule_type,
            query_config: serde_json::from_value(query_config).unwrap_or_default(),
            threshold: row.get("threshold"),
            threshold_type: row.get("threshold_type"),
            notification_channels: row.get("notification_channels"),
            alert_on_absent: row.get("alert_on_absent"),
            absent_for_seconds: row.get("absent_for_seconds"),
            eval_window_seconds: row.get("eval_window_seconds"),
            eval_interval_seconds: row.get("eval_interval_seconds"),
            labels: serde_json::from_value(labels).unwrap_or_default(),
            annotations: serde_json::from_value(annotations).unwrap_or_default(),
            enabled: row.get("enabled"),
            last_evaluated_at: row.get("last_evaluated_at"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        };

        rules.push(rule);
    }

    Ok(rules)
}

/// Evaluate a single alert rule
async fn evaluate_rule(
    rule: &AlertRule,
    db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
    event_publisher: &EventPublisher,
) -> Result<()> {
    info!(
        "Evaluating rule '{}' ({}) - metric={:?}",
        rule.name, rule.id, rule.query_config.metric_name()
    );

    // 1. Query ClickHouse for the metric value
    let value = query_metric_value(rule, clickhouse_pool).await?;

    info!(
        "Rule '{}': value={}, threshold={}, type={}",
        rule.name, value, rule.threshold, rule.threshold_type
    );

    // 2. Check if threshold is exceeded or data is absent
    let threshold_exceeded = check_threshold(value, rule.threshold, &rule.threshold_type);

    let data_absent = rule.alert_on_absent && value == 0.0 && rule.absent_for_seconds > 0;

    // 3. Get or create alert record
    let mut alert = get_or_create_alert(db_pool, rule).await?;

    // 4. Auto-reset: If alert has been in ALERT state for longer than eval_window_seconds,
    //    reset to OK so it can fire again. This allows periodic notifications for ongoing issues.
    let now = Utc::now();
    let eval_window = Duration::seconds(rule.eval_window_seconds as i64);

    if alert.state == "ALERT" && (now - alert.state_changed_at) > eval_window {
        info!(
            "Rule '{}': Auto-resetting from ALERT to OK (been in ALERT for {:?}, window is {:?})",
            rule.name,
            now - alert.state_changed_at,
            eval_window
        );

        // Reset state to OK in database
        reset_alert_state(db_pool, alert.id).await?;
        alert.state = "OK".to_string();
        alert.state_changed_at = now;
    }

    // 5. Determine current state (stored as string in DB)
    let current_state = alert.state.as_str();
    let absent_duration_exceeded = data_absent
        && (now - alert.checked_at) >= Duration::seconds(rule.absent_for_seconds as i64);
    let new_state = if threshold_exceeded || absent_duration_exceeded {
        "ALERT"
    } else {
        "OK"
    };

    if absent_duration_exceeded {
        info!(
            "Rule '{}': Data absent for >= {}s, triggering alert",
            rule.name, rule.absent_for_seconds
        );
    }

    // 6. Handle state transitions
    let state_changed = current_state != new_state;

    if state_changed {
        info!(
            "Rule '{}': State transition {} -> {}",
            rule.name, current_state, new_state
        );

        // Update alert state in database (also updates state_changed_at)
        update_alert_state(db_pool, alert.id, new_state, value).await?;

        // Send notifications
        send_alert_notifications(
            db_pool,
            rule,
            alert.id,
            new_state,
            value,
            absent_duration_exceeded,
        )
        .await?;

        // Emit platform event for the subscription system
        let event_type = if new_state == "ALERT" {
            PlatformEventType::AlertFired
        } else {
            PlatformEventType::AlertResolved
        };
        if let Err(e) = event_publisher
            .emit(
                event_type,
                rule.project_id,
                format!("alert:{}:{}", rule.id, alert.id),
                serde_json::json!({
                    "rule_id": rule.id,
                    "rule_name": rule.name,
                    "alert_id": alert.id,
                    "value": value,
                    "threshold": rule.threshold,
                    "threshold_type": rule.threshold_type,
                    "new_state": new_state,
                    "is_absent": absent_duration_exceeded,
                }),
            )
            .await
        {
            warn!("Failed to emit alert event: {}", e);
        }

        // Trigger MooDeng auto-investigation on ALERT transitions
        if new_state == "ALERT" {
            trigger_alert_investigation(db_pool, rule, alert.id, value, absent_duration_exceeded)
                .await;
        }

        // Record history with notification flag
        record_history(db_pool, alert.id, rule.id, new_state, value, true).await?;
    } else {
        // No state change, just record the check
        debug!(
            "Rule '{}': No state change (still {})",
            rule.name, current_state
        );
        record_history(db_pool, alert.id, rule.id, new_state, value, false).await?;
    }

    // 7. Update checked_at on alert
    update_alert_checked_at(db_pool, alert.id).await?;

    // 8. Update last_evaluated_at on rule
    update_rule_last_evaluated(db_pool, rule.id).await?;

    Ok(())
}

/// Query ClickHouse for the metric value based on alert configuration.
async fn query_metric_value(rule: &AlertRule, clickhouse_pool: &ClickHousePool) -> Result<f64> {
    match &rule.query_config {
        AlertQueryConfig::PromQL { promql } => query_promql_value(rule, promql).await,
        AlertQueryConfig::Llm { .. } => query_llm_metric(rule, clickhouse_pool).await,
        AlertQueryConfig::Metrics { .. } => query_metric_aggregation(rule, clickhouse_pool).await,
        AlertQueryConfig::LogPattern { .. } => query_log_pattern_count(rule, clickhouse_pool).await,
    }
}

/// Evaluate a PromQL expression and return the aggregated scalar value.
///
/// Runs an instant query at `now` with lookback of `eval_window_seconds`.
/// If the query returns multiple series, uses the maximum value for threshold
/// comparison (per-series alerting can be added later).
async fn query_promql_value(rule: &AlertRule, promql: &str) -> Result<f64> {
    let clickhouse_url =
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());
    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());

    let evaluator = crate::promql_provider::PromQLEvaluator::new(http_client, clickhouse_url);

    let now_ms = Utc::now().timestamp_millis();
    let window_ms = (rule.eval_window_seconds as i64) * 1000;
    let start_ms = now_ms - window_ms;
    let step_ms = window_ms;

    debug!(
        rule_id = %rule.id,
        promql = %promql,
        "Evaluating PromQL alert (instant query)"
    );

    let (batches, _otel_map) = evaluator
        .execute(promql, &rule.project_id, start_ms, now_ms, step_ms, true)
        .await
        .map_err(|e| anyhow::anyhow!("PromQL evaluation failed for rule '{}': {}", rule.name, e))?;

    use arrow_array::Array;

    let mut max_value: f64 = 0.0;
    let mut found_any = false;

    for batch in &batches {
        let value_col = batch
            .column_by_name("value")
            .context("PromQL result missing 'value' column")?;
        let values = value_col
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .context("PromQL 'value' column is not Float64")?;

        for i in 0..values.len() {
            if !values.is_null(i) {
                let v = values.value(i);
                if !v.is_nan() {
                    if !found_any || v > max_value {
                        max_value = v;
                    }
                    found_any = true;
                }
            }
        }
    }

    if !found_any {
        debug!(
            rule_id = %rule.id,
            "PromQL query returned no data points"
        );
    } else {
        debug!(
            rule_id = %rule.id,
            value = max_value,
            "PromQL query result"
        );
    }

    Ok(max_value)
}

/// Query log pattern match count
/// Uses the OTel-compatible logs table with body column
async fn query_log_pattern_count(
    rule: &AlertRule,
    clickhouse_pool: &ClickHousePool,
) -> Result<f64> {
    let patterns = match &rule.query_config {
        AlertQueryConfig::LogPattern { patterns, .. } => patterns,
        _ => anyhow::bail!("query_log_pattern_count called with non-LogPattern config"),
    };

    if patterns.is_empty() {
        return Ok(0.0);
    }

    // Build pattern conditions for the body column
    let mut pattern_conditions = Vec::new();
    for pattern in patterns {
        let escaped = escape_clickhouse_string(pattern);
        if pattern.contains('*') {
            let like_pattern = escaped.replace('*', "%");
            pattern_conditions.push(format!("body LIKE '{}'", like_pattern));
        } else {
            pattern_conditions.push(format!("position(body, '{}') > 0", escaped));
        }
    }
    let pattern_clause = pattern_conditions.join(" OR ");

    let query = format!(
        r#"
        SELECT count(*) as match_count
        FROM reiver.logs
        WHERE project_id = '{}'
          AND timestamp >= now() - INTERVAL {} SECOND
          AND ({})
        "#,
        rule.project_id, rule.eval_window_seconds, pattern_clause
    );

    debug!("Executing log pattern query: {}", query);

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct CountRow {
        match_count: u64,
    }

    let mut cursor = clickhouse_pool.as_ref().query(&query).fetch::<CountRow>()?;
    let total_count = if let Some(row) = cursor.next().await? {
        debug!("Log query returned match_count={}", row.match_count);
        row.match_count
    } else {
        debug!("Log query returned no rows");
        0
    };

    debug!(
        "Rule '{}': log pattern matched {} times",
        rule.name, total_count
    );
    Ok(total_count as f64)
}

/// Query metric aggregation
async fn query_metric_aggregation(
    rule: &AlertRule,
    clickhouse_pool: &ClickHousePool,
) -> Result<f64> {
    let (metric_name, filters, time_agg, space_agg) = match &rule.query_config {
        AlertQueryConfig::Metrics {
            metric_name,
            filters,
            time_aggregation,
            space_aggregation,
            ..
        } => (
            metric_name.as_str(),
            filters,
            time_aggregation.as_str(),
            space_aggregation.as_str(),
        ),
        _ => anyhow::bail!("query_metric_aggregation called with non-Metrics config"),
    };

    validate_aggregation_function(time_agg).context("Invalid time_aggregation function")?;
    validate_aggregation_function(space_agg).context("Invalid space_aggregation function")?;

    let mut filter_clauses = vec![
        format!("project_id = '{}'", rule.project_id),
        format!("metric_name = '{}'", escape_clickhouse_string(metric_name)),
        format!(
            "timestamp >= now() - INTERVAL {} SECOND",
            rule.eval_window_seconds
        ),
    ];

    for (key, value) in filters {
        filter_clauses.push(format!(
            "labels['{}'] = '{}'",
            escape_clickhouse_string(key),
            escape_clickhouse_string(value)
        ));
    }

    let where_clause = filter_clauses.join(" AND ");

    // Build aggregation query
    let query = format!(
        r#"
        SELECT {}({}(value)) as aggregated_value
        FROM metrics
        WHERE {}
        "#,
        space_agg, time_agg, where_clause
    );

    debug!("Executing metric query: {}", query);

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct ValueRow {
        aggregated_value: f64,
    }

    let mut cursor = clickhouse_pool.as_ref().query(&query).fetch::<ValueRow>()?;

    if let Some(row) = cursor.next().await? {
        Ok(row.aggregated_value)
    } else {
        Ok(0.0)
    }
}

/// Query LLM-specific metrics from the `llm_requests` table.
///
/// The metric is derived from `metric_name` by stripping the `llm.` prefix.
/// Model filtering uses `filters["model"]`.
async fn query_llm_metric(rule: &AlertRule, clickhouse_pool: &ClickHousePool) -> Result<f64> {
    let (metric_name, filters) = match &rule.query_config {
        AlertQueryConfig::Llm {
            metric_name,
            filters,
            ..
        } => (metric_name.as_str(), filters),
        _ => anyhow::bail!("query_llm_metric called with non-Llm config"),
    };

    let llm_metric = metric_name
        .strip_prefix("llm.")
        .context("LLM alert metric_name must start with 'llm.'")?;

    let model_filter = match filters.get("model") {
        Some(model) => format!(
            " AND gen_ai_request_model = '{}'",
            escape_clickhouse_string(model)
        ),
        None => String::new(),
    };

    let query = match llm_metric {
        "error_rate" => {
            // Calculate error rate as percentage
            format!(
                r#"
                SELECT 
                    if(count() > 0, countIf(status_code = 'error') * 100.0 / count(), 0) as metric_value
                FROM reiver.llm_requests
                WHERE project_id = '{}'
                  AND timestamp >= now() - INTERVAL {} SECOND
                  {}
                "#,
                rule.project_id, rule.eval_window_seconds, model_filter
            )
        }
        "latency_p95" => {
            // Get P95 latency in milliseconds
            format!(
                r#"
                SELECT quantile(0.95)(duration_ms) as metric_value
                FROM reiver.llm_requests
                WHERE project_id = '{}'
                  AND timestamp >= now() - INTERVAL {} SECOND
                  AND status_code = 'ok'
                  {}
                "#,
                rule.project_id, rule.eval_window_seconds, model_filter
            )
        }
        "latency_avg" => {
            // Get average latency in milliseconds
            format!(
                r#"
                SELECT avg(duration_ms) as metric_value
                FROM reiver.llm_requests
                WHERE project_id = '{}'
                  AND timestamp >= now() - INTERVAL {} SECOND
                  AND status_code = 'ok'
                  {}
                "#,
                rule.project_id, rule.eval_window_seconds, model_filter
            )
        }
        "cost_daily" => {
            // Get total cost for today
            format!(
                r#"
                SELECT sum(cost_usd) as metric_value
                FROM reiver.llm_requests
                WHERE project_id = '{}'
                  AND toDate(timestamp) = today()
                  {}
                "#,
                rule.project_id, model_filter
            )
        }
        "token_usage" => {
            // Get total tokens used in window
            format!(
                r#"
                SELECT sum(total_tokens) as metric_value
                FROM reiver.llm_requests
                WHERE project_id = '{}'
                  AND timestamp >= now() - INTERVAL {} SECOND
                  {}
                "#,
                rule.project_id, rule.eval_window_seconds, model_filter
            )
        }
        "request_count" => {
            // Get request count in window
            format!(
                r#"
                SELECT count() as metric_value
                FROM reiver.llm_requests
                WHERE project_id = '{}'
                  AND timestamp >= now() - INTERVAL {} SECOND
                  {}
                "#,
                rule.project_id, rule.eval_window_seconds, model_filter
            )
        }
        _ => {
            warn!("Unknown LLM metric type: {}", llm_metric);
            return Ok(0.0);
        }
    };

    debug!("Executing LLM metric query: {}", query);

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct ValueRow {
        metric_value: f64,
    }

    let mut cursor = clickhouse_pool.as_ref().query(&query).fetch::<ValueRow>()?;

    if let Some(row) = cursor.next().await? {
        Ok(row.metric_value)
    } else {
        Ok(0.0)
    }
}

/// Check if value exceeds threshold based on threshold type
fn check_threshold(value: f64, threshold: f64, threshold_type: &str) -> bool {
    match threshold_type {
        "above" => value > threshold,
        "below" => value < threshold,
        "above_or_equal" => value >= threshold,
        "below_or_equal" => value <= threshold,
        _ => {
            warn!(
                "Unknown threshold type '{}', defaulting to 'above'",
                threshold_type
            );
            value > threshold
        }
    }
}

/// Get or create alert record for a rule
/// Returns the existing alert or creates a new one
async fn get_or_create_alert(db_pool: &DbPool, rule: &AlertRule) -> Result<Alert> {
    // Use rule ID as fingerprint for single-threshold alerts
    let fingerprint = rule.id.to_string();

    // Try to get existing alert
    let existing = sqlx::query_as::<_, Alert>(
        r#"
        SELECT id, rule_id, fingerprint, labels, annotations, state, value, state_changed_at, checked_at, created_at
        FROM alerts
        WHERE rule_id = $1 AND fingerprint = $2
        "#
    )
    .bind(rule.id)
    .bind(&fingerprint)
    .fetch_optional(db_pool)
    .await?;

    if let Some(alert) = existing {
        return Ok(alert);
    }

    // Create new alert
    let alert_id = Uuid::new_v4();
    let now = Utc::now();

    sqlx::query(
        r#"
        INSERT INTO alerts (id, rule_id, fingerprint, labels, annotations, state, value, state_changed_at, checked_at, created_at)
        VALUES ($1, $2, $3, $4, $5, 'OK', NULL, $6, $6, $6)
        "#
    )
    .bind(alert_id)
    .bind(rule.id)
    .bind(&fingerprint)
    .bind(Json(&rule.labels))
    .bind(Json(&rule.annotations))
    .bind(now)
    .execute(db_pool)
    .await?;

    info!("Created new alert {} for rule '{}'", alert_id, rule.name);

    Ok(Alert {
        id: alert_id,
        rule_id: rule.id,
        fingerprint,
        labels: serde_json::to_value(&rule.labels)?,
        annotations: serde_json::to_value(&rule.annotations)?,
        state: "OK".to_string(),
        value: None,
        state_changed_at: now,
        checked_at: now,
        created_at: now,
    })
}

/// Update alert state in database
async fn update_alert_state(
    db_pool: &DbPool,
    alert_id: Uuid,
    state: &str,
    value: f64,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE alerts
        SET state = $1, value = $2, state_changed_at = NOW(), checked_at = NOW()
        WHERE id = $3
        "#,
    )
    .bind(state)
    .bind(value)
    .bind(alert_id)
    .execute(db_pool)
    .await?;

    Ok(())
}

/// Reset alert state to OK (for auto-reset after eval_window expires)
async fn reset_alert_state(db_pool: &DbPool, alert_id: Uuid) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE alerts
        SET state = 'OK', state_changed_at = NOW()
        WHERE id = $1
        "#,
    )
    .bind(alert_id)
    .execute(db_pool)
    .await?;

    Ok(())
}

/// Update alert checked_at timestamp
async fn update_alert_checked_at(db_pool: &DbPool, alert_id: Uuid) -> Result<()> {
    sqlx::query("UPDATE alerts SET checked_at = NOW() WHERE id = $1")
        .bind(alert_id)
        .execute(db_pool)
        .await?;

    Ok(())
}

/// Update rule last_evaluated_at timestamp
async fn update_rule_last_evaluated(db_pool: &DbPool, rule_id: Uuid) -> Result<()> {
    sqlx::query("UPDATE alert_rules SET last_evaluated_at = NOW() WHERE id = $1")
        .bind(rule_id)
        .execute(db_pool)
        .await?;

    Ok(())
}

/// Send alert notifications for state transitions
async fn send_alert_notifications(
    db_pool: &DbPool,
    rule: &AlertRule,
    alert_id: Uuid,
    state: &str,
    value: f64,
    is_absent: bool,
) -> Result<()> {
    let channels = &rule.notification_channels;

    if channels.is_empty() {
        debug!(
            "No notification channels configured for rule '{}'",
            rule.name
        );
        return Ok(());
    }

    let alert_state = match state {
        "ALERT" => AlertState::Firing,
        "OK" => AlertState::Ok,
        _ => AlertState::Ok,
    };

    let notification = AlertNotification {
        alert_id,
        rule_id: rule.id,
        rule_name: rule.name.clone(),
        state: alert_state,
        value: Some(value),
        threshold: Some(rule.threshold),
        compare_op: rule.threshold_type.clone(),
        labels: rule.labels.clone(),
        annotations: rule.annotations.clone(),
        fired_at: if state == "ALERT" {
            Some(Utc::now())
        } else {
            None
        },
        resolved_at: if state == "OK" {
            Some(Utc::now())
        } else {
            None
        },
        is_missing: is_absent,
    };

    let mut success_count = 0;
    let mut failure_count = 0;

    for channel_id in channels {
        match load_notification_channel(db_pool, *channel_id).await {
            Ok(Some(channel)) => match send_notification(&channel, &notification).await {
                Ok(_) => {
                    info!(
                        "    ✅ Sent notification to {:?} channel {}",
                        channel.channel_type, channel_id
                    );
                    success_count += 1;
                }
                Err(e) => {
                    error!(
                        "    ❌ Failed to send notification to channel {}: {}",
                        channel_id, e
                    );
                    failure_count += 1;
                }
            },
            Ok(None) => {
                warn!("    ⚠️  Notification channel {} not found", channel_id);
                failure_count += 1;
            }
            Err(e) => {
                error!(
                    "    ❌ Failed to load notification channel {}: {}",
                    channel_id, e
                );
                failure_count += 1;
            }
        }
    }

    if success_count > 0 || failure_count > 0 {
        info!(
            "📬 [NOTIFICATIONS] {} sent, {} failed for alert {}",
            success_count, failure_count, alert_id
        );
    }

    Ok(())
}

/// Record alert evaluation in history
async fn record_history(
    db_pool: &DbPool,
    alert_id: Uuid,
    rule_id: Uuid,
    state: &str,
    value: f64,
    notification_sent: bool,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO alert_history (alert_id, rule_id, state, value, notification_sent, checked_at)
        VALUES ($1, $2, $3, $4, $5, NOW())
        "#,
    )
    .bind(alert_id)
    .bind(rule_id)
    .bind(state)
    .bind(value)
    .bind(notification_sent)
    .execute(db_pool)
    .await?;

    Ok(())
}

/// Alert struct for database queries
#[derive(Debug, Clone)]
#[allow(dead_code)] // Some fields included in SELECT for future alert history features
struct Alert {
    id: Uuid,
    rule_id: Uuid,
    fingerprint: String,
    labels: Value,
    annotations: Value,
    state: String,
    value: Option<f64>,
    state_changed_at: DateTime<Utc>,
    checked_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
}

impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for Alert {
    fn from_row(row: &sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Alert {
            id: row.try_get("id")?,
            rule_id: row.try_get("rule_id")?,
            fingerprint: row.try_get("fingerprint")?,
            labels: row.try_get("labels")?,
            annotations: row.try_get("annotations")?,
            state: row.try_get("state")?,
            value: row.try_get("value")?,
            state_changed_at: row.try_get("state_changed_at")?,
            checked_at: row.try_get("checked_at")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// MooDeng auto-investigation trigger
// ═══════════════════════════════════════════════════════════════════════════

/// Fire-and-forget: check if auto_investigate is enabled, then POST to Flow.
async fn trigger_alert_investigation(
    db_pool: &DbPool,
    rule: &AlertRule,
    alert_id: Uuid,
    value: f64,
    is_absent: bool,
) {
    let project_id = rule.project_id;

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

    let channel_ids: Vec<Uuid> = rule.notification_channels.clone();

    let trigger_summary = if is_absent {
        format!("Alert '{}' fired: data absent", rule.name,)
    } else {
        format!(
            "Alert '{}' fired: value {:.2} {} threshold {:.2}",
            rule.name, value, rule.threshold_type, rule.threshold,
        )
    };

    let trigger_context = serde_json::json!({
        "alert_id": alert_id,
        "rule_id": rule.id,
        "rule_name": rule.name,
        "description": rule.description,
        "value": value,
        "threshold": rule.threshold,
        "threshold_type": rule.threshold_type,
        "is_absent": is_absent,
        "query_config": serde_json::to_value(&rule.query_config).unwrap_or_default(),
        "labels": rule.labels,
        "annotations": rule.annotations,
    });

    let payload = serde_json::json!({
        "project_id": project_id,
        "trigger_type": "alert",
        "trigger_ref": rule.id.to_string(),
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
            info!(
                "Triggered auto-investigation for alert rule '{}'",
                rule.name
            );
        }
        Ok(resp) => {
            debug!(
                "Auto-investigation request returned {}: rule '{}'",
                resp.status(),
                rule.name,
            );
        }
        Err(e) => {
            warn!(
                "Failed to trigger auto-investigation for rule '{}': {}",
                rule.name, e
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Threshold Check Tests
    // ========================================================================

    #[test]
    fn test_check_threshold_above_exceeded() {
        assert!(check_threshold(100.0, 50.0, "above"));
    }

    #[test]
    fn test_check_threshold_above_not_exceeded() {
        assert!(!check_threshold(50.0, 100.0, "above"));
    }

    #[test]
    fn test_check_threshold_above_equal_not_exceeded() {
        // Equal should NOT trigger for "above"
        assert!(!check_threshold(50.0, 50.0, "above"));
    }

    #[test]
    fn test_check_threshold_below_exceeded() {
        assert!(check_threshold(50.0, 100.0, "below"));
    }

    #[test]
    fn test_check_threshold_below_not_exceeded() {
        assert!(!check_threshold(100.0, 50.0, "below"));
    }

    #[test]
    fn test_check_threshold_below_equal_not_exceeded() {
        // Equal should NOT trigger for "below"
        assert!(!check_threshold(50.0, 50.0, "below"));
    }

    #[test]
    fn test_check_threshold_above_or_equal_exceeded() {
        assert!(check_threshold(100.0, 50.0, "above_or_equal"));
        assert!(check_threshold(50.0, 50.0, "above_or_equal")); // Equal should trigger
    }

    #[test]
    fn test_check_threshold_above_or_equal_not_exceeded() {
        assert!(!check_threshold(49.0, 50.0, "above_or_equal"));
    }

    #[test]
    fn test_check_threshold_below_or_equal_exceeded() {
        assert!(check_threshold(50.0, 100.0, "below_or_equal"));
        assert!(check_threshold(50.0, 50.0, "below_or_equal")); // Equal should trigger
    }

    #[test]
    fn test_check_threshold_below_or_equal_not_exceeded() {
        assert!(!check_threshold(51.0, 50.0, "below_or_equal"));
    }

    #[test]
    fn test_check_threshold_unknown_type_defaults_to_above() {
        // Unknown threshold type should default to "above"
        assert!(check_threshold(100.0, 50.0, "unknown"));
        assert!(!check_threshold(50.0, 100.0, "unknown"));
    }

    #[test]
    fn test_check_threshold_edge_cases() {
        // Zero values
        assert!(!check_threshold(0.0, 0.0, "above"));
        assert!(check_threshold(0.1, 0.0, "above"));

        // Negative values
        assert!(check_threshold(-1.0, -2.0, "above")); // -1 > -2
        assert!(check_threshold(-2.0, -1.0, "below")); // -2 < -1

        // Very small differences
        assert!(check_threshold(1.0001, 1.0, "above"));
        assert!(check_threshold(0.9999, 1.0, "below"));
    }

    // ========================================================================
    // Alert State Tests
    // ========================================================================

    #[test]
    fn test_alert_state_string_values() {
        // Verify state string values are consistent
        let ok_state = "OK";
        let alert_state = "ALERT";

        assert_ne!(ok_state, alert_state);
        assert_eq!(ok_state, "OK");
        assert_eq!(alert_state, "ALERT");
    }

    #[test]
    fn test_alert_state_transition_detection() {
        let current = "OK";
        let new_state = "ALERT";

        assert!(current != new_state); // State changed

        let current2 = "ALERT";
        let new_state2 = "ALERT";

        assert!(current2 == new_state2); // No state change
    }

    // ========================================================================
    // Threshold Type Tests
    // ========================================================================

    #[test]
    fn test_threshold_types() {
        let valid_types = vec!["above", "below", "above_or_equal", "below_or_equal"];

        for threshold_type in valid_types {
            // All valid types should not panic and return a boolean
            let _ = check_threshold(50.0, 50.0, threshold_type);
        }
    }

    // ========================================================================
    // Auto-Reset Logic Tests (simulate without DB)
    // ========================================================================

    #[test]
    fn test_auto_reset_timing_logic() {
        let state_changed_at = Utc::now() - Duration::minutes(10);
        let eval_window = Duration::seconds(300); // 5 minutes
        let now = Utc::now();

        let time_in_alert = now - state_changed_at;
        let should_reset = time_in_alert > eval_window;

        assert!(should_reset); // 10 minutes > 5 minute window
    }

    #[test]
    fn test_auto_reset_timing_not_ready() {
        let state_changed_at = Utc::now() - Duration::minutes(2);
        let eval_window = Duration::seconds(300); // 5 minutes
        let now = Utc::now();

        let time_in_alert = now - state_changed_at;
        let should_reset = time_in_alert > eval_window;

        assert!(!should_reset); // 2 minutes < 5 minute window
    }

    // ========================================================================
    // Metric Routing Tests
    // ========================================================================

    #[test]
    fn test_enum_variant_routing() {
        use crate::alerts::AlertQueryConfig;
        use std::collections::BTreeMap;

        let configs: Vec<(AlertQueryConfig, &str)> = vec![
            (
                AlertQueryConfig::Llm {
                    metric_name: "llm.error_rate".into(),
                    filters: BTreeMap::new(),
                    llm_metric: None,
                    llm_model: None,
                    llm_score_name: None,
                },
                "llm",
            ),
            (
                AlertQueryConfig::Metrics {
                    metric_name: "http.server.duration".into(),
                    filters: BTreeMap::new(),
                    group_by: vec![],
                    time_aggregation: "avg".into(),
                    space_aggregation: "sum".into(),
                },
                "metrics",
            ),
            (
                AlertQueryConfig::LogPattern {
                    patterns: vec!["error".into()],
                    log_source: "all".into(),
                },
                "log_pattern",
            ),
            (
                AlertQueryConfig::PromQL {
                    promql: "up == 0".into(),
                },
                "promql",
            ),
        ];

        for (config, expected) in configs {
            let routed = match &config {
                AlertQueryConfig::Llm { .. } => "llm",
                AlertQueryConfig::Metrics { .. } => "metrics",
                AlertQueryConfig::LogPattern { .. } => "log_pattern",
                AlertQueryConfig::PromQL { .. } => "promql",
            };
            assert_eq!(routed, expected);
        }
    }

    #[test]
    fn test_llm_prefix_stripping() {
        let metric_name = "llm.latency_p95";
        let stripped = metric_name.strip_prefix("llm.").unwrap();
        assert_eq!(stripped, "latency_p95");

        let not_llm = "http.server.duration";
        assert!(not_llm.strip_prefix("llm.").is_none());
    }
}
