//! Background Worker for LLM Rollout Auto-Promote/Rollback
//!
//! This worker monitors active rollouts and automatically promotes or rolls back
//! based on metric thresholds.

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::Instrument;
use uuid::Uuid;

use async_trait::async_trait;

use crate::app_state::RedisPool;
use crate::clickhouse_db::ClickHousePool;
use crate::gateway::domain_types::{RolloutStageStatus, RolloutStatus};
use crate::llm::cache::invalidate_rollout_cache;
use crate::llm::types::{RolloutVariant, VariantMetrics};

/// Default maximum error rate increase before rollback (5%).
/// Exposed in API responses so users understand the applied thresholds.
pub const DEFAULT_MAX_ERROR_RATE_INCREASE: f64 = 0.05;

/// Default maximum latency increase percentage before rollback (20%).
/// Exposed in API responses so users understand the applied thresholds.
pub const DEFAULT_MAX_LATENCY_INCREASE_PCT: f64 = 20.0;

/// Interval between rollout worker evaluation runs (seconds).
const ROLLOUT_WORKER_INTERVAL_SECS: u64 = 60;

/// Default minimum number of requests before evaluating a stage.
const DEFAULT_MIN_REQUESTS: u64 = 100;

/// Weight assigned when a rollout is completed (100% traffic to new version).
const COMPLETED_WEIGHT: i32 = 100;

/// Result of comparing target vs baseline metrics.
#[derive(Debug)]
pub enum ComparisonResult {
    /// All metrics are within acceptable thresholds.
    Pass,
    /// One or more metrics exceed thresholds.
    Fail(String),
    /// Not enough data to make a decision.
    Inconclusive,
}

/// Active rollout information.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct RolloutInfo {
    pub id: Uuid,
    pub project_id: Uuid,
    pub config_id: Uuid,
    pub target_version_id: Uuid,
    pub current_stage: i32,
    pub last_stage_change_at: Option<DateTime<Utc>>,
}

/// Stage configuration.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct StageInfo {
    pub id: Uuid,
    pub stage_order: i32,
    pub weight: i32,
    pub min_duration_minutes: Option<i32>,
    pub min_requests: Option<i32>,
    pub max_error_rate_increase: Option<Decimal>,
    pub max_latency_increase_pct: Option<Decimal>,
    pub min_quality_score: Option<Decimal>,
}

/// Dependencies for the rollout worker.
pub struct RolloutWorkerDeps {
    pub db: Arc<PgPool>,
    pub clickhouse: Arc<ClickHousePool>,
    pub redis: Arc<RedisPool>,
    pub event_publisher: Arc<reiver_core::events::EventPublisher>,
}

/// Start the rollout worker background task.
///
/// Returns a JoinHandle for the spawned task.
pub fn start_rollout_worker(
    db: Arc<PgPool>,
    clickhouse: Arc<ClickHousePool>,
    redis: Arc<RedisPool>,
    event_publisher: Arc<reiver_core::events::EventPublisher>,
    shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let deps = RolloutWorkerDeps {
            db,
            clickhouse,
            redis,
            event_publisher,
        };
        run_rollout_worker(deps, shutdown_rx).await;
    })
}

/// Run the rollout worker background task.
///
/// This task periodically checks all running auto-mode rollouts and
/// evaluates whether to promote, rollback, or continue collecting data.
async fn run_rollout_worker(deps: RolloutWorkerDeps, mut shutdown_rx: watch::Receiver<bool>) {
    let mut interval = tokio::time::interval(Duration::from_secs(ROLLOUT_WORKER_INTERVAL_SECS));

    loop {
        tokio::select! {
            _ = interval.tick() => {
                async {
                    // Get all running rollouts in 'auto' mode
                    let rollouts = match get_running_auto_rollouts(&deps).await {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::error!(error = %e, "Failed to fetch running rollouts");
                            return;
                        }
                    };

                    for rollout in rollouts {
                        if let Err(e) = evaluate_rollout(&deps, &rollout).await {
                            tracing::error!(
                                rollout_id = %rollout.id,
                                error = %e,
                                "Rollout evaluation failed"
                            );
                        }
                    }
                }
                .instrument(tracing::info_span!("flow.rollout_worker.tick"))
                .await
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    tracing::info!("Rollout worker received shutdown signal");
                    break;
                }
            }
        }
    }
}

/// Get all running auto-mode rollouts.
async fn get_running_auto_rollouts(
    deps: &RolloutWorkerDeps,
) -> Result<Vec<RolloutInfo>, sqlx::Error> {
    sqlx::query_as::<_, RolloutInfo>(&format!(
        r#"
            SELECT id, project_id, config_id, target_version_id,
                   current_stage, last_stage_change_at
            FROM llm_rollouts
            WHERE status = '{}' AND mode = 'auto'
            "#,
        RolloutStatus::Running.as_str()
    ))
    .fetch_all(deps.db.as_ref())
    .await
}

/// Evaluate a single rollout and take action if needed.
#[tracing::instrument(
    name = "flow.rollout_worker.evaluate",
    skip(deps, rollout),
    fields(rollout_id = %rollout.id)
)]
async fn evaluate_rollout(
    deps: &RolloutWorkerDeps,
    rollout: &RolloutInfo,
) -> Result<(), anyhow::Error> {
    // Get current stage info
    let current_stage = get_current_stage(deps, rollout).await?;

    // Check if enough time has passed since last stage change
    if !stage_duration_elapsed(rollout, &current_stage) {
        tracing::debug!(
            rollout_id = %rollout.id,
            stage = current_stage.stage_order,
            "Stage duration not yet elapsed"
        );
        return Ok(());
    }

    // Collect metrics for both variants
    let stage_started_at = rollout.last_stage_change_at.unwrap_or_else(Utc::now);
    let metrics = collect_stage_metrics(deps, rollout, &stage_started_at).await?;

    // Check if we have enough requests
    let min_requests = current_stage
        .min_requests
        .unwrap_or(DEFAULT_MIN_REQUESTS as i32) as u64;
    if metrics.target.request_count < min_requests {
        tracing::debug!(
            rollout_id = %rollout.id,
            stage = current_stage.stage_order,
            requests = metrics.target.request_count,
            min_requests = min_requests,
            "Not enough requests for evaluation"
        );
        // Save metrics snapshot for monitoring
        save_metrics_snapshot(deps, rollout, &current_stage, &metrics).await?;
        return Ok(());
    }

    // NOTE: Automatic promotion and rollback is disabled. The user should
    // manually decide whether to promote or rollback via the UI. In the future
    // we will implement user-defined conditions (similar to session profiles)
    // so users can create custom promotion/rollback logic.
    //
    // For now, we only save metrics snapshots for display in the UI.
    save_metrics_snapshot(deps, rollout, &current_stage, &metrics).await?;

    let comparison = compare_metrics(&metrics.target, &metrics.baseline, &current_stage);
    tracing::debug!(
        rollout_id = %rollout.id,
        stage = current_stage.stage_order,
        comparison = ?comparison,
        "Rollout metrics snapshot saved (auto-action disabled)"
    );

    Ok(())
}

/// Get the current stage configuration.
async fn get_current_stage(
    deps: &RolloutWorkerDeps,
    rollout: &RolloutInfo,
) -> Result<StageInfo, anyhow::Error> {
    sqlx::query_as::<_, StageInfo>(
        r#"
        SELECT id, stage_order, weight, min_duration_minutes, min_requests,
               max_error_rate_increase, max_latency_increase_pct, min_quality_score
        FROM llm_rollout_stages
        WHERE rollout_id = $1 AND stage_order = $2
        "#,
    )
    .bind(rollout.id)
    .bind(rollout.current_stage)
    .fetch_one(deps.db.as_ref())
    .await
    .map_err(|e| anyhow::anyhow!("Failed to get current stage: {}", e))
}

/// Check if the minimum stage duration has elapsed.
fn stage_duration_elapsed(rollout: &RolloutInfo, stage: &StageInfo) -> bool {
    let min_minutes = stage.min_duration_minutes.unwrap_or(10) as i64;
    if min_minutes == 0 {
        return true; // No minimum duration requirement
    }

    let stage_started = rollout.last_stage_change_at.unwrap_or_else(Utc::now);
    let elapsed = Utc::now().signed_duration_since(stage_started);
    elapsed >= ChronoDuration::minutes(min_minutes)
}

/// Metrics for both variants.
pub struct MetricsPair {
    pub target: VariantMetrics,
    pub baseline: VariantMetrics,
}

// ---------------------------------------------------------------------------
// RolloutMetricsSource trait
// ---------------------------------------------------------------------------

/// Abstraction over the metrics backend used by the rollout worker.
///
/// The production implementation queries ClickHouse + Postgres.  Tests
/// can substitute [`InMemoryMetricsSource`] to exercise the evaluation
/// and state machine logic without any infrastructure.
#[async_trait]
pub trait RolloutMetricsSource: Send + Sync {
    async fn collect_metrics(
        &self,
        rollout: &RolloutInfo,
        since: &DateTime<Utc>,
    ) -> Result<MetricsPair, anyhow::Error>;
}

/// ClickHouse + Postgres backed [`RolloutMetricsSource`].
struct ClickHouseMetricsSource<'a> {
    deps: &'a RolloutWorkerDeps,
}

#[async_trait]
impl<'a> RolloutMetricsSource for ClickHouseMetricsSource<'a> {
    async fn collect_metrics(
        &self,
        rollout: &RolloutInfo,
        since: &DateTime<Utc>,
    ) -> Result<MetricsPair, anyhow::Error> {
        collect_stage_metrics(self.deps, rollout, since).await
    }
}

/// In-memory [`RolloutMetricsSource`] for tests.
///
/// Returns pre-configured metrics regardless of the rollout or time window.
pub struct InMemoryMetricsSource {
    pub target: VariantMetrics,
    pub baseline: VariantMetrics,
}

#[async_trait]
impl RolloutMetricsSource for InMemoryMetricsSource {
    async fn collect_metrics(
        &self,
        _rollout: &RolloutInfo,
        _since: &DateTime<Utc>,
    ) -> Result<MetricsPair, anyhow::Error> {
        Ok(MetricsPair {
            target: self.target.clone(),
            baseline: self.baseline.clone(),
        })
    }
}

/// Collect metrics from ClickHouse for both variants.
async fn collect_stage_metrics(
    deps: &RolloutWorkerDeps,
    rollout: &RolloutInfo,
    since: &DateTime<Utc>,
) -> Result<MetricsPair, anyhow::Error> {
    let query = format!(
        r#"
        SELECT
            rollout_variant,
            count() as request_count,
            countIf(status_code = 'error') as error_count,
            if(count() > 0, countIf(status_code = 'error') / count(), 0) as error_rate,
            avg(duration_ms) as avg_latency_ms,
            quantile(0.95)(duration_ms) as p95_latency_ms,
            avg(cost_usd) as avg_cost_usd
        FROM reiver.llm_requests
        WHERE rollout_id = '{}'
          AND timestamp >= '{}'
        GROUP BY rollout_variant
        "#,
        rollout.id,
        since.format("%Y-%m-%d %H:%M:%S")
    );

    #[derive(Debug, clickhouse::Row, serde::Deserialize)]
    struct MetricRow {
        rollout_variant: String,
        request_count: u64,
        error_count: u64,
        error_rate: f64,
        avg_latency_ms: f64,
        p95_latency_ms: f64,
        avg_cost_usd: f64,
    }

    let rows: Vec<MetricRow> = match deps.clickhouse.query(&query).fetch_all().await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!(
                rollout_id = %rollout.id,
                error = %e,
                "Failed to fetch rollout metrics from ClickHouse"
            );
            return Err(anyhow::anyhow!("ClickHouse query failed: {}", e));
        }
    };

    let mut target = VariantMetrics::default();
    let mut baseline = VariantMetrics::default();

    // Query quality scores from PostgreSQL
    // First get request_ids for each variant, then query average scores
    let quality_scores = collect_quality_scores(deps, rollout, since)
        .await
        .unwrap_or_default();

    for row in rows {
        let avg_quality_score = quality_scores.get(&row.rollout_variant).copied();

        let metrics = VariantMetrics {
            request_count: row.request_count,
            error_count: row.error_count,
            error_rate: row.error_rate,
            avg_latency_ms: row.avg_latency_ms,
            p95_latency_ms: row.p95_latency_ms,
            avg_cost_usd: Decimal::try_from(row.avg_cost_usd).unwrap_or_default(),
            avg_quality_score,
        };

        match RolloutVariant::from_str(&row.rollout_variant) {
            Some(RolloutVariant::Target) => target = metrics,
            Some(RolloutVariant::Baseline) => baseline = metrics,
            None => {}
        }
    }

    Ok(MetricsPair { target, baseline })
}

/// Collect quality scores from PostgreSQL for each variant.
///
/// This function:
/// 1. Queries ClickHouse to get request_ids for each variant in the rollout
/// 2. Queries PostgreSQL to get average quality scores for those requests
async fn collect_quality_scores(
    deps: &RolloutWorkerDeps,
    rollout: &RolloutInfo,
    since: &DateTime<Utc>,
) -> Result<std::collections::HashMap<String, f64>, anyhow::Error> {
    use std::collections::HashMap;

    // Step 1: Get request_ids grouped by variant from ClickHouse
    let request_ids_query = format!(
        r#"
        SELECT 
            rollout_variant,
            groupArray(request_id) as request_ids
        FROM reiver.llm_requests
        WHERE rollout_id = '{}'
          AND timestamp >= '{}'
          AND rollout_variant != ''
        GROUP BY rollout_variant
        "#,
        rollout.id,
        since.format("%Y-%m-%d %H:%M:%S")
    );

    #[derive(Debug, clickhouse::Row, serde::Deserialize)]
    struct RequestIdRow {
        rollout_variant: String,
        request_ids: Vec<String>,
    }

    let request_id_rows: Vec<RequestIdRow> =
        match deps.clickhouse.query(&request_ids_query).fetch_all().await {
            Ok(rows) => rows,
            Err(e) => {
                tracing::warn!(
                    rollout_id = %rollout.id,
                    error = %e,
                    "Failed to fetch request IDs for quality scores, skipping quality check"
                );
                return Ok(HashMap::new());
            }
        };

    if request_id_rows.is_empty() {
        return Ok(HashMap::new());
    }

    let mut result: HashMap<String, f64> = HashMap::new();

    // Step 2: Query PostgreSQL for average scores for each variant
    for row in request_id_rows {
        if row.request_ids.is_empty() {
            continue;
        }

        // Query PostgreSQL for average quality score across all score types
        // We use numeric scores (0-100 range) for quality comparison
        let avg_score: Option<(rust_decimal::Decimal,)> = sqlx::query_as(
            r#"
            SELECT AVG(score_value) as avg_score
            FROM llm_evaluation_scores
            WHERE project_id = $1
              AND request_id = ANY($2)
              AND score_type = 'number'
            "#,
        )
        .bind(rollout.project_id)
        .bind(&row.request_ids)
        .fetch_optional(deps.db.as_ref())
        .await
        .ok()
        .flatten();

        if let Some((avg,)) = avg_score {
            if let Some(score_f64) = avg.to_f64() {
                result.insert(row.rollout_variant.clone(), score_f64);
            }
        }
    }

    Ok(result)
}

/// Compare target vs baseline metrics against thresholds.
pub fn compare_metrics(
    target: &VariantMetrics,
    baseline: &VariantMetrics,
    stage: &StageInfo,
) -> ComparisonResult {
    // Need at least some baseline data for comparison
    if baseline.request_count < 10 || target.request_count < 10 {
        return ComparisonResult::Inconclusive;
    }

    // Error rate check
    let max_error_increase = stage
        .max_error_rate_increase
        .and_then(|d| d.to_f64())
        .unwrap_or(DEFAULT_MAX_ERROR_RATE_INCREASE);
    let error_rate_diff = target.error_rate - baseline.error_rate;
    if error_rate_diff > max_error_increase {
        return ComparisonResult::Fail(format!(
            "Error rate {:.2}% exceeds baseline {:.2}% by more than {:.1}%",
            target.error_rate * 100.0,
            baseline.error_rate * 100.0,
            max_error_increase * 100.0
        ));
    }

    // Latency check
    let max_latency_increase_pct = stage
        .max_latency_increase_pct
        .and_then(|d| d.to_f64())
        .unwrap_or(DEFAULT_MAX_LATENCY_INCREASE_PCT);
    let latency_increase_pct =
        crate::utils::percentage_change(target.avg_latency_ms, baseline.avg_latency_ms);
    if latency_increase_pct > max_latency_increase_pct {
        return ComparisonResult::Fail(format!(
            "Latency {:.0}ms exceeds baseline {:.0}ms by {:.1}% (max {:.1}%)",
            target.avg_latency_ms,
            baseline.avg_latency_ms,
            latency_increase_pct,
            max_latency_increase_pct
        ));
    }

    // Quality score check (if available)
    if let Some(min_score) = stage.min_quality_score {
        if let Some(min_score_f) = min_score.to_f64() {
            if let Some(target_score) = target.avg_quality_score {
                if target_score < min_score_f {
                    return ComparisonResult::Fail(format!(
                        "Quality score {:.2} below minimum {:.2}",
                        target_score, min_score_f
                    ));
                }
            }
        }
    }

    ComparisonResult::Pass
}

// The following functions are preserved for future use when user-defined
// promotion/rollback conditions are implemented (similar to session profiles).
#[allow(dead_code)]
async fn is_final_stage(
    deps: &RolloutWorkerDeps,
    rollout: &RolloutInfo,
    current_stage: &StageInfo,
) -> Result<bool, anyhow::Error> {
    let next_stage: Option<i32> = sqlx::query_scalar(
        "SELECT stage_order FROM llm_rollout_stages WHERE rollout_id = $1 AND stage_order = $2",
    )
    .bind(rollout.id)
    .bind(current_stage.stage_order + 1)
    .fetch_optional(deps.db.as_ref())
    .await?;

    Ok(next_stage.is_none())
}

#[allow(dead_code)]
async fn promote_to_next_stage(
    deps: &RolloutWorkerDeps,
    rollout: &RolloutInfo,
    current_stage: &StageInfo,
) -> Result<(), anyhow::Error> {
    let mut tx = deps.db.begin().await?;

    // Mark current stage as passed
    sqlx::query(&format!(
        "UPDATE llm_rollout_stages SET status = '{}', completed_at = NOW() WHERE id = $1",
        RolloutStageStatus::Passed.as_str()
    ))
    .bind(current_stage.id)
    .execute(&mut *tx)
    .await?;

    // Get next stage
    let next_stage: StageInfo = sqlx::query_as(
        r#"
        SELECT id, stage_order, weight, min_duration_minutes, min_requests,
               max_error_rate_increase, max_latency_increase_pct, min_quality_score
        FROM llm_rollout_stages
        WHERE rollout_id = $1 AND stage_order = $2
        "#,
    )
    .bind(rollout.id)
    .bind(current_stage.stage_order + 1)
    .fetch_one(&mut *tx)
    .await?;

    // Mark next stage as active
    sqlx::query(&format!(
        "UPDATE llm_rollout_stages SET status = '{}', started_at = NOW() WHERE id = $1",
        RolloutStageStatus::Active.as_str()
    ))
    .bind(next_stage.id)
    .execute(&mut *tx)
    .await?;

    // Update rollout
    sqlx::query(
        r#"
        UPDATE llm_rollouts
        SET current_stage = $1, current_weight = $2, last_stage_change_at = NOW()
        WHERE id = $3
        "#,
    )
    .bind(next_stage.stage_order)
    .bind(next_stage.weight)
    .bind(rollout.id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    // Invalidate cache so gateway picks up the weight change
    invalidate_rollout_cache(&deps.redis, rollout.project_id, rollout.config_id).await;

    Ok(())
}

#[allow(dead_code)]
async fn complete_rollout(
    deps: &RolloutWorkerDeps,
    rollout: &RolloutInfo,
) -> Result<(), anyhow::Error> {
    let mut tx = deps.db.begin().await?;

    // Mark all remaining stages as passed
    sqlx::query(
        &format!(
            "UPDATE llm_rollout_stages SET status = '{}', completed_at = NOW() WHERE rollout_id = $1 AND status IN ('{}', '{}')",
            RolloutStageStatus::Passed.as_str(),
            RolloutStageStatus::Pending.as_str(),
            RolloutStageStatus::Active.as_str(),
        ),
    )
    .bind(rollout.id)
    .execute(&mut *tx)
    .await?;

    // Update config's active version
    sqlx::query("UPDATE llm_prompt_configs SET active_version_id = $1 WHERE id = $2")
        .bind(rollout.target_version_id)
        .bind(rollout.config_id)
        .execute(&mut *tx)
        .await?;

    // Complete rollout
    sqlx::query(
        &format!(
            "UPDATE llm_rollouts SET status = '{}', completed_at = NOW(), current_weight = {} WHERE id = $1",
            RolloutStatus::Completed.as_str(),
            COMPLETED_WEIGHT,
        ),
    )
    .bind(rollout.id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    // Invalidate cache so gateway stops routing to this rollout
    invalidate_rollout_cache(&deps.redis, rollout.project_id, rollout.config_id).await;

    Ok(())
}

#[allow(dead_code)]
async fn rollback_rollout(
    deps: &RolloutWorkerDeps,
    rollout: &RolloutInfo,
    reason: &str,
    metrics: &MetricsPair,
) -> Result<(), anyhow::Error> {
    let mut tx = deps.db.begin().await?;

    // Mark current stage as failed
    sqlx::query(
        &format!(
            "UPDATE llm_rollout_stages SET status = '{}', completed_at = NOW() WHERE rollout_id = $1 AND status = '{}'",
            RolloutStageStatus::Failed.as_str(),
            RolloutStageStatus::Active.as_str(),
        ),
    )
    .bind(rollout.id)
    .execute(&mut *tx)
    .await?;

    // Rollback the rollout
    sqlx::query(
        &format!(
            "UPDATE llm_rollouts SET status = '{}', completed_at = NOW(), current_weight = 0 WHERE id = $1",
            RolloutStatus::RolledBack.as_str(),
        ),
    )
    .bind(rollout.id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    // Invalidate cache so gateway stops routing to this rollout
    invalidate_rollout_cache(&deps.redis, rollout.project_id, rollout.config_id).await;

    tracing::warn!(
        rollout_id = %rollout.id,
        config_id = %rollout.config_id,
        reason = %reason,
        "Rollout rolled back"
    );

    // Fetch rollout name for the notification
    let rollout_name: Option<String> =
        sqlx::query_scalar("SELECT name FROM llm_rollouts WHERE id = $1")
            .bind(rollout.id)
            .fetch_optional(deps.db.as_ref())
            .await
            .unwrap_or(None);
    let rollout_name = rollout_name.unwrap_or_else(|| rollout.id.to_string());

    if let Err(e) = deps
        .event_publisher
        .emit(
            reiver_core::events::PlatformEventType::RolloutRolledBack,
            rollout.project_id,
            format!("rollout_rollback:{}", rollout.id),
            serde_json::json!({
                "rollout_id": rollout.id,
                "rollout_name": rollout_name,
                "reason": reason,
                "target_error_rate": metrics.target.error_rate * 100.0,
                "baseline_error_rate": metrics.baseline.error_rate * 100.0,
            }),
        )
        .await
    {
        tracing::warn!("Failed to emit RolloutRolledBack event: {}", e);
    }

    Ok(())
}

/// Save a metrics snapshot for monitoring and debugging.
async fn save_metrics_snapshot(
    deps: &RolloutWorkerDeps,
    rollout: &RolloutInfo,
    stage: &StageInfo,
    metrics: &MetricsPair,
) -> Result<(), anyhow::Error> {
    // Insert target metrics
    sqlx::query(
        r#"
        INSERT INTO llm_rollout_metrics 
        (rollout_id, stage_order, variant, request_count, error_count, error_rate, 
         avg_latency_ms, p95_latency_ms, avg_cost_usd, avg_quality_score)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        "#,
    )
    .bind(rollout.id)
    .bind(stage.stage_order)
    .bind(RolloutVariant::Target.as_str())
    .bind(metrics.target.request_count as i64)
    .bind(metrics.target.error_count as i64)
    .bind(metrics.target.error_rate)
    .bind(metrics.target.avg_latency_ms)
    .bind(metrics.target.p95_latency_ms)
    .bind(metrics.target.avg_cost_usd)
    .bind(metrics.target.avg_quality_score)
    .execute(deps.db.as_ref())
    .await?;

    // Insert baseline metrics
    sqlx::query(
        r#"
        INSERT INTO llm_rollout_metrics 
        (rollout_id, stage_order, variant, request_count, error_count, error_rate, 
         avg_latency_ms, p95_latency_ms, avg_cost_usd, avg_quality_score)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        "#,
    )
    .bind(rollout.id)
    .bind(stage.stage_order)
    .bind(RolloutVariant::Baseline.as_str())
    .bind(metrics.baseline.request_count as i64)
    .bind(metrics.baseline.error_count as i64)
    .bind(metrics.baseline.error_rate)
    .bind(metrics.baseline.avg_latency_ms)
    .bind(metrics.baseline.p95_latency_ms)
    .bind(metrics.baseline.avg_cost_usd)
    .bind(metrics.baseline.avg_quality_score)
    .execute(deps.db.as_ref())
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    struct StageInfoConfig {
        max_error_rate_increase: Option<Decimal>,
        max_latency_increase_pct: Option<Decimal>,
        min_quality_score: Option<Decimal>,
        min_duration_minutes: Option<i32>,
        min_requests: Option<i32>,
    }

    fn create_stage_info(config: StageInfoConfig) -> StageInfo {
        StageInfo {
            id: Uuid::new_v4(),
            stage_order: 1,
            weight: 50,
            min_duration_minutes: config.min_duration_minutes,
            min_requests: config.min_requests,
            max_error_rate_increase: config.max_error_rate_increase,
            max_latency_increase_pct: config.max_latency_increase_pct,
            min_quality_score: config.min_quality_score,
        }
    }

    fn create_variant_metrics(
        request_count: u64,
        error_rate: f64,
        avg_latency_ms: f64,
        avg_quality_score: Option<f64>,
    ) -> VariantMetrics {
        VariantMetrics {
            request_count,
            error_count: (request_count as f64 * error_rate) as u64,
            error_rate,
            avg_latency_ms,
            p95_latency_ms: avg_latency_ms * 1.5,
            avg_cost_usd: dec!(0.01),
            avg_quality_score,
        }
    }

    // ========================================================================
    // compare_metrics Tests
    // ========================================================================

    #[test]
    fn test_compare_metrics_pass() {
        let stage = create_stage_info(StageInfoConfig {
            max_error_rate_increase: Some(dec!(0.05)),
            max_latency_increase_pct: Some(dec!(20.0)),
            min_quality_score: None,
            min_duration_minutes: None,
            min_requests: None,
        });
        let target = create_variant_metrics(100, 0.02, 150.0, None);
        let baseline = create_variant_metrics(100, 0.01, 140.0, None);

        let result = compare_metrics(&target, &baseline, &stage);
        assert!(matches!(result, ComparisonResult::Pass));
    }

    #[test]
    fn test_compare_metrics_fail_error_rate_exceeds_threshold() {
        let stage = create_stage_info(StageInfoConfig {
            max_error_rate_increase: Some(dec!(0.05)),
            max_latency_increase_pct: Some(dec!(20.0)),
            min_quality_score: None,
            min_duration_minutes: None,
            min_requests: None,
        });
        // Target error rate 15%, baseline 5% -> 10% increase exceeds 5% threshold
        let target = create_variant_metrics(100, 0.15, 150.0, None);
        let baseline = create_variant_metrics(100, 0.05, 140.0, None);

        let result = compare_metrics(&target, &baseline, &stage);
        assert!(matches!(result, ComparisonResult::Fail(_)));
        if let ComparisonResult::Fail(reason) = result {
            assert!(reason.contains("Error rate"), "Reason: {}", reason);
        }
    }

    #[test]
    fn test_compare_metrics_fail_latency_exceeds_threshold() {
        let stage = create_stage_info(StageInfoConfig {
            max_error_rate_increase: Some(dec!(0.05)),
            max_latency_increase_pct: Some(dec!(20.0)),
            min_quality_score: None,
            min_duration_minutes: None,
            min_requests: None,
        });
        // Target latency 200ms, baseline 100ms -> 100% increase exceeds 20% threshold
        let target = create_variant_metrics(100, 0.01, 200.0, None);
        let baseline = create_variant_metrics(100, 0.01, 100.0, None);

        let result = compare_metrics(&target, &baseline, &stage);
        assert!(matches!(result, ComparisonResult::Fail(_)));
        if let ComparisonResult::Fail(reason) = result {
            assert!(reason.contains("Latency"), "Reason: {}", reason);
        }
    }

    #[test]
    fn test_compare_metrics_inconclusive_insufficient_baseline() {
        let stage = create_stage_info(StageInfoConfig {
            max_error_rate_increase: Some(dec!(0.05)),
            max_latency_increase_pct: Some(dec!(20.0)),
            min_quality_score: None,
            min_duration_minutes: None,
            min_requests: None,
        });
        let target = create_variant_metrics(100, 0.02, 150.0, None);
        let baseline = create_variant_metrics(5, 0.01, 140.0, None); // Only 5 requests

        let result = compare_metrics(&target, &baseline, &stage);
        assert!(matches!(result, ComparisonResult::Inconclusive));
    }

    #[test]
    fn test_compare_metrics_inconclusive_insufficient_target() {
        let stage = create_stage_info(StageInfoConfig {
            max_error_rate_increase: Some(dec!(0.05)),
            max_latency_increase_pct: Some(dec!(20.0)),
            min_quality_score: None,
            min_duration_minutes: None,
            min_requests: None,
        });
        let target = create_variant_metrics(5, 0.02, 150.0, None); // Only 5 requests
        let baseline = create_variant_metrics(100, 0.01, 140.0, None);

        let result = compare_metrics(&target, &baseline, &stage);
        assert!(matches!(result, ComparisonResult::Inconclusive));
    }

    #[test]
    fn test_compare_metrics_fail_quality_score_below_minimum() {
        let stage = create_stage_info(StageInfoConfig {
            max_error_rate_increase: Some(dec!(0.05)),
            max_latency_increase_pct: Some(dec!(20.0)),
            min_quality_score: Some(dec!(80.0)), // Minimum quality score 80
            min_duration_minutes: None,
            min_requests: None,
        });
        let target = create_variant_metrics(100, 0.01, 150.0, Some(70.0)); // Score 70 < 80
        let baseline = create_variant_metrics(100, 0.01, 140.0, Some(85.0));

        let result = compare_metrics(&target, &baseline, &stage);
        assert!(matches!(result, ComparisonResult::Fail(_)));
        if let ComparisonResult::Fail(reason) = result {
            assert!(reason.contains("Quality score"), "Reason: {}", reason);
        }
    }

    #[test]
    fn test_compare_metrics_pass_quality_score_above_minimum() {
        let stage = create_stage_info(StageInfoConfig {
            max_error_rate_increase: Some(dec!(0.05)),
            max_latency_increase_pct: Some(dec!(20.0)),
            min_quality_score: Some(dec!(80.0)), // Minimum quality score 80
            min_duration_minutes: None,
            min_requests: None,
        });
        let target = create_variant_metrics(100, 0.01, 150.0, Some(90.0)); // Score 90 > 80
        let baseline = create_variant_metrics(100, 0.01, 140.0, Some(85.0));

        let result = compare_metrics(&target, &baseline, &stage);
        assert!(matches!(result, ComparisonResult::Pass));
    }

    #[test]
    fn test_compare_metrics_pass_no_quality_score_with_threshold() {
        // Quality threshold set but no score data - should not fail
        let stage = create_stage_info(StageInfoConfig {
            max_error_rate_increase: Some(dec!(0.05)),
            max_latency_increase_pct: Some(dec!(20.0)),
            min_quality_score: Some(dec!(80.0)),
            min_duration_minutes: None,
            min_requests: None,
        });
        let target = create_variant_metrics(100, 0.01, 150.0, None); // No quality score
        let baseline = create_variant_metrics(100, 0.01, 140.0, None);

        let result = compare_metrics(&target, &baseline, &stage);
        assert!(matches!(result, ComparisonResult::Pass));
    }

    #[test]
    fn test_compare_metrics_uses_default_thresholds() {
        // No thresholds specified - should use defaults
        let stage = create_stage_info(StageInfoConfig {
            max_error_rate_increase: None,
            max_latency_increase_pct: None,
            min_quality_score: None,
            min_duration_minutes: None,
            min_requests: None,
        });
        // Error rate increase: 10% (exceeds default 5%)
        let target = create_variant_metrics(100, 0.15, 150.0, None);
        let baseline = create_variant_metrics(100, 0.05, 140.0, None);

        let result = compare_metrics(&target, &baseline, &stage);
        assert!(matches!(result, ComparisonResult::Fail(_)));
    }

    #[test]
    fn test_compare_metrics_latency_zero_baseline() {
        // Zero baseline latency should not cause division by zero
        let stage = create_stage_info(StageInfoConfig {
            max_error_rate_increase: Some(dec!(0.05)),
            max_latency_increase_pct: Some(dec!(20.0)),
            min_quality_score: None,
            min_duration_minutes: None,
            min_requests: None,
        });
        let target = create_variant_metrics(100, 0.01, 150.0, None);
        let baseline = create_variant_metrics(100, 0.01, 0.0, None); // Zero latency

        let result = compare_metrics(&target, &baseline, &stage);
        // Should pass (latency check skipped when baseline is 0)
        assert!(matches!(result, ComparisonResult::Pass));
    }

    // ========================================================================
    // stage_duration_elapsed Tests
    // ========================================================================

    #[test]
    fn test_stage_duration_elapsed_true() {
        let stage = create_stage_info(StageInfoConfig {
            max_error_rate_increase: None,
            max_latency_increase_pct: None,
            min_quality_score: None,
            min_duration_minutes: Some(10), // 10 min required
            min_requests: None,
        });
        let rollout = RolloutInfo {
            id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            config_id: Uuid::new_v4(),
            target_version_id: Uuid::new_v4(),
            current_stage: 1,
            last_stage_change_at: Some(Utc::now() - ChronoDuration::minutes(15)), // 15 min ago
        };

        assert!(stage_duration_elapsed(&rollout, &stage));
    }

    #[test]
    fn test_stage_duration_elapsed_false() {
        let stage = create_stage_info(StageInfoConfig {
            max_error_rate_increase: None,
            max_latency_increase_pct: None,
            min_quality_score: None,
            min_duration_minutes: Some(10), // 10 min required
            min_requests: None,
        });
        let rollout = RolloutInfo {
            id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            config_id: Uuid::new_v4(),
            target_version_id: Uuid::new_v4(),
            current_stage: 1,
            last_stage_change_at: Some(Utc::now() - ChronoDuration::minutes(5)), // Only 5 min ago
        };

        assert!(!stage_duration_elapsed(&rollout, &stage));
    }

    #[test]
    fn test_stage_duration_elapsed_zero_requirement() {
        let stage = create_stage_info(StageInfoConfig {
            max_error_rate_increase: None,
            max_latency_increase_pct: None,
            min_quality_score: None,
            min_duration_minutes: Some(0), // 0 min required
            min_requests: None,
        });
        let rollout = RolloutInfo {
            id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            config_id: Uuid::new_v4(),
            target_version_id: Uuid::new_v4(),
            current_stage: 1,
            last_stage_change_at: Some(Utc::now()), // Just now
        };

        assert!(stage_duration_elapsed(&rollout, &stage));
    }

    #[test]
    fn test_stage_duration_elapsed_no_last_change() {
        let stage = create_stage_info(StageInfoConfig {
            max_error_rate_increase: None,
            max_latency_increase_pct: None,
            min_quality_score: None,
            min_duration_minutes: Some(10), // 10 min required
            min_requests: None,
        });
        let rollout = RolloutInfo {
            id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            config_id: Uuid::new_v4(),
            target_version_id: Uuid::new_v4(),
            current_stage: 1,
            last_stage_change_at: None, // No recorded time
        };

        // Should use current time, so not elapsed
        assert!(!stage_duration_elapsed(&rollout, &stage));
    }

    #[test]
    fn test_stage_duration_uses_default_when_none() {
        let stage = create_stage_info(StageInfoConfig {
            max_error_rate_increase: None,
            max_latency_increase_pct: None,
            min_quality_score: None,
            min_duration_minutes: None, // No min duration (defaults to 10)
            min_requests: None,
        });
        let rollout = RolloutInfo {
            id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            config_id: Uuid::new_v4(),
            target_version_id: Uuid::new_v4(),
            current_stage: 1,
            last_stage_change_at: Some(Utc::now() - ChronoDuration::minutes(5)), // 5 min ago
        };

        // Default is 10 min, only 5 elapsed
        assert!(!stage_duration_elapsed(&rollout, &stage));
    }

    // ========================================================================
    // RolloutMetricsSource / InMemoryMetricsSource Tests
    // ========================================================================

    #[tokio::test]
    async fn test_in_memory_metrics_source_returns_configured_metrics() {
        let source = InMemoryMetricsSource {
            target: create_variant_metrics(500, 0.02, 120.0, Some(85.0)),
            baseline: create_variant_metrics(500, 0.01, 100.0, Some(90.0)),
        };
        let rollout = RolloutInfo {
            id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            config_id: Uuid::new_v4(),
            target_version_id: Uuid::new_v4(),
            current_stage: 0,
            last_stage_change_at: Some(Utc::now() - ChronoDuration::hours(1)),
        };

        let pair = source.collect_metrics(&rollout, &Utc::now()).await.unwrap();
        assert_eq!(pair.target.request_count, 500);
        assert_eq!(pair.baseline.request_count, 500);
        assert_eq!(pair.target.avg_quality_score, Some(85.0));
    }

    #[tokio::test]
    async fn test_evaluate_lifecycle_pass_with_in_memory_metrics() {
        let source = InMemoryMetricsSource {
            target: create_variant_metrics(200, 0.01, 110.0, None),
            baseline: create_variant_metrics(200, 0.01, 100.0, None),
        };

        let stage = create_stage_info(StageInfoConfig {
            max_error_rate_increase: Some(dec!(0.05)),
            max_latency_increase_pct: Some(dec!(20.0)),
            min_quality_score: None,
            min_duration_minutes: Some(0),
            min_requests: Some(100),
        });

        let rollout = RolloutInfo {
            id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            config_id: Uuid::new_v4(),
            target_version_id: Uuid::new_v4(),
            current_stage: 0,
            last_stage_change_at: Some(Utc::now() - ChronoDuration::hours(1)),
        };

        assert!(stage_duration_elapsed(&rollout, &stage));

        let pair = source.collect_metrics(&rollout, &Utc::now()).await.unwrap();
        assert!(pair.target.request_count >= stage.min_requests.unwrap_or(0) as u64);

        let comparison = compare_metrics(&pair.target, &pair.baseline, &stage);
        assert!(
            matches!(comparison, ComparisonResult::Pass),
            "metrics within thresholds should pass"
        );
    }

    #[tokio::test]
    async fn test_evaluate_lifecycle_fail_triggers_rollback_signal() {
        let source = InMemoryMetricsSource {
            target: create_variant_metrics(200, 0.20, 300.0, None),
            baseline: create_variant_metrics(200, 0.01, 100.0, None),
        };

        let stage = create_stage_info(StageInfoConfig {
            max_error_rate_increase: Some(dec!(0.05)),
            max_latency_increase_pct: Some(dec!(20.0)),
            min_quality_score: None,
            min_duration_minutes: Some(0),
            min_requests: Some(100),
        });

        let rollout = RolloutInfo {
            id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            config_id: Uuid::new_v4(),
            target_version_id: Uuid::new_v4(),
            current_stage: 0,
            last_stage_change_at: Some(Utc::now() - ChronoDuration::hours(1)),
        };

        let pair = source.collect_metrics(&rollout, &Utc::now()).await.unwrap();
        let comparison = compare_metrics(&pair.target, &pair.baseline, &stage);
        assert!(
            matches!(comparison, ComparisonResult::Fail(_)),
            "20% error rate should exceed 5% threshold"
        );
    }

    #[tokio::test]
    async fn test_evaluate_lifecycle_insufficient_requests_is_inconclusive() {
        let source = InMemoryMetricsSource {
            target: create_variant_metrics(5, 0.01, 110.0, None),
            baseline: create_variant_metrics(5, 0.01, 100.0, None),
        };

        let stage = create_stage_info(StageInfoConfig {
            max_error_rate_increase: Some(dec!(0.05)),
            max_latency_increase_pct: Some(dec!(20.0)),
            min_quality_score: None,
            min_duration_minutes: Some(0),
            min_requests: Some(100),
        });

        let rollout = RolloutInfo {
            id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            config_id: Uuid::new_v4(),
            target_version_id: Uuid::new_v4(),
            current_stage: 0,
            last_stage_change_at: Some(Utc::now() - ChronoDuration::hours(1)),
        };

        let pair = source.collect_metrics(&rollout, &Utc::now()).await.unwrap();

        let min_requests = stage.min_requests.unwrap_or(DEFAULT_MIN_REQUESTS as i32) as u64;
        assert!(
            pair.target.request_count < min_requests,
            "should have fewer requests than minimum"
        );

        let comparison = compare_metrics(&pair.target, &pair.baseline, &stage);
        assert!(
            matches!(comparison, ComparisonResult::Inconclusive),
            "insufficient data should be inconclusive"
        );
    }

    #[tokio::test]
    async fn test_evaluate_lifecycle_quality_gate_blocks_promotion() {
        let source = InMemoryMetricsSource {
            target: create_variant_metrics(200, 0.01, 110.0, Some(60.0)),
            baseline: create_variant_metrics(200, 0.01, 100.0, Some(90.0)),
        };

        let stage = create_stage_info(StageInfoConfig {
            max_error_rate_increase: Some(dec!(0.05)),
            max_latency_increase_pct: Some(dec!(20.0)),
            min_quality_score: Some(dec!(80.0)),
            min_duration_minutes: Some(0),
            min_requests: Some(100),
        });

        let rollout = RolloutInfo {
            id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            config_id: Uuid::new_v4(),
            target_version_id: Uuid::new_v4(),
            current_stage: 0,
            last_stage_change_at: Some(Utc::now() - ChronoDuration::hours(1)),
        };

        let pair = source.collect_metrics(&rollout, &Utc::now()).await.unwrap();
        let comparison = compare_metrics(&pair.target, &pair.baseline, &stage);
        assert!(
            matches!(comparison, ComparisonResult::Fail(ref msg) if msg.contains("Quality")),
            "quality score 60 < min 80 should fail"
        );
    }

    #[tokio::test]
    async fn test_evaluate_lifecycle_duration_not_elapsed_skips_evaluation() {
        let stage = create_stage_info(StageInfoConfig {
            max_error_rate_increase: Some(dec!(0.05)),
            max_latency_increase_pct: Some(dec!(20.0)),
            min_quality_score: None,
            min_duration_minutes: Some(30),
            min_requests: Some(100),
        });

        let rollout = RolloutInfo {
            id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            config_id: Uuid::new_v4(),
            target_version_id: Uuid::new_v4(),
            current_stage: 0,
            last_stage_change_at: Some(Utc::now() - ChronoDuration::minutes(5)),
        };

        assert!(
            !stage_duration_elapsed(&rollout, &stage),
            "5 min elapsed < 30 min required, should skip evaluation"
        );
    }
}
