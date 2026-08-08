//! Billing worker for budget alerts and monthly charge generation.
//!
//! This worker:
//! 1. Runs hourly via tokio interval
//! 2. Checks budget thresholds and triggers notifications
//! 3. Monitors for orphaned Stripe records (invoices/subscriptions)
//! 4. Cleans up orphaned records (resolved after 90 days, unresolved after 1 year for PII compliance)
//! 5. Escalates stale unresolved orphaned records (>30 days) for investigation
//! 6. Every hour, ensures `pending_charges` exist for the previous calendar month (idempotent)

use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use sqlx::Row;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::alerts::{load_notification_channel, send_notification, AlertNotification, AlertState};
use crate::clickhouse_db::ClickHousePool;
use crate::config::Config;
use crate::db::DbPool;

use crate::billing::BillingService;

// =========================================================================
// Worker Configuration Constants
// =========================================================================

/// Minimum usage increase (in percent) required to re-send a threshold alert
/// during the cooldown period. This prevents alert fatigue while still notifying
/// about significant usage increases.
const BUDGET_REALERT_THRESHOLD_PERCENT: i32 = 10;

/// Number of days to retain resolved orphaned records before cleanup.
/// This provides time for auditing while preventing indefinite data growth.
const ORPHAN_RECORD_RETENTION_DAYS: i64 = 90;

/// Number of days after which unresolved orphaned records trigger escalation warnings.
/// Records older than this likely represent data sync issues that need manual investigation.
const ORPHAN_RECORD_ESCALATION_THRESHOLD_DAYS: i64 = 30;

/// Check if a timestamp is in a different billing period than the current time.
/// Returns true if the timestamp is in a previous month (billing period reset).
///
/// Used to reset budget alert deduplication at billing period boundaries.
fn is_different_billing_period(timestamp: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    timestamp.year() != now.year() || timestamp.month() != now.month()
}

/// Maximum retention for unresolved orphaned records before deletion (PII compliance).
/// The webhook_payload may contain customer email, name, and billing address.
const ORPHAN_RECORD_MAX_RETENTION_DAYS: i64 = 365;

/// Decide whether a budget alert should be sent based on deduplication rules.
///
/// Returns `true` if the alert should fire, `false` if it should be suppressed.
/// The decision tree handles:
/// - Exceeded alerts: cooldown period, billing period reset
/// - Threshold alerts: cooldown, re-alert on significant usage increase, billing period reset
/// - Neither exceeded nor at threshold: always suppress
fn should_send_alert(
    budget_exceeded: bool,
    threshold_exceeded: bool,
    current_percent: i32,
    last_exceeded_alert_at: Option<DateTime<Utc>>,
    last_threshold_alert_at: Option<DateTime<Utc>>,
    last_alert_percent: Option<i32>,
    now: DateTime<Utc>,
    cooldown: Duration,
) -> bool {
    if budget_exceeded {
        match last_exceeded_alert_at {
            Some(last)
                if !is_different_billing_period(last, now)
                    && now.signed_duration_since(last) < cooldown =>
            {
                false
            }
            _ => true,
        }
    } else if threshold_exceeded {
        match (last_threshold_alert_at, last_alert_percent) {
            (Some(last), _) if is_different_billing_period(last, now) => true,
            (Some(last), Some(last_pct))
                if now.signed_duration_since(last) < cooldown =>
            {
                (current_percent - last_pct) >= BUDGET_REALERT_THRESHOLD_PERCENT
            }
            (Some(last), None) if now.signed_duration_since(last) < cooldown => false,
            _ => true,
        }
    } else {
        false
    }
}

// =========================================================================
// Internal Types
// =========================================================================

/// Budget data with alert history for threshold checking.
/// Used internally by `check_budget_thresholds` to track alert deduplication state.
struct BudgetWithAlertHistory {
    id: Uuid,
    organization_id: Uuid,
    monthly_budget_usd: Decimal,
    alert_threshold_percent: i32,
    last_threshold_alert_at: Option<chrono::DateTime<Utc>>,
    last_exceeded_alert_at: Option<chrono::DateTime<Utc>>,
    last_alert_percent: Option<i32>,
}

/// Start the billing worker.
/// Runs hourly to check budget thresholds and generate monthly charges.
///
/// # Arguments
/// * `db_pool` - Database connection pool
/// * `clickhouse_pool` - ClickHouse connection pool
/// * `config` - Application configuration (for budget alert settings)
/// * `shutdown_rx` - Shutdown signal receiver
pub async fn start_billing_worker(
    db_pool: Arc<DbPool>,
    clickhouse_pool: Arc<ClickHousePool>,
    redis_pool: Arc<crate::app_state::RedisPool>,
    entitlements: Arc<dyn reiver_core::entitlements::EntitlementChecker>,
    moodeng_project_id: Option<Uuid>,
    config: Arc<Config>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<JoinHandle<()>> {
    // Validate cooldown_hours to ensure it's within a reasonable range
    // - Minimum: 1 hour (negative or zero values would cause unexpected behavior)
    // - Maximum: 168 hours (1 week) to prevent overflow in date arithmetic
    const MIN_COOLDOWN_HOURS: i64 = 1;
    const MAX_COOLDOWN_HOURS: i64 = 168; // 1 week
    let cooldown_hours = config
        .budget_alert_cooldown_hours
        .clamp(MIN_COOLDOWN_HOURS, MAX_COOLDOWN_HOURS);
    if config.budget_alert_cooldown_hours < MIN_COOLDOWN_HOURS {
        warn!(
            configured_value = config.budget_alert_cooldown_hours,
            effective_value = cooldown_hours,
            min_value = MIN_COOLDOWN_HOURS,
            "budget_alert_cooldown_hours was below minimum, clamping to {} hour(s)",
            MIN_COOLDOWN_HOURS
        );
    } else if config.budget_alert_cooldown_hours > MAX_COOLDOWN_HOURS {
        warn!(
            configured_value = config.budget_alert_cooldown_hours,
            effective_value = cooldown_hours,
            max_value = MAX_COOLDOWN_HOURS,
            "budget_alert_cooldown_hours exceeded maximum, clamping to {} hours",
            MAX_COOLDOWN_HOURS
        );
    }

    info!(
        cooldown_hours = cooldown_hours,
        "Starting billing worker (runs hourly)"
    );

    // Run every hour
    let mut interval = time::interval(time::Duration::from_secs(3600));

    let stripe_client = config.stripe_api_key.as_ref().map(|key| stripe::Client::new(key));

    let handle = tokio::spawn(async move {
        let billing_service = BillingService::new(
            db_pool.clone(),
            clickhouse_pool.clone(),
            entitlements.clone(),
            moodeng_project_id,
        );
        let meter_service = if let Some(ref key) = config.stripe_api_key {
            reiver_core::billing::MeterService::from_api_key(key, db_pool.clone())
        } else {
            reiver_core::billing::MeterService::noop()
        };

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let now = Utc::now();
                    debug!("Billing worker tick at {}", now);

                    if let Err(e) = check_budget_thresholds(&db_pool, &billing_service, entitlements.as_ref(), cooldown_hours).await {
                        error!("Failed to check budget thresholds: {}", e);
                    }

                    if let Err(e) = check_orphaned_stripe_records(&db_pool).await {
                        error!("Failed to check orphaned Stripe records: {}", e);
                    }

                    if let Err(e) = cleanup_resolved_orphaned_records(&db_pool).await {
                        error!("Failed to clean up orphaned records: {}", e);
                    }

                    if let Err(e) = check_stale_unresolved_orphaned_records(&db_pool).await {
                        error!("Failed to check stale unresolved orphaned records: {}", e);
                    }

                    if let Err(e) = reconcile_stuck_cancellations(&db_pool, stripe_client.as_ref()).await {
                        error!("Failed to reconcile stuck cancellations: {}", e);
                    }

                    if let Err(e) = report_watch_byok_usage(&db_pool, &redis_pool, &billing_service, entitlements.as_ref(), &meter_service).await {
                        error!("Failed to report Watch/BYOK usage to meter: {}", e);
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Billing worker received shutdown signal, stopping gracefully");
                        break;
                    }
                }
            }
        }
        info!("Billing worker stopped");
    });

    Ok(handle)
}


/// Check budget thresholds and send notifications.
///
/// # Arguments
/// * `db_pool` - Database connection pool
/// * `billing_service` - Billing service for budget queries
/// * `cooldown_hours` - Hours between sending the same type of budget alert
///
/// Alert deduplication logic:
/// - Threshold alerts: sent once per cooldown period, or when usage increases by 10%+ since last alert
/// - Exceeded alerts: sent once per cooldown period
/// - Alerts are reset at the start of each billing period
async fn check_budget_thresholds(
    db_pool: &DbPool,
    billing_service: &BillingService,
    entitlements: &dyn reiver_core::entitlements::EntitlementChecker,
    cooldown_hours: i64,
) -> Result<()> {
    debug!(
        cooldown_hours = cooldown_hours,
        "Checking budget thresholds"
    );

    let budgets: Vec<BudgetWithAlertHistory> = sqlx::query(
        r#"
        SELECT id, organization_id, monthly_budget_usd, alert_threshold_percent,
               last_threshold_alert_at, last_exceeded_alert_at, last_alert_percent
        FROM billing_budgets
        WHERE enabled = true
          AND project_id IS NULL
        "#,
    )
    .fetch_all(db_pool)
    .await
    .context("Failed to fetch budgets")?
    .into_iter()
    .map(|r| BudgetWithAlertHistory {
        id: r.get("id"),
        organization_id: r.get("organization_id"),
        monthly_budget_usd: r.get("monthly_budget_usd"),
        alert_threshold_percent: r.get("alert_threshold_percent"),
        last_threshold_alert_at: r.get("last_threshold_alert_at"),
        last_exceeded_alert_at: r.get("last_exceeded_alert_at"),
        last_alert_percent: r.get("last_alert_percent"),
    })
    .collect();

    if budgets.is_empty() {
        debug!("No enabled budgets found");
        return Ok(());
    }

    // Batch-fetch all data: 2 ClickHouse + 3 Postgres queries total instead of 4N.
    let org_ids: Vec<Uuid> = budgets.iter().map(|b| b.organization_id).collect();

    // 1. All project IDs for these orgs (1 Postgres query)
    let project_rows: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT id, organization_id FROM projects WHERE organization_id = ANY($1)",
    )
    .bind(&org_ids)
    .fetch_all(db_pool)
    .await
    .context("Failed to batch-fetch project IDs")?;

    let all_project_ids: Vec<Uuid> = project_rows.iter().map(|(id, _)| *id).collect();

    let mut org_project_ids: std::collections::HashMap<Uuid, Vec<Uuid>> =
        std::collections::HashMap::new();
    for (project_id, org_id) in &project_rows {
        org_project_ids
            .entry(*org_id)
            .or_default()
            .push(*project_id);
    }

    // 2. Batch Watch + BYOK usage from ClickHouse (2 queries)
    let watch_by_project = billing_service
        .batch_watch_usage(&all_project_ids)
        .await
        .context("Failed to batch-fetch Watch usage")?;
    let byok_by_project = billing_service
        .batch_byok_cost(&all_project_ids)
        .await
        .context("Failed to batch-fetch BYOK cost")?;

    // Now compute budget status per org from pre-fetched data.
    let now = Utc::now();
    let cooldown_duration = Duration::hours(cooldown_hours);

    for budget in &budgets {
        let project_ids = match org_project_ids.get(&budget.organization_id) {
            Some(ids) if !ids.is_empty() => ids,
            _ => continue,
        };

        let (mut spans, mut logs, mut metrics) = (0u64, 0u64, 0u64);
        let mut gw_cost_raw = 0.0f64;
        for pid in project_ids {
            if let Some(&(s, l, m)) = watch_by_project.get(pid) {
                spans += s;
                logs += l;
                metrics += m;
            }
            if let Some(gw) = byok_by_project.get(pid) {
                gw_cost_raw += gw.total_cost_usd;
            }
        }

        let tier = entitlements.get_config(budget.organization_id).await?;
        let traces_logs_price = tier.config.watch.traces_logs_per_gb_usd;
        let metrics_price = tier.config.watch.metrics_per_million_usd;
        let watch_cost = billing_service.calculate_cost(spans, logs, metrics, traces_logs_price, metrics_price);
        let gateway_rate = reiver_core::billing::credits::get_gateway_fee_rate(
            entitlements,
            budget.organization_id,
        )
        .await?;
        let gateway_fee = Decimal::from_f64_retain(gw_cost_raw)
            .unwrap_or(Decimal::ZERO)
            * gateway_rate;
        let estimated_cost = watch_cost + gateway_fee;

        let usage_percent = if budget.monthly_budget_usd > Decimal::ZERO {
            (estimated_cost / budget.monthly_budget_usd * Decimal::from(100))
                .to_f64()
                .unwrap_or(0.0)
        } else {
            0.0
        };

        let threshold_exceeded = usage_percent >= budget.alert_threshold_percent as f64;
        let budget_exceeded = estimated_cost >= budget.monthly_budget_usd;

        let current_percent = usage_percent as i32;
        let should_send = should_send_alert(
            budget_exceeded,
            threshold_exceeded,
            current_percent,
            budget.last_exceeded_alert_at,
            budget.last_threshold_alert_at,
            budget.last_alert_percent,
            now,
            cooldown_duration,
        );

        if !should_send {
            continue;
        }

        // Get notification channels for this organization
        let channels: Vec<Uuid> = match sqlx::query(
            r#"
            SELECT nc.id
            FROM notification_channels nc
            JOIN projects p ON p.id = nc.project_id
            WHERE p.organization_id = $1
              AND nc.enabled = true
            LIMIT 5
            "#,
        )
        .bind(budget.organization_id)
        .fetch_all(db_pool)
        .await
        {
            Ok(rows) => rows.into_iter().map(|r| r.get("id")).collect(),
            Err(e) => {
                warn!(
                    organization_id = %budget.organization_id,
                    error = %e,
                    "Failed to fetch notification channels for budget alert"
                );
                continue;
            }
        };

        if channels.is_empty() {
            debug!(
                "No notification channels for org {}, skipping alert",
                budget.organization_id
            );
            continue;
        }

        let alert_state = AlertState::Firing;

        let message = if budget_exceeded {
            format!(
                "Budget exceeded: ${:.2} spent of ${:.2} budget ({:.1}%)",
                estimated_cost, budget.monthly_budget_usd, usage_percent
            )
        } else {
            format!(
                "Budget threshold reached: ${:.2} spent of ${:.2} budget ({:.1}% of {}% threshold)",
                estimated_cost,
                budget.monthly_budget_usd,
                usage_percent,
                budget.alert_threshold_percent
            )
        };

        let notification = AlertNotification {
            alert_id: budget.id,
            rule_id: budget.id,
            rule_name: format!("Budget Alert: {}", message),
            state: alert_state,
            value: Some(usage_percent),
            threshold: Some(budget.alert_threshold_percent as f64),
            compare_op: "above".to_string(),
            labels: std::collections::BTreeMap::new(),
            annotations: std::collections::BTreeMap::new(),
            fired_at: Some(Utc::now()),
            resolved_at: None,
            is_missing: false,
        };

        let mut alert_sent = false;
        for channel_id in channels {
            match load_notification_channel(db_pool, channel_id).await {
                Ok(Some(channel)) => {
                    if let Err(e) = send_notification(&channel, &notification, None).await {
                        warn!(
                            "Failed to send budget alert to channel {}: {}",
                            channel_id, e
                        );
                    } else {
                        info!("Sent budget alert to channel {}", channel_id);
                        alert_sent = true;
                    }
                }
                Ok(None) => {
                    debug!("Notification channel {} not found", channel_id);
                }
                Err(e) => {
                    warn!("Failed to load notification channel {}: {}", channel_id, e);
                }
            }
        }

        // Record that alert was sent to prevent duplicates
        if alert_sent {
            if let Err(e) = billing_service
                .record_budget_alert_sent(budget.id, budget_exceeded, current_percent)
                .await
            {
                warn!(
                    "Failed to record budget alert sent for budget {}: {}",
                    budget.id, e
                );
            }
        }
    }

    Ok(())
}

/// Check for unresolved orphaned Stripe records (invoices and subscriptions).
///
/// Orphaned records occur when webhook events reference customers that don't exist
/// in our database. This could indicate:
/// - Data sync issues between Stripe and our system
/// - Customers deleted without canceling subscriptions
/// - Manual Stripe-side operations not linked to our system
///
/// This function logs warnings and metrics for monitoring systems to alert on.
async fn check_orphaned_stripe_records(db_pool: &DbPool) -> Result<()> {
    debug!("Checking for orphaned Stripe records");

    // Count unresolved orphaned invoices
    let orphaned_invoices: Option<i64> =
        sqlx::query_scalar("SELECT COUNT(*) FROM orphaned_invoices WHERE resolved = false")
            .fetch_optional(db_pool)
            .await
            .context("Failed to count orphaned invoices")?;

    // Count unresolved orphaned subscriptions
    let orphaned_subscriptions: Option<i64> =
        sqlx::query_scalar("SELECT COUNT(*) FROM orphaned_subscriptions WHERE resolved = false")
            .fetch_optional(db_pool)
            .await
            .context("Failed to count orphaned subscriptions")?;

    let invoice_count = orphaned_invoices.unwrap_or(0);
    let subscription_count = orphaned_subscriptions.unwrap_or(0);

    // Log at appropriate level based on severity
    if invoice_count > 0 || subscription_count > 0 {
        // Get details of recent orphaned records for investigation
        let recent_orphaned_invoices: Vec<(String, String, i64)> = match sqlx::query_as(
            r#"
            SELECT stripe_invoice_id, stripe_customer_id, total_cents
            FROM orphaned_invoices
            WHERE resolved = false
            ORDER BY created_at DESC
            LIMIT 5
            "#,
        )
        .fetch_all(db_pool)
        .await
        {
            Ok(rows) => rows,
            Err(e) => {
                warn!(error = %e, "Failed to fetch recent orphaned invoices for logging");
                vec![]
            }
        };

        let recent_orphaned_subscriptions: Vec<(String, String, String)> = match sqlx::query_as(
            r#"
            SELECT stripe_subscription_id, stripe_customer_id, status
            FROM orphaned_subscriptions
            WHERE resolved = false
            ORDER BY created_at DESC
            LIMIT 5
            "#,
        )
        .fetch_all(db_pool)
        .await
        {
            Ok(rows) => rows,
            Err(e) => {
                warn!(error = %e, "Failed to fetch recent orphaned subscriptions for logging");
                vec![]
            }
        };

        // Log warning with actionable information
        warn!(
            orphaned_invoices = invoice_count,
            orphaned_subscriptions = subscription_count,
            recent_invoice_ids = ?recent_orphaned_invoices.iter().map(|(id, _, _)| id).collect::<Vec<_>>(),
            recent_subscription_ids = ?recent_orphaned_subscriptions.iter().map(|(id, _, _)| id).collect::<Vec<_>>(),
            "ORPHANED STRIPE RECORDS DETECTED: {} invoices and {} subscriptions require manual investigation. \
             Query orphaned_invoices and orphaned_subscriptions tables for details.",
            invoice_count, subscription_count
        );

        // Log individual record details at debug level for troubleshooting
        for (invoice_id, customer_id, total_cents) in &recent_orphaned_invoices {
            debug!(
                invoice_id = %invoice_id,
                customer_id = %customer_id,
                total_cents = total_cents,
                "Orphaned invoice details"
            );
        }

        for (sub_id, customer_id, status) in &recent_orphaned_subscriptions {
            debug!(
                subscription_id = %sub_id,
                customer_id = %customer_id,
                status = %status,
                "Orphaned subscription details"
            );
        }
    } else {
        debug!("No orphaned Stripe records found");
    }

    Ok(())
}

/// Clean up orphaned records after retention period.
///
/// This prevents indefinite growth of the orphaned_invoices and orphaned_subscriptions
/// tables while maintaining audit trail during the retention period.
///
/// Records are deleted if:
/// 1. They are resolved AND were resolved more than ORPHAN_RECORD_RETENTION_DAYS ago, OR
/// 2. They are unresolved AND were created more than ORPHAN_RECORD_MAX_RETENTION_DAYS ago
///    (PII retention compliance - webhook_payload may contain customer email, name, address)
async fn cleanup_resolved_orphaned_records(db_pool: &DbPool) -> Result<()> {
    debug!("Cleaning up orphaned records");

    // Delete resolved orphaned invoices older than retention period
    // Uses resolved_at (not updated_at) since we're cleaning up based on when they were resolved
    let resolved_invoices_deleted = sqlx::query(
        r#"
        DELETE FROM orphaned_invoices 
        WHERE resolved = true 
          AND resolved_at < NOW() - INTERVAL '1 day' * $1
        "#,
    )
    .bind(ORPHAN_RECORD_RETENTION_DAYS)
    .execute(db_pool)
    .await
    .context("Failed to clean up resolved orphaned invoices")?;

    // Delete resolved orphaned subscriptions older than retention period
    let resolved_subscriptions_deleted = sqlx::query(
        r#"
        DELETE FROM orphaned_subscriptions 
        WHERE resolved = true 
          AND resolved_at < NOW() - INTERVAL '1 day' * $1
        "#,
    )
    .bind(ORPHAN_RECORD_RETENTION_DAYS)
    .execute(db_pool)
    .await
    .context("Failed to clean up resolved orphaned subscriptions")?;

    // Delete very old unresolved orphaned invoices (PII retention compliance)
    // These records contain webhook_payload with customer PII that must be purged
    let stale_invoices_deleted = sqlx::query(
        r#"
        DELETE FROM orphaned_invoices 
        WHERE resolved = false 
          AND created_at < NOW() - INTERVAL '1 day' * $1
        "#,
    )
    .bind(ORPHAN_RECORD_MAX_RETENTION_DAYS)
    .execute(db_pool)
    .await
    .context("Failed to clean up stale unresolved orphaned invoices")?;

    // Delete very old unresolved orphaned subscriptions (PII retention compliance)
    let stale_subscriptions_deleted = sqlx::query(
        r#"
        DELETE FROM orphaned_subscriptions 
        WHERE resolved = false 
          AND created_at < NOW() - INTERVAL '1 day' * $1
        "#,
    )
    .bind(ORPHAN_RECORD_MAX_RETENTION_DAYS)
    .execute(db_pool)
    .await
    .context("Failed to clean up stale unresolved orphaned subscriptions")?;

    let resolved_invoice_count = resolved_invoices_deleted.rows_affected();
    let resolved_subscription_count = resolved_subscriptions_deleted.rows_affected();
    let stale_invoice_count = stale_invoices_deleted.rows_affected();
    let stale_subscription_count = stale_subscriptions_deleted.rows_affected();

    if resolved_invoice_count > 0 || resolved_subscription_count > 0 {
        info!(
            invoices = resolved_invoice_count,
            subscriptions = resolved_subscription_count,
            retention_days = ORPHAN_RECORD_RETENTION_DAYS,
            "Cleaned up resolved orphaned records"
        );
    }

    // Log stale unresolved record deletions at warning level since these indicate
    // records that were never investigated and are being purged for PII compliance
    if stale_invoice_count > 0 || stale_subscription_count > 0 {
        warn!(
            invoices = stale_invoice_count,
            subscriptions = stale_subscription_count,
            max_retention_days = ORPHAN_RECORD_MAX_RETENTION_DAYS,
            "PII COMPLIANCE: Deleted stale unresolved orphaned records that exceeded max retention period. \
             These records were never investigated and contained customer PII in webhook_payload."
        );
    }

    if resolved_invoice_count == 0
        && resolved_subscription_count == 0
        && stale_invoice_count == 0
        && stale_subscription_count == 0
    {
        debug!("No orphaned records to clean up");
    }

    Ok(())
}

/// Check for stale unresolved orphaned records and log escalation warnings.
///
/// Records older than ORPHAN_RECORD_ESCALATION_THRESHOLD_DAYS that haven't been
/// resolved likely indicate systemic issues that need attention:
/// - Customer data sync problems
/// - Webhooks from test environments
/// - Manual Stripe operations not linked to our system
///
/// This function logs warnings to trigger alerting/monitoring systems.
async fn check_stale_unresolved_orphaned_records(db_pool: &DbPool) -> Result<()> {
    debug!("Checking for stale unresolved orphaned records");

    // Count stale unresolved invoices (older than escalation threshold)
    let stale_invoices: Option<i64> = sqlx::query_scalar(
        r#"
        SELECT COUNT(*) FROM orphaned_invoices 
        WHERE resolved = false 
          AND created_at < NOW() - INTERVAL '1 day' * $1
        "#,
    )
    .bind(ORPHAN_RECORD_ESCALATION_THRESHOLD_DAYS)
    .fetch_optional(db_pool)
    .await
    .context("Failed to count stale orphaned invoices")?;

    // Count stale unresolved subscriptions
    let stale_subscriptions: Option<i64> = sqlx::query_scalar(
        r#"
        SELECT COUNT(*) FROM orphaned_subscriptions 
        WHERE resolved = false 
          AND created_at < NOW() - INTERVAL '1 day' * $1
        "#,
    )
    .bind(ORPHAN_RECORD_ESCALATION_THRESHOLD_DAYS)
    .fetch_optional(db_pool)
    .await
    .context("Failed to count stale orphaned subscriptions")?;

    let invoice_count = stale_invoices.unwrap_or(0);
    let subscription_count = stale_subscriptions.unwrap_or(0);

    if invoice_count > 0 || subscription_count > 0 {
        // Get oldest unresolved records for context
        let oldest_invoice: Option<(String, chrono::DateTime<Utc>)> = sqlx::query_as(
            r#"
            SELECT stripe_invoice_id, created_at
            FROM orphaned_invoices
            WHERE resolved = false
            ORDER BY created_at ASC
            LIMIT 1
            "#,
        )
        .fetch_optional(db_pool)
        .await
        .unwrap_or(None);

        let oldest_subscription: Option<(String, chrono::DateTime<Utc>)> = sqlx::query_as(
            r#"
            SELECT stripe_subscription_id, created_at
            FROM orphaned_subscriptions
            WHERE resolved = false
            ORDER BY created_at ASC
            LIMIT 1
            "#,
        )
        .fetch_optional(db_pool)
        .await
        .unwrap_or(None);

        let oldest_invoice_days = oldest_invoice
            .as_ref()
            .map(|(_, created_at)| Utc::now().signed_duration_since(*created_at).num_days())
            .unwrap_or(0);

        let oldest_subscription_days = oldest_subscription
            .as_ref()
            .map(|(_, created_at)| Utc::now().signed_duration_since(*created_at).num_days())
            .unwrap_or(0);

        error!(
            stale_invoices = invoice_count,
            stale_subscriptions = subscription_count,
            threshold_days = ORPHAN_RECORD_ESCALATION_THRESHOLD_DAYS,
            oldest_invoice_id = ?oldest_invoice.as_ref().map(|(id, _)| id),
            oldest_invoice_age_days = oldest_invoice_days,
            oldest_subscription_id = ?oldest_subscription.as_ref().map(|(id, _)| id),
            oldest_subscription_age_days = oldest_subscription_days,
            "ESCALATION: Orphaned Stripe records require urgent attention. \
             {} invoice(s) and {} subscription(s) unresolved for more than {} days. \
             These records contain PII and indicate customer data sync issues. \
             Query orphaned_invoices and orphaned_subscriptions tables and resolve manually.",
            invoice_count, subscription_count, ORPHAN_RECORD_ESCALATION_THRESHOLD_DAYS
        );
    } else {
        debug!("No stale unresolved orphaned records found");
    }

    Ok(())
}

/// Threshold for how long a subscription can be in pending_cancellation state
/// before it's considered stuck and auto-reconciled.
const PENDING_CANCELLATION_THRESHOLD_MINUTES: i64 = 5;

/// Max stuck subscriptions to reconcile per tick to avoid hammering Stripe.
const MAX_RECONCILIATIONS_PER_TICK: usize = 10;

/// Auto-reconcile subscriptions stuck in `pending_cancellation` by fetching
/// the live state from Stripe and updating the local DB accordingly.
///
/// If no Stripe client is available (key not configured), falls back to
/// log-only warnings.
async fn reconcile_stuck_cancellations(
    db_pool: &DbPool,
    stripe_client: Option<&stripe::Client>,
) -> Result<()> {
    debug!("Checking for stuck pending_cancellation subscriptions");

    let stuck: Vec<(Uuid, String, DateTime<Utc>)> = sqlx::query_as(
        r#"
        SELECT organization_id, stripe_subscription_id, updated_at
        FROM stripe_subscriptions
        WHERE status = 'pending_cancellation'
          AND updated_at < NOW() - INTERVAL '1 minute' * $1
        ORDER BY updated_at ASC
        LIMIT $2
        "#,
    )
    .bind(PENDING_CANCELLATION_THRESHOLD_MINUTES)
    .bind(MAX_RECONCILIATIONS_PER_TICK as i64)
    .fetch_all(db_pool)
    .await
    .context("Failed to check for stuck pending_cancellation subscriptions")?;

    if stuck.is_empty() {
        debug!("No stuck pending_cancellation subscriptions found");
        return Ok(());
    }

    let client = match stripe_client {
        Some(c) => c,
        None => {
            for (org_id, sub_id, updated_at) in &stuck {
                let mins = Utc::now().signed_duration_since(*updated_at).num_minutes();
                warn!(
                    organization_id = %org_id,
                    subscription_id = %sub_id,
                    minutes_stuck = mins,
                    "Stuck subscription detected but no Stripe client configured for auto-reconciliation"
                );
            }
            return Ok(());
        }
    };

    for (org_id, sub_id_str, updated_at) in &stuck {
        let mins = Utc::now().signed_duration_since(*updated_at).num_minutes();

        match stripe_billing::subscription::RetrieveSubscription::new(sub_id_str.as_str())
            .send(client)
            .await
        {
            Ok(sub) => {
                let new_status = sub.status.as_str();

                match sqlx::query(
                    "UPDATE stripe_subscriptions SET status = $1, updated_at = NOW() \
                     WHERE stripe_subscription_id = $2",
                )
                .bind(new_status)
                .bind(sub_id_str)
                .execute(db_pool)
                .await
                {
                    Ok(_) => {
                        warn!(
                            organization_id = %org_id,
                            subscription_id = %sub_id_str,
                            minutes_stuck = mins,
                            new_status = new_status,
                            "Auto-reconciled stuck subscription"
                        );
                    }
                    Err(e) => {
                        warn!(
                            organization_id = %org_id,
                            subscription_id = %sub_id_str,
                            error = %e,
                            "Failed to update reconciled subscription status in DB"
                        );
                    }
                }
            }
            Err(e) => {
                warn!(
                    organization_id = %org_id,
                    subscription_id = %sub_id_str,
                    error = %e,
                    "Failed to retrieve subscription from Stripe for reconciliation"
                );
            }
        }
    }

    Ok(())
}

/// Compute the charge amount, billable flag, and description from Watch and BYOK totals.
/// Returns `(amount_to_store, billable, description)`.
fn compute_charge_amount(
    watch_amount: Decimal,
    byok_fees: Decimal,
    period_start: NaiveDate,
) -> (Decimal, bool, String) {
    let total_amount = watch_amount + byok_fees;
    let min_charge = Decimal::new(1, 2); // $0.01
    let billable = total_amount >= min_charge;
    let amount_to_store = if billable { total_amount } else { Decimal::ZERO };

    let mut desc_parts = Vec::new();
    if !billable {
        desc_parts.push("Below minimum".to_string());
    } else {
        if watch_amount > Decimal::ZERO {
            desc_parts.push(format!("Watch Usage ${}", watch_amount.round_dp(2)));
        }
        if byok_fees > Decimal::ZERO {
            desc_parts.push(format!("BYOK Fees ${}", byok_fees.round_dp(2)));
        }
    }
    let description = format!(
        "Reiver {} — {}",
        period_start.format("%B %Y"),
        desc_parts.join(", "),
    );

    (amount_to_store, billable, description)
}

/// Compute the previous calendar month's billing period boundaries.
/// Returns `(period_start, period_end)` where both are `NaiveDate`.
/// `period_end` is the 1st of the current month (exclusive upper bound).
#[allow(dead_code)]
fn previous_billing_period(now: DateTime<Utc>) -> (NaiveDate, NaiveDate) {
    let first_of_current =
        NaiveDate::from_ymd_opt(now.year(), now.month(), 1).expect("Day 1 is valid");
    let prev_month_start = if now.month() == 1 {
        NaiveDate::from_ymd_opt(now.year() - 1, 12, 1).expect("Valid date")
    } else {
        NaiveDate::from_ymd_opt(now.year(), now.month() - 1, 1).expect("Valid date")
    };
    (prev_month_start, first_of_current)
}

/// Report Watch observability usage to Stripe Meters as raw GB.
///
/// Called every hour. Queries ClickHouse for bytes ingested since the last
/// report (tracked in Redis as an RFC 3339 timestamp) and sends raw GB
/// per org to the `observability_gb` meter. Stripe's graduated pricing
/// handles allotment (first X GB at $0) and overage ($0.25/GB after).
async fn report_watch_byok_usage(
    db_pool: &DbPool,
    redis_pool: &crate::app_state::RedisPool,
    billing_service: &BillingService,
    _entitlements: &dyn reiver_core::entitlements::EntitlementChecker,
    meter_service: &reiver_core::billing::MeterService,
) -> Result<()> {
    let redis_key = "billing:last_watch_byok_report";
    let now = Utc::now();

    let last_report: Option<String> = {
        let mut conn = redis_pool.get().await.context("Redis connection failed")?;
        redis::cmd("GET")
            .arg(redis_key)
            .query_async::<Option<String>>(&mut *conn)
            .await
            .unwrap_or(None)
    };

    let period_start = last_report
        .and_then(|s| {
            chrono::DateTime::parse_from_rfc3339(&s)
                .ok()
                .map(|dt| dt.with_timezone(&Utc))
        })
        .unwrap_or_else(|| now - chrono::Duration::hours(1));

    if period_start >= now {
        debug!("Watch/BYOK usage already reported up to {}", period_start);
        return Ok(());
    }

    let start_str = period_start.format("%Y-%m-%d %H:%M:%S").to_string();
    let end_str = now.format("%Y-%m-%d %H:%M:%S").to_string();
    let hour_ts = now.format("%Y%m%d%H").to_string();

    debug!(
        period_start = %start_str,
        period_end = %end_str,
        "Reporting Watch observability GB to Stripe Meters"
    );

    let orgs: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM organizations")
        .fetch_all(db_pool)
        .await
        .context("Failed to list organizations")?;

    for org_id in orgs {
        let (spans_bytes, logs_bytes, _metrics_count) = billing_service
            .get_watch_usage_for_period(org_id, &start_str, &end_str)
            .await
            .unwrap_or((0, 0, 0));

        let total_bytes = spans_bytes + logs_bytes;
        if total_bytes == 0 {
            continue;
        }

        // Convert bytes to whole GB (round up so any partial GB is counted)
        let gb = ((total_bytes as f64) / 1_000_000_000.0).ceil() as i64;
        if gb <= 0 {
            continue;
        }

        let idempotency_key = format!("{}-obs-{}", org_id, hour_ts);
        meter_service.record_observability_gb(org_id, gb, idempotency_key);

        debug!(
            organization_id = %org_id,
            bytes = total_bytes,
            gb = gb,
            "Reported observability GB to meter"
        );
    }

    let mut conn = redis_pool.get().await.context("Redis connection failed")?;
    let _: () = redis::cmd("SET")
        .arg(redis_key)
        .arg(now.to_rfc3339())
        .query_async(&mut *conn)
        .await
        .context("Failed to update last-report watermark")?;

    debug!("Watch observability usage reporting complete");
    Ok(())
}

pub async fn generate_combined_charge(
    db_pool: &DbPool,
    billing_service: &BillingService,
    entitlements: &dyn reiver_core::entitlements::EntitlementChecker,
    org_id: Uuid,
    period_start: NaiveDate,
    period_end: NaiveDate,
) -> Result<()> {
    let start_str = period_start.format("%Y-%m-%d").to_string();
    let end_str = period_end.format("%Y-%m-%d").to_string();

    // --- Watch usage from ClickHouse ---
    let (spans_bytes, logs_bytes, metrics_count) = billing_service
        .get_watch_usage_for_period(org_id, &start_str, &end_str)
        .await
        .context("Failed to query Watch usage from ClickHouse")?;

    let tier = entitlements.get_config(org_id).await?;
    let traces_logs_price = tier.config.watch.traces_logs_per_gb_usd;
    let metrics_price = tier.config.watch.metrics_per_million_usd;
    let watch_amount = billing_service.calculate_cost(
        spans_bytes,
        logs_bytes,
        metrics_count,
        traces_logs_price,
        metrics_price,
    );

    // --- BYOK fees from ClickHouse llm_cost_daily ---
    let (byok_fees, fee_count) = billing_service
        .get_byok_fees(org_id, &start_str, &end_str)
        .await
        .context("Failed to compute BYOK fees from ClickHouse")?;
    let fee_count = fee_count as i64;

    let line_items = serde_json::json!({
        "watch_usage": {
            "amount_usd": watch_amount.to_string(),
            "spans_ingested_bytes": spans_bytes,
            "logs_ingested_bytes": logs_bytes,
            "metrics_count": metrics_count,
            "traces_logs_per_gb_usd": traces_logs_price.to_string(),
            "metrics_per_million_usd": metrics_price.to_string(),
        },
        "flow_byok_fees": {
            "amount_usd": byok_fees.to_string(),
            "fee_count": fee_count,
        },
    });

    let (amount_to_store, billable, description) =
        compute_charge_amount(watch_amount, byok_fees, period_start);

    sqlx::query(
        r#"
        INSERT INTO pending_charges
            (organization_id, charge_type, billing_period_start, billing_period_end,
             amount_usd, description, line_items)
        VALUES ($1, 'platform_usage', $2, $3, $4, $5, $6)
        ON CONFLICT (organization_id, billing_period_start) DO NOTHING
        "#,
    )
    .bind(org_id)
    .bind(period_start)
    .bind(period_end)
    .bind(amount_to_store)
    .bind(&description)
    .bind(line_items)
    .execute(db_pool)
    .await
    .context("Failed to insert combined pending charge")?;

    if billable {
        info!(organization_id = %org_id, amount = %amount_to_store, "Combined pending charge created");
    } else {
        debug!(organization_id = %org_id, "Charge below minimum, recorded as $0");
    }
    Ok(())
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    // =====================================================================
    // is_different_billing_period
    // =====================================================================

    #[test]
    fn test_same_month_same_year() {
        let a = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        let b = Utc.with_ymd_and_hms(2026, 6, 30, 23, 59, 59).unwrap();
        assert!(!is_different_billing_period(a, b));
    }

    #[test]
    fn test_different_month_same_year() {
        let a = Utc.with_ymd_and_hms(2026, 5, 15, 0, 0, 0).unwrap();
        let b = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        assert!(is_different_billing_period(a, b));
    }

    #[test]
    fn test_december_to_january() {
        let dec = Utc.with_ymd_and_hms(2025, 12, 31, 23, 59, 59).unwrap();
        let jan = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        assert!(is_different_billing_period(dec, jan));
    }

    #[test]
    fn test_same_month_different_year() {
        let a = Utc.with_ymd_and_hms(2025, 6, 15, 0, 0, 0).unwrap();
        let b = Utc.with_ymd_and_hms(2026, 6, 15, 0, 0, 0).unwrap();
        assert!(is_different_billing_period(a, b));
    }

    #[test]
    fn test_identical_timestamps() {
        let ts = Utc.with_ymd_and_hms(2026, 3, 15, 12, 0, 0).unwrap();
        assert!(!is_different_billing_period(ts, ts));
    }

    // =====================================================================
    // previous_billing_period
    // =====================================================================

    #[test]
    fn test_prev_period_february() {
        let now = Utc.with_ymd_and_hms(2026, 2, 15, 10, 0, 0).unwrap();
        let (start, end) = previous_billing_period(now);
        assert_eq!(start, NaiveDate::from_ymd_opt(2026, 1, 1).unwrap());
        assert_eq!(end, NaiveDate::from_ymd_opt(2026, 2, 1).unwrap());
    }

    #[test]
    fn test_prev_period_january_wraps_to_december() {
        let now = Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap();
        let (start, end) = previous_billing_period(now);
        assert_eq!(start, NaiveDate::from_ymd_opt(2025, 12, 1).unwrap());
        assert_eq!(end, NaiveDate::from_ymd_opt(2026, 1, 1).unwrap());
    }

    #[test]
    fn test_prev_period_march_first() {
        let now = Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0).unwrap();
        let (start, end) = previous_billing_period(now);
        assert_eq!(start, NaiveDate::from_ymd_opt(2026, 2, 1).unwrap());
        assert_eq!(end, NaiveDate::from_ymd_opt(2026, 3, 1).unwrap());
    }

    #[test]
    fn test_prev_period_leap_year() {
        // 2028 is a leap year. March 1 -> Feb period.
        let now = Utc.with_ymd_and_hms(2028, 3, 1, 0, 0, 0).unwrap();
        let (start, end) = previous_billing_period(now);
        assert_eq!(start, NaiveDate::from_ymd_opt(2028, 2, 1).unwrap());
        assert_eq!(end, NaiveDate::from_ymd_opt(2028, 3, 1).unwrap());
    }

    // =====================================================================
    // compute_charge_amount
    // =====================================================================

    #[test]
    fn test_charge_both_components() {
        let period = NaiveDate::from_ymd_opt(2026, 5, 1).unwrap();
        let (amount, billable, desc) =
            compute_charge_amount(Decimal::new(5_00, 2), Decimal::new(3_00, 2), period);
        assert_eq!(amount, Decimal::new(8_00, 2)); // $8.00
        assert!(billable);
        assert!(desc.contains("Watch Usage"));
        assert!(desc.contains("BYOK Fees"));
        assert!(desc.contains("May 2026"));
    }

    #[test]
    fn test_charge_below_minimum() {
        let period = NaiveDate::from_ymd_opt(2026, 6, 1).unwrap();
        let (amount, billable, desc) =
            compute_charge_amount(Decimal::new(5, 3), Decimal::ZERO, period); // $0.005
        assert_eq!(amount, Decimal::ZERO);
        assert!(!billable);
        assert!(desc.contains("Below minimum"));
    }

    #[test]
    fn test_charge_byok_only() {
        let period = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let (amount, billable, desc) =
            compute_charge_amount(Decimal::ZERO, Decimal::new(2, 2), period); // $0.02
        assert_eq!(amount, Decimal::new(2, 2));
        assert!(billable);
        assert!(!desc.contains("Watch Usage"));
        assert!(desc.contains("BYOK Fees"));
        assert!(desc.contains("January 2026"));
    }

    #[test]
    fn test_charge_zero_both() {
        let period = NaiveDate::from_ymd_opt(2026, 7, 1).unwrap();
        let (amount, billable, _desc) =
            compute_charge_amount(Decimal::ZERO, Decimal::ZERO, period);
        assert_eq!(amount, Decimal::ZERO);
        assert!(!billable);
    }

    // =====================================================================
    // should_send_alert
    // =====================================================================

    fn dt(y: i32, m: u32, d: u32, h: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, 0, 0).unwrap()
    }

    #[test]
    fn test_alert_exceeded_never_alerted() {
        assert!(should_send_alert(
            true, true, 110, None, None, None,
            dt(2026, 6, 10, 12), Duration::hours(24),
        ));
    }

    #[test]
    fn test_alert_exceeded_within_cooldown() {
        let now = dt(2026, 6, 10, 12);
        let last = dt(2026, 6, 10, 11); // 1 hour ago
        assert!(!should_send_alert(
            true, true, 110, Some(last), None, None,
            now, Duration::hours(24),
        ));
    }

    #[test]
    fn test_alert_exceeded_past_cooldown() {
        let now = dt(2026, 6, 10, 12);
        let last = dt(2026, 6, 9, 11); // 25 hours ago
        assert!(should_send_alert(
            true, true, 110, Some(last), None, None,
            now, Duration::hours(24),
        ));
    }

    #[test]
    fn test_alert_exceeded_new_billing_period() {
        let now = dt(2026, 6, 1, 0);
        let last = dt(2026, 5, 31, 23); // previous month, within cooldown
        assert!(should_send_alert(
            true, true, 110, Some(last), None, None,
            now, Duration::hours(24),
        ));
    }

    #[test]
    fn test_alert_threshold_never_alerted() {
        assert!(should_send_alert(
            false, true, 85, None, None, None,
            dt(2026, 6, 10, 12), Duration::hours(24),
        ));
    }

    #[test]
    fn test_alert_threshold_within_cooldown_small_increase() {
        let now = dt(2026, 6, 10, 12);
        let last = dt(2026, 6, 10, 11); // 1h ago
        // Usage went from 80% to 85% -- increase of 5%, below re-alert threshold of 10%
        assert!(!should_send_alert(
            false, true, 85, None, Some(last), Some(80),
            now, Duration::hours(24),
        ));
    }

    #[test]
    fn test_alert_threshold_within_cooldown_big_increase() {
        let now = dt(2026, 6, 10, 12);
        let last = dt(2026, 6, 10, 11); // 1h ago
        // Usage went from 80% to 95% -- increase of 15%, above re-alert threshold of 10%
        assert!(should_send_alert(
            false, true, 95, None, Some(last), Some(80),
            now, Duration::hours(24),
        ));
    }

    #[test]
    fn test_alert_threshold_within_cooldown_no_prev_percent() {
        let now = dt(2026, 6, 10, 12);
        let last = dt(2026, 6, 10, 11);
        assert!(!should_send_alert(
            false, true, 85, None, Some(last), None,
            now, Duration::hours(24),
        ));
    }

    #[test]
    fn test_alert_threshold_new_billing_period() {
        let now = dt(2026, 6, 1, 0);
        let last = dt(2026, 5, 31, 23);
        assert!(should_send_alert(
            false, true, 85, None, Some(last), Some(80),
            now, Duration::hours(24),
        ));
    }

    #[test]
    fn test_alert_neither_exceeded_nor_threshold() {
        assert!(!should_send_alert(
            false, false, 50, None, None, None,
            dt(2026, 6, 10, 12), Duration::hours(24),
        ));
    }
}
