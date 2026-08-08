//! Billing service for usage queries and cost calculation.
//!
//! # Security: Defense-in-Depth Validation Pattern
//!
//! This module implements **defense-in-depth validation** where input validation
//! is performed at multiple layers:
//!
//! 1. **API Layer** ([`crate::api::billing`]): First line of defense, validates user input
//!    and returns user-friendly error messages. Catches most invalid inputs early.
//!
//! 2. **Service Layer** (this module): Second line of defense, re-validates inputs
//!    even though the API layer should have already validated them.
//!
//! ## Why Duplicate Validation?
//!
//! This pattern provides several security benefits:
//!
//! - **Future-proofing**: If someone adds a new API endpoint that calls the service
//!   directly without proper validation, the service will still catch invalid inputs.
//!
//! - **Internal API protection**: The service may be called from background workers,
//!   CLI tools, or internal APIs that bypass the HTTP API layer.
//!
//! - **Defense against bugs**: If a bug in the API layer allows invalid data through,
//!   the service layer provides a safety net.
//!
//! - **Clear error boundaries**: Each layer is responsible for its own correctness
//!   and doesn't rely on assumptions about upstream validation.
//!
//! ## Trade-offs
//!
//! - **Performance**: Minimal impact - validation is cheap compared to database operations.
//! - **Maintenance**: Validation logic must be kept in sync between layers.
//! - **Clarity**: Each function documents its validation requirements explicitly.
//!
//! ## Pattern Usage
//!
//! Functions like `create_budget` and `update_budget` demonstrate this pattern:
//!
//! ```ignore
//! // Defense-in-depth: validate inputs even though API layer should validate
//! if monthly_budget_usd <= Decimal::ZERO {
//!     anyhow::bail!("Budget amount must be greater than zero");
//! }
//! ```

use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use sqlx::Row;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use crate::clickhouse_db::ClickHousePool;
use crate::db::DbPool;
use crate::entitlements::checker::EntitlementChecker;

use super::credits;
use super::types::*;
use super::utils::build_uuid_in_clause;

/// Service for billing-related operations.
pub struct BillingService {
    db: Arc<DbPool>,
    clickhouse: Arc<ClickHousePool>,
    entitlements: Arc<dyn EntitlementChecker>,
    moodeng_project_id: Option<Uuid>,
}

impl BillingService {
    /// Create a new billing service.
    pub fn new(
        db: Arc<DbPool>,
        clickhouse: Arc<ClickHousePool>,
        entitlements: Arc<dyn EntitlementChecker>,
        moodeng_project_id: Option<Uuid>,
    ) -> Self {
        Self {
            db,
            clickhouse,
            entitlements,
            moodeng_project_id,
        }
    }

    // =========================================================================
    // Budget Operations
    // =========================================================================

    /// Get budget for an organization (org-wide budget).
    pub async fn get_org_budget(&self, organization_id: Uuid) -> Result<Option<Budget>> {
        let row = sqlx::query(
            r#"
            SELECT id, organization_id, project_id, monthly_budget_usd,
                   alert_threshold_percent, enabled, created_by, created_at, updated_at,
                   last_threshold_alert_at, last_exceeded_alert_at, last_alert_percent
            FROM billing_budgets
            WHERE organization_id = $1
              AND project_id IS NULL
            "#,
        )
        .bind(organization_id)
        .fetch_optional(self.db.as_ref())
        .await
        .context("Failed to fetch org budget")?;

        Ok(row.map(|r| Budget {
            id: r.get("id"),
            organization_id: r.get("organization_id"),
            project_id: r.get("project_id"),
            monthly_budget_usd: r.get("monthly_budget_usd"),
            alert_threshold_percent: r.get("alert_threshold_percent"),
            enabled: r.get("enabled"),
            created_by: r.get("created_by"),
            created_at: r.get("created_at"),
            updated_at: r.get("updated_at"),
            last_threshold_alert_at: r.get("last_threshold_alert_at"),
            last_exceeded_alert_at: r.get("last_exceeded_alert_at"),
            last_alert_percent: r.get("last_alert_percent"),
        }))
    }

    /// Create a budget.
    ///
    /// # Validation (defense-in-depth)
    /// - `monthly_budget_usd` must be > 0 and <= 9,999,999.99
    /// - `alert_threshold_percent` must be between 1 and 100
    ///
    /// These validations mirror the API layer for defense-in-depth.
    pub async fn create_budget(
        &self,
        organization_id: Uuid,
        project_id: Option<Uuid>,
        monthly_budget_usd: Decimal,
        alert_threshold_percent: i32,
        created_by: Option<Uuid>,
    ) -> Result<Budget> {
        // Defense-in-depth: validate inputs even though API layer should validate
        if monthly_budget_usd <= Decimal::ZERO {
            anyhow::bail!("Budget amount must be greater than zero");
        }
        let max_budget = Decimal::new(999_999_999, 2); // $9,999,999.99
        if monthly_budget_usd > max_budget {
            anyhow::bail!("Budget amount exceeds maximum allowed value");
        }
        if alert_threshold_percent < 1 || alert_threshold_percent > 100 {
            anyhow::bail!("Alert threshold must be between 1 and 100 percent");
        }

        let row = sqlx::query(
            r#"
            INSERT INTO billing_budgets (organization_id, project_id, monthly_budget_usd,
                                         alert_threshold_percent, created_by)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (organization_id, project_id) DO UPDATE
            SET monthly_budget_usd = EXCLUDED.monthly_budget_usd,
                alert_threshold_percent = EXCLUDED.alert_threshold_percent,
                updated_at = NOW()
            RETURNING id, organization_id, project_id, monthly_budget_usd,
                      alert_threshold_percent, enabled, created_by, created_at, updated_at,
                      last_threshold_alert_at, last_exceeded_alert_at, last_alert_percent
            "#,
        )
        .bind(organization_id)
        .bind(project_id)
        .bind(monthly_budget_usd)
        .bind(alert_threshold_percent)
        .bind(created_by)
        .fetch_one(self.db.as_ref())
        .await
        .context("Failed to create budget")?;

        Ok(Budget {
            id: row.get("id"),
            organization_id: row.get("organization_id"),
            project_id: row.get("project_id"),
            monthly_budget_usd: row.get("monthly_budget_usd"),
            alert_threshold_percent: row.get("alert_threshold_percent"),
            enabled: row.get("enabled"),
            created_by: row.get("created_by"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
            last_threshold_alert_at: row.get("last_threshold_alert_at"),
            last_exceeded_alert_at: row.get("last_exceeded_alert_at"),
            last_alert_percent: row.get("last_alert_percent"),
        })
    }

    /// Update a budget.
    ///
    /// # Arguments
    /// * `budget_id` - The budget to update
    /// * `organization_id` - The organization that owns the budget (for ownership verification)
    /// * `monthly_budget_usd` - Optional new budget amount
    /// * `alert_threshold_percent` - Optional new alert threshold
    /// * `enabled` - Optional enabled/disabled state
    ///
    /// # Validation (defense-in-depth)
    /// - `monthly_budget_usd` if provided, must be > 0 and <= 9,999,999.99
    /// - `alert_threshold_percent` if provided, must be between 1 and 100
    ///
    /// # Security
    /// Verifies that the budget belongs to the specified organization before updating.
    /// This provides defense-in-depth even if the API layer also validates ownership.
    pub async fn update_budget(
        &self,
        budget_id: Uuid,
        organization_id: Uuid,
        monthly_budget_usd: Option<Decimal>,
        alert_threshold_percent: Option<i32>,
        enabled: Option<bool>,
    ) -> Result<Budget> {
        // Defense-in-depth: validate inputs even though API layer should validate
        if let Some(amount) = monthly_budget_usd {
            if amount <= Decimal::ZERO {
                anyhow::bail!("Budget amount must be greater than zero");
            }
            let max_budget = Decimal::new(999_999_999, 2); // $9,999,999.99
            if amount > max_budget {
                anyhow::bail!("Budget amount exceeds maximum allowed value");
            }
        }
        if let Some(threshold) = alert_threshold_percent {
            if threshold < 1 || threshold > 100 {
                anyhow::bail!("Alert threshold must be between 1 and 100 percent");
            }
        }

        // Verify budget belongs to the organization (defense-in-depth)
        // Use UPDATE ... WHERE to atomically check ownership and update
        let row = sqlx::query(
            r#"
            UPDATE billing_budgets
            SET monthly_budget_usd = COALESCE($3, monthly_budget_usd),
                alert_threshold_percent = COALESCE($4, alert_threshold_percent),
                enabled = COALESCE($5, enabled),
                updated_at = NOW()
            WHERE id = $1 AND organization_id = $2
            RETURNING id, organization_id, project_id, monthly_budget_usd,
                      alert_threshold_percent, enabled, created_by, created_at, updated_at,
                      last_threshold_alert_at, last_exceeded_alert_at, last_alert_percent
            "#,
        )
        .bind(budget_id)
        .bind(organization_id)
        .bind(monthly_budget_usd)
        .bind(alert_threshold_percent)
        .bind(enabled)
        .fetch_optional(self.db.as_ref())
        .await
        .context("Failed to update budget")?
        .ok_or_else(|| anyhow::anyhow!("Budget not found or access denied"))?;

        Ok(Budget {
            id: row.get("id"),
            organization_id: row.get("organization_id"),
            project_id: row.get("project_id"),
            monthly_budget_usd: row.get("monthly_budget_usd"),
            alert_threshold_percent: row.get("alert_threshold_percent"),
            enabled: row.get("enabled"),
            created_by: row.get("created_by"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
            last_threshold_alert_at: row.get("last_threshold_alert_at"),
            last_exceeded_alert_at: row.get("last_exceeded_alert_at"),
            last_alert_percent: row.get("last_alert_percent"),
        })
    }

    /// Record that a budget alert was sent.
    /// Updates the tracking columns to prevent duplicate alerts.
    pub async fn record_budget_alert_sent(
        &self,
        budget_id: Uuid,
        is_exceeded: bool,
        usage_percent: i32,
    ) -> Result<()> {
        if is_exceeded {
            sqlx::query(
                r#"
                UPDATE billing_budgets
                SET last_exceeded_alert_at = NOW(),
                    last_alert_percent = $2,
                    updated_at = NOW()
                WHERE id = $1
                "#,
            )
            .bind(budget_id)
            .bind(usage_percent)
            .execute(self.db.as_ref())
            .await
            .context("Failed to record exceeded alert")?;
        } else {
            sqlx::query(
                r#"
                UPDATE billing_budgets
                SET last_threshold_alert_at = NOW(),
                    last_alert_percent = $2,
                    updated_at = NOW()
                WHERE id = $1
                "#,
            )
            .bind(budget_id)
            .bind(usage_percent)
            .execute(self.db.as_ref())
            .await
            .context("Failed to record threshold alert")?;
        }
        Ok(())
    }

    // =========================================================================
    // Usage Queries (ClickHouse)
    // =========================================================================

    /// Get project IDs for an organization.
    async fn get_project_ids(&self, organization_id: Uuid) -> Result<Vec<Uuid>> {
        let rows = sqlx::query("SELECT id FROM projects WHERE organization_id = $1")
            .bind(organization_id)
            .fetch_all(self.db.as_ref())
            .await
            .context("Failed to fetch project IDs")?;

        Ok(rows.iter().map(|r| r.get("id")).collect())
    }

    // =========================================================================
    // ClickHouse Query Helpers
    // =========================================================================

    /// Query Watch (observability) usage from ClickHouse.
    /// Returns `(spans_bytes, logs_bytes, metrics_count)`.
    async fn query_watch_usage(
        &self,
        project_id_clause: &str,
        date_filter: Option<(&str, &str)>,
    ) -> Result<(u64, u64, u64)> {
        let date_clause = match date_filter {
            Some((start, end)) => format!("AND date >= '{}' AND date < '{}'", start, end),
            None => "AND date >= toStartOfMonth(today())".to_string(),
        };

        let query = format!(
            r#"
            SELECT event_type,
                   sum(value) as value
            FROM reiver.usage
            WHERE project_id IN ({})
              {}
            GROUP BY event_type
            "#,
            project_id_clause, date_clause
        );

        let mut cursor = self.clickhouse.as_ref().query(&query).fetch::<UsageRow>()?;

        let mut spans_bytes: u64 = 0;
        let mut logs_bytes: u64 = 0;
        let mut metrics_count: u64 = 0;

        while let Some(row) = cursor.next().await? {
            match row.event_type.as_str() {
                "span" => spans_bytes += row.value,
                "log" => logs_bytes += row.value,
                "metric" => metrics_count += row.value,
                _ => {}
            }
        }

        Ok((spans_bytes, logs_bytes, metrics_count))
    }

    /// Query aggregate BYOK cost from ClickHouse `llm_cost_daily`.
    /// Returns `GatewayCostRow` with raw provider cost (fee not applied).
    async fn query_byok_cost(
        &self,
        project_id_clause: &str,
        date_filter: Option<(&str, &str)>,
    ) -> Result<GatewayCostRow> {
        let date_clause = match date_filter {
            Some((start, end)) => format!("AND date >= '{}' AND date < '{}'", start, end),
            None => "AND date >= toStartOfMonth(today())".to_string(),
        };

        let query = format!(
            r#"
            SELECT
                sum(request_count) as request_count,
                sum(input_tokens) as input_tokens,
                sum(output_tokens) as output_tokens,
                toFloat64(sum(total_cost_usd)) as total_cost_usd
            FROM reiver.llm_cost_daily
            WHERE project_id IN ({})
              {}
              AND is_platform_key = 0
            "#,
            project_id_clause, date_clause
        );

        Ok(self
            .clickhouse
            .as_ref()
            .query(&query)
            .fetch_one()
            .await
            .unwrap_or(GatewayCostRow {
                request_count: 0,
                input_tokens: 0,
                output_tokens: 0,
                total_cost_usd: 0.0,
            }))
    }

    // =========================================================================
    // Batch Query Helpers (for billing worker)
    // =========================================================================

    /// Batch query Watch usage for many projects at once, current month.
    /// Returns `HashMap<Uuid, (spans_bytes, logs_bytes, metrics_count)>` keyed by project_id.
    pub async fn batch_watch_usage(
        &self,
        project_ids: &[Uuid],
    ) -> Result<HashMap<Uuid, (u64, u64, u64)>> {
        if project_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let id_clause =
            build_uuid_in_clause(project_ids).expect("project_ids not empty, checked above");

        let query = format!(
            r#"
            SELECT project_id, event_type,
                   sum(value) as value
            FROM reiver.usage
            WHERE project_id IN ({})
              AND date >= toStartOfMonth(today())
            GROUP BY project_id, event_type
            "#,
            id_clause
        );

        let mut cursor = self
            .clickhouse
            .as_ref()
            .query(&query)
            .fetch::<ProjectUsageRow>()?;

        let mut result: HashMap<Uuid, (u64, u64, u64)> = HashMap::new();
        while let Some(row) = cursor.next().await? {
            if let Ok(pid) = Uuid::parse_str(&row.project_id) {
                let entry = result.entry(pid).or_insert((0, 0, 0));
                match row.event_type.as_str() {
                    "span" => entry.0 += row.value,
                    "log" => entry.1 += row.value,
                    "metric" => entry.2 += row.value,
                    _ => {}
                }
            }
        }
        Ok(result)
    }

    /// Batch query BYOK cost for many projects at once, current month.
    /// Returns `HashMap<Uuid, GatewayCostRow>` keyed by project_id.
    pub async fn batch_byok_cost(
        &self,
        project_ids: &[Uuid],
    ) -> Result<HashMap<Uuid, GatewayCostRow>> {
        if project_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let id_clause =
            build_uuid_in_clause(project_ids).expect("project_ids not empty, checked above");

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct ProjectCostRow {
            project_id: String,
            request_count: u64,
            input_tokens: u64,
            output_tokens: u64,
            total_cost_usd: f64,
        }

        let query = format!(
            r#"
            SELECT
                toString(project_id) as project_id,
                sum(request_count) as request_count,
                sum(input_tokens) as input_tokens,
                sum(output_tokens) as output_tokens,
                toFloat64(sum(total_cost_usd)) as total_cost_usd
            FROM reiver.llm_cost_daily
            WHERE project_id IN ({})
              AND date >= toStartOfMonth(today())
              AND is_platform_key = 0
            GROUP BY project_id
            "#,
            id_clause
        );

        let mut cursor = self
            .clickhouse
            .as_ref()
            .query(&query)
            .fetch::<ProjectCostRow>()?;

        let mut result: HashMap<Uuid, GatewayCostRow> = HashMap::new();
        while let Some(row) = cursor.next().await? {
            if let Ok(pid) = Uuid::parse_str(&row.project_id) {
                result.insert(
                    pid,
                    GatewayCostRow {
                        request_count: row.request_count,
                        input_tokens: row.input_tokens,
                        output_tokens: row.output_tokens,
                        total_cost_usd: row.total_cost_usd,
                    },
                );
            }
        }
        Ok(result)
    }

    // =========================================================================
    // Usage Queries
    // =========================================================================

    /// Get current usage for an organization in the billing period.
    pub async fn get_current_usage(&self, organization_id: Uuid) -> Result<UsageSummary> {
        let project_ids = self.get_project_ids(organization_id).await?;

        if project_ids.is_empty() {
            return Ok(UsageSummary {
                organization_id,
                period_start: start_of_month(Utc::now()),
                period_end: Utc::now(),
                spans_ingested_bytes: 0,
                logs_ingested_bytes: 0,
                metrics_count: 0,
                estimated_cost_usd: Decimal::ZERO,
                amount_paid_usd: Decimal::ZERO,
                gateway_requests: 0,
                gateway_input_tokens: 0,
                gateway_output_tokens: 0,
                gateway_cost_usd: Decimal::ZERO,
            });
        }

        let project_id_clause =
            build_uuid_in_clause(&project_ids).expect("project_ids not empty, checked above");

        let (spans_ingested_bytes, logs_ingested_bytes, metrics_count) =
            self.query_watch_usage(&project_id_clause, None).await?;

        let tier = self.entitlements.get_config(organization_id).await?;
        let estimated_cost_usd = self.calculate_cost(
            spans_ingested_bytes,
            logs_ingested_bytes,
            metrics_count,
            tier.config.watch.traces_logs_per_gb_usd,
            tier.config.watch.metrics_per_million_usd,
        );

        let gw_row = self.query_byok_cost(&project_id_clause, None).await?;

        let period_start = start_of_month(Utc::now());
        let amount_paid_usd: Decimal = sqlx::query_scalar(
            r#"
            SELECT COALESCE(SUM(amount_usd), 0)
            FROM pending_charges
            WHERE organization_id = $1
              AND billing_period_start = $2
              AND status = 'paid'
            "#,
        )
        .bind(organization_id)
        .bind(period_start.date_naive())
        .fetch_one(self.db.as_ref())
        .await
        .unwrap_or(Decimal::ZERO);

        let gateway_cost_usd = self
            .calculate_gateway_fees(organization_id, &project_id_clause, None)
            .await?;

        Ok(UsageSummary {
            organization_id,
            period_start,
            period_end: Utc::now(),
            spans_ingested_bytes,
            logs_ingested_bytes,
            metrics_count,
            estimated_cost_usd,
            amount_paid_usd,
            gateway_requests: gw_row.request_count,
            gateway_input_tokens: gw_row.input_tokens,
            gateway_output_tokens: gw_row.output_tokens,
            gateway_cost_usd,
        })
    }

    /// Get usage breakdown by project.
    pub async fn get_usage_by_project(&self, organization_id: Uuid) -> Result<Vec<ProjectUsage>> {
        let project_ids = self.get_project_ids(organization_id).await?;

        if project_ids.is_empty() {
            return Ok(vec![]);
        }

        let project_names: HashMap<Uuid, String> =
            sqlx::query("SELECT id, name FROM projects WHERE organization_id = $1")
                .bind(organization_id)
                .fetch_all(self.db.as_ref())
                .await
                .context("Failed to fetch project names")?
                .into_iter()
                .map(|r| (r.get("id"), r.get("name")))
                .collect();

        let project_id_clause =
            build_uuid_in_clause(&project_ids).expect("project_ids not empty, checked above");

        let query = format!(
            r#"
            SELECT project_id, event_type,
                   sum(value) as value
            FROM reiver.usage
            WHERE project_id IN ({})
              AND date >= toStartOfMonth(today())
            GROUP BY project_id, event_type
            "#,
            project_id_clause
        );

        let mut cursor = self
            .clickhouse
            .as_ref()
            .query(&query)
            .fetch::<ProjectUsageRow>()?;

        let mut project_usage: HashMap<String, (u64, u64, u64)> = HashMap::new();

        while let Some(row) = cursor.next().await? {
            let entry = project_usage
                .entry(row.project_id.clone())
                .or_insert((0, 0, 0));
            match row.event_type.as_str() {
                "span" => entry.0 += row.value,
                "log" => entry.1 += row.value,
                "metric" => entry.2 += row.value,
                _ => {}
            }
        }

        // BYOK provider cost per project (is_platform_key=0). Fee applied in Rust.
        let gw_query = format!(
            r#"
            SELECT
                toString(project_id) as project_id,
                sum(request_count) as request_count,
                toFloat64(sum(total_cost_usd)) as total_cost_usd
            FROM reiver.llm_cost_daily
            WHERE project_id IN ({})
              AND date >= toStartOfMonth(today())
              AND is_platform_key = 0
            GROUP BY project_id
            "#,
            project_id_clause
        );

        let mut gw_cursor = self
            .clickhouse
            .as_ref()
            .query(&gw_query)
            .fetch::<ProjectGatewayCostRow>()?;

        let gateway_rate =
            credits::get_gateway_fee_rate(self.entitlements.as_ref(), organization_id).await?;
        let moodeng_rate =
            credits::get_moodeng_fee_rate(self.entitlements.as_ref(), organization_id).await?;

        let mut gw_by_project: HashMap<Uuid, (u64, Decimal)> = HashMap::new();
        while let Some(row) = gw_cursor.next().await? {
            if let Ok(pid) = Uuid::parse_str(&row.project_id) {
                let rate = if self.moodeng_project_id == Some(pid) {
                    moodeng_rate
                } else {
                    gateway_rate
                };
                let fee = Decimal::from_f64_retain(row.total_cost_usd)
                    .unwrap_or(Decimal::ZERO)
                    * rate;
                gw_by_project.insert(pid, (row.request_count, fee));
            }
        }

        let tier = self.entitlements.get_config(organization_id).await?;
        let tl_rate = tier.config.watch.traces_logs_per_gb_usd;
        let m_rate = tier.config.watch.metrics_per_million_usd;

        let mut seen: std::collections::HashSet<Uuid> = std::collections::HashSet::new();

        let mut result: Vec<ProjectUsage> = project_usage
            .into_iter()
            .filter_map(|(project_id_str, (span_bytes, log_bytes, metrics))| {
                let project_id = Uuid::parse_str(&project_id_str).ok()?;
                let (gw_requests, gw_fee) = gw_by_project
                    .get(&project_id)
                    .cloned()
                    .unwrap_or((0, Decimal::ZERO));
                seen.insert(project_id);
                Some(ProjectUsage {
                    project_id,
                    project_name: project_names.get(&project_id).cloned(),
                    spans_ingested_bytes: span_bytes,
                    logs_ingested_bytes: log_bytes,
                    metrics_count: metrics,
                    estimated_cost_usd: self
                        .calculate_cost(span_bytes, log_bytes, metrics, tl_rate, m_rate),
                    gateway_requests: gw_requests,
                    gateway_cost_usd: gw_fee,
                })
            })
            .collect();

        for (project_id, (gw_requests, gw_fee)) in &gw_by_project {
            if seen.contains(project_id) {
                continue;
            }
            result.push(ProjectUsage {
                project_id: *project_id,
                project_name: project_names.get(project_id).cloned(),
                spans_ingested_bytes: 0,
                logs_ingested_bytes: 0,
                metrics_count: 0,
                estimated_cost_usd: Decimal::ZERO,
                gateway_requests: *gw_requests,
                gateway_cost_usd: *gw_fee,
            });
        }

        Ok(result)
    }

    /// Get AI Gateway cost breakdown by model for the current billing period.
    pub async fn get_gateway_cost_by_model(
        &self,
        organization_id: Uuid,
    ) -> Result<Vec<GatewayModelCost>> {
        let project_ids = self.get_project_ids(organization_id).await?;

        if project_ids.is_empty() {
            return Ok(vec![]);
        }

        let project_id_clause =
            build_uuid_in_clause(&project_ids).expect("project_ids not empty, checked above");

        let query = format!(
            r#"
            SELECT
                gen_ai_system,
                gen_ai_request_model,
                sum(request_count) as request_count,
                sum(input_tokens) as input_tokens,
                sum(output_tokens) as output_tokens,
                toFloat64(sum(total_cost_usd)) as total_cost_usd
            FROM reiver.llm_cost_daily
            WHERE project_id IN ({})
              AND date >= toStartOfMonth(today())
              AND is_platform_key = 0
            GROUP BY gen_ai_system, gen_ai_request_model
            ORDER BY total_cost_usd DESC
            LIMIT 50
            "#,
            project_id_clause
        );

        let mut cursor = self
            .clickhouse
            .as_ref()
            .query(&query)
            .fetch::<ModelGatewayCostRow>()?;

        let gateway_rate =
            credits::get_gateway_fee_rate(self.entitlements.as_ref(), organization_id).await?;

        let mut results = Vec::new();
        while let Some(row) = cursor.next().await? {
            results.push(GatewayModelCost {
                provider: row.gen_ai_system,
                model: row.gen_ai_request_model,
                request_count: row.request_count,
                input_tokens: row.input_tokens,
                output_tokens: row.output_tokens,
                cost_usd: Decimal::from_f64_retain(row.total_cost_usd)
                    .unwrap_or(Decimal::ZERO)
                    * gateway_rate,
            });
        }

        Ok(results)
    }

    /// Get BYOK fee total for an organization in a date range.
    /// Fetches raw provider cost from `llm_cost_daily` and applies the
    /// tier-aware fee rate in Rust for precise Decimal arithmetic.
    pub async fn get_byok_fees(
        &self,
        organization_id: Uuid,
        period_start: &str,
        period_end: &str,
    ) -> Result<(Decimal, u64)> {
        let project_ids = self.get_project_ids(organization_id).await?;
        if project_ids.is_empty() {
            return Ok((Decimal::ZERO, 0));
        }

        let project_id_clause =
            build_uuid_in_clause(&project_ids).expect("project_ids not empty");

        let row = self
            .query_byok_cost(&project_id_clause, Some((period_start, period_end)))
            .await?;

        let fees = self
            .calculate_gateway_fees(organization_id, &project_id_clause, Some((period_start, period_end)))
            .await?;
        Ok((fees, row.request_count))
    }

    // =========================================================================
    // Gateway Fee Calculation
    // =========================================================================

    /// Calculate gateway fees for an organization, splitting MooDeng traffic
    /// from regular gateway traffic when `moodeng_project_id` is configured.
    /// Each segment gets its own tier-based fee rate applied.
    ///
    /// Uses a subtraction approach: query total cost for all projects, then
    /// query MooDeng cost separately. Regular cost = total - MooDeng.
    async fn calculate_gateway_fees(
        &self,
        organization_id: Uuid,
        project_id_clause: &str,
        period: Option<(&str, &str)>,
    ) -> Result<Decimal> {
        let gateway_rate =
            credits::get_gateway_fee_rate(self.entitlements.as_ref(), organization_id).await?;

        match self.moodeng_project_id {
            Some(moodeng_pid)
                if project_id_clause.contains(&moodeng_pid.to_string()) =>
            {
                let moodeng_rate =
                    credits::get_moodeng_fee_rate(self.entitlements.as_ref(), organization_id)
                        .await?;

                let total_row = self
                    .query_byok_cost(project_id_clause, period)
                    .await?;
                let total_cost = Decimal::from_f64_retain(total_row.total_cost_usd)
                    .unwrap_or(Decimal::ZERO);

                let moodeng_clause = format!("'{}'", moodeng_pid);
                let moodeng_row = self
                    .query_byok_cost(&moodeng_clause, period)
                    .await?;
                let moodeng_cost = Decimal::from_f64_retain(moodeng_row.total_cost_usd)
                    .unwrap_or(Decimal::ZERO);

                let regular_cost = (total_cost - moodeng_cost).max(Decimal::ZERO);

                Ok(regular_cost * gateway_rate + moodeng_cost * moodeng_rate)
            }
            _ => {
                let row = self
                    .query_byok_cost(project_id_clause, period)
                    .await?;
                Ok(Decimal::from_f64_retain(row.total_cost_usd)
                    .unwrap_or(Decimal::ZERO)
                    * gateway_rate)
            }
        }
    }

    // =========================================================================
    // Cost Calculation
    // =========================================================================

    /// Calculate cost from ingested data sizes.
    /// Traces + logs are billed per GB; metrics per million data points.
    pub fn calculate_cost(
        &self,
        spans_bytes: u64,
        logs_bytes: u64,
        metrics_count: u64,
        traces_logs_per_gb_usd: Decimal,
        metrics_per_million_usd: Decimal,
    ) -> Decimal {
        let gb = Decimal::from(1_000_000_000u64);
        let million = Decimal::from(1_000_000u64);
        let traces_logs_cost = (Decimal::from(spans_bytes.saturating_add(logs_bytes)) / gb)
            * traces_logs_per_gb_usd;
        let metrics_cost =
            (Decimal::from(metrics_count) / million) * metrics_per_million_usd;
        traces_logs_cost + metrics_cost
    }

    /// Get Watch (observability) usage for an organization in a date range.
    /// Returns `(spans_bytes, logs_bytes, metrics_count)`.
    pub async fn get_watch_usage_for_period(
        &self,
        organization_id: Uuid,
        period_start: &str,
        period_end: &str,
    ) -> Result<(u64, u64, u64)> {
        let project_ids = self.get_project_ids(organization_id).await?;
        if project_ids.is_empty() {
            return Ok((0, 0, 0));
        }

        let project_id_clause =
            build_uuid_in_clause(&project_ids).expect("project_ids not empty");

        self.query_watch_usage(&project_id_clause, Some((period_start, period_end)))
            .await
    }

    // =========================================================================
    // Budget Status
    // =========================================================================

    /// Get budget status with current usage comparison.
    pub async fn get_budget_status(&self, organization_id: Uuid) -> Result<Option<BudgetStatus>> {
        let budget = match self.get_org_budget(organization_id).await? {
            Some(b) if b.enabled => b,
            _ => return Ok(None),
        };

        let usage = self.get_current_usage(organization_id).await?;
        let (total_cost, usage_percent, threshold_exceeded, budget_exceeded) =
            compute_budget_status(
                usage.estimated_cost_usd,
                usage.gateway_cost_usd,
                budget.monthly_budget_usd,
                budget.alert_threshold_percent,
            );

        Ok(Some(BudgetStatus {
            budget,
            current_cost_usd: total_cost,
            usage_percent,
            threshold_exceeded,
            budget_exceeded,
        }))
    }

}

// =========================================================================
// Helper Functions
// =========================================================================

/// Get the start of the current month.
fn start_of_month(dt: DateTime<Utc>) -> DateTime<Utc> {
    dt.date_naive()
        .with_day(1)
        .expect("Day 1 is always valid")
        .and_hms_opt(0, 0, 0)
        .expect("00:00:00 is always valid")
        .and_utc()
}

// =========================================================================
// Tests
// =========================================================================

/// Compute budget status from pre-fetched values.
/// Pure function extracted for testability.
pub fn compute_budget_status(
    estimated_cost_usd: Decimal,
    gateway_cost_usd: Decimal,
    monthly_budget_usd: Decimal,
    alert_threshold_percent: i32,
) -> (Decimal, f64, bool, bool) {
    let total_cost = estimated_cost_usd + gateway_cost_usd;

    let usage_percent = if monthly_budget_usd > Decimal::ZERO {
        (total_cost / monthly_budget_usd * Decimal::from(100))
            .to_f64()
            .unwrap_or(0.0)
    } else {
        0.0
    };

    let threshold_exceeded = usage_percent >= alert_threshold_percent as f64;
    let budget_exceeded = total_cost >= monthly_budget_usd;

    (total_cost, usage_percent, threshold_exceeded, budget_exceeded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Timelike};

    #[test]
    fn test_start_of_month() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 14, 30, 0).unwrap();
        let start = start_of_month(dt);

        assert_eq!(start.year(), 2024);
        assert_eq!(start.month(), 6);
        assert_eq!(start.day(), 1);
        assert_eq!(start.hour(), 0);
        assert_eq!(start.minute(), 0);
        assert_eq!(start.second(), 0);
    }

    fn calc_cost(spans: u64, logs: u64, metrics: u64, tl_rate: Decimal, m_rate: Decimal) -> Decimal {
        let gb = Decimal::from(1_000_000_000u64);
        let million = Decimal::from(1_000_000u64);
        let traces_logs_cost =
            (Decimal::from(spans.saturating_add(logs)) / gb) * tl_rate;
        let metrics_cost = (Decimal::from(metrics) / million) * m_rate;
        traces_logs_cost + metrics_cost
    }

    const TL_RATE: Decimal = Decimal::from_parts(20, 0, 0, false, 2); // $0.20/GB
    const M_RATE: Decimal = Decimal::from_parts(10, 0, 0, false, 2);  // $0.10/M

    // =========================================================================
    // calculate_cost tests
    // =========================================================================

    #[test]
    fn test_calculate_cost_zero() {
        assert_eq!(calc_cost(0, 0, 0, TL_RATE, M_RATE), Decimal::ZERO);
    }

    #[test]
    fn test_calculate_cost_exact_1gb_spans() {
        let cost = calc_cost(1_000_000_000, 0, 0, TL_RATE, M_RATE);
        assert_eq!(cost, Decimal::new(20, 2)); // $0.20
    }

    #[test]
    fn test_calculate_cost_exact_1m_metrics() {
        let cost = calc_cost(0, 0, 1_000_000, TL_RATE, M_RATE);
        assert_eq!(cost, Decimal::new(10, 2)); // $0.10
    }

    #[test]
    fn test_calculate_cost_mixed() {
        // 500MB spans + 500MB logs = 1GB total -> $0.20
        // 500K metrics = 0.5M -> $0.05
        let cost = calc_cost(500_000_000, 500_000_000, 500_000, TL_RATE, M_RATE);
        assert_eq!(cost, Decimal::new(25, 2)); // $0.25
    }

    #[test]
    fn test_calculate_cost_large_values_no_overflow() {
        // 100 GB spans + 100 GB logs = 200 GB -> $40.00
        // 50M metrics -> $5.00
        let cost = calc_cost(100_000_000_000, 100_000_000_000, 50_000_000, TL_RATE, M_RATE);
        let expected = Decimal::new(40_00, 2) + Decimal::new(5_00, 2);
        assert_eq!(cost, expected); // $45.00
    }

    #[test]
    fn test_calculate_cost_1_byte() {
        let cost = calc_cost(1, 0, 0, TL_RATE, M_RATE);
        assert!(cost > Decimal::ZERO);
        assert!(cost < Decimal::new(1, 8));
    }

    #[test]
    fn test_calculate_cost_custom_pricing() {
        let tl = Decimal::new(50, 2); // $0.50/GB
        let m = Decimal::new(25, 2);  // $0.25/M
        // 2 GB -> $1.00, 4M metrics -> $1.00
        let cost = calc_cost(2_000_000_000, 0, 4_000_000, tl, m);
        assert_eq!(cost, Decimal::new(2_00, 2)); // $2.00
    }

    #[test]
    fn test_calculate_cost_saturating_add() {
        let cost = calc_cost(u64::MAX, u64::MAX, 0, TL_RATE, M_RATE);
        let expected = calc_cost(u64::MAX, 0, 0, TL_RATE, M_RATE);
        assert_eq!(cost, expected);
    }

    // =========================================================================
    // BYOK fee calculation contract
    // =========================================================================

    #[test]
    fn test_byok_fee_from_f64_matches_decimal() {
        let raw_cost: f64 = 100.0;
        let fee_rate = Decimal::new(3, 2); // 3%
        let fee = Decimal::from_f64_retain(raw_cost).unwrap() * fee_rate;
        assert_eq!(fee, Decimal::new(3_00, 2)); // $3.00
    }

    #[test]
    fn test_byok_fee_from_f64_small_value() {
        let raw_cost: f64 = 0.50;
        let fee_rate = Decimal::new(3, 2);
        let fee = Decimal::from_f64_retain(raw_cost).unwrap() * fee_rate;
        assert_eq!(fee, Decimal::new(15, 3)); // $0.015
    }

    #[test]
    fn test_byok_fee_zero_cost() {
        let raw_cost: f64 = 0.0;
        let fee_rate = Decimal::new(3, 2);
        let fee = Decimal::from_f64_retain(raw_cost).unwrap() * fee_rate;
        assert_eq!(fee, Decimal::ZERO);
    }

    // =========================================================================
    // Budget status math tests
    // =========================================================================

    #[test]
    fn test_budget_status_zero_budget() {
        let (total, pct, _, _) =
            compute_budget_status(Decimal::new(50, 0), Decimal::ZERO, Decimal::ZERO, 80);
        assert_eq!(total, Decimal::new(50, 0));
        assert_eq!(pct, 0.0); // no division by zero
    }

    #[test]
    fn test_budget_status_at_threshold() {
        // $80 of $100 budget, threshold 80%
        let (_, pct, threshold_exceeded, budget_exceeded) = compute_budget_status(
            Decimal::new(80, 0),
            Decimal::ZERO,
            Decimal::new(100, 0),
            80,
        );
        assert!((pct - 80.0).abs() < 0.001);
        assert!(threshold_exceeded);
        assert!(!budget_exceeded);
    }

    #[test]
    fn test_budget_status_below_threshold() {
        // $79.99 of $100 budget, threshold 80%
        let (_, pct, threshold_exceeded, _) = compute_budget_status(
            Decimal::new(7999, 2),
            Decimal::ZERO,
            Decimal::new(100, 0),
            80,
        );
        assert!(pct < 80.0);
        assert!(!threshold_exceeded);
    }

    #[test]
    fn test_budget_status_exceeded() {
        // $100 of $100 budget
        let (_, _, _, budget_exceeded) = compute_budget_status(
            Decimal::new(100, 0),
            Decimal::ZERO,
            Decimal::new(100, 0),
            80,
        );
        assert!(budget_exceeded);
    }

    #[test]
    fn test_budget_status_includes_gateway_cost() {
        // Watch $40 + Gateway $45 = $85 of $100 budget
        let (total, pct, threshold_exceeded, _) = compute_budget_status(
            Decimal::new(40, 0),
            Decimal::new(45, 0),
            Decimal::new(100, 0),
            80,
        );
        assert_eq!(total, Decimal::new(85, 0));
        assert!((pct - 85.0).abs() < 0.001);
        assert!(threshold_exceeded);
    }
}
