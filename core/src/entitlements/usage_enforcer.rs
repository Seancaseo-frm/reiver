use anyhow::{Context, Result};
use uuid::Uuid;

use crate::clickhouse_db::ClickHousePool;
use crate::db::DbPool;
use crate::entitlements::EntitlementChecker;

/// Outcome of a usage limit check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UsageGate {
    Allowed,
    Denied { reason: String },
}

impl UsageGate {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allowed)
    }
}

/// Checks whether an organization has exceeded its metering allotments.
///
/// Used to hard-cap free tier usage (since free tier has no Stripe subscription
/// and thus no Stripe-level usage gating). Paid tiers rely on Stripe's graduated
/// pricing for overage charges; this enforcer is primarily for free tier but
/// works for any tier.
pub struct UsageEnforcer {
    db: DbPool,
    clickhouse: ClickHousePool,
    entitlements: std::sync::Arc<dyn EntitlementChecker>,
}

impl UsageEnforcer {
    pub fn new(
        db: DbPool,
        clickhouse: ClickHousePool,
        entitlements: std::sync::Arc<dyn EntitlementChecker>,
    ) -> Self {
        Self {
            db,
            clickhouse,
            entitlements,
        }
    }

    /// Check if the organization can ingest more observability data.
    pub async fn check_observability_gb(&self, organization_id: Uuid) -> Result<UsageGate> {
        let tier = self.entitlements.get_config(organization_id).await?;
        let limit_gb = tier.config.watch.ingestion_gb_included;

        if limit_gb < 0 {
            return Ok(UsageGate::Allowed);
        }

        // Only hard-cap the free tier (paid tiers pay overage via Stripe)
        let has_subscription = self.has_active_subscription(organization_id).await?;
        if has_subscription {
            return Ok(UsageGate::Allowed);
        }

        let used_gb = self.get_observability_gb_this_period(organization_id).await?;

        if used_gb >= limit_gb {
            Ok(UsageGate::Denied {
                reason: format!(
                    "Observability limit reached: {used_gb}/{limit_gb} GB ingested this billing period. Upgrade to continue ingesting."
                ),
            })
        } else {
            Ok(UsageGate::Allowed)
        }
    }

    async fn get_observability_gb_this_period(&self, organization_id: Uuid) -> Result<i64> {
        let project_ids = self.get_project_ids(organization_id).await?;
        if project_ids.is_empty() {
            return Ok(0);
        }

        let billing_start = current_billing_period_start();
        let project_list = project_ids
            .iter()
            .map(|id| format!("'{}'", id))
            .collect::<Vec<_>>()
            .join(", ");

        let query = format!(
            r#"
            SELECT sum(bytes) as total_bytes
            FROM reiver.usage
            WHERE project_id IN ({})
              AND timestamp >= '{}'
              AND event_type IN ('span', 'log')
            "#,
            project_list,
            billing_start.format("%Y-%m-%d %H:%M:%S"),
        );

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            total_bytes: u64,
        }

        match self.clickhouse.query(&query).fetch_one::<Row>().await {
            Ok(row) => Ok((row.total_bytes as f64 / 1_000_000_000.0).ceil() as i64),
            Err(_) => Ok(0),
        }
    }

    async fn has_active_subscription(&self, organization_id: Uuid) -> Result<bool> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT 1 FROM stripe_subscriptions WHERE organization_id = $1 AND status IN ('active', 'trialing') LIMIT 1",
        )
        .bind(organization_id)
        .fetch_optional(&self.db)
        .await
        .context("Failed to check subscription status")?;

        Ok(row.is_some())
    }

    async fn get_project_ids(&self, organization_id: Uuid) -> Result<Vec<Uuid>> {
        let ids: Vec<(Uuid,)> = sqlx::query_as(
            "SELECT id FROM projects WHERE organization_id = $1",
        )
        .bind(organization_id)
        .fetch_all(&self.db)
        .await
        .context("Failed to fetch project IDs")?;

        Ok(ids.into_iter().map(|(id,)| id).collect())
    }
}

/// Returns the first day of the current billing period (first of the month, UTC).
fn current_billing_period_start() -> chrono::DateTime<chrono::Utc> {
    use chrono::{Datelike, Utc};
    let now = Utc::now();
    now.with_day(1)
        .unwrap_or(now)
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, Timelike};

    #[test]
    fn test_billing_period_start() {
        let start = current_billing_period_start();
        assert_eq!(start.day(), 1);
        assert_eq!(start.hour(), 0);
    }

    #[test]
    fn test_usage_gate_is_allowed() {
        assert!(UsageGate::Allowed.is_allowed());
        assert!(!UsageGate::Denied {
            reason: "test".into()
        }
        .is_allowed());
    }
}
