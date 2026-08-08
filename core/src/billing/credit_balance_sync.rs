use std::sync::Arc;
use std::time::Duration;

use futures_util::stream::{self, StreamExt};
use stripe::Client;
use stripe_billing::billing_credit_balance_summary::RetrieveForMyAccountBillingCreditBalanceSummary;
use stripe_billing::billing_credit_balance_summary::RetrieveForMyAccountBillingCreditBalanceSummaryFilter;
use stripe_billing::billing_credit_balance_summary::RetrieveForMyAccountBillingCreditBalanceSummaryFilterType;
use tracing::{debug, error, trace, warn};
use uuid::Uuid;

use crate::app_state::RedisPool;
use crate::db::DbPool;

const SYNC_INTERVAL: Duration = Duration::from_secs(30);
const REDIS_KEY_PREFIX: &str = "billing:stripe_credits:";
const REDIS_TTL_SECS: u64 = 60;
/// Max concurrent Stripe API calls during sync.
const CONCURRENCY_LIMIT: usize = 10;

/// Spawn a background task that syncs Stripe credit balances to Redis every 30 seconds.
///
/// Only queries orgs that have both an active subscription AND at least one credit grant.
/// Uses concurrent Stripe API calls (up to `CONCURRENCY_LIMIT` at a time).
pub fn spawn_credit_balance_sync(
    stripe_client: Client,
    db: Arc<DbPool>,
    redis: Arc<RedisPool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(SYNC_INTERVAL);
        loop {
            interval.tick().await;
            if let Err(e) = sync_credit_balances(&stripe_client, &db, &redis).await {
                error!(error = %e, "Credit balance sync failed");
            }
        }
    })
}

/// Convenience: spawn from just an API key string.
pub fn spawn_credit_balance_sync_from_key(
    api_key: &str,
    db: Arc<DbPool>,
    redis: Arc<RedisPool>,
) -> tokio::task::JoinHandle<()> {
    spawn_credit_balance_sync(Client::new(api_key), db, redis)
}

async fn sync_credit_balances(
    client: &Client,
    db: &DbPool,
    redis: &RedisPool,
) -> anyhow::Result<()> {
    let rows: Vec<(Uuid, String)> = sqlx::query_as(
        r#"
        SELECT sc.organization_id, sc.stripe_customer_id
        FROM stripe_customers sc
        JOIN stripe_subscriptions ss ON ss.organization_id = sc.organization_id
        WHERE ss.status IN ('active', 'trialing', 'past_due')
        "#,
    )
    .fetch_all(db)
    .await?;

    if rows.is_empty() {
        trace!("No subscribed orgs to sync credit balances for");
        return Ok(());
    }

    debug!(count = rows.len(), "Syncing Stripe credit balances");

    let results: Vec<(Uuid, bool)> = stream::iter(rows)
        .map(|(org_id, stripe_customer_id)| {
            let client = client.clone();
            async move {
                let has_credits = check_customer_credits(&client, &stripe_customer_id).await;
                (org_id, has_credits)
            }
        })
        .buffer_unordered(CONCURRENCY_LIMIT)
        .collect()
        .await;

    for (org_id, has_credits) in results {
        let cache_key = format!("{}{}", REDIS_KEY_PREFIX, org_id);

        if let Ok(mut conn) = redis.get().await {
            let _ = redis::cmd("SET")
                .arg(&cache_key)
                .arg(if has_credits { "1" } else { "0" })
                .arg("EX")
                .arg(REDIS_TTL_SECS)
                .query_async::<()>(&mut *conn)
                .await;
        }

        trace!(
            organization_id = %org_id,
            has_credits = has_credits,
            "Credit balance synced"
        );
    }

    Ok(())
}

async fn check_customer_credits(client: &Client, stripe_customer_id: &str) -> bool {
    let filter = RetrieveForMyAccountBillingCreditBalanceSummaryFilter::new(
        RetrieveForMyAccountBillingCreditBalanceSummaryFilterType::ApplicabilityScope,
    );

    match RetrieveForMyAccountBillingCreditBalanceSummary::new(filter)
        .customer(stripe_customer_id)
        .send(client)
        .await
    {
        Ok(summary) => summary.balances.iter().any(|b| {
            b.available_balance
                .monetary
                .as_ref()
                .map(|m| m.value > 0)
                .unwrap_or(false)
        }),
        Err(e) => {
            warn!(
                stripe_customer_id = %stripe_customer_id,
                error = %e,
                "Failed to fetch credit balance from Stripe"
            );
            false
        }
    }
}
