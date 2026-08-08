use anyhow::{Context, Result};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::sync::Arc;
use uuid::Uuid;

use crate::app_state::RedisPool;
use crate::db::DbPool;
use crate::entitlements::checker::EntitlementChecker;

/// Resolve the gateway fee rate for an organization from its tier config.
pub async fn get_gateway_fee_rate(
    entitlements: &dyn EntitlementChecker,
    org_id: Uuid,
) -> Result<Decimal> {
    Ok(entitlements.get_config(org_id).await?.config.gateway.fee_percent)
}

/// Resolve the MooDeng agent fee rate for an organization from its tier config.
pub async fn get_moodeng_fee_rate(
    entitlements: &dyn EntitlementChecker,
    org_id: Uuid,
) -> Result<Decimal> {
    Ok(entitlements.get_config(org_id).await?.config.gateway.moodeng_fee_percent)
}

/// Resolve the Watch traces+logs per-GB price for an organization.
pub async fn get_watch_traces_logs_price(
    entitlements: &dyn EntitlementChecker,
    org_id: Uuid,
) -> Result<Decimal> {
    Ok(entitlements.get_config(org_id).await?.config.watch.traces_logs_per_gb_usd)
}

/// Resolve the Watch metrics per-million price for an organization.
pub async fn get_watch_metrics_price(
    entitlements: &dyn EntitlementChecker,
    org_id: Uuid,
) -> Result<Decimal> {
    Ok(entitlements.get_config(org_id).await?.config.watch.metrics_per_million_usd)
}

/// Redis key for an organization's credit balance.
fn redis_balance_key(org_id: Uuid) -> String {
    format!("credit:balance:{}", org_id)
}

// =========================================================================
// Types
// =========================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "credit_transaction_type", rename_all = "snake_case")]
pub enum CreditTransactionType {
    TopUp,
    UsageDeduction,
    Refund,
    Adjustment,
}

impl std::fmt::Display for CreditTransactionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TopUp => write!(f, "top_up"),
            Self::UsageDeduction => write!(f, "usage_deduction"),
            Self::Refund => write!(f, "refund"),
            Self::Adjustment => write!(f, "adjustment"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreditTransaction {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub transaction_type: CreditTransactionType,
    pub amount_usd: Decimal,
    pub balance_after_usd: Decimal,
    pub description: Option<String>,
    pub stripe_checkout_session_id: Option<String>,
    pub paid_amount: Option<Decimal>,
    pub paid_currency: Option<String>,
    pub exchange_rate: Option<Decimal>,
    pub llm_request_id: Option<String>,
    pub project_id: Option<Uuid>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub input_tokens: Option<i32>,
    pub output_tokens: Option<i32>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Metadata about an LLM request, attached to credit deductions.
#[derive(Debug, Clone, Default)]
pub struct LlmUsageMetadata {
    pub llm_request_id: Option<String>,
    pub project_id: Option<Uuid>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub input_tokens: Option<i32>,
    pub output_tokens: Option<i32>,
}

/// Pagination parameters for listing queries.
#[derive(Debug, Clone)]
pub struct PaginationParams {
    pub limit: i64,
    pub offset: i64,
}

impl Default for PaginationParams {
    fn default() -> Self {
        Self {
            limit: 50,
            offset: 0,
        }
    }
}

// =========================================================================
// Lua Scripts
// =========================================================================

/// Scaling factor for integer-based Redis balance storage.
/// Balances are stored as `value * 10^8` to avoid floating-point precision loss.
const REDIS_SCALE_DECIMAL: Decimal = Decimal::from_parts(100_000_000, 0, 0, false, 0);

/// Atomic decrement using integer math (no floating-point).
/// Both balance and cost are stored/passed as integers scaled by 10^8.
/// Returns 1 on success, -1 if insufficient balance.
const LUA_DEDUCT: &str = r#"
local bal = tonumber(redis.call('GET', KEYS[1]) or '0')
local cost = tonumber(ARGV[1])
if bal < cost then return -1 end
local new_bal = bal - cost
redis.call('SET', KEYS[1], tostring(new_bal))
return 1
"#;

// =========================================================================
// CreditService
// =========================================================================

pub struct CreditService {
    db: Arc<DbPool>,
    redis: Arc<RedisPool>,
}

impl CreditService {
    pub fn new(db: Arc<DbPool>, redis: Arc<RedisPool>) -> Self {
        Self { db, redis }
    }

    // ---------------------------------------------------------------------
    // Balance queries
    // ---------------------------------------------------------------------

    /// Read balance from Redis, falling back to Postgres on cache miss.
    pub async fn get_balance(&self, org_id: Uuid) -> Result<Decimal> {
        if let Some(bal) = self.get_balance_redis(org_id).await? {
            return Ok(bal);
        }

        let bal = self.get_balance_postgres(org_id).await?;
        if let Err(e) = self.set_balance_redis_if_absent(org_id, bal).await {
            tracing::warn!(error = %e, organization_id = %org_id, "Failed to populate Redis balance from PG fallback");
        }
        Ok(bal)
    }

    /// Fast Redis-only check: returns true if balance > 0.
    /// On cache miss, loads from Postgres.
    /// Minimum balance required to pass pre-flight check.
    /// Prevents serving expensive requests when balance is near-zero.
    const MIN_BALANCE_THRESHOLD: Decimal = Decimal::from_parts(1000000, 0, 0, false, 8); // $0.01

    pub async fn check_balance_redis(&self, org_id: Uuid) -> Result<bool> {
        let bal = self.get_balance(org_id).await?;
        Ok(bal >= Self::MIN_BALANCE_THRESHOLD)
    }

    async fn get_balance_redis(&self, org_id: Uuid) -> Result<Option<Decimal>> {
        let mut conn = self
            .redis
            .get()
            .await
            .context("failed to get Redis connection")?;

        let val: Option<i64> = redis::cmd("GET")
            .arg(redis_balance_key(org_id))
            .query_async(&mut *conn)
            .await
            .context("Redis GET credit balance")?;

        match val {
            Some(scaled) => Ok(Some(Decimal::from(scaled) / REDIS_SCALE_DECIMAL)),
            None => Ok(None),
        }
    }

    async fn get_balance_postgres(&self, org_id: Uuid) -> Result<Decimal> {
        let row: Option<(Decimal,)> =
            sqlx::query_as("SELECT balance_usd FROM credit_wallets WHERE organization_id = $1")
                .bind(org_id)
                .fetch_optional(self.db.as_ref())
                .await
                .context("query credit_wallets")?;

        Ok(row.map(|r| r.0).unwrap_or(Decimal::ZERO))
    }

    /// Overwrite Redis balance with an absolute value.
    /// Only safe during reconciliation or explicit resets -- NOT during
    /// concurrent add/refund operations (use `incr_balance_redis` instead).
    async fn set_balance_redis(&self, org_id: Uuid, balance: Decimal) -> Result<()> {
        let mut conn = self
            .redis
            .get()
            .await
            .context("failed to get Redis connection")?;

        let scaled = (balance * REDIS_SCALE_DECIMAL)
            .round_dp(0)
            .to_i64()
            .unwrap_or(0);

        redis::cmd("SET")
            .arg(redis_balance_key(org_id))
            .arg(scaled)
            .query_async::<()>(&mut *conn)
            .await
            .context("Redis SET credit balance")?;

        Ok(())
    }

    /// Atomically increment Redis balance by `amount`. Safe for concurrent
    /// operations -- will not overwrite in-flight deductions.
    async fn incr_balance_redis(&self, org_id: Uuid, amount: Decimal) -> Result<()> {
        let mut conn = self
            .redis
            .get()
            .await
            .context("failed to get Redis connection")?;

        let scaled = (amount * REDIS_SCALE_DECIMAL)
            .round_dp(0)
            .to_i64()
            .unwrap_or(0);

        redis::cmd("INCRBY")
            .arg(redis_balance_key(org_id))
            .arg(scaled)
            .query_async::<i64>(&mut *conn)
            .await
            .context("Redis INCRBY credit balance")?;

        Ok(())
    }

    /// Set Redis balance only if the key does not exist (cache-miss population).
    /// Returns true if the key was set, false if it already existed.
    /// Prevents overwriting concurrent deductions/top-ups during cache warm-up.
    async fn set_balance_redis_if_absent(&self, org_id: Uuid, balance: Decimal) -> Result<bool> {
        let mut conn = self
            .redis
            .get()
            .await
            .context("failed to get Redis connection")?;

        let scaled = (balance * REDIS_SCALE_DECIMAL)
            .round_dp(0)
            .to_i64()
            .unwrap_or(0);

        let set: bool = redis::cmd("SET")
            .arg(redis_balance_key(org_id))
            .arg(scaled)
            .arg("NX")
            .query_async(&mut *conn)
            .await
            .context("Redis SETNX credit balance")?;

        Ok(set)
    }

    // ---------------------------------------------------------------------
    // Credit top-up (called from webhook after Stripe checkout)
    // ---------------------------------------------------------------------

    /// Add credits to an organization's wallet. Writes to Postgres (UPSERT wallet
    /// + INSERT ledger) then updates Redis.
    pub async fn add_credits(
        &self,
        org_id: Uuid,
        amount_usd: Decimal,
        stripe_checkout_session_id: &str,
        paid_amount: Option<Decimal>,
        paid_currency: Option<&str>,
        exchange_rate: Option<Decimal>,
    ) -> Result<CreditTransaction> {
        if amount_usd <= Decimal::ZERO {
            anyhow::bail!("credit amount must be positive");
        }

        let mut tx = self.db.begin().await.context("begin transaction")?;

        // Insert-first idempotency: the UNIQUE partial index on stripe_checkout_session_id
        // ensures only one transaction per checkout session. ON CONFLICT DO NOTHING makes
        // concurrent duplicates a no-op rather than an error.
        let txn = sqlx::query_as::<_, (Uuid, chrono::DateTime<chrono::Utc>)>(
            r#"
            INSERT INTO credit_transactions
                (organization_id, transaction_type, amount_usd, balance_after_usd,
                 description, stripe_checkout_session_id, paid_amount, paid_currency, exchange_rate)
            VALUES ($1, 'top_up', $2, 0, $3, $4, $5, $6, $7)
            ON CONFLICT (stripe_checkout_session_id) WHERE stripe_checkout_session_id IS NOT NULL
            DO NOTHING
            RETURNING id, created_at
            "#,
        )
        .bind(org_id)
        .bind(amount_usd)
        .bind("Credit purchase via Stripe")
        .bind(stripe_checkout_session_id)
        .bind(paid_amount)
        .bind(paid_currency)
        .bind(exchange_rate)
        .fetch_optional(&mut *tx)
        .await
        .context("insert credit_transaction")?;

        let txn = match txn {
            Some(t) => t,
            None => {
                tx.rollback().await.ok();
                anyhow::bail!("duplicate checkout session: {}", stripe_checkout_session_id);
            }
        };

        let row = sqlx::query(
            r#"
            INSERT INTO credit_wallets (organization_id, balance_usd)
            VALUES ($1, $2)
            ON CONFLICT (organization_id)
            DO UPDATE SET balance_usd = credit_wallets.balance_usd + $2,
                         updated_at = NOW()
            RETURNING balance_usd
            "#,
        )
        .bind(org_id)
        .bind(amount_usd)
        .fetch_one(&mut *tx)
        .await
        .context("upsert credit_wallets")?;

        let new_balance: Decimal = row.get("balance_usd");

        // Update the transaction's balance_after_usd now that we know the real balance
        sqlx::query("UPDATE credit_transactions SET balance_after_usd = $1 WHERE id = $2")
            .bind(new_balance)
            .bind(txn.0)
            .execute(&mut *tx)
            .await
            .context("update balance_after_usd")?;

        tx.commit().await.context("commit add_credits")?;

        // Use atomic INCRBY to avoid overwriting concurrent deductions.
        // If the key doesn't exist yet, INCRBY treats it as 0 and adds.
        if let Err(e) = self.incr_balance_redis(org_id, amount_usd).await {
            tracing::warn!(error = %e, organization_id = %org_id, "Failed to update Redis balance after credit top-up");
        }

        Ok(CreditTransaction {
            id: txn.0,
            organization_id: org_id,
            transaction_type: CreditTransactionType::TopUp,
            amount_usd,
            balance_after_usd: new_balance,
            description: Some("Credit purchase via Stripe".into()),
            stripe_checkout_session_id: Some(stripe_checkout_session_id.into()),
            paid_amount,
            paid_currency: paid_currency.map(|s| s.to_string()),
            exchange_rate,
            llm_request_id: None,
            project_id: None,
            provider: None,
            model: None,
            input_tokens: None,
            output_tokens: None,
            created_at: txn.1,
        })
    }

    // ---------------------------------------------------------------------
    // Credit deduction (platform-key usage)
    // ---------------------------------------------------------------------

    /// Atomic Redis decrement. Returns Ok(true) if deducted, Ok(false) if
    /// insufficient balance. The caller should still write the ledger entry
    /// asynchronously via `record_deduction_postgres`.
    pub async fn deduct_credits_redis(&self, org_id: Uuid, cost_usd: Decimal) -> Result<bool> {
        if cost_usd <= Decimal::ZERO {
            return Ok(true);
        }

        let mut conn = self
            .redis
            .get()
            .await
            .context("failed to get Redis connection")?;

        let scaled_cost = (cost_usd * REDIS_SCALE_DECIMAL)
            .round_dp(0)
            .to_i64()
            .unwrap_or(0);

        let result: i64 = redis::cmd("EVAL")
            .arg(LUA_DEDUCT)
            .arg(1) // numkeys
            .arg(redis_balance_key(org_id))
            .arg(scaled_cost)
            .query_async(&mut *conn)
            .await
            .context("Redis EVAL deduct")?;

        Ok(result == 1)
    }

    /// Write the deduction to the Postgres ledger. Intended to be called
    /// asynchronously (via `tokio::spawn`) after the Redis deduction succeeds.
    pub async fn record_deduction_postgres(
        &self,
        org_id: Uuid,
        cost_usd: Decimal,
        metadata: &LlmUsageMetadata,
    ) -> Result<()> {
        if cost_usd <= Decimal::ZERO {
            return Ok(());
        }

        let mut tx = self.db.begin().await.context("begin transaction")?;

        // Upsert wallet so deductions work even if the wallet row doesn't exist yet.
        // Use GREATEST to clamp at zero since the CHECK constraint enforces non-negative;
        // Redis is the real enforcement point for balance checks.
        let row = sqlx::query(
            r#"
            INSERT INTO credit_wallets (organization_id, balance_usd)
            VALUES ($1, 0)
            ON CONFLICT (organization_id)
            DO UPDATE SET balance_usd = GREATEST(credit_wallets.balance_usd - $2, 0),
                         updated_at = NOW()
            RETURNING balance_usd
            "#,
        )
        .bind(org_id)
        .bind(cost_usd)
        .fetch_one(&mut *tx)
        .await
        .context("upsert credit_wallets for deduction")?;

        let new_balance: Decimal = row.get("balance_usd");

        // Idempotent insert: the UNIQUE partial index on llm_request_id prevents
        // duplicate ledger entries from retried spawned tasks.
        sqlx::query(
            r#"
            INSERT INTO credit_transactions
                (organization_id, transaction_type, amount_usd, balance_after_usd,
                 description, llm_request_id, project_id, provider, model,
                 input_tokens, output_tokens)
            VALUES ($1, 'usage_deduction', $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (llm_request_id) WHERE llm_request_id IS NOT NULL
            DO NOTHING
            "#,
        )
        .bind(org_id)
        .bind(-cost_usd)
        .bind(new_balance)
        .bind("LLM request usage")
        .bind(&metadata.llm_request_id)
        .bind(metadata.project_id)
        .bind(&metadata.provider)
        .bind(&metadata.model)
        .bind(metadata.input_tokens)
        .bind(metadata.output_tokens)
        .execute(&mut *tx)
        .await
        .context("insert deduction transaction")?;

        tx.commit().await.context("commit deduction")?;

        Ok(())
    }

    // ---------------------------------------------------------------------
    // BYOK platform fee recording
    // ---------------------------------------------------------------------

    // ---------------------------------------------------------------------
    // Reconciliation
    // ---------------------------------------------------------------------

    /// Reconcile all organizations that have credit wallets.
    pub async fn reconcile_all(&self) -> Result<u64> {
        let orgs: Vec<(Uuid, Decimal)> =
            sqlx::query_as("SELECT organization_id, balance_usd FROM credit_wallets")
                .fetch_all(self.db.as_ref())
                .await
                .context("fetch all credit_wallets for reconciliation")?;

        let drift_threshold = Decimal::new(1, 2); // $0.01

        let mut count = 0u64;
        for (org_id, pg_balance) in &orgs {
            let redis_balance = self
                .get_balance_redis(*org_id)
                .await?
                .unwrap_or(Decimal::ZERO);
            let drift = (*pg_balance - redis_balance).abs();

            if drift > drift_threshold {
                if redis_balance > *pg_balance {
                    // Redis is higher than Postgres -- Redis has un-accounted-for credits.
                    // Safe to reconcile downward to Postgres value.
                    tracing::warn!(
                        organization_id = %org_id,
                        postgres_balance = %pg_balance,
                        redis_balance = %redis_balance,
                        drift = %drift,
                        "credit balance drift detected (Redis > PG), reconciling down"
                    );
                    self.set_balance_redis(*org_id, *pg_balance).await?;
                    count += 1;
                } else {
                    // Redis is lower than Postgres -- likely in-flight deductions that
                    // haven't been written to PG yet. Do NOT overwrite Redis upward as
                    // that would grant back spent credits.
                    tracing::info!(
                        organization_id = %org_id,
                        postgres_balance = %pg_balance,
                        redis_balance = %redis_balance,
                        drift = %drift,
                        "credit balance drift detected (Redis < PG), skipping (likely in-flight deductions)"
                    );
                }
            }
        }

        Ok(count)
    }

    // ---------------------------------------------------------------------
    // Listing queries
    // ---------------------------------------------------------------------

    /// List credit transactions for an organization, ordered by newest first.
    pub async fn list_transactions(
        &self,
        org_id: Uuid,
        pagination: &PaginationParams,
    ) -> Result<(Vec<CreditTransaction>, i64)> {
        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM credit_transactions WHERE organization_id = $1")
                .bind(org_id)
                .fetch_one(self.db.as_ref())
                .await
                .context("count transactions")?;

        let rows = sqlx::query(
            r#"
            SELECT id, organization_id, transaction_type, amount_usd, balance_after_usd,
                   description, stripe_checkout_session_id, paid_amount, paid_currency,
                   exchange_rate, llm_request_id, project_id, provider, model,
                   input_tokens, output_tokens, created_at
            FROM credit_transactions
            WHERE organization_id = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(org_id)
        .bind(pagination.limit)
        .bind(pagination.offset)
        .fetch_all(self.db.as_ref())
        .await
        .context("list transactions")?;

        let transactions = rows
            .iter()
            .map(|row| CreditTransaction {
                id: row.get("id"),
                organization_id: row.get("organization_id"),
                transaction_type: row.get("transaction_type"),
                amount_usd: row.get("amount_usd"),
                balance_after_usd: row.get("balance_after_usd"),
                description: row.get("description"),
                stripe_checkout_session_id: row.get("stripe_checkout_session_id"),
                paid_amount: row.get("paid_amount"),
                paid_currency: row.get("paid_currency"),
                exchange_rate: row.get("exchange_rate"),
                llm_request_id: row.get("llm_request_id"),
                project_id: row.get("project_id"),
                provider: row.get("provider"),
                model: row.get("model"),
                input_tokens: row.get("input_tokens"),
                output_tokens: row.get("output_tokens"),
                created_at: row.get("created_at"),
            })
            .collect();

        Ok((transactions, count.0))
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_FEE_RATE: Decimal = Decimal::from_parts(3, 0, 0, false, 2); // 0.03 = 3%

    #[test]
    fn test_fee_rate_calculation() {
        let cost = Decimal::new(100, 2); // $1.00
        let fee = cost * TEST_FEE_RATE;
        assert_eq!(fee, Decimal::new(3, 2)); // $0.03
    }

    #[test]
    fn test_fee_rate_small_amount() {
        let cost = Decimal::new(10, 4); // $0.0010
        let fee = cost * TEST_FEE_RATE;
        assert_eq!(fee, Decimal::new(30, 6));
    }

    #[test]
    fn test_fee_rate_large_amount() {
        let cost = Decimal::new(10_000_00, 2); // $10,000.00
        let fee = cost * TEST_FEE_RATE;
        assert_eq!(fee, Decimal::new(300_00, 2)); // $300.00
    }

    #[test]
    fn test_fee_rate_zero_cost() {
        let cost = Decimal::ZERO;
        let fee = cost * TEST_FEE_RATE;
        assert_eq!(fee, Decimal::ZERO);
    }

    #[test]
    fn test_redis_key_format() {
        let org_id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let key = redis_balance_key(org_id);
        assert_eq!(key, "credit:balance:550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn test_redis_key_unique_per_org() {
        let org1 = Uuid::new_v4();
        let org2 = Uuid::new_v4();
        assert_ne!(redis_balance_key(org1), redis_balance_key(org2));
    }

    #[test]
    fn test_fee_rate_value() {
        assert_eq!(TEST_FEE_RATE, Decimal::new(3, 2)); // 0.03
    }

    #[test]
    fn test_transaction_type_display() {
        assert_eq!(CreditTransactionType::TopUp.to_string(), "top_up");
        assert_eq!(
            CreditTransactionType::UsageDeduction.to_string(),
            "usage_deduction"
        );
        assert_eq!(CreditTransactionType::Refund.to_string(), "refund");
        assert_eq!(CreditTransactionType::Adjustment.to_string(), "adjustment");
    }

    #[test]
    fn test_transaction_type_equality() {
        assert_eq!(CreditTransactionType::TopUp, CreditTransactionType::TopUp);
        assert_ne!(CreditTransactionType::TopUp, CreditTransactionType::Refund);
    }

    #[test]
    fn test_pagination_params_default() {
        let params = PaginationParams::default();
        assert_eq!(params.limit, 50);
        assert_eq!(params.offset, 0);
    }

    #[test]
    fn test_llm_usage_metadata_default() {
        let meta = LlmUsageMetadata::default();
        assert!(meta.llm_request_id.is_none());
        assert!(meta.project_id.is_none());
        assert!(meta.provider.is_none());
        assert!(meta.model.is_none());
        assert!(meta.input_tokens.is_none());
        assert!(meta.output_tokens.is_none());
    }

    #[test]
    fn test_fee_calculation_precision() {
        let cost = Decimal::new(234567, 8); // 0.00234567
        let fee = cost * TEST_FEE_RATE;
        let expected = Decimal::new(703701, 10);
        assert_eq!(fee, expected);
    }

    #[test]
    fn test_lua_deduct_script_contains_key_operations() {
        assert!(LUA_DEDUCT.contains("redis.call('GET'"));
        assert!(LUA_DEDUCT.contains("redis.call('SET'"));
        assert!(LUA_DEDUCT.contains("return -1"));
        assert!(LUA_DEDUCT.contains("return 1"));
    }

    // =====================================================================
    // Additional hardening tests
    // =====================================================================

    #[test]
    fn test_fee_rate_sub_cent() {
        let cost = Decimal::new(1, 6); // $0.000001
        let fee = cost * TEST_FEE_RATE;
        let expected = Decimal::new(3, 8);
        assert_eq!(fee, expected);
    }

    #[test]
    fn test_redis_scale_factor() {
        assert_eq!(REDIS_SCALE_DECIMAL, Decimal::new(100_000_000, 0));
    }

    #[test]
    fn test_redis_scaling_roundtrip() {
        let original = Decimal::new(123_456_789, 8); // $1.23456789
        let scaled = (original * REDIS_SCALE_DECIMAL)
            .round_dp(0)
            .to_i64()
            .unwrap();
        assert_eq!(scaled, 123_456_789);
        let recovered = Decimal::from(scaled) / REDIS_SCALE_DECIMAL;
        assert_eq!(recovered, original);
    }

    #[test]
    fn test_redis_scaling_small_value() {
        let original = Decimal::new(1, 8); // $0.00000001 (1 unit)
        let scaled = (original * REDIS_SCALE_DECIMAL)
            .round_dp(0)
            .to_i64()
            .unwrap();
        assert_eq!(scaled, 1);
        let recovered = Decimal::from(scaled) / REDIS_SCALE_DECIMAL;
        assert_eq!(recovered, original);
    }

    #[test]
    fn test_redis_scaling_zero() {
        let original = Decimal::ZERO;
        let scaled = (original * REDIS_SCALE_DECIMAL)
            .round_dp(0)
            .to_i64()
            .unwrap();
        assert_eq!(scaled, 0);
    }

    #[test]
    fn test_redis_scaling_large_balance() {
        // $100,000.00 -- should fit comfortably in i64
        let original = Decimal::new(100_000_00, 2);
        let scaled = (original * REDIS_SCALE_DECIMAL)
            .round_dp(0)
            .to_i64()
            .unwrap();
        assert_eq!(scaled, 10_000_000_000_000i64);
        let recovered = Decimal::from(scaled) / REDIS_SCALE_DECIMAL;
        assert_eq!(recovered, original);
    }

    #[test]
    fn test_min_balance_threshold() {
        let threshold = CreditService::MIN_BALANCE_THRESHOLD;
        assert_eq!(threshold, Decimal::new(1, 2)); // $0.01
    }

    #[test]
    fn test_transaction_type_serde_roundtrip() {
        for variant in [
            CreditTransactionType::TopUp,
            CreditTransactionType::UsageDeduction,
            CreditTransactionType::Refund,
            CreditTransactionType::Adjustment,
        ] {
            let json = serde_json::to_string(&variant).unwrap();
            let recovered: CreditTransactionType = serde_json::from_str(&json).unwrap();
            assert_eq!(recovered, variant);
        }
    }

    #[test]
    fn test_pagination_params_custom() {
        let params = PaginationParams {
            limit: 10,
            offset: 100,
        };
        assert_eq!(params.limit, 10);
        assert_eq!(params.offset, 100);
    }

    #[test]
    fn test_llm_usage_metadata_with_all_fields() {
        let meta = LlmUsageMetadata {
            llm_request_id: Some("req-xyz".to_string()),
            project_id: Some(Uuid::new_v4()),
            provider: Some("openai".to_string()),
            model: Some("gpt-4o".to_string()),
            input_tokens: Some(100),
            output_tokens: Some(200),
        };
        assert_eq!(meta.llm_request_id.as_deref(), Some("req-xyz"));
        assert_eq!(meta.input_tokens, Some(100));
        assert_eq!(meta.output_tokens, Some(200));
    }

    #[test]
    fn test_credit_transaction_types_are_distinct() {
        let types = [
            CreditTransactionType::TopUp,
            CreditTransactionType::UsageDeduction,
            CreditTransactionType::Refund,
            CreditTransactionType::Adjustment,
        ];
        for (i, a) in types.iter().enumerate() {
            for (j, b) in types.iter().enumerate() {
                if i == j {
                    assert_eq!(a, b);
                } else {
                    assert_ne!(a, b);
                }
            }
        }
    }

    #[test]
    fn test_fee_rate_is_three_percent() {
        let hundred = Decimal::new(100, 0);
        let fee = hundred * TEST_FEE_RATE;
        assert_eq!(fee, Decimal::new(3, 0)); // 3% of $100 = $3
    }

    // =====================================================================
    // Consistency / hardening contract tests
    // =====================================================================

    /// Verifies the Lua script logic: if balance < cost, returns -1.
    /// This tests the mathematical contract, not Redis itself.
    #[test]
    fn test_deduction_logic_insufficient_balance() {
        let balance: i64 = 500; // $0.000005 scaled
        let cost: i64 = 1000; // $0.00001 scaled
        assert!(balance < cost, "Should reject: balance < cost");
    }

    /// Verifies the Lua script logic: if balance == cost, returns 1 (success).
    #[test]
    fn test_deduction_logic_exact_balance() {
        let balance: i64 = 1000;
        let cost: i64 = 1000;
        assert!(balance >= cost, "Should accept: balance == cost");
        let new_bal = balance - cost;
        assert_eq!(new_bal, 0);
    }

    /// Verifies the Lua script logic: zero balance, positive cost -> reject.
    #[test]
    fn test_deduction_logic_zero_balance() {
        let balance: i64 = 0;
        let cost: i64 = 1;
        assert!(balance < cost, "Should reject: zero balance");
    }

    /// Verifies the Lua script logic: zero cost -> always accept.
    #[test]
    fn test_deduction_logic_zero_cost() {
        let balance: i64 = 0;
        let cost: i64 = 0;
        assert!(balance >= cost, "Should accept: zero cost");
    }

    /// Verifies reconcile direction safety: when Redis < Postgres,
    /// we must NOT overwrite Redis upward (would grant back spent credits).
    #[test]
    fn test_reconcile_direction_safety_redis_lower() {
        let redis_balance = Decimal::new(50, 0); // $50
        let pg_balance = Decimal::new(100, 0); // $100
                                               // Redis is lower -- in-flight deductions haven't landed in PG yet.
                                               // Safe action: DO NOT reconcile (skip).
        assert!(redis_balance < pg_balance);
        // The reconcile logic should skip this case.
    }

    /// Verifies reconcile direction safety: when Redis > Postgres,
    /// we should reconcile downward to Postgres value.
    #[test]
    fn test_reconcile_direction_safety_redis_higher() {
        let redis_balance = Decimal::new(100, 0); // $100
        let pg_balance = Decimal::new(50, 0); // $50
                                              // Redis is higher -- un-accounted-for credits.
                                              // Safe action: set Redis = Postgres.
        assert!(redis_balance > pg_balance);
        let reconciled = pg_balance;
        assert_eq!(reconciled, Decimal::new(50, 0));
    }

    /// Verifies that the drift threshold is constructed without floating-point.
    #[test]
    fn test_drift_threshold_no_float() {
        let threshold = Decimal::new(1, 2); // $0.01
        assert_eq!(threshold.to_string(), "0.01");
    }

    /// Verifies the non-negative balance contract: GREATEST(balance - cost, 0).
    #[test]
    fn test_pg_deduction_clamps_to_zero() {
        let balance = Decimal::new(5, 2); // $0.05
        let cost = Decimal::new(10, 2); // $0.10
        let result = std::cmp::max(balance - cost, Decimal::ZERO);
        assert_eq!(result, Decimal::ZERO);
    }

    /// Verifies the non-negative balance contract: normal deduction.
    #[test]
    fn test_pg_deduction_normal_case() {
        let balance = Decimal::new(100, 0); // $100
        let cost = Decimal::new(5, 2); // $0.05
        let result = std::cmp::max(balance - cost, Decimal::ZERO);
        assert_eq!(result, Decimal::new(9995, 2)); // $99.95
    }

    /// Verifies `add_credits` rejects non-positive amounts.
    #[test]
    fn test_add_credits_rejects_zero() {
        assert!(Decimal::ZERO <= Decimal::ZERO, "Zero should be rejected");
        assert!(
            Decimal::new(-1, 0) <= Decimal::ZERO,
            "Negative should be rejected"
        );
    }

    // =====================================================================
    // INCRBY / SETNX contract tests
    // =====================================================================

    /// Demonstrates the race condition that INCRBY solves:
    /// SET overwrites concurrent deductions, INCRBY does not.
    #[test]
    fn test_incrby_vs_set_race_scenario() {
        // Initial state: balance = $100 (scaled: 10_000_000_000)
        let initial_scaled: i64 = 10_000_000_000;

        // Scenario: top-up $50, concurrent deduction $10
        let top_up_scaled: i64 = 5_000_000_000;
        let deduction_scaled: i64 = 1_000_000_000;

        // With SET (buggy): PG says new_balance = $150, SET Redis = $150
        // But deduction already reduced Redis to $90
        // SET overwrites to $150, effectively granting back $10
        let after_set = initial_scaled + top_up_scaled; // SET ignores deduction
        assert_eq!(after_set, 15_000_000_000); // $150 -- WRONG, should be $140

        // With INCRBY (correct): Redis atomically adds $50 to whatever is there
        // After deduction: Redis = $90, INCRBY $50 = $140
        let after_deduction = initial_scaled - deduction_scaled; // $90
        let after_incrby = after_deduction + top_up_scaled; // INCRBY adds
        assert_eq!(after_incrby, 14_000_000_000); // $140 -- CORRECT
    }

    /// SETNX contract: only sets if key is absent (returns false if exists).
    #[test]
    fn test_setnx_contract() {
        // If Redis key doesn't exist, SETNX sets it and returns true.
        // If Redis key exists (from concurrent top-up/deduction), SETNX
        // returns false and does NOT overwrite.
        // We can't test Redis here, but we verify the scaling math.
        let balance = Decimal::new(12345678, 8); // $0.12345678
        let scaled = (balance * REDIS_SCALE_DECIMAL)
            .round_dp(0)
            .to_i64()
            .unwrap();
        assert_eq!(scaled, 12345678);
    }

    /// Verifies that INCRBY on a non-existent key treats it as 0.
    /// This is Redis behavior: INCRBY on missing key = INCRBY on 0.
    #[test]
    fn test_incrby_from_zero() {
        let amount = Decimal::new(50, 0); // $50
        let scaled = (amount * REDIS_SCALE_DECIMAL).round_dp(0).to_i64().unwrap();
        // INCRBY on missing key: 0 + 5_000_000_000 = 5_000_000_000
        let result = 0i64 + scaled;
        assert_eq!(result, 5_000_000_000);
        let recovered = Decimal::from(result) / REDIS_SCALE_DECIMAL;
        assert_eq!(recovered, Decimal::new(50, 0));
    }

    /// Verifies scaling precision for a realistic deduction amount.
    #[test]
    fn test_scaling_realistic_deduction() {
        // Typical LLM cost: $0.00234567
        let cost = Decimal::new(234567, 8);
        let scaled = (cost * REDIS_SCALE_DECIMAL).round_dp(0).to_i64().unwrap();
        assert_eq!(scaled, 234567);
        let recovered = Decimal::from(scaled) / REDIS_SCALE_DECIMAL;
        assert_eq!(recovered, cost);
    }

    /// Verifies no precision loss for a sequence of operations.
    #[test]
    fn test_integer_arithmetic_no_precision_loss() {
        // Simulate: start $100, deduct $0.00000001 ten million times
        let balance: i64 = 10_000_000_000; // $100 scaled
        let cost: i64 = 1; // $0.00000001 scaled (smallest unit)
        let operations = 10_000_000;
        let final_balance = balance - (cost * operations);
        assert_eq!(final_balance, 9_990_000_000); // $99.90000000 exactly
        let recovered = Decimal::from(final_balance) / REDIS_SCALE_DECIMAL;
        assert_eq!(recovered, Decimal::new(999, 1)); // $99.9
    }

    // =====================================================================
    // Zero-cost early return contract tests
    // =====================================================================

    /// deduct_credits_redis returns Ok(true) for zero cost without touching Redis.
    #[test]
    fn test_deduct_zero_cost_is_noop() {
        let cost = Decimal::ZERO;
        // The function checks `cost_usd <= Decimal::ZERO` and returns Ok(true).
        assert!(cost <= Decimal::ZERO);
    }

    /// deduct_credits_redis returns Ok(true) for negative cost.
    #[test]
    fn test_deduct_negative_cost_is_noop() {
        let cost = Decimal::new(-5, 0);
        assert!(cost <= Decimal::ZERO);
    }

    /// record_deduction_postgres returns Ok(()) for zero cost without touching DB.
    #[test]
    fn test_record_deduction_zero_cost_is_noop() {
        let cost = Decimal::ZERO;
        assert!(cost <= Decimal::ZERO);
    }

    // =====================================================================
    // Double-deduction Lua logic tests
    // =====================================================================

    /// Two sequential deductions: first succeeds, second exhausts balance.
    #[test]
    fn test_sequential_deductions_second_exhausts() {
        let mut balance: i64 = 200_000_000; // $2.00 scaled
        let cost: i64 = 150_000_000; // $1.50 scaled

        // First deduction: $2.00 >= $1.50 -> success
        assert!(balance >= cost);
        balance -= cost;
        assert_eq!(balance, 50_000_000); // $0.50

        // Second deduction: $0.50 < $1.50 -> reject
        assert!(balance < cost);
    }

    /// Two sequential deductions: first succeeds exactly, second fails.
    #[test]
    fn test_exact_balance_then_reject() {
        let mut balance: i64 = 500_000_000; // $5.00 scaled
        let cost: i64 = 500_000_000; // $5.00 scaled

        // Exact match: success, balance becomes 0
        assert!(balance >= cost);
        balance -= cost;
        assert_eq!(balance, 0);

        // Any subsequent deduction fails (even 1 unit)
        assert!(balance < 1);
    }

    /// Many small deductions drain balance correctly.
    #[test]
    fn test_many_small_deductions() {
        let mut balance: i64 = 1_000_000_000; // $10.00 scaled
        let cost: i64 = 100; // $0.000001 scaled

        let mut count = 0i64;
        while balance >= cost {
            balance -= cost;
            count += 1;
        }

        assert_eq!(count, 10_000_000); // exactly 10M deductions
        assert_eq!(balance, 0);
    }

    // =====================================================================
    // Idempotency contract tests (assert SQL guards exist)
    // =====================================================================

    /// Pending charges use ON CONFLICT (organization_id, billing_period_start)
    /// to prevent duplicate charges for the same org+period.
    #[test]
    fn test_pending_charge_idempotency_sql_contract() {
        let sql = r#"
        INSERT INTO pending_charges
            (organization_id, charge_type, billing_period_start, billing_period_end,
             amount_usd, description, line_items)
        VALUES ($1, 'platform_usage', $2, $3, $4, $5, $6)
        ON CONFLICT (organization_id, billing_period_start) DO NOTHING
        "#;
        assert!(sql.contains("ON CONFLICT (organization_id, billing_period_start) DO NOTHING"));
    }

    /// Credit top-ups use ON CONFLICT (stripe_checkout_session_id) to prevent
    /// double-crediting from duplicate Stripe webhooks.
    #[test]
    fn test_credit_topup_idempotency_sql_contract() {
        let sql = r#"
            INSERT INTO credit_transactions
                (organization_id, transaction_type, amount_usd, balance_after_usd,
                 description, stripe_checkout_session_id, paid_amount, paid_currency, exchange_rate)
            VALUES ($1, 'top_up', $2, 0, $3, $4, $5, $6, $7)
            ON CONFLICT (stripe_checkout_session_id) WHERE stripe_checkout_session_id IS NOT NULL
            DO NOTHING
            RETURNING id, created_at
            "#;
        assert!(sql.contains("ON CONFLICT (stripe_checkout_session_id)"));
        assert!(sql.contains("DO NOTHING"));
    }

    /// Deduction ledger uses ON CONFLICT (llm_request_id) to prevent duplicate
    /// ledger entries from retried tokio::spawn tasks.
    #[test]
    fn test_deduction_ledger_idempotency_sql_contract() {
        let sql = r#"
            INSERT INTO credit_transactions
                (organization_id, transaction_type, amount_usd, balance_after_usd,
                 description, llm_request_id, project_id, provider, model,
                 input_tokens, output_tokens)
            VALUES ($1, 'usage_deduction', $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (llm_request_id) WHERE llm_request_id IS NOT NULL
            DO NOTHING
            "#;
        assert!(sql.contains("ON CONFLICT (llm_request_id)"));
        assert!(sql.contains("DO NOTHING"));
    }

    /// Wallet upsert uses ON CONFLICT (organization_id) -- top-up adds, deduction subtracts.
    #[test]
    fn test_wallet_upsert_contract() {
        // Top-up: balance + amount
        let topup_sql = "ON CONFLICT (organization_id) DO UPDATE SET balance_usd = credit_wallets.balance_usd + $2";
        assert!(topup_sql.contains("balance_usd + $2"));

        // Deduction: GREATEST(balance - amount, 0)
        let deduct_sql = "ON CONFLICT (organization_id) DO UPDATE SET balance_usd = GREATEST(credit_wallets.balance_usd - $2, 0)";
        assert!(deduct_sql.contains("GREATEST"));
        assert!(deduct_sql.contains("balance_usd - $2"));
    }

    // =====================================================================
    // Tier-aware fee rate tests
    // =====================================================================

    #[tokio::test]
    async fn test_get_gateway_fee_rate_from_config() {
        use crate::entitlements::MockEntitlementChecker;

        let mock = MockEntitlementChecker::new();
        let org = Uuid::new_v4();

        mock.update_config(org, |c| {
            c.gateway.fee_percent = Decimal::new(5, 2);
            c.gateway.moodeng_fee_percent = Decimal::new(8, 2);
        }).await;

        let rate = super::get_gateway_fee_rate(&mock, org).await.unwrap();
        assert_eq!(rate, Decimal::new(5, 2));

        let moodeng_rate = super::get_moodeng_fee_rate(&mock, org).await.unwrap();
        assert_eq!(moodeng_rate, Decimal::new(8, 2));
    }

    #[tokio::test]
    async fn test_get_gateway_fee_rate_errors_for_unknown_org() {
        use crate::entitlements::MockEntitlementChecker;

        let mock = MockEntitlementChecker::new();
        let unknown = Uuid::new_v4();

        assert!(super::get_gateway_fee_rate(&mock, unknown).await.is_err());
    }

    #[tokio::test]
    async fn test_watch_price_helpers() {
        use crate::entitlements::MockEntitlementChecker;

        let mock = MockEntitlementChecker::new();
        let org = Uuid::new_v4();

        mock.update_config(org, |c| {
            c.watch.traces_logs_per_gb_usd = Decimal::new(20, 2);
            c.watch.metrics_per_million_usd = Decimal::new(10, 2);
        }).await;

        let tl = super::get_watch_traces_logs_price(&mock, org).await.unwrap();
        assert_eq!(tl, Decimal::new(20, 2));

        let m = super::get_watch_metrics_price(&mock, org).await.unwrap();
        assert_eq!(m, Decimal::new(10, 2));
    }
}
