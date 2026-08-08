//! Stripe implementation of the PaymentProvider trait.

use async_trait::async_trait;
use sqlx::Row;
use std::sync::Arc;
use stripe::Client;
use stripe_billing::billing_portal_session::CreateBillingPortalSession;
use stripe_billing::invoice::ListInvoice;
use stripe_billing::subscription::{
    CancelSubscription, CreateSubscription, CreateSubscriptionItems, RetrieveSubscription,
    UpdateSubscription, UpdateSubscriptionItems,
};
use stripe_core::customer::CreateCustomer;
use stripe_core::payment_intent::CreatePaymentIntent;
use stripe_core::setup_intent::{CreateSetupIntent, RetrieveSetupIntent};
use stripe_payment::payment_method::{
    DetachPaymentMethod, ListPaymentMethod, RetrievePaymentMethod,
};
use stripe_types::{Currency, Expandable};
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

use crate::app_state::RedisPool;
use crate::db::DbPool;
use crate::rate_limit::{acquire_billing_lock, LockError};

use super::payments::{subscription_status, PaymentMethodStatus};
use super::provider::*;
use super::utils::{is_retriable_error, RetryConfig};

/// Default TTL for subscription creation lock (30 seconds).
/// This should be long enough to cover the Stripe API call and database operations.
const SUBSCRIPTION_LOCK_TTL_SECONDS: u64 = 30;

/// Stripe implementation of the PaymentProvider trait.
pub struct StripePaymentProvider {
    client: Client,
    db: Arc<DbPool>,
    redis: Arc<RedisPool>,
    webhook_secret: Option<String>,
    metered_price_id: Option<String>,
}

impl StripePaymentProvider {
    /// Create a new Stripe payment provider.
    ///
    /// # Arguments
    /// * `api_key` - Stripe secret API key (sk_test_... or sk_live_...)
    /// * `db` - Database connection pool
    /// * `redis` - Redis connection pool for distributed locking
    /// * `webhook_secret` - Optional webhook signing secret (whsec_...)
    ///
    /// # Security
    /// - The API key is stored in memory only, never logged
    /// - Use environment variables, never hardcode keys
    pub fn new(
        api_key: &str,
        db: Arc<DbPool>,
        redis: Arc<RedisPool>,
        webhook_secret: Option<String>,
        metered_price_id: Option<String>,
    ) -> Self {
        // Validate API key format without logging the actual key
        // Stripe API keys follow strict format: sk_test_... or sk_live_...
        if !api_key.starts_with("sk_test_") && !api_key.starts_with("sk_live_") {
            // Log at error level since malformed keys will cause all payment operations to fail
            error!(
                "SECURITY: Stripe API key has invalid format (should start with sk_test_ or sk_live_). \
                 All payment operations will fail until this is corrected. \
                 Verify STRIPE_API_KEY environment variable is set correctly."
            );
        } else if api_key.starts_with("sk_live_") {
            info!("Stripe payment provider initialized with production API key");
        } else {
            info!("Stripe payment provider initialized with test API key");
        }

        // Validate webhook secret format without logging the actual secret
        // Stripe webhook secrets follow format: whsec_...
        if let Some(ref secret) = webhook_secret {
            if !secret.starts_with("whsec_") {
                error!(
                    "SECURITY: Stripe webhook secret has invalid format (should start with whsec_). \
                     Webhook signature verification will fail until this is corrected. \
                     Verify STRIPE_WEBHOOK_SECRET environment variable is set correctly."
                );
            }
        }

        let client = Client::new(api_key);
        Self {
            client,
            db,
            redis,
            webhook_secret,
            metered_price_id,
        }
    }

    /// Get organization details for customer creation.
    async fn get_org_details(
        &self,
        organization_id: Uuid,
    ) -> PaymentResult<(String, Option<String>)> {
        let row = sqlx::query(
            r#"
            SELECT o.id, o.name, u.email
            FROM organizations o
            LEFT JOIN memberships m ON m.organization_id = o.id AND m.role = 'owner'
            LEFT JOIN users u ON u.id = m.user_id
            WHERE o.id = $1
            LIMIT 1
            "#,
        )
        .bind(organization_id)
        .fetch_optional(self.db.as_ref())
        .await?;

        match row {
            Some(r) => Ok((r.get("name"), r.get("email"))),
            None => Err(PaymentError::CustomerNotFound(format!(
                "Organization {} not found",
                organization_id
            ))),
        }
    }

    /// Store a payment method in the database.
    /// Uses a transaction to ensure atomicity when setting as default.
    async fn store_payment_method(
        &self,
        organization_id: Uuid,
        customer_id: &str,
        payment_method_id: &str,
        display_name: Option<&str>,
        card_brand: Option<&str>,
        card_last_four: Option<&str>,
        card_exp_month: Option<i32>,
        card_exp_year: Option<i32>,
        is_default: bool,
        created_by: Option<Uuid>,
    ) -> PaymentResult<Uuid> {
        // Use a transaction to ensure atomicity
        let mut tx = self.db.begin().await?;

        // If setting as default, unset other defaults first
        if is_default {
            sqlx::query("UPDATE payment_methods SET is_default = false WHERE organization_id = $1")
                .bind(organization_id)
                .execute(&mut *tx)
                .await?;
        }

        // Use ON CONFLICT to handle race conditions where the same payment method
        // is confirmed simultaneously. The provider_payment_method_id should be unique.
        // If a duplicate exists, we return the existing row's ID instead of inserting.
        let row = sqlx::query(
            r#"
            INSERT INTO payment_methods (
                organization_id, provider, status,
                provider_customer_id, provider_payment_method_id,
                display_name, card_brand, card_last_four, card_exp_month, card_exp_year,
                is_default, created_by
            )
            VALUES ($1, 'stripe', 'active', $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (provider_payment_method_id) WHERE provider_payment_method_id IS NOT NULL DO UPDATE
            SET updated_at = NOW()
            RETURNING id, (xmax = 0) AS inserted
            "#,
        )
        .bind(organization_id)
        .bind(customer_id)
        .bind(payment_method_id)
        .bind(display_name)
        .bind(card_brand)
        .bind(card_last_four)
        .bind(card_exp_month)
        .bind(card_exp_year)
        .bind(is_default)
        .bind(created_by)
        .fetch_one(&mut *tx)
        .await?;

        let pm_id: Uuid = row.get("id");
        let was_inserted: bool = row.get("inserted");

        // If this was a duplicate (race condition), skip the default payment method update
        // since the first request would have handled it
        if !was_inserted {
            trace!(
                payment_method_id = %payment_method_id,
                "Payment method already exists - race condition handled"
            );
            tx.commit().await?;
            return Ok(pm_id);
        }

        // Update organization's default payment method if requested
        if is_default {
            sqlx::query("UPDATE organizations SET default_payment_method_id = $1 WHERE id = $2")
                .bind(pm_id)
                .bind(organization_id)
                .execute(&mut *tx)
                .await?;
        }

        // Commit the transaction
        tx.commit().await?;

        Ok(pm_id)
    }

    /// Fetch payment methods from the Stripe API for a customer and store any
    /// that are missing locally.  Returns `true` if at least one active default
    /// payment method now exists in the DB.
    ///
    /// This is used as a fallback when the local DB has no record — e.g. the
    /// `payment_method.attached` webhook was never delivered (missing Stripe
    /// webhook configuration) or was processed before the handler existed.
    pub async fn sync_payment_methods_from_stripe(
        &self,
        organization_id: Uuid,
    ) -> PaymentResult<bool> {
        let customer = match self.get_customer(organization_id).await? {
            Some(c) => c,
            None => return Ok(false),
        };

        let methods = ListPaymentMethod::new()
            .customer(&customer.provider_customer_id)
            .limit(10)
            .send(&self.client)
            .await
            .map_err(|e| self.map_stripe_error(e))?;

        if methods.data.is_empty() {
            return Ok(false);
        }

        let mut stored_any = false;
        for (i, pm) in methods.data.iter().enumerate() {
            let pm_id_str = pm.id.to_string();

            let (card_brand, card_last_four, card_exp_month, card_exp_year) =
                if let Some(card) = &pm.card {
                    (
                        Some(card.brand.clone()),
                        Some(card.last4.clone()),
                        Some(card.exp_month as i32),
                        Some(card.exp_year as i32),
                    )
                } else {
                    (None, None, None, None)
                };

            let display_name = card_brand
                .as_ref()
                .zip(card_last_four.as_ref())
                .map(|(brand, last4)| format!("{} ending in {}", brand, last4));

            let is_default = i == 0;

            match self
                .store_payment_method(
                    organization_id,
                    &customer.provider_customer_id,
                    &pm_id_str,
                    display_name.as_deref(),
                    card_brand.as_deref(),
                    card_last_four.as_deref(),
                    card_exp_month,
                    card_exp_year,
                    is_default,
                    None,
                )
                .await
            {
                Ok(_) => {
                    stored_any = true;
                    info!(
                        organization_id = %organization_id,
                        payment_method_id = %pm_id_str,
                        "Synced payment method from Stripe"
                    );
                }
                Err(e) => {
                    warn!(
                        organization_id = %organization_id,
                        payment_method_id = %pm_id_str,
                        error = %e,
                        "Failed to sync payment method from Stripe"
                    );
                }
            }
        }

        Ok(stored_any)
    }

    /// Map Stripe errors to PaymentError with appropriate categorization.
    fn map_stripe_error(&self, err: stripe::StripeError) -> PaymentError {
        match &err {
            stripe::StripeError::Stripe(api_errors, _status) => {
                trace!("Stripe API error: code={:?}", api_errors.code);

                let code_str = api_errors.code.as_ref().map(|c| c.as_str());
                let msg = api_errors
                    .message
                    .clone()
                    .unwrap_or_else(|| "Payment failed".into());

                match code_str {
                    Some("card_declined") => PaymentError::PaymentDeclined(msg),
                    Some("expired_card") => PaymentError::CardExpired,
                    Some("rate_limit") => PaymentError::RateLimited,
                    Some("resource_missing") => PaymentError::SubscriptionAlreadyCanceled,
                    _ => {
                        let msg_lower = msg.to_lowercase();
                        if msg_lower.contains("already canceled")
                            || msg_lower.contains("cannot be canceled")
                            || msg_lower.contains("no such subscription")
                        {
                            PaymentError::SubscriptionAlreadyCanceled
                        } else if msg_lower.contains("insufficient funds") {
                            PaymentError::InsufficientFunds
                        } else if msg_lower.contains("authentication") {
                            PaymentError::AuthorizationRequired(msg)
                        } else {
                            PaymentError::ProviderError(msg)
                        }
                    }
                }
            }
            _ => PaymentError::ProviderError(format!("Stripe error: {}", err)),
        }
    }

    /// Create a Stripe customer with retry logic for transient failures.
    ///
    /// Implements exponential backoff retry for rate limits and network errors.
    ///
    /// # Idempotency
    /// Application-level idempotency is provided by the database row lock in
    /// `get_or_create_customer()`. The lock ensures:
    /// 1. Only one request can create a customer for an organization at a time
    /// 2. If Stripe call succeeds but DB insert fails, subsequent calls will
    ///    create a new Stripe customer (acceptable - orphaned customers can be
    ///    cleaned up via Stripe dashboard or reconciliation job)
    /// 3. The ON CONFLICT clause handles race conditions where the same customer
    ///    is inserted twice
    ///
    async fn create_customer_with_retry(
        &self,
        name: &str,
        email: Option<&str>,
        organization_id: Uuid,
    ) -> PaymentResult<stripe_core::Customer> {
        let config = RetryConfig::default();
        let mut attempt = 0;
        let mut delay = config.initial_delay;

        loop {
            let mut req = CreateCustomer::new().name(name).metadata(
                [(String::from("organization_id"), organization_id.to_string())]
                    .into_iter()
                    .collect::<std::collections::HashMap<String, String>>(),
            );
            if let Some(e) = email {
                req = req.email(e);
            }

            match req.send(&self.client).await {
                Ok(customer) => return Ok(customer),
                Err(err) => {
                    let payment_err = self.map_stripe_error(err);

                    if attempt >= config.max_retries || !is_retriable_error(&payment_err) {
                        return Err(payment_err);
                    }

                    attempt += 1;
                    warn!(
                        operation = "CreateCustomer",
                        attempt = attempt,
                        max_retries = config.max_retries,
                        delay_ms = delay.as_millis() as u64,
                        error = %payment_err,
                        "Retrying after transient error"
                    );

                    tokio::time::sleep(delay).await;

                    delay = std::time::Duration::from_millis(
                        (delay.as_millis() as f64 * config.multiplier) as u64,
                    )
                    .min(config.max_delay);
                }
            }
        }
    }

    /// Cancel a Stripe subscription with retry logic for transient failures.
    ///
    /// Implements exponential backoff retry for rate limits and network errors.
    async fn cancel_subscription_with_retry(
        &self,
        sub_id: &str,
        at_period_end: bool,
    ) -> PaymentResult<bool> {
        let config = RetryConfig::default();
        let mut attempt = 0;
        let mut delay = config.initial_delay;

        loop {
            let result = if at_period_end {
                UpdateSubscription::new(sub_id)
                    .cancel_at_period_end(true)
                    .send(&self.client)
                    .await
                    .map(|_| true)
            } else {
                CancelSubscription::new(sub_id)
                    .send(&self.client)
                    .await
                    .map(|_| false)
            };

            match result {
                Ok(is_at_period_end) => return Ok(is_at_period_end),
                Err(err) => {
                    let payment_err = self.map_stripe_error(err);

                    if matches!(payment_err, PaymentError::SubscriptionAlreadyCanceled) {
                        return Err(payment_err);
                    }

                    if attempt >= config.max_retries || !is_retriable_error(&payment_err) {
                        return Err(payment_err);
                    }

                    attempt += 1;
                    warn!(
                        operation = "CancelSubscription",
                        attempt = attempt,
                        max_retries = config.max_retries,
                        delay_ms = delay.as_millis() as u64,
                        error = %payment_err,
                        "Retrying subscription cancellation after transient error"
                    );

                    tokio::time::sleep(delay).await;

                    delay = std::time::Duration::from_millis(
                        (delay.as_millis() as f64 * config.multiplier) as u64,
                    )
                    .min(config.max_delay);
                }
            }
        }
    }

    /// Charge a customer's saved default payment method off-session.
    /// Returns the Stripe PaymentIntent ID on success.
    pub async fn charge_saved_payment_method(
        &self,
        organization_id: Uuid,
        amount_usd: rust_decimal::Decimal,
        description: &str,
    ) -> PaymentResult<String> {
        use rust_decimal::prelude::ToPrimitive;

        let customer_row = sqlx::query(
            "SELECT stripe_customer_id FROM stripe_customers WHERE organization_id = $1",
        )
        .bind(organization_id)
        .fetch_optional(self.db.as_ref())
        .await?;

        let stripe_customer_id: String = match customer_row {
            Some(r) => r.get("stripe_customer_id"),
            None => {
                return Err(PaymentError::CustomerNotFound(
                    "No Stripe customer on file for this organization".into(),
                ))
            }
        };

        let pm_row = sqlx::query(
            r#"
            SELECT provider_payment_method_id
            FROM payment_methods
            WHERE provider_customer_id = $1
              AND is_default = true
              AND status = 'active'
            LIMIT 1
            "#,
        )
        .bind(&stripe_customer_id)
        .fetch_optional(self.db.as_ref())
        .await?;

        let stripe_pm_id: String = match pm_row {
            Some(r) => r.get("provider_payment_method_id"),
            None => return Err(PaymentError::PaymentMethodNotFound(Uuid::nil())),
        };

        let amount_cents = (amount_usd * rust_decimal::Decimal::from(100))
            .to_i64()
            .unwrap_or(0);
        if amount_cents <= 0 {
            return Err(PaymentError::ProviderError(
                "Charge amount must be positive".into(),
            ));
        }

        let pi = CreatePaymentIntent::new(amount_cents, Currency::USD)
            .customer(&stripe_customer_id)
            .payment_method(&stripe_pm_id)
            .confirm(true)
            .description(description)
            .send(&self.client)
            .await
            .map_err(|e| {
                warn!(organization_id = %organization_id, error = %e, "Stripe PaymentIntent creation failed");
                PaymentError::ProviderError(format!("Stripe error: {}", e))
            })?;

        info!(
            organization_id = %organization_id,
            payment_intent_id = %pi.id,
            amount_cents = amount_cents,
            "Off-session PaymentIntent created"
        );

        Ok(pi.id.to_string())
    }
}

#[async_trait]
impl PaymentProvider for StripePaymentProvider {
    fn provider_type(&self) -> PaymentProviderType {
        PaymentProviderType::Stripe
    }

    // =========================================================================
    // Customer Operations
    // =========================================================================

    async fn get_or_create_customer(&self, organization_id: Uuid) -> PaymentResult<CustomerInfo> {
        // Use a transaction with row lock to prevent race conditions
        // that could result in duplicate Stripe customers
        let mut tx = self.db.begin().await?;

        // Lock the organization row to prevent concurrent customer creation
        // This serializes customer creation attempts for the same organization
        sqlx::query("SELECT id FROM organizations WHERE id = $1 FOR UPDATE")
            .bind(organization_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| {
                PaymentError::CustomerNotFound(format!(
                    "Organization {} not found",
                    organization_id
                ))
            })?;

        // Check for existing customer within the lock
        let existing = sqlx::query(
            r#"
            SELECT stripe_customer_id, email, name
            FROM stripe_customers
            WHERE organization_id = $1
            "#,
        )
        .bind(organization_id)
        .fetch_optional(&mut *tx)
        .await?;

        if let Some(row) = existing {
            // Customer already exists, commit and return
            tx.commit().await?;
            return Ok(CustomerInfo {
                provider_customer_id: row.get("stripe_customer_id"),
                organization_id,
                email: row.get("email"),
                name: row.get("name"),
            });
        }

        // Get org details for customer creation
        let (org_name, email) = self.get_org_details(organization_id).await?;

        // Create Stripe customer with retry logic for transient failures
        // Note: Database transaction with row lock (above) prevents duplicate
        // customer creation. The ON CONFLICT clause handles the case where
        // Stripe call succeeds but DB insert fails - on retry, we'll find
        // the existing customer in the DB check above.
        let stripe_customer = self
            .create_customer_with_retry(&org_name, email.as_deref(), organization_id)
            .await?;

        // Store in database within the transaction
        sqlx::query(
            r#"
            INSERT INTO stripe_customers (organization_id, stripe_customer_id, email, name)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (organization_id) DO UPDATE
            SET stripe_customer_id = EXCLUDED.stripe_customer_id,
                email = EXCLUDED.email,
                name = EXCLUDED.name,
                updated_at = NOW()
            "#,
        )
        .bind(organization_id)
        .bind(stripe_customer.id.as_str())
        .bind(email.as_deref())
        .bind(&org_name)
        .execute(&mut *tx)
        .await?;

        // Commit the transaction
        tx.commit().await?;

        info!(
            organization_id = %organization_id,
            "Created Stripe customer"
        );

        Ok(CustomerInfo {
            provider_customer_id: stripe_customer.id.to_string(),
            organization_id,
            email,
            name: Some(org_name),
        })
    }

    async fn get_customer(&self, organization_id: Uuid) -> PaymentResult<Option<CustomerInfo>> {
        let row = sqlx::query(
            r#"
            SELECT stripe_customer_id, email, name
            FROM stripe_customers
            WHERE organization_id = $1
            "#,
        )
        .bind(organization_id)
        .fetch_optional(self.db.as_ref())
        .await?;

        Ok(row.map(|r| CustomerInfo {
            provider_customer_id: r.get("stripe_customer_id"),
            organization_id,
            email: r.get("email"),
            name: r.get("name"),
        }))
    }

    // =========================================================================
    // Payment Method Operations
    // =========================================================================

    async fn create_setup_intent(&self, organization_id: Uuid) -> PaymentResult<SetupIntentInfo> {
        let customer = self.get_or_create_customer(organization_id).await?;

        let setup_intent = CreateSetupIntent::new()
            .customer(&customer.provider_customer_id)
            .payment_method_types(vec![String::from("card")])
            .metadata(
                [(String::from("organization_id"), organization_id.to_string())]
                    .into_iter()
                    .collect::<std::collections::HashMap<String, String>>(),
            )
            .send(&self.client)
            .await
            .map_err(|e| self.map_stripe_error(e))?;

        trace!(
            organization_id = %organization_id,
            "Created setup intent"
        );

        Ok(SetupIntentInfo {
            client_secret: setup_intent.client_secret.unwrap_or_default(),
            setup_intent_id: setup_intent.id.to_string(),
        })
    }

    async fn confirm_payment_method(
        &self,
        organization_id: Uuid,
        setup_intent_id: &str,
        set_as_default: bool,
        created_by: Option<Uuid>,
    ) -> PaymentResult<PaymentMethodInfo> {
        // Validate input
        validate_setup_intent_id(setup_intent_id)?;

        let setup_intent = RetrieveSetupIntent::new(setup_intent_id)
            .send(&self.client)
            .await
            .map_err(|e| self.map_stripe_error(e))?;

        // Get payment method ID from the setup intent
        let pm_id = setup_intent
            .payment_method
            .ok_or_else(|| PaymentError::InvalidSetupIntent("No payment method attached".into()))?;

        let pm_id_str = pm_id.id().to_string();

        let stripe_pm = RetrievePaymentMethod::new(pm_id_str.as_str())
            .send(&self.client)
            .await
            .map_err(|e| self.map_stripe_error(e))?;

        // Extract card details (safe to store - not full card number)
        let (card_brand, card_last_four, card_exp_month, card_exp_year) =
            if let Some(card) = &stripe_pm.card {
                (
                    Some(card.brand.clone()),
                    Some(card.last4.clone()),
                    Some(card.exp_month as i32),
                    Some(card.exp_year as i32),
                )
            } else {
                (None, None, None, None)
            };

        let display_name = card_brand
            .as_ref()
            .zip(card_last_four.as_ref())
            .map(|(brand, last4)| format!("{} ending in {}", brand, last4));

        // Get customer info
        let customer = self.get_or_create_customer(organization_id).await?;

        // Store in database
        let db_id = self
            .store_payment_method(
                organization_id,
                &customer.provider_customer_id,
                &pm_id_str,
                display_name.as_deref(),
                card_brand.as_deref(),
                card_last_four.as_deref(),
                card_exp_month,
                card_exp_year,
                set_as_default,
                created_by,
            )
            .await?;

        info!(
            organization_id = %organization_id,
            payment_method_id = %db_id,
            "Payment method added"
        );

        Ok(PaymentMethodInfo {
            id: db_id,
            organization_id,
            provider_payment_method_id: pm_id_str,
            display_name,
            card_brand,
            card_last_four,
            card_exp_month,
            card_exp_year,
            is_default: set_as_default,
            status: PaymentMethodStatus::Active,
        })
    }

    /// List all **active** payment methods for an organization.
    ///
    /// # Filtering
    /// Only returns payment methods with `status = 'active'`. This excludes:
    /// - `canceled` - Payment methods that have been deleted
    /// - `expired` - Payment methods with expired cards
    /// - `failed` - Payment methods that failed verification
    /// - `pending` - Payment methods awaiting confirmation
    ///
    /// # Ordering
    /// Results are ordered with the default payment method first, then by creation date (newest first).
    async fn list_payment_methods(
        &self,
        organization_id: Uuid,
    ) -> PaymentResult<Vec<PaymentMethodInfo>> {
        let rows = sqlx::query(
            r#"
            SELECT id, organization_id, provider_payment_method_id,
                display_name, card_brand, card_last_four, card_exp_month, card_exp_year,
                is_default, status
            FROM payment_methods
            WHERE organization_id = $1 AND status = 'active'
            ORDER BY is_default DESC, created_at DESC
            "#,
        )
        .bind(organization_id)
        .fetch_all(self.db.as_ref())
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| PaymentMethodInfo {
                id: row.get("id"),
                organization_id: row.get("organization_id"),
                provider_payment_method_id: row.get("provider_payment_method_id"),
                display_name: row.get("display_name"),
                card_brand: row.get("card_brand"),
                card_last_four: row.get("card_last_four"),
                card_exp_month: row.get("card_exp_month"),
                card_exp_year: row.get("card_exp_year"),
                is_default: row.get("is_default"),
                status: row.get("status"),
            })
            .collect())
    }

    async fn get_payment_method(
        &self,
        payment_method_id: Uuid,
    ) -> PaymentResult<Option<PaymentMethodInfo>> {
        let row = sqlx::query(
            r#"
            SELECT id, organization_id, provider_payment_method_id,
                display_name, card_brand, card_last_four, card_exp_month, card_exp_year,
                is_default, status
            FROM payment_methods
            WHERE id = $1
            "#,
        )
        .bind(payment_method_id)
        .fetch_optional(self.db.as_ref())
        .await?;

        Ok(row.map(|r| PaymentMethodInfo {
            id: r.get("id"),
            organization_id: r.get("organization_id"),
            provider_payment_method_id: r.get("provider_payment_method_id"),
            display_name: r.get("display_name"),
            card_brand: r.get("card_brand"),
            card_last_four: r.get("card_last_four"),
            card_exp_month: r.get("card_exp_month"),
            card_exp_year: r.get("card_exp_year"),
            is_default: r.get("is_default"),
            status: r.get("status"),
        }))
    }

    async fn set_default_payment_method(
        &self,
        organization_id: Uuid,
        payment_method_id: Uuid,
    ) -> PaymentResult<()> {
        // Verify ownership
        let pm = self
            .get_payment_method(payment_method_id)
            .await?
            .ok_or(PaymentError::PaymentMethodNotFound(payment_method_id))?;

        if pm.organization_id != organization_id {
            return Err(PaymentError::PaymentMethodNotFound(payment_method_id));
        }

        // Use a transaction to ensure atomicity of the default payment method updates
        let mut tx = self.db.begin().await?;

        // Lock the organization row to prevent race conditions with concurrent
        // requests trying to set different payment methods as default
        sqlx::query("SELECT id FROM organizations WHERE id = $1 FOR UPDATE")
            .bind(organization_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| PaymentError::CustomerNotFound("Organization not found".into()))?;

        // Update defaults - unset all, then set the new one
        sqlx::query("UPDATE payment_methods SET is_default = false WHERE organization_id = $1")
            .bind(organization_id)
            .execute(&mut *tx)
            .await?;

        sqlx::query("UPDATE payment_methods SET is_default = true WHERE id = $1")
            .bind(payment_method_id)
            .execute(&mut *tx)
            .await?;

        sqlx::query("UPDATE organizations SET default_payment_method_id = $1 WHERE id = $2")
            .bind(payment_method_id)
            .bind(organization_id)
            .execute(&mut *tx)
            .await?;

        // Commit the transaction
        tx.commit().await?;

        trace!(
            organization_id = %organization_id,
            payment_method_id = %payment_method_id,
            "Set default payment method"
        );

        Ok(())
    }

    async fn delete_payment_method(
        &self,
        organization_id: Uuid,
        payment_method_id: Uuid,
    ) -> PaymentResult<()> {
        // Verify ownership
        let pm = self
            .get_payment_method(payment_method_id)
            .await?
            .ok_or(PaymentError::PaymentMethodNotFound(payment_method_id))?;

        if pm.organization_id != organization_id {
            return Err(PaymentError::PaymentMethodNotFound(payment_method_id));
        }

        // Use a transaction to ensure atomicity of database operations
        let mut tx = self.db.begin().await?;

        // Check if this payment method is used by an active subscription
        // Cannot delete a payment method that is the default for an active subscription
        let active_status_check = format!(
            r#"
            SELECT 1 FROM stripe_subscriptions ss
            JOIN payment_methods pm ON pm.provider_customer_id = ss.stripe_customer_id
            WHERE pm.id = $1
              AND ss.status IN ({})
              AND pm.is_default = true
            LIMIT 1
            "#,
            subscription_status::payment_method_bound_states_sql()
        );
        let has_active_subscription = sqlx::query(&active_status_check)
            .bind(payment_method_id)
            .fetch_optional(&mut *tx)
            .await?;

        if has_active_subscription.is_some() {
            // Log detailed reason for debugging, but return generic message to user
            debug!(
                organization_id = %organization_id,
                payment_method_id = %payment_method_id,
                "Cannot delete default payment method - active subscription exists"
            );
            return Err(PaymentError::ProviderError(
                "Cannot delete this payment method. Please update your billing settings first."
                    .into(),
            ));
        }

        // IMPORTANT: Update DB FIRST, then attempt Stripe detach
        // This ensures our database (source of truth) is in a consistent state
        // even if the Stripe API call fails or times out.

        // Soft delete from database (update status instead of deleting)
        sqlx::query(
            "UPDATE payment_methods SET status = 'canceled', updated_at = NOW() WHERE id = $1",
        )
        .bind(payment_method_id)
        .execute(&mut *tx)
        .await?;

        // Clear from organization if it was the default
        sqlx::query(
            "UPDATE organizations SET default_payment_method_id = NULL WHERE default_payment_method_id = $1",
        )
        .bind(payment_method_id)
        .execute(&mut *tx)
        .await?;

        // Commit the transaction - our DB is now updated
        tx.commit().await?;

        // Now attempt to detach from Stripe (best effort, after DB is committed)
        // If this fails, the payment method will remain on Stripe but is marked
        // as canceled in our system. This is acceptable because:
        // 1. The user won't see it in our UI anymore
        // 2. It can be cleaned up by a reconciliation job
        // 3. It will eventually expire on Stripe's side
        if let Err(e) = DetachPaymentMethod::new(pm.provider_payment_method_id.as_str())
            .send(&self.client)
            .await
        {
            let error_type = match &e {
                stripe::StripeError::Stripe(api_errors, _) => {
                    format!("Stripe({:?})", api_errors.code)
                }
                stripe::StripeError::ClientError(_) => "ClientError".to_string(),
                stripe::StripeError::Timeout => "Timeout".to_string(),
                _ => "Other".to_string(),
            };
            warn!(
                payment_method_id = %payment_method_id,
                stripe_pm_id = %pm.provider_payment_method_id,
                error_type = %error_type,
                "Failed to detach payment method from Stripe - will be cleaned up by reconciliation"
            );
        }

        info!(
            organization_id = %organization_id,
            payment_method_id = %payment_method_id,
            "Deleted payment method"
        );

        Ok(())
    }

    // =========================================================================
    // Subscription Operations
    // =========================================================================

    async fn create_subscription(
        &self,
        organization_id: Uuid,
        price_id: &str,
        payment_method_id: Option<Uuid>,
    ) -> PaymentResult<SubscriptionInfo> {
        // Validate input
        validate_price_id(price_id)?;

        // Acquire distributed lock to prevent race conditions where two concurrent
        // requests could both create Stripe subscriptions before either commits to DB.
        //
        // This replaces the FOR UPDATE database lock which caused potential deadlocks
        // when get_or_create_customer started a nested transaction.
        //
        // Lock TTL is 30 seconds - long enough for Stripe API call + DB operations.
        let _lock = acquire_billing_lock(
            &self.redis,
            &organization_id,
            "create_subscription",
            SUBSCRIPTION_LOCK_TTL_SECONDS,
        )
        .await
        .map_err(|e| match e {
            LockError::AlreadyLocked => PaymentError::RateLimited,
            LockError::RedisError(msg) => {
                // Log the Redis error but proceed cautiously
                // In production, you might want to fail closed instead
                error!(
                    organization_id = %organization_id,
                    error = %msg,
                    "Failed to acquire subscription lock - proceeding with database lock only"
                );
                PaymentError::ProviderError(
                    "Unable to process subscription request. Please try again.".into(),
                )
            }
        })?;

        // Get or create customer BEFORE starting the subscription transaction
        // to avoid nested transaction issues (get_or_create_customer has its own tx)
        let customer = self.get_or_create_customer(organization_id).await?;

        let stripe_pm_id = if let Some(pm_id) = payment_method_id {
            let pm = self
                .get_payment_method(pm_id)
                .await?
                .ok_or(PaymentError::PaymentMethodNotFound(pm_id))?;
            Some(pm.provider_payment_method_id)
        } else {
            let methods = self.list_payment_methods(organization_id).await?;
            methods
                .into_iter()
                .find(|m| m.is_default)
                .map(|m| m.provider_payment_method_id)
        };

        let mut tx = self.db.begin().await?;

        let existing_sub_query = format!(
            r#"
            SELECT stripe_subscription_id
            FROM stripe_subscriptions
            WHERE organization_id = $1 
              AND status IN ({})
            LIMIT 1
            "#,
            subscription_status::active_states_sql()
        );
        let existing = sqlx::query(&existing_sub_query)
            .bind(organization_id)
            .fetch_optional(&mut *tx)
            .await?;

        if existing.is_some() {
            debug!(
                organization_id = %organization_id,
                "Cannot create subscription - organization already has active subscription"
            );
            return Err(PaymentError::ProviderError(
                "A subscription already exists. Please manage your existing subscription.".into(),
            ));
        }

        let config = RetryConfig::default();
        let mut attempt = 0;
        let mut delay = config.initial_delay;

        let subscription = loop {
            let mut items = vec![CreateSubscriptionItems {
                price: Some(price_id.to_string()),
                quantity: Some(1),
                ..Default::default()
            }];

            if let Some(ref metered_price) = self.metered_price_id {
                items.push(CreateSubscriptionItems {
                    price: Some(metered_price.clone()),
                    ..Default::default()
                });
            }

            let mut req = CreateSubscription::new()
                .customer(&customer.provider_customer_id)
                .items(items)
                .payment_behavior(
                    stripe_billing::subscription::CreateSubscriptionPaymentBehavior::DefaultIncomplete,
                )
                .expand(vec![String::from("latest_invoice.payment_intent")])
                .metadata(
                    [(String::from("organization_id"), organization_id.to_string())]
                        .into_iter()
                        .collect::<std::collections::HashMap<String, String>>(),
                );

            if let Some(ref pm_str) = stripe_pm_id {
                req = req.default_payment_method(pm_str);
            }

            match req.send(&self.client).await {
                Ok(sub) => break sub,
                Err(err) => {
                    let payment_err = self.map_stripe_error(err);

                    if attempt >= config.max_retries || !is_retriable_error(&payment_err) {
                        return Err(payment_err);
                    }

                    attempt += 1;
                    warn!(
                        operation = "CreateSubscription",
                        organization_id = %organization_id,
                        attempt = attempt,
                        max_retries = config.max_retries,
                        delay_ms = delay.as_millis() as u64,
                        error = %payment_err,
                        "Retrying subscription creation after transient error"
                    );

                    tokio::time::sleep(delay).await;

                    delay = std::time::Duration::from_millis(
                        (delay.as_millis() as f64 * config.multiplier) as u64,
                    )
                    .min(config.max_delay);
                }
            }
        };

        // Store in database (within the same transaction)
        sqlx::query(
            r#"
            INSERT INTO stripe_subscriptions (
                organization_id, stripe_subscription_id, stripe_customer_id,
                status, current_period_start, current_period_end, price_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (stripe_subscription_id) DO UPDATE
            SET status = EXCLUDED.status,
                current_period_start = EXCLUDED.current_period_start,
                current_period_end = EXCLUDED.current_period_end
            "#,
        )
        .bind(organization_id)
        .bind(subscription.id.as_str())
        .bind(&customer.provider_customer_id)
        .bind(subscription.status.as_str())
        .bind(chrono::DateTime::from_timestamp(subscription.start_date, 0))
        .bind(chrono::DateTime::from_timestamp(
            subscription.billing_cycle_anchor,
            0,
        ))
        .bind(price_id)
        .execute(&mut *tx)
        .await?;

        // Commit the transaction
        tx.commit().await?;

        // In the new Stripe API, payment_intent is no longer on Invoice directly.
        // The client_secret for initial payment confirmation can be retrieved
        // from the payment settings or by expanding the invoice's payments.
        // For DefaultIncomplete behavior, the frontend uses the subscription status.
        let client_secret: Option<String> = None;

        info!(
            organization_id = %organization_id,
            subscription_id = %subscription.id,
            status = %subscription.status.as_str(),
            "Created subscription"
        );

        Ok(SubscriptionInfo {
            subscription_id: subscription.id.to_string(),
            status: subscription.status.as_str().to_string(),
            current_period_start: chrono::DateTime::from_timestamp(subscription.start_date, 0),
            current_period_end: chrono::DateTime::from_timestamp(
                subscription.billing_cycle_anchor,
                0,
            ),
            client_secret,
            cancel_at_period_end: subscription.cancel_at_period_end,
        })
    }

    async fn update_subscription(
        &self,
        organization_id: Uuid,
        new_price_id: &str,
        payment_method_id: Option<Uuid>,
    ) -> PaymentResult<SubscriptionInfo> {
        validate_price_id(new_price_id)?;

        let existing_sub_query = format!(
            r#"
            SELECT stripe_subscription_id, price_id
            FROM stripe_subscriptions
            WHERE organization_id = $1
              AND status IN ({})
            ORDER BY created_at DESC
            LIMIT 1
            "#,
            subscription_status::active_states_sql()
        );
        let existing = sqlx::query(&existing_sub_query)
            .bind(organization_id)
            .fetch_optional(self.db.as_ref())
            .await?;

        let row = match existing {
            Some(r) => r,
            None => {
                return self
                    .create_subscription(organization_id, new_price_id, payment_method_id)
                    .await;
            }
        };

        let stripe_sub_id: String = row.get("stripe_subscription_id");
        let current_price: Option<String> = row.get("price_id");

        if current_price.as_deref() == Some(new_price_id) {
            let sub = self.get_subscription(organization_id).await?;
            return sub.ok_or(PaymentError::SubscriptionNotFound(organization_id));
        }

        let subscription = RetrieveSubscription::new(stripe_sub_id.as_str())
            .expand(vec![String::from("items")])
            .send(&self.client)
            .await
            .map_err(|e| self.map_stripe_error(e))?;

        let metered_price = self.metered_price_id.as_deref();
        let tier_item = subscription
            .items
            .data
            .iter()
            .find(|item| {
                let price_id = item.price.id.as_str();
                Some(price_id) != metered_price
            })
            .or_else(|| subscription.items.data.first());

        let has_metered_item = metered_price.is_some()
            && subscription.items.data.iter().any(|item| {
                Some(item.price.id.as_str()) == metered_price
            });

        let existing_item_id = tier_item.map(|item| item.id.to_string());

        let mut items = if let Some(ref item_id) = existing_item_id {
            vec![UpdateSubscriptionItems {
                id: Some(item_id.clone()),
                price: Some(new_price_id.to_string()),
                quantity: Some(1),
                ..Default::default()
            }]
        } else {
            vec![UpdateSubscriptionItems {
                price: Some(new_price_id.to_string()),
                quantity: Some(1),
                ..Default::default()
            }]
        };

        if let Some(mp) = metered_price {
            if !has_metered_item {
                items.push(UpdateSubscriptionItems {
                    price: Some(mp.to_string()),
                    ..Default::default()
                });
            }
        }

        let updated = UpdateSubscription::new(stripe_sub_id.as_str())
            .items(items)
            .proration_behavior(
                stripe_billing::subscription::UpdateSubscriptionProrationBehavior::CreateProrations,
            )
            .send(&self.client)
            .await
            .map_err(|e| self.map_stripe_error(e))?;

        sqlx::query(
            r#"
            UPDATE stripe_subscriptions
            SET price_id = $2, status = $3, updated_at = NOW()
            WHERE stripe_subscription_id = $1
            "#,
        )
        .bind(&stripe_sub_id)
        .bind(new_price_id)
        .bind(updated.status.as_str())
        .execute(self.db.as_ref())
        .await?;

        info!(
            organization_id = %organization_id,
            subscription_id = %stripe_sub_id,
            new_price_id = %new_price_id,
            "Subscription updated with proration"
        );

        Ok(SubscriptionInfo {
            subscription_id: updated.id.to_string(),
            status: updated.status.as_str().to_string(),
            current_period_start: chrono::DateTime::from_timestamp(updated.start_date, 0),
            current_period_end: chrono::DateTime::from_timestamp(
                updated.billing_cycle_anchor,
                0,
            ),
            client_secret: None,
            cancel_at_period_end: updated.cancel_at_period_end,
        })
    }

    async fn cancel_subscription(
        &self,
        organization_id: Uuid,
        at_period_end: bool,
    ) -> PaymentResult<()> {
        // Allow cancellation of subscriptions in various active-ish states
        // - active: Normal active subscription
        // - past_due: Payment failed but subscription still exists
        // - trialing: In trial period
        // - incomplete: Awaiting payment confirmation
        let cancel_status_query = format!(
            r#"
            SELECT stripe_subscription_id, status
            FROM stripe_subscriptions
            WHERE organization_id = $1 
              AND status IN ({})
            ORDER BY created_at DESC
            LIMIT 1
            "#,
            subscription_status::cancelable_states_sql()
        );
        let row = sqlx::query(&cancel_status_query)
            .bind(organization_id)
            .fetch_optional(self.db.as_ref())
            .await?;

        let subscription_row = row.ok_or(PaymentError::SubscriptionNotFound(organization_id))?;

        let stripe_sub_id: String = subscription_row.get("stripe_subscription_id");
        let current_status: String = subscription_row.get("status");

        // For incomplete subscriptions, always cancel immediately since they haven't started billing
        let effective_at_period_end = if current_status == subscription_status::INCOMPLETE {
            if at_period_end {
                trace!(
                    organization_id = %organization_id,
                    "Incomplete subscription requested cancel at period end, forcing immediate cancel"
                );
            }
            false
        } else {
            at_period_end
        };

        // Step 1: Set local state to pending_cancellation BEFORE calling Stripe
        // This provides resilience: if Stripe succeeds but final DB update fails,
        // the pending state indicates cancellation was initiated
        sqlx::query(
            r#"
            UPDATE stripe_subscriptions 
            SET status = $2, updated_at = NOW() 
            WHERE stripe_subscription_id = $1
            "#,
        )
        .bind(&stripe_sub_id)
        .bind(subscription_status::PENDING_CANCELLATION)
        .execute(self.db.as_ref())
        .await?;

        // Step 2: Call Stripe API with retry logic for transient failures
        let stripe_result = self
            .cancel_subscription_with_retry(&stripe_sub_id, effective_at_period_end)
            .await;

        // Step 3: Update final state based on Stripe result
        match stripe_result {
            Ok(is_at_period_end) => {
                // Stripe succeeded - update to final state
                let final_query = if is_at_period_end {
                    r#"
                    UPDATE stripe_subscriptions 
                    SET status = $2, cancel_at_period_end = true, updated_at = NOW() 
                    WHERE stripe_subscription_id = $1
                    "#
                } else {
                    r#"
                    UPDATE stripe_subscriptions 
                    SET status = 'canceled', canceled_at = NOW(), updated_at = NOW() 
                    WHERE stripe_subscription_id = $1
                    "#
                };

                let final_status = if is_at_period_end {
                    &current_status
                } else {
                    subscription_status::CANCELED
                };

                let db_result = sqlx::query(final_query)
                    .bind(&stripe_sub_id)
                    .bind(final_status)
                    .execute(self.db.as_ref())
                    .await;

                if let Err(ref e) = db_result {
                    // Log the discrepancy - webhook will reconcile this eventually
                    // The pending_cancellation state indicates cancellation was initiated
                    warn!(
                        organization_id = %organization_id,
                        subscription_id = %stripe_sub_id,
                        error = %e,
                        "DATABASE SYNC WARNING: Stripe subscription canceled but final DB update failed. \
                         Subscription is in pending_cancellation state. Webhook reconciliation will fix this."
                    );
                }
                db_result?;

                if is_at_period_end {
                    info!(
                        organization_id = %organization_id,
                        subscription_id = %stripe_sub_id,
                        "Subscription set to cancel at period end"
                    );
                } else {
                    info!(
                        organization_id = %organization_id,
                        subscription_id = %stripe_sub_id,
                        "Subscription canceled immediately"
                    );
                }

                Ok(())
            }
            Err(stripe_err) => {
                // Check if the error indicates the subscription is already canceled
                // This can happen with concurrent cancel requests - the first succeeds,
                // the second sees "already canceled" from Stripe
                // Use typed error matching instead of fragile string matching
                if matches!(stripe_err, PaymentError::SubscriptionAlreadyCanceled) {
                    // Subscription is already canceled on Stripe's side - update our DB to match
                    info!(
                        organization_id = %organization_id,
                        subscription_id = %stripe_sub_id,
                        "Subscription already canceled on Stripe - updating local state to match"
                    );

                    let update_result = sqlx::query(
                        "UPDATE stripe_subscriptions SET status = 'canceled', canceled_at = NOW(), updated_at = NOW() WHERE stripe_subscription_id = $1",
                    )
                    .bind(&stripe_sub_id)
                    .execute(self.db.as_ref())
                    .await;

                    if let Err(ref e) = update_result {
                        warn!(
                            organization_id = %organization_id,
                            subscription_id = %stripe_sub_id,
                            error = %e,
                            "Failed to update subscription status after discovering it was already canceled"
                        );
                    }

                    // Return success since the subscription is indeed canceled
                    return Ok(());
                }

                // Stripe failed for other reasons - revert to original status
                warn!(
                    organization_id = %organization_id,
                    subscription_id = %stripe_sub_id,
                    error = %stripe_err,
                    "Stripe cancellation failed, reverting to original status"
                );

                let revert_result = sqlx::query(
                    "UPDATE stripe_subscriptions SET status = $2, updated_at = NOW() WHERE stripe_subscription_id = $1",
                )
                .bind(&stripe_sub_id)
                .bind(&current_status)
                .execute(self.db.as_ref())
                .await;

                if let Err(ref e) = revert_result {
                    // Critical: both Stripe and revert failed
                    error!(
                        organization_id = %organization_id,
                        subscription_id = %stripe_sub_id,
                        stripe_error = %stripe_err,
                        revert_error = %e,
                        "CRITICAL: Stripe cancellation failed AND status revert failed. \
                         Subscription stuck in pending_cancellation state. Manual intervention required."
                    );
                }

                Err(stripe_err)
            }
        }
    }

    /// Get the most recent subscription for an organization.
    ///
    /// # Returns
    /// Returns the most recent subscription **regardless of status**, including canceled ones.
    /// This is intentional because:
    /// 1. The frontend can display the current subscription status (even if canceled)
    /// 2. After cancellation, users can verify the cancellation is in effect
    /// 3. The `status` field in the response indicates whether it's active or not
    ///
    /// # Checking for Active Subscription
    /// Callers who need to know if the org has an **active** subscription should check
    /// the returned `status` field against active states (active, trialing, past_due, incomplete).
    /// Use `subscription_status::is_active()` for this check.
    ///
    /// # Note
    /// If an organization creates a new subscription after canceling one, this will
    /// return the newer subscription since it orders by `created_at DESC`.
    async fn get_subscription(
        &self,
        organization_id: Uuid,
    ) -> PaymentResult<Option<SubscriptionInfo>> {
        let row = sqlx::query(
            r#"
            SELECT stripe_subscription_id, status, current_period_start, current_period_end,
                   cancel_at_period_end
            FROM stripe_subscriptions
            WHERE organization_id = $1
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(organization_id)
        .fetch_optional(self.db.as_ref())
        .await?;

        Ok(row.map(|r| SubscriptionInfo {
            subscription_id: r.get("stripe_subscription_id"),
            status: r.get("status"),
            current_period_start: r.get("current_period_start"),
            current_period_end: r.get("current_period_end"),
            client_secret: None,
            cancel_at_period_end: r.get("cancel_at_period_end"),
        }))
    }

    // =========================================================================
    // Invoice Operations
    // =========================================================================

    async fn list_invoices(
        &self,
        organization_id: Uuid,
        limit: i64,
        offset: i64,
    ) -> PaymentResult<(Vec<InvoiceInfo>, i64)> {
        let total: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM invoices WHERE organization_id = $1")
                .bind(organization_id)
                .fetch_one(self.db.as_ref())
                .await?;

        let rows = sqlx::query(
            r#"
            SELECT id, invoice_number, status, total_cents, currency,
                   period_start, period_end, paid_at, invoice_pdf_url, hosted_invoice_url
            FROM invoices
            WHERE organization_id = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(organization_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(self.db.as_ref())
        .await?;

        let invoices = rows
            .into_iter()
            .map(|r| InvoiceInfo {
                id: r.get("id"),
                invoice_number: r.get("invoice_number"),
                status: r.get("status"),
                total_cents: r.get("total_cents"),
                currency: r.get("currency"),
                period_start: r.get("period_start"),
                period_end: r.get("period_end"),
                paid_at: r.get("paid_at"),
                invoice_pdf_url: r.get("invoice_pdf_url"),
                hosted_invoice_url: r.get("hosted_invoice_url"),
            })
            .collect();

        Ok((invoices, total))
    }

    // =========================================================================
    // Billing Portal Operations
    // =========================================================================

    async fn create_billing_portal_session(
        &self,
        organization_id: Uuid,
        return_url: &str,
    ) -> PaymentResult<String> {
        let customer = self.get_or_create_customer(organization_id).await?;

        let session = CreateBillingPortalSession::new()
            .customer(&customer.provider_customer_id)
            .return_url(return_url)
            .send(&self.client)
            .await
            .map_err(|e| {
                error!(organization_id = %organization_id, error = %e, "Failed to create billing portal session");
                PaymentError::ProviderError(format!("Failed to create portal session: {}", e))
            })?;

        Ok(session.url)
    }

    async fn sync_invoices(&self, organization_id: Uuid) -> PaymentResult<u64> {
        let customer = self.get_customer(organization_id).await?.ok_or_else(|| {
            PaymentError::CustomerNotFound(format!(
                "No Stripe customer for organization {}",
                organization_id
            ))
        })?;

        let invoices = ListInvoice::new()
            .customer(&customer.provider_customer_id)
            .limit(100)
            .send(&self.client)
            .await
            .map_err(|e| {
                error!(organization_id = %organization_id, error = %e, "Failed to list Stripe invoices");
                PaymentError::ProviderError(format!("Failed to list invoices: {}", e))
            })?;

        let mut synced: u64 = 0;
        for inv in &invoices.data {
            let stripe_invoice_id = inv
                .id
                .as_ref()
                .map(|id| id.to_string())
                .unwrap_or_default();
            let invoice_number = inv.number.as_deref().unwrap_or(&stripe_invoice_id);
            let status = inv
                .status
                .as_ref()
                .map(|s| s.as_str().to_string())
                .unwrap_or_else(|| "unknown".to_string());
            let total_cents = inv.total;
            let currency = format!("{:?}", inv.currency).to_lowercase();
            let period_start = chrono::DateTime::from_timestamp(inv.period_start, 0);
            let period_end = chrono::DateTime::from_timestamp(inv.period_end, 0);
            let paid_at = inv
                .status_transitions
                .paid_at
                .and_then(|t| chrono::DateTime::from_timestamp(t, 0));
            let invoice_pdf_url = inv.invoice_pdf.as_deref();
            let hosted_invoice_url = inv.hosted_invoice_url.as_deref();

            sqlx::query(
                r#"
                INSERT INTO invoices (
                    organization_id, stripe_invoice_id, invoice_number, status,
                    total_cents, currency, period_start, period_end, paid_at,
                    invoice_pdf_url, hosted_invoice_url
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (stripe_invoice_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    total_cents = EXCLUDED.total_cents,
                    paid_at = EXCLUDED.paid_at,
                    invoice_pdf_url = EXCLUDED.invoice_pdf_url,
                    hosted_invoice_url = EXCLUDED.hosted_invoice_url
                "#,
            )
            .bind(organization_id)
            .bind(&stripe_invoice_id)
            .bind(invoice_number)
            .bind(&status)
            .bind(total_cents)
            .bind(&currency)
            .bind(period_start)
            .bind(period_end)
            .bind(paid_at)
            .bind(invoice_pdf_url)
            .bind(hosted_invoice_url)
            .execute(self.db.as_ref())
            .await?;

            synced += 1;
        }

        info!(organization_id = %organization_id, count = synced, "Synced invoices from Stripe");
        Ok(synced)
    }

    // =========================================================================
    // Webhook Operations
    // =========================================================================

    async fn verify_webhook(
        &self,
        payload: &str,
        signature: &str,
    ) -> PaymentResult<Option<WebhookEvent>> {
        let webhook_secret = self
            .webhook_secret
            .as_ref()
            .ok_or(PaymentError::NotConfigured)?;

        // Use stripe_webhook (1.0.0-rc.5) for signature verification.
        // This crate is updated weekly from Stripe's OpenAPI spec and supports
        // current API versions (including 2026-03-25.dahlia).
        let verified_event =
            stripe_webhook::Webhook::construct_event(payload, signature, webhook_secret).map_err(
                |e| {
                    warn!(error = ?e, "Stripe webhook signature verification failed");
                    PaymentError::InvalidWebhookSignature
                },
            )?;

        // Parse raw payload as serde_json::Value for our downstream processing,
        // since the rest of the codebase operates on Value rather than typed Event.
        let event_data: serde_json::Value = serde_json::from_str(payload).map_err(|e| {
            warn!(error = %e, "Failed to parse Stripe webhook payload as JSON");
            PaymentError::InvalidWebhookSignature
        })?;

        let event_id_string = verified_event.id.to_string();
        let event_id = event_id_string.as_str();
        let event_type_string = verified_event.type_.to_string();
        let event_type_str = event_type_string.as_str();

        // Store for idempotency
        let result = sqlx::query(
            r#"
            INSERT INTO stripe_events (stripe_event_id, event_type, data)
            VALUES ($1, $2, $3)
            ON CONFLICT (stripe_event_id) DO NOTHING
            RETURNING id
            "#,
        )
        .bind(event_id)
        .bind(event_type_str)
        .bind(&event_data)
        .fetch_optional(self.db.as_ref())
        .await?;

        if result.is_none() {
            let existing: Option<bool> = sqlx::query_scalar(
                "SELECT processed FROM stripe_events WHERE stripe_event_id = $1",
            )
            .bind(event_id)
            .fetch_optional(self.db.as_ref())
            .await?;

            match existing {
                Some(true) => {
                    trace!(event_id = %event_id, "Webhook event already processed - skipping");
                    return Ok(None);
                }
                Some(false) => {
                    debug!(event_id = %event_id, "Webhook event exists but not processed - reprocessing");
                }
                None => {
                    warn!(event_id = %event_id, "Webhook event disappeared during idempotency check");
                    return Ok(None);
                }
            }
        }

        let organization_id = event_data
            .get("data")
            .and_then(|d| d.get("object"))
            .and_then(|o| o.get("metadata"))
            .and_then(|m| m.get("organization_id"))
            .and_then(|id| id.as_str())
            .and_then(|s| Uuid::parse_str(s).ok());

        let event_type = match event_type_str {
            "customer.subscription.created" => WebhookEventType::SubscriptionCreated,
            "customer.subscription.updated" => WebhookEventType::SubscriptionUpdated,
            "customer.subscription.deleted" => WebhookEventType::SubscriptionDeleted,
            "invoice.paid" => WebhookEventType::InvoicePaid,
            "invoice.payment_failed" => WebhookEventType::InvoicePaymentFailed,
            "payment_method.attached" => WebhookEventType::PaymentMethodAttached,
            "payment_method.detached" => WebhookEventType::PaymentMethodDetached,
            "customer.updated" => WebhookEventType::CustomerUpdated,
            "customer.deleted" => WebhookEventType::CustomerDeleted,
            "checkout.session.completed" => WebhookEventType::CheckoutSessionCompleted,
            other => WebhookEventType::Unknown(other.to_string()),
        };

        Ok(Some(WebhookEvent {
            event_id: event_id.to_string(),
            event_type,
            organization_id,
            data: event_data,
        }))
    }

    async fn mark_event_processed(
        &self,
        event_id: &str,
        error_message: Option<&str>,
    ) -> PaymentResult<()> {
        sqlx::query(
            r#"
            UPDATE stripe_events
            SET processed = true, processed_at = NOW(), error_message = $2
            WHERE stripe_event_id = $1
            "#,
        )
        .bind(event_id)
        .bind(error_message)
        .execute(self.db.as_ref())
        .await?;

        Ok(())
    }
}
