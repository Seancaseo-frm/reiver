//! Payment method types and Stripe integration types.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Payment provider type enum (for database storage).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "payment_provider", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum PaymentProviderKind {
    Stripe,
}

impl Default for PaymentProviderKind {
    fn default() -> Self {
        Self::Stripe
    }
}

/// Payment method status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "payment_method_status", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum PaymentMethodStatus {
    Active,
    Pending,
    Expired,
    Failed,
    Canceled,
}

impl Default for PaymentMethodStatus {
    fn default() -> Self {
        Self::Pending
    }
}

// ============================================================================
// Status Constants
// ============================================================================

/// Subscription status constants matching Stripe's status values.
/// These are used in SQL queries to ensure type safety and prevent typos.
pub mod subscription_status {
    /// Subscription is active and billing normally
    pub const ACTIVE: &str = "active";
    /// Payment failed but subscription still exists (Stripe will retry)
    pub const PAST_DUE: &str = "past_due";
    /// Subscription has been canceled
    pub const CANCELED: &str = "canceled";
    /// Subscription is in trial period
    pub const TRIALING: &str = "trialing";
    /// Initial payment is pending (requires payment confirmation)
    pub const INCOMPLETE: &str = "incomplete";
    /// Initial payment failed and subscription was not activated
    pub const INCOMPLETE_EXPIRED: &str = "incomplete_expired";
    /// Subscription is paused
    pub const PAUSED: &str = "paused";
    /// Subscription ended after all retries exhausted
    pub const UNPAID: &str = "unpaid";
    /// Cancellation is in progress (local state before Stripe confirmation)
    /// This provides resilience if Stripe call succeeds but DB update fails
    pub const PENDING_CANCELLATION: &str = "pending_cancellation";

    /// Returns true if the subscription is in an "active-ish" state
    /// (can be charged, should have access to features)
    ///
    /// # Note on `pending_cancellation`
    /// This status is included because it represents a transient state during
    /// cancellation. If a cancellation request succeeds at Stripe but the final
    /// DB update fails, the subscription will be in `pending_cancellation` state.
    ///
    /// Including it in `is_active` ensures:
    /// 1. Users maintain access during the brief cancellation window
    /// 2. If stuck due to transient failure, the billing worker's Stripe reconciliation
    ///    will eventually correct the state via webhook events
    /// 3. Users aren't incorrectly denied service for infrastructure issues
    ///
    /// The alternative (treating it as inactive) would cause user-facing errors
    /// for a state that is almost always resolved within seconds.
    pub fn is_active(status: &str) -> bool {
        matches!(
            status,
            ACTIVE | PAST_DUE | TRIALING | INCOMPLETE | PENDING_CANCELLATION
        )
    }

    /// Returns true if the subscription can be canceled
    pub fn is_cancelable(status: &str) -> bool {
        matches!(status, ACTIVE | PAST_DUE | TRIALING | INCOMPLETE)
    }

    /// Returns true if the subscription is pending cancellation
    pub fn is_pending_cancellation(status: &str) -> bool {
        status == PENDING_CANCELLATION
    }

    // =========================================================================
    // SQL Fragment Constants
    // =========================================================================
    // These constants provide SQL fragments for use in queries.
    // They centralize status lists to prevent typos and inconsistencies.
    // Using const instead of functions avoids allocation on every call.
    //
    // IMPORTANT: Why `pending_cancellation` is NOT included in these SQL constants
    // ============================================================================
    // The `pending_cancellation` status is included in `is_active()` (Rust function)
    // but intentionally OMITTED from the SQL constants. Here's why:
    //
    // 1. ACTIVE_STATES_SQL - Used to prevent duplicate subscription creation.
    //    If a subscription is in `pending_cancellation`, the user has requested
    //    cancellation and should be allowed to create a new subscription. Including
    //    it would block new subscriptions during the brief cancellation window.
    //
    // 2. CANCELABLE_STATES_SQL - Used to find subscriptions that can be canceled.
    //    A subscription in `pending_cancellation` is already being canceled, so
    //    it should not appear in this list (would cause "already canceled" errors).
    //
    // 3. PAYMENT_METHOD_BOUND_STATES_SQL - Used to check if a payment method can
    //    be deleted. If cancellation is pending, we should allow payment method
    //    deletion since the subscription is going away anyway.
    //
    // The `is_active()` function includes `pending_cancellation` because it's used
    // for feature access checks - users should maintain access during the brief
    // cancellation window while the Stripe API call completes.

    /// SQL fragment for subscription states that should be considered "active"
    /// for subscription creation checks (prevents duplicate active subscriptions).
    ///
    /// Note: `pending_cancellation` is intentionally excluded - see comment above.
    ///
    /// Usage: `format!("status IN ({})", ACTIVE_STATES_SQL)`
    pub const ACTIVE_STATES_SQL: &str = "'active', 'trialing', 'past_due', 'incomplete'";

    /// SQL fragment for subscription states that can be canceled.
    ///
    /// Note: `pending_cancellation` is intentionally excluded - see comment above.
    ///
    /// Usage: `format!("status IN ({})", CANCELABLE_STATES_SQL)`
    pub const CANCELABLE_STATES_SQL: &str = "'active', 'past_due', 'trialing', 'incomplete'";

    /// SQL fragment for subscription states that indicate the subscription
    /// is tied to a payment method (used when checking if payment method can be deleted).
    ///
    /// Note: `pending_cancellation` is intentionally excluded - see comment above.
    ///
    /// Usage: `format!("status IN ({})", PAYMENT_METHOD_BOUND_STATES_SQL)`
    pub const PAYMENT_METHOD_BOUND_STATES_SQL: &str = "'active', 'trialing', 'past_due'";

    // Deprecated function wrappers for backwards compatibility
    // TODO: Remove these after updating all call sites to use the constants directly

    /// Returns SQL fragment for active subscription states.
    #[inline]
    pub fn active_states_sql() -> &'static str {
        ACTIVE_STATES_SQL
    }

    /// Returns SQL fragment for cancelable subscription states.
    #[inline]
    pub fn cancelable_states_sql() -> &'static str {
        CANCELABLE_STATES_SQL
    }

    /// Returns SQL fragment for payment-method-bound subscription states.
    #[inline]
    pub fn payment_method_bound_states_sql() -> &'static str {
        PAYMENT_METHOD_BOUND_STATES_SQL
    }
}

/// Invoice status constants matching Stripe's status values.
pub mod invoice_status {
    /// Invoice is not yet finalized
    pub const DRAFT: &str = "draft";
    /// Invoice is finalized and awaiting payment
    pub const OPEN: &str = "open";
    /// Invoice has been paid
    pub const PAID: &str = "paid";
    /// Invoice has been voided (canceled without payment)
    pub const VOID: &str = "void";
    /// Payment cannot be collected (exhausted retries)
    pub const UNCOLLECTIBLE: &str = "uncollectible";

    /// Returns true if the invoice is considered "unpaid"
    /// (needs attention or payment)
    pub fn is_unpaid(status: &str) -> bool {
        matches!(status, OPEN | UNCOLLECTIBLE)
    }

    /// Returns true if the invoice is "settled" (no further action needed)
    pub fn is_settled(status: &str) -> bool {
        matches!(status, PAID | VOID)
    }
}

/// Payment method stored in the database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaymentMethod {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub provider: PaymentProviderKind,
    pub status: PaymentMethodStatus,

    // Provider-specific IDs
    pub provider_customer_id: Option<String>,
    pub provider_payment_method_id: Option<String>,
    pub provider_subscription_id: Option<String>,

    // Display info
    pub display_name: Option<String>,
    pub card_brand: Option<String>,
    pub card_last_four: Option<String>,
    pub card_exp_month: Option<i32>,
    pub card_exp_year: Option<i32>,

    // Billing details
    pub billing_email: Option<String>,
    pub billing_name: Option<String>,
    pub billing_address_line1: Option<String>,
    pub billing_address_line2: Option<String>,
    pub billing_city: Option<String>,
    pub billing_state: Option<String>,
    pub billing_postal_code: Option<String>,
    pub billing_country: Option<String>,

    pub is_default: bool,
    pub metadata: serde_json::Value,
    pub created_by: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Stripe customer record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StripeCustomer {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub stripe_customer_id: String,
    pub email: Option<String>,
    pub name: Option<String>,
    pub currency: String,
    pub metadata: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Stripe subscription record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StripeSubscription {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub stripe_subscription_id: String,
    pub stripe_customer_id: String,
    pub status: String,
    pub current_period_start: Option<DateTime<Utc>>,
    pub current_period_end: Option<DateTime<Utc>>,
    pub price_id: Option<String>,
    pub quantity: Option<i32>,
    pub cancel_at_period_end: bool,
    pub canceled_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    pub trial_start: Option<DateTime<Utc>>,
    pub trial_end: Option<DateTime<Utc>>,
    pub metadata: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Invoice record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Invoice {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub invoice_number: String,
    pub status: String,
    pub provider: Option<PaymentProviderKind>,
    pub provider_invoice_id: Option<String>,
    pub subtotal_cents: i64,
    pub tax_cents: i64,
    pub total_cents: i64,
    pub amount_paid_cents: i64,
    pub amount_due_cents: i64,
    pub currency: String,
    pub period_start: Option<DateTime<Utc>>,
    pub period_end: Option<DateTime<Utc>>,
    pub due_date: Option<DateTime<Utc>>,
    pub paid_at: Option<DateTime<Utc>>,
    pub invoice_pdf_url: Option<String>,
    pub hosted_invoice_url: Option<String>,
    pub line_items: serde_json::Value,
    pub metadata: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Stripe webhook event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StripeEvent {
    pub id: Uuid,
    pub stripe_event_id: String,
    pub event_type: String,
    pub data: serde_json::Value,
    pub processed: bool,
    pub processed_at: Option<DateTime<Utc>>,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
}

// Note: API request/response types are defined in src/api/payments.rs
// where they are actually used. This avoids duplicate type definitions.
