//! Payment provider trait for abstraction and testability.
//!
//! This module defines the `PaymentProvider` trait that abstracts payment operations,
//! allowing for different implementations (Stripe, mock for testing).

use async_trait::async_trait;
use uuid::Uuid;

use super::payments::*;

/// Errors that can occur during payment operations.
#[derive(Debug, thiserror::Error)]
pub enum PaymentError {
    #[error("Payment provider not configured")]
    NotConfigured,

    #[error("Customer not found: {0}")]
    CustomerNotFound(String),

    #[error("Payment method not found: {0}")]
    PaymentMethodNotFound(Uuid),

    #[error("Subscription not found for organization: {0}")]
    SubscriptionNotFound(Uuid),

    #[error("Subscription is already canceled or does not exist")]
    SubscriptionAlreadyCanceled,

    #[error("Invalid payment method: {0}")]
    InvalidPaymentMethod(String),

    #[error("Invalid setup intent: {0}")]
    InvalidSetupIntent(String),

    #[error("Payment declined: {0}")]
    PaymentDeclined(String),

    #[error("Card expired")]
    CardExpired,

    #[error("Insufficient funds")]
    InsufficientFunds,

    #[error("Authorization required: {0}")]
    AuthorizationRequired(String),

    #[error("Rate limited by payment provider")]
    RateLimited,

    #[error("Payment provider error: {0}")]
    ProviderError(String),

    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("Webhook signature verification failed")]
    InvalidWebhookSignature,

    #[error("Duplicate event: {0}")]
    DuplicateEvent(String),
}

impl From<anyhow::Error> for PaymentError {
    fn from(err: anyhow::Error) -> Self {
        PaymentError::ProviderError(err.to_string())
    }
}

impl From<sqlx::Error> for PaymentError {
    fn from(err: sqlx::Error) -> Self {
        PaymentError::DatabaseError(err.to_string())
    }
}

/// Result type for payment operations.
pub type PaymentResult<T> = Result<T, PaymentError>;

/// Trait for payment provider operations.
///
/// This abstraction allows for:
/// - Multiple payment provider implementations (Stripe, etc.)
/// - Mock implementations for testing
/// - Consistent error handling across providers
#[async_trait]
pub trait PaymentProvider: Send + Sync {
    /// Get the provider type.
    fn provider_type(&self) -> PaymentProviderType;

    // =========================================================================
    // Customer Operations
    // =========================================================================

    /// Get or create a customer for the organization.
    async fn get_or_create_customer(&self, organization_id: Uuid) -> PaymentResult<CustomerInfo>;

    /// Get customer by organization ID.
    async fn get_customer(&self, organization_id: Uuid) -> PaymentResult<Option<CustomerInfo>>;

    // =========================================================================
    // Payment Method Operations
    // =========================================================================

    /// Create a setup intent for adding a new payment method.
    /// Returns a client secret that the frontend uses to complete the setup.
    async fn create_setup_intent(&self, organization_id: Uuid) -> PaymentResult<SetupIntentInfo>;

    /// Confirm a payment method after the frontend completes setup.
    async fn confirm_payment_method(
        &self,
        organization_id: Uuid,
        setup_intent_id: &str,
        set_as_default: bool,
        created_by: Option<Uuid>,
    ) -> PaymentResult<PaymentMethodInfo>;

    /// List all payment methods for an organization.
    async fn list_payment_methods(
        &self,
        organization_id: Uuid,
    ) -> PaymentResult<Vec<PaymentMethodInfo>>;

    /// Get a specific payment method.
    async fn get_payment_method(
        &self,
        payment_method_id: Uuid,
    ) -> PaymentResult<Option<PaymentMethodInfo>>;

    /// Set a payment method as the default for an organization.
    async fn set_default_payment_method(
        &self,
        organization_id: Uuid,
        payment_method_id: Uuid,
    ) -> PaymentResult<()>;

    /// Delete a payment method.
    async fn delete_payment_method(
        &self,
        organization_id: Uuid,
        payment_method_id: Uuid,
    ) -> PaymentResult<()>;

    // =========================================================================
    // Subscription Operations
    // =========================================================================

    /// Create a subscription for an organization.
    async fn create_subscription(
        &self,
        organization_id: Uuid,
        price_id: &str,
        payment_method_id: Option<Uuid>,
    ) -> PaymentResult<SubscriptionInfo>;

    /// Update a subscription's price (for tier upgrades/downgrades).
    /// Stripe handles proration automatically.
    /// If no active subscription exists, creates a new one.
    async fn update_subscription(
        &self,
        organization_id: Uuid,
        new_price_id: &str,
        payment_method_id: Option<Uuid>,
    ) -> PaymentResult<SubscriptionInfo>;

    /// Cancel a subscription.
    async fn cancel_subscription(
        &self,
        organization_id: Uuid,
        at_period_end: bool,
    ) -> PaymentResult<()>;

    /// Get the current subscription for an organization.
    async fn get_subscription(
        &self,
        organization_id: Uuid,
    ) -> PaymentResult<Option<SubscriptionInfo>>;

    // =========================================================================
    // Invoice Operations
    // =========================================================================

    /// List invoices for an organization.
    async fn list_invoices(
        &self,
        organization_id: Uuid,
        limit: i64,
        offset: i64,
    ) -> PaymentResult<(Vec<InvoiceInfo>, i64)>;

    // =========================================================================
    // Billing Portal Operations
    // =========================================================================

    /// Create a billing portal session for the customer to manage their subscription.
    /// Returns the portal URL.
    async fn create_billing_portal_session(
        &self,
        organization_id: Uuid,
        return_url: &str,
    ) -> PaymentResult<String> {
        let _ = (organization_id, return_url);
        Err(PaymentError::NotConfigured)
    }

    /// Sync invoices from the payment provider to the local database.
    /// Returns the number of invoices synced.
    async fn sync_invoices(&self, organization_id: Uuid) -> PaymentResult<u64> {
        let _ = organization_id;
        Err(PaymentError::NotConfigured)
    }

    // =========================================================================
    // Webhook Operations
    // =========================================================================

    /// Verify and parse a webhook payload.
    /// Returns None if the event was already processed (idempotency).
    async fn verify_webhook(
        &self,
        payload: &str,
        signature: &str,
    ) -> PaymentResult<Option<WebhookEvent>>;

    /// Mark a webhook event as processed.
    async fn mark_event_processed(
        &self,
        event_id: &str,
        error_message: Option<&str>,
    ) -> PaymentResult<()>;
}

/// Provider type for identification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaymentProviderType {
    Stripe,
    Mock,
}

/// Customer information returned by the provider.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CustomerInfo {
    pub provider_customer_id: String,
    pub organization_id: Uuid,
    pub email: Option<String>,
    pub name: Option<String>,
}

/// Setup intent information for adding payment methods.
///
/// Note: The `client_secret` field is intentionally redacted in Debug output
/// to prevent accidental exposure in logs.
#[derive(Clone, serde::Serialize)]
pub struct SetupIntentInfo {
    pub client_secret: String,
    pub setup_intent_id: String,
}

impl std::fmt::Debug for SetupIntentInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SetupIntentInfo")
            .field("client_secret", &"[REDACTED]")
            .field("setup_intent_id", &self.setup_intent_id)
            .finish()
    }
}

/// Payment method information.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PaymentMethodInfo {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub provider_payment_method_id: String,
    pub display_name: Option<String>,
    pub card_brand: Option<String>,
    pub card_last_four: Option<String>,
    pub card_exp_month: Option<i32>,
    pub card_exp_year: Option<i32>,
    pub is_default: bool,
    pub status: PaymentMethodStatus,
}

/// Subscription information.
///
/// Note: The `client_secret` field is intentionally redacted in Debug output
/// to prevent accidental exposure in logs.
#[derive(Clone, serde::Serialize)]
pub struct SubscriptionInfo {
    pub subscription_id: String,
    pub status: String,
    pub current_period_start: Option<chrono::DateTime<chrono::Utc>>,
    pub current_period_end: Option<chrono::DateTime<chrono::Utc>>,
    /// Client secret for incomplete subscriptions requiring payment confirmation
    pub client_secret: Option<String>,
    pub cancel_at_period_end: bool,
}

impl std::fmt::Debug for SubscriptionInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscriptionInfo")
            .field("subscription_id", &self.subscription_id)
            .field("status", &self.status)
            .field("current_period_start", &self.current_period_start)
            .field("current_period_end", &self.current_period_end)
            .field(
                "client_secret",
                &self.client_secret.as_ref().map(|_| "[REDACTED]"),
            )
            .field("cancel_at_period_end", &self.cancel_at_period_end)
            .finish()
    }
}

/// Invoice information.
#[derive(Debug, Clone, serde::Serialize)]
pub struct InvoiceInfo {
    pub id: Uuid,
    pub invoice_number: String,
    pub status: String,
    pub total_cents: i64,
    pub currency: String,
    pub period_start: Option<chrono::DateTime<chrono::Utc>>,
    pub period_end: Option<chrono::DateTime<chrono::Utc>>,
    pub paid_at: Option<chrono::DateTime<chrono::Utc>>,
    pub invoice_pdf_url: Option<String>,
    pub hosted_invoice_url: Option<String>,
}

/// Webhook event types.
#[derive(Debug, Clone)]
pub enum WebhookEventType {
    SubscriptionCreated,
    SubscriptionUpdated,
    SubscriptionDeleted,
    InvoicePaid,
    InvoicePaymentFailed,
    PaymentMethodAttached,
    PaymentMethodDetached,
    CustomerUpdated,
    CustomerDeleted,
    CheckoutSessionCompleted,
    Unknown(String),
}

/// Parsed webhook event.
#[derive(Debug, Clone)]
pub struct WebhookEvent {
    pub event_id: String,
    pub event_type: WebhookEventType,
    pub organization_id: Option<Uuid>,
    pub data: serde_json::Value,
}

// ============================================================================
// Input Validation Helpers
// ============================================================================

/// Validate that a Stripe ID suffix contains only valid characters.
/// Stripe IDs use base62 encoding (alphanumeric characters only) after the prefix.
#[inline]
fn is_valid_stripe_id_suffix(suffix: &str) -> bool {
    !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_alphanumeric())
}

/// Validate a price ID format (Stripe price IDs start with "price_").
///
/// # Security
/// Validates both format and character set to prevent injection of malformed IDs.
/// Stripe price IDs are typically 20+ characters with alphanumeric suffix.
pub fn validate_price_id(price_id: &str) -> PaymentResult<()> {
    if price_id.is_empty() {
        return Err(PaymentError::InvalidPaymentMethod(
            "Price ID cannot be empty".into(),
        ));
    }

    const PREFIX: &str = "price_";
    const MIN_LENGTH: usize = 20; // "price_" (6) + at least 14 chars

    // Check prefix and minimum length
    if !price_id.starts_with(PREFIX) || price_id.len() < MIN_LENGTH {
        return Err(PaymentError::InvalidPaymentMethod(
            "Invalid price ID format".into(),
        ));
    }

    // Validate suffix contains only alphanumeric characters (Stripe uses base62)
    let suffix = &price_id[PREFIX.len()..];
    if !is_valid_stripe_id_suffix(suffix) {
        return Err(PaymentError::InvalidPaymentMethod(
            "Invalid price ID characters".into(),
        ));
    }

    Ok(())
}

/// Validate a setup intent ID format (Stripe setup intents start with "seti_").
///
/// # Security
/// Validates both format and character set to prevent injection of malformed IDs.
/// Stripe setup intent IDs are typically 25+ characters with alphanumeric suffix.
pub fn validate_setup_intent_id(setup_intent_id: &str) -> PaymentResult<()> {
    if setup_intent_id.is_empty() {
        return Err(PaymentError::InvalidSetupIntent(
            "Setup intent ID cannot be empty".into(),
        ));
    }

    const PREFIX: &str = "seti_";
    const MIN_LENGTH: usize = 20; // "seti_" (5) + at least 15 chars

    // Check prefix and minimum length
    if !setup_intent_id.starts_with(PREFIX) || setup_intent_id.len() < MIN_LENGTH {
        return Err(PaymentError::InvalidSetupIntent(
            "Invalid setup intent ID format".into(),
        ));
    }

    // Validate suffix contains only alphanumeric characters (Stripe uses base62)
    let suffix = &setup_intent_id[PREFIX.len()..];
    if !is_valid_stripe_id_suffix(suffix) {
        return Err(PaymentError::InvalidSetupIntent(
            "Invalid setup intent ID characters".into(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod validation_tests {
    use super::*;

    #[test]
    fn test_valid_price_id() {
        assert!(validate_price_id("price_1234567890abcdef").is_ok());
        assert!(validate_price_id("price_H5ggYwtDq4fbrJ").is_ok());
    }

    #[test]
    fn test_invalid_price_id() {
        // Too short
        assert!(validate_price_id("price_abc").is_err());
        // Wrong prefix
        assert!(validate_price_id("prod_1234567890abcdef").is_err());
        // Special characters
        assert!(validate_price_id("price_!@#$%^&*()abcdef").is_err());
        // Empty
        assert!(validate_price_id("").is_err());
    }

    #[test]
    fn test_valid_setup_intent_id() {
        assert!(validate_setup_intent_id("seti_1234567890abcdefgh").is_ok());
        assert!(validate_setup_intent_id("seti_H5ggYwtDq4fbrJQR").is_ok());
    }

    #[test]
    fn test_invalid_setup_intent_id() {
        // Too short
        assert!(validate_setup_intent_id("seti_abc").is_err());
        // Wrong prefix
        assert!(validate_setup_intent_id("pi_1234567890abcdefgh").is_err());
        // Special characters
        assert!(validate_setup_intent_id("seti_!@#$%^&*()abcdef").is_err());
        // Empty
        assert!(validate_setup_intent_id("").is_err());
    }
}
