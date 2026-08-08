//! Mock payment provider for testing.
//!
//! This module provides a mock implementation of the PaymentProvider trait
//! that stores data in memory and can be configured to simulate various
//! scenarios (success, failures, card declined, etc.).

use async_trait::async_trait;
use chrono::{Duration, Utc};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

use super::payments::PaymentMethodStatus;
use super::provider::*;

/// Configuration for mock behavior.
#[derive(Debug, Clone, Default)]
pub struct MockConfig {
    /// If true, all operations will fail
    pub fail_all: bool,
    /// If set, payment operations will return this error
    pub payment_error: Option<PaymentError>,
    /// Simulate rate limiting
    pub rate_limited: bool,
    /// Simulate card decline
    pub card_declined: bool,
    /// Simulate expired card
    pub card_expired: bool,
}

/// Mock payment provider for testing.
pub struct MockPaymentProvider {
    config: Arc<RwLock<MockConfig>>,
    customers: Arc<RwLock<HashMap<Uuid, CustomerInfo>>>,
    payment_methods: Arc<RwLock<HashMap<Uuid, PaymentMethodInfo>>>,
    subscriptions: Arc<RwLock<HashMap<Uuid, SubscriptionInfo>>>,
    invoices: Arc<RwLock<HashMap<Uuid, Vec<InvoiceInfo>>>>,
    setup_intents: Arc<RwLock<HashMap<String, Uuid>>>,
    events: Arc<RwLock<HashMap<String, bool>>>,
    next_id_counter: Arc<RwLock<u64>>,
}

impl MockPaymentProvider {
    /// Create a new mock payment provider.
    pub fn new() -> Self {
        Self {
            config: Arc::new(RwLock::new(MockConfig::default())),
            customers: Arc::new(RwLock::new(HashMap::new())),
            payment_methods: Arc::new(RwLock::new(HashMap::new())),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            invoices: Arc::new(RwLock::new(HashMap::new())),
            setup_intents: Arc::new(RwLock::new(HashMap::new())),
            events: Arc::new(RwLock::new(HashMap::new())),
            next_id_counter: Arc::new(RwLock::new(1)),
        }
    }

    /// Configure the mock behavior.
    pub fn configure(&self, config: MockConfig) {
        *self.config.write().unwrap() = config;
    }

    /// Set the mock to fail all operations.
    pub fn set_fail_all(&self, fail: bool) {
        self.config.write().unwrap().fail_all = fail;
    }

    /// Set the mock to simulate card decline.
    pub fn set_card_declined(&self, declined: bool) {
        self.config.write().unwrap().card_declined = declined;
    }

    /// Set the mock to simulate expired card.
    pub fn set_card_expired(&self, expired: bool) {
        self.config.write().unwrap().card_expired = expired;
    }

    /// Set the mock to simulate rate limiting.
    pub fn set_rate_limited(&self, limited: bool) {
        self.config.write().unwrap().rate_limited = limited;
    }

    /// Get the number of payment methods stored.
    pub fn payment_method_count(&self) -> usize {
        self.payment_methods.read().unwrap().len()
    }

    /// Get the number of customers stored.
    pub fn customer_count(&self) -> usize {
        self.customers.read().unwrap().len()
    }

    /// Clear all stored data.
    pub fn clear(&self) {
        self.customers.write().unwrap().clear();
        self.payment_methods.write().unwrap().clear();
        self.subscriptions.write().unwrap().clear();
        self.invoices.write().unwrap().clear();
        self.setup_intents.write().unwrap().clear();
        self.events.write().unwrap().clear();
    }

    /// Add a mock invoice for testing.
    pub fn add_invoice(&self, organization_id: Uuid, invoice: InvoiceInfo) {
        let mut invoices = self.invoices.write().unwrap();
        invoices.entry(organization_id).or_default().push(invoice);
    }

    fn check_config(&self) -> PaymentResult<()> {
        let config = self.config.read().unwrap();
        if config.fail_all {
            return Err(PaymentError::ProviderError(
                "Mock configured to fail".into(),
            ));
        }
        if config.rate_limited {
            return Err(PaymentError::RateLimited);
        }
        if config.card_declined {
            return Err(PaymentError::PaymentDeclined("Card declined".into()));
        }
        if config.card_expired {
            return Err(PaymentError::CardExpired);
        }
        if let Some(ref err) = config.payment_error {
            return Err(err.clone());
        }
        Ok(())
    }

    fn next_id(&self) -> u64 {
        let mut counter = self.next_id_counter.write().unwrap();
        let id = *counter;
        *counter += 1;
        id
    }
}

impl Default for MockPaymentProvider {
    fn default() -> Self {
        Self::new()
    }
}

// Implement Clone for PaymentError to allow storing it in config
impl Clone for PaymentError {
    fn clone(&self) -> Self {
        match self {
            Self::NotConfigured => Self::NotConfigured,
            Self::CustomerNotFound(s) => Self::CustomerNotFound(s.clone()),
            Self::PaymentMethodNotFound(id) => Self::PaymentMethodNotFound(*id),
            Self::SubscriptionNotFound(id) => Self::SubscriptionNotFound(*id),
            Self::SubscriptionAlreadyCanceled => Self::SubscriptionAlreadyCanceled,
            Self::InvalidPaymentMethod(s) => Self::InvalidPaymentMethod(s.clone()),
            Self::InvalidSetupIntent(s) => Self::InvalidSetupIntent(s.clone()),
            Self::PaymentDeclined(s) => Self::PaymentDeclined(s.clone()),
            Self::CardExpired => Self::CardExpired,
            Self::InsufficientFunds => Self::InsufficientFunds,
            Self::AuthorizationRequired(s) => Self::AuthorizationRequired(s.clone()),
            Self::RateLimited => Self::RateLimited,
            Self::ProviderError(s) => Self::ProviderError(s.clone()),
            Self::DatabaseError(s) => Self::DatabaseError(s.clone()),
            Self::InvalidWebhookSignature => Self::InvalidWebhookSignature,
            Self::DuplicateEvent(s) => Self::DuplicateEvent(s.clone()),
        }
    }
}

#[async_trait]
impl PaymentProvider for MockPaymentProvider {
    fn provider_type(&self) -> PaymentProviderType {
        PaymentProviderType::Mock
    }

    async fn get_or_create_customer(&self, organization_id: Uuid) -> PaymentResult<CustomerInfo> {
        self.check_config()?;

        let mut customers = self.customers.write().unwrap();
        if let Some(customer) = customers.get(&organization_id) {
            return Ok(customer.clone());
        }

        let customer = CustomerInfo {
            provider_customer_id: format!("cus_mock_{}", self.next_id()),
            organization_id,
            email: Some(format!("org-{}@example.com", organization_id)),
            name: Some(format!("Organization {}", organization_id)),
        };

        customers.insert(organization_id, customer.clone());
        Ok(customer)
    }

    async fn get_customer(&self, organization_id: Uuid) -> PaymentResult<Option<CustomerInfo>> {
        self.check_config()?;
        Ok(self
            .customers
            .read()
            .unwrap()
            .get(&organization_id)
            .cloned())
    }

    async fn create_setup_intent(&self, organization_id: Uuid) -> PaymentResult<SetupIntentInfo> {
        self.check_config()?;

        // Ensure customer exists
        self.get_or_create_customer(organization_id).await?;

        // Generate a valid Stripe-format setup intent ID
        // Must be 20+ chars with alphanumeric suffix (no underscores)
        // Format: seti_ + 15 alphanumeric chars (total 20 chars minimum)
        let setup_intent_id = format!("seti_testmock{:010}", self.next_id());
        let client_secret = format!("{}_secret_mock", setup_intent_id);

        self.setup_intents
            .write()
            .unwrap()
            .insert(setup_intent_id.clone(), organization_id);

        Ok(SetupIntentInfo {
            client_secret,
            setup_intent_id,
        })
    }

    async fn confirm_payment_method(
        &self,
        organization_id: Uuid,
        setup_intent_id: &str,
        set_as_default: bool,
        _created_by: Option<Uuid>,
    ) -> PaymentResult<PaymentMethodInfo> {
        self.check_config()?;
        validate_setup_intent_id(setup_intent_id)?;

        // Verify setup intent exists
        let intent_org = self
            .setup_intents
            .read()
            .unwrap()
            .get(setup_intent_id)
            .copied()
            .ok_or_else(|| PaymentError::InvalidSetupIntent(setup_intent_id.into()))?;

        if intent_org != organization_id {
            return Err(PaymentError::InvalidSetupIntent(
                "Setup intent belongs to different organization".into(),
            ));
        }

        // If setting as default, unset other defaults
        if set_as_default {
            let mut methods = self.payment_methods.write().unwrap();
            for pm in methods.values_mut() {
                if pm.organization_id == organization_id {
                    pm.is_default = false;
                }
            }
        }

        let pm_id = Uuid::new_v4();
        let pm = PaymentMethodInfo {
            id: pm_id,
            organization_id,
            provider_payment_method_id: format!("pm_mock_{}", self.next_id()),
            display_name: Some("Visa ending in 4242".into()),
            card_brand: Some("visa".into()),
            card_last_four: Some("4242".into()),
            card_exp_month: Some(12),
            card_exp_year: Some(2030),
            is_default: set_as_default,
            status: PaymentMethodStatus::Active,
        };

        self.payment_methods
            .write()
            .unwrap()
            .insert(pm_id, pm.clone());

        // Remove setup intent
        self.setup_intents.write().unwrap().remove(setup_intent_id);

        Ok(pm)
    }

    async fn list_payment_methods(
        &self,
        organization_id: Uuid,
    ) -> PaymentResult<Vec<PaymentMethodInfo>> {
        self.check_config()?;

        let methods: Vec<_> = self
            .payment_methods
            .read()
            .unwrap()
            .values()
            .filter(|pm| {
                pm.organization_id == organization_id && pm.status == PaymentMethodStatus::Active
            })
            .cloned()
            .collect();

        Ok(methods)
    }

    async fn get_payment_method(
        &self,
        payment_method_id: Uuid,
    ) -> PaymentResult<Option<PaymentMethodInfo>> {
        self.check_config()?;
        Ok(self
            .payment_methods
            .read()
            .unwrap()
            .get(&payment_method_id)
            .cloned())
    }

    async fn set_default_payment_method(
        &self,
        organization_id: Uuid,
        payment_method_id: Uuid,
    ) -> PaymentResult<()> {
        self.check_config()?;

        let mut methods = self.payment_methods.write().unwrap();

        // Verify ownership
        let pm = methods
            .get(&payment_method_id)
            .ok_or(PaymentError::PaymentMethodNotFound(payment_method_id))?;

        if pm.organization_id != organization_id {
            return Err(PaymentError::PaymentMethodNotFound(payment_method_id));
        }

        // Update defaults
        for pm in methods.values_mut() {
            if pm.organization_id == organization_id {
                pm.is_default = pm.id == payment_method_id;
            }
        }

        Ok(())
    }

    async fn delete_payment_method(
        &self,
        organization_id: Uuid,
        payment_method_id: Uuid,
    ) -> PaymentResult<()> {
        self.check_config()?;

        let mut methods = self.payment_methods.write().unwrap();

        let pm = methods
            .get(&payment_method_id)
            .ok_or(PaymentError::PaymentMethodNotFound(payment_method_id))?;

        if pm.organization_id != organization_id {
            return Err(PaymentError::PaymentMethodNotFound(payment_method_id));
        }

        methods.remove(&payment_method_id);
        Ok(())
    }

    async fn create_subscription(
        &self,
        organization_id: Uuid,
        price_id: &str,
        _payment_method_id: Option<Uuid>,
    ) -> PaymentResult<SubscriptionInfo> {
        self.check_config()?;
        validate_price_id(price_id)?;

        let now = Utc::now();
        let sub = SubscriptionInfo {
            subscription_id: format!("sub_mock_{}", self.next_id()),
            status: "active".into(),
            current_period_start: Some(now),
            current_period_end: Some(now + Duration::days(30)),
            client_secret: None,
            cancel_at_period_end: false,
        };

        self.subscriptions
            .write()
            .unwrap()
            .insert(organization_id, sub.clone());
        Ok(sub)
    }

    async fn update_subscription(
        &self,
        organization_id: Uuid,
        new_price_id: &str,
        _payment_method_id: Option<Uuid>,
    ) -> PaymentResult<SubscriptionInfo> {
        self.check_config()?;
        validate_price_id(new_price_id)?;

        let existing = {
            let subs = self.subscriptions.read().unwrap();
            subs.get(&organization_id).cloned()
        };

        if let Some(sub) = existing {
            Ok(sub)
        } else {
            self.create_subscription(organization_id, new_price_id, None)
                .await
        }
    }

    async fn cancel_subscription(
        &self,
        organization_id: Uuid,
        at_period_end: bool,
    ) -> PaymentResult<()> {
        self.check_config()?;

        let mut subs = self.subscriptions.write().unwrap();
        let sub = subs
            .get_mut(&organization_id)
            .ok_or(PaymentError::SubscriptionNotFound(organization_id))?;

        if at_period_end {
            sub.cancel_at_period_end = true;
        } else {
            sub.status = "canceled".into();
        }

        Ok(())
    }

    async fn get_subscription(
        &self,
        organization_id: Uuid,
    ) -> PaymentResult<Option<SubscriptionInfo>> {
        self.check_config()?;
        Ok(self
            .subscriptions
            .read()
            .unwrap()
            .get(&organization_id)
            .cloned())
    }

    async fn list_invoices(
        &self,
        organization_id: Uuid,
        limit: i64,
        offset: i64,
    ) -> PaymentResult<(Vec<InvoiceInfo>, i64)> {
        self.check_config()?;

        let invoices = self.invoices.read().unwrap();
        let org_invoices = invoices.get(&organization_id).cloned().unwrap_or_default();
        let total = org_invoices.len() as i64;

        let paginated: Vec<_> = org_invoices
            .into_iter()
            .skip(offset as usize)
            .take(limit as usize)
            .collect();

        Ok((paginated, total))
    }

    async fn verify_webhook(
        &self,
        payload: &str,
        _signature: &str,
    ) -> PaymentResult<Option<WebhookEvent>> {
        self.check_config()?;

        // Parse the payload as JSON to get event ID
        let data: serde_json::Value =
            serde_json::from_str(payload).map_err(|_| PaymentError::InvalidWebhookSignature)?;

        let event_id = data
            .get("id")
            .and_then(|v| v.as_str())
            .ok_or(PaymentError::InvalidWebhookSignature)?
            .to_string();

        // Check for duplicate
        let mut events = self.events.write().unwrap();
        if events.contains_key(&event_id) {
            return Ok(None);
        }
        events.insert(event_id.clone(), false);

        let event_type = data
            .get("type")
            .and_then(|v| v.as_str())
            .map(|s| match s {
                "customer.subscription.created" => WebhookEventType::SubscriptionCreated,
                "customer.subscription.updated" => WebhookEventType::SubscriptionUpdated,
                "customer.subscription.deleted" => WebhookEventType::SubscriptionDeleted,
                "customer.deleted" => WebhookEventType::CustomerDeleted,
                "invoice.paid" => WebhookEventType::InvoicePaid,
                "invoice.payment_failed" => WebhookEventType::InvoicePaymentFailed,
                other => WebhookEventType::Unknown(other.to_string()),
            })
            .unwrap_or(WebhookEventType::Unknown("unknown".into()));

        Ok(Some(WebhookEvent {
            event_id,
            event_type,
            organization_id: None,
            data,
        }))
    }

    async fn mark_event_processed(
        &self,
        event_id: &str,
        _error_message: Option<&str>,
    ) -> PaymentResult<()> {
        self.check_config()?;
        self.events
            .write()
            .unwrap()
            .insert(event_id.to_string(), true);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_customer() {
        let provider = MockPaymentProvider::new();
        let org_id = Uuid::new_v4();

        let customer = provider.get_or_create_customer(org_id).await.unwrap();
        assert_eq!(customer.organization_id, org_id);
        assert!(customer.provider_customer_id.starts_with("cus_mock_"));

        // Getting again should return same customer
        let customer2 = provider.get_or_create_customer(org_id).await.unwrap();
        assert_eq!(
            customer.provider_customer_id,
            customer2.provider_customer_id
        );
    }

    #[tokio::test]
    async fn test_payment_method_flow() {
        let provider = MockPaymentProvider::new();
        let org_id = Uuid::new_v4();

        // Create setup intent
        let intent = provider.create_setup_intent(org_id).await.unwrap();
        // Mock generates valid Stripe-format IDs: seti_testmock + 10 digits
        assert!(intent.setup_intent_id.starts_with("seti_testmock"));

        // Confirm payment method
        let pm = provider
            .confirm_payment_method(org_id, &intent.setup_intent_id, true, None)
            .await
            .unwrap();
        assert_eq!(pm.organization_id, org_id);
        assert!(pm.is_default);

        // List payment methods
        let methods = provider.list_payment_methods(org_id).await.unwrap();
        assert_eq!(methods.len(), 1);
    }

    #[tokio::test]
    async fn test_subscription_flow() {
        let provider = MockPaymentProvider::new();
        let org_id = Uuid::new_v4();

        // Create subscription with valid Stripe price ID format (20+ chars, alphanumeric)
        let sub = provider
            .create_subscription(org_id, "price_test1234567890ab", None)
            .await
            .unwrap();
        assert_eq!(sub.status, "active");

        // Get subscription
        let sub2 = provider.get_subscription(org_id).await.unwrap().unwrap();
        assert_eq!(sub.subscription_id, sub2.subscription_id);

        // Cancel subscription
        provider.cancel_subscription(org_id, true).await.unwrap();
        let sub3 = provider.get_subscription(org_id).await.unwrap().unwrap();
        assert!(sub3.cancel_at_period_end);
    }

    #[tokio::test]
    async fn test_error_simulation() {
        let provider = MockPaymentProvider::new();
        let org_id = Uuid::new_v4();

        // Simulate card decline
        provider.set_card_declined(true);
        let result = provider.create_setup_intent(org_id).await;
        assert!(matches!(result, Err(PaymentError::PaymentDeclined(_))));

        // Reset and test rate limiting
        provider.set_card_declined(false);
        provider.set_rate_limited(true);
        let result = provider.create_setup_intent(org_id).await;
        assert!(matches!(result, Err(PaymentError::RateLimited)));
    }

    #[tokio::test]
    async fn test_validation() {
        let provider = MockPaymentProvider::new();
        let org_id = Uuid::new_v4();

        // Invalid setup intent ID
        let result = provider
            .confirm_payment_method(org_id, "invalid", true, None)
            .await;
        assert!(matches!(result, Err(PaymentError::InvalidSetupIntent(_))));

        // Invalid price ID
        let result = provider.create_subscription(org_id, "invalid", None).await;
        assert!(matches!(result, Err(PaymentError::InvalidPaymentMethod(_))));
    }
}
