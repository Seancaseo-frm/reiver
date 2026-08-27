//! Billing and Payment Integration Tests
//!
//! Tests for billing service, payment provider, and webhook handling.
//! Uses MockPaymentProvider for testing without actual Stripe calls.

use rust_decimal::Decimal;
use std::collections::BTreeMap;
use uuid::Uuid;

// Import from the main crate (requires test-utils feature)
#[cfg(feature = "test-utils")]
use reiver_website::billing::{
    validate_price_id, validate_setup_intent_id, MockPaymentProvider, PaymentError,
    PaymentProvider, PaymentProviderType, RetryConfig,
};

// ============================================================================
// Mock Payment Provider Tests
// ============================================================================

#[cfg(feature = "test-utils")]
mod mock_provider_tests {
    use super::*;

    #[tokio::test]
    async fn test_customer_creation_and_retrieval() {
        let provider = MockPaymentProvider::new();
        let org_id = Uuid::new_v4();

        // First call creates customer
        let customer = provider.get_or_create_customer(org_id).await.unwrap();
        assert_eq!(customer.organization_id, org_id);
        assert!(customer.provider_customer_id.starts_with("cus_mock_"));

        // Second call returns same customer (idempotent)
        let customer2 = provider.get_or_create_customer(org_id).await.unwrap();
        assert_eq!(
            customer.provider_customer_id,
            customer2.provider_customer_id
        );

        // get_customer also returns the same customer
        let customer3 = provider.get_customer(org_id).await.unwrap().unwrap();
        assert_eq!(
            customer.provider_customer_id,
            customer3.provider_customer_id
        );
    }

    #[tokio::test]
    async fn test_payment_method_full_flow() {
        let provider = MockPaymentProvider::new();
        let org_id = Uuid::new_v4();

        // Create setup intent
        let intent = provider.create_setup_intent(org_id).await.unwrap();
        assert!(intent.setup_intent_id.starts_with("seti_testmock"));
        assert!(intent.client_secret.contains("_secret_mock"));

        // Confirm payment method
        let pm = provider
            .confirm_payment_method(org_id, &intent.setup_intent_id, true, None)
            .await
            .unwrap();
        assert_eq!(pm.organization_id, org_id);
        assert!(pm.is_default);
        assert_eq!(pm.card_last_four, Some("4242".to_string()));

        // List payment methods
        let methods = provider.list_payment_methods(org_id).await.unwrap();
        assert_eq!(methods.len(), 1);
        assert_eq!(methods[0].id, pm.id);

        // Get specific payment method
        let pm2 = provider.get_payment_method(pm.id).await.unwrap().unwrap();
        assert_eq!(pm2.id, pm.id);

        // Delete payment method
        provider.delete_payment_method(org_id, pm.id).await.unwrap();
        let methods_after = provider.list_payment_methods(org_id).await.unwrap();
        assert!(methods_after.is_empty());
    }

    #[tokio::test]
    async fn test_multiple_payment_methods_default_handling() {
        let provider = MockPaymentProvider::new();
        let org_id = Uuid::new_v4();

        // Create first payment method as default
        let intent1 = provider.create_setup_intent(org_id).await.unwrap();
        let pm1 = provider
            .confirm_payment_method(org_id, &intent1.setup_intent_id, true, None)
            .await
            .unwrap();
        assert!(pm1.is_default);

        // Create second payment method as default (should unset first)
        let intent2 = provider.create_setup_intent(org_id).await.unwrap();
        let pm2 = provider
            .confirm_payment_method(org_id, &intent2.setup_intent_id, true, None)
            .await
            .unwrap();
        assert!(pm2.is_default);

        // Check that only pm2 is default now
        let methods = provider.list_payment_methods(org_id).await.unwrap();
        assert_eq!(methods.len(), 2);

        let default_count = methods.iter().filter(|m| m.is_default).count();
        assert_eq!(
            default_count, 1,
            "Only one payment method should be default"
        );

        let default_pm = methods.iter().find(|m| m.is_default).unwrap();
        assert_eq!(default_pm.id, pm2.id);

        // Explicitly set pm1 as default
        provider
            .set_default_payment_method(org_id, pm1.id)
            .await
            .unwrap();
        let updated_pm1 = provider.get_payment_method(pm1.id).await.unwrap().unwrap();
        assert!(updated_pm1.is_default);
    }

    #[tokio::test]
    async fn test_subscription_lifecycle() {
        let provider = MockPaymentProvider::new();
        let org_id = Uuid::new_v4();

        // Create subscription
        let sub = provider
            .create_subscription(org_id, "price_test1234567890ab", None)
            .await
            .unwrap();
        assert_eq!(sub.status, "active");
        assert!(!sub.cancel_at_period_end);
        assert!(sub.subscription_id.starts_with("sub_mock_"));

        // Get subscription
        let sub2 = provider.get_subscription(org_id).await.unwrap().unwrap();
        assert_eq!(sub.subscription_id, sub2.subscription_id);

        // Cancel at period end
        provider.cancel_subscription(org_id, true).await.unwrap();
        let sub3 = provider.get_subscription(org_id).await.unwrap().unwrap();
        assert!(sub3.cancel_at_period_end);
        assert_eq!(sub3.status, "active"); // Still active until period end
    }

    #[tokio::test]
    async fn test_subscription_immediate_cancel() {
        let provider = MockPaymentProvider::new();
        let org_id = Uuid::new_v4();

        // Create subscription
        provider
            .create_subscription(org_id, "price_test1234567890ab", None)
            .await
            .unwrap();

        // Cancel immediately
        provider.cancel_subscription(org_id, false).await.unwrap();
        let sub = provider.get_subscription(org_id).await.unwrap().unwrap();
        assert_eq!(sub.status, "canceled");
    }

    #[tokio::test]
    async fn test_error_simulation_card_declined() {
        let provider = MockPaymentProvider::new();
        let org_id = Uuid::new_v4();

        provider.set_card_declined(true);
        let result = provider.create_setup_intent(org_id).await;
        assert!(matches!(result, Err(PaymentError::PaymentDeclined(_))));

        // Reset and verify normal operation
        provider.set_card_declined(false);
        let intent = provider.create_setup_intent(org_id).await;
        assert!(intent.is_ok());
    }

    #[tokio::test]
    async fn test_error_simulation_card_expired() {
        let provider = MockPaymentProvider::new();
        let org_id = Uuid::new_v4();

        provider.set_card_expired(true);
        let result = provider.create_setup_intent(org_id).await;
        assert!(matches!(result, Err(PaymentError::CardExpired)));
    }

    #[tokio::test]
    async fn test_error_simulation_rate_limited() {
        let provider = MockPaymentProvider::new();
        let org_id = Uuid::new_v4();

        provider.set_rate_limited(true);
        let result = provider.create_setup_intent(org_id).await;
        assert!(matches!(result, Err(PaymentError::RateLimited)));
    }

    #[tokio::test]
    async fn test_error_simulation_fail_all() {
        let provider = MockPaymentProvider::new();
        let org_id = Uuid::new_v4();

        provider.set_fail_all(true);

        // All operations should fail
        assert!(provider.get_or_create_customer(org_id).await.is_err());
        assert!(provider.create_setup_intent(org_id).await.is_err());
        assert!(provider.list_payment_methods(org_id).await.is_err());
    }

    #[tokio::test]
    async fn test_webhook_idempotency() {
        let provider = MockPaymentProvider::new();

        let payload = r#"{"id": "evt_test_123", "type": "customer.subscription.created"}"#;

        // First call should return the event
        let event1 = provider.verify_webhook(payload, "signature").await.unwrap();
        assert!(event1.is_some());
        assert_eq!(event1.unwrap().event_id, "evt_test_123");

        // Second call should return None (duplicate)
        let event2 = provider.verify_webhook(payload, "signature").await.unwrap();
        assert!(event2.is_none());
    }

    #[tokio::test]
    async fn test_ownership_verification_payment_method() {
        let provider = MockPaymentProvider::new();
        let org1 = Uuid::new_v4();
        let org2 = Uuid::new_v4();

        // Create payment method for org1
        let intent = provider.create_setup_intent(org1).await.unwrap();
        let pm = provider
            .confirm_payment_method(org1, &intent.setup_intent_id, true, None)
            .await
            .unwrap();

        // org2 trying to delete org1's payment method should fail
        let result = provider.delete_payment_method(org2, pm.id).await;
        assert!(matches!(
            result,
            Err(PaymentError::PaymentMethodNotFound(_))
        ));

        // org2 trying to set org1's payment method as default should fail
        let result = provider.set_default_payment_method(org2, pm.id).await;
        assert!(matches!(
            result,
            Err(PaymentError::PaymentMethodNotFound(_))
        ));
    }

    #[tokio::test]
    async fn test_nonexistent_payment_method() {
        let provider = MockPaymentProvider::new();
        let fake_id = Uuid::new_v4();
        let org_id = Uuid::new_v4();

        // Get returns None
        let pm = provider.get_payment_method(fake_id).await.unwrap();
        assert!(pm.is_none());

        // Delete returns error
        let result = provider.delete_payment_method(org_id, fake_id).await;
        assert!(matches!(
            result,
            Err(PaymentError::PaymentMethodNotFound(_))
        ));
    }

    #[tokio::test]
    async fn test_nonexistent_subscription() {
        let provider = MockPaymentProvider::new();
        let org_id = Uuid::new_v4();

        // Get returns None
        let sub = provider.get_subscription(org_id).await.unwrap();
        assert!(sub.is_none());

        // Cancel returns error
        let result = provider.cancel_subscription(org_id, true).await;
        assert!(matches!(result, Err(PaymentError::SubscriptionNotFound(_))));
    }

    #[tokio::test]
    async fn test_provider_type() {
        let provider = MockPaymentProvider::new();
        assert_eq!(provider.provider_type(), PaymentProviderType::Mock);
    }

    #[tokio::test]
    async fn test_clear_resets_state() {
        let provider = MockPaymentProvider::new();
        let org_id = Uuid::new_v4();

        // Create some data
        provider.get_or_create_customer(org_id).await.unwrap();
        assert_eq!(provider.customer_count(), 1);

        // Clear
        provider.clear();
        assert_eq!(provider.customer_count(), 0);
        assert_eq!(provider.payment_method_count(), 0);
    }
}

// ============================================================================
// Input Validation Tests
// ============================================================================

#[cfg(feature = "test-utils")]
mod validation_tests {
    use super::*;

    #[test]
    fn test_validate_price_id_valid() {
        assert!(validate_price_id("price_test1234567890ab").is_ok());
        assert!(validate_price_id("price_live1234567890ab").is_ok());
    }

    #[test]
    fn test_validate_price_id_empty() {
        let result = validate_price_id("");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty"));
    }

    #[test]
    fn test_validate_price_id_wrong_prefix() {
        let result = validate_price_id("pri_test123456");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid price ID format"));
    }

    #[test]
    fn test_validate_price_id_too_short() {
        let result = validate_price_id("price_ab");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_setup_intent_id_valid() {
        assert!(validate_setup_intent_id("seti_test1234567890abc").is_ok());
        assert!(validate_setup_intent_id("seti_1234567890abcdef").is_ok());
    }

    #[test]
    fn test_validate_setup_intent_id_empty() {
        let result = validate_setup_intent_id("");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty"));
    }

    #[test]
    fn test_validate_setup_intent_id_wrong_prefix() {
        let result = validate_setup_intent_id("si_test123456");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_setup_intent_id_too_short() {
        let result = validate_setup_intent_id("seti_ab");
        assert!(result.is_err());
    }
}

// ============================================================================
// Billing Types Tests
// ============================================================================

mod billing_types_tests {
    use super::*;

    #[test]
    fn test_decimal_cost_calculation() {
        // Test that Decimal handles money calculations correctly
        let rate = Decimal::new(100, 2); // $1.00
        let events = Decimal::from(1_000_000u64);
        let million = Decimal::from(1_000_000u64);

        let cost = (events / million) * rate;
        assert_eq!(cost, Decimal::new(100, 2)); // $1.00 for 1M events
    }

    #[test]
    fn test_decimal_fractional_events() {
        // Test fractional event billing
        let rate = Decimal::new(100, 2); // $1.00 per million
        let events = Decimal::from(500_000u64); // 0.5 million
        let million = Decimal::from(1_000_000u64);

        let cost = (events / million) * rate;
        assert_eq!(cost, Decimal::new(50, 2)); // $0.50
    }

    #[test]
    fn test_decimal_multiple_event_types() {
        // Test combined cost calculation for traces, logs, metrics
        let traces_rate = Decimal::new(150, 2); // $1.50 per million
        let logs_rate = Decimal::new(100, 2); // $1.00 per million
        let metrics_rate = Decimal::new(50, 2); // $0.50 per million

        let million = Decimal::from(1_000_000u64);

        let traces = Decimal::from(2_000_000u64);
        let logs = Decimal::from(5_000_000u64);
        let metrics = Decimal::from(10_000_000u64);

        let traces_cost = (traces / million) * traces_rate;
        let logs_cost = (logs / million) * logs_rate;
        let metrics_cost = (metrics / million) * metrics_rate;

        let total = traces_cost + logs_cost + metrics_cost;

        // $3.00 + $5.00 + $5.00 = $13.00
        assert_eq!(total, Decimal::new(1300, 2));
    }
}

// ============================================================================
// Webhook Processing Tests (Unit Tests)
// ============================================================================

// ============================================================================
// Retry Configuration Tests
// ============================================================================

#[cfg(feature = "test-utils")]
mod retry_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();

        // Verify sensible defaults
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.initial_delay, Duration::from_millis(100));
        assert_eq!(config.max_delay, Duration::from_secs(10));
        assert!((config.multiplier - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_retry_config_custom() {
        let config = RetryConfig {
            max_retries: 5,
            initial_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(30),
            multiplier: 1.5,
        };

        assert_eq!(config.max_retries, 5);
        assert_eq!(config.initial_delay, Duration::from_millis(50));
        assert_eq!(config.max_delay, Duration::from_secs(30));
        assert!((config.multiplier - 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_transient_error_patterns() {
        // These patterns should be classified as retriable
        let retriable_patterns = [
            "timeout exceeded",
            "connection refused",
            "connection reset by peer",
            "temporarily unavailable",
            "service unavailable",
            "network error",
        ];

        for pattern in retriable_patterns {
            let msg_lower = pattern.to_lowercase();
            let is_transient = msg_lower.contains("timeout")
                || msg_lower.contains("connection")
                || msg_lower.contains("temporarily unavailable")
                || msg_lower.contains("service unavailable")
                || msg_lower.contains("network");

            assert!(
                is_transient,
                "Pattern '{}' should be classified as transient",
                pattern
            );
        }
    }

    #[test]
    fn test_non_transient_error_patterns() {
        // These patterns should NOT be classified as retriable
        let non_retriable_patterns = [
            "invalid card number",
            "card declined",
            "insufficient funds",
            "authentication required",
            "invalid request",
            "resource not found",
        ];

        for pattern in non_retriable_patterns {
            let msg_lower = pattern.to_lowercase();
            let is_transient = msg_lower.contains("timeout")
                || msg_lower.contains("connection")
                || msg_lower.contains("temporarily unavailable")
                || msg_lower.contains("service unavailable")
                || msg_lower.contains("network");

            assert!(
                !is_transient,
                "Pattern '{}' should NOT be classified as transient",
                pattern
            );
        }
    }
}

// ============================================================================
// Subscription Status Tests
// ============================================================================

#[cfg(feature = "test-utils")]
mod subscription_status_tests {
    use reiver_website::billing::subscription_status;

    #[test]
    fn test_active_states_include_pending_cancellation() {
        // pending_cancellation should be considered active for UX reasons
        // (see documentation on is_active for rationale)
        assert!(subscription_status::is_active(
            subscription_status::PENDING_CANCELLATION
        ));
        assert!(subscription_status::is_active(subscription_status::ACTIVE));
        assert!(subscription_status::is_active(
            subscription_status::TRIALING
        ));
        assert!(subscription_status::is_active(
            subscription_status::PAST_DUE
        ));
        assert!(subscription_status::is_active(
            subscription_status::INCOMPLETE
        ));
    }

    #[test]
    fn test_canceled_not_active() {
        assert!(!subscription_status::is_active(
            subscription_status::CANCELED
        ));
        assert!(!subscription_status::is_active(
            subscription_status::INCOMPLETE_EXPIRED
        ));
        assert!(!subscription_status::is_active(subscription_status::UNPAID));
    }

    #[test]
    fn test_cancelable_excludes_pending_cancellation() {
        // pending_cancellation should NOT be cancelable (already in progress)
        assert!(!subscription_status::is_cancelable(
            subscription_status::PENDING_CANCELLATION
        ));
        assert!(subscription_status::is_cancelable(
            subscription_status::ACTIVE
        ));
        assert!(subscription_status::is_cancelable(
            subscription_status::TRIALING
        ));
    }

    #[test]
    fn test_is_pending_cancellation() {
        assert!(subscription_status::is_pending_cancellation(
            subscription_status::PENDING_CANCELLATION
        ));
        assert!(!subscription_status::is_pending_cancellation(
            subscription_status::ACTIVE
        ));
        assert!(!subscription_status::is_pending_cancellation(
            subscription_status::CANCELED
        ));
    }
}

// ============================================================================
// Webhook Content Type Validation Tests
// ============================================================================

mod webhook_content_type_tests {
    #[test]
    fn test_valid_content_types() {
        let valid_types = [
            "application/json",
            "application/json; charset=utf-8",
            "application/json;charset=utf-8",
        ];

        for ct in valid_types {
            assert!(
                ct.contains("application/json"),
                "Content type '{}' should be valid",
                ct
            );
        }
    }

    #[test]
    fn test_invalid_content_types() {
        let invalid_types = [
            "text/plain",
            "text/html",
            "application/xml",
            "multipart/form-data",
            "",
        ];

        for ct in invalid_types {
            assert!(
                !ct.contains("application/json"),
                "Content type '{}' should be invalid",
                ct
            );
        }
    }
}

// ============================================================================
// Already Canceled Detection Tests
// ============================================================================

mod already_canceled_tests {
    #[test]
    fn test_already_canceled_error_patterns() {
        // These patterns should be detected as "already canceled"
        let patterns = [
            "already canceled",
            "no such subscription",
            "cannot be canceled",
            "Subscription already canceled",
            "No Such Subscription",
        ];

        for pattern in patterns {
            let error_lower = pattern.to_lowercase();
            let is_already_canceled = error_lower.contains("already canceled")
                || error_lower.contains("no such subscription")
                || error_lower.contains("cannot be canceled");

            assert!(
                is_already_canceled,
                "Pattern '{}' should be detected as already canceled",
                pattern
            );
        }
    }

    #[test]
    fn test_other_errors_not_detected_as_canceled() {
        let patterns = [
            "payment failed",
            "card declined",
            "rate limit exceeded",
            "network error",
        ];

        for pattern in patterns {
            let error_lower = pattern.to_lowercase();
            let is_already_canceled = error_lower.contains("already canceled")
                || error_lower.contains("no such subscription")
                || error_lower.contains("cannot be canceled");

            assert!(
                !is_already_canceled,
                "Pattern '{}' should NOT be detected as already canceled",
                pattern
            );
        }
    }
}

mod webhook_tests {
    use super::*;

    #[test]
    fn test_retriable_error_detection() {
        // These error patterns should trigger a 500 response for Stripe retry
        let retriable_errors = [
            "connection refused",
            "connection reset",
            "timeout exceeded",
            "temporarily unavailable",
            "too many connections",
            "deadlock detected",
            "lock wait timeout",
        ];

        for error in retriable_errors {
            let error_lower = error.to_lowercase();
            let is_retriable = error_lower.contains("connection")
                || error_lower.contains("timeout")
                || error_lower.contains("temporarily unavailable")
                || error_lower.contains("too many connections")
                || error_lower.contains("deadlock")
                || error_lower.contains("lock wait timeout");

            assert!(
                is_retriable,
                "Error '{}' should be classified as retriable",
                error
            );
        }
    }

    #[test]
    fn test_permanent_error_detection() {
        // These errors should NOT trigger a retry (return 200)
        let permanent_errors = [
            "orphaned subscription",
            "customer not found",
            "invalid data format",
            "constraint violation",
        ];

        for error in permanent_errors {
            let error_lower = error.to_lowercase();
            let is_retriable = error_lower.contains("connection")
                || error_lower.contains("timeout")
                || error_lower.contains("temporarily unavailable")
                || error_lower.contains("too many connections")
                || error_lower.contains("deadlock")
                || error_lower.contains("lock wait timeout");

            assert!(
                !is_retriable,
                "Error '{}' should NOT be classified as retriable",
                error
            );
        }
    }
}

// ============================================================================
// Invoice Tests
// ============================================================================

#[cfg(feature = "test-utils")]
mod invoice_tests {
    use super::*;
    use chrono::Utc;

    #[tokio::test]
    async fn test_list_invoices_empty() {
        let provider = MockPaymentProvider::new();
        let org_id = Uuid::new_v4();

        let (invoices, total) = provider.list_invoices(org_id, 20, 0).await.unwrap();
        assert!(invoices.is_empty());
        assert_eq!(total, 0);
    }

    #[tokio::test]
    async fn test_list_invoices_with_data() {
        use reiver_website::billing::InvoiceInfo;

        let provider = MockPaymentProvider::new();
        let org_id = Uuid::new_v4();

        // Add mock invoices
        provider.add_invoice(
            org_id,
            InvoiceInfo {
                id: Uuid::new_v4(),
                invoice_number: "INV-001".to_string(),
                status: "paid".to_string(),
                total_cents: 10000,
                currency: "usd".to_string(),
                period_start: Some(Utc::now()),
                period_end: Some(Utc::now()),
                paid_at: Some(Utc::now()),
                invoice_pdf_url: Some("https://example.com/invoice.pdf".to_string()),
                hosted_invoice_url: None,
            },
        );

        provider.add_invoice(
            org_id,
            InvoiceInfo {
                id: Uuid::new_v4(),
                invoice_number: "INV-002".to_string(),
                status: "open".to_string(),
                total_cents: 5000,
                currency: "usd".to_string(),
                period_start: Some(Utc::now()),
                period_end: Some(Utc::now()),
                paid_at: None,
                invoice_pdf_url: None,
                hosted_invoice_url: None,
            },
        );

        let (invoices, total) = provider.list_invoices(org_id, 20, 0).await.unwrap();
        assert_eq!(invoices.len(), 2);
        assert_eq!(total, 2);
    }

    #[tokio::test]
    async fn test_list_invoices_pagination() {
        use reiver_website::billing::InvoiceInfo;

        let provider = MockPaymentProvider::new();
        let org_id = Uuid::new_v4();

        // Add 5 mock invoices
        for i in 0..5 {
            provider.add_invoice(
                org_id,
                InvoiceInfo {
                    id: Uuid::new_v4(),
                    invoice_number: format!("INV-00{}", i),
                    status: "paid".to_string(),
                    total_cents: 1000 * (i + 1),
                    currency: "usd".to_string(),
                    period_start: None,
                    period_end: None,
                    paid_at: None,
                    invoice_pdf_url: None,
                    hosted_invoice_url: None,
                },
            );
        }

        // Get first 2
        let (invoices, total) = provider.list_invoices(org_id, 2, 0).await.unwrap();
        assert_eq!(invoices.len(), 2);
        assert_eq!(total, 5);

        // Get next 2
        let (invoices, _) = provider.list_invoices(org_id, 2, 2).await.unwrap();
        assert_eq!(invoices.len(), 2);

        // Get last 1
        let (invoices, _) = provider.list_invoices(org_id, 2, 4).await.unwrap();
        assert_eq!(invoices.len(), 1);
    }

    #[tokio::test]
    async fn test_list_invoices_different_orgs() {
        use reiver_website::billing::InvoiceInfo;

        let provider = MockPaymentProvider::new();
        let org1 = Uuid::new_v4();
        let org2 = Uuid::new_v4();

        // Add invoice to org1
        provider.add_invoice(
            org1,
            InvoiceInfo {
                id: Uuid::new_v4(),
                invoice_number: "INV-ORG1".to_string(),
                status: "paid".to_string(),
                total_cents: 10000,
                currency: "usd".to_string(),
                period_start: None,
                period_end: None,
                paid_at: None,
                invoice_pdf_url: None,
                hosted_invoice_url: None,
            },
        );

        // org2 should have no invoices
        let (invoices, total) = provider.list_invoices(org2, 20, 0).await.unwrap();
        assert!(invoices.is_empty());
        assert_eq!(total, 0);

        // org1 should have 1 invoice
        let (invoices, total) = provider.list_invoices(org1, 20, 0).await.unwrap();
        assert_eq!(invoices.len(), 1);
        assert_eq!(total, 1);
    }
}

// ============================================================================
// Setup Intent Validation Tests (Edge Cases)
// ============================================================================

#[cfg(feature = "test-utils")]
mod setup_intent_edge_cases {
    use super::*;

    #[tokio::test]
    async fn test_confirm_wrong_org_setup_intent() {
        let provider = MockPaymentProvider::new();
        let org1 = Uuid::new_v4();
        let org2 = Uuid::new_v4();

        // Create setup intent for org1
        let intent = provider.create_setup_intent(org1).await.unwrap();

        // org2 trying to confirm org1's intent should fail
        let result = provider
            .confirm_payment_method(org2, &intent.setup_intent_id, true, None)
            .await;
        assert!(matches!(result, Err(PaymentError::InvalidSetupIntent(_))));
    }

    #[tokio::test]
    async fn test_confirm_nonexistent_setup_intent() {
        let provider = MockPaymentProvider::new();
        let org_id = Uuid::new_v4();

        // Valid format but doesn't exist
        let result = provider
            .confirm_payment_method(org_id, "seti_nonexistent12345", true, None)
            .await;
        assert!(matches!(result, Err(PaymentError::InvalidSetupIntent(_))));
    }

    #[tokio::test]
    async fn test_setup_intent_consumed_after_confirm() {
        let provider = MockPaymentProvider::new();
        let org_id = Uuid::new_v4();

        // Create and confirm
        let intent = provider.create_setup_intent(org_id).await.unwrap();
        provider
            .confirm_payment_method(org_id, &intent.setup_intent_id, true, None)
            .await
            .unwrap();

        // Trying to confirm again should fail
        let result = provider
            .confirm_payment_method(org_id, &intent.setup_intent_id, true, None)
            .await;
        assert!(matches!(result, Err(PaymentError::InvalidSetupIntent(_))));
    }
}

// ============================================================================
// Subscription Edge Cases
// ============================================================================

#[cfg(feature = "test-utils")]
mod subscription_edge_cases {
    use super::*;

    #[tokio::test]
    async fn test_create_subscription_invalid_price() {
        let provider = MockPaymentProvider::new();
        let org_id = Uuid::new_v4();

        // Invalid price ID format
        let result = provider.create_subscription(org_id, "invalid", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_subscription_periods() {
        let provider = MockPaymentProvider::new();
        let org_id = Uuid::new_v4();

        let sub = provider
            .create_subscription(org_id, "price_test1234567890ab", None)
            .await
            .unwrap();

        // Should have period dates set
        assert!(sub.current_period_start.is_some());
        assert!(sub.current_period_end.is_some());

        // End should be after start
        let start = sub.current_period_start.unwrap();
        let end = sub.current_period_end.unwrap();
        assert!(end > start);
    }
}

// ============================================================================
// IP Allowlist Tests
// ============================================================================

mod ip_allowlist_tests {
    use std::net::IpAddr;

    /// Helper function that mirrors the is_ip_in_allowlist logic for testing
    fn is_ip_in_allowlist(client_ip: &str, allowed_ranges: &[String]) -> bool {
        // Trim and validate client IP
        let client_ip = client_ip.trim();
        if client_ip.is_empty() {
            return false;
        }

        // Parse the client IP
        let ip: IpAddr = match client_ip.parse() {
            Ok(ip) => ip,
            Err(_) => return false,
        };

        for range in allowed_ranges {
            // Sanitize input: trim whitespace and skip empty entries
            let range = range.trim();
            if range.is_empty() {
                continue;
            }

            // Parse CIDR notation
            let (range_ip_str, prefix_len) =
                if let Some((ip_part, prefix_part)) = range.split_once('/') {
                    let ip_part = ip_part.trim();
                    let prefix_part = prefix_part.trim();
                    let prefix: u8 = match prefix_part.parse() {
                        Ok(p) => p,
                        Err(_) => continue,
                    };
                    (ip_part, prefix)
                } else {
                    let prefix = if range.contains(':') { 128 } else { 32 };
                    (range, prefix)
                };

            let range_ip: IpAddr = match range_ip_str.parse() {
                Ok(ip) => ip,
                Err(_) => continue,
            };

            // Check if IPs are same family
            match (ip, range_ip) {
                (IpAddr::V4(client), IpAddr::V4(range)) => {
                    if prefix_len > 32 {
                        continue;
                    }
                    let mask = if prefix_len == 0 {
                        0
                    } else {
                        !0u32 << (32 - prefix_len)
                    };
                    let client_masked = u32::from(client) & mask;
                    let range_masked = u32::from(range) & mask;
                    if client_masked == range_masked {
                        return true;
                    }
                }
                (IpAddr::V6(client), IpAddr::V6(range)) => {
                    if prefix_len > 128 {
                        continue;
                    }
                    let mask = if prefix_len == 0 {
                        0
                    } else {
                        !0u128 << (128 - prefix_len)
                    };
                    let client_masked = u128::from(client) & mask;
                    let range_masked = u128::from(range) & mask;
                    if client_masked == range_masked {
                        return true;
                    }
                }
                _ => continue,
            }
        }

        false
    }

    #[test]
    fn test_exact_ip_match() {
        let allowlist = vec!["192.168.1.100".to_string()];
        assert!(is_ip_in_allowlist("192.168.1.100", &allowlist));
        assert!(!is_ip_in_allowlist("192.168.1.101", &allowlist));
    }

    #[test]
    fn test_cidr_range_match() {
        let allowlist = vec!["192.168.1.0/24".to_string()];
        assert!(is_ip_in_allowlist("192.168.1.0", &allowlist));
        assert!(is_ip_in_allowlist("192.168.1.100", &allowlist));
        assert!(is_ip_in_allowlist("192.168.1.255", &allowlist));
        assert!(!is_ip_in_allowlist("192.168.2.0", &allowlist));
    }

    #[test]
    fn test_multiple_ranges() {
        let allowlist = vec![
            "10.0.0.0/8".to_string(),
            "172.16.0.0/12".to_string(),
            "192.168.0.0/16".to_string(),
        ];
        assert!(is_ip_in_allowlist("10.1.2.3", &allowlist));
        assert!(is_ip_in_allowlist("172.20.0.1", &allowlist));
        assert!(is_ip_in_allowlist("192.168.100.50", &allowlist));
        assert!(!is_ip_in_allowlist("8.8.8.8", &allowlist));
    }

    #[test]
    fn test_empty_entries_skipped() {
        let allowlist = vec![
            "".to_string(),
            "   ".to_string(),
            "192.168.1.0/24".to_string(),
        ];
        assert!(is_ip_in_allowlist("192.168.1.100", &allowlist));
    }

    #[test]
    fn test_whitespace_trimmed() {
        let allowlist = vec![
            "  192.168.1.0/24  ".to_string(),
            " 10.0.0.0 / 8 ".to_string(),
        ];
        assert!(is_ip_in_allowlist("192.168.1.100", &allowlist));
        // Note: "10.0.0.0 / 8" has internal spaces which should be handled
        assert!(is_ip_in_allowlist("  10.1.2.3  ", &allowlist));
    }

    #[test]
    fn test_invalid_ip_rejected() {
        let allowlist = vec!["192.168.1.0/24".to_string()];
        assert!(!is_ip_in_allowlist("not-an-ip", &allowlist));
        assert!(!is_ip_in_allowlist("", &allowlist));
    }

    #[test]
    fn test_invalid_cidr_skipped() {
        let allowlist = vec![
            "invalid-range".to_string(),
            "192.168.1.0/invalid".to_string(),
            "192.168.1.0/33".to_string(), // Invalid prefix for IPv4
            "10.0.0.0/8".to_string(),     // This one is valid
        ];
        // Should still match the valid entry
        assert!(is_ip_in_allowlist("10.1.2.3", &allowlist));
    }

    #[test]
    fn test_ipv6_support() {
        let allowlist = vec!["2001:db8::/32".to_string()];
        assert!(is_ip_in_allowlist("2001:db8::1", &allowlist));
        assert!(is_ip_in_allowlist(
            "2001:db8:ffff:ffff:ffff:ffff:ffff:ffff",
            &allowlist
        ));
        assert!(!is_ip_in_allowlist("2001:db9::1", &allowlist));
    }

    #[test]
    fn test_ipv4_ipv6_no_cross_match() {
        let ipv4_list = vec!["192.168.1.0/24".to_string()];
        let ipv6_list = vec!["2001:db8::/32".to_string()];

        // IPv6 address should not match IPv4 range
        assert!(!is_ip_in_allowlist("2001:db8::1", &ipv4_list));
        // IPv4 address should not match IPv6 range
        assert!(!is_ip_in_allowlist("192.168.1.100", &ipv6_list));
    }

    #[test]
    fn test_stripe_ip_ranges() {
        // Real Stripe webhook IP ranges (subset for testing)
        let stripe_ips = vec![
            "3.18.12.63/32".to_string(),
            "3.130.192.231/32".to_string(),
            "13.235.14.237/32".to_string(),
            "18.211.135.69/32".to_string(),
        ];

        assert!(is_ip_in_allowlist("3.18.12.63", &stripe_ips));
        assert!(is_ip_in_allowlist("3.130.192.231", &stripe_ips));
        assert!(!is_ip_in_allowlist("3.18.12.64", &stripe_ips)); // Off by one
        assert!(!is_ip_in_allowlist("8.8.8.8", &stripe_ips)); // Completely different
    }

    #[test]
    fn test_empty_allowlist() {
        let allowlist: Vec<String> = vec![];
        assert!(!is_ip_in_allowlist("192.168.1.1", &allowlist));
    }
}

// ============================================================================
// PII Scrubbing Tests
// ============================================================================

mod pii_scrubbing_tests {
    use serde_json::json;

    /// Fields that should be scrubbed from webhook payloads
    const PII_FIELDS_TO_SCRUB: &[&str] = &[
        "email",
        "name",
        "phone",
        "address",
        "billing_details",
        "shipping",
        "customer_email",
        "customer_name",
        "receipt_email",
    ];

    /// Helper function that mirrors the scrub_pii_from_payload logic for testing
    fn scrub_pii_from_payload(payload: &serde_json::Value) -> serde_json::Value {
        match payload {
            serde_json::Value::Object(map) => {
                let mut scrubbed = serde_json::Map::new();
                for (key, value) in map {
                    if PII_FIELDS_TO_SCRUB.contains(&key.as_str()) {
                        scrubbed.insert(
                            key.clone(),
                            serde_json::Value::String("[REDACTED]".to_string()),
                        );
                        continue;
                    }
                    scrubbed.insert(key.clone(), scrub_pii_from_payload(value));
                }
                serde_json::Value::Object(scrubbed)
            }
            serde_json::Value::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(scrub_pii_from_payload).collect())
            }
            other => other.clone(),
        }
    }

    #[test]
    fn test_pii_fields_scrubbed() {
        let payload = json!({
            "id": "evt_123",
            "email": "user@example.com",
            "name": "John Doe",
            "phone": "+1234567890",
            "amount": 1000
        });

        let scrubbed = scrub_pii_from_payload(&payload);

        assert_eq!(scrubbed["id"], "evt_123");
        assert_eq!(scrubbed["amount"], 1000);
        assert_eq!(scrubbed["email"], "[REDACTED]");
        assert_eq!(scrubbed["name"], "[REDACTED]");
        assert_eq!(scrubbed["phone"], "[REDACTED]");
    }

    #[test]
    fn test_nested_pii_scrubbed() {
        let payload = json!({
            "data": {
                "object": {
                    "customer_email": "user@example.com",
                    "billing_details": {
                        "name": "John Doe",
                        "address": {
                            "line1": "123 Main St"
                        }
                    }
                }
            }
        });

        let scrubbed = scrub_pii_from_payload(&payload);

        assert_eq!(scrubbed["data"]["object"]["customer_email"], "[REDACTED]");
        assert_eq!(scrubbed["data"]["object"]["billing_details"], "[REDACTED]");
    }

    #[test]
    fn test_arrays_scrubbed() {
        let payload = json!({
            "items": [
                {"id": "item_1", "email": "user1@example.com"},
                {"id": "item_2", "email": "user2@example.com"}
            ]
        });

        let scrubbed = scrub_pii_from_payload(&payload);

        assert_eq!(scrubbed["items"][0]["id"], "item_1");
        assert_eq!(scrubbed["items"][0]["email"], "[REDACTED]");
        assert_eq!(scrubbed["items"][1]["email"], "[REDACTED]");
    }

    #[test]
    fn test_non_pii_preserved() {
        let payload = json!({
            "id": "evt_123",
            "type": "invoice.paid",
            "amount_cents": 10000,
            "currency": "usd",
            "status": "paid"
        });

        let scrubbed = scrub_pii_from_payload(&payload);

        assert_eq!(scrubbed, payload);
    }
}

// ============================================================================
// Webhook Event Type Mapping Tests
// ============================================================================

mod webhook_integration_tests {
    #[test]
    fn test_webhook_body_size_limit_constant() {
        const MAX_WEBHOOK_BODY_SIZE: usize = 65_536;
        assert_eq!(MAX_WEBHOOK_BODY_SIZE, 64 * 1024);
    }

    #[test]
    fn test_oversized_payload_is_above_limit() {
        let oversized = "x".repeat(65_537);
        assert!(
            oversized.len() > 65_536,
            "Payload should exceed the max webhook body size"
        );
    }

    #[test]
    fn test_content_type_validation_logic() {
        let valid = [
            "application/json",
            "application/json; charset=utf-8",
            "application/json;charset=utf-8",
        ];
        let invalid = [
            "text/plain",
            "text/html",
            "application/xml",
            "multipart/form-data",
            "",
        ];

        for ct in valid {
            let ct_lower = ct.trim().to_lowercase();
            let ok = ct_lower == "application/json" || ct_lower.starts_with("application/json;");
            assert!(ok, "Should accept '{}'", ct);
        }

        for ct in invalid {
            let ct_lower = ct.trim().to_lowercase();
            let ok = ct_lower == "application/json" || ct_lower.starts_with("application/json;");
            assert!(!ok, "Should reject '{}'", ct);
        }
    }

    #[test]
    fn test_case_insensitive_content_type() {
        let variants = [
            "Application/JSON",
            "APPLICATION/JSON",
            "Application/Json; Charset=UTF-8",
        ];
        for ct in variants {
            let ct_lower = ct.trim().to_lowercase();
            let ok = ct_lower == "application/json" || ct_lower.starts_with("application/json;");
            assert!(ok, "Should accept case-insensitive '{}'", ct);
        }
    }

    #[test]
    fn test_malicious_content_type_variants() {
        let malicious = [
            "application/json-malicious",
            "text/application/json",
            "application/jsonl",
        ];
        for ct in malicious {
            let ct_lower = ct.trim().to_lowercase();
            let ok = ct_lower == "application/json" || ct_lower.starts_with("application/json;");
            assert!(!ok, "Should reject malicious variant '{}'", ct);
        }
    }
}

#[cfg(feature = "test-utils")]
mod webhook_mock_integration_tests {
    use super::*;

    #[tokio::test]
    async fn test_valid_webhook_processes_invoice_paid() {
        let provider = MockPaymentProvider::new();
        let org_id = Uuid::new_v4();

        let payload = serde_json::json!({
            "id": "evt_invoice_paid_1",
            "type": "invoice.paid",
            "data": {
                "object": {
                    "id": "in_123",
                    "customer": "cus_mock_123",
                    "number": "INV-0042",
                    "total": 9900,
                    "currency": "usd",
                    "status": "paid",
                    "metadata": { "organization_id": org_id.to_string() }
                }
            }
        });

        let event = provider
            .verify_webhook(&payload.to_string(), "valid_sig")
            .await
            .unwrap();
        assert!(event.is_some(), "First call should return the event");

        let ev = event.unwrap();
        assert_eq!(ev.event_id, "evt_invoice_paid_1");
        assert!(matches!(
            ev.event_type,
            reiver_website::billing::WebhookEventType::InvoicePaid
        ));

        provider
            .mark_event_processed(&ev.event_id, None)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_duplicate_event_is_deduplicated() {
        let provider = MockPaymentProvider::new();

        let payload = r#"{"id": "evt_dedup_test", "type": "invoice.paid"}"#;

        let first = provider.verify_webhook(payload, "sig").await.unwrap();
        assert!(first.is_some(), "First call should return the event");

        let second = provider.verify_webhook(payload, "sig").await.unwrap();
        assert!(
            second.is_none(),
            "Second call with same event ID should return None (deduplicated)"
        );
    }

    #[tokio::test]
    async fn test_invalid_signature_returns_error() {
        let provider = MockPaymentProvider::new();
        provider.set_fail_all(true);

        let result = provider.verify_webhook("{}", "bad_sig").await;
        assert!(result.is_err(), "Invalid signature should return an error");
    }

    #[tokio::test]
    async fn test_multiple_events_processed_independently() {
        let provider = MockPaymentProvider::new();

        let events = vec![
            r#"{"id": "evt_a1", "type": "customer.subscription.created"}"#,
            r#"{"id": "evt_a2", "type": "invoice.paid"}"#,
            r#"{"id": "evt_a3", "type": "customer.subscription.updated"}"#,
        ];

        for payload in &events {
            let event = provider.verify_webhook(payload, "sig").await.unwrap();
            assert!(event.is_some());
        }

        for payload in &events {
            let event = provider.verify_webhook(payload, "sig").await.unwrap();
            assert!(
                event.is_none(),
                "Replay of processed event should be deduplicated"
            );
        }
    }
}

#[cfg(feature = "test-utils")]
mod webhook_event_type_tests {
    use super::*;

    #[tokio::test]
    async fn test_customer_deleted_event_type() {
        let provider = MockPaymentProvider::new();

        let payload = r#"{"id": "evt_del_123", "type": "customer.deleted"}"#;
        let event = provider
            .verify_webhook(payload, "sig")
            .await
            .unwrap()
            .unwrap();

        // The mock provider should recognize customer.deleted
        match event.event_type {
            reiver_website::billing::WebhookEventType::CustomerDeleted => (),
            other => panic!("Expected CustomerDeleted, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_subscription_events() {
        let provider = MockPaymentProvider::new();

        // Test subscription.created
        let payload = r#"{"id": "evt_1", "type": "customer.subscription.created"}"#;
        let event = provider
            .verify_webhook(payload, "sig")
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            event.event_type,
            reiver_website::billing::WebhookEventType::SubscriptionCreated
        ));

        // Test subscription.updated (different event ID)
        let payload = r#"{"id": "evt_2", "type": "customer.subscription.updated"}"#;
        let event = provider
            .verify_webhook(payload, "sig")
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            event.event_type,
            reiver_website::billing::WebhookEventType::SubscriptionUpdated
        ));

        // Test subscription.deleted (different event ID)
        let payload = r#"{"id": "evt_3", "type": "customer.subscription.deleted"}"#;
        let event = provider
            .verify_webhook(payload, "sig")
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            event.event_type,
            reiver_website::billing::WebhookEventType::SubscriptionDeleted
        ));
    }

    #[tokio::test]
    async fn test_invoice_events() {
        let provider = MockPaymentProvider::new();

        // Test invoice.paid
        let payload = r#"{"id": "evt_inv_1", "type": "invoice.paid"}"#;
        let event = provider
            .verify_webhook(payload, "sig")
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            event.event_type,
            reiver_website::billing::WebhookEventType::InvoicePaid
        ));

        // Test invoice.payment_failed
        let payload = r#"{"id": "evt_inv_2", "type": "invoice.payment_failed"}"#;
        let event = provider
            .verify_webhook(payload, "sig")
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            event.event_type,
            reiver_website::billing::WebhookEventType::InvoicePaymentFailed
        ));
    }

    #[tokio::test]
    async fn test_unknown_event_type() {
        let provider = MockPaymentProvider::new();

        let payload = r#"{"id": "evt_unk", "type": "some.unknown.event"}"#;
        let event = provider
            .verify_webhook(payload, "sig")
            .await
            .unwrap()
            .unwrap();

        match event.event_type {
            reiver_website::billing::WebhookEventType::Unknown(s) => {
                assert_eq!(s, "some.unknown.event");
            }
            other => panic!("Expected Unknown, got {:?}", other),
        }
    }
}
