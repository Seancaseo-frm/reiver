//! Stripe Schema Discovery
//!
//! Discovers schema information from Stripe API metadata.
//! Since Stripe has a well-defined API with consistent object schemas,
//! we provide hardcoded schema definitions that match the Stripe API.

use arrow::datatypes::DataType;
use async_trait::async_trait;
use tracing::{debug, info, instrument};

use super::{DiscoveryResult, SchemaDiscovery};
use crate::warehouse::sources::types::RegisteredSource;
use crate::warehouse::types::{SemanticType, TypedColumn, TypedSchema};

// ============================================================================
// Stripe Table Definitions
// ============================================================================

/// Stripe object types we support with their schemas.
#[derive(Debug, Clone, Copy)]
pub enum StripeObject {
    Customers,
    Charges,
    PaymentIntents,
    Subscriptions,
    Invoices,
    InvoiceItems,
    Products,
    Prices,
    Coupons,
    BalanceTransactions,
    Payouts,
    Refunds,
    Disputes,
    Events,
}

impl StripeObject {
    /// Get all supported Stripe objects.
    pub fn all() -> Vec<Self> {
        vec![
            StripeObject::Customers,
            StripeObject::Charges,
            StripeObject::PaymentIntents,
            StripeObject::Subscriptions,
            StripeObject::Invoices,
            StripeObject::InvoiceItems,
            StripeObject::Products,
            StripeObject::Prices,
            StripeObject::Coupons,
            StripeObject::BalanceTransactions,
            StripeObject::Payouts,
            StripeObject::Refunds,
            StripeObject::Disputes,
            StripeObject::Events,
        ]
    }

    /// Get the table name for this object.
    pub fn table_name(&self) -> &'static str {
        match self {
            StripeObject::Customers => "customers",
            StripeObject::Charges => "charges",
            StripeObject::PaymentIntents => "payment_intents",
            StripeObject::Subscriptions => "subscriptions",
            StripeObject::Invoices => "invoices",
            StripeObject::InvoiceItems => "invoice_items",
            StripeObject::Products => "products",
            StripeObject::Prices => "prices",
            StripeObject::Coupons => "coupons",
            StripeObject::BalanceTransactions => "balance_transactions",
            StripeObject::Payouts => "payouts",
            StripeObject::Refunds => "refunds",
            StripeObject::Disputes => "disputes",
            StripeObject::Events => "events",
        }
    }

    /// Get the description for this object.
    pub fn description(&self) -> &'static str {
        match self {
            StripeObject::Customers => "Customer objects allow you to perform recurring charges",
            StripeObject::Charges => "Charge objects represent a single charge attempt",
            StripeObject::PaymentIntents => "PaymentIntent guides you through the payment process",
            StripeObject::Subscriptions => "Subscriptions allow recurring billing",
            StripeObject::Invoices => "Invoices are statements of amounts owed",
            StripeObject::InvoiceItems => "Invoice items added to an invoice before finalization",
            StripeObject::Products => "Products describe the specific goods or services",
            StripeObject::Prices => "Prices define the unit cost, currency, and billing cycle",
            StripeObject::Coupons => {
                "Coupons contain information about a percent-off or amount-off discount"
            }
            StripeObject::BalanceTransactions => "Balance transaction history",
            StripeObject::Payouts => "Payout objects represent funds moving from Stripe to bank",
            StripeObject::Refunds => "Refund objects represent reversed payments",
            StripeObject::Disputes => "Dispute objects represent a cardholder disputing a charge",
            StripeObject::Events => "Event objects represent Stripe webhook events",
        }
    }
}

// ============================================================================
// Schema Builders
// ============================================================================

/// Helper to create a money column (amounts in cents).
fn money_column(name: &str, desc: &str, source_name: &str) -> TypedColumn {
    TypedColumn::new(name, &DataType::Int64, true, "integer (cents)", source_name)
        .with_semantic(SemanticType::Money {
            currency: None,
            in_cents: true,
        })
        .with_description(desc)
}

/// Helper to create a timestamp column (Unix epoch).
fn timestamp_column(name: &str, desc: &str, source_name: &str) -> TypedColumn {
    TypedColumn::new(name, &DataType::Int64, true, "integer (epoch)", source_name)
        .with_semantic(SemanticType::Timestamp {
            precision: crate::warehouse::types::TimestampPrecision::Seconds,
            source_timezone: "UTC".to_string(),
        })
        .with_description(desc)
}

/// Helper to create a string column.
fn string_column(name: &str, desc: &str, nullable: bool, source_name: &str) -> TypedColumn {
    TypedColumn::new(name, &DataType::Utf8, nullable, "string", source_name).with_description(desc)
}

/// Helper to create an ID column.
fn id_column(name: &str, desc: &str, source_name: &str) -> TypedColumn {
    TypedColumn::new(name, &DataType::Utf8, false, "string (id)", source_name)
        .with_semantic(SemanticType::Identifier)
        .with_description(desc)
}

/// Helper to create a boolean column.
fn bool_column(name: &str, desc: &str, nullable: bool, source_name: &str) -> TypedColumn {
    TypedColumn::new(name, &DataType::Boolean, nullable, "boolean", source_name)
        .with_description(desc)
}

/// Helper to create a JSON column.
fn json_column(name: &str, desc: &str, source_name: &str) -> TypedColumn {
    TypedColumn::new(name, &DataType::Utf8, true, "json", source_name).with_description(desc)
}

/// Build schema for customers.
fn customers_schema(source_name: &str) -> TypedSchema {
    TypedSchema::new("customers", source_name)
        .with_column(id_column(
            "id",
            "Unique identifier for the customer",
            source_name,
        ))
        .with_column(string_column(
            "email",
            "Customer's email address",
            true,
            source_name,
        ))
        .with_column(string_column(
            "name",
            "Customer's full name",
            true,
            source_name,
        ))
        .with_column(string_column(
            "description",
            "Description of the customer",
            true,
            source_name,
        ))
        .with_column(string_column(
            "phone",
            "Customer's phone number",
            true,
            source_name,
        ))
        .with_column(timestamp_column(
            "created",
            "Time at which the customer was created",
            source_name,
        ))
        .with_column(string_column(
            "currency",
            "Three-letter ISO currency code",
            true,
            source_name,
        ))
        .with_column(string_column(
            "default_source",
            "ID of default payment source",
            true,
            source_name,
        ))
        .with_column(bool_column(
            "delinquent",
            "Whether customer has unpaid invoices",
            true,
            source_name,
        ))
        .with_column(bool_column(
            "livemode",
            "Has the value true if in live mode",
            false,
            source_name,
        ))
        .with_column(json_column(
            "metadata",
            "Set of key-value pairs",
            source_name,
        ))
        .with_column(json_column("address", "Customer's address", source_name))
        .with_column(json_column("shipping", "Shipping information", source_name))
        .with_column(money_column(
            "balance",
            "Customer's account balance in cents",
            source_name,
        ))
}

/// Build schema for charges.
fn charges_schema(source_name: &str) -> TypedSchema {
    TypedSchema::new("charges", source_name)
        .with_column(id_column(
            "id",
            "Unique identifier for the charge",
            source_name,
        ))
        .with_column(money_column(
            "amount",
            "Amount charged in cents",
            source_name,
        ))
        .with_column(money_column(
            "amount_captured",
            "Amount captured",
            source_name,
        ))
        .with_column(money_column(
            "amount_refunded",
            "Amount refunded",
            source_name,
        ))
        .with_column(string_column(
            "currency",
            "Three-letter ISO currency code",
            false,
            source_name,
        ))
        .with_column(string_column(
            "customer",
            "ID of the customer",
            true,
            source_name,
        ))
        .with_column(string_column(
            "description",
            "Description of the charge",
            true,
            source_name,
        ))
        .with_column(bool_column(
            "captured",
            "Whether the charge was captured",
            false,
            source_name,
        ))
        .with_column(bool_column(
            "paid",
            "Whether the charge succeeded",
            false,
            source_name,
        ))
        .with_column(bool_column(
            "refunded",
            "Whether the charge has been refunded",
            false,
            source_name,
        ))
        .with_column(bool_column(
            "disputed",
            "Whether the charge has been disputed",
            false,
            source_name,
        ))
        .with_column(string_column(
            "status",
            "Status of the charge",
            false,
            source_name,
        ))
        .with_column(string_column(
            "failure_code",
            "Error code for failed charges",
            true,
            source_name,
        ))
        .with_column(string_column(
            "failure_message",
            "Failure message",
            true,
            source_name,
        ))
        .with_column(string_column(
            "payment_intent",
            "ID of PaymentIntent",
            true,
            source_name,
        ))
        .with_column(string_column(
            "payment_method",
            "ID of payment method",
            true,
            source_name,
        ))
        .with_column(string_column(
            "receipt_email",
            "Email to send receipt",
            true,
            source_name,
        ))
        .with_column(string_column(
            "receipt_url",
            "URL for charge receipt",
            true,
            source_name,
        ))
        .with_column(timestamp_column(
            "created",
            "Time at which the charge was created",
            source_name,
        ))
        .with_column(bool_column(
            "livemode",
            "Has the value true if in live mode",
            false,
            source_name,
        ))
        .with_column(json_column(
            "metadata",
            "Set of key-value pairs",
            source_name,
        ))
        .with_column(json_column(
            "billing_details",
            "Billing information",
            source_name,
        ))
        .with_column(json_column(
            "outcome",
            "Details about charge outcome",
            source_name,
        ))
}

/// Build schema for payment intents.
fn payment_intents_schema(source_name: &str) -> TypedSchema {
    TypedSchema::new("payment_intents", source_name)
        .with_column(id_column(
            "id",
            "Unique identifier for the PaymentIntent",
            source_name,
        ))
        .with_column(money_column(
            "amount",
            "Amount intended to collect",
            source_name,
        ))
        .with_column(money_column(
            "amount_capturable",
            "Amount that can be captured",
            source_name,
        ))
        .with_column(money_column(
            "amount_received",
            "Amount that was collected",
            source_name,
        ))
        .with_column(string_column(
            "currency",
            "Three-letter ISO currency code",
            false,
            source_name,
        ))
        .with_column(string_column(
            "customer",
            "ID of the customer",
            true,
            source_name,
        ))
        .with_column(string_column(
            "description",
            "Description",
            true,
            source_name,
        ))
        .with_column(string_column(
            "status",
            "Status of the PaymentIntent",
            false,
            source_name,
        ))
        .with_column(string_column(
            "capture_method",
            "Method of capture",
            true,
            source_name,
        ))
        .with_column(string_column(
            "confirmation_method",
            "Confirmation method",
            true,
            source_name,
        ))
        .with_column(string_column(
            "latest_charge",
            "ID of latest charge",
            true,
            source_name,
        ))
        .with_column(string_column(
            "payment_method",
            "ID of payment method",
            true,
            source_name,
        ))
        .with_column(string_column(
            "client_secret",
            "Client secret for client-side confirmation",
            true,
            source_name,
        ))
        .with_column(bool_column(
            "livemode",
            "Has the value true if in live mode",
            false,
            source_name,
        ))
        .with_column(timestamp_column(
            "created",
            "Time at which the PaymentIntent was created",
            source_name,
        ))
        .with_column(timestamp_column(
            "canceled_at",
            "Time at which the PaymentIntent was canceled",
            source_name,
        ))
        .with_column(json_column(
            "metadata",
            "Set of key-value pairs",
            source_name,
        ))
        .with_column(json_column(
            "payment_method_options",
            "Payment method specific options",
            source_name,
        ))
        .with_column(json_column(
            "last_payment_error",
            "Error on last payment attempt",
            source_name,
        ))
}

/// Build schema for subscriptions.
fn subscriptions_schema(source_name: &str) -> TypedSchema {
    TypedSchema::new("subscriptions", source_name)
        .with_column(id_column(
            "id",
            "Unique identifier for the subscription",
            source_name,
        ))
        .with_column(string_column(
            "customer",
            "ID of the customer",
            false,
            source_name,
        ))
        .with_column(string_column(
            "status",
            "Subscription status",
            false,
            source_name,
        ))
        .with_column(string_column(
            "currency",
            "Three-letter ISO currency code",
            true,
            source_name,
        ))
        .with_column(string_column(
            "default_payment_method",
            "Default payment method",
            true,
            source_name,
        ))
        .with_column(string_column(
            "latest_invoice",
            "Latest invoice ID",
            true,
            source_name,
        ))
        .with_column(bool_column(
            "cancel_at_period_end",
            "Cancel at period end",
            false,
            source_name,
        ))
        .with_column(bool_column(
            "livemode",
            "Has the value true if in live mode",
            false,
            source_name,
        ))
        .with_column(timestamp_column(
            "created",
            "Time at which subscription was created",
            source_name,
        ))
        .with_column(timestamp_column(
            "current_period_start",
            "Start of current period",
            source_name,
        ))
        .with_column(timestamp_column(
            "current_period_end",
            "End of current period",
            source_name,
        ))
        .with_column(timestamp_column(
            "canceled_at",
            "When subscription was canceled",
            source_name,
        ))
        .with_column(timestamp_column(
            "ended_at",
            "When subscription ended",
            source_name,
        ))
        .with_column(timestamp_column(
            "trial_start",
            "Start of trial period",
            source_name,
        ))
        .with_column(timestamp_column(
            "trial_end",
            "End of trial period",
            source_name,
        ))
        .with_column(json_column("items", "Subscription items", source_name))
        .with_column(json_column(
            "metadata",
            "Set of key-value pairs",
            source_name,
        ))
}

/// Build schema for invoices.
fn invoices_schema(source_name: &str) -> TypedSchema {
    TypedSchema::new("invoices", source_name)
        .with_column(id_column(
            "id",
            "Unique identifier for the invoice",
            source_name,
        ))
        .with_column(string_column("number", "Invoice number", true, source_name))
        .with_column(string_column(
            "customer",
            "ID of the customer",
            true,
            source_name,
        ))
        .with_column(string_column(
            "subscription",
            "ID of the subscription",
            true,
            source_name,
        ))
        .with_column(string_column(
            "status",
            "Status of the invoice",
            true,
            source_name,
        ))
        .with_column(string_column(
            "collection_method",
            "Collection method",
            true,
            source_name,
        ))
        .with_column(string_column(
            "currency",
            "Three-letter ISO currency code",
            true,
            source_name,
        ))
        .with_column(string_column(
            "customer_email",
            "Customer email",
            true,
            source_name,
        ))
        .with_column(string_column(
            "hosted_invoice_url",
            "URL for hosted invoice page",
            true,
            source_name,
        ))
        .with_column(string_column(
            "invoice_pdf",
            "URL for PDF version",
            true,
            source_name,
        ))
        .with_column(money_column(
            "amount_due",
            "Amount due in cents",
            source_name,
        ))
        .with_column(money_column("amount_paid", "Amount paid", source_name))
        .with_column(money_column(
            "amount_remaining",
            "Amount remaining",
            source_name,
        ))
        .with_column(money_column(
            "subtotal",
            "Subtotal before discounts and taxes",
            source_name,
        ))
        .with_column(money_column("tax", "Tax amount", source_name))
        .with_column(money_column(
            "total",
            "Total after discounts and taxes",
            source_name,
        ))
        .with_column(bool_column(
            "attempted",
            "Whether payment was attempted",
            true,
            source_name,
        ))
        .with_column(bool_column(
            "paid",
            "Whether invoice is paid",
            true,
            source_name,
        ))
        .with_column(bool_column(
            "livemode",
            "Has the value true if in live mode",
            false,
            source_name,
        ))
        .with_column(timestamp_column(
            "created",
            "Time at which invoice was created",
            source_name,
        ))
        .with_column(timestamp_column(
            "due_date",
            "Due date of invoice",
            source_name,
        ))
        .with_column(timestamp_column(
            "period_start",
            "Start of billing period",
            source_name,
        ))
        .with_column(timestamp_column(
            "period_end",
            "End of billing period",
            source_name,
        ))
        .with_column(json_column("lines", "Invoice line items", source_name))
        .with_column(json_column(
            "metadata",
            "Set of key-value pairs",
            source_name,
        ))
}

/// Build schema for products.
fn products_schema(source_name: &str) -> TypedSchema {
    TypedSchema::new("products", source_name)
        .with_column(id_column(
            "id",
            "Unique identifier for the product",
            source_name,
        ))
        .with_column(string_column("name", "Product name", false, source_name))
        .with_column(string_column(
            "description",
            "Product description",
            true,
            source_name,
        ))
        .with_column(bool_column(
            "active",
            "Whether product is available for purchase",
            false,
            source_name,
        ))
        .with_column(bool_column(
            "livemode",
            "Has the value true if in live mode",
            false,
            source_name,
        ))
        .with_column(string_column(
            "default_price",
            "ID of default price",
            true,
            source_name,
        ))
        .with_column(string_column(
            "unit_label",
            "Label for product's units",
            true,
            source_name,
        ))
        .with_column(timestamp_column(
            "created",
            "Time at which product was created",
            source_name,
        ))
        .with_column(timestamp_column(
            "updated",
            "Time at which product was last updated",
            source_name,
        ))
        .with_column(json_column("images", "Product image URLs", source_name))
        .with_column(json_column(
            "metadata",
            "Set of key-value pairs",
            source_name,
        ))
}

/// Build schema for prices.
fn prices_schema(source_name: &str) -> TypedSchema {
    TypedSchema::new("prices", source_name)
        .with_column(id_column(
            "id",
            "Unique identifier for the price",
            source_name,
        ))
        .with_column(string_column(
            "product",
            "ID of the product",
            false,
            source_name,
        ))
        .with_column(string_column(
            "currency",
            "Three-letter ISO currency code",
            false,
            source_name,
        ))
        .with_column(string_column(
            "type",
            "One of one_time or recurring",
            false,
            source_name,
        ))
        .with_column(string_column(
            "billing_scheme",
            "Billing scheme",
            true,
            source_name,
        ))
        .with_column(string_column(
            "nickname",
            "Price nickname",
            true,
            source_name,
        ))
        .with_column(money_column(
            "unit_amount",
            "Unit amount in cents",
            source_name,
        ))
        .with_column(bool_column(
            "active",
            "Whether price is available",
            false,
            source_name,
        ))
        .with_column(bool_column(
            "livemode",
            "Has the value true if in live mode",
            false,
            source_name,
        ))
        .with_column(timestamp_column(
            "created",
            "Time at which price was created",
            source_name,
        ))
        .with_column(json_column(
            "recurring",
            "Recurring price details",
            source_name,
        ))
        .with_column(json_column(
            "metadata",
            "Set of key-value pairs",
            source_name,
        ))
}

/// Build schema for balance transactions.
fn balance_transactions_schema(source_name: &str) -> TypedSchema {
    TypedSchema::new("balance_transactions", source_name)
        .with_column(id_column(
            "id",
            "Unique identifier for the balance transaction",
            source_name,
        ))
        .with_column(money_column(
            "amount",
            "Gross amount of transaction",
            source_name,
        ))
        .with_column(money_column(
            "fee",
            "Fees paid for this transaction",
            source_name,
        ))
        .with_column(money_column(
            "net",
            "Net amount of transaction",
            source_name,
        ))
        .with_column(string_column(
            "currency",
            "Three-letter ISO currency code",
            false,
            source_name,
        ))
        .with_column(string_column(
            "description",
            "Description",
            true,
            source_name,
        ))
        .with_column(string_column(
            "source",
            "ID of related object",
            true,
            source_name,
        ))
        .with_column(string_column(
            "status",
            "Transaction status",
            false,
            source_name,
        ))
        .with_column(string_column(
            "type",
            "Transaction type",
            false,
            source_name,
        ))
        .with_column(string_column(
            "reporting_category",
            "Reporting category",
            true,
            source_name,
        ))
        .with_column(timestamp_column(
            "created",
            "Time at which transaction was created",
            source_name,
        ))
        .with_column(timestamp_column(
            "available_on",
            "When funds become available",
            source_name,
        ))
        .with_column(json_column(
            "fee_details",
            "Fee details breakdown",
            source_name,
        ))
}

/// Build schema for payouts.
fn payouts_schema(source_name: &str) -> TypedSchema {
    TypedSchema::new("payouts", source_name)
        .with_column(id_column(
            "id",
            "Unique identifier for the payout",
            source_name,
        ))
        .with_column(money_column(
            "amount",
            "Amount to be transferred",
            source_name,
        ))
        .with_column(string_column(
            "currency",
            "Three-letter ISO currency code",
            false,
            source_name,
        ))
        .with_column(string_column("status", "Payout status", false, source_name))
        .with_column(string_column(
            "type",
            "bank_account or card",
            false,
            source_name,
        ))
        .with_column(string_column(
            "method",
            "instant or standard",
            false,
            source_name,
        ))
        .with_column(string_column(
            "destination",
            "ID of bank account or card",
            true,
            source_name,
        ))
        .with_column(string_column(
            "balance_transaction",
            "ID of balance transaction",
            true,
            source_name,
        ))
        .with_column(string_column(
            "failure_code",
            "Error code for failed payouts",
            true,
            source_name,
        ))
        .with_column(string_column(
            "failure_message",
            "Failure message",
            true,
            source_name,
        ))
        .with_column(bool_column(
            "automatic",
            "Whether payout was automatic",
            false,
            source_name,
        ))
        .with_column(bool_column(
            "livemode",
            "Has the value true if in live mode",
            false,
            source_name,
        ))
        .with_column(timestamp_column(
            "created",
            "Time at which payout was created",
            source_name,
        ))
        .with_column(timestamp_column(
            "arrival_date",
            "Expected arrival date",
            source_name,
        ))
        .with_column(json_column(
            "metadata",
            "Set of key-value pairs",
            source_name,
        ))
}

/// Build schema for refunds.
fn refunds_schema(source_name: &str) -> TypedSchema {
    TypedSchema::new("refunds", source_name)
        .with_column(id_column(
            "id",
            "Unique identifier for the refund",
            source_name,
        ))
        .with_column(money_column(
            "amount",
            "Amount refunded in cents",
            source_name,
        ))
        .with_column(string_column(
            "currency",
            "Three-letter ISO currency code",
            false,
            source_name,
        ))
        .with_column(string_column(
            "charge",
            "ID of the charge refunded",
            true,
            source_name,
        ))
        .with_column(string_column(
            "payment_intent",
            "ID of payment intent",
            true,
            source_name,
        ))
        .with_column(string_column("status", "Refund status", false, source_name))
        .with_column(string_column(
            "reason",
            "Reason for refund",
            true,
            source_name,
        ))
        .with_column(string_column(
            "balance_transaction",
            "ID of balance transaction",
            true,
            source_name,
        ))
        .with_column(string_column(
            "failure_reason",
            "Failure reason",
            true,
            source_name,
        ))
        .with_column(string_column(
            "receipt_number",
            "Receipt number",
            true,
            source_name,
        ))
        .with_column(timestamp_column(
            "created",
            "Time at which refund was created",
            source_name,
        ))
        .with_column(json_column(
            "metadata",
            "Set of key-value pairs",
            source_name,
        ))
}

/// Build schema for disputes.
fn disputes_schema(source_name: &str) -> TypedSchema {
    TypedSchema::new("disputes", source_name)
        .with_column(id_column(
            "id",
            "Unique identifier for the dispute",
            source_name,
        ))
        .with_column(money_column("amount", "Disputed amount", source_name))
        .with_column(string_column(
            "currency",
            "Three-letter ISO currency code",
            false,
            source_name,
        ))
        .with_column(string_column(
            "charge",
            "ID of the charge disputed",
            false,
            source_name,
        ))
        .with_column(string_column(
            "payment_intent",
            "ID of payment intent",
            true,
            source_name,
        ))
        .with_column(string_column(
            "status",
            "Dispute status",
            false,
            source_name,
        ))
        .with_column(string_column(
            "reason",
            "Dispute reason",
            false,
            source_name,
        ))
        .with_column(bool_column(
            "is_charge_refundable",
            "Whether charge is refundable",
            false,
            source_name,
        ))
        .with_column(bool_column(
            "livemode",
            "Has the value true if in live mode",
            false,
            source_name,
        ))
        .with_column(timestamp_column(
            "created",
            "Time at which dispute was created",
            source_name,
        ))
        .with_column(json_column("evidence", "Dispute evidence", source_name))
        .with_column(json_column(
            "evidence_details",
            "Evidence requirements",
            source_name,
        ))
        .with_column(json_column(
            "metadata",
            "Set of key-value pairs",
            source_name,
        ))
}

/// Build schema for events.
fn events_schema(source_name: &str) -> TypedSchema {
    TypedSchema::new("events", source_name)
        .with_column(id_column(
            "id",
            "Unique identifier for the event",
            source_name,
        ))
        .with_column(string_column("type", "Event type", false, source_name))
        .with_column(string_column(
            "api_version",
            "API version used to render data",
            true,
            source_name,
        ))
        .with_column(bool_column(
            "livemode",
            "Has the value true if in live mode",
            false,
            source_name,
        ))
        .with_column(timestamp_column(
            "created",
            "Time at which event occurred",
            source_name,
        ))
        .with_column(json_column("data", "Event data object", source_name))
        .with_column(json_column(
            "request",
            "Info about triggering request",
            source_name,
        ))
}

/// Build schema for invoice items.
fn invoice_items_schema(source_name: &str) -> TypedSchema {
    TypedSchema::new("invoice_items", source_name)
        .with_column(id_column(
            "id",
            "Unique identifier for the invoice item",
            source_name,
        ))
        .with_column(string_column(
            "customer",
            "ID of the customer",
            false,
            source_name,
        ))
        .with_column(string_column(
            "invoice",
            "ID of the invoice",
            true,
            source_name,
        ))
        .with_column(string_column(
            "subscription",
            "ID of the subscription",
            true,
            source_name,
        ))
        .with_column(string_column("price", "ID of the price", true, source_name))
        .with_column(string_column(
            "currency",
            "Three-letter ISO currency code",
            false,
            source_name,
        ))
        .with_column(string_column(
            "description",
            "Description",
            true,
            source_name,
        ))
        .with_column(money_column("amount", "Amount in cents", source_name))
        .with_column(money_column(
            "unit_amount",
            "Unit amount in cents",
            source_name,
        ))
        .with_column(
            TypedColumn::new("quantity", &DataType::Int32, true, "integer", source_name)
                .with_description("Quantity of units"),
        )
        .with_column(bool_column(
            "discountable",
            "Whether item can be discounted",
            false,
            source_name,
        ))
        .with_column(bool_column(
            "proration",
            "Whether item is a proration",
            false,
            source_name,
        ))
        .with_column(bool_column(
            "livemode",
            "Has the value true if in live mode",
            false,
            source_name,
        ))
        .with_column(timestamp_column(
            "date",
            "Date invoice item was added",
            source_name,
        ))
        .with_column(timestamp_column(
            "period_start",
            "Start of period",
            source_name,
        ))
        .with_column(timestamp_column("period_end", "End of period", source_name))
        .with_column(json_column(
            "metadata",
            "Set of key-value pairs",
            source_name,
        ))
}

/// Build schema for coupons.
fn coupons_schema(source_name: &str) -> TypedSchema {
    TypedSchema::new("coupons", source_name)
        .with_column(id_column(
            "id",
            "Unique identifier for the coupon",
            source_name,
        ))
        .with_column(string_column(
            "name",
            "Name of the coupon",
            true,
            source_name,
        ))
        .with_column(string_column(
            "currency",
            "Currency for amount_off",
            true,
            source_name,
        ))
        .with_column(string_column(
            "duration",
            "One of forever, once, or repeating",
            false,
            source_name,
        ))
        .with_column(money_column(
            "amount_off",
            "Amount off in cents",
            source_name,
        ))
        .with_column(
            TypedColumn::new(
                "percent_off",
                &DataType::Float64,
                true,
                "decimal",
                source_name,
            )
            .with_description("Percent off (0 to 100)"),
        )
        .with_column(
            TypedColumn::new(
                "duration_in_months",
                &DataType::Int32,
                true,
                "integer",
                source_name,
            )
            .with_description("Number of months for repeating coupons"),
        )
        .with_column(
            TypedColumn::new(
                "max_redemptions",
                &DataType::Int32,
                true,
                "integer",
                source_name,
            )
            .with_description("Maximum number of times coupon can be redeemed"),
        )
        .with_column(
            TypedColumn::new(
                "times_redeemed",
                &DataType::Int32,
                true,
                "integer",
                source_name,
            )
            .with_description("Number of times coupon has been redeemed"),
        )
        .with_column(bool_column(
            "valid",
            "Whether coupon is still valid",
            false,
            source_name,
        ))
        .with_column(bool_column(
            "livemode",
            "Has the value true if in live mode",
            false,
            source_name,
        ))
        .with_column(timestamp_column(
            "created",
            "Time at which coupon was created",
            source_name,
        ))
        .with_column(timestamp_column(
            "redeem_by",
            "Date after which coupon cannot be used",
            source_name,
        ))
        .with_column(json_column(
            "metadata",
            "Set of key-value pairs",
            source_name,
        ))
}

// ============================================================================
// Stripe Schema Discovery
// ============================================================================

/// Stripe schema discovery implementation.
///
/// Since Stripe has a well-defined API, we provide static schema definitions
/// that match the Stripe API structure.
pub struct StripeSchemaDiscovery {
    /// List of objects to include (empty = all).
    objects: Vec<StripeObject>,
}

impl StripeSchemaDiscovery {
    /// Create a new Stripe schema discovery.
    pub fn new() -> Self {
        Self {
            objects: StripeObject::all(),
        }
    }

    /// Limit discovery to specific objects.
    pub fn with_objects(mut self, objects: Vec<StripeObject>) -> Self {
        self.objects = objects;
        self
    }

    /// Get schema for a Stripe object.
    fn get_schema(&self, obj: StripeObject, source_name: &str) -> TypedSchema {
        match obj {
            StripeObject::Customers => customers_schema(source_name),
            StripeObject::Charges => charges_schema(source_name),
            StripeObject::PaymentIntents => payment_intents_schema(source_name),
            StripeObject::Subscriptions => subscriptions_schema(source_name),
            StripeObject::Invoices => invoices_schema(source_name),
            StripeObject::InvoiceItems => invoice_items_schema(source_name),
            StripeObject::Products => products_schema(source_name),
            StripeObject::Prices => prices_schema(source_name),
            StripeObject::Coupons => coupons_schema(source_name),
            StripeObject::BalanceTransactions => balance_transactions_schema(source_name),
            StripeObject::Payouts => payouts_schema(source_name),
            StripeObject::Refunds => refunds_schema(source_name),
            StripeObject::Disputes => disputes_schema(source_name),
            StripeObject::Events => events_schema(source_name),
        }
    }
}

impl Default for StripeSchemaDiscovery {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SchemaDiscovery for StripeSchemaDiscovery {
    #[instrument(skip(self, source))]
    async fn discover_schemas(
        &self,
        source: &RegisteredSource,
    ) -> DiscoveryResult<Vec<TypedSchema>> {
        info!("Discovering Stripe schemas for source: {}", source.name);

        let schemas: Vec<TypedSchema> = self
            .objects
            .iter()
            .map(|obj| self.get_schema(*obj, &source.name))
            .collect();

        debug!("Discovered {} Stripe tables", schemas.len());
        Ok(schemas)
    }

    #[instrument(skip(self, source))]
    async fn discover_table_schema(
        &self,
        source: &RegisteredSource,
        table_name: &str,
    ) -> DiscoveryResult<Option<TypedSchema>> {
        info!(
            "Discovering Stripe schema for table: {}.{}",
            source.name, table_name
        );

        // Find the matching object
        for obj in &self.objects {
            if obj.table_name() == table_name {
                return Ok(Some(self.get_schema(*obj, &source.name)));
            }
        }

        // Table not found in Stripe objects
        Ok(None)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stripe_objects_all() {
        let objects = StripeObject::all();
        assert!(objects.len() >= 10);
    }

    #[test]
    fn test_customers_schema() {
        let schema = customers_schema("stripe");
        assert_eq!(schema.table_name, "customers");
        assert!(!schema.columns.is_empty());

        // Check for key columns
        let col_names: Vec<_> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(col_names.contains(&"id"));
        assert!(col_names.contains(&"email"));
        assert!(col_names.contains(&"created"));
    }

    #[test]
    fn test_charges_schema_has_money_columns() {
        let schema = charges_schema("stripe");

        // Find amount column
        let amount = schema.columns.iter().find(|c| c.name == "amount").unwrap();
        assert!(amount.is_money());
    }

    #[test]
    fn test_schema_discovery_all() {
        let discovery = StripeSchemaDiscovery::new();
        assert_eq!(discovery.objects.len(), StripeObject::all().len());
    }

    #[test]
    fn test_schema_discovery_filtered() {
        let discovery = StripeSchemaDiscovery::new()
            .with_objects(vec![StripeObject::Customers, StripeObject::Charges]);

        assert_eq!(discovery.objects.len(), 2);
    }
}
