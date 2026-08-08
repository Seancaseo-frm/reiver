//! Payment methods API endpoints for Stripe integration.
//!
//! # Security
//!
//! - All endpoints require authentication via JWT
//! - Admin-only operations check organization role
//! - Webhook endpoint verifies Stripe signature
//! - No sensitive data (full card numbers) is logged or stored
//!
//! # Error Handling
//!
//! Uses typed PaymentError for consistent error responses.
//! Maps provider-specific errors to appropriate HTTP status codes.

use axum::{
    body::Bytes,
    extract::{ConnectInfo, DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use chrono::DateTime;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

use rust_decimal::prelude::ToPrimitive;

use crate::api::auth_helpers::{authenticate, require_admin};
use crate::app_state::WebsiteState;
use crate::audit::{AuditCaller, AuditEventBuilder, AuditEventType, AuditOrigin};
use crate::billing::{
    validate_price_id, PaymentError, PaymentMethodInfo, PaymentProvider, StripePaymentProvider,
    WebhookEvent, WebhookEventType,
};
use crate::db::DbPool;
use crate::rate_limit::{
    check_payment_method_confirm_rate_limit, check_setup_intent_rate_limit,
    check_subscription_rate_limit, check_webhook_rate_limit, extract_client_ip,
};

// ============================================================================
// Webhook Processing Error Types
// ============================================================================

/// Error type for webhook processing that indicates whether the error is retriable.
///
/// Using typed errors instead of string matching on error messages:
/// 1. Prevents information leakage about internal error handling
/// 2. Makes retry behavior explicit and testable
/// 3. Avoids fragile string matching that could break with library updates
#[derive(Debug)]
enum WebhookProcessingError {
    /// Transient error - Stripe should retry the webhook
    /// Examples: database connection issues, timeouts, deadlocks
    Transient(anyhow::Error),

    /// Permanent error - should not be retried
    /// Examples: validation errors, orphaned data, business logic errors
    Permanent(anyhow::Error),
}

impl WebhookProcessingError {
    /// Create a transient (retriable) error
    fn transient(err: impl Into<anyhow::Error>) -> Self {
        Self::Transient(err.into())
    }

    /// Create a permanent (non-retriable) error
    fn permanent(err: impl Into<anyhow::Error>) -> Self {
        Self::Permanent(err.into())
    }

    /// Returns true if this error is transient and should be retried
    fn is_retriable(&self) -> bool {
        matches!(self, Self::Transient(_))
    }

    /// Get the underlying error message
    fn message(&self) -> String {
        match self {
            Self::Transient(e) | Self::Permanent(e) => e.to_string(),
        }
    }
}

impl std::fmt::Display for WebhookProcessingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transient(e) => write!(f, "transient error: {}", e),
            Self::Permanent(e) => write!(f, "permanent error: {}", e),
        }
    }
}

/// Convert sqlx errors to WebhookProcessingError with appropriate retry classification.
/// Database connection/timeout errors are transient; constraint violations are permanent.
impl From<sqlx::Error> for WebhookProcessingError {
    fn from(err: sqlx::Error) -> Self {
        match &err {
            // Connection and timeout errors are transient
            sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed | sqlx::Error::WorkerCrashed => {
                Self::Transient(err.into())
            }

            // IO errors are typically transient (network issues)
            sqlx::Error::Io(_) => Self::Transient(err.into()),

            // Protocol errors could be either, treat as transient to be safe
            sqlx::Error::Protocol(_) => Self::Transient(err.into()),

            // Database constraint violations, type errors, etc. are permanent
            sqlx::Error::Database(_)
            | sqlx::Error::RowNotFound
            | sqlx::Error::TypeNotFound { .. }
            | sqlx::Error::ColumnNotFound(_)
            | sqlx::Error::ColumnDecode { .. }
            | sqlx::Error::Decode(_)
            | sqlx::Error::Configuration(_) => Self::Permanent(err.into()),

            // Tls errors are typically configuration issues (permanent)
            sqlx::Error::Tls(_) => Self::Permanent(err.into()),

            // Migration errors shouldn't happen during webhooks
            sqlx::Error::Migrate(_) => Self::Permanent(err.into()),

            // Catch-all: treat unknown errors as permanent to avoid infinite retries
            _ => Self::Permanent(err.into()),
        }
    }
}

/// Maximum webhook body size (64KB).
/// This prevents DoS attacks via extremely large payloads.
/// Stripe webhook events are typically a few KB at most.
const MAX_WEBHOOK_BODY_SIZE: usize = 65_536;

/// Create the payments router.
pub fn create_payments_router() -> Router<Arc<WebsiteState>> {
    // Create webhook route with body size limit for DoS protection
    let webhook_route = Router::new()
        .route("/webhook", post(handle_stripe_webhook))
        .layer(DefaultBodyLimit::max(MAX_WEBHOOK_BODY_SIZE));

    Router::new()
        // Payment method endpoints
        .route("/methods", get(list_payment_methods))
        .route("/methods/status", get(get_payment_method_status))
        .route("/methods/setup", post(create_setup_intent))
        .route("/methods/confirm", post(confirm_payment_method))
        .route("/methods/{id}", get(get_payment_method))
        .route("/methods/{id}", delete(delete_payment_method))
        .route("/methods/{id}/default", post(set_default_payment_method))
        // Subscription endpoints
        .route("/subscription", get(get_subscription))
        .route("/subscription", post(create_subscription))
        .route("/subscription/cancel", post(cancel_subscription))
        // Tier change (self-service upgrade/downgrade)
        .route("/tier", post(change_tier))
        .route("/tiers", get(list_available_tiers))
        // Invoice endpoints
        .route("/invoices", get(list_invoices))
        .route("/invoices/sync", post(sync_invoices))
        // Billing portal
        .route("/portal", post(create_billing_portal_session))
        // Credit purchase
        .route("/credits/checkout", post(create_credits_checkout))
        .route("/credits/balance", get(get_credit_balance))
        .route("/credits/transactions", get(list_credit_transactions))
        // Webhook endpoint (no auth - verified by Stripe signature)
        // Body size limited via separate router with DefaultBodyLimit layer
        .merge(webhook_route)
}

// ============================================================================
// Response Types
// ============================================================================

#[derive(Serialize)]
struct ApiResponse<T: Serialize> {
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl<T: Serialize> ApiResponse<T> {
    fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }
}

/// Error response helper (always returns ApiResponse<()>).
fn api_error(message: impl Into<String>) -> ApiResponse<()> {
    ApiResponse {
        success: false,
        data: None,
        error: Some(message.into()),
    }
}

#[derive(Serialize)]
struct PaymentMethodsData {
    payment_methods: Vec<PaymentMethodInfo>,
    default_payment_method_id: Option<Uuid>,
}

#[derive(Serialize)]
struct PaymentMethodStatusData {
    has_payment_method: bool,
}

#[derive(Serialize)]
struct InvoicesData {
    invoices: Vec<crate::billing::InvoiceInfo>,
    total: i64,
}

// ============================================================================
// Request Types
// ============================================================================

#[derive(Deserialize)]
pub struct ConfirmPaymentMethodRequest {
    setup_intent_id: String,
    #[serde(default = "default_true")]
    set_as_default: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Deserialize)]
pub struct CreateSubscriptionRequest {
    price_id: String,
    payment_method_id: Option<Uuid>,
}

#[derive(Deserialize)]
pub struct CancelSubscriptionRequest {
    #[serde(default = "default_true")]
    at_period_end: bool,
}

#[derive(Deserialize)]
pub struct InvoicesQuery {
    limit: Option<i64>,
    offset: Option<i64>,
}

// ============================================================================
// Billing Payment Method Cache
// ============================================================================

/// Invalidate the `billing:pm:{org_id}` Redis cache when a payment method is
/// added, removed, or changed as default.
async fn invalidate_billing_pm_cache(redis: &crate::app_state::RedisPool, org_id: Uuid) {
    let key = format!("billing:pm:{}", org_id);
    if let Ok(mut conn) = redis.get().await {
        let _ = bb8_redis::redis::cmd("DEL")
            .arg(&key)
            .query_async::<()>(&mut *conn)
            .await;
    }
}

// ============================================================================
// Payment Provider Factory
// ============================================================================

/// Create a payment provider from application state.
fn create_payment_provider(
    state: &WebsiteState,
) -> Result<impl PaymentProvider, (StatusCode, Json<ApiResponse<()>>)> {
    let api_key = state.config.stripe_api_key.as_ref().ok_or_else(|| {
        warn!("Stripe API key not configured");
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(api_error("Payment system not configured")),
        )
    })?;

    Ok(StripePaymentProvider::new(
        api_key,
        state.db.clone(),
        state.redis.clone(),
        state.config.stripe_webhook_secret.clone(),
        state.config.stripe_metered_price_id.clone(),
    ))
}

/// Map PaymentError to HTTP response.
fn map_payment_error(err: PaymentError) -> (StatusCode, Json<ApiResponse<()>>) {
    let (status, message) = match &err {
        PaymentError::NotConfigured => (
            StatusCode::SERVICE_UNAVAILABLE,
            "Payment system not configured",
        ),
        PaymentError::CustomerNotFound(_) => (StatusCode::NOT_FOUND, "Customer not found"),
        PaymentError::PaymentMethodNotFound(_) => {
            (StatusCode::NOT_FOUND, "Payment method not found")
        }
        PaymentError::SubscriptionNotFound(_) => (StatusCode::NOT_FOUND, "Subscription not found"),
        PaymentError::SubscriptionAlreadyCanceled => {
            (StatusCode::OK, "Subscription is already canceled")
        }
        PaymentError::InvalidPaymentMethod(_) => {
            (StatusCode::BAD_REQUEST, "Invalid payment method")
        }
        PaymentError::InvalidSetupIntent(_) => (StatusCode::BAD_REQUEST, "Invalid setup intent"),
        PaymentError::PaymentDeclined(msg) => {
            debug!("Payment declined: {}", msg);
            (StatusCode::PAYMENT_REQUIRED, "Payment was declined")
        }
        PaymentError::CardExpired => (StatusCode::PAYMENT_REQUIRED, "Card has expired"),
        PaymentError::InsufficientFunds => (StatusCode::PAYMENT_REQUIRED, "Insufficient funds"),
        PaymentError::AuthorizationRequired(_) => (
            StatusCode::PAYMENT_REQUIRED,
            "Additional authorization required",
        ),
        PaymentError::RateLimited => (
            StatusCode::TOO_MANY_REQUESTS,
            "Too many requests, please try again later",
        ),
        PaymentError::InvalidWebhookSignature => {
            (StatusCode::BAD_REQUEST, "Invalid webhook signature")
        }
        PaymentError::DuplicateEvent(_) => (StatusCode::OK, "Event already processed"),
        PaymentError::ProviderError(msg) => {
            error!("Payment provider error: {}", msg);
            (StatusCode::BAD_GATEWAY, "Payment service error")
        }
        PaymentError::DatabaseError(msg) => {
            error!("Database error in payments: {}", msg);
            (StatusCode::INTERNAL_SERVER_ERROR, "Internal error")
        }
    };

    (status, Json(api_error(message)))
}

// ============================================================================
// Payment Method Endpoints
// ============================================================================

/// List payment methods for the organization.
/// GET /api/payments/methods
///
/// # Authorization
/// Requires admin role - payment method details (last 4 digits, expiration)
/// are considered sensitive billing information.
async fn list_payment_methods(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let auth = match authenticate(&headers, &state).await {
        Ok(auth) => auth,
        Err(e) => return e.into_response(),
    };

    // Require admin role to view payment methods (contains sensitive billing data)
    if let Err(e) = require_admin(&state, auth.user_id, auth.organization_id).await {
        return e.into_response();
    }

    let provider = match create_payment_provider(&state) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };

    match provider.list_payment_methods(auth.organization_id).await {
        Ok(methods) => {
            let default_id = methods.iter().find(|m| m.is_default).map(|m| m.id);
            (
                StatusCode::OK,
                Json(ApiResponse::success(PaymentMethodsData {
                    payment_methods: methods,
                    default_payment_method_id: default_id,
                })),
            )
                .into_response()
        }
        Err(e) => map_payment_error(e).into_response(),
    }
}

/// Whether the organization has an active default payment method (boolean only).
/// GET /api/payments/methods/status
///
/// Requires org admin — same audience as Billing & Usage.
async fn get_payment_method_status(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let auth = match authenticate(&headers, &state).await {
        Ok(auth) => auth,
        Err(e) => return e.into_response(),
    };

    if let Err(e) = require_admin(&state, auth.user_id, auth.organization_id).await {
        return e.into_response();
    }

    let has: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM payment_methods pm
            JOIN stripe_customers sc ON sc.stripe_customer_id = pm.provider_customer_id
            WHERE sc.organization_id = $1
              AND pm.is_default = true
              AND pm.status = 'active'
        )
        "#,
    )
    .bind(auth.organization_id)
    .fetch_one(&*state.db)
    .await
    .unwrap_or(false);

    let has = if !has {
        if let Some(api_key) = state.config.stripe_api_key.as_ref() {
            let provider = StripePaymentProvider::new(
                api_key,
                state.db.clone(),
                state.redis.clone(),
                state.config.stripe_webhook_secret.clone(),
                state.config.stripe_metered_price_id.clone(),
            );
            provider
                .sync_payment_methods_from_stripe(auth.organization_id)
                .await
                .unwrap_or(false)
        } else {
            false
        }
    } else {
        true
    };

    (
        StatusCode::OK,
        Json(ApiResponse::success(PaymentMethodStatusData {
            has_payment_method: has,
        })),
    )
        .into_response()
}

/// Create a setup intent for adding a new payment method.
/// POST /api/payments/methods/setup
///
/// # Rate Limiting
/// Limited to 5 setup intents per hour per organization to prevent abuse.
/// Setup intents can incur costs at scale if not rate limited.
async fn create_setup_intent(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let auth = match authenticate(&headers, &state).await {
        Ok(auth) => auth,
        Err(e) => return e.into_response(),
    };

    // Apply strict rate limit for setup intent creation (5/hour per org)
    if let Err(e) = check_setup_intent_rate_limit(&state.redis, &auth.organization_id).await {
        trace!(
            organization_id = %auth.organization_id,
            error = %e,
            "Setup intent rate limit exceeded"
        );
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(api_error(
                "Too many payment method setup attempts. Please try again later.",
            )),
        )
            .into_response();
    }

    let provider = match create_payment_provider(&state) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };

    match provider.create_setup_intent(auth.organization_id).await {
        Ok(intent) => (StatusCode::OK, Json(ApiResponse::success(intent))).into_response(),
        Err(e) => map_payment_error(e).into_response(),
    }
}

/// Confirm a payment method after successful Stripe setup.
/// POST /api/payments/methods/confirm
///
/// # Rate Limiting
/// Limited to 10 confirmations per hour per organization to prevent abuse.
/// While setup intent creation is already rate limited, this provides defense-in-depth
/// against attackers who may try to rapidly retry confirmations.
async fn confirm_payment_method(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(request): Json<ConfirmPaymentMethodRequest>,
) -> impl IntoResponse {
    let auth = match authenticate(&headers, &state).await {
        Ok(auth) => auth,
        Err(e) => return e.into_response(),
    };

    // Apply rate limit for payment method confirmation (10/hour per org)
    if let Err(e) =
        check_payment_method_confirm_rate_limit(&state.redis, &auth.organization_id).await
    {
        trace!(
            organization_id = %auth.organization_id,
            error = %e,
            "Payment method confirm rate limit exceeded"
        );
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(api_error(
                "Too many payment method confirmation attempts. Please try again later.",
            )),
        )
            .into_response();
    }

    let provider = match create_payment_provider(&state) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };

    match provider
        .confirm_payment_method(
            auth.organization_id,
            &request.setup_intent_id,
            request.set_as_default,
            Some(auth.user_id),
        )
        .await
    {
        Ok(pm) => {
            let audit_origin = AuditOrigin::from_headers(&headers);
            let audit_caller = AuditCaller::from_headers(&headers);
            let _ = AuditEventBuilder::new(AuditEventType::PaymentMethodAdded)
                .organization(auth.organization_id)
                .actor(auth.user_id)
                .resource("payment_method", pm.id)
                .details(serde_json::json!({
                    "created": {
                        "card_brand": pm.card_brand,
                        "card_last_four": pm.card_last_four,
                        "is_default": pm.is_default
                    }
                }))
                .origin(
                    &audit_origin.origin_type,
                    &audit_origin.origin_ref,
                    &audit_origin.origin_reason,
                )
                .caller(
                    &audit_caller.caller_type,
                    &audit_caller.key_label,
                    &audit_caller.key_prefix,
                )
                .success()
                .log(state.clickhouse.as_ref())
                .await;

            info!(
                user_id = %auth.user_id,
                organization_id = %auth.organization_id,
                payment_method_id = %pm.id,
                "Payment method added"
            );
            invalidate_billing_pm_cache(&state.redis, auth.organization_id).await;
            (StatusCode::OK, Json(ApiResponse::success(pm))).into_response()
        }
        Err(e) => map_payment_error(e).into_response(),
    }
}

/// Get a specific payment method.
/// GET /api/payments/methods/{id}
///
/// # Authorization
/// Requires admin role - payment method details (last 4 digits, expiration)
/// are considered sensitive billing information.
async fn get_payment_method(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(payment_method_id): Path<Uuid>,
) -> impl IntoResponse {
    let auth = match authenticate(&headers, &state).await {
        Ok(auth) => auth,
        Err(e) => return e.into_response(),
    };

    // Require admin role to view payment method details (contains sensitive billing data)
    if let Err(e) = require_admin(&state, auth.user_id, auth.organization_id).await {
        return e.into_response();
    }

    let provider = match create_payment_provider(&state) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };

    match provider.get_payment_method(payment_method_id).await {
        Ok(Some(pm)) => {
            // Verify ownership (defense-in-depth, admin check above ensures org membership)
            if pm.organization_id != auth.organization_id {
                return (
                    StatusCode::NOT_FOUND,
                    Json(api_error("Payment method not found")),
                )
                    .into_response();
            }
            (StatusCode::OK, Json(ApiResponse::success(pm))).into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(api_error("Payment method not found")),
        )
            .into_response(),
        Err(e) => map_payment_error(e).into_response(),
    }
}

/// Delete a payment method.
/// DELETE /api/payments/methods/{id}
async fn delete_payment_method(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(payment_method_id): Path<Uuid>,
) -> impl IntoResponse {
    let auth = match authenticate(&headers, &state).await {
        Ok(auth) => auth,
        Err(e) => return e.into_response(),
    };

    if let Err(e) = require_admin(&state, auth.user_id, auth.organization_id).await {
        return e.into_response();
    }

    let provider = match create_payment_provider(&state) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };

    match provider
        .delete_payment_method(auth.organization_id, payment_method_id)
        .await
    {
        Ok(()) => {
            let audit_origin = AuditOrigin::from_headers(&headers);
            let audit_caller = AuditCaller::from_headers(&headers);
            let _ = AuditEventBuilder::new(AuditEventType::PaymentMethodRemoved)
                .organization(auth.organization_id)
                .actor(auth.user_id)
                .resource("payment_method", payment_method_id)
                .details(serde_json::json!({
                    "deleted": {
                        "payment_method_id": payment_method_id.to_string()
                    }
                }))
                .origin(
                    &audit_origin.origin_type,
                    &audit_origin.origin_ref,
                    &audit_origin.origin_reason,
                )
                .caller(
                    &audit_caller.caller_type,
                    &audit_caller.key_label,
                    &audit_caller.key_prefix,
                )
                .success()
                .log(state.clickhouse.as_ref())
                .await;

            info!(
                user_id = %auth.user_id,
                payment_method_id = %payment_method_id,
                "Payment method deleted"
            );
            invalidate_billing_pm_cache(&state.redis, auth.organization_id).await;
            (StatusCode::OK, Json(ApiResponse::success(()))).into_response()
        }
        Err(e) => map_payment_error(e).into_response(),
    }
}

/// Set a payment method as default.
/// POST /api/payments/methods/{id}/default
async fn set_default_payment_method(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(payment_method_id): Path<Uuid>,
) -> impl IntoResponse {
    let auth = match authenticate(&headers, &state).await {
        Ok(auth) => auth,
        Err(e) => return e.into_response(),
    };

    // Require admin to change default payment method
    if let Err(e) = require_admin(&state, auth.user_id, auth.organization_id).await {
        return e.into_response();
    }

    let provider = match create_payment_provider(&state) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };

    match provider
        .set_default_payment_method(auth.organization_id, payment_method_id)
        .await
    {
        Ok(()) => {
            let audit_origin = AuditOrigin::from_headers(&headers);
            let audit_caller = AuditCaller::from_headers(&headers);
            let _ = AuditEventBuilder::new(AuditEventType::PaymentMethodDefaultChanged)
                .organization(auth.organization_id)
                .actor(auth.user_id)
                .resource("payment_method", payment_method_id)
                .details(serde_json::json!({
                    "after": {
                        "default_payment_method_id": payment_method_id.to_string()
                    }
                }))
                .origin(
                    &audit_origin.origin_type,
                    &audit_origin.origin_ref,
                    &audit_origin.origin_reason,
                )
                .caller(
                    &audit_caller.caller_type,
                    &audit_caller.key_label,
                    &audit_caller.key_prefix,
                )
                .success()
                .log(state.clickhouse.as_ref())
                .await;

            invalidate_billing_pm_cache(&state.redis, auth.organization_id).await;
            (StatusCode::OK, Json(ApiResponse::success(()))).into_response()
        }
        Err(e) => map_payment_error(e).into_response(),
    }
}

// ============================================================================
// Subscription Endpoints
// ============================================================================

/// Get the current subscription.
/// GET /api/payments/subscription
///
/// Deliberately does NOT require admin role -- all org members should be able
/// to see what plan the organization is on (status, period, etc.).
/// Write operations (create, cancel) still require admin.
async fn get_subscription(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let auth = match authenticate(&headers, &state).await {
        Ok(auth) => auth,
        Err(e) => return e.into_response(),
    };

    let provider = match create_payment_provider(&state) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };

    match provider.get_subscription(auth.organization_id).await {
        Ok(sub) => (StatusCode::OK, Json(ApiResponse::success(sub))).into_response(),
        Err(e) => map_payment_error(e).into_response(),
    }
}

/// Create a subscription.
/// POST /api/payments/subscription
///
/// # Rate Limiting
/// Limited to 3 subscription attempts per hour per organization to prevent abuse.
/// This prevents rapid subscription creation/cancellation that could cause billing issues.
async fn create_subscription(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(request): Json<CreateSubscriptionRequest>,
) -> impl IntoResponse {
    let auth = match authenticate(&headers, &state).await {
        Ok(auth) => auth,
        Err(e) => return e.into_response(),
    };

    if let Err(e) = require_admin(&state, auth.user_id, auth.organization_id).await {
        return e.into_response();
    }

    // Apply rate limit for subscription creation (3/hour per org)
    if let Err(e) = check_subscription_rate_limit(&state.redis, &auth.organization_id).await {
        trace!(
            organization_id = %auth.organization_id,
            error = %e,
            "Subscription creation rate limit exceeded"
        );
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(api_error(
                "Too many subscription attempts. Please try again later.",
            )),
        )
            .into_response();
    }

    // Validate price_id format before any other processing (fail-fast)
    // This prevents malformed IDs from reaching the provider
    if let Err(e) = validate_price_id(&request.price_id) {
        warn!(
            user_id = %auth.user_id,
            organization_id = %auth.organization_id,
            price_id = %request.price_id,
            error = %e,
            "Invalid price ID format"
        );
        return (
            StatusCode::BAD_REQUEST,
            Json(api_error("Invalid price ID format")),
        )
            .into_response();
    }

    // Validate price_id: check env-var allowlist first, then fall back to
    // tier_definitions.stripe_price_id in the DB.
    let env_allowed = &state.config.stripe_allowed_price_ids;
    let allowed_by_env = !env_allowed.is_empty() && env_allowed.contains(&request.price_id);

    let allowed_by_tier = if !allowed_by_env {
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM tier_definitions WHERE stripe_price_id = $1",
        )
        .bind(&request.price_id)
        .fetch_one(state.db.as_ref())
        .await
        .unwrap_or(0)
            > 0
    } else {
        false
    };

    if !allowed_by_env && !allowed_by_tier {
        warn!(
            user_id = %auth.user_id,
            organization_id = %auth.organization_id,
            price_id = %request.price_id,
            "Attempted to create subscription with disallowed price ID"
        );
        return (StatusCode::BAD_REQUEST, Json(api_error("Invalid price ID"))).into_response();
    }

    let provider = match create_payment_provider(&state) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };

    match provider
        .create_subscription(
            auth.organization_id,
            &request.price_id,
            request.payment_method_id,
        )
        .await
    {
        Ok(sub) => {
            let audit_origin = AuditOrigin::from_headers(&headers);
            let audit_caller = AuditCaller::from_headers(&headers);
            let _ = AuditEventBuilder::new(AuditEventType::SubscriptionCreated)
                .organization(auth.organization_id)
                .actor(auth.user_id)
                .details(serde_json::json!({
                    "created": {
                        "subscription_id": sub.subscription_id,
                        "price_id": request.price_id,
                        "subscription_status": sub.status
                    }
                }))
                .origin(
                    &audit_origin.origin_type,
                    &audit_origin.origin_ref,
                    &audit_origin.origin_reason,
                )
                .caller(
                    &audit_caller.caller_type,
                    &audit_caller.key_label,
                    &audit_caller.key_prefix,
                )
                .success()
                .log(state.clickhouse.as_ref())
                .await;

            info!(
                user_id = %auth.user_id,
                organization_id = %auth.organization_id,
                subscription_id = %sub.subscription_id,
                "Subscription created"
            );
            (StatusCode::OK, Json(ApiResponse::success(sub))).into_response()
        }
        Err(e) => map_payment_error(e).into_response(),
    }
}

/// Cancel a subscription.
/// POST /api/payments/subscription/cancel
///
/// # Rate Limiting
/// Limited to 3 subscription operations per hour per organization to prevent abuse.
/// This prevents rapid subscription cancellation/recreation that could cause billing issues.
async fn cancel_subscription(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(request): Json<CancelSubscriptionRequest>,
) -> impl IntoResponse {
    let auth = match authenticate(&headers, &state).await {
        Ok(auth) => auth,
        Err(e) => return e.into_response(),
    };

    if let Err(e) = require_admin(&state, auth.user_id, auth.organization_id).await {
        return e.into_response();
    }

    // Apply rate limit for subscription cancellation (shares limit with creation: 3/hour per org)
    if let Err(e) = check_subscription_rate_limit(&state.redis, &auth.organization_id).await {
        trace!(
            organization_id = %auth.organization_id,
            error = %e,
            "Subscription cancellation rate limit exceeded"
        );
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(api_error(
                "Too many subscription operations. Please try again later.",
            )),
        )
            .into_response();
    }

    let provider = match create_payment_provider(&state) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };

    match provider
        .cancel_subscription(auth.organization_id, request.at_period_end)
        .await
    {
        Ok(()) => {
            let audit_origin = AuditOrigin::from_headers(&headers);
            let audit_caller = AuditCaller::from_headers(&headers);
            let _ = AuditEventBuilder::new(AuditEventType::SubscriptionCanceled)
                .organization(auth.organization_id)
                .actor(auth.user_id)
                .details(serde_json::json!({
                    "deleted": {
                        "at_period_end": request.at_period_end
                    }
                }))
                .origin(
                    &audit_origin.origin_type,
                    &audit_origin.origin_ref,
                    &audit_origin.origin_reason,
                )
                .caller(
                    &audit_caller.caller_type,
                    &audit_caller.key_label,
                    &audit_caller.key_prefix,
                )
                .success()
                .log(state.clickhouse.as_ref())
                .await;

            info!(
                user_id = %auth.user_id,
                organization_id = %auth.organization_id,
                at_period_end = request.at_period_end,
                "Subscription canceled"
            );
            (StatusCode::OK, Json(ApiResponse::success(()))).into_response()
        }
        Err(e) => map_payment_error(e).into_response(),
    }
}

// ============================================================================
// Invoice Endpoints
// ============================================================================

/// List invoices for the organization.
/// GET /api/payments/invoices
///
/// # Authorization
/// Requires admin role - invoices contain sensitive billing information
/// including amounts and payment status that should not be visible to all members.
async fn list_invoices(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Query(query): Query<InvoicesQuery>,
) -> impl IntoResponse {
    let auth = match authenticate(&headers, &state).await {
        Ok(auth) => auth,
        Err(e) => return e.into_response(),
    };

    // Require admin role to view invoices (contains sensitive billing data)
    if let Err(e) = require_admin(&state, auth.user_id, auth.organization_id).await {
        return e.into_response();
    }

    let provider = match create_payment_provider(&state) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };

    // Validate limit: minimum 1, maximum 100, default 20
    let limit = query.limit.unwrap_or(20).max(1).min(100);
    // Cap offset to prevent performance issues with very large offsets
    // Maximum offset of 10000 prevents expensive deep pagination
    const MAX_OFFSET: i64 = 10000;
    let offset = query.offset.unwrap_or(0).max(0).min(MAX_OFFSET);

    match provider
        .list_invoices(auth.organization_id, limit, offset)
        .await
    {
        Ok((invoices, total)) => (
            StatusCode::OK,
            Json(ApiResponse::success(InvoicesData { invoices, total })),
        )
            .into_response(),
        Err(e) => map_payment_error(e).into_response(),
    }
}

/// Sync invoices from Stripe to the local database.
/// POST /api/payments/invoices/sync
///
/// # Authorization
/// Requires admin role -- this is a reconciliation tool for backfilling
/// historical invoices that were created before webhook setup.
async fn sync_invoices(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let auth = match authenticate(&headers, &state).await {
        Ok(auth) => auth,
        Err(e) => return e.into_response(),
    };

    if let Err(e) = require_admin(&state, auth.user_id, auth.organization_id).await {
        return e.into_response();
    }

    let provider = match create_payment_provider(&state) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };

    match provider.sync_invoices(auth.organization_id).await {
        Ok(count) => (
            StatusCode::OK,
            Json(ApiResponse::success(serde_json::json!({ "synced": count }))),
        )
            .into_response(),
        Err(e) => map_payment_error(e).into_response(),
    }
}

/// Create a Stripe Customer Portal session for subscription management.
/// POST /api/payments/portal
///
/// Any authenticated org member can access the portal -- it lets users view
/// their subscription status, update payment methods, and download invoices.
async fn create_billing_portal_session(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let auth = match authenticate(&headers, &state).await {
        Ok(auth) => auth,
        Err(e) => return e.into_response(),
    };

    let provider = match create_payment_provider(&state) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };

    let portal_path = &state.config.stripe_portal_return_url;
    let return_url = if portal_path.starts_with("http://") || portal_path.starts_with("https://") {
        portal_path.clone()
    } else {
        let base = state.config.base_url.trim_end_matches('/');
        let path = portal_path.trim_start_matches('/');
        format!("{}/{}", base, path)
    };
    match provider
        .create_billing_portal_session(auth.organization_id, &return_url)
        .await
    {
        Ok(url) => (
            StatusCode::OK,
            Json(ApiResponse::success(serde_json::json!({ "url": url }))),
        )
            .into_response(),
        Err(e) => map_payment_error(e).into_response(),
    }
}

// ============================================================================
// Webhook Processing Helpers
// ============================================================================

/// PII fields that should be scrubbed from webhook payloads before storage.
/// These fields contain personally identifiable information that shouldn't be
/// retained longer than necessary in orphaned record tables.
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

/// Scrub PII fields from a webhook payload for storage in orphaned records tables.
///
/// # Privacy
/// This function removes sensitive PII fields from webhook payloads before storage
/// to minimize data retention risk. The scrubbed payload retains enough information
/// for debugging (IDs, amounts, statuses) without storing customer PII.
///
/// Fields removed:
/// - email, name, phone (customer contact info)
/// - address, billing_details, shipping (location/address data)
/// - customer_email, customer_name (inline customer references)
/// - receipt_email (payment receipts)
fn scrub_pii_from_payload(payload: &serde_json::Value) -> serde_json::Value {
    match payload {
        serde_json::Value::Object(map) => {
            let mut scrubbed = serde_json::Map::new();
            for (key, value) in map {
                // Skip PII fields entirely
                if PII_FIELDS_TO_SCRUB.contains(&key.as_str()) {
                    scrubbed.insert(
                        key.clone(),
                        serde_json::Value::String("[REDACTED]".to_string()),
                    );
                    continue;
                }
                // Recursively scrub nested objects and arrays
                scrubbed.insert(key.clone(), scrub_pii_from_payload(value));
            }
            serde_json::Value::Object(scrubbed)
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(scrub_pii_from_payload).collect())
        }
        // Primitives (strings, numbers, bools, nulls) pass through unchanged
        other => other.clone(),
    }
}

/// Data for storing an orphaned invoice (one whose customer doesn't exist in our DB).
struct OrphanedInvoiceData<'a> {
    stripe_invoice_id: &'a str,
    customer_id: &'a str,
    invoice_number: &'a str,
    total_cents: i64,
    currency: &'a str,
    status: &'a str,
    period_start: Option<DateTime<chrono::Utc>>,
    period_end: Option<DateTime<chrono::Utc>>,
    paid_at: Option<DateTime<chrono::Utc>>,
    invoice_pdf_url: Option<&'a str>,
    hosted_invoice_url: Option<&'a str>,
    /// Webhook payload with PII already scrubbed
    webhook_payload: serde_json::Value,
}

/// Store an orphaned invoice in the database for manual investigation.
///
/// # Privacy
/// The webhook_payload is scrubbed of PII before being passed to this function.
/// Only non-PII debugging information (IDs, amounts, statuses) is stored.
/// Access to orphaned_invoices table should still be restricted to billing administrators.
/// Records are purged per data retention policy (90 days after resolution).
async fn store_orphaned_invoice(
    db: &DbPool,
    data: &OrphanedInvoiceData<'_>,
) -> Result<(), WebhookProcessingError> {
    let insert_result = sqlx::query(
        r#"
        INSERT INTO orphaned_invoices (
            stripe_invoice_id, stripe_customer_id, invoice_number,
            total_cents, currency, status,
            period_start, period_end, paid_at,
            invoice_pdf_url, hosted_invoice_url, webhook_payload
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (stripe_invoice_id) DO UPDATE
        SET status = EXCLUDED.status,
            total_cents = EXCLUDED.total_cents,
            currency = EXCLUDED.currency,
            period_start = COALESCE(EXCLUDED.period_start, orphaned_invoices.period_start),
            period_end = COALESCE(EXCLUDED.period_end, orphaned_invoices.period_end),
            paid_at = EXCLUDED.paid_at,
            invoice_pdf_url = COALESCE(EXCLUDED.invoice_pdf_url, orphaned_invoices.invoice_pdf_url),
            hosted_invoice_url = COALESCE(EXCLUDED.hosted_invoice_url, orphaned_invoices.hosted_invoice_url),
            webhook_payload = EXCLUDED.webhook_payload,
            updated_at = NOW()
        "#,
    )
    .bind(data.stripe_invoice_id)
    .bind(data.customer_id)
    .bind(data.invoice_number)
    .bind(data.total_cents)
    .bind(data.currency)
    .bind(data.status)
    .bind(data.period_start)
    .bind(data.period_end)
    .bind(data.paid_at)
    .bind(data.invoice_pdf_url)
    .bind(data.hosted_invoice_url)
    .bind(&data.webhook_payload)
    .execute(db)
    .await;

    if let Err(e) = insert_result {
        error!(
            customer_id = %data.customer_id,
            invoice_id = %data.stripe_invoice_id,
            invoice_number = %data.invoice_number,
            total_cents = data.total_cents,
            error = %e,
            "CRITICAL: Failed to insert orphaned invoice into database - returning error for Stripe retry"
        );
        // Database errors during insert are transient (Stripe should retry)
        return Err(WebhookProcessingError::from(e));
    }

    error!(
        customer_id = %data.customer_id,
        invoice_id = %data.stripe_invoice_id,
        invoice_number = %data.invoice_number,
        total_cents = data.total_cents,
        currency = %data.currency,
        status = %data.status,
        "ORPHANED INVOICE: Customer not found in database. Invoice stored in orphaned_invoices table for manual investigation."
    );

    Ok(())
}

/// Process subscription created/updated events.
///
/// Uses a database transaction for consistency when customer is found.
/// This ensures the customer verification and subscription upsert are atomic.
async fn process_subscription_event(
    db: &DbPool,
    clickhouse: &reiver_core::clickhouse_db::ClickHousePool,
    event: &WebhookEvent,
) -> Result<(), WebhookProcessingError> {
    let data = &event.data;

    // Extract subscription data from the event
    // Missing required fields are permanent errors (malformed webhook)
    let subscription = data
        .get("data")
        .and_then(|d| d.get("object"))
        .ok_or_else(|| {
            WebhookProcessingError::permanent(anyhow::anyhow!(
                "Missing subscription object in event"
            ))
        })?;

    let stripe_subscription_id =
        subscription
            .get("id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                WebhookProcessingError::permanent(anyhow::anyhow!("Missing subscription ID"))
            })?;

    let status = subscription.get("status")
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| {
            warn!(event_id = %event.event_id, "Webhook missing 'status' field, defaulting to 'unknown'");
            "unknown"
        });

    let customer_id = subscription
        .get("customer")
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| {
            warn!(event_id = %event.event_id, "Webhook missing 'customer' field");
            ""
        });

    let current_period_start = subscription
        .get("current_period_start")
        .and_then(|v| v.as_i64())
        .and_then(|ts| DateTime::from_timestamp(ts, 0));

    let current_period_end = subscription
        .get("current_period_end")
        .and_then(|v| v.as_i64())
        .and_then(|ts| DateTime::from_timestamp(ts, 0));

    let cancel_at_period_end = subscription
        .get("cancel_at_period_end")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // Extract price_id from subscription items (used for both normal and orphaned cases)
    let price_id = subscription
        .get("items")
        .and_then(|items| items.get("data"))
        .and_then(|data| data.get(0))
        .and_then(|item| item.get("price"))
        .and_then(|price| price.get("id"))
        .and_then(|id| id.as_str());

    // Use a transaction for consistency - verify customer and upsert subscription atomically
    let mut tx = db.begin().await?;

    // First, verify the customer exists to prevent silent failures
    // The INSERT ... SELECT would insert nothing if customer doesn't exist
    let customer_row =
        sqlx::query("SELECT organization_id FROM stripe_customers WHERE stripe_customer_id = $1")
            .bind(customer_id)
            .fetch_optional(&mut *tx)
            .await?;

    let organization_id: Option<Uuid> = customer_row.map(|r| r.get("organization_id"));

    if organization_id.is_none() {
        // Rollback transaction - we're going to the orphaned path
        drop(tx);

        // Customer not found - this is an orphaned subscription event
        // Store for investigation and manual resolution
        //
        // PRIVACY: Scrub PII from webhook payload before storage to minimize data retention risk.
        // Only non-PII debugging information (IDs, amounts, statuses) is retained.
        // Access to orphaned_subscriptions table should still be restricted to billing administrators.
        // Records are purged per data retention policy (90 days after resolution).
        let scrubbed_payload = scrub_pii_from_payload(&event.data);

        let insert_result = sqlx::query(
            r#"
            INSERT INTO orphaned_subscriptions (
                stripe_subscription_id, stripe_customer_id, status,
                current_period_start, current_period_end, cancel_at_period_end,
                price_id, webhook_payload, stripe_event_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (stripe_subscription_id) DO UPDATE
            SET status = EXCLUDED.status,
                current_period_start = EXCLUDED.current_period_start,
                current_period_end = EXCLUDED.current_period_end,
                cancel_at_period_end = EXCLUDED.cancel_at_period_end,
                webhook_payload = EXCLUDED.webhook_payload,
                stripe_event_id = EXCLUDED.stripe_event_id
            "#,
        )
        .bind(stripe_subscription_id)
        .bind(customer_id)
        .bind(status)
        .bind(current_period_start)
        .bind(current_period_end)
        .bind(cancel_at_period_end)
        .bind(price_id)
        .bind(&scrubbed_payload)
        .bind(&event.event_id)
        .execute(db)
        .await;

        if let Err(e) = insert_result {
            // Critical: Failed to store orphaned subscription - this data could be lost
            // Return transient error to trigger Stripe webhook retry
            error!(
                customer_id = %customer_id,
                subscription_id = %stripe_subscription_id,
                status = %status,
                error = %e,
                "CRITICAL: Failed to insert orphaned subscription into database - returning error for Stripe retry"
            );
            return Err(WebhookProcessingError::from(e));
        }

        error!(
            customer_id = %customer_id,
            subscription_id = %stripe_subscription_id,
            status = %status,
            event_id = %event.event_id,
            "ORPHANED SUBSCRIPTION EVENT: Customer not found in database. \
             Subscription stored in orphaned_subscriptions table for manual investigation."
        );

        // Return Ok - the orphaned subscription is now safely stored
        return Ok(());
    }

    let org_id = organization_id.unwrap();

    // Now upsert subscription with verified organization_id (within transaction)
    sqlx::query(
        r#"
        INSERT INTO stripe_subscriptions (
            stripe_subscription_id, stripe_customer_id, status,
            current_period_start, current_period_end, cancel_at_period_end,
            organization_id, price_id
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (stripe_subscription_id) DO UPDATE
        SET status = EXCLUDED.status,
            current_period_start = EXCLUDED.current_period_start,
            current_period_end = EXCLUDED.current_period_end,
            cancel_at_period_end = EXCLUDED.cancel_at_period_end,
            price_id = COALESCE(EXCLUDED.price_id, stripe_subscriptions.price_id),
            updated_at = NOW()
        "#,
    )
    .bind(stripe_subscription_id)
    .bind(customer_id)
    .bind(status)
    .bind(current_period_start)
    .bind(current_period_end)
    .bind(cancel_at_period_end)
    .bind(org_id)
    .bind(price_id)
    .execute(&mut *tx)
    .await?;

    // Commit the transaction
    tx.commit().await?;

    // Sync tier from Stripe price_id (handles changes via Dashboard/Portal)
    if let Some(stripe_price) = price_id {
        let matching_tier: Option<Uuid> = sqlx::query_scalar(
            "SELECT id FROM tier_definitions WHERE stripe_price_id = $1",
        )
        .bind(stripe_price)
        .fetch_optional(db)
        .await
        .unwrap_or(None);

        if let Some(tier_id) = matching_tier {
            let current_tier: Option<Uuid> = sqlx::query_scalar(
                "SELECT tier_definition_id FROM organizations WHERE id = $1",
            )
            .bind(org_id)
            .fetch_optional(db)
            .await
            .unwrap_or(None);

            if current_tier != Some(tier_id) {
                if let Err(e) = sqlx::query(
                    "UPDATE organizations SET tier_definition_id = $1 WHERE id = $2",
                )
                .bind(tier_id)
                .bind(org_id)
                .execute(db)
                .await
                {
                    warn!(
                        organization_id = %org_id,
                        tier_id = %tier_id,
                        error = %e,
                        "Failed to sync tier from webhook price_id"
                    );
                } else {
                    info!(
                        organization_id = %org_id,
                        tier_id = %tier_id,
                        stripe_price = %stripe_price,
                        "Synced org tier from Stripe subscription price_id"
                    );
                }
            }
        }
    }

    let audit_type = if status == "active"
        || matches!(event.event_type, WebhookEventType::SubscriptionCreated)
    {
        AuditEventType::SubscriptionCreated
    } else {
        AuditEventType::SubscriptionUpdated
    };
    let _ = AuditEventBuilder::new(audit_type)
        .organization(org_id)
        .details(serde_json::json!({
            "subscription_id": stripe_subscription_id,
            "status": status,
            "price_id": price_id,
            "source": "stripe_webhook"
        }))
        .success()
        .log(clickhouse)
        .await;

    info!(
        subscription_id = %stripe_subscription_id,
        organization_id = %org_id,
        status = %status,
        "Subscription updated from webhook"
    );

    Ok(())
}

/// Process subscription deleted events.
async fn process_subscription_deleted(
    db: &DbPool,
    clickhouse: &reiver_core::clickhouse_db::ClickHousePool,
    event: &WebhookEvent,
) -> Result<(), WebhookProcessingError> {
    let data = &event.data;

    let subscription = data
        .get("data")
        .and_then(|d| d.get("object"))
        .ok_or_else(|| {
            WebhookProcessingError::permanent(anyhow::anyhow!(
                "Missing subscription object in event"
            ))
        })?;

    let stripe_subscription_id =
        subscription
            .get("id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                WebhookProcessingError::permanent(anyhow::anyhow!("Missing subscription ID"))
            })?;

    let customer_id = subscription.get("customer").and_then(|v| v.as_str());

    let organization_id: Option<Uuid> = if let Some(cid) = customer_id {
        sqlx::query_as::<_, (Uuid,)>(
            "SELECT organization_id FROM stripe_customers WHERE stripe_customer_id = $1",
        )
        .bind(cid)
        .fetch_optional(db)
        .await?
        .map(|(oid,)| oid)
    } else {
        None
    };

    // Mark subscription as canceled
    let result = sqlx::query(
        r#"
        UPDATE stripe_subscriptions
        SET status = 'canceled', canceled_at = NOW(), ended_at = NOW(), updated_at = NOW()
        WHERE stripe_subscription_id = $1
        "#,
    )
    .bind(stripe_subscription_id)
    .execute(db)
    .await?;

    if result.rows_affected() == 0 {
        // Subscription not found in our database - this could indicate:
        // 1. Subscription was created directly in Stripe without going through our API
        // 2. Customer was deleted before subscription
        // 3. Data sync issue between Stripe and our system
        warn!(
            subscription_id = %stripe_subscription_id,
            event_id = %event.event_id,
            "Subscription deletion event received but subscription not found in database - may be orphaned or never synced"
        );
    } else {
        let mut audit = AuditEventBuilder::new(AuditEventType::SubscriptionCanceled)
            .details(serde_json::json!({
                "subscription_id": stripe_subscription_id,
                "source": "stripe_webhook"
            }))
            .success();
        if let Some(oid) = organization_id {
            audit = audit.organization(oid);
        }
        audit.log(clickhouse).await;

        info!(
            subscription_id = %stripe_subscription_id,
            "Subscription deleted from webhook"
        );
    }

    Ok(())
}

/// Process invoice paid events.
async fn process_invoice_paid(
    db: &DbPool,
    clickhouse: &reiver_core::clickhouse_db::ClickHousePool,
    event: &WebhookEvent,
) -> Result<(), WebhookProcessingError> {
    let data = &event.data;

    let invoice = data
        .get("data")
        .and_then(|d| d.get("object"))
        .ok_or_else(|| {
            WebhookProcessingError::permanent(anyhow::anyhow!("Missing invoice object in event"))
        })?;

    let stripe_invoice_id = invoice
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| WebhookProcessingError::permanent(anyhow::anyhow!("Missing invoice ID")))?;

    let invoice_number = invoice
        .get("number")
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| {
            warn!(event_id = %event.event_id, "Webhook missing 'number' field, using invoice ID");
            stripe_invoice_id
        });

    let customer_id = invoice
        .get("customer")
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| {
            warn!(event_id = %event.event_id, "Webhook missing 'customer' field");
            ""
        });

    let total_cents = invoice
        .get("total")
        .and_then(|v| v.as_i64())
        .unwrap_or_else(|| {
            warn!(event_id = %event.event_id, "Webhook missing 'total' field, defaulting to 0");
            0
        });

    let currency = invoice
        .get("currency")
        .and_then(|v| v.as_str())
        .unwrap_or("usd");

    let period_start = invoice
        .get("period_start")
        .and_then(|v| v.as_i64())
        .and_then(|ts| DateTime::from_timestamp(ts, 0));

    let period_end = invoice
        .get("period_end")
        .and_then(|v| v.as_i64())
        .and_then(|ts| DateTime::from_timestamp(ts, 0));

    let invoice_pdf_url = invoice.get("invoice_pdf").and_then(|v| v.as_str());

    let hosted_invoice_url = invoice.get("hosted_invoice_url").and_then(|v| v.as_str());

    // Get organization_id from customer
    let org_row =
        sqlx::query("SELECT organization_id FROM stripe_customers WHERE stripe_customer_id = $1")
            .bind(customer_id)
            .fetch_optional(db)
            .await?;

    let organization_id: Option<Uuid> = org_row.map(|r| r.get("organization_id"));

    if let Some(org_id) = organization_id {
        // Upsert invoice using provider_invoice_id as unique constraint.
        // Stripe invoice IDs (in_xxx) are globally unique and safer than organization_id + invoice_number,
        // which could theoretically conflict if an orphaned invoice is resolved incorrectly.
        sqlx::query(
            r#"
            INSERT INTO invoices (
                organization_id, invoice_number, status, provider, provider_invoice_id,
                total_cents, currency, period_start, period_end, paid_at,
                invoice_pdf_url, hosted_invoice_url
            )
            VALUES ($1, $2, 'paid', 'stripe', $3, $4, $5, $6, $7, NOW(), $8, $9)
            ON CONFLICT (provider_invoice_id) DO UPDATE
            SET status = 'paid',
                total_cents = EXCLUDED.total_cents,
                paid_at = NOW(),
                invoice_pdf_url = EXCLUDED.invoice_pdf_url,
                hosted_invoice_url = EXCLUDED.hosted_invoice_url,
                updated_at = NOW()
            "#,
        )
        .bind(org_id)
        .bind(invoice_number)
        .bind(stripe_invoice_id)
        .bind(total_cents)
        .bind(currency)
        .bind(period_start)
        .bind(period_end)
        .bind(invoice_pdf_url)
        .bind(hosted_invoice_url)
        .execute(db)
        .await?;

        info!(
            invoice_id = %stripe_invoice_id,
            organization_id = %org_id,
            total_cents = total_cents,
            "Invoice paid and recorded"
        );

        let _ = AuditEventBuilder::new(AuditEventType::InvoicePaid)
            .organization(org_id)
            .details(serde_json::json!({
                "invoice_id": stripe_invoice_id,
                "invoice_number": invoice_number,
                "total_cents": total_cents,
                "currency": currency,
                "source": "stripe_webhook"
            }))
            .success()
            .log(clickhouse)
            .await;
    } else {
        // Orphaned invoice - customer not found in our database
        // Store for investigation; if insert fails, return error to trigger Stripe retry
        // Scrub PII from the webhook payload before storage
        let scrubbed_payload = scrub_pii_from_payload(&event.data);
        store_orphaned_invoice(
            db,
            &OrphanedInvoiceData {
                stripe_invoice_id,
                customer_id,
                invoice_number,
                total_cents,
                currency,
                status: "paid",
                period_start,
                period_end,
                paid_at: Some(chrono::Utc::now()),
                invoice_pdf_url,
                hosted_invoice_url,
                webhook_payload: scrubbed_payload,
            },
        )
        .await?;

        // Note: We intentionally don't return an error here because:
        // 1. We don't want Stripe to keep retrying the webhook
        // 2. The payment was successful on Stripe's side
        // 3. The invoice is now stored for manual investigation
    }

    Ok(())
}

/// Process invoice payment failed events.
/// Updates subscription status to 'past_due' and stores the failed invoice.
async fn process_invoice_payment_failed(
    db: &DbPool,
    clickhouse: &reiver_core::clickhouse_db::ClickHousePool,
    event: &WebhookEvent,
) -> Result<(), WebhookProcessingError> {
    let data = &event.data;

    let invoice = data
        .get("data")
        .and_then(|d| d.get("object"))
        .ok_or_else(|| {
            WebhookProcessingError::permanent(anyhow::anyhow!("Missing invoice object in event"))
        })?;

    let stripe_invoice_id = invoice
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| WebhookProcessingError::permanent(anyhow::anyhow!("Missing invoice ID")))?;

    let invoice_number = invoice
        .get("number")
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| {
            warn!(event_id = %event.event_id, "Webhook missing 'number' field, using invoice ID");
            stripe_invoice_id
        });

    let customer_id = invoice
        .get("customer")
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| {
            warn!(event_id = %event.event_id, "Webhook missing 'customer' field");
            ""
        });

    let subscription_id = invoice.get("subscription").and_then(|v| v.as_str());

    let total_cents = invoice
        .get("total")
        .and_then(|v| v.as_i64())
        .unwrap_or_else(|| {
            warn!(event_id = %event.event_id, "Webhook missing 'total' field, defaulting to 0");
            0
        });

    let currency = invoice
        .get("currency")
        .and_then(|v| v.as_str())
        .unwrap_or("usd");

    let attempt_count = invoice
        .get("attempt_count")
        .and_then(|v| v.as_i64())
        .unwrap_or(1);

    let next_payment_attempt = invoice
        .get("next_payment_attempt")
        .and_then(|v| v.as_i64())
        .and_then(|ts| DateTime::from_timestamp(ts, 0));

    // Update subscription status to 'past_due' if there's an associated subscription
    if let Some(sub_id) = subscription_id {
        sqlx::query(
            r#"
            UPDATE stripe_subscriptions
            SET status = 'past_due', updated_at = NOW()
            WHERE stripe_subscription_id = $1 AND status != 'canceled'
            "#,
        )
        .bind(sub_id)
        .execute(db)
        .await?;
    }

    // Get organization_id from customer
    let org_row =
        sqlx::query("SELECT organization_id FROM stripe_customers WHERE stripe_customer_id = $1")
            .bind(customer_id)
            .fetch_optional(db)
            .await?;

    let organization_id: Option<Uuid> = org_row.map(|r| r.get("organization_id"));

    if let Some(org_id) = organization_id {
        // Upsert invoice with failed status using provider_invoice_id as unique constraint.
        // Stripe invoice IDs (in_xxx) are globally unique and safer than organization_id + invoice_number.
        sqlx::query(
            r#"
            INSERT INTO invoices (
                organization_id, invoice_number, status, provider, provider_invoice_id,
                total_cents, currency
            )
            VALUES ($1, $2, 'open', 'stripe', $3, $4, $5)
            ON CONFLICT (provider_invoice_id) DO UPDATE
            SET status = 'open',
                total_cents = EXCLUDED.total_cents,
                updated_at = NOW()
            "#,
        )
        .bind(org_id)
        .bind(invoice_number)
        .bind(stripe_invoice_id)
        .bind(total_cents)
        .bind(currency)
        .execute(db)
        .await?;

        warn!(
            organization_id = %org_id,
            invoice_id = %stripe_invoice_id,
            invoice_number = %invoice_number,
            total_cents = total_cents,
            attempt_count = attempt_count,
            next_attempt = ?next_payment_attempt,
            "Invoice payment failed - subscription marked as past_due"
        );

        let _ = AuditEventBuilder::new(AuditEventType::InvoicePaymentFailed)
            .organization(org_id)
            .details(serde_json::json!({
                "invoice_id": stripe_invoice_id,
                "invoice_number": invoice_number,
                "total_cents": total_cents,
                "attempt_count": attempt_count,
                "source": "stripe_webhook"
            }))
            .success()
            .log(clickhouse)
            .await;
    } else {
        // Orphaned invoice - customer not found
        // Store for investigation; if insert fails, return error to trigger Stripe retry
        let period_start = invoice
            .get("period_start")
            .and_then(|v| v.as_i64())
            .and_then(|ts| DateTime::from_timestamp(ts, 0));

        let period_end = invoice
            .get("period_end")
            .and_then(|v| v.as_i64())
            .and_then(|ts| DateTime::from_timestamp(ts, 0));

        let invoice_pdf_url = invoice.get("invoice_pdf").and_then(|v| v.as_str());

        let hosted_invoice_url = invoice.get("hosted_invoice_url").and_then(|v| v.as_str());

        // Scrub PII from the webhook payload before storage
        let scrubbed_payload = scrub_pii_from_payload(&event.data);
        store_orphaned_invoice(
            db,
            &OrphanedInvoiceData {
                stripe_invoice_id,
                customer_id,
                invoice_number,
                total_cents,
                currency,
                status: "open",
                period_start,
                period_end,
                paid_at: None,
                invoice_pdf_url,
                hosted_invoice_url,
                webhook_payload: scrubbed_payload,
            },
        )
        .await?;
    }

    Ok(())
}

/// Process payment method detached events.
async fn process_payment_method_detached(
    db: &DbPool,
    clickhouse: &reiver_core::clickhouse_db::ClickHousePool,
    event: &WebhookEvent,
) -> Result<(), WebhookProcessingError> {
    let data = &event.data;

    let payment_method = data
        .get("data")
        .and_then(|d| d.get("object"))
        .ok_or_else(|| {
            WebhookProcessingError::permanent(anyhow::anyhow!(
                "Missing payment_method object in event"
            ))
        })?;

    let stripe_pm_id = payment_method
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            WebhookProcessingError::permanent(anyhow::anyhow!("Missing payment method ID"))
        })?;

    let stripe_customer_id = payment_method.get("customer").and_then(|v| v.as_str());

    let organization_id: Option<Uuid> = if let Some(cid) = stripe_customer_id {
        sqlx::query_as::<_, (Uuid,)>(
            "SELECT organization_id FROM stripe_customers WHERE stripe_customer_id = $1",
        )
        .bind(cid)
        .fetch_optional(db)
        .await?
        .map(|(oid,)| oid)
    } else {
        None
    };

    // Mark payment method as canceled
    sqlx::query(
        "UPDATE payment_methods SET status = 'canceled', is_default = false, updated_at = NOW() WHERE provider_payment_method_id = $1"
    )
    .bind(stripe_pm_id)
    .execute(db)
    .await?;

    // If the org now has no default payment method, promote the remaining active one
    if let Some(org_id) = organization_id {
        let has_default: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM payment_methods WHERE organization_id = $1 AND status = 'active' AND is_default = true)"
        )
        .bind(org_id)
        .fetch_one(db)
        .await
        .unwrap_or(false);

        if !has_default {
            let fallback_pm: Option<Uuid> = sqlx::query_scalar(
                "SELECT id FROM payment_methods WHERE organization_id = $1 AND status = 'active' ORDER BY created_at DESC LIMIT 1"
            )
            .bind(org_id)
            .fetch_optional(db)
            .await?;

            if let Some(pm_id) = fallback_pm {
                sqlx::query("UPDATE payment_methods SET is_default = true WHERE id = $1")
                    .bind(pm_id)
                    .execute(db)
                    .await?;
                sqlx::query(
                    "UPDATE organizations SET default_payment_method_id = $1 WHERE id = $2",
                )
                .bind(pm_id)
                .bind(org_id)
                .execute(db)
                .await?;
                info!(organization_id = %org_id, payment_method_id = %pm_id, "Promoted remaining payment method to default");
            }
        }
    }

    let card = payment_method.get("card");
    let card_brand = card.and_then(|c| c.get("brand")).and_then(|v| v.as_str());
    let card_last_four = card.and_then(|c| c.get("last4")).and_then(|v| v.as_str());

    let mut audit = AuditEventBuilder::new(AuditEventType::PaymentMethodRemoved)
        .details(serde_json::json!({
            "deleted": {
                "card_brand": card_brand,
                "card_last_four": card_last_four,
                "source": "stripe_webhook"
            }
        }))
        .success();
    if let Some(oid) = organization_id {
        audit = audit.organization(oid);
    }
    audit.log(clickhouse).await;

    info!(
        payment_method_id = %stripe_pm_id,
        "Payment method detached via webhook"
    );

    Ok(())
}

/// Process payment_method.attached events from Stripe.
///
/// When a payment method is added via the Stripe billing portal (rather than
/// our own setup intent + confirm flow), we only learn about it through this
/// webhook. Without handling it, `has_payment_method` stays false even though
/// the card exists in Stripe.
async fn process_payment_method_attached(
    db: &DbPool,
    clickhouse: &reiver_core::clickhouse_db::ClickHousePool,
    event: &WebhookEvent,
) -> Result<(), WebhookProcessingError> {
    let payment_method = event
        .data
        .get("data")
        .and_then(|d| d.get("object"))
        .ok_or_else(|| {
            WebhookProcessingError::permanent(anyhow::anyhow!(
                "Missing payment_method object in event"
            ))
        })?;

    let stripe_pm_id = payment_method
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            WebhookProcessingError::permanent(anyhow::anyhow!("Missing payment method ID"))
        })?;

    let stripe_customer_id = payment_method
        .get("customer")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            WebhookProcessingError::permanent(anyhow::anyhow!(
                "Missing customer ID on payment method"
            ))
        })?;

    // Look up the organization for this Stripe customer
    let org_row: Option<(Uuid,)> = sqlx::query_as(
        "SELECT organization_id FROM stripe_customers WHERE stripe_customer_id = $1",
    )
    .bind(stripe_customer_id)
    .fetch_optional(db)
    .await?;

    let organization_id = match org_row {
        Some((oid,)) => oid,
        None => {
            debug!(
                customer_id = %stripe_customer_id,
                payment_method_id = %stripe_pm_id,
                "payment_method.attached for unknown customer - skipping"
            );
            return Ok(());
        }
    };

    // Extract card details
    let card = payment_method.get("card");
    let card_brand = card.and_then(|c| c.get("brand")).and_then(|v| v.as_str());
    let card_last_four = card.and_then(|c| c.get("last4")).and_then(|v| v.as_str());
    let card_exp_month = card
        .and_then(|c| c.get("exp_month"))
        .and_then(|v| v.as_i64())
        .map(|v| v as i32);
    let card_exp_year = card
        .and_then(|c| c.get("exp_year"))
        .and_then(|v| v.as_i64())
        .map(|v| v as i32);
    let display_name = card_brand
        .zip(card_last_four)
        .map(|(brand, last4)| format!("{} ending in {}", brand, last4));

    // Check if we already have an active payment method for this org
    let has_existing: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM payment_methods WHERE organization_id = $1 AND status = 'active')"
    )
    .bind(organization_id)
    .fetch_one(db)
    .await
    .unwrap_or(false);

    let is_default = !has_existing;

    let mut tx = db.begin().await?;

    if is_default {
        sqlx::query("UPDATE payment_methods SET is_default = false WHERE organization_id = $1")
            .bind(organization_id)
            .execute(&mut *tx)
            .await?;
    }

    sqlx::query(
        r#"
        INSERT INTO payment_methods (
            organization_id, provider, status,
            provider_customer_id, provider_payment_method_id,
            display_name, card_brand, card_last_four, card_exp_month, card_exp_year,
            is_default
        )
        VALUES ($1, 'stripe', 'active', $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (provider_payment_method_id) WHERE provider_payment_method_id IS NOT NULL DO UPDATE
        SET status = 'active',
            display_name = EXCLUDED.display_name,
            card_brand = EXCLUDED.card_brand,
            card_last_four = EXCLUDED.card_last_four,
            card_exp_month = EXCLUDED.card_exp_month,
            card_exp_year = EXCLUDED.card_exp_year,
            is_default = EXCLUDED.is_default,
            updated_at = NOW()
        "#,
    )
    .bind(organization_id)
    .bind(stripe_customer_id)
    .bind(stripe_pm_id)
    .bind(display_name.as_deref())
    .bind(card_brand)
    .bind(card_last_four)
    .bind(card_exp_month)
    .bind(card_exp_year)
    .bind(is_default)
    .execute(&mut *tx)
    .await?;

    if is_default {
        let pm_id: Option<Uuid> = sqlx::query_scalar(
            "SELECT id FROM payment_methods WHERE provider_payment_method_id = $1",
        )
        .bind(stripe_pm_id)
        .fetch_optional(&mut *tx)
        .await?;

        if let Some(pm_id) = pm_id {
            sqlx::query("UPDATE organizations SET default_payment_method_id = $1 WHERE id = $2")
                .bind(pm_id)
                .bind(organization_id)
                .execute(&mut *tx)
                .await?;
        }
    }

    tx.commit().await?;

    let _ = AuditEventBuilder::new(AuditEventType::PaymentMethodAdded)
        .organization(organization_id)
        .details(serde_json::json!({
            "created": {
                "card_brand": card_brand,
                "card_last_four": card_last_four,
                "is_default": is_default,
                "source": "stripe_webhook"
            }
        }))
        .success()
        .log(clickhouse)
        .await;

    info!(
        organization_id = %organization_id,
        payment_method_id = %stripe_pm_id,
        is_default = is_default,
        "Payment method stored from webhook"
    );

    Ok(())
}

/// Process customer deleted events.
///
/// When a customer is deleted in Stripe, we need to:
/// Sync default payment method when the customer is updated in Stripe.
///
/// Stripe fires `customer.updated` when `invoice_settings.default_payment_method`
/// changes (e.g. user sets a new default via Stripe's billing portal).
async fn process_customer_updated(
    db: &DbPool,
    _clickhouse: &reiver_core::clickhouse_db::ClickHousePool,
    event: &WebhookEvent,
) -> Result<(), WebhookProcessingError> {
    let customer = event
        .data
        .get("data")
        .and_then(|d| d.get("object"))
        .ok_or_else(|| {
            WebhookProcessingError::permanent(anyhow::anyhow!("Missing customer object in event"))
        })?;

    let stripe_customer_id = customer
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| WebhookProcessingError::permanent(anyhow::anyhow!("Missing customer ID")))?;

    let default_pm_stripe_id = customer
        .get("invoice_settings")
        .and_then(|s| s.get("default_payment_method"))
        .and_then(|v| v.as_str());

    let default_pm_stripe_id = match default_pm_stripe_id {
        Some(id) if !id.is_empty() => id,
        _ => return Ok(()),
    };

    let org_row: Option<(Uuid,)> = sqlx::query_as(
        "SELECT organization_id FROM stripe_customers WHERE stripe_customer_id = $1",
    )
    .bind(stripe_customer_id)
    .fetch_optional(db)
    .await?;

    let organization_id = match org_row {
        Some((oid,)) => oid,
        None => return Ok(()),
    };

    let mut tx = db.begin().await?;

    // Clear existing defaults for this org
    sqlx::query("UPDATE payment_methods SET is_default = false WHERE organization_id = $1")
        .bind(organization_id)
        .execute(&mut *tx)
        .await?;

    // Set the Stripe-designated default
    let pm_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM payment_methods WHERE provider_payment_method_id = $1 AND status = 'active'"
    )
    .bind(default_pm_stripe_id)
    .fetch_optional(&mut *tx)
    .await?;

    if let Some(pm_id) = pm_id {
        sqlx::query("UPDATE payment_methods SET is_default = true WHERE id = $1")
            .bind(pm_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE organizations SET default_payment_method_id = $1 WHERE id = $2")
            .bind(pm_id)
            .bind(organization_id)
            .execute(&mut *tx)
            .await?;
        info!(
            organization_id = %organization_id,
            payment_method_id = %pm_id,
            stripe_pm = %default_pm_stripe_id,
            "Synced default payment method from Stripe customer.updated"
        );
    } else {
        debug!(
            stripe_pm = %default_pm_stripe_id,
            "Default payment method from Stripe not found locally — may arrive via payment_method.attached"
        );
    }

    tx.commit().await?;
    Ok(())
}

/// 1. Mark all their payment methods as canceled
/// 2. Mark all their subscriptions as canceled
/// 3. Remove the customer record from our database
///
/// This prevents orphaned data when customers are deleted directly in Stripe.
async fn process_customer_deleted(
    db: &DbPool,
    clickhouse: &reiver_core::clickhouse_db::ClickHousePool,
    event: &WebhookEvent,
) -> Result<(), WebhookProcessingError> {
    let data = &event.data;

    let customer = data
        .get("data")
        .and_then(|d| d.get("object"))
        .ok_or_else(|| {
            WebhookProcessingError::permanent(anyhow::anyhow!("Missing customer object in event"))
        })?;

    let stripe_customer_id = customer
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| WebhookProcessingError::permanent(anyhow::anyhow!("Missing customer ID")))?;

    // Look up the organization for this customer
    let org_row =
        sqlx::query("SELECT organization_id FROM stripe_customers WHERE stripe_customer_id = $1")
            .bind(stripe_customer_id)
            .fetch_optional(db)
            .await?;

    let organization_id: Option<Uuid> = org_row.map(|r| r.get("organization_id"));

    if organization_id.is_none() {
        // Customer not found in our database - nothing to clean up
        debug!(
            customer_id = %stripe_customer_id,
            "Customer deleted event received but customer not found in database - may already be deleted"
        );
        return Ok(());
    }

    let org_id = organization_id.unwrap();

    // Use a transaction to ensure all cleanup is atomic
    let mut tx = db.begin().await?;

    // 1. Mark all payment methods for this customer as canceled
    let pm_result = sqlx::query(
        r#"
        UPDATE payment_methods 
        SET status = 'canceled', updated_at = NOW() 
        WHERE provider_customer_id = $1 AND status != 'canceled'
        "#,
    )
    .bind(stripe_customer_id)
    .execute(&mut *tx)
    .await?;

    // 2. Mark all subscriptions for this customer as canceled
    let sub_result = sqlx::query(
        r#"
        UPDATE stripe_subscriptions 
        SET status = 'canceled', canceled_at = NOW(), ended_at = NOW(), updated_at = NOW() 
        WHERE stripe_customer_id = $1 AND status != 'canceled'
        "#,
    )
    .bind(stripe_customer_id)
    .execute(&mut *tx)
    .await?;

    // 3. Clear the default payment method on the organization
    sqlx::query("UPDATE organizations SET default_payment_method_id = NULL WHERE id = $1")
        .bind(org_id)
        .execute(&mut *tx)
        .await?;

    // 4. Delete the customer record
    sqlx::query("DELETE FROM stripe_customers WHERE stripe_customer_id = $1")
        .bind(stripe_customer_id)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;

    info!(
        organization_id = %org_id,
        customer_id = %stripe_customer_id,
        payment_methods_canceled = pm_result.rows_affected(),
        subscriptions_canceled = sub_result.rows_affected(),
        "Customer deleted - cleaned up associated data"
    );

    let _ = AuditEventBuilder::new(AuditEventType::PaymentMethodRemoved)
        .organization(org_id)
        .details(serde_json::json!({
            "customer_deleted": true,
            "payment_methods_canceled": pm_result.rows_affected(),
            "subscriptions_canceled": sub_result.rows_affected(),
            "source": "stripe_webhook"
        }))
        .success()
        .log(clickhouse)
        .await;

    Ok(())
}

// ============================================================================
// Webhook IP Validation
// ============================================================================

/// Check if a client IP is in the allowed Stripe IP ranges.
///
/// # Arguments
/// * `client_ip` - The client IP address as a string
/// * `allowed_ranges` - List of allowed CIDR ranges (e.g., "3.18.12.63/32")
///
/// # Input Sanitization
/// - Empty strings are skipped
/// - Leading/trailing whitespace is trimmed from each entry
/// - Invalid CIDR notation is skipped with a warning logged at first occurrence
///
/// # Security
/// This function is constant-time with respect to the allowlist - it always iterates
/// through ALL entries before returning. This prevents timing side-channels that could
/// reveal information about the allowlist structure.
///
/// # Returns
/// `true` if IP is in an allowed range, `false` otherwise
fn is_ip_in_allowlist(client_ip: &str, allowed_ranges: &[String]) -> bool {
    use std::net::IpAddr;

    // Trim and validate client IP
    let client_ip = client_ip.trim();
    if client_ip.is_empty() {
        warn!("Empty client IP provided for allowlist check");
        return false;
    }

    // Parse the client IP
    let ip: IpAddr = match client_ip.parse() {
        Ok(ip) => ip,
        Err(_) => {
            warn!(client_ip = %client_ip, "Failed to parse client IP for allowlist check");
            return false;
        }
    };

    // Track if we found a match - we continue checking all entries for constant-time behavior
    let mut found = false;

    for range in allowed_ranges {
        // Sanitize input: trim whitespace and skip empty entries
        let range = range.trim();
        if range.is_empty() {
            continue;
        }

        // Parse CIDR notation: "IP/prefix_len" or just "IP" (treated as /32 or /128)
        let (range_ip_str, prefix_len) = if let Some((ip_part, prefix_part)) = range.split_once('/')
        {
            let ip_part = ip_part.trim();
            let prefix_part = prefix_part.trim();

            let prefix: u8 = match prefix_part.parse() {
                Ok(p) => p,
                Err(_) => {
                    debug!(range = %range, "Invalid CIDR prefix in allowlist entry - skipping");
                    continue;
                }
            };
            (ip_part, prefix)
        } else {
            // No prefix - treat as single IP (/32 for IPv4, /128 for IPv6)
            let prefix = if range.contains(':') { 128 } else { 32 };
            (range, prefix)
        };

        let range_ip: IpAddr = match range_ip_str.parse() {
            Ok(ip) => ip,
            Err(_) => {
                debug!(range = %range, "Invalid IP address in allowlist entry - skipping");
                continue;
            }
        };

        // Check if IPs are same family
        match (ip, range_ip) {
            (IpAddr::V4(client), IpAddr::V4(range)) => {
                if prefix_len > 32 {
                    debug!(range = %range, prefix_len = prefix_len, "IPv4 prefix length > 32 - skipping");
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
                    // Don't return early - continue checking all ranges for constant-time behavior
                    found = true;
                }
            }
            (IpAddr::V6(client), IpAddr::V6(range)) => {
                if prefix_len > 128 {
                    debug!(range = %range, prefix_len = prefix_len, "IPv6 prefix length > 128 - skipping");
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
                    // Don't return early - continue checking all ranges for constant-time behavior
                    found = true;
                }
            }
            _ => continue, // IPv4/IPv6 mismatch
        }
    }

    found
}

// ============================================================================
// Webhook Endpoint
// ============================================================================

/// Handle Stripe webhooks.
/// POST /api/payments/webhook
///
/// # Security
/// - Body size limited to 64KB to prevent DoS attacks
/// - Rate limited to prevent DoS attacks (100/min, 1000/hour per IP)
/// - Optional IP allowlisting for defense-in-depth
/// - Verifies Stripe signature before processing
/// - Stores events for idempotency (prevents replay attacks)
/// - Does not require authentication (signature is auth)
async fn handle_stripe_webhook(
    State(state): State<Arc<WebsiteState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // Validate content-type header (defense-in-depth)
    // Accept only "application/json" or "application/json; charset=..." (with optional params)
    // Reject variants like "application/json-malicious" or "text/application/json"
    // Note: Media types are case-insensitive per HTTP spec (RFC 7231)
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let is_valid_content_type = {
        let ct = content_type.trim().to_lowercase();
        ct == "application/json" || ct.starts_with("application/json;")
    };

    if !is_valid_content_type {
        trace!(content_type = %content_type, "Webhook received with invalid content-type");
        return (
            StatusCode::BAD_REQUEST,
            Json(api_error("Invalid content type")),
        )
            .into_response();
    }

    // Convert body bytes to string (Stripe webhooks are always UTF-8 JSON)
    let body = match String::from_utf8(body.to_vec()) {
        Ok(s) => s,
        Err(_) => {
            trace!("Webhook body is not valid UTF-8");
            return (
                StatusCode::BAD_REQUEST,
                Json(api_error("Invalid request body")),
            )
                .into_response();
        }
    };

    // Rate limit webhook requests per IP to prevent DoS attacks
    let client_ip = extract_client_ip(&addr);
    if let Err(e) = check_webhook_rate_limit(&state.redis, &client_ip, "stripe_webhook").await {
        warn!(client_ip = %client_ip, error = %e, "Webhook rate limit exceeded");
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(api_error("Rate limit exceeded")),
        )
            .into_response();
    }

    // Optional: Validate webhook source IP against allowlist (defense-in-depth)
    if state.config.stripe_webhook_ip_allowlist_enabled {
        if !is_ip_in_allowlist(&client_ip, &state.config.stripe_webhook_ip_allowlist) {
            warn!(
                client_ip = %client_ip,
                "Webhook request from IP not in allowlist - rejecting"
            );
            return (StatusCode::FORBIDDEN, Json(api_error("Forbidden"))).into_response();
        }
        trace!(client_ip = %client_ip, "Webhook IP verified against allowlist");
    }

    // Get signature from headers
    let signature = match headers.get("stripe-signature") {
        Some(sig) => match sig.to_str() {
            Ok(s) => s,
            Err(_) => {
                trace!("Stripe signature header contains invalid characters");
                return (
                    StatusCode::BAD_REQUEST,
                    Json(api_error("Invalid signature header")),
                )
                    .into_response();
            }
        },
        None => {
            trace!("Missing Stripe signature in webhook");
            return (
                StatusCode::BAD_REQUEST,
                Json(api_error("Missing signature")),
            )
                .into_response();
        }
    };

    let provider = match create_payment_provider(&state) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };

    // Verify and parse webhook
    let event = match provider.verify_webhook(&body, signature).await {
        Ok(Some(event)) => event,
        Ok(None) => {
            // Duplicate event - return success to Stripe
            return (StatusCode::OK, Json(ApiResponse::success(()))).into_response();
        }
        Err(PaymentError::InvalidWebhookSignature) => {
            warn!("Invalid webhook signature received");
            return (
                StatusCode::BAD_REQUEST,
                Json(api_error("Invalid signature")),
            )
                .into_response();
        }
        Err(e) => return map_payment_error(e).into_response(),
    };

    // Process event based on type
    let db = state.db.as_ref();
    let ch = state.clickhouse.as_ref();
    let result: Result<(), WebhookProcessingError> = match event.event_type {
        WebhookEventType::SubscriptionCreated => {
            info!(event_id = %event.event_id, "Processing subscription.created");
            process_subscription_event(db, ch, &event).await
        }
        WebhookEventType::SubscriptionUpdated => {
            info!(event_id = %event.event_id, "Processing subscription.updated");
            process_subscription_event(db, ch, &event).await
        }
        WebhookEventType::SubscriptionDeleted => {
            info!(event_id = %event.event_id, "Processing subscription.deleted");
            process_subscription_deleted(db, ch, &event).await
        }
        WebhookEventType::InvoicePaid => {
            info!(event_id = %event.event_id, "Processing invoice.paid");
            process_invoice_paid(db, ch, &event).await
        }
        WebhookEventType::InvoicePaymentFailed => {
            warn!(event_id = %event.event_id, "Processing invoice.payment_failed");
            process_invoice_payment_failed(db, ch, &event).await
        }
        WebhookEventType::PaymentMethodAttached => {
            info!(event_id = %event.event_id, "Processing payment_method.attached");
            process_payment_method_attached(db, ch, &event).await
        }
        WebhookEventType::PaymentMethodDetached => {
            trace!(event_id = %event.event_id, "Processing payment method detached");
            process_payment_method_detached(db, ch, &event).await
        }
        WebhookEventType::CustomerUpdated => {
            info!(event_id = %event.event_id, "Processing customer.updated");
            process_customer_updated(db, ch, &event).await
        }
        WebhookEventType::CustomerDeleted => {
            info!(event_id = %event.event_id, "Processing customer.deleted");
            process_customer_deleted(db, ch, &event).await
        }
        WebhookEventType::CheckoutSessionCompleted => {
            info!(event_id = %event.event_id, "Processing checkout.session.completed");
            process_checkout_session_completed(&state, &event).await
        }
        WebhookEventType::Unknown(ref event_type) => {
            // Log unhandled event types at debug level for monitoring.
            // This helps identify event types that may need handlers in the future.
            // Common unhandled events include: payment_intent.*, charge.*, checkout.session.*
            debug!(
                event_id = %event.event_id,
                event_type = %event_type,
                organization_id = ?event.organization_id,
                "Received unhandled Stripe webhook event type - no action taken"
            );
            Ok(())
        }
    };

    // Handle errors with proper retry semantics using typed errors
    // This avoids fragile string matching and makes retry behavior explicit
    match result {
        Ok(()) => {
            // Success - mark as processed and return 200
            let _ = provider.mark_event_processed(&event.event_id, None).await;
            (StatusCode::OK, Json(ApiResponse::success(()))).into_response()
        }
        Err(ref e) => {
            let error_msg = e.message();

            if e.is_retriable() {
                // Transient error - return 500 so Stripe will retry
                // Do NOT mark as processed so it can be reprocessed
                error!(
                    event_id = %event.event_id,
                    event_type = ?event.event_type,
                    error_type = "transient",
                    "Transient error processing webhook - returning 500 for Stripe retry"
                );
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(api_error("Temporary error, please retry")),
                )
                    .into_response()
            } else {
                // Permanent error - log, mark as processed with error, return 200
                // Returning 200 prevents infinite retries for errors that won't resolve
                error!(
                    event_id = %event.event_id,
                    event_type = ?event.event_type,
                    error_type = "permanent",
                    "Permanent error processing webhook - event logged for investigation"
                );
                let _ = provider
                    .mark_event_processed(&event.event_id, Some(&error_msg))
                    .await;
                (StatusCode::OK, Json(ApiResponse::success(()))).into_response()
            }
        }
    }
}

// ============================================================================
// Flow Credits Endpoints
// ============================================================================

/// Returns the number of decimal places for a Stripe currency's smallest unit.
/// Stripe sends `amount_total` in the smallest currency unit: cents for USD (2),
/// whole yen for JPY (0), fils for BHD (3), etc.
/// See: https://docs.stripe.com/currencies#zero-decimal
#[allow(dead_code)]
fn stripe_currency_decimals(currency: &str) -> u32 {
    match currency.to_lowercase().as_str() {
        // Zero-decimal currencies
        "bif" | "clp" | "djf" | "gnf" | "jpy" | "kmf" | "krw" | "mga" | "pyg" | "rwf" | "ugx"
        | "vnd" | "vuv" | "xaf" | "xof" | "xpf" => 0,
        // Three-decimal currencies
        "bhd" | "jod" | "kwd" | "omr" | "tnd" => 3,
        // Everything else is two-decimal (cents)
        _ => 2,
    }
}

#[derive(Deserialize)]
struct CreditsCheckoutRequest {
    credit_amount_usd: rust_decimal::Decimal,
    success_url: String,
    cancel_url: String,
}

#[derive(Serialize)]
struct CreditsCheckoutResponse {
    checkout_url: String,
}

/// POST /api/payments/credits/checkout
///
/// Creates a Stripe Checkout Session for purchasing Flow credits.
/// The user pays `credit_amount_usd * (1 + gateway_fee_percent)` where
/// the fee is resolved from the org's tier. On success, the wallet is
/// credited with `credit_amount_usd`.
async fn create_credits_checkout(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(req): Json<CreditsCheckoutRequest>,
) -> impl IntoResponse {
    let auth = match authenticate(&headers, &state).await {
        Ok(a) => a,
        Err(e) => return e.into_response(),
    };

    if let Err(e) = require_admin(&state, auth.user_id, auth.organization_id).await {
        return e.into_response();
    }

    let org_id = auth.organization_id;

    let min_credit = rust_decimal::Decimal::new(1, 0); // $1
    let max_credit = rust_decimal::Decimal::new(10_000, 0); // $10,000

    if req.credit_amount_usd < min_credit {
        return (
            StatusCode::BAD_REQUEST,
            Json(api_error(&format!(
                "Minimum credit purchase is ${min_credit}"
            ))),
        )
            .into_response();
    }

    if req.credit_amount_usd > max_credit {
        return (
            StatusCode::BAD_REQUEST,
            Json(api_error(&format!(
                "Maximum credit purchase is ${max_credit}"
            ))),
        )
            .into_response();
    }

    let api_key = match state.config.stripe_api_key.as_ref() {
        Some(k) => k,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(api_error("Payment system not configured")),
            )
                .into_response()
        }
    };

    let gateway_rate = match reiver_core::billing::credits::get_gateway_fee_rate(
        state.entitlements.as_ref(),
        org_id,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            error!(error = %e, "Failed to resolve gateway fee rate");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(api_error("Failed to resolve pricing")),
            )
                .into_response();
        }
    };
    let multiplier = rust_decimal::Decimal::ONE + gateway_rate;
    let charge_amount = req.credit_amount_usd * multiplier;
    let charge_cents_decimal = (charge_amount * rust_decimal::Decimal::new(100, 0)).round_dp(0);
    let charge_cents = match charge_cents_decimal.to_i64() {
        Some(c) if c > 0 => c,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(api_error("Invalid charge amount")),
            )
                .into_response();
        }
    };

    let client = stripe::Client::new(api_key);

    let metadata: std::collections::HashMap<String, String> = [
        (String::from("organization_id"), org_id.to_string()),
        (String::from("type"), String::from("credit_purchase")),
        (
            String::from("credit_amount_usd"),
            req.credit_amount_usd.to_string(),
        ),
    ]
    .into_iter()
    .collect();

    use stripe_checkout::checkout_session as cs;

    let mut price_data = cs::CreateCheckoutSessionLineItemsPriceData::new(
        stripe_types::Currency::USD,
    );
    price_data.unit_amount = Some(charge_cents);
    price_data.product_data = Some(cs::ProductData::new(format!(
        "Flow Credits - ${}",
        req.credit_amount_usd
    )));

    let result = cs::CreateCheckoutSession::new()
        .mode(stripe_shared::CheckoutSessionMode::Payment)
        .success_url(&req.success_url)
        .cancel_url(&req.cancel_url)
        .metadata(metadata)
        .line_items(vec![cs::CreateCheckoutSessionLineItems {
            price_data: Some(price_data),
            quantity: Some(1),
            ..Default::default()
        }])
        .send(&client)
        .await;

    match result {
        Ok(session) => {
            let url = session.url.unwrap_or_default();
            info!(
                organization_id = %org_id,
                credit_amount = %req.credit_amount_usd,
                charge_amount = %charge_amount,
                "Created Stripe Checkout Session for credit purchase"
            );
            (
                StatusCode::OK,
                Json(ApiResponse::success(CreditsCheckoutResponse {
                    checkout_url: url,
                })),
            )
                .into_response()
        }
        Err(e) => {
            error!(error = %e, "Failed to create Stripe Checkout Session");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(api_error("Failed to create checkout session")),
            )
                .into_response()
        }
    }
}

/// Process a checkout.session.completed webhook event for credit purchases.
async fn process_checkout_session_completed(
    state: &WebsiteState,
    event: &WebhookEvent,
) -> Result<(), WebhookProcessingError> {
    let session_data = event
        .data
        .get("data")
        .and_then(|d| d.get("object"))
        .ok_or_else(|| {
            WebhookProcessingError::permanent(anyhow::anyhow!(
                "Missing session data in checkout event"
            ))
        })?;

    let metadata = session_data.get("metadata").ok_or_else(|| {
        WebhookProcessingError::permanent(anyhow::anyhow!("Missing metadata in checkout session"))
    })?;

    let purchase_type = metadata.get("type").and_then(|v| v.as_str()).unwrap_or("");

    if purchase_type != "credit_purchase" {
        debug!(
            event_id = %event.event_id,
            purchase_type = %purchase_type,
            "Checkout session is not a credit purchase, skipping"
        );
        return Ok(());
    }

    let payment_status = session_data
        .get("payment_status")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if payment_status != "paid" {
        warn!(
            event_id = %event.event_id,
            payment_status = %payment_status,
            "Checkout session payment_status is not 'paid', skipping credit grant"
        );
        return Ok(());
    }

    let org_id_str = metadata
        .get("organization_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            WebhookProcessingError::permanent(anyhow::anyhow!(
                "Missing organization_id in checkout metadata"
            ))
        })?;

    let org_id = Uuid::parse_str(org_id_str).map_err(|_| {
        WebhookProcessingError::permanent(anyhow::anyhow!("Invalid organization_id in metadata"))
    })?;

    let credit_amount_str = metadata
        .get("credit_amount_usd")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            WebhookProcessingError::permanent(anyhow::anyhow!(
                "Missing credit_amount_usd in metadata"
            ))
        })?;

    let credit_amount: rust_decimal::Decimal = credit_amount_str.parse().map_err(|_| {
        WebhookProcessingError::permanent(anyhow::anyhow!("Invalid credit_amount_usd value"))
    })?;

    let session_id = session_data
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    // Look up the Stripe customer ID for this organization
    let stripe_customer_id: String = sqlx::query_scalar(
        "SELECT stripe_customer_id FROM stripe_customers WHERE organization_id = $1 LIMIT 1",
    )
    .bind(org_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| WebhookProcessingError::transient(anyhow::anyhow!("DB error: {}", e)))?
    .ok_or_else(|| {
        WebhookProcessingError::permanent(anyhow::anyhow!(
            "No Stripe customer found for organization {}",
            org_id
        ))
    })?;

    // Convert credit amount to cents for Stripe
    let credit_cents = (credit_amount * rust_decimal::Decimal::from(100))
        .round_dp(0)
        .to_i64()
        .unwrap_or(0);

    // Create a Stripe Credit Grant instead of adding to custom wallet
    use stripe_billing::billing_credit_grant::{
        CreateBillingCreditGrant, CreateBillingCreditGrantAmount,
        CreateBillingCreditGrantAmountMonetary, CreateBillingCreditGrantAmountType,
        CreateBillingCreditGrantApplicabilityConfig,
        CreateBillingCreditGrantApplicabilityConfigScope,
        CreateBillingCreditGrantApplicabilityConfigScopePriceType,
    };

    let amount = CreateBillingCreditGrantAmount {
        monetary: Some(CreateBillingCreditGrantAmountMonetary::new(
            stripe_types::Currency::USD,
            credit_cents,
        )),
        type_: CreateBillingCreditGrantAmountType::Monetary,
    };

    let scope = CreateBillingCreditGrantApplicabilityConfigScope {
        price_type: Some(
            CreateBillingCreditGrantApplicabilityConfigScopePriceType::Metered,
        ),
        prices: None,
    };

    let applicability = CreateBillingCreditGrantApplicabilityConfig::new(scope);

    let client = state.stripe_client.as_ref().ok_or_else(|| {
        WebhookProcessingError::permanent(anyhow::anyhow!("Stripe not configured"))
    })?;

    let mut metadata_map = std::collections::HashMap::new();
    metadata_map.insert("checkout_session_id".to_string(), session_id.to_string());
    metadata_map.insert("organization_id".to_string(), org_id.to_string());

    use stripe::StripeRequest;

    let idempotency_key = stripe::IdempotencyKey::new(format!("credit_grant_{}", session_id))
        .map_err(|e| {
            WebhookProcessingError::permanent(anyhow::anyhow!("Invalid idempotency key: {}", e))
        })?;

    CreateBillingCreditGrant::new(amount, applicability)
        .customer(&stripe_customer_id)
        .category(stripe_shared::BillingCreditGrantCategory::Paid)
        .metadata(metadata_map)
        .customize()
        .request_strategy(stripe::RequestStrategy::Idempotent(idempotency_key))
        .send(client)
        .await
        .map_err(|e| {
            WebhookProcessingError::transient(anyhow::anyhow!(
                "Failed to create Stripe credit grant: {}",
                e
            ))
        })?;

    info!(
        organization_id = %org_id,
        credit_amount_usd = %credit_amount,
        credit_cents = credit_cents,
        stripe_session_id = %session_id,
        "Stripe Credit Grant created via checkout"
    );

    let _ = AuditEventBuilder::new(AuditEventType::SubscriptionCreated)
        .organization(org_id)
        .details(serde_json::json!({
            "credit_purchase": true,
            "credit_amount_usd": credit_amount.to_string(),
            "stripe_session_id": session_id,
            "source": "stripe_webhook"
        }))
        .success()
        .log(state.clickhouse.as_ref())
        .await;

    Ok(())
}

// ============================================================================
// Credit Balance & Transaction API Endpoints
// ============================================================================

/// GET /api/payments/credits/balance
async fn get_credit_balance(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let auth = match authenticate(&headers, &state).await {
        Ok(a) => a,
        Err(e) => return e.into_response(),
    };

    let org_id = auth.organization_id;

    let client = match state.stripe_client.as_ref() {
        Some(c) => c,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(api_error("Payment system not configured")),
            )
                .into_response()
        }
    };

    let stripe_customer_id: Option<String> = sqlx::query_scalar(
        "SELECT stripe_customer_id FROM stripe_customers WHERE organization_id = $1 LIMIT 1",
    )
    .bind(org_id)
    .fetch_optional(&*state.db)
    .await
    .unwrap_or(None);

    let stripe_customer_id = match stripe_customer_id {
        Some(id) => id,
        None => {
            return (
                StatusCode::OK,
                Json(serde_json::json!({
                    "balance_usd": "0",
                    "balance_cents": 0,
                    "organization_id": org_id,
                })),
            )
                .into_response()
        }
    };

    use stripe_billing::billing_credit_balance_summary::{
        RetrieveForMyAccountBillingCreditBalanceSummary,
        RetrieveForMyAccountBillingCreditBalanceSummaryFilter,
        RetrieveForMyAccountBillingCreditBalanceSummaryFilterType,
    };

    let filter = RetrieveForMyAccountBillingCreditBalanceSummaryFilter::new(
        RetrieveForMyAccountBillingCreditBalanceSummaryFilterType::ApplicabilityScope,
    );

    match RetrieveForMyAccountBillingCreditBalanceSummary::new(filter)
        .customer(&stripe_customer_id)
        .send(client)
        .await
    {
        Ok(summary) => {
            let balance_cents: i64 = summary
                .balances
                .iter()
                .filter_map(|b| b.available_balance.monetary.as_ref())
                .map(|m| m.value)
                .sum();
            let balance_usd =
                rust_decimal::Decimal::new(balance_cents, 2);
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "balance_usd": balance_usd.to_string(),
                    "balance_cents": balance_cents,
                    "organization_id": org_id,
                })),
            )
                .into_response()
        }
        Err(e) => {
            error!(error = %e, "Failed to get credit balance from Stripe");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(api_error("Failed to get credit balance")),
            )
                .into_response()
        }
    }
}

#[derive(Deserialize)]
struct TransactionQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    #[serde(default)]
    #[allow(dead_code)]
    offset: i64,
}

fn default_limit() -> i64 {
    50
}

/// GET /api/payments/credits/transactions
async fn list_credit_transactions(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Query(query): Query<TransactionQuery>,
) -> impl IntoResponse {
    let auth = match authenticate(&headers, &state).await {
        Ok(a) => a,
        Err(e) => return e.into_response(),
    };

    let org_id = auth.organization_id;

    let client = match state.stripe_client.as_ref() {
        Some(c) => c,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(api_error("Payment system not configured")),
            )
                .into_response()
        }
    };

    let stripe_customer_id: Option<String> = sqlx::query_scalar(
        "SELECT stripe_customer_id FROM stripe_customers WHERE organization_id = $1 LIMIT 1",
    )
    .bind(org_id)
    .fetch_optional(&*state.db)
    .await
    .unwrap_or(None);

    let stripe_customer_id = match stripe_customer_id {
        Some(id) => id,
        None => {
            return (
                StatusCode::OK,
                Json(serde_json::json!({
                    "transactions": [],
                    "count": 0,
                    "has_more": false,
                })),
            )
                .into_response()
        }
    };

    use stripe_billing::billing_credit_balance_transaction::ListBillingCreditBalanceTransaction;

    let limit = query.limit.min(100);

    match ListBillingCreditBalanceTransaction::new()
        .customer(&stripe_customer_id)
        .limit(limit as i64)
        .send(client)
        .await
    {
        Ok(list) => {
            let transactions: Vec<serde_json::Value> = list
                .data
                .iter()
                .map(|tx| {
                    serde_json::json!({
                        "id": tx.id.as_str(),
                        "type": tx.type_.as_ref().map(|t| t.as_str()).unwrap_or("unknown"),
                        "created": tx.created,
                        "credit_grant": tx.credit_grant.id(),
                    })
                })
                .collect();
            let count = transactions.len() as i64;
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "transactions": transactions,
                    "count": count,
                    "has_more": list.has_more,
                })),
            )
                .into_response()
        }
        Err(e) => {
            error!(error = %e, "Failed to list credit transactions from Stripe");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(api_error("Failed to list transactions")),
            )
                .into_response()
        }
    }
}

// ============================================================================
// Self-Service Tier Change
// ============================================================================

#[derive(Deserialize)]
struct ChangeTierRequest {
    tier_id: Uuid,
}

/// POST /api/payments/tier
///
/// Self-service tier upgrade/downgrade. Updates the org's tier and syncs
/// the Stripe subscription (with proration).
async fn change_tier(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(request): Json<ChangeTierRequest>,
) -> impl IntoResponse {
    let auth = match authenticate(&headers, &state).await {
        Ok(a) => a,
        Err(e) => return e.into_response(),
    };

    if let Err(e) = require_admin(&state, auth.user_id, auth.organization_id).await {
        return e.into_response();
    }

    if let Err(e) = check_subscription_rate_limit(&state.redis, &auth.organization_id).await {
        warn!(
            organization_id = %auth.organization_id,
            error = %e,
            "Rate limited tier change attempt"
        );
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(api_error("Too many plan change attempts. Please try again later.")),
        )
            .into_response();
    }

    let tier_row = sqlx::query(
        "SELECT id, stripe_price_id, display_name FROM tier_definitions WHERE id = $1 AND is_public = true",
    )
    .bind(request.tier_id)
    .fetch_optional(state.db.as_ref())
    .await;

    let tier_row = match tier_row {
        Ok(Some(r)) => r,
        Ok(None) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(api_error("Invalid tier")),
            )
                .into_response();
        }
        Err(e) => {
            error!(error = %e, "DB error looking up tier");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(api_error("Internal error")),
            )
                .into_response();
        }
    };

    let stripe_price_id: Option<String> = tier_row.get("stripe_price_id");
    let tier_display_name: String = tier_row.get("display_name");

    let provider = match create_payment_provider(&state) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };

    if let Some(ref price_id) = stripe_price_id {
        match provider
            .update_subscription(auth.organization_id, price_id, None)
            .await
        {
            Ok(_) => {}
            Err(PaymentError::PaymentMethodNotFound(_)) => {
                return (
                    StatusCode::PAYMENT_REQUIRED,
                    Json(api_error(
                        "Please add a payment method before upgrading.",
                    )),
                )
                    .into_response();
            }
            Err(e) => return map_payment_error(e).into_response(),
        }
    }

    let result = sqlx::query(
        "UPDATE organizations SET tier_definition_id = $1 WHERE id = $2",
    )
    .bind(request.tier_id)
    .bind(auth.organization_id)
    .execute(state.db.as_ref())
    .await;

    if let Err(e) = result {
        error!(error = %e, "Failed to update org tier");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(api_error("Failed to update plan")),
        )
            .into_response();
    }

    if let Err(e) = state.entitlements.refresh_cache().await {
        warn!(error = %e, "Failed to refresh entitlement cache after tier change");
    }

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let _ = AuditEventBuilder::new(AuditEventType::TierChanged)
        .organization(auth.organization_id)
        .actor(auth.user_id)
        .details(serde_json::json!({
            "new_tier_id": request.tier_id,
            "new_tier_name": tier_display_name,
            "source": "self_service",
        }))
        .origin(
            &audit_origin.origin_type,
            &audit_origin.origin_ref,
            &audit_origin.origin_reason,
        )
        .caller(
            &audit_caller.caller_type,
            &audit_caller.key_label,
            &audit_caller.key_prefix,
        )
        .success()
        .log(&state.clickhouse)
        .await;

    (
        StatusCode::OK,
        Json(ApiResponse::success(serde_json::json!({
            "tier_id": request.tier_id,
            "tier_name": tier_display_name,
        }))),
    )
        .into_response()
}

// ============================================================================
// Public Tiers Listing
// ============================================================================

/// GET /api/payments/tiers
///
/// Returns available tier definitions for the self-service tier picker.
/// Requires authentication but not admin role.
async fn list_available_tiers(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let auth = match authenticate(&headers, &state).await {
        Ok(a) => a,
        Err(e) => return e.into_response(),
    };

    let current_tier_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT tier_definition_id FROM organizations WHERE id = $1",
    )
    .bind(auth.organization_id)
    .fetch_optional(state.db.as_ref())
    .await
    .ok()
    .flatten();

    let rows = sqlx::query(
        "SELECT id, name, display_name, stripe_price_id, config, is_public \
         FROM tier_definitions \
         WHERE is_public = true OR id = $1 \
         ORDER BY name ASC",
    )
    .bind(current_tier_id)
    .fetch_all(state.db.as_ref())
    .await;

    let rows = match rows {
        Ok(r) => r,
        Err(e) => {
            error!(error = %e, "Failed to list tiers");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(api_error("Failed to load plans")),
            )
                .into_response();
        }
    };

    let tiers: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| {
            let id: Uuid = r.get("id");
            serde_json::json!({
                "id": id,
                "name": r.get::<String, _>("name"),
                "display_name": r.get::<String, _>("display_name"),
                "stripe_price_id": r.get::<Option<String>, _>("stripe_price_id"),
                "config": r.get::<serde_json::Value, _>("config"),
                "is_public": r.get::<bool, _>("is_public"),
                "is_current": current_tier_id == Some(id),
            })
        })
        .collect();

    (StatusCode::OK, Json(ApiResponse::success(tiers))).into_response()
}

#[cfg(test)]
mod tests {
    use rust_decimal::prelude::ToPrimitive;
    use rust_decimal::Decimal;

    const PLATFORM_FEE_MULTIPLIER_STR: &str = "1.03";

    fn compute_charge_cents(credit_amount_usd: Decimal) -> Option<i64> {
        let multiplier: Decimal = PLATFORM_FEE_MULTIPLIER_STR.parse().unwrap();
        let charge_amount = credit_amount_usd * multiplier;
        let charge_cents_decimal = (charge_amount * Decimal::new(100, 0)).round_dp(0);
        let cents = charge_cents_decimal.to_i64()?;
        if cents > 0 {
            Some(cents)
        } else {
            None
        }
    }

    #[test]
    fn test_charge_cents_ten_dollars() {
        // $10 * 1.03 = $10.30 = 1030 cents
        let cents = compute_charge_cents(Decimal::new(10, 0)).unwrap();
        assert_eq!(cents, 1030);
    }

    #[test]
    fn test_charge_cents_one_dollar() {
        // $1 * 1.03 = $1.03 = 103 cents
        let cents = compute_charge_cents(Decimal::new(1, 0)).unwrap();
        assert_eq!(cents, 103);
    }

    #[test]
    fn test_charge_cents_hundred_dollars() {
        // $100 * 1.03 = $103.00 = 10300 cents
        let cents = compute_charge_cents(Decimal::new(100, 0)).unwrap();
        assert_eq!(cents, 10300);
    }

    #[test]
    fn test_charge_cents_rounding_fractional() {
        // $0.01 * 1.03 = $0.0103 -> rounds to 1 cent
        let cents = compute_charge_cents(Decimal::new(1, 2)).unwrap();
        assert_eq!(cents, 1);
    }

    #[test]
    fn test_charge_cents_zero_returns_none() {
        let result = compute_charge_cents(Decimal::ZERO);
        assert!(result.is_none());
    }

    #[test]
    fn test_charge_cents_large_amount() {
        // $10,000 * 1.03 = $10,300.00 = 1,030,000 cents
        let cents = compute_charge_cents(Decimal::new(10_000, 0)).unwrap();
        assert_eq!(cents, 1_030_000);
    }

    #[test]
    fn test_charge_cents_precise_amount() {
        // $49.99 * 1.03 = $51.4897 -> 5149 cents
        let cents = compute_charge_cents(Decimal::new(4999, 2)).unwrap();
        assert_eq!(cents, 5149);
    }

    #[test]
    fn test_min_credit_purchase_boundary() {
        let min_credit = Decimal::new(1, 0);
        // Amounts below minimum
        assert!(Decimal::new(99, 2) < min_credit); // $0.99
                                                   // Exact minimum
        assert!(Decimal::new(1, 0) >= min_credit); // $1.00
    }

    #[test]
    fn test_max_credit_purchase_boundary() {
        let max_credit = Decimal::new(10_000, 0);
        // Amounts above maximum
        assert!(Decimal::new(10_001, 0) > max_credit);
        // Exact maximum
        assert!(Decimal::new(10_000, 0) <= max_credit);
    }

    #[test]
    fn test_exchange_rate_usd() {
        // For USD payments, exchange rate should be 1.0
        let paid_currency = Some("usd");
        let exchange_rate = if paid_currency == Some("usd") || paid_currency.is_none() {
            Some(Decimal::new(1, 0))
        } else {
            None
        };
        assert_eq!(exchange_rate, Some(Decimal::new(1, 0)));
    }

    #[test]
    fn test_exchange_rate_foreign_currency() {
        let credit_amount = Decimal::new(100, 0); // $100 credits
        let multiplier: Decimal = PLATFORM_FEE_MULTIPLIER_STR.parse().unwrap();
        let charge_usd = credit_amount * multiplier; // $103
        let paid_amount = Decimal::new(95_00, 2); // €95.00

        let exchange_rate = if charge_usd > Decimal::ZERO {
            Some(paid_amount / charge_usd)
        } else {
            None
        };

        let rate = exchange_rate.unwrap();
        // €95 / $103 ≈ 0.92233...
        assert!(rate > Decimal::new(92, 2));
        assert!(rate < Decimal::new(93, 2));
    }

    // =====================================================================
    // Currency decimal tests
    // =====================================================================

    #[test]
    fn test_stripe_currency_usd_is_two_decimal() {
        assert_eq!(super::stripe_currency_decimals("usd"), 2);
        assert_eq!(super::stripe_currency_decimals("USD"), 2);
    }

    #[test]
    fn test_stripe_currency_eur_is_two_decimal() {
        assert_eq!(super::stripe_currency_decimals("eur"), 2);
    }

    #[test]
    fn test_stripe_currency_jpy_is_zero_decimal() {
        assert_eq!(super::stripe_currency_decimals("jpy"), 0);
        assert_eq!(super::stripe_currency_decimals("JPY"), 0);
    }

    #[test]
    fn test_stripe_currency_krw_is_zero_decimal() {
        assert_eq!(super::stripe_currency_decimals("krw"), 0);
    }

    #[test]
    fn test_stripe_currency_bhd_is_three_decimal() {
        assert_eq!(super::stripe_currency_decimals("bhd"), 3);
    }

    #[test]
    fn test_stripe_currency_kwd_is_three_decimal() {
        assert_eq!(super::stripe_currency_decimals("kwd"), 3);
    }

    #[test]
    fn test_paid_amount_usd_cents() {
        // Stripe sends 1030 for $10.30 in USD
        let decimals = super::stripe_currency_decimals("usd");
        let amount = Decimal::new(1030, decimals);
        assert_eq!(amount, Decimal::new(1030, 2)); // $10.30
    }

    #[test]
    fn test_paid_amount_jpy_whole_yen() {
        // Stripe sends 1500 for ¥1500 in JPY (zero-decimal)
        let decimals = super::stripe_currency_decimals("jpy");
        let amount = Decimal::new(1500, decimals);
        assert_eq!(amount, Decimal::new(1500, 0)); // ¥1500
    }

    #[test]
    fn test_paid_amount_bhd_fils() {
        // Stripe sends 5250 for 5.250 BHD (three-decimal)
        let decimals = super::stripe_currency_decimals("bhd");
        let amount = Decimal::new(5250, decimals);
        assert_eq!(amount, Decimal::new(5250, 3)); // 5.250 BHD
    }

    #[test]
    fn test_unknown_currency_defaults_to_two_decimal() {
        assert_eq!(super::stripe_currency_decimals("xyz"), 2);
    }
}
