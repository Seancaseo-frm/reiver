//! Billing API endpoints for usage tracking and budget management.

use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post, put},
    Json, Router,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info};

use crate::api::auth_helpers::{authenticate, require_admin, ErrorResponse};
use crate::app_state::WebsiteState;
use crate::audit::{AuditCaller, AuditEventBuilder, AuditEventType, AuditOrigin};
use crate::billing::{
    Budget, BudgetStatus, CreateBudgetRequest, GatewayModelCost, ProjectUsage,
    UpdateBudgetRequest, UsageSummary,
};
use crate::rate_limit::{check_authenticated_rate_limit, RateLimitType};

// ============================================================================
// Input Validation
// ============================================================================
//
// DEFENSE-IN-DEPTH PATTERN:
// These validation functions are duplicated in the service layer (BillingService).
// This is intentional - see the module documentation in src/billing/service.rs for
// the full rationale. In summary:
//
// 1. API layer validates first and returns user-friendly error messages
// 2. Service layer re-validates as a safety net for internal callers
//
// Keep validation logic in sync between both layers when making changes.

/// Validate that a budget amount is positive.
fn validate_budget_amount(amount: Decimal) -> Result<(), String> {
    if amount <= Decimal::ZERO {
        return Err("Budget amount must be greater than zero".to_string());
    }
    // Cap at a reasonable maximum to prevent overflow issues
    let max_budget = Decimal::new(999_999_999, 2); // $9,999,999.99
    if amount > max_budget {
        return Err("Budget amount exceeds maximum allowed value".to_string());
    }
    Ok(())
}

/// Validate that an alert threshold percentage is within valid range.
fn validate_alert_threshold(percent: i32) -> Result<(), String> {
    if percent < 1 || percent > 100 {
        return Err("Alert threshold must be between 1 and 100 percent".to_string());
    }
    Ok(())
}

/// Create the billing router.
pub fn create_billing_router() -> Router<Arc<WebsiteState>> {
    Router::new()
        // Usage endpoints
        .route("/usage", get(get_current_usage))
        .route("/usage/by-project", get(get_usage_by_project))
        .route("/usage/gateway-models", get(get_gateway_models))
        // Budget endpoints
        .route("/budget", get(get_budget))
        .route("/budget", post(create_budget))
        .route("/budget", put(update_budget))
        .route("/budget/status", get(get_budget_status))
}

// ============================================================================
// Response Types
// ============================================================================

#[derive(Serialize)]
struct UsageResponse {
    success: bool,
    data: UsageSummary,
}

#[derive(Serialize)]
struct ProjectUsageResponse {
    success: bool,
    data: Vec<ProjectUsage>,
}

#[derive(Serialize)]
struct BudgetResponse {
    success: bool,
    data: Option<Budget>,
}

#[derive(Serialize)]
struct BudgetStatusResponse {
    success: bool,
    data: Option<BudgetStatus>,
}

#[derive(Serialize)]
struct GatewayModelsResponse {
    success: bool,
    data: Vec<GatewayModelCost>,
}

// ============================================================================
// Query Parameters
// ============================================================================

// ============================================================================
// Usage Endpoints
// ============================================================================

/// Get current billing period usage.
/// GET /api/billing/usage
///
/// # Rate Limiting
/// Limited to 30/min, 120/hour per user to prevent ClickHouse DoS.
async fn get_current_usage(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let auth = match authenticate(&headers, &state).await {
        Ok(auth) => auth,
        Err(e) => return e.into_response(),
    };

    // Apply rate limiting for billing endpoints (queries ClickHouse)
    if let Err(_) = check_authenticated_rate_limit(
        &state.redis,
        &auth.user_id,
        RateLimitType::Billing,
        &state.config,
    )
    .await
    {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(ErrorResponse::new(
                "Too many requests. Please try again later.",
            )),
        )
            .into_response();
    }

    match state.billing.get_current_usage(auth.organization_id).await {
        Ok(usage) => (
            StatusCode::OK,
            Json(UsageResponse {
                success: true,
                data: usage,
            }),
        )
            .into_response(),
        Err(e) => {
            error!(organization_id = %auth.organization_id, error = %e, "Failed to get usage");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new("Failed to retrieve usage data")),
            )
                .into_response()
        }
    }
}

/// Get usage breakdown by project.
/// GET /api/billing/usage/by-project
///
/// # Rate Limiting
/// Limited to 30/min, 120/hour per user to prevent ClickHouse DoS.
async fn get_usage_by_project(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let auth = match authenticate(&headers, &state).await {
        Ok(auth) => auth,
        Err(e) => return e.into_response(),
    };

    // Apply rate limiting for billing endpoints (queries ClickHouse)
    if let Err(_) = check_authenticated_rate_limit(
        &state.redis,
        &auth.user_id,
        RateLimitType::Billing,
        &state.config,
    )
    .await
    {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(ErrorResponse::new(
                "Too many requests. Please try again later.",
            )),
        )
            .into_response();
    }

    match state
        .billing
        .get_usage_by_project(auth.organization_id)
        .await
    {
        Ok(usage) => (
            StatusCode::OK,
            Json(ProjectUsageResponse {
                success: true,
                data: usage,
            }),
        )
            .into_response(),
        Err(e) => {
            error!(organization_id = %auth.organization_id, error = %e, "Failed to get usage by project");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new("Failed to retrieve usage data")),
            )
                .into_response()
        }
    }
}

/// Get AI Gateway cost breakdown by model.
/// GET /api/billing/usage/gateway-models
async fn get_gateway_models(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let auth = match authenticate(&headers, &state).await {
        Ok(auth) => auth,
        Err(e) => return e.into_response(),
    };

    if let Err(_) = check_authenticated_rate_limit(
        &state.redis,
        &auth.user_id,
        RateLimitType::Billing,
        &state.config,
    )
    .await
    {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(ErrorResponse::new(
                "Too many requests. Please try again later.",
            )),
        )
            .into_response();
    }

    match state
        .billing
        .get_gateway_cost_by_model(auth.organization_id)
        .await
    {
        Ok(models) => (
            StatusCode::OK,
            Json(GatewayModelsResponse {
                success: true,
                data: models,
            }),
        )
            .into_response(),
        Err(e) => {
            error!(organization_id = %auth.organization_id, error = %e, "Failed to get gateway model costs");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(
                    "Failed to retrieve gateway cost data",
                )),
            )
                .into_response()
        }
    }
}

// ============================================================================
// Budget Endpoints
// ============================================================================

/// Get organization budget.
/// GET /api/billing/budget
async fn get_budget(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let auth = match authenticate(&headers, &state).await {
        Ok(auth) => auth,
        Err(e) => return e.into_response(),
    };

    match state.billing.get_org_budget(auth.organization_id).await {
        Ok(budget) => (
            StatusCode::OK,
            Json(BudgetResponse {
                success: true,
                data: budget,
            }),
        )
            .into_response(),
        Err(e) => {
            error!(organization_id = %auth.organization_id, error = %e, "Failed to get budget");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new("Failed to retrieve budget")),
            )
                .into_response()
        }
    }
}

/// Create or update organization budget.
/// POST /api/billing/budget
async fn create_budget(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(request): Json<CreateBudgetRequest>,
) -> impl IntoResponse {
    let auth = match authenticate(&headers, &state).await {
        Ok(auth) => auth,
        Err(e) => return e.into_response(),
    };

    // Require admin role for budget creation
    if let Err(e) = require_admin(&state, auth.user_id, auth.organization_id).await {
        return e.into_response();
    }

    // Validate input values
    if let Err(e) = validate_budget_amount(request.monthly_budget_usd) {
        return (StatusCode::BAD_REQUEST, Json(ErrorResponse::new(e))).into_response();
    }

    let threshold = request.alert_threshold_percent.unwrap_or(80);
    if let Err(e) = validate_alert_threshold(threshold) {
        return (StatusCode::BAD_REQUEST, Json(ErrorResponse::new(e))).into_response();
    }

    match state
        .billing
        .create_budget(
            auth.organization_id,
            request.project_id,
            request.monthly_budget_usd,
            threshold,
            Some(auth.user_id),
        )
        .await
    {
        Ok(budget) => {
            let audit_origin = AuditOrigin::from_headers(&headers);
            let audit_caller = AuditCaller::from_headers(&headers);
            let _ = AuditEventBuilder::new(AuditEventType::BudgetCreated)
                .organization(auth.organization_id)
                .actor(auth.user_id)
                .resource("budget", budget.id)
                .details(serde_json::json!({
                    "created": {
                        "monthly_budget_usd": budget.monthly_budget_usd.to_string(),
                        "alert_threshold_percent": budget.alert_threshold_percent,
                        "project_id": budget.project_id
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
                budget_id = %budget.id,
                monthly_budget_usd = %budget.monthly_budget_usd,
                "Budget created"
            );

            (
                StatusCode::OK,
                Json(BudgetResponse {
                    success: true,
                    data: Some(budget),
                }),
            )
                .into_response()
        }
        Err(e) => {
            error!(organization_id = %auth.organization_id, error = %e, "Failed to create budget");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new("Failed to create budget")),
            )
                .into_response()
        }
    }
}

/// Update organization budget.
/// PUT /api/billing/budget
async fn update_budget(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(request): Json<UpdateBudgetRequest>,
) -> impl IntoResponse {
    let auth = match authenticate(&headers, &state).await {
        Ok(auth) => auth,
        Err(e) => return e.into_response(),
    };

    // Require admin role for budget updates
    if let Err(e) = require_admin(&state, auth.user_id, auth.organization_id).await {
        return e.into_response();
    }

    // Validate input values if provided
    if let Some(amount) = request.monthly_budget_usd {
        if let Err(e) = validate_budget_amount(amount) {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(e)),
            )
                .into_response();
        }
    }

    if let Some(threshold) = request.alert_threshold_percent {
        if let Err(e) = validate_alert_threshold(threshold) {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(e)),
            )
                .into_response();
        }
    }

    // Get existing budget
    let budget = match state.billing.get_org_budget(auth.organization_id).await {
        Ok(Some(b)) => b,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::new("No budget found for this organization")),
            )
                .into_response();
        }
        Err(e) => {
            error!(organization_id = %auth.organization_id, error = %e, "Failed to get budget");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new("Failed to retrieve budget")),
            )
                .into_response();
        }
    };

    match state
        .billing
        .update_budget(
            budget.id,
            auth.organization_id,
            request.monthly_budget_usd,
            request.alert_threshold_percent,
            request.enabled,
        )
        .await
    {
        Ok(updated_budget) => {
            let audit_origin = AuditOrigin::from_headers(&headers);
            let audit_caller = AuditCaller::from_headers(&headers);
            let _ = AuditEventBuilder::new(AuditEventType::BudgetUpdated)
                .organization(auth.organization_id)
                .actor(auth.user_id)
                .resource("budget", updated_budget.id)
                .details(serde_json::json!({
                    "before": {
                        "monthly_budget_usd": budget.monthly_budget_usd.to_string(),
                        "alert_threshold_percent": budget.alert_threshold_percent,
                        "project_id": budget.project_id
                    },
                    "after": {
                        "monthly_budget_usd": updated_budget.monthly_budget_usd.to_string(),
                        "alert_threshold_percent": updated_budget.alert_threshold_percent,
                        "project_id": updated_budget.project_id
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
                budget_id = %updated_budget.id,
                "Budget updated"
            );

            (
                StatusCode::OK,
                Json(BudgetResponse {
                    success: true,
                    data: Some(updated_budget),
                }),
            )
                .into_response()
        }
        Err(e) => {
            error!(organization_id = %auth.organization_id, error = %e, "Failed to update budget");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new("Failed to update budget")),
            )
                .into_response()
        }
    }
}

/// Get budget status with usage comparison.
/// GET /api/billing/budget/status
async fn get_budget_status(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let auth = match authenticate(&headers, &state).await {
        Ok(auth) => auth,
        Err(e) => return e.into_response(),
    };

    match state.billing.get_budget_status(auth.organization_id).await {
        Ok(status) => (
            StatusCode::OK,
            Json(BudgetStatusResponse {
                success: true,
                data: status,
            }),
        )
            .into_response(),
        Err(e) => {
            error!(organization_id = %auth.organization_id, error = %e, "Failed to get budget status");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new("Failed to retrieve budget status")),
            )
                .into_response()
        }
    }
}
