//! Shared observability and billing pipeline for all gateway endpoints.
//!
//! The `finalize_and_send` function handles the common tail of every gateway
//! request: cost calculation, credit deduction (or BYOK fee), and ClickHouse
//! ingestion via the `llm_request_tx` channel. Both chat completions and
//! future endpoints (embeddings, etc.) use this to avoid duplicating billing.

use std::sync::Arc;
use uuid::Uuid;

use crate::app_state::FlowState;
use crate::llm::types::LlmRequest;

/// Common billing context needed after the `LlmRequest` is built.
pub(crate) struct BillingContext {
    pub _billing_project_id: Uuid,
    pub is_platform_key: bool,
    pub org_id: Option<Uuid>,
}

/// Prepare an `LlmRequest` (pricing), process credit billing, and send it
/// to the ClickHouse ingestion channel. Runs on the current task (call from
/// within a `tokio::spawn`).
pub(crate) async fn finalize_and_send(
    state: &Arc<FlowState>,
    mut llm_request: LlmRequest,
    billing: &BillingContext,
    session_id: &str,
    session_name: &str,
) {
    llm_request.session_id = session_id.to_string();
    llm_request.session_name = session_name.to_string();

    let request_id = llm_request.request_id.clone();
    let project_id_str = llm_request.project_id.clone();
    let model = llm_request.gen_ai_request_model.clone();

    let prepared = match state
        .llm_processor
        .prepare_gateway_request(llm_request)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(
                request_id = %request_id,
                project_id = %project_id_str,
                model = %model,
                error = %e,
                "Failed to prepare gateway request for batch"
            );
            return;
        }
    };

    process_billing(state, &prepared, billing);

    if let Err(e) = state.llm_request_tx.try_send(prepared) {
        tracing::warn!(
            request_id = %request_id,
            error = %e,
            "llm_request buffer full or closed, dropping observability write"
        );
    }
}

/// Usage billing: report platform-key usage to Stripe Meters.
/// BYOK fees are derived from ClickHouse `llm_cost_daily` at invoice time
/// (cost_usd * 0.03), so no per-request write is needed.
fn process_billing(
    state: &Arc<FlowState>,
    prepared: &LlmRequest,
    billing: &BillingContext,
) {
    if !billing.is_platform_key {
        return;
    }

    let cost = prepared.cost_usd;
    if cost <= rust_decimal::Decimal::ZERO {
        return;
    }

    let oid = match billing.org_id {
        Some(id) => id,
        None => return,
    };

    state.meter_service.record_usage(oid, cost);
}
