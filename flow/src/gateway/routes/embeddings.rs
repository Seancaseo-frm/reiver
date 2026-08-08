//! POST /v1/embeddings — OpenAI-compatible embeddings endpoint.
//!
//! Reuses the shared primitives from [`context`], [`guardrails`], and
//! [`observability`] for request context, PII masking, content policy,
//! billing gates, and ClickHouse ingestion.

use std::borrow::Cow;
use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{Json, State},
    http::HeaderMap,
};
use crate::app_state::FlowState;
use crate::gateway::embedding_types::{EmbeddingRequest, EmbeddingResponse};
use crate::gateway::error::GatewayError;
use crate::gateway::guardrails::{check_content_policy, mask_pii_text, report_input_guardrail_violation};
use crate::llm::types::LlmRequest;

use super::context::RequestContext;
use super::observability::{BillingContext, finalize_and_send};
use super::get_introspection_settings;
use crate::gateway::observability::current_otel_ids;

/// POST /v1/embeddings
pub(crate) async fn embeddings(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    Json(mut request): Json<EmbeddingRequest>,
) -> Result<Json<EmbeddingResponse>, GatewayError> {
    let start = Instant::now();

    if let Err(errors) = request.validate() {
        return Err(GatewayError::ValidationError(errors.join("; ")));
    }

    let ctx = RequestContext::from_headers(&headers)?;
    let project_id = ctx.project_id;
    let billing_pid = ctx.billing_project_id;
    let request_id = ctx.request_id.clone();

    let resolved = ctx.resolve_provider_and_key(&state, &request.model).await?;
    let provider_name = resolved.provider.as_str().to_string();
    let is_platform_key = resolved.key.is_platform;

    let org_id = state.get_organization_id(billing_pid).await.unwrap_or(None);
    ctx.check_billing_gates(&state, org_id, is_platform_key).await?;

    let settings = get_introspection_settings(&state, project_id).await;

    // PII masking
    let mut pii_detected = false;
    for text in request.input.texts_mut() {
        let masked = mask_pii_text(text);
        if let Cow::Owned(replacement) = masked {
            pii_detected = true;
            *text = replacement;
        }
    }

    // Content policy guardrails
    if !settings.guardrail_config.is_noop() {
        let texts: Vec<&str> = request.input.texts();
        if let Some(violation) =
            check_content_policy(&settings.guardrail_config, &texts, pii_detected)
        {
            report_input_guardrail_violation(
                &state,
                project_id,
                &request_id,
                &provider_name,
                &request.model,
                &violation,
            )
            .await;
            return Err(GatewayError::GuardrailViolation {
                rule: violation.rule,
                detail: violation.detail,
            });
        }
    }

    let gateway_router = state.gateway_router.clone();
    let response = gateway_router
        .embed(&request, &resolved.key.key)
        .await?;

    // Capture duration before spawning so it reflects actual request latency,
    // not time spent waiting in the tokio runtime queue.
    let duration = start.elapsed();

    let state_bg = state.clone();
    let model = request.model.clone();
    let response_model = response.model.clone();
    let usage = response.usage.clone();
    let provider_bg = provider_name.clone();
    let user_id = request.user.clone().unwrap_or_default();

    tokio::spawn(async move {
        let (trace_id, span_id) = current_otel_ids();
        let llm_request = LlmRequest {
            project_id: project_id.to_string(),
            request_id: format!("{}:{}", trace_id, span_id),
            trace_id,
            span_id,
            gen_ai_system: provider_bg,
            gen_ai_request_model: model,
            gen_ai_response_model: response_model,
            gen_ai_operation_name: "embedding".to_string(),
            input_tokens: usage.prompt_tokens,
            output_tokens: 0,
            total_tokens: usage.total_tokens,
            cost_usd: rust_decimal::Decimal::ZERO,
            timestamp: chrono::Utc::now(),
            duration_ms: duration.as_millis().min(u32::MAX as u128) as u32,
            status_code: "ok".to_string(),
            service_name: "reiver-gateway".to_string(),
            is_platform_key,
            user_id,
            ..Default::default()
        };
        let billing = BillingContext {
            _billing_project_id: billing_pid,
            is_platform_key,
            org_id,
        };
        finalize_and_send(&state_bg, llm_request, &billing, "", "").await;
    });

    tracing::debug!(
        request_id = %request_id,
        project_id = %project_id,
        model = %request.model,
        provider = %provider_name,
        duration_ms = %start.elapsed().as_millis(),
        "Embedding request completed"
    );

    Ok(Json(response))
}

