//! HTTP routes for the AI Gateway.
//!
//! Implements OpenAI-compatible endpoints that route to multiple LLM providers.
//!
//! The pipeline is split into phase-based submodules:
//! - `validation` — request validation, PII masking, session budgets
//! - `resolve` — model resolution, provider selection, routing
//! - `execution` — provider call orchestration, retry, fallback
//! - `response` — response formatting, observability, billing hooks
//! - `settings` — per-project gateway settings resolution

pub(crate) mod context;
mod embeddings;
mod execution;
pub(crate) mod observability;
mod resolve;
mod response;
mod settings;
mod validation;

use axum::{
    extract::{DefaultBodyLimit, Json, State},
    http::{HeaderMap, HeaderName, HeaderValue},
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

use crate::app_state::FlowState;
use crate::gateway::cache::{is_cacheable, CacheStatus};
use crate::gateway::error::GatewayError;
use crate::gateway::observability::{build_llm_request, LlmRequestParams};
use crate::gateway::otel_publisher::SpanData;
use crate::gateway::prompt_resolver::{
    apply_prompt_config, resolve_prompt_config, PromptResolution,
};
use crate::gateway::provider_manager::ProviderKeyStore;
use crate::gateway::provider_types::Provider;
use crate::gateway::types::{ChatCompletionRequest, ChatCompletionResponse, ThinkingConfig};
use std::collections::BTreeMap;
use tracing::Instrument;

use execution::{execute_with_fallback, handle_streaming_with_chain, StreamingContext};
use response::{
    apply_output_guardrails, emit_non_streaming_observability, enforce_output_contract,
    spawn_judge_if_sampled, OutputContractContext,
};
use validation::{check_session_budget_pre, increment_session_budget, mask_request_pii};

/// Per-project gateway hot-path settings fetched from the database.
pub(crate) struct IntrospectionSettings {
    pub(crate) enabled: bool,
    pub(crate) budget_tokens: u32,
    pub(crate) session_budget_usd: Option<f64>,
    pub(crate) guardrail_config: crate::gateway::guardrails::GuardrailConfig,
    pub(crate) agent_enabled: bool,
    pub(crate) agent_scopes: Vec<String>,
    /// Fraction of prompt-config requests to evaluate with LLM-as-judge (0.0-1.0).
    /// `None` or `0.0` means disabled.
    pub(crate) judge_sample_rate: Option<f64>,
    /// Project-level default fallback models (used when request has no `models` array).
    pub(crate) default_fallback_models: Vec<String>,
    /// Project-level default provider preferences (used when request has no `provider` object).
    pub(crate) provider_preferences: Option<crate::gateway::types::ProviderPreferences>,
    /// Project-level fallback toggle.
    pub(crate) fallback_enabled: bool,
    /// Per-project agent personality and domain context.
    pub(crate) agent_soul: crate::api::llm_settings::AgentSoul,
}

/// Maximum request body size for gateway requests (10 MB).
const MAX_BODY_SIZE: usize = 10 * 1024 * 1024;

/// Special model name that triggers automatic provider selection.
const MODEL_AUTO: &str = "auto";

/// Create a `HeaderValue` from a string, falling back to a static default if the
/// string contains invalid header characters.
fn header_value(s: &str, fallback: &'static str) -> HeaderValue {
    HeaderValue::from_str(s).unwrap_or_else(|_| HeaderValue::from_static(fallback))
}

/// Create the gateway router with all endpoints.
pub fn create_gateway_router() -> Router<Arc<FlowState>> {
    Router::new()
        .route("/chat/completions", post(chat_completions))
        .route("/embeddings", post(embeddings::embeddings))
        .route("/sessions/{session_id}/end", post(end_session))
        .route("/models", axum::routing::get(list_models))
        .layer(DefaultBodyLimit::max(MAX_BODY_SIZE))
}

/// POST /v1/sessions/{session_id}/end — Mark a session as ended.
///
/// Reserves a dedup slot immediately and returns 202. A background task
/// waits 30 seconds (for ClickHouse buffer flush), then sends the Kafka
/// evaluation job. If the pod restarts during the delay, the 30-minute
/// idle poll discovers the session after the short per-session reservation
/// has expired, so the fallback can enqueue it safely.
async fn end_session(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    axum::extract::Path(session_id): axum::extract::Path<String>,
) -> Result<(axum::http::StatusCode, Json<serde_json::Value>), GatewayError> {
    let project_id = crate::api::extract_project_id(&headers).map_err(|_| {
        GatewayError::AuthenticationFailed("Missing or invalid X-Project-Id header".to_string())
    })?;

    if session_id.is_empty() {
        return Err(GatewayError::ValidationError(
            "session_id must not be empty".to_string(),
        ));
    }

    let pid = project_id.to_string();

    let reserved = crate::gateway::session_evaluator::try_reserve_session(
        &state.redis, &pid, &session_id,
    )
    .await;

    if !reserved {
        return Ok((
            axum::http::StatusCode::ACCEPTED,
            Json(serde_json::json!({
                "session_id": session_id,
                "status": "already_enqueued"
            })),
        ));
    }

    let kafka = state.kafka.clone();
    let redis = state.redis.clone();
    let sid = session_id.clone();

    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(
            crate::gateway::session_evaluator::END_SESSION_DELAY_SECS,
        ))
        .await;

        let message = reiver_core::kafka::SessionEvalJobKafkaMessage {
            project_id: pid.clone(),
            session_id: sid.clone(),
            enqueued_at: chrono::Utc::now().to_rfc3339(),
        };

        if let Err(e) = kafka.send_session_eval_job(&message).await {
            tracing::warn!(
                project_id = %pid,
                session_id = %sid,
                error = %e,
                "Failed to send session eval job after end_session delay"
            );
            crate::gateway::session_evaluator::unreserve_session(&redis, &pid, &sid).await;
        } else {
            crate::gateway::session_evaluator::confirm_session_enqueued(&redis, &pid, &sid).await;
            tracing::info!(
                project_id = %pid,
                session_id = %sid,
                "Session eval job enqueued after end_session delay"
            );
        }
    });

    Ok((
        axum::http::StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "session_id": session_id,
            "status": "evaluation_scheduled"
        })),
    ))
}

// ========================================================================
// Phase coordination — routes incoming requests through the pipeline
// ========================================================================

/// POST /v1/chat/completions - OpenAI-compatible chat completion endpoint.
///
/// Routes requests to the appropriate LLM provider based on the model name.
/// Supports both streaming (SSE) and non-streaming responses.
async fn chat_completions(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, GatewayError> {
    let start = Instant::now();

    let request: ChatCompletionRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            let project_id = headers
                .get("X-Project-Id")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("unknown");
            tracing::warn!(
                project_id = %project_id,
                error = %e,
                body_len = body.len(),
                "Failed to deserialize chat completion request"
            );
            return Err(GatewayError::ValidationError(format!(
                "Invalid request body: {}",
                e
            )));
        }
    };

    let project_id_header: String = headers
        .get("X-Project-Id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown")
        .to_string();

    let request_id = Uuid::now_v7().to_string();
    let requested_model = request.model.clone();
    let root_span = tracing::info_span!(
        "gateway.chat_completion",
        request_id = %request_id,
        model = %request.model,
        requested_model = %requested_model,
        project_id = %project_id_header,
        provider = tracing::field::Empty,
        stream = tracing::field::Empty,
        cache = tracing::field::Empty,
        provider_chain = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
        otel.status_message = tracing::field::Empty,
        gen_ai.operation.name = "chat",
        gen_ai.request.model = %request.model,
        gen_ai.provider.name = tracing::field::Empty,
    );

    let result = chat_completions_inner(
        state,
        headers,
        request,
        start,
        request_id.clone(),
        root_span.clone(),
    )
    .instrument(root_span.clone())
    .await;

    if let Err(ref e) = result {
        root_span.record("otel.status_code", "ERROR");
        root_span.record("otel.status_message", &e.to_string());
        let _guard = root_span.enter();
        tracing::warn!(
            request_id = %request_id,
            project_id = %project_id_header,
            error = %e,
            error_type = %e.error_type_str(),
            "Gateway request failed"
        );
    }

    result
}

async fn chat_completions_inner(
    state: Arc<FlowState>,
    headers: HeaderMap,
    request: ChatCompletionRequest,
    start: Instant,
    request_id: String,
    root_span: tracing::Span,
) -> Result<Response, GatewayError> {
    if let Err(errors) = request.validate() {
        tracing::warn!(
            request_id = %request_id,
            model = %request.model,
            errors = %errors.join("; "),
            "Gateway request validation failed"
        );
        return Err(GatewayError::ValidationError(errors.join("; ")));
    }

    let ctx = context::RequestContext::from_headers(&headers)?;
    let project_id = ctx.project_id;
    let billing_pid = ctx.billing_project_id;
    root_span.record("project_id", tracing::field::display(&project_id));

    // If model is empty but models array has entries, use the first as primary.
    let mut request = request;
    if request.model.trim().is_empty() {
        if let Some(ref models) = request.models {
            if let Some(first) = models.first() {
                request.model = first.clone();
            }
        }
    }

    let is_auto_mode = request.model.is_empty() || request.model == MODEL_AUTO;
    let gateway_router = state.gateway_router.clone();

    // 3a. Fetch per-project gateway settings (guardrails, budgets, etc.)
    let settings = get_introspection_settings(&state, project_id).await;

    // For "auto" mode, resolve the best model using the preference list.
    // Priority: per-request `models` array > project-level `default_fallback_models` setting.
    // When `provider.sort` is "latency", candidates are sorted by P95 latency.
    let initial_provider = if is_auto_mode {
        let sort_by_latency = request
            .provider
            .as_ref()
            .and_then(|p| p.sort.as_deref())
            .map_or(false, |s| s == "latency");
        let candidate_models = request.models.as_deref();
        let project_defaults = &settings.default_fallback_models;
        let route = state
            .provider_manager
            .resolve_auto_extended(
                project_id,
                state.as_ref(),
                candidate_models,
                Some(project_defaults.as_slice()),
                sort_by_latency,
            )
            .await?;
        request.model = route.model_id;
        route.provider
    } else {
        Provider::from_model_prefix(&request.model).ok_or_else(|| {
            tracing::warn!(
                request_id = %request_id,
                project_id = %project_id,
                model = %request.model,
                "Unsupported model requested"
            );
            GatewayError::UnsupportedModel(request.model.clone())
        })?
    };

    let original_provider = initial_provider;

    // Capture body-provided prompt config name before the request is moved.
    let body_config_name: Option<String> = request.prompt_config.clone();
    // 4. Run prompt resolution and provider key fetch concurrently
    let (prompt_result, key_result) = tokio::join!(
        resolve_prompt_config(
            state.prompt_store.as_ref(),
            project_id,
            &headers,
            body_config_name.as_deref(),
        ),
        state.get_key(project_id, original_provider),
    );

    let resolved_key = key_result.ok_or_else(|| {
        GatewayError::MissingProviderKey(format!(
            "API key not configured for provider '{}'",
            original_provider
        ))
    })?;
    let resolved_base_url = resolved_key.base_url;

    let prompt_resolution: Option<PromptResolution> =
        if let Some((resolution, version_config)) = prompt_result {
            let model_before = request.model.clone();
            apply_prompt_config(&mut request, &version_config, &headers)?;

            if request.model == model_before && is_auto_mode {
                tracing::warn!(
                    request_id = %request_id,
                    project_id = %project_id,
                    config_id = %resolution.config_id,
                    version_id = %resolution.version_id,
                    auto_resolved_model = %model_before,
                    "Prompt config did not override model; auto-resolved model will be used. \
                     Set the model field in the prompt version to override."
                );
            }

            tracing::debug!(
                request_id = %request_id,
                project_id = %project_id,
                config_id = %resolution.config_id,
                version_id = %resolution.version_id,
                variant = %resolution.variant,
                rollout_id = ?resolution.rollout_id,
                model = %request.model,
                "Applied prompt configuration"
            );

            Some(resolution)
        } else {
            None
        };

    // Extract routing fields before clearing them.
    let request_models = request.models.take();
    let request_provider_prefs = request
        .provider
        .take()
        .or_else(|| settings.provider_preferences.clone());

    // Clear gateway-only fields so they are not forwarded to providers.
    // OpenAI's passthrough path serializes the whole struct; unknown fields cause 400s.
    request.prompt_config = None;
    request.prompt_variables = None;

    // Compute fallback_allowed before passing to resolve_provider_chain.
    let fallback_allowed = request_provider_prefs
        .as_ref()
        .and_then(|p| p.allow_fallbacks)
        .unwrap_or(settings.fallback_enabled && state.fallback_config.enable_fallback);

    // Resolve the complete ordered provider chain (primary + fallbacks).
    let chain = resolve::resolve_provider_chain(
        &state,
        &gateway_router,
        project_id,
        &request.model,
        request_models,
        &settings.default_fallback_models,
        request_provider_prefs.as_ref(),
        fallback_allowed,
        resolved_base_url.as_deref(),
    )
    .await?;

    // Use the primary candidate's provider for spans and pre-execution checks.
    let provider = chain[0].provider;
    let provider_name = provider.as_str();
    let is_platform_key = chain[0].is_platform_key;
    root_span.record("provider", provider_name);
    root_span.record("gen_ai.provider.name", provider_name);

    let chain_desc: Vec<String> = chain
        .iter()
        .map(|c| format!("{}:{}", c.provider, c.model))
        .collect();
    root_span.record("provider_chain", &chain_desc.join(", "));

    if settings.enabled && request.thinking.is_none() {
        request.thinking = Some(ThinkingConfig {
            thinking_type: crate::gateway::types::ThinkingToggle::Enabled,
            budget_tokens: Some(settings.budget_tokens),
        });
    }

    // Keep the effective request, cache decision, and observability record in
    // agreement with Anthropic. These model families reject non-default
    // sampling fields, so the provider adapter will not send them.
    if provider == Provider::Anthropic
        && crate::gateway::providers::anthropic::uses_provider_managed_sampling(&request.model)
    {
        if request.temperature.is_some() || request.top_p.is_some() {
            tracing::debug!(
                model = %request.model,
                "Removed sampling parameters unsupported by this Anthropic model"
            );
        }
        request.temperature = None;
        request.top_p = None;
    }

    // Apply spotlighting: wrap untrusted-role messages in delimiters
    crate::gateway::guardrails::apply_spotlighting(&settings.guardrail_config, &mut request);

    let input_pii_detected = mask_request_pii(&state, project_id, &mut request).await;

    if !settings.guardrail_config.is_noop() {
        use crate::gateway::guardrails::{check_input_guardrails, report_input_guardrail_violation};
        if let Some(violation) =
            check_input_guardrails(&settings.guardrail_config, &request, input_pii_detected)
        {
            report_input_guardrail_violation(
                &state,
                project_id,
                &request_id,
                provider_name,
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

    let session_id = headers
        .get("x-reiver-session-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_default();

    let session_name = headers
        .get("x-reiver-session-name")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_default();

    // Log content for all session-tagged requests. The 2-hour ClickHouse
    // column TTL auto-clears it; the session evaluator copies matching
    // sessions to Postgres before expiry.
    let log_content = state.config.gateway_log_content || !session_id.is_empty();

    check_session_budget_pre(
        &state,
        project_id,
        &session_id,
        settings.session_budget_usd,
        &request_id,
    )
    .await?;

    let org_id = state.get_organization_id(billing_pid).await.unwrap_or(None);
    ctx.check_billing_gates(&state, org_id, is_platform_key).await?;

    let mut is_streaming = request.stream.unwrap_or(false);

    // Auto-downgrade to non-streaming for providers that don't support it
    if is_streaming && !provider.supports_streaming() {
        tracing::debug!(
            provider = %provider_name,
            "Provider does not support streaming, falling back to non-streaming"
        );
        is_streaming = false;
        request.stream = Some(false);
    }

    root_span.record("stream", is_streaming);

    tracing::debug!(
        request_id = %request_id,
        project_id = %project_id,
        model = %request.model,
        provider = %provider_name,
        streaming = is_streaming,
        "Processing gateway request"
    );

    if is_streaming {
        let fallback_config = state.fallback_config.clone();
        return handle_streaming_with_chain(
            state,
            StreamingContext {
                gateway_router,
                request,
                project_id,
                billing_project_id: billing_pid,
                start,
                request_id,
                prompt_resolution: prompt_resolution.clone(),
                fallback_config: &fallback_config,
                session_id,
                session_name,
                session_budget_usd: settings.session_budget_usd,
                guardrail_config: settings.guardrail_config,
                judge_sample_rate: settings.judge_sample_rate,
                log_content,
                org_id,
            },
            &chain,
        )
        .await;
    }

    // 7.5. Check cache for non-streaming requests
    let should_cache = is_cacheable(&request);
    let mut cache_status = if should_cache {
        CacheStatus::Miss
    } else {
        CacheStatus::Skip
    };

    if should_cache {
        if let Some(cached) = state.gateway_cache.get(project_id, &request).await {
            cache_status = CacheStatus::Hit;
            root_span.record("cache", "hit");
            let duration = start.elapsed();

            // Record cache hit in observability (in background)
            let mut llm_request = build_llm_request(LlmRequestParams {
                project_id,
                request: &request,
                response: &cached.response,
                provider: provider_name,
                duration,
                log_content,
                fallback_used: false,
                original_model: request.model.clone(),
                retry_count: 0,
                guardrail_violations: Vec::new(),
                is_platform_key,
            });
            llm_request.session_id = session_id.clone();
            llm_request.session_name = session_name.clone();

            let llm_processor = state.llm_processor.clone();
            let llm_request_tx = state.llm_request_tx.clone();
            let model_for_log = request.model.clone();
            let request_id_for_log = request_id.clone();
            let meter_service_cache = state.meter_service.clone();
            let is_platform_key_cache = is_platform_key;
            let org_id_cache = org_id;
            tokio::spawn(async move {
                let prepared = match llm_processor.prepare_gateway_request(llm_request).await {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::error!(
                            request_id = %request_id_for_log,
                            project_id = %project_id,
                            model = %model_for_log,
                            error = %e,
                            "Failed to prepare cached gateway request for batch"
                        );
                        return;
                    }
                };

                if is_platform_key_cache {
                    if let Some(oid) = org_id_cache {
                        let cost = prepared.cost_usd;
                        if cost > rust_decimal::Decimal::ZERO {
                            meter_service_cache.record_usage(oid, cost);
                        }
                    }
                }

                if let Err(e) = llm_request_tx.try_send(prepared) {
                    tracing::warn!(
                        request_id = %request_id_for_log,
                        error = %e,
                        "llm_request buffer full or closed, dropping observability write"
                    );
                }
            });

            tracing::debug!(
                request_id = %request_id,
                project_id = %project_id,
                model = %request.model,
                provider = %provider_name,
                duration_ms = %duration.as_millis(),
                "Gateway cache hit"
            );

            // Per-project OTel: cache hit metrics + span
            emit_project_cache_hit(
                &state,
                project_id,
                provider_name,
                &request.model,
                duration,
                &cached.response,
                &request_id,
            );

            // Return cached response with cache header
            let cached_model = cached.response.model.clone();
            let mut resp = Json(cached.response).into_response();
            resp.headers_mut().insert(
                HeaderName::from_static("x-reiver-cache"),
                HeaderValue::from_static(cache_status.as_str()),
            );
            resp.headers_mut().insert(
                HeaderName::from_static("x-reiver-provider"),
                header_value(provider_name, "unknown"),
            );
            resp.headers_mut().insert(
                HeaderName::from_static("x-reiver-model-used"),
                header_value(&cached_model, "unknown"),
            );
            resp.headers_mut().insert(
                HeaderName::from_static("x-request-id"),
                header_value(&request_id, "unknown"),
            );
            return Ok(resp);
        }
    }
    root_span.record("cache", cache_status.as_str());

    // 8. Execute non-streaming request with retry and fallback
    let fallback_result = execute_with_fallback(
        &state,
        &gateway_router,
        &request,
        &chain,
        &state.fallback_config,
        project_id,
    )
    .await;

    // Extract result, provider used, fallback metadata, and add fallback headers
    let mut model_used_for_cost = request.model.clone();
    let mut obs_fallback_used = false;
    let obs_original_model = request.model.clone();
    let mut obs_retry_count: u32 = 0;
    let (result, mut response_headers, actual_provider) = match fallback_result {
        Ok(fb_result) => {
            let mut headers = Vec::new();
            let actual_provider = fb_result.provider_used.to_string();
            model_used_for_cost = fb_result.model_used.clone();
            obs_fallback_used = fb_result.fallback_used;
            obs_retry_count = fb_result.retry_count;
            headers.push((
                HeaderName::from_static("x-reiver-provider"),
                header_value(fb_result.provider_used.as_str(), "unknown"),
            ));
            headers.push((
                HeaderName::from_static("x-reiver-model-used"),
                header_value(&fb_result.model_used, "unknown"),
            ));
            if fb_result.fallback_used {
                headers.push((
                    HeaderName::from_static("x-reiver-fallback-used"),
                    HeaderValue::from_static("true"),
                ));
                headers.push((
                    HeaderName::from_static("x-reiver-original-model"),
                    header_value(&request.model, "unknown"),
                ));
            }
            if fb_result.retry_count > 0 {
                headers.push((
                    HeaderName::from_static("x-reiver-retry-count"),
                    header_value(&fb_result.retry_count.to_string(), "0"),
                ));
            }
            (Ok(fb_result.result), headers, actual_provider)
        }
        Err(e) => (Err(e), Vec::new(), provider_name.to_string()),
    };

    let mut result: Result<ChatCompletionResponse, GatewayError> = result;

    let duration = start.elapsed();

    let tracker = state.gateway_router.latency_tracker();
    tracker.record(&actual_provider, duration);

    {
        let cb = state.gateway_router.circuit_breaker();
        if result.is_ok() && !obs_fallback_used {
            cb.record_success(&provider);
        } else if result.is_err() || obs_fallback_used {
            cb.record_failure(&provider);
        }
    }

    if settings.session_budget_usd.is_some() && !session_id.is_empty() {
        if let Ok(ref response) = result {
            increment_session_budget(
                &state,
                project_id,
                &session_id,
                &actual_provider,
                &model_used_for_cost,
                &response.usage,
                settings.session_budget_usd,
            )
            .await;
        }
    }

    // Capture token usage before output guardrails may turn Ok→Err.
    // The user is billed regardless of guardrail blocks, so the dashboard must match.
    let pre_guardrail_usage = result.as_ref().ok().map(|r| r.usage.clone());

    let allowed_tools_for_guardrails: Option<Vec<String>> = prompt_resolution
        .as_ref()
        .and_then(|r| r.allowed_tools.clone());
    result = apply_output_guardrails(
        &state,
        &settings.guardrail_config,
        result,
        project_id,
        &request_id,
        allowed_tools_for_guardrails.as_deref(),
        &actual_provider,
        &request.model,
    )
    .await;

    if let Ok(ref response) = result {
        let response_text = response
            .choices
            .first()
            .and_then(|c| c.message.content.as_deref())
            .unwrap_or_default();
        spawn_judge_if_sampled(
            &state,
            &request,
            response_text,
            billing_pid,
            &request_id,
            &prompt_resolution,
            settings.judge_sample_rate,
        );
    }

    result = enforce_output_contract(
        &OutputContractContext {
            state: &state,
            gateway_router: &gateway_router,
            request: &request,
            provider_key: &chain[0].key,
            model_id: request.model.clone(),
            provider,
            project_id,
            request_id: &request_id,
            prompt_resolution: &prompt_resolution,
            is_platform_key,
            provider_impl: chain[0].provider_impl.clone(),
        },
        result,
        &mut response_headers,
    )
    .await;

    let obs_guardrail_violations: Vec<String> = match &result {
        Err(GatewayError::GuardrailViolation { rule, .. }) => vec![rule.to_string()],
        _ => Vec::new(),
    };

    // Per-project OTel: emit metrics + span AFTER output guardrails
    // so the span reflects the final user-facing result.
    emit_project_request_otel(
        &state,
        project_id,
        &actual_provider,
        &request.model,
        duration,
        &result,
        pre_guardrail_usage.as_ref(),
        should_cache,
        obs_fallback_used,
        obs_retry_count,
        &request_id,
    );

    emit_non_streaming_observability(
        &state,
        &request,
        &result,
        &actual_provider,
        project_id,
        billing_pid,
        &request_id,
        duration,
        prompt_resolution.as_ref(),
        &session_id,
        &session_name,
        log_content,
        obs_fallback_used,
        obs_original_model,
        obs_retry_count,
        obs_guardrail_violations,
        is_platform_key,
        org_id,
    );

    // Store in cache if this was a cacheable request that missed
    if should_cache && cache_status == CacheStatus::Miss {
        if let Ok(ref response) = result {
            let gateway_cache = state.gateway_cache.clone();
            let request_for_cache = request.clone();
            let response_for_cache = response.clone();

            tokio::spawn(async move {
                gateway_cache
                    .set(project_id, &request_for_cache, &response_for_cache)
                    .await;
            });
        }
    }

    // Return response with headers
    match result {
        Ok(response) => {
            let mut resp = Json(response).into_response();
            for (name, value) in response_headers {
                resp.headers_mut().insert(name, value);
            }
            resp.headers_mut().insert(
                HeaderName::from_static("x-reiver-cache"),
                HeaderValue::from_static(cache_status.as_str()),
            );
            resp.headers_mut().insert(
                HeaderName::from_static("x-request-id"),
                header_value(&request_id, "unknown"),
            );

            tracing::debug!(
                request_id = %request_id,
                project_id = %project_id,
                model = %request.model,
                provider = %actual_provider,
                duration_ms = %duration.as_millis(),
                cache_status = %cache_status.as_str(),
                "Gateway request completed successfully"
            );

            Ok(resp)
        }
        Err(e) => {
            tracing::warn!(
                request_id = %request_id,
                project_id = %project_id,
                model = %request.model,
                provider = %actual_provider,
                duration_ms = %duration.as_millis(),
                error = %e,
                "Gateway request failed"
            );
            Err(e)
        }
    }
}

// ---------------------------------------------------------------------------
// Per-project OTel publisher helpers
// ---------------------------------------------------------------------------

/// Emit per-project OTel data for a cache-hit response.
fn emit_project_cache_hit(
    state: &FlowState,
    project_id: Uuid,
    provider: &str,
    model: &str,
    duration: std::time::Duration,
    response: &ChatCompletionResponse,
    request_id: &str,
) {
    let mut labels = BTreeMap::new();
    labels.insert("gen_ai.provider.name".into(), provider.to_string());
    labels.insert("gen_ai.request.model".into(), model.to_string());

    state.otel_publisher.emit_counter(
        project_id,
        "gen_ai.client.cache.hit",
        1.0,
        labels.clone(),
    );

    labels.insert("gen_ai.operation.name".into(), "chat".into());
    state.otel_publisher.emit_histogram(
        project_id,
        "gen_ai.client.operation.duration",
        duration.as_secs_f64(),
        labels.clone(),
    );

    // Token usage from cached response
    let usage = &response.usage;
    if usage.prompt_tokens > 0 {
        let mut input_labels = labels.clone();
        input_labels.insert("gen_ai.token.type".into(), "input".into());
        state.otel_publisher.emit_counter(
            project_id,
            "gen_ai.client.token.usage",
            usage.prompt_tokens as f64,
            input_labels,
        );
    }
    if usage.completion_tokens > 0 {
        let mut output_labels = labels.clone();
        output_labels.insert("gen_ai.token.type".into(), "output".into());
        state.otel_publisher.emit_counter(
            project_id,
            "gen_ai.client.token.usage",
            usage.completion_tokens as f64,
            output_labels,
        );
    }

    // Emit span for the cache-hit operation
    let now = chrono::Utc::now();
    let start_time = now - chrono::Duration::from_std(duration).unwrap_or_default();
    let mut span_attrs = std::collections::HashMap::new();
    span_attrs.insert("gen_ai.provider.name".into(), provider.to_string());
    span_attrs.insert("gen_ai.request.model".into(), model.to_string());
    span_attrs.insert("gen_ai.operation.name".into(), "chat".into());
    span_attrs.insert("request_id".into(), request_id.to_string());
    span_attrs.insert("cache.hit".into(), "true".into());
    span_attrs.insert(
        "gen_ai.usage.input_tokens".into(),
        usage.prompt_tokens.to_string(),
    );
    span_attrs.insert(
        "gen_ai.usage.output_tokens".into(),
        usage.completion_tokens.to_string(),
    );

    state.otel_publisher.emit_span(
        project_id,
        SpanData {
            project_key: project_id.to_string(),
            trace_id: uuid::Uuid::new_v4().to_string().replace('-', ""),
            span_id: uuid::Uuid::new_v4().to_string().replace('-', "")[..16].to_string(),
            parent_span_id: None,
            span_name: format!("gen_ai.chat {}", model),
            span_kind: "SPAN_KIND_CLIENT".into(),
            service_name: None,
            start_time: Some(start_time),
            duration_ns: Some(duration.as_nanos() as i64),
            status_code: "STATUS_CODE_OK".into(),
            status_message: None,
            span_attributes: span_attrs,
            resource_attributes: std::collections::HashMap::new(),
        },
    );
}

/// Emit per-project OTel data for a completed (non-cached) LLM request.
///
/// `pre_guardrail_usage` carries the token usage captured before output guardrails
/// may have turned a successful response into an error. This ensures token metrics
/// match billing even when the response was blocked by a guardrail.
fn emit_project_request_otel(
    state: &FlowState,
    project_id: Uuid,
    provider: &str,
    model: &str,
    duration: std::time::Duration,
    result: &Result<ChatCompletionResponse, GatewayError>,
    pre_guardrail_usage: Option<&crate::gateway::types::Usage>,
    was_cacheable: bool,
    fallback_used: bool,
    retry_count: u32,
    request_id: &str,
) {
    let mut labels = BTreeMap::new();
    labels.insert("gen_ai.provider.name".into(), provider.to_string());
    labels.insert("gen_ai.request.model".into(), model.to_string());
    labels.insert("gen_ai.operation.name".into(), "chat".into());

    // Duration histogram — Watch derives .count, .sum, and _bucket series
    state.otel_publisher.emit_histogram(
        project_id,
        "gen_ai.client.operation.duration",
        duration.as_secs_f64(),
        labels.clone(),
    );

    // Token usage — use pre-guardrail usage (always reflects actual consumption)
    // falling back to the current result's usage if available.
    let usage = pre_guardrail_usage.or_else(|| result.as_ref().ok().map(|r| &r.usage));
    if let Some(usage) = usage {
        if usage.prompt_tokens > 0 {
            let mut input_labels = labels.clone();
            input_labels.insert("gen_ai.token.type".into(), "input".into());
            state.otel_publisher.emit_counter(
                project_id,
                "gen_ai.client.token.usage",
                usage.prompt_tokens as f64,
                input_labels,
            );
        }
        if usage.completion_tokens > 0 {
            let mut output_labels = labels.clone();
            output_labels.insert("gen_ai.token.type".into(), "output".into());
            state.otel_publisher.emit_counter(
                project_id,
                "gen_ai.client.token.usage",
                usage.completion_tokens as f64,
                output_labels,
            );
        }
    }

    // Cache miss (if cacheable)
    if was_cacheable {
        let cache_labels = BTreeMap::from([
            ("gen_ai.provider.name".into(), provider.to_string()),
            ("gen_ai.request.model".into(), model.to_string()),
        ]);
        state.otel_publisher.emit_counter(
            project_id,
            "gen_ai.client.cache.miss",
            1.0,
            cache_labels,
        );
    }

    // Fallback metric
    if fallback_used {
        let fb_labels = BTreeMap::from([
            ("gen_ai.provider.name".into(), provider.to_string()),
            ("gen_ai.request.model".into(), model.to_string()),
        ]);
        state.otel_publisher.emit_counter(
            project_id,
            "gen_ai.client.fallback.used",
            1.0,
            fb_labels,
        );
    }

    // Build span attributes
    let now = chrono::Utc::now();
    let start_time = now - chrono::Duration::from_std(duration).unwrap_or_default();
    let mut span_attrs = std::collections::HashMap::new();
    span_attrs.insert("gen_ai.provider.name".into(), provider.to_string());
    span_attrs.insert("gen_ai.request.model".into(), model.to_string());
    span_attrs.insert("gen_ai.operation.name".into(), "chat".into());
    span_attrs.insert("request_id".into(), request_id.to_string());
    span_attrs.insert("cache.hit".into(), "false".into());

    if fallback_used {
        span_attrs.insert("fallback.used".into(), "true".into());
    }
    if retry_count > 0 {
        span_attrs.insert("retry_count".into(), retry_count.to_string());
    }
    if let Some(usage) = usage {
        span_attrs.insert(
            "gen_ai.usage.input_tokens".into(),
            usage.prompt_tokens.to_string(),
        );
        span_attrs.insert(
            "gen_ai.usage.output_tokens".into(),
            usage.completion_tokens.to_string(),
        );
    }

    let (status_code, status_message) = match result {
        Ok(resp) => {
            if !resp.model.is_empty() {
                span_attrs.insert("gen_ai.response.model".into(), resp.model.clone());
            }
            ("STATUS_CODE_OK".to_string(), None)
        }
        Err(e) => {
            let error_type = match e {
                GatewayError::ProviderError { .. } => "provider_error",
                GatewayError::RateLimitExceeded { .. } => "rate_limit",
                GatewayError::GuardrailViolation { .. } => "guardrail_violation",
                GatewayError::Timeout(_) => "timeout",
                _ => "internal_error",
            };
            let mut err_labels = labels.clone();
            err_labels.insert("error.type".into(), error_type.into());
            state.otel_publisher.emit_counter(
                project_id,
                "gen_ai.client.error",
                1.0,
                err_labels,
            );

            span_attrs.insert("error.type".into(), error_type.into());
            span_attrs.insert("error.message".into(), e.to_string());

            ("STATUS_CODE_ERROR".to_string(), Some(e.to_string()))
        }
    };

    state.otel_publisher.emit_span(
        project_id,
        SpanData {
            project_key: project_id.to_string(),
            trace_id: uuid::Uuid::new_v4().to_string().replace('-', ""),
            span_id: uuid::Uuid::new_v4().to_string().replace('-', "")[..16].to_string(),
            parent_span_id: None,
            span_name: format!("gen_ai.chat {}", model),
            span_kind: "SPAN_KIND_CLIENT".into(),
            service_name: None,
            start_time: Some(start_time),
            duration_ns: Some(duration.as_nanos() as i64),
            status_code,
            status_message,
            span_attributes: span_attrs,
            resource_attributes: std::collections::HashMap::new(),
        },
    );
}

/// GET /v1/models - List available models from the in-memory catalog cache.
async fn list_models(State(state): State<Arc<FlowState>>) -> impl IntoResponse {
    let entries = state.model_catalog_cache.all_entries().await;

    let models: Vec<serde_json::Value> = entries
        .iter()
        .map(|entry| {
            serde_json::json!({
                "id": entry.gateway_model_id(),
                "object": "model",
                "owned_by": entry.provider_slug,
            })
        })
        .collect();

    Json(serde_json::json!({
        "object": "list",
        "data": models,
    }))
}

/// Fetch per-project gateway hot-path settings from the database.
///
/// Uses an in-memory cache with 60 s TTL to avoid a DB round-trip on every
/// request. Fails open — defaults are returned on any DB error so requests
/// are never blocked.
pub(crate) use settings::get_introspection_settings;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::fallback::{
        calculate_retry_delay, is_retryable_error, should_fallback, FallbackConfig, FallbackResult,
    };
    use crate::gateway::provider_types::Provider;

    #[test]
    fn test_is_retryable_error_rate_limit() {
        let error = GatewayError::RateLimitExceeded {
            limit: 100,
            reset_seconds: 60,
        };
        assert!(is_retryable_error(&error));
    }

    #[test]
    fn test_is_retryable_error_server_error() {
        let error = GatewayError::ProviderError {
            provider: Provider::OpenAi,
            status: 500,
            message: "Internal server error".to_string(),
        };
        assert!(is_retryable_error(&error));
    }

    #[test]
    fn test_is_retryable_error_502() {
        let error = GatewayError::ProviderError {
            provider: Provider::Anthropic,
            status: 502,
            message: "Bad gateway".to_string(),
        };
        assert!(is_retryable_error(&error));
    }

    #[test]
    fn test_is_retryable_error_503() {
        let error = GatewayError::ProviderError {
            provider: Provider::Google,
            status: 503,
            message: "Service unavailable".to_string(),
        };
        assert!(is_retryable_error(&error));
    }

    #[test]
    fn test_is_not_retryable_error_400() {
        let error = GatewayError::ProviderError {
            provider: Provider::OpenAi,
            status: 400,
            message: "Bad request".to_string(),
        };
        assert!(!is_retryable_error(&error));
    }

    #[test]
    fn test_is_not_retryable_error_401() {
        let error = GatewayError::AuthenticationFailed("Invalid key".to_string());
        assert!(!is_retryable_error(&error));
    }

    #[test]
    fn test_is_not_retryable_error_validation() {
        let error = GatewayError::ValidationError("Missing model".to_string());
        assert!(!is_retryable_error(&error));
    }

    // ========================================================================
    // Should Fallback Tests
    // ========================================================================

    #[test]
    fn test_should_fallback_rate_limit() {
        let error = GatewayError::RateLimitExceeded {
            limit: 100,
            reset_seconds: 60,
        };
        assert!(should_fallback(&error));
    }

    #[test]
    fn test_should_fallback_server_error() {
        let error = GatewayError::ProviderError {
            provider: Provider::OpenAi,
            status: 500,
            message: "Server error".to_string(),
        };
        assert!(should_fallback(&error));
    }

    #[test]
    fn test_should_not_fallback_auth_error() {
        let error = GatewayError::AuthenticationFailed("Invalid key".to_string());
        assert!(!should_fallback(&error));
    }

    #[test]
    fn test_should_not_fallback_validation_error() {
        let error = GatewayError::ValidationError("Invalid request".to_string());
        assert!(!should_fallback(&error));
    }

    // ========================================================================
    // Retry Delay Calculation Tests
    // ========================================================================

    #[test]
    fn test_calculate_retry_delay_exponential_backoff() {
        let config = FallbackConfig::default();

        let delay0 = calculate_retry_delay(0, &config);
        let delay1 = calculate_retry_delay(1, &config);
        let delay2 = calculate_retry_delay(2, &config);

        // Each delay should be at least as long as the previous (equal is
        // possible when jitter ranges overlap at boundaries).
        assert!(delay1 >= delay0);
        assert!(delay2 >= delay1);
    }

    #[test]
    fn test_calculate_retry_delay_respects_max() {
        let config = FallbackConfig::default();

        // After many retries, delay should be capped at max
        let delay_high = calculate_retry_delay(10, &config);
        assert!(delay_high <= config.max_retry_delay);
    }

    // ========================================================================
    // Fallback Config Tests
    // ========================================================================

    #[test]
    fn test_fallback_config_default() {
        let config = FallbackConfig::default();

        assert_eq!(config.max_retries, 2);
        assert!(config.enable_fallback);
    }

    // ========================================================================
    // FallbackResult Tests
    // ========================================================================

    #[test]
    fn test_fallback_result_success() {
        let fallback_result =
            FallbackResult::primary("response".to_string(), "gpt-4o".to_string(), Provider::OpenAi, 0);

        assert!(!fallback_result.fallback_used);
        assert_eq!(fallback_result.retry_count, 0);
        assert_eq!(fallback_result.provider_used, Provider::OpenAi);
        assert_eq!(fallback_result.model_used, "gpt-4o");
    }

    #[test]
    fn test_fallback_result_with_fallback() {
        let fallback_result = FallbackResult::fallback(
            "response".to_string(),
            "claude-sonnet-4-6".to_string(),
            Provider::Anthropic,
            2,
        );

        assert!(fallback_result.fallback_used);
        assert_eq!(fallback_result.model_used, "claude-sonnet-4-6");
        assert_eq!(fallback_result.retry_count, 2);
        assert_eq!(fallback_result.provider_used, Provider::Anthropic);
    }

    // ========================================================================
    // Gateway-only field stripping tests
    // ========================================================================

    /// Regression: gateway-only fields `prompt_config` and `prompt_variables`
    /// must be cleared before the request is forwarded to providers.
    /// OpenAI's passthrough serializes the full struct, and unknown fields
    /// cause HTTP 400 from the OpenAI API.
    #[test]
    fn test_gateway_fields_cleared_before_serialization() {
        use crate::gateway::types::{ChatMessage, MessageContent, MessageRole};
        use std::collections::HashMap;

        let mut request = ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text("hi".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            }],
            prompt_config: Some("my-prompt".to_string()),
            prompt_variables: Some(HashMap::from([(
                "user".to_string(),
                serde_json::json!("Alice"),
            )])),
            ..Default::default()
        };

        // Simulate the clearing that happens in the route handler
        request.prompt_config = None;
        request.prompt_variables = None;

        let body = serde_json::to_value(&request).unwrap();
        assert!(
            body.get("prompt_config").is_none(),
            "prompt_config must not appear in serialized request"
        );
        assert!(
            body.get("prompt_variables").is_none(),
            "prompt_variables must not appear in serialized request"
        );
    }

    #[test]
    fn test_models_and_provider_stripped_before_serialization() {
        let mut request = make_simple_request_for_routes();
        request.models = Some(vec!["gpt-4o".into(), "claude-sonnet-4-6".into()]);
        request.provider = Some(crate::gateway::types::ProviderPreferences {
            order: Some(vec!["anthropic".into()]),
            ..Default::default()
        });

        let _models = request.models.take();
        let _provider = request.provider.take();

        let body = serde_json::to_value(&request).unwrap();
        assert!(
            body.get("models").is_none(),
            "models must not appear in serialized request after take()"
        );
        assert!(
            body.get("provider").is_none(),
            "provider must not appear in serialized request after take()"
        );
    }

    fn make_simple_request_for_routes() -> ChatCompletionRequest {
        use crate::gateway::types::{ChatMessage, MessageContent, MessageRole};
        ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text("hello".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            }],
            ..Default::default()
        }
    }
}
