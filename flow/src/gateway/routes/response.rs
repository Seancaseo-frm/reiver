//! Response formatting, observability, billing hooks, and output guardrails.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::http::{HeaderName, HeaderValue};
use axum::response::{
    sse::{Event, Sse},
    IntoResponse, Response,
};
use futures::stream::StreamExt;
use uuid::Uuid;

use std::collections::BTreeMap;

use crate::app_state::FlowState;
use crate::gateway::domain_types::OutputFailureAction;
use crate::gateway::error::GatewayError;
use crate::gateway::observability::{
    build_error_llm_request, build_llm_request, build_streaming_llm_request, ErrorLlmRequestParams,
    LlmRequestParams, StreamingLlmRequestParams,
};
use crate::gateway::otel_publisher::SpanData;
use crate::gateway::prompt_resolver::PromptResolution;
use crate::gateway::provider_types::Provider;
use crate::gateway::providers::ChatCompletionStream;
use crate::gateway::router::GatewayRouter;
use crate::gateway::stream_processor::{StreamChunkProcessor, StreamCompletionSummary};
use crate::gateway::types::{ChatCompletionRequest, ChatCompletionResponse, Usage};

use super::execution::execute_with_fallback;
use super::resolve::ProviderCandidate;
use super::validation::increment_session_budget;

// ========================================================================
// Output guardrails
// ========================================================================

/// Apply output guardrails: PII masking, topic blocklist, tool call validation,
/// exfiltration scanning, LLM-as-judge quality check.
pub(super) async fn apply_output_guardrails(
    state: &Arc<FlowState>,
    guardrail_config: &crate::gateway::guardrails::GuardrailConfig,
    mut result: Result<ChatCompletionResponse, GatewayError>,
    project_id: Uuid,
    request_id: &str,
    allowed_tools: Option<&[String]>,
    provider: &str,
    model: &str,
) -> Result<ChatCompletionResponse, GatewayError> {
    if guardrail_config.is_noop() {
        return result;
    }

    if let Ok(ref mut response) = result {
        use crate::gateway::guardrails::{check_output_guardrails, OutputGuardrailCheck};

        let mut all_response_text = Vec::new();
        let mut all_thinking_text = Vec::new();

        for choice in &mut response.choices {
            if guardrail_config.mask_output_pii {
                if let Some(ref mut content) = choice.message.content {
                    if let Some(masked) = crate::pii::redact_if_changed(content) {
                        *content = masked;
                    }
                }
                if let Some(ref mut thinking) = choice.message.thinking {
                    if let Some(masked) = crate::pii::redact_if_changed(&thinking.content) {
                        thinking.content = masked;
                    }
                }
            }

            if let Some(ref content) = choice.message.content {
                all_response_text.push(content.clone());
            }
            if let Some(ref thinking) = choice.message.thinking {
                all_thinking_text.push(thinking.content.clone());
            }
        }

        let response_text = all_response_text.join("\n");
        let thinking_text: Option<String> = if all_thinking_text.is_empty() {
            None
        } else {
            Some(all_thinking_text.join("\n"))
        };

        // Collect tool call names from the response for validation
        let tool_call_names: Vec<&str> = response
            .choices
            .iter()
            .flat_map(|c| c.message.tool_calls.iter().flatten())
            .map(|tc| tc.function.name.as_str())
            .collect();

        {
            match check_output_guardrails(
                guardrail_config,
                &response_text,
                thinking_text.as_deref(),
                &tool_call_names,
                allowed_tools,
            ) {
                OutputGuardrailCheck::Block(violation) => {
                    tracing::info!(
                        request_id = %request_id,
                        project_id = %project_id,
                        rule = %violation.rule,
                        "Output guardrail triggered, blocking response"
                    );

                    // Per-project OTel: output guardrail blocked metric
                    state.otel_publisher.emit_counter(
                        project_id,
                        "gen_ai.client.guardrail.blocked",
                        1.0,
                        BTreeMap::from([
                            ("gen_ai.provider.name".into(), provider.to_string()),
                            ("gen_ai.request.model".into(), model.to_string()),
                            ("guardrail.rule".into(), violation.rule.to_string()),
                        ]),
                    );

                    let _ = state
                        .event_publisher
                        .emit(
                            reiver_core::events::PlatformEventType::LlmGuardrailTriggered,
                            project_id,
                            format!("guardrail_output:{}", request_id),
                            serde_json::json!({
                                "rule": violation.rule.to_string(),
                                "phase": "output",
                                "request_id": request_id,
                            }),
                        )
                        .await;
                    result = Err(GatewayError::GuardrailViolation {
                        rule: violation.rule,
                        detail: violation.detail,
                    });
                }
                OutputGuardrailCheck::Pass => {}
            }
        }
    }

    result
}

// ========================================================================
// LLM-as-judge evaluation
// ========================================================================

/// Spawn a background LLM-as-judge evaluation if:
/// 1. A prompt config was resolved for this request (Prompt Hub integration).
/// 2. The project has a `judge_sample_rate` > 0.
/// 3. This request was randomly sampled at that rate.
///
/// Used for prompt version quality comparison and rollout quality gates.
pub(super) fn spawn_judge_if_sampled(
    state: &Arc<FlowState>,
    request: &ChatCompletionRequest,
    response_text: &str,
    billing_project_id: Uuid,
    request_id: &str,
    prompt_resolution: &Option<PromptResolution>,
    judge_sample_rate: Option<f64>,
) {
    let _resolution = match prompt_resolution {
        Some(r) => r,
        None => return,
    };
    let rate = match judge_sample_rate {
        Some(r) if r > 0.0 => r,
        _ => return,
    };
    if rand::random::<f64>() >= rate {
        return;
    }

    use crate::gateway::types::MessageContent;
    let user_query: String = request
        .messages
        .iter()
        .filter(|m| m.role == crate::gateway::types::MessageRole::User)
        .filter_map(|m| {
            if let Some(MessageContent::Text(s)) = &m.content {
                Some(s.as_str())
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("\n");

    let response_text = response_text.to_string();
    let state = state.clone();
    let request_id = request_id.to_string();
    let db = state.db.clone();

    tokio::spawn(async move {
        use crate::gateway::evaluator::{persist_judge_scores, run_llm_judge};
        match run_llm_judge(&state, billing_project_id, &user_query, &response_text).await {
            Some(scores) => {
                tracing::info!(
                    request_id = %request_id,
                    project_id = %billing_project_id,
                    score = %scores.average,
                    "LLM-as-judge prompt quality score"
                );
                persist_judge_scores(&db, billing_project_id, &request_id, &scores).await;
            }
            None => {
                tracing::warn!(
                    request_id = %request_id,
                    project_id = %billing_project_id,
                    "LLM-as-judge evaluation failed, skipping"
                );
            }
        }
    });
}

// ========================================================================
// Output contract enforcement
// ========================================================================

/// Parameters for the output contract enforcement pipeline.
pub(super) struct OutputContractContext<'a> {
    pub(super) state: &'a Arc<FlowState>,
    pub(super) gateway_router: &'a GatewayRouter,
    pub(super) request: &'a ChatCompletionRequest,
    pub(super) provider_key: &'a str,
    pub(super) model_id: String,
    pub(super) provider: Provider,
    pub(super) project_id: Uuid,
    pub(super) request_id: &'a str,
    pub(super) prompt_resolution: &'a Option<PromptResolution>,
    pub(super) is_platform_key: bool,
    pub(super) provider_impl: Arc<dyn crate::gateway::providers::LlmProvider>,
}

/// Enforce the output contract (JSON schema) on non-streaming responses.
pub(super) async fn enforce_output_contract(
    ctx: &OutputContractContext<'_>,
    mut result: Result<ChatCompletionResponse, GatewayError>,
    response_headers: &mut Vec<(HeaderName, HeaderValue)>,
) -> Result<ChatCompletionResponse, GatewayError> {
    let resolution = match ctx.prompt_resolution {
        Some(r) => r,
        None => return result,
    };
    let schema = match resolution.output_schema {
        Some(ref s) => s,
        None => return result,
    };

    let schema_violation: Option<String> = if let Ok(ref response) = result {
        let response_text = response
            .choices
            .first()
            .and_then(|c| c.message.content.as_deref())
            .unwrap_or("")
            .to_string();
        validate_against_schema(&response_text, schema).err()
    } else {
        None
    };

    let detail = match schema_violation {
        Some(d) => d,
        None => return result,
    };

    let action = resolution.output_failure_action;
    match action {
        OutputFailureAction::LogOnly => {
            tracing::warn!(
                request_id = %ctx.request_id,
                project_id = %ctx.project_id,
                detail = %detail,
                "Output contract violation (log_only)"
            );
        }
        OutputFailureAction::Retry | OutputFailureAction::RetryThenPassthrough => {
            let retry_candidate = ProviderCandidate {
                provider: ctx.provider,
                model: ctx.model_id.clone(),
                key: ctx.provider_key.to_string(),
                is_platform_key: ctx.is_platform_key,
                provider_impl: ctx.provider_impl.clone(),
            };
            let retry_fb = execute_with_fallback(
                ctx.state,
                ctx.gateway_router,
                ctx.request,
                &[retry_candidate],
                &ctx.state.fallback_config,
                ctx.project_id,
            )
            .await;

            match retry_fb {
                Ok(fb) => {
                    let retry_text = fb
                        .result
                        .choices
                        .first()
                        .and_then(|c| c.message.content.as_deref())
                        .unwrap_or("")
                        .to_string();
                    match validate_against_schema(&retry_text, schema) {
                        Ok(()) => result = Ok(fb.result),
                        Err(retry_detail) => {
                            if action == OutputFailureAction::RetryThenPassthrough {
                                tracing::warn!(
                                    request_id = %ctx.request_id,
                                    project_id = %ctx.project_id,
                                    detail = %retry_detail,
                                    "Output contract still violated after retry (passthrough)"
                                );
                                response_headers.push((
                                    HeaderName::from_static("x-output-contract-violation"),
                                    HeaderValue::from_static("true"),
                                ));
                            } else {
                                result = Err(GatewayError::OutputContractViolation {
                                    detail: retry_detail,
                                });
                            }
                        }
                    }
                }
                Err(e) => {
                    if action == OutputFailureAction::RetryThenPassthrough {
                        tracing::warn!(
                            request_id = %ctx.request_id,
                            project_id = %ctx.project_id,
                            error = %e,
                            "Retry for output contract failed (passthrough)"
                        );
                        response_headers.push((
                            HeaderName::from_static("x-output-contract-violation"),
                            HeaderValue::from_static("true"),
                        ));
                    } else {
                        result = Err(GatewayError::OutputContractViolation {
                            detail: format!("retry provider call failed: {}", e),
                        });
                    }
                }
            }
        }
        OutputFailureAction::Error => {
            result = Err(GatewayError::OutputContractViolation { detail });
        }
    }

    result
}

/// Validate a JSON response text against a JSON schema.
///
/// Returns `Ok(())` if the text is valid JSON conforming to the schema.
/// Returns `Err(human_readable_reason)` otherwise.
fn validate_against_schema(text: &str, schema: &serde_json::Value) -> Result<(), String> {
    let instance: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(e) => return Err(format!("response is not valid JSON: {}", e)),
    };

    match jsonschema::validate(schema, &instance) {
        Ok(()) => Ok(()),
        Err(e) => Err(e.to_string()),
    }
}

// ========================================================================
// Non-streaming observability
// ========================================================================

/// Emit observability data for a non-streaming request in a background task.
pub(super) fn emit_non_streaming_observability(
    state: &Arc<FlowState>,
    request: &ChatCompletionRequest,
    result: &Result<ChatCompletionResponse, GatewayError>,
    actual_provider: &str,
    project_id: Uuid,
    billing_project_id: Uuid,
    _request_id: &str,
    duration: Duration,
    prompt_resolution: Option<&PromptResolution>,
    session_id: &str,
    session_name: &str,
    log_content: bool,
    fallback_used: bool,
    original_model: String,
    retry_count: u32,
    guardrail_violations: Vec<String>,
    is_platform_key: bool,
    org_id: Option<Uuid>,
) {
    use super::observability::{finalize_and_send, BillingContext};

    let state = state.clone();
    let request_clone = request.clone();
    let actual_provider = actual_provider.to_string();
    let prompt_resolution = prompt_resolution.cloned();
    let session_id = session_id.to_string();
    let session_name = session_name.to_string();

    let result_for_obs: Result<ChatCompletionResponse, (String, String)> = match result {
        Ok(response) => Ok(response.clone()),
        Err(e) => {
            let error_type = match e {
                GatewayError::ProviderError { status, .. } => format!("provider_error_{}", status),
                GatewayError::RateLimitExceeded { .. } => "rate_limit_exceeded".to_string(),
                GatewayError::Timeout(_) => "timeout".to_string(),
                _ => "error".to_string(),
            };
            Err((error_type, e.to_string()))
        }
    };

    tokio::spawn(async move {
        let mut llm_request = match result_for_obs {
            Ok(response) => build_llm_request(LlmRequestParams {
                project_id,
                request: &request_clone,
                response: &response,
                provider: &actual_provider,
                duration,
                log_content,
                fallback_used,
                original_model,
                retry_count,
                guardrail_violations,
                is_platform_key,
            }),
            Err((error_type, error_message)) => build_error_llm_request(ErrorLlmRequestParams {
                project_id,
                request: &request_clone,
                provider: &actual_provider,
                duration,
                error_type: &error_type,
                error_message: &error_message,
                log_content,
                fallback_used,
                original_model,
                retry_count,
                guardrail_violations,
                is_platform_key,
            }),
        };

        if let Some(ref resolution) = prompt_resolution {
            llm_request.rollout_id = resolution
                .rollout_id
                .map(|id| id.to_string())
                .unwrap_or_default();
            llm_request.rollout_variant = resolution.variant.to_string();
            llm_request.prompt_config_id = resolution.config_id.to_string();
            llm_request.prompt_version_id = resolution.version_id.to_string();
        }

        let billing = BillingContext {
            _billing_project_id: billing_project_id,
            is_platform_key,
            org_id,
        };
        finalize_and_send(&state, llm_request, &billing, &session_id, &session_name).await;
    });
}

// ========================================================================
// Streaming response handler
// ========================================================================

/// Context for handling a streaming response with observability and fallback headers.
pub(super) struct StreamingResponseContext {
    pub(super) chunk_stream: ChatCompletionStream,
    pub(super) request: ChatCompletionRequest,
    pub(super) project_id: Uuid,
    pub(super) billing_project_id: Uuid,
    pub(super) provider_name: &'static str,
    pub(super) start: Instant,
    pub(super) request_id: String,
    pub(super) prompt_resolution: Option<PromptResolution>,
    pub(super) fallback_used: bool,
    pub(super) model_used: String,
    pub(super) retry_count: u32,
    pub(super) session_id: String,
    pub(super) session_name: String,
    pub(super) session_budget_usd: Option<f64>,
    pub(super) guardrail_config: crate::gateway::guardrails::GuardrailConfig,
    pub(super) judge_sample_rate: Option<f64>,
    pub(super) log_content: bool,
    pub(super) is_platform_key: bool,
    pub(super) org_id: Option<Uuid>,
}

/// Handle streaming chat completion request.
///
/// Returns an SSE stream of ChatCompletionChunk objects.
/// Each chunk is sent directly to Kafka for real-time observability.
/// No in-memory buffering - chunks are stored in ClickHouse via Kafka.
pub(super) async fn handle_streaming_response(
    state: Arc<FlowState>,
    ctx: StreamingResponseContext,
) -> Result<Response, GatewayError> {
    let _span = tracing::info_span!(
        "gateway.stream.response",
        provider = %ctx.provider_name,
        model = %ctx.model_used,
        project_id = %ctx.project_id,
    )
    .entered();

    state.metrics.active_streams.add(1, &[]);

    let project_id = ctx.project_id;
    let latency_tracker = state.gateway_router.latency_tracker();

    let (mut processor_inner, completion_rx) = StreamChunkProcessor::new(
        ctx.model_used.clone(),
        ctx.start,
        state.kafka.clone(),
        ctx.provider_name.to_string(),
        project_id,
        Some(latency_tracker),
    );
    processor_inner.mask_output_pii = ctx.guardrail_config.mask_output_pii;
    processor_inner.block_exfiltration_urls = ctx.guardrail_config.block_exfiltration_urls;
    processor_inner.blocked_tools = ctx.guardrail_config.blocked_tools.clone();
    processor_inner.allowed_tools = ctx
        .prompt_resolution
        .as_ref()
        .and_then(|r| r.allowed_tools.clone());
    processor_inner.client_include_usage = ctx
        .request
        .stream_options
        .as_ref()
        .map_or(false, |opts| opts.include_usage);

    // Enable judge buffer when prompt config is resolved and judge is sampled
    let judge_enabled_for_stream = ctx.prompt_resolution.is_some()
        && ctx
            .judge_sample_rate
            .map_or(false, |r| r > 0.0 && rand::random::<f64>() < r);
    // Enable response content accumulation for judge OR session profile content logging
    if judge_enabled_for_stream || ctx.log_content {
        processor_inner.judge_buffer = Some(parking_lot::Mutex::new(String::with_capacity(4096)));
    }

    let processor = Arc::new(processor_inner);

    // Create an SSE stream that transforms chunks to SSE events and sends to Kafka
    let processor_for_stream = processor.clone();
    let sse_stream = ctx
        .chunk_stream
        .map(move |chunk_result| processor_for_stream.process(chunk_result));

    // Clone state for the done callback
    let state_for_done = state.clone();
    let request_for_done = ctx.request.clone();
    let log_content = ctx.log_content;
    let request_id_for_done = ctx.request_id.clone();
    let prompt_resolution_for_done = ctx.prompt_resolution.clone();
    let provider_name = ctx.provider_name;
    let start = ctx.start;
    let session_id_for_done = ctx.session_id.clone();
    let session_name_for_done = ctx.session_name.clone();
    let session_budget_usd_for_done = ctx.session_budget_usd;
    let judge_enabled_for_done = judge_enabled_for_stream;
    let obs_fallback_used = ctx.fallback_used;
    let obs_original_model = request_for_done.model.clone();
    let obs_retry_count = ctx.retry_count;
    let is_platform_key_for_done = ctx.is_platform_key;
    let billing_pid_for_done = ctx.billing_project_id;
    let org_id_for_done = ctx.org_id;

    const COMPLETION_RECV_TIMEOUT: Duration = Duration::from_secs(2);

    // Add [DONE] marker at the end and record summary observability
    let done_stream = futures::stream::once(async move {
        let duration = start.elapsed();

        // Receive completion summary (with timeout if stream ended without final chunk)
        let mut summary: StreamCompletionSummary =
            match tokio::time::timeout(COMPLETION_RECV_TIMEOUT, completion_rx).await {
                Ok(Ok(s)) => s,
                Ok(Err(_)) => StreamCompletionSummary {
                    model: String::new(),
                    usage: None,
                    ttfb_ms: 0,
                    error: Some("stream ended without completion".to_string()),
                    response_content: None,
                },
                Err(_) => StreamCompletionSummary {
                    model: String::new(),
                    usage: None,
                    ttfb_ms: 0,
                    error: Some("stream ended without final chunk (timeout)".to_string()),
                    response_content: None,
                },
            };

        let model_for_log = summary.model.clone();
        let usage_for_budget = summary.usage.clone();
        let judge_response_content = summary.response_content.take();

        // Detect guardrail violations from the stream processor
        let stream_guardrail_violations: Vec<String> = if let Some(ref err) = summary.error {
            if err.contains("guardrail") || err.contains("Guardrail") {
                vec![err.clone()]
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        // Build LLM request for summary metrics
        let mut llm_request = if let Some(ref error_msg) = summary.error {
            build_error_llm_request(ErrorLlmRequestParams {
                project_id,
                request: &request_for_done,
                provider: provider_name,
                duration,
                error_type: "stream_error",
                error_message: error_msg,
                log_content,
                fallback_used: obs_fallback_used,
                original_model: obs_original_model,
                retry_count: obs_retry_count,
                guardrail_violations: stream_guardrail_violations,
                is_platform_key: is_platform_key_for_done,
            })
        } else {
            build_streaming_llm_request(StreamingLlmRequestParams {
                project_id,
                request: &request_for_done,
                provider: provider_name,
                model: summary.model,
                duration,
                time_to_first_token_ms: summary.ttfb_ms,
                usage: summary.usage,
                log_content,
                fallback_used: obs_fallback_used,
                original_model: obs_original_model,
                retry_count: obs_retry_count,
                guardrail_violations: stream_guardrail_violations,
                is_platform_key: is_platform_key_for_done,
            })
        };

        // Add rollout tracking info
        if let Some(ref resolution) = prompt_resolution_for_done {
            llm_request.rollout_id = resolution
                .rollout_id
                .map(|id| id.to_string())
                .unwrap_or_default();
            llm_request.rollout_variant = resolution.variant.to_string();
            llm_request.prompt_config_id = resolution.config_id.to_string();
            llm_request.prompt_version_id = resolution.version_id.to_string();
        }

        llm_request.session_id = session_id_for_done.clone();
        llm_request.session_name = session_name_for_done;

        // Populate streaming response_content from the accumulated judge buffer
        // when content logging is enabled (needed for session profile replay).
        if log_content {
            if let Some(ref content) = judge_response_content {
                llm_request.response_content = content.clone();
            }
        }

        // Spawn background task for summary observability (batched insert)
        let llm_processor = state_for_done.llm_processor.clone();
        let llm_request_tx = state_for_done.llm_request_tx.clone();
        let request_id_for_spawn = request_id_for_done.clone();
        let model_for_spawn = model_for_log.clone();
        let meter_service_for_spawn = state_for_done.meter_service.clone();
        tokio::spawn(async move {
            let prepared = match llm_processor.prepare_gateway_request(llm_request).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!(
                        request_id = %request_id_for_spawn,
                        project_id = %project_id,
                        model = %model_for_spawn,
                        error = %e,
                        "Failed to prepare streaming gateway request for batch"
                    );
                    return;
                }
            };

            // Usage billing for streaming requests.
            if is_platform_key_for_done {
                if let Some(oid) = org_id_for_done {
                    let cost = prepared.cost_usd;
                    if cost > rust_decimal::Decimal::ZERO {
                        meter_service_for_spawn.record_usage(oid, cost);
                    }
                }
            }

            if let Err(e) = llm_request_tx.try_send(prepared) {
                tracing::warn!(
                    request_id = %request_id_for_spawn,
                    error = %e,
                    "llm_request buffer full or closed, dropping observability write"
                );
            }
        });

        if session_budget_usd_for_done.is_some() && !session_id_for_done.is_empty() {
            if let Some(ref usage) = usage_for_budget {
                increment_session_budget(
                    &state_for_done,
                    project_id,
                    &session_id_for_done,
                    provider_name,
                    &model_for_log,
                    usage,
                    session_budget_usd_for_done,
                )
                .await;
            }
        }

        state_for_done.metrics.active_streams.add(-1, &[]);

        // Per-project OTel: emit metrics + span for the streaming request
        emit_project_streaming_otel(
            &state_for_done,
            project_id,
            provider_name,
            &model_for_log,
            duration,
            usage_for_budget.as_ref(),
            summary.error.as_deref(),
            obs_fallback_used,
            obs_retry_count,
            summary.ttfb_ms,
            &request_id_for_done,
        );

        tracing::debug!(
            request_id = %request_id_for_done,
            project_id = %project_id,
            model = %model_for_log,
            provider = %provider_name,
            duration_ms = %duration.as_millis(),
            "Streaming gateway request completed"
        );

        // Post-stream LLM-as-judge evaluation
        if judge_enabled_for_done {
            if let Some(response_content) = judge_response_content {
                if !response_content.is_empty() {
                    use crate::gateway::types::MessageContent;
                    let user_query: String = request_for_done
                        .messages
                        .iter()
                        .filter(|m| m.role == crate::gateway::types::MessageRole::User)
                        .filter_map(|m| {
                            if let Some(MessageContent::Text(s)) = &m.content {
                                Some(s.as_str())
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>()
                        .join("\n");
                    let state_for_judge = state_for_done.clone();
                    let request_id_for_judge = request_id_for_done.clone();
                    let db_for_judge = state_for_done.db.clone();
                    tokio::spawn(async move {
                        use crate::gateway::evaluator::{persist_judge_scores, run_llm_judge};
                        match run_llm_judge(
                            &state_for_judge,
                            billing_pid_for_done,
                            &user_query,
                            &response_content,
                        )
                        .await
                        {
                            Some(scores) => {
                                tracing::info!(
                                    request_id = %request_id_for_judge,
                                    project_id = %billing_pid_for_done,
                                    score = %scores.average,
                                    "LLM-as-judge prompt quality score (stream)"
                                );
                                persist_judge_scores(
                                    &db_for_judge,
                                    billing_pid_for_done,
                                    &request_id_for_judge,
                                    &scores,
                                )
                                .await;
                            }
                            None => {
                                tracing::warn!(
                                    request_id = %request_id_for_judge,
                                    project_id = %billing_pid_for_done,
                                    "LLM-as-judge evaluation failed (stream), skipping"
                                );
                            }
                        }
                    });
                }
            }
        }

        Ok::<_, Infallible>(Event::default().data("[DONE]"))
    });

    // Combine streams
    let combined_stream = sse_stream.chain(done_stream);

    let mut response = Sse::new(combined_stream)
        .keep_alive(axum::response::sse::KeepAlive::default())
        .into_response();

    // Add request ID and provider headers to SSE response
    response.headers_mut().insert(
        HeaderName::from_static("x-request-id"),
        super::header_value(&ctx.request_id, "unknown"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-reiver-provider"),
        super::header_value(ctx.provider_name, "unknown"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-reiver-model-used"),
        super::header_value(&ctx.model_used, "unknown"),
    );

    // Add fallback headers if applicable
    if ctx.fallback_used {
        response.headers_mut().insert(
            HeaderName::from_static("x-reiver-fallback-used"),
            HeaderValue::from_static("true"),
        );
    }
    if ctx.retry_count > 0 {
        response.headers_mut().insert(
            HeaderName::from_static("x-reiver-retry-count"),
            super::header_value(&ctx.retry_count.to_string(), "0"),
        );
    }

    Ok(response)
}

// ---------------------------------------------------------------------------
// Per-project OTel publisher helper for streaming requests
// ---------------------------------------------------------------------------

/// Emit per-project OTel data for a completed streaming LLM request.
fn emit_project_streaming_otel(
    state: &FlowState,
    project_id: Uuid,
    provider: &str,
    model: &str,
    duration: Duration,
    usage: Option<&Usage>,
    error: Option<&str>,
    fallback_used: bool,
    retry_count: u32,
    ttfb_ms: u32,
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

    // Token usage metrics
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

    // Error metric
    if error.is_some() {
        let mut err_labels = labels.clone();
        err_labels.insert("error.type".into(), "stream_error".into());
        state
            .otel_publisher
            .emit_counter(project_id, "gen_ai.client.error", 1.0, err_labels);
    }

    // Build span
    let now = chrono::Utc::now();
    let start_time = now - chrono::Duration::from_std(duration).unwrap_or_default();
    let mut span_attrs = std::collections::HashMap::new();
    span_attrs.insert("gen_ai.provider.name".into(), provider.to_string());
    span_attrs.insert("gen_ai.request.model".into(), model.to_string());
    span_attrs.insert("gen_ai.operation.name".into(), "chat".into());
    span_attrs.insert("request_id".into(), request_id.to_string());
    span_attrs.insert("stream".into(), "true".into());

    if ttfb_ms > 0 {
        span_attrs.insert("time_to_first_token_ms".into(), ttfb_ms.to_string());
    }
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

    let (status_code, status_message) = if let Some(err) = error {
        span_attrs.insert("error.message".into(), err.to_string());
        ("STATUS_CODE_ERROR".to_string(), Some(err.to_string()))
    } else {
        ("STATUS_CODE_OK".to_string(), None)
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
