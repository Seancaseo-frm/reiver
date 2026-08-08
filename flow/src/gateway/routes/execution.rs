//! Provider call orchestration, retry logic, and fallback handling.

use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

use axum::response::Response;

use crate::app_state::FlowState;
use crate::gateway::error::GatewayError;
use crate::gateway::fallback::{
    calculate_retry_delay, is_retryable_error, should_fallback, FallbackConfig, FallbackResult,
};
use crate::gateway::prompt_resolver::PromptResolution;
use crate::gateway::provider_types::Provider;
use crate::gateway::router::GatewayRouter;
use crate::gateway::types::{ChatCompletionRequest, ChatCompletionResponse};
use reiver_core::events::PlatformEventType;
use tracing::Instrument;

use super::resolve::ProviderCandidate;
use super::response::{handle_streaming_response, StreamingResponseContext};

// ========================================================================
// Retry helpers
// ========================================================================

/// Outcome of attempting a request with retries against a single provider.
pub(super) enum RetryOutcome<T> {
    /// Request succeeded.
    Success { result: T, retries: u32 },
    /// All retries exhausted or a non-retryable error was encountered.
    Failed { error: GatewayError, retries: u32 },
}

/// Generic retry loop: calls `operation` up to `config.max_retries + 1` times,
/// backing off between retryable failures.
pub(super) async fn retry_with_backoff<T, F, Fut>(
    config: &FallbackConfig,
    mut operation: F,
) -> RetryOutcome<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, GatewayError>>,
{
    let mut retries = 0u32;
    let mut last_error: Option<GatewayError> = None;

    for attempt in 0..=config.max_retries {
        if attempt > 0 {
            retries += 1;
        }

        let attempt_span = tracing::debug_span!(
            "gateway.provider.attempt",
            attempt = attempt,
            max_retries = config.max_retries,
        );

        let err = match operation().instrument(attempt_span).await {
            Ok(result) => return RetryOutcome::Success { result, retries },
            Err(e) => e,
        };

        if !is_retryable_error(&err) {
            return RetryOutcome::Failed {
                error: err,
                retries,
            };
        }

        last_error = Some(err);
        if attempt >= config.max_retries {
            continue;
        }
        let delay = calculate_retry_delay(attempt, config);
        tokio::time::sleep(delay).await;
    }

    RetryOutcome::Failed {
        error: last_error
            .unwrap_or_else(|| GatewayError::InternalError("Retry loop exhausted".to_string())),
        retries,
    }
}

// ========================================================================
// Non-streaming execution
// ========================================================================

/// Execute a chat completion request by iterating the provider chain.
///
/// Tries each candidate in order, skipping circuit-broken providers and
/// falling back on `should_fallback`-eligible errors.
pub(super) async fn execute_with_fallback(
    state: &Arc<FlowState>,
    router: &GatewayRouter,
    request: &ChatCompletionRequest,
    chain: &[ProviderCandidate],
    config: &FallbackConfig,
    project_id: Uuid,
) -> Result<FallbackResult<ChatCompletionResponse>, GatewayError> {
    let primary_provider = chain.first().map(|c| c.provider).unwrap_or(Provider::OpenAi);
    let chain_desc: Vec<String> = chain
        .iter()
        .map(|c| format!("{}:{}", c.provider, c.model))
        .collect();
    let span = tracing::info_span!(
        "gateway.execute",
        provider = %primary_provider,
        model = %request.model,
        chain_length = chain.len(),
        provider_chain = %chain_desc.join(", "),
        fallback_used = tracing::field::Empty,
        fallback_provider = tracing::field::Empty,
        fallback_model = tracing::field::Empty,
        total_retries = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
        otel.status_message = tracing::field::Empty,
    );
    let result = execute_chain(state, router, request, chain, config, project_id, span.clone())
        .instrument(span.clone())
        .await;

    if let Err(ref e) = result {
        span.record("otel.status_code", "ERROR");
        span.record("otel.status_message", &e.to_string());
    }

    result
}

async fn execute_chain(
    state: &Arc<FlowState>,
    router: &GatewayRouter,
    request: &ChatCompletionRequest,
    chain: &[ProviderCandidate],
    config: &FallbackConfig,
    project_id: Uuid,
    exec_span: tracing::Span,
) -> Result<FallbackResult<ChatCompletionResponse>, GatewayError> {
    let mut last_error = GatewayError::InternalError("No provider candidates".to_string());
    let mut total_retries = 0u32;

    for (i, candidate) in chain.iter().enumerate() {
        // Skip circuit-broken or degraded providers.
        let circuit_open = router.circuit_breaker().is_open(&candidate.provider);
        let degraded = router.latency_tracker().is_degraded(&candidate.provider);
        if circuit_open || degraded {
            let reason = if circuit_open {
                format!("{} circuit breaker is open", candidate.provider)
            } else {
                format!("{} is degraded (high P99 latency)", candidate.provider)
            };
            tracing::info!(
                provider = %candidate.provider,
                model = %candidate.model,
                %reason,
                "Skipping provider in chain"
            );
            last_error = GatewayError::Timeout(reason);
            continue;
        }

        let mut call_request = request.clone();
        call_request.model = candidate.model.clone();

        tracing::info!(
            provider = %candidate.provider,
            model = %candidate.model,
            chain_index = i,
            "Attempting provider"
        );

        let outcome = retry_with_backoff(config, || {
            router.chat_completion_with_provider(
                &call_request,
                &candidate.key,
                candidate.provider_impl.as_ref(),
            )
        })
        .await;

        match outcome {
            RetryOutcome::Success { result, retries } => {
                total_retries += retries;
                let fallback_used = i > 0;
                if fallback_used {
                    tracing::info!(
                        original_model = %request.model,
                        fallback_model = %candidate.model,
                        provider = %candidate.provider,
                        retries = %total_retries,
                        "Successfully used fallback model"
                    );
                    exec_span.record("fallback_provider", candidate.provider.as_str());
                    exec_span.record("fallback_model", candidate.model.as_str());
                }
                exec_span.record("fallback_used", fallback_used);
                exec_span.record("total_retries", total_retries);
                return if fallback_used {
                    Ok(FallbackResult::fallback(
                        result, candidate.model.clone(), candidate.provider, total_retries,
                    ))
                } else {
                    Ok(FallbackResult::primary(
                        result, candidate.model.clone(), candidate.provider, total_retries,
                    ))
                };
            }
            RetryOutcome::Failed { error, retries } => {
                total_retries += retries;
                tracing::warn!(
                    provider = %candidate.provider,
                    model = %candidate.model,
                    chain_index = i,
                    error = %error,
                    retries = retries,
                    "Provider attempt failed"
                );
                emit_provider_key_error_event(state, project_id, candidate, &error);
                if !should_fallback(&error) {
                    return Err(error);
                }
                last_error = error;
            }
        }
    }

    Err(last_error)
}

// ========================================================================
// Streaming execution
// ========================================================================

/// Context for the streaming execution path. Carries metadata needed by
/// `handle_streaming_response` that is orthogonal to provider resolution.
pub(super) struct StreamingContext<'a> {
    pub(super) gateway_router: Arc<GatewayRouter>,
    pub(super) request: ChatCompletionRequest,
    pub(super) project_id: Uuid,
    pub(super) billing_project_id: Uuid,
    pub(super) start: Instant,
    pub(super) request_id: String,
    pub(super) prompt_resolution: Option<PromptResolution>,
    pub(super) fallback_config: &'a FallbackConfig,
    pub(super) session_id: String,
    pub(super) session_name: String,
    pub(super) session_budget_usd: Option<f64>,
    pub(super) guardrail_config: crate::gateway::guardrails::GuardrailConfig,
    pub(super) judge_sample_rate: Option<f64>,
    pub(super) log_content: bool,
    pub(super) org_id: Option<Uuid>,
}

/// Handle streaming chat completion by iterating the provider chain.
///
/// Fallback can only happen in the connection phase — once a stream is
/// established we cannot switch providers mid-stream.
pub(super) async fn handle_streaming_with_chain(
    state: Arc<FlowState>,
    ctx: StreamingContext<'_>,
    chain: &[ProviderCandidate],
) -> Result<Response, GatewayError> {
    let primary_provider = chain.first().map(|c| c.provider).unwrap_or(Provider::OpenAi);
    let chain_desc: Vec<String> = chain
        .iter()
        .map(|c| format!("{}:{}", c.provider, c.model))
        .collect();
    let span = tracing::info_span!(
        "gateway.stream.execute",
        provider = %primary_provider,
        model = %ctx.request.model,
        chain_length = chain.len(),
        provider_chain = %chain_desc.join(", "),
        fallback_used = tracing::field::Empty,
        fallback_provider = tracing::field::Empty,
        fallback_model = tracing::field::Empty,
    );
    async {
        let mut last_error = GatewayError::InternalError("No provider candidates".to_string());
        let mut total_retries = 0u32;

        for (i, candidate) in chain.iter().enumerate() {
            let circuit_open = ctx.gateway_router.circuit_breaker().is_open(&candidate.provider);
            let degraded = ctx.gateway_router.latency_tracker().is_degraded(&candidate.provider);
            if circuit_open || degraded {
                let reason = if circuit_open {
                    format!("{} circuit breaker is open", candidate.provider)
                } else {
                    format!("{} is degraded (high P99 latency)", candidate.provider)
                };
                tracing::info!(
                    provider = %candidate.provider,
                    model = %candidate.model,
                    %reason,
                    "Skipping streaming provider in chain"
                );
                last_error = GatewayError::Timeout(reason);
                continue;
            }

            let mut call_request = ctx.request.clone();
            call_request.model = candidate.model.clone();

            tracing::info!(
                provider = %candidate.provider,
                model = %candidate.model,
                chain_index = i,
                "Attempting streaming provider"
            );

            let outcome = retry_with_backoff(ctx.fallback_config, || {
                ctx.gateway_router.stream_chat_completion_with_provider(
                    &call_request,
                    &candidate.key,
                    candidate.provider_impl.as_ref(),
                )
            })
            .await;

            match outcome {
                RetryOutcome::Success { result: stream, retries } => {
                    total_retries += retries;
                    let fallback_used = i > 0;
                    ctx.gateway_router.circuit_breaker().record_success(&candidate.provider);
                    if fallback_used {
                        tracing::info!(
                            original_model = %ctx.request.model,
                            fallback_model = %candidate.model,
                            provider = %candidate.provider,
                            retries = %total_retries,
                            "Streaming request using fallback model"
                        );
                    }
                    return handle_streaming_response(
                        state,
                        StreamingResponseContext {
                            chunk_stream: stream,
                            request: call_request,
                            project_id: ctx.project_id,
                            billing_project_id: ctx.billing_project_id,
                            provider_name: candidate.provider.as_str(),
                            start: ctx.start,
                            request_id: ctx.request_id,
                            prompt_resolution: ctx.prompt_resolution,
                            fallback_used,
                            model_used: candidate.model.clone(),
                            retry_count: total_retries,
                            session_id: ctx.session_id,
                            session_name: ctx.session_name,
                            session_budget_usd: ctx.session_budget_usd,
                            guardrail_config: ctx.guardrail_config,
                            judge_sample_rate: ctx.judge_sample_rate,
                            log_content: ctx.log_content,
                            is_platform_key: candidate.is_platform_key,
                            org_id: ctx.org_id,
                        },
                    )
                    .await;
                }
                RetryOutcome::Failed { error, retries } => {
                    total_retries += retries;
                    ctx.gateway_router.circuit_breaker().record_failure(&candidate.provider);
                    tracing::warn!(
                        provider = %candidate.provider,
                        model = %candidate.model,
                        chain_index = i,
                        error = %error,
                        retries = retries,
                        "Streaming provider attempt failed"
                    );
                    emit_provider_key_error_event(&state, ctx.project_id, candidate, &error);
                    if !should_fallback(&error) {
                        return Err(error);
                    }
                    last_error = error;
                }
            }
        }

        Err(last_error)
    }
    .instrument(span)
    .await
}

/// Fire-and-forget platform event when a BYOK key produces a 401/402/403/404.
/// Spawns a background task so the fallback chain is never blocked.
fn emit_provider_key_error_event(
    state: &Arc<FlowState>,
    project_id: Uuid,
    candidate: &ProviderCandidate,
    error: &GatewayError,
) {
    let (status, message) = match error {
        GatewayError::ProviderError { status, message, .. }
            if !candidate.is_platform_key
                && (*status == 401 || *status == 402 || *status == 403 || *status == 404) =>
        {
            (*status, message.clone())
        }
        _ => return,
    };

    let publisher = state.event_publisher.clone();
    let provider = candidate.provider.as_str().to_string();
    let model = candidate.model.clone();

    tokio::spawn(async move {
        let _ = publisher
            .emit(
                PlatformEventType::ProviderKeyError,
                project_id,
                format!("provider_key_error:{}:{}", provider, status),
                serde_json::json!({
                    "provider": provider,
                    "model": model,
                    "status": status,
                    "message": message,
                }),
            )
            .await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::fallback::FallbackConfig;

    /// Regression: `retry_with_backoff` must report the correct retry count
    /// when the operation succeeds on a retry attempt.  Previously the counter
    /// was incremented after the operation call, so a success on attempt 1
    /// returned `retries: 0` instead of `retries: 1`.
    #[tokio::test]
    async fn test_retry_with_backoff_counts_retries_on_success() {
        use std::sync::atomic::{AtomicU32, Ordering};

        let call_count = AtomicU32::new(0);
        let config = FallbackConfig::default().with_max_retries(3);

        let outcome: RetryOutcome<&str> = retry_with_backoff(&config, || {
            let n = call_count.fetch_add(1, Ordering::SeqCst);
            async move {
                if n == 0 {
                    Err(GatewayError::Timeout("simulated".into()))
                } else {
                    Ok("ok")
                }
            }
        })
        .await;

        match outcome {
            RetryOutcome::Success { result, retries } => {
                assert_eq!(result, "ok");
                assert_eq!(
                    retries, 1,
                    "Operation succeeded on attempt 1 (the first retry), so retries must be 1"
                );
            }
            RetryOutcome::Failed { .. } => panic!("Expected success after one retry"),
        }
    }

    /// retry_with_backoff must report retries = 0 when the first attempt succeeds.
    #[tokio::test]
    async fn test_retry_with_backoff_zero_retries_on_immediate_success() {
        let config = FallbackConfig::default();

        let outcome: RetryOutcome<&str> = retry_with_backoff(&config, || async { Ok("ok") }).await;

        match outcome {
            RetryOutcome::Success { retries, .. } => {
                assert_eq!(retries, 0, "Immediate success should report 0 retries");
            }
            RetryOutcome::Failed { .. } => panic!("Expected immediate success"),
        }
    }

    /// retry_with_backoff must report the correct count when all retries are exhausted.
    #[tokio::test]
    async fn test_retry_with_backoff_counts_retries_on_exhaustion() {
        let config = FallbackConfig::default().with_max_retries(2);

        let outcome: RetryOutcome<&str> = retry_with_backoff(&config, || async {
            Err(GatewayError::Timeout("always fails".into()))
        })
        .await;

        match outcome {
            RetryOutcome::Failed { retries, .. } => {
                assert_eq!(
                    retries, 2,
                    "With max_retries=2, all attempts exhausted should report retries=2"
                );
            }
            RetryOutcome::Success { .. } => panic!("Expected failure"),
        }
    }
}
