//! Internal gateway HTTP client for MooDeng agent loops.
//!
//! Instead of calling LLM providers directly, the agent routes requests
//! through the gateway endpoint so it gets prompt hub resolution,
//! observability logging, caching, guardrails, and model routing for free.
//!
//! Includes structured error classification and automatic retry with
//! exponential backoff for transient failures (429, 529, 5xx).

use futures::stream::{self, Stream, StreamExt};
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use crate::gateway::error::GatewayError;
use crate::gateway::types::{
    ChatCompletionChunk, ChatCompletionRequest, ChatCompletionResponse, ToolCall, Usage,
};

/// Inject the current tracing span's OTel context as a `traceparent` header
/// on a reqwest request builder, so downstream axum handlers can join the
/// same trace.
fn inject_trace_context(builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    let cx = tracing::Span::current().context();
    let propagator = TraceContextPropagator::new();
    let mut headers = std::collections::HashMap::<String, String>::new();
    propagator.inject_context(&cx, &mut headers);
    let mut b = builder;
    for (k, v) in headers {
        if let Ok(hv) = reqwest::header::HeaderValue::from_str(&v) {
            b = b.header(k, hv);
        }
    }
    b
}

// ── Error classification ─────────────────────────────────────────────────

/// Structured error from a gateway call, allowing callers to react
/// differently to rate limits vs context overflow vs fatal errors.
#[derive(Debug, thiserror::Error)]
pub enum GatewayCallError {
    #[error("rate limited (retry after {retry_after_ms:?}ms)")]
    RateLimited { retry_after_ms: Option<u64> },

    #[error("context too long for model")]
    ContextTooLong,

    #[error("provider overloaded (retry after {retry_after_ms:?}ms)")]
    Overloaded { retry_after_ms: Option<u64> },

    #[error("transient gateway error ({status}): {body}")]
    Transient { status: u16, body: String },

    #[error("fatal gateway error ({status}): {body}")]
    Fatal { status: u16, body: String },

    /// The Reiver platform requires a payment method or credits.
    /// Distinct from a provider returning 402 for *its own* billing.
    #[error("payment required: {message}")]
    PaymentRequired { message: String },

    /// An upstream provider returned 402 (e.g. DeepSeek "Insufficient Balance").
    #[error("provider billing error: {message}")]
    ProviderBillingError { message: String },

    #[error("network error: {0}")]
    Network(#[from] reqwest::Error),
}

fn error_source_chain(err: &(dyn std::error::Error + 'static)) -> String {
    let mut chain = vec![err.to_string()];
    let mut source = err.source();
    while let Some(e) = source {
        chain.push(e.to_string());
        source = e.source();
    }
    chain.join(" -> ")
}

impl GatewayCallError {
    pub fn is_context_too_long(&self) -> bool {
        matches!(self, Self::ContextTooLong)
    }
}

fn classify_error(
    status: reqwest::StatusCode,
    body: &str,
    headers: &reqwest::header::HeaderMap,
) -> GatewayCallError {
    let retry_after_ms = headers
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<f64>().ok())
        .map(|secs| (secs * 1000.0) as u64);

    let code = status.as_u16();
    match code {
        429 => GatewayCallError::RateLimited { retry_after_ms },
        413 => GatewayCallError::ContextTooLong,
        // Some providers return 400 with "context_length_exceeded" in the body
        400 if body.contains("context_length_exceeded")
            || body.contains("maximum context length")
            || body.contains("too many tokens") =>
        {
            GatewayCallError::ContextTooLong
        }
        402 => classify_402(body),
        529 => GatewayCallError::Overloaded { retry_after_ms },
        500..=599 => GatewayCallError::Transient {
            status: code,
            body: body.to_string(),
        },
        _ => GatewayCallError::Fatal {
            status: code,
            body: body.to_string(),
        },
    }
}

/// Distinguish Reiver billing errors from upstream provider billing errors.
///
/// The gateway uses `error_type` in the JSON body:
///   - `"payment_required"` / `"insufficient_credits"` → Reiver billing
///   - `"api_error"` (or anything else) → the upstream provider ran out of balance
fn classify_402(body: &str) -> GatewayCallError {
    let error_type = serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v.get("error")?.get("type")?.as_str().map(String::from));

    let message = serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v.get("error")?.get("message")?.as_str().map(String::from))
        .unwrap_or_else(|| body.to_string());

    match error_type.as_deref() {
        Some("payment_required") | Some("insufficient_credits") => {
            GatewayCallError::PaymentRequired { message }
        }
        _ => GatewayCallError::ProviderBillingError { message },
    }
}

// ── Retry configuration ──────────────────────────────────────────────────

struct RetryPolicy {
    max_retries: u32,
    base_delay_ms: u64,
    max_delay_ms: u64,
}

const RATE_LIMIT_POLICY: RetryPolicy = RetryPolicy {
    max_retries: 3,
    base_delay_ms: 2_000,
    max_delay_ms: 16_000,
};
const OVERLOADED_POLICY: RetryPolicy = RetryPolicy {
    max_retries: 2,
    base_delay_ms: 5_000,
    max_delay_ms: 30_000,
};
const TRANSIENT_POLICY: RetryPolicy = RetryPolicy {
    max_retries: 3,
    base_delay_ms: 500,
    max_delay_ms: 2_000,
};

fn retry_policy_for(err: &GatewayCallError) -> Option<&'static RetryPolicy> {
    match err {
        GatewayCallError::RateLimited { .. } => Some(&RATE_LIMIT_POLICY),
        GatewayCallError::Overloaded { .. } => Some(&OVERLOADED_POLICY),
        GatewayCallError::Transient { .. } => Some(&TRANSIENT_POLICY),
        _ => None,
    }
}

fn backoff_ms(policy: &RetryPolicy, attempt: u32, server_hint_ms: Option<u64>) -> u64 {
    if let Some(hint) = server_hint_ms {
        return hint.min(policy.max_delay_ms);
    }
    let delay = policy.base_delay_ms * 2u64.pow(attempt);
    delay.min(policy.max_delay_ms)
}

// ── Public API ───────────────────────────────────────────────────────────

/// Aggregated result from a non-streaming gateway call.
pub struct GatewayCallResult {
    pub content: String,
    pub tool_calls: Vec<ToolCall>,
    pub usage: Usage,
    pub finish_reason: String,
    pub model: String,
    /// Reasoning/thinking content from the model (DeepSeek R1, Anthropic extended thinking, etc.).
    pub thinking: Option<String>,
}

/// Send a gateway request with retry logic. Returns the raw `reqwest::Response`
/// on success; callers parse it as JSON or SSE depending on streaming mode.
async fn send_with_retry(
    client: &reqwest::Client,
    url: &str,
    request: &ChatCompletionRequest,
    project_id: Uuid,
    session_id: Option<&str>,
    billing_project_id: Option<Uuid>,
    label: &str,
) -> Result<reqwest::Response, GatewayCallError> {
    let mut last_err: Option<GatewayCallError> = None;

    for attempt in 0..=RATE_LIMIT_POLICY.max_retries {
        let mut builder = client
            .post(url)
            .header("X-Project-Id", project_id.to_string());
        if let Some(bid) = billing_project_id {
            builder = builder.header("X-Billing-Project-Id", bid.to_string());
        }
        if let Some(sid) = session_id {
            builder = builder
                .header("x-reiver-session-id", sid)
                .header("x-reiver-session-name", "moodeng");
        }
        let resp = match inject_trace_context(builder.json(request)).send().await {
            Ok(r) => r,
            Err(e) => {
                if attempt < TRANSIENT_POLICY.max_retries {
                    tracing::warn!(attempt, error = %error_source_chain(&e), "{label} network error, retrying");
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms(
                        &TRANSIENT_POLICY,
                        attempt,
                        None,
                    )))
                    .await;
                    last_err = Some(GatewayCallError::Network(e));
                    continue;
                }
                return Err(GatewayCallError::Network(e));
            }
        };

        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }

        let headers = resp.headers().clone();
        let body = resp.text().await.unwrap_or_default();
        let err = classify_error(status, &body, &headers);

        if err.is_context_too_long() {
            return Err(err);
        }

        let policy = match retry_policy_for(&err) {
            Some(p) if attempt < p.max_retries => p,
            _ => return Err(err),
        };

        let hint = match &err {
            GatewayCallError::RateLimited { retry_after_ms }
            | GatewayCallError::Overloaded { retry_after_ms } => *retry_after_ms,
            _ => None,
        };
        let delay = backoff_ms(policy, attempt, hint);

        tracing::warn!(attempt, delay_ms = delay, error = %err, "{label} call failed, retrying");
        tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
        last_err = Some(err);
    }

    Err(last_err.unwrap_or_else(|| GatewayCallError::Fatal {
        status: 0,
        body: "retry loop exhausted without error".into(),
    }))
}

/// Non-streaming gateway call with automatic retry for transient failures.
pub async fn call_gateway(
    client: &reqwest::Client,
    gateway_url: &str,
    project_id: Uuid,
    request: &ChatCompletionRequest,
    session_id: Option<&str>,
    billing_project_id: Option<Uuid>,
) -> Result<GatewayCallResult, GatewayCallError> {
    let mut req = request.clone();
    req.stream = Some(false);
    let url = format!("{gateway_url}/api/gateway/v1/chat/completions");

    let resp = send_with_retry(
        client, &url, &req, project_id, session_id, billing_project_id, "Gateway",
    )
    .await?;

    let response: ChatCompletionResponse =
        resp.json().await.map_err(|e| GatewayCallError::Fatal {
            status: 0,
            body: format!("Failed to parse gateway response: {e}"),
        })?;

    let choice = response.choices.into_iter().next();
    let (content, tool_calls, finish_reason, thinking) = match choice {
        Some(c) => (
            c.message.content.unwrap_or_default(),
            c.message.tool_calls.unwrap_or_default(),
            c.finish_reason.as_str().to_string(),
            c.message.thinking.map(|t| t.content),
        ),
        None => (String::new(), Vec::new(), "stop".to_string(), None),
    };

    Ok(GatewayCallResult {
        content,
        tool_calls,
        usage: response.usage,
        finish_reason,
        model: response.model,
        thinking,
    })
}

pub use crate::gateway::providers::ChatCompletionStream;

/// Result of a streaming gateway call, including metadata from response headers.
pub struct GatewayStreamResult {
    pub stream: ChatCompletionStream,
    pub provider: String,
}

/// Streaming gateway call with automatic retry for transient failures.
pub async fn call_gateway_stream(
    client: &reqwest::Client,
    gateway_url: &str,
    project_id: Uuid,
    request: &ChatCompletionRequest,
    session_id: Option<&str>,
    billing_project_id: Option<Uuid>,
) -> Result<GatewayStreamResult, GatewayCallError> {
    let mut req = request.clone();
    req.stream = Some(true);
    let url = format!("{gateway_url}/api/gateway/v1/chat/completions");

    let resp = send_with_retry(
        client, &url, &req, project_id, session_id, billing_project_id, "Gateway stream",
    )
    .await?;

    let provider = resp
        .headers()
        .get("x-reiver-provider")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    let chunk_stream = sse_to_chunks(resp.bytes_stream());
    Ok(GatewayStreamResult {
        stream: Box::pin(chunk_stream),
        provider,
    })
}

/// Convert a raw byte stream (SSE) into a stream of `ChatCompletionChunk`.
fn sse_to_chunks(
    byte_stream: impl Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send + 'static,
) -> impl Stream<Item = Result<ChatCompletionChunk, GatewayError>> + Send {
    let line_stream = bytes_to_lines(byte_stream);

    line_stream.filter_map(|line_result| async move {
        match line_result {
            Err(e) => Some(Err(GatewayError::InternalError(format!(
                "SSE stream error: {e}"
            )))),
            Ok(line) => {
                let data = line.strip_prefix("data: ")?;
                if data == "[DONE]" {
                    return None;
                }
                match serde_json::from_str::<ChatCompletionChunk>(data) {
                    Ok(chunk) => Some(Ok(chunk)),
                    Err(e) => {
                        tracing::warn!(error = %e, raw = %data, "Failed to parse SSE chunk");
                        None
                    }
                }
            }
        }
    })
}

/// Split a byte stream into lines, handling partial reads across chunk boundaries.
fn bytes_to_lines(
    byte_stream: impl Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send + 'static,
) -> impl Stream<Item = Result<String, reqwest::Error>> + Send {
    stream::unfold(
        (Box::pin(byte_stream), String::new()),
        |(mut stream, mut buf)| async move {
            loop {
                if let Some(pos) = buf.find('\n') {
                    let line = buf[..pos].trim_end_matches('\r').to_string();
                    buf = buf[pos + 1..].to_string();
                    if line.is_empty() {
                        continue;
                    }
                    return Some((Ok(line), (stream, buf)));
                }
                match stream.next().await {
                    Some(Ok(bytes)) => {
                        buf.push_str(&String::from_utf8_lossy(&bytes));
                    }
                    Some(Err(e)) => return Some((Err(e), (stream, buf))),
                    None => {
                        if !buf.trim().is_empty() {
                            let line = std::mem::take(&mut buf);
                            return Some((Ok(line.trim().to_string()), (stream, buf)));
                        }
                        return None;
                    }
                }
            }
        },
    )
}
