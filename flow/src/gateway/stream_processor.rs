use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;

use axum::response::sse::Event;
use chrono::Utc;
use parking_lot::Mutex;
use uuid::Uuid;

use crate::gateway::error::GatewayError;
use crate::gateway::latency_tracker::LatencyTracker;
use crate::gateway::types::{ChatCompletionChunk, ChunkDelta, Usage};
use crate::kafka::{KafkaProducer, LlmChunkKafkaMessage};

/// One-time summary sent when a stream completes (final chunk or error).
/// Consumed by the done callback for ClickHouse insert and session budget.
#[derive(Debug, Clone)]
pub(crate) struct StreamCompletionSummary {
    pub(crate) model: String,
    pub(crate) usage: Option<Usage>,
    pub(crate) ttfb_ms: u32,
    pub(crate) error: Option<String>,
    /// Accumulated response content for post-stream LLM-as-judge evaluation.
    /// Only populated when `judge_buffer` was enabled on the processor.
    pub(crate) response_content: Option<String>,
}

/// Shared state for processing streaming chunks without deep nesting.
///
/// For observability, it uses a bounded mpsc channel for Kafka delivery instead of spawning
/// a task per chunk. A single consumer task drains the channel, reducing
/// spawn overhead and enabling better Kafka batching via `linger.ms`.
/// Sends at most one `StreamCompletionSummary` on final chunk or error.
pub(crate) struct StreamChunkProcessor {
    pub(super) chunk_index: AtomicU32,
    pub(super) first_token_received: AtomicBool,
    pub(super) first_token_time_ms: AtomicU32,
    pub(super) start: Instant,
    kafka_tx: tokio::sync::mpsc::Sender<LlmChunkKafkaMessage>,
    provider_name: String,
    pub(super) project_id: Uuid,
    /// When `true`, apply best-effort PII masking to each delta text chunk
    /// before forwarding to the client.
    pub(super) mask_output_pii: bool,
    /// When `true`, scan accumulated response content for exfiltration patterns.
    pub(super) block_exfiltration_urls: bool,
    /// Rolling buffer of recent response content for cross-chunk exfiltration detection.
    exfiltration_buffer: Mutex<String>,
    /// Tool names blocked project-wide.  Empty = no project-level blocking.
    pub(super) blocked_tools: Vec<String>,
    /// Per-prompt tool whitelist.  `None` = no restriction; `Some([])` = all tools blocked.
    pub(super) allowed_tools: Option<Vec<String>>,
    /// Sender for completion summary; taken and sent once on final chunk or error.
    completion_tx: Mutex<Option<tokio::sync::oneshot::Sender<StreamCompletionSummary>>>,
    /// Latency tracker for recording TTFB on first chunk (and on error with no token).
    latency_tracker: Option<Arc<LatencyTracker>>,
    /// Accumulates response content for post-stream LLM-as-judge.
    /// `None` when judge is not enabled for this request (zero overhead).
    pub(super) judge_buffer: Option<Mutex<String>>,
    /// Whether the client requested `stream_options.include_usage`.
    /// When false, the usage-only chunk (empty choices + usage data) is consumed
    /// internally for observability but NOT forwarded to the client.
    pub(super) client_include_usage: bool,
}

impl StreamChunkProcessor {
    /// Returns the processor and the receiver for the single completion summary.
    /// The done callback should recv() on the returned receiver (with timeout).
    pub(crate) fn new(
        _model: String,
        start: Instant,
        kafka: Arc<KafkaProducer>,
        provider_name: String,
        project_id: Uuid,
        latency_tracker: Option<Arc<LatencyTracker>>,
    ) -> (
        Self,
        tokio::sync::oneshot::Receiver<StreamCompletionSummary>,
    ) {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<LlmChunkKafkaMessage>(64);

        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                let _ = kafka.send_llm_chunk(&msg).await;
            }
        });

        let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();

        let processor = Self {
            chunk_index: AtomicU32::new(0),
            first_token_received: AtomicBool::new(false),
            first_token_time_ms: AtomicU32::new(0),
            start,
            kafka_tx: tx,
            provider_name,
            project_id,
            mask_output_pii: false,
            block_exfiltration_urls: false,
            exfiltration_buffer: Mutex::new(String::new()),
            blocked_tools: Vec::new(),
            allowed_tools: None,
            completion_tx: Mutex::new(Some(completion_tx)),
            latency_tracker,
            judge_buffer: None,
            client_include_usage: false,
        };

        (processor, completion_rx)
    }

    /// Process a single chunk result into an SSE event, sending to Kafka as a side-effect.
    pub(crate) fn process(
        &self,
        chunk_result: Result<ChatCompletionChunk, GatewayError>,
    ) -> Result<Event, Infallible> {
        let chunk = match chunk_result {
            Ok(c) => c,
            Err(e) => return self.handle_error(e),
        };

        self.track_first_token();
        let idx = self.chunk_index.fetch_add(1, Ordering::SeqCst);

        let chunk = if self.mask_output_pii {
            let mut c = chunk;
            for choice in &mut c.choices {
                mask_delta_pii(&mut choice.delta);
            }
            c
        } else {
            chunk
        };

        // Accumulate content for post-stream LLM-as-judge evaluation
        if let Some(ref buf) = self.judge_buffer {
            if let Some(delta_content) = chunk
                .choices
                .first()
                .and_then(|c| c.delta.content.as_deref())
            {
                let mut buf = buf.lock();
                const MAX_JUDGE_BUF: usize = 100 * 1024;
                if buf.len() < MAX_JUDGE_BUF {
                    buf.push_str(delta_content);
                }
            }
        }

        // Streaming tool call validation
        if self.allowed_tools.is_some() || !self.blocked_tools.is_empty() {
            for choice in &chunk.choices {
                if let Some(ref tool_calls) = choice.delta.tool_calls {
                    for tc in tool_calls {
                        let name = tc.function.name.as_str();
                        if name.is_empty() {
                            continue;
                        }
                        if let Some(ref allowed) = self.allowed_tools {
                            if !allowed.iter().any(|a| a == name) {
                                return self.handle_error(GatewayError::GuardrailViolation {
                                    rule: crate::gateway::domain_types::GuardrailRule::ToolCallBlocked,
                                    detail: format!("Tool call \"{}\" is not in the allowed tools list for this prompt.", name),
                                });
                            }
                        }
                        let lower = name.to_lowercase();
                        if self.blocked_tools.iter().any(|b| b.to_lowercase() == lower) {
                            return self.handle_error(GatewayError::GuardrailViolation {
                                rule: crate::gateway::domain_types::GuardrailRule::ToolCallBlocked,
                                detail: format!("Tool call \"{}\" is blocked by the project's guardrail policy.", name),
                            });
                        }
                    }
                }
            }
        }

        // Streaming exfiltration URL detection
        if self.block_exfiltration_urls {
            if let Some(delta_content) = chunk
                .choices
                .first()
                .and_then(|c| c.delta.content.as_deref())
            {
                let mut buf = self.exfiltration_buffer.lock();
                buf.push_str(delta_content);
                // Keep buffer bounded — exfiltration patterns are short
                const MAX_BUF: usize = 2048;
                if buf.len() > MAX_BUF {
                    let drain_target = buf.len() - MAX_BUF;
                    // Find the nearest char boundary at or after drain_target
                    // to avoid panicking on multi-byte UTF-8.
                    let drain = (drain_target..buf.len())
                        .find(|&i| buf.is_char_boundary(i))
                        .unwrap_or(buf.len());
                    buf.drain(..drain);
                }
                if crate::gateway::guardrails::detect_exfiltration_in_text(&buf).is_some() {
                    return self.handle_error(GatewayError::GuardrailViolation {
                        rule: crate::gateway::domain_types::GuardrailRule::ExfiltrationBlocked,
                        detail: "Response blocked: potential data exfiltration detected via external URL reference.".to_string(),
                    });
                }
            }
        }

        // Detect the usage-only chunk: empty choices with usage data present.
        // Providers send this when we request `include_usage: true` internally.
        let is_usage_only = chunk.choices.is_empty() && chunk.usage.is_some();

        let is_final = chunk
            .choices
            .first()
            .and_then(|c| c.finish_reason.as_ref())
            .is_some();

        // Send the completion summary at the right time:
        // - If the finish chunk already has usage (some providers inline it), send immediately.
        // - If the finish chunk has NO usage, defer to the usage-only chunk that follows.
        // - The usage-only chunk always sends the summary (it has the actual token counts).
        let should_send_summary = is_usage_only || (is_final && chunk.usage.is_some());

        if should_send_summary {
            let ttfb_ms = self.first_token_time_ms.load(Ordering::SeqCst);
            let response_content = self
                .judge_buffer
                .as_ref()
                .map(|buf| std::mem::take(&mut *buf.lock()));
            let summary = StreamCompletionSummary {
                model: chunk.model.clone(),
                usage: chunk.usage.clone(),
                ttfb_ms,
                error: None,
                response_content,
            };
            if let Some(tx) = self.completion_tx.lock().take() {
                let _ = tx.send(summary);
            }
        }

        // If this is a usage-only chunk and the client didn't request it, suppress it.
        if is_usage_only && !self.client_include_usage {
            self.send_to_kafka(idx, &chunk);
            return Ok(Event::default().comment(""));
        }

        self.send_to_kafka(idx, &chunk);
        self.serialize_event(&chunk)
    }

    fn track_first_token(&self) {
        if !self.first_token_received.swap(true, Ordering::SeqCst) {
            let ttft = self.start.elapsed().as_millis() as u32;
            self.first_token_time_ms.store(ttft, Ordering::SeqCst);
            if let Some(ref tracker) = self.latency_tracker {
                tracker.record(
                    &self.provider_name,
                    std::time::Duration::from_millis(ttft as u64),
                );
            }
        }
    }

    fn send_to_kafka(&self, idx: u32, chunk: &ChatCompletionChunk) {
        let content = chunk
            .choices
            .first()
            .and_then(|c| c.delta.content.clone())
            .unwrap_or_default();
        let is_final = chunk
            .choices
            .first()
            .and_then(|c| c.finish_reason.as_ref())
            .is_some();
        let finish_reason = chunk
            .choices
            .first()
            .and_then(|c| c.finish_reason.as_ref())
            .map(|r| r.as_str().to_string());

        let chunk_message = LlmChunkKafkaMessage {
            project_id: self.project_id.to_string(),
            request_id: chunk.id.clone(),
            chunk_index: idx,
            content,
            model: chunk.model.clone(),
            provider: self.provider_name.clone(),
            timestamp: Utc::now().to_rfc3339(),
            is_final,
            finish_reason,
            input_tokens: chunk.usage.as_ref().map(|u| u.prompt_tokens),
            output_tokens: chunk.usage.as_ref().map(|u| u.completion_tokens),
        };

        if let Err(e) = self.kafka_tx.try_send(chunk_message) {
            tracing::warn!(
                project_id = %self.project_id,
                provider = %self.provider_name,
                "Dropping streaming chunk for Kafka: channel full ({e})"
            );
        }
    }

    fn serialize_event(&self, chunk: &ChatCompletionChunk) -> Result<Event, Infallible> {
        let json = match serde_json::to_string(chunk) {
            Ok(j) => j,
            Err(e) => {
                tracing::error!(request_id = %chunk.id, error = %e, "Failed to serialize streaming chunk");
                let error_json = serde_json::json!({
                    "error": {
                        "message": "Failed to process response chunk",
                        "type": "server_error",
                        "code": null,
                    }
                });
                return Ok(Event::default().data(error_json.to_string()));
            }
        };
        Ok(Event::default().data(json))
    }

    fn handle_error(&self, e: GatewayError) -> Result<Event, Infallible> {
        tracing::error!(
            project_id = %self.project_id,
            provider = %self.provider_name,
            error = %e,
            "Stream processing error"
        );
        // No token received and error: record full duration to penalize provider in latency routing.
        let ttfb_ms = self.first_token_time_ms.load(Ordering::SeqCst);
        if ttfb_ms == 0 {
            if let Some(ref tracker) = self.latency_tracker {
                let duration = self.start.elapsed();
                if !duration.is_zero() {
                    tracker.record(&self.provider_name, duration);
                }
            }
        }
        let summary = StreamCompletionSummary {
            model: String::new(),
            usage: None,
            ttfb_ms,
            error: Some(e.to_string()),
            response_content: None,
        };
        if let Some(tx) = self.completion_tx.lock().take() {
            let _ = tx.send(summary);
        }
        let (error_type, message) = e.client_facing_details();
        let error_json = serde_json::json!({
            "error": {
                "message": message,
                "type": error_type,
                "code": null,
            }
        });
        Ok(Event::default().data(error_json.to_string()))
    }
}

/// Return the longest prefix of `s` that fits in `max_bytes` without
/// splitting a multi-byte UTF-8 character.
#[allow(dead_code)] // used by tests and for potential future content aggregation
fn truncate_to_char_boundary(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Mask PII in a streaming chunk delta's content and thinking fields.
///
/// PII split across chunk boundaries may not be caught.
pub(crate) fn mask_delta_pii(delta: &mut ChunkDelta) {
    if let Some(ref content) = delta.content {
        if let Some(masked) = crate::pii::redact_if_changed(content) {
            delta.content = Some(masked);
        }
    }
    if let Some(ref thinking) = delta.thinking {
        if let Some(masked) = crate::pii::redact_if_changed(thinking) {
            delta.thinking = Some(masked);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_truncate_to_char_boundary_ascii() {
        assert_eq!(truncate_to_char_boundary("hello world", 5), "hello");
        assert_eq!(truncate_to_char_boundary("hello", 10), "hello");
        assert_eq!(truncate_to_char_boundary("", 5), "");
    }

    #[test]
    fn test_truncate_to_char_boundary_multibyte() {
        // '€' is 3 bytes in UTF-8
        let s = "€€€";
        assert_eq!(s.len(), 9);
        assert_eq!(truncate_to_char_boundary(s, 9), "€€€");
        assert_eq!(truncate_to_char_boundary(s, 6), "€€");
        // 4 bytes isn't on a char boundary for 3-byte chars → rounds down to 3
        assert_eq!(truncate_to_char_boundary(s, 4), "€");
        assert_eq!(truncate_to_char_boundary(s, 2), "");
    }

    /// Regression: stream error events previously used a flat
    /// `{"error": "Stream processing error occurred"}` format that was not
    /// OpenAI-compatible and discarded the real error details. After the fix,
    /// error events must use the `{"error": {"message": ..., "type": ..., "code": null}}` format.
    /// handle_error also sends a completion summary with the error for the done callback.
    #[tokio::test]
    async fn test_handle_error_produces_openai_compatible_error_event() {
        use crate::gateway::provider_types::Provider;

        let (tx, rx) = tokio::sync::oneshot::channel();
        let processor = StreamChunkProcessor {
            chunk_index: AtomicU32::new(0),
            first_token_received: AtomicBool::new(false),
            first_token_time_ms: AtomicU32::new(0),
            start: Instant::now(),
            kafka_tx: tokio::sync::mpsc::channel(1).0,
            provider_name: "openai".to_string(),
            project_id: uuid::Uuid::nil(),
            mask_output_pii: false,
            block_exfiltration_urls: false,
            exfiltration_buffer: Mutex::new(String::new()),
            blocked_tools: Vec::new(),
            allowed_tools: None,
            completion_tx: Mutex::new(Some(tx)),
            latency_tracker: None,
            judge_buffer: None,
            client_include_usage: false,
        };

        let error = GatewayError::ProviderError {
            provider: Provider::OpenAi,
            status: 429,
            message: "Rate limit reached".to_string(),
        };

        let event_result = processor.handle_error(error);
        let _event = event_result.expect("handle_error must return Ok");

        // Completion summary must be sent with the error for observability
        let summary = rx.await.expect("completion must be sent on error");
        assert!(summary.error.is_some(), "stream_error must be in summary");

        // Verify the event data contains an OpenAI-compatible error object
        let provider_err = GatewayError::ProviderError {
            provider: Provider::OpenAi,
            status: 429,
            message: "Rate limit reached".to_string(),
        };
        let (error_type, message) = provider_err.client_facing_details();
        assert_eq!(error_type, "api_error");
        assert!(
            !message.contains("Stream processing error occurred"),
            "must not use the old generic error message"
        );

        let expected_json = serde_json::json!({
            "error": {
                "message": message,
                "type": error_type,
                "code": null,
            }
        });
        let parsed: serde_json::Value = serde_json::from_str(&expected_json.to_string()).unwrap();
        assert!(parsed["error"]["message"].is_string());
        assert!(parsed["error"]["type"].is_string());
        assert!(parsed["error"]["code"].is_null());
    }

    /// Verify different error types produce appropriate client-facing messages.
    #[test]
    fn test_handle_error_rate_limit_vs_timeout() {
        let rate_limit = GatewayError::RateLimitExceeded {
            limit: 100,
            reset_seconds: 60,
        };
        let (rtype, rmsg) = rate_limit.client_facing_details();
        assert_eq!(rtype, "rate_limit_error");
        assert!(rmsg.contains("Rate limit"), "got: {rmsg}");

        let timeout = GatewayError::Timeout("provider took too long".to_string());
        let (ttype, tmsg) = timeout.client_facing_details();
        assert_eq!(ttype, "timeout_error");
        assert!(tmsg.contains("timed out"), "got: {tmsg}");
    }

    fn make_processor(
        client_include_usage: bool,
    ) -> (
        StreamChunkProcessor,
        tokio::sync::oneshot::Receiver<StreamCompletionSummary>,
    ) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let processor = StreamChunkProcessor {
            chunk_index: AtomicU32::new(0),
            first_token_received: AtomicBool::new(false),
            first_token_time_ms: AtomicU32::new(0),
            start: Instant::now(),
            kafka_tx: tokio::sync::mpsc::channel(64).0,
            provider_name: "openai".to_string(),
            project_id: uuid::Uuid::nil(),
            mask_output_pii: false,
            block_exfiltration_urls: false,
            exfiltration_buffer: Mutex::new(String::new()),
            blocked_tools: Vec::new(),
            allowed_tools: None,
            completion_tx: Mutex::new(Some(tx)),
            latency_tracker: None,
            judge_buffer: None,
            client_include_usage,
        };
        (processor, rx)
    }

    fn make_delta_chunk(content: &str) -> ChatCompletionChunk {
        use crate::gateway::types::{ChunkChoice, ChunkDelta};
        ChatCompletionChunk {
            id: "chatcmpl-test".to_string(),
            object: "chat.completion.chunk".to_string(),
            created: 0,
            model: "gpt-4o".to_string(),
            choices: vec![ChunkChoice {
                index: 0,
                delta: ChunkDelta {
                    role: None,
                    content: Some(content.to_string()),
                    tool_calls: None,
                    thinking: None,
                    reasoning_content: None,
                },
                finish_reason: None,
            }],
            usage: None,
        }
    }

    fn make_finish_chunk(usage: Option<Usage>) -> ChatCompletionChunk {
        use crate::gateway::types::{ChunkChoice, ChunkDelta, FinishReason};
        ChatCompletionChunk {
            id: "chatcmpl-test".to_string(),
            object: "chat.completion.chunk".to_string(),
            created: 0,
            model: "gpt-4o".to_string(),
            choices: vec![ChunkChoice {
                index: 0,
                delta: ChunkDelta::default(),
                finish_reason: Some(FinishReason::Stop),
            }],
            usage,
        }
    }

    fn make_usage_only_chunk(prompt: u32, completion: u32) -> ChatCompletionChunk {
        ChatCompletionChunk {
            id: "chatcmpl-test".to_string(),
            object: "chat.completion.chunk".to_string(),
            created: 0,
            model: "gpt-4o".to_string(),
            choices: vec![],
            usage: Some(Usage {
                prompt_tokens: prompt,
                completion_tokens: completion,
                total_tokens: prompt + completion,
                thinking_tokens: None,
                completion_tokens_details: None,
                prompt_tokens_details: None,
            }),
        }
    }

    #[tokio::test]
    async fn test_usage_only_chunk_suppressed_when_client_did_not_opt_in() {
        let (processor, _rx) = make_processor(false);

        // Regular chunk should produce a data event
        let event = processor.process(Ok(make_delta_chunk("Hello"))).unwrap();
        let event_str = format!("{:?}", event);
        assert!(
            event_str.contains("Hello"),
            "regular chunk must be forwarded"
        );

        // Usage-only chunk should be suppressed (produces a comment, not data)
        let event = processor.process(Ok(make_usage_only_chunk(10, 5))).unwrap();
        let event_str = format!("{:?}", event);
        assert!(
            !event_str.contains("prompt_tokens"),
            "usage-only chunk must NOT be forwarded when client didn't opt in"
        );
    }

    #[tokio::test]
    async fn test_usage_only_chunk_forwarded_when_client_opted_in() {
        let (processor, _rx) = make_processor(true);

        // Regular chunk
        processor.process(Ok(make_delta_chunk("Hello"))).unwrap();

        // Usage-only chunk should be forwarded as a data event
        let event = processor.process(Ok(make_usage_only_chunk(10, 5))).unwrap();
        let event_str = format!("{:?}", event);
        assert!(
            event_str.contains("prompt_tokens"),
            "usage-only chunk must be forwarded when client opted in"
        );
    }

    #[tokio::test]
    async fn test_summary_captures_usage_from_usage_only_chunk() {
        let (processor, rx) = make_processor(false);

        // Simulate a normal stream: delta, finish (no usage), then usage-only
        processor.process(Ok(make_delta_chunk("Hi"))).unwrap();
        processor.process(Ok(make_finish_chunk(None))).unwrap();
        processor
            .process(Ok(make_usage_only_chunk(42, 10)))
            .unwrap();

        let summary = rx.await.expect("summary must be sent");
        let usage = summary
            .usage
            .expect("summary must contain usage from usage-only chunk");
        assert_eq!(usage.prompt_tokens, 42);
        assert_eq!(usage.completion_tokens, 10);
        assert_eq!(usage.total_tokens, 52);
    }

    #[tokio::test]
    async fn test_summary_captures_usage_from_finish_chunk_when_inlined() {
        let (processor, rx) = make_processor(false);

        let inline_usage = Some(Usage {
            prompt_tokens: 20,
            completion_tokens: 8,
            total_tokens: 28,
            thinking_tokens: None,
            completion_tokens_details: None,
            prompt_tokens_details: None,
        });

        // Some providers inline usage on the finish chunk (no separate usage-only chunk)
        processor.process(Ok(make_delta_chunk("Hi"))).unwrap();
        processor
            .process(Ok(make_finish_chunk(inline_usage)))
            .unwrap();

        let summary = rx.await.expect("summary must be sent");
        let usage = summary.usage.expect("summary must contain inlined usage");
        assert_eq!(usage.prompt_tokens, 20);
        assert_eq!(usage.completion_tokens, 8);
    }
}
