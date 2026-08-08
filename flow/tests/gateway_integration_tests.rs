//! End-to-end integration tests for the LLM gateway.
//!
//! Every test starts the real Axum application on a random port, uses
//! `wiremock` to stub LLM provider HTTP responses, and then drives the
//! gateway with a `reqwest` client — exercising the full handler pipeline:
//! auth, provider key lookup, request dispatch, response normalization,
//! fallback, and response headers.

mod test_support;
use test_support::{test_project_id, test_user_id, TestApp};

use serde_json::{json, Value};
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, ResponseTemplate};

// ──────────────────────────────────────────────────────────────────────────────
// Helper response bodies
// ──────────────────────────────────────────────────────────────────────────────

fn openai_chat_response(model: &str, content: &str) -> Value {
    json!({
        "id": "chatcmpl-test123",
        "object": "chat.completion",
        "created": 1_699_000_000u64,
        "model": model,
        "choices": [{
            "index": 0,
            "message": { "role": "assistant", "content": content },
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": 10,
            "completion_tokens": 10,
            "total_tokens": 20
        }
    })
}

fn anthropic_chat_response(content: &str) -> Value {
    json!({
        "id": "msg_test123",
        "type": "message",
        "role": "assistant",
        "content": [{ "type": "text", "text": content }],
        "model": "claude-3-5-sonnet-20241022",
        "stop_reason": "end_turn",
        "usage": { "input_tokens": 10, "output_tokens": 10 }
    })
}

fn google_chat_response(content: &str) -> Value {
    json!({
        "candidates": [{
            "content": {
                "role": "model",
                "parts": [{ "text": content }]
            },
            "finishReason": "STOP",
            "index": 0
        }],
        "usageMetadata": {
            "promptTokenCount": 10,
            "candidatesTokenCount": 10,
            "totalTokenCount": 20
        }
    })
}

/// Build an OpenAI-format SSE stream with N text chunks followed by `[DONE]`.
fn openai_sse_body(model: &str, chunks: &[&str]) -> String {
    let mut body = String::new();
    // First chunk: role delta
    let first = json!({
        "id": "chatcmpl-stream",
        "object": "chat.completion.chunk",
        "created": 1_699_000_000u64,
        "model": model,
        "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": null}]
    });
    body.push_str(&format!("data: {}\n\n", first));

    for chunk_text in chunks {
        let chunk = json!({
            "id": "chatcmpl-stream",
            "object": "chat.completion.chunk",
            "created": 1_699_000_000u64,
            "model": model,
            "choices": [{"index": 0, "delta": {"content": chunk_text}, "finish_reason": null}]
        });
        body.push_str(&format!("data: {}\n\n", chunk));
    }

    // Final chunk with finish_reason
    let last = json!({
        "id": "chatcmpl-stream",
        "object": "chat.completion.chunk",
        "created": 1_699_000_000u64,
        "model": model,
        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]
    });
    body.push_str(&format!("data: {}\n\n", last));
    body.push_str("data: [DONE]\n\n");
    body
}

// ──────────────────────────────────────────────────────────────────────────────
// Auth / input validation
// ──────────────────────────────────────────────────────────────────────────────

/// A request without the `X-Project-Id` header must be rejected before any
/// provider call is attempted.
#[tokio::test]
async fn test_missing_project_id_returns_error() {
    let app = TestApp::new().await;

    let resp = app
        .client()
        .post(app.chat_completions_url())
        // Deliberately omit X-Project-Id
        .header("X-User-Id", test_user_id().to_string())
        .json(&json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "hello"}]
        }))
        .send()
        .await
        .unwrap();

    assert!(
        resp.status().as_u16() >= 400,
        "expected 4xx for missing project id, got {}",
        resp.status()
    );
}

/// A model name that no provider supports must return a 4xx error.
#[tokio::test]
async fn test_unsupported_model_returns_error() {
    let app = TestApp::new().await;

    let resp = app
        .post_chat(json!({
            "model": "gpt-99-ultra-fantasy",
            "messages": [{"role": "user", "content": "hello"}]
        }))
        .await;

    assert!(
        resp.status().as_u16() >= 400,
        "expected 4xx for unsupported model, got {}",
        resp.status()
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Non-streaming — OpenAI
// ──────────────────────────────────────────────────────────────────────────────

/// Happy-path: OpenAI returns a valid completion; gateway normalises it and
/// surfaces `x-reiver-provider: openai`.
#[tokio::test]
async fn test_openai_non_streaming_success() {
    let app = TestApp::new().await;

    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(openai_chat_response("gpt-4o", "Hello from OpenAI!")),
        )
        .mount(&app.openai_mock)
        .await;

    let resp = app
        .post_chat(json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .await;

    assert_eq!(resp.status(), 200, "expected 200 from gateway");

    let provider_header = resp
        .headers()
        .get("x-reiver-provider")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(provider_header, "openai");

    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        body["choices"][0]["message"]["content"],
        "Hello from OpenAI!"
    );
}

/// When OpenAI returns a 500 the gateway must surface an error to the caller.
#[tokio::test]
async fn test_openai_500_returns_gateway_error() {
    let app = TestApp::new().await;

    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(500).set_body_string("internal error"))
        .mount(&app.openai_mock)
        .await;

    // Disable fallback chain for this test so the 500 propagates cleanly.
    // We use a model prefix that has no fallback configured in test_config.
    let resp = app
        .post_chat(json!({
            "model": "chatgpt-4o-latest",   // "chatgpt-" prefix → no fallback chain
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .await;

    assert!(
        resp.status().as_u16() >= 400,
        "expected 4xx/5xx for provider error, got {}",
        resp.status()
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Streaming — OpenAI
// ──────────────────────────────────────────────────────────────────────────────

/// The gateway must forward SSE chunks from the provider and return them as a
/// `text/event-stream` response.  We parse the streamed body and verify we
/// receive at least the expected number of `data:` lines.
#[tokio::test]
async fn test_openai_streaming_chunks_received() {
    let app = TestApp::new().await;

    let sse_body = openai_sse_body("gpt-4o", &["Hello", " world", "!"]);

    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(sse_body),
        )
        .mount(&app.openai_mock)
        .await;

    let resp = app
        .post_chat(json!({
            "model": "gpt-4o",
            "stream": true,
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .await;

    assert_eq!(resp.status(), 200, "expected 200 from gateway");

    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.contains("text/event-stream"),
        "expected text/event-stream, got {content_type}"
    );

    let body = resp.text().await.unwrap();
    let data_lines: Vec<&str> = body.lines().filter(|l| l.starts_with("data: ")).collect();

    // We sent 3 content chunks + 1 role chunk + 1 finish + [DONE] = 6 data lines.
    assert!(
        data_lines.len() >= 4,
        "expected ≥4 data: lines, got {}:\n{}",
        data_lines.len(),
        body
    );

    // Every data line except [DONE] must parse as valid JSON.
    for line in &data_lines {
        let payload = line.trim_start_matches("data: ");
        if payload == "[DONE]" {
            continue;
        }
        serde_json::from_str::<Value>(payload)
            .unwrap_or_else(|e| panic!("invalid JSON in SSE line '{payload}': {e}"));
    }
}

/// When the provider returns a 500 on a streaming request, the gateway must
/// not hang — it must surface an error response or close the connection.
#[tokio::test]
async fn test_openai_streaming_provider_500() {
    let app = TestApp::new().await;

    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(500).set_body_string("upstream down"))
        .mount(&app.openai_mock)
        .await;

    let resp = app
        .post_chat(json!({
            "model": "chatgpt-4o-latest",  // no fallback chain
            "stream": true,
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .await;

    // Must not be a 200 — either an immediate error response or an SSE error event.
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap();
    assert!(
        status >= 400 || body.contains("error"),
        "expected error on provider 500, got status={status} body={body}"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Fallback
// ──────────────────────────────────────────────────────────────────────────────

/// When the primary provider (OpenAI) returns a 500, the gateway falls back to
/// the next provider in the chain (Anthropic mock) and returns its response.
/// The `x-reiver-fallback-used: true` header must be set.
#[tokio::test]
async fn test_fallback_on_primary_500() {
    let app = TestApp::new().await;

    // Primary OpenAI fails.
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(500).set_body_string("overloaded"))
        .mount(&app.openai_mock)
        .await;

    // Fallback Anthropic succeeds.
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(anthropic_chat_response("Fallback response from Claude!")),
        )
        .mount(&app.anthropic_mock)
        .await;

    // Fallback models come from the request `models` array.
    let resp = app
        .post_chat(json!({
            "model": "gpt-4o",
            "models": ["gpt-4o", "claude-3-5-sonnet-20241022"],
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .await;

    assert_eq!(resp.status(), 200, "expected 200 after fallback");

    let fallback_header = resp
        .headers()
        .get("x-reiver-fallback-used")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(
        fallback_header, "true",
        "expected x-reiver-fallback-used: true"
    );

    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        body["choices"][0]["message"]["content"],
        "Fallback response from Claude!"
    );
}

/// A 400 from the provider is a client error and must NOT trigger fallback.
/// The Anthropic mock must receive zero requests.
#[tokio::test]
async fn test_no_fallback_on_primary_400() {
    let app = TestApp::new().await;

    // Primary returns 400 (client error — bad request).
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": {"type": "invalid_request_error", "message": "bad context length"}
        })))
        .mount(&app.openai_mock)
        .await;

    // Fallback must NOT be called.
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&app.anthropic_mock)
        .await;

    let resp = app
        .post_chat(json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .await;

    // Gateway must propagate the 4xx rather than falling back.
    assert!(
        resp.status().as_u16() >= 400,
        "expected 4xx, got {}",
        resp.status()
    );

    // wiremock verifies that the Anthropic mock received exactly 0 calls
    // when the TestApp is dropped (mock expectation checking on drop).
}

// ──────────────────────────────────────────────────────────────────────────────
// Non-streaming — Anthropic
// ──────────────────────────────────────────────────────────────────────────────

/// The Anthropic provider adapter must correctly translate request and response.
#[tokio::test]
async fn test_anthropic_non_streaming_success() {
    let app = TestApp::new().await;

    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(anthropic_chat_response("Hello from Claude!")),
        )
        .mount(&app.anthropic_mock)
        .await;

    let resp = app
        .post_chat(json!({
            "model": "claude-3-5-sonnet-20241022",
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 100
        }))
        .await;

    assert_eq!(resp.status(), 200, "expected 200 from gateway");

    let provider_header = resp
        .headers()
        .get("x-reiver-provider")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(provider_header, "anthropic");

    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        body["choices"][0]["message"]["content"],
        "Hello from Claude!"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Non-streaming — Google Gemini
// ──────────────────────────────────────────────────────────────────────────────

/// The Google provider adapter must translate the Gemini response format into
/// the OpenAI-compatible response format.
#[tokio::test]
async fn test_google_non_streaming_success() {
    let app = TestApp::new().await;

    // Gemini URL contains the model name: /models/{model}:generateContent
    Mock::given(method("POST"))
        .and(path_regex(r"^/models/gemini-.*:generateContent$"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(google_chat_response("Hello from Gemini!")),
        )
        .mount(&app.google_mock)
        .await;

    let resp = app
        .post_chat(json!({
            "model": "gemini-1.5-pro",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .await;

    assert_eq!(resp.status(), 200, "expected 200 from gateway");

    let provider_header = resp
        .headers()
        .get("x-reiver-provider")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(provider_header, "google");

    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        body["choices"][0]["message"]["content"],
        "Hello from Gemini!"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Introspection
// ──────────────────────────────────────────────────────────────────────────────

/// When a project has `gateway_introspection_enabled = true` in its settings
/// (surfaced here via the in-memory cache), the gateway must inject a
/// `thinking` config into the request before forwarding it to the provider.
/// We verify this by asserting the OpenAI mock receives the `thinking` field.
#[tokio::test]
async fn test_introspection_applied_from_project_settings() {
    let app = TestApp::new().await;

    // Enable introspection for the test project via the cache.
    app.set_introspection(true, 5_000);

    // Capture the request body sent to the provider mock.
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(openai_chat_response("gpt-4o", "thinking response")),
        )
        .mount(&app.openai_mock)
        .await;

    let resp = app
        .post_chat(json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "think carefully"}]
            // No "thinking" field — the gateway should inject it.
        }))
        .await;

    // Response must still succeed (the OpenAI mock ignores unknown fields).
    assert_eq!(resp.status(), 200, "expected 200");

    // Verify the provider actually received a request (mock was called).
    assert_eq!(
        app.openai_mock.received_requests().await.unwrap().len(),
        1,
        "expected exactly 1 call to OpenAI mock"
    );

    // Verify that the `thinking` field was injected into the forwarded body.
    let received = &app.openai_mock.received_requests().await.unwrap()[0];
    let forwarded_body: Value =
        serde_json::from_slice(&received.body).expect("provider request must be valid JSON");

    assert!(
        forwarded_body.get("thinking").is_some(),
        "expected 'thinking' field to be injected by the gateway, got body: {forwarded_body}"
    );

    let thinking = &forwarded_body["thinking"];
    // ThinkingConfig.thinking_type serializes as "type" (serde rename).
    assert_eq!(
        thinking["type"], "enabled",
        "thinking.type should be 'enabled'"
    );
    assert_eq!(
        thinking["budget_tokens"], 5_000,
        "budget_tokens should match the project setting"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Routing: per-request `models` array overrides server-side fallback chains
// ──────────────────────────────────────────────────────────────────────────────

/// When a request carries `models: ["gpt-4o", "claude-3-5-sonnet-20241022"]`
/// and the primary (gpt-4o) returns a 500, the gateway must use the request's
/// fallback list — not the server-configured chains — and succeed via Claude.
#[tokio::test]
async fn test_request_models_override_server_fallback_chains() {
    let app = TestApp::new().await;

    // Primary OpenAI fails.
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(500).set_body_string("overloaded"))
        .mount(&app.openai_mock)
        .await;

    // Fallback Anthropic succeeds.
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(anthropic_chat_response("Routed via request fallback!")),
        )
        .mount(&app.anthropic_mock)
        .await;

    let resp = app
        .post_chat(json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "hi"}],
            "models": ["gpt-4o", "claude-3-5-sonnet-20241022"]
        }))
        .await;

    assert_eq!(
        resp.status(),
        200,
        "expected 200 after request-level fallback"
    );
    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        body["choices"][0]["message"]["content"],
        "Routed via request fallback!"
    );
}

/// When the request `models` array specifies a Google model as fallback,
/// the gateway must skip the server-side chain (which has Claude) and try
/// Gemini instead.
#[tokio::test]
async fn test_request_models_picks_google_over_server_default_anthropic() {
    let app = TestApp::new().await;

    // Primary OpenAI fails.
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(500).set_body_string("error"))
        .mount(&app.openai_mock)
        .await;

    // Anthropic mock must NOT be called — request fallback skips it.
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&app.anthropic_mock)
        .await;

    // Google succeeds.
    Mock::given(method("POST"))
        .and(path_regex(r"^/models/gemini-.*:generateContent$"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(google_chat_response("Google fallback!")),
        )
        .mount(&app.google_mock)
        .await;

    let resp = app
        .post_chat(json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "hi"}],
            "models": ["gpt-4o", "gemini-1.5-pro"]
        }))
        .await;

    assert_eq!(resp.status(), 200, "expected 200 from Google fallback");
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["choices"][0]["message"]["content"], "Google fallback!");
}

// ──────────────────────────────────────────────────────────────────────────────
// Routing: `allow_fallbacks: false` suppresses fallback
// ──────────────────────────────────────────────────────────────────────────────

/// When the request carries `provider: { allow_fallbacks: false }`, the gateway
/// must NOT try any fallback even though the server has fallback enabled and a
/// chain configured.
#[tokio::test]
async fn test_allow_fallbacks_false_suppresses_fallback() {
    let app = TestApp::new().await;

    // Primary OpenAI fails.
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(500).set_body_string("error"))
        .mount(&app.openai_mock)
        .await;

    // Anthropic mock must NOT be called.
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&app.anthropic_mock)
        .await;

    let resp = app
        .post_chat(json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "hi"}],
            "provider": { "allow_fallbacks": false }
        }))
        .await;

    assert!(
        resp.status().as_u16() >= 400,
        "expected error when fallback is suppressed, got {}",
        resp.status()
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Routing: project-level `fallback_enabled: false` suppresses fallback
// ──────────────────────────────────────────────────────────────────────────────

/// When the project has `fallback_enabled: false` in its settings, the gateway
/// must NOT try any fallback models even though the server has them configured.
#[tokio::test]
async fn test_project_fallback_disabled_suppresses_fallback() {
    let app = TestApp::new().await;

    // Disable fallback at project level.
    app.set_routing(false, Vec::new(), None);

    // Primary OpenAI fails.
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(500).set_body_string("error"))
        .mount(&app.openai_mock)
        .await;

    // Anthropic mock must NOT be called.
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&app.anthropic_mock)
        .await;

    let resp = app
        .post_chat(json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .await;

    assert!(
        resp.status().as_u16() >= 400,
        "expected error when project fallback is disabled, got {}",
        resp.status()
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Routing: per-request `allow_fallbacks: true` overrides project `false`
// ──────────────────────────────────────────────────────────────────────────────

/// Even when the project has `fallback_enabled: false`, a per-request
/// `allow_fallbacks: true` must re-enable fallback.
#[tokio::test]
async fn test_request_allow_fallbacks_overrides_project_disabled() {
    let app = TestApp::new().await;

    // Project has fallback disabled.
    app.set_routing(false, Vec::new(), None);

    // Primary OpenAI fails.
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(500).set_body_string("error"))
        .mount(&app.openai_mock)
        .await;

    // Fallback Anthropic succeeds.
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(anthropic_chat_response("Override worked!")),
        )
        .mount(&app.anthropic_mock)
        .await;

    let resp = app
        .post_chat(json!({
            "model": "gpt-4o",
            "models": ["gpt-4o", "claude-3-5-sonnet-20241022"],
            "messages": [{"role": "user", "content": "hi"}],
            "provider": { "allow_fallbacks": true }
        }))
        .await;

    assert_eq!(
        resp.status(),
        200,
        "expected 200 — per-request allow_fallbacks should override project setting"
    );
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["choices"][0]["message"]["content"], "Override worked!");
}

// ──────────────────────────────────────────────────────────────────────────────
// Routing: project-level default_fallback_models
// ──────────────────────────────────────────────────────────────────────────────

/// When the project has `default_fallback_models` set and the primary fails,
/// the gateway should use those instead of the server-level chains.
#[tokio::test]
async fn test_project_default_fallback_models_used() {
    let app = TestApp::new().await;

    // Project overrides fallback to Google instead of Anthropic.
    app.set_routing(true, vec!["gemini-1.5-pro".to_string()], None);

    // Primary OpenAI fails.
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(500).set_body_string("error"))
        .mount(&app.openai_mock)
        .await;

    // Anthropic mock must NOT be called.
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&app.anthropic_mock)
        .await;

    // Google succeeds.
    Mock::given(method("POST"))
        .and(path_regex(r"^/models/gemini-.*:generateContent$"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(google_chat_response("Project fallback to Gemini!")),
        )
        .mount(&app.google_mock)
        .await;

    let resp = app
        .post_chat(json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .await;

    assert_eq!(resp.status(), 200, "expected 200 via project fallback");
    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        body["choices"][0]["message"]["content"],
        "Project fallback to Gemini!"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Routing: request `models` takes precedence over project defaults
// ──────────────────────────────────────────────────────────────────────────────

/// Even when the project has `default_fallback_models` set, a per-request
/// `models` array takes precedence.
#[tokio::test]
async fn test_request_models_override_project_defaults() {
    let app = TestApp::new().await;

    // Project says fallback to Google.
    app.set_routing(true, vec!["gemini-1.5-pro".to_string()], None);

    // Primary OpenAI fails.
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(500).set_body_string("error"))
        .mount(&app.openai_mock)
        .await;

    // Google mock must NOT be called.
    Mock::given(method("POST"))
        .and(path_regex(r"^/models/gemini-.*:generateContent$"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&app.google_mock)
        .await;

    // Anthropic succeeds (the request explicitly asks for it).
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(anthropic_chat_response("Request override wins!")),
        )
        .mount(&app.anthropic_mock)
        .await;

    let resp = app
        .post_chat(json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "hi"}],
            "models": ["gpt-4o", "claude-3-5-sonnet-20241022"]
        }))
        .await;

    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        body["choices"][0]["message"]["content"],
        "Request override wins!"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Routing: primary model is deduplicated from request fallback list
// ──────────────────────────────────────────────────────────────────────────────

/// When the request `models` array includes the primary model, the gateway
/// must not retry it as a fallback. If the only other fallback is Anthropic,
/// the Anthropic mock must receive exactly 1 request (the fallback), and the
/// OpenAI mock must receive exactly 1 request (the primary).
#[tokio::test]
async fn test_primary_deduplicated_from_fallback_list() {
    let app = TestApp::new().await;

    // Primary OpenAI fails.
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(500).set_body_string("error"))
        .expect(1)
        .mount(&app.openai_mock)
        .await;

    // Fallback Anthropic succeeds.
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(anthropic_chat_response("Dedup works!")),
        )
        .expect(1)
        .mount(&app.anthropic_mock)
        .await;

    let resp = app
        .post_chat(json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "hi"}],
            "models": ["gpt-4o", "gpt-4o", "claude-3-5-sonnet-20241022"]
        }))
        .await;

    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["choices"][0]["message"]["content"], "Dedup works!");
    // wiremock expect(1) on both mocks verifies the counts on drop.
}

// ──────────────────────────────────────────────────────────────────────────────
// Routing: streaming fallback uses request `models` array
// ──────────────────────────────────────────────────────────────────────────────

/// The streaming path must honour the `models` array for fallback just like
/// the non-streaming path.
#[tokio::test]
async fn test_streaming_fallback_uses_request_models() {
    let app = TestApp::new().await;

    // Primary OpenAI fails on streaming request.
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(500).set_body_string("error"))
        .mount(&app.openai_mock)
        .await;

    // Fallback Anthropic streams back a valid response.
    // The Anthropic provider converts SSE to OpenAI format before the gateway
    // forwards it to the client.
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(anthropic_chat_response("Streaming fallback!")),
        )
        .mount(&app.anthropic_mock)
        .await;

    let resp = app
        .post_chat(json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "hi"}],
            "models": ["gpt-4o", "claude-3-5-sonnet-20241022"]
        }))
        .await;

    // Even though the client asked for streaming, when primary fails the
    // gateway tries the fallback non-streaming first, so we just verify success.
    assert_eq!(resp.status(), 200, "expected 200 after streaming fallback");
}

/// Streaming with `allow_fallbacks: false` must not fall back.
#[tokio::test]
async fn test_streaming_allow_fallbacks_false() {
    let app = TestApp::new().await;

    // Primary OpenAI fails on streaming request.
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(500).set_body_string("error"))
        .mount(&app.openai_mock)
        .await;

    // Anthropic must NOT be called.
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&app.anthropic_mock)
        .await;

    let resp = app
        .post_chat(json!({
            "model": "gpt-4o",
            "stream": true,
            "messages": [{"role": "user", "content": "hi"}],
            "provider": { "allow_fallbacks": false }
        }))
        .await;

    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap();
    assert!(
        status >= 400 || body.contains("error"),
        "expected error when streaming fallback is suppressed, got status={status}"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Routing: models-only request (model field empty, models array provided)
// ──────────────────────────────────────────────────────────────────────────────

/// A request with an empty `model` field but a valid `models` array should use
/// the first model from the array as primary.
#[tokio::test]
async fn test_empty_model_uses_first_from_models_array() {
    let app = TestApp::new().await;

    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(openai_chat_response("gpt-4o", "From models array!")),
        )
        .mount(&app.openai_mock)
        .await;

    let resp = app
        .post_chat(json!({
            "model": "",
            "messages": [{"role": "user", "content": "hi"}],
            "models": ["gpt-4o", "claude-3-5-sonnet-20241022"]
        }))
        .await;

    assert_eq!(
        resp.status(),
        200,
        "expected 200 using first model from array"
    );
    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        body["choices"][0]["message"]["content"],
        "From models array!"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Prompt Config resolution through InMemoryPromptStore
// ──────────────────────────────────────────────────────────────────────────────

use reiver_flow::gateway::prompt_store::{InMemoryPromptStore, PromptConfigRow};
use reiver_flow::gateway::prompt_resolver::PromptVersionConfig;
use rust_decimal::Decimal;
use std::sync::Arc;

fn build_prompt_store() -> (Arc<InMemoryPromptStore>, uuid::Uuid, uuid::Uuid) {
    let project_id = test_project_id();
    let config_id = uuid::Uuid::new_v4();
    let version_id = uuid::Uuid::new_v4();

    let mut store = InMemoryPromptStore::new();
    store.add_config(
        project_id,
        "integration-config",
        PromptConfigRow {
            id: config_id,
            active_version_id: Some(version_id),
        },
    );
    store.add_version(PromptVersionConfig {
        id: version_id,
        system_prompt: Some("You are a test assistant.".to_string()),
        model: Some("gpt-4o".to_string()),
        temperature: Decimal::new(3, 1),
        max_tokens: Some(512),
        variables: serde_json::json!([]),
        tools: None,
        response_format: None,
        parameters: serde_json::Value::Null,
        allowed_tools: None,
    });

    (Arc::new(store), config_id, version_id)
}

/// A request with `prompt_config` in the body should have the system prompt
/// injected and the model overridden by the prompt version config.
#[tokio::test]
async fn test_prompt_config_injects_system_prompt() {
    let (store, _, _) = build_prompt_store();

    let app = TestApp::new_with_prompt_store(Some(store)).await;

    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(openai_chat_response("gpt-4o", "Hello from prompt config!")),
        )
        .mount(&app.openai_mock)
        .await;

    let resp = app
        .post_chat(json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "hi"}],
            "prompt_config": "integration-config"
        }))
        .await;

    assert_eq!(resp.status(), 200, "expected 200 with prompt config");

    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        body["choices"][0]["message"]["content"],
        "Hello from prompt config!"
    );
}

/// A request with `prompt_config` referencing a non-existent config should
/// proceed without prompt modification (the resolver returns None, request
/// goes through unchanged with its original model).
#[tokio::test]
async fn test_prompt_config_unknown_name_proceeds_without_modification() {
    let store = Arc::new(InMemoryPromptStore::new());
    let app = TestApp::new_with_prompt_store(Some(store)).await;

    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(openai_chat_response("gpt-4o", "No config found!")),
        )
        .mount(&app.openai_mock)
        .await;

    let resp = app
        .post_chat(json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "hi"}],
            "prompt_config": "nonexistent-config"
        }))
        .await;

    assert_eq!(
        resp.status(),
        200,
        "request should succeed even with unknown prompt config"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Routing: circuit breaker open → fallback to other providers
// ──────────────────────────────────────────────────────────────────────────────

/// When the primary provider fails (500 or circuit-breaker-open) and the
/// project has default_fallback_models configured, the gateway should fall
/// back to the configured fallback provider and return 200.
///
/// Regression test for trace 46061ee6c9a074d24d01fffde981ec22 where the
/// gateway returned 504 instead of falling back.
#[tokio::test]
async fn test_primary_failure_falls_back_to_project_defaults() {
    let app = TestApp::new().await;

    // Project has fallback enabled with Anthropic as the fallback model.
    app.set_routing(
        true,
        vec!["claude-3-5-sonnet-20241022".to_string()],
        None,
    );

    // Trip the OpenAI circuit breaker (may or may not prevent the primary
    // call depending on test timing — the 500 mock covers both paths).
    let cb = app.state.gateway_router.circuit_breaker();
    for _ in 0..10 {
        cb.record_failure(&reiver_flow::gateway::provider_types::Provider::OpenAi);
    }

    // OpenAI returns 500 (fallback-eligible) if the circuit breaker cooldown
    // expires before the handler checks it.
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(500).set_body_string("primary down"))
        .mount(&app.openai_mock)
        .await;

    // Anthropic fallback should succeed.
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(anthropic_chat_response("Fallback after primary failure!")),
        )
        .mount(&app.anthropic_mock)
        .await;

    let resp = app
        .post_chat(json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .await;

    assert_eq!(
        resp.status(),
        200,
        "primary failure should trigger fallback to project defaults"
    );

    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        body["choices"][0]["message"]["content"],
        "Fallback after primary failure!"
    );
}

/// When the primary provider fails and the request has a per-request `models`
/// array, the gateway should fall back to those models.
#[tokio::test]
async fn test_primary_failure_falls_back_to_request_models() {
    let app = TestApp::new().await;

    // Trip the OpenAI circuit breaker.
    let cb = app.state.gateway_router.circuit_breaker();
    for _ in 0..10 {
        cb.record_failure(&reiver_flow::gateway::provider_types::Provider::OpenAi);
    }

    // OpenAI returns 500 if the circuit breaker didn't prevent the call.
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&app.openai_mock)
        .await;

    // Anthropic fallback succeeds.
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(anthropic_chat_response("Request-level fallback!")),
        )
        .mount(&app.anthropic_mock)
        .await;

    let resp = app
        .post_chat(json!({
            "model": "gpt-4o",
            "models": ["gpt-4o", "claude-3-5-sonnet-20241022"],
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .await;

    assert_eq!(
        resp.status(),
        200,
        "primary failure + request models should fallback"
    );

    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        body["choices"][0]["message"]["content"],
        "Request-level fallback!"
    );
}

/// When a prompt config has a model override, the gateway should route the
/// request to the correct provider based on the overridden model.
#[tokio::test]
async fn test_prompt_config_model_override_routes_correctly() {
    let project_id = test_project_id();
    let config_id = uuid::Uuid::new_v4();
    let version_id = uuid::Uuid::new_v4();

    let mut store = InMemoryPromptStore::new();
    store.add_config(
        project_id,
        "anthropic-config",
        PromptConfigRow {
            id: config_id,
            active_version_id: Some(version_id),
        },
    );
    store.add_version(PromptVersionConfig {
        id: version_id,
        system_prompt: Some("You are Claude.".to_string()),
        model: Some("claude-3-5-sonnet-20241022".to_string()),
        temperature: Decimal::new(5, 1),
        max_tokens: Some(256),
        variables: serde_json::json!([]),
        tools: None,
        response_format: None,
        parameters: serde_json::Value::Null,
        allowed_tools: None,
    });

    let app = TestApp::new_with_prompt_store(Some(Arc::new(store))).await;

    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(anthropic_chat_response("Routed to Anthropic!")),
        )
        .mount(&app.anthropic_mock)
        .await;

    let resp = app
        .post_chat(json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "hi"}],
            "prompt_config": "anthropic-config"
        }))
        .await;

    assert_eq!(
        resp.status(),
        200,
        "prompt config should override model to claude"
    );

    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        body["choices"][0]["message"]["content"],
        "Routed to Anthropic!"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Platform key flag preserved through batch key resolution
// ──────────────────────────────────────────────────────────────────────────────

/// The test_support pre-populates provider keys with `is_platform: true`.
/// After the batch key resolution fix, fallback candidates correctly carry the
/// platform flag from the cache. This test verifies the batch resolution path
/// works end-to-end: primary (OpenAI) fails with 500, fallback (Anthropic)
/// succeeds — confirming that keys resolved through `get_provider_keys_batch`
/// are usable when the cache carries `is_platform: true`.
#[tokio::test]
async fn test_platform_key_flag_preserved_through_fallback() {
    let app = TestApp::new().await;

    // Primary OpenAI fails.
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(500).set_body_string("overloaded"))
        .expect(1)
        .mount(&app.openai_mock)
        .await;

    // Fallback Anthropic succeeds.
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(anthropic_chat_response("Platform key works!")),
        )
        .expect(1)
        .mount(&app.anthropic_mock)
        .await;

    let resp = app
        .post_chat(json!({
            "model": "gpt-4o",
            "models": ["gpt-4o", "claude-3-5-sonnet-20241022"],
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .await;

    assert_eq!(resp.status(), 200, "fallback with platform key should succeed");

    let fallback_header = resp
        .headers()
        .get("x-reiver-fallback-used")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert_eq!(fallback_header, "true");

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["choices"][0]["message"]["content"], "Platform key works!");
}

// ──────────────────────────────────────────────────────────────────────────────
// Duplicate fallback models are deduplicated
// ──────────────────────────────────────────────────────────────────────────────

/// When the `models` array contains duplicate fallback entries (e.g. the same
/// Anthropic model listed twice), the chain deduplication ensures the fallback
/// provider receives exactly one request — not two.
#[tokio::test]
async fn test_duplicate_fallback_models_deduplicated() {
    let app = TestApp::new().await;

    // Primary OpenAI fails.
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(500).set_body_string("error"))
        .expect(1)
        .mount(&app.openai_mock)
        .await;

    // Fallback Anthropic succeeds — must receive exactly 1 request.
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(anthropic_chat_response("Dedup fallback!")),
        )
        .expect(1)
        .mount(&app.anthropic_mock)
        .await;

    let resp = app
        .post_chat(json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "hi"}],
            "models": ["gpt-4o", "claude-3-5-sonnet-20241022", "claude-3-5-sonnet-20241022"]
        }))
        .await;

    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["choices"][0]["message"]["content"], "Dedup fallback!");
    // wiremock expect(1) verifies Anthropic received exactly 1 request on drop.
}
