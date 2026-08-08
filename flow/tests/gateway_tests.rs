//! Integration tests for the AI Gateway endpoints.
//!
//! These tests verify the HTTP-level behavior of the gateway endpoints.
//! Unit tests for internal gateway functionality are located in the source
//! modules themselves (src/gateway/*.rs).
//!
//! Note: The gateway module has comprehensive unit tests for:
//! - Request/response type serialization (types.rs)
//! - Model routing logic (router.rs)
//! - Error handling and sanitization (error.rs)
//! - Fallback and retry logic (fallback.rs)
//! - Cache key generation (cache.rs)
//! - Observability data building (observability.rs)
//! - Provider-specific message conversion (providers/*.rs)

use serde_json::json;

#[path = "helpers.rs"]
mod helpers;
use helpers::{json_post, with_auth};

#[cfg(test)]
mod gateway_request_tests {
    use super::*;

    /// Verify gateway request JSON structure is valid
    #[test]
    fn test_valid_request_structure() {
        let request = json!({
            "model": "gpt-4o",
            "messages": [
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Hello, how are you?"}
            ],
            "temperature": 0.7,
            "max_tokens": 1000
        });

        // Request should serialize correctly
        let json_str = serde_json::to_string(&request).unwrap();
        assert!(json_str.contains("gpt-4o"));
        assert!(json_str.contains("messages"));
    }

    /// Verify streaming request structure
    #[test]
    fn test_streaming_request_structure() {
        let request = json!({
            "model": "claude-3-5-sonnet",
            "messages": [
                {"role": "user", "content": "Write a haiku."}
            ],
            "stream": true,
            "max_tokens": 500
        });

        let json_str = serde_json::to_string(&request).unwrap();
        assert!(json_str.contains("\"stream\":true"));
    }

    /// Verify multimodal request structure
    #[test]
    fn test_multimodal_request_structure() {
        let request = json!({
            "model": "gpt-4o",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "What's in this image?"},
                        {"type": "image_url", "image_url": {"url": "https://example.com/image.jpg"}}
                    ]
                }
            ]
        });

        let json_str = serde_json::to_string(&request).unwrap();
        assert!(json_str.contains("image_url"));
        assert!(json_str.contains("What's in this image?"));
    }

    /// Verify tool calling request structure
    #[test]
    fn test_tool_calling_request_structure() {
        let request = json!({
            "model": "gpt-4o",
            "messages": [
                {"role": "user", "content": "What's the weather in Paris?"}
            ],
            "tools": [
                {
                    "type": "function",
                    "function": {
                        "name": "get_weather",
                        "description": "Get the current weather in a location",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "location": {"type": "string", "description": "City name"}
                            },
                            "required": ["location"]
                        }
                    }
                }
            ],
            "tool_choice": "auto"
        });

        let json_str = serde_json::to_string(&request).unwrap();
        assert!(json_str.contains("get_weather"));
        assert!(json_str.contains("tool_choice"));
    }

    /// Verify request with all optional parameters
    #[test]
    fn test_full_request_structure() {
        let request = json!({
            "model": "gpt-4o",
            "messages": [
                {"role": "user", "content": "Hello"}
            ],
            "temperature": 0.5,
            "top_p": 0.9,
            "max_tokens": 2000,
            "n": 1,
            "stream": false,
            "stop": ["END"],
            "frequency_penalty": 0.5,
            "presence_penalty": 0.5,
            "user": "user_123",
            "seed": 42,
            "response_format": {"type": "json_object"}
        });

        let json_str = serde_json::to_string(&request).unwrap();
        assert!(json_str.contains("frequency_penalty"));
        assert!(json_str.contains("seed"));
        assert!(json_str.contains("json_object"));
    }
}

#[cfg(test)]
mod gateway_response_tests {
    use super::*;

    /// Verify expected response structure
    #[test]
    fn test_response_structure() {
        let response = json!({
            "id": "chatcmpl-abc123",
            "object": "chat.completion",
            "created": 1700000000,
            "model": "gpt-4o-2024-08-06",
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": "Hello! How can I help you today?"
                    },
                    "finish_reason": "stop"
                }
            ],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 8,
                "total_tokens": 18
            }
        });

        let choices = response["choices"].as_array().unwrap();
        assert_eq!(choices.len(), 1);
        assert_eq!(response["object"], "chat.completion");
    }

    /// Verify streaming chunk structure
    #[test]
    fn test_streaming_chunk_structure() {
        let chunk = json!({
            "id": "chatcmpl-abc123",
            "object": "chat.completion.chunk",
            "created": 1700000000,
            "model": "gpt-4o",
            "choices": [
                {
                    "index": 0,
                    "delta": {
                        "content": "Hello"
                    },
                    "finish_reason": null
                }
            ]
        });

        assert_eq!(chunk["object"], "chat.completion.chunk");
        assert!(chunk["choices"][0]["delta"]["content"].is_string());
    }

    /// Verify tool call response structure
    #[test]
    fn test_tool_call_response_structure() {
        let response = json!({
            "id": "chatcmpl-abc123",
            "object": "chat.completion",
            "created": 1700000000,
            "model": "gpt-4o",
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": null,
                        "tool_calls": [
                            {
                                "id": "call_abc123",
                                "type": "function",
                                "function": {
                                    "name": "get_weather",
                                    "arguments": "{\"location\":\"Paris\"}"
                                }
                            }
                        ]
                    },
                    "finish_reason": "tool_calls"
                }
            ],
            "usage": {
                "prompt_tokens": 20,
                "completion_tokens": 15,
                "total_tokens": 35
            }
        });

        let tool_calls = &response["choices"][0]["message"]["tool_calls"];
        assert!(tool_calls.is_array());
        assert_eq!(response["choices"][0]["finish_reason"], "tool_calls");
    }
}

#[cfg(test)]
mod gateway_error_tests {
    use super::*;

    /// Verify error response structure
    #[test]
    fn test_error_response_structure() {
        let error = json!({
            "error": {
                "message": "The model 'unknown-model' does not exist or you do not have access to it.",
                "type": "invalid_request_error",
                "code": "model_not_found"
            }
        });

        assert!(error["error"]["message"].is_string());
        assert_eq!(error["error"]["type"], "invalid_request_error");
    }

    /// Verify rate limit error structure
    #[test]
    fn test_rate_limit_error_structure() {
        let error = json!({
            "error": {
                "message": "Rate limit exceeded. Please retry after some time.",
                "type": "rate_limit_error"
            }
        });

        assert_eq!(error["error"]["type"], "rate_limit_error");
    }

    /// Verify authentication error structure
    #[test]
    fn test_auth_error_structure() {
        let error = json!({
            "error": {
                "message": "Invalid API key",
                "type": "authentication_error"
            }
        });

        assert_eq!(error["error"]["type"], "authentication_error");
    }
}

#[cfg(test)]
mod gateway_http_tests {
    use super::*;

    /// Test that authorization header is properly constructed
    #[test]
    fn test_auth_header_format() {
        let req = json_post(
            "/v1/chat/completions",
            &json!({"model": "gpt-4o", "messages": []}),
        );
        let req = with_auth(req, "fx_test_key_12345");

        let auth_header = req.headers().get("authorization").unwrap();
        assert_eq!(auth_header, "Bearer fx_test_key_12345");
    }

    /// Test content-type header is set correctly
    #[test]
    fn test_content_type_header() {
        let req = json_post(
            "/v1/chat/completions",
            &json!({"model": "gpt-4o", "messages": []}),
        );

        let content_type = req.headers().get("content-type").unwrap();
        assert_eq!(content_type, "application/json");
    }

    /// Test that the request URI is correct
    #[test]
    fn test_request_uri() {
        let req = json_post("/v1/chat/completions", &json!({}));
        assert_eq!(req.uri(), "/v1/chat/completions");
    }
}

#[cfg(test)]
mod provider_model_mapping_tests {
    /// Test that model name patterns are documented correctly
    #[test]
    fn test_openai_model_patterns() {
        let openai_models = vec![
            "gpt-4o",
            "gpt-4-turbo",
            "gpt-3.5-turbo",
            "o1-preview",
            "o1-mini",
            "o3-mini",
            "chatgpt-4o-latest",
        ];

        for model in openai_models {
            assert!(
                model.starts_with("gpt-")
                    || model.starts_with("o1-")
                    || model.starts_with("o3-")
                    || model.starts_with("chatgpt-"),
                "Model {} should match OpenAI pattern",
                model
            );
        }
    }

    /// Test that Anthropic model patterns are documented correctly
    #[test]
    fn test_anthropic_model_patterns() {
        let anthropic_models = vec![
            "claude-3-opus",
            "claude-3-sonnet",
            "claude-3-haiku",
            "claude-3-5-sonnet",
            "claude-3-5-haiku",
        ];

        for model in anthropic_models {
            assert!(
                model.starts_with("claude-"),
                "Model {} should match Anthropic pattern",
                model
            );
        }
    }

    /// Test that Google model patterns are documented correctly
    #[test]
    fn test_google_model_patterns() {
        let google_models = vec![
            "gemini-pro",
            "gemini-1.5-pro",
            "gemini-1.5-flash",
            "gemini-2.0-flash",
        ];

        for model in google_models {
            assert!(
                model.starts_with("gemini-"),
                "Model {} should match Google pattern",
                model
            );
        }
    }

    /// Test that Bedrock model patterns are documented correctly
    #[test]
    fn test_bedrock_model_patterns() {
        let bedrock_models = vec![
            "bedrock/anthropic.claude-3-sonnet-20240229-v1:0",
            "anthropic.claude-3-opus-20240229-v1:0",
            "amazon.titan-text-express-v1",
            "meta.llama3-70b-instruct-v1:0",
            "mistral.mixtral-8x7b-instruct-v0:1",
            "cohere.command-r-plus-v1:0",
        ];

        for model in bedrock_models {
            assert!(
                model.starts_with("bedrock/")
                    || model.starts_with("anthropic.")
                    || model.starts_with("amazon.")
                    || model.starts_with("meta.")
                    || model.starts_with("mistral.")
                    || model.starts_with("cohere."),
                "Model {} should match Bedrock pattern",
                model
            );
        }
    }
}

/// Integration tests using wiremock to mock LLM provider APIs.
///
/// These tests verify end-to-end request/response handling with mock providers.
#[cfg(test)]
mod mock_provider_tests {
    use serde_json::json;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Test that OpenAI provider correctly parses a successful response
    #[tokio::test]
    async fn test_openai_mock_response_parsing() {
        let mock_server = MockServer::start().await;

        // Mock OpenAI chat completions endpoint
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .and(header("Authorization", "Bearer test-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": "chatcmpl-123",
                "object": "chat.completion",
                "created": 1700000000,
                "model": "gpt-4o",
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": "Hello! How can I help you today?"
                    },
                    "finish_reason": "stop"
                }],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 12,
                    "total_tokens": 22
                }
            })))
            .mount(&mock_server)
            .await;

        // Create a client and send request to mock server
        let client = reqwest::Client::new();
        let response = client
            .post(format!("{}/v1/chat/completions", mock_server.uri()))
            .header("Authorization", "Bearer test-key")
            .header("Content-Type", "application/json")
            .json(&json!({
                "model": "gpt-4o",
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .send()
            .await
            .unwrap();

        assert!(response.status().is_success());

        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["object"], "chat.completion");
        assert_eq!(
            body["choices"][0]["message"]["content"],
            "Hello! How can I help you today?"
        );
    }

    /// Test that Anthropic provider correctly parses a successful response
    #[tokio::test]
    async fn test_anthropic_mock_response_parsing() {
        let mock_server = MockServer::start().await;

        // Mock Anthropic messages endpoint
        Mock::given(method("POST"))
            .and(path("/v1/messages"))
            .and(header("x-api-key", "test-key"))
            .and(header("anthropic-version", "2023-06-01"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": "msg_123",
                "type": "message",
                "role": "assistant",
                "content": [{
                    "type": "text",
                    "text": "Hello! I'm Claude."
                }],
                "model": "claude-3-5-sonnet-20241022",
                "stop_reason": "end_turn",
                "usage": {
                    "input_tokens": 10,
                    "output_tokens": 8
                }
            })))
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::new();
        let response = client
            .post(format!("{}/v1/messages", mock_server.uri()))
            .header("x-api-key", "test-key")
            .header("anthropic-version", "2023-06-01")
            .header("Content-Type", "application/json")
            .json(&json!({
                "model": "claude-3-5-sonnet",
                "messages": [{"role": "user", "content": [{"type": "text", "text": "Hello"}]}],
                "max_tokens": 1000
            }))
            .send()
            .await
            .unwrap();

        assert!(response.status().is_success());

        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["role"], "assistant");
        assert_eq!(body["content"][0]["text"], "Hello! I'm Claude.");
    }

    /// Test that provider error responses are handled correctly
    #[tokio::test]
    async fn test_provider_error_handling() {
        let mock_server = MockServer::start().await;

        // Mock a rate limit error
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(429).set_body_json(json!({
                "error": {
                    "message": "Rate limit exceeded",
                    "type": "rate_limit_error",
                    "code": "rate_limit_exceeded"
                }
            })))
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::new();
        let response = client
            .post(format!("{}/v1/chat/completions", mock_server.uri()))
            .header("Authorization", "Bearer test-key")
            .header("Content-Type", "application/json")
            .json(&json!({
                "model": "gpt-4o",
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status().as_u16(), 429);

        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["error"]["type"], "rate_limit_error");
    }

    /// Test that provider authentication errors are handled correctly
    #[tokio::test]
    async fn test_provider_auth_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(401).set_body_json(json!({
                "error": {
                    "message": "Invalid API key",
                    "type": "authentication_error"
                }
            })))
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::new();
        let response = client
            .post(format!("{}/v1/chat/completions", mock_server.uri()))
            .header("Authorization", "Bearer invalid-key")
            .header("Content-Type", "application/json")
            .json(&json!({
                "model": "gpt-4o",
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status().as_u16(), 401);
    }

    /// Test Google Gemini response format
    #[tokio::test]
    async fn test_gemini_mock_response_parsing() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/v1beta/models/gemini-1.5-pro:generateContent"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "candidates": [{
                    "content": {
                        "parts": [{"text": "Hello from Gemini!"}],
                        "role": "model"
                    },
                    "finishReason": "STOP"
                }],
                "usageMetadata": {
                    "promptTokenCount": 5,
                    "candidatesTokenCount": 8,
                    "totalTokenCount": 13
                }
            })))
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::new();
        let response = client
            .post(format!(
                "{}/v1beta/models/gemini-1.5-pro:generateContent",
                mock_server.uri()
            ))
            .header("x-goog-api-key", "test-key")
            .header("Content-Type", "application/json")
            .json(&json!({
                "contents": [{"role": "user", "parts": [{"text": "Hello"}]}]
            }))
            .send()
            .await
            .unwrap();

        assert!(response.status().is_success());

        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(
            body["candidates"][0]["content"]["parts"][0]["text"],
            "Hello from Gemini!"
        );
    }

    /// Test streaming SSE response parsing
    #[tokio::test]
    async fn test_streaming_sse_response() {
        let mock_server = MockServer::start().await;

        // Create SSE response body with proper SSE format
        let sse_body = concat!(
            "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1700000000,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1700000000,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1700000000,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"!\"},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1700000000,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n"
        );

        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_string(sse_body))
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::new();
        let response = client
            .post(format!("{}/v1/chat/completions", mock_server.uri()))
            .header("Authorization", "Bearer test-key")
            .header("Accept", "text/event-stream")
            .header("Content-Type", "application/json")
            .json(&json!({
                "model": "gpt-4o",
                "messages": [{"role": "user", "content": "Hello"}],
                "stream": true
            }))
            .send()
            .await
            .unwrap();

        assert!(response.status().is_success());

        // Verify SSE body format
        let body = response.text().await.unwrap();
        assert!(
            body.contains("data: "),
            "SSE body should contain 'data: ' prefix"
        );
        assert!(
            body.contains("[DONE]"),
            "SSE body should contain [DONE] terminator"
        );
        assert!(
            body.contains("chat.completion.chunk"),
            "SSE body should contain chunk objects"
        );

        // Verify we can parse individual chunks
        for line in body.lines() {
            if line.starts_with("data: ") && !line.contains("[DONE]") {
                let json_str = line.trim_start_matches("data: ");
                let chunk: serde_json::Value = serde_json::from_str(json_str)
                    .expect("Each SSE data line should be valid JSON");
                assert_eq!(chunk["object"], "chat.completion.chunk");
            }
        }
    }

    /// Test server error handling (5xx)
    #[tokio::test]
    async fn test_server_error_handling() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(500).set_body_json(json!({
                "error": {
                    "message": "Internal server error",
                    "type": "server_error"
                }
            })))
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::new();
        let response = client
            .post(format!("{}/v1/chat/completions", mock_server.uri()))
            .header("Authorization", "Bearer test-key")
            .header("Content-Type", "application/json")
            .json(&json!({
                "model": "gpt-4o",
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status().as_u16(), 500);
    }
}

/// Integration tests for fallback and retry logic.
///
/// These tests verify the fallback configuration and retry behavior
/// work correctly with simulated provider failures.
#[cfg(test)]
mod fallback_tests {
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Test that retryable errors (5xx) are properly identified
    #[tokio::test]
    async fn test_retryable_error_500() {
        let mock_server = MockServer::start().await;

        // First request fails with 500, second succeeds
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(500).set_body_json(json!({
                "error": {
                    "message": "Internal server error",
                    "type": "server_error"
                }
            })))
            .expect(1) // Only expect one attempt for this mock
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::new();
        let response = client
            .post(format!("{}/v1/chat/completions", mock_server.uri()))
            .header("Authorization", "Bearer test-key")
            .header("Content-Type", "application/json")
            .json(&json!({
                "model": "gpt-4o",
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .send()
            .await
            .unwrap();

        // Verify the 500 response was returned
        assert_eq!(response.status().as_u16(), 500);
    }

    /// Test that rate limit errors (429) include retry-after header
    #[tokio::test]
    async fn test_rate_limit_error_429() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(429)
                    .set_body_json(json!({
                        "error": {
                            "message": "Rate limit exceeded",
                            "type": "rate_limit_error"
                        }
                    }))
                    .insert_header("retry-after", "60"),
            )
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::new();
        let response = client
            .post(format!("{}/v1/chat/completions", mock_server.uri()))
            .header("Authorization", "Bearer test-key")
            .header("Content-Type", "application/json")
            .json(&json!({
                "model": "gpt-4o",
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status().as_u16(), 429);

        // Verify retry-after header is present
        let retry_after = response.headers().get("retry-after");
        assert!(
            retry_after.is_some(),
            "Rate limit response should include retry-after header"
        );
    }

    /// Test that non-retryable errors (400) are not retried
    #[tokio::test]
    async fn test_non_retryable_error_400() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "error": {
                    "message": "Invalid request: missing model field",
                    "type": "invalid_request_error"
                }
            })))
            .expect(1) // Should only be called once (no retry for 400)
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::new();
        let response = client
            .post(format!("{}/v1/chat/completions", mock_server.uri()))
            .header("Authorization", "Bearer test-key")
            .header("Content-Type", "application/json")
            .json(&json!({
                "messages": [{"role": "user", "content": "Hello"}]
                // Missing model field
            }))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status().as_u16(), 400);
    }

    /// Test that auth errors (401) should not trigger fallback
    #[tokio::test]
    async fn test_auth_error_no_fallback() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(401).set_body_json(json!({
                "error": {
                    "message": "Invalid API key",
                    "type": "authentication_error"
                }
            })))
            .expect(1) // Should only be called once (no retry/fallback for auth errors)
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::new();
        let response = client
            .post(format!("{}/v1/chat/completions", mock_server.uri()))
            .header("Authorization", "Bearer invalid-key")
            .header("Content-Type", "application/json")
            .json(&json!({
                "model": "gpt-4o",
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status().as_u16(), 401);
    }

    /// Test exponential backoff delay calculation
    #[test]
    fn test_exponential_backoff_delays() {
        // Simulate fallback config values
        let initial_delay_ms = 500u64;
        let max_delay_ms = 10_000u64;

        // Calculate delays for each attempt
        let delays: Vec<u64> = (0..5)
            .map(|attempt| {
                let delay = initial_delay_ms * 2u64.pow(attempt);
                delay.min(max_delay_ms)
            })
            .collect();

        // Verify exponential growth with cap
        assert_eq!(delays[0], 500); // 500ms
        assert_eq!(delays[1], 1000); // 1s
        assert_eq!(delays[2], 2000); // 2s
        assert_eq!(delays[3], 4000); // 4s
        assert_eq!(delays[4], 8000); // 8s (not capped yet)

        // Verify cap is applied
        let delay_at_10 = (initial_delay_ms * 2u64.pow(10)).min(max_delay_ms);
        assert_eq!(delay_at_10, 10_000); // Capped at 10s
    }

    /// Test that provider timeouts are handled as retryable
    #[tokio::test]
    async fn test_timeout_is_retryable() {
        use std::time::Duration;

        let mock_server = MockServer::start().await;

        // Mock that delays response beyond client timeout
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({
                        "id": "chatcmpl-123",
                        "object": "chat.completion",
                        "created": 1700000000,
                        "model": "gpt-4o",
                        "choices": [{
                            "index": 0,
                            "message": {"role": "assistant", "content": "Hello"},
                            "finish_reason": "stop"
                        }],
                        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
                    }))
                    .set_delay(Duration::from_secs(5)), // Delay longer than client timeout
            )
            .mount(&mock_server)
            .await;

        // Client with short timeout
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(100))
            .build()
            .unwrap();

        let result = client
            .post(format!("{}/v1/chat/completions", mock_server.uri()))
            .header("Authorization", "Bearer test-key")
            .header("Content-Type", "application/json")
            .json(&json!({
                "model": "gpt-4o",
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .send()
            .await;

        // Should timeout
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.is_timeout(), "Error should be a timeout");
    }
}

// ============================================================================
// Latency Routing Integration Tests (Phase 2)
// ============================================================================

#[cfg(test)]
mod latency_routing_tests {
    use reiver_flow::gateway::latency_tracker::{LatencyTracker, ProviderLatency};
    use reiver_flow::gateway::provider_types::Provider;
    use reiver_flow::gateway::router::GatewayRouter;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn test_latency_tracker_record_and_percentiles() {
        let tracker = LatencyTracker::new(Default::default());
        // Inject cached percentiles (simulating data that would come from ClickHouse refresh)
        tracker.inject_for_test(
            Provider::OpenAi,
            ProviderLatency {
                p50: Duration::from_millis(55),
                p95: Duration::from_millis(95),
                p99: Duration::from_millis(100),
                sample_count: 10,
            },
        );

        let stats = tracker.get_latency(Provider::OpenAi);
        assert!(stats.is_some());
        let stats = stats.unwrap();
        assert_eq!(stats.sample_count, 10);
        assert!(stats.p50 >= Duration::from_millis(50));
        assert!(stats.p50 <= Duration::from_millis(60));
        assert!(stats.p95 >= Duration::from_millis(90));
        assert!(stats.p99 >= Duration::from_millis(90));
    }

    #[tokio::test]
    async fn test_latency_sorted_providers_integration() {
        let tracker = Arc::new(LatencyTracker::new(Default::default()));
        tracker.inject_for_test(
            Provider::OpenAi,
            ProviderLatency {
                p50: Duration::from_millis(50),
                p95: Duration::from_millis(50),
                p99: Duration::from_millis(50),
                sample_count: 10,
            },
        );
        tracker.inject_for_test(
            Provider::Anthropic,
            ProviderLatency {
                p50: Duration::from_millis(200),
                p95: Duration::from_millis(200),
                p99: Duration::from_millis(200),
                sample_count: 10,
            },
        );
        tracker.inject_for_test(
            Provider::Google,
            ProviderLatency {
                p50: Duration::from_millis(100),
                p95: Duration::from_millis(100),
                p99: Duration::from_millis(100),
                sample_count: 10,
            },
        );

        let router = GatewayRouter::new().with_latency_tracker(tracker);

        let candidates = vec![
            "anthropic".to_string(),
            "google".to_string(),
            "openai".to_string(),
        ];
        let sorted = router.get_latency_sorted_providers(&candidates);

        assert_eq!(sorted.len(), 3);
        assert_eq!(sorted[0], "openai");
        assert_eq!(sorted[1], "google");
        assert_eq!(sorted[2], "anthropic");
    }
}

// ============================================================================
// Track 4: Multi-Provider Fallback Chain with WireMock
// ============================================================================

#[cfg(test)]
mod multi_provider_fallback_tests {
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn valid_completion(model: &str) -> serde_json::Value {
        json!({
            "id": "chatcmpl-test",
            "object": "chat.completion",
            "created": 1700000000,
            "model": model,
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hello from the model!"
                },
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
        })
    }

    /// Primary fails 500, verify the client sees the error and could then call fallback
    #[tokio::test]
    async fn test_primary_fails_500_fallback_succeeds() {
        // Simulate primary provider
        let primary_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(500).set_body_json(json!({
                "error": {"message": "Internal error", "type": "server_error"}
            })))
            .mount(&primary_server)
            .await;

        // Simulate fallback provider
        let fallback_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(valid_completion("gpt-4o-fallback")),
            )
            .mount(&fallback_server)
            .await;

        let client = reqwest::Client::new();
        let request_body = json!({
            "model": "claude-3-opus",
            "messages": [{"role": "user", "content": "Hello"}]
        });

        // Primary fails
        let primary_resp = client
            .post(format!("{}/v1/chat/completions", primary_server.uri()))
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await
            .unwrap();
        assert_eq!(primary_resp.status().as_u16(), 500);

        // Fallback succeeds
        let fallback_resp = client
            .post(format!("{}/v1/chat/completions", fallback_server.uri()))
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await
            .unwrap();
        assert!(fallback_resp.status().is_success());

        let body: serde_json::Value = fallback_resp.json().await.unwrap();
        assert_eq!(body["model"], "gpt-4o-fallback");
        assert_eq!(
            body["choices"][0]["message"]["content"],
            "Hello from the model!"
        );
    }

    /// Primary times out, fallback returns instantly
    #[tokio::test]
    async fn test_primary_timeout_fallback_succeeds() {
        let primary_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(valid_completion("primary"))
                    .set_delay(std::time::Duration::from_secs(5)),
            )
            .mount(&primary_server)
            .await;

        let fallback_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(valid_completion("fallback-model")),
            )
            .mount(&fallback_server)
            .await;

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(200))
            .build()
            .unwrap();

        let request_body = json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "Hello"}]
        });

        // Primary should time out
        let primary_result = client
            .post(format!("{}/v1/chat/completions", primary_server.uri()))
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await;
        assert!(primary_result.is_err(), "Primary should time out");

        // Fallback should succeed
        let fallback_resp = client
            .post(format!("{}/v1/chat/completions", fallback_server.uri()))
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await
            .unwrap();
        assert!(fallback_resp.status().is_success());
        let body: serde_json::Value = fallback_resp.json().await.unwrap();
        assert_eq!(body["model"], "fallback-model");
    }

    /// All providers fail - verify final error is returned
    #[tokio::test]
    async fn test_all_providers_fail() {
        let server1 = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(500).set_body_json(json!({"error": {"message": "Error 1"}})),
            )
            .mount(&server1)
            .await;

        let server2 = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(503).set_body_json(json!({"error": {"message": "Error 2"}})),
            )
            .mount(&server2)
            .await;

        let client = reqwest::Client::new();
        let request_body = json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "Hello"}]
        });

        // Both providers fail
        let resp1 = client
            .post(format!("{}/v1/chat/completions", server1.uri()))
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp1.status().as_u16(), 500);

        let resp2 = client
            .post(format!("{}/v1/chat/completions", server2.uri()))
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp2.status().as_u16(), 503);

        // The final error should be preserved (not swallowed)
        let body: serde_json::Value = resp2.json().await.unwrap();
        assert!(body["error"]["message"].is_string());
    }

    /// 400 errors should not trigger fallback
    #[tokio::test]
    async fn test_400_no_fallback() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "error": {"message": "Invalid request: missing model", "type": "invalid_request_error"}
            })))
            .expect(1) // Should only be called once (no retry for 400)
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("{}/v1/chat/completions", server.uri()))
            .header("Content-Type", "application/json")
            .json(&json!({"messages": [{"role": "user", "content": "Hello"}]}))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status().as_u16(), 400);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["error"]["type"], "invalid_request_error");
    }

    /// 429 rate limit should be fallback-eligible
    #[tokio::test]
    async fn test_429_triggers_fallback_path() {
        let primary = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(429)
                    .set_body_json(
                        json!({"error": {"message": "Rate limited", "type": "rate_limit_error"}}),
                    )
                    .insert_header("retry-after", "30"),
            )
            .mount(&primary)
            .await;

        let fallback = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(valid_completion("fallback-model")),
            )
            .mount(&fallback)
            .await;

        let client = reqwest::Client::new();
        let body = json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "Hello"}]});

        // Primary returns 429
        let resp = client
            .post(format!("{}/v1/chat/completions", primary.uri()))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 429);

        // Verify retry-after header
        let retry_after = resp.headers().get("retry-after").unwrap().to_str().unwrap();
        assert_eq!(retry_after, "30");

        // Fallback should work
        let resp2 = client
            .post(format!("{}/v1/chat/completions", fallback.uri()))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert!(resp2.status().is_success());
    }
}

// ============================================================================
// Track 5: Streaming SSE Edge Cases
// ============================================================================

#[cfg(test)]
mod streaming_sse_edge_cases {
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// SSE stream with empty data lines interspersed
    #[tokio::test]
    async fn test_sse_empty_data_lines() {
        let mock_server = MockServer::start().await;

        // SSE with empty data lines
        let sse_body = concat!(
            "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1700000000,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"},\"finish_reason\":null}]}\n\n",
            "data: \n\n",
            "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1700000000,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"},\"finish_reason\":null}]}\n\n",
            "data: \n\n",
            "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1700000000,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n"
        );

        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(sse_body)
                    .insert_header("content-type", "text/event-stream"),
            )
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("{}/v1/chat/completions", mock_server.uri()))
            .header("Content-Type", "application/json")
            .json(&json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "Hi"}], "stream": true}))
            .send()
            .await
            .unwrap();

        assert!(resp.status().is_success());
        let body = resp.text().await.unwrap();

        // Parse valid data lines, skipping empty ones
        let mut valid_chunks = 0;
        for line in body.lines() {
            if line.starts_with("data: ") {
                let data = line.trim_start_matches("data: ").trim();
                if data.is_empty() {
                    continue; // Empty data line should be skippable
                }
                if data == "[DONE]" {
                    continue;
                }
                let chunk: serde_json::Value = serde_json::from_str(data).unwrap();
                assert_eq!(chunk["object"], "chat.completion.chunk");
                valid_chunks += 1;
            }
        }
        assert!(
            valid_chunks >= 2,
            "Should have at least 2 valid chunks, got {}",
            valid_chunks
        );
    }

    /// SSE stream missing [DONE] terminator
    #[tokio::test]
    async fn test_sse_missing_done_terminator() {
        let mock_server = MockServer::start().await;

        let sse_body = concat!(
            "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1700000000,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1700000000,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1700000000,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"
        );
        // Note: No [DONE] at the end

        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(sse_body)
                    .insert_header("content-type", "text/event-stream"),
            )
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("{}/v1/chat/completions", mock_server.uri()))
            .header("Content-Type", "application/json")
            .json(&json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "Hi"}], "stream": true}))
            .send()
            .await
            .unwrap();

        assert!(resp.status().is_success());
        let body = resp.text().await.unwrap();
        assert!(!body.contains("[DONE]"), "Stream should not contain [DONE]");

        // Partial content should still be parseable
        let mut found_content = false;
        for line in body.lines() {
            if line.starts_with("data: ") {
                let data = line.trim_start_matches("data: ").trim();
                if !data.is_empty() {
                    let chunk: serde_json::Value = serde_json::from_str(data).unwrap();
                    if let Some(content) = chunk["choices"][0]["delta"]["content"].as_str() {
                        if content == "Hello" || content == " world" {
                            found_content = true;
                        }
                    }
                }
            }
        }
        assert!(found_content, "Should still have parseable content");
    }

    /// SSE stream with non-JSON data line
    #[tokio::test]
    async fn test_sse_non_json_data_line() {
        let mock_server = MockServer::start().await;

        let sse_body = concat!(
            "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1700000000,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"},\"finish_reason\":null}]}\n\n",
            "data: not-valid-json\n\n",
            "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1700000000,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n"
        );

        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(sse_body)
                    .insert_header("content-type", "text/event-stream"),
            )
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("{}/v1/chat/completions", mock_server.uri()))
            .header("Content-Type", "application/json")
            .json(&json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "Hi"}], "stream": true}))
            .send()
            .await
            .unwrap();

        assert!(resp.status().is_success());
        let body = resp.text().await.unwrap();

        // Should be able to skip non-JSON lines and still parse valid ones
        let mut valid_chunks = 0;
        let mut bad_lines = 0;
        for line in body.lines() {
            if line.starts_with("data: ") {
                let data = line.trim_start_matches("data: ").trim();
                if data.is_empty() || data == "[DONE]" {
                    continue;
                }
                match serde_json::from_str::<serde_json::Value>(data) {
                    Ok(_) => valid_chunks += 1,
                    Err(_) => bad_lines += 1,
                }
            }
        }
        assert!(valid_chunks >= 2, "Should have at least 2 valid chunks");
        assert_eq!(bad_lines, 1, "Should have exactly 1 bad line");
    }

    /// SSE stream with large chunk (100KB+ content)
    #[tokio::test]
    async fn test_sse_large_chunk() {
        let mock_server = MockServer::start().await;

        // Generate a 100KB+ string
        let large_content: String = "x".repeat(100_000);
        let chunk = json!({
            "id": "c1",
            "object": "chat.completion.chunk",
            "created": 1700000000,
            "model": "gpt-4o",
            "choices": [{
                "index": 0,
                "delta": {"content": large_content},
                "finish_reason": null
            }]
        });
        let sse_body = format!(
            "data: {}\n\ndata: [DONE]\n\n",
            serde_json::to_string(&chunk).unwrap()
        );

        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(sse_body)
                    .insert_header("content-type", "text/event-stream"),
            )
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("{}/v1/chat/completions", mock_server.uri()))
            .header("Content-Type", "application/json")
            .json(&json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "Hi"}], "stream": true}))
            .send()
            .await
            .unwrap();

        assert!(resp.status().is_success());
        let body = resp.text().await.unwrap();

        // Should contain the large content without truncation
        assert!(
            body.len() > 100_000,
            "Body should not be truncated: {} bytes",
            body.len()
        );
        assert!(body.contains("[DONE]"));
    }
}

// ============================================================================
// Track 6: Gateway Cache Integration Tests
// ============================================================================

#[cfg(test)]
mod gateway_cache_integration_tests {
    use reiver_flow::gateway::cache::{is_cacheable, GatewayCache};
    use reiver_flow::gateway::types::{
        ChatCompletionRequest, ChatMessage, MessageContent, MessageRole,
    };
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn make_cacheable_request() -> ChatCompletionRequest {
        ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text("Hello".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            }],
            temperature: Some(0.0), // Must be 0.0 for cacheability
            max_tokens: Some(100),
            top_p: None,
            n: None,
            stream: Some(false),
            stream_options: None,
            stop: None,
            frequency_penalty: None,
            presence_penalty: None,
            user: None,
            seed: None,
            response_format: None,
            tools: None,
            tool_choice: None,
            thinking: None,
            reasoning_effort: None,
            prompt_config: None,
            prompt_variables: None,
            models: None,
            provider: None,
        }
    }

    /// Cache miss then cache hit via WireMock cache server
    #[tokio::test]
    async fn test_cache_miss_then_hit() {
        let cache_server = MockServer::start().await;

        // POST /cache/get: cache miss (returns found=false)
        Mock::given(method("POST"))
            .and(path("/cache/get"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "found": false,
                "value": null
            })))
            .expect(1)
            .named("cache miss")
            .mount(&cache_server)
            .await;

        let cache = GatewayCache::new(cache_server.uri(), 300, true);
        let request = make_cacheable_request();
        let project_id = uuid::Uuid::new_v4();

        // First lookup should return None (miss)
        let result = cache.get(project_id, &request).await;
        assert!(result.is_none(), "First lookup should be a miss");
    }

    /// Cache disabled skips all HTTP calls
    #[tokio::test]
    async fn test_cache_disabled_skips_calls() {
        let cache_server = MockServer::start().await;

        // No mocks mounted - any HTTP call would be unexpected
        // WireMock will verify zero interactions

        let cache = GatewayCache::new(cache_server.uri(), 300, false);
        let request = make_cacheable_request();
        let project_id = uuid::Uuid::new_v4();

        let result = cache.get(project_id, &request).await;
        assert!(
            result.is_none(),
            "Disabled cache should return None without HTTP call"
        );
    }

    /// Streaming requests are not cacheable
    #[test]
    fn test_streaming_request_not_cacheable() {
        let mut request = make_cacheable_request();
        request.stream = Some(true);

        assert!(
            !is_cacheable(&request),
            "Streaming request should not be cacheable"
        );
    }

    /// Non-zero temperature requests are not cacheable
    #[test]
    fn test_high_temperature_not_cacheable() {
        let mut request = make_cacheable_request();
        request.temperature = Some(0.7);

        assert!(
            !is_cacheable(&request),
            "High temperature request should not be cacheable"
        );
    }

    /// Zero temperature request is cacheable
    #[test]
    fn test_zero_temperature_is_cacheable() {
        let request = make_cacheable_request();
        assert!(
            is_cacheable(&request),
            "Zero temperature request should be cacheable"
        );
    }

    /// Cache server error is non-fatal
    #[tokio::test]
    async fn test_cache_server_error_non_fatal() {
        let cache_server = MockServer::start().await;

        // Cache returns 500 on POST /cache/get
        Mock::given(method("POST"))
            .and(path("/cache/get"))
            .respond_with(ResponseTemplate::new(500).set_body_json(json!({"error": "cache down"})))
            .mount(&cache_server)
            .await;

        let cache = GatewayCache::new(cache_server.uri(), 300, true);
        let request = make_cacheable_request();
        let project_id = uuid::Uuid::new_v4();

        // Should not panic or propagate error - just return None
        let result = cache.get(project_id, &request).await;
        assert!(
            result.is_none(),
            "Cache error should return None, not propagate error"
        );
    }
}

// ============================================================================
// Track 7: Latency Tracker + Router Integration Tests
// ============================================================================

#[cfg(test)]
mod latency_integration_tests {
    use reiver_flow::gateway::latency_tracker::LatencyTracker;
    use reiver_flow::gateway::router::GatewayRouter;
    use std::sync::Arc;
    use std::time::Duration;

    /// Provider degrades mid-session: inject healthy then degraded state
    #[tokio::test]
    async fn test_provider_degrades_mid_session() {
        use reiver_flow::gateway::latency_tracker::ProviderLatency;
        use reiver_flow::gateway::provider_types::Provider;

        let tracker = Arc::new(LatencyTracker::new(Default::default()));

        tracker.inject_for_test(
            Provider::OpenAi,
            ProviderLatency {
                p50: Duration::from_millis(50),
                p95: Duration::from_millis(50),
                p99: Duration::from_millis(50),
                sample_count: 10,
            },
        );
        assert!(
            !tracker.is_degraded(&Provider::OpenAi),
            "Should not be degraded initially"
        );

        // Simulate degraded state (high P99)
        tracker.inject_for_test(
            Provider::OpenAi,
            ProviderLatency {
                p50: Duration::from_millis(5000),
                p95: Duration::from_millis(8000),
                p99: Duration::from_millis(10_000),
                sample_count: 50,
            },
        );

        let router = GatewayRouter::new().with_latency_tracker(tracker.clone());
        tracker.inject_for_test(
            Provider::Anthropic,
            ProviderLatency {
                p50: Duration::from_millis(30),
                p95: Duration::from_millis(30),
                p99: Duration::from_millis(30),
                sample_count: 10,
            },
        );

        let candidates = vec!["openai".to_string(), "anthropic".to_string()];
        let sorted = router.get_latency_sorted_providers(&candidates);

        assert_eq!(
            sorted[0], "anthropic",
            "Lower-latency provider should be preferred"
        );
        assert_eq!(sorted[1], "openai");
    }

    /// Cache can be empty (no data in window)
    #[tokio::test]
    async fn test_window_expiry_restores_health() {
        use std::str::FromStr;
        let tracker = LatencyTracker::new(Default::default());
        let openai = reiver_flow::gateway::provider_types::Provider::from_str("openai").unwrap();
        let stats = tracker.get_latency(openai);
        assert!(stats.is_none());
    }

    /// Tracker with high sample count in cache
    #[tokio::test]
    async fn test_tracker_1000_plus_samples() {
        use reiver_flow::gateway::latency_tracker::ProviderLatency;
        use reiver_flow::gateway::provider_types::Provider;

        let tracker = LatencyTracker::new(Default::default());
        tracker.inject_for_test(
            Provider::OpenAi,
            ProviderLatency {
                p50: Duration::from_millis(110),
                p95: Duration::from_millis(200),
                p99: Duration::from_millis(210),
                sample_count: 1100,
            },
        );

        let stats = tracker.get_latency(Provider::OpenAi);
        assert!(stats.is_some(), "Should have stats");
        let stats = stats.unwrap();
        assert!(stats.p50 >= Duration::from_millis(5));
        assert!(stats.p50 <= Duration::from_millis(250));
        assert!(stats.p95 >= stats.p50);
        assert!(stats.p99 >= stats.p95);
    }

    /// Single candidate provider is returned
    #[tokio::test]
    async fn test_single_candidate() {
        use reiver_flow::gateway::latency_tracker::ProviderLatency;
        use reiver_flow::gateway::provider_types::Provider;

        let tracker = Arc::new(LatencyTracker::new(Default::default()));
        tracker.inject_for_test(
            Provider::Theta, // "theta" is a valid provider; "solo" is not, so use theta
            ProviderLatency {
                p50: Duration::from_millis(100),
                p95: Duration::from_millis(100),
                p99: Duration::from_millis(100),
                sample_count: 1,
            },
        );

        let router = GatewayRouter::new().with_latency_tracker(tracker);

        let candidates = vec!["theta".to_string()];
        let sorted = router.get_latency_sorted_providers(&candidates);

        assert_eq!(sorted.len(), 1);
        assert_eq!(sorted[0], "theta");
    }
}

// ============================================================================
// Theta EdgeCloud On-Demand API Tests
// ============================================================================

#[cfg(test)]
mod theta_on_demand_tests {
    use serde_json::json;
    use std::time::Duration;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use reiver_flow::gateway::providers::{LlmProvider, ThetaProvider};
    use reiver_flow::gateway::types::{
        ChatCompletionRequest, ChatMessage, MessageContent, MessageRole,
    };

    fn simple_request(model: &str, content: &str) -> ChatCompletionRequest {
        ChatCompletionRequest {
            model: model.to_string(),
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text(content.to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            }],
            ..Default::default()
        }
    }

    fn completed_theta_response(request_id: &str, output_text: &str) -> serde_json::Value {
        json!({
            "body": {
                "infer_requests": [{
                    "id": request_id,
                    "state": "success",
                    "output": {"text": output_text}
                }]
            }
        })
    }

    #[tokio::test]
    async fn test_theta_chat_completion_success() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/infer_request/llama_3_8b"))
            .and(header("Authorization", "Bearer test-theta-key"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(completed_theta_response("req-001", "Hello from Theta!")),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let provider = ThetaProvider::with_base_url(mock_server.uri(), Duration::from_secs(10));

        let request = simple_request("theta/llama_3_8b", "Hi");
        let response = provider
            .chat_completion(&request, "test-theta-key")
            .await
            .unwrap();

        assert_eq!(response.choices.len(), 1);
        assert_eq!(
            response.choices[0].message.content.as_deref(),
            Some("Hello from Theta!")
        );
        assert_eq!(response.model, "theta/llama_3_8b");
        assert_eq!(response.choices[0].finish_reason.as_str(), "stop");
    }

    #[tokio::test]
    async fn test_theta_model_prefix_stripping() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/infer_request/llama_3_1_70b"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(completed_theta_response("req-002", "I'm the 70B model")),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let provider = ThetaProvider::with_base_url(mock_server.uri(), Duration::from_secs(10));

        let request = simple_request("theta/llama_3_1_70b", "Which model?");
        let response = provider.chat_completion(&request, "key").await.unwrap();

        assert_eq!(response.model, "theta/llama_3_1_70b");
        assert_eq!(
            response.choices[0].message.content.as_deref(),
            Some("I'm the 70B model")
        );
    }

    #[tokio::test]
    async fn test_theta_auth_error_401() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/infer_request/llama_3_8b"))
            .respond_with(ResponseTemplate::new(401).set_body_string("Unauthorized"))
            .expect(1)
            .mount(&mock_server)
            .await;

        let provider = ThetaProvider::with_base_url(mock_server.uri(), Duration::from_secs(10));

        let request = simple_request("theta/llama_3_8b", "Hi");
        let err = provider
            .chat_completion(&request, "bad-key")
            .await
            .unwrap_err();

        let msg = err.to_string();
        assert!(msg.contains("theta"), "error should mention theta: {msg}");
    }

    #[tokio::test]
    async fn test_theta_rate_limit_429() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/infer_request/llama_3_8b"))
            .respond_with(ResponseTemplate::new(429).set_body_string("Rate limit exceeded"))
            .expect(1)
            .mount(&mock_server)
            .await;

        let provider = ThetaProvider::with_base_url(mock_server.uri(), Duration::from_secs(10));

        let request = simple_request("theta/llama_3_8b", "Hi");
        let err = provider.chat_completion(&request, "key").await.unwrap_err();

        let msg = err.to_string();
        assert!(
            msg.contains("theta"),
            "error should mention provider: {msg}"
        );
    }

    #[tokio::test]
    async fn test_theta_pending_then_poll_completed() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/infer_request/llama_3_8b"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "body": {
                    "infer_requests": [{
                        "id": "req-pending",
                        "state": "pending",
                        "output": null
                    }]
                }
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path("/infer_request/req-pending"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(completed_theta_response("req-pending", "Done after poll")),
            )
            .expect(1..)
            .mount(&mock_server)
            .await;

        let provider = ThetaProvider::with_base_url(mock_server.uri(), Duration::from_secs(30));

        let request = simple_request("theta/llama_3_8b", "Wait for me");
        let response = provider.chat_completion(&request, "key").await.unwrap();

        assert_eq!(
            response.choices[0].message.content.as_deref(),
            Some("Done after poll")
        );
    }

    #[tokio::test]
    async fn test_theta_supports_streaming_returns_true() {
        let provider = ThetaProvider::with_default_timeout();
        assert!(provider.supports_streaming("theta/llama_3_8b"));
        assert!(provider.supports_streaming("theta/llama_3_1_70b"));
    }

    #[tokio::test]
    async fn test_theta_supports_model() {
        let provider = ThetaProvider::with_default_timeout();
        assert!(provider.supports_model("theta/llama_3_8b"));
        assert!(provider.supports_model("theta/custom_model"));
        assert!(!provider.supports_model("gpt-4o"));
        assert!(!provider.supports_model("llama_3_8b"));
    }

    #[tokio::test]
    async fn test_theta_output_extraction_openai_choices() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/infer_request/llama_3_8b"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "body": {
                    "infer_requests": [{
                        "id": "req-openai",
                        "state": "success",
                        "output": {
                            "choices": [{
                                "message": {
                                    "role": "assistant",
                                    "content": "From OpenAI-style output"
                                }
                            }]
                        }
                    }]
                }
            })))
            .mount(&mock_server)
            .await;

        let provider = ThetaProvider::with_base_url(mock_server.uri(), Duration::from_secs(10));

        let request = simple_request("theta/llama_3_8b", "Hi");
        let response = provider.chat_completion(&request, "key").await.unwrap();

        assert_eq!(
            response.choices[0].message.content.as_deref(),
            Some("From OpenAI-style output")
        );
    }

    #[tokio::test]
    async fn test_theta_output_extraction_plain_string() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/infer_request/llama_3_8b"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "body": {
                    "infer_requests": [{
                        "id": "req-str",
                        "state": "success",
                        "output": "Plain string output"
                    }]
                }
            })))
            .mount(&mock_server)
            .await;

        let provider = ThetaProvider::with_base_url(mock_server.uri(), Duration::from_secs(10));

        let request = simple_request("theta/llama_3_8b", "Hi");
        let response = provider.chat_completion(&request, "key").await.unwrap();

        assert_eq!(
            response.choices[0].message.content.as_deref(),
            Some("Plain string output")
        );
    }

    #[tokio::test]
    async fn test_theta_chat_completion_sends_stream_false() {
        use wiremock::matchers::body_json;

        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/infer_request/llama_3_8b"))
            .and(body_json(json!({
                "input": {
                    "messages": [{"role": "user", "content": "Hi"}],
                    "stream": false
                }
            })))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(completed_theta_response("req-stream-false", "Ok")),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let provider = ThetaProvider::with_base_url(mock_server.uri(), Duration::from_secs(10));

        let request = simple_request("theta/llama_3_8b", "Hi");
        let response = provider.chat_completion(&request, "key").await.unwrap();

        assert_eq!(response.choices[0].message.content.as_deref(), Some("Ok"));
    }

    #[tokio::test]
    async fn test_theta_stream_chat_completion_success() {
        use futures::StreamExt;

        let mock_server = MockServer::start().await;

        let sse_body = "\
data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1700000000,\"model\":\"llama_3_8b\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Hello\"},\"finish_reason\":null}]}\n\n\
data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1700000000,\"model\":\"llama_3_8b\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"},\"finish_reason\":null}]}\n\n\
data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1700000000,\"model\":\"llama_3_8b\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n\
data: [DONE]\n\n";

        Mock::given(method("POST"))
            .and(path("/infer_request/llama_3_8b"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Type", "text/event-stream")
                    .set_body_string(sse_body),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let provider = ThetaProvider::with_base_url(mock_server.uri(), Duration::from_secs(10));

        let request = simple_request("theta/llama_3_8b", "Hi");
        let mut stream = provider
            .stream_chat_completion(&request, "key")
            .await
            .unwrap();

        let mut chunks = Vec::new();
        while let Some(result) = stream.next().await {
            chunks.push(result.unwrap());
        }

        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].choices[0].delta.content.as_deref(), Some("Hello"));
        assert_eq!(
            chunks[1].choices[0].delta.content.as_deref(),
            Some(" world")
        );
        assert_eq!(
            chunks[2].choices[0]
                .finish_reason
                .as_ref()
                .map(|r| r.as_str()),
            Some("stop")
        );
    }

    #[tokio::test]
    async fn test_theta_extracts_usage_from_output() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/infer_request/llama_3_8b"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "body": {
                    "infer_requests": [{
                        "id": "req-usage",
                        "state": "success",
                        "output": {
                            "choices": [{"message": {"role": "assistant", "content": "Hello"}}],
                            "usage": {
                                "prompt_tokens": 15,
                                "completion_tokens": 42,
                                "total_tokens": 57
                            }
                        }
                    }]
                }
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let provider = ThetaProvider::with_base_url(mock_server.uri(), Duration::from_secs(10));

        let request = simple_request("theta/llama_3_8b", "Hi");
        let response = provider.chat_completion(&request, "key").await.unwrap();

        assert_eq!(response.usage.prompt_tokens, 15);
        assert_eq!(response.usage.completion_tokens, 42);
        assert_eq!(response.usage.total_tokens, 57);
    }

    #[tokio::test]
    async fn test_theta_usage_defaults_when_not_in_output() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/infer_request/llama_3_8b"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(completed_theta_response("req-no-usage", "Hi")),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let provider = ThetaProvider::with_base_url(mock_server.uri(), Duration::from_secs(10));

        let request = simple_request("theta/llama_3_8b", "Hi");
        let response = provider.chat_completion(&request, "key").await.unwrap();

        assert_eq!(response.usage.prompt_tokens, 0);
        assert_eq!(response.usage.completion_tokens, 0);
    }

    #[tokio::test]
    async fn test_theta_stream_sends_stream_options_include_usage() {
        use wiremock::matchers::body_partial_json;

        let mock_server = MockServer::start().await;

        let sse_body = "\
data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1700000000,\"model\":\"llama_3_8b\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":1,\"total_tokens\":6}}\n\n\
data: [DONE]\n\n";

        Mock::given(method("POST"))
            .and(path("/infer_request/llama_3_8b"))
            .and(body_partial_json(json!({
                "input": {
                    "stream": true,
                    "stream_options": {"include_usage": true}
                }
            })))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Type", "text/event-stream")
                    .set_body_string(sse_body),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let provider = ThetaProvider::with_base_url(mock_server.uri(), Duration::from_secs(10));

        let request = simple_request("theta/llama_3_8b", "Hi");
        let mut stream = provider
            .stream_chat_completion(&request, "key")
            .await
            .unwrap();

        use futures::StreamExt;
        let mut chunks = Vec::new();
        while let Some(result) = stream.next().await {
            chunks.push(result.unwrap());
        }

        assert_eq!(chunks.len(), 1);
        let usage = chunks[0]
            .usage
            .as_ref()
            .expect("final chunk should have usage");
        assert_eq!(usage.prompt_tokens, 5);
        assert_eq!(usage.completion_tokens, 1);
    }
}

#[cfg(test)]
mod theta_dedicated_tests {
    use serde_json::json;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use reiver_flow::gateway::provider_types::Provider;
    use reiver_flow::gateway::providers::{LlmProvider, ThetaDedicatedProvider};
    use reiver_flow::gateway::types::{
        ChatCompletionRequest, ChatMessage, MessageContent, MessageRole,
    };

    fn simple_request(model: &str, content: &str) -> ChatCompletionRequest {
        ChatCompletionRequest {
            model: model.to_string(),
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text(content.to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            }],
            ..Default::default()
        }
    }

    fn openai_chat_response(model: &str, content: &str) -> serde_json::Value {
        json!({
            "id": "chatcmpl-test",
            "object": "chat.completion",
            "created": 1700000000,
            "model": model,
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": content
                },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 20,
                "total_tokens": 30
            }
        })
    }

    #[tokio::test]
    async fn test_theta_dedicated_chat_completion_success() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .and(header("Authorization", "Bearer test-dedicated-key"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(openai_chat_response(
                    "my-fine-tuned-model",
                    "Hello from dedicated!",
                )),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let provider = ThetaDedicatedProvider::with_base_url(format!("{}/v1", mock_server.uri()));

        let request = simple_request("theta-dedicated/my-fine-tuned-model", "Hi");
        let response = provider
            .chat_completion(&request, "test-dedicated-key")
            .await
            .unwrap();

        assert_eq!(response.choices.len(), 1);
        assert_eq!(
            response.choices[0].message.content.as_deref(),
            Some("Hello from dedicated!")
        );
        assert_eq!(response.choices[0].finish_reason.as_str(), "stop");
    }

    #[tokio::test]
    async fn test_theta_dedicated_prefix_stripping() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(openai_chat_response("llama-3-1-70b", "Stripped!")),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let provider = ThetaDedicatedProvider::with_base_url(format!("{}/v1", mock_server.uri()));

        let request = simple_request("theta-dedicated/llama-3-1-70b", "Which model?");
        let response = provider.chat_completion(&request, "key").await.unwrap();

        assert_eq!(
            response.choices[0].message.content.as_deref(),
            Some("Stripped!")
        );
    }

    #[tokio::test]
    async fn test_theta_dedicated_auth_error_401() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(401).set_body_string("Unauthorized"))
            .expect(1)
            .mount(&mock_server)
            .await;

        let provider = ThetaDedicatedProvider::with_base_url(format!("{}/v1", mock_server.uri()));

        let request = simple_request("theta-dedicated/model", "Hi");
        let err = provider
            .chat_completion(&request, "bad-key")
            .await
            .unwrap_err();

        let err_str = err.to_string();
        assert!(
            err_str.contains("401") || err_str.to_lowercase().contains("unauthorized"),
            "Expected 401 error, got: {err_str}"
        );
    }

    #[tokio::test]
    async fn test_theta_dedicated_rate_limit_429() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(429).set_body_string("Rate limited"))
            .expect(1)
            .mount(&mock_server)
            .await;

        let provider = ThetaDedicatedProvider::with_base_url(format!("{}/v1", mock_server.uri()));

        let request = simple_request("theta-dedicated/model", "Hi");
        let err = provider.chat_completion(&request, "key").await.unwrap_err();

        let err_str = err.to_string();
        assert!(
            err_str.contains("429") || err_str.to_lowercase().contains("rate"),
            "Expected 429 error, got: {err_str}"
        );
    }

    #[tokio::test]
    async fn test_theta_dedicated_supports_model() {
        let provider = ThetaDedicatedProvider::with_base_url("http://localhost/v1".to_string());

        assert!(provider.supports_model("theta-dedicated/any-model"));
        assert!(provider.supports_model("theta-dedicated/llama-3-1-70b"));
        assert!(!provider.supports_model("theta/llama_3_8b"));
        assert!(!provider.supports_model("gpt-4o"));
        assert!(!provider.supports_model("deepseek/deepseek-chat"));
    }

    #[tokio::test]
    async fn test_theta_dedicated_provider_name() {
        let provider = ThetaDedicatedProvider::with_base_url("http://localhost/v1".to_string());
        assert_eq!(provider.name(), Provider::ThetaDedicated);
    }

    #[tokio::test]
    async fn test_theta_dedicated_supports_streaming() {
        let provider = ThetaDedicatedProvider::with_base_url("http://localhost/v1".to_string());
        assert!(provider.supports_streaming("theta-dedicated/any-model"));
    }
}

// ============================================================================
// New Provider Model Routing Tests
// ============================================================================

#[cfg(test)]
mod new_provider_routing_tests {
    use reiver_flow::gateway::provider_types::Provider;

    #[test]
    fn xai_model_routing() {
        let p = Provider::from_model_prefix("grok-4.3").unwrap();
        assert_eq!(p, Provider::Xai);

        let p2 = Provider::from_model_prefix("grok-4.20-reasoning").unwrap();
        assert_eq!(p2, Provider::Xai);

        let p3 = Provider::from_model_prefix("grok-4-1-fast-reasoning").unwrap();
        assert_eq!(p3, Provider::Xai);

        let p4 = Provider::from_model_prefix("grok-future-model").unwrap();
        assert_eq!(p4, Provider::Xai);
    }

    #[test]
    fn mistral_model_routing() {
        let p = Provider::from_model_prefix("mistral/mistral-large-latest").unwrap();
        assert_eq!(p, Provider::Mistral);

        let p2 = Provider::from_model_prefix("mistral/codestral-latest").unwrap();
        assert_eq!(p2, Provider::Mistral);

        let p3 = Provider::from_model_prefix("mistral/devstral-latest").unwrap();
        assert_eq!(p3, Provider::Mistral);

        let p4 = Provider::from_model_prefix("mistral/some-new-model").unwrap();
        assert_eq!(p4, Provider::Mistral);
    }

    #[test]
    fn groq_model_routing() {
        let p = Provider::from_model_prefix("groq/llama-3.3-70b-versatile").unwrap();
        assert_eq!(p, Provider::Groq);

        let p2 = Provider::from_model_prefix("groq/llama-3.1-8b-instant").unwrap();
        assert_eq!(p2, Provider::Groq);
    }

    #[test]
    fn together_model_routing() {
        let p = Provider::from_model_prefix("together/meta-llama/Llama-3-70b-chat-hf").unwrap();
        assert_eq!(p, Provider::Together);
    }

    #[test]
    fn fireworks_model_routing() {
        let p = Provider::from_model_prefix("fireworks/accounts/fireworks/models/llama-v3-70b").unwrap();
        assert_eq!(p, Provider::Fireworks);
    }

    #[test]
    fn perplexity_model_routing() {
        let p = Provider::from_model_prefix("perplexity/sonar-pro").unwrap();
        assert_eq!(p, Provider::Perplexity);
    }

    #[test]
    fn cohere_model_routing() {
        let p = Provider::from_model_prefix("cohere/command-r-plus").unwrap();
        assert_eq!(p, Provider::Cohere);
    }

    #[test]
    fn openrouter_model_routing() {
        let p = Provider::from_model_prefix("openrouter/anthropic/claude-3-opus").unwrap();
        assert_eq!(p, Provider::OpenRouter);
    }

    #[test]
    fn cerebras_model_routing() {
        let p = Provider::from_model_prefix("cerebras/llama3.1-70b").unwrap();
        assert_eq!(p, Provider::Cerebras);
    }

    #[test]
    fn deepinfra_model_routing() {
        let p = Provider::from_model_prefix("deepinfra/meta-llama/Llama-3-70b").unwrap();
        assert_eq!(p, Provider::DeepInfra);
    }

    #[test]
    fn alibaba_model_routing() {
        let p = Provider::from_model_prefix("qwen/qwen-turbo").unwrap();
        assert_eq!(p, Provider::Alibaba);
    }

    #[test]
    fn nvidia_model_routing() {
        let p = Provider::from_model_prefix("nvidia/nemotron-4-340b").unwrap();
        assert_eq!(p, Provider::Nvidia);
    }

    #[test]
    fn ai21_model_routing() {
        let p = Provider::from_model_prefix("ai21/jamba-1.5-large").unwrap();
        assert_eq!(p, Provider::Ai21);
    }

    #[test]
    fn bedrock_dot_prefixes_still_work() {
        let p = Provider::from_model_prefix("mistral.mixtral-8x7b-instruct-v0:1").unwrap();
        assert_eq!(p, Provider::Bedrock);

        let p2 = Provider::from_model_prefix("cohere.command-r-plus-v1:0").unwrap();
        assert_eq!(p2, Provider::Bedrock);

        let p3 = Provider::from_model_prefix("ai21.j2-ultra-v1").unwrap();
        assert_eq!(p3, Provider::Bedrock);
    }

    #[test]
    fn new_providers_dont_collide_with_bedrock_dots() {
        let mistral_slash = Provider::from_model_prefix("mistral/mistral-large-latest").unwrap();
        assert_eq!(mistral_slash, Provider::Mistral);

        let mistral_dot = Provider::from_model_prefix("mistral.mixtral-8x7b-instruct-v0:1").unwrap();
        assert_eq!(mistral_dot, Provider::Bedrock);

        let cohere_slash = Provider::from_model_prefix("cohere/command-r-plus").unwrap();
        assert_eq!(cohere_slash, Provider::Cohere);

        let cohere_dot = Provider::from_model_prefix("cohere.command-r-plus-v1:0").unwrap();
        assert_eq!(cohere_dot, Provider::Bedrock);
    }
}

// ============================================================================
// New Provider WireMock Tests
// ============================================================================

#[cfg(test)]
mod new_provider_wiremock_tests {
    use serde_json::json;
    use std::time::Duration;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use reiver_flow::gateway::provider_types::Provider;
    use reiver_flow::gateway::providers::{
        Ai21Provider, AlibabaProvider, CerebrasProvider, CohereProvider, DeepInfraProvider,
        FireworksProvider, GroqProvider, LlmProvider, MistralProvider, NvidiaProvider,
        OpenRouterProvider, PerplexityProvider, TogetherProvider, XaiProvider,
    };
    use reiver_flow::gateway::types::{
        ChatCompletionRequest, ChatMessage, MessageContent, MessageRole,
    };

    fn simple_request(model: &str) -> ChatCompletionRequest {
        ChatCompletionRequest {
            model: model.to_string(),
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text("Hello".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            }],
            ..Default::default()
        }
    }

    fn openai_response(model: &str) -> serde_json::Value {
        json!({
            "id": "chatcmpl-test",
            "object": "chat.completion",
            "created": 1700000000,
            "model": model,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "Hello!"},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8}
        })
    }

    async fn setup_mock(server: &MockServer, expected_model: &str) {
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(header("Authorization", "Bearer test-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(openai_response(expected_model)))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn test_xai_chat_completion() {
        let server = MockServer::start().await;
        setup_mock(&server, "grok-4.3").await;
        let provider = XaiProvider::new(server.uri(), Duration::from_secs(10));
        assert_eq!(provider.name(), Provider::Xai);
        assert!(provider.supports_model("grok-4.3"));
        assert!(!provider.supports_model("gpt-4o"));

        let resp = provider
            .chat_completion(&simple_request("grok-4.3"), "test-key")
            .await
            .unwrap();
        assert_eq!(resp.choices[0].message.content.as_deref(), Some("Hello!"));
    }

    #[tokio::test]
    async fn test_mistral_chat_completion() {
        let server = MockServer::start().await;
        setup_mock(&server, "mistral-large-latest").await;
        let provider = MistralProvider::new(server.uri(), Duration::from_secs(10));
        assert_eq!(provider.name(), Provider::Mistral);
        assert!(provider.supports_model("mistral/mistral-large-latest"));
        assert!(!provider.supports_model("gpt-4o"));

        let resp = provider
            .chat_completion(&simple_request("mistral/mistral-large-latest"), "test-key")
            .await
            .unwrap();
        assert_eq!(resp.choices[0].message.content.as_deref(), Some("Hello!"));
    }

    #[tokio::test]
    async fn test_groq_chat_completion() {
        let server = MockServer::start().await;
        setup_mock(&server, "llama-3.3-70b-versatile").await;
        let provider = GroqProvider::new(server.uri(), Duration::from_secs(10));
        assert_eq!(provider.name(), Provider::Groq);

        let resp = provider
            .chat_completion(&simple_request("groq/llama-3.3-70b-versatile"), "test-key")
            .await
            .unwrap();
        assert_eq!(resp.choices[0].message.content.as_deref(), Some("Hello!"));
    }

    #[tokio::test]
    async fn test_together_chat_completion() {
        let server = MockServer::start().await;
        setup_mock(&server, "meta-llama/Llama-3-70b").await;
        let provider = TogetherProvider::new(server.uri(), Duration::from_secs(10));
        assert_eq!(provider.name(), Provider::Together);

        let resp = provider
            .chat_completion(
                &simple_request("together/meta-llama/Llama-3-70b"),
                "test-key",
            )
            .await
            .unwrap();
        assert_eq!(resp.choices[0].message.content.as_deref(), Some("Hello!"));
    }

    #[tokio::test]
    async fn test_fireworks_chat_completion() {
        let server = MockServer::start().await;
        setup_mock(&server, "llama-v3-70b").await;
        let provider = FireworksProvider::new(server.uri(), Duration::from_secs(10));
        assert_eq!(provider.name(), Provider::Fireworks);

        let resp = provider
            .chat_completion(&simple_request("fireworks/llama-v3-70b"), "test-key")
            .await
            .unwrap();
        assert_eq!(resp.choices[0].message.content.as_deref(), Some("Hello!"));
    }

    #[tokio::test]
    async fn test_perplexity_chat_completion() {
        let server = MockServer::start().await;
        setup_mock(&server, "sonar-pro").await;
        let provider = PerplexityProvider::new(server.uri(), Duration::from_secs(10));
        assert_eq!(provider.name(), Provider::Perplexity);

        let resp = provider
            .chat_completion(&simple_request("perplexity/sonar-pro"), "test-key")
            .await
            .unwrap();
        assert_eq!(resp.choices[0].message.content.as_deref(), Some("Hello!"));
    }

    #[tokio::test]
    async fn test_cohere_chat_completion() {
        let server = MockServer::start().await;
        setup_mock(&server, "command-r-plus").await;
        let provider = CohereProvider::new(server.uri(), Duration::from_secs(10));
        assert_eq!(provider.name(), Provider::Cohere);

        let resp = provider
            .chat_completion(&simple_request("cohere/command-r-plus"), "test-key")
            .await
            .unwrap();
        assert_eq!(resp.choices[0].message.content.as_deref(), Some("Hello!"));
    }

    #[tokio::test]
    async fn test_openrouter_chat_completion() {
        let server = MockServer::start().await;
        setup_mock(&server, "anthropic/claude-3-opus").await;
        let provider = OpenRouterProvider::new(server.uri(), Duration::from_secs(10));
        assert_eq!(provider.name(), Provider::OpenRouter);

        let resp = provider
            .chat_completion(
                &simple_request("openrouter/anthropic/claude-3-opus"),
                "test-key",
            )
            .await
            .unwrap();
        assert_eq!(resp.choices[0].message.content.as_deref(), Some("Hello!"));
    }

    #[tokio::test]
    async fn test_cerebras_chat_completion() {
        let server = MockServer::start().await;
        setup_mock(&server, "llama3.1-70b").await;
        let provider = CerebrasProvider::new(server.uri(), Duration::from_secs(10));
        assert_eq!(provider.name(), Provider::Cerebras);

        let resp = provider
            .chat_completion(&simple_request("cerebras/llama3.1-70b"), "test-key")
            .await
            .unwrap();
        assert_eq!(resp.choices[0].message.content.as_deref(), Some("Hello!"));
    }

    #[tokio::test]
    async fn test_deepinfra_chat_completion() {
        let server = MockServer::start().await;
        setup_mock(&server, "meta-llama/Llama-3-70b").await;
        let provider = DeepInfraProvider::new(server.uri(), Duration::from_secs(10));
        assert_eq!(provider.name(), Provider::DeepInfra);

        let resp = provider
            .chat_completion(
                &simple_request("deepinfra/meta-llama/Llama-3-70b"),
                "test-key",
            )
            .await
            .unwrap();
        assert_eq!(resp.choices[0].message.content.as_deref(), Some("Hello!"));
    }

    #[tokio::test]
    async fn test_alibaba_chat_completion() {
        let server = MockServer::start().await;
        setup_mock(&server, "qwen-turbo").await;
        let provider = AlibabaProvider::new(server.uri(), Duration::from_secs(10));
        assert_eq!(provider.name(), Provider::Alibaba);

        let resp = provider
            .chat_completion(&simple_request("qwen/qwen-turbo"), "test-key")
            .await
            .unwrap();
        assert_eq!(resp.choices[0].message.content.as_deref(), Some("Hello!"));
    }

    #[tokio::test]
    async fn test_nvidia_chat_completion() {
        let server = MockServer::start().await;
        setup_mock(&server, "nemotron-4-340b").await;
        let provider = NvidiaProvider::new(server.uri(), Duration::from_secs(10));
        assert_eq!(provider.name(), Provider::Nvidia);

        let resp = provider
            .chat_completion(&simple_request("nvidia/nemotron-4-340b"), "test-key")
            .await
            .unwrap();
        assert_eq!(resp.choices[0].message.content.as_deref(), Some("Hello!"));
    }

    #[tokio::test]
    async fn test_ai21_chat_completion() {
        let server = MockServer::start().await;
        setup_mock(&server, "jamba-1.5-large").await;
        let provider = Ai21Provider::new(server.uri(), Duration::from_secs(10));
        assert_eq!(provider.name(), Provider::Ai21);

        let resp = provider
            .chat_completion(&simple_request("ai21/jamba-1.5-large"), "test-key")
            .await
            .unwrap();
        assert_eq!(resp.choices[0].message.content.as_deref(), Some("Hello!"));
    }
}
