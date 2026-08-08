//! AI Gateway Module
//!
//! Provides an OpenAI-compatible API gateway that routes requests to multiple
//! LLM providers (OpenAI, Anthropic, Google Gemini, AWS Bedrock).
//!
//! # Overview
//!
//! The gateway accepts requests in OpenAI's chat completion format and routes
//! them to the appropriate provider based on the model name:
//!
//! - `gpt-*`, `o1-*` → OpenAI
//! - `claude-*` → Anthropic
//! - `gemini-*` → Google
//! - `bedrock/*`, `anthropic.*`, `amazon.*`, `meta.*` → AWS Bedrock
//!
//! # User Experience
//!
//! Users can use the standard OpenAI SDK with a custom base URL:
//!
//! ```python
//! from openai import OpenAI
//!
//! client = OpenAI(
//!     api_key="fx_project_key",
//!     base_url="https://reiver.ai/api/gateway/v1"
//! )
//!
//! # Works with ANY model - gateway handles routing
//! response = client.chat.completions.create(
//!     model="claude-3-opus",  # or gpt-4o, gemini-pro, etc.
//!     messages=[{"role": "user", "content": "Hello"}]
//! )
//! ```
//!
//! # Feature Support Matrix
//!
//! | Feature                  | OpenAI | Anthropic | Google | Bedrock |
//! |--------------------------|--------|-----------|--------|---------|
//! | Chat completions         | ✅     | ✅        | ✅     | ✅      |
//! | Streaming                | ✅     | ✅        | ✅     | ✅      |
//! | System messages          | ✅     | ✅        | ✅     | ✅      |
//! | Function calling (basic) | ✅     | ⚠️ Partial| ❌     | ❌      |
//! | Tool use responses       | ✅     | ❌        | ❌     | ❌      |
//! | JSON mode                | ✅     | ✅        | ✅     | ✅      |
//! | Image input              | ✅     | ✅        | ✅     | ⚠️      |
//!
//! ## Tool/Function Calling Limitations
//!
//! **Current Status: Experimental**
//!
//! Tool/function calling support varies by provider:
//!
//! - **OpenAI**: Full support for `tools` and `tool_calls`. Requests and responses
//!   pass through directly without translation.
//!
//! - **Anthropic**: Partial support. The gateway converts Anthropic's `tool_use`
//!   response blocks to OpenAI's `tool_calls` format. However, sending `tool_result`
//!   messages back (for multi-turn tool conversations) is not yet implemented.
//!   Single-turn tool calls work, but iterative tool use loops do not.
//!
//! - **Google Gemini**: Function calling not implemented. The gateway accepts the
//!   `tools` parameter but does not translate it to Gemini's function declaration
//!   format. Use the native Gemini SDK for function calling.
//!
//! - **AWS Bedrock**: Tool calling not implemented. Bedrock's Converse API supports
//!   tool use, but translation between OpenAI and Bedrock tool formats is not yet
//!   available. Use the AWS SDK directly for tool use scenarios.
//!
//! **Recommendation**: For production tool/function calling workflows, use the
//! native provider SDKs. The gateway's tool calling support should only be used
//! for simple, single-turn tool calls with OpenAI models.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                   Reiver AI Gateway                        │
//! ├─────────────────────────────────────────────────────────────┤
//! │  POST /v1/chat/completions (OpenAI-compatible)              │
//! │                                                              │
//! │  ┌───────────────────────────────────────────────────────┐  │
//! │  │                   GatewayRouter                        │  │
//! │  │                                                        │  │
//! │  │  model="gpt-4o"        → OpenAiProvider               │  │
//! │  │  model="claude-3-opus" → AnthropicProvider            │  │
//! │  │  model="gemini-pro"    → GoogleProvider               │  │
//! │  │  model="bedrock/..."   → BedrockProvider              │  │
//! │  └───────────────────────────────────────────────────────┘  │
//! │                                                              │
//! │  Each provider translates OpenAI format ↔ provider format   │
//! │  Observability spans captured automatically                 │
//! └─────────────────────────────────────────────────────────────┘
//! ```

pub mod cache;
pub mod circuit_breaker;
pub mod global_model_stats;
pub mod model_catalog_cache;
pub mod domain_types;
pub mod embedding_types;
pub mod error;
pub mod evaluator;
pub mod fallback;
pub mod guardrails;
pub mod latency_sync;
pub mod latency_tracker;
pub mod llm_request_buffer;
pub mod observability;
pub mod otel_publisher;
pub mod prompt_resolver;
pub mod prompt_store;
pub mod prompt_variable_def;
pub mod provider_manager;
pub mod provider_types;
pub mod providers;
pub mod router;
pub mod routes;
pub mod session_eval_consumer;
pub mod session_evaluator;
pub mod stream_processor;
pub mod types;

// Re-export commonly used items
pub use domain_types::{
    AllocationType, ComparisonStatus, GuardrailRule, OutputFailureAction, RolloutStageStatus,
    RolloutStatus, TestStatus,
};
pub use error::GatewayError;
pub use prompt_variable_def::VariableDefinition;
pub use provider_manager::ProviderManager;
pub use provider_types::Provider;
pub use providers::ChatCompletionStream;
pub use router::GatewayRouter;
pub use routes::create_gateway_router;
pub use types::{ChatCompletionChunk, ChatCompletionRequest, ChatCompletionResponse};
