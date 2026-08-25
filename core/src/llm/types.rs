//! GenAI Semantic Conventions and LLM Data Types
//!
//! Based on OpenTelemetry Semantic Conventions for GenAI:
//! https://opentelemetry.io/docs/specs/semconv/gen-ai/

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use std::collections::HashMap;
use std::str::FromStr;

/// OpenTelemetry GenAI semantic convention attribute names.
///
/// Aligned with <https://opentelemetry.io/docs/specs/semconv/registry/attributes/gen-ai/>.
/// Where the spec renamed an attribute, we define both the current name and the
/// deprecated alias so the SDK processor can accept either.
pub mod genai_attributes {
    // Provider / system
    pub const PROVIDER_NAME: &str = "gen_ai.provider.name";
    /// Deprecated by the spec -- kept for backwards-compat ingestion.
    pub const SYSTEM_DEPRECATED: &str = "gen_ai.system";
    pub const OPERATION_NAME: &str = "gen_ai.operation.name";

    // Request attributes
    pub const REQUEST_MODEL: &str = "gen_ai.request.model";
    pub const REQUEST_MAX_TOKENS: &str = "gen_ai.request.max_tokens";
    pub const REQUEST_TEMPERATURE: &str = "gen_ai.request.temperature";
    pub const REQUEST_TOP_P: &str = "gen_ai.request.top_p";
    pub const REQUEST_TOP_K: &str = "gen_ai.request.top_k";
    pub const REQUEST_STOP_SEQUENCES: &str = "gen_ai.request.stop_sequences";
    pub const REQUEST_FREQUENCY_PENALTY: &str = "gen_ai.request.frequency_penalty";
    pub const REQUEST_PRESENCE_PENALTY: &str = "gen_ai.request.presence_penalty";

    // Response attributes
    pub const RESPONSE_MODEL: &str = "gen_ai.response.model";
    pub const RESPONSE_ID: &str = "gen_ai.response.id";
    pub const RESPONSE_FINISH_REASONS: &str = "gen_ai.response.finish_reasons";

    // Usage attributes
    pub const USAGE_INPUT_TOKENS: &str = "gen_ai.usage.input_tokens";
    pub const USAGE_OUTPUT_TOKENS: &str = "gen_ai.usage.output_tokens";

    // Error attributes (custom extension -- no spec equivalent)
    pub const ERROR_TYPE: &str = "gen_ai.error.type";

    // Conversation / session tracking
    pub const CONVERSATION_ID: &str = "gen_ai.conversation.id";
    /// Deprecated alias -- kept for backwards-compat ingestion.
    pub const SESSION_ID_DEPRECATED: &str = "gen_ai.session.id";
    /// Custom extension (no spec equivalent).
    pub const SESSION_NAME: &str = "gen_ai.session.name";
    /// Custom extension (no spec equivalent).
    pub const USER_ID: &str = "gen_ai.user.id";

    // Performance attributes (custom extension)
    pub const TIME_TO_FIRST_TOKEN_MS: &str = "gen_ai.performance.time_to_first_token_ms";

    // Cache attributes -- spec names
    pub const CACHE_READ_INPUT_TOKENS: &str = "gen_ai.usage.cache_read.input_tokens";
    pub const CACHE_CREATION_INPUT_TOKENS: &str = "gen_ai.usage.cache_creation.input_tokens";
    /// Deprecated aliases -- kept for backwards-compat ingestion.
    pub const CACHE_READ_TOKENS_DEPRECATED: &str = "gen_ai.usage.cache_read_tokens";
    pub const CACHE_WRITE_TOKENS_DEPRECATED: &str = "gen_ai.usage.cache_write_tokens";

    // Content attributes (optional, can be disabled for privacy) -- spec names
    pub const INPUT_MESSAGES: &str = "gen_ai.input.messages";
    pub const OUTPUT_MESSAGES: &str = "gen_ai.output.messages";
    /// Deprecated aliases -- kept for backwards-compat ingestion.
    pub const REQUEST_MESSAGES_DEPRECATED: &str = "gen_ai.request.messages";
    pub const RESPONSE_CONTENT_DEPRECATED: &str = "gen_ai.response.content";
}

/// Row from the `model_catalog` table, mirroring the OpenRouter API shape.
/// Nested API objects (`pricing`, `architecture`, `top_provider`, etc.) are
/// stored as raw JSONB.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ModelCatalogRow {
    pub id: String,
    pub name: String,
    pub created: Option<i64>,
    pub context_length: Option<i32>,
    pub pricing: serde_json::Value,
    pub architecture: serde_json::Value,
    pub top_provider: serde_json::Value,
    pub supported_parameters: serde_json::Value,
    pub provider_slug: String,
    pub model_slug: String,
    pub enabled: bool,
}

/// Simplified pricing lookup structure for the in-memory cache.
///
/// Contains per-token costs for fast cost calculations.
/// This struct is used by [`CostCalculator`] for lock-free pricing lookups.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelPricing {
    /// The AI provider (e.g., "openai", "anthropic", "google").
    pub provider: String,

    /// The model name (e.g., "gpt-4o", "claude-3-5-sonnet").
    pub model: String,

    /// Cost per input token in USD.
    pub input_cost_per_token: Decimal,

    /// Cost per output token in USD.
    pub output_cost_per_token: Decimal,

    /// Cost per cache read token in USD (for providers with prompt caching).
    pub cache_read_cost_per_token: Decimal,

    /// Cost per cache write token in USD (for providers with prompt caching).
    pub cache_write_cost_per_token: Decimal,
}

/// Parse a per-token price string from the OpenRouter JSONB pricing object.
fn parse_pricing_field(pricing: &serde_json::Value, key: &str) -> Decimal {
    pricing
        .get(key)
        .and_then(|v| v.as_str())
        .and_then(|s| Decimal::from_str(s).ok())
        .unwrap_or(Decimal::ZERO)
}

impl From<ModelCatalogRow> for ModelPricing {
    fn from(row: ModelCatalogRow) -> Self {
        Self {
            provider: row.provider_slug,
            model: row.model_slug,
            input_cost_per_token: parse_pricing_field(&row.pricing, "prompt"),
            output_cost_per_token: parse_pricing_field(&row.pricing, "completion"),
            cache_read_cost_per_token: parse_pricing_field(&row.pricing, "input_cache_read"),
            cache_write_cost_per_token: parse_pricing_field(&row.pricing, "input_cache_write"),
        }
    }
}

/// LLM Request extracted from OTLP spans.
///
/// Represents a single LLM API call with all relevant metadata extracted from
/// OpenTelemetry GenAI semantic convention attributes. Used for storage in
/// ClickHouse and analytics queries.
///
/// # Fields Overview
///
/// - **Identifiers**: `project_id`, `request_id`, `trace_id`, `span_id`
/// - **GenAI Attributes**: Model and operation info per OpenTelemetry GenAI conventions
/// - **Token Usage**: Input, output, and cache token counts
/// - **Cost**: Calculated USD cost based on dynamic pricing
/// - **Timing**: Timestamp, duration, and time-to-first-token
/// - **Status**: Success/error status with error details
/// - **Session & User**: Conversation session and user tracking
/// - **Content**: Optional request/response messages (can be disabled for privacy)
/// - **Custom Properties**: User-defined key-value pairs (from `reiver.*` span attributes)
/// - **Scores**: Evaluation scores added via API after the request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmRequest {
    /// The project identifier (UUID) this request belongs to.
    pub project_id: String,

    /// Unique request identifier in format `{trace_id}:{span_id}`.
    pub request_id: String,

    /// OpenTelemetry trace ID for distributed tracing correlation.
    pub trace_id: String,

    /// OpenTelemetry span ID for this specific LLM call.
    pub span_id: String,

    /// The AI provider/system (e.g., "openai", "anthropic", "google").
    /// From `gen_ai.system` attribute.
    pub gen_ai_system: String,

    /// The model requested by the user (e.g., "gpt-4o", "claude-3-5-sonnet").
    /// From `gen_ai.request.model` attribute.
    pub gen_ai_request_model: String,

    /// The actual model used by the provider (may include version suffix).
    /// From `gen_ai.response.model` attribute.
    pub gen_ai_response_model: String,

    /// The operation type (e.g., "chat", "completion", "embedding").
    /// From `gen_ai.operation.name` attribute.
    pub gen_ai_operation_name: String,

    /// Number of input/prompt tokens consumed.
    /// From `gen_ai.usage.input_tokens` attribute.
    pub input_tokens: u32,

    /// Number of output/completion tokens generated.
    /// From `gen_ai.usage.output_tokens` attribute.
    pub output_tokens: u32,

    /// Total tokens (input + output). Computed field.
    pub total_tokens: u32,

    /// Number of tokens read from cache (for providers that support prompt caching).
    /// From `gen_ai.usage.cache_read_tokens` attribute.
    pub cache_read_tokens: u32,

    /// Number of tokens written to cache (for providers that support prompt caching).
    /// From `gen_ai.usage.cache_write_tokens` attribute.
    pub cache_write_tokens: u32,

    /// Calculated cost in USD based on token usage and dynamic pricing.
    /// Computed using the `CostCalculator` with pricing from external sources.
    pub cost_usd: Decimal,

    /// When the LLM request started (from span start time).
    pub timestamp: DateTime<Utc>,

    /// Total request latency in milliseconds (span duration).
    pub duration_ms: u32,

    /// Time to first token in milliseconds (for streaming responses).
    /// From `gen_ai.performance.time_to_first_token_ms` attribute.
    pub time_to_first_token_ms: u32,

    /// Request status: "ok" for success, "error" for failures.
    pub status_code: String,

    /// Error type if the request failed (e.g., "rate_limit_exceeded").
    /// From `gen_ai.error.type` attribute.
    pub error_type: String,

    /// Error message with details about the failure.
    pub error_message: String,

    /// Session ID for grouping related requests in a conversation.
    /// From `gen_ai.conversation.id` or the deprecated `gen_ai.session.id` alias.
    pub session_id: String,

    /// Human-readable session name/title.
    /// From `gen_ai.session.name` attribute.
    pub session_name: String,

    /// User ID for tracking per-user analytics.
    /// From `gen_ai.user.id` attribute.
    pub user_id: String,

    /// JSON-encoded request messages (optional, can be disabled for privacy).
    /// From `gen_ai.request.messages` attribute.
    pub request_messages: String,

    /// The assistant's response content (optional, can be disabled for privacy).
    /// From `gen_ai.response.content` attribute.
    pub response_content: String,

    /// Custom key-value properties from span attributes prefixed with `reiver.`.
    /// Allows users to attach arbitrary metadata to LLM requests.
    pub properties: HashMap<String, String>,

    /// Evaluation scores added via the scores API endpoint.
    /// Maps score name (e.g., "relevance", "accuracy") to numeric value.
    pub scores: HashMap<String, f64>,

    /// The service name from OpenTelemetry resource attributes.
    pub service_name: String,

    // Fallback and guardrail tracking
    /// Whether a fallback model was used instead of the originally requested model.
    pub fallback_used: bool,
    /// The model originally requested before fallback was applied.
    pub original_model: String,
    /// Number of retries before a successful response (or final failure).
    pub retry_count: u32,
    /// Guardrail rule names that were triggered (e.g. "pii_blocked", "token_limit").
    pub guardrail_violations: Vec<String>,

    // Request parameters (for session replay)
    /// Sampling temperature used for this request.
    pub temperature: Option<f32>,
    /// Nucleus sampling (top-p) used for this request.
    pub top_p: Option<f32>,
    /// Maximum tokens requested for this completion.
    pub max_tokens: Option<u32>,
    /// Frequency penalty applied to this request.
    pub frequency_penalty: Option<f32>,
    /// Presence penalty applied to this request.
    pub presence_penalty: Option<f32>,

    // Tool call tracking
    /// Number of tool calls in this request's messages.
    pub tool_call_count: u32,
    /// Unique tool names referenced in this request's messages.
    pub tool_names: Vec<String>,

    /// Whether this request used a platform-managed API key (true) or a BYOK key (false).
    pub is_platform_key: bool,

    // Rollout tracking fields (for progressive deployment)
    /// The rollout ID if this request is part of a rollout.
    pub rollout_id: String,
    /// The variant assigned: "target" or "baseline".
    pub rollout_variant: String,
    /// The prompt config ID.
    pub prompt_config_id: String,
    /// The prompt version ID used for this request.
    pub prompt_version_id: String,
}

impl Default for LlmRequest {
    fn default() -> Self {
        Self {
            project_id: String::new(),
            request_id: String::new(),
            trace_id: String::new(),
            span_id: String::new(),
            gen_ai_system: String::new(),
            gen_ai_request_model: String::new(),
            gen_ai_response_model: String::new(),
            gen_ai_operation_name: String::new(),
            input_tokens: 0,
            output_tokens: 0,
            total_tokens: 0,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
            cost_usd: Decimal::ZERO,
            timestamp: Utc::now(),
            duration_ms: 0,
            time_to_first_token_ms: 0,
            status_code: "ok".to_string(),
            error_type: String::new(),
            error_message: String::new(),
            session_id: String::new(),
            session_name: String::new(),
            user_id: String::new(),
            request_messages: String::new(),
            response_content: String::new(),
            properties: HashMap::new(),
            scores: HashMap::new(),
            service_name: String::new(),
            fallback_used: false,
            original_model: String::new(),
            retry_count: 0,
            guardrail_violations: Vec::new(),
            temperature: None,
            top_p: None,
            max_tokens: None,
            frequency_penalty: None,
            presence_penalty: None,
            tool_call_count: 0,
            tool_names: Vec::new(),
            is_platform_key: false,
            rollout_id: String::new(),
            rollout_variant: String::new(),
            prompt_config_id: String::new(),
            prompt_version_id: String::new(),
        }
    }
}

// ============================================================================
// Rollout Variant Enum
// ============================================================================

/// Represents the variant assigned to a request during a progressive rollout.
///
/// Replaces raw string comparisons (`"target"` / `"baseline"`) with a type-safe enum.
/// Database rows and ClickHouse queries still store variants as strings; use
/// [`RolloutVariant::from_str`] to parse and [`RolloutVariant::as_str`] or
/// [`Display`] to convert back.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RolloutVariant {
    Target,
    Baseline,
}

impl RolloutVariant {
    /// Return the canonical lowercase string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            RolloutVariant::Target => "target",
            RolloutVariant::Baseline => "baseline",
        }
    }

    /// Parse a string into a `RolloutVariant`, returning `None` for unknown values.
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "target" => Some(RolloutVariant::Target),
            "baseline" => Some(RolloutVariant::Baseline),
            _ => None,
        }
    }
}

impl std::fmt::Display for RolloutVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ============================================================================
// Rollout Metrics Types
// ============================================================================

/// Metrics for a rollout variant (target or baseline).
/// Used for comparing performance during progressive deployments.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VariantMetrics {
    /// Number of requests processed
    pub request_count: u64,
    /// Number of requests that resulted in errors
    pub error_count: u64,
    /// Error rate as a decimal (0.0 - 1.0)
    pub error_rate: f64,
    /// Average response latency in milliseconds
    pub avg_latency_ms: f64,
    /// 95th percentile latency in milliseconds
    pub p95_latency_ms: f64,
    /// Average cost per request in USD
    pub avg_cost_usd: Decimal,
    /// Average quality score (if evaluation scores are available)
    pub avg_quality_score: Option<f64>,
}
