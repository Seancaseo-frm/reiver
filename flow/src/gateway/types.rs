//! OpenAI-compatible request/response types for the AI Gateway.
//!
//! These types implement the OpenAI Chat Completions API format, which serves
//! as the universal interface for the gateway. All provider requests are translated
//! to/from these types.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Stringly-typed enum helpers
// ---------------------------------------------------------------------------

macro_rules! string_enum {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $($(#[$vmeta:meta])* $variant:ident => $str:literal),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub enum $name {
            $($(#[$vmeta])* $variant,)+
            Other(String),
        }

        impl $name {
            pub fn as_str(&self) -> &str {
                match self {
                    $(Self::$variant => $str,)+
                    Self::Other(s) => s,
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let s = String::deserialize(deserializer)?;
                Ok(match s.as_str() {
                    $($str => Self::$variant,)+
                    _ => Self::Other(s),
                })
            }
        }
    };
}

// ---------------------------------------------------------------------------
// Wire-compatible enums replacing raw String fields
// ---------------------------------------------------------------------------

string_enum! {
    /// Reason the model stopped generating tokens.
    pub enum FinishReason {
        Stop => "stop",
        Length => "length",
        ToolCalls => "tool_calls",
        ContentFilter => "content_filter",
    }
}

string_enum! {
    /// The kind of tool (currently only `function`).
    pub enum ToolType {
        Function => "function",
    }
}

impl Default for ToolType {
    fn default() -> Self {
        Self::Function
    }
}

string_enum! {
    /// Toggle for extended thinking / introspection.
    pub enum ThinkingToggle {
        Enabled => "enabled",
        Disabled => "disabled",
    }
}

string_enum! {
    /// Reasoning effort level for OpenAI o-series models.
    pub enum ReasoningEffort {
        Low => "low",
        Medium => "medium",
        High => "high",
    }
}

string_enum! {
    /// Response format type.
    pub enum ResponseFormatType {
        Text => "text",
        JsonObject => "json_object",
        JsonSchema => "json_schema",
    }
}

string_enum! {
    /// Source of thinking/reasoning content.
    pub enum ThinkingType {
        ExtendedThinking => "extended_thinking",
        Reasoning => "reasoning",
        GeminiThinking => "gemini_thinking",
    }
}

string_enum! {
    /// Tool choice mode (non-specific).
    pub enum ToolChoiceMode {
        None => "none",
        Auto => "auto",
        Required => "required",
    }
}

/// OpenAI-compatible chat completion request.
///
/// This is the universal request format that users send to the gateway.
/// The gateway routes to the appropriate provider based on the `model` field.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ChatCompletionRequest {
    /// Model identifier (e.g., "gpt-4o", "claude-3-opus", "gemini-pro").
    /// The gateway uses this to route to the correct provider.
    pub model: String,

    /// The messages for the chat conversation.
    pub messages: Vec<ChatMessage>,

    /// Sampling temperature (0.0 to 2.0). Higher = more random.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,

    /// Maximum tokens to generate in the response.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,

    /// Alternative to temperature, nucleus sampling.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,

    /// Number of completions to generate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub n: Option<u32>,

    /// Whether to stream partial responses.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,

    /// Options for streaming responses. Only relevant when `stream` is true.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_options: Option<StreamOptions>,

    /// Sequences where the API will stop generating.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop: Option<StopSequence>,

    /// Penalize new tokens based on their frequency in the text so far.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frequency_penalty: Option<f32>,

    /// Penalize new tokens based on whether they appear in the text so far.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub presence_penalty: Option<f32>,

    /// Unique identifier for the end-user (for abuse monitoring).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,

    /// Seed for deterministic sampling.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seed: Option<i64>,

    /// Tools (functions) the model may call.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<Tool>>,

    /// Controls which (if any) tool is called by the model.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<ToolChoice>,

    /// Response format specification.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_format: Option<ResponseFormat>,

    /// Introspection/extended thinking configuration.
    /// When enabled, the model will expose its reasoning process.
    /// For Anthropic: Uses extended thinking with thinking blocks.
    /// For OpenAI o-series: Uses reasoning_effort parameter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thinking: Option<ThinkingConfig>,

    /// Reasoning effort for OpenAI o-series models (o1, o3, o4-mini).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,

    /// Name of the Flow prompt config to apply to this request.
    /// Alternative to the `X-Reiver-Prompt-Config` header.
    /// Use with the OpenAI SDK via `extra_body={"prompt_config": "my-config"}`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_config: Option<String>,

    /// Runtime template variables to interpolate into the managed prompt.
    /// Alternative to `X-Reiver-Var-*` headers with no 255-char length limit.
    /// Use with the OpenAI SDK via `extra_body={"prompt_variables": {"user": "Alice"}}`.
    /// Header variables (`X-Reiver-Var-*`) take precedence over body variables
    /// when the same key appears in both.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_variables: Option<std::collections::HashMap<String, serde_json::Value>>,

    /// Ordered fallback models. If the primary `model` fails (5xx, rate limit,
    /// timeout), these are tried in order. Overrides the project's default
    /// fallback chain for this request.
    /// Use with the OpenAI SDK via `extra_body={"models": ["gpt-4o", "claude-sonnet-4-6"]}`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub models: Option<Vec<String>>,

    /// Provider routing preferences for this request. Controls which providers
    /// are tried, in what order, and whether fallback is allowed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<ProviderPreferences>,
}

/// Per-request provider routing preferences.
///
/// Controls how the gateway selects among available providers for the requested
/// model. When a model can be served by multiple providers (e.g., Claude via
/// Anthropic direct or AWS Bedrock), these preferences determine the routing.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ProviderPreferences {
    /// Ordered list of provider slugs to try (e.g., `["anthropic", "bedrock"]`).
    /// Providers not in this list are tried after, in default order.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order: Option<Vec<String>>,

    /// Restrict routing to only these providers. Others are excluded entirely.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub only: Option<Vec<String>>,

    /// Skip these providers for this request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ignore: Option<Vec<String>>,

    /// Whether to allow fallback to other models/providers on failure.
    /// Defaults to true when absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_fallbacks: Option<bool>,

    /// Sort strategy for provider selection: `"latency"` sorts by P95 latency.
    /// Default behaviour uses the platform's built-in ordering.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<String>,
}

/// Configuration for extended thinking/introspection.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ThinkingConfig {
    /// Whether thinking is enabled.
    #[serde(rename = "type")]
    pub thinking_type: ThinkingToggle,

    /// Maximum tokens for thinking (Anthropic).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub budget_tokens: Option<u32>,
}

/// Maximum allowed messages in a single request (to prevent DoS).
const MAX_MESSAGES: usize = 1000;
/// Maximum allowed max_tokens value.
const MAX_MAX_TOKENS: u32 = 1_000_000;
/// Maximum allowed size for a single message content (1 MB).
/// This prevents a single very large message from consuming excessive memory
/// while still allowing reasonable content including base64 images.
const MAX_MESSAGE_CONTENT_SIZE: usize = 1024 * 1024;

impl ChatCompletionRequest {
    /// Validate the request and return errors for invalid values.
    ///
    /// Checks:
    /// - model is not empty
    /// - messages array is not empty and within limits
    /// - individual message content does not exceed 1MB
    /// - temperature is in valid range (0.0-2.0)
    /// - max_tokens is within limits
    /// - top_p is in valid range (0.0-1.0)
    /// - frequency_penalty is in valid range (-2.0-2.0)
    /// - presence_penalty is in valid range (-2.0-2.0)
    /// - stop sequences are not empty
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        // Model must not be empty unless a prompt_config or models array is provided.
        let has_models_array = self.models.as_ref().is_some_and(|m| !m.is_empty());
        if self.model.trim().is_empty() && self.prompt_config.is_none() && !has_models_array {
            errors.push("model field is required and cannot be empty".to_string());
        }

        // Validate models array
        if let Some(ref models) = self.models {
            if models.len() > 5 {
                errors.push(format!(
                    "models array exceeds maximum of 5 fallback models (received {})",
                    models.len()
                ));
            }
            if models.iter().any(|m| m.trim().is_empty()) {
                errors.push("models array must not contain empty strings".to_string());
            }
        }

        // Messages must not be empty
        if self.messages.is_empty() {
            errors.push("messages array must contain at least one message".to_string());
        }

        // Messages array size limit
        if self.messages.len() > MAX_MESSAGES {
            errors.push(format!(
                "messages array exceeds maximum of {} messages (received {})",
                MAX_MESSAGES,
                self.messages.len()
            ));
        }

        // Validate individual message content sizes
        for (i, message) in self.messages.iter().enumerate() {
            if let Some(content) = &message.content {
                let size = content.size_bytes();
                if size > MAX_MESSAGE_CONTENT_SIZE {
                    errors.push(format!(
                        "message {} content exceeds maximum size of {} bytes (received {} bytes)",
                        i, MAX_MESSAGE_CONTENT_SIZE, size
                    ));
                }
            }
        }

        // Validate temperature (0.0 to 1.0 — safe for all providers including Anthropic)
        if let Some(temp) = self.temperature {
            if !(0.0..=1.0).contains(&temp) {
                errors.push(format!(
                    "temperature must be between 0.0 and 1.0 (received {})",
                    temp
                ));
            }
        }

        // Validate max_tokens (positive and within limit)
        if let Some(max_tokens) = self.max_tokens {
            if max_tokens == 0 {
                errors.push("max_tokens must be greater than 0".to_string());
            }
            if max_tokens > MAX_MAX_TOKENS {
                errors.push(format!(
                    "max_tokens exceeds maximum of {} (received {})",
                    MAX_MAX_TOKENS, max_tokens
                ));
            }
        }

        // Validate top_p (0.0 to 1.0)
        if let Some(top_p) = self.top_p {
            if !(0.0..=1.0).contains(&top_p) {
                errors.push(format!(
                    "top_p must be between 0.0 and 1.0 (received {})",
                    top_p
                ));
            }
        }

        // Validate frequency_penalty (-2.0 to 2.0)
        if let Some(freq) = self.frequency_penalty {
            if !(-2.0..=2.0).contains(&freq) {
                errors.push(format!(
                    "frequency_penalty must be between -2.0 and 2.0 (received {})",
                    freq
                ));
            }
        }

        // Validate presence_penalty (-2.0 to 2.0)
        if let Some(pres) = self.presence_penalty {
            if !(-2.0..=2.0).contains(&pres) {
                errors.push(format!(
                    "presence_penalty must be between -2.0 and 2.0 (received {})",
                    pres
                ));
            }
        }

        // Validate n (must be at least 1 if specified)
        if let Some(n) = self.n {
            if n == 0 {
                errors.push("n must be at least 1".to_string());
            }
            if n > 10 {
                errors.push(format!("n exceeds maximum of 10 (received {})", n));
            }
        }

        // Validate stop sequences (must not be empty)
        if let Some(stop) = &self.stop {
            match stop {
                StopSequence::Single(s) if s.is_empty() => {
                    errors.push("stop sequence cannot be an empty string".to_string());
                }
                StopSequence::Multiple(v) if v.is_empty() => {
                    errors.push("stop sequences array cannot be empty".to_string());
                }
                StopSequence::Multiple(v) if v.iter().any(|s| s.is_empty()) => {
                    errors.push("stop sequences array cannot contain empty strings".to_string());
                }
                _ => {}
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

impl Default for ChatCompletionRequest {
    fn default() -> Self {
        Self {
            model: String::new(),
            messages: Vec::new(),
            temperature: None,
            max_tokens: None,
            top_p: None,
            n: None,
            stream: None,
            stream_options: None,
            stop: None,
            frequency_penalty: None,
            presence_penalty: None,
            user: None,
            seed: None,
            tools: None,
            tool_choice: None,
            response_format: None,
            thinking: None,
            reasoning_effort: None,
            prompt_config: None,
            prompt_variables: None,
            models: None,
            provider: None,
        }
    }
}

/// Options for streaming responses.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StreamOptions {
    /// When true, an additional chunk with usage statistics is sent before the
    /// `[DONE]` marker. Mirrors OpenAI's `stream_options.include_usage`.
    #[serde(default)]
    pub include_usage: bool,
}

/// Stop sequence can be a single string or array of strings.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum StopSequence {
    Single(String),
    Multiple(Vec<String>),
}

/// The role of a chat message author.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MessageRole {
    System,
    User,
    Assistant,
    Tool,
    #[serde(other)]
    Other,
}

/// A message in the chat conversation.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ChatMessage {
    /// The role of the message author: "system", "user", "assistant", or "tool".
    pub role: MessageRole,

    /// The content of the message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<MessageContent>,

    /// The name of the author (optional, for multi-user conversations).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Tool calls made by the assistant.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ToolCall>>,

    /// For tool messages: the ID of the tool call this is responding to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,

    /// DeepSeek reasoning content. Must be passed back on assistant messages
    /// for multi-turn conversations with reasoning models (R1, etc.).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
}

/// Collect all system messages from a slice of chat messages and return their
/// combined text content, separated by double newlines.
///
/// This is used by provider adapters (Anthropic, Google) that need to extract
/// the system prompt from the OpenAI-format messages array into a single string.
///
/// Returns `None` if there are no system messages or if none have content.
pub fn find_system_message_text(messages: &[ChatMessage]) -> Option<String> {
    let combined = messages
        .iter()
        .filter(|m| m.role == MessageRole::System)
        .filter_map(|m| m.content.as_ref())
        .map(|c| c.as_text())
        .filter(|t| !t.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n");
    Some(combined).filter(|s| !s.is_empty())
}

/// Return an iterator over messages that are not system messages.
///
/// Provider adapters typically handle system messages separately (e.g., as a top-level
/// `system` parameter in Anthropic, or `system_instruction` in Gemini) and need to
/// iterate over only the non-system messages for conversion.
pub fn non_system_messages(messages: &[ChatMessage]) -> impl Iterator<Item = &ChatMessage> {
    messages.iter().filter(|m| m.role != MessageRole::System)
}

/// Message content can be a simple string or an array of content parts (for multimodal).
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(untagged)]
pub enum MessageContent {
    Text(String),
    Parts(Vec<ContentPart>),
}

impl MessageContent {
    /// Extract text content as a string.
    pub fn as_text(&self) -> String {
        match self {
            MessageContent::Text(s) => s.clone(),
            MessageContent::Parts(parts) => parts
                .iter()
                .filter_map(|p| {
                    if let ContentPart::Text { text } = p {
                        Some(text.as_str())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
                .join(""),
        }
    }

    /// Calculate the approximate size of this content in bytes.
    ///
    /// For text content, returns the string length.
    /// For multimodal content, includes text, image URL/data, and document data sizes.
    pub fn size_bytes(&self) -> usize {
        match self {
            MessageContent::Text(s) => s.len(),
            MessageContent::Parts(parts) => parts
                .iter()
                .map(|p| match p {
                    ContentPart::Text { text } => text.len(),
                    ContentPart::ImageUrl { image_url } => image_url.url.len(),
                    ContentPart::DocumentUrl { document_url } => document_url.url.len(),
                })
                .sum(),
        }
    }
}

/// A part of multimodal message content.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(tag = "type")]
pub enum ContentPart {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "image_url")]
    ImageUrl { image_url: ImageUrl },
    /// A document (PDF or other file) attached inline as a base64 data URL or external URL.
    #[serde(rename = "document_url")]
    DocumentUrl { document_url: DocumentUrl },
}

/// Image URL specification for multimodal messages.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct ImageUrl {
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Document URL specification for file attachments (PDFs, etc.).
///
/// Supported formats:
/// - Base64 data URL: `data:application/pdf;base64,<base64-data>`
/// - External URL: `https://example.com/document.pdf` (provider support varies)
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct DocumentUrl {
    /// The document source: a base64 data URL or an https:// URL.
    pub url: String,
    /// MIME type override (e.g. `"application/pdf"`).
    /// If omitted, it is inferred from the data URL prefix or the file extension.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    /// Optional display name for the document shown to the model.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
}

/// Tool definition for function calling.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Tool {
    #[serde(rename = "type")]
    pub tool_type: ToolType,
    pub function: FunctionDefinition,
}

/// Function definition for tool calling.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FunctionDefinition {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<serde_json::Value>,
}

/// Tool choice specification.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum ToolChoice {
    Mode(ToolChoiceMode),
    Specific {
        #[serde(rename = "type")]
        tool_type: ToolType,
        function: ToolChoiceFunction,
    },
}

/// Specific function to call.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ToolChoiceFunction {
    pub name: String,
}

/// Tool call made by the assistant.
///
/// In streaming responses, the `index` field identifies which tool call a
/// delta belongs to (required by the OpenAI streaming format when the model
/// invokes multiple tools in a single turn). Non-streaming responses and
/// request messages leave `index` as `None`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ToolCall {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<u32>,
    #[serde(default)]
    pub id: String,
    #[serde(rename = "type", default)]
    pub tool_type: ToolType,
    pub function: FunctionCall,
}

/// Function call details.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FunctionCall {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub arguments: String,
}

/// Response format specification.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ResponseFormat {
    #[serde(rename = "type")]
    pub format_type: ResponseFormatType,
}

/// Object type for non-streaming chat completions.
pub const OBJECT_CHAT_COMPLETION: &str = "chat.completion";
/// Object type for streaming chat completion chunks.
pub const OBJECT_CHAT_COMPLETION_CHUNK: &str = "chat.completion.chunk";

/// OpenAI-compatible chat completion response.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ChatCompletionResponse {
    /// Unique identifier for this completion.
    pub id: String,

    /// Object type, always "chat.completion".
    pub object: String,

    /// Unix timestamp of when the completion was created.
    pub created: u64,

    /// The model used for the completion.
    pub model: String,

    /// The list of completion choices.
    pub choices: Vec<Choice>,

    /// Token usage statistics.
    pub usage: Usage,

    /// System fingerprint for reproducibility.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_fingerprint: Option<String>,
}

impl ChatCompletionResponse {
    /// Create a new response with the given parameters.
    pub fn new(id: String, model: String, choices: Vec<Choice>, usage: Usage) -> Self {
        Self {
            id,
            object: OBJECT_CHAT_COMPLETION.to_string(),
            created: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            model,
            choices,
            usage,
            system_fingerprint: None,
        }
    }
}

/// A completion choice.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Choice {
    /// The index of this choice.
    pub index: u32,

    /// The generated message.
    pub message: AssistantMessage,

    /// The reason the model stopped generating.
    pub finish_reason: FinishReason,

    /// Log probability information (if requested).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logprobs: Option<serde_json::Value>,
}

/// Assistant message in the response.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AssistantMessage {
    pub role: MessageRole,

    /// The content of the message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,

    /// Tool calls made by the assistant.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ToolCall>>,

    /// Thinking/reasoning content from introspection-enabled models.
    /// Contains the model's internal reasoning process.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thinking: Option<ThinkingContent>,
}

/// Thinking/reasoning content from AI model introspection.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ThinkingContent {
    /// The thinking/reasoning text.
    pub content: String,

    /// Number of tokens used for thinking.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tokens: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub thinking_type: Option<ThinkingType>,
}

/// Token usage statistics.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Usage {
    /// Number of tokens in the prompt.
    pub prompt_tokens: u32,

    /// Number of tokens in the completion.
    pub completion_tokens: u32,

    /// Total tokens used (prompt + completion).
    pub total_tokens: u32,

    /// Number of thinking/reasoning tokens (for introspection-enabled requests).
    /// Anthropic: Thinking tokens from extended thinking (included in completion_tokens).
    /// OpenAI o-series: Reasoning tokens from completion_tokens_details.reasoning_tokens.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thinking_tokens: Option<u32>,

    /// OpenAI completion token breakdown (o-series reasoning models).
    /// Used to extract `reasoning_tokens` into `thinking_tokens`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion_tokens_details: Option<CompletionTokensDetails>,

    /// OpenAI prompt token breakdown (includes cache hit information).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_tokens_details: Option<PromptTokensDetails>,
}

/// OpenAI prompt token details (cache hits, audio tokens, etc.).
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct PromptTokensDetails {
    #[serde(default)]
    pub cached_tokens: u32,
}

/// OpenAI completion token details (returned by o-series reasoning models).
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct CompletionTokensDetails {
    /// Tokens used for internal reasoning (not visible in the final response text).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accepted_prediction_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rejected_prediction_tokens: Option<u32>,
}

/// Streaming chat completion chunk (for SSE responses).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatCompletionChunk {
    pub id: String,
    pub object: String, // "chat.completion.chunk"
    pub created: u64,
    pub model: String,
    pub choices: Vec<ChunkChoice>,
    /// Usage information (only present in final chunk for some providers).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
}

impl ChatCompletionChunk {
    /// Create a new streaming chunk.
    pub fn new(id: String, model: String, choices: Vec<ChunkChoice>) -> Self {
        Self {
            id,
            object: OBJECT_CHAT_COMPLETION_CHUNK.to_string(),
            created: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            model,
            choices,
            usage: None,
        }
    }

    /// Create a chunk with a content delta.
    pub fn with_content(id: String, model: String, content: String) -> Self {
        Self::new(
            id,
            model,
            vec![ChunkChoice {
                index: 0,
                delta: ChunkDelta {
                    role: None,
                    content: Some(content),
                    tool_calls: None,
                    thinking: None,
                    reasoning_content: None,
                },
                finish_reason: None,
            }],
        )
    }

    /// Create a chunk with a thinking/reasoning delta.
    ///
    /// Used by Anthropic extended thinking streaming to forward the model's
    /// chain-of-thought to the client in real time.
    pub fn with_thinking(id: String, model: String, thinking: String) -> Self {
        Self::new(
            id,
            model,
            vec![ChunkChoice {
                index: 0,
                delta: ChunkDelta {
                    role: None,
                    content: None,
                    tool_calls: None,
                    thinking: Some(thinking),
                    reasoning_content: None,
                },
                finish_reason: None,
            }],
        )
    }

    /// Create a chunk indicating the stream is finished.
    pub fn finished(id: String, model: String, finish_reason: FinishReason) -> Self {
        Self::new(
            id,
            model,
            vec![ChunkChoice {
                index: 0,
                delta: ChunkDelta {
                    role: None,
                    content: None,
                    tool_calls: None,
                    thinking: None,
                    reasoning_content: None,
                },
                finish_reason: Some(finish_reason),
            }],
        )
    }
}

/// A choice in a streaming chunk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkChoice {
    pub index: u32,
    pub delta: ChunkDelta,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<FinishReason>,
}

/// Delta content in a streaming chunk.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ChunkDelta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<MessageRole>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ToolCall>>,
    /// Thinking/reasoning content delta (Anthropic extended thinking streaming).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thinking: Option<String>,
    /// DeepSeek reasoning_content delta (R1/reasoner models).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
}

/// OpenAI-compatible error response.
#[derive(Debug, Clone, Serialize)]
pub struct ErrorResponse {
    pub error: ErrorDetail,
}

/// Error detail structure.
#[derive(Debug, Clone, Serialize)]
pub struct ErrorDetail {
    pub message: String,
    #[serde(rename = "type")]
    pub error_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub param: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
}

impl ErrorResponse {
    pub fn new(message: impl Into<String>, error_type: impl Into<String>) -> Self {
        Self {
            error: ErrorDetail {
                message: message.into(),
                error_type: error_type.into(),
                param: None,
                code: None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_simple_request() {
        let json = r#"{
            "model": "gpt-4o",
            "messages": [
                {"role": "user", "content": "Hello!"}
            ]
        }"#;

        let request: ChatCompletionRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.model, "gpt-4o");
        assert_eq!(request.messages.len(), 1);
        assert_eq!(request.messages[0].role, MessageRole::User);
    }

    #[test]
    fn test_deserialize_multimodal_content() {
        let json = r#"{
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
        }"#;

        let request: ChatCompletionRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.messages.len(), 1);
        if let Some(MessageContent::Parts(parts)) = &request.messages[0].content {
            assert_eq!(parts.len(), 2);
        } else {
            panic!("Expected Parts content");
        }
    }

    #[test]
    fn test_serialize_response() {
        let response = ChatCompletionResponse::new(
            "chatcmpl-123".to_string(),
            "gpt-4o".to_string(),
            vec![Choice {
                index: 0,
                message: AssistantMessage {
                    role: MessageRole::Assistant,
                    content: Some("Hello! How can I help?".to_string()),
                    tool_calls: None,
                    thinking: None,
                },

                finish_reason: FinishReason::Stop,
                logprobs: None,
            }],
            Usage {
                prompt_tokens: 10,
                completion_tokens: 8,
                total_tokens: 18,
                thinking_tokens: None,
                completion_tokens_details: None,
                prompt_tokens_details: None,
            },
        );

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("chat.completion"));
        assert!(json.contains("gpt-4o"));
    }

    #[test]
    fn test_message_content_as_text() {
        let text_content = MessageContent::Text("Hello".to_string());
        assert_eq!(text_content.as_text(), "Hello");

        let parts_content = MessageContent::Parts(vec![
            ContentPart::Text {
                text: "Hello ".to_string(),
            },
            ContentPart::Text {
                text: "World".to_string(),
            },
        ]);
        assert_eq!(parts_content.as_text(), "Hello World");
    }

    #[test]
    fn test_message_content_size_bytes() {
        // Text content
        let text_content = MessageContent::Text("Hello".to_string());
        assert_eq!(text_content.size_bytes(), 5);

        // Multimodal content
        let parts_content = MessageContent::Parts(vec![
            ContentPart::Text {
                text: "Hello".to_string(),
            },
            ContentPart::ImageUrl {
                image_url: ImageUrl {
                    url: "https://example.com/image.jpg".to_string(),
                    detail: None,
                },
            },
        ]);
        // 5 bytes for "Hello" + 29 bytes for the URL "https://example.com/image.jpg"
        assert_eq!(parts_content.size_bytes(), 34);
    }

    #[test]
    fn test_validate_message_content_size() {
        // Create a message with content that exceeds 1MB
        let large_content = "x".repeat(1024 * 1024 + 1); // 1MB + 1 byte
        let request = ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text(large_content)),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            }],
            temperature: None,
            max_tokens: None,
            top_p: None,
            n: None,
            stream: None,
            stream_options: None,
            stop: None,
            frequency_penalty: None,
            presence_penalty: None,
            user: None,
            seed: None,
            tools: None,
            tool_choice: None,
            response_format: None,
            thinking: None,
            reasoning_effort: None,
            prompt_config: None,
            prompt_variables: None,
            models: None,
            provider: None,
        };

        let result = request.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.contains("exceeds maximum size")));
    }

    #[test]
    fn test_validate_message_content_size_ok() {
        // Create a message with content under 1MB
        let content = "x".repeat(1024 * 1024 - 1); // 1MB - 1 byte
        let request = ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text(content)),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            }],
            temperature: None,
            max_tokens: None,
            top_p: None,
            n: None,
            stream: None,
            stream_options: None,
            stop: None,
            frequency_penalty: None,
            presence_penalty: None,
            user: None,
            seed: None,
            tools: None,
            tool_choice: None,
            response_format: None,
            thinking: None,
            reasoning_effort: None,
            prompt_config: None,
            prompt_variables: None,
            models: None,
            provider: None,
        };

        let result = request.validate();
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_empty_stop_sequence_single() {
        let request = ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text("Hello".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            }],
            stop: Some(StopSequence::Single(String::new())),
            ..Default::default()
        };

        let result = request.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.contains("stop sequence cannot be an empty string")));
    }

    #[test]
    fn test_validate_empty_stop_sequence_array() {
        let request = ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text("Hello".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            }],
            stop: Some(StopSequence::Multiple(vec![])),
            ..Default::default()
        };

        let result = request.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.contains("stop sequences array cannot be empty")));
    }

    #[test]
    fn test_validate_stop_sequence_with_empty_string_in_array() {
        let request = ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text("Hello".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            }],
            stop: Some(StopSequence::Multiple(vec![
                "END".to_string(),
                String::new(),
            ])),
            ..Default::default()
        };

        let result = request.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.contains("stop sequences array cannot contain empty strings")));
    }

    #[test]
    fn test_validate_valid_stop_sequences() {
        let request = ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text("Hello".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            }],
            stop: Some(StopSequence::Multiple(vec![
                "END".to_string(),
                "STOP".to_string(),
            ])),
            ..Default::default()
        };

        let result = request.validate();
        assert!(result.is_ok());
    }

    /// Regression: `ToolCall` was missing the `index` field required by the
    /// OpenAI streaming format. Without it, clients cannot correlate streaming
    /// tool call argument deltas with the correct tool call when the model
    /// invokes multiple tools in a single turn.
    #[test]
    fn test_tool_call_index_serialization_roundtrip() {
        let tc = ToolCall {
            index: Some(0),
            id: "call_abc".to_string(),
            tool_type: ToolType::Function,
            function: FunctionCall {
                name: "get_weather".to_string(),
                arguments: "{}".to_string(),
            },
        };

        let json = serde_json::to_value(&tc).unwrap();
        assert_eq!(json["index"], 0, "index must be serialized when Some");
        assert_eq!(json["id"], "call_abc");

        let deserialized: ToolCall = serde_json::from_value(json).unwrap();
        assert_eq!(deserialized.index, Some(0));
        assert_eq!(deserialized.id, "call_abc");
    }

    /// `ToolCall` with `index: None` must NOT emit the `index` field in JSON,
    /// preserving backward compatibility for non-streaming responses.
    #[test]
    fn test_tool_call_index_none_omitted_from_json() {
        let tc = ToolCall {
            index: None,
            id: "call_abc".to_string(),
            tool_type: ToolType::Function,
            function: FunctionCall {
                name: "get_weather".to_string(),
                arguments: "{}".to_string(),
            },
        };

        let json = serde_json::to_value(&tc).unwrap();
        assert!(
            json.get("index").is_none(),
            "index must be omitted from JSON when None, got: {}",
            serde_json::to_string_pretty(&json).unwrap()
        );
    }

    /// Regression: `find_system_message_text` previously used `.find()` which
    /// returned only the first system message. Subsequent system messages were
    /// silently dropped, causing Anthropic and Google providers to lose context.
    #[test]
    fn test_find_system_message_text_collects_all_system_messages() {
        let messages = vec![
            ChatMessage {
                role: MessageRole::System,
                content: Some(MessageContent::Text(
                    "You are a helpful assistant.".to_string(),
                )),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            },
            ChatMessage {
                role: MessageRole::System,
                content: Some(MessageContent::Text("Always respond in JSON.".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            },
            ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text("Hello".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            },
        ];

        let system = find_system_message_text(&messages);
        let text = system.expect("should return combined system text");
        assert!(
            text.contains("You are a helpful assistant."),
            "must include first system message, got: {text}"
        );
        assert!(
            text.contains("Always respond in JSON."),
            "must include second system message, got: {text}"
        );
    }

    #[test]
    fn test_find_system_message_text_single_message() {
        let messages = vec![
            ChatMessage {
                role: MessageRole::System,
                content: Some(MessageContent::Text("You are helpful.".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            },
            ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text("Hi".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            },
        ];

        assert_eq!(
            find_system_message_text(&messages),
            Some("You are helpful.".to_string())
        );
    }

    #[test]
    fn test_find_system_message_text_none_when_no_system() {
        let messages = vec![ChatMessage {
            role: MessageRole::User,
            content: Some(MessageContent::Text("Hi".to_string())),
            name: None,
            tool_calls: None,
            tool_call_id: None,
            reasoning_content: None,
        }];

        assert_eq!(find_system_message_text(&messages), None);
    }

    fn make_simple_request() -> ChatCompletionRequest {
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
            ..Default::default()
        }
    }

    #[test]
    fn test_validate_temperature_valid_zero() {
        let mut req = make_simple_request();
        req.temperature = Some(0.0);
        assert!(req.validate().is_ok());
    }

    #[test]
    fn test_validate_temperature_valid_mid() {
        let mut req = make_simple_request();
        req.temperature = Some(0.5);
        assert!(req.validate().is_ok());
    }

    #[test]
    fn test_validate_temperature_valid_upper_bound() {
        let mut req = make_simple_request();
        req.temperature = Some(1.0);
        assert!(req.validate().is_ok());
    }

    #[test]
    fn test_validate_temperature_none_is_valid() {
        let req = make_simple_request();
        assert!(req.validate().is_ok());
    }

    #[test]
    fn test_validate_temperature_rejects_above_one() {
        let mut req = make_simple_request();
        req.temperature = Some(1.1);
        let errors = req.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("temperature")));
    }

    #[test]
    fn test_validate_temperature_rejects_two() {
        let mut req = make_simple_request();
        req.temperature = Some(2.0);
        let errors = req.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("temperature")));
    }

    #[test]
    fn test_validate_temperature_rejects_negative() {
        let mut req = make_simple_request();
        req.temperature = Some(-0.1);
        let errors = req.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("temperature")));
    }

    // ====================================================================
    // Routing: `models` array validation
    // ====================================================================

    #[test]
    fn test_validate_models_array_accepted_when_model_empty() {
        let mut req = make_simple_request();
        req.model = String::new();
        req.models = Some(vec!["gpt-4o".into(), "claude-sonnet-4-6".into()]);
        assert!(req.validate().is_ok());
    }

    #[test]
    fn test_validate_models_none_and_model_empty_rejected() {
        let mut req = make_simple_request();
        req.model = String::new();
        req.models = None;
        let errors = req.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("model field is required")));
    }

    #[test]
    fn test_validate_models_empty_vec_and_model_empty_rejected() {
        let mut req = make_simple_request();
        req.model = String::new();
        req.models = Some(vec![]);
        let errors = req.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("model field is required")));
    }

    #[test]
    fn test_validate_models_array_max_five() {
        let mut req = make_simple_request();
        req.models = Some(vec![
            "a".into(),
            "b".into(),
            "c".into(),
            "d".into(),
            "e".into(),
        ]);
        assert!(req.validate().is_ok());
    }

    #[test]
    fn test_validate_models_array_exceeds_max() {
        let mut req = make_simple_request();
        req.models = Some(vec![
            "a".into(),
            "b".into(),
            "c".into(),
            "d".into(),
            "e".into(),
            "f".into(),
        ]);
        let errors = req.validate().unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.contains("exceeds maximum of 5 fallback models")));
    }

    #[test]
    fn test_validate_models_array_rejects_empty_strings() {
        let mut req = make_simple_request();
        req.models = Some(vec!["gpt-4o".into(), "".into()]);
        let errors = req.validate().unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.contains("must not contain empty strings")));
    }

    #[test]
    fn test_validate_models_array_rejects_whitespace_only() {
        let mut req = make_simple_request();
        req.models = Some(vec!["gpt-4o".into(), "   ".into()]);
        let errors = req.validate().unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.contains("must not contain empty strings")));
    }

    #[test]
    fn test_validate_models_none_with_model_set_ok() {
        let req = make_simple_request();
        assert!(req.validate().is_ok());
    }

    // ====================================================================
    // Routing: `provider` (ProviderPreferences) serde
    // ====================================================================

    #[test]
    fn test_provider_preferences_default_is_all_none() {
        let prefs = ProviderPreferences::default();
        assert!(prefs.order.is_none());
        assert!(prefs.only.is_none());
        assert!(prefs.ignore.is_none());
        assert!(prefs.allow_fallbacks.is_none());
        assert!(prefs.sort.is_none());
    }

    #[test]
    fn test_provider_preferences_serde_round_trip() {
        let prefs = ProviderPreferences {
            order: Some(vec!["anthropic".into(), "bedrock".into()]),
            only: None,
            ignore: Some(vec!["openai".into()]),
            allow_fallbacks: Some(false),
            sort: Some("latency".into()),
        };
        let json = serde_json::to_string(&prefs).unwrap();
        let back: ProviderPreferences = serde_json::from_str(&json).unwrap();
        assert_eq!(back.order.as_ref().unwrap().len(), 2);
        assert_eq!(back.allow_fallbacks, Some(false));
        assert_eq!(back.sort.as_deref(), Some("latency"));
        assert!(back.only.is_none());
        assert_eq!(back.ignore.as_ref().unwrap(), &vec!["openai".to_string()]);
    }

    #[test]
    fn test_provider_preferences_skip_serializing_none_fields() {
        let prefs = ProviderPreferences::default();
        let json = serde_json::to_string(&prefs).unwrap();
        assert_eq!(json, "{}");
    }

    #[test]
    fn test_chat_completion_request_models_and_provider_not_serialized_when_none() {
        let req = make_simple_request();
        let body = serde_json::to_value(&req).unwrap();
        assert!(
            body.get("models").is_none(),
            "models must not appear when None"
        );
        assert!(
            body.get("provider").is_none(),
            "provider must not appear when None"
        );
    }

    #[test]
    fn test_chat_completion_request_models_deserialization() {
        let json = r#"{
            "model": "",
            "messages": [{"role": "user", "content": "hi"}],
            "models": ["gpt-4o", "claude-sonnet-4-6"]
        }"#;
        let req: ChatCompletionRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.models.as_ref().unwrap().len(), 2);
        assert!(req.model.is_empty());
    }

    #[test]
    fn test_chat_completion_request_provider_deserialization() {
        let json = r#"{
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "hi"}],
            "provider": {
                "order": ["bedrock", "anthropic"],
                "allow_fallbacks": false,
                "sort": "latency"
            }
        }"#;
        let req: ChatCompletionRequest = serde_json::from_str(json).unwrap();
        let prefs = req.provider.as_ref().unwrap();
        assert_eq!(prefs.order.as_ref().unwrap().len(), 2);
        assert_eq!(prefs.allow_fallbacks, Some(false));
        assert_eq!(prefs.sort.as_deref(), Some("latency"));
    }

    #[test]
    fn test_find_system_message_text_skips_empty_content() {
        let messages = vec![
            ChatMessage {
                role: MessageRole::System,
                content: Some(MessageContent::Text(String::new())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            },
            ChatMessage {
                role: MessageRole::System,
                content: Some(MessageContent::Text("Second system.".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            },
        ];

        assert_eq!(
            find_system_message_text(&messages),
            Some("Second system.".to_string())
        );
    }
}
