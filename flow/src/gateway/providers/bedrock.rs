//! AWS Bedrock provider adapter.
//!
//! Translates between OpenAI chat completion format and AWS Bedrock's Converse API.
//! Uses the AWS SDK for Rust for authentication and API calls.
//!
//! # Streaming
//!
//! Streaming uses the AWS SDK's `converse_stream()` method which properly handles
//! the AWS event stream binary format. The events are converted to OpenAI-compatible
//! `ChatCompletionChunk` format for SSE streaming.
//!
//! # Client Caching
//!
//! AWS SDK clients are cached by access key ID to reuse connection pools across requests.
//! This significantly improves performance for high-volume workloads.

use async_trait::async_trait;
use aws_sdk_bedrockruntime::config::{Credentials, Region};
use aws_sdk_bedrockruntime::types::{
    ContentBlock, ConversationRole, ConverseStreamOutput, DocumentBlock, DocumentFormat,
    DocumentSource, ImageBlock, ImageFormat, ImageSource, Message, SystemContentBlock,
    Tool as SdkTool, ToolConfiguration, ToolInputSchema, ToolResultBlock, ToolResultContentBlock,
    ToolResultStatus, ToolSpecification, ToolUseBlock,
};
use aws_sdk_bedrockruntime::Client as BedrockClient;
use base64::Engine as _;
use parking_lot::RwLock;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use super::common::{create_http_client, parse_provider_error};
use super::sse::map_finish_reason_to_openai;
use super::{ChatCompletionStream, LlmProvider};
use crate::gateway::error::GatewayError;
use crate::gateway::provider_types::Provider;
use crate::gateway::types::{
    AssistantMessage, ChatCompletionChunk, ChatCompletionRequest, ChatCompletionResponse,
    ChatMessage, Choice, ChunkChoice, ChunkDelta, ContentPart, FinishReason, MessageContent,
    MessageRole, ToolCall, ToolType, Usage,
};
use aws_sdk_bedrockruntime::operation::converse_stream::ConverseStreamError;
use aws_smithy_types::Document;

const DEFAULT_TIMEOUT_SECS: u64 = 180; // 3 minutes default for Bedrock (cold starts)

/// Maximum number of cached Bedrock clients per provider instance.
/// This prevents unbounded memory growth while still providing reuse benefits.
const MAX_CACHED_CLIENTS: usize = 100;

/// TTL for cached Bedrock clients in seconds.
/// Clients older than this will be recreated to handle credential rotation.
/// Set to 15 minutes - long enough for connection pool benefits, short enough
/// to pick up rotated credentials reasonably quickly.
const CLIENT_CACHE_TTL_SECS: u64 = 15 * 60;

/// Cached Bedrock client with creation timestamp for TTL expiration.
struct CachedClient {
    client: BedrockClient,
    created_at: std::time::Instant,
}

/// AWS Bedrock provider adapter.
///
/// Supports models from multiple providers through Bedrock:
/// - Anthropic Claude models (anthropic.claude-*)
/// - Amazon Titan models (amazon.titan-*)
/// - Meta Llama models (meta.llama-*)
/// - Mistral models (mistral.*)
/// - Cohere models (cohere.*)
pub struct BedrockProvider {
    client: Client,
    region: String,
    /// Cache of AWS SDK clients keyed by access key ID.
    /// Clients are cached to reuse connection pools across requests.
    /// Each entry includes a creation timestamp for TTL expiration.
    sdk_client_cache: Arc<RwLock<HashMap<String, CachedClient>>>,
}

impl BedrockProvider {
    /// Create a new Bedrock provider with default settings.
    pub fn new() -> Self {
        Self::with_timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
    }

    /// Create a new Bedrock provider with a custom timeout.
    pub fn with_timeout(timeout: Duration) -> Self {
        Self::with_region_and_timeout("us-east-1".to_string(), timeout)
    }

    /// Create with custom region and timeout.
    pub fn with_region_and_timeout(region: String, timeout: Duration) -> Self {
        Self {
            client: create_http_client(timeout),
            region,
            sdk_client_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Extract the actual model ID from gateway model string.
    ///
    /// Gateway format: `bedrock/anthropic.claude-3-sonnet-20240229-v1:0`
    /// Bedrock format: `anthropic.claude-3-sonnet-20240229-v1:0`
    fn extract_model_id(&self, model: &str) -> String {
        model.strip_prefix("bedrock/").unwrap_or(model).to_string()
    }

    /// Map model aliases to full Bedrock model IDs.
    fn map_model_name(&self, model: &str) -> String {
        let base = self.extract_model_id(model);

        match base.as_str() {
            // Claude aliases
            "claude-sonnet-4-6" => "anthropic.claude-sonnet-4-6-v1:0".to_string(),
            "claude-opus-4-6" => "anthropic.claude-opus-4-6-v1:0".to_string(),
            "claude-haiku-4-5" => "anthropic.claude-haiku-4-5-20251001-v1:0".to_string(),
            "claude-sonnet-4" => "anthropic.claude-sonnet-4-20250514-v1:0".to_string(),
            // Llama aliases
            "llama-3-70b" => "meta.llama3-70b-instruct-v1:0".to_string(),
            "llama-3-8b" => "meta.llama3-8b-instruct-v1:0".to_string(),
            // Pass through if already fully qualified
            _ => base,
        }
    }

    /// Convert JSON to Smithy [`Document`].
    ///
    /// Requires `--cfg aws_sdk_unstable` (set in the workspace `.cargo/config.toml`) so
    /// `aws_smithy_types::Document` implements `serde::Deserialize`.
    fn json_value_to_document(value: serde_json::Value) -> Document {
        serde_json::from_value(value).unwrap_or_else(|e| {
            tracing::warn!("Failed to convert JSON to AWS Document, falling back to null: {e}");
            Document::default()
        })
    }

    //todo doesn't axum already have this functionality?
    /// Parse a base64 data URL into raw bytes and MIME type.
    ///
    /// Accepts `data:<mime>;base64,<data>` format. Returns `None` if the URL is invalid.
    fn parse_data_url(url: &str) -> Option<(Vec<u8>, String)> {
        if !url.starts_with("data:") {
            return None;
        }
        let parts: Vec<&str> = url.splitn(2, ',').collect();
        if parts.len() != 2 {
            return None;
        }
        let header = parts[0];
        let data = parts[1];
        let mime_type = header
            .strip_prefix("data:")?
            .split(';')
            .next()
            .unwrap_or("application/octet-stream")
            .to_string();
        let bytes =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, data).ok()?;
        Some((bytes, mime_type))
    }

    /// Map a MIME type string to a Bedrock `ImageFormat`.
    fn mime_to_image_format(mime: &str) -> Option<ImageFormat> {
        match mime {
            "image/jpeg" | "image/jpg" => Some(ImageFormat::Jpeg),
            "image/png" => Some(ImageFormat::Png),
            "image/gif" => Some(ImageFormat::Gif),
            "image/webp" => Some(ImageFormat::Webp),
            _ => None,
        }
    }

    /// Map a MIME type string to a Bedrock `DocumentFormat`.
    fn mime_to_document_format(mime: &str) -> Option<DocumentFormat> {
        match mime {
            "application/pdf" => Some(DocumentFormat::Pdf),
            "text/csv" => Some(DocumentFormat::Csv),
            "application/msword" => Some(DocumentFormat::Doc),
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document" => {
                Some(DocumentFormat::Docx)
            }
            "text/html" => Some(DocumentFormat::Html),
            "text/markdown" | "text/x-markdown" => Some(DocumentFormat::Md),
            "text/plain" => Some(DocumentFormat::Txt),
            "application/vnd.ms-excel" => Some(DocumentFormat::Xls),
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" => {
                Some(DocumentFormat::Xlsx)
            }
            _ => None,
        }
    }

    /// Convert OpenAI messages to Bedrock Converse format.
    fn convert_messages(
        &self,
        messages: &[ChatMessage],
    ) -> (Option<Vec<BedrockSystemContent>>, Vec<BedrockMessage>) {
        let mut system_prompts: Vec<BedrockSystemContent> = Vec::new();
        let mut bedrock_messages: Vec<BedrockMessage> = Vec::new();

        for msg in messages {
            match msg.role {
                MessageRole::System => {
                    if let Some(content) = &msg.content {
                        system_prompts.push(BedrockSystemContent {
                            text: content.as_text(),
                        });
                    }
                }
                MessageRole::User => {
                    let content_blocks: Vec<BedrockContentBlock> = match &msg.content {
                        None | Some(MessageContent::Text(_)) => {
                            let text = msg.content.as_ref()
                                .map(|c| c.as_text())
                                .unwrap_or_default();
                            vec![BedrockContentBlock::Text { text }]
                        }
                        Some(MessageContent::Parts(parts)) => {
                            parts.iter().filter_map(|part| {
                                match part {
                                    ContentPart::Text { text } => {
                                        Some(BedrockContentBlock::Text { text: text.clone() })
                                    }
                                    ContentPart::ImageUrl { image_url } => {
                                        let url = &image_url.url;
                                        if let Some((bytes, mime)) = Self::parse_data_url(url) {
                                            let format_str = match mime.as_str() {
                                                "image/jpeg" | "image/jpg" => "jpeg",
                                                "image/png" => "png",
                                                "image/gif" => "gif",
                                                "image/webp" => "webp",
                                                _ => {
                                                    tracing::warn!(mime, "Unsupported image MIME type for Bedrock, skipping");
                                                    return None;
                                                }
                                            };
                                            let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
                                            Some(BedrockContentBlock::Image {
                                                image: BedrockImageContent {
                                                    format: format_str.to_string(),
                                                    source: BedrockImageSource { bytes: b64 },
                                                },
                                            })
                                        } else {
                                            tracing::warn!(url_prefix = %url.chars().take(30).collect::<String>(), "Invalid or unsupported image URL for Bedrock, skipping");
                                            None
                                        }
                                    }
                                    ContentPart::DocumentUrl { document_url } => {
                                        let url = &document_url.url;
                                        if let Some((bytes, mime)) = Self::parse_data_url(url) {
                                            let (format_str, default_name) = match mime.as_str() {
                                                "application/pdf" => ("pdf", "document.pdf"),
                                                "text/csv" => ("csv", "document.csv"),
                                                "application/msword" => ("doc", "document.doc"),
                                                "application/vnd.openxmlformats-officedocument.wordprocessingml.document" => ("docx", "document.docx"),
                                                "text/html" => ("html", "document.html"),
                                                "text/markdown" | "text/x-markdown" => ("md", "document.md"),
                                                "text/plain" => ("txt", "document.txt"),
                                                "application/vnd.ms-excel" => ("xls", "document.xls"),
                                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" => ("xlsx", "document.xlsx"),
                                                _ => {
                                                    tracing::warn!(mime, "Unsupported document MIME type for Bedrock, skipping");
                                                    return None;
                                                }
                                            };
                                            let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
                                            let name = document_url.filename.clone()
                                                .unwrap_or_else(|| default_name.to_string());
                                            Some(BedrockContentBlock::Document {
                                                document: BedrockDocumentContent {
                                                    format: format_str.to_string(),
                                                    name,
                                                    source: BedrockDocumentSource { bytes: b64 },
                                                },
                                            })
                                        } else {
                                            tracing::warn!(url_prefix = %url.chars().take(30).collect::<String>(), "Invalid or unsupported document URL for Bedrock, skipping");
                                            None
                                        }
                                    }
                                }
                            }).collect()
                        }
                    };

                    if !content_blocks.is_empty() {
                        bedrock_messages.push(BedrockMessage {
                            role: "user".to_string(),
                            content: content_blocks,
                        });
                    }
                }
                MessageRole::Assistant => {
                    let mut content_blocks: Vec<BedrockContentBlock> =
                        Vec::with_capacity(msg.content.iter().len() + msg.tool_calls.iter().len());

                    // Add text content if present
                    if let Some(ref content) = msg.content {
                        let text = content.as_text();
                        if !text.is_empty() {
                            content_blocks.push(BedrockContentBlock::Text { text });
                        }
                    }

                    // Add tool use blocks for tool_calls
                    if let Some(ref tool_calls) = msg.tool_calls {
                        for tc in tool_calls {
                            let input = serde_json::from_str(&tc.function.arguments)
                                .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));

                            content_blocks.push(BedrockContentBlock::ToolUse {
                                tool_use: BedrockToolUse {
                                    tool_use_id: tc.id.clone(),
                                    name: tc.function.name.clone(),
                                    input,
                                },
                            });
                        }
                    }

                    if !content_blocks.is_empty() {
                        bedrock_messages.push(BedrockMessage {
                            role: "assistant".to_string(),
                            content: content_blocks,
                        });
                    }
                }
                MessageRole::Tool | MessageRole::Other => {
                    if let Some(ref tool_call_id) = msg.tool_call_id {
                        let content_text = msg
                            .content
                            .as_ref()
                            .map(|c| c.as_text())
                            .unwrap_or_default();

                        let tool_result = BedrockContentBlock::ToolResult {
                            tool_result: BedrockToolResult {
                                tool_use_id: tool_call_id.clone(),
                                content: vec![BedrockToolResultContent::Text {
                                    text: content_text,
                                }],
                                status: None,
                            },
                        };

                        // Check if the last message is a user message with tool results
                        // If so, append to it (Bedrock expects all tool results in one user message)
                        let should_append = bedrock_messages
                            .last()
                            .map(|last| {
                                last.role == "user"
                                    && last.content.iter().any(|b| {
                                        matches!(b, BedrockContentBlock::ToolResult { .. })
                                    })
                            })
                            .unwrap_or(false);

                        if should_append {
                            if let Some(last_msg) = bedrock_messages.last_mut() {
                                last_msg.content.push(tool_result);
                            }
                        } else {
                            bedrock_messages.push(BedrockMessage {
                                role: "user".to_string(),
                                content: vec![tool_result],
                            });
                        }
                    } else {
                        tracing::warn!(
                            role = ?msg.role,
                            "Unknown message role, treating as 'user'. Supported roles: user, assistant, system, tool"
                        );
                        let text = msg
                            .content
                            .as_ref()
                            .map(|c| c.as_text())
                            .unwrap_or_default();

                        bedrock_messages.push(BedrockMessage {
                            role: "user".to_string(),
                            content: vec![BedrockContentBlock::Text { text }],
                        });
                    }
                }
            }
        }

        let system = if system_prompts.is_empty() {
            None
        } else {
            Some(system_prompts)
        };

        (system, bedrock_messages)
    }

    /// Convert OpenAI tools to Bedrock tool config.
    fn convert_tools(
        &self,
        tools: &Option<Vec<crate::gateway::types::Tool>>,
    ) -> Option<BedrockToolConfig> {
        tools
            .as_ref()
            .map(|tools| {
                let bedrock_tools: Vec<BedrockToolSpec> = tools
                    .iter()
                    .filter(|t| t.tool_type == ToolType::Function)
                    .map(|t| BedrockToolSpec {
                        tool_spec: BedrockToolSpecInner {
                            name: t.function.name.clone(),
                            description: t.function.description.clone().unwrap_or_default(),
                            input_schema: BedrockToolInputSchema {
                                json: t.function.parameters.clone().unwrap_or(serde_json::json!({
                                    "type": "object",
                                    "properties": {}
                                })),
                            },
                        },
                    })
                    .collect();

                BedrockToolConfig {
                    tools: bedrock_tools,
                }
            })
            .filter(|config| !config.tools.is_empty())
    }

    /// Convert Bedrock response to OpenAI format.
    fn convert_response(
        &self,
        response: BedrockConverseResponse,
        model: &str,
    ) -> ChatCompletionResponse {
        // Extract text content
        let content: String = response
            .output
            .message
            .content
            .iter()
            .filter_map(|block| {
                if let BedrockContentBlock::Text { text } = block {
                    Some(text.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join("");

        // Extract tool use blocks and convert to OpenAI tool_calls format
        let tool_calls: Vec<ToolCall> = response
            .output
            .message
            .content
            .iter()
            .filter_map(|block| {
                if let BedrockContentBlock::ToolUse { tool_use } = block {
                    Some(ToolCall {
                        index: None,
                        id: tool_use.tool_use_id.clone(),
                        tool_type: ToolType::Function,
                        function: crate::gateway::types::FunctionCall {
                            name: tool_use.name.clone(),
                            arguments: serde_json::to_string(&tool_use.input).unwrap_or_default(),
                        },
                    })
                } else {
                    None
                }
            })
            .collect();

        let finish_reason = if !tool_calls.is_empty() {
            FinishReason::ToolCalls
        } else {
            map_finish_reason_to_openai(&response.stop_reason, Provider::Bedrock)
        };

        ChatCompletionResponse::new(
            format!("bedrock-{}", uuid::Uuid::new_v4()),
            model.to_string(),
            vec![Choice {
                index: 0,
                message: AssistantMessage {
                    role: MessageRole::Assistant,
                    content: (!content.is_empty()).then_some(content),
                    tool_calls: (!tool_calls.is_empty()).then_some(tool_calls),
                    thinking: None,
                },
                finish_reason,
                logprobs: None,
            }],
            Usage {
                prompt_tokens: response.usage.input_tokens,
                completion_tokens: response.usage.output_tokens,
                total_tokens: response.usage.total_tokens,
                thinking_tokens: None,
                completion_tokens_details: None,
                prompt_tokens_details: None,
            },
        )
    }

    /// Parse AWS credentials from the API key format.
    ///
    /// # Supported Format
    ///
    /// **JSON format:**
    /// ```json
    /// {"access_key_id": "AKIAIOSFODNN7EXAMPLE", "secret_access_key": "wJalrXUtnFEMI/...", "session_token": "optional"}
    /// ```
    ///
    /// Also supports AWS SDK naming conventions with `aws_` prefix:
    /// ```json
    /// {"aws_access_key_id": "...", "aws_secret_access_key": "...", "aws_session_token": "..."}
    /// ```
    fn parse_credentials(api_key: &str) -> Result<AwsCredentials, GatewayError> {
        let api_key = api_key.trim();

        // Support both standard and AWS SDK naming conventions
        #[derive(Deserialize)]
        struct AwsCredentialsJson {
            aws_access_key_id: Option<String>,
            aws_secret_access_key: Option<String>,
            aws_session_token: Option<String>,
            access_key_id: Option<String>,
            secret_access_key: Option<String>,
            session_token: Option<String>,
        }

        let creds: AwsCredentialsJson = serde_json::from_str(api_key).map_err(|e| {
            tracing::warn!(
                error = %e,
                "Failed to parse AWS credentials as JSON"
            );
            GatewayError::AuthenticationFailed(
                "Invalid AWS credentials format. Expected JSON: {\"access_key_id\": \"...\", \"secret_access_key\": \"...\", \"session_token\": \"...\"}".to_string(),
            )
        })?;

        // Try aws_ prefixed fields first, then fall back to non-prefixed
        let access_key_id = creds
            .aws_access_key_id
            .or(creds.access_key_id)
            .ok_or_else(|| {
                GatewayError::AuthenticationFailed(
                    "Missing access_key_id in AWS credentials JSON".to_string(),
                )
            })?;

        let secret_access_key = creds
            .aws_secret_access_key
            .or(creds.secret_access_key)
            .ok_or_else(|| {
                GatewayError::AuthenticationFailed(
                    "Missing secret_access_key in AWS credentials JSON".to_string(),
                )
            })?;

        let session_token = creds.aws_session_token.or(creds.session_token);

        Ok(AwsCredentials {
            access_key_id,
            secret_access_key,
            session_token,
        })
    }

    /// Get or create an AWS SDK Bedrock client from credentials.
    ///
    /// This is used for streaming requests which require the AWS SDK to properly
    /// handle the binary event stream format.
    ///
    /// Clients are cached by access key ID to reuse connection pools across requests.
    /// The cache is limited to MAX_CACHED_CLIENTS to prevent unbounded memory growth.
    /// Clients expire after CLIENT_CACHE_TTL_SECS to handle credential rotation.
    fn get_or_create_sdk_client(&self, credentials: &AwsCredentials) -> BedrockClient {
        let cache_key = credentials.access_key_id.clone();
        let ttl = std::time::Duration::from_secs(CLIENT_CACHE_TTL_SECS);

        // Try to get from cache first (read lock)
        {
            let cache = self.sdk_client_cache.read();
            if let Some(cached) = cache.get(&cache_key) {
                // Check if the cached client is still valid (not expired)
                if cached.created_at.elapsed() < ttl {
                    return cached.client.clone();
                }
                // Client is expired, will recreate below
                tracing::debug!(
                    access_key_prefix = %&cache_key[..cache_key.len().min(8)],
                    "Bedrock SDK client cache entry expired, recreating"
                );
            }
        }

        // Create new client (write lock)
        let mut cache = self.sdk_client_cache.write();

        // Double-check after acquiring write lock (another thread may have inserted a fresh client)
        if let Some(cached) = cache.get(&cache_key) {
            if cached.created_at.elapsed() < ttl {
                return cached.client.clone();
            }
        }

        // Evict expired entries and oldest entries if cache is full
        // First, remove all expired entries
        let now = std::time::Instant::now();
        cache.retain(|_, cached| now.duration_since(cached.created_at) < ttl);

        // If still at capacity, evict some entries
        if cache.len() >= MAX_CACHED_CLIENTS {
            // Remove about 10% of entries to avoid frequent evictions
            let to_remove = cache.len() / 10 + 1;
            let keys_to_remove: Vec<String> = cache.keys().take(to_remove).cloned().collect();
            for key in keys_to_remove {
                cache.remove(&key);
            }
            tracing::debug!(
                evicted = to_remove,
                cache_size = cache.len(),
                "Evicted Bedrock SDK clients from cache"
            );
        }

        // Create and cache the new client
        let aws_creds = Credentials::new(
            &credentials.access_key_id,
            &credentials.secret_access_key,
            credentials.session_token.clone(),
            None, // expiry
            "reiver-gateway",
        );

        let config = aws_sdk_bedrockruntime::Config::builder()
            .region(Region::new(self.region.clone()))
            .credentials_provider(aws_creds)
            .build();

        let client = BedrockClient::from_conf(config);
        let cached = CachedClient {
            client: client.clone(),
            created_at: std::time::Instant::now(),
        };
        cache.insert(cache_key, cached);

        client
    }

    /// Convert OpenAI messages to AWS SDK Message types.
    fn convert_messages_to_sdk(
        &self,
        messages: &[ChatMessage],
    ) -> (Vec<SystemContentBlock>, Vec<Message>) {
        let mut system_blocks: Vec<SystemContentBlock> = Vec::new();
        let mut sdk_messages: Vec<Message> = Vec::new();

        for msg in messages {
            match msg.role {
                MessageRole::System => {
                    if let Some(content) = &msg.content {
                        system_blocks.push(SystemContentBlock::Text(content.as_text()));
                    }
                }
                MessageRole::User => {
                    let mut builder = Message::builder().role(ConversationRole::User);
                    let has_parts = matches!(&msg.content, Some(MessageContent::Parts(_)));
                    if has_parts {
                        if let Some(MessageContent::Parts(parts)) = &msg.content {
                            for part in parts {
                                match part {
                                    ContentPart::Text { text } => {
                                        builder = builder.content(ContentBlock::Text(text.clone()));
                                    }
                                    ContentPart::ImageUrl { image_url } => {
                                        if let Some((bytes, mime)) =
                                            Self::parse_data_url(&image_url.url)
                                        {
                                            if let Some(fmt) = Self::mime_to_image_format(&mime) {
                                                let blob = aws_smithy_types::Blob::new(bytes);
                                                if let Ok(img) = ImageBlock::builder()
                                                    .format(fmt)
                                                    .source(ImageSource::Bytes(blob))
                                                    .build()
                                                {
                                                    builder =
                                                        builder.content(ContentBlock::Image(img));
                                                }
                                            } else {
                                                tracing::warn!(mime, "Unsupported image MIME type for Bedrock SDK, skipping");
                                            }
                                        } else {
                                            tracing::warn!(
                                                "Invalid image data URL for Bedrock SDK, skipping"
                                            );
                                        }
                                    }
                                    ContentPart::DocumentUrl { document_url } => {
                                        if let Some((bytes, mime)) =
                                            Self::parse_data_url(&document_url.url)
                                        {
                                            let effective_mime =
                                                document_url.media_type.as_deref().unwrap_or(&mime);
                                            if let Some(fmt) =
                                                Self::mime_to_document_format(effective_mime)
                                            {
                                                let blob = aws_smithy_types::Blob::new(bytes);
                                                let name = document_url
                                                    .filename
                                                    .clone()
                                                    .unwrap_or_else(|| "document".to_string());
                                                if let Ok(doc) = DocumentBlock::builder()
                                                    .format(fmt)
                                                    .name(name)
                                                    .source(DocumentSource::Bytes(blob))
                                                    .build()
                                                {
                                                    builder = builder
                                                        .content(ContentBlock::Document(doc));
                                                }
                                            } else {
                                                tracing::warn!(mime, "Unsupported document MIME type for Bedrock SDK, skipping");
                                            }
                                        } else {
                                            tracing::warn!("Invalid document data URL for Bedrock SDK, skipping");
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        let text = msg
                            .content
                            .as_ref()
                            .map(|c| c.as_text())
                            .unwrap_or_default();
                        builder = builder.content(ContentBlock::Text(text));
                    }

                    if let Ok(message) = builder.build() {
                        sdk_messages.push(message);
                    }
                }
                MessageRole::Assistant => {
                    let mut builder = Message::builder().role(ConversationRole::Assistant);

                    // Add text content if present
                    if let Some(ref content) = msg.content {
                        let text = content.as_text();
                        if !text.is_empty() {
                            builder = builder.content(ContentBlock::Text(text));
                        }
                    }

                    // Add tool use blocks for tool_calls
                    if let Some(ref tool_calls) = msg.tool_calls {
                        for tc in tool_calls {
                            let args_json: serde_json::Value =
                                serde_json::from_str(&tc.function.arguments)
                                    .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));
                            let input = Self::json_value_to_document(args_json);

                            let tool_use = ToolUseBlock::builder()
                                .tool_use_id(&tc.id)
                                .name(&tc.function.name)
                                .input(input)
                                .build()
                                .ok();

                            if let Some(tu) = tool_use {
                                builder = builder.content(ContentBlock::ToolUse(tu));
                            }
                        }
                    }

                    if let Ok(message) = builder.build() {
                        sdk_messages.push(message);
                    }
                }
                MessageRole::Tool | MessageRole::Other => {
                    if let Some(ref tool_call_id) = msg.tool_call_id {
                        let content_text = msg
                            .content
                            .as_ref()
                            .map(|c| c.as_text())
                            .unwrap_or_default();

                        let tool_result = ToolResultBlock::builder()
                            .tool_use_id(tool_call_id)
                            .content(ToolResultContentBlock::Text(content_text.clone()))
                            .status(ToolResultStatus::Success)
                            .build()
                            .ok();

                        if let Some(tr) = tool_result {
                            // Check if the last message is a user message with tool results
                            let should_append = sdk_messages
                                .last()
                                .map(|last| {
                                    last.role() == &ConversationRole::User
                                        && last
                                            .content()
                                            .iter()
                                            .any(|b| matches!(b, ContentBlock::ToolResult(_)))
                                })
                                .unwrap_or(false);

                            if should_append {
                                // We can't easily modify SDK messages, so just add a new one
                                if let Ok(message) = Message::builder()
                                    .role(ConversationRole::User)
                                    .content(ContentBlock::ToolResult(tr))
                                    .build()
                                {
                                    // Merge with previous by rebuilding
                                    if let Some(prev) = sdk_messages.pop() {
                                        if let Ok(tool_result) = ToolResultBlock::builder()
                                            .tool_use_id(tool_call_id)
                                            .content(ToolResultContentBlock::Text(
                                                content_text.clone(),
                                            ))
                                            .status(ToolResultStatus::Success)
                                            .build()
                                        {
                                            let mut new_builder =
                                                Message::builder().role(ConversationRole::User);
                                            for content in prev.content() {
                                                new_builder = new_builder.content(content.clone());
                                            }
                                            new_builder = new_builder
                                                .content(ContentBlock::ToolResult(tool_result));
                                            if let Ok(merged) = new_builder.build() {
                                                sdk_messages.push(merged);
                                            } else {
                                                sdk_messages.push(prev);
                                                sdk_messages.push(message);
                                            }
                                        } else {
                                            tracing::warn!(
                                                tool_call_id = %tool_call_id,
                                                "Failed to build Bedrock ToolResultBlock — skipping tool result merge"
                                            );
                                            sdk_messages.push(prev);
                                            sdk_messages.push(message);
                                        }
                                    }
                                }
                            } else {
                                if let Ok(message) = Message::builder()
                                    .role(ConversationRole::User)
                                    .content(ContentBlock::ToolResult(tr))
                                    .build()
                                {
                                    sdk_messages.push(message);
                                }
                            }
                        }
                    } else {
                        tracing::warn!(
                            role = ?msg.role,
                            "Unknown message role, treating as 'user'. Supported roles: user, assistant, system, tool"
                        );
                        let text = msg
                            .content
                            .as_ref()
                            .map(|c| c.as_text())
                            .unwrap_or_default();

                        if let Ok(message) = Message::builder()
                            .role(ConversationRole::User)
                            .content(ContentBlock::Text(text))
                            .build()
                        {
                            sdk_messages.push(message);
                        }
                    }
                }
            }
        }

        (system_blocks, sdk_messages)
    }

    /// Convert OpenAI tools to AWS SDK ToolConfiguration.
    fn convert_tools_to_sdk(
        &self,
        tools: &Option<Vec<crate::gateway::types::Tool>>,
    ) -> Option<ToolConfiguration> {
        tools.as_ref().and_then(|tools| {
            let sdk_tools: Vec<SdkTool> = tools
                .iter()
                .filter(|t| t.tool_type == ToolType::Function)
                .filter_map(|t| {
                    let schema = t
                        .function
                        .parameters
                        .clone()
                        .map(|p| Self::json_value_to_document(p))
                        .unwrap_or(aws_smithy_types::Document::Object(
                            std::collections::HashMap::new(),
                        ));

                    let tool_spec = ToolSpecification::builder()
                        .name(&t.function.name)
                        .description(t.function.description.clone().unwrap_or_default())
                        .input_schema(ToolInputSchema::Json(schema))
                        .build()
                        .ok()?;

                    Some(SdkTool::ToolSpec(tool_spec))
                })
                .collect();

            if sdk_tools.is_empty() {
                None
            } else {
                ToolConfiguration::builder()
                    .set_tools(Some(sdk_tools))
                    .build()
                    .ok()
            }
        })
    }

    /// Sign an AWS request using Signature Version 4.
    ///
    /// Returns a SignedRequest containing the authorization header and other headers.
    fn sign_request(
        &self,
        credentials: &AwsCredentials,
        uri_path: &str,
        body: &[u8],
    ) -> SignedRequest {
        let datetime = chrono::Utc::now();
        let date_str = datetime.format("%Y%m%d").to_string();
        let datetime_str = datetime.format("%Y%m%dT%H%M%SZ").to_string();

        let host = format!("bedrock-runtime.{}.amazonaws.com", self.region);
        let payload_hash = sha256_hex(body);

        // Build canonical headers based on whether we have a session token
        let (signed_headers, canonical_headers) = if let Some(ref token) = credentials.session_token
        {
            (
                "content-type;host;x-amz-date;x-amz-security-token".to_string(),
                format!(
                    "content-type:application/json\nhost:{}\nx-amz-date:{}\nx-amz-security-token:{}\n",
                    host, datetime_str, token
                )
            )
        } else {
            (
                "content-type;host;x-amz-date".to_string(),
                format!(
                    "content-type:application/json\nhost:{}\nx-amz-date:{}\n",
                    host, datetime_str
                ),
            )
        };

        // Create canonical request
        let canonical_request = format!(
            "POST\n{}\n\n{}\n{}\n{}",
            uri_path, canonical_headers, signed_headers, payload_hash
        );

        // Create string to sign
        let credential_scope = format!("{}/{}/bedrock/aws4_request", date_str, self.region);
        let canonical_request_hash = sha256_hex(canonical_request.as_bytes());
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            datetime_str, credential_scope, canonical_request_hash
        );

        // Calculate signing key
        let k_date = hmac_sha256(
            format!("AWS4{}", credentials.secret_access_key).as_bytes(),
            date_str.as_bytes(),
        );
        let k_region = hmac_sha256(&k_date, self.region.as_bytes());
        let k_service = hmac_sha256(&k_region, b"bedrock");
        let k_signing = hmac_sha256(&k_service, b"aws4_request");

        // Calculate signature
        let signature = hex::encode(hmac_sha256(&k_signing, string_to_sign.as_bytes()));

        // Build authorization header
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            credentials.access_key_id, credential_scope, signed_headers, signature
        );

        SignedRequest {
            host,
            datetime_str,
            authorization,
            session_token: credentials.session_token.clone(),
        }
    }
}

/// Parsed AWS credentials.
struct AwsCredentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

/// Result of signing an AWS request with SigV4.
struct SignedRequest {
    host: String,
    datetime_str: String,
    authorization: String,
    session_token: Option<String>,
}

/// Map an AWS SDK `ConverseStreamError` to the appropriate `GatewayError`.
fn map_converse_stream_error(err: &ConverseStreamError) -> GatewayError {
    use super::common::record_otel_error;

    let message = err
        .meta()
        .message()
        .unwrap_or("Unknown Bedrock error")
        .to_string();

    record_otel_error(&format!("bedrock returned error: {message}"));

    if err.is_throttling_exception() {
        return GatewayError::RateLimitExceeded {
            limit: 0,
            reset_seconds: 30,
        };
    }

    if err.is_access_denied_exception() {
        return GatewayError::AuthenticationFailed(message);
    }

    if err.is_model_timeout_exception() {
        return GatewayError::Timeout(message);
    }

    let status = if err.is_validation_exception() {
        400
    } else if err.is_resource_not_found_exception() {
        404
    } else if err.is_service_unavailable_exception() || err.is_model_not_ready_exception() {
        503
    } else {
        500
    };

    GatewayError::ProviderError {
        provider: Provider::Bedrock,
        status,
        message,
    }
}

impl Default for BedrockProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl LlmProvider for BedrockProvider {
    fn name(&self) -> Provider {
        Provider::Bedrock
    }

    fn supports_model(&self, model: &str) -> bool {
        // Explicit bedrock/ prefix
        model.starts_with("bedrock/")
            // Or Bedrock model IDs
            || model.starts_with("anthropic.")
            || model.starts_with("amazon.")
            || model.starts_with("meta.")
            || model.starts_with("mistral.")
            || model.starts_with("cohere.")
            || model.starts_with("ai21.")
    }

    #[tracing::instrument(
        name = "provider.bedrock.chat_completion",
        skip(self, request, api_key),
        fields(
            model = %request.model,
            message_count = request.messages.len(),
            input_tokens = tracing::field::Empty,
            output_tokens = tracing::field::Empty,
            total_tokens = tracing::field::Empty,
            finish_reason = tracing::field::Empty,
            http_status = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
            otel.status_message = tracing::field::Empty,
            gen_ai.provider.name = "aws.bedrock",
            gen_ai.operation.name = "chat",
            gen_ai.request.model = %request.model,
            gen_ai.response.model = tracing::field::Empty,
            gen_ai.usage.input_tokens = tracing::field::Empty,
            gen_ai.usage.output_tokens = tracing::field::Empty,
            gen_ai.response.finish_reasons = tracing::field::Empty,
        )
    )]
    async fn chat_completion(
        &self,
        request: &ChatCompletionRequest,
        api_key: &str,
    ) -> Result<ChatCompletionResponse, GatewayError> {
        let credentials = Self::parse_credentials(api_key)?;

        let model_id = self.map_model_name(&request.model);
        let url = format!(
            "https://bedrock-runtime.{}.amazonaws.com/model/{}/converse",
            self.region,
            urlencoding::encode(&model_id)
        );

        let (system, messages) = self.convert_messages(&request.messages);

        let inference_config = BedrockInferenceConfig {
            max_tokens: request.max_tokens,
            temperature: request.temperature,
            top_p: request.top_p,
            stop_sequences: request.stop.as_ref().map(|s| match s {
                crate::gateway::types::StopSequence::Single(s) => vec![s.clone()],
                crate::gateway::types::StopSequence::Multiple(v) => v.clone(),
            }),
        };

        let bedrock_request = BedrockConverseRequest {
            messages,
            system,
            inference_config: Some(inference_config),
            tool_config: self.convert_tools(&request.tools),
        };

        let body = serde_json::to_vec(&bedrock_request).map_err(|e| {
            GatewayError::InternalError(format!("Failed to serialize request: {}", e))
        })?;

        let uri = format!("/model/{}/converse", urlencoding::encode(&model_id));
        let signed = self.sign_request(&credentials, &uri, &body);

        let mut req = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("Host", &signed.host)
            .header("X-Amz-Date", &signed.datetime_str)
            .header("Authorization", &signed.authorization);

        if let Some(ref token) = signed.session_token {
            req = req.header("X-Amz-Security-Token", token);
        }

        let response = req.body(body).send().await?;

        let status = response.status();
        tracing::Span::current().record("http_status", status.as_u16());

        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(parse_provider_error(
                &error_text,
                Provider::Bedrock,
                status.as_u16(),
            ));
        }

        let bedrock_response: BedrockConverseResponse = response
            .json()
            .await
            .map_err(|e| GatewayError::InternalError(format!("Failed to parse response: {}", e)))?;

        let result = self.convert_response(bedrock_response, &request.model);
        let span = tracing::Span::current();
        span.record("gen_ai.response.model", result.model.as_str());
        span.record("input_tokens", result.usage.prompt_tokens as u64);
        span.record("output_tokens", result.usage.completion_tokens as u64);
        span.record("total_tokens", result.usage.total_tokens as u64);
        span.record(
            "gen_ai.usage.input_tokens",
            result.usage.prompt_tokens as i64,
        );
        span.record(
            "gen_ai.usage.output_tokens",
            result.usage.completion_tokens as i64,
        );
        if let Some(choice) = result.choices.first() {
            span.record("finish_reason", choice.finish_reason.as_str());
            span.record(
                "gen_ai.response.finish_reasons",
                choice.finish_reason.as_str(),
            );
        }
        Ok(result)
    }

    #[tracing::instrument(
        name = "provider.bedrock.stream_chat_completion",
        skip(self, request, api_key),
        fields(
            model = %request.model,
            otel.status_code = tracing::field::Empty,
            otel.status_message = tracing::field::Empty,
            gen_ai.provider.name = "aws.bedrock",
            gen_ai.operation.name = "chat",
            gen_ai.request.model = %request.model,
        )
    )]
    async fn stream_chat_completion(
        &self,
        request: &ChatCompletionRequest,
        api_key: &str,
    ) -> Result<ChatCompletionStream, GatewayError> {
        // Parse AWS credentials and get cached SDK client
        let credentials = Self::parse_credentials(api_key)?;
        let client = self.get_or_create_sdk_client(&credentials);

        let model_id = self.map_model_name(&request.model);
        let (system_blocks, sdk_messages) = self.convert_messages_to_sdk(&request.messages);

        // Build the converse_stream request using the AWS SDK
        let mut stream_request = client.converse_stream().model_id(&model_id);

        // Add system prompts
        for system_block in system_blocks {
            stream_request = stream_request.system(system_block);
        }

        // Add messages
        for message in sdk_messages {
            stream_request = stream_request.messages(message);
        }

        // Add inference configuration
        let mut inference_config = aws_sdk_bedrockruntime::types::InferenceConfiguration::builder();
        if let Some(max_tokens) = request.max_tokens {
            inference_config = inference_config.max_tokens(max_tokens as i32);
        }
        if let Some(temp) = request.temperature {
            inference_config = inference_config.temperature(temp);
        }
        if let Some(top_p) = request.top_p {
            inference_config = inference_config.top_p(top_p);
        }
        if let Some(stop) = &request.stop {
            let stop_seqs: Vec<String> = match stop {
                crate::gateway::types::StopSequence::Single(s) => vec![s.clone()],
                crate::gateway::types::StopSequence::Multiple(v) => v.clone(),
            };
            for seq in stop_seqs {
                inference_config = inference_config.stop_sequences(seq);
            }
        }
        stream_request = stream_request.inference_config(inference_config.build());

        // Add tool configuration if provided
        if let Some(tool_config) = self.convert_tools_to_sdk(&request.tools) {
            stream_request = stream_request.tool_config(tool_config);
        }

        // Send the request
        let response = stream_request.send().await.map_err(|e| {
            tracing::error!(
                model_id = %model_id,
                error = %e,
                "Bedrock converse_stream request failed"
            );
            if let Some(service_err) = e.as_service_error() {
                return map_converse_stream_error(service_err);
            }
            GatewayError::NetworkError(format!("Bedrock streaming request failed: {e}"))
        })?;

        // Get the event stream from the response
        let mut event_stream = response.stream;

        // Capture model for use in stream
        let model = request.model.clone();
        let chunk_id = format!("chatcmpl-{}", uuid::Uuid::new_v4());

        // Convert AWS SDK events to OpenAI-compatible chunks using async_stream
        let chunk_stream = async_stream::stream! {
            let mut sent_role = false;
            let mut captured_usage: Option<Usage> = None;
            let mut tool_call_index: u32 = 0;
            let mut current_tool_index: u32 = 0;

            loop {
                match event_stream.recv().await {
                    Ok(Some(event)) => {
                        match event {
                            ConverseStreamOutput::ContentBlockStart(start_event) => {
                                if let Some(start) = start_event.start {
                                    match start {
                                        aws_sdk_bedrockruntime::types::ContentBlockStart::ToolUse(tool_start) => {
                                            // Emit role header if not yet sent
                                            if !sent_role {
                                                sent_role = true;
                                                yield Ok(ChatCompletionChunk::new(
                                                    chunk_id.clone(),
                                                    model.clone(),
                                                    vec![ChunkChoice {
                                                        index: 0,
                                                        delta: ChunkDelta {
                                                            role: Some(MessageRole::Assistant),
                                                            ..Default::default()
                                                        },
                                                        finish_reason: None,
                                                    }],
                                                ));
                                            }
                                            // Emit tool-call start chunk with id and name
                                            current_tool_index = tool_call_index;
                                            tool_call_index += 1;
                                            yield Ok(ChatCompletionChunk::new(
                                                chunk_id.clone(),
                                                model.clone(),
                                                vec![ChunkChoice {
                                                    index: 0,
                                                    delta: ChunkDelta {
                                                        tool_calls: Some(vec![ToolCall {
                                                            index: Some(current_tool_index),
                                                            id: tool_start.tool_use_id.clone(),
                                                            tool_type: ToolType::Function,
                                                            function: crate::gateway::types::FunctionCall {
                                                                name: tool_start.name.clone(),
                                                                arguments: String::new(),
                                                            },
                                                        }]),
                                                        ..Default::default()
                                                    },
                                                    finish_reason: None,
                                                }],
                                            ));
                                        }
                                        _ => {
                                            // Text or other block start: emit role header if needed
                                            if !sent_role {
                                                sent_role = true;
                                                yield Ok(ChatCompletionChunk::new(
                                                    chunk_id.clone(),
                                                    model.clone(),
                                                    vec![ChunkChoice {
                                                        index: 0,
                                                        delta: ChunkDelta {
                                                            role: Some(MessageRole::Assistant),
                                                            ..Default::default()
                                                        },
                                                        finish_reason: None,
                                                    }],
                                                ));
                                            }
                                        }
                                    }
                                } else if !sent_role {
                                    sent_role = true;
                                    yield Ok(ChatCompletionChunk::new(
                                        chunk_id.clone(),
                                        model.clone(),
                                        vec![ChunkChoice {
                                            index: 0,
                                            delta: ChunkDelta {
                                                role: Some(MessageRole::Assistant),
                                                ..Default::default()
                                            },
                                            finish_reason: None,
                                        }],
                                    ));
                                }
                            }
                            ConverseStreamOutput::ContentBlockDelta(delta_event) => {
                                // Emit role header if not yet sent
                                if !sent_role {
                                    sent_role = true;
                                    yield Ok(ChatCompletionChunk::new(
                                        chunk_id.clone(),
                                        model.clone(),
                                        vec![ChunkChoice {
                                            index: 0,
                                            delta: ChunkDelta {
                                                role: Some(MessageRole::Assistant),
                                                ..Default::default()
                                            },
                                            finish_reason: None,
                                        }],
                                    ));
                                }

                                if let Some(delta) = delta_event.delta {
                                    match delta {
                                        aws_sdk_bedrockruntime::types::ContentBlockDelta::Text(text) => {
                                            yield Ok(ChatCompletionChunk::with_content(
                                                chunk_id.clone(),
                                                model.clone(),
                                                text,
                                            ));
                                        }
                                        aws_sdk_bedrockruntime::types::ContentBlockDelta::ToolUse(tool_delta) => {
                                            yield Ok(ChatCompletionChunk::new(
                                                chunk_id.clone(),
                                                model.clone(),
                                                vec![ChunkChoice {
                                                    index: 0,
                                                    delta: ChunkDelta {
                                                        tool_calls: Some(vec![ToolCall {
                                                            index: Some(current_tool_index),
                                                            id: String::new(),
                                                            tool_type: ToolType::Function,
                                                            function: crate::gateway::types::FunctionCall {
                                                                name: String::new(),
                                                                arguments: tool_delta.input.clone(),
                                                            },
                                                        }]),
                                                        ..Default::default()
                                                    },
                                                    finish_reason: None,
                                                }],
                                            ));
                                        }
                                        _ => {} // Image deltas and other future types
                                    }
                                }
                            }
                            ConverseStreamOutput::MessageStop(stop_event) => {
                                use aws_sdk_bedrockruntime::types::StopReason;
                                let openai_reason = match stop_event.stop_reason {
                                    StopReason::EndTurn => FinishReason::Stop,
                                    StopReason::MaxTokens => FinishReason::Length,
                                    StopReason::StopSequence => FinishReason::Stop,
                                    StopReason::ToolUse => FinishReason::ToolCalls,
                                    StopReason::ContentFiltered => FinishReason::ContentFilter,
                                    StopReason::GuardrailIntervened => FinishReason::ContentFilter,
                                    _ => FinishReason::Stop,
                                };
                                let mut chunk = ChatCompletionChunk::finished(
                                    chunk_id.clone(),
                                    model.clone(),
                                    openai_reason,
                                );
                                // Attach usage captured from the preceding Metadata event
                                chunk.usage = captured_usage.take();
                                yield Ok(chunk);
                            }
                            ConverseStreamOutput::Metadata(metadata) => {
                                // Capture token usage — will be attached to the stop chunk
                                if let Some(usage) = metadata.usage {
                                    captured_usage = Some(Usage {
                                        prompt_tokens: usage.input_tokens as u32,
                                        completion_tokens: usage.output_tokens as u32,
                                        total_tokens: (usage.input_tokens + usage.output_tokens) as u32,
                                        thinking_tokens: None,
                                        completion_tokens_details: None,
                                        prompt_tokens_details: None,
                                    });
                                }
                            }
                            // Ignore MessageStart, ContentBlockStop, and other future event types
                            _ => {}
                        }
                    }
                    Ok(None) => {
                        // Stream ended
                        break;
                    }
                    Err(e) => {
                        tracing::error!(
                            model = %model,
                            error = %e,
                            "Bedrock stream event error"
                        );
                        yield Err(GatewayError::InternalError(format!(
                            "Stream error: {}",
                            e
                        )));
                        break;
                    }
                }
            }
        };

        Ok(Box::pin(chunk_stream))
    }
}

// Helper functions for AWS SigV4 signing

fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

// Bedrock-specific request/response types

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BedrockConverseRequest {
    messages: Vec<BedrockMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<Vec<BedrockSystemContent>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    inference_config: Option<BedrockInferenceConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_config: Option<BedrockToolConfig>,
}

/// Tool configuration for Bedrock Converse API.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BedrockToolConfig {
    tools: Vec<BedrockToolSpec>,
}

/// Tool specification for Bedrock.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BedrockToolSpec {
    tool_spec: BedrockToolSpecInner,
}

/// Inner tool specification with name, description, and parameters.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BedrockToolSpecInner {
    name: String,
    description: String,
    input_schema: BedrockToolInputSchema,
}

/// Input schema for a tool (JSON Schema format).
#[derive(Debug, Serialize)]
struct BedrockToolInputSchema {
    json: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct BedrockMessage {
    role: String,
    content: Vec<BedrockContentBlock>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum BedrockContentBlock {
    Text {
        text: String,
    },
    /// Image content block for vision-capable models.
    Image {
        image: BedrockImageContent,
    },
    /// Document content block (PDFs and other files).
    Document {
        document: BedrockDocumentContent,
    },
    /// Tool use block in model responses
    ToolUse {
        #[serde(rename = "toolUse")]
        tool_use: BedrockToolUse,
    },
    /// Tool result block in user messages
    ToolResult {
        #[serde(rename = "toolResult")]
        tool_result: BedrockToolResult,
    },
}

/// Image content for Bedrock Converse API (JSON path).
#[derive(Debug, Serialize, Deserialize)]
struct BedrockImageContent {
    format: String, // "jpeg", "png", "gif", "webp"
    source: BedrockImageSource,
}

#[derive(Debug, Serialize, Deserialize)]
struct BedrockImageSource {
    bytes: String, // base64-encoded image bytes (Bedrock JSON format accepts base64 strings)
}

/// Document content for Bedrock Converse API (JSON path).
#[derive(Debug, Serialize, Deserialize)]
struct BedrockDocumentContent {
    format: String, // "pdf", "csv", "doc", "docx", "html", "md", "txt", "xls", "xlsx"
    name: String,
    source: BedrockDocumentSource,
}

#[derive(Debug, Serialize, Deserialize)]
struct BedrockDocumentSource {
    bytes: String, // base64-encoded document bytes
}

/// Tool use in model responses.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BedrockToolUse {
    tool_use_id: String,
    name: String,
    input: serde_json::Value,
}

/// Tool result in user messages.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BedrockToolResult {
    tool_use_id: String,
    content: Vec<BedrockToolResultContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<String>,
}

/// Tool result content.
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum BedrockToolResultContent {
    Text { text: String },
}

#[derive(Debug, Serialize)]
struct BedrockSystemContent {
    text: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BedrockInferenceConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stop_sequences: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BedrockConverseResponse {
    output: BedrockOutput,
    stop_reason: String,
    usage: BedrockUsage,
}

#[derive(Debug, Deserialize)]
struct BedrockOutput {
    message: BedrockMessage,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BedrockUsage {
    input_tokens: u32,
    output_tokens: u32,
    total_tokens: u32,
}

// Note: Streaming uses the AWS SDK's event stream types (ConverseStreamOutput)
// instead of custom serde types, as the AWS event stream format is binary-encoded
// and requires proper SDK handling.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::types::{FunctionCall, Tool};

    #[test]
    fn test_supports_model() {
        let provider = BedrockProvider::new();

        // Explicit bedrock prefix
        assert!(provider.supports_model("bedrock/anthropic.claude-3-sonnet-20240229-v1:0"));

        // Direct model IDs
        assert!(provider.supports_model("anthropic.claude-3-opus-20240229-v1:0"));
        assert!(provider.supports_model("amazon.titan-text-express-v1"));
        assert!(provider.supports_model("meta.llama3-70b-instruct-v1:0"));
        assert!(provider.supports_model("mistral.mixtral-8x7b-instruct-v0:1"));

        // Not Bedrock models
        assert!(!provider.supports_model("gpt-4"));
        assert!(!provider.supports_model("claude-sonnet-4-6")); // No prefix
        assert!(!provider.supports_model("gemini-pro"));
    }

    #[test]
    fn test_extract_model_id() {
        let provider = BedrockProvider::new();

        assert_eq!(
            provider.extract_model_id("bedrock/anthropic.claude-3-sonnet-20240229-v1:0"),
            "anthropic.claude-3-sonnet-20240229-v1:0"
        );
        assert_eq!(
            provider.extract_model_id("anthropic.claude-3-sonnet-20240229-v1:0"),
            "anthropic.claude-3-sonnet-20240229-v1:0"
        );
    }

    #[test]
    fn test_convert_messages() {
        let provider = BedrockProvider::new();

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
                content: Some(MessageContent::Text("Hello".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            },
        ];

        let (system, bedrock_messages) = provider.convert_messages(&messages);

        assert!(system.is_some());
        assert_eq!(system.unwrap()[0].text, "You are helpful.");
        assert_eq!(bedrock_messages.len(), 1);
        assert_eq!(bedrock_messages[0].role, "user");
    }

    #[test]
    fn test_parse_credentials_json() {
        // Standard JSON format
        let creds = BedrockProvider::parse_credentials(
            r#"{"access_key_id": "AKIAIOSFODNN7EXAMPLE", "secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}"#
        ).unwrap();
        assert_eq!(creds.access_key_id, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(
            creds.secret_access_key,
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        );
        assert!(creds.session_token.is_none());
    }

    #[test]
    fn test_parse_credentials_json_with_session_token() {
        let creds = BedrockProvider::parse_credentials(
            r#"{"access_key_id": "AKIAIOSFODNN7EXAMPLE", "secret_access_key": "secret", "session_token": "token123"}"#
        ).unwrap();
        assert_eq!(creds.access_key_id, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(creds.secret_access_key, "secret");
        assert_eq!(creds.session_token, Some("token123".to_string()));
    }

    #[test]
    fn test_parse_credentials_json_aws_prefixed() {
        // AWS SDK naming convention with aws_ prefix
        let creds = BedrockProvider::parse_credentials(
            r#"{"aws_access_key_id": "AKIAIOSFODNN7EXAMPLE", "aws_secret_access_key": "secret", "aws_session_token": "token"}"#
        ).unwrap();
        assert_eq!(creds.access_key_id, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(creds.secret_access_key, "secret");
        assert_eq!(creds.session_token, Some("token".to_string()));
    }

    #[test]
    fn test_parse_credentials_invalid() {
        // Non-JSON format is invalid
        assert!(BedrockProvider::parse_credentials("JUST_ACCESS_KEY").is_err());

        // Colon-separated format is no longer supported
        assert!(BedrockProvider::parse_credentials("ACCESS:SECRET").is_err());

        // Invalid JSON
        assert!(BedrockProvider::parse_credentials("{invalid json}").is_err());

        // Missing required fields in JSON
        assert!(BedrockProvider::parse_credentials(r#"{"access_key_id": "test"}"#).is_err());
    }

    #[test]
    fn test_convert_tools_to_bedrock_format() {
        use crate::gateway::types::{FunctionDefinition, Tool};

        let provider = BedrockProvider::new();

        let tools = Some(vec![Tool {
            tool_type: ToolType::Function,
            function: FunctionDefinition {
                name: "get_weather".to_string(),
                description: Some("Get the current weather".to_string()),
                parameters: Some(serde_json::json!({
                    "type": "object",
                    "properties": {
                        "location": { "type": "string" }
                    },
                    "required": ["location"]
                })),
            },
        }]);

        let result = provider.convert_tools(&tools);
        assert!(result.is_some());

        let tool_config = result.unwrap();
        assert_eq!(tool_config.tools.len(), 1);
        assert_eq!(tool_config.tools[0].tool_spec.name, "get_weather");
        assert_eq!(
            tool_config.tools[0].tool_spec.description,
            "Get the current weather"
        );
    }

    #[test]
    fn test_convert_messages_with_tool_calls() {
        use crate::gateway::types::{FunctionCall, ToolCall};

        let provider = BedrockProvider::new();

        let messages = vec![
            ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text(
                    "What's the weather in Tokyo?".to_string(),
                )),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            },
            ChatMessage {
                role: MessageRole::Assistant,
                content: None,
                name: None,
                tool_calls: Some(vec![ToolCall {
                    index: None,
                    id: "call_1".to_string(),
                    tool_type: ToolType::Function,
                    function: FunctionCall {
                        name: "get_weather".to_string(),
                        arguments: r#"{"location": "Tokyo"}"#.to_string(),
                    },
                }]),
                tool_call_id: None,
                reasoning_content: None,
            },
            ChatMessage {
                role: MessageRole::Other,
                content: Some(MessageContent::Text(
                    r#"{"temperature": 22, "unit": "celsius"}"#.to_string(),
                )),
                name: Some("get_weather".to_string()),
                tool_calls: None,
                tool_call_id: Some("call_1".to_string()),
                reasoning_content: None,
            },
        ];

        let (system, bedrock_messages) = provider.convert_messages(&messages);

        // Should have no system prompts
        assert!(system.is_none());

        // Should have 3 messages: user, assistant with tool use, user with tool result
        assert_eq!(bedrock_messages.len(), 3);

        // First message: user text
        assert_eq!(bedrock_messages[0].role, "user");

        // Second message: assistant with tool use
        assert_eq!(bedrock_messages[1].role, "assistant");
        assert!(bedrock_messages[1]
            .content
            .iter()
            .any(|b| matches!(b, BedrockContentBlock::ToolUse { .. })));

        // Third message: user with tool result
        assert_eq!(bedrock_messages[2].role, "user");
        assert!(bedrock_messages[2]
            .content
            .iter()
            .any(|b| matches!(b, BedrockContentBlock::ToolResult { .. })));

        // Verify tool use details
        if let Some(BedrockContentBlock::ToolUse { tool_use }) = bedrock_messages[1]
            .content
            .iter()
            .find(|b| matches!(b, BedrockContentBlock::ToolUse { .. }))
        {
            assert_eq!(tool_use.name, "get_weather");
            assert_eq!(tool_use.tool_use_id, "call_1");
        } else {
            panic!("Expected ToolUse block");
        }

        // Verify tool result details
        if let Some(BedrockContentBlock::ToolResult { tool_result }) = bedrock_messages[2]
            .content
            .iter()
            .find(|b| matches!(b, BedrockContentBlock::ToolResult { .. }))
        {
            assert_eq!(tool_result.tool_use_id, "call_1");
        } else {
            panic!("Expected ToolResult block");
        }
    }

    #[test]
    fn test_json_value_to_document() {
        let value = serde_json::json!({
            "name": "test",
            "count": 42,
            "enabled": true,
            "nested": {
                "array": [1, 2, 3]
            }
        });

        let doc = BedrockProvider::json_value_to_document(value);

        // Verify it's an object
        if let aws_smithy_types::Document::Object(map) = doc {
            assert!(map.contains_key("name"));
            assert!(map.contains_key("count"));
            assert!(map.contains_key("enabled"));
            assert!(map.contains_key("nested"));
        } else {
            panic!("Expected Document::Object");
        }
    }

    /// Regression: `json_value_to_document` previously used `expect()` which
    /// would panic the request handler on conversion failure. The fix returns
    /// an empty Document object as a fallback instead of panicking.
    #[test]
    fn test_json_value_to_document_deeply_nested() {
        let mut value = serde_json::json!("leaf");
        for _ in 0..128 {
            value = serde_json::json!({ "nested": value });
        }
        // Must not panic regardless of nesting depth
        let doc = BedrockProvider::json_value_to_document(value);
        // Result is either a valid Document::Object or the empty fallback
        match doc {
            aws_smithy_types::Document::Object(_) => {}
            _ => panic!("Expected Document::Object (real or fallback)"),
        }
    }

    #[test]
    fn test_convert_response_with_tool_use() {
        let provider = BedrockProvider::new();

        let bedrock_response = BedrockConverseResponse {
            output: BedrockOutput {
                message: BedrockMessage {
                    role: "assistant".to_string(),
                    content: vec![BedrockContentBlock::ToolUse {
                        tool_use: BedrockToolUse {
                            tool_use_id: "call_123".to_string(),
                            name: "get_weather".to_string(),
                            input: serde_json::json!({"location": "Tokyo"}),
                        },
                    }],
                },
            },
            stop_reason: "tool_use".to_string(),
            usage: BedrockUsage {
                input_tokens: 10,
                output_tokens: 15,
                total_tokens: 25,
            },
        };

        let response =
            provider.convert_response(bedrock_response, "anthropic.claude-3-sonnet-20240229-v1:0");
        let choice = &response.choices[0];

        // Should have tool_calls but no content
        assert!(choice.message.content.is_none());
        assert!(choice.message.tool_calls.is_some());

        let tool_calls = choice.message.tool_calls.as_ref().unwrap();
        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0].id, "call_123");
        assert_eq!(tool_calls[0].function.name, "get_weather");
        assert!(tool_calls[0].function.arguments.contains("Tokyo"));

        // Finish reason should be "tool_calls"
        assert_eq!(choice.finish_reason, FinishReason::ToolCalls);
    }

    // ========================================================================
    // convert_tools_to_sdk Tests
    // ========================================================================

    #[test]
    fn test_convert_tools_empty() {
        let provider = BedrockProvider::new();
        let tools: Option<Vec<Tool>> = None;

        let result = provider.convert_tools(&tools);
        assert!(result.is_none());
    }

    #[test]
    fn test_convert_tools_empty_vec() {
        let provider = BedrockProvider::new();
        let tools: Option<Vec<Tool>> = Some(vec![]);

        let result = provider.convert_tools(&tools);
        // Empty vec should also return None or empty config
        assert!(result.is_none() || result.unwrap().tools.is_empty());
    }

    #[test]
    fn test_convert_tools_multiple() {
        use crate::gateway::types::{FunctionDefinition, Tool};

        let provider = BedrockProvider::new();

        let tools = Some(vec![
            Tool {
                tool_type: ToolType::Function,
                function: FunctionDefinition {
                    name: "get_weather".to_string(),
                    description: Some("Get weather info".to_string()),
                    parameters: Some(serde_json::json!({
                        "type": "object",
                        "properties": {
                            "location": { "type": "string" }
                        }
                    })),
                },
            },
            Tool {
                tool_type: ToolType::Function,
                function: FunctionDefinition {
                    name: "get_time".to_string(),
                    description: Some("Get current time".to_string()),
                    parameters: Some(serde_json::json!({
                        "type": "object",
                        "properties": {
                            "timezone": { "type": "string" }
                        }
                    })),
                },
            },
        ]);

        let result = provider.convert_tools(&tools);
        assert!(result.is_some());

        let tool_config = result.unwrap();
        assert_eq!(tool_config.tools.len(), 2);

        let names: Vec<&str> = tool_config
            .tools
            .iter()
            .map(|t| t.tool_spec.name.as_str())
            .collect();
        assert!(names.contains(&"get_weather"));
        assert!(names.contains(&"get_time"));
    }

    #[test]
    fn test_convert_tools_no_parameters() {
        use crate::gateway::types::{FunctionDefinition, Tool};

        let provider = BedrockProvider::new();

        let tools = Some(vec![Tool {
            tool_type: ToolType::Function,
            function: FunctionDefinition {
                name: "no_params_function".to_string(),
                description: Some("A function without parameters".to_string()),
                parameters: None,
            },
        }]);

        let result = provider.convert_tools(&tools);
        assert!(result.is_some());

        let tool_config = result.unwrap();
        assert_eq!(tool_config.tools.len(), 1);
        assert_eq!(tool_config.tools[0].tool_spec.name, "no_params_function");
    }

    #[test]
    fn test_convert_tools_no_description() {
        use crate::gateway::types::{FunctionDefinition, Tool};

        let provider = BedrockProvider::new();

        let tools = Some(vec![Tool {
            tool_type: ToolType::Function,
            function: FunctionDefinition {
                name: "simple_function".to_string(),
                description: None,
                parameters: Some(serde_json::json!({"type": "object"})),
            },
        }]);

        let result = provider.convert_tools(&tools);
        assert!(result.is_some());

        let tool_config = result.unwrap();
        assert_eq!(tool_config.tools.len(), 1);
        // Description should be empty or default
        assert!(
            tool_config.tools[0].tool_spec.description.is_empty()
                || tool_config.tools[0].tool_spec.description == "No description provided"
        );
    }

    /// Regression: Bedrock streaming errors were all mapped to `ProviderError { status: 500 }`
    /// regardless of the actual error type. `ThrottlingException` was misclassified as a
    /// server error, causing unnecessary retries instead of proper rate-limit handling.
    #[test]
    fn test_map_converse_stream_error_throttling() {
        use aws_smithy_types::error::ErrorMetadata;

        let meta = ErrorMetadata::builder()
            .code("ThrottlingException")
            .message("Too many requests")
            .build();
        let err = ConverseStreamError::ThrottlingException(
            aws_sdk_bedrockruntime::types::error::ThrottlingException::builder()
                .meta(meta)
                .message("Too many requests")
                .build(),
        );

        let gateway_err = map_converse_stream_error(&err);
        assert!(
            matches!(gateway_err, GatewayError::RateLimitExceeded { .. }),
            "ThrottlingException must produce RateLimitExceeded, got: {:?}",
            gateway_err
        );
    }

    /// Regression: `AccessDeniedException` was mapped to `ProviderError { status: 500 }`,
    /// which is retryable. Auth errors should not be retried.
    #[test]
    fn test_map_converse_stream_error_access_denied() {
        use aws_smithy_types::error::ErrorMetadata;

        let meta = ErrorMetadata::builder()
            .code("AccessDeniedException")
            .message("You don't have access")
            .build();
        let err = ConverseStreamError::AccessDeniedException(
            aws_sdk_bedrockruntime::types::error::AccessDeniedException::builder()
                .meta(meta)
                .message("You don't have access")
                .build(),
        );

        let gateway_err = map_converse_stream_error(&err);
        assert!(
            matches!(gateway_err, GatewayError::AuthenticationFailed(_)),
            "AccessDeniedException must produce AuthenticationFailed, got: {:?}",
            gateway_err
        );
    }

    #[test]
    fn test_map_converse_stream_error_validation() {
        use aws_smithy_types::error::ErrorMetadata;

        let meta = ErrorMetadata::builder()
            .code("ValidationException")
            .message("Invalid input")
            .build();
        let err = ConverseStreamError::ValidationException(
            aws_sdk_bedrockruntime::types::error::ValidationException::builder()
                .meta(meta)
                .message("Invalid input")
                .build(),
        );

        let gateway_err = map_converse_stream_error(&err);
        match gateway_err {
            GatewayError::ProviderError { status, .. } => {
                assert_eq!(status, 400, "ValidationException must map to status 400");
            }
            other => panic!("expected ProviderError, got: {:?}", other),
        }
    }

    #[test]
    fn test_map_converse_stream_error_model_timeout() {
        use aws_smithy_types::error::ErrorMetadata;

        let meta = ErrorMetadata::builder()
            .code("ModelTimeoutException")
            .message("Model timed out")
            .build();
        let err = ConverseStreamError::ModelTimeoutException(
            aws_sdk_bedrockruntime::types::error::ModelTimeoutException::builder()
                .meta(meta)
                .message("Model timed out")
                .build(),
        );

        let gateway_err = map_converse_stream_error(&err);
        assert!(
            matches!(gateway_err, GatewayError::Timeout(_)),
            "ModelTimeoutException must produce Timeout, got: {:?}",
            gateway_err
        );
    }

    /// Regression: Bedrock's non-streaming `chat_completion` bypassed `parse_provider_error`,
    /// constructing a raw `ProviderError` for all HTTP errors. This meant 429 responses
    /// produced `ProviderError { status: 429 }` instead of `RateLimitExceeded`, so the
    /// `Retry-After` header was missing from the gateway response.
    #[test]
    fn test_bedrock_nonstreaming_429_uses_parse_provider_error() {
        let error_text = r#"{"message": "Rate limit exceeded"}"#;
        let err = parse_provider_error(error_text, Provider::Bedrock, 429);
        assert!(
            matches!(err, GatewayError::RateLimitExceeded { .. }),
            "Bedrock 429 must produce RateLimitExceeded (via parse_provider_error), got: {:?}",
            err
        );
    }

    /// Regression: Bedrock non-streaming errors were not JSON-parsed, so the raw
    /// response body (often containing nested JSON) was used as-is for the error
    /// message instead of extracting the human-readable message field.
    #[test]
    fn test_bedrock_nonstreaming_error_message_extracted() {
        let error_text =
            r#"{"error": {"message": "Model not found", "type": "invalid_request_error"}}"#;
        let err = parse_provider_error(error_text, Provider::Bedrock, 404);
        match err {
            GatewayError::ProviderError {
                message, status, ..
            } => {
                assert_eq!(status, 404);
                assert_eq!(message, "Model not found");
            }
            other => panic!("expected ProviderError, got: {:?}", other),
        }
    }

    /// Regression: Bedrock streaming tool call chunks did not carry the
    /// `index` field, so clients could not correlate argument deltas with
    /// the correct tool call when the model invoked multiple tools.
    #[test]
    fn test_bedrock_streaming_tool_call_index_assignment() {
        // Simulate what the streaming code does: assign sequential indices
        // via `tool_call_index` / `current_tool_index`.
        let mut tool_call_index: u32 = 0;

        // First tool call start
        let first_idx = tool_call_index;
        tool_call_index += 1;
        let tc_start_0 = ToolCall {
            index: Some(first_idx),
            id: "tool_use_aaa".to_string(),
            tool_type: ToolType::Function,
            function: FunctionCall {
                name: "get_weather".to_string(),
                arguments: String::new(),
            },
        };
        assert_eq!(
            tc_start_0.index,
            Some(0),
            "first tool start must have index 0"
        );

        // First tool call delta uses current_tool_index (= first_idx)
        let tc_delta_0 = ToolCall {
            index: Some(first_idx),
            id: String::new(),
            tool_type: ToolType::Function,
            function: FunctionCall {
                name: String::new(),
                arguments: r#"{"location":"Tokyo"}"#.to_string(),
            },
        };
        assert_eq!(
            tc_delta_0.index,
            Some(0),
            "first tool delta must carry index 0"
        );

        // Second tool call start
        let second_idx = tool_call_index;
        tool_call_index += 1;
        let _ = tool_call_index; // suppress unused warning
        let tc_start_1 = ToolCall {
            index: Some(second_idx),
            id: "tool_use_bbb".to_string(),
            tool_type: ToolType::Function,
            function: FunctionCall {
                name: "get_time".to_string(),
                arguments: String::new(),
            },
        };
        assert_eq!(
            tc_start_1.index,
            Some(1),
            "second tool start must have index 1"
        );

        // Verify JSON serialization includes index
        let json = serde_json::to_value(&tc_start_0).unwrap();
        assert_eq!(json["index"], 0, "index must appear in serialized JSON");
    }
}
