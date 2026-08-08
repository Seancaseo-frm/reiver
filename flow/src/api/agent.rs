//! In-app AI Agent — agentic loop with MCP tool execution.
//!
//! Provides conversation management and a chat endpoint that runs an
//! LLM-in-the-loop with all registered platform actions as tools.

use axum::{
    extract::State,
    http::HeaderMap,
    response::{
        sse::{Event, Sse},
        IntoResponse, Response,
    },
    routing::{delete, get, post},
    Json, Router,
};
use futures::stream::Stream;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use uuid::Uuid;

use tracing::Instrument;

use crate::api::{extract_project_id, extract_user_id};
use crate::app_state::FlowState;
use crate::error::{AppError, Result};
use crate::gateway::providers::ChatCompletionStream;
use crate::gateway::types::{
    ChatCompletionRequest, ChatMessage, FunctionCall, MessageContent, MessageRole, Tool, ToolCall,
    ToolType, Usage,
};

use crate::api::agent_attachments::{build_user_content, upload_attachment};
use crate::api::agent_context::{
    auto_compact, drop_orphaned_tool_messages, prune_stale_tool_results, snip_compact,
    COMPACT_THRESHOLD, CONTEXT_TOKEN_BUDGET,
};
use crate::api::agent_executor::{
    extract_tool_text, resolve_tool_name, truncate_tool_result, TOOL_TIMEOUT,
};
use crate::api::agent_persistence::{
    create_conversation, delete_conversation, list_conversations, list_messages, save_message_owned,
};

pub use crate::api::agent_persistence::Message;

const MAX_AGENT_TURNS: usize = 20;
const MAX_MESSAGE_BYTES: usize = 32_768;
/// Number of recent user turns whose tool results are kept verbatim.
/// Older tool results are replaced with a staleness stub.
const TOOL_FRESHNESS_TURNS: usize = 3;

// ═══════════════════════════════════════════════════════════════════════════
// Secret scrubbing — redact known credential patterns before LLM context
// ═══════════════════════════════════════════════════════════════════════════

fn secret_patterns() -> &'static regex::RegexSet {
    static PATTERNS: std::sync::OnceLock<regex::RegexSet> = std::sync::OnceLock::new();
    PATTERNS.get_or_init(|| {
        regex::RegexSet::new([
            r"sk-proj-[A-Za-z0-9_-]{20,}",
            r"sk-[A-Za-z0-9]{20,}",
            r"sk-ant-[A-Za-z0-9_-]{20,}",
            r"sk_live_[A-Za-z0-9]{20,}",
            r"sk_test_[A-Za-z0-9]{20,}",
            r"gh[pousr]_[A-Za-z0-9_]{20,}",
            r"AKIA[A-Z0-9]{16}",
            r"xox[bpras]-[A-Za-z0-9\-]{20,}",
        ])
        .expect("invalid secret patterns")
    })
}

fn secret_regex() -> &'static regex::Regex {
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    RE.get_or_init(|| {
        regex::Regex::new(
            r"(?:sk-proj-[A-Za-z0-9_\-]{20,}|sk-[A-Za-z0-9]{20,}|sk-ant-[A-Za-z0-9_\-]{20,}|sk_live_[A-Za-z0-9]{20,}|sk_test_[A-Za-z0-9]{20,}|gh[pousr]_[A-Za-z0-9_]{20,}|AKIA[A-Z0-9]{16}|xox[bpras]-[A-Za-z0-9\-]{20,})"
        ).expect("invalid secret regex")
    })
}

/// Scrub known secret patterns from a message. Returns the scrubbed text
/// and whether any redaction occurred.
fn scrub_secrets(text: &str) -> (String, bool) {
    if !secret_patterns().is_match(text) {
        return (text.to_string(), false);
    }
    let scrubbed = secret_regex().replace_all(text, "[REDACTED]");
    (scrubbed.into_owned(), true)
}

// ═══════════════════════════════════════════════════════════════════════════
// Router
// ═══════════════════════════════════════════════════════════════════════════

pub fn create_agent_router() -> Router<Arc<FlowState>> {
    Router::new()
        .route("/chat", post(agent_chat))
        .route("/attachments", post(upload_attachment))
        .route("/conversations", get(list_conversations))
        .route("/conversations", post(create_conversation))
        .route("/conversations/{id}", delete(delete_conversation))
        .route("/conversations/{id}/messages", get(list_messages))
}

// ═══════════════════════════════════════════════════════════════════════════
// Tool format conversion (rmcp::model::Tool <-> gateway Tool)
// ═══════════════════════════════════════════════════════════════════════════

/// Convert MCP registry tools into OpenAI-compatible gateway tool definitions.
pub(crate) fn mcp_tools_to_gateway_tools(mcp_tools: &[rmcp::model::Tool]) -> Vec<Tool> {
    mcp_tools
        .iter()
        .map(|t| {
            let parameters: Option<serde_json::Value> = {
                let map = t.input_schema.as_ref().clone();
                if map.is_empty() {
                    None
                } else {
                    Some(serde_json::Value::Object(map))
                }
            };
            Tool {
                tool_type: ToolType::Function,
                function: crate::gateway::types::FunctionDefinition {
                    name: t.name.to_string(),
                    description: t.description.as_ref().map(|d| d.to_string()),
                    parameters,
                },
            }
        })
        .collect()
}

// ═══════════════════════════════════════════════════════════════════════════
// Request / Response types
// ═══════════════════════════════════════════════════════════════════════════

// this is used so moodeng that "see" the page that the user is seeing
#[derive(Debug, Deserialize)]
pub struct PageContext {
    pub route: Option<String>,
    pub entity_id: Option<String>,
    pub entity_type: Option<String>,
    pub snapshot: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct AgentChatRequest {
    pub conversation_id: Option<Uuid>,
    pub message: String,
    #[serde(default)]
    pub page_context: Option<PageContext>,
    #[serde(default)]
    pub attachment_ids: Option<Vec<Uuid>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentEvent {
    ConversationCreated {
        conversation_id: Uuid,
    },
    TextDelta {
        content: String,
    },
    ToolStart {
        call_id: String,
        name: String,
        input: serde_json::Value,
    },
    ToolResult {
        call_id: String,
        name: String,
        output: serde_json::Value,
    },
    Done {
        conversation_id: Uuid,
    },
    Status {
        content: String,
    },
    Error {
        error: String,
    },
}

// ═══════════════════════════════════════════════════════════════════════════
// Agent Chat (Agentic Loop)
// ═══════════════════════════════════════════════════════════════════════════

#[tracing::instrument(
    name = "agent.chat",
    skip_all,
    fields(project_id, user_id, conversation_id)
)]
async fn agent_chat(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    Json(req): Json<AgentChatRequest>,
) -> Result<Response> {
    let project_id = extract_project_id(&headers)?;
    let user_id = extract_user_id(&headers)?;
    let user_jwt = headers
        .get("x-user-jwt")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let span = tracing::Span::current();
    span.record("project_id", tracing::field::display(project_id));
    span.record("user_id", tracing::field::display(user_id));

    if req.message.len() > MAX_MESSAGE_BYTES {
        return Err(AppError::Validation(format!(
            "Message too long ({} bytes, max {})",
            req.message.len(),
            MAX_MESSAGE_BYTES
        )));
    }

    // Evict cached settings so the agent always sees the latest scopes/config.
    // This avoids stale reads when multiple Flow pods are behind a load balancer
    // and only one received the cache invalidation from a settings save.
    state.introspection_settings_cache.remove(&project_id);
    let settings = crate::gateway::routes::get_introspection_settings(&state, project_id).await;
    if !settings.agent_enabled {
        return Err(AppError::BadRequest(
            "AI Agent is disabled for this project".into(),
        ));
    }

    // Acquire per-conversation lock to prevent interleaved messages
    let conversation_id = match req.conversation_id {
        Some(id) => {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM agent_conversations \
                 WHERE id = $1 AND project_id = $2 AND user_id = $3)",
            )
            .bind(id)
            .bind(project_id)
            .bind(user_id)
            .fetch_one(state.db.as_ref())
            .await?;
            if !exists {
                return Err(AppError::NotFound("Conversation not found".into()));
            }
            id
        }
        None => {
            let title = req.message.chars().take(80).collect::<String>();
            let row: (Uuid,) = sqlx::query_as(
                "INSERT INTO agent_conversations (project_id, user_id, title) \
                 VALUES ($1, $2, $3) RETURNING id",
            )
            .bind(project_id)
            .bind(user_id)
            .bind(&title)
            .fetch_one(state.db.as_ref())
            .await?;
            row.0
        }
    };

    span.record("conversation_id", tracing::field::display(conversation_id));

    // Acquire per-conversation lock
    let lock = state
        .agent_conversation_locks
        .entry(conversation_id)
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone();

    let is_new = req.conversation_id.is_none();

    // Scrub known secret patterns before the message enters the LLM context
    let (safe_message, was_redacted) = scrub_secrets(&req.message);
    if was_redacted {
        tracing::warn!(%project_id, %user_id, "Secret pattern detected and redacted from agent chat input");
    }

    // Build prompt hub variables from page context for the moodeng-assistant config
    let prompt_config = state
        .moodeng_project_id
        .map(|_| "moodeng-assistant".to_string());
    let mut prompt_variables = std::collections::HashMap::new();
    if let Some(ref pc) = req.page_context {
        if let Some(ref route) = pc.route {
            prompt_variables.insert("route".into(), serde_json::Value::String(route.clone()));
        }
        if let Some(ref entity_type) = pc.entity_type {
            prompt_variables.insert(
                "entity_type".into(),
                serde_json::Value::String(entity_type.clone()),
            );
        }
        if let Some(ref entity_id) = pc.entity_id {
            prompt_variables.insert(
                "entity_id".into(),
                serde_json::Value::String(entity_id.clone()),
            );
        }
        if let Some(ref snapshot) = pc.snapshot {
            prompt_variables.insert("snapshot".into(), snapshot.clone());
        }
    }

    let stream = run_agent_stream(
        state,
        project_id,
        user_id,
        user_jwt,
        conversation_id,
        is_new,
        safe_message,
        was_redacted,
        req.page_context,
        req.attachment_ids,
        prompt_config,
        prompt_variables,
        lock,
    );

    Ok(Sse::new(stream)
        .keep_alive(axum::response::sse::KeepAlive::default())
        .into_response())
}

fn run_agent_stream(
    state: Arc<FlowState>,
    project_id: Uuid,
    user_id: Uuid,
    user_jwt: String,
    conversation_id: Uuid,
    is_new_conversation: bool,
    user_message: String,
    secrets_redacted: bool,
    page_context: Option<PageContext>,
    attachment_ids: Option<Vec<Uuid>>,
    prompt_config: Option<String>,
    prompt_variables: std::collections::HashMap<String, serde_json::Value>,
    lock: Arc<tokio::sync::Mutex<()>>,
) -> impl Stream<Item = std::result::Result<Event, Infallible>> {
    let (tx, mut rx) = mpsc::channel::<AgentEvent>(64);

    let parent_span = tracing::Span::current();
    tokio::spawn(async move {
        let _guard = lock.lock().await;

        if secrets_redacted {
            let _ = tx
                .send(AgentEvent::TextDelta {
                    content: "⚠️ I detected what looks like a secret in your message and redacted it for security. \
                 Please use the secure deposit link instead — I can create one for you.\n\n"
                        .to_string(),
                })
                .await;
        }

        run_agent_loop(
            state,
            project_id,
            user_id,
            user_jwt,
            conversation_id,
            is_new_conversation,
            &user_message,
            page_context.as_ref(),
            attachment_ids,
            prompt_config,
            prompt_variables,
            &tx,
        )
        .await;
    }.instrument(parent_span));

    async_stream::stream! {
        while let Some(evt) = rx.recv().await {
            match serde_json::to_string(&evt) {
                Ok(json) => yield Ok(Event::default().data(json)),
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to serialize AgentEvent");
                }
            }
        }
    }
}

/// Core agentic loop: LLM -> tool calls -> LLM -> ... -> final text.
/// Sends events to the client as they happen via the mpsc channel.
#[tracing::instrument(
    name = "agent.loop",
    skip(state, user_jwt, user_message, page_context, attachment_ids, prompt_config, prompt_variables, tx),
    fields(
        %project_id, %user_id, %conversation_id,
        gen_ai.agent.name = "moodeng",
        gen_ai.operation.name = "invoke_agent",
        turn_count = tracing::field::Empty,
        model = tracing::field::Empty,
        provider = tracing::field::Empty,
        total_input_tokens = tracing::field::Empty,
        total_output_tokens = tracing::field::Empty,
        tool_count = tracing::field::Empty,
    )
)]
async fn run_agent_loop(
    state: Arc<FlowState>,
    project_id: Uuid,
    user_id: Uuid,
    user_jwt: String,
    conversation_id: Uuid,
    is_new_conversation: bool,
    user_message: &str,
    page_context: Option<&PageContext>,
    attachment_ids: Option<Vec<Uuid>>,
    prompt_config: Option<String>,
    prompt_variables: std::collections::HashMap<String, serde_json::Value>,
    tx: &mpsc::Sender<AgentEvent>,
) {
    if let Err(e) = run_agent_loop_inner(
        &state,
        project_id,
        user_id,
        &user_jwt,
        conversation_id,
        is_new_conversation,
        user_message,
        page_context,
        attachment_ids,
        prompt_config,
        prompt_variables,
        tx,
    )
    .await
    {
        state.metrics.provider_error.add(
            1,
            &[
                opentelemetry::KeyValue::new("gen_ai.operation.name", "agent_chat"),
                opentelemetry::KeyValue::new("error.type", "loop_error"),
            ],
        );
        tracing::error!(error = %e, "Agent loop failed");
        let _ = tx
            .send(AgentEvent::Error {
                error: e.to_string(),
            })
            .await;
    }
}

/// Accumulated result from consuming a streaming LLM response.
struct StreamedResponse {
    content: String,
    thinking: String,
    tool_calls: Vec<ToolCall>,
    usage: Usage,
    finish_reason: String,
    ttft_ms: Option<f64>,
    model: String,
}

/// Consume a streaming LLM response, forwarding text deltas to the client
/// immediately and accumulating tool call fragments into complete tool calls.
async fn consume_llm_stream(
    mut stream: ChatCompletionStream,
    tx: &mpsc::Sender<AgentEvent>,
) -> anyhow::Result<StreamedResponse> {
    let stream_start = std::time::Instant::now();
    let mut content = String::new();
    let mut thinking = String::new();
    let mut tool_call_map: std::collections::HashMap<u32, ToolCall> =
        std::collections::HashMap::new();
    let mut usage = Usage::default();
    let mut finish_reason = String::new();
    let mut ttft_ms: Option<f64> = None;
    let mut model = String::new();

    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.map_err(|e| anyhow::anyhow!("Stream error: {e}"))?;

        if model.is_empty() && !chunk.model.is_empty() {
            model = chunk.model.clone();
        }

        if let Some(u) = chunk.usage {
            usage = u;
        }

        for choice in &chunk.choices {
            if let Some(fr) = &choice.finish_reason {
                finish_reason = fr.as_str().to_string();
            }

            if let Some(text) = &choice.delta.content {
                if !text.is_empty() {
                    if ttft_ms.is_none() {
                        ttft_ms = Some(stream_start.elapsed().as_secs_f64() * 1000.0);
                    }
                    content.push_str(text);
                    let _ = tx
                        .send(AgentEvent::TextDelta {
                            content: text.to_string(),
                        })
                        .await;
                }
            }

            if let Some(delta_tcs) = &choice.delta.tool_calls {
                if ttft_ms.is_none() {
                    ttft_ms = Some(stream_start.elapsed().as_secs_f64() * 1000.0);
                }
                for delta_tc in delta_tcs {
                    let idx = delta_tc.index.unwrap_or(0);
                    let entry = tool_call_map.entry(idx).or_insert_with(|| ToolCall {
                        index: Some(idx),
                        id: String::new(),
                        tool_type: ToolType::Function,
                        function: FunctionCall {
                            name: String::new(),
                            arguments: String::new(),
                        },
                    });
                    if !delta_tc.id.is_empty() {
                        entry.id = delta_tc.id.clone();
                    }
                    if !delta_tc.function.name.is_empty() {
                        entry.function.name.push_str(&delta_tc.function.name);
                    }
                    entry
                        .function
                        .arguments
                        .push_str(&delta_tc.function.arguments);
                }
            }

            if let Some(text) = &choice.delta.thinking {
                if !text.is_empty() {
                    thinking.push_str(text);
                }
            }
            if let Some(text) = &choice.delta.reasoning_content {
                if !text.is_empty() {
                    thinking.push_str(text);
                }
            }
        }
    }

    let mut tool_calls: Vec<ToolCall> = tool_call_map.into_values().collect();
    tool_calls.sort_by_key(|tc| tc.index.unwrap_or(0));

    Ok(StreamedResponse {
        content,
        thinking,
        tool_calls,
        usage,
        finish_reason,
        ttft_ms,
        model,
    })
}

// ═══════════════════════════════════════════════════════════════════════════

async fn run_agent_loop_inner(
    state: &FlowState,
    project_id: Uuid,
    user_id: Uuid,
    user_jwt: &str,
    conversation_id: Uuid,
    is_new_conversation: bool,
    user_message: &str,
    page_context: Option<&PageContext>,
    attachment_ids: Option<Vec<Uuid>>,
    prompt_config: Option<String>,
    mut prompt_variables: std::collections::HashMap<String, serde_json::Value>,
    tx: &mpsc::Sender<AgentEvent>,
) -> anyhow::Result<()> {
    let loop_start = std::time::Instant::now();
    let metrics = &state.metrics;
    let metric_attrs = [opentelemetry::KeyValue::new(
        "project_id",
        project_id.to_string(),
    )];
    let mut last_model = String::new();
    let mut last_provider = String::new();
    let moodeng = crate::moodeng::MoodengClient::new(state, project_id);

    if is_new_conversation {
        let _ = tx
            .send(AgentEvent::ConversationCreated { conversation_id })
            .await;
    }

    let settings = crate::gateway::routes::get_introspection_settings(state, project_id).await;

    if !settings.agent_soul.is_empty() {
        if let Ok(soul_json) = serde_json::to_value(&settings.agent_soul) {
            prompt_variables.insert("soul".into(), soul_json);
        }
    }

    let (kb_context, topology) = tokio::join!(
        fetch_kb_context(state.db.as_ref(), &state.kb_embedder, user_message),
        fetch_topology_context(state.clickhouse.as_ref(), state.redis.as_ref(), project_id),
    );
    if !kb_context.is_empty() {
        prompt_variables.insert(
            "kb_context".into(),
            serde_json::Value::String(kb_context),
        );
    }
    if !topology.is_empty() {
        prompt_variables.insert("topology".into(), serde_json::Value::String(topology));
    }

    let loop_span = tracing::Span::current();

    let history_rows: Vec<Message> = async {
        sqlx::query_as(
            "SELECT id, conversation_id, role, content, tool_calls, \
                    tool_call_id, tool_name, metadata, created_at \
             FROM ( \
                 SELECT id, conversation_id, role, content, tool_calls, \
                        tool_call_id, tool_name, metadata, created_at \
                 FROM agent_messages \
                 WHERE conversation_id = $1 \
                 ORDER BY created_at DESC \
                 LIMIT 200 \
             ) recent \
             ORDER BY created_at ASC",
        )
        .bind(conversation_id)
        .fetch_all(state.db.as_ref())
        .await
    }
    .instrument(tracing::info_span!("agent.load_history", %conversation_id))
    .await?;

    let mut messages = prune_stale_tool_results(&history_rows, TOOL_FRESHNESS_TURNS);
    drop_orphaned_tool_messages(&mut messages);

    // Build user message content — may be multimodal if attachments are present
    let user_content = build_user_content(state, project_id, user_message, &attachment_ids).await;

    messages.push(ChatMessage {
        role: MessageRole::User,
        content: Some(user_content),
        name: None,
        tool_calls: None,
        tool_call_id: None,
        reasoning_content: None,
    });

    let page_meta = page_context.map(|pc| {
        serde_json::json!({
            "route": pc.route,
            "entity_id": pc.entity_id,
            "entity_type": pc.entity_type,
        })
    });
    {
        let db = state.db.clone();
        let user_msg = user_message.to_string();
        tokio::spawn(async move {
            if let Err(e) = save_message_owned(
                &db,
                conversation_id,
                "user",
                Some(&user_msg),
                None,
                None,
                None,
                page_meta,
            )
            .await
            {
                tracing::warn!(error = %e, "Background save of user message failed");
            }
        });
    }

    let agent_scopes = settings.agent_scopes.clone();

    let mcp_tools = state.action_registry.tools_list_filtered(&agent_scopes);
    let gateway_tools = mcp_tools_to_gateway_tools(&mcp_tools);
    loop_span.record("tool_count", gateway_tools.len() as u64);

    let action_ctx = state.build_action_context(
        project_id,
        reiver_mcp::action::Caller::User {
            user_id,
            jwt: user_jwt.to_string(),
        },
        agent_scopes,
        (
            "agent_chat",
            &conversation_id.to_string(),
            "user chat request",
        ),
    );

    // Agentic loop (streaming)
    let mut turn_count: usize = 0;
    let mut cumulative_input_tokens: u64 = 0;
    let mut cumulative_output_tokens: u64 = 0;
    let pv = if prompt_variables.is_empty() {
        None
    } else {
        Some(prompt_variables.clone())
    };
    let token_budget = (CONTEXT_TOKEN_BUDGET as f32 * COMPACT_THRESHOLD) as u32;

    for turn in 0..MAX_AGENT_TURNS {
        turn_count += 1;

        // Context compression: Layer 1 (snip) then Layer 2 (auto-compact)
        let pre_compact = snip_compact(&mut messages, token_budget, 3);
        if pre_compact > token_budget {
            auto_compact(&mut messages, &moodeng, Some(&conversation_id.to_string())).await;
        }

        // Warn the model when it's running low on tool turns
        let remaining = MAX_AGENT_TURNS - turn;
        if remaining == 2 {
            messages.push(ChatMessage {
                role: MessageRole::System,
                content: Some(MessageContent::Text(
                    "You have 2 tool turns remaining. Summarize your findings and answer the user's question now. Do not start new investigations.".to_string(),
                )),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            });
        }

        let turn_span = tracing::info_span!(
            "agent.turn",
            turn = turn_count,
            message_count = messages.len(),
            outcome = tracing::field::Empty,
        );
        let turn_result: anyhow::Result<bool> = async {
        let chat_request = ChatCompletionRequest {
            model: String::new(),
            messages: messages.clone(),
            temperature: None,
            max_tokens: None,
            top_p: None,
            n: None,
            stream: Some(true),
            stream_options: None,
            stop: None,
            frequency_penalty: None,
            presence_penalty: None,
            user: None,
            seed: None,
            tools: if gateway_tools.is_empty() { None } else { Some(gateway_tools.clone()) },
            tool_choice: None,
            response_format: None,
            thinking: None,
            reasoning_effort: None,
            prompt_config: prompt_config.clone(),
            prompt_variables: pv.clone(),
            models: None,
            provider: None,
        };

        let llm_span = tracing::info_span!(
            "agent.llm_call",
            message_count = messages.len(),
            input_tokens = tracing::field::Empty,
            output_tokens = tracing::field::Empty,
            total_tokens = tracing::field::Empty,
            finish_reason = tracing::field::Empty,
            ttft_ms = tracing::field::Empty,
            gen_ai.usage.cache_read.input_tokens = tracing::field::Empty,
        );
        let gateway_result = match moodeng.call_llm_stream(
            &chat_request,
            Some(&conversation_id.to_string()),
        )
        .await
        {
            Ok(r) => r,
            Err(crate::api::gateway_client::GatewayCallError::ContextTooLong) => {
                tracing::Span::current().record("outcome", "context_too_long");
                let _ = tx
                    .send(AgentEvent::Status {
                        content: "context_too_long".to_string(),
                    })
                    .await;
                let _ = tx
                    .send(AgentEvent::TextDelta {
                        content: "The conversation has grown too long for the model. Please start a new conversation."
                            .to_string(),
                    })
                    .await;
                let _ = tx.send(AgentEvent::Done { conversation_id }).await;
                return Ok(true);
            }
            Err(crate::api::gateway_client::GatewayCallError::RateLimited { .. }) => {
                tracing::Span::current().record("outcome", "rate_limited");
                let _ = tx
                    .send(AgentEvent::Status {
                        content: "rate_limited".to_string(),
                    })
                    .await;
                let _ = tx
                    .send(AgentEvent::TextDelta {
                        content: "The AI service is temporarily rate limited. Please try again shortly."
                            .to_string(),
                    })
                    .await;
                let _ = tx.send(AgentEvent::Done { conversation_id }).await;
                return Ok(true);
            }
            Err(crate::api::gateway_client::GatewayCallError::PaymentRequired { .. }) => {
                tracing::Span::current().record("outcome", "payment_required");
                let _ = tx
                    .send(AgentEvent::Status {
                        content: "payment_required".to_string(),
                    })
                    .await;
                let _ = tx
                    .send(AgentEvent::TextDelta {
                        content: "Your organization needs a payment method or credits to use this feature. Please add a payment method in **Settings → Billing** to continue."
                            .to_string(),
                    })
                    .await;
                let _ = tx.send(AgentEvent::Done { conversation_id }).await;
                return Ok(true);
            }
            Err(crate::api::gateway_client::GatewayCallError::ProviderBillingError { message }) => {
                tracing::Span::current().record("outcome", "provider_billing_error");
                let _ = tx
                    .send(AgentEvent::Status {
                        content: "provider_billing_error".to_string(),
                    })
                    .await;
                let _ = tx
                    .send(AgentEvent::TextDelta {
                        content: format!(
                            "The AI provider returned a billing error: {message}. Please check your provider account balance or switch to a different integration in **Settings → LLM Gateway**."
                        ),
                    })
                    .await;
                let _ = tx.send(AgentEvent::Done { conversation_id }).await;
                return Ok(true);
            }
            Err(e) => {
                return Err(anyhow::anyhow!("Gateway stream failed: {e}"));
            }
        };

        if !gateway_result.provider.is_empty() {
            last_provider = gateway_result.provider.clone();
        }

        let streamed = consume_llm_stream(gateway_result.stream, tx)
            .instrument(llm_span.clone())
            .await?;

        if streamed.usage.total_tokens > 0 {
            llm_span.record("input_tokens", streamed.usage.prompt_tokens as u64);
            llm_span.record("output_tokens", streamed.usage.completion_tokens as u64);
            llm_span.record("total_tokens", streamed.usage.total_tokens as u64);
        }
        if let Some(ref details) = streamed.usage.prompt_tokens_details {
            if details.cached_tokens > 0 {
                llm_span.record("gen_ai.usage.cache_read.input_tokens", details.cached_tokens as i64);
            }
        }
        llm_span.record("finish_reason", streamed.finish_reason.as_str());
        if let Some(ttft) = streamed.ttft_ms {
            llm_span.record("ttft_ms", ttft);
        }

        metrics.token_usage.add(
            streamed.usage.prompt_tokens as u64,
            &[
                opentelemetry::KeyValue::new("gen_ai.token.type", "input"),
                opentelemetry::KeyValue::new("gen_ai.operation.name", "agent_chat"),
                opentelemetry::KeyValue::new("gen_ai.request.model", streamed.model.clone()),
                opentelemetry::KeyValue::new("gen_ai.system", last_provider.clone()),
            ],
        );
        metrics.token_usage.add(
            streamed.usage.completion_tokens as u64,
            &[
                opentelemetry::KeyValue::new("gen_ai.token.type", "output"),
                opentelemetry::KeyValue::new("gen_ai.operation.name", "agent_chat"),
                opentelemetry::KeyValue::new("gen_ai.request.model", streamed.model.clone()),
                opentelemetry::KeyValue::new("gen_ai.system", last_provider.clone()),
            ],
        );
        cumulative_input_tokens += streamed.usage.prompt_tokens as u64;
        cumulative_output_tokens += streamed.usage.completion_tokens as u64;
        if !streamed.model.is_empty() {
            last_model = streamed.model.clone();
        }

        let has_tool_calls = !streamed.tool_calls.is_empty();

        if has_tool_calls {
            tracing::Span::current().record("outcome", "tool_use");
            let tool_calls = &streamed.tool_calls;

            let tc_json = serde_json::to_value(tool_calls)?;
            {
                let db = state.db.clone();
                let assistant_content = if streamed.content.is_empty() { None } else { Some(streamed.content.clone()) };
                let tc_json = tc_json.clone();
                let thinking_meta = if streamed.thinking.is_empty() {
                    None
                } else {
                    Some(serde_json::json!({ "reasoning_content": streamed.thinking }))
                };
                tokio::spawn(async move {
                    if let Err(e) = save_message_owned(
                        &db, conversation_id, "assistant",
                        assistant_content.as_deref(), Some(&tc_json), None, None, thinking_meta,
                    ).await {
                        tracing::warn!(error = %e, "Background save of assistant tool-call message failed");
                    }
                });
            }

            messages.push(ChatMessage {
                role: MessageRole::Assistant,
                content: if streamed.content.is_empty() { None } else { Some(MessageContent::Text(streamed.content.clone())) },
                name: None,
                tool_calls: Some(tool_calls.clone()),
                tool_call_id: None,
                reasoning_content: if streamed.thinking.is_empty() { None } else { Some(streamed.thinking.clone()) },
            });

            for tc in tool_calls {
                let input: serde_json::Value =
                    serde_json::from_str(&tc.function.arguments).unwrap_or(serde_json::json!({}));

                let effective_tool_name = resolve_tool_name(&tc.function.name, &input);

                let _ = tx
                    .send(AgentEvent::ToolStart {
                        call_id: tc.id.clone(),
                        name: effective_tool_name.clone(),
                        input: input.clone(),
                    })
                    .await;
                metrics.agent_tool_calls.add(1, &[opentelemetry::KeyValue::new("tool_name", effective_tool_name.clone())]);

                let tool_span = tracing::info_span!(
                    "agent.tool.execute",
                    tool_name = %effective_tool_name,
                    call_id = %tc.id,
                );

                let result_text = async {
                    let tool_result = tokio::time::timeout(
                        TOOL_TIMEOUT,
                        state.action_registry.call_tool(&tc.function.name, input, &action_ctx),
                    )
                    .await;

                    match tool_result {
                        Ok(Ok(call_result)) => extract_tool_text(&call_result.content),
                        Ok(Err(e)) => format!("Error: {}", e.message),
                        Err(_) => "Error: tool execution timed out after 30s".to_string(),
                    }
                }
                .instrument(tool_span)
                .await;

                let result_text = truncate_tool_result(&result_text);

                let result_json: serde_json::Value =
                    serde_json::from_str(&result_text).unwrap_or(serde_json::Value::String(result_text.clone()));

                let _ = tx
                    .send(AgentEvent::ToolResult {
                        call_id: tc.id.clone(),
                        name: effective_tool_name.clone(),
                        output: result_json.clone(),
                    })
                    .await;

                if effective_tool_name == "create_secret_slot" {
                    if let Some(slot_id_str) = result_json.get("slot_id").and_then(|v| v.as_str()) {
                        if let Ok(slot_id) = Uuid::parse_str(slot_id_str) {
                            let poll_span = tracing::info_span!("agent.poll_slot", %slot_id);
                            let _ = tx
                                .send(AgentEvent::Status {
                                    content: "waiting_for_secret".to_string(),
                                })
                                .await;
                            async {
                                let mut filled = false;
                                for attempt in 0..60u32 {
                                    tokio::time::sleep(Duration::from_secs(2)).await;
                                    match crate::api::secret_slots::is_slot_filled(&state.db, slot_id).await {
                                        Ok(true) => {
                                            tracing::info!(attempt, "Secret slot filled by user");
                                            filled = true;
                                            break;
                                        }
                                        Ok(false) => {
                                            if attempt % 7 == 6 {
                                                let _ = tx
                                                    .send(AgentEvent::Status {
                                                        content: "waiting_for_secret".to_string(),
                                                    })
                                                    .await;
                                            }
                                        }
                                        Err(e) => {
                                            tracing::warn!(error = %e, "Failed to poll slot status");
                                            break;
                                        }
                                    }
                                }
                                if filled {
                                    let _ = tx
                                        .send(AgentEvent::Status {
                                            content: "secret_deposited".to_string(),
                                        })
                                        .await;
                                }
                            }
                            .instrument(poll_span)
                            .await;
                        }
                    }
                }

                {
                    let db = state.db.clone();
                    let result_text_owned = result_text.clone();
                    let tc_id = tc.id.clone();
                    let tc_name = effective_tool_name.clone();
                    tokio::spawn(async move {
                        if let Err(e) = save_message_owned(
                            &db, conversation_id, "tool",
                            Some(&result_text_owned), None, Some(&tc_id), Some(&tc_name), None,
                        ).await {
                            tracing::warn!(error = %e, "Background save of tool result failed");
                        }
                    });
                }

                messages.push(ChatMessage {
                    role: MessageRole::Tool,
                    content: Some(MessageContent::Text(result_text)),
                    name: None,
                    tool_calls: None,
                    tool_call_id: Some(tc.id.clone()),
                    reasoning_content: None,
                });
            }
            Ok(false)
        } else {
            tracing::Span::current().record("outcome", "final_response");

            let _ = tx.send(AgentEvent::Done { conversation_id }).await;

            let db = state.db.clone();
            let final_text = streamed.content.clone();
            tokio::spawn(async move {
                if let Err(e) = save_message_owned(
                    &db, conversation_id, "assistant",
                    Some(&final_text), None, None, None, None,
                ).await {
                    tracing::warn!(error = %e, "Background save of final assistant message failed");
                }
                let _ = sqlx::query(
                    "UPDATE agent_conversations SET updated_at = NOW() WHERE id = $1",
                )
                .bind(conversation_id)
                .execute(db.as_ref())
                .await;
            });

            Ok(true)
        }
        }
        .instrument(turn_span)
        .await;

        let turn_done = turn_result?;
        if turn_done {
            loop_span.record("turn_count", turn_count);
            loop_span.record("total_input_tokens", cumulative_input_tokens);
            loop_span.record("total_output_tokens", cumulative_output_tokens);
            metrics.agent_turns.record(turn_count as f64, &metric_attrs);
            metrics.operation_duration.record(
                loop_start.elapsed().as_secs_f64(),
                &[
                    opentelemetry::KeyValue::new("gen_ai.operation.name", "agent_chat"),
                    opentelemetry::KeyValue::new("gen_ai.request.model", last_model.clone()),
                    opentelemetry::KeyValue::new("gen_ai.system", last_provider.clone()),
                ],
            );
            return Ok(());
        }
    }

    loop_span.record("turn_count", turn_count);
    loop_span.record("total_input_tokens", cumulative_input_tokens);
    loop_span.record("total_output_tokens", cumulative_output_tokens);
    metrics.agent_turns.record(turn_count as f64, &metric_attrs);
    metrics.operation_duration.record(
        loop_start.elapsed().as_secs_f64(),
        &[
            opentelemetry::KeyValue::new("gen_ai.operation.name", "agent_chat"),
            opentelemetry::KeyValue::new("gen_ai.request.model", last_model.clone()),
            opentelemetry::KeyValue::new("gen_ai.system", last_provider.clone()),
        ],
    );
    let _ = tx
        .send(AgentEvent::Status {
            content: "max_turns".to_string(),
        })
        .await;
    let _ = tx
        .send(AgentEvent::TextDelta {
            content: "I've reached the maximum number of tool calls for this turn. Please continue with a new message."
                .to_string(),
        })
        .await;
    let _ = tx.send(AgentEvent::Done { conversation_id }).await;
    Ok(())
}

const KB_SIMILARITY_THRESHOLD: f64 = 0.6;
const KB_TOKEN_BUDGET: usize = 8000;
const KB_CANDIDATE_LIMIT: i64 = 20;

fn estimate_tokens(text: &str) -> usize {
    text.len() / 4
}

async fn fetch_kb_context(
    db: &sqlx::PgPool,
    embedder: &reiver_core::embeddings::KbEmbedder,
    user_message: &str,
) -> String {
    let query_embedding = match embedder.embed(vec![user_message.to_string()]).await {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(error = %e, "KB pre-fetch embedding failed");
            return String::new();
        }
    };
    let query_vec = match query_embedding.into_iter().next() {
        Some(v) => v,
        None => return String::new(),
    };

    let vec_str = format!(
        "[{}]",
        query_vec
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );

    let rows: Vec<(String, String, String, f64)> = match sqlx::query_as(
        "SELECT d.title, d.category, c.content, \
                1 - (c.embedding <=> $1::vector) AS similarity \
         FROM knowledge_base_chunks c \
         JOIN knowledge_base_documents d ON c.document_id = d.id \
         WHERE d.enabled = true AND d.embedding_status = 'ready' \
         ORDER BY c.embedding <=> $1::vector \
         LIMIT $2",
    )
    .bind(&vec_str)
    .bind(KB_CANDIDATE_LIMIT)
    .fetch_all(db)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "KB pre-fetch query failed");
            return String::new();
        }
    };

    let mut parts = Vec::new();
    let mut tokens_used: usize = 0;

    for (title, category, content, sim) in rows {
        if sim < KB_SIMILARITY_THRESHOLD {
            break;
        }
        let entry = format!("**{title}** ({category}):\n{content}");
        let entry_tokens = estimate_tokens(&entry);
        if tokens_used + entry_tokens > KB_TOKEN_BUDGET && !parts.is_empty() {
            break;
        }
        tokens_used += entry_tokens;
        parts.push(entry);
    }

    parts.join("\n\n---\n\n")
}

const TOPOLOGY_CACHE_TTL_SECS: u64 = 600;

async fn fetch_topology_context(
    clickhouse: &reiver_core::clickhouse_db::ClickHousePool,
    redis: &crate::app_state::RedisPool,
    project_id: Uuid,
) -> String {
    use std::collections::BTreeMap;

    let cache_key = format!("topology:{project_id}");

    if let Ok(mut conn) = redis.get().await {
        if let Ok(val) = bb8_redis::redis::cmd("GET")
            .arg(&cache_key)
            .query_async::<Option<String>>(&mut *conn)
            .await
        {
            if let Some(cached) = val {
                return cached;
            }
        }
    }

    let pid = project_id.to_string();

    let (trace_svcs, log_svcs, metric_svcs, infra_rows, metric_prefixes) = tokio::join!(
        query_trace_services(clickhouse, &pid),
        query_log_services(clickhouse, &pid),
        query_metric_services(clickhouse, &pid),
        query_infra(clickhouse, &pid),
        query_metric_prefixes(clickhouse, &pid),
    );

    #[derive(Default)]
    struct Entry {
        has_traces: bool,
        trace_health: Option<String>,
        has_logs: bool,
        has_metrics: bool,
        statefulsets: Vec<String>,
    }

    let mut components: BTreeMap<String, Entry> = BTreeMap::new();

    for svc in &trace_svcs {
        let e = components.entry(svc.service_name.clone()).or_default();
        e.has_traces = true;
        let err_pct = if svc.spans > 0 {
            (svc.errors as f64 / svc.spans as f64 * 100.0).round() as u64
        } else {
            0
        };
        e.trace_health = Some(if err_pct > 0 {
            format!("{err_pct}% errors")
        } else {
            "healthy".into()
        });
    }

    for name in &log_svcs {
        components.entry(name.clone()).or_default().has_logs = true;
    }

    for name in &metric_svcs {
        components.entry(name.clone()).or_default().has_metrics = true;
    }

    for (svc, sts) in &infra_rows {
        let e = components.entry(svc.clone()).or_default();
        if !e.statefulsets.contains(sts) {
            e.statefulsets.push(sts.clone());
        }
    }
    for e in components.values_mut() {
        e.statefulsets.sort();
    }

    if components.is_empty() && metric_prefixes.is_empty() {
        return String::new();
    }

    let mut sections = Vec::new();

    if !components.is_empty() {
        let mut lines = Vec::new();
        for (name, e) in &components {
            let mut signals = Vec::new();
            if e.has_traces {
                let health = e.trace_health.as_deref().unwrap_or("healthy");
                signals.push(format!("traces ({health})"));
            }
            if e.has_logs {
                signals.push("logs".into());
            }
            if e.has_metrics {
                signals.push("metrics".into());
            }
            let mut line = format!("- {name}: {}", signals.join(", "));
            if !e.statefulsets.is_empty() {
                line.push_str(&format!(" [statefulsets: {}]", e.statefulsets.join(", ")));
            }
            lines.push(line);
        }
        sections.push(format!("Components:\n{}", lines.join("\n")));
    }

    if !metric_prefixes.is_empty() {
        let prefixes: Vec<String> = metric_prefixes
            .iter()
            .map(|(p, c)| format!("{p} ({c})"))
            .collect();
        sections.push(format!("Metric prefixes: {}", prefixes.join(", ")));
    }

    let result = sections.join("\n\n");

    if !result.is_empty() {
        if let Ok(mut conn) = redis.get().await {
            let _ = bb8_redis::redis::cmd("SET")
                .arg(&cache_key)
                .arg(&result)
                .arg("EX")
                .arg(TOPOLOGY_CACHE_TTL_SECS)
                .query_async::<()>(&mut *conn)
                .await;
        }
    }

    result
}

struct TraceSvc {
    service_name: String,
    spans: u64,
    errors: u64,
}

async fn query_trace_services(
    ch: &reiver_core::clickhouse_db::ClickHousePool,
    project_id: &str,
) -> Vec<TraceSvc> {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct Row {
        service_name: String,
        spans: u64,
        errors: u64,
    }

    match ch
        .query(&format!(
            "SELECT service_name, sum(span_count) AS spans, sum(error_count) AS errors \
             FROM reiver.discovered_services_agg \
             WHERE project_id = '{}' \
             GROUP BY service_name \
             ORDER BY spans DESC",
            project_id
        ))
        .fetch_all::<Row>()
        .await
    {
        Ok(rows) => rows
            .into_iter()
            .map(|r| TraceSvc {
                service_name: r.service_name,
                spans: r.spans,
                errors: r.errors,
            })
            .collect(),
        Err(e) => {
            tracing::warn!(error = %e, "topology: trace services query failed");
            Vec::new()
        }
    }
}

async fn query_log_services(
    ch: &reiver_core::clickhouse_db::ClickHousePool,
    project_id: &str,
) -> Vec<String> {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct Row {
        service_name: String,
    }

    match ch
        .query(&format!(
            "SELECT DISTINCT service_name \
             FROM reiver.logs \
             WHERE project_id = '{}' \
               AND timestamp >= now() - INTERVAL 24 HOUR \
               AND service_name != ''",
            project_id
        ))
        .fetch_all::<Row>()
        .await
    {
        Ok(rows) => rows.into_iter().map(|r| r.service_name).collect(),
        Err(e) => {
            tracing::warn!(error = %e, "topology: log services query failed");
            Vec::new()
        }
    }
}

async fn query_metric_services(
    ch: &reiver_core::clickhouse_db::ClickHousePool,
    project_id: &str,
) -> Vec<String> {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct Row {
        service_name: String,
    }

    match ch
        .query(&format!(
            "SELECT DISTINCT resource_attributes['service.name'] AS service_name \
             FROM reiver.time_series_v1 \
             WHERE project_id = '{}' \
               AND resource_attributes['service.name'] != ''",
            project_id
        ))
        .fetch_all::<Row>()
        .await
    {
        Ok(rows) => rows.into_iter().map(|r| r.service_name).collect(),
        Err(e) => {
            tracing::warn!(error = %e, "topology: metric services query failed");
            Vec::new()
        }
    }
}

async fn query_infra(
    ch: &reiver_core::clickhouse_db::ClickHousePool,
    project_id: &str,
) -> Vec<(String, String)> {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct Row {
        service_name: String,
        statefulset: String,
    }

    match ch
        .query(&format!(
            "SELECT DISTINCT \
                 service_name, \
                 resource_attributes['k8s.statefulset.name'] AS statefulset \
             FROM reiver.logs \
             WHERE project_id = '{}' \
               AND timestamp >= now() - INTERVAL 24 HOUR \
               AND resource_attributes['k8s.statefulset.name'] != ''",
            project_id
        ))
        .fetch_all::<Row>()
        .await
    {
        Ok(rows) => rows
            .into_iter()
            .map(|r| (r.service_name, r.statefulset))
            .collect(),
        Err(e) => {
            tracing::warn!(error = %e, "topology: infra query failed");
            Vec::new()
        }
    }
}

async fn query_metric_prefixes(
    ch: &reiver_core::clickhouse_db::ClickHousePool,
    project_id: &str,
) -> Vec<(String, u64)> {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct Row {
        prefix: String,
        cnt: u64,
    }

    match ch
        .query(&format!(
            "SELECT \
                 splitByRegexp('[._]', metric_name)[1] AS prefix, \
                 count() AS cnt \
             FROM reiver.time_series_v1 \
             WHERE project_id = '{}' \
               AND unix_milli >= toUnixTimestamp(now() - INTERVAL 24 HOUR) * 1000 \
             GROUP BY prefix \
             HAVING prefix != '' \
             ORDER BY cnt DESC \
             LIMIT 30",
            project_id
        ))
        .fetch_all::<Row>()
        .await
    {
        Ok(rows) => rows.into_iter().map(|r| (r.prefix, r.cnt)).collect(),
        Err(e) => {
            tracing::warn!(error = %e, "topology: metric prefixes query failed");
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::agent_context::drop_orphaned_tool_messages;
    use crate::gateway::types::{
        ChatMessage, FunctionCall, MessageContent, MessageRole, ToolCall, ToolType,
    };

    fn user_msg(text: &str) -> ChatMessage {
        ChatMessage {
            role: MessageRole::User,
            content: Some(MessageContent::Text(text.into())),
            name: None,
            tool_calls: None,
            tool_call_id: None,
            reasoning_content: None,
        }
    }

    fn assistant_text(text: &str) -> ChatMessage {
        ChatMessage {
            role: MessageRole::Assistant,
            content: Some(MessageContent::Text(text.into())),
            name: None,
            tool_calls: None,
            tool_call_id: None,
            reasoning_content: None,
        }
    }

    fn assistant_tool_calls(ids: &[&str]) -> ChatMessage {
        ChatMessage {
            role: MessageRole::Assistant,
            content: None,
            name: None,
            tool_calls: Some(
                ids.iter()
                    .map(|id| ToolCall {
                        index: None,
                        id: id.to_string(),
                        tool_type: ToolType::Function,
                        function: FunctionCall {
                            name: "test_tool".into(),
                            arguments: "{}".into(),
                        },
                    })
                    .collect(),
            ),
            tool_call_id: None,
            reasoning_content: None,
        }
    }

    fn tool_result(call_id: &str) -> ChatMessage {
        ChatMessage {
            role: MessageRole::Tool,
            content: Some(MessageContent::Text("result".into())),
            name: None,
            tool_calls: None,
            tool_call_id: Some(call_id.into()),
            reasoning_content: None,
        }
    }

    /// Validates that a message array meets the OpenAI API contract:
    /// every tool message must follow an assistant message with a matching tool_call_id,
    /// and every assistant tool_call must have a corresponding tool response before the
    /// next user/assistant message.
    fn assert_valid_message_sequence(messages: &[ChatMessage]) {
        use std::collections::HashSet;

        let mut pending_tool_call_ids: HashSet<String> = HashSet::new();

        for (i, msg) in messages.iter().enumerate() {
            match msg.role {
                MessageRole::Assistant => {
                    assert!(
                        pending_tool_call_ids.is_empty(),
                        "At message {i}: new assistant message while tool_calls {:?} are still unanswered",
                        pending_tool_call_ids
                    );
                    if let Some(tcs) = &msg.tool_calls {
                        for tc in tcs {
                            pending_tool_call_ids.insert(tc.id.clone());
                        }
                    }
                }
                MessageRole::Tool => {
                    let id = msg.tool_call_id.as_deref().unwrap_or("");
                    assert!(
                        pending_tool_call_ids.remove(id),
                        "At message {i}: tool response for '{id}' has no pending tool_call"
                    );
                }
                MessageRole::User => {
                    assert!(
                        pending_tool_call_ids.is_empty(),
                        "At message {i}: user message while tool_calls {:?} are still unanswered",
                        pending_tool_call_ids
                    );
                }
                _ => {}
            }
        }
    }

    #[test]
    fn test_drop_orphaned_removes_tool_without_assistant() {
        let mut messages = vec![
            tool_result("orphan_tc"),
            user_msg("hello"),
            assistant_text("hi"),
        ];
        drop_orphaned_tool_messages(&mut messages);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].role, MessageRole::User);
        assert_valid_message_sequence(&messages);
    }

    #[test]
    fn test_drop_orphaned_removes_tail_assistant_with_unanswered() {
        let mut messages = vec![
            user_msg("do something"),
            assistant_tool_calls(&["tc1", "tc2"]),
        ];
        drop_orphaned_tool_messages(&mut messages);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].role, MessageRole::User);
        assert_valid_message_sequence(&messages);
    }

    #[test]
    fn test_drop_orphaned_injects_stubs_for_partial_mid_history() {
        // Simulates: agent crashed after executing tc1 but before tc2,
        // then a new user message was stored.
        let mut messages = vec![
            user_msg("check both"),
            assistant_tool_calls(&["tc1", "tc2"]),
            tool_result("tc1"),
            user_msg("followup"),
            assistant_text("answer"),
        ];
        drop_orphaned_tool_messages(&mut messages);

        // tc2 should now have a stub response
        assert_eq!(messages.len(), 6);
        assert_eq!(messages[3].role, MessageRole::Tool);
        assert_eq!(messages[3].tool_call_id.as_deref(), Some("tc2"));
        assert!(messages[3]
            .content
            .as_ref()
            .unwrap()
            .as_text()
            .contains("unavailable"));
        assert_valid_message_sequence(&messages);
    }

    #[test]
    fn test_drop_orphaned_injects_stubs_all_unanswered_mid_history() {
        // Agent crashed before executing any tool calls, then user sent another message.
        let mut messages = vec![
            user_msg("question"),
            assistant_tool_calls(&["tc1", "tc2"]),
            user_msg("another question"),
            assistant_text("response"),
        ];
        drop_orphaned_tool_messages(&mut messages);

        // Both tc1 and tc2 should have stub responses inserted
        assert_eq!(messages.len(), 6);
        assert_eq!(messages[2].role, MessageRole::Tool);
        assert_eq!(messages[2].tool_call_id.as_deref(), Some("tc1"));
        assert_eq!(messages[3].role, MessageRole::Tool);
        assert_eq!(messages[3].tool_call_id.as_deref(), Some("tc2"));
        assert_valid_message_sequence(&messages);
    }

    #[test]
    fn test_drop_orphaned_valid_sequence_unchanged() {
        let mut messages = vec![
            user_msg("question"),
            assistant_tool_calls(&["tc1"]),
            tool_result("tc1"),
            assistant_text("done"),
            user_msg("thanks"),
        ];
        let original_len = messages.len();
        drop_orphaned_tool_messages(&mut messages);
        assert_eq!(messages.len(), original_len);
        assert_valid_message_sequence(&messages);
    }

    #[test]
    fn test_drop_orphaned_multiple_incomplete_turns() {
        // Two separate incomplete tool-call sequences in history.
        let mut messages = vec![
            user_msg("first"),
            assistant_tool_calls(&["tc1", "tc2"]),
            tool_result("tc1"),
            // tc2 missing
            user_msg("second"),
            assistant_tool_calls(&["tc3", "tc4"]),
            tool_result("tc3"),
            // tc4 missing
            user_msg("third"),
        ];
        drop_orphaned_tool_messages(&mut messages);
        assert_valid_message_sequence(&messages);

        // Verify stubs were injected for tc2 and tc4
        let tool_ids: Vec<&str> = messages
            .iter()
            .filter(|m| m.role == MessageRole::Tool)
            .map(|m| m.tool_call_id.as_deref().unwrap_or(""))
            .collect();
        assert!(tool_ids.contains(&"tc1"));
        assert!(tool_ids.contains(&"tc2"));
        assert!(tool_ids.contains(&"tc3"));
        assert!(tool_ids.contains(&"tc4"));
    }

    #[test]
    fn agent_event_json_text_delta() {
        let ev = AgentEvent::TextDelta {
            content: "hello".to_string(),
        };
        let v = serde_json::to_value(&ev).unwrap();
        assert_eq!(v["type"], "text_delta");
        assert_eq!(v["content"], "hello");
        assert!(v.as_object().unwrap().len() == 2);
    }

    #[test]
    fn agent_event_json_tool_start() {
        let ev = AgentEvent::ToolStart {
            call_id: "c1".to_string(),
            name: "my_tool".to_string(),
            input: serde_json::json!({"x": 1}),
        };
        let v = serde_json::to_value(&ev).unwrap();
        assert_eq!(v["type"], "tool_start");
        assert_eq!(v["call_id"], "c1");
        assert_eq!(v["name"], "my_tool");
        assert_eq!(v["input"], serde_json::json!({"x": 1}));
    }

    #[test]
    fn agent_event_json_conversation_created() {
        let id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let ev = AgentEvent::ConversationCreated {
            conversation_id: id,
        };
        let s = serde_json::to_string(&ev).unwrap();
        assert!(s.contains("\"type\":\"conversation_created\""));
        assert!(s.contains("550e8400-e29b-41d4-a716-446655440000"));
    }

    #[test]
    fn agent_event_json_error() {
        let ev = AgentEvent::Error {
            error: "oops".to_string(),
        };
        let v = serde_json::to_value(&ev).unwrap();
        assert_eq!(v["type"], "error");
        assert_eq!(v["error"], "oops");
    }
}
