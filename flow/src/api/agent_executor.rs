//! Headless tool loop executor for the in-app agent.
//!
//! Contains the core LLM + tool-call loop used by both the interactive
//! agent and headless investigation/task endpoints.

use rmcp::model::RawContent;
use serde::Serialize;
use std::time::Duration;
use uuid::Uuid;

use crate::api::agent::mcp_tools_to_gateway_tools;
use crate::api::agent_context::{snip_compact, COMPACT_THRESHOLD, CONTEXT_TOKEN_BUDGET};
use crate::app_state::FlowState;
use crate::gateway::types::{ChatCompletionRequest, ChatMessage, MessageContent, MessageRole};

pub const TOOL_TIMEOUT: Duration = Duration::from_secs(30);
pub const MAX_TOOL_RESULT_BYTES: usize = 8_192;

/// Extract plain text from MCP content items.
pub fn extract_tool_text(content: &[rmcp::model::Content]) -> String {
    content
        .iter()
        .filter_map(|c| match &c.raw {
            RawContent::Text(tc) => Some(tc.text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// For facade tools like `execute`, extract the specific action name so the UI
/// can render specialized components (e.g. SecretDepositCard for create_secret_slot).
pub fn resolve_tool_name(tool_name: &str, input: &serde_json::Value) -> String {
    if tool_name == "execute" {
        if let Some(action) = input.get("action").and_then(|v| v.as_str()) {
            return action.to_string();
        }
    }
    tool_name.to_string()
}

pub fn truncate_tool_result(text: &str) -> String {
    if text.len() <= MAX_TOOL_RESULT_BYTES {
        return text.to_string();
    }
    let mut truncated = text[..MAX_TOOL_RESULT_BYTES].to_string();
    truncated.push_str("\n... [truncated]");
    truncated
}

/// One recorded tool call for the audit trail.
#[derive(Debug, Clone, Serialize)]
pub struct ToolCallRecord {
    pub tool_name: String,
    pub result_preview: String,
}

/// Why the agent loop terminated.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LoopOutcome {
    Completed {
        assistant_text: String,
    },
    MaxTurns {
        assistant_text: String,
        turns_used: usize,
    },
    ContextTooLong {
        assistant_text: String,
        estimated_tokens: u32,
    },
    RateLimited {
        turns_completed: usize,
    },
    ModelError {
        error: String,
        turns_completed: usize,
    },
    Aborted {
        reason: String,
    },
}

impl LoopOutcome {
    pub fn assistant_text(&self) -> &str {
        match self {
            Self::Completed { assistant_text }
            | Self::MaxTurns { assistant_text, .. }
            | Self::ContextTooLong { assistant_text, .. } => assistant_text,
            Self::RateLimited { .. } | Self::ModelError { .. } | Self::Aborted { .. } => "",
        }
    }

    pub fn error_detail(&self) -> &str {
        match self {
            Self::ModelError { error, .. } => error,
            Self::Aborted { reason } => reason,
            Self::RateLimited { .. } => "rate limited",
            Self::ContextTooLong { .. } => "context too long",
            _ => "",
        }
    }

    pub fn is_success(&self) -> bool {
        matches!(self, Self::Completed { .. })
    }

    pub fn status_str(&self) -> &'static str {
        match self {
            Self::Completed { .. } => "completed",
            Self::MaxTurns { .. } => "max_turns",
            Self::ContextTooLong { .. } => "context_too_long",
            Self::RateLimited { .. } => "rate_limited",
            Self::ModelError { .. } => "model_error",
            Self::Aborted { .. } => "aborted",
        }
    }
}

/// Callback invoked when the model produces a final text response (no tool
/// calls). Return `Some(correction)` to reject the response and inject a
/// follow-up user message, or `None` to accept. Fires at most once per loop.
pub type StopHook = Box<dyn Fn(&str) -> Option<String> + Send + Sync>;

/// Result of a headless (non-streaming) tool loop execution.
#[derive(Debug)]
pub struct ToolLoopResult {
    pub outcome: LoopOutcome,
    pub tool_calls_log: Vec<ToolCallRecord>,
    pub total_input_tokens: u64,
    pub total_output_tokens: u64,
    pub model_used: String,
}

/// Run the core LLM + tool-call loop without SSE streaming.
///
/// Used by both the interactive agent (which wraps this with SSE events)
/// and the headless auto-investigation endpoint.
pub async fn run_tool_loop(
    state: &FlowState,
    project_id: Uuid,
    action_ctx: reiver_mcp::action::ActionContext,
    prompt_config: Option<String>,
    prompt_variables: std::collections::HashMap<String, serde_json::Value>,
    user_message: String,
    max_turns: usize,
    stop_hook: Option<&StopHook>,
    session_id: Option<String>,
) -> anyhow::Result<ToolLoopResult> {
    let moodeng = crate::moodeng::MoodengClient::new(state, project_id);

    let mcp_tools = state
        .action_registry
        .tools_list_filtered(&action_ctx.scopes);
    let gateway_tools = mcp_tools_to_gateway_tools(&mcp_tools);

    let mut messages: Vec<ChatMessage> = Vec::new();
    messages.push(ChatMessage {
        role: MessageRole::User,
        content: Some(MessageContent::Text(user_message)),
        name: None,
        tool_calls: None,
        tool_call_id: None,
        reasoning_content: None,
    });

    let mut cumulative_input_tokens: u64 = 0;
    let mut cumulative_output_tokens: u64 = 0;
    let mut tool_calls_log: Vec<ToolCallRecord> = Vec::new();
    let mut model_used = String::new();
    let mut stop_hook_fired = false;

    let pv = if prompt_variables.is_empty() {
        None
    } else {
        Some(prompt_variables.clone())
    };

    let token_budget = (CONTEXT_TOKEN_BUDGET as f32 * COMPACT_THRESHOLD) as u32;

    for turn in 0..max_turns {
        snip_compact(&mut messages, token_budget, 2);

        let chat_request = ChatCompletionRequest {
            model: String::new(),
            messages: messages.clone(),
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
            tools: if gateway_tools.is_empty() {
                None
            } else {
                Some(gateway_tools.clone())
            },
            tool_choice: None,
            response_format: None,
            thinking: None,
            reasoning_effort: None,
            prompt_config: prompt_config.clone(),
            prompt_variables: pv.clone(),
            models: None,
            provider: None,
        };

        let result = match moodeng.call_llm(&chat_request, session_id.as_deref()).await {
            Ok(r) => r,
            Err(crate::api::gateway_client::GatewayCallError::ContextTooLong) => {
                return Ok(ToolLoopResult {
                    outcome: LoopOutcome::ContextTooLong {
                        assistant_text: String::new(),
                        estimated_tokens: 0,
                    },
                    tool_calls_log,
                    total_input_tokens: cumulative_input_tokens,
                    total_output_tokens: cumulative_output_tokens,
                    model_used,
                });
            }
            Err(crate::api::gateway_client::GatewayCallError::RateLimited { .. }) => {
                return Ok(ToolLoopResult {
                    outcome: LoopOutcome::RateLimited {
                        turns_completed: turn,
                    },
                    tool_calls_log,
                    total_input_tokens: cumulative_input_tokens,
                    total_output_tokens: cumulative_output_tokens,
                    model_used,
                });
            }
            Err(e) => {
                return Ok(ToolLoopResult {
                    outcome: LoopOutcome::ModelError {
                        error: e.to_string(),
                        turns_completed: turn,
                    },
                    tool_calls_log,
                    total_input_tokens: cumulative_input_tokens,
                    total_output_tokens: cumulative_output_tokens,
                    model_used,
                });
            }
        };

        cumulative_input_tokens += result.usage.prompt_tokens as u64;
        cumulative_output_tokens += result.usage.completion_tokens as u64;
        if !result.model.is_empty() {
            model_used = result.model.clone();
        }

        if result.tool_calls.is_empty() {
            if let Some(hook) = stop_hook {
                if !stop_hook_fired {
                    if let Some(correction) = hook(&result.content) {
                        stop_hook_fired = true;
                        messages.push(ChatMessage {
                            role: MessageRole::Assistant,
                            content: Some(MessageContent::Text(result.content.clone())),
                            name: None,
                            tool_calls: None,
                            tool_call_id: None,
                            reasoning_content: None,
                        });
                        messages.push(ChatMessage {
                            role: MessageRole::User,
                            content: Some(MessageContent::Text(correction)),
                            name: None,
                            tool_calls: None,
                            tool_call_id: None,
                            reasoning_content: None,
                        });
                        continue;
                    }
                }
            }
            return Ok(ToolLoopResult {
                outcome: LoopOutcome::Completed {
                    assistant_text: result.content,
                },
                tool_calls_log,
                total_input_tokens: cumulative_input_tokens,
                total_output_tokens: cumulative_output_tokens,
                model_used,
            });
        }

        let tool_calls = result.tool_calls;

        messages.push(ChatMessage {
            role: MessageRole::Assistant,
            content: if result.content.is_empty() {
                None
            } else {
                Some(MessageContent::Text(result.content.clone()))
            },
            name: None,
            tool_calls: Some(tool_calls.clone()),
            tool_call_id: None,
            reasoning_content: result.thinking.clone(),
        });

        for tc in &tool_calls {
            let input: serde_json::Value =
                serde_json::from_str(&tc.function.arguments).unwrap_or(serde_json::json!({}));

            let effective_tool_name = resolve_tool_name(&tc.function.name, &input);

            let result_text = {
                let tool_result = tokio::time::timeout(
                    TOOL_TIMEOUT,
                    state
                        .action_registry
                        .call_tool(&tc.function.name, input, &action_ctx),
                )
                .await;

                match tool_result {
                    Ok(Ok(call_result)) => extract_tool_text(&call_result.content),
                    Ok(Err(e)) => format!("Error: {}", e.message),
                    Err(_) => "Error: tool execution timed out after 30s".to_string(),
                }
            };

            let result_text = truncate_tool_result(&result_text);

            tool_calls_log.push(ToolCallRecord {
                tool_name: effective_tool_name,
                result_preview: result_text.chars().take(200).collect(),
            });

            messages.push(ChatMessage {
                role: MessageRole::Tool,
                content: Some(MessageContent::Text(result_text)),
                name: None,
                tool_calls: None,
                tool_call_id: Some(tc.id.clone()),
                reasoning_content: None,
            });
        }
    }

    Ok(ToolLoopResult {
        outcome: LoopOutcome::MaxTurns {
            assistant_text: "Task reached maximum tool call turns.".to_string(),
            turns_used: max_turns,
        },
        tool_calls_log,
        total_input_tokens: cumulative_input_tokens,
        total_output_tokens: cumulative_output_tokens,
        model_used,
    })
}
