//! Context window management for the in-app agent.
//!
//! Provides utilities for pruning stale tool results, fixing orphaned messages,
//! estimating token usage, and compacting conversation history to fit within
//! the LLM context window.

use crate::gateway::types::{
    ChatCompletionRequest, ChatMessage, MessageContent, MessageRole, ToolCall,
};

/// Estimated token budget for the agent context window.
pub const CONTEXT_TOKEN_BUDGET: u32 = 100_000;
/// Fraction of CONTEXT_TOKEN_BUDGET at which compaction triggers.
pub const COMPACT_THRESHOLD: f32 = 0.8;

/// Estimate total tokens for a slice of chat messages (chars/4 heuristic).
pub fn estimate_context_tokens(messages: &[ChatMessage]) -> u32 {
    let mut total = 0u32;
    for msg in messages {
        total += 4; // per-message overhead
        if let Some(content) = &msg.content {
            let text = content.as_text();
            total += crate::gateway::observability::estimate_tokens(&text);
        }
        if let Some(tcs) = &msg.tool_calls {
            for tc in tcs {
                total += crate::gateway::observability::estimate_tokens(&tc.function.name);
                total += crate::gateway::observability::estimate_tokens(&tc.function.arguments);
            }
        }
    }
    total
}

/// Drop the oldest complete turns (user + assistant + tool messages) from the
/// context until estimated tokens fit within the budget. Always preserves the
/// system message (if any) and the last `min_keep_turns` turns.
pub fn snip_compact(messages: &mut Vec<ChatMessage>, budget: u32, min_keep_turns: usize) -> u32 {
    let estimated = estimate_context_tokens(messages);
    if estimated <= budget {
        return estimated;
    }

    let turn_starts: Vec<usize> = messages
        .iter()
        .enumerate()
        .filter(|(_, m)| m.role == MessageRole::User)
        .map(|(i, _)| i)
        .collect();

    if turn_starts.len() <= min_keep_turns {
        return estimated;
    }

    let system_prefix_len = turn_starts.first().copied().unwrap_or(0);

    let droppable_turns = turn_starts.len() - min_keep_turns;
    let mut turns_to_drop = 0;
    let mut new_estimate = estimated;

    for t in 0..droppable_turns {
        if new_estimate <= budget {
            break;
        }
        let turn_start = turn_starts[t];
        let turn_end = turn_starts.get(t + 1).copied().unwrap_or(messages.len());
        for i in turn_start..turn_end {
            if let Some(content) = &messages[i].content {
                new_estimate = new_estimate.saturating_sub(
                    crate::gateway::observability::estimate_tokens(&content.as_text()) + 4,
                );
            }
        }
        turns_to_drop = t + 1;
    }

    if turns_to_drop > 0 {
        let cut_start = system_prefix_len;
        let cut_end = turn_starts[turns_to_drop];
        let dropped_count = cut_end - cut_start;

        messages.splice(
            cut_start..cut_end,
            std::iter::once(ChatMessage {
                role: MessageRole::System,
                content: Some(MessageContent::Text(format!(
                    "[{dropped_count} earlier messages omitted to fit context window. \
                     {turns_to_drop} turn(s) removed.]"
                ))),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            }),
        );

        tracing::info!(
            turns_dropped = turns_to_drop,
            messages_removed = dropped_count,
            estimated_before = estimated,
            estimated_after = new_estimate,
            "Snip compact: dropped old turns to fit context budget"
        );
    }

    new_estimate
}

/// Fork a cheap LLM call to summarize the dropped conversation history.
/// The summary replaces the stub left by snip_compact.
pub async fn auto_compact(
    messages: &mut Vec<ChatMessage>,
    moodeng: &crate::moodeng::MoodengClient<'_>,
    session_id: Option<&str>,
) {
    let stub_idx = messages.iter().position(|m| {
        m.role == MessageRole::System
            && m.content
                .as_ref()
                .map(|c| c.as_text().contains("earlier messages omitted"))
                .unwrap_or(false)
    });
    let stub_idx = match stub_idx {
        Some(i) => i,
        None => return,
    };

    let context_for_summary: String = messages
        .iter()
        .take(stub_idx)
        .chain(messages.iter().skip(stub_idx + 1).take(4))
        .filter_map(|m| {
            let role = match m.role {
                MessageRole::User => "User",
                MessageRole::Assistant => "Assistant",
                MessageRole::System => "System",
                MessageRole::Tool => "Tool",
                _ => "Other",
            };
            m.content
                .as_ref()
                .map(|c| format!("{role}: {}", c.as_text()))
        })
        .collect::<Vec<_>>()
        .join("\n");

    if context_for_summary.is_empty() {
        return;
    }

    let summary_prompt = format!(
        "Summarize this conversation history concisely (max 3 sentences). \
         Focus on what the user asked for and what actions were taken:\n\n{context_for_summary}"
    );

    let summary_request = ChatCompletionRequest {
        model: String::new(),
        messages: vec![ChatMessage {
            role: MessageRole::User,
            content: Some(MessageContent::Text(summary_prompt)),
            name: None,
            tool_calls: None,
            tool_call_id: None,
            reasoning_content: None,
        }],
        temperature: Some(0.0),
        max_tokens: Some(200),
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
        prompt_config: Some("moodeng-summarize".to_string()),
        prompt_variables: None,
        models: None,
        provider: None,
    };

    let compact_span = tracing::info_span!("agent.auto_compact");
    let result = async { moodeng.call_llm(&summary_request, session_id).await }
        .instrument(compact_span)
        .await;

    match result {
        Ok(r) if !r.content.is_empty() => {
            messages[stub_idx] = ChatMessage {
                role: MessageRole::System,
                content: Some(MessageContent::Text(format!(
                    "[Conversation summary: {}]",
                    r.content.trim()
                ))),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            };
            tracing::info!(
                summary_tokens = crate::gateway::observability::estimate_tokens(&r.content),
                "Auto-compact: replaced stub with LLM summary"
            );
        }
        Ok(_) => {
            tracing::warn!("Auto-compact: LLM returned empty summary, keeping stub");
        }
        Err(e) => {
            tracing::warn!(error = %e, "Auto-compact: summary LLM call failed, keeping stub");
        }
    }
}

/// Convert DB messages to chat messages, replacing tool result content in
/// older turns with a staleness stub.
pub fn prune_stale_tool_results(
    rows: &[super::agent::Message],
    freshness_turns: usize,
) -> Vec<ChatMessage> {
    let turn_starts: Vec<usize> = rows
        .iter()
        .enumerate()
        .filter(|(_, m)| m.role == "user")
        .map(|(i, _)| i)
        .collect();

    let total_turns = turn_starts.len();
    let fresh_cutoff_turn = total_turns.saturating_sub(freshness_turns);
    let fresh_from_idx = turn_starts.get(fresh_cutoff_turn).copied().unwrap_or(0);

    rows.iter()
        .enumerate()
        .map(|(i, msg)| {
            if i >= fresh_from_idx || msg.role != "tool" {
                return db_message_to_chat_message(msg);
            }
            let tool = msg.tool_name.as_deref().unwrap_or("unknown");
            ChatMessage {
                role: MessageRole::Tool,
                content: Some(MessageContent::Text(format!(
                    "[Tool result from {tool} omitted — data may be stale. Re-call the tool if needed.]"
                ))),
                name: None,
                tool_calls: None,
                tool_call_id: msg.tool_call_id.clone(),
                reasoning_content: None,
            }
        })
        .collect()
}

/// Remove tool messages whose corresponding assistant `tool_calls` message was
/// not loaded, and remove trailing assistant messages that have `tool_calls`
/// without any following tool responses. For assistant messages in the middle of
/// the history that have partially-answered tool_calls, inject stub tool
/// responses so the LLM API never receives an invalid sequence.
pub fn drop_orphaned_tool_messages(messages: &mut Vec<ChatMessage>) {
    use std::collections::HashSet;

    let mut known_tool_call_ids = HashSet::new();
    for msg in messages.iter() {
        if msg.role == MessageRole::Assistant {
            if let Some(tcs) = &msg.tool_calls {
                for tc in tcs {
                    known_tool_call_ids.insert(tc.id.clone());
                }
            }
        }
    }

    let before = messages.len();
    messages.retain(|msg| {
        if msg.role != MessageRole::Tool {
            return true;
        }
        match &msg.tool_call_id {
            Some(id) => known_tool_call_ids.contains(id.as_str()),
            None => true,
        }
    });

    let mut answered_ids = HashSet::new();
    for msg in messages.iter() {
        if msg.role == MessageRole::Tool {
            if let Some(id) = &msg.tool_call_id {
                answered_ids.insert(id.clone());
            }
        }
    }

    while let Some(last) = messages.last() {
        if last.role != MessageRole::Assistant {
            break;
        }
        if let Some(tcs) = &last.tool_calls {
            let all_answered = tcs.iter().all(|tc| answered_ids.contains(&tc.id));
            if !all_answered {
                messages.pop();
                continue;
            }
        }
        break;
    }

    let mut i = 0;
    while i < messages.len() {
        if messages[i].role == MessageRole::Assistant {
            if let Some(tcs) = messages[i].tool_calls.clone() {
                let unanswered: Vec<ToolCall> = tcs
                    .iter()
                    .filter(|tc| !answered_ids.contains(&tc.id))
                    .cloned()
                    .collect();
                if !unanswered.is_empty() {
                    let mut insert_at = i + 1;
                    while insert_at < messages.len()
                        && messages[insert_at].role == MessageRole::Tool
                    {
                        insert_at += 1;
                    }
                    let stubs: Vec<ChatMessage> = unanswered
                        .iter()
                        .map(|tc| ChatMessage {
                            role: MessageRole::Tool,
                            content: Some(MessageContent::Text(
                                "[Tool result unavailable — execution was interrupted. \
                                 Re-call the tool if needed.]"
                                    .to_string(),
                            )),
                            name: None,
                            tool_calls: None,
                            tool_call_id: Some(tc.id.clone()),
                            reasoning_content: None,
                        })
                        .collect();
                    let stub_count = stubs.len();
                    messages.splice(insert_at..insert_at, stubs);
                    for tc in &unanswered {
                        answered_ids.insert(tc.id.clone());
                    }
                    i = insert_at + stub_count;
                    continue;
                }
            }
        }
        i += 1;
    }

    let dropped = before.saturating_sub(messages.len());
    if dropped > 0 {
        tracing::info!(
            dropped_orphans = dropped,
            "Dropped orphaned tool/assistant messages from loaded history"
        );
    }
}

/// Convert a persisted DB message row to an in-memory chat message.
pub fn db_message_to_chat_message(msg: &super::agent::Message) -> ChatMessage {
    let role = match msg.role.as_str() {
        "system" => MessageRole::System,
        "user" => MessageRole::User,
        "assistant" => MessageRole::Assistant,
        "tool" => MessageRole::Tool,
        _ => MessageRole::Other,
    };

    let tool_calls: Option<Vec<ToolCall>> = msg
        .tool_calls
        .as_ref()
        .and_then(|v| serde_json::from_value(v.clone()).ok());

    let reasoning_content = msg
        .metadata
        .as_ref()
        .and_then(|m| m.get("reasoning_content"))
        .and_then(|v| v.as_str())
        .map(String::from);

    ChatMessage {
        role,
        content: msg
            .content
            .as_ref()
            .map(|s| MessageContent::Text(s.clone())),
        name: None,
        tool_calls,
        tool_call_id: msg.tool_call_id.clone(),
        reasoning_content,
    }
}

use tracing::Instrument;
