//! Session replay engine — shared replay logic used by the sessions API
//! (SSE streaming replay) and the prompt compiler (batch evaluation).
//!
//! `SessionReplayer` owns a gateway HTTP client and knows how to:
//! 1. Load saved request messages from Postgres.
//! 2. Prepare messages for replay (deserialize, swap system prompt).
//! 3. Replay a single request through the gateway.
//!
//! Both callers compose these primitives without duplicating the core logic.

use reiver_core::db::DbPool;
use uuid::Uuid;

use crate::gateway::types::{ChatCompletionRequest, ChatMessage, MessageContent, MessageRole};

// ── Stored row ──────────────────────────────────────────────────────────

/// A single saved request from `session_request_content`.
#[derive(Debug, sqlx::FromRow)]
pub struct SavedRequest {
    pub request_messages: String,
    pub response_content: String,
    pub gen_ai_request_model: String,
}

// ── Replay result ───────────────────────────────────────────────────────

/// Outcome of replaying one request through the gateway.
#[derive(Debug)]
pub struct ReplayedRequest {
    pub content: String,
    pub model: String,
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub latency_ms: u64,
    /// The last user message text (useful for judge evaluation).
    pub last_user_query: String,
}

// ── Prepared messages ───────────────────────────────────────────────────

/// Messages ready to be sent to the gateway, plus extracted metadata.
#[derive(Debug)]
pub struct PreparedMessages {
    pub messages: Vec<ChatMessage>,
    pub last_user_query: String,
    pub original_model: String,
}

// ── SessionReplayer ─────────────────────────────────────────────────────

/// Encapsulates session replay logic: loading, message preparation, and
/// gateway execution.  Stateless beyond the HTTP client / DB pool it borrows.
pub struct SessionReplayer<'a> {
    pub db: &'a DbPool,
    pub http_client: &'a reqwest::Client,
    pub flow_url: &'a str,
}

impl<'a> SessionReplayer<'a> {
    pub fn new(db: &'a DbPool, http_client: &'a reqwest::Client, flow_url: &'a str) -> Self {
        Self {
            db,
            http_client,
            flow_url,
        }
    }

    /// Load saved request rows for a session from Postgres.
    pub async fn load_requests(
        &self,
        project_id: Uuid,
        session_id: &str,
        limit: Option<i64>,
    ) -> anyhow::Result<Vec<SavedRequest>> {
        let rows: Vec<SavedRequest> = if let Some(n) = limit {
            sqlx::query_as(
                "SELECT request_messages, response_content, gen_ai_request_model \
                 FROM session_request_content \
                 WHERE project_id = $1 AND session_id = $2 \
                 ORDER BY timestamp ASC \
                 LIMIT $3",
            )
            .bind(project_id)
            .bind(session_id)
            .bind(n)
            .fetch_all(self.db)
            .await?
        } else {
            sqlx::query_as(
                "SELECT request_messages, response_content, gen_ai_request_model \
                 FROM session_request_content \
                 WHERE project_id = $1 AND session_id = $2 \
                 ORDER BY timestamp ASC",
            )
            .bind(project_id)
            .bind(session_id)
            .fetch_all(self.db)
            .await?
        };
        Ok(rows)
    }

    /// Deserialize stored messages and optionally replace the system prompt.
    ///
    /// Returns [`PreparedMessages`] containing the chat messages ready for
    /// a gateway call, the last user query text, and the original model.
    pub fn prepare_messages(
        saved: &SavedRequest,
        system_prompt_override: Option<&str>,
    ) -> anyhow::Result<PreparedMessages> {
        let stored: Vec<ChatMessage> = serde_json::from_str(&saved.request_messages)
            .map_err(|e| anyhow::anyhow!("failed to parse request_messages: {e}"))?;

        let mut messages = Vec::with_capacity(stored.len());
        let mut last_user_query = String::new();
        let mut system_replaced = false;

        for msg in stored {
            if msg.role == MessageRole::System {
                if let Some(override_text) = system_prompt_override {
                    if !system_replaced {
                        messages.push(ChatMessage {
                            role: MessageRole::System,
                            content: Some(MessageContent::Text(override_text.to_string())),
                            name: None,
                            tool_calls: None,
                            tool_call_id: None,
                            reasoning_content: None,
                        });
                        system_replaced = true;
                    }
                    continue;
                }
            }
            if msg.role == MessageRole::User {
                if let Some(MessageContent::Text(ref t)) = msg.content {
                    last_user_query = t.clone();
                }
            }
            messages.push(msg);
        }

        // If we were asked to override but the original had no system message,
        // prepend one.
        if system_prompt_override.is_some() && !system_replaced {
            messages.insert(
                0,
                ChatMessage {
                    role: MessageRole::System,
                    content: Some(MessageContent::Text(
                        system_prompt_override.unwrap().to_string(),
                    )),
                    name: None,
                    tool_calls: None,
                    tool_call_id: None,
                    reasoning_content: None,
                },
            );
        }

        Ok(PreparedMessages {
            messages,
            last_user_query,
            original_model: saved.gen_ai_request_model.clone(),
        })
    }

    /// Replay prepared messages through the gateway and return the result.
    pub async fn replay_request(
        &self,
        project_id: Uuid,
        prepared: PreparedMessages,
        model: &str,
        temperature: Option<f32>,
        max_tokens: Option<u32>,
        top_p: Option<f32>,
        billing_project_id: Option<Uuid>,
    ) -> anyhow::Result<ReplayedRequest> {
        Self::execute_gateway_request(
            self.http_client,
            self.flow_url,
            project_id,
            prepared,
            model,
            temperature,
            max_tokens,
            top_p,
            billing_project_id,
        )
        .await
    }

    /// Static version of replay that doesn't require `&self`.
    /// Useful in contexts where the replayer can't be borrowed (e.g. `unfold` closures).
    pub async fn execute_gateway_request(
        http_client: &reqwest::Client,
        flow_url: &str,
        project_id: Uuid,
        prepared: PreparedMessages,
        model: &str,
        temperature: Option<f32>,
        max_tokens: Option<u32>,
        top_p: Option<f32>,
        billing_project_id: Option<Uuid>,
    ) -> anyhow::Result<ReplayedRequest> {
        let request = ChatCompletionRequest {
            model: model.to_string(),
            messages: prepared.messages,
            temperature,
            stream: Some(false),
            max_tokens,
            top_p,
            ..Default::default()
        };

        let start = std::time::Instant::now();
        let gw_result = crate::api::gateway_client::call_gateway(
            http_client,
            flow_url,
            project_id,
            &request,
            None,
            billing_project_id,
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

        Ok(ReplayedRequest {
            content: gw_result.content,
            model: gw_result.model,
            prompt_tokens: gw_result.usage.prompt_tokens,
            completion_tokens: gw_result.usage.completion_tokens,
            latency_ms: start.elapsed().as_millis() as u64,
            last_user_query: prepared.last_user_query,
        })
    }
}

// ── Unit tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn system(text: &str) -> ChatMessage {
        ChatMessage {
            role: MessageRole::System,
            content: Some(MessageContent::Text(text.into())),
            name: None,
            tool_calls: None,
            tool_call_id: None,
            reasoning_content: None,
        }
    }

    fn user(text: &str) -> ChatMessage {
        ChatMessage {
            role: MessageRole::User,
            content: Some(MessageContent::Text(text.into())),
            name: None,
            tool_calls: None,
            tool_call_id: None,
            reasoning_content: None,
        }
    }

    fn assistant(text: &str) -> ChatMessage {
        ChatMessage {
            role: MessageRole::Assistant,
            content: Some(MessageContent::Text(text.into())),
            name: None,
            tool_calls: None,
            tool_call_id: None,
            reasoning_content: None,
        }
    }

    fn tool_call_assistant() -> ChatMessage {
        use crate::gateway::types::{FunctionCall, ToolCall, ToolType};
        ChatMessage {
            role: MessageRole::Assistant,
            content: None,
            name: None,
            tool_calls: Some(vec![ToolCall {
                index: None,
                id: "call_123".into(),
                tool_type: ToolType::Function,
                function: FunctionCall {
                    name: "get_weather".into(),
                    arguments: r#"{"city":"Paris"}"#.into(),
                },
            }]),
            tool_call_id: None,
            reasoning_content: None,
        }
    }

    fn tool_result() -> ChatMessage {
        ChatMessage {
            role: MessageRole::Tool,
            content: Some(MessageContent::Text(r#"{"temp":22}"#.into())),
            name: None,
            tool_calls: None,
            tool_call_id: Some("call_123".into()),
            reasoning_content: None,
        }
    }

    fn make_saved(messages: &[ChatMessage]) -> SavedRequest {
        SavedRequest {
            request_messages: serde_json::to_string(messages).unwrap(),
            response_content: String::new(),
            gen_ai_request_model: "gpt-4o".into(),
        }
    }

    #[test]
    fn prepare_replaces_system_prompt() {
        let saved = make_saved(&[
            system("original system"),
            user("hello"),
            assistant("hi there"),
        ]);

        let prepared = SessionReplayer::prepare_messages(&saved, Some("new system")).unwrap();

        assert_eq!(prepared.messages.len(), 3);
        assert_eq!(prepared.messages[0].role, MessageRole::System);
        match &prepared.messages[0].content {
            Some(MessageContent::Text(t)) => assert_eq!(t, "new system"),
            _ => panic!("expected text content"),
        }
        assert_eq!(prepared.last_user_query, "hello");
        assert_eq!(prepared.original_model, "gpt-4o");
    }

    #[test]
    fn prepare_keeps_original_when_no_override() {
        let saved = make_saved(&[system("original system"), user("hello")]);

        let prepared = SessionReplayer::prepare_messages(&saved, None).unwrap();

        assert_eq!(prepared.messages.len(), 2);
        match &prepared.messages[0].content {
            Some(MessageContent::Text(t)) => assert_eq!(t, "original system"),
            _ => panic!("expected text content"),
        }
    }

    #[test]
    fn prepare_inserts_system_when_missing() {
        let saved = make_saved(&[user("hello")]);

        let prepared = SessionReplayer::prepare_messages(&saved, Some("injected")).unwrap();

        assert_eq!(prepared.messages.len(), 2);
        assert_eq!(prepared.messages[0].role, MessageRole::System);
        match &prepared.messages[0].content {
            Some(MessageContent::Text(t)) => assert_eq!(t, "injected"),
            _ => panic!("expected text content"),
        }
    }

    #[test]
    fn prepare_preserves_tool_messages() {
        let saved = make_saved(&[
            system("sys"),
            user("what's the weather?"),
            tool_call_assistant(),
            tool_result(),
            assistant("It's 22°C in Paris."),
        ]);

        let prepared = SessionReplayer::prepare_messages(&saved, Some("new sys")).unwrap();

        assert_eq!(prepared.messages.len(), 5);
        assert_eq!(prepared.messages[0].role, MessageRole::System);
        assert_eq!(prepared.messages[2].role, MessageRole::Assistant);
        assert!(prepared.messages[2].tool_calls.is_some());
        assert_eq!(prepared.messages[3].role, MessageRole::Tool);
        assert_eq!(
            prepared.messages[3].tool_call_id.as_deref(),
            Some("call_123")
        );
        assert_eq!(prepared.last_user_query, "what's the weather?");
    }

    #[test]
    fn prepare_tracks_last_user_query_in_multiturn() {
        let saved = make_saved(&[
            system("sys"),
            user("first question"),
            assistant("first answer"),
            user("second question"),
        ]);

        let prepared = SessionReplayer::prepare_messages(&saved, None).unwrap();
        assert_eq!(prepared.last_user_query, "second question");
    }
}
