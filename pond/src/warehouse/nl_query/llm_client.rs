//! HTTP client for calling the Flow (LLM Gateway) chat completions API.
//!
//! Thin wrapper around reqwest that sends prompts to Flow and extracts
//! the generated SQL from the response.

use anyhow::Result;
use serde::{Deserialize, Serialize};

use super::prompt_builder::ChatMessage;

/// Temperature for SQL generation. Set to 0.0 for deterministic output,
/// which is preferred for SQL generation to avoid non-deterministic queries.
const SQL_GENERATION_TEMPERATURE: f32 = 0.0;

/// Maximum number of tokens the LLM can generate in a single response.
/// 2048 tokens is sufficient for even complex SQL queries with CTEs.
const SQL_GENERATION_MAX_TOKENS: u32 = 2048;

/// Timeout for a single LLM API call in seconds.
/// 60 seconds allows for complex queries while preventing indefinite hangs.
const LLM_REQUEST_TIMEOUT_SECS: u64 = 60;

/// Client for the Flow LLM Gateway.
pub struct LlmClient<'a> {
    http_client: &'a reqwest::Client,
    flow_url: &'a str,
    api_key: &'a str,
}

/// Request body for the Flow chat completions API.
#[derive(Debug, Serialize)]
struct ChatCompletionRequest {
    model: String,
    messages: Vec<ChatMessage>,
    temperature: f32,
    max_tokens: u32,
}

/// Response from the Flow chat completions API.
#[derive(Debug, Deserialize)]
struct ChatCompletionResponse {
    choices: Vec<ChatChoice>,
    model: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatChoiceMessage,
}

#[derive(Debug, Deserialize)]
struct ChatChoiceMessage {
    content: Option<String>,
}

impl<'a> LlmClient<'a> {
    /// Create a new LLM client.
    pub fn new(http_client: &'a reqwest::Client, flow_url: &'a str, api_key: &'a str) -> Self {
        Self {
            http_client,
            flow_url,
            api_key,
        }
    }

    /// Call the LLM to generate SQL from the given messages.
    ///
    /// Returns (sql, actual_model_used). The actual model may differ from
    /// the requested model if the gateway performed model fallback.
    #[tracing::instrument(
        name = "warehouse.nl_query.generate_sql",
        skip_all,
        err(Display)
    )]
    pub async fn generate_sql(
        &self,
        model: &str,
        messages: Vec<ChatMessage>,
    ) -> Result<(String, String)> {
        let url = format!(
            "{}/api/gateway/v1/chat/completions",
            self.flow_url.trim_end_matches('/')
        );

        let request_body = ChatCompletionRequest {
            model: model.to_string(),
            messages,
            temperature: SQL_GENERATION_TEMPERATURE,
            max_tokens: SQL_GENERATION_MAX_TOKENS,
        };

        let response = self
            .http_client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&request_body)
            .timeout(std::time::Duration::from_secs(LLM_REQUEST_TIMEOUT_SECS))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to call Flow gateway: {}", e))?;

        let status = response.status();
        if !status.is_success() {
            let error_body = response
                .text()
                .await
                .unwrap_or_else(|_| "Unable to read error body".to_string());

            // Log the full error body for debugging but do NOT include it in
            // the user-facing error. Upstream providers sometimes echo auth
            // headers or API keys in error responses.
            tracing::warn!(
                status = %status,
                error_body = %error_body,
                "Flow gateway returned error"
            );

            // Return a sanitized error message to the user
            let user_message = match status.as_u16() {
                401 | 403 => "LLM gateway authentication failed. Check your API key.",
                429 => "LLM rate limit exceeded. Try again later.",
                500..=599 => "LLM gateway is temporarily unavailable. Try again later.",
                _ => "LLM gateway request failed.",
            };
            return Err(anyhow::anyhow!(
                "Flow gateway returned HTTP {}: {}",
                status,
                user_message
            ));
        }

        let completion: ChatCompletionResponse = response
            .json()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to parse Flow response: {}", e))?;

        // Track the actual model used (may differ from requested model due to fallback)
        let actual_model = completion
            .model
            .unwrap_or_else(|| model.to_string());

        let content = completion
            .choices
            .first()
            .and_then(|c| c.message.content.as_ref())
            .ok_or_else(|| anyhow::anyhow!("No content in LLM response"))?;

        // Clean the response: strip markdown code fences if the LLM wrapped the SQL
        let sql = clean_sql_response(content);

        if sql.is_empty() {
            return Err(anyhow::anyhow!("LLM returned empty SQL"));
        }

        Ok((sql, actual_model))
    }
}

/// Clean the LLM response to extract pure SQL.
///
/// Handles common formatting issues:
/// - Strips markdown code fences (```sql ... ```)
/// - Trims whitespace
/// - Removes trailing semicolons
fn clean_sql_response(content: &str) -> String {
    let mut sql = content.trim().to_string();

    // Strip markdown code fences
    if sql.starts_with("```") {
        // Remove opening fence (with optional language tag)
        if let Some(first_newline) = sql.find('\n') {
            sql = sql[first_newline + 1..].to_string();
        }
        // Remove closing fence
        if sql.ends_with("```") {
            sql = sql[..sql.len() - 3].to_string();
        }
    }

    // Trim whitespace and trailing semicolons
    sql = sql.trim().to_string();
    while sql.ends_with(';') {
        sql.pop();
    }
    sql = sql.trim().to_string();

    sql
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clean_sql_response_plain() {
        assert_eq!(
            clean_sql_response("SELECT count(*) FROM orders"),
            "SELECT count(*) FROM orders"
        );
    }

    #[test]
    fn test_clean_sql_response_with_fences() {
        assert_eq!(
            clean_sql_response("```sql\nSELECT count(*) FROM orders\n```"),
            "SELECT count(*) FROM orders"
        );
    }

    #[test]
    fn test_clean_sql_response_with_semicolon() {
        assert_eq!(
            clean_sql_response("SELECT count(*) FROM orders;"),
            "SELECT count(*) FROM orders"
        );
    }

    #[test]
    fn test_clean_sql_response_with_whitespace() {
        assert_eq!(
            clean_sql_response("  \nSELECT 1\n  "),
            "SELECT 1"
        );
    }

    #[test]
    fn test_clean_sql_response_removes_sql_label() {
        // Some LLMs prefix with just ```sql without a closing fence
        assert_eq!(
            clean_sql_response("```sql\nSELECT 1```"),
            "SELECT 1"
        );
    }

    #[test]
    fn test_clean_sql_response_multiline() {
        let input = "```sql\nSELECT\n  id,\n  name\nFROM orders\nWHERE id > 0\n```";
        let result = clean_sql_response(input);
        assert!(result.contains("SELECT"));
        assert!(result.contains("FROM orders"));
        assert!(result.contains("WHERE id > 0"));
    }

    #[test]
    fn test_constants_values() {
        // Ensure module-level constants have the expected values
        assert_eq!(SQL_GENERATION_TEMPERATURE, 0.0);
        assert_eq!(SQL_GENERATION_MAX_TOKENS, 2048);
        assert_eq!(LLM_REQUEST_TIMEOUT_SECS, 60);
    }
}
