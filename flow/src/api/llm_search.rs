//! LLM Search API
//!
//! Text search across LLM request and response content stored in ClickHouse.

use axum::{extract::State, routing::post, Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::app_state::FlowState;
use crate::error::{AppError, Result};
use crate::utils::escape_clickhouse_string;

const MAX_SEARCH_LIMIT: u32 = 100;
const PREVIEW_LENGTH: usize = 200;

pub fn create_llm_search_router() -> Router<Arc<FlowState>> {
    Router::new().route("/", post(search_llm_requests))
}

#[derive(Debug, Deserialize)]
pub struct SearchRequest {
    pub project_id: Uuid,
    pub query: String,
    #[serde(default = "default_search_limit")]
    pub limit: u32,
    pub model: Option<String>,
    pub user_id: Option<String>,
    pub session_id: Option<String>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
}

fn default_search_limit() -> u32 {
    10
}

#[derive(Debug, Serialize)]
pub struct SearchResult {
    pub request_id: String,
    pub content_preview: String,
    pub model: String,
    pub user_id: String,
    pub session_id: String,
    pub timestamp: DateTime<Utc>,
    pub status_code: String,
    pub duration_ms: u32,
}

#[derive(Debug, Serialize)]
pub struct SearchResponse {
    pub results: Vec<SearchResult>,
    pub query: String,
    pub total: u64,
}

async fn search_llm_requests(
    State(state): State<Arc<FlowState>>,
    Json(req): Json<SearchRequest>,
) -> Result<Json<SearchResponse>> {
    if req.query.trim().is_empty() {
        return Err(AppError::Validation("Query cannot be empty".to_string()));
    }

    let escaped_query = escape_clickhouse_string(&req.query);
    let limit = req.limit.min(MAX_SEARCH_LIMIT);

    let mut filters = vec![
        format!("project_id = '{}'", req.project_id),
        format!(
            "(positionCaseInsensitive(request_messages, '{q}') > 0 OR positionCaseInsensitive(response_content, '{q}') > 0)",
            q = escaped_query,
        ),
    ];

    if let Some(ref model) = req.model {
        filters.push(format!(
            "gen_ai_request_model = '{}'",
            escape_clickhouse_string(model)
        ));
    }
    if let Some(ref uid) = req.user_id {
        filters.push(format!("user_id = '{}'", escape_clickhouse_string(uid)));
    }
    if let Some(ref sid) = req.session_id {
        filters.push(format!("session_id = '{}'", escape_clickhouse_string(sid)));
    }
    if let Some(start) = req.start_time {
        filters.push(format!(
            "timestamp >= toDateTime64('{}', 9)",
            start.format("%Y-%m-%d %H:%M:%S")
        ));
    } else if req.end_time.is_none() {
        filters.push("timestamp >= now() - INTERVAL 30 DAY".to_string());
    }
    if let Some(end) = req.end_time {
        filters.push(format!(
            "timestamp <= toDateTime64('{}', 9)",
            end.format("%Y-%m-%d %H:%M:%S")
        ));
    }

    let where_clause = filters.join(" AND ");

    let query = format!(
        r#"
        SELECT
            request_id,
            if(
                positionCaseInsensitive(response_content, '{escaped_query}') > 0,
                response_content,
                request_messages
            ) as matched_content,
            gen_ai_request_model as model,
            user_id,
            session_id,
            timestamp,
            status_code,
            duration_ms
        FROM reiver.llm_requests
        WHERE {where_clause}
        ORDER BY timestamp DESC
        LIMIT {limit}
        "#,
    );

    #[derive(Debug, clickhouse::Row, serde::Deserialize)]
    struct ResultRow {
        request_id: String,
        matched_content: String,
        model: String,
        user_id: String,
        session_id: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: DateTime<Utc>,
        status_code: String,
        duration_ms: u32,
    }

    let rows: Vec<ResultRow> = state
        .clickhouse
        .query(&query)
        .fetch_all()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Search query failed: {}", e)))?;

    let total = rows.len() as u64;

    let results: Vec<SearchResult> = rows
        .into_iter()
        .map(|row| {
            let content_preview = truncate_preview(&row.matched_content, PREVIEW_LENGTH);
            SearchResult {
                request_id: row.request_id,
                content_preview,
                model: row.model,
                user_id: row.user_id,
                session_id: row.session_id,
                timestamp: row.timestamp,
                status_code: row.status_code,
                duration_ms: row.duration_ms,
            }
        })
        .collect();

    Ok(Json(SearchResponse {
        results,
        query: req.query,
        total,
    }))
}

fn truncate_preview(content: &str, max_chars: usize) -> String {
    if content.chars().count() <= max_chars {
        content.to_string()
    } else {
        format!(
            "{}...",
            content.chars().take(max_chars - 3).collect::<String>()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_query_rejected() {
        let req = SearchRequest {
            project_id: Uuid::nil(),
            query: "   ".to_string(),
            limit: 10,
            model: None,
            user_id: None,
            session_id: None,
            start_time: None,
            end_time: None,
        };
        assert!(req.query.trim().is_empty());
    }

    #[test]
    fn test_search_limit_capped() {
        let limit: u32 = 500;
        assert_eq!(limit.min(MAX_SEARCH_LIMIT), MAX_SEARCH_LIMIT);
    }

    #[test]
    fn test_preview_truncation_ascii() {
        let content = "a".repeat(300);
        let preview = truncate_preview(&content, PREVIEW_LENGTH);
        assert_eq!(preview.chars().count(), PREVIEW_LENGTH);
        assert!(preview.ends_with("..."));
    }

    #[test]
    fn test_preview_truncation_multibyte() {
        let content = "日本語".repeat(100);
        let preview = truncate_preview(&content, PREVIEW_LENGTH);
        assert_eq!(preview.chars().count(), PREVIEW_LENGTH);
        assert!(preview.ends_with("..."));
    }

    #[test]
    fn test_short_content_no_truncation() {
        let content = "hello world";
        let preview = truncate_preview(content, PREVIEW_LENGTH);
        assert_eq!(preview, "hello world");
    }
}
