//! Root cause suggestions — currently a stub.
//! The log_templates table was removed (it was never populated).
//! This module returns empty results until a templatization pipeline is built.
//!
//! TODO: Consider querying reiver.logs directly to provide root cause
//! suggestions (e.g. group by body/severity_text to find dominant error patterns).

use serde::Serialize;
use uuid::Uuid;

use crate::clickhouse_db::ClickHousePool;

#[derive(Debug, Serialize)]
pub struct RootCauseSuggestion {
    pub pattern: String,
    pub count: u64,
    pub pct: f64,
}

#[derive(Debug, Serialize)]
pub struct RootCauseSuggestionsResponse {
    pub suggestions: Vec<RootCauseSuggestion>,
    pub total_logs: u64,
}

/// Fetch dominant OTLP log templates in [start_ms, end_ms).
pub async fn fetch_root_cause_suggestions(
    _clickhouse: &ClickHousePool,
    _project_id: Uuid,
    _start_ms: i64,
    _end_ms: i64,
) -> Result<RootCauseSuggestionsResponse, crate::error::AppError> {
    Ok(RootCauseSuggestionsResponse {
        suggestions: vec![],
        total_logs: 0,
    })
}
