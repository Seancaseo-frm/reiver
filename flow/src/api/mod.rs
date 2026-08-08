// LLM Observability APIs
pub mod a2a_receiver;
pub mod agent;
pub mod agent_attachments;
pub mod agent_audit;
pub mod agent_context;
pub mod agent_executor;
pub mod agent_persistence;
pub mod agent_task;
pub mod gateway_client;
pub mod investigate;
pub mod llm_integrations;
pub mod llm_metrics;
pub mod llm_playground;
pub mod llm_proposals;
pub mod llm_rollouts;
pub mod llm_scores;
pub mod llm_search;
pub mod llm_sessions;
pub mod llm_settings;
pub mod prompt_compiler;
pub mod secret_slots;
pub mod session_profiles;
pub mod session_replay;

/// Default limit for list endpoints (scores, metrics, sessions, rollouts).
pub fn default_list_limit() -> u32 {
    50
}

use crate::app_state::FlowState;
use crate::error::{AppError, Result};
use axum::http::HeaderMap;
use axum::Router;
use std::sync::Arc;
use uuid::Uuid;

/// Extract the authenticated user ID from the trusted `X-User-Id` header.
///
/// The website gateway validates the JWT and project access before
/// forwarding the request here with this header. Flow trusts it.
pub fn extract_user_id(headers: &HeaderMap) -> Result<Uuid> {
    headers
        .get("X-User-Id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| AppError::Auth("Missing or invalid X-User-Id header".to_string()))
}

/// Extract the project ID from the trusted `X-Project-Id` header.
///
/// The website gateway validates the API key and resolves the project
/// before forwarding the request here with this header. Flow trusts it.
pub fn extract_project_id(headers: &HeaderMap) -> Result<Uuid> {
    headers
        .get("X-Project-Id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| AppError::Auth("Missing or invalid X-Project-Id header".to_string()))
}

/// Extract the organization ID from the trusted `X-Organization-Id` header.
///
/// The website gateway resolves the org from the project and forwards it
/// so Flow can attribute audit events to the correct organization.
pub fn extract_organization_id(headers: &HeaderMap) -> Option<Uuid> {
    headers
        .get("X-Organization-Id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| Uuid::parse_str(s).ok())
}

/// Create the Flow (LLM Gateway) API router with gateway + LLM observability routes.
///
/// All routes are served behind the website proxy which handles authentication
/// and project access verification before forwarding to Flow.
pub fn create_flow_api_router() -> Router<Arc<FlowState>> {
    Router::new()
        // AI Gateway (OpenAI-compatible)
        .nest("/gateway/v1", crate::gateway::create_gateway_router())
        // LLM Observability API endpoints
        .nest("/llm/sessions", llm_sessions::create_llm_sessions_router())
        .nest("/llm/scores", llm_scores::create_llm_scores_router())
        .nest("/llm/metrics", llm_metrics::create_llm_metrics_router())
        .nest(
            "/llm/prompts",
            llm_rollouts::create_llm_rollouts_router()
                .merge(llm_proposals::create_llm_proposals_router()),
        )
        .nest(
            "/llm/compiler",
            prompt_compiler::create_compiler_page_router(),
        )
        .nest(
            "/llm/playground",
            llm_playground::create_llm_playground_router(),
        )
        .nest("/llm/search", llm_search::create_llm_search_router())
        // LLM provider integrations and gateway settings
        .nest(
            "/llm/integrations",
            llm_integrations::create_llm_integrations_router(),
        )
        .nest("/llm/settings", llm_settings::create_llm_settings_router())
        // Model catalog (no project context required)
        .nest("/llm/models", llm_settings::create_llm_models_router())
        // In-app AI agent
        .nest("/agent", agent::create_agent_router())
        // Secret slots for agent-mediated credential setup
        .nest("/secrets", secret_slots::create_secret_slots_router())
        .nest("/internal", {
            Router::new()
                .merge(investigate::create_investigate_router())
                .merge(agent_task::create_agent_task_router())
                .merge(prompt_compiler::create_prompt_compiler_router())
        })
}
