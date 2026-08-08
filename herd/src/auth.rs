use axum::http::HeaderMap;
use serde::Deserialize;
use uuid::Uuid;

use crate::app_state::HerdState;

pub struct AgentAuth {
    pub project_id: Uuid,
    pub organization_id: Uuid,
    pub key_id: Uuid,
}

#[derive(Deserialize)]
struct ValidateKeyResponse {
    project_id: Uuid,
    organization_id: Uuid,
    key_id: Uuid,
    #[serde(default)]
    key_type: String,
}

/// Authenticate an A2A protocol request via `Authorization: Bearer <agent-token>`.
/// Validates the token against the website's `/api/auth/validate-key` endpoint
/// and requires `key_type == "agent"`.
pub async fn resolve_agent_auth(
    state: &HerdState,
    headers: &HeaderMap,
) -> Result<AgentAuth, String> {
    let api_key = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .filter(|k| !k.is_empty())
        .ok_or_else(|| "Missing or invalid Authorization header".to_string())?;

    let resp = state
        .http_client
        .get(format!("{}/api/auth/validate-key", state.website_url))
        .bearer_auth(api_key)
        .send()
        .await
        .map_err(|e| format!("Auth service unreachable: {e}"))?;

    if !resp.status().is_success() {
        return Err("Invalid API key".to_string());
    }

    let info: ValidateKeyResponse = resp
        .json()
        .await
        .map_err(|e| format!("Auth response parse error: {e}"))?;

    if info.key_type != "agent" {
        return Err("A2A requires an agent token. SDK keys are not accepted.".to_string());
    }

    Ok(AgentAuth {
        project_id: info.project_id,
        organization_id: info.organization_id,
        key_id: info.key_id,
    })
}
