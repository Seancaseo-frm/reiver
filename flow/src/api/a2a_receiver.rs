//! A2A protocol receiver endpoint.
//!
//! Accepts JSON-RPC 2.0 `SendMessage` calls forwarded by Herd's message
//! worker and runs MooDeng's headless tool loop to produce a response.
//! Authentication uses HMAC-SHA256 signature verification with the org's
//! webhook secret.

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
    Json, Router,
};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::sync::Arc;
use uuid::Uuid;

use crate::api::agent_executor::{run_tool_loop, LoopOutcome};
use crate::app_state::FlowState;

const MAX_A2A_TURNS: usize = 25;

pub fn create_a2a_receiver_router() -> Router<Arc<FlowState>> {
    Router::new().route("/a2a", post(handle_a2a))
}

// ── JSON-RPC 2.0 types (minimal subset for SendMessage) ──────────────

#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    jsonrpc: String,
    id: serde_json::Value,
    method: String,
    #[serde(default)]
    params: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct JsonRpcResponse {
    jsonrpc: String,
    id: serde_json::Value,
    result: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct JsonRpcErrorResponse {
    jsonrpc: String,
    id: serde_json::Value,
    error: JsonRpcError,
}

#[derive(Debug, Serialize)]
struct JsonRpcError {
    code: i32,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<serde_json::Value>,
}

// ── A2A message types (minimal subset) ───────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SendMessageParams {
    message: A2aMessage,
    #[serde(default)]
    metadata: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct A2aMessage {
    #[serde(default)]
    parts: Vec<A2aPart>,
    #[serde(default)]
    context_id: Option<String>,
    #[serde(default)]
    metadata: Option<serde_json::Value>,
    #[allow(dead_code)]
    #[serde(default)]
    role: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct A2aPart {
    #[serde(default)]
    text: Option<String>,
}

// ── Authentication ───────────────────────────────────────────────────

/// Verify the HMAC-SHA256 signature sent by Herd in the X-Herd-Signature header.
fn verify_signature(
    webhook_secret: &str,
    body: &[u8],
    headers: &HeaderMap,
) -> Result<(), JsonRpcErrorResponse> {
    let signature = headers
        .get("x-herd-signature")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            jsonrpc_error(
                serde_json::Value::Null,
                -32600,
                "Missing X-Herd-Signature header",
            )
        })?;

    let mut mac = Hmac::<Sha256>::new_from_slice(webhook_secret.as_bytes())
        .map_err(|_| jsonrpc_error(serde_json::Value::Null, -32600, "Invalid webhook secret"))?;
    mac.update(body);
    let expected = hex::encode(mac.finalize().into_bytes());

    if !constant_time_eq(signature.as_bytes(), expected.as_bytes()) {
        return Err(jsonrpc_error(
            serde_json::Value::Null,
            -32600,
            "Invalid signature",
        ));
    }

    Ok(())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

// ── Handler ──────────────────────────────────────────────────────────

async fn handle_a2a(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> (StatusCode, Json<serde_json::Value>) {
    let request: JsonRpcRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("A2A receiver: JSON-RPC parse error: {e}");
            return (
                StatusCode::OK,
                Json(
                    serde_json::to_value(jsonrpc_error(
                        serde_json::Value::Null,
                        -32700,
                        "Parse error",
                    ))
                    .unwrap(),
                ),
            );
        }
    };

    if request.jsonrpc != "2.0" {
        return error_response(&request.id, -32600, "jsonrpc must be \"2.0\"");
    }

    if request.method != "SendMessage" && request.method != "message/send" {
        return error_response(
            &request.id,
            -32601,
            &format!(
                "Method not supported: {}. Only SendMessage is handled.",
                request.method
            ),
        );
    }

    let project_id = match state.moodeng_project_id {
        Some(pid) => pid,
        None => return error_response(&request.id, -32600, "A2A receiver not configured"),
    };

    // Look up the org's webhook secret and verify Herd's signature
    let webhook_secret: Option<String> = sqlx::query_scalar(
        "SELECT o.webhook_secret FROM organizations o
         JOIN projects p ON p.organization_id = o.id
         WHERE p.id = $1",
    )
    .bind(project_id)
    .fetch_optional(state.db.as_ref())
    .await
    .ok()
    .flatten()
    .flatten();

    match webhook_secret {
        Some(ref secret) => {
            if let Err(err) = verify_signature(secret, &body, &headers) {
                return (StatusCode::OK, Json(serde_json::to_value(err).unwrap()));
            }
        }
        None => {
            return error_response(
                &request.id,
                -32600,
                "Webhook secret not configured for this organization",
            );
        }
    }

    let params: SendMessageParams = match serde_json::from_value(request.params.clone()) {
        Ok(p) => p,
        Err(e) => return error_response(&request.id, -32602, &format!("Invalid params: {e}")),
    };

    let user_text = params
        .message
        .parts
        .iter()
        .filter_map(|p| p.text.as_deref())
        .collect::<Vec<_>>()
        .join("\n");

    if user_text.is_empty() {
        return error_response(&request.id, -32602, "Message contains no text parts");
    }

    let task_id = Uuid::now_v7();

    tracing::info!(
        %task_id,
        %project_id,
        text_len = user_text.len(),
        "A2A receiver: processing SendMessage"
    );

    let settings = crate::gateway::routes::get_introspection_settings(&state, project_id).await;
    if !settings.agent_enabled {
        return error_response(&request.id, -32600, "AI Agent is disabled for this project");
    }

    let scopes = settings.agent_scopes.clone();
    let action_ctx = state.build_action_context(
        project_id,
        reiver_mcp::action::Caller::System,
        scopes,
        ("a2a", &task_id.to_string(), "inbound"),
    );

    let prompt_config = state
        .moodeng_project_id
        .map(|_| "moodeng-assistant".to_string());

    let mut prompt_variables = std::collections::HashMap::new();
    if !settings.agent_soul.is_empty() {
        if let Ok(soul_json) = serde_json::to_value(&settings.agent_soul) {
            prompt_variables.insert("soul".into(), soul_json);
        }
    }

    let result = run_tool_loop(
        &state,
        project_id,
        action_ctx,
        prompt_config,
        prompt_variables,
        user_text,
        MAX_A2A_TURNS,
        None,
        Some(task_id.to_string()),
    )
    .await;

    let (status_state, assistant_text) = match &result {
        Ok(r) => match &r.outcome {
            LoopOutcome::Completed { assistant_text } => ("completed", assistant_text.clone()),
            LoopOutcome::MaxTurns { assistant_text, .. } => ("completed", assistant_text.clone()),
            LoopOutcome::ContextTooLong { assistant_text, .. } => {
                ("completed", assistant_text.clone())
            }
            LoopOutcome::RateLimited { .. } => ("failed", "Rate limited".to_string()),
            LoopOutcome::ModelError { error, .. } => ("failed", format!("Model error: {error}")),
            LoopOutcome::Aborted { reason } => ("failed", format!("Aborted: {reason}")),
        },
        Err(e) => {
            tracing::error!(%task_id, error = %e, "A2A receiver: tool loop failed");
            ("failed", format!("Internal error: {e}"))
        }
    };

    tracing::info!(%task_id, status = status_state, "A2A receiver: completed");

    let task_response = serde_json::json!({
        "id": task_id.to_string(),
        "contextId": params.message.context_id,
        "status": {
            "state": status_state,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        },
        "artifacts": [{
            "artifactId": Uuid::now_v7().to_string(),
            "parts": [{ "text": assistant_text }],
        }],
    });

    let response = JsonRpcResponse {
        jsonrpc: "2.0".into(),
        id: request.id,
        result: task_response,
    };

    (
        StatusCode::OK,
        Json(serde_json::to_value(response).unwrap()),
    )
}

// ── Helpers ──────────────────────────────────────────────────────────

fn jsonrpc_error(id: serde_json::Value, code: i32, message: &str) -> JsonRpcErrorResponse {
    JsonRpcErrorResponse {
        jsonrpc: "2.0".into(),
        id,
        error: JsonRpcError {
            code,
            message: message.into(),
            data: None,
        },
    }
}

fn error_response(
    id: &serde_json::Value,
    code: i32,
    message: &str,
) -> (StatusCode, Json<serde_json::Value>) {
    let err = jsonrpc_error(id.clone(), code, message);
    (StatusCode::OK, Json(serde_json::to_value(err).unwrap()))
}
