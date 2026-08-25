//! MCP-over-HTTP handler with per-request authentication.
//!
//! Implements the MCP Streamable HTTP transport as a stateless JSON-RPC
//! handler. Each request carries auth context via trusted proxy headers
//! (`X-Project-Id`) or a Bearer API key for direct connections.

use std::sync::Arc;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use opentelemetry::KeyValue;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::action::{ActionContext, Caller};
use crate::client::InternalClient;
use crate::metrics::McpMetrics;
use crate::registry::ActionRegistry;

/// Shared state for the MCP HTTP handler.
pub struct McpHttpState {
    pub registry: Arc<ActionRegistry>,
    pub http_client: reqwest::Client,
    pub website_url: String,
    pub flow_url: String,
    pub watch_url: String,
    pub meter_service: Option<reiver_core::billing::MeterService>,
    pub db: Option<reiver_core::db::DbPool>,
}

#[derive(Deserialize)]
pub struct JsonRpcRequest {
    #[allow(dead_code)]
    jsonrpc: String,
    id: Option<serde_json::Value>,
    method: String,
    #[serde(default)]
    params: Option<serde_json::Value>,
}

#[derive(Serialize)]
struct JsonRpcResponse {
    jsonrpc: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

#[derive(Serialize)]
struct JsonRpcError {
    code: i32,
    message: String,
}

impl JsonRpcResponse {
    fn success(id: Option<serde_json::Value>, result: serde_json::Value) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: Some(result),
            error: None,
        }
    }
    fn error(id: Option<serde_json::Value>, code: i32, message: String) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(JsonRpcError { code, message }),
        }
    }
}

/// Resolve the project identity from the incoming request.
///
/// 1. If `X-Project-Id` is present, trust it (request came through the website proxy).
/// 2. Otherwise, if `Authorization: Bearer <key>` is present, validate the key
///    against the website to resolve the project.
/// 3. If neither is present, reject.
#[tracing::instrument(name = "mcp.auth.resolve", skip_all)]
async fn resolve_auth(
    state: &McpHttpState,
    headers: &HeaderMap,
) -> Result<
    (
        Uuid,
        String,
        Vec<String>,
        String,
        String,
        Option<Uuid>,
        Option<Uuid>,
    ),
    (StatusCode, String),
> {
    // Trusted proxy path
    if let Some(pid) = headers
        .get("X-Project-Id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| Uuid::parse_str(s).ok())
    {
        let api_key = headers
            .get("Authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "))
            .unwrap_or("")
            .to_string();

        let scopes: Vec<String> = headers
            .get("X-Key-Scopes")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();

        let org_id: Option<Uuid> = headers
            .get("X-Organization-Id")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| Uuid::parse_str(s).ok());

        return Ok((
            pid,
            api_key,
            scopes,
            String::new(),
            String::new(),
            None,
            org_id,
        ));
    }

    // Direct connection — validate API key against website
    let api_key = headers
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .ok_or_else(|| {
            McpMetrics::global().auth_failure.add(1, &[]);
            (
                StatusCode::UNAUTHORIZED,
                "Missing X-Project-Id or Authorization header".into(),
            )
        })?;

    #[derive(Deserialize)]
    struct ValidateResp {
        project_id: Uuid,
        organization_id: Uuid,
        #[serde(default)]
        scopes: Vec<String>,
        #[serde(default)]
        key_type: String,
        #[serde(default)]
        key_prefix: String,
        #[serde(default)]
        label: String,
        #[serde(default)]
        created_by: Option<Uuid>,
    }

    let resp = state
        .http_client
        .get(format!("{}/api/auth/validate-key", state.website_url))
        .bearer_auth(api_key)
        .send()
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                format!("Auth service unreachable: {e}"),
            )
        })?;

    if !resp.status().is_success() {
        McpMetrics::global().auth_failure.add(1, &[]);
        return Err((StatusCode::UNAUTHORIZED, "Invalid API key".into()));
    }

    let info: ValidateResp = resp.json().await.map_err(|e| {
        (
            StatusCode::BAD_GATEWAY,
            format!("Auth response parse error: {e}"),
        )
    })?;

    if info.key_type != "agent" {
        McpMetrics::global().auth_failure.add(1, &[]);
        return Err((StatusCode::FORBIDDEN, "MCP requires an agent token. SDK keys are not accepted. Create an agent token in project settings.".into()));
    }

    Ok((
        info.project_id,
        api_key.to_string(),
        info.scopes,
        info.key_prefix,
        info.label,
        info.created_by,
        Some(info.organization_id),
    ))
}

/// Main MCP HTTP handler — dispatches JSON-RPC methods.
#[tracing::instrument(
    name = "mcp.http.request",
    skip_all,
    fields(
        rpc.method = %request.method,
        project_id = tracing::field::Empty,
        key_prefix = tracing::field::Empty,
        key_label = tracing::field::Empty,
    )
)]
pub async fn handle_mcp(
    State(state): State<Arc<McpHttpState>>,
    headers: HeaderMap,
    Json(request): Json<JsonRpcRequest>,
) -> Response {
    let started = std::time::Instant::now();
    let method = request.method.clone();
    let metrics = McpMetrics::global();

    metrics
        .request_count
        .add(1, &[KeyValue::new("method", method.clone())]);

    // Notifications (no id) don't need a response
    if request.id.is_none() || request.id.as_ref() == Some(&serde_json::Value::Null) {
        return StatusCode::ACCEPTED.into_response();
    }

    let id = request.id.clone();

    // Only protocol negotiation is public. Documentation resources are public
    // elsewhere on the docs site, but the remote MCP resource surface must
    // authenticate so a successful read proves the configured agent token.
    let response = match request.method.as_str() {
        method if !method_requires_auth(method) => match method {
            "initialize" => Json(handle_initialize(id, &state)).into_response(),
            "ping" => Json(JsonRpcResponse::success(id, serde_json::json!({}))).into_response(),
            _ => unreachable!("public method allowlist and dispatcher must agree"),
        },
        _ => {
            // Resources and tools require an authenticated agent token.
            let (project_id, api_key, scopes, key_prefix, key_label, created_by, org_id) =
                match resolve_auth(&state, &headers).await {
                    Ok(auth) => auth,
                    Err((status, msg)) => {
                        record_duration(metrics, &method, started);
                        return (status, Json(JsonRpcResponse::error(id, -32000, msg)))
                            .into_response();
                    }
                };

            let current_span = tracing::Span::current();
            current_span.record("project_id", tracing::field::display(project_id));
            current_span.record("key_prefix", key_prefix.as_str());
            current_span.record("key_label", key_label.as_str());

            let mut http_client = InternalClient::new_for_user(
                state.website_url.clone(),
                state.flow_url.clone(),
                state.watch_url.clone(),
                project_id,
                state.http_client.clone(),
                api_key,
            )
            .with_creator("agent", &key_label, &key_prefix)
            .with_origin("agent_token", &key_label, "MCP tool call");
            if let Some(uid) = created_by {
                http_client = http_client.with_user_id(uid);
            }

            let context = ActionContext {
                project_id,
                caller: Caller::ApiKey {
                    key_id: Uuid::nil(),
                },
                scopes,
                http: http_client,
                db: state.db.clone(),
                clickhouse: None,
                encryptor: None,
                asset_storage: None,
                kb_embedder: None,
                meter_service: state.meter_service.clone(),
                organization_id: org_id,
                entitlements: std::sync::Arc::new(reiver_core::entitlements::UnlimitedEntitlements),
                key_prefix,
                key_label,
            };

            let rpc_response = match request.method.as_str() {
                "resources/list" => handle_list_resources(id.clone()),
                "resources/read" => handle_read_resource(id.clone(), request.params),
                "tools/list" => handle_list_tools(id.clone(), &state, &context.scopes),
                "tools/call" => {
                    handle_call_tool(id.clone(), &state, &context, request.params).await
                }
                _ => JsonRpcResponse::error(
                    id,
                    -32601,
                    format!("Method not found: {}", request.method),
                ),
            };

            Json(rpc_response).into_response()
        }
    };

    record_duration(metrics, &method, started);
    response
}

fn method_requires_auth(method: &str) -> bool {
    !matches!(method, "initialize" | "ping")
}

fn record_duration(metrics: &McpMetrics, method: &str, started: std::time::Instant) {
    metrics.request_duration_ms.record(
        started.elapsed().as_secs_f64() * 1000.0,
        &[KeyValue::new("method", method.to_string())],
    );
}

#[tracing::instrument(name = "mcp.initialize", skip_all)]
fn handle_initialize(id: Option<serde_json::Value>, state: &McpHttpState) -> JsonRpcResponse {
    let tools_count = state.registry.tools_list().len();
    let result = serde_json::json!({
        "protocolVersion": "2025-03-26",
        "capabilities": {
            "tools": { "listChanged": false },
            "resources": {}
        },
        "serverInfo": {
            "name": "reiver-mcp",
            "version": env!("CARGO_PKG_VERSION")
        },
        "instructions": format!(
            "{} This server exposes {} scoped tools.",
            crate::docs::SERVER_INSTRUCTIONS,
            tools_count,
        )
    });
    JsonRpcResponse::success(id, result)
}

#[tracing::instrument(name = "mcp.tools.list", skip_all)]
fn handle_list_tools(
    id: Option<serde_json::Value>,
    state: &McpHttpState,
    scopes: &[String],
) -> JsonRpcResponse {
    let tools = state.registry.tools_list_filtered(scopes);
    let tools_json = serde_json::to_value(&tools).unwrap_or(serde_json::json!([]));
    JsonRpcResponse::success(id, serde_json::json!({ "tools": tools_json }))
}

#[tracing::instrument(
    name = "mcp.tool.call",
    skip_all,
    fields(
        gen_ai.tool.name = tracing::field::Empty,
        gen_ai.operation.name = "execute_tool",
        gen_ai.agent.name = "reiver-mcp",
        project_id = tracing::field::Empty,
        key_prefix = tracing::field::Empty,
        key_label = tracing::field::Empty,
    )
)]
async fn handle_call_tool(
    id: Option<serde_json::Value>,
    state: &McpHttpState,
    context: &ActionContext,
    params: Option<serde_json::Value>,
) -> JsonRpcResponse {
    let metrics = McpMetrics::global();

    let params = match params {
        Some(p) => p,
        None => return JsonRpcResponse::error(id, -32602, "Missing params".into()),
    };

    let name = match params.get("name").and_then(|n| n.as_str()) {
        Some(n) => n.to_string(),
        None => return JsonRpcResponse::error(id, -32602, "Missing params.name".into()),
    };

    let span = tracing::Span::current();
    span.record("gen_ai.tool.name", &name.as_str());
    span.record("project_id", tracing::field::display(context.project_id));
    span.record("key_prefix", context.key_prefix.as_str());
    span.record("key_label", context.key_label.as_str());
    metrics
        .tool_call_count
        .add(1, &[KeyValue::new("gen_ai.tool.name", name.clone())]);

    let tool_start = std::time::Instant::now();

    let arguments = params
        .get("arguments")
        .cloned()
        .unwrap_or(serde_json::json!({}));

    let result = match state.registry.call_tool(&name, arguments, context).await {
        Ok(result) => {
            let result_json = serde_json::to_value(&result).unwrap_or(serde_json::json!({}));
            JsonRpcResponse::success(id, result_json)
        }
        Err(e) => JsonRpcResponse::error(id, -32000, e.message.to_string()),
    };

    let duration_ms = tool_start.elapsed().as_millis() as u64;
    metrics.tool_call_duration_ms.record(
        duration_ms as f64,
        &[KeyValue::new("gen_ai.tool.name", name.clone())],
    );

    result
}

fn handle_list_resources(id: Option<serde_json::Value>) -> JsonRpcResponse {
    let resources: Vec<serde_json::Value> = crate::docs::ALL_DOCS
        .iter()
        .map(|doc| {
            serde_json::json!({
                "uri": doc.uri,
                "name": doc.name,
                "description": doc.description,
                "mimeType": "text/markdown"
            })
        })
        .collect();
    JsonRpcResponse::success(id, serde_json::json!({ "resources": resources }))
}

fn handle_read_resource(
    id: Option<serde_json::Value>,
    params: Option<serde_json::Value>,
) -> JsonRpcResponse {
    let uri = params
        .as_ref()
        .and_then(|p| p.get("uri"))
        .and_then(|u| u.as_str());

    let uri = match uri {
        Some(u) => u,
        None => return JsonRpcResponse::error(id, -32602, "Missing params.uri".into()),
    };

    match crate::docs::find_doc(uri) {
        Some(doc) => {
            let contents = serde_json::json!({
                "contents": [{
                    "uri": doc.uri,
                    "mimeType": "text/markdown",
                    "text": doc.content
                }]
            });
            JsonRpcResponse::success(id, contents)
        }
        None => JsonRpcResponse::error(id, -32002, format!("Resource not found: {uri}")),
    }
}

#[cfg(test)]
mod tests {
    use super::method_requires_auth;

    #[test]
    fn remote_resources_require_an_agent_token() {
        assert!(!method_requires_auth("initialize"));
        assert!(!method_requires_auth("ping"));
        assert!(method_requires_auth("resources/list"));
        assert!(method_requires_auth("resources/read"));
        assert!(method_requires_auth("tools/list"));
        assert!(method_requires_auth("tools/call"));
    }
}
