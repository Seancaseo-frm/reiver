use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
    Json, Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::a2a::errors::A2aError;
use crate::a2a::jsonrpc::*;
use crate::a2a::types::*;
use crate::app_state::HerdState;
use crate::auth;
use crate::pipeline;
use crate::routing_cache::CachedPushConfig;

fn serialize_jsonrpc<T: Serialize>(value: &T) -> Json<serde_json::Value> {
    match serde_json::to_value(value) {
        Ok(v) => Json(v),
        Err(e) => {
            tracing::error!("Failed to serialize JSON-RPC response: {}", e);
            Json(serde_json::json!({
                "jsonrpc": "2.0",
                "id": null,
                "error": { "code": -32603, "message": "Internal serialization error" }
            }))
        }
    }
}

pub fn router() -> Router<Arc<HerdState>> {
    Router::new().route("/a2a", post(jsonrpc_dispatch))
}

const A2A_MESSAGES_TOPIC: &str = "reiver.a2a.messages";

async fn authenticate(
    state: &HerdState,
    headers: &HeaderMap,
) -> Result<auth::AgentAuth, JsonRpcErrorResponse> {
    auth::resolve_agent_auth(state, headers)
        .await
        .map_err(|msg| {
            tracing::warn!("A2A auth failed: {}", msg);
            JsonRpcErrorResponse::new(
                serde_json::Value::Null,
                JsonRpcError {
                    code: -32600,
                    message: msg,
                    data: None,
                },
            )
        })
}

/// Kafka envelope for a2a messages (includes routing metadata).
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct A2aMessageEnvelope {
    task_id: String,
    context_id: Option<String>,
    source_agent_id: Uuid,
    target_agent_id: Uuid,
    source_org_id: Uuid,
    target_org_id: Uuid,
    method: String,
    message: Message,
    configuration: Option<SendMessageConfiguration>,
    metadata: Option<serde_json::Value>,
    pipeline_flags: PipelineFlags,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PipelineFlags {
    pii_redacted: bool,
    injection_flagged: bool,
}

async fn jsonrpc_dispatch(
    State(state): State<Arc<HerdState>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> (StatusCode, Json<serde_json::Value>) {
    let request: JsonRpcRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("JSON-RPC parse error: {}", e);
            let err = JsonRpcErrorResponse::parse_error();
            return (StatusCode::OK, serialize_jsonrpc(&err));
        }
    };

    if let Err(e) = request.validate() {
        let err = JsonRpcErrorResponse::new(request.id.clone(), e);
        return (StatusCode::OK, serialize_jsonrpc(&err));
    }

    let auth = match authenticate(&state, &headers).await {
        Ok(a) => a,
        Err(err) => {
            return (StatusCode::OK, serialize_jsonrpc(&err));
        }
    };

    tracing::info!(a2a.method = %request.method, "Dispatching JSON-RPC method");

    let result = match request.method.as_str() {
        methods::SEND_MESSAGE => handle_send_message(&state, auth.key_id, &request).await,
        methods::GET_TASK => {
            handle_get_task(&state, auth.project_id, auth.organization_id, &request).await
        }
        methods::LIST_TASKS => {
            handle_list_tasks(&state, auth.project_id, auth.organization_id, &request).await
        }
        methods::CANCEL_TASK => {
            handle_cancel_task(&state, auth.project_id, auth.organization_id, &request).await
        }
        methods::CREATE_PUSH_NOTIFICATION_CONFIG => {
            handle_create_push_config(&state, auth.key_id, &request).await
        }
        methods::GET_PUSH_NOTIFICATION_CONFIG => {
            handle_get_push_config(&state, auth.project_id, &request).await
        }
        methods::LIST_PUSH_NOTIFICATION_CONFIGS => {
            handle_list_push_configs(&state, auth.project_id, &request).await
        }
        methods::DELETE_PUSH_NOTIFICATION_CONFIG => {
            handle_delete_push_config(&state, auth.project_id, &request).await
        }
        methods::GET_EXTENDED_AGENT_CARD => {
            let err = A2aError::UnsupportedOperation
                .to_jsonrpc_error_response(request.id.clone(), Some("Not implemented"));
            Err(err)
        }
        methods::SEND_STREAMING_MESSAGE | methods::SUBSCRIBE_TO_TASK => {
            let err = A2aError::UnsupportedOperation.to_jsonrpc_error_response(
                request.id.clone(),
                Some("Streaming is not supported. Use push notifications for async updates."),
            );
            Err(err)
        }
        _ => Err(JsonRpcErrorResponse::method_not_found(
            request.id.clone(),
            &request.method,
        )),
    };

    match result {
        Ok(resp) => (StatusCode::OK, serialize_jsonrpc(&resp)),
        Err(err) => (StatusCode::OK, serialize_jsonrpc(&err)),
    }
}

/// Resolve the source agent from the token's `key_id`.
async fn resolve_source_agent(state: &HerdState, key_id: Uuid) -> Result<(Uuid, Uuid, Uuid), String> {
    let row: Option<(Uuid, Uuid, Uuid)> = sqlx::query_as(
        "SELECT id, organization_id, project_id FROM a2a_agents WHERE key_id = $1 AND enabled = true",
    )
    .bind(key_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(|e| format!("DB error resolving agent by key_id: {}", e))?;

    row.ok_or_else(|| "No registered agent found for this token".into())
}

/// Checks whether `source_agent_id` is allowed to send messages to `target_agent_id`.
///
/// Pure lookup — no side effects, no webhooks. Access must be pre-approved
/// via `request_access` before messages can be sent.
///
/// Access tiers (evaluated top-to-bottom, short-circuits on first match):
///   1. Same project → instant allow
///   2. In-memory `AccessCache` hit (source_agent, target_agent) → allow
///   3. DB grant lookup (most recent row per agent pair) → if approved, cache + allow
///   4. No approved grant → reject
async fn resolve_and_check_access(
    state: &HerdState,
    target_agent_id: Uuid,
    source_agent_id: Uuid,
    source_project_id: Uuid,
) -> Result<(Uuid, Uuid), A2aError> {
    let row: Option<(Uuid, Uuid, String)> = sqlx::query_as(
        "SELECT organization_id, project_id, visibility FROM a2a_agents WHERE id = $1 AND enabled = true",
    )
    .bind(target_agent_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(|e| {
        tracing::error!("DB error resolving target agent: {}", e);
        A2aError::InvalidAgentResponse
    })?;

    let (target_org_id, target_project_id, visibility) = row.ok_or(A2aError::TaskNotFound)?;

    // Private agents are only reachable from within the same project
    if visibility == "private" && target_project_id != source_project_id {
        return Err(A2aError::TaskNotFound);
    }

    // Same project: instant allow
    if target_project_id == source_project_id {
        return Ok((target_agent_id, target_org_id));
    }

    // --- Grant-based access (cross-project, same or different org) ---

    // 1. Check in-memory cache (agent-to-agent)
    if state.access_cache.is_approved(source_agent_id, target_agent_id) {
        return Ok((target_agent_id, target_org_id));
    }

    // 2. Check DB for the most recent grant row (agent-to-agent)
    let db_status: Option<String> = sqlx::query_scalar(
        "SELECT status FROM a2a_access_grants
         WHERE granted_agent_id = $1 AND target_agent_id = $2
         ORDER BY requested_at DESC
         LIMIT 1",
    )
    .bind(source_agent_id)
    .bind(target_agent_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(|e| {
        tracing::error!("DB error checking access grant: {}", e);
        A2aError::InvalidAgentResponse
    })?;

    if db_status.as_deref() == Some("approved") {
        state.access_cache.approve(source_agent_id, target_agent_id);
        return Ok((target_agent_id, target_org_id));
    }

    Err(A2aError::AccessDenied)
}

async fn handle_send_message(
    state: &HerdState,
    key_id: Uuid,
    request: &JsonRpcRequest,
) -> Result<JsonRpcResponse, JsonRpcErrorResponse> {
    let params: SendMessageRequest = serde_json::from_value(request.params.clone())
        .map_err(|e| JsonRpcErrorResponse::invalid_params(request.id.clone(), &e.to_string()))?;

    let (source_agent_id, source_org_id, source_project_id) = resolve_source_agent(state, key_id)
        .await
        .map_err(|e| JsonRpcErrorResponse::internal_error(request.id.clone(), &e))?;

    // The message must target an agent. We look for target info in the message metadata.
    let target_agent_id_str = params
        .message
        .metadata
        .as_ref()
        .and_then(|m| m.get("targetAgentId"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            JsonRpcErrorResponse::invalid_params(
                request.id.clone(),
                "message.metadata.targetAgentId is required",
            )
        })?;

    let target_agent_id = Uuid::parse_str(target_agent_id_str).map_err(|_| {
        JsonRpcErrorResponse::invalid_params(request.id.clone(), "Invalid targetAgentId UUID")
    })?;

    // Access check (agent-to-agent)
    let (_target_id, target_org_id) =
        resolve_and_check_access(state, target_agent_id, source_agent_id, source_project_id)
            .await
            .map_err(|e| e.to_jsonrpc_error_response(request.id.clone(), None))?;

    // Run enterprise pipeline on message parts
    let mut message = params.message.clone();
    let mut flags = PipelineFlags::default();
    flags.pii_redacted = pipeline::pii::scrub_message_parts(&mut message.parts);
    if pipeline::injection::detect_injection(&message.parts) {
        flags.injection_flagged = true;
        return Err(JsonRpcErrorResponse::new(
            request.id.clone(),
            JsonRpcError {
                code: -32600,
                message: "Message rejected: potential prompt injection detected".into(),
                data: None,
            },
        ));
    }

    // Create task
    let task_id = Uuid::now_v7().to_string();
    let context_id = message
        .context_id
        .clone()
        .unwrap_or_else(|| Uuid::now_v7().to_string());

    let task = Task {
        id: task_id.clone(),
        context_id: Some(context_id.clone()),
        status: TaskStatus {
            state: TaskState::Submitted,
            message: None,
            timestamp: Some(Utc::now()),
        },
        artifacts: None,
        history: None,
        metadata: params.metadata.clone(),
    };

    // Produce to Redpanda
    let envelope = A2aMessageEnvelope {
        task_id: task_id.clone(),
        context_id: Some(context_id),
        source_agent_id,
        target_agent_id,
        source_org_id,
        target_org_id,
        method: "message/send".into(),
        message,
        configuration: params.configuration,
        metadata: params.metadata,
        pipeline_flags: flags,
        timestamp: Utc::now(),
    };

    let payload = serde_json::to_vec(&envelope).map_err(|e| {
        JsonRpcErrorResponse::internal_error(request.id.clone(), &format!("Serialization: {}", e))
    })?;

    state
        .kafka
        .send_to_topic(A2A_MESSAGES_TOPIC, &target_agent_id.to_string(), &payload)
        .await
        .map_err(|e| {
            tracing::error!("Failed to produce to Redpanda: {}", e);
            JsonRpcErrorResponse::internal_error(request.id.clone(), "Message delivery failed")
        })?;

    let result = serde_json::to_value(&task).map_err(|e| {
        tracing::error!("Failed to serialize task: {}", e);
        JsonRpcErrorResponse::internal_error(request.id.clone(), "Failed to serialize task")
    })?;
    Ok(JsonRpcResponse::success(request.id.clone(), result))
}

async fn handle_get_task(
    state: &HerdState,
    _project_id: Uuid,
    _org_id: Uuid,
    request: &JsonRpcRequest,
) -> Result<JsonRpcResponse, JsonRpcErrorResponse> {
    let params: GetTaskRequest = serde_json::from_value(request.params.clone())
        .map_err(|e| JsonRpcErrorResponse::invalid_params(request.id.clone(), &e.to_string()))?;

    let history_length = params.history_length.unwrap_or(50);

    // Query ClickHouse for task state
    let task_row = state
        .clickhouse
        .query(
            "SELECT task_id, context_id, status, metadata, artifacts, updated_at, created_at
             FROM a2a_tasks FINAL
             WHERE task_id = ?
             LIMIT 1",
        )
        .bind(&params.id)
        .fetch_optional::<ClickHouseTaskRow>()
        .await
        .map_err(|e| {
            tracing::error!("ClickHouse task query error: {}", e);
            JsonRpcErrorResponse::internal_error(request.id.clone(), "Failed to get task")
        })?
        .ok_or_else(|| {
            A2aError::TaskNotFound.to_jsonrpc_error_response(request.id.clone(), None)
        })?;

    // Query message history from ClickHouse
    let messages: Vec<ClickHouseMessageRow> = state
        .clickhouse
        .query(
            "SELECT message_id, task_id, context_id, role, parts, reference_task_ids, metadata, created_at
             FROM a2a_messages
             WHERE task_id = ?
             ORDER BY created_at DESC
             LIMIT ?",
        )
        .bind(&params.id)
        .bind(history_length)
        .fetch_all()
        .await
        .map_err(|e| {
            tracing::error!("ClickHouse message history query error: {}", e);
            JsonRpcErrorResponse::internal_error(request.id.clone(), "Failed to get message history")
        })?;

    let history: Vec<Message> = messages
        .into_iter()
        .rev()
        .map(|m| Message {
            message_id: m.message_id.to_string(),
            context_id: m.context_id.map(|c| c.to_string()),
            task_id: Some(m.task_id.to_string()),
            role: if m.role == "agent" { Role::Agent } else { Role::User },
            parts: serde_json::from_str(&m.parts).unwrap_or_else(|e| {
                tracing::warn!(message_id = %m.message_id, "Corrupt message parts JSON: {}", e);
                vec![]
            }),
            metadata: match serde_json::from_str(&m.metadata) {
                Ok(v) => Some(v),
                Err(e) => {
                    tracing::warn!(message_id = %m.message_id, "Corrupt message metadata JSON: {}", e);
                    None
                }
            },
            extensions: None,
            reference_task_ids: if m.reference_task_ids.is_empty() {
                None
            } else {
                Some(m.reference_task_ids.iter().map(|u| u.to_string()).collect())
            },
        })
        .collect();

    let task = Task {
        id: task_row.task_id.to_string(),
        context_id: task_row.context_id.map(|c| c.to_string()),
        status: TaskStatus {
            state: parse_task_state(&task_row.status),
            message: None,
            timestamp: Some(task_row.updated_at),
        },
        artifacts: match serde_json::from_str(&task_row.artifacts) {
            Ok(v) => Some(v),
            Err(e) => {
                if task_row.artifacts != "[]" && !task_row.artifacts.is_empty() {
                    tracing::warn!(task_id = %task_row.task_id, "Corrupt task artifacts JSON: {}", e);
                }
                None
            }
        },
        history: if history.is_empty() {
            None
        } else {
            Some(history)
        },
        metadata: match serde_json::from_str(&task_row.metadata) {
            Ok(v) => Some(v),
            Err(e) => {
                if task_row.metadata != "{}" && !task_row.metadata.is_empty() {
                    tracing::warn!(task_id = %task_row.task_id, "Corrupt task metadata JSON: {}", e);
                }
                None
            }
        },
    };

    let result = serde_json::to_value(&task).map_err(|e| {
        tracing::error!("Failed to serialize task: {}", e);
        JsonRpcErrorResponse::internal_error(request.id.clone(), "Failed to serialize task")
    })?;
    Ok(JsonRpcResponse::success(request.id.clone(), result))
}

async fn handle_list_tasks(
    state: &HerdState,
    _project_id: Uuid,
    _org_id: Uuid,
    request: &JsonRpcRequest,
) -> Result<JsonRpcResponse, JsonRpcErrorResponse> {
    let params: ListTasksRequest = serde_json::from_value(request.params.clone())
        .map_err(|e| JsonRpcErrorResponse::invalid_params(request.id.clone(), &e.to_string()))?;

    let page_size = params.page_size.unwrap_or(50).min(100);

    let tasks: Vec<ClickHouseTaskRow> = state
        .clickhouse
        .query(
            "SELECT task_id, context_id, status, metadata, artifacts, updated_at, created_at
             FROM a2a_tasks FINAL
             ORDER BY created_at DESC
             LIMIT ?",
        )
        .bind(page_size)
        .fetch_all()
        .await
        .map_err(|e| {
            tracing::error!("ClickHouse list tasks query error: {}", e);
            JsonRpcErrorResponse::internal_error(request.id.clone(), "Failed to list tasks")
        })?;

    let task_list: Vec<Task> = tasks
        .into_iter()
        .map(|row| Task {
            id: row.task_id.to_string(),
            context_id: row.context_id.map(|c| c.to_string()),
            status: TaskStatus {
                state: parse_task_state(&row.status),
                message: None,
                timestamp: Some(row.updated_at),
            },
            artifacts: None,
            history: None,
            metadata: match serde_json::from_str(&row.metadata) {
                Ok(v) => Some(v),
                Err(e) => {
                    if row.metadata != "{}" && !row.metadata.is_empty() {
                        tracing::warn!(task_id = %row.task_id, "Corrupt task metadata JSON: {}", e);
                    }
                    None
                }
            },
        })
        .collect();

    let response = ListTasksResponse {
        total_size: task_list.len() as u32,
        page_size,
        next_page_token: String::new(),
        tasks: task_list,
    };

    let result = serde_json::to_value(&response).map_err(|e| {
        tracing::error!("Failed to serialize task list: {}", e);
        JsonRpcErrorResponse::internal_error(request.id.clone(), "Failed to serialize task list")
    })?;
    Ok(JsonRpcResponse::success(request.id.clone(), result))
}

async fn handle_cancel_task(
    state: &HerdState,
    _project_id: Uuid,
    _org_id: Uuid,
    request: &JsonRpcRequest,
) -> Result<JsonRpcResponse, JsonRpcErrorResponse> {
    let params: CancelTaskRequest = serde_json::from_value(request.params.clone())
        .map_err(|e| JsonRpcErrorResponse::invalid_params(request.id.clone(), &e.to_string()))?;

    // Produce cancellation event to Redpanda
    let cancel_event = serde_json::json!({
        "taskId": params.id,
        "method": "tasks/cancel",
        "status": "canceled",
        "timestamp": Utc::now().to_rfc3339(),
    });

    let payload = serde_json::to_vec(&cancel_event).map_err(|e| {
        JsonRpcErrorResponse::internal_error(request.id.clone(), &format!("Serialization: {}", e))
    })?;

    state
        .kafka
        .send_to_topic(A2A_MESSAGES_TOPIC, &params.id, &payload)
        .await
        .map_err(|e| {
            tracing::error!("Failed to produce cancellation: {}", e);
            JsonRpcErrorResponse::internal_error(request.id.clone(), "Cancel delivery failed")
        })?;

    let task = Task {
        id: params.id,
        context_id: None,
        status: TaskStatus {
            state: TaskState::Canceled,
            message: None,
            timestamp: Some(Utc::now()),
        },
        artifacts: None,
        history: None,
        metadata: params.metadata,
    };

    let result = serde_json::to_value(&task).map_err(|e| {
        tracing::error!("Failed to serialize task: {}", e);
        JsonRpcErrorResponse::internal_error(request.id.clone(), "Failed to serialize task")
    })?;
    Ok(JsonRpcResponse::success(request.id.clone(), result))
}

// Push notification config handlers delegate to the push module
async fn handle_create_push_config(
    state: &HerdState,
    key_id: Uuid,
    request: &JsonRpcRequest,
) -> Result<JsonRpcResponse, JsonRpcErrorResponse> {
    let params: CreatePushNotificationConfigRequest =
        serde_json::from_value(request.params.clone()).map_err(|e| {
            JsonRpcErrorResponse::invalid_params(request.id.clone(), &e.to_string())
        })?;

    let (agent_id, _org_id, _project_id) = resolve_source_agent(state, key_id)
        .await
        .map_err(|e| JsonRpcErrorResponse::internal_error(request.id.clone(), &e))?;

    let config_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO a2a_push_configs (id, task_id, agent_id, webhook_url, auth_scheme, auth_credentials)
         VALUES ($1, $2, $3, $4, $5, $6)"
    )
    .bind(config_id)
    .bind(&params.task_id)
    .bind(agent_id)
    .bind(&params.push_notification_config.url)
    .bind(params.push_notification_config.authentication.as_ref().map(|a| &a.scheme))
    .bind(params.push_notification_config.authentication.as_ref().and_then(|a| a.credentials.as_deref()))
    .execute(state.db.as_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to create push config: {}", e);
        JsonRpcErrorResponse::internal_error(request.id.clone(), "Failed to create push config")
    })?;

    state.routing_cache.insert_push_config(
        params.task_id.clone(),
        CachedPushConfig {
            id: config_id,
            webhook_url: params.push_notification_config.url.clone(),
            auth_scheme: params
                .push_notification_config
                .authentication
                .as_ref()
                .map(|a| a.scheme.clone()),
            auth_credentials: params
                .push_notification_config
                .authentication
                .as_ref()
                .and_then(|a| a.credentials.clone()),
        },
    );

    let result_config = PushNotificationConfig {
        id: Some(config_id.to_string()),
        url: params.push_notification_config.url,
        token: None,
        authentication: params.push_notification_config.authentication,
    };

    let result = serde_json::to_value(&TaskPushNotificationConfig {
        task_id: params.task_id,
        push_notification_config: result_config,
    })
    .map_err(|e| {
        tracing::error!("Failed to serialize push config: {}", e);
        JsonRpcErrorResponse::internal_error(request.id.clone(), "Failed to serialize push config")
    })?;

    Ok(JsonRpcResponse::success(request.id.clone(), result))
}

async fn handle_get_push_config(
    state: &HerdState,
    _project_id: Uuid,
    request: &JsonRpcRequest,
) -> Result<JsonRpcResponse, JsonRpcErrorResponse> {
    let params: GetPushNotificationConfigRequest = serde_json::from_value(request.params.clone())
        .map_err(|e| {
        JsonRpcErrorResponse::invalid_params(request.id.clone(), &e.to_string())
    })?;

    let config_id = Uuid::parse_str(&params.id).map_err(|_| {
        JsonRpcErrorResponse::invalid_params(request.id.clone(), "Invalid config id")
    })?;

    let row: Option<(Uuid, String, String, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT id, task_id, webhook_url, auth_scheme, auth_credentials
         FROM a2a_push_configs WHERE id = $1 AND task_id = $2",
    )
    .bind(config_id)
    .bind(&params.task_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to get push config: {}", e);
        JsonRpcErrorResponse::internal_error(request.id.clone(), "Failed to get push config")
    })?;

    let (id, task_id, url, scheme, creds) = row.ok_or_else(|| {
        A2aError::TaskNotFound
            .to_jsonrpc_error_response(request.id.clone(), Some("Push config not found"))
    })?;

    let config = TaskPushNotificationConfig {
        task_id,
        push_notification_config: PushNotificationConfig {
            id: Some(id.to_string()),
            url,
            token: None,
            authentication: scheme.map(|s| AuthenticationInfo {
                scheme: s,
                credentials: creds,
            }),
        },
    };

    let result = serde_json::to_value(&config).map_err(|e| {
        tracing::error!("Failed to serialize push config: {}", e);
        JsonRpcErrorResponse::internal_error(request.id.clone(), "Failed to serialize push config")
    })?;
    Ok(JsonRpcResponse::success(request.id.clone(), result))
}

async fn handle_list_push_configs(
    state: &HerdState,
    _project_id: Uuid,
    request: &JsonRpcRequest,
) -> Result<JsonRpcResponse, JsonRpcErrorResponse> {
    let params: ListPushNotificationConfigsRequest = serde_json::from_value(request.params.clone())
        .map_err(|e| JsonRpcErrorResponse::invalid_params(request.id.clone(), &e.to_string()))?;

    let rows: Vec<(Uuid, String, String, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT id, task_id, webhook_url, auth_scheme, auth_credentials
         FROM a2a_push_configs WHERE task_id = $1 ORDER BY created_at",
    )
    .bind(&params.task_id)
    .fetch_all(state.db.as_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to list push configs: {}", e);
        JsonRpcErrorResponse::internal_error(request.id.clone(), "Failed to list push configs")
    })?;

    let configs: Vec<TaskPushNotificationConfig> = rows
        .into_iter()
        .map(
            |(id, task_id, url, scheme, creds)| TaskPushNotificationConfig {
                task_id,
                push_notification_config: PushNotificationConfig {
                    id: Some(id.to_string()),
                    url,
                    token: None,
                    authentication: scheme.map(|s| AuthenticationInfo {
                        scheme: s,
                        credentials: creds,
                    }),
                },
            },
        )
        .collect();

    let response = ListPushNotificationConfigsResponse {
        configs: Some(configs),
        next_page_token: None,
    };

    let result = serde_json::to_value(&response).map_err(|e| {
        tracing::error!("Failed to serialize push configs: {}", e);
        JsonRpcErrorResponse::internal_error(request.id.clone(), "Failed to serialize push configs")
    })?;
    Ok(JsonRpcResponse::success(request.id.clone(), result))
}

async fn handle_delete_push_config(
    state: &HerdState,
    _project_id: Uuid,
    request: &JsonRpcRequest,
) -> Result<JsonRpcResponse, JsonRpcErrorResponse> {
    let params: DeletePushNotificationConfigRequest =
        serde_json::from_value(request.params.clone()).map_err(|e| {
            JsonRpcErrorResponse::invalid_params(request.id.clone(), &e.to_string())
        })?;

    let config_id = Uuid::parse_str(&params.id).map_err(|_| {
        JsonRpcErrorResponse::invalid_params(request.id.clone(), "Invalid config id")
    })?;

    let result = sqlx::query("DELETE FROM a2a_push_configs WHERE id = $1 AND task_id = $2")
        .bind(config_id)
        .bind(&params.task_id)
        .execute(state.db.as_ref())
        .await
        .map_err(|e| {
            tracing::error!("Failed to delete push config: {}", e);
            JsonRpcErrorResponse::internal_error(request.id.clone(), "Failed to delete push config")
        })?;

    if result.rows_affected() == 0 {
        return Err(A2aError::TaskNotFound
            .to_jsonrpc_error_response(request.id.clone(), Some("Push config not found")));
    }

    state
        .routing_cache
        .remove_push_config(&params.task_id, config_id);

    Ok(JsonRpcResponse::success(
        request.id.clone(),
        serde_json::json!({}),
    ))
}

// ClickHouse row types
#[derive(Debug, clickhouse::Row, serde::Deserialize)]
struct ClickHouseTaskRow {
    task_id: Uuid,
    context_id: Option<Uuid>,
    status: String,
    metadata: String,
    artifacts: String,
    updated_at: chrono::DateTime<Utc>,
    #[allow(dead_code)]
    created_at: chrono::DateTime<Utc>,
}

#[derive(Debug, clickhouse::Row, serde::Deserialize)]
struct ClickHouseMessageRow {
    message_id: Uuid,
    task_id: Uuid,
    context_id: Option<Uuid>,
    role: String,
    parts: String,
    reference_task_ids: Vec<Uuid>,
    metadata: String,
    #[allow(dead_code)]
    created_at: chrono::DateTime<Utc>,
}

fn parse_task_state(s: &str) -> TaskState {
    match s {
        "submitted" => TaskState::Submitted,
        "working" => TaskState::Working,
        "completed" => TaskState::Completed,
        "failed" => TaskState::Failed,
        "canceled" => TaskState::Canceled,
        "input-required" => TaskState::InputRequired,
        "rejected" => TaskState::Rejected,
        "auth-required" => TaskState::AuthRequired,
        _ => TaskState::Unknown,
    }
}
