//! JSON-RPC 2.0 envelope types for the A2A protocol binding.
//!
//! A2A v1.0 uses JSON-RPC 2.0 as its primary protocol binding. All A2A
//! operations are dispatched through a single `POST /a2a` endpoint using
//! JSON-RPC method names.

use serde::{Deserialize, Serialize};

/// JSON-RPC 2.0 request envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub id: serde_json::Value,
    pub method: String,
    #[serde(default)]
    pub params: serde_json::Value,
}

/// JSON-RPC 2.0 success response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub id: serde_json::Value,
    pub result: serde_json::Value,
}

/// JSON-RPC 2.0 error response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcErrorResponse {
    pub jsonrpc: String,
    pub id: serde_json::Value,
    pub error: JsonRpcError,
}

/// JSON-RPC 2.0 error object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// A2A v1.0 JSON-RPC method names (§5.3).
pub mod methods {
    pub const SEND_MESSAGE: &str = "SendMessage";
    pub const SEND_STREAMING_MESSAGE: &str = "SendStreamingMessage";
    pub const GET_TASK: &str = "GetTask";
    pub const LIST_TASKS: &str = "ListTasks";
    pub const CANCEL_TASK: &str = "CancelTask";
    pub const SUBSCRIBE_TO_TASK: &str = "SubscribeToTask";
    pub const CREATE_PUSH_NOTIFICATION_CONFIG: &str = "CreateTaskPushNotificationConfig";
    pub const GET_PUSH_NOTIFICATION_CONFIG: &str = "GetTaskPushNotificationConfig";
    pub const LIST_PUSH_NOTIFICATION_CONFIGS: &str = "ListTaskPushNotificationConfigs";
    pub const DELETE_PUSH_NOTIFICATION_CONFIG: &str = "DeleteTaskPushNotificationConfig";
    pub const GET_EXTENDED_AGENT_CARD: &str = "GetExtendedAgentCard";
}

impl JsonRpcRequest {
    pub fn validate(&self) -> Result<(), JsonRpcError> {
        if self.jsonrpc != "2.0" {
            return Err(JsonRpcError {
                code: -32600,
                message: "Invalid Request: jsonrpc must be \"2.0\"".into(),
                data: None,
            });
        }
        if self.method.is_empty() {
            return Err(JsonRpcError {
                code: -32600,
                message: "Invalid Request: method must not be empty".into(),
                data: None,
            });
        }
        Ok(())
    }
}

impl JsonRpcResponse {
    pub fn success(id: serde_json::Value, result: serde_json::Value) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            result,
        }
    }
}

impl JsonRpcErrorResponse {
    pub fn new(id: serde_json::Value, error: JsonRpcError) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            error,
        }
    }

    pub fn parse_error() -> Self {
        Self::new(
            serde_json::Value::Null,
            JsonRpcError {
                code: -32700,
                message: "Parse error".into(),
                data: None,
            },
        )
    }

    pub fn invalid_request(id: serde_json::Value) -> Self {
        Self::new(
            id,
            JsonRpcError {
                code: -32600,
                message: "Invalid Request".into(),
                data: None,
            },
        )
    }

    pub fn method_not_found(id: serde_json::Value, method: &str) -> Self {
        Self::new(
            id,
            JsonRpcError {
                code: -32601,
                message: format!("Method not found: {}", method),
                data: None,
            },
        )
    }

    pub fn invalid_params(id: serde_json::Value, detail: &str) -> Self {
        Self::new(
            id,
            JsonRpcError {
                code: -32602,
                message: format!("Invalid params: {}", detail),
                data: None,
            },
        )
    }

    pub fn internal_error(id: serde_json::Value, detail: &str) -> Self {
        Self::new(
            id,
            JsonRpcError {
                code: -32603,
                message: format!("Internal error: {}", detail),
                data: None,
            },
        )
    }
}
