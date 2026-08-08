//! A2A v1.0 error codes mapped to JSON-RPC error codes (§5.4).

use super::jsonrpc::{JsonRpcError, JsonRpcErrorResponse};

/// A2A-specific error types with their JSON-RPC numeric codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum A2aError {
    TaskNotFound,
    TaskNotCancelable,
    PushNotificationNotSupported,
    UnsupportedOperation,
    ContentTypeNotSupported,
    InvalidAgentResponse,
    ExtendedAgentCardNotConfigured,
    ExtensionSupportRequired,
    VersionNotSupported,
    AccessDenied,
}

impl A2aError {
    pub fn code(&self) -> i32 {
        match self {
            A2aError::TaskNotFound => -32001,
            A2aError::TaskNotCancelable => -32002,
            A2aError::PushNotificationNotSupported => -32003,
            A2aError::UnsupportedOperation => -32004,
            A2aError::ContentTypeNotSupported => -32005,
            A2aError::InvalidAgentResponse => -32006,
            A2aError::ExtendedAgentCardNotConfigured => -32007,
            A2aError::ExtensionSupportRequired => -32008,
            A2aError::VersionNotSupported => -32009,
            A2aError::AccessDenied => -32010,
        }
    }

    pub fn default_message(&self) -> &'static str {
        match self {
            A2aError::TaskNotFound => "Task not found",
            A2aError::TaskNotCancelable => "Task is not cancelable",
            A2aError::PushNotificationNotSupported => "Push notifications not supported",
            A2aError::UnsupportedOperation => "Unsupported operation",
            A2aError::ContentTypeNotSupported => "Content type not supported",
            A2aError::InvalidAgentResponse => "Invalid agent response",
            A2aError::ExtendedAgentCardNotConfigured => "Extended agent card not configured",
            A2aError::ExtensionSupportRequired => "Extension support required",
            A2aError::VersionNotSupported => "Version not supported",
            A2aError::AccessDenied => "Access not granted to target agent",
        }
    }

    pub fn to_jsonrpc_error(&self, detail: Option<&str>) -> JsonRpcError {
        let message = match detail {
            Some(d) => format!("{}: {}", self.default_message(), d),
            None => self.default_message().to_string(),
        };
        JsonRpcError {
            code: self.code(),
            message,
            data: None,
        }
    }

    pub fn to_jsonrpc_error_response(
        &self,
        id: serde_json::Value,
        detail: Option<&str>,
    ) -> JsonRpcErrorResponse {
        JsonRpcErrorResponse::new(id, self.to_jsonrpc_error(detail))
    }
}
