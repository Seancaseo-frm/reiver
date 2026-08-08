//! A2A v1.0 Protocol Data Model
//!
//! Full implementation of the Agent-to-Agent protocol canonical data model
//! as specified in the A2A v1.0 specification (released 2026-03-12).
//! All JSON field names use camelCase per the spec's JSON naming convention.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// 4.1 Core Objects
// ---------------------------------------------------------------------------

/// Task is the core unit of action in A2A. It has a current status, optional
/// artifacts (results), and optional message history.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Task {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_id: Option<String>,
    pub status: TaskStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifacts: Option<Vec<Artifact>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history: Option<Vec<Message>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// Container for the status of a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskStatus {
    pub state: TaskState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<Message>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<DateTime<Utc>>,
}

/// Lifecycle states of a Task (A2A v1.0 §4.1.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::Display)]
pub enum TaskState {
    #[serde(rename = "unknown")]
    Unknown,
    #[serde(rename = "submitted")]
    Submitted,
    #[serde(rename = "working")]
    Working,
    #[serde(rename = "completed")]
    Completed,
    #[serde(rename = "failed")]
    Failed,
    #[serde(rename = "canceled")]
    Canceled,
    #[serde(rename = "input-required")]
    InputRequired,
    #[serde(rename = "rejected")]
    Rejected,
    #[serde(rename = "auth-required")]
    AuthRequired,
}

impl TaskState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskState::Completed | TaskState::Failed | TaskState::Canceled | TaskState::Rejected
        )
    }
}

/// One unit of communication between client and server.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub message_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    pub role: Role,
    pub parts: Vec<Part>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reference_task_ids: Option<Vec<String>>,
}

/// Sender role for a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    User,
    Agent,
}

/// A section of communication content (oneOf: text, raw, url, data).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Part {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw: Option<String>, // base64-encoded bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
}

/// Artifacts represent task outputs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Artifact {
    pub artifact_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub parts: Vec<Part>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Vec<String>>,
}

// ---------------------------------------------------------------------------
// 4.2 Streaming Events (used for push notification payloads too)
// ---------------------------------------------------------------------------

/// Notification that a task's status changed.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskStatusUpdateEvent {
    pub task_id: String,
    pub context_id: String,
    pub status: TaskStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// Notification that a task artifact was generated or updated.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskArtifactUpdateEvent {
    pub task_id: String,
    pub context_id: String,
    pub artifact: Artifact,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub append: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_chunk: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// Wrapper for streaming/push notification responses. Exactly one field is set.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<Task>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<Message>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_update: Option<TaskStatusUpdateEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact_update: Option<TaskArtifactUpdateEvent>,
}

// ---------------------------------------------------------------------------
// 4.3 Push Notification Objects
// ---------------------------------------------------------------------------

/// Configuration for push notifications on a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PushNotificationConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authentication: Option<AuthenticationInfo>,
}

/// Authentication details for push notification delivery.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthenticationInfo {
    pub scheme: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials: Option<String>,
}

/// Wraps PushNotificationConfig with its parent task_id for CRUD operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskPushNotificationConfig {
    pub task_id: String,
    pub push_notification_config: PushNotificationConfig,
}

// ---------------------------------------------------------------------------
// 4.4 Agent Discovery Objects
// ---------------------------------------------------------------------------

/// Self-describing manifest for an agent (A2A v1.0 §4.4.1).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentCard {
    pub name: String,
    pub description: String,
    pub supported_interfaces: Vec<AgentInterface>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<AgentProvider>,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub documentation_url: Option<String>,
    pub capabilities: AgentCapabilities,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security_schemes: Option<HashMap<String, SecurityScheme>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security_requirements: Option<Vec<HashMap<String, Vec<String>>>>,
    pub default_input_modes: Vec<String>,
    pub default_output_modes: Vec<String>,
    pub skills: Vec<AgentSkill>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signatures: Option<Vec<AgentCardSignature>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub icon_url: Option<String>,
}

/// Service provider of an agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentProvider {
    pub url: String,
    pub organization: String,
}

/// Optional capabilities of an agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentCapabilities {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub streaming: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub push_notifications: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Vec<AgentExtension>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extended_agent_card: Option<bool>,
}

/// A protocol extension declared by an agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentExtension {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
}

/// A distinct capability or function that an agent can perform.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentSkill {
    pub id: String,
    pub name: String,
    pub description: String,
    pub tags: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub examples: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_modes: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_modes: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security_requirements: Option<Vec<HashMap<String, Vec<String>>>>,
}

/// Declares a URL + transport + protocol version for interacting with an agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentInterface {
    pub url: String,
    pub protocol_binding: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    pub protocol_version: String,
}

/// JWS signature of an AgentCard (RFC 7515).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentCardSignature {
    pub protected: String,
    pub signature: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub header: Option<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// 4.5 Security Objects
// ---------------------------------------------------------------------------

/// Discriminated union for security schemes (OpenAPI 3.2 style).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityScheme {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key_security_scheme: Option<ApiKeySecurityScheme>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_auth_security_scheme: Option<HttpAuthSecurityScheme>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oauth2_security_scheme: Option<OAuth2SecurityScheme>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub open_id_connect_security_scheme: Option<OpenIdConnectSecurityScheme>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mtls_security_scheme: Option<MutualTlsSecurityScheme>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiKeySecurityScheme {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub location: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpAuthSecurityScheme {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub scheme: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bearer_format: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OAuth2SecurityScheme {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub flows: OAuthFlows,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oauth2_metadata_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenIdConnectSecurityScheme {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub open_id_connect_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MutualTlsSecurityScheme {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// OAuth 2.0 flows configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OAuthFlows {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorization_code: Option<AuthorizationCodeOAuthFlow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_credentials: Option<ClientCredentialsOAuthFlow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_code: Option<DeviceCodeOAuthFlow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthorizationCodeOAuthFlow {
    pub authorization_url: String,
    pub token_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_url: Option<String>,
    pub scopes: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientCredentialsOAuthFlow {
    pub token_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_url: Option<String>,
    pub scopes: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceCodeOAuthFlow {
    pub device_authorization_url: String,
    pub token_url: String,
    pub scopes: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// 3.2 Operation Request/Response Types
// ---------------------------------------------------------------------------

/// Request to send a message to an agent (message/send).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SendMessageRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    pub message: Message,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub configuration: Option<SendMessageConfiguration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// Configuration for a send message request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SendMessageConfiguration {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accepted_output_modes: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_push_notification_config: Option<TaskPushNotificationConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history_length: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_immediately: Option<bool>,
}

/// The response to a SendMessage can be either a Task or a direct Message.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SendMessageResponse {
    Task(Task),
    Message(Message),
}

/// Request to get a task by ID.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetTaskRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history_length: Option<u32>,
}

/// Request to list tasks with filters.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListTasksRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<TaskState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_size: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history_length: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_timestamp_after: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_artifacts: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListTasksResponse {
    pub tasks: Vec<Task>,
    pub next_page_token: String,
    pub page_size: u32,
    pub total_size: u32,
}

/// Request to cancel a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelTaskRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// Request to create a push notification config on a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreatePushNotificationConfigRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    pub task_id: String,
    pub push_notification_config: PushNotificationConfig,
}

/// Request to get a push notification config.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetPushNotificationConfigRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    pub task_id: String,
    pub id: String,
}

/// Request to list push notification configs for a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListPushNotificationConfigsRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    pub task_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_size: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListPushNotificationConfigsResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub configs: Option<Vec<TaskPushNotificationConfig>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

/// Request to delete a push notification config.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeletePushNotificationConfigRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    pub task_id: String,
    pub id: String,
}

/// Request to get the extended agent card.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetExtendedAgentCardRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
}

// ---------------------------------------------------------------------------
// Herd-internal types (not part of A2A spec)
// ---------------------------------------------------------------------------

/// Visibility levels for agents registered on Reiver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::Display)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum AgentVisibility {
    Private,
    Org,
    Public,
}

/// Status of a cross-org access grant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::Display)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum AccessGrantStatus {
    Pending,
    Approved,
    Denied,
    Revoked,
}

/// Registered agent row (Postgres).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisteredAgent {
    pub id: Uuid,
    pub project_id: Uuid,
    pub organization_id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub agent_card: AgentCard,
    pub visibility: AgentVisibility,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Access grant row (Postgres).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessGrant {
    pub id: Uuid,
    pub target_agent_id: Uuid,
    pub target_org_id: Uuid,
    pub granted_org_id: Option<Uuid>,
    pub granted_agent_id: Option<Uuid>,
    pub status: AccessGrantStatus,
    pub requested_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub resolved_by: Option<Uuid>,
}
