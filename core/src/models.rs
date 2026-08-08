use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
#[allow(dead_code)] // User model for future authentication features
pub struct User {
    pub id: Uuid,
    pub email: String,
    pub password_hash: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Organization {
    pub id: Uuid,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Project {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub name: String,
    pub slug: String,
    pub created_by: Option<Uuid>, // who created it (audit)
    pub created_at: DateTime<Utc>,
    pub settings: Option<serde_json::Value>,
    /// GitHub repository URL for linking exceptions to commits/PRs
    #[serde(skip_serializing_if = "Option::is_none")]
    pub github_repo_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
#[allow(dead_code)] // ProjectKey model - project key validation done differently
pub struct ProjectKey {
    pub id: Uuid,
    pub project_id: Uuid,
    pub key: String,
    pub rate_limit: i32,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Dashboard {
    pub id: Uuid,
    pub project_id: Uuid,
    pub user_id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub is_default: bool,
    pub layout_config: serde_json::Value,
    pub refresh_interval: Option<i32>,
    pub time_range: Option<String>,
    pub locked: bool,
    pub import_source: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct DashboardTab {
    pub id: Uuid,
    pub dashboard_id: Uuid,
    pub name: String,
    pub display_order: i32,
    pub icon: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct DashboardWidget {
    pub id: Uuid,
    pub dashboard_id: Uuid,
    pub tab_id: Option<Uuid>,
    pub widget_type: String,
    pub widget_config: serde_json::Value,
    pub position_x: i32,
    pub position_y: i32,
    pub width: i32,
    pub height: i32,
    pub title: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct KnowledgeBaseDocument {
    pub id: Uuid,
    pub title: String,
    pub category: String,
    pub source_type: String,
    pub original_content: Option<String>,
    pub original_filename: Option<String>,
    pub severity: String,
    pub enabled: bool,
    /// "pending", "processing", "ready", "failed"
    pub embedding_status: String,
    pub embedding_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct KnowledgeBaseChunk {
    pub id: Uuid,
    pub document_id: Uuid,
    pub content: String,
    pub chunk_index: i32,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExceptionPayload {
    pub project_key: String,
    pub timestamp: Option<DateTime<Utc>>,
    pub level: String,
    pub message: String,
    pub exception: Option<ExceptionInfo>,
    pub context: Option<serde_json::Value>,
    pub tags: Option<serde_json::Value>,
    pub user: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
    // Deployment & environment context (extracted from resource attributes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub environment: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployment_id: Option<String>,
    /// VCS repository URL for GitHub integration (from vcs.repository.url.full)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repository_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime: Option<String>,
    // Kubernetes / container context
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container_id: Option<String>,
    // HTTP context
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_url: Option<String>,
    // User context
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExceptionInfo {
    #[serde(rename = "type")]
    pub exception_type: String,
    pub value: String,
    pub stacktrace: Option<Vec<StackFrame>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StackFrame {
    pub filename: Option<String>,
    pub function: Option<String>,
    pub lineno: Option<i32>,
    pub colno: Option<i32>,
    pub code: Option<String>,
    /// Whether this frame is from application code (true) or library code (false).
    /// If not provided, will be determined automatically based on filename patterns.
    #[serde(default)]
    pub in_app: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Exception {
    pub id: Uuid,
    pub project_id: Uuid,
    pub fingerprint: String,
    pub level: String,
    pub message: String, // OTel: required (if exception_type not set)
    pub exception_type: Option<String>, // OTel: required (if message not set) - at least one must be set
    pub exception_value: Option<String>, // OTel: optional
    pub stacktrace: serde_json::Value, // OTel: recommended but optional, empty JSON array if not present
    pub context: serde_json::Value,
    pub tags: serde_json::Value,
    pub user_data: serde_json::Value,
    pub timestamp: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

/// Exception with span_id (for trace detail view)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExceptionWithSpan {
    pub id: Uuid,
    pub project_id: Uuid,
    pub fingerprint: String,
    pub level: String,
    pub message: String,
    pub exception_type: Option<String>,
    pub exception_value: Option<String>,
    pub stacktrace: serde_json::Value, // OTel: recommended but optional, empty JSON array if not present
    pub context: serde_json::Value,
    pub tags: serde_json::Value,
    pub user_data: serde_json::Value,
    pub timestamp: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub span_id: Option<String>,
    pub service_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ExceptionGroup {
    pub id: Uuid,
    pub project_id: Uuid,
    pub fingerprint: String,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub count: i64,
    pub status: String,
    pub level: String,
    pub message: String,
    pub exception_type: Option<String>,
    pub exception_value: Option<String>,
    pub service_name: Option<String>,
    // Deployment & environment context
    pub environment: Option<String>,
    pub version: Option<String>,
    pub deployment_id: Option<String>,
    pub region: Option<String>,
    pub host_name: Option<String>,
    pub runtime: Option<String>,
    // Kubernetes / container context
    pub pod_name: Option<String>,
    pub cluster_name: Option<String>,
    pub container_id: Option<String>,
    // HTTP context
    pub http_method: Option<String>,
    pub http_url: Option<String>,
    // User context
    pub user_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExceptionGroupDetail {
    pub group: ExceptionGroup,
    pub recent_exceptions: Vec<Exception>,
    pub traces: Vec<Trace>,
    pub flag_changes: Vec<FlagChange>, // Feature flag changes that may be related
}

/// Navigation info for traversing between exception instances within a group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExceptionNavigation {
    pub current_error_id: Uuid,
    pub current_timestamp: DateTime<Utc>,
    pub prev_error_id: Option<Uuid>,
    pub prev_timestamp: Option<DateTime<Utc>>,
    pub next_error_id: Option<Uuid>,
    pub next_timestamp: Option<DateTime<Utc>>,
    pub total_count: i64,
    pub current_index: i64, // 1-based index
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct FlagChange {
    pub id: Uuid,
    pub flag_id: String,
    pub flag_name: Option<String>,
    pub environment: Option<String>,
    pub change_type: String,
    pub changed_by: Option<serde_json::Value>,
    pub impacted_services: Option<Vec<String>>,
    pub timestamp: DateTime<Utc>,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectStats {
    pub total_exceptions: i64,
    pub unresolved_exceptions: i64,
    pub resolved_exceptions: i64, // Count of resolved exception groups
    pub exception_rate_24h: Vec<ExceptionRatePoint>,
}

/// Extended stats that include exception groups list (for SSE streaming)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectStatsWithExceptions {
    pub stats: ProjectStats,
    pub exception_groups: Vec<ExceptionGroup>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExceptionRatePoint {
    pub time: DateTime<Utc>,
    pub count: i64,
}

// ============================================================================
// Tracing (Spans) Models
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanPayload {
    pub project_key: String,
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub trace_state: Option<String>,
    pub span_name: String,   // OTel: name
    pub span_kind: SpanKind, // OTel: kind
    pub service_name: Option<String>,
    pub start_time: Option<DateTime<Utc>>,
    pub duration_ns: Option<i64>, // Changed to nanoseconds for OTel
    pub status_code: StatusCode,  // OTel status code
    pub status_message: Option<String>,
    pub span_attributes: std::collections::HashMap<String, String>, // Structured attributes
    pub resource_attributes: std::collections::HashMap<String, String>,
    pub events: Option<Vec<SpanEvent>>,
    pub links: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SpanKind {
    #[default]
    SpanKindUnspecified = 0,
    SpanKindInternal = 1,
    SpanKindServer = 2,
    SpanKindClient = 3,
    SpanKindProducer = 4,
    SpanKindConsumer = 5,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StatusCode {
    #[default]
    StatusCodeUnset = 0,
    StatusCodeOk = 1,
    StatusCodeError = 2,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanEvent {
    pub timestamp: DateTime<Utc>,
    pub name: String,
    pub attributes: std::collections::HashMap<String, String>,
}

// ============================================================================
// Logging (Logs) Models
// ============================================================================

/// Raw OTLP log payload for Kafka queue
/// Contains raw bytes to be parsed by the consumer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawOtlpLogPayload {
    pub project_key: String,
    pub content_type: String, // "json" or "protobuf"
    #[serde(with = "base64_bytes")]
    pub raw_bytes: Vec<u8>,
    #[serde(default)]
    pub ingested_bytes: u64,
}

/// Raw OTLP trace payload for Kafka queue
/// Contains raw bytes to be parsed by the consumer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawOtlpTracePayload {
    pub project_key: String,
    pub content_type: String, // "json" or "protobuf"
    #[serde(with = "base64_bytes")]
    pub raw_bytes: Vec<u8>,
    #[serde(default)]
    pub ingested_bytes: u64,
}

/// Raw OTLP metrics payload for Kafka queue
/// Contains raw bytes to be parsed by the consumer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawOtlpMetricsPayload {
    pub project_key: String,
    pub content_type: String, // "json" or "protobuf"
    #[serde(with = "base64_bytes")]
    pub raw_bytes: Vec<u8>,
}

/// Helper module for base64 serialization of bytes
mod base64_bytes {
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
        encoded.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        base64::engine::general_purpose::STANDARD
            .decode(s)
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Span {
    // Required fields per OTel spec
    pub span_id: String,
    pub trace_id: String,
    pub span_name: String,
    pub timestamp: DateTime<Utc>, // OTel: start_time (required)
    pub duration_ns: i64,         // OTel: duration (required, nanoseconds)

    // Optional fields per OTel spec (use empty string instead of None for better ClickHouse performance)
    pub parent_span_id: String, // OTel: optional, empty for root spans
    pub trace_state: String,    // OTel: optional, empty if not present
    pub span_kind: String,      // OTel: optional (SPAN_KIND_*), empty if not present
    pub service_name: String,   // OTel: optional (from resource), empty if not present
    pub status_code: String, // OTel: optional (STATUS_CODE_*), empty or STATUS_CODE_UNSET if not present
    pub status_message: String, // OTel: optional, empty if not present
    #[serde(alias = "span_attributes", rename = "attributes")]
    pub span_attributes: serde_json::Value, // OTel: optional (default empty)
    pub resource_attributes: serde_json::Value, // OTel: optional (default empty)
    pub events: serde_json::Value, // OTel: optional (default empty array)
    pub links: serde_json::Value, // OTel: optional (default empty array)

    // Internal field (not part of OTel spec)
    pub project_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trace {
    pub trace_id: String,
    pub project_id: Uuid,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub duration_ns: i64, // nanoseconds
    pub span_count: i64,
    pub service_count: i64,
    pub status: String, // "ok", "error" (if any span has error status)
    /// Primary service name (the root span's service, or the most common service in the trace)
    #[serde(default)]
    pub service_name: String,
    /// Root span name (e.g. "POST /api/agent/chat"), for display in trace lists
    #[serde(default)]
    pub root_span_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceDetail {
    pub trace: Trace,
    pub spans: Vec<Span>,
    pub exceptions: Vec<ExceptionWithSpan>,
}
