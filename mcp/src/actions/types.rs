use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// LLM Provider
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum LlmProvider {
    Openai,
    Anthropic,
    Google,
    Theta,
    Bedrock,
}

// ---------------------------------------------------------------------------
// Chat message role
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum MessageRole {
    System,
    User,
    Assistant,
}

// ---------------------------------------------------------------------------
// Notification channels
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum NotificationChannelType {
    Slack,
    Teams,
    Discord,
    Pagerduty,
    Webhook,
}

// ---------------------------------------------------------------------------
// Identity providers
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub enum AuthProviderKind {
    #[serde(rename = "okta")]
    Okta,
    #[serde(rename = "auth0")]
    Auth0,
    #[serde(rename = "entra_id")]
    EntraId,
    #[serde(rename = "onelogin")]
    OneLogin,
    #[serde(rename = "ping_identity")]
    PingIdentity,
    #[serde(rename = "keycloak")]
    Keycloak,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum OneLoginRegion {
    Us,
    Eu,
}

// ---------------------------------------------------------------------------
// AWS
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum AwsServiceType {
    Ec2,
    Rds,
    Lambda,
    S3,
    Ecs,
    Eks,
    #[serde(rename = "dynamodb")]
    DynamoDb,
    Sqs,
    Sns,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AwsAuthMethod {
    Role,
    AccessKey,
}

// ---------------------------------------------------------------------------
// Observability filters
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ExceptionStatus {
    Unresolved,
    Resolved,
    Ignored,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum IncidentStatus {
    Open,
    Closed,
}

// ---------------------------------------------------------------------------
// Alert rules
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ThresholdType {
    Above,
    Below,
}

/// Query configuration embedded in an alert rule.
///
/// The `query_type` discriminator selects the variant: `"metrics"` (default),
/// `"log_pattern"`, `"promql"`, or `"llm"`. Only fields relevant to the
/// chosen type are used.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct AlertQueryConfigInput {
    /// Discriminator: `"metrics"` (default), `"log_pattern"`, `"promql"`, or `"llm"`.
    #[serde(default = "default_query_type")]
    pub query_type: String,
    /// Metric name. Required for `metrics` and `llm` types.
    /// For LLM gateway metrics use an "llm." prefix: "llm.error_rate",
    /// "llm.latency_p95", "llm.latency_avg", "llm.cost_daily",
    /// "llm.token_usage", "llm.request_count".
    #[serde(default)]
    pub metric_name: Option<String>,
    /// Key-value filters (e.g. `{"model": "<observed-model-id>"}` for LLM,
    /// `{"service.name": "api"}` for OTel metrics).
    #[serde(default)]
    pub filters: BTreeMap<String, String>,
    /// Dimensions to group results by
    #[serde(default)]
    pub group_by: Vec<String>,
    /// Time aggregation function (defaults to "avg"). One of: avg, sum, min, max, count, p50, p95, p99.
    #[serde(default = "default_time_agg")]
    pub time_aggregation: String,
    /// Space aggregation function (defaults to "avg"). One of: avg, sum, min, max, count.
    #[serde(default = "default_space_agg")]
    pub space_aggregation: String,
    /// Log patterns to match. Required for `log_pattern` type.
    #[serde(default)]
    pub patterns: Option<Vec<String>>,
    /// Log source filter: "all", "otlp", or "unstructured"
    #[serde(default)]
    pub log_source: Option<String>,
    /// Raw PromQL expression. Required for `promql` type.
    #[serde(default)]
    pub promql: Option<String>,
}

fn default_query_type() -> String {
    "metrics".into()
}
fn default_time_agg() -> String {
    "avg".into()
}
fn default_space_agg() -> String {
    "avg".into()
}

/// Input for creating a new alert rule. The `project_id` is injected automatically.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct CreateAlertRuleData {
    /// Human-readable rule name
    pub name: String,
    /// Optional description of what this rule monitors
    #[serde(default)]
    pub description: Option<String>,
    /// Query that defines the metric/condition to evaluate
    pub query_config: AlertQueryConfigInput,
    /// Numeric threshold value that triggers the alert
    #[serde(default)]
    pub threshold: f64,
    /// Whether to alert when the metric is above or below the threshold (defaults to "above")
    #[serde(default = "default_threshold_type")]
    pub threshold_type: ThresholdType,
    /// UUIDs of notification channels to send alerts to (from list_notification_channels)
    #[serde(default)]
    pub notification_channels: Vec<String>,
    /// Fire an alert when data stops arriving for absent_for_seconds
    #[serde(default)]
    pub alert_on_absent: bool,
    /// Seconds of missing data before an absence alert fires (default: 300)
    #[serde(default = "default_absent_for")]
    pub absent_for_seconds: i32,
    /// Evaluation window in seconds (default: 300)
    #[serde(default = "default_eval_window")]
    pub eval_window_seconds: i32,
    /// Evaluation interval in seconds (default: 60)
    #[serde(default = "default_eval_interval")]
    pub eval_interval_seconds: i32,
    /// Key-value labels for organizing and filtering rules
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    /// Key-value annotations (e.g. runbook URLs, descriptions)
    #[serde(default)]
    pub annotations: BTreeMap<String, String>,
    /// Whether the rule is enabled (defaults to true)
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_threshold_type() -> ThresholdType {
    ThresholdType::Above
}
fn default_absent_for() -> i32 {
    300
}
fn default_eval_window() -> i32 {
    300
}
fn default_eval_interval() -> i32 {
    60
}
fn default_enabled() -> bool {
    true
}

/// Input for updating an existing alert rule (partial update — only provided fields are changed).
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct UpdateAlertRuleData {
    /// Updated rule name
    pub name: Option<String>,
    /// Updated description
    pub description: Option<String>,
    /// Updated query configuration
    pub query_config: Option<AlertQueryConfigInput>,
    /// Updated threshold value
    pub threshold: Option<f64>,
    /// Updated threshold direction
    pub threshold_type: Option<ThresholdType>,
    /// Updated notification channel UUIDs
    pub notification_channels: Option<Vec<String>>,
    /// Updated absence alerting flag
    pub alert_on_absent: Option<bool>,
    /// Updated absence timeout in seconds
    pub absent_for_seconds: Option<i32>,
    /// Updated evaluation window in seconds
    pub eval_window_seconds: Option<i32>,
    /// Updated evaluation interval in seconds
    pub eval_interval_seconds: Option<i32>,
    /// Updated labels
    pub labels: Option<BTreeMap<String, String>>,
    /// Updated annotations
    pub annotations: Option<BTreeMap<String, String>>,
    /// Updated enabled flag
    pub enabled: Option<bool>,
}

// ---------------------------------------------------------------------------
// Widget queries (PromQL)
// ---------------------------------------------------------------------------

/// Time range for a widget query.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct TimeRange {
    /// Start of the range — ISO 8601 timestamp or relative expression like "now-1h"
    pub from: String,
    /// End of the range — ISO 8601 timestamp or relative expression like "now"
    pub to: String,
}

pub fn default_time_range() -> TimeRange {
    TimeRange {
        from: "now-1h".into(),
        to: "now".into(),
    }
}

/// PromQL-based widget query configuration.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct PromQLQueryConfig {
    /// PromQL expression (e.g. "rate(http_requests_total[5m])")
    pub promql: String,
    /// Legend template for series labels (e.g. "{{method}} {{status}}")
    #[serde(default)]
    pub legend_format: Option<String>,
    /// Additional sub-queries to overlay on the same chart
    #[serde(default)]
    pub queries: Option<Vec<PromQLSubQuery>>,
    /// If true, evaluate as an instant query instead of a range query
    #[serde(default)]
    pub instant: Option<bool>,
}

/// A sub-query overlaid on the same chart as the primary PromQL expression.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct PromQLSubQuery {
    /// PromQL expression for this sub-query
    pub promql: String,
    /// Legend template for this sub-query's series labels
    #[serde(default)]
    pub legend_format: Option<String>,
}

// ---------------------------------------------------------------------------
// Gateway settings
// ---------------------------------------------------------------------------

/// Guardrail configuration for the LLM gateway.
/// All fields default to off/empty. Only provided fields are updated.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct GuardrailConfigInput {
    /// Trust model: "agent" (tool results are untrusted) or "chatbot" (user + tool messages untrusted). Null disables role-based scanning.
    pub trust_mode: Option<String>,
    /// Input phrases that reject the request (case-insensitive substring match). Empty list disables.
    pub blocked_input_topics: Option<Vec<String>>,
    /// Max estimated prompt tokens (chars / 4). None disables the cap.
    pub max_prompt_tokens: Option<u32>,
    /// Block requests containing PII instead of redacting (default: false)
    pub pii_block_on_detect: Option<bool>,
    /// Scan untrusted-role messages for prompt injection patterns. Requires trust_mode to be set. (default: false)
    pub prompt_injection_detection: Option<bool>,
    /// Wrap untrusted-role messages in delimiters with a canary instruction. Requires trust_mode to be set. (default: false)
    pub spotlighting_enabled: Option<bool>,
    /// Mask PII in response content before returning to the client (default: false)
    pub mask_output_pii: Option<bool>,
    /// Output phrases that reject the response before it reaches the client. Empty list disables.
    pub blocked_output_topics: Option<Vec<String>>,
    /// Minimum LLM-as-judge quality score (0.0-1.0). None disables quality scoring.
    pub min_quality_score: Option<f64>,
    /// Tool names blocked project-wide (case-sensitive). Empty list disables.
    pub blocked_tools: Option<Vec<String>>,
    /// Block responses with data exfiltration patterns — markdown/HTML images pointing to external URLs (default: false)
    pub block_exfiltration_urls: Option<bool>,
}

/// Project-level provider routing preferences.
///
/// These defaults live in Reiver. Applications normally send `model: "auto"`
/// and omit per-request `models` / `provider` overrides so Flow owns routing.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct ProviderPreferencesInput {
    /// Ordered provider slugs to prefer, e.g. ["anthropic", "bedrock"].
    pub order: Option<Vec<String>>,
    /// Restrict routing to only these provider slugs.
    pub only: Option<Vec<String>>,
    /// Exclude these provider slugs from routing.
    pub ignore: Option<Vec<String>>,
    /// Allow fallback to other configured models/providers.
    pub allow_fallbacks: Option<bool>,
    /// Routing sort strategy. Currently supported: "latency".
    pub sort: Option<String>,
}

/// LLM gateway settings (partial update — only provided fields are changed).
/// Use get_gateway_settings first to see current values.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct GatewaySettingsInput {
    /// Enable extended thinking / introspection in gateway responses (default: false)
    pub introspection_enabled: Option<bool>,
    /// Token budget for the thinking block, 0-200000 (default: 10000)
    #[schemars(range(min = 0, max = 200000))]
    pub thinking_budget_tokens: Option<i32>,

    /// Enable automatic provider fallback on transient errors (default: true)
    pub fallback_enabled: Option<bool>,
    /// Ordered provider names for fallback, e.g. ["anthropic", "openai", "google"]
    pub fallback_order: Option<Vec<String>>,
    /// Ordered project-level model candidates used by `model: "auto"` and as
    /// the default fallback chain when an application supplies no `models`
    /// array. Use model IDs returned by `list` resource `model_catalog`.
    /// An explicit empty array clears the configured list so Flow derives
    /// candidates from the project's enabled integrations.
    pub default_fallback_models: Option<Vec<String>>,
    /// Project-level provider routing defaults. Applications normally omit
    /// per-request provider overrides and let Reiver apply these settings.
    pub provider_preferences: Option<ProviderPreferencesInput>,
    /// Enable automatic retries on transient errors (default: true)
    pub retry_enabled: Option<bool>,
    /// Max retry attempts, 1-10 (default: 3)
    #[schemars(range(min = 1, max = 10))]
    pub retry_max_attempts: Option<i32>,

    /// Monthly spend cap in USD. None means unlimited.
    pub monthly_budget_usd: Option<f64>,
    /// Alert when approaching the monthly budget (default: true)
    pub budget_alert_enabled: Option<bool>,
    /// Hard-stop all requests when budget is exhausted (default: false)
    pub budget_hard_stop: Option<bool>,
    /// Max USD cost per individual request. None means unlimited.
    pub per_request_limit_usd: Option<f64>,

    /// Enable rate limiting (default: false)
    pub rate_limit_enabled: Option<bool>,
    /// Requests per minute limit (default: 60)
    #[schemars(range(min = 1))]
    pub rate_limit_rpm: Option<i32>,

    /// Max USD spend per session. None or 0.0 disables session budgets.
    pub session_budget_usd: Option<f64>,

    /// Enable the in-app AI agent for this project (default: true)
    pub agent_enabled: Option<bool>,
    /// Scopes the in-app agent is allowed to use. Defaults to read-only scopes.
    pub agent_scopes: Option<Vec<String>>,

    /// Content safety guardrails. Only provided sub-fields are changed.
    pub guardrails: Option<GuardrailConfigInput>,

    /// Session profiles: named filter sets that determine which sessions
    /// get their content preserved for replay. Replaces the full list when provided.
    pub session_profiles: Option<Vec<SessionProfileInput>>,

    /// Session labels: user-defined taxonomy for automatic session classification.
    /// Each label has a name and an optional definition that guides the classifier.
    /// Replaces the full list when provided. Max 50 labels, unique names.
    pub session_labels: Option<Vec<SessionLabelInput>>,

    /// Agent Soul: per-project personality and domain context for MooDeng.
    /// Only provided sub-fields are changed.
    pub agent_soul: Option<AgentSoulInput>,
}

/// A label in the user-defined session taxonomy for automatic classification.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct SessionLabelInput {
    /// Label name (unique, non-empty)
    pub name: String,
    /// Classification criteria for the label. If empty, the classifier uses
    /// best-judgement from the name alone.
    #[serde(default)]
    pub definition: Option<String>,
}

/// A session profile defining criteria for preserving session content.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct SessionProfileInput {
    /// Profile ID (UUID). Generate a new one for creation, or use an existing one to update.
    pub id: String,
    /// Human-readable profile name
    pub name: String,
    /// How filters are combined: "AND" or "OR" (default: AND)
    pub logic: Option<String>,
    /// Filter rules for matching sessions
    pub filters: Vec<SessionFilterInput>,
}

/// A single filter condition within a session profile.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct SessionFilterInput {
    /// Virtual field path (e.g. "errors.count", "latency.avg_ms", "tools.names").
    /// Use get resource "session_profile_filter_fields" to discover available fields.
    pub field: String,
    /// Comparison operator for numeric filters: "lt", "lte", "gt", "gte"
    pub op: Option<String>,
    /// Threshold value (number for numeric fields, string for set fields)
    pub value: Option<serde_json::Value>,
}

/// Per-project personality and domain context for MooDeng (partial update).
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct AgentSoulInput {
    /// High-level description of what this project does.
    pub project_description: Option<String>,
    /// Tech stack, infrastructure, and deployment details.
    pub tech_context: Option<String>,
    /// Freeform instructions injected into MooDeng's system prompt.
    pub custom_instructions: Option<String>,
    /// Communication style: "concise", "detailed", "casual", or "formal".
    pub tone: Option<String>,
    /// Services MooDeng should know about when investigating.
    pub key_services: Option<Vec<KeyServiceInput>>,
    /// SLOs and thresholds MooDeng references when evaluating health.
    pub important_thresholds: Option<Vec<String>>,
    /// Known quirks MooDeng factors in before escalating.
    pub known_issues: Option<Vec<String>>,
    /// Step-by-step workflows for specific triggers.
    pub playbooks: Option<Vec<PlaybookInput>>,
    /// Hard constraints MooDeng must never violate.
    pub never_do: Option<Vec<String>>,
    /// Rules MooDeng must always follow.
    pub always_do: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct KeyServiceInput {
    pub name: String,
    pub description: Option<String>,
    pub owner: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct PlaybookInput {
    pub trigger: String,
    pub instructions: String,
}
