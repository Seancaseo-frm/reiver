//! Billing types for usage tracking and cost calculation.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Budget configuration for an organization or project.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Budget {
    pub id: Uuid,
    pub organization_id: Uuid,
    /// Optional project scope (None = org-wide budget)
    pub project_id: Option<Uuid>,
    /// Monthly budget limit in USD
    pub monthly_budget_usd: Decimal,
    /// Alert when usage reaches this percentage of budget (default: 80)
    pub alert_threshold_percent: i32,
    /// Whether this budget is active
    pub enabled: bool,
    pub created_by: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Timestamp of last threshold alert sent (prevents repeated alerts)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_threshold_alert_at: Option<DateTime<Utc>>,
    /// Timestamp of last budget exceeded alert sent
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_exceeded_alert_at: Option<DateTime<Utc>>,
    /// The usage percentage when last alert was sent
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_alert_percent: Option<i32>,
}

/// Current usage summary for the billing period.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageSummary {
    pub organization_id: Uuid,
    pub period_start: DateTime<Utc>,
    pub period_end: DateTime<Utc>,
    /// Ingested bytes for spans
    pub spans_ingested_bytes: u64,
    /// Ingested bytes for logs
    pub logs_ingested_bytes: u64,
    /// Count of metric data points
    pub metrics_count: u64,
    /// Estimated cost for this period
    pub estimated_cost_usd: Decimal,
    /// Amount already paid via approved charges for this period
    pub amount_paid_usd: Decimal,
    /// AI Gateway: total LLM requests in this period
    #[serde(default)]
    pub gateway_requests: u64,
    /// AI Gateway: total input tokens
    #[serde(default)]
    pub gateway_input_tokens: u64,
    /// AI Gateway: total output tokens
    #[serde(default)]
    pub gateway_output_tokens: u64,
    /// AI Gateway: estimated LLM cost for this period
    #[serde(default)]
    pub gateway_cost_usd: Decimal,
}

/// Usage breakdown by project.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectUsage {
    pub project_id: Uuid,
    pub project_name: Option<String>,
    pub spans_ingested_bytes: u64,
    pub logs_ingested_bytes: u64,
    pub metrics_count: u64,
    pub estimated_cost_usd: Decimal,
    /// AI Gateway: LLM request count for this project
    #[serde(default)]
    pub gateway_requests: u64,
    /// AI Gateway: estimated LLM cost for this project
    #[serde(default)]
    pub gateway_cost_usd: Decimal,
}

/// Budget status with current usage comparison.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BudgetStatus {
    pub budget: Budget,
    pub current_cost_usd: Decimal,
    /// Percentage of budget used
    pub usage_percent: f64,
    /// Whether threshold has been exceeded
    pub threshold_exceeded: bool,
    /// Whether budget has been exceeded
    pub budget_exceeded: bool,
}

/// Request to update budget settings.
#[derive(Debug, Clone, Deserialize)]
pub struct UpdateBudgetRequest {
    pub monthly_budget_usd: Option<Decimal>,
    pub alert_threshold_percent: Option<i32>,
    pub enabled: Option<bool>,
}

/// Request to create a budget.
#[derive(Debug, Clone, Deserialize)]
pub struct CreateBudgetRequest {
    pub project_id: Option<Uuid>,
    pub monthly_budget_usd: Decimal,
    pub alert_threshold_percent: Option<i32>,
}

/// AI Gateway cost breakdown by model for the billing period.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayModelCost {
    pub provider: String,
    pub model: String,
    pub request_count: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cost_usd: Decimal,
}

/// ClickHouse row for aggregated usage query (from reiver.usage).
#[derive(Debug, Clone, clickhouse::Row, Deserialize)]
pub struct UsageRow {
    pub event_type: String,
    pub value: u64,
}

/// ClickHouse row for per-project usage query (from reiver.usage).
#[derive(Debug, Clone, clickhouse::Row, Deserialize)]
pub struct ProjectUsageRow {
    pub project_id: String,
    pub event_type: String,
    pub value: u64,
}

/// ClickHouse row for aggregated BYOK cost query (from reiver.llm_cost_daily).
#[derive(Debug, Clone, clickhouse::Row, Deserialize)]
pub struct GatewayCostRow {
    pub request_count: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_cost_usd: f64,
}

/// ClickHouse row for per-project BYOK cost query (from reiver.llm_cost_daily).
#[derive(Debug, Clone, clickhouse::Row, Deserialize)]
pub struct ProjectGatewayCostRow {
    pub project_id: String,
    pub request_count: u64,
    pub total_cost_usd: f64,
}

/// ClickHouse row for per-model BYOK cost query (from reiver.llm_cost_daily).
#[derive(Debug, Clone, clickhouse::Row, Deserialize)]
pub struct ModelGatewayCostRow {
    pub gen_ai_system: String,
    pub gen_ai_request_model: String,
    pub request_count: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_cost_usd: f64,
}
