//! LLM Gateway Settings API
//!
//! Manage Gateway configuration: introspection, defaults, rate limits, cost controls.

use axum::{
    extract::State,
    http::HeaderMap,
    routing::{get, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use std::collections::HashMap;
use std::sync::Arc;

use crate::api::{extract_organization_id, extract_project_id, extract_user_id};
use crate::app_state::FlowState;
use crate::audit::{AuditCaller, AuditEventBuilder, AuditEventType, AuditOrigin};
use crate::error::Result;

const DEFAULT_THINKING_BUDGET_TOKENS: i32 = 10000;
const DEFAULT_RATE_LIMIT_RPM: i32 = 60;
/// LLM Gateway settings
#[derive(Debug, Serialize, Deserialize)]
pub struct LlmSettings {
    // Introspection
    #[serde(default)]
    pub introspection_enabled: bool,
    #[serde(default = "default_thinking_budget")]
    pub thinking_budget_tokens: i32,

    // Fallback behavior
    #[serde(default = "default_true")]
    pub fallback_enabled: bool,
    #[serde(default)]
    pub fallback_order: Option<Vec<String>>,
    #[serde(default = "default_true")]
    pub retry_enabled: bool,
    #[serde(default = "default_retry_attempts")]
    pub retry_max_attempts: i32,

    // Cost controls
    #[serde(default)]
    pub monthly_budget_usd: Option<f64>,
    #[serde(default = "default_true")]
    pub budget_alert_enabled: bool,
    #[serde(default)]
    pub budget_hard_stop: bool,
    #[serde(default)]
    pub per_request_limit_usd: Option<f64>,

    // Rate limiting
    #[serde(default)]
    pub rate_limit_enabled: bool,
    #[serde(default = "default_rpm")]
    pub rate_limit_rpm: i32,

    // Session cost budget
    /// Maximum USD spend allowed per session. None / 0.0 means disabled.
    #[serde(default)]
    pub session_budget_usd: Option<f64>,

    // Guardrails
    /// Per-project input/output content safety guardrails.
    /// All fields default to empty/null (off). Configure via the UI to activate
    /// individual checks independently.
    #[serde(default)]
    pub guardrails: crate::gateway::guardrails::GuardrailConfig,

    /// Whether the in-app AI agent is enabled for this project.
    #[serde(default = "default_true")]
    pub agent_enabled: bool,

    /// Scopes the in-app AI agent is allowed to use. Defaults to read-only scopes.
    #[serde(default = "default_agent_scopes")]
    pub agent_scopes: Vec<String>,

    /// Whether MooDeng should auto-investigate alerts and exceptions.
    #[serde(default)]
    pub auto_investigate: bool,

    /// Fraction of prompt-config requests to evaluate with LLM-as-judge (0.0-1.0).
    /// `None` or `0.0` means disabled. Used for prompt version quality comparison.
    #[serde(default)]
    pub judge_sample_rate: Option<f64>,

    /// Project-level default fallback models. Used when a request has no `models` array.
    #[serde(default)]
    pub default_fallback_models: Vec<String>,

    /// Project-level default provider preferences. Used when a request has
    /// no `provider` object.
    #[serde(default)]
    pub provider_preferences: Option<crate::gateway::types::ProviderPreferences>,

    /// Session profiles: named filter sets that determine which sessions
    /// get their content preserved for replay.
    #[serde(default)]
    pub session_profiles: Vec<crate::api::session_profiles::SessionProfile>,

    /// Session labels: user-defined taxonomy for automatic session classification.
    /// The moodeng session classifier assigns these labels to idle sessions.
    #[serde(default)]
    pub session_labels: Vec<SessionLabel>,

    /// MooDeng's per-project personality and domain context.
    #[serde(default)]
    pub agent_soul: AgentSoul,
}

/// A label in the user-defined session taxonomy. The classifier uses `definition`
/// to decide whether to apply this label. When `definition` is empty, the
/// classifier uses best-judgement based on the label `name` alone.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SessionLabel {
    pub name: String,
    #[serde(default)]
    pub definition: String,
}

/// Per-project personality and domain context that shapes MooDeng's behavior.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AgentSoul {
    #[serde(default)]
    pub project_description: String,
    #[serde(default)]
    pub tech_context: String,
    #[serde(default)]
    pub custom_instructions: String,
    #[serde(default)]
    pub tone: Option<String>,
    #[serde(default)]
    pub key_services: Vec<KeyService>,
    #[serde(default)]
    pub important_thresholds: Vec<String>,
    #[serde(default)]
    pub known_issues: Vec<String>,
    #[serde(default)]
    pub playbooks: Vec<Playbook>,
    #[serde(default)]
    pub never_do: Vec<String>,
    #[serde(default)]
    pub always_do: Vec<String>,
}

impl AgentSoul {
    pub fn is_empty(&self) -> bool {
        self.project_description.is_empty()
            && self.tech_context.is_empty()
            && self.custom_instructions.is_empty()
            && self.tone.is_none()
            && self.key_services.is_empty()
            && self.important_thresholds.is_empty()
            && self.known_issues.is_empty()
            && self.playbooks.is_empty()
            && self.never_do.is_empty()
            && self.always_do.is_empty()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct KeyService {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub owner: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Playbook {
    pub trigger: String,
    pub instructions: String,
}

fn default_true() -> bool {
    true
}
fn default_thinking_budget() -> i32 {
    DEFAULT_THINKING_BUDGET_TOKENS
}
fn default_retry_attempts() -> i32 {
    3
}
fn default_rpm() -> i32 {
    DEFAULT_RATE_LIMIT_RPM
}
fn default_agent_scopes() -> Vec<String> {
    reiver_mcp::scope::READ_ONLY_SCOPES
        .iter()
        .map(|s| s.to_string())
        .collect()
}

impl Default for LlmSettings {
    fn default() -> Self {
        Self {
            introspection_enabled: false,
            thinking_budget_tokens: DEFAULT_THINKING_BUDGET_TOKENS,
            fallback_enabled: true,
            fallback_order: Some(vec![
                "anthropic".to_string(),
                "openai".to_string(),
                "google".to_string(),
            ]),
            retry_enabled: true,
            retry_max_attempts: 3,
            monthly_budget_usd: None,
            budget_alert_enabled: true,
            budget_hard_stop: false,
            per_request_limit_usd: None,
            rate_limit_enabled: false,
            rate_limit_rpm: DEFAULT_RATE_LIMIT_RPM,
            session_budget_usd: None,
            guardrails: crate::gateway::guardrails::GuardrailConfig::default(),
            agent_enabled: true,
            agent_scopes: default_agent_scopes(),
            auto_investigate: false,
            judge_sample_rate: None,
            default_fallback_models: Vec::new(),
            provider_preferences: None,
            session_profiles: Vec::new(),
            session_labels: Vec::new(),
            agent_soul: AgentSoul::default(),
        }
    }
}

/// Settings row from database
#[derive(FromRow)]
struct SettingRow {
    key: String,
    value: String,
}

/// Create the LLM settings router
pub fn create_llm_settings_router() -> Router<Arc<FlowState>> {
    Router::new()
        .route("/", get(get_settings))
        .route("/", put(update_settings))
        .route("/models", get(list_project_models))
        .route("/filter-fields", get(list_filter_fields))
}

/// Standalone router that serves the model catalog without requiring a project context.
/// Mounted at `/llm/models` for platform-admin pages like SyncDashboard.
pub fn create_llm_models_router() -> Router<Arc<FlowState>> {
    Router::new()
        .route("/", get(list_models))
        .route("/pricing", get(list_model_pricing))
}

/// Get LLM Gateway settings for a project
async fn get_settings(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
) -> Result<Json<LlmSettings>> {
    let project_id = extract_project_id(&headers)?;

    // Fetch all gateway settings
    let settings_rows: Vec<SettingRow> = sqlx::query_as(
        r#"
        SELECT key, value
        FROM project_settings
        WHERE project_id = $1 AND key LIKE 'gateway_%'
        "#,
    )
    .bind(project_id)
    .fetch_all(state.db.as_ref())
    .await?;

    let mut settings = LlmSettings::default();
    for row in settings_rows {
        apply_setting_row(&mut settings, &row);
    }
    Ok(Json(settings))
}

fn apply_setting_row(settings: &mut LlmSettings, row: &SettingRow) {
    match row.key.as_str() {
        "gateway_introspection_enabled" => {
            settings.introspection_enabled = row.value == "true";
        }
        "gateway_thinking_budget_tokens" => {
            settings.thinking_budget_tokens =
                row.value.parse().unwrap_or(DEFAULT_THINKING_BUDGET_TOKENS);
        }
        "gateway_fallback_enabled" => {
            settings.fallback_enabled = row.value == "true";
        }
        "gateway_fallback_order" => {
            settings.fallback_order = serde_json::from_str(&row.value).ok();
        }
        "gateway_retry_enabled" => {
            settings.retry_enabled = row.value == "true";
        }
        "gateway_retry_max_attempts" => {
            settings.retry_max_attempts = row.value.parse().unwrap_or(3);
        }
        "gateway_monthly_budget_usd" => {
            settings.monthly_budget_usd = row.value.parse().ok();
        }
        "gateway_budget_alert_enabled" => {
            settings.budget_alert_enabled = row.value == "true";
        }
        "gateway_budget_hard_stop" => {
            settings.budget_hard_stop = row.value == "true";
        }
        "gateway_per_request_limit_usd" => {
            settings.per_request_limit_usd = row.value.parse().ok();
        }
        "gateway_rate_limit_enabled" => {
            settings.rate_limit_enabled = row.value == "true";
        }
        "gateway_rate_limit_rpm" => {
            settings.rate_limit_rpm = row.value.parse().unwrap_or(DEFAULT_RATE_LIMIT_RPM);
        }
        "gateway_session_budget_usd" => {
            settings.session_budget_usd = row.value.parse().ok();
        }
        "gateway_guardrails" => {
            if !row.value.is_empty() {
                if let Ok(cfg) = serde_json::from_str(&row.value) {
                    settings.guardrails = cfg;
                }
            }
        }
        "gateway_agent_enabled" => {
            settings.agent_enabled = row.value == "true";
        }
        "gateway_agent_scopes" => {
            if !row.value.is_empty() {
                if let Ok(scopes) = serde_json::from_str::<Vec<String>>(&row.value) {
                    settings.agent_scopes = scopes;
                }
            }
        }
        "gateway_auto_investigate" => {
            settings.auto_investigate = row.value == "true";
        }
        "gateway_judge_sample_rate" => {
            settings.judge_sample_rate = row.value.parse().ok().filter(|&v: &f64| v > 0.0);
        }
        "gateway_default_fallback_models" => {
            if !row.value.is_empty() {
                if let Ok(models) = serde_json::from_str::<Vec<String>>(&row.value) {
                    settings.default_fallback_models = models;
                }
            }
        }
        "gateway_provider_preferences" => {
            if !row.value.is_empty() {
                if let Ok(prefs) = serde_json::from_str(&row.value) {
                    settings.provider_preferences = Some(prefs);
                }
            }
        }
        "gateway_session_profiles" => {
            if !row.value.is_empty() {
                if let Ok(mut profiles) = serde_json::from_str::<
                    Vec<crate::api::session_profiles::SessionProfile>,
                >(&row.value)
                {
                    crate::api::session_profiles::migrate_profiles(&mut profiles);
                    settings.session_profiles = profiles;
                }
            }
        }
        "gateway_session_labels" => {
            if !row.value.is_empty() {
                if let Ok(labels) = serde_json::from_str::<Vec<SessionLabel>>(&row.value) {
                    settings.session_labels = labels;
                }
            }
        }
        "gateway_agent_soul" => {
            if !row.value.is_empty() {
                if let Ok(soul) = serde_json::from_str(&row.value) {
                    settings.agent_soul = soul;
                }
            }
        }
        _ => {}
    }
}

/// Update LLM Gateway settings for a project
async fn update_settings(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    Json(settings): Json<LlmSettings>,
) -> Result<Json<LlmSettings>> {
    let project_id = extract_project_id(&headers)?;
    let user_id = extract_user_id(&headers).ok();
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);

    let before = internal_get_settings(state.db.as_ref(), project_id).await?;

    // Validate settings
    let mut errors: Vec<String> = Vec::new();
    if settings.retry_max_attempts < 1 || settings.retry_max_attempts > 10 {
        errors.push("retry_max_attempts must be between 1 and 10".into());
    }
    if settings.thinking_budget_tokens < 0 || settings.thinking_budget_tokens > 200_000 {
        errors.push("thinking_budget_tokens must be between 0 and 200000".into());
    }
    if settings.rate_limit_rpm < 1 {
        errors.push("rate_limit_rpm must be at least 1".into());
    }
    if let Some(budget) = settings.monthly_budget_usd {
        if budget < 0.0 {
            errors.push("monthly_budget_usd cannot be negative".into());
        }
    }
    if let Some(limit) = settings.per_request_limit_usd {
        if limit < 0.0 {
            errors.push("per_request_limit_usd cannot be negative".into());
        }
    }
    if let Some(rate) = settings.judge_sample_rate {
        if !(0.0..=1.0).contains(&rate) {
            errors.push("judge_sample_rate must be between 0.0 and 1.0".into());
        }
    }
    if let Err(e) = reiver_mcp::scope::validate_scope_names(&settings.agent_scopes) {
        errors.push(format!("agent_scopes: {e}"));
    }
    for (i, profile) in settings.session_profiles.iter().enumerate() {
        if profile.name.trim().is_empty() {
            errors.push(format!("session_profiles[{}]: name cannot be empty", i));
        }
        if profile.filters.is_empty() {
            errors.push(format!(
                "session_profiles[{}]: must have at least one filter",
                i
            ));
        }
        for (j, filter) in profile.filters.iter().enumerate() {
            if !crate::api::session_profiles::is_valid_field(&filter.field) {
                errors.push(format!(
                    "session_profiles[{}].filters[{}]: unknown field '{}'",
                    i, j, filter.field
                ));
            }
        }
    }
    if settings.session_labels.len() > 50 {
        errors.push("session_labels cannot have more than 50 labels".into());
    }

    // Enforce tier-based max label types limit
    if let Ok(Some(org_id)) = state.get_organization_id(project_id).await {
        if let Ok(tier) = state.entitlements.get_config(org_id).await {
            let max = tier.config.prompt_hub.max_labels;
            if max >= 0 && (settings.session_labels.len() as i64) > max {
                errors.push(format!(
                    "Your plan allows at most {} label types (current: {}). Upgrade for more.",
                    max,
                    settings.session_labels.len()
                ));
            }
        }
    }
    for (i, label) in settings.session_labels.iter().enumerate() {
        if label.name.trim().is_empty() {
            errors.push(format!("session_labels[{}]: name cannot be empty", i));
        }
    }
    {
        let mut seen = std::collections::HashSet::new();
        for label in &settings.session_labels {
            if !seen.insert(label.name.as_str()) {
                errors.push(format!(
                    "session_labels: duplicate label name '{}'",
                    label.name
                ));
            }
        }
    }
    if !errors.is_empty() {
        return Err(crate::error::AppError::Validation(errors.join("; ")));
    }

    // Build settings map
    let settings_map: Vec<(&str, String)> = vec![
        (
            "gateway_introspection_enabled",
            settings.introspection_enabled.to_string(),
        ),
        (
            "gateway_thinking_budget_tokens",
            settings.thinking_budget_tokens.to_string(),
        ),
        (
            "gateway_fallback_enabled",
            settings.fallback_enabled.to_string(),
        ),
        (
            "gateway_fallback_order",
            serde_json::to_string(&settings.fallback_order).unwrap_or_default(),
        ),
        ("gateway_retry_enabled", settings.retry_enabled.to_string()),
        (
            "gateway_retry_max_attempts",
            settings.retry_max_attempts.to_string(),
        ),
        (
            "gateway_monthly_budget_usd",
            settings
                .monthly_budget_usd
                .map(|v| v.to_string())
                .unwrap_or_default(),
        ),
        (
            "gateway_budget_alert_enabled",
            settings.budget_alert_enabled.to_string(),
        ),
        (
            "gateway_budget_hard_stop",
            settings.budget_hard_stop.to_string(),
        ),
        (
            "gateway_per_request_limit_usd",
            settings
                .per_request_limit_usd
                .map(|v| v.to_string())
                .unwrap_or_default(),
        ),
        (
            "gateway_rate_limit_enabled",
            settings.rate_limit_enabled.to_string(),
        ),
        (
            "gateway_rate_limit_rpm",
            settings.rate_limit_rpm.to_string(),
        ),
        (
            "gateway_session_budget_usd",
            settings
                .session_budget_usd
                .map(|v| v.to_string())
                .unwrap_or_default(),
        ),
        (
            "gateway_guardrails",
            serde_json::to_string(&settings.guardrails).unwrap_or_default(),
        ),
        ("gateway_agent_enabled", settings.agent_enabled.to_string()),
        (
            "gateway_agent_scopes",
            serde_json::to_string(&settings.agent_scopes).unwrap_or_default(),
        ),
        (
            "gateway_auto_investigate",
            settings.auto_investigate.to_string(),
        ),
        (
            "gateway_judge_sample_rate",
            settings
                .judge_sample_rate
                .map(|v| v.to_string())
                .unwrap_or_default(),
        ),
        (
            "gateway_default_fallback_models",
            serde_json::to_string(&settings.default_fallback_models).unwrap_or_default(),
        ),
        (
            "gateway_provider_preferences",
            serde_json::to_string(&settings.provider_preferences)
                .unwrap_or_else(|_| "null".to_string()),
        ),
        (
            "gateway_session_profiles",
            serde_json::to_string(&settings.session_profiles).unwrap_or_else(|_| "[]".to_string()),
        ),
        (
            "gateway_session_labels",
            serde_json::to_string(&settings.session_labels).unwrap_or_else(|_| "[]".to_string()),
        ),
        (
            "gateway_agent_soul",
            serde_json::to_string(&settings.agent_soul).unwrap_or_else(|_| "{}".to_string()),
        ),
    ];

    // Use transaction to ensure atomicity of all settings updates
    let mut tx = state.db.begin().await?;

    // Upsert all settings within the transaction
    for (key, value) in settings_map {
        sqlx::query(
            r#"
            INSERT INTO project_settings (project_id, key, value)
            VALUES ($1, $2, $3)
            ON CONFLICT (project_id, key) DO UPDATE SET value = $3
            "#,
        )
        .bind(project_id)
        .bind(key)
        .bind(value)
        .execute(&mut *tx)
        .await?;
    }

    // Commit the transaction
    tx.commit().await?;

    // Invalidate settings cache so the gateway picks up changes immediately
    state.introspection_settings_cache.remove(&project_id);

    // Invalidate Redis-cached session labels so the consumer picks up changes immediately
    if let Ok(mut conn) = state.redis.get().await {
        let cache_key = format!("session_labels:{}", project_id);
        let _ = redis::cmd("DEL")
            .arg(&cache_key)
            .query_async::<i64>(&mut *conn)
            .await;
    }

    // Re-read persisted values to return the canonical state
    let saved = internal_get_settings(state.db.as_ref(), project_id).await?;

    let org_id = extract_organization_id(&headers);
    let mut audit = AuditEventBuilder::new(AuditEventType::LlmSettingsUpdated)
        .project(&project_id.to_string())
        .details(serde_json::json!({
            "before": {
                "introspection_enabled": before.introspection_enabled,
                "thinking_budget_tokens": before.thinking_budget_tokens,
                "fallback_enabled": before.fallback_enabled,
                "retry_enabled": before.retry_enabled,
                "retry_max_attempts": before.retry_max_attempts,
                "monthly_budget_usd": before.monthly_budget_usd,
                "budget_alert_enabled": before.budget_alert_enabled,
                "budget_hard_stop": before.budget_hard_stop,
                "per_request_limit_usd": before.per_request_limit_usd,
                "rate_limit_enabled": before.rate_limit_enabled,
                "rate_limit_rpm": before.rate_limit_rpm,
                "session_budget_usd": before.session_budget_usd,
                "agent_enabled": before.agent_enabled,
                "auto_investigate": before.auto_investigate,
                "judge_sample_rate": before.judge_sample_rate,
                "session_profile_count": before.session_profiles.len(),
                "session_labels": before.session_labels,
            },
            "after": {
                "introspection_enabled": saved.introspection_enabled,
                "thinking_budget_tokens": saved.thinking_budget_tokens,
                "fallback_enabled": saved.fallback_enabled,
                "retry_enabled": saved.retry_enabled,
                "retry_max_attempts": saved.retry_max_attempts,
                "monthly_budget_usd": saved.monthly_budget_usd,
                "budget_alert_enabled": saved.budget_alert_enabled,
                "budget_hard_stop": saved.budget_hard_stop,
                "per_request_limit_usd": saved.per_request_limit_usd,
                "rate_limit_enabled": saved.rate_limit_enabled,
                "rate_limit_rpm": saved.rate_limit_rpm,
                "session_budget_usd": saved.session_budget_usd,
                "agent_enabled": saved.agent_enabled,
                "auto_investigate": saved.auto_investigate,
                "judge_sample_rate": saved.judge_sample_rate,
                "session_profile_count": saved.session_profiles.len(),
                "session_labels": saved.session_labels,
            }
        }))
        .origin(
            &audit_origin.origin_type,
            &audit_origin.origin_ref,
            &audit_origin.origin_reason,
        )
        .caller(
            &audit_caller.caller_type,
            &audit_caller.key_label,
            &audit_caller.key_prefix,
        )
        .success();
    if let Some(uid) = user_id {
        audit = audit.user(uid);
    }
    if let Some(oid) = org_id {
        audit = audit.organization(oid);
    }
    audit.log(&state.clickhouse).await;

    emit_session_profile_diffs(
        &before.session_profiles,
        &saved.session_profiles,
        &project_id.to_string(),
        user_id,
        org_id,
        &audit_origin,
        &audit_caller,
        &state.clickhouse,
    )
    .await;

    Ok(Json(saved))
}

// ---------------------------------------------------------------------------
// Session profile audit diff
// ---------------------------------------------------------------------------

async fn emit_session_profile_diffs(
    before: &[crate::api::session_profiles::SessionProfile],
    after: &[crate::api::session_profiles::SessionProfile],
    project_id: &str,
    user_id: Option<uuid::Uuid>,
    org_id: Option<uuid::Uuid>,
    origin: &AuditOrigin,
    caller: &AuditCaller,
    ch: &crate::clickhouse_db::ClickHousePool,
) {
    let before_map: HashMap<uuid::Uuid, &crate::api::session_profiles::SessionProfile> =
        before.iter().map(|p| (p.id, p)).collect();
    let after_map: HashMap<uuid::Uuid, &crate::api::session_profiles::SessionProfile> =
        after.iter().map(|p| (p.id, p)).collect();

    let build = |event_type: AuditEventType, profile_id: uuid::Uuid, details: serde_json::Value| {
        let mut audit = AuditEventBuilder::new(event_type)
            .resource("session_profile", profile_id)
            .project(project_id)
            .details(details)
            .origin(
                &origin.origin_type,
                &origin.origin_ref,
                &origin.origin_reason,
            )
            .caller(&caller.caller_type, &caller.key_label, &caller.key_prefix)
            .success();
        if let Some(uid) = user_id {
            audit = audit.user(uid);
        }
        if let Some(oid) = org_id {
            audit = audit.organization(oid);
        }
        audit
    };

    for (id, profile) in &after_map {
        if !before_map.contains_key(id) {
            build(
                AuditEventType::SessionProfileCreated,
                *id,
                serde_json::json!({
                    "name": profile.name,
                    "filter_count": profile.filters.len(),
                }),
            )
            .log(ch)
            .await;
        }
    }

    for (id, profile) in &before_map {
        if !after_map.contains_key(id) {
            build(
                AuditEventType::SessionProfileDeleted,
                *id,
                serde_json::json!({
                    "name": profile.name,
                }),
            )
            .log(ch)
            .await;
        }
    }

    for (id, old) in &before_map {
        if let Some(new) = after_map.get(id) {
            let old_json = serde_json::to_value(old).ok();
            let new_json = serde_json::to_value(new).ok();
            if old_json != new_json {
                build(
                    AuditEventType::SessionProfileUpdated,
                    *id,
                    serde_json::json!({
                        "before": { "name": old.name, "filter_count": old.filters.len() },
                        "after":  { "name": new.name, "filter_count": new.filters.len() },
                    }),
                )
                .log(ch)
                .await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Model catalog endpoint
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct ModelCatalogResponse {
    providers: Vec<ProviderModels>,
}

#[derive(Debug, Serialize)]
struct ProviderModels {
    id: String,
    name: String,
    description: String,
    docs_url: String,
    auth_type: String,
    supports_streaming: bool,
    models: Vec<ModelEntry>,
}

#[derive(Debug, Serialize)]
struct ModelEntry {
    id: String,
    name: String,
}

/// Return all known models grouped by provider, served from in-memory cache.
async fn list_models(State(state): State<Arc<FlowState>>) -> Json<ModelCatalogResponse> {
    Json(build_catalog(&state, None).await)
}

/// Return models filtered to providers the project has an enabled integration for.
async fn list_project_models(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
) -> Result<Json<ModelCatalogResponse>> {
    let project_id = extract_project_id(&headers)?;

    #[derive(sqlx::FromRow)]
    struct ProviderRow {
        provider: String,
    }

    let rows: Vec<ProviderRow> = sqlx::query_as(
        "SELECT provider FROM llm_provider_integrations WHERE project_id = $1 AND enabled = true",
    )
    .bind(project_id)
    .fetch_all(state.db.as_ref())
    .await?;

    let configured: std::collections::HashSet<String> =
        rows.into_iter().map(|r| r.provider).collect();

    Ok(Json(build_catalog(&state, Some(&configured)).await))
}

async fn build_catalog(
    state: &FlowState,
    filter: Option<&std::collections::HashSet<String>>,
) -> ModelCatalogResponse {
    use crate::gateway::provider_types::Provider;
    use strum::IntoEnumIterator;

    let catalog_models = state.model_catalog_cache.all_providers_with_models().await;
    let catalog_map: std::collections::HashMap<String, Vec<_>> =
        catalog_models.into_iter().collect();

    let providers = Provider::iter()
        .filter(|provider| match filter {
            Some(set) => set.contains(provider.as_str()),
            None => true,
        })
        .map(|provider| {
            let models = catalog_map
                .get(provider.as_str())
                .map(|entries| {
                    entries
                        .iter()
                        .map(|e| ModelEntry {
                            id: e.gateway_model_id(),
                            name: e.name.clone(),
                        })
                        .collect()
                })
                .unwrap_or_default();

            ProviderModels {
                id: provider.as_str().to_string(),
                name: provider.display_name().to_string(),
                description: provider.description().to_string(),
                docs_url: provider.docs_url().to_string(),
                auth_type: provider.auth_type().to_string(),
                supports_streaming: provider.supports_streaming(),
                models,
            }
        })
        .collect();

    ModelCatalogResponse { providers }
}

// ---------------------------------------------------------------------------
// Public pricing / model-catalog endpoint
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct PricingResponse {
    providers: Vec<PricingProviderModels>,
}

#[derive(Debug, Serialize)]
struct PricingProviderModels {
    id: String,
    name: String,
    models: Vec<PricingModelEntry>,
}

#[derive(Debug, Serialize)]
struct PricingModelEntry {
    id: String,
    name: String,
    context_length: Option<i32>,
    pricing: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    latency: Option<LatencyInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_rate: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    security: Option<SecurityInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_count_24h: Option<u64>,
}

#[derive(Debug, Serialize)]
struct LatencyInfo {
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    ttft_p50_ms: f64,
    ttft_p95_ms: f64,
}

#[derive(Debug, Serialize)]
struct SecurityInfo {
    guardrail_rate: f64,
    pii_rate: f64,
    injection_rate: f64,
}

/// Public pricing endpoint: model catalog enriched with latency, error, and security stats.
async fn list_model_pricing(State(state): State<Arc<FlowState>>) -> Json<PricingResponse> {
    use crate::gateway::provider_types::Provider;
    use strum::IntoEnumIterator;

    let catalog_models = state.model_catalog_cache.all_providers_with_models().await;
    let catalog_map: std::collections::HashMap<String, Vec<_>> =
        catalog_models.into_iter().collect();

    let stats_map = state.global_model_stats.all().await;

    let providers: Vec<PricingProviderModels> = Provider::iter()
        .filter(|provider| provider.as_str() != "openrouter")
        .filter_map(|provider| {
            let entries = catalog_map.get(provider.as_str())?;
            if entries.is_empty() {
                return None;
            }

            let models: Vec<PricingModelEntry> = entries
                .iter()
                .map(|e| {
                    let stats = stats_map.get(&(
                        e.provider_slug.clone(),
                        e.gateway_model_id(),
                    ));

                    PricingModelEntry {
                        id: e.gateway_model_id(),
                        name: e.name.clone(),
                        context_length: e.context_length,
                        pricing: e.pricing.clone(),
                        latency: stats.as_ref().map(|s| LatencyInfo {
                            p50_ms: s.p50_ms,
                            p95_ms: s.p95_ms,
                            p99_ms: s.p99_ms,
                            ttft_p50_ms: s.ttft_p50_ms,
                            ttft_p95_ms: s.ttft_p95_ms,
                        }),
                        error_rate: stats.as_ref().map(|s| s.error_rate),
                        security: stats.as_ref().map(|s| SecurityInfo {
                            guardrail_rate: s.guardrail_rate,
                            pii_rate: s.pii_rate,
                            injection_rate: s.injection_rate,
                        }),
                        request_count_24h: stats.as_ref().map(|s| s.request_count_24h),
                    }
                })
                .collect();

            Some(PricingProviderModels {
                id: provider.as_str().to_string(),
                name: provider.display_name().to_string(),
                models,
            })
        })
        .collect();

    Json(PricingResponse { providers })
}

/// Return the available virtual fields for session profile filters.
async fn list_filter_fields() -> Json<Vec<crate::api::session_profiles::FieldDescriptor>> {
    Json(crate::api::session_profiles::available_fields())
}

/// Internal helper to read settings for a specific project.
async fn internal_get_settings(
    db: &crate::db::DbPool,
    project_id: uuid::Uuid,
) -> Result<LlmSettings> {
    let settings_rows: Vec<SettingRow> = sqlx::query_as(
        r#"
        SELECT key, value
        FROM project_settings
        WHERE project_id = $1 AND key LIKE 'gateway_%'
        "#,
    )
    .bind(project_id)
    .fetch_all(db)
    .await?;

    let mut settings = LlmSettings::default();
    for row in settings_rows {
        apply_setting_row(&mut settings, &row);
    }
    Ok(settings)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(key: &str, value: &str) -> SettingRow {
        SettingRow {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    // ====================================================================
    // Default values
    // ====================================================================

    #[test]
    fn default_settings_has_empty_fallback_models() {
        let s = LlmSettings::default();
        assert!(s.default_fallback_models.is_empty());
    }

    #[test]
    fn default_settings_has_no_provider_preferences() {
        let s = LlmSettings::default();
        assert!(s.provider_preferences.is_none());
    }

    #[test]
    fn default_settings_fallback_enabled() {
        let s = LlmSettings::default();
        assert!(s.fallback_enabled);
    }

    // ====================================================================
    // apply_setting_row: new routing fields
    // ====================================================================

    #[test]
    fn apply_fallback_enabled_true() {
        let mut s = LlmSettings::default();
        s.fallback_enabled = false;
        apply_setting_row(&mut s, &row("gateway_fallback_enabled", "true"));
        assert!(s.fallback_enabled);
    }

    #[test]
    fn apply_fallback_enabled_false() {
        let mut s = LlmSettings::default();
        apply_setting_row(&mut s, &row("gateway_fallback_enabled", "false"));
        assert!(!s.fallback_enabled);
    }

    #[test]
    fn apply_default_fallback_models() {
        let mut s = LlmSettings::default();
        apply_setting_row(
            &mut s,
            &row(
                "gateway_default_fallback_models",
                r#"["gpt-4o","claude-sonnet-4-6"]"#,
            ),
        );
        assert_eq!(s.default_fallback_models.len(), 2);
        assert_eq!(s.default_fallback_models[0], "gpt-4o");
        assert_eq!(s.default_fallback_models[1], "claude-sonnet-4-6");
    }

    #[test]
    fn apply_default_fallback_models_empty_string_keeps_default() {
        let mut s = LlmSettings::default();
        s.default_fallback_models = vec!["existing".into()];
        apply_setting_row(&mut s, &row("gateway_default_fallback_models", ""));
        assert_eq!(
            s.default_fallback_models,
            vec!["existing"],
            "Empty value should not overwrite"
        );
    }

    #[test]
    fn apply_default_fallback_models_invalid_json_keeps_default() {
        let mut s = LlmSettings::default();
        s.default_fallback_models = vec!["existing".into()];
        apply_setting_row(&mut s, &row("gateway_default_fallback_models", "not-json"));
        assert_eq!(s.default_fallback_models, vec!["existing"]);
    }

    #[test]
    fn apply_provider_preferences_full() {
        let mut s = LlmSettings::default();
        let json = r#"{
            "order": ["bedrock", "anthropic"],
            "only": ["anthropic", "bedrock"],
            "ignore": ["openai"],
            "allow_fallbacks": false,
            "sort": "latency"
        }"#;
        apply_setting_row(&mut s, &row("gateway_provider_preferences", json));

        let prefs = s.provider_preferences.unwrap();
        assert_eq!(prefs.order.as_ref().unwrap().len(), 2);
        assert_eq!(prefs.only.as_ref().unwrap().len(), 2);
        assert_eq!(prefs.ignore.as_ref().unwrap(), &vec!["openai".to_string()]);
        assert_eq!(prefs.allow_fallbacks, Some(false));
        assert_eq!(prefs.sort.as_deref(), Some("latency"));
    }

    #[test]
    fn apply_provider_preferences_empty_string_keeps_none() {
        let mut s = LlmSettings::default();
        apply_setting_row(&mut s, &row("gateway_provider_preferences", ""));
        assert!(s.provider_preferences.is_none());
    }

    #[test]
    fn apply_provider_preferences_null_string_keeps_none() {
        let mut s = LlmSettings::default();
        apply_setting_row(&mut s, &row("gateway_provider_preferences", "null"));
        assert!(
            s.provider_preferences.is_none(),
            "JSON 'null' should not deserialize into Some(ProviderPreferences)"
        );
    }

    #[test]
    fn apply_provider_preferences_minimal() {
        let mut s = LlmSettings::default();
        apply_setting_row(
            &mut s,
            &row("gateway_provider_preferences", r#"{"sort":"latency"}"#),
        );
        let prefs = s.provider_preferences.unwrap();
        assert_eq!(prefs.sort.as_deref(), Some("latency"));
        assert!(prefs.order.is_none());
        assert!(prefs.only.is_none());
    }

    // ====================================================================
    // Serde round-trip for LlmSettings
    // ====================================================================

    #[test]
    fn llm_settings_serde_round_trip_new_fields() {
        let mut s = LlmSettings::default();
        s.default_fallback_models = vec!["gpt-4o".into(), "claude-sonnet-4-6".into()];
        s.provider_preferences = Some(crate::gateway::types::ProviderPreferences {
            order: Some(vec!["bedrock".into()]),
            sort: Some("latency".into()),
            ..Default::default()
        });
        s.fallback_enabled = false;

        let json = serde_json::to_string(&s).unwrap();
        let back: LlmSettings = serde_json::from_str(&json).unwrap();

        assert_eq!(back.default_fallback_models, s.default_fallback_models);
        assert!(!back.fallback_enabled);
        let prefs = back.provider_preferences.unwrap();
        assert_eq!(prefs.order.as_ref().unwrap(), &vec!["bedrock".to_string()]);
        assert_eq!(prefs.sort.as_deref(), Some("latency"));
    }

    #[test]
    fn llm_settings_deserialize_without_new_fields_uses_defaults() {
        let json = r#"{"introspection_enabled": true}"#;
        let s: LlmSettings = serde_json::from_str(json).unwrap();
        assert!(s.default_fallback_models.is_empty());
        assert!(s.provider_preferences.is_none());
        assert!(s.fallback_enabled);
    }
}
