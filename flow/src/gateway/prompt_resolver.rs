//! Prompt Configuration Resolver for the AI Gateway.
//!
//! This module handles variant assignment for progressive rollouts and applies
//! prompt configuration modifications to incoming requests.
//!
//! # Caching
//!
//! Active rollouts and version configs are cached in Redis to reduce database load:
//! - Active rollouts: 30 second TTL (short because weights change during rollouts)
//! - Version configs: 5 minute TTL (immutable once created)

use axum::http::HeaderMap;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;
use std::collections::HashMap;
use tracing;
use uuid::Uuid;

use crate::gateway::domain_types::{AllocationType, OutputFailureAction};
use crate::gateway::prompt_store::PromptStore;
use crate::gateway::prompt_variable_def::VariableDefinition;
use crate::gateway::types::{
    ChatCompletionRequest, ChatMessage, ContentPart, MessageContent, MessageRole,
};
use crate::llm::template::compile_prompt;
use crate::llm::types::RolloutVariant;

/// Maximum length for X-Reiver-* header values to prevent memory exhaustion
const MAX_HEADER_VALUE_LENGTH: usize = 255;

/// Safely extract a header value with length validation.
/// Returns None if the header is missing, not valid UTF-8, or exceeds MAX_HEADER_VALUE_LENGTH.
fn get_validated_header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .filter(|s| s.len() <= MAX_HEADER_VALUE_LENGTH)
        .map(|s| s.to_string())
}

/// Resolution result containing the variant assignment and version info.
#[derive(Debug, Clone)]
pub struct PromptResolution {
    /// The rollout ID (if part of a rollout)
    pub rollout_id: Option<Uuid>,
    /// The variant assigned: Target or Baseline
    pub variant: RolloutVariant,
    /// The prompt config ID
    pub config_id: Uuid,
    /// The prompt version ID being used
    pub version_id: Uuid,

    /// JSON schema from the prompt version's `response_format` field.
    /// When set, the gateway validates the LLM response against this schema
    /// after the provider call (non-streaming only).
    pub output_schema: Option<Value>,

    pub output_failure_action: OutputFailureAction,

    /// Tool name whitelist from the prompt version. `None` = no restriction.
    /// Empty vec = no tools allowed.
    pub allowed_tools: Option<Vec<String>>,
}

/// Full version configuration for applying to requests.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct PromptVersionConfig {
    pub id: Uuid,
    pub system_prompt: Option<String>,
    pub model: Option<String>,
    pub temperature: Decimal,
    pub max_tokens: Option<i32>,
    /// Template variable definitions for Handlebars compilation.
    /// Each element has shape: `{name, type? (alias var_type), required?, default?, description?, values?, max_chars?, min?, max?}`
    #[serde(default)]
    pub variables: Value,
    /// OpenAI-compatible tool/function definitions
    pub tools: Option<Value>,
    /// JSON schema for structured output (response_format).
    /// Also used by the gateway for post-response output contract validation.
    pub response_format: Option<Value>,
    /// Extra parameters map.  Currently used for `output_failure_action`
    /// (`"error"` | `"retry"` | `"retry_then_passthrough"` | `"log_only"`).
    #[serde(default)]
    pub parameters: Value,
    /// Tool name whitelist. `NULL` = no restriction (all tools allowed).
    /// Empty array = no tools allowed. Only tool calls matching these names
    /// are permitted; non-matching tools are stripped from the request.
    pub allowed_tools: Option<Value>,
}

/// Parse the `allowed_tools` JSON value into a typed `Option<Vec<String>>`.
/// `None` (SQL NULL) = no restriction. `Some([])` = no tools allowed.
fn extract_allowed_tools(config: &PromptVersionConfig) -> Option<Vec<String>> {
    config
        .allowed_tools
        .as_ref()
        .and_then(|v| serde_json::from_value::<Vec<String>>(v.clone()).ok())
}

/// Extract the output contract fields from a resolved `PromptVersionConfig`.
///
/// `output_schema` is taken from `response_format` (already stored + forwarded to the
/// provider; the gateway also validates the response against it post-call).
/// `output_failure_action` is taken from `parameters["output_failure_action"]`.
fn extract_output_contract(config: &PromptVersionConfig) -> (Option<Value>, OutputFailureAction) {
    let output_schema = config.response_format.clone();
    let output_failure_action = config
        .parameters
        .get("output_failure_action")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<OutputFailureAction>().ok())
        .unwrap_or_default();
    (output_schema, output_failure_action)
}

/// Active rollout information from the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveRollout {
    pub id: Uuid,
    pub config_id: Uuid,
    pub target_version_id: Uuid,
    pub baseline_version_id: Option<Uuid>,
    pub current_weight: i32,
    pub allocation_type: AllocationType,
}

#[derive(Debug, sqlx::FromRow)]
pub struct ActiveRolloutRow {
    pub id: Uuid,
    pub config_id: Uuid,
    pub target_version_id: Uuid,
    pub baseline_version_id: Option<Uuid>,
    pub current_weight: i32,
    pub allocation_type: String,
}

impl From<ActiveRolloutRow> for ActiveRollout {
    fn from(row: ActiveRolloutRow) -> Self {
        Self {
            id: row.id,
            config_id: row.config_id,
            target_version_id: row.target_version_id,
            baseline_version_id: row.baseline_version_id,
            current_weight: row.current_weight,
            allocation_type: row.allocation_type.parse().unwrap_or_default(),
        }
    }
}

/// Deterministic hash for sticky variant assignment.
///
/// Combines the allocation key (user ID or session ID) with the rollout ID so
/// the same key is stable within a rollout but reshuffles across different ones.
fn stable_hash(key: &str, rollout_id: &Uuid) -> u32 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    rollout_id.hash(&mut hasher);
    hasher.finish() as u32
}

/// Assign a rollout variant based on the configured weight and allocation type.
///
/// - `Random`: each request independently rolls the dice.
/// - `UserSticky`: hashes `x-reiver-user-id` so the same user always sees
///   the same variant for a given rollout.
/// - `SessionSticky`: hashes `x-reiver-session-id` for session-level stability.
fn assign_variant(
    rollout: &ActiveRollout,
    headers: &HeaderMap,
) -> RolloutVariant {
    let weight = (rollout.current_weight.max(0) as u32).min(100);
    if weight == 0 {
        return RolloutVariant::Baseline;
    }
    if weight >= 100 {
        return RolloutVariant::Target;
    }

    let bucket = match rollout.allocation_type {
        AllocationType::UserSticky => {
            let user_id = get_validated_header(headers, "x-reiver-user-id")
                .unwrap_or_default();
            stable_hash(&user_id, &rollout.id) % 100
        }
        AllocationType::SessionSticky => {
            let session_id = get_validated_header(headers, "x-reiver-session-id")
                .unwrap_or_default();
            stable_hash(&session_id, &rollout.id) % 100
        }
        AllocationType::Random => rand::random::<u32>() % 100,
    };

    if bucket < weight {
        RolloutVariant::Target
    } else {
        RolloutVariant::Baseline
    }
}

/// Attempt to resolve a forced variant override from request headers.
///
/// Checks for the `X-Reiver-Force-Variant` header, validates it, and looks up
/// the active rollout for the given config. Returns `None` at any step that fails,
/// allowing the caller to fall back to default resolution.
async fn resolve_forced_variant(
    store: &dyn PromptStore,
    config_id: Uuid,
    active_version_id: Uuid,
    headers: &HeaderMap,
) -> Option<(PromptResolution, PromptVersionConfig)> {
    let variant_str = get_validated_header(headers, "x-reiver-force-variant")?;
    let variant = RolloutVariant::from_str(&variant_str)?;
    let rollout = store.get_active_rollout(config_id).await?;

    let version_id = if variant == RolloutVariant::Target {
        rollout.target_version_id
    } else {
        rollout.baseline_version_id.unwrap_or(active_version_id)
    };

    let version = store.get_version_config(version_id).await?;
    let (output_schema, output_failure_action) = extract_output_contract(&version);
    let allowed_tools = extract_allowed_tools(&version);
    Some((
        PromptResolution {
            rollout_id: Some(rollout.id),
            variant,
            config_id,
            version_id,
            output_schema,
            output_failure_action,
            allowed_tools,
        },
        version,
    ))
}

/// Result of resolving and compiling a prompt config for the playground.
/// Contains the compiled system prompt (variables substituted) and all
/// version settings that should override the request.
#[derive(Debug, Clone)]
pub struct CompiledPrompt {
    pub config_id: Uuid,
    pub version_id: Uuid,
    pub version_number: i32,
    /// Compiled system prompt (Handlebars variables substituted). `None` if the
    /// version has no system_prompt defined.
    pub system_prompt: Option<String>,
    pub model: Option<String>,
    pub temperature: Decimal,
    pub max_tokens: Option<i32>,
    pub tools: Option<Value>,
    pub response_format: Option<Value>,
    pub allowed_tools: Option<Value>,
}

/// Resolve a prompt config by name (and optional version override), validate
/// variables, and compile the Handlebars template.
///
/// This is the shared utility used by both the playground and the gateway.
/// It does NOT require headers or rollout logic — just a direct DB lookup.
///
/// Returns an error string on failure (suitable for user-facing 422 messages).
pub async fn resolve_and_compile_prompt(
    store: &dyn PromptStore,
    db: &PgPool,
    project_id: Uuid,
    config_name: &str,
    version_id_override: Option<Uuid>,
    variables: Option<&HashMap<String, Value>>,
) -> Result<CompiledPrompt, String> {
    let config_row = store
        .get_config_by_name(project_id, config_name)
        .await
        .ok_or_else(|| format!("Prompt config '{}' not found in this project", config_name))?;

    let config_id = config_row.id;

    // Determine which version to load
    let target_version_id = version_id_override
        .or(config_row.active_version_id)
        .ok_or_else(|| format!("Prompt config '{}' has no active version", config_name))?;

    // Load version config (cached)
    let version = store
        .get_version_config(target_version_id)
        .await
        .ok_or_else(|| format!("Prompt version {} not found", target_version_id))?;

    // Load the version number (still direct DB -- version number is only
    // used by the playground/compile endpoint and not part of the hot path)
    let version_number: (i32,) =
        sqlx::query_as("SELECT version FROM llm_prompt_versions WHERE id = $1")
            .bind(target_version_id)
            .fetch_optional(db)
            .await
            .map_err(|e| format!("Database error loading version number: {e}"))?
            .unwrap_or((0,));

    // Merge variables with defaults
    let mut vars = variables.cloned().unwrap_or_default();

    // Validate variables against schema (also injects defaults)
    if let Err((variable, detail)) = validate_prompt_variables(&version, &mut vars) {
        return Err(format!("Variable '{}': {}", variable, detail));
    }

    // Compile the system prompt template
    let compiled_system_prompt = match &version.system_prompt {
        Some(template) if !vars.is_empty() => match compile_prompt(template, &vars) {
            Ok(compiled) => Some(compiled),
            Err(e) => {
                return Err(format!(
                    "Prompt template compilation failed for '{}': {}",
                    config_name, e
                ));
            }
        },
        Some(template) => Some(template.clone()),
        None => None,
    };

    Ok(CompiledPrompt {
        config_id,
        version_id: target_version_id,
        version_number: version_number.0,
        system_prompt: compiled_system_prompt,
        model: version.model.clone(),
        temperature: version.temperature,
        max_tokens: version.max_tokens,
        tools: version.tools.clone(),
        response_format: version.response_format.clone(),
        allowed_tools: version.allowed_tools.clone(),
    })
}

/// Resolve prompt config when an explicit config name is provided via header or body.
///
/// Looks up the named config, checks for a forced variant override, and falls back
/// to the config's active version. Returns `None` if neither source provides a name,
/// the config is not found, or the config has no active version — allowing the caller
/// to fall through to project-level rollout resolution.
///
/// Header (`X-Reiver-Prompt-Config`) takes precedence over `body_config_name`.
async fn resolve_explicit_config(
    store: &dyn PromptStore,
    project_id: Uuid,
    headers: &HeaderMap,
    body_config_name: Option<&str>,
) -> Option<(PromptResolution, PromptVersionConfig)> {
    // Header takes precedence; body field is the fallback.
    let config_name = get_validated_header(headers, "x-reiver-prompt-config")
        .or_else(|| body_config_name.map(|s| s.to_string()))?;

    let config_row = store.get_config_by_name(project_id, &config_name).await?;
    let config_id = config_row.id;
    let active_version_id = config_row.active_version_id?;

    // Try force variant with active rollout first
    if let Some(result) =
        resolve_forced_variant(store, config_id, active_version_id, headers).await
    {
        return Some(result);
    }

    // Check for an active rollout and assign variant by weight
    if let Some(rollout) = store.get_active_rollout(config_id).await {
        let variant = assign_variant(&rollout, headers);
        let version_id = if variant == RolloutVariant::Target {
            rollout.target_version_id
        } else {
            rollout.baseline_version_id.unwrap_or(active_version_id)
        };
        let version = store.get_version_config(version_id).await?;
        let (output_schema, output_failure_action) = extract_output_contract(&version);
        let allowed_tools = extract_allowed_tools(&version);
        return Some((
            PromptResolution {
                rollout_id: Some(rollout.id),
                variant,
                config_id,
                version_id,
                output_schema,
                output_failure_action,
                allowed_tools,
            },
            version,
        ));
    }

    // No active rollout - use active version
    let version = store.get_version_config(active_version_id).await?;
    let (output_schema, output_failure_action) = extract_output_contract(&version);
    let allowed_tools = extract_allowed_tools(&version);
    Some((
        PromptResolution {
            rollout_id: None,
            variant: RolloutVariant::Baseline,
            config_id,
            version_id: active_version_id,
            output_schema,
            output_failure_action,
            allowed_tools,
        },
        version,
    ))
}

/// Resolve which prompt version to use for this request.
///
/// Checks for an explicit config name from the header (`X-Reiver-Prompt-Config`)
/// or body (`prompt_config` field), honouring whichever is present.
///
/// Returns `None` if no prompt config is specified — the request proceeds with
/// its own messages as-is.
#[tracing::instrument(
    name = "gateway.resolve_prompt",
    skip(store, headers),
    fields(project_id = %project_id)
)]
pub async fn resolve_prompt_config(
    store: &dyn PromptStore,
    project_id: Uuid,
    headers: &HeaderMap,
    body_config_name: Option<&str>,
) -> Option<(PromptResolution, PromptVersionConfig)> {
    resolve_explicit_config(store, project_id, headers, body_config_name).await
}

/// Extract template variables from request headers.
///
/// Variables are passed via the `X-Reiver-Var-{name}` header pattern.
/// For example: `X-Reiver-Var-User-Name: Alice` sets variable `user_name` to "Alice"
///
/// Variable names in headers use kebab-case which is converted to snake_case for templates.
fn extract_template_variables(headers: &HeaderMap) -> HashMap<String, Value> {
    let mut variables = HashMap::new();
    const VAR_PREFIX: &str = "x-reiver-var-";

    for (name, value) in headers.iter() {
        let header_name = name.as_str().to_lowercase();
        if let Some(var_name) = header_name.strip_prefix(VAR_PREFIX) {
            // Convert kebab-case to snake_case for template compatibility
            let var_name = var_name.replace('-', "_");

            // Validate header value length
            if let Ok(value_str) = value.to_str() {
                if value_str.len() <= MAX_HEADER_VALUE_LENGTH {
                    // Try to parse as JSON for typed values, fall back to string
                    let json_value = serde_json::from_str(value_str)
                        .unwrap_or_else(|_| Value::String(value_str.to_string()));
                    variables.insert(var_name, json_value);
                }
            }
        }
    }

    variables
}

/// Apply prompt version configuration to a chat completion request.
///
// ---------------------------------------------------------------------------
// Variable schema validation
// ---------------------------------------------------------------------------

/// Validate `variables` map against the variable schema defined in `config.variables`.
///
/// Returns `Ok(())` if all constraints pass.
/// Returns `Err((variable_name, human_readable_reason))` on the first failure.
///
/// As a side effect, default values are injected into `variables` for absent
/// optional variables that have a `default` defined.
fn validate_enum_var(name: &str, val: &Value, allowed: &[String]) -> Result<(), (String, String)> {
    let val_str = match val {
        Value::String(s) => s.as_str().to_string(),
        other => other.to_string(),
    };
    let lower = val_str.to_lowercase();
    if allowed.iter().any(|a| a.to_lowercase() == lower) {
        return Ok(());
    }
    Err((
        name.to_string(),
        format!(
            "value '{}' is not one of the allowed values: {}",
            val_str,
            allowed.join(", ")
        ),
    ))
}

fn validate_string_var(name: &str, val: &Value, max_chars: usize) -> Result<(), (String, String)> {
    if let serde_json::Value::String(s) = val {
        let len = s.chars().count();
        if len > max_chars {
            return Err((
                name.to_string(),
                format!(
                    "string length {} exceeds the maximum of {} characters",
                    len, max_chars
                ),
            ));
        }
    }
    Ok(())
}

fn validate_number_var(
    name: &str,
    val: &Value,
    min: Option<f64>,
    max: Option<f64>,
) -> Result<(), (String, String)> {
    let num = match val {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    };
    let n = match num {
        Some(n) if n.is_nan() || n.is_infinite() => {
            return Err((
                name.to_string(),
                format!("value is not a finite number: {}", val),
            ));
        }
        Some(n) => n,
        None => return Ok(()),
    };
    if let Some(lo) = min {
        if n < lo {
            return Err((
                name.to_string(),
                format!("value {} is below the minimum of {}", n, lo),
            ));
        }
    }
    if let Some(hi) = max {
        if n > hi {
            return Err((
                name.to_string(),
                format!("value {} exceeds the maximum of {}", n, hi),
            ));
        }
    }
    Ok(())
}

fn validate_prompt_variables(
    config: &PromptVersionConfig,
    variables: &mut HashMap<String, Value>,
) -> Result<(), (String, String)> {
    let definitions: Vec<VariableDefinition> = match &config.variables {
        Value::Array(arr) if !arr.is_empty() => {
            match serde_json::from_value(config.variables.clone()) {
                Ok(defs) => defs,
                Err(_) => return Ok(()), // malformed schema → skip
            }
        }
        _ => return Ok(()),
    };

    for def in &definitions {
        let Some(val) = variables.get(&def.name) else {
            if let Some(ref default_val) = def.default {
                variables.insert(def.name.clone(), default_val.clone());
            } else if def.required {
                return Err((
                    def.name.clone(),
                    "required variable is missing and has no default".to_string(),
                ));
            }
            continue;
        };

        match def.var_type.as_str() {
            "enum" => {
                if let Some(ref allowed) = def.values {
                    validate_enum_var(&def.name, val, allowed)?;
                }
            }
            "string" => {
                if let Some(max_chars) = def.max_chars {
                    validate_string_var(&def.name, val, max_chars)?;
                }
            }
            "number" => validate_number_var(&def.name, val, def.min, def.max)?,
            _ => {} // "boolean" and unknown types: no constraint
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------

/// This modifies the request in-place:
/// - Validates prompt_variables against the schema in `config.variables`
/// - Compiles and injects/prepends system prompt with template variables
/// - Overrides model (if specified)
/// - Overrides temperature (if specified)
/// - Overrides max_tokens (if specified)
/// - Applies tools/functions (if specified)
/// - Applies response_format (if specified)
///
/// Template variables come from two sources, merged in priority order:
/// 1. `X-Reiver-Var-*` headers (highest priority — more explicit, set at the edge)
/// 2. `request.prompt_variables` body field (fills in any key not present in headers)
///
/// Returns `Err(GatewayError::PromptVariableValidation)` if any variable fails the
/// schema constraint defined in `config.variables`. The request is not modified on error.
pub fn apply_prompt_config(
    request: &mut ChatCompletionRequest,
    config: &PromptVersionConfig,
    headers: &HeaderMap,
) -> Result<(), crate::gateway::error::GatewayError> {
    // Header variables take precedence; body variables fill the rest.
    let mut variables = extract_template_variables(headers);
    if let Some(ref body_vars) = request.prompt_variables {
        for (k, v) in body_vars {
            variables.entry(k.clone()).or_insert_with(|| v.clone());
        }
    }

    // Validate variables against schema (also injects defaults).
    if let Err((variable, detail)) = validate_prompt_variables(config, &mut variables) {
        return Err(
            crate::gateway::error::GatewayError::PromptVariableValidation { variable, detail },
        );
    }

    // Apply system prompt with template compilation.
    // Skip injection when the messages already contain a system message
    // (e.g. multi-turn agent loops that re-send prompt_config for model/
    // temperature settings but already carry the system prompt in context).
    let already_has_system = request
        .messages
        .first()
        .is_some_and(|m| m.role == MessageRole::System);

    if !already_has_system {
        if let Some(ref system_prompt) = config.system_prompt {
            let compiled_prompt = if !variables.is_empty() {
                match compile_prompt(system_prompt, &variables) {
                    Ok(compiled) => compiled,
                    Err(e) => {
                        return Err(
                            crate::gateway::error::GatewayError::PromptVariableValidation {
                                variable: "_template".to_string(),
                                detail: format!("Template compilation failed: {}", e),
                            },
                        );
                    }
                }
            } else {
                system_prompt.clone()
            };
            inject_system_prompt(request, &compiled_prompt);
        }
    }

    // Apply model override (skip empty strings which are equivalent to None)
    if let Some(ref model) = config.model {
        if !model.trim().is_empty() {
            request.model = model.clone();
        }
    }

    request.temperature = Some(config.temperature.to_f32().unwrap_or(0.5));

    // Apply max_tokens override (skip invalid negative values from DB)
    if let Some(max_tokens) = config.max_tokens {
        if let Ok(valid) = u32::try_from(max_tokens) {
            if valid > 0 {
                request.max_tokens = Some(valid);
            }
        }
    }

    // Apply tools/functions if specified in config and request doesn't already have tools
    if request.tools.is_none() {
        if let Some(ref tools_json) = config.tools {
            if let Ok(tools) = serde_json::from_value(tools_json.clone()) {
                request.tools = Some(tools);
            }
        }
    }

    // Enforce tool whitelist: strip tools not in the allowed list.
    // NULL allowed_tools = no restriction; empty array = no tools allowed.
    if let Some(ref allowed_json) = config.allowed_tools {
        if let Ok(allowed) = serde_json::from_value::<Vec<String>>(allowed_json.clone()) {
            if let Some(ref mut tools) = request.tools {
                tools.retain(|t| allowed.iter().any(|a| a == &t.function.name));
            }
            // Also clear tool_choice if no tools remain
            if request.tools.as_ref().map_or(true, |t| t.is_empty()) {
                request.tool_choice = None;
            }
        }
    }

    // Apply response_format if specified in config and request doesn't already have one
    if request.response_format.is_none() {
        if let Some(ref format_json) = config.response_format {
            if let Ok(response_format) = serde_json::from_value(format_json.clone()) {
                request.response_format = Some(response_format);
            }
        }
    }

    Ok(())
}

/// Inject a system prompt into the request.
///
/// Behavior:
/// - If the first message is already a system message, prepend the new content
/// - Otherwise, insert a new system message at the start
fn inject_system_prompt(request: &mut ChatCompletionRequest, system_prompt: &str) {
    if let Some(first) = request.messages.first_mut() {
        if first.role == MessageRole::System {
            match first.content {
                Some(MessageContent::Text(ref existing)) => {
                    first.content = Some(MessageContent::Text(format!(
                        "{}\n\n{}",
                        system_prompt, existing
                    )));
                }
                Some(MessageContent::Parts(ref mut parts)) => {
                    parts.insert(
                        0,
                        ContentPart::Text {
                            text: system_prompt.to_string(),
                        },
                    );
                }
                None => {
                    first.content = Some(MessageContent::Text(system_prompt.to_string()));
                }
            }
        } else {
            // Insert new system message at start
            request.messages.insert(
                0,
                ChatMessage {
                    role: MessageRole::System,
                    content: Some(MessageContent::Text(system_prompt.to_string())),
                    name: None,
                    tool_calls: None,
                    tool_call_id: None,
                    reasoning_content: None,
                },
            );
        }
    } else {
        // No messages at all, add system message
        request.messages.push(ChatMessage {
            role: MessageRole::System,
            content: Some(MessageContent::Text(system_prompt.to_string())),
            name: None,
            tool_calls: None,
            tool_call_id: None,
            reasoning_content: None,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inject_system_prompt_new() {
        let mut request = ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text("Hello".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            }],
            prompt_variables: None,
            models: None,
            provider: None,
            ..Default::default()
        };

        inject_system_prompt(&mut request, "You are helpful.");

        assert_eq!(request.messages.len(), 2);
        assert_eq!(request.messages[0].role, MessageRole::System);
        assert_eq!(
            request.messages[0].content,
            Some(MessageContent::Text("You are helpful.".to_string()))
        );
    }

    #[test]
    fn test_inject_system_prompt_prepend() {
        let mut request = ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![
                ChatMessage {
                    role: MessageRole::System,
                    content: Some(MessageContent::Text("Existing system prompt.".to_string())),
                    name: None,
                    tool_calls: None,
                    tool_call_id: None,
                    reasoning_content: None,
                },
                ChatMessage {
                    role: MessageRole::User,
                    content: Some(MessageContent::Text("Hello".to_string())),
                    name: None,
                    tool_calls: None,
                    tool_call_id: None,
                    reasoning_content: None,
                },
            ],
            prompt_variables: None,
            models: None,
            provider: None,
            ..Default::default()
        };

        inject_system_prompt(&mut request, "New prompt.");

        assert_eq!(request.messages.len(), 2);
        assert_eq!(
            request.messages[0].content,
            Some(MessageContent::Text(
                "New prompt.\n\nExisting system prompt.".to_string()
            ))
        );
    }

    /// Regression: when the existing system message had `MessageContent::Parts`
    /// (multimodal content like images), `inject_system_prompt` silently
    /// replaced the entire content with just the new prompt string, losing
    /// the original parts.
    #[test]
    fn test_inject_system_prompt_preserves_multipart_content() {
        use crate::gateway::types::{ContentPart, ImageUrl};

        let existing_parts = vec![
            ContentPart::Text {
                text: "Existing instructions.".to_string(),
            },
            ContentPart::ImageUrl {
                image_url: ImageUrl {
                    url: "data:image/png;base64,abc".to_string(),
                    detail: None,
                },
            },
        ];

        let mut request = ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![
                ChatMessage {
                    role: MessageRole::System,
                    content: Some(MessageContent::Parts(existing_parts)),
                    name: None,
                    tool_calls: None,
                    tool_call_id: None,
                    reasoning_content: None,
                },
                ChatMessage {
                    role: MessageRole::User,
                    content: Some(MessageContent::Text("Hello".to_string())),
                    name: None,
                    tool_calls: None,
                    tool_call_id: None,
                    reasoning_content: None,
                },
            ],
            prompt_variables: None,
            models: None,
            provider: None,
            ..Default::default()
        };

        inject_system_prompt(&mut request, "New prompt.");

        assert_eq!(request.messages.len(), 2);
        match &request.messages[0].content {
            Some(MessageContent::Parts(parts)) => {
                assert_eq!(
                    parts.len(),
                    3,
                    "Original 2 parts + prepended text part = 3 total"
                );
                assert!(
                    matches!(&parts[0], ContentPart::Text { text } if text == "New prompt."),
                    "First part must be the prepended system prompt"
                );
                assert!(
                    matches!(&parts[1], ContentPart::Text { text } if text == "Existing instructions."),
                    "Second part must be the original text"
                );
                assert!(
                    matches!(&parts[2], ContentPart::ImageUrl { .. }),
                    "Third part must be the original image"
                );
            }
            other => panic!(
                "Expected MessageContent::Parts after inject, got: {:?}",
                other
            ),
        }
    }

    #[test]
    fn test_extract_template_variables() {
        use axum::http::{HeaderName, HeaderValue};

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-reiver-var-user-name"),
            HeaderValue::from_static("Alice"),
        );
        headers.insert(
            HeaderName::from_static("x-reiver-var-user-role"),
            HeaderValue::from_static("developer"),
        );
        headers.insert(
            HeaderName::from_static("x-reiver-var-count"),
            HeaderValue::from_static("42"),
        );
        // Non-variable header should be ignored
        headers.insert(
            HeaderName::from_static("authorization"),
            HeaderValue::from_static("Bearer token"),
        );

        let vars = extract_template_variables(&headers);

        assert_eq!(vars.len(), 3);
        assert_eq!(
            vars.get("user_name"),
            Some(&Value::String("Alice".to_string()))
        );
        assert_eq!(
            vars.get("user_role"),
            Some(&Value::String("developer".to_string()))
        );
        // Numeric string should be parsed as number
        assert_eq!(vars.get("count"), Some(&Value::Number(42.into())));
    }

    /// Regression: `"NaN"` parsed as f64::NAN bypassed all bounds checks because
    /// `NaN < min` is always false in IEEE 754 comparisons.
    #[test]
    fn test_validate_number_var_rejects_nan() {
        let result = super::validate_number_var(
            "score",
            &serde_json::Value::String("NaN".to_string()),
            Some(0.0),
            Some(100.0),
        );
        assert!(result.is_err(), "NaN must be rejected by number validation");
        let (name, detail) = result.unwrap_err();
        assert_eq!(name, "score");
        assert!(detail.contains("not a finite number"), "detail: {}", detail);
    }

    /// Regression: `"Infinity"` parsed as f64::INFINITY bypassed the max
    /// bound check when no explicit max was set.
    #[test]
    fn test_validate_number_var_rejects_infinity() {
        let result = super::validate_number_var(
            "count",
            &serde_json::Value::String("Infinity".to_string()),
            Some(0.0),
            None,
        );
        assert!(
            result.is_err(),
            "Infinity must be rejected by number validation"
        );
    }

    #[test]
    fn test_validate_number_var_rejects_negative_infinity() {
        let result = super::validate_number_var(
            "count",
            &serde_json::Value::String("-Infinity".to_string()),
            None,
            Some(100.0),
        );
        assert!(
            result.is_err(),
            "-Infinity must be rejected by number validation"
        );
    }

    /// Regression: `config.max_tokens = -1` (a sentinel or bad DB value) was cast
    /// with `as u32`, wrapping to 4294967295 and effectively disabling the limit.
    /// The fix uses `u32::try_from` and skips negative/zero values.
    #[test]
    fn test_apply_prompt_config_negative_max_tokens_ignored() {
        let mut request = ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text("Hello".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            }],
            prompt_variables: None,
            models: None,
            provider: None,
            ..Default::default()
        };

        let config = PromptVersionConfig {
            id: Uuid::new_v4(),
            system_prompt: None,
            model: None,
            temperature: Decimal::new(5, 1),
            max_tokens: Some(-1),
            variables: serde_json::json!([]),
            tools: None,
            response_format: None,
            parameters: serde_json::Value::Null,
            allowed_tools: None,
        };

        let headers = HeaderMap::new();
        apply_prompt_config(&mut request, &config, &headers).expect("should not fail");

        assert_eq!(
            request.max_tokens, None,
            "Negative max_tokens must not wrap to u32::MAX"
        );
    }

    /// `type` in JSON must populate `var_type` so `max_chars` applies (regression for dual-struct serde bug).
    #[test]
    fn test_apply_prompt_config_enforces_max_chars_with_type_key() {
        let mut request = ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text("Hello".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            }],
            prompt_variables: Some({
                let mut m = std::collections::HashMap::new();
                m.insert(
                    "topic".to_string(),
                    serde_json::Value::String("abcdefghij".to_string()),
                );
                m
            }),
            models: None,
            provider: None,
            ..Default::default()
        };

        let config = PromptVersionConfig {
            id: Uuid::new_v4(),
            system_prompt: Some("Talk about {{topic}}.".to_string()),
            model: None,
            temperature: Decimal::new(5, 1),
            max_tokens: None,
            variables: serde_json::json!([{
                "name": "topic",
                "type": "string",
                "required": true,
                "max_chars": 3
            }]),
            tools: None,
            response_format: None,
            parameters: serde_json::Value::Null,
            allowed_tools: None,
        };

        let headers = HeaderMap::new();
        let err = apply_prompt_config(&mut request, &config, &headers).unwrap_err();
        let crate::gateway::error::GatewayError::PromptVariableValidation { variable, detail } =
            err
        else {
            panic!("expected PromptVariableValidation, got {:?}", err);
        };
        assert_eq!(variable, "topic");
        assert!(detail.contains("exceeds the maximum"), "detail: {}", detail);
    }

    #[test]
    fn test_apply_prompt_config_enforces_max_chars_with_var_type_alias() {
        let mut request = ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text("Hello".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            }],
            prompt_variables: Some({
                let mut m = std::collections::HashMap::new();
                m.insert(
                    "topic".to_string(),
                    serde_json::Value::String("abcdefghij".to_string()),
                );
                m
            }),
            models: None,
            provider: None,
            ..Default::default()
        };

        let config = PromptVersionConfig {
            id: Uuid::new_v4(),
            system_prompt: Some("Talk about {{topic}}.".to_string()),
            model: None,
            temperature: Decimal::new(5, 1),
            max_tokens: None,
            variables: serde_json::json!([{
                "name": "topic",
                "var_type": "string",
                "required": true,
                "max_chars": 3
            }]),
            tools: None,
            response_format: None,
            parameters: serde_json::Value::Null,
            allowed_tools: None,
        };

        let headers = HeaderMap::new();
        let err = apply_prompt_config(&mut request, &config, &headers).unwrap_err();
        let crate::gateway::error::GatewayError::PromptVariableValidation { variable, .. } = err
        else {
            panic!("expected PromptVariableValidation");
        };
        assert_eq!(variable, "topic");
    }

    /// Zero max_tokens should also be skipped — it's not a meaningful limit.
    #[test]
    fn test_apply_prompt_config_zero_max_tokens_ignored() {
        let mut request = ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text("Hello".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            }],
            prompt_variables: None,
            models: None,
            provider: None,
            ..Default::default()
        };

        let config = PromptVersionConfig {
            id: Uuid::new_v4(),
            system_prompt: None,
            model: None,
            temperature: Decimal::new(5, 1),
            max_tokens: Some(0),
            variables: serde_json::json!([]),
            tools: None,
            response_format: None,
            parameters: serde_json::Value::Null,
            allowed_tools: None,
        };

        let headers = HeaderMap::new();
        apply_prompt_config(&mut request, &config, &headers).expect("should not fail");

        assert_eq!(request.max_tokens, None, "Zero max_tokens must be skipped");
    }

    #[test]
    fn test_apply_prompt_config_with_template() {
        let mut request = ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text("Hello".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            }],
            prompt_variables: None,
            models: None,
            provider: None,
            ..Default::default()
        };

        let config = PromptVersionConfig {
            id: Uuid::new_v4(),
            system_prompt: Some("Hello {{user_name}}, you are a {{role}}.".to_string()),
            model: None,
            temperature: Decimal::new(5, 1),
            max_tokens: None,
            variables: serde_json::json!([]),
            tools: None,
            response_format: None,
            parameters: serde_json::Value::Null,
            allowed_tools: None,
        };

        use axum::http::{HeaderName, HeaderValue};
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-reiver-var-user-name"),
            HeaderValue::from_static("Alice"),
        );
        headers.insert(
            HeaderName::from_static("x-reiver-var-role"),
            HeaderValue::from_static("assistant"),
        );

        apply_prompt_config(&mut request, &config, &headers).expect("apply_prompt_config failed");

        assert_eq!(request.messages.len(), 2);
        assert_eq!(
            request.messages[0].content,
            Some(MessageContent::Text(
                "Hello Alice, you are a assistant.".to_string()
            ))
        );
    }

    fn make_rollout(weight: i32, alloc: AllocationType) -> ActiveRollout {
        ActiveRollout {
            id: Uuid::new_v4(),
            config_id: Uuid::new_v4(),
            target_version_id: Uuid::new_v4(),
            baseline_version_id: Some(Uuid::new_v4()),
            current_weight: weight,
            allocation_type: alloc,
        }
    }

    fn headers_with(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                axum::http::header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                v.parse().unwrap(),
            );
        }
        h
    }

    #[test]
    fn test_assign_variant_weight_zero() {
        let rollout = make_rollout(0, AllocationType::Random);
        let headers = HeaderMap::new();
        for _ in 0..100 {
            assert_eq!(assign_variant(&rollout, &headers), RolloutVariant::Baseline);
        }
    }

    #[test]
    fn test_assign_variant_weight_100() {
        let rollout = make_rollout(100, AllocationType::Random);
        let headers = HeaderMap::new();
        for _ in 0..100 {
            assert_eq!(assign_variant(&rollout, &headers), RolloutVariant::Target);
        }
    }

    #[test]
    fn test_assign_variant_random_distribution() {
        let rollout = make_rollout(50, AllocationType::Random);
        let headers = HeaderMap::new();
        let target_count = (0..1000)
            .filter(|_| assign_variant(&rollout, &headers) == RolloutVariant::Target)
            .count();
        assert!(
            (350..=650).contains(&target_count),
            "Expected ~500 target out of 1000, got {target_count}"
        );
    }

    #[test]
    fn test_assign_variant_user_sticky_deterministic() {
        let rollout = make_rollout(50, AllocationType::UserSticky);
        let headers = headers_with(&[("x-reiver-user-id", "user-abc-123")]);
        let first = assign_variant(&rollout, &headers);
        for _ in 0..50 {
            assert_eq!(
                assign_variant(&rollout, &headers),
                first,
                "Sticky allocation must be deterministic for the same user"
            );
        }
    }

    #[test]
    fn test_assign_variant_session_sticky_deterministic() {
        let rollout = make_rollout(50, AllocationType::SessionSticky);
        let headers = headers_with(&[("x-reiver-session-id", "sess-xyz-789")]);
        let first = assign_variant(&rollout, &headers);
        for _ in 0..50 {
            assert_eq!(
                assign_variant(&rollout, &headers),
                first,
                "Sticky allocation must be deterministic for the same session"
            );
        }
    }

    #[test]
    fn test_assign_variant_sticky_different_rollouts() {
        let headers = headers_with(&[("x-reiver-user-id", "user-test")]);
        let mut saw_different = false;
        for _ in 0..20 {
            let r1 = make_rollout(50, AllocationType::UserSticky);
            let r2 = make_rollout(50, AllocationType::UserSticky);
            if assign_variant(&r1, &headers) != assign_variant(&r2, &headers) {
                saw_different = true;
                break;
            }
        }
        assert!(
            saw_different,
            "Different rollout IDs should (usually) produce different assignments"
        );
    }

    #[test]
    fn test_stable_hash_deterministic() {
        let id = Uuid::new_v4();
        let h1 = stable_hash("test-key", &id);
        let h2 = stable_hash("test-key", &id);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_stable_hash_varies_with_key() {
        let id = Uuid::new_v4();
        let h1 = stable_hash("user-a", &id);
        let h2 = stable_hash("user-b", &id);
        assert_ne!(h1, h2);
    }

    // -----------------------------------------------------------------------
    // resolve_prompt_config tests (uses InMemoryPromptStore)
    // -----------------------------------------------------------------------

    use crate::gateway::prompt_store::{InMemoryPromptStore, PromptConfigRow};

    fn version_config(id: Uuid) -> PromptVersionConfig {
        PromptVersionConfig {
            id,
            system_prompt: Some("You are a helpful assistant.".to_string()),
            model: Some("gpt-4o".to_string()),
            temperature: Decimal::new(7, 1),
            max_tokens: Some(1024),
            variables: serde_json::json!([]),
            tools: None,
            response_format: None,
            parameters: serde_json::Value::Null,
            allowed_tools: None,
        }
    }

    fn seed_store(
        project_id: Uuid,
        config_name: &str,
    ) -> (InMemoryPromptStore, Uuid, Uuid) {
        let config_id = Uuid::new_v4();
        let version_id = Uuid::new_v4();
        let mut store = InMemoryPromptStore::new();
        store.add_config(
            project_id,
            config_name,
            PromptConfigRow {
                id: config_id,
                active_version_id: Some(version_id),
            },
        );
        store.add_version(version_config(version_id));
        (store, config_id, version_id)
    }

    // -- Config lookup --

    #[tokio::test]
    async fn resolve_returns_none_when_no_config_name() {
        let store = InMemoryPromptStore::new();
        let headers = HeaderMap::new();
        let result = super::resolve_prompt_config(&store, Uuid::new_v4(), &headers, None).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn resolve_returns_none_when_config_not_found() {
        let store = InMemoryPromptStore::new();
        let headers = HeaderMap::new();
        let result = super::resolve_prompt_config(
            &store,
            Uuid::new_v4(),
            &headers,
            Some("nonexistent"),
        )
        .await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn resolve_wrong_project_returns_none() {
        let project_a = Uuid::new_v4();
        let project_b = Uuid::new_v4();
        let (store, _, _) = seed_store(project_a, "my-config");
        let headers = HeaderMap::new();

        let result = super::resolve_prompt_config(
            &store,
            project_b,
            &headers,
            Some("my-config"),
        )
        .await;
        assert!(result.is_none(), "Config in project A must not resolve for project B");
    }

    #[tokio::test]
    async fn resolve_found_config_returns_baseline_without_rollout() {
        let project_id = Uuid::new_v4();
        let (store, config_id, version_id) = seed_store(project_id, "my-config");
        let headers = HeaderMap::new();

        let (resolution, version) = super::resolve_prompt_config(
            &store,
            project_id,
            &headers,
            Some("my-config"),
        )
        .await
        .expect("should resolve");

        assert_eq!(resolution.config_id, config_id);
        assert_eq!(resolution.version_id, version_id);
        assert_eq!(resolution.variant, RolloutVariant::Baseline);
        assert!(resolution.rollout_id.is_none());
        assert_eq!(version.model.as_deref(), Some("gpt-4o"));
    }

    #[tokio::test]
    async fn resolve_returns_none_when_no_active_version() {
        let project_id = Uuid::new_v4();
        let config_id = Uuid::new_v4();
        let mut store = InMemoryPromptStore::new();
        store.add_config(
            project_id,
            "draft-config",
            PromptConfigRow {
                id: config_id,
                active_version_id: None,
            },
        );
        let headers = HeaderMap::new();

        let result = super::resolve_prompt_config(
            &store,
            project_id,
            &headers,
            Some("draft-config"),
        )
        .await;
        assert!(result.is_none(), "Config with no active version must return None");
    }

    // -- Header vs body config name --

    #[tokio::test]
    async fn resolve_header_takes_precedence_over_body() {
        let project_id = Uuid::new_v4();
        let mut store = InMemoryPromptStore::new();

        let header_config_id = Uuid::new_v4();
        let header_version_id = Uuid::new_v4();
        store.add_config(
            project_id,
            "header-config",
            PromptConfigRow {
                id: header_config_id,
                active_version_id: Some(header_version_id),
            },
        );
        store.add_version(version_config(header_version_id));

        let body_config_id = Uuid::new_v4();
        let body_version_id = Uuid::new_v4();
        store.add_config(
            project_id,
            "body-config",
            PromptConfigRow {
                id: body_config_id,
                active_version_id: Some(body_version_id),
            },
        );
        store.add_version(version_config(body_version_id));

        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::HeaderName::from_static("x-reiver-prompt-config"),
            "header-config".parse().unwrap(),
        );

        let (resolution, _) = super::resolve_prompt_config(
            &store,
            project_id,
            &headers,
            Some("body-config"),
        )
        .await
        .expect("should resolve");

        assert_eq!(resolution.config_id, header_config_id);
    }

    #[tokio::test]
    async fn resolve_uses_body_when_no_header() {
        let project_id = Uuid::new_v4();
        let (store, config_id, _) = seed_store(project_id, "body-only");
        let headers = HeaderMap::new();

        let (resolution, _) = super::resolve_prompt_config(
            &store,
            project_id,
            &headers,
            Some("body-only"),
        )
        .await
        .expect("should resolve");
        assert_eq!(resolution.config_id, config_id);
    }

    // -- Rollout resolution --

    #[tokio::test]
    async fn resolve_with_rollout_weight_100_returns_target() {
        let project_id = Uuid::new_v4();
        let config_id = Uuid::new_v4();
        let baseline_version_id = Uuid::new_v4();
        let target_version_id = Uuid::new_v4();
        let rollout_id = Uuid::new_v4();

        let mut store = InMemoryPromptStore::new();
        store.add_config(
            project_id,
            "rollout-config",
            PromptConfigRow {
                id: config_id,
                active_version_id: Some(baseline_version_id),
            },
        );
        store.add_version(version_config(baseline_version_id));

        let mut target_version = version_config(target_version_id);
        target_version.model = Some("claude-3-5-sonnet".to_string());
        store.add_version(target_version);

        store.add_rollout(
            config_id,
            ActiveRollout {
                id: rollout_id,
                config_id,
                target_version_id,
                baseline_version_id: Some(baseline_version_id),
                current_weight: 100,
                allocation_type: AllocationType::Random,
            },
        );

        let headers = HeaderMap::new();
        let (resolution, version) = super::resolve_prompt_config(
            &store,
            project_id,
            &headers,
            Some("rollout-config"),
        )
        .await
        .expect("should resolve");

        assert_eq!(resolution.rollout_id, Some(rollout_id));
        assert_eq!(resolution.variant, RolloutVariant::Target);
        assert_eq!(resolution.version_id, target_version_id);
        assert_eq!(version.model.as_deref(), Some("claude-3-5-sonnet"));
    }

    #[tokio::test]
    async fn resolve_with_rollout_weight_0_returns_baseline() {
        let project_id = Uuid::new_v4();
        let config_id = Uuid::new_v4();
        let baseline_version_id = Uuid::new_v4();
        let target_version_id = Uuid::new_v4();
        let rollout_id = Uuid::new_v4();

        let mut store = InMemoryPromptStore::new();
        store.add_config(
            project_id,
            "rollout-zero",
            PromptConfigRow {
                id: config_id,
                active_version_id: Some(baseline_version_id),
            },
        );
        store.add_version(version_config(baseline_version_id));
        store.add_version(version_config(target_version_id));

        store.add_rollout(
            config_id,
            ActiveRollout {
                id: rollout_id,
                config_id,
                target_version_id,
                baseline_version_id: Some(baseline_version_id),
                current_weight: 0,
                allocation_type: AllocationType::Random,
            },
        );

        let headers = HeaderMap::new();
        let (resolution, _) = super::resolve_prompt_config(
            &store,
            project_id,
            &headers,
            Some("rollout-zero"),
        )
        .await
        .expect("should resolve");

        assert_eq!(resolution.variant, RolloutVariant::Baseline);
        assert_eq!(resolution.version_id, baseline_version_id);
    }

    #[tokio::test]
    async fn resolve_no_baseline_version_falls_back_to_active() {
        let project_id = Uuid::new_v4();
        let config_id = Uuid::new_v4();
        let active_version_id = Uuid::new_v4();
        let target_version_id = Uuid::new_v4();

        let mut store = InMemoryPromptStore::new();
        store.add_config(
            project_id,
            "no-baseline",
            PromptConfigRow {
                id: config_id,
                active_version_id: Some(active_version_id),
            },
        );
        store.add_version(version_config(active_version_id));
        store.add_version(version_config(target_version_id));

        store.add_rollout(
            config_id,
            ActiveRollout {
                id: Uuid::new_v4(),
                config_id,
                target_version_id,
                baseline_version_id: None,
                current_weight: 0,
                allocation_type: AllocationType::Random,
            },
        );

        let headers = HeaderMap::new();
        let (resolution, _) = super::resolve_prompt_config(
            &store,
            project_id,
            &headers,
            Some("no-baseline"),
        )
        .await
        .expect("should resolve");

        assert_eq!(resolution.variant, RolloutVariant::Baseline);
        assert_eq!(
            resolution.version_id, active_version_id,
            "With no baseline_version_id, should fall back to active_version_id"
        );
    }

    // -- Force-variant header --

    #[tokio::test]
    async fn resolve_force_variant_target() {
        let project_id = Uuid::new_v4();
        let config_id = Uuid::new_v4();
        let baseline_version_id = Uuid::new_v4();
        let target_version_id = Uuid::new_v4();
        let rollout_id = Uuid::new_v4();

        let mut store = InMemoryPromptStore::new();
        store.add_config(
            project_id,
            "force-test",
            PromptConfigRow {
                id: config_id,
                active_version_id: Some(baseline_version_id),
            },
        );
        store.add_version(version_config(baseline_version_id));
        store.add_version(version_config(target_version_id));

        store.add_rollout(
            config_id,
            ActiveRollout {
                id: rollout_id,
                config_id,
                target_version_id,
                baseline_version_id: Some(baseline_version_id),
                current_weight: 0, // normally would get baseline
                allocation_type: AllocationType::Random,
            },
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::HeaderName::from_static("x-reiver-force-variant"),
            "target".parse().unwrap(),
        );

        let (resolution, _) = super::resolve_prompt_config(
            &store,
            project_id,
            &headers,
            Some("force-test"),
        )
        .await
        .expect("should resolve");

        assert_eq!(resolution.variant, RolloutVariant::Target);
        assert_eq!(resolution.version_id, target_version_id);
    }

    #[tokio::test]
    async fn resolve_force_variant_baseline() {
        let project_id = Uuid::new_v4();
        let config_id = Uuid::new_v4();
        let baseline_version_id = Uuid::new_v4();
        let target_version_id = Uuid::new_v4();
        let rollout_id = Uuid::new_v4();

        let mut store = InMemoryPromptStore::new();
        store.add_config(
            project_id,
            "force-baseline",
            PromptConfigRow {
                id: config_id,
                active_version_id: Some(baseline_version_id),
            },
        );
        store.add_version(version_config(baseline_version_id));
        store.add_version(version_config(target_version_id));

        store.add_rollout(
            config_id,
            ActiveRollout {
                id: rollout_id,
                config_id,
                target_version_id,
                baseline_version_id: Some(baseline_version_id),
                current_weight: 100, // normally would get target
                allocation_type: AllocationType::Random,
            },
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::HeaderName::from_static("x-reiver-force-variant"),
            "baseline".parse().unwrap(),
        );

        let (resolution, _) = super::resolve_prompt_config(
            &store,
            project_id,
            &headers,
            Some("force-baseline"),
        )
        .await
        .expect("should resolve");

        assert_eq!(resolution.variant, RolloutVariant::Baseline);
        assert_eq!(resolution.version_id, baseline_version_id);
    }

    #[tokio::test]
    async fn resolve_force_variant_ignored_without_rollout() {
        let project_id = Uuid::new_v4();
        let (store, config_id, version_id) = seed_store(project_id, "no-rollout");

        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::HeaderName::from_static("x-reiver-force-variant"),
            "target".parse().unwrap(),
        );

        let (resolution, _) = super::resolve_prompt_config(
            &store,
            project_id,
            &headers,
            Some("no-rollout"),
        )
        .await
        .expect("should resolve with active version despite force header");

        assert_eq!(resolution.config_id, config_id);
        assert_eq!(resolution.version_id, version_id);
        assert_eq!(resolution.variant, RolloutVariant::Baseline);
    }

    // -- Output contract extraction --

    #[tokio::test]
    async fn resolve_extracts_output_schema_and_allowed_tools() {
        let project_id = Uuid::new_v4();
        let config_id = Uuid::new_v4();
        let version_id = Uuid::new_v4();

        let mut store = InMemoryPromptStore::new();
        store.add_config(
            project_id,
            "schema-test",
            PromptConfigRow {
                id: config_id,
                active_version_id: Some(version_id),
            },
        );

        let response_format = serde_json::json!({
            "type": "json_schema",
            "json_schema": {
                "name": "response",
                "schema": { "type": "object" }
            }
        });
        store.add_version(PromptVersionConfig {
            id: version_id,
            system_prompt: None,
            model: Some("gpt-4o".to_string()),
            temperature: Decimal::new(5, 1),
            max_tokens: None,
            variables: serde_json::json!([]),
            tools: None,
            response_format: Some(response_format.clone()),
            parameters: serde_json::json!({ "output_failure_action": "retry" }),
            allowed_tools: Some(serde_json::json!(["search", "calculate"])),
        });

        let headers = HeaderMap::new();
        let (resolution, _) = super::resolve_prompt_config(
            &store,
            project_id,
            &headers,
            Some("schema-test"),
        )
        .await
        .expect("should resolve");

        assert_eq!(resolution.output_schema, Some(response_format));
        assert_eq!(resolution.output_failure_action, OutputFailureAction::Retry);
        assert_eq!(
            resolution.allowed_tools,
            Some(vec!["search".to_string(), "calculate".to_string()])
        );
    }

    // -- Variable injection and validation through apply_prompt_config --

    #[tokio::test]
    async fn resolve_then_apply_compiles_template_with_body_variables() {
        let project_id = Uuid::new_v4();
        let config_id = Uuid::new_v4();
        let version_id = Uuid::new_v4();

        let mut store = InMemoryPromptStore::new();
        store.add_config(
            project_id,
            "template-test",
            PromptConfigRow {
                id: config_id,
                active_version_id: Some(version_id),
            },
        );
        store.add_version(PromptVersionConfig {
            id: version_id,
            system_prompt: Some("Hello {{name}}, your role is {{role}}.".to_string()),
            model: Some("gpt-4o".to_string()),
            temperature: Decimal::new(5, 1),
            max_tokens: None,
            variables: serde_json::json!([
                { "name": "name", "type": "string", "required": true },
                { "name": "role", "type": "string", "required": true }
            ]),
            tools: None,
            response_format: None,
            parameters: serde_json::Value::Null,
            allowed_tools: None,
        });

        let headers = HeaderMap::new();
        let (_, version) = super::resolve_prompt_config(
            &store,
            project_id,
            &headers,
            Some("template-test"),
        )
        .await
        .expect("should resolve");

        let mut request = ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text("Hi".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            }],
            prompt_variables: Some({
                let mut m = HashMap::new();
                m.insert("name".to_string(), Value::String("Alice".to_string()));
                m.insert("role".to_string(), Value::String("engineer".to_string()));
                m
            }),
            models: None,
            provider: None,
            ..Default::default()
        };

        apply_prompt_config(&mut request, &version, &headers).expect("apply should succeed");

        assert_eq!(request.messages.len(), 2);
        assert_eq!(
            request.messages[0].content,
            Some(MessageContent::Text(
                "Hello Alice, your role is engineer.".to_string()
            ))
        );
    }

    #[tokio::test]
    async fn resolve_then_apply_header_variables_take_precedence() {
        let project_id = Uuid::new_v4();
        let config_id = Uuid::new_v4();
        let version_id = Uuid::new_v4();

        let mut store = InMemoryPromptStore::new();
        store.add_config(
            project_id,
            "var-precedence",
            PromptConfigRow {
                id: config_id,
                active_version_id: Some(version_id),
            },
        );
        store.add_version(PromptVersionConfig {
            id: version_id,
            system_prompt: Some("Greeting: {{greeting}}".to_string()),
            model: None,
            temperature: Decimal::new(5, 1),
            max_tokens: None,
            variables: serde_json::json!([]),
            tools: None,
            response_format: None,
            parameters: serde_json::Value::Null,
            allowed_tools: None,
        });

        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::HeaderName::from_static("x-reiver-var-greeting"),
            "Header Hello".parse().unwrap(),
        );

        let (_, version) = super::resolve_prompt_config(
            &store,
            project_id,
            &headers,
            Some("var-precedence"),
        )
        .await
        .expect("should resolve");

        let mut request = ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: Some(MessageContent::Text("Hi".to_string())),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            }],
            prompt_variables: Some({
                let mut m = HashMap::new();
                m.insert("greeting".to_string(), Value::String("Body Hello".to_string()));
                m
            }),
            models: None,
            provider: None,
            ..Default::default()
        };

        apply_prompt_config(&mut request, &version, &headers).expect("apply should succeed");

        assert_eq!(
            request.messages[0].content,
            Some(MessageContent::Text("Greeting: Header Hello".to_string()))
        );
    }

    // -- System prompt injection modes --

    #[tokio::test]
    async fn resolve_then_apply_skips_system_prompt_when_already_present() {
        let project_id = Uuid::new_v4();
        let (store, _, _) = seed_store(project_id, "skip-sys");

        let headers = HeaderMap::new();
        let (_, version) = super::resolve_prompt_config(
            &store,
            project_id,
            &headers,
            Some("skip-sys"),
        )
        .await
        .expect("should resolve");

        let mut request = ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![
                ChatMessage {
                    role: MessageRole::System,
                    content: Some(MessageContent::Text("Existing system.".to_string())),
                    name: None,
                    tool_calls: None,
                    tool_call_id: None,
                    reasoning_content: None,
                },
                ChatMessage {
                    role: MessageRole::User,
                    content: Some(MessageContent::Text("Hi".to_string())),
                    name: None,
                    tool_calls: None,
                    tool_call_id: None,
                    reasoning_content: None,
                },
            ],
            prompt_variables: None,
            models: None,
            provider: None,
            ..Default::default()
        };

        apply_prompt_config(&mut request, &version, &headers).expect("apply should succeed");

        assert_eq!(request.messages.len(), 2, "should not add a third message");
        assert_eq!(
            request.messages[0].content,
            Some(MessageContent::Text("Existing system.".to_string())),
            "system prompt should be left unchanged when already present"
        );
    }

    // -- Rollout with sticky allocation through resolve --

    #[tokio::test]
    async fn resolve_user_sticky_is_deterministic_through_full_path() {
        let project_id = Uuid::new_v4();
        let config_id = Uuid::new_v4();
        let baseline_id = Uuid::new_v4();
        let target_id = Uuid::new_v4();

        let mut store = InMemoryPromptStore::new();
        store.add_config(
            project_id,
            "sticky-test",
            PromptConfigRow {
                id: config_id,
                active_version_id: Some(baseline_id),
            },
        );
        store.add_version(version_config(baseline_id));
        store.add_version(version_config(target_id));

        store.add_rollout(
            config_id,
            ActiveRollout {
                id: Uuid::new_v4(),
                config_id,
                target_version_id: target_id,
                baseline_version_id: Some(baseline_id),
                current_weight: 50,
                allocation_type: AllocationType::UserSticky,
            },
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::HeaderName::from_static("x-reiver-user-id"),
            "user-123".parse().unwrap(),
        );

        let first = super::resolve_prompt_config(
            &store,
            project_id,
            &headers,
            Some("sticky-test"),
        )
        .await
        .expect("should resolve")
        .0
        .variant;

        for _ in 0..20 {
            let variant = super::resolve_prompt_config(
                &store,
                project_id,
                &headers,
                Some("sticky-test"),
            )
            .await
            .expect("should resolve")
            .0
            .variant;
            assert_eq!(variant, first, "UserSticky must be deterministic");
        }
    }
}
