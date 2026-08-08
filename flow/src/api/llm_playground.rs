//! LLM Developer Playground API
//!
//! Provides a playground endpoint for testing prompts against multiple models
//! with optional auto-evaluation.

use axum::{extract::State, routing::post, Json, Router};
use futures::future::join_all;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tracing::warn;
use uuid::Uuid;

/// Timeout for individual model requests in compare mode (30 seconds)
const MODEL_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

use crate::app_state::FlowState;
use crate::error::{AppError, Result};
use crate::gateway::prompt_resolver::{resolve_and_compile_prompt, CompiledPrompt};
use crate::gateway::provider_types::Provider;
use crate::gateway::types::{
    ChatCompletionRequest, ChatMessage, ContentPart, MessageContent, MessageRole,
};

pub fn create_llm_playground_router() -> Router<Arc<FlowState>> {
    Router::new()
        .route("/", post(run_playground))
        .route("/compare", post(compare_models))
}

/// Request for playground execution
#[derive(Debug, Clone, Deserialize)]
pub struct PlaygroundRequest {
    pub project_id: Uuid,
    /// Model to use. Set to `"auto"` (or omit alongside `use_fallback_chain: true`)
    /// to route through the project's configured fallback chain, exactly as live
    /// traffic does. The actual model used is returned in the response.
    #[serde(default = "default_model")]
    pub model: String,
    pub messages: Vec<PlaygroundMessage>,
    pub temperature: Option<f32>,
    pub max_tokens: Option<u32>,
    /// Optional: run same prompt against multiple models
    pub compare_models: Option<Vec<String>>,
    /// Optional: run LLM-as-judge evaluation on response
    pub auto_evaluate: Option<bool>,
    /// When `true` (or when `model == "auto"`), route through the project's
    /// configured fallback chain instead of a specific model. This lets you
    /// test prompts on the same path that live API-key traffic takes.
    #[serde(default)]
    pub use_fallback_chain: bool,
    /// When `true`, ask the model to expose its internal reasoning process.
    /// The backend selects the correct vendor-specific parameters automatically
    /// based on whichever model is actually used (extended thinking for Anthropic,
    /// reasoning_effort for OpenAI o-series).
    #[serde(default)]
    pub enable_introspection: bool,
    /// Managed prompt config name. When set, the active version's system_prompt,
    /// model, temperature, max_tokens, tools, and response_format are resolved
    /// and applied — just like the gateway does for `/v1/chat/completions`.
    /// Template variables in the system prompt are compiled via Handlebars.
    #[serde(default)]
    pub prompt_config: Option<String>,
    /// Optional: test a specific version instead of the active one.
    /// Only used when `prompt_config` is set.
    #[serde(default)]
    pub prompt_version_id: Option<Uuid>,
    /// Runtime template variables for Handlebars substitution in the prompt's
    /// system_prompt. Only used when `prompt_config` is set.
    #[serde(default)]
    pub prompt_variables: Option<HashMap<String, serde_json::Value>>,
}

fn default_model() -> String {
    "auto".to_string()
}

/// Content for a single playground message — either plain text or an array of
/// content parts (text, images, or documents).
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum PlaygroundContent {
    Text(String),
    Parts(Vec<ContentPart>),
}

impl PlaygroundContent {
    /// Extract a plain-text representation for use in evaluation prompts.
    pub fn as_text(&self) -> String {
        match self {
            PlaygroundContent::Text(s) => s.clone(),
            PlaygroundContent::Parts(parts) => parts
                .iter()
                .filter_map(|p| {
                    if let ContentPart::Text { text } = p {
                        Some(text.as_str())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
                .join(""),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PlaygroundMessage {
    pub role: String,
    pub content: PlaygroundContent,
}

/// Response from playground
#[derive(Debug, Serialize)]
pub struct PlaygroundResponse {
    pub model: String,
    /// Provider that handled this request (e.g. `"openai"`, `"anthropic"`).
    pub provider: String,
    pub response: String,
    pub usage: PlaygroundUsage,
    pub latency_ms: u64,
    pub cost_usd: Decimal,
    pub evaluation: Option<PlaygroundEvaluation>,
    /// `true` when the fallback chain was used and a provider other than the
    /// primary handled the request.
    pub fallback_used: bool,
    /// When a `prompt_config` was used, the resolved version ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_version_id: Option<Uuid>,
    /// When a `prompt_config` was used, the resolved version number.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_version_number: Option<i32>,
}

#[derive(Debug, Serialize)]
pub struct PlaygroundUsage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
}

#[derive(Debug, Serialize)]
pub struct PlaygroundEvaluation {
    pub relevance: f64,
    pub coherence: f64,
    pub helpfulness: f64,
    pub summary: String,
}

/// Response from model comparison
#[derive(Debug, Serialize)]
pub struct CompareResponse {
    pub responses: Vec<PlaygroundResponse>,
    pub cost_comparison: Vec<CostBreakdown>,
    pub fastest_model: String,
    pub cheapest_model: String,
}

#[derive(Debug, Serialize)]
pub struct CostBreakdown {
    pub model: String,
    pub cost_usd: Decimal,
    pub tokens_per_dollar: u32,
}

/// Run a single playground request
async fn run_playground(
    State(state): State<Arc<FlowState>>,
    Json(mut req): Json<PlaygroundRequest>,
) -> Result<Json<PlaygroundResponse>> {
    // Resolve prompt config if specified (before routing decision, since it may override model)
    let compiled = resolve_prompt_for_playground(&state, &mut req).await?;

    let use_fallback = req.use_fallback_chain || req.model == "auto";

    if use_fallback {
        run_playground_with_fallback_chain(&state, req, compiled.as_ref()).await
    } else {
        run_playground_direct(&state, req, compiled.as_ref()).await
    }
}

/// Resolve and apply a managed prompt config to the playground request.
///
/// When `req.prompt_config` is set, this loads the version, validates variables,
/// compiles the Handlebars template, and applies model/temperature/max_tokens
/// overrides to the request in-place. The compiled result is returned so the
/// response can include version metadata.
async fn resolve_prompt_for_playground(
    state: &FlowState,
    req: &mut PlaygroundRequest,
) -> Result<Option<CompiledPrompt>> {
    let config_name = match &req.prompt_config {
        Some(name) if !name.is_empty() => name.clone(),
        _ => return Ok(None),
    };

    let compiled = resolve_and_compile_prompt(
        state.prompt_store.as_ref(),
        &state.db,
        req.project_id,
        &config_name,
        req.prompt_version_id,
        req.prompt_variables.as_ref(),
    )
    .await
    .map_err(|e| AppError::Validation(e))?;

    // Apply version overrides to the request
    if let Some(ref model) = compiled.model {
        if !model.trim().is_empty() {
            req.model = model.clone();
        }
    }
    if compiled.temperature.to_string() != "0" || req.temperature.is_none() {
        req.temperature =
            Some(rust_decimal::prelude::ToPrimitive::to_f32(&compiled.temperature).unwrap_or(0.5));
    }
    if let Some(max_tokens) = compiled.max_tokens {
        if let Ok(valid) = u32::try_from(max_tokens) {
            if valid > 0 {
                req.max_tokens = Some(valid);
            }
        }
    }

    Ok(Some(compiled))
}

/// Playground: route through the project's configured fallback chain.
///
/// Identical path to live API-key traffic: primary model is tried first, then
/// fallbacks in latency-sorted order on error. Returns which model/provider was
/// actually used so the developer can see what live traffic would hit.
async fn run_playground_with_fallback_chain(
    state: &FlowState,
    req: PlaygroundRequest,
    compiled: Option<&CompiledPrompt>,
) -> Result<Json<PlaygroundResponse>> {
    let mut chat_request = build_chat_request(&req, compiled);
    if req.enable_introspection {
        apply_introspection(&req.model, &mut chat_request);
    }

    if let Err(errors) = chat_request.validate() {
        return Err(AppError::Validation(errors.join("; ")));
    }

    let flow_url = state.internal_urls.flow.clone();
    let start = Instant::now();
    let gw_result = crate::api::gateway_client::call_gateway(
        &state.agent_http_client,
        &flow_url,
        req.project_id,
        &chat_request,
        None,
        None,
    )
    .await
    .map_err(|e| gateway_call_err_to_app_err(e))?;

    let latency_ms = start.elapsed().as_millis() as u64;

    let model_used = gw_result.model.clone();
    let provider_used = provider_from_model(&model_used);
    let fallback_used = model_used != req.model && req.model != "auto";

    let cost = state
        .llm_processor
        .cost_calculator()
        .calculate_cost(
            &provider_used,
            &model_used,
            gw_result.usage.prompt_tokens,
            gw_result.usage.completion_tokens,
            0,
            0,
        )
        .await
        .unwrap_or(rust_decimal::Decimal::ZERO);

    let evaluation = if req.auto_evaluate.unwrap_or(false) {
        evaluate_response(
            state,
            &EvaluationContext {
                project_id: req.project_id,
                messages: &req.messages,
                response: &gw_result.content,
            },
        )
        .await
        .ok()
    } else {
        None
    };

    Ok(Json(PlaygroundResponse {
        model: model_used,
        provider: provider_used,
        response: gw_result.content,
        usage: PlaygroundUsage {
            prompt_tokens: gw_result.usage.prompt_tokens,
            completion_tokens: gw_result.usage.completion_tokens,
            total_tokens: gw_result.usage.total_tokens,
        },
        latency_ms,
        cost_usd: cost,
        evaluation,
        fallback_used,
        prompt_version_id: compiled.map(|c| c.version_id),
        prompt_version_number: compiled.map(|c| c.version_number),
    }))
}

/// Playground: route directly to a specific model (original behavior).
async fn run_playground_direct(
    state: &FlowState,
    req: PlaygroundRequest,
    compiled: Option<&CompiledPrompt>,
) -> Result<Json<PlaygroundResponse>> {
    let mut chat_request = build_chat_request(&req, compiled);
    if req.enable_introspection {
        apply_introspection(&req.model, &mut chat_request);
    }

    if let Err(errors) = chat_request.validate() {
        return Err(AppError::Validation(errors.join("; ")));
    }

    let flow_url = state.internal_urls.flow.clone();
    let start = Instant::now();
    let gw_result = crate::api::gateway_client::call_gateway(
        &state.agent_http_client,
        &flow_url,
        req.project_id,
        &chat_request,
        None,
        None,
    )
    .await
    .map_err(|e| gateway_call_err_to_app_err(e))?;

    let latency_ms = start.elapsed().as_millis() as u64;

    let model_used = gw_result.model.clone();
    let provider_used = provider_from_model(&model_used);

    let cost = state
        .llm_processor
        .cost_calculator()
        .calculate_cost(
            &provider_used,
            &model_used,
            gw_result.usage.prompt_tokens,
            gw_result.usage.completion_tokens,
            0,
            0,
        )
        .await
        .unwrap_or(rust_decimal::Decimal::ZERO);

    let evaluation = if req.auto_evaluate.unwrap_or(false) {
        evaluate_response(
            state,
            &EvaluationContext {
                project_id: req.project_id,
                messages: &req.messages,
                response: &gw_result.content,
            },
        )
        .await
        .ok()
    } else {
        None
    };

    Ok(Json(PlaygroundResponse {
        model: model_used,
        provider: provider_used,
        response: gw_result.content,
        usage: PlaygroundUsage {
            prompt_tokens: gw_result.usage.prompt_tokens,
            completion_tokens: gw_result.usage.completion_tokens,
            total_tokens: gw_result.usage.total_tokens,
        },
        latency_ms,
        cost_usd: cost,
        evaluation,
        fallback_used: false,
        prompt_version_id: compiled.map(|c| c.version_id),
        prompt_version_number: compiled.map(|c| c.version_number),
    }))
}

/// Compare the same prompt across multiple models
async fn compare_models(
    State(state): State<Arc<FlowState>>,
    Json(req): Json<PlaygroundRequest>,
) -> Result<Json<CompareResponse>> {
    let models = req
        .compare_models
        .clone()
        .unwrap_or_else(|| vec![req.model.clone()]);

    if models.is_empty() {
        return Err(AppError::Validation(
            "At least one model required".to_string(),
        ));
    }

    if models.len() > 5 {
        return Err(AppError::Validation(
            "Maximum 5 models for comparison".to_string(),
        ));
    }

    // Run requests in parallel with timeout per model
    let futures: Vec<_> = models
        .iter()
        .map(|model| {
            let state = state.clone();
            let req = req.clone();
            let model = model.clone();
            async move {
                match timeout(
                    MODEL_REQUEST_TIMEOUT,
                    run_single_model(&state, req.project_id, &req, &model),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(AppError::Internal(anyhow::anyhow!(
                        "Model '{}' request timed out after {} seconds",
                        model,
                        MODEL_REQUEST_TIMEOUT.as_secs()
                    ))),
                }
            }
        })
        .collect();

    let results = join_all(futures).await;

    // Collect successful responses, log failures
    let mut responses = Vec::new();
    for result in results {
        match result {
            Ok(response) => responses.push(response),
            Err(e) => {
                warn!(error = %e, "Model comparison request failed");
            }
        }
    }

    if responses.is_empty() {
        return Err(AppError::Internal(anyhow::anyhow!(
            "All model requests failed"
        )));
    }

    // Build cost comparison
    let cost_comparison: Vec<CostBreakdown> = responses
        .iter()
        .map(|r| CostBreakdown {
            model: r.model.clone(),
            cost_usd: r.cost_usd,
            tokens_per_dollar: if r.cost_usd > Decimal::ZERO {
                (Decimal::from(r.usage.total_tokens) / r.cost_usd)
                    .to_string()
                    .parse()
                    .unwrap_or(0)
            } else {
                0
            },
        })
        .collect();

    // Find fastest and cheapest
    let fastest_model = responses
        .iter()
        .min_by_key(|r| r.latency_ms)
        .map(|r| r.model.clone())
        .unwrap_or_default();

    let cheapest_model = responses
        .iter()
        .min_by_key(|r| r.cost_usd)
        .map(|r| r.model.clone())
        .unwrap_or_default();

    Ok(Json(CompareResponse {
        responses,
        cost_comparison,
        fastest_model,
        cheapest_model,
    }))
}

/// Run a single model request (used by compare_models)
async fn run_single_model(
    state: &FlowState,
    project_id: Uuid,
    req: &PlaygroundRequest,
    model: &str,
) -> Result<PlaygroundResponse> {
    let mut chat_request = build_chat_request(req, None);
    chat_request.model = model.to_string();

    let flow_url = state.internal_urls.flow.clone();
    let start = Instant::now();
    let gw_result = crate::api::gateway_client::call_gateway(
        &state.agent_http_client,
        &flow_url,
        project_id,
        &chat_request,
        None,
        None,
    )
    .await
    .map_err(|e| gateway_call_err_to_app_err(e))?;

    let latency_ms = start.elapsed().as_millis() as u64;

    let model_used = gw_result.model.clone();
    let provider_used = provider_from_model(&model_used);

    let cost = state
        .llm_processor
        .cost_calculator()
        .calculate_cost(
            &provider_used,
            &model_used,
            gw_result.usage.prompt_tokens,
            gw_result.usage.completion_tokens,
            0,
            0,
        )
        .await
        .unwrap_or(rust_decimal::Decimal::ZERO);

    Ok(PlaygroundResponse {
        model: model_used,
        provider: provider_used,
        response: gw_result.content,
        usage: PlaygroundUsage {
            prompt_tokens: gw_result.usage.prompt_tokens,
            completion_tokens: gw_result.usage.completion_tokens,
            total_tokens: gw_result.usage.total_tokens,
        },
        latency_ms,
        cost_usd: cost,
        evaluation: None,
        fallback_used: false,
        prompt_version_id: None,
        prompt_version_number: None,
    })
}

/// Build a ChatCompletionRequest from playground request
/// Attach the correct introspection parameters for `model` to `request`.
///
/// Each vendor uses a different mechanism:
/// - Anthropic (claude-*): `thinking = { type: "enabled", budget_tokens: 10000 }`
/// - OpenAI o-series (o1, o3, o4*): `reasoning_effort = "medium"`
/// All other models are passed through unchanged.
fn apply_introspection(model: &str, request: &mut ChatCompletionRequest) {
    use crate::gateway::types::ThinkingConfig;

    let provider = Provider::from_model_prefix(model);
    match provider {
        Some(Provider::Anthropic) => {
            request.thinking = Some(ThinkingConfig {
                thinking_type: crate::gateway::types::ThinkingToggle::Enabled,
                budget_tokens: Some(10_000),
            });
        }
        Some(Provider::OpenAi) => {
            if model.starts_with('o') && model.chars().nth(1).map_or(false, |c| c.is_ascii_digit())
            {
                request.reasoning_effort = Some(crate::gateway::types::ReasoningEffort::Medium);
            }
        }
        _ => {}
    }
}

fn build_chat_request(
    req: &PlaygroundRequest,
    compiled: Option<&CompiledPrompt>,
) -> ChatCompletionRequest {
    let mut messages: Vec<ChatMessage> = req
        .messages
        .iter()
        .map(|m| {
            let content = match &m.content {
                PlaygroundContent::Text(s) => MessageContent::Text(s.clone()),
                PlaygroundContent::Parts(parts) => MessageContent::Parts(parts.clone()),
            };
            ChatMessage {
                role: match m.role.as_str() {
                    "system" => MessageRole::System,
                    "user" => MessageRole::User,
                    "assistant" => MessageRole::Assistant,
                    _ => MessageRole::Other,
                },
                content: Some(content),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            }
        })
        .collect();

    // Inject compiled system prompt from prompt_config (prepend or merge)
    if let Some(compiled) = compiled {
        if let Some(ref system_prompt) = compiled.system_prompt {
            let already_has_system = messages
                .first()
                .is_some_and(|m| m.role == MessageRole::System);

            if already_has_system {
                // Prepend to existing system message
                if let Some(first) = messages.first_mut() {
                    match &first.content {
                        Some(MessageContent::Text(existing)) => {
                            first.content = Some(MessageContent::Text(format!(
                                "{}\n\n{}",
                                system_prompt, existing
                            )));
                        }
                        _ => {
                            first.content = Some(MessageContent::Text(system_prompt.clone()));
                        }
                    }
                }
            } else {
                messages.insert(
                    0,
                    ChatMessage {
                        role: MessageRole::System,
                        content: Some(MessageContent::Text(system_prompt.clone())),
                        name: None,
                        tool_calls: None,
                        tool_call_id: None,
                        reasoning_content: None,
                    },
                );
            }
        }
    }

    let mut request = ChatCompletionRequest {
        model: req.model.clone(),
        messages,
        temperature: req.temperature,
        max_tokens: req.max_tokens,
        stream: Some(false),
        prompt_variables: None,
        models: None,
        provider: None,
        ..Default::default()
    };

    // Apply tools and response_format from compiled prompt
    if let Some(compiled) = compiled {
        if request.tools.is_none() {
            if let Some(ref tools_json) = compiled.tools {
                if let Ok(tools) = serde_json::from_value(tools_json.clone()) {
                    request.tools = Some(tools);
                }
            }
        }
        if request.response_format.is_none() {
            if let Some(ref format_json) = compiled.response_format {
                if let Ok(response_format) = serde_json::from_value(format_json.clone()) {
                    request.response_format = Some(response_format);
                }
            }
        }
    }

    request
}

/// Context for LLM-as-judge evaluation
struct EvaluationContext<'a> {
    pub project_id: Uuid,
    pub messages: &'a [PlaygroundMessage],
    pub response: &'a str,
}

/// Run LLM-as-judge evaluation on a playground response.
///
/// Delegates to [`crate::gateway::evaluator::run_llm_judge`] which contains
/// the shared evaluation logic used by both the playground and the background
/// output guardrail quality check.
async fn evaluate_response(
    state: &FlowState,
    ctx: &EvaluationContext<'_>,
) -> Result<PlaygroundEvaluation> {
    let user_query = ctx
        .messages
        .iter()
        .filter(|m| m.role == "user")
        .map(|m| m.content.as_text())
        .collect::<Vec<_>>()
        .join("\n");

    let scores =
        crate::gateway::evaluator::run_llm_judge(state, ctx.project_id, &user_query, ctx.response)
            .await
            .ok_or_else(|| AppError::Internal(anyhow::anyhow!("LLM-as-judge evaluation failed")))?;

    Ok(PlaygroundEvaluation {
        relevance: scores.relevance,
        coherence: scores.coherence,
        helpfulness: scores.helpfulness,
        summary: scores.summary,
    })
}

/// Derive provider name from model string for display purposes.
fn provider_from_model(model: &str) -> String {
    Provider::from_model_prefix(model)
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| model.split('/').next().unwrap_or("unknown").to_string())
}

/// Convert a gateway_client call error into an AppError.
fn gateway_call_err_to_app_err(err: crate::api::gateway_client::GatewayCallError) -> AppError {
    use crate::api::gateway_client::GatewayCallError;
    match err {
        GatewayCallError::RateLimited { .. } => {
            AppError::External("Rate limited by provider. Please try again.".into())
        }
        GatewayCallError::ContextTooLong => {
            AppError::BadRequest("Context too long for the selected model.".into())
        }
        GatewayCallError::Overloaded { .. } => {
            AppError::External("Provider overloaded. Please try again later.".into())
        }
        GatewayCallError::Transient { body, .. } => AppError::External(body),
        GatewayCallError::Fatal { status, body } => {
            if status < 500 {
                AppError::BadRequest(body)
            } else {
                AppError::External(body)
            }
        }
        GatewayCallError::PaymentRequired { message } => AppError::BadRequest(message),
        GatewayCallError::ProviderBillingError { message } => AppError::External(message),
        GatewayCallError::Network(e) => AppError::External(e.to_string()),
    }
}
