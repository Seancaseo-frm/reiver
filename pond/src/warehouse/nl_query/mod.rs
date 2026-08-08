//! Natural language to SQL query module.
//!
//! Converts user questions in plain English into SQL queries using an LLM,
//! validates the generated SQL, and executes it through the warehouse query engine.

pub mod cache;
pub mod conversation;
pub mod llm_client;
pub mod prompt_builder;
pub mod suggestions;
pub mod validator;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::app_state::PondState;
use crate::warehouse::catalog::CatalogRepository;

use self::conversation::ConversationRepository;
use self::llm_client::LlmClient;
use self::prompt_builder::PromptBuilder;
use self::validator::SqlValidator;

/// Maximum number of retries after the initial attempt fails.
/// Total LLM calls = 1 (initial) + MAX_RETRIES = 3.
const MAX_RETRIES: u32 = 2;

/// Request for a natural language query.
#[derive(Debug, Deserialize)]
pub struct NLQueryRequest {
    /// The user's question in plain English.
    pub question: String,
    /// Optional: preferred LLM model (default: gpt-4o).
    pub model: Option<String>,
    /// Optional: continue an existing conversation for multi-turn queries.
    pub conversation_id: Option<Uuid>,
}

/// Response from a natural language query.
#[derive(Debug, Serialize)]
pub struct NLQueryResponse {
    /// The generated SQL query.
    pub sql: String,
    /// Query result columns.
    pub columns: Vec<crate::api::warehouse::ColumnInfo>,
    /// Query result rows.
    pub rows: Vec<Vec<serde_json::Value>>,
    /// Number of rows returned.
    pub row_count: usize,
    /// Query execution time in milliseconds.
    pub execution_time_ms: u64,
    /// Number of LLM attempts it took to generate valid SQL.
    pub attempts: u32,
    /// The LLM model that generated the SQL.
    pub model_used: String,
    /// Conversation ID for multi-turn follow-ups.
    pub conversation_id: Uuid,
}

/// Execute a natural language query against the warehouse.
///
/// Pipeline: cache check -> catalog load -> conversation load -> few-shot ->
///           prompt build -> LLM call -> validate -> execute -> persist turn.
/// On failure, retries with error context up to MAX_RETRY_ATTEMPTS times.
#[tracing::instrument(name = "warehouse.nl_query.execute_nl_query", skip_all, err(Display))]
pub async fn execute_nl_query(
    state: &Arc<PondState>,
    project_id: Uuid,
    user_id: Uuid,
    question: &str,
    model: Option<&str>,
    conversation_id: Option<Uuid>,
) -> Result<NLQueryResponse> {
    let flow_url = &state.config.flow_gateway_url;
    let requested_model = model.unwrap_or("gpt-4o");

    tracing::info!(
        project_id = %project_id,
        question_len = question.len(),
        model = %requested_model,
        "Starting NL query"
    );

    // Invalidate NL cache when catalog has changed for this project.
    if state.table_cache_dirty.contains(&project_id) {
        state.nl_query_cache.invalidate_project(project_id);
    }

    // --- Cache check ---
    if let Some(cached_sql) = state.nl_query_cache.get(project_id, question) {
        tracing::info!("NL query cache hit, validating cached SQL");

        let catalog_repo = CatalogRepository::new(state.db.clone());
        let entries = catalog_repo
            .list_entries(project_id)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to load catalog: {}", e))?;
        let validator = SqlValidator::new();

        if validator.validate_sql(&cached_sql, &entries).is_ok() {
            let start = std::time::Instant::now();
            if let Ok((columns, rows)) = execute_sql_query(state, project_id, &cached_sql).await {
                let row_count = rows.len();
                let execution_time_ms = start.elapsed().as_millis() as u64;

                let conv_repo = ConversationRepository::new((*state.db).clone());
                let conv_id = resolve_or_create_conversation(
                    &conv_repo,
                    project_id,
                    user_id,
                    conversation_id,
                )
                .await?;
                let turn_idx = conv_repo
                    .next_turn_index(conv_id)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to get turn index: {}", e))?;
                let _ = conv_repo
                    .insert_turn(
                        conv_id,
                        turn_idx,
                        question,
                        &cached_sql,
                        Some(execution_time_ms as i32),
                        Some(row_count as i32),
                        None,
                    )
                    .await;

                return Ok(NLQueryResponse {
                    sql: cached_sql,
                    columns,
                    rows,
                    row_count,
                    execution_time_ms,
                    attempts: 0,
                    model_used: requested_model.to_string(),
                    conversation_id: conv_id,
                });
            }
        }
        state.nl_query_cache.remove(project_id, question);
    }

    // --- Catalog ---
    let catalog_repo = CatalogRepository::new(state.db.clone());
    let entries = catalog_repo
        .list_entries(project_id)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to load catalog: {}", e))?;

    if entries.is_empty() {
        return Err(anyhow::anyhow!(
            "No tables found in the catalog for this project. Add data sources first."
        ));
    }

    let schema_context = PromptBuilder::build_schema_context(&entries);

    tracing::info!(
        table_count = schema_context.table_count,
        schema_chars = schema_context.formatted.len(),
        "Built schema context"
    );

    // --- Conversation history ---
    let conv_repo = ConversationRepository::new((*state.db).clone());
    let conv_id =
        resolve_or_create_conversation(&conv_repo, project_id, user_id, conversation_id).await?;

    let prior_turns = conv_repo.load_turns(conv_id).await.unwrap_or_default();

    // --- Few-shot examples from query history ---
    let history_entries = conv_repo
        .recent_successful_pairs(project_id, 5)
        .await
        .unwrap_or_default();

    // --- LLM call ---
    let api_key = get_project_api_key(state, project_id).await?;
    let llm_client = LlmClient::new(&state.http_client, flow_url, &api_key);
    let validator = SqlValidator::new();

    let mut last_error: Option<String> = None;
    let mut attempt = 0;
    let mut actual_model = requested_model.to_string();

    loop {
        attempt += 1;

        let messages = if !prior_turns.is_empty() {
            PromptBuilder::build_conversational_prompt(
                &schema_context,
                &prior_turns,
                question,
                last_error.as_deref(),
                &history_entries,
            )
        } else {
            PromptBuilder::build_prompt_with_few_shot(
                &schema_context,
                question,
                last_error.as_deref(),
                &history_entries,
            )
        };

        let (generated_sql, model_used) =
            llm_client.generate_sql(requested_model, messages).await?;
        actual_model = model_used;

        tracing::info!(
            attempt = attempt,
            model_used = %actual_model,
            sql_len = generated_sql.len(),
            "LLM generated SQL"
        );

        match validator.validate_sql(&generated_sql, &entries) {
            Ok(validated) => {
                let start = std::time::Instant::now();
                let result = execute_sql_query(state, project_id, &validated.sql).await;

                match result {
                    Ok((columns, rows)) => {
                        let row_count = rows.len();
                        let execution_time_ms = start.elapsed().as_millis() as u64;

                        tracing::info!(
                            row_count = row_count,
                            execution_time_ms = execution_time_ms,
                            attempts = attempt,
                            model_used = %actual_model,
                            "NL query completed successfully"
                        );

                        // Persist the turn
                        let turn_idx = conv_repo
                            .next_turn_index(conv_id)
                            .await
                            .map_err(|e| anyhow::anyhow!("Failed to get turn index: {}", e))?;
                        let _ = conv_repo
                            .insert_turn(
                                conv_id,
                                turn_idx,
                                question,
                                &validated.sql,
                                Some(execution_time_ms as i32),
                                Some(row_count as i32),
                                None,
                            )
                            .await;

                        // Cache the successful query
                        state
                            .nl_query_cache
                            .insert(project_id, question, &validated.sql);

                        return Ok(NLQueryResponse {
                            sql: validated.sql,
                            columns,
                            rows,
                            row_count,
                            execution_time_ms,
                            attempts: attempt,
                            model_used: actual_model,
                            conversation_id: conv_id,
                        });
                    }
                    Err(e) => {
                        tracing::warn!(
                            attempt = attempt,
                            generated_sql = %validated.sql,
                            error = %e,
                            "NL query SQL execution failed"
                        );

                        if attempt > MAX_RETRIES {
                            let turn_idx = conv_repo.next_turn_index(conv_id).await.unwrap_or(0);
                            let _ = conv_repo
                                .insert_turn(
                                    conv_id,
                                    turn_idx,
                                    question,
                                    &validated.sql,
                                    None,
                                    None,
                                    Some(&e.to_string()),
                                )
                                .await;

                            return Err(anyhow::anyhow!(
                                "Failed to generate a valid query after {} attempts. \
                                 The AI could not produce a working SQL query for your question. \
                                 Try rephrasing or simplifying your question.",
                                attempt
                            ));
                        }

                        last_error = Some(format!(
                            "SQL execution failed.\nGenerated SQL: {}\nError: {}",
                            validated.sql, e
                        ));
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    attempt = attempt,
                    generated_sql = %generated_sql,
                    error = %e,
                    "NL query SQL validation failed"
                );

                if attempt > MAX_RETRIES {
                    let turn_idx = conv_repo.next_turn_index(conv_id).await.unwrap_or(0);
                    let _ = conv_repo
                        .insert_turn(
                            conv_id,
                            turn_idx,
                            question,
                            &generated_sql,
                            None,
                            None,
                            Some(&e.to_string()),
                        )
                        .await;

                    return Err(anyhow::anyhow!(
                        "Failed to generate a valid query after {} attempts. \
                         The AI could not produce a working SQL query for your question. \
                         Try rephrasing or simplifying your question.",
                        attempt
                    ));
                }

                last_error = Some(format!(
                    "SQL validation failed.\nGenerated SQL: {}\nError: {}",
                    generated_sql, e
                ));
            }
        }
    }
}

/// Resolve an existing conversation or create a new one.
async fn resolve_or_create_conversation(
    repo: &ConversationRepository,
    project_id: Uuid,
    user_id: Uuid,
    conversation_id: Option<Uuid>,
) -> Result<Uuid> {
    match conversation_id {
        Some(id) => {
            let owned = repo
                .verify_ownership(id, project_id)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to verify conversation: {}", e))?;
            if !owned {
                return Err(anyhow::anyhow!(
                    "Conversation not found or does not belong to this project"
                ));
            }
            Ok(id)
        }
        None => repo
            .create(project_id, user_id)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create conversation: {}", e)),
    }
}

/// Execute a SQL query with project-level isolation.
///
/// SECURITY: Validates table access against the project's registered tables
/// and rewrites the SQL for the correct storage backend before execution.
/// This prevents cross-project data access and access to system tables.
#[tracing::instrument(name = "warehouse.nl_query.execute_sql_query", skip_all, err(Display))]
async fn execute_sql_query(
    state: &Arc<PondState>,
    project_id: Uuid,
    sql: &str,
) -> Result<(
    Vec<crate::api::warehouse::ColumnInfo>,
    Vec<Vec<serde_json::Value>>,
)> {
    use crate::warehouse::query::executor::ExecutionOptions;

    // Validate table access and rewrite SQL for the project's storage tier.
    // This reuses the same security logic as the regular query endpoint,
    // ensuring NL-generated queries cannot access tables outside the project.
    let rewritten_sql =
        crate::api::warehouse::validate_and_rewrite_nl_query(state, project_id, sql)
            .await
            .map_err(|e| anyhow::anyhow!("Table access validation failed: {}", e))?;

    let executor = &state.warehouse_query_executor;

    let options = ExecutionOptions {
        limit: Some(1000),
        timeout_secs: Some(30),
        max_memory_bytes: Some(50 * 1024 * 1024),
    };

    let result = executor
        .execute(&rewritten_sql, options)
        .await
        .map_err(|e| anyhow::anyhow!("Query execution failed: {}", e))?;

    // Convert executor ColumnInfo to API ColumnInfo
    let columns: Vec<crate::api::warehouse::ColumnInfo> = result
        .columns
        .iter()
        .map(|c| crate::api::warehouse::ColumnInfo {
            name: c.name.clone(),
            data_type: c.data_type.clone(),
        })
        .collect();

    Ok((columns, result.rows))
}

/// Get the project's API key for the Flow gateway.
#[tracing::instrument(
    name = "warehouse.nl_query.get_project_api_key",
    skip_all,
    err(Display)
)]
async fn get_project_api_key(state: &Arc<PondState>, project_id: Uuid) -> Result<String> {
    // Look up the project's gateway API key from project_settings
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT value FROM project_settings WHERE project_id = $1 AND key = 'gateway_api_key'",
    )
    .bind(project_id)
    .fetch_optional(&*state.db)
    .await?;

    if let Some((encrypted_key,)) = row {
        // Decrypt the API key
        let api_key = state
            .encryptor
            .decrypt(&encrypted_key)
            .map_err(|e| anyhow::anyhow!("Failed to decrypt API key: {}", e))?;
        Ok(api_key)
    } else {
        // Fall back to checking for OpenAI key directly
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT value FROM project_settings WHERE project_id = $1 AND key = 'gateway_openai_api_key'"
        )
        .bind(project_id)
        .fetch_optional(&*state.db)
        .await?;

        if let Some((encrypted_key,)) = row {
            let api_key = state
                .encryptor
                .decrypt(&encrypted_key)
                .map_err(|e| anyhow::anyhow!("Failed to decrypt API key: {}", e))?;
            Ok(api_key)
        } else {
            Err(anyhow::anyhow!(
                "No gateway API key configured for this project. \
                 Add an API key in Project Settings > LLM Gateway."
            ))
        }
    }
}
