//! LLM Evaluation Scores API
//!
//! Endpoints for submitting and querying evaluation scores for LLM requests.
//! Supports human feedback, LLM-as-judge, and automated evaluators.
//!
//! Note: Scores are stored in PostgreSQL as the source of truth.
//! The `scores` map in ClickHouse's llm_requests table can be synced
//! via a background worker if denormalized access is needed.

use axum::{
    extract::{Path, Query, State},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::app_state::FlowState;
use crate::error::{AppError, Result};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScoreType {
    Number,
    Boolean,
    Category,
}

impl Default for ScoreType {
    fn default() -> Self {
        ScoreType::Number
    }
}

impl ScoreType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ScoreType::Number => "number",
            ScoreType::Boolean => "boolean",
            ScoreType::Category => "category",
        }
    }
}

pub fn create_llm_scores_router() -> Router<Arc<FlowState>> {
    Router::new()
        .route("/", post(submit_score))
        .route("/", get(list_scores))
        .route("/request/{request_id}", get(get_request_scores))
        .route("/batch", post(submit_batch_scores))
}

/// Request to submit a single evaluation score
#[derive(Debug, Deserialize)]
pub struct SubmitScoreRequest {
    pub project_id: Uuid,
    pub request_id: String,
    pub score_name: String,
    pub score_value: Decimal,
    #[serde(default)]
    pub score_type: ScoreType,
    pub reason: Option<String>,
    pub evaluator_type: Option<String>,
    pub evaluator_id: Option<String>,
}

/// Validate a score's type and value
fn validate_score(score_type: &ScoreType, score_value: Decimal) -> Result<()> {
    if *score_type == ScoreType::Boolean
        && !(score_value == Decimal::ZERO || score_value == Decimal::ONE)
    {
        return Err(AppError::Validation(
            "Boolean scores must be 0 or 1".to_string(),
        ));
    }

    if *score_type == ScoreType::Number
        && (score_value < Decimal::ZERO || score_value > Decimal::from(100))
    {
        return Err(AppError::Validation(
            "Numeric scores must be between 0 and 100".to_string(),
        ));
    }

    Ok(())
}

/// Submit a single evaluation score
async fn submit_score(
    State(state): State<Arc<FlowState>>,
    Json(req): Json<SubmitScoreRequest>,
) -> Result<Json<serde_json::Value>> {
    validate_score(&req.score_type, req.score_value)?;

    let score_id = Uuid::new_v4();

    // Insert into PostgreSQL (source of truth for scores)
    sqlx::query(
        r#"
        INSERT INTO llm_evaluation_scores 
        (id, project_id, request_id, score_name, score_value, score_type, reason, evaluator_type, evaluator_id, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
        "#,
    )
    .bind(score_id)
    .bind(req.project_id)
    .bind(&req.request_id)
    .bind(&req.score_name)
    .bind(req.score_value)
    .bind(req.score_type.as_str())
    .bind(&req.reason)
    .bind(&req.evaluator_type)
    .bind(&req.evaluator_id)
    .execute(state.db.as_ref())
    .await
    .map_err(|e| AppError::Database(e))?;

    // Note: ClickHouse denormalized scores are synced via background worker
    // to avoid expensive ALTER TABLE UPDATE mutations on every score submission

    Ok(Json(serde_json::json!({
        "success": true,
        "score_id": score_id.to_string(),
        "request_id": req.request_id
    })))
}

/// Batch score submission request
#[derive(Debug, Deserialize)]
pub struct BatchScoreRequest {
    pub project_id: Uuid,
    pub scores: Vec<SingleScore>,
}

#[derive(Debug, Deserialize)]
pub struct SingleScore {
    pub request_id: String,
    pub score_name: String,
    pub score_value: Decimal,
    #[serde(default)]
    pub score_type: ScoreType,
    pub reason: Option<String>,
    pub evaluator_type: Option<String>,
    pub evaluator_id: Option<String>,
}

/// Submit multiple scores in batch using a single transaction
///
/// This operation is atomic - either all scores are inserted or none are.
/// If any batch fails, the entire transaction is rolled back.
async fn submit_batch_scores(
    State(state): State<Arc<FlowState>>,
    Json(req): Json<BatchScoreRequest>,
) -> Result<Json<serde_json::Value>> {
    if req.scores.is_empty() {
        return Ok(Json(serde_json::json!({
            "success": true,
            "inserted": 0
        })));
    }

    // Validate all scores first
    let mut validation_errors: Vec<String> = Vec::new();
    for (idx, score) in req.scores.iter().enumerate() {
        if let Err(e) = validate_score(&score.score_type, score.score_value) {
            validation_errors.push(format!("Score {}: {}", idx, e));
        }
    }

    if !validation_errors.is_empty() {
        return Err(AppError::Validation(format!(
            "Validation errors: {}",
            validation_errors.join("; ")
        )));
    }

    // Use a transaction for batch insert - atomic all-or-nothing semantics
    let mut tx = state.db.begin().await.map_err(|e| AppError::Database(e))?;

    let mut success_count = 0;
    let now = Utc::now();

    // Build and execute batch insert using multi-value INSERT for better performance
    // Process in chunks to avoid overly large queries
    const CHUNK_SIZE: usize = 100;

    for chunk in req.scores.chunks(CHUNK_SIZE) {
        // Build multi-value INSERT
        let mut values_parts: Vec<String> = Vec::with_capacity(chunk.len());

        for (i, _) in chunk.iter().enumerate() {
            let base = i * 10; // 10 columns per row
            values_parts.push(format!(
                "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
                base + 1,
                base + 2,
                base + 3,
                base + 4,
                base + 5,
                base + 6,
                base + 7,
                base + 8,
                base + 9,
                base + 10
            ));
        }

        let query = format!(
            r#"
            INSERT INTO llm_evaluation_scores 
            (id, project_id, request_id, score_name, score_value, score_type, reason, evaluator_type, evaluator_id, created_at)
            VALUES {}
            "#,
            values_parts.join(", ")
        );

        let mut query_builder = sqlx::query(&query);

        for score in chunk {
            query_builder = query_builder
                .bind(Uuid::new_v4())
                .bind(req.project_id)
                .bind(&score.request_id)
                .bind(&score.score_name)
                .bind(score.score_value)
                .bind(score.score_type.as_str())
                .bind(&score.reason)
                .bind(&score.evaluator_type)
                .bind(&score.evaluator_id)
                .bind(now);
        }

        // On any failure, rollback the entire transaction (all-or-nothing)
        match query_builder.execute(&mut *tx).await {
            Ok(result) => success_count += result.rows_affected() as usize,
            Err(e) => {
                tracing::error!("Batch score insert failed, rolling back transaction: {}", e);
                // Transaction is automatically rolled back when tx is dropped without commit
                return Err(AppError::Database(e));
            }
        }
    }

    // Commit the transaction - all chunks succeeded
    tx.commit().await.map_err(|e| AppError::Database(e))?;

    Ok(Json(serde_json::json!({
        "success": true,
        "inserted": success_count
    })))
}

/// Query parameters for listing scores
#[derive(Debug, Deserialize)]
pub struct ListScoresParams {
    pub project_id: Uuid,
    #[serde(default = "crate::api::default_list_limit")]
    pub limit: u32,
    #[serde(default)]
    pub offset: u32,
    pub score_name: Option<String>,
    pub evaluator_type: Option<String>,
    pub start_date: Option<DateTime<Utc>>,
    pub end_date: Option<DateTime<Utc>>,
}

/// Maximum allowed limit for query results to prevent expensive queries
const MAX_LIMIT: u32 = 1000;

/// Score response
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct ScoreResponse {
    pub id: Uuid,
    pub project_id: Uuid,
    pub request_id: String,
    pub score_name: String,
    pub score_value: Decimal,
    pub score_type: String,
    pub reason: Option<String>,
    pub evaluator_type: Option<String>,
    pub evaluator_id: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// List scores
async fn list_scores(
    State(state): State<Arc<FlowState>>,
    Query(params): Query<ListScoresParams>,
) -> Result<Json<Vec<ScoreResponse>>> {
    // Cap limit to prevent expensive queries
    let limit = params.limit.min(MAX_LIMIT);

    // Build query with dynamic filters
    let mut conditions = vec!["project_id = $1".to_string()];
    let mut bind_idx = 2;

    if params.score_name.is_some() {
        conditions.push(format!("score_name = ${}", bind_idx));
        bind_idx += 1;
    }

    if params.evaluator_type.is_some() {
        conditions.push(format!("evaluator_type = ${}", bind_idx));
        bind_idx += 1;
    }

    if params.start_date.is_some() {
        conditions.push(format!("created_at >= ${}", bind_idx));
        bind_idx += 1;
    }

    if params.end_date.is_some() {
        conditions.push(format!("created_at <= ${}", bind_idx));
        // bind_idx not needed after this
    }

    let query = format!(
        r#"
        SELECT id, project_id, request_id, score_name, score_value, score_type, 
               reason, evaluator_type, evaluator_id, created_at
        FROM llm_evaluation_scores
        WHERE {}
        ORDER BY created_at DESC
        LIMIT {} OFFSET {}
        "#,
        conditions.join(" AND "),
        limit,
        params.offset
    );

    // Build the query with binds
    let mut query_builder = sqlx::query_as::<_, ScoreResponse>(&query).bind(params.project_id);

    if let Some(ref name) = params.score_name {
        query_builder = query_builder.bind(name);
    }

    if let Some(ref eval_type) = params.evaluator_type {
        query_builder = query_builder.bind(eval_type);
    }

    if let Some(start_date) = params.start_date {
        query_builder = query_builder.bind(start_date);
    }

    if let Some(end_date) = params.end_date {
        query_builder = query_builder.bind(end_date);
    }

    let scores: Vec<ScoreResponse> = query_builder
        .fetch_all(state.db.as_ref())
        .await
        .map_err(|e| AppError::Database(e))?;

    Ok(Json(scores))
}

/// Get scores for a specific request
async fn get_request_scores(
    State(state): State<Arc<FlowState>>,
    Path(request_id): Path<String>,
    Query(params): Query<ProjectIdParam>,
) -> Result<Json<Vec<ScoreResponse>>> {
    let scores: Vec<ScoreResponse> = sqlx::query_as(
        r#"
        SELECT id, project_id, request_id, score_name, score_value, score_type,
               reason, evaluator_type, evaluator_id, created_at
        FROM llm_evaluation_scores
        WHERE project_id = $1 AND request_id = $2
        ORDER BY created_at DESC
        "#,
    )
    .bind(params.project_id)
    .bind(&request_id)
    .fetch_all(state.db.as_ref())
    .await
    .map_err(|e| AppError::Database(e))?;

    Ok(Json(scores))
}

#[derive(Debug, Deserialize)]
pub struct ProjectIdParam {
    pub project_id: Uuid,
}
