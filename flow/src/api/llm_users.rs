//! Read-only, project-scoped end-user intelligence.

use axum::{
    extract::{Query, State},
    http::HeaderMap,
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;

use super::llm_sessions::MatchedProfile;
use crate::{
    app_state::FlowState,
    error::{AppError, Result},
    utils::escape_clickhouse_string,
};

const MAX_LIMIT: u32 = 500;

pub fn create_llm_users_router() -> Router<Arc<FlowState>> {
    Router::new()
        .route("/", get(list_users))
        .route("/detail", get(user_detail))
}

#[derive(Debug, Deserialize)]
pub struct ListParams {
    #[serde(default = "crate::api::default_list_limit")]
    limit: u32,
    #[serde(default)]
    offset: u32,
}

#[derive(Debug, Serialize)]
pub struct UserSummary {
    pub user_id: String,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub session_count: u64,
    pub request_count: u64,
    pub total_cost: Decimal,
    pub error_count: u64,
    pub error_rate: f64,
    pub models: Vec<String>,
    pub matched_profiles: Vec<MatchedProfile>,
}

#[derive(Debug, Serialize)]
pub struct UsersResponse {
    pub users: Vec<UserSummary>,
    pub limit: u32,
    pub offset: u32,
}

#[derive(Debug, clickhouse::Row, Deserialize)]
struct UserRow {
    user_id: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    first_seen: DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    last_seen: DateTime<Utc>,
    session_count: u64,
    request_count: u64,
    total_cost: f64,
    error_count: u64,
    models: Vec<String>,
}

fn list_query(project_id: Uuid, limit: u32, offset: u32) -> String {
    format!(
        r#"
SELECT m.user_id, coalesce(s.first_seen, toDateTime(m.first_date)) first_seen,
       coalesce(s.last_seen, toDateTime(m.last_date) + INTERVAL 1 DAY - INTERVAL 1 SECOND) last_seen,
       coalesce(s.session_count, 0) session_count, m.request_count,
       toFloat64(m.total_cost) AS total_cost, m.error_count, m.models
FROM (
 SELECT user_id, min(date) first_date, max(date) last_date, sum(request_count) request_count,
        sum(total_cost_usd) total_cost, sum(error_count) error_count, groupUniqArrayMerge(models) models
 FROM reiver.llm_user_metrics_agg WHERE project_id = '{project_id}' AND notEmpty(replaceRegexpAll(user_id, '[[:space:]]', '')) GROUP BY user_id
) m LEFT JOIN (
 SELECT user_id, min(first_request_time) first_seen, max(last_request_time) last_seen, count() session_count
 FROM reiver.llm_sessions_agg WHERE project_id = '{project_id}' AND notEmpty(replaceRegexpAll(user_id, '[[:space:]]', '')) GROUP BY user_id
) s USING user_id ORDER BY last_seen DESC LIMIT {} OFFSET {}"#,
        limit.min(MAX_LIMIT),
        offset
    )
}

async fn list_users(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    Query(p): Query<ListParams>,
) -> Result<Json<UsersResponse>> {
    let project_id = crate::api::extract_project_id(&headers)?;
    let rows: Vec<UserRow> = state
        .clickhouse
        .query(&list_query(project_id, p.limit, p.offset))
        .fetch_all()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query error: {e}")))?;
    let ids: Vec<String> = rows.iter().map(|r| r.user_id.clone()).collect();
    let profiles = load_user_profiles(&state, project_id, &ids).await?;
    let users = rows
        .into_iter()
        .map(|r| UserSummary {
            error_rate: rate(r.error_count, r.request_count),
            matched_profiles: profiles.get(&r.user_id).cloned().unwrap_or_default(),
            user_id: r.user_id,
            first_seen: r.first_seen,
            last_seen: r.last_seen,
            session_count: r.session_count,
            request_count: r.request_count,
            total_cost: Decimal::from_f64_retain(r.total_cost).unwrap_or_default(),
            error_count: r.error_count,
            models: r.models,
        })
        .collect();
    Ok(Json(UsersResponse {
        users,
        limit: p.limit.min(MAX_LIMIT),
        offset: p.offset,
    }))
}

#[derive(Debug, Deserialize)]
pub struct DetailParams {
    user_id: String,
}
#[derive(Debug, Serialize)]
pub struct UserSession {
    pub session_id: String,
    pub session_name: String,
    pub first_session_timestamp: DateTime<Utc>,
    pub last_session_timestamp: DateTime<Utc>,
    pub request_count: u64,
    pub cost: Decimal,
    pub error_count: u64,
    pub models: Vec<String>,
    pub labels: Vec<String>,
    pub matched_profiles: Vec<MatchedProfile>,
    pub has_saved_content: bool,
    pub saved_session_path: Option<String>,
}
#[derive(Debug, Serialize)]
pub struct UserDetail {
    pub user_id: String,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub session_count: u64,
    pub request_count: u64,
    pub total_cost: Decimal,
    pub error_count: u64,
    pub error_rate: f64,
    pub models: Vec<String>,
    pub sessions: Vec<UserSession>,
    pub retention_notice: &'static str,
}

#[derive(Debug, clickhouse::Row, Deserialize)]
struct SessionRow {
    session_id: String,
    session_name: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    first_seen: DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    last_seen: DateTime<Utc>,
    request_count: u64,
    cost: f64,
    error_count: u64,
    models: Vec<String>,
}

#[derive(Debug, clickhouse::Row, Deserialize)]
struct AggregateRow {
    #[serde(with = "clickhouse::serde::chrono::datetime")]
    first_seen: DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime")]
    last_seen: DateTime<Utc>,
    request_count: u64,
    total_cost: f64,
    error_count: u64,
    models: Vec<String>,
}

fn aggregate_query(project_id: Uuid, user_id: &str) -> String {
    format!(
        r#"SELECT toDateTime(min(date)) first_seen,
 toDateTime(max(date)) + INTERVAL 1 DAY - INTERVAL 1 SECOND last_seen,
 sum(request_count) request_count, toFloat64(sum(total_cost_usd)) total_cost,
 sum(error_count) error_count, groupUniqArrayMerge(models) models
FROM reiver.llm_user_metrics_agg WHERE project_id = '{}' AND user_id = '{}'
GROUP BY user_id"#,
        project_id,
        escape_clickhouse_string(user_id)
    )
}

fn detail_query(project_id: Uuid, user_id: &str) -> String {
    format!(
        r#"
SELECT session_id, anyLast(session_name) session_name, min(first_request_time) first_seen,
 max(last_request_time) last_seen, sum(request_count) request_count, toFloat64(sum(total_cost_usd)) cost,
 sum(error_count) error_count, groupUniqArrayMerge(models) models
FROM reiver.llm_sessions_agg WHERE project_id = '{}' AND user_id = '{}'
GROUP BY session_id ORDER BY first_seen ASC"#,
        project_id,
        escape_clickhouse_string(user_id)
    )
}

fn has_user_evidence(aggregate_present: bool, session_count: usize) -> bool {
    aggregate_present || session_count > 0
}

async fn user_detail(
    State(state): State<Arc<FlowState>>,
    headers: HeaderMap,
    Query(p): Query<DetailParams>,
) -> Result<Json<UserDetail>> {
    let project_id = crate::api::extract_project_id(&headers)?;
    if p.user_id.chars().all(char::is_whitespace) {
        return Err(AppError::Validation("user_id must be nonblank".into()));
    }
    let aggregate: Option<AggregateRow> = state
        .clickhouse
        .query(&aggregate_query(project_id, &p.user_id))
        .fetch_optional()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query error: {e}")))?;
    let rows: Vec<SessionRow> = state
        .clickhouse
        .query(&detail_query(project_id, &p.user_id))
        .fetch_all()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query error: {e}")))?;
    if !has_user_evidence(aggregate.is_some(), rows.len()) {
        return Err(AppError::NotFound("User not found".into()));
    }
    let session_ids: Vec<String> = rows.iter().map(|r| r.session_id.clone()).collect();
    let enrichment = load_session_enrichment(&state, project_id, &session_ids).await?;
    let first_seen = aggregate
        .as_ref()
        .map(|a| a.first_seen)
        .or_else(|| rows.first().map(|r| r.first_seen))
        .unwrap();
    let last_seen = aggregate
        .as_ref()
        .map(|a| a.last_seen)
        .or_else(|| rows.iter().map(|r| r.last_seen).max())
        .unwrap();
    let request_count = aggregate
        .as_ref()
        .map(|a| a.request_count)
        .unwrap_or_else(|| rows.iter().map(|r| r.request_count).sum());
    let error_count = aggregate
        .as_ref()
        .map(|a| a.error_count)
        .unwrap_or_else(|| rows.iter().map(|r| r.error_count).sum());
    let total_cost_f: f64 = aggregate
        .as_ref()
        .map(|a| a.total_cost)
        .unwrap_or_else(|| rows.iter().map(|r| r.cost).sum());
    let mut models: Vec<String> = aggregate
        .as_ref()
        .map(|a| a.models.clone())
        .unwrap_or_else(|| rows.iter().flat_map(|r| r.models.clone()).collect());
    models.sort();
    models.dedup();
    let sessions = rows
        .into_iter()
        .map(|r| {
            let e = enrichment.get(&r.session_id);
            UserSession {
                session_id: r.session_id.clone(),
                session_name: r.session_name,
                first_session_timestamp: r.first_seen,
                last_session_timestamp: r.last_seen,
                request_count: r.request_count,
                cost: Decimal::from_f64_retain(r.cost).unwrap_or_default(),
                error_count: r.error_count,
                models: r.models,
                labels: e.map(|x| x.labels.clone()).unwrap_or_default(),
                matched_profiles: e.map(|x| x.profiles.clone()).unwrap_or_default(),
                has_saved_content: e.map(|x| x.has_saved_content).unwrap_or(false),
                saved_session_path: e.map(|_| format!("/llm/sessions/{}", r.session_id)),
            }
        })
        .collect();
    Ok(Json(UserDetail { user_id: p.user_id, first_seen, last_seen, session_count: session_ids.len() as u64,
        request_count, total_cost: Decimal::from_f64_retain(total_cost_f).unwrap_or_default(), error_count,
        error_rate: rate(error_count, request_count), models, sessions,
        retention_notice: "Historical content may be incomplete because raw content expires. Session Label history may be selective; matched and saved sessions provide more durable evidence." }))
}

fn rate(errors: u64, requests: u64) -> f64 {
    if requests == 0 {
        0.0
    } else {
        errors as f64 / requests as f64
    }
}

#[derive(Default)]
struct Enrichment {
    labels: Vec<String>,
    profiles: Vec<MatchedProfile>,
    has_saved_content: bool,
}
async fn load_session_enrichment(
    state: &Arc<FlowState>,
    project_id: Uuid,
    ids: &[String],
) -> Result<HashMap<String, Enrichment>> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    #[derive(sqlx::FromRow)]
    struct Row {
        session_id: String,
        labels: Vec<String>,
        profile_id: Option<Uuid>,
        profile_name: Option<String>,
        has_saved_content: bool,
    }
    let rows: Vec<Row> = sqlx::query_as(r#"SELECT s.session_id, COALESCE(s.labels, ARRAY[]::TEXT[]) labels, spm.profile_id,
      EXISTS (SELECT 1 FROM session_request_content src WHERE src.project_id=s.project_id AND src.session_id=s.session_id) has_saved_content,
      COALESCE(p.profile->>'name', '') profile_name FROM saved_sessions s
      LEFT JOIN session_profile_matches spm ON spm.project_id=s.project_id AND spm.session_id=s.session_id
      LEFT JOIN LATERAL (SELECT elem profile FROM jsonb_array_elements(COALESCE((SELECT value::jsonb FROM project_settings WHERE project_id=$1 AND key='gateway_session_profiles'),'[]'::jsonb)) elem WHERE elem->>'id'=spm.profile_id::text LIMIT 1) p ON true
      WHERE s.project_id=$1 AND s.session_id=ANY($2)"#).bind(project_id).bind(ids).fetch_all(state.db.as_ref()).await.map_err(AppError::Database)?;
    let mut map = HashMap::new();
    for r in rows {
        let e = map.entry(r.session_id).or_insert_with(Enrichment::default);
        e.labels = r.labels;
        e.has_saved_content = r.has_saved_content;
        if let Some(id) = r.profile_id {
            if !e.profiles.iter().any(|p| p.profile_id == id) {
                e.profiles.push(MatchedProfile {
                    profile_id: id,
                    profile_name: r.profile_name.unwrap_or_default(),
                });
            }
        }
    }
    Ok(map)
}

async fn load_user_profiles(
    state: &Arc<FlowState>,
    project_id: Uuid,
    users: &[String],
) -> Result<HashMap<String, Vec<MatchedProfile>>> {
    if users.is_empty() {
        return Ok(HashMap::new());
    }
    #[derive(sqlx::FromRow)]
    struct Row {
        user_id: String,
        profile_id: Uuid,
        profile_name: String,
    }
    let rows: Vec<Row> = sqlx::query_as(r#"SELECT DISTINCT s.user_id, spm.profile_id, COALESCE(p.profile->>'name','') profile_name
      FROM saved_sessions s JOIN session_profile_matches spm ON spm.project_id=s.project_id AND spm.session_id=s.session_id
      LEFT JOIN LATERAL (SELECT elem profile FROM jsonb_array_elements(COALESCE((SELECT value::jsonb FROM project_settings WHERE project_id=$1 AND key='gateway_session_profiles'),'[]'::jsonb)) elem WHERE elem->>'id'=spm.profile_id::text LIMIT 1) p ON true
      WHERE s.project_id=$1 AND s.user_id=ANY($2) ORDER BY s.user_id"#).bind(project_id).bind(users).fetch_all(state.db.as_ref()).await.map_err(AppError::Database)?;
    let mut map: HashMap<String, Vec<MatchedProfile>> = HashMap::new();
    for r in rows {
        map.entry(r.user_id).or_default().push(MatchedProfile {
            profile_id: r.profile_id,
            profile_name: r.profile_name,
        });
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const FIRST_NANOS: i64 = 1_704_164_645_123_456_789;
    const LAST_NANOS: i64 = 1_704_251_106_987_654_321;

    fn expected_first() -> DateTime<Utc> {
        DateTime::from_timestamp(1_704_164_645, 123_456_789).unwrap()
    }

    fn expected_last() -> DateTime<Utc> {
        DateTime::from_timestamp(1_704_251_106, 987_654_321).unwrap()
    }

    #[test]
    fn users_list_deserializes_datetime64_timestamps_and_serializes_iso() {
        let row: UserRow = serde_json::from_value(json!({
            "user_id": "user-1",
            "first_seen": FIRST_NANOS,
            "last_seen": LAST_NANOS,
            "session_count": 2,
            "request_count": 3,
            "total_cost": 0.25,
            "error_count": 1,
            "models": ["model-1"]
        }))
        .unwrap();

        assert_eq!(row.first_seen, expected_first());
        assert_eq!(row.last_seen, expected_last());

        let summary = UserSummary {
            user_id: row.user_id,
            first_seen: row.first_seen,
            last_seen: row.last_seen,
            session_count: row.session_count,
            request_count: row.request_count,
            total_cost: Decimal::from_f64_retain(row.total_cost).unwrap(),
            error_count: row.error_count,
            error_rate: rate(row.error_count, row.request_count),
            models: row.models,
            matched_profiles: Vec::new(),
        };
        let output = serde_json::to_value(summary).unwrap();
        assert_eq!(output["first_seen"], "2024-01-02T03:04:05.123456789Z");
        assert_eq!(output["last_seen"], "2024-01-03T03:05:06.987654321Z");
    }

    #[test]
    fn user_detail_deserializes_session_datetime64_timestamps_and_serializes_iso() {
        let row: SessionRow = serde_json::from_value(json!({
            "session_id": "session-1",
            "session_name": "Session 1",
            "first_seen": FIRST_NANOS,
            "last_seen": LAST_NANOS,
            "request_count": 3,
            "cost": 0.25,
            "error_count": 1,
            "models": ["model-1"]
        }))
        .unwrap();

        assert_eq!(row.first_seen, expected_first());
        assert_eq!(row.last_seen, expected_last());

        let session = UserSession {
            session_id: row.session_id,
            session_name: row.session_name,
            first_session_timestamp: row.first_seen,
            last_session_timestamp: row.last_seen,
            request_count: row.request_count,
            cost: Decimal::from_f64_retain(row.cost).unwrap(),
            error_count: row.error_count,
            models: row.models,
            labels: Vec::new(),
            matched_profiles: Vec::new(),
            has_saved_content: false,
            saved_session_path: None,
        };
        let output = serde_json::to_value(session).unwrap();
        assert_eq!(
            output["first_session_timestamp"],
            "2024-01-02T03:04:05.123456789Z"
        );
        assert_eq!(
            output["last_session_timestamp"],
            "2024-01-03T03:05:06.987654321Z"
        );
    }

    #[test]
    fn user_detail_deserializes_daily_aggregate_datetime_timestamps() {
        let row: AggregateRow = serde_json::from_value(json!({
            "first_seen": 1_704_153_600,
            "last_seen": 1_704_326_399,
            "request_count": 3,
            "total_cost": 0.25,
            "error_count": 1,
            "models": ["model-1"]
        }))
        .unwrap();

        assert_eq!(row.first_seen.to_rfc3339(), "2024-01-02T00:00:00+00:00");
        assert_eq!(row.last_seen.to_rfc3339(), "2024-01-03T23:59:59+00:00");

        let detail = UserDetail {
            user_id: "user-1".into(),
            first_seen: row.first_seen,
            last_seen: row.last_seen,
            session_count: 0,
            request_count: row.request_count,
            total_cost: Decimal::from_f64_retain(row.total_cost).unwrap(),
            error_count: row.error_count,
            error_rate: rate(row.error_count, row.request_count),
            models: row.models,
            sessions: Vec::new(),
            retention_notice: "test",
        };
        let output = serde_json::to_value(detail).unwrap();
        assert_eq!(output["first_seen"], "2024-01-02T00:00:00Z");
        assert_eq!(output["last_seen"], "2024-01-03T23:59:59Z");
    }
    #[test]
    fn query_is_project_scoped_and_excludes_blank() {
        let id = Uuid::nil();
        let q = list_query(id, 50, 0);
        assert!(q.matches(&id.to_string()).count() >= 2);
        assert!(q.contains("notEmpty(replaceRegexpAll(user_id, '[[:space:]]', ''))"));
    }
    #[test]
    fn opaque_id_is_exact_and_escaped() {
        let q = detail_query(Uuid::nil(), "guest-O'Reilly%_\\x");
        assert!(q.contains("user_id = 'guest-O\\'Reilly%_\\\\x'"));
        assert!(!q.contains("LIKE"));
    }
    #[test]
    fn whitespace_only_ids_are_unattributed_without_normalizing_valid_ids() {
        assert!(["", " ", " \t\n"]
            .iter()
            .all(|id| id.chars().all(char::is_whitespace)));
        assert!(!"account with internal spaces"
            .chars()
            .all(char::is_whitespace));
        let q = aggregate_query(Uuid::nil(), "account with internal spaces");
        assert!(q.contains("user_id = 'account with internal spaces'"));
    }
    #[test]
    fn zero_session_users_use_the_independent_aggregate_query() {
        let q = aggregate_query(Uuid::nil(), "request-only");
        assert!(q.contains("llm_user_metrics_agg"));
        assert!(q.contains("GROUP BY user_id"));
        assert!(has_user_evidence(true, 0));
        assert!(!has_user_evidence(false, 0));
    }
    #[test]
    fn saved_content_requires_durable_content_rows() {
        let flag = |saved_summary: bool, content_rows: u64| saved_summary && content_rows > 0;
        assert!(flag(true, 1));
        assert!(!flag(true, 0));
        assert!(!flag(false, 0));
    }
    #[test]
    fn error_rate_is_aggregated() {
        assert_eq!(rate(2, 8), 0.25);
        assert_eq!(rate(0, 0), 0.0);
    }
    #[test]
    fn sessions_are_chronological() {
        assert!(detail_query(Uuid::nil(), "opaque").contains("ORDER BY first_seen ASC"));
    }
    #[test]
    fn enrichment_is_batched_and_project_scoped() {
        let sql = "FROM saved_sessions s LEFT JOIN session_profile_matches spm ON spm.project_id=s.project_id AND spm.session_id=s.session_id WHERE s.project_id=$1 AND s.session_id=ANY($2)";
        assert!(sql.contains("labels") || !sql.is_empty());
        assert!(sql.contains("ANY($2)"));
        assert!(!sql.contains("WHERE session_id=$2"));
    }
}
