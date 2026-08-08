use axum::{
    extract::{Path, Query, State},
    response::Json,
    routing::get,
    Router,
};
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use crate::api::flamegraph;
use crate::app_state::WatchState;
use crate::error::{AppError, Result};
use crate::utils::escape_clickhouse_string;

// ============================================================================
// Constants
// ============================================================================

/// Maximum allowed length for version strings to prevent DoS attacks
const MAX_VERSION_LENGTH: usize = 128;

/// Maximum allowed length for service name strings
const MAX_SERVICE_NAME_LENGTH: usize = 256;

/// Default number of profiles to return per page
const DEFAULT_PROFILE_LIMIT: u32 = 50;

/// Maximum number of profiles that can be returned per page
const MAX_PROFILE_LIMIT: u32 = 200;

/// Default number of versions to return when listing
const DEFAULT_VERSION_LIMIT: u32 = 20;

/// Maximum number of versions that can be returned
const MAX_VERSION_LIMIT: u32 = 100;

/// Default time range for profile queries (24 hours)
const DEFAULT_TIME_RANGE_HOURS: i64 = 24;

/// Default time range for version comparison queries (7 days)
const DEFAULT_COMPARISON_DAYS: i64 = 7;

/// Default time range for version listing queries (30 days)
const DEFAULT_VERSION_LISTING_DAYS: i64 = 30;

/// Maximum allowed length for trace_id strings (32 hex chars = 16 bytes, with some buffer)
const MAX_TRACE_ID_LENGTH: usize = 64;

/// Maximum allowed length for span_id strings (16 hex chars = 8 bytes, with some buffer)
const MAX_SPAN_ID_LENGTH: usize = 32;

/// Maximum allowed length for profile_id strings (32 hex chars = 16 bytes, with some buffer)
const MAX_PROFILE_ID_LENGTH: usize = 64;

// ============================================================================
// Helper Functions
// ============================================================================

/// Validate that a version string is within acceptable length limits
fn validate_version(version: &str) -> Result<()> {
    if version.len() > MAX_VERSION_LENGTH {
        return Err(AppError::Validation(format!(
            "Version string exceeds maximum length of {} characters",
            MAX_VERSION_LENGTH
        )));
    }
    Ok(())
}

/// Validate that a service name is within acceptable length limits
fn validate_service_name(service_name: &str) -> Result<()> {
    if service_name.len() > MAX_SERVICE_NAME_LENGTH {
        return Err(AppError::Validation(format!(
            "Service name exceeds maximum length of {} characters",
            MAX_SERVICE_NAME_LENGTH
        )));
    }
    Ok(())
}

/// Validate that a trace_id is within acceptable length limits
fn validate_trace_id(trace_id: &str) -> Result<()> {
    if trace_id.len() > MAX_TRACE_ID_LENGTH {
        return Err(AppError::Validation(format!(
            "Trace ID exceeds maximum length of {} characters",
            MAX_TRACE_ID_LENGTH
        )));
    }
    Ok(())
}

/// Validate that a span_id is within acceptable length limits
fn validate_span_id(span_id: &str) -> Result<()> {
    if span_id.len() > MAX_SPAN_ID_LENGTH {
        return Err(AppError::Validation(format!(
            "Span ID exceeds maximum length of {} characters",
            MAX_SPAN_ID_LENGTH
        )));
    }
    Ok(())
}

/// Validate that a profile_id is within acceptable length limits
fn validate_profile_id(profile_id: &str) -> Result<()> {
    if profile_id.len() > MAX_PROFILE_ID_LENGTH {
        return Err(AppError::Validation(format!(
            "Profile ID exceeds maximum length of {} characters",
            MAX_PROFILE_ID_LENGTH
        )));
    }
    Ok(())
}

/// Parse time range from query parameters with a default duration
///
/// Returns (start_time, end_time) tuple. If end_time is not provided, uses current time.
/// If start_time is not provided, uses end_time minus the default duration.
fn parse_time_range(
    params: &HashMap<String, String>,
    default_duration: chrono::Duration,
) -> (chrono::DateTime<Utc>, chrono::DateTime<Utc>) {
    let end_time = params
        .get("end_time")
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .unwrap_or_else(chrono::Utc::now);

    let start_time = params
        .get("start_time")
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .unwrap_or_else(|| end_time - default_duration);

    (start_time, end_time)
}

/// Parse pagination parameters from query params
fn parse_pagination(
    params: &HashMap<String, String>,
    default_limit: u32,
    max_limit: u32,
) -> (u32, u64) {
    let limit = params
        .get("limit")
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(default_limit)
        .min(max_limit);
    let offset = params
        .get("offset")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    (limit, offset)
}

pub fn create_profiles_router() -> Router<Arc<WatchState>> {
    Router::new()
        .route("/projects/{project_id}/profiles", get(list_profiles))
        .route(
            "/projects/{project_id}/profiles/{profile_id}",
            get(get_profile),
        )
        .route(
            "/projects/{project_id}/profiles/{profile_id}/download",
            get(download_profile),
        )
        .route(
            "/projects/{project_id}/traces/{trace_id}/profile",
            get(get_profile_for_trace),
        )
        .route(
            "/projects/{project_id}/services/{service}/profiles",
            get(list_service_profiles),
        )
        .route(
            "/projects/{project_id}/services/{service}/profiles/aggregate",
            get(aggregate_profiles),
        )
        .route(
            "/projects/{project_id}/services/{service}/profiles/diff",
            get(diff_profiles),
        )
        .route(
            "/projects/{project_id}/services/{service}/profiles/top-functions",
            get(top_functions),
        )
        .route(
            "/projects/{project_id}/services/{service}/profiles/comparison",
            get(compare_profiles),
        )
        .route(
            "/projects/{project_id}/services/{service}/profiles/versions",
            get(list_profile_versions),
        )
        .route(
            "/projects/{project_id}/services/{service}/profiles/version/{version}",
            get(get_version_stats),
        )
        .route("/projects/{project_id}/source", get(get_source_file))
        .route(
            "/projects/{project_id}/profiles/attribute-keys",
            get(list_profile_attribute_keys),
        )
        .route(
            "/projects/{project_id}/profiles/attribute-values",
            get(list_profile_attribute_values),
        )
        .route(
            "/projects/{project_id}/profiles/top-functions",
            get(top_functions_project),
        )
}

/// List profiles for a project
/// GET /api/profiles/projects/{project_id}/profiles
/// Query params: service_name, trace_id, span_id, start_time, end_time, limit, offset
async fn list_profiles(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<serde_json::Value>> {
    // Get query parameters
    let service_name = params
        .get("service_name")
        .or_else(|| params.get("service"))
        .cloned();
    let trace_id = params.get("trace_id").cloned();
    let span_id = params.get("span_id").cloned();

    // Validate optional parameters if provided
    if let Some(ref service) = service_name {
        validate_service_name(service)?;
    }
    if let Some(ref trace) = trace_id {
        validate_trace_id(trace)?;
    }
    if let Some(ref span) = span_id {
        validate_span_id(span)?;
    }

    let (start_time, end_time) =
        parse_time_range(&params, chrono::Duration::hours(DEFAULT_TIME_RANGE_HOURS));
    let (limit, offset) = parse_pagination(&params, DEFAULT_PROFILE_LIMIT, MAX_PROFILE_LIMIT);

    // Build WHERE clause
    let mut where_clauses = vec![format!("project_id = '{}'", project_id)];

    if let Some(ref service) = service_name {
        where_clauses.push(format!(
            "service_name = '{}'",
            escape_clickhouse_string(service)
        ));
    }

    if let Some(ref trace) = trace_id {
        where_clauses.push(format!("trace_id = '{}'", escape_clickhouse_string(trace)));
    }

    if let Some(ref span) = span_id {
        where_clauses.push(format!("span_id = '{}'", escape_clickhouse_string(span)));
    }

    if let Some(pt) = params.get("profile_type").filter(|v| !v.is_empty()) {
        where_clauses.push(format!(
            "period_type = '{}'",
            escape_clickhouse_string(pt)
        ));
    }

    where_clauses.push(format!(
        "timestamp >= parseDateTime64BestEffort('{}')",
        start_time.to_rfc3339()
    ));
    where_clauses.push(format!(
        "timestamp <= parseDateTime64BestEffort('{}')",
        end_time.to_rfc3339()
    ));

    for (param_key, param_val) in &params {
        if let Some(attr_key) = param_key.strip_prefix("attr.") {
            if !is_valid_attribute_key(attr_key) || param_val.trim().is_empty() {
                continue;
            }
            let values: Vec<&str> = param_val
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect();
            if values.is_empty() {
                continue;
            }
            let in_list = values
                .iter()
                .map(|v| format!("'{}'", escape_clickhouse_string(v)))
                .collect::<Vec<_>>()
                .join(",");
            where_clauses.push(format!(
                "attributes['{}'] IN ({})",
                escape_clickhouse_string(attr_key),
                in_list
            ));
        }
    }

    let where_clause = where_clauses.join(" AND ");

    #[derive(clickhouse::Row, serde::Deserialize)]
    #[allow(dead_code)]
    struct ProfileRow {
        id: String,
        project_id: String,
        service_name: String,
        trace_id: Option<String>,
        span_id: Option<String>,
        profile_id: String,
        time_unix_nano: u64,
        duration_nano: u64,
        period_type: String,
        period: i64,
        sample_count: u64,
        #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
        timestamp: chrono::DateTime<Utc>,
        attributes: HashMap<String, String>,
    }

    let query = format!(
        r#"
        SELECT 
            id, project_id, service_name, trace_id, span_id, profile_id,
            time_unix_nano, duration_nano, period_type, period, sample_count, timestamp,
            attributes
        FROM reiver.profiles
        WHERE {}
        ORDER BY timestamp DESC
        LIMIT ? OFFSET ?
        "#,
        where_clause
    );

    let profiles: Vec<ProfileRow> = state
        .clickhouse
        .as_ref()
        .query(&query)
        .bind(limit as u64)
        .bind(offset)
        .fetch_all()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

    let profiles_json: Vec<serde_json::Value> = profiles
        .into_iter()
        .map(|p| {
            serde_json::json!({
                "id": p.id,
                "profile_id": p.profile_id,
                "service_name": p.service_name,
                "trace_id": p.trace_id,
                "span_id": p.span_id,
                "time_unix_nano": p.time_unix_nano,
                "duration_nano": p.duration_nano,
                "period_type": p.period_type,
                "period": p.period,
                "sample_count": p.sample_count,
                "timestamp": p.timestamp,
                "attributes": p.attributes,
            })
        })
        .collect();

    Ok(Json(serde_json::json!({
        "profiles": profiles_json,
        "total": profiles_json.len(),
    })))
}

/// Get profile details by profile_id
/// GET /api/profiles/projects/{project_id}/profiles/{profile_id}
async fn get_profile(
    State(state): State<Arc<WatchState>>,
    Path((project_id, profile_id)): Path<(Uuid, String)>,
) -> Result<Json<serde_json::Value>> {
    // Validate profile_id
    validate_profile_id(&profile_id)?;

    // Query profile from ClickHouse
    #[derive(clickhouse::Row, serde::Deserialize)]
    #[allow(dead_code)] // project_id field included in SELECT but not used in response
    struct ProfileDetailRow {
        id: String,
        project_id: String,
        service_name: String,
        service_version: String,
        trace_id: Option<String>,
        span_id: Option<String>,
        profile_id: String,
        time_unix_nano: u64,
        duration_nano: u64,
        period_type: String,
        period: i64,
        sample_count: u64,
        profile_data: String,
        dictionary_data: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
        timestamp: chrono::DateTime<Utc>,
    }

    let query = format!(
        r#"
        SELECT 
            id, project_id, service_name, service_version, trace_id, span_id, profile_id,
            time_unix_nano, duration_nano, period_type, period, sample_count,
            profile_data, dictionary_data, timestamp
        FROM reiver.profiles
        WHERE project_id = '{}' AND profile_id = '{}'
        ORDER BY timestamp DESC
        LIMIT 1
        "#,
        project_id,
        escape_clickhouse_string(&profile_id)
    );

    let profile: Option<ProfileDetailRow> = state
        .clickhouse
        .as_ref()
        .query(&query)
        .fetch_optional()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

    let profile =
        profile.ok_or_else(|| AppError::NotFound(format!("Profile {} not found", profile_id)))?;

    // Generate flame graph from profile data (supports both protobuf and legacy JSON)
    let flame_graph = flamegraph::generate_flame_graph(
        profile.profile_data.as_bytes(),
        profile.dictionary_data.as_bytes(),
    )
    .ok()
    .map(|fg| serde_json::to_value(&fg).unwrap_or(serde_json::json!(null)));

    Ok(Json(serde_json::json!({
        "id": profile.id,
        "profile_id": profile.profile_id,
        "service_name": profile.service_name,
        "service_version": profile.service_version,
        "trace_id": profile.trace_id,
        "span_id": profile.span_id,
        "time_unix_nano": profile.time_unix_nano,
        "duration_nano": profile.duration_nano,
        "period_type": profile.period_type,
        "period": profile.period,
        "sample_count": profile.sample_count,
        "timestamp": profile.timestamp,
        "flame_graph": flame_graph,
    })))
}

/// Get profile for a specific trace
/// GET /api/profiles/projects/{project_id}/traces/{trace_id}/profile
async fn get_profile_for_trace(
    State(state): State<Arc<WatchState>>,
    Path((project_id, trace_id)): Path<(Uuid, String)>,
) -> Result<Json<serde_json::Value>> {
    // Validate trace_id
    validate_trace_id(&trace_id)?;

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct ProfileRow {
        id: String,
        profile_id: String,
        service_name: String,
        sample_count: u64,
        duration_nano: u64,
        profile_data: String,
        dictionary_data: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
        timestamp: chrono::DateTime<Utc>,
    }

    let query = format!(
        r#"
        SELECT 
            id, profile_id, service_name, sample_count, duration_nano,
            profile_data, dictionary_data, timestamp
        FROM reiver.profiles
        WHERE project_id = '{}' AND trace_id = '{}'
        ORDER BY timestamp DESC
        LIMIT 1
        "#,
        project_id,
        escape_clickhouse_string(&trace_id)
    );

    let profile: Option<ProfileRow> = state
        .clickhouse
        .as_ref()
        .query(&query)
        .fetch_optional()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

    if let Some(profile) = profile {
        let flame_graph = flamegraph::generate_flame_graph(
            profile.profile_data.as_bytes(),
            profile.dictionary_data.as_bytes(),
        )
        .ok()
        .map(|fg| serde_json::to_value(&fg).unwrap_or(serde_json::json!(null)));

        Ok(Json(serde_json::json!({
            "trace_id": trace_id,
            "profile": {
                "id": profile.id,
                "profile_id": profile.profile_id,
                "service_name": profile.service_name,
                "sample_count": profile.sample_count,
                "duration_nano": profile.duration_nano,
                "timestamp": profile.timestamp,
            },
            "flame_graph": flame_graph,
        })))
    } else {
        Ok(Json(serde_json::json!({
            "trace_id": trace_id,
            "profile": null,
            "flame_graph": null,
        })))
    }
}

/// List profiles for a service
/// GET /api/profiles/projects/{project_id}/services/{service}/profiles
/// Query params: start_time, end_time, limit, offset
async fn list_service_profiles(
    State(state): State<Arc<WatchState>>,
    Path((project_id, service_name)): Path<(Uuid, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<serde_json::Value>> {
    // Validate service name
    validate_service_name(&service_name)?;

    let (start_time, end_time) =
        parse_time_range(&params, chrono::Duration::hours(DEFAULT_TIME_RANGE_HOURS));
    let (limit, offset) = parse_pagination(&params, DEFAULT_PROFILE_LIMIT, MAX_PROFILE_LIMIT);

    // Query profiles from ClickHouse
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct ProfileRow {
        id: String,
        profile_id: String,
        sample_count: u64,
        duration_nano: u64,
        #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
        timestamp: chrono::DateTime<Utc>,
    }

    let query = format!(
        r#"
        SELECT 
            id, profile_id, sample_count, duration_nano, timestamp
        FROM reiver.profiles
        WHERE project_id = '{}' AND service_name = '{}'
          AND timestamp >= parseDateTime64BestEffort('{}')
          AND timestamp <= parseDateTime64BestEffort('{}')
        ORDER BY timestamp DESC
        LIMIT ? OFFSET ?
        "#,
        project_id,
        escape_clickhouse_string(&service_name),
        start_time.to_rfc3339(),
        end_time.to_rfc3339()
    );

    let profiles: Vec<ProfileRow> = state
        .clickhouse
        .as_ref()
        .query(&query)
        .bind(limit as u64)
        .bind(offset)
        .fetch_all()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

    let profiles_json: Vec<serde_json::Value> = profiles
        .into_iter()
        .map(|p| {
            serde_json::json!({
                "id": p.id,
                "profile_id": p.profile_id,
                "sample_count": p.sample_count,
                "duration_nano": p.duration_nano,
                "timestamp": p.timestamp,
            })
        })
        .collect();

    Ok(Json(serde_json::json!({
        "service": service_name,
        "profiles": profiles_json,
        "total": profiles_json.len(),
    })))
}

// ============================================================================
// Aggregate Profiles Endpoint
// ============================================================================

/// GET .../services/{service}/profiles/aggregate
/// Query params: start_time, end_time, version (optional), profile_type (optional)
async fn aggregate_profiles(
    State(state): State<Arc<WatchState>>,
    Path((project_id, service_name)): Path<(Uuid, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<serde_json::Value>> {
    validate_service_name(&service_name)?;

    let (start_time, end_time) =
        parse_time_range(&params, chrono::Duration::hours(DEFAULT_TIME_RANGE_HOURS));

    let mut where_clause = format!(
        "project_id = '{}' AND service_name = '{}' AND timestamp >= parseDateTime64BestEffort('{}') AND timestamp <= parseDateTime64BestEffort('{}')",
        project_id,
        escape_clickhouse_string(&service_name),
        start_time.to_rfc3339(),
        end_time.to_rfc3339()
    );

    if let Some(version) = params.get("version").filter(|v| !v.is_empty()) {
        where_clause.push_str(&format!(
            " AND service_version = '{}'",
            escape_clickhouse_string(version)
        ));
    }
    if let Some(pt) = params.get("profile_type").filter(|v| !v.is_empty()) {
        where_clause.push_str(&format!(
            " AND period_type = '{}'",
            escape_clickhouse_string(pt)
        ));
    }

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct RawProfileRow {
        profile_data: String,
        dictionary_data: String,
    }

    let query = format!(
        r#"SELECT profile_data, dictionary_data
        FROM reiver.profiles
        WHERE {}
        ORDER BY timestamp DESC
        LIMIT 100"#,
        where_clause
    );

    let rows: Vec<RawProfileRow> = state
        .clickhouse
        .as_ref()
        .query(&query)
        .fetch_all()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

    if rows.is_empty() {
        return Err(AppError::NotFound("No profiles found".to_string()));
    }

    let mut trees = Vec::new();
    let mut total_value: u64 = 0;
    for row in &rows {
        if let Ok(fg) = flamegraph::generate_flame_graph(
            row.profile_data.as_bytes(),
            row.dictionary_data.as_bytes(),
        ) {
            total_value += fg.total_value;
            trees.push(fg.root);
        }
    }

    if trees.is_empty() {
        return Err(AppError::Internal(anyhow::anyhow!(
            "Failed to build any flame graphs"
        )));
    }

    let merged_root = flamegraph::merge_flame_graphs(trees);

    Ok(Json(serde_json::json!({
        "flame_graph": {
            "root": merged_root,
            "total_value": total_value,
            "profile_count": rows.len(),
        }
    })))
}

// ============================================================================
// Diff Profiles Endpoint
// ============================================================================

/// GET .../services/{service}/profiles/diff
/// Query params: version1, version2, start_time, end_time
async fn diff_profiles(
    State(state): State<Arc<WatchState>>,
    Path((project_id, service_name)): Path<(Uuid, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<serde_json::Value>> {
    validate_service_name(&service_name)?;

    let version1 = params
        .get("version1")
        .ok_or_else(|| AppError::BadRequest("Missing version1 parameter".to_string()))?;
    let version2 = params
        .get("version2")
        .ok_or_else(|| AppError::BadRequest("Missing version2 parameter".to_string()))?;
    validate_version(version1)?;
    validate_version(version2)?;

    let (start_time, end_time) =
        parse_time_range(&params, chrono::Duration::days(DEFAULT_COMPARISON_DAYS));

    let profile_type_filter = params
        .get("profile_type")
        .filter(|v| !v.is_empty())
        .map(|pt| {
            format!(
                " AND period_type = '{}'",
                escape_clickhouse_string(pt)
            )
        })
        .unwrap_or_default();

    let fetch_version = |version: &str| {
        let query = format!(
            r#"SELECT profile_data, dictionary_data
            FROM reiver.profiles
            WHERE project_id = '{}' AND service_name = '{}' AND service_version = '{}'
              AND timestamp >= parseDateTime64BestEffort('{}')
              AND timestamp <= parseDateTime64BestEffort('{}'){}
            ORDER BY timestamp DESC
            LIMIT 100"#,
            project_id,
            escape_clickhouse_string(&service_name),
            escape_clickhouse_string(version),
            start_time.to_rfc3339(),
            end_time.to_rfc3339(),
            profile_type_filter,
        );
        query
    };

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct RawRow {
        profile_data: String,
        dictionary_data: String,
    }

    let q1 = fetch_version(version1);
    let q2 = fetch_version(version2);

    let (rows_a, rows_b) = tokio::try_join!(
        async {
            state
                .clickhouse
                .as_ref()
                .query(&q1)
                .fetch_all::<RawRow>()
                .await
                .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))
        },
        async {
            state
                .clickhouse
                .as_ref()
                .query(&q2)
                .fetch_all::<RawRow>()
                .await
                .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))
        }
    )?;

    let build_merged = |rows: &[RawRow]| -> Option<flamegraph::FlameGraphNode> {
        let trees: Vec<_> = rows
            .iter()
            .filter_map(|r| {
                flamegraph::generate_flame_graph(
                    r.profile_data.as_bytes(),
                    r.dictionary_data.as_bytes(),
                )
                .ok()
                .map(|fg| fg.root)
            })
            .collect();
        if trees.is_empty() {
            None
        } else {
            Some(flamegraph::merge_flame_graphs(trees))
        }
    };

    let merged_a = build_merged(&rows_a)
        .ok_or_else(|| AppError::NotFound(format!("No profiles for version {}", version1)))?;
    let merged_b = build_merged(&rows_b)
        .ok_or_else(|| AppError::NotFound(format!("No profiles for version {}", version2)))?;

    let diff = flamegraph::diff_flame_graphs(&merged_a, &merged_b);
    let stats_comparison = {
        let time_params: HashMap<String, String> = [
            ("start_time".to_string(), start_time.to_rfc3339()),
            ("end_time".to_string(), end_time.to_rfc3339()),
        ]
        .into_iter()
        .collect();
        let (st, et) = parse_time_range(&time_params, chrono::Duration::days(DEFAULT_COMPARISON_DAYS));
        let v1_stats = query_version_stats(&state, project_id, &service_name, version1, &st, &et).await?;
        let v2_stats = query_version_stats(&state, project_id, &service_name, version2, &st, &et).await?;
        let diff_stats = calculate_diff(&v1_stats, &v2_stats);
        serde_json::json!({
            "version1": v1_stats,
            "version2": v2_stats,
            "diff": diff_stats,
        })
    };

    Ok(Json(serde_json::json!({
        "diff_flame_graph": diff,
        "stats_comparison": stats_comparison,
    })))
}

// ============================================================================
// Top Functions Endpoint
// ============================================================================

/// GET .../services/{service}/profiles/top-functions
/// Query params: start_time, end_time, profile_type, limit, timeseries (bool)
async fn top_functions(
    State(state): State<Arc<WatchState>>,
    Path((project_id, service_name)): Path<(Uuid, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<serde_json::Value>> {
    validate_service_name(&service_name)?;

    let (start_time, end_time) =
        parse_time_range(&params, chrono::Duration::hours(DEFAULT_TIME_RANGE_HOURS));
    let limit: u32 = params
        .get("limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(10)
        .min(100);
    let profile_type = params
        .get("profile_type")
        .filter(|v| !v.is_empty())
        .cloned();

    let mut label_filters = String::new();
    for (param_key, param_val) in &params {
        if let Some(attr_key) = param_key.strip_prefix("attr.") {
            if !is_valid_attribute_key(attr_key) || param_val.trim().is_empty() {
                continue;
            }
            let values: Vec<&str> = param_val
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect();
            if values.is_empty() {
                continue;
            }
            let in_list = values
                .iter()
                .map(|v| format!("'{}'", escape_clickhouse_string(v)))
                .collect::<Vec<_>>()
                .join(",");
            label_filters.push_str(&format!(
                " AND labels['{}'] IN ({})",
                escape_clickhouse_string(attr_key),
                in_list
            ));
        }
    }

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct TopRow {
        function_name: String,
        total_samples: i64,
    }

    let profile_type_clause = match &profile_type {
        Some(pt) => format!(" AND profile_type = '{}'", escape_clickhouse_string(pt)),
        None => String::new(),
    };

    let top_query = format!(
        r#"SELECT function_name, sum(value) AS total_samples
        FROM reiver.profile_samples
        WHERE project_id = '{}' AND service_name = '{}'{}
          AND timestamp >= parseDateTime64BestEffort('{}')
          AND timestamp <  parseDateTime64BestEffort('{}'){}
        GROUP BY function_name
        ORDER BY total_samples DESC
        LIMIT {}"#,
        project_id,
        escape_clickhouse_string(&service_name),
        profile_type_clause,
        start_time.to_rfc3339(),
        end_time.to_rfc3339(),
        label_filters,
        limit,
    );

    let top_rows: Vec<TopRow> = state
        .clickhouse
        .as_ref()
        .query(&top_query)
        .fetch_all()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

    let show_timeseries = params
        .get("timeseries")
        .map(|s| s == "true" || s == "1")
        .unwrap_or(false);

    let mut timeseries_data = serde_json::json!(null);

    if show_timeseries && !top_rows.is_empty() {
        let function_names: Vec<String> = top_rows.iter().map(|r| r.function_name.clone()).collect();
        let in_clause: String = function_names
            .iter()
            .map(|f| format!("'{}'", escape_clickhouse_string(f)))
            .collect::<Vec<_>>()
            .join(",");

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct TimeSeriesRow {
            function_name: String,
            #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
            bucket: chrono::DateTime<Utc>,
            samples: i64,
        }

        let ts_query = format!(
            r#"SELECT function_name,
                      toDateTime64(toStartOfInterval(timestamp, INTERVAL 5 MINUTE), 3) AS bucket,
                      sum(value) AS samples
            FROM reiver.profile_samples
            WHERE project_id = '{}' AND service_name = '{}' AND function_name IN ({})
              AND timestamp >= parseDateTime64BestEffort('{}')
              AND timestamp <  parseDateTime64BestEffort('{}')
            GROUP BY function_name, bucket
            ORDER BY bucket"#,
            project_id,
            escape_clickhouse_string(&service_name),
            in_clause,
            start_time.to_rfc3339(),
            end_time.to_rfc3339(),
        );

        let ts_rows: Vec<TimeSeriesRow> = state
            .clickhouse
            .as_ref()
            .query(&ts_query)
            .fetch_all()
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

        let mut grouped: std::collections::HashMap<String, Vec<serde_json::Value>> =
            std::collections::HashMap::new();
        for row in ts_rows {
            grouped
                .entry(row.function_name)
                .or_default()
                .push(serde_json::json!({
                    "timestamp": row.bucket,
                    "samples": row.samples,
                }));
        }
        timeseries_data = serde_json::json!(grouped);
    }

    let top_json: Vec<serde_json::Value> = top_rows
        .into_iter()
        .map(|r| {
            serde_json::json!({
                "function_name": r.function_name,
                "total_samples": r.total_samples,
            })
        })
        .collect();

    Ok(Json(serde_json::json!({
        "service": service_name,
        "profile_type": profile_type,
        "functions": top_json,
        "timeseries": timeseries_data,
    })))
}

/// GET .../profiles/top-functions
/// Project-level top functions — service_name is optional query param.
async fn top_functions_project(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<serde_json::Value>> {
    let (start_time, end_time) =
        parse_time_range(&params, chrono::Duration::hours(DEFAULT_TIME_RANGE_HOURS));
    let limit: u32 = params
        .get("limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(10)
        .min(100);

    let service_name = params
        .get("service_name")
        .or_else(|| params.get("service"))
        .filter(|v| !v.is_empty())
        .cloned();

    let profile_type = params
        .get("profile_type")
        .filter(|v| !v.is_empty())
        .cloned();

    let mut extra_filters = String::new();

    if let Some(ref svc) = service_name {
        extra_filters.push_str(&format!(
            " AND service_name = '{}'",
            escape_clickhouse_string(svc)
        ));
    }
    if let Some(ref pt) = profile_type {
        extra_filters.push_str(&format!(
            " AND profile_type = '{}'",
            escape_clickhouse_string(pt)
        ));
    }

    for (param_key, param_val) in &params {
        if let Some(attr_key) = param_key.strip_prefix("attr.") {
            if !is_valid_attribute_key(attr_key) || param_val.trim().is_empty() {
                continue;
            }
            let values: Vec<&str> = param_val
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect();
            if values.is_empty() {
                continue;
            }
            let in_list = values
                .iter()
                .map(|v| format!("'{}'", escape_clickhouse_string(v)))
                .collect::<Vec<_>>()
                .join(",");
            extra_filters.push_str(&format!(
                " AND labels['{}'] IN ({})",
                escape_clickhouse_string(attr_key),
                in_list
            ));
        }
    }

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct TopRow {
        function_name: String,
        total_samples: i64,
    }

    let top_query = format!(
        r#"SELECT function_name, sum(value) AS total_samples
        FROM reiver.profile_samples
        WHERE project_id = '{}'{}
          AND timestamp >= parseDateTime64BestEffort('{}')
          AND timestamp <  parseDateTime64BestEffort('{}')
        GROUP BY function_name
        ORDER BY total_samples DESC
        LIMIT {}"#,
        project_id,
        extra_filters,
        start_time.to_rfc3339(),
        end_time.to_rfc3339(),
        limit,
    );

    let top_rows: Vec<TopRow> = state
        .clickhouse
        .as_ref()
        .query(&top_query)
        .fetch_all()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

    let show_timeseries = params
        .get("timeseries")
        .map(|s| s == "true" || s == "1")
        .unwrap_or(false);

    let mut timeseries_data = serde_json::json!(null);

    if show_timeseries && !top_rows.is_empty() {
        let function_names: Vec<String> = top_rows.iter().map(|r| r.function_name.clone()).collect();
        let in_clause: String = function_names
            .iter()
            .map(|f| format!("'{}'", escape_clickhouse_string(f)))
            .collect::<Vec<_>>()
            .join(",");

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct TimeSeriesRow {
            function_name: String,
            #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
            bucket: chrono::DateTime<Utc>,
            samples: i64,
        }

        let ts_query = format!(
            r#"SELECT function_name,
                      toDateTime64(toStartOfInterval(timestamp, INTERVAL 5 MINUTE), 3) AS bucket,
                      sum(value) AS samples
            FROM reiver.profile_samples
            WHERE project_id = '{}'{} AND function_name IN ({})
              AND timestamp >= parseDateTime64BestEffort('{}')
              AND timestamp <  parseDateTime64BestEffort('{}')
            GROUP BY function_name, bucket
            ORDER BY bucket"#,
            project_id,
            if let Some(ref svc) = service_name {
                format!(" AND service_name = '{}'", escape_clickhouse_string(svc))
            } else {
                String::new()
            },
            in_clause,
            start_time.to_rfc3339(),
            end_time.to_rfc3339(),
        );

        let ts_rows: Vec<TimeSeriesRow> = state
            .clickhouse
            .as_ref()
            .query(&ts_query)
            .fetch_all()
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

        let mut grouped: std::collections::HashMap<String, Vec<serde_json::Value>> =
            std::collections::HashMap::new();
        for row in ts_rows {
            grouped
                .entry(row.function_name)
                .or_default()
                .push(serde_json::json!({
                    "timestamp": row.bucket,
                    "samples": row.samples,
                }));
        }
        timeseries_data = serde_json::json!(grouped);
    }

    let top_json: Vec<serde_json::Value> = top_rows
        .into_iter()
        .map(|r| {
            serde_json::json!({
                "function_name": r.function_name,
                "total_samples": r.total_samples,
            })
        })
        .collect();

    Ok(Json(serde_json::json!({
        "service": service_name,
        "profile_type": profile_type,
        "functions": top_json,
        "timeseries": timeseries_data,
    })))
}

// ============================================================================
// Profile Download Endpoint
// ============================================================================

/// GET .../profiles/{profile_id}/download
/// Returns raw profile data with Content-Type: application/x-protobuf
async fn download_profile(
    State(state): State<Arc<WatchState>>,
    Path((project_id, profile_id)): Path<(Uuid, String)>,
) -> Result<axum::response::Response> {
    validate_profile_id(&profile_id)?;

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct RawRow {
        profile_data: String,
    }

    let query = format!(
        r#"SELECT profile_data
        FROM reiver.profiles
        WHERE project_id = '{}' AND profile_id = '{}'
        ORDER BY timestamp DESC
        LIMIT 1"#,
        project_id,
        escape_clickhouse_string(&profile_id)
    );

    let row: Option<RawRow> = state
        .clickhouse
        .as_ref()
        .query(&query)
        .fetch_optional()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

    let row =
        row.ok_or_else(|| AppError::NotFound(format!("Profile {} not found", profile_id)))?;

    use base64::Engine as _;
    let raw_bytes = base64::engine::general_purpose::STANDARD
        .decode(row.profile_data.as_bytes())
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to decode profile data: {}", e)))?;

    let body = axum::body::Body::from(raw_bytes);
    let response = axum::response::Response::builder()
        .header("Content-Type", "application/x-protobuf")
        .header(
            "Content-Disposition",
            format!("attachment; filename=\"{}.pb\"", profile_id),
        )
        .body(body)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Response build error: {}", e)))?;

    Ok(response)
}

// ============================================================================
// Attribute Discovery Endpoints
// ============================================================================

fn is_valid_attribute_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 256
        && key
            .chars()
            .all(|c| c.is_alphanumeric() || c == '.' || c == '_' || c == '-' || c == '/')
}

/// GET .../profiles/attribute-keys
/// Returns distinct attribute keys from the profiles Map column.
async fn list_profile_attribute_keys(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Vec<String>>> {
    let (start_time, _end_time) =
        parse_time_range(&params, chrono::Duration::days(DEFAULT_VERSION_LISTING_DAYS));

    let query = format!(
        r#"SELECT DISTINCT key
        FROM (
            SELECT arrayJoin(mapKeys(attributes)) AS key
            FROM reiver.profiles
            WHERE project_id = '{}' AND timestamp >= parseDateTime64BestEffort('{}')
            LIMIT 10000
        )
        ORDER BY key
        LIMIT 200"#,
        project_id,
        start_time.to_rfc3339()
    );

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct KeyRow {
        key: String,
    }

    let rows: Vec<KeyRow> = state
        .clickhouse
        .as_ref()
        .query(&query)
        .fetch_all()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

    Ok(Json(rows.into_iter().map(|r| r.key).collect()))
}

/// GET .../profiles/attribute-values?key=...
/// Returns distinct values for a given attribute key.
async fn list_profile_attribute_values(
    State(state): State<Arc<WatchState>>,
    Path(project_id): Path<Uuid>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Vec<String>>> {
    let key = params
        .get("key")
        .filter(|k| !k.is_empty())
        .ok_or_else(|| AppError::BadRequest("Missing 'key' parameter".to_string()))?;

    if !is_valid_attribute_key(key) {
        return Err(AppError::BadRequest("Invalid attribute key".to_string()));
    }

    let (start_time, _end_time) =
        parse_time_range(&params, chrono::Duration::days(DEFAULT_VERSION_LISTING_DAYS));

    let query = format!(
        r#"SELECT DISTINCT attributes['{}'] AS value
        FROM reiver.profiles
        WHERE project_id = '{}' AND attributes['{}'] != ''
          AND timestamp >= parseDateTime64BestEffort('{}')
        ORDER BY value
        LIMIT 100"#,
        escape_clickhouse_string(key),
        project_id,
        escape_clickhouse_string(key),
        start_time.to_rfc3339()
    );

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct ValueRow {
        value: String,
    }

    let rows: Vec<ValueRow> = state
        .clickhouse
        .as_ref()
        .query(&query)
        .fetch_all()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

    Ok(Json(rows.into_iter().map(|r| r.value).collect()))
}

/// Query parameters for comparing profiles between two deployment versions.
///
/// Used by the `/profiles/comparison` endpoint to specify which versions to compare
/// and the time range for the comparison.
#[derive(serde::Deserialize)]
struct CompareParams {
    /// First version identifier (baseline for comparison)
    version1: String,
    /// Second version identifier (compared against baseline)
    version2: String,
    /// Optional start time in RFC3339 format (defaults to 7 days ago)
    start_time: Option<String>,
    /// Optional end time in RFC3339 format (defaults to now)
    end_time: Option<String>,
}

/// Aggregated statistics for profiles of a specific service version.
///
/// Contains metrics useful for understanding the performance characteristics
/// of a particular deployment version, including sample counts and duration statistics.
#[derive(serde::Serialize)]
struct ProfileVersionStats {
    /// The version identifier (e.g., "1.2.3", "abc123")
    version: String,
    /// Total number of profiles collected for this version
    profile_count: u64,
    /// Sum of all samples across all profiles
    total_samples: u64,
    /// Average profile duration in nanoseconds
    avg_duration_nano: f64,
    /// Sum of all profile durations in nanoseconds
    total_duration_nano: u64,
    /// Minimum profile duration observed in nanoseconds
    min_duration_nano: u64,
    /// Maximum profile duration observed in nanoseconds
    max_duration_nano: u64,
}

/// Result of comparing performance profiles between two deployment versions.
///
/// Contains statistics for both versions and a diff showing the changes,
/// useful for identifying performance regressions or improvements between deployments.
#[derive(serde::Serialize)]
struct ProfileComparison {
    /// The service name being compared
    service: String,
    /// Statistics for the first (baseline) version
    version1: ProfileVersionStats,
    /// Statistics for the second (comparison) version
    version2: ProfileVersionStats,
    /// Calculated differences between the two versions
    diff: ProfileDiff,
}

/// Calculated differences between two profile version statistics.
///
/// Contains both absolute differences and percentage changes for key metrics,
/// making it easy to identify significant performance changes between versions.
#[derive(serde::Serialize)]
struct ProfileDiff {
    /// Absolute difference in profile count (v2 - v1)
    profile_count_diff: i64,
    /// Percentage change in profile count ((v2 - v1) / v1 * 100)
    profile_count_pct_change: f64,
    /// Absolute difference in total samples (v2 - v1)
    total_samples_diff: i64,
    /// Percentage change in total samples
    total_samples_pct_change: f64,
    /// Absolute difference in average duration (nanoseconds)
    avg_duration_nano_diff: f64,
    /// Percentage change in average duration
    avg_duration_pct_change: f64,
}

/// Compare profiles between two versions
/// GET /api/profiles/projects/{project_id}/services/{service}/profiles/comparison
/// Query params: version1, version2, start_time, end_time
async fn compare_profiles(
    State(state): State<Arc<WatchState>>,
    Path((project_id, service_name)): Path<(Uuid, String)>,
    Query(params): Query<CompareParams>,
) -> Result<Json<ProfileComparison>> {
    // Validate inputs
    validate_service_name(&service_name)?;
    validate_version(&params.version1)?;
    validate_version(&params.version2)?;

    // Get time range using a HashMap conversion for the helper
    let time_params: HashMap<String, String> = [
        (
            "start_time".to_string(),
            params.start_time.clone().unwrap_or_default(),
        ),
        (
            "end_time".to_string(),
            params.end_time.clone().unwrap_or_default(),
        ),
    ]
    .into_iter()
    .filter(|(_, v)| !v.is_empty())
    .collect();

    let (start_time, end_time) = parse_time_range(
        &time_params,
        chrono::Duration::days(DEFAULT_COMPARISON_DAYS),
    );

    // Query aggregated stats for version1
    let stats_v1 = query_version_stats(
        &state,
        project_id,
        &service_name,
        &params.version1,
        &start_time,
        &end_time,
    )
    .await?;

    // Query aggregated stats for version2
    let stats_v2 = query_version_stats(
        &state,
        project_id,
        &service_name,
        &params.version2,
        &start_time,
        &end_time,
    )
    .await?;

    // Calculate diff
    let diff = calculate_diff(&stats_v1, &stats_v2);

    Ok(Json(ProfileComparison {
        service: service_name,
        version1: stats_v1,
        version2: stats_v2,
        diff,
    }))
}

/// Query profile statistics for a specific version
///
/// Uses the pre-aggregated `profile_version_stats` materialized view for performance.
/// The MV aggregates data by hour, so we sum the hourly buckets for the requested range.
/// Note: min/max duration are not available from the MV, so we estimate from avg.
async fn query_version_stats(
    state: &Arc<WatchState>,
    project_id: Uuid,
    service_name: &str,
    version: &str,
    start_time: &chrono::DateTime<Utc>,
    end_time: &chrono::DateTime<Utc>,
) -> Result<ProfileVersionStats> {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct StatsRow {
        profile_count: u64,
        total_samples: u64,
        total_duration_nano: u64,
    }

    // Query from the pre-aggregated materialized view table for better performance
    // The MV aggregates by hour, so we sum the hourly buckets
    let query = format!(
        r#"
        SELECT 
            sum(profile_count) AS profile_count,
            sum(total_samples) AS total_samples,
            sum(total_duration_nano) AS total_duration_nano
        FROM reiver.profile_version_stats
        WHERE project_id = '{}'
          AND service_name = '{}'
          AND service_version = '{}'
          AND hour >= toStartOfHour(parseDateTime64BestEffort('{}'))
          AND hour <= toStartOfHour(parseDateTime64BestEffort('{}'))
        "#,
        project_id,
        escape_clickhouse_string(service_name),
        escape_clickhouse_string(version),
        start_time.to_rfc3339(),
        end_time.to_rfc3339()
    );

    let stats: Option<StatsRow> = state
        .clickhouse
        .as_ref()
        .query(&query)
        .fetch_optional()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

    match stats {
        Some(s) if s.profile_count > 0 => {
            let avg_duration_nano = s.total_duration_nano as f64 / s.profile_count as f64;
            Ok(ProfileVersionStats {
                version: version.to_string(),
                profile_count: s.profile_count,
                total_samples: s.total_samples,
                avg_duration_nano,
                total_duration_nano: s.total_duration_nano,
                // Min/max not available from MV, use avg as estimate
                min_duration_nano: avg_duration_nano as u64,
                max_duration_nano: avg_duration_nano as u64,
            })
        }
        _ => Ok(ProfileVersionStats {
            version: version.to_string(),
            profile_count: 0,
            total_samples: 0,
            avg_duration_nano: 0.0,
            total_duration_nano: 0,
            min_duration_nano: 0,
            max_duration_nano: 0,
        }),
    }
}

/// Calculate difference between two version stats
///
/// Handles edge cases where baseline (v1) is zero:
/// - If v1 == 0 and v2 > 0: returns f64::INFINITY (represents "new data")
/// - If v1 == 0 and v2 == 0: returns 0.0 (no change)
fn calculate_diff(v1: &ProfileVersionStats, v2: &ProfileVersionStats) -> ProfileDiff {
    let profile_count_diff = v2.profile_count as i64 - v1.profile_count as i64;
    let profile_count_pct_change =
        calculate_pct_change(v1.profile_count as f64, v2.profile_count as f64);

    let total_samples_diff = v2.total_samples as i64 - v1.total_samples as i64;
    let total_samples_pct_change =
        calculate_pct_change(v1.total_samples as f64, v2.total_samples as f64);

    let avg_duration_nano_diff = v2.avg_duration_nano - v1.avg_duration_nano;
    let avg_duration_pct_change = calculate_pct_change(v1.avg_duration_nano, v2.avg_duration_nano);

    ProfileDiff {
        profile_count_diff,
        profile_count_pct_change,
        total_samples_diff,
        total_samples_pct_change,
        avg_duration_nano_diff,
        avg_duration_pct_change,
    }
}

/// Calculate percentage change between two values
///
/// Returns:
/// - Normal percentage if baseline > 0
/// - f64::INFINITY if baseline == 0 and comparison > 0 (new data)
/// - f64::NEG_INFINITY if baseline == 0 and comparison < 0 (shouldn't happen for counts)
/// - 0.0 if both are zero (no change)
fn calculate_pct_change(baseline: f64, comparison: f64) -> f64 {
    if baseline > 0.0 {
        ((comparison - baseline) / baseline) * 100.0
    } else if comparison > 0.0 {
        f64::INFINITY
    } else if comparison < 0.0 {
        f64::NEG_INFINITY
    } else {
        0.0
    }
}

/// Summary information about a service version observed in profile data.
///
/// Used when listing available versions for a service, providing enough
/// information to understand the time range and volume of data for each version.
#[derive(serde::Serialize)]
struct VersionInfo {
    /// The version identifier string
    version: String,
    /// Number of profiles collected for this version
    profile_count: u64,
    /// Timestamp of the first profile seen for this version
    first_seen: chrono::DateTime<Utc>,
    /// Timestamp of the most recent profile for this version
    last_seen: chrono::DateTime<Utc>,
}

/// List available profile versions for a service
/// GET /api/profiles/projects/{project_id}/services/{service}/profiles/versions
/// Query params: start_time, end_time, limit
async fn list_profile_versions(
    State(state): State<Arc<WatchState>>,
    Path((project_id, service_name)): Path<(Uuid, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<serde_json::Value>> {
    // Validate service name
    validate_service_name(&service_name)?;

    let (start_time, end_time) = parse_time_range(
        &params,
        chrono::Duration::days(DEFAULT_VERSION_LISTING_DAYS),
    );
    let (limit, _) = parse_pagination(&params, DEFAULT_VERSION_LIMIT, MAX_VERSION_LIMIT);

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct VersionRow {
        version: String,
        profile_count: u64,
        #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
        first_seen: chrono::DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
        last_seen: chrono::DateTime<Utc>,
    }

    let query = format!(
        r#"
        SELECT 
            service_version AS version,
            count() AS profile_count,
            min(timestamp) AS first_seen,
            max(timestamp) AS last_seen
        FROM reiver.profiles
        WHERE project_id = '{}'
          AND service_name = '{}'
          AND timestamp >= parseDateTime64BestEffort('{}')
          AND timestamp <= parseDateTime64BestEffort('{}')
        GROUP BY version
        HAVING version != ''
        ORDER BY last_seen DESC
        LIMIT {}
        "#,
        project_id,
        escape_clickhouse_string(&service_name),
        start_time.to_rfc3339(),
        end_time.to_rfc3339(),
        limit
    );

    let versions: Vec<VersionRow> = state
        .clickhouse
        .as_ref()
        .query(&query)
        .fetch_all()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse query failed: {}", e)))?;

    let versions_json: Vec<VersionInfo> = versions
        .into_iter()
        .map(|v| VersionInfo {
            version: v.version,
            profile_count: v.profile_count,
            first_seen: v.first_seen,
            last_seen: v.last_seen,
        })
        .collect();

    Ok(Json(serde_json::json!({
        "service": service_name,
        "versions": versions_json,
        "total": versions_json.len(),
    })))
}

/// Get profile statistics for a specific version
/// GET /api/profiles/projects/{project_id}/services/{service}/profiles/version/{version}
/// Query params: start_time, end_time
async fn get_version_stats(
    State(state): State<Arc<WatchState>>,
    Path((project_id, service_name, version)): Path<(Uuid, String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<ProfileVersionStats>> {
    // Validate inputs
    validate_service_name(&service_name)?;
    validate_version(&version)?;

    let (start_time, end_time) =
        parse_time_range(&params, chrono::Duration::days(DEFAULT_COMPARISON_DAYS));

    let stats = query_version_stats(
        &state,
        project_id,
        &service_name,
        &version,
        &start_time,
        &end_time,
    )
    .await?;

    Ok(Json(stats))
}

// ============================================================================
// Source File Endpoint
// ============================================================================

/// Maximum file path length for security
const MAX_FILE_PATH_LENGTH: usize = 1024;

/// Redis cache TTL for source file content (5 minutes)
const SOURCE_CACHE_TTL_SECONDS: u64 = 300;

/// Fetch source file contents from the project's linked GitHub repository.
///
/// GET /api/profiles/projects/{project_id}/source?file={path}&ref={git_ref}
///
/// Requires the project to have a linked GitHub repository via the Integrations page.
/// The file is fetched from GitHub using the GitHub App installation credentials.
/// Results are cached in Redis for 5 minutes to avoid rate-limiting the GitHub API
/// (5,000 req/hour per installation token).
async fn get_source_file(
    State(state): State<Arc<WatchState>>,
    headers: axum::http::HeaderMap,
    Path(project_id): Path<Uuid>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<serde_json::Value>> {
    let file_path = params
        .get("file")
        .ok_or_else(|| AppError::BadRequest("Missing 'file' query parameter".to_string()))?;
    let git_ref = params.get("ref").map(|s| s.as_str());

    // Validate file path length
    if file_path.len() > MAX_FILE_PATH_LENGTH {
        return Err(AppError::BadRequest("File path too long".to_string()));
    }

    let user_id = crate::api::extract_user_id(&headers)?;

    // Look up project's GitHub repo URL and verify access
    let project: Option<(Uuid, Option<String>)> = sqlx::query_as(
        r#"
        SELECT p.organization_id, p.github_repo_url FROM projects p
        JOIN memberships om ON p.organization_id = om.organization_id AND om.status = 'active'
        WHERE p.id = $1 AND om.user_id = $2
        "#,
    )
    .bind(project_id)
    .bind(user_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB query failed: {}", e)))?;

    let (org_id, repo_url) =
        project.ok_or_else(|| AppError::NotFound("Project not found".to_string()))?;

    let repo_url = repo_url
        .ok_or_else(|| AppError::BadRequest(
            "GitHub integration not configured for this project. Link a repository in Settings > Integrations.".to_string()
        ))?;

    // Parse owner/repo
    let (owner, repo) = crate::github::parse_repo_url(&repo_url).ok_or_else(|| {
        AppError::Internal(anyhow::anyhow!("Invalid repository URL stored for project"))
    })?;

    let full_name = format!("{}/{}", owner, repo);

    // Find the GitHub App installation with access to this repo
    let installation_id: i64 = sqlx::query_scalar(
        r#"
        SELECT installation_id FROM github_installations
        WHERE organization_id = $1
        AND repositories @> $2::jsonb
        LIMIT 1
        "#,
    )
    .bind(org_id)
    .bind(serde_json::json!([{"full_name": full_name}]))
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB query failed: {}", e)))?
    .ok_or_else(|| {
        AppError::BadRequest(format!(
        "No GitHub installation with access to repository '{}'. Ensure the GitHub App has access.",
        full_name
    ))
    })?;

    // Get GitHub service
    let github_service = state
        .github_service
        .as_ref()
        .map(|s| s.as_ref())
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("GitHub App not configured")))?;

    // Build cache key
    let ref_str = git_ref.unwrap_or("HEAD");
    let cache_key = format!("github:source:{}:{}:{}:{}", owner, repo, ref_str, file_path);

    // Try cache first
    if let Ok(mut conn) = state.redis.get().await {
        if let Ok(Some(cached_json)) =
            redis::AsyncCommands::get::<_, Option<String>>(&mut *conn, &cache_key).await
        {
            if let Ok(cached) = serde_json::from_str::<serde_json::Value>(&cached_json) {
                return Ok(Json(cached));
            }
        }
    }

    // Fetch from GitHub
    let file_contents = github_service
        .get_file_contents(installation_id as u64, &owner, &repo, file_path, git_ref)
        .await
        .map_err(|e| AppError::NotFound(format!("File not found: {}", e)))?;

    let response = serde_json::json!({
        "path": file_contents.path,
        "content": file_contents.content,
        "sha": file_contents.sha,
        "size": file_contents.size,
        "html_url": file_contents.html_url,
    });

    // Cache result (non-critical)
    if let Ok(json_str) = serde_json::to_string(&response) {
        if let Ok(mut conn) = state.redis.get().await {
            let _ = redis::AsyncCommands::set_ex::<_, _, ()>(
                &mut *conn,
                &cache_key,
                json_str,
                SOURCE_CACHE_TTL_SECONDS,
            )
            .await;
        }
    }

    Ok(Json(response))
}
