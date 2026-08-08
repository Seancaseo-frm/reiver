//! OTLP (OpenTelemetry Protocol) HTTP endpoints
//!
//! Handlers for:
//! - `/v1/traces` - Queues raw OTLP traces to Kafka
//! - `/v1/logs` - Queues raw OTLP logs to Kafka
//! - `/v1/metrics` - Queues raw OTLP metrics to Kafka
//! - `/v1/profiles` - Stores profiles directly in ClickHouse (development API)
//!
//! All parsing and processing happens in dedicated Kafka consumers.

use axum::{
    extract::{DefaultBodyLimit, State},
    http::StatusCode,
    routing::post,
    Router,
};
use clickhouse::Row;
use serde::Serialize;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

use crate::app_state::WatchState;
use crate::error::{AppError, Result};

const MAX_OTLP_BODY_SIZE: usize = 10 * 1024 * 1024; // 10 MB

pub fn create_otlp_router() -> Router<Arc<WatchState>> {
    Router::new()
        .route("/traces", post(otlp_http_traces))
        .route("/metrics", post(otlp_http_metrics))
        .route("/logs", post(otlp_http_logs))
        .route("/profiles", post(otlp_http_profiles))
        .layer(DefaultBodyLimit::max(MAX_OTLP_BODY_SIZE))
}

// ============================================================================
// Simplified Handlers (Queue to Kafka)
// ============================================================================

/// Handle OTLP HTTP traces endpoint
/// POST /v1/traces (Content-Type: application/x-protobuf or application/json)
/// Just validates auth, rate limits, and queues raw payload to Kafka
async fn otlp_http_traces(
    State(state): State<Arc<WatchState>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<StatusCode> {
    use crate::models::RawOtlpTracePayload;

    let project_id = crate::api::extract_project_id(&headers)?;

    if let Some(rejection) = check_observability_limit(&state, project_id).await {
        return Err(rejection);
    }

    let content_type_simple = if is_json_content_type(&headers) {
        "json"
    } else {
        "protobuf"
    };

    let raw_bytes = maybe_decompress(&headers, &body)?;
    let ingested_bytes = raw_bytes.len() as u64;

    let payload = RawOtlpTracePayload {
        project_key: project_id.to_string(),
        content_type: content_type_simple.to_string(),
        raw_bytes,
        ingested_bytes,
    };

    state.kafka.enqueue_raw_otlp_trace(&payload).map_err(|e| {
        error!("[OTLP Traces] Failed to enqueue to Kafka: {}", e);
        AppError::Internal(anyhow::anyhow!("Kafka enqueue failed: {}", e))
    })?;

    info!(
        "[OTLP Traces] Enqueued request to Kafka ({} bytes)",
        body.len()
    );
    Ok(StatusCode::ACCEPTED)
}

/// Handle OTLP HTTP logs endpoint
/// POST /v1/logs (Content-Type: application/x-protobuf or application/json)
/// Just validates auth, rate limits, and queues raw payload to Kafka
async fn otlp_http_logs(
    State(state): State<Arc<WatchState>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<StatusCode> {
    use crate::models::RawOtlpLogPayload;

    let project_id = crate::api::extract_project_id(&headers)?;

    if let Some(rejection) = check_observability_limit(&state, project_id).await {
        return Err(rejection);
    }

    let content_type_simple = if is_json_content_type(&headers) {
        "json"
    } else {
        "protobuf"
    };

    let raw_bytes = maybe_decompress(&headers, &body)?;
    let ingested_bytes = raw_bytes.len() as u64;

    let payload = RawOtlpLogPayload {
        project_key: project_id.to_string(),
        content_type: content_type_simple.to_string(),
        raw_bytes,
        ingested_bytes,
    };

    state.kafka.enqueue_raw_otlp_log(&payload).map_err(|e| {
        error!("[OTLP Logs] Failed to enqueue to Kafka: {}", e);
        AppError::Internal(anyhow::anyhow!("Kafka enqueue failed: {}", e))
    })?;

    info!(
        "[OTLP Logs] Enqueued request to Kafka ({} bytes)",
        body.len()
    );
    Ok(StatusCode::ACCEPTED)
}

/// Handle OTLP HTTP metrics endpoint
/// POST /v1/metrics (Content-Type: application/x-protobuf or application/json)
/// Just validates auth, rate limits, and queues raw payload to Kafka
async fn otlp_http_metrics(
    State(state): State<Arc<WatchState>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<StatusCode> {
    use crate::models::RawOtlpMetricsPayload;

    let project_id = crate::api::extract_project_id(&headers)?;

    let content_type_simple = if is_json_content_type(&headers) {
        "json"
    } else {
        "protobuf"
    };

    let raw_bytes = maybe_decompress(&headers, &body)?;

    let payload = RawOtlpMetricsPayload {
        project_key: project_id.to_string(),
        content_type: content_type_simple.to_string(),
        raw_bytes,
    };

    state
        .kafka
        .enqueue_raw_otlp_metrics(&payload)
        .map_err(|e| {
            error!("[OTLP Metrics] Failed to enqueue to Kafka: {}", e);
            AppError::Internal(anyhow::anyhow!("Kafka enqueue failed: {}", e))
        })?;

    info!(
        "[OTLP Metrics] Enqueued request to Kafka ({} bytes)",
        body.len()
    );
    Ok(StatusCode::ACCEPTED)
}

// ============================================================================
// Helper Functions
// ============================================================================

fn is_json_content_type(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|ct| ct.contains("json"))
        .unwrap_or(false)
}

fn is_gzip_encoded(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .map(|ce| ce.contains("gzip"))
        .unwrap_or(false)
}

/// Decompress gzip body if Content-Encoding indicates gzip, otherwise return as-is.
fn maybe_decompress(headers: &axum::http::HeaderMap, body: &axum::body::Bytes) -> Result<Vec<u8>> {
    if is_gzip_encoded(headers) {
        let mut decoder = flate2::read::GzDecoder::new(body.as_ref());
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).map_err(|e| {
            error!("[OTLP] gzip decompression failed: {}", e);
            AppError::Validation(format!("gzip decompression failed: {}", e))
        })?;
        Ok(decompressed)
    } else {
        Ok(body.to_vec())
    }
}

// ============================================================================
// Profiles Handler (Direct ClickHouse - Development API)
// ============================================================================

/// Handle OTLP HTTP profiles endpoint
/// POST /v1/profiles (Content-Type: application/x-protobuf or application/json)
/// Note: Profiles still writes directly to ClickHouse (can be moved to Kafka later)
async fn otlp_http_profiles(
    State(state): State<Arc<WatchState>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<StatusCode> {
    use opentelemetry_proto::tonic::collector::profiles::v1development::ExportProfilesServiceRequest;
    use prost::Message;

    let project_id = crate::api::extract_project_id(&headers)?;

    let raw_bytes = maybe_decompress(&headers, &body)?;

    let export_request = if is_json_content_type(&headers) {
        serde_json::from_slice::<ExportProfilesServiceRequest>(&raw_bytes).map_err(|e| {
            error!("[OTLP Profiles] Failed to parse JSON: {}", e);
            AppError::Validation(format!("Invalid OTLP profiles JSON: {}", e))
        })?
    } else {
        ExportProfilesServiceRequest::decode(raw_bytes.as_slice()).map_err(|e| {
            error!("[OTLP Profiles] Failed to parse protobuf: {}", e);
            AppError::Validation(format!("Invalid OTLP profiles protobuf: {}", e))
        })?
    };

    // Store profiles in ClickHouse
    let profile_count = store_profiles_in_clickhouse(
        &state,
        project_id,
        export_request.resource_profiles,
        export_request.dictionary,
    )
    .await?;

    info!("[OTLP Profiles] Processed {} profiles", profile_count);
    Ok(StatusCode::OK)
}

/// Store profiles in ClickHouse
async fn store_profiles_in_clickhouse(
    state: &Arc<WatchState>,
    project_id: uuid::Uuid,
    resource_profiles: Vec<opentelemetry_proto::tonic::profiles::v1development::ResourceProfiles>,
    dictionary: Option<opentelemetry_proto::tonic::profiles::v1development::ProfilesDictionary>,
) -> Result<usize> {
    use chrono::Utc;
    use prost::Message;

    #[derive(Row, Serialize)]
    struct ProfileInsert {
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
        #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
        created_at: chrono::DateTime<Utc>,
        attributes: std::collections::HashMap<String, String>,
        comments: Vec<String>,
    }

    #[derive(Row, Serialize)]
    struct ProfileSampleInsert {
        project_id: String,
        service_name: String,
        service_version: String,
        profile_type: String,
        profile_id: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
        timestamp: chrono::DateTime<Utc>,
        function_name: String,
        filename: String,
        line_number: u32,
        value: i64,
        labels: std::collections::HashMap<String, String>,
    }

    let mut inserter = state
        .clickhouse
        .as_ref()
        .inserter::<ProfileInsert>("profiles")
        .with_period(Some(Duration::from_millis(100)))
        .with_max_rows(500_000);

    let mut samples_inserter = state
        .clickhouse
        .as_ref()
        .inserter::<ProfileSampleInsert>("profile_samples")
        .with_period(Some(Duration::from_millis(100)))
        .with_max_rows(1_000_000);

    let now = Utc::now();
    let project_id_str = project_id.to_string();
    let mut total_profiles = 0;

    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD;

    let resolve_attributes = |attr_indices: &[i32],
                               dict: &opentelemetry_proto::tonic::profiles::v1development::ProfilesDictionary|
     -> std::collections::HashMap<String, String> {
        let mut map = std::collections::HashMap::new();
        for &idx in attr_indices {
            let idx = idx as usize;
            if idx == 0 || idx >= dict.attribute_table.len() {
                continue;
            }
            let attr = &dict.attribute_table[idx];
            let key = dict
                .string_table
                .get(attr.key_strindex as usize)
                .cloned()
                .unwrap_or_default();
            if key.is_empty() {
                continue;
            }
            let value = attr
                .value
                .as_ref()
                .map(|v| format_any_value(v))
                .unwrap_or_default();
            map.insert(key, value);
        }
        map
    };

    let mut dict_buf = Vec::new();
    if let Some(ref d) = dictionary {
        d.encode(&mut dict_buf).unwrap_or_default();
    }
    let dictionary_b64 = b64.encode(&dict_buf);

    for resource_profile in resource_profiles {
        let resource = resource_profile.resource.unwrap_or_default();
        let service_name = extract_service_name(&resource.attributes);
        let service_name_str = service_name.as_deref().unwrap_or("unknown");
        let service_version =
            extract_attribute_string(&resource.attributes, "service.version").unwrap_or_default();

        for scope_profile in resource_profile.scope_profiles {
            for profile in scope_profile.profiles {
                let profile_id = profile.profile_id.clone();
                let profile_id_str = if profile_id.is_empty() {
                    uuid::Uuid::new_v4().to_string()
                } else {
                    hex::encode(&profile_id)
                };

                let (trace_id, span_id) =
                    extract_trace_correlation_from_samples(&profile.sample, dictionary.as_ref());

                let profile_type = resolve_profile_type(&profile, dictionary.as_ref());

                let (profile_attributes, profile_comments) = if let Some(ref dict) = dictionary {
                    let attrs = resolve_attributes(&profile.attribute_indices, dict);
                    let comments: Vec<String> = profile
                        .comment_strindices
                        .iter()
                        .filter_map(|&idx| {
                            dict.string_table
                                .get(idx as usize)
                                .filter(|s| !s.is_empty())
                                .cloned()
                        })
                        .collect();
                    (attrs, comments)
                } else {
                    (std::collections::HashMap::new(), Vec::new())
                };

                let time_nanos = profile.time_unix_nano;
                let duration_nanos = profile.duration_nano;

                let timestamp = if time_nanos > 0 {
                    chrono::DateTime::from_timestamp(
                        (time_nanos / 1_000_000_000) as i64,
                        (time_nanos % 1_000_000_000) as u32,
                    )
                    .unwrap_or(now)
                } else {
                    now
                };

                // Explode samples into the analytics table
                if let Some(ref dict) = dictionary {
                    let string_table = &dict.string_table;
                    let function_table = &dict.function_table;
                    let location_table = &dict.location_table;
                    let stack_table = &dict.stack_table;

                    for sample in &profile.sample {
                        let value: i64 = if !sample.values.is_empty() {
                            sample.values.iter().sum::<i64>().max(1)
                        } else {
                            1
                        };

                        let sample_ts = sample
                            .timestamps_unix_nano
                            .first()
                            .filter(|&&t| t > 0)
                            .and_then(|&t| {
                                chrono::DateTime::from_timestamp(
                                    (t / 1_000_000_000) as i64,
                                    (t % 1_000_000_000) as u32,
                                )
                            })
                            .unwrap_or(timestamp);

                        let sample_labels = resolve_attributes(&sample.attribute_indices, dict);

                        let stack_idx = sample.stack_index as usize;
                        let location_indices = if stack_idx > 0 && stack_idx < stack_table.len() {
                            &stack_table[stack_idx].location_indices
                        } else {
                            continue;
                        };

                        if let Some(&loc_idx) = location_indices.first() {
                            let loc_idx = loc_idx as usize;
                            if loc_idx > 0 && loc_idx < location_table.len() {
                                let loc = &location_table[loc_idx];
                                if let Some(line) = loc.line.first() {
                                    let func_idx = line.function_index as usize;
                                    if func_idx > 0 && func_idx < function_table.len() {
                                        let func = &function_table[func_idx];
                                        let fname = string_table
                                            .get(func.name_strindex as usize)
                                            .cloned()
                                            .unwrap_or_default();
                                        let file = string_table
                                            .get(func.filename_strindex as usize)
                                            .cloned()
                                            .unwrap_or_default();

                                        if let Err(e) = samples_inserter
                                            .write(&ProfileSampleInsert {
                                                project_id: project_id_str.clone(),
                                                service_name: service_name_str.to_string(),
                                                service_version: service_version.clone(),
                                                profile_type: profile_type.clone(),
                                                profile_id: profile_id_str.clone(),
                                                timestamp: sample_ts,
                                                function_name: fname,
                                                filename: file,
                                                line_number: line.line.max(0) as u32,
                                                value,
                                                labels: sample_labels.clone(),
                                            })
                                            .await
                                        {
                                            error!("[Profiles] Failed to write sample row: {}", e);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                let mut profile_buf = Vec::new();
                profile.encode(&mut profile_buf).unwrap_or_default();
                let profile_b64 = b64.encode(&profile_buf);

                inserter
                    .write(&ProfileInsert {
                        id: uuid::Uuid::new_v4().to_string(),
                        project_id: project_id_str.clone(),
                        service_name: service_name_str.to_string(),
                        service_version: service_version.clone(),
                        trace_id,
                        span_id,
                        profile_id: profile_id_str,
                        time_unix_nano: time_nanos,
                        duration_nano: duration_nanos,
                        period_type: profile_type,
                        period: profile.period,
                        sample_count: profile.sample.len() as u64,
                        profile_data: profile_b64,
                        dictionary_data: dictionary_b64.clone(),
                        timestamp,
                        created_at: now,
                        attributes: profile_attributes,
                        comments: profile_comments,
                    })
                    .await
                    .map_err(|e| {
                        AppError::Internal(anyhow::anyhow!("ClickHouse insert error: {}", e))
                    })?;

                total_profiles += 1;
            }
        }
    }

    inserter
        .commit()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse commit error: {}", e)))?;
    inserter
        .end()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("ClickHouse end error: {}", e)))?;

    if let Err(e) = samples_inserter.commit().await {
        error!("[Profiles] Failed to commit profile_samples: {}", e);
    }
    if let Err(e) = samples_inserter.end().await {
        error!("[Profiles] Failed to end profile_samples inserter: {}", e);
    }

    Ok(total_profiles)
}

/// Resolve the actual profile type (cpu, alloc_space, etc.) from the OTLP profile.
fn resolve_profile_type(
    profile: &opentelemetry_proto::tonic::profiles::v1development::Profile,
    dictionary: Option<&opentelemetry_proto::tonic::profiles::v1development::ProfilesDictionary>,
) -> String {
    if let (Some(st), Some(dict)) = (&profile.sample_type, dictionary) {
        dict.string_table
            .get(st.type_strindex as usize)
            .filter(|s| !s.is_empty())
            .cloned()
            .unwrap_or_else(|| "cpu".to_string())
    } else {
        "cpu".to_string()
    }
}

// ============================================================================
// Profile Helper Functions
// ============================================================================

fn extract_service_name(
    attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
) -> Option<String> {
    extract_attribute_string(attributes, "service.name")
}

fn extract_attribute_string(
    attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
    key: &str,
) -> Option<String> {
    for attr in attributes {
        if attr.key == key {
            if let Some(value) = &attr.value {
                if let Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s),
                ) = &value.value
                {
                    return Some(s.clone());
                }
            }
        }
    }
    None
}

fn extract_trace_correlation_from_samples(
    samples: &[opentelemetry_proto::tonic::profiles::v1development::Sample],
    dictionary: Option<&opentelemetry_proto::tonic::profiles::v1development::ProfilesDictionary>,
) -> (Option<String>, Option<String>) {
    let Some(dict) = dictionary else {
        return (None, None);
    };

    for sample in samples {
        let link_idx = sample.link_index as usize;
        if link_idx > 0 && link_idx < dict.link_table.len() {
            let link = &dict.link_table[link_idx];
            let trace_id = if !link.trace_id.is_empty() {
                Some(format_trace_id(&link.trace_id))
            } else {
                None
            };
            let span_id = if !link.span_id.is_empty() {
                Some(format_span_id(&link.span_id))
            } else {
                None
            };
            if trace_id.is_some() || span_id.is_some() {
                return (trace_id, span_id);
            }
        }
    }

    (None, None)
}

fn format_trace_id(bytes: &[u8]) -> String {
    if bytes.len() == 16 {
        format!(
            "{:032x}",
            u128::from_be_bytes(bytes.try_into().unwrap_or([0u8; 16]))
        )
    } else {
        hex::encode(bytes)
    }
}

fn format_any_value(v: &opentelemetry_proto::tonic::common::v1::AnyValue) -> String {
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    match &v.value {
        Some(Value::StringValue(s)) => s.clone(),
        Some(Value::BoolValue(b)) => b.to_string(),
        Some(Value::IntValue(i)) => i.to_string(),
        Some(Value::DoubleValue(d)) => d.to_string(),
        Some(Value::BytesValue(b)) => hex::encode(b),
        _ => String::new(),
    }
}

fn format_span_id(bytes: &[u8]) -> String {
    if bytes.len() == 8 {
        format!(
            "{:016x}",
            u64::from_be_bytes(bytes.try_into().unwrap_or([0u8; 8]))
        )
    } else {
        hex::encode(bytes)
    }
}

/// Check if the project's organization has exceeded its observability GB limit.
/// Returns `Some(AppError)` if the limit is exceeded (free tier only).
///
/// Uses the preloaded `obs_limits` cache on WatchState (refreshed every 60s).
/// The project→org mapping is also cached in-process to avoid per-request DB lookups.
async fn check_observability_limit(
    state: &WatchState,
    project_id: uuid::Uuid,
) -> Option<AppError> {
    use reiver_core::entitlements::UsageEnforcer;
    use std::sync::OnceLock;
    use std::time::Instant;
    use quick_cache::sync::Cache;

    #[derive(Clone)]
    struct ProjectOrgEntry {
        org_id: uuid::Uuid,
        expires_at: Instant,
    }

    static PROJECT_ORG_CACHE: OnceLock<Cache<uuid::Uuid, ProjectOrgEntry>> = OnceLock::new();
    let project_cache = PROJECT_ORG_CACHE.get_or_init(|| Cache::new(10_000));

    // Resolve project → org with local cache (1 hour TTL)
    let org_id = if let Some(entry) = project_cache.get(&project_id) {
        if entry.expires_at > Instant::now() {
            entry.org_id
        } else {
            match sqlx::query_scalar::<_, uuid::Uuid>("SELECT organization_id FROM projects WHERE id = $1")
                .bind(project_id)
                .fetch_optional(state.db.as_ref())
                .await
                .ok()
                .flatten()
            {
                Some(id) => {
                    project_cache.insert(project_id, ProjectOrgEntry {
                        org_id: id,
                        expires_at: Instant::now() + Duration::from_secs(3600),
                    });
                    id
                }
                None => return None,
            }
        }
    } else {
        match sqlx::query_scalar::<_, uuid::Uuid>("SELECT organization_id FROM projects WHERE id = $1")
            .bind(project_id)
            .fetch_optional(state.db.as_ref())
            .await
            .ok()
            .flatten()
        {
            Some(id) => {
                project_cache.insert(project_id, ProjectOrgEntry {
                    org_id: id,
                    expires_at: Instant::now() + Duration::from_secs(3600),
                });
                id
            }
            None => return None,
        }
    };

    // Look up cached limits — no DB/CH query
    let limits = match state.obs_limits.get(org_id).await {
        Some(l) => l,
        None => return None, // org not in cache yet, allow (will appear after next refresh)
    };

    // Unlimited or paid tier — always allow
    if limits.limit_gb < 0 || limits.has_subscription {
        return None;
    }

    // Free tier with a finite cap — check actual usage
    let enforcer = UsageEnforcer::new(
        state.db.as_ref().clone(),
        state.clickhouse.as_ref().clone(),
        state.entitlements.clone(),
    );

    match enforcer.check_observability_gb(org_id).await {
        Ok(reiver_core::entitlements::UsageGate::Denied { reason }) => {
            Some(AppError::Forbidden(reason))
        }
        _ => None,
    }
}
