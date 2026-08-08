//! Helper functions for integration tests

use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ── Response structs ────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct LoginRequest {
    email: String,
    password: String,
}

#[derive(Debug, Deserialize)]
struct AuthResponse {
    token: String,
    user: UserResponse,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct UserResponse {
    id: Uuid,
    email: String,
    created_at: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Project {
    id: Uuid,
    organization_id: Uuid,
    name: String,
    created_by: Option<Uuid>,
    created_at: String,
    settings: Option<serde_json::Value>,
    github_repo_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ProjectKey {
    #[allow(dead_code)]
    id: Uuid,
    key: String,
    #[allow(dead_code)]
    created_at: String,
}

// ── Shared test context ─────────────────────────────────────────────

/// Holds all state created during test setup.
/// Each E2E test creates its own `TestContext` via `setup()` for isolation.
#[allow(dead_code)]
pub struct TestContext {
    pub client: reqwest::Client,
    pub base_url: String,
    pub token: String,
    pub user_id: Uuid,
    pub project_id: Uuid,
    pub project_key: String,
}

/// Log a human-readable step label to stderr during test runs.
pub fn step(name: &str) {
    eprintln!("\n  -- {}", name);
}

/// Register a fresh user, create a project, fetch the project key.
/// Returns a fully-populated `TestContext`.
pub async fn setup() -> TestContext {
    let base_url =
        std::env::var("E2E_BASE_URL").unwrap_or_else(|_| "http://localhost:3003".to_string());
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()
        .expect("failed to build reqwest client");

    let (token, user_id) = signup(&client, &base_url).await;
    let (project_id, project_key) = create_project(&client, &base_url, &token).await;

    TestContext {
        client,
        base_url,
        token,
        user_id,
        project_id,
        project_key,
    }
}

// ── Auth / project helpers ──────────────────────────────────────────

/// Register a new user with a random email. Returns `(jwt_token, user_id)`.
pub async fn signup(client: &reqwest::Client, base_url: &str) -> (String, Uuid) {
    let email = format!("e2e-{}@test.local", Uuid::new_v4());
    let password = "TestPassword123!";

    let resp = client
        .post(format!("{}/api/auth/signup", base_url))
        .json(&serde_json::json!({
            "email": email,
            "password": password,
        }))
        .send()
        .await
        .expect("signup request failed");

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert!(status.is_success(), "signup returned {}: {}", status, body);

    let auth: AuthResponse = serde_json::from_str(&body).expect("failed to parse signup response");
    (auth.token, auth.user.id)
}

/// Create a project and return `(project_id, project_key)`.
pub async fn create_project(
    client: &reqwest::Client,
    base_url: &str,
    token: &str,
) -> (Uuid, String) {
    let project_name = format!("e2e-project-{}", Uuid::new_v4());

    let resp = client
        .post(format!("{}/api/projects", base_url))
        .bearer_auth(token)
        .json(&serde_json::json!({ "name": project_name }))
        .send()
        .await
        .expect("create project request failed");

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert!(
        status.is_success(),
        "create project returned {}: {}",
        status,
        body
    );

    let project: Project = serde_json::from_str(&body).expect("failed to parse project response");
    let project_id = project.id;

    // Fetch the default project key created automatically
    let resp = client
        .get(format!("{}/api/projects/{}/keys", base_url, project_id))
        .bearer_auth(token)
        .send()
        .await
        .expect("list project keys request failed");

    let keys: Vec<ProjectKey> = resp.json().await.expect("failed to parse project keys");
    assert!(!keys.is_empty(), "project should have at least one key");

    (project_id, keys[0].key.clone())
}

// ── OTLP payload builders ───────────────────────────────────────────

/// Build a valid OTLP JSON trace payload (single span).
pub fn build_otlp_trace_payload(
    trace_id: &str,
    span_id: &str,
    service_name: &str,
) -> serde_json::Value {
    let now_nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let end_nanos = now_nanos + 100_000_000; // +100 ms

    serde_json::json!({
        "resourceSpans": [{
            "resource": {
                "attributes": [{
                    "key": "service.name",
                    "value": { "stringValue": service_name }
                }]
            },
            "scopeSpans": [{
                "scope": { "name": "e2e-test" },
                "spans": [{
                    "traceId": trace_id,
                    "spanId": span_id,
                    "name": "test-operation",
                    "kind": 2,
                    "startTimeUnixNano": now_nanos.to_string(),
                    "endTimeUnixNano": end_nanos.to_string(),
                    "status": { "code": 1 }
                }]
            }]
        }]
    })
}

/// Describes a single span for multi-span trace payloads.
pub struct SpanDef {
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub name: String,
    pub kind: u32,
    pub duration_ms: u64,
}

/// Build an OTLP trace payload with multiple spans.
pub fn build_otlp_trace_payload_multi_span(
    trace_id: &str,
    service_name: &str,
    spans: &[SpanDef],
) -> serde_json::Value {
    let now_nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

    let span_values: Vec<serde_json::Value> = spans
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let start = now_nanos + (i as i64) * 10_000_000; // stagger by 10ms
            let end = start + (s.duration_ms as i64) * 1_000_000;
            let mut span_json = serde_json::json!({
                "traceId": trace_id,
                "spanId": s.span_id,
                "name": s.name,
                "kind": s.kind,
                "startTimeUnixNano": start.to_string(),
                "endTimeUnixNano": end.to_string(),
                "status": { "code": 1 }
            });
            if let Some(ref parent) = s.parent_span_id {
                span_json["parentSpanId"] = serde_json::json!(parent);
            }
            span_json
        })
        .collect();

    serde_json::json!({
        "resourceSpans": [{
            "resource": {
                "attributes": [{
                    "key": "service.name",
                    "value": { "stringValue": service_name }
                }]
            },
            "scopeSpans": [{
                "scope": { "name": "e2e-test" },
                "spans": span_values
            }]
        }]
    })
}

/// Build an exception payload for ingestion through the proxy.
pub fn build_exception_payload(
    message: &str,
    exception_type: &str,
    project_key: &str,
) -> serde_json::Value {
    serde_json::json!({
        "project_key": project_key,
        "level": "error",
        "message": message,
        "exception": {
            "type": exception_type,
            "value": message,
            "stacktrace": [{
                "filename": "e2e_tests.rs",
                "function": "test_exception_flow",
                "lineno": 42,
                "in_app": true
            }]
        },
        "service_name": "e2e-test-svc",
        "environment": "test"
    })
}

/// Build an exception payload with extended metadata fields.
/// Uses exception_type in the stacktrace function name to ensure
/// different exception types produce different fingerprints.
pub fn build_exception_payload_with_metadata(
    message: &str,
    exception_type: &str,
    project_key: &str,
    service_name: &str,
    environment: &str,
    trace_id: Option<&str>,
) -> serde_json::Value {
    let mut payload = serde_json::json!({
        "project_key": project_key,
        "level": "error",
        "message": message,
        "exception": {
            "type": exception_type,
            "value": message,
            "stacktrace": [{
                "filename": "e2e_tests.rs",
                "function": format!("test_{}", exception_type),
                "lineno": 42,
                "in_app": true
            }]
        },
        "service_name": service_name,
        "environment": environment
    });
    if let Some(tid) = trace_id {
        payload["trace_id"] = serde_json::json!(tid);
    }
    payload
}

/// Build a valid OTLP JSON log payload.
pub fn build_otlp_log_payload(trace_id: &str, message: &str, severity: &str) -> serde_json::Value {
    build_otlp_log_payload_with_service(trace_id, message, severity, "e2e-test-svc")
}

/// Build a valid OTLP JSON log payload with a custom service name.
pub fn build_otlp_log_payload_with_service(
    trace_id: &str,
    message: &str,
    severity: &str,
    service_name: &str,
) -> serde_json::Value {
    let now_nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let severity_number = match severity {
        "INFO" => 9,
        "WARN" => 13,
        "ERROR" => 17,
        _ => 9,
    };

    serde_json::json!({
        "resourceLogs": [{
            "resource": {
                "attributes": [{
                    "key": "service.name",
                    "value": { "stringValue": service_name }
                }]
            },
            "scopeLogs": [{
                "scope": { "name": "e2e-test" },
                "logRecords": [{
                    "timeUnixNano": now_nanos.to_string(),
                    "severityNumber": severity_number,
                    "severityText": severity,
                    "body": { "stringValue": message },
                    "traceId": trace_id,
                    "spanId": ""
                }]
            }]
        }]
    })
}

/// Build a valid OTLP JSON metrics payload (gauge).
pub fn build_otlp_metrics_payload(metric_name: &str, value: f64) -> serde_json::Value {
    build_otlp_metrics_payload_with_labels(metric_name, value, &[("env", "test")])
}

/// Build an OTLP JSON metrics payload (gauge) with custom labels.
pub fn build_otlp_metrics_payload_with_labels(
    metric_name: &str,
    value: f64,
    labels: &[(&str, &str)],
) -> serde_json::Value {
    let now_nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

    let attributes: Vec<serde_json::Value> = labels
        .iter()
        .map(|(k, v)| {
            serde_json::json!({
                "key": k,
                "value": { "stringValue": v }
            })
        })
        .collect();

    serde_json::json!({
        "resourceMetrics": [{
            "resource": {
                "attributes": [{
                    "key": "service.name",
                    "value": { "stringValue": "e2e-test-svc" }
                }]
            },
            "scopeMetrics": [{
                "scope": { "name": "e2e-test" },
                "metrics": [{
                    "name": metric_name,
                    "gauge": {
                        "dataPoints": [{
                            "timeUnixNano": now_nanos.to_string(),
                            "asDouble": value,
                            "attributes": attributes
                        }]
                    }
                }]
            }]
        }]
    })
}

// ── Non-OTLP payload builders ───────────────────────────────────────

/// Build a direct log ingestion payload (POST /api/watch/logs/ingest).
pub fn build_direct_log_payload(message: &str, level: &str, service: &str) -> serde_json::Value {
    serde_json::json!({
        "message": message,
        "level": level,
        "service": service,
        "source": "e2e-test",
        "timestamp": chrono::Utc::now().to_rfc3339()
    })
}

/// Build a feature flag change event payload.
pub fn build_feature_flag_event_payload(
    flag_id: &str,
    change_type: &str,
    project_key: &str,
) -> serde_json::Value {
    serde_json::json!({
        "event_type": "feature_flag_change",
        "project_key": project_key,
        "flag_id": flag_id,
        "flag_name": format!("Flag {}", flag_id),
        "change_type": change_type,
        "environment": "test",
        "new_value": true,
        "prev_value": false,
        "changed_by": {
            "type": "user",
            "email": "e2e-test@test.local",
            "name": "E2E Tester"
        },
        "impacted_services": ["e2e-test-svc"],
        "timestamp": chrono::Utc::now().to_rfc3339()
    })
}

/// Build a notification channel create payload.
pub fn build_notification_channel_payload(name: &str, channel_type: &str) -> serde_json::Value {
    serde_json::json!({
        "name": name,
        "channel_type": channel_type,
        "config": {
            "url": "https://hooks.example.com/e2e-test"
        },
        "enabled": true
    })
}

/// Build a one-time maintenance window payload.
pub fn build_maintenance_window_one_time_payload(
    name: &str,
    project_id: &Uuid,
) -> serde_json::Value {
    let start = chrono::Utc::now() + chrono::Duration::hours(1);
    let end = start + chrono::Duration::hours(2);
    serde_json::json!({
        "project_id": project_id,
        "name": name,
        "description": "E2E test one-time window",
        "schedule_type": "one_time",
        "start_time": start.to_rfc3339(),
        "end_time": end.to_rfc3339(),
        "enabled": true
    })
}

/// Build a recurring maintenance window payload.
pub fn build_maintenance_window_recurring_payload(
    name: &str,
    project_id: &Uuid,
) -> serde_json::Value {
    serde_json::json!({
        "project_id": project_id,
        "name": name,
        "description": "E2E test recurring daily window",
        "schedule_type": "recurring",
        "recurrence_type": "daily",
        "recurrence_start_time": "02:00",
        "recurrence_duration_minutes": 60,
        "recurrence_timezone": "UTC",
        "enabled": true
    })
}

/// Build an OTLP trace payload with a service version attribute.
pub fn build_otlp_trace_payload_with_version(
    trace_id: &str,
    span_id: &str,
    service_name: &str,
    service_version: &str,
) -> serde_json::Value {
    let now_nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let end_nanos = now_nanos + 100_000_000; // +100 ms

    serde_json::json!({
        "resourceSpans": [{
            "resource": {
                "attributes": [
                    {
                        "key": "service.name",
                        "value": { "stringValue": service_name }
                    },
                    {
                        "key": "service.version",
                        "value": { "stringValue": service_version }
                    }
                ]
            },
            "scopeSpans": [{
                "scope": { "name": "e2e-test" },
                "spans": [{
                    "traceId": trace_id,
                    "spanId": span_id,
                    "name": "versioned-operation",
                    "kind": 2,
                    "startTimeUnixNano": now_nanos.to_string(),
                    "endTimeUnixNano": end_nanos.to_string(),
                    "status": { "code": 1 }
                }]
            }]
        }]
    })
}

/// Build an X-Ray segment payload.
pub fn build_xray_segment(name: &str, segment_id: &str, trace_id: &str) -> serde_json::Value {
    let now = chrono::Utc::now().timestamp() as f64;
    serde_json::json!({
        "name": name,
        "id": segment_id,
        "trace_id": trace_id,
        "start_time": now - 0.5,
        "end_time": now,
        "http": {
            "request": {
                "method": "GET",
                "url": "https://example.com/api/test"
            },
            "response": {
                "status": 200
            }
        }
    })
}

/// Build a CloudWatch Kinesis Firehose payload.
/// The `data` field should be base64-encoded gzipped CloudWatch JSON,
/// but for testing we send a minimal base64 payload.
pub fn build_cloudwatch_kinesis_payload() -> serde_json::Value {
    use base64::Engine;
    // Minimal CloudWatch log event JSON
    let log_event = serde_json::json!({
        "messageType": "DATA_MESSAGE",
        "owner": "123456789012",
        "logGroup": "/e2e/test",
        "logStream": "e2e-stream",
        "logEvents": [{
            "id": "e2e-log-event-1",
            "timestamp": chrono::Utc::now().timestamp_millis(),
            "message": "E2E CloudWatch test log message"
        }]
    });
    let json_bytes = serde_json::to_vec(&log_event).unwrap();
    // gzip compress
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(&json_bytes).unwrap();
    let compressed = encoder.finish().unwrap();
    let encoded = base64::engine::general_purpose::STANDARD.encode(&compressed);

    serde_json::json!({
        "requestId": format!("e2e-req-{}", Uuid::new_v4()),
        "timestamp": chrono::Utc::now().timestamp_millis() as u64,
        "records": [{
            "recordId": format!("e2e-rec-{}", Uuid::new_v4()),
            "data": encoded,
            "approximateArrivalTimestamp": chrono::Utc::now().timestamp_millis() as u64
        }]
    })
}

/// Build an Azure Monitor log payload (array of log entries).
pub fn build_azure_monitor_payload(message: &str, service: &str) -> serde_json::Value {
    serde_json::json!([{
        "Message": message,
        "Level": "Information",
        "TimeGenerated": chrono::Utc::now().to_rfc3339(),
        "ResourceId": format!("/subscriptions/e2e/resourceGroups/test/providers/Microsoft.Compute/virtualMachines/{}", service),
        "Computer": service
    }])
}

/// Build a GCP log entry payload.
pub fn build_gcp_log_payload(message: &str, service: &str) -> serde_json::Value {
    serde_json::json!({
        "entries": [{
            "textPayload": message,
            "severity": "INFO",
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "resource": {
                "type": "gce_instance",
                "labels": {
                    "instance_name": service
                }
            },
            "logName": format!("projects/e2e-test/logs/{}", service)
        }]
    })
}

/// Build an explain plan payload for database monitoring.
pub fn build_explain_plan_payload(project_id: &Uuid) -> serde_json::Value {
    serde_json::json!({
        "database_name": "e2e_test_db",
        "database_host": "localhost:5432",
        "database_type": "postgresql",
        "query_template": "SELECT * FROM users WHERE id = $1",
        "explain_plan": {
            "Plan": {
                "Node Type": "Index Scan",
                "Relation Name": "users",
                "Index Name": "users_pkey",
                "Startup Cost": 0.29,
                "Total Cost": 8.30,
                "Plan Rows": 1,
                "Plan Width": 100
            }
        },
        "execution_time_ms": 1.5,
        "planning_time_ms": 0.2,
        "total_cost": 8.30,
        "rows_estimated": 1,
        "rows_actual": 1,
        "has_full_table_scan": false,
        "has_missing_index": false,
        "has_sequential_scan": false,
        "query_fingerprint": format!("e2e-fp-{}", project_id)
    })
}

/// Build a query metrics payload for database monitoring.
pub fn build_query_metrics_payload(project_id: &Uuid) -> serde_json::Value {
    let now = chrono::Utc::now();
    serde_json::json!({
        "database_name": "e2e_test_db",
        "database_host": "localhost:5432",
        "database_type": "postgresql",
        "query_fingerprint": format!("e2e-qfp-{}", project_id),
        "query_template": "SELECT * FROM orders WHERE user_id = $1",
        "calls": 150,
        "total_time_ms": 450.0,
        "mean_time_ms": 3.0,
        "min_time_ms": 0.5,
        "max_time_ms": 25.0,
        "stddev_time_ms": 2.1,
        "rows_affected": 0,
        "rows_returned": 150,
        "first_seen": (now - chrono::Duration::hours(24)).to_rfc3339(),
        "last_seen": now.to_rfc3339()
    })
}

/// Build a health check result report payload.
pub fn build_health_check_result_payload(check_id: &str, check_name: &str) -> serde_json::Value {
    serde_json::json!({
        "check_id": check_id,
        "check_type": "http",
        "check_name": check_name,
        "target": "https://example.com",
        "status": "up",
        "success": true,
        "response_time_ms": 42.5,
        "dns_time_ms": 5.0,
        "connect_time_ms": 10.0,
        "tls_time_ms": 15.0,
        "first_byte_time_ms": 35.0,
        "http_status_code": 200,
        "http_response_size": 1024,
        "ssl_valid": true,
        "ssl_days_until_expiry": 365,
        "ssl_issuer": "Let's Encrypt",
        "ssl_subject": "example.com",
        "timestamp": chrono::Utc::now().timestamp(),
        "agent_id": "e2e-agent-1",
        "agent_location": "us-east-1"
    })
}

/// Build a minimal OTLP profile payload.
pub fn build_otlp_profile_payload(service_name: &str) -> serde_json::Value {
    let now_nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
    serde_json::json!({
        "resourceProfiles": [{
            "resource": {
                "attributes": [{
                    "key": "service.name",
                    "value": { "stringValue": service_name }
                }]
            },
            "scopeProfiles": [{
                "scope": { "name": "e2e-test" },
                "profiles": [{
                    "profileId": format!("{:032x}", Uuid::new_v4().as_u128()),
                    "timeUnixNano": now_nanos,
                    "durationNano": 1000000000u64,
                    "sample": [{
                        "stackIndex": 0,
                        "values": [100],
                        "attributeIndices": [],
                        "linkIndex": 0,
                        "timestampsUnixNano": []
                    }],
                    "period": 0,
                    "commentStrindices": [],
                    "droppedAttributesCount": 0,
                    "originalPayloadFormat": "",
                    "originalPayload": [],
                    "attributeIndices": []
                }],
                "schemaUrl": ""
            }],
            "schemaUrl": ""
        }],
        "dictionary": {
            "mappingTable": [],
            "locationTable": [],
            "functionTable": [],
            "linkTable": [],
            "stringTable": [""],
            "attributeTable": [],
            "stackTable": []
        }
    })
}

/// Build an OTLP trace payload with span-level attributes (e.g. deployment.environment).
/// This is needed for testing exception-trace enrichment, which reads from `span_attributes`.
pub fn build_otlp_trace_payload_with_span_attrs(
    trace_id: &str,
    span_id: &str,
    service_name: &str,
    span_attributes: &[(&str, &str)],
) -> serde_json::Value {
    let now_nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let end_nanos = now_nanos + 100_000_000; // +100 ms

    let attrs: Vec<serde_json::Value> = span_attributes
        .iter()
        .map(|(k, v)| {
            serde_json::json!({
                "key": k,
                "value": { "stringValue": v }
            })
        })
        .collect();

    serde_json::json!({
        "resourceSpans": [{
            "resource": {
                "attributes": [{
                    "key": "service.name",
                    "value": { "stringValue": service_name }
                }]
            },
            "scopeSpans": [{
                "scope": { "name": "e2e-test" },
                "spans": [{
                    "traceId": trace_id,
                    "spanId": span_id,
                    "name": "enriched-operation",
                    "kind": 2,
                    "startTimeUnixNano": now_nanos.to_string(),
                    "endTimeUnixNano": end_nanos.to_string(),
                    "status": { "code": 1 },
                    "attributes": attrs
                }]
            }]
        }]
    })
}

/// Build a minimal OTLP profile payload with trace correlation via link table.
pub fn build_otlp_profile_payload_with_trace(
    service_name: &str,
    trace_id: &str,
    span_id: &str,
) -> serde_json::Value {
    let now_nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;

    // Convert trace_id hex string to byte array for the link table
    let trace_id_bytes: Vec<u8> = (0..trace_id.len())
        .step_by(2)
        .filter_map(|i| u8::from_str_radix(&trace_id[i..i + 2], 16).ok())
        .collect();
    let span_id_bytes: Vec<u8> = (0..span_id.len())
        .step_by(2)
        .filter_map(|i| u8::from_str_radix(&span_id[i..i + 2], 16).ok())
        .collect();

    serde_json::json!({
        "resourceProfiles": [{
            "resource": {
                "attributes": [{
                    "key": "service.name",
                    "value": { "stringValue": service_name }
                }]
            },
            "scopeProfiles": [{
                "scope": { "name": "e2e-test" },
                "profiles": [{
                    "profileId": format!("{:032x}", Uuid::new_v4().as_u128()),
                    "timeUnixNano": now_nanos,
                    "durationNano": 1000000000u64,
                    "sample": [{
                        "stackIndex": 0,
                        "values": [100],
                        "attributeIndices": [],
                        "linkIndex": 1,
                        "timestampsUnixNano": []
                    }],
                    "period": 0,
                    "commentStrindices": [],
                    "droppedAttributesCount": 0,
                    "originalPayloadFormat": "",
                    "originalPayload": [],
                    "attributeIndices": []
                }],
                "schemaUrl": ""
            }],
            "schemaUrl": ""
        }],
        "dictionary": {
            "mappingTable": [],
            "locationTable": [],
            "functionTable": [],
            "linkTable": [
                { "traceId": [], "spanId": [] },
                { "traceId": trace_id_bytes, "spanId": span_id_bytes }
            ],
            "stringTable": [""],
            "attributeTable": [],
            "stackTable": []
        }
    })
}

/// Build a widget query payload for the spans table.
pub fn build_widget_query_payload() -> serde_json::Value {
    let now = chrono::Utc::now();
    let one_hour_ago = now - chrono::Duration::hours(1);
    serde_json::json!({
        "query": {
            "table": "spans",
            "select": [
                { "fn": "count", "alias": "total" }
            ],
            "limit": 10
        },
        "time_range": {
            "from": one_hour_ago.to_rfc3339(),
            "to": now.to_rfc3339()
        }
    })
}

// ── Retry helper ────────────────────────────────────────────────────

/// Retry a check function up to `max_attempts` times, sleeping `interval`
/// between attempts. Returns `true` if the check eventually succeeded.
pub async fn wait_for<F, Fut>(
    description: &str,
    max_attempts: u32,
    interval: std::time::Duration,
    check_fn: F,
) -> bool
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for attempt in 1..=max_attempts {
        if check_fn().await {
            eprintln!(
                "    [ok] {} (attempt {}/{})",
                description, attempt, max_attempts
            );
            return true;
        }
        if attempt < max_attempts {
            eprintln!(
                "    [wait] {} not ready, retrying in {:?} ({}/{})",
                description, interval, attempt, max_attempts
            );
            tokio::time::sleep(interval).await;
        }
    }
    eprintln!(
        "    [FAIL] {} did not succeed after {} attempts",
        description, max_attempts
    );
    false
}

// ── HTTP request builders (used by unit tests) ─────────────────────

/// Build an HTTP POST request with a JSON body.
#[allow(dead_code)]
pub fn json_post(uri: &str, body: &serde_json::Value) -> http::Request<String> {
    http::Request::builder()
        .method(http::Method::POST)
        .uri(uri)
        .header("content-type", "application/json")
        .body(body.to_string())
        .expect("failed to build POST request")
}

/// Build an HTTP GET request.
#[allow(dead_code)]
pub fn json_get(uri: &str) -> http::Request<()> {
    http::Request::builder()
        .method(http::Method::GET)
        .uri(uri)
        .body(())
        .expect("failed to build GET request")
}

// ── Legacy helper (kept for existing tests) ─────────────────────────

/// Get a project key for the given email/password
#[allow(dead_code)]
pub async fn get_project_key_for_user(
    api_url: &str,
    email: &str,
    password: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();

    // Login
    let login_url = format!("{}/api/auth/login", api_url.trim_end_matches('/'));
    let login_req = LoginRequest {
        email: email.to_string(),
        password: password.to_string(),
    };

    let auth_response: AuthResponse = client
        .post(&login_url)
        .json(&login_req)
        .send()
        .await?
        .json()
        .await?;

    println!("Logged in as: {}", auth_response.user.email);

    // Get projects
    let projects_url = format!("{}/api/projects", api_url.trim_end_matches('/'));
    let projects: Vec<Project> = client
        .get(&projects_url)
        .header("Authorization", format!("Bearer {}", auth_response.token))
        .send()
        .await?
        .json()
        .await?;

    if projects.is_empty() {
        return Err("No projects found. Please create a project first.".into());
    }

    let project = &projects[0];
    println!("Using project: {} ({})", project.name, project.id);

    // Get project keys
    let keys_url = format!(
        "{}/api/projects/{}/keys",
        api_url.trim_end_matches('/'),
        project.id
    );
    let keys: Vec<ProjectKey> = client
        .get(&keys_url)
        .header("Authorization", format!("Bearer {}", auth_response.token))
        .send()
        .await?
        .json()
        .await?;

    if keys.is_empty() {
        return Err("No project keys found. Please create a project key first.".into());
    }

    let project_key = &keys[0].key;
    println!("Found project key: {}...", &project_key[..8]);

    Ok(project_key.clone())
}
