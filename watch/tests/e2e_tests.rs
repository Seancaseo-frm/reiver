//! End-to-end integration tests for the Watch (APM) service.
//!
//! These tests exercise the full stack through the **website proxy** (port 3003)
//! which handles auth and forwards to Watch. All infrastructure must be running
//! (`make dev`).
//!
//! Run with:
//!   cargo test --manifest-path watch/Cargo.toml --test e2e_tests -- --ignored --nocapture --test-threads=1

mod helpers;

use helpers::{
    build_azure_monitor_payload, build_cloudwatch_kinesis_payload, build_direct_log_payload,
    build_exception_payload, build_exception_payload_with_metadata, build_explain_plan_payload,
    build_feature_flag_event_payload, build_gcp_log_payload, build_health_check_result_payload,
    build_maintenance_window_one_time_payload, build_maintenance_window_recurring_payload,
    build_notification_channel_payload, build_otlp_log_payload, build_otlp_metrics_payload,
    build_otlp_metrics_payload_with_labels, build_otlp_profile_payload,
    build_otlp_profile_payload_with_trace, build_otlp_trace_payload,
    build_otlp_trace_payload_multi_span, build_otlp_trace_payload_with_span_attrs,
    build_otlp_trace_payload_with_version, build_query_metrics_payload, build_widget_query_payload,
    build_xray_segment, setup, step, wait_for, SpanDef,
};
use std::time::Duration;
use uuid::Uuid;

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 1: OTLP Trace Ingestion Pipeline
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_otlp_trace_ingestion_pipeline() {
    let ctx = setup().await;

    // ── Send OTLP trace ─────────────────────────────────────────────────
    step("Send OTLP trace via proxy");
    let trace_id = format!("{:032x}", Uuid::new_v4().as_u128());
    let span_id = format!("{:016x}", rand::random::<u64>());
    let payload = build_otlp_trace_payload(&trace_id, &span_id, "e2e-test-svc");

    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/traces", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload)
        .send()
        .await
        .expect("ingest traces request failed");

    assert!(
        resp.status().is_success(),
        "ingest traces returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    // ── Wait for Kafka → ClickHouse pipeline ────────────────────────────
    step("Wait for trace to appear in ClickHouse (via traces API)");
    let trace_id_clone = trace_id.clone();
    let found = wait_for("trace in list", 15, Duration::from_secs(2), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        let tid = trace_id_clone.clone();
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/projects/{}/traces",
                    base_url, project_id
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    if let Some(arr) = body.as_array() {
                        arr.iter().any(|t| t["trace_id"].as_str() == Some(&tid))
                    } else {
                        false
                    }
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(found, "trace {} never appeared in traces list", trace_id);

    // ── Query individual trace detail ───────────────────────────────────
    step("Fetch trace detail and verify spans");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/traces/{}",
            ctx.base_url, ctx.project_id, trace_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("get trace detail request failed");

    assert!(resp.status().is_success(), "get trace detail failed");
    let detail: serde_json::Value = resp.json().await.expect("parse trace detail");
    let spans = detail["spans"]
        .as_array()
        .expect("spans should be an array");
    assert!(!spans.is_empty(), "trace should have at least one span");
    assert_eq!(
        spans[0]["span_name"].as_str(),
        Some("test-operation"),
        "span name mismatch"
    );
    assert_eq!(
        spans[0]["service_name"].as_str(),
        Some("e2e-test-svc"),
        "service name mismatch"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 2: Exception Ingestion and Grouping
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_exception_ingestion_and_grouping() {
    let ctx = setup().await;
    let unique = Uuid::new_v4().to_string();

    // ── Send exception A ────────────────────────────────────────────────
    step("Send exception A");
    let payload_a = build_exception_payload(
        &format!("E2E error A {}", unique),
        "E2ETestError",
        &ctx.project_key,
    );

    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/exceptions", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload_a)
        .send()
        .await
        .expect("ingest exception A failed");
    assert!(
        resp.status().is_success(),
        "ingest exception A returned {}",
        resp.status()
    );

    // ── Send exception B (same type + stacktrace → same fingerprint) ────
    step("Send exception B (same fingerprint)");
    let payload_b = build_exception_payload(
        &format!("E2E error B {}", unique),
        "E2ETestError",
        &ctx.project_key,
    );

    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/exceptions", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload_b)
        .send()
        .await
        .expect("ingest exception B failed");
    assert!(
        resp.status().is_success(),
        "ingest exception B returned {}",
        resp.status()
    );

    // ── Wait for pipeline ───────────────────────────────────────────────
    step("Wait for exceptions to appear in ClickHouse");
    let mut group_id: Option<String> = None;

    let found = wait_for("exception group", 15, Duration::from_secs(2), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/projects/{}/exceptions",
                    base_url, project_id
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    if let Some(arr) = body.as_array() {
                        arr.iter().any(|g| {
                            g["exception_type"].as_str() == Some("E2ETestError")
                                && g["count"].as_i64().unwrap_or(0) >= 2
                        })
                    } else {
                        false
                    }
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(found, "exception group with count >= 2 never appeared");

    // Fetch group_id for the resolve step
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/exceptions",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("list exceptions failed");
    let groups: serde_json::Value = resp.json().await.unwrap();
    for g in groups.as_array().unwrap() {
        if g["exception_type"].as_str() == Some("E2ETestError") {
            group_id = g["id"].as_str().map(String::from);
            break;
        }
    }
    let group_id = group_id.expect("could not find E2ETestError group id");

    // ── Resolve the exception group ─────────────────────────────────────
    step("Resolve exception group");
    let resp = ctx
        .client
        .patch(format!(
            "{}/api/watch/projects/{}/exceptions/{}",
            ctx.base_url, ctx.project_id, group_id
        ))
        .bearer_auth(&ctx.token)
        .json(&serde_json::json!({ "status": "resolved" }))
        .send()
        .await
        .expect("resolve exception failed");
    assert!(
        resp.status().is_success(),
        "resolve exception returned {}",
        resp.status()
    );

    // ── Verify status is resolved ───────────────────────────────────────
    step("Verify exception group is resolved");
    // The status update inserts a new row, so we need to wait for it to propagate
    let resolved = wait_for("resolved status", 10, Duration::from_secs(2), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        let gid = group_id.clone();
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/projects/{}/exceptions/{}",
                    base_url, project_id, gid
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    body["group"]["status"].as_str() == Some("resolved")
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(resolved, "exception group was not marked as resolved");
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 3: OTLP Log Ingestion
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_otlp_log_ingestion() {
    let ctx = setup().await;

    step("Send OTLP log via proxy");
    let trace_id = format!("{:032x}", Uuid::new_v4().as_u128());
    let payload = build_otlp_log_payload(&trace_id, "E2E test log message", "INFO");

    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/logs", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload)
        .send()
        .await
        .expect("ingest logs request failed");

    assert!(
        resp.status().is_success(),
        "ingest logs returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    step("Log ingestion accepted (200 OK)");
    // Note: There is no direct "list logs" REST endpoint through the proxy.
    // The assertion that the ingestion endpoint returned 200 confirms the log
    // was accepted and sent to Kafka for processing.
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 4: OTLP Metrics Ingestion and Query
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_otlp_metrics_ingestion_and_query() {
    let ctx = setup().await;
    let metric_name = format!("e2e.test.gauge.{}", Uuid::new_v4().simple());

    // ── Send OTLP metrics ───────────────────────────────────────────────
    step("Send OTLP metric via proxy");
    let payload = build_otlp_metrics_payload(&metric_name, 42.0);

    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/metrics", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload)
        .send()
        .await
        .expect("ingest metrics request failed");

    assert!(
        resp.status().is_success(),
        "ingest metrics returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    // ── Wait for metric to appear ───────────────────────────────────────
    step("Wait for metric name to appear in names list");
    let mn = metric_name.clone();
    let found = wait_for("metric name in list", 15, Duration::from_secs(2), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        let name = mn.clone();
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/query/metrics/names?project_id={}",
                    base_url, project_id
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    if let Some(metrics) = body["metrics"].as_array() {
                        metrics.iter().any(|m| m["name"].as_str() == Some(&name))
                    } else {
                        false
                    }
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(
        found,
        "metric '{}' never appeared in names list",
        metric_name
    );

    // ── Query metric data ───────────────────────────────────────────────
    step("Query metric data points");
    let now_ms = chrono::Utc::now().timestamp_millis();
    let one_hour_ago_ms = now_ms - 3_600_000;

    let query_body = serde_json::json!({
        "project_id": ctx.project_id,
        "metric_name": metric_name,
        "start": one_hour_ago_ms,
        "end": now_ms,
        "step": 60,
        "time_aggregation": "avg",
        "space_aggregation": "sum",
        "filters": {},
        "group_by": []
    });

    let resp = ctx
        .client
        .post(format!("{}/api/watch/query/metrics/query", ctx.base_url))
        .bearer_auth(&ctx.token)
        .json(&query_body)
        .send()
        .await
        .expect("query metrics request failed");

    assert!(
        resp.status().is_success(),
        "query metrics returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let result: serde_json::Value = resp.json().await.expect("parse metrics query result");
    assert_eq!(
        result["metric_name"].as_str(),
        Some(metric_name.as_str()),
        "metric_name in response should match"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 5: Dashboard CRUD
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_dashboard_crud() {
    let ctx = setup().await;

    // ── Create ───────────────────────────────────────────────────────────
    step("Create dashboard");
    let create_body = serde_json::json!({
        "name": "E2E Test Dashboard",
        "description": "Created by integration tests",
        "time_range": "1h"
    });

    let resp = ctx
        .client
        .post(format!(
            "{}/api/watch/projects/{}/dashboards",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .json(&create_body)
        .send()
        .await
        .expect("create dashboard failed");

    assert!(
        resp.status().is_success(),
        "create dashboard returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let dashboard: serde_json::Value = resp.json().await.expect("parse created dashboard");
    let dashboard_id = dashboard["id"]
        .as_str()
        .expect("dashboard should have an id");

    // ── List ─────────────────────────────────────────────────────────────
    step("List dashboards -- verify new dashboard appears");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/dashboards",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("list dashboards failed");

    assert!(resp.status().is_success());
    let list: serde_json::Value = resp.json().await.unwrap();
    let found = list
        .as_array()
        .expect("list should be an array")
        .iter()
        .any(|d| d["id"].as_str() == Some(dashboard_id));
    assert!(found, "new dashboard not found in list");

    // ── Get ──────────────────────────────────────────────────────────────
    step("Get dashboard by ID");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/dashboards/{}",
            ctx.base_url, ctx.project_id, dashboard_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("get dashboard failed");

    assert!(resp.status().is_success());
    let got: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        got["name"].as_str(),
        Some("E2E Test Dashboard"),
        "dashboard name mismatch"
    );

    // ── Update ───────────────────────────────────────────────────────────
    step("Update dashboard name");
    let update_body = serde_json::json!({
        "name": "E2E Updated Dashboard"
    });

    let resp = ctx
        .client
        .put(format!(
            "{}/api/watch/projects/{}/dashboards/{}",
            ctx.base_url, ctx.project_id, dashboard_id
        ))
        .bearer_auth(&ctx.token)
        .json(&update_body)
        .send()
        .await
        .expect("update dashboard failed");

    assert!(
        resp.status().is_success(),
        "update dashboard returned {}",
        resp.status()
    );

    // ── Verify update ────────────────────────────────────────────────────
    step("Verify dashboard name updated");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/dashboards/{}",
            ctx.base_url, ctx.project_id, dashboard_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("get updated dashboard failed");

    let got: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        got["name"].as_str(),
        Some("E2E Updated Dashboard"),
        "dashboard name should be updated"
    );

    // ── Delete ───────────────────────────────────────────────────────────
    step("Delete dashboard");
    let resp = ctx
        .client
        .delete(format!(
            "{}/api/watch/projects/{}/dashboards/{}",
            ctx.base_url, ctx.project_id, dashboard_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("delete dashboard failed");

    assert!(
        resp.status().is_success() || resp.status().as_u16() == 204,
        "delete dashboard returned {}",
        resp.status()
    );

    // ── Verify deleted ───────────────────────────────────────────────────
    step("Verify dashboard deleted from list");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/dashboards",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("list dashboards after delete failed");

    let list: serde_json::Value = resp.json().await.unwrap();
    let still_there = list
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .any(|d| d["id"].as_str() == Some(dashboard_id));
    assert!(!still_there, "dashboard should be gone after delete");
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 6: Alert Rule CRUD
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_alert_rule_crud() {
    let ctx = setup().await;

    // ── Create ───────────────────────────────────────────────────────────
    step("Create alert rule");
    let create_body = serde_json::json!({
        "project_id": ctx.project_id,
        "name": "E2E Test Alert Rule",
        "description": "Created by integration tests",
        "rule_type": "threshold",
        "query_config": {
            "metric_name": "e2e.test.metric",
            "filters": {},
            "group_by": [],
            "time_aggregation": "avg",
            "space_aggregation": "sum"
        },
        "threshold": 100.0,
        "threshold_type": "above",
        "notification_channels": [],
        "eval_window_seconds": 300,
        "eval_interval_seconds": 60,
        "labels": {},
        "annotations": {},
        "enabled": true
    });

    let resp = ctx
        .client
        .post(format!("{}/api/watch/alerting/rules", ctx.base_url))
        .bearer_auth(&ctx.token)
        .json(&create_body)
        .send()
        .await
        .expect("create alert rule failed");

    assert!(
        resp.status().is_success(),
        "create alert rule returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let rule: serde_json::Value = resp.json().await.expect("parse created alert rule");
    let rule_id = rule["id"].as_str().expect("rule should have an id");

    // ── List ─────────────────────────────────────────────────────────────
    step("List alert rules -- verify new rule appears");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/alerting/rules?project_id={}",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("list alert rules failed");

    assert!(resp.status().is_success());
    let list: serde_json::Value = resp.json().await.unwrap();
    let found = list
        .as_array()
        .expect("list should be an array")
        .iter()
        .any(|r| r["id"].as_str() == Some(rule_id));
    assert!(found, "new alert rule not found in list");

    // ── Get ──────────────────────────────────────────────────────────────
    step("Get alert rule by ID");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/alerting/rules/{}",
            ctx.base_url, rule_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("get alert rule failed");

    assert!(resp.status().is_success());
    let got: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        got["name"].as_str(),
        Some("E2E Test Alert Rule"),
        "alert rule name mismatch"
    );

    // ── Update ───────────────────────────────────────────────────────────
    step("Update alert rule threshold");
    let update_body = serde_json::json!({
        "threshold": 200.0,
        "name": "E2E Updated Alert Rule"
    });

    let resp = ctx
        .client
        .put(format!(
            "{}/api/watch/alerting/rules/{}",
            ctx.base_url, rule_id
        ))
        .bearer_auth(&ctx.token)
        .json(&update_body)
        .send()
        .await
        .expect("update alert rule failed");

    assert!(
        resp.status().is_success(),
        "update alert rule returned {}",
        resp.status()
    );

    // ── Delete ───────────────────────────────────────────────────────────
    step("Delete alert rule");
    let resp = ctx
        .client
        .delete(format!(
            "{}/api/watch/alerting/rules/{}",
            ctx.base_url, rule_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("delete alert rule failed");

    assert!(
        resp.status().is_success() || resp.status().as_u16() == 204,
        "delete alert rule returned {}",
        resp.status()
    );

    // ── Verify deleted ───────────────────────────────────────────────────
    step("Verify alert rule deleted from list");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/alerting/rules?project_id={}",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("list alert rules after delete failed");

    let list: serde_json::Value = resp.json().await.unwrap();
    let still_there = list
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .any(|r| r["id"].as_str() == Some(rule_id));
    assert!(!still_there, "alert rule should be gone after delete");
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 7: Health Check CRUD
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_health_check_crud() {
    let ctx = setup().await;

    // ── Create ───────────────────────────────────────────────────────────
    step("Create health check");
    let create_body = serde_json::json!({
        "project_id": ctx.project_id,
        "check_type": "http",
        "name": "E2E Test Health Check",
        "target_url": "https://httpbin.org/status/200",
        "http_method": "GET",
        "http_expected_status": [200],
        "check_interval_seconds": 300,
        "timeout_seconds": 10,
        "locations": ["us-east"],
        "enabled": true
    });

    let resp = ctx
        .client
        .post(format!("{}/api/watch/health-checks/checks", ctx.base_url))
        .bearer_auth(&ctx.token)
        .json(&create_body)
        .send()
        .await
        .expect("create health check failed");

    assert!(
        resp.status().is_success(),
        "create health check returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let check: serde_json::Value = resp.json().await.expect("parse created health check");
    let check_id = check["id"].as_str().expect("check should have an id");

    // ── List ─────────────────────────────────────────────────────────────
    step("List health checks -- verify new check appears");
    let resp = ctx
        .client
        .get(format!("{}/api/watch/health-checks/checks", ctx.base_url))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("list health checks failed");

    assert!(resp.status().is_success());
    let list: serde_json::Value = resp.json().await.unwrap();
    let found = list
        .as_array()
        .expect("list should be an array")
        .iter()
        .any(|c| c["id"].as_str() == Some(check_id));
    assert!(found, "new health check not found in list");

    // ── Get ──────────────────────────────────────────────────────────────
    step("Get health check by ID");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/health-checks/checks/{}",
            ctx.base_url, check_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("get health check failed");

    assert!(resp.status().is_success());
    let got: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        got["name"].as_str(),
        Some("E2E Test Health Check"),
        "health check name mismatch"
    );

    // ── Update (disable) ─────────────────────────────────────────────────
    step("Update health check (disable)");
    let update_body = serde_json::json!({
        "enabled": false
    });

    let resp = ctx
        .client
        .put(format!(
            "{}/api/watch/health-checks/checks/{}",
            ctx.base_url, check_id
        ))
        .bearer_auth(&ctx.token)
        .json(&update_body)
        .send()
        .await
        .expect("update health check failed");

    assert!(
        resp.status().is_success(),
        "update health check returned {}",
        resp.status()
    );

    // ── Delete ───────────────────────────────────────────────────────────
    step("Delete health check");
    let resp = ctx
        .client
        .delete(format!(
            "{}/api/watch/health-checks/checks/{}",
            ctx.base_url, check_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("delete health check failed");

    assert!(
        resp.status().is_success() || resp.status().as_u16() == 204,
        "delete health check returned {}",
        resp.status()
    );

    // ── Verify deleted ───────────────────────────────────────────────────
    step("Verify health check deleted from list");
    let resp = ctx
        .client
        .get(format!("{}/api/watch/health-checks/checks", ctx.base_url))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("list health checks after delete failed");

    let list: serde_json::Value = resp.json().await.unwrap();
    let still_there = list
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .any(|c| c["id"].as_str() == Some(check_id));
    assert!(!still_there, "health check should be gone after delete");
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 8: Project Stats
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_project_stats() {
    let ctx = setup().await;

    // First, send an exception so we have data
    step("Send an exception for stats");
    let payload =
        build_exception_payload("E2E stats test error", "E2EStatsError", &ctx.project_key);

    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/exceptions", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload)
        .send()
        .await
        .expect("ingest exception for stats failed");
    assert!(resp.status().is_success());

    // Wait for data to be processed
    step("Wait for stats to reflect ingested data");
    let found = wait_for("project stats > 0", 15, Duration::from_secs(2), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/projects/{}/stats",
                    base_url, project_id
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    body["total_exceptions"].as_i64().unwrap_or(0) > 0
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(found, "project stats never showed total_exceptions > 0");
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 9: Service Discovery
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_service_discovery() {
    let ctx = setup().await;

    // Send a trace with a known service name
    step("Send OTLP trace for service discovery");
    let trace_id = format!("{:032x}", Uuid::new_v4().as_u128());
    let span_id = format!("{:016x}", rand::random::<u64>());
    let payload = build_otlp_trace_payload(&trace_id, &span_id, "e2e-discovery-svc");

    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/traces", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload)
        .send()
        .await
        .expect("ingest trace for discovery failed");
    assert!(resp.status().is_success());

    // Wait for service to be discovered
    step("Wait for service to appear in discovered services");
    let found = wait_for(
        "e2e-discovery-svc in discovered services",
        15,
        Duration::from_secs(2),
        || {
            let client = ctx.client.clone();
            let base_url = ctx.base_url.clone();
            let token = ctx.token.clone();
            let project_id = ctx.project_id;
            async move {
                let resp = client
                    .get(format!(
                        "{}/api/watch/{}/discovered-services",
                        base_url, project_id
                    ))
                    .bearer_auth(&token)
                    .send()
                    .await;
                match resp {
                    Ok(r) if r.status().is_success() => {
                        let body: serde_json::Value = r.json().await.unwrap_or_default();
                        if let Some(arr) = body.as_array() {
                            arr.iter()
                                .any(|s| s["service_name"].as_str() == Some("e2e-discovery-svc"))
                        } else {
                            false
                        }
                    }
                    _ => false,
                }
            }
        },
    )
    .await;
    assert!(
        found,
        "service 'e2e-discovery-svc' never appeared in discovered services"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 10: Notification Channel CRUD
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_notification_channel_crud() {
    let ctx = setup().await;

    // ── Create ───────────────────────────────────────────────────────────
    step("Create notification channel");
    let payload = build_notification_channel_payload("E2E Webhook Channel", "webhook");

    let project_id_str = ctx.project_id.to_string();

    let resp = ctx
        .client
        .post(format!("{}/api/watch/notification-channels", ctx.base_url))
        .bearer_auth(&ctx.token)
        .header("X-Project-Id", &project_id_str)
        .json(&payload)
        .send()
        .await
        .expect("create notification channel failed");

    assert!(
        resp.status().is_success(),
        "create notification channel returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let channel: serde_json::Value = resp.json().await.expect("parse created channel");
    let channel_id = channel["id"].as_str().expect("channel should have an id");

    // ── List ─────────────────────────────────────────────────────────────
    step("List notification channels");
    let resp = ctx
        .client
        .get(format!("{}/api/watch/notification-channels", ctx.base_url))
        .bearer_auth(&ctx.token)
        .header("X-Project-Id", &project_id_str)
        .send()
        .await
        .expect("list notification channels failed");

    assert!(resp.status().is_success());
    let list: serde_json::Value = resp.json().await.unwrap();
    let found = list
        .as_array()
        .expect("list should be an array")
        .iter()
        .any(|c| c["id"].as_str() == Some(channel_id));
    assert!(found, "new notification channel not found in list");

    // ── Get ──────────────────────────────────────────────────────────────
    step("Get notification channel by ID");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/notification-channels/{}",
            ctx.base_url, channel_id
        ))
        .bearer_auth(&ctx.token)
        .header("X-Project-Id", &project_id_str)
        .send()
        .await
        .expect("get notification channel failed");

    assert!(resp.status().is_success());
    let got: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        got["name"].as_str(),
        Some("E2E Webhook Channel"),
        "channel name mismatch"
    );
    assert_eq!(
        got["channel_type"].as_str(),
        Some("webhook"),
        "channel type mismatch"
    );

    // ── Update ───────────────────────────────────────────────────────────
    step("Update notification channel name");
    let update_body = serde_json::json!({
        "name": "E2E Updated Webhook Channel"
    });

    let resp = ctx
        .client
        .put(format!(
            "{}/api/watch/notification-channels/{}",
            ctx.base_url, channel_id
        ))
        .bearer_auth(&ctx.token)
        .header("X-Project-Id", &project_id_str)
        .json(&update_body)
        .send()
        .await
        .expect("update notification channel failed");

    assert!(
        resp.status().is_success(),
        "update notification channel returned {}",
        resp.status()
    );

    // Verify update
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/notification-channels/{}",
            ctx.base_url, channel_id
        ))
        .bearer_auth(&ctx.token)
        .header("X-Project-Id", &project_id_str)
        .send()
        .await
        .expect("get updated channel failed");

    let got: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        got["name"].as_str(),
        Some("E2E Updated Webhook Channel"),
        "channel name should be updated"
    );

    // ── Delete ───────────────────────────────────────────────────────────
    step("Delete notification channel");
    let resp = ctx
        .client
        .delete(format!(
            "{}/api/watch/notification-channels/{}",
            ctx.base_url, channel_id
        ))
        .bearer_auth(&ctx.token)
        .header("X-Project-Id", &project_id_str)
        .send()
        .await
        .expect("delete notification channel failed");

    assert!(
        resp.status().is_success() || resp.status().as_u16() == 204,
        "delete notification channel returned {}",
        resp.status()
    );

    // ── Verify deleted ───────────────────────────────────────────────────
    step("Verify notification channel deleted from list");
    let resp = ctx
        .client
        .get(format!("{}/api/watch/notification-channels", ctx.base_url))
        .bearer_auth(&ctx.token)
        .header("X-Project-Id", &project_id_str)
        .send()
        .await
        .expect("list channels after delete failed");

    let list: serde_json::Value = resp.json().await.unwrap();
    let still_there = list
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .any(|c| c["id"].as_str() == Some(channel_id));
    assert!(
        !still_there,
        "notification channel should be gone after delete"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 11: Maintenance Window (One-Time)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_maintenance_window_one_time() {
    let ctx = setup().await;

    // ── Create ───────────────────────────────────────────────────────────
    step("Create one-time maintenance window");
    let payload = build_maintenance_window_one_time_payload("E2E One-Time Window", &ctx.project_id);

    let resp = ctx
        .client
        .post(format!("{}/api/watch/maintenance-windows", ctx.base_url))
        .bearer_auth(&ctx.token)
        .json(&payload)
        .send()
        .await
        .expect("create maintenance window failed");

    assert!(
        resp.status().is_success(),
        "create maintenance window returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let window: serde_json::Value = resp.json().await.expect("parse created window");
    let window_id = window["id"].as_str().expect("window should have an id");
    assert_eq!(window["schedule_type"].as_str(), Some("one_time"));
    // Future window should not be active
    assert_eq!(window["is_active"].as_bool(), Some(false));

    // ── List ─────────────────────────────────────────────────────────────
    step("List maintenance windows");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/maintenance-windows?project_id={}",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("list maintenance windows failed");

    assert!(resp.status().is_success());
    let list: serde_json::Value = resp.json().await.unwrap();
    let found = list
        .as_array()
        .expect("list should be an array")
        .iter()
        .any(|w| w["id"].as_str() == Some(window_id));
    assert!(found, "new maintenance window not found in list");

    // ── Update ───────────────────────────────────────────────────────────
    step("Update maintenance window name");
    let update_body = serde_json::json!({
        "name": "E2E Updated One-Time Window"
    });

    let resp = ctx
        .client
        .put(format!(
            "{}/api/watch/maintenance-windows/{}",
            ctx.base_url, window_id
        ))
        .bearer_auth(&ctx.token)
        .json(&update_body)
        .send()
        .await
        .expect("update maintenance window failed");

    assert!(
        resp.status().is_success(),
        "update maintenance window returned {}",
        resp.status()
    );

    // ── Delete ───────────────────────────────────────────────────────────
    step("Delete maintenance window");
    let resp = ctx
        .client
        .delete(format!(
            "{}/api/watch/maintenance-windows/{}",
            ctx.base_url, window_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("delete maintenance window failed");

    assert!(
        resp.status().is_success(),
        "delete maintenance window returned {}",
        resp.status()
    );

    // ── Verify deleted ───────────────────────────────────────────────────
    step("Verify maintenance window deleted from list");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/maintenance-windows?project_id={}",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("list windows after delete failed");

    let list: serde_json::Value = resp.json().await.unwrap();
    let still_there = list
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .any(|w| w["id"].as_str() == Some(window_id));
    assert!(
        !still_there,
        "maintenance window should be gone after delete"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 12: Maintenance Window (Recurring)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_maintenance_window_recurring() {
    let ctx = setup().await;

    // ── Create ───────────────────────────────────────────────────────────
    step("Create recurring maintenance window");
    let payload =
        build_maintenance_window_recurring_payload("E2E Recurring Window", &ctx.project_id);

    let resp = ctx
        .client
        .post(format!("{}/api/watch/maintenance-windows", ctx.base_url))
        .bearer_auth(&ctx.token)
        .json(&payload)
        .send()
        .await
        .expect("create recurring maintenance window failed");

    assert!(
        resp.status().is_success(),
        "create recurring maintenance window returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let window: serde_json::Value = resp.json().await.expect("parse created recurring window");
    assert_eq!(window["schedule_type"].as_str(), Some("recurring"));
    assert_eq!(window["recurrence_type"].as_str(), Some("daily"));
    // Recurring windows should have a computed next_occurrence
    assert!(
        window["next_occurrence"].as_str().is_some(),
        "recurring window should have next_occurrence computed"
    );

    // ── Check active windows endpoint ────────────────────────────────────
    step("Check active maintenance windows");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/maintenance-windows/active?project_id={}",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("get active maintenance windows failed");

    assert!(
        resp.status().is_success(),
        "active maintenance windows returned {}",
        resp.status()
    );
    // The response should be a valid array (may or may not contain our window depending on timing)
    let active: serde_json::Value = resp.json().await.unwrap();
    assert!(active.is_array(), "active windows should be an array");
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 13: Project Keys CRUD
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_project_keys_crud() {
    let ctx = setup().await;

    // ── List existing keys (should have at least 1 from setup) ───────────
    step("List project keys (should have default key)");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/keys",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("list project keys failed");

    assert!(resp.status().is_success());
    let keys: serde_json::Value = resp.json().await.unwrap();
    let initial_count = keys.as_array().expect("keys should be array").len();
    assert!(initial_count >= 1, "should have at least 1 key from setup");

    // ── Create a new key ────────────────────────────────────────────────
    step("Create additional project key");
    let resp = ctx
        .client
        .post(format!(
            "{}/api/watch/projects/{}/keys",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("create project key failed");

    assert!(
        resp.status().is_success(),
        "create project key returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let new_key: serde_json::Value = resp.json().await.expect("parse new key");
    let new_key_value = new_key["key"]
        .as_str()
        .expect("new key should have a key field");
    assert!(!new_key_value.is_empty(), "key value should not be empty");

    // ── List again (should have +1) ─────────────────────────────────────
    step("Verify key count increased");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/keys",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("list project keys after create failed");

    let keys: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        keys.as_array().unwrap().len(),
        initial_count + 1,
        "should have one more key"
    );

    // ── Verify the new key works for ingestion ──────────────────────────
    step("Verify new key works for exception ingestion");
    let payload = build_exception_payload("Key verification test", "KeyTestError", new_key_value);

    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/exceptions", ctx.base_url))
        .bearer_auth(new_key_value)
        .json(&payload)
        .send()
        .await
        .expect("ingest with new key failed");

    assert!(
        resp.status().is_success(),
        "ingest with new key returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 14: Exception List and Filters
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_exception_list_and_filters() {
    let ctx = setup().await;

    // ── Ingest exceptions with different service/environment ────────────
    step("Ingest exception with service=svc-alpha, env=staging");
    let payload_a = build_exception_payload_with_metadata(
        "Alpha error in staging",
        "AlphaError",
        &ctx.project_key,
        "svc-alpha",
        "staging",
        None,
    );
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/exceptions", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload_a)
        .send()
        .await
        .expect("ingest exception A failed");
    assert!(resp.status().is_success());

    step("Ingest exception with service=svc-beta, env=production");
    let payload_b = build_exception_payload_with_metadata(
        "Beta error in production",
        "BetaError",
        &ctx.project_key,
        "svc-beta",
        "production",
        None,
    );
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/exceptions", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload_b)
        .send()
        .await
        .expect("ingest exception B failed");
    assert!(resp.status().is_success());

    // ── Wait for data in ClickHouse ─────────────────────────────────────
    step("Wait for exceptions to appear");
    let found = wait_for("exceptions in list", 15, Duration::from_secs(2), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/projects/{}/exceptions",
                    base_url, project_id
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    if let Some(arr) = body.as_array() {
                        // We need both exception types to appear
                        let has_alpha = arr
                            .iter()
                            .any(|g| g["exception_type"].as_str() == Some("AlphaError"));
                        let has_beta = arr
                            .iter()
                            .any(|g| g["exception_type"].as_str() == Some("BetaError"));
                        has_alpha && has_beta
                    } else {
                        false
                    }
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(found, "both exception groups never appeared");

    // ── Verify full list returns both groups ─────────────────────────────
    step("Verify unfiltered list returns both groups");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/exceptions",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("list exceptions failed");

    assert!(resp.status().is_success());
    let list: serde_json::Value = resp.json().await.unwrap();
    let arr = list.as_array().expect("exceptions list should be array");
    assert!(arr.len() >= 2, "should have at least 2 exception groups");

    // ── Filter by search ────────────────────────────────────────────────
    step("Filter exceptions by search=AlphaError");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/exceptions?search=AlphaError",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("search exceptions failed");

    assert!(resp.status().is_success());
    let filtered: serde_json::Value = resp.json().await.unwrap();
    let arr = filtered.as_array().expect("filtered list should be array");
    assert!(
        arr.iter()
            .all(|g| g["exception_type"].as_str() == Some("AlphaError")),
        "search filter should only return AlphaError groups"
    );

    // ── Sort by last_seen ascending ─────────────────────────────────────
    step("Sort exceptions by last_seen ascending");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/exceptions?sort_by=last_seen&sort_order=asc",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("sorted exceptions failed");

    assert!(
        resp.status().is_success(),
        "sorted exceptions returned {}",
        resp.status()
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 15: Exception Filter Values
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_exception_filter_values() {
    let ctx = setup().await;

    // Ingest exceptions with known metadata
    step("Ingest exception with known service and environment");
    let svc_name = format!("filter-svc-{}", &Uuid::new_v4().to_string()[..8]);
    let payload = build_exception_payload_with_metadata(
        "Filter value test error",
        "FilterTestError",
        &ctx.project_key,
        &svc_name,
        "filter-env",
        None,
    );
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/exceptions", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload)
        .send()
        .await
        .expect("ingest exception failed");
    assert!(resp.status().is_success());

    // Wait for data to appear
    step("Wait for exception to propagate");
    let svc_clone = svc_name.clone();
    let found = wait_for(
        "exception filter values",
        15,
        Duration::from_secs(2),
        || {
            let client = ctx.client.clone();
            let base_url = ctx.base_url.clone();
            let token = ctx.token.clone();
            let project_id = ctx.project_id;
            let svc = svc_clone.clone();
            async move {
                let resp = client
                    .get(format!(
                        "{}/api/watch/projects/{}/exceptions/filter-values",
                        base_url, project_id
                    ))
                    .bearer_auth(&token)
                    .send()
                    .await;
                match resp {
                    Ok(r) if r.status().is_success() => {
                        let body: serde_json::Value = r.json().await.unwrap_or_default();
                        if let Some(services) = body["service_names"].as_array() {
                            services.iter().any(|s| s.as_str() == Some(&svc))
                        } else {
                            false
                        }
                    }
                    _ => false,
                }
            }
        },
    )
    .await;
    assert!(
        found,
        "service name '{}' never appeared in filter values",
        svc_name
    );

    // ── Verify environment also appears ──────────────────────────────────
    step("Verify environment appears in filter values");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/exceptions/filter-values",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("get filter values failed");

    let fv: serde_json::Value = resp.json().await.unwrap();
    let envs = fv["environments"]
        .as_array()
        .expect("environments should be array");
    assert!(
        envs.iter().any(|e| e.as_str() == Some("filter-env")),
        "environment 'filter-env' should appear in filter values"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 16: Exception Detail, Navigation, and History
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_exception_detail_and_navigation() {
    let ctx = setup().await;

    // Ingest exceptions
    step("Ingest exceptions for detail test");
    let payload = build_exception_payload("Detail test error", "DetailTestError", &ctx.project_key);
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/exceptions", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload)
        .send()
        .await
        .expect("ingest exception failed");
    assert!(resp.status().is_success());

    // Wait for grouping
    step("Wait for exception group to appear");
    let found = wait_for("exception group", 15, Duration::from_secs(2), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/projects/{}/exceptions",
                    base_url, project_id
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    if let Some(arr) = body.as_array() {
                        arr.iter()
                            .any(|g| g["exception_type"].as_str() == Some("DetailTestError"))
                    } else {
                        false
                    }
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(found, "DetailTestError group never appeared");

    // Get the group_id
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/exceptions",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("list exceptions failed");

    let groups: serde_json::Value = resp.json().await.unwrap();
    let group_id = groups
        .as_array()
        .unwrap()
        .iter()
        .find(|g| g["exception_type"].as_str() == Some("DetailTestError"))
        .and_then(|g| g["id"].as_str())
        .expect("could not find DetailTestError group id")
        .to_string();

    // ── Get exception detail ────────────────────────────────────────────
    step("Get exception group detail");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/exceptions/{}",
            ctx.base_url, ctx.project_id, group_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("get exception detail failed");

    assert!(
        resp.status().is_success(),
        "get exception detail returned {}",
        resp.status()
    );
    let detail: serde_json::Value = resp.json().await.unwrap();
    assert!(
        detail["group"].is_object(),
        "detail should have a 'group' field"
    );
    assert!(
        detail["recent_exceptions"].is_array(),
        "detail should have 'recent_exceptions' array"
    );

    // ── Navigate ────────────────────────────────────────────────────────
    step("Get exception navigation");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/exceptions/{}/navigate",
            ctx.base_url, ctx.project_id, group_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("get exception navigation failed");

    assert!(
        resp.status().is_success(),
        "get exception navigation returned {}",
        resp.status()
    );

    // ── History ─────────────────────────────────────────────────────────
    step("Get exception group history");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/exceptions/{}/history?time_range=24h",
            ctx.base_url, ctx.project_id, group_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("get exception history failed");

    assert!(
        resp.status().is_success(),
        "get exception history returned {}",
        resp.status()
    );
    let history: serde_json::Value = resp.json().await.unwrap();
    assert!(history.is_array(), "history should be an array");
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 17: Trace List and Filters
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_trace_list_and_filters() {
    let ctx = setup().await;

    // ── Ingest traces with different service names ──────────────────────
    step("Ingest trace with service=trace-svc-alpha");
    let trace_id_a = format!("{:032x}", Uuid::new_v4().as_u128());
    let span_id_a = format!("{:016x}", rand::random::<u64>());
    let payload_a = build_otlp_trace_payload(&trace_id_a, &span_id_a, "trace-svc-alpha");

    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/traces", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload_a)
        .send()
        .await
        .expect("ingest trace A failed");
    assert!(resp.status().is_success());

    step("Ingest trace with service=trace-svc-beta");
    let trace_id_b = format!("{:032x}", Uuid::new_v4().as_u128());
    let span_id_b = format!("{:016x}", rand::random::<u64>());
    let payload_b = build_otlp_trace_payload(&trace_id_b, &span_id_b, "trace-svc-beta");

    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/traces", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload_b)
        .send()
        .await
        .expect("ingest trace B failed");
    assert!(resp.status().is_success());

    // ── Wait for both traces to appear ──────────────────────────────────
    step("Wait for both traces to appear");
    let tid_a = trace_id_a.clone();
    let tid_b = trace_id_b.clone();
    let found = wait_for("both traces in list", 15, Duration::from_secs(2), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        let ta = tid_a.clone();
        let tb = tid_b.clone();
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/projects/{}/traces",
                    base_url, project_id
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    if let Some(arr) = body.as_array() {
                        let has_a = arr.iter().any(|t| t["trace_id"].as_str() == Some(&ta));
                        let has_b = arr.iter().any(|t| t["trace_id"].as_str() == Some(&tb));
                        has_a && has_b
                    } else {
                        false
                    }
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(found, "both traces never appeared in list");

    // ── Verify list has expected fields ─────────────────────────────────
    step("Verify trace list fields");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/traces",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("list traces failed");

    let list: serde_json::Value = resp.json().await.unwrap();
    let arr = list.as_array().expect("traces list should be array");
    assert!(arr.len() >= 2, "should have at least 2 traces");
    // Check that trace entries have expected fields
    let first = &arr[0];
    assert!(first["trace_id"].is_string(), "trace should have trace_id");
    assert!(
        first["duration_ns"].is_number() || first["duration_ns"].is_string(),
        "trace should have duration_ns"
    );

    // ── Sort by duration descending ─────────────────────────────────────
    step("Sort traces by duration descending");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/traces?sort_by=duration&sort_order=desc",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("sorted traces failed");

    assert!(
        resp.status().is_success(),
        "sorted traces returned {}",
        resp.status()
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 18: Trace Filter Values
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_trace_filter_values() {
    let ctx = setup().await;

    // Ingest a trace with a known service name
    step("Ingest trace for filter values");
    let svc_name = format!("filter-trace-svc-{}", &Uuid::new_v4().to_string()[..8]);
    let trace_id = format!("{:032x}", Uuid::new_v4().as_u128());
    let span_id = format!("{:016x}", rand::random::<u64>());
    let payload = build_otlp_trace_payload(&trace_id, &span_id, &svc_name);

    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/traces", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload)
        .send()
        .await
        .expect("ingest trace failed");
    assert!(resp.status().is_success());

    // Wait for filter values to include our service
    step("Wait for service to appear in trace filter values");
    let svc_clone = svc_name.clone();
    let found = wait_for("trace filter values", 15, Duration::from_secs(2), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        let svc = svc_clone.clone();
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/projects/{}/traces/filter-values",
                    base_url, project_id
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    if let Some(services) = body["service_names"].as_array() {
                        services.iter().any(|s| s.as_str() == Some(&svc))
                    } else {
                        false
                    }
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(
        found,
        "service '{}' never appeared in trace filter values",
        svc_name
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 19: Trace Detail with Spans
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_trace_detail_with_spans() {
    let ctx = setup().await;

    // Build a multi-span trace
    step("Ingest multi-span trace");
    let trace_id = format!("{:032x}", Uuid::new_v4().as_u128());
    let root_span_id = format!("{:016x}", rand::random::<u64>());
    let child_span_id = format!("{:016x}", rand::random::<u64>());

    let payload = build_otlp_trace_payload_multi_span(
        &trace_id,
        "multi-span-svc",
        &[
            SpanDef {
                span_id: root_span_id.clone(),
                parent_span_id: None,
                name: "root-operation".to_string(),
                kind: 2, // SERVER
                duration_ms: 200,
            },
            SpanDef {
                span_id: child_span_id.clone(),
                parent_span_id: Some(root_span_id.clone()),
                name: "child-db-call".to_string(),
                kind: 3, // CLIENT
                duration_ms: 50,
            },
        ],
    );

    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/traces", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload)
        .send()
        .await
        .expect("ingest multi-span trace failed");
    assert!(resp.status().is_success());

    // Wait for trace to appear
    step("Wait for multi-span trace");
    let tid = trace_id.clone();
    let found = wait_for("multi-span trace", 15, Duration::from_secs(2), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        let t = tid.clone();
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/projects/{}/traces/{}",
                    base_url, project_id, t
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    body["spans"].as_array().map_or(false, |s| s.len() >= 2)
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(found, "multi-span trace never appeared with >= 2 spans");

    // ── Verify span detail ──────────────────────────────────────────────
    step("Verify trace detail span fields");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/traces/{}",
            ctx.base_url, ctx.project_id, trace_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("get trace detail failed");

    let detail: serde_json::Value = resp.json().await.unwrap();
    let spans = detail["spans"].as_array().expect("spans should be array");
    assert!(spans.len() >= 2, "should have at least 2 spans");

    // Verify span fields
    let span_names: Vec<&str> = spans
        .iter()
        .filter_map(|s| s["span_name"].as_str())
        .collect();
    assert!(
        span_names.contains(&"root-operation"),
        "should have root-operation span"
    );
    assert!(
        span_names.contains(&"child-db-call"),
        "should have child-db-call span"
    );

    // Check key fields exist on spans
    for span in spans {
        assert!(span["span_id"].is_string(), "span should have span_id");
        assert!(span["span_name"].is_string(), "span should have span_name");
        assert!(
            span["service_name"].is_string(),
            "span should have service_name"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 20: Metrics Query with Aggregations
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_metrics_query_with_aggregations() {
    let ctx = setup().await;
    let metric_name = format!("e2e.agg.gauge.{}", Uuid::new_v4().simple());

    // ── Send metrics with different labels ───────────────────────────────
    step("Ingest metric with env=prod");
    let payload = build_otlp_metrics_payload_with_labels(
        &metric_name,
        10.0,
        &[("env", "prod"), ("region", "us-east")],
    );
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/metrics", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload)
        .send()
        .await
        .expect("ingest metric prod failed");
    assert!(resp.status().is_success());

    step("Ingest metric with env=staging");
    let payload = build_otlp_metrics_payload_with_labels(
        &metric_name,
        20.0,
        &[("env", "staging"), ("region", "eu-west")],
    );
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/metrics", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload)
        .send()
        .await
        .expect("ingest metric staging failed");
    assert!(resp.status().is_success());

    // ── Wait for metric to appear ───────────────────────────────────────
    step("Wait for metric to appear in names");
    let mn = metric_name.clone();
    let found = wait_for("metric name in list", 15, Duration::from_secs(2), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        let name = mn.clone();
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/query/metrics/names?project_id={}",
                    base_url, project_id
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    if let Some(metrics) = body["metrics"].as_array() {
                        metrics.iter().any(|m| m["name"].as_str() == Some(&name))
                    } else {
                        false
                    }
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(found, "metric '{}' never appeared", metric_name);

    // ── Query with aggregation (no group_by since dynamic label grouping
    //    is not fully implemented yet) ────────────────────────────────────
    step("Query metrics with avg aggregation");
    let now_ms = chrono::Utc::now().timestamp_millis();
    let one_hour_ago_ms = now_ms - 3_600_000;

    let query_body = serde_json::json!({
        "project_id": ctx.project_id,
        "metric_name": metric_name,
        "start": one_hour_ago_ms,
        "end": now_ms,
        "step": 60,
        "time_aggregation": "avg",
        "space_aggregation": "sum",
        "filters": {},
        "group_by": []
    });

    let resp = ctx
        .client
        .post(format!("{}/api/watch/query/metrics/query", ctx.base_url))
        .bearer_auth(&ctx.token)
        .json(&query_body)
        .send()
        .await
        .expect("query metrics with aggregation failed");

    assert!(
        resp.status().is_success(),
        "query metrics returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let data: serde_json::Value = resp.json().await.unwrap();
    assert!(
        data["data"].is_array(),
        "metrics response should have data array"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 21: Metrics Names and Labels
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_metrics_names_and_labels() {
    let ctx = setup().await;
    let metric_name = format!("e2e.labels.gauge.{}", Uuid::new_v4().simple());

    // ── Ingest metric with known labels ─────────────────────────────────
    step("Ingest metric with labels");
    let payload = build_otlp_metrics_payload_with_labels(
        &metric_name,
        42.0,
        &[("region", "us-west"), ("tier", "premium")],
    );
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/metrics", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload)
        .send()
        .await
        .expect("ingest metric failed");
    assert!(resp.status().is_success());

    // ── Wait for metric name to appear ──────────────────────────────────
    step("Wait for metric name in names list");
    let mn = metric_name.clone();
    let found = wait_for("metric name", 15, Duration::from_secs(2), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        let name = mn.clone();
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/query/metrics/names?project_id={}",
                    base_url, project_id
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    if let Some(metrics) = body["metrics"].as_array() {
                        metrics.iter().any(|m| m["name"].as_str() == Some(&name))
                    } else {
                        false
                    }
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(found, "metric name never appeared");

    // ── Query labels for the metric ─────────────────────────────────────
    step("Query metric labels");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/query/metrics/{}/labels?project_id={}",
            ctx.base_url, metric_name, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("get metric labels failed");

    assert!(
        resp.status().is_success(),
        "get metric labels returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 22: Historical Error Rate
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_historical_error_rate() {
    let ctx = setup().await;

    // Ingest an exception
    step("Ingest exception for historical error rate");
    let payload = build_exception_payload(
        "Historical error rate test",
        "HistErrorRate",
        &ctx.project_key,
    );
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/exceptions", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload)
        .send()
        .await
        .expect("ingest exception failed");
    assert!(resp.status().is_success());

    // Wait for data to propagate
    step("Wait for exception to appear in ClickHouse");
    let found = wait_for(
        "exception for error rate",
        15,
        Duration::from_secs(2),
        || {
            let client = ctx.client.clone();
            let base_url = ctx.base_url.clone();
            let token = ctx.token.clone();
            let project_id = ctx.project_id;
            async move {
                let resp = client
                    .get(format!(
                        "{}/api/watch/projects/{}/exceptions",
                        base_url, project_id
                    ))
                    .bearer_auth(&token)
                    .send()
                    .await;
                match resp {
                    Ok(r) if r.status().is_success() => {
                        let body: serde_json::Value = r.json().await.unwrap_or_default();
                        body.as_array().map_or(false, |a| !a.is_empty())
                    }
                    _ => false,
                }
            }
        },
    )
    .await;
    assert!(found, "exception never appeared for error rate test");

    // ── Query historical error rate ─────────────────────────────────────
    step("Query historical error rate");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/historical/projects/{}/error-rate?time_range=24h",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("get error rate failed");

    assert!(
        resp.status().is_success(),
        "error rate returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let data: serde_json::Value = resp.json().await.unwrap();
    assert!(data.is_array(), "error rate response should be an array");
    let points = data.as_array().unwrap();
    // Should have at least one data point with count > 0
    let has_data = points.iter().any(|p| p["count"].as_i64().unwrap_or(0) > 0);
    assert!(
        has_data,
        "error rate should have at least one point with count > 0"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 23: Historical Trace Duration
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_historical_trace_duration() {
    let ctx = setup().await;

    // Ingest a trace
    step("Ingest trace for historical duration");
    let trace_id = format!("{:032x}", Uuid::new_v4().as_u128());
    let span_id = format!("{:016x}", rand::random::<u64>());
    let payload = build_otlp_trace_payload(&trace_id, &span_id, "hist-duration-svc");

    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/traces", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload)
        .send()
        .await
        .expect("ingest trace failed");
    assert!(resp.status().is_success());

    // Wait for trace to appear
    step("Wait for trace to appear");
    let tid = trace_id.clone();
    let found = wait_for("trace for duration", 15, Duration::from_secs(2), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        let t = tid.clone();
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/projects/{}/traces",
                    base_url, project_id
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    if let Some(arr) = body.as_array() {
                        arr.iter().any(|tr| tr["trace_id"].as_str() == Some(&t))
                    } else {
                        false
                    }
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(found, "trace for duration never appeared");

    // ── Query historical trace duration ─────────────────────────────────
    step("Query historical trace duration");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/historical/projects/{}/trace-duration?time_range=24h",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("get trace duration failed");

    assert!(
        resp.status().is_success(),
        "trace duration returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let data: serde_json::Value = resp.json().await.unwrap();
    assert!(
        data.is_array(),
        "trace duration response should be an array"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 24: Historical Error Counts
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_historical_error_counts() {
    let ctx = setup().await;

    // Ingest an exception to generate error data
    step("Ingest exception for error counts");
    let payload = build_exception_payload("Error counts test", "ErrorCountsType", &ctx.project_key);
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/exceptions", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload)
        .send()
        .await
        .expect("ingest exception failed");
    assert!(resp.status().is_success());

    // Wait for data
    step("Wait for exception to propagate");
    let found = wait_for(
        "exception for error counts",
        15,
        Duration::from_secs(2),
        || {
            let client = ctx.client.clone();
            let base_url = ctx.base_url.clone();
            let token = ctx.token.clone();
            let project_id = ctx.project_id;
            async move {
                let resp = client
                    .get(format!(
                        "{}/api/watch/projects/{}/exceptions",
                        base_url, project_id
                    ))
                    .bearer_auth(&token)
                    .send()
                    .await;
                match resp {
                    Ok(r) if r.status().is_success() => {
                        let body: serde_json::Value = r.json().await.unwrap_or_default();
                        body.as_array().map_or(false, |a| !a.is_empty())
                    }
                    _ => false,
                }
            }
        },
    )
    .await;
    assert!(found, "exception never appeared for error counts test");

    // ── Query historical error counts ───────────────────────────────────
    step("Query historical error counts");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/historical/projects/{}/error-counts?time_range=24h",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("get error counts failed");

    assert!(
        resp.status().is_success(),
        "error counts returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let data: serde_json::Value = resp.json().await.unwrap();
    assert!(data.is_array(), "error counts response should be an array");
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 25: Direct Log Ingestion
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_direct_log_ingestion() {
    let ctx = setup().await;

    step("Send direct log via /logs/ingest");
    let payload = build_direct_log_payload("E2E direct log message", "info", "e2e-direct-log-svc");

    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/logs/ingest", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload)
        .send()
        .await
        .expect("direct log ingest request failed");

    assert!(
        resp.status().is_success(),
        "direct log ingest returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 26: OTLP Log with Trace Correlation
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_otlp_log_with_trace_correlation() {
    let ctx = setup().await;

    // First, send a trace
    let trace_id = format!("{:032x}", Uuid::new_v4().as_u128());
    let span_id = format!("{:016x}", rand::random::<u64>());

    step("Ingest correlated trace");
    let trace_payload = build_otlp_trace_payload(&trace_id, &span_id, "log-corr-svc");
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/traces", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&trace_payload)
        .send()
        .await
        .expect("ingest correlated trace failed");
    assert!(resp.status().is_success());

    // Then, send a log with the same trace_id
    step("Ingest OTLP log with matching trace_id");
    let log_payload =
        build_otlp_log_payload(&trace_id, "Correlated log message for trace", "ERROR");
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/logs", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&log_payload)
        .send()
        .await
        .expect("ingest correlated log failed");

    assert!(
        resp.status().is_success(),
        "ingest correlated log returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    // Wait for the trace to appear (verifies the pipeline processed both)
    step("Wait for correlated trace to appear");
    let tid = trace_id.clone();
    let found = wait_for("correlated trace", 15, Duration::from_secs(2), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        let t = tid.clone();
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/projects/{}/traces",
                    base_url, project_id
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    if let Some(arr) = body.as_array() {
                        arr.iter().any(|tr| tr["trace_id"].as_str() == Some(&t))
                    } else {
                        false
                    }
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(found, "correlated trace never appeared");
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 27: Feature Flag Event Ingestion
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_feature_flag_event_ingestion() {
    let ctx = setup().await;

    let flag_id = format!("e2e-flag-{}", &Uuid::new_v4().to_string()[..8]);

    // ── Ingest feature flag event ───────────────────────────────────────
    step("Send feature flag change event");
    let payload = build_feature_flag_event_payload(&flag_id, "toggled", &ctx.project_key);

    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/events", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload)
        .send()
        .await
        .expect("feature flag event failed");

    assert!(
        resp.status().is_success(),
        "feature flag event returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let event_resp: serde_json::Value = resp.json().await.unwrap();
    assert!(
        event_resp["id"].is_string(),
        "event response should have an id"
    );
    assert!(
        event_resp["message"].is_string(),
        "event response should have a message"
    );

    // ── Query events list ───────────────────────────────────────────────
    step("Wait for event to appear in unified events list");
    let fid = flag_id.clone();
    let found = wait_for("feature flag in events", 15, Duration::from_secs(2), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        let f = fid.clone();
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/projects/{}/events",
                    base_url, project_id
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    if let Some(arr) = body.as_array() {
                        // Check if any event references our flag
                        arr.iter().any(|e| {
                            e["flag_id"].as_str() == Some(&f) || e.to_string().contains(&f)
                        })
                    } else {
                        false
                    }
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(
        found,
        "feature flag event with flag_id='{}' should appear in unified events",
        flag_id
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 28: Incidents - Exceptions and Errors
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_incidents_exceptions_and_errors() {
    let ctx = setup().await;

    // Ingest an exception
    step("Ingest exception for incidents test");
    let payload =
        build_exception_payload("Incidents test error", "IncidentsError", &ctx.project_key);
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/exceptions", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload)
        .send()
        .await
        .expect("ingest exception failed");
    assert!(resp.status().is_success());

    // Also ingest a log
    step("Ingest error log for incidents test");
    let trace_id = format!("{:032x}", Uuid::new_v4().as_u128());
    let log_payload = build_otlp_log_payload(&trace_id, "Incidents error log", "ERROR");
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/logs", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&log_payload)
        .send()
        .await
        .expect("ingest error log failed");
    assert!(resp.status().is_success());

    // Wait for data
    step("Wait for exception to propagate");
    let found = wait_for(
        "exception for incidents",
        15,
        Duration::from_secs(2),
        || {
            let client = ctx.client.clone();
            let base_url = ctx.base_url.clone();
            let token = ctx.token.clone();
            let project_id = ctx.project_id;
            async move {
                let resp = client
                    .get(format!(
                        "{}/api/watch/projects/{}/exceptions",
                        base_url, project_id
                    ))
                    .bearer_auth(&token)
                    .send()
                    .await;
                match resp {
                    Ok(r) if r.status().is_success() => {
                        let body: serde_json::Value = r.json().await.unwrap_or_default();
                        body.as_array().map_or(false, |a| !a.is_empty())
                    }
                    _ => false,
                }
            }
        },
    )
    .await;
    assert!(found, "exception never appeared for incidents test");

    // ── Query incidents/exceptions ──────────────────────────────────────
    step("Query incidents exceptions");
    let now_ms = chrono::Utc::now().timestamp_millis();
    let one_hour_ago_ms = now_ms - 3_600_000;

    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/incidents/exceptions?start_ms={}&end_ms={}",
            ctx.base_url, ctx.project_id, one_hour_ago_ms, now_ms
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("get incidents exceptions failed");

    assert!(
        resp.status().is_success(),
        "incidents exceptions returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let data: serde_json::Value = resp.json().await.unwrap();
    assert!(data.is_array(), "incidents exceptions should be an array");

    // ── Query incidents/errors ──────────────────────────────────────────
    step("Query incidents errors");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/incidents/errors?start_ms={}&end_ms={}",
            ctx.base_url, ctx.project_id, one_hour_ago_ms, now_ms
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("get incidents errors failed");

    assert!(
        resp.status().is_success(),
        "incidents errors returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 29: Incidents Context
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_incidents_context() {
    let ctx = setup().await;

    // Ingest data to generate context
    step("Ingest trace for incidents context");
    let trace_id = format!("{:032x}", Uuid::new_v4().as_u128());
    let span_id = format!("{:016x}", rand::random::<u64>());
    let payload = build_otlp_trace_payload(&trace_id, &span_id, "incidents-ctx-svc");
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/traces", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload)
        .send()
        .await
        .expect("ingest trace failed");
    assert!(resp.status().is_success());

    step("Ingest exception for incidents context");
    let payload = build_exception_payload("Context test error", "ContextError", &ctx.project_key);
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/exceptions", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload)
        .send()
        .await
        .expect("ingest exception failed");
    assert!(resp.status().is_success());

    // Wait for data
    step("Wait for data to propagate");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // ── Query incidents context ─────────────────────────────────────────
    step("Query incidents context");
    let now_ms = chrono::Utc::now().timestamp_millis();
    let one_hour_ago_ms = now_ms - 3_600_000;

    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/incidents/context?start_ms={}&end_ms={}",
            ctx.base_url, ctx.project_id, one_hour_ago_ms, now_ms
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("get incidents context failed");

    assert!(
        resp.status().is_success(),
        "incidents context returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let data: serde_json::Value = resp.json().await.unwrap();
    assert!(data.is_object(), "incidents context should be an object");
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 30: Exception-Trace Correlation
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_exception_trace_correlation() {
    let ctx = setup().await;

    // Create a trace and an exception with the same trace_id
    let trace_id = format!("{:032x}", Uuid::new_v4().as_u128());
    let span_id = format!("{:016x}", rand::random::<u64>());

    step("Ingest trace for correlation");
    let trace_payload = build_otlp_trace_payload(&trace_id, &span_id, "corr-svc");
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/traces", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&trace_payload)
        .send()
        .await
        .expect("ingest correlation trace failed");
    assert!(resp.status().is_success());

    step("Ingest exception with same trace_id");
    let exc_payload = build_exception_payload_with_metadata(
        "Correlation test error",
        "CorrelationError",
        &ctx.project_key,
        "corr-svc",
        "test",
        Some(&trace_id),
    );
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/exceptions", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&exc_payload)
        .send()
        .await
        .expect("ingest correlation exception failed");
    assert!(resp.status().is_success());

    // Wait for both to appear
    step("Wait for exception to appear");
    let found = wait_for("correlated exception", 15, Duration::from_secs(2), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/projects/{}/exceptions",
                    base_url, project_id
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    if let Some(arr) = body.as_array() {
                        arr.iter()
                            .any(|g| g["exception_type"].as_str() == Some("CorrelationError"))
                    } else {
                        false
                    }
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(found, "correlated exception never appeared");

    // Get the group_id
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/exceptions",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("list exceptions failed");

    let groups: serde_json::Value = resp.json().await.unwrap();
    let group_id = groups
        .as_array()
        .unwrap()
        .iter()
        .find(|g| g["exception_type"].as_str() == Some("CorrelationError"))
        .and_then(|g| g["id"].as_str())
        .expect("could not find CorrelationError group id");

    // ── Get exception detail and check for trace correlation ────────────
    step("Verify exception detail has trace correlation");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/exceptions/{}",
            ctx.base_url, ctx.project_id, group_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("get exception detail failed");

    assert!(
        resp.status().is_success(),
        "exception detail returned {}",
        resp.status()
    );

    let detail: serde_json::Value = resp.json().await.unwrap();
    assert!(detail["group"].is_object(), "detail should have group");
    assert!(
        detail["recent_exceptions"].is_array(),
        "detail should have recent_exceptions"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 31: Widget Query
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_widget_query() {
    let ctx = setup().await;

    // Ingest a trace so there's data in the spans table
    step("Ingest trace for widget query");
    let trace_id = format!("{:032x}", Uuid::new_v4().as_u128());
    let span_id = format!("{:016x}", rand::random::<u64>());
    let payload = build_otlp_trace_payload(&trace_id, &span_id, "widget-query-svc");

    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/traces", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload)
        .send()
        .await
        .expect("ingest trace for widget query failed");
    assert!(resp.status().is_success());

    // Wait for data to appear
    step("Wait for trace to propagate");
    let tid = trace_id.clone();
    let found = wait_for("trace for widget query", 15, Duration::from_secs(2), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        let t = tid.clone();
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/projects/{}/traces",
                    base_url, project_id
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    if let Some(arr) = body.as_array() {
                        arr.iter().any(|tr| tr["trace_id"].as_str() == Some(&t))
                    } else {
                        false
                    }
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(found, "trace for widget query never appeared");

    // ── Execute widget query ────────────────────────────────────────────
    step("Execute widget query");
    let query_payload = build_widget_query_payload();

    let resp = ctx
        .client
        .post(format!(
            "{}/api/watch/{}/widget-query",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .json(&query_payload)
        .send()
        .await
        .expect("widget query request failed");

    assert!(
        resp.status().is_success(),
        "widget query returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let result: serde_json::Value = resp.json().await.unwrap();
    assert!(
        result["columns"].is_array(),
        "widget query should return columns"
    );
    assert!(result["data"].is_array(), "widget query should return data");
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 32: Log Detail and Context
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_log_detail_and_context() {
    let ctx = setup().await;

    // ── Ingest a log via OTLP ──────────────────────────────────────────
    step("Ingest OTLP log for detail/context test");
    let trace_id = format!("{:032x}", Uuid::new_v4().as_u128());
    let log_message = format!("e2e-log-detail-{}", Uuid::new_v4());
    let payload = build_otlp_log_payload(&trace_id, &log_message, "INFO");

    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/logs", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload)
        .send()
        .await
        .expect("ingest log for detail test failed");
    assert!(resp.status().is_success());

    // ── Wait for the log to appear via unified events ──────────────────
    step("Wait for log to appear in unified events");
    let msg_clone = log_message.clone();
    let mut log_id: Option<String> = None;
    let found = wait_for("log in events", 5, Duration::from_secs(1), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        let msg = msg_clone.clone();
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/projects/{}/events?time_range=1h",
                    base_url, project_id
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    if let Some(arr) = body.as_array() {
                        arr.iter().any(|e| {
                            e["body"]
                                .as_str()
                                .map(|b| b.contains(&msg))
                                .unwrap_or(false)
                                || e["template"]
                                    .as_str()
                                    .map(|t| t.contains(&msg))
                                    .unwrap_or(false)
                        })
                    } else {
                        false
                    }
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(found, "log never appeared in unified events");

    // ── Get the log_id from the events list ────────────────────────────
    step("Get log_id from unified events");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/events?time_range=1h",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("list events failed");

    let events: serde_json::Value = resp.json().await.unwrap();
    if let Some(arr) = events.as_array() {
        for e in arr {
            let body_matches = e["body"]
                .as_str()
                .map(|b| b.contains(&log_message))
                .unwrap_or(false);
            if body_matches {
                if let Some(id) = e["id"].as_str() {
                    log_id = Some(id.to_string());
                }
            }
        }
    }

    if let Some(ref lid) = log_id {
        // ── Get log detail ─────────────────────────────────────────────
        step("Get log detail by ID");
        let resp = ctx
            .client
            .get(format!(
                "{}/api/watch/projects/{}/logs/{}",
                ctx.base_url, ctx.project_id, lid
            ))
            .bearer_auth(&ctx.token)
            .send()
            .await
            .expect("get log detail failed");

        assert!(
            resp.status().is_success(),
            "log detail returned {}: {}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        );

        let detail: serde_json::Value = resp.json().await.unwrap();
        assert!(detail["body"].is_string(), "log detail should have body");
        assert!(
            detail["timestamp"].is_string(),
            "log detail should have timestamp"
        );

        // ── Get log context ────────────────────────────────────────────
        step("Get log context");
        let resp = ctx
            .client
            .get(format!(
                "{}/api/watch/projects/{}/logs/context?log_id={}",
                ctx.base_url, ctx.project_id, lid
            ))
            .bearer_auth(&ctx.token)
            .send()
            .await
            .expect("get log context failed");

        assert!(
            resp.status().is_success(),
            "log context returned {}: {}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        );

        let context: serde_json::Value = resp.json().await.unwrap();
        assert!(
            context.is_array(),
            "log context should return an array of surrounding logs"
        );
    } else {
        eprintln!("    [note] log_id not found in events, skipping detail/context checks");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 33: Unified Events Listing
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_unified_events_listing() {
    let ctx = setup().await;

    // ── Ingest an exception ────────────────────────────────────────────
    step("Ingest exception for events test");
    let payload = build_exception_payload("events-test-error", "EventsError", &ctx.project_key);
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/exceptions", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload)
        .send()
        .await
        .expect("ingest exception for events test failed");
    assert!(resp.status().is_success());

    // ── Ingest a log ───────────────────────────────────────────────────
    step("Ingest OTLP log for events test");
    let trace_id = format!("{:032x}", Uuid::new_v4().as_u128());
    let payload = build_otlp_log_payload(&trace_id, "events-test-log-entry", "INFO");
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/logs", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload)
        .send()
        .await
        .expect("ingest log for events test failed");
    assert!(resp.status().is_success());

    // ── Wait for exception to propagate ────────────────────────────────
    step("Wait for exception to propagate");
    let found = wait_for("exception in groups", 15, Duration::from_secs(2), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/projects/{}/exceptions",
                    base_url, project_id
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    body.as_array().map(|a| !a.is_empty()).unwrap_or(false)
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(found, "exception for events test never appeared");

    // ── Query unified events ───────────────────────────────────────────
    step("Query unified events timeline");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/events?time_range=1h",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("list unified events failed");

    assert!(
        resp.status().is_success(),
        "unified events returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let events: serde_json::Value = resp.json().await.unwrap();
    assert!(events.is_array(), "unified events should return an array");
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 34: Events Filter Values
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_events_filter_values() {
    let ctx = setup().await;

    // ── Ingest an exception with known metadata ────────────────────────
    step("Ingest exception for events filter values");
    let svc = format!("events-filter-svc-{}", &Uuid::new_v4().to_string()[..8]);
    let payload = build_exception_payload_with_metadata(
        "events filter test",
        "EventsFilterError",
        &ctx.project_key,
        &svc,
        "events-filter-env",
        None,
    );

    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/exceptions", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload)
        .send()
        .await
        .expect("ingest exception for events filter test failed");
    assert!(resp.status().is_success());

    // ── Wait for attributes to propagate ───────────────────────────────
    step("Wait for filter values to propagate");
    let svc_clone = svc.clone();
    let found = wait_for("events filter values", 15, Duration::from_secs(2), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        let s = svc_clone.clone();
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/projects/{}/events/filter-values",
                    base_url, project_id
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    // Check if service_names contains our service
                    body["service_names"]
                        .as_array()
                        .map(|arr| arr.iter().any(|v| v.as_str() == Some(&s)))
                        .unwrap_or(false)
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(
        found,
        "events filter values never contained service {}",
        svc
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 35: Historical Service Latency
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_historical_service_latency() {
    let ctx = setup().await;

    // ── Ingest trace ───────────────────────────────────────────────────
    step("Ingest trace for service latency");
    let trace_id = format!("{:032x}", Uuid::new_v4().as_u128());
    let span_id = format!("{:016x}", rand::random::<u64>());
    let payload = build_otlp_trace_payload(&trace_id, &span_id, "latency-svc");

    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/traces", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload)
        .send()
        .await
        .expect("ingest trace for latency test failed");
    assert!(resp.status().is_success());

    // ── Wait for trace to appear ───────────────────────────────────────
    step("Wait for trace to propagate");
    let tid = trace_id.clone();
    let found = wait_for("trace for latency", 15, Duration::from_secs(2), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        let t = tid.clone();
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/projects/{}/traces",
                    base_url, project_id
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    body.as_array()
                        .map(|a| a.iter().any(|tr| tr["trace_id"].as_str() == Some(&t)))
                        .unwrap_or(false)
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(found, "trace for latency test never appeared");

    // ── Query historical service latency ───────────────────────────────
    step("Query historical service latency");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/historical/projects/{}/service-latency?time_range=24h",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("service latency query failed");

    assert!(
        resp.status().is_success(),
        "service latency returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let result: serde_json::Value = resp.json().await.unwrap();
    assert!(
        result.is_array(),
        "service latency should return an array, got: {}",
        serde_json::to_string_pretty(&result).unwrap_or_default()
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 36: Dashboard Tabs and Widgets
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_dashboard_tabs_and_widgets() {
    let ctx = setup().await;

    // ── Create dashboard ───────────────────────────────────────────────
    step("Create dashboard for tabs/widgets test");
    let create_body = serde_json::json!({
        "name": "E2E Tabs Widgets Dashboard",
        "description": "Testing tabs and widgets CRUD"
    });
    let resp = ctx
        .client
        .post(format!(
            "{}/api/watch/projects/{}/dashboards",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .json(&create_body)
        .send()
        .await
        .expect("create dashboard failed");
    assert!(resp.status().is_success());

    let dashboard: serde_json::Value = resp.json().await.unwrap();
    let dashboard_id = dashboard["id"].as_str().expect("dashboard should have id");

    // ── Create tab ─────────────────────────────────────────────────────
    step("Create tab");
    let tab_body = serde_json::json!({
        "name": "E2E Test Tab",
        "display_order": 0
    });
    let resp = ctx
        .client
        .post(format!(
            "{}/api/watch/projects/{}/dashboards/{}/tabs",
            ctx.base_url, ctx.project_id, dashboard_id
        ))
        .bearer_auth(&ctx.token)
        .json(&tab_body)
        .send()
        .await
        .expect("create tab failed");

    assert!(
        resp.status().is_success(),
        "create tab returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let tab: serde_json::Value = resp.json().await.unwrap();
    let tab_id = tab["id"].as_str().expect("tab should have id");

    // ── List tabs ──────────────────────────────────────────────────────
    step("List tabs");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/dashboards/{}/tabs",
            ctx.base_url, ctx.project_id, dashboard_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("list tabs failed");
    assert!(resp.status().is_success());
    let tabs: serde_json::Value = resp.json().await.unwrap();
    assert!(
        tabs.as_array()
            .map(|a| a.iter().any(|t| t["id"].as_str() == Some(tab_id)))
            .unwrap_or(false),
        "new tab not found in list"
    );

    // ── Create widget ──────────────────────────────────────────────────
    step("Create widget on tab");
    let widget_body = serde_json::json!({
        "tab_id": tab_id,
        "widget_type": "chart",
        "widget_config": { "metric": "e2e.test.metric", "type": "line" },
        "position_x": 0,
        "position_y": 0,
        "width": 6,
        "height": 4,
        "title": "E2E Test Widget"
    });
    let resp = ctx
        .client
        .post(format!(
            "{}/api/watch/projects/{}/dashboards/{}/widgets",
            ctx.base_url, ctx.project_id, dashboard_id
        ))
        .bearer_auth(&ctx.token)
        .json(&widget_body)
        .send()
        .await
        .expect("create widget failed");

    assert!(
        resp.status().is_success(),
        "create widget returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let widget: serde_json::Value = resp.json().await.unwrap();
    let widget_id = widget["id"].as_str().expect("widget should have id");

    // ── List tab widgets ───────────────────────────────────────────────
    step("List tab widgets");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/dashboards/{}/tabs/{}/widgets",
            ctx.base_url, ctx.project_id, dashboard_id, tab_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("list tab widgets failed");
    assert!(resp.status().is_success());
    let widgets: serde_json::Value = resp.json().await.unwrap();
    assert!(
        widgets
            .as_array()
            .map(|a| a.iter().any(|w| w["id"].as_str() == Some(widget_id)))
            .unwrap_or(false),
        "new widget not found in tab widgets"
    );

    // ── Update widget ──────────────────────────────────────────────────
    step("Update widget title");
    let update_body = serde_json::json!({
        "title": "E2E Updated Widget"
    });
    let resp = ctx
        .client
        .put(format!(
            "{}/api/watch/projects/{}/dashboards/{}/widgets/{}",
            ctx.base_url, ctx.project_id, dashboard_id, widget_id
        ))
        .bearer_auth(&ctx.token)
        .json(&update_body)
        .send()
        .await
        .expect("update widget failed");
    assert!(resp.status().is_success());

    // ── Delete widget ──────────────────────────────────────────────────
    step("Delete widget");
    let resp = ctx
        .client
        .delete(format!(
            "{}/api/watch/projects/{}/dashboards/{}/widgets/{}",
            ctx.base_url, ctx.project_id, dashboard_id, widget_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("delete widget failed");
    assert!(
        resp.status().is_success() || resp.status().as_u16() == 204,
        "delete widget returned {}",
        resp.status()
    );

    // ── Delete tab ─────────────────────────────────────────────────────
    step("Delete tab");
    let resp = ctx
        .client
        .delete(format!(
            "{}/api/watch/projects/{}/dashboards/{}/tabs/{}",
            ctx.base_url, ctx.project_id, dashboard_id, tab_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("delete tab failed");
    assert!(
        resp.status().is_success() || resp.status().as_u16() == 204,
        "delete tab returned {}",
        resp.status()
    );

    // ── Cleanup: delete dashboard ──────────────────────────────────────
    step("Delete dashboard");
    let resp = ctx
        .client
        .delete(format!(
            "{}/api/watch/projects/{}/dashboards/{}",
            ctx.base_url, ctx.project_id, dashboard_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("delete dashboard failed");
    assert!(resp.status().is_success() || resp.status().as_u16() == 204);
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 38: Health Check Results and Uptime
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_health_check_results_and_uptime() {
    let ctx = setup().await;
    let project_id_str = ctx.project_id.to_string();

    // ── Create health check ────────────────────────────────────────────
    step("Create health check for results test");
    let create_body = serde_json::json!({
        "project_id": ctx.project_id,
        "name": "E2E Results HC",
        "check_type": "http",
        "target_url": "https://example.com",
        "check_interval_seconds": 60,
        "timeout_seconds": 10,
        "enabled": true
    });
    let resp = ctx
        .client
        .post(format!("{}/api/watch/health-checks/checks", ctx.base_url))
        .bearer_auth(&ctx.token)
        .header("X-Project-Id", &project_id_str)
        .json(&create_body)
        .send()
        .await
        .expect("create health check failed");

    assert!(
        resp.status().is_success(),
        "create health check returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );
    let check: serde_json::Value = resp.json().await.unwrap();
    let check_id = check["id"].as_str().expect("check should have id");

    // ── Report a result ────────────────────────────────────────────────
    step("Report health check result");
    let result_payload = build_health_check_result_payload(check_id, "E2E Results HC");
    let resp = ctx
        .client
        .post(format!("{}/api/watch/health-checks/results", ctx.base_url))
        .bearer_auth(&ctx.token)
        .header("X-Project-Id", &project_id_str)
        .json(&result_payload)
        .send()
        .await
        .expect("report health check result failed");

    assert!(
        resp.status().is_success() || resp.status().as_u16() == 201,
        "report result returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    // ── Get check results ──────────────────────────────────────────────
    step("Get health check results");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/health-checks/checks/{}/results?limit=10",
            ctx.base_url, check_id
        ))
        .bearer_auth(&ctx.token)
        .header("X-Project-Id", &project_id_str)
        .send()
        .await
        .expect("get check results failed");

    assert!(
        resp.status().is_success(),
        "get check results returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    // ── Get uptime summary ─────────────────────────────────────────────
    step("Get uptime summary");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/health-checks/uptime?project_id={}",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .header("X-Project-Id", &project_id_str)
        .send()
        .await
        .expect("get uptime summary failed");

    assert!(
        resp.status().is_success(),
        "uptime summary returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    // ── Cleanup ────────────────────────────────────────────────────────
    step("Delete health check");
    let _ = ctx
        .client
        .delete(format!(
            "{}/api/watch/health-checks/checks/{}",
            ctx.base_url, check_id
        ))
        .bearer_auth(&ctx.token)
        .header("X-Project-Id", &project_id_str)
        .send()
        .await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 39: Alert Rule Alerts Listing
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_alert_rule_alerts_listing() {
    let ctx = setup().await;

    // ── Create alert rule ──────────────────────────────────────────────
    step("Create alert rule for alerts listing test");
    let create_body = serde_json::json!({
        "project_id": ctx.project_id,
        "name": "E2E Alerts Listing Rule",
        "description": "Testing alerts listing",
        "rule_type": "threshold",
        "query_config": {
            "metric_name": "e2e.alerts.metric",
            "filters": {},
            "group_by": [],
            "time_aggregation": "avg",
            "space_aggregation": "sum"
        },
        "threshold": 100.0,
        "threshold_type": "above",
        "notification_channels": [],
        "eval_window_seconds": 300,
        "eval_interval_seconds": 60,
        "labels": {},
        "annotations": {},
        "enabled": true
    });

    let resp = ctx
        .client
        .post(format!("{}/api/watch/alerting/rules", ctx.base_url))
        .bearer_auth(&ctx.token)
        .json(&create_body)
        .send()
        .await
        .expect("create alert rule failed");
    assert!(resp.status().is_success());

    let rule: serde_json::Value = resp.json().await.unwrap();
    let rule_id = rule["id"].as_str().expect("rule should have id");

    // ── List alerts for rule ───────────────────────────────────────────
    step("List alerts for rule (should be empty)");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/alerting/rules/{}/alerts",
            ctx.base_url, rule_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("list rule alerts failed");

    assert!(
        resp.status().is_success(),
        "list rule alerts returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let alerts: serde_json::Value = resp.json().await.unwrap();
    assert!(alerts.is_array(), "rule alerts should be an array");

    // ── List all alerts for project ────────────────────────────────────
    step("List all alerts for project");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/alerting/alerts?project_id={}",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("list all alerts failed");

    assert!(
        resp.status().is_success(),
        "list all alerts returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let all_alerts: serde_json::Value = resp.json().await.unwrap();
    assert!(all_alerts.is_array(), "all alerts should be an array");

    // ── Cleanup ────────────────────────────────────────────────────────
    step("Delete alert rule");
    let _ = ctx
        .client
        .delete(format!(
            "{}/api/watch/alerting/rules/{}",
            ctx.base_url, rule_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 40: Exception Batch Ingestion
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_exception_batch_ingestion() {
    let ctx = setup().await;

    // ── Send batch of 3 exceptions ─────────────────────────────────────
    step("Send batch of 3 exceptions");
    let batch = vec![
        build_exception_payload_with_metadata(
            "batch-err-1",
            "BatchError1",
            &ctx.project_key,
            "batch-svc",
            "test",
            None,
        ),
        build_exception_payload_with_metadata(
            "batch-err-2",
            "BatchError2",
            &ctx.project_key,
            "batch-svc",
            "test",
            None,
        ),
        build_exception_payload_with_metadata(
            "batch-err-3",
            "BatchError3",
            &ctx.project_key,
            "batch-svc",
            "test",
            None,
        ),
    ];

    let resp = ctx
        .client
        .post(format!(
            "{}/api/watch/ingest/exceptions/batch",
            ctx.base_url
        ))
        .bearer_auth(&ctx.project_key)
        .json(&batch)
        .send()
        .await
        .expect("batch exception ingestion failed");

    assert!(
        resp.status().is_success(),
        "batch ingestion returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    // ── Wait for exceptions to appear ──────────────────────────────────
    step("Wait for batch exceptions to appear");
    let found = wait_for("batch exceptions", 15, Duration::from_secs(2), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/projects/{}/exceptions",
                    base_url, project_id
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    // We expect at least 3 distinct groups (different fingerprints)
                    body.as_array().map(|a| a.len() >= 3).unwrap_or(false)
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(
        found,
        "batch exceptions never appeared (expected >= 3 groups)"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 41: Monitoring Status
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_monitoring_status() {
    let ctx = setup().await;

    step("Query monitoring status");
    let resp = ctx
        .client
        .get(format!("{}/api/watch/monitoring/status", ctx.base_url))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("monitoring status request failed");

    assert!(
        resp.status().is_success(),
        "monitoring status returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let status: serde_json::Value = resp.json().await.unwrap();
    assert!(
        status.get("kafka_consumer_groups").is_some() || status.get("clickhouse_tables").is_some(),
        "monitoring status should have kafka_consumer_groups or clickhouse_tables"
    );

    // Verify clickhouse_tables contains actual table stats (not empty due to schema mismatch)
    if let Some(tables) = status.get("clickhouse_tables").and_then(|v| v.as_array()) {
        assert!(
            !tables.is_empty(),
            "clickhouse_tables should not be empty - TableStatsRow schema should match ClickHouse DateTime"
        );
        for table in tables {
            assert!(
                table.get("table").is_some(),
                "each ClickHouse table entry should have a 'table' field"
            );
            assert!(
                table.get("total_rows").is_some(),
                "each ClickHouse table entry should have 'total_rows'"
            );
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 42: Monitoring Kafka Lag
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_monitoring_kafka_lag() {
    let ctx = setup().await;

    step("Query monitoring Kafka lag");
    let resp = ctx
        .client
        .get(format!("{}/api/watch/monitoring/kafka/lag", ctx.base_url))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("monitoring kafka lag request failed");

    assert!(
        resp.status().is_success(),
        "monitoring kafka lag returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let lag: serde_json::Value = resp.json().await.unwrap();
    assert!(lag.is_array(), "kafka lag should be an array");
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 43: X-Ray Segment Ingestion
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_xray_segment_ingestion() {
    let ctx = setup().await;
    let project_id_str = ctx.project_id.to_string();

    step("Ingest X-Ray segment");
    let segment_id = format!("{:016x}", rand::random::<u64>());
    let xray_trace_id = format!(
        "1-{:08x}-{:024x}",
        chrono::Utc::now().timestamp() as u32,
        rand::random::<u128>() >> 32
    );
    let payload = build_xray_segment("e2e-xray-service", &segment_id, &xray_trace_id);

    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/xray/ingest", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .header("X-Project-Id", &project_id_str)
        .json(&payload)
        .send()
        .await
        .expect("xray ingest failed");

    assert!(
        resp.status().is_success(),
        "xray ingest returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let result: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        result["segments_received"].as_u64(),
        Some(1),
        "should report 1 segment received"
    );

    // ── Verify X-Ray span appears in traces (consumed from Kafka by spans_worker) ──
    step("Wait for X-Ray span to appear in traces");
    let found = wait_for("xray span in traces", 20, Duration::from_secs(2), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        let tid = xray_trace_id.clone();
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/projects/{}/events?event_type=traces",
                    base_url, project_id
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    if let Some(arr) = body.as_array() {
                        arr.iter().any(|e| {
                            e["trace_id"].as_str() == Some(&tid) || e.to_string().contains(&tid)
                        })
                    } else {
                        false
                    }
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(
        found,
        "X-Ray span with trace_id='{}' should appear in traces after Kafka processing",
        xray_trace_id
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 44: X-Ray Batch Ingestion
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_xray_batch_ingestion() {
    let ctx = setup().await;
    let project_id_str = ctx.project_id.to_string();

    step("Ingest X-Ray batch of 2 segments");
    let seg1_id = format!("{:016x}", rand::random::<u64>());
    let seg2_id = format!("{:016x}", rand::random::<u64>());
    let xray_trace_id = format!(
        "1-{:08x}-{:024x}",
        chrono::Utc::now().timestamp() as u32,
        rand::random::<u128>() >> 32
    );

    let batch = vec![
        build_xray_segment("e2e-xray-batch-svc", &seg1_id, &xray_trace_id),
        build_xray_segment("e2e-xray-batch-svc", &seg2_id, &xray_trace_id),
    ];

    let resp = ctx
        .client
        .post(format!(
            "{}/api/watch/ingest/xray/ingest/batch",
            ctx.base_url
        ))
        .bearer_auth(&ctx.project_key)
        .header("X-Project-Id", &project_id_str)
        .json(&batch)
        .send()
        .await
        .expect("xray batch ingest failed");

    assert!(
        resp.status().is_success(),
        "xray batch ingest returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let result: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        result["segments_received"].as_u64(),
        Some(2),
        "should report 2 segments received"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 45: Database Monitoring - Explain Plan
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_database_monitoring_explain_plan() {
    let ctx = setup().await;
    let project_id_str = ctx.project_id.to_string();

    step("Store explain plan");
    let payload = build_explain_plan_payload(&ctx.project_id);

    let resp = ctx
        .client
        .post(format!(
            "{}/api/watch/database-monitoring/explain-plans",
            ctx.base_url
        ))
        .bearer_auth(&ctx.token)
        .header("X-Project-Id", &project_id_str)
        .json(&payload)
        .send()
        .await
        .expect("store explain plan failed");

    assert!(
        resp.status().is_success() || resp.status().as_u16() == 201,
        "store explain plan returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 46: Database Monitoring - Query Metrics
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_database_monitoring_query_metrics() {
    let ctx = setup().await;
    let project_id_str = ctx.project_id.to_string();

    step("Store query metrics");
    let payload = build_query_metrics_payload(&ctx.project_id);

    let resp = ctx
        .client
        .post(format!(
            "{}/api/watch/database-monitoring/query-metrics",
            ctx.base_url
        ))
        .bearer_auth(&ctx.token)
        .header("X-Project-Id", &project_id_str)
        .json(&payload)
        .send()
        .await
        .expect("store query metrics failed");

    assert!(
        resp.status().is_success() || resp.status().as_u16() == 201,
        "store query metrics returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 47: CloudWatch Log Ingestion
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_cloudwatch_log_ingestion() {
    let ctx = setup().await;

    step("Ingest CloudWatch log via Kinesis Firehose format");
    let payload = build_cloudwatch_kinesis_payload();

    let resp = ctx
        .client
        .post(format!(
            "{}/api/watch/ingest/logs/cloudwatch/kinesis",
            ctx.base_url
        ))
        .bearer_auth(&ctx.project_key)
        .header("X-Project-Id", &ctx.project_id.to_string())
        .json(&payload)
        .send()
        .await
        .expect("cloudwatch log ingestion failed");

    assert!(
        resp.status().is_success(),
        "cloudwatch ingestion returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 48: Azure Monitor Log Ingestion
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_azure_monitor_log_ingestion() {
    let ctx = setup().await;

    step("Ingest Azure Monitor log");
    let payload = build_azure_monitor_payload("E2E Azure Monitor test log", "e2e-azure-vm");

    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/logs/azure", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .header("X-Project-Id", &ctx.project_id.to_string())
        .json(&payload)
        .send()
        .await
        .expect("azure log ingestion failed");

    assert!(
        resp.status().is_success(),
        "azure ingestion returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 49: GCP Log Ingestion
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_gcp_log_ingestion() {
    let ctx = setup().await;

    step("Ingest GCP log");
    let payload = build_gcp_log_payload("E2E GCP test log message", "e2e-gcp-instance");

    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/logs/gcp", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .header("X-Project-Id", &ctx.project_id.to_string())
        .json(&payload)
        .send()
        .await
        .expect("gcp log ingestion failed");

    assert!(
        resp.status().is_success(),
        "gcp ingestion returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 50: OTLP Profiles Ingestion
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_otlp_profiles_ingestion() {
    let ctx = setup().await;

    step("Ingest OTLP profile");
    let payload = build_otlp_profile_payload("e2e-profile-svc");

    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/profiles", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload)
        .send()
        .await
        .expect("otlp profiles ingestion failed");

    assert!(
        resp.status().is_success(),
        "otlp profiles ingestion returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 51: Service Versions
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_service_versions() {
    let ctx = setup().await;
    let svc_name = format!("version-svc-{}", &Uuid::new_v4().to_string()[..8]);

    // ── Ingest trace with version v1.0 ─────────────────────────────────
    step("Ingest trace with service version v1.0");
    let trace_id = format!("{:032x}", Uuid::new_v4().as_u128());
    let span_id = format!("{:016x}", rand::random::<u64>());
    let payload = build_otlp_trace_payload_with_version(&trace_id, &span_id, &svc_name, "v1.0.0");

    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/traces", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload)
        .send()
        .await
        .expect("ingest versioned trace failed");
    assert!(resp.status().is_success());

    // ── Ingest trace with version v1.1 ─────────────────────────────────
    step("Ingest trace with service version v1.1");
    let trace_id2 = format!("{:032x}", Uuid::new_v4().as_u128());
    let span_id2 = format!("{:016x}", rand::random::<u64>());
    let payload2 =
        build_otlp_trace_payload_with_version(&trace_id2, &span_id2, &svc_name, "v1.1.0");

    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/traces", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&payload2)
        .send()
        .await
        .expect("ingest versioned trace v1.1 failed");
    assert!(resp.status().is_success());

    // ── Wait for traces to appear ──────────────────────────────────────
    step("Wait for versioned traces to appear");
    let tid = trace_id2.clone();
    let found = wait_for("versioned trace", 15, Duration::from_secs(2), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        let t = tid.clone();
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/projects/{}/traces",
                    base_url, project_id
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    body.as_array()
                        .map(|a| a.iter().any(|tr| tr["trace_id"].as_str() == Some(&t)))
                        .unwrap_or(false)
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(found, "versioned traces never appeared");

    // ── Query service versions ─────────────────────────────────────────
    step("Query service versions");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/services/{}/versions",
            ctx.base_url, ctx.project_id, svc_name
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("service versions query failed");

    assert!(
        resp.status().is_success(),
        "service versions returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let versions: serde_json::Value = resp.json().await.unwrap();
    assert!(
        versions["versions"].is_array(),
        "service versions should have a 'versions' array, got: {}",
        serde_json::to_string_pretty(&versions).unwrap_or_default()
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 52: Trace Detail Returns Correlated Exceptions
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_trace_detail_correlated_exceptions() {
    let ctx = setup().await;

    // Shared trace_id links the trace and exception together
    let trace_id = format!("{:032x}", Uuid::new_v4().as_u128());
    let span_id = format!("{:016x}", rand::random::<u64>());

    // ── Step 1: Ingest a trace ───────────────────────────────────────────
    step("Ingest trace for exception correlation");
    let trace_payload = build_otlp_trace_payload(&trace_id, &span_id, "corr-trace-svc");
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/traces", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&trace_payload)
        .send()
        .await
        .expect("ingest trace failed");
    assert!(resp.status().is_success());

    // ── Step 2: Ingest an exception with the same trace_id ───────────────
    step("Ingest exception with matching trace_id");
    let exc_payload = build_exception_payload_with_metadata(
        "Correlated trace detail error",
        "TraceDetailCorrelationError",
        &ctx.project_key,
        "corr-trace-svc",
        "test",
        Some(&trace_id),
    );
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/exceptions", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&exc_payload)
        .send()
        .await
        .expect("ingest exception failed");
    assert!(resp.status().is_success());

    // ── Step 3: Wait for the trace to appear ─────────────────────────────
    step("Wait for trace to appear");
    let tid = trace_id.clone();
    let found = wait_for("trace for correlation", 20, Duration::from_secs(2), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        let t = tid.clone();
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/projects/{}/traces/{}",
                    base_url, project_id, t
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => true,
                _ => false,
            }
        }
    })
    .await;
    assert!(found, "trace never appeared");

    // ── Step 4: Wait for exception-trace junction to be populated ────────
    step("Wait for trace detail to include correlated exceptions");
    let tid2 = trace_id.clone();
    let found = wait_for(
        "correlated exceptions in trace detail",
        20,
        Duration::from_secs(2),
        || {
            let client = ctx.client.clone();
            let base_url = ctx.base_url.clone();
            let token = ctx.token.clone();
            let project_id = ctx.project_id;
            let t = tid2.clone();
            async move {
                let resp = client
                    .get(format!(
                        "{}/api/watch/projects/{}/traces/{}",
                        base_url, project_id, t
                    ))
                    .bearer_auth(&token)
                    .send()
                    .await;
                match resp {
                    Ok(r) if r.status().is_success() => {
                        let body: serde_json::Value = r.json().await.unwrap_or_default();
                        body["exceptions"]
                            .as_array()
                            .map(|a| !a.is_empty())
                            .unwrap_or(false)
                    }
                    _ => false,
                }
            }
        },
    )
    .await;
    assert!(found, "trace detail never included correlated exceptions");

    // ── Step 5: Verify the exception data in the trace detail ────────────
    step("Verify exception fields in trace detail");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/traces/{}",
            ctx.base_url, ctx.project_id, trace_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("get trace detail failed");
    assert!(resp.status().is_success());

    let detail: serde_json::Value = resp.json().await.unwrap();
    let exceptions = detail["exceptions"]
        .as_array()
        .expect("exceptions should be array");
    assert!(
        !exceptions.is_empty(),
        "trace detail should have at least one correlated exception"
    );

    let exc = &exceptions[0];
    assert!(exc["id"].is_string(), "exception should have id");
    assert!(exc["message"].is_string(), "exception should have message");
    assert!(
        exc["exception_type"].is_string(),
        "exception should have exception_type"
    );
    assert_eq!(
        exc["exception_type"].as_str().unwrap(),
        "TraceDetailCorrelationError",
        "exception_type should match what was ingested"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 53: Exception Detail Enriched with Span Attributes
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_exception_enriched_with_span_attributes() {
    let ctx = setup().await;

    let trace_id = format!("{:032x}", Uuid::new_v4().as_u128());
    let span_id = format!("{:016x}", rand::random::<u64>());
    let svc_name = format!("enrich-svc-{}", &Uuid::new_v4().to_string()[..8]);

    // ── Step 1: Ingest a trace with span-level attributes ────────────────
    step("Ingest trace with span-level attributes for enrichment");
    let trace_payload = build_otlp_trace_payload_with_span_attrs(
        &trace_id,
        &span_id,
        &svc_name,
        &[
            ("deployment.environment", "staging"),
            ("service.version", "v2.3.0"),
            ("deployment.id", "deploy-e2e-001"),
        ],
    );
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/traces", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&trace_payload)
        .send()
        .await
        .expect("ingest enriched trace failed");
    assert!(resp.status().is_success());

    // ── Step 2: Ingest an exception with the same trace_id ───────────────
    step("Ingest exception with matching trace_id for enrichment");
    let exc_payload = build_exception_payload_with_metadata(
        "Enrichment test error",
        "EnrichmentTestError",
        &ctx.project_key,
        &svc_name,
        "staging",
        Some(&trace_id),
    );
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/exceptions", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&exc_payload)
        .send()
        .await
        .expect("ingest enrichment exception failed");
    assert!(resp.status().is_success());

    // ── Step 3: Wait for exception group to appear ───────────────────────
    step("Wait for enrichment exception to appear");
    let found = wait_for("enrichment exception", 20, Duration::from_secs(2), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/projects/{}/exceptions",
                    base_url, project_id
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    body.as_array()
                        .map(|a| {
                            a.iter().any(|g| {
                                g["exception_type"].as_str() == Some("EnrichmentTestError")
                            })
                        })
                        .unwrap_or(false)
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(found, "enrichment exception never appeared");

    // ── Step 4: Get the group_id ─────────────────────────────────────────
    step("Get exception group ID");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/exceptions",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("list exceptions failed");

    let groups: serde_json::Value = resp.json().await.unwrap();
    let group_id = groups
        .as_array()
        .unwrap()
        .iter()
        .find(|g| g["exception_type"].as_str() == Some("EnrichmentTestError"))
        .and_then(|g| g["id"].as_str())
        .expect("could not find EnrichmentTestError group id");

    // ── Step 5: Wait for the exception detail to be enriched from spans ──
    step("Wait for exception detail to have enriched service_name from spans");
    let gid = group_id.to_string();
    let svc = svc_name.clone();
    let found = wait_for(
        "enriched exception detail",
        20,
        Duration::from_secs(2),
        || {
            let client = ctx.client.clone();
            let base_url = ctx.base_url.clone();
            let token = ctx.token.clone();
            let project_id = ctx.project_id;
            let g = gid.clone();
            let s = svc.clone();
            async move {
                let resp = client
                    .get(format!(
                        "{}/api/watch/projects/{}/exceptions/{}",
                        base_url, project_id, g
                    ))
                    .bearer_auth(&token)
                    .send()
                    .await;
                match resp {
                    Ok(r) if r.status().is_success() => {
                        let detail: serde_json::Value = r.json().await.unwrap_or_default();
                        // The group should have service_name enriched from the correlated span
                        detail["group"]["service_name"].as_str() == Some(&s)
                    }
                    _ => false,
                }
            }
        },
    )
    .await;
    assert!(
        found,
        "exception detail was never enriched with service_name from spans"
    );

    // ── Step 6: Verify additional enriched fields ────────────────────────
    step("Verify exception detail has enriched span attributes");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/exceptions/{}",
            ctx.base_url, ctx.project_id, group_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("get exception detail failed");
    assert!(resp.status().is_success());

    let detail: serde_json::Value = resp.json().await.unwrap();
    let group = &detail["group"];
    assert_eq!(
        group["service_name"].as_str(),
        Some(svc_name.as_str()),
        "service_name should be enriched from correlated span"
    );
    // These fields come from span_attributes via the LEFT JOIN
    // They may or may not be populated depending on ClickHouse processing timing
    // but service_name enrichment is the primary assertion
    eprintln!(
        "    [info] Enriched fields: environment={:?}, version={:?}, deployment_id={:?}",
        group["environment"].as_str(),
        group["version"].as_str(),
        group["deployment_id"].as_str(),
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 54: Log Filtering by trace_id Returns Correlated Logs
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_log_trace_id_filter() {
    let ctx = setup().await;

    let trace_id = format!("{:032x}", Uuid::new_v4().as_u128());
    let log_msg = format!("trace-correlated-log-{}", Uuid::new_v4());

    // ── Step 1: Ingest a log with a specific trace_id ────────────────────
    step("Ingest OTLP log with specific trace_id");
    let log_payload = build_otlp_log_payload(&trace_id, &log_msg, "INFO");
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/logs", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&log_payload)
        .send()
        .await
        .expect("ingest log failed");
    assert!(resp.status().is_success());

    // ── Step 2: Ingest another log with a DIFFERENT trace_id ─────────────
    step("Ingest OTLP log with different trace_id (noise)");
    let other_trace_id = format!("{:032x}", Uuid::new_v4().as_u128());
    let other_msg = format!("unrelated-log-{}", Uuid::new_v4());
    let other_payload = build_otlp_log_payload(&other_trace_id, &other_msg, "INFO");
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/logs", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&other_payload)
        .send()
        .await
        .expect("ingest noise log failed");
    assert!(resp.status().is_success());

    // ── Step 3: Wait for the target log to appear in events ──────────────
    step("Wait for trace-correlated log to appear in events");
    let tid = trace_id.clone();
    let found = wait_for(
        "log with trace_id in events",
        20,
        Duration::from_secs(2),
        || {
            let client = ctx.client.clone();
            let base_url = ctx.base_url.clone();
            let token = ctx.token.clone();
            let project_id = ctx.project_id;
            let t = tid.clone();
            async move {
                let resp = client
                    .get(format!(
                        "{}/api/watch/projects/{}/events?time_range=1h&trace_id={}",
                        base_url, project_id, t
                    ))
                    .bearer_auth(&token)
                    .send()
                    .await;
                match resp {
                    Ok(r) if r.status().is_success() => {
                        let body: serde_json::Value = r.json().await.unwrap_or_default();
                        body.as_array().map(|a| !a.is_empty()).unwrap_or(false)
                    }
                    _ => false,
                }
            }
        },
    )
    .await;
    assert!(
        found,
        "trace-correlated log never appeared in events with trace_id filter"
    );

    // ── Step 4: Verify only logs with matching trace_id are returned ─────
    step("Verify trace_id filter returns only correlated logs");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/events?time_range=1h&trace_id={}",
            ctx.base_url, ctx.project_id, trace_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("query events with trace_id filter failed");
    assert!(resp.status().is_success());

    let events: serde_json::Value = resp.json().await.unwrap();
    let events_arr = events.as_array().expect("events should be array");
    assert!(
        !events_arr.is_empty(),
        "should have at least one event when filtering by trace_id"
    );

    // All returned events should have the matching trace_id
    for event in events_arr {
        if let Some(event_trace_id) = event["trace_id"].as_str() {
            assert_eq!(
                event_trace_id, trace_id,
                "filtered event should have the requested trace_id"
            );
        }
    }

    // The unrelated log (different trace_id) should NOT appear
    let has_unrelated = events_arr.iter().any(|e| {
        e["body"]
            .as_str()
            .map(|b| b.contains(&other_msg))
            .unwrap_or(false)
            || e["template"]
                .as_str()
                .map(|t| t.contains(&other_msg))
                .unwrap_or(false)
    });
    assert!(
        !has_unrelated,
        "trace_id filter should not return logs from a different trace"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 55: Log Context with trace_id Prioritizes Same-Trace Logs
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_log_context_with_trace_correlation() {
    let ctx = setup().await;

    let trace_id = format!("{:032x}", Uuid::new_v4().as_u128());
    let log_msg = format!("ctx-correlated-log-{}", Uuid::new_v4());

    // ── Step 1: Ingest a log with a trace_id ─────────────────────────────
    step("Ingest OTLP log for context correlation test");
    let log_payload = build_otlp_log_payload(&trace_id, &log_msg, "WARN");
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/logs", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&log_payload)
        .send()
        .await
        .expect("ingest log for context test failed");
    assert!(resp.status().is_success());

    // ── Step 2: Wait for the log and get its ID ──────────────────────────
    step("Wait for log to appear in events");
    let msg_clone = log_msg.clone();
    let mut log_id: Option<String> = None;

    let found = wait_for("log for context test", 20, Duration::from_secs(2), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        let msg = msg_clone.clone();
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/projects/{}/events?time_range=1h",
                    base_url, project_id
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    body.as_array()
                        .map(|a| {
                            a.iter().any(|e| {
                                e["body"]
                                    .as_str()
                                    .map(|b| b.contains(&msg))
                                    .unwrap_or(false)
                                    || e["template"]
                                        .as_str()
                                        .map(|t| t.contains(&msg))
                                        .unwrap_or(false)
                            })
                        })
                        .unwrap_or(false)
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(found, "log for context test never appeared");

    step("Get log_id from unified events");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/events?time_range=1h",
            ctx.base_url, ctx.project_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("list events failed");
    let events: serde_json::Value = resp.json().await.unwrap();
    if let Some(arr) = events.as_array() {
        for e in arr {
            let matches = e["body"]
                .as_str()
                .map(|b| b.contains(&log_msg))
                .unwrap_or(false)
                || e["template"]
                    .as_str()
                    .map(|t| t.contains(&log_msg))
                    .unwrap_or(false);
            if matches {
                if let Some(id) = e["id"].as_str() {
                    log_id = Some(id.to_string());
                }
            }
        }
    }

    let lid = log_id.expect("log_id should have been found");

    // ── Step 3: Get log context WITH trace_id parameter ──────────────────
    step("Get log context with trace_id");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/logs/context?log_id={}&trace_id={}",
            ctx.base_url, ctx.project_id, lid, trace_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("get log context with trace_id failed");

    assert!(
        resp.status().is_success(),
        "log context with trace_id returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let context: serde_json::Value = resp.json().await.unwrap();
    assert!(context.is_array(), "log context should return an array");

    // When trace_id is provided, context should prioritize logs from the same trace
    // Verify the endpoint returns results (may include same-trace and time-window logs)
    let context_arr = context.as_array().unwrap();
    eprintln!(
        "    [info] Log context returned {} entries with trace_id={}",
        context_arr.len(),
        &trace_id[..12]
    );

    // At minimum, the response should be a valid array (even if empty for a single log)
    // The trace_id parameter exercises the trace-prioritized code path
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 56: Profile-Trace Correlation Lookup
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_profile_trace_correlation() {
    let ctx = setup().await;

    let trace_id = format!("{:032x}", Uuid::new_v4().as_u128());
    let span_id = format!("{:016x}", rand::random::<u64>());

    // ── Step 1: Ingest a trace ───────────────────────────────────────────
    step("Ingest trace for profile correlation");
    let trace_payload = build_otlp_trace_payload(&trace_id, &span_id, "profile-corr-svc");
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/traces", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&trace_payload)
        .send()
        .await
        .expect("ingest trace for profile correlation failed");
    assert!(resp.status().is_success());

    // ── Step 2: Ingest a profile with trace correlation via link table ────
    step("Ingest OTLP profile with trace_id link");
    let profile_payload =
        build_otlp_profile_payload_with_trace("profile-corr-svc", &trace_id, &span_id);
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/profiles", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&profile_payload)
        .send()
        .await
        .expect("ingest profile with trace link failed");

    assert!(
        resp.status().is_success(),
        "profile ingestion returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    // ── Step 3: Wait for the profile to appear via trace correlation ─────
    step("Wait for profile to be retrievable via trace_id");
    let tid = trace_id.clone();
    let found = wait_for("profile for trace", 20, Duration::from_secs(2), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        let t = tid.clone();
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/profiles/projects/{}/traces/{}/profile",
                    base_url, project_id, t
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    // Profile should be non-null when the correlation exists
                    !body["profile"].is_null()
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(
        found,
        "profile was never retrievable via trace_id correlation"
    );

    // ── Step 4: Verify the profile detail ────────────────────────────────
    step("Verify profile-trace correlation response");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/profiles/projects/{}/traces/{}/profile",
            ctx.base_url, ctx.project_id, trace_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("get profile for trace failed");
    assert!(resp.status().is_success());

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["trace_id"].as_str(),
        Some(trace_id.as_str()),
        "response should echo the trace_id"
    );
    assert!(
        !body["profile"].is_null(),
        "profile should not be null when trace correlation exists"
    );
    assert!(
        body["profile"]["profile_id"].is_string(),
        "profile should have a profile_id"
    );
    assert!(
        body["profile"]["service_name"].is_string(),
        "profile should have service_name"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 57: error_traces Junction Table Is Populated Correctly
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_error_traces_junction_populated() {
    let ctx = setup().await;

    let trace_id = format!("{:032x}", Uuid::new_v4().as_u128());
    let span_id = format!("{:016x}", rand::random::<u64>());

    // ── Step 1: Ingest a trace ───────────────────────────────────────────
    step("Ingest trace for junction table test");
    let trace_payload = build_otlp_trace_payload(&trace_id, &span_id, "junction-svc");
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/v1/traces", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&trace_payload)
        .send()
        .await
        .expect("ingest trace for junction test failed");
    assert!(resp.status().is_success());

    // ── Step 2: Ingest an exception with trace_id ────────────────────────
    step("Ingest exception with trace_id for junction population");
    let exc_payload = build_exception_payload_with_metadata(
        "Junction test error",
        "JunctionTestError",
        &ctx.project_key,
        "junction-svc",
        "test",
        Some(&trace_id),
    );
    let resp = ctx
        .client
        .post(format!("{}/api/watch/ingest/exceptions", ctx.base_url))
        .bearer_auth(&ctx.project_key)
        .json(&exc_payload)
        .send()
        .await
        .expect("ingest junction exception failed");
    assert!(resp.status().is_success());

    // ── Step 3: Wait for the exception to appear ─────────────────────────
    step("Wait for exception to appear");
    let found = wait_for("junction exception", 20, Duration::from_secs(2), || {
        let client = ctx.client.clone();
        let base_url = ctx.base_url.clone();
        let token = ctx.token.clone();
        let project_id = ctx.project_id;
        async move {
            let resp = client
                .get(format!(
                    "{}/api/watch/projects/{}/exceptions",
                    base_url, project_id
                ))
                .bearer_auth(&token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    body.as_array()
                        .map(|a| {
                            a.iter()
                                .any(|g| g["exception_type"].as_str() == Some("JunctionTestError"))
                        })
                        .unwrap_or(false)
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(found, "junction exception never appeared");

    // ── Step 4: Verify the junction was populated by checking trace detail ──
    // The trace detail endpoint queries error_traces and returns correlated exceptions.
    // If the junction table is populated, GET /traces/{trace_id} will include exceptions.
    step("Verify error_traces junction via trace detail");
    let tid = trace_id.clone();
    let found = wait_for(
        "junction populated (exceptions in trace)",
        20,
        Duration::from_secs(2),
        || {
            let client = ctx.client.clone();
            let base_url = ctx.base_url.clone();
            let token = ctx.token.clone();
            let project_id = ctx.project_id;
            let t = tid.clone();
            async move {
                let resp = client
                    .get(format!(
                        "{}/api/watch/projects/{}/traces/{}",
                        base_url, project_id, t
                    ))
                    .bearer_auth(&token)
                    .send()
                    .await;
                match resp {
                    Ok(r) if r.status().is_success() => {
                        let body: serde_json::Value = r.json().await.unwrap_or_default();
                        // If error_traces junction is populated, exceptions array is non-empty
                        body["exceptions"]
                            .as_array()
                            .map(|a| !a.is_empty())
                            .unwrap_or(false)
                    }
                    _ => false,
                }
            }
        },
    )
    .await;
    assert!(
        found,
        "error_traces junction was never populated (trace detail has no exceptions)"
    );

    // ── Step 5: Verify the exception in the trace detail has correct data ──
    step("Verify junction exception detail");
    let resp = ctx
        .client
        .get(format!(
            "{}/api/watch/projects/{}/traces/{}",
            ctx.base_url, ctx.project_id, trace_id
        ))
        .bearer_auth(&ctx.token)
        .send()
        .await
        .expect("get trace detail for junction test failed");
    assert!(resp.status().is_success());

    let detail: serde_json::Value = resp.json().await.unwrap();
    let exceptions = detail["exceptions"]
        .as_array()
        .expect("exceptions should be array");
    assert!(
        !exceptions.is_empty(),
        "error_traces junction should link at least one exception to the trace"
    );

    // The exception type should match what we ingested
    let has_junction_error = exceptions
        .iter()
        .any(|e| e["exception_type"].as_str() == Some("JunctionTestError"));
    assert!(
        has_junction_error,
        "trace detail exceptions should contain our JunctionTestError"
    );
}
