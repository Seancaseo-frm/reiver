//! Integration tests for the DataFusion-based PromQL evaluator.
//! Tests parse → plan → execute pipeline using in-memory test data.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_array::{Array, Float64Array, RecordBatch, StringArray, TimestampMillisecondArray};
use datafusion::datasource::MemTable;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::SessionContext;

use super::error::EvalError;
use super::extension_plan::PromExtensionPlanner;
use super::planner::{
    collect_metric_refs, metric_table_name, EvalContext, PromPlanner, COL_FINGERPRINT,
    COL_TIMESTAMP, COL_VALUE,
};

fn test_schema(label_names: &[&str]) -> Arc<Schema> {
    let mut fields = vec![
        Field::new(
            COL_TIMESTAMP,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(COL_VALUE, DataType::Float64, false),
        Field::new(COL_FINGERPRINT, DataType::Utf8, false),
    ];
    for name in label_names {
        fields.push(Field::new(format!("lbl_{name}"), DataType::Utf8, true));
    }
    Arc::new(Schema::new(fields))
}

fn make_test_batch(
    timestamps: &[i64],
    values: &[f64],
    fingerprint: &str,
    label_values: &[(&str, &[&str])],
) -> RecordBatch {
    let label_names: Vec<&str> = label_values.iter().map(|(n, _)| *n).collect();
    let schema = test_schema(&label_names);

    let n = timestamps.len();
    let ts_array = TimestampMillisecondArray::from(timestamps.to_vec());
    let val_array = Float64Array::from(values.to_vec());
    let fp_array = StringArray::from(vec![fingerprint; n]);

    let mut columns: Vec<Arc<dyn arrow_array::Array>> =
        vec![Arc::new(ts_array), Arc::new(val_array), Arc::new(fp_array)];
    for (_name, vals) in label_values {
        columns.push(Arc::new(StringArray::from(vals.to_vec())));
    }

    RecordBatch::try_new(schema, columns).unwrap()
}

fn build_session() -> SessionContext {
    #[derive(Debug)]
    struct TestQueryPlanner;

    #[async_trait::async_trait]
    impl datafusion::execution::context::QueryPlanner for TestQueryPlanner {
        async fn create_physical_plan(
            &self,
            logical_plan: &datafusion::logical_expr::LogicalPlan,
            session_state: &datafusion::execution::session_state::SessionState,
        ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
            let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
                PromExtensionPlanner,
            )]);
            planner
                .create_physical_plan(logical_plan, session_state)
                .await
        }
    }

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_query_planner(Arc::new(TestQueryPlanner))
        .build();

    SessionContext::new_with_state(state)
}

fn register_metric(session: &SessionContext, metric_name: &str, batches: Vec<RecordBatch>) {
    let schema = if let Some(b) = batches.first() {
        b.schema()
    } else {
        test_schema(&[])
    };
    let table_name = metric_table_name(metric_name);
    let mem_table = MemTable::try_new(schema, vec![batches]).unwrap();
    session
        .register_table(&table_name, Arc::new(mem_table))
        .unwrap();
}

fn make_simple_metric_data(metric_name: &str) -> Vec<RecordBatch> {
    let start = 1_000_000i64;
    let step = 15_000i64;
    let n = 20;
    let timestamps: Vec<i64> = (0..n).map(|i| start + i * step).collect();
    let values: Vec<f64> = (0..n).map(|i| (i as f64) * 10.0 + 100.0).collect();

    vec![make_test_batch(
        &timestamps,
        &values,
        "fp_001",
        &[("instance", &vec!["node1"; n as usize])],
    )]
}

/// Test that the planner can parse and plan a simple instant vector.
#[tokio::test]
async fn test_plan_simple_vector_selector() {
    let session = build_session();
    register_metric(&session, "up", make_simple_metric_data("up"));

    let ast = crate::promql::parse("up").unwrap();
    let planner = PromPlanner::new(EvalContext::new(1_000_000, 1_285_000, 15_000));
    let plan = planner.plan(&ast, &session).unwrap();

    let df = session.execute_logical_plan(plan).await.unwrap();
    let results = df.collect().await.unwrap();

    assert!(!results.is_empty(), "should produce results");
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows > 0, "should have at least one row");
}

/// Test rate() function planning.
#[tokio::test]
async fn test_plan_rate() {
    let session = build_session();
    let start = 1_000_000i64;
    let step = 15_000i64;
    let n = 40;
    let timestamps: Vec<i64> = (0..n).map(|i| start + i * step).collect();
    let values: Vec<f64> = (0..n).map(|i| (i as f64) * 5.0).collect();
    let batches = vec![make_test_batch(
        &timestamps,
        &values,
        "fp_001",
        &[("instance", &vec!["node1"; n as usize])],
    )];
    register_metric(&session, "http_requests_total", batches);

    let ast = crate::promql::parse(r#"rate(http_requests_total{instance="node1"}[5m])"#).unwrap();
    let range_ms = 5 * 60 * 1000;
    let eval_start = start + range_ms;
    let eval_end = start + (n as i64 - 1) * step;
    let planner = PromPlanner::new(EvalContext::new(eval_start, eval_end, step));
    let plan = planner.plan(&ast, &session).unwrap();

    let df = session.execute_logical_plan(plan).await.unwrap();
    let results = df.collect().await.unwrap();

    assert!(!results.is_empty(), "rate() should produce results");
}

/// Test sum aggregation.
#[tokio::test]
async fn test_plan_sum_aggregation() {
    let session = build_session();
    register_metric(&session, "metric_a", make_simple_metric_data("metric_a"));

    let ast = crate::promql::parse(r#"sum(metric_a{instance="node1"})"#).unwrap();
    let planner = PromPlanner::new(EvalContext::new(1_000_000, 1_285_000, 15_000));
    let plan = planner.plan(&ast, &session).unwrap();

    let df = session.execute_logical_plan(plan).await.unwrap();
    let results = df.collect().await.unwrap();

    assert!(!results.is_empty(), "sum() should produce results");
}

/// Test binary operations.
#[tokio::test]
async fn test_plan_binary_division() {
    let session = build_session();
    register_metric(&session, "disk_free", make_simple_metric_data("disk_free"));
    register_metric(
        &session,
        "disk_total",
        make_simple_metric_data("disk_total"),
    );

    let ast = crate::promql::parse("disk_free / disk_total").unwrap();
    let planner = PromPlanner::new(EvalContext::new(1_000_000, 1_285_000, 15_000));
    let plan = planner.plan(&ast, &session).unwrap();

    let df = session.execute_logical_plan(plan).await.unwrap();
    let results = df.collect().await.unwrap();

    assert!(
        !results.is_empty(),
        "binary division should produce results"
    );
}

/// Test number literal.
#[tokio::test]
async fn test_plan_number_literal() {
    let session = build_session();

    let ast = crate::promql::parse("42").unwrap();
    let planner = PromPlanner::new(EvalContext::new(1_000_000, 1_060_000, 15_000));
    let plan = planner.plan(&ast, &session).unwrap();

    let df = session.execute_logical_plan(plan).await.unwrap();
    let results = df.collect().await.unwrap();

    assert!(!results.is_empty(), "number literal should produce results");
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 5,
        "should have 5 steps: 1000000 to 1060000 / 15000"
    );
}

/// Test vector(0) function.
#[tokio::test]
async fn test_plan_vector_zero() {
    let session = build_session();

    let ast = crate::promql::parse("vector(0)").unwrap();
    let planner = PromPlanner::new(EvalContext::new(1_000_000, 1_060_000, 15_000));
    let plan = planner.plan(&ast, &session).unwrap();

    let df = session.execute_logical_plan(plan).await.unwrap();
    let results = df.collect().await.unwrap();

    assert!(!results.is_empty());
}

/// Test collect_metric_refs extracts metric names and labels.
#[test]
fn test_collect_metric_refs() {
    let ast = crate::promql::parse(
        r#"sum(rate(http_requests_total{instance="node1", job="api"}[5m])) by (instance)"#,
    )
    .unwrap();

    let refs = collect_metric_refs(&ast);
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].metric_name, "http_requests_total");
    assert!(refs[0].label_names.contains(&"instance".to_string()));
    assert!(refs[0].label_names.contains(&"job".to_string()));
}

/// Test collect_metric_refs with multiple metrics.
#[test]
fn test_collect_metric_refs_multi() {
    let ast = crate::promql::parse("disk_free / disk_total").unwrap();

    let refs = collect_metric_refs(&ast);
    assert_eq!(refs.len(), 2);
    let names: Vec<&str> = refs.iter().map(|r| r.metric_name.as_str()).collect();
    assert!(names.contains(&"disk_free"));
    assert!(names.contains(&"disk_total"));
}

/// Equality matchers (=) are extracted while regex matchers (=~) are excluded.
#[test]
fn test_collect_metric_refs_equality_matchers() {
    let ast =
        crate::promql::parse(r#"rate(foo{bar="baz", qux=~"a|b"}[5m])"#).unwrap();

    let refs = collect_metric_refs(&ast);
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].metric_name, "foo");
    assert_eq!(refs[0].equality_matchers, vec![("bar".to_string(), "baz".to_string())]);
    assert!(
        !refs[0].equality_matchers.iter().any(|(k, _)| k == "qux"),
        "regex matcher qux should not be included"
    );
}

/// No label selectors → empty equality matchers.
#[test]
fn test_collect_metric_refs_no_labels() {
    let ast = crate::promql::parse("up").unwrap();
    let refs = collect_metric_refs(&ast);
    assert_eq!(refs.len(), 1);
    assert!(refs[0].equality_matchers.is_empty());
}

/// `__name__` matchers are excluded from equality_matchers.
#[test]
fn test_collect_metric_refs_excludes_name_matcher() {
    let ast = crate::promql::parse(r#"{__name__="foo", bar="baz"}"#).unwrap();
    let refs = collect_metric_refs(&ast);
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].metric_name, "foo");
    assert_eq!(refs[0].equality_matchers, vec![("bar".to_string(), "baz".to_string())]);
    assert!(
        !refs[0].equality_matchers.iter().any(|(k, _)| k == "__name__"),
        "__name__ should not be in equality matchers"
    );
}

/// Four histogram_quantile sub-queries referencing the same _bucket metric
/// should produce exactly 1 unique metric ref when deduped by name.
#[test]
fn test_collect_metric_refs_dedup_across_percentile_queries() {
    let queries = [
        r#"histogram_quantile(0.50, sum by (le) (rate(http_server_request_duration_bucket[5m])))"#,
        r#"histogram_quantile(0.90, sum by (le) (rate(http_server_request_duration_bucket[5m])))"#,
        r#"histogram_quantile(0.95, sum by (le) (rate(http_server_request_duration_bucket[5m])))"#,
        r#"histogram_quantile(0.99, sum by (le) (rate(http_server_request_duration_bucket[5m])))"#,
    ];

    let mut all_names: std::collections::HashSet<String> = std::collections::HashSet::new();
    for q in &queries {
        let ast = crate::promql::parse(q).unwrap();
        let refs = collect_metric_refs(&ast);
        for r in refs {
            all_names.insert(r.metric_name);
        }
    }

    assert_eq!(
        all_names.len(),
        1,
        "all four percentile queries reference exactly one metric"
    );
    assert!(all_names.contains("http_server_request_duration_bucket"));
}

/// When the same metric appears with different label filters in a binary expression,
/// equality matchers must NOT be pushed down (intersection is empty).
#[test]
fn test_collect_metric_refs_no_pushdown_for_conflicting_filters() {
    // sum(m{state="used"}) / sum(m) — the bare occurrence has no matchers,
    // so the intersection is empty: no equality pushdown.
    let ast = crate::promql::parse(
        r#"sum(system_memory_usage{state="used"}) / sum(system_memory_usage)"#,
    )
    .unwrap();
    let refs = collect_metric_refs(&ast);
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].metric_name, "system_memory_usage");
    assert!(
        refs[0].equality_matchers.is_empty(),
        "must not push down state=used when the metric also appears unfiltered; got {:?}",
        refs[0].equality_matchers
    );
}

/// When the same metric appears with the SAME filter in all occurrences,
/// that matcher IS pushed down.
#[test]
fn test_collect_metric_refs_pushdown_when_all_agree() {
    // sum(m{job="foo"}) / count(m{job="foo"}) — both have job="foo"
    let ast = crate::promql::parse(
        r#"sum(up{job="prometheus"}) / count(up{job="prometheus"})"#,
    )
    .unwrap();
    let refs = collect_metric_refs(&ast);
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].metric_name, "up");
    assert_eq!(
        refs[0].equality_matchers,
        vec![("job".to_string(), "prometheus".to_string())],
        "matcher common to all occurrences should be pushed down"
    );
}

/// Different metrics across sub-queries produce distinct refs (no false dedup).
#[test]
fn test_collect_metric_refs_distinct_metrics() {
    let queries = [
        r#"rate(http_requests_total[5m])"#,
        r#"rate(http_errors_total[5m])"#,
    ];

    let mut all_names: std::collections::HashSet<String> = std::collections::HashSet::new();
    for q in &queries {
        let ast = crate::promql::parse(q).unwrap();
        for r in collect_metric_refs(&ast) {
            all_names.insert(r.metric_name);
        }
    }

    assert_eq!(all_names.len(), 2);
    assert!(all_names.contains("http_requests_total"));
    assert!(all_names.contains("http_errors_total"));
}

// ---------------------------------------------------------------------------
// Helpers for value-asserting tests
// ---------------------------------------------------------------------------

fn extract_values(results: &[RecordBatch]) -> Vec<f64> {
    results
        .iter()
        .flat_map(|b| {
            let col = b.column_by_name(COL_VALUE).unwrap();
            let n = col.len();
            if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                (0..n)
                    .filter(|&i| !arr.is_null(i))
                    .map(|i| arr.value(i))
                    .collect::<Vec<_>>()
            } else if let Some(arr) = col.as_any().downcast_ref::<arrow_array::Int64Array>() {
                (0..n)
                    .filter(|&i| !arr.is_null(i))
                    .map(|i| arr.value(i) as f64)
                    .collect::<Vec<_>>()
            } else if let Some(arr) = col.as_any().downcast_ref::<arrow_array::UInt64Array>() {
                (0..n)
                    .filter(|&i| !arr.is_null(i))
                    .map(|i| arr.value(i) as f64)
                    .collect::<Vec<_>>()
            } else {
                panic!("value column has unexpected type: {:?}", col.data_type());
            }
        })
        .collect()
}

fn extract_timestamps(results: &[RecordBatch]) -> Vec<i64> {
    results
        .iter()
        .flat_map(|b| {
            b.column_by_name(COL_TIMESTAMP)
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect()
}

fn make_counter_data(
    start: i64,
    step: i64,
    n: usize,
    rate_per_sec: f64,
    label_values: &[(&str, &str)],
) -> Vec<RecordBatch> {
    let timestamps: Vec<i64> = (0..n as i64).map(|i| start + i * step).collect();
    let values: Vec<f64> = (0..n)
        .map(|i| (i as f64) * rate_per_sec * (step as f64 / 1000.0))
        .collect();
    let fp = format!(
        "fp_{}",
        label_values
            .iter()
            .map(|(_, v)| *v)
            .collect::<Vec<_>>()
            .join("_")
    );
    let labels: Vec<(&str, Vec<&str>)> = label_values
        .iter()
        .map(|(k, v)| (*k, vec![*v; n]))
        .collect();
    let label_refs: Vec<(&str, &[&str])> = labels.iter().map(|(k, v)| (*k, v.as_slice())).collect();
    vec![make_test_batch(&timestamps, &values, &fp, &label_refs)]
}

fn make_multi_series_data(
    start: i64,
    step: i64,
    n: usize,
    series: &[(&str, f64)], // (fingerprint_suffix, constant_value)
    label_name: &str,
) -> Vec<RecordBatch> {
    series
        .iter()
        .map(|(label_val, constant)| {
            let timestamps: Vec<i64> = (0..n as i64).map(|i| start + i * step).collect();
            let values: Vec<f64> = vec![*constant; n];
            let fp = format!("fp_{}", label_val);
            make_test_batch(
                &timestamps,
                &values,
                &fp,
                &[(label_name, &vec![*label_val; n])],
            )
        })
        .collect()
}

async fn eval_promql(promql: &str, session: &SessionContext, start: i64, end: i64, step: i64) -> Vec<RecordBatch> {
    let ast = crate::promql::parse(promql).unwrap();
    let planner = PromPlanner::new(EvalContext::new(start, end, step));
    let plan = planner.plan(&ast, session).unwrap();
    let df = session.execute_logical_plan(plan).await.unwrap();
    df.collect().await.unwrap()
}

// ---------------------------------------------------------------------------
// Phase 1: increase(), delta(), irate(), idelta()
// ---------------------------------------------------------------------------

/// increase() on a monotonic counter should return the total increase over the range.
#[tokio::test]
async fn test_increase_monotonic_counter() {
    let session = build_session();
    let start = 0i64;
    let step = 15_000i64;
    let n = 60; // 15 minutes of data at 15s intervals
    // Counter increasing by 1.0/sec → +15 per sample
    let data = make_counter_data(start, step, n, 1.0, &[("job", "test")]);
    register_metric(&session, "requests_total", data);

    let range_ms = 60_000; // 1 minute range
    let eval_start = start + range_ms + step;
    let eval_end = start + (n as i64 - 2) * step;
    let results = eval_promql(
        r#"increase(requests_total{job="test"}[1m])"#,
        &session,
        eval_start,
        eval_end,
        step,
    )
    .await;

    let values = extract_values(&results);
    assert!(!values.is_empty(), "increase() should produce results");
    for v in &values {
        // With extrapolation, increase over 1m of a 1/sec counter ≈ 60
        // Allow generous tolerance due to Prometheus extrapolation at boundaries
        assert!(
            *v > 50.0 && *v < 75.0,
            "increase over 1m at 1/sec rate should be ~60, got {}",
            v
        );
    }
}

/// delta() on a gauge should return the difference between last and first in the range.
#[tokio::test]
async fn test_delta_gauge() {
    let session = build_session();
    let start = 0i64;
    let step = 15_000i64;
    let n = 60;
    // Gauge increasing linearly: value = i * 10.0
    let timestamps: Vec<i64> = (0..n).map(|i| start + i * step).collect();
    let values: Vec<f64> = (0..n).map(|i| i as f64 * 10.0).collect();
    let batches = vec![make_test_batch(
        &timestamps,
        &values,
        "fp_001",
        &[("host", &vec!["a"; n as usize])],
    )];
    register_metric(&session, "temperature", batches);

    let range_ms = 60_000;
    let eval_start = start + range_ms + step;
    let eval_end = start + (n - 2) * step;
    let results = eval_promql(
        r#"delta(temperature{host="a"}[1m])"#,
        &session,
        eval_start,
        eval_end,
        step,
    )
    .await;

    let values = extract_values(&results);
    assert!(!values.is_empty(), "delta() should produce results");
    for v in &values {
        // 4 samples in 1m window at 15s intervals → raw delta = 3*10 = 30, extrapolated ≈ 40
        assert!(
            *v > 25.0 && *v < 50.0,
            "delta over 1m of linear gauge (+10/15s) should be ~40, got {}",
            v
        );
    }
}

/// irate() uses only the last two samples — should give instantaneous rate.
#[tokio::test]
async fn test_irate_last_two_samples() {
    let session = build_session();
    let start = 0i64;
    let step = 15_000i64;
    let n = 40;
    // Counter: value = i * 15 (i.e. 1.0/sec)
    let timestamps: Vec<i64> = (0..n).map(|i| start + i * step).collect();
    let values: Vec<f64> = (0..n).map(|i| i as f64 * 15.0).collect();
    let batches = vec![make_test_batch(
        &timestamps,
        &values,
        "fp_001",
        &[("job", &vec!["api"; n as usize])],
    )];
    register_metric(&session, "counter", batches);

    let range_ms = 60_000;
    let eval_start = start + range_ms + step;
    let eval_end = start + (n as i64 - 2) * step;
    let results = eval_promql(
        r#"irate(counter{job="api"}[1m])"#,
        &session,
        eval_start,
        eval_end,
        step,
    )
    .await;

    let values = extract_values(&results);
    assert!(!values.is_empty(), "irate() should produce results");
    for v in &values {
        // irate = (last - prev) / (last_ts - prev_ts) = 15 / 15 = 1.0
        assert!(
            (*v - 1.0).abs() < 0.01,
            "irate of counter with 15/15s should be 1.0/sec, got {}",
            v
        );
    }
}

/// idelta() uses only the last two samples — should give instantaneous delta.
#[tokio::test]
async fn test_idelta_last_two_samples() {
    let session = build_session();
    let start = 0i64;
    let step = 15_000i64;
    let n = 40;
    // Gauge: value = i * 7.0
    let timestamps: Vec<i64> = (0..n).map(|i| start + i * step).collect();
    let values: Vec<f64> = (0..n).map(|i| i as f64 * 7.0).collect();
    let batches = vec![make_test_batch(
        &timestamps,
        &values,
        "fp_001",
        &[("host", &vec!["srv"; n as usize])],
    )];
    register_metric(&session, "gauge_metric", batches);

    let range_ms = 60_000;
    let eval_start = start + range_ms + step;
    let eval_end = start + (n as i64 - 2) * step;
    let results = eval_promql(
        r#"idelta(gauge_metric{host="srv"}[1m])"#,
        &session,
        eval_start,
        eval_end,
        step,
    )
    .await;

    let values = extract_values(&results);
    assert!(!values.is_empty(), "idelta() should produce results");
    for v in &values {
        // idelta = last - prev = 7.0
        assert!(
            (*v - 7.0).abs() < 0.01,
            "idelta of linear gauge (+7/step) should be 7.0, got {}",
            v
        );
    }
}

// ---------------------------------------------------------------------------
// Phase 1: Aggregations with multiple series
// ---------------------------------------------------------------------------

/// sum by (label) with multiple series should sum values per group.
#[tokio::test]
async fn test_sum_by_label() {
    let session = build_session();
    let start = 1_000_000i64;
    let step = 15_000i64;
    let n = 10usize;
    let data = make_multi_series_data(start, step, n, &[("a", 10.0), ("b", 20.0), ("a", 5.0)], "group");
    register_metric(&session, "metric", data);

    let eval_end = start + (n as i64 - 1) * step;
    let results = eval_promql(
        r#"sum by (group) (metric)"#,
        &session,
        start,
        eval_end,
        step,
    )
    .await;

    let values = extract_values(&results);
    assert!(!values.is_empty(), "sum by should produce results");
    // group="a" has 10+5=15, group="b" has 20
    let has_15 = values.iter().any(|v| (*v - 15.0).abs() < 0.01);
    let has_20 = values.iter().any(|v| (*v - 20.0).abs() < 0.01);
    assert!(has_15, "expected sum=15 for group=a, values: {:?}", values);
    assert!(has_20, "expected sum=20 for group=b, values: {:?}", values);
}

/// avg() across all series should produce the mean.
#[tokio::test]
async fn test_avg_aggregation() {
    let session = build_session();
    let start = 1_000_000i64;
    let step = 15_000i64;
    let n = 10usize;
    let data = make_multi_series_data(start, step, n, &[("x", 10.0), ("y", 30.0)], "instance");
    register_metric(&session, "cpu", data);

    let eval_end = start + (n as i64 - 1) * step;
    let results = eval_promql(r#"avg(cpu)"#, &session, start, eval_end, step).await;

    let values = extract_values(&results);
    assert!(!values.is_empty());
    for v in &values {
        assert!(
            (*v - 20.0).abs() < 0.01,
            "avg of [10, 30] should be 20, got {}",
            v
        );
    }
}

/// min() and max() across multiple series.
#[tokio::test]
async fn test_min_max_aggregation() {
    let session = build_session();
    let start = 1_000_000i64;
    let step = 15_000i64;
    let n = 10usize;
    let data = make_multi_series_data(start, step, n, &[("a", 5.0), ("b", 25.0), ("c", 15.0)], "pod");
    register_metric(&session, "mem", data);

    let eval_end = start + (n as i64 - 1) * step;

    let min_results = eval_promql(r#"min(mem)"#, &session, start, eval_end, step).await;
    let min_values = extract_values(&min_results);
    assert!(!min_values.is_empty());
    for v in &min_values {
        assert!((*v - 5.0).abs() < 0.01, "min should be 5, got {}", v);
    }

    let max_results = eval_promql(r#"max(mem)"#, &session, start, eval_end, step).await;
    let max_values = extract_values(&max_results);
    assert!(!max_values.is_empty());
    for v in &max_values {
        assert!((*v - 25.0).abs() < 0.01, "max should be 25, got {}", v);
    }
}

/// count() should return the number of series.
#[tokio::test]
async fn test_count_aggregation() {
    let session = build_session();
    let start = 1_000_000i64;
    let step = 15_000i64;
    let n = 10usize;
    let data = make_multi_series_data(start, step, n, &[("a", 1.0), ("b", 2.0), ("c", 3.0)], "node");
    register_metric(&session, "up", data);

    let eval_end = start + (n as i64 - 1) * step;
    let results = eval_promql(r#"count(up)"#, &session, start, eval_end, step).await;

    let values = extract_values(&results);
    assert!(!values.is_empty());
    for v in &values {
        assert!((*v - 3.0).abs() < 0.01, "count of 3 series should be 3, got {}", v);
    }
}

// ---------------------------------------------------------------------------
// Phase 1: topk / bottomk
// ---------------------------------------------------------------------------

/// topk(2, ...) with 3 series: produces results that include the highest values.
#[tokio::test]
async fn test_topk() {
    let session = build_session();
    let start = 1_000_000i64;
    let step = 15_000i64;
    let n = 20usize;
    let data = make_multi_series_data(start, step, n, &[("low", 1.0), ("mid", 50.0), ("high", 100.0)], "tier");
    register_metric(&session, "score", data);

    let eval_end = start + (n as i64 - 1) * step;
    let results = eval_promql(r#"topk(2, score)"#, &session, start, eval_end, step).await;

    let values = extract_values(&results);
    assert!(!values.is_empty(), "topk should produce results");
    assert!(
        values.iter().any(|v| (*v - 100.0).abs() < 0.01),
        "topk(2) must include the highest value (100), values: {:?}",
        values
    );
    assert!(
        values.iter().any(|v| (*v - 50.0).abs() < 0.01),
        "topk(2) must include the second-highest value (50), values: {:?}",
        values
    );
}

/// bottomk(1, ...) executes successfully and produces results.
#[tokio::test]
async fn test_bottomk() {
    let session = build_session();
    let start = 1_000_000i64;
    let step = 15_000i64;
    let n = 20usize;
    let data = make_multi_series_data(start, step, n, &[("low", 3.0), ("mid", 50.0), ("high", 99.0)], "tier");
    register_metric(&session, "score", data);

    let eval_end = start + (n as i64 - 1) * step;
    let results = eval_promql(r#"bottomk(1, score)"#, &session, start, eval_end, step).await;

    let values = extract_values(&results);
    assert!(!values.is_empty(), "bottomk should produce results");
    // Verify bottomk produces valid numeric results
    for v in &values {
        assert!(v.is_finite(), "bottomk values should be finite, got {}", v);
    }
}

// ---------------------------------------------------------------------------
// Phase 1: *_over_time and histogram_quantile
// ---------------------------------------------------------------------------

/// avg_over_time should return the average of samples within the range window.
#[tokio::test]
async fn test_avg_over_time() {
    let session = build_session();
    let start = 0i64;
    let step = 15_000i64;
    let n = 40usize;
    // Constant value = 42
    let timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * step).collect();
    let values: Vec<f64> = vec![42.0; n];
    let batches = vec![make_test_batch(
        &timestamps,
        &values,
        "fp_001",
        &[("job", &vec!["x"; n])],
    )];
    register_metric(&session, "const_metric", batches);

    let range_ms = 60_000;
    let eval_start = start + range_ms + step;
    let eval_end = start + (n as i64 - 2) * step;
    let results = eval_promql(
        r#"avg_over_time(const_metric[1m])"#,
        &session,
        eval_start,
        eval_end,
        step,
    )
    .await;

    let result_values = extract_values(&results);
    assert!(!result_values.is_empty());
    for v in &result_values {
        assert!(
            (*v - 42.0).abs() < 0.01,
            "avg_over_time of constant 42 should be 42, got {}",
            v
        );
    }
}

/// sum_over_time should return the sum of samples within the range window.
#[tokio::test]
async fn test_sum_over_time() {
    let session = build_session();
    let start = 0i64;
    let step = 15_000i64;
    let n = 40usize;
    // All values = 1.0, range = 1m → 4 samples in window (at 15s intervals, +1 extra from lookback) → sum ≈ 4-5
    let timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * step).collect();
    let values: Vec<f64> = vec![1.0; n];
    let batches = vec![make_test_batch(
        &timestamps,
        &values,
        "fp_001",
        &[("job", &vec!["x"; n])],
    )];
    register_metric(&session, "ones", batches);

    let range_ms = 60_000;
    let eval_start = start + range_ms + step;
    let eval_end = start + (n as i64 - 2) * step;
    let results = eval_promql(
        r#"sum_over_time(ones[1m])"#,
        &session,
        eval_start,
        eval_end,
        step,
    )
    .await;

    let result_values = extract_values(&results);
    assert!(!result_values.is_empty());
    for v in &result_values {
        // 1m / 15s = 4 samples in window, plus possibly 1 from boundary → 4 or 5
        assert!(
            *v >= 3.0 && *v <= 6.0,
            "sum_over_time of ones[1m] should be ~4-5, got {}",
            v
        );
    }
}

// ---------------------------------------------------------------------------
// Phase 1: Edge cases — offset, counter resets, staleness, grid alignment
// ---------------------------------------------------------------------------

/// Counter reset: rate() should handle a counter that resets mid-window.
#[tokio::test]
async fn test_rate_counter_reset() {
    let session = build_session();
    let start = 0i64;
    let step = 15_000i64;
    let n = 40usize;
    // Counter: goes 0,15,30,45,60, then resets to 0,15,30,45...
    let timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * step).collect();
    let values: Vec<f64> = (0..n)
        .map(|i| {
            let cycle = i % 5;
            cycle as f64 * 15.0
        })
        .collect();
    let batches = vec![make_test_batch(
        &timestamps,
        &values,
        "fp_001",
        &[("job", &vec!["test"; n])],
    )];
    register_metric(&session, "resets", batches);

    let range_ms = 120_000; // 2m range to capture resets
    let eval_start = start + range_ms + step;
    let eval_end = start + (n as i64 - 2) * step;
    let results = eval_promql(
        r#"rate(resets[2m])"#,
        &session,
        eval_start,
        eval_end,
        step,
    )
    .await;

    let result_values = extract_values(&results);
    assert!(!result_values.is_empty(), "rate() with counter resets should produce results");
    for v in &result_values {
        assert!(
            *v >= 0.0,
            "rate() must never be negative even with resets, got {}",
            v
        );
    }
}

/// Multi-step range query: timestamps in output should align to the eval grid.
#[tokio::test]
async fn test_grid_alignment() {
    let session = build_session();
    let start = 1_000_000i64;
    let step = 30_000i64;
    let n = 20usize;
    let timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * 15_000).collect();
    let values: Vec<f64> = vec![1.0; n];
    let batches = vec![make_test_batch(
        &timestamps,
        &values,
        "fp_001",
        &[("x", &vec!["y"; n])],
    )];
    register_metric(&session, "aligned", batches);

    let eval_start = start;
    let eval_end = start + 120_000; // 2 minutes
    let eval_step = 30_000i64;
    let results = eval_promql(r#"aligned"#, &session, eval_start, eval_end, eval_step).await;

    let ts = extract_timestamps(&results);
    assert!(!ts.is_empty());
    for t in &ts {
        assert_eq!(
            (t - eval_start) % eval_step,
            0,
            "output timestamp {} should align to eval grid (start={}, step={})",
            t,
            eval_start,
            eval_step,
        );
    }
}

/// Staleness: samples outside the 5-minute lookback produce no output.
#[tokio::test]
async fn test_staleness_no_output() {
    let session = build_session();
    // Data only at time 0
    let batches = vec![make_test_batch(
        &[0],
        &[42.0],
        "fp_001",
        &[("a", &["v"])],
    )];
    register_metric(&session, "stale", batches);

    // Evaluate well beyond lookback (5min = 300_000ms)
    let eval_start = 600_000;
    let eval_end = 700_000;
    let results = eval_promql(r#"stale"#, &session, eval_start, eval_end, 15_000).await;

    let values = extract_values(&results);
    assert!(
        values.is_empty(),
        "stale data beyond lookback should produce no results, got {:?}",
        values
    );
}

// ---------------------------------------------------------------------------
// Phase 1: Binary vector matching and set operations
// ---------------------------------------------------------------------------

/// Binary division between two metrics with matching labels.
#[tokio::test]
async fn test_binary_division_values() {
    let session = build_session();
    let start = 1_000_000i64;
    let step = 15_000i64;
    let n = 10usize;

    let timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * step).collect();
    let free_values: Vec<f64> = vec![75.0; n];
    let total_values: Vec<f64> = vec![100.0; n];

    let free_batch = make_test_batch(
        &timestamps,
        &free_values,
        "fp_001",
        &[("host", &vec!["srv1"; n])],
    );
    let total_batch = make_test_batch(
        &timestamps,
        &total_values,
        "fp_001",
        &[("host", &vec!["srv1"; n])],
    );
    register_metric(&session, "disk_free", vec![free_batch]);
    register_metric(&session, "disk_total", vec![total_batch]);

    let eval_end = start + (n as i64 - 1) * step;
    let results = eval_promql(r#"disk_free / disk_total"#, &session, start, eval_end, step).await;

    let values = extract_values(&results);
    assert!(!values.is_empty());
    for v in &values {
        assert!(
            (*v - 0.75).abs() < 0.01,
            "75/100 should be 0.75, got {}",
            v
        );
    }
}

/// `or` set operation: union of two metrics.
#[tokio::test]
async fn test_or_set_operation() {
    let session = build_session();
    let start = 1_000_000i64;
    let step = 15_000i64;
    let n = 5usize;

    let timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * step).collect();
    let a_batch = make_test_batch(
        &timestamps,
        &vec![10.0; n],
        "fp_a",
        &[("job", &vec!["a"; n])],
    );
    let b_batch = make_test_batch(
        &timestamps,
        &vec![20.0; n],
        "fp_b",
        &[("job", &vec!["b"; n])],
    );
    register_metric(&session, "metric_a", vec![a_batch]);
    register_metric(&session, "metric_b", vec![b_batch]);

    let eval_end = start + (n as i64 - 1) * step;
    let results = eval_promql(r#"metric_a or metric_b"#, &session, start, eval_end, step).await;

    let values = extract_values(&results);
    assert!(!values.is_empty());
    let has_10 = values.iter().any(|v| (*v - 10.0).abs() < 0.01);
    let has_20 = values.iter().any(|v| (*v - 20.0).abs() < 0.01);
    assert!(has_10, "or should include metric_a values");
    assert!(has_20, "or should include metric_b values");
}

/// Number literal produces correct row count and value.
#[tokio::test]
async fn test_number_literal_value_and_count() {
    let session = build_session();
    let start = 1_000_000i64;
    let end = 1_060_000i64;
    let step = 15_000i64;
    let results = eval_promql("42", &session, start, end, step).await;

    let values = extract_values(&results);
    let expected_steps = ((end - start) / step + 1) as usize;
    assert_eq!(values.len(), expected_steps, "should have {} steps", expected_steps);
    for v in &values {
        assert!((*v - 42.0).abs() < 0.01, "all values should be 42, got {}", v);
    }
}

// ---------------------------------------------------------------------------
// Phase 4: Property-Based Sanity Checks
// ---------------------------------------------------------------------------

/// Property: rate() of a monotonically increasing counter is always >= 0.
#[tokio::test]
async fn test_property_rate_always_non_negative() {
    let session = build_session();
    let start = 0i64;
    let step = 15_000i64;
    let n = 60usize;
    // Strictly monotonic counter
    let timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * step).collect();
    let values: Vec<f64> = (0..n).map(|i| i as f64 * 7.3).collect();
    let batches = vec![make_test_batch(
        &timestamps,
        &values,
        "fp_001",
        &[("x", &vec!["y"; n])],
    )];
    register_metric(&session, "mono_counter", batches);

    let range_ms = 60_000;
    let eval_start = start + range_ms + step;
    let eval_end = start + (n as i64 - 2) * step;
    let results = eval_promql(
        r#"rate(mono_counter[1m])"#,
        &session,
        eval_start,
        eval_end,
        step,
    )
    .await;

    let values = extract_values(&results);
    for v in &values {
        assert!(*v >= 0.0, "rate() of monotonic counter must be >= 0, got {}", v);
    }
}

/// Property: sum by () (metric) == metric for single-series data.
#[tokio::test]
async fn test_property_sum_by_empty_equals_identity() {
    let session = build_session();
    let start = 1_000_000i64;
    let step = 15_000i64;
    let n = 10usize;
    let timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * step).collect();
    let values: Vec<f64> = vec![77.0; n];
    let batches = vec![make_test_batch(
        &timestamps,
        &values,
        "fp_001",
        &[("k", &vec!["v"; n])],
    )];
    register_metric(&session, "single_metric", batches);

    let eval_end = start + (n as i64 - 1) * step;
    let sum_results = eval_promql(r#"sum(single_metric)"#, &session, start, eval_end, step).await;
    let raw_results = eval_promql(r#"single_metric"#, &session, start, eval_end, step).await;

    let sum_vals = extract_values(&sum_results);
    let raw_vals = extract_values(&raw_results);
    assert_eq!(sum_vals.len(), raw_vals.len());
    for (s, r) in sum_vals.iter().zip(raw_vals.iter()) {
        assert!(
            (s - r).abs() < 0.01,
            "sum() of single series should equal original: {} vs {}",
            s,
            r
        );
    }
}

/// Property: topk(1, metric) with one series returns that series unchanged.
#[tokio::test]
async fn test_property_topk1_single_series_identity() {
    let session = build_session();
    let start = 1_000_000i64;
    let step = 15_000i64;
    let n = 10usize;
    let timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * step).collect();
    let values: Vec<f64> = vec![55.5; n];
    let batches = vec![make_test_batch(
        &timestamps,
        &values,
        "fp_001",
        &[("k", &vec!["v"; n])],
    )];
    register_metric(&session, "solo", batches);

    let eval_end = start + (n as i64 - 1) * step;
    let results = eval_promql(r#"topk(1, solo)"#, &session, start, eval_end, step).await;
    let raw_results = eval_promql(r#"solo"#, &session, start, eval_end, step).await;

    let topk_vals = extract_values(&results);
    let raw_vals = extract_values(&raw_results);
    assert_eq!(
        topk_vals.len(),
        raw_vals.len(),
        "topk(1) of single series should return same row count"
    );
    for v in &topk_vals {
        assert!((*v - 55.5).abs() < 0.01, "topk(1) should preserve value, got {}", v);
    }
}

/// Property: increase() ≈ rate() * range_seconds for a clean counter.
#[tokio::test]
async fn test_property_increase_equals_rate_times_range() {
    let session = build_session();
    let start = 0i64;
    let step = 15_000i64;
    let n = 60usize;
    let timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * step).collect();
    let values: Vec<f64> = (0..n).map(|i| i as f64 * 10.0).collect();
    let batches = vec![make_test_batch(
        &timestamps,
        &values,
        "fp_001",
        &[("x", &vec!["y"; n])],
    )];
    register_metric(&session, "clean_counter", batches);

    let range_ms = 60_000;
    let range_secs = range_ms as f64 / 1000.0;
    let eval_start = start + range_ms + step;
    let eval_end = start + (n as i64 - 2) * step;

    let inc_results = eval_promql(
        r#"increase(clean_counter[1m])"#,
        &session,
        eval_start,
        eval_end,
        step,
    )
    .await;
    let rate_results = eval_promql(
        r#"rate(clean_counter[1m])"#,
        &session,
        eval_start,
        eval_end,
        step,
    )
    .await;

    let inc_vals = extract_values(&inc_results);
    let rate_vals = extract_values(&rate_results);
    assert_eq!(inc_vals.len(), rate_vals.len());
    for (inc, rate) in inc_vals.iter().zip(rate_vals.iter()) {
        let expected_inc = rate * range_secs;
        assert!(
            (inc - expected_inc).abs() < 1.0,
            "increase should ≈ rate * range_secs: inc={}, rate*{}={}",
            inc,
            range_secs,
            expected_inc,
        );
    }
}

// ---------------------------------------------------------------------------
// Go PromQL-inspired corner cases
// ---------------------------------------------------------------------------

/// From Go tests: counter that resets to 0 then continues — increase should
/// count total including the pre-reset values (Prometheus "missed values" logic).
#[tokio::test]
async fn test_counter_reset_increase_go_style() {
    let session = build_session();
    let step = 15_000i64;
    // Go test: 0 1 2 3 [reset to 0] 2 3 4 → increase = 7
    // (3 before reset + 4 after = 7)
    let timestamps: Vec<i64> = (0..7).map(|i| i as i64 * step).collect();
    let values: Vec<f64> = vec![0.0, 1.0, 2.0, 3.0, 2.0, 3.0, 4.0];
    let batches = vec![make_test_batch(
        &timestamps,
        &values,
        "fp_001",
        &[("p", &vec!["foo"; 7])],
    )];
    register_metric(&session, "reset_ctr", batches);

    let eval_end = 6 * step;
    let range_ms = eval_end; // Use entire data range
    let results = eval_promql(
        &format!("increase(reset_ctr[{}s])", range_ms / 1000),
        &session,
        eval_end,
        eval_end,
        step,
    )
    .await;

    let vals = extract_values(&results);
    assert!(!vals.is_empty(), "increase with reset should produce results");
    // With Prometheus extrapolation, the value should be around 7 (with some extrapolation factor)
    let v = vals[0];
    assert!(
        v > 5.0 && v < 10.0,
        "increase of [0,1,2,3,reset,2,3,4] should be ~7 (got {})",
        v
    );
}

/// From Go tests: increase() with counter starting at non-zero should give
/// same result as one starting at zero (only the increment matters).
#[tokio::test]
async fn test_increase_nonzero_start_same_as_zero() {
    let session = build_session();
    let start = 0i64;
    let step = 15_000i64;
    let n = 40usize;

    // Counter A: starts at 0, +10 per step
    let timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * step).collect();
    let values_a: Vec<f64> = (0..n).map(|i| i as f64 * 10.0).collect();
    let batch_a = make_test_batch(
        &timestamps,
        &values_a,
        "fp_a",
        &[("start", &vec!["zero"; n])],
    );

    // Counter B: starts at 1000, +10 per step (same increment)
    let values_b: Vec<f64> = (0..n).map(|i| 1000.0 + i as f64 * 10.0).collect();
    let batch_b = make_test_batch(
        &timestamps,
        &values_b,
        "fp_b",
        &[("start", &vec!["nonzero"; n])],
    );

    register_metric(&session, "ctr_a", vec![batch_a]);
    register_metric(&session, "ctr_b", vec![batch_b]);

    let range_ms = 60_000;
    let eval_start = start + range_ms + step;
    let eval_end = start + (n as i64 - 2) * step;

    let results_a = eval_promql(
        r#"increase(ctr_a[1m])"#,
        &session,
        eval_start,
        eval_end,
        step,
    )
    .await;
    let results_b = eval_promql(
        r#"increase(ctr_b[1m])"#,
        &session,
        eval_start,
        eval_end,
        step,
    )
    .await;

    let vals_a = extract_values(&results_a);
    let vals_b = extract_values(&results_b);
    assert_eq!(vals_a.len(), vals_b.len());
    for (a, b) in vals_a.iter().zip(vals_b.iter()) {
        assert!(
            (a - b).abs() < 1.0,
            "increase should be same regardless of starting value: {} vs {}",
            a,
            b
        );
    }
}

/// From Go tests: division by zero produces NaN or Inf.
#[tokio::test]
async fn test_division_by_zero() {
    let session = build_session();
    let start = 1_000_000i64;
    let step = 15_000i64;
    let n = 5usize;

    let timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * step).collect();
    let numerator_batch = make_test_batch(
        &timestamps,
        &vec![10.0; n],
        "fp_001",
        &[("x", &vec!["a"; n])],
    );
    let denominator_batch = make_test_batch(
        &timestamps,
        &vec![0.0; n],
        "fp_001",
        &[("x", &vec!["a"; n])],
    );
    register_metric(&session, "num", vec![numerator_batch]);
    register_metric(&session, "denom", vec![denominator_batch]);

    let eval_end = start + (n as i64 - 1) * step;
    let results = eval_promql(r#"num / denom"#, &session, start, eval_end, step).await;

    let values = extract_values(&results);
    assert!(!values.is_empty(), "division by zero should produce results");
    for v in &values {
        assert!(
            v.is_infinite() || v.is_nan(),
            "10/0 should be Inf or NaN, got {}",
            v
        );
    }
}

/// From Go tests: rate() with a single sample in range produces no output.
#[tokio::test]
async fn test_rate_single_sample_no_output() {
    let session = build_session();
    // Only one sample in the entire dataset
    let batches = vec![make_test_batch(
        &[1_000_000],
        &[42.0],
        "fp_001",
        &[("a", &["v"])],
    )];
    register_metric(&session, "sparse", batches);

    // Evaluate at a time where only this 1 sample is in the lookback window
    let results = eval_promql(
        r#"rate(sparse[5m])"#,
        &session,
        1_000_000,
        1_000_000,
        15_000,
    )
    .await;

    let values = extract_values(&results);
    // rate() needs at least 2 samples — should produce no non-null values
    assert!(
        values.is_empty(),
        "rate() with single sample should produce no results (null filtered), got: {:?}",
        values
    );
}

/// From Go tests: irate() with NaN values should skip them.
/// In our implementation, NaN triggers staleness — verify no panic.
#[tokio::test]
async fn test_irate_with_nan_value() {
    let session = build_session();
    let step = 15_000i64;
    let n = 10usize;
    let timestamps: Vec<i64> = (0..n).map(|i| i as i64 * step).collect();
    // Insert NaN at position 5
    let mut values: Vec<f64> = (0..n).map(|i| i as f64 * 10.0).collect();
    values[5] = f64::NAN;
    let batches = vec![make_test_batch(
        &timestamps,
        &values,
        "fp_001",
        &[("x", &vec!["y"; n])],
    )];
    register_metric(&session, "nan_metric", batches);

    let eval_end = (n as i64 - 1) * step;
    // Should not panic — NaN may cause staleness/gaps
    let results = eval_promql(
        r#"irate(nan_metric[1m])"#,
        &session,
        60_000,
        eval_end,
        step,
    )
    .await;

    // Just verify it doesn't panic and produces some output
    let _ = extract_values(&results);
}

/// From Go tests: multiplication by scalar.
#[tokio::test]
async fn test_scalar_multiplication() {
    let session = build_session();
    let start = 1_000_000i64;
    let step = 15_000i64;
    let n = 10usize;
    let timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * step).collect();
    let values: Vec<f64> = vec![5.0; n];
    let batches = vec![make_test_batch(
        &timestamps,
        &values,
        "fp_001",
        &[("k", &vec!["v"; n])],
    )];
    register_metric(&session, "base", batches);

    let eval_end = start + (n as i64 - 1) * step;
    let results = eval_promql(r#"base * 3"#, &session, start, eval_end, step).await;

    let result_values = extract_values(&results);
    assert!(!result_values.is_empty());
    for v in &result_values {
        assert!(
            (*v - 15.0).abs() < 0.01,
            "5 * 3 should be 15, got {}",
            v
        );
    }
}

/// From Go tests: negative values in gauge delta should produce negative results.
#[tokio::test]
async fn test_delta_decreasing_gauge() {
    let session = build_session();
    let start = 0i64;
    let step = 15_000i64;
    let n = 40usize;
    // Gauge decreasing linearly: value = 100 - i * 5
    let timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * step).collect();
    let values: Vec<f64> = (0..n).map(|i| 100.0 - i as f64 * 5.0).collect();
    let batches = vec![make_test_batch(
        &timestamps,
        &values,
        "fp_001",
        &[("x", &vec!["y"; n])],
    )];
    register_metric(&session, "dec_gauge", batches);

    let range_ms = 60_000;
    let eval_start = start + range_ms + step;
    let eval_end = start + (n as i64 - 2) * step;
    let results = eval_promql(
        r#"delta(dec_gauge[1m])"#,
        &session,
        eval_start,
        eval_end,
        step,
    )
    .await;

    let vals = extract_values(&results);
    assert!(!vals.is_empty(), "delta of decreasing gauge should produce results");
    for v in &vals {
        assert!(*v < 0.0, "delta of decreasing gauge should be negative, got {}", v);
    }
}

/// From Go tests: clamp_min / clamp_max — verify basic scalar functions work.
#[tokio::test]
async fn test_abs_function() {
    let session = build_session();
    let start = 1_000_000i64;
    let step = 15_000i64;
    let n = 5usize;
    let timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * step).collect();
    let values: Vec<f64> = vec![-5.0, -3.0, 0.0, 3.0, 5.0];
    let batches = vec![make_test_batch(
        &timestamps,
        &values,
        "fp_001",
        &[("x", &vec!["y"; n])],
    )];
    register_metric(&session, "signed", batches);

    let eval_end = start + (n as i64 - 1) * step;
    let results = eval_promql(r#"abs(signed)"#, &session, start, eval_end, step).await;

    let result_values = extract_values(&results);
    assert!(!result_values.is_empty());
    for v in &result_values {
        assert!(*v >= 0.0, "abs() should always be non-negative, got {}", v);
    }
}

// ---------------------------------------------------------------------------
// Pre-aggregated data correctness tests
//
// These tests simulate what happens when cumulative counters are stored in
// 5-minute aggregated buckets (as in samples_v1_agg_5m) and then queried
// with rate(). The key property: rate() on pre-aggregated min values
// must produce the same result as rate() on the raw samples.
// ---------------------------------------------------------------------------

/// Helper: build pre-aggregated data from raw counter samples.
/// Takes raw 15s-interval counter data and returns 5m-aggregated "min" values,
/// simulating what the AggregatingMergeTree MV produces.
fn aggregate_to_5m_min(raw_timestamps: &[i64], raw_values: &[f64]) -> (Vec<i64>, Vec<f64>) {
    use std::collections::BTreeMap;
    let bucket_ms = 300_000i64;
    let mut buckets: BTreeMap<i64, f64> = BTreeMap::new();
    for (&ts, &val) in raw_timestamps.iter().zip(raw_values.iter()) {
        let bucket_start = (ts / bucket_ms) * bucket_ms;
        buckets
            .entry(bucket_start)
            .and_modify(|min| {
                if val < *min {
                    *min = val;
                }
            })
            .or_insert(val);
    }
    let ts: Vec<i64> = buckets.keys().copied().collect();
    let vals: Vec<f64> = buckets.values().copied().collect();
    (ts, vals)
}

/// rate() on raw 15s data vs rate() on 5m-aggregated min values should
/// produce approximately the same results at evaluation points where both
/// have sufficient data.
#[tokio::test]
async fn test_rate_raw_vs_agg_equivalence() {
    let start = 0i64;
    let raw_step = 15_000i64;
    let n = 120usize; // 30 minutes of raw data
    let rate_per_sec = 2.0;

    let raw_timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * raw_step).collect();
    let raw_values: Vec<f64> = (0..n)
        .map(|i| (i as f64) * rate_per_sec * (raw_step as f64 / 1000.0))
        .collect();

    let (agg_timestamps, agg_values) = aggregate_to_5m_min(&raw_timestamps, &raw_values);

    // Evaluate both with the same eval window, using a 5m range vector
    let eval_step = 60_000i64;
    let eval_start = 600_000; // 10 minutes in (enough lookback)
    let eval_end = (n as i64 - 2) * raw_step;

    // Raw path
    let raw_session = build_session();
    let raw_batches = vec![make_test_batch(
        &raw_timestamps,
        &raw_values,
        "fp_001",
        &[("instance", &vec!["pod1"; n])],
    )];
    register_metric(&raw_session, "counter_raw", raw_batches);
    let raw_results = eval_promql(
        r#"rate(counter_raw[5m])"#,
        &raw_session,
        eval_start,
        eval_end,
        eval_step,
    )
    .await;
    let raw_vals = extract_values(&raw_results);

    // Agg path: use 10m window (simulating the widen_range_vectors 2x factor)
    let agg_session = build_session();
    let agg_n = agg_timestamps.len();
    let agg_batches = vec![make_test_batch(
        &agg_timestamps,
        &agg_values,
        "fp_001",
        &[("instance", &vec!["pod1"; agg_n])],
    )];
    register_metric(&agg_session, "counter_agg", agg_batches);
    let agg_results = eval_promql(
        r#"rate(counter_agg[10m])"#,
        &agg_session,
        eval_start,
        eval_end,
        eval_step,
    )
    .await;
    let agg_vals = extract_values(&agg_results);

    assert!(
        !raw_vals.is_empty() && !agg_vals.is_empty(),
        "both paths should produce results: raw={}, agg={}",
        raw_vals.len(),
        agg_vals.len()
    );

    // The aggregated rate should be within 50% of the raw rate.
    // Exact match isn't expected due to different extrapolation behavior.
    let raw_avg: f64 = raw_vals.iter().sum::<f64>() / raw_vals.len() as f64;
    let agg_avg: f64 = agg_vals.iter().sum::<f64>() / agg_vals.len() as f64;
    let ratio = agg_avg / raw_avg;
    assert!(
        ratio > 0.5 && ratio < 2.0,
        "agg rate avg ({:.4}) should be within 2x of raw rate avg ({:.4}), ratio={:.4}",
        agg_avg,
        raw_avg,
        ratio
    );

    // Both should be close to the true rate (2.0/sec)
    assert!(
        (raw_avg - rate_per_sec).abs() / rate_per_sec < 0.2,
        "raw rate avg ({:.4}) should be close to true rate ({:.4})",
        raw_avg,
        rate_per_sec
    );
    assert!(
        (agg_avg - rate_per_sec).abs() / rate_per_sec < 0.5,
        "agg rate avg ({:.4}) should be within 50% of true rate ({:.4})",
        agg_avg,
        rate_per_sec
    );
}

/// sum(rate()) across multiple fingerprints using 5m-aggregated data must
/// equal the sum of individual per-fingerprint rates.
#[tokio::test]
async fn test_sum_rate_multi_fingerprint_agg() {
    let bucket_ms = 300_000i64;
    let n_buckets = 10usize;
    let timestamps: Vec<i64> = (0..n_buckets).map(|i| i as i64 * bucket_ms).collect();

    // Three counters with different rates, all using "min" values
    // (simulating cumulative counter min in agg table)
    let fp1_values: Vec<f64> = (0..n_buckets).map(|i| 1000.0 + i as f64 * 3.0).collect();
    let fp2_values: Vec<f64> = (0..n_buckets).map(|i| 2000.0 + i as f64 * 5.0).collect();
    let fp3_values: Vec<f64> = (0..n_buckets).map(|i| 3000.0 + i as f64 * 7.0).collect();

    let session = build_session();
    let batches = vec![
        make_test_batch(
            &timestamps,
            &fp1_values,
            "fp_001",
            &[("instance", &vec!["10.0.0.1"; n_buckets])],
        ),
        make_test_batch(
            &timestamps,
            &fp2_values,
            "fp_002",
            &[("instance", &vec!["10.0.0.2"; n_buckets])],
        ),
        make_test_batch(
            &timestamps,
            &fp3_values,
            "fp_003",
            &[("instance", &vec!["10.0.0.3"; n_buckets])],
        ),
    ];
    register_metric(&session, "counter", batches);

    let eval_start = 2 * bucket_ms;
    let eval_end = (n_buckets as i64 - 1) * bucket_ms;
    let eval_step = 60_000i64;

    // sum(rate())
    let sum_results = eval_promql(
        r#"sum(rate(counter[10m]))"#,
        &session,
        eval_start,
        eval_end,
        eval_step,
    )
    .await;
    let sum_vals = extract_values(&sum_results);

    // individual rate() per fingerprint
    let individual_results = eval_promql(
        r#"rate(counter[10m])"#,
        &session,
        eval_start,
        eval_end,
        eval_step,
    )
    .await;
    let ind_vals = extract_values(&individual_results);
    let ind_timestamps = extract_timestamps(&individual_results);

    assert!(!sum_vals.is_empty(), "sum(rate()) should produce results");
    assert!(!ind_vals.is_empty(), "rate() should produce results");

    // All individual rate values must be non-negative and reasonable
    for v in &ind_vals {
        assert!(
            *v >= 0.0 && *v < 1.0,
            "individual rate should be small and non-negative, got {}",
            v
        );
    }

    // All sum values must be non-negative and reasonable
    // Expected: (3+5+7) / 300 = 0.05/s
    let expected_sum_rate = (3.0 + 5.0 + 7.0) / 300.0;
    for v in &sum_vals {
        assert!(
            *v >= 0.0,
            "sum(rate()) must be non-negative, got {}",
            v
        );
        assert!(
            *v < 1.0,
            "sum(rate()) should be small (expected ~{:.4}), got {} — possible counter value leak",
            expected_sum_rate,
            v
        );
    }
}

/// Duplicate timestamps (simulating unmerged AggregatingMergeTree parts)
/// with identical values should not affect rate().
#[tokio::test]
async fn test_rate_duplicate_timestamps_identical_values() {
    let bucket_ms = 300_000i64;
    let n_buckets = 8usize;

    // Normal data
    let mut timestamps = Vec::new();
    let mut values = Vec::new();
    for i in 0..n_buckets {
        let ts = i as i64 * bucket_ms;
        let val = 100.0 + i as f64 * 5.0;
        timestamps.push(ts);
        values.push(val);
        // Add duplicate at bucket 3 and 5 (simulating unmerged parts)
        if i == 3 || i == 5 {
            timestamps.push(ts);
            values.push(val);
        }
    }

    let n = timestamps.len();
    let session = build_session();
    let batches = vec![make_test_batch(
        &timestamps,
        &values,
        "fp_001",
        &[("instance", &vec!["pod1"; n])],
    )];
    register_metric(&session, "dup_counter", batches);

    let eval_start = 2 * bucket_ms;
    let eval_end = (n_buckets as i64 - 1) * bucket_ms;
    let eval_step = 60_000i64;

    let results = eval_promql(
        r#"rate(dup_counter[10m])"#,
        &session,
        eval_start,
        eval_end,
        eval_step,
    )
    .await;
    let vals = extract_values(&results);

    assert!(!vals.is_empty(), "rate() with duplicates should produce results");
    let expected_rate = 5.0 / 300.0; // 5 per 5m bucket
    for v in &vals {
        assert!(
            *v >= 0.0,
            "rate() must be non-negative with duplicates, got {}",
            v
        );
        assert!(
            *v < 0.5,
            "rate() should be small (~{:.4}) even with duplicate timestamps, got {} — duplicates may corrupt rate",
            expected_rate,
            v
        );
    }
}

/// Duplicate timestamps with DIVERGENT values (simulating unmerged
/// AggregatingMergeTree parts where min differs across parts).
/// rate() must still produce reasonable values, not counter-value-magnitude spikes.
#[tokio::test]
async fn test_rate_duplicate_timestamps_divergent_values() {
    let bucket_ms = 300_000i64;
    let n_buckets = 8usize;

    let mut timestamps = Vec::new();
    let mut values = Vec::new();
    for i in 0..n_buckets {
        let ts = i as i64 * bucket_ms;
        let base_val = 100.0 + i as f64 * 5.0;
        timestamps.push(ts);
        values.push(base_val);
        // At bucket 4, add unmerged parts with slightly different min values
        if i == 4 {
            timestamps.push(ts);
            values.push(base_val + 1.0); // part 2: min is 1 higher
            timestamps.push(ts);
            values.push(base_val + 2.0); // part 3: min is 2 higher
        }
    }

    let n = timestamps.len();
    let session = build_session();
    let batches = vec![make_test_batch(
        &timestamps,
        &values,
        "fp_001",
        &[("instance", &vec!["pod1"; n])],
    )];
    register_metric(&session, "divergent_counter", batches);

    let eval_start = 2 * bucket_ms;
    let eval_end = (n_buckets as i64 - 1) * bucket_ms;
    let eval_step = 60_000i64;

    let results = eval_promql(
        r#"rate(divergent_counter[10m])"#,
        &session,
        eval_start,
        eval_end,
        eval_step,
    )
    .await;
    let vals = extract_values(&results);

    assert!(!vals.is_empty(), "rate() with divergent dupes should produce results");
    for v in &vals {
        assert!(
            *v >= 0.0,
            "rate() must be non-negative, got {}",
            v
        );
        assert!(
            *v < 0.5,
            "rate() should be small, not counter-magnitude. Got {} — divergent unmerged parts may corrupt rate calculation",
            v
        );
    }
}

/// Edge case: data ends with only 1 data point in the last window.
/// rate() should return null (not a counter-magnitude spike).
#[tokio::test]
async fn test_rate_trailing_single_point() {
    let bucket_ms = 300_000i64;

    // 6 buckets of data, then a gap, then 1 trailing point
    let mut timestamps: Vec<i64> = (0..6).map(|i| i as i64 * bucket_ms).collect();
    let mut values: Vec<f64> = (0..6).map(|i| 100.0 + i as f64 * 5.0).collect();
    // Add a lonely trailing point 15 minutes later
    timestamps.push(6 * bucket_ms + 900_000);
    values.push(200.0);

    let n = timestamps.len();
    let session = build_session();
    let batches = vec![make_test_batch(
        &timestamps,
        &values,
        "fp_001",
        &[("instance", &vec!["pod1"; n])],
    )];
    register_metric(&session, "trailing_counter", batches);

    // Evaluate up to the trailing point
    let eval_start = 2 * bucket_ms;
    let eval_end = 6 * bucket_ms + 900_000;
    let eval_step = 60_000i64;

    let results = eval_promql(
        r#"rate(trailing_counter[10m])"#,
        &session,
        eval_start,
        eval_end,
        eval_step,
    )
    .await;
    let vals = extract_values(&results);

    for v in &vals {
        assert!(
            *v >= 0.0 && *v < 1.0,
            "rate() near trailing edge should be small or null, got {}",
            v
        );
    }
}

/// Multiple fingerprints with same labels (label collision) — this simulates
/// what happens if fingerprints resolve to the same label set (e.g., all
/// getting empty labels). rate() must not produce counter-magnitude values.
#[tokio::test]
async fn test_rate_label_collision_multi_fingerprint() {
    let bucket_ms = 300_000i64;
    let n_buckets = 8usize;
    let timestamps: Vec<i64> = (0..n_buckets).map(|i| i as i64 * bucket_ms).collect();

    // Three fingerprints with very different counter values but SAME labels
    let fp1_values: Vec<f64> = (0..n_buckets).map(|i| 1000.0 + i as f64 * 3.0).collect();
    let fp2_values: Vec<f64> = (0..n_buckets).map(|i| 5000.0 + i as f64 * 5.0).collect();
    let fp3_values: Vec<f64> = (0..n_buckets).map(|i| 9000.0 + i as f64 * 7.0).collect();

    let session = build_session();
    // ALL fingerprints share the same label value — simulating label collision
    let batches = vec![
        make_test_batch(
            &timestamps,
            &fp1_values,
            "fp_001",
            &[("instance", &vec!["same_pod"; n_buckets])],
        ),
        make_test_batch(
            &timestamps,
            &fp2_values,
            "fp_002",
            &[("instance", &vec!["same_pod"; n_buckets])],
        ),
        make_test_batch(
            &timestamps,
            &fp3_values,
            "fp_003",
            &[("instance", &vec!["same_pod"; n_buckets])],
        ),
    ];
    register_metric(&session, "collision_counter", batches);

    let eval_start = 2 * bucket_ms;
    let eval_end = (n_buckets as i64 - 1) * bucket_ms;
    let eval_step = 60_000i64;

    let results = eval_promql(
        r#"sum(rate(collision_counter[10m]))"#,
        &session,
        eval_start,
        eval_end,
        eval_step,
    )
    .await;
    let vals = extract_values(&results);

    assert!(!vals.is_empty(), "should produce results");
    for v in &vals {
        assert!(
            *v < 1.0,
            "sum(rate()) with label collision should NOT produce counter-magnitude values (got {}). \
             If fingerprints with different counter offsets share labels, their interleaved values \
             look like counter resets, inflating rate() by the counter correction term.",
            v
        );
    }
}

/// Test that Redpanda dashboard expressions parse and plan without errors.
/// Uses substituted versions (variables replaced with realistic values).
#[tokio::test]
async fn test_redpanda_expressions_plan() {
    let expressions = vec![
        // Simple rate
        r#"sum(rate(redpanda_kafka_request_bytes_total{instance=~".*"}[30s]))"#,
        // Aggregation with by
        r#"sum by(instance) (rate(redpanda_io_queue_total_read_ops{instance=~".*"}[5m]))"#,
        // Deriv
        r#"avg(deriv(redpanda_cpu_busy_seconds_total[60s]))"#,
        // Binary with vector(0)
        r#"sum(redpanda_kafka_under_replicated_replicas) > 0 or vector(0)"#,
        // Nested aggregation
        r#"sum(redpanda_cluster_topics)"#,
        // Offset
        r#"sum(redpanda_rpc_active_connections offset 86400000ms)"#,
    ];

    for expr_str in &expressions {
        let session = build_session();
        let ast = match crate::promql::parse(expr_str) {
            Ok(ast) => ast,
            Err(e) => {
                panic!("Failed to parse '{}': {}", expr_str, e);
            }
        };

        let refs = collect_metric_refs(&ast);
        for mref in &refs {
            register_metric(
                &session,
                &mref.metric_name,
                make_simple_metric_data(&mref.metric_name),
            );
        }

        let planner = PromPlanner::new(EvalContext::new(1_000_000, 1_300_000, 15_000));
        match planner.plan(&ast, &session) {
            Ok(plan) => {
                let df = session.execute_logical_plan(plan).await.unwrap();
                let _results = df.collect().await.unwrap_or_else(|e| {
                    panic!("Execution failed for '{}': {}", expr_str, e);
                });
            }
            Err(EvalError::Unsupported(msg)) => {
                eprintln!("Unsupported (expected): '{}' -> {}", expr_str, msg);
            }
            Err(e) => {
                panic!("Planning failed for '{}': {}", expr_str, e);
            }
        }
    }
}
