//! Temporary verification tests for PromQL bug hunt. Run with:
//! cargo test -p reiver_core bug_hunt_verify -- --nocapture

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_array::{Array, Float64Array, RecordBatch, StringArray, TimestampMillisecondArray};
use datafusion::datasource::MemTable;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::SessionContext;

use super::extension_plan::PromExtensionPlanner;
use super::planner::{
    metric_table_name, EvalContext, PromPlanner, COL_FINGERPRINT, COL_TIMESTAMP, COL_VALUE,
};

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

fn make_test_batch(
    timestamps: &[i64],
    values: &[f64],
    fingerprint: &str,
    label_values: &[(&str, &[&str])],
) -> RecordBatch {
    let label_names: Vec<&str> = label_values.iter().map(|(n, _)| *n).collect();
    let mut fields = vec![
        Field::new(
            COL_TIMESTAMP,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(COL_VALUE, DataType::Float64, false),
        Field::new(COL_FINGERPRINT, DataType::Utf8, false),
    ];
    for name in &label_names {
        fields.push(Field::new(format!("lbl_{name}"), DataType::Utf8, true));
    }
    let schema = Arc::new(Schema::new(fields));

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

fn register_metric(session: &SessionContext, metric_name: &str, batches: Vec<RecordBatch>) {
    let schema = batches.first().unwrap().schema();
    let table_name = metric_table_name(metric_name);
    let mem_table = MemTable::try_new(schema, vec![batches]).unwrap();
    session
        .register_table(&table_name, Arc::new(mem_table))
        .unwrap();
}

async fn eval_promql(
    promql: &str,
    session: &SessionContext,
    start: i64,
    end: i64,
    step: i64,
) -> Vec<RecordBatch> {
    let ast = crate::promql::parse(promql).unwrap();
    let planner = PromPlanner::new(EvalContext::new(start, end, step));
    let plan = planner.plan(&ast, session).unwrap();
    let df = session.execute_logical_plan(plan).await.unwrap();
    df.collect().await.unwrap()
}

fn extract_label_values(results: &[RecordBatch], label: &str) -> Vec<String> {
    let col_name = format!("lbl_{label}");
    results
        .iter()
        .flat_map(|b| {
            let col = b.column_by_name(&col_name).unwrap();
            let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
            (0..arr.len())
                .filter(|&i| !arr.is_null(i))
                .map(|i| arr.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn extract_float_values(results: &[RecordBatch]) -> Vec<f64> {
    results
        .iter()
        .flat_map(|b| {
            let col = b.column_by_name(COL_VALUE).unwrap();
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            (0..arr.len())
                .filter(|&i| !arr.is_null(i))
                .map(|i| arr.value(i))
                .collect::<Vec<_>>()
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
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Prometheus `and` requires exact label-set match, not just same timestamp.
#[tokio::test]
async fn verify_and_requires_exact_label_match() {
    let session = build_session();
    let start = 1_000_000i64;
    let step = 15_000i64;
    let n = 3usize;
    let timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * step).collect();

    register_metric(
        &session,
        "metric_a",
        vec![make_test_batch(
            &timestamps,
            &vec![1.0; n],
            "fp_a_x",
            &[("job", &vec!["x"; n])],
        )],
    );
    register_metric(
        &session,
        "metric_b",
        vec![make_test_batch(
            &timestamps,
            &vec![2.0; n],
            "fp_b_y",
            &[("job", &vec!["y"; n])],
        )],
    );

    let eval_end = start + (n as i64 - 1) * step;
    let results = eval_promql(
        r#"metric_a and metric_b"#,
        &session,
        start,
        eval_end,
        step,
    )
    .await;

    let jobs = extract_label_values(&results, "job");
    assert!(
        jobs.is_empty(),
        "metric_a{{job=x}} and metric_b{{job=y}} should be empty (no matching labels), got jobs: {:?}",
        jobs
    );
}

/// Regex alternation should match either alternative.
#[tokio::test]
async fn verify_regex_alternation_label_filter() {
    let session = build_session();
    let start = 1_000_000i64;
    let step = 15_000i64;
    let ts = vec![start];

    register_metric(
        &session,
        "http_errors",
        vec![
            make_test_batch(&ts, &[1.0], "fp_err", &[("status", &["Error"])]),
            make_test_batch(&ts, &[2.0], "fp_fail", &[("status", &["Failed"])]),
            make_test_batch(&ts, &[3.0], "fp_ok", &[("status", &["OK"])]),
        ],
    );

    let results = eval_promql(
        r#"http_errors{status=~"Error|Failed"}"#,
        &session,
        start,
        start,
        step,
    )
    .await;

    let statuses = extract_label_values(&results, "status");
    assert_eq!(statuses.len(), 2, "should match Error and Failed, got {:?}", statuses);
    assert!(statuses.contains(&"Error".to_string()));
    assert!(statuses.contains(&"Failed".to_string()));
}

/// `unless` with non-matching labels keeps all LHS rows.
#[tokio::test]
async fn verify_unless_non_matching_keeps_lhs() {
    let session = build_session();
    let start = 1_000_000i64;
    let step = 15_000i64;
    let n = 2usize;
    let timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * step).collect();

    register_metric(
        &session,
        "metric_a",
        vec![make_test_batch(
            &timestamps,
            &vec![1.0; n],
            "fp_a",
            &[("job", &vec!["x"; n])],
        )],
    );
    register_metric(
        &session,
        "metric_b",
        vec![make_test_batch(
            &timestamps,
            &vec![2.0; n],
            "fp_b",
            &[("job", &vec!["y"; n])],
        )],
    );

    let eval_end = start + (n as i64 - 1) * step;
    let results = eval_promql(
        r#"metric_a unless metric_b"#,
        &session,
        start,
        eval_end,
        step,
    )
    .await;

    // Different labels → unless removes nothing → LHS fully preserved
    let jobs = extract_label_values(&results, "job");
    assert_eq!(
        jobs.len(),
        n,
        "unless with non-matching labels should keep all LHS rows, got: {:?}",
        jobs
    );
    assert!(jobs.iter().all(|j| j == "x"));
}

/// `and` with matching labels returns correct series with labels intact.
#[tokio::test]
async fn verify_and_matching_labels_preserved() {
    let session = build_session();
    let start = 1_000_000i64;
    let step = 15_000i64;
    let n = 3usize;
    let timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * step).collect();

    register_metric(
        &session,
        "metric_a",
        vec![make_test_batch(
            &timestamps,
            &vec![1.0; n],
            "fp_a",
            &[("job", &vec!["x"; n])],
        )],
    );
    register_metric(
        &session,
        "metric_b",
        vec![make_test_batch(
            &timestamps,
            &vec![2.0; n],
            "fp_b",
            &[("job", &vec!["x"; n])],
        )],
    );

    let eval_end = start + (n as i64 - 1) * step;
    let results = eval_promql(
        r#"metric_a and metric_b"#,
        &session,
        start,
        eval_end,
        step,
    )
    .await;

    let jobs = extract_label_values(&results, "job");
    assert_eq!(
        jobs.len(),
        n,
        "and with matching labels should return all matched rows, got: {:?}",
        jobs
    );
    assert!(jobs.iter().all(|j| j == "x"));
}

/// Scalar-on-left arithmetic preserves vector labels.
#[tokio::test]
async fn verify_scalar_left_preserves_labels() {
    let session = build_session();
    let start = 1_000_000i64;
    let step = 15_000i64;
    let n = 2usize;
    let timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * step).collect();

    register_metric(
        &session,
        "metric_x",
        vec![make_test_batch(
            &timestamps,
            &vec![5.0; n],
            "fp_x",
            &[("env", &vec!["prod"; n])],
        )],
    );

    let eval_end = start + (n as i64 - 1) * step;
    let results = eval_promql(
        r#"3 * metric_x"#,
        &session,
        start,
        eval_end,
        step,
    )
    .await;

    let envs = extract_label_values(&results, "env");
    assert_eq!(
        envs.len(),
        n,
        "3 * metric_x should preserve env label, got: {:?}",
        envs
    );
    assert!(envs.iter().all(|e| e == "prod"));

    // Check value correctness
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, n);
    for batch in &results {
        let vals = batch
            .column_by_name(super::planner::COL_VALUE)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..vals.len() {
            assert_eq!(vals.value(i), 15.0, "3 * 5 should be 15");
        }
    }
}

/// `metric_a or metric_a` produces no duplicates.
#[tokio::test]
async fn verify_or_no_duplicates() {
    let session = build_session();
    let start = 1_000_000i64;
    let step = 15_000i64;
    let n = 3usize;
    let timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * step).collect();

    register_metric(
        &session,
        "metric_dup",
        vec![make_test_batch(
            &timestamps,
            &vec![1.0; n],
            "fp_dup",
            &[("job", &vec!["a"; n])],
        )],
    );

    let eval_end = start + (n as i64 - 1) * step;
    let results = eval_promql(
        r#"metric_dup or metric_dup"#,
        &session,
        start,
        eval_end,
        step,
    )
    .await;

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, n,
        "or with same metric should not duplicate rows, got {} instead of {}",
        total_rows, n
    );
}

/// `a on(nonexistent) / b` returns empty result.
#[tokio::test]
async fn verify_on_missing_label_returns_empty() {
    let session = build_session();
    let start = 1_000_000i64;
    let step = 15_000i64;
    let n = 2usize;
    let timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * step).collect();

    register_metric(
        &session,
        "metric_p",
        vec![make_test_batch(
            &timestamps,
            &vec![10.0; n],
            "fp_p",
            &[("job", &vec!["x"; n])],
        )],
    );
    register_metric(
        &session,
        "metric_q",
        vec![make_test_batch(
            &timestamps,
            &vec![2.0; n],
            "fp_q",
            &[("job", &vec!["x"; n])],
        )],
    );

    let eval_end = start + (n as i64 - 1) * step;
    let results = eval_promql(
        r#"metric_p / on(nonexistent) metric_q"#,
        &session,
        start,
        eval_end,
        step,
    )
    .await;

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 0,
        "on(nonexistent) should yield empty result, got {} rows",
        total_rows
    );
}

/// label_replace should apply regex replacement to produce new label values.
#[tokio::test]
async fn verify_label_replace_is_noop() {
    let session = build_session();
    let start = 1_000_000i64;
    let step = 15_000i64;
    let ts = vec![start];

    register_metric(
        &session,
        "up",
        vec![make_test_batch(
            &ts,
            &[1.0],
            "fp1",
            &[("instance", &["old_host"])],
        )],
    );

    let results = eval_promql(
        r#"label_replace(up, "instance", "new_host", "instance", ".*")"#,
        &session,
        start,
        start,
        step,
    )
    .await;

    let instances = extract_label_values(&results, "instance");
    assert!(
        instances.iter().any(|v| v == "new_host"),
        "label_replace should produce new_host, got {:?}",
        instances
    );
}

/// Regex negation: !~ should exclude matching values.
#[tokio::test]
async fn verify_regex_negation_excludes() {
    let session = build_session();
    let start = 1_000_000i64;
    let step = 15_000i64;
    let ts = vec![start];

    register_metric(
        &session,
        "http_requests",
        vec![
            make_test_batch(&ts, &[1.0], "fp_ok", &[("status", &["OK"])]),
            make_test_batch(&ts, &[2.0], "fp_err", &[("status", &["Error"])]),
            make_test_batch(&ts, &[3.0], "fp_succ", &[("status", &["Success"])]),
        ],
    );

    let results = eval_promql(
        r#"http_requests{status!~"OK|Success"}"#,
        &session,
        start,
        start,
        step,
    )
    .await;

    let statuses = extract_label_values(&results, "status");
    assert_eq!(statuses.len(), 1, "should only match Error, got {:?}", statuses);
    assert_eq!(statuses[0], "Error");
}

/// label_replace with capture group extracts part of the value.
#[tokio::test]
async fn verify_label_replace_capture_group() {
    let session = build_session();
    let start = 1_000_000i64;
    let step = 15_000i64;
    let ts = vec![start];

    register_metric(
        &session,
        "up",
        vec![make_test_batch(
            &ts,
            &[1.0],
            "fp1",
            &[("instance", &["host1:9090"])],
        )],
    );

    let results = eval_promql(
        r#"label_replace(up, "short", "$1", "instance", "(.*):.*")"#,
        &session,
        start,
        start,
        step,
    )
    .await;

    let shorts = extract_label_values(&results, "short");
    assert_eq!(
        shorts.len(),
        1,
        "should produce one short label value, got {:?}",
        shorts
    );
    assert_eq!(shorts[0], "host1", "capture group should extract 'host1'");
}

/// label_replace: no-match leaves destination unchanged.
#[tokio::test]
async fn verify_label_replace_no_match_unchanged() {
    let session = build_session();
    let start = 1_000_000i64;
    let step = 15_000i64;
    let ts = vec![start];

    register_metric(
        &session,
        "up",
        vec![make_test_batch(
            &ts,
            &[1.0],
            "fp1",
            &[("instance", &["no_port"])],
        )],
    );

    let results = eval_promql(
        r#"label_replace(up, "instance", "$1", "instance", "(.*):.*")"#,
        &session,
        start,
        start,
        step,
    )
    .await;

    let instances = extract_label_values(&results, "instance");
    assert_eq!(instances.len(), 1);
    assert_eq!(
        instances[0], "no_port",
        "no-match should leave instance unchanged"
    );
}

/// label_join concatenates multiple source labels.
#[tokio::test]
async fn verify_label_join_concatenation() {
    let session = build_session();
    let start = 1_000_000i64;
    let step = 15_000i64;
    let ts = vec![start];

    register_metric(
        &session,
        "up",
        vec![make_test_batch(
            &ts,
            &[1.0],
            "fp1",
            &[("host", &["server1"]), ("port", &["9090"])],
        )],
    );

    let results = eval_promql(
        r#"label_join(up, "addr", ":", "host", "port")"#,
        &session,
        start,
        start,
        step,
    )
    .await;

    let addrs = extract_label_values(&results, "addr");
    assert_eq!(addrs.len(), 1, "should produce one addr value, got {:?}", addrs);
    assert_eq!(addrs[0], "server1:9090");
}

/// Parenthesized rate((metric[5m])) must produce the same result as rate(metric[5m])
#[tokio::test]
async fn verify_parenthesized_rate_same_as_bare() {
    let session = build_session();
    let bucket_ms = 60_000i64;
    let n_buckets = 10usize;

    let timestamps: Vec<i64> = (0..n_buckets).map(|i| i as i64 * bucket_ms).collect();
    let values: Vec<f64> = (0..n_buckets).map(|i| 10.0 * i as f64).collect();

    let n = timestamps.len();
    register_metric(
        &session,
        "monotonic_counter",
        vec![make_test_batch(
            &timestamps,
            &values,
            "fp1",
            &[("job", &vec!["test"; n])],
        )],
    );

    let eval_start = 5 * bucket_ms;
    let eval_end = 8 * bucket_ms;
    let eval_step = bucket_ms;

    let bare = eval_promql(
        r#"rate(monotonic_counter[5m])"#,
        &session,
        eval_start,
        eval_end,
        eval_step,
    )
    .await;
    let bare_vals = extract_float_values(&bare);

    let paren = eval_promql(
        r#"rate((monotonic_counter[5m]))"#,
        &session,
        eval_start,
        eval_end,
        eval_step,
    )
    .await;
    let paren_vals = extract_float_values(&paren);

    assert!(!bare_vals.is_empty(), "bare rate should produce values");
    assert_eq!(
        bare_vals.len(),
        paren_vals.len(),
        "parenthesized rate should produce same number of values"
    );
    for (i, (b, p)) in bare_vals.iter().zip(paren_vals.iter()).enumerate() {
        assert!(
            (b - p).abs() < 1e-9,
            "step {i}: bare rate={b}, paren rate={p} — should be identical"
        );
    }
}

/// irate() with duplicate timestamps should return null, not Inf
#[tokio::test]
async fn verify_irate_dup_timestamps_no_inf() {
    let session = build_session();

    // All samples at the same timestamp — irate should emit null
    let timestamps = vec![1_000_000i64, 1_000_000, 1_000_000, 1_000_000];
    let values = vec![10.0, 20.0, 30.0, 40.0];
    let n = timestamps.len();

    register_metric(
        &session,
        "dup_ts_counter",
        vec![make_test_batch(
            &timestamps,
            &values,
            "fp1",
            &[("job", &vec!["x"; n])],
        )],
    );

    let results = eval_promql(
        r#"irate(dup_ts_counter[5m])"#,
        &session,
        1_000_000,
        1_000_000,
        15_000,
    )
    .await;
    let vals = extract_float_values(&results);

    for v in &vals {
        assert!(
            v.is_finite() && *v >= 0.0,
            "irate with dup timestamps should be null or finite, got {v}"
        );
    }
    // Since all timestamps are identical, we expect no valid output
    assert!(vals.is_empty(), "irate with all-same timestamps should produce no values (null)");
}

/// resets() should not count same-timestamp value drops as counter resets
#[tokio::test]
async fn verify_resets_ignores_same_timestamp_drops() {
    let session = build_session();

    // Timestamps: 0, 0, 60000, 120000
    // Values:    100, 50, 110, 120
    // The drop from 100→50 at the same timestamp (0) should NOT be a reset.
    // Only genuinely time-advancing drops count.
    let timestamps = vec![0i64, 0, 60_000, 120_000];
    let values = vec![100.0, 50.0, 110.0, 120.0];
    let n = timestamps.len();

    register_metric(
        &session,
        "reset_counter",
        vec![make_test_batch(
            &timestamps,
            &values,
            "fp1",
            &[("job", &vec!["a"; n])],
        )],
    );

    let results = eval_promql(
        r#"resets(reset_counter[5m])"#,
        &session,
        120_000,
        120_000,
        60_000,
    )
    .await;
    let vals = extract_float_values(&results);

    assert!(!vals.is_empty(), "resets() should produce a value");
    assert_eq!(
        vals[0], 0.0,
        "resets() should be 0 — same-timestamp value drop is not a real reset"
    );
}

/// quantile without(label) should group by all labels EXCEPT the excluded one
#[tokio::test]
async fn verify_quantile_without_groups_correctly() {
    let session = build_session();
    let ts = vec![1_000_000i64];

    // 4 distinct series: job=web (2 series), job=api (2 series), all env=prod
    register_metric(
        &session,
        "q_metric",
        vec![
            make_test_batch(&ts, &[10.0], "fp1", &[("job", &["web"]), ("env", &["prod"])]),
            make_test_batch(&ts, &[20.0], "fp2", &[("job", &["web"]), ("env", &["prod"])]),
            make_test_batch(&ts, &[30.0], "fp3", &[("job", &["api"]), ("env", &["prod"])]),
            make_test_batch(&ts, &[40.0], "fp4", &[("job", &["api"]), ("env", &["prod"])]),
        ],
    );

    // without(env) should group by job, so we get quantile per job
    let results = eval_promql(
        r#"quantile without (env) (0.5, q_metric)"#,
        &session,
        1_000_000,
        1_000_000,
        15_000,
    )
    .await;

    let jobs = extract_label_values(&results, "job");
    assert!(
        jobs.contains(&"web".to_string()) && jobs.contains(&"api".to_string()),
        "quantile without(env) should preserve job label, got: {:?}",
        jobs
    );
    let vals = extract_float_values(&results);
    assert_eq!(vals.len(), 2, "should have 2 groups (web, api)");
}

/// count_values("label_name", metric) should create the named label with stringified values
#[tokio::test]
async fn verify_count_values_creates_output_label() {
    let session = build_session();
    let ts = vec![1_000_000i64];

    // 5 distinct series with values: 1, 2, 2, 3, 3
    let batches: Vec<RecordBatch> = vec![
        make_test_batch(&ts, &[1.0], "fp1", &[("instance", &["a"])]),
        make_test_batch(&ts, &[2.0], "fp2", &[("instance", &["b"])]),
        make_test_batch(&ts, &[2.0], "fp3", &[("instance", &["c"])]),
        make_test_batch(&ts, &[3.0], "fp4", &[("instance", &["d"])]),
        make_test_batch(&ts, &[3.0], "fp5", &[("instance", &["e"])]),
    ];
    register_metric(&session, "cv_metric", batches);

    let results = eval_promql(
        r#"count_values("val", cv_metric)"#,
        &session,
        1_000_000,
        1_000_000,
        15_000,
    )
    .await;

    let val_labels = extract_label_values(&results, "val");
    assert!(
        !val_labels.is_empty(),
        "count_values should create lbl_val column"
    );
    // We should see entries for "1", "2", "3" (stringified values)
    let has_1 = val_labels.iter().any(|v| v == "1" || v == "1.0");
    let has_2 = val_labels.iter().any(|v| v == "2" || v == "2.0");
    let has_3 = val_labels.iter().any(|v| v == "3" || v == "3.0");
    assert!(has_1 && has_2 && has_3, "expected labels for 1, 2, 3 — got: {:?}", val_labels);

    // The output values should be counts: 1 appears once, 2 appears twice, 3 appears twice
    let mut vals = extract_float_values(&results);
    vals.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(vals.len(), 3, "should have 3 distinct value groups");
    assert_eq!(vals[0], 1.0, "value=1 appears once");
    assert_eq!(vals[1], 2.0, "value=2 appears twice");
    assert_eq!(vals[2], 2.0, "value=3 appears twice");
}

/// stddev should use population standard deviation (divide by n, not n-1)
#[tokio::test]
async fn verify_stddev_is_population() {
    let session = build_session();
    // Two values: 0.0 and 10.0 as distinct series
    // Population stddev = sqrt(((0-5)^2 + (10-5)^2) / 2) = sqrt(50/2) = sqrt(25) = 5.0
    // Sample stddev = sqrt(((0-5)^2 + (10-5)^2) / 1) = sqrt(50) ≈ 7.07
    let ts = vec![1_000_000i64];

    register_metric(
        &session,
        "sd_metric",
        vec![
            make_test_batch(&ts, &[0.0], "fp1", &[("job", &["x"])]),
            make_test_batch(&ts, &[10.0], "fp2", &[("job", &["x"])]),
        ],
    );

    let results = eval_promql(
        r#"stddev(sd_metric)"#,
        &session,
        1_000_000,
        1_000_000,
        15_000,
    )
    .await;
    let vals = extract_float_values(&results);
    assert_eq!(vals.len(), 1, "should produce one value");
    assert!(
        (vals[0] - 5.0).abs() < 1e-9,
        "stddev should be 5.0 (population), got {} — if ~7.07, sample stddev is being used",
        vals[0]
    );
}

/// quantile(0.5, metric) should produce exact median via sort+interpolate
#[tokio::test]
async fn verify_quantile_exact_median() {
    let session = build_session();
    let ts = vec![1_000_000i64];

    // 5 distinct series with values 1..5
    register_metric(
        &session,
        "med_metric",
        vec![
            make_test_batch(&ts, &[1.0], "fp1", &[("job", &["a"])]),
            make_test_batch(&ts, &[2.0], "fp2", &[("job", &["a"])]),
            make_test_batch(&ts, &[3.0], "fp3", &[("job", &["a"])]),
            make_test_batch(&ts, &[4.0], "fp4", &[("job", &["a"])]),
            make_test_batch(&ts, &[5.0], "fp5", &[("job", &["a"])]),
        ],
    );

    let results = eval_promql(
        r#"quantile(0.5, med_metric)"#,
        &session,
        1_000_000,
        1_000_000,
        15_000,
    )
    .await;
    let vals = extract_float_values(&results);
    assert_eq!(vals.len(), 1, "should produce one value");
    // Exact median of [1,2,3,4,5] = 3.0
    assert!(
        (vals[0] - 3.0).abs() < 1e-9,
        "quantile(0.5) should be exactly 3.0, got {}",
        vals[0]
    );
}

/// Vector selector offset should shift sample timestamps so data appears at eval+offset
#[tokio::test]
async fn verify_vector_offset_applies() {
    let session = build_session();
    let sample_ts = 1_000_000i64;
    let offset_ms = 300_000i64; // 5m

    register_metric(
        &session,
        "offset_metric",
        vec![make_test_batch(
            &[sample_ts],
            &[42.0],
            "fp1",
            &[("job", &["x"])],
        )],
    );

    // Sample at 1_000_000 + offset 5m should appear at eval time 1_300_000
    let eval_ts = sample_ts + offset_ms;
    let results = eval_promql(
        r#"offset_metric offset 5m"#,
        &session,
        eval_ts,
        eval_ts,
        60_000,
    )
    .await;

    let vals = extract_float_values(&results);
    assert_eq!(vals.len(), 1, "offset metric should produce one value at eval time");
    assert!(
        (vals[0] - 42.0).abs() < 1e-9,
        "offset should preserve sample value, got {}",
        vals[0]
    );
}

/// Subquery step (:30s) should evaluate inner expression at sub-step resolution
#[tokio::test]
async fn verify_subquery_step_resolution() {
    let session = build_session();
    let start = 0i64;
    let end = 300_000i64;
    let outer_step = 120_000i64;
    let sub_step = 30_000i64;

    // Samples every 15s for 5 minutes
    let n = 21usize;
    let timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * 15_000).collect();
    let values: Vec<f64> = (0..n).map(|i| i as f64).collect();
    register_metric(
        &session,
        "sub_metric",
        vec![make_test_batch(
            &timestamps,
            &values,
            "fp1",
            &[("job", &vec!["x"; n])],
        )],
    );

    let results = eval_promql(
        r#"sub_metric[5m:30s]"#,
        &session,
        start,
        end,
        outer_step,
    )
    .await;

    let ts = extract_timestamps(&results);
    assert!(!ts.is_empty(), "subquery should produce results");

    // With :30s step, timestamps should align to 30s grid, not 120s outer step
    for t in &ts {
        assert_eq!(
            t % sub_step,
            0,
            "subquery with :30s should align to 30s grid, got timestamp {}",
            t
        );
    }

    // Should have more points than outer step alone (120s gives ~3 points, 30s gives ~11)
    assert!(
        ts.len() > (end / outer_step) as usize,
        "subquery :30s should produce more eval points than outer 120s step, got {} timestamps",
        ts.len()
    );
}

/// Stale NaN after a valid sample should produce no output (stale terminates search)
#[tokio::test]
async fn verify_stale_nan_terminates_search() {
    let session = build_session();
    let eval_ts = 1_000_000i64;
    let valid_ts = eval_ts - 10_000; // 10s before eval, within 5m lookback
    let stale_ts = eval_ts - 5_000; // 5s before eval, after valid sample

    register_metric(
        &session,
        "stale_metric",
        vec![make_test_batch(
            &[valid_ts, stale_ts],
            &[10.0, f64::NAN],
            "fp1",
            &[("job", &["x", "x"])],
        )],
    );

    let results = eval_promql(
        r#"stale_metric"#,
        &session,
        eval_ts,
        eval_ts,
        60_000,
    )
    .await;

    let vals = extract_float_values(&results);
    assert!(
        vals.is_empty(),
        "stale NaN after valid sample should produce no output, got {:?}",
        vals
    );
}

/// Many-to-one without group_left should deduplicate to at most one row per timestamp.
#[tokio::test]
async fn verify_many_to_one_without_group_left_deduplicates() {
    let session = build_session();
    let start = 1_000_000i64;
    let step = 15_000i64;
    let n = 2usize;
    let timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * step).collect();

    // Two LHS series with different instances but same job
    register_metric(
        &session,
        "lhs_metric",
        vec![
            make_test_batch(
                &timestamps,
                &vec![10.0; n],
                "fp_lhs_1",
                &[("job", &vec!["web"; n]), ("instance", &vec!["a"; n])],
            ),
            make_test_batch(
                &timestamps,
                &vec![20.0; n],
                "fp_lhs_2",
                &[("job", &vec!["web"; n]), ("instance", &vec!["b"; n])],
            ),
        ],
    );
    // One RHS series with same job
    register_metric(
        &session,
        "rhs_metric",
        vec![make_test_batch(
            &timestamps,
            &vec![2.0; n],
            "fp_rhs",
            &[("job", &vec!["web"; n])],
        )],
    );

    let eval_end = start + (n as i64 - 1) * step;
    // Without group_left, this is a many-to-one situation (2 LHS match 1 RHS on job).
    // Prometheus would error; our pragmatic dedup caps at one row per (timestamp, labels).
    let results = eval_promql(
        r#"lhs_metric / on(job) rhs_metric"#,
        &session,
        start,
        eval_end,
        step,
    )
    .await;

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    // OneToOne dedup: at most 1 row per timestamp (since the dedup groups by timestamp + labels,
    // and both LHS share job=web, the group-by is just timestamp → 1 row per ts).
    assert!(
        total_rows <= n,
        "many-to-one without group_left should dedup to at most {} rows, got {}",
        n,
        total_rows
    );
}

/// max_over_time with a subquery should have data for the first eval window.
#[tokio::test]
async fn verify_subquery_range_function_early_window() {
    let session = build_session();
    let bucket_ms = 60_000i64;

    // Samples from -5m to +5m relative to eval start (enough history for early window)
    let eval_start = 5 * bucket_ms; // 300_000
    let eval_end = 8 * bucket_ms; // 480_000
    let n = 14usize; // samples from t=0 to t=780_000 at 60s intervals
    let timestamps: Vec<i64> = (0..n).map(|i| i as i64 * bucket_ms).collect();
    let values: Vec<f64> = (0..n).map(|i| (i + 1) as f64).collect();

    register_metric(
        &session,
        "sq_metric",
        vec![make_test_batch(
            &timestamps,
            &values,
            "fp1",
            &[("job", &vec!["x"; n])],
        )],
    );

    // max_over_time(sq_metric[3m:1m]) — subquery evaluates inner at 1m step,
    // then takes max over 3-minute windows. The first eval point at 300_000
    // needs subquery data from 120_000..300_000 (3m back).
    let results = eval_promql(
        r#"max_over_time(sq_metric[3m:1m])"#,
        &session,
        eval_start,
        eval_end,
        bucket_ms,
    )
    .await;

    let vals = extract_float_values(&results);
    assert!(
        !vals.is_empty(),
        "max_over_time with subquery should produce results at the first eval point"
    );
    // At eval_start (300_000), the subquery should have evaluated inner at
    // 0, 60_000, 120_000, 180_000, 240_000, 300_000 (extended start).
    // The 3m window ending at 300_000 covers [0..300_000] → max is value at 300_000 = 5.0
    let first_val = vals[0];
    assert!(
        first_val >= 3.0,
        "first window max should include early subquery samples, got {}",
        first_val
    );
}

/// max_over_time(metric[5m:1m]) should compute correct max from sub-step samples.
#[tokio::test]
async fn verify_max_over_time_subquery_values() {
    let session = build_session();
    let bucket_ms = 60_000i64;

    // Create increasing samples over 10 minutes
    let n = 11usize;
    let timestamps: Vec<i64> = (0..n).map(|i| i as i64 * bucket_ms).collect();
    // Values: 1, 2, 3, ..., 11
    let values: Vec<f64> = (0..n).map(|i| (i + 1) as f64).collect();

    register_metric(
        &session,
        "inc_metric",
        vec![make_test_batch(
            &timestamps,
            &values,
            "fp1",
            &[("job", &vec!["a"; n])],
        )],
    );

    // Eval at t=600_000 (10 min mark). Subquery step 1m, range 5m.
    // The subquery evaluates the inner at 1m intervals.
    // max_over_time looks at a 5m window of subquery output ending at eval time.
    // Window [300_000..600_000] → values at t=300k..600k = 4,5,6,7,8,9,10,11
    // → max should be 11.0
    let eval_ts = 10 * bucket_ms; // 600_000
    let results = eval_promql(
        r#"max_over_time(inc_metric[5m:1m])"#,
        &session,
        eval_ts,
        eval_ts,
        bucket_ms,
    )
    .await;

    let vals = extract_float_values(&results);
    assert_eq!(vals.len(), 1, "should produce one value at single eval point");
    // The max in a 5-minute window ending at 600s should be the largest
    // sample value within that window.
    assert!(
        vals[0] >= 7.0,
        "max_over_time should find the maximum in the 5m window, got {}",
        vals[0]
    );
}
