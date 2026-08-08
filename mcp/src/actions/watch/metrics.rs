use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::registry::ActionRegistry;

// ── List Metric Names ───────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListMetricNamesInput {
    /// Filter metric names by prefix (e.g. "http." to find all HTTP metrics)
    pub prefix: Option<String>,
    /// Maximum number of results to return (default: 100)
    #[schemars(range(min = 1, max = 1000))]
    pub limit: Option<u32>,
}

#[derive(Serialize)]
pub struct ListMetricNamesOutput {
    pub metrics: serde_json::Value,
}

pub struct ListMetricNames;

#[async_trait]
impl PlatformAction for ListMetricNames {
    type Input = ListMetricNamesInput;
    type Output = ListMetricNamesOutput;

    fn name(&self) -> &'static str {
        "list_metric_names"
    }
    fn description(&self) -> &'static str {
        "List available OpenTelemetry metric names for the current project. Returns each \
         metric's name, type (gauge/counter/histogram), unit, series count, \
         and last seen timestamp. Use prefix to narrow results (e.g. \"http.\"). \
         Use the returned names with the otel_metrics analysis to query time series data."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let mut path = format!("/api/projects/{}/metrics/names", ctx.project_id);
        let mut params = vec![];
        if let Some(ref p) = input.prefix {
            params.push(format!("prefix={}", urlencoding::encode(p)));
        }
        if let Some(l) = input.limit {
            params.push(format!("limit={l}"));
        }
        if !params.is_empty() {
            path.push_str(&format!("?{}", params.join("&")));
        }

        let resp = ctx.http.watch_get(&path).await?;
        let mut body: serde_json::Value = resp.json().await?;
        if let Some(arr) = body.get_mut("metrics").and_then(|v| v.as_array_mut()) {
            for item in arr.iter_mut() {
                if let Some(obj) = item.as_object_mut() {
                    obj.remove("label_keys");
                }
            }
        } else if let Some(arr) = body.as_array_mut() {
            for item in arr.iter_mut() {
                if let Some(obj) = item.as_object_mut() {
                    obj.remove("label_keys");
                }
            }
        }
        Ok(ListMetricNamesOutput { metrics: body })
    }
}

// ── Query Metrics ───────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct QueryMetricsInput {
    /// Name of the metric to query (e.g. "http.server.duration"). Use list metric_names to discover available metrics.
    pub metric_name: String,
    /// Start of the time range (ISO 8601 timestamp, e.g. "2024-01-15T00:00:00Z")
    pub from: Option<String>,
    /// End of the time range (ISO 8601 timestamp)
    pub to: Option<String>,
    /// Aggregation interval in seconds (default: 60)
    #[schemars(range(min = 1, max = 86400))]
    pub step: Option<u32>,
    /// Time aggregation function: avg, sum, min, max, count, p50, p95, p99 (default: "avg")
    pub time_aggregation: Option<String>,
    /// Space aggregation function: avg, sum, min, max, count (default: "avg")
    pub space_aggregation: Option<String>,
    /// Key-value attribute filters (e.g. {"service.name": "api"})
    pub filters: Option<std::collections::BTreeMap<String, String>>,
    /// Dimensions to group results by (e.g. ["service.name", "http.method"])
    pub group_by: Option<Vec<String>>,
}

#[derive(Serialize)]
pub struct QueryMetricsOutput {
    pub metrics: serde_json::Value,
}

pub struct QueryMetrics;

#[async_trait]
impl PlatformAction for QueryMetrics {
    type Input = QueryMetricsInput;
    type Output = QueryMetricsOutput;

    fn name(&self) -> &'static str {
        "query_metrics"
    }
    fn description(&self) -> &'static str {
        "Query OpenTelemetry metrics by name with time range, aggregation, and grouping. \
         Use list metric_names first to discover available metrics. Returns time series \
         data points. Supports time/space aggregation functions and attribute-based filtering."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let mut body = serde_json::json!({
            "project_id": ctx.project_id,
            "metric_name": input.metric_name,
        });
        let obj = body.as_object_mut().unwrap();
        if let Some(ref f) = input.from {
            obj.insert("start".into(), serde_json::json!(f));
        }
        if let Some(ref t) = input.to {
            obj.insert("end".into(), serde_json::json!(t));
        }
        if let Some(s) = input.step {
            obj.insert("step".into(), serde_json::json!(s));
        }
        if let Some(ref ta) = input.time_aggregation {
            obj.insert("time_aggregation".into(), serde_json::json!(ta));
        }
        if let Some(ref sa) = input.space_aggregation {
            obj.insert("space_aggregation".into(), serde_json::json!(sa));
        }
        if let Some(ref f) = input.filters {
            obj.insert("filters".into(), serde_json::json!(f));
        }
        if let Some(ref g) = input.group_by {
            obj.insert("group_by".into(), serde_json::json!(g));
        }

        let resp = ctx
            .http
            .watch_post("/api/query/metrics/query", &body)
            .await?;
        let metrics = resp.json().await?;
        Ok(QueryMetricsOutput { metrics })
    }
}

// ── Registration ─────────────────────────────────────────────────────

pub fn register(registry: &mut ActionRegistry) {
    registry.register(ListMetricNames);
    registry.register(QueryMetrics);
}
