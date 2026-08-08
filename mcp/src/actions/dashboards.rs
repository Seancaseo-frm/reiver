use async_trait::async_trait;
use futures::future::join_all;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::actions::types::{default_time_range, PromQLQueryConfig, TimeRange};
use crate::registry::ActionRegistry;

// ── List Dashboards ─────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListDashboardsInput {}

#[derive(Serialize)]
pub struct ListDashboardsOutput {
    pub dashboards: serde_json::Value,
}

pub struct ListDashboards;

#[async_trait]
impl PlatformAction for ListDashboards {
    type Input = ListDashboardsInput;
    type Output = ListDashboardsOutput;

    fn name(&self) -> &'static str {
        "list_dashboards"
    }
    fn description(&self) -> &'static str {
        "List all dashboards for the current project. Returns each dashboard's ID, name, \
         description, and widget count. Use get_dashboard to retrieve the full layout and widgets."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        _input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let resp = ctx
            .http
            .website_get(&format!("/api/dashboards/{pid}/dashboards"))
            .await?;
        let dashboards = resp.json().await?;
        Ok(ListDashboardsOutput { dashboards })
    }
}

// ── Get Dashboard ───────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct GetDashboardInput {
    /// ID of the dashboard to retrieve
    pub dashboard_id: String,
}

#[derive(Serialize)]
pub struct GetDashboardOutput {
    pub dashboard: serde_json::Value,
}

pub struct GetDashboard;

#[async_trait]
impl PlatformAction for GetDashboard {
    type Input = GetDashboardInput;
    type Output = GetDashboardOutput;

    fn name(&self) -> &'static str {
        "get_dashboard"
    }
    fn description(&self) -> &'static str {
        "Get a specific dashboard by ID, including its full layout and all widget definitions. \
         Each widget contains a query config that can be executed with query_widget."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let resp = ctx
            .http
            .website_get(&format!(
                "/api/dashboards/{pid}/dashboards/{}",
                input.dashboard_id
            ))
            .await?;
        let dashboard = resp.json().await?;
        Ok(GetDashboardOutput { dashboard })
    }
}

// ── List Dashboard Templates ────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListDashboardTemplatesInput {}

#[derive(Serialize)]
pub struct ListDashboardTemplatesOutput {
    pub templates: serde_json::Value,
}

pub struct ListDashboardTemplates;

#[async_trait]
impl PlatformAction for ListDashboardTemplates {
    type Input = ListDashboardTemplatesInput;
    type Output = ListDashboardTemplatesOutput;

    fn name(&self) -> &'static str {
        "list_dashboard_templates"
    }
    fn description(&self) -> &'static str {
        "List pre-built dashboard templates (e.g. HTTP Overview, Database Metrics, LLM Gateway). \
         Use create_dashboard_from_template with a template ID to instantiate one."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        _input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let resp = ctx
            .http
            .website_get("/api/dashboards/dashboard-templates")
            .await?;
        let templates = resp.json().await?;
        Ok(ListDashboardTemplatesOutput { templates })
    }
}

// ── Create Dashboard ────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct CreateDashboardInput {
    /// Name for the new dashboard
    pub name: String,
    /// What this dashboard monitors or visualizes
    pub description: Option<String>,
}

#[derive(Serialize)]
pub struct CreateDashboardOutput {
    pub dashboard: serde_json::Value,
}

pub struct CreateDashboard;

#[async_trait]
impl PlatformAction for CreateDashboard {
    type Input = CreateDashboardInput;
    type Output = CreateDashboardOutput;

    fn name(&self) -> &'static str {
        "create_dashboard"
    }
    fn description(&self) -> &'static str {
        "Create a new empty dashboard in the current project. The dashboard starts with no \
         widgets — add them through the UI or use query_widget to test queries before adding. \
         For a pre-populated dashboard, use create_dashboard_from_template instead."
    }
    fn required_scope(&self) -> String {
        "observability:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let body = serde_json::json!({
            "name": input.name,
            "description": input.description,
        });
        let resp = ctx
            .http
            .website_post(&format!("/api/dashboards/{pid}/dashboards"), &body)
            .await?;
        let dashboard = resp.json().await?;
        Ok(CreateDashboardOutput { dashboard })
    }
}

// ── Create Dashboard From Template ──────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct CreateDashboardFromTemplateInput {
    /// ID of the template to instantiate (from list_dashboard_templates)
    pub template_id: String,
}

#[derive(Serialize)]
pub struct CreateDashboardFromTemplateOutput {
    pub dashboard: serde_json::Value,
}

pub struct CreateDashboardFromTemplate;

#[async_trait]
impl PlatformAction for CreateDashboardFromTemplate {
    type Input = CreateDashboardFromTemplateInput;
    type Output = CreateDashboardFromTemplateOutput;

    fn name(&self) -> &'static str {
        "create_dashboard_from_template"
    }
    fn description(&self) -> &'static str {
        "Create a new dashboard from a pre-built template. The template provides a ready-made \
         layout with widgets and queries. Use list_dashboard_templates to see available options."
    }
    fn required_scope(&self) -> String {
        "observability:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let body = serde_json::json!({ "template_id": input.template_id });
        let resp = ctx
            .http
            .website_post(
                &format!("/api/dashboards/{pid}/dashboards/from-template"),
                &body,
            )
            .await?;
        let dashboard = resp.json().await?;
        Ok(CreateDashboardFromTemplateOutput { dashboard })
    }
}

// ── Query Widget ────────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct QueryWidgetInput {
    /// PromQL query configuration
    pub query: PromQLQueryConfig,
    /// Time range for the query (defaults to last 1 hour if omitted)
    #[serde(default = "default_time_range")]
    pub time_range: TimeRange,
    /// Optional dashboard variables for parameterized queries (e.g. {"service": "api-gateway"})
    #[serde(default)]
    pub variables: Option<std::collections::BTreeMap<String, serde_json::Value>>,
}

#[derive(Serialize)]
pub struct QueryWidgetOutput {
    pub result: serde_json::Value,
}

pub struct QueryWidget;

#[async_trait]
impl PlatformAction for QueryWidget {
    type Input = QueryWidgetInput;
    type Output = QueryWidgetOutput;

    fn name(&self) -> &'static str {
        "query_widget"
    }
    fn description(&self) -> &'static str {
        "Execute a PromQL query against the project's OpenTelemetry metrics. Use this to \
         test queries before creating dashboard widgets, or for ad-hoc metric analysis. \
         Discover available metric names first with `list` resource 'metric_names'. \
         Common patterns: rate(http_requests_total[5m]) for throughput, \
         histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m])) for \
         latency percentiles, sum(rate(http_requests_total{status=~\"5..\"}[5m])) for \
         error rates. time_range defaults to the last 1 hour if omitted."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let body = serde_json::json!({
            "query": input.query,
            "time_range": input.time_range,
            "variables": input.variables,
        });
        let resp = ctx
            .http
            .watch_post(&format!("/api/{pid}/widget-query"), &body)
            .await?;
        let result = resp.json().await?;
        Ok(QueryWidgetOutput { result })
    }
}

// ── Update Dashboard ────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct UpdateDashboardInput {
    /// ID of the dashboard to update
    pub dashboard_id: String,
    /// New name for the dashboard
    pub name: Option<String>,
    /// New description for the dashboard
    pub description: Option<String>,
}

#[derive(Serialize)]
pub struct UpdateDashboardOutput {
    pub dashboard: serde_json::Value,
}

pub struct UpdateDashboard;

#[async_trait]
impl PlatformAction for UpdateDashboard {
    type Input = UpdateDashboardInput;
    type Output = UpdateDashboardOutput;

    fn name(&self) -> &'static str {
        "update_dashboard"
    }
    fn description(&self) -> &'static str {
        "Update a dashboard's name or description."
    }
    fn required_scope(&self) -> String {
        "observability:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let mut body = serde_json::Map::new();
        if let Some(n) = input.name {
            body.insert("name".into(), serde_json::Value::String(n));
        }
        if let Some(d) = input.description {
            body.insert("description".into(), serde_json::Value::String(d));
        }
        let resp = ctx
            .http
            .website_put(
                &format!("/api/dashboards/{pid}/dashboards/{}", input.dashboard_id),
                &serde_json::Value::Object(body),
            )
            .await?;
        let dashboard = resp.json().await?;
        Ok(UpdateDashboardOutput { dashboard })
    }
}

// ── List Widgets ────────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ListWidgetsInput {
    /// Dashboard ID to list widgets for
    pub dashboard_id: String,
}

#[derive(Serialize)]
pub struct ListWidgetsOutput {
    pub widgets: serde_json::Value,
}

pub struct ListWidgets;

#[async_trait]
impl PlatformAction for ListWidgets {
    type Input = ListWidgetsInput;
    type Output = ListWidgetsOutput;

    fn name(&self) -> &'static str {
        "list_widgets"
    }
    fn description(&self) -> &'static str {
        "List all widgets on a dashboard. Returns each widget's ID, title, type, \
         position, and query configuration."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let resp = ctx
            .http
            .website_get(&format!(
                "/api/dashboards/{pid}/dashboards/{}/widgets",
                input.dashboard_id
            ))
            .await?;
        let widgets = resp.json().await?;
        Ok(ListWidgetsOutput { widgets })
    }
}

// ── Create Widget ───────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct CreateWidgetInput {
    /// Dashboard ID to add the widget to
    pub dashboard_id: String,
    /// Widget title
    pub title: String,
    /// Widget type: "timeseries" for trends over time, "stat" for a single current value, "table" for breakdowns, "bar" or "pie" for comparisons, "heatmap" for density, "top_list" for rankings
    pub widget_type: String,
    /// PromQL query configuration for the widget data
    pub query: PromQLQueryConfig,
    /// Grid position — column (0-based)
    #[serde(default)]
    pub x: Option<u32>,
    /// Grid position — row (0-based)
    #[serde(default)]
    pub y: Option<u32>,
    /// Widget width in grid units (default: 6)
    #[serde(default)]
    pub w: Option<u32>,
    /// Widget height in grid units (default: 4)
    #[serde(default)]
    pub h: Option<u32>,
}

#[derive(Serialize)]
pub struct CreateWidgetOutput {
    pub widget: serde_json::Value,
}

pub struct CreateWidget;

#[async_trait]
impl PlatformAction for CreateWidget {
    type Input = CreateWidgetInput;
    type Output = CreateWidgetOutput;

    fn name(&self) -> &'static str {
        "create_widget"
    }
    fn description(&self) -> &'static str {
        "Add a PromQL-powered widget to a dashboard. Always test the query with \
         widget_query first to verify it returns data. Choose the right widget_type \
         for the data: 'timeseries' for trends over time, 'stat' for a single current \
         value, 'table' for breakdowns, 'bar' or 'pie' for comparisons. Use legend_format \
         to label series clearly (e.g. '{{method}} {{status}}')."
    }
    fn required_scope(&self) -> String {
        "observability:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let body = serde_json::json!({
            "title": input.title,
            "widget_type": input.widget_type,
            "widget_config": { "query": input.query },
            "position_x": input.x.unwrap_or(0),
            "position_y": input.y.unwrap_or(0),
            "width": input.w.unwrap_or(6),
            "height": input.h.unwrap_or(4),
        });
        let resp = ctx
            .http
            .website_post(
                &format!(
                    "/api/dashboards/{pid}/dashboards/{}/widgets",
                    input.dashboard_id
                ),
                &body,
            )
            .await?;
        let widget = resp.json().await?;
        Ok(CreateWidgetOutput { widget })
    }
}

// ── Update Widget ───────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct UpdateWidgetInput {
    /// Dashboard ID containing the widget
    pub dashboard_id: String,
    /// Widget ID to update
    pub widget_id: String,
    /// Updated widget title
    pub title: Option<String>,
    /// Updated widget type
    pub widget_type: Option<String>,
    /// Updated PromQL query configuration
    pub query: Option<PromQLQueryConfig>,
    /// Updated grid position — column
    pub x: Option<u32>,
    /// Updated grid position — row
    pub y: Option<u32>,
    /// Updated widget width
    pub w: Option<u32>,
    /// Updated widget height
    pub h: Option<u32>,
}

#[derive(Serialize)]
pub struct UpdateWidgetOutput {
    pub widget: serde_json::Value,
}

pub struct UpdateWidget;

#[async_trait]
impl PlatformAction for UpdateWidget {
    type Input = UpdateWidgetInput;
    type Output = UpdateWidgetOutput;

    fn name(&self) -> &'static str {
        "update_widget"
    }
    fn description(&self) -> &'static str {
        "Update an existing dashboard widget's title, chart type, PromQL query, or grid \
         position. Test the new query with widget_query before applying to make sure it \
         returns expected data."
    }
    fn required_scope(&self) -> String {
        "observability:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let mut body = serde_json::Map::new();
        if let Some(t) = input.title {
            body.insert("title".into(), serde_json::Value::String(t));
        }
        if let Some(wt) = input.widget_type {
            body.insert("widget_type".into(), serde_json::Value::String(wt));
        }
        if let Some(q) = input.query {
            body.insert(
                "widget_config".into(),
                serde_json::json!({ "query": serde_json::to_value(q)? }),
            );
        }
        if let Some(x) = input.x {
            body.insert("position_x".into(), serde_json::json!(x));
        }
        if let Some(y) = input.y {
            body.insert("position_y".into(), serde_json::json!(y));
        }
        if let Some(w) = input.w {
            body.insert("width".into(), serde_json::json!(w));
        }
        if let Some(h) = input.h {
            body.insert("height".into(), serde_json::json!(h));
        }
        let resp = ctx
            .http
            .website_put(
                &format!(
                    "/api/dashboards/{pid}/dashboards/{}/widgets/{}",
                    input.dashboard_id, input.widget_id
                ),
                &serde_json::Value::Object(body),
            )
            .await?;
        let widget = resp.json().await?;
        Ok(UpdateWidgetOutput { widget })
    }
}

// ── Delete Widget ───────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct DeleteWidgetInput {
    /// Dashboard ID containing the widget
    pub dashboard_id: String,
    /// Widget ID to delete
    pub widget_id: String,
}

#[derive(Serialize)]
pub struct DeleteWidgetOutput {
    pub success: bool,
}

pub struct DeleteWidget;

#[async_trait]
impl PlatformAction for DeleteWidget {
    type Input = DeleteWidgetInput;
    type Output = DeleteWidgetOutput;

    fn name(&self) -> &'static str {
        "delete_widget"
    }
    fn description(&self) -> &'static str {
        "Remove a widget from a dashboard. This only removes the chart — it does not \
         delete the underlying data."
    }
    fn required_scope(&self) -> String {
        "observability:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        ctx.http
            .website_delete(&format!(
                "/api/dashboards/{pid}/dashboards/{}/widgets/{}",
                input.dashboard_id, input.widget_id
            ))
            .await?;
        Ok(DeleteWidgetOutput { success: true })
    }
}

// ── Dashboard Snapshot ──────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct DashboardSnapshotInput {
    /// ID of the dashboard to snapshot
    pub dashboard_id: String,
    /// Time range for all widget queries (defaults to last 1 hour if omitted)
    #[serde(default = "default_time_range")]
    pub time_range: TimeRange,
    /// Optional dashboard variables (e.g. {"service": "api-gateway"})
    #[serde(default)]
    pub variables: Option<std::collections::BTreeMap<String, serde_json::Value>>,
}

#[derive(Serialize)]
pub struct DashboardSnapshotOutput {
    pub dashboard_name: String,
    pub dashboard_description: Option<String>,
    pub time_range: TimeRange,
    pub widgets: Vec<WidgetSnapshot>,
}

#[derive(Serialize)]
pub struct WidgetSnapshot {
    pub widget_id: String,
    pub title: Option<String>,
    pub widget_type: String,
    pub columns: Vec<String>,
    pub data: Vec<serde_json::Value>,
    pub error: Option<String>,
}

pub struct DashboardSnapshot;

#[async_trait]
impl PlatformAction for DashboardSnapshot {
    type Input = DashboardSnapshotInput;
    type Output = DashboardSnapshotOutput;

    fn name(&self) -> &'static str {
        "dashboard_snapshot"
    }
    fn description(&self) -> &'static str {
        "Get a full snapshot of a dashboard — executes every widget's query and returns \
         the data as structured JSON, equivalent to viewing the dashboard in the UI. \
         Start here when a user asks about their application's health or monitoring \
         status. Returns all widget data so you can analyze trends, spot anomalies, and \
         answer questions without asking the user to look at charts. time_range defaults \
         to the last 1 hour if omitted; set it explicitly for longer windows (last 24h \
         for daily patterns, last 7d for trends). Use list dashboards first to find \
         the dashboard_id."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;

        let dashboard_resp = ctx
            .http
            .website_get(&format!(
                "/api/dashboards/{pid}/dashboards/{}",
                input.dashboard_id
            ))
            .await?;
        let dashboard: serde_json::Value = dashboard_resp.json().await?;

        let dashboard_name = dashboard["name"].as_str().unwrap_or("Untitled").to_string();
        let dashboard_description = dashboard["description"].as_str().map(String::from);

        let widgets_resp = ctx
            .http
            .website_get(&format!(
                "/api/dashboards/{pid}/dashboards/{}/widgets",
                input.dashboard_id
            ))
            .await?;
        let widgets: Vec<serde_json::Value> = widgets_resp.json().await?;

        let time_range_json = serde_json::to_value(&input.time_range)?;
        let variables_json = input.variables.clone();

        let futures: Vec<_> = widgets
            .iter()
            .map(|widget| {
                let widget_id = widget["id"].as_str().unwrap_or("").to_string();
                let title = widget["title"].as_str().map(String::from);
                let widget_type = widget["widget_type"]
                    .as_str()
                    .unwrap_or("unknown")
                    .to_string();
                let widget_config = widget["widget_config"].clone();
                let time_range = time_range_json.clone();
                let variables = variables_json.clone();
                let http = ctx.http.clone();

                async move {
                    let query = widget_config
                        .get("query")
                        .cloned()
                        .unwrap_or(widget_config.clone());
                    if query.is_null()
                        || (query.is_object() && query.as_object().map_or(true, |o| o.is_empty()))
                    {
                        return WidgetSnapshot {
                            widget_id,
                            title,
                            widget_type,
                            columns: vec![],
                            data: vec![],
                            error: Some("No query configured".into()),
                        };
                    }

                    let body = serde_json::json!({
                        "query": query,
                        "time_range": time_range,
                        "variables": variables,
                    });

                    match http
                        .watch_post(&format!("/api/{pid}/widget-query"), &body)
                        .await
                    {
                        Ok(resp) => match resp.json::<serde_json::Value>().await {
                            Ok(result) => {
                                let columns = result["columns"]
                                    .as_array()
                                    .map(|arr| {
                                        arr.iter()
                                            .filter_map(|v| v.as_str().map(String::from))
                                            .collect()
                                    })
                                    .unwrap_or_default();
                                let data = result["data"].as_array().cloned().unwrap_or_default();
                                WidgetSnapshot {
                                    widget_id,
                                    title,
                                    widget_type,
                                    columns,
                                    data,
                                    error: None,
                                }
                            }
                            Err(e) => WidgetSnapshot {
                                widget_id,
                                title,
                                widget_type,
                                columns: vec![],
                                data: vec![],
                                error: Some(format!("Failed to parse response: {e}")),
                            },
                        },
                        Err(e) => WidgetSnapshot {
                            widget_id,
                            title,
                            widget_type,
                            columns: vec![],
                            data: vec![],
                            error: Some(format!("Query failed: {e}")),
                        },
                    }
                }
            })
            .collect();

        let widget_snapshots = join_all(futures).await;

        Ok(DashboardSnapshotOutput {
            dashboard_name,
            dashboard_description,
            time_range: input.time_range,
            widgets: widget_snapshots,
        })
    }
}

// ── Import Grafana Dashboard ─────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub struct ImportGrafanaDashboardInput {
    /// Full Grafana dashboard JSON export (the object you get from Grafana's
    /// "Share > Export > Save to file"). Accepts both wrapped format
    /// (with a top-level `dashboard` key) and flat format.
    pub grafana_json: serde_json::Value,
    /// If true, returns a preview of what would be imported (widget count,
    /// warnings) without actually creating the dashboard.
    #[serde(default)]
    pub dry_run: Option<bool>,
}

#[derive(Serialize)]
pub struct ImportGrafanaDashboardOutput {
    pub result: serde_json::Value,
}

pub struct ImportGrafanaDashboard;

#[async_trait]
impl PlatformAction for ImportGrafanaDashboard {
    type Input = ImportGrafanaDashboardInput;
    type Output = ImportGrafanaDashboardOutput;

    fn name(&self) -> &'static str {
        "import_grafana_dashboard"
    }
    fn description(&self) -> &'static str {
        "Import a Grafana dashboard JSON export into the current project. The Grafana \
         JSON is converted to native dashboard widgets with PromQL queries preserved. \
         Prometheus-only labels (like `job`) are automatically stripped. Set dry_run=true \
         to preview the import without creating anything."
    }
    fn required_scope(&self) -> String {
        "observability:write".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let pid = ctx.project_id;
        let dry_run = input.dry_run.unwrap_or(false);
        let url = format!(
            "/api/dashboards/{pid}/dashboards/import-grafana?dry_run={dry_run}"
        );
        let resp = ctx.http.website_post(&url, &input.grafana_json).await?;
        let result = resp.json().await?;
        Ok(ImportGrafanaDashboardOutput { result })
    }
}

// ── Registration ─────────────────────────────────────────────────────

pub fn register(registry: &mut ActionRegistry) {
    registry.register(ListDashboards);
    registry.register(GetDashboard);
    registry.register(ListDashboardTemplates);
    registry.register(CreateDashboard);
    registry.register(CreateDashboardFromTemplate);
    registry.register(ImportGrafanaDashboard);
    registry.register(QueryWidget);
    registry.register(UpdateDashboard);
    registry.register(ListWidgets);
    registry.register(CreateWidget);
    registry.register(UpdateWidget);
    registry.register(DeleteWidget);
    registry.register(DashboardSnapshot);
}
