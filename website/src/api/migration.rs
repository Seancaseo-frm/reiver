use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize};
use std::sync::LazyLock;

// ---------------------------------------------------------------------------
// Datadog JSON structs (input)
// ---------------------------------------------------------------------------

fn default_untitled() -> String {
    "Untitled Dashboard".to_string()
}

/// Deserializer that handles `requests` being either an array (normal) or
/// an object (scatterplot, hostmap). When it's an object, we return an
/// empty Vec since we can't process those widget types anyway.
fn deserialize_requests_flexible<'de, D>(deserializer: D) -> Result<Vec<DatadogRequest>, D::Error>
where
    D: Deserializer<'de>,
{
    let value: serde_json::Value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::Array(_) => {
            serde_json::from_value(value).map_err(serde::de::Error::custom)
        }
        _ => Ok(vec![]),
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DatadogDashboard {
    #[serde(default = "default_untitled")]
    pub title: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub layout_type: Option<String>,
    #[serde(default)]
    pub template_variables: Vec<DatadogTemplateVariable>,
    #[serde(default)]
    pub widgets: Vec<DatadogWidget>,
    /// Legacy format uses `graphs` instead of `widgets`.
    #[serde(default)]
    pub graphs: Vec<DatadogLegacyGraph>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DatadogTemplateVariable {
    pub name: String,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub default: Option<String>,
    #[serde(default)]
    pub defaults: Vec<String>,
    #[serde(default)]
    pub available_values: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DatadogWidget {
    #[serde(default)]
    pub id: Option<i64>,
    pub definition: DatadogDefinition,
    #[serde(default)]
    pub layout: Option<DatadogLayout>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DatadogDefinition {
    #[serde(rename = "type")]
    pub widget_type: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default, deserialize_with = "deserialize_requests_flexible")]
    pub requests: Vec<DatadogRequest>,
    /// Group widgets contain nested widgets.
    #[serde(default)]
    pub widgets: Vec<DatadogWidget>,
    #[serde(default)]
    pub yaxis: Option<serde_json::Value>,
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub columns: Option<serde_json::Value>,
    #[serde(default)]
    pub indexes: Option<serde_json::Value>,
    #[serde(default)]
    pub query: Option<String>,
    /// Catch-all for any other fields in the definition.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DatadogRequest {
    /// Old-format query string.
    #[serde(default)]
    pub q: Option<String>,
    #[serde(default)]
    pub aggregator: Option<String>,
    /// New-format structured queries.
    #[serde(default)]
    pub queries: Vec<DatadogStructuredQuery>,
    /// New-format formulas combining named queries.
    #[serde(default)]
    pub formulas: Vec<DatadogFormula>,
    #[serde(default)]
    pub response_format: Option<String>,
    #[serde(default)]
    pub display_type: Option<String>,
    #[serde(default)]
    pub style: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DatadogStructuredQuery {
    #[serde(default)]
    pub query: Option<String>,
    #[serde(default)]
    pub data_source: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub aggregator: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DatadogFormula {
    #[serde(default)]
    pub formula: Option<String>,
    #[serde(default)]
    pub alias: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DatadogLayout {
    #[serde(default)]
    pub x: f64,
    #[serde(default)]
    pub y: f64,
    #[serde(default)]
    pub width: f64,
    #[serde(default)]
    pub height: f64,
}

/// Legacy Datadog format: `graphs[]` with `definition.viz` instead of `type`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DatadogLegacyGraph {
    #[serde(default)]
    pub title: Option<String>,
    pub definition: DatadogLegacyDefinition,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DatadogLegacyDefinition {
    #[serde(default)]
    pub viz: Option<String>,
    #[serde(default)]
    pub requests: Vec<DatadogRequest>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Reiver output types
// ---------------------------------------------------------------------------

/// The result of converting a Datadog dashboard.
#[derive(Debug, Clone, Serialize)]
pub struct ImportResult {
    /// Dashboard title.
    pub name: String,
    /// Dashboard description.
    pub description: Option<String>,
    /// Variables for `layout_config`.
    pub variables: Vec<serde_json::Value>,
    /// Converted tabs with their widgets.
    pub tabs: Vec<ConvertedTab>,
    /// Human-readable warnings about skipped/unsupported items.
    pub warnings: Vec<String>,
    /// Count of successfully converted widgets.
    pub converted_count: usize,
    /// Count of skipped widgets.
    pub skipped_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConvertedTab {
    pub name: String,
    pub icon: Option<String>,
    pub widgets: Vec<ConvertedWidget>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConvertedWidget {
    pub widget_type: String,
    pub title: Option<String>,
    pub config: serde_json::Value,
    pub x: i32,
    pub y: i32,
    pub w: i32,
    pub h: i32,
}

// ---------------------------------------------------------------------------
// Query translation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ParsedQuery {
    pub aggregation: String,
    pub metric_name: String,
    pub filters: Vec<(String, String)>,
    pub group_by: Vec<String>,
    pub rollup_interval: Option<String>,
    pub order_by: Option<String>,
    pub order_dir: Option<String>,
    pub limit: Option<i64>,
    pub is_rate: bool,
    pub is_count: bool,
    pub original: String,
    pub needs_manual_review: bool,
}

static BASIC_QUERY_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?x)
        ^
        (\w+)                          # aggregation function (avg, sum, min, max, count)
        :
        ([\w./_-]+)                    # metric name
        \{([^}]*)\}                    # filter block {tag:val,...}
        (?:\s*by\s*\{([^}]*)\})?      # optional grouping by {g1,g2}
        (.*)                           # trailing modifiers (.rollup, .as_count, etc.)
        $
    ",
    )
    .expect("basic query regex")
});

static TOP_QUERY_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?x)
        ^top\(
        (.+?)                          # inner query
        ,\s*(\d+)                      # limit N
        ,\s*'(\w+)'                    # aggregation ('mean', 'max', ...)
        ,\s*'(\w+)'                    # direction ('desc', 'asc')
        \)$
    ",
    )
    .expect("top() query regex")
});

static ROLLUP_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\.rollup\((\w+),\s*(\d+)\)").expect("rollup regex"));

/// Parse a Datadog metric query string into structured components.
///
/// Handles patterns like:
/// - `avg:system.cpu.user{host:web-*,env:prod} by {host}`
/// - `top(sum:kubernetes.cpu{...} by {ns}, 10, 'mean', 'desc')`
/// - `sum:requests{...}.as_count()`
pub fn parse_datadog_query(q: &str) -> ParsedQuery {
    let trimmed = q.trim();
    let original = trimmed.to_string();

    // Check for unsupported patterns first.
    let unsupported_patterns = ["fill(", "forecast(", "anomaly(", "outliers(", "ewma("];
    for pat in &unsupported_patterns {
        if trimmed.contains(pat) {
            return ParsedQuery {
                aggregation: String::new(),
                metric_name: String::new(),
                filters: vec![],
                group_by: vec![],
                rollup_interval: None,
                order_by: None,
                order_dir: None,
                limit: None,
                is_rate: false,
                is_count: false,
                original,
                needs_manual_review: true,
            };
        }
    }

    // Check for arithmetic between metrics (e.g., "a + b / c").
    if trimmed.contains(" + ")
        || trimmed.contains(" - ")
        || trimmed.contains(" / ")
        || trimmed.contains(" * ")
    {
        // Only flag if it's not inside a function call like top()
        let without_top = if trimmed.starts_with("top(") {
            ""
        } else {
            trimmed
        };
        if !without_top.is_empty()
            && (without_top.contains(" + ")
                || without_top.contains(" - ")
                || without_top.contains(" / ")
                || without_top.contains(" * "))
        {
            return ParsedQuery {
                aggregation: String::new(),
                metric_name: String::new(),
                filters: vec![],
                group_by: vec![],
                rollup_interval: None,
                order_by: None,
                order_dir: None,
                limit: None,
                is_rate: false,
                is_count: false,
                original,
                needs_manual_review: true,
            };
        }
    }

    // Handle top() wrapper.
    if let Some(caps) = TOP_QUERY_RE.captures(trimmed) {
        let inner = caps.get(1).map(|m| m.as_str()).unwrap_or("");
        let limit: i64 = caps
            .get(2)
            .and_then(|m| m.as_str().parse().ok())
            .unwrap_or(10);
        let _top_agg = caps.get(3).map(|m| m.as_str()).unwrap_or("mean");
        let direction = caps.get(4).map(|m| m.as_str()).unwrap_or("desc");

        let mut parsed = parse_datadog_query(inner);
        parsed.limit = Some(limit);
        parsed.order_by = Some("value".to_string());
        parsed.order_dir = Some(direction.to_string());
        parsed.original = original;
        return parsed;
    }

    // Match basic pattern: agg:metric{filters} by {groups} .modifiers
    if let Some(caps) = BASIC_QUERY_RE.captures(trimmed) {
        let aggregation = caps.get(1).map(|m| m.as_str()).unwrap_or("avg").to_string();
        let metric_name = caps.get(2).map(|m| m.as_str()).unwrap_or("").to_string();
        let filter_str = caps.get(3).map(|m| m.as_str()).unwrap_or("");
        let group_str = caps.get(4).map(|m| m.as_str()).unwrap_or("");
        let trailing = caps.get(5).map(|m| m.as_str()).unwrap_or("");

        let filters = parse_filters(filter_str);
        let group_by = parse_group_by(group_str);

        let mut rollup_interval = None;
        if let Some(rollup_caps) = ROLLUP_RE.captures(trailing) {
            let seconds: u64 = rollup_caps
                .get(2)
                .and_then(|m| m.as_str().parse().ok())
                .unwrap_or(60);
            rollup_interval = Some(format_interval(seconds));
        }

        let is_count = trailing.contains(".as_count()");
        let is_rate = trailing.contains(".as_rate()");

        return ParsedQuery {
            aggregation,
            metric_name,
            filters,
            group_by,
            rollup_interval,
            order_by: None,
            order_dir: None,
            limit: None,
            is_rate,
            is_count,
            original,
            needs_manual_review: false,
        };
    }

    // Couldn't parse -- mark for manual review.
    ParsedQuery {
        aggregation: String::new(),
        metric_name: String::new(),
        filters: vec![],
        group_by: vec![],
        rollup_interval: None,
        order_by: None,
        order_dir: None,
        limit: None,
        is_rate: false,
        is_count: false,
        original,
        needs_manual_review: true,
    }
}

fn parse_filters(filter_str: &str) -> Vec<(String, String)> {
    let trimmed = filter_str.trim();
    if trimmed.is_empty() || trimmed == "*" {
        return vec![];
    }
    trimmed
        .split(',')
        .filter_map(|pair| {
            let pair = pair.trim();
            if pair.is_empty() || pair == "*" {
                return None;
            }
            // Template variable references like $env or kube_cluster:$k8s_cluster
            let (key, value) = if let Some(idx) = pair.find(':') {
                (
                    pair[..idx].trim().to_string(),
                    pair[idx + 1..].trim().to_string(),
                )
            } else {
                // Bare filter like `*` or a tag name without value
                return None;
            };
            Some((key, value))
        })
        .collect()
}

fn parse_group_by(group_str: &str) -> Vec<String> {
    let trimmed = group_str.trim();
    if trimmed.is_empty() {
        return vec![];
    }
    trimmed
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn format_interval(seconds: u64) -> String {
    if seconds >= 3600 && seconds % 3600 == 0 {
        format!("{}h", seconds / 3600)
    } else if seconds >= 60 && seconds % 60 == 0 {
        format!("{}m", seconds / 60)
    } else {
        format!("{}s", seconds)
    }
}

/// Convert a `ParsedQuery` into a Reiver widget query config (JSON).
pub fn query_to_widget_config(parsed: &ParsedQuery) -> serde_json::Value {
    if parsed.needs_manual_review {
        return serde_json::json!({
            "table": "metrics",
            "description": format!("Manual review needed: {}", parsed.original),
            "raw_query": parsed.original,
        });
    }

    let agg = if parsed.is_count {
        "sum".to_string()
    } else {
        parsed.aggregation.clone()
    };

    // Build WHERE clause.
    let mut where_parts = vec![format!("metric_name = '{}'", parsed.metric_name)];
    for (key, value) in &parsed.filters {
        if value.starts_with('$') {
            // Template variable -- keep as placeholder.
            where_parts.push(format!("{} = '{}'", key, value));
        } else if value.contains('*') {
            let like_val = value.replace('*', "%");
            where_parts.push(format!("{} LIKE '{}'", key, like_val));
        } else {
            where_parts.push(format!("{} = '{}'", key, value));
        }
    }
    let where_clause = where_parts.join(" AND ");

    let interval = parsed
        .rollup_interval
        .clone()
        .unwrap_or_else(|| "1m".to_string());

    let mut config = serde_json::json!({
        "table": "metrics",
        "select": [{"fn": agg, "field": "value", "alias": "value"}],
        "where": where_clause,
        "interval": interval,
    });

    if !parsed.group_by.is_empty() {
        config["groupBy"] = serde_json::json!(parsed.group_by);
    }

    if let Some(ref order_by) = parsed.order_by {
        let dir = parsed.order_dir.as_deref().unwrap_or("desc");
        config["orderBy"] = serde_json::json!({ "field": order_by, "direction": dir });
    }

    if let Some(limit) = parsed.limit {
        config["limit"] = serde_json::json!(limit);
    }

    if parsed.is_rate {
        config["description"] =
            serde_json::json!(format!("Converted from rate query: {}", parsed.original));
    }

    config
}

// ---------------------------------------------------------------------------
// Widget type mapping
// ---------------------------------------------------------------------------

/// Map a Datadog widget type to its Reiver equivalent.
/// Returns `None` for unsupported types (group widgets are handled separately).
pub fn map_widget_type(dd_type: &str) -> Option<&'static str> {
    match dd_type {
        "timeseries" => Some("timeseries"),
        "query_value" => Some("stat"),
        "toplist" => Some("table"),
        "table" | "query_table" => Some("table"),
        "log_stream" => Some("table"),
        _ => None,
    }
}

/// Check if a Datadog type is a group widget.
pub fn is_group_widget(dd_type: &str) -> bool {
    dd_type == "group"
}

/// Check if a Datadog type is explicitly skipped (with warning).
pub fn skipped_widget_reason(dd_type: &str) -> Option<&'static str> {
    match dd_type {
        "note" => Some("Note/text widgets are not supported yet"),
        "heatmap" => Some("Heatmap widgets are not supported"),
        "hostmap" => Some("Host map widgets are not supported"),
        "event_stream" => Some("Event stream widgets are not supported"),
        "event_timeline" => Some("Event timeline widgets are not supported"),
        "image" => Some("Image widgets are not supported"),
        "free_text" => Some("Free text widgets are not supported"),
        "iframe" => Some("IFrame widgets are not supported"),
        "alert_graph" => Some("Alert graph widgets are not supported"),
        "alert_value" => Some("Alert value widgets are not supported"),
        "check_status" => Some("Check status widgets are not supported"),
        "scatterplot" => Some("Scatterplot widgets are not supported"),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Layout conversion
// ---------------------------------------------------------------------------

/// Convert Datadog widget layouts to Reiver 12-column grid positions.
///
/// For `"free"` layouts: scales from Datadog's coordinate space (~94 units wide)
/// to a 12-column grid. For `"ordered"` layouts: stacks widgets 2-per-row.
pub fn convert_layouts(
    widgets: &[DatadogWidget],
    layout_type: Option<&str>,
) -> Vec<(i32, i32, i32, i32)> {
    let is_free = layout_type.map(|l| l == "free").unwrap_or(false);

    if is_free || widgets.iter().any(|w| w.layout.is_some()) {
        convert_free_layout(widgets)
    } else {
        convert_ordered_layout(widgets.len())
    }
}

fn convert_free_layout(widgets: &[DatadogWidget]) -> Vec<(i32, i32, i32, i32)> {
    // Find max width across all widgets to determine the coordinate space.
    let max_width = widgets
        .iter()
        .filter_map(|w| w.layout.as_ref().map(|l| l.x + l.width))
        .fold(0.0_f64, f64::max)
        .max(1.0);

    widgets
        .iter()
        .map(|w| {
            if let Some(ref layout) = w.layout {
                let grid_x = ((layout.x / max_width) * 12.0).round() as i32;
                let grid_w = ((layout.width / max_width) * 12.0).round().max(2.0) as i32;
                let grid_h = (layout.height / 5.0).round().max(2.0) as i32;
                let grid_y = (layout.y / 5.0).round() as i32;

                // Clamp to 12-column grid.
                let grid_x = grid_x.min(10);
                let grid_w = grid_w.clamp(2, 12 - grid_x);

                (grid_x, grid_y, grid_w, grid_h)
            } else {
                (0, 0, 6, 4)
            }
        })
        .collect()
}

fn convert_ordered_layout(count: usize) -> Vec<(i32, i32, i32, i32)> {
    let mut positions = Vec::with_capacity(count);
    for i in 0..count {
        let col = (i % 2) as i32;
        let row = (i / 2) as i32;
        positions.push((col * 6, row * 4, 6, 4));
    }
    positions
}

// ---------------------------------------------------------------------------
// Dashboard conversion orchestrator
// ---------------------------------------------------------------------------

/// Convert a Datadog dashboard export into Reiver's import format.
pub fn convert_dashboard(dd: DatadogDashboard) -> ImportResult {
    let mut warnings: Vec<String> = Vec::new();
    let mut converted_count = 0_usize;
    let mut skipped_count = 0_usize;

    // Convert template variables.
    let variables: Vec<serde_json::Value> = dd
        .template_variables
        .iter()
        .map(|tv| {
            serde_json::json!({
                "name": tv.name,
                "label": tv.prefix.as_deref().unwrap_or(&tv.name),
                "default": tv.default.as_deref().unwrap_or("*"),
            })
        })
        .collect();

    let layout_type = dd.layout_type.as_deref();

    // Handle legacy format: convert graphs[] to widgets[].
    let widgets = if dd.widgets.is_empty() && !dd.graphs.is_empty() {
        warnings.push("Legacy Datadog format detected (graphs[] instead of widgets[]). Conversion is best-effort.".to_string());
        dd.graphs
            .iter()
            .map(|g| DatadogWidget {
                id: None,
                definition: DatadogDefinition {
                    widget_type: g
                        .definition
                        .viz
                        .clone()
                        .unwrap_or_else(|| "timeseries".to_string()),
                    title: g.title.clone(),
                    requests: g.definition.requests.clone(),
                    widgets: vec![],
                    yaxis: None,
                    content: None,
                    columns: None,
                    indexes: None,
                    query: None,
                    extra: g.definition.extra.clone(),
                },
                layout: None,
            })
            .collect::<Vec<_>>()
    } else {
        dd.widgets
    };

    // Separate group widgets (-> tabs) from regular widgets.
    let mut group_widgets: Vec<&DatadogWidget> = Vec::new();
    let mut top_level_widgets: Vec<&DatadogWidget> = Vec::new();

    for widget in &widgets {
        if is_group_widget(&widget.definition.widget_type) {
            group_widgets.push(widget);
        } else {
            top_level_widgets.push(widget);
        }
    }

    let mut tabs: Vec<ConvertedTab> = Vec::new();

    if group_widgets.is_empty() {
        // No groups -- all widgets go into a single default tab.
        let layouts = convert_layouts(
            &top_level_widgets
                .iter()
                .map(|w| (*w).clone())
                .collect::<Vec<_>>(),
            layout_type,
        );

        let mut tab_widgets = Vec::new();
        for (i, widget) in top_level_widgets.iter().enumerate() {
            let (x, y, w, h) = layouts.get(i).copied().unwrap_or((0, 0, 6, 4));
            match convert_widget(widget, x, y, w, h) {
                WidgetConversion::Converted(cw) => {
                    converted_count += 1;
                    tab_widgets.push(cw);
                }
                WidgetConversion::Skipped(reason) => {
                    skipped_count += 1;
                    warnings.push(reason);
                }
            }
        }

        tabs.push(ConvertedTab {
            name: "Overview".to_string(),
            icon: None,
            widgets: tab_widgets,
        });
    } else {
        // Each group becomes a tab. Top-level non-group widgets go into "General".
        if !top_level_widgets.is_empty() {
            let layouts = convert_layouts(
                &top_level_widgets
                    .iter()
                    .map(|w| (*w).clone())
                    .collect::<Vec<_>>(),
                layout_type,
            );

            let mut tab_widgets = Vec::new();
            for (i, widget) in top_level_widgets.iter().enumerate() {
                let (x, y, w, h) = layouts.get(i).copied().unwrap_or((0, 0, 6, 4));
                match convert_widget(widget, x, y, w, h) {
                    WidgetConversion::Converted(cw) => {
                        converted_count += 1;
                        tab_widgets.push(cw);
                    }
                    WidgetConversion::Skipped(reason) => {
                        skipped_count += 1;
                        warnings.push(reason);
                    }
                }
            }

            if !tab_widgets.is_empty() {
                tabs.push(ConvertedTab {
                    name: "General".to_string(),
                    icon: None,
                    widgets: tab_widgets,
                });
            }
        }

        // Convert each group into a tab.
        for group in &group_widgets {
            let tab_name = group
                .definition
                .title
                .clone()
                .unwrap_or_else(|| "Untitled".to_string());
            let child_widgets = &group.definition.widgets;

            let child_refs: Vec<DatadogWidget> = child_widgets.to_vec();
            let layouts = convert_layouts(&child_refs, layout_type);

            let mut tab_widgets = Vec::new();
            for (i, child) in child_widgets.iter().enumerate() {
                let (x, y, w, h) = layouts.get(i).copied().unwrap_or((0, 0, 6, 4));
                match convert_widget(child, x, y, w, h) {
                    WidgetConversion::Converted(cw) => {
                        converted_count += 1;
                        tab_widgets.push(cw);
                    }
                    WidgetConversion::Skipped(reason) => {
                        skipped_count += 1;
                        warnings.push(reason);
                    }
                }
            }

            tabs.push(ConvertedTab {
                name: tab_name,
                icon: None,
                widgets: tab_widgets,
            });
        }
    }

    ImportResult {
        name: dd.title,
        description: dd.description,
        variables,
        tabs,
        warnings,
        converted_count,
        skipped_count,
    }
}

// ---------------------------------------------------------------------------
// Single-widget conversion
// ---------------------------------------------------------------------------

enum WidgetConversion {
    Converted(ConvertedWidget),
    Skipped(String),
}

fn convert_widget(widget: &DatadogWidget, x: i32, y: i32, w: i32, h: i32) -> WidgetConversion {
    let dd_type = &widget.definition.widget_type;
    let title = widget.definition.title.clone();

    // Check if it's a known-skipped type.
    if let Some(reason) = skipped_widget_reason(dd_type) {
        let msg = format!(
            "Skipped widget '{}' (type: {}): {}",
            title.as_deref().unwrap_or("untitled"),
            dd_type,
            reason
        );
        return WidgetConversion::Skipped(msg);
    }

    // Map widget type.
    let dh_type = match map_widget_type(dd_type) {
        Some(t) => t,
        None => {
            let msg = format!(
                "Skipped widget '{}': unsupported type '{}'",
                title.as_deref().unwrap_or("untitled"),
                dd_type,
            );
            return WidgetConversion::Skipped(msg);
        }
    };

    // Convert query configuration from requests.
    let config = convert_requests(&widget.definition, dh_type);

    WidgetConversion::Converted(ConvertedWidget {
        widget_type: dh_type.to_string(),
        title,
        config,
        x,
        y,
        w,
        h,
    })
}

/// Convert Datadog widget requests into a Reiver widget config.
fn convert_requests(def: &DatadogDefinition, dh_type: &str) -> serde_json::Value {
    // Special handling for log_stream: create a logs table query.
    if def.widget_type == "log_stream" {
        return serde_json::json!({
            "table": "logs",
            "select": [
                {"field": "timestamp", "alias": "timestamp"},
                {"field": "severity", "alias": "severity"},
                {"field": "body", "alias": "message"},
            ],
            "orderBy": {"field": "timestamp", "direction": "desc"},
            "limit": 100,
            "description": "Converted from Datadog log_stream widget",
        });
    }

    if def.requests.is_empty() {
        return serde_json::json!({
            "table": "metrics",
            "description": "No query data found in original widget",
        });
    }

    // Process the primary request.
    let primary = &def.requests[0];
    let primary_config = convert_single_request(primary);

    let mut config = primary_config;

    // If there's a second request, add it as secondaryQuery.
    if def.requests.len() >= 2 {
        let secondary = &def.requests[1];
        let secondary_config = convert_single_request(secondary);
        config["secondaryQuery"] = secondary_config;
    }

    // Warn if >2 requests.
    if def.requests.len() > 2 {
        let existing_desc = config
            .get("description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let extra_note = format!(
            "{}. Note: {} additional queries from original widget were dropped (max 2 supported)",
            existing_desc,
            def.requests.len() - 2
        );
        config["description"] = serde_json::json!(extra_note.trim_start_matches(". "));
    }

    // For stat widgets, set display preferences.
    if dh_type == "stat" {
        if config.get("select").is_some() {
            // Use the request aggregator if available.
            if let Some(agg) = primary.aggregator.as_deref() {
                config["statAggregation"] = serde_json::json!(agg);
            }
        }
    }

    config
}

fn convert_single_request(req: &DatadogRequest) -> serde_json::Value {
    // Old format: use `q` field.
    if let Some(ref q) = req.q {
        let parsed = parse_datadog_query(q);
        return query_to_widget_config(&parsed);
    }

    // New format: use `queries[]` and `formulas[]`.
    if !req.queries.is_empty() {
        // If there are formulas, the query is complex -- flag for manual review.
        if !req.formulas.is_empty() {
            let formula_strs: Vec<String> = req
                .formulas
                .iter()
                .filter_map(|f| f.formula.clone())
                .collect();
            return serde_json::json!({
                "table": "metrics",
                "description": format!(
                    "Manual review needed: complex formula query ({})",
                    formula_strs.join(", ")
                ),
                "raw_formulas": formula_strs,
            });
        }

        // Single structured query without formula: parse the query string.
        if let Some(ref query_str) = req.queries[0].query {
            let parsed = parse_datadog_query(query_str);
            return query_to_widget_config(&parsed);
        }
    }

    serde_json::json!({
        "table": "metrics",
        "description": "No parseable query found",
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_query() {
        let q = "avg:system.cpu.user{host:web-01,env:prod} by {host}";
        let parsed = parse_datadog_query(q);

        assert!(!parsed.needs_manual_review);
        assert_eq!(parsed.aggregation, "avg");
        assert_eq!(parsed.metric_name, "system.cpu.user");
        assert_eq!(
            parsed.filters,
            vec![
                ("host".to_string(), "web-01".to_string()),
                ("env".to_string(), "prod".to_string()),
            ]
        );
        assert_eq!(parsed.group_by, vec!["host".to_string()]);
    }

    #[test]
    fn test_parse_query_with_wildcard() {
        let q = "sum:requests.count{host:web-*}";
        let parsed = parse_datadog_query(q);

        assert!(!parsed.needs_manual_review);
        assert_eq!(parsed.aggregation, "sum");
        assert_eq!(parsed.metric_name, "requests.count");
        assert_eq!(
            parsed.filters,
            vec![("host".to_string(), "web-*".to_string())]
        );
    }

    #[test]
    fn test_parse_query_with_rollup() {
        let q = "avg:system.load.1{*}.rollup(avg, 300)";
        let parsed = parse_datadog_query(q);

        assert!(!parsed.needs_manual_review);
        assert_eq!(parsed.rollup_interval, Some("5m".to_string()));
    }

    #[test]
    fn test_parse_query_as_count() {
        let q = "sum:http.requests{service:web}.as_count()";
        let parsed = parse_datadog_query(q);

        assert!(!parsed.needs_manual_review);
        assert!(parsed.is_count);
    }

    #[test]
    fn test_parse_top_query() {
        let q = "top(sum:kubernetes.cpu.usage.total{*} by {kube_namespace}, 10, 'mean', 'desc')";
        let parsed = parse_datadog_query(q);

        assert!(!parsed.needs_manual_review);
        assert_eq!(parsed.aggregation, "sum");
        assert_eq!(parsed.metric_name, "kubernetes.cpu.usage.total");
        assert_eq!(parsed.limit, Some(10));
        assert_eq!(parsed.order_dir, Some("desc".to_string()));
    }

    #[test]
    fn test_parse_unsupported_query() {
        let q = "avg:system.cpu{*}.fill(linear)";
        let parsed = parse_datadog_query(q);
        assert!(parsed.needs_manual_review);
    }

    #[test]
    fn test_parse_arithmetic_query() {
        let q = "avg:system.cpu.user{*} + avg:system.cpu.system{*}";
        let parsed = parse_datadog_query(q);
        assert!(parsed.needs_manual_review);
    }

    #[test]
    fn test_query_to_widget_config_basic() {
        let q = "avg:system.cpu.user{env:prod} by {host}";
        let parsed = parse_datadog_query(q);
        let config = query_to_widget_config(&parsed);

        assert_eq!(config["table"], "metrics");
        assert_eq!(
            config["where"],
            "metric_name = 'system.cpu.user' AND env = 'prod'"
        );
        assert_eq!(config["groupBy"], serde_json::json!(["host"]));
        assert_eq!(config["select"][0]["fn"], "avg");
    }

    #[test]
    fn test_query_to_widget_config_wildcard() {
        let q = "sum:requests{host:web-*}";
        let parsed = parse_datadog_query(q);
        let config = query_to_widget_config(&parsed);

        assert_eq!(
            config["where"],
            "metric_name = 'requests' AND host LIKE 'web-%'"
        );
    }

    #[test]
    fn test_map_widget_type_known() {
        assert_eq!(map_widget_type("timeseries"), Some("timeseries"));
        assert_eq!(map_widget_type("query_value"), Some("stat"));
        assert_eq!(map_widget_type("toplist"), Some("table"));
        assert_eq!(map_widget_type("table"), Some("table"));
        assert_eq!(map_widget_type("query_table"), Some("table"));
        assert_eq!(map_widget_type("log_stream"), Some("table"));
    }

    #[test]
    fn test_map_widget_type_unknown() {
        assert_eq!(map_widget_type("heatmap"), None);
        assert_eq!(map_widget_type("hostmap"), None);
    }

    #[test]
    fn test_convert_ordered_layout() {
        let positions = convert_ordered_layout(5);
        assert_eq!(positions.len(), 5);
        assert_eq!(positions[0], (0, 0, 6, 4));
        assert_eq!(positions[1], (6, 0, 6, 4));
        assert_eq!(positions[2], (0, 4, 6, 4));
        assert_eq!(positions[3], (6, 4, 6, 4));
        assert_eq!(positions[4], (0, 8, 6, 4));
    }

    #[test]
    fn test_convert_free_layout() {
        let widgets = vec![
            DatadogWidget {
                id: None,
                definition: DatadogDefinition {
                    widget_type: "timeseries".to_string(),
                    title: Some("Test".to_string()),
                    requests: vec![],
                    widgets: vec![],
                    yaxis: None,
                    content: None,
                    columns: None,
                    indexes: None,
                    query: None,
                    extra: serde_json::Map::new(),
                },
                layout: Some(DatadogLayout {
                    x: 0.0,
                    y: 0.0,
                    width: 47.0,
                    height: 15.0,
                }),
            },
            DatadogWidget {
                id: None,
                definition: DatadogDefinition {
                    widget_type: "query_value".to_string(),
                    title: Some("Test 2".to_string()),
                    requests: vec![],
                    widgets: vec![],
                    yaxis: None,
                    content: None,
                    columns: None,
                    indexes: None,
                    query: None,
                    extra: serde_json::Map::new(),
                },
                layout: Some(DatadogLayout {
                    x: 47.0,
                    y: 0.0,
                    width: 47.0,
                    height: 15.0,
                }),
            },
        ];

        let positions = convert_free_layout(&widgets);
        assert_eq!(positions.len(), 2);
        // First widget: x=0, full half-width (47/94*12 = 6)
        assert_eq!(positions[0].0, 0); // x
        assert_eq!(positions[0].2, 6); // w
                                       // Second widget: x=6, full half-width
        assert_eq!(positions[1].0, 6); // x
        assert_eq!(positions[1].2, 6); // w
    }

    #[test]
    fn test_convert_simple_dashboard() {
        let dd = DatadogDashboard {
            title: "Test Dashboard".to_string(),
            description: Some("A test".to_string()),
            layout_type: Some("ordered".to_string()),
            template_variables: vec![DatadogTemplateVariable {
                name: "env".to_string(),
                prefix: Some("environment".to_string()),
                default: Some("prod".to_string()),
                defaults: vec![],
                available_values: vec![],
            }],
            widgets: vec![DatadogWidget {
                id: Some(1),
                definition: DatadogDefinition {
                    widget_type: "timeseries".to_string(),
                    title: Some("CPU Usage".to_string()),
                    requests: vec![DatadogRequest {
                        q: Some("avg:system.cpu.user{*}".to_string()),
                        aggregator: None,
                        queries: vec![],
                        formulas: vec![],
                        response_format: None,
                        display_type: None,
                        style: None,
                    }],
                    widgets: vec![],
                    yaxis: None,
                    content: None,
                    columns: None,
                    indexes: None,
                    query: None,
                    extra: serde_json::Map::new(),
                },
                layout: None,
            }],
            graphs: vec![],
        };

        let result = convert_dashboard(dd);
        assert_eq!(result.name, "Test Dashboard");
        assert_eq!(result.tabs.len(), 1);
        assert_eq!(result.tabs[0].name, "Overview");
        assert_eq!(result.tabs[0].widgets.len(), 1);
        assert_eq!(result.tabs[0].widgets[0].widget_type, "timeseries");
        assert_eq!(result.converted_count, 1);
        assert_eq!(result.skipped_count, 0);
        assert!(result.warnings.is_empty());
    }

    #[test]
    fn test_convert_dashboard_with_groups() {
        let dd = DatadogDashboard {
            title: "Grouped Dashboard".to_string(),
            description: None,
            layout_type: Some("ordered".to_string()),
            template_variables: vec![],
            widgets: vec![
                DatadogWidget {
                    id: Some(1),
                    definition: DatadogDefinition {
                        widget_type: "group".to_string(),
                        title: Some("CPU Metrics".to_string()),
                        requests: vec![],
                        widgets: vec![DatadogWidget {
                            id: Some(2),
                            definition: DatadogDefinition {
                                widget_type: "timeseries".to_string(),
                                title: Some("CPU Usage".to_string()),
                                requests: vec![DatadogRequest {
                                    q: Some("avg:system.cpu{*}".to_string()),
                                    aggregator: None,
                                    queries: vec![],
                                    formulas: vec![],
                                    response_format: None,
                                    display_type: None,
                                    style: None,
                                }],
                                widgets: vec![],
                                yaxis: None,
                                content: None,
                                columns: None,
                                indexes: None,
                                query: None,
                                extra: serde_json::Map::new(),
                            },
                            layout: None,
                        }],
                        yaxis: None,
                        content: None,
                        columns: None,
                        indexes: None,
                        query: None,
                        extra: serde_json::Map::new(),
                    },
                    layout: None,
                },
                DatadogWidget {
                    id: Some(3),
                    definition: DatadogDefinition {
                        widget_type: "note".to_string(),
                        title: Some("A note".to_string()),
                        requests: vec![],
                        widgets: vec![],
                        yaxis: None,
                        content: Some("Hello".to_string()),
                        columns: None,
                        indexes: None,
                        query: None,
                        extra: serde_json::Map::new(),
                    },
                    layout: None,
                },
            ],
            graphs: vec![],
        };

        let result = convert_dashboard(dd);
        // The note is a top-level non-group widget but it's skipped, so no "General" tab.
        // One tab for the group.
        assert_eq!(result.tabs.len(), 1);
        assert_eq!(result.tabs[0].name, "CPU Metrics");
        assert_eq!(result.tabs[0].widgets.len(), 1);
        assert_eq!(result.converted_count, 1);
        assert_eq!(result.skipped_count, 1);
        assert_eq!(result.warnings.len(), 1);
        assert!(result.warnings[0].contains("Note/text widgets"));
    }

    #[test]
    fn test_convert_log_stream_widget() {
        let dd = DatadogDashboard {
            title: "Logs".to_string(),
            description: None,
            layout_type: Some("ordered".to_string()),
            template_variables: vec![],
            widgets: vec![DatadogWidget {
                id: Some(1),
                definition: DatadogDefinition {
                    widget_type: "log_stream".to_string(),
                    title: Some("Application Logs".to_string()),
                    requests: vec![],
                    widgets: vec![],
                    yaxis: None,
                    content: None,
                    columns: None,
                    indexes: None,
                    query: Some("service:web status:error".to_string()),
                    extra: serde_json::Map::new(),
                },
                layout: None,
            }],
            graphs: vec![],
        };

        let result = convert_dashboard(dd);
        assert_eq!(result.tabs[0].widgets[0].widget_type, "table");
        assert_eq!(result.tabs[0].widgets[0].config["table"], "logs");
        assert_eq!(result.converted_count, 1);
    }

    #[test]
    fn test_parse_template_variable_in_query() {
        let q = "sum:kubernetes.cpu.usage.total{kube_cluster:$k8s_cluster} by {kube_cluster}";
        let parsed = parse_datadog_query(q);

        assert!(!parsed.needs_manual_review);
        assert_eq!(
            parsed.filters,
            vec![("kube_cluster".to_string(), "$k8s_cluster".to_string())]
        );
    }

    #[test]
    fn test_fixture_test_dashboard() {
        let json = include_str!("../../tests/fixtures/datadog/test_dashboard.json");
        let dd: DatadogDashboard = serde_json::from_str(json).expect("parse test_dashboard");
        let result = convert_dashboard(dd);

        assert_eq!(result.name, "System Metrics Dashboard");
        assert_eq!(result.tabs.len(), 1);
        assert_eq!(result.converted_count, 4); // timeseries, timeseries, toplist, query_value
        assert_eq!(result.skipped_count, 1); // note
        assert!(result.warnings.iter().any(|w| w.contains("Note/text")));
    }

    #[test]
    fn test_fixture_k8s_cluster_overview() {
        let json = include_str!("../../tests/fixtures/datadog/k8s_cluster_overview.json");
        let dd: DatadogDashboard = serde_json::from_str(json).expect("parse k8s_cluster_overview");
        let result = convert_dashboard(dd);

        assert_eq!(result.name, "Kubernetes - Cluster Overview");
        assert!(result.variables.len() >= 3); // k8s_cluster, namespace, node
                                              // Should have timeseries, query_value, toplist, log_stream widgets converted
        assert!(result.converted_count >= 7);
    }

    #[test]
    fn test_fixture_legacy_format() {
        let json = include_str!("../../tests/fixtures/datadog/legacy_format.json");
        let dd: DatadogDashboard = serde_json::from_str(json).expect("parse legacy_format");
        let result = convert_dashboard(dd);

        // Legacy format should still convert
        assert!(result.converted_count > 0);
        assert!(result
            .warnings
            .iter()
            .any(|w| w.contains("Legacy Datadog format")));
    }

    #[test]
    fn test_fixture_aws_ec2_optimization() {
        let json = include_str!("../../tests/fixtures/datadog/aws_ec2_optimization.json");
        let dd: DatadogDashboard = serde_json::from_str(json).expect("parse aws_ec2");
        let result = convert_dashboard(dd);

        assert_eq!(result.name, "Cloud Optimization - AWS EC2");
        // Should have multiple tabs from group widgets
        assert!(result.tabs.len() >= 3);
        // Should have warnings for unsupported types (heatmap, hostmap, scatterplot)
        assert!(result.warnings.iter().any(|w| w.contains("not supported")));
    }

    #[test]
    fn test_fixture_k8s_capacity_planning() {
        let json = include_str!("../../tests/fixtures/datadog/k8s_capacity_planning.json");
        let dd: DatadogDashboard = serde_json::from_str(json).expect("parse k8s_capacity_planning");
        let result = convert_dashboard(dd);

        assert_eq!(result.name, "Kubernetes Capacity Planning");
        // Has multiple group widgets -> multiple tabs
        assert!(result.tabs.len() >= 3);
        // Contains query_table widgets that should map to "table"
        assert!(result
            .tabs
            .iter()
            .any(|t| t.widgets.iter().any(|w| w.widget_type == "table")));
    }

    #[test]
    fn test_new_format_query_with_formulas() {
        let req = DatadogRequest {
            q: None,
            aggregator: None,
            queries: vec![
                DatadogStructuredQuery {
                    query: Some("avg:system.cpu.user{*}".to_string()),
                    data_source: Some("metrics".to_string()),
                    name: Some("query1".to_string()),
                    aggregator: Some("avg".to_string()),
                },
                DatadogStructuredQuery {
                    query: Some("avg:system.cpu.system{*}".to_string()),
                    data_source: Some("metrics".to_string()),
                    name: Some("query2".to_string()),
                    aggregator: Some("avg".to_string()),
                },
            ],
            formulas: vec![DatadogFormula {
                formula: Some("query1 + query2".to_string()),
                alias: Some("Total CPU".to_string()),
            }],
            response_format: None,
            display_type: None,
            style: None,
        };

        let config = convert_single_request(&req);
        assert!(config.get("description").is_some());
        let desc = config["description"].as_str().unwrap();
        assert!(desc.contains("Manual review"));
    }
}
