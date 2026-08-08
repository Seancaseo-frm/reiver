use serde::{Deserialize, Serialize};

use super::migration::{ConvertedTab, ConvertedWidget, ImportResult};

// ---------------------------------------------------------------------------
// Grafana JSON structs (input)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GrafanaDashboardExport {
    #[serde(default)]
    pub dashboard: Option<GrafanaDashboardInner>,
    #[serde(default)]
    pub panels: Option<Vec<GrafanaPanel>>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub templating: Option<GrafanaTemplating>,
    #[serde(default)]
    pub rows: Option<Vec<GrafanaRow>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GrafanaDashboardInner {
    #[serde(default = "default_untitled")]
    pub title: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub panels: Vec<GrafanaPanel>,
    #[serde(default)]
    pub templating: Option<GrafanaTemplating>,
    #[serde(default)]
    pub rows: Option<Vec<GrafanaRow>>,
}

fn default_untitled() -> String {
    "Untitled Grafana Dashboard".to_string()
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GrafanaPanel {
    #[serde(default)]
    pub id: Option<i64>,
    #[serde(rename = "type", default)]
    pub panel_type: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub targets: Option<Vec<GrafanaTarget>>,
    #[serde(rename = "gridPos", default)]
    pub grid_pos: Option<GrafanaGridPos>,
    #[serde(default)]
    pub panels: Option<Vec<GrafanaPanel>>,
    #[serde(default)]
    pub options: Option<serde_json::Value>,
    #[serde(rename = "fieldConfig", default)]
    pub field_config: Option<serde_json::Value>,
    #[serde(default)]
    pub span: Option<f64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GrafanaTarget {
    #[serde(default)]
    pub expr: Option<String>,
    #[serde(rename = "legendFormat", default)]
    pub legend_format: Option<String>,
    #[serde(rename = "refId", default)]
    pub ref_id: Option<String>,
    #[serde(default)]
    pub instant: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GrafanaGridPos {
    #[serde(default)]
    pub x: i32,
    #[serde(default)]
    pub y: i32,
    #[serde(default = "default_width")]
    pub w: i32,
    #[serde(default = "default_height")]
    pub h: i32,
}

fn default_width() -> i32 {
    24
}
fn default_height() -> i32 {
    8
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GrafanaTemplating {
    #[serde(default)]
    pub list: Vec<GrafanaVariable>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GrafanaVariable {
    pub name: String,
    #[serde(rename = "type", default)]
    pub var_type: String,
    #[serde(default)]
    pub query: Option<serde_json::Value>,
    #[serde(default)]
    pub current: Option<serde_json::Value>,
    #[serde(default)]
    pub options: Option<Vec<serde_json::Value>>,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub hide: Option<i32>,
    #[serde(default)]
    pub multi: Option<bool>,
    #[serde(rename = "includeAll", default)]
    pub include_all: Option<bool>,
    #[serde(rename = "allValue", default)]
    pub all_value: Option<String>,
    #[serde(default)]
    pub regex: Option<String>,
    #[serde(default)]
    pub refresh: Option<serde_json::Value>,
}

/// Legacy Grafana format uses `rows` instead of flat `panels`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GrafanaRow {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub panels: Vec<GrafanaPanel>,
    #[serde(default)]
    pub collapse: Option<bool>,
}

// ---------------------------------------------------------------------------
// Conversion
// ---------------------------------------------------------------------------

pub fn convert_dashboard(export: GrafanaDashboardExport) -> ImportResult {
    let mut warnings: Vec<String> = Vec::new();
    let mut converted_count = 0_usize;
    let mut skipped_count = 0_usize;

    let (title, description, panels, templating, rows) = if let Some(inner) = export.dashboard {
        (
            inner.title,
            inner.description,
            inner.panels,
            inner.templating,
            inner.rows,
        )
    } else {
        (
            export.title.unwrap_or_else(default_untitled),
            export.description,
            export.panels.unwrap_or_default(),
            export.templating,
            export.rows,
        )
    };

    let variables = convert_variables(&templating);

    let all_panels = if panels.is_empty() {
        if let Some(row_list) = rows {
            warnings.push(
                "Legacy Grafana format (rows[] instead of panels[]). Conversion is best-effort."
                    .to_string(),
            );
            flatten_rows(row_list)
        } else {
            vec![]
        }
    } else {
        panels
    };

    let mut row_panels: Vec<(String, Vec<&GrafanaPanel>)> = Vec::new();
    let mut current_group: Vec<&GrafanaPanel> = Vec::new();
    let mut current_tab_name = "Overview".to_string();

    for panel in &all_panels {
        if panel.panel_type == "row" {
            if !current_group.is_empty() {
                row_panels.push((current_tab_name.clone(), std::mem::take(&mut current_group)));
            }
            current_tab_name = panel
                .title
                .clone()
                .unwrap_or_else(|| "Untitled".to_string());

            if let Some(nested) = &panel.panels {
                for nested_panel in nested {
                    current_group.push(nested_panel);
                }
            }
        } else if let Some(name) = is_section_header(panel) {
            if !current_group.is_empty() {
                row_panels.push((current_tab_name.clone(), std::mem::take(&mut current_group)));
            }
            current_tab_name = name;
        } else {
            current_group.push(panel);
        }
    }

    if !current_group.is_empty() {
        row_panels.push((current_tab_name, current_group));
    }

    if row_panels.is_empty() {
        row_panels.push(("Overview".to_string(), vec![]));
    }

    let mut tabs: Vec<ConvertedTab> = Vec::new();

    for (tab_name, tab_panels) in &row_panels {
        let mut tab_widgets = Vec::new();

        for panel in tab_panels {
            match convert_panel(panel) {
                PanelConversion::Converted(cw, panel_warnings) => {
                    converted_count += 1;
                    warnings.extend(panel_warnings);
                    tab_widgets.push(cw);
                }
                PanelConversion::Skipped(reason) => {
                    skipped_count += 1;
                    warnings.push(reason);
                }
            }
        }

        tabs.push(ConvertedTab {
            name: tab_name.clone(),
            icon: None,
            widgets: tab_widgets,
        });
    }

    ImportResult {
        name: title,
        description,
        variables,
        tabs,
        warnings,
        converted_count,
        skipped_count,
    }
}

// ---------------------------------------------------------------------------
// Variable conversion
// ---------------------------------------------------------------------------

/// Check whether a string is a Grafana-internal interpolation token that has
/// no meaning outside of Grafana (e.g. `$__auto_interval_interval`, `$_all`).
fn is_grafana_internal_token(s: &str) -> bool {
    s.starts_with("$__") || s.starts_with("$_") || s.starts_with("${__")
}

fn convert_variables(templating: &Option<GrafanaTemplating>) -> Vec<serde_json::Value> {
    let list = match templating {
        Some(t) => &t.list,
        None => return vec![],
    };

    list.iter()
        .filter(|v| v.var_type != "datasource")
        .map(|v| {
            let raw_default = v
                .current
                .as_ref()
                .and_then(|c| {
                    c.get("value")
                        .and_then(|val| val.as_str().map(|s| s.to_string()))
                })
                .unwrap_or_default();

            // Detect interval variables: explicit type or Grafana auto-interval tokens.
            let is_interval = v.var_type == "interval"
                || raw_default.starts_with("$__auto_interval");

            let effective_type = if is_interval {
                "interval"
            } else {
                &v.var_type
            };

            let default_value = if is_interval {
                if raw_default.is_empty() || is_grafana_internal_token(&raw_default) {
                    "5m".to_string()
                } else {
                    raw_default.clone()
                }
            } else if raw_default.is_empty() || is_grafana_internal_token(&raw_default) {
                ".*".to_string()
            } else {
                raw_default
            };

            let query_str = v.query.as_ref().map(|q| match q {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Object(obj) => obj
                    .get("query")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                other => other.to_string(),
            });

            let options: Vec<String> = v
                .options
                .as_ref()
                .map(|opts| {
                    opts.iter()
                        .filter_map(|o| {
                            o.get("value")
                                .and_then(|val| val.as_str().map(|s| s.to_string()))
                        })
                        .filter(|s| !is_grafana_internal_token(s))
                        .collect()
                })
                .unwrap_or_default();

            let mut obj = serde_json::json!({
                "name": v.name,
                "label": v.label.as_deref().unwrap_or(&v.name),
                "type": effective_type,
                "default": default_value,
            });

            if let Some(q) = query_str {
                obj["query"] = serde_json::Value::String(q);
            }
            if !options.is_empty() {
                obj["options"] = serde_json::json!(options);
            }
            if v.multi.unwrap_or(false) {
                obj["multi"] = serde_json::Value::Bool(true);
            }
            if v.include_all.unwrap_or(false) {
                obj["includeAll"] = serde_json::Value::Bool(true);
                let cleaned = match &v.all_value {
                    Some(val) if !val.is_empty() && !is_grafana_internal_token(val) => {
                        val.clone()
                    }
                    _ => ".*".to_string(),
                };
                obj["allValue"] = serde_json::Value::String(cleaned);
            } else if let Some(all_val) = &v.all_value {
                let cleaned = if all_val.is_empty() || is_grafana_internal_token(all_val) {
                    ".*".to_string()
                } else {
                    all_val.clone()
                };
                obj["allValue"] = serde_json::Value::String(cleaned);
            }

            obj
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Strip Prometheus-only labels from imported PromQL
// ---------------------------------------------------------------------------

/// Labels that are Prometheus scrape-config concepts with no OTel equivalent.
/// `job` is the main one; labels prefixed with `__` are Prometheus internal.
/// Labels like `namespace`, `instance`, `pod` are NOT stripped here -- they
/// have OTel equivalents and are handled by the label mapping layer.
const PROM_ONLY_LABELS: &[&str] = &["job"];

fn is_prom_internal_label(name: &str) -> bool {
    name.starts_with("__")
}

/// Strip Prometheus-only label matchers from a PromQL expression string.
/// Returns the cleaned expression and a list of warnings for labels removed.
fn strip_prom_only_labels(expr: &str) -> (String, Vec<String>) {
    let mut result = String::with_capacity(expr.len());
    let mut warnings = Vec::new();
    let mut pos = 0;

    while pos < expr.len() {
        if let Some(offset) = expr[pos..].find('{') {
            let brace_open = pos + offset;
            result.push_str(&expr[pos..=brace_open]);

            let after_open = brace_open + 1;
            let mut depth = 1u32;
            let mut i = after_open;
            let mut in_quotes = false;
            let bytes = expr.as_bytes();
            while i < bytes.len() && depth > 0 {
                if in_quotes {
                    if bytes[i] == b'"' && (i == 0 || bytes[i - 1] != b'\\') {
                        in_quotes = false;
                    }
                } else {
                    match bytes[i] {
                        b'"' => in_quotes = true,
                        b'{' => depth += 1,
                        b'}' => depth -= 1,
                        _ => {}
                    }
                }
                if depth > 0 {
                    i += 1;
                }
            }

            let inner = &expr[after_open..i];
            let filtered = filter_label_matchers(inner, &mut warnings);
            result.push_str(&filtered);
            result.push('}');
            pos = i + 1;
        } else {
            result.push_str(&expr[pos..]);
            break;
        }
    }

    (result, warnings)
}

/// Parse the inside of a `{...}` block and remove matchers on Prometheus-only
/// labels. Handles quoted values (with escaped characters) correctly.
fn filter_label_matchers(inner: &str, warnings: &mut Vec<String>) -> String {
    let matchers = split_label_matchers(inner);
    let mut kept = Vec::new();

    for m in matchers {
        let trimmed = m.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some(label_name) = extract_label_name(trimmed) {
            if PROM_ONLY_LABELS.contains(&label_name.as_str()) || is_prom_internal_label(&label_name) {
                warnings.push(format!(
                    "Stripped Prometheus-only label matcher: {}",
                    trimmed
                ));
                continue;
            }
        }
        kept.push(trimmed.to_string());
    }

    kept.join(", ")
}

/// Split label matchers on commas, respecting quoted strings.
fn split_label_matchers(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let bytes = s.as_bytes();
    let mut i = 0;
    let mut in_quotes = false;

    while i < bytes.len() {
        if bytes[i] == b'"' {
            if !in_quotes {
                in_quotes = true;
            } else if i > 0 && bytes[i - 1] != b'\\' {
                in_quotes = false;
            }
        } else if bytes[i] == b',' && !in_quotes {
            parts.push(&s[start..i]);
            start = i + 1;
        }
        i += 1;
    }
    parts.push(&s[start..]);
    parts
}

/// Extract the label name from a matcher string like `job="foo"` or `job=~".*"`.
fn extract_label_name(matcher: &str) -> Option<String> {
    let name_end = matcher
        .find(|c: char| c == '=' || c == '!' || c == '~')
        .unwrap_or(matcher.len());
    let name = matcher[..name_end].trim();
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

// ---------------------------------------------------------------------------
// Panel conversion
// ---------------------------------------------------------------------------

enum PanelConversion {
    Converted(ConvertedWidget, Vec<String>),
    Skipped(String),
}

fn convert_panel(panel: &GrafanaPanel) -> PanelConversion {
    let grafana_type = &panel.panel_type;
    let title = panel.title.clone();

    // Handle text panels as section headers (no PromQL needed)
    if grafana_type == "text" {
        let content = panel
            .options
            .as_ref()
            .and_then(|o| o.get("content"))
            .and_then(|c| c.as_str())
            .unwrap_or("")
            .to_string();

        if content.is_empty() {
            return PanelConversion::Skipped(format!(
                "Skipped panel '{}' (type: text): empty content",
                title.as_deref().unwrap_or("untitled"),
            ));
        }

        let (x, y, w, h) = if let Some(gp) = &panel.grid_pos {
            (
                gp.x / 2,
                gp.y,
                std::cmp::max(gp.w / 2, 1),
                std::cmp::min(gp.h, 2),
            )
        } else {
            (0, 0, 12, 1)
        };

        let config = serde_json::json!({ "content": content });
        return PanelConversion::Converted(ConvertedWidget {
            title,
            widget_type: "text".to_string(),
            config,
            x,
            y,
            w,
            h,
        }, vec![]);
    }

    let widget_type = match map_panel_type(grafana_type) {
        Some(wt) => wt,
        None => {
            return PanelConversion::Skipped(format!(
                "Skipped panel '{}' (type: {}): unsupported panel type",
                title.as_deref().unwrap_or("untitled"),
                grafana_type,
            ));
        }
    };

    let (x, y, w, h) = if let Some(gp) = &panel.grid_pos {
        let scaled_h = std::cmp::max(gp.h / 2, 1);
        (gp.x / 2, gp.y, std::cmp::max(gp.w / 2, 1), scaled_h)
    } else if let Some(span) = panel.span {
        let w = std::cmp::max((span as i32) / 2, 1);
        (0, 0, w, 3)
    } else {
        (0, 0, 6, 3)
    };

    let mut import_warnings: Vec<String> = Vec::new();
    let promql_exprs: Vec<serde_json::Value> = panel
        .targets
        .as_ref()
        .map(|targets| {
            targets
                .iter()
                .filter_map(|t| {
                    t.expr.as_ref().map(|expr| {
                        let (cleaned_expr, ws) = strip_prom_only_labels(expr);
                        for w in ws {
                            import_warnings.push(format!(
                                "Panel '{}': {}",
                                title.as_deref().unwrap_or("untitled"),
                                w
                            ));
                        }
                        let mut obj = serde_json::json!({ "promql": cleaned_expr });
                        if let Some(legend) = &t.legend_format {
                            obj["legend_format"] = serde_json::Value::String(legend.clone());
                        }
                        obj
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    // Only set instant for stat-like widgets. Timeseries panels should always
    // use range queries even if individual targets declare instant.
    let use_instant = matches!(widget_type, "stat")
        && panel
            .targets
            .as_ref()
            .map(|targets| targets.iter().any(|t| t.instant == Some(true)))
            .unwrap_or(false);

    if promql_exprs.is_empty() {
        return PanelConversion::Skipped(format!(
            "Skipped panel '{}' (type: {}): no PromQL targets found",
            title.as_deref().unwrap_or("untitled"),
            grafana_type,
        ));
    }

    let mut config = if promql_exprs.len() == 1 {
        let mut inner = promql_exprs[0].clone();
        if let Some(obj) = inner.as_object_mut() {
            add_display_options(obj, panel, widget_type);
            if use_instant {
                obj.insert("instant".to_string(), serde_json::Value::Bool(true));
            }
        }
        serde_json::json!({ "query": inner })
    } else {
        let mut inner = serde_json::json!({
            "queries": promql_exprs,
        });
        if let Some(first) = promql_exprs.first().and_then(|p| p.get("promql")) {
            inner["promql"] = first.clone();
        }
        if let Some(obj) = inner.as_object_mut() {
            add_display_options(obj, panel, widget_type);
            if use_instant {
                obj.insert("instant".to_string(), serde_json::Value::Bool(true));
            }
        }
        serde_json::json!({ "query": inner })
    };

    // Place unit at the top-level config so all widget types can read it
    // uniformly as config.unit, regardless of widget type.
    if let Some(unit) = extract_panel_unit(panel) {
        config["unit"] = serde_json::Value::String(unit);
    }

    PanelConversion::Converted(ConvertedWidget {
        widget_type: widget_type.to_string(),
        title,
        config,
        x,
        y,
        w,
        h,
    }, import_warnings)
}

fn map_panel_type(grafana_type: &str) -> Option<&'static str> {
    match grafana_type {
        "timeseries" | "graph" => Some("timeseries"),
        "stat" | "singlestat" => Some("stat"),
        "gauge" => Some("stat"),
        "bargauge" => Some("stat"),
        "table" | "table-old" => Some("table"),
        "histogram" => Some("histogram"),
        "piechart" | "piechart2" => Some("pie"),
        "barchart" => Some("bar"),
        "heatmap" => Some("timeseries"),
        "row" => None,
        "text" => None,
        "news" | "dashlist" | "alertlist" | "annolist" | "pluginlist" => None,
        _ => Some("timeseries"),
    }
}

fn add_display_options(
    obj: &mut serde_json::Map<String, serde_json::Value>,
    panel: &GrafanaPanel,
    widget_type: &str,
) {
    if widget_type == "stat" && panel.panel_type == "gauge" {
        obj.insert(
            "stat_type".to_string(),
            serde_json::Value::String("gauge".to_string()),
        );
    }
    if widget_type == "stat" && panel.panel_type == "bargauge" {
        obj.insert(
            "stat_type".to_string(),
            serde_json::Value::String("bargauge".to_string()),
        );
    }

    if let Some(options) = &panel.options {
        if let Some(legend) = options.get("legend") {
            if let Some(mode) = legend.get("displayMode").and_then(|m| m.as_str()) {
                if mode == "hidden" {
                    obj.insert("hideLegend".to_string(), serde_json::Value::Bool(true));
                }
            }
        }
        if let Some(tooltip) = options.get("tooltip") {
            if let Some(mode) = tooltip.get("mode").and_then(|m| m.as_str()) {
                if mode == "multi" {
                    obj.insert(
                        "tooltipMode".to_string(),
                        serde_json::Value::String("shared".to_string()),
                    );
                }
            }
        }
    }

    if let Some(field_config) = &panel.field_config {
        let defaults = field_config.get("defaults");

        // Note: `unit` is extracted separately by extract_panel_unit() and
        // placed at the top-level config. It is NOT inserted into the query
        // object so all widget types can read it uniformly as config.unit.

        if let Some(min) = defaults.and_then(|d| d.get("min")).and_then(|v| v.as_f64()) {
            obj.insert("min".to_string(), serde_json::json!(min));
        }
        if let Some(max) = defaults.and_then(|d| d.get("max")).and_then(|v| v.as_f64()) {
            obj.insert("max".to_string(), serde_json::json!(max));
        }

        let custom = defaults.and_then(|d| d.get("custom"));

        // Log scale: fieldConfig.defaults.custom.scaleDistribution.type == "log"
        if let Some(scale_type) = custom
            .and_then(|c| c.get("scaleDistribution"))
            .and_then(|sd| sd.get("type"))
            .and_then(|t| t.as_str())
        {
            if scale_type == "log" {
                let base = custom
                    .and_then(|c| c.get("scaleDistribution"))
                    .and_then(|sd| sd.get("log"))
                    .and_then(|l| l.as_u64())
                    .unwrap_or(2);
                obj.insert(
                    "yScale".to_string(),
                    serde_json::Value::String(format!("log{}", base)),
                );
            }
        }

        // Stacking: fieldConfig.defaults.custom.stacking.mode
        if let Some(stacking_mode) = custom
            .and_then(|c| c.get("stacking"))
            .and_then(|s| s.get("mode"))
            .and_then(|m| m.as_str())
        {
            if stacking_mode != "none" {
                obj.insert(
                    "stacking".to_string(),
                    serde_json::Value::String(stacking_mode.to_string()),
                );
            }
        }

        if let Some(steps) = defaults
            .and_then(|d| d.get("thresholds"))
            .and_then(|t| t.get("steps"))
            .and_then(|s| s.as_array())
        {
            let thresholds: Vec<serde_json::Value> = steps
                .iter()
                .filter_map(|step| {
                    let color = step.get("color")?.as_str()?;
                    let value = step.get("value").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    Some(serde_json::json!({
                        "value": value,
                        "color": color,
                    }))
                })
                .collect();
            if !thresholds.is_empty() {
                obj.insert("thresholds".to_string(), serde_json::json!(thresholds));
            }
        }
    }
}

/// Extract the display unit from a Grafana panel's fieldConfig.
fn extract_panel_unit(panel: &GrafanaPanel) -> Option<String> {
    panel
        .field_config
        .as_ref()?
        .get("defaults")?
        .get("unit")?
        .as_str()
        .map(|s| s.to_string())
}

// ---------------------------------------------------------------------------
// Section header detection — full-width text panels with heading content
// ---------------------------------------------------------------------------

fn is_section_header(panel: &GrafanaPanel) -> Option<String> {
    if panel.panel_type != "text" {
        return None;
    }
    let is_full_width = panel
        .grid_pos
        .as_ref()
        .map(|gp| gp.w >= 20)
        .unwrap_or(false);
    if !is_full_width {
        return None;
    }
    let content = panel.options.as_ref()?.get("content")?.as_str()?;
    extract_heading_text(content)
}

fn extract_heading_text(html: &str) -> Option<String> {
    // Match <h1>...</h1>, <h2>...</h2>, etc., stripping inner HTML attributes/tags
    for tag in &["h1", "h2", "h3"] {
        if let Some(start) = html.find(&format!("<{}", tag)) {
            if let Some(close_bracket) = html[start..].find('>') {
                let after_tag = start + close_bracket + 1;
                if let Some(end_tag) = html[after_tag..].find(&format!("</{}", tag)) {
                    let inner = &html[after_tag..after_tag + end_tag];
                    let text = inner
                        .replace("<br>", " ")
                        .replace("<br/>", " ")
                        .replace("<br />", " ");
                    // Strip any remaining HTML tags
                    let mut clean = String::new();
                    let mut in_tag = false;
                    for ch in text.chars() {
                        if ch == '<' {
                            in_tag = true;
                        } else if ch == '>' {
                            in_tag = false;
                        } else if !in_tag {
                            clean.push(ch);
                        }
                    }
                    let trimmed = clean.trim().to_string();
                    if !trimmed.is_empty() {
                        return Some(trimmed);
                    }
                }
            }
        }
    }
    // Fallback: markdown headings like "### Title"
    for line in html.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix('#') {
            let text = rest.trim_start_matches('#').trim();
            if !text.is_empty() {
                return Some(text.to_string());
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Legacy row format flattening
// ---------------------------------------------------------------------------

fn flatten_rows(rows: Vec<GrafanaRow>) -> Vec<GrafanaPanel> {
    let mut panels = Vec::new();
    let mut y_offset = 0;

    for row in rows {
        panels.push(GrafanaPanel {
            id: None,
            panel_type: "row".to_string(),
            title: row.title.clone(),
            targets: None,
            grid_pos: Some(GrafanaGridPos {
                x: 0,
                y: y_offset,
                w: 24,
                h: 1,
            }),
            panels: None,
            options: None,
            field_config: None,
            span: None,
        });
        y_offset += 1;

        for p in row.panels {
            let span = p.span.unwrap_or(12.0);
            let w = std::cmp::max((span * 2.0) as i32, 1);
            let h = 6;
            panels.push(GrafanaPanel {
                grid_pos: p.grid_pos.clone().or(Some(GrafanaGridPos {
                    x: 0,
                    y: y_offset,
                    w,
                    h,
                })),
                ..p
            });
            y_offset += h;
        }
    }

    panels
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_var(name: &str, var_type: &str, current: Option<serde_json::Value>,
                include_all: Option<bool>, all_value: Option<String>,
                options: Option<Vec<serde_json::Value>>) -> GrafanaVariable {
        GrafanaVariable {
            name: name.to_string(),
            label: Some(name.to_string()),
            var_type: var_type.to_string(),
            query: None,
            current,
            include_all,
            multi: None,
            all_value,
            options,
            hide: None,
            refresh: None,
            regex: None,
        }
    }

    #[test]
    fn convert_variables_filters_grafana_tokens_from_options() {
        let templating = Some(GrafanaTemplating {
            list: vec![test_var(
                "health_status", "custom",
                Some(serde_json::json!({"value": ".*"})),
                Some(true),
                Some(".*".to_string()),
                Some(vec![
                    serde_json::json!({"value": "$__all", "text": "All"}),
                    serde_json::json!({"value": "Healthy", "text": "Healthy"}),
                    serde_json::json!({"value": "Degraded", "text": "Degraded"}),
                ]),
            )],
        });

        let vars = convert_variables(&templating);
        assert_eq!(vars.len(), 1);

        let opts = vars[0]["options"].as_array().unwrap();
        let opt_values: Vec<&str> = opts.iter().map(|v| v.as_str().unwrap()).collect();
        assert_eq!(opt_values, vec!["Healthy", "Degraded"]);
        assert!(!opt_values.contains(&"$__all"));
    }

    #[test]
    fn convert_variables_filters_auto_interval_token() {
        let templating = Some(GrafanaTemplating {
            list: vec![test_var(
                "interval", "interval",
                Some(serde_json::json!({"value": "$__auto_interval_interval"})),
                None,
                None,
                Some(vec![
                    serde_json::json!({"value": "$__auto_interval_interval", "text": "auto"}),
                    serde_json::json!({"value": "1m", "text": "1m"}),
                    serde_json::json!({"value": "5m", "text": "5m"}),
                ]),
            )],
        });

        let vars = convert_variables(&templating);
        assert_eq!(vars.len(), 1);
        assert_eq!(vars[0]["default"], "5m");

        let opts = vars[0]["options"].as_array().unwrap();
        let opt_values: Vec<&str> = opts.iter().map(|v| v.as_str().unwrap()).collect();
        assert_eq!(opt_values, vec!["1m", "5m"]);
    }

    #[test]
    fn is_grafana_internal_token_detects_all_forms() {
        assert!(is_grafana_internal_token("$__all"));
        assert!(is_grafana_internal_token("$__auto_interval_interval"));
        assert!(is_grafana_internal_token("$_all"));
        assert!(is_grafana_internal_token("${__auto}"));
        assert!(!is_grafana_internal_token("argocd"));
        assert!(!is_grafana_internal_token("Healthy"));
        assert!(!is_grafana_internal_token("5m"));
    }
}
