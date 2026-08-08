//! Session Profile types for the session evaluation system.
//!
//! Session profiles define criteria for which LLM sessions should be preserved
//! with full content (request/response bodies) for replay. A background evaluator
//! matches completed sessions against these profiles and tags matches.
//!
//! Filters use a virtual-field registry: each filter references a dotted path
//! (e.g. `errors.count`, `latency.avg_ms`, `tools.names`) that is resolved at
//! evaluation time against a [`SessionAggregates`] struct. New dimensions can
//! be added by extending the registry without touching the evaluation logic.

use std::collections::HashMap;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Virtual field registry
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldKind {
    Numeric,
    Set,
}

#[derive(Debug, Clone)]
pub enum FieldValue {
    Numeric(f64),
    Set(Vec<String>),
}

pub struct FieldDef {
    pub kind: FieldKind,
    pub namespace: &'static str,
    pub label: &'static str,
    pub unit: Option<&'static str>,
    extract: fn(&SessionAggregates) -> FieldValue,
}

impl FieldDef {
    pub fn extract(&self, agg: &SessionAggregates) -> FieldValue {
        (self.extract)(agg)
    }
}

/// Descriptor returned to the frontend so it can render the filter UI
/// without hardcoding the list of available fields.
#[derive(Debug, Clone, Serialize)]
pub struct FieldDescriptor {
    pub field: &'static str,
    pub kind: FieldKind,
    pub namespace: &'static str,
    pub label: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit: Option<&'static str>,
}

macro_rules! register_fields {
    ($map:ident, $( ($path:expr, $kind:expr, $ns:expr, $label:expr, $unit:expr, $extractor:expr) ),+ $(,)?) => {
        $(
            $map.insert($path, FieldDef {
                kind: $kind,
                namespace: $ns,
                label: $label,
                unit: $unit,
                extract: $extractor,
            });
        )+
    };
}

pub fn field_registry() -> &'static HashMap<&'static str, FieldDef> {
    static REGISTRY: OnceLock<HashMap<&str, FieldDef>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        let mut m = HashMap::new();
        register_fields!(
            m,
            (
                "errors.count",
                FieldKind::Numeric,
                "Errors",
                "count",
                None,
                |a: &SessionAggregates| FieldValue::Numeric(a.error_count as f64)
            ),
            (
                "latency.avg_ms",
                FieldKind::Numeric,
                "Latency",
                "avg_ms",
                Some("ms"),
                |a: &SessionAggregates| FieldValue::Numeric(a.avg_latency_ms as f64)
            ),
            (
                "latency.max_ms",
                FieldKind::Numeric,
                "Latency",
                "max_ms",
                Some("ms"),
                |a: &SessionAggregates| FieldValue::Numeric(a.max_latency_ms as f64)
            ),
            (
                "cost.total",
                FieldKind::Numeric,
                "Cost",
                "total",
                Some("USD"),
                |a: &SessionAggregates| FieldValue::Numeric(a.total_cost)
            ),
            (
                "cost.avg_per_call",
                FieldKind::Numeric,
                "Cost",
                "avg_per_call",
                Some("USD"),
                |a: &SessionAggregates| FieldValue::Numeric(a.avg_cost_per_call)
            ),
            (
                "model.names",
                FieldKind::Set,
                "Model",
                "names",
                None,
                |a: &SessionAggregates| FieldValue::Set(a.models.clone())
            ),
            (
                "provider.names",
                FieldKind::Set,
                "Provider",
                "names",
                None,
                |a: &SessionAggregates| FieldValue::Set(a.providers.clone())
            ),
            (
                "prompt.ids",
                FieldKind::Set,
                "Prompt",
                "ids",
                None,
                |a: &SessionAggregates| FieldValue::Set(a.prompt_config_ids.clone())
            ),
            (
                "fallback.count",
                FieldKind::Numeric,
                "Fallback",
                "count",
                None,
                |a: &SessionAggregates| FieldValue::Numeric(a.fallback_count as f64)
            ),
            (
                "guardrail.count",
                FieldKind::Numeric,
                "Guardrail",
                "count",
                None,
                |a: &SessionAggregates| FieldValue::Numeric(a.guardrail_count as f64)
            ),
            (
                "tools.count",
                FieldKind::Numeric,
                "Tools",
                "count",
                None,
                |a: &SessionAggregates| FieldValue::Numeric(a.tool_call_count as f64)
            ),
            (
                "tools.names",
                FieldKind::Set,
                "Tools",
                "names",
                None,
                |a: &SessionAggregates| FieldValue::Set(a.tool_names.clone())
            ),
            (
                "labels.names",
                FieldKind::Set,
                "Labels",
                "names",
                None,
                |a: &SessionAggregates| FieldValue::Set(a.labels.clone())
            ),
        );
        m
    })
}

/// Returns descriptors for every registered field, sorted by namespace then label.
pub fn available_fields() -> Vec<FieldDescriptor> {
    let reg = field_registry();
    let mut out: Vec<FieldDescriptor> = reg
        .iter()
        .map(|(path, def)| FieldDescriptor {
            field: path,
            kind: def.kind,
            namespace: def.namespace,
            label: def.label,
            unit: def.unit,
        })
        .collect();
    out.sort_by(|a, b| a.namespace.cmp(b.namespace).then(a.label.cmp(b.label)));
    out
}

/// Check that a field path exists in the registry.
pub fn is_valid_field(field: &str) -> bool {
    field_registry().contains_key(field)
}

// ---------------------------------------------------------------------------
// Backward-compat: map legacy `type` values to new `field` paths
// ---------------------------------------------------------------------------

fn legacy_type_to_field(legacy: &str) -> Option<&'static str> {
    match legacy {
        "has_errors" => Some("errors.count"),
        "avg_latency_ms" => Some("latency.avg_ms"),
        "max_latency_ms" => Some("latency.max_ms"),
        "avg_cost_per_call" => Some("cost.avg_per_call"),
        "total_cost" => Some("cost.total"),
        "model" => Some("model.names"),
        "provider" => Some("provider.names"),
        "prompt" => Some("prompt.ids"),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Profile & filter types
// ---------------------------------------------------------------------------

/// A named collection of filters with configurable AND/OR logic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionProfile {
    pub id: Uuid,
    pub name: String,
    #[serde(default = "default_logic")]
    pub logic: FilterLogic,
    #[serde(default)]
    pub filters: Vec<SessionFilter>,
}

/// How filters within a profile are combined.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum FilterLogic {
    And,
    Or,
}

fn default_logic() -> FilterLogic {
    FilterLogic::And
}

/// A single filter rule within a session profile.
///
/// Accepts both the new `field` key and the legacy `type` key for backward
/// compatibility. On deserialization the legacy form is transparently upgraded.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionFilter {
    /// Virtual field path (e.g. "errors.count", "latency.avg_ms", "tools.names").
    #[serde(default, alias = "type")]
    pub field: String,

    /// Comparison operator for numeric filters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub op: Option<ComparisonOp>,

    /// Value for the filter. Meaning depends on the field's kind:
    /// - Numeric: f64 threshold
    /// - Set: string to match (contains)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonOp {
    Lt,
    Lte,
    Gt,
    Gte,
}

/// Aggregated session data used for evaluating profile filters.
pub struct SessionAggregates {
    pub error_count: u64,
    pub avg_latency_ms: u32,
    pub max_latency_ms: u32,
    pub total_cost: f64,
    pub avg_cost_per_call: f64,
    pub providers: Vec<String>,
    pub models: Vec<String>,
    pub prompt_config_ids: Vec<String>,
    pub fallback_count: u64,
    pub guardrail_count: u64,
    pub tool_call_count: u64,
    pub tool_names: Vec<String>,
    pub labels: Vec<String>,
}

// ---------------------------------------------------------------------------
// Migration helper
// ---------------------------------------------------------------------------

/// Upgrade legacy filters in-place. Call after deserializing stored profiles.
pub fn migrate_profiles(profiles: &mut [SessionProfile]) {
    for profile in profiles.iter_mut() {
        for filter in profile.filters.iter_mut() {
            if let Some(new_field) = legacy_type_to_field(&filter.field) {
                let was_has_errors = filter.field == "has_errors";
                filter.field = new_field.to_string();
                if was_has_errors {
                    filter.op = Some(ComparisonOp::Gte);
                    filter.value = Some(serde_json::json!(1));
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

impl SessionProfile {
    /// Evaluate this profile against a session's aggregated data.
    pub fn matches(&self, agg: &SessionAggregates) -> bool {
        if self.filters.is_empty() {
            return false;
        }
        match self.logic {
            FilterLogic::And => self.filters.iter().all(|f| f.evaluate(agg)),
            FilterLogic::Or => self.filters.iter().any(|f| f.evaluate(agg)),
        }
    }
}

impl SessionFilter {
    fn evaluate(&self, agg: &SessionAggregates) -> bool {
        let registry = field_registry();
        let def = match registry.get(self.field.as_str()) {
            Some(d) => d,
            None => return false,
        };

        match def.extract(agg) {
            FieldValue::Numeric(actual) => self.compare_numeric(actual),
            FieldValue::Set(items) => self.match_string_list(&items),
        }
    }

    fn compare_numeric(&self, actual: f64) -> bool {
        let threshold = self.value.as_ref().and_then(|v| v.as_f64()).unwrap_or(0.0);
        match self.op.as_ref().unwrap_or(&ComparisonOp::Gte) {
            ComparisonOp::Lt => actual < threshold,
            ComparisonOp::Lte => actual <= threshold,
            ComparisonOp::Gt => actual > threshold,
            ComparisonOp::Gte => actual >= threshold,
        }
    }

    fn match_string_list(&self, items: &[String]) -> bool {
        let target = self.value.as_ref().and_then(|v| v.as_str()).unwrap_or("");
        if target.is_empty() {
            return false;
        }
        items.iter().any(|s| s == target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prof(name: &str, logic: FilterLogic, filters: Vec<SessionFilter>) -> SessionProfile {
        SessionProfile {
            id: Uuid::new_v4(),
            name: name.into(),
            logic,
            filters,
        }
    }

    fn filter(
        field: &str,
        op: Option<ComparisonOp>,
        value: Option<serde_json::Value>,
    ) -> SessionFilter {
        SessionFilter {
            field: field.into(),
            op,
            value,
        }
    }

    fn matched_names(profiles: &[SessionProfile], agg: &SessionAggregates) -> Vec<String> {
        profiles
            .iter()
            .filter(|p| p.matches(agg))
            .map(|p| p.name.clone())
            .collect()
    }

    fn default_agg() -> SessionAggregates {
        SessionAggregates {
            error_count: 0,
            avg_latency_ms: 0,
            max_latency_ms: 0,
            total_cost: 0.0,
            avg_cost_per_call: 0.0,
            providers: vec![],
            models: vec![],
            prompt_config_ids: vec![],
            fallback_count: 0,
            guardrail_count: 0,
            tool_call_count: 0,
            tool_names: vec![],
            labels: vec![],
        }
    }

    fn make_profiles() -> Vec<SessionProfile> {
        vec![
            prof(
                "has-errors",
                FilterLogic::And,
                vec![filter(
                    "errors.count",
                    Some(ComparisonOp::Gte),
                    Some(serde_json::json!(1)),
                )],
            ),
            prof(
                "high-cost",
                FilterLogic::And,
                vec![filter(
                    "cost.total",
                    Some(ComparisonOp::Gte),
                    Some(serde_json::json!(5.0)),
                )],
            ),
            prof(
                "slow",
                FilterLogic::And,
                vec![filter(
                    "latency.avg_ms",
                    Some(ComparisonOp::Gt),
                    Some(serde_json::json!(1000)),
                )],
            ),
            prof(
                "cheap-errors",
                FilterLogic::And,
                vec![
                    filter(
                        "errors.count",
                        Some(ComparisonOp::Gte),
                        Some(serde_json::json!(1)),
                    ),
                    filter(
                        "cost.total",
                        Some(ComparisonOp::Lt),
                        Some(serde_json::json!(1.0)),
                    ),
                ],
            ),
            prof(
                "uses-openai",
                FilterLogic::And,
                vec![filter(
                    "provider.names",
                    None,
                    Some(serde_json::json!("openai")),
                )],
            ),
            prof(
                "uses-gpt4",
                FilterLogic::And,
                vec![filter(
                    "model.names",
                    None,
                    Some(serde_json::json!("gpt-4o")),
                )],
            ),
            prof(
                "uses-claude",
                FilterLogic::And,
                vec![filter(
                    "model.names",
                    None,
                    Some(serde_json::json!("claude-sonnet")),
                )],
            ),
            prof("empty-filters", FilterLogic::And, vec![]),
        ]
    }

    #[test]
    fn error_session_matches_correct_profiles() {
        let agg = SessionAggregates {
            error_count: 3,
            avg_latency_ms: 2000,
            max_latency_ms: 8000,
            total_cost: 0.40,
            avg_cost_per_call: 0.04,
            providers: vec!["openai".into()],
            models: vec!["gpt-4o".into()],
            ..default_agg()
        };
        let profiles = make_profiles();
        let matched = matched_names(&profiles, &agg);

        assert!(matched.contains(&"has-errors".to_string()));
        assert!(matched.contains(&"slow".to_string()));
        assert!(matched.contains(&"cheap-errors".to_string()));
        assert!(matched.contains(&"uses-openai".to_string()));
        assert!(matched.contains(&"uses-gpt4".to_string()));
        assert!(!matched.contains(&"high-cost".to_string()));
        assert!(!matched.contains(&"uses-claude".to_string()));
        assert!(!matched.contains(&"empty-filters".to_string()));
    }

    #[test]
    fn expensive_session_matches_only_cost_profile() {
        let agg = SessionAggregates {
            error_count: 0,
            avg_latency_ms: 200,
            max_latency_ms: 400,
            total_cost: 12.50,
            avg_cost_per_call: 1.25,
            providers: vec!["anthropic".into()],
            models: vec!["claude-sonnet".into()],
            ..default_agg()
        };
        let profiles = make_profiles();
        let matched = matched_names(&profiles, &agg);

        assert!(matched.contains(&"high-cost".to_string()));
        assert!(matched.contains(&"uses-claude".to_string()));
        assert!(!matched.contains(&"has-errors".to_string()));
        assert!(!matched.contains(&"slow".to_string()));
        assert!(!matched.contains(&"cheap-errors".to_string()));
        assert!(!matched.contains(&"uses-openai".to_string()));
        assert!(!matched.contains(&"uses-gpt4".to_string()));
    }

    #[test]
    fn perfect_session_matches_nothing() {
        let agg = SessionAggregates {
            avg_latency_ms: 50,
            max_latency_ms: 100,
            total_cost: 0.01,
            avg_cost_per_call: 0.001,
            providers: vec!["local".into()],
            models: vec!["local-model".into()],
            ..default_agg()
        };
        let profiles = make_profiles();
        let matched = matched_names(&profiles, &agg);
        assert!(
            matched.is_empty(),
            "expected no matches, got: {:?}",
            matched
        );
    }

    #[test]
    fn multi_model_session() {
        let agg = SessionAggregates {
            avg_latency_ms: 500,
            max_latency_ms: 900,
            total_cost: 2.00,
            avg_cost_per_call: 0.20,
            providers: vec!["openai".into(), "anthropic".into()],
            models: vec!["gpt-4o".into(), "claude-sonnet".into()],
            ..default_agg()
        };
        let profiles = make_profiles();
        let matched = matched_names(&profiles, &agg);

        assert!(matched.contains(&"uses-openai".to_string()));
        assert!(matched.contains(&"uses-gpt4".to_string()));
        assert!(matched.contains(&"uses-claude".to_string()));
        assert!(!matched.contains(&"has-errors".to_string()));
        assert!(!matched.contains(&"slow".to_string()));
        assert!(!matched.contains(&"high-cost".to_string()));
    }

    #[test]
    fn or_vs_and_logic_with_same_filters() {
        let filters = vec![
            filter(
                "errors.count",
                Some(ComparisonOp::Gte),
                Some(serde_json::json!(1)),
            ),
            filter(
                "cost.total",
                Some(ComparisonOp::Gte),
                Some(serde_json::json!(100.0)),
            ),
        ];
        let and_profile = prof("and-profile", FilterLogic::And, filters.clone());
        let or_profile = prof("or-profile", FilterLogic::Or, filters);

        let agg = SessionAggregates {
            error_count: 1,
            avg_latency_ms: 100,
            max_latency_ms: 200,
            total_cost: 0.10,
            avg_cost_per_call: 0.01,
            ..default_agg()
        };
        let profiles = vec![and_profile, or_profile];
        let matched = matched_names(&profiles, &agg);

        assert!(
            !matched.contains(&"and-profile".to_string()),
            "AND should fail when only one filter passes"
        );
        assert!(
            matched.contains(&"or-profile".to_string()),
            "OR should pass when one filter passes"
        );
    }

    #[test]
    fn comparison_operator_boundaries() {
        let threshold = 1500;
        let profiles = vec![
            prof(
                "lt-1500",
                FilterLogic::And,
                vec![filter(
                    "latency.avg_ms",
                    Some(ComparisonOp::Lt),
                    Some(serde_json::json!(threshold)),
                )],
            ),
            prof(
                "lte-1500",
                FilterLogic::And,
                vec![filter(
                    "latency.avg_ms",
                    Some(ComparisonOp::Lte),
                    Some(serde_json::json!(threshold)),
                )],
            ),
            prof(
                "gt-1500",
                FilterLogic::And,
                vec![filter(
                    "latency.avg_ms",
                    Some(ComparisonOp::Gt),
                    Some(serde_json::json!(threshold)),
                )],
            ),
            prof(
                "gte-1500",
                FilterLogic::And,
                vec![filter(
                    "latency.avg_ms",
                    Some(ComparisonOp::Gte),
                    Some(serde_json::json!(threshold)),
                )],
            ),
        ];

        let agg_exact = SessionAggregates {
            avg_latency_ms: 1500,
            max_latency_ms: 1500,
            ..default_agg()
        };
        let matched = matched_names(&profiles, &agg_exact);
        assert!(
            !matched.contains(&"lt-1500".to_string()),
            "1500 is not < 1500"
        );
        assert!(matched.contains(&"lte-1500".to_string()), "1500 is <= 1500");
        assert!(
            !matched.contains(&"gt-1500".to_string()),
            "1500 is not > 1500"
        );
        assert!(matched.contains(&"gte-1500".to_string()), "1500 is >= 1500");

        let agg_below = SessionAggregates {
            avg_latency_ms: 1499,
            max_latency_ms: 1499,
            ..default_agg()
        };
        let matched = matched_names(&profiles, &agg_below);
        assert!(matched.contains(&"lt-1500".to_string()));
        assert!(matched.contains(&"lte-1500".to_string()));
        assert!(!matched.contains(&"gt-1500".to_string()));
        assert!(!matched.contains(&"gte-1500".to_string()));

        let agg_above = SessionAggregates {
            avg_latency_ms: 1501,
            max_latency_ms: 1501,
            ..default_agg()
        };
        let matched = matched_names(&profiles, &agg_above);
        assert!(!matched.contains(&"lt-1500".to_string()));
        assert!(!matched.contains(&"lte-1500".to_string()));
        assert!(matched.contains(&"gt-1500".to_string()));
        assert!(matched.contains(&"gte-1500".to_string()));
    }

    #[test]
    fn all_numeric_filter_types() {
        let profiles = vec![
            prof(
                "avg-latency",
                FilterLogic::And,
                vec![filter(
                    "latency.avg_ms",
                    Some(ComparisonOp::Gt),
                    Some(serde_json::json!(500)),
                )],
            ),
            prof(
                "max-latency",
                FilterLogic::And,
                vec![filter(
                    "latency.max_ms",
                    Some(ComparisonOp::Gt),
                    Some(serde_json::json!(3000)),
                )],
            ),
            prof(
                "total-cost",
                FilterLogic::And,
                vec![filter(
                    "cost.total",
                    Some(ComparisonOp::Gte),
                    Some(serde_json::json!(1.0)),
                )],
            ),
            prof(
                "avg-cost",
                FilterLogic::And,
                vec![filter(
                    "cost.avg_per_call",
                    Some(ComparisonOp::Gte),
                    Some(serde_json::json!(0.10)),
                )],
            ),
        ];
        let agg = SessionAggregates {
            avg_latency_ms: 800,
            max_latency_ms: 4000,
            total_cost: 2.50,
            avg_cost_per_call: 0.25,
            ..default_agg()
        };
        let matched = matched_names(&profiles, &agg);

        assert!(matched.contains(&"avg-latency".to_string()));
        assert!(matched.contains(&"max-latency".to_string()));
        assert!(matched.contains(&"total-cost".to_string()));
        assert!(matched.contains(&"avg-cost".to_string()));
    }

    #[test]
    fn prompt_filter() {
        let profiles = vec![
            prof(
                "uses-prompt-abc",
                FilterLogic::And,
                vec![filter(
                    "prompt.ids",
                    None,
                    Some(serde_json::json!("prompt-abc")),
                )],
            ),
            prof(
                "uses-prompt-xyz",
                FilterLogic::And,
                vec![filter(
                    "prompt.ids",
                    None,
                    Some(serde_json::json!("prompt-xyz")),
                )],
            ),
        ];
        let agg = SessionAggregates {
            prompt_config_ids: vec!["prompt-abc".into()],
            ..default_agg()
        };
        let matched = matched_names(&profiles, &agg);
        assert!(matched.contains(&"uses-prompt-abc".to_string()));
        assert!(!matched.contains(&"uses-prompt-xyz".to_string()));
    }

    #[test]
    fn edge_case_missing_value_defaults_to_zero_threshold() {
        let profiles = vec![prof(
            "no-value",
            FilterLogic::And,
            vec![filter("latency.avg_ms", None, None)],
        )];
        let agg = SessionAggregates {
            avg_latency_ms: 100,
            ..default_agg()
        };
        assert!(matched_names(&profiles, &agg).contains(&"no-value".to_string()));
    }

    #[test]
    fn edge_case_string_filter_with_empty_value_never_matches() {
        let profiles = vec![
            prof(
                "empty-model",
                FilterLogic::And,
                vec![filter("model.names", None, Some(serde_json::json!("")))],
            ),
            prof(
                "null-model",
                FilterLogic::And,
                vec![filter("model.names", None, None)],
            ),
        ];
        let agg = SessionAggregates {
            models: vec!["gpt-4o".into()],
            ..default_agg()
        };
        let matched = matched_names(&profiles, &agg);
        assert!(
            matched.is_empty(),
            "empty/null model value should never match"
        );
    }

    #[test]
    fn serde_round_trip() {
        let profile = SessionProfile {
            id: Uuid::new_v4(),
            name: "test".into(),
            logic: FilterLogic::Or,
            filters: vec![
                filter(
                    "latency.avg_ms",
                    Some(ComparisonOp::Gt),
                    Some(serde_json::json!(1000)),
                ),
                filter("provider.names", None, Some(serde_json::json!("openai"))),
            ],
        };
        let json = serde_json::to_string(&profile).unwrap();
        let back: SessionProfile = serde_json::from_str(&json).unwrap();
        assert_eq!(back.logic, FilterLogic::Or);
        assert_eq!(back.filters.len(), 2);
    }

    #[test]
    fn legacy_type_deserializes_via_alias() {
        let json = r#"{"type":"avg_latency_ms","op":"gt","value":500}"#;
        let f: SessionFilter = serde_json::from_str(json).unwrap();
        assert_eq!(f.field, "avg_latency_ms");
    }

    #[test]
    fn migrate_converts_legacy_types() {
        let mut profiles = vec![prof(
            "legacy",
            FilterLogic::And,
            vec![
                SessionFilter {
                    field: "has_errors".into(),
                    op: None,
                    value: None,
                },
                SessionFilter {
                    field: "avg_latency_ms".into(),
                    op: Some(ComparisonOp::Gt),
                    value: Some(serde_json::json!(1000)),
                },
                SessionFilter {
                    field: "model".into(),
                    op: None,
                    value: Some(serde_json::json!("gpt-4o")),
                },
            ],
        )];
        migrate_profiles(&mut profiles);
        assert_eq!(profiles[0].filters[0].field, "errors.count");
        assert_eq!(profiles[0].filters[0].op, Some(ComparisonOp::Gte));
        assert_eq!(profiles[0].filters[0].value, Some(serde_json::json!(1)));
        assert_eq!(profiles[0].filters[1].field, "latency.avg_ms");
        assert_eq!(profiles[0].filters[2].field, "model.names");
    }

    #[test]
    fn tools_filter_numeric() {
        let profiles = vec![prof(
            "many-tools",
            FilterLogic::And,
            vec![filter(
                "tools.count",
                Some(ComparisonOp::Gte),
                Some(serde_json::json!(3)),
            )],
        )];
        let agg = SessionAggregates {
            tool_call_count: 5,
            ..default_agg()
        };
        assert!(matched_names(&profiles, &agg).contains(&"many-tools".to_string()));

        let agg2 = SessionAggregates {
            tool_call_count: 1,
            ..default_agg()
        };
        assert!(matched_names(&profiles, &agg2).is_empty());
    }

    #[test]
    fn tools_filter_set() {
        let profiles = vec![prof(
            "uses-search",
            FilterLogic::And,
            vec![filter(
                "tools.names",
                None,
                Some(serde_json::json!("search")),
            )],
        )];
        let agg = SessionAggregates {
            tool_names: vec!["search".into(), "get".into()],
            ..default_agg()
        };
        assert!(matched_names(&profiles, &agg).contains(&"uses-search".to_string()));
    }

    #[test]
    fn fallback_and_guardrail_filters() {
        let profiles = vec![
            prof(
                "has-fallbacks",
                FilterLogic::And,
                vec![filter(
                    "fallback.count",
                    Some(ComparisonOp::Gte),
                    Some(serde_json::json!(1)),
                )],
            ),
            prof(
                "has-guardrails",
                FilterLogic::And,
                vec![filter(
                    "guardrail.count",
                    Some(ComparisonOp::Gte),
                    Some(serde_json::json!(1)),
                )],
            ),
        ];
        let agg = SessionAggregates {
            fallback_count: 2,
            guardrail_count: 0,
            ..default_agg()
        };
        let matched = matched_names(&profiles, &agg);
        assert!(matched.contains(&"has-fallbacks".to_string()));
        assert!(!matched.contains(&"has-guardrails".to_string()));
    }

    #[test]
    fn unknown_field_never_matches() {
        let profiles = vec![prof(
            "bogus",
            FilterLogic::And,
            vec![filter(
                "bogus.field",
                Some(ComparisonOp::Gte),
                Some(serde_json::json!(1)),
            )],
        )];
        let agg = default_agg();
        assert!(matched_names(&profiles, &agg).is_empty());
    }

    #[test]
    fn available_fields_returns_all_registered() {
        let fields = available_fields();
        assert!(fields.len() >= 13);
        assert!(fields.iter().any(|f| f.field == "tools.count"));
        assert!(fields.iter().any(|f| f.field == "errors.count"));
        assert!(fields.iter().any(|f| f.field == "fallback.count"));
        assert!(fields.iter().any(|f| f.field == "labels.names"));
    }

    #[test]
    fn migrate_is_idempotent() {
        let mut profiles = vec![prof(
            "legacy",
            FilterLogic::And,
            vec![SessionFilter {
                field: "has_errors".into(),
                op: None,
                value: None,
            }],
        )];
        migrate_profiles(&mut profiles);
        let field_after_first = profiles[0].filters[0].field.clone();
        let op_after_first = profiles[0].filters[0].op.clone();
        let value_after_first = profiles[0].filters[0].value.clone();

        migrate_profiles(&mut profiles);
        assert_eq!(profiles[0].filters[0].field, field_after_first);
        assert_eq!(profiles[0].filters[0].op, op_after_first);
        assert_eq!(profiles[0].filters[0].value, value_after_first);
    }

    #[test]
    fn migrate_leaves_new_style_filters_unchanged() {
        let mut profiles = vec![prof(
            "modern",
            FilterLogic::And,
            vec![
                filter(
                    "errors.count",
                    Some(ComparisonOp::Gte),
                    Some(serde_json::json!(5)),
                ),
                filter("tools.names", None, Some(serde_json::json!("search"))),
            ],
        )];
        migrate_profiles(&mut profiles);
        assert_eq!(profiles[0].filters[0].field, "errors.count");
        assert_eq!(profiles[0].filters[0].op, Some(ComparisonOp::Gte));
        assert_eq!(profiles[0].filters[0].value, Some(serde_json::json!(5)));
        assert_eq!(profiles[0].filters[1].field, "tools.names");
    }

    #[test]
    fn migrate_all_legacy_types() {
        let legacy_types = vec![
            ("has_errors", "errors.count"),
            ("avg_latency_ms", "latency.avg_ms"),
            ("max_latency_ms", "latency.max_ms"),
            ("avg_cost_per_call", "cost.avg_per_call"),
            ("total_cost", "cost.total"),
            ("model", "model.names"),
            ("provider", "provider.names"),
            ("prompt", "prompt.ids"),
        ];
        for (legacy, expected) in &legacy_types {
            let mut profiles = vec![prof(
                "test",
                FilterLogic::And,
                vec![SessionFilter {
                    field: legacy.to_string(),
                    op: Some(ComparisonOp::Gte),
                    value: Some(serde_json::json!(1)),
                }],
            )];
            migrate_profiles(&mut profiles);
            assert_eq!(
                profiles[0].filters[0].field, *expected,
                "Legacy type '{}' should migrate to '{}'",
                legacy, expected
            );
        }
    }

    #[test]
    fn field_registry_kinds_are_correct() {
        let reg = field_registry();

        let numeric_fields = [
            "errors.count",
            "latency.avg_ms",
            "latency.max_ms",
            "cost.total",
            "cost.avg_per_call",
            "fallback.count",
            "guardrail.count",
            "tools.count",
        ];
        for f in &numeric_fields {
            assert_eq!(
                reg.get(f).unwrap().kind,
                FieldKind::Numeric,
                "Field {} should be Numeric",
                f
            );
        }

        let set_fields = [
            "model.names",
            "provider.names",
            "prompt.ids",
            "tools.names",
            "labels.names",
        ];
        for f in &set_fields {
            assert_eq!(
                reg.get(f).unwrap().kind,
                FieldKind::Set,
                "Field {} should be Set",
                f
            );
        }
    }

    #[test]
    fn field_extraction_returns_correct_values() {
        let reg = field_registry();
        let agg = SessionAggregates {
            error_count: 7,
            avg_latency_ms: 250,
            max_latency_ms: 1200,
            total_cost: 3.14,
            avg_cost_per_call: 0.42,
            providers: vec!["openai".into()],
            models: vec!["gpt-4o".into()],
            prompt_config_ids: vec!["p1".into()],
            fallback_count: 2,
            guardrail_count: 1,
            tool_call_count: 10,
            tool_names: vec!["search".into(), "get".into()],
            labels: vec!["support".into()],
        };

        match reg.get("errors.count").unwrap().extract(&agg) {
            FieldValue::Numeric(v) => assert_eq!(v, 7.0),
            _ => panic!("Expected Numeric"),
        }
        match reg.get("tools.count").unwrap().extract(&agg) {
            FieldValue::Numeric(v) => assert_eq!(v, 10.0),
            _ => panic!("Expected Numeric"),
        }
        match reg.get("tools.names").unwrap().extract(&agg) {
            FieldValue::Set(v) => {
                assert!(v.contains(&"search".to_string()));
                assert!(v.contains(&"get".to_string()));
            }
            _ => panic!("Expected Set"),
        }
        match reg.get("cost.total").unwrap().extract(&agg) {
            FieldValue::Numeric(v) => assert!((v - 3.14).abs() < f64::EPSILON),
            _ => panic!("Expected Numeric"),
        }
    }

    #[test]
    fn combined_tool_and_error_profile() {
        let profiles = vec![prof(
            "error-with-tools",
            FilterLogic::And,
            vec![
                filter(
                    "errors.count",
                    Some(ComparisonOp::Gte),
                    Some(serde_json::json!(1)),
                ),
                filter(
                    "tools.count",
                    Some(ComparisonOp::Gte),
                    Some(serde_json::json!(1)),
                ),
                filter(
                    "tools.names",
                    None,
                    Some(serde_json::json!("dangerous_tool")),
                ),
            ],
        )];

        let agg_match = SessionAggregates {
            error_count: 2,
            tool_call_count: 3,
            tool_names: vec!["dangerous_tool".into(), "safe_tool".into()],
            ..default_agg()
        };
        assert!(matched_names(&profiles, &agg_match).contains(&"error-with-tools".to_string()));

        let agg_no_tool = SessionAggregates {
            error_count: 2,
            tool_call_count: 3,
            tool_names: vec!["safe_tool".into()],
            ..default_agg()
        };
        assert!(
            matched_names(&profiles, &agg_no_tool).is_empty(),
            "Should not match when dangerous_tool is not in tool_names"
        );

        let agg_no_errors = SessionAggregates {
            tool_call_count: 3,
            tool_names: vec!["dangerous_tool".into()],
            ..default_agg()
        };
        assert!(
            matched_names(&profiles, &agg_no_errors).is_empty(),
            "Should not match AND profile when error_count is 0"
        );
    }

    #[test]
    fn numeric_filter_with_zero_threshold() {
        let profiles = vec![prof(
            "any-tools",
            FilterLogic::And,
            vec![filter(
                "tools.count",
                Some(ComparisonOp::Gt),
                Some(serde_json::json!(0)),
            )],
        )];
        let agg_with_tools = SessionAggregates {
            tool_call_count: 1,
            ..default_agg()
        };
        assert!(matched_names(&profiles, &agg_with_tools).contains(&"any-tools".to_string()));

        let agg_no_tools = default_agg();
        assert!(matched_names(&profiles, &agg_no_tools).is_empty());
    }

    #[test]
    fn labels_filter_matches_when_label_present() {
        let profiles = vec![
            prof(
                "has-support",
                FilterLogic::And,
                vec![filter(
                    "labels.names",
                    None,
                    Some(serde_json::json!("support")),
                )],
            ),
            prof(
                "has-billing",
                FilterLogic::And,
                vec![filter(
                    "labels.names",
                    None,
                    Some(serde_json::json!("billing")),
                )],
            ),
        ];
        let agg = SessionAggregates {
            labels: vec!["support".into(), "onboarding".into()],
            ..default_agg()
        };
        let matched = matched_names(&profiles, &agg);
        assert!(matched.contains(&"has-support".to_string()));
        assert!(!matched.contains(&"has-billing".to_string()));
    }

    #[test]
    fn labels_filter_no_match_when_labels_empty() {
        let profiles = vec![prof(
            "has-support",
            FilterLogic::And,
            vec![filter(
                "labels.names",
                None,
                Some(serde_json::json!("support")),
            )],
        )];
        let agg = default_agg();
        assert!(matched_names(&profiles, &agg).is_empty());
    }

    #[test]
    fn labels_combined_with_other_filters() {
        let profiles = vec![
            prof(
                "costly-support",
                FilterLogic::And,
                vec![
                    filter("labels.names", None, Some(serde_json::json!("support"))),
                    filter(
                        "cost.total",
                        Some(ComparisonOp::Gte),
                        Some(serde_json::json!(5.0)),
                    ),
                ],
            ),
            prof(
                "support-or-expensive",
                FilterLogic::Or,
                vec![
                    filter("labels.names", None, Some(serde_json::json!("support"))),
                    filter(
                        "cost.total",
                        Some(ComparisonOp::Gte),
                        Some(serde_json::json!(100.0)),
                    ),
                ],
            ),
        ];

        let agg_cheap_support = SessionAggregates {
            labels: vec!["support".into()],
            total_cost: 1.0,
            ..default_agg()
        };
        let matched = matched_names(&profiles, &agg_cheap_support);
        assert!(
            !matched.contains(&"costly-support".to_string()),
            "AND: support present but cost too low"
        );
        assert!(
            matched.contains(&"support-or-expensive".to_string()),
            "OR: support label satisfies one branch"
        );

        let agg_expensive_support = SessionAggregates {
            labels: vec!["support".into()],
            total_cost: 10.0,
            ..default_agg()
        };
        let matched = matched_names(&profiles, &agg_expensive_support);
        assert!(
            matched.contains(&"costly-support".to_string()),
            "AND: both label and cost match"
        );
        assert!(
            matched.contains(&"support-or-expensive".to_string()),
            "OR: both branches match"
        );
    }

    #[test]
    fn labels_extraction_returns_correct_values() {
        let reg = field_registry();
        let agg = SessionAggregates {
            labels: vec!["support".into(), "billing".into()],
            ..default_agg()
        };
        match reg.get("labels.names").unwrap().extract(&agg) {
            FieldValue::Set(v) => {
                assert!(v.contains(&"support".to_string()));
                assert!(v.contains(&"billing".to_string()));
                assert_eq!(v.len(), 2);
            }
            _ => panic!("Expected Set"),
        }
    }
}
