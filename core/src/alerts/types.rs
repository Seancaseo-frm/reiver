//! Alert system types and data structures.
//! Simplified HyperDX-style model with OK/ALERT states and single thresholds.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;
use uuid::Uuid;

/// Deserialize a `String` treating both absent and `null` as `String::default()`.
fn null_as_default_string<'de, D: Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    Option::<String>::deserialize(d).map(|o| o.unwrap_or_default())
}

/// Deserialize a `Vec<T>` treating both absent and `null` as an empty vec.
fn null_as_default_vec<'de, D, T>(d: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<Vec<T>>::deserialize(d).map(|o| o.unwrap_or_default())
}

// ============================================================================
// Validation
// ============================================================================

/// Errors that can occur during alert configuration validation.
#[derive(Debug, Error)]
pub enum AlertValidationError {
    #[error("Invalid aggregation function: '{0}'. Must be one of: avg, sum, min, max, count, any, anyLast, median, stddevPop, stddevSamp, varPop, varSamp, quantile, quantileExact")]
    InvalidAggregation(String),
}

/// Valid ClickHouse aggregation functions for alert queries.
///
/// This allowlist prevents SQL injection by ensuring only known-safe
/// function names are interpolated into queries.
const VALID_AGGREGATION_FUNCTIONS: &[&str] = &[
    "avg",
    "sum",
    "min",
    "max",
    "count",
    "any",
    "anylast",
    "median",
    "stddevpop",
    "stddevsamp",
    "varpop",
    "varsamp",
    "quantile",
    "quantileexact",
];

/// Validate that an aggregation function name is in the allowlist.
///
/// # Security
/// This function prevents SQL injection by validating user-provided
/// aggregation function names against a known-safe allowlist before
/// they are interpolated into SQL queries.
///
/// # Arguments
/// * `name` - The aggregation function name to validate
///
/// # Returns
/// * `Ok(())` if the function name is valid
/// * `Err(AlertValidationError::InvalidAggregation)` if invalid
pub fn validate_aggregation_function(name: &str) -> Result<(), AlertValidationError> {
    let normalized = name.to_lowercase();
    if !VALID_AGGREGATION_FUNCTIONS.contains(&normalized.as_str()) {
        return Err(AlertValidationError::InvalidAggregation(name.to_string()));
    }
    Ok(())
}

/// Alert rule type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RuleType {
    Threshold,
}

impl Default for RuleType {
    fn default() -> Self {
        RuleType::Threshold
    }
}

/// Alert state - simplified to OK and ALERT (using Firing for notifications)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AlertState {
    /// Alert is not firing (OK state)
    Ok,
    /// Alert is actively firing
    Firing,
}

impl AlertState {
    pub fn as_str(&self) -> &'static str {
        match self {
            AlertState::Ok => "OK",
            AlertState::Firing => "ALERT",
        }
    }
}

impl std::fmt::Display for AlertState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Query configuration for alert rules.
///
/// Internally tagged by `query_type`. Legacy JSONB rows without a `query_type`
/// field are inferred from other fields during deserialization.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "query_type")]
pub enum AlertQueryConfig {
    /// OTel / infrastructure metric alert
    #[serde(rename = "metrics")]
    Metrics {
        metric_name: String,
        #[serde(default)]
        filters: BTreeMap<String, String>,
        #[serde(default)]
        group_by: Vec<String>,
        #[serde(default = "default_time_aggregation")]
        time_aggregation: String,
        #[serde(default = "default_space_aggregation")]
        space_aggregation: String,
    },
    /// Log pattern count alert
    #[serde(rename = "log_pattern")]
    LogPattern {
        patterns: Vec<String>,
        #[serde(default = "default_log_source")]
        log_source: String,
    },
    /// Raw PromQL expression alert
    #[serde(rename = "promql")]
    PromQL { promql: String },
    /// LLM gateway metric alert (queries `llm_requests` table)
    #[serde(rename = "llm")]
    Llm {
        /// The metric name with `llm.` prefix (e.g. `llm.error_rate`)
        metric_name: String,
        #[serde(default)]
        filters: BTreeMap<String, String>,
        #[serde(default)]
        llm_metric: Option<String>,
        #[serde(default)]
        llm_model: Option<String>,
        #[serde(default)]
        llm_score_name: Option<String>,
    },
}

impl AlertQueryConfig {
    pub fn metric_name(&self) -> Option<&str> {
        match self {
            Self::Metrics { metric_name, .. } | Self::Llm { metric_name, .. } => {
                Some(metric_name.as_str())
            }
            _ => None,
        }
    }
}

impl Default for AlertQueryConfig {
    fn default() -> Self {
        Self::Metrics {
            metric_name: String::new(),
            filters: BTreeMap::new(),
            group_by: Vec::new(),
            time_aggregation: default_time_aggregation(),
            space_aggregation: default_space_aggregation(),
        }
    }
}

/// Backward-compatible deserialization: if `query_type` is present, use it as
/// the serde tag. If absent, infer the variant from populated fields.
impl<'de> Deserialize<'de> for AlertQueryConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        let raw: serde_json::Value = serde_json::Value::deserialize(deserializer)?;
        let obj = raw.as_object().ok_or_else(|| D::Error::custom("expected object"))?;

        let query_type = obj
            .get("query_type")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        match query_type {
            "metrics" | "log_pattern" | "promql" | "llm" => {
                // Has an explicit tag -- let serde's tagged-enum logic handle it.
                #[derive(Deserialize)]
                #[serde(tag = "query_type")]
                enum Tagged {
                    #[serde(rename = "metrics")]
                    Metrics {
                        #[serde(default, deserialize_with = "null_as_default_string")]
                        metric_name: String,
                        #[serde(default)]
                        filters: BTreeMap<String, String>,
                        #[serde(default)]
                        group_by: Vec<String>,
                        #[serde(default = "default_time_aggregation")]
                        time_aggregation: String,
                        #[serde(default = "default_space_aggregation")]
                        space_aggregation: String,
                    },
                    #[serde(rename = "log_pattern")]
                    LogPattern {
                        #[serde(default, deserialize_with = "null_as_default_vec")]
                        patterns: Vec<String>,
                        #[serde(default = "default_log_source")]
                        log_source: String,
                    },
                    #[serde(rename = "promql")]
                    PromQL {
                        #[serde(default, deserialize_with = "null_as_default_string")]
                        promql: String,
                    },
                    #[serde(rename = "llm")]
                    Llm {
                        #[serde(default, deserialize_with = "null_as_default_string")]
                        metric_name: String,
                        #[serde(default)]
                        filters: BTreeMap<String, String>,
                        #[serde(default)]
                        llm_metric: Option<String>,
                        #[serde(default)]
                        llm_model: Option<String>,
                        #[serde(default)]
                        llm_score_name: Option<String>,
                    },
                }
                let tagged: Tagged =
                    serde_json::from_value(raw).map_err(D::Error::custom)?;
                Ok(match tagged {
                    Tagged::Metrics { metric_name, filters, group_by, time_aggregation, space_aggregation } =>
                        Self::Metrics { metric_name, filters, group_by, time_aggregation, space_aggregation },
                    Tagged::LogPattern { patterns, log_source } =>
                        Self::LogPattern { patterns, log_source },
                    Tagged::PromQL { promql } => Self::PromQL { promql },
                    Tagged::Llm { metric_name, filters, llm_metric, llm_model, llm_score_name } =>
                        Self::Llm { metric_name, filters, llm_metric, llm_model, llm_score_name },
                })
            }
            _ => {
                // No tag -- infer from fields (backward compat for legacy JSONB rows)
                let has_promql = obj
                    .get("promql")
                    .and_then(|v| v.as_str())
                    .is_some_and(|s| !s.trim().is_empty());
                let has_patterns = obj
                    .get("patterns")
                    .and_then(|v| v.as_array())
                    .is_some_and(|a| !a.is_empty());
                let metric_name_str = obj
                    .get("metric_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                if has_promql {
                    let promql = obj["promql"].as_str().unwrap_or("").to_string();
                    Ok(Self::PromQL { promql })
                } else if has_patterns {
                    let patterns: Vec<String> = obj
                        .get("patterns")
                        .cloned()
                        .and_then(|v| serde_json::from_value(v).ok())
                        .unwrap_or_default();
                    let log_source = obj
                        .get("log_source")
                        .and_then(|v| v.as_str())
                        .unwrap_or("all")
                        .to_string();
                    Ok(Self::LogPattern { patterns, log_source })
                } else if metric_name_str.starts_with("llm.") {
                    let filters: BTreeMap<String, String> = obj
                        .get("filters")
                        .cloned()
                        .and_then(|v| serde_json::from_value(v).ok())
                        .unwrap_or_default();
                    let llm_metric = obj.get("llm_metric").and_then(|v| v.as_str()).map(String::from);
                    let llm_model = obj.get("llm_model").and_then(|v| v.as_str()).map(String::from);
                    let llm_score_name = obj.get("llm_score_name").and_then(|v| v.as_str()).map(String::from);
                    Ok(Self::Llm {
                        metric_name: metric_name_str.to_string(),
                        filters,
                        llm_metric,
                        llm_model,
                        llm_score_name,
                    })
                } else {
                    let filters: BTreeMap<String, String> = obj
                        .get("filters")
                        .cloned()
                        .and_then(|v| serde_json::from_value(v).ok())
                        .unwrap_or_default();
                    let group_by: Vec<String> = obj
                        .get("group_by")
                        .cloned()
                        .and_then(|v| serde_json::from_value(v).ok())
                        .unwrap_or_default();
                    let time_aggregation = obj
                        .get("time_aggregation")
                        .and_then(|v| v.as_str())
                        .unwrap_or("avg")
                        .to_string();
                    let space_aggregation = obj
                        .get("space_aggregation")
                        .and_then(|v| v.as_str())
                        .unwrap_or("sum")
                        .to_string();
                    Ok(Self::Metrics {
                        metric_name: metric_name_str.to_string(),
                        filters,
                        group_by,
                        time_aggregation,
                        space_aggregation,
                    })
                }
            }
        }
    }
}

fn default_time_aggregation() -> String {
    "avg".to_string()
}

fn default_space_aggregation() -> String {
    "sum".to_string()
}

fn default_log_source() -> String {
    "all".to_string()
}

/// An alert rule definition - simplified HyperDX-style model
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRule {
    pub id: Uuid,
    pub project_id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub rule_type: RuleType,
    pub query_config: AlertQueryConfig,

    /// Single threshold value
    pub threshold: f64,
    /// Threshold comparison: "above" or "below"
    pub threshold_type: String,
    /// Notification channel UUIDs
    pub notification_channels: Vec<Uuid>,

    /// Alert on absent data
    pub alert_on_absent: bool,
    pub absent_for_seconds: i32,

    /// Evaluation settings
    pub eval_window_seconds: i32,
    pub eval_interval_seconds: i32,

    /// Labels and annotations
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,

    /// State
    pub enabled: bool,
    pub last_evaluated_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl AlertRule {
    /// Get notification channels
    #[allow(dead_code)] // Helper for future notification integration
    pub fn get_all_channels(&self) -> Vec<Uuid> {
        self.notification_channels.clone()
    }

    /// Check if threshold is exceeded
    #[allow(dead_code)] // Helper for alert evaluation logic
    pub fn check_threshold(&self, value: f64) -> bool {
        match self.threshold_type.as_str() {
            "above" => value > self.threshold,
            "below" => value < self.threshold,
            _ => value > self.threshold, // default to above
        }
    }
}

/// Compute a fingerprint for alert grouping from labels
#[allow(dead_code)] // Helper for alert deduplication
pub fn compute_alert_fingerprint(labels: &BTreeMap<String, String>) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    for (key, value) in labels {
        key.hash(&mut hasher);
        value.hash(&mut hasher);
    }
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fingerprint_consistency() {
        let mut labels = BTreeMap::new();
        labels.insert("env".to_string(), "prod".to_string());
        labels.insert("host".to_string(), "web-1".to_string());

        let fp1 = compute_alert_fingerprint(&labels);
        let fp2 = compute_alert_fingerprint(&labels);

        assert_eq!(fp1, fp2);
    }

    #[test]
    fn test_validate_aggregation_function_valid() {
        // All valid functions should pass
        let valid_functions = [
            "avg",
            "sum",
            "min",
            "max",
            "count",
            "any",
            "anyLast",
            "median",
            "stddevPop",
            "stddevSamp",
            "varPop",
            "varSamp",
            "quantile",
            "quantileExact",
            // Case insensitive
            "AVG",
            "SUM",
            "Min",
            "MAX",
        ];

        for func in valid_functions {
            assert!(
                validate_aggregation_function(func).is_ok(),
                "Expected '{}' to be valid",
                func
            );
        }
    }

    #[test]
    fn test_deserialize_tagged_metrics() {
        let json = r#"{"query_type":"metrics","metric_name":"http.requests","filters":{},"time_aggregation":"sum","space_aggregation":"avg"}"#;
        let config: AlertQueryConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(config, AlertQueryConfig::Metrics { .. }));
        assert_eq!(config.metric_name(), Some("http.requests"));
    }

    #[test]
    fn test_deserialize_tagged_log_pattern() {
        let json = r#"{"query_type":"log_pattern","patterns":["error","panic"],"log_source":"otlp"}"#;
        let config: AlertQueryConfig = serde_json::from_str(json).unwrap();
        match &config {
            AlertQueryConfig::LogPattern { patterns, log_source } => {
                assert_eq!(patterns, &["error", "panic"]);
                assert_eq!(log_source, "otlp");
            }
            _ => panic!("expected LogPattern"),
        }
    }

    #[test]
    fn test_deserialize_tagged_promql() {
        let json = r#"{"query_type":"promql","promql":"sum(rate(http_requests_total[5m]))"}"#;
        let config: AlertQueryConfig = serde_json::from_str(json).unwrap();
        match &config {
            AlertQueryConfig::PromQL { promql } => {
                assert_eq!(promql, "sum(rate(http_requests_total[5m]))");
            }
            _ => panic!("expected PromQL"),
        }
    }

    #[test]
    fn test_deserialize_tagged_llm() {
        let json = r#"{"query_type":"llm","metric_name":"llm.error_rate","filters":{"model":"gpt-4o"}}"#;
        let config: AlertQueryConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(config, AlertQueryConfig::Llm { .. }));
        assert_eq!(config.metric_name(), Some("llm.error_rate"));
    }

    #[test]
    fn test_deserialize_legacy_metric_no_tag() {
        let json = r#"{"metric_name":"cpu.utilization","filters":{},"time_aggregation":"avg","space_aggregation":"sum"}"#;
        let config: AlertQueryConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(config, AlertQueryConfig::Metrics { .. }));
        assert_eq!(config.metric_name(), Some("cpu.utilization"));
    }

    #[test]
    fn test_deserialize_legacy_llm_prefix_no_tag() {
        let json = r#"{"metric_name":"llm.latency_p95","filters":{"model":"gpt-4o"}}"#;
        let config: AlertQueryConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(config, AlertQueryConfig::Llm { .. }));
    }

    #[test]
    fn test_deserialize_legacy_patterns_no_tag() {
        let json = r#"{"patterns":["error"],"log_source":"all"}"#;
        let config: AlertQueryConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(config, AlertQueryConfig::LogPattern { .. }));
    }

    #[test]
    fn test_deserialize_legacy_promql_no_tag() {
        let json = r#"{"promql":"up == 0"}"#;
        let config: AlertQueryConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(config, AlertQueryConfig::PromQL { .. }));
    }

    #[test]
    fn test_deserialize_empty_defaults_to_metrics() {
        let json = r#"{}"#;
        let config: AlertQueryConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(config, AlertQueryConfig::Metrics { .. }));
    }

    #[test]
    fn test_roundtrip_serialization() {
        let config = AlertQueryConfig::PromQL { promql: "up".to_string() };
        let json = serde_json::to_string(&config).unwrap();
        let parsed: AlertQueryConfig = serde_json::from_str(&json).unwrap();
        match parsed {
            AlertQueryConfig::PromQL { promql } => assert_eq!(promql, "up"),
            _ => panic!("roundtrip failed"),
        }
    }

    #[test]
    fn test_tagged_metrics_missing_metric_name_defaults_to_empty() {
        let json = r#"{"query_type":"metrics"}"#;
        let config: AlertQueryConfig = serde_json::from_str(json).unwrap();
        match config {
            AlertQueryConfig::Metrics { metric_name, .. } => assert_eq!(metric_name, ""),
            _ => panic!("expected Metrics"),
        }
    }

    #[test]
    fn test_tagged_promql_null_promql_defaults_to_empty() {
        let json = r#"{"query_type":"promql","promql":null}"#;
        let config: AlertQueryConfig = serde_json::from_str(json).unwrap();
        match config {
            AlertQueryConfig::PromQL { promql } => assert_eq!(promql, ""),
            _ => panic!("expected PromQL"),
        }
    }

    #[test]
    fn test_tagged_log_pattern_missing_patterns_defaults_to_empty() {
        let json = r#"{"query_type":"log_pattern"}"#;
        let config: AlertQueryConfig = serde_json::from_str(json).unwrap();
        match config {
            AlertQueryConfig::LogPattern { patterns, .. } => assert!(patterns.is_empty()),
            _ => panic!("expected LogPattern"),
        }
    }

    #[test]
    fn test_legacy_empty_promql_not_inferred_as_promql() {
        let json = r#"{"promql":"","metric_name":"cpu.util"}"#;
        let config: AlertQueryConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(config, AlertQueryConfig::Metrics { .. }));
    }

    #[test]
    fn test_legacy_promql_takes_priority_over_patterns() {
        let json = r#"{"promql":"up == 0","patterns":["error"]}"#;
        let config: AlertQueryConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(config, AlertQueryConfig::PromQL { .. }));
    }

    #[test]
    fn test_serialization_includes_query_type_tag() {
        let config = AlertQueryConfig::Metrics {
            metric_name: "cpu".into(),
            filters: BTreeMap::new(),
            group_by: vec![],
            time_aggregation: "avg".into(),
            space_aggregation: "sum".into(),
        };
        let val: serde_json::Value = serde_json::to_value(&config).unwrap();
        assert_eq!(val["query_type"], "metrics");
    }

    #[test]
    fn test_validate_aggregation_function_invalid() {
        // Invalid/malicious inputs should be rejected
        let invalid_inputs = [
            "DROP TABLE",
            "avg); DROP TABLE metrics; --",
            "unknown_func",
            "",
            "SELECT",
            "1; DELETE FROM",
            "avg(",
            "sum)",
        ];

        for input in invalid_inputs {
            assert!(
                validate_aggregation_function(input).is_err(),
                "Expected '{}' to be invalid",
                input
            );
        }
    }
}
