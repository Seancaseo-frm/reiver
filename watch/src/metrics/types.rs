//! Metric types and structures for the metrics system.

#![allow(dead_code)] // Types module - some types reserved for future features

use serde::{Deserialize, Serialize};

/// Metric temporality - how values relate over time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Temporality {
    /// Values are independent measurements (e.g., CPU usage at a point in time)
    #[default]
    Unspecified,
    /// Values represent changes since the last measurement
    Delta,
    /// Values are cumulative totals since start
    Cumulative,
}

impl Temporality {
    pub fn as_str(&self) -> &'static str {
        match self {
            Temporality::Unspecified => "unspecified",
            Temporality::Delta => "delta",
            Temporality::Cumulative => "cumulative",
        }
    }
}

impl std::fmt::Display for Temporality {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Type of metric being recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum MetricType {
    /// A point-in-time value (e.g., temperature, queue length)
    #[default]
    Gauge,
    /// A monotonically increasing value (e.g., request count)
    Sum,
    /// A distribution of values (e.g., request latencies)
    Histogram,
    /// Pre-computed quantiles
    Summary,
}

impl MetricType {
    pub fn as_str(&self) -> &'static str {
        match self {
            MetricType::Gauge => "gauge",
            MetricType::Sum => "sum",
            MetricType::Histogram => "histogram",
            MetricType::Summary => "summary",
        }
    }
}

impl std::fmt::Display for MetricType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A single metric sample to be stored.
#[derive(Debug, Clone)]
pub struct MetricSample {
    pub org_id: uuid::Uuid,
    pub project_id: uuid::Uuid,
    pub metric_name: String,
    pub fingerprint: u64,
    pub unix_milli: i64,
    pub value: f64,
    pub temporality: Temporality,
    pub metric_type: MetricType,
    pub labels_json: String,
    pub flags: u8,
}

/// Time aggregation operations supported for metrics queries.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum TimeAggregation {
    Sum,
    #[default]
    Avg,
    Min,
    Max,
    Count,
    #[serde(rename = "count_distinct")]
    CountDistinct,
    Rate,
    Increase,
    Last,
    /// 50th percentile (median)
    P50,
    /// 75th percentile
    P75,
    /// 90th percentile
    P90,
    /// 95th percentile
    P95,
    /// 99th percentile
    P99,
    /// 99.9th percentile
    #[serde(rename = "p999")]
    P999,
}

impl TimeAggregation {
    pub fn as_str(&self) -> &'static str {
        match self {
            TimeAggregation::Sum => "sum",
            TimeAggregation::Avg => "avg",
            TimeAggregation::Min => "min",
            TimeAggregation::Max => "max",
            TimeAggregation::Count => "count",
            TimeAggregation::CountDistinct => "count_distinct",
            TimeAggregation::Rate => "rate",
            TimeAggregation::Increase => "increase",
            TimeAggregation::Last => "last",
            TimeAggregation::P50 => "p50",
            TimeAggregation::P75 => "p75",
            TimeAggregation::P90 => "p90",
            TimeAggregation::P95 => "p95",
            TimeAggregation::P99 => "p99",
            TimeAggregation::P999 => "p999",
        }
    }

    /// Returns the percentile value (0.0-1.0) if this is a percentile aggregation.
    pub fn percentile_value(&self) -> Option<f64> {
        match self {
            TimeAggregation::P50 => Some(0.50),
            TimeAggregation::P75 => Some(0.75),
            TimeAggregation::P90 => Some(0.90),
            TimeAggregation::P95 => Some(0.95),
            TimeAggregation::P99 => Some(0.99),
            TimeAggregation::P999 => Some(0.999),
            _ => None,
        }
    }

    /// Returns true if this is a percentile aggregation.
    pub fn is_percentile(&self) -> bool {
        self.percentile_value().is_some()
    }
}

/// Space aggregation operations (across time series).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SpaceAggregation {
    #[default]
    Sum,
    Avg,
    Min,
    Max,
    Count,
}

impl SpaceAggregation {
    pub fn as_str(&self) -> &'static str {
        match self {
            SpaceAggregation::Sum => "sum",
            SpaceAggregation::Avg => "avg",
            SpaceAggregation::Min => "min",
            SpaceAggregation::Max => "max",
            SpaceAggregation::Count => "count",
        }
    }
}
