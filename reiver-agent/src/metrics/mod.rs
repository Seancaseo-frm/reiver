use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;

pub mod system;
pub mod collector;

pub use collector::Collector;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metric {
    pub name: String,
    pub value: MetricValue,
    pub r#type: MetricType,
    pub timestamp: DateTime<Utc>,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MetricValue {
    Float(f64),
    Int(i64),
    UInt(u64),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MetricType {
    Gauge,
    Count,
    MonotonicCount,
    Rate,
}

impl From<f64> for MetricValue {
    fn from(value: f64) -> Self {
        MetricValue::Float(value)
    }
}

impl From<i64> for MetricValue {
    fn from(value: i64) -> Self {
        MetricValue::Int(value)
    }
}

impl From<u64> for MetricValue {
    fn from(value: u64) -> Self {
        MetricValue::UInt(value)
    }
}

