//! Shared ClickHouse insert structs for the v1 metrics tables.

use chrono::{DateTime, Utc};
use clickhouse::Row;
use serde::Serialize;
use uuid::Uuid;

#[derive(Row, Serialize, Clone)]
pub struct SampleInsert {
    #[serde(with = "clickhouse::serde::uuid")]
    pub project_id: Uuid,
    pub metric_name: String,
    pub fingerprint: u64,
    pub unix_milli: i64,
    pub value: f64,
    pub temporality: String,
    pub metric_type: String,
    pub flags: u8,
    pub resource_attributes: Vec<(String, String)>,
    pub metric_attributes: Vec<(String, String)>,
    pub labels: String,
}

#[derive(Row, Serialize, Clone)]
pub struct TimeSeriesInsert {
    #[serde(with = "clickhouse::serde::uuid")]
    pub project_id: Uuid,
    pub metric_name: String,
    pub fingerprint: u64,
    pub labels: String,
    pub temporality: String,
    pub metric_type: String,
    pub unix_milli: i64,
    pub resource_attributes: Vec<(String, String)>,
    pub metric_attributes: Vec<(String, String)>,
}

#[derive(Row, Serialize, Clone)]
pub struct ExemplarInsert {
    #[serde(with = "clickhouse::serde::uuid")]
    pub project_id: Uuid,
    pub metric_name: String,
    pub fingerprint: u64,
    pub exemplar_time_unix_nano: i64,
    pub trace_id: String,
    pub span_id: String,
    pub value: f64,
    pub filtered_attributes: Vec<(String, String)>,
}

#[derive(Row, Serialize, Clone)]
pub struct FilterValueInsert {
    pub project_id: String,
    pub attribute_type: String,
    pub attribute_value: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub last_seen: DateTime<Utc>,
}
