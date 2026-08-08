//! Shared Arrow columnar builders for connectors.
//!
//! Provides `ColumnBuilder` and `ColumnBuilders` types that connectors use
//! to build Arrow `RecordBatch`es directly from source data, avoiding an
//! intermediate `serde_json::Value` representation.

use super::{ConnectorError, ConnectorResult};
use crate::warehouse::types::{ColumnType, TableSchema};
use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int32Builder,
    Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

pub(crate) enum ColumnBuilder {
    Str(StringBuilder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Bool(BooleanBuilder),
    TimestampMicros(TimestampMicrosecondBuilder),
    Date32(Date32Builder),
}

impl ColumnBuilder {
    pub(crate) fn new(col_type: &ColumnType, capacity: usize) -> Self {
        match col_type {
            ColumnType::String | ColumnType::Uuid | ColumnType::Json => {
                Self::Str(StringBuilder::with_capacity(capacity, capacity * 32))
            }
            ColumnType::Int32 => Self::Int32(Int32Builder::with_capacity(capacity)),
            ColumnType::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            ColumnType::Float32 => Self::Float32(Float32Builder::with_capacity(capacity)),
            ColumnType::Float64 | ColumnType::Decimal => {
                Self::Float64(Float64Builder::with_capacity(capacity))
            }
            ColumnType::Boolean => Self::Bool(BooleanBuilder::with_capacity(capacity)),
            ColumnType::Timestamp => {
                Self::TimestampMicros(TimestampMicrosecondBuilder::with_capacity(capacity))
            }
            ColumnType::Date => Self::Date32(Date32Builder::with_capacity(capacity)),
        }
    }

    pub(crate) fn append_string(&mut self, val: Option<&str>) {
        if let Self::Str(b) = self {
            match val {
                Some(s) => b.append_value(s),
                None => b.append_null(),
            }
        }
    }

    pub(crate) fn append_i32(&mut self, val: Option<i32>) {
        if let Self::Int32(b) = self {
            match val {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
        }
    }

    pub(crate) fn append_i64(&mut self, val: Option<i64>) {
        if let Self::Int64(b) = self {
            match val {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
        }
    }

    pub(crate) fn append_f32(&mut self, val: Option<f32>) {
        if let Self::Float32(b) = self {
            match val {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
        }
    }

    pub(crate) fn append_f64(&mut self, val: Option<f64>) {
        if let Self::Float64(b) = self {
            match val {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
        }
    }

    pub(crate) fn append_bool(&mut self, val: Option<bool>) {
        if let Self::Bool(b) = self {
            match val {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
        }
    }

    pub(crate) fn append_timestamp(&mut self, val: Option<i64>) {
        if let Self::TimestampMicros(b) = self {
            match val {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
        }
    }

    pub(crate) fn append_date32(&mut self, val: Option<i32>) {
        if let Self::Date32(b) = self {
            match val {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
        }
    }

    pub(crate) fn append_null(&mut self) {
        match self {
            Self::Str(b) => b.append_null(),
            Self::Int32(b) => b.append_null(),
            Self::Int64(b) => b.append_null(),
            Self::Float32(b) => b.append_null(),
            Self::Float64(b) => b.append_null(),
            Self::Bool(b) => b.append_null(),
            Self::TimestampMicros(b) => b.append_null(),
            Self::Date32(b) => b.append_null(),
        }
    }

    /// Append a value extracted from JSON. Handles String, Int32, Int64,
    /// Float32, Float64, and Boolean conversion from `serde_json::Value`.
    /// Timestamp and Date columns are left null -- connectors must call
    /// `append_timestamp` / `append_date32` directly with parsed values.
    pub(crate) fn append_json_value(&mut self, val: Option<&serde_json::Value>) {
        let val = match val {
            Some(v) if !v.is_null() => v,
            _ => {
                self.append_null();
                return;
            }
        };
        match self {
            Self::Str(b) => {
                if let Some(s) = val.as_str() {
                    b.append_value(s);
                } else {
                    b.append_value(val.to_string());
                }
            }
            Self::Int32(b) => {
                if let Some(n) = val.as_i64().and_then(|n| i32::try_from(n).ok()) {
                    b.append_value(n);
                } else if let Some(n) = val.as_str().and_then(|s| s.parse::<i32>().ok()) {
                    b.append_value(n);
                } else {
                    b.append_null();
                }
            }
            Self::Int64(b) => {
                if let Some(n) = val.as_i64() {
                    b.append_value(n);
                } else if let Some(n) = val.as_f64() {
                    b.append_value(n as i64);
                } else if let Some(n) = val.as_str().and_then(|s| s.parse::<i64>().ok()) {
                    b.append_value(n);
                } else {
                    b.append_null();
                }
            }
            Self::Float32(b) => {
                if let Some(f) = val.as_f64() {
                    b.append_value(f as f32);
                } else if let Some(f) = val.as_str().and_then(|s| s.parse::<f32>().ok()) {
                    b.append_value(f);
                } else {
                    b.append_null();
                }
            }
            Self::Float64(b) => {
                if let Some(f) = val.as_f64() {
                    b.append_value(f);
                } else if let Some(f) = val.as_str().and_then(|s| s.parse::<f64>().ok()) {
                    b.append_value(f);
                } else {
                    b.append_null();
                }
            }
            Self::Bool(b) => {
                if let Some(v) = val.as_bool() {
                    b.append_value(v);
                } else if let Some(s) = val.as_str() {
                    b.append_value(s.eq_ignore_ascii_case("true"));
                } else {
                    b.append_null();
                }
            }
            Self::TimestampMicros(_) | Self::Date32(_) => {
                self.append_null();
            }
        }
    }

    pub(crate) fn finish(self) -> ArrayRef {
        match self {
            Self::Str(mut b) => Arc::new(b.finish()),
            Self::Int32(mut b) => Arc::new(b.finish()),
            Self::Int64(mut b) => Arc::new(b.finish()),
            Self::Float32(mut b) => Arc::new(b.finish()),
            Self::Float64(mut b) => Arc::new(b.finish()),
            Self::Bool(mut b) => Arc::new(b.finish()),
            Self::TimestampMicros(mut b) => Arc::new(b.finish()),
            Self::Date32(mut b) => Arc::new(b.finish()),
        }
    }
}

pub(crate) struct ColumnBuilders {
    builders: Vec<ColumnBuilder>,
    len: usize,
}

impl ColumnBuilders {
    pub(crate) fn new(schema: &TableSchema, capacity: usize) -> Self {
        let builders = schema
            .columns
            .iter()
            .map(|col| ColumnBuilder::new(&col.data_type, capacity))
            .collect();
        Self { builders, len: 0 }
    }

    pub(crate) fn builder(&mut self, idx: usize) -> &mut ColumnBuilder {
        &mut self.builders[idx]
    }

    pub(crate) fn row_complete(&mut self) {
        self.len += 1;
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn finish(self, schema: Arc<Schema>) -> ConnectorResult<RecordBatch> {
        let columns: Vec<ArrayRef> = self.builders.into_iter().map(|b| b.finish()).collect();
        RecordBatch::try_new(schema, columns)
            .map_err(|e| ConnectorError::Internal(format!("Failed to create RecordBatch: {}", e)))
    }
}
