//! Shared Arrow utilities for blockchain connectors.
//!
//! Contains helpers for converting `TableSchema` to Arrow `Schema` and
//! building `StringArray`s, used by both Bitcoin and Ethereum connectors.

use arrow::datatypes::{DataType, Field, Schema};

use crate::warehouse::types::{ColumnType, TableSchema};

/// Convert a `TableSchema` into an Arrow `Schema`.
pub fn to_arrow_schema(table_schema: &TableSchema) -> Schema {
    let fields: Vec<Field> = table_schema
        .columns
        .iter()
        .map(|c| {
            let dt = match c.data_type {
                ColumnType::Int32 => DataType::Int32,
                ColumnType::Int64 => DataType::Int64,
                ColumnType::Float32 => DataType::Float32,
                ColumnType::Float64 => DataType::Float64,
                ColumnType::Boolean => DataType::Boolean,
                ColumnType::Timestamp => DataType::Timestamp(
                    arrow::datatypes::TimeUnit::Microsecond,
                    Some("UTC".into()),
                ),
                ColumnType::Date => DataType::Date32,
                ColumnType::Decimal => DataType::Float64,
                ColumnType::String | ColumnType::Json | ColumnType::Uuid => DataType::Utf8,
            };
            Field::new(&c.name, dt, c.nullable)
        })
        .collect();
    Schema::new(fields)
}

/// Build a non-nullable `StringArray` from owned strings.
pub fn string_array(values: Vec<String>) -> arrow::array::StringArray {
    arrow::array::StringArray::from(values)
}

/// Build a nullable `StringArray` from owned optional strings.
pub fn opt_string_array(values: Vec<Option<String>>) -> arrow::array::StringArray {
    arrow::array::StringArray::from(
        values
            .iter()
            .map(|v| v.as_deref())
            .collect::<Vec<Option<&str>>>(),
    )
}
