//! Shared ClickHouse type parser.
//!
//! Uses the `clickhouse-data-type` crate to parse ClickHouse type strings
//! into Arrow `DataType` and pgwire `Type`, correctly handling all wrapper
//! types (`Nullable`, `LowCardinality`) and parameterised types
//! (`DateTime64(3)`, `Decimal(18,4)`, `FixedString(32)`, etc.).

use arrow::datatypes::{DataType, TimeUnit};
use clickhouse_data_type::low_cardinality::LowCardinalityDataType;
use clickhouse_data_type::nullable::NullableTypeName;
use clickhouse_data_type::type_name::TypeName;
use pgwire::api::Type;

// ── Public API ──────────────────────────────────────────────────────────

/// Convert a ClickHouse type string to an Arrow `DataType` and nullable flag.
///
/// Falls back to string-based parsing for types the grammar crate cannot
/// handle, ensuring queries never fail due to exotic column types.
pub fn ch_type_to_arrow(ch_type: &str) -> (DataType, bool) {
    let parsed: TypeName = match ch_type.parse() {
        Ok(t) => t,
        Err(_) => return fallback_arrow(ch_type),
    };
    match parsed {
        TypeName::Nullable(ref inner) => (nullable_to_arrow(inner), true),
        TypeName::LowCardinality(ref inner) => low_cardinality_to_arrow(inner),
        ref other => (type_name_to_arrow(other), false),
    }
}

/// Convert a ClickHouse type string to a pgwire `Type`.
///
/// Falls back to `Type::TEXT` for unrecognised types.
pub fn ch_type_to_pg(ch_type: &str) -> Type {
    let parsed: TypeName = match ch_type.parse() {
        Ok(t) => t,
        Err(_) => return fallback_pg(ch_type),
    };
    match parsed {
        TypeName::Nullable(ref inner) => nullable_to_pg(inner),
        TypeName::LowCardinality(ref inner) => low_cardinality_to_pg(inner),
        ref other => type_name_to_pg(other),
    }
}

/// Check whether a ClickHouse type string represents a numeric type.
///
/// Correctly handles wrapper types (`Nullable`, `LowCardinality`) and
/// parameterised decimal variants (`Decimal(18,4)`, `Decimal128(5)`, etc.).
/// Falls back to a string-based check for types the grammar crate cannot parse.
pub fn ch_type_is_numeric(ch_type: &str) -> bool {
    let parsed: TypeName = match ch_type.parse() {
        Ok(t) => t,
        Err(_) => return fallback_is_numeric(ch_type),
    };
    match parsed {
        TypeName::Nullable(ref inner) => nullable_is_numeric(inner),
        TypeName::LowCardinality(ref inner) => low_cardinality_is_numeric(inner),
        ref other => type_name_is_numeric(other),
    }
}

fn type_name_is_numeric(ty: &TypeName) -> bool {
    matches!(
        ty,
        TypeName::Int8
            | TypeName::Int16
            | TypeName::Int32
            | TypeName::Int64
            | TypeName::Int128
            | TypeName::Int256
            | TypeName::UInt8
            | TypeName::UInt16
            | TypeName::UInt32
            | TypeName::UInt64
            | TypeName::UInt256
            | TypeName::Float32
            | TypeName::Float64
            | TypeName::Decimal(_, _)
    )
}

fn nullable_is_numeric(n: &NullableTypeName) -> bool {
    matches!(
        n,
        NullableTypeName::Int8
            | NullableTypeName::Int16
            | NullableTypeName::Int32
            | NullableTypeName::Int64
            | NullableTypeName::Int128
            | NullableTypeName::Int256
            | NullableTypeName::UInt8
            | NullableTypeName::UInt16
            | NullableTypeName::UInt32
            | NullableTypeName::UInt64
            | NullableTypeName::UInt256
            | NullableTypeName::Float32
            | NullableTypeName::Float64
            | NullableTypeName::Decimal(_, _)
    )
}

fn low_cardinality_is_numeric(lc: &LowCardinalityDataType) -> bool {
    match lc {
        LowCardinalityDataType::Nullable(inner) => nullable_is_numeric(inner),
        LowCardinalityDataType::Int8
        | LowCardinalityDataType::Int16
        | LowCardinalityDataType::Int32
        | LowCardinalityDataType::Int64
        | LowCardinalityDataType::UInt8
        | LowCardinalityDataType::UInt16
        | LowCardinalityDataType::UInt32
        | LowCardinalityDataType::UInt64
        | LowCardinalityDataType::Float32
        | LowCardinalityDataType::Float64 => true,
        _ => false,
    }
}

fn fallback_is_numeric(ch_type: &str) -> bool {
    let mut inner = ch_type;
    loop {
        if inner.starts_with("Nullable(") && inner.ends_with(')') {
            inner = &inner[9..inner.len() - 1];
        } else if inner.starts_with("LowCardinality(") && inner.ends_with(')') {
            inner = &inner[15..inner.len() - 1];
        } else {
            break;
        }
    }
    matches!(
        inner,
        "Int8" | "Int16" | "Int32" | "Int64" | "Int128" | "Int256"
            | "UInt8" | "UInt16" | "UInt32" | "UInt64" | "UInt128" | "UInt256"
            | "Float32" | "Float64"
    ) || inner.starts_with("Decimal")
}

// ── DateTime64 precision helpers ────────────────────────────────────────

fn datetime64_time_unit(precision: u8) -> TimeUnit {
    match precision {
        0..=3 => TimeUnit::Millisecond,
        4..=6 => TimeUnit::Microsecond,
        _ => TimeUnit::Nanosecond,
    }
}

fn parse_datetime64_precision(s: &str) -> u8 {
    s.find('(')
        .and_then(|start| s[start + 1..].find(|c: char| !c.is_ascii_digit()).or(Some(s.len() - start - 1)).map(|end| &s[start + 1..start + 1 + end]))
        .and_then(|digits| digits.parse::<u8>().ok())
        .unwrap_or(3)
}

// ── TypeName → Arrow ────────────────────────────────────────────────────

fn type_name_to_arrow(ty: &TypeName) -> DataType {
    match ty {
        TypeName::String | TypeName::FixedString(_) => DataType::Utf8,
        TypeName::Int8 => DataType::Int8,
        TypeName::Int16 => DataType::Int16,
        TypeName::Int32 => DataType::Int32,
        TypeName::Int64 => DataType::Int64,
        TypeName::Int128 | TypeName::Int256 => DataType::Utf8,
        TypeName::UInt8 => DataType::UInt8,
        TypeName::UInt16 => DataType::UInt16,
        TypeName::UInt32 => DataType::UInt32,
        TypeName::UInt64 => DataType::UInt64,
        TypeName::UInt256 => DataType::Utf8,
        TypeName::Float32 => DataType::Float32,
        TypeName::Float64 => DataType::Float64,
        TypeName::Decimal(_, _) => DataType::Float64,
        TypeName::Date | TypeName::DateTime(_) => {
            DataType::Timestamp(TimeUnit::Millisecond, None)
        }
        TypeName::DateTime64(precision, _) => {
            DataType::Timestamp(datetime64_time_unit(precision.0 as u8), None)
        }
        TypeName::Uuid => DataType::Utf8,
        _ => DataType::Utf8,
    }
}

// ── NullableTypeName → Arrow ────────────────────────────────────────────

fn nullable_to_arrow(n: &NullableTypeName) -> DataType {
    match n {
        NullableTypeName::String | NullableTypeName::FixedString(_) => DataType::Utf8,
        NullableTypeName::Int8 => DataType::Int8,
        NullableTypeName::Int16 => DataType::Int16,
        NullableTypeName::Int32 => DataType::Int32,
        NullableTypeName::Int64 => DataType::Int64,
        NullableTypeName::Int128 | NullableTypeName::Int256 => DataType::Utf8,
        NullableTypeName::UInt8 => DataType::UInt8,
        NullableTypeName::UInt16 => DataType::UInt16,
        NullableTypeName::UInt32 => DataType::UInt32,
        NullableTypeName::UInt64 => DataType::UInt64,
        NullableTypeName::UInt256 => DataType::Utf8,
        NullableTypeName::Float32 => DataType::Float32,
        NullableTypeName::Float64 => DataType::Float64,
        NullableTypeName::Decimal(_, _) => DataType::Float64,
        NullableTypeName::Date | NullableTypeName::DateTime(_) => {
            DataType::Timestamp(TimeUnit::Millisecond, None)
        }
        NullableTypeName::DateTime64(precision, _) => {
            DataType::Timestamp(datetime64_time_unit(precision.0 as u8), None)
        }
        NullableTypeName::Uuid => DataType::Utf8,
        _ => DataType::Utf8,
    }
}

// ── LowCardinalityDataType → Arrow ──────────────────────────────────────

fn low_cardinality_to_arrow(lc: &LowCardinalityDataType) -> (DataType, bool) {
    match lc {
        LowCardinalityDataType::Nullable(inner) => (nullable_to_arrow(inner), true),
        LowCardinalityDataType::String | LowCardinalityDataType::FixedString(_) => {
            (DataType::Utf8, false)
        }
        LowCardinalityDataType::Int8 => (DataType::Int8, false),
        LowCardinalityDataType::Int16 => (DataType::Int16, false),
        LowCardinalityDataType::Int32 => (DataType::Int32, false),
        LowCardinalityDataType::Int64 => (DataType::Int64, false),
        LowCardinalityDataType::UInt8 => (DataType::UInt8, false),
        LowCardinalityDataType::UInt16 => (DataType::UInt16, false),
        LowCardinalityDataType::UInt32 => (DataType::UInt32, false),
        LowCardinalityDataType::UInt64 => (DataType::UInt64, false),
        LowCardinalityDataType::Float32 => (DataType::Float32, false),
        LowCardinalityDataType::Float64 => (DataType::Float64, false),
        LowCardinalityDataType::Date | LowCardinalityDataType::DateTime(_) => {
            (DataType::Timestamp(TimeUnit::Millisecond, None), false)
        }
        _ => (DataType::Utf8, false),
    }
}

// ── TypeName → pgwire Type ──────────────────────────────────────────────

fn type_name_to_pg(ty: &TypeName) -> Type {
    match ty {
        TypeName::String | TypeName::FixedString(_) => Type::TEXT,
        TypeName::Uuid => Type::UUID,
        TypeName::Int8 | TypeName::Int16 | TypeName::UInt8 => Type::INT2,
        TypeName::Int32 | TypeName::UInt16 => Type::INT4,
        TypeName::Int64 | TypeName::UInt32 => Type::INT8,
        TypeName::UInt64 => Type::NUMERIC,
        TypeName::Float32 => Type::FLOAT4,
        TypeName::Float64 => Type::FLOAT8,
        TypeName::Decimal(_, _) => Type::NUMERIC,
        TypeName::Date => Type::DATE,
        TypeName::DateTime(None) => Type::TIMESTAMP,
        TypeName::DateTime(Some(_)) => Type::TIMESTAMPTZ,
        TypeName::DateTime64(_, None) => Type::TIMESTAMP,
        TypeName::DateTime64(_, Some(_)) => Type::TIMESTAMPTZ,
        TypeName::Enum8(_) | TypeName::Enum16(_) => Type::TEXT,
        TypeName::Array(_) | TypeName::Map(_, _) | TypeName::Tuple(_) => Type::TEXT,
        _ => Type::TEXT,
    }
}

// ── NullableTypeName → pgwire Type ──────────────────────────────────────

fn nullable_to_pg(n: &NullableTypeName) -> Type {
    match n {
        NullableTypeName::String | NullableTypeName::FixedString(_) => Type::TEXT,
        NullableTypeName::Uuid => Type::UUID,
        NullableTypeName::Int8 | NullableTypeName::Int16 | NullableTypeName::UInt8 => Type::INT2,
        NullableTypeName::Int32 | NullableTypeName::UInt16 => Type::INT4,
        NullableTypeName::Int64 | NullableTypeName::UInt32 => Type::INT8,
        NullableTypeName::UInt64 => Type::NUMERIC,
        NullableTypeName::Float32 => Type::FLOAT4,
        NullableTypeName::Float64 => Type::FLOAT8,
        NullableTypeName::Decimal(_, _) => Type::NUMERIC,
        NullableTypeName::Date => Type::DATE,
        NullableTypeName::DateTime(None) => Type::TIMESTAMP,
        NullableTypeName::DateTime(Some(_)) => Type::TIMESTAMPTZ,
        NullableTypeName::DateTime64(_, None) => Type::TIMESTAMP,
        NullableTypeName::DateTime64(_, Some(_)) => Type::TIMESTAMPTZ,
        NullableTypeName::Enum8(_) | NullableTypeName::Enum16(_) => Type::TEXT,
        _ => Type::TEXT,
    }
}

// ── LowCardinalityDataType → pgwire Type ────────────────────────────────

fn low_cardinality_to_pg(lc: &LowCardinalityDataType) -> Type {
    match lc {
        LowCardinalityDataType::Nullable(inner) => nullable_to_pg(inner),
        LowCardinalityDataType::String | LowCardinalityDataType::FixedString(_) => Type::TEXT,
        LowCardinalityDataType::Int8 | LowCardinalityDataType::UInt8 => Type::INT2,
        LowCardinalityDataType::Int16 => Type::INT2,
        LowCardinalityDataType::Int32 | LowCardinalityDataType::UInt16 => Type::INT4,
        LowCardinalityDataType::Int64 | LowCardinalityDataType::UInt32 => Type::INT8,
        LowCardinalityDataType::UInt64 => Type::NUMERIC,
        LowCardinalityDataType::Float32 => Type::FLOAT4,
        LowCardinalityDataType::Float64 => Type::FLOAT8,
        LowCardinalityDataType::Date => Type::DATE,
        LowCardinalityDataType::DateTime(None) => Type::TIMESTAMP,
        LowCardinalityDataType::DateTime(Some(_)) => Type::TIMESTAMPTZ,
        _ => Type::TEXT,
    }
}

// ── Fallback parsers ────────────────────────────────────────────────────

fn fallback_arrow(ch_type: &str) -> (DataType, bool) {
    let mut inner = ch_type;
    let mut nullable = false;
    loop {
        if inner.starts_with("Nullable(") && inner.ends_with(')') {
            inner = &inner[9..inner.len() - 1];
            nullable = true;
        } else if inner.starts_with("LowCardinality(") && inner.ends_with(')') {
            inner = &inner[15..inner.len() - 1];
        } else {
            break;
        }
    }
    let dt = match inner {
        "String" => DataType::Utf8,
        "Int8" => DataType::Int8,
        "Int16" => DataType::Int16,
        "Int32" => DataType::Int32,
        "Int64" => DataType::Int64,
        "UInt8" => DataType::UInt8,
        "UInt16" => DataType::UInt16,
        "UInt32" => DataType::UInt32,
        "UInt64" => DataType::UInt64,
        "Float32" => DataType::Float32,
        "Float64" => DataType::Float64,
        "Bool" => DataType::Boolean,
        t if t.starts_with("DateTime64") => {
            let precision = parse_datetime64_precision(t);
            DataType::Timestamp(datetime64_time_unit(precision), None)
        }
        "DateTime" | "Date" => DataType::Timestamp(TimeUnit::Millisecond, None),
        "UUID" => DataType::Utf8,
        t if t.starts_with("Decimal") => DataType::Float64,
        t if t.starts_with("FixedString") => DataType::Utf8,
        _ => DataType::Utf8,
    };
    (dt, nullable)
}

fn fallback_pg(ch_type: &str) -> Type {
    let mut inner = ch_type;
    loop {
        if inner.starts_with("Nullable(") && inner.ends_with(')') {
            inner = &inner[9..inner.len() - 1];
        } else if inner.starts_with("LowCardinality(") && inner.ends_with(')') {
            inner = &inner[15..inner.len() - 1];
        } else {
            break;
        }
    }
    match inner {
        "String" => Type::TEXT,
        "UUID" => Type::UUID,
        "Int8" | "Int16" | "UInt8" => Type::INT2,
        "Int32" | "UInt16" => Type::INT4,
        "Int64" | "UInt32" => Type::INT8,
        "UInt64" => Type::NUMERIC,
        "Float32" => Type::FLOAT4,
        "Float64" => Type::FLOAT8,
        "Bool" => Type::BOOL,
        "Date" => Type::DATE,
        "DateTime" => Type::TIMESTAMP,
        t if t.starts_with("DateTime64") => {
            if t.contains(',') {
                Type::TIMESTAMPTZ
            } else {
                Type::TIMESTAMP
            }
        }
        t if t.starts_with("DateTime(") => Type::TIMESTAMPTZ,
        t if t.starts_with("Decimal") => Type::NUMERIC,
        t if t.starts_with("FixedString") => Type::TEXT,
        t if t.starts_with("Array") => Type::TEXT,
        t if t.starts_with("Map") => Type::TEXT,
        t if t.starts_with("Enum") => Type::TEXT,
        _ => Type::TEXT,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Arrow mapping ───────────────────────────────────────────────────

    #[test]
    fn simple_types_to_arrow() {
        assert_eq!(ch_type_to_arrow("String"), (DataType::Utf8, false));
        assert_eq!(ch_type_to_arrow("Int32"), (DataType::Int32, false));
        assert_eq!(ch_type_to_arrow("UInt64"), (DataType::UInt64, false));
        assert_eq!(ch_type_to_arrow("Float32"), (DataType::Float32, false));
        assert_eq!(ch_type_to_arrow("Float64"), (DataType::Float64, false));
        assert_eq!(ch_type_to_arrow("UUID"), (DataType::Utf8, false));
    }

    #[test]
    fn nullable_to_arrow_test() {
        let (dt, nullable) = ch_type_to_arrow("Nullable(String)");
        assert_eq!(dt, DataType::Utf8);
        assert!(nullable);

        let (dt, nullable) = ch_type_to_arrow("Nullable(Int64)");
        assert_eq!(dt, DataType::Int64);
        assert!(nullable);
    }

    #[test]
    fn low_cardinality_to_arrow_test() {
        let (dt, nullable) = ch_type_to_arrow("LowCardinality(String)");
        assert_eq!(dt, DataType::Utf8);
        assert!(!nullable);
    }

    #[test]
    fn low_cardinality_nullable_to_arrow_test() {
        let (dt, nullable) = ch_type_to_arrow("LowCardinality(Nullable(String))");
        assert_eq!(dt, DataType::Utf8);
        assert!(nullable, "LowCardinality(Nullable(...)) must be nullable");
    }

    #[test]
    fn float32_not_widened_to_float64() {
        let (dt, _) = ch_type_to_arrow("Float32");
        assert_eq!(dt, DataType::Float32, "Float32 must map to Float32, not Float64");

        let (dt, _) = ch_type_to_arrow("Nullable(Float32)");
        assert_eq!(dt, DataType::Float32);
    }

    #[test]
    fn datetime64_to_arrow_test() {
        let (dt, nullable) = ch_type_to_arrow("DateTime64(3)");
        assert_eq!(dt, DataType::Timestamp(TimeUnit::Millisecond, None));
        assert!(!nullable);

        let (dt, _) = ch_type_to_arrow("DateTime64(6)");
        assert_eq!(
            dt,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            "DateTime64(6) must map to Microsecond, not Millisecond"
        );

        let (dt, _) = ch_type_to_arrow("DateTime64(9)");
        assert_eq!(
            dt,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            "DateTime64(9) must map to Nanosecond"
        );
    }

    #[test]
    fn nullable_datetime64_precision_test() {
        let (dt, nullable) = ch_type_to_arrow("Nullable(DateTime64(6))");
        assert_eq!(dt, DataType::Timestamp(TimeUnit::Microsecond, None));
        assert!(nullable);
    }

    #[test]
    fn low_cardinality_nullable_datetime_to_pg_test() {
        assert_eq!(
            ch_type_to_pg("LowCardinality(Nullable(String))"),
            Type::TEXT,
        );
        assert_eq!(
            ch_type_to_pg("LowCardinality(Nullable(Int64))"),
            Type::INT8,
        );
    }

    // ── pgwire mapping ──────────────────────────────────────────────────

    #[test]
    fn simple_types_to_pg() {
        assert_eq!(ch_type_to_pg("String"), Type::TEXT);
        assert_eq!(ch_type_to_pg("Int32"), Type::INT4);
        assert_eq!(ch_type_to_pg("Float32"), Type::FLOAT4);
        assert_eq!(ch_type_to_pg("Float64"), Type::FLOAT8);
        assert_eq!(ch_type_to_pg("UUID"), Type::UUID);
    }

    #[test]
    fn nullable_to_pg_test() {
        assert_eq!(ch_type_to_pg("Nullable(String)"), Type::TEXT);
        assert_eq!(ch_type_to_pg("Nullable(Int64)"), Type::INT8);
    }

    // ── ch_type_is_numeric ──────────────────────────────────────────────

    #[test]
    fn numeric_basic_types() {
        assert!(ch_type_is_numeric("Int8"));
        assert!(ch_type_is_numeric("Int16"));
        assert!(ch_type_is_numeric("Int32"));
        assert!(ch_type_is_numeric("Int64"));
        assert!(ch_type_is_numeric("Int128"));
        assert!(ch_type_is_numeric("Int256"));
        assert!(ch_type_is_numeric("UInt8"));
        assert!(ch_type_is_numeric("UInt16"));
        assert!(ch_type_is_numeric("UInt32"));
        assert!(ch_type_is_numeric("UInt64"));
        assert!(ch_type_is_numeric("UInt256"));
        assert!(ch_type_is_numeric("Float32"));
        assert!(ch_type_is_numeric("Float64"));
    }

    #[test]
    fn numeric_decimal_variants() {
        assert!(ch_type_is_numeric("Decimal(18,2)"));
        assert!(ch_type_is_numeric("Decimal(10,4)"));
    }

    #[test]
    fn numeric_nullable_wrapped() {
        assert!(ch_type_is_numeric("Nullable(Int64)"));
        assert!(ch_type_is_numeric("Nullable(Float32)"));
        assert!(ch_type_is_numeric("Nullable(Decimal(18,2))"));
    }

    #[test]
    fn numeric_low_cardinality_wrapped() {
        assert!(ch_type_is_numeric("LowCardinality(Float32)"));
        assert!(ch_type_is_numeric("LowCardinality(Int32)"));
    }

    #[test]
    fn non_numeric_types() {
        assert!(!ch_type_is_numeric("String"));
        assert!(!ch_type_is_numeric("DateTime"));
        assert!(!ch_type_is_numeric("UUID"));
        assert!(!ch_type_is_numeric("Nullable(String)"));
        assert!(!ch_type_is_numeric("Date"));
    }

    #[test]
    fn bool_is_not_numeric() {
        assert!(!ch_type_is_numeric("Bool"), "Bool must not be classified as numeric");
        assert!(!ch_type_is_numeric("Nullable(Bool)"), "Nullable(Bool) must not be numeric");
    }

    #[test]
    fn uint128_is_numeric_via_fallback() {
        assert!(ch_type_is_numeric("UInt128"), "UInt128 must be numeric via fallback path");
    }
}
