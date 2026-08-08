//! ClickHouse-to-Postgres type mapping and value encoding.
//!
//! Converts ClickHouse column types to PostgreSQL wire protocol types,
//! and encodes `serde_json::Value` rows into pgwire `DataRow` format.
//!
//! Also provides Arrow RecordBatch → pgwire Response conversion for
//! catalog queries executed through DataFusion.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, StringArray,
    StringViewArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use futures::stream;

use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::Type;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::warehouse::query::executor::ColumnInfo;

/// Map a ClickHouse type string to a Postgres wire protocol type.
///
/// Delegates to the shared `ch_type_parser` module which uses a proper
/// grammar-based parser, correctly handling `Nullable`, `LowCardinality`,
/// and all parameterised types.
pub fn clickhouse_type_to_pg(ch_type: &str) -> Type {
    crate::warehouse::ch_type_parser::ch_type_to_pg(ch_type)
}

/// Convert a `ColumnInfo` (from the query executor) to a pgwire `FieldInfo`.
///
/// Uses text format by default (simple query protocol).
pub fn column_info_to_field_info(col: &ColumnInfo) -> FieldInfo {
    column_info_to_field_info_with_format(col, FieldFormat::Text)
}

/// Convert a `ColumnInfo` to a `FieldInfo` with the specified format.
///
/// Binary format is supported for types where pgwire's `ToSql` is
/// implemented: bool, i16, i32, i64, f32, f64, &str, &[u8] (BYTEA).
/// For other types (DATE, TIMESTAMP, NUMERIC, etc.) we fall back to text
/// format since their binary encoding requires more complex serialization.
pub fn column_info_to_field_info_with_format(
    col: &ColumnInfo,
    requested_format: FieldFormat,
) -> FieldInfo {
    let pg_type = clickhouse_type_to_pg(&col.data_type);
    let format = if requested_format == FieldFormat::Binary {
        // Only use binary for types we can safely encode in binary
        match pg_type {
            Type::BOOL | Type::INT2 | Type::INT4 | Type::INT8 | Type::FLOAT4 | Type::FLOAT8
            | Type::TEXT | Type::VARCHAR | Type::BYTEA => FieldFormat::Binary,
            // For DATE, TIMESTAMP, NUMERIC, etc. stick with text
            _ => FieldFormat::Text,
        }
    } else {
        FieldFormat::Text
    };
    FieldInfo::new(col.name.clone(), None, None, pg_type, format)
}

/// Convert a `NativeColumnInfo` (from native block streaming) to a pgwire `FieldInfo`.
///
/// Uses `klickhouse_type_to_pg` for direct type mapping without string
/// round-trips.
pub fn native_column_to_field_info(
    col: &crate::warehouse::query::executor::NativeColumnInfo,
) -> FieldInfo {
    let pg_type = klickhouse_type_to_pg(&col.klickhouse_type);
    FieldInfo::new(col.name.clone(), None, None, pg_type, FieldFormat::Text)
}

/// Encode a `serde_json::Value` into the pgwire `DataRowEncoder`.
///
/// The value is encoded according to the target Postgres type. NULL values
/// are handled uniformly. Numbers are coerced to the appropriate Rust type
/// before encoding; everything else falls back to text representation.
pub fn encode_value(
    encoder: &mut DataRowEncoder,
    value: &serde_json::Value,
    pg_type: &Type,
) -> PgWireResult<()> {
    if value.is_null() {
        return encoder.encode_field(&None::<&str>);
    }

    match *pg_type {
        Type::BOOL => {
            let v = match value {
                serde_json::Value::Bool(b) => *b,
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() { i != 0 }
                    else if let Some(u) = n.as_u64() { u != 0 }
                    else if let Some(f) = n.as_f64() { f != 0.0 }
                    else { false }
                }
                _ => {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "22023".to_owned(),
                        format!("Cannot convert {} to boolean", value),
                    ))));
                }
            };
            encoder.encode_field(&v)
        }
        Type::INT2 => {
            let raw = value_to_i64(value)?;
            let v = i16::try_from(raw).map_err(|_| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "22003".to_owned(),
                    format!("Value {} out of range for INT2", raw),
                )))
            })?;
            encoder.encode_field(&v)
        }
        Type::INT4 => {
            let raw = value_to_i64(value)?;
            let v = i32::try_from(raw).map_err(|_| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "22003".to_owned(),
                    format!("Value {} out of range for INT4", raw),
                )))
            })?;
            encoder.encode_field(&v)
        }
        Type::INT8 => {
            let v = value_to_i64(value)?;
            encoder.encode_field(&v)
        }
        Type::FLOAT4 => {
            let f64_val = value_to_f64(value)?;
            let v = f64_val as f32;
            if v.is_infinite() && !f64_val.is_infinite() {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "22003".to_owned(),
                    format!("Value {} out of range for FLOAT4", f64_val),
                ))));
            }
            encoder.encode_field(&v)
        }
        Type::FLOAT8 => {
            let v = value_to_f64(value)?;
            encoder.encode_field(&v)
        }
        // TEXT, VARCHAR, DATE, TIMESTAMP, NUMERIC, and all other types
        // are encoded as text strings (the Postgres text wire format).
        _ => {
            let s = value_to_text(value);
            encoder.encode_field(&s)
        }
    }
}

/// Extract an i64 from a JSON value, with best-effort coercion.
/// Returns an error for values that cannot be represented as i64 (e.g. u64 > i64::MAX).
fn value_to_i64(value: &serde_json::Value) -> PgWireResult<i64> {
    match value {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                return Ok(i);
            }
            if let Some(u) = n.as_u64() {
                return i64::try_from(u).map_err(|_| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "22003".to_owned(),
                        format!("Value {} out of range for INT8", u),
                    )))
                });
            }
            if let Some(f) = n.as_f64() {
                let rounded = f.round();
                // i64::MAX as f64 rounds up to 9223372036854775808.0 (i64::MAX + 1)
                // because f64 lacks sufficient precision. Using >= is correct:
                // any f64 >= that rounded value overflows on `as i64`.
                // i64::MIN is exactly representable in f64, so < is correct.
                if rounded.is_nan() || rounded.is_infinite()
                    || rounded < (i64::MIN as f64)
                    || rounded >= (i64::MAX as f64)
                {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "22003".to_owned(),
                        format!("Value {} out of range for INT8", f),
                    ))));
                }
                return Ok(rounded as i64);
            }
            Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "22003".to_owned(),
                "Number cannot be represented as INT8".to_owned(),
            ))))
        }
        serde_json::Value::String(s) => s.parse::<i64>().map_err(|_| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "22P02".to_owned(),
                format!("Invalid input syntax for integer: \"{}\"", s),
            )))
        }),
        serde_json::Value::Bool(b) => Ok(if *b { 1 } else { 0 }),
        _ => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "22023".to_owned(),
            format!("Cannot convert {} to integer", value),
        )))),
    }
}

/// Extract an f64 from a JSON value, with best-effort coercion.
/// Returns an error for string values that cannot be parsed as f64.
fn value_to_f64(value: &serde_json::Value) -> PgWireResult<f64> {
    match value {
        serde_json::Value::Number(n) => n.as_f64().ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "22003".to_owned(),
                format!("Number {} cannot be represented as FLOAT8", n),
            )))
        }),
        serde_json::Value::String(s) => s.parse::<f64>().map_err(|_| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "22P02".to_owned(),
                format!("Invalid input syntax for float: \"{}\"", s),
            )))
        }),
        serde_json::Value::Bool(b) => Ok(if *b { 1.0 } else { 0.0 }),
        _ => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "22023".to_owned(),
            format!("Cannot convert {} to float", value),
        )))),
    }
}

/// Convert a JSON value to its text representation for Postgres text format.
fn value_to_text(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => if *b { "true".to_string() } else { "false".to_string() },
        // Arrays, objects, etc. → JSON text
        other => other.to_string(),
    }
}

// =============================================================================
// klickhouse::Value → pgwire encoding (direct, no JSON intermediate)
// =============================================================================

/// Map a `klickhouse::Type` directly to a pgwire `Type`.
///
/// Avoids the string round-trip through `klickhouse_type_to_string` +
/// `clickhouse_type_to_pg` by matching on the typed enum.
pub fn klickhouse_type_to_pg(ty: &klickhouse::Type) -> Type {
    match ty {
        klickhouse::Type::Nullable(inner) => klickhouse_type_to_pg(inner),
        klickhouse::Type::LowCardinality(inner) => klickhouse_type_to_pg(inner),
        klickhouse::Type::Int8 | klickhouse::Type::Int16 | klickhouse::Type::UInt8 => Type::INT2,
        klickhouse::Type::Int32 | klickhouse::Type::UInt16 => Type::INT4,
        klickhouse::Type::Int64 | klickhouse::Type::UInt32 => Type::INT8,
        klickhouse::Type::UInt64 => Type::NUMERIC,
        klickhouse::Type::Float32 => Type::FLOAT4,
        klickhouse::Type::Float64 => Type::FLOAT8,
        klickhouse::Type::String | klickhouse::Type::FixedString(_) => Type::TEXT,
        klickhouse::Type::Uuid => Type::UUID,
        klickhouse::Type::Date => Type::DATE,
        klickhouse::Type::DateTime(_) => Type::TIMESTAMPTZ,
        klickhouse::Type::DateTime64(_, _) => Type::TIMESTAMPTZ,
        klickhouse::Type::Decimal32(_)
        | klickhouse::Type::Decimal64(_)
        | klickhouse::Type::Decimal128(_)
        | klickhouse::Type::Decimal256(_) => Type::NUMERIC,
        klickhouse::Type::Int128
        | klickhouse::Type::UInt128
        | klickhouse::Type::Int256
        | klickhouse::Type::UInt256 => Type::NUMERIC,
        klickhouse::Type::Ipv4 | klickhouse::Type::Ipv6 => Type::TEXT,
        klickhouse::Type::Enum8(_) | klickhouse::Type::Enum16(_) => Type::TEXT,
        klickhouse::Type::Array(_)
        | klickhouse::Type::Map(_, _)
        | klickhouse::Type::Tuple(_) => Type::TEXT,
        _ => Type::TEXT,
    }
}

/// Encode a `klickhouse::Value` directly into a pgwire `DataRowEncoder`.
///
/// This bypasses the `serde_json::Value` intermediate used by `encode_value`,
/// going straight from the ClickHouse native protocol value to the Postgres
/// wire format. Used by the streaming pgwire path for zero-copy encoding.
pub fn encode_klickhouse_value(
    encoder: &mut DataRowEncoder,
    value: &klickhouse::Value,
) -> PgWireResult<()> {
    if matches!(value, klickhouse::Value::Null) {
        return encoder.encode_field(&None::<&str>);
    }

    match value {
        klickhouse::Value::UInt8(v) => encoder.encode_field(&(*v as i16)),
        klickhouse::Value::UInt16(v) => encoder.encode_field(&(*v as i32)),
        klickhouse::Value::UInt32(v) => encoder.encode_field(&(*v as i64)),
        klickhouse::Value::UInt64(v) => encoder.encode_field(&v.to_string()),
        klickhouse::Value::Int8(v) => encoder.encode_field(&(*v as i16)),
        klickhouse::Value::Int16(v) => encoder.encode_field(v),
        klickhouse::Value::Int32(v) => encoder.encode_field(v),
        klickhouse::Value::Int64(v) => encoder.encode_field(v),
        klickhouse::Value::Float32(v) => encoder.encode_field(v),
        klickhouse::Value::Float64(v) => encoder.encode_field(v),
        klickhouse::Value::String(bytes) => {
            match std::str::from_utf8(bytes) {
                Ok(s) => encoder.encode_field(&s),
                Err(_) => {
                    let hex = bytes_to_pg_hex(bytes);
                    encoder.encode_field(&hex)
                }
            }
        }
        klickhouse::Value::Uuid(u) => {
            let s = u.to_string();
            encoder.encode_field(&s)
        }
        klickhouse::Value::Date(d) => {
            let date: chrono::NaiveDate = (*d).into();
            let s = date.format("%Y-%m-%d").to_string();
            encoder.encode_field(&s)
        }
        klickhouse::Value::DateTime(dt) => {
            let chrono_dt: chrono::DateTime<chrono_tz::Tz> = (*dt).try_into()
                .map_err(|e| PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "22008".to_owned(),
                    format!("DateTime conversion error: {}", e),
                ))))?;
            let s = chrono_dt.to_rfc3339();
            encoder.encode_field(&s)
        }
        klickhouse::Value::DateTime64(dt) => {
            let chrono_dt: chrono::DateTime<chrono_tz::Tz> = (*dt).try_into()
                .map_err(|e| PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "22008".to_owned(),
                    format!("DateTime64 conversion error: {}", e),
                ))))?;
            let s = chrono_dt.to_rfc3339();
            encoder.encode_field(&s)
        }
        klickhouse::Value::Decimal32(scale, v) => {
            let s = format_decimal_128(*v as i128, *scale);
            encoder.encode_field(&s)
        }
        klickhouse::Value::Decimal64(scale, v) => {
            let s = format_decimal_128(*v as i128, *scale);
            encoder.encode_field(&s)
        }
        klickhouse::Value::Decimal128(scale, v) => {
            let s = format_decimal_128(*v as i128, *scale);
            encoder.encode_field(&s)
        }
        klickhouse::Value::Decimal256(scale, v) => {
            let s = format_decimal_256(v, *scale);
            encoder.encode_field(&s)
        }
        klickhouse::Value::Int128(v) => {
            let s = v.to_string();
            encoder.encode_field(&s)
        }
        klickhouse::Value::UInt128(v) => {
            let s = v.to_string();
            encoder.encode_field(&s)
        }
        klickhouse::Value::Int256(v) => {
            let s = v.to_string();
            encoder.encode_field(&s)
        }
        klickhouse::Value::UInt256(v) => {
            let s = v.to_string();
            encoder.encode_field(&s)
        }
        klickhouse::Value::Ipv4(ip) => {
            let s = ip.to_string();
            encoder.encode_field(&s)
        }
        klickhouse::Value::Ipv6(ip) => {
            let s = ip.to_string();
            encoder.encode_field(&s)
        }
        klickhouse::Value::Enum8(v) => {
            let s = v.to_string();
            encoder.encode_field(&s)
        }
        klickhouse::Value::Enum16(v) => {
            let s = v.to_string();
            encoder.encode_field(&s)
        }
        klickhouse::Value::Array(arr) => {
            let s = klickhouse_array_to_text(arr);
            encoder.encode_field(&s)
        }
        klickhouse::Value::Tuple(arr) => {
            let s = klickhouse_array_to_text(arr);
            encoder.encode_field(&s)
        }
        klickhouse::Value::Map(keys, values) => {
            let s = klickhouse_map_to_text(keys, values);
            encoder.encode_field(&s)
        }
        klickhouse::Value::Point(p) => {
            let s = format!("({},{})", p.0[0], p.0[1]);
            encoder.encode_field(&s)
        }
        klickhouse::Value::Ring(r) => {
            let s = format!("{:?}", r.0.iter().map(|p| (p.0[0], p.0[1])).collect::<Vec<_>>());
            encoder.encode_field(&s)
        }
        klickhouse::Value::Polygon(p) => {
            let s = format!("{:?}", p);
            encoder.encode_field(&s)
        }
        klickhouse::Value::MultiPolygon(mp) => {
            let s = format!("{:?}", mp);
            encoder.encode_field(&s)
        }
        klickhouse::Value::Null => encoder.encode_field(&None::<&str>),
        klickhouse::Value::BFloat16(v) => encoder.encode_field(&(f32::from(*v) as f64)),
    }
}

/// Format a Decimal128 value with the given scale for text encoding.
fn format_decimal_128(value: i128, scale: usize) -> String {
    if scale == 0 {
        return value.to_string();
    }
    let divisor = 10i128.pow(scale as u32);
    let integer_part = value / divisor;
    let fractional_part = (value % divisor).unsigned_abs();
    if value < 0 && integer_part == 0 {
        format!("-0.{:0>width$}", fractional_part, width = scale)
    } else {
        format!("{}.{:0>width$}", integer_part, fractional_part, width = scale)
    }
}

/// Format a Decimal256 (klickhouse i256) with scale for text encoding.
///
/// Tries to fit the value into i128 for standard formatting.
/// Falls back to string-based decimal point insertion for larger values.
fn format_decimal_256(v: &klickhouse::i256, scale: usize) -> String {
    // i256 stores 32 bytes in big-endian order. Try fitting into i128 first.
    let bytes = &v.0;
    let high_half = &bytes[..16];
    let is_negative = high_half[0] & 0x80 != 0;
    let high_is_sign_extension = if is_negative {
        high_half.iter().all(|&b| b == 0xFF)
    } else {
        high_half.iter().all(|&b| b == 0x00)
    };

    if high_is_sign_extension && scale <= 38 {
        let mut low_bytes = [0u8; 16];
        low_bytes.copy_from_slice(&bytes[16..]);
        let low = i128::from_be_bytes(low_bytes);
        // Verify sign consistency
        if (is_negative && low < 0) || (!is_negative && low >= 0) {
            return format_decimal_128(low, scale);
        }
    }

    // For values that don't fit in i128, use string-based formatting.
    // Convert 256-bit big-endian two's complement to decimal string.
    let (is_neg, abs_bytes) = if is_negative {
        let mut abs = [0u8; 32];
        let mut carry = true;
        for i in (0..32).rev() {
            let inverted = !bytes[i];
            let (sum, c) = inverted.overflowing_add(if carry { 1 } else { 0 });
            abs[i] = sum;
            carry = c;
        }
        (true, abs)
    } else {
        let mut abs = [0u8; 32];
        abs.copy_from_slice(bytes);
        (false, abs)
    };

    // Convert big-endian bytes to decimal string via repeated division
    let mut digits = Vec::new();
    let mut value = abs_bytes.to_vec();
    loop {
        let all_zero = value.iter().all(|&b| b == 0);
        if all_zero {
            break;
        }
        let mut remainder: u16 = 0;
        for byte in value.iter_mut() {
            let cur = (remainder << 8) | (*byte as u16);
            *byte = (cur / 10) as u8;
            remainder = cur % 10;
        }
        digits.push(b'0' + remainder as u8);
    }
    if digits.is_empty() {
        digits.push(b'0');
    }
    digits.reverse();
    let digit_str = String::from_utf8(digits).unwrap_or_else(|_| "0".to_string());

    if scale == 0 {
        if is_neg { format!("-{}", digit_str) } else { digit_str }
    } else if digit_str.len() <= scale {
        let padded = format!("{:0>width$}", digit_str, width = scale);
        if is_neg {
            format!("-0.{}", padded)
        } else {
            format!("0.{}", padded)
        }
    } else {
        let split_at = digit_str.len() - scale;
        let (int_part, frac_part) = digit_str.split_at(split_at);
        if is_neg {
            format!("-{}.{}", int_part, frac_part)
        } else {
            format!("{}.{}", int_part, frac_part)
        }
    }
}

/// Format a klickhouse value as text (non-recursive, for simple values).
fn klickhouse_value_to_text(value: &klickhouse::Value) -> String {
    match value {
        klickhouse::Value::Null => "NULL".to_string(),
        klickhouse::Value::String(bytes) => {
            String::from_utf8_lossy(bytes).into_owned()
        }
        klickhouse::Value::UInt8(v) => v.to_string(),
        klickhouse::Value::UInt16(v) => v.to_string(),
        klickhouse::Value::UInt32(v) => v.to_string(),
        klickhouse::Value::UInt64(v) => v.to_string(),
        klickhouse::Value::Int8(v) => v.to_string(),
        klickhouse::Value::Int16(v) => v.to_string(),
        klickhouse::Value::Int32(v) => v.to_string(),
        klickhouse::Value::Int64(v) => v.to_string(),
        klickhouse::Value::Float32(v) => v.to_string(),
        klickhouse::Value::Float64(v) => v.to_string(),
        klickhouse::Value::Uuid(u) => u.to_string(),
        klickhouse::Value::Ipv4(ip) => ip.to_string(),
        klickhouse::Value::Ipv6(ip) => ip.to_string(),
        klickhouse::Value::Int128(v) => v.to_string(),
        klickhouse::Value::UInt128(v) => v.to_string(),
        klickhouse::Value::Int256(v) => v.to_string(),
        klickhouse::Value::UInt256(v) => v.to_string(),
        other => format!("{:?}", other),
    }
}

/// Format a klickhouse Array/Tuple as a Postgres-style text array.
fn klickhouse_array_to_text(arr: &[klickhouse::Value]) -> String {
    let mut s = String::from("[");
    for (i, v) in arr.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&klickhouse_value_to_text(v));
    }
    s.push(']');
    s
}

/// Escape a string for embedding in a JSON-like double-quoted context.
fn escape_json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            _ => out.push(ch),
        }
    }
    out
}

/// Format a klickhouse Map as a JSON-like text object.
fn klickhouse_map_to_text(keys: &[klickhouse::Value], values: &[klickhouse::Value]) -> String {
    let mut s = String::from("{");
    for (i, (k, v)) in keys.iter().zip(values.iter()).enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push('"');
        s.push_str(&escape_json_string(&klickhouse_value_to_text(k)));
        s.push_str("\":");
        if matches!(v, klickhouse::Value::String(_)) {
            s.push('"');
            s.push_str(&escape_json_string(&klickhouse_value_to_text(v)));
            s.push('"');
        } else {
            s.push_str(&klickhouse_value_to_text(v));
        }
    }
    s.push('}');
    s
}

// =============================================================================
// Arrow DataType / RecordBatch → pgwire encoding
// =============================================================================
//
// Used for catalog queries executed through DataFusion (pg_catalog,
// information_schema). DataFusion returns Arrow RecordBatch results which
// must be converted to pgwire DataRow format.

/// Map an Arrow `DataType` to the closest pgwire `Type`.
pub fn arrow_type_to_pg(dt: &DataType) -> Type {
    match dt {
        DataType::Boolean => Type::BOOL,
        DataType::Int8 | DataType::Int16 => Type::INT2,
        DataType::Int32 | DataType::UInt16 => Type::INT4,
        DataType::Int64 | DataType::UInt32 => Type::INT8,
        DataType::UInt8 => Type::INT2,
        DataType::UInt64 => Type::NUMERIC,
        DataType::Float16 | DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Type::TEXT,
        DataType::Date32 | DataType::Date64 => Type::DATE,
        DataType::Timestamp(_, None) => Type::TIMESTAMP,
        DataType::Timestamp(_, Some(_)) => Type::TIMESTAMPTZ,
        DataType::Time32(_) | DataType::Time64(_) => Type::TEXT,
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => Type::NUMERIC,
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => Type::BYTEA,
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => Type::TEXT,
        DataType::Struct(_) | DataType::Map(_, _) => Type::TEXT,
        DataType::Null => Type::TEXT,
        _ => Type::TEXT,
    }
}

/// Returns `true` for Arrow data types that `encode_arrow_value` handles
/// natively (without needing an external `ArrayFormatter`).
fn is_native_pg_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::FixedSizeBinary(_)
    )
}

/// Encode a single value from an Arrow `ArrayRef` at the given row index
/// into a pgwire `DataRowEncoder`.
///
/// For non-primitive types, pass a pre-created `ArrayFormatter` to avoid
/// re-creating it for every row.
pub fn encode_arrow_value(
    encoder: &mut DataRowEncoder,
    array: &ArrayRef,
    row: usize,
    fallback_formatter: Option<&arrow::util::display::ArrayFormatter<'_>>,
) -> PgWireResult<()> {
    if array.is_null(row) {
        return encoder.encode_field(&None::<&str>);
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().expect("matched DataType::Boolean");
            encoder.encode_field(&arr.value(row))
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().expect("matched DataType::Int8");
            encoder.encode_field(&(arr.value(row) as i16))
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().expect("matched DataType::Int16");
            encoder.encode_field(&arr.value(row))
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().expect("matched DataType::Int32");
            encoder.encode_field(&arr.value(row))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().expect("matched DataType::Int64");
            encoder.encode_field(&arr.value(row))
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().expect("matched DataType::UInt8");
            encoder.encode_field(&(arr.value(row) as i16))
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().expect("matched DataType::UInt16");
            encoder.encode_field(&(arr.value(row) as i32))
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().expect("matched DataType::UInt32");
            encoder.encode_field(&(arr.value(row) as i64))
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().expect("matched DataType::UInt64");
            encoder.encode_field(&arr.value(row).to_string())
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().expect("matched DataType::Float32");
            encoder.encode_field(&arr.value(row))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().expect("matched DataType::Float64");
            encoder.encode_field(&arr.value(row))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().expect("matched DataType::Utf8");
            encoder.encode_field(&arr.value(row))
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().expect("matched DataType::LargeUtf8");
            encoder.encode_field(&arr.value(row))
        }
        DataType::Utf8View => {
            let arr = array.as_any().downcast_ref::<StringViewArray>().expect("matched DataType::Utf8View");
            encoder.encode_field(&arr.value(row))
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().expect("matched DataType::Binary");
            encoder.encode_field(&bytes_to_pg_hex(arr.value(row)))
        }
        DataType::LargeBinary => {
            let arr = array.as_any().downcast_ref::<LargeBinaryArray>().expect("matched DataType::LargeBinary");
            encoder.encode_field(&bytes_to_pg_hex(arr.value(row)))
        }
        DataType::FixedSizeBinary(_) => {
            let arr = array.as_any().downcast_ref::<arrow::array::FixedSizeBinaryArray>()
                .expect("matched DataType::FixedSizeBinary");
            encoder.encode_field(&bytes_to_pg_hex(arr.value(row)))
        }
        _ => {
            let text = format_arrow_value(array.as_ref(), row, fallback_formatter)?;
            encoder.encode_field(&text)
        }
    }
}

/// `FormatOptions` configured for PostgreSQL-compatible output.
///
/// Timestamps use a space separator (not the ISO 8601 'T') and include
/// sub-second precision.  Timezone-aware timestamps append the UTC offset.
fn pg_format_options() -> arrow::util::display::FormatOptions<'static> {
    arrow::util::display::FormatOptions::new()
        .with_timestamp_format(Some("%Y-%m-%d %H:%M:%S%.f"))
        .with_timestamp_tz_format(Some("%Y-%m-%d %H:%M:%S%.f%:z"))
}

/// Format an Arrow value to text using a pre-created or on-demand `ArrayFormatter`.
fn format_arrow_value(
    array: &dyn Array,
    row: usize,
    fallback_formatter: Option<&arrow::util::display::ArrayFormatter<'_>>,
) -> PgWireResult<String> {
    if let Some(fmt) = fallback_formatter {
        Ok(fmt.value(row).to_string())
    } else {
        let formatter = arrow::util::display::ArrayFormatter::try_new(
            array,
            &pg_format_options(),
        );
        match formatter {
            Ok(fmt) => Ok(fmt.value(row).to_string()),
            Err(e) => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("Failed to format Arrow value: {}", e),
            )))),
        }
    }
}

/// Encode raw bytes as PostgreSQL hex-format bytea (`\x` prefix followed by hex pairs).
fn bytes_to_pg_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(2 + bytes.len() * 2);
    s.push_str("\\x");
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{:02x}", b);
    }
    s
}

/// Convert Arrow `RecordBatch` results (from DataFusion) to pgwire `Response`.
///
/// Builds field descriptors from the Arrow schema and encodes each row
/// using text format. This is the bridge between DataFusion catalog query
/// results and the pgwire wire protocol.
pub fn record_batches_to_response(batches: Vec<RecordBatch>) -> PgWireResult<Vec<Response>> {
    if batches.is_empty() {
        // Return an empty query response
        let fields = Arc::new(vec![]);
        let row_stream = stream::iter(Vec::new());
        let mut response = QueryResponse::new(fields, row_stream);
        response.set_command_tag("SELECT 0");
        return Ok(vec![Response::Query(response)]);
    }

    // Build field descriptors from the first batch's schema
    let schema = batches[0].schema();
    let fields: Vec<FieldInfo> = schema
        .fields()
        .iter()
        .map(|f| {
            let pg_type = arrow_type_to_pg(f.data_type());
            FieldInfo::new(
                f.name().clone(),
                None,
                None,
                pg_type,
                FieldFormat::Text,
            )
        })
        .collect();
    let fields = Arc::new(fields);

    // Encode all rows from all batches
    let mut data_rows = Vec::new();
    let mut total_rows = 0usize;

    for batch in &batches {
        // Pre-create ArrayFormatters for non-primitive columns (once per column per batch).
        // Fail fast if formatter creation fails instead of retrying per-row.
        let formatters: Vec<Option<arrow::util::display::ArrayFormatter>> = batch
            .columns()
            .iter()
            .map(|col| {
                if is_native_pg_type(col.data_type()) {
                    None
                } else {
                    Some(arrow::util::display::ArrayFormatter::try_new(
                        col.as_ref(),
                        &pg_format_options(),
                    ))
                }
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|opt| match opt {
                None => Ok(None),
                Some(Ok(fmt)) => Ok(Some(fmt)),
                Some(Err(e)) => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    format!("Failed to create Arrow formatter: {}", e),
                )))),
            })
            .collect::<PgWireResult<Vec<_>>>()?;

        for row_idx in 0..batch.num_rows() {
            let mut encoder = DataRowEncoder::new(fields.clone());
            for (col_idx, column) in batch.columns().iter().enumerate() {
                encode_arrow_value(
                    &mut encoder,
                    column,
                    row_idx,
                    formatters[col_idx].as_ref(),
                )?;
            }
            data_rows.push(Ok(encoder.take_row()));
            total_rows += 1;
        }
    }

    let row_stream = stream::iter(data_rows);
    let mut response = QueryResponse::new(fields, row_stream);
    response.set_command_tag(&format!("SELECT {}", total_rows));

    Ok(vec![Response::Query(response)])
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BooleanArray, Float64Array, Int32Array, Int64Array, StringArray, NullArray,
    };
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    #[test]
    fn test_clickhouse_type_mapping() {
        assert_eq!(clickhouse_type_to_pg("String"), Type::TEXT);
        assert_eq!(clickhouse_type_to_pg("Int32"), Type::INT4);
        assert_eq!(clickhouse_type_to_pg("Int64"), Type::INT8);
        assert_eq!(clickhouse_type_to_pg("Float64"), Type::FLOAT8);
        assert_eq!(clickhouse_type_to_pg("Bool"), Type::BOOL);
        assert_eq!(clickhouse_type_to_pg("Date"), Type::DATE);
        assert_eq!(clickhouse_type_to_pg("DateTime"), Type::TIMESTAMP);
        assert_eq!(clickhouse_type_to_pg("DateTime64(3)"), Type::TIMESTAMP);
        assert_eq!(clickhouse_type_to_pg("Decimal(18, 4)"), Type::NUMERIC);
        assert_eq!(clickhouse_type_to_pg("UUID"), Type::UUID);
        assert_eq!(clickhouse_type_to_pg("Nullable(UUID)"), Type::UUID);
    }

    #[test]
    fn test_nullable_stripping() {
        assert_eq!(clickhouse_type_to_pg("Nullable(Int32)"), Type::INT4);
        assert_eq!(clickhouse_type_to_pg("Nullable(String)"), Type::TEXT);
        assert_eq!(clickhouse_type_to_pg("Nullable(Float64)"), Type::FLOAT8);
    }

    // ── arrow_type_to_pg ──

    #[test]
    fn test_arrow_type_to_pg_integers() {
        assert_eq!(arrow_type_to_pg(&DataType::Boolean), Type::BOOL);
        assert_eq!(arrow_type_to_pg(&DataType::Int8), Type::INT2);
        assert_eq!(arrow_type_to_pg(&DataType::Int16), Type::INT2);
        assert_eq!(arrow_type_to_pg(&DataType::Int32), Type::INT4);
        assert_eq!(arrow_type_to_pg(&DataType::Int64), Type::INT8);
        // Unsigned widening
        assert_eq!(arrow_type_to_pg(&DataType::UInt8), Type::INT2);
        assert_eq!(arrow_type_to_pg(&DataType::UInt16), Type::INT4);
        assert_eq!(arrow_type_to_pg(&DataType::UInt32), Type::INT8);
        assert_eq!(arrow_type_to_pg(&DataType::UInt64), Type::NUMERIC);
    }

    #[test]
    fn test_arrow_type_to_pg_floats() {
        assert_eq!(arrow_type_to_pg(&DataType::Float16), Type::FLOAT4);
        assert_eq!(arrow_type_to_pg(&DataType::Float32), Type::FLOAT4);
        assert_eq!(arrow_type_to_pg(&DataType::Float64), Type::FLOAT8);
    }

    #[test]
    fn test_arrow_type_to_pg_strings_and_binary() {
        assert_eq!(arrow_type_to_pg(&DataType::Utf8), Type::TEXT);
        assert_eq!(arrow_type_to_pg(&DataType::LargeUtf8), Type::TEXT);
        assert_eq!(arrow_type_to_pg(&DataType::Binary), Type::BYTEA);
        assert_eq!(arrow_type_to_pg(&DataType::LargeBinary), Type::BYTEA);
        assert_eq!(arrow_type_to_pg(&DataType::FixedSizeBinary(16)), Type::BYTEA);
    }

    #[test]
    fn test_arrow_type_to_pg_temporal() {
        assert_eq!(arrow_type_to_pg(&DataType::Date32), Type::DATE);
        assert_eq!(arrow_type_to_pg(&DataType::Date64), Type::DATE);
        assert_eq!(
            arrow_type_to_pg(&DataType::Timestamp(TimeUnit::Microsecond, None)),
            Type::TIMESTAMP
        );
        assert_eq!(
            arrow_type_to_pg(&DataType::Timestamp(
                TimeUnit::Nanosecond,
                Some("UTC".into())
            )),
            Type::TIMESTAMPTZ
        );
        // Time types fall back to TEXT
        assert_eq!(
            arrow_type_to_pg(&DataType::Time32(TimeUnit::Millisecond)),
            Type::TEXT
        );
        assert_eq!(
            arrow_type_to_pg(&DataType::Time64(TimeUnit::Microsecond)),
            Type::TEXT
        );
    }

    #[test]
    fn test_arrow_type_to_pg_complex() {
        assert_eq!(arrow_type_to_pg(&DataType::Decimal128(18, 4)), Type::NUMERIC);
        assert_eq!(arrow_type_to_pg(&DataType::Decimal256(38, 10)), Type::NUMERIC);
        // List, Struct, Map, Null all fall back to TEXT
        assert_eq!(
            arrow_type_to_pg(&DataType::List(Arc::new(Field::new("item", DataType::Int32, true)))),
            Type::TEXT
        );
        assert_eq!(
            arrow_type_to_pg(&DataType::Struct(vec![Field::new("a", DataType::Utf8, true)].into())),
            Type::TEXT
        );
        assert_eq!(
            arrow_type_to_pg(&DataType::Map(
                Arc::new(Field::new("entries", DataType::Utf8, true)),
                false,
            )),
            Type::TEXT
        );
        assert_eq!(arrow_type_to_pg(&DataType::Null), Type::TEXT);
    }

    // ── encode_value ──

    /// Helper: create a DataRowEncoder with a specific type.
    fn make_typed_encoder(pg_type: Type) -> (DataRowEncoder, Arc<Vec<FieldInfo>>) {
        let fields = Arc::new(vec![FieldInfo::new(
            "test".to_owned(),
            None,
            None,
            pg_type,
            FieldFormat::Text,
        )]);
        (DataRowEncoder::new(fields.clone()), fields)
    }

    #[test]
    fn test_encode_value_null() {
        // NULL should encode without error for any type
        let types = [Type::BOOL, Type::INT2, Type::INT4, Type::INT8, Type::FLOAT4, Type::FLOAT8, Type::TEXT];
        for pg_type in &types {
            let (mut encoder, _) = make_typed_encoder(pg_type.clone());
            encode_value(&mut encoder, &serde_json::Value::Null, pg_type)
                .unwrap_or_else(|e| panic!("NULL encode failed for {:?}: {}", pg_type, e));
        }
    }

    #[test]
    fn test_encode_value_bool_coercion() {
        // Direct bool
        let (mut enc, _) = make_typed_encoder(Type::BOOL);
        encode_value(&mut enc, &serde_json::json!(true), &Type::BOOL).unwrap();

        let (mut enc, _) = make_typed_encoder(Type::BOOL);
        encode_value(&mut enc, &serde_json::json!(false), &Type::BOOL).unwrap();

        // Number -> bool (1 = true, 0 = false)
        let (mut enc, _) = make_typed_encoder(Type::BOOL);
        encode_value(&mut enc, &serde_json::json!(1), &Type::BOOL).unwrap();

        let (mut enc, _) = make_typed_encoder(Type::BOOL);
        encode_value(&mut enc, &serde_json::json!(0), &Type::BOOL).unwrap();

        // String -> bool is no longer supported (strings should not arrive as BOOL)
        let (mut enc, _) = make_typed_encoder(Type::BOOL);
        assert!(encode_value(&mut enc, &serde_json::json!("true"), &Type::BOOL).is_err());

        let (mut enc, _) = make_typed_encoder(Type::BOOL);
        assert!(encode_value(&mut enc, &serde_json::json!("false"), &Type::BOOL).is_err());
    }

    #[test]
    fn test_encode_value_integers() {
        // INT2
        let (mut enc, _) = make_typed_encoder(Type::INT2);
        encode_value(&mut enc, &serde_json::json!(42), &Type::INT2).unwrap();

        // INT4
        let (mut enc, _) = make_typed_encoder(Type::INT4);
        encode_value(&mut enc, &serde_json::json!(100000), &Type::INT4).unwrap();

        // INT8
        let (mut enc, _) = make_typed_encoder(Type::INT8);
        encode_value(&mut enc, &serde_json::json!(9999999999i64), &Type::INT8).unwrap();

        // String -> int coercion
        let (mut enc, _) = make_typed_encoder(Type::INT4);
        encode_value(&mut enc, &serde_json::json!("123"), &Type::INT4).unwrap();

        // Bool -> int coercion
        let (mut enc, _) = make_typed_encoder(Type::INT4);
        encode_value(&mut enc, &serde_json::json!(true), &Type::INT4).unwrap();
    }

    #[test]
    fn test_encode_value_floats() {
        let (mut enc, _) = make_typed_encoder(Type::FLOAT4);
        encode_value(&mut enc, &serde_json::json!(3.14), &Type::FLOAT4).unwrap();

        let (mut enc, _) = make_typed_encoder(Type::FLOAT8);
        encode_value(&mut enc, &serde_json::json!(2.718281828), &Type::FLOAT8).unwrap();

        // String -> float coercion
        let (mut enc, _) = make_typed_encoder(Type::FLOAT8);
        encode_value(&mut enc, &serde_json::json!("99.99"), &Type::FLOAT8).unwrap();
    }

    #[test]
    fn test_encode_value_text_fallback() {
        // DATE, TIMESTAMP, NUMERIC, and unknown types use text encoding
        let (mut enc, _) = make_typed_encoder(Type::DATE);
        encode_value(&mut enc, &serde_json::json!("2024-01-15"), &Type::DATE).unwrap();

        let (mut enc, _) = make_typed_encoder(Type::TIMESTAMP);
        encode_value(&mut enc, &serde_json::json!("2024-01-15T10:30:00Z"), &Type::TIMESTAMP).unwrap();

        let (mut enc, _) = make_typed_encoder(Type::NUMERIC);
        encode_value(&mut enc, &serde_json::json!("12345.6789"), &Type::NUMERIC).unwrap();

        // JSON objects/arrays are serialized as text
        let (mut enc, _) = make_typed_encoder(Type::TEXT);
        encode_value(&mut enc, &serde_json::json!({"key": "value"}), &Type::TEXT).unwrap();

        let (mut enc, _) = make_typed_encoder(Type::TEXT);
        encode_value(&mut enc, &serde_json::json!([1, 2, 3]), &Type::TEXT).unwrap();
    }

    // ── encode_arrow_value ──

    #[test]
    fn test_encode_arrow_value_boolean() {
        let arr: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), Some(false), None]));
        let fields = Arc::new(vec![FieldInfo::new(
            "v".to_owned(), None, None, Type::BOOL, FieldFormat::Text,
        )]);

        let mut enc = DataRowEncoder::new(fields.clone());
        encode_arrow_value(&mut enc, &arr, 0, None).unwrap();

        let mut enc = DataRowEncoder::new(fields.clone());
        encode_arrow_value(&mut enc, &arr, 1, None).unwrap();

        let mut enc = DataRowEncoder::new(fields.clone());
        encode_arrow_value(&mut enc, &arr, 2, None).unwrap();
    }

    #[test]
    fn test_encode_arrow_value_int32() {
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![Some(42), Some(-1), None]));
        let fields = Arc::new(vec![FieldInfo::new(
            "v".to_owned(), None, None, Type::INT4, FieldFormat::Text,
        )]);

        let mut enc = DataRowEncoder::new(fields.clone());
        encode_arrow_value(&mut enc, &arr, 0, None).unwrap();

        let mut enc = DataRowEncoder::new(fields.clone());
        encode_arrow_value(&mut enc, &arr, 1, None).unwrap();

        let mut enc = DataRowEncoder::new(fields.clone());
        encode_arrow_value(&mut enc, &arr, 2, None).unwrap();
    }

    #[test]
    fn test_encode_arrow_value_int64() {
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![Some(9999999999i64), None]));
        let fields = Arc::new(vec![FieldInfo::new(
            "v".to_owned(), None, None, Type::INT8, FieldFormat::Text,
        )]);

        let mut enc = DataRowEncoder::new(fields.clone());
        encode_arrow_value(&mut enc, &arr, 0, None).unwrap();

        let mut enc = DataRowEncoder::new(fields.clone());
        encode_arrow_value(&mut enc, &arr, 1, None).unwrap();
    }

    #[test]
    fn test_encode_arrow_value_float64() {
        let arr: ArrayRef = Arc::new(Float64Array::from(vec![Some(3.14), Some(-0.001), None]));
        let fields = Arc::new(vec![FieldInfo::new(
            "v".to_owned(), None, None, Type::FLOAT8, FieldFormat::Text,
        )]);

        let mut enc = DataRowEncoder::new(fields.clone());
        encode_arrow_value(&mut enc, &arr, 0, None).unwrap();

        let mut enc = DataRowEncoder::new(fields.clone());
        encode_arrow_value(&mut enc, &arr, 2, None).unwrap();
    }

    #[test]
    fn test_encode_arrow_value_utf8() {
        let arr: ArrayRef = Arc::new(StringArray::from(vec![Some("hello"), Some(""), None]));
        let fields = Arc::new(vec![FieldInfo::new(
            "v".to_owned(), None, None, Type::TEXT, FieldFormat::Text,
        )]);

        let mut enc = DataRowEncoder::new(fields.clone());
        encode_arrow_value(&mut enc, &arr, 0, None).unwrap();

        let mut enc = DataRowEncoder::new(fields.clone());
        encode_arrow_value(&mut enc, &arr, 1, None).unwrap();

        let mut enc = DataRowEncoder::new(fields.clone());
        encode_arrow_value(&mut enc, &arr, 2, None).unwrap();
    }

    #[test]
    fn test_encode_arrow_value_null_array() {
        let arr: ArrayRef = Arc::new(NullArray::new(2));
        let fields = Arc::new(vec![FieldInfo::new(
            "v".to_owned(), None, None, Type::TEXT, FieldFormat::Text,
        )]);

        let mut enc = DataRowEncoder::new(fields.clone());
        encode_arrow_value(&mut enc, &arr, 0, None).unwrap();
    }

    #[test]
    fn test_record_batches_to_response_empty() {
        let result = record_batches_to_response(vec![]).unwrap();
        assert_eq!(result.len(), 1, "Expected one Response");
    }

    #[test]
    fn test_record_batches_to_response_with_data() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("alice"), Some("bob")])),
            ],
        )
        .unwrap();

        let result = record_batches_to_response(vec![batch]).unwrap();
        assert_eq!(result.len(), 1, "Expected one Response");
    }

    // ── column_info_to_field_info_with_format ──

    #[test]
    fn test_field_info_binary_supported_types() {
        use crate::warehouse::query::executor::ColumnInfo;

        // Types that should get Binary format when requested
        let supported = vec![
            ("Bool", Type::BOOL),
            ("Int8", Type::INT2),
            ("Int32", Type::INT4),
            ("Int64", Type::INT8),
            ("Float32", Type::FLOAT4),
            ("Float64", Type::FLOAT8),
            ("String", Type::TEXT),
        ];

        for (ch_type, expected_pg_type) in &supported {
            let col = ColumnInfo {
                name: "test".to_owned(),
                data_type: ch_type.to_string(),
                nullable: false,
            };
            let fi = column_info_to_field_info_with_format(&col, FieldFormat::Binary);
            assert_eq!(
                fi.format(),
                FieldFormat::Binary,
                "Expected Binary format for ClickHouse type {}, pg_type {:?}",
                ch_type,
                expected_pg_type
            );
        }
    }

    #[test]
    fn test_field_info_binary_fallback_for_unsupported() {
        use crate::warehouse::query::executor::ColumnInfo;

        // Types that should fall back to Text format even when Binary is requested
        let unsupported = vec!["Date", "DateTime", "Decimal(18, 4)"];

        for ch_type in &unsupported {
            let col = ColumnInfo {
                name: "test".to_owned(),
                data_type: ch_type.to_string(),
                nullable: false,
            };
            let fi = column_info_to_field_info_with_format(&col, FieldFormat::Binary);
            assert_eq!(
                fi.format(),
                FieldFormat::Text,
                "Expected Text fallback for unsupported type {}",
                ch_type
            );
        }
    }

    #[test]
    fn test_field_info_text_always_text() {
        use crate::warehouse::query::executor::ColumnInfo;

        // When Text is requested, any type should get Text format
        let all_types = vec!["Bool", "Int32", "Float64", "String", "Date", "DateTime"];

        for ch_type in &all_types {
            let col = ColumnInfo {
                name: "test".to_owned(),
                data_type: ch_type.to_string(),
                nullable: false,
            };
            let fi = column_info_to_field_info_with_format(&col, FieldFormat::Text);
            assert_eq!(
                fi.format(),
                FieldFormat::Text,
                "Expected Text format when Text requested for type {}",
                ch_type
            );
        }
    }

    // ── Value coercion corner cases ──

    #[test]
    fn test_encode_value_int2_overflow() {
        // 99999 doesn't fit in i16 -- should return an error, not panic
        let (mut enc, _) = make_typed_encoder(Type::INT2);
        let result = encode_value(&mut enc, &serde_json::json!(99999), &Type::INT2);
        assert!(result.is_err(), "Expected overflow error for INT2, got Ok");
    }

    #[test]
    fn test_encode_value_int8_from_float() {
        // Float truncates to integer -- should not panic
        let (mut enc, _) = make_typed_encoder(Type::INT8);
        encode_value(&mut enc, &serde_json::json!(3.14), &Type::INT8).unwrap();
    }

    #[test]
    fn test_encode_value_int4_non_numeric_string_returns_error() {
        let (mut enc, _) = make_typed_encoder(Type::INT4);
        let result = encode_value(&mut enc, &serde_json::json!("abc"), &Type::INT4);
        assert!(result.is_err(), "Non-numeric string must return an error, not silently coerce to 0");
    }

    #[test]
    fn test_encode_value_int4_from_array() {
        // Arrays cannot be meaningfully converted to integers
        let (mut enc, _) = make_typed_encoder(Type::INT4);
        assert!(encode_value(&mut enc, &serde_json::json!([1, 2]), &Type::INT4).is_err());
    }

    #[test]
    fn test_encode_value_bool_from_object() {
        let (mut enc, _) = make_typed_encoder(Type::BOOL);
        assert!(
            encode_value(&mut enc, &serde_json::json!({"a": 1}), &Type::BOOL).is_err(),
            "JSON object must not silently coerce to boolean"
        );
    }

    #[test]
    fn test_encode_value_text_from_bool() {
        // Bool encoded as TEXT produces "true"/"false" strings
        let (mut enc, _) = make_typed_encoder(Type::TEXT);
        encode_value(&mut enc, &serde_json::json!(true), &Type::TEXT).unwrap();
    }

    #[test]
    fn test_encode_value_text_from_nested_json() {
        // Nested JSON serializes to JSON text -- should not panic
        let (mut enc, _) = make_typed_encoder(Type::TEXT);
        encode_value(&mut enc, &serde_json::json!({"a": [1, 2]}), &Type::TEXT).unwrap();
    }

    // ==================== clickhouse_type_to_pg Advanced Tests ====================

    #[test]
    fn test_clickhouse_type_to_pg_low_cardinality_nullable() {
        // LowCardinality(Nullable(String)) -- not explicitly handled,
        // falls to TEXT via default path (LowCardinality doesn't start with Nullable)
        let pg_type = clickhouse_type_to_pg("LowCardinality(Nullable(String))");
        assert_eq!(pg_type, Type::TEXT);
    }

    #[test]
    fn test_clickhouse_type_to_pg_datetime64_with_tz() {
        let pg_type = clickhouse_type_to_pg("DateTime64(6, 'UTC')");
        assert_eq!(pg_type, Type::TIMESTAMPTZ);
    }

    #[test]
    fn test_clickhouse_type_to_pg_datetime64_precision_3() {
        let pg_type = clickhouse_type_to_pg("DateTime64(3)");
        assert_eq!(pg_type, Type::TIMESTAMP);
    }

    #[test]
    fn test_clickhouse_type_to_pg_decimal_high_precision() {
        let pg_type = clickhouse_type_to_pg("Decimal(38, 18)");
        assert_eq!(pg_type, Type::NUMERIC);
    }

    #[test]
    fn test_clickhouse_type_to_pg_nullable_decimal() {
        let pg_type = clickhouse_type_to_pg("Nullable(Decimal(18, 4))");
        assert_eq!(pg_type, Type::NUMERIC);
    }

    #[test]
    fn test_clickhouse_type_to_pg_nullable_datetime64() {
        let pg_type = clickhouse_type_to_pg("Nullable(DateTime64(3))");
        assert_eq!(pg_type, Type::TIMESTAMP);
    }

    #[test]
    fn test_clickhouse_type_to_pg_fixed_string() {
        let pg_type = clickhouse_type_to_pg("FixedString(36)");
        assert_eq!(pg_type, Type::TEXT);
    }

    #[test]
    fn test_clickhouse_type_to_pg_unknown_type() {
        let pg_type = clickhouse_type_to_pg("SomeCustomType");
        assert_eq!(pg_type, Type::TEXT);
    }

    // ==================== encode_value Boundary Tests ====================

    #[test]
    fn test_encode_value_int4_at_max_boundary() {
        let (mut enc, _) = make_typed_encoder(Type::INT4);
        encode_value(&mut enc, &serde_json::json!(i32::MAX), &Type::INT4).unwrap();
    }

    #[test]
    fn test_encode_value_int4_at_min_boundary() {
        let (mut enc, _) = make_typed_encoder(Type::INT4);
        encode_value(&mut enc, &serde_json::json!(i32::MIN), &Type::INT4).unwrap();
    }

    #[test]
    fn test_encode_value_float8_json_null_from_nan() {
        // serde_json::json!(f64::NAN) produces Value::Null since NaN
        // is not representable in JSON. This test verifies null encoding.
        let (mut enc, _) = make_typed_encoder(Type::FLOAT8);
        encode_value(&mut enc, &serde_json::json!(null), &Type::FLOAT8).unwrap();
    }

    #[test]
    fn test_encode_value_float8_nan_string() {
        // ClickHouse may return "nan" as a JSON string for NaN values.
        // value_to_f64 parses this to f64::NAN which pgwire encodes correctly.
        let (mut enc, _) = make_typed_encoder(Type::FLOAT8);
        encode_value(&mut enc, &serde_json::json!("nan"), &Type::FLOAT8).unwrap();
    }

    #[test]
    fn test_encode_value_float8_infinity_string() {
        let (mut enc, _) = make_typed_encoder(Type::FLOAT8);
        encode_value(&mut enc, &serde_json::json!("inf"), &Type::FLOAT8).unwrap();
    }

    #[test]
    fn test_encode_value_float8_negative_infinity_string() {
        let (mut enc, _) = make_typed_encoder(Type::FLOAT8);
        encode_value(&mut enc, &serde_json::json!("-inf"), &Type::FLOAT8).unwrap();
    }

    #[test]
    fn test_encode_value_int8_max_boundary() {
        let (mut enc, _) = make_typed_encoder(Type::INT8);
        encode_value(&mut enc, &serde_json::json!(i64::MAX), &Type::INT8).unwrap();
    }

    // ==================== record_batches_to_response Tests ====================

    #[test]
    fn test_record_batches_to_response_multiple_batches() {
        use arrow::datatypes::{Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef],
        ).unwrap();

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![3, 4, 5])) as ArrayRef],
        ).unwrap();

        let response = record_batches_to_response(vec![batch1, batch2]).unwrap();
        // Should return exactly one Response
        assert_eq!(response.len(), 1);
    }

    #[test]
    fn test_value_to_i64_large_u64_returns_error() {
        let large = serde_json::json!(u64::MAX);
        assert!(value_to_i64(&large).is_err(), "u64::MAX must return an error");

        let just_over = serde_json::json!(i64::MAX as u64 + 1);
        assert!(value_to_i64(&just_over).is_err(), "i64::MAX+1 must return an error");

        let fits = serde_json::json!(i64::MAX as u64);
        assert_eq!(value_to_i64(&fits).unwrap(), i64::MAX, "i64::MAX should convert correctly");
    }

    #[test]
    fn test_value_to_i64_normal_values() {
        assert_eq!(value_to_i64(&serde_json::json!(0)).unwrap(), 0);
        assert_eq!(value_to_i64(&serde_json::json!(42)).unwrap(), 42);
        assert_eq!(value_to_i64(&serde_json::json!(-1)).unwrap(), -1);
        assert_eq!(value_to_i64(&serde_json::json!("123")).unwrap(), 123);
        assert_eq!(value_to_i64(&serde_json::json!(true)).unwrap(), 1);
        assert_eq!(value_to_i64(&serde_json::json!(false)).unwrap(), 0);
        // Null is handled by encode_value before reaching value_to_i64
        assert!(value_to_i64(&serde_json::json!(null)).is_err());
    }

    #[test]
    fn test_encode_arrow_uint64_no_overflow() {
        let arr: ArrayRef = Arc::new(UInt64Array::from(vec![
            Some(42u64),
            Some(i64::MAX as u64),
            Some(i64::MAX as u64 + 1),
            Some(u64::MAX),
            None,
        ]));
        let fields = Arc::new(vec![FieldInfo::new(
            "v".to_owned(), None, None, Type::NUMERIC, FieldFormat::Text,
        )]);

        // All u64 values should encode as NUMERIC strings without error
        let mut enc = DataRowEncoder::new(fields.clone());
        encode_arrow_value(&mut enc, &arr, 0, None).unwrap();

        let mut enc = DataRowEncoder::new(fields.clone());
        encode_arrow_value(&mut enc, &arr, 1, None).unwrap();

        // Values exceeding i64::MAX encode correctly as NUMERIC
        let mut enc = DataRowEncoder::new(fields.clone());
        encode_arrow_value(&mut enc, &arr, 2, None).unwrap();

        let mut enc = DataRowEncoder::new(fields.clone());
        encode_arrow_value(&mut enc, &arr, 3, None).unwrap();

        // NULL should encode without error
        let mut enc = DataRowEncoder::new(fields.clone());
        encode_arrow_value(&mut enc, &arr, 4, None).unwrap();
    }

    #[test]
    fn test_clickhouse_datetime_tz_maps_to_timestamptz() {
        assert_eq!(clickhouse_type_to_pg("DateTime('UTC')"), Type::TIMESTAMPTZ);
        assert_eq!(clickhouse_type_to_pg("DateTime('Europe/Berlin')"), Type::TIMESTAMPTZ);
        assert_eq!(clickhouse_type_to_pg("DateTime"), Type::TIMESTAMP);
    }

    #[test]
    fn test_clickhouse_lowcardinality_stripped() {
        assert_eq!(clickhouse_type_to_pg("LowCardinality(String)"), Type::TEXT);
        assert_eq!(clickhouse_type_to_pg("LowCardinality(Int32)"), Type::INT4);
        assert_eq!(clickhouse_type_to_pg("LowCardinality(UInt8)"), Type::INT2);
        assert_eq!(clickhouse_type_to_pg("LowCardinality(Nullable(String))"), Type::TEXT);
        assert_eq!(clickhouse_type_to_pg("LowCardinality(Nullable(Int32))"), Type::INT4);
    }

    #[test]
    fn test_arrow_timestamp_tz_mapping() {
        assert_eq!(
            arrow_type_to_pg(&DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)),
            Type::TIMESTAMP,
        );
        assert_eq!(
            arrow_type_to_pg(&DataType::Timestamp(
                arrow::datatypes::TimeUnit::Millisecond,
                Some("UTC".into()),
            )),
            Type::TIMESTAMPTZ,
        );
    }

    // ── value_to_i64 edge cases ──

    #[test]
    fn test_value_to_i64_rejects_float_above_i64_max() {
        // i64::MAX as f64 rounds up to 9223372036854775808.0 which is i64::MAX+1
        let val = serde_json::json!(9223372036854775808.0_f64);
        assert!(
            value_to_i64(&val).is_err(),
            "Float equal to i64::MAX as f64 (which rounds up) must be rejected"
        );
    }

    #[test]
    fn test_value_to_i64_accepts_valid_float() {
        let val = serde_json::json!(42.0);
        assert_eq!(value_to_i64(&val).unwrap(), 42);
    }

    #[test]
    fn test_value_to_i64_rejects_json_null_from_nan() {
        // json!(f64::NAN) produces Value::Null because JSON has no NaN representation.
        // This test verifies that such null values are rejected.
        let val = serde_json::json!(f64::NAN);
        assert!(val.is_null(), "json!(f64::NAN) must produce Value::Null");
        assert!(value_to_i64(&val).is_err());
    }

    #[test]
    fn test_value_to_i64_rejects_json_null_from_infinity() {
        // json!(f64::INFINITY) produces Value::Null because JSON has no infinity representation.
        let val = serde_json::json!(f64::INFINITY);
        assert!(val.is_null(), "json!(f64::INFINITY) must produce Value::Null");
        assert!(value_to_i64(&val).is_err());
    }

    #[test]
    fn test_value_to_i64_float_boundary_i64_min() {
        // i64::MIN is exactly representable in f64
        let val = serde_json::json!(i64::MIN as f64);
        // serde_json may parse this as i64 directly; if so, as_i64() handles it.
        // Either way, the result must be i64::MIN.
        assert_eq!(
            value_to_i64(&val).unwrap(),
            i64::MIN,
            "i64::MIN (exactly representable in f64) must be accepted"
        );
    }

    #[test]
    fn test_value_to_i64_float_boundary_below_i64_min() {
        // A value below i64::MIN must be rejected.
        // i64::MIN as f64 = -9223372036854775808.0, subtract one ULP
        let below = (i64::MIN as f64) * 1.0000000000000002;
        let val = serde_json::Value::Number(
            serde_json::Number::from_f64(below).expect("finite")
        );
        assert!(
            value_to_i64(&val).is_err(),
            "Value below i64::MIN must be rejected"
        );
    }

    #[test]
    fn test_value_to_i64_float_large_positive_within_range() {
        // A large positive float that fits in i64
        let val = serde_json::json!(1e18);
        let result = value_to_i64(&val).unwrap();
        assert_eq!(result, 1_000_000_000_000_000_000i64);
    }

    // ── FLOAT4 overflow ──

    #[test]
    fn test_float4_rejects_overflow() {
        let val = serde_json::json!(1e300);
        let mut encoder = DataRowEncoder::new(std::sync::Arc::new(vec![
            FieldInfo::new("col".to_string(), None, None, Type::FLOAT4, FieldFormat::Binary),
        ]));
        let result = encode_value(&mut encoder, &val, &Type::FLOAT4);
        assert!(
            result.is_err(),
            "f64 value exceeding f32::MAX must be rejected for FLOAT4"
        );
    }

    #[test]
    fn test_float4_accepts_valid_value() {
        let val = serde_json::json!(3.14);
        let mut encoder = DataRowEncoder::new(std::sync::Arc::new(vec![
            FieldInfo::new("col".to_string(), None, None, Type::FLOAT4, FieldFormat::Binary),
        ]));
        let result = encode_value(&mut encoder, &val, &Type::FLOAT4);
        assert!(result.is_ok());
    }

    #[test]
    fn test_bool_rejects_json_array() {
        let val = serde_json::json!([1, 2, 3]);
        let mut encoder = DataRowEncoder::new(std::sync::Arc::new(vec![
            FieldInfo::new("col".to_string(), None, None, Type::BOOL, FieldFormat::Text),
        ]));
        let result = encode_value(&mut encoder, &val, &Type::BOOL);
        assert!(
            result.is_err(),
            "JSON array must not silently coerce to boolean"
        );
    }

    #[test]
    fn test_bool_rejects_json_object() {
        let val = serde_json::json!({"key": "value"});
        let mut encoder = DataRowEncoder::new(std::sync::Arc::new(vec![
            FieldInfo::new("col".to_string(), None, None, Type::BOOL, FieldFormat::Text),
        ]));
        let result = encode_value(&mut encoder, &val, &Type::BOOL);
        assert!(
            result.is_err(),
            "JSON object must not silently coerce to boolean"
        );
    }

    #[test]
    fn test_format_decimal_128_negative_between_zero_and_minus_one() {
        assert_eq!(format_decimal_128(-5, 2), "-0.05");
        assert_eq!(format_decimal_128(-1, 1), "-0.1");
        assert_eq!(format_decimal_128(-99, 2), "-0.99");
        assert_eq!(format_decimal_128(-1, 3), "-0.001");
        assert_eq!(format_decimal_128(-999, 4), "-0.0999");
    }

    #[test]
    fn test_format_decimal_128_positive_and_negative_normal() {
        assert_eq!(format_decimal_128(12345, 2), "123.45");
        assert_eq!(format_decimal_128(-12345, 2), "-123.45");
        assert_eq!(format_decimal_128(0, 2), "0.00");
        assert_eq!(format_decimal_128(100, 2), "1.00");
        assert_eq!(format_decimal_128(-100, 2), "-1.00");
    }

    #[test]
    fn test_format_decimal_128_scale_zero() {
        assert_eq!(format_decimal_128(42, 0), "42");
        assert_eq!(format_decimal_128(-42, 0), "-42");
        assert_eq!(format_decimal_128(0, 0), "0");
    }

    #[test]
    fn test_format_decimal_256_standard_notation() {
        // Value 12345 with scale 2 should produce "123.45", not "12345e-2"
        let mut bytes = [0u8; 32];
        bytes[31] = 0x39; // 12345 = 0x3039
        bytes[30] = 0x30;
        let v = klickhouse::i256(bytes);
        assert_eq!(format_decimal_256(&v, 2), "123.45");
    }

    #[test]
    fn test_format_decimal_256_scale_zero() {
        let mut bytes = [0u8; 32];
        bytes[31] = 42;
        let v = klickhouse::i256(bytes);
        assert_eq!(format_decimal_256(&v, 0), "42");
    }

    #[test]
    fn test_format_decimal_256_zero_value() {
        let bytes = [0u8; 32];
        let v = klickhouse::i256(bytes);
        assert_eq!(format_decimal_256(&v, 2), "0.00");
    }

    #[test]
    fn test_format_decimal_256_small_fractional() {
        // Value 5 with scale 3 -> "0.005"
        let mut bytes = [0u8; 32];
        bytes[31] = 5;
        let v = klickhouse::i256(bytes);
        assert_eq!(format_decimal_256(&v, 3), "0.005");
    }

    #[test]
    fn test_klickhouse_map_to_text_escapes_quotes_in_keys() {
        let keys = vec![klickhouse::Value::String(b"he\"llo".to_vec())];
        let values = vec![klickhouse::Value::UInt32(42)];
        let result = klickhouse_map_to_text(&keys, &values);
        assert_eq!(result, r#"{"he\"llo":42}"#);
    }

    #[test]
    fn test_klickhouse_map_to_text_escapes_string_values() {
        let keys = vec![klickhouse::Value::String(b"key".to_vec())];
        let values = vec![klickhouse::Value::String(b"val\"ue".to_vec())];
        let result = klickhouse_map_to_text(&keys, &values);
        assert_eq!(result, r#"{"key":"val\"ue"}"#);
    }

    #[test]
    fn test_klickhouse_map_to_text_backslash_in_key() {
        let keys = vec![klickhouse::Value::String(b"a\\b".to_vec())];
        let values = vec![klickhouse::Value::UInt64(1)];
        let result = klickhouse_map_to_text(&keys, &values);
        assert_eq!(result, r#"{"a\\b":1}"#);
    }

    #[test]
    fn test_format_decimal_256_high_scale_no_panic() {
        // Decimal256(76, 40): value 1 with scale 40 should NOT panic.
        // Previously the i128 fast-path would overflow on 10i128.pow(40).
        let mut bytes = [0u8; 32];
        bytes[31] = 1;
        let v = klickhouse::i256(bytes);
        let result = format_decimal_256(&v, 40);
        assert!(
            result.contains('.'),
            "Expected decimal point in output, got: {}",
            result,
        );
        assert!(
            result.starts_with("0."),
            "Value 1 at scale 40 should start with '0.', got: {}",
            result,
        );
    }
}
