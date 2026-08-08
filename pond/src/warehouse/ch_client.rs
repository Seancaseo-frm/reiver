//! Shared klickhouse (native TCP) client wrapper.
//!
//! Provides a thin abstraction over `klickhouse::Client` for all internal
//! ClickHouse communication.  Handles connection setup, dynamic-schema
//! queries via `RawRow`, DDL execution, and progress-based billing stats.

use std::time::Duration;

use futures::StreamExt;
use klickhouse::{Client, ClientOptions, ConnectionManager, KlickhouseError, Progress, RawRow};
use serde_json::Value as JsonValue;
use thiserror::Error;
use tokio::sync::broadcast;

/// bb8-managed pool of native ClickHouse TCP connections.
pub type NativePool = bb8::Pool<ConnectionManager>;

/// Configuration for connecting to ClickHouse via native TCP protocol.
#[derive(Debug, Clone)]
pub struct NativeChConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
}

impl NativeChConfig {
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub fn client_options(&self) -> ClientOptions {
        ClientOptions {
            username: self.username.clone(),
            password: self.password.clone(),
            default_database: self.database.clone(),
            tcp_nodelay: true,
        }
    }

    /// Build a bb8 connection pool for native TCP connections.
    pub async fn create_pool(&self, max_size: u32) -> ChClientResult<NativePool> {
        let manager = ConnectionManager::new(self.addr(), self.client_options())
            .await
            .map_err(|e| {
                ChClientError::Connection(format!(
                    "Failed to resolve {}: {}",
                    self.addr(),
                    e
                ))
            })?;

        bb8::Pool::builder()
            .max_size(max_size)
            .min_idle(Some(1))
            .connection_timeout(Duration::from_secs(5))
            .idle_timeout(Some(Duration::from_secs(300)))
            .max_lifetime(Some(Duration::from_secs(1800)))
            .build(manager)
            .await
            .map_err(|e| {
                ChClientError::Connection(format!("Failed to build connection pool: {}", e))
            })
    }
}

#[derive(Debug, Error)]
pub enum ChClientError {
    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Query error: {0}")]
    Query(#[from] KlickhouseError),

    #[error("Conversion error: {0}")]
    Conversion(String),
}

pub type ChClientResult<T> = Result<T, ChClientError>;

/// Accumulated query progress stats (billing).
#[derive(Debug, Clone, Default)]
pub struct QueryStats {
    pub read_rows: u64,
    pub read_bytes: u64,
}

/// Wrapper around `klickhouse::Client`.
#[derive(Clone)]
pub struct ChClient {
    inner: Client,
}

impl ChClient {
    /// Connect to ClickHouse via native TCP protocol.
    pub async fn connect(config: &NativeChConfig) -> ChClientResult<Self> {
        let client = Client::connect(config.addr(), config.client_options())
            .await
            .map_err(|e| {
                ChClientError::Connection(format!("Failed to connect to {}: {}", config.addr(), e))
            })?;
        Ok(Self { inner: client })
    }

    pub fn inner(&self) -> &Client {
        &self.inner
    }

    /// Execute DDL or other statements that return no rows.
    pub async fn execute(&self, sql: &str) -> ChClientResult<()> {
        self.inner.execute(sql).await?;
        Ok(())
    }

    /// Execute a query and collect all rows dynamically (no compile-time schema).
    pub async fn query_collect(&self, sql: &str) -> ChClientResult<Vec<RawRow>> {
        let rows = self.inner.query_collect::<RawRow>(sql).await?;
        Ok(rows)
    }

    /// Execute a query returning a stream of `RawRow` batches with memory budget.
    ///
    /// Returns `(rows, stats)`.  When `max_memory_bytes` is set, stops
    /// collecting rows once the estimated in-memory size is exceeded and
    /// marks the result as truncated.
    pub async fn query_with_budget(
        &self,
        sql: &str,
        max_memory_bytes: Option<usize>,
    ) -> ChClientResult<(Vec<RawRow>, bool)> {
        let mut stream = self.inner.query::<RawRow>(sql).await?;
        let mut rows = Vec::new();
        let mut estimated_memory: usize = 0;
        let mut truncated = false;

        while let Some(result) = stream.next().await {
            let row = result?;
            let row_size = estimate_raw_row_size(&row);
            estimated_memory += row_size;

            if let Some(limit) = max_memory_bytes {
                if estimated_memory > limit {
                    truncated = true;
                    break;
                }
            }
            rows.push(row);
        }

        Ok((rows, truncated))
    }

    /// Subscribe to query execution progress for billing stats.
    pub fn subscribe_progress(&self) -> broadcast::Receiver<(klickhouse::Uuid, Progress)> {
        self.inner.subscribe_progress()
    }

    /// Collect progress stats from the receiver until it closes.
    pub fn accumulate_progress(
        mut rx: broadcast::Receiver<(klickhouse::Uuid, Progress)>,
    ) -> QueryStats {
        let mut stats = QueryStats::default();
        while let Ok((_id, p)) = rx.try_recv() {
            stats.read_rows += p.read_rows;
            stats.read_bytes += p.read_bytes;
        }
        stats
    }

    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

/// Convert a `klickhouse::Value` to `serde_json::Value` for the existing
/// `QueryResult` format used throughout the codebase.
pub fn klickhouse_value_to_json(val: klickhouse::Value) -> JsonValue {
    match val {
        klickhouse::Value::UInt8(v) => JsonValue::Number(v.into()),
        klickhouse::Value::UInt16(v) => JsonValue::Number(v.into()),
        klickhouse::Value::UInt32(v) => JsonValue::Number(v.into()),
        klickhouse::Value::UInt64(v) => JsonValue::Number(v.into()),
        klickhouse::Value::Int8(v) => JsonValue::Number(v.into()),
        klickhouse::Value::Int16(v) => JsonValue::Number(v.into()),
        klickhouse::Value::Int32(v) => JsonValue::Number(v.into()),
        klickhouse::Value::Int64(v) => JsonValue::Number(v.into()),
        klickhouse::Value::Float32(v) => serde_json::Number::from_f64(v as f64)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        klickhouse::Value::Float64(v) => serde_json::Number::from_f64(v)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        klickhouse::Value::String(bytes) => match String::from_utf8(bytes) {
            Ok(s) => JsonValue::String(s),
            Err(e) => {
                let hex: String = e
                    .into_bytes()
                    .iter()
                    .map(|b| format!("{:02x}", b))
                    .collect();
                JsonValue::String(format!("\\x{}", hex))
            }
        },
        klickhouse::Value::Uuid(u) => JsonValue::String(u.to_string()),
        klickhouse::Value::Date(d) => JsonValue::Number(d.0.into()),
        klickhouse::Value::DateTime(dt) => JsonValue::Number(dt.1.into()),
        klickhouse::Value::DateTime64(dt) => JsonValue::Number(dt.1.into()),
        klickhouse::Value::Null => JsonValue::Null,
        klickhouse::Value::Array(arr) => {
            JsonValue::Array(arr.into_iter().map(klickhouse_value_to_json).collect())
        }
        klickhouse::Value::Tuple(arr) => {
            JsonValue::Array(arr.into_iter().map(klickhouse_value_to_json).collect())
        }
        klickhouse::Value::Map(keys, values) => {
            let entries: serde_json::Map<String, JsonValue> = keys
                .into_iter()
                .zip(values)
                .map(|(k, v)| {
                    let key_str = match klickhouse_value_to_json(k) {
                        JsonValue::String(s) => s,
                        other => other.to_string(),
                    };
                    (key_str, klickhouse_value_to_json(v))
                })
                .collect();
            JsonValue::Object(entries)
        }
        klickhouse::Value::Ipv4(ip) => JsonValue::String(ip.to_string()),
        klickhouse::Value::Ipv6(ip) => JsonValue::String(ip.to_string()),
        klickhouse::Value::Int128(v) => JsonValue::String(v.to_string()),
        klickhouse::Value::UInt128(v) => JsonValue::String(v.to_string()),
        klickhouse::Value::Int256(v) => JsonValue::String(v.to_string()),
        klickhouse::Value::UInt256(v) => JsonValue::String(v.to_string()),
        klickhouse::Value::Decimal32(_, v) => JsonValue::Number(v.into()),
        klickhouse::Value::Decimal64(_, v) => JsonValue::Number(v.into()),
        klickhouse::Value::Decimal128(scale, v) => {
            let s = format_decimal(v as i128, scale);
            JsonValue::String(s)
        }
        klickhouse::Value::Decimal256(scale, v) => JsonValue::String(format!("{}e-{}", v, scale)),
        klickhouse::Value::Enum8(v) => JsonValue::Number(v.into()),
        klickhouse::Value::Enum16(v) => JsonValue::Number(v.into()),
        klickhouse::Value::Point(p) => JsonValue::Array(vec![
            serde_json::Number::from_f64(p.0[0])
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null),
            serde_json::Number::from_f64(p.0[1])
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null),
        ]),
        klickhouse::Value::Ring(r) => JsonValue::Array(
            r.0.into_iter()
                .map(|p| klickhouse_value_to_json(klickhouse::Value::Point(p)))
                .collect(),
        ),
        klickhouse::Value::Polygon(p) => JsonValue::Array(
            p.0.into_iter()
                .map(|r| klickhouse_value_to_json(klickhouse::Value::Ring(r)))
                .collect(),
        ),
        klickhouse::Value::MultiPolygon(mp) => JsonValue::Array(
            mp.0.into_iter()
                .map(|p| klickhouse_value_to_json(klickhouse::Value::Polygon(p)))
                .collect(),
        ),
        klickhouse::Value::BFloat16(v) => serde_json::Number::from_f64(f32::from(v) as f64)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
    }
}

/// Map a `klickhouse::Type` to the ClickHouse type name string used in `ColumnInfo`.
pub fn klickhouse_type_to_string(ty: &klickhouse::Type) -> String {
    ty.to_string()
}

/// Check if a klickhouse type is nullable.
pub fn klickhouse_type_is_nullable(ty: &klickhouse::Type) -> bool {
    matches!(ty, klickhouse::Type::Nullable(_))
}

fn format_decimal(value: i128, scale: usize) -> String {
    if scale == 0 {
        return value.to_string();
    }
    let divisor = 10i128.pow(scale as u32);
    let integer_part = value / divisor;
    let fractional_part = (value % divisor).unsigned_abs();
    let sign = if value < 0 && integer_part == 0 {
        "-"
    } else {
        ""
    };
    format!(
        "{}{}.{:0>width$}",
        sign,
        integer_part,
        fractional_part,
        width = scale
    )
}

#[cfg(test)]
mod format_decimal_tests {
    use super::format_decimal;

    #[test]
    fn positive_value() {
        assert_eq!(format_decimal(12345, 2), "123.45");
    }

    #[test]
    fn negative_large() {
        assert_eq!(format_decimal(-12345, 2), "-123.45");
    }

    #[test]
    fn negative_fractional_only() {
        assert_eq!(format_decimal(-1, 2), "-0.01");
    }

    #[test]
    fn negative_half() {
        assert_eq!(format_decimal(-50, 2), "-0.50");
    }

    #[test]
    fn zero() {
        assert_eq!(format_decimal(0, 2), "0.00");
    }

    #[test]
    fn scale_zero() {
        assert_eq!(format_decimal(-42, 0), "-42");
    }

    #[test]
    fn negative_one_scale_four() {
        assert_eq!(format_decimal(-1, 4), "-0.0001");
    }

    #[test]
    fn negative_exact_boundary() {
        assert_eq!(format_decimal(-100, 2), "-1.00");
    }
}

fn estimate_raw_row_size(row: &RawRow) -> usize {
    // Rough estimate: 64 bytes per column as a baseline
    std::cmp::max(row.len() * 64, 128)
}

/// Convert a klickhouse `Block` into an Arrow `RecordBatch`.
///
/// Maps ClickHouse native types to Arrow types and builds columnar arrays.
/// Nullable columns are unwrapped; if the inner value is `klickhouse::Value::Null`
/// the Arrow array records a null at that position.
pub fn block_to_record_batch(
    block: &klickhouse::block::Block,
) -> ChClientResult<arrow::record_batch::RecordBatch> {
    use arrow::array::*;
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    if block.rows == 0 || block.column_types.is_empty() {
        let schema = Arc::new(Schema::empty());
        return arrow::record_batch::RecordBatch::try_new(schema, vec![])
            .map_err(|e| ChClientError::Conversion(format!("Empty batch: {}", e)));
    }

    let num_rows = block.rows as usize;
    let col_names: Vec<&String> = block.column_data.keys().collect();
    let mut fields = Vec::with_capacity(col_names.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(col_names.len());

    for (col_name, col_type) in &block.column_types {
        let values = match block.column_data.get(col_name) {
            Some(v) => v,
            None => continue,
        };

        let (inner_type, nullable) = match col_type {
            klickhouse::Type::Nullable(inner) => (inner.as_ref(), true),
            other => (other as &klickhouse::Type, false),
        };

        let (arrow_type, array) = build_arrow_column(inner_type, values, num_rows, nullable)?;
        fields.push(Field::new(col_name.clone(), arrow_type, nullable));
        arrays.push(array);
    }

    let schema = Arc::new(Schema::new(fields));
    arrow::record_batch::RecordBatch::try_new(schema, arrays)
        .map_err(|e| ChClientError::Conversion(format!("Failed to build RecordBatch: {}", e)))
}

fn unwrap_nullable(val: &klickhouse::Value) -> (bool, &klickhouse::Value) {
    match val {
        klickhouse::Value::Null => (true, val),
        _ => (false, val),
    }
}

fn build_arrow_column(
    ch_type: &klickhouse::Type,
    values: &[klickhouse::Value],
    num_rows: usize,
    nullable: bool,
) -> ChClientResult<(arrow::datatypes::DataType, arrow::array::ArrayRef)> {
    use arrow::array::*;
    use arrow::datatypes::{DataType, TimeUnit};
    use std::sync::Arc;

    macro_rules! build_primitive {
        ($arrow_dt:expr, $builder_ty:ty, $extract:expr) => {{
            let mut builder = <$builder_ty>::with_capacity(num_rows);
            for val in values {
                let (is_null, inner) = unwrap_nullable(val);
                if is_null {
                    builder.append_null();
                } else {
                    match $extract(inner) {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
            }
            Ok(($arrow_dt, Arc::new(builder.finish()) as ArrayRef))
        }};
    }

    match ch_type {
        klickhouse::Type::UInt8 => {
            build_primitive!(DataType::UInt8, UInt8Builder, |v: &klickhouse::Value| {
                match v {
                    klickhouse::Value::UInt8(n) => Some(*n),
                    _ => None,
                }
            })
        }
        klickhouse::Type::UInt16 => {
            build_primitive!(DataType::UInt16, UInt16Builder, |v: &klickhouse::Value| {
                match v {
                    klickhouse::Value::UInt16(n) => Some(*n),
                    _ => None,
                }
            })
        }
        klickhouse::Type::UInt32 => {
            build_primitive!(DataType::UInt32, UInt32Builder, |v: &klickhouse::Value| {
                match v {
                    klickhouse::Value::UInt32(n) => Some(*n),
                    _ => None,
                }
            })
        }
        klickhouse::Type::UInt64 => {
            build_primitive!(DataType::UInt64, UInt64Builder, |v: &klickhouse::Value| {
                match v {
                    klickhouse::Value::UInt64(n) => Some(*n),
                    _ => None,
                }
            })
        }
        klickhouse::Type::Int8 => {
            build_primitive!(DataType::Int8, Int8Builder, |v: &klickhouse::Value| {
                match v {
                    klickhouse::Value::Int8(n) => Some(*n),
                    _ => None,
                }
            })
        }
        klickhouse::Type::Int16 => {
            build_primitive!(DataType::Int16, Int16Builder, |v: &klickhouse::Value| {
                match v {
                    klickhouse::Value::Int16(n) => Some(*n),
                    _ => None,
                }
            })
        }
        klickhouse::Type::Int32 => {
            build_primitive!(DataType::Int32, Int32Builder, |v: &klickhouse::Value| {
                match v {
                    klickhouse::Value::Int32(n) => Some(*n),
                    _ => None,
                }
            })
        }
        klickhouse::Type::Int64 => {
            build_primitive!(DataType::Int64, Int64Builder, |v: &klickhouse::Value| {
                match v {
                    klickhouse::Value::Int64(n) => Some(*n),
                    _ => None,
                }
            })
        }
        klickhouse::Type::Float32 => build_primitive!(
            DataType::Float32,
            Float32Builder,
            |v: &klickhouse::Value| {
                match v {
                    klickhouse::Value::Float32(n) => Some(*n),
                    _ => None,
                }
            }
        ),
        klickhouse::Type::Float64 => build_primitive!(
            DataType::Float64,
            Float64Builder,
            |v: &klickhouse::Value| {
                match v {
                    klickhouse::Value::Float64(n) => Some(*n),
                    _ => None,
                }
            }
        ),
        klickhouse::Type::String | klickhouse::Type::FixedString(_) => {
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
            for val in values {
                let (is_null, inner) = unwrap_nullable(val);
                if is_null {
                    builder.append_null();
                } else {
                    match inner {
                        klickhouse::Value::String(bytes) => match std::str::from_utf8(bytes) {
                            Ok(s) => builder.append_value(s),
                            Err(_) => {
                                let hex: String =
                                    bytes.iter().map(|b| format!("{:02x}", b)).collect();
                                builder.append_value(format!("\\x{}", hex));
                            }
                        },
                        _ => builder.append_null(),
                    }
                }
            }
            Ok((DataType::Utf8, Arc::new(builder.finish()) as ArrayRef))
        }
        klickhouse::Type::Date => {
            build_primitive!(DataType::Date32, Date32Builder, |v: &klickhouse::Value| {
                match v {
                    klickhouse::Value::Date(d) => Some(d.0 as i32),
                    _ => None,
                }
            })
        }
        klickhouse::Type::DateTime(_) => build_primitive!(
            DataType::Timestamp(TimeUnit::Second, None),
            TimestampSecondBuilder,
            |v: &klickhouse::Value| {
                match v {
                    klickhouse::Value::DateTime(dt) => Some(dt.1 as i64),
                    _ => None,
                }
            }
        ),
        klickhouse::Type::DateTime64(precision, _) => {
            let tu = match precision {
                0..=3 => TimeUnit::Millisecond,
                4..=6 => TimeUnit::Microsecond,
                _ => TimeUnit::Nanosecond,
            };
            let arrow_dt = DataType::Timestamp(tu.clone(), None);
            let mut builder = TimestampMillisecondBuilder::with_capacity(num_rows);
            match tu {
                TimeUnit::Millisecond => {
                    for val in values {
                        let (is_null, inner) = unwrap_nullable(val);
                        if is_null {
                            builder.append_null();
                        } else {
                            match inner {
                                klickhouse::Value::DateTime64(dt) => {
                                    builder.append_value(dt.1 as i64)
                                }
                                _ => builder.append_null(),
                            }
                        }
                    }
                    Ok((
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        Arc::new(builder.finish()) as ArrayRef,
                    ))
                }
                TimeUnit::Microsecond => {
                    let mut builder = TimestampMicrosecondBuilder::with_capacity(num_rows);
                    for val in values {
                        let (is_null, inner) = unwrap_nullable(val);
                        if is_null {
                            builder.append_null();
                        } else {
                            match inner {
                                klickhouse::Value::DateTime64(dt) => {
                                    builder.append_value(dt.1 as i64)
                                }
                                _ => builder.append_null(),
                            }
                        }
                    }
                    Ok((
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        Arc::new(builder.finish()) as ArrayRef,
                    ))
                }
                _ => {
                    let mut builder = TimestampNanosecondBuilder::with_capacity(num_rows);
                    for val in values {
                        let (is_null, inner) = unwrap_nullable(val);
                        if is_null {
                            builder.append_null();
                        } else {
                            match inner {
                                klickhouse::Value::DateTime64(dt) => {
                                    builder.append_value(dt.1 as i64)
                                }
                                _ => builder.append_null(),
                            }
                        }
                    }
                    Ok((
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        Arc::new(builder.finish()) as ArrayRef,
                    ))
                }
            }
        }
        klickhouse::Type::Uuid => {
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 36);
            for val in values {
                let (is_null, inner) = unwrap_nullable(val);
                if is_null {
                    builder.append_null();
                } else {
                    match inner {
                        klickhouse::Value::Uuid(u) => builder.append_value(u.to_string()),
                        _ => builder.append_null(),
                    }
                }
            }
            Ok((DataType::Utf8, Arc::new(builder.finish()) as ArrayRef))
        }
        klickhouse::Type::Decimal32(scale) => build_primitive!(
            DataType::Float64,
            Float64Builder,
            |v: &klickhouse::Value| {
                match v {
                    klickhouse::Value::Decimal32(s, n) => {
                        let divisor = 10f64.powi(*s as i32);
                        Some(*n as f64 / divisor)
                    }
                    _ => None,
                }
            }
        ),
        klickhouse::Type::Decimal64(scale) => build_primitive!(
            DataType::Float64,
            Float64Builder,
            |v: &klickhouse::Value| {
                match v {
                    klickhouse::Value::Decimal64(s, n) => {
                        let divisor = 10f64.powi(*s as i32);
                        Some(*n as f64 / divisor)
                    }
                    _ => None,
                }
            }
        ),
        klickhouse::Type::Decimal128(scale) => build_primitive!(
            DataType::Float64,
            Float64Builder,
            |v: &klickhouse::Value| {
                match v {
                    klickhouse::Value::Decimal128(s, n) => {
                        let divisor = 10f64.powi(*s as i32);
                        Some(*n as f64 / divisor)
                    }
                    _ => None,
                }
            }
        ),
        klickhouse::Type::Int128 => {
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 40);
            for val in values {
                let (is_null, inner) = unwrap_nullable(val);
                if is_null {
                    builder.append_null();
                } else {
                    match inner {
                        klickhouse::Value::Int128(n) => builder.append_value(n.to_string()),
                        _ => builder.append_null(),
                    }
                }
            }
            Ok((DataType::Utf8, Arc::new(builder.finish()) as ArrayRef))
        }
        klickhouse::Type::UInt128 => {
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 40);
            for val in values {
                let (is_null, inner) = unwrap_nullable(val);
                if is_null {
                    builder.append_null();
                } else {
                    match inner {
                        klickhouse::Value::UInt128(n) => builder.append_value(n.to_string()),
                        _ => builder.append_null(),
                    }
                }
            }
            Ok((DataType::Utf8, Arc::new(builder.finish()) as ArrayRef))
        }
        klickhouse::Type::Ipv4 | klickhouse::Type::Ipv6 => {
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 45);
            for val in values {
                let (is_null, inner) = unwrap_nullable(val);
                if is_null {
                    builder.append_null();
                } else {
                    match inner {
                        klickhouse::Value::Ipv4(ip) => builder.append_value(ip.to_string()),
                        klickhouse::Value::Ipv6(ip) => builder.append_value(ip.to_string()),
                        _ => builder.append_null(),
                    }
                }
            }
            Ok((DataType::Utf8, Arc::new(builder.finish()) as ArrayRef))
        }
        _ => {
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
            for val in values {
                let (is_null, _inner) = unwrap_nullable(val);
                if is_null {
                    builder.append_null();
                } else {
                    let json = klickhouse_value_to_json(val.clone());
                    builder.append_value(json.to_string());
                }
            }
            Ok((DataType::Utf8, Arc::new(builder.finish()) as ArrayRef))
        }
    }
}

/// Convert an Arrow `RecordBatch` into `Vec<RawRow>` for native insertion.
pub fn record_batch_to_raw_rows(
    batch: &arrow::record_batch::RecordBatch,
) -> ChClientResult<Vec<RawRow>> {
    use arrow::array::*;
    let schema = batch.schema();
    let num_rows = batch.num_rows();
    let mut rows = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut raw_row = RawRow::default();
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let col = batch.column(col_idx);
            let name = field.name().clone();

            if col.is_null(row_idx) {
                raw_row.set_typed(
                    name,
                    Some(klickhouse::Type::String),
                    klickhouse::Value::Null,
                );
                continue;
            }

            let value = arrow_value_to_klickhouse(col.as_ref(), row_idx, field.data_type())?;
            raw_row.try_set(name, value).map_err(|e| {
                ChClientError::Conversion(format!("Failed to set row value: {}", e))
            })?;
        }
        rows.push(raw_row);
    }

    Ok(rows)
}

fn arrow_value_to_klickhouse(
    col: &dyn arrow::array::Array,
    row_idx: usize,
    data_type: &arrow::datatypes::DataType,
) -> ChClientResult<klickhouse::Value> {
    use arrow::array::*;
    use arrow::datatypes::{DataType, TimeUnit};

    match data_type {
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(klickhouse::Value::UInt8(if arr.value(row_idx) {
                1
            } else {
                0
            }))
        }
        DataType::Int8 => {
            let arr = col.as_any().downcast_ref::<Int8Array>().unwrap();
            Ok(klickhouse::Value::Int8(arr.value(row_idx)))
        }
        DataType::Int16 => {
            let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
            Ok(klickhouse::Value::Int16(arr.value(row_idx)))
        }
        DataType::Int32 => {
            let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(klickhouse::Value::Int32(arr.value(row_idx)))
        }
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(klickhouse::Value::Int64(arr.value(row_idx)))
        }
        DataType::UInt8 => {
            let arr = col.as_any().downcast_ref::<UInt8Array>().unwrap();
            Ok(klickhouse::Value::UInt8(arr.value(row_idx)))
        }
        DataType::UInt16 => {
            let arr = col.as_any().downcast_ref::<UInt16Array>().unwrap();
            Ok(klickhouse::Value::UInt16(arr.value(row_idx)))
        }
        DataType::UInt32 => {
            let arr = col.as_any().downcast_ref::<UInt32Array>().unwrap();
            Ok(klickhouse::Value::UInt32(arr.value(row_idx)))
        }
        DataType::UInt64 => {
            let arr = col.as_any().downcast_ref::<UInt64Array>().unwrap();
            Ok(klickhouse::Value::UInt64(arr.value(row_idx)))
        }
        DataType::Float32 => {
            let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
            Ok(klickhouse::Value::Float32(arr.value(row_idx)))
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(klickhouse::Value::Float64(arr.value(row_idx)))
        }
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(klickhouse::Value::String(
                arr.value(row_idx).as_bytes().to_vec(),
            ))
        }
        DataType::LargeUtf8 => {
            let arr = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Ok(klickhouse::Value::String(
                arr.value(row_idx).as_bytes().to_vec(),
            ))
        }
        DataType::Binary => {
            let arr = col.as_any().downcast_ref::<BinaryArray>().unwrap();
            Ok(klickhouse::Value::String(arr.value(row_idx).to_vec()))
        }
        DataType::LargeBinary => {
            let arr = col.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            Ok(klickhouse::Value::String(arr.value(row_idx).to_vec()))
        }
        DataType::Date32 => {
            let arr = col.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = arr.value(row_idx);
            let days_u16 = u16::try_from(days).map_err(|_| {
                ChClientError::Conversion(format!(
                    "Date32 value {} days since epoch is out of ClickHouse Date range (0..65535)",
                    days,
                ))
            })?;
            Ok(klickhouse::Value::Date(klickhouse::Date(days_u16)))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let arr = col.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
            Ok(klickhouse::Value::UInt32(arr.value(row_idx) as u32))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            Ok(klickhouse::Value::Int64(arr.value(row_idx)))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            Ok(klickhouse::Value::Int64(arr.value(row_idx)))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            Ok(klickhouse::Value::Int64(arr.value(row_idx)))
        }
        _ => {
            let arr = col.as_any().downcast_ref::<StringArray>();
            if let Some(str_arr) = arr {
                Ok(klickhouse::Value::String(
                    str_arr.value(row_idx).as_bytes().to_vec(),
                ))
            } else {
                Ok(klickhouse::Value::String(b"<unsupported type>".to_vec()))
            }
        }
    }
}

#[cfg(test)]
mod datetime64_tests {
    use super::*;

    #[test]
    fn test_datetime64_nanosecond_uses_nanosecond_builder() {
        use arrow::datatypes::TimeUnit;

        let precision = 9u8;
        let tu = match precision {
            0..=3 => TimeUnit::Millisecond,
            4..=6 => TimeUnit::Microsecond,
            _ => TimeUnit::Nanosecond,
        };
        assert_eq!(
            tu,
            TimeUnit::Nanosecond,
            "Precision 9 must map to Nanosecond"
        );

        let source = include_str!("ch_client.rs");
        let dt64_region = source
            .split("klickhouse::Type::DateTime64(precision, _) =>")
            .nth(1)
            .expect("DateTime64 match arm must exist");
        let dt64_body = dt64_region
            .split("klickhouse::Type::Uuid")
            .next()
            .unwrap_or(dt64_region);

        assert!(
            dt64_body.contains("TimestampNanosecondBuilder"),
            "DateTime64 handler must use TimestampNanosecondBuilder for nanosecond precision"
        );
        assert!(
            dt64_body.contains("TimestampMicrosecondBuilder"),
            "DateTime64 handler must use TimestampMicrosecondBuilder for microsecond precision"
        );
    }
}

#[cfg(test)]
mod date32_tests {
    use super::*;
    use arrow::array::Date32Array;
    use arrow::datatypes::DataType;
    use std::sync::Arc;

    #[test]
    fn test_date32_valid_range() {
        let arr = Date32Array::from(vec![Some(0), Some(100), Some(65535)]);
        let col: Arc<dyn arrow::array::Array> = Arc::new(arr);

        for i in 0..3 {
            let result = arrow_value_to_klickhouse(col.as_ref(), i, &DataType::Date32);
            assert!(result.is_ok(), "Valid date at index {} must succeed", i);
        }
    }

    #[test]
    fn test_date32_negative_days_rejected() {
        let arr = Date32Array::from(vec![Some(-100)]);
        let col: Arc<dyn arrow::array::Array> = Arc::new(arr);

        let result = arrow_value_to_klickhouse(col.as_ref(), 0, &DataType::Date32);
        assert!(
            result.is_err(),
            "Negative days (pre-1970 date) must be rejected, not silently wrapped to u16"
        );
    }

    #[test]
    fn test_date32_overflow_rejected() {
        let arr = Date32Array::from(vec![Some(70000)]);
        let col: Arc<dyn arrow::array::Array> = Arc::new(arr);

        let result = arrow_value_to_klickhouse(col.as_ref(), 0, &DataType::Date32);
        assert!(
            result.is_err(),
            "Days > 65535 must be rejected, not silently truncated to u16"
        );
    }
}
