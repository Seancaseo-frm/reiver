//! Shared schema conversion utilities for connectors.
//!
//! This module provides common functions for converting between Arrow schemas
//! and the internal TableSchema representation used by the warehouse.
//!
//! # Rich Type System
//!
//! This module also provides functions for mapping source-specific types to Arrow
//! DataTypes with full precision information, and for creating TypedColumn instances
//! with semantic metadata.

use arrow::datatypes::{DataType, Schema, TimeUnit};
use regex::Regex;
use std::sync::OnceLock;

use crate::warehouse::types::{
    ColumnSchema, ColumnType, DurationUnit, SemanticType, TableSchema, TypedColumn, TypedSchema,
};

/// Convert an Arrow schema to our internal TableSchema representation.
///
/// This function maps Arrow field types to our simplified ColumnType enum
/// and preserves nullability information.
pub fn arrow_schema_to_table_schema(schema: &Schema) -> TableSchema {
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let data_type = arrow_type_to_column_type(field.data_type());
            let timezone = if matches!(data_type, ColumnType::Timestamp) {
                Some("UTC".to_string())
            } else {
                None
            };
            ColumnSchema {
                name: field.name().clone(),
                data_type,
                nullable: field.is_nullable(),
                description: None,
                timezone,
            }
        })
        .collect();

    TableSchema { columns }
}

/// Convert an Arrow DataType to our internal ColumnType representation.
///
/// Maps Arrow's rich type system to our simplified column types.
/// Unknown or complex types default to String for maximum compatibility.
pub fn arrow_type_to_column_type(dt: &DataType) -> ColumnType {
    match dt {
        DataType::Boolean => ColumnType::Boolean,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::UInt8 | DataType::UInt16 => {
            ColumnType::Int32
        }
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => ColumnType::Int64,
        DataType::Float16 | DataType::Float32 | DataType::Float64 => ColumnType::Float64,
        DataType::Utf8 | DataType::LargeUtf8 => ColumnType::String,
        DataType::Date32 | DataType::Date64 => ColumnType::Date,
        DataType::Timestamp(_, _) => ColumnType::Timestamp,
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => ColumnType::Decimal,
        _ => ColumnType::String, // Default to string for unknown types
    }
}

// =============================================================================
// PostgreSQL Type Mapping (Arrow-based)
// =============================================================================

/// Regex for parsing decimal/numeric precision: DECIMAL(18,4) or NUMERIC(10,2)
fn decimal_precision_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"(?i)(?:decimal|numeric)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)").unwrap()
    })
}

/// Map a PostgreSQL type string to Arrow DataType with full precision.
///
/// This function parses PostgreSQL type names (as returned by `information_schema.columns`)
/// and maps them to the most appropriate Arrow DataType, preserving precision information.
///
/// # Arguments
/// * `pg_type` - PostgreSQL type name (e.g., "integer", "numeric(18,4)", "timestamp with time zone")
///
/// # Returns
/// A tuple of (Arrow DataType, Option<SemanticType>, source_type_name)
pub fn pg_type_to_arrow(pg_type: &str) -> (DataType, Option<SemanticType>, String) {
    let pg_type_lower = pg_type.to_lowercase();
    let source_type_name = pg_type.to_string();

    // Check for decimal/numeric with precision
    if let Some(caps) = decimal_precision_regex().captures(&pg_type_lower) {
        let raw_precision: u32 = caps.get(1).unwrap().as_str().parse().unwrap_or(38);
        let raw_scale: i32 = caps.get(2).unwrap().as_str().parse().unwrap_or(0);

        if raw_precision > 38 {
            // Decimal128 supports at most precision 38. Fall back to Utf8
            // so the value is preserved as a string rather than panicking.
            return (DataType::Utf8, None, source_type_name);
        }

        let precision = raw_precision as u8;
        let scale = raw_scale as i8;
        return (DataType::Decimal128(precision, scale), None, source_type_name);
    }

    // Match the base type
    let (data_type, semantic) = match pg_type_lower.as_str() {
        // Integer types
        "smallint" | "int2" | "smallserial" => (DataType::Int16, None),
        "integer" | "int" | "int4" | "serial" => (DataType::Int32, None),
        "bigint" | "int8" | "bigserial" => (DataType::Int64, None),
        "oid" => (DataType::UInt32, None),

        // Floating point
        "real" | "float4" => (DataType::Float32, None),
        "double precision" | "float8" => (DataType::Float64, None),

        // Decimal/Numeric without explicit precision (arbitrary precision)
        "numeric" | "decimal" => (DataType::Decimal128(38, 18), None),

        // Money type (locale-dependent, 2 decimal places)
        "money" => (
            DataType::Decimal128(19, 2),
            Some(SemanticType::Money {
                currency: None, // PostgreSQL money is locale-dependent
                in_cents: false,
            }),
        ),

        // Boolean
        "boolean" | "bool" => (DataType::Boolean, None),

        // Character types
        "text" => (DataType::Utf8, None),
        "name" => (DataType::Utf8, None), // 64-byte internal identifier
        s if s.starts_with("character varying") || s.starts_with("varchar") => {
            (DataType::Utf8, None)
        }
        s if s.starts_with("character") || s.starts_with("char") => (DataType::Utf8, None),

        // Binary types
        "bytea" => (DataType::Binary, None),
        s if s.starts_with("bit varying") || s.starts_with("varbit") => (DataType::Binary, None),
        s if s.starts_with("bit") => (DataType::Binary, None),

        // Date/Time types
        "date" => (DataType::Date32, None),
        "time" | "time without time zone" => (DataType::Time64(TimeUnit::Microsecond), None),
        "time with time zone" | "timetz" => (DataType::Time64(TimeUnit::Microsecond), None),
        "timestamp" | "timestamp without time zone" => {
            (DataType::Timestamp(TimeUnit::Microsecond, None), None)
        }
        "timestamp with time zone" | "timestamptz" => (
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            None,
        ),
        "interval" => (
            DataType::Duration(TimeUnit::Microsecond),
            Some(SemanticType::Duration {
                unit: DurationUnit::Microseconds,
            }),
        ),

        // UUID
        "uuid" => (DataType::FixedSizeBinary(16), None),

        // JSON types (stored as UTF-8 strings)
        "json" | "jsonb" => (DataType::Utf8, None),

        // Network types (stored as strings)
        "inet" | "cidr" | "macaddr" | "macaddr8" => (DataType::Utf8, None),

        // XML
        "xml" => (DataType::Utf8, None),

        // Text search
        "tsvector" | "tsquery" => (DataType::Utf8, None),

        // Geometric types (stored as strings for now)
        "point" | "line" | "lseg" | "box" | "path" | "polygon" | "circle" => {
            (DataType::Utf8, None)
        }

        // Range types (stored as strings for now)
        s if s.ends_with("range") => (DataType::Utf8, None),

        // Array types
        s if s.ends_with("[]") => {
            // Parse element type
            let element_type = &s[..s.len() - 2];
            let (element_arrow, _, _) = pg_type_to_arrow(element_type);
            (
                DataType::List(std::sync::Arc::new(arrow::datatypes::Field::new(
                    "item",
                    element_arrow,
                    true,
                ))),
                None,
            )
        }

        // Default: string for unknown types
        _ => (DataType::Utf8, None),
    };

    (data_type, semantic, source_type_name)
}

/// Map a MySQL type string to Arrow DataType with full precision.
///
/// # Arguments
/// * `mysql_type` - MySQL type name (e.g., "int unsigned", "decimal(10,2)", "varchar(255)")
///
/// # Returns
/// A tuple of (Arrow DataType, Option<SemanticType>, source_type_name)
pub fn mysql_type_to_arrow(mysql_type: &str) -> (DataType, Option<SemanticType>, String) {
    let mysql_type_lower = mysql_type.to_lowercase();
    let source_type_name = mysql_type.to_string();
    let is_unsigned = mysql_type_lower.contains("unsigned");

    if let Some(caps) = decimal_precision_regex().captures(&mysql_type_lower) {
        let raw_precision: u32 = caps.get(1).unwrap().as_str().parse().unwrap_or(10);
        let raw_scale: i32 = caps.get(2).unwrap().as_str().parse().unwrap_or(0);

        if raw_precision > 38 {
            return (DataType::Utf8, None, source_type_name);
        }

        let precision = raw_precision as u8;
        let scale = raw_scale as i8;
        return (DataType::Decimal128(precision, scale), None, source_type_name);
    }

    // Extract base type (remove "unsigned" and other modifiers)
    let base_type = mysql_type_lower
        .replace("unsigned", "")
        .replace("zerofill", "")
        .trim()
        .to_string();

    let (data_type, semantic) = match base_type.as_str() {
        // Integer types
        "tinyint" | "tinyint(1)" => {
            if is_unsigned {
                (DataType::UInt8, None)
            } else if base_type == "tinyint(1)" {
                (DataType::Boolean, None) // MySQL boolean
            } else {
                (DataType::Int8, None)
            }
        }
        s if s.starts_with("tinyint") => {
            if is_unsigned {
                (DataType::UInt8, None)
            } else {
                (DataType::Int8, None)
            }
        }
        s if s.starts_with("smallint") => {
            if is_unsigned {
                (DataType::UInt16, None)
            } else {
                (DataType::Int16, None)
            }
        }
        s if s.starts_with("mediumint") => {
            if is_unsigned {
                (DataType::UInt32, None)
            } else {
                (DataType::Int32, None)
            }
        }
        s if s.starts_with("int") || s.starts_with("integer") => {
            if is_unsigned {
                (DataType::UInt32, None)
            } else {
                (DataType::Int32, None)
            }
        }
        s if s.starts_with("bigint") => {
            if is_unsigned {
                (DataType::UInt64, None)
            } else {
                (DataType::Int64, None)
            }
        }

        // Floating point
        "float" => (DataType::Float32, None),
        "double" | "double precision" | "real" => (DataType::Float64, None),

        // Decimal without precision
        "decimal" | "numeric" | "dec" | "fixed" => (DataType::Decimal128(10, 0), None),

        // Boolean
        "boolean" | "bool" => (DataType::Boolean, None),

        // String types
        s if s.starts_with("char") => (DataType::Utf8, None),
        s if s.starts_with("varchar") => (DataType::Utf8, None),
        "tinytext" | "text" | "mediumtext" | "longtext" => (DataType::Utf8, None),
        s if s.starts_with("enum") => (DataType::Utf8, Some(SemanticType::Categorical)),
        s if s.starts_with("set") => (DataType::Utf8, None),

        // Binary types
        s if s.starts_with("binary") || s.starts_with("varbinary") => (DataType::Binary, None),
        "tinyblob" | "blob" | "mediumblob" | "longblob" => (DataType::Binary, None),
        s if s.starts_with("bit") => (DataType::Binary, None),

        // Date/Time types
        "date" => (DataType::Date32, None),
        "time" => (DataType::Time64(TimeUnit::Microsecond), None),
        "datetime" => (DataType::Timestamp(TimeUnit::Microsecond, None), None),
        "timestamp" => (
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            None,
        ),
        "year" => (DataType::Int16, None),

        // JSON
        "json" => (DataType::Utf8, None),

        // Spatial types (stored as binary WKB)
        "geometry" | "point" | "linestring" | "polygon" | "multipoint" | "multilinestring"
        | "multipolygon" | "geometrycollection" => (DataType::Binary, None),

        // Default
        _ => (DataType::Utf8, None),
    };

    (data_type, semantic, source_type_name)
}

/// Map Stripe field types to Arrow DataType with semantic information.
///
/// # Arguments
/// * `field_name` - The Stripe field name (e.g., "amount", "created", "customer")
/// * `json_type` - The JSON type from Stripe (e.g., "integer", "string", "object")
///
/// # Returns
/// A tuple of (Arrow DataType, Option<SemanticType>, source_type_name)
pub fn stripe_type_to_arrow(
    field_name: &str,
    json_type: &str,
) -> (DataType, Option<SemanticType>, String) {
    let source_type_name = format!("stripe::{}", json_type);

    // Amount fields are in cents
    let amount_fields = [
        "amount",
        "amount_captured",
        "amount_refunded",
        "fee",
        "net",
        "unit_amount",
        "amount_due",
        "amount_paid",
        "amount_remaining",
        "total",
        "subtotal",
    ];

    // Timestamp fields (Unix seconds)
    let timestamp_fields = [
        "created",
        "updated",
        "period_start",
        "period_end",
        "trial_start",
        "trial_end",
        "canceled_at",
        "ended_at",
        "current_period_start",
        "current_period_end",
        "billing_cycle_anchor",
    ];

    // Identifier fields
    let id_fields = [
        "id",
        "customer",
        "payment_intent",
        "invoice",
        "subscription",
        "source",
        "charge",
        "balance_transaction",
        "product",
        "price",
    ];

    // Status/categorical fields
    let status_fields = ["status", "type", "currency", "object", "livemode"];

    let (data_type, semantic) = if amount_fields.contains(&field_name) {
        (
            DataType::Int64,
            Some(SemanticType::Money {
                currency: None, // Currency is in a separate field
                in_cents: true,
            }),
        )
    } else if timestamp_fields.contains(&field_name) {
        (
            DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
            None,
        )
    } else if id_fields.contains(&field_name) || field_name.ends_with("_id") {
        (DataType::Utf8, Some(SemanticType::Identifier))
    } else if status_fields.contains(&field_name) {
        (DataType::Utf8, Some(SemanticType::Categorical))
    } else {
        // Default mapping based on JSON type
        match json_type {
            "integer" => (DataType::Int64, None),
            "number" => (DataType::Float64, None),
            "boolean" => (DataType::Boolean, None),
            "string" => (DataType::Utf8, None),
            "array" => (
                DataType::List(std::sync::Arc::new(arrow::datatypes::Field::new(
                    "item",
                    DataType::Utf8,
                    true,
                ))),
                None,
            ),
            "object" => (DataType::Utf8, None), // Store as JSON string
            _ => (DataType::Utf8, None),
        }
    };

    (data_type, semantic, source_type_name)
}

/// Create a TypedColumn from Arrow schema information.
pub fn create_typed_column(
    name: &str,
    data_type: DataType,
    nullable: bool,
    source_type_name: &str,
    source_name: &str,
    semantic: Option<SemanticType>,
) -> TypedColumn {
    let mut col = TypedColumn::new(name, &data_type, nullable, source_type_name, source_name);
    if let Some(sem) = semantic {
        col = col.with_semantic(sem);
    }
    col
}

/// Create a TypedSchema from an Arrow schema with source information.
pub fn arrow_schema_to_typed_schema(
    arrow_schema: &Schema,
    table_name: &str,
    source_name: &str,
    source_types: Option<&[String]>,
) -> TypedSchema {
    let mut typed_schema = TypedSchema::new(table_name, source_name);

    for (i, field) in arrow_schema.fields().iter().enumerate() {
        // Get source type from provided types or generate from Arrow type
        let source_type_string: String = source_types
            .and_then(|types| types.get(i))
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("{:?}", field.data_type()));

        let column = TypedColumn::new(
            field.name(),
            field.data_type(),
            field.is_nullable(),
            source_type_string,
            source_name,
        );
        typed_schema = typed_schema.with_column(column);
    }

    typed_schema
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
    use std::sync::Arc;

    #[test]
    fn test_arrow_schema_to_table_schema() {
        let arrow_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, false),
            Field::new("created_at", DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None), true),
        ]);

        let table_schema = arrow_schema_to_table_schema(&arrow_schema);

        assert_eq!(table_schema.columns.len(), 4);

        assert_eq!(table_schema.columns[0].name, "id");
        assert_eq!(table_schema.columns[0].data_type, ColumnType::Int64);
        assert!(!table_schema.columns[0].nullable);

        assert_eq!(table_schema.columns[1].name, "name");
        assert_eq!(table_schema.columns[1].data_type, ColumnType::String);
        assert!(table_schema.columns[1].nullable);

        assert_eq!(table_schema.columns[2].name, "active");
        assert_eq!(table_schema.columns[2].data_type, ColumnType::Boolean);

        assert_eq!(table_schema.columns[3].name, "created_at");
        assert_eq!(table_schema.columns[3].data_type, ColumnType::Timestamp);
    }

    #[test]
    fn test_arrow_type_to_column_type_mappings() {
        // Integer types
        assert_eq!(arrow_type_to_column_type(&DataType::Int8), ColumnType::Int32);
        assert_eq!(arrow_type_to_column_type(&DataType::Int16), ColumnType::Int32);
        assert_eq!(arrow_type_to_column_type(&DataType::Int32), ColumnType::Int32);
        assert_eq!(arrow_type_to_column_type(&DataType::Int64), ColumnType::Int64);
        assert_eq!(arrow_type_to_column_type(&DataType::UInt32), ColumnType::Int64);

        // Float types
        assert_eq!(arrow_type_to_column_type(&DataType::Float32), ColumnType::Float64);
        assert_eq!(arrow_type_to_column_type(&DataType::Float64), ColumnType::Float64);

        // String types
        assert_eq!(arrow_type_to_column_type(&DataType::Utf8), ColumnType::String);
        assert_eq!(arrow_type_to_column_type(&DataType::LargeUtf8), ColumnType::String);

        // Date/time types
        assert_eq!(arrow_type_to_column_type(&DataType::Date32), ColumnType::Date);
        assert_eq!(arrow_type_to_column_type(&DataType::Date64), ColumnType::Date);

        // Decimal types
        assert_eq!(arrow_type_to_column_type(&DataType::Decimal128(10, 2)), ColumnType::Decimal);

        // Unknown types default to String
        assert_eq!(arrow_type_to_column_type(&DataType::Binary), ColumnType::String);
    }

    // =========================================================================
    // Type mapping function tests
    // =========================================================================

    #[test]
    fn test_pg_type_to_arrow_basic() {
        // Integer types
        let (dt, sem, _) = pg_type_to_arrow("integer");
        assert!(matches!(dt, DataType::Int32));
        assert!(sem.is_none());

        let (dt, sem, _) = pg_type_to_arrow("bigint");
        assert!(matches!(dt, DataType::Int64));
        assert!(sem.is_none());

        let (dt, sem, _) = pg_type_to_arrow("smallint");
        assert!(matches!(dt, DataType::Int16));
        assert!(sem.is_none());
    }

    #[test]
    fn test_pg_type_to_arrow_text_types() {
        let (dt, _, _) = pg_type_to_arrow("text");
        assert!(matches!(dt, DataType::Utf8));

        let (dt, _, _) = pg_type_to_arrow("varchar(255)");
        assert!(matches!(dt, DataType::Utf8));

        let (dt, _, _) = pg_type_to_arrow("character varying");
        assert!(matches!(dt, DataType::Utf8));
    }

    #[test]
    fn test_pg_type_to_arrow_numeric() {
        // Basic numeric defaults
        let (dt, _, _) = pg_type_to_arrow("numeric");
        assert!(matches!(dt, DataType::Decimal128(38, 18)));

        // Numeric with precision
        let (dt, _, _) = pg_type_to_arrow("numeric(18,4)");
        assert!(matches!(dt, DataType::Decimal128(18, 4)));

        // Precision > 38 falls back to Utf8 (Decimal128 max precision is 38)
        let (dt, _, _) = pg_type_to_arrow("numeric(100,50)");
        assert!(matches!(dt, DataType::Utf8), "precision > 38 should fall back to Utf8, got {:?}", dt);

        // Precision exactly 38 still uses Decimal128
        let (dt, _, _) = pg_type_to_arrow("numeric(38,10)");
        assert!(matches!(dt, DataType::Decimal128(38, 10)));

        // Precision 39 falls back
        let (dt, _, _) = pg_type_to_arrow("numeric(39,10)");
        assert!(matches!(dt, DataType::Utf8), "precision 39 should fall back to Utf8, got {:?}", dt);
    }

    #[test]
    fn test_pg_type_to_arrow_money() {
        let (dt, sem, _) = pg_type_to_arrow("money");
        assert!(matches!(dt, DataType::Decimal128(19, 2)));
        assert!(matches!(sem, Some(SemanticType::Money { currency: None, in_cents: false })));
    }

    #[test]
    fn test_pg_type_to_arrow_timestamps() {
        // Timestamp without timezone
        let (dt, _, _) = pg_type_to_arrow("timestamp");
        assert!(matches!(dt, DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)));

        // Timestamp with timezone
        let (dt, _, _) = pg_type_to_arrow("timestamp with time zone");
        // Should have a timezone
        if let DataType::Timestamp(_, tz) = dt {
            assert!(tz.is_some());
        } else {
            panic!("Expected Timestamp type");
        }

        // timestamptz alias
        let (dt, _, _) = pg_type_to_arrow("timestamptz");
        if let DataType::Timestamp(_, tz) = dt {
            assert!(tz.is_some());
        } else {
            panic!("Expected Timestamp type");
        }
    }

    #[test]
    fn test_pg_type_to_arrow_uuid() {
        let (dt, sem, _) = pg_type_to_arrow("uuid");
        assert!(matches!(dt, DataType::FixedSizeBinary(16)));
        // PostgreSQL UUID doesn't have semantic metadata by default
        assert!(sem.is_none());
    }

    #[test]
    fn test_pg_type_to_arrow_arrays() {
        // Integer array
        let (dt, _, _) = pg_type_to_arrow("int4[]");
        if let DataType::List(field) = dt {
            assert!(matches!(field.data_type(), DataType::Int32));
        } else {
            panic!("Expected List type for int4[], got {:?}", dt);
        }

        // Text array
        let (dt, _, _) = pg_type_to_arrow("text[]");
        if let DataType::List(field) = dt {
            assert!(matches!(field.data_type(), DataType::Utf8));
        } else {
            panic!("Expected List type for text[]");
        }
    }

    #[test]
    fn test_pg_type_to_arrow_interval() {
        let (dt, sem, _) = pg_type_to_arrow("interval");
        assert!(matches!(dt, DataType::Duration(_)));
        assert!(matches!(sem, Some(SemanticType::Duration { .. })));
    }

    #[test]
    fn test_mysql_type_to_arrow_basic() {
        // Signed integers
        let (dt, _, _) = mysql_type_to_arrow("int");
        assert!(matches!(dt, DataType::Int32));

        let (dt, _, _) = mysql_type_to_arrow("bigint");
        assert!(matches!(dt, DataType::Int64));

        let (dt, _, _) = mysql_type_to_arrow("tinyint");
        assert!(matches!(dt, DataType::Int8));
    }

    #[test]
    fn test_mysql_type_to_arrow_unsigned() {
        let (dt, _, _) = mysql_type_to_arrow("int unsigned");
        assert!(matches!(dt, DataType::UInt32));

        let (dt, _, _) = mysql_type_to_arrow("bigint unsigned");
        assert!(matches!(dt, DataType::UInt64));

        let (dt, _, _) = mysql_type_to_arrow("tinyint unsigned");
        assert!(matches!(dt, DataType::UInt8));
    }

    #[test]
    fn test_mysql_type_to_arrow_boolean_detection() {
        // MySQL uses tinyint(1) for booleans
        let (dt, _, _) = mysql_type_to_arrow("tinyint(1)");
        assert!(matches!(dt, DataType::Boolean));

        // Other tinyint sizes should be Int8
        let (dt, _, _) = mysql_type_to_arrow("tinyint(4)");
        assert!(matches!(dt, DataType::Int8));
    }

    #[test]
    fn test_mysql_type_to_arrow_decimal() {
        let (dt, _, _) = mysql_type_to_arrow("decimal(10,2)");
        assert!(matches!(dt, DataType::Decimal128(10, 2)));

        // MySQL decimal without precision uses default (10, 0)
        let (dt, _, _) = mysql_type_to_arrow("decimal");
        assert!(matches!(dt, DataType::Decimal128(10, 0)));
    }

    #[test]
    fn test_mysql_type_to_arrow_enum() {
        let (dt, sem, _) = mysql_type_to_arrow("enum('a','b','c')");
        assert!(matches!(dt, DataType::Utf8));
        assert!(matches!(sem, Some(SemanticType::Categorical)));
    }

    #[test]
    fn test_mysql_type_to_arrow_text_types() {
        let (dt, _, _) = mysql_type_to_arrow("varchar(255)");
        assert!(matches!(dt, DataType::Utf8));

        let (dt, _, _) = mysql_type_to_arrow("text");
        assert!(matches!(dt, DataType::Utf8));

        // All MySQL text types map to Utf8
        let (dt, _, _) = mysql_type_to_arrow("longtext");
        assert!(matches!(dt, DataType::Utf8));
    }

    #[test]
    fn test_stripe_type_to_arrow_amounts() {
        // Amount fields should be Money with in_cents=true
        let (dt, sem, _) = stripe_type_to_arrow("amount", "integer");
        assert!(matches!(dt, DataType::Int64));
        if let Some(SemanticType::Money { in_cents, .. }) = sem {
            assert!(in_cents, "Stripe amounts should be in cents");
        } else {
            panic!("Expected Money semantic type for amount field");
        }

        // amount_due is in the amount_fields list
        let (dt, sem, _) = stripe_type_to_arrow("amount_due", "integer");
        assert!(matches!(dt, DataType::Int64));
        assert!(matches!(sem, Some(SemanticType::Money { in_cents: true, .. })));
    }

    #[test]
    fn test_stripe_type_to_arrow_timestamps() {
        let (dt, _, _) = stripe_type_to_arrow("created", "integer");
        // Stripe timestamps are Unix seconds
        assert!(matches!(dt, DataType::Timestamp(arrow::datatypes::TimeUnit::Second, _)));

        // 'updated' is in the timestamp_fields list (not 'updated_at')
        let (dt, _, _) = stripe_type_to_arrow("updated", "integer");
        assert!(matches!(dt, DataType::Timestamp(arrow::datatypes::TimeUnit::Second, _)));
    }

    #[test]
    fn test_stripe_type_to_arrow_ids() {
        let (dt, sem, _) = stripe_type_to_arrow("id", "string");
        assert!(matches!(dt, DataType::Utf8));
        assert!(matches!(sem, Some(SemanticType::Identifier)));

        let (dt, sem, _) = stripe_type_to_arrow("customer_id", "string");
        assert!(matches!(dt, DataType::Utf8));
        assert!(matches!(sem, Some(SemanticType::Identifier)));
    }

    #[test]
    fn test_stripe_type_to_arrow_categorical() {
        let (dt, sem, _) = stripe_type_to_arrow("status", "string");
        assert!(matches!(dt, DataType::Utf8));
        assert!(matches!(sem, Some(SemanticType::Categorical)));

        let (dt, sem, _) = stripe_type_to_arrow("currency", "string");
        assert!(matches!(dt, DataType::Utf8));
        assert!(matches!(sem, Some(SemanticType::Categorical)));
    }

    #[test]
    fn test_stripe_type_to_arrow_regular_fields() {
        // Regular string field without special semantic
        let (dt, sem, _) = stripe_type_to_arrow("description", "string");
        assert!(matches!(dt, DataType::Utf8));
        assert!(sem.is_none());

        // Regular number field
        let (dt, sem, _) = stripe_type_to_arrow("quantity", "integer");
        assert!(matches!(dt, DataType::Int64));
        assert!(sem.is_none());
    }

    #[test]
    fn test_mysql_decimal_precision_over_38_falls_back_to_utf8() {
        let (dt, _, _) = mysql_type_to_arrow("decimal(50,10)");
        assert_eq!(
            dt,
            DataType::Utf8,
            "MySQL DECIMAL with precision > 38 must fall back to Utf8 instead of panicking"
        );
    }

    #[test]
    fn test_mysql_decimal_precision_38_ok() {
        let (dt, _, _) = mysql_type_to_arrow("decimal(38,9)");
        assert_eq!(dt, DataType::Decimal128(38, 9));
    }

    #[test]
    fn test_mysql_decimal_precision_18_ok() {
        let (dt, _, _) = mysql_type_to_arrow("decimal(18,4)");
        assert_eq!(dt, DataType::Decimal128(18, 4));
    }

    #[test]
    fn test_pg_decimal_precision_over_38_falls_back_to_utf8() {
        let (dt, _, _) = pg_type_to_arrow("numeric(65,10)");
        assert_eq!(dt, DataType::Utf8);
    }
}
