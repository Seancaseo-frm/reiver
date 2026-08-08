//! SQL Server Schema Inference
//!
//! Queries INFORMATION_SCHEMA to discover tables and their schemas,
//! and maps SQL Server types to Arrow/ColumnType.

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use crate::warehouse::types::{ColumnSchema, ColumnType, TableSchema};

/// Column metadata from SQL Server INFORMATION_SCHEMA.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub table_name: String,
    pub column_name: String,
    pub ordinal_position: i32,
    pub data_type: String,
    pub is_nullable: bool,
    pub character_maximum_length: Option<i32>,
    pub numeric_precision: Option<i32>,
    pub numeric_scale: Option<i32>,
}

/// Map SQL Server data type to warehouse ColumnType.
pub fn sqlserver_type_to_column_type(data_type: &str) -> ColumnType {
    match data_type.to_lowercase().as_str() {
        // Integer types
        "tinyint" => ColumnType::Int32,
        "smallint" => ColumnType::Int32,
        "int" | "integer" => ColumnType::Int32,
        "bigint" => ColumnType::Int64,

        // Floating point types
        "real" | "float" => ColumnType::Float64,
        "decimal" | "numeric" | "money" | "smallmoney" => ColumnType::Decimal,

        // Boolean
        "bit" => ColumnType::Boolean,

        // String types
        "char" | "varchar" | "text" => ColumnType::String,
        "nchar" | "nvarchar" | "ntext" => ColumnType::String,

        // Date/time types
        "date" => ColumnType::Date,
        "time" => ColumnType::String, // Time without date stored as string
        "datetime" | "datetime2" | "smalldatetime" => ColumnType::Timestamp,
        "datetimeoffset" => ColumnType::Timestamp,

        // Binary types (stored as base64 string)
        "binary" | "varbinary" | "image" => ColumnType::String,

        // UUID
        "uniqueidentifier" => ColumnType::Uuid,

        // XML
        "xml" => ColumnType::String,

        // SQL variant and other types
        _ => ColumnType::String,
    }
}

/// Map SQL Server data type to Arrow DataType.
pub fn sqlserver_type_to_arrow(data_type: &str) -> DataType {
    match data_type.to_lowercase().as_str() {
        // Integer types
        "tinyint" => DataType::Int8,
        "smallint" => DataType::Int16,
        "int" | "integer" => DataType::Int32,
        "bigint" => DataType::Int64,

        // Floating point types
        "real" => DataType::Float32,
        "float" => DataType::Float64,
        "decimal" | "numeric" | "money" | "smallmoney" => DataType::Float64, // Simplified

        // Boolean
        "bit" => DataType::Boolean,

        // String types
        "char" | "varchar" | "text" => DataType::Utf8,
        "nchar" | "nvarchar" | "ntext" => DataType::Utf8,

        // Date/time types
        "date" => DataType::Date32,
        "time" => DataType::Utf8, // Time as string
        "datetime" | "datetime2" | "smalldatetime" => {
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        }
        "datetimeoffset" => DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),

        // Binary types
        "binary" | "varbinary" | "image" => DataType::Utf8, // Base64 encoded

        // UUID
        "uniqueidentifier" => DataType::Utf8, // UUID as string

        // Default to string
        _ => DataType::Utf8,
    }
}

/// Build an Arrow Schema from column information.
pub fn build_arrow_schema(columns: &[ColumnInfo]) -> Schema {
    let fields: Vec<Field> = columns
        .iter()
        .map(|col| {
            let data_type = sqlserver_type_to_arrow(&col.data_type);
            Field::new(&col.column_name, data_type, col.is_nullable)
        })
        .collect();

    Schema::new(fields)
}

/// Build a TableSchema from column information.
pub fn build_table_schema(columns: &[ColumnInfo]) -> TableSchema {
    let column_schemas: Vec<ColumnSchema> = columns
        .iter()
        .map(|col| ColumnSchema {
            name: col.column_name.clone(),
            data_type: sqlserver_type_to_column_type(&col.data_type),
            nullable: col.is_nullable,
            description: None,
            timezone: None,
        })
        .collect();

    TableSchema {
        columns: column_schemas,
    }
}

/// SQL query to get table list from INFORMATION_SCHEMA.
pub const LIST_TABLES_QUERY: &str = r#"
SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = @P1
  AND TABLE_SCHEMA = @P2
  AND TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME
"#;

/// SQL query to get column information from INFORMATION_SCHEMA.
pub const GET_COLUMNS_QUERY: &str = r#"
SELECT 
    TABLE_NAME,
    COLUMN_NAME,
    ORDINAL_POSITION,
    DATA_TYPE,
    CASE WHEN IS_NULLABLE = 'YES' THEN 1 ELSE 0 END AS IS_NULLABLE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_CATALOG = @P1
  AND TABLE_SCHEMA = @P2
  AND TABLE_NAME = @P3
ORDER BY ORDINAL_POSITION
"#;

/// SQL query to get all columns for all tables in a schema.
pub const GET_ALL_COLUMNS_QUERY: &str = r#"
SELECT 
    TABLE_NAME,
    COLUMN_NAME,
    ORDINAL_POSITION,
    DATA_TYPE,
    CASE WHEN IS_NULLABLE = 'YES' THEN 1 ELSE 0 END AS IS_NULLABLE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_CATALOG = @P1
  AND TABLE_SCHEMA = @P2
ORDER BY TABLE_NAME, ORDINAL_POSITION
"#;

/// SQL query to estimate row count for a table.
pub const ESTIMATE_ROW_COUNT_QUERY: &str = r#"
SELECT SUM(p.rows) AS row_count
FROM sys.partitions p
JOIN sys.tables t ON p.object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = @P1
  AND t.name = @P2
  AND p.index_id IN (0, 1)
"#;

/// Check if CDC is enabled for a table.
pub const CHECK_CDC_ENABLED_QUERY: &str = r#"
SELECT is_tracked_by_cdc
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = @P1
  AND t.name = @P2
"#;

/// Get capture instance name for a CDC-enabled table.
pub const GET_CDC_CAPTURE_INSTANCE_QUERY: &str = r#"
SELECT capture_instance
FROM cdc.change_tables ct
JOIN sys.tables t ON ct.source_object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = @P1
  AND t.name = @P2
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqlserver_type_mapping() {
        // Integer types
        assert_eq!(sqlserver_type_to_column_type("int"), ColumnType::Int32);
        assert_eq!(sqlserver_type_to_column_type("bigint"), ColumnType::Int64);
        assert_eq!(sqlserver_type_to_column_type("tinyint"), ColumnType::Int32);

        // Float types
        assert_eq!(sqlserver_type_to_column_type("float"), ColumnType::Float64);
        assert_eq!(sqlserver_type_to_column_type("decimal"), ColumnType::Decimal);
        assert_eq!(sqlserver_type_to_column_type("money"), ColumnType::Decimal);

        // String types
        assert_eq!(sqlserver_type_to_column_type("varchar"), ColumnType::String);
        assert_eq!(sqlserver_type_to_column_type("nvarchar"), ColumnType::String);

        // Date types
        assert_eq!(sqlserver_type_to_column_type("datetime"), ColumnType::Timestamp);
        assert_eq!(sqlserver_type_to_column_type("datetime2"), ColumnType::Timestamp);
        assert_eq!(sqlserver_type_to_column_type("date"), ColumnType::Date);

        // Boolean
        assert_eq!(sqlserver_type_to_column_type("bit"), ColumnType::Boolean);

        // UUID
        assert_eq!(sqlserver_type_to_column_type("uniqueidentifier"), ColumnType::Uuid);
    }

    #[test]
    fn test_arrow_type_mapping() {
        assert!(matches!(sqlserver_type_to_arrow("int"), DataType::Int32));
        assert!(matches!(sqlserver_type_to_arrow("bigint"), DataType::Int64));
        assert!(matches!(sqlserver_type_to_arrow("varchar"), DataType::Utf8));
        assert!(matches!(sqlserver_type_to_arrow("bit"), DataType::Boolean));
        assert!(matches!(sqlserver_type_to_arrow("datetime"), DataType::Timestamp(_, _)));
    }

    #[test]
    fn test_build_table_schema() {
        let columns = vec![
            ColumnInfo {
                table_name: "users".to_string(),
                column_name: "id".to_string(),
                ordinal_position: 1,
                data_type: "int".to_string(),
                is_nullable: false,
                character_maximum_length: None,
                numeric_precision: Some(10),
                numeric_scale: Some(0),
            },
            ColumnInfo {
                table_name: "users".to_string(),
                column_name: "name".to_string(),
                ordinal_position: 2,
                data_type: "nvarchar".to_string(),
                is_nullable: true,
                character_maximum_length: Some(255),
                numeric_precision: None,
                numeric_scale: None,
            },
        ];

        let schema = build_table_schema(&columns);
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.columns[0].name, "id");
        assert_eq!(schema.columns[0].data_type, ColumnType::Int32);
        assert!(!schema.columns[0].nullable);
        assert_eq!(schema.columns[1].name, "name");
        assert_eq!(schema.columns[1].data_type, ColumnType::String);
        assert!(schema.columns[1].nullable);
    }
}
