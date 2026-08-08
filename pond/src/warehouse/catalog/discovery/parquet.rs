//! Parquet Schema Discovery
//!
//! Discovers schema information from Parquet files stored in object storage.

use arrow::datatypes::DataType;
use async_trait::async_trait;
use tracing::{debug, info, instrument, warn};

use super::{DiscoveryError, DiscoveryResult, SchemaDiscovery};
use crate::warehouse::sources::types::{RegisteredSource, SourceBackend, SourceConfig};
use crate::warehouse::types::{TypedColumn, TypedSchema};

// ============================================================================
// Arrow to TypedColumn Conversion
// ============================================================================

/// Convert an Arrow schema field to a TypedColumn.
fn field_to_typed_column(field: &arrow::datatypes::Field, source_name: &str) -> TypedColumn {
    let source_type_name = format_arrow_type(field.data_type());

    TypedColumn::new(
        field.name(),
        field.data_type(),
        field.is_nullable(),
        &source_type_name,
        source_name,
    )
}

/// Format an Arrow DataType as a human-readable string.
fn format_arrow_type(dt: &DataType) -> String {
    match dt {
        DataType::Null => "null".to_string(),
        DataType::Boolean => "boolean".to_string(),
        DataType::Int8 => "int8".to_string(),
        DataType::Int16 => "int16".to_string(),
        DataType::Int32 => "int32".to_string(),
        DataType::Int64 => "int64".to_string(),
        DataType::UInt8 => "uint8".to_string(),
        DataType::UInt16 => "uint16".to_string(),
        DataType::UInt32 => "uint32".to_string(),
        DataType::UInt64 => "uint64".to_string(),
        DataType::Float16 => "float16".to_string(),
        DataType::Float32 => "float32".to_string(),
        DataType::Float64 => "float64".to_string(),
        DataType::Timestamp(unit, tz) => {
            let unit_str = match unit {
                arrow::datatypes::TimeUnit::Second => "s",
                arrow::datatypes::TimeUnit::Millisecond => "ms",
                arrow::datatypes::TimeUnit::Microsecond => "us",
                arrow::datatypes::TimeUnit::Nanosecond => "ns",
            };
            match tz {
                Some(tz) => format!("timestamp[{}, {}]", unit_str, tz),
                None => format!("timestamp[{}]", unit_str),
            }
        }
        DataType::Date32 => "date32".to_string(),
        DataType::Date64 => "date64".to_string(),
        DataType::Time32(unit) => {
            let unit_str = match unit {
                arrow::datatypes::TimeUnit::Second => "s",
                arrow::datatypes::TimeUnit::Millisecond => "ms",
                _ => "?",
            };
            format!("time32[{}]", unit_str)
        }
        DataType::Time64(unit) => {
            let unit_str = match unit {
                arrow::datatypes::TimeUnit::Microsecond => "us",
                arrow::datatypes::TimeUnit::Nanosecond => "ns",
                _ => "?",
            };
            format!("time64[{}]", unit_str)
        }
        DataType::Duration(unit) => {
            let unit_str = match unit {
                arrow::datatypes::TimeUnit::Second => "s",
                arrow::datatypes::TimeUnit::Millisecond => "ms",
                arrow::datatypes::TimeUnit::Microsecond => "us",
                arrow::datatypes::TimeUnit::Nanosecond => "ns",
            };
            format!("duration[{}]", unit_str)
        }
        DataType::Interval(_) => "interval".to_string(),
        DataType::Binary => "binary".to_string(),
        DataType::FixedSizeBinary(size) => format!("binary[{}]", size),
        DataType::LargeBinary => "large_binary".to_string(),
        DataType::BinaryView => "binary_view".to_string(),
        DataType::Utf8 => "utf8".to_string(),
        DataType::LargeUtf8 => "large_utf8".to_string(),
        DataType::Utf8View => "utf8_view".to_string(),
        DataType::List(field) => format!("list<{}>", format_arrow_type(field.data_type())),
        DataType::ListView(field) => format!("list_view<{}>", format_arrow_type(field.data_type())),
        DataType::FixedSizeList(field, size) => {
            format!("list[{}]<{}>", size, format_arrow_type(field.data_type()))
        }
        DataType::LargeList(field) => {
            format!("large_list<{}>", format_arrow_type(field.data_type()))
        }
        DataType::LargeListView(field) => {
            format!("large_list_view<{}>", format_arrow_type(field.data_type()))
        }
        DataType::Struct(fields) => {
            let field_strs: Vec<_> = fields
                .iter()
                .map(|f| format!("{}: {}", f.name(), format_arrow_type(f.data_type())))
                .collect();
            format!("struct<{}>", field_strs.join(", "))
        }
        DataType::Union(_, _) => "union".to_string(),
        DataType::Dictionary(key, value) => {
            format!(
                "dict<{}, {}>",
                format_arrow_type(key),
                format_arrow_type(value)
            )
        }
        DataType::Decimal32(precision, scale) => format!("decimal32({}, {})", precision, scale),
        DataType::Decimal64(precision, scale) => format!("decimal64({}, {})", precision, scale),
        DataType::Decimal128(precision, scale) => format!("decimal({}, {})", precision, scale),
        DataType::Decimal256(precision, scale) => format!("decimal256({}, {})", precision, scale),
        DataType::Map(field, _) => format!("map<{}>", format_arrow_type(field.data_type())),
        DataType::RunEndEncoded(_, _) => "run_end_encoded".to_string(),
    }
}

// ============================================================================
// Parquet Schema Discovery
// ============================================================================

/// Parquet schema discovery implementation.
///
/// Discovers schema information from Parquet files in object storage.
pub struct ParquetSchemaDiscovery {
    /// Maximum number of files to check for schema (for multiple files).
    max_files_to_check: usize,
}

impl ParquetSchemaDiscovery {
    /// Create a new Parquet schema discovery.
    pub fn new() -> Self {
        Self {
            max_files_to_check: 5,
        }
    }

    /// Set maximum files to check.
    pub fn with_max_files(mut self, max: usize) -> Self {
        self.max_files_to_check = max;
        self
    }

    /// Get the bucket URL and prefix from source configuration.
    fn get_storage_config(&self, source: &RegisteredSource) -> DiscoveryResult<(String, String)> {
        match &source.backend {
            SourceBackend::ObjectStorage {
                bucket_url, prefix, ..
            } => Ok((bucket_url.clone(), prefix.clone())),
            _ => Err(DiscoveryError::NotConfigured(format!(
                "Source {} is not configured as object storage",
                source.name
            ))),
        }
    }

    /// Get table names from the source configuration.
    fn get_table_names(&self, source: &RegisteredSource) -> Vec<String> {
        // Check if specific tables are configured
        match &source.config {
            SourceConfig::Parquet { config } => {
                // ExternalSourceConfig might have tables defined
                // For now, return empty and rely on directory listing
                Vec::new()
            }
            _ => Vec::new(),
        }
    }

    /// Convert an Arrow schema to a TypedSchema.
    fn arrow_schema_to_typed(
        &self,
        arrow_schema: &arrow::datatypes::Schema,
        table_name: &str,
        source_name: &str,
    ) -> TypedSchema {
        let mut schema = TypedSchema::new(table_name, source_name);

        for field in arrow_schema.fields() {
            let col = field_to_typed_column(field, source_name);
            schema = schema.with_column(col);
        }

        schema
    }

    /// Read schema from a local Parquet file.
    async fn read_parquet_schema_from_path(
        &self,
        path: &str,
        source_name: &str,
    ) -> DiscoveryResult<TypedSchema> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use std::fs::File;

        let file = File::open(path).map_err(|e| {
            DiscoveryError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Failed to open file {}: {}", path, e),
            ))
        })?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
            DiscoveryError::Parse(format!("Failed to read Parquet metadata: {}", e))
        })?;

        let arrow_schema = builder.schema();

        // Extract table name from file path
        let table_name = std::path::Path::new(path)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown");

        Ok(self.arrow_schema_to_typed(&arrow_schema, table_name, source_name))
    }

    /// Read schema from a Parquet file in memory (bytes).
    fn read_parquet_schema_from_bytes(
        &self,
        bytes: &[u8],
        table_name: &str,
        source_name: &str,
    ) -> DiscoveryResult<TypedSchema> {
        use bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let bytes = Bytes::copy_from_slice(bytes);

        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(|e| {
            DiscoveryError::Parse(format!("Failed to read Parquet metadata: {}", e))
        })?;

        let arrow_schema = builder.schema();
        Ok(self.arrow_schema_to_typed(&arrow_schema, table_name, source_name))
    }
}

impl Default for ParquetSchemaDiscovery {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SchemaDiscovery for ParquetSchemaDiscovery {
    #[instrument(skip(self, source))]
    async fn discover_schemas(
        &self,
        source: &RegisteredSource,
    ) -> DiscoveryResult<Vec<TypedSchema>> {
        info!("Discovering Parquet schemas for source: {}", source.name);

        let (bucket_url, prefix) = self.get_storage_config(source)?;

        // For object storage, we would need to:
        // 1. List files in the bucket/prefix
        // 2. Download metadata from each Parquet file
        // 3. Build schemas from the Parquet metadata
        //
        // This requires async object storage access (S3 client, etc.)
        // For now, we'll return an empty list and log a warning.
        // The actual implementation would use object_store crate.

        warn!(
            "Object storage schema discovery not fully implemented. \
             Use refresh_source_catalog with specific file paths."
        );

        debug!("Would scan bucket {} prefix {}", bucket_url, prefix);

        // If this is configured with a local path (for testing), try that
        if bucket_url.starts_with("file://") || bucket_url.starts_with('/') {
            let base_path = bucket_url.strip_prefix("file://").unwrap_or(&bucket_url);
            let full_path = format!("{}/{}", base_path, prefix);

            if let Ok(entries) = std::fs::read_dir(&full_path) {
                let mut schemas = Vec::new();

                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                        if let Some(path_str) = path.to_str() {
                            match self
                                .read_parquet_schema_from_path(path_str, &source.name)
                                .await
                            {
                                Ok(schema) => schemas.push(schema),
                                Err(e) => warn!("Failed to read schema from {:?}: {}", path, e),
                            }
                        }
                    }
                }

                return Ok(schemas);
            }
        }

        Ok(Vec::new())
    }

    #[instrument(skip(self, source))]
    async fn discover_table_schema(
        &self,
        source: &RegisteredSource,
        table_name: &str,
    ) -> DiscoveryResult<Option<TypedSchema>> {
        info!(
            "Discovering Parquet schema for table: {}.{}",
            source.name, table_name
        );

        let (bucket_url, prefix) = self.get_storage_config(source)?;

        // Try to find a matching Parquet file
        let file_path = format!("{}/{}/{}.parquet", bucket_url, prefix, table_name);

        // For local files
        if file_path.starts_with("file://") || file_path.starts_with('/') {
            let path = file_path.strip_prefix("file://").unwrap_or(&file_path);

            // Check if file exists first
            if !std::path::Path::new(path).exists() {
                return Ok(None);
            }

            return self
                .read_parquet_schema_from_path(path, &source.name)
                .await
                .map(Some);
        }

        // For S3/R2, we would need to use the object_store crate
        // Return None as we can't verify if the table exists
        Ok(None)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    #[test]
    fn test_format_arrow_type_primitives() {
        assert_eq!(format_arrow_type(&DataType::Int32), "int32");
        assert_eq!(format_arrow_type(&DataType::Float64), "float64");
        assert_eq!(format_arrow_type(&DataType::Utf8), "utf8");
        assert_eq!(format_arrow_type(&DataType::Boolean), "boolean");
    }

    #[test]
    fn test_format_arrow_type_timestamps() {
        assert_eq!(
            format_arrow_type(&DataType::Timestamp(TimeUnit::Microsecond, None)),
            "timestamp[us]"
        );
        assert_eq!(
            format_arrow_type(&DataType::Timestamp(
                TimeUnit::Nanosecond,
                Some("UTC".into())
            )),
            "timestamp[ns, UTC]"
        );
    }

    #[test]
    fn test_format_arrow_type_decimal() {
        assert_eq!(
            format_arrow_type(&DataType::Decimal128(18, 4)),
            "decimal(18, 4)"
        );
    }

    #[test]
    fn test_format_arrow_type_list() {
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        assert_eq!(format_arrow_type(&list_type), "list<int32>");
    }

    #[test]
    fn test_format_arrow_type_struct() {
        let fields = vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
        ];
        let struct_type = DataType::Struct(fields.into());
        assert_eq!(
            format_arrow_type(&struct_type),
            "struct<name: utf8, age: int32>"
        );
    }

    #[test]
    fn test_field_to_typed_column() {
        let field = Field::new("test_column", DataType::Int64, true);
        let col = field_to_typed_column(&field, "parquet_source");

        assert_eq!(col.name, "test_column");
        assert!(col.nullable);
        assert_eq!(col.source_name, "parquet_source");
        assert_eq!(col.source_type_name, "int64");
    }

    #[test]
    fn test_arrow_schema_to_typed() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Float64, true),
        ]);

        let discovery = ParquetSchemaDiscovery::new();
        let typed = discovery.arrow_schema_to_typed(&schema, "test_table", "parquet");

        assert_eq!(typed.table_name, "test_table");
        assert_eq!(typed.source_name, "parquet");
        assert_eq!(typed.columns.len(), 3);

        let col_names: Vec<_> = typed.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(col_names, vec!["id", "name", "amount"]);
    }
}
