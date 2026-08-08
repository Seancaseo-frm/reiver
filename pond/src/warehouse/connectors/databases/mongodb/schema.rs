//! MongoDB Schema Inference and BSON to Arrow Conversion
//!
//! This module handles:
//! - Sampling documents to infer collection schemas
//! - Converting BSON types to Arrow types
//! - Flattening nested documents with `__` separator
//! - Converting BSON documents to Arrow RecordBatches

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
    TimestampMillisecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use bson::{Bson, Document};

use crate::warehouse::connectors::{ConnectorError, ConnectorResult};
use crate::warehouse::types::{ColumnSchema, ColumnType, TableSchema};

/// Typed builder enum for single-pass document conversion.
///
/// Holds different Arrow array builders to enable building all columns
/// in a single pass through the documents.
enum TypedBuilder {
    Boolean(BooleanBuilder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float64(Float64Builder),
    Timestamp(TimestampMillisecondBuilder),
    String(StringBuilder),
}

/// Separator used for flattening nested document fields.
/// Using double underscore to minimize collision with user-defined field names.
pub const FIELD_SEPARATOR: &str = "__";

/// Inferred field information from BSON sampling.
#[derive(Debug, Clone)]
pub struct InferredField {
    /// Field name (with flattening separators if nested)
    pub name: String,
    /// Arrow data type
    pub arrow_type: DataType,
    /// Whether null values were observed
    pub nullable: bool,
    /// Original BSON type hint (for debugging)
    pub bson_type_hint: String,
}

/// Schema inference result.
#[derive(Debug, Clone)]
pub struct InferredSchema {
    /// All discovered fields
    pub fields: Vec<InferredField>,
    /// Number of documents sampled
    pub sample_count: usize,
}

impl InferredSchema {
    /// Convert to Arrow Schema.
    pub fn to_arrow_schema(&self) -> Schema {
        let fields: Vec<Field> = self
            .fields
            .iter()
            // Always mark columns as nullable in Arrow to handle edge cases
            .map(|f| {
                // Lists are serialized as JSON strings, so use Utf8 type
                let data_type = match &f.arrow_type {
                    DataType::List(_) => DataType::Utf8,
                    other => other.clone(),
                };
                Field::new(&f.name, data_type, true)
            })
            .collect();
        Schema::new(fields)
    }

    /// Convert to warehouse TableSchema.
    pub fn to_table_schema(&self) -> TableSchema {
        let columns: Vec<ColumnSchema> = self
            .fields
            .iter()
            .map(|f| ColumnSchema {
                name: f.name.clone(),
                data_type: arrow_type_to_column_type(&f.arrow_type),
                nullable: f.nullable,
                description: None,
                timezone: if matches!(f.arrow_type, DataType::Timestamp(_, _)) {
                    Some("UTC".to_string())
                } else {
                    None
                },
            })
            .collect();
        TableSchema { columns }
    }
}

/// Convert Arrow DataType to warehouse ColumnType.
fn arrow_type_to_column_type(arrow_type: &DataType) -> ColumnType {
    match arrow_type {
        DataType::Boolean => ColumnType::Boolean,
        DataType::Int32 => ColumnType::Int32,
        DataType::Int64 => ColumnType::Int64,
        DataType::Float64 => ColumnType::Float64,
        DataType::Utf8 | DataType::LargeUtf8 => ColumnType::String,
        DataType::Timestamp(_, _) => ColumnType::Timestamp,
        DataType::Date32 | DataType::Date64 => ColumnType::Date,
        DataType::List(_) => ColumnType::Json, // Lists are stored as JSON
        _ => ColumnType::String, // Default to string for unknown types
    }
}

/// Infer schema from a sample of documents.
///
/// # Arguments
/// * `documents` - Sample documents to analyze
/// * `max_nested_depth` - Maximum depth for flattening (deeper becomes JSON)
///
/// # Returns
/// Inferred schema with all discovered fields.
pub fn infer_schema(documents: &[Document], max_nested_depth: usize) -> InferredSchema {
    let mut field_types: HashMap<String, (DataType, bool, String)> = HashMap::new();

    for doc in documents {
        let flattened = flatten_document(doc, "", 0, max_nested_depth);
        for (name, bson_value) in flattened {
            let (arrow_type, bson_hint) = bson_to_arrow_type(&bson_value);
            
            field_types
                .entry(name)
                .and_modify(|(existing_type, nullable, existing_hint)| {
                    // If we see a null, mark as nullable
                    if matches!(bson_value, Bson::Null) {
                        *nullable = true;
                    }
                    // Merge types - widen if necessary, passing hints for proper string handling
                    let (merged_type, merged_hint) = merge_arrow_types(
                        existing_type,
                        existing_hint,
                        &arrow_type,
                        &bson_hint,
                    );
                    *existing_type = merged_type;
                    *existing_hint = merged_hint;
                })
                .or_insert_with(|| {
                    let nullable = matches!(bson_value, Bson::Null);
                    (arrow_type, nullable, bson_hint)
                });
        }
    }

    // Mark all fields as nullable to be safe.
    // MongoDB is schema-less, so fields can be missing in any document.
    // The `nullable` variable from tracking is not used - we always mark nullable.
    let fields: Vec<InferredField> = field_types
        .into_iter()
        .map(|(name, (arrow_type, _nullable, bson_hint))| InferredField {
            name,
            arrow_type,
            nullable: true, // All fields are nullable in schema-less MongoDB
            bson_type_hint: bson_hint,
        })
        .collect();

    // Sort fields for consistent ordering (put _id first)
    let mut sorted_fields = fields;
    sorted_fields.sort_by(|a, b| {
        if a.name == "_id" {
            std::cmp::Ordering::Less
        } else if b.name == "_id" {
            std::cmp::Ordering::Greater
        } else {
            a.name.cmp(&b.name)
        }
    });

    InferredSchema {
        fields: sorted_fields,
        sample_count: documents.len(),
    }
}

/// Flatten a BSON document into key-value pairs with flattened field names.
///
/// Nested documents are flattened using `__` separator up to `max_depth`.
/// Deeper nested documents and arrays of objects are serialized as JSON strings.
pub fn flatten_document(
    doc: &Document,
    prefix: &str,
    depth: usize,
    max_depth: usize,
) -> Vec<(String, Bson)> {
    let mut result = Vec::new();

    for (key, value) in doc.iter() {
        let full_key = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{}{}{}", prefix, FIELD_SEPARATOR, key)
        };

        match value {
            Bson::Document(nested) if depth < max_depth => {
                // Recursively flatten nested documents
                let nested_pairs = flatten_document(nested, &full_key, depth + 1, max_depth);
                result.extend(nested_pairs);
            }
            Bson::Document(_) => {
                // Max depth reached - serialize as JSON string
                let json_str = serde_json::to_string(value).unwrap_or_default();
                result.push((full_key, Bson::String(json_str)));
            }
            Bson::Array(arr) => {
                // Check if array contains only scalars
                if is_scalar_array(arr) {
                    result.push((full_key, value.clone()));
                } else {
                    // Array of objects/mixed - serialize as JSON
                    let json_str = serde_json::to_string(value).unwrap_or_default();
                    result.push((full_key, Bson::String(json_str)));
                }
            }
            _ => {
                result.push((full_key, value.clone()));
            }
        }
    }

    result
}

/// Check if an array contains only scalar values.
fn is_scalar_array(arr: &[Bson]) -> bool {
    arr.iter().all(|v| {
        matches!(
            v,
            Bson::Double(_)
                | Bson::String(_)
                | Bson::Boolean(_)
                | Bson::Int32(_)
                | Bson::Int64(_)
                | Bson::Null
        )
    })
}

/// Convert a BSON value to an Arrow DataType.
fn bson_to_arrow_type(bson: &Bson) -> (DataType, String) {
    match bson {
        Bson::Double(_) => (DataType::Float64, "Double".to_string()),
        Bson::String(_) => (DataType::Utf8, "String".to_string()),
        Bson::Boolean(_) => (DataType::Boolean, "Boolean".to_string()),
        Bson::Int32(_) => (DataType::Int32, "Int32".to_string()),
        Bson::Int64(_) => (DataType::Int64, "Int64".to_string()),
        Bson::DateTime(_) => (
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            "DateTime".to_string(),
        ),
        Bson::ObjectId(_) => (DataType::Utf8, "ObjectId".to_string()),
        Bson::Null => (DataType::Utf8, "Null".to_string()), // Null defaults to Utf8
        Bson::Array(arr) if !arr.is_empty() => {
            // Infer array element type from first element
            let (elem_type, _) = bson_to_arrow_type(&arr[0]);
            (
                DataType::List(Arc::new(Field::new("item", elem_type, true))),
                "Array".to_string(),
            )
        }
        Bson::Array(_) => (
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            "Array".to_string(),
        ),
        Bson::Document(_) => (DataType::Utf8, "Document".to_string()), // Serialized as JSON
        Bson::Binary(_) => (DataType::Utf8, "Binary".to_string()), // Base64 encoded
        Bson::RegularExpression(_) => (DataType::Utf8, "Regex".to_string()),
        Bson::JavaScriptCode(_) => (DataType::Utf8, "JavaScript".to_string()),
        Bson::JavaScriptCodeWithScope(_) => (DataType::Utf8, "JavaScriptWithScope".to_string()),
        Bson::Timestamp(_) => (
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            "Timestamp".to_string(),
        ),
        Bson::Symbol(_) => (DataType::Utf8, "Symbol".to_string()),
        Bson::Decimal128(_) => (DataType::Float64, "Decimal128".to_string()),
        Bson::Undefined => (DataType::Utf8, "Undefined".to_string()),
        Bson::MaxKey => (DataType::Utf8, "MaxKey".to_string()),
        Bson::MinKey => (DataType::Utf8, "MinKey".to_string()),
        Bson::DbPointer(_) => (DataType::Utf8, "DbPointer".to_string()),
    }
}

/// Merge two Arrow types into a compatible supertype.
///
/// The `existing_hint` and `new_hint` parameters indicate the original BSON type
/// (e.g., "String" vs "Null"). This helps distinguish between a real string type
/// and a null/unknown that defaults to Utf8.
fn merge_arrow_types(
    existing: &DataType,
    existing_hint: &str,
    new: &DataType,
    new_hint: &str,
) -> (DataType, String) {
    if existing == new {
        // Same types - prefer the more specific hint
        let hint = if existing_hint == "Null" { new_hint } else { existing_hint };
        return (existing.clone(), hint.to_string());
    }

    match (existing, new) {
        // Numeric widening
        (DataType::Int32, DataType::Int64) | (DataType::Int64, DataType::Int32) => {
            (DataType::Int64, "Int64".to_string())
        }
        (DataType::Int32, DataType::Float64)
        | (DataType::Float64, DataType::Int32)
        | (DataType::Int64, DataType::Float64)
        | (DataType::Float64, DataType::Int64) => (DataType::Float64, "Float64".to_string()),

        // Handle Utf8 vs other types
        // Only treat Utf8 as "unknown" if the hint indicates it came from Null
        (DataType::Utf8, other) if existing_hint == "Null" => {
            (other.clone(), new_hint.to_string())
        }
        (other, DataType::Utf8) if new_hint == "Null" => {
            (other.clone(), existing_hint.to_string())
        }
        
        // If either is a real string (not null), result should be string
        (DataType::Utf8, _) | (_, DataType::Utf8) => (DataType::Utf8, "String".to_string()),

        // Default to Utf8 for incompatible types (mixed types become JSON strings)
        _ => (DataType::Utf8, "Mixed".to_string()),
    }
}

/// Builder for converting BSON documents to Arrow RecordBatches.
pub struct BsonToArrowConverter {
    schema: Arc<Schema>,
}

impl BsonToArrowConverter {
    /// Create a new converter with the given schema.
    pub fn new(schema: Arc<Schema>) -> Self {
        Self { schema }
    }

    /// Convert a batch of BSON documents to an Arrow RecordBatch.
    ///
    /// Uses a single-pass approach: iterates through documents once and
    /// updates all field builders simultaneously for better performance.
    pub fn convert(&self, documents: &[Document]) -> ConnectorResult<RecordBatch> {
        if documents.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        // Pre-create all builders
        let mut builders = self.create_builders(documents.len());

        // Single pass through all documents
        for doc in documents {
            for (idx, field) in self.schema.fields().iter().enumerate() {
                let field_name = field.name();
                let value = self.get_flattened_value(doc, field_name);
                self.append_value_to_builder(&mut builders[idx], field.data_type(), value);
            }
        }

        // Finish all builders and collect arrays
        let arrays: Vec<ArrayRef> = builders
            .into_iter()
            .zip(self.schema.fields().iter())
            .map(|(builder, field)| self.finish_builder(builder, field.data_type()))
            .collect();

        RecordBatch::try_new(self.schema.clone(), arrays).map_err(|e| {
            ConnectorError::Internal(format!("Failed to create RecordBatch: {}", e))
        })
    }

    /// Create builders for all fields.
    fn create_builders(&self, capacity: usize) -> Vec<TypedBuilder> {
        self.schema
            .fields()
            .iter()
            .map(|field| match field.data_type() {
                DataType::Boolean => TypedBuilder::Boolean(BooleanBuilder::with_capacity(capacity)),
                DataType::Int32 => TypedBuilder::Int32(Int32Builder::with_capacity(capacity)),
                DataType::Int64 => TypedBuilder::Int64(Int64Builder::with_capacity(capacity)),
                DataType::Float64 => TypedBuilder::Float64(Float64Builder::with_capacity(capacity)),
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    TypedBuilder::Timestamp(TimestampMillisecondBuilder::with_capacity(capacity))
                }
                DataType::Utf8 | DataType::List(_) | _ => {
                    TypedBuilder::String(StringBuilder::with_capacity(capacity, capacity * 64))
                }
            })
            .collect()
    }

    /// Append a value to the appropriate builder.
    fn append_value_to_builder(
        &self,
        builder: &mut TypedBuilder,
        data_type: &DataType,
        value: Option<&Bson>,
    ) {
        match (builder, data_type) {
            (TypedBuilder::Boolean(b), DataType::Boolean) => match value {
                Some(Bson::Boolean(v)) => b.append_value(*v),
                _ => b.append_null(),
            },
            (TypedBuilder::Int32(b), DataType::Int32) => match value {
                Some(Bson::Int32(v)) => b.append_value(*v),
                Some(Bson::Int64(v)) => b.append_value(*v as i32),
                _ => b.append_null(),
            },
            (TypedBuilder::Int64(b), DataType::Int64) => match value {
                Some(Bson::Int64(v)) => b.append_value(*v),
                Some(Bson::Int32(v)) => b.append_value(*v as i64),
                _ => b.append_null(),
            },
            (TypedBuilder::Float64(b), DataType::Float64) => match value {
                Some(Bson::Double(v)) => b.append_value(*v),
                Some(Bson::Int32(v)) => b.append_value(*v as f64),
                Some(Bson::Int64(v)) => b.append_value(*v as f64),
                Some(Bson::Decimal128(d)) => {
                    if let Ok(f) = d.to_string().parse::<f64>() {
                        b.append_value(f);
                    } else {
                        b.append_null();
                    }
                }
                _ => b.append_null(),
            },
            (TypedBuilder::Timestamp(b), DataType::Timestamp(TimeUnit::Millisecond, _)) => {
                match value {
                    Some(Bson::DateTime(dt)) => b.append_value(dt.timestamp_millis()),
                    Some(Bson::Timestamp(ts)) => b.append_value((ts.time as i64) * 1000),
                    _ => b.append_null(),
                }
            }
            (TypedBuilder::String(b), DataType::Utf8) => match value {
                Some(Bson::String(s)) => b.append_value(s),
                Some(Bson::ObjectId(oid)) => b.append_value(oid.to_hex()),
                Some(Bson::Null) => b.append_null(),
                Some(Bson::Binary(bin)) => {
                    b.append_value(base64::Engine::encode(
                        &base64::engine::general_purpose::STANDARD,
                        &bin.bytes,
                    ));
                }
                Some(other) => {
                    let json = serde_json::to_string(other).unwrap_or_default();
                    b.append_value(&json);
                }
                None => b.append_null(),
            },
            (TypedBuilder::String(b), DataType::List(_)) => match value {
                Some(Bson::Array(arr)) => {
                    let json = serde_json::to_string(arr).unwrap_or_default();
                    b.append_value(&json);
                }
                _ => b.append_null(),
            },
            // Default case - serialize to JSON string
            (TypedBuilder::String(b), _) => match value {
                Some(v) => {
                    let json = serde_json::to_string(v).unwrap_or_default();
                    b.append_value(&json);
                }
                None => b.append_null(),
            },
            _ => {} // Mismatched builder/type - should not happen
        }
    }

    /// Finish a builder and return the array.
    fn finish_builder(&self, builder: TypedBuilder, _data_type: &DataType) -> ArrayRef {
        match builder {
            TypedBuilder::Boolean(mut b) => Arc::new(b.finish()),
            TypedBuilder::Int32(mut b) => Arc::new(b.finish()),
            TypedBuilder::Int64(mut b) => Arc::new(b.finish()),
            TypedBuilder::Float64(mut b) => Arc::new(b.finish()),
            TypedBuilder::Timestamp(mut b) => {
                // Add UTC timezone to match schema expectation
                let array = b.finish();
                let tz: Arc<str> = Arc::from("UTC");
                Arc::new(array.with_timezone_opt(Some(tz)))
            }
            TypedBuilder::String(mut b) => Arc::new(b.finish()),
        }
    }

    /// Get a value from a document using the flattened field name.
    ///
    /// Returns a reference to the value to avoid cloning.
    fn get_flattened_value<'a>(&self, doc: &'a Document, field_name: &str) -> Option<&'a Bson> {
        // Split the field name by separator and traverse the document
        let parts: Vec<&str> = field_name.split(FIELD_SEPARATOR).collect();
        
        if parts.is_empty() {
            return None;
        }
        
        // Navigate through nested documents
        let mut current_doc = doc;
        for (i, part) in parts.iter().enumerate() {
            if i == parts.len() - 1 {
                // Last part - return the value reference
                return current_doc.get(*part);
            } else {
                // Intermediate part - navigate deeper
                match current_doc.get(*part) {
                    Some(Bson::Document(d)) => {
                        current_doc = d;
                    }
                    _ => return None,
                }
            }
        }
        
        None
    }
}

/// Extract scalar fields suitable for indexing.
///
/// Returns field names and their values for fields that can be indexed in ClickHouse.
pub fn extract_indexable_fields(
    doc: &Document,
    max_nested_depth: usize,
) -> Vec<(String, IndexableValue)> {
    let flattened = flatten_document(doc, "", 0, max_nested_depth);
    let mut result = Vec::new();

    for (name, value) in flattened {
        if let Some(indexable) = bson_to_indexable(&value) {
            result.push((name, indexable));
        }
    }

    result
}

/// A value that can be indexed in ClickHouse.
#[derive(Debug, Clone)]
pub enum IndexableValue {
    String(String),
    Int64(i64),
    Float64(f64),
    Boolean(bool),
    DateTime(i64), // Milliseconds since epoch
}

impl IndexableValue {
    /// Convert to SQL literal for ClickHouse.
    pub fn to_sql_literal(&self) -> String {
        match self {
            IndexableValue::String(s) => {
                // Escape single quotes
                format!("'{}'", s.replace('\'', "''"))
            }
            IndexableValue::Int64(i) => i.to_string(),
            IndexableValue::Float64(f) => f.to_string(),
            IndexableValue::Boolean(b) => if *b { "1" } else { "0" }.to_string(),
            IndexableValue::DateTime(ms) => {
                format!("toDateTime64({} / 1000, 3)", ms)
            }
        }
    }
}

/// Convert BSON value to IndexableValue if possible.
fn bson_to_indexable(bson: &Bson) -> Option<IndexableValue> {
    match bson {
        Bson::String(s) => Some(IndexableValue::String(s.clone())),
        Bson::ObjectId(oid) => Some(IndexableValue::String(oid.to_hex())),
        Bson::Int32(i) => Some(IndexableValue::Int64(*i as i64)),
        Bson::Int64(i) => Some(IndexableValue::Int64(*i)),
        Bson::Double(f) => Some(IndexableValue::Float64(*f)),
        Bson::Boolean(b) => Some(IndexableValue::Boolean(*b)),
        Bson::DateTime(dt) => Some(IndexableValue::DateTime(dt.timestamp_millis())),
        _ => None, // Arrays, documents, binary, etc. are not indexable
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn test_flatten_simple_document() {
        let doc = doc! {
            "name": "Alice",
            "age": 30
        };
        let flattened = flatten_document(&doc, "", 0, 3);
        assert_eq!(flattened.len(), 2);
    }

    #[test]
    fn test_flatten_nested_document() {
        let doc = doc! {
            "user": {
                "name": "Alice",
                "address": {
                    "city": "NYC"
                }
            }
        };
        let flattened = flatten_document(&doc, "", 0, 3);
        
        let field_names: Vec<&str> = flattened.iter().map(|(n, _)| n.as_str()).collect();
        assert!(field_names.contains(&"user__name"));
        assert!(field_names.contains(&"user__address__city"));
    }

    #[test]
    fn test_flatten_max_depth() {
        let doc = doc! {
            "level1": {
                "level2": {
                    "level3": {
                        "level4": "deep"
                    }
                }
            }
        };
        let flattened = flatten_document(&doc, "", 0, 2);
        
        // At depth 2, level3 should be serialized as JSON
        let field_names: Vec<&str> = flattened.iter().map(|(n, _)| n.as_str()).collect();
        assert!(field_names.contains(&"level1__level2__level3"));
    }

    #[test]
    fn test_infer_schema_basic() {
        let docs = vec![
            doc! { "_id": bson::oid::ObjectId::new(), "name": "Alice", "age": 30 },
            doc! { "_id": bson::oid::ObjectId::new(), "name": "Bob", "age": 25 },
        ];
        let schema = infer_schema(&docs, 3);
        
        assert_eq!(schema.sample_count, 2);
        assert!(schema.fields.iter().any(|f| f.name == "_id"));
        assert!(schema.fields.iter().any(|f| f.name == "name"));
        assert!(schema.fields.iter().any(|f| f.name == "age"));
    }

    #[test]
    fn test_bson_to_arrow_type() {
        assert!(matches!(
            bson_to_arrow_type(&Bson::String("test".to_string())).0,
            DataType::Utf8
        ));
        assert!(matches!(
            bson_to_arrow_type(&Bson::Int32(42)).0,
            DataType::Int32
        ));
        assert!(matches!(
            bson_to_arrow_type(&Bson::Double(3.14)).0,
            DataType::Float64
        ));
        assert!(matches!(
            bson_to_arrow_type(&Bson::Boolean(true)).0,
            DataType::Boolean
        ));
    }

    #[test]
    fn test_merge_arrow_types() {
        // Numeric widening
        assert_eq!(
            merge_arrow_types(&DataType::Int32, "Int32", &DataType::Int64, "Int64").0,
            DataType::Int64
        );
        assert_eq!(
            merge_arrow_types(&DataType::Int32, "Int32", &DataType::Float64, "Float64").0,
            DataType::Float64
        );
        
        // Null (Utf8 with "Null" hint) should yield to other types
        assert_eq!(
            merge_arrow_types(&DataType::Utf8, "Null", &DataType::Int32, "Int32").0,
            DataType::Int32
        );
        
        // Real string should stay string even when merged with other types
        assert_eq!(
            merge_arrow_types(&DataType::Utf8, "String", &DataType::Int32, "Int32").0,
            DataType::Utf8
        );
        
        // Real string merged with null should stay string
        assert_eq!(
            merge_arrow_types(&DataType::Utf8, "String", &DataType::Utf8, "Null").0,
            DataType::Utf8
        );
    }

    #[test]
    fn test_indexable_value_to_sql() {
        assert_eq!(
            IndexableValue::String("test".to_string()).to_sql_literal(),
            "'test'"
        );
        assert_eq!(
            IndexableValue::String("it's".to_string()).to_sql_literal(),
            "'it''s'"
        );
        assert_eq!(IndexableValue::Int64(42).to_sql_literal(), "42");
        assert_eq!(IndexableValue::Boolean(true).to_sql_literal(), "1");
    }
}
