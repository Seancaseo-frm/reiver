//! In-Place Parquet Rewriter
//!
//! Handles updates and deletes to Parquet files stored in R2/S3.
//! Since Parquet is an immutable format, we implement updates as:
//! 1. Read existing Parquet file
//! 2. Apply changes in memory (inserts, updates, deletes)
//! 3. Write updated Parquet file (atomic replace)
//!
//! This module is used by the materialized mode sync executor to maintain
//! up-to-date Parquet files as WAL/CDC events arrive.

use arrow::array::*;
use arrow::compute::{concat_batches, filter_record_batch};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, info};

use crate::warehouse::connectors::wal_index::types::{PrimaryKey, WalEvent, WalEventType};

// ============================================================================
// Types
// ============================================================================

/// A change to apply to a Parquet file.
#[derive(Debug, Clone)]
pub enum ParquetChange {
    /// Insert a new row.
    Insert {
        values: HashMap<String, ColumnValue>,
    },
    /// Update an existing row by primary key.
    Update {
        primary_key: PrimaryKey,
        new_values: HashMap<String, ColumnValue>,
    },
    /// Delete a row by primary key.
    Delete {
        primary_key: PrimaryKey,
    },
}

/// A column value that can be inserted or updated.
#[derive(Debug, Clone)]
pub enum ColumnValue {
    Null,
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    String(String),
    Binary(Vec<u8>),
    Date32(i32),
    Timestamp(i64),
}

impl ColumnValue {
    /// Convert WAL event column value to ParquetChange column value.
    pub fn from_wal(value: &crate::warehouse::connectors::wal_index::types::ColumnValue) -> Self {
        use crate::warehouse::connectors::wal_index::types::ColumnValue as WalValue;
        
        // WAL ColumnValue has a simpler set of variants
        match value {
            WalValue::Null => ColumnValue::Null,
            WalValue::Bool(v) => ColumnValue::Bool(*v),
            WalValue::Int64(v) => ColumnValue::Int64(*v),
            WalValue::Float64(v) => ColumnValue::Float64(*v),
            WalValue::String(v) => ColumnValue::String(v.clone()),
            WalValue::Bytes(v) => ColumnValue::Binary(v.clone()),
            WalValue::Timestamp(v) => ColumnValue::Timestamp(*v),
        }
    }
}

// ============================================================================
// Errors
// ============================================================================

/// Errors that can occur during Parquet rewriting.
#[derive(Debug, Error)]
pub enum RewriteError {
    #[error("Failed to read Parquet: {0}")]
    ReadError(String),
    
    #[error("Failed to write Parquet: {0}")]
    WriteError(String),
    
    #[error("Schema mismatch: {0}")]
    SchemaMismatch(String),
    
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    
    #[error("Primary key not found in schema")]
    PrimaryKeyNotFound,
}

/// Result type for rewrite operations.
pub type RewriteResult<T> = Result<T, RewriteError>;

// ============================================================================
// Parquet Rewriter
// ============================================================================

/// Rewrites Parquet files with applied changes.
pub struct ParquetRewriter {
    /// Primary key column names for row identification.
    primary_key_columns: Vec<String>,
    /// Compression to use when writing.
    compression: parquet::basic::Compression,
}

impl ParquetRewriter {
    /// Create a new Parquet rewriter.
    pub fn new(primary_key_columns: Vec<String>) -> Self {
        Self {
            primary_key_columns,
            compression: parquet::basic::Compression::SNAPPY,
        }
    }
    
    /// Create a rewriter with custom compression.
    pub fn with_compression(
        primary_key_columns: Vec<String>,
        compression: parquet::basic::Compression,
    ) -> Self {
        Self {
            primary_key_columns,
            compression,
        }
    }
    
    /// Apply changes to a Parquet file and return the updated bytes.
    ///
    /// This is the main entry point for in-place rewriting:
    /// 1. Read existing Parquet data
    /// 2. Apply changes (inserts, updates, deletes)
    /// 3. Return new Parquet bytes
    pub fn apply_changes(
        &self,
        parquet_data: &Bytes,
        changes: &[ParquetChange],
    ) -> RewriteResult<Bytes> {
        // Read existing data
        let batches = self.read_parquet(parquet_data)?;
        
        if batches.is_empty() {
            // No existing data, just apply inserts
            let inserts: Vec<_> = changes
                .iter()
                .filter_map(|c| match c {
                    ParquetChange::Insert { values } => Some(values),
                    _ => None,
                })
                .collect();
            
            if inserts.is_empty() {
                return Ok(parquet_data.clone());
            }
            
            // Would need schema to create new batch
            return Err(RewriteError::SchemaMismatch(
                "Cannot insert into empty file without schema".to_string(),
            ));
        }
        
        let schema = batches[0].schema();
        
        // Merge all batches into one
        let combined = concat_batches(&schema, &batches)?;
        
        // Apply changes
        let updated = self.apply_changes_to_batch(&combined, changes)?;
        
        // Write back to Parquet
        self.write_parquet(&updated)
    }
    
    /// Apply changes to a RecordBatch.
    pub fn apply_changes_to_batch(
        &self,
        batch: &RecordBatch,
        changes: &[ParquetChange],
    ) -> RewriteResult<RecordBatch> {
        use arrow::array::Array;
        
        let schema = batch.schema();
        let num_rows = batch.num_rows();
        
        // Build a filter for rows to keep (not deleted or updated)
        let mut rows_to_delete = vec![false; num_rows];
        let mut updates: Vec<HashMap<String, ColumnValue>> = Vec::new();
        let mut inserts: Vec<&HashMap<String, ColumnValue>> = Vec::new();
        
        for change in changes {
            match change {
                ParquetChange::Delete { primary_key } => {
                    if let Some(row_idx) = self.find_row_by_pk(batch, primary_key)? {
                        rows_to_delete[row_idx] = true;
                    }
                }
                ParquetChange::Update { primary_key, new_values } => {
                    if let Some(row_idx) = self.find_row_by_pk(batch, primary_key)? {
                        rows_to_delete[row_idx] = true; // Delete old row
                        updates.push(new_values.clone()); // Queue updated values as new row
                    }
                }
                ParquetChange::Insert { values } => {
                    inserts.push(values);
                }
            }
        }
        
        // Filter out deleted/updated rows
        let keep_mask: BooleanArray = rows_to_delete
            .iter()
            .map(|&deleted| Some(!deleted))
            .collect();
        
        let filtered = filter_record_batch(batch, &keep_mask)?;
        
        // Collect all new rows (updates + inserts)
        let all_new_rows: Vec<&HashMap<String, ColumnValue>> = updates
            .iter()
            .chain(inserts.iter().copied())
            .collect();
        
        if all_new_rows.is_empty() {
            return Ok(filtered);
        }
        
        // Build a new batch from the new rows
        let new_batch = self.build_batch_from_values(&schema, &all_new_rows)?;
        
        // Concatenate filtered batch with new rows
        if filtered.num_rows() == 0 {
            Ok(new_batch)
        } else {
            concat_batches(&schema, &[filtered, new_batch])
                .map_err(|e| RewriteError::Arrow(e))
        }
    }
    
    /// Build a RecordBatch from column value maps.
    fn build_batch_from_values(
        &self,
        schema: &SchemaRef,
        rows: &[&HashMap<String, ColumnValue>],
    ) -> RewriteResult<RecordBatch> {
        use arrow::array::{ArrayBuilder, StringBuilder, Int64Builder, Float64Builder, 
                          BooleanBuilder, Int32Builder, TimestampMillisecondBuilder, BinaryBuilder};
        
        if rows.is_empty() {
            return Err(RewriteError::WriteError("No rows to build".to_string()));
        }
        
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
        
        for field in schema.fields() {
            let column_name = field.name();
            let data_type = field.data_type();
            
            let array: ArrayRef = match data_type {
                DataType::Int64 => {
                    let mut builder = Int64Builder::with_capacity(rows.len());
                    for row in rows {
                        if let Some(value) = row.get(column_name) {
                            match value {
                                ColumnValue::Int64(v) => builder.append_value(*v),
                                ColumnValue::Null => builder.append_null(),
                                _ => builder.append_null(),
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Int32 => {
                    let mut builder = Int32Builder::with_capacity(rows.len());
                    for row in rows {
                        if let Some(value) = row.get(column_name) {
                            match value {
                                ColumnValue::Int32(v) => builder.append_value(*v),
                                ColumnValue::Int64(v) => match i32::try_from(*v) {
                                    Ok(v32) => builder.append_value(v32),
                                    Err(_) => {
                                        if field.is_nullable() {
                                            builder.append_null();
                                        } else {
                                            return Err(RewriteError::SchemaMismatch(format!(
                                                "Int64 value {} out of range for non-nullable Int32 column {}",
                                                v, column_name
                                            )));
                                        }
                                    }
                                },
                                ColumnValue::Null => builder.append_null(),
                                _ => builder.append_null(),
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Float64 => {
                    let mut builder = Float64Builder::with_capacity(rows.len());
                    for row in rows {
                        if let Some(value) = row.get(column_name) {
                            match value {
                                ColumnValue::Float64(v) => builder.append_value(*v),
                                ColumnValue::Float32(v) => builder.append_value(*v as f64),
                                ColumnValue::Int64(v) => builder.append_value(*v as f64),
                                ColumnValue::Null => builder.append_null(),
                                _ => builder.append_null(),
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Utf8 => {
                    let mut builder = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
                    for row in rows {
                        if let Some(value) = row.get(column_name) {
                            match value {
                                ColumnValue::String(v) => builder.append_value(v),
                                ColumnValue::Null => builder.append_null(),
                                _ => builder.append_null(),
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Boolean => {
                    let mut builder = BooleanBuilder::with_capacity(rows.len());
                    for row in rows {
                        if let Some(value) = row.get(column_name) {
                            match value {
                                ColumnValue::Bool(v) => builder.append_value(*v),
                                ColumnValue::Null => builder.append_null(),
                                _ => builder.append_null(),
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Timestamp(unit, _) => {
                    macro_rules! build_timestamp {
                        ($builder_ty:ty) => {{
                            let mut builder = <$builder_ty>::with_capacity(rows.len());
                            for row in rows {
                                if let Some(value) = row.get(column_name) {
                                    match value {
                                        ColumnValue::Timestamp(v) => builder.append_value(*v),
                                        ColumnValue::Int64(v) => builder.append_value(*v),
                                        ColumnValue::Null => builder.append_null(),
                                        _ => builder.append_null(),
                                    }
                                } else {
                                    builder.append_null();
                                }
                            }
                            Arc::new(builder.finish()) as Arc<dyn Array>
                        }};
                    }
                    match unit {
                        TimeUnit::Second => build_timestamp!(TimestampSecondBuilder),
                        TimeUnit::Millisecond => build_timestamp!(TimestampMillisecondBuilder),
                        TimeUnit::Microsecond => build_timestamp!(TimestampMicrosecondBuilder),
                        TimeUnit::Nanosecond => build_timestamp!(TimestampNanosecondBuilder),
                    }
                }
                DataType::Binary => {
                    let mut builder = BinaryBuilder::with_capacity(rows.len(), rows.len() * 64);
                    for row in rows {
                        if let Some(value) = row.get(column_name) {
                            match value {
                                ColumnValue::Binary(v) => builder.append_value(v),
                                ColumnValue::Null => builder.append_null(),
                                _ => builder.append_null(),
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                // For unsupported types, create null array
                _ => {
                    debug!(
                        column = column_name,
                        data_type = ?data_type,
                        "Using null array for unsupported data type"
                    );
                    arrow::array::new_null_array(data_type, rows.len())
                }
            };
            
            columns.push(array);
        }
        
        RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| RewriteError::Arrow(e))
    }
    
    /// Apply WAL events to a Parquet file.
    ///
    /// Converts WAL events to ParquetChanges and applies them.
    pub fn apply_wal_events(
        &self,
        parquet_data: &Bytes,
        events: &[WalEvent],
    ) -> RewriteResult<Bytes> {
        let changes: Vec<ParquetChange> = events
            .iter()
            .filter_map(|event| self.wal_event_to_change(event))
            .collect();
        
        if changes.is_empty() {
            return Ok(parquet_data.clone());
        }
        
        info!(
            event_count = events.len(),
            change_count = changes.len(),
            "Applying WAL events to Parquet"
        );
        
        self.apply_changes(parquet_data, &changes)
    }
    
    /// Convert a WAL event to a ParquetChange.
    fn wal_event_to_change(&self, event: &WalEvent) -> Option<ParquetChange> {
        match event.event_type {
            WalEventType::Insert => {
                let values: HashMap<String, ColumnValue> = event
                    .columns
                    .iter()
                    .map(|(k, v)| (k.clone(), ColumnValue::from_wal(v)))
                    .collect();
                Some(ParquetChange::Insert { values })
            }
            WalEventType::Update => {
                let new_values: HashMap<String, ColumnValue> = event
                    .columns
                    .iter()
                    .map(|(k, v)| (k.clone(), ColumnValue::from_wal(v)))
                    .collect();
                Some(ParquetChange::Update {
                    primary_key: event.primary_key.clone(),
                    new_values,
                })
            }
            WalEventType::Delete => {
                Some(ParquetChange::Delete {
                    primary_key: event.primary_key.clone(),
                })
            }
        }
    }
    
    // ========================================================================
    // Private Helpers
    // ========================================================================
    
    /// Read Parquet data into RecordBatches.
    fn read_parquet(&self, data: &Bytes) -> RewriteResult<Vec<RecordBatch>> {
        // Clone the Bytes to pass ownership - Bytes is cheap to clone (Arc-backed)
        let reader = ParquetRecordBatchReaderBuilder::try_new(data.clone())
            .map_err(|e| RewriteError::ReadError(e.to_string()))?
            .build()
            .map_err(|e| RewriteError::ReadError(e.to_string()))?;
        
        let batches: Result<Vec<_>, _> = reader.collect();
        batches.map_err(|e| RewriteError::ReadError(e.to_string()))
    }
    
    /// Write a RecordBatch to Parquet bytes.
    fn write_parquet(&self, batch: &RecordBatch) -> RewriteResult<Bytes> {
        let mut buffer = Vec::new();
        
        let props = WriterProperties::builder()
            .set_compression(self.compression)
            .build();
        
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))?;
        writer.write(batch)?;
        writer.close()?;
        
        Ok(Bytes::from(buffer))
    }
    
    /// Find a row by primary key (supports single and composite keys).
    fn find_row_by_pk(
        &self,
        batch: &RecordBatch,
        primary_key: &PrimaryKey,
    ) -> RewriteResult<Option<usize>> {
        use arrow::array::Array;

        if self.primary_key_columns.is_empty() {
            return Err(RewriteError::PrimaryKeyNotFound);
        }

        match primary_key {
            PrimaryKey::Composite(parts) => {
                if parts.len() != self.primary_key_columns.len() {
                    return Err(RewriteError::PrimaryKeyNotFound);
                }
                let col_indices: Vec<usize> = self.primary_key_columns.iter()
                    .map(|name| batch.schema().index_of(name)
                        .map_err(|_| RewriteError::PrimaryKeyNotFound))
                    .collect::<RewriteResult<_>>()?;

                let num_rows = batch.num_rows();
                'row: for row_idx in 0..num_rows {
                    for (part_pk, &col_idx) in parts.iter().zip(col_indices.iter()) {
                        let array = batch.column(col_idx);
                        if array.is_null(row_idx) || !Self::matches_single_pk(array.as_ref(), row_idx, part_pk) {
                            continue 'row;
                        }
                    }
                    return Ok(Some(row_idx));
                }
                Ok(None)
            }
            single_pk => {
                let pk_col = &self.primary_key_columns[0];
                let col_idx = batch.schema().index_of(pk_col)
                    .map_err(|_| RewriteError::PrimaryKeyNotFound)?;
                let array = batch.column(col_idx);

                for row_idx in 0..array.len() {
                    if array.is_null(row_idx) {
                        continue;
                    }
                    if Self::matches_single_pk(array.as_ref(), row_idx, single_pk) {
                        return Ok(Some(row_idx));
                    }
                }
                Ok(None)
            }
        }
    }

    fn matches_single_pk(array: &dyn Array, row_idx: usize, pk: &PrimaryKey) -> bool {
        match pk {
            PrimaryKey::Int64(expected) => {
                array.as_any().downcast_ref::<Int64Array>()
                    .map_or(false, |arr| arr.value(row_idx) == *expected)
            }
            PrimaryKey::String(expected) => {
                array.as_any().downcast_ref::<StringArray>()
                    .map_or(false, |arr| arr.value(row_idx) == expected.as_str())
            }
            PrimaryKey::Composite(_) => false,
        }
    }
}

/// Create Parquet bytes from RecordBatches.
///
/// Utility function for creating new Parquet files.
pub fn batches_to_parquet(
    batches: &[RecordBatch],
    compression: parquet::basic::Compression,
) -> RewriteResult<Bytes> {
    if batches.is_empty() {
        return Err(RewriteError::WriteError("No batches to write".to_string()));
    }
    
    let schema = batches[0].schema();
    let mut buffer = Vec::new();
    
    let props = WriterProperties::builder()
        .set_compression(compression)
        .build();
    
    let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props))?;
    
    for batch in batches {
        writer.write(batch)?;
    }
    
    writer.close()?;
    
    Ok(Bytes::from(buffer))
}

/// Read Parquet bytes into RecordBatches.
///
/// Utility function for reading Parquet files.
pub fn parquet_to_batches(data: &Bytes) -> RewriteResult<Vec<RecordBatch>> {
    // Clone the Bytes to pass ownership - Bytes is cheap to clone (Arc-backed)
    let reader = ParquetRecordBatchReaderBuilder::try_new(data.clone())
        .map_err(|e| RewriteError::ReadError(e.to_string()))?
        .build()
        .map_err(|e| RewriteError::ReadError(e.to_string()))?;
    
    let batches: Result<Vec<_>, _> = reader.collect();
    batches.map_err(|e| RewriteError::ReadError(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_column_value_from_wal() {
        use crate::warehouse::connectors::wal_index::types::ColumnValue as WalValue;
        
        let wal_value = WalValue::String("test".to_string());
        let pq_value = ColumnValue::from_wal(&wal_value);
        
        match pq_value {
            ColumnValue::String(s) => assert_eq!(s, "test"),
            _ => panic!("Expected String variant"),
        }
    }
    
    #[test]
    fn test_parquet_rewriter_new() {
        let rewriter = ParquetRewriter::new(vec!["id".to_string()]);
        assert_eq!(rewriter.primary_key_columns, vec!["id"]);
    }

    #[test]
    fn test_int64_overflow_to_int32_returns_error() {
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("val", DataType::Int32, false),
        ]));

        let rewriter = ParquetRewriter::new(vec!["id".to_string()]);
        let mut row = HashMap::new();
        row.insert("val".to_string(), ColumnValue::Int64(3_000_000_000));
        let rows: Vec<&HashMap<String, ColumnValue>> = vec![&row];

        let result = rewriter.build_batch_from_values(&schema, &rows);
        assert!(
            result.is_err(),
            "Expected error for out-of-range Int64 in non-nullable Int32 column",
        );
    }

    #[test]
    fn test_int64_fits_in_int32_succeeds() {
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("val", DataType::Int32, false),
        ]));

        let rewriter = ParquetRewriter::new(vec!["id".to_string()]);
        let mut row = HashMap::new();
        row.insert("val".to_string(), ColumnValue::Int64(42));
        let rows: Vec<&HashMap<String, ColumnValue>> = vec![&row];

        let batch = rewriter.build_batch_from_values(&schema, &rows).unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(col.value(0), 42);
    }

    #[test]
    fn test_int64_overflow_nullable_appends_null() {
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("val", DataType::Int32, true),
        ]));

        let rewriter = ParquetRewriter::new(vec!["id".to_string()]);
        let mut row = HashMap::new();
        row.insert("val".to_string(), ColumnValue::Int64(3_000_000_000));
        let rows: Vec<&HashMap<String, ColumnValue>> = vec![&row];

        let batch = rewriter.build_batch_from_values(&schema, &rows).unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert!(col.is_null(0), "Overflowing Int64 in nullable Int32 column should produce null");
    }
}
