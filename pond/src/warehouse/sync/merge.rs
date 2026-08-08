//! Shared merge utilities for copy-on-write sync and compaction.
//!
//! Provides Parquet reading, PK-based row merging, and metadata column
//! stripping so both the sync executor and compaction worker can reuse
//! the same logic.

use anyhow::Result;
use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, StringArray};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::util::display::ArrayFormatter;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::sync::Arc;

/// Read Parquet bytes into RecordBatches.
pub fn read_parquet_bytes(data: &Bytes) -> Result<Vec<RecordBatch>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(data.clone())
        .map_err(|e| anyhow::anyhow!("Failed to create Parquet reader: {}", e))?
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build Parquet reader: {}", e))?;

    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| anyhow::anyhow!("Failed to read Parquet batch: {}", e))?;
        batches.push(batch);
    }
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    tracing::debug!(batches = batches.len(), total_rows = total_rows, "Read Parquet bytes into batches");
    Ok(batches)
}

/// Sentinel returned by [`cell_to_string`] for NULL values.
///
/// Uses a NUL byte so it can never collide with a real (non-null) cell value,
/// which is always valid UTF-8 text.
pub const NULL_SENTINEL: &str = "\0";

/// Map an Arrow DataType to a stable, version-independent tag for PK hashing.
///
/// Arrow's `Display` implementation for `DataType` can change between crate
/// versions (e.g. `"Utf8"` vs `"String"`). Using a hand-maintained tag
/// guarantees PK keys remain consistent across upgrades.
fn stable_type_tag(dt: &DataType) -> &'static str {
    match dt {
        DataType::Boolean => "Bool",
        DataType::Int8 => "I8",
        DataType::Int16 => "I16",
        DataType::Int32 => "I32",
        DataType::Int64 => "I64",
        DataType::UInt8 => "U8",
        DataType::UInt16 => "U16",
        DataType::UInt32 => "U32",
        DataType::UInt64 => "U64",
        DataType::Float16 => "F16",
        DataType::Float32 => "F32",
        DataType::Float64 => "F64",
        DataType::Utf8 | DataType::LargeUtf8 => "S",
        DataType::Binary | DataType::LargeBinary => "B",
        DataType::Date32 => "D32",
        DataType::Date64 => "D64",
        DataType::Timestamp(_, _) => "TS",
        DataType::Decimal128(_, _) => "Dec128",
        DataType::Decimal256(_, _) => "Dec256",
        _ => "Other",
    }
}

/// Extract a string representation of a cell value for use in PK key building.
///
/// Returns [`NULL_SENTINEL`] for NULL values so they are always
/// distinguishable from real values (including empty strings).
///
/// Returns an error if the array type cannot be formatted.  All standard Arrow
/// array types are supported by `ArrayFormatter`, so an error indicates a
/// corrupted array or an exotic extension type that should never appear in PK
/// columns.
pub fn cell_to_string(col: &dyn Array, row: usize) -> Result<String> {
    if col.is_null(row) {
        return Ok(NULL_SENTINEL.to_string());
    }

    let type_tag = stable_type_tag(col.data_type());

    let raw_value = if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        arr.value(row).to_string()
    } else if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        arr.value(row).to_string()
    } else if let Ok(fmt) = ArrayFormatter::try_new(col, &Default::default()) {
        fmt.value(row).to_string()
    } else {
        anyhow::bail!(
            "cell_to_string: unsupported array type {:?} at row {} — \
             PK columns must use a formattable Arrow type",
            col.data_type(),
            row,
        );
    };

    Ok(format!("{}:{}", type_tag, raw_value))
}

/// Build a deterministic string key from composite PK columns for a given row.
///
/// NULL components are encoded as a `\x00` marker byte (no length prefix).
/// Non-NULL components are encoded as `\x01` + length-prefixed type-tagged
/// value (e.g. `"I64:42"`). Components are separated by `\x1F`. The tag +
/// length-prefix scheme makes the encoding injective even when values
/// contain the separator byte.
fn build_pk_key(pk_cols: &[&dyn Array], row: usize) -> Result<String> {
    let mut key = String::new();
    for (i, col) in pk_cols.iter().enumerate() {
        if i > 0 {
            key.push('\x1F');
        }
        if col.is_null(row) {
            key.push('\x00');
        } else {
            let val = cell_to_string(*col, row)?;
            key.push('\x01');
            key.push_str(&val.len().to_string());
            key.push(':');
            key.push_str(&val);
        }
    }
    Ok(key)
}

/// Merge new batches into existing batches by primary key.
///
/// For rows with matching PKs, the new row replaces the existing one.
/// Rows in existing batches with no matching PK in new data are kept as-is.
/// Rows in new batches with no matching PK in existing data are appended.
///
/// If `pk_columns` is empty, the new batches are simply appended.
///
pub fn merge_batches_by_pk(
    existing_batches: &[RecordBatch],
    new_batches: &[RecordBatch],
    pk_columns: &[String],
) -> Result<Vec<RecordBatch>> {
    if pk_columns.is_empty() {
        let mut result: Vec<RecordBatch> = existing_batches.to_vec();
        result.extend(new_batches.iter().cloned());
        return Ok(result);
    }

    if new_batches.is_empty() {
        return Ok(existing_batches.to_vec());
    }

    let new_schema = new_batches[0].schema();
    let mut missing_new: Vec<&str> = Vec::new();
    let new_pk_indices: Vec<usize> = pk_columns
        .iter()
        .filter_map(|name| {
            match new_schema.index_of(name) {
                Ok(idx) => Some(idx),
                Err(_) => {
                    missing_new.push(name.as_str());
                    None
                }
            }
        })
        .collect();

    if !missing_new.is_empty() {
        anyhow::bail!(
            "PK columns missing from new data schema: [{}]",
            missing_new.join(", ")
        );
    }

    // Resolve PK column indices in existing schema (only if there are existing batches)
    let pk_indices: Vec<usize> = if !existing_batches.is_empty() {
        let schema = existing_batches[0].schema();
        let mut missing_existing: Vec<&str> = Vec::new();
        let indices: Vec<usize> = pk_columns
            .iter()
            .filter_map(|name| {
                match schema.index_of(name) {
                    Ok(idx) => Some(idx),
                    Err(_) => {
                        missing_existing.push(name.as_str());
                        None
                    }
                }
            })
            .collect();

        if !missing_existing.is_empty() {
            anyhow::bail!(
                "PK columns missing from existing schema: [{}]",
                missing_existing.join(", ")
            );
        }
        indices
    } else {
        Vec::new()
    };

    // Pass 1: build dedup map and cache keys per (batch_idx, row_idx).
    // For duplicate PKs, `seen_pks` retains the *last* occurrence.
    let mut seen_pks: HashMap<String, (usize, usize)> = HashMap::new();
    let mut new_batch_keys: Vec<Vec<String>> = Vec::with_capacity(new_batches.len());
    for (batch_idx, batch) in new_batches.iter().enumerate() {
        let pk_cols: Vec<&dyn Array> = new_pk_indices
            .iter()
            .map(|&idx| batch.column(idx).as_ref())
            .collect();
        let mut keys: Vec<String> = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = build_pk_key(&pk_cols, row)?;
            seen_pks.insert(key.clone(), (batch_idx, row));
            keys.push(key);
        }
        new_batch_keys.push(keys);
    }

    // Filter existing batches: keep only rows whose PK is NOT in new data.
    // Pre-compute keys for existing batches to avoid rebuilding per-row.
    let mut result = Vec::new();
    for batch in existing_batches {
        let pk_cols: Vec<&dyn Array> = pk_indices
            .iter()
            .map(|&idx| batch.column(idx).as_ref())
            .collect();

        let existing_keys: Vec<String> = (0..batch.num_rows())
            .map(|row| build_pk_key(&pk_cols, row))
            .collect::<Result<Vec<_>>>()?;

        let mask: BooleanArray = existing_keys
            .iter()
            .map(|key| Some(!seen_pks.contains_key(key)))
            .collect();

        let filtered = filter_record_batch(batch, &mask)
            .map_err(|e| anyhow::anyhow!("Failed to filter existing batch during merge: {}", e))?;
        if filtered.num_rows() > 0 {
            result.push(filtered);
        }
    }

    // Build keep-masks for each new batch using cached keys.
    for (batch_idx, batch) in new_batches.iter().enumerate() {
        let keys = &new_batch_keys[batch_idx];
        let mask: BooleanArray = keys
            .iter()
            .enumerate()
            .map(|(row, key)| {
                Some(seen_pks.get(key) == Some(&(batch_idx, row)))
            })
            .collect();
        let filtered = filter_record_batch(batch, &mask)
            .map_err(|e| anyhow::anyhow!("Failed to deduplicate new batch during merge: {}", e))?;
        if filtered.num_rows() > 0 {
            result.push(filtered);
        }
    }

    // Reconcile schemas: if batches come from different schema versions,
    // project them all to a unified schema to avoid mixed-schema output.
    if result.len() > 1 {
        let first_schema = result[0].schema();
        let needs_reconciliation = result.iter().skip(1).any(|b| b.schema() != first_schema);
        if needs_reconciliation {
            let unified = unify_batch_schemas(&result)?;
            tracing::debug!(
                unified_columns = unified.fields().len(),
                "Schema reconciliation required during merge"
            );
            result = result
                .iter()
                .map(|b| project_batch_to_schema(b, &unified))
                .collect::<Result<Vec<_>>>()?;
        }
    }

    let rows_after: usize = result.iter().map(|b| b.num_rows()).sum();
    tracing::debug!(
        existing_batches = existing_batches.len(),
        new_batches = new_batches.len(),
        output_batches = result.len(),
        rows_after = rows_after,
        "PK merge completed"
    );
    Ok(result)
}

/// Compute a unified schema from all batches by taking the union of fields.
///
/// Fields present in some batches but not others are marked nullable.
/// If the same field name appears with different types, returns an error.
pub fn unify_batch_schemas(batches: &[RecordBatch]) -> Result<SchemaRef> {
    if batches.is_empty() {
        anyhow::bail!("Cannot unify schemas of zero batches");
    }

    let mut fields: Vec<Field> = Vec::new();
    let mut field_indices: HashMap<String, usize> = HashMap::new();

    for batch in batches {
        for field in batch.schema().fields() {
            match field_indices.get(field.name()) {
                Some(&idx) => {
                    let existing = &fields[idx];
                    if existing.data_type() != field.data_type() {
                        anyhow::bail!(
                            "Schema mismatch for column '{}': {:?} vs {:?}",
                            field.name(),
                            existing.data_type(),
                            field.data_type(),
                        );
                    }
                    if field.is_nullable() && !existing.is_nullable() {
                        fields[idx] = existing.clone().with_nullable(true);
                    }
                }
                None => {
                    let idx = fields.len();
                    // Fields not present in all batches must be nullable
                    let f = if batches.iter().all(|b| b.schema().field_with_name(field.name()).is_ok()) {
                        field.as_ref().clone()
                    } else {
                        field.as_ref().clone().with_nullable(true)
                    };
                    fields.push(f);
                    field_indices.insert(field.name().clone(), idx);
                }
            }
        }
    }

    Ok(Arc::new(arrow::datatypes::Schema::new(fields)))
}

/// Project a batch to a target schema, padding missing columns with nulls.
pub fn project_batch_to_schema(batch: &RecordBatch, target: &SchemaRef) -> Result<RecordBatch> {
    let source_schema = batch.schema();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target.fields().len());

    for field in target.fields() {
        match source_schema.index_of(field.name()) {
            Ok(idx) => columns.push(batch.column(idx).clone()),
            Err(_) => {
                columns.push(arrow::array::new_null_array(field.data_type(), batch.num_rows()));
            }
        }
    }

    RecordBatch::try_new(target.clone(), columns)
        .map_err(|e| anyhow::anyhow!("Failed to project batch to unified schema: {}", e))
}

/// Strip `_dh_sync_version`, `_dh_op`, and `_dh_last_op` metadata columns from batches.
///
/// Used during merge-on-write to clean legacy files that still contain
/// these columns. Returns batches without the metadata columns.
pub fn strip_metadata_columns(batches: &[RecordBatch]) -> Result<Vec<RecordBatch>> {
    const META_COLS: &[&str] = &["_dh_sync_version", "_dh_op", "_dh_last_op"];

    let mut result = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        let has_meta = schema.fields().iter().any(|f| META_COLS.contains(&f.name().as_str()));

        if !has_meta {
            result.push(batch.clone());
            continue;
        }

        let keep_indices: Vec<usize> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| !META_COLS.contains(&f.name().as_str()))
            .map(|(i, _)| i)
            .collect();

        let new_fields: Vec<arrow::datatypes::FieldRef> = keep_indices
            .iter()
            .map(|&i| schema.field(i).clone().into())
            .collect();
        let new_schema = std::sync::Arc::new(arrow::datatypes::Schema::new_with_metadata(
            new_fields,
            schema.metadata().clone(),
        ));

        let new_columns: Vec<arrow::array::ArrayRef> =
            keep_indices.iter().map(|&i| batch.column(i).clone()).collect();

        let stripped_count = schema.fields().len() - keep_indices.len();
        tracing::debug!(stripped_columns = stripped_count, "Stripped metadata columns from batch");
        let new_batch = RecordBatch::try_new(new_schema, new_columns)
            .map_err(|e| anyhow::anyhow!("Failed to strip metadata columns: {}", e))?;
        result.push(new_batch);
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn make_schema_with_meta() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("_dh_sync_version", DataType::Int64, false),
            Field::new("_dh_op", DataType::Utf8, false),
        ]))
    }

    #[test]
    fn test_merge_batches_by_pk_replaces_matching() {
        let schema = make_schema();

        let existing = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])),
            ],
        )
        .unwrap();

        let new = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![2, 4])),
                Arc::new(StringArray::from(vec!["Bobby", "Dave"])),
            ],
        )
        .unwrap();

        let result =
            merge_batches_by_pk(&[existing], &[new], &["id".to_string()]).unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4); // id=1,3 kept + id=2,4 from new

        let mut all_rows: Vec<(i64, String)> = Vec::new();
        for batch in &result {
            let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let names = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..batch.num_rows() {
                all_rows.push((ids.value(i), names.value(i).to_string()));
            }
        }
        all_rows.sort_by_key(|(id, _)| *id);

        assert_eq!(all_rows[0], (1, "Alice".to_string()));
        assert_eq!(all_rows[1], (2, "Bobby".to_string())); // replaced
        assert_eq!(all_rows[2], (3, "Carol".to_string()));
        assert_eq!(all_rows[3], (4, "Dave".to_string())); // new
    }

    #[test]
    fn test_merge_batches_by_pk_empty_pk_appends() {
        let schema = make_schema();

        let existing = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["Alice"])),
            ],
        )
        .unwrap();

        let new = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(StringArray::from(vec!["Bob"])),
            ],
        )
        .unwrap();

        let result = merge_batches_by_pk(&[existing], &[new], &[]).unwrap();
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_merge_batches_empty_existing() {
        let schema = make_schema();

        let new = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            ],
        )
        .unwrap();

        let result =
            merge_batches_by_pk(&[], &[new], &["id".to_string()]).unwrap();
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_strip_metadata_columns() {
        let schema = make_schema_with_meta();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
                Arc::new(Int64Array::from(vec![5, 10])),
                Arc::new(StringArray::from(vec!["I", "U"])),
            ],
        )
        .unwrap();

        let result = strip_metadata_columns(&[batch]).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_columns(), 2);
        assert_eq!(result[0].schema().field(0).name(), "id");
        assert_eq!(result[0].schema().field(1).name(), "name");
    }

    #[test]
    fn test_strip_metadata_columns_no_meta() {
        let schema = make_schema();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["Alice"])),
            ],
        )
        .unwrap();

        let result = strip_metadata_columns(&[batch]).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_columns(), 2);
    }

    #[test]
    fn test_merge_deduplicates_new_batches_by_pk() {
        let schema = make_schema();

        let existing = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["Alice"])),
            ],
        )
        .unwrap();

        // Two new batches both contain id=2, only the last one should survive
        let new1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![2, 3])),
                Arc::new(StringArray::from(vec!["Bob_v1", "Carol"])),
            ],
        )
        .unwrap();

        let new2 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(StringArray::from(vec!["Bob_v2"])),
            ],
        )
        .unwrap();

        let result = merge_batches_by_pk(
            &[existing],
            &[new1, new2],
            &["id".to_string()],
        )
        .unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3, "Should have exactly 3 rows: id=1, id=2 (v2), id=3");

        let mut all_rows: Vec<(i64, String)> = Vec::new();
        for batch in &result {
            let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let names = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..batch.num_rows() {
                all_rows.push((ids.value(i), names.value(i).to_string()));
            }
        }
        all_rows.sort_by_key(|(id, _)| *id);

        assert_eq!(all_rows[0], (1, "Alice".to_string()));
        assert_eq!(all_rows[1], (2, "Bob_v2".to_string()), "Last occurrence should win");
        assert_eq!(all_rows[2], (3, "Carol".to_string()));
    }

    #[test]
    fn test_merge_dedup_within_single_new_batch() {
        let schema = make_schema();

        // Single new batch has duplicate PK id=1
        let new_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 1])),
                Arc::new(StringArray::from(vec!["First", "Second"])),
            ],
        )
        .unwrap();

        let result = merge_batches_by_pk(
            &[],
            &[new_batch],
            &["id".to_string()],
        )
        .unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1, "Duplicate PK should be deduped to one row");

        let names = result[0].column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(names.value(0), "Second", "Last occurrence should win");
    }

    #[test]
    fn test_cell_to_string_fallback_differs_by_value_not_index() {
        use arrow::array::Date32Array;

        let arr = Date32Array::from(vec![Some(100), Some(200)]);
        let s0 = cell_to_string(&arr, 0).unwrap();
        let s1 = cell_to_string(&arr, 1).unwrap();
        assert_ne!(s0, s1, "different values at different indices must produce different keys");

        let same = Date32Array::from(vec![Some(42), Some(42)]);
        let k0 = cell_to_string(&same, 0).unwrap();
        let k1 = cell_to_string(&same, 1).unwrap();
        assert_eq!(k0, k1, "same value at different indices must produce the same key");
    }

    // ========== Regression tests for bug fixes ==========

    #[test]
    fn test_merge_large_existing_batches_correctness() {
        let schema = make_schema();

        let existing = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            ],
        )
        .unwrap();

        let new_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![2, 4])),
                Arc::new(StringArray::from(vec!["B_new", "D_new"])),
            ],
        )
        .unwrap();

        let result = merge_batches_by_pk(
            &[existing],
            &[new_batch],
            &["id".to_string()],
        )
        .unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5, "Should have 5 rows total (3 kept + 2 replaced)");

        let mut all_ids: Vec<i64> = Vec::new();
        let mut all_names: Vec<String> = Vec::new();
        for batch in &result {
            let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let names = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..batch.num_rows() {
                all_ids.push(ids.value(i));
                all_names.push(names.value(i).to_string());
            }
        }
        all_ids.sort();
        assert_eq!(all_ids, vec![1, 2, 3, 4, 5]);

        let id2_idx = all_ids.iter().position(|&id| id == 2).unwrap();
        let id4_idx = all_ids.iter().position(|&id| id == 4).unwrap();

        let mut paired: Vec<(i64, String)> = Vec::new();
        for batch in &result {
            let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let names = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..batch.num_rows() {
                paired.push((ids.value(i), names.value(i).to_string()));
            }
        }
        paired.sort_by_key(|(id, _)| *id);
        assert_eq!(paired[1].1, "B_new", "ID 2 should have new value");
        assert_eq!(paired[3].1, "D_new", "ID 4 should have new value");
        assert_eq!(paired[0].1, "a", "ID 1 should keep original value");
    }

    #[test]
    fn test_cell_to_string_null_vs_empty_string() {
        let arr = StringArray::from(vec![Some(""), None]);
        let empty = cell_to_string(&arr, 0).unwrap();
        let null = cell_to_string(&arr, 1).unwrap();
        assert_ne!(
            empty, null,
            "NULL and empty string must produce different representations"
        );
        assert_eq!(null, NULL_SENTINEL);
        assert_eq!(empty, "S:", "Empty string should be type-prefixed");
    }

    #[test]
    fn test_merge_distinguishes_null_and_empty_string_pk() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("val", DataType::Utf8, false),
        ]));

        let existing = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some(""), None])),
                Arc::new(StringArray::from(vec!["empty_old", "null_old"])),
            ],
        )
        .unwrap();

        let new_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("")])),
                Arc::new(StringArray::from(vec!["empty_new"])),
            ],
        )
        .unwrap();

        let result = merge_batches_by_pk(
            &[existing],
            &[new_batch],
            &["id".to_string()],
        )
        .unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 2,
            "NULL-keyed row and empty-string-keyed row are distinct PKs"
        );

        let mut all_rows: Vec<(Option<String>, String)> = Vec::new();
        for batch in &result {
            let ids = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            let vals = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..batch.num_rows() {
                let id = if ids.is_null(i) { None } else { Some(ids.value(i).to_string()) };
                all_rows.push((id, vals.value(i).to_string()));
            }
        }

        let null_row = all_rows.iter().find(|(id, _)| id.is_none()).unwrap();
        assert_eq!(null_row.1, "null_old", "NULL-keyed row should be untouched");

        let empty_row = all_rows.iter().find(|(id, _)| *id == Some("".to_string())).unwrap();
        assert_eq!(empty_row.1, "empty_new", "Empty-string-keyed row should be replaced");
    }

    #[test]
    fn test_cell_to_string_type_discriminator() {
        let int_arr = Int64Array::from(vec![42]);
        let str_arr = StringArray::from(vec!["42"]);

        let int_key = cell_to_string(&int_arr, 0).unwrap();
        let str_key = cell_to_string(&str_arr, 0).unwrap();

        assert_ne!(
            int_key, str_key,
            "Int64(42) and String(\"42\") must produce different PK keys: int={}, str={}",
            int_key, str_key,
        );
        assert!(int_key.starts_with("I64:"), "Int64 key must be type-prefixed: {}", int_key);
        assert!(str_key.starts_with("S:"), "String key must be type-prefixed: {}", str_key);
    }

    #[test]
    fn test_cell_to_string_uses_value_not_index() {
        let arr = Int64Array::from(vec![100, 200, 100]);
        let key0 = cell_to_string(&arr, 0).unwrap();
        let key2 = cell_to_string(&arr, 2).unwrap();
        assert_eq!(
            key0, key2,
            "Same value at different row indices must produce the same key"
        );

        let key1 = cell_to_string(&arr, 1).unwrap();
        assert_ne!(
            key0, key1,
            "Different values must produce different keys"
        );
    }

    #[test]
    fn test_cell_to_string_null_sentinel() {
        let arr = Int64Array::from(vec![Some(42), None]);
        let null_key = cell_to_string(&arr, 1).unwrap();
        assert_eq!(null_key, NULL_SENTINEL, "NULL values must return the sentinel");

        let val_key = cell_to_string(&arr, 0).unwrap();
        assert_ne!(val_key, NULL_SENTINEL, "Non-NULL values must not match sentinel");
    }

    #[test]
    fn test_cell_to_string_returns_result_not_panic() {
        // Verify that cell_to_string returns Ok for all commonly used PK
        // column types instead of panicking.
        use arrow::array::{Float64Array, BooleanArray as ArrowBooleanArray, Date32Array};

        assert!(cell_to_string(&Int64Array::from(vec![1]), 0).is_ok());
        assert!(cell_to_string(&StringArray::from(vec!["x"]), 0).is_ok());
        assert!(cell_to_string(&Float64Array::from(vec![1.5]), 0).is_ok());
        assert!(cell_to_string(&ArrowBooleanArray::from(vec![true]), 0).is_ok());
        assert!(cell_to_string(&Date32Array::from(vec![18000]), 0).is_ok());

        let result = cell_to_string(&Int64Array::from(vec![7]), 0)
            .unwrap();
        assert!(
            result.starts_with("I64:"),
            "Result should include the stable type tag: {}",
            result,
        );
    }

    #[test]
    fn test_stable_type_tag_consistency() {
        use arrow::array::{Float64Array, BooleanArray as ArrowBooleanArray, Date32Array, UInt64Array};

        let i64_arr = Int64Array::from(vec![1]);
        let str_arr = StringArray::from(vec!["x"]);
        let f64_arr = Float64Array::from(vec![1.0]);
        let bool_arr = ArrowBooleanArray::from(vec![true]);
        let date_arr = Date32Array::from(vec![100]);
        let u64_arr = UInt64Array::from(vec![42u64]);

        let cases: Vec<(&dyn Array, &str)> = vec![
            (&i64_arr, "I64:"),
            (&str_arr, "S:"),
            (&f64_arr, "F64:"),
            (&bool_arr, "Bool:"),
            (&date_arr, "D32:"),
            (&u64_arr, "U64:"),
        ];

        for (arr, expected_prefix) in cases {
            let key = cell_to_string(arr, 0).unwrap();
            assert!(
                key.starts_with(expected_prefix),
                "Expected prefix '{}' for {:?}, got: {}",
                expected_prefix,
                arr.data_type(),
                key,
            );
        }
    }

    #[test]
    fn test_merge_mixed_schema_reconciles() {
        let schema_v1 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let schema_v2 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("email", DataType::Utf8, true),
        ]));

        let existing = RecordBatch::try_new(
            schema_v1,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            ],
        )
        .unwrap();

        let new_batch = RecordBatch::try_new(
            schema_v2,
            vec![
                Arc::new(Int64Array::from(vec![3])),
                Arc::new(StringArray::from(vec!["Carol"])),
                Arc::new(StringArray::from(vec![Some("carol@test.com")])),
            ],
        )
        .unwrap();

        let result = merge_batches_by_pk(
            &[existing],
            &[new_batch],
            &["id".to_string()],
        )
        .unwrap();

        let unified_schema = result[0].schema();
        for batch in &result {
            assert_eq!(
                batch.schema(),
                unified_schema,
                "All result batches must share the same unified schema"
            );
        }

        assert_eq!(unified_schema.fields().len(), 3, "Unified schema should have 3 columns");
        assert!(unified_schema.field_with_name("email").is_ok(), "Unified schema should include 'email'");

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_merge_mixed_schema_type_conflict_errors() {
        let schema_v1 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let schema_v2 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        let existing = RecordBatch::try_new(
            schema_v1,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![100])),
            ],
        )
        .unwrap();

        let new_batch = RecordBatch::try_new(
            schema_v2,
            vec![
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(StringArray::from(vec!["hello"])),
            ],
        )
        .unwrap();

        let result = merge_batches_by_pk(
            &[existing],
            &[new_batch],
            &["id".to_string()],
        );

        assert!(result.is_err(), "Type conflict should produce an error");
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Schema mismatch"), "Error should mention schema mismatch: {}", err_msg);
    }
}
