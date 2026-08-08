//! Column-Level Statistics Sidecar
//!
//! Produces a lightweight JSON sidecar file alongside each Parquet file at
//! write time.  The sidecar contains per-column min/max, null count, and
//! distinct count, enabling query-time file pruning without downloading
//! Parquet footers.
//!
//! # Naming Convention
//!
//! For a Parquet file at `<key>.parquet`, the sidecar is stored at
//! `<key>.parquet.stats.json`.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use super::parquet::{write_parquet, ParquetError, ParquetResult, WriteOptions};
use super::parquet_metadata::{
    extract_file_stats, ColumnDataType, ColumnValue, FileStats,
};

// ============================================================================
// Sidecar Data Structures
// ============================================================================

/// Column-level statistics sidecar for a single Parquet file.
///
/// Stored as `<parquet_key>.stats.json` in R2 alongside the Parquet file.
/// Designed to be general-purpose -- works for any data source (blockchain,
/// database connectors, external Parquet, user uploads, etc.).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileColumnStats {
    /// Schema version for forward compatibility.
    pub version: u8,
    /// Total number of rows in the Parquet file.
    pub row_count: u64,
    /// Compressed size of the Parquet file in bytes.
    pub size_bytes: u64,
    /// Per-column statistics.
    pub columns: Vec<ColumnSidecarStats>,
    /// Columns by which this file's data is sorted (set during compaction).
    ///
    /// When present, min/max statistics on these columns are guaranteed tight
    /// (no false positives), enabling more aggressive query-time pruning.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort_columns: Option<Vec<String>>,
}

/// Statistics for a single column within a Parquet file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSidecarStats {
    /// Column name.
    pub name: String,
    /// Data type as a human-readable string (e.g. "int64", "utf8").
    pub data_type: String,
    /// Number of null values in this column.
    pub null_count: u64,
    /// Distinct value count (from Parquet statistics, if available).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distinct_count: Option<u64>,
    /// Minimum value (typed as JSON: number, string, or bool).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<serde_json::Value>,
    /// Maximum value (typed as JSON: number, string, or bool).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<serde_json::Value>,
}

// ============================================================================
// Conversions
// ============================================================================

impl FileColumnStats {
    /// Current sidecar schema version.
    const VERSION: u8 = 1;

    /// Build sidecar stats from the existing `parquet_metadata::FileStats`.
    pub fn from_file_stats(stats: &FileStats, size_bytes: u64) -> Self {
        let columns = stats
            .columns
            .values()
            .map(|cs| ColumnSidecarStats {
                name: cs.name.clone(),
                data_type: column_data_type_str(&cs.data_type),
                null_count: cs.null_count.unwrap_or(0),
                distinct_count: cs.distinct_count,
                min: cs.min.as_ref().and_then(column_value_to_json),
                max: cs.max.as_ref().and_then(column_value_to_json),
            })
            .collect();

        Self {
            version: Self::VERSION,
            row_count: stats.row_count,
            size_bytes,
            columns,
            sort_columns: None,
        }
    }

    /// Set the sort columns metadata (call after building stats during compaction).
    pub fn with_sort_columns(mut self, sort_columns: Vec<String>) -> Self {
        if sort_columns.is_empty() {
            self.sort_columns = None;
        } else {
            self.sort_columns = Some(sort_columns);
        }
        self
    }

    /// Serialize to JSON bytes suitable for uploading to R2.
    pub fn to_json_bytes(&self) -> Result<Bytes, ParquetError> {
        serde_json::to_vec(self)
            .map(Bytes::from)
            .map_err(|e| ParquetError::Write(format!("Failed to serialize stats sidecar: {}", e)))
    }

    /// Deserialize from JSON bytes (e.g. downloaded from R2).
    pub fn from_json_bytes(data: &[u8]) -> Result<Self, ParquetError> {
        serde_json::from_slice(data)
            .map_err(|e| ParquetError::Read(format!("Failed to deserialize stats sidecar: {}", e)))
    }

    /// Return the R2 key for the stats sidecar given a Parquet key.
    pub fn stats_key(parquet_key: &str) -> String {
        format!("{}.stats.json", parquet_key)
    }
}

// ============================================================================
// Core Write Function
// ============================================================================

/// Write Arrow RecordBatches to Parquet **and** extract column-level stats.
///
/// This is the primary integration point.  All write paths should call this
/// instead of `write_parquet` directly to automatically produce a sidecar.
///
/// Returns `(parquet_bytes, stats)`.
pub fn write_parquet_with_stats(
    schema: Arc<Schema>,
    batches: &[RecordBatch],
    options: WriteOptions,
) -> ParquetResult<(Bytes, FileColumnStats)> {
    let parquet_bytes = write_parquet(schema, batches, options)?;

    let file_stats = extract_file_stats("memory", &parquet_bytes)
        .map_err(|e| ParquetError::Read(format!("Failed to extract stats from written Parquet: {}", e)))?;

    let stats = FileColumnStats::from_file_stats(&file_stats, parquet_bytes.len() as u64);

    Ok((parquet_bytes, stats))
}

// ============================================================================
// Helpers
// ============================================================================

/// Convert `ColumnDataType` to a stable string representation.
fn column_data_type_str(dt: &ColumnDataType) -> String {
    match dt {
        ColumnDataType::Int32 => "int32".to_string(),
        ColumnDataType::Int64 => "int64".to_string(),
        ColumnDataType::Float32 => "float32".to_string(),
        ColumnDataType::Float64 => "float64".to_string(),
        ColumnDataType::String => "utf8".to_string(),
        ColumnDataType::Boolean => "boolean".to_string(),
        ColumnDataType::Timestamp => "timestamp".to_string(),
        ColumnDataType::Other => "other".to_string(),
    }
}

/// Convert a `ColumnValue` to a `serde_json::Value` for the sidecar.
///
/// Returns `None` for non-finite floats (NaN, Infinity) so callers don't
/// mistake a meaningless stat for a real one.
fn column_value_to_json(val: &ColumnValue) -> Option<serde_json::Value> {
    match val {
        ColumnValue::Int32(v) => Some(serde_json::Value::Number((*v).into())),
        ColumnValue::Int64(v) => Some(serde_json::Value::Number((*v).into())),
        ColumnValue::Float32(v) => {
            serde_json::Number::from_f64(*v as f64)
                .map(serde_json::Value::Number)
        }
        ColumnValue::Float64(v) => {
            serde_json::Number::from_f64(*v)
                .map(serde_json::Value::Number)
        }
        ColumnValue::String(s) => Some(serde_json::Value::String(s.clone())),
        ColumnValue::Boolean(b) => Some(serde_json::Value::Bool(*b)),
        ColumnValue::Bytes(b) => {
            Some(serde_json::Value::String(hex::encode(b)))
        }
    }
}

/// Extract column-level stats from already-written Parquet bytes.
///
/// Use this when the Parquet data was produced by `write_parquet_chunked` or
/// another code path that does not use `write_parquet_with_stats` directly.
pub fn extract_stats_from_parquet_bytes(parquet_bytes: &[u8]) -> ParquetResult<FileColumnStats> {
    let file_stats = extract_file_stats("memory", parquet_bytes)
        .map_err(|e| ParquetError::Read(format!("Failed to extract stats: {}", e)))?;
    Ok(FileColumnStats::from_file_stats(&file_stats, parquet_bytes.len() as u64))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field};

    fn make_test_batches() -> (Arc<Schema>, Vec<RecordBatch>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec![
                    Some("alice"),
                    Some("bob"),
                    None,
                    Some("carol"),
                    Some("dave"),
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(95.0),
                    Some(87.5),
                    Some(92.0),
                    None,
                    Some(88.0),
                ])),
            ],
        )
        .unwrap();

        (schema, vec![batch])
    }

    #[test]
    fn test_write_parquet_with_stats_produces_valid_stats() {
        let (schema, batches) = make_test_batches();
        let (parquet_bytes, stats) =
            write_parquet_with_stats(schema, &batches, WriteOptions::default()).unwrap();

        assert!(!parquet_bytes.is_empty());
        assert_eq!(stats.version, 1);
        assert_eq!(stats.row_count, 5);
        assert!(stats.size_bytes > 0);
        assert_eq!(stats.columns.len(), 3);

        // Check id column
        let id_col = stats.columns.iter().find(|c| c.name == "id").unwrap();
        assert_eq!(id_col.data_type, "int64");
        assert_eq!(id_col.null_count, 0);
        assert_eq!(id_col.min, Some(serde_json::json!(1)));
        assert_eq!(id_col.max, Some(serde_json::json!(5)));

        // Check name column (has one null)
        let name_col = stats.columns.iter().find(|c| c.name == "name").unwrap();
        assert_eq!(name_col.data_type, "utf8");
        assert_eq!(name_col.null_count, 1);

        // Check score column (has one null)
        let score_col = stats.columns.iter().find(|c| c.name == "score").unwrap();
        assert_eq!(score_col.data_type, "float64");
        assert_eq!(score_col.null_count, 1);
    }

    #[test]
    fn test_stats_json_round_trip() {
        let (schema, batches) = make_test_batches();
        let (_, stats) =
            write_parquet_with_stats(schema, &batches, WriteOptions::default()).unwrap();

        let json_bytes = stats.to_json_bytes().unwrap();
        let deserialized = FileColumnStats::from_json_bytes(&json_bytes).unwrap();

        assert_eq!(deserialized.version, stats.version);
        assert_eq!(deserialized.row_count, stats.row_count);
        assert_eq!(deserialized.size_bytes, stats.size_bytes);
        assert_eq!(deserialized.columns.len(), stats.columns.len());

        for (orig, deser) in stats.columns.iter().zip(deserialized.columns.iter()) {
            assert_eq!(orig.name, deser.name);
            assert_eq!(orig.data_type, deser.data_type);
            assert_eq!(orig.null_count, deser.null_count);
            assert_eq!(orig.min, deser.min);
            assert_eq!(orig.max, deser.max);
        }
    }

    #[test]
    fn test_stats_key_generation() {
        assert_eq!(
            FileColumnStats::stats_key("projects/abc/warm/src/tbl/2025-01-15/1_0000_uuid.parquet"),
            "projects/abc/warm/src/tbl/2025-01-15/1_0000_uuid.parquet.stats.json"
        );
    }

    #[test]
    fn test_empty_batches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
        )
        .unwrap();

        let (parquet_bytes, stats) =
            write_parquet_with_stats(schema, &[batch], WriteOptions::default()).unwrap();

        assert!(!parquet_bytes.is_empty());
        assert_eq!(stats.row_count, 0);
    }

    #[test]
    fn test_column_value_to_json_coverage() {
        assert_eq!(column_value_to_json(&ColumnValue::Int32(42)), Some(serde_json::json!(42)));
        assert_eq!(column_value_to_json(&ColumnValue::Int64(100)), Some(serde_json::json!(100)));
        assert_eq!(column_value_to_json(&ColumnValue::Float64(3.14)), Some(serde_json::json!(3.14)));
        assert_eq!(
            column_value_to_json(&ColumnValue::String("hello".to_string())),
            Some(serde_json::json!("hello"))
        );
        assert_eq!(column_value_to_json(&ColumnValue::Boolean(true)), Some(serde_json::json!(true)));
        assert_eq!(
            column_value_to_json(&ColumnValue::Bytes(vec![0xde, 0xad])),
            Some(serde_json::json!("dead"))
        );
    }

    #[test]
    fn test_column_value_to_json_nan_returns_none() {
        assert_eq!(column_value_to_json(&ColumnValue::Float64(f64::NAN)), None);
        assert_eq!(column_value_to_json(&ColumnValue::Float64(f64::INFINITY)), None);
        assert_eq!(column_value_to_json(&ColumnValue::Float64(f64::NEG_INFINITY)), None);
        assert_eq!(column_value_to_json(&ColumnValue::Float32(f32::NAN)), None);
    }
}
