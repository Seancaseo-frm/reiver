//! Parquet Metadata Utilities
//!
//! Provides efficient reading of Parquet file metadata without downloading
//! the entire file. This is crucial for the hybrid indexing strategy where
//! we extract numeric min/max stats from metadata (free) and only build FST
//! indexes for low-cardinality string columns.
//!
//! # Cost Savings
//!
//! - Parquet footer is typically 4-8KB
//! - Downloading a 100GB file just for stats costs ~$10 egress
//! - Reading just the footer costs ~$0.0001

use std::collections::HashMap;
use thiserror::Error;

/// Errors that can occur when reading Parquet metadata.
#[derive(Debug, Error)]
pub enum ParquetMetadataError {
    #[error("Failed to read Parquet file: {0}")]
    ReadError(String),

    #[error("Invalid Parquet file: {0}")]
    InvalidFile(String),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    /// The footer probe was too small; the caller should retry with at least
    /// `needed` bytes from the end of the file.
    #[error("Need more data: {needed} bytes required")]
    NeedMoreData { needed: usize },
}

/// Result type for Parquet metadata operations.
pub type ParquetMetadataResult<T> = Result<T, ParquetMetadataError>;

/// Statistics for a column extracted from Parquet metadata.
#[derive(Debug, Clone)]
pub struct ColumnStats {
    /// Column name
    pub name: String,
    /// Column data type
    pub data_type: ColumnDataType,
    /// Minimum value (if available)
    pub min: Option<ColumnValue>,
    /// Maximum value (if available)
    pub max: Option<ColumnValue>,
    /// Number of null values
    pub null_count: Option<u64>,
    /// Distinct value count (if available)
    pub distinct_count: Option<u64>,
    /// Total row count for this column
    pub row_count: u64,
}

/// Simplified data type for column statistics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnDataType {
    Int32,
    Int64,
    Float32,
    Float64,
    String,
    Boolean,
    Timestamp,
    Other,
}

/// A column value from Parquet statistics.
#[derive(Debug, Clone)]
pub enum ColumnValue {
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
    Boolean(bool),
    Bytes(Vec<u8>),
}

impl ColumnValue {
    /// Convert to i64 if numeric, for range comparisons.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            ColumnValue::Int32(v) => Some(*v as i64),
            ColumnValue::Int64(v) => Some(*v),
            _ => None,
        }
    }

    /// Convert to f64 if numeric, for range comparisons.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            ColumnValue::Int32(v) => Some(*v as f64),
            ColumnValue::Int64(v) => Some(*v as f64),
            ColumnValue::Float32(v) => Some(*v as f64),
            ColumnValue::Float64(v) => Some(*v),
            _ => None,
        }
    }

    /// Convert to string if applicable.
    pub fn as_string(&self) -> Option<&str> {
        match self {
            ColumnValue::String(s) => Some(s.as_str()),
            _ => None,
        }
    }
}

/// Aggregated file statistics from Parquet metadata.
#[derive(Debug, Clone)]
pub struct FileStats {
    /// Path to the file
    pub file_path: String,
    /// Total row count
    pub row_count: u64,
    /// Statistics per column
    pub columns: HashMap<String, ColumnStats>,
}

impl FileStats {
    /// Get columns that are good candidates for FST indexing.
    ///
    /// Returns columns that:
    /// - Are string type
    /// - Have low cardinality (distinct_count < threshold)
    /// - Are not high-cardinality by name pattern (e.g., *_id, *_uuid)
    pub fn fst_candidate_columns(&self, max_cardinality: u64) -> Vec<String> {
        self.columns
            .iter()
            .filter(|(name, stats)| {
                // Must be string type
                if stats.data_type != ColumnDataType::String {
                    return false;
                }

                // Skip high-cardinality columns by name
                if is_high_cardinality_column_name(name) {
                    return false;
                }

                // Check distinct count if available
                if let Some(distinct) = stats.distinct_count {
                    distinct <= max_cardinality
                } else {
                    // If no distinct count, be conservative and assume it's worth trying
                    // (will be filtered during actual FST building)
                    true
                }
            })
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Get numeric columns with their min/max stats.
    ///
    /// These stats can be used for skip indexing without any additional cost.
    pub fn numeric_columns_with_stats(&self) -> Vec<(String, Option<f64>, Option<f64>)> {
        self.columns
            .iter()
            .filter(|(_, stats)| {
                matches!(
                    stats.data_type,
                    ColumnDataType::Int32
                        | ColumnDataType::Int64
                        | ColumnDataType::Float32
                        | ColumnDataType::Float64
                )
            })
            .map(|(name, stats)| {
                let min = stats.min.as_ref().and_then(|v| v.as_f64());
                let max = stats.max.as_ref().and_then(|v| v.as_f64());
                (name.clone(), min, max)
            })
            .collect()
    }
}

/// Check if a column name suggests high cardinality.
pub fn is_high_cardinality_column_name(name: &str) -> bool {
    let lower = name.to_lowercase();

    // Common high-cardinality patterns
    lower.ends_with("_id")
        || lower.ends_with("_uuid")
        || lower.ends_with("_token")
        || lower.ends_with("_hash")
        || lower == "id"
        || lower == "uuid"
        || lower.contains("timestamp")
        || lower.contains("created_at")
        || lower.contains("updated_at")
        || lower.contains("modified_at")
        || lower.contains("email")
        || lower.contains("phone")
        || lower.contains("ip_address")
}

/// Extract file statistics from Parquet metadata bytes.
///
/// This function parses the Parquet file footer to extract column statistics
/// without needing to read the actual data.
///
/// # Arguments
///
/// * `file_path` - Path to the file (for reporting)
/// * `data` - Complete file data (or at minimum, the footer bytes)
///
/// # Note
///
/// For large files, consider using `extract_stats_from_footer` with just
/// the last N bytes of the file to avoid downloading the entire file.
pub fn extract_file_stats(file_path: &str, data: &[u8]) -> ParquetMetadataResult<FileStats> {
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;

    let reader = SerializedFileReader::new(bytes::Bytes::from(data.to_vec()))
        .map_err(|e| ParquetMetadataError::ReadError(e.to_string()))?;

    let metadata = reader.metadata();
    let file_metadata = metadata.file_metadata();

    let mut total_rows = 0u64;
    let mut columns: HashMap<String, ColumnStats> = HashMap::new();

    // Iterate through row groups to collect stats
    for rg_idx in 0..metadata.num_row_groups() {
        let rg_metadata = metadata.row_group(rg_idx);
        total_rows += rg_metadata.num_rows() as u64;

        // Collect stats from each column chunk
        for col_idx in 0..rg_metadata.num_columns() {
            let col_chunk = rg_metadata.column(col_idx);
            let col_path = col_chunk.column_path();
            let col_name = col_path.string();

            // Get or create column stats
            let stats_entry = columns
                .entry(col_name.clone())
                .or_insert_with(|| ColumnStats {
                    name: col_name.clone(),
                    data_type: physical_type_to_data_type(col_chunk.column_type()),
                    min: None,
                    max: None,
                    null_count: Some(0),
                    distinct_count: None,
                    row_count: 0,
                });

            stats_entry.row_count += rg_metadata.num_rows() as u64;

            if let Some(stats) = col_chunk.statistics() {
                // Aggregate null counts
                if let Some(current) = stats_entry.null_count {
                    if let Some(null_count) = stats.null_count_opt() {
                        stats_entry.null_count = Some(current + null_count);
                    }
                }

                // Aggregate distinct counts: use max (values overlap across row groups,
                // so summing overestimates; max is a better lower-bound heuristic)
                if let Some(distinct) = stats.distinct_count_opt() {
                    stats_entry.distinct_count =
                        Some(stats_entry.distinct_count.unwrap_or(0).max(distinct));
                }

                // Extract min/max (taking the overall min/max across row groups)
                // Note: update_min_max handles the case where min/max is not set via min_opt()/max_opt()
                update_min_max(stats_entry, stats.clone());
            }
        }
    }

    Ok(FileStats {
        file_path: file_path.to_string(),
        row_count: total_rows,
        columns,
    })
}

/// Convert Parquet physical type to our simplified data type.
fn physical_type_to_data_type(physical_type: parquet::basic::Type) -> ColumnDataType {
    use parquet::basic::Type;

    match physical_type {
        Type::BOOLEAN => ColumnDataType::Boolean,
        Type::INT32 => ColumnDataType::Int32,
        Type::INT64 => ColumnDataType::Int64,
        Type::INT96 => ColumnDataType::Timestamp,
        Type::FLOAT => ColumnDataType::Float32,
        Type::DOUBLE => ColumnDataType::Float64,
        Type::BYTE_ARRAY | Type::FIXED_LEN_BYTE_ARRAY => ColumnDataType::String,
    }
}

/// Update min/max values in column stats from Parquet statistics.
fn update_min_max(
    stats_entry: &mut ColumnStats,
    parquet_stats: parquet::file::statistics::Statistics,
) {
    use parquet::file::statistics::Statistics;

    match parquet_stats {
        Statistics::Int32(s) => {
            if let Some(min) = s.min_opt() {
                let new_min = ColumnValue::Int32(*min);
                stats_entry.min = Some(merge_min(stats_entry.min.take(), new_min));
            }
            if let Some(max) = s.max_opt() {
                let new_max = ColumnValue::Int32(*max);
                stats_entry.max = Some(merge_max(stats_entry.max.take(), new_max));
            }
        }
        Statistics::Int64(s) => {
            if let Some(min) = s.min_opt() {
                let new_min = ColumnValue::Int64(*min);
                stats_entry.min = Some(merge_min(stats_entry.min.take(), new_min));
            }
            if let Some(max) = s.max_opt() {
                let new_max = ColumnValue::Int64(*max);
                stats_entry.max = Some(merge_max(stats_entry.max.take(), new_max));
            }
        }
        Statistics::Float(s) => {
            if let Some(min) = s.min_opt() {
                let new_min = ColumnValue::Float32(*min);
                stats_entry.min = Some(merge_min(stats_entry.min.take(), new_min));
            }
            if let Some(max) = s.max_opt() {
                let new_max = ColumnValue::Float32(*max);
                stats_entry.max = Some(merge_max(stats_entry.max.take(), new_max));
            }
        }
        Statistics::Double(s) => {
            if let Some(min) = s.min_opt() {
                let new_min = ColumnValue::Float64(*min);
                stats_entry.min = Some(merge_min(stats_entry.min.take(), new_min));
            }
            if let Some(max) = s.max_opt() {
                let new_max = ColumnValue::Float64(*max);
                stats_entry.max = Some(merge_max(stats_entry.max.take(), new_max));
            }
        }
        Statistics::ByteArray(s) => {
            if let Some(min) = s.min_opt() {
                if let Ok(s) = std::str::from_utf8(min.data()) {
                    let new_min = ColumnValue::String(s.to_string());
                    stats_entry.min = Some(merge_min(stats_entry.min.take(), new_min));
                }
            }
            if let Some(max) = s.max_opt() {
                if let Ok(s) = std::str::from_utf8(max.data()) {
                    let new_max = ColumnValue::String(s.to_string());
                    stats_entry.max = Some(merge_max(stats_entry.max.take(), new_max));
                }
            }
        }
        Statistics::FixedLenByteArray(s) => {
            if let Some(min) = s.min_opt() {
                let new_min = ColumnValue::Bytes(min.data().to_vec());
                stats_entry.min = Some(merge_min(stats_entry.min.take(), new_min));
            }
            if let Some(max) = s.max_opt() {
                let new_max = ColumnValue::Bytes(max.data().to_vec());
                stats_entry.max = Some(merge_max(stats_entry.max.take(), new_max));
            }
        }
        Statistics::Boolean(s) => {
            if let Some(min) = s.min_opt() {
                let new_min = ColumnValue::Boolean(*min);
                stats_entry.min = Some(merge_min(stats_entry.min.take(), new_min));
            }
            if let Some(max) = s.max_opt() {
                let new_max = ColumnValue::Boolean(*max);
                stats_entry.max = Some(merge_max(stats_entry.max.take(), new_max));
            }
        }
        Statistics::Int96(_) => {
            // Int96 is deprecated and typically used for timestamps
            // We don't track min/max for these
        }
    }
}

/// Merge two min values, keeping the smaller one.
fn merge_min(current: Option<ColumnValue>, new: ColumnValue) -> ColumnValue {
    match current {
        None => new,
        Some(curr) => {
            match (&curr, &new) {
                (ColumnValue::Int32(a), ColumnValue::Int32(b)) => {
                    if b < a {
                        new
                    } else {
                        curr
                    }
                }
                (ColumnValue::Int64(a), ColumnValue::Int64(b)) => {
                    if b < a {
                        new
                    } else {
                        curr
                    }
                }
                (ColumnValue::Float32(a), ColumnValue::Float32(b)) => {
                    if a.is_nan() {
                        new
                    } else if b.is_nan() {
                        curr
                    } else if b < a {
                        new
                    } else {
                        curr
                    }
                }
                (ColumnValue::Float64(a), ColumnValue::Float64(b)) => {
                    if a.is_nan() {
                        new
                    } else if b.is_nan() {
                        curr
                    } else if b < a {
                        new
                    } else {
                        curr
                    }
                }
                (ColumnValue::String(a), ColumnValue::String(b)) => {
                    if b < a {
                        new
                    } else {
                        curr
                    }
                }
                (ColumnValue::Boolean(a), ColumnValue::Boolean(b)) => {
                    // false < true
                    if !b && *a {
                        new
                    } else {
                        curr
                    }
                }
                (ColumnValue::Bytes(a), ColumnValue::Bytes(b)) => {
                    if b < a {
                        new
                    } else {
                        curr
                    }
                }
                _ => curr,
            }
        }
    }
}

/// Merge two max values, keeping the larger one.
fn merge_max(current: Option<ColumnValue>, new: ColumnValue) -> ColumnValue {
    match current {
        None => new,
        Some(curr) => {
            match (&curr, &new) {
                (ColumnValue::Int32(a), ColumnValue::Int32(b)) => {
                    if b > a {
                        new
                    } else {
                        curr
                    }
                }
                (ColumnValue::Int64(a), ColumnValue::Int64(b)) => {
                    if b > a {
                        new
                    } else {
                        curr
                    }
                }
                (ColumnValue::Float32(a), ColumnValue::Float32(b)) => {
                    if a.is_nan() {
                        new
                    } else if b.is_nan() {
                        curr
                    } else if b > a {
                        new
                    } else {
                        curr
                    }
                }
                (ColumnValue::Float64(a), ColumnValue::Float64(b)) => {
                    if a.is_nan() {
                        new
                    } else if b.is_nan() {
                        curr
                    } else if b > a {
                        new
                    } else {
                        curr
                    }
                }
                (ColumnValue::String(a), ColumnValue::String(b)) => {
                    if b > a {
                        new
                    } else {
                        curr
                    }
                }
                (ColumnValue::Boolean(a), ColumnValue::Boolean(b)) => {
                    // true > false
                    if *b && !a {
                        new
                    } else {
                        curr
                    }
                }
                (ColumnValue::Bytes(a), ColumnValue::Bytes(b)) => {
                    if b > a {
                        new
                    } else {
                        curr
                    }
                }
                _ => curr,
            }
        }
    }
}

/// Decision on how to build indexes for a file.
#[derive(Debug, Clone)]
pub struct IndexingDecision {
    /// Columns that should have FST indexes built (requires full download)
    pub fst_columns: Vec<String>,
    /// Numeric columns with min/max stats (free from metadata)
    pub numeric_stats: Vec<(String, Option<f64>, Option<f64>)>,
    /// Whether we need to download the full file
    pub needs_full_download: bool,
    /// Estimated row count from metadata
    pub estimated_rows: u64,
}

impl IndexingDecision {
    /// Create an indexing decision from file stats.
    ///
    /// # Arguments
    /// * `stats` - File statistics from Parquet metadata
    /// * `max_fst_cardinality` - Maximum distinct values for FST indexing
    pub fn from_file_stats(stats: &FileStats, max_fst_cardinality: u64) -> Self {
        let fst_columns = stats.fst_candidate_columns(max_fst_cardinality);
        let numeric_stats = stats.numeric_columns_with_stats();
        let needs_full_download = !fst_columns.is_empty();

        Self {
            fst_columns,
            numeric_stats,
            needs_full_download,
            estimated_rows: stats.row_count,
        }
    }
}

// ============================================================================
// Footer-Only Reading (Efficient Metadata Extraction)
// ============================================================================

/// Minimum bytes to read for a Parquet footer (magic + metadata length).
/// Parquet files end with a 4-byte metadata-length field followed by the
/// 4-byte magic number `PAR1`.
const FOOTER_PROBE_SIZE: usize = 8 * 1024; // 8 KB covers most footers

/// Parse Parquet metadata from raw footer bytes.
///
/// The caller should provide the **last N bytes** of the file (typically 8 KB)
/// together with the total file size. This function locates the Parquet footer
/// within those trailing bytes and returns column statistics.
///
/// # Arguments
/// * `file_path`  - Logical file path (for reporting only).
/// * `tail_bytes` - The last N bytes of the Parquet file.
/// * `file_size`  - Total size of the file in bytes.
pub fn extract_stats_from_footer(
    file_path: &str,
    tail_bytes: &[u8],
    file_size: usize,
) -> ParquetMetadataResult<FileStats> {
    use parquet::file::metadata::ParquetMetaDataReader;

    if tail_bytes.len() < 8 {
        return Err(ParquetMetadataError::InvalidFile(
            "Footer bytes too small".to_string(),
        ));
    }

    // Use the official `try_parse_sized` API which handles the footer
    // magic bytes, metadata length, and Thrift decoding internally.
    let tail = bytes::Bytes::from(tail_bytes.to_vec());
    let mut reader = ParquetMetaDataReader::new();
    if let Err(e) = reader.try_parse_sized(&tail, file_size as u64) {
        if let parquet::errors::ParquetError::NeedMoreData(needed) = e {
            return Err(ParquetMetadataError::NeedMoreData { needed });
        }
        return Err(ParquetMetadataError::ReadError(format!(
            "Failed to parse footer: {}",
            e
        )));
    }

    let metadata = reader.finish().map_err(|e| {
        ParquetMetadataError::ReadError(format!("Failed to finish metadata: {}", e))
    })?;

    stats_from_parquet_metadata(file_path, &metadata)
}

/// Build `FileStats` from already-decoded `ParquetMetaData`.
fn stats_from_parquet_metadata(
    file_path: &str,
    metadata: &parquet::file::metadata::ParquetMetaData,
) -> ParquetMetadataResult<FileStats> {
    let mut total_rows = 0u64;
    let mut columns: HashMap<String, ColumnStats> = HashMap::new();

    for rg_idx in 0..metadata.num_row_groups() {
        let rg_metadata = metadata.row_group(rg_idx);
        total_rows += rg_metadata.num_rows() as u64;

        for col_idx in 0..rg_metadata.num_columns() {
            let col_chunk = rg_metadata.column(col_idx);
            let col_name = col_chunk.column_path().string();

            let entry = columns
                .entry(col_name.clone())
                .or_insert_with(|| ColumnStats {
                    name: col_name.clone(),
                    data_type: physical_type_to_data_type(col_chunk.column_type()),
                    min: None,
                    max: None,
                    null_count: Some(0),
                    distinct_count: None,
                    row_count: 0,
                });

            entry.row_count += rg_metadata.num_rows() as u64;

            if let Some(stats) = col_chunk.statistics() {
                if let Some(current) = entry.null_count {
                    if let Some(nc) = stats.null_count_opt() {
                        entry.null_count = Some(current + nc);
                    }
                }
                if let Some(distinct) = stats.distinct_count_opt() {
                    entry.distinct_count = Some(entry.distinct_count.unwrap_or(0).max(distinct));
                }
                update_min_max(entry, stats.clone());
            }
        }
    }

    Ok(FileStats {
        file_path: file_path.to_string(),
        row_count: total_rows,
        columns,
    })
}

/// Recommended probe size for footer-only reads.
pub fn footer_probe_size() -> usize {
    FOOTER_PROBE_SIZE
}

// ============================================================================
// Timestamp-Based Synthetic Partitioning
// ============================================================================

/// Column-name heuristics used to find a timestamp column when no explicit
/// `time_column` is configured.
fn is_likely_timestamp_column(name: &str, data_type: &ColumnDataType) -> bool {
    if *data_type == ColumnDataType::Timestamp {
        return true;
    }
    // Int64 columns with time-like names are often epoch-millis timestamps.
    if *data_type == ColumnDataType::Int64 {
        let lower = name.to_lowercase();
        return lower.ends_with("_at")
            || lower.ends_with("_time")
            || lower.ends_with("_ts")
            || lower == "event_ts"
            || lower == "timestamp"
            || lower == "created"
            || lower == "updated";
    }
    false
}

/// Derive synthetic `YYYY/MM` partitions from Parquet footer timestamp statistics.
///
/// For each file, reads its min timestamp value and assigns a `YYYY/MM` partition
/// key. If the timestamps overlap too heavily (>80% of files land in the same
/// month), the function returns `None` to signal that this strategy is not useful.
///
/// # Arguments
/// * `file_stats`  - File path + stats pairs (from `extract_file_stats` or `extract_stats_from_footer`).
/// * `time_column`  - Explicit column name, or `None` to auto-detect.
///
/// # Returns
/// `Some((column, partitions))` on success, where `partitions` maps `YYYY/MM` keys
/// to file paths. `None` if no usable timestamp column is found or if the
/// distribution is too skewed.
pub fn derive_timestamp_partitions(
    file_stats: &[(String, FileStats)],
    time_column: Option<&str>,
) -> Option<(String, HashMap<String, Vec<String>>)> {
    if file_stats.is_empty() {
        return None;
    }

    // Step 1: Identify the timestamp column.
    let ts_column = if let Some(col) = time_column {
        col.to_string()
    } else {
        // Auto-detect: pick the first column that looks like a timestamp.
        let first_stats = &file_stats[0].1;
        first_stats
            .columns
            .iter()
            .find(|(name, cs)| is_likely_timestamp_column(name, &cs.data_type))
            .map(|(name, _)| name.clone())?
    };

    // Step 2: Bucket each file by YYYY/MM from its min timestamp value.
    let mut partitions: HashMap<String, Vec<String>> = HashMap::new();
    let mut assigned = 0usize;

    for (path, stats) in file_stats {
        if let Some(col_stats) = stats.columns.get(&ts_column) {
            if let Some(ref min_val) = col_stats.min {
                if let Some(key) = timestamp_to_month_key(min_val) {
                    partitions.entry(key).or_default().push(path.clone());
                    assigned += 1;
                    continue;
                }
            }
        }
        // Files without stats go into a catch-all partition.
        partitions
            .entry("unknown".to_string())
            .or_default()
            .push(path.clone());
    }

    // Not enough files with usable timestamps.
    if assigned < file_stats.len() / 2 {
        return None;
    }

    // Step 3: Check distribution skew.
    // If >80% of files land in a single partition, this strategy is useless.
    let max_bucket_size = partitions.values().map(|v| v.len()).max().unwrap_or(0);
    let total = file_stats.len();
    if max_bucket_size as f64 / total as f64 > 0.8 {
        return None;
    }

    Some((ts_column, partitions))
}

/// Convert a `ColumnValue` representing a timestamp into a `YYYY/MM` key.
fn timestamp_to_month_key(value: &ColumnValue) -> Option<String> {
    match value {
        // Int64 timestamps are typically epoch-microseconds or epoch-milliseconds.
        ColumnValue::Int64(v) => {
            // Try microseconds first (common for Parquet TIMESTAMP_MICROS).
            let secs = if v.abs() > 1_000_000_000_000_000 {
                // Looks like microseconds.
                v / 1_000_000
            } else if v.abs() >= 1_000_000_000_000 {
                // Looks like milliseconds.
                v / 1_000
            } else {
                // Already seconds.
                *v
            };
            let dt = chrono::DateTime::from_timestamp(secs, 0)?;
            Some(dt.format("%Y/%m").to_string())
        }
        ColumnValue::String(s) => {
            // Try ISO-8601 date or datetime prefix.
            if s.len() >= 7 {
                // "YYYY-MM" or "YYYY-MM-DD..."
                let year: u32 = s.get(0..4)?.parse().ok()?;
                let month: u32 = s.get(5..7)?.parse().ok()?;
                if (1..=12).contains(&month) && (1970..=2099).contains(&year) {
                    return Some(format!("{}/{:02}", year, month));
                }
            }
            None
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_high_cardinality_detection() {
        assert!(is_high_cardinality_column_name("user_id"));
        assert!(is_high_cardinality_column_name("order_uuid"));
        assert!(is_high_cardinality_column_name("created_at"));
        assert!(is_high_cardinality_column_name("TIMESTAMP"));
        assert!(is_high_cardinality_column_name("email_address"));

        assert!(!is_high_cardinality_column_name("status"));
        assert!(!is_high_cardinality_column_name("country"));
        assert!(!is_high_cardinality_column_name("category"));
    }

    #[test]
    fn test_column_value_conversions() {
        let int_val = ColumnValue::Int64(42);
        assert_eq!(int_val.as_i64(), Some(42));
        assert_eq!(int_val.as_f64(), Some(42.0));
        assert_eq!(int_val.as_string(), None);

        let str_val = ColumnValue::String("test".to_string());
        assert_eq!(str_val.as_string(), Some("test"));
        assert_eq!(str_val.as_i64(), None);
    }

    #[test]
    fn test_merge_min() {
        let a = Some(ColumnValue::Int64(10));
        let b = ColumnValue::Int64(5);
        let result = merge_min(a, b);
        assert!(matches!(result, ColumnValue::Int64(5)));

        let none_case = merge_min(None, ColumnValue::Int64(42));
        assert!(matches!(none_case, ColumnValue::Int64(42)));
    }

    #[test]
    fn test_merge_max() {
        let a = Some(ColumnValue::Int64(10));
        let b = ColumnValue::Int64(5);
        let result = merge_max(a, b);
        assert!(matches!(result, ColumnValue::Int64(10)));

        let b_larger = ColumnValue::Int64(20);
        let result2 = merge_max(Some(ColumnValue::Int64(10)), b_larger);
        assert!(matches!(result2, ColumnValue::Int64(20)));
    }

    #[test]
    fn test_indexing_decision() {
        let mut columns = HashMap::new();
        columns.insert(
            "status".to_string(),
            ColumnStats {
                name: "status".to_string(),
                data_type: ColumnDataType::String,
                min: None,
                max: None,
                null_count: None,
                distinct_count: Some(5),
                row_count: 1000,
            },
        );
        columns.insert(
            "user_id".to_string(),
            ColumnStats {
                name: "user_id".to_string(),
                data_type: ColumnDataType::String,
                min: None,
                max: None,
                null_count: None,
                distinct_count: Some(10000),
                row_count: 1000,
            },
        );
        columns.insert(
            "amount".to_string(),
            ColumnStats {
                name: "amount".to_string(),
                data_type: ColumnDataType::Float64,
                min: Some(ColumnValue::Float64(0.0)),
                max: Some(ColumnValue::Float64(1000.0)),
                null_count: None,
                distinct_count: None,
                row_count: 1000,
            },
        );

        let stats = FileStats {
            file_path: "test.parquet".to_string(),
            row_count: 1000,
            columns,
        };

        let decision = IndexingDecision::from_file_stats(&stats, 10000);

        // status should be FST candidate (low cardinality string)
        assert!(decision.fst_columns.contains(&"status".to_string()));
        // user_id should NOT be FST candidate (high cardinality by name)
        assert!(!decision.fst_columns.contains(&"user_id".to_string()));
        // amount should have numeric stats
        assert!(decision
            .numeric_stats
            .iter()
            .any(|(name, _, _)| name == "amount"));
        // We need full download because we have FST candidates
        assert!(decision.needs_full_download);
    }

    // ========================================================================
    // Timestamp column detection tests
    // ========================================================================

    #[test]
    fn test_is_likely_timestamp_column() {
        assert!(is_likely_timestamp_column(
            "event_ts",
            &ColumnDataType::Int64
        ));
        assert!(is_likely_timestamp_column(
            "created_at",
            &ColumnDataType::Int64
        ));
        assert!(is_likely_timestamp_column(
            "updated_time",
            &ColumnDataType::Int64
        ));
        assert!(is_likely_timestamp_column(
            "ts_col",
            &ColumnDataType::Timestamp
        ));
        assert!(is_likely_timestamp_column(
            "anything",
            &ColumnDataType::Timestamp
        ));

        assert!(!is_likely_timestamp_column(
            "amount",
            &ColumnDataType::Int64
        ));
        assert!(!is_likely_timestamp_column(
            "user_count",
            &ColumnDataType::Int64
        ));
        assert!(!is_likely_timestamp_column(
            "status",
            &ColumnDataType::String
        ));
    }

    #[test]
    fn test_timestamp_to_month_key_micros() {
        // 2024-06-15T00:00:00 UTC in microseconds
        let micros = 1718409600_000_000i64;
        let val = ColumnValue::Int64(micros);
        let key = timestamp_to_month_key(&val);
        assert_eq!(key, Some("2024/06".to_string()));
    }

    #[test]
    fn test_timestamp_to_month_key_millis() {
        // 2024-01-15T12:00:00 UTC in milliseconds
        let millis = 1705320000_000i64;
        let val = ColumnValue::Int64(millis);
        let key = timestamp_to_month_key(&val);
        assert_eq!(key, Some("2024/01".to_string()));
    }

    #[test]
    fn test_timestamp_to_month_key_seconds() {
        // 2023-03-01T00:00:00 UTC in seconds
        let secs = 1677628800i64;
        let val = ColumnValue::Int64(secs);
        let key = timestamp_to_month_key(&val);
        assert_eq!(key, Some("2023/03".to_string()));
    }

    #[test]
    fn test_timestamp_to_month_key_string_iso() {
        let val = ColumnValue::String("2024-09-15T10:30:00Z".to_string());
        let key = timestamp_to_month_key(&val);
        assert_eq!(key, Some("2024/09".to_string()));
    }

    // ========================================================================
    // derive_timestamp_partitions tests
    // ========================================================================

    fn make_file_stats_with_ts(file_path: &str, ts_min: i64, ts_max: i64) -> (String, FileStats) {
        let mut columns = HashMap::new();
        columns.insert(
            "event_ts".to_string(),
            ColumnStats {
                name: "event_ts".to_string(),
                data_type: ColumnDataType::Int64,
                min: Some(ColumnValue::Int64(ts_min)),
                max: Some(ColumnValue::Int64(ts_max)),
                null_count: None,
                distinct_count: None,
                row_count: 1000,
            },
        );
        (
            file_path.to_string(),
            FileStats {
                file_path: file_path.to_string(),
                row_count: 1000,
                columns,
            },
        )
    }

    #[test]
    fn test_derive_timestamp_partitions_non_overlapping() {
        let file_stats = vec![
            // Jan 2024 (epoch seconds)
            make_file_stats_with_ts("file1.parquet", 1704067200, 1704153600),
            // Feb 2024
            make_file_stats_with_ts("file2.parquet", 1706745600, 1706832000),
            // Mar 2024
            make_file_stats_with_ts("file3.parquet", 1709251200, 1709337600),
            // Apr 2024
            make_file_stats_with_ts("file4.parquet", 1711929600, 1712016000),
        ];

        let result = derive_timestamp_partitions(&file_stats, Some("event_ts"));
        assert!(result.is_some(), "Should produce timestamp partitions");

        let (column, partitions) = result.unwrap();
        assert_eq!(column, "event_ts");
        assert!(
            partitions.len() >= 4,
            "Should have at least 4 partitions (one per month)"
        );
    }

    #[test]
    fn test_derive_timestamp_partitions_skewed() {
        // All files in the same month → should be rejected
        let file_stats = vec![
            make_file_stats_with_ts("file1.parquet", 1704067200, 1704153600),
            make_file_stats_with_ts("file2.parquet", 1704067200, 1704153600),
            make_file_stats_with_ts("file3.parquet", 1704067200, 1704153600),
            make_file_stats_with_ts("file4.parquet", 1704067200, 1704153600),
            make_file_stats_with_ts("file5.parquet", 1704067200, 1704153600),
        ];

        let result = derive_timestamp_partitions(&file_stats, Some("event_ts"));
        assert!(result.is_none(), "Skewed data should be rejected");
    }

    #[test]
    fn test_derive_timestamp_partitions_auto_detect_column() {
        let file_stats = vec![
            make_file_stats_with_ts("file1.parquet", 1704067200, 1704153600),
            make_file_stats_with_ts("file2.parquet", 1706745600, 1706832000),
            make_file_stats_with_ts("file3.parquet", 1709251200, 1709337600),
        ];

        // No explicit column name — should auto-detect "event_ts" by suffix
        let result = derive_timestamp_partitions(&file_stats, None);
        assert!(result.is_some(), "Should auto-detect event_ts column");
        assert_eq!(result.unwrap().0, "event_ts");
    }

    #[test]
    fn test_derive_timestamp_partitions_empty() {
        let result = derive_timestamp_partitions(&[], Some("event_ts"));
        assert!(result.is_none());
    }

    #[test]
    fn test_derive_timestamp_partitions_no_ts_column() {
        let mut columns = HashMap::new();
        columns.insert(
            "amount".to_string(),
            ColumnStats {
                name: "amount".to_string(),
                data_type: ColumnDataType::Float64,
                min: Some(ColumnValue::Float64(0.0)),
                max: Some(ColumnValue::Float64(100.0)),
                null_count: None,
                distinct_count: None,
                row_count: 1000,
            },
        );
        let file_stats = vec![(
            "file1.parquet".to_string(),
            FileStats {
                file_path: "file1.parquet".to_string(),
                row_count: 1000,
                columns,
            },
        )];

        let result = derive_timestamp_partitions(&file_stats, None);
        assert!(result.is_none(), "No timestamp column should return None");
    }

    #[test]
    fn test_merge_min_bytes_lexicographic() {
        let a = Some(ColumnValue::Bytes(vec![0x10, 0x20]));
        let b = ColumnValue::Bytes(vec![0x05, 0x30]);
        let result = merge_min(a, b);
        assert!(
            matches!(result, ColumnValue::Bytes(ref v) if v == &[0x05, 0x30]),
            "merge_min should pick the lexicographically smaller Bytes value"
        );

        // Reverse: b > a should keep a
        let a2 = Some(ColumnValue::Bytes(vec![0x05, 0x30]));
        let b2 = ColumnValue::Bytes(vec![0x10, 0x20]);
        let result2 = merge_min(a2, b2);
        assert!(
            matches!(result2, ColumnValue::Bytes(ref v) if v == &[0x05, 0x30]),
            "merge_min should keep the smaller Bytes value"
        );
    }

    #[test]
    fn test_merge_max_bytes_lexicographic() {
        let a = Some(ColumnValue::Bytes(vec![0x05, 0x30]));
        let b = ColumnValue::Bytes(vec![0x10, 0x20]);
        let result = merge_max(a, b);
        assert!(
            matches!(result, ColumnValue::Bytes(ref v) if v == &[0x10, 0x20]),
            "merge_max should pick the lexicographically larger Bytes value"
        );
    }

    #[test]
    fn test_timestamp_millis_boundary() {
        // 1_000_000_000_000 ms = Sept 9, 2001 - should be treated as milliseconds
        let val = ColumnValue::Int64(1_000_000_000_000);
        let result = timestamp_to_month_key(&val);
        assert!(result.is_some(), "Boundary value should parse");
        let key = result.unwrap();
        assert!(
            key.starts_with("2001/"),
            "1_000_000_000_000 should be milliseconds (~2001), got {}",
            key
        );
    }

    #[test]
    fn test_timestamp_seconds_below_boundary() {
        // Just below the millisecond boundary: should be seconds
        let val = ColumnValue::Int64(999_999_999_999);
        let result = timestamp_to_month_key(&val);
        assert!(result.is_some());
        let key = result.unwrap();
        // 999_999_999_999 seconds ~ year 33658 — that's huge, but the code
        // treats it as seconds. The heuristic can't be perfect, but the
        // boundary at 10^12 correctly distinguishes common timestamp ranges.
        assert!(!key.starts_with("2001/"));
    }
}
