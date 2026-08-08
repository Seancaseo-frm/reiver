//! Table Format Support for External Data Sources
//!
//! This module provides support for reading metadata from common open table formats:
//! - **Apache Iceberg**: Uses `metadata/` directory with manifest-list and manifest files
//! - **Delta Lake**: Uses `_delta_log/` directory with JSON transaction logs
//!
//! When customers already use these formats, reiver leverages their manifests
//! instead of building custom FST/Xor indexes. This provides:
//! - File list without bucket listing
//! - Column statistics without sampling
//! - Partition info for pruning
//! - Consistent file views (no partial reads during updates)

pub mod detector;
pub mod delta;
pub mod iceberg;

pub use detector::{detect_table_format, TableFormatDetector};
pub use delta::{DeltaTableReader, DeltaAddAction, DeltaColumnStats};
pub use iceberg::{IcebergTableReader, IcebergDataFile, IcebergColumnStats};

use crate::warehouse::types::TableFormat;
use thiserror::Error;

/// Errors that can occur during table format operations.
#[derive(Debug, Error)]
pub enum TableFormatError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON parsing error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Invalid metadata: {0}")]
    InvalidMetadata(String),

    #[error("Unsupported table format version: {0}")]
    UnsupportedVersion(String),

    #[error("Missing required file: {0}")]
    MissingFile(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Unsupported: {0}")]
    Unsupported(String),
}

/// Result type for table format operations.
pub type TableFormatResult<T> = Result<T, TableFormatError>;

/// Column statistics from a table format.
///
/// These statistics are used for file pruning and query optimization.
#[derive(Debug, Clone)]
pub struct ColumnStats {
    /// Column name.
    pub column_name: String,
    /// Number of null values.
    pub null_count: Option<i64>,
    /// Minimum value (as string for comparability).
    pub min_value: Option<String>,
    /// Maximum value (as string for comparability).
    pub max_value: Option<String>,
    /// Number of distinct values (if available).
    pub distinct_count: Option<i64>,
}

/// File information from a table format.
///
/// This provides a unified view of file metadata regardless of whether
/// it comes from Iceberg manifests or Delta transaction logs.
#[derive(Debug, Clone)]
pub struct DataFileInfo {
    /// File path (relative to table root or absolute).
    pub path: String,
    /// File size in bytes.
    pub size_bytes: i64,
    /// Number of records in the file.
    pub record_count: i64,
    /// Partition values (if partitioned).
    pub partition_values: std::collections::HashMap<String, String>,
    /// Column statistics (if available).
    pub column_stats: Vec<ColumnStats>,
}

/// Unified trait for reading table format metadata.
///
/// This trait provides a common interface for both Iceberg and Delta Lake,
/// allowing reiver to work with either format transparently.
#[async_trait::async_trait]
pub trait TableFormatReader: Send + Sync {
    /// Get the table format type.
    fn format(&self) -> TableFormat;

    /// List all data files in the current table snapshot.
    async fn list_data_files(&self) -> TableFormatResult<Vec<DataFileInfo>>;

    /// Get the partition columns for this table.
    async fn get_partition_columns(&self) -> TableFormatResult<Vec<String>>;

    /// Get the current snapshot/version ID.
    async fn current_version(&self) -> TableFormatResult<String>;

    /// Check if the table has been modified since the given version.
    async fn is_modified_since(&self, version: &str) -> TableFormatResult<bool>;
}

/// Create a table format reader for the detected format.
pub async fn create_reader(
    format: TableFormat,
    table_location: &str,
) -> TableFormatResult<Box<dyn TableFormatReader>> {
    match format {
        TableFormat::Iceberg => {
            let reader = IcebergTableReader::new(table_location).await?;
            Ok(Box::new(reader))
        }
        TableFormat::DeltaLake => {
            let reader = DeltaTableReader::new(table_location).await?;
            Ok(Box::new(reader))
        }
        TableFormat::RawParquet | TableFormat::Auto => {
            Err(TableFormatError::InvalidMetadata(
                "Cannot create reader for RawParquet or Auto format".to_string(),
            ))
        }
    }
}
