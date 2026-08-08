//! Apache Iceberg Table Format Support
//!
//! This module provides minimal support for reading Iceberg table metadata.
//! It parses the JSON metadata files to extract:
//! - List of data files from manifests
//! - Partition information
//! - Column statistics
//!
//! # Iceberg Table Structure
//!
//! ```text
//! table_location/
//! ├── metadata/
//! │   ├── v1.metadata.json
//! │   ├── v2.metadata.json (current)
//! │   ├── snap-123-uuid.avro (snapshot manifest list)
//! │   └── manifest-*.avro (data file manifests)
//! └── data/
//!     └── *.parquet
//! ```

use super::{ColumnStats, DataFileInfo, TableFormatError, TableFormatResult, TableFormatReader};
use crate::warehouse::types::TableFormat;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Column statistics from Iceberg manifest entries.
#[derive(Debug, Clone, Default)]
pub struct IcebergColumnStats {
    /// Number of null values.
    pub null_count: Option<i64>,
    /// Lower bound value.
    pub lower_bound: Option<String>,
    /// Upper bound value.
    pub upper_bound: Option<String>,
}

impl From<IcebergColumnStats> for ColumnStats {
    fn from(stats: IcebergColumnStats) -> Self {
        ColumnStats {
            column_name: String::new(), // Set by caller
            null_count: stats.null_count,
            min_value: stats.lower_bound,
            max_value: stats.upper_bound,
            distinct_count: None,
        }
    }
}

/// Data file information from Iceberg manifest.
#[derive(Debug, Clone)]
pub struct IcebergDataFile {
    /// File path.
    pub file_path: String,
    /// File format (typically "PARQUET").
    pub file_format: String,
    /// File size in bytes.
    pub file_size_in_bytes: i64,
    /// Number of records.
    pub record_count: i64,
    /// Partition data.
    pub partition: HashMap<String, String>,
    /// Column statistics by column ID.
    pub column_stats: HashMap<i32, IcebergColumnStats>,
}

impl From<IcebergDataFile> for DataFileInfo {
    fn from(file: IcebergDataFile) -> Self {
        DataFileInfo {
            path: file.file_path,
            size_bytes: file.file_size_in_bytes,
            record_count: file.record_count,
            partition_values: file.partition,
            column_stats: file
                .column_stats
                .into_iter()
                .map(|(id, stats)| {
                    let mut cs: ColumnStats = stats.into();
                    cs.column_name = format!("column_{}", id);
                    cs
                })
                .collect(),
        }
    }
}

/// Iceberg table metadata (simplified).
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
struct IcebergMetadata {
    /// Metadata format version.
    format_version: i32,
    /// Table UUID.
    #[serde(default)]
    table_uuid: String,
    /// Table location.
    location: String,
    /// Current snapshot ID.
    current_snapshot_id: Option<i64>,
    /// List of snapshots.
    #[serde(default)]
    snapshots: Vec<IcebergSnapshot>,
    /// Partition spec.
    #[serde(default)]
    partition_specs: Vec<IcebergPartitionSpec>,
    /// Schema.
    #[serde(default)]
    schemas: Vec<IcebergSchema>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
struct IcebergSnapshot {
    snapshot_id: i64,
    #[serde(default)]
    manifest_list: String,
    #[serde(default)]
    timestamp_ms: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
struct IcebergPartitionSpec {
    spec_id: i32,
    #[serde(default)]
    fields: Vec<IcebergPartitionField>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
struct IcebergPartitionField {
    source_id: i32,
    field_id: i32,
    name: String,
    transform: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
struct IcebergSchema {
    schema_id: i32,
    #[serde(default)]
    fields: Vec<IcebergField>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
struct IcebergField {
    id: i32,
    name: String,
    #[serde(rename = "type")]
    field_type: serde_json::Value,
    required: bool,
}

/// Reader for Apache Iceberg table metadata.
///
/// This provides access to table metadata without requiring the full
/// Iceberg Rust SDK. It parses JSON metadata files directly.
#[derive(Debug)]
pub struct IcebergTableReader {
    /// Parsed metadata.
    metadata: IcebergMetadata,
}

impl IcebergTableReader {
    /// Create a new Iceberg table reader.
    ///
    /// This reads the latest metadata file from the table location.
    pub async fn new(table_location: &str) -> TableFormatResult<Self> {
        let metadata = Self::load_metadata(table_location).await?;

        Ok(Self { metadata })
    }

    /// Load metadata from the table location.
    async fn load_metadata(table_location: &str) -> TableFormatResult<IcebergMetadata> {
        let metadata_dir = if table_location.ends_with('/') {
            format!("{}metadata", table_location)
        } else {
            format!("{}/metadata", table_location)
        };

        // Find the latest metadata file
        let metadata_path = Self::find_latest_metadata(&metadata_dir)?;
        
        // Read and parse the metadata
        let content = tokio::fs::read_to_string(&metadata_path)
            .await
            .map_err(|e| TableFormatError::Io(e))?;

        serde_json::from_str(&content).map_err(|e| TableFormatError::Json(e))
    }

    /// Find the latest metadata.json file in the metadata directory.
    fn find_latest_metadata(metadata_dir: &str) -> TableFormatResult<String> {
        let path = Path::new(metadata_dir);
        
        if !path.is_dir() {
            return Err(TableFormatError::MissingFile(format!(
                "Metadata directory not found: {}",
                metadata_dir
            )));
        }

        let mut latest_version = -1i64;
        let mut latest_file = None;

        for entry in std::fs::read_dir(path).map_err(TableFormatError::Io)? {
            let entry = entry.map_err(TableFormatError::Io)?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            if name_str.ends_with(".metadata.json") {
                // Extract version number (e.g., "v2.metadata.json" -> 2)
                if let Some(version_str) = name_str.strip_suffix(".metadata.json") {
                    let version = version_str
                        .strip_prefix('v')
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(-1);

                    if version > latest_version {
                        latest_version = version;
                        latest_file = Some(entry.path().to_string_lossy().to_string());
                    }
                }
            }
        }

        latest_file.ok_or_else(|| {
            TableFormatError::MissingFile("No metadata.json files found".to_string())
        })
    }

    /// Get the current snapshot ID.
    pub fn current_snapshot_id(&self) -> Option<i64> {
        self.metadata.current_snapshot_id
    }

    /// Get the current snapshot.
    pub fn current_snapshot(&self) -> Option<&IcebergSnapshot> {
        let snapshot_id = self.metadata.current_snapshot_id?;
        self.metadata
            .snapshots
            .iter()
            .find(|s| s.snapshot_id == snapshot_id)
    }

    /// Get partition column names.
    pub fn partition_columns(&self) -> Vec<String> {
        self.metadata
            .partition_specs
            .first()
            .map(|spec| {
                spec.fields
                    .iter()
                    .map(|f| f.name.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get the manifest list path for the current snapshot.
    pub fn manifest_list_path(&self) -> Option<String> {
        self.current_snapshot().map(|s| s.manifest_list.clone())
    }

    /// List data files from the current snapshot.
    ///
    /// Note: Avro manifest parsing is not yet implemented. This requires the
    /// `apache-avro` crate to read manifest-list and manifest files. Use raw
    /// Parquet mode for now -- Pond queries the Parquet files directly via
    /// ClickHouse's s3() function without needing manifest metadata.
    pub async fn list_data_files_impl(&self) -> TableFormatResult<Vec<IcebergDataFile>> {
        Err(TableFormatError::Unsupported(
            "Iceberg manifest parsing is not yet supported. \
             Use raw Parquet format instead -- Pond can query Parquet files directly \
             without Iceberg metadata.".to_string(),
        ))
    }
}

#[async_trait::async_trait]
impl TableFormatReader for IcebergTableReader {
    fn format(&self) -> TableFormat {
        TableFormat::Iceberg
    }

    async fn list_data_files(&self) -> TableFormatResult<Vec<DataFileInfo>> {
        let files = self.list_data_files_impl().await?;
        Ok(files.into_iter().map(DataFileInfo::from).collect())
    }

    async fn get_partition_columns(&self) -> TableFormatResult<Vec<String>> {
        Ok(self.partition_columns())
    }

    async fn current_version(&self) -> TableFormatResult<String> {
        self.current_snapshot_id()
            .map(|id| id.to_string())
            .ok_or_else(|| TableFormatError::InvalidMetadata("No current snapshot".to_string()))
    }

    async fn is_modified_since(&self, version: &str) -> TableFormatResult<bool> {
        let current = self.current_version().await?;
        Ok(current != version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn create_test_metadata(table_path: &Path) -> String {
        let metadata_dir = table_path.join("metadata");
        fs::create_dir_all(&metadata_dir).unwrap();

        let metadata = r#"{
            "format-version": 2,
            "table-uuid": "test-uuid",
            "location": "/test/table",
            "current-snapshot-id": 123,
            "snapshots": [
                {
                    "snapshot-id": 123,
                    "manifest-list": "snap-123-uuid.avro",
                    "timestamp-ms": 1700000000000
                }
            ],
            "partition-specs": [
                {
                    "spec-id": 0,
                    "fields": [
                        {
                            "source-id": 1,
                            "field-id": 1000,
                            "name": "date",
                            "transform": "day"
                        }
                    ]
                }
            ],
            "schemas": [
                {
                    "schema-id": 0,
                    "fields": [
                        {
                            "id": 1,
                            "name": "timestamp",
                            "type": "timestamp",
                            "required": true
                        },
                        {
                            "id": 2,
                            "name": "user_id",
                            "type": "string",
                            "required": false
                        }
                    ]
                }
            ]
        }"#;

        let metadata_file = metadata_dir.join("v1.metadata.json");
        fs::write(&metadata_file, metadata).unwrap();
        
        table_path.to_string_lossy().to_string()
    }

    #[tokio::test]
    async fn test_iceberg_reader_creation() {
        let temp_dir = TempDir::new().unwrap();
        let table_path = create_test_metadata(temp_dir.path());

        let reader = IcebergTableReader::new(&table_path).await.unwrap();
        
        assert_eq!(reader.current_snapshot_id(), Some(123));
        assert_eq!(reader.partition_columns(), vec!["date".to_string()]);
    }

    #[tokio::test]
    async fn test_iceberg_version() {
        let temp_dir = TempDir::new().unwrap();
        let table_path = create_test_metadata(temp_dir.path());

        let reader = IcebergTableReader::new(&table_path).await.unwrap();
        
        let version = reader.current_version().await.unwrap();
        assert_eq!(version, "123");
    }

    #[test]
    fn test_find_latest_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let metadata_dir = temp_dir.path().join("metadata");
        fs::create_dir_all(&metadata_dir).unwrap();

        // Create multiple versions
        fs::write(metadata_dir.join("v1.metadata.json"), "{}").unwrap();
        fs::write(metadata_dir.join("v2.metadata.json"), "{}").unwrap();
        fs::write(metadata_dir.join("v3.metadata.json"), "{}").unwrap();

        let latest = IcebergTableReader::find_latest_metadata(
            metadata_dir.to_str().unwrap()
        ).unwrap();

        assert!(latest.contains("v3.metadata.json"));
    }
}
