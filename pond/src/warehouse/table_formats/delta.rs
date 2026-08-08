//! Delta Lake Table Format Support
//!
//! This module provides minimal support for reading Delta Lake table metadata.
//! It parses the JSON transaction log files to extract:
//! - List of active data files
//! - Partition information
//! - Column statistics
//!
//! # Delta Lake Table Structure
//!
//! ```text
//! table_location/
//! ├── _delta_log/
//! │   ├── 00000000000000000000.json
//! │   ├── 00000000000000000001.json
//! │   ├── ...
//! │   └── _last_checkpoint (optional)
//! └── *.parquet
//! ```

use super::{ColumnStats, DataFileInfo, TableFormatError, TableFormatResult, TableFormatReader};
use crate::warehouse::types::TableFormat;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;


/// Maximum number of commit files to replay when loading Delta table state.
/// Tables with more commits should use checkpoints for efficient loading.
/// If exceeded, only the most recent MAX_COMMITS_TO_REPLAY commits are loaded
/// and a warning is logged.
const MAX_COMMITS_TO_REPLAY: usize = 10_000;

/// Column statistics from Delta Lake add actions.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeltaColumnStats {
    /// Number of records.
    #[serde(default)]
    pub num_records: i64,
    /// Minimum values by column name.
    #[serde(default)]
    pub min_values: HashMap<String, serde_json::Value>,
    /// Maximum values by column name.
    #[serde(default)]
    pub max_values: HashMap<String, serde_json::Value>,
    /// Null counts by column name.
    #[serde(default)]
    pub null_count: HashMap<String, i64>,
}

impl DeltaColumnStats {
    /// Convert to a list of ColumnStats.
    pub fn to_column_stats(&self) -> Vec<ColumnStats> {
        let mut columns: HashMap<String, ColumnStats> = HashMap::new();

        // Process min values
        for (name, value) in &self.min_values {
            let cs = columns.entry(name.clone()).or_insert_with(|| ColumnStats {
                column_name: name.clone(),
                null_count: None,
                min_value: None,
                max_value: None,
                distinct_count: None,
            });
            cs.min_value = Some(value.to_string());
        }

        // Process max values
        for (name, value) in &self.max_values {
            let cs = columns.entry(name.clone()).or_insert_with(|| ColumnStats {
                column_name: name.clone(),
                null_count: None,
                min_value: None,
                max_value: None,
                distinct_count: None,
            });
            cs.max_value = Some(value.to_string());
        }

        // Process null counts
        for (name, count) in &self.null_count {
            let cs = columns.entry(name.clone()).or_insert_with(|| ColumnStats {
                column_name: name.clone(),
                null_count: None,
                min_value: None,
                max_value: None,
                distinct_count: None,
            });
            cs.null_count = Some(*count);
        }

        columns.into_values().collect()
    }
}

/// Delta Lake add action (file addition).
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeltaAddAction {
    /// File path (relative to table root).
    pub path: String,
    /// Partition values.
    #[serde(default)]
    pub partition_values: HashMap<String, String>,
    /// File size in bytes.
    #[serde(default)]
    pub size: i64,
    /// Modification timestamp.
    #[serde(default)]
    pub modification_time: i64,
    /// Whether data change.
    #[serde(default)]
    pub data_change: bool,
    /// Statistics as JSON string.
    #[serde(default)]
    pub stats: Option<String>,
}

impl DeltaAddAction {
    /// Parse the stats JSON string.
    pub fn parse_stats(&self) -> Option<DeltaColumnStats> {
        self.stats
            .as_ref()
            .and_then(|s| serde_json::from_str(s).ok())
    }
}

impl From<DeltaAddAction> for DataFileInfo {
    fn from(action: DeltaAddAction) -> Self {
        let stats = action.parse_stats();
        let record_count = stats.as_ref().map(|s| s.num_records).unwrap_or(0);
        let column_stats = stats.map(|s| s.to_column_stats()).unwrap_or_default();

        DataFileInfo {
            path: action.path,
            size_bytes: action.size,
            record_count,
            partition_values: action.partition_values,
            column_stats,
        }
    }
}

/// Delta Lake remove action (file deletion).
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct DeltaRemoveAction {
    /// File path to remove.
    path: String,
    /// Deletion timestamp.
    #[serde(default)]
    deletion_timestamp: Option<i64>,
    /// Whether data change.
    #[serde(default)]
    data_change: bool,
}

/// Delta Lake transaction log action.
#[derive(Debug, Clone, Deserialize, Serialize)]
struct DeltaAction {
    /// Add action.
    add: Option<DeltaAddAction>,
    /// Remove action.
    remove: Option<DeltaRemoveAction>,
    /// Metadata action (ignored for now).
    #[serde(rename = "metaData")]
    metadata: Option<serde_json::Value>,
    /// Protocol action (ignored for now).
    protocol: Option<serde_json::Value>,
    /// Commit info (ignored for now).
    #[serde(rename = "commitInfo")]
    commit_info: Option<serde_json::Value>,
}

/// Delta Lake checkpoint information.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize, Serialize)]
struct DeltaCheckpoint {
    /// Version number.
    version: i64,
    /// Size (number of actions).
    size: i64,
}

/// Reader for Delta Lake transaction logs.
///
/// This reads the transaction log to determine the current state of the table
/// (which files are active, their statistics, etc.).
#[derive(Debug)]
pub struct DeltaTableReader {
    /// Current version (latest commit).
    current_version: i64,
    /// Active files (after applying all add/remove actions).
    active_files: HashMap<String, DeltaAddAction>,
}

impl DeltaTableReader {
    /// Create a new Delta table reader.
    ///
    /// This reads all transaction logs to build the current table state.
    pub async fn new(table_location: &str) -> TableFormatResult<Self> {
        let delta_log_path = if table_location.ends_with('/') {
            format!("{}_delta_log", table_location)
        } else {
            format!("{}/_delta_log", table_location)
        };

        let (current_version, active_files) = Self::load_state(&delta_log_path).await?;

        Ok(Self {
            current_version,
            active_files,
        })
    }

    /// Load the table state by replaying the transaction log.
    async fn load_state(
        delta_log_path: &str,
    ) -> TableFormatResult<(i64, HashMap<String, DeltaAddAction>)> {
        let path = Path::new(delta_log_path);

        if !path.is_dir() {
            return Err(TableFormatError::MissingFile(format!(
                "Delta log directory not found: {}",
                delta_log_path
            )));
        }

        // Find all JSON commit files and sort by version
        let mut commit_files: Vec<(i64, String)> = Vec::new();

        for entry in std::fs::read_dir(path).map_err(TableFormatError::Io)? {
            let entry = entry.map_err(TableFormatError::Io)?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            if name_str.ends_with(".json") {
                // Parse version number (e.g., "00000000000000000001.json" -> 1)
                if let Ok(version) = name_str
                    .strip_suffix(".json")
                    .unwrap_or("")
                    .parse::<i64>()
                {
                    commit_files.push((version, entry.path().to_string_lossy().to_string()));
                }
            }
        }

        if commit_files.is_empty() {
            return Err(TableFormatError::MissingFile(
                "No commit files found in Delta log".to_string(),
            ));
        }

        // Sort by version
        commit_files.sort_by_key(|(v, _)| *v);

        let total_commits = commit_files.len();
        if total_commits > MAX_COMMITS_TO_REPLAY {
            return Err(TableFormatError::Unsupported(format!(
                "Delta table has {} commit files, exceeding the replay limit of {}. \
                 Use Delta checkpoints (_last_checkpoint) for tables with large transaction logs.",
                total_commits, MAX_COMMITS_TO_REPLAY
            )));
        }

        // Replay commits
        let mut active_files: HashMap<String, DeltaAddAction> = HashMap::new();
        let mut current_version = 0i64;

        for (version, file_path) in commit_files {
            current_version = version;
            
            let content = tokio::fs::read_to_string(&file_path)
                .await
                .map_err(TableFormatError::Io)?;

            // Each line is a JSON action
            for line in content.lines() {
                if line.trim().is_empty() {
                    continue;
                }

                let action: DeltaAction = serde_json::from_str(line)
                    .map_err(TableFormatError::Json)?;

                if let Some(add) = action.add {
                    active_files.insert(add.path.clone(), add);
                }

                if let Some(remove) = action.remove {
                    active_files.remove(&remove.path);
                }
            }
        }

        Ok((current_version, active_files))
    }

    /// Get the current version number.
    pub fn version(&self) -> i64 {
        self.current_version
    }

    /// Get all active files.
    pub fn active_files(&self) -> &HashMap<String, DeltaAddAction> {
        &self.active_files
    }

    /// List all active data files.
    pub fn list_active_files(&self) -> Vec<DeltaAddAction> {
        self.active_files.values().cloned().collect()
    }

    /// Get partition columns from the first file's partition values.
    pub fn infer_partition_columns(&self) -> Vec<String> {
        self.active_files
            .values()
            .next()
            .map(|f| f.partition_values.keys().cloned().collect())
            .unwrap_or_default()
    }
}

#[async_trait::async_trait]
impl TableFormatReader for DeltaTableReader {
    fn format(&self) -> TableFormat {
        TableFormat::DeltaLake
    }

    async fn list_data_files(&self) -> TableFormatResult<Vec<DataFileInfo>> {
        Ok(self
            .list_active_files()
            .into_iter()
            .map(DataFileInfo::from)
            .collect())
    }

    async fn get_partition_columns(&self) -> TableFormatResult<Vec<String>> {
        Ok(self.infer_partition_columns())
    }

    async fn current_version(&self) -> TableFormatResult<String> {
        Ok(self.current_version.to_string())
    }

    async fn is_modified_since(&self, version: &str) -> TableFormatResult<bool> {
        let current = self.current_version.to_string();
        Ok(current != version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn create_test_delta_table(table_path: &Path) -> String {
        let delta_log = table_path.join("_delta_log");
        fs::create_dir_all(&delta_log).unwrap();

        // First commit: add two files
        let commit_0 = r#"{"add":{"path":"part-00000.parquet","partitionValues":{"date":"2024-01-01"},"size":1024,"modificationTime":1700000000000,"dataChange":true,"stats":"{\"numRecords\":100}"}}
{"add":{"path":"part-00001.parquet","partitionValues":{"date":"2024-01-01"},"size":2048,"modificationTime":1700000000000,"dataChange":true}}"#;
        fs::write(
            delta_log.join("00000000000000000000.json"),
            commit_0,
        )
        .unwrap();

        // Second commit: add one file, remove one
        let commit_1 = r#"{"add":{"path":"part-00002.parquet","partitionValues":{"date":"2024-01-02"},"size":3072,"modificationTime":1700100000000,"dataChange":true}}
{"remove":{"path":"part-00000.parquet","deletionTimestamp":1700100000000,"dataChange":true}}"#;
        fs::write(
            delta_log.join("00000000000000000001.json"),
            commit_1,
        )
        .unwrap();

        table_path.to_string_lossy().to_string()
    }

    #[tokio::test]
    async fn test_delta_reader_creation() {
        let temp_dir = TempDir::new().unwrap();
        let table_path = create_test_delta_table(temp_dir.path());

        let reader = DeltaTableReader::new(&table_path).await.unwrap();

        assert_eq!(reader.version(), 1);
        assert_eq!(reader.active_files().len(), 2); // part-00001 and part-00002
    }

    #[tokio::test]
    async fn test_delta_file_removal() {
        let temp_dir = TempDir::new().unwrap();
        let table_path = create_test_delta_table(temp_dir.path());

        let reader = DeltaTableReader::new(&table_path).await.unwrap();
        let files = reader.list_active_files();

        // part-00000 was removed in commit 1
        assert!(!files.iter().any(|f| f.path == "part-00000.parquet"));
        assert!(files.iter().any(|f| f.path == "part-00001.parquet"));
        assert!(files.iter().any(|f| f.path == "part-00002.parquet"));
    }

    #[tokio::test]
    async fn test_delta_partition_inference() {
        let temp_dir = TempDir::new().unwrap();
        let table_path = create_test_delta_table(temp_dir.path());

        let reader = DeltaTableReader::new(&table_path).await.unwrap();
        let partitions = reader.infer_partition_columns();

        assert_eq!(partitions, vec!["date".to_string()]);
    }

    #[tokio::test]
    async fn test_delta_stats_parsing() {
        let temp_dir = TempDir::new().unwrap();
        let delta_log = temp_dir.path().join("_delta_log");
        fs::create_dir_all(&delta_log).unwrap();

        let stats_json = r#"{"numRecords":100,"minValues":{"id":1,"name":"alice"},"maxValues":{"id":100,"name":"zoe"},"nullCount":{"id":0,"name":5}}"#;
        let commit = format!(
            r#"{{"add":{{"path":"data.parquet","partitionValues":{{}},"size":1024,"modificationTime":1700000000000,"dataChange":true,"stats":{:?}}}}}"#,
            stats_json
        );
        fs::write(delta_log.join("00000000000000000000.json"), commit).unwrap();

        let reader = DeltaTableReader::new(temp_dir.path().to_str().unwrap())
            .await
            .unwrap();

        let files = reader.list_active_files();
        assert_eq!(files.len(), 1);

        let stats = files[0].parse_stats().unwrap();
        assert_eq!(stats.num_records, 100);
    }

    #[test]
    fn test_delta_column_stats_conversion() {
        let stats = DeltaColumnStats {
            num_records: 100,
            min_values: [("id".to_string(), serde_json::json!(1))].into_iter().collect(),
            max_values: [("id".to_string(), serde_json::json!(100))].into_iter().collect(),
            null_count: [("id".to_string(), 0)].into_iter().collect(),
        };

        let column_stats = stats.to_column_stats();
        assert_eq!(column_stats.len(), 1);
        
        let id_stats = column_stats.iter().find(|s| s.column_name == "id").unwrap();
        assert_eq!(id_stats.min_value, Some("1".to_string()));
        assert_eq!(id_stats.max_value, Some("100".to_string()));
        assert_eq!(id_stats.null_count, Some(0));
    }

    #[tokio::test]
    async fn test_delta_excessive_commits_returns_error() {
        let temp_dir = TempDir::new().unwrap();
        let delta_log = temp_dir.path().join("_delta_log");
        fs::create_dir_all(&delta_log).unwrap();

        // Create MAX_COMMITS_TO_REPLAY + 1 commit files
        for i in 0..=(MAX_COMMITS_TO_REPLAY as i64) {
            let commit = r#"{"add":{"path":"data.parquet","partitionValues":{},"size":1024,"modificationTime":1700000000000,"dataChange":true}}"#;
            fs::write(
                delta_log.join(format!("{:020}.json", i)),
                commit,
            )
            .unwrap();
        }

        let result = DeltaTableReader::new(temp_dir.path().to_str().unwrap()).await;
        assert!(result.is_err(), "should return error when commits exceed limit");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("exceeding the replay limit"),
            "error message should mention replay limit, got: {}",
            err
        );
    }
}
