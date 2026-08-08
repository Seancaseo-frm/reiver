//! Table Format Detection
//!
//! Automatically detects whether a given path contains an Iceberg or Delta Lake table
//! by checking for characteristic directories and files.

use crate::warehouse::types::TableFormat;
use std::path::Path;

/// Detects table format from a given table location.
///
/// # Detection Logic
///
/// 1. **Iceberg**: Check for `metadata/` directory containing `.json` metadata files
/// 2. **Delta Lake**: Check for `_delta_log/` directory containing transaction logs
/// 3. **Raw Parquet**: Neither format detected, assume plain Parquet files
///
/// # Arguments
/// * `table_location` - Path to the table root (local or S3/GCS URL)
///
/// # Returns
/// The detected `TableFormat`.
pub async fn detect_table_format(table_location: &str) -> TableFormat {
    let detector = TableFormatDetector::new(table_location);
    detector.detect().await
}

/// Table format detector with configurable detection logic.
#[derive(Debug, Clone)]
pub struct TableFormatDetector {
    /// Table location (local path or object store URL).
    location: String,
}

impl TableFormatDetector {
    /// Create a new detector for the given table location.
    pub fn new(location: impl Into<String>) -> Self {
        Self {
            location: location.into(),
        }
    }

    /// Detect the table format.
    pub async fn detect(&self) -> TableFormat {
        // Check for Iceberg first (more specific)
        if self.is_iceberg().await {
            return TableFormat::Iceberg;
        }

        // Check for Delta Lake
        if self.is_delta().await {
            return TableFormat::DeltaLake;
        }

        // Default to raw Parquet
        TableFormat::RawParquet
    }

    /// Check if the location contains an Iceberg table.
    ///
    /// Iceberg tables have a `metadata/` directory containing:
    /// - `version-hint.text` (optional)
    /// - `v*.metadata.json` or `*.metadata.json` files
    async fn is_iceberg(&self) -> bool {
        let metadata_path = self.join_path("metadata");
        
        // For local paths, check directory existence
        if !self.location.starts_with("s3://") 
            && !self.location.starts_with("gs://") 
            && !self.location.starts_with("az://") 
        {
            let path = Path::new(&metadata_path);
            if path.is_dir() {
                // Check for metadata.json files
                if let Ok(entries) = std::fs::read_dir(path) {
                    for entry in entries.flatten() {
                        let name = entry.file_name();
                        let name_str = name.to_string_lossy();
                        if name_str.ends_with(".metadata.json") {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        // For object store paths, we would need to list the bucket
        // This is a placeholder - in production, use object_store crate
        self.check_object_store_path_exists(&metadata_path).await
    }

    /// Check if the location contains a Delta Lake table.
    ///
    /// Delta Lake tables have a `_delta_log/` directory containing:
    /// - `00000000000000000000.json` (and subsequent numbered files)
    /// - `_last_checkpoint` (optional)
    async fn is_delta(&self) -> bool {
        let delta_log_path = self.join_path("_delta_log");
        
        // For local paths, check directory existence
        if !self.location.starts_with("s3://") 
            && !self.location.starts_with("gs://") 
            && !self.location.starts_with("az://") 
        {
            let path = Path::new(&delta_log_path);
            if path.is_dir() {
                // Check for numbered JSON files
                if let Ok(entries) = std::fs::read_dir(path) {
                    for entry in entries.flatten() {
                        let name = entry.file_name();
                        let name_str = name.to_string_lossy();
                        if name_str.ends_with(".json") 
                            && name_str.chars().take_while(|c| c.is_ascii_digit()).count() > 0 
                        {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        // For object store paths, we would need to list the bucket
        self.check_object_store_path_exists(&delta_log_path).await
    }

    /// Join a path segment to the table location.
    fn join_path(&self, segment: &str) -> String {
        if self.location.ends_with('/') {
            format!("{}{}", self.location, segment)
        } else {
            format!("{}/{}", self.location, segment)
        }
    }

    /// Check if a path exists in object storage.
    ///
    /// Object store format detection is not yet implemented. For cloud-stored
    /// tables, users should explicitly set the table format in their source
    /// configuration. Auto-detection works for local paths only.
    async fn check_object_store_path_exists(&self, _path: &str) -> bool {
        tracing::debug!(
            location = %self.location,
            "Table format auto-detection is not supported for object store paths; \
             defaulting to RawParquet. Set the table format explicitly in source configuration."
        );
        false
    }
}

/// Synchronous detection for local paths only.
///
/// This is useful for testing and CLI tools where async is not needed.
pub fn detect_table_format_sync(table_location: &str) -> TableFormat {
    // Only works for local paths
    if table_location.starts_with("s3://") 
        || table_location.starts_with("gs://") 
        || table_location.starts_with("az://") 
    {
        return TableFormat::RawParquet; // Can't detect without async
    }

    let metadata_path = if table_location.ends_with('/') {
        format!("{}metadata", table_location)
    } else {
        format!("{}/metadata", table_location)
    };

    let delta_log_path = if table_location.ends_with('/') {
        format!("{}_delta_log", table_location)
    } else {
        format!("{}/_delta_log", table_location)
    };

    // Check for Iceberg
    let metadata_dir = Path::new(&metadata_path);
    if metadata_dir.is_dir() {
        if let Ok(entries) = std::fs::read_dir(metadata_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if name_str.ends_with(".metadata.json") {
                    return TableFormat::Iceberg;
                }
            }
        }
    }

    // Check for Delta Lake
    let delta_dir = Path::new(&delta_log_path);
    if delta_dir.is_dir() {
        if let Ok(entries) = std::fs::read_dir(delta_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if name_str.ends_with(".json")
                    && name_str.chars().take_while(|c| c.is_ascii_digit()).count() > 0
                {
                    return TableFormat::DeltaLake;
                }
            }
        }
    }

    TableFormat::RawParquet
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_detect_iceberg_table() {
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path();

        // Create Iceberg metadata directory
        let metadata_dir = table_path.join("metadata");
        fs::create_dir(&metadata_dir).unwrap();

        // Create a metadata file
        fs::write(
            metadata_dir.join("v1.metadata.json"),
            r#"{"format-version": 2}"#,
        )
        .unwrap();

        let format = detect_table_format_sync(table_path.to_str().unwrap());
        assert_eq!(format, TableFormat::Iceberg);
    }

    #[test]
    fn test_detect_delta_table() {
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path();

        // Create Delta log directory
        let delta_log_dir = table_path.join("_delta_log");
        fs::create_dir(&delta_log_dir).unwrap();

        // Create a transaction log file
        fs::write(
            delta_log_dir.join("00000000000000000000.json"),
            r#"{"add": {"path": "file.parquet"}}"#,
        )
        .unwrap();

        let format = detect_table_format_sync(table_path.to_str().unwrap());
        assert_eq!(format, TableFormat::DeltaLake);
    }

    #[test]
    fn test_detect_raw_parquet() {
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path();

        // Just create a parquet file, no table format
        fs::write(table_path.join("data.parquet"), b"parquet data").unwrap();

        let format = detect_table_format_sync(table_path.to_str().unwrap());
        assert_eq!(format, TableFormat::RawParquet);
    }

    #[test]
    fn test_iceberg_priority_over_delta() {
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path();

        // Create both directories (unlikely in practice but tests priority)
        let metadata_dir = table_path.join("metadata");
        fs::create_dir(&metadata_dir).unwrap();
        fs::write(
            metadata_dir.join("v1.metadata.json"),
            r#"{"format-version": 2}"#,
        )
        .unwrap();

        let delta_log_dir = table_path.join("_delta_log");
        fs::create_dir(&delta_log_dir).unwrap();
        fs::write(
            delta_log_dir.join("00000000000000000000.json"),
            r#"{"add": {"path": "file.parquet"}}"#,
        )
        .unwrap();

        // Iceberg should be detected first
        let format = detect_table_format_sync(table_path.to_str().unwrap());
        assert_eq!(format, TableFormat::Iceberg);
    }
}
