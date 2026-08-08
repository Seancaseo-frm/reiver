//! Metadata Sampler
//!
//! Samples Parquet file metadata to build a DataProfile for AI configuration generation.

use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::HashMap;
use thiserror::Error;
use uuid::Uuid;

use crate::warehouse::types::ColumnType;

use super::types::{ColumnProfile, DataProfile};

// ============================================================================
// Static Regex
// ============================================================================

/// Regex for parsing key=value partition patterns in file paths.
/// Compiled once at startup for performance.
static PARTITION_KV_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"([a-zA-Z_]+)=([^/]+)").expect("Invalid partition regex pattern"));

// ============================================================================
// Errors
// ============================================================================

/// Errors that can occur during metadata sampling.
#[derive(Debug, Error)]
pub enum SamplerError {
    #[error("No files found to sample")]
    NoFiles,

    #[error("Failed to read metadata: {0}")]
    MetadataError(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Result type for sampler operations.
pub type SamplerResult<T> = Result<T, SamplerError>;

// ============================================================================
// Constants
// ============================================================================

/// Default number of files to sample.
const DEFAULT_SAMPLE_COUNT: usize = 10;

// ============================================================================
// Metadata Sampler
// ============================================================================

/// Samples Parquet file metadata to build data profiles.
pub struct MetadataSampler {
    /// Maximum number of files to sample.
    max_files: usize,
}

impl MetadataSampler {
    /// Create a new metadata sampler.
    pub fn new() -> Self {
        Self {
            max_files: DEFAULT_SAMPLE_COUNT,
        }
    }

    /// Create a sampler with a custom sample count.
    pub fn with_max_files(max_files: usize) -> Self {
        Self { max_files }
    }

    /// Build a DataProfile from file metadata.
    ///
    /// This method takes pre-collected column statistics and file paths,
    /// and builds a comprehensive DataProfile.
    pub fn build_profile(
        &self,
        source_id: Uuid,
        column_stats: Vec<ColumnStats>,
        file_paths: Vec<String>,
    ) -> SamplerResult<DataProfile> {
        if file_paths.is_empty() {
            return Err(SamplerError::NoFiles);
        }

        let mut profile = DataProfile::new(source_id);
        profile.file_count = file_paths.len();
        profile.sample_file_paths = file_paths.iter().take(self.max_files).cloned().collect();

        // Get estimated row count from first column before moving column_stats
        let row_count_estimate = column_stats.first().and_then(|s| s.row_count).unwrap_or(0);

        // Build column profiles
        for stats in column_stats {
            let mut col_profile = ColumnProfile::new(&stats.name, stats.data_type);
            col_profile.estimated_cardinality = stats.distinct_count;
            col_profile.null_ratio = stats.null_ratio;
            col_profile.min_value = stats.min_value;
            col_profile.max_value = stats.max_value;
            col_profile.sample_values = stats.sample_values;
            col_profile.avg_value_length = stats.avg_length;

            // Check if this looks like a time column
            if col_profile.looks_like_timestamp() {
                profile.detected_time_columns.push(col_profile.name.clone());
            }

            profile.columns.push(col_profile);
        }

        // Detect partition pattern from file paths
        profile.detected_partition_pattern =
            self.detect_partition_pattern(&profile.sample_file_paths);

        // Estimate total row count from sampled files
        profile.estimated_row_count = row_count_estimate * file_paths.len() as u64;

        Ok(profile)
    }

    /// Detect partition pattern from file paths.
    ///
    /// Looks for common partition patterns like:
    /// - `year=2024/month=01/day=15`
    /// - `dt=2024-01-15`
    /// - `date=2024-01-15`
    pub fn detect_partition_pattern(&self, paths: &[String]) -> Option<String> {
        if paths.is_empty() {
            return None;
        }

        // Try to find common partition segments
        let mut partition_keys: HashMap<String, usize> = HashMap::new();

        // Use static regex for performance
        for path in paths {
            for cap in PARTITION_KV_REGEX.captures_iter(path) {
                let key = cap.get(1)?.as_str().to_string();
                *partition_keys.entry(key).or_insert(0) += 1;
            }
        }

        if partition_keys.is_empty() {
            return None;
        }

        // Only include keys that appear in majority of files.
        // Use (len + 1) / 2 to ensure proper majority threshold:
        // - 2 files: threshold = 1 (50%)
        // - 3 files: threshold = 2 (66%)
        // - 4 files: threshold = 2 (50%)
        let threshold = (paths.len() + 1) / 2;
        let mut consistent_keys: Vec<_> = partition_keys
            .into_iter()
            .filter(|(_, count)| *count >= threshold)
            .map(|(key, _)| key)
            .collect();

        if consistent_keys.is_empty() {
            return None;
        }

        // Sort keys by common ordering: year, month, day, hour, date, dt
        let key_order = |k: &String| -> usize {
            match k.to_lowercase().as_str() {
                "year" => 0,
                "month" => 1,
                "day" => 2,
                "hour" => 3,
                "date" | "dt" => 4,
                _ => 10,
            }
        };
        consistent_keys.sort_by_key(key_order);

        // Build pattern string in the format: key={key}/key2={key2}
        let pattern = consistent_keys
            .iter()
            .map(|k| format!("{}={{{}}}", k, k))
            .collect::<Vec<_>>()
            .join("/");

        Some(pattern)
    }

    /// Select the best time column from candidates.
    ///
    /// Priority order:
    /// 1. `timestamp`
    /// 2. `created_at`
    /// 3. `event_time`
    /// 4. `time`
    /// 5. `date`
    /// 6. First Timestamp/Date type column
    pub fn select_time_column(&self, profile: &DataProfile) -> Option<String> {
        let priority_names = [
            "timestamp",
            "created_at",
            "event_time",
            "occurred_at",
            "time",
            "date",
        ];

        // Check by name priority
        for name in priority_names {
            if profile
                .detected_time_columns
                .iter()
                .any(|c| c.to_lowercase() == name)
            {
                return profile
                    .detected_time_columns
                    .iter()
                    .find(|c| c.to_lowercase() == name)
                    .cloned();
            }
        }

        // Check for columns ending with _at
        for col in &profile.detected_time_columns {
            if col.to_lowercase().ends_with("_at") {
                return Some(col.clone());
            }
        }

        // Return first detected time column
        profile.detected_time_columns.first().cloned()
    }
}

impl Default for MetadataSampler {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Column Statistics
// ============================================================================

/// Statistics for a single column from Parquet metadata.
#[derive(Debug, Clone)]
pub struct ColumnStats {
    /// Column name.
    pub name: String,
    /// Column data type.
    pub data_type: ColumnType,
    /// Number of distinct values (if available from Parquet stats).
    pub distinct_count: Option<u64>,
    /// Null value ratio.
    pub null_ratio: f64,
    /// Minimum value (as string).
    pub min_value: Option<String>,
    /// Maximum value (as string).
    pub max_value: Option<String>,
    /// Sample values.
    pub sample_values: Vec<String>,
    /// Average value length (for strings).
    pub avg_length: Option<f64>,
    /// Total row count in sampled files.
    pub row_count: Option<u64>,
}

impl ColumnStats {
    /// Create new column stats.
    pub fn new(name: impl Into<String>, data_type: ColumnType) -> Self {
        Self {
            name: name.into(),
            data_type,
            distinct_count: None,
            null_ratio: 0.0,
            min_value: None,
            max_value: None,
            sample_values: Vec::new(),
            avg_length: None,
            row_count: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_partition_pattern_hive_style() {
        let sampler = MetadataSampler::new();
        let paths = vec![
            "data/year=2024/month=01/day=15/file1.parquet".to_string(),
            "data/year=2024/month=01/day=16/file2.parquet".to_string(),
            "data/year=2024/month=02/day=01/file3.parquet".to_string(),
        ];

        let pattern = sampler.detect_partition_pattern(&paths);
        assert!(pattern.is_some());
        let pattern = pattern.unwrap();
        assert!(pattern.contains("year="));
        assert!(pattern.contains("month="));
        assert!(pattern.contains("day="));
    }

    #[test]
    fn test_detect_partition_pattern_date() {
        let sampler = MetadataSampler::new();
        let paths = vec![
            "data/dt=2024-01-15/file1.parquet".to_string(),
            "data/dt=2024-01-16/file2.parquet".to_string(),
        ];

        let pattern = sampler.detect_partition_pattern(&paths);
        assert!(pattern.is_some());
        let pattern = pattern.unwrap();
        assert!(pattern.contains("dt="));
    }

    #[test]
    fn test_detect_partition_pattern_none() {
        let sampler = MetadataSampler::new();
        let paths = vec![
            "data/file1.parquet".to_string(),
            "data/file2.parquet".to_string(),
        ];

        let pattern = sampler.detect_partition_pattern(&paths);
        assert!(pattern.is_none());
    }

    #[test]
    fn test_select_time_column_priority() {
        let sampler = MetadataSampler::new();
        let mut profile = DataProfile::new(Uuid::new_v4());
        profile.detected_time_columns = vec![
            "date".to_string(),
            "created_at".to_string(),
            "timestamp".to_string(),
        ];

        let selected = sampler.select_time_column(&profile);
        assert_eq!(selected, Some("timestamp".to_string()));
    }

    #[test]
    fn test_select_time_column_at_suffix() {
        let sampler = MetadataSampler::new();
        let mut profile = DataProfile::new(Uuid::new_v4());
        profile.detected_time_columns = vec!["processed_at".to_string(), "some_date".to_string()];

        let selected = sampler.select_time_column(&profile);
        assert_eq!(selected, Some("processed_at".to_string()));
    }
}
