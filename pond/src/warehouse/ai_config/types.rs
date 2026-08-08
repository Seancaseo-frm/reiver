//! AI Configuration Generator Types
//!
//! Data structures for profiling data sources and generating configuration recommendations.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::warehouse::types::{
    CardinalityHint, ColumnType, ExternalSourceConfig, IndexColumnConfig, TableFormat,
};

// ============================================================================
// Data Profile
// ============================================================================

/// Profile of a data source based on sampling.
///
/// Contains aggregated statistics about the data that can be used
/// to generate optimal indexing configurations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataProfile {
    /// Source ID that was profiled.
    pub source_id: Uuid,
    /// When the profile was generated.
    pub profiled_at: DateTime<Utc>,
    /// Column profiles with statistics.
    pub columns: Vec<ColumnProfile>,
    /// Number of files sampled.
    pub file_count: usize,
    /// Estimated total row count across all files.
    pub estimated_row_count: u64,
    /// Detected partition pattern (e.g., "year={year}/month={month}/day={day}").
    pub detected_partition_pattern: Option<String>,
    /// Columns that appear to be time-based (candidates for time_column).
    pub detected_time_columns: Vec<String>,
    /// Sample file paths that were analyzed.
    pub sample_file_paths: Vec<String>,
    /// Detected table format (Iceberg, Delta, or raw Parquet).
    pub detected_table_format: Option<TableFormat>,
}

impl DataProfile {
    /// Create a new empty data profile.
    pub fn new(source_id: Uuid) -> Self {
        Self {
            source_id,
            profiled_at: Utc::now(),
            columns: Vec::new(),
            file_count: 0,
            estimated_row_count: 0,
            detected_partition_pattern: None,
            detected_time_columns: Vec::new(),
            sample_file_paths: Vec::new(),
            detected_table_format: None,
        }
    }

    /// Get a column profile by name.
    pub fn get_column(&self, name: &str) -> Option<&ColumnProfile> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Get columns that look like identifiers (high cardinality).
    pub fn identifier_columns(&self) -> Vec<&ColumnProfile> {
        self.columns
            .iter()
            .filter(|c| c.looks_like_identifier())
            .collect()
    }

    /// Get columns that look like categories (low cardinality).
    pub fn category_columns(&self) -> Vec<&ColumnProfile> {
        self.columns
            .iter()
            .filter(|c| c.looks_like_category())
            .collect()
    }
}

// ============================================================================
// Column Profile
// ============================================================================

/// Profile of a single column based on sampling.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnProfile {
    /// Column name.
    pub name: String,
    /// Inferred data type.
    pub data_type: ColumnType,
    /// Ratio of null values (0.0 to 1.0).
    pub null_ratio: f64,
    /// Estimated number of distinct values.
    pub estimated_cardinality: Option<u64>,
    /// Minimum value (as string for display).
    pub min_value: Option<String>,
    /// Maximum value (as string for display).
    pub max_value: Option<String>,
    /// Sample values from the column.
    pub sample_values: Vec<String>,
    /// Average value length (for strings).
    pub avg_value_length: Option<f64>,
}

impl ColumnProfile {
    /// Create a new column profile.
    pub fn new(name: impl Into<String>, data_type: ColumnType) -> Self {
        Self {
            name: name.into(),
            data_type,
            null_ratio: 0.0,
            estimated_cardinality: None,
            min_value: None,
            max_value: None,
            sample_values: Vec::new(),
            avg_value_length: None,
        }
    }

    /// Check if this column looks like an identifier (high cardinality).
    pub fn looks_like_identifier(&self) -> bool {
        let name_lower = self.name.to_lowercase();

        // Check naming patterns
        let id_patterns = [
            "_id", "_uuid", "uuid", "id", "_key", "email", "phone",
            "address", "token", "hash", "signature",
        ];

        if id_patterns.iter().any(|p| name_lower.ends_with(p) || name_lower == *p) {
            return true;
        }

        // Check cardinality if available
        if let Some(cardinality) = self.estimated_cardinality {
            // High cardinality relative to a reasonable dataset
            if cardinality > 100_000 {
                return true;
            }
        }

        false
    }

    /// Check if this column looks like a category (low cardinality).
    pub fn looks_like_category(&self) -> bool {
        let name_lower = self.name.to_lowercase();

        // Check naming patterns
        let category_patterns = [
            "status", "state", "type", "kind", "category", "level",
            "priority", "country", "region", "tier", "plan", "role",
        ];

        if category_patterns.iter().any(|p| name_lower.contains(p)) {
            return true;
        }

        // Check cardinality if available
        if let Some(cardinality) = self.estimated_cardinality {
            if cardinality < 100 {
                return true;
            }
        }

        false
    }

    /// Check if this column looks like a timestamp.
    pub fn looks_like_timestamp(&self) -> bool {
        let name_lower = self.name.to_lowercase();

        // Check data type first
        if matches!(self.data_type, ColumnType::Timestamp | ColumnType::Date) {
            return true;
        }

        // Check naming patterns
        let time_patterns = [
            "timestamp", "created_at", "updated_at", "deleted_at",
            "event_time", "occurred_at", "date", "time", "_at",
        ];

        time_patterns.iter().any(|p| name_lower.contains(p))
    }

    /// Infer a cardinality hint based on the profile.
    pub fn infer_cardinality_hint(&self) -> CardinalityHint {
        if let Some(cardinality) = self.estimated_cardinality {
            if cardinality < 100 {
                CardinalityHint::VeryLow
            } else if cardinality < 10_000 {
                CardinalityHint::Low
            } else if cardinality < 100_000 {
                CardinalityHint::Medium
            } else if cardinality < 1_000_000 {
                CardinalityHint::High
            } else {
                CardinalityHint::VeryHigh
            }
        } else if self.looks_like_identifier() {
            CardinalityHint::VeryHigh
        } else if self.looks_like_category() {
            CardinalityHint::Low
        } else {
            CardinalityHint::Medium
        }
    }
}

// ============================================================================
// Config Recommendation
// ============================================================================

/// AI-generated configuration recommendation.
///
/// Contains the recommended configuration along with explanations
/// for each decision to help users understand and verify the suggestions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigRecommendation {
    /// The recommended configuration.
    pub config: ExternalSourceConfig,
    /// Overall confidence score (0.0 to 1.0).
    pub confidence: f64,
    /// Explanations for each configuration decision.
    pub explanations: Vec<ConfigExplanation>,
    /// Warnings or suggestions for the user.
    pub warnings: Vec<String>,
}

impl ConfigRecommendation {
    /// Create a new recommendation with the given config.
    pub fn new(config: ExternalSourceConfig) -> Self {
        Self {
            config,
            confidence: 0.0,
            explanations: Vec::new(),
            warnings: Vec::new(),
        }
    }

    /// Add an explanation.
    pub fn add_explanation(&mut self, field: impl Into<String>, reason: impl Into<String>, confidence: f64) {
        self.explanations.push(ConfigExplanation {
            field: field.into(),
            reason: reason.into(),
            confidence,
        });
    }

    /// Add a warning.
    pub fn add_warning(&mut self, warning: impl Into<String>) {
        self.warnings.push(warning.into());
    }

    /// Calculate overall confidence from individual explanations.
    pub fn calculate_confidence(&mut self) {
        if self.explanations.is_empty() {
            self.confidence = 0.5; // Default when no explanations
        } else {
            self.confidence = self.explanations.iter().map(|e| e.confidence).sum::<f64>()
                / self.explanations.len() as f64;
        }
    }
}

/// Explanation for a specific configuration decision.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigExplanation {
    /// Which configuration field this explains.
    pub field: String,
    /// Human-readable reason for the decision.
    pub reason: String,
    /// Confidence in this specific decision (0.0 to 1.0).
    pub confidence: f64,
}

// ============================================================================
// Builder Helpers
// ============================================================================

/// Builder for creating column configurations from profiles.
pub fn build_index_columns(profile: &DataProfile) -> Vec<IndexColumnConfig> {
    profile
        .columns
        .iter()
        .filter(|col| {
            // Skip columns that shouldn't be indexed
            !col.looks_like_identifier() ||
                // Include if low enough cardinality
                col.estimated_cardinality.map(|c| c < 1_000_000).unwrap_or(false)
        })
        .filter(|col| {
            // Skip numeric columns (use min/max stats instead)
            !matches!(
                col.data_type,
                ColumnType::Int32 | ColumnType::Int64 | ColumnType::Float64 | 
                ColumnType::Decimal | ColumnType::Timestamp | ColumnType::Date
            )
        })
        .map(|col| {
            IndexColumnConfig::with_cardinality(&col.name, col.infer_cardinality_hint())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_profile_looks_like_identifier() {
        let user_id = ColumnProfile::new("user_id", ColumnType::String);
        assert!(user_id.looks_like_identifier());

        let email = ColumnProfile::new("email", ColumnType::String);
        assert!(email.looks_like_identifier());

        let status = ColumnProfile::new("status", ColumnType::String);
        assert!(!status.looks_like_identifier());
    }

    #[test]
    fn test_column_profile_looks_like_category() {
        let status = ColumnProfile::new("status", ColumnType::String);
        assert!(status.looks_like_category());

        let country = ColumnProfile::new("country_code", ColumnType::String);
        assert!(country.looks_like_category());

        let user_id = ColumnProfile::new("user_id", ColumnType::String);
        assert!(!user_id.looks_like_category());
    }

    #[test]
    fn test_column_profile_looks_like_timestamp() {
        let created = ColumnProfile::new("created_at", ColumnType::Timestamp);
        assert!(created.looks_like_timestamp());

        let event_time = ColumnProfile::new("event_time", ColumnType::String);
        assert!(event_time.looks_like_timestamp());

        let name = ColumnProfile::new("name", ColumnType::String);
        assert!(!name.looks_like_timestamp());
    }

    #[test]
    fn test_infer_cardinality_hint() {
        let mut col = ColumnProfile::new("test", ColumnType::String);

        col.estimated_cardinality = Some(50);
        assert_eq!(col.infer_cardinality_hint(), CardinalityHint::VeryLow);

        col.estimated_cardinality = Some(5_000);
        assert_eq!(col.infer_cardinality_hint(), CardinalityHint::Low);

        col.estimated_cardinality = Some(50_000);
        assert_eq!(col.infer_cardinality_hint(), CardinalityHint::Medium);

        col.estimated_cardinality = Some(500_000);
        assert_eq!(col.infer_cardinality_hint(), CardinalityHint::High);

        col.estimated_cardinality = Some(5_000_000);
        assert_eq!(col.infer_cardinality_hint(), CardinalityHint::VeryHigh);
    }

    #[test]
    fn test_config_recommendation_confidence() {
        let mut rec = ConfigRecommendation::new(ExternalSourceConfig::default());
        rec.add_explanation("time_column", "Detected timestamp column", 0.9);
        rec.add_explanation("partition_pattern", "Detected hive-style partitions", 0.8);
        rec.calculate_confidence();

        assert!((rec.confidence - 0.85).abs() < 0.01);
    }
}
