//! AI Configuration Provider
//!
//! Trait and implementations for AI-powered configuration generation.
//! The mock implementation uses heuristics; a real implementation would
//! call an LLM service.

use async_trait::async_trait;
use thiserror::Error;
use tracing::debug;

use crate::warehouse::types::{
    CardinalityHint, ExternalSourceConfig, IndexColumnConfig, MutabilityStrategy, RefreshConfig,
    RefreshInterval, TableFormat, TimeUnit,
};

use super::sampler::MetadataSampler;
use super::types::{build_index_columns, ConfigRecommendation, DataProfile};

// ============================================================================
// Errors
// ============================================================================

/// Errors that can occur during AI configuration generation.
#[derive(Debug, Error)]
pub enum AIConfigError {
    #[error("Insufficient data for analysis")]
    InsufficientData,

    #[error("Provider error: {0}")]
    Provider(String),

    #[error("Configuration error: {0}")]
    Config(String),
}

/// Result type for AI config operations.
pub type AIConfigResult<T> = Result<T, AIConfigError>;

// ============================================================================
// AI Config Provider Trait
// ============================================================================

/// Trait for AI-powered configuration generation.
///
/// Implementations can use various strategies:
/// - Mock: Rule-based heuristics
/// - Gateway: Call to AI gateway (Claude, GPT, etc.)
/// - Hybrid: Heuristics with AI validation
#[async_trait]
pub trait AIConfigProvider: Send + Sync {
    /// Generate a configuration recommendation from a data profile.
    async fn generate_config(&self, profile: &DataProfile) -> AIConfigResult<ConfigRecommendation>;
}

// ============================================================================
// Mock AI Provider
// ============================================================================

/// Mock AI provider that uses rule-based heuristics.
///
/// This implementation mimics what an AI would do by analyzing:
/// - Column names for semantic meaning
/// - Data types for appropriate indexing
/// - Cardinality for index strategy selection
/// - File paths for partition detection
///
/// When a real AI is available, create a new implementation that
/// calls the AI gateway with the DataProfile as context.
pub struct MockAIConfigProvider {
    sampler: MetadataSampler,
}

impl MockAIConfigProvider {
    /// Create a new mock AI provider.
    pub fn new() -> Self {
        Self {
            sampler: MetadataSampler::new(),
        }
    }

    /// Detect the best time column from the profile.
    fn detect_time_column(&self, profile: &DataProfile) -> Option<String> {
        self.sampler.select_time_column(profile)
    }

    /// Build index column configurations from the profile.
    fn build_index_configs(&self, profile: &DataProfile) -> Vec<IndexColumnConfig> {
        build_index_columns(profile)
    }

    /// Detect mutability strategy based on partition pattern.
    fn detect_mutability(&self, profile: &DataProfile) -> MutabilityStrategy {
        // If we detected date-based partitions, use rolling window
        if let Some(pattern) = &profile.detected_partition_pattern {
            let has_time_partition = pattern.contains("year")
                || pattern.contains("month")
                || pattern.contains("day")
                || pattern.contains("date")
                || pattern.contains("dt");

            if has_time_partition {
                return MutabilityStrategy::RollingWindow {
                    window: 1,
                    unit: TimeUnit::Day,
                };
            }
        }

        // Default to all immutable (safe choice for historical data)
        MutabilityStrategy::AllImmutable
    }

    /// Detect table format from file paths.
    fn detect_table_format(&self, profile: &DataProfile) -> TableFormat {
        for path in &profile.sample_file_paths {
            if path.contains("_delta_log") || path.contains("delta_log") {
                return TableFormat::DeltaLake;
            }
            if path.contains("metadata/") && path.contains(".metadata.json") {
                return TableFormat::Iceberg;
            }
        }

        // Check if profile already detected format
        profile
            .detected_table_format
            .unwrap_or(TableFormat::RawParquet)
    }

    /// Generate refresh configuration.
    fn generate_refresh_config(&self, profile: &DataProfile) -> RefreshConfig {
        // If data appears to be time-partitioned, use hourly refresh
        if profile.detected_partition_pattern.is_some() {
            RefreshConfig {
                mutable_refresh: RefreshInterval::Hourly,
                auto_discover: true,
                discovery_interval: RefreshInterval::Hourly,
            }
        } else {
            // For non-partitioned data, refresh on query
            RefreshConfig {
                mutable_refresh: RefreshInterval::OnQuery,
                auto_discover: true,
                discovery_interval: RefreshInterval::Every6Hours,
            }
        }
    }
}

impl Default for MockAIConfigProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AIConfigProvider for MockAIConfigProvider {
    async fn generate_config(&self, profile: &DataProfile) -> AIConfigResult<ConfigRecommendation> {
        if profile.columns.is_empty() {
            return Err(AIConfigError::InsufficientData);
        }

        debug!(
            source_id = %profile.source_id,
            column_count = profile.columns.len(),
            file_count = profile.file_count,
            "Generating config recommendation"
        );

        let mut config = ExternalSourceConfig::default();
        let mut recommendation = ConfigRecommendation::new(config.clone());

        // Detect table format
        let table_format = self.detect_table_format(profile);
        config.table_format = table_format;
        recommendation.add_explanation(
            "table_format",
            format!("Detected {} based on file structure", table_format),
            0.9,
        );

        // Detect time column
        if let Some(time_col) = self.detect_time_column(profile) {
            config.time_column = Some(time_col.clone());
            recommendation.add_explanation(
                "time_column",
                format!(
                    "Selected '{}' as the time column based on naming and type",
                    time_col
                ),
                0.85,
            );
        } else {
            recommendation.add_warning(
                "No time column detected. Consider specifying one for better partition handling."
                    .to_string(),
            );
        }

        // Detect partition pattern
        if let Some(pattern) = &profile.detected_partition_pattern {
            config.partition_pattern = Some(pattern.clone());
            recommendation.add_explanation(
                "partition_pattern",
                format!("Detected partition pattern: {}", pattern),
                0.9,
            );
        }

        // Detect mutability strategy
        config.mutability = self.detect_mutability(profile);
        let mutability_reason = match &config.mutability {
            MutabilityStrategy::RollingWindow { window, unit } => {
                format!(
                    "Using rolling window ({} {:?}) based on date partitioning",
                    window, unit
                )
            }
            MutabilityStrategy::AllImmutable => {
                "Treating all partitions as immutable (no date-based partitioning detected)"
                    .to_string()
            }
            MutabilityStrategy::AllMutable => "All data treated as mutable".to_string(),
            MutabilityStrategy::FileAge { hours } => {
                format!("Files older than {} hours are immutable", hours)
            }
        };
        recommendation.add_explanation("mutability", mutability_reason, 0.8);

        // Build index columns
        config.index_columns = self.build_index_configs(profile);
        if !config.index_columns.is_empty() {
            recommendation.add_explanation(
                "index_columns",
                format!(
                    "Selected {} columns for indexing based on cardinality analysis",
                    config.index_columns.len()
                ),
                0.75,
            );

            // Add explanations for individual columns
            for col_config in &config.index_columns {
                if let Some(col_profile) = profile.get_column(&col_config.name) {
                    let cardinality_str = match col_config.cardinality {
                        Some(CardinalityHint::VeryLow) => "very low",
                        Some(CardinalityHint::Low) => "low",
                        Some(CardinalityHint::Medium) => "medium",
                        Some(CardinalityHint::High) => "high",
                        Some(CardinalityHint::VeryHigh) => "very high",
                        None => "unknown",
                    };
                    debug!(
                        column = %col_config.name,
                        cardinality = cardinality_str,
                        "Index column configured"
                    );
                }
            }
        }

        // Set refresh config
        config.refresh = self.generate_refresh_config(profile);
        recommendation.add_explanation(
            "refresh",
            format!(
                "Mutable refresh set to {:?}",
                config.refresh.mutable_refresh
            ),
            0.7,
        );

        // Update config in recommendation and calculate confidence
        recommendation.config = config;
        recommendation.calculate_confidence();

        Ok(recommendation)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::warehouse::types::ColumnType;
    use uuid::Uuid;

    fn create_test_profile() -> DataProfile {
        let mut profile = DataProfile::new(Uuid::new_v4());

        profile.columns = vec![
            {
                let mut c = super::super::types::ColumnProfile::new("user_id", ColumnType::String);
                c.estimated_cardinality = Some(1_000_000);
                c
            },
            {
                let mut c = super::super::types::ColumnProfile::new("status", ColumnType::String);
                c.estimated_cardinality = Some(5);
                c
            },
            {
                let mut c =
                    super::super::types::ColumnProfile::new("created_at", ColumnType::Timestamp);
                c
            },
            {
                let mut c = super::super::types::ColumnProfile::new("country", ColumnType::String);
                c.estimated_cardinality = Some(200);
                c
            },
        ];

        profile.detected_time_columns = vec!["created_at".to_string()];
        profile.sample_file_paths = vec!["data/year=2024/month=01/day=15/file.parquet".to_string()];
        profile.detected_partition_pattern =
            Some("year={year}/month={month}/day={day}".to_string());
        profile.file_count = 100;

        profile
    }

    #[tokio::test]
    async fn test_mock_provider_generates_config() {
        let provider = MockAIConfigProvider::new();
        let profile = create_test_profile();

        let recommendation = provider.generate_config(&profile).await.unwrap();

        assert!(recommendation.confidence > 0.5);
        assert!(!recommendation.explanations.is_empty());
        assert!(recommendation.config.time_column.is_some());
        assert!(recommendation.config.partition_pattern.is_some());
    }

    #[tokio::test]
    async fn test_mock_provider_detects_time_column() {
        let provider = MockAIConfigProvider::new();
        let profile = create_test_profile();

        let recommendation = provider.generate_config(&profile).await.unwrap();

        assert_eq!(
            recommendation.config.time_column,
            Some("created_at".to_string())
        );
    }

    #[tokio::test]
    async fn test_mock_provider_sets_rolling_window() {
        let provider = MockAIConfigProvider::new();
        let profile = create_test_profile();

        let recommendation = provider.generate_config(&profile).await.unwrap();

        assert!(matches!(
            recommendation.config.mutability,
            MutabilityStrategy::RollingWindow { .. }
        ));
    }

    #[tokio::test]
    async fn test_mock_provider_empty_profile_error() {
        let provider = MockAIConfigProvider::new();
        let profile = DataProfile::new(Uuid::new_v4());

        let result = provider.generate_config(&profile).await;
        assert!(result.is_err());
    }
}
