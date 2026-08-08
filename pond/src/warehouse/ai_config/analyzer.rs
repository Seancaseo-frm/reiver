//! Configuration Analyzer
//!
//! Orchestrates the AI configuration generation process:
//! 1. Samples metadata from the data source
//! 2. Builds a data profile
//! 3. Generates configuration recommendations

use std::sync::Arc;
use thiserror::Error;
use tracing::info;
use uuid::Uuid;

use crate::warehouse::sources::{DataSourceRegistry, RegisteredSource, SourceBackend};

use super::provider::AIConfigProvider;
use super::sampler::{ColumnStats, MetadataSampler};
use super::types::{ConfigRecommendation, DataProfile};

// ============================================================================
// Errors
// ============================================================================

/// Errors that can occur during configuration analysis.
#[derive(Debug, Error)]
pub enum AnalyzerError {
    #[error("Source not found: {0}")]
    SourceNotFound(String),

    #[error("Unsupported source type for analysis")]
    UnsupportedSourceType,

    #[error(
        "Empty data profile: no columns or files found to analyze. Ensure the source has data."
    )]
    EmptyProfile,

    #[error("Sampling error: {0}")]
    Sampling(#[from] super::sampler::SamplerError),

    #[error("AI provider error: {0}")]
    Provider(#[from] super::provider::AIConfigError),

    #[error("Registry error: {0}")]
    Registry(#[from] crate::warehouse::sources::RegistryError),

    #[error("Storage error: {0}")]
    Storage(String),
}

/// Result type for analyzer operations.
pub type AnalyzerResult<T> = Result<T, AnalyzerError>;

// ============================================================================
// Config Analyzer
// ============================================================================

/// Analyzes data sources and generates configuration recommendations.
pub struct ConfigAnalyzer<P: AIConfigProvider> {
    /// Source registry for resolving sources.
    registry: Arc<DataSourceRegistry>,
    /// Metadata sampler for building profiles.
    sampler: MetadataSampler,
    /// AI provider for generating recommendations.
    provider: P,
}

impl<P: AIConfigProvider> ConfigAnalyzer<P> {
    /// Create a new config analyzer.
    pub fn new(registry: Arc<DataSourceRegistry>, provider: P) -> Self {
        Self {
            registry,
            sampler: MetadataSampler::new(),
            provider,
        }
    }

    /// Analyze a source and generate a configuration recommendation.
    ///
    /// This is the main entry point for the AI configuration feature.
    pub async fn analyze_source(
        &self,
        project_id: Uuid,
        source_id: Uuid,
    ) -> AnalyzerResult<ConfigRecommendation> {
        // Resolve the source
        let source = self.registry.resolve_by_id(project_id, source_id).await?;

        info!(
            source_id = %source_id,
            source_name = %source.name,
            "Analyzing source for config generation"
        );

        // Build profile based on source type
        let profile = self.build_profile(&source).await?;

        // Validate that the profile has data before passing to provider
        if profile.columns.is_empty() {
            return Err(AnalyzerError::EmptyProfile);
        }

        // Generate recommendation
        let recommendation = self.provider.generate_config(&profile).await?;

        info!(
            source_id = %source_id,
            confidence = recommendation.confidence,
            explanation_count = recommendation.explanations.len(),
            "Generated configuration recommendation"
        );

        Ok(recommendation)
    }

    /// Build a data profile for a source.
    async fn build_profile(&self, source: &RegisteredSource) -> AnalyzerResult<DataProfile> {
        match &source.backend {
            SourceBackend::ObjectStorage {
                bucket_url, prefix, ..
            } => {
                // For object storage, we would list files and sample Parquet metadata
                // For now, return a mock profile that would be filled by actual sampling
                self.build_object_storage_profile(source.id, bucket_url, prefix)
                    .await
            }
            SourceBackend::ClickHouseNative {
                database,
                table_prefix,
            } => {
                // For ClickHouse native, we could query system tables for statistics
                self.build_clickhouse_profile(source.id, database, table_prefix)
                    .await
            }
            SourceBackend::ExternalDatabase { .. } => Err(AnalyzerError::UnsupportedSourceType),
            SourceBackend::ExternalApi { .. } => {
                // External API sources (like Google Sheets) don't need AI config
                // They use cold tier and don't sync data to storage
                Err(AnalyzerError::UnsupportedSourceType)
            }
        }
    }

    /// Build a profile for object storage sources.
    async fn build_object_storage_profile(
        &self,
        source_id: Uuid,
        _bucket_url: &str,
        _prefix: &str,
    ) -> AnalyzerResult<DataProfile> {
        // TODO: Implement actual file listing and Parquet metadata reading
        // For now, return a minimal profile that the provider can work with

        // In a real implementation, this would:
        // 1. List files in the bucket/prefix
        // 2. Sample a subset of Parquet files
        // 3. Read Parquet footers for schema and statistics
        // 4. Aggregate statistics across sampled files

        let profile = DataProfile::new(source_id);
        Ok(profile)
    }

    /// Build a profile for ClickHouse native sources.
    async fn build_clickhouse_profile(
        &self,
        source_id: Uuid,
        _database: &str,
        _table_prefix: &str,
    ) -> AnalyzerResult<DataProfile> {
        // TODO: Query ClickHouse system tables for statistics
        // For now, return a minimal profile

        // In a real implementation, this would:
        // 1. Query system.columns for schema info
        // 2. Query system.parts for row counts and sizes
        // 3. Sample data for cardinality estimation

        let profile = DataProfile::new(source_id);
        Ok(profile)
    }

    /// Analyze with explicit column stats (for testing or external sampling).
    pub async fn analyze_with_stats(
        &self,
        source_id: Uuid,
        column_stats: Vec<ColumnStats>,
        file_paths: Vec<String>,
    ) -> AnalyzerResult<ConfigRecommendation> {
        let profile = self
            .sampler
            .build_profile(source_id, column_stats, file_paths)?;
        let recommendation = self.provider.generate_config(&profile).await?;
        Ok(recommendation)
    }
}

#[cfg(test)]
mod tests {
    use super::super::provider::MockAIConfigProvider;
    use super::*;
    use crate::warehouse::types::ColumnType;

    #[tokio::test]
    async fn test_analyze_with_stats() {
        // Create a mock analyzer without a real registry
        // For this test, we'll just test the analyze_with_stats method

        let column_stats = vec![
            {
                let mut c = ColumnStats::new("user_id", ColumnType::String);
                c.distinct_count = Some(500_000);
                c
            },
            {
                let mut c = ColumnStats::new("status", ColumnType::String);
                c.distinct_count = Some(5);
                c.sample_values = vec!["active".to_string(), "inactive".to_string()];
                c
            },
            {
                let mut c = ColumnStats::new("created_at", ColumnType::Timestamp);
                c
            },
        ];

        let file_paths = vec![
            "data/year=2024/month=01/file.parquet".to_string(),
            "data/year=2024/month=02/file.parquet".to_string(),
        ];

        let sampler = MetadataSampler::new();
        let provider = MockAIConfigProvider::new();
        let source_id = Uuid::new_v4();

        let profile = sampler
            .build_profile(source_id, column_stats, file_paths)
            .unwrap();
        let recommendation = provider.generate_config(&profile).await.unwrap();

        assert!(recommendation.confidence > 0.0);
        assert!(!recommendation.explanations.is_empty());
    }
}
