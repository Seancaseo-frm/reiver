//! AI-Powered Configuration Generator
//!
//! This module provides AI-powered configuration generation for data sources.
//! It analyzes data profiles and generates optimal indexing configurations.
//!
//! # Overview
//!
//! Non-technical customers often don't know the best way to configure their
//! data sources for optimal query performance. This module automates that
//! process by:
//!
//! 1. **Sampling**: Reading Parquet metadata to understand data characteristics
//! 2. **Profiling**: Building a profile of each column's type, cardinality, and patterns
//! 3. **Recommending**: Using AI (mocked with heuristics initially) to suggest configs
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────┐     ┌─────────────┐     ┌─────────────────────┐
//! │   Sampler   │────>│  DataProfile│────>│ AIConfigProvider    │
//! │  (metadata) │     │  (analysis) │     │ (mock or real AI)   │
//! └─────────────┘     └─────────────┘     └─────────────────────┘
//!                                                   │
//!                                                   v
//!                                         ┌─────────────────────┐
//!                                         │ ConfigRecommendation│
//!                                         │ + explanations      │
//!                                         └─────────────────────┘
//! ```
//!
//! # Usage
//!
//! ```ignore
//! use reiver::warehouse::ai_config::{ConfigAnalyzer, MockAIConfigProvider};
//!
//! let analyzer = ConfigAnalyzer::new(registry, MockAIConfigProvider::new());
//! let recommendation = analyzer.analyze_source(project_id, source_id).await?;
//!
//! println!("Suggested config: {:?}", recommendation.config);
//! for explanation in &recommendation.explanations {
//!     println!("- {}: {}", explanation.field, explanation.reason);
//! }
//! ```

pub mod analyzer;
pub mod provider;
pub mod sampler;
pub mod types;

pub use analyzer::{ConfigAnalyzer, AnalyzerError, AnalyzerResult};
pub use provider::{AIConfigProvider, MockAIConfigProvider, AIConfigError, AIConfigResult};
pub use sampler::{MetadataSampler, ColumnStats, SamplerError, SamplerResult};
pub use types::{
    DataProfile, ColumnProfile, ConfigRecommendation, ConfigExplanation,
    build_index_columns,
};
