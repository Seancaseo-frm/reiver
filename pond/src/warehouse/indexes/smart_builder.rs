//! Smart Index Builder for Append-Only Data
//!
//! Optimizes FST index building for append-only data patterns by:
//! - Freezing old partitions (no rebuilds after threshold)
//! - Rebuilding hot partitions incrementally
//! - Merging per-file FSTs into partition FSTs on freeze
//!
//! This significantly reduces index maintenance costs for time-series data.
//!
//! # Integration with External Sources
//!
//! For external Parquet sources, use `with_mutability_strategy()` to configure
//! partition freezing based on user-provided mutability rules:
//!
//! ```ignore
//! use reiver::warehouse::types::MutabilityStrategy;
//!
//! let strategy = MutabilityStrategy::RollingWindow {
//!     window: 1,
//!     unit: TimeUnit::Day,
//! };
//!
//! let builder = SmartIndexBuilder::with_defaults()
//!     .with_mutability_strategy(strategy);
//! ```

use chrono::{DateTime, Duration, Utc};
use std::collections::{HashMap, HashSet};
use thiserror::Error;

use crate::warehouse::types::MutabilityStrategy;

/// Errors that can occur during smart index building.
#[derive(Debug, Error)]
pub enum SmartBuilderError {
    #[error("Partition not found: {0}")]
    PartitionNotFound(String),

    #[error("Index operation failed: {0}")]
    IndexError(String),

    #[error("Storage error: {0}")]
    StorageError(String),
}

/// Result type for smart builder operations.
pub type SmartBuilderResult<T> = Result<T, SmartBuilderError>;

/// Configuration for the smart index builder.
#[derive(Debug, Clone)]
pub struct SmartIndexBuilderConfig {
    /// Partitions older than this are considered frozen.
    /// Default: 24 hours
    ///
    /// Note: This is overridden if `mutability_strategy` is set.
    pub freeze_threshold: Duration,

    /// Target size for partition FST before considering split.
    /// Default: 100 MB
    pub target_partition_size_bytes: usize,

    /// Maximum files per partition before forcing merge.
    /// Default: 100
    pub max_files_per_partition: usize,

    /// Minimum files before merging into partition FST.
    /// Default: 10
    pub min_files_for_merge: usize,

    /// Optional mutability strategy from external source config.
    ///
    /// When set, this overrides `freeze_threshold` and uses the strategy's
    /// rules to determine partition mutability.
    pub mutability_strategy: Option<MutabilityStrategy>,
}

impl Default for SmartIndexBuilderConfig {
    fn default() -> Self {
        Self {
            freeze_threshold: Duration::hours(24),
            target_partition_size_bytes: 100 * 1024 * 1024, // 100 MB
            max_files_per_partition: 100,
            min_files_for_merge: 10,
            mutability_strategy: None,
        }
    }
}

impl SmartIndexBuilderConfig {
    /// Create a config from a MutabilityStrategy.
    ///
    /// This is useful when configuring the builder from an ExternalSourceConfig.
    pub fn from_mutability_strategy(strategy: MutabilityStrategy) -> Self {
        let freeze_threshold = strategy
            .mutable_window()
            .unwrap_or_else(|| Duration::hours(24));

        Self {
            freeze_threshold,
            mutability_strategy: Some(strategy),
            ..Default::default()
        }
    }

    /// Set the mutability strategy.
    pub fn with_mutability_strategy(mut self, strategy: MutabilityStrategy) -> Self {
        if let Some(window) = strategy.mutable_window() {
            self.freeze_threshold = window;
        }
        self.mutability_strategy = Some(strategy);
        self
    }
}

/// Metadata about a partition's status.
#[derive(Debug, Clone)]
pub struct PartitionStatus {
    /// Partition key (e.g., "2025/01/15")
    pub partition_key: String,

    /// When the partition was last modified
    pub last_modified: DateTime<Utc>,

    /// Whether the partition is frozen (no more writes expected)
    pub is_frozen: bool,

    /// Number of files in the partition
    pub file_count: usize,

    /// Whether a merge is needed
    pub needs_merge: bool,

    /// Estimated size in bytes
    pub estimated_size_bytes: usize,
}

/// Smart Index Builder for append-only data.
///
/// Tracks partition metadata and determines which partitions need
/// index rebuilding vs which are frozen and can be skipped.
///
/// # Example
/// ```ignore
/// let config = SmartIndexBuilderConfig::default();
/// let mut builder = SmartIndexBuilder::new(config);
///
/// // Register partitions with their last modified time
/// builder.register_partition("2025/01/15", Utc::now() - Duration::days(2), 50);
/// builder.register_partition("2025/01/17", Utc::now(), 10);
///
/// // Check which partitions need work
/// for status in builder.get_partition_statuses() {
///     if status.is_frozen {
///         println!("Partition {} is frozen, skipping", status.partition_key);
///     } else {
///         println!("Partition {} is hot, needs rebuild", status.partition_key);
///     }
/// }
/// ```
#[derive(Debug)]
pub struct SmartIndexBuilder {
    config: SmartIndexBuilderConfig,

    /// Metadata for each partition
    partition_metadata: HashMap<String, PartitionMetadata>,

    /// Partitions that have been marked as permanently frozen
    frozen_partitions: HashSet<String>,
}

/// Internal metadata for a partition.
#[derive(Debug, Clone)]
struct PartitionMetadata {
    partition_key: String,
    last_modified: DateTime<Utc>,
    file_count: usize,
    estimated_size_bytes: usize,
    /// Whether we've merged this partition's FSTs
    merged: bool,
}

impl SmartIndexBuilder {
    /// Create a new smart index builder with the given configuration.
    pub fn new(config: SmartIndexBuilderConfig) -> Self {
        Self {
            config,
            partition_metadata: HashMap::new(),
            frozen_partitions: HashSet::new(),
        }
    }

    /// Create a builder with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(SmartIndexBuilderConfig::default())
    }

    /// Create a builder with a mutability strategy.
    ///
    /// This is a convenience method for external source configuration.
    pub fn with_mutability_strategy(strategy: MutabilityStrategy) -> Self {
        Self::new(SmartIndexBuilderConfig::from_mutability_strategy(strategy))
    }

    /// Set the mutability strategy after creation.
    pub fn set_mutability_strategy(&mut self, strategy: MutabilityStrategy) {
        if let Some(window) = strategy.mutable_window() {
            self.config.freeze_threshold = window;
        }
        self.config.mutability_strategy = Some(strategy);
    }

    /// Get the current mutability strategy (if set).
    pub fn mutability_strategy(&self) -> Option<&MutabilityStrategy> {
        self.config.mutability_strategy.as_ref()
    }

    /// Register a partition with its metadata.
    ///
    /// Call this when discovering partitions in storage or after syncing new data.
    pub fn register_partition(
        &mut self,
        partition_key: impl Into<String>,
        last_modified: DateTime<Utc>,
        file_count: usize,
    ) {
        self.register_partition_with_size(partition_key, last_modified, file_count, 0)
    }

    /// Register a partition with size estimate.
    pub fn register_partition_with_size(
        &mut self,
        partition_key: impl Into<String>,
        last_modified: DateTime<Utc>,
        file_count: usize,
        estimated_size_bytes: usize,
    ) {
        let key = partition_key.into();
        self.partition_metadata.insert(
            key.clone(),
            PartitionMetadata {
                partition_key: key,
                last_modified,
                file_count,
                estimated_size_bytes,
                merged: false,
            },
        );
    }

    /// Update a partition's last modified time.
    pub fn update_partition(&mut self, partition_key: &str, last_modified: DateTime<Utc>) {
        if let Some(meta) = self.partition_metadata.get_mut(partition_key) {
            meta.last_modified = last_modified;
            // If partition is updated, it's no longer frozen
            self.frozen_partitions.remove(partition_key);
        }
    }

    /// Increment file count for a partition.
    pub fn add_file_to_partition(&mut self, partition_key: &str) {
        if let Some(meta) = self.partition_metadata.get_mut(partition_key) {
            meta.file_count += 1;
            meta.last_modified = Utc::now();
        }
    }

    /// Check if a partition should be rebuilt.
    ///
    /// A partition should be rebuilt if:
    /// - It's a hot partition (modified within freeze_threshold)
    /// - It has too many files and needs compaction
    pub fn should_rebuild_partition(&self, partition_key: &str) -> bool {
        // Never rebuild frozen partitions
        if self.frozen_partitions.contains(partition_key) {
            return false;
        }

        if let Some(meta) = self.partition_metadata.get(partition_key) {
            let is_hot = self.is_hot_partition(meta);
            let needs_compaction = meta.file_count > self.config.max_files_per_partition;
            is_hot || needs_compaction
        } else {
            // Unknown partition - rebuild to be safe
            true
        }
    }

    /// Check if a partition is frozen (no more writes expected).
    pub fn is_frozen(&self, partition_key: &str) -> bool {
        if self.frozen_partitions.contains(partition_key) {
            return true;
        }

        if let Some(meta) = self.partition_metadata.get(partition_key) {
            !self.is_hot_partition(meta)
        } else {
            false
        }
    }

    /// Check if a partition is hot (recently modified).
    fn is_hot_partition(&self, meta: &PartitionMetadata) -> bool {
        let now = Utc::now();

        // If we have a mutability strategy, use it
        if let Some(strategy) = &self.config.mutability_strategy {
            return strategy.is_mutable(meta.last_modified, now);
        }

        // Fall back to threshold-based check
        let threshold = now - self.config.freeze_threshold;
        meta.last_modified > threshold
    }

    /// Check if a partition is mutable based on its partition date (not last modified).
    ///
    /// This is useful when the partition key contains date information that determines
    /// mutability (e.g., "year=2024/month=12/day=15").
    pub fn is_partition_mutable_by_date(&self, partition_key: &str) -> bool {
        let now = Utc::now();

        // If we have a mutability strategy, use it with partition date
        if let Some(strategy) = &self.config.mutability_strategy {
            // Try to parse the partition date
            if let Some(partition_time) = parse_partition_date(partition_key) {
                return strategy.is_mutable(partition_time, now);
            }
        }

        // Can't determine - assume mutable for safety
        true
    }

    /// Mark a partition as permanently frozen.
    ///
    /// Call this after successfully merging a partition's FSTs to prevent
    /// future rebuilds.
    pub fn mark_frozen(&mut self, partition_key: impl Into<String>) {
        let key = partition_key.into();
        self.frozen_partitions.insert(key.clone());
        if let Some(meta) = self.partition_metadata.get_mut(&key) {
            meta.merged = true;
        }
    }

    /// Check if a partition needs its per-file FSTs merged into a single partition FST.
    pub fn needs_merge(&self, partition_key: &str) -> bool {
        if let Some(meta) = self.partition_metadata.get(partition_key) {
            // Merge if:
            // 1. Not already merged
            // 2. Has enough files to be worth merging
            // 3. Is frozen (no more updates expected)
            !meta.merged
                && meta.file_count >= self.config.min_files_for_merge
                && self.is_frozen(partition_key)
        } else {
            false
        }
    }

    /// Get status for all partitions.
    pub fn get_partition_statuses(&self) -> Vec<PartitionStatus> {
        self.partition_metadata
            .values()
            .map(|meta| PartitionStatus {
                partition_key: meta.partition_key.clone(),
                last_modified: meta.last_modified,
                is_frozen: self.is_frozen(&meta.partition_key),
                file_count: meta.file_count,
                needs_merge: self.needs_merge(&meta.partition_key),
                estimated_size_bytes: meta.estimated_size_bytes,
            })
            .collect()
    }

    /// Get partitions that need rebuilding.
    pub fn get_hot_partitions(&self) -> Vec<String> {
        self.partition_metadata
            .keys()
            .filter(|key| self.should_rebuild_partition(key))
            .cloned()
            .collect()
    }

    /// Get partitions that are frozen and need merging.
    pub fn get_partitions_needing_merge(&self) -> Vec<String> {
        self.partition_metadata
            .keys()
            .filter(|key| self.needs_merge(key))
            .cloned()
            .collect()
    }

    /// Get frozen partitions that can be skipped during index rebuild.
    pub fn get_frozen_partitions(&self) -> Vec<String> {
        self.partition_metadata
            .keys()
            .filter(|key| self.is_frozen(key) && !self.needs_merge(key))
            .cloned()
            .collect()
    }

    /// Clear metadata for a partition (e.g., when it's deleted).
    pub fn remove_partition(&mut self, partition_key: &str) {
        self.partition_metadata.remove(partition_key);
        self.frozen_partitions.remove(partition_key);
    }

    /// Get partition count statistics.
    pub fn stats(&self) -> SmartBuilderStats {
        let total = self.partition_metadata.len();
        let frozen = self
            .partition_metadata
            .keys()
            .filter(|k| self.is_frozen(k))
            .count();
        let hot = self.get_hot_partitions().len();
        let needs_merge = self.get_partitions_needing_merge().len();

        SmartBuilderStats {
            total_partitions: total,
            frozen_partitions: frozen,
            hot_partitions: hot,
            partitions_needing_merge: needs_merge,
        }
    }
}

/// Statistics about partition states.
#[derive(Debug, Clone)]
pub struct SmartBuilderStats {
    /// Total number of tracked partitions
    pub total_partitions: usize,
    /// Partitions that are frozen
    pub frozen_partitions: usize,
    /// Partitions that are hot (need rebuild)
    pub hot_partitions: usize,
    /// Partitions that need FST merging
    pub partitions_needing_merge: usize,
}

/// Parse a partition key into a datetime if it follows a date pattern.
///
/// Supports formats:
/// - "2025/01/15" (year/month/day)
/// - "2025/01" (year/month)
/// - "year=2025/month=01/day=15" (Hive-style)
pub fn parse_partition_date(partition_key: &str) -> Option<DateTime<Utc>> {
    // Try year/month/day format
    if let Some(date) = parse_ymd(partition_key) {
        return Some(date.and_hms_opt(0, 0, 0)?.and_utc());
    }

    // Try year/month format
    if let Some(date) = parse_ym(partition_key) {
        return Some(date.and_hms_opt(0, 0, 0)?.and_utc());
    }

    // Try Hive-style
    if let Some(date) = parse_hive_style(partition_key) {
        return Some(date.and_hms_opt(0, 0, 0)?.and_utc());
    }

    None
}

fn parse_ymd(s: &str) -> Option<chrono::NaiveDate> {
    let parts: Vec<&str> = s.split('/').collect();
    if parts.len() == 3 {
        let year: i32 = parts[0].parse().ok()?;
        let month: u32 = parts[1].parse().ok()?;
        let day: u32 = parts[2].parse().ok()?;
        chrono::NaiveDate::from_ymd_opt(year, month, day)
    } else {
        None
    }
}

fn parse_ym(s: &str) -> Option<chrono::NaiveDate> {
    let parts: Vec<&str> = s.split('/').collect();
    if parts.len() == 2 {
        let year: i32 = parts[0].parse().ok()?;
        let month: u32 = parts[1].parse().ok()?;
        chrono::NaiveDate::from_ymd_opt(year, month, 1)
    } else {
        None
    }
}

fn parse_hive_style(s: &str) -> Option<chrono::NaiveDate> {
    let mut year: Option<i32> = None;
    let mut month: Option<u32> = None;
    let mut day: Option<u32> = None;

    for part in s.split('/') {
        if let Some(val) = part.strip_prefix("year=") {
            year = val.parse().ok();
        } else if let Some(val) = part.strip_prefix("month=") {
            month = val.parse().ok();
        } else if let Some(val) = part.strip_prefix("day=") {
            day = val.parse().ok();
        }
    }

    if let (Some(y), Some(m)) = (year, month) {
        chrono::NaiveDate::from_ymd_opt(y, m, day.unwrap_or(1))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    #[test]
    fn test_smart_builder_default_config() {
        let config = SmartIndexBuilderConfig::default();
        assert_eq!(config.freeze_threshold, Duration::hours(24));
        assert_eq!(config.max_files_per_partition, 100);
        assert_eq!(config.min_files_for_merge, 10);
    }

    #[test]
    fn test_register_partition() {
        let mut builder = SmartIndexBuilder::with_defaults();
        let now = Utc::now();

        builder.register_partition("2025/01/15", now - Duration::days(2), 50);
        builder.register_partition("2025/01/17", now, 10);

        let statuses = builder.get_partition_statuses();
        assert_eq!(statuses.len(), 2);
    }

    #[test]
    fn test_frozen_partition() {
        let mut builder = SmartIndexBuilder::with_defaults();
        let now = Utc::now();

        // Old partition (frozen)
        builder.register_partition("2025/01/15", now - Duration::days(2), 50);

        // Recent partition (hot)
        builder.register_partition("2025/01/17", now, 10);

        assert!(builder.is_frozen("2025/01/15"));
        assert!(!builder.is_frozen("2025/01/17"));
    }

    #[test]
    fn test_should_rebuild() {
        let mut builder = SmartIndexBuilder::with_defaults();
        let now = Utc::now();

        // Old partition - should not rebuild
        builder.register_partition("old", now - Duration::days(2), 50);

        // Recent partition - should rebuild
        builder.register_partition("recent", now, 10);

        assert!(!builder.should_rebuild_partition("old"));
        assert!(builder.should_rebuild_partition("recent"));
    }

    #[test]
    fn test_needs_merge() {
        let mut builder = SmartIndexBuilder::with_defaults();
        let now = Utc::now();

        // Old partition with enough files - needs merge
        builder.register_partition("old-many-files", now - Duration::days(2), 50);

        // Old partition with few files - no merge needed
        builder.register_partition("old-few-files", now - Duration::days(2), 5);

        // Recent partition - no merge (still hot)
        builder.register_partition("recent", now, 50);

        assert!(builder.needs_merge("old-many-files"));
        assert!(!builder.needs_merge("old-few-files")); // Not enough files
        assert!(!builder.needs_merge("recent")); // Still hot
    }

    #[test]
    fn test_mark_frozen() {
        let mut builder = SmartIndexBuilder::with_defaults();
        let now = Utc::now();

        builder.register_partition("partition", now, 50);

        // Initially hot
        assert!(!builder.is_frozen("partition"));

        // Mark as frozen
        builder.mark_frozen("partition");

        // Now frozen, won't rebuild
        assert!(builder.is_frozen("partition"));
        assert!(!builder.should_rebuild_partition("partition"));
    }

    #[test]
    fn test_stats() {
        let mut builder = SmartIndexBuilder::with_defaults();
        let now = Utc::now();

        builder.register_partition("old1", now - Duration::days(2), 50);
        builder.register_partition("old2", now - Duration::days(3), 20);
        builder.register_partition("hot1", now, 10);

        let stats = builder.stats();
        assert_eq!(stats.total_partitions, 3);
        assert_eq!(stats.hot_partitions, 1);
        assert_eq!(stats.partitions_needing_merge, 2); // old1 and old2 need merge
    }

    #[test]
    fn test_parse_partition_date_ymd() {
        let date = parse_partition_date("2025/01/15");
        assert!(date.is_some());
        let dt = date.unwrap();
        assert_eq!(dt.year(), 2025);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 15);
    }

    #[test]
    fn test_parse_partition_date_ym() {
        let date = parse_partition_date("2025/01");
        assert!(date.is_some());
        let dt = date.unwrap();
        assert_eq!(dt.year(), 2025);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 1); // Default to 1st
    }

    #[test]
    fn test_parse_partition_date_hive() {
        let date = parse_partition_date("year=2025/month=01/day=15");
        assert!(date.is_some());
        let dt = date.unwrap();
        assert_eq!(dt.year(), 2025);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 15);
    }

    #[test]
    fn test_parse_partition_date_invalid() {
        assert!(parse_partition_date("invalid").is_none());
        assert!(parse_partition_date("abc/def/ghi").is_none());
    }
}
