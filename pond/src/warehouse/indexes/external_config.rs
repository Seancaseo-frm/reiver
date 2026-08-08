//! External Source Configuration and Partition Management
//!
//! This module provides utilities for managing external Parquet data sources:
//! - Partition pattern parsing (Hive-style patterns like "year={year}/month={month}")
//! - Mutability resolution based on time windows
//! - Integration with index refresh logic

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use regex::{Regex, RegexBuilder};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use thiserror::Error;

/// Maximum allowed pattern length to prevent DoS via very long patterns.
const MAX_PATTERN_LENGTH: usize = 1024;

/// Maximum number of placeholders allowed in a pattern.
const MAX_PLACEHOLDERS: usize = 20;

/// Maximum regex size in bytes (to limit regex compilation complexity).
/// This is set high enough to handle typical partition patterns while
/// still protecting against pathological regex attacks.
const MAX_REGEX_SIZE: usize = 10 * 1024 * 1024; // 10MB - regex crate default

use crate::warehouse::types::{ExternalSourceConfig, MutabilityStrategy, RefreshInterval};

/// Errors that can occur during external config operations.
#[derive(Debug, Error)]
pub enum ExternalConfigError {
    #[error("Invalid partition pattern: {0}")]
    InvalidPartitionPattern(String),

    #[error("Failed to parse partition value: {0}")]
    PartitionParseError(String),

    #[error("Missing partition component: {0}")]
    MissingPartitionComponent(String),

    #[error("Invalid date in partition: {0}")]
    InvalidDate(String),
}

/// Result type for external config operations.
pub type ExternalConfigResult<T> = Result<T, ExternalConfigError>;

/// Parsed partition information from a file path.
#[derive(Debug, Clone)]
pub struct ParsedPartition {
    /// Original file path.
    pub path: String,
    /// Extracted partition values (e.g., {"year": "2024", "month": "12", "day": "15"}).
    pub values: HashMap<String, String>,
    /// Parsed date from partition values (if applicable).
    pub date: Option<NaiveDate>,
    /// Parsed datetime from partition values (if applicable).
    pub datetime: Option<NaiveDateTime>,
}

impl ParsedPartition {
    /// Check if this partition is mutable according to the given strategy.
    pub fn is_mutable(&self, strategy: &MutabilityStrategy, now: DateTime<Utc>) -> bool {
        match strategy {
            MutabilityStrategy::AllImmutable => false,
            MutabilityStrategy::AllMutable => true,
            MutabilityStrategy::RollingWindow { window, unit } => {
                if let Some(date) = self.date {
                    let partition_time = date
                        .and_hms_opt(0, 0, 0)
                        .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc));

                    if let Some(pt) = partition_time {
                        let duration = unit.to_duration(*window);
                        let cutoff = now - duration;
                        pt > cutoff
                    } else {
                        // Can't parse time, assume mutable for safety
                        true
                    }
                } else {
                    // No date info, assume mutable for safety
                    true
                }
            }
            MutabilityStrategy::FileAge { .. } => {
                // FileAge requires file modification time, not partition time
                // This should be checked separately with file metadata
                true
            }
        }
    }
}

/// Parser for Hive-style partition patterns.
///
/// Supports patterns like:
/// - `year={year}/month={month}/day={day}`
/// - `dt={date}` (where date is YYYY-MM-DD)
/// - `hour={hour}`
#[derive(Debug, Clone)]
pub struct PartitionPatternParser {
    /// Original pattern string.
    pattern: String,
    /// Compiled regex for matching paths.
    regex: Regex,
    /// Named capture groups in order.
    capture_names: Vec<String>,
}

impl PartitionPatternParser {
    /// Create a new partition pattern parser.
    ///
    /// # Arguments
    /// * `pattern` - Hive-style pattern like "year={year}/month={month}/day={day}"
    ///
    /// # Supported placeholders
    /// - `{year}` - 4-digit year
    /// - `{month}` - 1-2 digit month
    /// - `{day}` - 1-2 digit day
    /// - `{hour}` - 1-2 digit hour
    /// - `{date}` - ISO date (YYYY-MM-DD)
    /// - `{datetime}` - ISO datetime (YYYY-MM-DDTHH:MM:SS)
    /// - `{*}` - Any other named capture (captures word characters)
    ///
    /// # Security
    ///
    /// Pattern complexity is limited to prevent ReDoS attacks:
    /// - Maximum pattern length: 1024 characters
    /// - Maximum placeholders: 20
    /// - Regex size limited to 8KB
    pub fn new(pattern: &str) -> ExternalConfigResult<Self> {
        // Validate pattern length
        if pattern.len() > MAX_PATTERN_LENGTH {
            return Err(ExternalConfigError::InvalidPartitionPattern(format!(
                "Pattern exceeds maximum length of {} characters",
                MAX_PATTERN_LENGTH
            )));
        }

        let mut regex_pattern = String::new();
        let mut capture_names = Vec::new();
        let mut in_placeholder = false;
        let mut placeholder = String::new();

        for ch in pattern.chars() {
            match ch {
                '{' => {
                    if in_placeholder {
                        return Err(ExternalConfigError::InvalidPartitionPattern(
                            "Nested braces not allowed".to_string(),
                        ));
                    }
                    in_placeholder = true;
                    placeholder.clear();
                }
                '}' => {
                    if !in_placeholder {
                        return Err(ExternalConfigError::InvalidPartitionPattern(
                            "Unmatched closing brace".to_string(),
                        ));
                    }
                    in_placeholder = false;

                    // Add named capture group based on placeholder type
                    // Check placeholder count limit
                    if capture_names.len() >= MAX_PLACEHOLDERS {
                        return Err(ExternalConfigError::InvalidPartitionPattern(format!(
                            "Pattern exceeds maximum of {} placeholders",
                            MAX_PLACEHOLDERS
                        )));
                    }

                    // Validate placeholder name (must be alphanumeric + underscore)
                    if !placeholder
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '_')
                    {
                        return Err(ExternalConfigError::InvalidPartitionPattern(format!(
                            "Invalid placeholder name '{}': only letters, numbers, and underscores allowed",
                            placeholder
                        )));
                    }

                    let group_pattern = match placeholder.as_str() {
                        "year" => r"(?P<year>\d{4})",
                        "month" => r"(?P<month>\d{1,2})",
                        "day" => r"(?P<day>\d{1,2})",
                        "hour" => r"(?P<hour>\d{1,2})",
                        "date" => r"(?P<date>\d{4}-\d{2}-\d{2})",
                        "datetime" => r"(?P<datetime>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})",
                        name => {
                            // Generic named capture for any other placeholder
                            capture_names.push(name.to_string());
                            regex_pattern.push_str(&format!(r"(?P<{}>\w+)", name));
                            continue;
                        }
                    };

                    capture_names.push(placeholder.clone());
                    regex_pattern.push_str(group_pattern);
                }
                '/' | '\\' => {
                    if in_placeholder {
                        placeholder.push(ch);
                    } else {
                        regex_pattern.push_str(r"[/\\]");
                    }
                }
                '.' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '^' | '$' | '|' => {
                    if in_placeholder {
                        placeholder.push(ch);
                    } else {
                        regex_pattern.push('\\');
                        regex_pattern.push(ch);
                    }
                }
                _ => {
                    if in_placeholder {
                        placeholder.push(ch);
                    } else {
                        regex_pattern.push(ch);
                    }
                }
            }
        }

        if in_placeholder {
            return Err(ExternalConfigError::InvalidPartitionPattern(
                "Unclosed placeholder".to_string(),
            ));
        }

        // Use non-greedy matching to prevent catastrophic backtracking
        // The .*? is non-greedy, which is safer than greedy .*
        let full_pattern = format!(r".*?{}.*?", regex_pattern);

        // Use RegexBuilder with size_limit to prevent ReDoS
        let regex = RegexBuilder::new(&full_pattern)
            .size_limit(MAX_REGEX_SIZE)
            .build()
            .map_err(|e| {
                ExternalConfigError::InvalidPartitionPattern(format!("Invalid regex: {}", e))
            })?;

        Ok(Self {
            pattern: pattern.to_string(),
            regex,
            capture_names,
        })
    }

    /// Parse a file path to extract partition values.
    pub fn parse(&self, path: &str) -> Option<ParsedPartition> {
        let captures = self.regex.captures(path)?;
        let mut values = HashMap::new();

        for name in &self.capture_names {
            if let Some(m) = captures.name(name) {
                values.insert(name.clone(), m.as_str().to_string());
            }
        }

        if values.is_empty() {
            return None;
        }

        // Try to parse date from captured values
        let date = self.extract_date(&values);
        let datetime = self.extract_datetime(&values);

        Some(ParsedPartition {
            path: path.to_string(),
            values,
            date,
            datetime,
        })
    }

    /// Extract a NaiveDate from partition values.
    fn extract_date(&self, values: &HashMap<String, String>) -> Option<NaiveDate> {
        // Try direct date capture first
        if let Some(date_str) = values.get("date") {
            if let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                return Some(date);
            }
        }

        // Try year/month/day components
        let year = values.get("year")?.parse::<i32>().ok()?;
        let month = values.get("month")?.parse::<u32>().ok()?;
        let day = values
            .get("day")
            .and_then(|d| d.parse::<u32>().ok())
            .unwrap_or(1);

        NaiveDate::from_ymd_opt(year, month, day)
    }

    /// Extract a NaiveDateTime from partition values.
    fn extract_datetime(&self, values: &HashMap<String, String>) -> Option<NaiveDateTime> {
        // Try direct datetime capture first
        if let Some(dt_str) = values.get("datetime") {
            if let Ok(dt) = NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%dT%H:%M:%S") {
                return Some(dt);
            }
        }

        // Build from date + hour
        let date = self.extract_date(values)?;
        let hour = values
            .get("hour")
            .and_then(|h| h.parse::<u32>().ok())
            .unwrap_or(0);

        date.and_hms_opt(hour, 0, 0)
    }

    /// Get the original pattern string.
    pub fn pattern(&self) -> &str {
        &self.pattern
    }
}

/// Resolves partition mutability based on configuration.
#[derive(Debug, Clone)]
pub struct PartitionMutabilityResolver {
    /// Mutability strategy from config.
    strategy: MutabilityStrategy,
    /// Partition pattern parser (if pattern is configured).
    parser: Option<PartitionPatternParser>,
}

impl PartitionMutabilityResolver {
    /// Create a new mutability resolver from external source config.
    pub fn from_config(config: &ExternalSourceConfig) -> ExternalConfigResult<Self> {
        let parser = config
            .partition_pattern
            .as_ref()
            .map(|p| PartitionPatternParser::new(p))
            .transpose()?;

        Ok(Self {
            strategy: config.mutability.clone(),
            parser,
        })
    }

    /// Create a resolver with a specific strategy and optional pattern.
    pub fn new(
        strategy: MutabilityStrategy,
        partition_pattern: Option<&str>,
    ) -> ExternalConfigResult<Self> {
        let parser = partition_pattern
            .map(PartitionPatternParser::new)
            .transpose()?;

        Ok(Self { strategy, parser })
    }

    /// Check if a file at the given path should be considered mutable.
    ///
    /// # Arguments
    /// * `path` - File path (may include partition directories)
    /// * `file_mtime` - File modification time (used for FileAge strategy)
    /// * `now` - Current time
    pub fn is_file_mutable(
        &self,
        path: &str,
        file_mtime: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> bool {
        match &self.strategy {
            MutabilityStrategy::AllImmutable => false,
            MutabilityStrategy::AllMutable => true,
            MutabilityStrategy::FileAge { hours } => {
                // Use file modification time
                if let Some(mtime) = file_mtime {
                    let cutoff = now - chrono::Duration::hours(*hours as i64);
                    mtime > cutoff
                } else {
                    // No mtime available, assume mutable for safety
                    true
                }
            }
            MutabilityStrategy::RollingWindow { .. } => {
                // Parse partition from path
                if let Some(ref parser) = self.parser {
                    if let Some(partition) = parser.parse(path) {
                        return partition.is_mutable(&self.strategy, now);
                    }
                }
                // Can't determine partition, assume mutable for safety
                true
            }
        }
    }

    /// Get the index of files by their mutability status.
    ///
    /// Returns (immutable_files, mutable_files).
    pub fn partition_by_mutability<'a>(
        &self,
        files: impl IntoIterator<Item = (&'a str, Option<DateTime<Utc>>)>,
        now: DateTime<Utc>,
    ) -> (Vec<&'a str>, Vec<&'a str>) {
        let mut immutable = Vec::new();
        let mut mutable = Vec::new();

        for (path, mtime) in files {
            if self.is_file_mutable(path, mtime, now) {
                mutable.push(path);
            } else {
                immutable.push(path);
            }
        }

        (immutable, mutable)
    }

    /// Check if an index refresh is needed based on configuration.
    pub fn should_refresh(
        &self,
        refresh_config: &RefreshInterval,
        last_refresh: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> bool {
        refresh_config.should_refresh(last_refresh, now)
    }

    /// Get the mutability strategy.
    pub fn strategy(&self) -> &MutabilityStrategy {
        &self.strategy
    }
}

/// Discover partitions from a list of file paths.
pub fn discover_partitions(
    pattern: &PartitionPatternParser,
    paths: impl IntoIterator<Item = impl AsRef<str>>,
) -> Vec<ParsedPartition> {
    paths
        .into_iter()
        .filter_map(|p| pattern.parse(p.as_ref()))
        .collect()
}

/// Group files by their partition date.
pub fn group_by_partition_date(
    pattern: &PartitionPatternParser,
    paths: impl IntoIterator<Item = impl AsRef<str>>,
) -> HashMap<Option<NaiveDate>, Vec<String>> {
    let mut groups: HashMap<Option<NaiveDate>, Vec<String>> = HashMap::new();

    for path in paths {
        let path_str = path.as_ref().to_string();
        let date = pattern.parse(&path_str).and_then(|p| p.date);
        groups.entry(date).or_default().push(path_str);
    }

    groups
}

// ============================================================================
// Partition Strategy Enum
// ============================================================================

/// Describes the detected (or configured) partition strategy for a set of files.
///
/// The strategy is determined once per table during index building and stored in
/// the catalog so that the query rewriter can generate appropriate partition
/// hints without re-running detection.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum PartitionStrategy {
    /// Hive-style `key=value/` directory segments (most common).
    /// Stores the pattern string so it can be re-parsed into a `PartitionPatternParser`.
    HiveStyle {
        pattern: String,
        columns: Vec<String>,
    },

    /// Synthetic partitions derived from a timestamp column's min/max statistics.
    /// Files are bucketed by `YYYY/MM` from the column's minimum value.
    ///
    /// **Scaling note:** `file_partitions` stores every file path in the JSONB
    /// column. For tables with >100 k files this will become a multi-megabyte
    /// blob causing slow DB reads/writes and TOAST overhead. A future
    /// optimization should store only the bucketing *rule* (column + granularity)
    /// and recompute keys at query time or during index load.
    TimestampBucket {
        column: String,
        /// Pre-computed mapping from file path to partition key.
        file_partitions: HashMap<String, String>,
    },

    /// Hash-based uniform bucketing for large flat datasets.
    HashBucket { num_buckets: usize },

    /// No partitioning detected — all files go into a single "default" partition.
    /// FSTs still provide per-file pruning.
    Flat,
}

impl Hash for PartitionStrategy {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            PartitionStrategy::HiveStyle { pattern, columns } => {
                pattern.hash(state);
                columns.hash(state);
            }
            PartitionStrategy::TimestampBucket {
                column,
                file_partitions,
            } => {
                column.hash(state);
                let mut entries: Vec<_> = file_partitions.iter().collect();
                entries.sort_by_key(|(k, _)| *k);
                for (k, v) in entries {
                    k.hash(state);
                    v.hash(state);
                }
            }
            PartitionStrategy::HashBucket { num_buckets } => {
                num_buckets.hash(state);
            }
            PartitionStrategy::Flat => {}
        }
    }
}

impl PartitionStrategy {
    /// Assign a partition key for a single file path using this strategy.
    ///
    /// For Hive-style strategies this compiles a regex on every call.
    /// When processing many files in a loop, prefer [`partition_key_for_with_parser`]
    /// together with [`build_parser`] to avoid repeated regex compilation.
    pub fn partition_key_for(&self, file_path: &str) -> String {
        match self {
            PartitionStrategy::HiveStyle { pattern, columns } => {
                if let Ok(parser) = PartitionPatternParser::new(pattern) {
                    return Self::hive_key_from_parser(&parser, columns, file_path);
                }
                "default".to_string()
            }
            PartitionStrategy::TimestampBucket {
                file_partitions, ..
            } => file_partitions
                .get(file_path)
                .cloned()
                .unwrap_or_else(|| "unknown".to_string()),
            PartitionStrategy::HashBucket { num_buckets } => {
                hash_bucket_key(file_path, *num_buckets)
            }
            PartitionStrategy::Flat => "default".to_string(),
        }
    }

    /// Like [`partition_key_for`] but accepts a pre-built parser to avoid
    /// regex recompilation on every call. Use [`build_parser`] to create one.
    pub fn partition_key_for_with_parser(
        &self,
        file_path: &str,
        parser: Option<&PartitionPatternParser>,
    ) -> String {
        match self {
            PartitionStrategy::HiveStyle { columns, .. } => {
                if let Some(p) = parser {
                    return Self::hive_key_from_parser(p, columns, file_path);
                }
                // Fallback: single-shot (should not happen if caller passes parser).
                self.partition_key_for(file_path)
            }
            _ => self.partition_key_for(file_path),
        }
    }

    /// Build a `PartitionPatternParser` for this strategy (only useful for
    /// `HiveStyle`; returns `None` for other variants).
    pub fn build_parser(&self) -> Option<PartitionPatternParser> {
        match self {
            PartitionStrategy::HiveStyle { pattern, .. } => {
                PartitionPatternParser::new(pattern).ok()
            }
            _ => None,
        }
    }

    /// Extract a deterministic partition key from parsed Hive values,
    /// using the ordered `columns` vec to guarantee consistent ordering.
    fn hive_key_from_parser(
        parser: &PartitionPatternParser,
        columns: &[String],
        file_path: &str,
    ) -> String {
        if let Some(parsed) = parser.parse(file_path) {
            let parts: Vec<String> = columns
                .iter()
                .filter_map(|col| parsed.values.get(col).map(|v| format!("{}={}", col, v)))
                .collect();
            if !parts.is_empty() {
                return parts.join("/");
            }
        }
        "default".to_string()
    }

    /// Short description for logging/display.
    pub fn label(&self) -> &'static str {
        match self {
            PartitionStrategy::HiveStyle { .. } => "hive-style",
            PartitionStrategy::TimestampBucket { .. } => "timestamp-bucket",
            PartitionStrategy::HashBucket { .. } => "hash-bucket",
            PartitionStrategy::Flat => "flat",
        }
    }
}

// ============================================================================
// Detection Cascade
// ============================================================================

/// Run the multi-strategy detection cascade on a set of file paths and
/// optional file statistics.
///
/// Priority order:
/// 1. **Hive-style** — cheapest to detect, most common in practice.
/// 2. **Timestamp-bucket** — requires footer stats but produces good partitions
///    when files cover different time ranges.
/// 3. **Hash-bucket** — uniform fallback for large flat datasets (>50 files).
/// 4. **Flat** — single partition, FSTs handle all pruning.
///
/// # Arguments
/// * `file_paths`  - All file keys/paths for the table.
/// * `file_stats`  - Optional file statistics (from Parquet footers). If empty,
///                   timestamp bucketing is skipped.
/// * `time_column` - Explicit timestamp column name from `ExternalSourceConfig`,
///                   or `None` for auto-detection.
pub fn detect_partition_strategy(
    file_paths: &[String],
    file_stats: &[(String, crate::warehouse::parquet_metadata::FileStats)],
    time_column: Option<&str>,
) -> PartitionStrategy {
    let path_refs: Vec<&str> = file_paths.iter().map(|s| s.as_str()).collect();

    // 1. Try Hive-style detection (cheap — only looks at path strings).
    if let Some(layout) = detect_hive_partitioning(&path_refs, 5) {
        return PartitionStrategy::HiveStyle {
            pattern: layout.parser.pattern().to_string(),
            columns: layout.columns,
        };
    }

    // 2. Try timestamp-based partitioning if we have footer stats.
    if !file_stats.is_empty() {
        if let Some((column, partitions)) =
            crate::warehouse::parquet_metadata::derive_timestamp_partitions(file_stats, time_column)
        {
            // Flatten the partition map to file_path -> partition_key.
            let mut file_partitions = HashMap::new();
            for (key, paths) in &partitions {
                for p in paths {
                    file_partitions.insert(p.clone(), key.clone());
                }
            }
            return PartitionStrategy::TimestampBucket {
                column,
                file_partitions,
            };
        }
    }

    // 3. Hash-bucket fallback for large datasets.
    if file_paths.len() > 50 {
        let target_per_bucket = 30;
        let num_buckets = (file_paths.len() / target_per_bucket).max(1);
        return PartitionStrategy::HashBucket { num_buckets };
    }

    // 4. Flat — single partition.
    PartitionStrategy::Flat
}

// ============================================================================
// Auto-Detection of Partition Schemes
// ============================================================================

/// Regex for detecting Hive-style `key=value` path segments.
/// Matches segments like `year=2024`, `country=US`, `dt=2024-01-15`.
static HIVE_SEGMENT_RE: std::sync::LazyLock<Regex> = std::sync::LazyLock::new(|| {
    // Use a non-anchored pattern so `captures_iter` can find all `key=value`
    // segments within a path. The `/` separators are not consumed as part of
    // the match, so overlapping boundary characters aren't an issue.
    Regex::new(r"(\w+)=([^/]+)").expect("invalid hive segment regex")
});

/// A detected Hive-style partition layout.
///
/// Produced by `detect_hive_partitioning` when file paths consistently contain
/// `key=value` directory segments.
#[derive(Debug, Clone)]
pub struct DetectedHiveLayout {
    /// The ordered list of partition column names (e.g., `["year", "month", "day"]`).
    pub columns: Vec<String>,
    /// A pre-built `PartitionPatternParser` for extracting values from paths.
    pub parser: PartitionPatternParser,
}

/// Examine file paths and detect Hive-style `key=value/` directory segments.
///
/// Samples up to `max_sample` paths. If at least 80% of them share the same
/// set of Hive partition columns (same names in the same order), returns a
/// `DetectedHiveLayout` with a `PartitionPatternParser` built from the
/// discovered pattern.
///
/// Returns `None` if paths are not Hive-partitioned or the pattern is
/// inconsistent across files.
pub fn detect_hive_partitioning(paths: &[&str], max_sample: usize) -> Option<DetectedHiveLayout> {
    if paths.is_empty() {
        return None;
    }

    let sample_size = paths.len().min(max_sample);
    let sample = &paths[..sample_size];

    // Extract column names from each sampled path.
    let mut pattern_counts: HashMap<Vec<String>, usize> = HashMap::new();

    for path in sample {
        // Strip the filename (last segment) to avoid matching `key=value`
        // patterns that might appear in filenames (e.g. `report_type=daily.parquet`).
        let dir_part = match path.rfind('/') {
            Some(pos) => &path[..pos],
            None => continue, // no directory component — skip
        };

        let columns: Vec<String> = HIVE_SEGMENT_RE
            .captures_iter(dir_part)
            .map(|cap| cap[1].to_string())
            .collect();

        if !columns.is_empty() {
            *pattern_counts.entry(columns).or_insert(0) += 1;
        }
    }

    if pattern_counts.is_empty() {
        return None;
    }

    // Find the most common column pattern.
    let (best_columns, best_count) = pattern_counts.into_iter().max_by_key(|(_, count)| *count)?;

    // Require >= 80% agreement.
    let threshold = (sample_size as f64 * 0.8).ceil() as usize;
    if best_count < threshold {
        return None;
    }

    // Build a PartitionPatternParser from the discovered columns.
    // Map well-known column names to typed placeholders.
    let pattern_segments: Vec<String> = best_columns
        .iter()
        .map(|col| {
            let placeholder = match col.as_str() {
                "year" | "yr" => "{year}",
                "month" | "mo" => "{month}",
                "day" | "dd" => "{day}",
                "hour" | "hr" => "{hour}",
                "date" | "dt" => "{date}",
                "datetime" => "{datetime}",
                other => return format!("{}={{{}}}", other, other),
            };
            format!("{}={}", col, placeholder)
        })
        .collect();

    let pattern_str = pattern_segments.join("/");
    let parser = PartitionPatternParser::new(&pattern_str).ok()?;

    Some(DetectedHiveLayout {
        columns: best_columns,
        parser,
    })
}

// ============================================================================
// Hash-Bucketed Partitioning (Fallback)
// ============================================================================

/// Assign files to hash-based buckets for uniform partition distribution.
///
/// When no natural partition structure is detected, this provides synthetic
/// partitions so the `HierarchicalSkipIndex` can still group files into
/// manageable clusters rather than a single flat list.
///
/// # Arguments
/// * `file_paths` - All file paths to partition.
/// * `target_files_per_bucket` - Desired number of files per bucket (default ~30).
///
/// # Returns
/// A map from partition key (`"bucket_0"`, `"bucket_1"`, ...) to file paths.
pub fn hash_bucket_partitions(
    file_paths: &[String],
    target_files_per_bucket: usize,
) -> HashMap<String, Vec<String>> {
    if file_paths.is_empty() {
        return HashMap::new();
    }

    let target = if target_files_per_bucket == 0 {
        30
    } else {
        target_files_per_bucket
    };
    let num_buckets = (file_paths.len() / target).max(1);

    let mut buckets: HashMap<String, Vec<String>> = HashMap::new();

    for path in file_paths {
        // Hash just the filename (last segment) for deterministic assignment.
        let filename = path.rsplit('/').next().unwrap_or(path);
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        filename.hash(&mut hasher);
        let bucket_idx = (hasher.finish() as usize) % num_buckets;

        let key = format!("bucket_{}", bucket_idx);
        buckets.entry(key).or_default().push(path.clone());
    }

    buckets
}

/// Assign a single file to its hash bucket key.
///
/// This is the per-file equivalent of `hash_bucket_partitions` for use during
/// index building where files are processed one at a time.
pub fn hash_bucket_key(file_path: &str, num_buckets: usize) -> String {
    let filename = file_path.rsplit('/').next().unwrap_or(file_path);
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    filename.hash(&mut hasher);
    let bucket_idx = (hasher.finish() as usize) % num_buckets.max(1);
    format!("bucket_{}", bucket_idx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::warehouse::types::TimeUnit;

    #[test]
    fn test_hive_style_partition_parsing() {
        let parser = PartitionPatternParser::new("year={year}/month={month}/day={day}").unwrap();

        let partition = parser.parse("s3://bucket/data/year=2024/month=12/day=15/file.parquet");
        assert!(partition.is_some());

        let p = partition.unwrap();
        assert_eq!(p.values.get("year"), Some(&"2024".to_string()));
        assert_eq!(p.values.get("month"), Some(&"12".to_string()));
        assert_eq!(p.values.get("day"), Some(&"15".to_string()));
        assert_eq!(p.date, Some(NaiveDate::from_ymd_opt(2024, 12, 15).unwrap()));
    }

    #[test]
    fn test_date_partition_parsing() {
        let parser = PartitionPatternParser::new("dt={date}").unwrap();

        let partition = parser.parse("data/dt=2024-12-15/file.parquet");
        assert!(partition.is_some());

        let p = partition.unwrap();
        assert_eq!(p.values.get("date"), Some(&"2024-12-15".to_string()));
        assert_eq!(p.date, Some(NaiveDate::from_ymd_opt(2024, 12, 15).unwrap()));
    }

    #[test]
    fn test_partition_mutability_rolling_window() {
        let parser = PartitionPatternParser::new("year={year}/month={month}/day={day}").unwrap();
        let now = DateTime::parse_from_rfc3339("2024-12-15T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let strategy = MutabilityStrategy::RollingWindow {
            window: 1,
            unit: TimeUnit::Day,
        };

        // Today's partition should be mutable
        let today = parser
            .parse("data/year=2024/month=12/day=15/file.parquet")
            .unwrap();
        assert!(today.is_mutable(&strategy, now));

        // Yesterday's partition should be immutable (outside 1-day window)
        let yesterday = parser
            .parse("data/year=2024/month=12/day=13/file.parquet")
            .unwrap();
        assert!(!yesterday.is_mutable(&strategy, now));
    }

    #[test]
    fn test_partition_mutability_all_immutable() {
        let parser = PartitionPatternParser::new("year={year}/month={month}/day={day}").unwrap();
        let now = Utc::now();

        let strategy = MutabilityStrategy::AllImmutable;

        let partition = parser
            .parse("data/year=2024/month=12/day=15/file.parquet")
            .unwrap();
        assert!(!partition.is_mutable(&strategy, now));
    }

    #[test]
    fn test_partition_mutability_all_mutable() {
        let parser = PartitionPatternParser::new("year={year}/month={month}/day={day}").unwrap();
        let now = Utc::now();

        let strategy = MutabilityStrategy::AllMutable;

        let partition = parser
            .parse("data/year=2020/month=01/day=01/file.parquet")
            .unwrap();
        assert!(partition.is_mutable(&strategy, now));
    }

    #[test]
    fn test_resolver_file_age_strategy() {
        let resolver =
            PartitionMutabilityResolver::new(MutabilityStrategy::FileAge { hours: 24 }, None)
                .unwrap();

        let now = Utc::now();
        let recent_mtime = now - chrono::Duration::hours(12);
        let old_mtime = now - chrono::Duration::hours(48);

        assert!(resolver.is_file_mutable("any/path.parquet", Some(recent_mtime), now));
        assert!(!resolver.is_file_mutable("any/path.parquet", Some(old_mtime), now));
    }

    #[test]
    fn test_partition_by_mutability() {
        let resolver = PartitionMutabilityResolver::new(
            MutabilityStrategy::RollingWindow {
                window: 1,
                unit: TimeUnit::Day,
            },
            Some("year={year}/month={month}/day={day}"),
        )
        .unwrap();

        let now = DateTime::parse_from_rfc3339("2024-12-15T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let files = vec![
            ("data/year=2024/month=12/day=15/file1.parquet", None),
            ("data/year=2024/month=12/day=14/file2.parquet", None),
            ("data/year=2024/month=12/day=13/file3.parquet", None),
        ];

        let (immutable, mutable) = resolver.partition_by_mutability(files, now);

        // With a 1-day rolling window at 12:00 on Dec 15:
        // cutoff = Dec 14 at 12:00
        // Dec 15 (at 00:00) > cutoff -> mutable
        // Dec 14 (at 00:00) < cutoff -> immutable
        // Dec 13 (at 00:00) < cutoff -> immutable
        assert_eq!(mutable.len(), 1); // Only day 15 is within 1-day window from 12:00
        assert_eq!(immutable.len(), 2); // day 14 and 13 are outside
    }

    #[test]
    fn test_group_by_partition_date() {
        let parser = PartitionPatternParser::new("year={year}/month={month}/day={day}").unwrap();

        let paths = vec![
            "data/year=2024/month=12/day=15/file1.parquet",
            "data/year=2024/month=12/day=15/file2.parquet",
            "data/year=2024/month=12/day=14/file3.parquet",
        ];

        let groups = group_by_partition_date(&parser, paths);

        let date_15 = NaiveDate::from_ymd_opt(2024, 12, 15);
        let date_14 = NaiveDate::from_ymd_opt(2024, 12, 14);

        assert_eq!(groups.get(&date_15).map(|v| v.len()), Some(2));
        assert_eq!(groups.get(&date_14).map(|v| v.len()), Some(1));
    }

    // ========================================================================
    // Hive-style auto-detection tests
    // ========================================================================

    #[test]
    fn test_detect_hive_partitioning_standard_layout() {
        let paths = vec![
            "s3://bucket/data/year=2024/month=01/file1.parquet",
            "s3://bucket/data/year=2024/month=02/file2.parquet",
            "s3://bucket/data/year=2024/month=03/file3.parquet",
            "s3://bucket/data/year=2024/month=04/file4.parquet",
            "s3://bucket/data/year=2024/month=05/file5.parquet",
        ];

        let result = detect_hive_partitioning(&paths, 5);
        assert!(result.is_some(), "Should detect hive partitioning");

        let layout = result.unwrap();
        assert_eq!(layout.columns, vec!["year", "month"]);

        // The parser should extract values correctly
        let parsed = layout.parser.parse(paths[0]);
        assert!(parsed.is_some());
        let p = parsed.unwrap();
        assert_eq!(p.values.get("year"), Some(&"2024".to_string()));
        assert_eq!(p.values.get("month"), Some(&"01".to_string()));
    }

    #[test]
    fn test_detect_hive_partitioning_with_custom_columns() {
        let paths = vec![
            "data/country=US/region=west/file1.parquet",
            "data/country=US/region=east/file2.parquet",
            "data/country=UK/region=south/file3.parquet",
            "data/country=DE/region=north/file4.parquet",
            "data/country=FR/region=central/file5.parquet",
        ];

        let result = detect_hive_partitioning(&paths, 5);
        assert!(result.is_some());

        let layout = result.unwrap();
        assert_eq!(layout.columns, vec!["country", "region"]);
    }

    #[test]
    fn test_detect_hive_partitioning_flat_paths() {
        let paths = vec![
            "data/file1.parquet",
            "data/file2.parquet",
            "data/file3.parquet",
        ];

        let result = detect_hive_partitioning(&paths, 5);
        assert!(
            result.is_none(),
            "Flat paths should not be detected as Hive"
        );
    }

    #[test]
    fn test_detect_hive_partitioning_mixed_paths() {
        // Only 2 out of 5 have Hive segments — below 80% threshold
        let paths = vec![
            "data/year=2024/month=01/file1.parquet",
            "data/year=2024/month=02/file2.parquet",
            "data/flat/file3.parquet",
            "data/flat/file4.parquet",
            "data/flat/file5.parquet",
        ];

        let result = detect_hive_partitioning(&paths, 5);
        assert!(
            result.is_none(),
            "Mixed paths should not pass 80% threshold"
        );
    }

    #[test]
    fn test_detect_hive_partitioning_empty() {
        let paths: Vec<&str> = vec![];
        let result = detect_hive_partitioning(&paths, 5);
        assert!(result.is_none());
    }

    // ========================================================================
    // Hash bucket tests
    // ========================================================================

    #[test]
    fn test_hash_bucket_partitions_distribution() {
        let paths: Vec<String> = (0..100)
            .map(|i| format!("data/file_{}.parquet", i))
            .collect();

        let buckets = hash_bucket_partitions(&paths, 30);

        // With 100 files and target 30/bucket, expect ~3-4 buckets
        assert!(buckets.len() >= 2, "Should have multiple buckets");
        assert!(buckets.len() <= 10, "Should not have too many buckets");

        // All files should be assigned
        let total: usize = buckets.values().map(|v| v.len()).sum();
        assert_eq!(total, 100);
    }

    #[test]
    fn test_hash_bucket_partitions_small_dataset() {
        let paths: Vec<String> = (0..5).map(|i| format!("data/file_{}.parquet", i)).collect();

        let buckets = hash_bucket_partitions(&paths, 30);

        // With only 5 files, should get 1 bucket (5/30 rounds down to 0, clamped to 1)
        assert_eq!(buckets.len(), 1);
        let total: usize = buckets.values().map(|v| v.len()).sum();
        assert_eq!(total, 5);
    }

    #[test]
    fn test_hash_bucket_key_deterministic() {
        let key1 = hash_bucket_key("data/file_42.parquet", 10);
        let key2 = hash_bucket_key("data/file_42.parquet", 10);
        assert_eq!(key1, key2, "Same file should always map to same bucket");
    }

    #[test]
    fn test_hash_bucket_partitions_empty() {
        let paths: Vec<String> = vec![];
        let buckets = hash_bucket_partitions(&paths, 30);
        assert!(buckets.is_empty());
    }

    // ========================================================================
    // PartitionStrategy tests
    // ========================================================================

    #[test]
    fn test_partition_strategy_hive_key_assignment() {
        let strategy = PartitionStrategy::HiveStyle {
            pattern: "year={year}/month={month}".to_string(),
            columns: vec!["year".to_string(), "month".to_string()],
        };

        let key = strategy.partition_key_for("s3://bucket/data/year=2024/month=06/file.parquet");
        // The key should contain the partition values
        assert!(
            key.contains("year=2024"),
            "Key should contain year=2024, got: {}",
            key
        );
        assert!(
            key.contains("month=06"),
            "Key should contain month=06, got: {}",
            key
        );
    }

    #[test]
    fn test_partition_strategy_hash_bucket_key() {
        let strategy = PartitionStrategy::HashBucket { num_buckets: 5 };

        let key = strategy.partition_key_for("data/file_1.parquet");
        assert!(
            key.starts_with("bucket_"),
            "Key should be bucket_N, got: {}",
            key
        );
    }

    #[test]
    fn test_partition_strategy_flat_key() {
        let strategy = PartitionStrategy::Flat;
        let key = strategy.partition_key_for("data/file.parquet");
        assert_eq!(key, "default");
    }

    #[test]
    fn test_partition_strategy_timestamp_bucket_key() {
        let mut file_partitions = HashMap::new();
        file_partitions.insert("data/file1.parquet".to_string(), "2024/01".to_string());
        file_partitions.insert("data/file2.parquet".to_string(), "2024/02".to_string());

        let strategy = PartitionStrategy::TimestampBucket {
            column: "event_ts".to_string(),
            file_partitions,
        };

        assert_eq!(strategy.partition_key_for("data/file1.parquet"), "2024/01");
        assert_eq!(strategy.partition_key_for("data/file2.parquet"), "2024/02");
        assert_eq!(
            strategy.partition_key_for("data/unknown.parquet"),
            "unknown"
        );
    }

    #[test]
    fn test_partition_strategy_serde_roundtrip() {
        let strategies = vec![
            PartitionStrategy::HiveStyle {
                pattern: "year={year}/month={month}".to_string(),
                columns: vec!["year".to_string(), "month".to_string()],
            },
            PartitionStrategy::TimestampBucket {
                column: "event_ts".to_string(),
                file_partitions: HashMap::new(),
            },
            PartitionStrategy::HashBucket { num_buckets: 10 },
            PartitionStrategy::Flat,
        ];

        for strategy in strategies {
            let json = serde_json::to_string(&strategy).expect("serialize");
            let restored: PartitionStrategy = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(strategy.label(), restored.label());
        }
    }

    // ========================================================================
    // Detection cascade tests
    // ========================================================================

    #[test]
    fn test_detect_partition_strategy_hive_wins() {
        let paths: Vec<String> = (0..10)
            .map(|i| {
                format!(
                    "data/year=2024/month={:02}/file_{}.parquet",
                    (i % 12) + 1,
                    i
                )
            })
            .collect();

        let strategy = detect_partition_strategy(&paths, &[], None);
        assert_eq!(strategy.label(), "hive-style");
    }

    #[test]
    fn test_detect_partition_strategy_flat_small_dataset() {
        let paths: Vec<String> = (0..10)
            .map(|i| format!("data/file_{}.parquet", i))
            .collect();

        let strategy = detect_partition_strategy(&paths, &[], None);
        assert_eq!(strategy.label(), "flat");
    }

    #[test]
    fn test_detect_partition_strategy_hash_bucket_large_flat() {
        let paths: Vec<String> = (0..100)
            .map(|i| format!("data/file_{}.parquet", i))
            .collect();

        let strategy = detect_partition_strategy(&paths, &[], None);
        assert_eq!(strategy.label(), "hash-bucket");
    }
}
