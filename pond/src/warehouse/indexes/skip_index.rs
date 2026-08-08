//! Data Skipping Index
//!
//! FST-based indexes to skip Parquet files that don't contain matching values.
//!
//! PERFORMANCE: For TB-scale data with millions of files, use `HierarchicalSkipIndex`
//! which organizes files by partition for fast partition pruning before file filtering.
//!
//! MEMORY SAFETY: The index automatically skips building summaries for high-cardinality
//! columns (> MAX_SUMMARY_CARDINALITY) to prevent OOM during FST union operations.
//!
//! ## Numeric Column Support
//!
//! For numeric columns (Int32, Int64, Float64), we track min/max statistics per file
//! rather than FST sets. This enables efficient range filtering:
//! - `WHERE amount >= 1000` - skip files where max(amount) < 1000
//! - `WHERE price < 50` - skip files where min(price) >= 50

use fst::set::OpBuilder;
use fst::{Automaton, IntoStreamer, Set, SetBuilder, Streamer};
use regex_automata::dfa::{dense, Automaton as _};
use regex_automata::util::syntax;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use thiserror::Error;

use super::fst_backing::FstBacking;

// ============================================================================
// Substring Automaton (wraps regex-automata DFA for fst::Automaton)
// ============================================================================

/// An FST-compatible automaton for substring matching.
///
/// Wraps a `regex-automata` dense DFA built from `(?-u).*<escaped>.*` so that
/// `fst::Set::search` can prune branches via `can_match` (dead-state detection).
pub struct SubstringAutomaton {
    dfa: dense::DFA<Vec<u32>>,
    start: regex_automata::util::primitives::StateID,
}

impl SubstringAutomaton {
    /// Build a substring automaton for the given literal.
    ///
    /// Returns `None` if the DFA cannot be built (e.g., pattern too complex).
    pub fn new(substring: &str) -> Option<Self> {
        let escaped = regex_escape(substring);
        let pattern = format!(".*{}.*", escaped);

        let dfa = dense::DFA::builder()
            .syntax(syntax::Config::new().unicode(false).utf8(false).dot_matches_new_line(true))
            .configure(
                dense::DFA::config()
                    .match_kind(regex_automata::MatchKind::All)
                    .dfa_size_limit(Some(256 * 1024)), // 256 KB cap
            )
            .build(&pattern)
            .ok()?;

        let input = regex_automata::Input::new(b"" as &[u8]);
        let start = dfa.start_state_forward(&input).ok()?;
        Some(Self { dfa, start })
    }
}

impl Automaton for SubstringAutomaton {
    type State = regex_automata::util::primitives::StateID;

    fn start(&self) -> Self::State {
        self.start
    }

    fn is_match(&self, state: &Self::State) -> bool {
        self.dfa.is_match_state(*state)
    }

    fn can_match(&self, state: &Self::State) -> bool {
        !self.dfa.is_dead_state(*state)
    }

    fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
        self.dfa.next_state(*state, byte)
    }

    fn accept_eof(&self, state: &Self::State) -> Option<Self::State> {
        let s = self.dfa.next_eoi_state(*state);
        Some(s)
    }
}

/// Escape regex metacharacters in a literal string for use with `SubstringAutomaton`.
///
/// Targets the non-unicode, non-verbose `regex-automata` syntax config.
/// Not a general-purpose regex escaper.
fn regex_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    for c in s.chars() {
        match c {
            '\\' | '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^'
            | '$' | '-' => {
                out.push('\\');
                out.push(c);
            }
            _ => out.push(c),
        }
    }
    out
}

/// Maximum number of unique values to track in a summary FST.
/// Columns with higher cardinality (e.g., UUIDs, timestamps) are skipped
/// to prevent memory explosion during FST union operations.
const MAX_SUMMARY_CARDINALITY: usize = 100_000;

/// Returned by `build_file_pattern` when no files match the predicates.
/// Callers should check for this value and short-circuit the query
/// (return empty results) instead of passing it to ClickHouse's `s3()`.
pub const EMPTY_MATCH_PATTERN: &str = "";

/// Maximum memory budget (in bytes) for building FST summaries.
/// If exceeded, the summary build is aborted for that column.
const MAX_SUMMARY_MEMORY_BYTES: usize = 50 * 1024 * 1024; // 50MB

/// Statistics for a numeric column in a single file.
///
/// PERFORMANCE: Enables fast range filtering for numeric columns.
/// Files can be skipped when the query range doesn't overlap with [min, max].
#[derive(Debug, Clone, Serialize)]
pub struct NumericColumnStats {
    min: f64,
    max: f64,
    null_count: u64,
    value_count: u64,
}

impl<'de> serde::Deserialize<'de> for NumericColumnStats {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct Raw {
            min: f64,
            max: f64,
            null_count: u64,
            value_count: u64,
        }
        let raw = Raw::deserialize(deserializer)?;
        let (min, max) = if raw.min.is_nan() || raw.max.is_nan() {
            (f64::NEG_INFINITY, f64::INFINITY)
        } else if raw.min > raw.max {
            (raw.max, raw.min)
        } else {
            (raw.min, raw.max)
        };
        Ok(Self { min, max, null_count: raw.null_count, value_count: raw.value_count })
    }
}

impl NumericColumnStats {
    /// Create stats from a collection of values.
    pub fn from_values(values: &[f64], null_count: u64) -> Option<Self> {
        if values.is_empty() {
            return None;
        }

        let (min, max, non_nan_count) = values.iter().copied()
            .filter(|v| !v.is_nan())
            .fold((f64::INFINITY, f64::NEG_INFINITY, 0u64), |(mn, mx, cnt), v| {
                (mn.min(v), mx.max(v), cnt + 1)
            });

        if min > max {
            return None;
        }
        Some(Self {
            min,
            max,
            null_count,
            value_count: non_nan_count,
        })
    }

    /// Create stats from explicit min/max values.
    /// Returns `None` when min/max are NaN or `min > max` (which would cause false negatives).
    pub fn new(min: f64, max: f64, null_count: u64, value_count: u64) -> Option<Self> {
        if min.is_nan() || max.is_nan() || min > max {
            return None;
        }
        Some(Self {
            min,
            max,
            null_count,
            value_count,
        })
    }

    pub fn min(&self) -> f64 { self.min }
    pub fn max(&self) -> f64 { self.max }
    pub fn null_count(&self) -> u64 { self.null_count }
    pub fn value_count(&self) -> u64 { self.value_count }

    /// Construct stats without validation. Only for tests verifying defensive
    /// NaN handling in `might_contain` and `merge`.
    #[cfg(test)]
    pub fn new_unchecked(min: f64, max: f64, null_count: u64, value_count: u64) -> Self {
        Self { min, max, null_count, value_count }
    }

    /// Check if a value might be in this file based on stats.
    pub fn might_contain(&self, value: f64) -> bool {
        if value.is_nan() || self.min.is_nan() || self.max.is_nan() {
            return true;
        }
        value >= self.min && value <= self.max
    }

    /// Check if any value in a range might be in this file.
    ///
    /// Returns true if the [query_min, query_max] range overlaps with [self.min, self.max].
    pub fn might_contain_range(&self, query_min: Option<f64>, query_max: Option<f64>) -> bool {
        let query_min = query_min.unwrap_or(f64::NEG_INFINITY);
        let query_max = query_max.unwrap_or(f64::INFINITY);

        if query_min.is_nan() || query_max.is_nan() || self.min.is_nan() || self.max.is_nan() {
            return true;
        }

        query_min <= self.max && query_max >= self.min
    }

    /// Check if any value > threshold might be in this file.
    pub fn might_contain_gt(&self, threshold: f64) -> bool {
        if threshold.is_nan() || self.max.is_nan() { return true; }
        self.max > threshold
    }

    /// Check if any value >= threshold might be in this file.
    pub fn might_contain_gte(&self, threshold: f64) -> bool {
        if threshold.is_nan() || self.max.is_nan() { return true; }
        self.max >= threshold
    }

    /// Check if any value < threshold might be in this file.
    pub fn might_contain_lt(&self, threshold: f64) -> bool {
        if threshold.is_nan() || self.min.is_nan() { return true; }
        self.min < threshold
    }

    /// Check if any value <= threshold might be in this file.
    pub fn might_contain_lte(&self, threshold: f64) -> bool {
        if threshold.is_nan() || self.min.is_nan() { return true; }
        self.min <= threshold
    }

    /// Merge stats from another file (for aggregated stats).
    ///
    /// NaN on either side is treated as "unknown bounds" and produces
    /// conservative infinite bounds to avoid false negatives.
    pub fn merge(&mut self, other: &NumericColumnStats) {
        if self.min.is_nan() || other.min.is_nan() {
            self.min = f64::NEG_INFINITY;
        } else {
            self.min = self.min.min(other.min);
        }
        if self.max.is_nan() || other.max.is_nan() {
            self.max = f64::INFINITY;
        } else {
            self.max = self.max.max(other.max);
        }
        self.null_count = self.null_count.saturating_add(other.null_count);
        self.value_count = self.value_count.saturating_add(other.value_count);
    }
}

/// Numeric statistics index for a single file.
///
/// Tracks min/max for all numeric columns in a file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileNumericIndex {
    /// File path
    pub file_path: String,
    /// Statistics per column
    pub columns: HashMap<String, NumericColumnStats>,
}

impl FileNumericIndex {
    /// Create a new empty numeric index for a file.
    pub fn new(file_path: &str) -> Self {
        Self {
            file_path: file_path.to_string(),
            columns: HashMap::new(),
        }
    }

    /// Add stats for a column.
    pub fn add_column(&mut self, column: &str, stats: NumericColumnStats) {
        self.columns.insert(column.to_string(), stats);
    }

    /// Check if the file might contain a value in a column.
    pub fn might_contain(&self, column: &str, value: f64) -> bool {
        match self.columns.get(column) {
            Some(stats) => stats.might_contain(value),
            None => true, // No stats = assume might contain
        }
    }

    /// Check if the file might contain values in a range.
    pub fn might_contain_range(
        &self,
        column: &str,
        min_value: Option<f64>,
        max_value: Option<f64>,
    ) -> bool {
        match self.columns.get(column) {
            Some(stats) => stats.might_contain_range(min_value, max_value),
            None => true,
        }
    }
}

/// Numeric range predicate for filtering.
#[derive(Debug, Clone)]
pub struct NumericRangePredicate {
    /// Minimum value (inclusive), or None for unbounded
    pub min_value: Option<f64>,
    /// Maximum value (inclusive), or None for unbounded
    pub max_value: Option<f64>,
}

impl NumericRangePredicate {
    /// Create a >= predicate.
    pub fn gte(value: f64) -> Self {
        Self {
            min_value: Some(value),
            max_value: None,
        }
    }

    /// Create a <= predicate.
    pub fn lte(value: f64) -> Self {
        Self {
            min_value: None,
            max_value: Some(value),
        }
    }

    /// Create a BETWEEN predicate.
    pub fn between(min: f64, max: f64) -> Self {
        Self {
            min_value: Some(min),
            max_value: Some(max),
        }
    }
}

/// Index for numeric columns across multiple files.
#[derive(Debug, Clone, Default)]
pub struct DataNumericIndex {
    /// Numeric indexes per file
    file_indexes: HashMap<String, FileNumericIndex>,
}

impl DataNumericIndex {
    /// Create a new empty numeric index.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a file's numeric index.
    pub fn add_file(&mut self, index: FileNumericIndex) {
        self.file_indexes.insert(index.file_path.clone(), index);
    }

    /// Remove a file's numeric index.
    pub fn remove_file(&mut self, file_path: &str) {
        self.file_indexes.remove(file_path);
    }

    /// Filter files by numeric range predicates.
    ///
    /// Returns files where the column values might fall within the range.
    pub fn filter_files_by_numeric_range(
        &self,
        predicates: &HashMap<String, NumericRangePredicate>,
    ) -> Vec<&str> {
        if predicates.is_empty() {
            return self.file_paths();
        }

        let total_files = self.file_indexes.len();

        let matching: Vec<&str> = self
            .file_indexes
            .values()
            .filter(|idx| {
                predicates.iter().all(|(col, pred)| {
                    idx.might_contain_range(col, pred.min_value, pred.max_value)
                })
            })
            .map(|idx| idx.file_path.as_str())
            .collect();

        let files_skipped = total_files - matching.len();

        tracing::debug!(
            total_files = total_files,
            matching_files = matching.len(),
            files_skipped = files_skipped,
            predicate_count = predicates.len(),
            "Numeric index filtered files by range predicates"
        );

        matching
    }

    /// Get all file paths.
    pub fn file_paths(&self) -> Vec<&str> {
        self.file_indexes.keys().map(|s| s.as_str()).collect()
    }

    /// Get total number of files.
    pub fn total_files(&self) -> usize {
        self.file_indexes.len()
    }

    /// Get aggregated stats for a column across all files.
    pub fn aggregated_stats(&self, column: &str) -> Option<NumericColumnStats> {
        let mut result: Option<NumericColumnStats> = None;

        for idx in self.file_indexes.values() {
            if let Some(stats) = idx.columns.get(column) {
                match &mut result {
                    Some(agg) => agg.merge(stats),
                    None => result = Some(stats.clone()),
                }
            }
        }

        result
    }
}

/// Errors that can occur during skip index operations.
#[derive(Debug, Error)]
pub enum SkipIndexError {
    #[error("FST error: {0}")]
    Fst(#[from] fst::Error),
    
    #[error("FST data exceeds memory limit: {size} bytes > {limit} bytes")]
    MemoryLimitExceeded { size: usize, limit: usize },
}

/// Result type for skip index operations.
pub type SkipIndexResult<T> = Result<T, SkipIndexError>;

/// Index for a single file's column values.
///
/// Note: Manual Debug implementation because `fst::Set` doesn't implement Debug.
pub struct FileSkipIndex {
    /// File path
    pub file_path: String,
    /// FST sets for each indexed column: column_name -> unique values
    pub column_values: HashMap<String, Set<FstBacking>>,
}

impl std::fmt::Debug for FileSkipIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileSkipIndex")
            .field("file_path", &self.file_path)
            .field("indexed_columns", &self.column_values.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl FileSkipIndex {
    /// Build a skip index for a file.
    pub fn build(
        file_path: &str,
        columns: HashMap<String, Vec<String>>,
    ) -> SkipIndexResult<Self> {
        let mut column_values = HashMap::new();

        for (column_name, values) in columns {
            let mut sorted_values = values;
            sorted_values.sort();
            sorted_values.dedup();

            let mut builder = SetBuilder::memory();
            for value in &sorted_values {
                builder.insert(value)?;
            }

            let raw = builder.into_inner()?;
            column_values.insert(column_name, Set::new(FstBacking::Owned(raw))?);
        }

        Ok(Self {
            file_path: file_path.to_string(),
            column_values,
        })
    }
    
    /// Build a skip index from pre-serialized FST data.
    ///
    /// This is used when loading skip indexes from the database where
    /// the FST has already been serialized to bytes.
    ///
    /// # Arguments
    /// * `file_path` - The path to the Parquet file
    /// * `column_name` - The column this FST indexes
    /// * `fst_bytes` - The serialized FST bytes
    ///
    /// # Returns
    /// A FileSkipIndex with a single column, or an error if:
    /// - FST deserialization fails
    /// - FST data exceeds memory limit (prevents OOM from corrupted data)
    ///
    /// # Memory Safety
    /// 
    /// This method validates that the FST data doesn't exceed `MAX_SUMMARY_MEMORY_BYTES`
    /// before deserializing. This prevents OOM attacks or corrupted data from crashing
    /// the process.
    pub fn from_serialized_fst(
        file_path: &str,
        column_name: &str,
        fst_backing: FstBacking,
    ) -> SkipIndexResult<Self> {
        let size = fst_backing.as_ref().len();
        if size > MAX_SUMMARY_MEMORY_BYTES {
            tracing::warn!(
                file_path = file_path,
                column = column_name,
                size = size,
                limit = MAX_SUMMARY_MEMORY_BYTES,
                "FST data exceeds memory limit, rejecting to prevent OOM"
            );
            return Err(SkipIndexError::MemoryLimitExceeded {
                size,
                limit: MAX_SUMMARY_MEMORY_BYTES,
            });
        }
        
        let fst_set = Set::new(fst_backing)?;
        let mut column_values = HashMap::new();
        column_values.insert(column_name.to_string(), fst_set);
        
        Ok(Self {
            file_path: file_path.to_string(),
            column_values,
        })
    }
    
    /// Add a column's FST to an existing FileSkipIndex.
    ///
    /// This allows building up a FileSkipIndex with multiple columns
    /// when loading from the database.
    ///
    /// # Memory Safety
    /// 
    /// This method validates that the FST data doesn't exceed `MAX_SUMMARY_MEMORY_BYTES`
    /// before deserializing.
    pub fn add_column_fst(
        &mut self,
        column_name: &str,
        fst_backing: FstBacking,
    ) -> SkipIndexResult<()> {
        let size = fst_backing.as_ref().len();
        if size > MAX_SUMMARY_MEMORY_BYTES {
            tracing::warn!(
                file_path = %self.file_path,
                column = column_name,
                size = size,
                limit = MAX_SUMMARY_MEMORY_BYTES,
                "FST data exceeds memory limit, rejecting to prevent OOM"
            );
            return Err(SkipIndexError::MemoryLimitExceeded {
                size,
                limit: MAX_SUMMARY_MEMORY_BYTES,
            });
        }
        
        let fst_set = Set::new(fst_backing)?;
        self.column_values.insert(column_name.to_string(), fst_set);
        Ok(())
    }
    
    /// Create an empty FileSkipIndex for a given file path.
    ///
    /// Use `add_column_fst` to add columns afterwards.
    pub fn new_empty(file_path: &str) -> Self {
        Self {
            file_path: file_path.to_string(),
            column_values: HashMap::new(),
        }
    }

    /// Check if the file might contain a specific value in a column.
    pub fn might_contain(&self, column: &str, value: &str) -> bool {
        match self.column_values.get(column) {
            Some(fst) => fst.contains(value),
            None => true, // If no index, assume it might contain
        }
    }

    /// Check if the file might contain values with a prefix.
    pub fn might_contain_prefix(&self, column: &str, prefix: &str) -> bool {
        if prefix.is_empty() {
            return true;
        }
        match self.column_values.get(column) {
            Some(fst) => {
                let upper = increment_last_byte(prefix);
                if upper == prefix {
                    // All chars are char::MAX — fall back to ge-only scan
                    let mut stream = fst.range().ge(prefix).into_stream();
                    stream.next().is_some()
                } else {
                    let mut stream = fst.range().ge(prefix).lt(&upper).into_stream();
                    stream.next().is_some()
                }
            }
            None => true, // If no index, assume it might contain
        }
    }

    /// Check if the file might contain a substring in any value of a column.
    ///
    /// Uses a `regex-automata` DFA compiled to `.*substring.*` and intersected
    /// with the FST set.  Dead-state detection allows the FST walk to skip
    /// branches that can never produce a match.
    ///
    /// Returns `true` if at least one key in the FST matches (i.e., the file
    /// might contain a row whose column value includes `substring`).
    pub fn might_contain_substring(&self, column: &str, substring: &str) -> bool {
        match self.column_values.get(column) {
            Some(fst_set) => {
                let aut = match SubstringAutomaton::new(substring) {
                    Some(a) => a,
                    None => return true, // can't build DFA, assume might contain
                };
                let mut stream = fst_set.search(&aut).into_stream();
                stream.next().is_some()
            }
            None => true, // no index, assume it might contain
        }
    }

    /// Like [`might_contain_substring`](Self::might_contain_substring) but
    /// accepts a pre-built automaton, avoiding redundant DFA compilation when
    /// the same substring is checked across many files.
    pub fn might_contain_substring_with_automaton(&self, column: &str, aut: &SubstringAutomaton) -> bool {
        match self.column_values.get(column) {
            Some(fst_set) => {
                let mut stream = fst_set.search(aut).into_stream();
                stream.next().is_some()
            }
            None => true,
        }
    }

    /// Check if the file might contain values in a range.
    ///
    /// PERFORMANCE: Uses FST range queries which are O(k) where k is
    /// the length of the boundary keys.
    pub fn might_contain_range(
        &self,
        column: &str,
        min_value: Option<&str>,
        max_value: Option<&str>,
    ) -> bool {
        self.might_contain_range_ex(column, min_value, false, max_value, false)
    }

    /// Like `might_contain_range` but supports exclusive bounds.
    pub fn might_contain_range_ex(
        &self,
        column: &str,
        min_value: Option<&str>,
        min_exclusive: bool,
        max_value: Option<&str>,
        max_exclusive: bool,
    ) -> bool {
        match self.column_values.get(column) {
            Some(fst) => {
                let stream = match (min_value, max_value) {
                    (Some(min), Some(max)) => {
                        let builder = if min_exclusive {
                            fst.range().gt(min)
                        } else {
                            fst.range().ge(min)
                        };
                        if max_exclusive {
                            builder.lt(max).into_stream()
                        } else {
                            builder.le(max).into_stream()
                        }
                    }
                    (Some(min), None) => {
                        if min_exclusive {
                            fst.range().gt(min).into_stream()
                        } else {
                            fst.range().ge(min).into_stream()
                        }
                    }
                    (None, Some(max)) => {
                        if max_exclusive {
                            fst.range().lt(max).into_stream()
                        } else {
                            fst.range().le(max).into_stream()
                        }
                    }
                    (None, None) => {
                        return true;
                    }
                };
                
                let mut stream = stream;
                stream.next().is_some()
            }
            None => true,
        }
    }

    /// Check if the file might contain values greater than a threshold.
    ///
    /// Convenience method for `column > value` predicates.
    pub fn might_contain_gt(&self, column: &str, value: &str) -> bool {
        match self.column_values.get(column) {
            Some(fst) => {
                let mut stream = fst.range().gt(value).into_stream();
                stream.next().is_some()
            }
            None => true,
        }
    }

    /// Check if the file might contain values greater than or equal to a threshold.
    ///
    /// Convenience method for `column >= value` predicates.
    pub fn might_contain_gte(&self, column: &str, value: &str) -> bool {
        self.might_contain_range(column, Some(value), None)
    }

    /// Check if the file might contain values less than a threshold.
    ///
    /// Convenience method for `column < value` predicates.
    pub fn might_contain_lt(&self, column: &str, value: &str) -> bool {
        match self.column_values.get(column) {
            Some(fst) => {
                let mut stream = fst.range().lt(value).into_stream();
                stream.next().is_some()
            }
            None => true,
        }
    }

    /// Check if the file might contain values less than or equal to a threshold.
    ///
    /// Convenience method for `column <= value` predicates.
    pub fn might_contain_lte(&self, column: &str, value: &str) -> bool {
        self.might_contain_range(column, None, Some(value))
    }

    /// Check if the file might contain any of the specified values.
    ///
    /// PERFORMANCE: For large IN lists, this is O(k * avg_key_len) where k is the
    /// list size and avg_key_len is the average key length. Returns early on first match.
    ///
    /// # Arguments
    /// * `column` - Column name to check
    /// * `values` - List of values to check for
    pub fn might_contain_any(&self, column: &str, values: &[&str]) -> bool {
        if values.is_empty() {
            return false; // Empty IN list = no matches
        }

        match self.column_values.get(column) {
            Some(fst) => {
                // Check each value - return true on first match
                for value in values {
                    if fst.contains(*value) {
                        return true;
                    }
                }
                false
            }
            None => true, // If no index, assume it might contain
        }
    }
}

/// Predicate type for skip index filtering.
#[derive(Debug, Clone)]
pub enum SkipPredicate {
    /// Equality check: column = 'value'
    Equals { column: String, value: String },
    /// Prefix check: column LIKE 'prefix%'
    Prefix { column: String, prefix: String },
    /// IN check: column IN ('a', 'b', 'c')
    In { column: String, values: Vec<String> },
    /// Range check: column >= min AND column <= max (either bound may be None)
    Range {
        column: String,
        min_value: Option<String>,
        max_value: Option<String>,
    },
    /// Greater than: column > value
    GreaterThan { column: String, value: String },
    /// Greater than or equal: column >= value
    GreaterThanOrEqual { column: String, value: String },
    /// Less than: column < value
    LessThan { column: String, value: String },
    /// Less than or equal: column <= value
    LessThanOrEqual { column: String, value: String },
}

/// Range predicate for a column.
#[derive(Debug, Clone)]
pub struct RangePredicate {
    pub min_value: Option<String>,
    pub max_value: Option<String>,
    /// When true, the lower bound is exclusive (`>` instead of `>=`).
    pub min_exclusive: bool,
    /// When true, the upper bound is exclusive (`<` instead of `<=`).
    pub max_exclusive: bool,
}

impl RangePredicate {
    /// Create a new range predicate with both bounds (inclusive).
    pub fn between(min: &str, max: &str) -> Self {
        Self {
            min_value: Some(min.to_string()),
            max_value: Some(max.to_string()),
            min_exclusive: false,
            max_exclusive: false,
        }
    }

    /// Create a >= predicate.
    pub fn gte(min: &str) -> Self {
        Self {
            min_value: Some(min.to_string()),
            max_value: None,
            min_exclusive: false,
            max_exclusive: false,
        }
    }

    /// Create a > predicate.
    pub fn gt(min: &str) -> Self {
        Self {
            min_value: Some(min.to_string()),
            max_value: None,
            min_exclusive: true,
            max_exclusive: false,
        }
    }

    /// Create a <= predicate.
    pub fn lte(max: &str) -> Self {
        Self {
            min_value: None,
            max_value: Some(max.to_string()),
            min_exclusive: false,
            max_exclusive: false,
        }
    }

    /// Create a < predicate.
    pub fn lt(max: &str) -> Self {
        Self {
            min_value: None,
            max_value: Some(max.to_string()),
            min_exclusive: false,
            max_exclusive: true,
        }
    }
}

/// A collection of predicates for skip index filtering.
#[derive(Debug, Clone, Default)]
pub struct SkipPredicates {
    pub equality: HashMap<String, String>,
    pub prefix: HashMap<String, String>,
    pub in_lists: HashMap<String, Vec<String>>,
    /// Range predicates (column -> (min, max))
    pub ranges: HashMap<String, RangePredicate>,
    /// Substring predicates (column -> substrings) for `LIKE '%term%'` / `CONTAINS`.
    /// Multiple substrings per column are supported (e.g., `col LIKE '%a%' AND col LIKE '%b%'`).
    pub substring: HashMap<String, Vec<String>>,
    /// Token search predicates for `hasToken()` full-text queries.
    /// Maps column name -> tokens. All tokens must be present (AND semantics).
    pub token_search: HashMap<String, Vec<String>>,
    /// Set when mutually exclusive predicates are detected (e.g., `col = 'a' AND col = 'b'`).
    /// A contradicted predicate set is unsatisfiable and should prune all files.
    pub contradicted: bool,
}

impl SkipPredicates {
    /// Create empty predicates.
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Add an equality predicate.  If the column already has a different
    /// value the predicate set becomes contradicted (unsatisfiable).
    /// Also contradicts if an existing IN list for this column doesn't
    /// contain the value.
    pub fn add_equals(&mut self, column: &str, value: &str) {
        if let Some(existing) = self.equality.get(column) {
            if existing != value {
                self.contradicted = true;
            }
            return;
        }
        if let Some(in_list) = self.in_lists.get(column) {
            if !in_list.iter().any(|v| v == value) {
                self.contradicted = true;
                return;
            }
        }
        self.equality.insert(column.to_string(), value.to_string());
    }
    
    /// Add a prefix predicate.  If the column already has a different
    /// prefix the predicate set becomes contradicted (unsatisfiable)
    /// unless one prefix is a prefix of the other (in which case the
    /// longer/tighter one wins).
    pub fn add_prefix(&mut self, column: &str, prefix: &str) {
        if let Some(existing) = self.prefix.get(column) {
            if prefix.starts_with(existing.as_str()) {
                self.prefix.insert(column.to_string(), prefix.to_string());
            } else if !existing.starts_with(prefix) {
                self.contradicted = true;
            }
            return;
        }
        self.prefix.insert(column.to_string(), prefix.to_string());
    }
    
    /// Add an IN predicate.  If the column already has an IN list the
    /// two lists are intersected.  An empty intersection makes the
    /// predicate set contradicted.  An empty initial list is also
    /// contradicted.  If an equality predicate exists for this column
    /// and the value is not in the new list, the set is contradicted.
    pub fn add_in(&mut self, column: &str, values: Vec<String>) {
        if values.is_empty() {
            self.contradicted = true;
            return;
        }
        if let Some(eq_val) = self.equality.get(column) {
            if !values.iter().any(|v| v == eq_val) {
                self.contradicted = true;
                return;
            }
        }
        if let Some(existing) = self.in_lists.get_mut(column) {
            let new_set: HashSet<&str> = values.iter().map(|s| s.as_str()).collect();
            existing.retain(|v| new_set.contains(v.as_str()));
            if existing.is_empty() {
                self.contradicted = true;
            }
            return;
        }
        self.in_lists.insert(column.to_string(), values);
    }

    /// Add a range predicate (BETWEEN).  Tightens existing bounds rather than
    /// overwriting them, consistent with `add_gte` / `add_lte`.
    pub fn add_range(&mut self, column: &str, min: &str, max: &str) {
        self.add_gte(column, min);
        self.add_lte(column, max);
    }

    /// Add a >= predicate.  If a lower bound already exists for this column,
    /// the tighter (higher) bound is kept.
    pub fn add_gte(&mut self, column: &str, value: &str) {
        self.ranges
            .entry(column.to_string())
            .and_modify(|r| {
                Self::tighten_min(r, value, false);
            })
            .or_insert_with(|| RangePredicate::gte(value));
    }

    /// Add a > predicate.  If a lower bound already exists for this column,
    /// the tighter (higher) bound is kept.
    pub fn add_gt(&mut self, column: &str, value: &str) {
        self.ranges
            .entry(column.to_string())
            .and_modify(|r| {
                Self::tighten_min(r, value, true);
            })
            .or_insert_with(|| RangePredicate::gt(value));
    }

    /// Add a <= predicate.  If an upper bound already exists for this column,
    /// the tighter (lower) bound is kept.
    pub fn add_lte(&mut self, column: &str, value: &str) {
        self.ranges
            .entry(column.to_string())
            .and_modify(|r| {
                Self::tighten_max(r, value, false);
            })
            .or_insert_with(|| RangePredicate::lte(value));
    }

    /// Add a < predicate.  If an upper bound already exists for this column,
    /// the tighter (lower) bound is kept.
    pub fn add_lt(&mut self, column: &str, value: &str) {
        self.ranges
            .entry(column.to_string())
            .and_modify(|r| {
                Self::tighten_max(r, value, true);
            })
            .or_insert_with(|| RangePredicate::lt(value));
    }

    /// Replace the lower bound only if `new_val` is tighter (higher) than the existing one.
    fn tighten_min(r: &mut RangePredicate, new_val: &str, new_exclusive: bool) {
        let dominated = match &r.min_value {
            None => true,
            Some(existing) => {
                let cmp = new_val.cmp(existing.as_str());
                cmp == std::cmp::Ordering::Greater
                    || (cmp == std::cmp::Ordering::Equal && new_exclusive && !r.min_exclusive)
            }
        };
        if dominated {
            r.min_value = Some(new_val.to_string());
            r.min_exclusive = new_exclusive;
        }
    }

    /// Replace the upper bound only if `new_val` is tighter (lower) than the existing one.
    fn tighten_max(r: &mut RangePredicate, new_val: &str, new_exclusive: bool) {
        let dominated = match &r.max_value {
            None => true,
            Some(existing) => {
                let cmp = new_val.cmp(existing.as_str());
                cmp == std::cmp::Ordering::Less
                    || (cmp == std::cmp::Ordering::Equal && new_exclusive && !r.max_exclusive)
            }
        };
        if dominated {
            r.max_value = Some(new_val.to_string());
            r.max_exclusive = new_exclusive;
        }
    }

    /// Add a substring predicate (for `LIKE '%term%'`).
    ///
    /// Multiple substrings for the same column are accumulated (AND semantics).
    pub fn add_substring(&mut self, column: &str, substring: &str) {
        self.substring
            .entry(column.to_string())
            .or_default()
            .push(substring.to_string());
    }

    /// Add a `hasToken()` predicate. Multiple tokens for the same column
    /// use AND semantics (all must be present).
    pub fn add_token(&mut self, column: &str, token: &str) {
        self.token_search
            .entry(column.to_string())
            .or_default()
            .push(token.to_string());
    }
    
    /// Check if there are any predicates.
    pub fn is_empty(&self) -> bool {
        !self.contradicted
            && self.equality.is_empty() 
            && self.prefix.is_empty() 
            && self.in_lists.is_empty()
            && self.ranges.is_empty()
            && self.substring.is_empty()
            && self.token_search.is_empty()
    }
}

// Use shared utilities from the warehouse utils module
use crate::warehouse::utils::increment_last_byte;

// ============================================================================
// Hierarchical Skip Index
// ============================================================================

/// A partition in the hierarchical index.
///
/// Partitions group files by a common key (e.g., date, year/month).
/// Each partition has a summary index for quick filtering.
#[derive(Debug)]
pub(crate) struct PartitionIndex {
    /// Partition key (e.g., "2025/01" for date partitions)
    pub partition_key: String,
    /// Summary of all values in this partition (union of file indexes)
    /// This allows fast O(k) partition pruning before checking individual files
    pub summary: HashMap<String, Set<FstBacking>>,
    /// Files within this partition
    pub files: HashMap<String, FileSkipIndex>,
    /// Total row count estimate for this partition
    pub estimated_rows: u64,
    /// Per-file row counts for accurate adjustment on replacement
    file_row_counts: HashMap<String, u64>,
    /// Estimated cardinality per column (for memory budget tracking)
    /// Columns exceeding MAX_SUMMARY_CARDINALITY are not included in summary
    column_cardinality: HashMap<String, usize>,
    /// Columns that have been marked as high-cardinality and excluded from summary
    high_cardinality_columns: std::collections::HashSet<String>,
}

impl PartitionIndex {
    /// Create a new partition index.
    pub fn new(partition_key: impl Into<String>) -> Self {
        Self {
            partition_key: partition_key.into(),
            summary: HashMap::new(),
            files: HashMap::new(),
            estimated_rows: 0,
            file_row_counts: HashMap::new(),
            column_cardinality: HashMap::new(),
            high_cardinality_columns: std::collections::HashSet::new(),
        }
    }
    
    /// Add a file to this partition and update the summary.
    ///
    /// PERFORMANCE: Uses FST streaming union operations with memory budget tracking.
    /// High-cardinality columns (> MAX_SUMMARY_CARDINALITY) are automatically excluded
    /// from the summary to prevent OOM during FST union operations.
    ///
    /// When a column exceeds the cardinality threshold:
    /// - The summary FST for that column is removed
    /// - The column is marked as high-cardinality
    /// - Future files skip summary updates for that column
    /// - File-level indexes still work for filtering
    ///
    /// PERFORMANCE: Uses early cardinality estimation before expensive FST union
    /// to avoid processing 100K+ keys just to detect high cardinality.
    pub fn add_file(&mut self, file_index: FileSkipIndex, row_count: u64) -> SkipIndexResult<()> {
        // Always update the summary with the new file's values, even for
        // replacements.  The summary is a monotonic union -- old values from a
        // replaced file remain (acceptable false positives), but new values
        // must be added to avoid false negatives in partition pruning.
        for (column, new_fst) in &file_index.column_values {
            // Skip high-cardinality columns - they would cause memory explosion
            if self.high_cardinality_columns.contains(column) {
                tracing::trace!(
                    partition = %self.partition_key,
                    column = %column,
                    "Skipping summary update for high-cardinality column"
                );
                continue;
            }
            
            match self.summary.remove(column) {
                Some(existing_fst) => {
                    let existing_cardinality = self.column_cardinality
                        .get(column)
                        .copied()
                        .unwrap_or(0);
                    let new_cardinality = new_fst.len();
                    
                    // Upper bound: union cardinality <= sum (could be less due to overlap)
                    let estimated_combined = existing_cardinality.saturating_add(new_cardinality);
                    
                    if estimated_combined > MAX_SUMMARY_CARDINALITY {
                        tracing::info!(
                            partition = %self.partition_key,
                            column = %column,
                            existing_cardinality = existing_cardinality,
                            new_cardinality = new_cardinality,
                            threshold = MAX_SUMMARY_CARDINALITY,
                            "Early detection: estimated combined cardinality ({}) exceeds limit ({}), excluding from summary",
                            estimated_combined,
                            MAX_SUMMARY_CARDINALITY,
                        );
                        self.high_cardinality_columns.insert(column.clone());
                        self.column_cardinality.remove(column);
                        continue;
                    }
                    
                    let union_result = (|| -> Result<Option<(Set<FstBacking>, usize)>, fst::Error> {
                        let mut union_stream = OpBuilder::new()
                            .add(&existing_fst)
                            .add(new_fst)
                            .union();
                        
                        let mut builder = SetBuilder::memory();
                        let mut count = 0usize;
                        let mut estimated_memory = 0usize;
                        
                        while let Some(key) = union_stream.next() {
                            count += 1;
                            estimated_memory += key.len() + 16;
                            
                            if count > MAX_SUMMARY_CARDINALITY {
                                tracing::info!(
                                    partition = %self.partition_key,
                                    column = %column,
                                    cardinality = count,
                                    "Column exceeds cardinality limit, excluding from summary"
                                );
                                return Ok(None);
                            }
                            
                            if estimated_memory > MAX_SUMMARY_MEMORY_BYTES {
                                tracing::info!(
                                    partition = %self.partition_key,
                                    column = %column,
                                    estimated_memory_mb = estimated_memory / (1024 * 1024),
                                    "Column exceeds memory limit, excluding from summary"
                                );
                                return Ok(None);
                            }
                            
                            builder.insert(key)?;
                        }
                        
                        let raw = builder.into_inner()?;
                        Ok(Some((Set::new(FstBacking::Owned(raw))?, count)))
                    })();

                    match union_result {
                        Ok(None) => {
                            self.high_cardinality_columns.insert(column.clone());
                            self.column_cardinality.remove(column);
                        }
                        Ok(Some((new_set, count))) => {
                            self.column_cardinality.insert(column.clone(), count);
                            self.summary.insert(column.clone(), new_set);
                        }
                        Err(e) => {
                            // Restore the original FST to avoid data loss
                            self.summary.insert(column.clone(), existing_fst);
                            return Err(e.into());
                        }
                    }
                }
                None => {
                    let actual_cardinality = new_fst.len();
                    
                    if actual_cardinality > MAX_SUMMARY_CARDINALITY {
                        tracing::info!(
                            partition = %self.partition_key,
                            column = %column,
                            cardinality = actual_cardinality,
                            "Initial FST exceeds cardinality limit, excluding from summary"
                        );
                        self.high_cardinality_columns.insert(column.clone());
                    } else {
                        let bytes = new_fst.as_fst().as_bytes().to_vec();
                        let cloned = Set::new(FstBacking::Owned(bytes))?;
                        self.column_cardinality.insert(column.clone(), actual_cardinality);
                        self.summary.insert(column.clone(), cloned);
                    }
                }
            }
        }
        
        let old_rows = self.file_row_counts.get(&file_index.file_path).copied().unwrap_or(0);
        let is_new = !self.files.contains_key(&file_index.file_path);
        let path = file_index.file_path.clone();
        self.files.insert(path.clone(), file_index);
        self.file_row_counts.insert(path, row_count);
        if is_new {
            self.estimated_rows = self.estimated_rows.saturating_add(row_count);
        } else {
            self.estimated_rows = self.estimated_rows.saturating_sub(old_rows).saturating_add(row_count);
        }
        Ok(())
    }
    
    /// Check if this partition might contain a value.
    /// This is a quick O(k) check (k = value length) before drilling into files.
    pub fn might_contain(&self, column: &str, value: &str) -> bool {
        match self.summary.get(column) {
            Some(fst) => fst.contains(value),
            None => true, // No summary for this column, assume it might contain
        }
    }
    
    /// Check if this partition might contain values with a prefix.
    pub fn might_contain_prefix(&self, column: &str, prefix: &str) -> bool {
        if prefix.is_empty() {
            return true;
        }
        match self.summary.get(column) {
            Some(fst) => {
                let upper = increment_last_byte(prefix);
                if upper == prefix {
                    let mut stream = fst.range().ge(prefix).into_stream();
                    stream.next().is_some()
                } else {
                    let mut stream = fst.range().ge(prefix).lt(&upper).into_stream();
                    stream.next().is_some()
                }
            }
            None => true,
        }
    }
    
    /// Like [`might_contain_substring`](Self::might_contain_substring) but
    /// reuses a pre-built automaton.
    pub fn might_contain_substring_with_automaton(&self, column: &str, aut: &SubstringAutomaton) -> bool {
        match self.summary.get(column) {
            Some(fst_set) => {
                let mut stream = fst_set.search(aut).into_stream();
                stream.next().is_some()
            }
            None => true,
        }
    }

    /// Check if this partition might contain values in a range (FST-based).
    pub fn might_contain_range_ex(
        &self,
        column: &str,
        min_value: Option<&str>,
        min_exclusive: bool,
        max_value: Option<&str>,
        max_exclusive: bool,
    ) -> bool {
        match self.summary.get(column) {
            Some(fst) => {
                let stream = match (min_value, max_value) {
                    (Some(min), Some(max)) => {
                        let builder = if min_exclusive {
                            fst.range().gt(min)
                        } else {
                            fst.range().ge(min)
                        };
                        if max_exclusive {
                            builder.lt(max).into_stream()
                        } else {
                            builder.le(max).into_stream()
                        }
                    }
                    (Some(min), None) => {
                        if min_exclusive {
                            fst.range().gt(min).into_stream()
                        } else {
                            fst.range().ge(min).into_stream()
                        }
                    }
                    (None, Some(max)) => {
                        if max_exclusive {
                            fst.range().lt(max).into_stream()
                        } else {
                            fst.range().le(max).into_stream()
                        }
                    }
                    (None, None) => return true,
                };
                let mut stream = stream;
                stream.next().is_some()
            }
            None => true,
        }
    }

    /// Check if this partition might contain any of the specified values.
    pub fn might_contain_any(&self, column: &str, values: &[&str]) -> bool {
        if values.is_empty() {
            return false;
        }
        match self.summary.get(column) {
            Some(fst) => values.iter().any(|v| fst.contains(*v)),
            None => true,
        }
    }

}

/// Hierarchical skip index for TB-scale datasets.
///
/// PERFORMANCE: This index organizes files into partitions (e.g., by date),
/// allowing O(k) partition pruning (k = key length) before filtering individual files.
/// For a table with 1M files across 1000 date partitions, this reduces
/// the search space from O(1M) to O(1K) + O(files_in_matching_partitions).
///
/// MEMORY SAFETY: Automatically excludes high-cardinality columns from global
/// summary to prevent OOM during FST union operations.
///
/// # Example
/// ```ignore
/// let mut index = HierarchicalSkipIndex::new();
///
/// // Add files with their partition keys
/// index.add_file("2025/01", file_index_jan1, 100000)?;
/// index.add_file("2025/01", file_index_jan2, 100000)?;
/// index.add_file("2025/02", file_index_feb1, 100000)?;
///
/// // Filter with partition hint
/// let files = index.filter_with_partition_hint(
///     &predicates,
///     Some(&["2025/01"]), // Only search January
/// );
/// ```
pub struct HierarchicalSkipIndex {
    /// Partitions indexed by partition key
    partitions: HashMap<String, PartitionIndex>,
    /// Global summary across all partitions (for queries without partition filter)
    global_summary: HashMap<String, Set<FstBacking>>,
    /// Total files across all partitions
    total_files: usize,
    /// Estimated cardinality per column in global summary
    global_cardinality: HashMap<String, usize>,
    /// Columns marked as high-cardinality and excluded from global summary
    global_high_cardinality: std::collections::HashSet<String>,
}

impl HierarchicalSkipIndex {
    /// Create a new hierarchical skip index.
    pub fn new() -> Self {
        Self {
            partitions: HashMap::new(),
            global_summary: HashMap::new(),
            total_files: 0,
            global_cardinality: HashMap::new(),
            global_high_cardinality: std::collections::HashSet::new(),
        }
    }
    
    /// Add a file to a partition.
    ///
    /// # Arguments
    /// * `partition_key` - The partition this file belongs to (e.g., "2025/01")
    /// * `file_index` - The file's skip index
    /// * `row_count` - Estimated row count for this file
    ///
    /// PERFORMANCE: Uses FST streaming union operations with memory budget tracking.
    /// High-cardinality columns are automatically excluded from summaries to prevent OOM.
    pub fn add_file(
        &mut self,
        partition_key: &str,
        file_index: FileSkipIndex,
        row_count: u64,
    ) -> SkipIndexResult<()> {
        // Clone column_values for global summary update (needed because
        // partition.add_file consumes file_index, and we must add to the
        // partition first to avoid inconsistency on error).
        let column_values: Vec<(String, FstBacking)> = file_index
            .column_values
            .iter()
            .filter(|(col, _)| !self.global_high_cardinality.contains(*col))
            .map(|(col, fst)| (col.clone(), FstBacking::Owned(fst.as_fst().as_bytes().to_vec())))
            .collect();

        // Add to partition first -- if this fails, the global summary is untouched
        let partition = self.partitions
            .entry(partition_key.to_string())
            .or_insert_with(|| PartitionIndex::new(partition_key));

        let was_new = !partition.files.contains_key(&file_index.file_path);
        partition.add_file(file_index, row_count)?;
        if was_new {
            self.total_files += 1;
        }

        // Update global summary using streaming union (for non-partitioned queries)
        for (column, new_fst_backing) in column_values {
            let new_fst = Set::new(new_fst_backing.clone())?;

            match self.global_summary.remove(&column) {
                Some(existing_fst) => {
                    let union_result = (|| -> Result<Option<(Set<FstBacking>, usize)>, fst::Error> {
                        let mut union_stream = OpBuilder::new()
                            .add(&existing_fst)
                            .add(&new_fst)
                            .union();
                        
                        let mut builder = SetBuilder::memory();
                        let mut count = 0usize;
                        let mut estimated_memory = 0usize;
                        
                        while let Some(key) = union_stream.next() {
                            count += 1;
                            estimated_memory += key.len() + 16;
                            
                            if count > MAX_SUMMARY_CARDINALITY {
                                tracing::info!(
                                    column = %column,
                                    cardinality = count,
                                    "Global summary: column exceeds cardinality limit, excluding"
                                );
                                return Ok(None);
                            }
                            
                            if estimated_memory > MAX_SUMMARY_MEMORY_BYTES {
                                tracing::info!(
                                    column = %column,
                                    estimated_memory_mb = estimated_memory / (1024 * 1024),
                                    "Global summary: column exceeds memory limit, excluding"
                                );
                                return Ok(None);
                            }
                            
                            builder.insert(key)?;
                        }
                        
                        let raw = builder.into_inner()?;
                        Ok(Some((Set::new(FstBacking::Owned(raw))?, count)))
                    })();

                    match union_result {
                        Ok(None) => {
                            self.global_high_cardinality.insert(column.clone());
                            self.global_cardinality.remove(&column);
                        }
                        Ok(Some((new_set, count))) => {
                            self.global_cardinality.insert(column.clone(), count);
                            self.global_summary.insert(column.clone(), new_set);
                        }
                        Err(e) => {
                            self.global_summary.insert(column.clone(), existing_fst);
                            return Err(e.into());
                        }
                    }
                }
                None => {
                    let actual_cardinality = new_fst.len();
                    
                    if actual_cardinality > MAX_SUMMARY_CARDINALITY {
                        tracing::info!(
                            column = %column,
                            cardinality = actual_cardinality,
                            "Global summary: initial FST exceeds cardinality limit, excluding"
                        );
                        self.global_high_cardinality.insert(column.clone());
                    } else {
                        let cloned = Set::new(new_fst_backing)?;
                        self.global_cardinality.insert(column.clone(), actual_cardinality);
                        self.global_summary.insert(column.clone(), cloned);
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Check if a column is marked as high-cardinality globally.
    pub fn is_high_cardinality(&self, column: &str) -> bool {
        self.global_high_cardinality.contains(column)
    }
    
    /// Get the set of high-cardinality columns.
    pub fn high_cardinality_columns(&self) -> &std::collections::HashSet<String> {
        &self.global_high_cardinality
    }
    
    /// Filter files using predicates with optional partition hints.
    ///
    /// PERFORMANCE: When partition hints are provided (e.g., from date predicates),
    /// only those partitions are searched. This is O(partitions_matching) instead
    /// of O(all_partitions).
    ///
    /// # Arguments
    /// * `predicates` - Equality predicates to filter by
    /// * `partition_hints` - Optional list of partition keys to restrict search to
    ///
    /// # Returns
    /// List of file paths that might contain matching rows.
    ///
    /// # Observability
    ///
    /// Logs filtering results with:
    /// - Partitions checked vs pruned
    /// - Files checked vs matched
    /// - Whether partition hints were used
    pub fn filter_with_partition_hint(
        &self,
        predicates: &HashMap<String, String>,
        partition_hints: Option<&[&str]>,
    ) -> Vec<&str> {
        if predicates.is_empty() {
            return match partition_hints {
                Some(hints) => hints.iter()
                    .filter_map(|key| self.partitions.get(*key))
                    .flat_map(|p| p.files.keys().map(|s| s.as_str()))
                    .collect(),
                None => self.all_file_paths(),
            };
        }
        
        let has_partition_hints = partition_hints.is_some();
        
        let partitions_to_search: Vec<&PartitionIndex> = match partition_hints {
            Some(hints) => {
                hints.iter()
                    .filter_map(|key| self.partitions.get(*key))
                    .filter(|p| {
                        predicates.iter().all(|(col, val)| p.might_contain(col, val))
                    })
                    .collect()
            }
            None => {
                self.partitions
                    .values()
                    .filter(|p| {
                        predicates.iter().all(|(col, val)| p.might_contain(col, val))
                    })
                    .collect()
            }
        };

        let total_partitions = match partition_hints {
            Some(hints) => hints.len(),
            None => self.partitions.len(),
        };
        let partitions_searched = partitions_to_search.len();
        let partitions_pruned = total_partitions.saturating_sub(partitions_searched);
        
        let mut matching_files = Vec::new();
        let mut files_checked = 0usize;
        
        for partition in partitions_to_search {
            for file_index in partition.files.values() {
                files_checked += 1;
                let matches = predicates.iter().all(|(col, val)| {
                    file_index.might_contain(col, val)
                });
                
                if matches {
                    matching_files.push(file_index.file_path.as_str());
                }
            }
        }
        
        let files_skipped = files_checked - matching_files.len();
        
        tracing::debug!(
            total_partitions = total_partitions,
            partitions_searched = partitions_searched,
            partitions_pruned = partitions_pruned,
            files_checked = files_checked,
            matching_files = matching_files.len(),
            files_skipped = files_skipped,
            has_partition_hints = has_partition_hints,
            predicate_count = predicates.len(),
            "Hierarchical skip index filtered files"
        );
        
        matching_files
    }
    
    /// Filter files by prefix predicates with partition hints.
    pub fn filter_prefix_with_partition_hint(
        &self,
        predicates: &HashMap<String, String>,
        partition_hints: Option<&[&str]>,
    ) -> Vec<&str> {
        if predicates.is_empty() {
            return match partition_hints {
                Some(hints) => hints.iter()
                    .filter_map(|key| self.partitions.get(*key))
                    .flat_map(|p| p.files.keys().map(|s| s.as_str()))
                    .collect(),
                None => self.all_file_paths(),
            };
        }
        
        let partitions_to_search: Vec<&PartitionIndex> = match partition_hints {
            Some(hints) => {
                hints.iter()
                    .filter_map(|key| self.partitions.get(*key))
                    .filter(|p| {
                        predicates.iter().all(|(col, prefix)| p.might_contain_prefix(col, prefix))
                    })
                    .collect()
            }
            None => {
                self.partitions
                    .values()
                    .filter(|p| {
                        predicates.iter().all(|(col, prefix)| p.might_contain_prefix(col, prefix))
                    })
                    .collect()
            }
        };
        
        let mut matching_files = Vec::new();
        
        for partition in partitions_to_search {
            for file_index in partition.files.values() {
                let matches = predicates.iter().all(|(col, prefix)| {
                    file_index.might_contain_prefix(col, prefix)
                });
                
                if matches {
                    matching_files.push(file_index.file_path.as_str());
                }
            }
        }
        
        matching_files
    }
    
    /// Get all file paths in the index.
    pub fn all_file_paths(&self) -> Vec<&str> {
        self.partitions
            .values()
            .flat_map(|p| p.files.keys().map(|s| s.as_str()))
            .collect()
    }
    
    /// Filter files by substring predicates with partition hints.
    ///
    /// Uses `SubstringAutomaton` (regex-automata DFA) to prune files whose
    /// FST definitely does not contain the substring.
    pub fn filter_substring_with_partition_hint(
        &self,
        predicates: &HashMap<String, String>,
        partition_hints: Option<&[&str]>,
    ) -> Vec<&str> {
        if predicates.is_empty() {
            return match partition_hints {
                Some(hints) => hints.iter()
                    .filter_map(|key| self.partitions.get(*key))
                    .flat_map(|p| p.files.keys().map(|s| s.as_str()))
                    .collect(),
                None => self.all_file_paths(),
            };
        }

        // Pre-build all automata once to avoid per-partition/per-file DFA recompilation
        let automata: Vec<(&str, SubstringAutomaton)> = predicates.iter()
            .filter_map(|(col, sub)| {
                let aut = SubstringAutomaton::new(sub);
                if aut.is_none() {
                    tracing::warn!(
                        column = %col,
                        pattern = %sub,
                        "Substring automaton too complex, predicate will not be used for skip-index pruning"
                    );
                }
                aut.map(|a| (col.as_str(), a))
            })
            .collect();

        let partitions_to_search: Vec<&PartitionIndex> = match partition_hints {
            Some(hints) => {
                hints.iter()
                    .filter_map(|key| self.partitions.get(*key))
                    .filter(|p| {
                        automata.iter().all(|(col, aut)| p.might_contain_substring_with_automaton(col, aut))
                    })
                    .collect()
            }
            None => {
                self.partitions
                    .values()
                    .filter(|p| {
                        automata.iter().all(|(col, aut)| p.might_contain_substring_with_automaton(col, aut))
                    })
                    .collect()
            }
        };

        let mut matching_files = Vec::new();
        for partition in partitions_to_search {
            for file_index in partition.files.values() {
                let matches = automata.iter().all(|(col, aut)| {
                    file_index.might_contain_substring_with_automaton(col, aut)
                });
                if matches {
                    matching_files.push(file_index.file_path.as_str());
                }
            }
        }
        matching_files
    }

    /// Get partitions that might contain a value.
    ///
    /// This is useful for generating partition-aware file patterns.
    pub fn partitions_containing(&self, column: &str, value: &str) -> Vec<&str> {
        self.partitions
            .values()
            .filter(|p| p.might_contain(column, value))
            .map(|p| p.partition_key.as_str())
            .collect()
    }
    
    /// Get partition keys.
    pub fn partition_keys(&self) -> Vec<&str> {
        self.partitions.keys().map(|s| s.as_str()).collect()
    }
    
    /// Get total number of files.
    pub fn total_files(&self) -> usize {
        self.total_files
    }
    
    /// Get number of partitions.
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }
    
    /// Get a specific partition by key.
    pub fn get_partition(&self, key: &str) -> Option<&PartitionIndex> {
        self.partitions.get(key)
    }
    
    /// Get an iterator over all partitions.
    ///
    /// Returns (partition_key, partition_index) pairs.
    pub fn partitions(&self) -> impl Iterator<Item = (&String, &PartitionIndex)> {
        self.partitions.iter()
    }
    
    /// Build an optimized file pattern for ClickHouse.
    ///
    /// Uses partition awareness to generate more targeted patterns.
    pub fn build_file_pattern(
        &self,
        base_prefix: &str,
        predicates: &HashMap<String, String>,
        partition_hints: Option<&[&str]>,
    ) -> String {
        if predicates.is_empty() && partition_hints.is_none() {
            return format!("{}/**/*.parquet", base_prefix);
        }
        
        let matching_files = self.filter_with_partition_hint(predicates, partition_hints);
        
        // If most files match, use glob pattern.
        // Guard `total_files > 1` so a single-file index returns the specific path.
        if (self.total_files > 1 && matching_files.len() > self.total_files / 2) || matching_files.len() > 100 {
            // Try to use partition-based pattern if we have hints
            if let Some(hints) = partition_hints {
                if hints.len() <= 10 {
                    // Generate partition-specific patterns
                    let patterns: Vec<String> = hints
                        .iter()
                        .map(|p| format!("{}/{}/*.parquet", base_prefix, p))
                        .collect();
                    
                    if patterns.len() == 1 {
                        return patterns.into_iter().next().expect("checked len == 1");
                    }
                    return format!("{{{}}}", patterns.join(","));
                }
            }
            return format!("{}/**/*.parquet", base_prefix);
        }
        
        if matching_files.is_empty() {
            return EMPTY_MATCH_PATTERN.to_string();
        }
        
        if matching_files.len() == 1 {
            return format!("{}/{}", base_prefix, matching_files[0]);
        }
        
        let prefixed: Vec<String> = matching_files
            .iter()
            .map(|f| format!("{}/{}", base_prefix, f))
            .collect();
        format!("{{{}}}", prefixed.join(","))
    }

    /// Build an optimized file pattern using both equality and substring predicates.
    ///
    /// First applies equality-based filtering via `filter_with_partition_hint`,
    /// then further narrows using substring predicates. Falls back to
    /// `build_file_pattern` when there are no substring predicates.
    pub fn build_file_pattern_with_substring(
        &self,
        base_prefix: &str,
        equality_predicates: &HashMap<String, String>,
        substring_predicates: &HashMap<String, Vec<String>>,
        partition_hints: Option<&[&str]>,
    ) -> String {
        // If no substring predicates, delegate to the original method
        if substring_predicates.is_empty()
            || substring_predicates.values().all(|v| v.is_empty())
        {
            return self.build_file_pattern(base_prefix, equality_predicates, partition_hints);
        }

        // Get the equality-filtered file set
        let mut matching_files = self.filter_with_partition_hint(equality_predicates, partition_hints);

        // Flatten substring predicates to single-value map for the existing method
        let flat_subs: HashMap<String, String> = substring_predicates
            .iter()
            .filter_map(|(col, subs)| subs.first().map(|s| (col.clone(), s.clone())))
            .collect();
        let substring_files: std::collections::HashSet<&str> = self
            .filter_substring_with_partition_hint(&flat_subs, partition_hints)
            .into_iter()
            .collect();
        matching_files.retain(|f| substring_files.contains(f));

        // For additional substrings per column, intersect further
        for (col, subs) in substring_predicates {
            for sub in subs.iter().skip(1) {
                let single: HashMap<String, String> =
                    [(col.clone(), sub.clone())].into_iter().collect();
                let extra: std::collections::HashSet<&str> = self
                    .filter_substring_with_partition_hint(&single, partition_hints)
                    .into_iter()
                    .collect();
                matching_files.retain(|f| extra.contains(f));
            }
        }

        // Apply the same heuristics as build_file_pattern.
        // Guard `total_files > 1` so a single-file index returns the specific path.
        if (self.total_files > 1 && matching_files.len() > self.total_files / 2) || matching_files.len() > 100 {
            if let Some(hints) = partition_hints {
                if hints.len() <= 10 {
                    let patterns: Vec<String> = hints
                        .iter()
                        .map(|p| format!("{}/{}/*.parquet", base_prefix, p))
                        .collect();
                    if patterns.len() == 1 {
                        return patterns.into_iter().next().expect("checked len == 1");
                    }
                    return format!("{{{}}}", patterns.join(","));
                }
            }
            return format!("{}/**/*.parquet", base_prefix);
        }

        if matching_files.is_empty() {
            return EMPTY_MATCH_PATTERN.to_string();
        }

        if matching_files.len() == 1 {
            return format!("{}/{}", base_prefix, matching_files[0]);
        }

        let prefixed: Vec<String> = matching_files
            .iter()
            .map(|f| format!("{}/{}", base_prefix, f))
            .collect();
        format!("{{{}}}", prefixed.join(","))
    }
    
    /// Filter files using full `SkipPredicates` with partition-aware pruning.
    ///
    /// Applies all predicate types (equality, prefix, range, IN, substring)
    /// at both the partition summary level and the per-file level.
    pub fn filter_with_skip_predicates(
        &self,
        predicates: &SkipPredicates,
        partition_hints: Option<&[&str]>,
    ) -> Vec<&str> {
        if predicates.contradicted {
            return Vec::new();
        }
        if predicates.is_empty() {
            return match partition_hints {
                Some(hints) => hints.iter()
                    .filter_map(|key| self.partitions.get(*key))
                    .flat_map(|p| p.files.keys().map(|s| s.as_str()))
                    .collect(),
                None => self.all_file_paths(),
            };
        }

        let in_list_refs: Vec<(&str, Vec<&str>)> = predicates.in_lists.iter()
            .map(|(col, values)| (col.as_str(), values.iter().map(|s| s.as_str()).collect()))
            .collect();

        let substring_automata: Vec<(&str, Vec<SubstringAutomaton>)> = predicates.substring.iter()
            .map(|(col, subs)| {
                let auts: Vec<SubstringAutomaton> = subs.iter()
                    .filter_map(|s| SubstringAutomaton::new(s))
                    .collect();
                (col.as_str(), auts)
            })
            .collect();

        let token_fts_cols: Vec<(String, &Vec<String>)> = predicates.token_search.iter()
            .map(|(col, tokens)| {
                let fts_col = format!("{}{}", crate::warehouse::indexes::fulltext_index::FTS_COLUMN_PREFIX, col);
                (fts_col, tokens)
            })
            .collect();

        let partitions_to_search: Vec<&PartitionIndex> = match partition_hints {
            Some(hints) => {
                hints.iter()
                    .filter_map(|key| self.partitions.get(*key))
                    .collect()
            }
            None => self.partitions.values().collect(),
        };

        let partitions_to_search: Vec<&PartitionIndex> = partitions_to_search
            .into_iter()
            .filter(|p| {
                predicates.equality.iter().all(|(col, val)| p.might_contain(col, val))
                && predicates.prefix.iter().all(|(col, pfx)| p.might_contain_prefix(col, pfx))
                && predicates.ranges.iter().all(|(col, range)| {
                    p.might_contain_range_ex(
                        col,
                        range.min_value.as_deref(),
                        range.min_exclusive,
                        range.max_value.as_deref(),
                        range.max_exclusive,
                    )
                })
                && in_list_refs.iter().all(|(col, refs)| p.might_contain_any(col, refs))
                && substring_automata.iter().all(|(col, auts)| {
                    auts.iter().all(|aut| p.might_contain_substring_with_automaton(col, aut))
                })
                && token_fts_cols.iter().all(|(fts_col, tokens)| {
                    tokens.iter().all(|tok| p.might_contain(fts_col, tok))
                })
            })
            .collect();

        if partitions_to_search.is_empty() {
            return Vec::new();
        }

        let estimated_files: usize = partitions_to_search.iter().map(|p| p.files.len()).sum();
        let mut matching_files = Vec::with_capacity(estimated_files);

        for partition in partitions_to_search {
            if partition.files.is_empty() {
                continue;
            }
            for file_index in partition.files.values() {
                let matches = predicates.equality.iter()
                        .all(|(col, val)| file_index.might_contain(col, val))
                    && predicates.prefix.iter()
                        .all(|(col, pfx)| file_index.might_contain_prefix(col, pfx))
                    && predicates.ranges.iter()
                        .all(|(col, range)| {
                            file_index.might_contain_range_ex(
                                col,
                                range.min_value.as_deref(),
                                range.min_exclusive,
                                range.max_value.as_deref(),
                                range.max_exclusive,
                            )
                        })
                    && in_list_refs.iter()
                        .all(|(col, refs)| file_index.might_contain_any(col, refs))
                    && substring_automata.iter()
                        .all(|(col, auts)| {
                            auts.iter().all(|aut| file_index.might_contain_substring_with_automaton(col, aut))
                        })
                    && token_fts_cols.iter()
                        .all(|(fts_col, tokens)| {
                            tokens.iter().all(|tok| file_index.might_contain(fts_col, tok))
                        });

                if matches {
                    matching_files.push(file_index.file_path.as_str());
                }
            }
        }

        matching_files
    }

    /// Build an optimized file pattern using full `SkipPredicates`.
    ///
    /// Applies all predicate types (equality, prefix, range, IN, substring)
    /// for file pruning, then formats the result as a ClickHouse s3() pattern.
    pub fn build_file_pattern_with_predicates(
        &self,
        base_prefix: &str,
        predicates: &SkipPredicates,
        partition_hints: Option<&[&str]>,
    ) -> String {
        if predicates.contradicted {
            return EMPTY_MATCH_PATTERN.to_string();
        }

        let matching_files = self.filter_with_skip_predicates(predicates, partition_hints);

        if (self.total_files > 1 && matching_files.len() > self.total_files / 2) || matching_files.len() > 100 {
            if let Some(hints) = partition_hints {
                if hints.len() <= 10 {
                    let patterns: Vec<String> = hints
                        .iter()
                        .map(|p| format!("{}/{}/*.parquet", base_prefix, p))
                        .collect();
                    if patterns.len() == 1 {
                        return patterns.into_iter().next().expect("checked len == 1");
                    }
                    return format!("{{{}}}", patterns.join(","));
                }
            }
            return format!("{}/**/*.parquet", base_prefix);
        }

        if matching_files.is_empty() {
            return EMPTY_MATCH_PATTERN.to_string();
        }

        if matching_files.len() == 1 {
            return format!("{}/{}", base_prefix, matching_files[0]);
        }

        let prefixed: Vec<String> = matching_files
            .iter()
            .map(|f| format!("{}/{}", base_prefix, f))
            .collect();
        format!("{{{}}}", prefixed.join(","))
    }

    /// Format a pre-filtered file list into a ClickHouse s3() pattern.
    ///
    /// Used when an external filtering step (e.g., sidecar stats) has already
    /// narrowed the file list beyond what skip indexes provide.
    pub fn format_file_pattern(
        &self,
        base_prefix: &str,
        matching_files: &[&str],
        partition_hints: Option<&[&str]>,
    ) -> String {
        if (self.total_files > 1 && matching_files.len() > self.total_files / 2)
            || matching_files.len() > 100
        {
            if let Some(hints) = partition_hints {
                if hints.len() <= 10 {
                    let patterns: Vec<String> = hints
                        .iter()
                        .map(|p| format!("{}/{}/*.parquet", base_prefix, p))
                        .collect();
                    if patterns.len() == 1 {
                        return patterns.into_iter().next().expect("checked len == 1");
                    }
                    return format!("{{{}}}", patterns.join(","));
                }
            }
            return format!("{}/**/*.parquet", base_prefix);
        }

        if matching_files.is_empty() {
            return EMPTY_MATCH_PATTERN.to_string();
        }

        if matching_files.len() == 1 {
            return format!("{}/{}", base_prefix, matching_files[0]);
        }

        let prefixed: Vec<String> = matching_files
            .iter()
            .map(|f| format!("{}/{}", base_prefix, f))
            .collect();
        format!("{{{}}}", prefixed.join(","))
    }

    /// Get statistics about the index.
    pub fn stats(&self) -> HierarchicalIndexStats {
        let partition_sizes: Vec<usize> = self.partitions.values().map(|p| p.files.len()).collect();
        let avg_files_per_partition = if partition_sizes.is_empty() {
            0.0
        } else {
            partition_sizes.iter().sum::<usize>() as f64 / partition_sizes.len() as f64
        };
        
        HierarchicalIndexStats {
            total_partitions: self.partitions.len(),
            total_files: self.total_files,
            avg_files_per_partition,
            max_files_in_partition: partition_sizes.into_iter().max().unwrap_or(0),
            indexed_columns: self.global_summary.keys().cloned().collect(),
            high_cardinality_columns: self.global_high_cardinality.iter().cloned().collect(),
        }
    }
}

impl Default for HierarchicalSkipIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about a hierarchical skip index.
#[derive(Debug, Clone)]
pub struct HierarchicalIndexStats {
    pub total_partitions: usize,
    pub total_files: usize,
    pub avg_files_per_partition: f64,
    pub max_files_in_partition: usize,
    /// Columns that have summary FST indexes
    pub indexed_columns: Vec<String>,
    /// Columns excluded from summaries due to high cardinality
    pub high_cardinality_columns: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_index() -> HierarchicalSkipIndex {
        let mut index = HierarchicalSkipIndex::new();

        // File 1: contains customers A-M
        let mut columns1 = HashMap::new();
        columns1.insert(
            "name".to_string(),
            vec!["Alice".to_string(), "Bob".to_string(), "Charlie".to_string()],
        );
        columns1.insert(
            "status".to_string(),
            vec!["active".to_string()],
        );
        let file1 = FileSkipIndex::build("file1.parquet", columns1).unwrap();
        index.add_file("__default__", file1, 1000).unwrap();

        // File 2: contains customers N-Z
        let mut columns2 = HashMap::new();
        columns2.insert(
            "name".to_string(),
            vec!["Nancy".to_string(), "Oliver".to_string(), "Zoe".to_string()],
        );
        columns2.insert(
            "status".to_string(),
            vec!["inactive".to_string()],
        );
        let file2 = FileSkipIndex::build("file2.parquet", columns2).unwrap();
        index.add_file("__default__", file2, 1000).unwrap();

        index
    }

    fn build_test_file1() -> FileSkipIndex {
        let mut columns = HashMap::new();
        columns.insert(
            "name".to_string(),
            vec!["Alice".to_string(), "Bob".to_string(), "Charlie".to_string()],
        );
        columns.insert(
            "status".to_string(),
            vec!["active".to_string()],
        );
        FileSkipIndex::build("file1.parquet", columns).unwrap()
    }

    fn build_test_file2() -> FileSkipIndex {
        let mut columns = HashMap::new();
        columns.insert(
            "name".to_string(),
            vec!["Nancy".to_string(), "Oliver".to_string(), "Zoe".to_string()],
        );
        columns.insert(
            "status".to_string(),
            vec!["inactive".to_string()],
        );
        FileSkipIndex::build("file2.parquet", columns).unwrap()
    }

    #[test]
    fn test_files_containing() {
        let index = create_test_index();

        let mut preds = HashMap::new();
        preds.insert("name".to_string(), "Alice".to_string());
        let files = index.filter_with_partition_hint(&preds, None);
        assert_eq!(files.len(), 1);
        assert!(files.contains(&"file1.parquet"));

        let mut preds = HashMap::new();
        preds.insert("name".to_string(), "Zoe".to_string());
        let files = index.filter_with_partition_hint(&preds, None);
        assert_eq!(files.len(), 1);
        assert!(files.contains(&"file2.parquet"));

        let mut preds = HashMap::new();
        preds.insert("name".to_string(), "Xavier".to_string());
        let files = index.filter_with_partition_hint(&preds, None);
        assert!(files.is_empty());
    }

    #[test]
    fn test_files_with_prefix() {
        let file1 = build_test_file1();
        let file2 = build_test_file2();

        assert!(file1.might_contain_prefix("name", "A"));
        assert!(file2.might_contain_prefix("name", "Z"));
    }

    #[test]
    fn test_status_filtering() {
        let index = create_test_index();

        let mut preds = HashMap::new();
        preds.insert("status".to_string(), "active".to_string());
        let files = index.filter_with_partition_hint(&preds, None);
        assert_eq!(files.len(), 1);
        assert!(files.contains(&"file1.parquet"));

        let mut preds = HashMap::new();
        preds.insert("status".to_string(), "inactive".to_string());
        let files = index.filter_with_partition_hint(&preds, None);
        assert_eq!(files.len(), 1);
        assert!(files.contains(&"file2.parquet"));
    }
    
    // ===== Hierarchical Skip Index Tests =====
    
    fn create_hierarchical_test_index() -> HierarchicalSkipIndex {
        let mut index = HierarchicalSkipIndex::new();
        
        // Partition 2025/01 - January data
        let mut cols_jan1 = HashMap::new();
        cols_jan1.insert("customer".to_string(), vec!["Alice".to_string(), "Bob".to_string()]);
        cols_jan1.insert("status".to_string(), vec!["active".to_string()]);
        let file_jan1 = FileSkipIndex::build("2025/01/data_001.parquet", cols_jan1).unwrap();
        index.add_file("2025/01", file_jan1, 10000).unwrap();
        
        let mut cols_jan2 = HashMap::new();
        cols_jan2.insert("customer".to_string(), vec!["Charlie".to_string(), "David".to_string()]);
        cols_jan2.insert("status".to_string(), vec!["active".to_string(), "pending".to_string()]);
        let file_jan2 = FileSkipIndex::build("2025/01/data_002.parquet", cols_jan2).unwrap();
        index.add_file("2025/01", file_jan2, 10000).unwrap();
        
        // Partition 2025/02 - February data
        let mut cols_feb1 = HashMap::new();
        cols_feb1.insert("customer".to_string(), vec!["Eve".to_string(), "Frank".to_string()]);
        cols_feb1.insert("status".to_string(), vec!["inactive".to_string()]);
        let file_feb1 = FileSkipIndex::build("2025/02/data_001.parquet", cols_feb1).unwrap();
        index.add_file("2025/02", file_feb1, 10000).unwrap();
        
        index
    }
    
    #[test]
    fn test_hierarchical_basic_stats() {
        let index = create_hierarchical_test_index();
        
        assert_eq!(index.total_files(), 3);
        assert_eq!(index.partition_count(), 2);
        
        let stats = index.stats();
        assert_eq!(stats.total_partitions, 2);
        assert_eq!(stats.total_files, 3);
    }
    
    #[test]
    fn test_hierarchical_partition_pruning() {
        let index = create_hierarchical_test_index();
        
        // Search for "inactive" status - only in February partition
        let mut predicates = HashMap::new();
        predicates.insert("status".to_string(), "inactive".to_string());
        
        let files = index.filter_with_partition_hint(&predicates, None);
        assert_eq!(files.len(), 1);
        assert!(files.contains(&"2025/02/data_001.parquet"));
    }
    
    #[test]
    fn test_hierarchical_with_partition_hint() {
        let index = create_hierarchical_test_index();
        
        // Search only in January partition
        let mut predicates = HashMap::new();
        predicates.insert("customer".to_string(), "Alice".to_string());
        
        let files = index.filter_with_partition_hint(&predicates, Some(&["2025/01"]));
        assert_eq!(files.len(), 1);
        assert!(files.contains(&"2025/01/data_001.parquet"));
        
        // Same search but wrong partition - should find nothing
        let files = index.filter_with_partition_hint(&predicates, Some(&["2025/02"]));
        assert!(files.is_empty());
    }
    
    #[test]
    fn test_hierarchical_partition_summary() {
        let index = create_hierarchical_test_index();
        
        // January partition should have combined values
        let jan = index.get_partition("2025/01").unwrap();
        
        assert!(jan.might_contain("customer", "Alice"));
        assert!(jan.might_contain("customer", "Charlie"));
        assert!(jan.might_contain("status", "active"));
        assert!(jan.might_contain("status", "pending"));
        assert!(!jan.might_contain("customer", "Eve")); // Eve is in Feb
    }
    
    #[test]
    fn test_hierarchical_partitions_containing() {
        let index = create_hierarchical_test_index();
        
        // "active" status is in January
        let partitions = index.partitions_containing("status", "active");
        assert!(partitions.contains(&"2025/01"));
        assert!(!partitions.contains(&"2025/02"));
        
        // "inactive" status is in February
        let partitions = index.partitions_containing("status", "inactive");
        assert!(!partitions.contains(&"2025/01"));
        assert!(partitions.contains(&"2025/02"));
    }

    // ===== SubstringAutomaton Tests =====

    #[test]
    fn test_substring_automaton_basic() {
        let file1 = build_test_file1();
        let file2 = build_test_file2();

        // "li" appears in Alice and Charlie (file1)
        assert!(file1.might_contain_substring("name", "li"), "Alice/Charlie contain 'li'");
        // file2 has Oliver which contains "li" too
        assert!(file2.might_contain_substring("name", "li"), "Oliver contains 'li'");

        // "ob" appears in Bob (file1) only
        assert!(file1.might_contain_substring("name", "ob"), "Bob contains 'ob'");
        assert!(!file2.might_contain_substring("name", "ob"), "No file2 name contains 'ob'");
    }

    #[test]
    fn test_substring_automaton_no_match() {
        let file1 = build_test_file1();
        let file2 = build_test_file2();

        assert!(!file1.might_contain_substring("name", "xyz"), "No name contains 'xyz'");
        assert!(!file2.might_contain_substring("name", "xyz"), "No name contains 'xyz'");
    }

    #[test]
    fn test_substring_automaton_special_chars() {
        // Build a small FST set with literal "a.b" and "axb"
        let mut builder = SetBuilder::memory();
        // Keys must be inserted in sorted order
        builder.insert("a.b").unwrap();
        builder.insert("axb").unwrap();
        let bytes = builder.into_inner().unwrap();
        let set = Set::new(bytes).unwrap();

        // "a.b" should match the literal "a.b" but NOT "axb"
        let aut = SubstringAutomaton::new("a.b").expect("DFA should build");
        let mut stream = set.search(&aut).into_stream();
        let mut matches = Vec::new();
        while let Some(key) = stream.next() {
            matches.push(std::str::from_utf8(key).unwrap().to_string());
        }
        assert!(matches.contains(&"a.b".to_string()), "literal 'a.b' should match");
        assert!(!matches.contains(&"axb".to_string()), "dot should be escaped, not match 'axb'");
    }

    #[test]
    fn test_substring_automaton_matches_across_newline() {
        let mut builder = SetBuilder::memory();
        builder.insert("line1\nline2").unwrap();
        builder.insert("nope").unwrap();
        let bytes = builder.into_inner().unwrap();
        let set = Set::new(bytes).unwrap();

        let aut = SubstringAutomaton::new("line2").expect("DFA should build");
        let mut stream = set.search(&aut).into_stream();
        let mut matches = Vec::new();
        while let Some(key) = stream.next() {
            matches.push(std::str::from_utf8(key).unwrap().to_string());
        }
        assert!(
            matches.contains(&"line1\nline2".to_string()),
            "SubstringAutomaton must match across newline bytes; got: {:?}",
            matches,
        );
        assert!(!matches.contains(&"nope".to_string()));
    }

    #[test]
    fn test_substring_automaton_empty_string() {
        // Empty substring should match everything
        let aut = SubstringAutomaton::new("").expect("DFA should build for empty string");

        let mut builder = SetBuilder::memory();
        builder.insert("anything").unwrap();
        builder.insert("hello").unwrap();
        let bytes = builder.into_inner().unwrap();
        let set = Set::new(bytes).unwrap();

        let mut stream = set.search(&aut).into_stream();
        let mut count = 0;
        while stream.next().is_some() {
            count += 1;
        }
        assert_eq!(count, 2, "Empty substring matches all keys");
    }

    #[test]
    fn test_file_skip_index_might_contain_substring() {
        let mut columns = HashMap::new();
        columns.insert(
            "name".to_string(),
            vec!["Alice".to_string(), "Bob".to_string(), "Charlie".to_string()],
        );
        let file = FileSkipIndex::build("test.parquet", columns).unwrap();

        assert!(file.might_contain_substring("name", "li"), "Alice/Charlie contain 'li'");
        assert!(file.might_contain_substring("name", "ob"), "Bob contains 'ob'");
        assert!(!file.might_contain_substring("name", "xyz"), "No name contains 'xyz'");
        // Unknown column returns true (conservative)
        assert!(file.might_contain_substring("unknown_col", "anything"));
    }

    #[test]
    fn test_hierarchical_files_containing_substring() {
        let file1 = build_test_file1();
        let file2 = build_test_file2();

        // "Nan" appears in Nancy (file2) only
        assert!(!file1.might_contain_substring("name", "Nan"));
        assert!(file2.might_contain_substring("name", "Nan"));

        // "active" is in file1 status column
        assert!(file1.might_contain_substring("status", "active"));
    }

    #[test]
    fn test_hierarchical_filter_substring() {
        let index = create_hierarchical_test_index();

        // "li" appears in Alice (Jan), Charlie doesn't have "li" wait - Charlie does: "Charlie" -> "li"
        // Also David doesn't. Eve/Frank don't.
        let mut preds = HashMap::new();
        preds.insert("customer".to_string(), "li".to_string());

        // Without partition hint
        let files = index.filter_substring_with_partition_hint(&preds, None);
        // Alice has "li", Charlie has "li" -> both Jan files should match
        assert!(files.contains(&"2025/01/data_001.parquet"), "Alice contains 'li'");
        assert!(files.contains(&"2025/01/data_002.parquet"), "Charlie contains 'li'");
        // Eve/Frank don't contain "li"
        assert!(!files.contains(&"2025/02/data_001.parquet"));

        // With partition hint restricting to Feb only -> no matches
        let files = index.filter_substring_with_partition_hint(&preds, Some(&["2025/02"]));
        assert!(files.is_empty(), "No Feb customer contains 'li'");
    }

    #[test]
    fn test_build_file_pattern_with_substring() {
        let index = create_hierarchical_test_index();

        // "Eve" only appears in Feb/data_001 -> 1 out of 3 files (below 50% heuristic)
        let mut subs = HashMap::new();
        subs.insert("customer".to_string(), vec!["Eve".to_string()]);
        let pattern = index.build_file_pattern_with_substring(
            "data", &HashMap::new(), &subs, None,
        );
        assert!(pattern.contains("2025/02/data_001.parquet"), "Pattern should include Feb file: {}", pattern);
        assert!(!pattern.contains("2025/01"), "Pattern should exclude Jan files: {}", pattern);

        // Substring "xyz" matches nothing -> nonexistent
        let mut subs_none = HashMap::new();
        subs_none.insert("customer".to_string(), vec!["xyz".to_string()]);
        let pattern = index.build_file_pattern_with_substring(
            "data", &HashMap::new(), &subs_none, None,
        );
        assert!(pattern.is_empty(), "No matches must return empty pattern: {}", pattern);
    }

    #[test]
    fn test_skip_predicates_add_substring_multi() {
        let mut preds = SkipPredicates::new();
        preds.add_substring("col", "a");
        preds.add_substring("col", "b");

        let subs = preds.substring.get("col").expect("col should have entries");
        assert_eq!(subs.len(), 2);
        assert!(subs.contains(&"a".to_string()));
        assert!(subs.contains(&"b".to_string()));

        // Different column
        preds.add_substring("other", "x");
        assert_eq!(preds.substring.len(), 2);
        assert!(!preds.is_empty());
    }

    // ========== Regression Tests ==========

    #[test]
    fn test_add_gte_then_lte_preserves_both_bounds() {
        let mut preds = SkipPredicates::new();
        preds.add_gte("price", "10");
        preds.add_lte("price", "50");

        let range = preds.ranges.get("price").expect("price range should exist");
        assert_eq!(
            range.min_value.as_deref(),
            Some("10"),
            "min_value (>=) must be preserved after add_lte"
        );
        assert_eq!(
            range.max_value.as_deref(),
            Some("50"),
            "max_value (<=) must be set"
        );
    }

    #[test]
    fn test_add_lte_then_gte_preserves_both_bounds() {
        let mut preds = SkipPredicates::new();
        preds.add_lte("price", "50");
        preds.add_gte("price", "10");

        let range = preds.ranges.get("price").expect("price range should exist");
        assert_eq!(range.min_value.as_deref(), Some("10"));
        assert_eq!(range.max_value.as_deref(), Some("50"));
    }

    #[test]
    fn test_add_gt_then_gte_keeps_tighter_bound() {
        let mut preds = SkipPredicates::new();
        // Lexicographic: "M" > "A", so "M" is the tighter lower bound
        preds.add_gt("name", "M");
        preds.add_gte("name", "A");

        let range = preds.ranges.get("name").expect("name range should exist");
        assert_eq!(
            range.min_value.as_deref(),
            Some("M"),
            "add_gt('M') then add_gte('A') must keep the tighter bound 'M'"
        );
        assert!(
            range.min_exclusive,
            "Exclusive flag from add_gt must be preserved"
        );
    }

    #[test]
    fn test_add_gte_then_gt_same_value_tightens_to_exclusive() {
        let mut preds = SkipPredicates::new();
        preds.add_gte("name", "M");
        preds.add_gt("name", "M");

        let range = preds.ranges.get("name").expect("name range should exist");
        assert_eq!(range.min_value.as_deref(), Some("M"));
        assert!(
            range.min_exclusive,
            "Same value: add_gt is tighter than add_gte"
        );
    }

    #[test]
    fn test_add_lt_then_lte_keeps_tighter_bound() {
        let mut preds = SkipPredicates::new();
        // Lexicographic: "D" < "Z", so "D" is the tighter upper bound
        preds.add_lt("name", "D");
        preds.add_lte("name", "Z");

        let range = preds.ranges.get("name").expect("name range should exist");
        assert_eq!(
            range.max_value.as_deref(),
            Some("D"),
            "add_lt('D') then add_lte('Z') must keep the tighter bound 'D'"
        );
        assert!(
            range.max_exclusive,
            "Exclusive flag from add_lt must be preserved"
        );
    }

    #[test]
    fn test_add_lte_then_lt_same_value_tightens_to_exclusive() {
        let mut preds = SkipPredicates::new();
        preds.add_lte("name", "D");
        preds.add_lt("name", "D");

        let range = preds.ranges.get("name").expect("name range should exist");
        assert_eq!(range.max_value.as_deref(), Some("D"));
        assert!(
            range.max_exclusive,
            "Same value: add_lt is tighter than add_lte"
        );
    }

    #[test]
    fn test_contradicted_equality_same_column_different_values() {
        let mut preds = SkipPredicates::new();
        preds.add_equals("col", "a");
        preds.add_equals("col", "b");
        assert!(preds.contradicted,
            "Same column with different equality values must be contradicted");
    }

    #[test]
    fn test_not_contradicted_equality_same_value() {
        let mut preds = SkipPredicates::new();
        preds.add_equals("col", "a");
        preds.add_equals("col", "a");
        assert!(!preds.contradicted,
            "Same column with identical equality values must NOT be contradicted");
    }

    #[test]
    fn test_contradicted_prefix_different_incompatible_prefixes() {
        let mut preds = SkipPredicates::new();
        preds.add_prefix("col", "foo");
        preds.add_prefix("col", "bar");
        assert!(preds.contradicted,
            "Incompatible prefixes must be contradicted");
    }

    #[test]
    fn test_not_contradicted_prefix_one_extends_the_other() {
        let mut preds = SkipPredicates::new();
        preds.add_prefix("col", "foo");
        preds.add_prefix("col", "foobar");
        assert!(!preds.contradicted,
            "If one prefix extends the other, it is NOT contradicted");
        assert_eq!(preds.prefix.get("col").unwrap(), "foobar",
            "The longer (tighter) prefix should win");
    }

    #[test]
    fn test_contradicted_in_empty_intersection() {
        let mut preds = SkipPredicates::new();
        preds.add_in("col", vec!["a".to_string(), "b".to_string()]);
        preds.add_in("col", vec!["c".to_string(), "d".to_string()]);
        assert!(preds.contradicted,
            "IN lists with empty intersection must be contradicted");
    }

    #[test]
    fn test_not_contradicted_in_overlapping() {
        let mut preds = SkipPredicates::new();
        preds.add_in("col", vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        preds.add_in("col", vec!["b".to_string(), "c".to_string(), "d".to_string()]);
        assert!(!preds.contradicted,
            "IN lists with non-empty intersection must NOT be contradicted");
        let values = preds.in_lists.get("col").unwrap();
        assert_eq!(values.len(), 2);
        assert!(values.contains(&"b".to_string()));
        assert!(values.contains(&"c".to_string()));
    }

    #[test]
    fn test_contradicted_predicates_prune_all_files() {
        let mut preds = SkipPredicates::new();
        preds.add_equals("status", "active");
        preds.add_equals("status", "pending");

        assert!(preds.contradicted,
            "Same column with different equality values must be contradicted");

        let mut index = HierarchicalSkipIndex::new();
        let mut values = HashMap::new();
        values.insert("status".to_string(), vec!["active".to_string(), "pending".to_string()]);
        let file = FileSkipIndex::build("data.parquet", values).unwrap();
        index.add_file("__default__", file, 1000).unwrap();

        let result = index.filter_with_skip_predicates(&preds, None);
        assert!(result.is_empty(),
            "Contradicted predicates must return empty file set, got: {:?}", result);
    }

    #[test]
    fn test_empty_prefix_returns_true() {
        let mut columns = std::collections::HashMap::new();
        columns.insert("name".to_string(), vec!["alpha".to_string(), "beta".to_string()]);
        let file = FileSkipIndex::build("test.parquet", columns).unwrap();

        assert!(
            file.might_contain_prefix("name", ""),
            "Empty prefix must return true (match everything)"
        );
    }

    #[test]
    fn test_might_contain_range_uses_inclusive_upper_bound() {
        let mut columns = std::collections::HashMap::new();
        columns.insert("col".to_string(), vec!["abc".to_string(), "xyz".to_string()]);
        let file = FileSkipIndex::build("test.parquet", columns).unwrap();

        // Exact match on both bounds
        assert!(
            file.might_contain_range("col", Some("abc"), Some("abc")),
            "Range [abc, abc] should match file containing 'abc'"
        );
        assert!(
            file.might_contain_range("col", Some("xyz"), Some("xyz")),
            "Range [xyz, xyz] should match file containing 'xyz'"
        );
    }

    #[test]
    fn test_numeric_stats_nan_guard() {
        let stats = NumericColumnStats::new(0.0, 100.0, 0, 1000).unwrap();

        assert!(
            stats.might_contain_range(Some(f64::NAN), Some(50.0)),
            "NaN query_min should return true (safe fallback)"
        );
        assert!(
            stats.might_contain_range(Some(50.0), Some(f64::NAN)),
            "NaN query_max should return true (safe fallback)"
        );
        assert!(
            stats.might_contain_gt(f64::NAN),
            "NaN threshold in might_contain_gt should return true"
        );
        assert!(
            stats.might_contain_gte(f64::NAN),
            "NaN threshold in might_contain_gte should return true"
        );
        assert!(
            stats.might_contain_lt(f64::NAN),
            "NaN threshold in might_contain_lt should return true"
        );
        assert!(
            stats.might_contain_lte(f64::NAN),
            "NaN threshold in might_contain_lte should return true"
        );
    }

    #[test]
    fn test_cardinality_threshold_boundary() {
        // The early-detection threshold uses exact cardinality via fst.len().
        // estimated_combined = existing_cardinality + new_fst.len().
        // Exclusion triggers when estimated_combined > MAX_SUMMARY_CARDINALITY.

        let mut partition = PartitionIndex::new("test_partition");

        let mut seed_values = HashMap::new();
        seed_values.insert("col".to_string(), vec!["seed".to_string()]);
        let seed = FileSkipIndex::build("seed.parquet", seed_values).unwrap();
        partition.add_file(seed, 1).unwrap();

        // A single-key FST has len() == 1.
        let new_cardinality = 1usize;

        // Case 1: existing + new == threshold exactly -> should NOT trigger (> not >=).
        let threshold = MAX_SUMMARY_CARDINALITY;
        let existing_for_boundary = threshold.saturating_sub(new_cardinality);
        partition.column_cardinality.insert("col".to_string(), existing_for_boundary);

        let mut at_values = HashMap::new();
        at_values.insert("col".to_string(), vec!["at_boundary".to_string()]);
        let at_file = FileSkipIndex::build("at.parquet", at_values).unwrap();
        partition.add_file(at_file, 1).unwrap();

        assert!(
            !partition.high_cardinality_columns.contains("col"),
            "At exactly threshold the column must NOT be excluded"
        );
        assert!(
            partition.summary.contains_key("col"),
            "Column summary must still exist at exact threshold"
        );

        // Case 2: existing + new > threshold -> MUST trigger.
        partition.column_cardinality.insert("col".to_string(), existing_for_boundary + 1);

        let mut over_values = HashMap::new();
        over_values.insert("col".to_string(), vec!["over_boundary".to_string()]);
        let over_file = FileSkipIndex::build("over.parquet", over_values).unwrap();
        partition.add_file(over_file, 1).unwrap();

        assert!(
            partition.high_cardinality_columns.contains("col"),
            "Above threshold the column MUST be excluded"
        );
        assert!(
            !partition.summary.contains_key("col"),
            "Column summary must be removed when above threshold"
        );
    }

    #[test]
    fn test_empty_fst_union_not_marked_high_cardinality() {
        let mut partition = PartitionIndex::new("test_partition");

        let empty_cols: HashMap<String, Vec<String>> = HashMap::new();
        let mut cols_with_empty = HashMap::new();
        cols_with_empty.insert("col".to_string(), Vec::<String>::new());

        let file1 = FileSkipIndex::build("f1.parquet", cols_with_empty.clone()).unwrap();
        partition.add_file(file1, 1).unwrap();

        let file2 = FileSkipIndex::build("f2.parquet", cols_with_empty.clone()).unwrap();
        partition.add_file(file2, 1).unwrap();

        assert!(
            !partition.high_cardinality_columns.contains("col"),
            "Empty FST union must NOT mark column as high-cardinality"
        );
    }

    #[test]
    fn test_hierarchical_empty_fst_union_not_marked_high_cardinality() {
        let mut hi = HierarchicalSkipIndex::new();

        let mut cols = HashMap::new();
        cols.insert("col".to_string(), Vec::<String>::new());

        let file1 = FileSkipIndex::build("f1.parquet", cols.clone()).unwrap();
        hi.add_file("p1", file1, 1).unwrap();

        let file2 = FileSkipIndex::build("f2.parquet", cols.clone()).unwrap();
        hi.add_file("p1", file2, 1).unwrap();

        assert!(
            !hi.is_high_cardinality("col"),
            "Empty FST union in global summary must NOT mark column as high-cardinality"
        );
    }

    #[test]
    fn test_build_file_pattern_empty_match_returns_empty_string() {
        let mut index = HierarchicalSkipIndex::new();
        let mut values = HashMap::new();
        values.insert("status".to_string(), vec!["active".to_string()]);
        let file_idx = FileSkipIndex::build("data.parquet", values).unwrap();
        index.add_file("__default__", file_idx, 1000).unwrap();

        let mut predicates = HashMap::new();
        predicates.insert("status".to_string(), "nonexistent_value".to_string());
        let pattern = index.build_file_pattern("prefix", &predicates, None);

        assert_eq!(
            pattern,
            EMPTY_MATCH_PATTERN,
            "When no files match, build_file_pattern must return EMPTY_MATCH_PATTERN"
        );
        assert!(
            pattern.is_empty(),
            "EMPTY_MATCH_PATTERN must be an empty string that callers can detect"
        );
    }

    #[test]
    fn test_build_file_pattern_fallback_uses_recursive_glob() {
        let index = create_test_index();

        let pattern = index.build_file_pattern("prefix", &HashMap::new(), None);
        assert!(
            pattern.contains("**/*.parquet"),
            "Fallback glob must be recursive (**/*.parquet), got: {}",
            pattern,
        );
    }

    #[test]
    fn test_build_file_pattern_without_substring_misses_pruning() {
        let index = create_hierarchical_test_index();

        // "Eve" only appears in Feb partition.
        // build_file_pattern (equality-only) with no equality predicates
        // returns ALL files (no pruning).
        let pattern_no_sub = index.build_file_pattern(
            "data", &HashMap::new(), None,
        );
        assert!(
            pattern_no_sub.contains("**"),
            "Without equality predicates, build_file_pattern must return a glob: {}",
            pattern_no_sub
        );

        // build_file_pattern_with_substring with a substring predicate on
        // "customer" = "Eve" narrows to just the Feb file.
        let mut subs = HashMap::new();
        subs.insert("customer".to_string(), vec!["Eve".to_string()]);
        let pattern_with_sub = index.build_file_pattern_with_substring(
            "data", &HashMap::new(), &subs, None,
        );
        assert!(
            pattern_with_sub.contains("2025/02/data_001.parquet"),
            "With substring predicate, only the matching file should be included: {}",
            pattern_with_sub
        );
        assert!(
            !pattern_with_sub.contains("2025/01"),
            "Jan files must be pruned by substring predicate: {}",
            pattern_with_sub
        );
    }

    #[test]
    fn test_might_contain_nan_returns_true() {
        let stats = NumericColumnStats::new(1.0, 100.0, 0, 10).unwrap();
        assert!(
            stats.might_contain(f64::NAN),
            "NaN must not cause false negatives in skip index"
        );
    }

    #[test]
    fn test_might_contain_normal_values() {
        let stats = NumericColumnStats::new(10.0, 20.0, 0, 5).unwrap();
        assert!(stats.might_contain(15.0));
        assert!(stats.might_contain(10.0));
        assert!(stats.might_contain(20.0));
        assert!(!stats.might_contain(9.99));
        assert!(!stats.might_contain(20.01));
    }

    #[test]
    fn test_nan_stats_are_conservative() {
        let stats = NumericColumnStats::new_unchecked(f64::NAN, 100.0, 0, 10);

        assert!(stats.might_contain(50.0), "NaN min -> conservative true");
        assert!(stats.might_contain_range(Some(10.0), Some(20.0)));
        assert!(stats.might_contain_lt(50.0));
        assert!(stats.might_contain_lte(50.0));

        let stats2 = NumericColumnStats::new_unchecked(0.0, f64::NAN, 0, 10);

        assert!(stats2.might_contain(50.0), "NaN max -> conservative true");
        assert!(stats2.might_contain_gt(50.0));
        assert!(stats2.might_contain_gte(50.0));
    }

    #[test]
    fn test_exclusive_range_bounds_prune_boundary_values() {
        let mut columns = std::collections::HashMap::new();
        columns.insert("val".to_string(), vec!["10".to_string(), "20".to_string(), "30".to_string()]);
        let file = FileSkipIndex::build("test.parquet", columns).unwrap();

        // Inclusive: [10, 30] matches
        assert!(file.might_contain_range("val", Some("10"), Some("30")));
        // Exclusive lower: (30, ...) should NOT match (no values > "30")
        assert!(
            !file.might_contain_range_ex("val", Some("30"), true, None, false),
            "Exclusive lower bound '30' must exclude file whose max is '30'"
        );
        // Inclusive lower: [30, ...) should match (value "30" is present)
        assert!(file.might_contain_range_ex("val", Some("30"), false, None, false));
        // Exclusive upper: (.., 10) should NOT match (no values < "10")
        assert!(
            !file.might_contain_range_ex("val", None, false, Some("10"), true),
            "Exclusive upper bound '10' must exclude file whose min is '10'"
        );
        // Inclusive upper: (.., 10] should match
        assert!(file.might_contain_range_ex("val", None, false, Some("10"), false));
    }

    #[test]
    fn test_numeric_stats_deserialization_swaps_inverted_bounds() {
        let json = r#"{"min": 20.0, "max": 10.0, "null_count": 0, "value_count": 5}"#;
        let stats: NumericColumnStats = serde_json::from_str(json).unwrap();
        assert!(
            stats.min() <= stats.max(),
            "Deserialization must swap inverted min/max: min={}, max={}",
            stats.min(), stats.max(),
        );
        assert_eq!(stats.min(), 10.0);
        assert_eq!(stats.max(), 20.0);
        assert!(
            stats.might_contain(15.0),
            "Value 15 must be within corrected range [10, 20]"
        );
    }

    #[test]
    fn test_numeric_stats_deserialization_preserves_valid_bounds() {
        let json = r#"{"min": 5.0, "max": 100.0, "null_count": 2, "value_count": 98}"#;
        let stats: NumericColumnStats = serde_json::from_str(json).unwrap();
        assert_eq!(stats.min(), 5.0);
        assert_eq!(stats.max(), 100.0);
        assert_eq!(stats.null_count(), 2);
        assert_eq!(stats.value_count(), 98);
    }

    #[test]
    fn test_skip_predicates_gt_lt_exclusive_flags() {
        let mut preds = SkipPredicates::new();
        preds.add_gt("price", "10");
        let range = preds.ranges.get("price").unwrap();
        assert!(range.min_exclusive);
        assert!(!range.max_exclusive);

        let mut preds2 = SkipPredicates::new();
        preds2.add_lt("price", "50");
        let range2 = preds2.ranges.get("price").unwrap();
        assert!(!range2.min_exclusive);
        assert!(range2.max_exclusive);
    }

    #[test]
    fn test_partition_cardinality_uses_actual_count_not_byte_estimate() {
        let mut partition = PartitionIndex::new("test");

        // Build an FST with long keys that inflate byte size relative to key count.
        // With the old `bytes / 10` heuristic, 5 keys of ~40 chars each would
        // produce an FST of ~200+ bytes, yielding an estimate of ~20+ (overcount).
        // The actual cardinality is 5, well within limits.
        let long_keys: Vec<String> = (0..5)
            .map(|i| format!("this_is_a_deliberately_long_key_prefix_{:04}", i))
            .collect();
        let mut values = HashMap::new();
        values.insert("col".to_string(), long_keys);
        let file = FileSkipIndex::build("long_keys.parquet", values).unwrap();

        partition.add_file(file, 100).unwrap();

        assert!(
            !partition.high_cardinality_columns.contains("col"),
            "Low-cardinality column with long keys must NOT be excluded"
        );
        assert!(
            partition.summary.contains_key("col"),
            "Column must be included in the summary"
        );
        assert_eq!(
            partition.column_cardinality.get("col").copied(),
            Some(5),
            "Cardinality must reflect actual key count, not byte-based estimate"
        );
    }

    #[test]
    fn test_hierarchical_cardinality_uses_actual_count_not_byte_estimate() {
        let mut index = HierarchicalSkipIndex::new();

        let long_keys: Vec<String> = (0..5)
            .map(|i| format!("this_is_a_deliberately_long_key_prefix_{:04}", i))
            .collect();
        let mut values = HashMap::new();
        values.insert("col".to_string(), long_keys);
        let file = FileSkipIndex::build("long_keys.parquet", values).unwrap();

        index.add_file("2025/01", file, 100).unwrap();

        assert!(
            !index.is_high_cardinality("col"),
            "Low-cardinality column with long keys must NOT be globally excluded"
        );
        assert!(
            index.global_summary.contains_key("col"),
            "Column must be included in the global summary"
        );
        assert_eq!(
            index.global_cardinality.get("col").copied(),
            Some(5),
            "Global cardinality must reflect actual key count"
        );
    }

    #[test]
    fn test_numeric_stats_deserialize_swapped_bounds() {
        let json = r#"{"min": 100.0, "max": 1.0, "null_count": 0, "value_count": 10}"#;
        let stats: NumericColumnStats = serde_json::from_str(json).unwrap();
        assert_eq!(stats.min(), 1.0, "Deserialization must swap min > max");
        assert_eq!(stats.max(), 100.0, "Deserialization must swap min > max");
    }

    /// Verify the NaN normalization logic directly, since JSON cannot represent
    /// NaN as a float. Binary serde formats (bincode, rmp) can produce NaN,
    /// so the Deserialize impl must handle it.
    #[test]
    fn test_numeric_stats_nan_normalization_invariant() {
        let nan_min = NumericColumnStats::new_unchecked(f64::NAN, 100.0, 0, 10);
        assert!(nan_min.might_contain(50.0), "NaN min must return true (conservative)");
        assert!(nan_min.might_contain(200.0), "NaN min must return true for out-of-range too");

        let nan_max = NumericColumnStats::new_unchecked(0.0, f64::NAN, 0, 10);
        assert!(nan_max.might_contain(50.0), "NaN max must return true (conservative)");

        assert!(NumericColumnStats::new(f64::NAN, 100.0, 0, 10).is_none());
        assert!(NumericColumnStats::new(0.0, f64::NAN, 0, 10).is_none());
        assert!(NumericColumnStats::new(f64::NAN, f64::NAN, 0, 0).is_none());
    }

    #[test]
    fn test_partition_add_file_duplicate_skips_fst_recomputation() {
        let mut partition = PartitionIndex::new("2025/01");

        let mut values = HashMap::new();
        values.insert("col".to_string(), vec!["a".to_string(), "b".to_string()]);
        let file = FileSkipIndex::build("file1.parquet", values.clone()).unwrap();
        partition.add_file(file, 100).unwrap();

        assert_eq!(partition.files.len(), 1);
        assert_eq!(partition.estimated_rows, 100);

        // Add the same file again
        let file_dup = FileSkipIndex::build("file1.parquet", values).unwrap();
        partition.add_file(file_dup, 100).unwrap();

        assert_eq!(partition.files.len(), 1, "Duplicate file must not increase file count");
        assert_eq!(partition.estimated_rows, 100, "Duplicate file must not double-count rows");
    }

    #[test]
    fn test_partition_add_file_different_paths_counted_separately() {
        let mut partition = PartitionIndex::new("2025/01");

        let mut values = HashMap::new();
        values.insert("col".to_string(), vec!["a".to_string()]);
        let file1 = FileSkipIndex::build("file1.parquet", values.clone()).unwrap();
        partition.add_file(file1, 50).unwrap();

        let file2 = FileSkipIndex::build("file2.parquet", values).unwrap();
        partition.add_file(file2, 75).unwrap();

        assert_eq!(partition.files.len(), 2);
        assert_eq!(partition.estimated_rows, 125);
    }

    #[test]
    fn test_partition_replacement_file_updates_summary() {
        let mut partition = PartitionIndex::new("2025/01");

        let mut v1 = HashMap::new();
        v1.insert("name".to_string(), vec!["Alice".to_string(), "Bob".to_string()]);
        let file = FileSkipIndex::build("data.parquet", v1).unwrap();
        partition.add_file(file, 100).unwrap();

        assert!(partition.summary.get("name").unwrap().contains("Alice"));
        assert!(partition.summary.get("name").unwrap().contains("Bob"));

        let mut v2 = HashMap::new();
        v2.insert("name".to_string(), vec!["Alice".to_string(), "Charlie".to_string()]);
        let replacement = FileSkipIndex::build("data.parquet", v2).unwrap();
        partition.add_file(replacement, 100).unwrap();

        assert_eq!(partition.files.len(), 1, "Replacement must not increase file count");
        assert_eq!(partition.estimated_rows, 100, "Replacement must not double-count rows");

        let summary = partition.summary.get("name").expect("name column must remain in summary");
        assert!(summary.contains("Alice"), "Retained value must remain in summary");
        assert!(
            summary.contains("Charlie"),
            "New value from replacement file must appear in summary"
        );
        assert!(
            summary.contains("Bob"),
            "Old value remains in summary (monotonic union, acceptable false positive)"
        );
    }

    #[test]
    fn test_hierarchical_replacement_file_updates_both_summaries() {
        let mut index = HierarchicalSkipIndex::new();

        let mut v1 = HashMap::new();
        v1.insert("city".to_string(), vec!["NYC".to_string(), "LA".to_string()]);
        let file = FileSkipIndex::build("data.parquet", v1).unwrap();
        index.add_file("2025/01", file, 200).unwrap();

        assert!(index.global_summary.get("city").unwrap().contains("NYC"));
        assert!(index.global_summary.get("city").unwrap().contains("LA"));

        let mut v2 = HashMap::new();
        v2.insert("city".to_string(), vec!["NYC".to_string(), "Chicago".to_string()]);
        let replacement = FileSkipIndex::build("data.parquet", v2).unwrap();
        index.add_file("2025/01", replacement, 200).unwrap();

        let global = index.global_summary.get("city").expect("city must be in global summary");
        assert!(global.contains("Chicago"), "New value must appear in global summary");
        assert!(global.contains("NYC"), "Retained value must remain in global summary");

        let partition = index.partitions.get("2025/01").expect("partition must exist");
        let part_summary = partition.summary.get("city").expect("city must be in partition summary");
        assert!(
            part_summary.contains("Chicago"),
            "New value must appear in partition summary (consistency with global)"
        );
        assert!(part_summary.contains("NYC"), "Retained value must remain in partition summary");

        assert_eq!(partition.files.len(), 1, "Replacement must not increase file count");
        assert_eq!(partition.estimated_rows, 200, "Replacement must not double-count rows");
    }

    #[test]
    fn test_partition_add_file_saturating_sub_no_panic() {
        let mut partition = PartitionIndex::new("test");

        let mut values = HashMap::new();
        values.insert("col".to_string(), vec!["a".to_string()]);
        let file = FileSkipIndex::build("file1.parquet", values.clone()).unwrap();
        partition.add_file(file, 100).unwrap();

        // Manually corrupt estimated_rows to be lower than old_rows
        partition.estimated_rows = 10;

        let replacement = FileSkipIndex::build("file1.parquet", values).unwrap();
        // Must not panic even though old_rows (100) > estimated_rows (10)
        partition.add_file(replacement, 50).unwrap();

        assert_eq!(partition.estimated_rows, 50, "Should recover via saturating_sub");
    }

    #[test]
    fn test_hierarchical_total_files_stable_on_replacement() {
        let mut index = HierarchicalSkipIndex::new();

        let mut v1 = HashMap::new();
        v1.insert("col".to_string(), vec!["a".to_string()]);
        let file1 = FileSkipIndex::build("file1.parquet", v1.clone()).unwrap();
        index.add_file("2025/01", file1, 100).unwrap();
        assert_eq!(index.total_files(), 1);

        let file2 = FileSkipIndex::build("file2.parquet", v1.clone()).unwrap();
        index.add_file("2025/01", file2, 100).unwrap();
        assert_eq!(index.total_files(), 2);

        // Replace file1 — total_files must stay 2, not increment to 3
        let replacement = FileSkipIndex::build("file1.parquet", v1).unwrap();
        index.add_file("2025/01", replacement, 100).unwrap();
        assert_eq!(index.total_files(), 2, "Replacing a file must not inflate total_files");
    }

    #[test]
    fn test_add_range_tightens_existing_bounds() {
        let mut preds = SkipPredicates::new();
        preds.add_gte("price", "20");
        preds.add_range("price", "10", "50");
        let range = preds.ranges.get("price").unwrap();
        assert_eq!(
            range.min_value.as_deref(),
            Some("20"),
            "add_range must not widen an already-tighter lower bound"
        );
        assert_eq!(
            range.max_value.as_deref(),
            Some("50"),
            "add_range must set the upper bound from the range"
        );
    }

    #[test]
    fn test_add_range_on_empty_predicates() {
        let mut preds = SkipPredicates::new();
        preds.add_range("price", "10", "50");
        let range = preds.ranges.get("price").unwrap();
        assert_eq!(range.min_value.as_deref(), Some("10"));
        assert_eq!(range.max_value.as_deref(), Some("50"));
    }

    #[test]
    fn test_cardinality_overflow_saturates_instead_of_panicking() {
        let mut partition = PartitionIndex::new("overflow_test");

        let mut seed_values = HashMap::new();
        seed_values.insert("col".to_string(), vec!["seed".to_string()]);
        let seed = FileSkipIndex::build("seed.parquet", seed_values).unwrap();
        partition.add_file(seed, 1).unwrap();

        // Simulate near-overflow: set existing cardinality close to usize::MAX
        partition.column_cardinality.insert("col".to_string(), usize::MAX - 1);

        let mut new_values = HashMap::new();
        new_values.insert("col".to_string(), vec!["overflow_trigger".to_string()]);
        let new_file = FileSkipIndex::build("overflow.parquet", new_values).unwrap();

        // Must not panic; saturating_add caps at usize::MAX which exceeds
        // MAX_SUMMARY_CARDINALITY, so the column is excluded.
        partition.add_file(new_file, 1).unwrap();

        assert!(
            partition.high_cardinality_columns.contains("col"),
            "Overflow must be detected and column excluded from summary"
        );
        assert!(
            !partition.summary.contains_key("col"),
            "Summary must be removed for overflowed column"
        );
    }

    // ---- Token search predicate tests ----

    #[test]
    fn test_token_search_prunes_files() {
        let fts_col = format!("{}message", crate::warehouse::indexes::fulltext_index::FTS_COLUMN_PREFIX);

        let mut file1_cols = HashMap::new();
        file1_cols.insert(fts_col.clone(), vec![
            "timeout".to_string(), "error".to_string(), "connection".to_string(),
        ]);
        let file1 = FileSkipIndex::build("file1.parquet", file1_cols).unwrap();

        let mut file2_cols = HashMap::new();
        file2_cols.insert(fts_col.clone(), vec![
            "request".to_string(), "completed".to_string(), "success".to_string(),
        ]);
        let file2 = FileSkipIndex::build("file2.parquet", file2_cols).unwrap();

        let mut index = HierarchicalSkipIndex::new();
        let mut partition = PartitionIndex::new("default");
        partition.add_file(file1, 100).unwrap();
        partition.add_file(file2, 100).unwrap();
        index.partitions.insert("default".to_string(), partition);
        index.total_files = 2;

        let mut predicates = SkipPredicates::new();
        predicates.add_token("message", "timeout");

        let files = index.filter_with_skip_predicates(&predicates, None);
        assert_eq!(files.len(), 1);
        assert!(files.contains(&"file1.parquet"));
    }

    #[test]
    fn test_token_search_and_semantics() {
        let fts_col = format!("{}message", crate::warehouse::indexes::fulltext_index::FTS_COLUMN_PREFIX);

        let mut file1_cols = HashMap::new();
        file1_cols.insert(fts_col.clone(), vec![
            "timeout".to_string(), "error".to_string(),
        ]);
        let file1 = FileSkipIndex::build("file1.parquet", file1_cols).unwrap();

        let mut index = HierarchicalSkipIndex::new();
        let mut partition = PartitionIndex::new("default");
        partition.add_file(file1, 100).unwrap();
        index.partitions.insert("default".to_string(), partition);
        index.total_files = 1;

        let mut predicates = SkipPredicates::new();
        predicates.add_token("message", "timeout");
        predicates.add_token("message", "nonexistent");

        let files = index.filter_with_skip_predicates(&predicates, None);
        assert!(files.is_empty(), "AND semantics: all tokens must be present");
    }

    #[test]
    fn test_token_search_missing_column_returns_file() {
        let mut file1_cols = HashMap::new();
        file1_cols.insert("status".to_string(), vec!["active".to_string()]);
        let file1 = FileSkipIndex::build("file1.parquet", file1_cols).unwrap();

        let mut index = HierarchicalSkipIndex::new();
        let mut partition = PartitionIndex::new("default");
        partition.add_file(file1, 100).unwrap();
        index.partitions.insert("default".to_string(), partition);
        index.total_files = 1;

        let mut predicates = SkipPredicates::new();
        predicates.add_token("body", "something");

        let files = index.filter_with_skip_predicates(&predicates, None);
        assert_eq!(files.len(), 1, "missing FTS column should not prune the file");
    }

    #[test]
    fn test_token_search_is_empty() {
        let mut preds = SkipPredicates::new();
        assert!(preds.is_empty());
        preds.add_token("col", "tok");
        assert!(!preds.is_empty());
    }
}
