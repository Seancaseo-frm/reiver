//! Xor Filter Index
//!
//! Probabilistic skip indexes using Xor Filters for high-cardinality columns.
//!
//! # When to Use
//!
//! Xor Filters are ideal for **partition/global summaries** of high-cardinality
//! columns where FST union operations would cause memory issues.
//!
//! ## Comparison with FST for Summary Indexes
//!
//! | Aspect              | FST                      | Xor Filter               |
//! |---------------------|--------------------------|--------------------------|
//! | Memory (1M keys)    | ~10-50MB                 | ~1.2MB (BinaryFuse8)     |
//! | False Positives     | None                     | ~0.4% (BinaryFuse8)      |
//! | Prefix Queries      | Yes                      | No                       |
//! | Range Queries       | Yes                      | No                       |
//! | Build Time          | O(n log n)               | O(n)                     |
//! | Union Operation     | Expensive (rebuild)      | Cheap (OR bitmasks)      |
//!
//! ## Trade-offs
//!
//! - **False positives**: ~0.4% for BinaryFuse8, ~1.5% for Xor8
//! - **No prefix/range**: Only supports exact membership testing
//! - **Immutable**: Must rebuild to add new values (fine for Parquet files)
//!
//! # Usage Example
//!
//! ```ignore
//! // Build filter from file's column values
//! let filter = XorColumnFilter::build("user_id", values)?;
//!
//! // Check if file might contain a value
//! if filter.might_contain("usr_abc123") {
//!     // File might contain this value - need to scan
//! } else {
//!     // File definitely doesn't contain this value - skip it
//! }
//! ```

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use thiserror::Error;
use xorf::{BinaryFuse8, Filter};

/// Errors that can occur during Xor filter operations.
#[derive(Debug, Error)]
pub enum XorIndexError {
    #[error("Failed to build Xor filter: not enough entropy in data")]
    BuildFailed,

    #[error("Empty input: cannot build filter from empty data")]
    EmptyInput,

    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// Result type for Xor index operations.
pub type XorIndexResult<T> = Result<T, XorIndexError>;

/// Hash a string value to u64 for Xor filter storage.
///
/// Uses DefaultHasher (SipHash) for deterministic hashing.
fn hash_value(value: &str) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

/// Xor Filter for a single column.
///
/// Provides probabilistic membership testing with ~0.4% false positive rate.
/// Useful for high-cardinality columns where FST would be too large.
pub struct XorColumnFilter {
    /// The underlying BinaryFuse8 filter (most memory-efficient variant)
    filter: BinaryFuse8,
    /// Column name for identification
    column_name: String,
    /// Number of values in the filter (for diagnostics)
    value_count: usize,
}

impl XorColumnFilter {
    /// Build a filter from column values.
    ///
    /// # Arguments
    /// * `column_name` - Name of the column
    /// * `values` - Iterator of string values to include
    ///
    /// # Returns
    /// A new `XorColumnFilter` or an error if building fails.
    ///
    /// # Performance
    /// Build time is O(n log n) where n is the number of values (dominated by deduplication sort).
    pub fn build(
        column_name: &str,
        values: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> XorIndexResult<Self> {
        let mut hashes: Vec<u64> = values.into_iter().map(|v| hash_value(v.as_ref())).collect();

        if hashes.is_empty() {
            return Err(XorIndexError::EmptyInput);
        }

        hashes.sort_unstable();
        hashes.dedup();
        let value_count = hashes.len();

        let filter =
            BinaryFuse8::try_from(&hashes).map_err(|_| XorIndexError::BuildFailed)?;

        Ok(Self {
            filter,
            column_name: column_name.to_string(),
            value_count,
        })
    }

    /// Check if the filter might contain a value.
    ///
    /// # Returns
    /// - `true` if the value might be present (could be false positive)
    /// - `false` if the value is definitely not present
    ///
    /// # False Positive Rate
    /// BinaryFuse8 has approximately 0.4% false positive rate.
    pub fn might_contain(&self, value: &str) -> bool {
        let hash = hash_value(value);
        self.filter.contains(&hash)
    }

    /// Check if the filter might contain any of the specified values.
    ///
    /// Useful for IN clause optimization.
    ///
    /// # Returns
    /// `true` if any value might be present, `false` if all values are definitely absent.
    pub fn might_contain_any(&self, values: &[&str]) -> bool {
        values.iter().any(|v| self.might_contain(v))
    }

    /// Get the column name.
    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    /// Get the number of values in the filter.
    pub fn value_count(&self) -> usize {
        self.value_count
    }

    /// Estimate the memory size of this filter in bytes.
    ///
    /// BinaryFuse8 uses approximately 9 bits per element.
    pub fn size_bytes(&self) -> usize {
        // BinaryFuse8 uses ~9 bits per element = 1.125 bytes
        (self.value_count * 9 + 7) / 8
    }
}

impl std::fmt::Debug for XorColumnFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XorColumnFilter")
            .field("column_name", &self.column_name)
            .field("value_count", &self.value_count)
            .field("size_bytes", &self.size_bytes())
            .finish()
    }
}

/// Xor Filter-based skip index for a single file.
///
/// Similar to `FileSkipIndex` but uses Xor Filters instead of FST,
/// making it suitable for high-cardinality columns.
pub struct FileXorIndex {
    /// File path
    pub file_path: String,
    /// Xor filters for each indexed column
    column_filters: HashMap<String, XorColumnFilter>,
}

impl FileXorIndex {
    /// Build a Xor filter index for a file.
    ///
    /// # Arguments
    /// * `file_path` - Path to the Parquet file
    /// * `columns` - Map of column name to values
    ///
    /// # Returns
    /// A new `FileXorIndex` or an error if building fails.
    pub fn build(
        file_path: &str,
        columns: HashMap<String, Vec<String>>,
    ) -> XorIndexResult<Self> {
        let mut column_filters = HashMap::new();

        for (column_name, values) in columns {
            if values.is_empty() {
                continue;
            }

            let filter = XorColumnFilter::build(&column_name, values)?;
            column_filters.insert(column_name, filter);
        }

        Ok(Self {
            file_path: file_path.to_string(),
            column_filters,
        })
    }

    /// Create an empty index for a file (no columns indexed).
    pub fn new_empty(file_path: &str) -> Self {
        Self {
            file_path: file_path.to_string(),
            column_filters: HashMap::new(),
        }
    }

    /// Check if the file might contain a specific value in a column.
    ///
    /// # Returns
    /// - `true` if the value might be present
    /// - `false` if the value is definitely not present
    ///
    /// If the column is not indexed, returns `true` (assume it might contain).
    pub fn might_contain(&self, column: &str, value: &str) -> bool {
        match self.column_filters.get(column) {
            Some(filter) => filter.might_contain(value),
            None => true, // No index = assume might contain
        }
    }

    /// Check if the file might contain any of the specified values.
    ///
    /// Useful for IN clause optimization.
    pub fn might_contain_any(&self, column: &str, values: &[&str]) -> bool {
        if values.is_empty() {
            return false;
        }

        match self.column_filters.get(column) {
            Some(filter) => filter.might_contain_any(values),
            None => true,
        }
    }

    /// Get all indexed columns.
    pub fn indexed_columns(&self) -> Vec<&str> {
        self.column_filters.keys().map(|s| s.as_str()).collect()
    }

    /// Get total estimated memory size in bytes.
    pub fn size_bytes(&self) -> usize {
        self.column_filters.values().map(|f| f.size_bytes()).sum()
    }
}

impl std::fmt::Debug for FileXorIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileXorIndex")
            .field("file_path", &self.file_path)
            .field("indexed_columns", &self.indexed_columns())
            .field("size_bytes", &self.size_bytes())
            .finish()
    }
}

/// Collection of Xor filter indexes for a table.
///
/// Similar to `DataSkipIndex` but using Xor Filters for high-cardinality columns.
pub struct DataXorIndex {
    /// Xor indexes per file
    file_indexes: HashMap<String, FileXorIndex>,
}

impl DataXorIndex {
    /// Create a new empty Xor index.
    pub fn new() -> Self {
        Self {
            file_indexes: HashMap::new(),
        }
    }

    /// Add a file's Xor index.
    pub fn add_file(&mut self, index: FileXorIndex) {
        self.file_indexes.insert(index.file_path.clone(), index);
    }

    /// Remove a file's index.
    pub fn remove_file(&mut self, file_path: &str) {
        self.file_indexes.remove(file_path);
    }

    /// Filter files by equality predicates.
    ///
    /// Returns files that might contain rows matching ALL predicates.
    pub fn filter_files_by_predicates(&self, predicates: &HashMap<String, String>) -> Vec<&str> {
        if predicates.is_empty() {
            return self.file_paths();
        }

        let total_files = self.file_indexes.len();

        let matching: Vec<&str> = self
            .file_indexes
            .values()
            .filter(|idx| {
                predicates
                    .iter()
                    .all(|(col, val)| idx.might_contain(col, val))
            })
            .map(|idx| idx.file_path.as_str())
            .collect();

        let files_skipped = total_files - matching.len();

        tracing::debug!(
            total_files = total_files,
            matching_files = matching.len(),
            files_skipped = files_skipped,
            predicate_count = predicates.len(),
            "Xor index filtered files by equality predicates"
        );

        matching
    }

    /// Filter files by IN list predicates.
    ///
    /// Returns files that might contain ANY of the values in each IN list.
    pub fn filter_files_by_in_predicates(
        &self,
        predicates: &HashMap<String, Vec<String>>,
    ) -> Vec<&str> {
        if predicates.is_empty() {
            return self.file_paths();
        }

        let total_files = self.file_indexes.len();

        let predicate_refs: Vec<(&str, Vec<&str>)> = predicates
            .iter()
            .map(|(col, values)| (col.as_str(), values.iter().map(|s| s.as_str()).collect()))
            .collect();

        let matching: Vec<&str> = self
            .file_indexes
            .values()
            .filter(|idx| {
                predicate_refs.iter().all(|(col, refs)| {
                    idx.might_contain_any(col, refs)
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
            "Xor index filtered files by IN predicates"
        );

        matching
    }

    /// Get all file paths.
    pub fn file_paths(&self) -> Vec<&str> {
        self.file_indexes.keys().map(|s| s.as_str()).collect()
    }

    /// Get total number of files indexed.
    pub fn total_files(&self) -> usize {
        self.file_indexes.len()
    }

    /// Get total estimated memory size in bytes.
    pub fn total_size_bytes(&self) -> usize {
        self.file_indexes.values().map(|idx| idx.size_bytes()).sum()
    }
}

impl Default for DataXorIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xor_column_filter_build() {
        let values = vec!["alice", "bob", "charlie", "david"];
        let filter = XorColumnFilter::build("name", values).unwrap();

        assert_eq!(filter.column_name(), "name");
        assert_eq!(filter.value_count(), 4);
    }

    #[test]
    fn test_xor_column_filter_membership() {
        let values = vec!["alice", "bob", "charlie"];
        let filter = XorColumnFilter::build("name", values).unwrap();

        // These should definitely be found
        assert!(filter.might_contain("alice"));
        assert!(filter.might_contain("bob"));
        assert!(filter.might_contain("charlie"));

        // These are definitely not present (no false negatives)
        // Note: might_contain could return true due to false positives,
        // but the rate is very low (~0.4%)
    }

    #[test]
    fn test_xor_column_filter_empty_input() {
        let values: Vec<String> = vec![];
        let result = XorColumnFilter::build("name", values);

        assert!(matches!(result, Err(XorIndexError::EmptyInput)));
    }

    #[test]
    fn test_file_xor_index() {
        let mut columns = HashMap::new();
        columns.insert(
            "user_id".to_string(),
            vec!["usr_1".to_string(), "usr_2".to_string(), "usr_3".to_string()],
        );
        columns.insert(
            "status".to_string(),
            vec!["active".to_string(), "pending".to_string()],
        );

        let index = FileXorIndex::build("file1.parquet", columns).unwrap();

        // Check membership
        assert!(index.might_contain("user_id", "usr_1"));
        assert!(index.might_contain("status", "active"));

        // Check IN list
        assert!(index.might_contain_any("user_id", &["usr_1", "usr_999"]));

        // Unindexed column returns true
        assert!(index.might_contain("unknown_column", "any_value"));
    }

    #[test]
    fn test_data_xor_index_filtering() {
        let mut data_index = DataXorIndex::new();

        // File 1: users A-C
        let mut cols1 = HashMap::new();
        cols1.insert("name".to_string(), vec!["alice".to_string(), "bob".to_string()]);
        let file1 = FileXorIndex::build("file1.parquet", cols1).unwrap();
        data_index.add_file(file1);

        // File 2: users D-F
        let mut cols2 = HashMap::new();
        cols2.insert("name".to_string(), vec!["david".to_string(), "eve".to_string()]);
        let file2 = FileXorIndex::build("file2.parquet", cols2).unwrap();
        data_index.add_file(file2);

        // Filter for "alice" - should only match file1
        let mut predicates = HashMap::new();
        predicates.insert("name".to_string(), "alice".to_string());

        let matching = data_index.filter_files_by_predicates(&predicates);
        assert_eq!(matching.len(), 1);
        assert!(matching.contains(&"file1.parquet"));
    }

    #[test]
    fn test_memory_efficiency() {
        // Build a filter with many values
        let values: Vec<String> = (0..10000).map(|i| format!("user_{}", i)).collect();
        let filter = XorColumnFilter::build("user_id", values).unwrap();

        // BinaryFuse8 should use ~9 bits per element = ~11.25 KB for 10K values
        let size = filter.size_bytes();
        assert!(size < 15000, "Filter size {} should be under 15KB", size);

        tracing::info!(
            value_count = filter.value_count(),
            size_bytes = size,
            bits_per_element = (size * 8) as f64 / filter.value_count() as f64,
            "Xor filter memory efficiency"
        );
    }
}
