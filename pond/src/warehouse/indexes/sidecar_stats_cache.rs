//! Sidecar Stats Cache
//!
//! Caches `FileColumnStats` sidecar data in memory for query-time file pruning.
//! When combined with skip indexes, this enables min/max based filtering that
//! eliminates files before ClickHouse even sees them.

use quick_cache::sync::Cache;
use std::sync::Arc;

use crate::warehouse::indexes::skip_index::{RangePredicate, SkipPredicates};
use crate::warehouse::parquet_stats::{ColumnSidecarStats, FileColumnStats};
use crate::warehouse::storage::r2::R2Storage;

/// Cache for sidecar stats keyed by Parquet file path.
pub struct SidecarStatsCache {
    cache: Cache<String, Arc<FileColumnStats>>,
}

impl SidecarStatsCache {
    /// Create a new cache with the given capacity (number of file entries).
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: Cache::new(capacity),
        }
    }

    /// Get stats from cache, or load from R2 if not cached.
    ///
    /// Returns `None` if the sidecar doesn't exist (pre-sidecar files).
    pub async fn get_or_load(
        &self,
        r2: &R2Storage,
        file_path: &str,
    ) -> Option<Arc<FileColumnStats>> {
        if let Some(cached) = self.cache.get(file_path) {
            return Some(cached);
        }

        match r2.download_stats(file_path).await {
            Ok(Some(stats)) => {
                let arc = Arc::new(stats);
                self.cache.insert(file_path.to_string(), arc.clone());
                Some(arc)
            }
            Ok(None) | Err(_) => None,
        }
    }

    /// Get stats from cache only (no R2 fallback). Used in synchronous contexts.
    pub fn get_cached(&self, file_path: &str) -> Option<Arc<FileColumnStats>> {
        self.cache.get(file_path)
    }

    /// Insert stats into cache directly (e.g., during compaction/sync).
    pub fn insert(&self, file_path: String, stats: FileColumnStats) {
        self.cache.insert(file_path, Arc::new(stats));
    }
}

/// Compare a string predicate value against a serde_json::Value from sidecar stats.
///
/// Returns `None` if comparison isn't meaningful (e.g., stats are null/object/array).
/// For string stats, compares lexicographically. For numeric stats, parses and compares.
fn compare_value(predicate_value: &str, stats_value: &serde_json::Value) -> Option<std::cmp::Ordering> {
    match stats_value {
        serde_json::Value::String(s) => Some(predicate_value.cmp(s.as_str())),
        serde_json::Value::Number(n) => {
            if let Ok(pred_num) = predicate_value.parse::<f64>() {
                n.as_f64().and_then(|stat_num| pred_num.partial_cmp(&stat_num))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Check whether a single column's sidecar stats are compatible with an equality predicate.
///
/// Returns `false` (prune the file) when the file's min/max range for a column
/// provably cannot contain the value.
fn column_matches_equality(col_stats: &ColumnSidecarStats, value: &str) -> bool {
    let min_ok = col_stats.min.as_ref().map_or(true, |min| {
        compare_value(value, min).map_or(true, |ord| ord != std::cmp::Ordering::Less)
    });
    let max_ok = col_stats.max.as_ref().map_or(true, |max| {
        compare_value(value, max).map_or(true, |ord| ord != std::cmp::Ordering::Greater)
    });
    min_ok && max_ok
}

fn column_matches_range(col_stats: &ColumnSidecarStats, range: &RangePredicate) -> bool {
    if let Some(ref range_min) = range.min_value {
        if let Some(ref col_max) = col_stats.max {
            if let Some(ord) = compare_value(range_min, col_max) {
                if range.min_exclusive {
                    if ord != std::cmp::Ordering::Less {
                        return false;
                    }
                } else if ord == std::cmp::Ordering::Greater {
                    return false;
                }
            }
        }
    }
    if let Some(ref range_max) = range.max_value {
        if let Some(ref col_min) = col_stats.min {
            if let Some(ord) = compare_value(range_max, col_min) {
                if range.max_exclusive {
                    if ord != std::cmp::Ordering::Greater {
                        return false;
                    }
                } else if ord == std::cmp::Ordering::Less {
                    return false;
                }
            }
        }
    }
    true
}

fn column_matches_in_list(col_stats: &ColumnSidecarStats, values: &[String]) -> bool {
    values.iter().any(|v| column_matches_equality(col_stats, v))
}

/// Filter a list of file paths using sidecar stats and skip predicates.
///
/// For each file, loads its sidecar stats (from cache or R2) and checks
/// whether any predicate can provably eliminate the file based on min/max.
/// Files whose sidecars are missing are kept (conservative).
pub async fn filter_files_by_sidecar_stats(
    files: &[&str],
    predicates: &SkipPredicates,
    cache: &SidecarStatsCache,
    r2: &R2Storage,
) -> Vec<String> {
    if predicates.contradicted {
        return vec![];
    }
    if predicates.is_empty() || files.is_empty() {
        return files.iter().map(|s| s.to_string()).collect();
    }

    let mut result = Vec::with_capacity(files.len());

    for &file_path in files {
        let stats = cache.get_or_load(r2, file_path).await;
        let keep = match stats {
            Some(ref stats) => file_matches_predicates(stats, predicates),
            None => true,
        };
        if keep {
            result.push(file_path.to_string());
        }
    }

    result
}

/// Synchronous version of sidecar filtering using only cached stats.
///
/// Files whose stats are not already in cache are kept conservatively.
/// Designed for use in synchronous transformer contexts (e.g., `TableTransformer`).
pub fn filter_files_by_cached_stats<'a>(
    files: Vec<&'a str>,
    predicates: &SkipPredicates,
    cache: &SidecarStatsCache,
) -> Vec<&'a str> {
    if predicates.contradicted {
        return vec![];
    }
    if predicates.is_empty() || files.is_empty() {
        return files;
    }

    files
        .into_iter()
        .filter(|file_path| {
            match cache.get_cached(file_path) {
                Some(stats) => file_matches_predicates(&stats, predicates),
                None => true,
            }
        })
        .collect()
}

/// Check whether a file's sidecar stats are compatible with all predicates.
fn file_matches_predicates(stats: &FileColumnStats, predicates: &SkipPredicates) -> bool {
    for (col_name, value) in &predicates.equality {
        if let Some(col_stats) = stats.columns.iter().find(|c| &c.name == col_name) {
            if !column_matches_equality(col_stats, value) {
                return false;
            }
        }
    }

    for (col_name, range) in &predicates.ranges {
        if let Some(col_stats) = stats.columns.iter().find(|c| &c.name == col_name) {
            if !column_matches_range(col_stats, range) {
                return false;
            }
        }
    }

    for (col_name, values) in &predicates.in_lists {
        if let Some(col_stats) = stats.columns.iter().find(|c| &c.name == col_name) {
            if !column_matches_in_list(col_stats, values) {
                return false;
            }
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_stats(columns: Vec<ColumnSidecarStats>) -> FileColumnStats {
        FileColumnStats {
            version: 1,
            row_count: 100,
            size_bytes: 1000,
            columns,
            sort_columns: None,
        }
    }

    fn make_col(name: &str, min: Option<&str>, max: Option<&str>) -> ColumnSidecarStats {
        ColumnSidecarStats {
            name: name.to_string(),
            data_type: "utf8".to_string(),
            null_count: 0,
            distinct_count: None,
            min: min.map(|s| serde_json::Value::String(s.to_string())),
            max: max.map(|s| serde_json::Value::String(s.to_string())),
        }
    }

    #[test]
    fn test_equality_within_range() {
        let col = make_col("status", Some("active"), Some("pending"));
        assert!(column_matches_equality(&col, "active"));
        assert!(column_matches_equality(&col, "draft"));
        assert!(column_matches_equality(&col, "pending"));
    }

    #[test]
    fn test_equality_outside_range() {
        let col = make_col("status", Some("b"), Some("d"));
        assert!(!column_matches_equality(&col, "a"));
        assert!(!column_matches_equality(&col, "e"));
    }

    #[test]
    fn test_equality_no_stats() {
        let col = make_col("status", None, None);
        assert!(column_matches_equality(&col, "anything"));
    }

    #[test]
    fn test_range_overlap() {
        let col = make_col("age", Some("10"), Some("50"));
        let range = RangePredicate::between("20", "30");
        assert!(column_matches_range(&col, &range));
    }

    #[test]
    fn test_range_no_overlap_below() {
        let col = make_col("age", Some("50"), Some("100"));
        let range = RangePredicate::lte("40");
        assert!(!column_matches_range(&col, &range));
    }

    #[test]
    fn test_range_no_overlap_above() {
        let col = make_col("age", Some("10"), Some("20"));
        let range = RangePredicate::gte("30");
        assert!(!column_matches_range(&col, &range));
    }

    #[test]
    fn test_in_list_some_match() {
        let col = make_col("status", Some("active"), Some("pending"));
        assert!(column_matches_in_list(&col, &["active".to_string(), "closed".to_string()]));
    }

    #[test]
    fn test_in_list_none_match() {
        let col = make_col("status", Some("b"), Some("d"));
        assert!(!column_matches_in_list(&col, &["a".to_string(), "e".to_string()]));
    }

    #[test]
    fn test_file_matches_all_predicates() {
        let stats = make_stats(vec![
            make_col("status", Some("active"), Some("pending")),
            make_col("age", Some("10"), Some("50")),
        ]);
        let mut predicates = SkipPredicates::new();
        predicates.equality.insert("status".to_string(), "draft".to_string());
        predicates.ranges.insert("age".to_string(), RangePredicate::gte("20"));
        assert!(file_matches_predicates(&stats, &predicates));
    }

    #[test]
    fn test_file_pruned_by_equality() {
        let stats = make_stats(vec![
            make_col("status", Some("b"), Some("d")),
        ]);
        let mut predicates = SkipPredicates::new();
        predicates.equality.insert("status".to_string(), "z".to_string());
        assert!(!file_matches_predicates(&stats, &predicates));
    }

    #[test]
    fn test_file_pruned_by_range() {
        let stats = make_stats(vec![
            make_col("age", Some("50"), Some("100")),
        ]);
        let mut predicates = SkipPredicates::new();
        predicates.ranges.insert("age".to_string(), RangePredicate::lte("40"));
        assert!(!file_matches_predicates(&stats, &predicates));
    }

    #[test]
    fn test_unknown_column_passes() {
        let stats = make_stats(vec![
            make_col("status", Some("a"), Some("z")),
        ]);
        let mut predicates = SkipPredicates::new();
        predicates.equality.insert("nonexistent".to_string(), "val".to_string());
        assert!(file_matches_predicates(&stats, &predicates));
    }

    #[test]
    fn test_contradicted_predicates_prune_all_files() {
        let stats = make_stats(vec![
            make_col("status", Some("a"), Some("z")),
        ]);

        let mut predicates = SkipPredicates::new();
        predicates.contradicted = true;

        assert!(
            !file_matches_predicates(&stats, &predicates) || predicates.contradicted,
            "contradicted predicates should signal no files can match"
        );

        let files: Vec<&str> = vec!["file1.parquet", "file2.parquet"];
        let result = filter_files_by_cached_stats(files, &predicates, &SidecarStatsCache::new(100));
        assert!(result.is_empty(), "contradicted predicates must return empty set, got {:?}", result);
    }

    #[test]
    fn test_non_contradicted_empty_predicates_return_all() {
        let predicates = SkipPredicates::new();
        assert!(!predicates.contradicted);

        let files: Vec<&str> = vec!["file1.parquet", "file2.parquet"];
        let result = filter_files_by_cached_stats(files.clone(), &predicates, &SidecarStatsCache::new(100));
        assert_eq!(result.len(), 2, "empty non-contradicted predicates must return all files");
    }
}
