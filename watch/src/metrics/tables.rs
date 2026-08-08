//! Table selection logic for metrics queries.
//!
//! This module provides logic for selecting the appropriate ClickHouse table
//! based on the query time range, optimizing for query performance.

#![allow(dead_code)] // Table selection logic - some functions for future time-series optimization

use super::TimeAggregation;

/// Error type for unsupported aggregations on pre-aggregated tables
#[derive(Debug, Clone)]
pub struct UnsupportedAggregationError {
    pub aggregation: String,
    pub table: String,
    pub suggestion: String,
}

impl std::fmt::Display for UnsupportedAggregationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} is not supported on {} table. {}",
            self.aggregation, self.table, self.suggestion
        )
    }
}

impl std::error::Error for UnsupportedAggregationError {}

// Time constants in milliseconds
const ONE_HOUR_MS: i64 = 60 * 60 * 1000;
const SIX_HOURS_MS: i64 = 6 * ONE_HOUR_MS;
const ONE_DAY_MS: i64 = 24 * ONE_HOUR_MS;
const ONE_WEEK_MS: i64 = 7 * ONE_DAY_MS;

/// Which samples table to use for a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SamplesTable {
    /// Raw samples (samples_v1)
    Raw,
    /// 5-minute aggregated (samples_v1_agg_5m)
    Agg5m,
    /// 30-minute aggregated (samples_v1_agg_30m)
    Agg30m,
}

impl SamplesTable {
    /// Get the table name for this samples table.
    pub fn table_name(&self) -> &'static str {
        match self {
            SamplesTable::Raw => "samples_v1",
            SamplesTable::Agg5m => "samples_v1_agg_5m",
            SamplesTable::Agg30m => "samples_v1_agg_30m",
        }
    }

    /// Bucket width in milliseconds. Used to extend the fetch window so
    /// range-vector functions like `rate()` have enough look-back data.
    pub fn bucket_ms(&self) -> i64 {
        match self {
            SamplesTable::Raw => 0,
            SamplesTable::Agg5m => 5 * 60 * 1000,
            SamplesTable::Agg30m => 30 * 60 * 1000,
        }
    }

    /// Get the aggregation expression for a given time aggregation.
    ///
    /// For percentiles, this returns a format string with {} placeholder for the percentile value.
    ///
    /// # Errors
    /// Returns an error if the aggregation is not supported on pre-aggregated tables
    /// (e.g., CountDistinct or Percentiles on Agg5m/Agg30m tables).
    pub fn aggregation_expr(
        &self,
        time_agg: TimeAggregation,
    ) -> Result<String, UnsupportedAggregationError> {
        match self {
            SamplesTable::Raw => Ok(match time_agg {
                TimeAggregation::Sum => "sum(value)".to_string(),
                TimeAggregation::Avg => "avg(value)".to_string(),
                TimeAggregation::Min => "min(value)".to_string(),
                TimeAggregation::Max => "max(value)".to_string(),
                TimeAggregation::Count => "count(value)".to_string(),
                TimeAggregation::CountDistinct => "countDistinct(value)".to_string(),
                TimeAggregation::Rate => "sum(value)".to_string(), // Caller divides by interval
                TimeAggregation::Increase => "sum(value)".to_string(),
                TimeAggregation::Last => "anyLast(value)".to_string(),
                // Percentiles use ClickHouse's quantile function
                TimeAggregation::P50 => "quantile(0.50)(value)".to_string(),
                TimeAggregation::P75 => "quantile(0.75)(value)".to_string(),
                TimeAggregation::P90 => "quantile(0.90)(value)".to_string(),
                TimeAggregation::P95 => "quantile(0.95)(value)".to_string(),
                TimeAggregation::P99 => "quantile(0.99)(value)".to_string(),
                TimeAggregation::P999 => "quantile(0.999)(value)".to_string(),
            }),
            SamplesTable::Agg5m | SamplesTable::Agg30m => match time_agg {
                TimeAggregation::Sum => Ok("sum(sum)".to_string()),
                TimeAggregation::Avg => Ok("sum(sum) / sum(count)".to_string()),
                TimeAggregation::Min => Ok("min(min)".to_string()),
                TimeAggregation::Max => Ok("max(max)".to_string()),
                TimeAggregation::Count => Ok("sum(count)".to_string()),
                TimeAggregation::CountDistinct => Err(UnsupportedAggregationError {
                    aggregation: "CountDistinct".to_string(),
                    table: self.table_name().to_string(),
                    suggestion: "Use a shorter time range to query raw data".to_string(),
                }),
                TimeAggregation::Rate => Ok("sum(sum)".to_string()),
                TimeAggregation::Increase => Ok("sum(sum)".to_string()),
                TimeAggregation::Last => Ok("anyLast(last)".to_string()),
                // Percentiles not supported on aggregated tables
                TimeAggregation::P50
                | TimeAggregation::P75
                | TimeAggregation::P90
                | TimeAggregation::P95
                | TimeAggregation::P99
                | TimeAggregation::P999 => Err(UnsupportedAggregationError {
                    aggregation: format!("{:?}", time_agg),
                    table: self.table_name().to_string(),
                    suggestion:
                        "Use a shorter time range to query raw data for percentile calculations"
                            .to_string(),
                }),
            },
        }
    }
}

/// Which time series table to use for metadata queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeSeriesTable {
    /// Base table (time_series_v1)
    Base,
    /// 6-hour rollup (time_series_v1_6hrs)
    Hours6,
    /// 1-day rollup (time_series_v1_1day)
    Day1,
}

impl TimeSeriesTable {
    /// Get the table name for this time series table.
    pub fn table_name(&self) -> &'static str {
        match self {
            TimeSeriesTable::Base => "time_series_v1",
            TimeSeriesTable::Hours6 => "time_series_v1_6hrs",
            TimeSeriesTable::Day1 => "time_series_v1_1day",
        }
    }
}

/// Select the appropriate samples table based on query time range.
///
/// For the PromQL evaluator, all aggregation happens in DataFusion after
/// fetching rows. The agg tables provide `last` (the final raw value in
/// each window), which is sufficient for `rate()`, `increase()`, and
/// `histogram_quantile()` on cumulative counters.
pub fn select_samples_table(start_ms: i64, end_ms: i64) -> SamplesTable {
    let range = end_ms - start_ms;

    if range <= ONE_HOUR_MS {
        SamplesTable::Raw
    } else if range <= ONE_DAY_MS {
        SamplesTable::Agg5m
    } else {
        SamplesTable::Agg30m
    }
}

/// Select samples table for the SQL widget path, which needs to fall back
/// to raw data for aggregations not supported on pre-aggregated tables
/// (e.g. percentiles, count distinct).
pub fn select_samples_table_for_agg(
    start_ms: i64,
    end_ms: i64,
    time_agg: TimeAggregation,
) -> SamplesTable {
    if matches!(time_agg, TimeAggregation::CountDistinct) || time_agg.is_percentile() {
        return SamplesTable::Raw;
    }
    select_samples_table(start_ms, end_ms)
}

/// Select the appropriate time series table based on query time range.
///
/// # Arguments
/// * `start_ms` - Start time in milliseconds since epoch
/// * `end_ms` - End time in milliseconds since epoch
///
/// # Returns
/// A tuple of (adjusted_start_ms, TimeSeriesTable)
pub fn select_time_series_table(start_ms: i64, end_ms: i64) -> (i64, TimeSeriesTable) {
    let range = end_ms - start_ms;

    if range < SIX_HOURS_MS {
        // Align to hour boundary
        let aligned_start = start_ms - (start_ms % ONE_HOUR_MS);
        (aligned_start, TimeSeriesTable::Base)
    } else if range < ONE_DAY_MS {
        // Align to 6-hour boundary
        let aligned_start = start_ms - (start_ms % SIX_HOURS_MS);
        (aligned_start, TimeSeriesTable::Hours6)
    } else {
        // Align to day boundary
        let aligned_start = start_ms - (start_ms % ONE_DAY_MS);
        (aligned_start, TimeSeriesTable::Day1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_samples_table_short_range_raw() {
        // <= 1 hour uses raw table
        assert_eq!(select_samples_table(0, ONE_HOUR_MS), SamplesTable::Raw);
        assert_eq!(select_samples_table(0, 30 * 60 * 1000), SamplesTable::Raw);
    }

    #[test]
    fn test_select_samples_table_medium_range_agg5m() {
        // > 1 hour and <= 1 day uses 5m agg
        let just_over_1h = ONE_HOUR_MS + 1;
        assert_eq!(select_samples_table(0, just_over_1h), SamplesTable::Agg5m);
        assert_eq!(
            select_samples_table(0, 6 * ONE_HOUR_MS),
            SamplesTable::Agg5m
        );
        assert_eq!(select_samples_table(0, ONE_DAY_MS), SamplesTable::Agg5m);
    }

    #[test]
    fn test_select_samples_table_long_range_agg30m() {
        // > 1 day uses 30m agg
        let just_over_1d = ONE_DAY_MS + 1;
        assert_eq!(select_samples_table(0, just_over_1d), SamplesTable::Agg30m);
        assert_eq!(
            select_samples_table(0, 7 * ONE_DAY_MS),
            SamplesTable::Agg30m
        );
        assert_eq!(
            select_samples_table(0, 30 * ONE_DAY_MS),
            SamplesTable::Agg30m
        );
    }

    #[test]
    fn test_select_samples_table_boundary_values() {
        // Exactly 1 hour -> Raw
        assert_eq!(select_samples_table(0, ONE_HOUR_MS), SamplesTable::Raw);
        // Exactly 1 day -> Agg5m (<=)
        assert_eq!(select_samples_table(0, ONE_DAY_MS), SamplesTable::Agg5m);
    }

    #[test]
    fn test_table_names() {
        assert_eq!(SamplesTable::Raw.table_name(), "samples_v1");
        assert_eq!(SamplesTable::Agg5m.table_name(), "samples_v1_agg_5m");
        assert_eq!(SamplesTable::Agg30m.table_name(), "samples_v1_agg_30m");
    }

    /// Regression: the lookback extension in ClickHouseMetricFetcher must
    /// happen AFTER table selection, not before. If the evaluator extends
    /// start_ms by 5 min before calling select_samples_table, a 1-hour
    /// query (60 min) becomes 65 min and incorrectly selects Agg5m instead
    /// of Raw. This test ensures the boundary is correct when table selection
    /// uses the original (un-extended) range.
    #[test]
    fn test_lookback_must_not_change_table_selection() {
        let lookback_5m: i64 = 5 * 60 * 1000;

        // 1-hour query: must stay Raw even though lookback would add 5 min
        let start = 1_000_000i64;
        let end = start + ONE_HOUR_MS;
        assert_eq!(
            select_samples_table(start, end),
            SamplesTable::Raw,
            "1-hour range must select Raw table"
        );
        // If lookback were applied BEFORE table selection, this would wrongly select Agg5m
        assert_eq!(
            select_samples_table(start - lookback_5m, end),
            SamplesTable::Agg5m,
            "1h+5m range selects Agg5m — proves the bug if lookback is applied before table selection"
        );

        // 1-day query: must stay Agg5m even with lookback
        let end_1d = start + ONE_DAY_MS;
        assert_eq!(
            select_samples_table(start, end_1d),
            SamplesTable::Agg5m,
            "1-day range must select Agg5m"
        );
        // If lookback were applied before table selection, 1d+5m crosses
        // into Agg30m — same class of bug as the 1h → Agg5m case.
        assert_eq!(
            select_samples_table(start - lookback_5m, end_1d),
            SamplesTable::Agg30m,
            "1d+5m crosses into Agg30m — proves the bug if lookback is applied before table selection"
        );
    }

    #[test]
    fn test_select_time_series_table() {
        // Short range -> Base
        let (_, table) = select_time_series_table(0, 4 * ONE_HOUR_MS);
        assert_eq!(table, TimeSeriesTable::Base);

        // Medium range -> Hours6
        let (_, table) = select_time_series_table(0, 12 * ONE_HOUR_MS);
        assert_eq!(table, TimeSeriesTable::Hours6);

        // Long range -> Day1
        let (_, table) = select_time_series_table(0, 48 * ONE_HOUR_MS);
        assert_eq!(table, TimeSeriesTable::Day1);
    }
}
