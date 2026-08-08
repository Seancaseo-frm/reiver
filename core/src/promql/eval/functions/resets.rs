// Adapted from GreptimeDB — Apache License 2.0

use arrow_array::{Float64Array, TimestampMillisecondArray};

use super::simple_range_udf;

simple_range_udf! {
    pub struct Resets => "prom_resets";
    fn resets(ts: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
        if values.is_empty() {
            None
        } else {
            let ts_vals = ts.values();
            let vals = values.values();
            let mut num_resets = 0;
            // For the current timestamp group, track the max value (counters
            // from unmerged parts — the true value is max of duplicates).
            let mut group_max = vals[0];
            let mut prev_ts = ts_vals[0];
            for i in 1..vals.len() {
                if ts_vals[i] == prev_ts {
                    // Same timestamp — track max within this group
                    if vals[i] > group_max {
                        group_max = vals[i];
                    }
                } else {
                    // Timestamp advanced — compare against previous group's max
                    if vals[i] < group_max {
                        num_resets += 1;
                    }
                    group_max = vals[i];
                    prev_ts = ts_vals[i];
                }
            }
            Some(num_resets as f64)
        }
    }
}
