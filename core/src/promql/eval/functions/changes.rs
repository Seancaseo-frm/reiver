// Adapted from GreptimeDB — Apache License 2.0

use arrow_array::{Float64Array, TimestampMillisecondArray};

use super::simple_range_udf;

simple_range_udf! {
    pub struct Changes => "prom_changes";
    fn changes(_ts: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
        if values.is_empty() {
            None
        } else {
            let (first, rest) = values.values().split_first().unwrap();
            let mut num_changes = 0;
            let mut prev = first;
            for cur in rest {
                if cur != prev && !(cur.is_nan() && prev.is_nan()) {
                    num_changes += 1;
                }
                prev = cur;
            }
            Some(num_changes as f64)
        }
    }
}
