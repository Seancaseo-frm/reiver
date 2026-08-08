// Adapted from GreptimeDB — Apache License 2.0

use arrow_array::{Float64Array, TimestampMillisecondArray};

use super::{linear_regression, simple_range_udf};

simple_range_udf! {
    pub struct Deriv => "prom_deriv";
    fn deriv(times: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
        if values.len() < 2 {
            None
        } else {
            let intercept_time = times.value(0);
            let (slope, _) = linear_regression(times, values, intercept_time);
            slope
        }
    }
}
