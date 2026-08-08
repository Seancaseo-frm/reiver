// Adapted from GreptimeDB — Apache License 2.0

use arrow_array::{Float64Array, TimestampMillisecondArray};

use super::compensated_sum_inc;
use super::simple_range_udf;

simple_range_udf! {
    pub struct AvgOverTime => "prom_avg_over_time";
    fn avg_over_time(_ts: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
        arrow::compute::sum(values).map(|s| s / values.len() as f64)
    }
}

simple_range_udf! {
    pub struct MinOverTime => "prom_min_over_time";
    fn min_over_time(_ts: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
        arrow::compute::min(values)
    }
}

simple_range_udf! {
    pub struct MaxOverTime => "prom_max_over_time";
    fn max_over_time(_ts: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
        arrow::compute::max(values)
    }
}

simple_range_udf! {
    pub struct SumOverTime => "prom_sum_over_time";
    fn sum_over_time(_ts: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
        arrow::compute::sum(values)
    }
}

simple_range_udf! {
    pub struct CountOverTime => "prom_count_over_time";
    fn count_over_time(_ts: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
        if values.is_empty() { None } else { Some(values.len() as f64) }
    }
}

simple_range_udf! {
    pub struct LastOverTime => "prom_last_over_time";
    fn last_over_time(_ts: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
        values.values().last().copied()
    }
}

simple_range_udf! {
    pub struct AbsentOverTime => "prom_absent_over_time";
    fn absent_over_time(_ts: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
        if values.is_empty() { Some(1.0) } else { None }
    }
}

simple_range_udf! {
    pub struct PresentOverTime => "prom_present_over_time";
    fn present_over_time(_ts: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
        if values.is_empty() { None } else { Some(1.0) }
    }
}

simple_range_udf! {
    pub struct StdvarOverTime => "prom_stdvar_over_time";
    fn stdvar_over_time(_ts: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
        if values.is_empty() {
            None
        } else {
            let mut count = 0;
            let mut mean: f64 = 0.0;
            let mut result: f64 = 0.0;
            for value in values {
                let value = value.unwrap();
                let new_count = count + 1;
                let delta1 = value - mean;
                let new_mean = delta1 / new_count as f64 + mean;
                let delta2 = value - new_mean;
                let new_result = result + delta1 * delta2;
                count += 1;
                mean = new_mean;
                result = new_result;
            }
            Some(result / count as f64)
        }
    }
}

simple_range_udf! {
    pub struct StddevOverTime => "prom_stddev_over_time";
    fn stddev_over_time(_ts: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
        if values.is_empty() {
            None
        } else {
            let mut count = 0.0;
            let mut mean = 0.0;
            let mut comp_mean = 0.0;
            let mut deviations_sum_sq = 0.0;
            let mut comp_deviations_sum_sq = 0.0;
            for v in values {
                count += 1.0;
                let current_value = v.unwrap();
                let delta = current_value - (mean + comp_mean);
                let (new_mean, new_comp_mean) = compensated_sum_inc(delta / count, mean, comp_mean);
                mean = new_mean;
                comp_mean = new_comp_mean;
                let (new_dev, new_comp_dev) = compensated_sum_inc(
                    delta * (current_value - (mean + comp_mean)),
                    deviations_sum_sq,
                    comp_deviations_sum_sq,
                );
                deviations_sum_sq = new_dev;
                comp_deviations_sum_sq = new_comp_dev;
            }
            Some(((deviations_sum_sq + comp_deviations_sum_sq) / count).sqrt())
        }
    }
}

simple_range_udf! {
    pub struct MadOverTime => "prom_mad_over_time";
    fn mad_over_time(_ts: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
        if values.is_empty() {
            return None;
        }
        let mut vals: Vec<f64> = values.values().to_vec();
        vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median = if vals.len() % 2 == 0 {
            (vals[vals.len() / 2 - 1] + vals[vals.len() / 2]) / 2.0
        } else {
            vals[vals.len() / 2]
        };
        let mut deviations: Vec<f64> = vals.iter().map(|v| (v - median).abs()).collect();
        deviations.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let mad = if deviations.len() % 2 == 0 {
            (deviations[deviations.len() / 2 - 1] + deviations[deviations.len() / 2]) / 2.0
        } else {
            deviations[deviations.len() / 2]
        };
        Some(mad)
    }
}
