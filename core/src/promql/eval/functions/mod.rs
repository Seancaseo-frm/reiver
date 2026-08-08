// Adapted from GreptimeDB src/promql/src/functions/ — Apache License 2.0
// PromQL range/instant functions implemented as DataFusion ScalarUDFs.

mod aggr_over_time;
mod changes;
mod deriv;
mod extrapolate_rate;
mod histogram_quantile;
mod idelta;
mod predict_linear;
mod quantile;
mod resets;
mod round;

pub use aggr_over_time::{
    AbsentOverTime, AvgOverTime, CountOverTime, LastOverTime, MadOverTime, MaxOverTime,
    MinOverTime, PresentOverTime, StddevOverTime, StdvarOverTime, SumOverTime,
};
pub use changes::Changes;
pub use deriv::Deriv;
pub use extrapolate_rate::{Delta, Increase, Rate};
pub use histogram_quantile::histogram_quantile_udf;
pub use idelta::IDelta;
pub use predict_linear::PredictLinear;
pub use quantile::{ExactQuantileUdaf, QuantileOverTime};
pub use resets::Resets;
pub use round::Round;

use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, DictionaryArray, Float64Array, TimestampMillisecondArray};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ColumnarValue;

use super::range_array::RangeArray;

pub(crate) fn extract_array(columnar_value: &ColumnarValue) -> Result<ArrayRef, DataFusionError> {
    match columnar_value {
        ColumnarValue::Array(array) => Ok(array.clone()),
        ColumnarValue::Scalar(scalar) => Ok(scalar.to_array_of_size(1)?),
    }
}

pub(crate) fn extract_range_array(
    columnar_value: &ColumnarValue,
) -> Result<RangeArray, DataFusionError> {
    let array = extract_array(columnar_value)?;
    let dict = array
        .as_any()
        .downcast_ref::<DictionaryArray<Int64Type>>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "expected DictionaryArray<Int64>, found {}",
                array.data_type()
            ))
        })?
        .clone();
    RangeArray::try_new(dict).map_err(|e| DataFusionError::External(Box::new(e)))
}

/// Kahan (compensated) summation — reduces floating-point rounding error.
/// Includes the Neumaier improvement for large-magnitude differences.
pub(crate) fn compensated_sum_inc(inc: f64, sum: f64, mut compensation: f64) -> (f64, f64) {
    let new_sum = sum + inc;
    if sum.abs() >= inc.abs() {
        compensation += (sum - new_sum) + inc;
    } else {
        compensation += (inc - new_sum) + sum;
    }
    (new_sum, compensation)
}

/// Least-squares linear regression over (time, value) pairs.
/// Returns (slope, intercept) relative to `intercept_time`.
pub(crate) fn linear_regression(
    times: &TimestampMillisecondArray,
    values: &Float64Array,
    intercept_time: i64,
) -> (Option<f64>, Option<f64>) {
    linear_regression_slices(times.values(), 0, values, 0, values.len(), intercept_time)
}

pub(crate) fn linear_regression_slices(
    times: &[i64],
    time_offset: usize,
    values: &Float64Array,
    value_offset: usize,
    len: usize,
    intercept_time: i64,
) -> (Option<f64>, Option<f64>) {
    let raw_values = values.values();
    let has_nulls = values.null_count() > 0;
    let mut count: f64 = 0.0;
    let mut sum_x: f64 = 0.0;
    let mut sum_y: f64 = 0.0;
    let mut sum_xy: f64 = 0.0;
    let mut sum_x2: f64 = 0.0;
    let mut comp_x: f64 = 0.0;
    let mut comp_y: f64 = 0.0;
    let mut comp_xy: f64 = 0.0;
    let mut comp_x2: f64 = 0.0;

    let mut const_y = true;
    let mut init_y = None;

    for i in 0..len {
        let time_idx = time_offset + i;
        let value_idx = value_offset + i;
        if has_nulls && values.is_null(value_idx) {
            continue;
        }
        let value = raw_values[value_idx];
        let time = times[time_idx] as f64;
        let initial = init_y.get_or_insert(value);
        if const_y && count > 0.0 && value != *initial {
            const_y = false;
        }
        count += 1.0;
        let x = (time - intercept_time as f64) / 1e3f64;
        (sum_x, comp_x) = compensated_sum_inc(x, sum_x, comp_x);
        (sum_y, comp_y) = compensated_sum_inc(value, sum_y, comp_y);
        (sum_xy, comp_xy) = compensated_sum_inc(x * value, sum_xy, comp_xy);
        (sum_x2, comp_x2) = compensated_sum_inc(x * x, sum_x2, comp_x2);
    }

    if count < 2.0 {
        return (None, None);
    }
    if const_y {
        let init_y = init_y.unwrap();
        if !init_y.is_finite() {
            return (None, None);
        }
        return (Some(0.0), Some(init_y));
    }

    sum_x += comp_x;
    sum_y += comp_y;
    sum_xy += comp_xy;
    sum_x2 += comp_x2;

    let cov_xy = sum_xy - sum_x * sum_y / count;
    let var_x = sum_x2 - sum_x * sum_x / count;
    let slope = cov_xy / var_x;
    let intercept = sum_y / count - slope * sum_x / count;
    (Some(slope), Some(intercept))
}

/// Macro to generate a simple range function UDF struct.
/// The function signature is `fn(timestamps, values) -> Option<f64>` and operates
/// on each range window independently.
macro_rules! simple_range_udf {
    (
        $(#[$meta:meta])*
        $vis:vis struct $Name:ident => $display_name:expr;
        fn $fn_name:ident($ts:ident: &TimestampMillisecondArray, $vals:ident: &Float64Array) -> Option<f64>
        $body:block
    ) => {
        $(#[$meta])*
        $vis struct $Name;

        impl $Name {
            pub const fn name() -> &'static str {
                $display_name
            }

            pub fn scalar_udf() -> datafusion::logical_expr::ScalarUDF {
                datafusion_expr::create_udf(
                    Self::name(),
                    vec![
                        $crate::promql::eval::range_array::RangeArray::convert_data_type(
                            arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                        ),
                        $crate::promql::eval::range_array::RangeArray::convert_data_type(
                            arrow_schema::DataType::Float64,
                        ),
                    ],
                    arrow_schema::DataType::Float64,
                    datafusion::logical_expr::Volatility::Volatile,
                    std::sync::Arc::new(Self::calc) as _,
                )
            }

            fn calc(input: &[datafusion::physical_plan::ColumnarValue]) -> Result<datafusion::physical_plan::ColumnarValue, datafusion::error::DataFusionError> {
                use super::{extract_range_array};
                use arrow_array::{Float64Array, TimestampMillisecondArray, builder::Float64Builder};

                assert_eq!(input.len(), 2, "{} expects 2 inputs", Self::name());
                let ts_range = extract_range_array(&input[0])?;
                let value_range = extract_range_array(&input[1])?;

                let mut builder = Float64Builder::with_capacity(ts_range.len());
                for i in 0..ts_range.len() {
                    let ts_arr = ts_range.get(i);
                    let val_arr = value_range.get(i);
                    match (ts_arr, val_arr) {
                        (Some(ts), Some(vals)) => {
                            let ts_ref = ts.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
                            let vals_ref = vals.as_any().downcast_ref::<Float64Array>().unwrap();
                            match $fn_name(ts_ref, vals_ref) {
                                Some(v) => builder.append_value(v),
                                None => builder.append_null(),
                            }
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok(datafusion::physical_plan::ColumnarValue::Array(std::sync::Arc::new(builder.finish())))
            }
        }

        fn $fn_name($ts: &TimestampMillisecondArray, $vals: &Float64Array) -> Option<f64>
        $body
    };
}

pub(crate) use simple_range_udf;
