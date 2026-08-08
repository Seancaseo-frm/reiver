// Adapted from GreptimeDB — Apache License 2.0
// Implements rate(), increase(), and delta() with correct Prometheus extrapolation semantics.

use std::sync::Arc;

use arrow_array::builder::Float64Builder;
use arrow_array::types::Int64Type;
use arrow_array::{Array, DictionaryArray, Float64Array, Int64Array, TimestampMillisecondArray};
use arrow_schema::{DataType, TimeUnit};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ColumnarValue;
use datafusion_expr::{create_udf, ScalarUDF, Volatility};

use super::extract_array;
use crate::promql::eval::range_array::{unpack, RangeArray};

pub type Delta = ExtrapolatedRate<false, false>;
pub type Rate = ExtrapolatedRate<true, true>;
pub type Increase = ExtrapolatedRate<true, false>;

#[derive(Debug)]
pub struct ExtrapolatedRate<const IS_COUNTER: bool, const IS_RATE: bool> {
    range_length: i64,
}

impl<const IS_COUNTER: bool, const IS_RATE: bool> ExtrapolatedRate<IS_COUNTER, IS_RATE> {
    fn new(range_length: i64) -> Self {
        Self { range_length }
    }

    fn func_name() -> &'static str {
        match (IS_COUNTER, IS_RATE) {
            (true, true) => "prom_rate",
            (true, false) => "prom_increase",
            (false, false) => "prom_delta",
            (false, true) => unreachable!("gauge rate is not supported"),
        }
    }

    fn scalar_udf_with_name(name: &str) -> ScalarUDF {
        let input_types = vec![
            RangeArray::convert_data_type(DataType::Timestamp(TimeUnit::Millisecond, None)),
            RangeArray::convert_data_type(DataType::Float64),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Int64,
        ];
        create_udf(
            name,
            input_types,
            DataType::Float64,
            Volatility::Volatile,
            Arc::new(move |input: &_| Self::create_function(input)?.calc(input)) as _,
        )
    }

    fn create_function(inputs: &[ColumnarValue]) -> Result<Self, DataFusionError> {
        if inputs.len() != 4 {
            return Err(DataFusionError::Plan(format!(
                "{} expects 4 inputs",
                Self::func_name()
            )));
        }
        let range_length_array = extract_array(&inputs[3])?;
        let range_length_array = range_length_array
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{}: expect Int64 as range length, found {}",
                    Self::func_name(),
                    range_length_array.data_type()
                ))
            })?;
        if range_length_array.is_empty() || range_length_array.is_null(0) {
            return Err(DataFusionError::Execution(format!(
                "{}: range length must contain a non-null Int64",
                Self::func_name()
            )));
        }
        Ok(Self::new(range_length_array.value(0)))
    }

    fn calc(&self, input: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        if input.len() != 4 {
            return Err(DataFusionError::Plan(format!(
                "{} expects 4 inputs",
                Self::func_name()
            )));
        }

        let ts_dict = extract_range_dict(
            &input[0],
            Self::func_name(),
            "timestamp range vector",
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )?;
        let value_dict = extract_range_dict(
            &input[1],
            Self::func_name(),
            "value range vector",
            &DataType::Float64,
        )?;
        let eval_ts_array = extract_eval_timestamps(&input[2], Self::func_name())?;

        let keys = ts_dict.keys().values();
        let num_windows = keys.len();
        if value_dict.keys().len() != num_windows {
            return Err(DataFusionError::Execution(format!(
                "{}: timestamp and value ranges should have the same number of windows",
                Self::func_name()
            )));
        }
        if eval_ts_array.len() != num_windows {
            return Err(DataFusionError::Execution(format!(
                "{}: evaluation timestamp vector should have the same number of rows as range inputs",
                Self::func_name()
            )));
        }

        let all_timestamps = ts_dict
            .values()
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("validated")
            .values();
        let all_values = value_dict
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("validated")
            .values();
        let eval_ts = eval_ts_array.values();

        let mut result_builder = Float64Builder::with_capacity(num_windows);
        let range_length = self.range_length;
        let range_length_secs = range_length as f64 / 1000.0;

        let mut counter_correction = 0.0;

        for index in 0..num_windows {
            let (raw_offset, raw_length) = unpack(keys[index]);
            let offset = raw_offset as usize;
            let length = raw_length as usize;

            if length < 2 {
                result_builder.append_null();
                continue;
            }

            let end = offset + length;

            // Deduplicate samples by timestamp within this window.
            // For counters keep max(value) per timestamp; for gauges keep last.
            let (deduped_ts, deduped_vals) = dedupe_window(
                &all_timestamps[offset..end],
                &all_values[offset..end],
                IS_COUNTER,
            );

            let dlen = deduped_ts.len();
            if dlen < 2 {
                result_builder.append_null();
                continue;
            }

            let first_value = deduped_vals[0];
            let last_value = deduped_vals[dlen - 1];

            // Counter reset detection on deduped (strictly increasing timestamps) data
            let result_value = if IS_COUNTER {
                // Always do full rescan on deduped data (incremental path
                // doesn't apply after dedup changes window composition)
                counter_correction = 0.0;
                for pair in deduped_vals.windows(2) {
                    if pair[1] < pair[0] {
                        counter_correction += pair[0];
                    }
                }
                last_value - first_value + counter_correction
            } else {
                last_value - first_value
            };

            let first_ts = deduped_ts[0];
            let last_ts = deduped_ts[dlen - 1];
            let range_end = eval_ts[index];
            let range_start = range_end - range_length;
            let sampled_interval_ms = (last_ts - first_ts) as f64;

            if sampled_interval_ms == 0.0 {
                result_builder.append_null();
                continue;
            }

            let average_interval_ms = sampled_interval_ms / (dlen - 1) as f64;
            let mut duration_to_start_ms = (first_ts - range_start) as f64;
            let duration_to_end_ms = (range_end - last_ts) as f64;

            if IS_COUNTER && result_value > 0.0 && first_value >= 0.0 {
                let duration_to_zero = sampled_interval_ms * (first_value / result_value);
                if duration_to_zero < duration_to_start_ms {
                    duration_to_start_ms = duration_to_zero;
                }
            }

            let extrapolation_threshold = average_interval_ms * 1.1;
            let mut extrapolated_interval_ms = sampled_interval_ms;

            if duration_to_start_ms < extrapolation_threshold {
                extrapolated_interval_ms += duration_to_start_ms;
            } else {
                extrapolated_interval_ms += average_interval_ms / 2.0;
            }
            if duration_to_end_ms < extrapolation_threshold {
                extrapolated_interval_ms += duration_to_end_ms;
            } else {
                extrapolated_interval_ms += average_interval_ms / 2.0;
            }

            let mut factor = extrapolated_interval_ms / sampled_interval_ms;
            if IS_RATE {
                factor /= range_length_secs;
            }
            result_builder.append_value(result_value * factor);
        }

        Ok(ColumnarValue::Array(Arc::new(result_builder.finish())))
    }
}

fn extract_range_dict(
    columnar_value: &ColumnarValue,
    func_name: &str,
    arg_name: &str,
    expected_value_type: &DataType,
) -> Result<DictionaryArray<Int64Type>, DataFusionError> {
    let array = extract_array(columnar_value)?;
    let dict = array
        .as_any()
        .downcast_ref::<DictionaryArray<Int64Type>>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "{func_name}: expect {arg_name} as DictionaryArray, found {}",
                array.data_type()
            ))
        })?
        .clone();
    if &dict.value_type() != expected_value_type {
        return Err(DataFusionError::Execution(format!(
            "{func_name}: expect {arg_name} values of type {expected_value_type}, found {}",
            dict.value_type()
        )));
    }
    RangeArray::try_new(dict.clone()).map_err(|e| DataFusionError::External(Box::new(e)))?;
    Ok(dict)
}

fn extract_eval_timestamps(
    columnar_value: &ColumnarValue,
    func_name: &str,
) -> Result<TimestampMillisecondArray, DataFusionError> {
    let array = extract_array(columnar_value)?;
    let timestamps = array
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "{func_name}: expect evaluation timestamps as Timestamp(Millisecond), found {}",
                array.data_type()
            ))
        })?;
    Ok(timestamps.clone())
}

impl ExtrapolatedRate<false, false> {
    pub const fn name() -> &'static str {
        "prom_delta"
    }
    pub fn scalar_udf() -> ScalarUDF {
        Self::scalar_udf_with_name(Self::name())
    }
}

impl ExtrapolatedRate<true, true> {
    pub const fn name() -> &'static str {
        "prom_rate"
    }
    pub fn scalar_udf() -> ScalarUDF {
        Self::scalar_udf_with_name(Self::name())
    }
}

impl ExtrapolatedRate<true, false> {
    pub const fn name() -> &'static str {
        "prom_increase"
    }
    pub fn scalar_udf() -> ScalarUDF {
        Self::scalar_udf_with_name(Self::name())
    }
}

/// Returns true if any adjacent timestamps are equal (duplicates exist).
#[inline]
fn has_duplicate_timestamps(timestamps: &[i64]) -> bool {
    timestamps.windows(2).any(|w| w[0] == w[1])
}

/// Deduplicate a window slice by timestamp.
/// For counters: keep max(value) per unique timestamp.
/// For gauges: keep the last value per unique timestamp.
fn dedupe_window(timestamps: &[i64], values: &[f64], is_counter: bool) -> (Vec<i64>, Vec<f64>) {
    if timestamps.is_empty() {
        return (Vec::new(), Vec::new());
    }

    // Fast path: no duplicates — just clone the slices
    if !has_duplicate_timestamps(timestamps) {
        return (timestamps.to_vec(), values.to_vec());
    }

    let mut deduped_ts: Vec<i64> = Vec::with_capacity(timestamps.len());
    let mut deduped_vals: Vec<f64> = Vec::with_capacity(values.len());

    deduped_ts.push(timestamps[0]);
    deduped_vals.push(values[0]);

    for i in 1..timestamps.len() {
        if timestamps[i] == *deduped_ts.last().unwrap() {
            let last_idx = deduped_vals.len() - 1;
            if is_counter {
                if values[i] > deduped_vals[last_idx] {
                    deduped_vals[last_idx] = values[i];
                }
            } else {
                deduped_vals[last_idx] = values[i];
            }
        } else {
            deduped_ts.push(timestamps[i]);
            deduped_vals.push(values[i]);
        }
    }

    (deduped_ts, deduped_vals)
}
