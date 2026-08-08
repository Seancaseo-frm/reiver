// Adapted from GreptimeDB — Apache License 2.0
// Implements idelta() and irate().

use std::sync::Arc;

use arrow_array::builder::Float64Builder;
use arrow_array::{Float64Array, TimestampMillisecondArray};
use arrow_schema::{DataType, TimeUnit};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ColumnarValue;
use datafusion_expr::{create_udf, ScalarUDF, Volatility};

use super::extract_range_array;
use crate::promql::eval::range_array::RangeArray;

#[derive(Debug)]
pub struct IDelta<const IS_RATE: bool>;

impl<const IS_RATE: bool> IDelta<IS_RATE> {
    pub const fn name() -> &'static str {
        if IS_RATE {
            "prom_irate"
        } else {
            "prom_idelta"
        }
    }

    pub fn scalar_udf() -> ScalarUDF {
        create_udf(
            Self::name(),
            vec![
                RangeArray::convert_data_type(DataType::Timestamp(TimeUnit::Millisecond, None)),
                RangeArray::convert_data_type(DataType::Float64),
            ],
            DataType::Float64,
            Volatility::Volatile,
            Arc::new(Self::calc) as _,
        )
    }

    fn calc(input: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        assert_eq!(input.len(), 2, "{} expects 2 inputs", Self::name());
        let ts_range = extract_range_array(&input[0])?;
        let value_range = extract_range_array(&input[1])?;

        if ts_range.len() != value_range.len() {
            return Err(DataFusionError::Execution(format!(
                "{}: input arrays must have the same length",
                Self::name()
            )));
        }

        let ts_values = ts_range
            .values()
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .values();
        let value_values = value_range
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values();

        let mut result_builder = Float64Builder::with_capacity(ts_range.len());

        for index in 0..ts_range.len() {
            let (ts_offset, len) = match ts_range.get_offset_length(index) {
                Some(v) => v,
                None => {
                    result_builder.append_null();
                    continue;
                }
            };
            let (value_offset, value_len) = match value_range.get_offset_length(index) {
                Some(v) => v,
                None => {
                    result_builder.append_null();
                    continue;
                }
            };

            if len != value_len || len < 2 {
                result_builder.append_null();
                continue;
            }

            // Find the last two samples with distinct timestamps
            let last_offset = ts_offset + len - 1;
            let mut prev_offset = last_offset - 1;
            while prev_offset > ts_offset && ts_values[prev_offset] == ts_values[last_offset] {
                prev_offset -= 1;
            }

            let sampled_interval =
                (ts_values[last_offset] - ts_values[prev_offset]) as f64 / 1000.0;

            if sampled_interval == 0.0 {
                result_builder.append_null();
                continue;
            }

            let last_value_offset = value_offset + (last_offset - ts_offset);
            let prev_value_offset = value_offset + (prev_offset - ts_offset);
            let last_value = value_values[last_value_offset];
            let prev_value = value_values[prev_value_offset];

            if !IS_RATE {
                result_builder.append_value(last_value - prev_value);
                continue;
            }

            let result_value = if last_value < prev_value {
                last_value // counter reset
            } else {
                last_value - prev_value
            };
            result_builder.append_value(result_value / sampled_interval);
        }

        Ok(ColumnarValue::Array(Arc::new(result_builder.finish())))
    }
}
