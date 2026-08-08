// Adapted from GreptimeDB — Apache License 2.0

use std::sync::Arc;

use arrow_array::builder::Float64Builder;
use arrow_array::{Float64Array, Int64Array};
use arrow_schema::{DataType, TimeUnit};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ColumnarValue;
use datafusion_common::ScalarValue;
use datafusion_expr::{create_udf, ScalarUDF, Volatility};

use super::{extract_range_array, linear_regression_slices};
use crate::promql::eval::range_array::RangeArray;

pub struct PredictLinear;

impl PredictLinear {
    pub const fn name() -> &'static str {
        "prom_predict_linear"
    }

    pub fn scalar_udf() -> ScalarUDF {
        create_udf(
            Self::name(),
            vec![
                RangeArray::convert_data_type(DataType::Timestamp(TimeUnit::Millisecond, None)),
                RangeArray::convert_data_type(DataType::Float64),
                DataType::Int64,
            ],
            DataType::Float64,
            Volatility::Volatile,
            Arc::new(Self::predict_linear) as _,
        )
    }

    fn predict_linear(input: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        if input.len() != 3 {
            return Err(DataFusionError::Plan(
                "prom_predict_linear expects 3 inputs".into(),
            ));
        }

        let ts_range = extract_range_array(&input[0])?;
        let value_range = extract_range_array(&input[1])?;
        let t_col = &input[2];

        if ts_range.len() != value_range.len() {
            return Err(DataFusionError::Execution(format!(
                "{}: input arrays must have the same length",
                Self::name()
            )));
        }

        let t_iter: Box<dyn Iterator<Item = Option<i64>>> = match t_col {
            ColumnarValue::Scalar(t_scalar) => {
                let t = if let ScalarValue::Int64(Some(t_val)) = t_scalar {
                    *t_val
                } else {
                    let null_array = Float64Array::new_null(ts_range.len());
                    return Ok(ColumnarValue::Array(Arc::new(null_array)));
                };
                Box::new((0..ts_range.len()).map(move |_| Some(t)))
            }
            ColumnarValue::Array(t_array) => {
                let t_array = t_array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "{}: expect Int64 as t array type",
                            Self::name()
                        ))
                    })?;
                Box::new(t_array.iter())
            }
        };

        let all_timestamps = ts_range
            .values()
            .as_any()
            .downcast_ref::<arrow_array::TimestampMillisecondArray>()
            .unwrap()
            .values();
        let all_values = value_range
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        let mut result_builder = Float64Builder::with_capacity(ts_range.len());
        for (index, t) in t_iter.enumerate() {
            let t = match t {
                Some(v) => v,
                None => {
                    result_builder.append_null();
                    continue;
                }
            };
            let (ts_offset, ts_len) = ts_range.get_offset_length(index).unwrap();
            let (value_offset, value_len) = value_range.get_offset_length(index).unwrap();
            if ts_len != value_len || ts_len < 2 {
                result_builder.append_null();
                continue;
            }
            let evaluate_ts = all_timestamps[ts_offset + ts_len - 1];
            let (slope, intercept) = linear_regression_slices(
                all_timestamps,
                ts_offset,
                all_values,
                value_offset,
                value_len,
                evaluate_ts,
            );
            match (slope, intercept) {
                (Some(s), Some(i)) => result_builder.append_value(s * t as f64 + i),
                _ => result_builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(result_builder.finish())))
    }
}
