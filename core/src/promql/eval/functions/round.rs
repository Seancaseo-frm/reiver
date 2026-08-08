// Adapted from GreptimeDB — Apache License 2.0

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Float64Array, PrimitiveArray};
use arrow_schema::DataType;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ColumnarValue;
use datafusion_common::ScalarValue;
use datafusion_expr::{create_udf, ScalarUDF, Volatility};

use super::extract_array;

pub struct Round;

impl Round {
    pub const fn name() -> &'static str {
        "prom_round"
    }

    pub fn scalar_udf() -> ScalarUDF {
        create_udf(
            Self::name(),
            vec![DataType::Float64, DataType::Float64],
            DataType::Float64,
            Volatility::Volatile,
            Arc::new(Self::round) as _,
        )
    }

    fn round(input: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        if input.len() != 2 {
            return Err(DataFusionError::Plan("prom_round expects 2 inputs".into()));
        }

        let value_array = extract_array(&input[0])?;
        let nearest_col = &input[1];

        match nearest_col {
            ColumnarValue::Scalar(nearest_scalar) => {
                let nearest = if let ScalarValue::Float64(Some(val)) = nearest_scalar {
                    *val
                } else {
                    let null_array = Float64Array::new_null(value_array.len());
                    return Ok(ColumnarValue::Array(Arc::new(null_array)));
                };
                let op = |a: f64| {
                    if nearest == 0.0 {
                        a.round()
                    } else {
                        (a / nearest).round() * nearest
                    }
                };
                let result: PrimitiveArray<Float64Type> =
                    value_array.as_primitive::<Float64Type>().unary(op);
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            ColumnarValue::Array(nearest_array) => {
                let value_arr = value_array.as_primitive::<Float64Type>();
                let nearest_arr = nearest_array.as_primitive::<Float64Type>();
                if value_arr.len() != nearest_arr.len() {
                    return Err(DataFusionError::Execution(
                        "round: input arrays must have the same length".into(),
                    ));
                }
                let result: PrimitiveArray<Float64Type> =
                    arrow::compute::binary(value_arr, nearest_arr, |a, nearest| {
                        if nearest == 0.0 {
                            a.round()
                        } else {
                            (a / nearest).round() * nearest
                        }
                    })?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }
    }
}
