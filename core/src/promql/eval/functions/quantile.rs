// Adapted from GreptimeDB — Apache License 2.0

use std::fmt::Debug;
use std::sync::Arc;

use arrow_array::builder::Float64Builder;
use arrow_array::Float64Array;
use arrow_schema::{DataType, Field, TimeUnit};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ColumnarValue;
use datafusion_common::ScalarValue;
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::{
    create_udf, Accumulator, AggregateUDF, AggregateUDFImpl, ScalarUDF, Signature, Volatility,
};

use arrow_array::Array;

use super::extract_range_array;
use crate::promql::eval::range_array::RangeArray;

pub struct QuantileOverTime;

impl QuantileOverTime {
    pub const fn name() -> &'static str {
        "prom_quantile_over_time"
    }

    pub fn scalar_udf() -> ScalarUDF {
        create_udf(
            Self::name(),
            vec![
                RangeArray::convert_data_type(DataType::Timestamp(TimeUnit::Millisecond, None)),
                RangeArray::convert_data_type(DataType::Float64),
                DataType::Float64,
            ],
            DataType::Float64,
            Volatility::Volatile,
            Arc::new(Self::quantile_over_time) as _,
        )
    }

    fn quantile_over_time(input: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        if input.len() != 3 {
            return Err(DataFusionError::Plan(
                "prom_quantile_over_time expects 3 inputs".into(),
            ));
        }

        let ts_range = extract_range_array(&input[0])?;
        let value_range = extract_range_array(&input[1])?;
        let quantile_col = &input[2];

        if ts_range.len() != value_range.len() {
            return Err(DataFusionError::Execution(format!(
                "{}: input arrays must have the same length",
                Self::name()
            )));
        }

        let all_values = value_range
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values();

        let mut result_builder = Float64Builder::with_capacity(ts_range.len());
        let mut scratch = Vec::new();

        match quantile_col {
            ColumnarValue::Scalar(quantile_scalar) => {
                let quantile = if let ScalarValue::Float64(Some(q)) = quantile_scalar {
                    *q
                } else {
                    f64::NAN
                };
                for index in 0..ts_range.len() {
                    let (value_offset, value_len) = value_range.get_offset_length(index).unwrap();
                    match quantile_with_scratch(
                        &all_values[value_offset..value_offset + value_len],
                        quantile,
                        &mut scratch,
                    ) {
                        Some(value) => result_builder.append_value(value),
                        None => result_builder.append_null(),
                    }
                }
            }
            ColumnarValue::Array(quantile_array) => {
                let quantile_array = quantile_array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "{}: expect Float64 as quantile type",
                            Self::name()
                        ))
                    })?;
                for index in 0..ts_range.len() {
                    let (value_offset, value_len) = value_range.get_offset_length(index).unwrap();
                    let quantile = if quantile_array.is_null(index) {
                        f64::NAN
                    } else {
                        quantile_array.value(index)
                    };
                    match quantile_with_scratch(
                        &all_values[value_offset..value_offset + value_len],
                        quantile,
                        &mut scratch,
                    ) {
                        Some(value) => result_builder.append_value(value),
                        None => result_builder.append_null(),
                    }
                }
            }
        }

        Ok(ColumnarValue::Array(Arc::new(result_builder.finish())))
    }
}

pub(crate) fn quantile_impl(values: &[f64], quantile: f64) -> Option<f64> {
    let mut scratch = Vec::new();
    quantile_with_scratch(values, quantile, &mut scratch)
}

fn quantile_with_scratch(values: &[f64], quantile: f64, scratch: &mut Vec<f64>) -> Option<f64> {
    if quantile.is_nan() || values.is_empty() {
        return Some(f64::NAN);
    }
    if quantile < 0.0 {
        return Some(f64::NEG_INFINITY);
    }
    if quantile > 1.0 {
        return Some(f64::INFINITY);
    }

    scratch.clear();
    scratch.extend_from_slice(values);
    scratch.sort_unstable_by(f64::total_cmp);

    let length = scratch.len();
    let rank = quantile * (length - 1) as f64;
    let lower_index = rank.floor() as usize;
    let upper_index = (length - 1).min(lower_index + 1);
    let weight = rank - rank.floor();

    Some(scratch[lower_index] * (1.0 - weight) + scratch[upper_index] * weight)
}

// --- Exact quantile UDAF for the `quantile()` aggregation operator ---

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExactQuantileUdaf {
    signature: Signature,
    quantile: u64, // f64 bits stored as u64 for Eq/Hash
}

impl ExactQuantileUdaf {
    pub fn new(q: f64) -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Float64, DataType::Float64], Volatility::Immutable),
            quantile: q.to_bits(),
        }
    }

    pub fn udaf(q: f64) -> AggregateUDF {
        AggregateUDF::from(Self::new(q))
    }

    fn q(&self) -> f64 {
        f64::from_bits(self.quantile)
    }
}

impl AggregateUDFImpl for ExactQuantileUdaf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "prom_exact_quantile"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> datafusion_common::Result<Box<dyn Accumulator>> {
        Ok(Box::new(ExactQuantileAccumulator {
            values: Vec::new(),
            quantile: self.q(),
        }))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> datafusion_common::Result<Vec<Arc<Field>>> {
        Ok(vec![Arc::new(Field::new(
            "values",
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            true,
        ))])
    }
}

#[derive(Debug)]
struct ExactQuantileAccumulator {
    values: Vec<f64>,
    quantile: f64,
}

impl Accumulator for ExactQuantileAccumulator {
    fn update_batch(&mut self, values: &[arrow_array::ArrayRef]) -> datafusion_common::Result<()> {
        let arr = values[0]
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Internal("expected Float64Array".into()))?;
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                self.values.push(arr.value(i));
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        match quantile_impl(&self.values, self.quantile) {
            Some(v) => Ok(ScalarValue::Float64(Some(v))),
            None => Ok(ScalarValue::Float64(None)),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.values.capacity() * std::mem::size_of::<f64>()
    }

    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        let list_values: Vec<ScalarValue> = self
            .values
            .iter()
            .map(|v| ScalarValue::Float64(Some(*v)))
            .collect();
        Ok(vec![ScalarValue::List(ScalarValue::new_list_from_iter(
            list_values.into_iter(),
            &DataType::Float64,
            true,
        ))])
    }

    fn merge_batch(&mut self, states: &[arrow_array::ArrayRef]) -> datafusion_common::Result<()> {
        use arrow_array::ListArray;
        let list_arr = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Internal("expected ListArray for state merge".into()))?;

        for i in 0..list_arr.len() {
            if list_arr.is_null(i) {
                continue;
            }
            let inner = list_arr.value(i);
            let float_arr = inner
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("expected Float64Array inside list".into())
                })?;
            for j in 0..float_arr.len() {
                if !float_arr.is_null(j) {
                    self.values.push(float_arr.value(j));
                }
            }
        }
        Ok(())
    }
}
