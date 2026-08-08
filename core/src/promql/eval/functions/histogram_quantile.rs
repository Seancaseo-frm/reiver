// histogram_quantile UDF — computes quantile from Prometheus histogram buckets.
// Receives arrays of le (upper bound) strings and bucket counts, interpolates.

use arrow_array::{
    builder::Float64Builder, Array, Float64Array, LargeListArray, LargeStringArray, ListArray,
    StringArray,
};
use arrow_schema::DataType;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

use super::extract_array;

pub fn histogram_quantile_udf() -> datafusion::logical_expr::ScalarUDF {
    datafusion_expr::create_udf(
        "prom_histogram_quantile",
        vec![
            DataType::Float64,
            DataType::List(Arc::new(arrow_schema::Field::new_list_field(
                DataType::Utf8,
                true,
            ))),
            DataType::List(Arc::new(arrow_schema::Field::new_list_field(
                DataType::Float64,
                true,
            ))),
        ],
        DataType::Float64,
        datafusion::logical_expr::Volatility::Volatile,
        Arc::new(calc) as _,
    )
}

/// Extract le strings from either StringArray or LargeStringArray
fn extract_le_strings(arr: &dyn Array) -> Option<Vec<String>> {
    if let Some(sa) = arr.as_any().downcast_ref::<StringArray>() {
        return Some((0..sa.len()).map(|i| sa.value(i).to_string()).collect());
    }
    if let Some(sa) = arr.as_any().downcast_ref::<LargeStringArray>() {
        return Some((0..sa.len()).map(|i| sa.value(i).to_string()).collect());
    }
    None
}

/// Extract a list of inner arrays from ListArray or LargeListArray
enum ListRef<'a> {
    Regular(&'a ListArray),
    Large(&'a LargeListArray),
}
impl ListRef<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Regular(l) => l.len(),
            Self::Large(l) => l.len(),
        }
    }
    fn value(&self, i: usize) -> Arc<dyn Array> {
        match self {
            Self::Regular(l) => l.value(i),
            Self::Large(l) => l.value(i),
        }
    }
}

fn calc(input: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    assert_eq!(input.len(), 3);

    let le_arr = extract_array(&input[1])?;
    let counts_arr = extract_array(&input[2])?;

    let le_lists: ListRef = if let Some(l) = le_arr.as_any().downcast_ref::<ListArray>() {
        ListRef::Regular(l)
    } else if let Some(l) = le_arr.as_any().downcast_ref::<LargeListArray>() {
        ListRef::Large(l)
    } else {
        return Err(DataFusionError::Execution(format!(
            "histogram_quantile: le must be List, found {:?}",
            le_arr.data_type()
        )));
    };

    let count_lists: ListRef = if let Some(l) = counts_arr.as_any().downcast_ref::<ListArray>() {
        ListRef::Regular(l)
    } else if let Some(l) = counts_arr.as_any().downcast_ref::<LargeListArray>() {
        ListRef::Large(l)
    } else {
        return Err(DataFusionError::Execution(format!(
            "histogram_quantile: counts must be List, found {:?}",
            counts_arr.data_type()
        )));
    };

    let num_rows = le_lists.len();

    let q_arr = match &input[0] {
        ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
        ColumnarValue::Array(a) => a.clone(),
    };
    let q_values = q_arr
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| {
            DataFusionError::Execution("histogram_quantile: q must be Float64".into())
        })?;

    let mut builder = Float64Builder::with_capacity(num_rows);

    for i in 0..num_rows {
        let q = q_values.value(i);
        let le_list = le_lists.value(i);
        let count_list = count_lists.value(i);

        let le_strs = extract_le_strings(le_list.as_ref());
        let count_floats = count_list.as_any().downcast_ref::<Float64Array>();

        match (&le_strs, count_floats) {
            (Some(les), Some(counts)) if les.len() == counts.len() && !les.is_empty() => {
                let result = compute_histogram_quantile_from_vecs(q, les, counts);
                match result {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            _ => {
                builder.append_null();
            }
        }
    }

    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

fn compute_histogram_quantile_from_vecs(
    q: f64,
    les: &[String],
    counts: &Float64Array,
) -> Option<f64> {
    if q.is_nan() || q < 0.0 || q > 1.0 {
        return Some(f64::NAN);
    }

    let mut buckets: Vec<(f64, f64)> = Vec::with_capacity(les.len());
    for i in 0..les.len() {
        let le_str = &les[i];
        let le_val: f64 = match le_str.as_str() {
            "+Inf" => f64::INFINITY,
            s => s.parse().ok()?,
        };
        let count = counts.value(i);
        buckets.push((le_val, count));
    }
    buckets.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

    if buckets.is_empty() {
        return None;
    }

    // Ensure +Inf bucket exists
    let total = match buckets.last() {
        Some((le, count)) if le.is_infinite() => *count,
        _ => return None,
    };

    if total == 0.0 {
        return Some(f64::NAN);
    }

    let rank = q * total;

    // Find the bucket where cumulative count crosses the rank
    let mut lower_bound = 0.0_f64;
    let mut lower_count = 0.0_f64;

    for &(upper_bound, upper_count) in &buckets {
        if upper_count >= rank {
            // Linear interpolation within this bucket
            if upper_bound == lower_bound {
                return Some(lower_bound);
            }
            if upper_bound.is_infinite() {
                return Some(lower_bound);
            }
            let fraction = (rank - lower_count) / (upper_count - lower_count);
            return Some(lower_bound + fraction * (upper_bound - lower_bound));
        }
        lower_bound = upper_bound;
        lower_count = upper_count;
    }

    Some(buckets.last()?.0)
}
