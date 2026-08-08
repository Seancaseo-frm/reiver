// Adapted from GreptimeDB src/promql/src/range_array.rs
// Copyright 2023 Greptime Team — Apache License 2.0
//
// An extended "array" based on DictionaryArray for representing sliding windows
// over time series data. Each element is a (offset, length) pair packed into an i64 key
// that references a slice of the underlying value array.

use arrow::array::ArrayData;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, DictionaryArray, Int64Array};
use arrow_schema::{DataType, Field};

use super::error::{EvalError, EvalResult};

pub type RangeTuple = (u32, u32);

/// Compound logical array representing several ranges (slices) of one base array.
/// Built on Arrow's DictionaryArray where each i64 key packs (offset, length) as two u32.
///
/// ```text
///  63        32│31         0
///  ┌───────────┼───────────┐
///  │offset(u32)│length(u32)│
///  └───────────┼───────────┘
/// ```
pub struct RangeArray {
    array: DictionaryArray<Int64Type>,
}

impl RangeArray {
    pub const fn key_type() -> DataType {
        DataType::Int64
    }

    pub fn value_type(&self) -> DataType {
        self.array.value_type()
    }

    pub fn try_new(dict: DictionaryArray<Int64Type>) -> EvalResult<Self> {
        let ranges: Vec<RangeTuple> = dict
            .keys()
            .iter()
            .map(|compound_key| {
                compound_key
                    .map(unpack)
                    .ok_or_else(|| EvalError::InvalidRange("empty range key".into()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Self::check_ranges(dict.values().len(), ranges.iter().copied())?;
        Ok(Self { array: dict })
    }

    pub fn from_ranges<R>(values: ArrayRef, ranges: R) -> EvalResult<Self>
    where
        R: IntoIterator<Item = RangeTuple> + Clone,
    {
        Self::check_ranges(values.len(), ranges.clone())?;
        unsafe { Ok(Self::from_ranges_unchecked(values, ranges)) }
    }

    /// # Safety
    /// Caller must ensure the given ranges are valid.
    pub unsafe fn from_ranges_unchecked<R>(values: ArrayRef, ranges: R) -> Self
    where
        R: IntoIterator<Item = RangeTuple>,
    {
        let key_array = Int64Array::from_iter(
            ranges
                .into_iter()
                .map(|(offset, length)| pack(offset, length)),
        );

        let mut data = ArrayData::builder(DataType::Dictionary(
            Box::new(Self::key_type()),
            Box::new(values.data_type().clone()),
        ))
        .len(key_array.len())
        .add_buffer(key_array.to_data().buffers()[0].clone())
        .add_child_data(values.to_data());

        match key_array.to_data().nulls() {
            Some(buffer) if key_array.to_data().null_count() > 0 => {
                data = data
                    .nulls(Some(buffer.clone()))
                    .null_count(key_array.to_data().null_count());
            }
            _ => data = data.null_count(0),
        }
        let array_data = unsafe { data.build_unchecked() };

        Self {
            array: array_data.into(),
        }
    }

    pub fn len(&self) -> usize {
        self.array.keys().len()
    }

    pub fn is_empty(&self) -> bool {
        self.array.keys().is_empty()
    }

    pub fn get(&self, index: usize) -> Option<ArrayRef> {
        if index >= self.len() {
            return None;
        }
        let compound_key = self.array.keys().value(index);
        let (offset, length) = unpack(compound_key);
        Some(self.array.values().slice(offset as usize, length as usize))
    }

    pub fn get_offset_length(&self, index: usize) -> Option<(usize, usize)> {
        if index >= self.len() {
            return None;
        }
        let compound_key = self.array.keys().value(index);
        let (offset, length) = unpack(compound_key);
        Some((offset as usize, length as usize))
    }

    pub fn into_dict(self) -> DictionaryArray<Int64Type> {
        self.array
    }

    fn check_ranges<R>(value_len: usize, ranges: R) -> EvalResult<()>
    where
        R: IntoIterator<Item = RangeTuple>,
    {
        for (offset, length) in ranges.into_iter() {
            if offset as usize + length as usize > value_len {
                return Err(EvalError::InvalidRange(format!(
                    "range ({}, {}) exceeds array length {}",
                    offset, length, value_len
                )));
            }
        }
        Ok(())
    }

    pub fn convert_field(field: &Field) -> Field {
        let value_type = Box::new(field.data_type().clone());
        Field::new(
            field.name(),
            Self::convert_data_type(*value_type),
            field.is_nullable(),
        )
    }

    pub fn convert_data_type(value_type: DataType) -> DataType {
        DataType::Dictionary(Box::new(Self::key_type()), Box::new(value_type))
    }

    pub fn values(&self) -> &ArrayRef {
        self.array.values()
    }

    pub fn ranges(&self) -> impl Iterator<Item = Option<RangeTuple>> + '_ {
        self.array
            .keys()
            .into_iter()
            .map(|compound| compound.map(unpack))
    }
}

impl std::fmt::Debug for RangeArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ranges: Vec<_> = self
            .array
            .keys()
            .iter()
            .map(|compound_key| {
                compound_key.map(|key| {
                    let (offset, length) = unpack(key);
                    offset..(offset + length)
                })
            })
            .collect();
        f.debug_struct("RangeArray")
            .field("base array", self.array.values())
            .field("ranges", &ranges)
            .finish()
    }
}

fn pack(offset: u32, length: u32) -> i64 {
    bytemuck::cast::<[u32; 2], i64>([offset, length])
}

pub(crate) fn unpack(compound: i64) -> (u32, u32) {
    let [offset, length] = bytemuck::cast::<i64, [u32; 2]>(compound);
    (offset, length)
}

#[cfg(test)]
mod test {
    use std::fmt::Write;
    use std::sync::Arc;

    use arrow_array::UInt64Array;

    use super::*;

    fn expand_format(range_array: &RangeArray) -> String {
        let mut result = String::new();
        for i in 0..range_array.len() {
            writeln!(result, "{:?}", range_array.get(i)).unwrap();
        }
        result
    }

    #[test]
    fn construct_from_ranges() {
        let values_array = Arc::new(UInt64Array::from_iter([1, 2, 3, 4, 5, 6, 7, 8, 9]));
        let ranges = [(0, 2), (0, 5), (1, 1), (3, 3), (8, 1), (9, 0)];
        let range_array = RangeArray::from_ranges(values_array, ranges).unwrap();
        assert_eq!(range_array.len(), 6);
    }

    #[test]
    fn illegal_range() {
        let values_array = Arc::new(UInt64Array::from_iter([1, 2, 3, 4, 5, 6, 7, 8, 9]));
        let ranges = [(9, 1)];
        assert!(RangeArray::from_ranges(values_array, ranges).is_err());
    }

    #[test]
    fn dict_array_round_trip() {
        let values_array = Arc::new(UInt64Array::from_iter([1, 2, 3, 4, 5, 6, 7, 8, 9]));
        let ranges = [(0, 4), (1, 4), (2, 4), (3, 4), (4, 4), (5, 4)];
        let range_array = RangeArray::from_ranges(values_array, ranges).unwrap();
        assert_eq!(range_array.len(), 6);

        let dict_array = range_array.into_dict();
        let rounded = RangeArray::try_new(dict_array).unwrap();
        assert_eq!(rounded.len(), 6);
        assert!(rounded.get(0).is_some());
    }

    #[test]
    fn empty_range_array() {
        let values_array = Arc::new(UInt64Array::from_iter([1, 2, 3, 4, 5, 6, 7, 8, 9]));
        let ranges: [(u32, u32); 0] = [];
        let range_array = RangeArray::from_ranges(values_array, ranges).unwrap();
        assert!(range_array.is_empty());
    }
}
