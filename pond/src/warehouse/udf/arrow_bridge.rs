use anyhow::Result;
use arrow::array::*;
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use wasmtime::{Instance, Store};

use gno_rs::wasm::runtime::HostState;
use gno_rs::wasm::udf::TableDescriptor;

const TYPE_TAG_I32: i32 = 0;
const TYPE_TAG_I64: i32 = 1;
const TYPE_TAG_F32: i32 = 2;
const TYPE_TAG_F64: i32 = 3;
const TYPE_TAG_STRING: i32 = 4;
const TYPE_TAG_BOOL: i32 = 5;
const TYPE_TAG_TIMESTAMP: i32 = 6;

const COLUMN_ENTRY_SIZE: usize = 16;

fn checked_i32(val: usize, label: &str) -> Result<i32> {
    i32::try_from(val)
        .map_err(|_| anyhow::anyhow!("{} ({}) exceeds WASM i32 address space", label, val))
}

pub struct ArrowWasmBridge;

impl ArrowWasmBridge {
    pub fn write_batch_to_wasm(
        store: &mut Store<HostState>,
        instance: &Instance,
        batch: &RecordBatch,
        input_schema: &TableDescriptor,
    ) -> Result<i32> {
        let alloc_fn = instance
            .get_typed_func::<i32, i32>(&mut *store, "alloc")
            .map_err(|e| anyhow::anyhow!("missing alloc export: {}", e))?;

        let memory = instance
            .get_memory(&mut *store, "memory")
            .ok_or_else(|| anyhow::anyhow!("missing memory export"))?;

        let row_count = batch.num_rows();
        let column_count = input_schema.fields.len();

        let mut column_ptrs: Vec<(i32, i32, i32)> = Vec::with_capacity(column_count);

        for field_desc in &input_schema.fields {
            let col_idx = batch
                .schema()
                .index_of(&field_desc.name)
                .map_err(|_| anyhow::anyhow!("column '{}' not found in batch", field_desc.name))?;
            let col = batch.column(col_idx);
            let type_tag = field_type_to_tag(&field_desc.field_type)?;
            let (data_ptr, null_ptr) =
                write_column_to_wasm(&mut *store, &alloc_fn, &memory, col, type_tag, row_count)?;
            column_ptrs.push((type_tag, data_ptr, null_ptr));
        }

        let header_size = 8 + column_count * COLUMN_ENTRY_SIZE;
        let header_ptr = alloc_fn.call(&mut *store, checked_i32(header_size, "header_size")?)?;

        {
            let mem_data = memory.data_mut(&mut *store);
            let base = header_ptr as usize;
            mem_data[base..base + 4].copy_from_slice(&checked_i32(row_count, "row_count")?.to_le_bytes());
            mem_data[base + 4..base + 8].copy_from_slice(&checked_i32(column_count, "column_count")?.to_le_bytes());

            for (i, (type_tag, data_ptr, null_ptr)) in column_ptrs.iter().enumerate() {
                let offset = base + 8 + i * COLUMN_ENTRY_SIZE;
                mem_data[offset..offset + 4].copy_from_slice(&type_tag.to_le_bytes());
                mem_data[offset + 4..offset + 8].copy_from_slice(&data_ptr.to_le_bytes());
                mem_data[offset + 8..offset + 12].copy_from_slice(&null_ptr.to_le_bytes());
                mem_data[offset + 12..offset + 16].copy_from_slice(&0i32.to_le_bytes());
            }
        }

        Ok(header_ptr)
    }

    pub fn read_batch_from_wasm(
        store: &mut Store<HostState>,
        instance: &Instance,
        output_ptr: i32,
        output_schema: &TableDescriptor,
    ) -> Result<RecordBatch> {
        let memory = instance
            .get_memory(&mut *store, "memory")
            .ok_or_else(|| anyhow::anyhow!("missing memory export"))?;

        let mem_data = memory.data(&*store);
        let base = output_ptr as usize;

        let row_count_i32 =
            i32::from_le_bytes(checked_slice(mem_data, base, 4)?.try_into()?);
        if row_count_i32 < 0 {
            anyhow::bail!("invalid negative row_count from WASM: {}", row_count_i32);
        }
        let row_count = row_count_i32 as usize;

        let column_count_i32 =
            i32::from_le_bytes(checked_slice(mem_data, base + 4, 4)?.try_into()?);
        if column_count_i32 < 0 {
            anyhow::bail!("invalid negative column_count from WASM: {}", column_count_i32);
        }
        let column_count = column_count_i32 as usize;

        if column_count != output_schema.fields.len() {
            anyhow::bail!(
                "output column count mismatch: header says {} but schema has {}",
                column_count,
                output_schema.fields.len()
            );
        }

        let mut fields = Vec::with_capacity(column_count);
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(column_count);

        for i in 0..column_count {
            let entry_offset = base + 8 + i * COLUMN_ENTRY_SIZE;
            let type_tag =
                i32::from_le_bytes(checked_slice(mem_data, entry_offset, 4)?.try_into()?);
            let data_ptr = i32::from_le_bytes(
                checked_slice(mem_data, entry_offset + 4, 4)?.try_into()?,
            ) as usize;
            let null_ptr = i32::from_le_bytes(
                checked_slice(mem_data, entry_offset + 8, 4)?.try_into()?,
            ) as usize;

            let field_desc = &output_schema.fields[i];
            let (field, array) =
                read_column_from_wasm(mem_data, data_ptr, null_ptr, type_tag, row_count, &field_desc.name)?;
            fields.push(field);
            columns.push(array);
        }

        let schema = Arc::new(Schema::new(fields));
        Ok(RecordBatch::try_new(schema, columns)?)
    }
}

fn field_type_to_tag(field_type: &str) -> Result<i32> {
    match field_type {
        "int32" | "int16" | "int8" | "uint8" | "uint16" => Ok(TYPE_TAG_I32),
        "int64" | "int" | "uint32" => Ok(TYPE_TAG_I64),
        "float32" => Ok(TYPE_TAG_F32),
        "float64" => Ok(TYPE_TAG_F64),
        "string" => Ok(TYPE_TAG_STRING),
        "bool" => Ok(TYPE_TAG_BOOL),
        "timestamp" => Ok(TYPE_TAG_TIMESTAMP),
        other => anyhow::bail!("unsupported UDF field type: {}", other),
    }
}

fn tag_to_arrow_type(tag: i32) -> Result<DataType> {
    match tag {
        TYPE_TAG_I32 => Ok(DataType::Int32),
        TYPE_TAG_I64 => Ok(DataType::Int64),
        TYPE_TAG_F32 => Ok(DataType::Float32),
        TYPE_TAG_F64 => Ok(DataType::Float64),
        TYPE_TAG_STRING => Ok(DataType::Utf8),
        TYPE_TAG_BOOL => Ok(DataType::Boolean),
        TYPE_TAG_TIMESTAMP => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        other => anyhow::bail!("unknown type tag: {}", other),
    }
}

fn write_null_bitmap(
    store: &mut Store<HostState>,
    alloc_fn: &wasmtime::TypedFunc<i32, i32>,
    memory: &wasmtime::Memory,
    col: &ArrayRef,
    row_count: usize,
) -> Result<i32> {
    if col.null_count() == 0 {
        return Ok(0i32);
    }
    let Some(nulls) = col.nulls() else {
        return Ok(0i32);
    };
    let bitmap_size = (row_count + 7) / 8;
    let ptr = alloc_fn.call(&mut *store, checked_i32(bitmap_size, "bitmap_size")?)?;
    let bitmap_bytes = nulls.inner().inner().as_slice();
    memory.data_mut(&mut *store)[ptr as usize..ptr as usize + bitmap_size]
        .copy_from_slice(&bitmap_bytes[..bitmap_size]);
    Ok(ptr)
}

fn write_column_to_wasm(
    store: &mut Store<HostState>,
    alloc_fn: &wasmtime::TypedFunc<i32, i32>,
    memory: &wasmtime::Memory,
    col: &ArrayRef,
    type_tag: i32,
    row_count: usize,
) -> Result<(i32, i32)> {
    let null_ptr = write_null_bitmap(store, alloc_fn, memory, col, row_count)?;

    match type_tag {
        TYPE_TAG_I32 => {
            let buf_size = row_count * 4;
            let ptr = alloc_fn.call(&mut *store, checked_i32(buf_size, "i32 buf_size")?)?;
            let arr = col.as_any().downcast_ref::<Int32Array>()
                .ok_or_else(|| anyhow::anyhow!("expected Int32Array"))?;
            let bytes = unsafe {
                std::slice::from_raw_parts(
                    arr.values().as_ptr() as *const u8,
                    buf_size,
                )
            };
            memory.data_mut(&mut *store)[ptr as usize..ptr as usize + buf_size]
                .copy_from_slice(bytes);
            Ok((ptr, null_ptr))
        }
        TYPE_TAG_I64 => {
            let buf_size = row_count * 8;
            let ptr = alloc_fn.call(&mut *store, checked_i32(buf_size, "i64 buf_size")?)?;
            let arr = col.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("expected Int64Array"))?;
            let bytes = unsafe {
                std::slice::from_raw_parts(
                    arr.values().as_ptr() as *const u8,
                    buf_size,
                )
            };
            memory.data_mut(&mut *store)[ptr as usize..ptr as usize + buf_size]
                .copy_from_slice(bytes);
            Ok((ptr, null_ptr))
        }
        TYPE_TAG_F32 => {
            let buf_size = row_count * 4;
            let ptr = alloc_fn.call(&mut *store, checked_i32(buf_size, "f32 buf_size")?)?;
            let arr = col.as_any().downcast_ref::<Float32Array>()
                .ok_or_else(|| anyhow::anyhow!("expected Float32Array"))?;
            let bytes = unsafe {
                std::slice::from_raw_parts(
                    arr.values().as_ptr() as *const u8,
                    buf_size,
                )
            };
            memory.data_mut(&mut *store)[ptr as usize..ptr as usize + buf_size]
                .copy_from_slice(bytes);
            Ok((ptr, null_ptr))
        }
        TYPE_TAG_F64 => {
            let buf_size = row_count * 8;
            let ptr = alloc_fn.call(&mut *store, checked_i32(buf_size, "f64 buf_size")?)?;
            let arr = col.as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow::anyhow!("expected Float64Array"))?;
            let bytes = unsafe {
                std::slice::from_raw_parts(
                    arr.values().as_ptr() as *const u8,
                    buf_size,
                )
            };
            memory.data_mut(&mut *store)[ptr as usize..ptr as usize + buf_size]
                .copy_from_slice(bytes);
            Ok((ptr, null_ptr))
        }
        TYPE_TAG_STRING => {
            let arr = col.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("expected StringArray"))?;

            let offsets_size = (row_count + 1) * 4;
            let mut total_bytes = 0usize;
            for i in 0..row_count {
                total_bytes += arr.value(i).len();
            }

            let total_size = offsets_size + total_bytes;
            let ptr = alloc_fn.call(&mut *store, checked_i32(total_size, "string total_size")?)?;

            let mem_data = memory.data_mut(&mut *store);
            let base = ptr as usize;

            let mut byte_offset: usize = 0;
            for i in 0..=row_count {
                let off = checked_i32(byte_offset, "string byte_offset")?;
                mem_data[base + i * 4..base + i * 4 + 4]
                    .copy_from_slice(&off.to_le_bytes());
                if i < row_count {
                    byte_offset = byte_offset.checked_add(arr.value(i).len())
                        .ok_or_else(|| anyhow::anyhow!("string data size overflow"))?;
                }
            }

            let string_base = base + offsets_size;
            let mut pos = 0usize;
            for i in 0..row_count {
                let s = arr.value(i);
                mem_data[string_base + pos..string_base + pos + s.len()]
                    .copy_from_slice(s.as_bytes());
                pos += s.len();
            }

            Ok((ptr, null_ptr))
        }
        TYPE_TAG_BOOL => {
            let arr = col.as_any().downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow::anyhow!("expected BooleanArray"))?;
            let ptr = alloc_fn.call(&mut *store, checked_i32(row_count, "bool buf_size")?)?;
            let mem_data = memory.data_mut(&mut *store);
            for i in 0..row_count {
                mem_data[ptr as usize + i] = if arr.value(i) { 1 } else { 0 };
            }
            Ok((ptr, null_ptr))
        }
        TYPE_TAG_TIMESTAMP => {
            let buf_size = row_count * 8;
            let ptr = alloc_fn.call(&mut *store, checked_i32(buf_size, "timestamp buf_size")?)?;
            let arr = col.as_any().downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| anyhow::anyhow!("expected TimestampMicrosecondArray"))?;
            let bytes = unsafe {
                std::slice::from_raw_parts(
                    arr.values().as_ptr() as *const u8,
                    buf_size,
                )
            };
            memory.data_mut(&mut *store)[ptr as usize..ptr as usize + buf_size]
                .copy_from_slice(bytes);
            Ok((ptr, null_ptr))
        }
        _ => anyhow::bail!("unsupported type tag for write: {}", type_tag),
    }
}

fn read_null_buffer(mem_data: &[u8], null_ptr: usize, row_count: usize) -> Result<Option<NullBuffer>> {
    if null_ptr == 0 {
        return Ok(None);
    }
    let bitmap_size = (row_count + 7) / 8;
    let bitmap_bytes = checked_slice(mem_data, null_ptr, bitmap_size)?.to_vec();
    Ok(Some(NullBuffer::new(BooleanBuffer::new(
        Buffer::from(bitmap_bytes),
        0,
        row_count,
    ))))
}

fn read_column_from_wasm(
    mem_data: &[u8],
    data_ptr: usize,
    null_ptr: usize,
    type_tag: i32,
    row_count: usize,
    col_name: &str,
) -> Result<(Field, ArrayRef)> {
    let dt = tag_to_arrow_type(type_tag)?;
    let nullable = null_ptr != 0;
    let field = Field::new(col_name, dt.clone(), nullable);
    let nulls = read_null_buffer(mem_data, null_ptr, row_count)?;

    let array: ArrayRef = match type_tag {
        TYPE_TAG_I32 => {
            let buf = checked_slice(mem_data, data_ptr, row_count * 4)?;
            let mut values = Vec::with_capacity(row_count);
            for i in 0..row_count {
                values.push(i32::from_le_bytes(buf[i * 4..i * 4 + 4].try_into()?));
            }
            Arc::new(Int32Array::new(ScalarBuffer::from(values), nulls))
        }
        TYPE_TAG_I64 => {
            let buf = checked_slice(mem_data, data_ptr, row_count * 8)?;
            let mut values = Vec::with_capacity(row_count);
            for i in 0..row_count {
                values.push(i64::from_le_bytes(buf[i * 8..i * 8 + 8].try_into()?));
            }
            Arc::new(Int64Array::new(ScalarBuffer::from(values), nulls))
        }
        TYPE_TAG_F32 => {
            let buf = checked_slice(mem_data, data_ptr, row_count * 4)?;
            let mut values = Vec::with_capacity(row_count);
            for i in 0..row_count {
                values.push(f32::from_le_bytes(buf[i * 4..i * 4 + 4].try_into()?));
            }
            Arc::new(Float32Array::new(ScalarBuffer::from(values), nulls))
        }
        TYPE_TAG_F64 => {
            let buf = checked_slice(mem_data, data_ptr, row_count * 8)?;
            let mut values = Vec::with_capacity(row_count);
            for i in 0..row_count {
                values.push(f64::from_le_bytes(buf[i * 8..i * 8 + 8].try_into()?));
            }
            Arc::new(Float64Array::new(ScalarBuffer::from(values), nulls))
        }
        TYPE_TAG_STRING => {
            let offsets_size = (row_count + 1) * 4;
            let offsets_buf = checked_slice(mem_data, data_ptr, offsets_size)?;
            let mut offsets = Vec::with_capacity(row_count + 1);
            for i in 0..=row_count {
                offsets.push(i32::from_le_bytes(offsets_buf[i * 4..i * 4 + 4].try_into()?));
            }
            let string_base = data_ptr + offsets_size;
            let mut values: Vec<Option<&str>> = Vec::with_capacity(row_count);
            for i in 0..row_count {
                if let Some(ref nb) = nulls {
                    if !nb.is_valid(i) {
                        values.push(None);
                        continue;
                    }
                }
                let start = offsets[i] as usize;
                let end = offsets[i + 1] as usize;
                if end < start {
                    anyhow::bail!(
                        "invalid string offsets in column '{}': end {} < start {} at row {}",
                        col_name, end, start, i
                    );
                }
                let s = std::str::from_utf8(checked_slice(mem_data, string_base + start, end - start)?)?;
                values.push(Some(s));
            }
            Arc::new(StringArray::from(values))
        }
        TYPE_TAG_BOOL => {
            let buf = checked_slice(mem_data, data_ptr, row_count)?;
            let mut values: Vec<Option<bool>> = Vec::with_capacity(row_count);
            for i in 0..row_count {
                if let Some(ref nb) = nulls {
                    if !nb.is_valid(i) {
                        values.push(None);
                        continue;
                    }
                }
                values.push(Some(buf[i] != 0));
            }
            Arc::new(BooleanArray::from(values))
        }
        TYPE_TAG_TIMESTAMP => {
            let buf = checked_slice(mem_data, data_ptr, row_count * 8)?;
            let mut values = Vec::with_capacity(row_count);
            for i in 0..row_count {
                values.push(i64::from_le_bytes(buf[i * 8..i * 8 + 8].try_into()?));
            }
            Arc::new(TimestampMicrosecondArray::new(ScalarBuffer::from(values), nulls))
        }
        _ => anyhow::bail!("unsupported type tag for read: {}", type_tag),
    };

    Ok((field, array))
}

fn checked_slice(mem_data: &[u8], start: usize, len: usize) -> Result<&[u8]> {
    mem_data.get(start..start.wrapping_add(len))
        .ok_or_else(|| anyhow::anyhow!(
            "WASM memory out of bounds: offset {} + length {} exceeds memory size {}",
            start, len, mem_data.len()
        ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_slice_valid() {
        let data = vec![0u8; 64];
        assert!(checked_slice(&data, 0, 4).is_ok());
        assert!(checked_slice(&data, 60, 4).is_ok());
    }

    #[test]
    fn checked_slice_out_of_bounds() {
        let data = vec![0u8; 64];
        assert!(checked_slice(&data, 61, 4).is_err());
        assert!(checked_slice(&data, 100, 1).is_err());
        assert!(checked_slice(&data, usize::MAX, 1).is_err());
    }

    #[test]
    fn read_null_buffer_zero_ptr_returns_none() {
        let data = vec![0u8; 64];
        assert!(read_null_buffer(&data, 0, 8).unwrap().is_none());
    }

    #[test]
    fn read_null_buffer_out_of_bounds() {
        let data = vec![0u8; 4];
        assert!(read_null_buffer(&data, 1, 64).is_err());
    }

    #[test]
    fn read_column_i32_out_of_bounds() {
        let data = vec![0u8; 4];
        let result = read_column_from_wasm(&data, 0, 0, TYPE_TAG_I32, 10, "col");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("out of bounds"));
    }

    #[test]
    fn read_column_string_bad_offsets() {
        let row_count: usize = 1;
        let offsets_size = (row_count + 1) * 4;
        let mut data = vec![0u8; offsets_size + 16];
        // offset[0] = 10, offset[1] = 5 → end < start
        data[0..4].copy_from_slice(&10i32.to_le_bytes());
        data[4..8].copy_from_slice(&5i32.to_le_bytes());

        let result = read_column_from_wasm(&data, 0, 0, TYPE_TAG_STRING, row_count, "s");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid string offsets"));
    }

    #[test]
    fn read_column_i32_valid() {
        let row_count = 2usize;
        let mut data = vec![0u8; row_count * 4];
        data[0..4].copy_from_slice(&42i32.to_le_bytes());
        data[4..8].copy_from_slice(&99i32.to_le_bytes());

        let (field, array) = read_column_from_wasm(&data, 0, 0, TYPE_TAG_I32, row_count, "x").unwrap();
        assert_eq!(field.name(), "x");
        let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(arr.value(0), 42);
        assert_eq!(arr.value(1), 99);
    }

    #[test]
    fn checked_i32_rejects_overflow() {
        let val = (i32::MAX as usize) + 1;
        let result = checked_i32(val, "test_val");
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("exceeds WASM i32 address space"), "got: {}", msg);
    }

    #[test]
    fn checked_i32_accepts_valid() {
        assert_eq!(checked_i32(0, "zero").unwrap(), 0);
        assert_eq!(checked_i32(1024, "small").unwrap(), 1024);
        assert_eq!(checked_i32(i32::MAX as usize, "max").unwrap(), i32::MAX);
    }
}
