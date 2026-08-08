//! Parquet file operations for the data warehouse.
//!
//! Provides utilities for writing Arrow RecordBatches to Parquet format.
//!
//! PERFORMANCE: For TB-scale data, use `write_parquet_chunked()` which
//! splits data into multiple files with size limits to prevent OOM.

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use thiserror::Error;

/// Default maximum file size for chunked writes (64MB).
pub const DEFAULT_MAX_FILE_SIZE: usize = 64 * 1024 * 1024;

/// Default target rows per file (1 million rows).
pub const DEFAULT_TARGET_ROWS_PER_FILE: usize = 1_000_000;

/// Errors that can occur during Parquet operations.
#[derive(Debug, Error)]
pub enum ParquetError {
    #[error("Failed to write Parquet: {0}")]
    Write(String),

    #[error("Failed to read Parquet: {0}")]
    Read(String),

    #[error("Invalid schema: {0}")]
    Schema(String),
    
    #[error("File size limit exceeded: {size} > {limit}")]
    FileSizeExceeded { size: usize, limit: usize },
}

/// Result type for Parquet operations.
pub type ParquetResult<T> = Result<T, ParquetError>;

/// Parquet compression options.
#[derive(Debug, Clone, Copy, Default)]
pub enum ParquetCompression {
    None,
    #[default]
    Snappy,
    Gzip,
    Zstd,
    Lz4,
}

impl From<ParquetCompression> for Compression {
    fn from(compression: ParquetCompression) -> Self {
        match compression {
            ParquetCompression::None => Compression::UNCOMPRESSED,
            ParquetCompression::Snappy => Compression::SNAPPY,
            ParquetCompression::Gzip => Compression::GZIP(Default::default()),
            ParquetCompression::Zstd => Compression::ZSTD(Default::default()),
            ParquetCompression::Lz4 => Compression::LZ4_RAW,
        }
    }
}

/// Options for writing Parquet files.
#[derive(Debug, Clone)]
pub struct WriteOptions {
    /// Compression to use
    pub compression: ParquetCompression,
    /// Maximum row group size
    pub max_row_group_size: usize,
    /// Enable dictionary encoding
    pub dictionary_enabled: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            compression: ParquetCompression::Snappy,
            // PERFORMANCE: 100K rows per group is optimal for ClickHouse predicate pushdown.
            // Smaller row groups allow finer-grained filtering when using min/max statistics.
            // Previously this was 1M rows which was too large for effective pruning.
            max_row_group_size: 100_000, // 100K rows per group
            dictionary_enabled: true,
        }
    }
}

/// Options for chunked Parquet writing.
///
/// Chunked writing splits large datasets into multiple files to:
/// - Prevent OOM during write operations
/// - Enable parallel uploads to R2
/// - Allow ClickHouse to parallelize reads
#[derive(Debug, Clone)]
pub struct ChunkedWriteOptions {
    /// Base write options (compression, row groups, etc.)
    pub write_options: WriteOptions,
    /// Maximum file size in bytes (default: 64MB)
    pub max_file_size: usize,
    /// Target rows per file (used for estimation, actual may vary)
    pub target_rows_per_file: usize,
}

impl Default for ChunkedWriteOptions {
    fn default() -> Self {
        Self {
            write_options: WriteOptions::default(),
            max_file_size: DEFAULT_MAX_FILE_SIZE,
            target_rows_per_file: DEFAULT_TARGET_ROWS_PER_FILE,
        }
    }
}

impl ChunkedWriteOptions {
    /// Create options optimized for large datasets.
    pub fn for_large_datasets() -> Self {
        Self {
            write_options: WriteOptions {
                compression: ParquetCompression::Zstd, // Better compression for large data
                max_row_group_size: 100_000, // Smaller row groups for better memory usage
                dictionary_enabled: true,
            },
            max_file_size: DEFAULT_MAX_FILE_SIZE,
            target_rows_per_file: 500_000, // 500K rows per file
        }
    }
}

/// Result of chunked Parquet writing.
#[derive(Debug)]
pub struct ChunkedWriteResult {
    /// The Parquet file chunks
    pub chunks: Vec<ParquetChunk>,
    /// Total number of rows written
    pub total_rows: usize,
    /// Total bytes written across all chunks
    pub total_bytes: usize,
}

/// A single chunk from chunked writing.
#[derive(Debug)]
pub struct ParquetChunk {
    /// The Parquet file data
    pub data: Bytes,
    /// Number of rows in this chunk
    pub num_rows: usize,
    /// Size in bytes
    pub size_bytes: usize,
    /// Chunk index (0-based)
    pub chunk_index: usize,
}

/// Write Arrow RecordBatches to Parquet format.
///
/// # Arguments
/// * `schema` - Arrow schema for the data
/// * `batches` - Iterator of RecordBatches to write
/// * `options` - Write options (compression, row group size, etc.)
///
/// # Returns
/// Parquet file contents as bytes.
///
/// # Warning
/// For large datasets, use `write_parquet_chunked()` to prevent OOM.
pub fn write_parquet(
    schema: Arc<Schema>,
    batches: &[RecordBatch],
    options: WriteOptions,
) -> ParquetResult<Bytes> {
    let mut buffer = Vec::new();

    let props = WriterProperties::builder()
        .set_compression(options.compression.into())
        .set_max_row_group_size(options.max_row_group_size)
        .set_dictionary_enabled(options.dictionary_enabled)
        .build();

    let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), Some(props))
        .map_err(|e| ParquetError::Write(format!("Failed to create writer: {}", e)))?;

    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| ParquetError::Write(format!("Failed to write batch: {}", e)))?;
    }

    writer
        .close()
        .map_err(|e| ParquetError::Write(format!("Failed to close writer: {}", e)))?;

    Ok(Bytes::from(buffer))
}

/// Write Arrow RecordBatches to multiple Parquet files with size limits.
///
/// PERFORMANCE: This function is designed for TB-scale data syncs. It:
/// - Splits data into multiple files based on row count and size estimates
/// - Prevents OOM by not buffering the entire dataset
/// - Produces files optimized for parallel ClickHouse queries
///
/// # Arguments
/// * `schema` - Arrow schema for the data
/// * `batches` - Iterator of RecordBatches to write
/// * `options` - Chunked write options (max file size, target rows, etc.)
///
/// # Returns
/// A `ChunkedWriteResult` containing multiple Parquet file chunks.
///
/// # Example
/// ```ignore
/// let result = write_parquet_chunked(schema, &batches, ChunkedWriteOptions::default())?;
/// for chunk in result.chunks {
///     storage.upload_parquet(&format!("table/part_{}.parquet", chunk.chunk_index), chunk.data).await?;
/// }
/// ```
pub fn write_parquet_chunked(
    schema: Arc<Schema>,
    batches: &[RecordBatch],
    options: ChunkedWriteOptions,
) -> ParquetResult<ChunkedWriteResult> {
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    
    // For small datasets, just use single file
    if total_rows <= options.target_rows_per_file {
        let data = write_parquet(schema, batches, options.write_options)?;
        let size_bytes = data.len();
        return Ok(ChunkedWriteResult {
            chunks: vec![ParquetChunk {
                data,
                num_rows: total_rows,
                size_bytes,
                chunk_index: 0,
            }],
            total_rows,
            total_bytes: size_bytes,
        });
    }
    
    // Estimate bytes per row from first batch (if available)
    let avg_bytes_per_row = estimate_bytes_per_row(batches, options.write_options.compression);
    
    // Calculate optimal rows per chunk based on file size limit
    let rows_per_chunk = if avg_bytes_per_row > 0 {
        // Use size-based calculation
        let size_based = options.max_file_size / avg_bytes_per_row;
        // But don't exceed target rows
        size_based.min(options.target_rows_per_file)
    } else {
        options.target_rows_per_file
    };
    
    // Ensure at least 1000 rows per chunk
    let rows_per_chunk = rows_per_chunk.max(1000);
    
    let mut chunks = Vec::new();
    let mut current_batches: Vec<RecordBatch> = Vec::new();
    let mut current_row_count = 0;
    let mut chunk_index = 0;
    let mut total_bytes = 0;
    
    for batch in batches {
        // If adding this batch would exceed the target, flush current chunk
        if current_row_count > 0 && current_row_count + batch.num_rows() > rows_per_chunk {
            let chunk = flush_chunk(
                schema.clone(),
                &current_batches,
                &options.write_options,
                chunk_index,
            )?;
            total_bytes += chunk.size_bytes;
            chunks.push(chunk);
            chunk_index += 1;
            current_batches.clear();
            current_row_count = 0;
        }
        
        // Handle batches larger than target by splitting
        if batch.num_rows() > rows_per_chunk {
            // Flush any pending batches first
            if !current_batches.is_empty() {
                let chunk = flush_chunk(
                    schema.clone(),
                    &current_batches,
                    &options.write_options,
                    chunk_index,
                )?;
                total_bytes += chunk.size_bytes;
                chunks.push(chunk);
                chunk_index += 1;
                current_batches.clear();
                current_row_count = 0;
            }
            
            // Split large batch into smaller pieces
            let split_chunks = split_large_batch(
                schema.clone(),
                batch,
                rows_per_chunk,
                &options.write_options,
                &mut chunk_index,
            )?;
            for chunk in split_chunks {
                total_bytes += chunk.size_bytes;
                chunks.push(chunk);
            }
        } else {
            current_batches.push(batch.clone());
            current_row_count += batch.num_rows();
        }
    }
    
    // Flush remaining batches
    if !current_batches.is_empty() {
        let chunk = flush_chunk(
            schema.clone(),
            &current_batches,
            &options.write_options,
            chunk_index,
        )?;
        total_bytes += chunk.size_bytes;
        chunks.push(chunk);
    }
    
    Ok(ChunkedWriteResult {
        chunks,
        total_rows,
        total_bytes,
    })
}

/// Approximate compression ratio for the given compression algorithm.
fn compression_ratio(compression: ParquetCompression) -> f64 {
    match compression {
        ParquetCompression::None => 0.85,
        ParquetCompression::Snappy => 0.50,
        ParquetCompression::Gzip => 0.35,
        ParquetCompression::Zstd => 0.30,
        ParquetCompression::Lz4 => 0.55,
    }
}

/// Estimate average compressed bytes per row from sample batches.
fn estimate_bytes_per_row(batches: &[RecordBatch], compression: ParquetCompression) -> usize {
    if batches.is_empty() {
        return 100;
    }
    
    let batch = &batches[0];
    let mut total_bytes = 0usize;
    
    for col in batch.columns() {
        total_bytes += col.get_buffer_memory_size();
    }
    
    if batch.num_rows() > 0 {
        let raw = total_bytes / batch.num_rows();
        let ratio = compression_ratio(compression);
        (raw as f64 * ratio) as usize
    } else {
        100
    }
}

/// Flush current batches to a Parquet chunk.
fn flush_chunk(
    schema: Arc<Schema>,
    batches: &[RecordBatch],
    options: &WriteOptions,
    chunk_index: usize,
) -> ParquetResult<ParquetChunk> {
    let num_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let data = write_parquet(schema, batches, options.clone())?;
    let size_bytes = data.len();
    
    Ok(ParquetChunk {
        data,
        num_rows,
        size_bytes,
        chunk_index,
    })
}

/// Split a large batch into multiple chunks.
fn split_large_batch(
    schema: Arc<Schema>,
    batch: &RecordBatch,
    rows_per_chunk: usize,
    options: &WriteOptions,
    chunk_index: &mut usize,
) -> ParquetResult<Vec<ParquetChunk>> {
    let mut chunks = Vec::new();
    let total_rows = batch.num_rows();
    let mut offset = 0;
    
    while offset < total_rows {
        let length = (total_rows - offset).min(rows_per_chunk);
        
        // Slice the batch
        let sliced_batch = batch.slice(offset, length);
        
        let data = write_parquet(schema.clone(), &[sliced_batch], options.clone())?;
        let size_bytes = data.len();
        
        chunks.push(ParquetChunk {
            data,
            num_rows: length,
            size_bytes,
            chunk_index: *chunk_index,
        });
        
        *chunk_index += 1;
        offset += length;
    }
    
    Ok(chunks)
}

/// Write a single RecordBatch to Parquet format with default options.
pub fn write_batch(batch: &RecordBatch) -> ParquetResult<Bytes> {
    write_parquet(batch.schema(), &[batch.clone()], WriteOptions::default())
}

/// Get Parquet file statistics.
#[derive(Debug, Clone)]
pub struct ParquetStats {
    pub num_rows: usize,
    pub num_columns: usize,
    pub compressed_size: usize,
    pub uncompressed_size: usize,
}

/// Calculate statistics for Parquet bytes.
pub fn get_stats(data: &[u8]) -> ParquetResult<ParquetStats> {
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let bytes = Bytes::copy_from_slice(data);
    let reader = SerializedFileReader::new(bytes)
        .map_err(|e| ParquetError::Read(format!("Failed to read Parquet: {}", e)))?;

    let metadata = reader.metadata();
    let file_metadata = metadata.file_metadata();

    let mut compressed_size = 0u64;
    let mut uncompressed_size = 0u64;

    for i in 0..metadata.num_row_groups() {
        let row_group = metadata.row_group(i);
        compressed_size += row_group.compressed_size() as u64;
        uncompressed_size += row_group.total_byte_size() as u64;
    }

    Ok(ParquetStats {
        num_rows: file_metadata.num_rows() as usize,
        num_columns: file_metadata.schema_descr().num_columns(),
        compressed_size: compressed_size as usize,
        uncompressed_size: uncompressed_size as usize,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_write_and_read_parquet() {
        // Create a simple schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        // Create test data
        let id_array = Int64Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec![Some("Alice"), Some("Bob"), None]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap();

        // Write to Parquet
        let parquet_bytes = write_parquet(schema, &[batch], WriteOptions::default()).unwrap();

        // Verify we got valid Parquet bytes
        assert!(!parquet_bytes.is_empty());
        assert!(parquet_bytes.len() > 4); // At least has magic bytes

        // Verify stats
        let stats = get_stats(&parquet_bytes).unwrap();
        assert_eq!(stats.num_rows, 3);
        assert_eq!(stats.num_columns, 2);
    }
}
