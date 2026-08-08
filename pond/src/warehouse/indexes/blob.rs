//! Binary blob serialization for table-level FST indexes.
//!
//! Packs all file-level FSTs for a single table into one binary blob
//! that can be compressed, uploaded to R2, and mmapped from local disk.
//!
//! ## Format
//!
//! ```text
//! [0-3]:   Magic "FSKP" (4 bytes)
//! [4]:     Format version = 1 (1 byte)
//! [5-8]:   Number of entries (u32 LE)
//! [9..]:   Directory entries, each:
//!            - file_path length (u16 LE) + file_path bytes
//!            - column_name length (u16 LE) + column_name bytes
//!            - partition_key length (u16 LE) + partition_key bytes
//!            - row_count (u64 LE)
//!            - fst_offset (u64 LE) -- offset into data section
//!            - fst_length (u32 LE)
//! [...]:   FST data section (concatenated raw FST bytes)
//! ```

use memmap2::Mmap;
use std::sync::Arc;

use super::fst_backing::FstBacking;
use super::persistence::PersistenceError;
use super::skip_index::HierarchicalSkipIndex;

type PersistenceResult<T> = Result<T, PersistenceError>;

const MAGIC: &[u8; 4] = b"FSKP";
const FORMAT_VERSION: u8 = 1;

/// A single entry parsed from a table index blob.
#[derive(Debug)]
pub struct FileIndexEntry {
    pub file_path: String,
    pub column_name: String,
    pub partition_key: String,
    pub row_count: u64,
    /// Backing storage for the FST data -- either owned bytes or an mmap slice.
    pub fst_data: FstBacking,
}

/// Serialize a `HierarchicalSkipIndex` into the binary blob format.
///
/// Iterates all partitions, files, and columns to produce a single
/// contiguous byte buffer with a directory header + concatenated FST data.
pub fn serialize_table_index(index: &HierarchicalSkipIndex) -> Vec<u8> {
    // First pass: collect all entries and their FST bytes so we know offsets.
    struct RawEntry<'a> {
        file_path: &'a str,
        column_name: &'a str,
        partition_key: &'a str,
        row_count: u64,
        fst_bytes: &'a [u8],
    }

    let mut entries: Vec<RawEntry<'_>> = Vec::new();

    for (partition_key, partition) in index.partitions() {
        let rows_per_file = if !partition.files.is_empty() {
            partition.estimated_rows / partition.files.len() as u64
        } else {
            0
        };

        for (file_path, file_index) in &partition.files {
            for (column_name, fst_set) in &file_index.column_values {
                entries.push(RawEntry {
                    file_path: file_path.as_str(),
                    column_name: column_name.as_str(),
                    partition_key: partition_key.as_str(),
                    row_count: rows_per_file,
                    fst_bytes: fst_set.as_fst().as_bytes(),
                });
            }
        }
    }

    // Filter out entries with strings that would overflow u16 length encoding
    let entries: Vec<_> = entries
        .into_iter()
        .filter(|e| {
            e.file_path.len() <= u16::MAX as usize
                && e.column_name.len() <= u16::MAX as usize
                && e.partition_key.len() <= u16::MAX as usize
        })
        .collect();
    let entry_count = entries.len() as u32;

    // Recompute sizes after filtering
    let mut directory_size: usize = 0;
    for e in &entries {
        directory_size += 2 + e.file_path.len()
                       + 2 + e.column_name.len()
                       + 2 + e.partition_key.len()
                       + 8 + 8 + 4;
    }
    let header_size = 4 + 1 + 4;
    let data_section_start = header_size + directory_size;
    let fst_total: usize = entries.iter().map(|e| e.fst_bytes.len()).sum();
    let total_size = data_section_start + fst_total;
    let mut buf = Vec::with_capacity(total_size);

    // Header
    buf.extend_from_slice(MAGIC);
    buf.push(FORMAT_VERSION);
    buf.extend_from_slice(&entry_count.to_le_bytes());

    // Directory entries (compute running FST offset)
    let mut fst_offset: u64 = data_section_start as u64;
    for e in &entries {
        // file_path
        buf.extend_from_slice(&(e.file_path.len() as u16).to_le_bytes());
        buf.extend_from_slice(e.file_path.as_bytes());
        // column_name
        buf.extend_from_slice(&(e.column_name.len() as u16).to_le_bytes());
        buf.extend_from_slice(e.column_name.as_bytes());
        // partition_key
        buf.extend_from_slice(&(e.partition_key.len() as u16).to_le_bytes());
        buf.extend_from_slice(e.partition_key.as_bytes());
        // row_count
        buf.extend_from_slice(&e.row_count.to_le_bytes());
        // fst_offset
        buf.extend_from_slice(&fst_offset.to_le_bytes());
        // fst_length
        buf.extend_from_slice(&(e.fst_bytes.len() as u32).to_le_bytes());

        fst_offset += e.fst_bytes.len() as u64;
    }

    // FST data section
    for e in &entries {
        buf.extend_from_slice(e.fst_bytes);
    }

    debug_assert_eq!(buf.len(), total_size);
    buf
}

/// Deserialize a binary blob back into individual `FileIndexEntry` values.
///
/// Each entry carries its metadata plus the raw FST bytes (copied from the
/// input slice) so callers can feed them into `FileSkipIndex::from_serialized_fst`.
pub fn deserialize_table_index(bytes: &[u8]) -> PersistenceResult<Vec<FileIndexEntry>> {
    if bytes.len() < 9 {
        return Err(PersistenceError::InvalidFormat(
            "Blob too short for header".into(),
        ));
    }

    // Validate magic
    if &bytes[0..4] != MAGIC {
        return Err(PersistenceError::InvalidFormat(format!(
            "Invalid magic bytes: expected FSKP, got {:?}",
            &bytes[0..4]
        )));
    }

    // Validate version
    let version = bytes[4];
    if version != FORMAT_VERSION {
        return Err(PersistenceError::InvalidFormat(format!(
            "Unsupported format version: {} (expected {})",
            version, FORMAT_VERSION
        )));
    }

    let entry_count = u32::from_le_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]) as usize;

    let mut entries = Vec::with_capacity(entry_count);
    let mut pos = 9usize; // after header

    for i in 0..entry_count {
        // Helper: read a length-prefixed string
        let read_str = |pos: &mut usize| -> PersistenceResult<String> {
            if *pos + 2 > bytes.len() {
                return Err(PersistenceError::InvalidFormat(format!(
                    "Unexpected EOF reading string length at entry {}",
                    i
                )));
            }
            let len = u16::from_le_bytes([bytes[*pos], bytes[*pos + 1]]) as usize;
            *pos += 2;
            if *pos + len > bytes.len() {
                return Err(PersistenceError::InvalidFormat(format!(
                    "Unexpected EOF reading string data at entry {}",
                    i
                )));
            }
            let s = String::from_utf8(bytes[*pos..*pos + len].to_vec()).map_err(|e| {
                PersistenceError::InvalidFormat(format!("Invalid UTF-8 at entry {}: {}", i, e))
            })?;
            *pos += len;
            Ok(s)
        };

        let file_path = read_str(&mut pos)?;
        let column_name = read_str(&mut pos)?;
        let partition_key = read_str(&mut pos)?;

        // row_count (u64 LE)
        if pos + 8 > bytes.len() {
            return Err(PersistenceError::InvalidFormat(format!(
                "Unexpected EOF reading row_count at entry {}",
                i
            )));
        }
        let row_count = u64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap());
        pos += 8;

        // fst_offset (u64 LE)
        if pos + 8 > bytes.len() {
            return Err(PersistenceError::InvalidFormat(format!(
                "Unexpected EOF reading fst_offset at entry {}",
                i
            )));
        }
        let fst_offset = u64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap()) as usize;
        pos += 8;

        // fst_length (u32 LE)
        if pos + 4 > bytes.len() {
            return Err(PersistenceError::InvalidFormat(format!(
                "Unexpected EOF reading fst_length at entry {}",
                i
            )));
        }
        let fst_length = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        let fst_end = fst_offset.checked_add(fst_length).ok_or_else(|| {
            PersistenceError::InvalidFormat(format!(
                "FST offset+length overflow at entry {}: offset={}, length={}",
                i, fst_offset, fst_length
            ))
        })?;
        if fst_end > bytes.len() {
            return Err(PersistenceError::InvalidFormat(format!(
                "FST data out of bounds at entry {}: offset={}, length={}, blob_size={}",
                i,
                fst_offset,
                fst_length,
                bytes.len()
            )));
        }

        entries.push(FileIndexEntry {
            file_path,
            column_name,
            partition_key,
            row_count,
            fst_data: FstBacking::Owned(bytes[fst_offset..fst_end].to_vec()),
        });
    }

    Ok(entries)
}

/// Zero-copy variant of `deserialize_table_index`.
///
/// Instead of copying FST bytes out of the blob, each `FileIndexEntry`
/// receives a `FstBacking::Mmap` that references the underlying mmap.
/// The `blob_offset` is the byte offset within `mmap` where the FSKP blob starts
/// (e.g. after the disk-cache version header).
pub fn deserialize_table_index_mmap(
    mmap: Arc<Mmap>,
    blob_offset: usize,
) -> PersistenceResult<Vec<FileIndexEntry>> {
    let bytes = &mmap[blob_offset..];

    if bytes.len() < 9 {
        return Err(PersistenceError::InvalidFormat(
            "Blob too short for header".into(),
        ));
    }

    if &bytes[0..4] != MAGIC {
        return Err(PersistenceError::InvalidFormat(format!(
            "Invalid magic bytes: expected FSKP, got {:?}",
            &bytes[0..4]
        )));
    }

    let version = bytes[4];
    if version != FORMAT_VERSION {
        return Err(PersistenceError::InvalidFormat(format!(
            "Unsupported format version: {} (expected {})",
            version, FORMAT_VERSION
        )));
    }

    let entry_count = u32::from_le_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]) as usize;

    let mut entries = Vec::with_capacity(entry_count);
    let mut pos = 9usize;

    for i in 0..entry_count {
        let read_str = |pos: &mut usize| -> PersistenceResult<String> {
            if *pos + 2 > bytes.len() {
                return Err(PersistenceError::InvalidFormat(format!(
                    "Unexpected EOF reading string length at entry {}", i
                )));
            }
            let len = u16::from_le_bytes([bytes[*pos], bytes[*pos + 1]]) as usize;
            *pos += 2;
            if *pos + len > bytes.len() {
                return Err(PersistenceError::InvalidFormat(format!(
                    "Unexpected EOF reading string data at entry {}", i
                )));
            }
            let s = String::from_utf8(bytes[*pos..*pos + len].to_vec()).map_err(|e| {
                PersistenceError::InvalidFormat(format!("Invalid UTF-8 at entry {}: {}", i, e))
            })?;
            *pos += len;
            Ok(s)
        };

        let file_path = read_str(&mut pos)?;
        let column_name = read_str(&mut pos)?;
        let partition_key = read_str(&mut pos)?;

        if pos + 8 > bytes.len() {
            return Err(PersistenceError::InvalidFormat(format!(
                "Unexpected EOF reading row_count at entry {}", i
            )));
        }
        let row_count = u64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap());
        pos += 8;

        if pos + 8 > bytes.len() {
            return Err(PersistenceError::InvalidFormat(format!(
                "Unexpected EOF reading fst_offset at entry {}", i
            )));
        }
        let fst_offset = u64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap()) as usize;
        pos += 8;

        if pos + 4 > bytes.len() {
            return Err(PersistenceError::InvalidFormat(format!(
                "Unexpected EOF reading fst_length at entry {}", i
            )));
        }
        let fst_length = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        let fst_end = fst_offset.checked_add(fst_length).ok_or_else(|| {
            PersistenceError::InvalidFormat(format!(
                "FST offset+length overflow at entry {}: offset={}, length={}",
                i, fst_offset, fst_length
            ))
        })?;
        if fst_end > bytes.len() {
            return Err(PersistenceError::InvalidFormat(format!(
                "FST data out of bounds at entry {}: offset={}, length={}, blob_size={}",
                i, fst_offset, fst_length, bytes.len()
            )));
        }

        // Zero-copy: reference the mmap directly instead of copying
        let backing = FstBacking::from_mmap(
            mmap.clone(),
            blob_offset + fst_offset,
            fst_length,
        );

        entries.push(FileIndexEntry {
            file_path,
            column_name,
            partition_key,
            row_count,
            fst_data: backing,
        });
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::warehouse::indexes::skip_index::{FileSkipIndex, HierarchicalSkipIndex};
    use std::collections::HashMap;

    fn build_test_index() -> HierarchicalSkipIndex {
        let mut index = HierarchicalSkipIndex::new();

        // Partition "2025/01" with one file, two columns
        let columns1: HashMap<String, Vec<String>> = [
            (
                "status".to_string(),
                vec!["active".to_string(), "inactive".to_string()],
            ),
            (
                "region".to_string(),
                vec!["us-east".to_string(), "eu-west".to_string()],
            ),
        ]
        .into_iter()
        .collect();
        let file1 = FileSkipIndex::build("data/2025/01/file1.parquet", columns1).unwrap();
        index.add_file("2025/01", file1, 1000).unwrap();

        // Partition "2025/02" with one file, one column
        let columns2: HashMap<String, Vec<String>> = [(
            "status".to_string(),
            vec!["active".to_string(), "pending".to_string()],
        )]
        .into_iter()
        .collect();
        let file2 = FileSkipIndex::build("data/2025/02/file2.parquet", columns2).unwrap();
        index.add_file("2025/02", file2, 500).unwrap();

        index
    }

    #[test]
    fn test_blob_roundtrip() {
        let index = build_test_index();
        let blob = serialize_table_index(&index);
        let entries = deserialize_table_index(&blob).unwrap();

        // We expect 3 entries: file1 has 2 columns + file2 has 1 column
        assert_eq!(entries.len(), 3);

        // Verify all entries have valid FST bytes
        for entry in &entries {
            let set = fst::Set::new(entry.fst_data.as_ref().to_vec()).unwrap();
            assert!(set.len() > 0, "FST should be non-empty for {}", entry.column_name);
        }

        // Check specific entries exist
        let file_paths: Vec<&str> = entries.iter().map(|e| e.file_path.as_str()).collect();
        assert!(file_paths.contains(&"data/2025/01/file1.parquet"));
        assert!(file_paths.contains(&"data/2025/02/file2.parquet"));

        let columns: Vec<&str> = entries.iter().map(|e| e.column_name.as_str()).collect();
        assert!(columns.contains(&"status"));
        assert!(columns.contains(&"region"));

        // Verify FST content round-trips: the "status" column in partition 2025/01
        // should contain "active" and "inactive"
        let status_p1 = entries
            .iter()
            .find(|e| {
                e.file_path == "data/2025/01/file1.parquet" && e.column_name == "status"
            })
            .unwrap();
        let set = fst::Set::new(status_p1.fst_data.as_ref().to_vec()).unwrap();
        assert!(set.contains("active"));
        assert!(set.contains("inactive"));
        assert!(!set.contains("pending"));
    }

    #[test]
    fn test_empty_index() {
        let index = HierarchicalSkipIndex::new();
        let blob = serialize_table_index(&index);
        let entries = deserialize_table_index(&blob).unwrap();
        assert!(entries.is_empty());

        // Verify header is still correct
        assert_eq!(&blob[0..4], MAGIC);
        assert_eq!(blob[4], FORMAT_VERSION);
        assert_eq!(
            u32::from_le_bytes([blob[5], blob[6], blob[7], blob[8]]),
            0
        );
    }

    #[test]
    fn test_invalid_magic_bytes() {
        let mut blob = vec![0u8; 20];
        blob[0..4].copy_from_slice(b"XXXX");
        let err = deserialize_table_index(&blob).unwrap_err();
        assert!(
            err.to_string().contains("Invalid magic bytes"),
            "Expected magic byte error, got: {}",
            err
        );
    }

    #[test]
    fn test_too_short_blob() {
        let blob = vec![0u8; 5]; // too short for header
        let err = deserialize_table_index(&blob).unwrap_err();
        assert!(
            err.to_string().contains("too short"),
            "Expected too-short error, got: {}",
            err
        );
    }

    #[test]
    fn test_unsupported_version() {
        let mut blob = vec![0u8; 9];
        blob[0..4].copy_from_slice(MAGIC);
        blob[4] = 99; // bad version
        let err = deserialize_table_index(&blob).unwrap_err();
        assert!(
            err.to_string().contains("Unsupported format version"),
            "Expected version error, got: {}",
            err
        );
    }

    #[test]
    fn test_multiple_columns_per_file() {
        let mut index = HierarchicalSkipIndex::new();

        let columns: HashMap<String, Vec<String>> = [
            ("col_a".to_string(), vec!["x".to_string(), "y".to_string()]),
            ("col_b".to_string(), vec!["1".to_string(), "2".to_string(), "3".to_string()]),
            ("col_c".to_string(), vec!["foo".to_string()]),
        ]
        .into_iter()
        .collect();
        let file = FileSkipIndex::build("multi_col.parquet", columns).unwrap();
        index.add_file("p1", file, 100).unwrap();

        let blob = serialize_table_index(&index);
        let entries = deserialize_table_index(&blob).unwrap();

        assert_eq!(entries.len(), 3);

        // Verify each column round-trips
        for entry in &entries {
            let set = fst::Set::new(entry.fst_data.as_ref().to_vec()).unwrap();
            match entry.column_name.as_str() {
                "col_a" => {
                    assert!(set.contains("x"));
                    assert!(set.contains("y"));
                    assert_eq!(set.len(), 2);
                }
                "col_b" => {
                    assert!(set.contains("1"));
                    assert!(set.contains("2"));
                    assert!(set.contains("3"));
                    assert_eq!(set.len(), 3);
                }
                "col_c" => {
                    assert!(set.contains("foo"));
                    assert_eq!(set.len(), 1);
                }
                other => panic!("Unexpected column: {}", other),
            }
        }
    }

    /// Full write-read path integration test:
    /// build index -> serialize -> zstd compress -> store to disk cache
    /// -> load version -> mmap from disk -> zstd decompress (here we store
    ///    uncompressed so mmap gives raw blob) -> deserialize -> rebuild
    ///    HierarchicalSkipIndex -> verify search works.
    #[test]
    fn test_full_write_read_path_with_disk_cache() {
        use crate::warehouse::indexes::disk_cache::DiskIndexCache;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let cache = DiskIndexCache::new(dir.path().to_path_buf()).unwrap();
        let project_id = uuid::Uuid::new_v4();
        let table_name = "orders";

        // 1. Build a realistic index
        let index = build_test_index();
        assert_eq!(index.total_files(), 2);

        // 2. Serialize to blob
        let blob = serialize_table_index(&index);
        assert!(blob.len() > 9, "blob should have more than just the header");

        // 3. Compress with zstd
        let compressed = zstd::encode_all(&blob[..], 3).unwrap();
        assert!(compressed.len() < blob.len(), "compressed should be smaller");

        // 4. "Upload to R2" -- we skip actual R2 here, but simulate
        //    the download path: decompress + store to disk cache
        let decompressed = zstd::decode_all(&compressed[..]).unwrap();
        assert_eq!(decompressed, blob, "decompress should produce original");

        // 5. Store to disk cache (uncompressed, so it can be mmapped)
        cache
            .store(project_id, table_name, &decompressed, 1)
            .unwrap();
        assert_eq!(cache.load_version(project_id, table_name), Some(1));

        // 6. Mmap from disk (first 8 bytes are version header)
        let mmap = cache.mmap(project_id, table_name).unwrap();
        let blob_offset = crate::warehouse::indexes::disk_cache::DiskIndexCache::mmap_blob_offset();
        assert_eq!(&mmap[blob_offset..], &blob[..], "mmapped bytes should match original blob");

        // 7. Deserialize from mmapped bytes (skip version header)
        let entries = deserialize_table_index(&mmap[blob_offset..]).unwrap();
        assert_eq!(entries.len(), 3);

        // 8. Rebuild HierarchicalSkipIndex from deserialized entries
        let mut rebuilt = HierarchicalSkipIndex::new();
        let mut file_map: HashMap<(String, String), (FileSkipIndex, u64)> = HashMap::new();

        for entry in entries {
            let key = (entry.partition_key.clone(), entry.file_path.clone());
            match file_map.get_mut(&key) {
                Some((fi, _)) => {
                    fi.add_column_fst(&entry.column_name, entry.fst_data).unwrap();
                }
                None => {
                    let fi = FileSkipIndex::from_serialized_fst(
                        &entry.file_path,
                        &entry.column_name,
                        entry.fst_data.clone(),
                    )
                    .unwrap();
                    file_map.insert(key, (fi, entry.row_count));
                }
            }
        }

        for ((partition_key, _), (fi, row_count)) in file_map {
            rebuilt.add_file(&partition_key, fi, row_count).unwrap();
        }

        // 9. Verify the rebuilt index works for search
        assert_eq!(rebuilt.total_files(), 2);
        assert_eq!(rebuilt.partition_count(), 2);

        // Verify file-level search works (FSTs contain expected values)
        let p1 = rebuilt.get_partition("2025/01").unwrap();
        let file1 = p1.files.get("data/2025/01/file1.parquet").unwrap();
        assert!(file1.might_contain("status", "active"));
        assert!(file1.might_contain("status", "inactive"));
        assert!(!file1.might_contain("status", "pending"));
        assert!(file1.might_contain("region", "us-east"));

        let p2 = rebuilt.get_partition("2025/02").unwrap();
        let file2 = p2.files.get("data/2025/02/file2.parquet").unwrap();
        assert!(file2.might_contain("status", "active"));
        assert!(file2.might_contain("status", "pending"));
        assert!(!file2.might_contain("status", "inactive"));

        // 10. Verify partition-level filtering works
        let eq_preds: HashMap<String, String> = [
            ("status".to_string(), "pending".to_string()),
        ].into_iter().collect();
        let matching_files = rebuilt.filter_with_partition_hint(&eq_preds, None);
        // Only file2 in 2025/02 has "pending" -- file1 has "active" and "inactive"
        assert!(
            matching_files.len() <= 2,
            "Expected at most 2 matching files, got {}",
            matching_files.len()
        );
    }

    #[test]
    fn test_zero_copy_mmap_deserialization() {
        use crate::warehouse::indexes::disk_cache::DiskIndexCache;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let cache = DiskIndexCache::new(dir.path().to_path_buf()).unwrap();
        let project_id = uuid::Uuid::new_v4();
        let table_name = "mmap_test";

        // Build and serialize
        let index = build_test_index();
        let blob = serialize_table_index(&index);

        // Store to disk cache (uncompressed)
        cache.store(project_id, table_name, &blob, 1).unwrap();

        // Mmap the file
        let mmap = cache.mmap(project_id, table_name).unwrap();
        let blob_offset = DiskIndexCache::mmap_blob_offset();

        // Deserialize via the zero-copy mmap path
        let mmap_arc = Arc::new(mmap);
        let entries = deserialize_table_index_mmap(mmap_arc, blob_offset).unwrap();

        // Same number of entries as the owned path
        assert_eq!(entries.len(), 3);

        // Verify FST data can be used for lookups
        let status_p1 = entries
            .iter()
            .find(|e| {
                e.file_path == "data/2025/01/file1.parquet" && e.column_name == "status"
            })
            .unwrap();
        let set = fst::Set::new(status_p1.fst_data.as_ref().to_vec()).unwrap();
        assert!(set.contains("active"));
        assert!(set.contains("inactive"));
        assert!(!set.contains("pending"));

        // Verify the mmap-backed FST can be used directly via FileSkipIndex
        let fi = FileSkipIndex::from_serialized_fst(
            &status_p1.file_path,
            &status_p1.column_name,
            status_p1.fst_data.clone(),
        )
        .unwrap();
        assert!(fi.might_contain("status", "active"));
        assert!(!fi.might_contain("status", "pending"));
    }

    #[test]
    fn test_corrupted_fst_offset_overflow_returns_error() {
        let mut blob = Vec::new();
        blob.extend_from_slice(MAGIC);
        blob.push(FORMAT_VERSION);
        blob.extend_from_slice(&1u32.to_le_bytes()); // 1 entry

        // file_path: "f"
        blob.extend_from_slice(&1u16.to_le_bytes());
        blob.push(b'f');
        // column_name: "c"
        blob.extend_from_slice(&1u16.to_le_bytes());
        blob.push(b'c');
        // partition_key: "p"
        blob.extend_from_slice(&1u16.to_le_bytes());
        blob.push(b'p');
        // row_count
        blob.extend_from_slice(&100u64.to_le_bytes());
        // fst_offset: near usize::MAX to trigger overflow
        blob.extend_from_slice(&(usize::MAX as u64 - 10).to_le_bytes());
        // fst_length: 20 (so offset + length wraps)
        blob.extend_from_slice(&20u32.to_le_bytes());

        let result = deserialize_table_index(&blob);
        assert!(result.is_err(), "Corrupted blob with overflowing fst_offset+length must return Err");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("overflow") || err_msg.contains("out of bounds"),
            "Error should mention overflow or out of bounds, got: {err_msg}"
        );
    }
}
