//! String Column Index
//!
//! FST-based index for fast exact-match and prefix lookups on string columns.

use fst::{IntoStreamer, Map, MapBuilder, Streamer};
use std::io;
use std::sync::Arc;
use thiserror::Error;

use super::fst_backing::FstBacking;
use super::skip_index::SubstringAutomaton;

/// Errors that can occur during column index operations.
#[derive(Debug, Error)]
pub enum ColumnIndexError {
    #[error("FST error: {0}")]
    Fst(#[from] fst::Error),

    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Keys must be sorted")]
    UnsortedKeys,
}

/// Result type for column index operations.
pub type ColumnIndexResult<T> = Result<T, ColumnIndexError>;

/// Location of a value in a Parquet file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileLocation {
    /// File identifier
    pub file_id: u32,
    /// Row offset within the file
    pub row_offset: u32,
}

impl FileLocation {
    /// Create a new file location.
    pub fn new(file_id: u32, row_offset: u32) -> Self {
        Self { file_id, row_offset }
    }

    /// Encode the location as a u64 for storage in FST.
    pub fn encode(&self) -> u64 {
        ((self.file_id as u64) << 32) | (self.row_offset as u64)
    }

    /// Decode a u64 back to a file location.
    pub fn decode(encoded: u64) -> Self {
        Self {
            file_id: (encoded >> 32) as u32,
            row_offset: encoded as u32,
        }
    }
}

/// FST-based index for string column values.
#[derive(Clone)]
pub struct StringColumnIndex {
    /// The FST map: value -> encoded file location
    index: Map<FstBacking>,
    /// Column name
    column_name: String,
}

impl std::fmt::Debug for StringColumnIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StringColumnIndex")
            .field("column_name", &self.column_name)
            .field("len", &self.index.len())
            .finish()
    }
}

impl StringColumnIndex {
    /// Build index from an iterator of (value, location) pairs.
    ///
    /// Keys must be provided in sorted order.
    pub fn build(
        column_name: &str,
        rows: impl IntoIterator<Item = (String, FileLocation)>,
    ) -> ColumnIndexResult<Self> {
        let mut builder = MapBuilder::memory();
        let mut last_key: Option<String> = None;

        for (value, location) in rows {
            if let Some(ref last) = last_key {
                if &value <= last {
                    return Err(ColumnIndexError::UnsortedKeys);
                }
            }
            builder.insert(&value, location.encode())?;
            last_key = Some(value);
        }

        let raw = builder.into_inner()?;
        Ok(Self {
            index: Map::new(FstBacking::Owned(raw))?,
            column_name: column_name.to_string(),
        })
    }

    /// Build index from unsorted data.
    ///
    /// Sorts the data before building the index.
    pub fn build_unsorted(
        column_name: &str,
        rows: Vec<(String, FileLocation)>,
    ) -> ColumnIndexResult<Self> {
        let mut sorted_rows = rows;
        sorted_rows.sort_by(|a, b| a.0.cmp(&b.0));

        // Dedup duplicate keys. FST requires strictly increasing keys,
        // so we keep the last occurrence for each value.
        sorted_rows.dedup_by(|b, a| {
            if a.0 == b.0 {
                a.1 = b.1;
                true
            } else {
                false
            }
        });

        Self::build(column_name, sorted_rows)
    }

    /// Approximate in-memory size of this index in bytes.
    pub fn estimated_byte_size(&self) -> usize {
        self.index.as_fst().as_bytes().len() + self.column_name.len()
    }

    /// O(k) lookup for exact match (k = key length).
    pub fn lookup(&self, value: &str) -> Option<FileLocation> {
        self.index.get(value).map(FileLocation::decode)
    }

    /// Prefix search. An empty prefix matches all entries.
    pub fn prefix_search(&self, prefix: &str) -> Vec<(String, FileLocation)> {
        let mut results = Vec::new();

        if prefix.is_empty() {
            let mut stream = self.index.stream();
            while let Some((key, value)) = stream.next() {
                if let Ok(s) = std::str::from_utf8(key) {
                    results.push((s.to_string(), FileLocation::decode(value)));
                }
            }
            return results;
        }

        let upper = increment_last_byte(prefix);
        let mut stream = if upper == prefix {
            self.index.range().ge(prefix).into_stream()
        } else {
            self.index.range().ge(prefix).lt(&upper).into_stream()
        };

        while let Some((key, value)) = stream.next() {
            if let Ok(s) = std::str::from_utf8(key) {
                results.push((s.to_string(), FileLocation::decode(value)));
            }
        }

        results
    }

    /// Check if any key in the FST map contains the given substring.
    ///
    /// Uses the `SubstringAutomaton` (regex-automata DFA) to walk the FST
    /// and stop early on the first match.
    pub fn contains_substring(&self, substring: &str) -> bool {
        let aut = match SubstringAutomaton::new(substring) {
            Some(a) => a,
            None => return true, // can't build DFA, assume might contain
        };
        let mut stream = self.index.search(&aut).into_stream();
        stream.next().is_some()
    }

    /// Get the column name.
    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    /// Get the number of entries in the index.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Approximate COUNT(DISTINCT column) - O(1).
    pub fn estimated_cardinality(&self) -> usize {
        self.index.len()
    }

    /// Check if column is low-cardinality (good for GROUP BY optimization).
    pub fn is_low_cardinality(&self, threshold: usize) -> bool {
        self.index.len() < threshold
    }

    /// Get all distinct values (for small cardinality columns).
    pub fn get_distinct_values(&self, limit: usize) -> Vec<String> {
        let mut results = Vec::new();
        let mut stream = self.index.stream();
        let mut count = 0;

        while let Some((key, _)) = stream.next() {
            if count >= limit {
                break;
            }
            if let Ok(s) = std::str::from_utf8(key) {
                results.push(s.to_string());
                count += 1;
            }
        }

        results
    }

    /// Get the size of the index in bytes.
    pub fn size_bytes(&self) -> usize {
        self.index.as_fst().as_bytes().len()
    }
    
    // ========================================================================
    // Serialization / Deserialization
    // ========================================================================
    
    /// Magic bytes for identifying StringColumnIndex format.
    const MAGIC: &'static [u8; 4] = b"SCIX";
    /// Current format version.
    const VERSION: u8 = 1;
    
    /// Serialize the index to bytes for storage.
    /// 
    /// Format:
    /// - 4 bytes: magic ("SCIX")
    /// - 1 byte: version
    /// - 4 bytes: column name length (u32, little-endian)
    /// - N bytes: column name (UTF-8)
    /// - remaining bytes: FST data
    pub fn to_bytes(&self) -> Vec<u8> {
        let column_name_bytes = self.column_name.as_bytes();
        let fst_bytes = self.index.as_fst().as_bytes();
        
        let mut result = Vec::with_capacity(
            Self::MAGIC.len() + 1 + 4 + column_name_bytes.len() + fst_bytes.len()
        );
        
        // Write header
        result.extend_from_slice(Self::MAGIC);
        result.push(Self::VERSION);
        
        // Write column name length and data
        result.extend_from_slice(&(column_name_bytes.len() as u32).to_le_bytes());
        result.extend_from_slice(column_name_bytes);
        
        // Write FST data
        result.extend_from_slice(fst_bytes);
        
        result
    }
    
    /// Deserialize an index from bytes.
    /// 
    /// Returns an error if the data is invalid or corrupted.
    pub fn from_bytes(data: &[u8]) -> ColumnIndexResult<Self> {
        // Minimum size: magic (4) + version (1) + column name len (4) + at least empty FST
        if data.len() < 9 {
            return Err(ColumnIndexError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "Data too short for StringColumnIndex",
            )));
        }
        
        // Check magic
        if &data[0..4] != Self::MAGIC {
            return Err(ColumnIndexError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid magic bytes, expected {:?}", Self::MAGIC),
            )));
        }
        
        // Check version
        let version = data[4];
        if version != Self::VERSION {
            return Err(ColumnIndexError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unsupported version {}, expected {}", version, Self::VERSION),
            )));
        }
        
        // Read column name length
        let column_name_len = u32::from_le_bytes([data[5], data[6], data[7], data[8]]) as usize;
        
        // Validate we have enough data (use checked_add to prevent overflow)
        let header_size = 9usize.checked_add(column_name_len).ok_or_else(|| {
            ColumnIndexError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "Column name length overflow",
            ))
        })?;
        if data.len() < header_size {
            return Err(ColumnIndexError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "Data too short for column name",
            )));
        }
        
        // Read column name
        let column_name = std::str::from_utf8(&data[9..9 + column_name_len])
            .map_err(|e| ColumnIndexError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid UTF-8 in column name: {}", e),
            )))?
            .to_string();
        
        // Read FST data
        let fst_bytes = &data[header_size..];
        let index = Map::new(FstBacking::Owned(fst_bytes.to_vec()))?;
        
        Ok(Self {
            index,
            column_name,
        })
    }
    
    /// Deserialize an index from an mmap-backed buffer (zero-copy).
    ///
    /// The `data` slice must start at the beginning of the SCIX blob
    /// (including the magic/version/column-name header). The FST payload
    /// is referenced directly from the mmap without copying.
    pub fn from_mmap(
        mmap: Arc<memmap2::Mmap>,
        blob_offset: usize,
        blob_len: usize,
    ) -> ColumnIndexResult<Self> {
        let data = &mmap[blob_offset..blob_offset + blob_len];

        if data.len() < 9 {
            return Err(ColumnIndexError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "Data too short for StringColumnIndex",
            )));
        }
        if &data[0..4] != Self::MAGIC {
            return Err(ColumnIndexError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid magic bytes, expected {:?}", Self::MAGIC),
            )));
        }
        let version = data[4];
        if version != Self::VERSION {
            return Err(ColumnIndexError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unsupported version {}, expected {}", version, Self::VERSION),
            )));
        }

        let column_name_len = u32::from_le_bytes([data[5], data[6], data[7], data[8]]) as usize;
        let header_size = 9usize.checked_add(column_name_len).ok_or_else(|| {
            ColumnIndexError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "Column name length overflow",
            ))
        })?;
        if data.len() < header_size {
            return Err(ColumnIndexError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "Data too short for column name",
            )));
        }

        let column_name = std::str::from_utf8(&data[9..9 + column_name_len])
            .map_err(|e| ColumnIndexError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid UTF-8 in column name: {}", e),
            )))?
            .to_string();

        let fst_offset = blob_offset + header_size;
        let fst_len = blob_len - header_size;
        let backing = FstBacking::from_mmap(mmap, fst_offset, fst_len);
        let index = Map::new(backing)?;

        Ok(Self {
            index,
            column_name,
        })
    }
}

// Use shared utilities from the warehouse utils module
use crate::warehouse::utils::increment_last_byte;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_location_encoding() {
        let loc = FileLocation::new(42, 1000);
        let encoded = loc.encode();
        let decoded = FileLocation::decode(encoded);

        assert_eq!(loc, decoded);
    }

    #[test]
    fn test_build_and_lookup() {
        let data = vec![
            ("alice@example.com".to_string(), FileLocation::new(1, 0)),
            ("bob@example.com".to_string(), FileLocation::new(1, 1)),
            ("charlie@example.com".to_string(), FileLocation::new(1, 2)),
        ];

        let index = StringColumnIndex::build_unsorted("email", data).unwrap();

        // Exact lookup
        let loc = index.lookup("bob@example.com");
        assert!(loc.is_some());
        assert_eq!(loc.unwrap().row_offset, 1);

        // Non-existent key
        assert!(index.lookup("dave@example.com").is_none());
    }

    #[test]
    fn test_prefix_search() {
        let data = vec![
            ("alice@example.com".to_string(), FileLocation::new(1, 0)),
            ("bob@example.com".to_string(), FileLocation::new(1, 1)),
            ("bob@other.com".to_string(), FileLocation::new(1, 2)),
        ];

        let index = StringColumnIndex::build_unsorted("email", data).unwrap();

        // Prefix search
        let results = index.prefix_search("bob@");
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_cardinality() {
        let data = vec![
            ("a".to_string(), FileLocation::new(1, 0)),
            ("b".to_string(), FileLocation::new(1, 1)),
            ("c".to_string(), FileLocation::new(1, 2)),
        ];

        let index = StringColumnIndex::build_unsorted("value", data).unwrap();

        assert_eq!(index.estimated_cardinality(), 3);
        assert!(index.is_low_cardinality(100));
        assert!(!index.is_low_cardinality(2));
    }
    
    #[test]
    fn test_serialization_roundtrip() {
        let data = vec![
            ("alice@example.com".to_string(), FileLocation::new(1, 0)),
            ("bob@example.com".to_string(), FileLocation::new(1, 1)),
            ("charlie@example.com".to_string(), FileLocation::new(2, 100)),
        ];

        let original = StringColumnIndex::build_unsorted("email", data).unwrap();
        
        // Serialize
        let bytes = original.to_bytes();
        assert!(!bytes.is_empty());
        
        // Deserialize
        let restored = StringColumnIndex::from_bytes(&bytes).unwrap();
        
        // Verify
        assert_eq!(restored.column_name(), "email");
        assert_eq!(restored.len(), 3);
        
        // Check lookups work
        let loc = restored.lookup("bob@example.com");
        assert!(loc.is_some());
        assert_eq!(loc.unwrap().file_id, 1);
        assert_eq!(loc.unwrap().row_offset, 1);
        
        let loc2 = restored.lookup("charlie@example.com");
        assert!(loc2.is_some());
        assert_eq!(loc2.unwrap().file_id, 2);
        assert_eq!(loc2.unwrap().row_offset, 100);
    }
    
    #[test]
    fn test_from_bytes_invalid_magic() {
        let bad_data = b"XXXX\x01\x00\x00\x00\x04test";
        let result = StringColumnIndex::from_bytes(bad_data);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_from_bytes_too_short() {
        let result = StringColumnIndex::from_bytes(b"SCIX");
        assert!(result.is_err());
    }

    #[test]
    fn test_contains_substring_match() {
        let rows = vec![
            ("alice@example.com".to_string(), FileLocation::new(0, 0)),
            ("bob@example.com".to_string(), FileLocation::new(0, 1)),
        ];
        let index = StringColumnIndex::build("email", rows).unwrap();

        assert!(index.contains_substring("alice"), "alice is in alice@example.com");
        assert!(index.contains_substring("example"), "example is in both emails");
        assert!(!index.contains_substring("charlie"), "charlie is not in any email");
    }

    #[test]
    fn test_contains_substring_special_chars() {
        let rows = vec![
            ("a.b.c".to_string(), FileLocation::new(0, 0)),
        ];
        let index = StringColumnIndex::build("col", rows).unwrap();

        assert!(index.contains_substring("a.b"), "literal 'a.b' is in 'a.b.c'");
        assert!(!index.contains_substring("abc"), "'abc' is not a substring of 'a.b.c'");
    }

    #[test]
    fn test_prefix_search_empty_returns_all() {
        let data = vec![
            ("alice".to_string(), FileLocation::new(1, 0)),
            ("bob".to_string(), FileLocation::new(1, 1)),
            ("charlie".to_string(), FileLocation::new(1, 2)),
        ];

        let index = StringColumnIndex::build_unsorted("name", data).unwrap();
        let results = index.prefix_search("");
        assert_eq!(results.len(), 3,
            "Empty prefix should match all entries, got {}", results.len());
    }

    #[test]
    fn test_from_bytes_rejects_oversized_column_name_len() {
        let mut data = Vec::new();
        data.extend_from_slice(b"SCIX"); // correct magic
        data.push(1); // version
        data.extend_from_slice(&u32::MAX.to_le_bytes()); // column_name_len = u32::MAX

        let result = StringColumnIndex::from_bytes(&data);
        assert!(result.is_err(), "Should reject oversized column_name_len");
    }

    #[test]
    fn test_prefix_search_with_char_max_prefix() {
        let max_char = char::MAX;
        let key = format!("{}value", max_char);
        let data = vec![
            (key.clone(), FileLocation::new(1, 0)),
        ];

        let index = StringColumnIndex::build_unsorted("col", data).unwrap();
        let prefix = max_char.to_string();
        let results = index.prefix_search(&prefix);
        assert_eq!(
            results.len(), 1,
            "Prefix search with char::MAX prefix must still find matching entries"
        );
    }
}
