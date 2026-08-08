//! Inverted Index for Low-Cardinality Columns
//!
//! Maps column values to Roaring Bitmaps of primary keys that contain that value.
//! Only used for columns with low cardinality (<100K distinct values).
//!
//! # Hash Collision Handling
//!
//! For non-integer PKs (strings, composites), we hash them to u32 for the bitmap.
//! To handle potential collisions, we maintain a reverse mapping from hash to
//! original PK strings. This ensures accurate PK resolution.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use parking_lot::RwLock;
use roaring::RoaringBitmap;
use super::types::{ColumnValue, PrimaryKey};

/// Maximum cardinality for inverted index (columns with more distinct values use skip indexes).
pub const MAX_INVERTED_INDEX_CARDINALITY: usize = 100_000;

/// An inverted index entry for a single value.
///
/// For integer PKs that fit in u32, we store them directly in the bitmap.
/// For string/composite PKs, we hash them to u32 and maintain a reverse mapping
/// to handle collisions correctly.
#[derive(Debug, Clone)]
pub struct InvertedIndexEntry {
    /// Hash of the value (for storage efficiency)
    pub value_hash: u64,
    /// Bitmap of primary key IDs (or hashes for non-integer PKs)
    pub pk_bitmap: RoaringBitmap,
    /// Reverse mapping from u32 hash to original PK strings (for non-integer PKs)
    /// This handles collision detection - if two different PKs hash to the same u32,
    /// we can still track them correctly.
    pk_hash_to_original: HashMap<u32, HashSet<String>>,
    /// Whether this entry contains any non-integer PKs
    has_string_pks: bool,
    /// Tracks which bitmap entries came from actual integer PKs (vs. string PK hashes).
    /// Without this, an integer PK lookup could falsely match a string PK whose hash
    /// collides with that integer value.
    integer_pks: RoaringBitmap,
}

impl InvertedIndexEntry {
    /// Create a new entry for a value.
    pub fn new(value_hash: u64) -> Self {
        Self {
            value_hash,
            pk_bitmap: RoaringBitmap::new(),
            pk_hash_to_original: HashMap::new(),
            has_string_pks: false,
            integer_pks: RoaringBitmap::new(),
        }
    }

    /// Compute the u32 hash for a non-integer PK.
    fn pk_to_u32_hash(pk: &PrimaryKey) -> u32 {
        (pk.stable_hash() % u32::MAX as u64) as u32
    }

    /// Add a primary key to this entry.
    pub fn add_pk(&mut self, pk: &PrimaryKey) {
        if let Some(pk_u32) = pk.as_u32() {
            self.pk_bitmap.insert(pk_u32);
            self.integer_pks.insert(pk_u32);
        } else {
            // For non-integer PKs, compute hash and track the original
            let hash = Self::pk_to_u32_hash(pk);
            self.pk_bitmap.insert(hash);
            self.has_string_pks = true;
            
            // Store the original PK string for collision resolution
            self.pk_hash_to_original
                .entry(hash)
                .or_insert_with(HashSet::new)
                .insert(pk.to_string_repr());
        }
    }

    /// Remove a primary key from this entry.
    pub fn remove_pk(&mut self, pk: &PrimaryKey) {
        if let Some(pk_u32) = pk.as_u32() {
            self.pk_bitmap.remove(pk_u32);
            self.integer_pks.remove(pk_u32);
        } else {
            let hash = Self::pk_to_u32_hash(pk);
            let pk_str = pk.to_string_repr();
            
            // Remove from the reverse mapping
            if let Some(originals) = self.pk_hash_to_original.get_mut(&hash) {
                originals.remove(&pk_str);
                
                // Only remove from bitmap if no more PKs map to this hash
                if originals.is_empty() {
                    self.pk_hash_to_original.remove(&hash);
                    self.pk_bitmap.remove(hash);
                }
            }
        }
    }

    /// Check if this entry contains a primary key.
    pub fn contains_pk(&self, pk: &PrimaryKey) -> bool {
        if let Some(pk_u32) = pk.as_u32() {
            if self.has_string_pks {
                self.integer_pks.contains(pk_u32)
            } else {
                self.pk_bitmap.contains(pk_u32)
            }
        } else {
            let hash = Self::pk_to_u32_hash(pk);
            if !self.pk_bitmap.contains(hash) {
                return false;
            }
            
            // Check reverse mapping to handle collisions
            self.pk_hash_to_original
                .get(&hash)
                .map(|originals| originals.contains(&pk.to_string_repr()))
                .unwrap_or(false)
        }
    }

    /// Get the number of PKs in this entry.
    ///
    /// For entries with string PKs, this counts actual PKs (not just bitmap entries)
    /// to correctly handle hash collisions.
    pub fn len(&self) -> u64 {
        if self.has_string_pks {
            let string_pk_count: usize = self.pk_hash_to_original
                .values()
                .map(|s| s.len())
                .sum();
            string_pk_count as u64 + self.integer_pks.len()
        } else {
            self.pk_bitmap.len()
        }
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.pk_bitmap.is_empty()
    }

    /// Get all original PK strings for non-integer PKs.
    pub fn get_string_pks(&self) -> Vec<String> {
        self.pk_hash_to_original
            .values()
            .flat_map(|s| s.iter().cloned())
            .collect()
    }

    /// Check if this entry has string PKs.
    pub fn has_string_pks(&self) -> bool {
        self.has_string_pks
    }

    /// Get the hash-to-original-PK reverse mapping for string PKs.
    pub fn pk_hash_mappings(&self) -> &HashMap<u32, HashSet<String>> {
        &self.pk_hash_to_original
    }

    /// Serialize bitmap to bytes.
    pub fn bitmap_to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        self.pk_bitmap.serialize_into(&mut bytes).unwrap_or_default();
        bytes
    }

    /// Deserialize bitmap from bytes.
    pub fn bitmap_from_bytes(bytes: &[u8]) -> Option<RoaringBitmap> {
        RoaringBitmap::deserialize_from(bytes).ok()
    }

    /// Serialize bitmap to base64.
    pub fn bitmap_to_base64(&self) -> String {
        BASE64.encode(self.bitmap_to_bytes())
    }

    /// Deserialize bitmap from base64.
    pub fn bitmap_from_base64(s: &str) -> Option<RoaringBitmap> {
        BASE64
            .decode(s)
            .ok()
            .and_then(|b| Self::bitmap_from_bytes(&b))
    }

    /// Serialize the full entry (including reverse mapping) to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        // Format: [bitmap_len:u64][bitmap_bytes][has_string_pks:u8]
        //         [integer_pks_len:u64][integer_pks_bytes]
        //         [mapping_count:u64][mappings...]
        let mut bytes = Vec::new();
        
        // Serialize bitmap
        let bitmap_bytes = self.bitmap_to_bytes();
        bytes.extend_from_slice(&(bitmap_bytes.len() as u64).to_le_bytes());
        bytes.extend(bitmap_bytes);
        
        // Serialize has_string_pks flag
        bytes.push(if self.has_string_pks { 1 } else { 0 });

        // Serialize integer_pks bitmap
        let mut int_pk_bytes = Vec::new();
        self.integer_pks.serialize_into(&mut int_pk_bytes).unwrap_or_default();
        bytes.extend_from_slice(&(int_pk_bytes.len() as u64).to_le_bytes());
        bytes.extend(int_pk_bytes);
        
        // Serialize reverse mapping
        bytes.extend_from_slice(&(self.pk_hash_to_original.len() as u64).to_le_bytes());
        for (hash, originals) in &self.pk_hash_to_original {
            bytes.extend_from_slice(&hash.to_le_bytes());
            bytes.extend_from_slice(&(originals.len() as u64).to_le_bytes());
            for orig in originals {
                let orig_bytes = orig.as_bytes();
                bytes.extend_from_slice(&(orig_bytes.len() as u64).to_le_bytes());
                bytes.extend(orig_bytes);
            }
        }
        
        bytes
    }

    /// Deserialize full entry from bytes.
    pub fn from_bytes(value_hash: u64, bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 8 {
            return None;
        }
        
        let mut offset = 0;
        
        // Deserialize bitmap
        let bitmap_len = u64::from_le_bytes(bytes[offset..offset + 8].try_into().ok()?) as usize;
        offset += 8;
        
        if bytes.len() < offset + bitmap_len + 1 {
            return None;
        }
        
        let bitmap = Self::bitmap_from_bytes(&bytes[offset..offset + bitmap_len])?;
        offset += bitmap_len;
        
        // Deserialize has_string_pks flag
        let has_string_pks = bytes[offset] == 1;
        offset += 1;

        // Deserialize integer_pks bitmap
        if offset + 8 > bytes.len() {
            return None;
        }
        let int_pk_len = u64::from_le_bytes(bytes[offset..offset + 8].try_into().ok()?) as usize;
        offset += 8;

        if offset + int_pk_len > bytes.len() {
            return None;
        }
        let integer_pks = Self::bitmap_from_bytes(&bytes[offset..offset + int_pk_len])?;
        offset += int_pk_len;
        
        // Deserialize reverse mapping
        let mut pk_hash_to_original = HashMap::new();
        
        if offset + 8 <= bytes.len() {
            let mapping_count = u64::from_le_bytes(bytes[offset..offset + 8].try_into().ok()?) as usize;
            offset += 8;
            
            for _ in 0..mapping_count {
                if offset + 12 > bytes.len() {
                    break;
                }
                
                let hash = u32::from_le_bytes(bytes[offset..offset + 4].try_into().ok()?);
                offset += 4;
                
                let originals_count = u64::from_le_bytes(bytes[offset..offset + 8].try_into().ok()?) as usize;
                offset += 8;
                
                let mut originals = HashSet::new();
                for _ in 0..originals_count {
                    if offset + 8 > bytes.len() {
                        break;
                    }
                    
                    let orig_len = u64::from_le_bytes(bytes[offset..offset + 8].try_into().ok()?) as usize;
                    offset += 8;
                    
                    if offset + orig_len > bytes.len() {
                        break;
                    }
                    
                    if let Ok(orig) = String::from_utf8(bytes[offset..offset + orig_len].to_vec()) {
                        originals.insert(orig);
                    }
                    offset += orig_len;
                }
                
                pk_hash_to_original.insert(hash, originals);
            }
        }

        Some(Self {
            value_hash,
            pk_bitmap: bitmap,
            pk_hash_to_original,
            has_string_pks,
            integer_pks,
        })
    }

    /// Serialize full entry to base64.
    pub fn to_full_base64(&self) -> String {
        BASE64.encode(self.to_bytes())
    }

    /// Deserialize full entry from base64.
    pub fn from_full_base64(value_hash: u64, s: &str) -> Option<Self> {
        BASE64.decode(s).ok().and_then(|b| Self::from_bytes(value_hash, &b))
    }
}

/// Inverted index for a single column.
#[derive(Debug)]
pub struct InvertedIndex {
    /// Column name
    column_name: String,
    /// Value hash -> index entry
    entries: HashMap<u64, InvertedIndexEntry>,
    /// Distinct value count
    distinct_values: usize,
}

impl InvertedIndex {
    /// Create a new inverted index for a column.
    pub fn new(column_name: impl Into<String>) -> Self {
        Self {
            column_name: column_name.into(),
            entries: HashMap::new(),
            distinct_values: 0,
        }
    }

    /// Get the column name.
    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    /// Get the number of distinct values.
    pub fn distinct_values(&self) -> usize {
        self.distinct_values
    }

    /// Check if this index has too many distinct values.
    pub fn exceeds_cardinality_limit(&self) -> bool {
        self.distinct_values > MAX_INVERTED_INDEX_CARDINALITY
    }

    /// Add a value -> PK mapping.
    pub fn add(&mut self, value: &ColumnValue, pk: &PrimaryKey) {
        let hash = value.stable_hash();

        let entry = self.entries.entry(hash).or_insert_with(|| {
            self.distinct_values += 1;
            InvertedIndexEntry::new(hash)
        });

        entry.add_pk(pk);
    }

    /// Remove a value -> PK mapping.
    pub fn remove(&mut self, value: &ColumnValue, pk: &PrimaryKey) {
        let hash = value.stable_hash();

        if let Some(entry) = self.entries.get_mut(&hash) {
            entry.remove_pk(pk);
            if entry.is_empty() {
                self.entries.remove(&hash);
                self.distinct_values = self.distinct_values.saturating_sub(1);
            }
        }
    }

    /// Get the bitmap of PKs that have a specific value.
    pub fn get(&self, value: &ColumnValue) -> Option<&RoaringBitmap> {
        let hash = value.stable_hash();
        self.entries.get(&hash).map(|e| &e.pk_bitmap)
    }

    /// Get the bitmap of PKs that have a specific value hash.
    pub fn get_by_hash(&self, hash: u64) -> Option<&RoaringBitmap> {
        self.entries.get(&hash).map(|e| &e.pk_bitmap)
    }

    /// Iterate over all entries.
    pub fn iter(&self) -> impl Iterator<Item = (&u64, &InvertedIndexEntry)> {
        self.entries.iter()
    }

    /// Get total number of indexed PKs.
    pub fn total_pks(&self) -> u64 {
        self.entries.values().map(|e| e.len()).sum()
    }

    /// Merge another index into this one.
    pub fn merge(&mut self, other: InvertedIndex) {
        for (hash, entry) in other.entries {
            if let Some(existing) = self.entries.get_mut(&hash) {
                existing.pk_bitmap |= &entry.pk_bitmap;
                existing.integer_pks |= &entry.integer_pks;

                if entry.has_string_pks {
                    existing.has_string_pks = true;
                    for (pk_hash, originals) in entry.pk_hash_to_original {
                        existing
                            .pk_hash_to_original
                            .entry(pk_hash)
                            .or_insert_with(HashSet::new)
                            .extend(originals);
                    }
                }
            } else {
                self.distinct_values += 1;
                self.entries.insert(hash, entry);
            }
        }
    }

    /// Get all entries for serialization.
    pub fn entries(&self) -> &HashMap<u64, InvertedIndexEntry> {
        &self.entries
    }

    /// Load an entry from storage.
    pub fn load_entry(&mut self, value_hash: u64, bitmap: RoaringBitmap) {
        let mut entry = InvertedIndexEntry::new(value_hash);
        entry.pk_bitmap = bitmap;

        if !self.entries.contains_key(&value_hash) {
            self.distinct_values += 1;
        }
        self.entries.insert(value_hash, entry);
    }
}

/// Manager for inverted indexes across multiple columns.
#[derive(Debug)]
pub struct InvertedIndexManager {
    /// Source identifier
    source_id: String,
    /// Table name
    table_name: String,
    /// Column name -> inverted index
    indexes: Arc<RwLock<HashMap<String, InvertedIndex>>>,
    /// Columns that have exceeded cardinality limit (switch to skip index)
    /// Uses HashSet for O(1) contains() checks instead of Vec's O(n)
    high_cardinality_columns: Arc<RwLock<HashSet<String>>>,
}

impl InvertedIndexManager {
    /// Create a new manager.
    pub fn new(source_id: impl Into<String>, table_name: impl Into<String>) -> Self {
        Self {
            source_id: source_id.into(),
            table_name: table_name.into(),
            indexes: Arc::new(RwLock::new(HashMap::new())),
            high_cardinality_columns: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Get the source ID.
    pub fn source_id(&self) -> &str {
        &self.source_id
    }

    /// Get the table name.
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Check if a column is high cardinality (uses skip index instead).
    /// Uses O(1) HashSet lookup.
    pub fn is_high_cardinality(&self, column: &str) -> bool {
        self.high_cardinality_columns.read().contains(column)
    }

    /// Mark a column as high cardinality.
    pub fn mark_high_cardinality(&self, column: &str) {
        let mut hc = self.high_cardinality_columns.write();
        hc.insert(column.to_string());
        // Remove the inverted index for this column
        self.indexes.write().remove(column);
    }

    /// Add a value -> PK mapping for a column.
    ///
    /// Returns true if the column should continue using inverted index,
    /// false if it has exceeded cardinality limit.
    pub fn add(&self, column: &str, value: &ColumnValue, pk: &PrimaryKey) -> bool {
        if self.is_high_cardinality(column) {
            return false;
        }

        let mut indexes = self.indexes.write();
        let index = indexes
            .entry(column.to_string())
            .or_insert_with(|| InvertedIndex::new(column));

        index.add(value, pk);

        if index.exceeds_cardinality_limit() {
            drop(indexes);
            self.mark_high_cardinality(column);
            return false;
        }

        true
    }

    /// Remove a value -> PK mapping for a column.
    pub fn remove(&self, column: &str, value: &ColumnValue, pk: &PrimaryKey) {
        if self.is_high_cardinality(column) {
            return;
        }

        let mut indexes = self.indexes.write();
        if let Some(index) = indexes.get_mut(column) {
            index.remove(value, pk);
        }
    }

    /// Get the inverted index for a column.
    pub fn get_index(&self, column: &str) -> Option<InvertedIndex> {
        // This is a bit awkward but necessary to return owned data
        let indexes = self.indexes.read();
        indexes.get(column).map(|idx| {
            let mut new_idx = InvertedIndex::new(column);
            for (hash, entry) in idx.entries() {
                new_idx.load_entry(*hash, entry.pk_bitmap.clone());
            }
            new_idx
        })
    }

    /// Get PKs matching a value in a column.
    pub fn get_matching_pks(&self, column: &str, value: &ColumnValue) -> Option<RoaringBitmap> {
        let indexes = self.indexes.read();
        indexes.get(column).and_then(|idx| idx.get(value).cloned())
    }

    /// Get PKs matching a value, including reverse mapping for string PKs.
    ///
    /// Returns `(bitmap, has_string_pks, hash_to_original_mapping)`.
    pub fn get_matching_pks_with_strings(
        &self,
        column: &str,
        value: &ColumnValue,
    ) -> Option<(RoaringBitmap, bool, HashMap<u32, HashSet<String>>)> {
        let indexes = self.indexes.read();
        let idx = indexes.get(column)?;
        let hash = value.stable_hash();
        let entry = idx.entries().get(&hash)?;
        Some((
            entry.pk_bitmap.clone(),
            entry.has_string_pks(),
            entry.pk_hash_mappings().clone(),
        ))
    }

    /// Get all indexed columns.
    pub fn indexed_columns(&self) -> Vec<String> {
        self.indexes.read().keys().cloned().collect()
    }

    /// Get all column indexes for serialization.
    pub fn all_indexes(&self) -> HashMap<String, InvertedIndex> {
        let indexes = self.indexes.read();
        indexes
            .iter()
            .map(|(name, idx)| {
                let mut new_idx = InvertedIndex::new(name);
                for (hash, entry) in idx.entries() {
                    new_idx.load_entry(*hash, entry.pk_bitmap.clone());
                }
                (name.clone(), new_idx)
            })
            .collect()
    }

    /// Load indexes from storage.
    pub fn load_indexes(&self, indexes: HashMap<String, InvertedIndex>) {
        let mut current = self.indexes.write();
        for (name, index) in indexes {
            current.insert(name, index);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inverted_index_entry() {
        let mut entry = InvertedIndexEntry::new(123);

        entry.add_pk(&PrimaryKey::from_i64(1));
        entry.add_pk(&PrimaryKey::from_i64(5));
        entry.add_pk(&PrimaryKey::from_i64(10));

        assert_eq!(entry.len(), 3);
        assert!(entry.contains_pk(&PrimaryKey::from_i64(1)));
        assert!(entry.contains_pk(&PrimaryKey::from_i64(5)));
        assert!(!entry.contains_pk(&PrimaryKey::from_i64(2)));

        entry.remove_pk(&PrimaryKey::from_i64(5));
        assert_eq!(entry.len(), 2);
        assert!(!entry.contains_pk(&PrimaryKey::from_i64(5)));
    }

    #[test]
    fn test_bitmap_serialization() {
        let mut entry = InvertedIndexEntry::new(456);
        entry.add_pk(&PrimaryKey::from_i64(1));
        entry.add_pk(&PrimaryKey::from_i64(100));
        entry.add_pk(&PrimaryKey::from_i64(1000));

        let base64 = entry.bitmap_to_base64();
        let restored = InvertedIndexEntry::bitmap_from_base64(&base64).unwrap();

        assert!(restored.contains(1));
        assert!(restored.contains(100));
        assert!(restored.contains(1000));
        assert!(!restored.contains(500));
    }

    #[test]
    fn test_inverted_index() {
        let mut index = InvertedIndex::new("status");

        let pending = ColumnValue::String("pending".to_string());
        let complete = ColumnValue::String("complete".to_string());

        index.add(&pending, &PrimaryKey::from_i64(1));
        index.add(&pending, &PrimaryKey::from_i64(3));
        index.add(&complete, &PrimaryKey::from_i64(2));

        assert_eq!(index.distinct_values(), 2);

        let pending_pks = index.get(&pending).unwrap();
        assert!(pending_pks.contains(1));
        assert!(pending_pks.contains(3));
        assert!(!pending_pks.contains(2));

        let complete_pks = index.get(&complete).unwrap();
        assert!(complete_pks.contains(2));
        assert!(!complete_pks.contains(1));
    }

    #[test]
    fn test_inverted_index_manager() {
        let manager = InvertedIndexManager::new("source", "table");

        let status_pending = ColumnValue::String("pending".to_string());
        let status_complete = ColumnValue::String("complete".to_string());

        manager.add("status", &status_pending, &PrimaryKey::from_i64(1));
        manager.add("status", &status_pending, &PrimaryKey::from_i64(2));
        manager.add("status", &status_complete, &PrimaryKey::from_i64(3));

        let pks = manager.get_matching_pks("status", &status_pending).unwrap();
        assert!(pks.contains(1));
        assert!(pks.contains(2));
        assert!(!pks.contains(3));
    }

    /// Regression: integer PK lookup must not return true when only a
    /// string PK whose hash collides with that integer value was added.
    #[test]
    fn test_contains_pk_no_false_positive_from_hash_collision() {
        let mut entry = InvertedIndexEntry::new(1);

        // Compute the hash of a known string PK, then use that hash value
        // as an integer PK to test the collision path.
        let string_pk = PrimaryKey::from_string("collision_test_key");
        let hash = InvertedIndexEntry::pk_to_u32_hash(&string_pk);

        // Add only the string PK
        entry.add_pk(&string_pk);

        // The string PK itself should be found
        assert!(entry.contains_pk(&string_pk));

        // An integer PK whose value equals the string PK's hash must NOT
        // be reported as present — it was never added.
        let int_pk = PrimaryKey::from_i64(hash as i64);
        assert!(
            !entry.contains_pk(&int_pk),
            "Integer PK {} must not match a colliding string PK hash",
            hash,
        );
    }

    #[test]
    fn test_mixed_integer_and_string_pks() {
        let mut entry = InvertedIndexEntry::new(1);

        entry.add_pk(&PrimaryKey::from_i64(10));
        entry.add_pk(&PrimaryKey::from_string("hello"));
        entry.add_pk(&PrimaryKey::from_i64(20));

        assert_eq!(entry.len(), 3);
        assert!(entry.contains_pk(&PrimaryKey::from_i64(10)));
        assert!(entry.contains_pk(&PrimaryKey::from_i64(20)));
        assert!(entry.contains_pk(&PrimaryKey::from_string("hello")));
        assert!(!entry.contains_pk(&PrimaryKey::from_i64(99)));
        assert!(!entry.contains_pk(&PrimaryKey::from_string("world")));

        entry.remove_pk(&PrimaryKey::from_i64(10));
        assert!(!entry.contains_pk(&PrimaryKey::from_i64(10)));
        assert_eq!(entry.len(), 2);
    }

    #[test]
    fn test_merge_preserves_integer_pks() {
        let mut idx_a = InvertedIndex::new("status");
        let mut idx_b = InvertedIndex::new("status");

        let val = ColumnValue::String("active".to_string());
        idx_a.add(&val, &PrimaryKey::from_i64(1));
        idx_a.add(&val, &PrimaryKey::from_i64(2));
        idx_b.add(&val, &PrimaryKey::from_i64(3));
        idx_b.add(&val, &PrimaryKey::from_i64(4));

        idx_a.merge(idx_b);

        let hash = val.stable_hash();
        let entry = idx_a.entries().get(&hash).unwrap();
        for pk_val in [1, 2, 3, 4] {
            assert!(
                entry.contains_pk(&PrimaryKey::from_i64(pk_val)),
                "PK {} must be present after merge",
                pk_val,
            );
        }
        assert_eq!(entry.len(), 4);
    }

    #[test]
    fn test_merge_preserves_string_pks() {
        let mut idx_a = InvertedIndex::new("tag");
        let mut idx_b = InvertedIndex::new("tag");

        let val = ColumnValue::String("important".to_string());
        let pk_a = PrimaryKey::from_string("doc-aaa");
        let pk_b = PrimaryKey::from_string("doc-bbb");
        idx_a.add(&val, &pk_a);
        idx_b.add(&val, &pk_b);

        idx_a.merge(idx_b);

        let hash = val.stable_hash();
        let entry = idx_a.entries().get(&hash).unwrap();

        assert!(entry.contains_pk(&pk_a), "String PK from idx_a must survive merge");
        assert!(entry.contains_pk(&pk_b), "String PK from idx_b must survive merge");
        assert!(entry.has_string_pks());
        assert_eq!(entry.len(), 2);
    }

    #[test]
    fn test_merge_mixed_integer_and_string_pks_across_indexes() {
        let mut idx_a = InvertedIndex::new("col");
        let mut idx_b = InvertedIndex::new("col");

        let val = ColumnValue::String("val".to_string());
        idx_a.add(&val, &PrimaryKey::from_i64(10));
        idx_b.add(&val, &PrimaryKey::from_string("str-pk"));

        idx_a.merge(idx_b);

        let hash = val.stable_hash();
        let entry = idx_a.entries().get(&hash).unwrap();

        assert!(entry.contains_pk(&PrimaryKey::from_i64(10)));
        assert!(entry.contains_pk(&PrimaryKey::from_string("str-pk")));
        assert!(entry.has_string_pks());
        assert!(!entry.contains_pk(&PrimaryKey::from_i64(99)));
        assert!(!entry.contains_pk(&PrimaryKey::from_string("other")));
    }

    #[test]
    fn test_high_cardinality_detection() {
        let manager = InvertedIndexManager::new("source", "table");

        // Add more than MAX_INVERTED_INDEX_CARDINALITY distinct values
        for i in 0..(MAX_INVERTED_INDEX_CARDINALITY + 100) {
            let value = ColumnValue::String(format!("value_{}", i));
            let result = manager.add("high_card_col", &value, &PrimaryKey::from_i64(i as i64));
            
            if i > MAX_INVERTED_INDEX_CARDINALITY {
                // Should return false once limit exceeded
                assert!(!result || manager.is_high_cardinality("high_card_col"));
            }
        }

        assert!(manager.is_high_cardinality("high_card_col"));
    }

    #[test]
    fn test_roundtrip_preserves_integer_pks_with_colliding_hash() {
        let mut entry = InvertedIndexEntry::new(42);

        let string_pk = PrimaryKey::from_string("collision_key");
        let hash = InvertedIndexEntry::pk_to_u32_hash(&string_pk);

        // Add both a string PK and an integer PK whose value equals the string PK's hash
        entry.add_pk(&string_pk);
        entry.add_pk(&PrimaryKey::from_i64(hash as i64));

        // Verify both are present before serialization
        assert!(entry.contains_pk(&string_pk));
        assert!(entry.contains_pk(&PrimaryKey::from_i64(hash as i64)));

        // Roundtrip through bytes
        let serialized = entry.to_bytes();
        let restored = InvertedIndexEntry::from_bytes(42, &serialized)
            .expect("deserialization should succeed");

        assert!(
            restored.contains_pk(&string_pk),
            "String PK must survive roundtrip",
        );
        assert!(
            restored.contains_pk(&PrimaryKey::from_i64(hash as i64)),
            "Integer PK whose value equals a string PK hash must survive roundtrip",
        );
        assert_eq!(entry.len(), restored.len());
    }

    #[test]
    fn test_roundtrip_integer_only() {
        let mut entry = InvertedIndexEntry::new(99);
        entry.add_pk(&PrimaryKey::from_i64(1));
        entry.add_pk(&PrimaryKey::from_i64(100));
        entry.add_pk(&PrimaryKey::from_i64(50000));

        let serialized = entry.to_bytes();
        let restored = InvertedIndexEntry::from_bytes(99, &serialized)
            .expect("deserialization should succeed");

        assert!(restored.contains_pk(&PrimaryKey::from_i64(1)));
        assert!(restored.contains_pk(&PrimaryKey::from_i64(100)));
        assert!(restored.contains_pk(&PrimaryKey::from_i64(50000)));
        assert!(!restored.contains_pk(&PrimaryKey::from_i64(2)));
        assert_eq!(entry.len(), restored.len());
    }

    #[test]
    fn test_roundtrip_mixed_pks() {
        let mut entry = InvertedIndexEntry::new(7);
        entry.add_pk(&PrimaryKey::from_i64(10));
        entry.add_pk(&PrimaryKey::from_string("doc-abc"));
        entry.add_pk(&PrimaryKey::from_i64(20));
        entry.add_pk(&PrimaryKey::from_string("doc-xyz"));

        let serialized = entry.to_bytes();
        let restored = InvertedIndexEntry::from_bytes(7, &serialized)
            .expect("deserialization should succeed");

        assert!(restored.contains_pk(&PrimaryKey::from_i64(10)));
        assert!(restored.contains_pk(&PrimaryKey::from_i64(20)));
        assert!(restored.contains_pk(&PrimaryKey::from_string("doc-abc")));
        assert!(restored.contains_pk(&PrimaryKey::from_string("doc-xyz")));
        assert!(!restored.contains_pk(&PrimaryKey::from_i64(30)));
        assert!(!restored.contains_pk(&PrimaryKey::from_string("missing")));
        assert_eq!(entry.len(), restored.len());
    }
}
