//! Common types for WAL-based indexing.

use std::fmt;
use std::hash::{Hash, Hasher};

/// A primary key value from the source database.
///
/// Supports multiple PK types (integer, string, UUID) with efficient
/// comparison and hashing for bitmap operations.
///
/// # Ordering
///
/// The ordering is defined as:
/// - Int64 values are ordered numerically
/// - String values are ordered lexicographically
/// - Composite values are ordered element-by-element
/// - Different types are ordered by discriminant (Int64 < String < Composite)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PrimaryKey {
    /// Integer primary key (most common, efficient for bitmaps)
    Int64(i64),
    /// String primary key
    String(String),
    /// Composite primary key (multiple columns)
    Composite(Vec<PrimaryKey>),
}

impl PartialOrd for PrimaryKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PrimaryKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (self, other) {
            // Same type comparisons
            (Self::Int64(a), Self::Int64(b)) => a.cmp(b),
            (Self::String(a), Self::String(b)) => a.cmp(b),
            (Self::Composite(a), Self::Composite(b)) => a.cmp(b),
            
            // Cross-type comparisons (order by discriminant)
            (Self::Int64(_), _) => Ordering::Less,
            (_, Self::Int64(_)) => Ordering::Greater,
            (Self::String(_), Self::Composite(_)) => Ordering::Less,
            (Self::Composite(_), Self::String(_)) => Ordering::Greater,
        }
    }
}

impl PrimaryKey {
    /// Create from an i64 value.
    pub fn from_i64(v: i64) -> Self {
        Self::Int64(v)
    }

    /// Create from a string value.
    pub fn from_string(v: impl Into<String>) -> Self {
        Self::String(v.into())
    }

    /// Try to extract as i64 (for bitmap operations).
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int64(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to extract as u32 (for Roaring bitmap).
    /// Returns None if value is negative or too large.
    pub fn as_u32(&self) -> Option<u32> {
        match self {
            Self::Int64(v) if *v >= 0 && *v <= u32::MAX as i64 => Some(*v as u32),
            _ => None,
        }
    }

    /// Convert to string representation for storage/display.
    pub fn to_string_repr(&self) -> String {
        match self {
            Self::Int64(v) => v.to_string(),
            Self::String(s) => s.clone(),
            Self::Composite(parts) => parts
                .iter()
                .map(|p| p.to_string_repr())
                .collect::<Vec<_>>()
                .join("|"),
        }
    }

    /// Parse from string representation.
    pub fn parse(s: &str, is_numeric: bool) -> Self {
        if is_numeric {
            s.parse::<i64>()
                .map(Self::Int64)
                .unwrap_or_else(|_| Self::String(s.to_string()))
        } else {
            Self::String(s.to_string())
        }
    }

    /// Compute a stable hash for this primary key.
    pub fn stable_hash(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl fmt::Display for PrimaryKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string_repr())
    }
}

impl From<i64> for PrimaryKey {
    fn from(v: i64) -> Self {
        Self::Int64(v)
    }
}

impl From<i32> for PrimaryKey {
    fn from(v: i32) -> Self {
        Self::Int64(v as i64)
    }
}

impl From<String> for PrimaryKey {
    fn from(v: String) -> Self {
        Self::String(v)
    }
}

impl From<&str> for PrimaryKey {
    fn from(v: &str) -> Self {
        Self::String(v.to_string())
    }
}

/// A column value from a WAL event.
#[derive(Debug, Clone)]
pub enum ColumnValue {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
    Timestamp(i64), // Milliseconds since epoch
}

impl ColumnValue {
    /// Check if this is a null value.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Try to extract as f64 (for numeric comparisons).
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Int64(v) => Some(*v as f64),
            Self::Float64(v) => Some(*v),
            Self::Timestamp(v) => Some(*v as f64),
            _ => None,
        }
    }

    /// Compute a stable hash for this value.
    pub fn stable_hash(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        match self {
            Self::Null => 0u8.hash(&mut hasher),
            Self::Bool(v) => {
                1u8.hash(&mut hasher);
                v.hash(&mut hasher);
            }
            Self::Int64(v) => {
                2u8.hash(&mut hasher);
                v.hash(&mut hasher);
            }
            Self::Float64(v) => {
                3u8.hash(&mut hasher);
                v.to_bits().hash(&mut hasher);
            }
            Self::String(v) => {
                4u8.hash(&mut hasher);
                v.hash(&mut hasher);
            }
            Self::Bytes(v) => {
                5u8.hash(&mut hasher);
                v.hash(&mut hasher);
            }
            Self::Timestamp(v) => {
                6u8.hash(&mut hasher);
                v.hash(&mut hasher);
            }
        }
        hasher.finish()
    }

    /// Convert to string representation for display.
    pub fn to_string_repr(&self) -> String {
        match self {
            Self::Null => "NULL".to_string(),
            Self::Bool(v) => v.to_string(),
            Self::Int64(v) => v.to_string(),
            Self::Float64(v) => v.to_string(),
            Self::String(v) => v.clone(),
            Self::Bytes(v) => format!("<{} bytes>", v.len()),
            Self::Timestamp(v) => v.to_string(),
        }
    }
}

impl PartialEq for ColumnValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::Int64(a), Self::Int64(b)) => a == b,
            (Self::Float64(a), Self::Float64(b)) => a.to_bits() == b.to_bits(),
            (Self::String(a), Self::String(b)) => a == b,
            (Self::Bytes(a), Self::Bytes(b)) => a == b,
            (Self::Timestamp(a), Self::Timestamp(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for ColumnValue {}

impl Hash for ColumnValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        match self {
            Self::Null => {}
            Self::Bool(v) => v.hash(state),
            Self::Int64(v) => v.hash(state),
            Self::Float64(v) => v.to_bits().hash(state),
            Self::String(v) => v.hash(state),
            Self::Bytes(v) => v.hash(state),
            Self::Timestamp(v) => v.hash(state),
        }
    }
}

/// Type of WAL event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalEventType {
    Insert,
    Update,
    Delete,
}

/// A WAL event representing a change to a row.
///
/// For update events, we optionally store old column values to enable
/// proper index removal. When old values change, we need to remove the PK
/// from the old value's inverted index entry and add it to the new value's entry.
#[derive(Debug, Clone)]
pub struct WalEvent {
    /// Type of change
    pub event_type: WalEventType,
    /// Primary key of the affected row
    pub primary_key: PrimaryKey,
    /// New column values (empty for deletes)
    pub columns: Vec<(String, ColumnValue)>,
    /// Old column values before the update (only for updates, None if not available)
    /// This is needed to properly remove PKs from inverted indexes when values change.
    pub old_columns: Option<Vec<(String, ColumnValue)>>,
    /// LSN or resume token for checkpointing
    pub checkpoint: Vec<u8>,
}

impl WalEvent {
    /// Create an insert event.
    pub fn insert(pk: impl Into<PrimaryKey>, columns: Vec<(String, ColumnValue)>, checkpoint: Vec<u8>) -> Self {
        Self {
            event_type: WalEventType::Insert,
            primary_key: pk.into(),
            columns,
            old_columns: None,
            checkpoint,
        }
    }

    /// Create an update event with only new values (old values not available).
    ///
    /// Note: This may lead to stale inverted index entries if values change.
    /// Use `update_with_old_values` when old values are available for proper index maintenance.
    pub fn update(pk: impl Into<PrimaryKey>, columns: Vec<(String, ColumnValue)>, checkpoint: Vec<u8>) -> Self {
        Self {
            event_type: WalEventType::Update,
            primary_key: pk.into(),
            columns,
            old_columns: None,
            checkpoint,
        }
    }

    /// Create an update event with both old and new values.
    ///
    /// This enables proper index maintenance by removing PKs from old value's
    /// inverted index entries and adding them to new value's entries.
    pub fn update_with_old_values(
        pk: impl Into<PrimaryKey>,
        old_columns: Vec<(String, ColumnValue)>,
        new_columns: Vec<(String, ColumnValue)>,
        checkpoint: Vec<u8>,
    ) -> Self {
        Self {
            event_type: WalEventType::Update,
            primary_key: pk.into(),
            columns: new_columns,
            old_columns: Some(old_columns),
            checkpoint,
        }
    }

    /// Create a delete event.
    pub fn delete(pk: impl Into<PrimaryKey>, checkpoint: Vec<u8>) -> Self {
        Self {
            event_type: WalEventType::Delete,
            primary_key: pk.into(),
            columns: Vec::new(),
            old_columns: None,
            checkpoint,
        }
    }

    /// Get a column value by name (new value).
    pub fn get_column(&self, name: &str) -> Option<&ColumnValue> {
        self.columns.iter().find(|(n, _)| n == name).map(|(_, v)| v)
    }

    /// Get an old column value by name (only available for updates with old values).
    pub fn get_old_column(&self, name: &str) -> Option<&ColumnValue> {
        self.old_columns
            .as_ref()
            .and_then(|cols| cols.iter().find(|(n, _)| n == name).map(|(_, v)| v))
    }

    /// Check if this update has old values available.
    pub fn has_old_values(&self) -> bool {
        self.old_columns.is_some()
    }

    /// Get columns that changed (have different old and new values).
    pub fn changed_columns(&self) -> Vec<&str> {
        let Some(old_cols) = &self.old_columns else {
            return Vec::new();
        };
        
        self.columns
            .iter()
            .filter(|(name, new_val)| {
                old_cols.iter()
                    .find(|(n, _)| n == name)
                    .map(|(_, old_val)| old_val != new_val)
                    .unwrap_or(true) // New column, counts as changed
            })
            .map(|(name, _)| name.as_str())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_primary_key_from_i64() {
        let pk = PrimaryKey::from_i64(42);
        assert_eq!(pk.as_i64(), Some(42));
        assert_eq!(pk.as_u32(), Some(42));
        assert_eq!(pk.to_string_repr(), "42");
    }

    #[test]
    fn test_primary_key_from_string() {
        let pk = PrimaryKey::from_string("abc-123");
        assert_eq!(pk.as_i64(), None);
        assert_eq!(pk.to_string_repr(), "abc-123");
    }

    #[test]
    fn test_column_value_hash() {
        let v1 = ColumnValue::String("test".to_string());
        let v2 = ColumnValue::String("test".to_string());
        assert_eq!(v1.stable_hash(), v2.stable_hash());

        let v3 = ColumnValue::Int64(42);
        assert_ne!(v1.stable_hash(), v3.stable_hash());
    }

    #[test]
    fn test_wal_event_creation() {
        let event = WalEvent::insert(
            42i64,
            vec![
                ("status".to_string(), ColumnValue::String("pending".to_string())),
                ("amount".to_string(), ColumnValue::Float64(1500.0)),
            ],
            vec![1, 2, 3],
        );

        assert_eq!(event.event_type, WalEventType::Insert);
        assert_eq!(event.primary_key.as_i64(), Some(42));
        assert!(event.get_column("status").is_some());
        assert!(event.get_column("missing").is_none());
    }
}
