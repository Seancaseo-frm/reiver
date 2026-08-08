//! Daily Event Aggregates
//!
//! Pre-aggregated event counts by day and event type.
//! This dramatically speeds up trend queries that count events over time.
//!
//! # Example
//!
//! Instead of:
//! ```sql
//! SELECT date_trunc('day', timestamp) as day, COUNT(*) 
//! FROM events 
//! WHERE event_type = 'purchase' 
//! GROUP BY day
//! ```
//!
//! We can query the pre-computed aggregate table directly.

use ahash::AHashMap;
use chrono::{DateTime, NaiveDate, Utc};
use compact_str::CompactString;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use uuid::Uuid;

/// Pre-aggregated daily event counts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyEventAggregate {
    /// Project ID.
    pub project_id: Uuid,
    /// Event type (e.g., "purchase", "page_view").
    pub event_type: CompactString,
    /// Date of aggregation.
    pub date: NaiveDate,
    /// Total event count for this day.
    pub count: u64,
    /// Unique users who triggered this event.
    pub unique_users: u64,
    /// When this aggregate was last updated.
    pub updated_at: DateTime<Utc>,
}

impl DailyEventAggregate {
    /// Create a new aggregate entry.
    pub fn new(project_id: Uuid, event_type: &str, date: NaiveDate) -> Self {
        Self {
            project_id,
            event_type: CompactString::from(event_type),
            date,
            count: 0,
            unique_users: 0,
            updated_at: Utc::now(),
        }
    }

    /// Increment the count.
    pub fn increment(&mut self, count: u64) {
        self.count += count;
        self.updated_at = Utc::now();
    }

    /// Set unique users (typically from HyperLogLog).
    pub fn set_unique_users(&mut self, unique_users: u64) {
        self.unique_users = unique_users;
        self.updated_at = Utc::now();
    }
}

/// Validate a table name to prevent SQL/DDL injection.
///
/// Valid table names contain only:
/// - Alphanumeric characters (a-z, A-Z, 0-9)
/// - Underscores (_)
/// - Dots (.) for database.table notation
/// - Must start with a letter or underscore
/// - Maximum 128 characters
fn is_valid_table_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 128 {
        return false;
    }
    
    let mut chars = name.chars();
    
    // First character must be letter or underscore
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    
    // Remaining characters must be alphanumeric, underscore, or dot
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
}

/// Manager for daily event aggregates.
/// 
/// Handles incremental updates and querying of pre-computed aggregates.
pub struct AggregateManager {
    /// In-memory buffer before flushing to ClickHouse.
    buffer: AHashMap<AggregateKey, DailyEventAggregate>,
    /// Maximum buffer size before flush.
    max_buffer_size: usize,
}

/// Key for aggregate lookup.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct AggregateKey {
    project_id: Uuid,
    event_type: CompactString,
    date: NaiveDate,
}

impl AggregateManager {
    /// Create a new aggregate manager.
    pub fn new(max_buffer_size: usize) -> Self {
        Self {
            buffer: AHashMap::new(),
            max_buffer_size,
        }
    }

    /// Create with default buffer size (10,000 entries).
    pub fn with_default_buffer() -> Self {
        Self::new(10_000)
    }

    /// Record an event for aggregation.
    pub fn record_event(&mut self, project_id: Uuid, event_type: &str, timestamp: DateTime<Utc>) {
        let date = timestamp.date_naive();
        let key = AggregateKey {
            project_id,
            event_type: CompactString::from(event_type),
            date,
        };

        let agg = self
            .buffer
            .entry(key)
            .or_insert_with(|| DailyEventAggregate::new(project_id, event_type, date));

        agg.increment(1);
    }

    /// Record a batch of events.
    pub fn record_batch(&mut self, project_id: Uuid, events: &[(String, DateTime<Utc>)]) {
        for (event_type, timestamp) in events {
            self.record_event(project_id, event_type, *timestamp);
        }
    }

    /// Check if buffer should be flushed.
    pub fn should_flush(&self) -> bool {
        self.buffer.len() >= self.max_buffer_size
    }

    /// Get all buffered aggregates and clear the buffer.
    pub fn drain(&mut self) -> Vec<DailyEventAggregate> {
        self.buffer.drain().map(|(_, v)| v).collect()
    }

    /// Get the current buffer size.
    pub fn buffer_size(&self) -> usize {
        self.buffer.len()
    }

    /// Generate the ClickHouse DDL for the aggregates table.
    ///
    /// # Panics
    ///
    /// Panics if the table name is invalid (contains non-alphanumeric characters).
    /// This is a programming error - table names should be validated at configuration time.
    pub fn clickhouse_ddl(table_name: &str) -> String {
        if !is_valid_table_name(table_name) {
            panic!(
                "Invalid table name '{}': must contain only letters, numbers, underscores, and dots",
                table_name
            );
        }

        format!(
            r#"
CREATE TABLE IF NOT EXISTS {} (
    project_id UUID,
    event_type String,
    date Date,
    count UInt64,
    unique_users UInt64,
    updated_at DateTime64(6)
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (project_id, event_type, date)
"#,
            table_name
        )
    }
}

/// Query builder for aggregate queries.
pub struct AggregateQuery {
    project_id: Uuid,
    event_types: SmallVec<[CompactString; 4]>,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
}

impl AggregateQuery {
    /// Create a new query builder.
    pub fn new(project_id: Uuid) -> Self {
        Self {
            project_id,
            event_types: SmallVec::new(),
            start_date: None,
            end_date: None,
        }
    }

    /// Filter by event type.
    pub fn event_type(mut self, event_type: &str) -> Self {
        self.event_types.push(CompactString::from(event_type));
        self
    }

    /// Filter by event types.
    pub fn event_types(mut self, event_types: &[&str]) -> Self {
        self.event_types.extend(event_types.iter().map(|s| CompactString::from(*s)));
        self
    }

    /// Set start date.
    pub fn from_date(mut self, date: NaiveDate) -> Self {
        self.start_date = Some(date);
        self
    }

    /// Set end date.
    pub fn to_date(mut self, date: NaiveDate) -> Self {
        self.end_date = Some(date);
        self
    }

    /// Build the ClickHouse SQL query.
    ///
    /// # Security
    ///
    /// - Table name is validated to prevent SQL injection
    /// - Event type values are escaped to prevent SQL injection
    ///
    /// # Panics
    ///
    /// Panics if the table name is invalid.
    pub fn build_sql(&self, table_name: &str) -> String {
        if !is_valid_table_name(table_name) {
            panic!(
                "Invalid table name '{}': must contain only letters, numbers, underscores, and dots",
                table_name
            );
        }

        let mut conditions: SmallVec<[String; 4]> = SmallVec::new();
        conditions.push(format!("project_id = '{}'", self.project_id));

        if !self.event_types.is_empty() {
            let types: Vec<String> = self.event_types
                .iter()
                .map(|t| format!("'{}'", escape_sql_string(t)))
                .collect();
            conditions.push(format!("event_type IN ({})", types.join(", ")));
        }

        if let Some(start) = self.start_date {
            conditions.push(format!("date >= '{}'", start));
        }

        if let Some(end) = self.end_date {
            conditions.push(format!("date <= '{}'", end));
        }

        format!(
            "SELECT date, event_type, sum(count) as count, sum(unique_users) as unique_users \
             FROM {} \
             WHERE {} \
             GROUP BY date, event_type \
             ORDER BY date",
            table_name,
            conditions.join(" AND ")
        )
    }
}

/// Escape special characters in SQL strings for ClickHouse.
///
/// This function escapes:
/// - Single quotes (') -> ('')
/// - Backslashes (\) -> (\\)
/// - Null bytes are removed to prevent truncation attacks
fn escape_sql_string(s: &str) -> String {
    // Remove null bytes to prevent truncation attacks
    let s = s.replace('\0', "");
    
    let mut result = String::with_capacity(s.len() + s.len() / 8);
    for ch in s.chars() {
        match ch {
            '\'' => result.push_str("''"),
            '\\' => result.push_str("\\\\"),
            _ => result.push(ch),
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregate_manager_record() {
        let mut manager = AggregateManager::new(100);
        let project_id = Uuid::new_v4();
        let now = Utc::now();

        manager.record_event(project_id, "purchase", now);
        manager.record_event(project_id, "purchase", now);
        manager.record_event(project_id, "page_view", now);

        assert_eq!(manager.buffer_size(), 2); // 2 different event types

        let aggregates = manager.drain();
        assert_eq!(aggregates.len(), 2);

        let purchase = aggregates.iter().find(|a| a.event_type == "purchase").unwrap();
        assert_eq!(purchase.count, 2);
    }

    #[test]
    fn test_aggregate_query_builder() {
        let project_id = Uuid::new_v4();
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 1, 31).unwrap();

        let query = AggregateQuery::new(project_id)
            .event_type("purchase")
            .from_date(start)
            .to_date(end);

        let sql = query.build_sql("daily_aggregates");
        assert!(sql.contains("purchase"));
        assert!(sql.contains("2024-01-01"));
        assert!(sql.contains("2024-01-31"));
    }

    #[test]
    fn test_should_flush() {
        let mut manager = AggregateManager::new(3);
        let project_id = Uuid::new_v4();
        let now = Utc::now();

        manager.record_event(project_id, "event1", now);
        manager.record_event(project_id, "event2", now);
        assert!(!manager.should_flush());

        manager.record_event(project_id, "event3", now);
        assert!(manager.should_flush());
    }
}
