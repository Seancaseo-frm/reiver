//! Session Stitching
//!
//! Pre-compute session boundaries for faster session replay and analysis.
//!
//! # Session Definition
//!
//! A session is a sequence of events from a single user, bounded by:
//! - **Inactivity timeout**: No events for N minutes (default: 30 min)
//! - **Maximum length**: Session can't exceed N hours (default: 24 hours)
//! - **Reset events**: Specific events that always start a new session (e.g., login)
//!
//! # Benefits
//!
//! - **Session replay**: Instantly find all events for a session by session_id
//! - **Session analytics**: Count sessions, avg session length, etc. without computing on query
//! - **User journey**: Track user progression across sessions

use chrono::{DateTime, Duration, Utc};
use lru::LruCache;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use uuid::Uuid;

/// Default maximum number of open sessions to track per stitcher.
/// Users beyond this limit will have their oldest open session evicted.
const DEFAULT_MAX_OPEN_SESSIONS: usize = 100_000;

/// Session stitching configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionConfig {
    /// Inactivity timeout to end a session (default: 30 minutes).
    pub timeout: Duration,
    /// Maximum session length (default: 24 hours).
    pub max_length: Duration,
    /// Events that force a new session (e.g., "login", "signup").
    pub session_reset_events: Vec<String>,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::minutes(30),
            max_length: Duration::hours(24),
            session_reset_events: vec![],
        }
    }
}

impl SessionConfig {
    /// Create a new session config.
    pub fn new(timeout_minutes: i64, max_hours: i64) -> Self {
        Self {
            timeout: Duration::minutes(timeout_minutes),
            max_length: Duration::hours(max_hours),
            session_reset_events: vec![],
        }
    }

    /// Add reset events.
    pub fn with_reset_events(mut self, events: Vec<String>) -> Self {
        self.session_reset_events = events;
        self
    }
}

/// Pre-computed session metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionMetadata {
    /// Unique session ID.
    pub session_id: Uuid,
    /// User ID.
    pub user_id: String,
    /// Session start time.
    pub started_at: DateTime<Utc>,
    /// Session end time (last event + timeout or explicit end).
    pub ended_at: DateTime<Utc>,
    /// Number of events in the session.
    pub event_count: u32,
    /// Duration of the session in seconds.
    pub duration_seconds: i64,
    /// Whether the session has screen recording data.
    pub has_recording: bool,
    /// First page/screen viewed.
    pub entry_page: Option<String>,
    /// Last page/screen viewed.
    pub exit_page: Option<String>,
    /// Device type (if known).
    pub device_type: Option<String>,
}

impl SessionMetadata {
    /// Create a new session.
    pub fn new(session_id: Uuid, user_id: &str, started_at: DateTime<Utc>) -> Self {
        Self {
            session_id,
            user_id: user_id.to_string(),
            started_at,
            ended_at: started_at,
            event_count: 0,
            duration_seconds: 0,
            has_recording: false,
            entry_page: None,
            exit_page: None,
            device_type: None,
        }
    }

    /// Update session with a new event.
    pub fn add_event(&mut self, event_time: DateTime<Utc>, page: Option<&str>) {
        self.event_count += 1;
        
        if event_time > self.ended_at {
            self.ended_at = event_time;
            self.duration_seconds = self.ended_at.signed_duration_since(self.started_at).num_seconds();
        }

        if self.entry_page.is_none() {
            self.entry_page = page.map(|s| s.to_string());
        }
        self.exit_page = page.map(|s| s.to_string());
    }

    /// Mark session as having recording data.
    pub fn mark_has_recording(&mut self) {
        self.has_recording = true;
    }

    /// Set device type.
    pub fn set_device_type(&mut self, device: &str) {
        self.device_type = Some(device.to_string());
    }
}

/// Session index for fast lookups.
///
/// # Session Ordering
///
/// Sessions are stored in the order they are added via `add_session()`.
/// The `user_sessions` map maintains session IDs in insertion order, which
/// is assumed to be chronological order.
///
/// **IMPORTANT**: To maintain correct time ordering in `get_user_sessions()`,
/// sessions must be added in chronological order (oldest first). If sessions
/// are added out of order, the returned list will not be sorted by time.
#[derive(Debug, Default)]
pub struct SessionIndex {
    /// Session ID -> session metadata.
    sessions: HashMap<Uuid, SessionMetadata>,
    /// User ID -> list of session IDs in insertion order.
    /// Maintains chronological order only if sessions are added in time order.
    user_sessions: HashMap<String, Vec<Uuid>>,
}

impl SessionIndex {
    /// Create a new session index.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a session to the index.
    ///
    /// **IMPORTANT**: Sessions should be added in chronological order (oldest first)
    /// to maintain correct time ordering in `get_user_sessions()`. Adding sessions
    /// out of order will result in unsorted session lists.
    pub fn add_session(&mut self, session: SessionMetadata) {
        let session_id = session.session_id;
        let user_id = session.user_id.clone();

        self.sessions.insert(session_id, session);
        self.user_sessions
            .entry(user_id)
            .or_default()
            .push(session_id);
    }

    /// Get a session by ID.
    pub fn get_session(&self, session_id: Uuid) -> Option<&SessionMetadata> {
        self.sessions.get(&session_id)
    }

    /// Get all sessions for a user.
    pub fn get_user_sessions(&self, user_id: &str) -> Vec<&SessionMetadata> {
        self.user_sessions
            .get(user_id)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.sessions.get(id))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get session count.
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Get user count.
    pub fn user_count(&self) -> usize {
        self.user_sessions.len()
    }
}

/// Session stitcher that processes events and assigns session IDs.
///
/// Uses an LRU cache for open sessions to prevent unbounded memory growth.
/// When the cache is full, the least recently accessed session is evicted.
pub struct SessionStitcher {
    config: SessionConfig,
    /// Current open sessions: user_id -> (session_id, last_event_time, session_start)
    /// Uses LRU cache to prevent unbounded memory growth.
    open_sessions: LruCache<String, (Uuid, DateTime<Utc>, DateTime<Utc>)>,
    /// Completed sessions
    completed_sessions: Vec<SessionMetadata>,
    /// Maximum number of open sessions to track.
    max_open_sessions: usize,
}

impl SessionStitcher {
    /// Create a new session stitcher with custom capacity.
    ///
    /// # Arguments
    /// * `config` - Session configuration
    /// * `max_open_sessions` - Maximum number of open sessions to track (LRU eviction when exceeded)
    pub fn new_with_capacity(config: SessionConfig, max_open_sessions: usize) -> Self {
        let capacity = NonZeroUsize::new(max_open_sessions).unwrap_or(NonZeroUsize::new(1).unwrap());
        Self {
            config,
            open_sessions: LruCache::new(capacity),
            completed_sessions: Vec::new(),
            max_open_sessions,
        }
    }

    /// Create a new session stitcher with default capacity (100,000 open sessions).
    pub fn new(config: SessionConfig) -> Self {
        Self::new_with_capacity(config, DEFAULT_MAX_OPEN_SESSIONS)
    }

    /// Create with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(SessionConfig::default())
    }
    
    /// Get the maximum open sessions capacity.
    pub fn max_open_sessions(&self) -> usize {
        self.max_open_sessions
    }

    /// Process an event and return the session ID.
    ///
    /// **IMPORTANT**: Events MUST be processed in chronological order per user.
    /// Out-of-order events may result in incorrect session boundaries.
    ///
    /// # Arguments
    /// * `user_id` - The user identifier
    /// * `event_type` - The type of event (used for session reset detection)
    /// * `event_time` - When the event occurred
    /// * `page` - Optional page/screen associated with the event
    ///
    /// # Returns
    /// The session ID for this event (new or existing session)
    pub fn process_event(
        &mut self,
        user_id: &str,
        event_type: &str,
        event_time: DateTime<Utc>,
        _page: Option<&str>,
    ) -> Uuid {
        // Allocate the user_id string once and reuse it
        let user_id_owned = user_id.to_string();
        
        // Check if this event is a reset event
        let is_reset = self.config.session_reset_events.iter()
            .any(|e| e == event_type);

        // Check if we have an open session for this user (using LruCache::get to update access order)
        if let Some(&(session_id, last_event_time, session_start)) = self.open_sessions.get(&user_id_owned) {
            let time_since_last = event_time.signed_duration_since(last_event_time);
            let session_duration = event_time.signed_duration_since(session_start);

            // Check if we should start a new session
            let should_start_new = is_reset
                || time_since_last > self.config.timeout
                || session_duration > self.config.max_length;

            if should_start_new {
                // Close the current session
                let mut closed_session = SessionMetadata::new(session_id, user_id, session_start);
                closed_session.ended_at = last_event_time;
                closed_session.duration_seconds = last_event_time.signed_duration_since(session_start).num_seconds();
                self.completed_sessions.push(closed_session);

                // Start a new session - use push() to capture any evicted entry
                let new_session_id = Uuid::new_v4();
                self.handle_session_insert(user_id_owned, (new_session_id, event_time, event_time));
                new_session_id
            } else {
                // Continue current session - use push() to capture any evicted entry
                self.handle_session_insert(user_id_owned, (session_id, event_time, session_start));
                session_id
            }
        } else {
            // Start a new session - use push() to capture any evicted entry
            let session_id = Uuid::new_v4();
            self.handle_session_insert(user_id_owned, (session_id, event_time, event_time));
            session_id
        }
    }

    /// Insert a session into the LRU cache, completing any evicted session.
    fn handle_session_insert(
        &mut self,
        user_id: String,
        value: (Uuid, DateTime<Utc>, DateTime<Utc>),
    ) {
        // Use push() which returns the evicted entry if capacity is exceeded
        if let Some((evicted_user, (evicted_session_id, evicted_last_time, evicted_start))) =
            self.open_sessions.push(user_id, value)
        {
            // Complete the evicted session so we don't lose data
            let mut evicted_session = SessionMetadata::new(evicted_session_id, &evicted_user, evicted_start);
            evicted_session.ended_at = evicted_last_time;
            evicted_session.duration_seconds = evicted_last_time.signed_duration_since(evicted_start).num_seconds();
            self.completed_sessions.push(evicted_session);
        }
    }

    /// Close all open sessions (e.g., at end of processing).
    pub fn close_all_sessions(&mut self, _end_time: DateTime<Utc>) {
        // Collect all entries first (LruCache doesn't have drain)
        let entries: Vec<_> = self.open_sessions.iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        
        for (user_id, (session_id, last_event_time, session_start)) in entries {
            let mut session = SessionMetadata::new(session_id, &user_id, session_start);
            session.ended_at = last_event_time;
            session.duration_seconds = last_event_time.signed_duration_since(session_start).num_seconds();
            self.completed_sessions.push(session);
        }
        
        self.open_sessions.clear();
    }

    /// Get completed sessions and clear the buffer.
    pub fn drain_completed(&mut self) -> Vec<SessionMetadata> {
        std::mem::take(&mut self.completed_sessions)
    }

    /// Get the number of open sessions.
    pub fn open_session_count(&self) -> usize {
        self.open_sessions.len()
    }

    /// Get the number of completed sessions.
    pub fn completed_session_count(&self) -> usize {
        self.completed_sessions.len()
    }
}

/// Validate a table name to prevent SQL injection.
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

/// Generate ClickHouse DDL for sessions table.
///
/// # Arguments
/// * `table_name` - The table name (e.g., "sessions" or "my_db.sessions")
///
/// # Panics
/// Panics if the table name is invalid (contains non-alphanumeric characters).
/// This is a programming error - table names should be validated at configuration time.
pub fn sessions_table_ddl(table_name: &str) -> String {
    if !is_valid_table_name(table_name) {
        panic!(
            "Invalid table name '{}': must contain only letters, numbers, underscores, and dots",
            table_name
        );
    }

    format!(
        r#"
CREATE TABLE IF NOT EXISTS {} (
    session_id UUID,
    user_id String,
    project_id UUID,
    started_at DateTime64(6),
    ended_at DateTime64(6),
    event_count UInt32,
    duration_seconds Int64,
    has_recording Bool,
    entry_page Nullable(String),
    exit_page Nullable(String),
    device_type Nullable(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(started_at)
ORDER BY (project_id, user_id, started_at)
"#,
        table_name
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_config_default() {
        let config = SessionConfig::default();
        assert_eq!(config.timeout, Duration::minutes(30));
        assert_eq!(config.max_length, Duration::hours(24));
    }

    #[test]
    fn test_session_metadata() {
        let session_id = Uuid::new_v4();
        let start = Utc::now();
        let mut session = SessionMetadata::new(session_id, "user123", start);

        session.add_event(start + Duration::minutes(5), Some("/page1"));
        session.add_event(start + Duration::minutes(10), Some("/page2"));

        assert_eq!(session.event_count, 2);
        assert_eq!(session.entry_page, Some("/page1".to_string()));
        assert_eq!(session.exit_page, Some("/page2".to_string()));
        assert!(session.duration_seconds >= 600); // ~10 minutes
    }

    #[test]
    fn test_session_stitcher_basic() {
        let mut stitcher = SessionStitcher::with_defaults();
        let now = Utc::now();

        // User's first event - starts new session
        let session1 = stitcher.process_event("user1", "page_view", now, Some("/"));
        
        // Same user, 5 minutes later - same session
        let session2 = stitcher.process_event("user1", "click", now + Duration::minutes(5), None);
        assert_eq!(session1, session2);

        // Same user, 40 minutes later (past timeout) - new session
        let session3 = stitcher.process_event("user1", "page_view", now + Duration::minutes(40), Some("/"));
        assert_ne!(session1, session3);
    }

    #[test]
    fn test_session_stitcher_reset_event() {
        let config = SessionConfig::default().with_reset_events(vec!["login".to_string()]);
        let mut stitcher = SessionStitcher::new(config);
        let now = Utc::now();

        // Start session
        let session1 = stitcher.process_event("user1", "page_view", now, Some("/"));

        // Login within timeout - should still start new session
        let session2 = stitcher.process_event("user1", "login", now + Duration::minutes(5), None);
        assert_ne!(session1, session2);
    }

    #[test]
    fn test_session_stitcher_max_length() {
        let config = SessionConfig::new(30, 1); // 30 min timeout, 1 hour max
        let mut stitcher = SessionStitcher::new(config);
        let now = Utc::now();

        // Start session
        let session1 = stitcher.process_event("user1", "event1", now, None);

        // 30 min later - still same session
        let session2 = stitcher.process_event("user1", "event2", now + Duration::minutes(30), None);
        assert_eq!(session1, session2);

        // 70 min later (past max length) - new session
        let session3 = stitcher.process_event("user1", "event3", now + Duration::minutes(70), None);
        assert_ne!(session1, session3);
    }

    #[test]
    fn test_session_index() {
        let mut index = SessionIndex::new();
        
        let session1 = SessionMetadata::new(Uuid::new_v4(), "user1", Utc::now());
        let session2 = SessionMetadata::new(Uuid::new_v4(), "user1", Utc::now());
        let session3 = SessionMetadata::new(Uuid::new_v4(), "user2", Utc::now());

        index.add_session(session1.clone());
        index.add_session(session2.clone());
        index.add_session(session3);

        assert_eq!(index.session_count(), 3);
        assert_eq!(index.user_count(), 2);
        assert_eq!(index.get_user_sessions("user1").len(), 2);
        assert_eq!(index.get_user_sessions("user2").len(), 1);
    }

    #[test]
    fn test_table_name_validation() {
        assert!(is_valid_table_name("sessions"));
        assert!(is_valid_table_name("my_sessions"));
        assert!(is_valid_table_name("db.sessions"));
        assert!(is_valid_table_name("_private_table"));
        assert!(!is_valid_table_name("")); // Empty
        assert!(!is_valid_table_name("123start")); // Starts with number
        assert!(!is_valid_table_name("has-dash")); // Invalid character
        assert!(!is_valid_table_name("has space")); // Space
        assert!(!is_valid_table_name("has;semicolon")); // Semicolon
        assert!(!is_valid_table_name(&"a".repeat(129))); // Too long
    }

    #[test]
    fn test_sessions_table_ddl_valid() {
        let ddl = sessions_table_ddl("my_sessions");
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS my_sessions"));
    }

    #[test]
    #[should_panic(expected = "Invalid table name")]
    fn test_sessions_table_ddl_invalid() {
        sessions_table_ddl("invalid;table");
    }
}
