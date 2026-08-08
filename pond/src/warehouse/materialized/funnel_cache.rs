//! Funnel Step Cache
//!
//! Pre-computed funnel step completions using Roaring Bitmaps.
//! This enables instant funnel conversion rate queries.
//!
//! # Example Funnel
//!
//! For a 3-step funnel (A -> B -> C):
//! - Step 1: Users who did A
//! - Step 2: Users who did A then B (within time window)
//! - Step 3: Users who did A then B then C (within time window)
//!
//! # Storage
//!
//! We store bitmaps of user IDs at each step. Conversion rate is simply:
//! `bitmap[n].len() / bitmap[n-1].len()`
//!
//! # Important Limitations
//!
//! ## User ID Type
//!
//! User IDs are stored as `u32` values for memory efficiency with Roaring Bitmaps.
//! This means:
//! - Maximum ~4.3 billion unique users per project
//! - User IDs must be mapped to u32 values (e.g., via a hash or lookup table)
//! - If you have string user IDs, hash them to u32 (accepting collision risk for very large user bases)
//!
//! ## Event Ordering
//!
//! **CRITICAL**: Events MUST be processed in chronological order for accurate funnel results.
//! The funnel builder tracks user progress through steps sequentially. If events arrive
//! out of order:
//! - Users may be incorrectly excluded from funnel steps
//! - Conversion windows may be miscalculated
//! - Step completion order may be incorrect
//!
//! Sort your events by timestamp before processing, or use a streaming approach
//! that guarantees chronological delivery per user.

use ahash::AHashMap;
use chrono::{DateTime, Duration, Utc};
use compact_str::CompactString;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use uuid::Uuid;

/// A funnel definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunnelStep {
    /// Event type for this step.
    pub event_type: CompactString,
    /// Optional event properties filter.
    pub filters: AHashMap<CompactString, CompactString>,
}

impl FunnelStep {
    /// Create a simple funnel step.
    pub fn new(event_type: &str) -> Self {
        Self {
            event_type: CompactString::from(event_type),
            filters: AHashMap::new(),
        }
    }

    /// Create a step with property filter.
    pub fn with_filter(event_type: &str, key: &str, value: &str) -> Self {
        let mut filters = AHashMap::new();
        filters.insert(CompactString::from(key), CompactString::from(value));
        Self {
            event_type: CompactString::from(event_type),
            filters,
        }
    }
}

/// Pre-computed funnel step completions.
#[derive(Debug, Clone)]
pub struct FunnelStepCache {
    /// Funnel ID.
    pub funnel_id: Uuid,
    /// Project ID.
    pub project_id: Uuid,
    /// Funnel steps.
    pub steps: SmallVec<[FunnelStep; 8]>,
    /// Conversion window (max time between steps).
    pub conversion_window: Duration,
    /// Bitmap of user IDs at each step.
    /// user_bitmaps[0] = users who completed step 1
    /// user_bitmaps[1] = users who completed steps 1 and 2
    /// etc.
    pub user_bitmaps: SmallVec<[RoaringBitmap; 8]>,
    /// When this cache was last updated.
    pub updated_at: DateTime<Utc>,
    /// Date range this cache covers.
    pub date_range_start: DateTime<Utc>,
    pub date_range_end: DateTime<Utc>,
}

impl FunnelStepCache {
    /// Create a new funnel cache.
    pub fn new(
        funnel_id: Uuid,
        project_id: Uuid,
        steps: SmallVec<[FunnelStep; 8]>,
        conversion_window: Duration,
        date_range_start: DateTime<Utc>,
        date_range_end: DateTime<Utc>,
    ) -> Self {
        let num_steps = steps.len();
        Self {
            funnel_id,
            project_id,
            steps,
            conversion_window,
            user_bitmaps: smallvec::smallvec![RoaringBitmap::new(); num_steps],
            updated_at: Utc::now(),
            date_range_start,
            date_range_end,
        }
    }

    /// Add a user to a specific step.
    ///
    /// Users should only be added to step N if they completed steps 1..N-1.
    pub fn add_user_to_step(&mut self, step_index: usize, user_id: u32) {
        if step_index < self.user_bitmaps.len() {
            self.user_bitmaps[step_index].insert(user_id);
            self.updated_at = Utc::now();
        }
    }

    /// Get the count of users at each step.
    pub fn step_counts(&self) -> Vec<u64> {
        self.user_bitmaps.iter().map(|bm| bm.len()).collect()
    }

    /// Get conversion rates between steps.
    ///
    /// Returns a vector of conversion rates:
    /// - rates[0] = conversion from step 1 to step 2
    /// - rates[n-2] = conversion from step n-1 to step n
    pub fn conversion_rates(&self) -> Vec<f64> {
        if self.user_bitmaps.len() < 2 {
            return vec![];
        }

        self.user_bitmaps
            .windows(2)
            .map(|window| {
                let prev_count = window[0].len() as f64;
                let curr_count = window[1].len() as f64;
                if prev_count > 0.0 {
                    curr_count / prev_count
                } else {
                    0.0
                }
            })
            .collect()
    }

    /// Get overall funnel conversion rate (first step to last step).
    pub fn overall_conversion(&self) -> f64 {
        if self.user_bitmaps.is_empty() {
            return 0.0;
        }

        let first = self.user_bitmaps.first().map(|bm| bm.len()).unwrap_or(0) as f64;
        let last = self.user_bitmaps.last().map(|bm| bm.len()).unwrap_or(0) as f64;

        if first > 0.0 {
            last / first
        } else {
            0.0
        }
    }

    /// Get dropoff between steps.
    pub fn dropoff_counts(&self) -> Vec<u64> {
        self.user_bitmaps
            .windows(2)
            .map(|window| {
                let prev = window[0].len();
                let curr = window[1].len();
                prev.saturating_sub(curr)
            })
            .collect()
    }

    /// Merge another funnel cache into this one.
    ///
    /// Useful for combining caches from different time periods.
    pub fn merge(&mut self, other: &FunnelStepCache) {
        if other.steps.len() != self.steps.len() {
            return; // Can't merge different funnel shapes
        }

        for (i, other_bitmap) in other.user_bitmaps.iter().enumerate() {
            self.user_bitmaps[i] |= other_bitmap;
        }

        // Update date range
        if other.date_range_start < self.date_range_start {
            self.date_range_start = other.date_range_start;
        }
        if other.date_range_end > self.date_range_end {
            self.date_range_end = other.date_range_end;
        }

        self.updated_at = Utc::now();
    }

    /// Get a summary of the funnel.
    pub fn summary(&self) -> FunnelSummary {
        FunnelSummary {
            funnel_id: self.funnel_id,
            step_names: self.steps.iter().map(|s| s.event_type.clone()).collect(),
            step_counts: self.step_counts().into(),
            conversion_rates: self.conversion_rates().into(),
            overall_conversion: self.overall_conversion(),
        }
    }
}

/// Summary of a funnel's performance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunnelSummary {
    pub funnel_id: Uuid,
    pub step_names: SmallVec<[CompactString; 8]>,
    pub step_counts: SmallVec<[u64; 8]>,
    pub conversion_rates: SmallVec<[f64; 8]>,
    pub overall_conversion: f64,
}

/// Builder for creating funnel caches from event streams.
pub struct FunnelCacheBuilder {
    funnel_id: Uuid,
    project_id: Uuid,
    steps: SmallVec<[FunnelStep; 8]>,
    conversion_window: Duration,
    /// Tracks user progress: user_id -> (current_step, last_step_time)
    user_progress: AHashMap<u32, (usize, DateTime<Utc>)>,
    /// Completed users at each step
    completed: SmallVec<[RoaringBitmap; 8]>,
    /// Date range of processed events
    date_range_start: Option<DateTime<Utc>>,
    date_range_end: Option<DateTime<Utc>>,
}

impl FunnelCacheBuilder {
    /// Create a new builder.
    pub fn new(
        funnel_id: Uuid,
        project_id: Uuid,
        steps: SmallVec<[FunnelStep; 8]>,
        conversion_window: Duration,
    ) -> Self {
        let num_steps = steps.len();
        Self {
            funnel_id,
            project_id,
            steps,
            conversion_window,
            user_progress: AHashMap::new(),
            completed: smallvec::smallvec![RoaringBitmap::new(); num_steps],
            date_range_start: None,
            date_range_end: None,
        }
    }

    /// Process an event and update funnel progress.
    ///
    /// # Arguments
    /// * `user_id` - User identifier as u32 (see module docs for mapping guidance)
    /// * `event_type` - The type of event (matched against funnel step event types)
    /// * `timestamp` - When the event occurred
    ///
    /// # Important: Chronological Order Required
    ///
    /// **Events MUST be processed in chronological order for accurate results.**
    /// Out-of-order events will result in incorrect funnel calculations because:
    /// - User progress is tracked sequentially
    /// - Conversion window checks depend on timestamp ordering
    ///
    /// If you cannot guarantee chronological order, consider buffering and sorting
    /// events before processing.
    pub fn process_event(&mut self, user_id: u32, event_type: &str, timestamp: DateTime<Utc>) {
        // Update date range
        self.date_range_start = Some(
            self.date_range_start
                .map(|d| d.min(timestamp))
                .unwrap_or(timestamp),
        );
        self.date_range_end = Some(
            self.date_range_end
                .map(|d| d.max(timestamp))
                .unwrap_or(timestamp),
        );

        // Find which step this event matches
        let step_index = self.steps.iter().position(|s| s.event_type == event_type);

        if let Some(idx) = step_index {
            if idx == 0 {
                // First step - always record
                self.user_progress.insert(user_id, (0, timestamp));
                self.completed[0].insert(user_id);
            } else {
                // Later step - check if user completed previous step
                if let Some(&(current_step, last_time)) = self.user_progress.get(&user_id) {
                    // Check if this is the next expected step
                    if idx == current_step + 1 {
                        // Check conversion window
                        if timestamp - last_time <= self.conversion_window {
                            self.user_progress.insert(user_id, (idx, timestamp));
                            self.completed[idx].insert(user_id);
                        }
                    }
                }
            }
        }
    }

    /// Build the funnel cache.
    pub fn build(self) -> FunnelStepCache {
        FunnelStepCache {
            funnel_id: self.funnel_id,
            project_id: self.project_id,
            steps: self.steps,
            conversion_window: self.conversion_window,
            user_bitmaps: self.completed,
            updated_at: Utc::now(),
            date_range_start: self.date_range_start.unwrap_or_else(Utc::now),
            date_range_end: self.date_range_end.unwrap_or_else(Utc::now),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_funnel_cache_creation() {
        let funnel_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        let steps = smallvec::smallvec![
            FunnelStep::new("page_view"),
            FunnelStep::new("add_to_cart"),
            FunnelStep::new("purchase"),
        ];
        let now = Utc::now();

        let cache = FunnelStepCache::new(
            funnel_id,
            project_id,
            steps,
            Duration::hours(24),
            now - Duration::days(7),
            now,
        );

        assert_eq!(cache.user_bitmaps.len(), 3);
        assert_eq!(cache.step_counts(), vec![0, 0, 0]);
    }

    #[test]
    fn test_funnel_conversion_rates() {
        let funnel_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        let steps = smallvec::smallvec![
            FunnelStep::new("step1"),
            FunnelStep::new("step2"),
            FunnelStep::new("step3"),
        ];
        let now = Utc::now();

        let mut cache = FunnelStepCache::new(
            funnel_id,
            project_id,
            steps,
            Duration::hours(24),
            now - Duration::days(7),
            now,
        );

        // 100 users at step 1
        for i in 0..100 {
            cache.add_user_to_step(0, i);
        }

        // 50 users at step 2
        for i in 0..50 {
            cache.add_user_to_step(1, i);
        }

        // 25 users at step 3
        for i in 0..25 {
            cache.add_user_to_step(2, i);
        }

        let rates = cache.conversion_rates();
        assert_eq!(rates.len(), 2);
        assert!((rates[0] - 0.5).abs() < 0.001); // 50/100
        assert!((rates[1] - 0.5).abs() < 0.001); // 25/50

        assert!((cache.overall_conversion() - 0.25).abs() < 0.001); // 25/100
    }

    #[test]
    fn test_funnel_builder() {
        let funnel_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        let steps = smallvec::smallvec![
            FunnelStep::new("view"),
            FunnelStep::new("click"),
            FunnelStep::new("convert"),
        ];
        let now = Utc::now();

        let mut builder = FunnelCacheBuilder::new(
            funnel_id,
            project_id,
            steps,
            Duration::hours(1),
        );

        // User 1: completes all steps
        builder.process_event(1, "view", now);
        builder.process_event(1, "click", now + Duration::minutes(5));
        builder.process_event(1, "convert", now + Duration::minutes(10));

        // User 2: completes first 2 steps
        builder.process_event(2, "view", now);
        builder.process_event(2, "click", now + Duration::minutes(5));

        // User 3: only views
        builder.process_event(3, "view", now);

        // User 4: clicks without viewing (should not count)
        builder.process_event(4, "click", now);

        let cache = builder.build();
        let counts = cache.step_counts();

        assert_eq!(counts[0], 3); // Users 1, 2, 3 viewed
        assert_eq!(counts[1], 2); // Users 1, 2 clicked (after viewing)
        assert_eq!(counts[2], 1); // Only user 1 converted
    }

    #[test]
    fn test_conversion_window() {
        let funnel_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        let steps = smallvec::smallvec![
            FunnelStep::new("step1"),
            FunnelStep::new("step2"),
        ];
        let now = Utc::now();

        let mut builder = FunnelCacheBuilder::new(
            funnel_id,
            project_id,
            steps,
            Duration::hours(1), // 1 hour window
        );

        // User 1: within window
        builder.process_event(1, "step1", now);
        builder.process_event(1, "step2", now + Duration::minutes(30));

        // User 2: outside window
        builder.process_event(2, "step1", now);
        builder.process_event(2, "step2", now + Duration::hours(2));

        let cache = builder.build();
        let counts = cache.step_counts();

        assert_eq!(counts[0], 2); // Both users started
        assert_eq!(counts[1], 1); // Only user 1 completed within window
    }
}
