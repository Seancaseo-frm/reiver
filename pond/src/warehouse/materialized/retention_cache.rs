//! Retention Cohort Cache
//!
//! Pre-computed retention cohorts for instant retention analysis.
//!
//! # Retention Analysis
//!
//! Tracks which users from each signup cohort returned in subsequent periods.
//!
//! Example retention matrix:
//! ```text
//! Cohort    | Week 0 | Week 1 | Week 2 | Week 3
//! ----------|--------|--------|--------|--------
//! Jan 1     | 100%   | 40%    | 30%    | 25%
//! Jan 8     | 100%   | 45%    | 35%    | 28%
//! Jan 15    | 100%   | 38%    | 32%    | --
//! ```

use ahash::AHashMap;
use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A cohort's retention data.
#[derive(Debug, Clone)]
pub struct CohortRetention {
    /// Cohort identifier (start of the cohort period).
    pub cohort_start: NaiveDate,
    /// Users who belong to this cohort (signed up in this period).
    pub cohort_users: RoaringBitmap,
    /// Returning users by period offset.
    /// Key: period offset (0 = same period, 1 = next period, etc.)
    /// Value: bitmap of users who were active in that period
    pub retention: AHashMap<u8, RoaringBitmap>,
}

impl CohortRetention {
    /// Create a new cohort.
    pub fn new(cohort_start: NaiveDate) -> Self {
        let mut retention = AHashMap::new();
        retention.insert(0, RoaringBitmap::new());
        
        Self {
            cohort_start,
            cohort_users: RoaringBitmap::new(),
            retention,
        }
    }

    /// Add a user to the cohort.
    pub fn add_user(&mut self, user_id: u32) {
        self.cohort_users.insert(user_id);
        self.retention.entry(0).or_default().insert(user_id);
    }

    /// Record a user's activity in a period.
    pub fn record_activity(&mut self, user_id: u32, period_offset: u8) {
        // Only count if user is in this cohort
        if self.cohort_users.contains(user_id) {
            self.retention.entry(period_offset).or_default().insert(user_id);
        }
    }

    /// Get cohort size.
    pub fn cohort_size(&self) -> u64 {
        self.cohort_users.len()
    }

    /// Get retention rate for a specific period.
    pub fn retention_rate(&self, period_offset: u8) -> f64 {
        let cohort_size = self.cohort_size() as f64;
        if cohort_size == 0.0 {
            return 0.0;
        }

        let active = self
            .retention
            .get(&period_offset)
            .map(|bm| bm.len())
            .unwrap_or(0) as f64;

        active / cohort_size
    }

    /// Get all retention rates as a vector.
    pub fn all_retention_rates(&self, max_periods: u8) -> Vec<f64> {
        (0..max_periods)
            .map(|p| self.retention_rate(p))
            .collect()
    }
}

/// Pre-computed retention cohorts.
#[derive(Debug)]
pub struct RetentionCohortCache {
    /// Project ID.
    pub project_id: Uuid,
    /// Cohort period type.
    pub period_type: RetentionPeriod,
    /// Cohorts by start date.
    cohorts: AHashMap<NaiveDate, CohortRetention>,
    /// User ID -> cohort date mapping (which cohort each user belongs to).
    user_cohorts: AHashMap<u32, NaiveDate>,
    /// When this cache was last updated.
    pub updated_at: DateTime<Utc>,
}

/// Period type for retention analysis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetentionPeriod {
    /// Daily cohorts.
    Day,
    /// Weekly cohorts (Monday start).
    Week,
    /// Monthly cohorts.
    Month,
}

impl RetentionPeriod {
    /// Get the cohort start date for a given timestamp.
    ///
    /// # Panics
    ///
    /// Panics if the date's year/month combination is invalid, which should never
    /// happen for dates derived from valid `DateTime<Utc>` values.
    pub fn cohort_date(&self, timestamp: DateTime<Utc>) -> NaiveDate {
        let date = timestamp.date_naive();
        match self {
            RetentionPeriod::Day => date,
            RetentionPeriod::Week => {
                // Find the Monday of this week
                let days_from_monday = date.weekday().num_days_from_monday();
                date - Duration::days(days_from_monday as i64)
            }
            RetentionPeriod::Month => {
                // First day of the month - this should never fail since:
                // 1. date.year() returns a valid year from the input
                // 2. date.month() returns 1-12
                // 3. Day 1 is valid for all months
                NaiveDate::from_ymd_opt(date.year(), date.month(), 1)
                    .expect("Day 1 of any month should always be valid")
            }
        }
    }

    /// Calculate the period offset between two dates.
    ///
    /// Returns a value from 0 to 255. Offsets larger than 255 are clamped to 255.
    /// This means retention analysis is limited to 255 periods (days/weeks/months)
    /// which is sufficient for most use cases (7+ months of daily, 5+ years of weekly,
    /// or 21+ years of monthly retention).
    pub fn period_offset(&self, cohort_date: NaiveDate, activity_date: NaiveDate) -> u8 {
        let diff = activity_date.signed_duration_since(cohort_date);
        
        // Clamp to u8::MAX (255) to prevent overflow
        let offset = match self {
            RetentionPeriod::Day => diff.num_days().max(0),
            RetentionPeriod::Week => (diff.num_days() / 7).max(0),
            RetentionPeriod::Month => {
                // Calculate month difference
                let cohort_months = cohort_date.year() as i64 * 12 + cohort_date.month() as i64;
                let activity_months = activity_date.year() as i64 * 12 + activity_date.month() as i64;
                (activity_months - cohort_months).max(0)
            }
        };
        
        offset.min(u8::MAX as i64) as u8
    }
}

impl RetentionCohortCache {
    /// Create a new retention cache.
    pub fn new(project_id: Uuid, period_type: RetentionPeriod) -> Self {
        Self {
            project_id,
            period_type,
            cohorts: AHashMap::new(),
            user_cohorts: AHashMap::new(),
            updated_at: Utc::now(),
        }
    }

    /// Register a new user (first seen event).
    pub fn register_user(&mut self, user_id: u32, first_seen: DateTime<Utc>) {
        let cohort_date = self.period_type.cohort_date(first_seen);
        
        // Create cohort if needed
        let cohort = self
            .cohorts
            .entry(cohort_date)
            .or_insert_with(|| CohortRetention::new(cohort_date));

        cohort.add_user(user_id);
        self.user_cohorts.insert(user_id, cohort_date);
        self.updated_at = Utc::now();
    }

    /// Record user activity.
    pub fn record_activity(&mut self, user_id: u32, activity_time: DateTime<Utc>) {
        // Find user's cohort
        if let Some(&cohort_date) = self.user_cohorts.get(&user_id) {
            let activity_date = activity_time.date_naive();
            let period_offset = self.period_type.period_offset(cohort_date, activity_date);

            if let Some(cohort) = self.cohorts.get_mut(&cohort_date) {
                cohort.record_activity(user_id, period_offset);
            }
        }
        self.updated_at = Utc::now();
    }

    /// Get retention matrix.
    ///
    /// Returns a map of cohort start date -> retention rates.
    pub fn retention_matrix(&self, max_periods: u8) -> AHashMap<NaiveDate, Vec<f64>> {
        self.cohorts
            .iter()
            .map(|(date, cohort)| (*date, cohort.all_retention_rates(max_periods)))
            .collect()
    }

    /// Get a specific cohort.
    pub fn get_cohort(&self, cohort_date: NaiveDate) -> Option<&CohortRetention> {
        self.cohorts.get(&cohort_date)
    }

    /// Get all cohort dates.
    pub fn cohort_dates(&self) -> Vec<NaiveDate> {
        let mut dates: Vec<_> = self.cohorts.keys().copied().collect();
        dates.sort();
        dates
    }

    /// Get summary statistics.
    pub fn summary(&self, max_periods: u8) -> RetentionSummary {
        let cohort_dates = self.cohort_dates();
        let matrix = self.retention_matrix(max_periods);

        // Calculate average retention for each period
        let mut avg_retention = vec![0.0; max_periods as usize];
        let num_cohorts = cohort_dates.len() as f64;

        if num_cohorts > 0.0 {
            for rates in matrix.values() {
                for (i, rate) in rates.iter().enumerate() {
                    if i < avg_retention.len() {
                        avg_retention[i] += rate;
                    }
                }
            }
            for rate in &mut avg_retention {
                *rate /= num_cohorts;
            }
        }

        RetentionSummary {
            project_id: self.project_id,
            period_type: self.period_type,
            num_cohorts: cohort_dates.len(),
            total_users: self.user_cohorts.len(),
            average_retention_by_period: avg_retention,
        }
    }
}

/// Summary of retention analysis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionSummary {
    pub project_id: Uuid,
    pub period_type: RetentionPeriod,
    pub num_cohorts: usize,
    pub total_users: usize,
    pub average_retention_by_period: Vec<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cohort_retention() {
        let start_date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let mut cohort = CohortRetention::new(start_date);

        // Add 100 users to cohort
        for i in 0..100 {
            cohort.add_user(i);
        }

        assert_eq!(cohort.cohort_size(), 100);
        assert!((cohort.retention_rate(0) - 1.0).abs() < 0.001);

        // 50 users return in week 1
        for i in 0..50 {
            cohort.record_activity(i, 1);
        }

        assert!((cohort.retention_rate(1) - 0.5).abs() < 0.001);

        // 25 users return in week 2
        for i in 0..25 {
            cohort.record_activity(i, 2);
        }

        assert!((cohort.retention_rate(2) - 0.25).abs() < 0.001);
    }

    #[test]
    fn test_period_cohort_date() {
        let timestamp = DateTime::parse_from_rfc3339("2024-01-17T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        // Day
        assert_eq!(
            RetentionPeriod::Day.cohort_date(timestamp),
            NaiveDate::from_ymd_opt(2024, 1, 17).unwrap()
        );

        // Week (Jan 17 is a Wednesday, so Monday is Jan 15)
        assert_eq!(
            RetentionPeriod::Week.cohort_date(timestamp),
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()
        );

        // Month
        assert_eq!(
            RetentionPeriod::Month.cohort_date(timestamp),
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()
        );
    }

    #[test]
    fn test_period_offset() {
        let cohort = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();

        // Same day
        let same_day = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        assert_eq!(RetentionPeriod::Day.period_offset(cohort, same_day), 0);

        // Next day
        let next_day = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        assert_eq!(RetentionPeriod::Day.period_offset(cohort, next_day), 1);

        // Same week
        let same_week = NaiveDate::from_ymd_opt(2024, 1, 6).unwrap();
        assert_eq!(RetentionPeriod::Week.period_offset(cohort, same_week), 0);

        // Next week
        let next_week = NaiveDate::from_ymd_opt(2024, 1, 8).unwrap();
        assert_eq!(RetentionPeriod::Week.period_offset(cohort, next_week), 1);

        // Next month
        let next_month = NaiveDate::from_ymd_opt(2024, 2, 15).unwrap();
        assert_eq!(RetentionPeriod::Month.period_offset(cohort, next_month), 1);
    }

    #[test]
    fn test_retention_cache() {
        let project_id = Uuid::new_v4();
        let mut cache = RetentionCohortCache::new(project_id, RetentionPeriod::Week);

        let week1_start = DateTime::parse_from_rfc3339("2024-01-01T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        
        let week1_activity = DateTime::parse_from_rfc3339("2024-01-03T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        
        let week2_activity = DateTime::parse_from_rfc3339("2024-01-10T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        // Register 10 users in week 1
        for i in 0..10 {
            cache.register_user(i, week1_start);
        }

        // All 10 active in week 1
        for i in 0..10 {
            cache.record_activity(i, week1_activity);
        }

        // 5 return in week 2
        for i in 0..5 {
            cache.record_activity(i, week2_activity);
        }

        let cohort_date = RetentionPeriod::Week.cohort_date(week1_start);
        let cohort = cache.get_cohort(cohort_date).unwrap();

        assert_eq!(cohort.cohort_size(), 10);
        assert!((cohort.retention_rate(0) - 1.0).abs() < 0.001);
        assert!((cohort.retention_rate(1) - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_retention_summary() {
        let project_id = Uuid::new_v4();
        let mut cache = RetentionCohortCache::new(project_id, RetentionPeriod::Day);

        let day1 = DateTime::parse_from_rfc3339("2024-01-01T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let day2 = DateTime::parse_from_rfc3339("2024-01-02T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        // Cohort 1: 10 users
        for i in 0..10 {
            cache.register_user(i, day1);
        }

        // Cohort 2: 10 users
        for i in 10..20 {
            cache.register_user(i, day2);
        }

        let summary = cache.summary(3);
        assert_eq!(summary.num_cohorts, 2);
        assert_eq!(summary.total_users, 20);
    }
}
