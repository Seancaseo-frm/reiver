//! Maintenance window system for scheduling downtime.
//!
//! This module provides:
//! - Maintenance window configuration and storage
//! - Active window detection (one-time and recurring)
//! - Integration points for alert and health check suppression

use chrono::{DateTime, Datelike, NaiveTime, TimeZone, Utc};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

use crate::db::DbPool;

/// Schedule type for maintenance windows
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "varchar", rename_all = "snake_case")]
#[allow(dead_code)] // Used for API type definition
pub enum ScheduleType {
    OneTime,
    Recurring,
}

impl Default for ScheduleType {
    fn default() -> Self {
        ScheduleType::OneTime
    }
}

/// Recurrence type for recurring maintenance windows
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[serde(rename_all = "lowercase")]
#[sqlx(type_name = "varchar", rename_all = "lowercase")]
#[allow(dead_code)] // Used for API type definition
pub enum RecurrenceType {
    Daily,
    Weekly,
    Monthly,
}

/// A maintenance window configuration
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct MaintenanceWindow {
    pub id: Uuid,
    pub project_id: Uuid,
    pub name: String,
    pub description: Option<String>,

    /// Schedule type: 'one_time' or 'recurring'
    pub schedule_type: String,

    /// For one-time: absolute start time (UTC)
    pub start_time: Option<DateTime<Utc>>,
    /// For one-time: absolute end time (UTC)
    pub end_time: Option<DateTime<Utc>>,

    /// For recurring: 'daily', 'weekly', 'monthly'
    pub recurrence_type: Option<String>,
    /// For weekly: 0=Sun, 1=Mon, ..., 6=Sat; For monthly: day of month (1-31)
    pub recurrence_days: Option<Vec<i32>>,
    /// Time of day when maintenance starts (HH:MM)
    pub recurrence_start_time: Option<NaiveTime>,
    /// Duration in minutes
    pub recurrence_duration_minutes: Option<i32>,
    /// IANA timezone (e.g., 'America/New_York')
    pub recurrence_timezone: Option<String>,
    /// Optional end date for recurring windows
    pub recurrence_end_date: Option<chrono::NaiveDate>,

    pub enabled: bool,
    pub created_by: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl MaintenanceWindow {
    /// Check if this maintenance window is currently active
    pub fn is_active(&self, now: DateTime<Utc>) -> bool {
        if !self.enabled {
            return false;
        }

        match self.schedule_type.as_str() {
            "one_time" => self.is_one_time_active(now),
            "recurring" => self.is_recurring_active(now),
            _ => false,
        }
    }

    /// Check if a one-time window is currently active
    fn is_one_time_active(&self, now: DateTime<Utc>) -> bool {
        match (self.start_time, self.end_time) {
            (Some(start), Some(end)) => now >= start && now <= end,
            _ => false,
        }
    }

    /// Check if a recurring window is currently active
    fn is_recurring_active(&self, now: DateTime<Utc>) -> bool {
        let recurrence_type = match &self.recurrence_type {
            Some(t) => t.as_str(),
            None => return false,
        };

        let start_time = match self.recurrence_start_time {
            Some(t) => t,
            None => return false,
        };

        let duration_minutes = match self.recurrence_duration_minutes {
            Some(d) => d,
            None => return false,
        };

        // Parse timezone (default to UTC if invalid)
        let tz: Tz = self
            .recurrence_timezone
            .as_ref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(chrono_tz::UTC);

        // Convert 'now' to the window's timezone
        let now_in_tz = now.with_timezone(&tz);
        let today = now_in_tz.date_naive();

        // Check if recurring window has ended
        if let Some(end_date) = self.recurrence_end_date {
            if today > end_date {
                return false;
            }
        }

        // Check if today matches the recurrence pattern
        let day_matches = match recurrence_type {
            "daily" => true,
            "weekly" => {
                let day_of_week = now_in_tz.weekday().num_days_from_sunday() as i32;
                self.recurrence_days
                    .as_ref()
                    .map(|days| days.contains(&day_of_week))
                    .unwrap_or(false)
            }
            "monthly" => {
                let day_of_month = now_in_tz.day() as i32;
                self.recurrence_days
                    .as_ref()
                    .map(|days| days.contains(&day_of_month))
                    .unwrap_or(false)
            }
            _ => false,
        };

        if !day_matches {
            return false;
        }

        // Check if current time is within the maintenance window
        let window_start = tz.from_local_datetime(&today.and_time(start_time)).single();

        match window_start {
            Some(start) => {
                let end = start + chrono::Duration::minutes(duration_minutes as i64);
                let start_utc = start.with_timezone(&Utc);
                let end_utc = end.with_timezone(&Utc);
                now >= start_utc && now <= end_utc
            }
            None => false, // Ambiguous or invalid time (DST transition)
        }
    }

    /// Compute the next occurrence of this maintenance window
    pub fn next_occurrence(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        if !self.enabled {
            return None;
        }

        match self.schedule_type.as_str() {
            "one_time" => {
                // For one-time, return start_time if it's in the future
                self.start_time.filter(|&start| start > after)
            }
            "recurring" => self.next_recurring_occurrence(after),
            _ => None,
        }
    }

    /// Compute the next occurrence of a recurring window
    fn next_recurring_occurrence(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let recurrence_type = self.recurrence_type.as_ref()?;
        let start_time = self.recurrence_start_time?;
        let _duration_minutes = self.recurrence_duration_minutes?;

        let tz: Tz = self
            .recurrence_timezone
            .as_ref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(chrono_tz::UTC);

        let after_in_tz = after.with_timezone(&tz);
        let mut candidate_date = after_in_tz.date_naive();

        // Search up to 366 days ahead
        for _ in 0..366 {
            // Check if candidate date is past the end date
            if let Some(end_date) = self.recurrence_end_date {
                if candidate_date > end_date {
                    return None;
                }
            }

            let day_matches = match recurrence_type.as_str() {
                "daily" => true,
                "weekly" => {
                    let day_of_week = tz
                        .from_local_datetime(
                            &candidate_date
                                .and_hms_opt(12, 0, 0)
                                .expect("12:00:00 is always a valid time"),
                        )
                        .single()
                        .map(|dt| dt.weekday().num_days_from_sunday() as i32)
                        .unwrap_or(-1);
                    self.recurrence_days
                        .as_ref()
                        .map(|days| days.contains(&day_of_week))
                        .unwrap_or(false)
                }
                "monthly" => {
                    let day_of_month = candidate_date.day() as i32;
                    self.recurrence_days
                        .as_ref()
                        .map(|days| days.contains(&day_of_month))
                        .unwrap_or(false)
                }
                _ => false,
            };

            if day_matches {
                if let Some(start) = tz
                    .from_local_datetime(&candidate_date.and_time(start_time))
                    .single()
                {
                    let start_utc = start.with_timezone(&Utc);
                    if start_utc > after {
                        return Some(start_utc);
                    }
                }
            }

            candidate_date = candidate_date.succ_opt()?;
        }

        None
    }
}

/// Load all enabled maintenance windows for a project
pub async fn load_project_windows(
    db: &DbPool,
    project_id: Uuid,
) -> anyhow::Result<Vec<MaintenanceWindow>> {
    let windows = sqlx::query_as::<_, MaintenanceWindow>(
        r#"
        SELECT id, project_id, name, description,
               schedule_type, start_time, end_time,
               recurrence_type, recurrence_days, recurrence_start_time,
               recurrence_duration_minutes, recurrence_timezone, recurrence_end_date,
               enabled, created_by, created_at, updated_at
        FROM maintenance_windows
        WHERE project_id = $1 AND enabled = true
        "#,
    )
    .bind(project_id)
    .fetch_all(db)
    .await?;

    Ok(windows)
}

/// Get all currently active maintenance windows for a project
pub async fn get_active_windows(
    db: &DbPool,
    project_id: Uuid,
) -> anyhow::Result<Vec<MaintenanceWindow>> {
    let windows = load_project_windows(db, project_id).await?;
    let now = Utc::now();

    Ok(windows.into_iter().filter(|w| w.is_active(now)).collect())
}

/// Check if a project is currently in maintenance
pub async fn is_project_in_maintenance(db: &DbPool, project_id: Uuid) -> anyhow::Result<bool> {
    let active_windows = get_active_windows(db, project_id).await?;
    Ok(!active_windows.is_empty())
}

/// Get IDs of projects currently in maintenance (for bulk checking)
#[allow(dead_code)] // Available for future bulk maintenance check
pub async fn get_projects_in_maintenance(db: &DbPool) -> anyhow::Result<Vec<Uuid>> {
    // Load all enabled windows and check which are active
    let windows = sqlx::query_as::<_, MaintenanceWindow>(
        r#"
        SELECT id, project_id, name, description,
               schedule_type, start_time, end_time,
               recurrence_type, recurrence_days, recurrence_start_time,
               recurrence_duration_minutes, recurrence_timezone, recurrence_end_date,
               enabled, created_by, created_at, updated_at
        FROM maintenance_windows
        WHERE enabled = true
        "#,
    )
    .fetch_all(db)
    .await?;

    let now = Utc::now();
    let mut project_ids: Vec<Uuid> = windows
        .into_iter()
        .filter(|w| w.is_active(now))
        .map(|w| w.project_id)
        .collect();

    project_ids.sort();
    project_ids.dedup();

    Ok(project_ids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn create_one_time_window(start: DateTime<Utc>, end: DateTime<Utc>) -> MaintenanceWindow {
        MaintenanceWindow {
            id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            name: "Test Window".to_string(),
            description: None,
            schedule_type: "one_time".to_string(),
            start_time: Some(start),
            end_time: Some(end),
            recurrence_type: None,
            recurrence_days: None,
            recurrence_start_time: None,
            recurrence_duration_minutes: None,
            recurrence_timezone: None,
            recurrence_end_date: None,
            enabled: true,
            created_by: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn create_recurring_window(
        recurrence_type: &str,
        days: Option<Vec<i32>>,
        start_time: NaiveTime,
        duration_minutes: i32,
        timezone: &str,
    ) -> MaintenanceWindow {
        MaintenanceWindow {
            id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            name: "Test Recurring Window".to_string(),
            description: None,
            schedule_type: "recurring".to_string(),
            start_time: None,
            end_time: None,
            recurrence_type: Some(recurrence_type.to_string()),
            recurrence_days: days,
            recurrence_start_time: Some(start_time),
            recurrence_duration_minutes: Some(duration_minutes),
            recurrence_timezone: Some(timezone.to_string()),
            recurrence_end_date: None,
            enabled: true,
            created_by: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn test_one_time_window_active() {
        let start = Utc.with_ymd_and_hms(2024, 8, 1, 10, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 8, 1, 11, 0, 0).unwrap();
        let window = create_one_time_window(start, end);

        // Before window
        let before = Utc.with_ymd_and_hms(2024, 8, 1, 9, 0, 0).unwrap();
        assert!(!window.is_active(before));

        // During window
        let during = Utc.with_ymd_and_hms(2024, 8, 1, 10, 30, 0).unwrap();
        assert!(window.is_active(during));

        // After window
        let after = Utc.with_ymd_and_hms(2024, 8, 1, 12, 0, 0).unwrap();
        assert!(!window.is_active(after));
    }

    #[test]
    fn test_recurring_daily_window() {
        let start_time = NaiveTime::from_hms_opt(22, 0, 0).unwrap();
        let window = create_recurring_window("daily", None, start_time, 60, "UTC");

        // During maintenance (22:30 UTC)
        let during = Utc.with_ymd_and_hms(2024, 8, 1, 22, 30, 0).unwrap();
        assert!(window.is_active(during));

        // Outside maintenance (21:00 UTC)
        let before = Utc.with_ymd_and_hms(2024, 8, 1, 21, 0, 0).unwrap();
        assert!(!window.is_active(before));

        // After maintenance (23:30 UTC)
        let after = Utc.with_ymd_and_hms(2024, 8, 1, 23, 30, 0).unwrap();
        assert!(!window.is_active(after));
    }

    #[test]
    fn test_recurring_weekly_window() {
        let start_time = NaiveTime::from_hms_opt(22, 0, 0).unwrap();
        // Monday only (1 = Monday in our encoding where 0 = Sunday)
        let window = create_recurring_window("weekly", Some(vec![1]), start_time, 60, "UTC");

        // Monday at 22:30 UTC (2024-07-29 is a Monday)
        let monday_during = Utc.with_ymd_and_hms(2024, 7, 29, 22, 30, 0).unwrap();
        assert!(window.is_active(monday_during));

        // Tuesday at 22:30 UTC (2024-07-30 is a Tuesday)
        let tuesday = Utc.with_ymd_and_hms(2024, 7, 30, 22, 30, 0).unwrap();
        assert!(!window.is_active(tuesday));
    }

    #[test]
    fn test_disabled_window() {
        let start = Utc.with_ymd_and_hms(2024, 8, 1, 10, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 8, 1, 11, 0, 0).unwrap();
        let mut window = create_one_time_window(start, end);
        window.enabled = false;

        let during = Utc.with_ymd_and_hms(2024, 8, 1, 10, 30, 0).unwrap();
        assert!(!window.is_active(during));
    }
}
