//! Data Freshness Indicators
//!
//! Tracks and reports data freshness for warehouse tables.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use uuid::Uuid;

/// Staleness level for table data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StalenessLevel {
    /// Less than 1 hour old
    Fresh,
    /// 1-6 hours old
    Moderate,
    /// 6-24 hours old
    Stale,
    /// More than 24 hours old
    VeryStale,
    /// Never synced or unknown
    Unknown,
}

impl StalenessLevel {
    /// Determine staleness level from duration since last sync.
    pub fn from_duration(duration: Option<Duration>) -> Self {
        match duration {
            None => Self::Unknown,
            Some(d) if d < Duration::hours(1) => Self::Fresh,
            Some(d) if d < Duration::hours(6) => Self::Moderate,
            Some(d) if d < Duration::hours(24) => Self::Stale,
            _ => Self::VeryStale,
        }
    }

    /// Get a human-readable description.
    pub fn description(&self) -> &'static str {
        match self {
            Self::Fresh => "Data is up to date (< 1 hour old)",
            Self::Moderate => "Data is moderately fresh (1-6 hours old)",
            Self::Stale => "Data is stale (6-24 hours old)",
            Self::VeryStale => "Data is very stale (> 24 hours old)",
            Self::Unknown => "Last sync time unknown",
        }
    }

    /// Check if data needs refresh.
    pub fn needs_refresh(&self) -> bool {
        matches!(self, Self::Stale | Self::VeryStale | Self::Unknown)
    }
}

impl std::fmt::Display for StalenessLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fresh => write!(f, "fresh"),
            Self::Moderate => write!(f, "moderate"),
            Self::Stale => write!(f, "stale"),
            Self::VeryStale => write!(f, "very_stale"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

/// Freshness information for a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableFreshness {
    /// Table name
    pub table_name: String,
    /// Source name
    pub source_name: String,
    /// Source ID
    pub source_id: Uuid,
    /// Last sync timestamp
    pub last_sync_at: Option<DateTime<Utc>>,
    /// Next scheduled sync
    pub next_sync_at: Option<DateTime<Utc>>,
    /// Current sync status
    pub sync_status: String,
    /// Time since last sync
    pub staleness: Option<Duration>,
    /// Staleness classification
    pub staleness_level: StalenessLevel,
    /// Minutes since last sync
    pub staleness_minutes: Option<i64>,
}

impl TableFreshness {
    /// Check if the table data is fresh.
    pub fn is_fresh(&self) -> bool {
        self.staleness_level == StalenessLevel::Fresh
    }

    /// Check if the table needs a sync.
    pub fn needs_sync(&self) -> bool {
        self.staleness_level.needs_refresh()
    }
}

/// Service for tracking data freshness.
pub struct FreshnessService {
    db: PgPool,
}

impl FreshnessService {
    /// Create a new freshness service.
    pub fn new(db: PgPool) -> Self {
        Self { db }
    }

    /// Get freshness for all warehouse tables.
    pub async fn get_all_freshness(&self) -> Result<Vec<TableFreshness>, sqlx::Error> {
        let now = Utc::now();

        let rows = sqlx::query(
            r#"
            SELECT 
                t.name as table_name,
                t.source_id,
                s.name as source_name,
                (
                    SELECT MAX(completed_at) 
                    FROM warehouse_syncs 
                    WHERE source_id = t.source_id 
                      AND table_name = t.name 
                      AND status = 'completed'
                ) as last_sync_at,
                sch_next.next_run_at,
                COALESCE(
                    (SELECT status FROM warehouse_jobs 
                     WHERE source_id = t.source_id 
                       AND (table_name = t.name OR table_name IS NULL)
                     ORDER BY scheduled_at DESC LIMIT 1),
                    'idle'
                ) as sync_status
            FROM warehouse_tables t
            JOIN warehouse_sources s ON s.id = t.source_id
            LEFT JOIN LATERAL (
                SELECT sch.next_run_at
                FROM warehouse_sync_schedules sch
                WHERE sch.source_id = t.source_id AND sch.enabled = true
                ORDER BY sch.next_run_at ASC NULLS LAST
                LIMIT 1
            ) sch_next ON true
            WHERE t.sync_enabled = true
            ORDER BY t.name
            "#
        )
        .fetch_all(&self.db)
        .await?;

        let freshness: Vec<TableFreshness> = rows
            .into_iter()
            .map(|row| {
                let last_sync: Option<DateTime<Utc>> = row.get("last_sync_at");
                let staleness = last_sync.map(|ls| now - ls);
                let staleness_level = StalenessLevel::from_duration(staleness);

                TableFreshness {
                    table_name: row.get("table_name"),
                    source_name: row.get("source_name"),
                    source_id: row.get("source_id"),
                    last_sync_at: last_sync,
                    next_sync_at: row.get("next_run_at"),
                    sync_status: row.get("sync_status"),
                    staleness,
                    staleness_level,
                    staleness_minutes: staleness.map(|d| d.num_minutes()),
                }
            })
            .collect();

        Ok(freshness)
    }

    /// Get freshness for a specific table.
    pub async fn get_table_freshness(
        &self,
        source_id: Uuid,
        table_name: &str,
    ) -> Result<Option<TableFreshness>, sqlx::Error> {
        let now = Utc::now();

        let row = sqlx::query(
            r#"
            SELECT 
                t.name as table_name,
                t.source_id,
                s.name as source_name,
                (
                    SELECT MAX(completed_at) 
                    FROM warehouse_syncs 
                    WHERE source_id = t.source_id 
                      AND table_name = t.name 
                      AND status = 'completed'
                ) as last_sync_at,
                sch.next_run_at,
                COALESCE(
                    (SELECT status FROM warehouse_jobs 
                     WHERE source_id = t.source_id 
                       AND (table_name = t.name OR table_name IS NULL)
                     ORDER BY scheduled_at DESC LIMIT 1),
                    'idle'
                ) as sync_status
            FROM warehouse_tables t
            JOIN warehouse_sources s ON s.id = t.source_id
            LEFT JOIN warehouse_sync_schedules sch ON sch.source_id = t.source_id AND sch.enabled = true
            WHERE t.source_id = $1 AND t.name = $2
            "#
        )
        .bind(source_id)
        .bind(table_name)
        .fetch_optional(&self.db)
        .await?;

        Ok(row.map(|r| {
            let last_sync: Option<DateTime<Utc>> = r.get("last_sync_at");
            let staleness = last_sync.map(|ls| now - ls);
            let staleness_level = StalenessLevel::from_duration(staleness);

            TableFreshness {
                table_name: r.get("table_name"),
                source_name: r.get("source_name"),
                source_id: r.get("source_id"),
                last_sync_at: last_sync,
                next_sync_at: r.get("next_run_at"),
                sync_status: r.get("sync_status"),
                staleness,
                staleness_level,
                staleness_minutes: staleness.map(|d| d.num_minutes()),
            }
        }))
    }

    /// Get tables that need refresh.
    pub async fn get_stale_tables(&self) -> Result<Vec<TableFreshness>, sqlx::Error> {
        let all = self.get_all_freshness().await?;
        Ok(all.into_iter().filter(|t| t.needs_sync()).collect())
    }

    /// Get freshness summary.
    pub async fn get_summary(&self) -> Result<FreshnessSummary, sqlx::Error> {
        let all = self.get_all_freshness().await?;

        let mut summary = FreshnessSummary::default();
        summary.total_tables = all.len();

        for table in &all {
            match table.staleness_level {
                StalenessLevel::Fresh => summary.fresh_count += 1,
                StalenessLevel::Moderate => summary.moderate_count += 1,
                StalenessLevel::Stale => summary.stale_count += 1,
                StalenessLevel::VeryStale => summary.very_stale_count += 1,
                StalenessLevel::Unknown => summary.unknown_count += 1,
            }
        }

        if summary.total_tables > 0 {
            summary.freshness_score =
                (summary.fresh_count as f64 / summary.total_tables as f64) * 100.0;
        }

        Ok(summary)
    }
}

/// Summary of data freshness across all tables.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FreshnessSummary {
    /// Total number of tables
    pub total_tables: usize,
    /// Tables with fresh data
    pub fresh_count: usize,
    /// Tables with moderate freshness
    pub moderate_count: usize,
    /// Tables with stale data
    pub stale_count: usize,
    /// Tables with very stale data
    pub very_stale_count: usize,
    /// Tables with unknown freshness
    pub unknown_count: usize,
    /// Overall freshness score (0-100)
    pub freshness_score: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_staleness_level_from_duration() {
        assert_eq!(
            StalenessLevel::from_duration(Some(Duration::minutes(30))),
            StalenessLevel::Fresh
        );
        assert_eq!(
            StalenessLevel::from_duration(Some(Duration::hours(3))),
            StalenessLevel::Moderate
        );
        assert_eq!(
            StalenessLevel::from_duration(Some(Duration::hours(12))),
            StalenessLevel::Stale
        );
        assert_eq!(
            StalenessLevel::from_duration(Some(Duration::hours(48))),
            StalenessLevel::VeryStale
        );
        assert_eq!(
            StalenessLevel::from_duration(None),
            StalenessLevel::Unknown
        );
    }

    #[test]
    fn test_needs_refresh() {
        assert!(!StalenessLevel::Fresh.needs_refresh());
        assert!(!StalenessLevel::Moderate.needs_refresh());
        assert!(StalenessLevel::Stale.needs_refresh());
        assert!(StalenessLevel::VeryStale.needs_refresh());
        assert!(StalenessLevel::Unknown.needs_refresh());
    }
}
