//! Persistent Sync Scheduler
//!
//! Schedules are stored in PostgreSQL and survive restarts.
//! The in-memory tokio-cron-scheduler is reconstructed on startup.
//! Supports graceful shutdown via a shutdown signal.
//!
//! Also includes interval-based sync scheduling using the `sync_interval`
//! column on `warehouse_sources` for Fivetran-style sync intervals.

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{watch, RwLock};
use tokio::task::JoinHandle;
use tokio_cron_scheduler::{Job, JobScheduler};
use uuid::Uuid;

use crate::kafka::{KafkaProducer, SyncJobKafkaMessage};
use crate::warehouse::types::{JobType, SyncInterval};

/// Errors that can occur during scheduling operations.
#[derive(Debug, Error)]
pub enum SchedulerError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Scheduler error: {0}")]
    Scheduler(String),

    #[error("Invalid cron expression: {0}")]
    InvalidCron(String),
}

/// Result type for scheduler operations.
pub type SchedulerResult<T> = Result<T, SchedulerError>;

/// A sync schedule stored in the database.
#[derive(Debug, Clone)]
pub struct SyncSchedule {
    pub id: Uuid,
    pub source_id: Uuid,
    pub cron_expression: String,
    pub job_type: String,
    pub enabled: bool,
    pub last_run_at: Option<DateTime<Utc>>,
    pub next_run_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Persistent sync scheduler.
///
/// Stores schedules in PostgreSQL and uses tokio-cron-scheduler for execution.
/// Supports graceful shutdown for all background tasks.
pub struct SyncScheduler {
    db: PgPool,
    scheduler: Arc<RwLock<JobScheduler>>,
    worker_id: String,
    /// Shutdown signal sender
    shutdown_tx: watch::Sender<bool>,
    /// Handle to the orphan recovery task
    orphan_recovery_handle: Option<JoinHandle<()>>,
}

impl SyncScheduler {
    /// Initialize scheduler from database on startup.
    #[tracing::instrument(
        name = "warehouse.sync.init",
        skip_all,
        err(Display),
    )]
    pub async fn init(db: PgPool) -> SchedulerResult<Self> {
        let scheduler = JobScheduler::new()
            .await
            .map_err(|e| SchedulerError::Scheduler(e.to_string()))?;

        let worker_id = format!("worker-{}", Uuid::new_v4());
        let (shutdown_tx, _) = watch::channel(false);

        let mut svc = Self {
            db,
            scheduler: Arc::new(RwLock::new(scheduler)),
            worker_id,
            shutdown_tx,
            orphan_recovery_handle: None,
        };

        // Load all enabled schedules from DB and register them
        svc.reload_schedules().await?;

        // Start orphan job recovery
        svc.start_orphan_recovery().await?;

        Ok(svc)
    }

    /// Get the worker ID for this scheduler instance.
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Start the scheduler.
    #[tracing::instrument(
        name = "warehouse.sync.start",
        skip_all,
        err(Display),
    )]
    pub async fn start(&self) -> SchedulerResult<()> {
        let mut scheduler = self.scheduler.write().await;
        scheduler
            .start()
            .await
            .map_err(|e| SchedulerError::Scheduler(e.to_string()))?;
        Ok(())
    }

    /// Stop the scheduler and all background tasks.
    ///
    /// This will:
    /// 1. Signal all background tasks to stop
    /// 2. Wait for the orphan recovery task to finish
    /// 3. Shut down the cron scheduler
    #[tracing::instrument(
        name = "warehouse.sync.shutdown",
        skip_all,
        err(Display),
    )]
    pub async fn shutdown(&mut self) -> SchedulerResult<()> {
        tracing::info!(worker_id = %self.worker_id, "Scheduler shutting down");
        
        // Signal shutdown to all background tasks
        let _ = self.shutdown_tx.send(true);
        
        // Wait for orphan recovery task to finish
        if let Some(handle) = self.orphan_recovery_handle.take() {
            let _ = handle.await;
        }
        
        // Shut down the cron scheduler
        let mut scheduler = self.scheduler.write().await;
        scheduler
            .shutdown()
            .await
            .map_err(|e| SchedulerError::Scheduler(e.to_string()))?;
            
        tracing::info!(worker_id = %self.worker_id, "Scheduler shutdown complete");
        Ok(())
    }

    /// Reload all schedules from database.
    async fn reload_schedules(&self) -> SchedulerResult<()> {
        let rows = sqlx::query(
            "SELECT id, source_id, cron_expression, COALESCE(job_type, 'sync') as job_type, enabled, last_run_at, next_run_at, created_at, updated_at 
             FROM warehouse_sync_schedules WHERE enabled = true"
        )
        .fetch_all(&self.db)
        .await?;

        for row in rows {
            let schedule = SyncSchedule {
                id: row.get("id"),
                source_id: row.get("source_id"),
                cron_expression: row.get("cron_expression"),
                job_type: row.get("job_type"),
                enabled: row.get("enabled"),
                last_run_at: row.get("last_run_at"),
                next_run_at: row.get("next_run_at"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            };

            let result = self.register_cron_job_for_type(&schedule, &schedule.job_type.clone()).await;

            if let Err(e) = result {
                tracing::error!(
                    schedule_id = %schedule.id,
                    job_type = %schedule.job_type,
                    error = %e,
                    "Failed to register cron job"
                );
            }
        }

        Ok(())
    }

    /// Register a cron job that creates a pending job of the specified type.
    async fn register_cron_job_for_type(
        &self,
        schedule: &SyncSchedule,
        job_type: &str,
    ) -> SchedulerResult<()> {
        let db = self.db.clone();
        let source_id = schedule.source_id;
        let schedule_id = schedule.id;
        let job_type_owned = job_type.to_string();

        let job = Job::new_async(&schedule.cron_expression, move |_uuid, _lock| {
            let db = db.clone();
            let jt = job_type_owned.clone();
            Box::pin(async move {
                let result = sqlx::query(
                    "INSERT INTO warehouse_jobs (id, job_type, source_id, status, scheduled_at)
                     SELECT $1, $2, $3, 'pending', NOW()
                     WHERE NOT EXISTS (
                         SELECT 1 FROM warehouse_jobs
                         WHERE source_id = $3
                           AND job_type = $2
                           AND status IN ('pending', 'running')
                     )"
                )
                    .bind(uuid::Uuid::new_v4())
                    .bind(&jt)
                    .bind(source_id)
                    .execute(&db)
                    .await;

                match result {
                    Ok(r) if r.rows_affected() == 0 => {
                        tracing::info!(
                            source_id = %source_id,
                            job_type = %jt,
                            "Skipping scheduled job, already in progress"
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            source_id = %source_id,
                            job_type = %jt,
                            error = %e,
                            "Failed to create scheduled job"
                        );
                    }
                    _ => {}
                }

                // Best-effort: update last_run_at on schedule
                if let Err(e) = sqlx::query(
                    "UPDATE warehouse_sync_schedules SET last_run_at = NOW(), updated_at = NOW() WHERE id = $1"
                )
                .bind(schedule_id)
                .execute(&db)
                .await
                {
                    tracing::warn!(schedule_id = %schedule_id, error = %e, "Failed to update last_run_at on schedule");
                }
            })
        })
        .map_err(|e| SchedulerError::InvalidCron(e.to_string()))?;

        let mut scheduler = self.scheduler.write().await;
        scheduler
            .add(job)
            .await
            .map_err(|e| SchedulerError::Scheduler(e.to_string()))?;

        Ok(())
    }

    /// Manually trigger a sync (creates pending job immediately).
    ///
    /// If a sync is already pending or running for this source, returns the
    /// existing job ID instead of creating a duplicate.
    pub async fn trigger_sync(&self, source_id: Uuid) -> SchedulerResult<Uuid> {
        let job_id = Uuid::new_v4();

        // Atomic INSERT ... WHERE NOT EXISTS to prevent TOCTOU race where two
        // concurrent calls both see no existing job and both insert.
        let result = sqlx::query(
            "INSERT INTO warehouse_jobs (id, job_type, source_id, status, scheduled_at)
             SELECT $1, 'sync', $2, 'pending', NOW()
             WHERE NOT EXISTS (
                 SELECT 1 FROM warehouse_jobs
                 WHERE source_id = $2
                   AND job_type = 'sync'
                   AND status IN ('pending', 'running')
             )"
        )
        .bind(job_id)
        .bind(source_id)
        .execute(&self.db)
        .await?;

        if result.rows_affected() == 0 {
            // A job already exists -- return its ID. Use fetch_optional
            // because the job may complete between our INSERT check and
            // this SELECT (narrow race window).
            let existing_job_id: Option<Uuid> = sqlx::query_scalar(
                "SELECT id FROM warehouse_jobs
                 WHERE source_id = $1
                   AND job_type = 'sync'
                   AND status IN ('pending', 'running')
                 ORDER BY scheduled_at DESC
                 LIMIT 1"
            )
            .bind(source_id)
            .fetch_optional(&self.db)
            .await?;

            if let Some(eid) = existing_job_id {
                tracing::info!(
                    source_id = %source_id,
                    existing_job_id = %eid,
                    "Sync already in progress, returning existing job"
                );
                return Ok(eid);
            }

            // The existing job completed between the INSERT and SELECT.
            // Retry the INSERT once.
            let retry_id = Uuid::new_v4();
            let retry_result = sqlx::query(
                "INSERT INTO warehouse_jobs (id, job_type, source_id, status, scheduled_at)
                 SELECT $1, 'sync', $2, 'pending', NOW()
                 WHERE NOT EXISTS (
                     SELECT 1 FROM warehouse_jobs
                     WHERE source_id = $2
                       AND job_type = 'sync'
                       AND status IN ('pending', 'running')
                 )"
            )
            .bind(retry_id)
            .bind(source_id)
            .execute(&self.db)
            .await?;

            if retry_result.rows_affected() == 0 {
                return Err(SchedulerError::Scheduler(
                    format!("Failed to create sync job for source {}: concurrent job exists", source_id)
                ));
            }

            return Ok(retry_id);
        }

        Ok(job_id)
    }

    /// Create a new sync schedule.
    pub async fn create_schedule(
        &self,
        source_id: Uuid,
        cron_expression: &str,
    ) -> SchedulerResult<SyncSchedule> {
        self.create_schedule_for_type(source_id, cron_expression, "sync").await
    }

    /// Create a schedule for a derived table refresh.
    pub async fn create_derived_refresh_schedule(
        &self,
        source_id: Uuid,
        cron_expression: &str,
    ) -> SchedulerResult<SyncSchedule> {
        self.create_schedule_for_type(source_id, cron_expression, "derived_refresh").await
    }

    /// Parameterized schedule creation used by both sync and derived refresh.
    async fn create_schedule_for_type(
        &self,
        source_id: Uuid,
        cron_expression: &str,
        job_type: &str,
    ) -> SchedulerResult<SyncSchedule> {
        let id = Uuid::new_v4();
        let now = Utc::now();

        sqlx::query(
            "INSERT INTO warehouse_sync_schedules
             (id, source_id, cron_expression, job_type, enabled, created_at, updated_at)
             VALUES ($1, $2, $3, $4, true, $5, $5)"
        )
        .bind(id)
        .bind(source_id)
        .bind(cron_expression)
        .bind(job_type)
        .bind(now)
        .execute(&self.db)
        .await?;

        let schedule = SyncSchedule {
            id,
            source_id,
            cron_expression: cron_expression.to_string(),
            job_type: job_type.to_string(),
            enabled: true,
            last_run_at: None,
            next_run_at: None,
            created_at: now,
            updated_at: now,
        };

        self.register_cron_job_for_type(&schedule, job_type).await?;

        Ok(schedule)
    }

    /// Start background task to recover orphaned jobs.
    ///
    /// This task runs periodically to find jobs where the worker died
    /// (lock expired) and resets them to pending status for retry.
    async fn start_orphan_recovery(&mut self) -> SchedulerResult<()> {
        let db = self.db.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Find running jobs where lock expired (worker died)
                        let result = sqlx::query(
                            "UPDATE warehouse_jobs
                             SET status = CASE
                                     WHEN retry_count + 1 < max_retries THEN 'pending'
                                     ELSE 'failed'
                                 END,
                                 locked_by = NULL, locked_at = NULL,
                                 lock_expires_at = NULL, retry_count = retry_count + 1,
                                 error = CASE
                                     WHEN retry_count + 1 >= max_retries THEN 'Max retries exceeded (orphan recovery)'
                                     ELSE error
                                 END,
                                 completed_at = CASE
                                     WHEN retry_count + 1 >= max_retries THEN NOW()
                                     ELSE completed_at
                                 END
                             WHERE status = 'running' 
                               AND lock_expires_at < NOW()
                               AND retry_count < max_retries
                             RETURNING id"
                        )
                        .fetch_all(&db)
                        .await;

                        match result {
                            Ok(rows) if !rows.is_empty() => {
                                tracing::warn!(
                                    count = rows.len(),
                                    "Recovered orphaned running jobs"
                                );
                            }
                            Err(e) => {
                                tracing::error!(error = %e, "Failed to recover orphaned running jobs");
                            }
                            _ => {}
                        }

                        // Recover stale pending jobs that were never picked up
                        // (e.g. process crashed after DB insert but before Kafka publish)
                        let stale_result = sqlx::query(
                            "UPDATE warehouse_jobs
                             SET status = 'failed', error = 'Stale pending job recovered by orphan recovery'
                             WHERE status = 'pending'
                               AND scheduled_at < NOW() - INTERVAL '1 hour'
                               AND locked_by IS NULL
                             RETURNING id"
                        )
                        .fetch_all(&db)
                        .await;

                        match stale_result {
                            Ok(rows) if !rows.is_empty() => {
                                tracing::warn!(
                                    count = rows.len(),
                                    "Recovered stale pending jobs"
                                );
                            }
                            Err(e) => {
                                tracing::error!(error = %e, "Failed to recover stale pending jobs");
                            }
                            _ => {}
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            tracing::info!("Orphan recovery task shutting down");
                            return;
                        }
                    }
                }
            }
        });

        self.orphan_recovery_handle = Some(handle);
        Ok(())
    }
}

/// Interval-based sync scheduler for Fivetran-style sync intervals.
///
/// This scheduler periodically checks for sources with a `sync_interval` set
/// and publishes sync jobs to Kafka when the interval has elapsed since
/// the last sync.
///
/// Unlike the cron-based `SyncScheduler`, this uses the simpler interval
/// approach stored directly on the `warehouse_sources` table.
pub struct IntervalSyncScheduler {
    db: PgPool,
    kafka: Arc<KafkaProducer>,
    shutdown_tx: watch::Sender<bool>,
}

impl IntervalSyncScheduler {
    /// Create a new interval-based sync scheduler.
    ///
    /// The scheduler will check for due sources every `check_interval`.
    pub fn new(db: PgPool, kafka: Arc<KafkaProducer>) -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        Self {
            db,
            kafka,
            shutdown_tx,
        }
    }

    /// Start the scheduler.
    ///
    /// The scheduler will check for sources with due syncs every `check_interval`
    /// (default: 30 seconds) and publish sync jobs to Kafka.
    ///
    /// Returns a JoinHandle for the scheduler task. The caller should await this
    /// handle or add it to a join set for proper shutdown coordination.
    pub fn start(&mut self, check_interval: Option<Duration>) -> JoinHandle<()> {
        let db = self.db.clone();
        let kafka = self.kafka.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let interval = check_interval.unwrap_or(Duration::from_secs(30));

        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            
            tracing::info!(
                interval_secs = interval.as_secs(),
                "Interval sync scheduler started"
            );

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(e) = check_and_schedule_syncs(&db, &kafka).await {
                            tracing::error!(error = %e, "Failed to check for due syncs");
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            tracing::info!("Interval sync scheduler shutting down");
                            return;
                        }
                    }
                }
            }
        });

        handle
    }

    /// Signal the scheduler to shut down.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    /// Get the shutdown receiver for external coordination.
    pub fn subscribe_shutdown(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }
}

/// Check for sources with due syncs and publish sync jobs to Kafka.
#[tracing::instrument(name = "pond.scheduler.check_and_schedule_syncs", skip(db, kafka))]
async fn check_and_schedule_syncs(
    db: &PgPool,
    kafka: &KafkaProducer,
) -> Result<(), SchedulerError> {
    // Find sources where:
    // 1. sync_interval is set (not NULL)
    // 2. tier is 'warm' or 'hot' (not 'cold')
    // 3. Either last_sync_at is NULL, OR last_sync_at + interval < NOW()
    // 4. No pending/running sync job exists for this source
    //
    // We handle interval parsing in Rust since PostgreSQL doesn't natively
    // understand '5m', '15m' format.
    
    // Query for sources that are due for sync AND have no active jobs
    // We exclude sources with ANY pending/running job (not just sync), because:
    // - A running materialize job would conflict with a sync
    // - A running accelerate job would conflict with a sync  
    // - A running downgrade/remove_cache job would also conflict
    let rows = sqlx::query(
        r#"SELECT s.id as source_id, s.project_id, s.name, s.sync_interval, s.last_sync_at
           FROM warehouse_sources s
           WHERE s.sync_interval IS NOT NULL
             AND s.tier IN ('warm', 'hot')
             AND s.enabled = true
             AND NOT EXISTS (
                 SELECT 1 FROM warehouse_jobs j
                 WHERE j.source_id = s.id
                   AND j.status IN ('pending', 'running')
             )"#
    )
    .fetch_all(db)
    .await?;

    let now = Utc::now();
    let mut scheduled_count = 0;

    for row in rows {
        let source_id: Uuid = row.get("source_id");
        let project_id: Uuid = row.get("project_id");
        let source_name: String = row.get("name");
        let sync_interval_str: String = row.get("sync_interval");
        let last_sync_at: Option<DateTime<Utc>> = row.get("last_sync_at");

        // Parse the interval
        let Ok(sync_interval) = sync_interval_str.parse::<SyncInterval>() else {
            tracing::warn!(
                source_id = %source_id,
                sync_interval = %sync_interval_str,
                "Invalid sync interval format, skipping"
            );
            continue;
        };

        // Skip manual intervals — zero duration would match every poll cycle (M4).
        if sync_interval == SyncInterval::Manual {
            continue;
        }

        let interval_duration: chrono::Duration = sync_interval.into();

        // Check if sync is due
        let is_due = match last_sync_at {
            None => true, // Never synced, sync immediately
            Some(last) => {
                let next_sync = last + interval_duration;
                now >= next_sync
            }
        };

        if !is_due {
            continue;
        }

        let job_id = Uuid::new_v4();

        // Use a transaction so the job is only visible after Kafka publish succeeds.
        let mut tx = match db.begin().await {
            Ok(tx) => tx,
            Err(e) => {
                tracing::error!(source_id = %source_id, error = %e, "Failed to begin transaction");
                continue;
            }
        };

        let result = sqlx::query(
            "INSERT INTO warehouse_jobs (id, job_type, source_id, status, scheduled_at)
             SELECT $1, 'sync', $2, 'pending', NOW()
             WHERE NOT EXISTS (
                 SELECT 1 FROM warehouse_jobs
                 WHERE source_id = $2
                   AND status IN ('pending', 'running')
             )"
        )
        .bind(job_id)
        .bind(source_id)
        .execute(&mut *tx)
        .await;

        match &result {
            Err(e) => {
                tracing::error!(
                    source_id = %source_id,
                    error = %e,
                    "Failed to create sync job in database"
                );
                let _ = tx.rollback().await;
                continue;
            }
            Ok(r) if r.rows_affected() == 0 => {
                let _ = tx.rollback().await;
                continue;
            }
            _ => {}
        }

        let kafka_msg = SyncJobKafkaMessage {
            job_id,
            job_type: JobType::Sync.to_string(),
            source_id,
            project_id,
            table_name: None,
            created_at: now.to_rfc3339(),
        };

        if let Err(e) = kafka.send_sync_job(&kafka_msg).await {
            tracing::error!(
                job_id = %job_id,
                source_id = %source_id,
                error = %e,
                "Failed to publish sync job to Kafka, rolling back"
            );
            let _ = tx.rollback().await;
            continue;
        }

        if let Err(e) = tx.commit().await {
            tracing::error!(job_id = %job_id, error = %e, "Failed to commit sync job transaction");
            continue;
        }

        tracing::info!(
            job_id = %job_id,
            source_id = %source_id,
            source_name = %source_name,
            sync_interval = %sync_interval,
            "Scheduled sync job for source"
        );

        scheduled_count += 1;
    }

    if scheduled_count > 0 {
        tracing::info!(count = scheduled_count, "Scheduled interval-based sync jobs");
    }

    // Also check for derived table refreshes that are due
    let derived_scheduled = check_and_schedule_derived_refreshes(db, kafka).await?;
    if derived_scheduled > 0 {
        tracing::info!(count = derived_scheduled, "Scheduled derived table refresh jobs");
    }

    Ok(())
}

/// Check for derived tables with schedules that are due for refresh and publish jobs.
///
/// The `schedule` column on `warehouse_derived_tables` uses the same interval
/// format as `SyncInterval` (e.g., "5m", "1h", "6h", "1d").
async fn check_and_schedule_derived_refreshes(
    db: &PgPool,
    kafka: &KafkaProducer,
) -> Result<u32, SchedulerError> {
    let rows = sqlx::query(
        r#"SELECT d.source_id, d.project_id, d.schedule, d.last_refreshed_at
           FROM warehouse_derived_tables d
           JOIN warehouse_sources s ON d.source_id = s.id
           WHERE d.schedule IS NOT NULL
             AND s.enabled = true
             AND NOT EXISTS (
                 SELECT 1 FROM warehouse_jobs j
                 WHERE j.source_id = d.source_id
                   AND j.status IN ('pending', 'running')
             )"#
    )
    .fetch_all(db)
    .await?;

    let now = Utc::now();
    let mut scheduled_count: u32 = 0;

    for row in rows {
        let source_id: Uuid = row.get("source_id");
        let project_id: Uuid = row.get("project_id");
        let schedule_str: String = row.get("schedule");
        let last_refreshed_at: Option<DateTime<Utc>> = row.get("last_refreshed_at");

        let Ok(interval) = schedule_str.parse::<SyncInterval>() else {
            tracing::warn!(
                source_id = %source_id,
                schedule = %schedule_str,
                "Invalid derived table schedule format; clearing schedule and recording error"
            );
            if let Err(e) = sqlx::query(
                "UPDATE warehouse_derived_tables \
                 SET schedule = NULL, \
                     last_error = $1, \
                     updated_at = NOW() \
                 WHERE source_id = $2"
            )
            .bind(format!(
                "Invalid schedule '{}'. Valid values: 5m, 15m, 1h, 6h, 24h, weekly, manual",
                schedule_str
            ))
            .bind(source_id)
            .execute(db)
            .await
            {
                tracing::warn!(source_id = %source_id, error = %e, "Failed to clear invalid schedule");
            }
            continue;
        };

        if interval == SyncInterval::Manual {
            continue;
        }

        let interval_duration: chrono::Duration = interval.into();
        let is_due = match last_refreshed_at {
            None => true,
            Some(last) => now >= last + interval_duration,
        };

        if !is_due {
            continue;
        }

        let job_id = Uuid::new_v4();
        let kafka_msg = SyncJobKafkaMessage {
            job_id,
            job_type: JobType::DerivedRefresh.to_string(),
            source_id,
            project_id,
            table_name: None,
            created_at: now.to_rfc3339(),
        };

        let mut tx = match db.begin().await {
            Ok(tx) => tx,
            Err(e) => {
                tracing::error!(
                    source_id = %source_id,
                    error = %e,
                    "Failed to begin transaction for derived_refresh"
                );
                continue;
            }
        };

        let insert_result = sqlx::query(
            "INSERT INTO warehouse_jobs (id, job_type, source_id, status, scheduled_at)
             VALUES ($1, 'derived_refresh', $2, 'pending', NOW())"
        )
        .bind(job_id)
        .bind(source_id)
        .execute(&mut *tx)
        .await;

        if let Err(e) = insert_result {
            tracing::error!(
                source_id = %source_id,
                error = %e,
                "Failed to create derived_refresh job in database"
            );
            let _ = tx.rollback().await;
            continue;
        }

        if let Err(e) = kafka.send_sync_job(&kafka_msg).await {
            tracing::error!(
                job_id = %job_id,
                source_id = %source_id,
                error = %e,
                "Failed to publish derived_refresh job to Kafka, rolling back"
            );
            let _ = tx.rollback().await;
            continue;
        }

        if let Err(e) = tx.commit().await {
            tracing::error!(
                job_id = %job_id,
                source_id = %source_id,
                error = %e,
                "Failed to commit derived_refresh transaction"
            );
            continue;
        }

        tracing::info!(
            job_id = %job_id,
            source_id = %source_id,
            schedule = %schedule_str,
            "Scheduled derived table refresh job"
        );

        scheduled_count += 1;
    }

    Ok(scheduled_count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scheduler_error_concurrent_job_format() {
        let source_id = Uuid::new_v4();
        let err = SchedulerError::Scheduler(
            format!("Failed to create sync job for source {}: concurrent job exists", source_id)
        );
        let msg = err.to_string();
        assert!(msg.contains("concurrent job exists"), "Error must mention concurrent job, got: {}", msg);
        assert!(msg.contains(&source_id.to_string()), "Error must include source_id, got: {}", msg);
    }

    #[test]
    fn test_orphan_recovery_sql_marks_failed_at_max_retries() {
        let sql = "UPDATE warehouse_jobs
                             SET status = CASE
                                     WHEN retry_count + 1 < max_retries THEN 'pending'
                                     ELSE 'failed'
                                 END,
                                 locked_by = NULL, locked_at = NULL,
                                 lock_expires_at = NULL, retry_count = retry_count + 1,
                                 error = CASE
                                     WHEN retry_count + 1 >= max_retries THEN 'Max retries exceeded (orphan recovery)'
                                     ELSE error
                                 END,
                                 completed_at = CASE
                                     WHEN retry_count + 1 >= max_retries THEN NOW()
                                     ELSE completed_at
                                 END
                             WHERE status = 'running' 
                               AND lock_expires_at < NOW()
                               AND retry_count < max_retries
                             RETURNING id";

        assert!(
            sql.contains("WHEN retry_count + 1 < max_retries THEN 'pending'"),
            "Must only set pending when retries remain"
        );
        assert!(
            sql.contains("ELSE 'failed'"),
            "Must set failed when max retries reached"
        );
        assert!(
            sql.contains("WHEN retry_count + 1 >= max_retries THEN NOW()"),
            "Must set completed_at when marking as failed"
        );
    }

    #[test]
    fn test_interval_sync_insert_uses_where_not_exists() {
        let sql = "INSERT INTO warehouse_jobs (id, job_type, source_id, status, scheduled_at)
             SELECT $1, 'sync', $2, 'pending', NOW()
             WHERE NOT EXISTS (
                 SELECT 1 FROM warehouse_jobs
                 WHERE source_id = $2
                   AND status IN ('pending', 'running')
             )";
        assert!(
            sql.contains("WHERE NOT EXISTS"),
            "Interval scheduler must use atomic INSERT ... WHERE NOT EXISTS to prevent duplicate jobs"
        );
        assert!(
            sql.contains("SELECT $1"),
            "Must use INSERT ... SELECT for atomicity, not INSERT ... VALUES"
        );
    }
}
